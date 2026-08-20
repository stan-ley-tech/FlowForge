package engine

import (
	"context"
	"log"
	"time"

	"github.com/stan-ley-tech/flowforge/engine/internal/model"
	"github.com/stan-ley-tech/flowforge/engine/internal/store"
)

// Scheduler runs the background sweeps that don't happen as a direct
// reaction to an API call: reclaiming leases a dead worker never
// released, failing steps that ran past their timeout, and admitting
// queued runs once concurrency headroom appears. Everything it does, it
// does by reading and writing the same store the API handlers use, so
// there's nothing here an engine restart could lose.
type Scheduler struct {
	engine   *Engine
	store    *store.Store
	interval time.Duration
}

func NewScheduler(e *Engine, s *store.Store, interval time.Duration) *Scheduler {
	if interval <= 0 {
		interval = time.Second
	}
	return &Scheduler{engine: e, store: s, interval: interval}
}

func (sch *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(sch.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sch.tick(ctx)
		}
	}
}

func (sch *Scheduler) tick(ctx context.Context) {
	if err := sch.reclaimExpiredLeases(ctx); err != nil {
		log.Printf("scheduler: reclaim expired leases: %v", err)
	}
	if err := sch.enforceStepTimeouts(ctx); err != nil {
		log.Printf("scheduler: enforce step timeouts: %v", err)
	}
	if err := sch.admitQueuedRuns(ctx); err != nil {
		log.Printf("scheduler: admit queued runs: %v", err)
	}
}

// reclaimExpiredLeases is the mechanism behind crash recovery: a step
// whose lease passed without a heartbeat or a completion report is
// assumed to belong to a worker that's gone, and is fed back into
// applyFailure exactly as if the worker had reported the failure itself.
// Whether it then retries or triggers compensation follows the same
// retry policy as any other failure.
func (sch *Scheduler) reclaimExpiredLeases(ctx context.Context) error {
	now := sch.engine.now()
	expired, err := sch.store.Read().FindExpiredLeaseSteps(ctx, now)
	if err != nil {
		return err
	}
	for _, step := range expired {
		err := sch.store.Atomic(ctx, func(q *store.Queries) error {
			fresh, err := q.GetStep(ctx, step.ID)
			if err != nil {
				return err
			}
			if fresh.Status != model.StepLeased || fresh.LeaseExpiresAt == nil || fresh.LeaseExpiresAt.After(now) {
				return nil // already handled (completed, failed, or re-leased) since the read above
			}
			run, err := q.GetRun(ctx, fresh.RunID)
			if err != nil {
				return err
			}
			if err := q.InsertEvent(ctx, model.Event{RunID: run.ID, Type: model.EventLeaseReclaimed, StepName: fresh.Name, CreatedAt: now,
				Detail: mustJSON(map[string]any{"worker": fresh.LeaseOwner, "attempt": fresh.Attempt})}); err != nil {
				return err
			}
			return sch.engine.applyFailure(ctx, q, run, fresh, "worker lease expired without completion", now)
		})
		if err != nil {
			log.Printf("scheduler: reclaim step %s: %v", step.ID, err)
		}
	}
	return nil
}

// enforceStepTimeouts fails any step that has been running longer than
// its own configured timeout, even if the worker executing it is alive
// and heartbeating - the timeout is a business-logic cap, not a
// liveness check.
func (sch *Scheduler) enforceStepTimeouts(ctx context.Context) error {
	now := sch.engine.now()
	leased, err := sch.store.Read().FindLeasedSteps(ctx)
	if err != nil {
		return err
	}
	for _, step := range leased {
		if step.TimeoutSeconds <= 0 || step.StartedAt == nil {
			continue
		}
		deadline := step.StartedAt.Add(time.Duration(step.TimeoutSeconds) * time.Second)
		if now.Before(deadline) {
			continue
		}
		err := sch.store.Atomic(ctx, func(q *store.Queries) error {
			fresh, err := q.GetStep(ctx, step.ID)
			if err != nil {
				return err
			}
			if fresh.Status != model.StepLeased {
				return nil
			}
			run, err := q.GetRun(ctx, fresh.RunID)
			if err != nil {
				return err
			}
			if err := q.InsertEvent(ctx, model.Event{RunID: run.ID, Type: model.EventStepTimedOut, StepName: fresh.Name, CreatedAt: now,
				Detail: mustJSON(map[string]any{"timeout_seconds": fresh.TimeoutSeconds})}); err != nil {
				return err
			}
			return sch.engine.applyFailure(ctx, q, run, fresh, "step exceeded its configured timeout", now)
		})
		if err != nil {
			log.Printf("scheduler: enforce timeout for step %s: %v", step.ID, err)
		}
	}
	return nil
}

// admitQueuedRuns promotes QUEUED runs to RUNNING as concurrency headroom
// appears, oldest first, respecting the workflow's MaxConcurrentRuns cap.
func (sch *Scheduler) admitQueuedRuns(ctx context.Context) error {
	names, err := sch.distinctQueuedWorkflowNames(ctx)
	if err != nil {
		return err
	}
	for _, name := range names {
		if err := sch.admitQueuedRunsFor(ctx, name); err != nil {
			log.Printf("scheduler: admit queued runs for %s: %v", name, err)
		}
	}
	return nil
}

func (sch *Scheduler) distinctQueuedWorkflowNames(ctx context.Context) ([]string, error) {
	runs, err := sch.store.Read().ListRuns(ctx, "", 500)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var names []string
	for _, r := range runs {
		if r.Status == model.RunQueued && !seen[r.WorkflowName] {
			seen[r.WorkflowName] = true
			names = append(names, r.WorkflowName)
		}
	}
	return names, nil
}

func (sch *Scheduler) admitQueuedRunsFor(ctx context.Context, workflowName string) error {
	return sch.store.Atomic(ctx, func(q *store.Queries) error {
		def, err := q.GetLatestWorkflowDef(ctx, workflowName)
		if err != nil {
			return err
		}
		if def.MaxConcurrentRuns <= 0 {
			return nil
		}
		running, err := q.CountRunsByStatus(ctx, workflowName, model.RunRunning, model.RunCancelling, model.RunCompensating)
		if err != nil {
			return err
		}
		headroom := def.MaxConcurrentRuns - running
		if headroom <= 0 {
			return nil
		}
		queued, err := q.ListQueuedRuns(ctx, workflowName, headroom)
		if err != nil {
			return err
		}
		now := sch.engine.now()
		for _, run := range queued {
			run.Status = model.RunRunning
			if err := q.UpdateRun(ctx, run); err != nil {
				return err
			}
			first, err := q.GetStepByIndex(ctx, run.ID, 0, false)
			if err != nil {
				return err
			}
			first.Status = model.StepReady
			first.ScheduledAt = now.Add(time.Duration(first.DelaySeconds) * time.Second)
			if err := q.UpdateStep(ctx, first); err != nil {
				return err
			}
			if err := q.InsertEvent(ctx, model.Event{RunID: run.ID, Type: model.EventRunAdmitted, CreatedAt: now}); err != nil {
				return err
			}
			if err := q.InsertEvent(ctx, model.Event{RunID: run.ID, Type: model.EventStepReady, StepName: first.Name, CreatedAt: now}); err != nil {
				return err
			}
		}
		return nil
	})
}
