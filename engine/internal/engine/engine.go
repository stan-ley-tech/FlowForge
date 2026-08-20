// Package engine implements FlowForge's orchestration rules on top of the
// store package: admitting runs, materializing and advancing steps,
// applying retry policy on failure, and driving compensation. It holds no
// state of its own beyond a clock and a lease duration - everything that
// matters is read from and written to the store inside a transaction, so
// restarting the process picks up exactly where the database left off.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/stan-ley-tech/flowforge/engine/internal/model"
	"github.com/stan-ley-tech/flowforge/engine/internal/store"
)

var (
	ErrWorkflowNotFound   = errors.New("workflow not found")
	ErrRunNotFound        = errors.New("run not found")
	ErrStepNotFound       = errors.New("step not found")
	ErrLeaseMismatch      = errors.New("lease token does not match or step is not leased")
	ErrRunAlreadyTerminal = errors.New("run is already in a terminal state")
)

const defaultLeaseDuration = 30 * time.Second

type Engine struct {
	store         *store.Store
	leaseDuration time.Duration
	now           func() time.Time
}

type Option func(*Engine)

func WithLeaseDuration(d time.Duration) Option {
	return func(e *Engine) { e.leaseDuration = d }
}

func WithClock(fn func() time.Time) Option {
	return func(e *Engine) { e.now = fn }
}

func New(s *store.Store, opts ...Option) *Engine {
	e := &Engine{
		store:         s,
		leaseDuration: defaultLeaseDuration,
		now:           time.Now,
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

func isTerminal(s model.RunStatus) bool {
	switch s {
	case model.RunCompleted, model.RunFailed, model.RunCancelled, model.RunCompensated:
		return true
	default:
		return false
	}
}

// RegisterWorkflow stores a workflow definition, filling in default retry
// policies for any step that didn't specify one.
func (e *Engine) RegisterWorkflow(ctx context.Context, def model.WorkflowDef) error {
	if def.Name == "" {
		return fmt.Errorf("workflow name is required")
	}
	if len(def.Steps) == 0 {
		return fmt.Errorf("workflow %q must have at least one step", def.Name)
	}
	if def.Version == 0 {
		def.Version = 1
	}
	for i, sd := range def.Steps {
		def.Steps[i].Retry = fillRetryDefaults(sd.Retry)
	}
	for i, cd := range def.Compensations {
		def.Compensations[i].Retry = fillRetryDefaults(cd.Retry)
	}
	def.CreatedAt = e.now()

	return e.store.Atomic(ctx, func(q *store.Queries) error {
		return q.UpsertWorkflowDef(ctx, def)
	})
}

func fillRetryDefaults(rp model.RetryPolicy) model.RetryPolicy {
	if rp.MaxAttempts == 0 {
		rp.MaxAttempts = model.DefaultRetryPolicy().MaxAttempts
	}
	if rp.BackoffBaseMs == 0 {
		rp.BackoffBaseMs = model.DefaultRetryPolicy().BackoffBaseMs
	}
	if rp.BackoffMultiplier == 0 {
		rp.BackoffMultiplier = model.DefaultRetryPolicy().BackoffMultiplier
	}
	if rp.BackoffMaxMs == 0 {
		rp.BackoffMaxMs = model.DefaultRetryPolicy().BackoffMaxMs
	}
	return rp
}

func stepFromDef(runID string, sd model.StepDef, index int, isCompensation bool, now time.Time) model.Step {
	rp := fillRetryDefaults(sd.Retry)
	return model.Step{
		ID:                uuid.NewString(),
		RunID:             runID,
		Name:              sd.Name,
		StepIndex:         index,
		IsCompensation:    isCompensation,
		CompensationOf:    sd.CompensationOf,
		DelaySeconds:      sd.DelaySeconds,
		Status:            model.StepPending,
		MaxAttempts:       rp.MaxAttempts,
		BackoffBaseMs:     rp.BackoffBaseMs,
		BackoffMultiplier: rp.BackoffMultiplier,
		BackoffMaxMs:      rp.BackoffMaxMs,
		JitterFraction:    rp.JitterFraction,
		TimeoutSeconds:    sd.TimeoutSeconds,
		ScheduledAt:       now,
		IdempotencyKey:    runID + ":" + sd.Name,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
}

// StartRun admits a new run of the named workflow. If the workflow's
// concurrency cap is already reached, the run is created in QUEUED state
// and the scheduler admits it later as capacity frees up.
func (e *Engine) StartRun(ctx context.Context, workflowName string, input json.RawMessage) (model.Run, error) {
	if input == nil {
		input = json.RawMessage("{}")
	}
	var run model.Run
	err := e.store.Atomic(ctx, func(q *store.Queries) error {
		def, err := q.GetLatestWorkflowDef(ctx, workflowName)
		if err != nil {
			return ErrWorkflowNotFound
		}

		now := e.now()
		admit := true
		if def.MaxConcurrentRuns > 0 {
			running, err := q.CountRunsByStatus(ctx, workflowName, model.RunRunning, model.RunCancelling, model.RunCompensating)
			if err != nil {
				return err
			}
			admit = running < def.MaxConcurrentRuns
		}

		run = model.Run{
			ID:              uuid.NewString(),
			WorkflowName:    def.Name,
			WorkflowVersion: def.Version,
			Status:          model.RunQueued,
			Input:           input,
			Context:         json.RawMessage("{}"),
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		if admit {
			run.Status = model.RunRunning
		}
		if err := q.InsertRun(ctx, run); err != nil {
			return err
		}

		for i, sd := range def.Steps {
			step := stepFromDef(run.ID, sd, i, false, now)
			if i == 0 && admit {
				step.Status = model.StepReady
				step.ScheduledAt = now.Add(time.Duration(sd.DelaySeconds) * time.Second)
			}
			if err := q.InsertStep(ctx, step); err != nil {
				return err
			}
		}

		evtType := model.EventRunAdmitted
		if !admit {
			evtType = model.EventRunStarted
		}
		if err := q.InsertEvent(ctx, model.Event{RunID: run.ID, Type: evtType, CreatedAt: now}); err != nil {
			return err
		}
		if admit {
			return q.InsertEvent(ctx, model.Event{RunID: run.ID, Type: model.EventStepReady, StepName: def.Steps[0].Name, CreatedAt: now})
		}
		return nil
	})
	return run, err
}

func (e *Engine) GetRun(ctx context.Context, id string) (model.Run, error) {
	run, err := e.store.Read().GetRun(ctx, id)
	if err != nil {
		return model.Run{}, ErrRunNotFound
	}
	return run, nil
}

func (e *Engine) ListRuns(ctx context.Context, workflowName string, limit int) ([]model.Run, error) {
	if limit <= 0 {
		limit = 50
	}
	return e.store.Read().ListRuns(ctx, workflowName, limit)
}

func (e *Engine) ListSteps(ctx context.Context, runID string) ([]model.Step, error) {
	return e.store.Read().ListSteps(ctx, runID)
}

func (e *Engine) ListHistory(ctx context.Context, runID string) ([]model.Event, error) {
	return e.store.Read().ListEvents(ctx, runID)
}

// Task is what a worker receives from PollTask: everything it needs to
// execute a step without a further round trip.
type Task struct {
	StepID         string
	RunID          string
	StepName       string
	IsCompensation bool
	CompensationOf string
	Attempt        int
	MaxAttempts    int
	TimeoutSeconds int
	IdempotencyKey string
	LeaseToken     string
	Input          json.RawMessage
	Context        json.RawMessage
}

// PollTask atomically claims the oldest eligible step for workflowName
// and returns it, or nil if nothing is currently ready.
func (e *Engine) PollTask(ctx context.Context, workflowName, workerID string) (*Task, error) {
	var task *Task
	err := e.store.Atomic(ctx, func(q *store.Queries) error {
		now := e.now()
		step, err := q.ClaimReadyStep(ctx, workflowName, workerID, e.leaseDuration, now)
		if err != nil {
			return err
		}
		if step == nil {
			return nil
		}
		run, err := q.GetRun(ctx, step.RunID)
		if err != nil {
			return err
		}
		evtType := model.EventStepClaimed
		if err := q.InsertEvent(ctx, model.Event{RunID: run.ID, Type: evtType, StepName: step.Name, CreatedAt: now,
			Detail: mustJSON(map[string]any{"attempt": step.Attempt, "worker": workerID, "is_compensation": step.IsCompensation})}); err != nil {
			return err
		}
		task = &Task{
			StepID: step.ID, RunID: run.ID, StepName: step.Name, IsCompensation: step.IsCompensation,
			CompensationOf: step.CompensationOf, Attempt: step.Attempt, MaxAttempts: step.MaxAttempts,
			TimeoutSeconds: step.TimeoutSeconds, IdempotencyKey: step.IdempotencyKey, LeaseToken: step.LeaseToken,
			Input: run.Input, Context: run.Context,
		}
		return nil
	})
	return task, err
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage("{}")
	}
	return b
}

// Heartbeat extends a step's lease. Long-running steps call this
// periodically so the scheduler doesn't mistake them for a dead worker.
func (e *Engine) Heartbeat(ctx context.Context, stepID, leaseToken string) error {
	return e.store.Atomic(ctx, func(q *store.Queries) error {
		step, err := q.GetStep(ctx, stepID)
		if err != nil {
			return ErrStepNotFound
		}
		if step.Status != model.StepLeased || step.LeaseToken != leaseToken {
			return ErrLeaseMismatch
		}
		now := e.now()
		expires := now.Add(e.leaseDuration)
		step.LeaseExpiresAt = &expires
		if err := q.UpdateStep(ctx, step); err != nil {
			return err
		}
		return q.InsertEvent(ctx, model.Event{RunID: step.RunID, Type: model.EventStepHeartbeat, StepName: step.Name, CreatedAt: now})
	})
}

// CompleteStep records a successful step execution and advances the run:
// to the next forward step, into compensation if the run is being
// cancelled, to the next compensation step, or to a terminal state if
// this was the last piece of work outstanding.
func (e *Engine) CompleteStep(ctx context.Context, stepID, leaseToken string, result json.RawMessage) error {
	if result == nil {
		result = json.RawMessage("null")
	}
	return e.store.Atomic(ctx, func(q *store.Queries) error {
		step, err := q.GetStep(ctx, stepID)
		if err != nil {
			return ErrStepNotFound
		}
		if step.Status != model.StepLeased || step.LeaseToken != leaseToken {
			return ErrLeaseMismatch
		}
		run, err := q.GetRun(ctx, step.RunID)
		if err != nil {
			return ErrRunNotFound
		}
		now := e.now()

		step.Status = model.StepCompleted
		if step.IsCompensation {
			step.Status = model.StepCompensated
		}
		step.Result = result
		step.LeaseToken = ""
		step.LeaseOwner = ""
		step.LeaseExpiresAt = nil
		if err := q.UpdateStep(ctx, step); err != nil {
			return err
		}

		evtType := model.EventStepCompleted
		if step.IsCompensation {
			evtType = model.EventCompensationCompleted
		}
		if err := q.InsertEvent(ctx, model.Event{RunID: run.ID, Type: evtType, StepName: step.Name, CreatedAt: now, Detail: result}); err != nil {
			return err
		}

		if step.IsCompensation {
			return e.advanceCompensation(ctx, q, run, step, now)
		}
		return e.advanceForward(ctx, q, run, step, result, now)
	})
}

func (e *Engine) advanceForward(ctx context.Context, q *store.Queries, run model.Run, step model.Step, result json.RawMessage, now time.Time) error {
	ctxMap := map[string]json.RawMessage{}
	if len(run.Context) > 0 {
		json.Unmarshal(run.Context, &ctxMap)
	}
	ctxMap[step.Name] = result
	mergedCtx, err := json.Marshal(ctxMap)
	if err != nil {
		return err
	}
	run.Context = mergedCtx

	if run.Status == model.RunCancelling {
		if err := q.UpdateRun(ctx, run); err != nil {
			return err
		}
		def, err := q.GetWorkflowDefVersion(ctx, run.WorkflowName, run.WorkflowVersion)
		if err != nil {
			return err
		}
		return e.beginCompensation(ctx, q, run, def, now, model.RunCancelling, model.RunCancelled)
	}

	next, err := q.GetStepByIndex(ctx, run.ID, step.StepIndex+1, false)
	if err != nil {
		run.Status = model.RunCompleted
		if err := q.UpdateRun(ctx, run); err != nil {
			return err
		}
		return q.InsertEvent(ctx, model.Event{RunID: run.ID, Type: model.EventRunCompleted, CreatedAt: now})
	}

	if err := q.UpdateRun(ctx, run); err != nil {
		return err
	}
	next.Status = model.StepReady
	next.ScheduledAt = now.Add(time.Duration(next.DelaySeconds) * time.Second)
	if err := q.UpdateStep(ctx, next); err != nil {
		return err
	}
	return q.InsertEvent(ctx, model.Event{RunID: run.ID, Type: model.EventStepReady, StepName: next.Name, CreatedAt: now})
}

func (e *Engine) advanceCompensation(ctx context.Context, q *store.Queries, run model.Run, step model.Step, now time.Time) error {
	next, err := q.GetStepByIndex(ctx, run.ID, step.StepIndex+1, true)
	if err != nil {
		if run.Status == model.RunCancelling {
			run.Status = model.RunCancelled
		} else {
			run.Status = model.RunCompensated
		}
		if err := q.UpdateRun(ctx, run); err != nil {
			return err
		}
		evtType := model.EventRunCompensated
		if run.Status == model.RunCancelled {
			evtType = model.EventRunCancelled
		}
		return q.InsertEvent(ctx, model.Event{RunID: run.ID, Type: evtType, CreatedAt: now})
	}

	next.Status = model.StepReady
	next.ScheduledAt = now
	if err := q.UpdateStep(ctx, next); err != nil {
		return err
	}
	return q.InsertEvent(ctx, model.Event{RunID: run.ID, Type: model.EventCompensationStepReady, StepName: next.Name, CreatedAt: now})
}

// beginCompensation materializes compensation steps for every completed
// forward step that declares one, in reverse order. If there's nothing to
// compensate, the run goes straight to doneStatusIfNoWork; otherwise it
// moves to inProgressStatus and the first compensation step is marked
// ready.
func (e *Engine) beginCompensation(ctx context.Context, q *store.Queries, run model.Run, def model.WorkflowDef, now time.Time, inProgressStatus, doneStatusIfNoWork model.RunStatus) error {
	steps, err := q.ListSteps(ctx, run.ID)
	if err != nil {
		return err
	}

	var completedDesc []model.Step
	for i := len(steps) - 1; i >= 0; i-- {
		s := steps[i]
		if !s.IsCompensation && s.Status == model.StepCompleted {
			completedDesc = append(completedDesc, s)
		}
	}

	idx := 0
	for _, fs := range completedDesc {
		cd, ok := def.CompensationFor(fs.Name)
		if !ok {
			continue
		}
		cs := stepFromDef(run.ID, cd, idx, true, now)
		if idx == 0 {
			cs.Status = model.StepReady
		}
		if err := q.InsertStep(ctx, cs); err != nil {
			return err
		}
		idx++
	}

	if idx == 0 {
		run.Status = doneStatusIfNoWork
		if err := q.UpdateRun(ctx, run); err != nil {
			return err
		}
		return q.InsertEvent(ctx, model.Event{RunID: run.ID, Type: model.EventRunCompensated, CreatedAt: now, Detail: mustJSON(map[string]any{"status": string(doneStatusIfNoWork), "compensations": 0})})
	}

	run.Status = inProgressStatus
	if err := q.UpdateRun(ctx, run); err != nil {
		return err
	}
	if err := q.InsertEvent(ctx, model.Event{RunID: run.ID, Type: model.EventCompensationTriggered, CreatedAt: now, Detail: mustJSON(map[string]any{"steps": idx})}); err != nil {
		return err
	}
	first, err := q.GetStepByIndex(ctx, run.ID, 0, true)
	if err != nil {
		return err
	}
	return q.InsertEvent(ctx, model.Event{RunID: run.ID, Type: model.EventCompensationStepReady, StepName: first.Name, CreatedAt: now})
}

// FailStep records a step failure reported by a worker and applies retry
// policy: schedule another attempt with backoff, or fail permanently and
// (for forward steps) trigger compensation.
func (e *Engine) FailStep(ctx context.Context, stepID, leaseToken, reason string) error {
	return e.store.Atomic(ctx, func(q *store.Queries) error {
		step, err := q.GetStep(ctx, stepID)
		if err != nil {
			return ErrStepNotFound
		}
		if step.Status != model.StepLeased || step.LeaseToken != leaseToken {
			return ErrLeaseMismatch
		}
		run, err := q.GetRun(ctx, step.RunID)
		if err != nil {
			return ErrRunNotFound
		}
		return e.applyFailure(ctx, q, run, step, reason, e.now())
	})
}

// applyFailure is the single path every kind of failure goes through:
// an explicit report from a worker, a step timeout, or a reclaimed lease
// standing in for a worker that never reported back at all.
func (e *Engine) applyFailure(ctx context.Context, q *store.Queries, run model.Run, step model.Step, reason string, now time.Time) error {
	step.Error = reason
	step.LeaseToken = ""
	step.LeaseOwner = ""
	step.LeaseExpiresAt = nil

	cancelling := !step.IsCompensation && run.Status == model.RunCancelling
	retry := step.Attempt < step.MaxAttempts && !cancelling

	if retry {
		step.Status = model.StepReady
		step.ScheduledAt = now.Add(nextBackoff(step))
		if err := q.UpdateStep(ctx, step); err != nil {
			return err
		}
		return q.InsertEvent(ctx, model.Event{RunID: run.ID, Type: model.EventStepRetryScheduled, StepName: step.Name, CreatedAt: now,
			Detail: mustJSON(map[string]any{"attempt": step.Attempt, "max_attempts": step.MaxAttempts, "reason": reason, "retry_at": step.ScheduledAt})})
	}

	step.Status = model.StepFailed
	if err := q.UpdateStep(ctx, step); err != nil {
		return err
	}
	failEvt := model.EventStepFailed
	if step.IsCompensation {
		failEvt = model.EventCompensationFailed
	}
	if err := q.InsertEvent(ctx, model.Event{RunID: run.ID, Type: failEvt, StepName: step.Name, CreatedAt: now,
		Detail: mustJSON(map[string]any{"attempt": step.Attempt, "reason": reason, "permanent": true})}); err != nil {
		return err
	}

	if step.IsCompensation {
		run.Status = model.RunFailed
		run.Error = fmt.Sprintf("compensation step %q failed permanently: %s", step.Name, reason)
		if err := q.UpdateRun(ctx, run); err != nil {
			return err
		}
		return q.InsertEvent(ctx, model.Event{RunID: run.ID, Type: model.EventRunFailed, CreatedAt: now, Detail: mustJSON(map[string]any{"error": run.Error})})
	}

	run.Error = reason
	def, err := q.GetWorkflowDefVersion(ctx, run.WorkflowName, run.WorkflowVersion)
	if err != nil {
		return err
	}
	inProgress, doneIfNoWork := model.RunCompensating, model.RunFailed
	if run.Status == model.RunCancelling {
		inProgress, doneIfNoWork = model.RunCancelling, model.RunCancelled
	}
	return e.beginCompensation(ctx, q, run, def, now, inProgress, doneIfNoWork)
}

// CancelRun requests cancellation. Steps not yet claimed are cancelled
// immediately; a step already in flight is left to finish or fail on its
// own, at which point the run moves into compensation instead of
// continuing forward.
func (e *Engine) CancelRun(ctx context.Context, runID string) error {
	return e.store.Atomic(ctx, func(q *store.Queries) error {
		run, err := q.GetRun(ctx, runID)
		if err != nil {
			return ErrRunNotFound
		}
		if isTerminal(run.Status) {
			return ErrRunAlreadyTerminal
		}
		now := e.now()

		if run.Status == model.RunQueued {
			run.Status = model.RunCancelled
			if err := q.UpdateRun(ctx, run); err != nil {
				return err
			}
			return q.InsertEvent(ctx, model.Event{RunID: run.ID, Type: model.EventRunCancelled, CreatedAt: now})
		}

		steps, err := q.ListSteps(ctx, runID)
		if err != nil {
			return err
		}
		hasLeasedForward := false
		for _, s := range steps {
			if s.IsCompensation {
				continue
			}
			switch s.Status {
			case model.StepPending, model.StepReady:
				s.Status = model.StepCancelled
				if err := q.UpdateStep(ctx, s); err != nil {
					return err
				}
			case model.StepLeased:
				hasLeasedForward = true
			}
		}

		run.Status = model.RunCancelling
		if err := q.UpdateRun(ctx, run); err != nil {
			return err
		}
		if err := q.InsertEvent(ctx, model.Event{RunID: run.ID, Type: model.EventRunCancelRequested, CreatedAt: now}); err != nil {
			return err
		}
		if hasLeasedForward {
			return nil
		}

		def, err := q.GetWorkflowDefVersion(ctx, run.WorkflowName, run.WorkflowVersion)
		if err != nil {
			return err
		}
		return e.beginCompensation(ctx, q, run, def, now, model.RunCancelling, model.RunCancelled)
	})
}
