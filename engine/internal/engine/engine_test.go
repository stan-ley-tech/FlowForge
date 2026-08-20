package engine

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stan-ley-tech/flowforge/engine/internal/model"
	"github.com/stan-ley-tech/flowforge/engine/internal/store"
)

func newTestEngine(t *testing.T, clock *time.Time) (*Engine, *store.Store) {
	t.Helper()
	s, err := store.Open(":memory:")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	e := New(s, WithLeaseDuration(2*time.Second), WithClock(func() time.Time { return *clock }))
	return e, s
}

func mustStep(name string, retry model.RetryPolicy, compensationOf string) model.StepDef {
	return model.StepDef{Name: name, TimeoutSeconds: 60, Retry: retry, CompensationOf: compensationOf}
}

// TestCrashAndResume is the scenario the whole project is built around:
// a worker claims a step and disappears without completing or failing
// it. Once its lease passes, the scheduler must put the step back up for
// grabs without re-running anything already completed, and a later poll
// must be able to pick it back up and finish the run.
func TestCrashAndResume(t *testing.T) {
	now := time.Now()
	clock := &now
	e, s := newTestEngine(t, clock)
	ctx := context.Background()

	def := model.WorkflowDef{
		Name: "demo",
		Steps: []model.StepDef{
			mustStep("step1", model.RetryPolicy{MaxAttempts: 2, BackoffBaseMs: 10, BackoffMultiplier: 1, BackoffMaxMs: 100}, ""),
			mustStep("step2", model.RetryPolicy{MaxAttempts: 3, BackoffBaseMs: 10, BackoffMultiplier: 1, BackoffMaxMs: 100}, ""),
			mustStep("step3", model.RetryPolicy{MaxAttempts: 2, BackoffBaseMs: 10, BackoffMultiplier: 1, BackoffMaxMs: 100}, ""),
		},
	}
	if _, err := e.RegisterWorkflow(ctx, def); err != nil {
		t.Fatalf("register workflow: %v", err)
	}

	run, err := e.StartRun(ctx, "demo", json.RawMessage(`{"order_id":42}`))
	if err != nil {
		t.Fatalf("start run: %v", err)
	}

	t1, err := e.PollTask(ctx, "demo", "worker-a")
	if err != nil || t1 == nil || t1.StepName != "step1" {
		t.Fatalf("expected step1, got %+v err=%v", t1, err)
	}
	if err := e.CompleteStep(ctx, t1.StepID, t1.LeaseToken, json.RawMessage(`{"charged":true}`)); err != nil {
		t.Fatalf("complete step1: %v", err)
	}

	t2, err := e.PollTask(ctx, "demo", "worker-a")
	if err != nil || t2 == nil || t2.StepName != "step2" || t2.Attempt != 1 {
		t.Fatalf("expected step2 attempt 1, got %+v err=%v", t2, err)
	}

	// worker-a is killed here: no heartbeat, no completion, no failure.
	*clock = clock.Add(3 * time.Second) // past the 2s lease
	sched := NewScheduler(e, s, time.Second)
	if err := sched.reclaimExpiredLeases(ctx); err != nil {
		t.Fatalf("reclaim expired leases: %v", err)
	}

	steps, err := e.ListSteps(ctx, run.ID)
	if err != nil {
		t.Fatalf("list steps: %v", err)
	}
	byName := map[string]model.Step{}
	for _, s := range steps {
		byName[s.Name] = s
	}
	if byName["step1"].Status != model.StepCompleted {
		t.Fatalf("step1 should still be completed, not re-run, got %s", byName["step1"].Status)
	}
	if byName["step2"].Status != model.StepReady {
		t.Fatalf("step2 should be back to READY after lease reclaim, got %s", byName["step2"].Status)
	}

	// The retry backoff computed on reclaim hasn't elapsed yet.
	*clock = clock.Add(time.Second)

	// A different worker (standing in for the restarted infrastructure)
	// picks the reclaimed step back up.
	t2b, err := e.PollTask(ctx, "demo", "worker-b")
	if err != nil || t2b == nil || t2b.StepName != "step2" || t2b.Attempt != 2 {
		t.Fatalf("expected step2 attempt 2 on worker-b, got %+v err=%v", t2b, err)
	}
	if err := e.CompleteStep(ctx, t2b.StepID, t2b.LeaseToken, json.RawMessage(`{"shipped":true}`)); err != nil {
		t.Fatalf("complete step2: %v", err)
	}

	t3, err := e.PollTask(ctx, "demo", "worker-b")
	if err != nil || t3 == nil || t3.StepName != "step3" {
		t.Fatalf("expected step3, got %+v err=%v", t3, err)
	}
	if err := e.CompleteStep(ctx, t3.StepID, t3.LeaseToken, json.RawMessage(`{"done":true}`)); err != nil {
		t.Fatalf("complete step3: %v", err)
	}

	finalRun, err := e.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if finalRun.Status != model.RunCompleted {
		t.Fatalf("expected run COMPLETED, got %s", finalRun.Status)
	}

	history, err := e.ListHistory(ctx, run.ID)
	if err != nil {
		t.Fatalf("list history: %v", err)
	}
	sawReclaim := false
	for _, evt := range history {
		if evt.Type == model.EventLeaseReclaimed {
			sawReclaim = true
		}
	}
	if !sawReclaim {
		t.Fatalf("expected a LEASE_RECLAIMED event in history, got %d events", len(history))
	}
}

// TestPermanentFailureTriggersCompensation checks that exhausting a
// step's retries rolls back completed steps that declared a
// compensation, in reverse order, and that the run lands in COMPENSATED
// once the rollback itself finishes.
func TestPermanentFailureTriggersCompensation(t *testing.T) {
	now := time.Now()
	clock := &now
	e, _ := newTestEngine(t, clock)
	ctx := context.Background()

	def := model.WorkflowDef{
		Name: "billing",
		Steps: []model.StepDef{
			mustStep("charge", model.RetryPolicy{MaxAttempts: 1, BackoffBaseMs: 10, BackoffMultiplier: 1, BackoffMaxMs: 100}, ""),
			mustStep("ship", model.RetryPolicy{MaxAttempts: 1, BackoffBaseMs: 10, BackoffMultiplier: 1, BackoffMaxMs: 100}, ""),
		},
		Compensations: []model.StepDef{
			mustStep("refund", model.RetryPolicy{MaxAttempts: 1, BackoffBaseMs: 10, BackoffMultiplier: 1, BackoffMaxMs: 100}, "charge"),
		},
	}
	if _, err := e.RegisterWorkflow(ctx, def); err != nil {
		t.Fatalf("register workflow: %v", err)
	}

	run, err := e.StartRun(ctx, "billing", json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("start run: %v", err)
	}

	charge, err := e.PollTask(ctx, "billing", "worker-a")
	if err != nil || charge == nil || charge.StepName != "charge" {
		t.Fatalf("expected charge, got %+v err=%v", charge, err)
	}
	if err := e.CompleteStep(ctx, charge.StepID, charge.LeaseToken, json.RawMessage(`{"charged":true}`)); err != nil {
		t.Fatalf("complete charge: %v", err)
	}

	ship, err := e.PollTask(ctx, "billing", "worker-a")
	if err != nil || ship == nil || ship.StepName != "ship" {
		t.Fatalf("expected ship, got %+v err=%v", ship, err)
	}
	if err := e.FailStep(ctx, ship.StepID, ship.LeaseToken, "warehouse unreachable"); err != nil {
		t.Fatalf("fail ship: %v", err)
	}

	mid, err := e.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if mid.Status != model.RunCompensating {
		t.Fatalf("expected run COMPENSATING after permanent failure, got %s", mid.Status)
	}

	refund, err := e.PollTask(ctx, "billing", "worker-a")
	if err != nil || refund == nil || !refund.IsCompensation || refund.StepName != "refund" {
		t.Fatalf("expected refund compensation task, got %+v err=%v", refund, err)
	}
	if err := e.CompleteStep(ctx, refund.StepID, refund.LeaseToken, json.RawMessage(`{"refunded":true}`)); err != nil {
		t.Fatalf("complete refund: %v", err)
	}

	final, err := e.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if final.Status != model.RunCompensated {
		t.Fatalf("expected run COMPENSATED, got %s", final.Status)
	}
}
