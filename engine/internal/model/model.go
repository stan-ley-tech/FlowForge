// Package model defines the durable state FlowForge persists: workflow
// definitions, runs, steps, and the event log. Nothing in this package
// talks to storage or the network; it's the vocabulary the rest of the
// engine shares.
package model

import (
	"encoding/json"
	"time"
)

type RunStatus string

const (
	RunQueued       RunStatus = "QUEUED"
	RunRunning      RunStatus = "RUNNING"
	RunCompleted    RunStatus = "COMPLETED"
	RunFailed       RunStatus = "FAILED"
	RunCancelling   RunStatus = "CANCELLING"
	RunCancelled    RunStatus = "CANCELLED"
	RunCompensating RunStatus = "COMPENSATING"
	RunCompensated  RunStatus = "COMPENSATED"
)

type StepStatus string

const (
	StepPending     StepStatus = "PENDING"
	StepReady       StepStatus = "READY"
	StepLeased      StepStatus = "LEASED"
	StepCompleted   StepStatus = "COMPLETED"
	StepFailed      StepStatus = "FAILED"
	StepCancelled   StepStatus = "CANCELLED"
	StepCompensated StepStatus = "COMPENSATED"
)

// RetryPolicy controls how a failed step is retried. Backoff for attempt
// n (1-indexed) is BackoffBaseMs * BackoffMultiplier^(n-1), capped at
// BackoffMaxMs, then jittered by +/- JitterFraction.
type RetryPolicy struct {
	MaxAttempts       int
	BackoffBaseMs     int64
	BackoffMultiplier float64
	BackoffMaxMs      int64
	JitterFraction    float64
}

func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:       3,
		BackoffBaseMs:     1000,
		BackoffMultiplier: 2.0,
		BackoffMaxMs:      60_000,
		JitterFraction:    0.2,
	}
}

// StepDef describes one step of a workflow definition: its position,
// timeout, retry policy, and how long to wait after the previous step
// completes before it becomes eligible to run.
type StepDef struct {
	Name           string
	TimeoutSeconds int
	DelaySeconds   int
	Retry          RetryPolicy
	CompensationOf string
}

type WorkflowDef struct {
	Name              string
	Version           int
	Steps             []StepDef
	Compensations     []StepDef
	MaxConcurrentRuns int
	CreatedAt         time.Time
}

func (w WorkflowDef) CompensationFor(stepName string) (StepDef, bool) {
	for _, c := range w.Compensations {
		if c.CompensationOf == stepName {
			return c, true
		}
	}
	return StepDef{}, false
}

type Run struct {
	ID              string
	WorkflowName    string
	WorkflowVersion int
	Status          RunStatus
	Input           json.RawMessage
	Context         json.RawMessage
	Error           string
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

type Step struct {
	ID                string
	RunID             string
	Name              string
	StepIndex         int
	IsCompensation    bool
	CompensationOf    string
	Status            StepStatus
	Attempt           int
	MaxAttempts       int
	BackoffBaseMs     int64
	BackoffMultiplier float64
	BackoffMaxMs      int64
	JitterFraction    float64
	TimeoutSeconds    int
	ScheduledAt       time.Time
	LeaseToken        string
	LeaseOwner        string
	LeaseExpiresAt    *time.Time
	StartedAt         *time.Time
	Result            json.RawMessage
	Error             string
	IdempotencyKey    string
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

type Event struct {
	ID        int64
	RunID     string
	Seq       int64
	Type      string
	StepName  string
	Detail    json.RawMessage
	CreatedAt time.Time
}

const (
	EventRunStarted            = "RUN_STARTED"
	EventRunAdmitted           = "RUN_ADMITTED"
	EventStepReady             = "STEP_READY"
	EventStepClaimed           = "STEP_CLAIMED"
	EventStepHeartbeat         = "STEP_HEARTBEAT"
	EventStepCompleted         = "STEP_COMPLETED"
	EventStepFailed            = "STEP_FAILED"
	EventStepRetryScheduled    = "STEP_RETRY_SCHEDULED"
	EventStepTimedOut          = "STEP_TIMED_OUT"
	EventLeaseReclaimed        = "LEASE_RECLAIMED"
	EventCompensationTriggered = "COMPENSATION_TRIGGERED"
	EventCompensationStepReady = "COMPENSATION_STEP_READY"
	EventCompensationCompleted = "COMPENSATION_STEP_COMPLETED"
	EventCompensationFailed    = "COMPENSATION_STEP_FAILED"
	EventRunCompleted          = "RUN_COMPLETED"
	EventRunFailed             = "RUN_FAILED"
	EventRunCancelRequested    = "RUN_CANCEL_REQUESTED"
	EventRunCancelled          = "RUN_CANCELLED"
	EventRunCompensated        = "RUN_COMPENSATED"
)
