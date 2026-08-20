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
	MaxAttempts       int     `json:"max_attempts"`
	BackoffBaseMs     int64   `json:"backoff_base_ms"`
	BackoffMultiplier float64 `json:"backoff_multiplier"`
	BackoffMaxMs      int64   `json:"backoff_max_ms"`
	JitterFraction    float64 `json:"jitter_fraction"`
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
	Name           string      `json:"name"`
	TimeoutSeconds int         `json:"timeout_seconds"`
	DelaySeconds   int         `json:"delay_seconds"`
	Retry          RetryPolicy `json:"retry"`
	CompensationOf string      `json:"compensation_of,omitempty"`
}

type WorkflowDef struct {
	Name              string    `json:"name"`
	Version           int       `json:"version"`
	Steps             []StepDef `json:"steps"`
	Compensations     []StepDef `json:"compensations,omitempty"`
	MaxConcurrentRuns int       `json:"max_concurrent_runs"`
	CreatedAt         time.Time `json:"created_at"`
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
	ID              string          `json:"id"`
	WorkflowName    string          `json:"workflow_name"`
	WorkflowVersion int             `json:"workflow_version"`
	Status          RunStatus       `json:"status"`
	Input           json.RawMessage `json:"input"`
	Context         json.RawMessage `json:"context"`
	Error           string          `json:"error,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
}

type Step struct {
	ID                string          `json:"id"`
	RunID             string          `json:"run_id"`
	Name              string          `json:"name"`
	StepIndex         int             `json:"step_index"`
	IsCompensation    bool            `json:"is_compensation"`
	CompensationOf    string          `json:"compensation_of,omitempty"`
	DelaySeconds      int             `json:"delay_seconds"`
	Status            StepStatus      `json:"status"`
	Attempt           int             `json:"attempt"`
	MaxAttempts       int             `json:"max_attempts"`
	BackoffBaseMs     int64           `json:"backoff_base_ms"`
	BackoffMultiplier float64         `json:"backoff_multiplier"`
	BackoffMaxMs      int64           `json:"backoff_max_ms"`
	JitterFraction    float64         `json:"jitter_fraction"`
	TimeoutSeconds    int             `json:"timeout_seconds"`
	ScheduledAt       time.Time       `json:"scheduled_at"`
	LeaseToken        string          `json:"-"`
	LeaseOwner        string          `json:"lease_owner,omitempty"`
	LeaseExpiresAt    *time.Time      `json:"lease_expires_at,omitempty"`
	StartedAt         *time.Time      `json:"started_at,omitempty"`
	Result            json.RawMessage `json:"result,omitempty"`
	Error             string          `json:"error,omitempty"`
	IdempotencyKey    string          `json:"idempotency_key"`
	CreatedAt         time.Time       `json:"created_at"`
	UpdatedAt         time.Time       `json:"updated_at"`
}

type Event struct {
	ID        int64           `json:"id"`
	RunID     string          `json:"run_id"`
	Seq       int64           `json:"seq"`
	Type      string          `json:"type"`
	StepName  string          `json:"step_name,omitempty"`
	Detail    json.RawMessage `json:"detail,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
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
