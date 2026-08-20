package api

import (
	"encoding/json"

	"github.com/stan-ley-tech/flowforge/engine/internal/engine"
	"github.com/stan-ley-tech/flowforge/engine/internal/model"
)

type startRunRequest struct {
	Input json.RawMessage `json:"input"`
}

type pollRequest struct {
	Workflow string `json:"workflow"`
	WorkerID string `json:"worker_id"`
}

type taskResponse struct {
	StepID         string          `json:"step_id"`
	RunID          string          `json:"run_id"`
	StepName       string          `json:"step_name"`
	IsCompensation bool            `json:"is_compensation"`
	CompensationOf string          `json:"compensation_of,omitempty"`
	Attempt        int             `json:"attempt"`
	MaxAttempts    int             `json:"max_attempts"`
	TimeoutSeconds int             `json:"timeout_seconds"`
	IdempotencyKey string          `json:"idempotency_key"`
	LeaseToken     string          `json:"lease_token"`
	Input          json.RawMessage `json:"input"`
	Context        json.RawMessage `json:"context"`
}

func taskToResponse(t *engine.Task) taskResponse {
	return taskResponse{
		StepID: t.StepID, RunID: t.RunID, StepName: t.StepName, IsCompensation: t.IsCompensation,
		CompensationOf: t.CompensationOf, Attempt: t.Attempt, MaxAttempts: t.MaxAttempts,
		TimeoutSeconds: t.TimeoutSeconds, IdempotencyKey: t.IdempotencyKey, LeaseToken: t.LeaseToken,
		Input: t.Input, Context: t.Context,
	}
}

type heartbeatRequest struct {
	LeaseToken string `json:"lease_token"`
}

type completeRequest struct {
	LeaseToken string          `json:"lease_token"`
	Result     json.RawMessage `json:"result"`
}

type failRequest struct {
	LeaseToken string `json:"lease_token"`
	Reason     string `json:"reason"`
}

type runDetail struct {
	model.Run
	Steps []model.Step `json:"steps"`
}

type errorResponse struct {
	Error string `json:"error"`
}
