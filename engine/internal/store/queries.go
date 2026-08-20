package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/stan-ley-tech/flowforge/engine/internal/model"
)

func fmtTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

func parseTime(s string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, s)
	return t
}

func fmtNullTime(t *time.Time) sql.NullString {
	if t == nil {
		return sql.NullString{}
	}
	return sql.NullString{String: fmtTime(*t), Valid: true}
}

func parseNullTime(ns sql.NullString) *time.Time {
	if !ns.Valid || ns.String == "" {
		return nil
	}
	t := parseTime(ns.String)
	return &t
}

// --- workflow definitions ---

func (q *Queries) UpsertWorkflowDef(ctx context.Context, def model.WorkflowDef) error {
	stepsJSON, err := json.Marshal(def.Steps)
	if err != nil {
		return err
	}
	compJSON, err := json.Marshal(def.Compensations)
	if err != nil {
		return err
	}
	if def.CreatedAt.IsZero() {
		def.CreatedAt = time.Now()
	}
	_, err = q.db.ExecContext(ctx, `
		INSERT INTO workflow_defs (name, version, max_concurrent_runs, steps_json, compensations_json, created_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(name, version) DO UPDATE SET
			max_concurrent_runs = excluded.max_concurrent_runs,
			steps_json = excluded.steps_json,
			compensations_json = excluded.compensations_json`,
		def.Name, def.Version, def.MaxConcurrentRuns, string(stepsJSON), string(compJSON), fmtTime(def.CreatedAt))
	if err != nil {
		return fmt.Errorf("upsert workflow def: %w", err)
	}

	_, err = q.db.ExecContext(ctx, `
		INSERT INTO workflow_latest (name, version) VALUES (?, ?)
		ON CONFLICT(name) DO UPDATE SET version = excluded.version
		WHERE excluded.version >= workflow_latest.version`,
		def.Name, def.Version)
	if err != nil {
		return fmt.Errorf("set latest workflow version: %w", err)
	}
	return nil
}

func (q *Queries) scanWorkflowDef(row *sql.Row) (model.WorkflowDef, error) {
	var def model.WorkflowDef
	var stepsJSON, compJSON, createdAt string
	if err := row.Scan(&def.Name, &def.Version, &def.MaxConcurrentRuns, &stepsJSON, &compJSON, &createdAt); err != nil {
		return model.WorkflowDef{}, err
	}
	if err := json.Unmarshal([]byte(stepsJSON), &def.Steps); err != nil {
		return model.WorkflowDef{}, err
	}
	if err := json.Unmarshal([]byte(compJSON), &def.Compensations); err != nil {
		return model.WorkflowDef{}, err
	}
	def.CreatedAt = parseTime(createdAt)
	return def, nil
}

func (q *Queries) GetWorkflowDefVersion(ctx context.Context, name string, version int) (model.WorkflowDef, error) {
	row := q.db.QueryRowContext(ctx, `
		SELECT name, version, max_concurrent_runs, steps_json, compensations_json, created_at
		FROM workflow_defs WHERE name = ? AND version = ?`, name, version)
	return q.scanWorkflowDef(row)
}

func (q *Queries) GetLatestWorkflowDef(ctx context.Context, name string) (model.WorkflowDef, error) {
	row := q.db.QueryRowContext(ctx, `
		SELECT d.name, d.version, d.max_concurrent_runs, d.steps_json, d.compensations_json, d.created_at
		FROM workflow_defs d
		JOIN workflow_latest l ON l.name = d.name AND l.version = d.version
		WHERE d.name = ?`, name)
	return q.scanWorkflowDef(row)
}

// --- runs ---

func (q *Queries) InsertRun(ctx context.Context, r model.Run) error {
	_, err := q.db.ExecContext(ctx, `
		INSERT INTO runs (id, workflow_name, workflow_version, status, input, context, error, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		r.ID, r.WorkflowName, r.WorkflowVersion, string(r.Status), string(r.Input), string(r.Context), r.Error,
		fmtTime(r.CreatedAt), fmtTime(r.UpdatedAt))
	return err
}

func (q *Queries) scanRun(row *sql.Row) (model.Run, error) {
	var r model.Run
	var status, input, ctxJSON, createdAt, updatedAt string
	if err := row.Scan(&r.ID, &r.WorkflowName, &r.WorkflowVersion, &status, &input, &ctxJSON, &r.Error, &createdAt, &updatedAt); err != nil {
		return model.Run{}, err
	}
	r.Status = model.RunStatus(status)
	r.Input = json.RawMessage(input)
	r.Context = json.RawMessage(ctxJSON)
	r.CreatedAt = parseTime(createdAt)
	r.UpdatedAt = parseTime(updatedAt)
	return r, nil
}

func (q *Queries) GetRun(ctx context.Context, id string) (model.Run, error) {
	row := q.db.QueryRowContext(ctx, `
		SELECT id, workflow_name, workflow_version, status, input, context, error, created_at, updated_at
		FROM runs WHERE id = ?`, id)
	return q.scanRun(row)
}

func (q *Queries) UpdateRun(ctx context.Context, r model.Run) error {
	r.UpdatedAt = time.Now()
	_, err := q.db.ExecContext(ctx, `
		UPDATE runs SET status = ?, context = ?, error = ?, updated_at = ?
		WHERE id = ?`,
		string(r.Status), string(r.Context), r.Error, fmtTime(r.UpdatedAt), r.ID)
	return err
}

func (q *Queries) ListRuns(ctx context.Context, workflowName string, limit int) ([]model.Run, error) {
	var rows *sql.Rows
	var err error
	if workflowName == "" {
		rows, err = q.db.QueryContext(ctx, `
			SELECT id, workflow_name, workflow_version, status, input, context, error, created_at, updated_at
			FROM runs ORDER BY created_at DESC LIMIT ?`, limit)
	} else {
		rows, err = q.db.QueryContext(ctx, `
			SELECT id, workflow_name, workflow_version, status, input, context, error, created_at, updated_at
			FROM runs WHERE workflow_name = ? ORDER BY created_at DESC LIMIT ?`, workflowName, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []model.Run
	for rows.Next() {
		var r model.Run
		var status, input, ctxJSON, createdAt, updatedAt string
		if err := rows.Scan(&r.ID, &r.WorkflowName, &r.WorkflowVersion, &status, &input, &ctxJSON, &r.Error, &createdAt, &updatedAt); err != nil {
			return nil, err
		}
		r.Status = model.RunStatus(status)
		r.Input = json.RawMessage(input)
		r.Context = json.RawMessage(ctxJSON)
		r.CreatedAt = parseTime(createdAt)
		r.UpdatedAt = parseTime(updatedAt)
		out = append(out, r)
	}
	return out, rows.Err()
}

func (q *Queries) CountRunsByStatus(ctx context.Context, workflowName string, statuses ...model.RunStatus) (int, error) {
	if len(statuses) == 0 {
		return 0, nil
	}
	query := `SELECT COUNT(*) FROM runs WHERE workflow_name = ? AND status IN (`
	args := []any{workflowName}
	for i, s := range statuses {
		if i > 0 {
			query += ","
		}
		query += "?"
		args = append(args, string(s))
	}
	query += ")"
	var n int
	err := q.db.QueryRowContext(ctx, query, args...).Scan(&n)
	return n, err
}

func (q *Queries) ListQueuedRuns(ctx context.Context, workflowName string, limit int) ([]model.Run, error) {
	rows, err := q.db.QueryContext(ctx, `
		SELECT id, workflow_name, workflow_version, status, input, context, error, created_at, updated_at
		FROM runs WHERE workflow_name = ? AND status = ? ORDER BY created_at ASC LIMIT ?`,
		workflowName, string(model.RunQueued), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []model.Run
	for rows.Next() {
		var r model.Run
		var status, input, ctxJSON, createdAt, updatedAt string
		if err := rows.Scan(&r.ID, &r.WorkflowName, &r.WorkflowVersion, &status, &input, &ctxJSON, &r.Error, &createdAt, &updatedAt); err != nil {
			return nil, err
		}
		r.Status = model.RunStatus(status)
		r.Input = json.RawMessage(input)
		r.Context = json.RawMessage(ctxJSON)
		r.CreatedAt = parseTime(createdAt)
		r.UpdatedAt = parseTime(updatedAt)
		out = append(out, r)
	}
	return out, rows.Err()
}

// --- steps ---

func (q *Queries) InsertStep(ctx context.Context, s model.Step) error {
	_, err := q.db.ExecContext(ctx, `
		INSERT INTO steps (
			id, run_id, name, step_index, is_compensation, compensation_of, status, attempt,
			max_attempts, backoff_base_ms, backoff_multiplier, backoff_max_ms, jitter_fraction,
			timeout_seconds, scheduled_at, lease_token, lease_owner, lease_expires_at, started_at,
			result, error, idempotency_key, created_at, updated_at
		) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		s.ID, s.RunID, s.Name, s.StepIndex, boolToInt(s.IsCompensation), s.CompensationOf, string(s.Status), s.Attempt,
		s.MaxAttempts, s.BackoffBaseMs, s.BackoffMultiplier, s.BackoffMaxMs, s.JitterFraction,
		s.TimeoutSeconds, fmtTime(s.ScheduledAt), s.LeaseToken, s.LeaseOwner, fmtNullTime(s.LeaseExpiresAt), fmtNullTime(s.StartedAt),
		string(s.Result), s.Error, s.IdempotencyKey, fmtTime(s.CreatedAt), fmtTime(s.UpdatedAt))
	return err
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func scanStepRow(scan func(dest ...any) error) (model.Step, error) {
	var s model.Step
	var isComp int
	var status, scheduledAt, createdAt, updatedAt string
	var leaseExpiresAt, startedAt sql.NullString
	var result string
	err := scan(
		&s.ID, &s.RunID, &s.Name, &s.StepIndex, &isComp, &s.CompensationOf, &status, &s.Attempt,
		&s.MaxAttempts, &s.BackoffBaseMs, &s.BackoffMultiplier, &s.BackoffMaxMs, &s.JitterFraction,
		&s.TimeoutSeconds, &scheduledAt, &s.LeaseToken, &s.LeaseOwner, &leaseExpiresAt, &startedAt,
		&result, &s.Error, &s.IdempotencyKey, &createdAt, &updatedAt,
	)
	if err != nil {
		return model.Step{}, err
	}
	s.IsCompensation = isComp != 0
	s.Status = model.StepStatus(status)
	s.ScheduledAt = parseTime(scheduledAt)
	s.LeaseExpiresAt = parseNullTime(leaseExpiresAt)
	s.StartedAt = parseNullTime(startedAt)
	s.Result = json.RawMessage(result)
	s.CreatedAt = parseTime(createdAt)
	s.UpdatedAt = parseTime(updatedAt)
	return s, nil
}

const stepColumns = `id, run_id, name, step_index, is_compensation, compensation_of, status, attempt,
	max_attempts, backoff_base_ms, backoff_multiplier, backoff_max_ms, jitter_fraction,
	timeout_seconds, scheduled_at, lease_token, lease_owner, lease_expires_at, started_at,
	result, error, idempotency_key, created_at, updated_at`

func (q *Queries) GetStep(ctx context.Context, id string) (model.Step, error) {
	row := q.db.QueryRowContext(ctx, `SELECT `+stepColumns+` FROM steps WHERE id = ?`, id)
	return scanStepRow(row.Scan)
}

func (q *Queries) GetStepByName(ctx context.Context, runID, name string, isCompensation bool) (model.Step, error) {
	row := q.db.QueryRowContext(ctx, `SELECT `+stepColumns+` FROM steps WHERE run_id = ? AND name = ? AND is_compensation = ?`,
		runID, name, boolToInt(isCompensation))
	return scanStepRow(row.Scan)
}

func (q *Queries) GetStepByIndex(ctx context.Context, runID string, index int, isCompensation bool) (model.Step, error) {
	row := q.db.QueryRowContext(ctx, `SELECT `+stepColumns+` FROM steps WHERE run_id = ? AND step_index = ? AND is_compensation = ?`,
		runID, index, boolToInt(isCompensation))
	return scanStepRow(row.Scan)
}

func (q *Queries) ListSteps(ctx context.Context, runID string) ([]model.Step, error) {
	rows, err := q.db.QueryContext(ctx, `SELECT `+stepColumns+` FROM steps WHERE run_id = ? ORDER BY is_compensation ASC, step_index ASC`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []model.Step
	for rows.Next() {
		s, err := scanStepRow(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func (q *Queries) UpdateStep(ctx context.Context, s model.Step) error {
	s.UpdatedAt = time.Now()
	_, err := q.db.ExecContext(ctx, `
		UPDATE steps SET
			status = ?, attempt = ?, scheduled_at = ?, lease_token = ?, lease_owner = ?,
			lease_expires_at = ?, started_at = ?, result = ?, error = ?, updated_at = ?
		WHERE id = ?`,
		string(s.Status), s.Attempt, fmtTime(s.ScheduledAt), s.LeaseToken, s.LeaseOwner,
		fmtNullTime(s.LeaseExpiresAt), fmtNullTime(s.StartedAt), string(s.Result), s.Error, fmtTime(s.UpdatedAt), s.ID)
	return err
}

// ClaimReadyStep atomically picks the single oldest-scheduled READY step
// whose scheduled_at has passed, leases it to workerID, and returns it.
// The UPDATE...RETURNING form makes selection and claim one statement, so
// two workers polling at the same time can never be handed the same step.
func (q *Queries) ClaimReadyStep(ctx context.Context, workerID string, leaseDuration time.Duration, now time.Time) (*model.Step, error) {
	leaseExpires := now.Add(leaseDuration)
	row := q.db.QueryRowContext(ctx, `
		UPDATE steps SET
			status = ?, attempt = attempt + 1, lease_token = lower(hex(randomblob(16))),
			lease_owner = ?, lease_expires_at = ?, started_at = ?, updated_at = ?
		WHERE id = (
			SELECT id FROM steps
			WHERE status = ? AND scheduled_at <= ?
			ORDER BY scheduled_at ASC
			LIMIT 1
		)
		RETURNING `+stepColumns,
		string(model.StepLeased), workerID, fmtTime(leaseExpires), fmtTime(now), fmtTime(now),
		string(model.StepReady), fmtTime(now))

	s, err := scanStepRow(row.Scan)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &s, nil
}

// FindExpiredLeaseSteps returns steps whose lease has passed without a
// heartbeat or completion - the set the scheduler reclaims on each tick.
func (q *Queries) FindExpiredLeaseSteps(ctx context.Context, now time.Time) ([]model.Step, error) {
	rows, err := q.db.QueryContext(ctx, `SELECT `+stepColumns+` FROM steps WHERE status = ? AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?`,
		string(model.StepLeased), fmtTime(now))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []model.Step
	for rows.Next() {
		s, err := scanStepRow(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// --- events ---

func (q *Queries) InsertEvent(ctx context.Context, e model.Event) error {
	var seq int64
	err := q.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq), 0) + 1 FROM events WHERE run_id = ?`, e.RunID).Scan(&seq)
	if err != nil {
		return err
	}
	if e.Detail == nil {
		e.Detail = json.RawMessage("{}")
	}
	if e.CreatedAt.IsZero() {
		e.CreatedAt = time.Now()
	}
	_, err = q.db.ExecContext(ctx, `
		INSERT INTO events (run_id, seq, type, step_name, detail, created_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		e.RunID, seq, e.Type, e.StepName, string(e.Detail), fmtTime(e.CreatedAt))
	return err
}

func (q *Queries) ListEvents(ctx context.Context, runID string) ([]model.Event, error) {
	rows, err := q.db.QueryContext(ctx, `
		SELECT id, run_id, seq, type, step_name, detail, created_at
		FROM events WHERE run_id = ? ORDER BY seq ASC`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []model.Event
	for rows.Next() {
		var e model.Event
		var detail, createdAt string
		if err := rows.Scan(&e.ID, &e.RunID, &e.Seq, &e.Type, &e.StepName, &detail, &createdAt); err != nil {
			return nil, err
		}
		e.Detail = json.RawMessage(detail)
		e.CreatedAt = parseTime(createdAt)
		out = append(out, e)
	}
	return out, rows.Err()
}
