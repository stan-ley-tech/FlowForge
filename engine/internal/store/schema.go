package store

const schema = `
CREATE TABLE IF NOT EXISTS workflow_defs (
	name                  TEXT NOT NULL,
	version               INTEGER NOT NULL,
	max_concurrent_runs   INTEGER NOT NULL DEFAULT 0,
	steps_json            TEXT NOT NULL,
	compensations_json    TEXT NOT NULL,
	created_at            TEXT NOT NULL,
	PRIMARY KEY (name, version)
);

CREATE TABLE IF NOT EXISTS workflow_latest (
	name    TEXT PRIMARY KEY,
	version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS runs (
	id               TEXT PRIMARY KEY,
	workflow_name    TEXT NOT NULL,
	workflow_version INTEGER NOT NULL,
	status           TEXT NOT NULL,
	input            TEXT NOT NULL,
	context          TEXT NOT NULL,
	error            TEXT NOT NULL DEFAULT '',
	created_at       TEXT NOT NULL,
	updated_at       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_runs_workflow_status ON runs(workflow_name, status);

CREATE TABLE IF NOT EXISTS steps (
	id                  TEXT PRIMARY KEY,
	run_id              TEXT NOT NULL REFERENCES runs(id),
	name                TEXT NOT NULL,
	step_index          INTEGER NOT NULL,
	is_compensation     INTEGER NOT NULL DEFAULT 0,
	compensation_of     TEXT NOT NULL DEFAULT '',
	delay_seconds       INTEGER NOT NULL DEFAULT 0,
	status              TEXT NOT NULL,
	attempt             INTEGER NOT NULL DEFAULT 0,
	max_attempts        INTEGER NOT NULL,
	backoff_base_ms     INTEGER NOT NULL,
	backoff_multiplier  REAL NOT NULL,
	backoff_max_ms      INTEGER NOT NULL,
	jitter_fraction     REAL NOT NULL,
	timeout_seconds     INTEGER NOT NULL,
	scheduled_at        TEXT NOT NULL,
	lease_token         TEXT NOT NULL DEFAULT '',
	lease_owner         TEXT NOT NULL DEFAULT '',
	lease_expires_at    TEXT,
	started_at          TEXT,
	result              TEXT NOT NULL DEFAULT '',
	error               TEXT NOT NULL DEFAULT '',
	idempotency_key     TEXT NOT NULL DEFAULT '',
	created_at          TEXT NOT NULL,
	updated_at          TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_steps_run ON steps(run_id, step_index);
CREATE INDEX IF NOT EXISTS idx_steps_claimable ON steps(status, scheduled_at);
CREATE INDEX IF NOT EXISTS idx_steps_leased ON steps(status, lease_expires_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_steps_run_name_kind ON steps(run_id, name, is_compensation);

CREATE TABLE IF NOT EXISTS events (
	id         INTEGER PRIMARY KEY AUTOINCREMENT,
	run_id     TEXT NOT NULL,
	seq        INTEGER NOT NULL,
	type       TEXT NOT NULL,
	step_name  TEXT NOT NULL DEFAULT '',
	detail     TEXT NOT NULL DEFAULT '{}',
	created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_run ON events(run_id, seq);
`
