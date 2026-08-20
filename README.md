# FlowForge

FlowForge is a small durable workflow engine. You define a workflow as an
ordered sequence of steps, start a run, and the engine tracks exactly where
that run is at all times - on disk, not in memory. Kill a worker mid-step,
restart it, restart the engine itself, and the run picks up from the step
it was actually on, not from the beginning.

It's split across three languages, each doing the part it's suited for:

- **`engine/`** (Go) - the orchestrator: REST API, persistent state, the
  lease-based task queue, retry/backoff, timeouts, compensation, and the
  scheduler loop that makes crash recovery possible.
- **`sdk-python/`** (Python) - the developer-facing SDK for defining
  workflows and running workers that execute steps.
- **`dashboard/`** (TypeScript/React) - a web UI for watching runs and
  inspecting their execution history.

See [docs/architecture.md](docs/architecture.md) for how the pieces fit
together.

## Running the engine

```
cd engine
go run ./cmd/flowforged
```

By default it listens on `:8080` and persists to `flowforge.db` in the
current directory. Both are overridable:

| Variable | Default | Purpose |
|---|---|---|
| `FLOWFORGE_ADDR` | `:8080` | HTTP listen address |
| `FLOWFORGE_DB_PATH` | `flowforge.db` | SQLite file |
| `FLOWFORGE_LEASE_SECONDS` | `30` | how long a worker holds a claimed step before it's reclaimed |
| `FLOWFORGE_SCHEDULER_INTERVAL_MS` | `1000` | how often the background sweep runs |

Register a workflow, start a run, and drive it by hand:

```
curl -X POST localhost:8080/v1/workflows -d '{
  "name": "order_pipeline",
  "steps": [
    {"name": "validate", "timeout_seconds": 30, "retry": {"max_attempts": 3, "backoff_base_ms": 500, "backoff_multiplier": 2, "backoff_max_ms": 30000}},
    {"name": "charge",   "timeout_seconds": 30, "retry": {"max_attempts": 3, "backoff_base_ms": 500, "backoff_multiplier": 2, "backoff_max_ms": 30000}},
    {"name": "ship",     "timeout_seconds": 30, "retry": {"max_attempts": 3, "backoff_base_ms": 500, "backoff_multiplier": 2, "backoff_max_ms": 30000}}
  ],
  "compensations": [
    {"name": "refund", "compensation_of": "charge", "timeout_seconds": 30, "retry": {"max_attempts": 3, "backoff_base_ms": 500, "backoff_multiplier": 2, "backoff_max_ms": 30000}}
  ]
}'

curl -X POST localhost:8080/v1/workflows/order_pipeline/runs -d '{"input": {"order_id": 42}}'

curl -X POST localhost:8080/v1/tasks/poll -d '{"workflow": "order_pipeline", "worker_id": "worker-1"}'
curl -X POST localhost:8080/v1/tasks/<step_id>/complete -d '{"lease_token": "<token>", "result": {}}'

curl localhost:8080/v1/runs/<run_id>
curl localhost:8080/v1/runs/<run_id>/history
```

Killing a worker after it claims a step but before it reports back
doesn't lose anything: once the lease expires, the scheduler puts the
step back up for grabs and the next poll picks it up, retry count
incremented, run context intact. Killing the engine process itself and
restarting it against the same database file has the same effect - there
is no in-memory state to lose.

Run the engine's test suite with `go test ./...` from `engine/`.

## Using the Python SDK

```
pip install -e ./sdk-python
```

```python
from flowforge import Client, RetryPolicy, Worker, Workflow

pipeline = Workflow("order_pipeline")

@pipeline.step("charge", retry=RetryPolicy(max_attempts=3))
def charge(ctx):
    return {"charged": True}

@pipeline.compensate("charge")
def refund(ctx):
    return {"refunded": True}

client = Client("http://localhost:8080")
client.register_workflow(pipeline)
client.start_run("order_pipeline", {"order_id": 42})

Worker(client, pipeline).run()
```

A step function takes a `Context` (`ctx.input` for the run's input,
`ctx.get("other_step")` for a prior step's result) and returns a
JSON-serializable result, or raises to signal failure - retry policy and
compensation are the engine's concern, not the worker's. The `Worker`
handles polling, heartbeating in-flight steps in a background thread, and
reporting completion or failure.

`examples/` has a full ten-step order pipeline and a walkthrough for the
crash-recovery scenario this project is built around - see
[examples/README.md](examples/README.md). It's not hypothetical: killing
the worker process mid-step and restarting it resumes the run from that
exact step, verified by hand against a running engine, including a
variant where the engine process itself is killed and restarted against
the same SQLite file.

Run the SDK's test suite with `pytest` from `sdk-python/` (after
`pip install -e ".[dev]"`).

## Status

This is under active development and being built incrementally. Each piece
lands with its own commit as it becomes real, rather than all at once.

Built so far: the Go engine (state machine, persistence, retries,
compensation, cancellation, the REST API) and the Python SDK (client,
workflow definitions, worker runtime). Not yet built: the dashboard.
