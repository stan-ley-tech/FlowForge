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

## Status

This is under active development and being built incrementally. Each piece
lands with its own commit as it becomes real, rather than all at once.
