# Crash/resume demo

This walks through the scenario FlowForge is built around: a ten-step
order pipeline, a worker killed mid-step, and the run picking back up
without redoing anything already done.

## 1. Start the engine

```
cd engine
go run ./cmd/flowforged
```

Leave it running. It listens on `:8080` by default.

## 2. Install the SDK and register the workflow

From the repo root:

```
pip install -e ./sdk-python
cd examples
python register_workflow.py
```

This registers `order_pipeline` - ten steps: `validate_order`,
`reserve_inventory`, `authorize_payment`, `capture_payment`,
`generate_invoice`, `pack_shipment`, `dispatch_carrier`,
`notify_customer`, `update_analytics`, `close_order` - with compensations
defined for the inventory and payment steps.

## 3. Start a run

```
python start_run.py
```

Prints a run id. Keep it - you'll use it to watch progress.

## 4. Run a worker that pauses on step 6

`pack_shipment` is the sixth step. Tell it to pause so there's a window
to kill the process while that step is actually in flight and holding a
lease:

```
FLOWFORGE_DEMO_PAUSE_STEP=pack_shipment FLOWFORGE_DEMO_PAUSE_SECONDS=25 python run_worker.py
```

(On Windows PowerShell: `$env:FLOWFORGE_DEMO_PAUSE_STEP="pack_shipment"; $env:FLOWFORGE_DEMO_PAUSE_SECONDS="25"; python run_worker.py`)

Watch the output. Steps 1-5 complete quickly. When you see:

```
[pack_shipment] pausing 25s - kill the worker now to test crash recovery
```

kill the process - Ctrl+C won't do it since that's a clean shutdown;
use `kill -9 <pid>` (or End Task / Stop-Process -Force on Windows) to
simulate an actual crash with no chance to report back.

## 5. Check the run while the worker is dead

```
curl localhost:8080/v1/runs/<run_id>
```

`pack_shipment` is still shown as `LEASED` at first. Wait past the
lease duration (30 seconds by default) and check again - it flips back
to `READY`, with an `error` field noting the lease expired, and attempt
count incremented. Steps 1-5 are untouched, still `COMPLETED`.

You can also optionally kill and restart the *engine* itself here (same
`FLOWFORGE_DB_PATH`) to confirm this isn't worker-crash-specific - the
run's state lives in SQLite, not in the engine's memory.

## 6. Restart the worker

```
python run_worker.py
```

No pause env vars this time. It polls, picks `pack_shipment` back up at
the next attempt, finishes it, and carries on through
`dispatch_carrier`, `notify_customer`, `update_analytics`, and
`close_order` on its own.

## 7. Confirm it's done

```
curl localhost:8080/v1/runs/<run_id>
curl localhost:8080/v1/runs/<run_id>/history
```

The run is `COMPLETED`. The history shows every step completing once in
order, plus a `STEP_RETRY_SCHEDULED` (or `LEASE_RECLAIMED`) entry for
`pack_shipment` marking exactly where the interruption happened - the
full record of a run that survived losing its worker mid-step.
