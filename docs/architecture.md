# Architecture

FlowForge separates workflow *orchestration* from workflow *execution*. The
engine (Go) owns durable state and decides what runs next; workers (Python)
own business logic and never hold state the engine doesn't already have on
disk. That split is what makes crash recovery possible: a worker can die at
any point and the engine's view of the world doesn't change, because the
worker never was the source of truth.

## Components

- **engine** (Go) - HTTP API, SQLite-backed state store, scheduler loop.
  Tracks workflow definitions, runs, steps, and the event log. Hands out
  work through a lease-based queue instead of pushing to specific workers.
- **sdk-python** - client library for defining workflows as ordered steps,
  plus a worker runtime that polls the engine, executes step functions, and
  reports results back.
- **dashboard** (TypeScript/React) - reads run and event history from the
  engine's API and renders it as a timeline.

## Run and step model

A workflow definition is an ordered list of steps. Starting a run
materializes one row per step, all `PENDING`, before anything executes.
Only the first step is immediately eligible; each subsequent step becomes
eligible (`READY`) only once the one before it reaches `COMPLETED`. This
keeps ordering a property of stored state, not of any in-memory scheduler,
so it survives an engine restart as cleanly as a worker restart.

Step states: `PENDING -> READY -> LEASED -> RUNNING -> COMPLETED`, with
`FAILED`, `CANCELLED`, and the compensation states (`COMPENSATING` /
`COMPENSATED`) branching off along the way.

## Leasing and crash recovery

Workers never get told to run something directly. They call
`POST /v1/tasks/poll`, and the engine atomically claims one `READY` step for
them, returning a lease token and a lease deadline. The worker must either
finish the step or send a heartbeat before that deadline.

A background scheduler loop in the engine sweeps for leases past their
deadline and puts those steps back to `READY` for another attempt. This is
the entire crash recovery mechanism: if a worker is killed mid-step, it
simply stops heartbeating, its lease expires, and the step becomes claimable
again - by the same worker on restart, or a different one entirely. No
special-case "resume" logic exists because nothing about a restart is
special: it's the same poll loop finding the same step in the same table.

## Retries, backoff, and timeouts

Every step carries its own retry policy (max attempts, base backoff,
multiplier, cap, jitter). A failure - whether reported explicitly by a
worker, caused by a lease expiring, or caused by a step's own timeout
being exceeded - goes through the same path: increment the attempt count,
and either compute the next backoff and set the step back to `READY` at a
future `scheduled_at`, or mark it permanently `FAILED` once attempts are
exhausted.

Step timeout and lease duration are deliberately different numbers: the
lease is about detecting a dead worker, the timeout is a business-logic cap
on how long a step is allowed to run at all, enforced by the engine even if
the worker is alive and heartbeating.

## Compensation

A step may declare a compensation step. When a run fails permanently or is
cancelled, the engine walks its completed steps in reverse and schedules
their compensations, same lease/retry machinery as forward execution. The
run only reaches a terminal `COMPENSATED` state once every compensation for
every completed step has itself completed.

## Idempotency

Each step task carries an idempotency key derived from `run_id` and step
name - stable across retries of that step, so worker-side handlers touching
external systems (payments, emails, writes to another service) can dedupe
using it. On the engine side, completion and failure reports carry the
lease token issued at claim time; a report with a stale or mismatched token
is rejected, so a worker that was already reclaimed cannot corrupt a step
another attempt is now working on.

## Concurrency control

Workflow definitions can cap how many runs execute concurrently. Runs
started beyond the cap sit in `QUEUED` and are admitted by the scheduler
loop as capacity frees up, rather than being rejected outright.

## Execution history

Every state transition is appended to an events table: run started, step
became ready, step claimed, step completed, step failed, retry scheduled,
lease reclaimed, compensation started, run reached a terminal state, and so
on. This log is both the audit trail exposed over the API and the data the
dashboard renders as a timeline - there's no separate visualization-specific
tracking to keep in sync.
