# 0021 — One scheduler over the whole DAG; every step body runs in a worker

Status: accepted (2026-07-29); builds on 0015 (per-item sub-DAGs), 0018 (keys
as identity) and 0019 (one metadata store)

## Context

Execution had two regimes. The orchestrator walked `dag.plan` serially — two
independent chains never overlapped — running plain steps and fan-out
emissions *in its own process* (uncapped DuckDB, BLAS state that forced
spawn-not-fork, user import side effects in the coordinator), while group
items ran in a per-group pool fed by a bespoke input protocol (`group_deps`
path lists through the pool initializer, per-item `inputs` dicts, a second
resolver mirroring the orchestrator's).

## Decision

The runner is a scheduler and nothing else. `Dag.schedule_graph()` yields
exact node edges — every plain/fan-out step, plus an `("items", f)` node per
group so a fan-out's dependents wait for its items while its emission does
not wait on member-only inputs — and one pool (per run, spawn processes or
threads) executes every task: a whole plain/fan-out step, or one item's
pending sub-steps. Consequences by construction:

- **Independent chains run concurrently.** Ready nodes launch together;
  reuse (the `Planner`'s verdicts) can complete a node with no tasks at all.
- **No user code in the orchestrator.** BLAS caps, fair DuckDB shares and
  crash isolation now cover plain steps too; the coordinator's own DuckDB
  use shrinks to catalog commits. (Spawn stays: the sampler and commit
  machinery keep this process multi-threaded.)
- **Workers resolve inputs from the flushed manifest.** The orchestrator
  commits + exports `_meta.json` before scheduling a node, so a worker reads
  one meta snapshot per (process, node, phase) and derives everything —
  the shared-payload path lists, per-item input dicts, and the orchestrator's
  twin resolver are deleted. The pool initializer now carries only per-run
  constants.
- **Cross-step `self` state is the declared contract, exactly.** Steps run
  in fresh instances, so what travels between them is params plus `Ledger`
  columns: seeded into each whole-step task, harvested from its result, and
  merged in completion order (deterministic along dependency chains). Item
  tasks still do not carry `self` back — unchanged rule, now enforced by
  shape rather than by footnote.
- **Fair shares follow real concurrency.** Task resources are sized at
  submit from what is actually in flight: a solo step gets the whole
  machine (what the orchestrator used to give it), a wide wave splits it.
- **Every task measures itself** (process mode): whole-step tasks flush
  `_metrics.<step>.parquet` (dot-separated — step names cannot contain dots,
  so item objects can't collide), items keep `_metrics_<fanout>_<key>`. In
  thread mode the orchestrator's sampler labels the union of active nodes.

## Consequences

- A run now always spins the pool up (workers spawn on demand), and a fast
  item may be packed into an already-warm worker instead of waiting for a
  cold one — same-process-per-item still holds; distinct-process placement
  was never a contract (the affected test now forces overlap explicitly).
- Undeclared attributes set on `self` in one step are no longer visible to
  later steps. They were an accident of in-orchestrator execution; the
  supported channel is params + declared Ledger columns.
- The failure path is uniform: a task error stops new scheduling, in-flight
  tasks settle and commit their progress — including nodes unrelated to the
  failure, which previously never got to start.
