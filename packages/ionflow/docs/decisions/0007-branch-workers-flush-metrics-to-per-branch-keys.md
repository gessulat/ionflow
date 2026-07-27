# 0007 — Branch workers flush metrics to per-branch keys

Status: accepted (2026-07-26)

## Context

The obvious channel for branch metrics is the pool result: workers hand
their samples back on completion and the orchestrator merges them into the
single `_metrics.parquet`. Rejected: a run killed mid-branch would lose all
branch samples — and failed runs are exactly where per-branch memory/CPU
data matters most.

## Decision

Each process-mode worker flushes periodically (and finally, on success or
failure) to its own `_metrics_<step>_<branch>.parquet` in the run folder.
Nothing travels over the result channel: the `metrics` view globs
`_metrics*.parquet`, unioning the orchestrator's object with the workers'.
One-writer-per-object holds — each process owns its key, no CAS.

## Consequences

- Failed and killed branches keep their samples up to the last flush; the
  worker's final write happens in a `finally`, swallowing its own errors so
  metrics never mask the branch's outcome.
- Wide fan-outs mean more small S3 PUTs, bounded by the 60s flush interval.
- The orchestrator's `_metrics.parquet` remains the run's only liveness
  heartbeat; branch objects are data, not staleness signals.
- These keys stay invisible to `parse_key` (run-dir level, like `_meta.json`),
  so run scans and drift checks never see them.
