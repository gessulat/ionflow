# 0007 — Branch workers flush metrics to per-branch keys

Status: accepted and implemented (2026-07-10)

## Context

Branch workers used to hand their samples back through the pool result
channel on completion; the orchestrator merged them into the single
`_metrics.parquet`. A run killed mid-branch therefore lost all branch
samples — and failed runs are exactly where per-branch memory/CPU data
matters most.

## Decision

Each process-mode worker flushes periodically (and finally, on success or
failure) to its own `_metrics_<step>_<branch>.parquet` in the run folder.
The result-channel merge is gone: the `metrics` view globs
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
