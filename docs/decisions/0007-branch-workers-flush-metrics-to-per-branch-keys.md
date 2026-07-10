# 0007 — Branch workers should flush metrics to per-branch keys

Status: accepted, not yet implemented (2026-07-10)

## Context

Branch workers sample themselves but only hand their rows back through the
pool result channel when the branch completes; the orchestrator merges them
into the single `_metrics.parquet`. A run killed mid-branch therefore loses
all branch samples — the 60s heartbeat flush carries only orchestrator rows.
Failed runs are exactly where per-branch memory/CPU data matters most.

## Decision

Branch workers should flush periodically to their own keys (e.g.
`_metrics_<step>_<branch>.parquet` in the run folder), preserving the
one-writer-per-object invariant — no CAS needed, each process owns its key.
The result-channel merge can stay for the happy path or be replaced by the
per-branch objects.

## Consequences

- The `metrics` view's glob must include the per-branch objects (and
  `parse_key` must keep them invisible to run scans, like `_metrics.parquet`).
- Many small S3 PUTs during wide fan-outs; bounded by the flush interval.
- The orchestrator's heartbeat remains the run's liveness signal — per-branch
  flushes are for crash-durable data, not staleness detection.
