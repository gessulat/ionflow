# 0009 — publish is a copy to a stable key, with a provenance sidecar

Status: accepted (2026-07-24)

## Context

Consumers outside microflow (trainers, cron jobs) want a fixed S3 path, not
an opaque `run_id=` folder discovered through `duck()`. The alternatives were
coupling every consumer to microflow's API, or hand-copying parts after each
run.

## Decision

`microflow publish FLOW RUN_ID STEP DEST [--where ...] [--force]` (also
`Flow.publish`) copies one step's output from a *complete* run to one plain
parquet object, plus a `<dest>.meta.json` sidecar recording flow, run_id,
step, filter, commit, and publish time. It refuses an existing destination
unless forced. Always a copy — run folders stay immutable and publish never
adds a mutable pointer object.

## Consequences

- Consumers read a fixed path with zero microflow dependency; the sidecar
  answers "what is this file?" forever.
- Replacing one object is an atomic PUT, but two related objects published
  separately can be observed mid-swap (S3 has no rename). Consumers needing
  cross-file consistency must compare the sidecars' run_id. Accepted and
  documented rather than hidden behind a pointer indirection.
- `--where` makes publish a query-and-rewrite (through `write_part`, honoring
  the boto3-writes rule), not a byte copy. It stays one verb + one filter;
  anything more is a flow step, not a publish flag.
