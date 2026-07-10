# 0002 — Resume copies parts; every run folder is self-contained

Status: accepted (2026-07-10)

## Context

Resume reuses the origin run's completed steps. The obvious alternative to
copying is referencing: record pointers to the origin's part keys in
`_meta.json` and bind step inputs across run folders.

## Decision

Resume server-side-copies (`CopyObject`) every completed part from the origin
run into the new run's folder (`runner.py`, `copy_part`). One folder = one
run, always: the hive `run_id=` views never reach across runs, provenance is
physical, and deleting or archiving any run folder can never break another
run. Server-side copy moves no data through the client and runs on a 16-way
thread pool, so the latency cost is small.

## Consequences

- Storage grows by the full size of reused steps on every resume. Accepted.
- Pointer-based reuse was considered and rejected: cross-run references would
  complicate the query views, break the immutability story, and make run
  deletion order-dependent.
