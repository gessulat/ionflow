# 0008 — Keyed incremental resume: fan-out items matched by branch_key, single origin

Status: accepted (2026-07-24)

## Context

A growing corpus (add 5 files to 500, re-run) had no good story: a fresh run
recomputes everything, and querying the union of runs is broken by design
(failed runs leave partial branch parts; resume duplicates parts under two
run_ids). Resume matched branches by list position, so it could only ever
reuse a frozen work-list. A cross-run content-addressed cache index was
considered and rejected.

## Decision

- A fan-out item's identity is a user-declared column: `field(branch_key=True)`
  on the fan-out step's output Table (one column, str/int, NOT NULL; exactly
  one distinct value per item, enforced at write). microflow never guesses
  identity — hashing parquet bytes is not canonical, and identity (path,
  etag, params) lives in the user's data anyway.
- Every run records `branch_keys` (fan-out step -> key per branch index) in
  `_meta.json`.
- Declaring a branch_key changes resume for that flow: the fan-out re-runs
  (refreshing the work-list), branch parts are copied from the origin where
  the key matches and computed where it doesn't, and steps further downstream
  are reused only when the key set is unchanged — otherwise they recompute.
  Removed keys simply drop out. Flows without branch_key keep positional
  resume with a frozen fan-out.
- The source is a single origin run, not a cache index. Because resume copies
  parts forward (0002), the latest complete run already contains the whole
  accumulated corpus — it *is* the cache, resolved with one ledger read.
  `run --incremental` sugars exactly that.

## Consequences

- "Add files, re-run" costs what the new files cost, while every run folder
  stays a single, complete, immutable corpus. Deletions work too — something
  a union-of-runs approach could never give.
- A complete run now means "assembled at this commit"; parts may have been
  computed by ancestor runs (chain via `origin_run_id`). The commit gate
  (0005) is unchanged and is what keeps reuse honest: same commit reuses
  freely, drift must be forced. Params still come from the origin.
- The key column is a contract: same key must mean same content. Users who
  re-upload a source file must reflect that in the key (e.g. include the
  etag), or resume will happily keep the stale part.
- Storage grows by the corpus size per incremental run (the 0002 trade-off at
  a higher cadence). Old complete runs can be lifecycle-expired safely —
  self-contained folders are what make both that and single-origin reuse
  possible.
- Origins predating branch keys can't be matched; resume warns and recomputes
  the keyed subgraph.
