# 0008 — Keyed incremental resume: items matched by `fanout_key`, one origin, one spelling

Status: accepted (2026-07-26); refined by 0014 (branch identity travels down a chain) and 0018 (item keys became the storage identity — the fanout_keys meta field and position matching are gone)

## Context

A growing corpus (add 5 files to 500, re-run) needs a story: a fresh run
recomputes everything, and querying the union of runs is broken by design
(failed runs leave partial branch parts; resume duplicates parts under two
run_ids). Positional branch matching can only ever reuse a frozen
work-list. A cross-run content-addressed cache index was considered and
rejected. And however the origin is chosen, resuming must not sprawl into
several user-facing spellings of one mechanism.

## Decision

- A fan-out item's identity is a user-declared column, marked `fanout_key`
  (one column, str/int, NOT NULL; exactly one distinct value per item,
  enforced at write). ionflow never guesses identity — hashing parquet
  bytes is not canonical, and identity (path, etag, params) lives in the
  user's data anyway. The marker is exported as a shortcut symmetric with
  `primary_key`:

  ```python
  class Doc(Table):
      path: str = fanout_key   # this Fanout item's identity
  ```

  `field(fanout_key=True)` combines it with other constraints. The name
  deliberately pairs with `Fanout[...]`: declaring a fan-out and declaring
  its item identity share the word `fanout`. A `branch_key` spelling was
  rejected — "branch" is runner vocabulary for one parallel execution;
  nothing in the authoring surface calls the feature "branching".
  `Table.fanout_key_column()` reads the marker back.
- Every run records `fanout_keys` (fan-out step -> key per branch index) in
  `_meta.json`.
- Declaring a fanout_key changes resume for that flow: the fan-out re-runs
  (refreshing the work-list), branch parts are copied from the origin where
  the key matches and computed where it doesn't, and steps further downstream
  are reused only when the key set is unchanged — otherwise they recompute.
  Removed keys simply drop out. Flows without a fanout_key keep positional
  resume with a frozen fan-out.
- The source is a single origin run, not a cache index. Because resume copies
  parts forward (0002), the latest complete run already contains the whole
  accumulated corpus — it *is* the cache, resolved with one ledger read.
- One resume spelling, end to end: `runner.run(..., resume=...)` takes an
  origin run_id or the string `"latest"`; the CLI exposes it as
  `run --resume RUN_ID|latest`. `"latest"` resolves to the newest complete
  run and falls through to a fresh full run when there is none, so cron jobs
  can pass it unconditionally; an explicit run_id that doesn't exist is an
  error. A separate `resume` command and an `--incremental` flag were
  considered and rejected: they are the same code path differing only in
  how the origin is chosen, would force newcomers to learn one feature as
  two, and would duplicate every sizing flag (`--max-workers` etc.) across
  commands. `allow_commit_drift` without `resume` is a ValueError, not a
  silent no-op. A run_id can never collide with the sentinel: ids are
  `<epoch-ms>-<hex8>`, so the literal string `latest` is unambiguous.

## Consequences

- "Add files, re-run" costs what the new files cost, while every run folder
  stays a single, complete, immutable corpus. Deletions work too — something
  a union-of-runs approach could never give.
- "Resume a failed run" and "incremental corpus run" are visibly the same
  operation with a different origin — which they are; there is one concept
  to learn and document, and every sizing flag exists exactly once.
- A complete run means "assembled at this commit"; parts may have been
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
- An origin whose `_meta.json` records no fanout keys (the flow declared its
  key after that run) can't be matched; resume warns and recomputes the
  keyed subgraph.
