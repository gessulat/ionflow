# 0019 — The flow catalog is the one metadata authority

Status: accepted (2026-07-29); amends 0012 (per-flow DuckLake catalog),
supersedes 0011 (ledger CAS on schrein) and the ledger/catalog role split

## Context

A run's metadata lived in three stores stitched together by invariants:
`ledger.parquet` (CAS, run bookkeeping), per-run `_meta.json` (progress +
resume provenance), and the DuckLake catalog (data registrations). 0011/0012
kept the roles separate on principle. The price was three consistency
stories and crash windows between the stores — "a run implies a catalog",
"a partial fan-out must not look resumable", and the registration-before-
meta ordering existed precisely because progress, registration and status
committed separately.

## Decision

The flow's DuckLake catalog (already a schrein CAS register) absorbs run
bookkeeping. Beyond the per-step data tables it holds two system tables with
**fixed** DDL: `runs` — the ledger — and `manifest`, mapping
`(run_id, step, item_key)` to the part object holding it. Every completion
point is a single CAS commit (`Catalog.commit_run`): genesis, the run's
ledger row, its new manifest rows, and the data-file registrations of the
parts those rows name, atomically. Consequences:

- **The invariant classes dissolve.** A reader can never see a part
  registered without its manifest row, a run without its tables, or a
  half-registered step — by construction, not by write ordering.
- **`ledger.parquet` is gone.** Its CAS rebuild loop (0011) with it; schrein
  now serves ionflow through the DuckLake alone. The user-facing `ledger`
  view remains, over `lake.runs`.
- **No schema migration, structurally.** `runs` stores params and Ledger
  columns as JSON text; the `ledger` view derives typed columns from the
  flow's declarations at read time. A new param is a view change, never a
  table change. (DuckLake inlines these small rows into the register
  artifact itself — no small-file churn in `_lake/`.)
- **`_meta.json` demotes to a write-only recovery export.** The runner still
  writes it at every commit point, but nothing reads it on the normal path;
  `backfill` is its only reader and now restores *everything* — ledger rows,
  manifests, registrations — for every recorded run (failed ones included)
  in one commit. `sync` re-derives the target catalog the same way instead
  of MERGEing ledger rows.
- **A failed run's completed work is queryable.** The failure commit lands
  partial group-member rows along with the status, so a dead run's finished
  items (not just fully-complete steps) survive into the catalog — resume
  and readers agree on what exists.
- Flow discovery keys off `catalog.ducklake` instead of the ledger object;
  `runs` and `manifest` join `ledger`/`metrics` as reserved step names.

## Consequences

- Every read of run state (ls, resume, TUI) downloads the register — the
  ledger-only fast path is gone. The artifact stays small (rows inline,
  data files register by reference), so this is one GET either way.
- Liveness is unchanged: the metrics-flush heartbeat still marks stale
  `running` rows at read time; a lease inside the register would be the
  next step if it ever matters.
- Concurrent runs of one flow contend on one register; schrein's rebase
  makes that safe (commit closures are replayable and idempotent — replays
  skip rows that already landed).
