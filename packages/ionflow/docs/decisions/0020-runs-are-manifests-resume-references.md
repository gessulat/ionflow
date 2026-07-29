# 0020 — A run is a manifest; resume references parts instead of copying them

Status: accepted (2026-07-29); supersedes 0002 (resume copies parts into a
self-contained run folder), builds on 0018 (keys as identity) and 0019 (one
metadata store)

## Context

0002 made every resume server-side-copy all reused parts so that one folder =
one run. For the motivating workload — 500 raw files, add 5, re-run — every
incremental run duplicated ~the whole corpus in storage. The copies existed
because run membership was inferred from the `run_id=` path segment at
registration; but 0012 made the catalog the only read path, and 0019 gave
that catalog a `manifest` table — run membership no longer needs to live in
pathnames.

## Decision

A part is written **once**, by the run that computes it, into that run's
folder; a run *is* its manifest rows. Resume records references — origin part
paths under the new run_id — and copies nothing. Mechanics:

- **Registration follows creation.** `Catalog._apply` registers a data file
  into the step's table only when its path lies inside the committing run's
  own folder; a referenced path is manifest-only. The data table `lake.<step>`
  thus holds every part exactly once, keyed by its *creating* run (hive) —
  the corpus — while `lake.manifest` says which parts each run comprises.
- **Per-run reads join the manifest.** `duck()` builds each step view as
  `read_parquet` over the manifest's part objects joined to a
  (path → run_id) mapping on the `filename` column: a part referenced by N
  runs appears once per run, exactly as the copies used to, at zero storage.
  Snapshot consistency holds because the paths come from one attached
  catalog snapshot and the objects are immutable.
- **References flatten.** A resumed run's manifest stores concrete paths,
  wherever they live — resuming a resume never chases a chain.
- **Workers already take paths, not positions** (0018), so cross-run inputs
  need no special casing anywhere in execution.
- **Publish and recovery read the manifest** for a run's parts; nothing
  LISTs a run folder to find data anymore.

## Consequences

- Storage per incremental run is O(changed items), not O(corpus). The
  latency of the copy fan-out disappears from resume as well.
- Deleting a run folder can now break runs that reference it. Nothing in
  ionflow deletes runs today; when a `gc`/`rm` verb arrives it must refuse
  (or cascade) while other manifests hold references — the manifest table is
  the refcount.
- Step views enumerate part paths inline (DuckDB's `read_parquet` does not
  take subquery arguments), so `duck()` SQL grows with distinct part count.
  Fine at ionflow's scale; revisit with a macro/temp-table form if it hurts.
- Hive pruning by `run_id` on the old views is traded for exact
  manifest-driven membership. Filters inside a part still push down.
