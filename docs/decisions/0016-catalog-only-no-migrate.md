# 0016 — the catalog is the only mode for stable flows; no migration workflow

Two removals, both deliberate:

**Classic mode is gone for stable flows.** 0015 introduced the catalog
behind `catalog = "ducklake"`; the flag and `IONFLOW_CATALOG` are removed.
Every stable flow registers completed steps into its DuckLake and reads
through it — `query.duck()` views an attached snapshot, and a bucket with
runs but no catalog is refused with a pointer at `ionflow catalog-backfill`
(the adoption path) rather than silently falling back to globs. Views keep
`run_id` as the last column, preserving the classic hive-partition position.
Prototype flows remain catalog-less by construction — untyped steps and
free schema churn have no catalog analogue — so the glob read path survives
only in their scratch namespace.

**`ionflow migrate` will not be built.** Schema evolution across historic
runs (the exploration spec's §3.3) is dropped, not deferred. The standing
semantics stay what they were: the drift check refuses mismatched history,
and a changed `Table` fails loudly at registration against the old catalog
table. The supported paths for a schema change are the existing ones —
rename the flow (a new namespace and catalog, per 0004), or, when historic
parts already conform to the new schema, `catalog-backfill --replace`.

Costs accepted: every run now performs the genesis check plus one CAS
commit per completed step (the full test suite went from ~20s to ~50s —
runner tests all exercise the catalog path now, which is also the point),
and the ducklake extension sits on every stable flow's run and read path.
