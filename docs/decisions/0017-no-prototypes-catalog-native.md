# 0017 — no prototype mode; the codebase is catalog-native

ionflow is unreleased with no consumers, so instead of carrying transition
machinery the codebase is rewritten as if DuckLake had always been the only
substrate. Two features are removed outright:

**Prototypes.** The `Prototype` mixin, the `_proto/` scratch namespace, the
dirty-git escape hatch (`commit_or_dirty`, `dirty:<sha>` records), untyped
steps with write-time schema inference, the ledger's `reset_on_conflict`,
and `ionflow clean` are all gone. Every flow carries the full guarantees —
clean git, typed steps, a catalog — from its first run. The iterate-fast
workflow prototypes served is a real need, but it deserves a design that
doesn't fork every subsystem into two modes; if it returns, it returns as
its own decision.

**Every classic-mode vestige.** With no catalog-less flows left: glob-based
step views, `Layout.table_glob`, `parse_key`, and the per-step part
sampling in discovery are deleted. `scan_runs` reduces to "flows with a
ledger" (one LIST, key-suffix match); the schema-drift check reads the
flow's catalog (one GET + DESCRIBE per step) instead of parquet footers —
a step without a catalog table yet is not drift, it simply gets its table
at the next genesis. `query.duck()` has exactly one real read path: attach
the catalog snapshot; typed empty views cover never-ran flows; runs
without a catalog mean the register was lost and are refused with a
pointer at `ionflow catalog-backfill`, the only recovery path. The runs/
LIST survives solely to detect that case and the presence of metrics
objects.

What deliberately stays outside the catalog, unchanged: `ledger.parquet`
(run bookkeeping), `_meta.json` (resume provenance), `_metrics*.parquet`
(telemetry) — per the role separation the schrein spec's scope guard
demands (0014, 0015).
