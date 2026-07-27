# 0012 — a per-flow DuckLake catalog is the substrate

Status: accepted (2026-07-26)

Every flow owns a schrein DuckLake at its root (`catalog.ducklake`, CAS
register; data path `_lake/` for catalog-owned files only). One table per
step, rows across runs keyed by `run_id`. There is no mode selector, no
scratch tier, and no glob-based read path — the catalog is how step
outputs become queryable, full stop.

Mechanics, and the invariants they carry:

- **Registration, never rewriting.** Parts are written by boto3 into
  immutable run folders (0010) and registered metadata-only via
  `ducklake_add_data_files(..., hive_partitioning => true)`: `run_id`
  comes from the part's `runs/run_id=<id>/` path segment, so files carry
  no run_id column and resume's server-side copies (0002) register under
  the new run's id for free.
- **Genesis precedes the first run object.** `CREATE TABLE IF NOT EXISTS`
  per step (persisted types — the read-side rule; NOT NULL only, all
  DuckLake enforces; PK/CHECK stay ionflow-side per 0003) commits before
  `_meta.json` or the ledger row exist. Invariant: a run implies a
  catalog, so "runs but no catalog" always means a lost register.
  `run_id` is reserved as a column name, checked at class definition.
- **One CAS commit per completed step**, by the orchestrator only, at the
  step's completion point — a fan-out's parts land atomically, so lake
  readers never see a partial fan-out. Registration failure fails the run.
- **Reads attach a snapshot.** `duck()` downloads and attaches read-only,
  one view per step (`run_id` last). A vanilla DuckDB client attaches the
  same catalog with no ionflow installed. A flow that never ran gets typed
  empty views; a lost register is refused with a pointer at
  `ionflow catalog-backfill` — the recovery path (one LIST, genesis, one
  registration commit), and deliberately the only one.
- **No schema-migration workflow.** The drift check reports mismatches
  and a changed `Table` fails loudly at registration; the supported paths
  are a new flow name (0004) or `catalog-backfill --replace` when
  historic parts conform.

Out of the lake, deliberately: `ledger.parquet` (run bookkeeping),
`_meta.json` (resume provenance), `_metrics*.parquet` (telemetry) — the
role separation the schrein spec's scope guard demands (0011). Costs
accepted: the ducklake extension on every run/read path, and one CAS
round-trip of commit latency per step — negligible against step runtimes.
