# ionflow on schrein: the catalog substrate

**Companion:** `cas-catalog-library.md` (the schrein spec). This document
describes how ionflow uses schrein; the load-bearing decisions are
recorded in `docs/decisions/0014` (ledger on `CasRegister`) and `0015`
(the per-flow DuckLake catalog).

## 0. The two roles, stated first

A lakehouse catalog is not only a schema store: in DuckLake the catalog
holds each table's **file list** — a Parquet file on S3 is not part of any
table until a commit registers it. That mechanism is what ionflow builds
its read side on. Run *tracking* is a different role and never enters the
lake:

| Component | Authority over |
|---|---|
| **catalog.ducklake** (per flow, schrein register) | The lake: table schemas, snapshots, and the file list that defines each step table's contents |
| **`ledger.parquet`** (schrein `CasRegister`) | Run bookkeeping: status, timestamps, commit, params |
| **`_meta.json`** | Per-run progress: `steps_complete`, `fanout_keys`, resume provenance |
| **`_metrics*.parquet`** | Run telemetry (its flush cadence doubles as the run heartbeat) |

## 1. Layout and write path

```
s3://bucket/<prefix>/<flow_id>/
  ledger.parquet              <- run bookkeeping (CAS)
  catalog.ducklake            <- schrein register: the flow's lake
  _lake/                      <- catalog-owned files only (none today)
  runs/run_id=<id>/           <- immutable run folders: _meta.json,
    <step>/part_N.parquet        _metrics*.parquet, boto3-written parts
```

The orchestrator (never a worker) talks to the catalog exactly three ways:

1. **Genesis** — idempotent `CREATE TABLE IF NOT EXISTS` per step from the
   `Table` DDL, committed *before* the first run object exists, so a run
   always implies a catalog.
2. **Registration** — one CAS commit per completed step:
   `ducklake_add_data_files(..., hive_partitioning => true)` per part.
   run_id is inferred from the hive path; parts are never rewritten; a
   fan-out's parts land in one commit, so lake readers never observe a
   partial fan-out. Registration failure fails the run.
3. **Recovery** — `ionflow catalog-backfill` rebuilds a lost or corrupt
   register from the run folders (one LIST, genesis, one registration
   commit). Bucket versioning is cheaper insurance still.

Commit rate: one CAS commit per completed step plus genesis. Concurrent
runs of one flow commute (different run_ids) and stay far below schrein's
comfort zone; rebases replay cleanly.

## 2. Read path

`query.duck()` attaches a downloaded catalog snapshot read-only: one view
per step (`run_id` kept as the last column), plus `ledger` and `metrics`
views over their own objects. A flow that never ran gets typed empty
views. Anyone with stock DuckDB attaches the same catalog with no ionflow
installed — typed schemas included.

Consequences of register-at-completion: a half-finished fan-out is
invisible to lake readers (its parts are unreferenced objects, not table
contents); a crashed run's unregistered parts are orphans; its registered
steps remain queryable under that run_id, with status always from the
ledger. Crash windows collapse into the same benign class as parts/meta
skew: parts uploaded but unregistered → the step recomputes on resume;
registered but meta unflushed → resume recomputes under a new run_id.

## 3. Costs, accepted

- The ducklake extension sits on every run and read path (loaded via
  schrein's three-way loader, decision 0016).
- One CAS round-trip of latency per completed step — negligible against
  step runtimes.
- A clobbered register breaks reads until `catalog-backfill` (or bucket
  versioning) restores it; data files and the ledger are unaffected.

## 4. Open

- **Retention** — run deletion as `DELETE WHERE run_id` + schrein
  `expire_snapshots`/`gc`; would give deletion real semantics and as-of
  time travel. Unscheduled.
- (`publish` blesses outputs into a separate stable publish DuckLake,
  0009: consumers who must not depend on the flow's bucket, layout, or
  run status attach that lake and see only blessed tables — which the
  flow catalog's discoverability does not replace.)
