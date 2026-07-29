# ionflow on schrein: the catalog substrate

**Companion:** `cas-catalog-library.md` (the schrein spec). This document
describes how ionflow uses schrein; the load-bearing decisions are 0012 (the
per-flow DuckLake catalog), 0019 (the catalog as the one metadata authority)
and 0020 (runs as manifests).

## 0. One authority

A DuckLake catalog holds each table's **file list** — a Parquet object is not
part of any table until a commit registers it. ionflow builds both its read
side and its run bookkeeping on that one CAS-swapped artifact:

| Object | Holds |
|---|---|
| **catalog.ducklake** (per flow, schrein register) | Everything with authority: per-step data tables (the corpus — each part once, under its creating run), `runs` (the ledger), `manifest` (run -> (step, item_key, path)) |
| **`_meta.json`** (per run) | Write-only recovery export; read only by `catalog-backfill` |
| **`_metrics*.parquet`** | Run telemetry (its flush cadence doubles as the run heartbeat) |

## 1. Layout and write path

```
s3://bucket/<prefix>/<flow_id>/
  catalog.ducklake            <- schrein register: the flow's lake
  _lake/                      <- catalog-owned files (system rows inline, so ~none)
  runs/run_id=<id>/           <- immutable folders: _meta.json, _metrics*.parquet,
    <step>/part_<key>.parquet    boto3-written parts, named by item key (0018)
```

The orchestrator (never a worker) writes through `Catalog.commit_run`: one
CAS commit per plan node (plus begin/final) carrying genesis, the run's
ledger row, manifest rows, and `ducklake_add_data_files` for the newly
created parts among them — atomically, so readers never observe a partial
fan-out, a part without its manifest row, or a run without its tables.
Referenced (reused) parts are manifest-only; hive `run_id` names a part's
creator. Commits are replay-safe: a rebase skips rows that already landed.
Recovery: `ionflow catalog-backfill` rebuilds the whole register — rows,
manifests, registrations, failed runs included — from the `_meta.json`
records in one commit; `sync` derives a target catalog the same way.

## 2. Read path

`query.duck()` reads one attached snapshot: per-step views join the
manifest's part objects to their referencing run (a part reused by N runs
appears once per run), and the `ledger` view flattens `runs`' JSON
params/custom into typed columns from the flow's declarations. A flow that
never ran gets typed empty views. Stock DuckDB attaches the same catalog
with no ionflow installed: `lake.<step>`, `lake.runs`, `lake.manifest`.

## 3. Costs, accepted

- The ducklake extension sits on every run and read path (loaded via
  schrein's three-way loader, decision 0013).
- One CAS round-trip per plan node — negligible against step runtimes.
- A clobbered register breaks reads AND bookkeeping until `catalog-backfill`
  (or bucket versioning) restores them; data files and metas are unaffected.

## 4. Open

- **Retention / gc** — with references (0020), run deletion must consult
  `lake.manifest` as the refcount; schrein `expire_snapshots`/`gc` cover the
  lake side. Unscheduled.
- (`publish` blesses outputs into a separate stable publish DuckLake, 0009.)
