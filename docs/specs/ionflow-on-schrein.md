# Spec exploration: ionflow step outputs in a schrein lake (optional)

**Status:** exploration, revised — the first draft blurred the ledger and
catalog roles (it moved the ledger into the catalog); this version keeps
them strictly separate
**Depends on:** `cas-catalog-library.md` (the schrein spec), specifically
its M0–M2 milestones shipping as a usable package

## 0. The two roles, stated first

Nothing in this document changes what each component is the authority
for. Catalog mode adds exactly one new interaction (§1); everything else
stays where it is.

| Component | Authority over | Changed here? |
|---|---|---|
| **schrein catalog** (per flow) | The lake: table schemas, schema evolution, snapshots, and the **file list that defines each table's contents** | New — this is what gets added |
| **`ledger.parquet`** | Run bookkeeping: run status, timestamps, commit, params | No — byte-for-byte as today |
| **`_meta.json`** | Per-run progress: `steps_complete`, `fanout_keys`, resume provenance | No — resume still reads this, not the catalog |
| **`_metrics*.parquet`** | Run telemetry (and, via its flush cadence, run liveness) | No — plain files in run folders, as today |

One point that the first draft failed to make explicit, and that
everything below rests on: **a lakehouse catalog is not only a schema
store.** In DuckLake, the catalog also holds each table's file list —
that is the core lakehouse mechanic. A Parquet file on S3 is not part of
any table until a catalog commit registers it. So a catalog *inherently*
answers "which files are in table X right now"; that is lake metadata,
not run tracking. Wherever this document talks about visibility, it is
this mechanism at work — never the catalog knowing anything about runs.

## 1. What catalog mode adds

```
s3://bucket/<flow_root>/
  ledger.parquet              <- unchanged (CAS, run bookkeeping)
  catalog.ducklake            <- NEW: schrein register, the lake for this flow
  run_id=<id>/_meta.json      <- unchanged
  run_id=<id>/_metrics*.parquet        <- unchanged
  run_id=<id>/<step>/part-N.parquet    <- unchanged (immutable, boto3-written)
```

- **One catalog per flow**, holding **one table per step**, rows across
  runs distinguished by `run_id`. Genesis (first run of the flow)
  creates the tables from the existing `Table` DDL.
- **The one new interaction:** when a step completes, the orchestrator
  registers that step's part files into the step's lake table — one
  metadata-only CAS commit via `ducklake_add_data_files`, which reads
  only Parquet footers. This is schrein §5.3's preferred
  "register, don't write" pattern verbatim, and it is the *only* thing
  ionflow ever asks the catalog to do during a run (plus `ALTER` for
  explicit migrations, §3.3).
- **Data writing is unchanged.** `write_part` casts, constraint-scans
  locally, uploads via boto3 (ADRs 0013 and 0003 stand). DuckLake never
  writes step data; ionflow never uses `INSERT INTO`. Fan-out workers
  are untouched — they upload parts and report keys, as today; only the
  orchestrator talks to the catalog.
- **Run tracking is unchanged.** Ledger start/end upserts, per-step
  `_meta.json` flushes, resume (0002/0008 — copy origin parts, which in
  catalog mode also means registering the copies under the new run_id),
  the staleness heuristic: all exactly as today. Resume decisions read
  `_meta.json`, never the catalog.
- **Reads move to the catalog** (in catalog mode): `query.duck()`
  attaches the catalog read-only instead of building glob views;
  `scan_runs`' bucket LIST and the footer-based drift check become
  catalog reads. Same SQL surface (`query`/`stats`/TUI unchanged). The
  `ledger` and `metrics` views keep their current non-catalog plumbing.

Commit rate: one CAS commit per completed step (plus one-time genesis).
Writers on a flow's register = concurrent runs of that flow. Even ten
concurrent runs of a ten-step flow stay far below schrein's
commits-per-minute comfort zone (§3/§9 there), and the closures commute
(different run_ids), so rebases replay cleanly.

## 2. Consequences of registration (not new run semantics)

Because a file is only *in* a table once registered, and a step's parts
are registered in one commit at completion:

- A half-finished fan-out is invisible **to lake readers** — its
  uploaded parts are unreferenced S3 objects, not table contents. Today
  readers glob paths, so partially written fan-outs are physically
  visible, and the runner defends with the "complete only when ALL
  parts written" invariant plus lazily-surfacing read errors. Those
  defenses remain for classic mode; catalog-mode readers simply never
  encounter the problem.
- A crashed run's unregistered parts are orphans; schrein's grace-TTL
  GC reaps them. Its *registered* steps remain queryable under that
  run_id — same as today, where a failed run's written parts are
  globbable and the ledger says `failed`. Status still comes from the
  ledger; the catalog holds no opinion on it.
- Crash windows: parts uploaded but not registered → step recomputed on
  resume (as today, via meta); registered but meta not flushed → resume
  recomputes under a new run_id and the origin's rows sit in the table
  like any failed run's output. No new inconsistency class; the catalog
  and meta can only skew in the same benign way parts and meta can
  today.

## 3. What ionflow gains

1. **Vanilla readers.** Anyone with stock DuckDB attaches the flow's
   catalog `READ_ONLY` from a notebook — no ionflow install, no key
   scheme knowledge, typed schemas included (Frozen-DuckLake pattern).
   Today consumers need `query.py` or the layout conventions. This is
   the headline feature. (They see the data tables; run status stays an
   ionflow concern — the ledger is not lake data.)
2. **Discovery becomes one GET.** Run/step enumeration for queries and
   the drift check stop scaling with bucket size; typed empty views
   come free (tables exist from genesis), deleting that special case in
   `query.py`.
3. **Schema evolution across history — the genuinely new capability,
   and squarely schrein's department.** Today a schema change affects
   only new runs; old runs drift, and the drift check can only refuse.
   A DuckLake schema change is a metadata-only catalog operation
   (schrein §5.6): an additive migration applies across all historic
   runs in one commit, no files rewritten. For long-lived incremental
   keyed flows (0008-style `Annotate`), that turns "schema change =
   start over" into "schema change = one `ionflow migrate` commit."
   Drift detection can then *propose* a migration instead of refusing.
4. **Clean read-side visibility** (§2) — partial fan-outs and
   half-written state can't reach lake readers.
5. **Principled retention.** Run deletion = `DELETE WHERE run_id` +
   snapshot expiry + grace-TTL GC, giving `ionflow clean` real
   semantics for stable flows; snapshots give as-of time travel.

## 4. What it costs

- **A new load-bearing dependency** (schrein + the ducklake extension)
  on the read/query path of catalog-mode flows — though notably *not*
  on the run-bookkeeping path, which stays ledger-only.
- **Version coupling.** Catalog format and ducklake extension versions
  couple the orchestrators writing one flow (schrein open question 2).
- **Test matrix.** Runner behaviors need classic and catalog coverage —
  the largest real cost. Mitigation: catalog mode is experimental at
  first; its integration tests mirror the resume matrix and are marked
  `slow`.
- **Commit latency per step** — one CAS round-trip, seconds worst-case
  under rebase; negligible against step runtimes.
- **Catalog blast radius.** A corrupt/clobbered register breaks *reads*
  for that flow until restored (bucket versioning) or rebuilt — data
  files and the ledger are unaffected, and `ionflow catalog backfill`
  (§6) re-registers everything from the run folders. Per-flow catalogs
  keep this contained.

## 5. Rejected variant: the ledger as a catalog table

The first draft proposed moving `ledger.parquet` (and `_meta.json`)
into the catalog, so that file registration and run status commit in
one atomic transaction. Rejected:

- It puts run bookkeeping inside the lake schema — exactly the
  role-conflation the schrein spec's §1.1 scope guard exists to
  prevent, and a standing source of confusion for anyone reading either
  spec.
- The atomicity it buys is marginal: §2 shows catalog/meta skew is the
  same benign class as today's parts/meta skew.
- It would put the ducklake extension on the bookkeeping path of every
  run, and prototype flows (which stay classic — schema churn would be
  migration noise, and `reset_on_conflict` has no catalog analogue)
  would need the classic ledger anyway, forcing two bookkeeping
  implementations.

The ledger stays a separate CAS object. (Reimplementing `ledger.py` on
schrein's `CasRegister` remains a possible mechanical refactor — same
mechanism, roles untouched.)

## 6. Optionality

- **`RunStore` seam** is unnecessary in this revision — since run
  bookkeeping doesn't move, the integration surface shrinks to: a
  `Catalog` hook the runner calls at genesis and step completion
  (no-op in classic mode), and a backend switch in `query.py` /
  `discovery.py` for the read side.
- **Config:** `[storage] catalog = "none" | "ducklake"` at project
  level; each run records the mode in its meta. Prototype flows are
  always classic.
- **`ionflow catalog backfill <flow>`:** registers pre-existing classic
  runs into a fresh catalog (footer reads, one commit) — both the
  migration path and the corruption-recovery path.
- **Dependency:** `ionflow[ducklake]` optional extra pulling schrein.

## 7. Open questions

1. **run_id materialization.** Register files with a catalog-side
   partition value (preferred; needs `ducklake_add_data_files`
   partition support — schrein open question 3) vs run_id as a real
   column in each part (breaks 0002's cheap server-side resume copies,
   which would carry the origin's run_id). The I0 spike settles this.
2. **Catalog granularity.** Per-flow (recommended: contention
   isolation, 0004 alignment, small blast radius) vs per-project (one
   attach for the whole lake). Multi-ATTACH over per-flow catalogs
   gives project-wide *querying* without cross-flow commit coupling;
   revisit only if that UX proves painful.
3. **`shaped()` columns:** catalog DDL must use `persisted_type()`
   (plain lists), the same read-side rule views and drift follow today.
4. **moto viability.** The catalog attach is a local file
   (download-then-attach), so moto only serves the register blob and
   data parts — both already proven — but ducklake-under-moto needs the
   I0 spike before any test-suite commitment.
5. **`ionflow migrate`.** §3.3 deserves its own small spec once the
   substrate exists: which `Table` diffs map to which `ALTER`s, and how
   drift output proposes them.
6. **Publish (0009).** Unchanged — a copy with a sidecar. Noted only:
   within a catalog, publish-as-reference into a stable table becomes
   safe (GC keeps referenced files), but that would be a new numbered
   decision superseding 0009, not a silent change.

## 8. Sequencing

- **Prereq:** schrein M0–M2 (register, DuckLake adapter, GC) shipped.
- **I0 — spike (throwaway):** moto + ducklake +
  `ducklake_add_data_files` on boto3-written parts with a partition
  value; settles §7.1 and §7.4.
- **I1 — catalog hook + genesis + per-step registration** behind the
  config flag; linear flows; `query.py`/`discovery.py` catalog read
  path.
- **I2 — fan-out + resume + `backfill`:** registration of copied parts
  on resume; exit = classic and catalog modes pass the same `-m ""`
  suite.
- **I3 — retention + `ionflow migrate`:** expiry/GC wiring for
  `clean`; the migration workflow (§3.3, §7.5).
