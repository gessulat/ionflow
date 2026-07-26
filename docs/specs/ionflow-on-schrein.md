# Spec exploration: ionflow running *on* schrein (optional catalog mode)

**Status:** exploration — not committed to; written to scope the idea honestly
**Depends on:** `cas-catalog-library.md` (the schrein spec), specifically its
M0–M2 milestones shipping as a usable package
**Relation to schrein §1.1:** this does not merge the ledger and the catalog
roles — it moves the run ledger *into* a catalog as an ordinary table

## 0. Summary and recommendation

Today ionflow's only planned schrein integration is publish targets
(schrein M4): runs live in immutable run folders, tracked by a CAS-swapped
`ledger.parquet`, and schrein manages a *separate* lake you publish into.

This document asks: what would it take for a flow's runs to live in a
schrein-managed DuckLake catalog directly — step outputs as catalog
tables, the run ledger as a catalog table — and is it worth it?

Conclusion in one paragraph: it is architecturally a surprisingly good
fit, because ionflow already does write-data-first (workers upload
Parquet via boto3; completion is recorded afterwards), which is exactly
schrein's preferred "register, don't write" commit pattern — the catalog
only ever ingests metadata-only commits, and workers don't change at all.
The wins are real (atomic step completion, vanilla-DuckDB readers,
one-GET discovery, schema migration across historic runs, principled
retention). The costs are real too (a new dependency, a doubled test
matrix for runner/resume, version coupling to the ducklake extension).
Recommendation: **build it as an optional backend behind a `RunStore`
seam, per-flow catalogs, stable flows only**, sequenced after schrein
M2, starting with a de-risking spike (§8, I0). Do not make it the
default until it has survived the full resume matrix and a real
workload.

## 1. What "runs on schrein" means precisely

Catalog mode changes *where run metadata lives and how completion becomes
visible*. It deliberately changes almost nothing about how data is written.

```
s3://bucket/<flow_root>/
  catalog.ducklake            <- schrein CAS register, one per flow
  run_id=<id>/<step>/part-N.parquet   <- unchanged: immutable, boto3-written
  run_id=<id>/_metrics*.parquet       <- unchanged: outside the catalog
```

- **One catalog per flow**, at the flow's existing S3 root. This follows
  flow identity = namespace (ADR 0004), bounds CAS contention to
  concurrent runs of *one* flow, and keeps the blast radius of a corrupt
  catalog to one flow. Cross-flow queries use DuckDB multi-ATTACH (a
  helper in `query.py`), not a shared register.
- **One catalog table per step**, rows across runs distinguished by
  `run_id` (partition value or column — open question §7.1). Plus a
  `runs` table replacing `ledger.parquet`, and a small `fanout_keys`
  table replacing that part of `_meta.json`.
- **Data files are written exactly as today**: `write_part` casts,
  constraint-scans locally, uploads via boto3 (ADR 0013 untouched).
  DuckLake never writes step data; ionflow never uses `INSERT INTO
  lake.step`. The catalog ingests files via `ducklake_add_data_files` —
  metadata-only, reads one Parquet footer, trivially replayable, which is
  schrein §5.3's preferred pattern verbatim.
- **Only the orchestrator commits.** Fan-out workers keep uploading
  their branch parts and reporting keys back, exactly as now. The
  multi-writer story the CAS protocol must handle is *concurrent runs of
  the same flow*, not workers.
- **Commit points per run:** run start (genesis/ensure tables from the
  `Table` DDL + insert `runs` row `running`), each step completion
  (register that step's part files + update the run's progress), run end
  (status `complete`/`failed`). That is S+2 CAS commits per run versus 2
  today — still deep inside schrein's commits-per-minute comfort zone
  (§4).
- **Metrics stay outside the catalog.** The 60s flush is the run
  heartbeat and must never be a CAS commit (rate, and a wedged CAS must
  not kill the heartbeat). `_metrics.parquet` keeps its current
  plain-overwrite path; staleness detection is unchanged. Optionally the
  final metrics file is registered at run end so `metrics` is queryable
  through the catalog.

## 2. What ionflow gains

1. **Atomic step completion — kills a whole class of invariants.**
   Today "a fan-out counts as completed only when ALL parts are written"
   is a carefully maintained bookkeeping invariant, missing parts
   surface lazily inside steps (CLAUDE.md gotcha), and completion state
   is smeared across parts + `_meta.json` + ledger row. In catalog mode
   a step is visible iff its registration commit landed; a partial
   fan-out is *structurally* invisible (unregistered files don't exist
   to readers); "registered" and "resumable" are the same fact; and
   status + progress + file list update in one atomic multi-table
   transaction — the exact property DuckLake exists to provide.
2. **Vanilla readers.** Anyone with stock DuckDB attaches the flow's
   catalog `READ_ONLY` from a notebook — no ionflow install, no
   knowledge of the key scheme, typed schema included
   (Frozen-DuckLake reader pattern). Today consumers need `query.py` or
   the layout conventions. This is arguably the headline feature.
3. **Discovery becomes one GET.** `scan_runs` (bucket LIST) and the
   drift check (one Parquet footer per step) collapse into reading the
   catalog. `ls`/TUI latency stops scaling with bucket size. Typed
   empty views come free (tables exist from run start), deleting the
   empty-view special case in `query.py`.
4. **Schema evolution across history — a genuinely new capability.**
   Today a schema change only affects new runs; old runs drift. DuckLake
   schema changes are metadata-only catalog operations (schrein §5.6):
   an additive migration (`ALTER TABLE ADD COLUMN ... DEFAULT`) applies
   across all historic runs at once, atomically, without rewriting a
   single Parquet file. For long-lived incremental keyed workloads
   (0008-style `Annotate` flows) this is the difference between
   "schema change = start over" and "schema change = one commit."
5. **Principled retention.** Run deletion becomes `DELETE WHERE run_id`
   + snapshot expiry + schrein GC with grace TTL, instead of ad-hoc
   prefix deletes; `ionflow clean` gets real semantics for stable flows.
6. **Time travel.** Catalog snapshots give as-of-commit queries over the
   whole flow state for free.

## 3. What it costs

- **A new load-bearing dependency** (schrein + ducklake extension) on
  the correctness path of every run in catalog mode.
- **Version coupling.** The catalog file format and ducklake extension
  version couple all orchestrators that write one flow (schrein open
  question 2). Orchestrator-only commits keep the exposed surface small,
  but a mixed-version fleet needs the refuse-older-writers rule.
- **Test matrix.** Every runner/resume behavior needs classic-mode and
  catalog-mode coverage. This is the largest real cost. Mitigation:
  catalog mode enters as experimental; its integration tests are marked
  `slow` like the resume matrix they mirror.
- **Commit latency per step** — a CAS round-trip, seconds worst-case
  under a rebase. Negligible against real step runtimes.
- **Catalog blast radius.** One corrupt/clobbered catalog object breaks
  reads for that flow (data files remain, and are re-registerable via
  backfill). Mitigations: per-flow catalogs, bucket versioning, and the
  fact that data was never writable through the catalog path.

## 4. Contention budget

Writers on one register = concurrent runs of one flow, each committing
S+2 times over minutes-to-hours. Even ten concurrent runs of a
ten-step flow average well under one commit per minute against a
KB–MB catalog; rebase closures are metadata-only replays that commute
(different run_ids). This fits schrein §3/§9 with orders of magnitude of
headroom. If a pathological flow (hundreds of very fast steps, many
concurrent runs) ever hits contention, the schrein-side log-structured
upgrade (§9) applies before any ionflow redesign does.

## 5. Failure modes (mapped against today)

| Failure | Today | Catalog mode |
|---|---|---|
| Crash after parts upload, before completion record | Parts exist, unrecorded → step recomputed on resume | Identical: files unregistered/invisible → recomputed; orphans reaped by GC after grace TTL |
| Partial fan-out at crash | Parts visible; invariant + lazy errors keep it non-resumable | Structurally invisible — nothing to defend |
| SIGKILL mid-run | Ledger stuck `running`; staleness from metrics heartbeat | Identical (heartbeat unchanged, outside catalog) |
| Failed run, some steps done | Resumable via meta `steps_complete` | Resumable via registered steps (same, one source of truth) |
| Two runs finish steps concurrently | Independent objects; ledger CAS at run end only | CAS rebase; commuting closures replay cleanly |
| Corrupt/clobbered register | Ledger lost → run listing gone, parts + meta survive | Catalog lost → reads gone until restore (bucket versioning) or `backfill` re-registration |

## 6. Module-by-module delta

- **New seam: `RunStore`** (the optionality mechanism, §7.2) —
  `begin_run`, `complete_step(step, part_keys)`, `finish_run(status)`,
  `runs()`, `parts(run_id, step)`, `step_schema(step)`, plus a
  query-attach hook. `ClassicStore` wraps today's `ledger.py` +
  `_meta.json` + LIST/glob machinery, behavior-identical. `DuckLakeStore`
  wraps `schrein.DuckLake.commit` with replayable closures.
- **`runner.py`** — `flush()`/`upsert()` calls become store calls; the
  per-step `flush()` becomes `complete_step` (in catalog mode: the
  registration commit). Workers, retries, sizing, metrics: untouched.
- **`ledger.py`** — becomes the guts of `ClassicStore`; unchanged
  otherwise. (Reimplementing it on `CasRegister` is an unrelated
  mechanical refactor.)
- **`query.py`** — `duck()` dispatches: classic = glob views (today),
  catalog = download-attach read-only, exposing the same view names +
  `ledger` view over the `runs` table. `metrics` view stays glob-based
  in both modes. Same SQL surface either way — `query`/`stats`/TUI
  don't notice.
- **`discovery.py`** — catalog mode replaces `scan_runs` LIST and the
  footer-based drift check with catalog reads; drift compares declared
  `Table` schema (via `persisted_type()` — the `shaped()` rule applies
  to catalog DDL exactly as it does to views) against catalog schema,
  and can now *offer* a migration instead of only refusing.
- **`storage.py`** — `write_part` unchanged (ADR 0013 and 0003 stand:
  constraints are enforced by our pre-upload scan; DuckLake would only
  check NOT NULL). `Layout` gains the catalog key.
- **Resume (ADR 0002, 0008)** — semantics preserved: copy origin parts
  into the new run folder (server-side copy, cheap), then register the
  copies under the new run_id. Keyed resume reads `fanout_keys` from the
  catalog and copies+registers per surviving item. A future
  "register-don't-copy" resume (sharing files across runs) becomes
  *safe* under catalog refcounting — GC only deletes unreferenced files
  — but changes run-deletion semantics; explicitly deferred, 0002
  stands.
- **`publish.py` (ADR 0009)** — unchanged: a copy with a sidecar. Noted
  for later: within a catalog, publish-as-reference into a stable table
  becomes safe for the same refcounting reason; that would be a new
  numbered decision superseding 0009's trade-off, not a silent change.
- **Prototype flows** — always classic mode. Scratch schema churn would
  translate into migration/table-drop noise in a catalog, and
  `reset_on_conflict` ledger resets have no clean catalog analogue.
  Catalog mode is for stable flows; this is a simplification, not a
  limitation.
- **`config.py`** — `[storage] catalog = "none" | "ducklake"` at the
  project level; each run records its mode in the `runs` row. New CLI:
  `ionflow catalog backfill <flow>` registers pre-existing classic runs
  into a fresh catalog (footer reads, one commit), which is both the
  migration path and the corruption-recovery path.

## 7. Open questions

1. **run_id materialization.** Options: (a) register files with a
   catalog-side partition value (needs `ducklake_add_data_files`
   partition support — schrein open question 3 already flags verifying
   this function's semantics); (b) write run_id as a real column into
   each part (breaks 0002's cheap server-side resume copy, since copies
   would carry the origin's run_id). (a) is strongly preferred; the I0
   spike must settle it.
2. **Catalog granularity.** Per-flow (recommended: contention isolation,
   0004 alignment, small blast radius) vs per-project (single attach for
   the whole lake — the original "datalake schema" vision). Multi-ATTACH
   gives per-project *querying* over per-flow catalogs; what it does not
   give is cross-flow atomic commits, which ionflow does not need.
   Revisit only if multi-attach UX proves painful.
3. **Transition state.** Does catalog mode maintain `ledger.parquet` as
   a read-only compat mirror during a deprecation window, or is
   `backfill` + a hard switch enough?
4. **moto viability.** The catalog attach is a local file (download-
   then-attach), so moto only serves the register blob and data parts —
   both already proven — but ducklake-extension-under-moto needs the I0
   spike before any test-suite commitment.
5. **`ionflow migrate`.** Schema evolution (§2.4) deserves its own small
   spec once the substrate exists: which `Table` diffs map to which
   `ALTER`s, and how drift-check output proposes them.

## 8. Sequencing

- **Prereq:** schrein M0–M2 (register, DuckLake adapter, GC) shipped.
- **I0 — spike (de-risk, throwaway):** moto + ducklake +
  `ducklake_add_data_files` on boto3-written parts with a partition
  value; settles §7.1 and §7.4. No merge.
- **I1 — `RunStore` seam:** pure refactor, classic behavior bit-for-bit,
  full suite green. Valuable on its own (it also isolates `ledger.py`
  for the eventual `CasRegister` swap).
- **I2 — `DuckLakeStore`, linear flows:** begin/complete/finish, query
  attach, drift-via-catalog, behind the config flag, experimental.
- **I3 — fan-out + keyed resume + `backfill`:** the full resume matrix
  in catalog mode; GC wiring; exit = classic and catalog modes pass the
  same `-m ""` suite.
- **I4 — retention + decisions:** `clean`/expiry semantics; separate
  ADRs for publish-as-reference (vs 0009) and register-don't-copy
  resume (vs 0002) — written only if wanted, defaults stand.
