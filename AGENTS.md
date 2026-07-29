# ionflow — agent notes

Barebones Metaflow alternative. Flow = class, step = method returning a typed table,
persisted as Parquet on S3. Only infra: one S3-compatible bucket — or none:
local mode (`Config.local_dir`, `ionflow setup --local`) stores everything in a
directory via schrein's `LocalClient`, `ionflow sync` pushes a flow's history
to a bucket later (0016). DuckDB-first.
See `packages/ionflow/README.md` for user-facing docs. Design decisions (the *why*
behind non-obvious choices) live in `packages/ionflow/docs/decisions/` — check there
before changing architecture, add a numbered record when you make such a decision.

## Repo layout (uv workspace)

The repo root is a **workspace root only** — no package lives there (decision 0017).
`ionflow` and `schrein` are siblings under `packages/`, each with its own
`pyproject.toml`, dev group, test suite, README, `docs/decisions/` and LICENSE:

```
pyproject.toml            # [tool.uv.workspace] + repo-wide ruff/ty config, no [project]
packages/ionflow/         # src/, tests/, examples/, docs/{decisions,specs,screenshots}/
packages/schrein/         # src/, tests/, docs/{decisions,specs}/
```

`uv sync` at the root builds **one** environment holding both members and every
dev group, so `uv run` works from anywhere in the repo; `--directory` picks which
member's config (pytest, in particular) applies. Paths below are relative to the
repo root unless stated otherwise.

## Testing (important)

```bash
uv run --directory packages/ionflow pytest        # default: fast set (incl. doctests)
uv run --directory packages/ionflow pytest -m ""  # FULL suite — run this before committing
uv run --directory packages/ionflow pytest -n 0 tests/test_x.py::test_y   # debug one test without xdist workers
```

- There is **no root test suite** (decision 0017): always pass `--directory` (or
  `cd` into the package). A bare `uv run pytest` at the root is a mistake.
- Tests run **parallel by default** (`-n 5 --dist worksteal`, set in the member's
  pyproject).
- The heaviest integration tests are marked `slow` and **skipped by default**:
  CLI end-to-end, process-pool run, and every multi-run resume test (plain and
  keyed/incremental). `-m ""` includes them.
  Anything touching `runner.py`, `cli.py`, or resume semantics MUST pass `-m ""`.
- Doctests live in modules and run automatically (`--doctest-modules` in addopts).
- S3 is faked with a moto server **subprocess** (started+warmed at conftest import,
  killed atexit). Each test gets a unique bucket; no cleanup. It must NEVER run
  in-process: DuckDB httpfs relation binds (any `rel.filter(...)` in a step)
  starve an in-process werkzeug server and wedge runner tests for minutes.
- `repo` fixture = session-scoped git repo, for tests that only *read* git.
  Use `clean_repo` (function-scoped) if the test mutates the repo or its cwd.
- The TUI has no automated tests (deliberate — human-tested). Don't add TUI tests.
- **Never poll for the suite with `pgrep -f`.** The waiting shell's own command line
  contains the pattern, so `until ! pgrep -f "bin/pytest"; do sleep 15; done` matches
  ITSELF and spins forever — this has already burned hours. Wait on the harness's
  completion notification for the backgrounded command, or match a string a waiter
  cannot contain (`\.venv/bin/pytest`). Never hand-roll a poll loop over something
  the harness already reports.
- **Never test what the library guarantees.** Writer and reader both derive from the
  same `Table`, so a write-then-read-back assertion tests pyarrow/DuckDB, not ionflow.
  Test only where ionflow's own mapping DIFFERS from the pass-through — `_persist`
  (a fixed array round-trips as a plain list), the constraint scans, the numpy
  conversion. Likewise, `_LEAF`/`_SQL` are lookup tables: spot-check the kinds,
  never enumerate the dict.

## Commands

```bash
uv sync                       # install the whole workspace (uv-managed, Python >=3.12)
uv run ruff check             # lint + typecheck are repo-wide, run from the root
uv run ty check
uv build --all-packages       # both wheels into dist/
uv run ionflow               # bare command opens the TUI
uv run ionflow setup         # wizard: writes .ionflow/config (TOML); --local = bucket-free dir store
uv run ionflow sync myproj:Annotate --to s3://bucket   # push local-store history to a bucket
uv run ionflow run myproj:Annotate --resume latest # keyed resume from latest complete run
uv run ionflow publish myproj:Annotate <run_id> <step> blessed_table  # --to s3://bucket/prefix

# the bundled example flow lives in the ionflow package, so run it from there
uv run --directory packages/ionflow ionflow run examples.embed:EmbedDocs   # sizing: --max-workers/--threads/--branch-retries
uv run --directory packages/ionflow ionflow query examples.embed:EmbedDocs "SELECT * FROM summary"
uv run --directory packages/ionflow ionflow stats examples.embed:EmbedDocs # per-step resources, latest run
```

## Performance analysis (start here when optimizing a step)

Every run records CPU/memory for every step and branch — always on, stored as
`_metrics.parquet` in the run folder, exposed as the `metrics` SQL view.

```bash
ionflow stats myproj.flows:Training [run_id]   # per-step: seconds, peak_rss_mb,
                                                # avg_cores, max_threads, processes
ionflow query myproj.flows:Training \
  "SELECT ts, rss/1e6 AS mb FROM metrics WHERE run_id='...' AND step='features' ORDER BY ts"
```

- `metrics` columns: `ts, step, branch, rss, threads, cpu_user, cpu_system, run_id`.
  CPU values are cumulative per process — diff them over a window; effective
  cores = delta(cpu_user+cpu_system) / delta(wall). `query.step_stats()` does this.
- Sampling is adaptive: 1s at run start; at 1024 samples per process the buffer
  folds (peaks preserved) and the interval doubles, so long runs stay bounded.
- In process mode every task measures itself: whole-step tasks sample with
  `branch` NULL, item tasks label `branch` with the ITEM KEY (a string). In
  thread mode the orchestrator's sampler covers everything, labelling the
  union of active nodes.
- Failed runs keep their metrics — check them first when a run dies.
- Typical workflow: `stats` to find the expensive step -> compare `peak_rss_mb` /
  `avg_cores` across run_ids after a change -> time series via `metrics` for detail.

## Architecture (one line per module, packages/ionflow/src/ionflow/)

- `schema.py`  — `Table`/`LedgerColumns`: columns as class annotations (bare = NOT NULL,
  `T | None` = nullable, `list[T]` = DuckDB array, `= primary_key` / `= check(...)` /
  `= fanout_key` / `= field(...)` to combine);
  compiles to DuckDB DDL + pyarrow schema. Column types must be importable at module
  scope (get_type_hints). numpy scalar types work as annotations (np.float32 -> FLOAT —
  narrow dtypes persist); `shaped(dtype, *shape)` = fixed array, enforced by the write
  cast but round-tripped by parquet as a plain list — read-side comparisons (views,
  drift) must use `Column.persisted_type()`, not `duckdb_type()`.
  `Table.make(name, cols, **constraints)` builds one programmatically (wide schemas).
  `= fanout_key` (one str/int NOT NULL column) marks a `Fanout` item's identity
  for keyed resume — named to pair with `Fanout[...]`;
  `Table.fanout_key_column()` reads it back.
  `FlowParams`: typed run inputs (inner `class Params`), int/float/str/bool only.
- `dag.py`     — DAG from method signatures: param name = upstream step; `Fanout`/`Branch`/
  `Collect` markers; validated at class definition. Every branch step belongs to the
  `Group` of the fan-out rooting its chain (0015); `dag.plan` is the step order with
  each group contracted into its fan-out node, `dag.roots` maps a branch step to that
  fan-out (0014), and `group.deps` are the intra-item edges. The global topological
  order is internal — `dag.downstream(step)` is the one thing that reads it.
  `dag.schedule_graph()` is the runner's exact node graph: steps plus an
  `("items", f)` node per group, so a group's dependents wait for its items while
  the fan-out's emission doesn't wait on member-only inputs.
  Refused here: a fan-out inside a branch, a per-item step reading a sibling per-item
  step whole (`Collect`/plain instead of `Branch`), a `Branch[...]` over a step that is
  neither, a fan-in feeding back into its own fan-out (a cycle once contracted), and
  the reserved step names (`ledger`/`metrics`/`runs`/`manifest`) and columns
  (`run_id`, the views' filename column).
- `plan.py`    — `Planner`: the reuse policy of a resumed run as pure functions of
  (dag, origin manifest, live manifest) — `upfront()` / `node_reuse()` /
  `member_reuse()` and the cross-ancestor taint rule (`unchanged`); inert on a
  fresh run, unit-tested without a store (test_plan). `preview()` powers
  `run --resume X --dry-run` (per-step reuse-vs-compute, assuming unchanged
  work-lists).
- `flow.py`    — `Flow` base; subclasses auto-register in `Flow._registry`, keyed by
  `flow_id()` = module-qualified class name — also the S3 namespace (rename/move a
  class = new flow); the runner
  binds `self.config` and all params during a run. Params become ledger columns
  (written from run start); resume reuses the origin's params from its runs row.
  There is no prototype/scratch mode (0012): every flow carries the full
  guarantees — clean git, typed steps, a catalog — from its first run.
  `discover_flows(package)` imports a package (and its submodules) to register
  the flows it defines — registration is a side effect of importing.
- `runner.py`  — a scheduler and nothing else (0021): ONE pool per run (spawn
  processes or threads) executes every task — a whole plain/fan-out step, or one
  item's pending sub-steps — over `dag.schedule_graph()`, so independent chains
  run concurrently and NO user code runs in the orchestrator. Reuse verdicts come
  from `plan.Planner`; commit-drift refusal (`allow_commit_drift`), in-worker
  sub-step retries (`branch_retries`), git gate, run status
  (running|complete|failed). Step inputs are **lazy httpfs relations**
  (pushdown; memory = query working set, not input size) — always read with
  `hive_partitioning=false` or the path's `run_id=` leaks in as a column. Task
  DuckDBs are capped + spill `temp_directory`; shares are sized at submit from
  the tasks actually in flight (a solo step gets the whole machine). A fan-out
  counts as completed only when ALL parts are written (partial fan-out must not
  look resumable). The orchestrator connects before the pool spawns, so
  `INSTALL httpfs` never races across cold workers. Progress accumulates in a
  `Manifest` and lands via `flush()` — one `Catalog.commit_run` per node (plus
  begin/final), CAS-atomic with the data registration (0019); `_meta.json` is
  written after each flush, and BECAUSE the flush precedes scheduling a node,
  workers resolve every input from one meta snapshot per (process, node, phase)
  (`_node_manifest`) — the pool initializer carries only per-run constants, and
  task payloads a few keys. Cross-step `self` state is the declared contract
  exactly: params + `Ledger` columns, seeded into whole-step tasks and merged
  from their results (item tasks never carry `self` back).
  A step may branch over another branch step (`Branch[T]` names the upstream
  *step*), so a branch is a whole sub-DAG. It runs **per item, not per stage**
  (0015): one worker owns one item end to end (`_task_worker` ->
  `_run_sub_dag`), independent sub-steps run concurrently in threads (own
  DuckDB cursor each), and item A can be in the last sub-step while item B is on
  the first — so intermediates scale with `max_workers`, not the work-list.
  Members of one item are same-process by construction and may hand over local
  artifacts by path. Completion is per (item, sub-step): `branch_retries` retries
  one sub-step, resume restarts an item at its first incomplete one, and the
  failure path lets in-flight tasks settle and commits everything that completed.
  Resume NEVER copies (0020): `reuse_slots` records references to the origin's
  part objects. Keyed resume (0008/0018): if the fan-out's Table declares a
  `fanout_key`, resume re-runs the fan-out, reuses origin parts by item key —
  keys name parts down the whole chain (0014), so reuse is a manifest lookup,
  never positional — and reuses downstream steps only when the key set is
  unchanged.
  `resume=<run_id>|"latest"` is the single resume spelling (0008), CLI `run --resume`;
  "latest" = newest complete run, falling through fresh when none exists;
  `run --resume X --dry-run` previews reuse-vs-compute without running.
- `state.py`   — the run's state as types, orchestrator-side only (nothing here crosses
  the process boundary). `Slot(step, key)` names one part by its ITEM KEY (0018 —
  position ints for unkeyed work, fanout_key values for keyed); `Manifest` maps
  Slot -> its part object (O(1) membership, `width`, `keys`/`paths` in SORTED key
  order — the one registration order, so reducers stay deterministic across
  resumes and reordered work-lists; serializes to meta pairs and lake rows);
  `RunRecord` renders the lake `runs` row (params/custom as JSON — the table
  never migrates) and the `_meta.json` recovery payload. `BASE_COLS` reserves
  the runs columns against params/Ledger names.
- `metrics.py` — adaptive psutil sampler (rss/threads/cpu per step+branch). The
  orchestrator writes `_metrics.parquet` (success AND failure; its 60s flush is the
  run heartbeat); in process mode every task flushes its own object —
  `_metrics.<step>.parquet` for whole-step tasks (branch NULL),
  `_metrics_<fanout>_<key>.parquet` for items (crash-durable, one per item, the
  worker covers its whole sub-DAG); the `metrics` view unions all of them.
  Sub-steps running concurrently in one worker share one label
  (`upload_db+search`) and NULL means the worker is between sub-steps; in thread
  mode the orchestrator's sampler labels the union of active nodes (a
  single-member group answers to the member's name).
- `store.py`   — one object store: bytes by key plus the DuckDB wiring to read them.
  `Store.of(cfg)` is **the only place local-vs-S3 is decided** (`S3Store` /
  `DirStore` over boto3 or schrein's `LocalClient`); it holds the one client,
  mints fresh connections (`connect`/`local_connection`, never memoized), and
  builds DuckLakes (`lake`). `uri()`/`key_for_uri()` are the key<->URI pair.
  `s3_client` stays module-level — the documented side-input hatch.
- `space.py`   — `Space`: one flow's region of a store — the key scheme
  (`part`/`meta`/`metrics`/`run_ids`) and the verbs over those keys. A part is
  named by its ITEM KEY (`part_<key_slug>.parquet`, 0018) — `key_slug` passes
  clean keys verbatim and sanitizes+hashes the rest; the manifest, never the
  filename, is the authoritative key->part mapping.
  Identity is the **`flow_id` string, never a class** (branch workers re-import the
  flow module under a loader name). Frozen dataclass, no I/O to construct, so build
  one wherever; the *store* is the per-command/per-worker object.
  `write_part` streams: cast-projection COPY
  to a local temp file (zstd) on a **fresh local connection**, one constraint
  scan (NOT NULL / CHECK / PK) of that file, then `Store.upload` (multipart above
  8MB — parts may exceed S3's 5GB single-PUT cap); `dest` may be a callable of
  the extracted fanout key (keyed items name themselves; a duplicate key raises
  before anything uploads). **Writes go through boto3,
  never DuckDB `COPY TO s3://`** (a failed httpfs write finalizes anyway, leaving
  a corrupt part visible — 0010). A COPY pipeline must NEVER read
  httpfs (deadlocks on partially consumed sources); relations are drained
  through `to_arrow_reader` instead. Violations raise `ConstraintViolation`.
  Also per-run `_meta.json` (`read_meta`/`write_meta` — the recovery export,
  read only by `backfill`), and `sync_to` (local -> S3 migration: copy run
  folders, then `lake.backfill` re-derives the whole target catalog — ledger
  rows included — from the copied metas; a DuckLake catalog holds absolute
  paths, so it is never copied). Timestamps are naive UTC (`utc_now`;
  tz-aware parquet needs pytz in DuckDB).
- `arrow.py`   — pure marshalling, no store knowledge.
  `to_arrow` auto-converts numpy step returns (dict of ndarrays — 2D+ nest as
  fixed-size lists — structured arrays, bare arrays mapped onto declared columns;
  complex rejected). `StepInput` wraps every step input: delegates to the lazy
  relation, adds `.np()` (materializes; ragged/NULL columns raise). numpy itself
  is optional in core (guarded imports), a dev dep for tests.
- `lake.py`    — both DuckLakes ionflow keeps. `Catalog`, every flow's catalog
  (0012/0019 — **the run's ONE metadata authority**): at the flow root, one data
  table per step PLUS the fixed-DDL system tables `runs` (the ledger;
  params/custom as JSON text) and `manifest` (run -> (step, item_key, path)).
  `commit_run` is the one write: genesis + runs-row upsert + manifest inserts +
  data-file registration of the NEWLY CREATED parts among them (a path inside
  the committing run's folder means "created here"; anything else is a
  reference, 0020) — one CAS commit, replay-safe (`_apply` skips rows that
  already landed). Registration failure fails the run. Reads: `runs()` and
  `run_state()` (row + manifest off one snapshot; "latest" = newest complete).
  DuckLake inlines the tiny system rows into the register artifact — no
  small-file churn in `_lake/`. `backfill()` / `ionflow catalog-backfill`
  restores EVERYTHING (rows, manifests, registrations, failed runs included)
  from the run folders' `_meta.json` records in one commit — the recovery path,
  and how `sync` derives the target catalog. No schema-migration workflow,
  deliberately: schema change = new flow name, or backfill --replace when old
  parts conform. `schema_drift()` compares declared columns against the catalog.
  And `publish()`: bless one step's output from a complete run into the publish
  lake (default `<prefix>/published/`, `--to` for any bucket) — atomic replace
  in one CAS commit, snapshot history instead of --force, `publications`
  provenance table (the `Publication` Table), optional `--where` filter. Still
  a copy into the lake's data path, never a pointer (0009); the parts come from
  the run's manifest, wherever they live.
- `query.py`   — `duck()`: one view per step — `read_parquet` over the parts the
  manifest names, joined to their referencing run on the `filename` column
  (0020; `lake.<step>` itself is the corpus: each part once, under its creating
  run) — plus the `ledger` view (`ledger_sql`: base runs columns + params/Ledger
  columns CAST out of their JSON storage against the flow's declarations) and
  `metrics`; typed empty views for steps with no parts. `run_id` is always the
  last view column. Refuses a bucket with runs but no catalog, pointing at
  catalog-backfill.
- `config.py`  — `.ionflow/config` (TOML) found by walking up like `.git`;
  `IONFLOW_*` env vars override; credentials only from the AWS chain, never stored.
  Exactly one of `bucket` / `local_dir` (relative `local_dir` anchors at the repo root).
- `cli.py` / `tui.py` — typer CLI; Textual TUI (bare `ionflow` launches it).
  The TUI owns its own startup helpers: `scan_runs` (one store LIST for flows
  with a catalog) and `startup_warnings` (orphaned data + `lake.schema_drift`).

## Conventions & gotchas

- Deps via `uv add` only, and name the member it belongs to:
  `uv add --package ionflow <dep>` (`--dev` for the dev group). Core deps:
  duckdb, boto3, pyarrow, typer, textual — **no pandas/polars in core**
  (steps may return anything arrow-convertible). ruff and ty are the exception:
  repo-wide tooling, so they live in the root's dev group.
- Runs refuse a dirty git tree, without exception. Tests always create/commit
  a repo first.
- Untyped steps (no return annotation) are a class-definition error.
- Python docs: a one-liner, plus short prose only for non-obvious constraints;
  doctests for examples (they run in the suite).
- Run folders on S3 are immutable (`_meta.json` is the exception — the run
  rewrites its own recovery record); the catalog register is the only
  CAS-mutated object. A part is stored once, by the run that computed it;
  other runs reference it through their manifests (0020) — never delete a run
  folder without checking `lake.manifest` for references.
- Backblaze B2 is unsupported (no If-Match conditional writes).
- A run killed by SIGKILL stays `running` in the ledger forever (can't catch it),
  but `ls`/TUI derive `running (stale?)` once its heartbeat (the 60s metrics flush)
  has been quiet for 300s; its metrics survive up to the last flush.

## schrein (sibling workspace package, `packages/schrein/`)

Sibling library: zero-infrastructure DuckLake catalogs on S3 conditional
writes (DuckLake only — schrein decision 0004), or on a plain directory via
`LocalClient` — a boto3-shaped client with enforced conditional writes,
single-host by design (schrein decision 0005). Spec:
`packages/schrein/docs/specs/cas-catalog-library.md`; status: M0 (CAS core) +
M1 (DuckLake adapter) + M2 (expire/gc/probe). ionflow depends on schrein for
one thing — `DuckLake` under the per-flow catalog and the publish lake
(0012/0019; the old separate ledger CAS died with 0019); schrein must never
import ionflow. Its decisions live in `packages/schrein/docs/decisions/`
(own numbering).

- Test it with `uv run --directory packages/schrein pytest` (fast set) or `-m ""`
  (full, incl. the CAS-contention hammer). Nothing collects it alongside
  ionflow's suite.
- Its tests launch `schrein.testing`, NOT bare moto: moto's
  conditional PUTs race under threads and silently lose updates
  (schrein decision 0003).
- The ducklake extension loads from the `duckdb-extension-ducklake` PyPI
  wheel when `INSTALL` can't reach extensions.duckdb.org (decision 0002);
  the wheel pins its exact duckdb version, so upgrade both together.
