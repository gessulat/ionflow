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
- Branch rows (`branch IS NOT NULL`) exist only for process-mode fan-outs;
  thread-mode branches are covered by the orchestrator's samples.
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
  each group contracted into its fan-out node, and `group.deps` the intra-item edges.
  Refused here: a fan-out inside a branch, a per-item step reading a sibling per-item
  step whole (`Collect`/plain instead of `Branch`), a `Branch[...]` over a step that is
  neither, and a fan-in feeding back into its own fan-out (a cycle once contracted).
- `flow.py`    — `Flow` base; subclasses auto-register in `Flow._registry`, keyed by
  `flow_id()` = module-qualified class name — also the S3 namespace (rename/move a
  class = new flow); the runner
  binds `self.config` and all params during a run. Params become ledger columns
  (written from run start); resume reuses the origin's params from `_meta.json`.
  There is no prototype/scratch mode (0012): every flow carries the full
  guarantees — clean git, typed steps, a catalog — from its first run.
- `runner.py`  — topo execution, ProcessPool fan-out (workers get S3 keys, not data),
  per-branch resume (refuses commit drift unless `allow_commit_drift`),
  in-worker branch retries (`branch_retries`), git gate,
  run status (running|complete|failed). Step inputs are **lazy httpfs relations**
  (pushdown; memory = query working set, not input size) — always read with
  `hive_partitioning=false` or the path's `run_id=` leaks in as a column. Worker
  DuckDBs are capped (`threads`, `memory_limit` ~ 80% RAM / workers) + spill
  `temp_directory`. A fan-out counts as completed only when ALL parts are written
  (partial fan-out must not look resumable). The orchestrator connects before
  spawning workers, so `INSTALL httpfs` (one-time download per duckdb version)
  never races across cold workers. Missing/unreadable parts now surface lazily
  inside the step or at write time (duckdb `IOException`), not at input binding.
  A step may branch over another branch step (`Branch[T]` names the upstream
  *step*), so a branch is a whole sub-DAG. It runs **per item, not per stage**
  (0015): one worker owns one item end to end (`_run_items` -> `_branch_worker`
  -> `_run_sub_dag`), independent sub-steps run concurrently in threads (own
  DuckDB cursor each), and item A can be in the last sub-step while item B is on
  the first — so intermediates scale with `max_workers`, not the work-list.
  Members of one item are same-process by construction and may hand over local
  artifacts by path. Completion is per (item, sub-step): `branch_retries` retries
  one sub-step, resume restarts an item at its first incomplete one, and
  sub-steps complete for every item are registered even when a later one fails.
  Keyed resume (0008): if the fan-out's Table declares a `fanout_key`, resume
  re-runs the fan-out, copies origin branch parts by item key (positions may
  shift), and reuses downstream steps only when the key set is unchanged; item
  keys per fan-out are recorded as `fanout_keys` in `_meta.json`. Item keys name
  every branch step down a chain, not just the first (0014) — chained steps are
  key-matched too, never positionally reused.
  `resume=<run_id>|"latest"` is the single resume spelling (0008), CLI `run --resume`;
  "latest" = newest complete run, falling through fresh when none exists.
- `publish.py` — bless one step's output from a complete run into the publish
  lake (a schrein DuckLake, default `<prefix>/published/`, `--to` for any
  bucket): atomic replace in one CAS commit, snapshot history instead of
  --force, `publications` provenance table, optional `--where` filter.
  Still a copy into the lake's data path, never a pointer (0009).
- `metrics.py` — adaptive psutil sampler (rss/threads/cpu per step+branch). The
  orchestrator writes `_metrics.parquet` (success AND failure; its 60s flush is the
  run heartbeat); process-mode item workers each flush their own
  `_metrics_<fanout>_<branch>.parquet` (crash-durable per-item samples — one object
  per item, the worker covers its whole sub-DAG); the `metrics` view unions all of
  them. Sub-steps running concurrently in one worker share one label
  (`upload_db+search`) and NULL means the worker is between sub-steps; in thread
  mode the group answers to the fan-out's name (one process interleaves items).
- `store.py`   — one object store: bytes by key plus the DuckDB wiring to read them.
  `Store.of(cfg)` is **the only place local-vs-S3 is decided** (`S3Store` /
  `DirStore` over boto3 or schrein's `LocalClient`); it holds the one client,
  mints fresh connections (`connect`/`local_connection`, never memoized), and
  builds CAS registers (`cas`) and DuckLakes (`lake`). `uri()`/`key_for_uri()`
  are the key<->URI pair; `copy_many` is a **generator** (a failed copy leaves
  the already-yielded prefix as real resume progress). `s3_client` stays
  module-level — the documented side-input hatch, re-exported by the
  `storage.py` shim, which is all that module still is.
- `space.py`   — `Space`: one flow's region of a store — the key scheme both ways
  (`part`/`meta`/`metrics`/`ledger` <-> `parse_part`) and the verbs over those keys.
  Identity is the **`flow_id` string, never a class** (branch workers re-import the
  flow module under a loader name). Frozen dataclass, no I/O to construct, so build
  one wherever; the *store* is the per-command/per-worker object.
  `write_part` streams: cast-projection COPY
  to a local temp file (zstd) on a **fresh local connection**, one constraint
  scan (NOT NULL / CHECK / PK) of that file, then `Store.upload` (multipart above
  8MB — parts may exceed S3's 5GB single-PUT cap). **Writes go through boto3,
  never DuckDB `COPY TO s3://`** (a failed httpfs write finalizes anyway, leaving
  a corrupt part visible — 0010). A COPY pipeline must NEVER read
  httpfs (deadlocks on partially consumed sources); relations are drained
  through `to_arrow_reader` instead. Violations raise `ConstraintViolation`.
  Also the ledger (append-only `ledger.parquet` via `upsert_runs`/`amend`, whose
  closure rebuilds the whole parquet from `old` on every CAS attempt — 0011, roles
  stay separate: the ledger is run bookkeeping, never a catalog), per-run
  `_meta.json` (`read_meta`/`write_meta`), and `sync_to` (local -> S3 migration:
  copy immutable run folders, MERGE ledger rows idempotently by run_id, rebuild
  the target catalog via `catalog.backfill` — a DuckLake catalog holds absolute
  paths, so it is re-derived, never copied). Timestamps are naive UTC (`utc_now`;
  tz-aware parquet needs pytz in DuckDB).
- `arrow.py`   — pure marshalling, no store knowledge.
  `to_arrow` auto-converts numpy step returns (dict of ndarrays — 2D+ nest as
  fixed-size lists — structured arrays, bare arrays mapped onto declared columns;
  complex rejected). `StepInput` wraps every step input: delegates to the lazy
  relation, adds `.np()` (materializes; ragged/NULL columns raise). numpy itself
  is optional in core (guarded imports), a dev dep for tests.
- `catalog.py` — every flow's DuckLake catalog (0012 — the only
  read/registration mode; no config flag): at the flow root, one table per
  step, run_id inferred from the parts' hive paths at registration (parts
  never rewritten). One CAS commit per completed step by the orchestrator;
  registration failure fails the run. `duck()` views an attached snapshot
  (run_id kept last) and refuses a pre-catalog bucket, pointing at
  `ionflow catalog-backfill` (the corruption-recovery path; there is no
  other read path). No schema-migration workflow, deliberately: schema
  change = new flow name, or backfill --replace when old parts conform.
  Ledger/meta/metrics untouched.
- `query.py`   — `duck()`: one view per step (hive `run_id=` partition surfaces as a
  column) + `ledger` view; typed empty views for steps with no parts.
- `discovery.py` — import a flows package to register flows; `scan_runs` (one bucket
  LIST); schema-drift check (one parquet footer per step).
- `config.py`  — `.ionflow/config` (TOML) found by walking up like `.git`;
  `IONFLOW_*` env vars override; credentials only from the AWS chain, never stored.
  Exactly one of `bucket` / `local_dir` (relative `local_dir` anchors at the repo root).
- `cli.py` / `tui.py` — typer CLI; Textual TUI (bare `ionflow` launches it).

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
- Run folders on S3 are immutable; the ledger is the only mutated object (CAS).
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
two things — `CasRegister` under the ledger (0011) and `DuckLake` under
the catalog (0012); schrein must never import ionflow. Its decisions live
in `packages/schrein/docs/decisions/` (own numbering).

- Test it with `uv run --directory packages/schrein pytest` (fast set) or `-m ""`
  (full, incl. the CAS-contention hammer). Nothing collects it alongside
  ionflow's suite.
- Its tests launch `schrein.testing`, NOT bare moto: moto's
  conditional PUTs race under threads and silently lose updates
  (schrein decision 0003).
- The ducklake extension loads from the `duckdb-extension-ducklake` PyPI
  wheel when `INSTALL` can't reach extensions.duckdb.org (decision 0002);
  the wheel pins its exact duckdb version, so upgrade both together.
