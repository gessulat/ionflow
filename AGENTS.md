# ionflow — agent notes

Barebones Metaflow alternative. Flow = class, step = method returning a typed
table, persisted locally as Parquet. An optional S3 peer shares metadata;
archival payload transfers are explicit (0026/0027); execution reuse may download
a declared output under the run policy (0032). DuckDB-first.
User docs: `packages/ionflow/README.md`. The *why* behind non-obvious choices
lives in `packages/ionflow/docs/decisions/` (cited below by number) — check it
before changing architecture; add a record when you make such a decision.

Run layout: `<prefix>/runs/<flow_id>/{catalog.ducklake,_lake/,run_id=<id>/}`.
Shared files and bundles live under `<prefix>/artifacts/` (0022–0025). No legacy
layout compatibility or migration: development stores can be recreated.

## Repo layout (uv workspace)

The repo root is a **workspace root only** — no package lives there (0017).
`ionflow` and `schrein` are siblings under `packages/`, each with its own
`pyproject.toml`, dev group, test suite, README and `docs/decisions/`. `uv sync`
at the root builds ONE environment holding both members and every dev group, so
`uv run` works from anywhere; `--directory` picks which member's config
(pytest, in particular) applies. Paths below are repo-root-relative.

## Testing (important)

```bash
uv run --directory packages/ionflow pytest        # default: fast set (incl. doctests)
uv run --directory packages/ionflow pytest -m ""  # FULL suite — run before committing
uv run --directory packages/ionflow pytest -n 0 tests/test_x.py::test_y  # debug, no xdist
```

- No root test suite (0017): always pass `--directory` (or `cd` into the
  package); tests run parallel by default (`-n 5 --dist worksteal`).
- `slow`-marked integration tests (CLI end-to-end, process-pool, every
  multi-run resume test) are skipped by default; `-m ""` includes them.
  Anything touching `runner.py`, `cli.py`, or resume semantics MUST pass `-m ""`.
- S3 is faked with a moto server **subprocess** (started at conftest import;
  unique bucket per test, no cleanup). NEVER in-process: DuckDB httpfs relation
  binds starve an in-process werkzeug server and wedge runner tests for minutes.
- `repo` fixture = session-scoped git repo for tests that only *read* git; use
  `clean_repo` (function-scoped) if the test mutates the repo or its cwd.
- The TUI has no automated tests (deliberate — human-tested). Don't add any.
- **Never poll for the suite with `pgrep -f`** — the waiting shell's own command
  line matches the pattern and spins forever (has burned hours); wait on the
  harness's completion notification instead.
- **Never test what the library guarantees.** Writer and reader derive from the
  same `Table`, so write-then-read-back tests pyarrow/DuckDB, not ionflow. Test
  only where ionflow's mapping DIFFERS from pass-through (`_persist`, constraint
  scans, numpy conversion); spot-check lookup tables, never enumerate them.
- Pure policy (resume reuse) is tested in `test_plan.py` over plain Manifests —
  extend there first, not with another moto run.

## Commands

```bash
uv sync                       # install the workspace (uv-managed, Python >=3.12)
uv run ruff check && uv run ty check   # lint + typecheck: repo-wide, from the root
uv build --all-packages       # both wheels into dist/
uv run ionflow                # bare command opens the TUI
uv run ionflow setup          # wizard -> .ionflow/config (TOML); --local = dir store
uv run ionflow sync myproj:Annotate --to s3://bucket
uv run ionflow run myproj:Annotate --resume latest   # keyed/incremental resume
uv run ionflow run myproj:Annotate --resume latest --dry-run  # reuse-vs-compute preview
uv run ionflow publish myproj:Annotate <run_id> <step> blessed_table

# the bundled example flow lives in the ionflow package, so run it from there
uv run --directory packages/ionflow ionflow run examples.embed:EmbedDocs   # also: query, stats
```

## Performance analysis (start here when optimizing a step)

Every run records CPU/memory for every step and item — always on, exposed as
the `metrics` view (`ts, step, branch, rss, threads, cpu_user, cpu_system, process_id,
run_id`); `ionflow stats <flow> [run_id]` summarizes it per step.

- CPU values are cumulative per process — diff over a window; effective cores =
  delta(cpu) / delta(wall). `query.step_stats()` does this.
- Adaptive sampling: 1s at start; at 1024 samples the buffer folds (peaks
  preserved) and the interval doubles. Process mode: every task samples itself
  (`branch` = item key, NULL for whole-step tasks); thread mode: the
  orchestrator labels active nodes.
- Failed runs keep their metrics — check them first when a run dies.

## Architecture (packages/ionflow/src/ionflow/)

- `artifact.py` — `Artifact` schemas/references and `ArtifactInputs`; `Artifacts`
  owns shared inventory, SHA-256 blobs, bundle descriptors, revision-checked
  metadata, verified cache materialization, sync and recovery. Declared identity
  fields replace content identity for interchangeable builds (0032); file manifests
  remain pinned per run. Artifact metadata uses strict Pydantic validation. Explicit stable
  kinds are independent of Python paths. Run bindings pin metadata snapshots.
- `provenance.py` — thread-local `ArtifactRuntime` records use and publication;
  workers report activities and the orchestrator commits them with run progress.
  Table steps retain artifact ancestry; branch scope stays item-specific. Resume
  retains original producers. Flow catalogs override portable producer evidence.

- `schema.py` — `Table`/`LedgerColumns`/`FlowParams`: columns as class
  annotations (bare = NOT NULL, `T | None` nullable, `list[T]` array,
  `= primary_key` / `= check(...)` / `= fanout_key` / `= field(...)`); compiles
  to DuckDB DDL + arrow schema; `Table.make()` builds one programmatically.
  FlowParams is a strict Pydantic model with JSON-portable nested values.
  numpy scalars work as annotations (np.float32 -> FLOAT, narrow dtypes
  persist); `shaped(...)` = fixed array enforced at write but round-tripped as
  a plain list — read-side comparisons must use `persisted_type()`, never
  `duckdb_type()`. `fanout_key` marks a `Fanout` item's identity (0008).
- `dag.py` — DAG from method signatures (param name = upstream step;
  `Fanout`/`Branch`/`Collect` markers), validated at class definition. Branch
  steps belong to the `Group` of the fan-out rooting their chain (0014/0015);
  `schedule_graph()` is the runner's node graph (steps + one `("items", f)`
  node per group). Refuses: nested fan-out, a per-item step reading a sibling
  whole, fan-in feeding its own fan-out, reserved step names
  (`ledger`/`metrics`/`runs`/`manifest`) and columns (`run_id`, the views'
  filename column).
- `plan.py` — `Planner`: the reuse policy of a resumed run as pure functions
  of (dag, origin manifest, live manifest); inert on a fresh run; `preview()`
  powers `--dry-run`.
- `flow.py` — `Flow` base; subclasses register under `flow_id()` =
  module-qualified class name, ALSO the S3 namespace (rename/move a class = new
  flow). No scratch mode (0012). `discover_flows` imports a package's flows.
- `composition.py` — `@subflow(Child, output="step")` and `Call[OutputSchema]`
  declare child nodes (0030). The runner records each child's parent before
  execution; resume finds attempts through the parent's origin chain. Children
  keep independent manifests and producers. Browser registration includes them.
- `runner.py` — a scheduler and nothing else (0021): one pool per run (spawn
  processes or threads) executes every task — a whole plain/fan-out step, or
  one item's pending sub-steps — so independent chains run concurrently and NO
  user code runs in the orchestrator. Reuse comes from `Planner`; progress
  accumulates in a `Manifest` and lands via one `Catalog.commit_run` per node,
  CAS-atomic with data registration (0019); `_meta.json` is exported after each
  flush, and BECAUSE the flush precedes scheduling, workers resolve every input
  from one meta snapshot per (process, node, phase). Step inputs are lazy
  httpfs relations — always read with `hive_partitioning=false` or the path's
  `run_id=` leaks in as a column. Task DuckDBs are capped + spill; shares are
  sized from tasks actually in flight (a solo step gets the whole machine).
  A branch is a whole sub-DAG run **per item, not per stage** (0015): one
  worker owns one item end to end, independent sub-steps concurrent in threads,
  same-process by construction (local artifacts hand over by path);
  `branch_retries` retries one sub-step, resume restarts an item at its first
  incomplete one. A fan-out completes only when ALL parts are written. Resume
  NEVER copies (0020); keyed resume matches by item key down the whole chain
  (0014/0018), reusing downstream steps only when the key set is unchanged;
  `resume=<run_id>|"latest"` is the single spelling (falls through fresh).
  Cross-step `self` state = params + artifact inputs + `Ledger` columns (item tasks
  never carry `self` back).
- `state.py` — `Slot(step, key)`: a part named by its ITEM KEY (0018 —
  position ints unkeyed, fanout_key values keyed); `Manifest`: Slot -> path,
  `keys`/`paths` SORTED (the one registration order — deterministic reducers);
  `RunRecord` renders the `runs` row (params/custom as JSON — the table never
  migrates) and the `_meta.json` recovery payload.
- `metrics.py` — adaptive psutil sampler. Orchestrator writes
  `_metrics.parquet` (its 60s flush is the run heartbeat); process-mode tasks
  flush their own objects (`_metrics.<step>` / `_metrics_<fanout>_<key>`); the
  `metrics` view unions all. Concurrent sub-steps share one label
  (`upload_db+search`); NULL = between steps.
- `storage.py` — `Storage` coordinates a mandatory local store and optional peer.
  Metadata refresh merges logical records without reading outputs or registering
  absent files. Explicit transfer plans pin revisions, direction and bytes.
  Runs have one execution origin; divergent artifact edits require resolution.
  Parent transfer selections include child results and resume history (0031);
  one approval pins versions across their separate flow catalogs.
- `store.py` — `Store.of(cfg)` selects one physical local/S3 endpoint; fresh
  connections, never memoized; builds DuckLakes. `s3_client` stays
  module-level — the documented side-input hatch.
- `space.py` — one flow's key scheme + verbs. Parts are
  `part_<key_slug>.parquet` (slug legible, manifest authoritative). Identity is
  the `flow_id` STRING, never a class (workers re-import under a loader name).
  `write_part` streams: COPY to a local temp file on a fresh local connection,
  one constraint scan, then boto3 upload (multipart >8MB) — **writes never go
  through DuckDB `COPY TO s3://`** (a failed httpfs write leaves a corrupt part
  visible, 0010), and a COPY pipeline must NEVER read httpfs (deadlock);
  relations drain through `to_arrow_reader`. `sync_to` delegates to the shared
  transfer planner. Timestamps: naive UTC (`utc_now`).
- `arrow.py` — pure marshalling: `to_arrow` (numpy dicts/structured/bare
  arrays; complex rejected), `StepInput[Schema]` (lazy relation + `.np()` and
  `.partition_by(column)` for ordered, lazy fan-out partitions; 0033); numpy
  optional in core.
- `lake.py` — both DuckLakes. `Catalog` = the run's ONE metadata authority
  (0019): per-step data tables + fixed-DDL `runs` (the ledger) and `manifest`
  system tables; `commit_run` = genesis + row upsert + manifest inserts +
  registration of NEWLY CREATED parts (own-folder path = created here, else a
  reference, 0020), one replay-safe CAS commit. `backfill()` restores
  everything from `_meta.json` records and metadata-only `_inventory` snapshots
  retained during transfer and eviction — the recovery path, and how `sync`
  derives its target. No schema-migration workflow, deliberately (new flow name
  or `backfill --replace`). `publish()` blesses a step's output into the
  publish lake (atomic replace, snapshot history, `publications` provenance).
- `query.py` — `duck()`: one view per step (`read_parquet` over manifest parts
  joined to their referencing run on the filename column; `lake.<step>` is the
  corpus, each part once under its creator) + `ledger` (params/Ledger columns
  CAST out of JSON against the declarations) + `metrics`; typed empty views;
  `run_id` always last. Refuses runs-without-catalog, pointing at
  catalog-backfill.
- `config.py` — `.ionflow/config` (TOML), found by walking up; `IONFLOW_*` env
  overrides; credentials only from the AWS chain. A local directory is required;
  `remote` is optional. Bucket-only configs describe low-level transfer endpoints.
- `cli.py` / `tui.py` — typer CLI; Textual TUI (bare `ionflow`); TUI startup =
  `scan_runs` (flows with a catalog) + `startup_warnings` (schema drift).

## Conventions & gotchas

- Deps via `uv add --package ionflow <dep>` (`--dev` for dev group). Core:
  duckdb, boto3, pyarrow, typer, textual — **no pandas/polars in core**;
  ruff/ty are repo-wide (root dev group).
- Runs refuse a dirty git tree, without exception. Tests create/commit a repo first.
- Untyped steps (no return annotation) are a class-definition error.
- Python docs: one-liner + short prose for non-obvious constraints only;
  doctests for examples (they run in the suite).
- Run folders are immutable (`_meta.json` excepted — the run rewrites its own
  recovery record); the catalog register is the only CAS-mutated object. A part
  is stored once by the run that computed it; others reference it (0020) —
  never delete a run folder without checking `lake.manifest` for references.
- Backblaze B2 is unsupported (no If-Match conditional writes).
- A SIGKILL'd run stays `running` in the ledger forever, but `ls`/TUI derive
  `running (stale?)` once its heartbeat has been quiet for 300s.

## schrein (sibling package, `packages/schrein/`)

Zero-infrastructure DuckLake catalogs on S3 conditional writes (DuckLake
only, schrein 0004), or a plain directory via `LocalClient` (single-host,
schrein 0005); spec in `packages/schrein/docs/specs/`. ionflow depends on
schrein for ONE thing — `DuckLake` under the per-flow catalog and the publish
lake and artifact registry (0012/0019/0023); schrein must never import ionflow.

- Test with `uv run --directory packages/schrein pytest` (`-m ""` = full, incl.
  the CAS-contention hammer). Nothing collects it alongside ionflow's suite.
- Its tests launch `schrein.testing`, NOT bare moto: moto's conditional PUTs
  race under threads and silently lose updates (schrein 0003).
- The ducklake extension loads from the `duckdb-extension-ducklake` PyPI wheel
  when `INSTALL` can't reach extensions.duckdb.org (schrein 0002); the wheel
  pins its duckdb version — upgrade both together.

## Declared identity (0032)

Artifact `identity=(...)` resolves by exact name from typed inputs, parameters, or
artifact dependencies. Lookup precedes consumed-input payload validation. Runs
accept `reuse="auto"|"local"|"never"`; never bypasses resume and child reuse.
Auto authorizes verified output downloads without changing browsing defaults.
Different platforms may produce different files under one ID. Transfers must use
run-pinned manifests, not substitute the preferred artifact payload.
