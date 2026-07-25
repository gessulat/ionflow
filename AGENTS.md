# microflow — agent notes

Barebones Metaflow alternative. Flow = class, step = method returning a typed table,
persisted as Parquet on S3. Only infra: one S3-compatible bucket. DuckDB-first.
See README.md for user-facing docs. Design decisions (the *why* behind
non-obvious choices) live in `docs/decisions/` — check there before changing
architecture, add a numbered record when you make such a decision.

## Testing (important)

```bash
uv run pytest              # default: fast set (incl. doctests)
uv run pytest -m ""        # FULL suite — run this before committing
uv run pytest -n 0 tests/test_x.py::test_y   # debug one test without xdist workers
```

- Tests run **parallel by default** (`-n 5 --dist worksteal`, set in pyproject).
- The heaviest integration tests are marked `slow` and **skipped by default**:
  CLI end-to-end, process-pool run, and every multi-run resume test (classic and
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
uv sync                       # install (uv-managed, Python >=3.12)
uv run microflow               # bare command opens the TUI
uv run microflow setup         # wizard: writes .microflow/config (TOML)
uv run microflow run examples.etl:ETLFlow
uv run microflow run myproj:Annotate --incremental   # keyed resume from latest complete run
uv run microflow query examples.etl:ETLFlow "SELECT * FROM summary"
uv run microflow stats examples.etl:ETLFlow          # per-step resources, latest run
```

## Performance analysis (start here when optimizing a step)

Every run records CPU/memory for every step and branch — always on, stored as
`_metrics.parquet` in the run folder, exposed as the `metrics` SQL view.

```bash
microflow stats myproj.flows:Training [run_id]   # per-step: seconds, peak_rss_mb,
                                                # avg_cores, max_threads, processes
microflow query myproj.flows:Training \
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

## Architecture (one line per module, src/microflow/)

- `schema.py`  — `Table`/`LedgerColumns`: columns as class annotations (bare = NOT NULL,
  `T | None` = nullable, `list[T]` = DuckDB array, `= primary_key` / `= check(...)` / `= field(...)`);
  compiles to DuckDB DDL + pyarrow schema. Column types must be importable at module
  scope (get_type_hints). numpy scalar types work as annotations (np.float32 -> FLOAT —
  narrow dtypes persist); `shaped(dtype, *shape)` = fixed array, enforced by the write
  cast but round-tripped by parquet as a plain list — read-side comparisons (views,
  drift) must use `Column.persisted_type()`, not `duckdb_type()`.
  `Table.make(name, cols, **constraints)` builds one programmatically (wide schemas).
  `field(branch_key=True)` (one str/int NOT NULL column) marks a fan-out item's
  identity for keyed resume; `Table.branch_key_column()` reads it back.
  `FlowParams`: typed run inputs (inner `class Params`), int/float/str/bool only.
- `dag.py`     — DAG from method signatures: param name = upstream step; `Fanout`/`Branch`/
  `Collect` markers; validated at class definition.
- `flow.py`    — `Flow` base; subclasses auto-register in `Flow._registry`, keyed by
  `flow_id()` = module-qualified class name — also the S3 namespace (rename/move a
  class = new flow); the runner
  binds `self.config` and all params during a run. Params become ledger columns
  (written from run start); resume reuses the origin's params from `_meta.json`.
  `Prototype` mixin / `prototype = True`: scratch namespace (`_proto/`), dirty git
  allowed, untyped steps allowed, drift check skipped, `microflow clean` wipes it.
- `runner.py`  — topo execution, ProcessPool fan-out (workers get S3 keys, not data),
  per-branch resume (refuses commit drift unless `allow_commit_drift`; prototypes
  warn), in-worker branch retries (`branch_retries`), git gate,
  run status (running|complete|failed). Step inputs are **lazy httpfs relations**
  (pushdown; memory = query working set, not input size) — always read with
  `hive_partitioning=false` or the path's `run_id=` leaks in as a column. Worker
  DuckDBs are capped (`threads`, `memory_limit` ~ 80% RAM / workers) + spill
  `temp_directory`. A fan-out counts as completed only when ALL parts are written
  (partial fan-out must not look resumable). The orchestrator connects before
  spawning workers, so `INSTALL httpfs` (one-time download per duckdb version)
  never races across cold workers. Missing/unreadable parts now surface lazily
  inside the step or at write time (duckdb `IOException`), not at input binding.
  Keyed resume (0008): if the fan-out's Table declares a `branch_key`, resume
  re-runs the fan-out, copies origin branch parts by item key (positions may
  shift), and reuses downstream steps only when the key set is unchanged; item
  keys per fan-out are recorded as `branch_keys` in `_meta.json`.
  `incremental=True` (CLI `run --incremental`) = resume from latest complete run.
- `metrics.py` — adaptive psutil sampler (rss/threads/cpu per step+branch). The
  orchestrator writes `_metrics.parquet` (success AND failure; its 60s flush is the
  run heartbeat); process-mode branch workers each flush their own
  `_metrics_<step>_<branch>.parquet` (crash-durable branch samples); the `metrics`
  view unions all of them.
- `storage.py` — S3 `Layout` (key scheme) + `parse_key` (its inverse), boto3 I/O,
  DuckDB connections. **Writes go through boto3, never DuckDB `COPY TO s3://`**
  (httpfs write path is unreliable). `write_part` streams: cast-projection COPY
  to a local temp file (zstd) on a **fresh local connection**, one constraint
  scan (NOT NULL / CHECK / PK) of that file, then `upload_file` (multipart —
  parts may exceed S3's 5GB single-PUT cap). A COPY pipeline must NEVER read
  httpfs (deadlocks on partially consumed sources); relations are drained
  through `to_arrow_reader` instead. Violations raise `ConstraintViolation`.
  `to_arrow` auto-converts numpy step returns (dict of ndarrays — 2D+ nest as
  fixed-size lists — structured arrays, bare arrays mapped onto declared columns;
  complex rejected). `StepInput` wraps every step input: delegates to the lazy
  relation, adds `.np()` (materializes; ragged/NULL columns raise). numpy itself
  is optional in core (guarded imports), a dev dep for tests.
- `ledger.py`  — append-only `ledger.parquet` via S3 conditional-write (If-Match) CAS;
  per-run `_meta.json`. Timestamps are naive UTC (tz-aware parquet needs pytz in DuckDB).
- `query.py`   — `duck()`: one view per step (hive `run_id=` partition surfaces as a
  column) + `ledger` view; typed empty views for steps with no parts.
- `discovery.py` — import a flows package to register flows; `scan_runs` (one bucket
  LIST); schema-drift check (one parquet footer per step).
- `config.py`  — `.microflow/config` (TOML) found by walking up like `.git`;
  `MICROFLOW_*` env vars override; credentials only from the AWS chain, never stored.
- `cli.py` / `tui.py` — typer CLI; Textual TUI (bare `microflow` launches it).

## Conventions & gotchas

- Deps via `uv add` only. Core deps: duckdb, boto3, pyarrow, typer, textual —
  **no pandas/polars in core** (steps may return anything arrow-convertible).
- Runs refuse a dirty git tree (no override for stable flows; `Prototype` flows run
  dirty and record `dirty:<sha>`). Tests always create/commit a repo first.
- Untyped steps (no return annotation) are a class-definition error except in
  `Prototype` flows, where the parquet schema is inferred at write time.
- Python docs: a one-liner, plus short prose only for non-obvious constraints;
  doctests for examples (they run in the suite).
- Run folders on S3 are immutable; the ledger is the only mutated object (CAS).
- Backblaze B2 is unsupported (no If-Match conditional writes).
- A run killed by SIGKILL stays `running` in the ledger forever (can't catch it),
  but `ls`/TUI derive `running (stale?)` once its heartbeat (the 60s metrics flush)
  has been quiet for 300s; its metrics survive up to the last flush.
