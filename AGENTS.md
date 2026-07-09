# microflow — agent notes

Barebones Metaflow alternative. Flow = class, step = method returning a typed table,
persisted as Parquet on S3. Only infra: one S3-compatible bucket. DuckDB-first.
See README.md for user-facing docs.

## Testing (important)

```bash
uv run pytest              # default: fast set, ~1.6s (46 checks incl. doctests)
uv run pytest -m ""        # FULL suite, ~2.2s — run this before committing
uv run pytest -n 0 tests/test_x.py::test_y   # debug one test without xdist workers
```

- Tests run **parallel by default** (`-n 5 --dist worksteal`, set in pyproject).
- The 4 heaviest integration tests are marked `slow` and **skipped by default**:
  CLI end-to-end, process-pool run, and both resume tests. `-m ""` includes them.
  Anything touching `runner.py`, `cli.py`, or resume semantics MUST pass `-m ""`.
- Doctests live in modules and run automatically (`--doctest-modules` in addopts).
- S3 is faked with an in-process moto server (started+warmed at conftest import,
  never stopped — it's a daemon thread). Each test gets a unique bucket; no cleanup.
- `repo` fixture = session-scoped git repo, for tests that only *read* git.
  Use `clean_repo` (function-scoped) if the test mutates the repo or its cwd.
- The TUI has no automated tests (deliberate — human-tested). Don't add TUI tests.

## Commands

```bash
uv sync                       # install (uv-managed, Python >=3.12)
uv run microflow               # bare command opens the TUI
uv run microflow setup         # wizard: writes .microflow/config (TOML)
uv run microflow run examples.etl:ETLFlow
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
  `T | None` = nullable, `list[T]` = DuckDB array, `= pk` / `= check(...)` / `= field(...)`);
  compiles to DuckDB DDL + pyarrow schema. Column types must be importable at module
  scope (get_type_hints).
  `Table.make(name, cols, **constraints)` builds one programmatically (wide schemas).
  `FlowParams`: typed run inputs (inner `class Params`), int/float/str/bool only.
- `dag.py`     — DAG from method signatures: param name = upstream step; `Fanout`/`Branch`/
  `Collect` markers; validated at class definition.
- `flow.py`    — `Flow` base; subclasses auto-register in `Flow._registry`; the runner
  binds `self.config` and all params during a run. Params become ledger columns
  (written from run start); resume reuses the origin's params from `_meta.json`.
  `Prototype` mixin / `prototype = True`: scratch namespace (`_proto/`), dirty git
  allowed, untyped steps allowed, drift check skipped, `microflow clean` wipes it.
- `runner.py`  — topo execution, ProcessPool fan-out (workers get S3 keys, not data),
  per-branch resume, in-worker branch retries (`branch_retries`), git gate,
  run status (running|complete|failed).
- `metrics.py` — adaptive psutil sampler (rss/threads/cpu per step+branch); one
  `_metrics.parquet` per run, written by the orchestrator on success AND failure,
  flushed every 60s mid-run (heartbeat + crash-durable metrics).
- `storage.py` — S3 `Layout` (key scheme) + `parse_key` (its inverse), boto3 I/O,
  DuckDB connections. **Writes go through boto3, never DuckDB `COPY TO s3://`**
  (httpfs write path is unreliable); validation = temp table from DDL, then PUT.
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
