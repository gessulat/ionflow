# miniflow — agent notes

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
uv run miniflow               # bare command opens the TUI
uv run miniflow setup         # wizard: writes .miniflow/config (TOML)
uv run miniflow run examples.etl:ETLFlow
uv run miniflow query examples.etl:ETLFlow "SELECT * FROM summary"
```

## Architecture (one line per module, src/miniflow/)

- `schema.py`  — `Table`/`LedgerColumns`: columns as class annotations (bare = NOT NULL,
  `T | None` = nullable, `= pk` / `= check(...)` / `= field(...)`); compiles to DuckDB DDL
  + pyarrow schema. Column types must be importable at module scope (get_type_hints).
- `dag.py`     — DAG from method signatures: param name = upstream step; `Fanout`/`Branch`/
  `Collect` markers; validated at class definition.
- `flow.py`    — `Flow` base; subclasses auto-register in `Flow._registry`.
- `runner.py`  — topo execution, ProcessPool fan-out (workers get S3 keys, not data),
  per-branch resume, git gate, run status (running|complete|failed).
- `storage.py` — S3 `Layout` (key scheme) + `parse_key` (its inverse), boto3 I/O,
  DuckDB connections. **Writes go through boto3, never DuckDB `COPY TO s3://`**
  (httpfs write path is unreliable); validation = temp table from DDL, then PUT.
- `ledger.py`  — append-only `ledger.parquet` via S3 conditional-write (If-Match) CAS;
  per-run `_meta.json`. Timestamps are naive UTC (tz-aware parquet needs pytz in DuckDB).
- `query.py`   — `duck()`: one view per step (hive `run_id=` partition surfaces as a
  column) + `ledger` view; typed empty views for steps with no parts.
- `discovery.py` — import a flows package to register flows; `scan_runs` (one bucket
  LIST); schema-drift check (one parquet footer per step).
- `config.py`  — `.miniflow/config` (TOML) found by walking up like `.git`;
  `MINIFLOW_*` env vars override; credentials only from the AWS chain, never stored.
- `cli.py` / `tui.py` — typer CLI; Textual TUI (bare `miniflow` launches it).

## Conventions & gotchas

- Deps via `uv add` only. Core deps: duckdb, boto3, pyarrow, typer, textual —
  **no pandas/polars in core** (steps may return anything arrow-convertible).
- Runs refuse a dirty git tree (no override). Tests always create/commit a repo first.
- Python docs: doctests only, no prose docstring conventions beyond a one-liner.
- Run folders on S3 are immutable; the ledger is the only mutated object (CAS).
- Backblaze B2 is unsupported (no If-Match conditional writes).
- A run killed by SIGKILL stays `running` in the ledger forever (can't catch it).
