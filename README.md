# miniflow

A barebones [Metaflow](https://metaflow.org) alternative. A **flow is a class**, a
**step is a method**, and every step returns a **typed table** that miniflow persists
as Parquet on S3. No Kubernetes, no scheduler, no metadata server — the only thing you
configure is one S3-compatible bucket.

- **The DAG is the method signatures.** A step's parameters name its upstream steps.
  No `self.next(...)`.
- **Schemas are declared, enforced, and queryable.** Each output table is a class;
  DuckDB validates types / `NOT NULL` / `CHECK` on write and exposes every run as SQL.
- **Runs are logged in an append-only ledger** in the bucket: `run_id`, `status`
  (`running` / `complete` / `failed`), `started`, `finished`, git `commit`,
  `steps_complete`, plus any flow-specific columns.
- **Fan-out / fan-in** over N inputs, in parallel, on one machine.
- **`resume`** re-runs a failed run, reusing every step that already completed —
  including individual branches of a partially-failed fan-out.
- **Clean git required.** miniflow refuses to start with a dirty working tree, so every
  run is pinned to a commit.

## Install

```bash
uv add miniflow    # or: pip install miniflow
```

## Configure (the only setup)

Run the wizard once per repo:

```bash
miniflow setup
```

It asks for your bucket (and optional prefix / endpoint / region) and writes them to
`.miniflow/config` at the repo root — a small TOML file, no secrets in it. It also asks
whether to gitignore `.miniflow/` (keep it local) or commit it (share with your team).
Re-run `miniflow setup` any time to change it.

**Credentials** come from the standard AWS chain (environment variables, `~/.aws/credentials`,
or an IAM role) and are never written to the repo. `miniflow setup` just checks they resolve
and that the bucket is reachable. Any command guides you into `setup` if the repo isn't
configured yet.

```
# .miniflow/config
bucket   = "my-bucket"
prefix   = "miniflow"
endpoint = "https://…"   # omit for AWS S3; set for MinIO / R2
region   = "us-east-1"
```

## A complete flow

Load rows, **fan out** by group, score each group **in parallel**, then **fan in** to a
summary. Note there is no wiring code — `scored` depends on `parts` purely because it has
a parameter named `parts`.

```python
import pyarrow as pa
from miniflow import Branch, Collect, Fanout, Flow, LedgerColumns, Table, check, pk


# --- Output schemas: annotations, compiled to DuckDB DDL on write ---
# A bare type is NOT NULL; `T | None` is nullable. Constraints attach as
# defaults: `= pk`, `= check("...")`, `= field(...)`.
class Raw(Table):
    id: int = pk
    grp: int
    value: float


class Part(Table):
    id: int
    grp: int
    value: float


class GroupScore(Table):
    grp: int
    total: float = check("total >= 0")


class Summary(Table):
    n_groups: int
    grand_total: float


class ETLFlow(Flow):
    # Extra ledger columns, populated during the run (small metadata only).
    class Ledger(LedgerColumns):
        n_rows: int

    def raw(self) -> Raw:                                    # root step (only `self`)
        return pa.table(
            {"id": [1, 2, 3, 4], "grp": [0, 0, 1, 1], "value": [1.0, 2.0, 3.0, 4.0]}
        )

    def parts(self, raw: Raw) -> Fanout[Part]:              # fan-out: one table per group
        groups = [r[0] for r in raw.project("grp").distinct().order("grp").fetchall()]
        return [raw.filter(f"grp = {g}") for g in groups]

    def scored(self, parts: Branch[Part]) -> GroupScore:   # runs once per branch, in parallel
        return parts.aggregate("grp, sum(value) AS total")

    def summary(self, scored: Collect[GroupScore], raw: Raw) -> Summary:   # fan-in
        self.n_rows = raw.aggregate("count(*) AS c").fetchone()[0]         # -> ledger column
        return scored.aggregate("count(*) AS n_groups, sum(total) AS grand_total")
```

Step inputs arrive as **DuckDB relations**, so you can stay in SQL (`.aggregate`,
`.filter`, `.project`) or drop to a DataFrame with `.pl()` / `.df()` / `.to_arrow_table()`
and return any of those.

## Run it

```python
run_id = ETLFlow.run()          # reads .miniflow/config (or pass config=Config(...))
```

or from the shell:

```bash
miniflow run mymodule:ETLFlow
```

## Query every run as one database

`duck()` returns a DuckDB connection with a **view per step table** plus a `ledger` view.
Views glob every run, so a new run shows up with nothing to sync — `run_id` is a column.

```python
con = ETLFlow.duck()
con.sql("SELECT run_id, grand_total FROM summary ORDER BY run_id").show()
con.sql("SELECT grp, total FROM scored WHERE run_id = ? ", params=[run_id]).show()
con.sql("SELECT run_id, started, finished, n_rows FROM ledger").show()
```

```bash
miniflow query mymodule:ETLFlow "SELECT * FROM summary"
miniflow ls mymodule:ETLFlow
```

## Browse in the terminal (TUI)

Bare `miniflow` (no subcommand) opens an interactive terminal UI:

```bash
miniflow                       # or: miniflow tui myproject.flows
```

- one **tab per Flow** in your project (point `miniflow setup` at your `flows` package —
  every `Flow` subclass auto-registers on import),
- each tab is a table of that flow's runs: `run_id`, `status`, `started`, `finished`, **plus
  that flow's custom ledger columns**,
- `Tab` cycles flows; `j`/`k`/PageUp/PageDown move the row cursor,
- **Enter** drills into a run to preview each step's output table; **Esc** goes back.

On startup it re-discovers your flows and warns if a flow's declared table schema has drifted
from what's stored on S3, or if there's run data on S3 with no matching Flow in code. If the
repo isn't configured yet, the TUI opens a setup screen first.

## Resume

```bash
miniflow resume mymodule:ETLFlow <origin_run_id>
```

A **new** `run_id` is created; completed step outputs are copied from the origin run
(server-side, no re-compute) and execution restarts at the first incomplete step. A
partially-failed fan-out re-runs only its failed branches.

## Storage layout

```
s3://<bucket>/<prefix>/<FlowName>/
  ledger.parquet                           # append-only run ledger (updated via S3 CAS)
  runs/
    run_id=<id>/
      _meta.json                           # live run state (progress, custom columns)
      raw/part_0.parquet
      parts/part_0.parquet  parts/part_1.parquet
      scored/part_0.parquet scored/part_1.parquet
      summary/part_0.parquet
```

Run folders are immutable, which is why no catalog database is needed — DuckDB reads the
Parquet directly and the ledger uses an S3 conditional-write (`If-Match`) for safe
concurrent appends.

## Notes & limits

- **DuckDB-first**, DataFrame-optional. polars/pandas are conveniences, not dependencies.
- **One level of fan-out** (kept deliberately small).
- **Backends**: AWS S3, MinIO (recent), Cloudflare R2. Backblaze B2 is unsupported (no
  `If-Match` conditional writes).
- Set custom ledger columns on non-branch (main-process) steps; branch steps run in
  separate processes and their `self` is not carried back.
