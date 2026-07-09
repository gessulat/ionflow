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

```bash
export MINIFLOW_BUCKET=my-bucket
export MINIFLOW_ENDPOINT=https://…   # omit for AWS S3; set for MinIO / R2
export AWS_ACCESS_KEY_ID=…
export AWS_SECRET_ACCESS_KEY=…
```

## A complete flow

Load rows, **fan out** by group, score each group **in parallel**, then **fan in** to a
summary. Note there is no wiring code — `scored` depends on `parts` purely because it has
a parameter named `parts`.

```python
import pyarrow as pa
from miniflow import Branch, Collect, Column, Fanout, Flow, LedgerColumns, Table


# --- Output schemas: one class per table, compiled to DuckDB DDL on write ---
class Raw(Table):
    id = Column(int, primary_key=True)
    grp = Column(int, nullable=False)
    value = Column(float, nullable=False)


class Part(Table):
    id = Column(int)
    grp = Column(int)
    value = Column(float)


class GroupScore(Table):
    grp = Column(int, nullable=False)
    total = Column(float, check="total >= 0")


class Summary(Table):
    n_groups = Column(int)
    grand_total = Column(float)


class ETLFlow(Flow):
    # Extra ledger columns, populated during the run (small metadata only).
    class Ledger(LedgerColumns):
        n_rows = Column(int)

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
run_id = ETLFlow.run()          # reads config from the environment
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
