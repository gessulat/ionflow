```
|  |   _
|  |  /
|  |  +-
|__|  |
|     /
```

# microflow

A barebones [Metaflow](https://metaflow.org) alternative. A **flow is a class**, a
**step is a method**, and every step returns a **typed table** that microflow persists
as Parquet on S3. No Kubernetes, no scheduler, no metadata server — the only thing you
configure is one S3-compatible bucket.

- **The DAG is the method signatures.** A step's parameters name its upstream steps.
  No `self.next(...)`.
- **Schemas are declared, enforced, and queryable.** Each output table is a class;
  DuckDB validates types / `NOT NULL` / `CHECK` on write and exposes every run as SQL.
- **Runs are logged in an append-only ledger** in the bucket: `run_id`, `status`
  (`running` / `complete` / `failed`), `started`, `finished`, git `commit`,
  `steps_complete`, plus any flow-specific columns.
- **Fan-out / fan-in** over N inputs, in parallel, on one machine. Transient branch
  failures (flaky S3) retry in place: `run(branch_retries=1)`.
- **`resume`** re-runs a failed run, reusing every step that already completed —
  including individual branches of a partially-failed fan-out.
- **Clean git required.** microflow refuses to start with a dirty working tree, so every
  run is pinned to a commit.

## Install

```bash
uv add microflow    # or: pip install microflow
```

## Configure (the only setup)

Run the wizard once per repo:

```bash
microflow setup
```

It asks for your bucket (and optional prefix / endpoint / region) and writes them to
`.microflow/config` at the repo root — a small TOML file, no secrets in it. It also asks
whether to gitignore `.microflow/` (keep it local) or commit it (share with your team).
Re-run `microflow setup` any time to change it.

**Credentials** come from the standard AWS chain (environment variables, `~/.aws/credentials`,
or an IAM role) and are never written to the repo. `microflow setup` just checks they resolve
and that the bucket is reachable. Any command guides you into `setup` if the repo isn't
configured yet.

```
# .microflow/config
bucket   = "my-bucket"
prefix   = "microflow"
endpoint = "https://…"   # omit for AWS S3; set for MinIO / R2
region   = "us-east-1"
```

## A complete flow

Load rows, **fan out** by group, score each group **in parallel**, then **fan in** to a
summary. Note there is no wiring code — `scored` depends on `parts` purely because it has
a parameter named `parts`.

```python
import pyarrow as pa
from microflow import Branch, Collect, Fanout, Flow, LedgerColumns, Table, check, pk


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

Columns can be lists: `embedding: list[float]` maps to DuckDB `DOUBLE[]` /
Arrow `list<double>` (nesting works too), so vectors don't need to be widened
into hundreds of scalar columns.

Wide or generated schemas don't have to be written out by hand —
`Table.make("FullAnnotation", {"peptide": str, **{f"frag_{i}": float | None for i in range(200)}})`
builds the same kind of class programmatically.

Step inputs arrive as **DuckDB relations**, so you can stay in SQL (`.aggregate`,
`.filter`, `.project`) or drop to a DataFrame with `.pl()` / `.df()` / `.to_arrow_table()`
and return any of those.

## Run it

```python
run_id = ETLFlow.run()          # reads .microflow/config (or pass config=Config(...))
```

or from the shell:

```bash
microflow run mymodule:ETLFlow
```

## Parameters

Typed run inputs, declared like everything else — annotations on an inner class.
A bare type is required; a class value is the default.

```python
from microflow import FlowParams

class Training(Flow):
    class Params(FlowParams):
        dataset: str          # required
        batch_size: int = 32

    def load(self) -> Raw:
        return read(self.dataset)          # params are bound to self
```

```bash
microflow run mymodule:Training -p dataset=d1 -p batch_size=64
```

```python
Training.run(params={"dataset": "d1"})
```

Every param becomes a **ledger column** (queryable per run, recorded from the moment
the run starts) and is stored in the run's meta; `resume` always reuses the origin
run's params. Types are limited to `int`/`float`/`str`/`bool` so params round-trip
JSON, parquet, and the command line.

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
microflow query mymodule:ETLFlow "SELECT * FROM summary"
microflow ls mymodule:ETLFlow
```

## Browse in the terminal (TUI)

Bare `microflow` (no subcommand) opens an interactive terminal UI:

```bash
microflow                       # or: microflow tui myproject.flows
```

- one **tab per Flow** in your project (point `microflow setup` at your `flows` package —
  every `Flow` subclass auto-registers on import),
- each tab is a table of that flow's runs: `run_id`, `status`, `started`, `finished`, **plus
  that flow's custom ledger columns**,
- `Tab` cycles flows; `j`/`k`/PageUp/PageDown move the row cursor,
- **Enter** drills into a run to preview each step's output table; **Esc** goes back.

On startup it re-discovers your flows and warns if a flow's declared table schema has drifted
from what's stored on S3, or if there's run data on S3 with no matching Flow in code. If the
repo isn't configured yet, the TUI opens a setup screen first.

## Resource metrics

Every run records CPU and memory for every step and branch — always on, negligible
overhead, one small `_metrics.parquet` per run. Sampling is adaptive (1s at start,
interval doubles when the 1024-sample cap folds the buffer), so even multi-day runs
stay bounded while peaks are preserved.

```bash
microflow stats mymodule:ETLFlow            # per-step: seconds, peak_rss_mb, avg_cores,
                                           # max_threads (defaults to the latest run)
microflow query mymodule:ETLFlow "SELECT * FROM metrics WHERE step='scored'"
```

The same summary appears in the TUI run drill-down ("stats" entry), and the raw
time series is the `metrics` SQL view (`ts, step, branch, rss, threads, cpu_user,
cpu_system, run_id`). Failed runs keep their metrics.

The buffer is flushed to S3 every 60s while the run executes, so metrics survive
hard kills (OOM, SIGKILL) up to the last minute and you can watch a running step's
memory live from another client (orchestrator process; process-mode branch samples
arrive with the final write). The flush also doubles as a **heartbeat**: `ls`
and the TUI show `running (stale?)` for runs whose heartbeat has been quiet for
5+ minutes — the honest state for a process that was killed without cleanup.

## Prototyping

Iterating on a flow fights the production guarantees (clean git, declared schemas,
immutable history). Mark a flow as a prototype to relax them — visibly, in code:

```python
from microflow import Flow, Prototype

class Scratch(Prototype, Flow):     # or: prototype = True as a class attribute
    def numbers(self):              # untyped step: schema inferred at write
        return pa.table({"x": [1, 2, 3]})

    def total(self, numbers) -> Totals:   # typed steps still validate as usual
        return numbers.aggregate("count(*) AS n, sum(x) AS total")
```

Prototype flows:

- run in a **scratch namespace** (`<prefix>/_proto/<Flow>/…`) — stable views, drift
  checks, and other clients never see the churn, and stable data is never touched;
- may run from a **dirty git tree** (ledger records `dirty:<sha>`);
- may **omit step output schemas** (untyped steps can't fan out — annotate
  `Fanout[...]` for that);
- tolerate ledger schema churn (a conflicting old scratch ledger is reset);
- can be wiped: `microflow clean mymodule:Scratch` (refused for stable flows —
  deletion is explicit and scratch-only, never a side effect of running).

Remove the mixin to promote: the flow points back at the stable namespace, and every
guarantee applies again from the first stable run. Untyped steps in a non-prototype
flow are a **class-definition error**.

## Resume

```bash
microflow resume mymodule:ETLFlow <origin_run_id>
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
- Steps can read **side inputs** (raw files that aren't step outputs) via `self.config`
  (e.g. `microflow.storage.s3_client(self.config)`). Side inputs escape run provenance —
  record their identifiers as ledger columns.
