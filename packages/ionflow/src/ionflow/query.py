import duckdb

from . import lake, metrics
from .arrow import read_parquet_sql
from .space import Space, utc_now
from .store import Store

# a flow with no runs has no catalog; the `ledger` view must still exist and type
_EMPTY_RUNS = (
    "(SELECT NULL::VARCHAR AS run_id, NULL::VARCHAR AS status, NULL::TIMESTAMP AS started, "
    'NULL::TIMESTAMP AS finished, NULL::VARCHAR AS "commit", NULL::VARCHAR AS origin_run_id, '
    "NULL::VARCHAR AS params, NULL::VARCHAR AS custom WHERE false)"
)


def ledger_sql(flow_cls, src="lake.runs"):
    """The `ledger` view over the `runs` table: base columns, then the flow's
    params and Ledger columns flattened out of their JSON storage — the table's
    schema never migrates; the view derives typed columns from the declarations.
    """
    cols = ['run_id, status, started, finished, "commit", origin_run_id']
    for n, c in flow_cls.params_columns().items():
        cols.append(f"CAST(params->>'{n}' AS {c.duckdb_type()}) AS {n}")
    for n, c in flow_cls.ledger_columns().items():
        op = "->" if c.is_vector() else "->>"  # -> keeps JSON arrays castable to DuckDB arrays
        cols.append(f"CAST(custom{op}'{n}' AS {c.persisted_type()}) AS {n}")
    return f"SELECT {', '.join(cols)} FROM {src}"


def _empty_view_sql(table):
    """Typed, zero-row SELECT standing in for a table with nothing on S3 yet.

    `run_id` is the catalog's, not the schema's, so it is appended here —
    last, matching its position in the real views.
    """
    return f"SELECT *, NULL::VARCHAR AS run_id FROM ({table.empty()})"


def _ledger_view(con, flow_cls, attached):
    con.execute(f"CREATE VIEW ledger AS {ledger_sql(flow_cls, 'lake.runs' if attached else _EMPTY_RUNS)}")


def step_rows(con, step, run_id, limit=100):
    """Preview of one step's output for one run (via the step's view)."""
    return con.sql(f"SELECT * FROM {step} WHERE run_id = '{run_id}' LIMIT {limit}")


def step_stats(con, run_id):
    """Per-step resource summary for one run, derived from the metrics samples.

    CPU deltas are computed per (step, branch) first because cumulative CPU
    times are per process; avg_cores = total CPU seconds / wall seconds.
    """
    return con.sql(f"""
        WITH g AS (
            SELECT step, branch, min(ts) AS t0, max(ts) AS t1,
                   max(cpu_user + cpu_system) - min(cpu_user + cpu_system) AS cpu,
                   max(rss) AS rss, max(threads) AS threads
            FROM metrics WHERE run_id = '{run_id}' AND step IS NOT NULL
            GROUP BY step, branch)
        SELECT step,
               round(epoch(max(t1) - min(t0)), 1) AS seconds,
               round(max(rss) / 1048576.0) AS peak_rss_mb,
               round(sum(cpu) / greatest(epoch(max(t1) - min(t0)), 0.001), 2) AS avg_cores,
               max(threads) AS max_threads,
               count(*) AS processes
        FROM g GROUP BY step ORDER BY min(t0)
    """)


def is_stale(space, run_id, started):
    """True if a run's heartbeat (metrics flush) has been quiet for STALE_AFTER.

    Skew-free on the primary path (see `Store.age`). With no metrics object yet —
    crashed before the first flush — it falls back to `started` against the local
    clock, a best-effort cross-machine comparison.
    """
    age = space.store.age(space.metrics(run_id))
    if age is None:
        age = (utc_now() - started).total_seconds()
    return age > metrics.STALE_AFTER


def _stale_running(con, space):
    """run_ids stuck in `running` whose heartbeat went quiet."""
    rows = con.sql("SELECT run_id, started FROM ledger WHERE status = 'running'").fetchall()
    return [r for r, s in rows if is_stale(space, r, s)]


def ledger_with_liveness(flow_cls, config, con=None):
    """Ledger relation where stale running runs read 'running (stale?)' — a
    SIGKILL'd run stays `running` forever, so liveness is derived at read time
    (one HEAD per running run).
    """
    store = Store.of(config)
    space = Space.for_flow(store, flow_cls)
    if con is None:  # ledger-only callers (ls) skip the step views entirely
        con = store.connect()
        _ledger_view(con, flow_cls, lake.Catalog(space).attach_readonly(con))
    stale = _stale_running(con, space)
    if not stale:
        return con.sql("SELECT * FROM ledger")
    ids = ", ".join(f"'{r}'" for r in stale)
    return con.sql(
        f"SELECT * REPLACE (CASE WHEN run_id IN ({ids}) THEN 'running (stale?)' ELSE status END AS status) FROM ledger"
    )


def duck(flow_cls, config):
    """DuckDB connection with a view per step table + `ledger` and `metrics` views.

    Step and ledger views sit over the flow's DuckLake catalog, attached
    read-only (one consistent snapshot). `metrics` keeps its own plumbing:
    telemetry objects are not lake data.
    """
    store = Store.of(config)
    con = store.connect()
    space = Space.for_flow(store, flow_cls)
    if attached := lake.Catalog(space).attach_readonly(con):
        for name in flow_cls._dag.steps:
            # run_id last: keeps one column order between real and empty views
            con.execute(f"CREATE VIEW {name} AS SELECT * EXCLUDE (run_id), run_id FROM lake.{name}")
    elif space.run_ids():
        # runs happened (genesis precedes the first run object) but the register is gone
        raise RuntimeError(
            f"{flow_cls.flow_id()}: runs exist but no catalog does; "
            f"run `ionflow catalog-backfill` to rebuild it from the run folders"
        )
    else:  # never ran
        for name, step in flow_cls._dag.steps.items():
            con.execute(f"CREATE VIEW {name} AS {_empty_view_sql(step.output_table)}")
    _ledger_view(con, flow_cls, attached)
    _metrics_view(con, space)
    return con


def _metrics_view(con, space):
    """View over every metrics object, falling back to a typed empty view.

    DuckDB binds read_parquet at CREATE time, so an absent glob raises here —
    that IS the "no metrics yet" signal, no LIST needed.
    """
    try:
        con.execute(f"CREATE VIEW metrics AS {read_parquet_sql(space.uri(space.metrics_glob))}")
    except duckdb.IOException:
        con.execute(f"CREATE VIEW metrics AS {_empty_view_sql(metrics.Sample)}")
