import duckdb

from . import catalog, metrics
from .schema import _q
from .storage import (
    Layout,
    connect,
    object_age,
    read_parquet_sql,
    s3_client,
    uri,
    utc_now,
)

# a flow with no runs has no ledger object; the view must still exist and type
_EMPTY_LEDGER_SQL = (
    "SELECT NULL::VARCHAR AS run_id, NULL::VARCHAR AS status, NULL::TIMESTAMP AS started, "
    'NULL::TIMESTAMP AS finished, NULL::VARCHAR AS "commit", '
    "NULL::VARCHAR AS origin_run_id WHERE false"
)


def _empty_view_sql(table):
    """Typed, zero-row SELECT standing in for a table with nothing on S3 yet.

    `run_id` goes last, matching its position in the catalog views.
    """
    cols = [f"NULL::{c.persisted_type()} AS {_q(name)}" for name, c in table.columns().items()]
    return "SELECT " + ", ".join(cols + ["NULL::VARCHAR AS run_id"]) + " WHERE false"


def _view_or_empty(con, name, body, empty_table):
    """CREATE VIEW over S3 objects, falling back to a typed empty view.

    DuckDB binds read_parquet at CREATE time, so an absent glob/object raises
    here — that IS the "nothing on S3 yet" signal, no LIST needed.
    """
    try:
        con.execute(f"CREATE VIEW {name} AS {body}")
    except duckdb.IOException:
        empty = _EMPTY_LEDGER_SQL if empty_table is None else _empty_view_sql(empty_table)
        con.execute(f"CREATE VIEW {name} AS {empty}")


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


def is_stale(client, config, layout, run_id, started):
    """True if a run's heartbeat (metrics flush) has been quiet for STALE_AFTER.

    Skew-free on the primary path (the object's LastModified vs S3's own
    clock). Runs with no metrics object yet — crashed before the first flush,
    or predating heartbeats — fall back to comparing `started` against the
    local clock, a best-effort cross-machine comparison.
    """
    age = object_age(client, config.bucket, layout.metrics(run_id))
    if age is None:
        age = (utc_now() - started).total_seconds()
    return age > metrics.STALE_AFTER


def _stale_running(con, client, config, layout):
    """run_ids stuck in `running` whose heartbeat went quiet."""
    rows = con.sql("SELECT run_id, started FROM ledger WHERE status = 'running'").fetchall()
    return [r for r, s in rows if is_stale(client, config, layout, r, s)]


def ledger_with_liveness(flow_cls, config, con=None):
    """Ledger relation where stale running runs read 'running (stale?)'.

    A SIGKILL'd run stays `running` in the ledger forever; this derives the
    honest display state at read time (one HEAD per running run).
    """
    if con is None:  # ledger-only callers (ls) skip the catalog attach entirely
        con = connect(config)
        _view_or_empty(
            con,
            "ledger",
            f"SELECT * FROM read_parquet('{uri(config, Layout.for_flow(config, flow_cls).ledger)}')",
            None,
        )
    layout = Layout.for_flow(config, flow_cls)
    stale = _stale_running(con, s3_client(config), config, layout)
    if not stale:
        return con.sql("SELECT * FROM ledger")
    ids = ", ".join(f"'{r}'" for r in stale)
    return con.sql(
        f"SELECT * REPLACE (CASE WHEN run_id IN ({ids}) THEN 'running (stale?)' ELSE status END AS status) FROM ledger"
    )


def duck(flow_cls, config):
    """DuckDB connection with a view per step table + a `ledger` view.

    Views sit over the flow's DuckLake catalog, attached read-only (a
    consistent snapshot), so `SELECT ... WHERE run_id = ...` time-travels. A
    flow that never ran gets typed empty views; runs without a catalog mean
    the register was lost — refused with a pointer at `catalog-backfill`.
    `ledger` and `metrics` keep their own plumbing: run bookkeeping and
    telemetry are not lake data.
    """
    con = connect(config)
    layout = Layout.for_flow(config, flow_cls)
    if catalog.Catalog(config, flow_cls).attach_readonly(con):
        for name in flow_cls._dag.steps:
            # run_id last: keeps one column order between real and empty views
            con.execute(f"CREATE VIEW {name} AS SELECT * EXCLUDE (run_id), run_id FROM lake.{name}")
    elif object_age(s3_client(config), config.bucket, layout.ledger) is not None:
        # runs happened (genesis precedes the first run object) but the register is gone
        raise RuntimeError(
            f"{flow_cls.flow_id()}: runs exist but no catalog does; "
            f"run `ionflow catalog-backfill` to rebuild it from the run folders"
        )
    else:  # never ran
        for name, step in flow_cls._dag.steps.items():
            con.execute(f"CREATE VIEW {name} AS {_empty_view_sql(step.output_table)}")
    _view_or_empty(con, "metrics", read_parquet_sql(uri(config, layout.metrics_glob)), metrics.Sample)
    _view_or_empty(con, "ledger", f"SELECT * FROM read_parquet('{uri(config, layout.ledger)}')", None)
    return con
