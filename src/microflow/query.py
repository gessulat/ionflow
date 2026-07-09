from . import metrics
from .storage import (
    METRICS_FILE,
    Layout,
    connect,
    list_keys,
    object_age,
    parse_key,
    read_parquet_sql,
    s3_client,
    uri,
    utc_now,
)


def _empty_view_sql(table):
    """Typed, zero-row SELECT so a step that produced no parts is still queryable.

    `run_id` goes last, matching where hive partitioning puts it in real views.
    """
    cols = [f"NULL::{c.duckdb_type()} AS {name}" for name, c in table.columns().items()]
    return "SELECT " + ", ".join(cols + ["NULL::VARCHAR AS run_id"]) + " WHERE false"


def _scan_parts(client, config, layout):
    """(step names with parts, any metrics present) from one LIST of runs/."""
    steps, has_metrics = set(), False
    for key in list_keys(client, config.bucket, layout.root + "/runs/"):
        parsed = parse_key(config.prefix + "/", key)
        if parsed and parsed[1]:
            steps.add(parsed[1])
        has_metrics = has_metrics or key.endswith("/" + METRICS_FILE)
    return steps, has_metrics


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
    con = con or duck(flow_cls, config)
    layout = Layout(config.prefix, flow_cls.__name__)
    stale = _stale_running(con, s3_client(config), config, layout)
    if not stale:
        return con.sql("SELECT * FROM ledger")
    ids = ", ".join(f"'{r}'" for r in stale)
    return con.sql(
        f"SELECT * REPLACE (CASE WHEN run_id IN ({ids}) "
        "THEN 'running (stale?)' ELSE status END AS status) FROM ledger"
    )


def duck(flow_cls, config):
    """DuckDB connection with a view per step table + a `ledger` view.

    Views glob every run, so `SELECT ... WHERE run_id = ...` time-travels and
    new runs appear with nothing to sync.
    """
    con = connect(config)
    layout = Layout(config.prefix, flow_cls.__name__)
    present, has_metrics = _scan_parts(s3_client(config), config, layout)
    for name, step in flow_cls._dag.steps.items():
        if name in present:
            body = read_parquet_sql(uri(config, layout.table_glob(name)))
        else:
            body = _empty_view_sql(step.output_table)
        con.execute(f"CREATE VIEW {name} AS {body}")
    if has_metrics:
        metrics_body = read_parquet_sql(uri(config, layout.metrics_glob))
    else:
        metrics_body = _empty_view_sql(metrics.Sample)
    con.execute(f"CREATE VIEW metrics AS {metrics_body}")
    con.execute(
        f"CREATE VIEW ledger AS SELECT * FROM read_parquet('{uri(config, layout.ledger)}')"
    )
    return con
