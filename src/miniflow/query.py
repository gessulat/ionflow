from .storage import Layout, connect, list_keys, parse_key, read_parquet_sql, s3_client, uri


def _empty_view_sql(table):
    """Typed, zero-row SELECT so a step that produced no parts is still queryable."""
    cols = ["NULL::VARCHAR AS run_id"] + [
        f"NULL::{c.duckdb_type()} AS {name}" for name, c in table.columns().items()
    ]
    return "SELECT " + ", ".join(cols) + " WHERE false"


def _steps_with_parts(client, config, layout):
    """Step names that have at least one part, from a single LIST of runs/."""
    keys = list_keys(client, config.bucket, layout.root + "/runs/")
    parsed = (parse_key(config.prefix + "/", k) for k in keys)
    return {step for p in parsed if p and (step := p[1])}


def step_rows(con, step, run_id, limit=100):
    """Preview of one step's output for one run (via the step's view)."""
    return con.sql(f"SELECT * FROM {step} WHERE run_id = '{run_id}' LIMIT {limit}")


def duck(flow_cls, config):
    """DuckDB connection with a view per step table + a `ledger` view.

    Views glob every run, so `SELECT ... WHERE run_id = ...` time-travels and
    new runs appear with nothing to sync.
    """
    con = connect(config)
    layout = Layout(config.prefix, flow_cls.__name__)
    present = _steps_with_parts(s3_client(config), config, layout)
    for name, step in flow_cls._dag.steps.items():
        if name in present:
            body = read_parquet_sql(uri(config, layout.table_glob(name)))
        else:
            body = _empty_view_sql(step.output_table)
        con.execute(f"CREATE VIEW {name} AS {body}")
    con.execute(
        f"CREATE VIEW ledger AS SELECT * FROM read_parquet('{uri(config, layout.ledger)}')"
    )
    return con
