from .storage import Layout, connect, read_parquet_sql, uri


def duck(flow_cls, config):
    """DuckDB connection with a view per step table + a `ledger` view.

    Views glob every run, so `SELECT ... WHERE run_id = ...` time-travels and
    new runs appear with nothing to sync.
    """
    con = connect(config)
    layout = Layout(config.prefix, flow_cls.__name__)
    for name in flow_cls._dag.steps:
        glob = uri(config, layout.table_glob(name))
        con.execute(f"CREATE VIEW {name} AS {read_parquet_sql(glob)}")
    con.execute(
        f"CREATE VIEW ledger AS SELECT * FROM read_parquet('{uri(config, layout.ledger)}')"
    )
    return con
