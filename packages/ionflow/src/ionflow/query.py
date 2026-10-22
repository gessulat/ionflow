import json
import typing

import duckdb
import pyarrow as pa

from . import lake, metrics
from .arrow import read_parquet_sql
from .artifact import Artifacts, MissingPayload
from .dag import PATH_COL
from .schema import _q
from .space import Space, utc_now
from .store import Store

# a flow with no runs has no catalog; the `ledger` view must still exist and type
_EMPTY_RUNS = (
    "(SELECT NULL::VARCHAR AS run_id, NULL::VARCHAR AS status, NULL::TIMESTAMP AS started, "
    'NULL::TIMESTAMP AS finished, NULL::VARCHAR AS "commit", NULL::VARCHAR AS origin_run_id, '
    "NULL::VARCHAR AS params, NULL::VARCHAR AS custom, NULL::VARCHAR AS artifacts WHERE false)"
)


def ledger_sql(flow_cls, src="lake.runs"):
    """The `ledger` view over the `runs` table: base columns, then the flow's
    params and Ledger columns flattened out of their JSON storage — the table's
    schema never migrates; the view derives typed columns from the declarations.
    """
    cols = ['run_id, status, started, finished, "commit", origin_run_id']
    for n, c in flow_cls.params_columns().items():
        cols.append(f"CAST(params->>'{n}' AS {c.duckdb_type()}) AS {_q(n)}")
    for n, c in flow_cls.ledger_columns().items():
        op = "->" if c.is_vector() else "->>"  # -> keeps JSON arrays castable to DuckDB arrays
        cols.append(f"CAST(custom{op}'{n}' AS {c.persisted_type()}) AS {_q(n)}")
    return f"SELECT {', '.join(cols)} FROM {src}"


def _empty_view_sql(table):
    """Typed, zero-row SELECT standing in for a table with nothing on S3 yet.

    `run_id` is the catalog's, not the schema's, so it is appended here —
    last, matching its position in the real views.
    """
    return f"SELECT *, NULL::VARCHAR AS run_id FROM ({table.empty()})"


def _json_view(con, name, records):
    """Bind small metadata records with a stable empty schema and JSON projection."""
    con.register("__ionflow_" + name, pa.table({"body": pa.array([json.dumps(r) for r in records], type=pa.string())}))
    con.execute(f"CREATE OR REPLACE VIEW {_q(name)} AS SELECT body::JSON AS record FROM __ionflow_{name}")


def artifact_binding_rows(flow_id, rows):
    records = []
    for run_id, body in rows:
        for role, value in json.loads(body or "{}").items():
            many = isinstance(value, list)
            for position, ref in enumerate(value if many else [value]):
                records.append(
                    {
                        "flow_id": flow_id,
                        "run_id": run_id,
                        "role": role,
                        "position": position if many else None,
                        "artifact": ref,
                    }
                )
    return records


def artifact_views(con, registry, activities, inputs=()):
    """Attach metadata views without a second DuckLake attachment name or live writers."""
    from .storage import Storage

    records = (
        Storage(registry.store.cfg).artifacts()
        if registry.store.local_dir
        else [r | registry.availability().get(r["id"], {}) for r in registry.records()]
    )
    _json_view(con, "artifact_inventory", records)
    con.execute("""CREATE VIEW artifacts AS SELECT record->>'id' AS id, record->>'kind' AS kind,
        record->>'name' AS name, record->>'payload' AS payload, record->>'digest' AS digest,
        record->'metadata' AS metadata, (record->>'revision')::BIGINT AS revision,
        (record->>'created')::TIMESTAMP AS created,
        coalesce((record->>'local')::BOOLEAN, false) AS local,
        coalesce((record->>'s3')::BOOLEAN, false) AS s3 FROM artifact_inventory""")
    files = [{"artifact_id": r["id"], **f} for r in records for f in r["content"]["files"]]
    _json_view(con, "artifact_members", files)
    con.execute("""CREATE VIEW artifact_files AS SELECT record->>'artifact_id' AS artifact_id,
        record->>'path' AS path, record->>'sha256' AS digest, (record->>'size')::BIGINT AS size
        FROM artifact_members""")
    _json_view(con, "artifact_selected_inputs", inputs)
    con.execute("""CREATE VIEW artifact_bindings AS SELECT record->>'flow_id' AS flow_id,
        record->>'run_id' AS run_id, record->>'role' AS role, (record->>'position')::BIGINT AS position,
        record->'artifact'->>'id' AS artifact_id, record->'artifact'->'metadata' AS metadata,
        (record->'artifact'->>'revision')::BIGINT AS revision FROM artifact_selected_inputs""")
    _json_view(con, "artifact_activity", activities)
    events = []
    for a in activities:
        for u in a["uses"]:
            events.append(
                {
                    "flow_id": a.get("flow_id"),
                    "run_id": a["run_id"],
                    "step": a["step"],
                    "key": a["key"],
                    "role": u["role"],
                    "event": "used",
                    "artifact_id": u["artifact"]["id"],
                }
            )
        for o in a["outputs"]:
            events.append(
                {
                    "flow_id": a.get("flow_id"),
                    "run_id": a["run_id"],
                    "step": a["step"],
                    "key": a["key"],
                    "role": o["role"],
                    "event": o["event"],
                    "artifact_id": o["artifact"]["id"],
                }
            )
    _json_view(con, "artifact_event_records", events)
    con.execute("""CREATE VIEW artifact_events AS SELECT record->>'flow_id' AS flow_id,
        record->>'run_id' AS run_id, record->>'step' AS step, record->>'key' AS item_key,
        record->>'role' AS role, record->>'event' AS event, record->>'artifact_id' AS artifact_id
        FROM artifact_event_records""")


def _artifact_step_view(con, name, schema, activities):
    rows = [
        o["artifact"] | {"run_id": a["run_id"], "item_key": a["key"]}
        for a in activities
        if a["step"] == name
        for o in a["outputs"]
        if o["role"] == "output"
    ]
    source = "__ionflow_artifact_output_" + name
    _json_view(con, source, rows)
    cols = ["record->>'id' AS id", "record->>'name' AS name", "record->'metadata' AS metadata"]
    for field, hint in schema._fields.items():
        typ = _artifact_sql_type(hint)
        # JSON extraction uses a bound path in a macro-free SELECT; field names are escaped as SQL literals.
        path = ("$.metadata." + json.dumps(field)).replace("'", "''")
        cols.append(f"CAST(json_extract_string(record, '{path}') AS {typ}) AS {_q(field)}")
    cols += ["record->>'item_key' AS item_key", "record->>'run_id' AS run_id"]
    con.execute(f"CREATE VIEW {_q(name)} AS SELECT {', '.join(cols)} FROM {_q(source)}")


def _artifact_sql_type(hint):
    if typing.get_origin(hint) in (typing.Union, type(str | None)):
        members = [t for t in typing.get_args(hint) if t is not type(None)]
        if len(members) != 1:
            return "JSON"
        hint = members[0]
    if typing.get_origin(hint) is list:
        return _artifact_sql_type(typing.get_args(hint)[0]) + "[]"
    return {str: "VARCHAR", int: "BIGINT", float: "DOUBLE", bool: "BOOLEAN"}.get(hint, "JSON")


def _step_view_sql(space, entries):
    """One step's view: the manifest's part objects, each row labeled with the
    run referencing its part (0020). A part shared by N runs appears once per
    run — exactly what the per-run copies used to materialize, at zero storage.
    `hive_partitioning` stays off: a part path's `run_id=` names its CREATOR,
    the join supplies the referencing run.
    """
    uris = ", ".join(f"'{space.uri(p)}'" for p in sorted({p for p, _ in entries}))
    values = ", ".join(f"('{space.uri(p)}', '{r}')" for p, r in entries)
    return (
        f"SELECT d.* EXCLUDE ({PATH_COL}), m.run_id "
        f"FROM read_parquet([{uris}], filename='{PATH_COL}', hive_partitioning=false) d "
        f"JOIN (VALUES {values}) m({PATH_COL}, run_id) ON d.{PATH_COL} = m.{PATH_COL}"
    )


def _ledger_view(con, flow_cls, attached, config=None):
    body = ledger_sql(flow_cls, "lake.runs" if attached else _EMPTY_RUNS)
    if attached:
        peer = None
        if config and config.remote:
            from .storage import Storage

            peer = Storage(config).peer
        flags = lake.Catalog(Space.for_flow(Store.of(config), flow_cls)).availability(peer)
        _json_view(con, "run_locations", [{"run_id": rid, **v} for rid, v in flags.items()])
        con.execute(
            f"CREATE VIEW ledger AS SELECT l.*, coalesce((p.record->>'local')::BOOLEAN, false) AS local, "
            f"coalesce((p.record->>'s3')::BOOLEAN, false) AS s3 FROM ({body}) l "
            "LEFT JOIN run_locations p ON p.record->>'run_id' = l.run_id"
        )
    else:
        con.execute(f"CREATE VIEW ledger AS SELECT *, false AS local, false AS s3 FROM ({body})")


def step_rows(con, step, run_id, limit=100):
    """Preview of one step's output for one run (via the step's view)."""
    present = con.execute("SELECT local FROM ledger WHERE run_id = ?", [run_id]).fetchone()
    if present is not None and not present[0]:
        raise MissingPayload(f"run {run_id}: download this run before reading outputs")
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
    rows = con.sql("SELECT run_id, started FROM ledger WHERE status = 'running' AND local").fetchall()
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
        _ledger_view(con, flow_cls, lake.Catalog(space).attach_readonly(con), config)
    stale = _stale_running(con, space)
    if not stale:
        return con.sql("SELECT * FROM ledger")
    ids = ", ".join(f"'{r}'" for r in stale)
    return con.sql(
        f"SELECT * REPLACE (CASE WHEN run_id IN ({ids}) THEN 'running (stale?)' ELSE status END AS status) FROM ledger"
    )


def duck(flow_cls, config):
    """DuckDB connection with a view per step + `ledger` and `metrics` views.

    All views derive from the flow's DuckLake catalog, attached read-only (one
    consistent snapshot): a step's view reads the parts its manifest rows name
    (per-run membership — `lake.<step>` itself is the corpus, each part once
    under its creating run). `metrics` keeps its own plumbing: telemetry
    objects are not lake data.
    """
    store = Store.of(config)
    con = store.connect()
    space = Space.for_flow(store, flow_cls)
    activities = []
    inputs = []
    if attached := lake.Catalog(space).attach_readonly(con):
        inputs = artifact_binding_rows(
            flow_cls.flow_id(), con.sql("SELECT run_id, artifacts FROM lake.runs").fetchall()
        )
        activities = [
            json.loads(v) | {"run_id": r, "flow_id": flow_cls.flow_id()}
            for r, v in con.sql("SELECT run_id, record FROM lake.artifact_activity").fetchall()
        ]
        by_step = {}
        for run_id, step, path in con.sql(
            "SELECT m.run_id, m.step, m.path FROM lake.manifest m "
            "JOIN lake.registered_parts p ON p.path = m.path "
            "JOIN lake.run_versions v ON v.run_id = m.run_id WHERE v.present"
        ).fetchall():
            by_step.setdefault(step, []).append((path, run_id))
        for name, step in flow_cls._dag.steps.items():
            if step.is_artifact:
                _artifact_step_view(con, name, step.output_table, activities)
                continue
            entries = by_step.get(name)
            # run_id last either way: one column order between real and empty views
            body = _step_view_sql(space, entries) if entries else _empty_view_sql(step.output_table)
            con.execute(f"CREATE VIEW {name} AS {body}")
    elif space.run_ids():
        # runs happened (genesis precedes the first run object) but the register is gone
        raise RuntimeError(
            f"{flow_cls.flow_id()}: runs exist but no catalog does; "
            f"run `ionflow catalog-backfill` to rebuild it from the run folders"
        )
    else:  # never ran
        for name, step in flow_cls._dag.steps.items():
            if step.is_artifact:
                _artifact_step_view(con, name, step.output_table, [])
                continue
            con.execute(f"CREATE VIEW {name} AS {_empty_view_sql(step.output_table)}")
    _ledger_view(con, flow_cls, attached, config)
    _metrics_view(con, space)
    _phases_view(con, space)
    artifact_views(con, Artifacts(store), activities, inputs)
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


def _phases_view(con: duckdb.DuckDBPyConnection, space: Space) -> None:
    from .phases import Phase

    try:
        glob = space.uri(f"{space.root}/run_id=*/_phases*.parquet")
        con.execute(f"CREATE VIEW phases AS {read_parquet_sql(glob)}")
    except duckdb.IOException:
        con.execute(f"CREATE VIEW phases AS {_empty_view_sql(Phase)}")


def phase_stats(con: duckdb.DuckDBPyConnection, run_id: str) -> duckdb.DuckDBPyRelation:
    """Phase boundaries and process memory, including spans shorter than a sampling interval."""
    return con.sql("""
        SELECT p.* EXCLUDE (rss_start, rss_end),
               epoch(coalesce(p.ended, timezone('UTC', now())) - p.started) AS seconds,
               greatest(max(p.rss_start), max(p.rss_end), coalesce(max(m.rss), 0)) / 1048576.0 AS peak_rss_mb
        FROM phases p LEFT JOIN metrics m
          ON m.run_id = p.run_id AND (m.branch = p.branch OR m.branch IS NULL)
         AND m.ts BETWEEN p.started AND coalesce(p.ended, timezone('UTC', now()))
        WHERE p.run_id = ? GROUP BY ALL ORDER BY p.started, p.span_id
    """, params=[run_id])
