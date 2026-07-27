"""Publish: bless one step's output into a stable DuckLake (decision 0009).

The publish lake is a schrein DuckLake independent of any flow's catalog —
consumers attach it read-only and see only blessed tables, plus a
`publications` table recording where each one came from. Publishing copies
(never references) the data into the lake's own data path, so a published
table needs no access to the pipeline's run folders and survives their
retention; the swap is one CAS commit, so readers see the previous version
or the new one, never a mix — and previous versions stay reachable through
DuckLake snapshots until expiry.
"""

import re
import uuid
from dataclasses import replace

from . import ledger, storage
from .storage import (
    Layout,
    connect,
    empty_relation,
    input_relation,
    make_client,
    parse_dest,
    part_keys,
    uri,
    utc_now,
    write_part,
)

PUBLISH_PREFIX = "published"  # default lake: s3://<bucket>/<config.prefix>/published/
PROVENANCE = "publications"  # per-lake provenance table, appended on every publish
_PROVENANCE_DDL = (
    f"CREATE TABLE IF NOT EXISTS lake.{PROVENANCE} ("
    "table_name VARCHAR NOT NULL, flow VARCHAR NOT NULL, run_id VARCHAR NOT NULL, "
    "step VARCHAR NOT NULL, filter VARCHAR, commit VARCHAR NOT NULL, published_at TIMESTAMP NOT NULL)"
)


def publish_lake(config, dest=None):
    """The publish DuckLake at `dest` ('s3://bucket/prefix') or the project
    default; returns (lake, target config).

    A custom `dest` supports publishing across buckets: consumers need read
    access to that location only, never to the pipeline bucket. The target
    config is `config` with the bucket swapped, so everything downstream flows
    through the ordinary make_client/uri seams.
    """
    if dest:
        if config.local_dir:
            raise ValueError("cannot publish from a local store to s3://…; `ionflow sync` the flow to a bucket first")
        bucket, prefix = parse_dest(dest, require_prefix=True)
        target = replace(config, bucket=bucket)
        return storage.ducklake_at(target, prefix), target
    return storage.ducklake_at(config, f"{config.prefix}/{PUBLISH_PREFIX}"), config


def publish(flow_cls, config, run_id, step, table, where=None, dest=None):
    """Copy `step`'s output from a complete run into the publish lake as `table`.

    Replaces the table's current contents atomically (previous versions stay
    reachable via DuckLake snapshots) and appends a `publications` row in the
    same commit. Returns that provenance row.
    """
    if step not in flow_cls._dag.steps:
        raise ValueError(f"unknown step '{step}' (steps: {', '.join(flow_cls._dag.steps)})")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table) or table == PROVENANCE:
        raise ValueError(f"invalid published table name '{table}'")
    layout = Layout.for_flow(config, flow_cls)
    client = make_client(config)
    meta = ledger.read_meta(client, config.bucket, layout.meta(run_id))
    if meta is None:
        raise RuntimeError(f"run not found: {run_id}")
    if not meta.get("finished"):
        raise RuntimeError(f"run {run_id} did not complete; only complete runs publish")

    lake, target = publish_lake(config, dest)
    out_table = flow_cls._dag.steps[step].output_table
    keys = part_keys(client, config.bucket, layout, run_id, step)
    con = connect(config)
    rel = input_relation(con, config, keys) if keys else empty_relation(con, out_table)
    if where:
        rel = rel.filter(where)
    # stage the frozen artifact inside the lake's data path; unique name, so
    # snapshots referencing an earlier publish keep their (immutable) file.
    # data_path is built from uri() by ducklake_at, so the store key is just
    # the path with the target's root stripped — no mode branch needed
    staged_uri = f"{lake.data_path}{table}/{run_id}-{uuid.uuid4().hex}.parquet"
    staged = staged_uri.removeprefix(uri(target, ""))
    write_part(make_client(target), target.bucket, staged, rel, out_table)

    ddl_cols = out_table.persisted_ddl()
    row = {
        "table_name": table,
        "flow": flow_cls.flow_id(),
        "run_id": run_id,
        "step": step,
        "filter": where,
        "commit": meta.get("commit"),
        "published_at": utc_now(),
    }

    def fn(con):
        con.execute("BEGIN")
        con.execute(f"CREATE TABLE IF NOT EXISTS lake.{table} ({ddl_cols})")
        con.execute(f"DELETE FROM lake.{table}")
        con.execute(f"CALL ducklake_add_data_files('lake', '{table}', '{staged_uri}')")
        con.execute(_PROVENANCE_DDL)
        con.execute(f"INSERT INTO lake.{PROVENANCE} VALUES (?, ?, ?, ?, ?, ?, ?)", list(row.values()))
        con.execute("COMMIT")

    lake.commit(fn)
    return row
