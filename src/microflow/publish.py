import json

from . import ledger
from .storage import (
    Layout,
    connect,
    empty_relation,
    input_relation,
    list_keys,
    object_age,
    s3_client,
    utc_now,
    write_part,
)


def _parse_dest(config, dest):
    """(bucket, key) from an `s3://bucket/key` URI or a bare key in the config bucket."""
    if dest.startswith("s3://"):
        bucket, _, key = dest.removeprefix("s3://").partition("/")
        if not (bucket and key):
            raise ValueError(f"destination must be s3://bucket/key, got: {dest}")
        return bucket, key
    return config.bucket, dest


def manifest_key(dest_key):
    """The provenance sidecar's key for a published object."""
    return dest_key + ".meta.json"


def publish(flow_cls, config, run_id, step, dest, where=None, force=False):
    """Copy `step`'s output from a complete run to `dest`; returns the manifest.

    The destination is one plain parquet object plus a `<dest>.meta.json`
    provenance sidecar, so consumers read a fixed path with no microflow
    dependency. Always a copy — run folders stay immutable. Replacing an
    object (`force`) is one atomic PUT, but two related objects published
    separately can still be observed mid-swap; consumers needing consistency
    across files should compare the sidecars' run_id.
    """
    if step not in flow_cls._dag.steps:
        raise ValueError(f"unknown step '{step}' (steps: {', '.join(flow_cls._dag.steps)})")
    layout = Layout.for_flow(config, flow_cls)
    client = s3_client(config)
    meta = ledger.read_meta(client, config.bucket, layout.meta(run_id))
    if meta is None:
        raise RuntimeError(f"run not found: {run_id}")
    if not meta.get("finished"):
        raise RuntimeError(f"run {run_id} did not complete; only complete runs publish")
    dest_bucket, dest_key = _parse_dest(config, dest)
    if not force and object_age(client, dest_bucket, dest_key) is not None:
        raise FileExistsError(f"s3://{dest_bucket}/{dest_key} exists; pass force=True (CLI: --force) to replace it")
    keys = sorted(list_keys(client, config.bucket, layout.step_dir(run_id, step) + "/"))
    con = connect(config)
    if keys:
        rel = input_relation(con, config, keys)
    else:  # e.g. an empty fan-out: publish a typed, zero-row file
        table = flow_cls._dag.steps[step].output_table
        if table is None:
            raise RuntimeError(f"step '{step}' wrote no parts in run {run_id} and declares no Table")
        rel = empty_relation(con, table)
    if where:
        rel = rel.filter(where)
    write_part(client, dest_bucket, dest_key, rel, None)
    manifest = {
        "flow": flow_cls.flow_id(),
        "run_id": run_id,
        "step": step,
        "where": where,
        "commit": meta.get("commit"),
        "published_at": utc_now().isoformat(),
    }
    client.put_object(Bucket=dest_bucket, Key=manifest_key(dest_key), Body=json.dumps(manifest).encode())
    return manifest
