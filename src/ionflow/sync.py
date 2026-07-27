"""Sync: push a flow's local-store history into an S3 bucket (decision 0014).

The migration path out of local prototyping mode: run folders are copied
byte-for-byte (they are immutable, so re-running sync is idempotent), ledger
rows are MERGED through the target's CAS ledger (never overwritten — runs
recorded on the bucket by others survive), and the target's catalog is
rebuilt from its run folders + ledger via `catalog.backfill` — a DuckLake
catalog stores absolute data-file locations, so the local catalog object
itself can never be copied, only re-derived. The local store is left
untouched; after syncing, point the config at the bucket (`ionflow setup`)
to continue there.
"""

import pathlib

from . import catalog, ledger, storage
from .storage import Layout


def parse_dest(dest):
    """'s3://bucket[/prefix]' -> (bucket, prefix or None)."""
    bucket, _, prefix = dest.removeprefix("s3://").partition("/")
    if not (dest.startswith("s3://") and bucket):
        raise ValueError(f"destination must be s3://bucket[/prefix], got: {dest}")
    return bucket, prefix.rstrip("/") or None


def sync_flow(flow_cls, local_cfg, target_cfg):
    """Copy one flow's runs + ledger from the local store to `target_cfg`'s
    bucket, then rebuild the flow's catalog there. Returns {copied, runs}.
    """
    if not local_cfg.local_dir:
        raise ValueError("sync sources a local store; the active config has no local_dir")
    src_layout = Layout.for_flow(local_cfg, flow_cls)
    dst_layout = Layout.for_flow(target_cfg, flow_cls)
    local = storage.make_client(local_cfg)
    s3 = storage.s3_client(target_cfg)
    root = pathlib.Path(local_cfg.local_dir)

    copied = 0
    for key in storage.list_keys(local, "", src_layout.root + "/"):
        tail = key.removeprefix(src_layout.root)
        # the ledger merges below; catalog + its data files are re-derived on
        # the target (they encode local absolute paths)
        if key == src_layout.ledger or tail.startswith(("/catalog.ducklake", "/_lake/")):
            continue
        s3.upload_file(str(root / key), target_cfg.bucket, dst_layout.root + tail)
        copied += 1

    runs = 0
    blob = storage.get_bytes_or_none(local, "", src_layout.ledger)
    if blob is not None:
        table = storage.table_from_bytes(blob)
        custom = [table.schema.field(n) for n in table.schema.names if n not in ledger.BASE_COLS]
        for row in table.to_pylist():
            ledger.upsert_run(s3, target_cfg.bucket, dst_layout.ledger, row, custom)
            runs += 1

    if runs:
        catalog.backfill(flow_cls, target_cfg, replace=True)
    return {"copied": copied, "runs": runs}
