"""Sync: push a flow's local-store history into an S3 bucket (decision 0014).

The migration path out of local prototyping mode: run folders are copied
byte-for-byte (they are immutable, so already-present target keys are
skipped and re-running sync is cheap), ledger rows are MERGED through the
target's CAS ledger in one commit (never overwritten — runs recorded on the
bucket by others survive), and the target's catalog is rebuilt from its run
folders + ledger via `catalog.backfill` — a DuckLake catalog stores absolute
data-file locations, so the local catalog object itself can never be copied,
only re-derived. The local store is left untouched; after syncing, point the
config at the bucket (`ionflow setup`) to continue there.
"""

import pathlib
from concurrent.futures import ThreadPoolExecutor

from schrein.ducklake import CATALOG_KEY

from . import catalog, ledger, storage
from .storage import Layout


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
    # the catalog and its data files are re-derived on the target (they encode
    # local absolute paths); the ledger merges below instead of copying
    internal = (f"/{CATALOG_KEY}", f"/{catalog.LAKE_SUBDIR}/")

    existing = set(storage.list_keys(s3, target_cfg.bucket, dst_layout.root + "/"))
    todo = []
    for key in storage.list_keys(local, local_cfg.bucket, src_layout.root + "/"):
        tail = key.removeprefix(src_layout.root)
        if key == src_layout.ledger or tail.startswith(internal) or dst_layout.root + tail in existing:
            continue  # run folders are immutable: a key already on the target is done
        todo.append((str(root / key), dst_layout.root + tail))
    with ThreadPoolExecutor(max_workers=16) as ex:  # many small objects, latency-bound
        list(ex.map(lambda t: s3.upload_file(t[0], target_cfg.bucket, t[1]), todo))

    runs = 0
    blob = storage.get_bytes_or_none(local, local_cfg.bucket, src_layout.ledger)
    if blob is not None:
        table = storage.table_from_bytes(blob)
        custom = [table.schema.field(n) for n in table.schema.names if n not in ledger.BASE_COLS]
        ledger.upsert_runs(s3, target_cfg.bucket, dst_layout.ledger, table.to_pylist(), custom)
        runs = table.num_rows

    if runs:
        catalog.backfill(flow_cls, target_cfg, replace=True)
    return {"copied": len(todo), "runs": runs}
