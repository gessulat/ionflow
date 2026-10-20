"""`sync`: migrating a flow's local-mode history to a bucket.

Local mode itself (runs, catalog, ledger, metrics, publish) is exercised by the
mode-parametrized `anycfg` fixture and by test_lake_contract's local tests; what
only exists here is the local -> S3 migration.
"""

from examples.embed import EmbedDocs
from ionflow.space import Space
from ionflow.store import Store


def _sync(flow_cls, src_cfg, dst_cfg):
    """What `ionflow sync` does: one space per side, then Space.sync_to."""
    src = Space.for_flow(Store.of(src_cfg), flow_cls)
    return src.sync_to(Space.for_flow(Store.of(dst_cfg), flow_cls), flow_cls)


def test_sync_local_history_to_bucket(localcfg, s3cfg, repo):
    first = EmbedDocs.run(localcfg, repo=repo, use_processes=False)

    n = _sync(EmbedDocs, localcfg, s3cfg)
    assert n["runs"] == 1 and n["copied"] > 0

    con = EmbedDocs.duck(s3cfg)  # plain S3-mode read of the synced flow
    assert con.sql(f"SELECT n_docs FROM summary WHERE run_id = '{first}'").fetchone() == (4,)
    assert con.sql("SELECT status FROM ledger").fetchall() == [("complete",)]
    assert con.sql(f"SELECT count(*) FROM metrics WHERE run_id = '{first}'").fetchone()[0] > 0

    # more local history accumulates; re-sync is incremental and idempotent
    second = EmbedDocs.run(localcfg, repo=repo, use_processes=False)
    n = _sync(EmbedDocs, localcfg, s3cfg)
    assert n["runs"] == 2
    con = EmbedDocs.duck(s3cfg)
    runs = {r[0] for r in con.sql("SELECT DISTINCT run_id FROM summary").fetchall()}
    assert runs == {first, second}
    assert con.sql("SELECT count(*) FROM ledger").fetchone() == (2,)


def test_sync_empty_store_is_supported(s3cfg, localcfg):
    assert _sync(EmbedDocs, s3cfg, localcfg) == {"copied": 0, "runs": 0}
