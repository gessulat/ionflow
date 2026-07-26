"""Keyed (incremental) resume: fan-out items matched across runs by fanout_key."""

import pyarrow as pa
import pytest
from flows import DupKeyFlow, KeyedFlow, KeyedItem, TwoKeyFlow

from ionflow import ConstraintViolation, ledger
from ionflow.storage import Layout, s3_client, write_part


def _put_n(cfg, n):
    s3_client(cfg).put_object(Bucket=cfg.bucket, Key="side/n", Body=str(n).encode())


def _tokens(con, run_id):
    return dict(con.sql(f"SELECT item, token FROM work WHERE run_id = '{run_id}'").fetchall())


def _agg(con, run_id):
    return con.sql(f"SELECT n, token FROM agg WHERE run_id = '{run_id}'").fetchone()


@pytest.mark.slow
def test_keyed_resume_computes_only_new_items(s3cfg, repo):
    _put_n(s3cfg, 3)
    origin = KeyedFlow.run(s3cfg, repo=repo, use_processes=False)
    _put_n(s3cfg, 5)
    resumed = KeyedFlow.run(s3cfg, repo=repo, resume=origin, use_processes=False)

    con = KeyedFlow.duck(s3cfg)
    old, new = _tokens(con, origin), _tokens(con, resumed)
    assert set(new) == {f"file-{g}" for g in range(5)}
    for item in old:  # matched items were server-side copied, not recomputed
        assert new[item] == old[item]
    # the collect step reflects the grown work-list, so it was recomputed
    assert _agg(con, resumed)[0] == 5
    assert _agg(con, resumed)[1] != _agg(con, origin)[1]
    status = con.sql(f"SELECT status FROM ledger WHERE run_id = '{resumed}'").fetchone()[0]
    assert status == "complete"


@pytest.mark.slow
def test_keyed_resume_drops_removed_items(s3cfg, repo):
    _put_n(s3cfg, 3)
    origin = KeyedFlow.run(s3cfg, repo=repo, use_processes=False)
    _put_n(s3cfg, 2)
    resumed = KeyedFlow.run(s3cfg, repo=repo, resume=origin, use_processes=False)

    con = KeyedFlow.duck(s3cfg)
    old, new = _tokens(con, origin), _tokens(con, resumed)
    assert set(new) == {"file-0", "file-1"}  # file-2 gone from the corpus and the run
    assert new == {k: old[k] for k in new}
    assert _agg(con, resumed)[0] == 2


@pytest.mark.slow
def test_keyed_resume_unchanged_worklist_reuses_downstream(s3cfg, repo):
    _put_n(s3cfg, 2)
    origin = KeyedFlow.run(s3cfg, repo=repo, use_processes=False)
    resumed = KeyedFlow.run(s3cfg, repo=repo, resume=origin, use_processes=False)

    con = KeyedFlow.duck(s3cfg)
    assert _tokens(con, resumed) == _tokens(con, origin)
    assert _agg(con, resumed) == _agg(con, origin)  # collect step copied, not recomputed


@pytest.mark.slow
def test_resume_from_origin_without_fanout_keys_recomputes(s3cfg, repo):
    _put_n(s3cfg, 2)
    origin = KeyedFlow.run(s3cfg, repo=repo, use_processes=False)
    # simulate an origin written before fanout keys existed
    layout = Layout.for_flow(s3cfg, KeyedFlow)
    client = s3_client(s3cfg)
    meta = ledger.read_meta(client, s3cfg.bucket, layout.meta(origin))
    del meta["fanout_keys"]
    ledger.write_meta(client, s3cfg.bucket, layout.meta(origin), meta)

    with pytest.warns(UserWarning, match="no fanout keys"):
        resumed = KeyedFlow.run(s3cfg, repo=repo, resume=origin, use_processes=False)
    con = KeyedFlow.duck(s3cfg)
    old, new = _tokens(con, origin), _tokens(con, resumed)
    assert set(new) == set(old)
    assert all(new[k] != old[k] for k in new)  # nothing reusable -> recomputed


@pytest.mark.slow
def test_resume_latest_uses_newest_complete_run(s3cfg, repo):
    _put_n(s3cfg, 2)
    first = KeyedFlow.run(s3cfg, repo=repo, use_processes=False, resume="latest")  # no prior: fresh
    _put_n(s3cfg, 3)
    second = KeyedFlow.run(s3cfg, repo=repo, use_processes=False, resume="latest")

    con = KeyedFlow.duck(s3cfg)
    t1, t2 = _tokens(con, first), _tokens(con, second)
    assert set(t2) == {"file-0", "file-1", "file-2"}
    assert t2["file-0"] == t1["file-0"] and t2["file-1"] == t1["file-1"]
    ptr = con.sql(f"SELECT origin_run_id FROM ledger WHERE run_id = '{second}'").fetchone()[0]
    assert ptr == first

    with pytest.raises(ValueError, match="do not pass params"):
        KeyedFlow.run(s3cfg, repo=repo, resume="latest", params={"x": 1})
    with pytest.raises(ValueError, match="only applies to a resumed run"):
        KeyedFlow.run(s3cfg, repo=repo, allow_commit_drift=True)


@pytest.mark.slow
def test_branch_reuse_tainted_by_other_keyed_ancestor(s3cfg, repo):
    """Matching fanout keys must not be reused when another input changed."""
    _put_n(s3cfg, 1)
    origin = TwoKeyFlow.run(s3cfg, repo=repo, use_processes=False)
    _put_n(s3cfg, 2)
    resumed = TwoKeyFlow.run(s3cfg, repo=repo, resume=origin, use_processes=False)

    con = TwoKeyFlow.duck(s3cfg)
    items = {r[0] for r in con.sql(f"SELECT item FROM work WHERE run_id = '{resumed}'").fetchall()}
    assert items == {"fixed-0@2", "fixed-1@2"}  # recomputed against the new collect input


def test_duplicate_fanout_keys_refused(s3cfg, repo):
    with pytest.raises(RuntimeError, match="duplicate fanout key"):
        DupKeyFlow.run(s3cfg, repo=repo, use_processes=False)


def test_write_part_extracts_and_validates_fanout_key(s3cfg):
    client = s3_client(s3cfg)
    one = pa.table({"g": [0, 0], "item": ["a", "a"]})
    assert write_part(client, s3cfg.bucket, "k1", one, KeyedItem, fanout_key="item") == "a"
    assert write_part(client, s3cfg.bucket, "k2", one, KeyedItem) is None

    two = pa.table({"g": [0, 1], "item": ["a", "b"]})
    with pytest.raises(ConstraintViolation, match="exactly one distinct"):
        write_part(client, s3cfg.bucket, "k3", two, KeyedItem, fanout_key="item")
    empty = pa.table({"g": pa.array([], pa.int64()), "item": pa.array([], pa.string())})
    with pytest.raises(ConstraintViolation, match="empty"):
        write_part(client, s3cfg.bucket, "k4", empty, KeyedItem, fanout_key="item")
