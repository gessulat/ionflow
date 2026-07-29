"""Keyed (incremental) resume: fan-out items matched across runs by fanout_key."""

import pyarrow as pa
import pytest
from flows import DupKeyFlow, KeyedChainFlow, KeyedFlow, KeyedItem, TwoKeyFlow, exec_counts, put_n

from ionflow import ConstraintViolation
from ionflow.space import Space
from ionflow.store import Store


def _tokens(con, run_id):
    return dict(con.sql(f"SELECT item, token FROM work WHERE run_id = '{run_id}'").fetchall())


def _agg(con, run_id):
    return con.sql(f"SELECT n, token FROM agg WHERE run_id = '{run_id}'").fetchone()


@pytest.mark.slow
def test_keyed_resume_grow_shrink_unchanged(cfg, repo):
    """One incremental lineage: the work-list grows, then shrinks, then holds."""
    put_n(cfg, 3)
    origin = KeyedFlow.run(cfg, repo=repo, use_processes=False)
    put_n(cfg, 5)
    grown = KeyedFlow.run(cfg, repo=repo, resume=origin, use_processes=False)

    con = KeyedFlow.duck(cfg)
    old, new = _tokens(con, origin), _tokens(con, grown)
    assert set(new) == {f"file-{g}" for g in range(5)}
    for item in old:  # matched items were server-side copied, not recomputed
        assert new[item] == old[item]
    # the collect step reflects the grown work-list, so it was recomputed
    assert _agg(con, grown)[0] == 5
    assert _agg(con, grown)[1] != _agg(con, origin)[1]
    assert con.sql(f"SELECT status FROM ledger WHERE run_id = '{grown}'").fetchone()[0] == "complete"

    put_n(cfg, 2)
    shrunk = KeyedFlow.run(cfg, repo=repo, resume=grown, use_processes=False)
    con = KeyedFlow.duck(cfg)
    kept = _tokens(con, shrunk)
    assert kept == {k: new[k] for k in ("file-0", "file-1")}  # the rest gone from the run
    assert _agg(con, shrunk)[0] == 2

    same = KeyedFlow.run(cfg, repo=repo, resume=shrunk, use_processes=False)
    con = KeyedFlow.duck(cfg)
    assert _tokens(con, same) == kept
    assert _agg(con, same) == _agg(con, shrunk)  # unchanged key set: collect copied, not recomputed


@pytest.mark.slow
def test_resume_latest_uses_newest_complete_run(cfg, repo):
    put_n(cfg, 2)
    first = KeyedFlow.run(cfg, repo=repo, use_processes=False, resume="latest")  # no prior: fresh
    put_n(cfg, 3)
    second = KeyedFlow.run(cfg, repo=repo, use_processes=False, resume="latest")

    con = KeyedFlow.duck(cfg)
    t1, t2 = _tokens(con, first), _tokens(con, second)
    assert set(t2) == {"file-0", "file-1", "file-2"}
    assert t2["file-0"] == t1["file-0"] and t2["file-1"] == t1["file-1"]
    ptr = con.sql(f"SELECT origin_run_id FROM ledger WHERE run_id = '{second}'").fetchone()[0]
    assert ptr == first

    with pytest.raises(ValueError, match="do not pass params"):
        KeyedFlow.run(cfg, repo=repo, resume="latest", params={"x": 1})
    with pytest.raises(ValueError, match="only applies to a resumed run"):
        KeyedFlow.run(cfg, repo=repo, allow_commit_drift=True)


@pytest.mark.slow
def test_branch_reuse_tainted_by_other_keyed_ancestor(cfg, repo):
    """Matching fanout keys must not be reused when another input changed."""
    put_n(cfg, 1)
    origin = TwoKeyFlow.run(cfg, repo=repo, use_processes=False)
    put_n(cfg, 2)
    resumed = TwoKeyFlow.run(cfg, repo=repo, resume=origin, use_processes=False)

    con = TwoKeyFlow.duck(cfg)
    items = {r[0] for r in con.sql(f"SELECT item FROM work WHERE run_id = '{resumed}'").fetchall()}
    assert items == {"fixed-0@2", "fixed-1@2"}  # recomputed against the new collect input


@pytest.mark.slow
def test_branch_collects_a_non_sibling_fanout_in_a_worker(cfg, repo):
    """A spawned worker resolves a Collect of another fan-out from the payload's dep paths."""
    put_n(cfg, 3)
    run_id = TwoKeyFlow.run(cfg, repo=repo, use_processes=True)

    con = TwoKeyFlow.duck(cfg)
    items = {r[0] for r in con.sql(f"SELECT item FROM work WHERE run_id = '{run_id}'").fetchall()}
    assert items == {"fixed-0@3", "fixed-1@3"}


def _chain(con, run_id):
    """item -> (work token, refine token, the work token refine actually read)."""
    rows = con.sql(
        "SELECT w.item, w.token, r.token, r.upstream FROM work w JOIN refine r USING (item, run_id) "
        f"WHERE run_id = '{run_id}'"
    ).fetchall()
    return {item: (wt, rt, up) for item, wt, rt, up in rows}


def _assert_aligned(con, run_id, items):
    """Every item has exactly one work/refine pair, and refine read its own item's work."""
    chain = _chain(con, run_id)
    assert set(chain) == items
    n = con.sql(f"SELECT count(*) FROM refine WHERE run_id = '{run_id}'").fetchone()[0]
    assert n == len(items)  # no item computed twice, none dropped
    for item, (work_token, _, upstream) in chain.items():
        assert upstream == work_token, f"{item}: refine read another branch's work output"
    return chain


@pytest.mark.slow
def test_keyed_resume_chained_branch_matches_new_items(cfg, repo):
    """A grown work-list reuses matched branches through the whole chain."""
    put_n(cfg, 2)
    origin = KeyedChainFlow.run(cfg, repo=repo, use_processes=False)
    put_n(cfg, 3)
    resumed = KeyedChainFlow.run(cfg, repo=repo, resume=origin, use_processes=False)

    con = KeyedChainFlow.duck(cfg)
    old = _assert_aligned(con, origin, {"file-0", "file-1"})
    new = _assert_aligned(con, resumed, {"file-0", "file-1", "file-2"})
    for item in old:  # matched items copied through both branch steps, not recomputed
        assert new[item] == old[item]


@pytest.mark.slow
def test_keyed_resume_chained_branch_survives_reordered_worklist(cfg, repo, tmp_path, monkeypatch):
    """Item keys, not branch positions, decide reuse — even for a partial chained step."""
    put_n(cfg, 2)
    monkeypatch.setenv("CHAIN_DIR", str(tmp_path))
    monkeypatch.setenv("KEYED_CHAIN_FAIL", "file-1")
    with pytest.raises(RuntimeError, match="file-1"):
        KeyedChainFlow.run(cfg, repo=repo, use_processes=False)

    con = KeyedChainFlow.duck(cfg)
    origin = con.sql("SELECT run_id FROM ledger WHERE status = 'failed'").fetchone()[0]
    old_work = dict(con.sql(f"SELECT item, token FROM work WHERE run_id = '{origin}'").fetchall())
    monkeypatch.delenv("KEYED_CHAIN_FAIL")
    Store.of(cfg).put("side/order", b"rev")  # same keys, branch positions swapped
    resumed = KeyedChainFlow.run(cfg, repo=repo, resume=origin, use_processes=False)

    con = KeyedChainFlow.duck(cfg)
    new = _assert_aligned(con, resumed, {"file-0", "file-1"})
    assert {item: t for item, (t, _, _) in new.items()} == old_work  # work matched by key
    # file-0's refine completed in the origin and was copied despite moving branch;
    # only file-1, which had failed, ran again
    assert exec_counts(tmp_path, "refine") == {"file-0": 1, "file-1": 2}


def test_duplicate_fanout_keys_refused(cfg, repo):
    with pytest.raises(RuntimeError, match="duplicate fanout key"):
        DupKeyFlow.run(cfg, repo=repo, use_processes=False)


def test_write_part_extracts_and_validates_fanout_key(cfg):
    space = Space.for_flow(Store.of(cfg), KeyedFlow)
    one = pa.table({"g": [0, 0], "item": ["a", "a"]})
    assert space.write_part("k1", one, KeyedItem, fanout_key="item") == "a"
    assert space.write_part("k2", one, KeyedItem) is None

    two = pa.table({"g": [0, 1], "item": ["a", "b"]})
    with pytest.raises(ConstraintViolation, match="exactly one distinct"):
        space.write_part("k3", two, KeyedItem, fanout_key="item")
    empty = pa.table({"g": pa.array([], pa.int64()), "item": pa.array([], pa.string())})
    with pytest.raises(ConstraintViolation, match="empty"):
        space.write_part("k4", empty, KeyedItem, fanout_key="item")
