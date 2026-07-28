"""Run state: the part bookkeeping and the two records a run is written to.

The end-to-end guarantees live in test_resume_contract.py; this pins the types
in isolation, where an out-of-order completion is cheap to construct.
"""

import datetime as dt

from ionflow.state import Progress, RunRecord, Slot

STARTED = dt.datetime(2026, 7, 28, 9, 0, 0)
FINISHED = dt.datetime(2026, 7, 28, 9, 5, 0)


def _record(**kw):
    base = dict(
        run_id="r1",
        status="running",
        started=STARTED,
        finished=None,
        commit="abc123",
        origin_run_id=None,
        params={"mult": 2},
        custom={},
        fanout_keys={},
        custom_fields=[],
    )
    return RunRecord(**{**base, **kw})


def test_slot_round_trips_the_on_disk_form():
    for text in ("vectors/0", "vectors/12", "up_load/7"):
        slot = Slot.parse(text)
        assert f"{slot.step}/{slot.branch}" == text


def test_keys_are_in_branch_order_not_completion_order():
    p = Progress()
    for i in (2, 0, 1):  # a worker pool completes items in any order
        p.add(Slot("work", i), f"runs/work/part_{i}.parquet")
    assert p.keys("work") == [f"runs/work/part_{i}.parquet" for i in (0, 1, 2)]


def test_width_and_membership_ignore_other_steps():
    p = Progress()
    p.add(Slot("work", 0), "k")
    p.add(Slot("agg", 0), "k")
    assert p.width("work") == 1
    assert Slot("agg", 0) in p
    assert Slot("work", 1) not in p
    assert p.width("absent") == 0 and p.keys("absent") == []


def test_a_repeated_add_does_not_widen_the_step():
    p = Progress()
    p.add(Slot("work", 0), "k0")
    p.add(Slot("work", 0), "k0")
    assert p.width("work") == 1


def test_to_list_is_the_meta_json_form():
    p = Progress()
    p.add(Slot("work", 1), "k")
    p.add(Slot("agg", 0), "k")
    p.add(Slot("work", 0), "k")
    assert p.to_list() == ["agg/0", "work/0", "work/1"]
    assert {Slot.parse(k) for k in p.to_list()} == {Slot("agg", 0), Slot("work", 0), Slot("work", 1)}


def test_meta_of_a_complete_run_carries_its_outcome():
    rec = _record(status="complete", finished=FINISHED, custom={"note": "ok"}, fanout_keys={"shards": [2, 7]})
    meta = rec.meta(["work/0"])
    assert list(meta) == [
        "run_id",
        "started",
        "finished",
        "commit",
        "origin_run_id",
        "steps_complete",
        "fanout_keys",
        "params",
        "custom",
    ]
    assert meta["started"] == "2026-07-28T09:00:00"
    assert meta["finished"] == "2026-07-28T09:05:00"
    assert meta["fanout_keys"] == {"shards": [2, 7]}  # ints stay ints (0008)
    assert meta["custom"] == {"note": "ok"}


def test_a_failed_run_claims_no_outcome():
    # the ledger records when a failed run stopped; _meta.json's `finished` is
    # publish's success flag, so it must stay empty
    rec = _record(status="failed", finished=FINISHED, custom={"note": "ok"})
    assert rec.meta(["work/0"])["finished"] is None
    assert rec.meta(["work/0"])["custom"] == {}
    assert rec.ledger_row()["finished"] == FINISHED
    assert "note" not in rec.ledger_row()


def test_ledger_row_flattens_params_and_ledger_columns():
    rec = _record(status="complete", finished=FINISHED, custom={"note": "ok"})
    assert rec.ledger_row() == {
        "run_id": "r1",
        "status": "complete",
        "started": STARTED,
        "finished": FINISHED,
        "commit": "abc123",
        "origin_run_id": None,
        "mult": 2,
        "note": "ok",
    }
