"""The reuse policy in isolation: pure `Planner` decisions over plain Manifests.

What actually re-runs is pinned end to end in test_fanout / test_incremental /
test_subdag; this pins the policy itself — no store, no workers, no moto.
"""

from flows import FlakyFlow, KeyedChainFlow, KeyedFlow, TwoKeyFlow

from ionflow.plan import Planner
from ionflow.state import Manifest, Slot


def _manifest(spec):
    """{step: keys} -> a Manifest with placeholder origin paths."""
    m = Manifest()
    for step, keys in spec.items():
        for k in keys:
            m.add(Slot(step, k), f"runs/run_id=o/{step}/{k}")
    return m


def test_fresh_run_plans_nothing():
    p = Planner(KeyedFlow._dag, Manifest())
    assert p.upfront() == []
    assert p.node_reuse(_manifest({"items": ["file-0"]}), "agg") == []
    assert p.member_reuse(_manifest({"items": ["file-0"]}), KeyedFlow._dag.groups["items"]) == []


def test_unkeyed_origin_is_reused_whole_and_upfront():
    origin = _manifest({"source": [0], "parts": [0, 1, 2], "work": [0, 1], "agg": []})
    p = Planner(FlakyFlow._dag, origin)
    # nothing keyed -> nothing deferred: every origin part, partial steps included
    assert set(p.upfront()) == {s for s, _ in origin.slots()}
    assert p.node_reuse(origin, "agg") == []  # not deferred, so not the lazy path's job


def test_keyed_origin_defers_and_matches_by_key():
    origin = _manifest({"items": ["file-0", "file-1"], "work": ["file-0", "file-1"], "agg": [0]})
    p = Planner(KeyedFlow._dag, origin)
    assert p.upfront() == []  # everything is downstream of the keyed fan-out

    grown = _manifest({"items": ["file-0", "file-1", "file-2"]})  # the re-run work-list
    assert set(p.member_reuse(grown, KeyedFlow._dag.groups["items"])) == {
        Slot("work", "file-0"),
        Slot("work", "file-1"),
    }
    assert p.node_reuse(grown, "agg") == []  # key set changed: the collect recomputes

    same = _manifest({"items": ["file-0", "file-1"]})
    assert p.node_reuse(same, "agg") == [Slot("agg", 0)]  # unchanged: reused


def test_changed_collect_input_taints_matching_keys():
    origin = _manifest({"fixed": ["fixed-0"], "others": ["other-0"], "work": ["fixed-0"]})
    p = Planner(TwoKeyFlow._dag, origin)
    group = TwoKeyFlow._dag.groups["fixed"]

    tainted = _manifest({"fixed": ["fixed-0"], "others": ["other-0", "other-1"]})
    assert p.member_reuse(tainted, group) == []  # matching key, changed collect: recompute

    clean = _manifest({"fixed": ["fixed-0"], "others": ["other-0"]})
    assert p.member_reuse(clean, group) == [Slot("work", "fixed-0")]


def test_chained_members_match_through_the_root_key():
    origin = _manifest({"items": ["file-0"], "work": ["file-0"], "refine": ["file-0"]})
    p = Planner(KeyedChainFlow._dag, origin)
    grown = _manifest({"items": ["file-0", "file-1"]})
    assert set(p.member_reuse(grown, KeyedChainFlow._dag.groups["items"])) == {
        Slot("work", "file-0"),
        Slot("refine", "file-0"),
    }


def test_preview_counts_reuse_vs_missing_work():
    failed = _manifest({"items": ["file-0", "file-1"], "work": ["file-0"]})
    assert Planner(KeyedFlow._dag, failed).preview() == {
        "items": (0, 2),  # a keyed fan-out always re-runs
        "work": (1, 1),  # one item reusable, one missing
        "agg": (0, 1),  # never ran
    }
    # a step with no origin parts and no root has unknown width
    assert Planner(FlakyFlow._dag, Manifest()).preview()["parts"] == (0, -1)
