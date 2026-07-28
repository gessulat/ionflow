"""The run-state surfaces resume rests on, pinned as bytes and orderings.

test_fanout/test_subdag/test_incremental assert what a resume *does*; this file
asserts the artifacts that behaviour reads back — `_meta.json`'s exact shape
(a cross-version surface: today's release must be readable as an origin), the
ledger row per status, and the order a step's parts are registered in.
"""

import datetime as dt
import json
import pathlib
import tempfile

import duckdb
import pytest
from flows import (
    ORDER_ITEMS,
    FlakyFlow,
    IntKeyFlow,
    OrderFlow,
    PartialFanFlow,
    PipelineFlow,
    ProbeFlow,
    exec_counts,
)
from schrein.ducklake import CATALOG_KEY

from examples.embed import EmbedDocs
from ionflow import lake
from ionflow.git import require_clean
from ionflow.space import Space
from ionflow.store import Store

META_KEYS = [
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

LEDGER_COLS = ["run_id", "status", "started", "finished", "commit", "origin_run_id"]


def _space(cfg, flow_cls):
    return Space.for_flow(Store.of(cfg), flow_cls)


def _ledger(cfg, flow_cls, run_id):
    space = _space(cfg, flow_cls)
    return next(r for r in space.read_rows(space.ledger) if r["run_id"] == run_id)


def _registered(cfg, flow_cls, step):
    """A step's data files in catalog registration order (basenames).

    Read from the catalog's own metadata through a plain ATTACH of the
    downloaded artifact: `data_file_id` is the order `register_parts` wrote
    them in, and that order is the table's scan order for every reader.
    """
    space = _space(cfg, flow_cls)
    blob = lake.Catalog(space).lake.register.read()[0]
    with tempfile.TemporaryDirectory() as tmp:
        local = pathlib.Path(tmp) / CATALOG_KEY
        local.write_bytes(blob)
        con = duckdb.connect()
        try:
            con.execute(f"ATTACH '{local}' AS meta (READ_ONLY)")
            rows = con.sql(
                "SELECT f.path FROM meta.ducklake_data_file f JOIN meta.ducklake_table t USING (table_id) "
                f"WHERE t.table_name = '{step}' ORDER BY f.data_file_id"
            ).fetchall()
        finally:
            con.close()
    return [pathlib.PurePosixPath(p).name for (p,) in rows]


def test_meta_json_shape_of_a_complete_run(cfg, repo):
    """_meta.json's keys, and steps_complete as sorted "step/branch" strings."""
    run_id = EmbedDocs.run(cfg, repo=repo, use_processes=False, params={"min_len": 1})

    space = _space(cfg, EmbedDocs)
    meta = json.loads(space.store.get(space.meta(run_id)).decode())  # plain UTF-8 JSON
    assert list(meta) == META_KEYS
    assert meta["steps_complete"] == [
        "by_source/0",
        "by_source/1",
        "docs/0",
        "summary/0",
        "vectors/0",
        "vectors/1",
    ]
    assert meta["run_id"] == run_id
    assert meta["commit"] == require_clean(repo)
    assert meta["origin_run_id"] is None
    assert meta["fanout_keys"] == {"by_source": ["arxiv", "wiki"]}
    assert meta["params"] == {"min_len": 1}
    assert meta["custom"] == {"n_docs": 4}
    started = dt.datetime.fromisoformat(meta["started"])
    assert started.tzinfo is None  # naive UTC, the project's one convention
    assert dt.datetime.fromisoformat(meta["finished"]) >= started


def test_meta_json_of_a_failed_run_claims_no_outcome(cfg, repo, monkeypatch):
    """A failed run keeps its progress but no finish time and no ledger columns."""
    monkeypatch.setenv("PROBE_FAIL", "1")
    with pytest.raises(RuntimeError, match="boom in doubled"):
        ProbeFlow.run(cfg, repo=repo, use_processes=False, params={"mult": 3})

    space = _space(cfg, ProbeFlow)
    run_id = space.read_rows(space.ledger)[0]["run_id"]
    meta = space.read_meta(run_id)
    assert list(meta) == META_KEYS
    assert meta["steps_complete"] == ["source/0"]
    assert meta["finished"] is None  # publish reads this as the success flag
    assert meta["custom"] == {}  # `note` was set, but only a complete run records it
    assert meta["params"] == {"mult": 3}


def test_partial_fanout_is_never_recorded_as_complete(cfg, repo):
    """Two items reached S3; the fan-out still counts as nothing (all-or-nothing)."""
    with pytest.raises(RuntimeError, match="fan-out died"):
        PartialFanFlow.run(cfg, repo=repo, use_processes=False)

    space = _space(cfg, PartialFanFlow)
    run_id = space.read_rows(space.ledger)[0]["run_id"]
    assert space.read_meta(run_id)["steps_complete"] == []
    # the parts really are there — completion is not "whatever exists on S3",
    # or a resume would silently continue with fewer branches
    assert space.store.list(space.step_dir(run_id, "parts") + "/") == [
        space.part(run_id, "parts", 0),
        space.part(run_id, "parts", 1),
    ]


def test_resume_reads_a_hand_written_origin_meta(cfg, repo, tmp_path, monkeypatch):
    """The origin's `_meta.json`, written by hand in today's on-disk format.

    An origin from any release with this format must still drive a resume: the
    "step/branch" strings alone decide what is copied and what re-runs.
    """
    monkeypatch.setenv("FLAKY_DIR", str(tmp_path))
    origin = FlakyFlow.run(cfg, repo=repo, use_processes=False)

    space = _space(cfg, FlakyFlow)
    space.write_meta(
        origin,
        {
            "run_id": origin,
            "started": "2026-01-01T00:00:00",
            "finished": "2026-01-01T00:01:00",
            "commit": require_clean(repo),
            "origin_run_id": None,
            "steps_complete": ["parts/0", "parts/1", "parts/2", "source/0", "work/0", "work/2"],
            "fanout_keys": {},
            "params": {},
            "custom": {},
        },
    )
    resumed = FlakyFlow.run(cfg, repo=repo, resume=origin, use_processes=False)

    # only the branch the meta omitted re-ran
    assert exec_counts(tmp_path, "work") == {"0": 1, "1": 2, "2": 1}

    con = FlakyFlow.duck(cfg)
    assert con.sql(f"SELECT n, total FROM agg WHERE run_id = '{resumed}'").fetchone() == (3, 6)
    assert con.sql(f"SELECT status FROM ledger WHERE run_id = '{resumed}'").fetchone()[0] == "complete"


def test_int_fanout_keys_survive_the_json_meta(cfg, repo):
    """Integer item keys stay ints in `_meta.json`, so a resume can match them."""
    origin = IntKeyFlow.run(cfg, repo=repo, use_processes=False)

    space = _space(cfg, IntKeyFlow)
    assert space.read_meta(origin)["fanout_keys"] == {"shards": [2, 7]}

    resumed = IntKeyFlow.run(cfg, repo=repo, resume=origin, use_processes=False)
    con = IntKeyFlow.duck(cfg)
    tokens = dict(con.sql(f"SELECT item, token FROM work WHERE run_id = '{resumed}'").fetchall())
    old = dict(con.sql(f"SELECT item, token FROM work WHERE run_id = '{origin}'").fetchall())
    assert tokens == old  # both items matched by key and were copied, not recomputed


@pytest.mark.slow
def test_registration_order_is_branch_order_not_completion_order(cfg, repo, tmp_path, monkeypatch):
    """Branches land back to front; their parts still register 0, 1, 2.

    Registration order is the data-file order in the catalog, hence the scan
    order every reader sees — insertion order would make a reducer's `first()`
    or an `arg_max` tie vary from run to run.
    """
    monkeypatch.setenv("ORDER_DIR", str(tmp_path))
    run_id = OrderFlow.run(cfg, repo=repo, use_processes=True, max_workers=ORDER_ITEMS)

    con = OrderFlow.duck(cfg)
    by_finish = [g for (g,) in con.sql(f"SELECT g FROM work WHERE run_id = '{run_id}' ORDER BY finished_at").fetchall()]
    assert by_finish == [2, 1, 0]  # the test is only meaningful if completion was reversed

    parts = [f"part_{i}.parquet" for i in range(ORDER_ITEMS)]
    assert _registered(cfg, OrderFlow, "work") == parts
    assert _registered(cfg, OrderFlow, "parts") == parts
    space = _space(cfg, OrderFlow)
    assert space.part_keys(run_id, "work") == [space.part(run_id, "work", i) for i in range(ORDER_ITEMS)]
    # and the registered order is what a plain scan returns
    assert [g for (g,) in con.sql(f"SELECT g FROM work WHERE run_id = '{run_id}'").fetchall()] == [0, 1, 2]


@pytest.mark.slow
def test_sub_steps_complete_for_every_item_survive_a_later_failure(s3cfg, repo, tmp_path, monkeypatch):
    """0015: completion is per (item, sub-step), and only full sub-steps register."""
    Store.of(s3cfg).put("side/n", b"2")
    monkeypatch.setenv("PIPE_DIR", str(tmp_path))
    monkeypatch.setenv("PIPE_FAIL", "search:raw-1")

    with pytest.raises(RuntimeError, match="search failed on raw-1"):
        PipelineFlow.run(s3cfg, repo=repo, use_processes=False)

    space = _space(s3cfg, PipelineFlow)
    run_id = space.read_rows(space.ledger)[0]["run_id"]
    assert space.read_meta(run_id)["steps_complete"] == [
        "convert/0",
        "convert/1",
        "jobs/0",
        "jobs/1",
        "search/0",
        "upload_db/0",
        "upload_db/1",
    ]

    con = PipelineFlow.duck(s3cfg)
    counts = {
        step: con.sql(f"SELECT count(*) FROM {step} WHERE run_id = '{run_id}'").fetchone()[0]
        for step in ("convert", "upload_db", "search")
    }
    # sub-steps done for every item stay queryable; `search`, done for one item
    # only, registers nothing although its part is on S3
    assert counts == {"convert": 2, "upload_db": 2, "search": 0}
    assert space.part(run_id, "search", 0) in space.store.list(space.step_dir(run_id, "search") + "/")


def test_ledger_row_per_status(cfg, repo, tmp_path, monkeypatch):
    """The ledger row's exact columns and values while running, complete, failed."""
    probe = tmp_path / "ledger.json"
    monkeypatch.setenv("LEDGER_PROBE", str(probe))
    done = ProbeFlow.run(cfg, repo=repo, use_processes=False, params={"mult": 3})
    mid_run = json.loads(probe.read_text())

    assert len(mid_run) == 1
    running = mid_run[0]
    assert list(running) == [*LEDGER_COLS, "mult", "note"]  # params, then Ledger columns
    assert running["run_id"] == done
    assert running["status"] == "running"
    assert running["finished"] is None
    assert running["origin_run_id"] is None
    assert running["mult"] == 3  # params are ledger columns from the first upsert
    assert running["note"] is None

    complete = _ledger(cfg, ProbeFlow, done)
    assert list(complete) == [*LEDGER_COLS, "mult", "note"]
    assert complete["status"] == "complete"
    assert complete["commit"] == require_clean(repo)
    assert complete["started"] <= complete["finished"]
    assert complete["started"].tzinfo is None  # naive UTC
    assert (complete["mult"], complete["note"]) == (3, "set during the run")

    monkeypatch.setenv("PROBE_FAIL", "1")
    with pytest.raises(RuntimeError, match="boom in doubled"):
        ProbeFlow.run(cfg, repo=repo, use_processes=False, params={"mult": 3})
    space = _space(cfg, ProbeFlow)
    failed = next(r for r in space.read_rows(space.ledger) if r["run_id"] != done)
    assert failed["status"] == "failed"
    assert failed["finished"] is not None  # unlike _meta.json, the ledger times a failure
    assert failed["mult"] == 3
    assert failed["note"] is None  # ledger columns are recorded only by a complete run
