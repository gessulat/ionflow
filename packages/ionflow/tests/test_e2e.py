import pathlib

import pytest
from flows import FlakyFlow

from examples.embed import EmbedDocs
from ionflow import runner
from ionflow.git import DirtyRepoError
from ionflow.space import Space
from ionflow.store import Store


def test_run_end_to_end(cfg, repo):
    run_id = EmbedDocs.run(cfg, repo=repo, use_processes=False)
    con = EmbedDocs.duck(cfg)

    # fan-out produced one part per source, aggregated back to a summary
    summary = con.sql(f"SELECT * FROM summary WHERE run_id = '{run_id}'").fetchall()
    assert summary[0][:2] == (2, 4)  # n_sources, n_docs
    assert summary[0][2] > 0  # avg_norm

    # each source embedded independently across the fan-out
    counts = con.sql(
        f"SELECT source, count(*), min(norm) > 0 FROM vectors WHERE run_id = '{run_id}' GROUP BY source ORDER BY source"
    ).fetchall()
    assert counts == [("arxiv", 2, True), ("wiki", 2, True)]

    # the shaped embedding column round-trips as a 64-wide list
    width = con.sql(f"SELECT DISTINCT len(embedding) FROM vectors WHERE run_id = '{run_id}'").fetchall()
    assert width == [(64,)]

    # ledger row complete with the custom column populated
    row = con.sql(f"SELECT status, finished, n_docs FROM ledger WHERE run_id = '{run_id}'").fetchone()
    assert row[0] == "complete"
    assert row[1] is not None
    assert row[2] == 4  # n_docs custom ledger column
    # per-part progress (the manifest) lives in _meta.json, not the ledger

    # resource metrics: every step sampled at least once; stats derivable
    from ionflow import query

    sampled = {r[0] for r in con.sql(f"SELECT DISTINCT step FROM metrics WHERE run_id = '{run_id}'").fetchall()}
    assert {"docs", "by_source", "vectors", "summary"} <= sampled
    stats = query.step_stats(con, run_id).fetchall()
    assert len(stats) == 4
    assert all(r[2] > 0 for r in stats)  # peak_rss_mb

    # a launch sees the flow; the catalog schema matches the declarations
    from ionflow.lake import schema_drift
    from ionflow.space import Space
    from ionflow.store import Store
    from ionflow.tui import scan_runs

    store = Store.of(cfg)
    assert scan_runs(store) == {EmbedDocs.flow_id()}
    assert schema_drift(EmbedDocs, Space.for_flow(store, EmbedDocs)) == []


@pytest.mark.slow
def test_run_with_processes(cfg, repo):
    run_id = EmbedDocs.run(cfg, repo=repo, use_processes=True, max_workers=2)
    con = EmbedDocs.duck(cfg)
    n_docs = con.sql(f"SELECT n_docs FROM summary WHERE run_id = '{run_id}'").fetchone()[0]
    assert n_docs == 4
    # branch workers (separate processes) contribute per-branch samples
    branches = con.sql(
        f"SELECT DISTINCT branch FROM metrics WHERE run_id = '{run_id}' "
        "AND step = 'vectors' AND branch IS NOT NULL ORDER BY branch"
    ).fetchall()
    assert branches == [("arxiv",), ("wiki",)]  # the branch label IS the item key
    # stats count workers only; the orchestrator's pool-wait is not the step's cost
    from ionflow import query

    totals = [r for r in query.step_stats(con, run_id).fetchall() if r[0] == "vectors"][0]
    assert totals[5] == 2  # processes


def test_dirty_git_refused(cfg, clean_repo):
    pathlib.Path(clean_repo, "dirty.txt").write_text("x")  # untracked -> dirty
    with pytest.raises(DirtyRepoError):
        EmbedDocs.run(cfg, repo=clean_repo)


def test_resume_refuses_commit_drift(cfg, clean_repo):
    from conftest import _git
    from flows import EmptyFanFlow

    origin = EmptyFanFlow.run(cfg, repo=clean_repo, use_processes=False)
    pathlib.Path(clean_repo, "flow.py").write_text("# changed\n")
    _git(clean_repo, "-c", "user.email=t@t.com", "-c", "user.name=t", "commit", "-am", "fix")

    with pytest.raises(runner.CommitDriftError, match="allow_commit_drift"):
        EmptyFanFlow.run(cfg, repo=clean_repo, resume=origin, use_processes=False)
    with pytest.warns(UserWarning, match="different commit"):
        resumed = EmptyFanFlow.run(cfg, repo=clean_repo, resume=origin, use_processes=False, allow_commit_drift=True)
    assert resumed != origin


def test_meta_json_is_the_cross_version_resume_surface(cfg, repo):
    """`_meta.json`'s shape is what a LATER release reads to resume an older run."""
    space = Space.for_flow(Store.of(cfg), FlakyFlow)
    run_id = FlakyFlow.run(cfg, repo=repo, use_processes=False)
    meta = space.read_meta(run_id)
    assert list(meta) == [
        "run_id",
        "status",
        "started",
        "finished",
        "commit",
        "origin_run_id",
        "manifest",
        "params",
        "custom",
        "artifacts",
        "parent",
        "activities",
        "version",
        "files",
    ]
    # the manifest maps (step, item key) -> the part object holding it
    assert {s: [k for k, _ in pairs] for s, pairs in meta["manifest"].items()} == {
        "agg": [0],
        "parts": [0, 1, 2],
        "source": [0],
        "work": [0, 1, 2],
    }
    assert all(p == space.part(run_id, s, k) for s, pairs in meta["manifest"].items() for k, p in pairs)

    # meta is the RECOVERY surface: a record written by hand in today's format
    # folds back into the catalog via backfill, and the restored run drives a
    # resume — `work` item 1 is absent, so exactly that item recomputes
    from ionflow.lake import backfill

    dropped = {("work", 1), ("agg", 0)}
    origin = dict(
        meta,
        run_id="hand-written",
        origin_run_id=None,
        manifest={
            s: [[k, p.replace(f"run_id={run_id}", "run_id=hand-written")] for k, p in pairs if (s, k) not in dropped]
            for s, pairs in meta["manifest"].items()
        },
    )
    for key in space.store.list(space.run_dir(run_id) + "/"):
        space.store.client.copy_object(
            Bucket=space.store.bucket,
            CopySource={"Bucket": space.store.bucket, "Key": key},
            Key=key.replace(f"run_id={run_id}", "run_id=hand-written"),
        )
    space.write_meta("hand-written", origin)
    backfill(FlakyFlow, space, replace=True)
    resumed = FlakyFlow.run(cfg, repo=repo, resume="hand-written", use_processes=False)
    assert FlakyFlow.duck(cfg).sql(f"SELECT n, total FROM agg WHERE run_id = '{resumed}'").fetchone() == (3, 6)
