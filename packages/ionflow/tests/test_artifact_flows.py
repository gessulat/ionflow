import pytest
from artifact_flows import BuildLibrary, Fasta, Index, Library, PublishFiles, Raw, Search, SelectIndex

from ionflow import Artifacts
from ionflow.lake import Catalog, backfill
from ionflow.space import Space
from ionflow.store import Store


def test_artifact_steps_inherit_table_ancestry_and_expose_sql(cfg, repo, tmp_path):
    cfg = cfg
    registry = Artifacts(cfg)
    p = tmp_path / "human.fa"
    p.write_text(">p\nPEPTIDE\n")
    fasta = registry.put(Fasta.from_path(p, organism="human"))
    run_id = BuildLibrary.run(
        cfg, repo=repo, use_processes=False, artifacts={"fasta": fasta.id}, params={"work_dir": str(tmp_path)}
    )
    library = registry.get(registry.records(Library._kind)[0]["id"], Library)
    assert registry.parents(library.id) == [fasta.id]
    assert registry.producers(library.id) == [
        {"flow_id": BuildLibrary.flow_id(), "run_id": run_id, "step": "library", "key": 0}
    ]
    con = BuildLibrary.duck(cfg)
    assert con.sql("SELECT id, model, run_id FROM library").fetchone() == (library.id, "7.2", run_id)
    assert con.sql("SELECT count(*) FROM artifacts").fetchone() == (2,)
    assert registry.duck().sql("SELECT count(*) FROM artifact_events WHERE event='generated'").fetchone() == (1,)
    assert not Store.of(cfg).list(f"{cfg.prefix}/{BuildLibrary.flow_id()}/")


@pytest.mark.slow
@pytest.mark.parametrize("processes", [False, True])
def test_branch_scoped_artifacts_resume_failure_and_recovery(cfg, repo, tmp_path, processes):
    registry = Artifacts(cfg)
    p = tmp_path / "payload"
    p.write_text("library")
    library = registry.put(Library.from_path(tmp_path, model="7.2", members={"data": p}))
    raws = []
    for name in ("a", "bb"):
        p = tmp_path / (name + ".raw")
        p.write_text(name)
        raws.append(registry.put(Raw.from_path(p)))
    given = {"library": library.id, "raw_files": [r.id for r in raws]}
    space = Space.for_flow(Store.of(cfg), Search)
    with pytest.raises(RuntimeError, match="failed after"):
        Search.run(
            cfg,
            repo=repo,
            use_processes=processes,
            max_workers=2,
            artifacts=given,
            params={"work_dir": str(tmp_path), "fail": True},
        )
    first = Catalog(space).runs()[0]["run_id"]
    row, triples = Catalog(space).run_state(first)
    scores = [a for a in row["activities"].values() if a["step"] == "score"]
    assert len(scores) == 2
    for activity in scores:
        used_raw = {u["artifact"]["id"] for u in activity["uses"] if u["artifact"]["kind"] == Raw._kind}
        assert used_raw == {activity["key"]}
        assert set(activity["outputs"][0]["parents"]) == {library.id}
    assert len(registry.records(Index._kind)) == 1
    assert all(registry.parents(raw.id) == [] and registry.producers(raw.id) == [] for raw in raws)
    with pytest.raises(ValueError, match="reuses"):
        Search.run(cfg, repo=repo, resume=first, artifacts=given)
    registry.update(library.id, revision=1, name="edited later")
    with pytest.raises(RuntimeError, match="failed after"):
        Search.run(cfg, repo=repo, use_processes=processes, max_workers=2, resume=first)
    resumed = Catalog(space).runs()[-1]["run_id"]
    row, _ = Catalog(space).run_state(resumed)
    outputs = [o for a in row["activities"].values() for o in a["outputs"] if o["role"] == "index"]
    assert all(o["event"] == "reused" and o["origin"]["run_id"] == first for o in outputs)
    assert row["artifacts"]["library"]["name"] == library.name
    space.store.delete(Catalog(space).key)
    assert backfill(Search, space) == 2
    assert Catalog(space).run_state(resumed)[0]["activities"] == row["activities"]


def test_invalid_bindings_fail_before_creating_run(cfg, repo, tmp_path):
    with pytest.raises(ValueError, match="artifact inputs"):
        BuildLibrary.run(cfg, repo=repo, artifacts={}, params={"work_dir": str(tmp_path)})
    assert Store.of(cfg).flow_ids() == []


def test_inventory_is_queryable_before_any_flow_runs(cfg):
    con = Artifacts(cfg).duck()
    assert con.sql("SELECT count(*) FROM artifacts").fetchone() == (0,)
    assert con.sql("SELECT count(*) FROM artifact_events").fetchone() == (0,)


def test_fanout_can_publish_before_returning_and_retains_failed_consumption(cfg, repo, tmp_path):
    registry = Artifacts(cfg)
    path = tmp_path / "input.fa"
    path.write_text("input")
    fasta = registry.put(Fasta.from_path(path))
    given = {"fasta": fasta.id}
    run_id = PublishFiles.run(cfg, repo=repo, use_processes=False, artifacts=given, params={"work_dir": str(tmp_path)})
    space = Space.for_flow(Store.of(cfg), PublishFiles)
    row, triples = Catalog(space).run_state(run_id)
    for _, key, aid in triples:
        activity = next(a for a in row["activities"].values() if a["key"] == key)
        assert activity["outputs"][0]["artifact"]["id"] == aid == key
        assert activity["outputs"][0]["event"] == "generated"
        assert registry.parents(aid) == [fasta.id]
    with pytest.raises(RuntimeError, match="before emitting"):
        PublishFiles.run(
            cfg,
            repo=repo,
            use_processes=False,
            artifacts=given,
            params={"work_dir": str(tmp_path), "fail": True},
        )
    assert len({a["run_id"] for a in registry.consumers(fasta.id)}) == 2


def test_artifact_views_cast_json_lists_and_preserve_reference_identity(cfg, repo, tmp_path):
    path = tmp_path / "index"
    path.write_text("index bytes")
    registry = Artifacts(cfg)
    ref = registry.put(Index.from_path(path, method="HCD", nces=[27, 28]))
    SelectIndex.run(cfg, repo=repo, use_processes=False, artifacts={"index": ref.id})
    con = SelectIndex.duck(cfg)
    assert con.sql("SELECT nces, typeof(nces) FROM selected").fetchone() == ([27, 28], "BIGINT[]")
    assert registry.producers(ref.id) == [] and registry.parents(ref.id) == []


@pytest.mark.slow
def test_sync_single_resumed_run_copies_original_parts_and_artifact_parents(localcfg, cfg, repo, tmp_path):
    from dataclasses import replace

    registry = Artifacts(localcfg)
    p = tmp_path / "human.fa"
    p.write_text(">p\nPEPTIDE\n")
    fasta = registry.put(Fasta.from_path(p))
    first = BuildLibrary.run(
        localcfg, repo=repo, use_processes=False, artifacts={"fasta": fasta.id}, params={"work_dir": str(tmp_path)}
    )
    second = BuildLibrary.run(localcfg, repo=repo, use_processes=False, resume=first)
    source = Space.for_flow(Store.of(localcfg), BuildLibrary)
    target_cfg = replace(cfg, prefix="nested/target")
    target = Space.for_flow(Store.of(target_cfg), BuildLibrary)
    assert source.sync_to(target, BuildLibrary, run_id=second)["runs"] == 2
    con = BuildLibrary.duck(target_cfg)
    assert con.sql("SELECT count(*) FROM digest").fetchone() == (2,)
    library_id = con.sql("SELECT id FROM library LIMIT 1").fetchone()[0]
    assert Artifacts(target_cfg).parents(library_id) == [fasta.id]
    assert Artifacts(target_cfg).producers(library_id)[0]["run_id"] == first
    assert target.read_meta(second)["manifest"]["digest"][0][1].startswith(target.root)
