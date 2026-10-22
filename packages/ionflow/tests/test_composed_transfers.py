import pytest
from subflow_flows import ArtifactPipeline, Convert, Nested, Pipeline, Raw, RawDB

from ionflow import Artifacts, Browser, Config, Storage
from ionflow.lake import Catalog, _meta_row
from ionflow.storage import execute_transfer
from ionflow.store import Store


@pytest.mark.slow
def test_parent_transfer_includes_child_results_and_directional_defaults(composition, workspaces, repo):
    c = composition
    source, receiver = workspaces
    raw = Artifacts(source.local).put(Raw.from_path(c.raw.local_path()))
    run_id = Pipeline.run(
        source.config,
        repo=repo,
        use_processes=False,
        max_workers=1,
        artifacts={"raw": raw.id},
        params=c.given["params"],
    )
    plan = source.plan("up", Pipeline.flow_id(), run_id)
    assert set(plan.run_groups) == {Convert.flow_id()}
    assert raw.id not in plan.artifacts
    (rawdb,) = Artifacts(source.local).records(RawDB._kind)
    assert rawdb["id"] in plan.artifacts
    assert any("/" + Convert.flow_id() + "/" in f["target"] for f in plan.files)
    assert len(plan.files) == len({f["target"] for f in plan.files})
    source.transfer(plan)
    receiver.refresh()
    preview = receiver.preview("down", Pipeline.flow_id(), run_id)
    assert any("/" + Convert.flow_id() + "/" in f["target"] for f in preview["file_details"])
    receiver.transfer(receiver.plan("down", Pipeline.flow_id(), run_id))
    browser = Browser(receiver.config, flows=[Pipeline])
    (child,) = browser.children(Pipeline.flow_id(), run_id)
    assert child["local"]
    assert browser.table(Convert.flow_id(), child["run_id"], "checked")["rows"] == [("input.raw:CONTENT",)]
    assert not Artifacts(receiver.local).availability()[rawdb["id"]]["local"]


@pytest.mark.slow
def test_child_revision_invalidates_parent_approval(composition, repo, tmp_path):
    c = composition
    run_id = Pipeline.run(c.cfg, repo=repo, use_processes=False, max_workers=1, **c.given)
    target = Store.of(Config(local_dir=str(tmp_path / "target")))
    workspace = Storage(c.cfg, target)
    plan = workspace.plan("up", Pipeline.flow_id(), run_id)
    (child,) = plan.run_groups[Convert.flow_id()]["runs"]
    changed = child | {"custom": child["custom"] | {"changed": True}}
    catalog = Catalog(workspace.local.space(Convert.flow_id()))
    catalog.commit_run(child["run_id"], row=_meta_row(changed, child["run_id"]))
    with pytest.raises(ValueError, match="source changed"):
        workspace.transfer(plan)
    assert target.list(target.prefix + "/") == []


@pytest.mark.slow
def test_nested_transfer_retains_all_child_tables(composition, repo, tmp_path):
    c = composition
    run_id = Nested.run(c.cfg, repo=repo, use_processes=False, max_workers=1, **c.given)
    target = Store.of(Config(local_dir=str(tmp_path / "target")))
    workspace = Storage(c.cfg, target)
    plan = workspace.plan("up", Nested.flow_id(), run_id)
    assert set(plan.run_groups) == {Pipeline.flow_id(), Convert.flow_id()}
    workspace.transfer(plan)
    browser = Browser(target.cfg, flows=[Nested])
    assert browser.table(Nested.flow_id(), run_id, "result")["rows"] == [("input.raw:CONTENT",)]
    (parent,) = browser.children(Nested.flow_id(), run_id)
    (child,) = browser.children(Pipeline.flow_id(), parent["run_id"])
    assert child["local"] and parent["local"]


@pytest.mark.slow
def test_resumed_artifact_parent_keeps_child_links_through_two_transfers(composition, repo, tmp_path):
    c = composition
    first = ArtifactPipeline.run(c.cfg, repo=repo, use_processes=False, max_workers=1, **c.given)
    second = ArtifactPipeline.run(c.cfg, repo=repo, use_processes=False, resume=first)
    third = ArtifactPipeline.run(c.cfg, repo=repo, use_processes=False, resume=second)
    peer = Store.of(Config(local_dir=str(tmp_path / "peer")))
    target = Store.of(Config(local_dir=str(tmp_path / "target")))
    execute_transfer(Storage(c.cfg, peer).plan("up", ArtifactPipeline.flow_id(), third))
    execute_transfer(Storage(peer.cfg, target).plan("up", ArtifactPipeline.flow_id(), third))
    browser = Browser(target.cfg, flows=[ArtifactPipeline])
    assert {row["run_id"] for row in browser.runs(ArtifactPipeline.flow_id())} == {first, second, third}
    (child,) = browser.children(ArtifactPipeline.flow_id(), third)
    assert child["local"] and child["reused"]
    assert browser.table(Convert.flow_id(), child["run_id"], "checked")["total"] == 1
