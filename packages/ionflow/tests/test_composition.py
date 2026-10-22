import pytest
from subflow_flows import Batch, Convert, Nested, Pipeline, Raw, RawDB, Result, TablePipeline

from ionflow import Artifacts, Browser, Call, Flow, subflow
from ionflow.lake import Catalog, backfill
from ionflow.provenance import ArtifactRuntime
from ionflow.space import Space
from ionflow.store import Store


@pytest.mark.slow
@pytest.mark.parametrize("processes", [False, True])
def test_composed_artifact_and_table_outputs(composition, repo, processes):
    c = composition
    run_id = Pipeline.run(c.cfg, repo=repo, use_processes=processes, max_workers=1, **c.given)
    browser = Browser(c.cfg, flows=[Pipeline], artifacts=[Raw, RawDB])
    (child,) = browser.children(Pipeline.flow_id(), run_id)
    assert child["parent"] == {"flow_id": Pipeline.flow_id(), "run_id": run_id, "step": "converted", "key": 0}
    assert child["flow_id"] == Convert.flow_id() and child["status"] == "complete"
    assert not child["reused"]
    with Pipeline.duck(c.cfg) as con:
        assert con.sql("SELECT text FROM result").fetchone()[0] == "input.raw:CONTENT"
    (ref,) = c.registry.records(RawDB._kind)
    assert c.registry.producers(ref["id"])[0]["flow_id"] == Convert.flow_id()
    assert c.registry.parents(ref["id"]) == [c.raw.id]
    table_id = TablePipeline.run(c.cfg, repo=repo, use_processes=processes, max_workers=1, **c.given)
    with TablePipeline.duck(c.cfg) as con:
        assert (
            con.sql("SELECT text FROM result WHERE run_id = ?", params=[table_id]).fetchone()[0] == "input.raw:CONTENT"
        )


@pytest.mark.slow
def test_failed_child_resumes_its_completed_steps(composition, repo, monkeypatch):
    c = composition
    monkeypatch.setenv("IONFLOW_CHILD_FAIL", "1")
    with pytest.raises(ValueError, match="child failed"):
        Pipeline.run(c.cfg, repo=repo, use_processes=False, max_workers=1, **c.given)
    parent_cat = Catalog(Space.for_flow(Store.of(c.cfg), Pipeline))
    first = parent_cat.runs()[0]["run_id"]
    browser = Browser(c.cfg, flows=[Pipeline])
    (child,) = browser.children(Pipeline.flow_id(), first)
    assert child["status"] == "failed"
    (original,) = c.registry.records(RawDB._kind)
    producer = c.registry.producers(original["id"])
    monkeypatch.delenv("IONFLOW_CHILD_FAIL")
    resumed = Pipeline.run(c.cfg, repo=repo, use_processes=False, max_workers=1, resume=first)
    attempts = browser.children(Pipeline.flow_id(), resumed)
    assert [row["status"] for row in attempts] == ["failed", "complete"]
    assert attempts[-1]["origin_run_id"] == child["run_id"]
    assert c.registry.producers(original["id"]) == producer
    child_cat = Catalog(Space.for_flow(Store.of(c.cfg), Convert))
    old = child_cat.run_state(child["run_id"])[1]
    new = child_cat.run_state(attempts[-1]["run_id"])[1]
    assert next(p for s, _, p in new if s == "rawdb") == next(p for s, _, p in old if s == "rawdb")
    child_cat.discard()
    assert backfill(Convert, child_cat.space) == 2
    assert browser.children(Pipeline.flow_id(), resumed)[-1]["parent"]["run_id"] == resumed


@pytest.mark.slow
def test_child_completion_survives_parent_output_failure(composition, repo, monkeypatch):
    c = composition
    publish = ArtifactRuntime.publish

    def fail_parent(self, candidate, **kwargs):
        if self.flow_id == Pipeline.flow_id():
            raise ValueError("parent output failed")
        return publish(self, candidate, **kwargs)

    monkeypatch.setattr(ArtifactRuntime, "publish", fail_parent)
    with pytest.raises(ValueError, match="parent output failed"):
        Pipeline.run(c.cfg, repo=repo, use_processes=False, max_workers=1, **c.given)
    browser = Browser(c.cfg, flows=[Pipeline])
    first = browser.runs(Pipeline.flow_id())[0]["run_id"]
    (original,) = browser.children(Pipeline.flow_id(), first)
    monkeypatch.setattr(ArtifactRuntime, "publish", publish)
    resumed = Pipeline.run(c.cfg, repo=repo, use_processes=False, max_workers=1, resume=first)
    (child,) = browser.children(Pipeline.flow_id(), resumed)
    assert child["run_id"] == original["run_id"] and child["reused"]


@pytest.mark.slow
def test_branch_calls_have_distinct_child_identity(composition, repo):
    c = composition
    run_id = Batch.run(
        c.cfg,
        repo=repo,
        use_processes=False,
        max_workers=2,
        artifacts={"raws": [c.raw.id, c.other.id]},
        params=c.given["params"],
    )
    browser = Browser(c.cfg, flows=[Batch])
    children = browser.children(Batch.flow_id(), run_id)
    assert {row["parent"]["key"] for row in children} == {c.raw.id, c.other.id}
    assert len({row["run_id"] for row in children}) == 2


def test_declarations_are_validated_and_visible(cfg):
    with pytest.raises(ValueError, match="whole table or artifact"):
        subflow(Convert, output="missing")
    with pytest.raises(TypeError, match="output schema differs"):

        class Wrong(Flow):
            @subflow(Convert, output="rawdb")
            def converted(self) -> Call[Result]:
                return Call()

    with pytest.raises(TypeError, match="dependency 'source'"):

        class WrongInput(Flow):
            def source(self) -> Raw:
                raise NotImplementedError

            @subflow(Convert, output="rawdb")
            def converted(self, source: Result) -> Call[RawDB]:
                return Call()

    description = Browser(cfg, flows=[Pipeline]).describe()
    parent, child = description["flows"]
    assert parent["steps"][0]["subflow"] == {"flow_id": Convert.flow_id(), "output": "rawdb"}
    assert child["id"] == Convert.flow_id()


@pytest.mark.slow
@pytest.mark.parametrize("processes", [False, True])
def test_nested_children_keep_stable_identity_and_pinned_metadata(composition, repo, processes):
    c = composition
    c.registry.update(c.raw.id, revision=c.raw.revision, name="edited after binding")
    given = {"params": c.given["params"], "artifacts": {"raw": c.raw}}
    run_id = Nested.run(c.cfg, repo=repo, use_processes=processes, max_workers=1, **given)
    browser = Browser(c.cfg, flows=[Nested])
    (parent,) = browser.children(Nested.flow_id(), run_id)
    (child,) = browser.children(Pipeline.flow_id(), parent["run_id"])
    assert child["flow_id"] == Convert.flow_id()
    assert child["artifacts"]["raw"]["name"] == "input.raw"
    assert set(browser.flows) == {Nested.flow_id(), Pipeline.flow_id(), Convert.flow_id()}
    assert Flow._registry[Nested.flow_id()] is Nested


@pytest.mark.slow
def test_metadata_refresh_preserves_child_links(composition, workspaces, repo):
    c = composition
    source, target = workspaces
    raw = Artifacts(source.local).put(Raw.from_path(c.raw.local_path()))
    run_id = Pipeline.run(
        source.config,
        repo=repo,
        use_processes=False,
        max_workers=1,
        params=c.given["params"],
        artifacts={"raw": raw.id},
    )
    source.refresh()
    target.refresh()
    browser = Browser(target.config, flows=[Pipeline])
    (child,) = browser.children(Pipeline.flow_id(), run_id)
    assert child["parent"]["run_id"] == run_id
    assert child["flow_id"] == Convert.flow_id() and not child["local"]
