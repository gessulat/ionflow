import multiprocessing
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from threading import Event

import pytest
from browser_flows import Bundle, Observe

from ionflow import Artifacts, Config, MissingPayload, Storage
from ionflow.lake import Catalog, backfill
from ionflow.storage import execute_transfer, plan_transfer
from ionflow.store import Store


def test_preview_never_constructs_peer_or_reads_payloads(observed, monkeypatch):
    storage = Storage(replace(observed.config, remote="s3://unreachable/shared"))
    bundle = observed.browser.artifacts(Bundle._kind)[0]

    def forbidden(*args, **kwargs):
        raise AssertionError("preview must use local metadata only")

    monkeypatch.setattr(Store, "download", forbidden)
    monkeypatch.setattr(Storage, "remote", property(forbidden))
    preview = storage.preview("up", Observe.flow_id(), observed.run_id)
    assert preview["preview"] and "token" not in preview
    assert preview["source"].endswith("/mf") and preview["target"] == "s3://unreachable/shared"
    names = [name for f in preview["file_details"] for name in f["names"]]
    assert any(name.endswith("/shared") for name in names)
    only_results = storage.preview("up", Observe.flow_id(), observed.run_id, include_artifacts=[])
    assert all("/_blobs/" not in f["target"] for f in only_results["file_details"])
    assert bundle["id"] in {i["id"] for i in only_results["inputs"]}
    assert all(i["bytes"] > 0 for i in only_results["inputs"])
    with pytest.raises(ValueError, match="not a referenced artifact"):
        storage.preview("up", artifact_ids=[bundle["id"]], include_artifacts=["unknown"])


def test_remove_artifact_keeps_shared_bytes_metadata_and_peer(observed, tmp_path, monkeypatch):
    original = observed.browser.artifacts(Bundle._kind)[0]
    ref = observed.registry.get(original["id"])
    cached = ref.local_path()
    root = tmp_path / "other-bundle"
    root.mkdir()
    (root / "shared").write_text("shared content")
    (root / "unique").write_text("other content")
    other = observed.registry.put(Bundle.from_path(root, document_id=observed.document.id))
    remote = Store.of(Config(local_dir=str(tmp_path / "peer"), prefix="mf"))
    workspace = Storage(observed.config, remote)
    details = workspace.plan("up", Observe.flow_id(), observed.run_id, include_artifacts=[]).details()
    assert "token" in details and all(i["bytes"] > 0 and not i["chosen"] for i in details["inputs"])
    assert all("/_blobs/" not in f["target"] for f in details["file_details"])
    workspace.transfer(workspace.plan("up", artifact_ids=[ref.id]))
    remote_records = Artifacts(remote).records()
    snapshot = ref.snapshot()

    def forbidden(*args, **kwargs):
        raise AssertionError("removal must not contact a peer")

    with monkeypatch.context() as patch:
        patch.setattr(Storage, "remote", property(forbidden))
        workspace.remove_local(artifact_id=ref.id)
    assert not cached.exists()
    assert observed.registry.get(ref.id).snapshot() == snapshot
    assert not observed.registry.availability()[ref.id]["local"]
    assert other.local_path().joinpath("shared").read_text() == "shared content"
    assert Artifacts(remote).records() == remote_records
    assert observed.document.local_path().exists()
    with pytest.raises(MissingPayload):
        ref.local_path()
    workspace.transfer(workspace.plan("down", artifact_ids=[ref.id]))
    assert ref.local_path().joinpath("unique").read_text() == "one's %_content"


def test_remove_run_refuses_reused_parts_and_can_restore(observed, repo, tmp_path):
    original = observed.run_id
    resumed = Observe.run(observed.config, repo=repo, resume=original, use_processes=False)
    with pytest.raises(ValueError, match="reuse"):
        observed.storage.remove_local(flow_id=Observe.flow_id(), run_id=original)
    workspace = Storage(observed.config, Store.of(Config(local_dir=str(tmp_path / "peer"), prefix="mf")))
    workspace.transfer(workspace.plan("up", Observe.flow_id(), original, include_artifacts=[]))
    workspace.remove_local(flow_id=Observe.flow_id(), run_id=resumed)
    workspace.remove_local(flow_id=Observe.flow_id(), run_id=original)
    records = Catalog(workspace.local.space(Observe.flow_id())).inventory()
    assert len(records) == 2 and all(not r["present"] for r in records)
    assert backfill(Observe, workspace.local.space(Observe.flow_id()), replace=True) == 2
    assert Catalog(workspace.local.space(Observe.flow_id())).inventory() == records
    assert observed.browser.artifacts(Bundle._kind)[0]["local"]
    with Observe.duck(observed.config) as con:
        assert con.sql("SELECT count(*) FROM rows").fetchone() == (0,)
    workspace.transfer(workspace.plan("down", Observe.flow_id(), original, include_artifacts=[]))
    assert observed.browser.table(Observe.flow_id(), original, "rows")["total"] == 3


def test_evicted_local_run_retains_recovery_metadata(observed):
    space = observed.storage.local.space(Observe.flow_id())
    before = Catalog(space).inventory()[0]
    assert not space.store.list(space.root + "/_inventory/")
    assert before["manifest"] and before["activities"] and before["artifacts"]
    observed.storage.remove_local(flow_id=Observe.flow_id(), run_id=observed.run_id)
    assert not space.store.list(space.run_dir(observed.run_id) + "/")
    assert backfill(Observe, space, replace=True) == 1
    assert Catalog(space).inventory() == [before | {"present": False}]
    with Observe.duck(observed.config) as con:
        assert con.sql("SELECT count(*) FROM rows").fetchone() == (0,)


def _evict_artifact(config, artifact_id, started, finished):
    storage = Storage(config)
    started.set()
    storage.remove_local(artifact_id=artifact_id)
    finished.set()


@pytest.mark.parametrize("operation", ["publish", "sync", "transfer"])
def test_eviction_waits_for_shared_blob_publication(shared_blob_publication, monkeypatch, operation):
    target, original, source, ref, bundle = shared_blob_publication
    if operation == "publish":

        def publish():
            target.put(Bundle.from_path(bundle, document_id=original.id))
    else:
        plan = plan_transfer(source.store, target.store, artifact_ids=[ref.id])

        def publish():
            if operation == "sync":
                source.sync_to(target, [ref.id])
            else:
                execute_transfer(plan)

    publishing, release = Event(), Event()
    mark_present = Artifacts.mark_present

    def pause(self, artifact_id):
        if self.store.identity == target.store.identity and artifact_id != original.id:
            publishing.set()
            assert release.wait(15)
        mark_present(self, artifact_id)

    monkeypatch.setattr(Artifacts, "mark_present", pause)
    ctx = multiprocessing.get_context("spawn")
    started, finished = ctx.Event(), ctx.Event()
    worker = ctx.Process(target=_evict_artifact, args=(target.store.cfg, original.id, started, finished))
    with ThreadPoolExecutor(1) as pool:
        publication = pool.submit(publish)
        try:
            assert publishing.wait(15)
            worker.start()
            assert started.wait(15)
            assert not finished.wait(0.5), "eviction overtook artifact publication"
        finally:
            release.set()
            if worker.pid is not None:
                worker.join(15)
                if worker.is_alive():
                    worker.terminate()
                    worker.join()
        publication.result()
    assert worker.exitcode == 0
    record = next(r for r in target.records() if r["kind"] == Bundle._kind)
    assert target.availability()[record["id"]]["local"]
    assert target.get(record["id"]).local_path().joinpath("shared").read_text() == "shared content"
    assert not target.availability()[original.id]["local"]


def test_transfer_rejects_blobs_evicted_after_planning(shared_blob_publication):
    target, original, source, ref, _ = shared_blob_publication
    plan = plan_transfer(source.store, target.store, artifact_ids=[ref.id])
    Storage(target.store.cfg).remove_local(artifact_id=original.id)
    with pytest.raises(MissingPayload, match="new transfer plan"):
        execute_transfer(plan)
    assert not target.availability()[ref.id]["local"]
    execute_transfer(plan_transfer(source.store, target.store, artifact_ids=[ref.id]))
    assert target.get(ref.id).local_path().joinpath("shared").read_text() == "shared content"


def test_removal_rejects_active_runs_and_path_escapes(observed, tmp_path):
    from ionflow.space import utc_now
    from ionflow.state import RunRecord

    with pytest.raises(ValueError, match="select one"):
        observed.storage.remove_local()
    with pytest.raises(ValueError, match="unsafe"):
        observed.storage.remove_local(flow_id="../../outside", run_id="run")
    record = RunRecord("active", "running", utc_now(), None, "commit", None, {}, {})
    cat = Catalog(observed.storage.local.space(Observe.flow_id()))
    cat.commit_run("active", row=record.row(), genesis=Observe)
    with pytest.raises(ValueError, match="running"):
        observed.storage.remove_local(artifact_id=observed.document.id)
    assert observed.document.local_path().exists()


def test_removal_rejects_symlink_escape_before_changing_presence(observed, tmp_path):
    from ionflow.eviction import _paths

    root = Path(observed.config.local_dir)
    (tmp_path / "outside").write_text("keep")
    (root / "escape").symlink_to(tmp_path / "outside")
    with pytest.raises(ValueError, match="escapes"):
        _paths(observed.storage.local, ["escape"])
