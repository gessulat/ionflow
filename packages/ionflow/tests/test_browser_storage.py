from dataclasses import replace
from pathlib import Path

import pytest
from browser_flows import Bundle, Observe

from ionflow import Artifacts, Config, MissingPayload, Storage
from ionflow.lake import Catalog
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
    assert observed.browser.artifacts(Bundle._kind)[0]["local"]
    with Observe.duck(observed.config) as con:
        assert con.sql("SELECT count(*) FROM rows").fetchone() == (0,)
    workspace.transfer(workspace.plan("down", Observe.flow_id(), original, include_artifacts=[]))
    assert observed.browser.table(Observe.flow_id(), original, "rows")["total"] == 3


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
