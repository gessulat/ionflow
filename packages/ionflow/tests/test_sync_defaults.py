import pytest
from browser_flows import Archive, Bundle, Raw, RawDB

from ionflow import Artifact, Artifacts, Browser, MissingPayload, Storage
from ionflow.lake import Catalog
from ionflow.storage import execute_transfer, plan_transfer


def test_defaults_are_inherited_and_described(localcfg):
    class Derived(Raw, kind="sync-derived", sync_up=True):
        pass

    assert Derived._sync == {"up": True, "down": False}
    assert Raw._sync == {"up": False, "down": False}
    assert Artifact._sync == {"up": True, "down": True}
    assert Derived._fields == {}
    kinds = Browser(localcfg, artifacts=[Raw, RawDB]).describe()["artifacts"]
    assert [kind["sync"] for kind in kinds] == [Raw._sync, RawDB._sync]


@pytest.mark.parametrize("value", [0, "false", []])
@pytest.mark.parametrize("option", ["sync_up", "sync_down"])
def test_defaults_require_boolean_declarations(value, option):
    with pytest.raises(TypeError, match="booleans"):
        type("Bad", (Artifact,), {}, kind="sync-bad", **{option: value})


def test_run_defaults_archive_rawdb_and_download_only_results(archive, monkeypatch):
    from ionflow.artifact import _KINDS

    a, b = archive.source, archive.client
    a.remove_local(artifact_id=archive.raw.id)
    # A generic client knows the policy from metadata, without application imports.
    monkeypatch.delitem(_KINDS, Raw._kind)
    monkeypatch.delitem(_KINDS, RawDB._kind)
    with monkeypatch.context() as patch:

        def forbidden(*args, **kwargs):
            raise AssertionError("preview must remain local and metadata-only")

        patch.setattr(Storage, "remote", property(forbidden))
        preview = a.preview("up", Archive.flow_id(), archive.run_id)
    assert {i["id"] for i in preview["inputs"] if i["chosen"]} == {archive.rawdb.id}
    plan = a.plan("up", Archive.flow_id(), archive.run_id)
    assert plan.artifacts == [archive.rawdb.id]
    a.transfer(plan)
    remote = Artifacts(a.remote)
    assert not remote.availability()[archive.raw.id]["local"]
    assert remote.availability()[archive.rawdb.id]["local"]
    assert remote.get(archive.rawdb.id).snapshot()["sync"] == {"up": True, "down": False}
    b.refresh()
    Artifacts(b.local).recover()
    preview = b.preview("down", Archive.flow_id(), archive.run_id)
    assert all(not i["chosen"] for i in preview["inputs"])
    plan = b.plan("down", Archive.flow_id(), archive.run_id)
    assert not plan.artifacts and plan.files
    assert all("/_blobs/" not in f["target"] for f in plan.files)
    b.transfer(plan)
    assert all(not row["local"] for row in b.artifacts())
    assert Catalog(b.local.space(Archive.flow_id())).availability()[archive.run_id]["local"]
    registry = Artifacts(b.local)
    assert registry.parents(archive.rawdb.id) == [archive.raw.id]
    with Archive.duck(b.config) as con:
        assert con.sql("SELECT size FROM sizes").fetchone() == (len("preferred rawdb data"),)
    with pytest.raises(MissingPayload):
        registry.get(archive.rawdb.id).local_path()
    selected = [archive.rawdb.id]
    preview = b.preview("down", Archive.flow_id(), archive.run_id, include_artifacts=selected)
    assert {i["id"] for i in preview["inputs"] if i["chosen"]} == set(selected)
    b.transfer(b.plan("down", Archive.flow_id(), archive.run_id, include_artifacts=selected))
    assert registry.get(archive.rawdb.id).local_path().read_text() == "preferred rawdb data"
    assert not registry.availability()[archive.raw.id]["local"]


def test_explicit_selections_override_both_defaults(archive):
    a = archive.source
    both = [archive.raw.id, archive.rawdb.id]
    for selected in ([], both, [archive.raw.id]):
        preview = a.preview("up", Archive.flow_id(), archive.run_id, include_artifacts=selected)
        assert {i["id"] for i in preview["inputs"] if i["chosen"]} == set(selected)
        plan = a.plan("up", Archive.flow_id(), archive.run_id, include_artifacts=selected)
        assert set(plan.artifacts) == set(selected)
    a.transfer(a.plan("up", Archive.flow_id(), archive.run_id, include_artifacts=both))
    assert all(row["local"] for row in Artifacts(a.remote).availability().values())


def test_direct_sync_uses_endpoint_direction_and_keeps_metadata(archive):
    a, b = archive.source, archive.client
    remote = Artifacts(a.remote)
    assert archive.registry.sync_to(remote, [archive.rawdb.id]) == 2
    target = Artifacts(b.local)
    assert remote.sync_to(target, [archive.rawdb.id]) == 2
    assert target.parents(archive.rawdb.id) == [archive.raw.id]
    assert all(not row["local"] for row in target.availability().values())
    plan = plan_transfer(a.remote, b.local, artifact_ids=[archive.rawdb.id], include_artifacts=[archive.rawdb.id])
    execute_transfer(plan)
    assert target.availability()[archive.rawdb.id]["local"]


@pytest.mark.parametrize("policy", [{"up": 1, "down": False}, {"up": True}, None])
def test_import_rejects_invalid_sync_policy(shared_blob_publication, policy):
    _, original, source, _, _ = shared_blob_publication
    record = original.snapshot() | {"sync": policy}
    with pytest.raises(ValueError, match="sync defaults"):
        source.merge_metadata([record])


def test_metadata_only_artifact_transfer_keeps_producer_evidence(observed, localcfg, tmp_path):
    from dataclasses import replace

    from ionflow.store import Store

    bundle = next(r["id"] for r in observed.registry.records() if r["kind"] == Bundle._kind)
    target = Artifacts(Store.of(replace(localcfg, local_dir=str(tmp_path / "client"))))
    plan = plan_transfer(observed.storage.local, target.store, artifact_ids=[bundle], include_artifacts=[])
    assert not plan.files
    execute_transfer(plan)
    assert target.producers(bundle) == observed.registry.producers(bundle)
    assert not target.availability()[bundle]["local"]
