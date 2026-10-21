"""A metadata-only S3 peer shared by two independent local workspaces."""

import pytest
from artifact_flows import BuildLibrary, Fasta

from ionflow import Artifacts, MissingPayload, Storage
from ionflow.lake import Catalog
from ionflow.space import Space
from ionflow.store import Store


def test_two_workstations_share_metadata_then_explicit_payloads(workspaces, repo, tmp_path, monkeypatch):
    a, b = workspaces
    fasta = tmp_path / "input.fa"
    fasta.write_text(">p\nPEPTIDE\n")
    ref = Artifacts(a.config).put(Fasta.from_path(fasta))
    rid = BuildLibrary.run(
        a.config, repo=repo, use_processes=False, artifacts={"fasta": ref.id}, params={"work_dir": str(tmp_path)}
    )

    def forbidden(*args, **kwargs):
        raise AssertionError("metadata refresh must not read payloads")

    with monkeypatch.context() as patch:
        patch.setattr(Store, "download", forbidden)
        patch.setattr(Space, "relation", forbidden)
        assert a.refresh()["error"] is None
        assert b.refresh()["error"] is None
    assert a.runs(BuildLibrary.flow_id())[0]["local"]
    assert not a.runs(BuildLibrary.flow_id())[0]["s3"]
    assert not b.runs(BuildLibrary.flow_id())[0]["local"]
    assert not b.runs(BuildLibrary.flow_id())[0]["s3"]
    assert len(b.artifacts()) == 2
    assert all(not r["local"] and not r["s3"] for r in b.artifacts())
    with pytest.raises(MissingPayload):
        Artifacts(b.config).get(ref.id).local_path()
    with pytest.raises(MissingPayload):
        BuildLibrary.run(b.config, repo=repo, resume=rid, use_processes=False)
    with BuildLibrary.duck(b.config) as con:
        assert con.sql("SELECT local, s3 FROM ledger").fetchone() == (False, False)
        assert con.sql("SELECT count(*) FROM digest").fetchone() == (0,)
    upload = a.plan("up", BuildLibrary.flow_id(), rid)
    assert upload.summary()["bytes"] > 0
    a.transfer(upload)
    assert b.refresh()["error"] is None
    assert b.runs(BuildLibrary.flow_id())[0]["s3"]
    b.transfer(b.plan("down", BuildLibrary.flow_id(), rid))
    assert b.runs(BuildLibrary.flow_id())[0]["local"]
    assert all(r["local"] and r["s3"] for r in b.artifacts())
    assert not b.plan("down", BuildLibrary.flow_id(), rid).files
    assert Artifacts(b.config).get(ref.id).local_path().read_bytes() == fasta.read_bytes()
    with BuildLibrary.duck(b.config) as con:
        assert con.sql("SELECT n FROM digest").fetchone() == (len(fasta.read_bytes()),)
    resumed = BuildLibrary.run(b.config, repo=repo, resume=rid, use_processes=False)
    assert resumed != rid


def test_artifact_edits_merge_and_concurrent_edits_conflict(workspaces, tmp_path):
    a, b = workspaces
    path = tmp_path / "input.fa"
    path.write_text("input")
    ref = Artifacts(a.config).put(Fasta.from_path(path))
    a.refresh()
    b.refresh()
    Artifacts(b.config).update(ref.id, revision=1, name="remote-only rename")
    assert not b.refresh()["conflicts"]
    assert not a.refresh()["conflicts"]
    assert Artifacts(a.config).get(ref.id).name == "remote-only rename"
    assert not Artifacts(b.config).availability()[ref.id]["local"]
    Artifacts(a.config).update(ref.id, revision=2, name="one")
    Artifacts(b.config).update(ref.id, revision=2, name="two")
    assert not a.refresh()["conflicts"]
    assert b.refresh()["conflicts"] == [ref.id]
    assert Artifacts(b.config).get(ref.id).name == "two"
    resolved = b.resolve_artifact(ref.id, prefer="local", local_revision=3, remote_revision=3)
    assert resolved["name"] == "two" and resolved["revision"] == 4
    assert not a.refresh()["conflicts"]
    assert Artifacts(a.config).get(ref.id).name == "two"


def test_run_download_can_exclude_unavailable_artifact_payloads(workspaces, repo, tmp_path):
    a, b = workspaces
    path = tmp_path / "input.fa"
    path.write_text(">p\nPEPTIDE\n")
    fasta = Artifacts(a.config).put(Fasta.from_path(path))
    rid = BuildLibrary.run(
        a.config, repo=repo, use_processes=False, artifacts={"fasta": fasta.id}, params={"work_dir": str(tmp_path)}
    )
    library = next(r["id"] for r in a.artifacts() if r["kind"] == "flow-library")
    a.transfer(a.plan("up", BuildLibrary.flow_id(), rid, include_artifacts=[library]))
    b.refresh()
    with pytest.raises(MissingPayload):
        b.plan("down", BuildLibrary.flow_id(), rid)
    results = b.plan("down", BuildLibrary.flow_id(), rid, include_artifacts=[])
    assert results.artifacts == []
    b.transfer(results)
    assert all(not r["local"] for r in b.artifacts())
    assert {r["id"] for r in b.artifacts()} == {fasta.id, library}
    assert b.runs(BuildLibrary.flow_id())[0]["local"]
    with BuildLibrary.duck(b.config) as con:
        assert con.sql("SELECT n FROM digest").fetchone() == (len(path.read_bytes()),)
    b.transfer(b.plan("down", artifact_ids=[library], include_artifacts=[library]))
    assert Artifacts(b.config).availability()[library]["local"]
    assert not Artifacts(b.config).availability()[fasta.id]["local"]
    with pytest.raises(ValueError, match="not a referenced artifact"):
        b.plan("down", artifact_ids=[library], include_artifacts=["unknown"])


def test_offline_refresh_retains_inventory_and_presence(workspaces, tmp_path, monkeypatch):
    a, _ = workspaces
    path = tmp_path / "input.fa"
    path.write_text("input")
    ref = Artifacts(a.config).put(Fasta.from_path(path))
    a.transfer(a.plan("up", artifact_ids=[ref.id]))
    before = a.artifacts()
    checked = a.status()["checked"]

    def offline(*args, **kwargs):
        raise OSError("offline")

    monkeypatch.setattr(a.remote.client, "get_object", offline)
    assert a.refresh()["error"] == "offline"
    assert a.status()["checked"] == checked
    assert a.artifacts() == before


def test_interrupted_transfer_does_not_claim_presence(workspaces, repo, tmp_path, monkeypatch):
    a, b = workspaces
    path = tmp_path / "input.fa"
    path.write_text("input")
    ref = Artifacts(a.config).put(Fasta.from_path(path))
    a.refresh()
    b.refresh()
    plan = a.plan("up", artifact_ids=[ref.id])

    def fail(*args, **kwargs):
        raise OSError("interrupted")

    with monkeypatch.context() as patch:
        patch.setattr(plan.target, "upload", fail)
        with pytest.raises(OSError, match="interrupted"):
            a.transfer(plan)
    assert not a.artifacts()[0]["s3"]
    a.transfer(a.plan("up", artifact_ids=[ref.id]))
    assert a.artifacts()[0]["s3"]


def test_run_progress_invalidates_remote_presence(workspaces, repo, tmp_path):
    a, _ = workspaces
    from examples.embed import EmbedDocs
    from ionflow.space import utc_now
    from ionflow.state import RunRecord

    cat = Catalog(a.local.space(EmbedDocs.flow_id()))
    row = RunRecord("live", "running", utc_now(), None, "commit", None, {}, {})
    cat.commit_run("live", row=row.row(), genesis=EmbedDocs)
    a.transfer(a.plan("up", EmbedDocs.flow_id(), "live"))
    assert a.runs(EmbedDocs.flow_id())[0]["s3"]
    row.status = "failed"
    row.finished = utc_now()
    cat.commit_run("live", row=row.row())
    a.refresh()
    assert not a.runs(EmbedDocs.flow_id())[0]["s3"]


def test_workspace_rejects_same_remote_and_invalid_direction(localcfg):
    with pytest.raises(ValueError, match="different"):
        Storage(localcfg, remote=Store.of(localcfg))


def test_remote_only_configuration_cannot_execute(s3cfg, repo):
    from examples.embed import EmbedDocs

    with pytest.raises(ValueError, match="local store"):
        EmbedDocs.run(s3cfg, repo=repo, use_processes=False)


def test_local_aliases_use_one_canonical_catalog_path(localcfg, tmp_path):
    from dataclasses import replace
    from pathlib import Path

    registry = Artifacts(localcfg)
    path = tmp_path / "input.fa"
    path.write_text("input")
    ref = registry.put(Fasta.from_path(path))
    alias = tmp_path / "alias"
    alias.symlink_to(Path(localcfg.local_dir), target_is_directory=True)
    other = Artifacts(replace(localcfg, local_dir=str(alias)))
    assert other.store.identity == registry.store.identity
    assert other.records()[0]["id"] == ref.id


def test_recovery_preserves_metadata_only_records_and_revisions(workspaces, repo, tmp_path):
    from ionflow.lake import backfill

    a, b = workspaces
    path = tmp_path / "input.fa"
    path.write_text("input")
    ref = Artifacts(a.config).put(Fasta.from_path(path))
    rid = BuildLibrary.run(
        a.config, repo=repo, use_processes=False, artifacts={"fasta": ref.id}, params={"work_dir": str(tmp_path)}
    )
    a.refresh()
    b.refresh()
    replica = Catalog(b.local.space(BuildLibrary.flow_id()))
    version = replica.inventory()[0]["version"]
    replica.discard()
    assert backfill(BuildLibrary, replica.space) == 1
    assert replica.inventory()[0]["version"] == version
    assert not replica.availability()[rid]["local"]
    registry = Artifacts(b.config)
    registry.store.delete(registry.lake.register.key)
    assert registry.recover() == 2
    assert all(not row["local"] for row in registry.availability().values())
    original = Catalog(a.local.space(BuildLibrary.flow_id()))
    original.discard()
    assert backfill(BuildLibrary, original.space) == 1
    assert original.inventory()[0]["version"] == version
    assert a.refresh()["error"] is None


def test_unchanged_refresh_does_not_rewrite_catalogs(workspaces, tmp_path, monkeypatch):
    a, _ = workspaces
    path = tmp_path / "input.fa"
    path.write_text("input")
    Artifacts(a.config).put(Fasta.from_path(path))
    a.refresh()
    a.refresh()

    def forbidden(*args, **kwargs):
        raise AssertionError("unchanged catalogs must not be rewritten")

    monkeypatch.setattr(Artifacts, "merge_metadata", forbidden)
    assert a.refresh()["error"] is None


def test_cli_payload_transfer_requires_directional_approval(workspaces, tmp_path, monkeypatch):
    from typer.testing import CliRunner

    from ionflow import cli

    a, _ = workspaces
    monkeypatch.setattr(cli, "_config_or_setup", lambda: a.config)
    path = tmp_path / "input.fa"
    path.write_text("input")
    ref = Artifacts(a.config).put(Fasta.from_path(path))
    command = ["sync", "--artifact", ref.id]
    runner = CliRunner()
    result = runner.invoke(cli.app, command + ["--plan"])
    assert result.exit_code == 0 and '"bytes": 5' in result.output
    result = runner.invoke(cli.app, command, input="n\n")
    assert result.exit_code != 0
    assert not Artifacts(a.remote).records()
    result = runner.invoke(cli.app, command + ["--yes"])
    assert result.exit_code == 0, result.exception
    assert Artifacts(a.remote).availability()[ref.id]["local"]
