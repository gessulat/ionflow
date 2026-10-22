"""A priori lookup and one logical identity across different concrete builds."""

from dataclasses import replace
from pathlib import Path

import pytest
from identity_flows import BranchBuild, Build, DictionaryBuild, Index, Mismatch, Parent, Source
from pydantic import ValidationError

from ionflow import Artifact, ArtifactInputs, Artifacts, Config, Flow, FlowParams
from ionflow.lake import Catalog
from ionflow.space import Space
from ionflow.storage import execute_transfer, plan_transfer
from ionflow.store import Store


def test_identity_is_declared_typed_and_immutable(identity_build):
    b = identity_build
    expected = Index.identity(library=b.source, method="HCD", nces=[30], losses=[])
    assert expected == Index.identity(losses=[], nces=[30], method="HCD", library=b.source.id)
    with pytest.raises(ValidationError):
        Index.identity(library=b.source, method="HCD", nces=["30"], losses=[])
    with pytest.raises(ValueError, match="exactly"):
        Index.identity(library=b.source, method="HCD")
    with pytest.raises(ValueError, match="content identity"):
        Source.identity()
    run_id = Build.run(b.cfg, repo=b.repo, use_processes=False, **b.given)
    ref = b.result(Build, run_id)
    assert ref.id == expected
    with pytest.raises(ValueError, match="immutable"):
        b.registry.update(ref.id, revision=ref.revision, metadata=ref.metadata | {"method": "CID"})


def test_identity_sources_fail_at_declaration():
    with pytest.raises(TypeError, match="identity field 'library'"):

        class Missing(Flow):
            def index(self) -> Index: ...

    with pytest.raises(TypeError, match="different source type"):

        class Wrong(Flow):
            class Inputs(ArtifactInputs):
                library: Source

            class Params(FlowParams):
                method: int
                nces: list[int]
                losses: list[str]

            def index(self, library: Source) -> Index: ...

    with pytest.raises(TypeError, match="distinct"):

        class Invalid(Artifact, identity=("missing",)):
            value: int


@pytest.mark.parametrize("flow", [Build, Parent])
def test_reuse_and_forced_resume_execute_every_child(identity_build, monkeypatch, flow):
    b = identity_build
    first = flow.run(b.cfg, repo=b.repo, use_processes=False, **b.given)
    first_ref = b.result(flow, first)
    old_bytes = (first_ref.local_path() / "index.arrow").read_bytes()
    second = flow.run(b.cfg, repo=b.repo, use_processes=False, **b.given)
    assert b.result(flow, second).id == first_ref.id
    assert len(b.calls()) == 1
    monkeypatch.setenv("IDENTITY_BUILD", "metal")
    third = flow.run(b.cfg, repo=b.repo, use_processes=False, resume=second, reuse="never")
    new_ref = b.result(flow, third)
    assert len(b.calls()) == 2
    assert new_ref.id == first_ref.id
    assert (new_ref.local_path() / "index.arrow").read_bytes() != old_bytes
    assert (first_ref.local_path() / "index.arrow").read_bytes() == old_bytes
    row = Catalog(Space.for_flow(Store.of(b.cfg), flow)).run_state(third)[0]
    assert row["reuse"] == "never"


@pytest.mark.parametrize("policy,expected_calls", [("auto", 0), ("local", 1), ("never", 1)])
def test_remote_lookup_obeys_policy_without_downloading_inputs(identity_build, s3cfg, policy, expected_calls):
    b = identity_build
    built = Build.run(b.cfg, repo=b.repo, use_processes=False, **b.given)
    ref = b.result(Build, built)
    remote = Store.of(s3cfg)
    execute_transfer(plan_transfer(b.registry.store, remote, artifact_ids=[ref.id], include_artifacts=[ref.id]))
    client_cfg = replace(
        b.cfg,
        local_dir=str(b.root / "client"),
        remote=f"s3://{s3cfg.bucket}/{s3cfg.prefix}",
        endpoint_url=s3cfg.endpoint_url,
        access_key=s3cfg.access_key,
        secret_key=s3cfg.secret_key,
        region=s3cfg.region,
    )
    client = Artifacts(client_cfg)
    client.merge_metadata([b.source.snapshot()])
    params = b.given["params"] | {"work_dir": str(b.root / "client-work")}
    if policy != "auto":
        client.put(Source.from_path(b.source.local_path()))
    run_id = Parent.run(
        client_cfg, repo=b.repo, use_processes=False, artifacts={"library": b.source.id}, params=params, reuse=policy
    )
    result = b.result(Parent, run_id, config=client_cfg)
    assert result.id == ref.id
    log = Path(params["work_dir"]) / "executed"
    assert (len(log.read_text().splitlines()) if log.exists() else 0) == expected_calls
    if policy == "auto":
        assert not client.has_payload(client.get(b.source.id))
        assert (result.local_path() / "index.arrow").read_bytes() == (ref.local_path() / "index.arrow").read_bytes()
        children = Catalog(Space.for_flow(client.store, Build)).runs()
        record, _ = Catalog(Space.for_flow(client.store, Build)).run_state(children[0]["run_id"])
        assert next(iter(record["activities"].values()))["reuse"]["source"] == "s3"


@pytest.mark.slow
def test_process_branches_use_the_same_identity_policy(identity_build):
    b = identity_build
    given = {"artifacts": {"sources": [b.source.id]}, "params": b.given["params"]}
    BranchBuild.run(b.cfg, repo=b.repo, use_processes=True, max_workers=1, **given)
    BranchBuild.run(b.cfg, repo=b.repo, use_processes=True, max_workers=1, **given)
    assert len(b.calls()) == 1


def test_pydantic_params_validate_nested_values_and_normalize(identity_build):
    b = identity_build
    assert Build.resolve_params(b.given["params"] | {"nces": [31, 29]})["nces"] == [29, 31]
    with pytest.raises(ValidationError):
        Build.resolve_params(b.given["params"] | {"nces": [True]})

    class Nested(FlowParams):
        digest: dict[str, list[int]]

    assert Nested.resolve({"digest": {"charges": [2, 3]}}) == {"digest": {"charges": [2, 3]}}
    with pytest.raises(ValidationError):
        Nested.resolve({"digest": {"charges": ["2"]}})


def test_run_transfers_pin_both_builds_and_keep_shared_copy(identity_build, s3cfg, monkeypatch):
    b = identity_build
    first = Build.run(b.cfg, repo=b.repo, use_processes=False, **b.given)
    first_ref = b.result(Build, first)
    remote = Store.of(s3cfg)
    execute_transfer(plan_transfer(b.registry.store, remote, Build.flow_id(), first, include_artifacts=[first_ref.id]))
    monkeypatch.setenv("IDENTITY_BUILD", "metal")
    second = Build.run(b.cfg, repo=b.repo, use_processes=False, reuse="never", **b.given)
    second_ref = b.result(Build, second)
    assert second_ref.id == first_ref.id
    execute_transfer(
        plan_transfer(b.registry.store, remote, Build.flow_id(), second, include_artifacts=[second_ref.id])
    )
    assert Artifacts(remote).get(first_ref.id).snapshot()["content"] == first_ref.snapshot()["content"]
    client_cfg = Config(local_dir=str(b.root / "restore"), prefix=b.cfg.prefix)
    client = Store.of(client_cfg)
    for run_id, original in [(first, first_ref), (second, second_ref)]:
        execute_transfer(plan_transfer(remote, client, Build.flow_id(), run_id, include_artifacts=[original.id]))
        restored = b.result(Build, run_id, config=client_cfg)
        assert (restored.local_path() / "index.arrow").read_bytes() == (
            original.local_path() / "index.arrow"
        ).read_bytes()
    b.registry.store.delete(b.registry.lake.register.key)
    assert b.registry.recover() == 2
    assert len(b.registry.payload_records(first_ref.id)) == 2
    assert (first_ref.local_path() / "index.arrow").read_bytes() != (
        second_ref.local_path() / "index.arrow"
    ).read_bytes()


def test_output_cannot_publish_under_an_unexpected_identity(identity_build):
    b = identity_build

    with pytest.raises(ValueError, match="identity differs"):
        Mismatch.run(b.cfg, repo=b.repo, use_processes=False, **b.given)
    assert b.registry.records(Index._kind) == []


@pytest.mark.parametrize("annotated", [True, False])
def test_typed_metadata_constraints_are_pydantic_validated(tmp_path, annotated):
    from typing import Annotated

    from pydantic import Field

    if annotated:

        class Bounded(Artifact, identity=("count",)):
            count: Annotated[int, Field(gt=0)]
    else:

        class Bounded(Artifact, identity=("count",)):
            count: int = Field(gt=0)

    with pytest.raises(TypeError, match="greater than"):
        Bounded.from_path(tmp_path, count=0)
    with pytest.raises(ValidationError, match="greater than"):
        Bounded.identity(count=0)


def test_recipe_payload_eviction_retains_other_artifacts_and_manifests(identity_build, monkeypatch):
    from ionflow.eviction import remove_local

    b = identity_build
    first = Build.run(b.cfg, repo=b.repo, use_processes=False, **b.given)
    first_ref = b.result(Build, first)
    shared = b.registry.put(Source.from_path(first_ref.local_path() / "index.arrow"))
    monkeypatch.setenv("IDENTITY_BUILD", "metal")
    second = Build.run(b.cfg, repo=b.repo, use_processes=False, reuse="never", **b.given)
    second_ref = b.result(Build, second)
    remove_local(b.registry.store, artifact_id=first_ref.id)
    assert b.registry.has_payload(shared)
    assert not b.registry.has_payload(second_ref)
    assert not b.registry.availability()[first_ref.id]["local"]
    assert len(b.registry.payload_records(first_ref.id)) == 2


def test_cli_validates_json_params_and_exposes_force_execution(identity_build, monkeypatch):
    from typer.testing import CliRunner

    from ionflow import cli

    b = identity_build
    monkeypatch.chdir(b.repo)
    monkeypatch.setattr(cli, "_config_or_setup", lambda: b.cfg)
    runner = CliRunner()
    args = [
        "run",
        "identity_flows:DictionaryBuild",
        "--threads",
        "-a",
        f"library={b.source.id}",
        "-p",
        f"work_dir={b.given['params']['work_dir']}",
        "-p",
        'digest={"charges":[2,3]}',
        "-p",
        "nces=[31,29]",
        "-p",
        'assignments=[{"file":"sample","nces":[29,31]}]',
    ]
    for flags in [["--reuse", "local"], ["--no-reuse"]]:
        result = runner.invoke(cli.app, args + flags)
        assert result.exit_code == 0, result.output
    assert len(b.calls()) == 2
    con = DictionaryBuild.duck(b.cfg)
    assert con.sql("SELECT json_extract(digest, '$.charges'), nces, reuse FROM ledger ORDER BY run_id").fetchall() == [
        ("[2,3]", [29, 31], "local"),
        ("[2,3]", [29, 31], "never"),
    ]
    assert con.sql("SELECT DISTINCT json_extract(assignments, '$[0].nces') FROM ledger").fetchall() == [("[29,31]",)]
    con.close()
    assert runner.invoke(cli.app, args + ["--reuse", "invalid"]).exit_code != 0


def test_concurrent_equivalent_publications_keep_exact_local_builds(anycfg, tmp_path):
    from concurrent.futures import ThreadPoolExecutor

    registry = Artifacts(anycfg)
    path = tmp_path / "source"
    path.write_bytes(b"input")
    source = registry.put(Source.from_path(path))
    candidates = []
    for suffix in ["cuda", "metal"]:
        path = tmp_path / suffix
        path.write_bytes(suffix.encode())
        candidates.append(
            Index.from_path(
                tmp_path,
                members={"index.arrow": path},
                library=source,
                method="HCD",
                nces=[30],
                losses=[],
                platform=suffix,
            )
        )
    with ThreadPoolExecutor(max_workers=2) as pool:
        refs = list(pool.map(registry.put, candidates))
    assert refs[0].id == refs[1].id
    payloads = {(ref.local_path() / "index.arrow").read_bytes() for ref in refs}
    assert payloads == {b"cuda", b"metal"} if anycfg.local_dir else len(payloads) == 1
    for ref in refs:
        assert (ref.local_path() / "index.arrow").read_text() == ref.metadata["platform"]


def test_corrupt_reuse_download_is_not_registered_and_can_be_retried(identity_build, s3cfg, monkeypatch):
    from ionflow.store import S3Store

    b = identity_build
    path = b.root / "remote-index"
    path.write_bytes(b"precomputed library")
    remote = Artifacts(s3cfg)
    ref = remote.put(
        Index.from_path(b.root, members={"index.arrow": path}, library=b.source, method="HCD", nces=[30], losses=[])
    )
    cfg = replace(
        b.cfg,
        remote=f"s3://{s3cfg.bucket}/{s3cfg.prefix}",
        endpoint_url=s3cfg.endpoint_url,
        access_key=s3cfg.access_key,
        secret_key=s3cfg.secret_key,
        region=s3cfg.region,
    )
    download = S3Store.download
    blob = f"{remote.root}/_blobs/sha256/{ref.snapshot()['content']['files'][0]['sha256']}"

    def corrupt(store, key, filename):
        download(store, key, filename)
        if key == blob:
            Path(filename).write_bytes(b"x" * path.stat().st_size)

    with monkeypatch.context() as patch:
        patch.setattr(S3Store, "download", corrupt)
        with pytest.raises(ValueError, match="checksum mismatch"):
            Build.run(cfg, repo=b.repo, use_processes=False, **b.given)
    assert not b.registry.availability(artifact_id=ref.id).get(ref.id, {}).get("local")
    assert b.calls() == []
    run_id = Build.run(cfg, repo=b.repo, use_processes=False, **b.given)
    assert (b.result(Build, run_id).local_path() / "index.arrow").read_bytes() == path.read_bytes()
    assert b.calls() == []
