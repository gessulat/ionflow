from concurrent.futures import ThreadPoolExecutor

import pytest

from ionflow import Artifact, Artifacts, MetadataConflict


class Fasta(Artifact, kind="test-fasta"):
    organism: str | None


class Library(Artifact, kind="test-library", payload="bundle"):
    model: str


def test_deduplication_ignores_names_and_reuses_blobs(anycfg, tmp_path, monkeypatch):
    registry = Artifacts(anycfg)
    a, b = tmp_path / "human.fasta", tmp_path / "renamed.fa"
    a.write_bytes(b">p\nPEPTIDE\n")
    b.write_bytes(a.read_bytes())
    uploads = []
    upload = registry.store.upload

    def track(key, path):
        uploads.append(key)
        upload(key, path)

    monkeypatch.setattr(registry.store, "upload", track)
    first = registry.put(Fasta.from_path(a))
    second = registry.put(Fasta.from_path(b))
    bundle = registry.put(Library.from_path(tmp_path, model="7.2", members={"a.fa": a, "b.fa": b}))
    assert first.id == second.id and first.name == "human.fasta"
    assert len(uploads) == 1
    assert len(registry.records()) == 2
    assert (bundle.local_path() / "b.fa").read_bytes() == a.read_bytes()
    assert first.local_path().read_bytes() == a.read_bytes()


def test_metadata_updates_require_revision_and_preserve_snapshots(cfg, tmp_path):
    p = tmp_path / "human.fa"
    p.write_text(">p\nPEPTIDE\n")
    registry = Artifacts(cfg)
    first = registry.put(Fasta.from_path(p, organism="human"))
    edited = registry.update(first.id, revision=1, name="reference", metadata={"organism": "mouse"})
    assert edited.id == first.id and edited.revision == 2
    assert first.metadata == {"organism": "human"}
    with pytest.raises(MetadataConflict):
        registry.update(first.id, revision=1, name="stale")
    with pytest.raises(MetadataConflict):
        registry.put(Fasta.from_path(p, organism="human"))
    with pytest.raises(TypeError):
        registry.update(first.id, revision=2, metadata={"organism": 7})


def test_bundle_paths_and_directory_links_are_validated(cfg, tmp_path):
    registry = Artifacts(cfg)
    p = tmp_path / "data"
    p.write_text("x")
    with pytest.raises(ValueError, match="unsafe"):
        registry.put(Library.from_path(tmp_path, model="x", members={"../escape": p}))
    with pytest.raises(ValueError, match="overlaps"):
        registry.put(Library.from_path(tmp_path, model="x", members={"a": p, "a/b": p}))
    (tmp_path / "directory-link").symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(ValueError, match="directory symlinks"):
        registry.put(Library.from_path(tmp_path, model="x"))
    assert registry.records() == []


def test_corrupt_download_is_not_exposed_as_cache_entry(cfg, tmp_path):
    p = tmp_path / "data.fa"
    p.write_text("payload")
    registry = Artifacts(cfg)
    ref = registry.put(Fasta.from_path(p))
    digest = ref.snapshot()["content"]["files"][0]["sha256"]
    registry.store.put(f"{registry.root}/_blobs/sha256/{digest}", b"corrupt")
    with pytest.raises(ValueError, match="checksum mismatch"):
        ref.local_path()


def test_concurrent_publication_retains_one_record(cfg, tmp_path):
    p = tmp_path / "data.fa"
    p.write_text("parallel")
    with ThreadPoolExecutor(2) as pool:
        ids = list(pool.map(lambda _: Artifacts(cfg).put(Fasta.from_path(p)).id, range(2)))
    assert ids[0] == ids[1]
    assert len(Artifacts(cfg).records()) == 1


def test_recovery_and_sync_preserve_edits_and_parent_closure(localcfg, s3cfg, tmp_path):
    p = tmp_path / "human.fa"
    p.write_text(">p\nPEPTIDE\n")
    source, target = Artifacts(localcfg), Artifacts(s3cfg)
    parent = source.put(Fasta.from_path(p))
    child = source.put(Library.from_path(tmp_path, model="x", members={"coefficients": p}))
    source.link(parent.id, child.id)
    source.update(child.id, revision=1, name="edited")
    assert source.sync_to(target, [child.id]) == 2
    assert target.get(child.id).name == "edited"
    assert target.links() == source.links()
    source.store.delete(source.lake.register.key)
    assert source.recover() == 2
    assert source.get(child.id).revision == 2
    assert source.get(child.id).name == "edited"
    assert source.links() == target.links()
    target.update(child.id, revision=2, name="conflict")
    source.update(child.id, revision=2, name="concurrent local edit")
    with pytest.raises(MetadataConflict):
        source.sync_to(target)


def test_interrupted_upload_does_not_register_artifact(cfg, tmp_path, monkeypatch):
    p = tmp_path / "human.fa"
    p.write_text("input")
    registry = Artifacts(cfg)

    def fail(key, path):
        raise OSError("interrupted")

    monkeypatch.setattr(registry.store, "upload", fail)
    with pytest.raises(OSError):
        registry.put(Fasta.from_path(p))
    assert registry.records() == []


def test_pinned_source_filenames_share_verified_cache_bytes(cfg, tmp_path):
    path = tmp_path / "first.fa"
    path.write_text("shared bytes")
    registry = Artifacts(cfg)
    first = registry.put(Fasta.from_path(path))
    alias = registry.get(first.id, Fasta, snapshot=first.snapshot() | {"filename": "other.fasta"})
    a, b = first.local_path(), alias.local_path()
    assert a.name == "first.fa" and b.name == "other.fasta"
    assert a.samefile(b)
    assert registry.evict(first.id)
    assert not a.exists() and not b.exists()
    assert registry.get(first.id).local_path().read_text() == "shared bytes"


def test_missing_registry_is_not_mistaken_for_empty_inventory(cfg, tmp_path):
    path = tmp_path / "input"
    path.write_text("payload")
    registry = Artifacts(cfg)
    ref = registry.put(Fasta.from_path(path))
    registry.store.delete(registry.lake.register.key)
    with pytest.raises(RuntimeError, match="artifacts recover"):
        registry.records()
    registry.recover()
    assert registry.get(ref.id).id == ref.id


def test_artifact_kind_has_one_payload_and_metadata_schema(cfg, tmp_path):
    class First(Artifact, kind="schema-contract"):
        value: str

    class Incompatible(Artifact, kind="schema-contract"):
        value: int

    path = tmp_path / "input"
    path.write_text("first")
    registry = Artifacts(cfg)
    registry.put(First.from_path(path, value="one"))
    path.write_text("different content")
    with pytest.raises(TypeError, match="different schema"):
        registry.put(Incompatible.from_path(path, value=1))


def test_sync_rejects_conflicting_kind_schema(localcfg, s3cfg, tmp_path):
    class Source(Artifact, kind="sync-schema-contract"):
        value: str

    class Target(Artifact, kind="sync-schema-contract"):
        value: int

    path = tmp_path / "input"
    path.write_text("source")
    source, target = Artifacts(localcfg), Artifacts(s3cfg)
    source.put(Source.from_path(path, value="one"))
    path.write_text("target")
    target.put(Target.from_path(path, value=1))
    with pytest.raises(TypeError, match="different schema"):
        source.sync_to(target)
    assert len(target.records()) == 1
