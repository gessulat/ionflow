import subprocess

import pytest
from schrein.testing import background_server

# Moto with atomic conditional PUTs (schrein decision 0003), as a subprocess —
# never in-process: DuckDB httpfs relation binds starve an in-process werkzeug
# server (see AGENTS.md, Testing). Spawned + warmed in the background at import
# so the boot overlaps with collection.
_endpoint = background_server()


def _git(cwd, *args):
    subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True)


def _make_repo(path):
    path.mkdir()
    _git(path, "init")
    (path / "flow.py").write_text("# flow\n")
    _git(path, "add", ".")
    _git(path, "-c", "user.email=t@t.com", "-c", "user.name=t", "commit", "-m", "init")
    return str(path)


@pytest.fixture
def clean_repo(tmp_path):
    """Function-scoped repo for tests that dirty it (test_git)."""
    return _make_repo(tmp_path / "repo")


@pytest.fixture(scope="session")
def repo(tmp_path_factory):
    """Shared clean repo: flow runs only read it (git status + rev-parse)."""
    return _make_repo(tmp_path_factory.mktemp("session") / "repo")


@pytest.fixture(scope="session")
def moto_endpoint():
    return _endpoint()


@pytest.fixture
def s3cfg(moto_endpoint, request):
    from ionflow.config import Config
    from ionflow.store import s3_client

    # unique bucket per test: no teardown needed (server dies with the process)
    cfg = Config(
        bucket=f"b-{abs(hash(request.node.nodeid)) % 10**8}",
        prefix="mf",
        endpoint_url=moto_endpoint,
        region="us-east-1",
        access_key="test",
        secret_key="test",
    )
    s3_client(cfg).create_bucket(Bucket=cfg.bucket)
    return cfg


@pytest.fixture
def cfg(localcfg):
    """The default for a mode-agnostic test: local mode, no moto round trips.

    Storage mode is one dispatch (`Store.of`), so a test that never reaches
    `S3Store`'s uri/connect/lake/client is not testing the mode — it is just
    paying for it. Tests that DO exercise those take `s3cfg` by name, and
    `anycfg` covers both where the difference is the point.
    """
    return localcfg


@pytest.fixture
def shared_blob_publication(localcfg, tmp_path):
    """A receiving store and incoming bundle sharing the receiver's only file."""
    from browser_flows import Bundle, Document

    from ionflow import Artifacts, Config

    target = Artifacts(localcfg)
    path = tmp_path / "shared"
    path.write_text("shared content")
    original = target.put(Document.from_path(path))
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "shared").write_text("shared content")
    (bundle / "unique").write_text("new content")
    source = Artifacts(Config(local_dir=str(tmp_path / "source"), prefix="mf"))
    incoming = source.put(Bundle.from_path(bundle, document_id=original.id))
    return target, original, source, incoming, bundle


@pytest.fixture
def localcfg(tmp_path):
    """Local-mode config: the whole store is a directory, no S3 involved."""
    from ionflow.config import Config

    return Config(local_dir=str(tmp_path / "store"), prefix="mf")


@pytest.fixture
def observed(localcfg, repo, tmp_path):
    """One real run with nested phases, a table, and a shared bundle."""
    from types import SimpleNamespace

    from browser_flows import Bundle, Document, Observe

    from ionflow import Artifacts, Browser, Storage

    path = tmp_path / "source.txt"
    path.write_text("one's %_content")
    registry = Artifacts(localcfg)
    document = registry.put(Document.from_path(path))
    run_id = Observe.run(localcfg, repo=repo, use_processes=False, artifacts={"document": document.id})
    return SimpleNamespace(
        config=localcfg,
        registry=registry,
        document=document,
        run_id=run_id,
        browser=Browser(localcfg, flows=[Observe], artifacts=[Document, Bundle]),
        storage=Storage(localcfg),
    )


@pytest.fixture(params=["s3", "local"])
def anycfg(request):
    """Both storage modes: the mode is one dispatch (Store.of), so most tests want both."""
    return request.getfixturevalue("s3cfg" if request.param == "s3" else "localcfg")


@pytest.fixture
def workspaces(s3cfg, tmp_path, monkeypatch):
    """Two local execution stores sharing a real HTTP S3 test endpoint."""
    from dataclasses import replace

    from ionflow import Storage

    monkeypatch.setattr("ionflow.storage.request_refresh", lambda config: None)
    return tuple(
        Storage(
            replace(s3cfg, bucket=None, local_dir=str(tmp_path / name), remote=f"s3://{s3cfg.bucket}/{s3cfg.prefix}")
        )
        for name in ("a", "b")
    )


@pytest.fixture
def archive(workspaces, repo, tmp_path):
    """A run with large inputs and opposite archive/client payload defaults."""
    from types import SimpleNamespace

    from browser_flows import Archive, Raw, RawDB

    from ionflow import Artifacts

    a, b = workspaces
    registry = Artifacts(a.local)
    path = tmp_path / "input.raw"
    path.write_text("original raw data")
    raw = registry.put(Raw.from_path(path))
    path = tmp_path / "input.raw.db"
    path.write_text("preferred rawdb data")
    rawdb = registry.put(RawDB.from_path(path))
    registry.link(raw.id, rawdb.id)
    rid = Archive.run(a.config, repo=repo, use_processes=False, artifacts={"raw": raw.id, "rawdb": rawdb.id})
    return SimpleNamespace(source=a, client=b, registry=registry, raw=raw, rawdb=rawdb, run_id=rid)


@pytest.fixture
def executioncfg(s3cfg, localcfg):
    """Local execution with explicit S3 credentials for user-code side effects."""
    from dataclasses import replace

    return replace(s3cfg, local_dir=localcfg.local_dir)


@pytest.fixture
def remote_run(localcfg, s3cfg, repo):
    """Execute locally, then explicitly transfer the snapshot for S3 catalog tests."""
    from ionflow.space import Space
    from ionflow.store import Store

    def run(flow, **kwargs):
        kwargs.setdefault("repo", repo)
        try:
            return flow.run(localcfg, **kwargs)
        finally:
            Space.for_flow(Store.of(localcfg), flow).sync_to(Space.for_flow(Store.of(s3cfg), flow), flow)

    return run
