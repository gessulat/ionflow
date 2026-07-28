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
def localcfg(tmp_path):
    """Local-mode config: the whole store is a directory, no S3 involved."""
    from ionflow.config import Config

    return Config(local_dir=str(tmp_path / "store"), prefix="mf")


@pytest.fixture(params=["s3", "local"])
def anycfg(request):
    """Both storage modes: the mode is one dispatch (Store.of), so most tests want both."""
    return request.getfixturevalue("s3cfg" if request.param == "s3" else "localcfg")
