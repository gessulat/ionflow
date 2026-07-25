import atexit
import socket
import subprocess
import sys
import threading
import time

import pytest

# Start + warm the moto server in a background thread at import time, so the
# heavy imports (boto3) and botocore's first-client JSON load overlap with
# collection instead of stalling each worker's first S3 test. The server runs
# as a subprocess, never in-process: DuckDB httpfs relation binds starve an
# in-process werkzeug server (see AGENTS.md, Testing).
_moto = {}


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _warm():
    import boto3

    port = _free_port()
    proc = subprocess.Popen(
        [sys.executable, "-m", "moto.server", "-H", "127.0.0.1", "-p", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    atexit.register(proc.kill)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
            break
        except OSError:
            time.sleep(0.05)
    _moto["endpoint"] = f"http://127.0.0.1:{port}"
    boto3.client(
        "s3",
        endpoint_url=_moto["endpoint"],
        region_name="us-east-1",
        aws_access_key_id="warm",
        aws_secret_access_key="warm",
    ).list_buckets()


_warmup = threading.Thread(target=_warm, daemon=True)
_warmup.start()


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
    _warmup.join()
    return _moto["endpoint"]  # subprocess; killed via atexit


@pytest.fixture
def s3cfg(moto_endpoint, request):
    from microflow import storage
    from microflow.config import Config

    # unique bucket per test: no teardown needed (server dies with the process)
    cfg = Config(
        bucket=f"b-{abs(hash(request.node.nodeid)) % 10**8}",
        prefix="mf",
        endpoint_url=moto_endpoint,
        region="us-east-1",
        access_key="test",
        secret_key="test",
    )
    storage.s3_client(cfg).create_bucket(Bucket=cfg.bucket)
    return cfg
