import subprocess
import threading

import pytest

# Start + warm the moto server in a background thread at import time, so the
# heavy imports (boto3/moto), Flask's lazy URL-map build, and botocore's
# first-client JSON load all overlap with collection instead of stalling the
# xdist controller boot and each worker's first S3 test (~0.4s apiece).
_moto = {}


def _warm():
    import boto3
    from moto.server import ThreadedMotoServer

    server = ThreadedMotoServer(port=0, verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    _moto["endpoint"] = f"http://{host}:{port}"
    boto3.client(
        "s3", endpoint_url=_moto["endpoint"], region_name="us-east-1",
        aws_access_key_id="warm", aws_secret_access_key="warm",
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
    return _moto["endpoint"]  # daemon thread; no stop() — saves ~0.5s teardown


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
