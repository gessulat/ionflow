import subprocess

import pytest
from moto.server import ThreadedMotoServer

from miniflow import storage
from miniflow.config import Config


def _git(cwd, *args):
    subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True)


@pytest.fixture
def clean_repo(tmp_path):
    repo = tmp_path / "repo"  # own subdir; tmp_path stays free for test scratch files
    repo.mkdir()
    _git(repo, "init")
    _git(repo, "config", "user.email", "t@t.com")
    _git(repo, "config", "user.name", "t")
    (repo / "flow.py").write_text("# flow\n")
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "init")
    return str(repo)


@pytest.fixture(scope="session")
def moto_endpoint():
    server = ThreadedMotoServer(port=0)
    server.start()
    host, port = server.get_host_and_port()
    yield f"http://{host}:{port}"
    server.stop()


@pytest.fixture
def s3cfg(moto_endpoint):
    cfg = Config(
        bucket="test-bucket",
        prefix="mf",
        endpoint_url=moto_endpoint,
        region="us-east-1",
        access_key="test",
        secret_key="test",
    )
    client = storage.s3_client(cfg)
    client.create_bucket(Bucket=cfg.bucket)
    yield cfg
    objs = client.list_objects_v2(Bucket=cfg.bucket).get("Contents", [])
    for o in objs:
        client.delete_object(Bucket=cfg.bucket, Key=o["Key"])
    client.delete_bucket(Bucket=cfg.bucket)
