import atexit
import pathlib
import socket
import subprocess
import sys
import threading
import time

import pytest

# Start + warm the moto server in a background thread at import time, so the
# heavy imports (boto3) overlap with collection. The server runs as a
# subprocess, never in-process: DuckDB httpfs holds relations open while
# reading and starves an in-process werkzeug server (lesson from ionflow).
# It is the moto_server.py wrapper, not bare moto: upstream moto's
# conditional PUTs race under threads and lose updates (see that file).
_moto = {}


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _warm():
    import boto3

    port = _free_port()
    server = pathlib.Path(__file__).parent / "moto_server.py"
    proc = subprocess.Popen(
        [sys.executable, str(server), str(port)],
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


def make_client(endpoint):
    """A fresh boto3 client against moto (one per thread in hammer tests)."""
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def moto_secret(endpoint):
    """DuckDB S3 secret pointing at the moto server (path-style, no SSL)."""
    host = endpoint.removeprefix("http://")
    return (
        "CREATE OR REPLACE SECRET moto (TYPE s3, KEY_ID 'test', SECRET 'test', "
        f"REGION 'us-east-1', ENDPOINT '{host}', USE_SSL false, URL_STYLE 'path')"
    )


@pytest.fixture(scope="session")
def moto_endpoint():
    _warmup.join()
    return _moto["endpoint"]  # subprocess; killed via atexit


@pytest.fixture
def s3(moto_endpoint, request):
    # unique bucket per test: no teardown needed (server dies with the process)
    client = make_client(moto_endpoint)
    bucket = f"b-{abs(hash(request.node.nodeid)) % 10**8}"
    client.create_bucket(Bucket=bucket)
    return client, bucket, moto_endpoint
