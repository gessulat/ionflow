import pytest
from helpers import make_client

from schrein.testing import background_server

# schrein.testing, not bare moto — upstream moto's conditional PUTs race under
# threads and lose updates (decision 0003). A subprocess, never in-process:
# DuckDB httpfs holds relations open while reading and starves an in-process
# werkzeug server. Spawned + warmed at import to overlap with collection.
_endpoint = background_server()


@pytest.fixture(scope="session")
def moto_endpoint():
    return _endpoint()


@pytest.fixture
def s3(moto_endpoint, request):
    # unique bucket per test: no teardown needed (server dies with the process)
    client = make_client(moto_endpoint)
    bucket = f"b-{abs(hash(request.node.nodeid)) % 10**8}"
    client.create_bucket(Bucket=bucket)
    return client, bucket, moto_endpoint
