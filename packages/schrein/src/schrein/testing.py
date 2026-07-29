"""Test double for S3: moto with ATOMIC conditional PUTs.

Upstream moto checks the precondition and stores the object non-atomically, so
under the threaded werkzeug server two conditional PUTs against the same etag
can BOTH pass and the second silently clobbers the first — a lost update real S3
can never produce (decision 0003). A process-wide lock around conditional PUTs
restores real-S3 semantics; unconditional traffic stays concurrent.

Run as a subprocess — never in-process, DuckDB httpfs starves an in-process
werkzeug server: `python -m schrein.testing <port>`. Requires moto (dev only).
"""

import atexit
import socket
import subprocess
import sys
import threading
import time

_lock = threading.Lock()


class AtomicConditionalPuts:
    def __init__(self, app):
        self.app = app

    def __call__(self, environ, start_response):
        conditional = environ["REQUEST_METHOD"] == "PUT" and (
            environ.get("HTTP_IF_MATCH") or environ.get("HTTP_IF_NONE_MATCH")
        )
        if not conditional:
            return self.app(environ, start_response)
        with _lock:  # flask materializes the response inside the call
            return self.app(environ, start_response)


def serve(port: int) -> None:
    from moto.server import DomainDispatcherApplication, create_backend_app
    from werkzeug.serving import run_simple

    app = AtomicConditionalPuts(DomainDispatcherApplication(create_backend_app))
    run_simple("127.0.0.1", port, app, threaded=True)


def background_server():
    """Spawn + warm a server subprocess in a background thread; call at conftest
    module scope so the boot and the heavy imports (boto3, botocore's
    first-client JSON load) overlap with test collection.

    Returns a zero-arg callable that joins the warmup and yields the endpoint
    URL. The subprocess is killed atexit; callers create a unique bucket per
    test and never clean up — the server dies with the run.
    """
    state = {}

    def warm():
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
        proc = subprocess.Popen(
            [sys.executable, "-m", "schrein.testing", str(port)],
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
        import boto3  # deferred on purpose: its import cost is part of the overlap

        endpoint = f"http://127.0.0.1:{port}"
        boto3.client(
            "s3",
            endpoint_url=endpoint,
            region_name="us-east-1",
            aws_access_key_id="warm",
            aws_secret_access_key="warm",
        ).list_buckets()
        state["endpoint"] = endpoint

    thread = threading.Thread(target=warm, daemon=True)
    thread.start()

    def endpoint():
        thread.join()
        return state["endpoint"]

    return endpoint


if __name__ == "__main__":
    serve(int(sys.argv[1]))
