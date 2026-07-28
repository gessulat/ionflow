"""Test double for S3: moto with ATOMIC conditional PUTs.

Upstream moto checks the precondition and stores the object non-atomically, so
under the threaded werkzeug server two conditional PUTs against the same etag
can BOTH pass and the second silently clobbers the first — a lost update real S3
can never produce (decision 0003). A process-wide lock around conditional PUTs
restores real-S3 semantics; unconditional traffic stays concurrent.

Run as a subprocess — never in-process, DuckDB httpfs starves an in-process
werkzeug server: `python -m schrein.testing <port>`. Requires moto (dev only).
"""

import sys
import threading

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


if __name__ == "__main__":
    serve(int(sys.argv[1]))
