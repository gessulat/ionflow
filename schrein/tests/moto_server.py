"""moto server subprocess with ATOMIC conditional PUTs.

Upstream moto checks If-Match/If-None-Match preconditions and stores the
object non-atomically: under the threaded werkzeug server, two concurrent
conditional PUTs against the same etag can BOTH pass the check, and the
second silently clobbers the first — a lost update real S3 can never
produce (its conditional-write check-and-swap is atomic). The CAS hammer
tests hit this reliably. A process-wide lock around conditional PUTs
restores real-S3 semantics; unconditional traffic (data-file uploads,
reads) stays concurrent.

Usage: python moto_server.py <port>
"""

import sys
import threading

from moto.server import DomainDispatcherApplication, create_backend_app
from werkzeug.serving import run_simple

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


if __name__ == "__main__":
    app = AtomicConditionalPuts(DomainDispatcherApplication(create_backend_app))
    run_simple("127.0.0.1", int(sys.argv[1]), app, threaded=True)
