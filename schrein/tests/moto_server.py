"""Entry point for the test S3 server — see schrein.testing for the why."""

import sys

from schrein.testing import serve

if __name__ == "__main__":
    serve(int(sys.argv[1]))
