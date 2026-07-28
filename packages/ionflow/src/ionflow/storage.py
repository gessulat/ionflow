"""Deprecated shim: `s3_client` only, the documented side-input escape hatch
(`ionflow.storage.s3_client(self.config)` — see the README).
"""

from .store import s3_client

__all__ = ["s3_client"]
