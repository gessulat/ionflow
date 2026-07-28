"""Deprecated: `s3_client` only, the documented side-input escape hatch
(`ionflow.storage.s3_client(self.config)` — see the README).

Everything else moved: client + connections to `store.py`, keys + run objects to
`space.py`, arrow/parquet marshalling to `arrow.py`.
"""

from .store import s3_client

__all__ = ["s3_client"]
