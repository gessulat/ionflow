"""Worker-local phase spans, stored as framework telemetry rather than step tables."""

from __future__ import annotations

import datetime as dt
import threading
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import psutil

from .schema import Table
from .space import Space, utc_now


class Phase(Table):
    span_id: str
    parent_id: str | None
    step: str
    branch: str | None
    phase: str
    started: dt.datetime
    ended: dt.datetime | None
    status: str
    rss_start: int
    rss_end: int | None


class Phases:
    """Record nested spans with a separate step context for each worker thread."""

    def __init__(self) -> None:
        self._local = threading.local()
        self._lock = threading.Lock()
        self._rows: list[dict] = []
        self._process = psutil.Process()

    @contextmanager
    def step(self, step: str, branch: str | int | None) -> Iterator[None]:
        """Bind the running step without confusing concurrently executing siblings."""
        self._local.context = (step, None if branch is None else str(branch))
        self._local.stack = []
        try:
            yield
        finally:
            del self._local.context
            del self._local.stack

    @contextmanager
    def phase(self, name: str) -> Iterator[None]:
        """Observe a named phase; failure closes the span and propagates unchanged."""
        if not isinstance(name, str) or not name.strip():
            raise ValueError("phase name must be a nonempty string")
        if not hasattr(self._local, "context"):
            raise RuntimeError("phases require a running step")
        step, branch = self._local.context
        stack = self._local.stack
        row = {
            "span_id": uuid.uuid4().hex,
            "parent_id": stack[-1] if stack else None,
            "step": step,
            "branch": branch,
            "phase": name,
            "started": utc_now(),
            "ended": None,
            "status": "running",
            "rss_start": self._process.memory_info().rss,
            "rss_end": None,
        }
        with self._lock:
            self._rows.append(row)
        stack.append(row["span_id"])
        status = "complete"
        try:
            yield
        except BaseException:
            status = "failed"
            raise
        finally:
            with self._lock:
                row.update(ended=utc_now(), status=status, rss_end=self._process.memory_info().rss)
            stack.pop()

    def write(self, space: Space, key: str) -> None:
        """Flush a consistent snapshot; each task owns one telemetry object."""
        import pyarrow as pa

        with self._lock:
            rows = [dict(row) for row in self._rows]
        if rows:
            space.put(key, pa.Table.from_pylist(rows, schema=Phase.arrow_schema()))


@contextmanager
def phase(flow: Any, name: str) -> Iterator[None]:
    """Enter the recorder attached to a running flow."""
    recorder = getattr(flow, "_phases", None)
    if recorder is None:
        raise RuntimeError("phases require a running flow")
    with recorder.phase(name):
        yield
