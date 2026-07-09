"""Flows used to exercise fan-out failure/resume and empty fan-out.

Importable (branch workers load it by file path) and controllable via env vars,
so a specific branch can be made to fail on one run and succeed on a resume.
"""

import os
import pathlib
import uuid

import pyarrow as pa

from microflow import Branch, Collect, Fanout, Flow, Table


class Src(Table):
    g: int


class Part(Table):
    g: int


class Out(Table):
    g: int
    doubled: int


class Agg(Table):
    n: int
    total: int


class FlakyFlow(Flow):
    """Branch g==1 fails while FLAKY_FAIL=1; each `work` call leaves a marker."""

    def source(self) -> Src:
        return pa.table({"g": [0, 1, 2]})

    def parts(self, source: Src) -> Fanout[Part]:
        return [source.filter(f"g = {g}") for g in (0, 1, 2)]

    def work(self, parts: Branch[Part]) -> Out:
        g = parts.project("g").fetchone()[0]
        pathlib.Path(os.environ["FLAKY_DIR"], f"exec_{g}_{uuid.uuid4().hex}").touch()
        if g == 1 and os.environ.get("FLAKY_FAIL") == "1":
            raise RuntimeError("boom on g=1")
        return parts.project("g, g * 2 AS doubled")

    def agg(self, work: Collect[Out]) -> Agg:
        return work.aggregate("count(*) AS n, sum(doubled) AS total")


class EmptyFanFlow(Flow):
    """Fan-out that yields zero branches."""

    def source(self) -> Src:
        return pa.table({"g": [0, 1, 2]})

    def parts(self, source: Src) -> Fanout[Part]:
        return []

    def work(self, parts: Branch[Part]) -> Out:
        return parts.project("g, g * 2 AS doubled")

    def agg(self, work: Collect[Out]) -> Agg:
        return work.aggregate("count(*) AS n, coalesce(sum(doubled), 0) AS total")
