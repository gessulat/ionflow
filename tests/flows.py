"""Flows used to exercise fan-out failure/resume and empty fan-out.

Importable (branch workers load it by file path) and controllable via env vars,
so a specific branch can be made to fail on one run and succeed on a resume.
"""

import os
import pathlib
import uuid

import pyarrow as pa

from ionflow import Branch, Collect, Fanout, Flow, FlowParams, Prototype, Table, fanout_key
from ionflow.storage import s3_client


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


class RetryFlow(Flow):
    """Every branch fails on its first attempt (marker files in RETRY_DIR count them)."""

    def source(self) -> Src:
        return pa.table({"g": [0, 1]})

    def parts(self, source: Src) -> Fanout[Part]:
        return [source.filter(f"g = {g}") for g in (0, 1)]

    def work(self, parts: Branch[Part]) -> Out:
        g = parts.project("g").fetchone()[0]
        marker = pathlib.Path(os.environ["RETRY_DIR"], f"tried_{g}")
        if not marker.exists():
            marker.touch()
            raise RuntimeError(f"transient failure on g={g}")
        return parts.project("g, g * 2 AS doubled")

    def agg(self, work: Collect[Out]) -> Agg:
        return work.aggregate("count(*) AS n, sum(doubled) AS total")


class ParamFlow(Flow):
    """Params bound on self, both in the orchestrator and inside branch workers."""

    class Params(FlowParams):
        mult: int = 1

    def source(self) -> Src:
        return pa.table({"g": [0, 1]})

    def parts(self, source: Src) -> Fanout[Part]:
        return [source.filter(f"g = {g}") for g in (0, 1)]

    def work(self, parts: Branch[Part]) -> Out:
        return parts.project(f"g, g * {self.mult} AS doubled")  # param read in a branch

    def agg(self, work: Collect[Out]) -> Agg:
        return work.aggregate("count(*) AS n, sum(doubled) AS total")


def _side_n(config):
    """The side input: an int stored in the bucket outside any run folder."""
    return int(s3_client(config).get_object(Bucket=config.bucket, Key="side/n")["Body"].read())


class SideInputFlow(Flow):
    """Reads a side object via self.config — in a root step and inside a branch."""

    def items(self) -> Fanout[Part]:
        return [pa.table({"g": [g]}) for g in range(_side_n(self.config))]

    def work(self, items: Branch[Part]) -> Out:
        n = _side_n(self.config)  # branch workers bind config on their own flow instance
        return items.project(f"g, g * {n} AS doubled")

    def agg(self, work: Collect[Out]) -> Agg:
        return work.aggregate("count(*) AS n, sum(doubled) AS total")


class ProtoFlow(Prototype, Flow):
    """Prototype: scratch namespace, dirty git ok, first step untyped."""

    def numbers(self):  # untyped: schema inferred at write time
        return pa.table({"x": [1, 2, 3]})

    def total(self, numbers) -> Agg:
        return numbers.aggregate("count(*) AS n, sum(x) AS total")


class KeyedItem(Table):
    g: int
    item: str = fanout_key


class KeyedOut(Table):
    item: str
    token: str


class KeyedAgg(Table):
    n: int
    token: str


class KeyedFlow(Flow):
    """Keyed fan-out sized by the side object; uuid tokens expose copy vs recompute."""

    def items(self) -> Fanout[KeyedItem]:
        return [pa.table({"g": [g], "item": [f"file-{g}"]}) for g in range(_side_n(self.config))]

    def work(self, items: Branch[KeyedItem]) -> KeyedOut:
        item = items.project("item").fetchone()[0]
        return pa.table({"item": [item], "token": [uuid.uuid4().hex]})

    def agg(self, work: Collect[KeyedOut]) -> KeyedAgg:
        n = work.aggregate("count(*) AS n").fetchone()[0]
        return pa.table({"n": [n], "token": [uuid.uuid4().hex]})


class DupKeyFlow(Flow):
    """Two fan-out items share a fanout key — the run must refuse."""

    def items(self) -> Fanout[KeyedItem]:
        return [pa.table({"g": [g], "item": ["same"]}) for g in (0, 1)]

    def work(self, items: Branch[KeyedItem]) -> KeyedOut:
        return items.project("item, 'x' AS token")


class TwoKeyFlow(Flow):
    """Branches over one keyed fan-out while collecting another (cross-input taint)."""

    def fixed(self) -> Fanout[KeyedItem]:
        return [pa.table({"g": [g], "item": [f"fixed-{g}"]}) for g in (0, 1)]

    def others(self) -> Fanout[KeyedItem]:
        return [pa.table({"g": [g], "item": [f"other-{g}"]}) for g in range(_side_n(self.config))]

    def work(self, fixed: Branch[KeyedItem], others: Collect[KeyedItem]) -> KeyedOut:
        item = fixed.project("item").fetchone()[0]
        n = others.aggregate("count(*) AS n").fetchone()[0]  # output depends on the collect
        return pa.table({"item": [f"{item}@{n}"], "token": [uuid.uuid4().hex]})


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
