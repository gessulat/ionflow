"""Flows used to exercise fan-out failure/resume and empty fan-out.

Importable (branch workers load it by file path) and controllable via env vars,
so a specific branch can be made to fail on one run and succeed on a resume.
"""

import os
import pathlib
import time
import uuid

import pyarrow as pa

from ionflow import Branch, Collect, Fanout, Flow, FlowParams, LedgerColumns, Table, fanout_key
from ionflow.store import Store, s3_client


class Src(Table):
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

    def parts(self, source: Src) -> Fanout[Src]:
        return [source.filter(f"g = {g}") for g in (0, 1, 2)]

    def work(self, parts: Branch[Src]) -> Out:
        g = parts.project("g").fetchone()[0]
        _mark("work", g)
        if g == 1 and os.environ.get("FLAKY_FAIL") == "1":
            raise RuntimeError("boom on g=1")
        return parts.project("g, g * 2 AS doubled")

    def agg(self, work: Collect[Out]) -> Agg:
        return work.aggregate("count(*) AS n, sum(doubled) AS total")


class ParamFlow(Flow):
    """Params bound on self, both in the orchestrator and inside branch workers."""

    class Params(FlowParams):
        mult: int = 1

    def source(self) -> Src:
        return pa.table({"g": [0, 1]})

    def parts(self, source: Src) -> Fanout[Src]:
        return [source.filter(f"g = {g}") for g in (0, 1)]

    def work(self, parts: Branch[Src]) -> Out:
        return parts.project(f"g, g * {self.mult} AS doubled")  # param read in a branch

    def agg(self, work: Collect[Out]) -> Agg:
        return work.aggregate("count(*) AS n, sum(doubled) AS total")


class NoteFlow(Flow):
    """Declares one ledger column, set in the first step. NOTE_SKIP leaves it
    unset; NOTE_FAIL fails the later step after it was set."""

    class Ledger(LedgerColumns):
        note: str

    def source(self) -> Src:
        if not os.environ.get("NOTE_SKIP"):
            self.note = "set during the run"
        return pa.table({"g": [1]})

    def doubled(self, source: Src) -> Out:
        if os.environ.get("NOTE_FAIL"):
            raise RuntimeError("boom in doubled")
        return source.project("g, g * 2 AS doubled")


def _side_n(config):
    """The side input: an int stored outside any run folder."""
    return int(Store.of(config).get("side/n"))


def put_n(config, n):
    """Set the `side/n` object `_side_n` reads — the tests' fan-out width knob."""
    Store.of(config).put("side/n", str(n).encode())


def _side_order(config):
    """Fan-out order knob: the optional `side/order` object ("rev" reverses)."""
    blob = Store.of(config).get("side/order")
    return blob.decode() if blob else "fwd"


class SideInputFlow(Flow):
    """Reads a side object via self.config — in a root step and inside a branch."""

    def items(self) -> Fanout[Src]:
        return [pa.table({"g": [g]}) for g in range(_side_n(self.config))]

    def work(self, items: Branch[Src]) -> Out:
        n = _side_n(self.config)  # branch workers bind config on their own flow instance
        return items.project(f"g, g * {n} AS doubled")

    def agg(self, work: Collect[Out]) -> Agg:
        return work.aggregate("count(*) AS n, sum(doubled) AS total")


class TwoGroupFlow(Flow):
    """Two fan-out groups in one run, each collecting a fan-out of its own width.

    Thread mode is the only place one process runs both groups, so it is the only
    place a worker binding cached for the first group could still be around for
    the second — with the first group's collect widths.
    """

    def side_a(self) -> Fanout[Src]:
        return [pa.table({"g": [g]}) for g in range(2)]

    def side_b(self) -> Fanout[Src]:
        return [pa.table({"g": [g]}) for g in range(5)]

    def items_a(self) -> Fanout[Src]:
        return [pa.table({"g": [g]}) for g in range(2)]

    def items_b(self) -> Fanout[Src]:
        return [pa.table({"g": [g]}) for g in range(2)]

    def work_a(self, items_a: Branch[Src], side_a: Collect[Src]) -> Out:
        return items_a.project(f"g, {side_a.aggregate('count(*)').fetchone()[0]} AS doubled")

    def work_b(self, items_b: Branch[Src], side_b: Collect[Src]) -> Out:
        return items_b.project(f"g, {side_b.aggregate('count(*)').fetchone()[0]} AS doubled")


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


class Refined(Table):
    g: int  # the fan-out item's g, read from the fan-out part
    seen: int  # the same g as it arrives through the upstream branch's output
    quadrupled: int


_MARK_DIRS = ("CHAIN_DIR", "PIPE_DIR", "FLAKY_DIR")  # one per flow family; never set together


def _mark(step, item):
    """Record one execution of `step` for `item`, if a marker dir is configured."""
    if d := next((v for n in _MARK_DIRS if (v := os.environ.get(n))), None):
        pathlib.Path(d, f"exec|{step}|{item}|{uuid.uuid4().hex}").touch()


def exec_counts(d, step):
    """item -> how many times `step` actually ran, from `_mark`'s marker files."""
    counts = {}
    for f in pathlib.Path(d).iterdir():
        kind, name, item, _ = f.name.split("|", 3) if "|" in f.name else (None, None, None, None)
        if kind == "exec" and name == step:
            counts[item] = counts.get(item, 0) + 1
    return counts


class ChainFlow(Flow):
    """Chained branches: `refine` branches over `work`, itself a branch of `parts`.

    CHAIN_FAIL="<step>:<g>" fails one branch of one step; CHAIN_DIR collects a
    marker per branch execution, so a resume's reuse is visible.
    """

    def source(self) -> Src:
        return pa.table({"g": [0, 1, 2]})

    def parts(self, source: Src) -> Fanout[Src]:
        return [source.filter(f"g = {g}") for g in (0, 1, 2)]

    def work(self, parts: Branch[Src]) -> Out:
        g = parts.project("g").fetchone()[0]
        _mark("work", g)
        if os.environ.get("CHAIN_FAIL") == f"work:{g}":
            raise RuntimeError(f"boom on work g={g}")
        return parts.project("g, g * 2 AS doubled")

    def refine(self, parts: Branch[Src], work: Branch[Out]) -> Refined:
        """Two branch inputs at the same index: the fan-out item and `work`'s output."""
        g = parts.project("g").fetchone()[0]
        _mark("refine", g)
        if os.environ.get("CHAIN_FAIL") == f"refine:{g}":
            raise RuntimeError(f"boom on refine g={g}")
        return work.project(f"{g} AS g, g AS seen, doubled * 2 AS quadrupled")

    def total(self, refine: Collect[Refined]) -> Agg:
        return refine.aggregate("count(*) AS n, sum(quadrupled) AS total")


class KeyedRefined(Table):
    item: str
    token: str
    upstream: str  # the token of the `work` output this branch was computed from


class KeyedChainFlow(Flow):
    """Keyed fan-out feeding a chained branch step, for incremental resume.

    Fan-out size comes from `side/n` and its order from `side/order`, so a resume
    can grow, shrink or merely reorder the work-list; KEYED_CHAIN_FAIL="<item>"
    fails `refine` for one item, which marks its executions in CHAIN_DIR.
    """

    def items(self) -> Fanout[KeyedItem]:
        gs = list(range(_side_n(self.config)))
        if _side_order(self.config) == "rev":
            gs.reverse()
        return [pa.table({"g": [g], "item": [f"file-{g}"]}) for g in gs]

    def work(self, items: Branch[KeyedItem]) -> KeyedOut:
        item = items.project("item").fetchone()[0]
        return pa.table({"item": [item], "token": [uuid.uuid4().hex]})

    def refine(self, work: Branch[KeyedOut]) -> KeyedRefined:
        item, token = work.project("item, token").fetchone()
        _mark("refine", item)
        if item == os.environ.get("KEYED_CHAIN_FAIL"):
            raise RuntimeError(f"boom on {item}")
        return pa.table({"item": [item], "token": [uuid.uuid4().hex], "upstream": [token]})


class Job(Table):
    raw: str = fanout_key  # stands in for a .raw file path


class Converted(Table):
    raw: str
    db: str  # local artifact, handed to the next sub-steps by path
    pid: int
    started: float


class Uploaded(Table):
    """A side effect made queryable: what the upload wrote, and where."""

    raw: str
    uri: str
    etag: str
    concurrent: bool


class Psm(Table):
    raw: str
    peptides: int
    pid: int
    concurrent: bool
    started: float


class Summary(Table):
    n: int
    peptides: int


def _pipe_dir():
    return pathlib.Path(os.environ["PIPE_DIR"])


def _rendezvous(name, other, item, timeout=5.0):
    """Announce `name` for `item` and wait for `other`; False if it never showed up.

    True only when both sub-steps of one item were in flight at the same time,
    so a serialised sub-DAG fails the assertion instead of merely being slower.
    """
    _pipe_dir().joinpath(f"at_{name}_{item}").touch()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if _pipe_dir().joinpath(f"at_{other}_{item}").exists():
            return True
        time.sleep(0.01)
    return False


def _fails_now(step, item):
    """PIPE_FAIL="<step>:<item>" fails that sub-step — once if PIPE_ONCE is set."""
    if os.environ.get("PIPE_FAIL") != f"{step}:{item}":
        return False
    if not os.environ.get("PIPE_ONCE"):
        return True
    marker = _pipe_dir() / f"tried_{step}_{item}"
    if marker.exists():
        return False
    marker.touch()
    return True


class PipelineFlow(Flow):
    """Per-item sub-DAG: convert a raw file, then upload and search it concurrently.

    The shape of a peptide search — the .db is a local artifact both later
    sub-steps read by path, which holds because one worker owns one whole item.
    """

    def jobs(self) -> Fanout[Job]:
        return [pa.table({"raw": [f"raw-{i}"]}) for i in range(_side_n(self.config))]

    def convert(self, jobs: Branch[Job]) -> Converted:
        raw = jobs.project("raw").fetchone()[0]
        _mark("convert", raw)
        if _fails_now("convert", raw):
            raise RuntimeError(f"convert failed on {raw}")
        db = _pipe_dir() / f"{raw}.db"
        db.write_text(f"spectra for {raw}")  # the 10 GB .db, in miniature
        return pa.table({"raw": [raw], "db": [str(db)], "pid": [os.getpid()], "started": [time.time()]})

    def upload_db(self, convert: Branch[Converted]) -> Uploaded:
        raw, db = convert.project("raw, db").fetchone()
        _mark("upload_db", raw)
        if _fails_now("upload_db", raw):
            raise RuntimeError(f"upload failed on {raw}")
        key = f"uploads/{raw}.db"  # same process as convert: read its artifact by path
        etag = s3_client(self.config).put_object(
            Bucket=self.config.bucket, Key=key, Body=pathlib.Path(db).read_bytes()
        )["ETag"]
        return pa.table(
            {
                "raw": [raw],
                "uri": [f"s3://{self.config.bucket}/{key}"],
                "etag": [etag.strip('"')],
                "concurrent": [_rendezvous("upload_db", "search", raw)],
            }
        )

    def search(self, convert: Branch[Converted]) -> Psm:
        raw, db = convert.project("raw, db").fetchone()
        _mark("search", raw)
        if _fails_now("search", raw):
            raise RuntimeError(f"search failed on {raw}")
        peptides = len(pathlib.Path(db).read_text().split())
        return pa.table(
            {
                "raw": [raw],
                "peptides": [peptides],
                "pid": [os.getpid()],
                "concurrent": [_rendezvous("search", "upload_db", raw)],
                "started": [time.time()],
            }
        )

    def summary(self, search: Collect[Psm]) -> Summary:
        return search.aggregate("count(*) AS n, sum(peptides) AS peptides")


class EmptyFanFlow(Flow):
    """Fan-out that yields zero branches."""

    def source(self) -> Src:
        return pa.table({"g": [0, 1, 2]})

    def parts(self, source: Src) -> Fanout[Src]:
        return []

    def work(self, parts: Branch[Src]) -> Out:
        return parts.project("g, g * 2 AS doubled")

    def agg(self, work: Collect[Out]) -> Agg:
        return work.aggregate("count(*) AS n, coalesce(sum(doubled), 0) AS total")
