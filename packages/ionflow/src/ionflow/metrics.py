import datetime as dt
import threading
import time

import psutil
import pyarrow as pa

from .schema import Table

BASE_INTERVAL = 1.0  # seconds between samples at run start
MAX_SAMPLES = 1024  # per sampler; at the cap, pairs fold and the interval doubles
FLUSH_INTERVAL = 60.0  # buffer flushed to S3 this often: heartbeat + crash-durable metrics
STALE_AFTER = 300.0  # 5 missed flushes: a `running` run quieter than this counts as dead


class Sample(Table):
    """One resource sample; the schema of the `metrics` view.

    `run_id` is absent by design — the hive partition adds it at read time.
    """

    ts: dt.datetime  # naive UTC, like the ledger
    step: str | None  # active step (NULL = orchestrator idle / pool-wait)
    branch: str | None  # the item key, set only by process-mode branch workers
    rss: int  # bytes
    threads: int  # native threads (includes DuckDB's)
    cpu_user: float  # cumulative process seconds
    cpu_system: float
    process_id: int


SCHEMA = Sample.arrow_schema()


class Sampler:
    """Adaptive rss/threads/CPU sampler on a daemon thread; peaks survive folding.

    Folding keeps the later ts and cumulative CPU but the max of rss/threads, so
    any run — seconds or days — ends with <= MAX_SAMPLES rows covering its whole
    life. `mark()` forces a sample, so even sub-second steps appear. A `flush`
    callback also hands the buffer over on the FLUSH_INTERVAL cadence; its errors
    are swallowed, since S3 flakiness must kill neither this thread nor the run.

    >>> s = Sampler()
    >>> s.mark("load"); s.mark("load", "file-3")
    >>> [(r[1], r[2]) for r in s.stop()][:2]
    [('load', None), ('load', 'file-3')]
    """

    def __init__(self, flush=None):
        self._proc = psutil.Process()
        self._interval = BASE_INTERVAL
        self._rows = []
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._label = (None, None)
        self._flush = flush
        self._last_sample = self._last_flush = time.monotonic()
        self._thread = threading.Thread(target=self._loop, daemon=True)

    def start(self):
        self._thread.start()
        return self

    def mark(self, step, branch=None):
        """Label subsequent samples with (step, branch); sample immediately."""
        self._label = (step, branch)
        self._sample()

    def rows(self):
        """Thread-safe copy of the buffered samples."""
        with self._lock:
            return list(self._rows)

    def stop(self):
        """Final sample, halt and join the thread, return the buffered rows.

        Joining guarantees no in-flight periodic flush can land on S3 after
        the caller's final (superset) write.
        """
        self._sample()
        self._stop_event.set()
        if self._thread.is_alive():
            self._thread.join(timeout=5)  # bounded: a wedged flush PUT must not stall the run
        return self.rows()

    def _sample(self):
        with self._proc.oneshot():
            rss = self._proc.memory_info().rss
            threads = self._proc.num_threads()
            cpu = self._proc.cpu_times()
        step, branch = self._label
        row = (time.time(), step, branch, rss, threads, cpu.user, cpu.system, self._proc.pid)
        with self._lock:
            self._rows.append(row)
            if len(self._rows) >= MAX_SAMPLES:
                self._fold()

    def _fold(self):
        rows, folded, i = self._rows, [], 0
        while i < len(rows) - 1:
            a, b = rows[i], rows[i + 1]
            if (a[1], a[2]) == (b[1], b[2]):  # never merge across a step boundary
                folded.append((b[0], b[1], b[2], max(a[3], b[3]), max(a[4], b[4]), b[5], b[6], b[7]))
                i += 2
            else:
                folded.append(a)
                i += 1
        folded.extend(rows[i:])
        self._rows = folded
        self._interval *= 2

    def _tick(self, now):
        """Run whichever duties are due at `now` (monotonic seconds)."""
        if now - self._last_sample >= self._interval:
            self._last_sample = now
            self._sample()
        if self._flush and now - self._last_flush >= FLUSH_INTERVAL:
            self._last_flush = now
            try:
                self._flush(self.rows())
            except Exception:  # noqa: BLE001 — the heartbeat must never kill the run
                pass

    def _loop(self):
        while True:
            wait = min(self._interval, FLUSH_INTERVAL) if self._flush else self._interval
            if self._stop_event.wait(wait):
                return
            self._tick(time.monotonic())


def to_table(rows):
    """Sample rows (from one or more samplers) as a pyarrow Table.

    >>> to_table([(1.0, "a", None, 1, 2, 0.1, 0.2, 123)]).schema.equals(SCHEMA)
    True
    """
    columns = list(zip(*rows, strict=True))
    ts = pa.array((int(t * 1e6) for t in columns[0]), SCHEMA.types[0])
    rest = [pa.array(c, t) for c, t in zip(columns[1:], SCHEMA.types[1:], strict=True)]
    return pa.Table.from_arrays([ts, *rest], schema=SCHEMA)


def write_parquet(space, key, rows):
    """Upload sample rows as one parquet object (no-op for empty rows)."""
    if rows:
        space.put(key, to_table(rows))
