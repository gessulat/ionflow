import threading
import time

import psutil
import pyarrow as pa

from . import storage

BASE_INTERVAL = 1.0  # seconds between samples at run start
MAX_SAMPLES = 1024  # per sampler; at the cap, pairs fold and the interval doubles

SCHEMA = pa.schema(
    [
        pa.field("ts", pa.timestamp("us")),  # naive UTC, like the ledger
        pa.field("step", pa.string()),
        pa.field("branch", pa.int64()),
        pa.field("rss", pa.int64()),  # bytes
        pa.field("threads", pa.int64()),  # native threads (includes DuckDB's)
        pa.field("cpu_user", pa.float64()),  # cumulative process seconds
        pa.field("cpu_system", pa.float64()),
    ]
)


class Sampler:
    """Adaptive process sampler: peaks survive folding, size is bounded.

    Samples rss / native threads / cumulative CPU every `BASE_INTERVAL`s on a
    daemon thread. When the buffer reaches `MAX_SAMPLES`, adjacent pairs fold
    (latest ts + cumulative CPU, max of rss/threads) and the interval doubles,
    so any run — seconds or days — ends with <= MAX_SAMPLES rows covering its
    whole life. `mark()` records step boundaries with a forced sample, so even
    sub-second steps appear.

    >>> s = Sampler()
    >>> s.mark("load"); s.mark("load", 3)
    >>> [(r[1], r[2]) for r in s.stop()][:2]
    [('load', None), ('load', 3)]
    """

    def __init__(self):
        self._proc = psutil.Process()
        self._interval = BASE_INTERVAL
        self._rows = []
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._label = (None, None)
        self._thread = threading.Thread(target=self._loop, daemon=True)

    def start(self):
        self._thread.start()
        return self

    def mark(self, step, branch=None):
        """Label subsequent samples with (step, branch); sample immediately."""
        self._label = (step, branch)
        self._sample()

    def stop(self):
        """Final sample, halt the thread, return the buffered rows."""
        self._sample()
        self._stop_event.set()
        return list(self._rows)

    def _sample(self):
        with self._proc.oneshot():
            rss = self._proc.memory_info().rss
            threads = self._proc.num_threads()
            cpu = self._proc.cpu_times()
        step, branch = self._label
        row = (time.time(), step, branch, rss, threads, cpu.user, cpu.system)
        with self._lock:
            self._rows.append(row)
            if len(self._rows) >= MAX_SAMPLES:
                self._fold()

    def _fold(self):
        rows, folded, i = self._rows, [], 0
        while i < len(rows) - 1:
            a, b = rows[i], rows[i + 1]
            if (a[1], a[2]) == (b[1], b[2]):  # never merge across a step boundary
                folded.append((b[0], b[1], b[2], max(a[3], b[3]), max(a[4], b[4]), b[5], b[6]))
                i += 2
            else:
                folded.append(a)
                i += 1
        folded.extend(rows[i:])
        self._rows = folded
        self._interval *= 2

    def _loop(self):
        while not self._stop_event.wait(self._interval):
            self._sample()


def to_table(rows):
    """Sample rows (from one or more samplers) as a pyarrow Table.

    >>> to_table([(1.0, "a", None, 1, 2, 0.1, 0.2)]).schema.equals(SCHEMA)
    True
    """
    columns = list(zip(*rows))
    ts = pa.array((int(t * 1e6) for t in columns[0]), pa.timestamp("us"))
    rest = [pa.array(c, f.type) for c, f in zip(columns[1:], list(SCHEMA)[1:])]
    return pa.Table.from_arrays([ts, *rest], schema=SCHEMA)


def write_parquet(client, bucket, key, rows):
    """Upload sample rows as one parquet object (no-op for empty rows)."""
    if rows:
        storage.put_parquet(client, bucket, key, to_table(rows))
