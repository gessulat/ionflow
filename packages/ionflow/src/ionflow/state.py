"""A run's mutable state and its bookkeeping, as types: `Manifest` owns which
parts exist and where, `RunRecord` the run's identity and outcome. Both know
their serialized forms, so the runner never spells them out.
"""

import datetime as dt
import json
import typing
from dataclasses import dataclass, field

# The `runs` table's column names — reserved against params/Ledger collisions.
BASE_COLS = frozenset(
    {"run_id", "status", "started", "finished", "commit", "origin_run_id", "params", "custom", "artifacts"}
)


class Slot(typing.NamedTuple):
    """One part: a step and the item key naming it (0 for a plain step).

    Item keys ARE the storage identity (decision 0018): an unkeyed fan-out's
    items are keyed by position ints, a keyed fan-out's by its `fanout_key`
    values — and a chained branch step answers to the same key (0014), so a
    resumed run matches parts across runs by (step, key) alone.
    """

    step: str
    key: str | int


class Manifest:
    """A run's parts: Slot -> the store key holding that part.

    `keys()`/`paths()` are in sorted key order — the single deterministic order
    (catalog registration, hence a reducer's scan order): insertion order varies
    under a worker pool, and a resumed run's mix of reused and computed parts
    has no meaningful insertion order at all. Keys within one step never mix
    types (ints for positions, or the fanout_key column's str/int values).

    >>> m = Manifest()
    >>> m.add(Slot("v", 2), "k2"); m.add(Slot("v", 0), "k0"); m.add(Slot("v", 1), "k1")
    >>> m.keys("v"), m.paths("v")
    ([0, 1, 2], ['k0', 'k1', 'k2'])
    >>> m.width("v"), Slot("v", 1) in m, Slot("v", 9) in m
    (3, True, False)
    >>> Manifest.from_meta(m.meta()).get("v", 2)
    'k2'
    """

    def __init__(self):
        self._parts: dict[str, dict[str | int, str]] = {}

    def add(self, slot, path):
        self._parts.setdefault(slot.step, {})[slot.key] = path

    def __contains__(self, slot):
        return slot.key in self._parts.get(slot.step, ())

    def get(self, step, key):
        """The part path written for (step, key), or None."""
        return self._parts.get(step, {}).get(key)

    def width(self, step):
        return len(self._parts.get(step, ()))

    def keys(self, step):
        """The step's item keys, sorted."""
        return sorted(self._parts.get(step, ()))

    def paths(self, step):
        """The step's part paths in sorted key order — the one registration/read order."""
        parts = self._parts.get(step, {})
        return [parts[k] for k in sorted(parts)]

    def slots(self, step=None):
        """Every (Slot, path) pair, one step's or all, in sorted key order."""
        steps = [step] if step is not None else sorted(self._parts)
        return [(Slot(s, k), self._parts[s][k]) for s in steps for k in sorted(self._parts.get(s, ()))]

    def meta(self):
        """JSON form: {step: [[key, path], ...]} — keys keep their int/str type."""
        return {s: [[k, p] for (_, k), p in self.slots(s)] for s in sorted(self._parts)}

    @classmethod
    def from_meta(cls, data):
        m = cls()
        for step, pairs in (data or {}).items():
            for key, path in pairs:
                m.add(Slot(step, key), path)
        return m

    @classmethod
    def from_rows(cls, rows):
        """From (step, key, path) triples — the lake `manifest` table's shape."""
        m = cls()
        for step, key, path in rows:
            m.add(Slot(step, key), path)
        return m


@dataclass
class RunRecord:
    """One run's bookkeeping, and the two forms it serializes to: the lake's
    `runs` row (the one authority, decision 0019) and the `_meta.json`
    recovery export that `backfill` reads when the register is lost.
    """

    run_id: str
    status: str
    started: dt.datetime
    finished: dt.datetime | None
    commit: str
    origin_run_id: str | None
    params: dict
    custom: dict
    artifacts: dict = field(default_factory=dict)

    @property
    def _ok(self):
        """True once the run is complete. A failed run keeps its finish time in
        the ledger row, but must not claim custom results — nor a `finished` in
        meta, which publish would read as the success flag."""
        return self.status == "complete"

    def row(self):
        """The lake `runs` row. params/custom travel as JSON text: the table's
        schema is fixed forever; the `ledger` view derives typed columns from
        the flow's declarations instead (no migration workflow, deliberately)."""
        return [
            self.run_id,
            self.status,
            self.started,
            self.finished,
            self.commit,
            self.origin_run_id,
            json.dumps(self.params),
            json.dumps(self.custom if self._ok else {}, default=str),
            json.dumps(self.artifacts),
        ]

    def meta(self, manifest, activities=None):
        """The `_meta.json` payload; `manifest` is the run's `Manifest`."""
        ok = self._ok
        return {
            "run_id": self.run_id,
            "status": self.status,
            "started": self.started.isoformat(),
            "finished": self.finished.isoformat() if ok and self.finished else None,
            "commit": self.commit,
            "origin_run_id": self.origin_run_id,
            "manifest": manifest.meta(),
            "params": self.params,
            "custom": self.custom if ok else {},
            "artifacts": self.artifacts,
            "activities": activities or {},
        }
