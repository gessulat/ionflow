"""A run's mutable state and its bookkeeping, as types: `Manifest` owns which
parts exist and where, `RunRecord` the run's identity and outcome. Both know
their serialized forms, so the runner never spells them out.
"""

import datetime as dt
import typing
from dataclasses import dataclass


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


@dataclass
class RunRecord:
    """One run's bookkeeping, and the two records `persist` writes from one
    state: `_meta.json` (manifest + resume provenance) and the ledger row.
    """

    run_id: str
    status: str
    started: dt.datetime
    finished: dt.datetime | None
    commit: str
    origin_run_id: str | None
    params: dict
    custom: dict
    custom_fields: list  # params + Ledger columns, as arrow fields for the ledger schema

    @property
    def _ok(self):
        """True once the run is complete. Publish reads `_meta.json`'s `finished`
        as the success flag, so a failed run — which does get a ledger finish
        time — must not claim one there."""
        return self.status == "complete"

    def meta(self, manifest):
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
        }

    def ledger_row(self):
        """The ledger's CAS parquet row: base columns, params and Ledger columns flattened."""
        return {
            "run_id": self.run_id,
            "status": self.status,
            "started": self.started,
            "finished": self.finished,
            "commit": self.commit,
            "origin_run_id": self.origin_run_id,
            **self.params,
            **(self.custom if self._ok else {}),
        }

    def persist(self, space, manifest, status):
        """Write both records at `status`: `_meta.json` first, then the ledger row."""
        self.status = status
        space.write_meta(self.run_id, self.meta(manifest))
        space.upsert_runs([self.ledger_row()], self.custom_fields)
