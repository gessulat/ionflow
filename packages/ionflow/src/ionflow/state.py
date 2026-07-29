"""A run's mutable state and its two bookkeeping records, as types: `Progress`
owns what has been written so far, `RunRecord` the run's identity and outcome.
Both know their on-disk forms, so the runner never spells them out.
"""

import datetime as dt
import typing
from dataclasses import dataclass


class Slot(typing.NamedTuple):
    """One written part: a step and its branch index (0 for a plain step).

    >>> Slot.parse("vectors/12")
    Slot(step='vectors', branch=12)
    """

    step: str
    branch: int

    @classmethod
    def parse(cls, text):
        """Read the `steps_complete` form every release writes into `_meta.json`."""
        step, branch = text.rsplit("/", 1)
        return cls(step, int(branch))


class Progress:
    """A run's completed parts: Slot -> the part key written for it.

    `keys()` is in NUMERIC branch order, the order the catalog registers a step's
    parts in: registration order is the lake's data-file order, hence a reader's
    scan order, so insertion order — nondeterministic under a worker pool — would
    make a reducer's `first()`, `arg_max` ties or `list_agg` vary between runs.

    >>> p = Progress()
    >>> p.add(Slot("v", 2), "k2"); p.add(Slot("v", 0), "k0"); p.add(Slot("v", 1), "k1")
    >>> p.keys("v")
    ['k0', 'k1', 'k2']
    >>> p.width("v"), Slot("v", 1) in p, Slot("v", 9) in p
    (3, True, False)
    >>> p.to_list()
    ['v/0', 'v/1', 'v/2']
    """

    def __init__(self):
        self._parts: dict[str, dict[int, str]] = {}

    def add(self, slot, key):
        self._parts.setdefault(slot.step, {})[slot.branch] = key

    def __contains__(self, slot):
        return slot.branch in self._parts.get(slot.step, ())

    def width(self, step):
        return len(self._parts.get(step, ()))

    def keys(self, step):
        """The step's part keys in branch order — the catalog's registration order."""
        return [key for _, key in sorted(self._parts.get(step, {}).items())]

    def to_list(self):
        """`steps_complete` as `_meta.json` carries it: `"step/branch"`, sorted."""
        return sorted(f"{step}/{branch}" for step, parts in self._parts.items() for branch in parts)


@dataclass
class RunRecord:
    """One run's bookkeeping, and the two objects `persist` writes it to from one
    state: `_meta.json` (progress + resume provenance) and the ledger row (status).
    """

    run_id: str
    status: str
    started: dt.datetime
    finished: dt.datetime | None
    commit: str
    origin_run_id: str | None
    params: dict
    custom: dict
    fanout_keys: dict
    custom_fields: list  # params + Ledger columns, as arrow fields for the ledger schema

    @property
    def _ok(self):
        """True once the run is complete. Publish reads `_meta.json`'s `finished`
        as the success flag, so a failed run — which does get a ledger finish
        time — must not claim one there."""
        return self.status == "complete"

    def meta(self, done):
        """The `_meta.json` payload; `done` is `Progress.to_list()`."""
        ok = self._ok
        return {
            "run_id": self.run_id,
            "started": self.started.isoformat(),
            "finished": self.finished.isoformat() if ok and self.finished else None,
            "commit": self.commit,
            "origin_run_id": self.origin_run_id,
            "steps_complete": done,
            "fanout_keys": self.fanout_keys,
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

    def persist(self, space, done, status):
        """Write both records at `status`: `_meta.json` first, then the ledger row."""
        self.status = status
        space.write_meta(self.run_id, self.meta(done))
        space.upsert_runs([self.ledger_row()], self.custom_fields)
