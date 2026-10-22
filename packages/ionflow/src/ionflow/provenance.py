"""Worker-local artifact use and output events; the orchestrator commits them."""

import copy
import json
import threading
import typing

from .artifact import Artifact, Artifacts
from .space import utc_now


def activity_key(step: str, key: str | int) -> str:
    """JSON preserves item-key types and cannot collide with step-name punctuation.

    >>> activity_key("search", "a:b")
    '["search", "a:b"]'
    """
    return json.dumps([step, key])


class ArtifactRuntime:
    """Thread-local step events, including independent sub-steps of one item."""

    def __init__(self, registry: Artifacts, flow_id: str, run_id: str) -> None:
        self.registry, self.flow_id, self.run_id = registry, flow_id, run_id
        self._local = threading.local()

    def begin(self, step: str, key: str | int) -> None:
        self._local.activity = {
            "step": step,
            "key": key,
            "uses": [],
            "outputs": [],
            "ancestors": [],
            "started": utc_now().isoformat(),
        }

    def restore(self, record: dict) -> None:
        self._local.activity = copy.deepcopy(record)

    def inherit(self, ancestors: list[str]) -> None:
        record = self._local.activity
        record["ancestors"] = sorted(set(record["ancestors"]) | set(ancestors))

    def use(self, value: str | Artifact, *, role: str = "input") -> Artifact:
        """Declare a dynamically selected dependency, preserving its pinned metadata."""
        ref = self.registry.get(value) if isinstance(value, str) else value
        if ref._record is None:
            raise ValueError("publish an artifact before using it")
        ref = self.registry.put(ref)
        record = self._local.activity
        entry = {"role": role, "artifact": ref.snapshot()}
        if entry not in record["uses"]:
            record["uses"].append(entry)
        self.inherit([ref.id])
        return ref

    def publish[A: Artifact](
        self,
        candidate: A,
        *,
        role: str = "output",
        parents: list[Artifact] | None = None,
    ) -> A:
        """Publish a side output and report its exact parents with this step's result."""
        existing = candidate._record is not None
        ref = self.registry.put(candidate)
        record = self._local.activity
        if any(o["role"] == role and o["artifact"]["id"] == ref.id for o in record["outputs"]):
            return ref
        outputs = {o["artifact"]["id"] for o in record["outputs"]}
        inputs = {u["artifact"]["id"] for u in record["uses"]}
        ancestors = (
            list((set(record["ancestors"]) - outputs) | inputs)
            if parents is None
            else [self.use(p, role="parent").id for p in parents]
        )
        if existing:
            ancestors = []
        origin = {"flow_id": self.flow_id, "run_id": self.run_id, "step": record["step"], "key": record["key"]}
        record["outputs"].append(
            {
                "role": role,
                "artifact": ref.snapshot(),
                "parents": sorted(set(ancestors)),
                "origin": origin,
                "event": "referenced" if existing else "generated",
            }
        )
        self.inherit([ref.id])
        return ref

    def reused(self, ref: Artifact, *, source: str, size: int) -> None:
        """Record why a producer body did not execute, retaining its original producer."""
        self.use(ref, role="reused")
        self._local.activity["reuse"] = {"identity": ref.id, "source": source, "bytes": size}

    def snapshot(self, *, key: str | int | None = None, narrow: bool = False) -> dict:
        record = copy.deepcopy(self._local.activity)
        if key is not None:
            record["key"] = key
            for output in record["outputs"]:
                output["origin"]["key"] = key
        if narrow:
            output = next(o for o in record["outputs"] if o["role"] == "output" and o["artifact"]["id"] == key)
            aid = output["artifact"]["id"]
            parents = output["parents"] if output["event"] == "generated" else []
            record["ancestors"] = sorted({aid, *parents})
            record["uses"] = [u for u in record["uses"] if u["artifact"]["id"] in record["ancestors"]]
            record["outputs"] = [output]
        return record


def reused_activity(record: dict) -> dict:
    """A new run references the old producer; it does not claim a fresh generation."""
    out = copy.deepcopy(record)
    for output in out["outputs"]:
        if output["event"] != "referenced":
            output["event"] = "reused"
    out["reused"] = True
    return out


def bindings(registry: Artifacts, snapshots: dict, declarations: dict) -> dict:
    """Reconstruct typed references without rereading mutable metadata."""
    out = {}
    for name, hint in declarations.items():
        many = typing.get_origin(hint) is list
        schema = typing.get_args(hint)[0] if many else hint
        values = snapshots[name] if many else [snapshots[name]]
        refs = [registry.get(r["id"], schema, snapshot=r) for r in values]
        out[name] = refs if many else refs[0]
    return out
