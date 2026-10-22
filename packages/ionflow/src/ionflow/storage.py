"""Local-first inventories and explicit payload transfers (0026, 0027)."""

from __future__ import annotations

import datetime as dt
import graphlib
import hashlib
import json
import logging
import tempfile
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath

from .artifact import Artifacts, MetadataConflict, MissingPayload, content_token, metadata_token
from .config import Config
from .lake import Catalog
from .space import utc_now
from .store import COPY_WORKERS, Store, parse_dest, payload_lock

logger = logging.getLogger(__name__)


def remote_config(config: Config, remote: str | None = None) -> Config:
    """Resolve an explicit peer without changing the local execution store.

    >>> remote_config(Config(local_dir="/tmp/work", remote="s3://bucket/shared")).prefix
    'shared'
    """
    url = remote or config.remote
    if not url:
        raise ValueError("no remote configured")
    if url.startswith("s3://"):
        bucket, prefix = parse_dest(url)
        return Config(
            bucket=bucket,
            prefix=prefix or config.prefix,
            endpoint_url=config.endpoint_url,
            region=config.region,
            access_key=config.access_key,
            secret_key=config.secret_key,
        )
    path = Path(url).expanduser()
    if not path.is_absolute():
        raise ValueError("remote must be s3://bucket/prefix or an absolute directory")
    return Config(local_dir=str(path.resolve()), prefix=config.prefix)


def _safe_path(path: str) -> str:
    """Validate a portable store-relative path.

    >>> _safe_path("run_id=one/step/part_0.parquet")
    'run_id=one/step/part_0.parquet'
    """
    p = PurePosixPath(path)
    if not p.parts or p.is_absolute() or ".." in p.parts or str(p) != path or "\\" in path:
        raise ValueError(f"unsafe stored path: {path!r}")
    return path


def _artifact_ids(record: dict) -> set[str]:
    ids = set()
    for value in record["artifacts"].values():
        ids.update(r["id"] for r in (value if isinstance(value, list) else [value]))
    for activity in record["activities"]:
        ids.update(activity["ancestors"])
        ids.update(r["artifact"]["id"] for r in [*activity["uses"], *activity["outputs"]])
    return ids


def _closure(registry: Artifacts, ids: set[str]) -> list[str]:
    records = {r["id"] for r in registry.records()}
    parents: dict[str, set[str]] = {}
    for parent, child, _, _ in registry.links():
        parents.setdefault(child, set()).add(parent)
    for activity in registry.activities():
        for output in activity["outputs"]:
            parents.setdefault(output["artifact"]["id"], set()).update(output["parents"])
    wanted = set(ids)
    while extra := {p for aid in wanted for p in parents.get(aid, ())} - wanted:
        wanted.update(extra)
    if missing := wanted - records:
        raise MissingPayload(f"artifact metadata unavailable: {', '.join(sorted(missing))}")
    return sorted(wanted)


def _selection(
    source: Store,
    flow_id: str | None,
    run_id: str | None,
    artifact_ids: list[str] | None,
    *,
    require_present: bool,
) -> tuple[Artifacts, dict[str, dict], list[str]]:
    """Resolve table reuse and declared child calls from metadata at one endpoint."""
    registry = Artifacts(source)
    groups, inventories, catalogs = {}, {}, {}
    ids = set(artifact_ids or [])
    if run_id is not None and flow_id is None:
        raise ValueError("run_id requires flow_id")
    if flow_id is None:
        return registry, groups, _closure(registry, ids)

    def inventory(identifier: str) -> dict:
        if identifier not in inventories:
            catalogs[identifier] = Catalog(source.space(identifier))
            inventories[identifier] = {r["run_id"]: r for r in catalogs[identifier].inventory()}
        return inventories[identifier]

    children = {}
    for identifier in source.flow_ids():
        for record in inventory(identifier).values():
            if parent := record.get("parent"):
                children.setdefault((parent["flow_id"], parent["run_id"]), []).append((identifier, record))
    pending = [(flow_id, rid) for rid in (inventory(flow_id) if run_id is None else [run_id])]
    selected = {}
    while pending:
        identifier, rid = pending.pop()
        records = inventory(identifier)
        if rid in selected.get(identifier, {}):
            continue
        if rid not in records:
            raise ValueError(f"run not found: {rid}")
        record = records[rid]
        if require_present and not record["present"] and record["files"]:
            raise MissingPayload(f"run {rid}: payload unavailable at source; upload it from its originating store")
        selected.setdefault(identifier, {})[rid] = record
        if record.get("origin_run_id"):
            pending.append((identifier, record["origin_run_id"]))
        ids.update(_artifact_ids(record))
        for _, _, path in record["manifest"]:
            _safe_path(path)
            if ":" in path and "/" not in path:
                ids.add(path)
            elif path.startswith("run_id="):
                pending.append((identifier, path.split("/", 1)[0].removeprefix("run_id=")))
            else:
                raise ValueError(f"unexpected run part path: {path}")
        # The nearest parent attempt wins per call site. A resumed parent can
        # retain a child completed before its own output was persisted.
        calls, ancestors, ancestor = {}, set(), rid
        while ancestor and ancestor not in ancestors:
            ancestors.add(ancestor)
            for child_flow, child in sorted(
                children.get((identifier, ancestor), []), key=lambda item: item[1]["run_id"], reverse=True
            ):
                link = child["parent"]
                calls.setdefault((link["step"], json.dumps(link["key"])), (child_flow, child["run_id"]))
            ancestor = records.get(ancestor, {}).get("origin_run_id")
        pending.extend(calls.values())
    for identifier, records in selected.items():
        dependencies = {
            rid: {
                p.split("/", 1)[0].removeprefix("run_id=")
                for _, _, p in records[rid]["manifest"]
                if p.startswith("run_id=") and not p.startswith(f"run_id={rid}/")
            }
            for rid in sorted(records)
        }
        groups[identifier] = {
            "runs": [records[rid] for rid in graphlib.TopologicalSorter(dependencies).static_order()],
            "tables": catalogs[identifier].tables(),
        }
    return registry, groups, _closure(registry, ids)


@dataclass
class TransferPlan:
    """A pinned directional selection. ``summary`` is safe to expose to interfaces."""

    source: Store
    target: Store
    flow_id: str | None
    runs: list[dict]
    artifacts: list[str]
    files: list[dict]
    tables: dict = field(default_factory=dict)
    artifact_records: list[dict] = field(default_factory=list)
    links: list[list] = field(default_factory=list)
    activities: list[dict] = field(default_factory=list)
    run_groups: dict[str, dict] = field(default_factory=dict)
    payload_records: list[dict] = field(default_factory=list)

    def _groups(self) -> dict[str, dict]:
        """Child catalogs first, then the selected parent catalog."""
        return self.run_groups | ({self.flow_id: {"runs": self.runs, "tables": self.tables}} if self.flow_id else {})

    def summary(self) -> dict:
        """Describe exactly which missing payload files need approval."""
        body = {
            "source": self.source.uri(self.source.prefix),
            "target": self.target.uri(self.target.prefix),
            "flow_id": self.flow_id,
            "runs": [r["run_id"] for r in self.runs],
            "artifacts": self.artifacts,
            "files": len(self.files),
            "bytes": sum(f["size"] for f in self.files),
        }
        if self.run_groups:
            body["children"] = [
                {"flow_id": identifier, "runs": [r["run_id"] for r in group["runs"]]}
                for identifier, group in sorted(self.run_groups.items())
            ]
        token = hashlib.sha256(
            json.dumps(
                [
                    body,
                    self.files,
                    {identifier: [r["version"] for r in group["runs"]] for identifier, group in self._groups().items()},
                    [[metadata_token(r), content_token(r)] for r in self.artifact_records + self.payload_records],
                ],
                sort_keys=True,
            ).encode()
        ).hexdigest()
        return body | {"token": token}

    def details(self) -> dict:
        """Expose member names and artifact choices without exposing credentials."""
        names: dict[str, list[str]] = {}
        missing = {f["target"]: f["size"] for f in self.files}
        payloads = self.payload_records + [r for r in self.artifact_records if r["id"] not in self.artifacts]
        for record in payloads:
            for member in record["content"]["files"]:
                key = f"{self.target.prefix}/artifacts/_blobs/sha256/{member['sha256']}"
                names.setdefault(key, []).append(f"{record['name']}/{member['path']}")
                if key not in missing:
                    info = self.target.stat(key)
                    missing[key] = member["size"] if info is None or info["size"] != member["size"] else 0
        return self.summary() | {
            "file_details": [f | {"names": sorted(set(names.get(f["target"], [f["target"]])))} for f in self.files],
            "inputs": [
                {
                    "id": r["id"],
                    "kind": r["kind"],
                    "name": r["name"],
                    "chosen": r["id"] in self.artifacts,
                    "bytes": sum(
                        missing.get(k, 0)
                        for k in {
                            f"{self.target.prefix}/artifacts/_blobs/sha256/{m['sha256']}"
                            for p in payloads
                            if p["id"] == r["id"]
                            for m in p["content"]["files"]
                        }
                    ),
                }
                for r in self.artifact_records
            ],
        }


def _choose_artifacts(
    records: dict[str, dict],
    ids: list[str],
    included: list[str] | None,
    direction: str | None,
) -> list[str]:
    """Use type defaults unless the caller supplies an exact checkbox selection."""
    if direction not in (None, "up", "down"):
        raise ValueError("direction must be up or down")
    chosen = (
        [aid for aid in ids if direction is None or records[aid]["sync"][direction]]
        if included is None
        else sorted(set(included))
    )
    if unknown := set(chosen) - set(ids):
        raise ValueError(f"not a referenced artifact: {', '.join(sorted(unknown))}")
    return chosen


def _payload_records(groups: dict, records: dict[str, dict], ids: list[str]) -> list[dict]:
    """Prefer the concrete manifests pinned by selected runs over current inventory."""
    pinned: dict[str, dict[str, dict]] = {}
    for group in groups.values():
        for run in group["runs"]:
            snapshots = []
            for bound in run["artifacts"].values():
                snapshots.extend(bound if isinstance(bound, list) else [bound])
            for activity in run["activities"]:
                snapshots.extend(v["artifact"] for v in [*activity["uses"], *activity["outputs"]])
            for snapshot in snapshots:
                pinned.setdefault(snapshot["id"], {})[content_token(snapshot)] = snapshot
    return [record for aid in ids for record in (list(pinned[aid].values()) if aid in pinned else [records[aid]])]


def plan_transfer(
    source: Store,
    target: Store,
    flow_id: str | None = None,
    run_id: str | None = None,
    artifact_ids: list[str] | None = None,
    *,
    include_artifacts: list[str] | None = None,
    direction: str | None = None,
) -> TransferPlan:
    """Plan a reference closure; optionally select artifact payloads within it.

    Unselected artifacts retain their metadata and lineage without requiring
    their payloads at either endpoint.
    """
    registry, groups, ids = _selection(source, flow_id, run_id, artifact_ids, require_present=True)
    if direction is None and bool(source.local_dir) != bool(target.local_dir):
        direction = "up" if source.local_dir else "down"
    source_records = {r["id"]: r for r in registry.records()}
    payload_ids = _choose_artifacts(source_records, ids, include_artifacts, direction)
    payload_records = _payload_records(groups, source_records, payload_ids)
    files = {}

    def add(relative: str, size: int, digest: str | None) -> None:
        _safe_path(relative)
        source_key, target_key = f"{source.prefix}/{relative}", f"{target.prefix}/{relative}"
        info = target.stat(target_key)
        if info is None or info["size"] != size:
            files[target_key] = {"source": source_key, "target": target_key, "size": size, "sha256": digest}

    for record in payload_records:
        for member in record["content"]["files"]:
            key = f"artifacts/_blobs/sha256/{member['sha256']}"
            info = source.stat(f"{source.prefix}/{key}")
            if info is None or info["size"] != member["size"]:
                raise MissingPayload(f"{record['id']}: payload unavailable at source")
            add(key, member["size"], member["sha256"])
    for identifier, group in groups.items():
        for record in group["runs"]:
            for f in record["files"]:
                add(f"runs/{identifier}/{f['path']}", f["size"], f["sha256"])
            # Telemetry is optional data, transferred only after explicit approval.
            space = source.space(identifier)
            for key in source.list(space.run_dir(record["run_id"]) + "/"):
                if PurePosixPath(key).name.startswith(("_metrics", "_phases")):
                    info = source.stat(key)
                    if info is not None:
                        add(key.removeprefix(source.prefix + "/"), info["size"], None)
    selected = set(ids)
    root = groups.pop(flow_id, {"runs": [], "tables": {}})
    return TransferPlan(
        source,
        target,
        flow_id,
        root["runs"],
        payload_ids,
        list(files.values()),
        root["tables"],
        [source_records[aid] for aid in ids],
        [link for link in registry.links() if link[0] in selected and link[1] in selected],
        [a for a in registry.activities() if any(o["artifact"]["id"] in selected for o in a["outputs"])],
        groups,
        payload_records,
    )


def execute_transfer(plan: TransferPlan, progress: Callable[[dict], None] | None = None) -> dict:
    """Copy approved files, verify them, then expose receiving registrations."""
    with payload_lock(plan.source, plan.target):
        return _execute_transfer(plan, progress)


def _execute_transfer(plan: TransferPlan, progress: Callable[[dict], None] | None) -> dict:
    source, target = plan.source, plan.target
    for identifier, group in plan._groups().items():
        current = {r["run_id"]: r for r in Catalog(source.space(identifier)).inventory()}
        if any(current.get(r["run_id"], {}).get("version") != r["version"] for r in group["runs"]):
            raise ValueError("source changed; review a new transfer plan")

    def copy(f: dict) -> dict:
        with tempfile.TemporaryDirectory(prefix="ionflow-transfer-") as tmp:
            path = Path(tmp) / "payload"
            source.download(f["source"], str(path))
            with path.open("rb") as stream:
                digest = hashlib.file_digest(stream, "sha256").hexdigest()
            if path.stat().st_size != f["size"] or (f["sha256"] and digest != f["sha256"]):
                raise ValueError(f"payload checksum mismatch: {f['source']}")
            target.upload(f["target"], str(path))
        return f

    done = 0
    total = plan.summary()["bytes"]
    with ThreadPoolExecutor(max_workers=COPY_WORKERS) as pool:
        for future in as_completed([pool.submit(copy, f) for f in plan.files]):
            f = future.result()
            done += f["size"]
            if progress:
                progress({"file": f["target"], "bytes": done, "total": total})
    if plan.artifact_records:
        registry = Artifacts(target)
        conflicts = registry.merge_metadata(plan.artifact_records)
        if conflicts:
            raise MetadataConflict("artifact metadata conflicts: " + ", ".join(conflicts))
        conflicts = registry.merge_metadata(plan.payload_records, received=True)
        if conflicts:
            raise MetadataConflict("artifact metadata conflicts: " + ", ".join(conflicts))
        for aid in plan.artifacts:
            registry.mark_present(aid)
        for p, c, relation, origin in plan.links:
            if [p, c, relation, origin] not in registry.links():
                registry.link(p, c, relation=relation, source=origin)
        metadata_ids = {record["id"] for record in plan.artifact_records}
        for activity in plan.activities:
            outputs = [o for o in activity["outputs"] if o["artifact"]["id"] in metadata_ids]
            if not outputs:
                continue
            parents = {p for o in outputs for p in o["parents"]}
            registry._import_activity(
                activity
                | {
                    "outputs": outputs,
                    "uses": [u for u in activity["uses"] if u["artifact"]["id"] in parents],
                    "ancestors": sorted(parents | {o["artifact"]["id"] for o in outputs}),
                }
            )
    for identifier, group in plan._groups().items():
        cat = Catalog(target.space(identifier))
        cat.merge_metadata(group["runs"], group["tables"])
        for record in group["runs"]:
            cat.mark_present(record)
            from .state import Manifest

            meta = record | {
                "manifest": Manifest.from_rows(
                    [
                        (s, k, target.space(identifier).root + "/" + p if p.startswith("run_id=") else p)
                        for s, k, p in record["manifest"]
                    ]
                ).meta(),
                "activities": {json.dumps([a["step"], a["key"]]): a for a in record["activities"]},
            }
            target.space(identifier).write_meta(record["run_id"], meta)
    return plan.summary() | {"copied": len(plan.files), "transferred_bytes": done}


class Storage:
    """One local workspace and optional peer; browsing transfers metadata only."""

    def __init__(self, config: Config, remote: Store | None = None) -> None:
        if not config.local_dir:
            raise ValueError("a workspace requires local_dir; S3 is an optional remote")
        self.config = config
        self.local = Store.of(config)
        self._remote = remote
        self._remote_config = remote_config(config) if config.remote else None
        self.peer = (
            remote.identity if remote else (Store.identity_for(self._remote_config) if self._remote_config else None)
        )
        if self.peer == self.local.identity:
            raise ValueError("local and remote must be different stores")
        self.state_key = f"{config.prefix}/_peers/{self.peer or 'local'}/refresh.json"

    @property
    def remote(self) -> Store | None:
        """Construct network clients only for explicit refreshes or transfers."""
        if self._remote is None and self._remote_config is not None:
            self._remote = Store.of(self._remote_config)
        return self._remote

    def status(self) -> dict:
        """Last refresh time and errors, available without contacting the remote."""
        try:
            state = json.loads(self.local.get(self.state_key) or "{}")
            if not isinstance(state, dict):
                return {}
            if state.get("checked") is not None:
                dt.datetime.fromisoformat(state["checked"])
            return state
        except (ValueError, TypeError):
            return {}

    def refresh(self) -> dict:
        """Reconcile metadata both ways; retain the last inventory on network failures."""
        if self.peer is None:
            return {"checked": None, "error": None, "conflicts": []}
        state = self.status()
        conflicts = []
        try:
            assert self.remote is not None
            flows = sorted(set(self.local.flow_ids()) | set(self.remote.flow_ids()))
            keys = ["artifacts/catalog.ducklake", *[f"runs/{flow_id}/catalog.ducklake" for flow_id in flows]]
            versions = {
                f"{store.identity}:{key}": hashlib.sha256(store.get(f"{store.prefix}/{key}") or b"").hexdigest()
                for store in (self.local, self.remote)
                for key in keys
            }
            if state.get("versions") == versions and not state.get("conflicts"):
                state = state | {"checked": utc_now().isoformat(), "error": None}
                self.local.put(self.state_key, json.dumps(state).encode())
                return state
            a, b = Artifacts(self.local), Artifacts(self.remote)
            mine, theirs = a.records(), b.records()
            ap, bp = a.availability(), b.availability()
            conflicts.extend(a.merge_metadata(theirs, self.peer, bp))
            conflicts.extend(b.merge_metadata(mine, self.local.identity, ap))
            links_a, links_b = a.links(), b.links()
            for registry, incoming, existing in ((a, links_b, links_a), (b, links_a, links_b)):
                for p, c, relation, source in incoming:
                    if [p, c, relation, source] not in existing:
                        registry.link(p, c, relation=relation, source=source)
            pa, pb = a.provenance(), b.provenance()
            for registry, incoming, existing in ((a, pb, pa), (b, pa, pb)):
                known = {registry._activity_id(r): r for r in existing}
                for record in incoming:
                    if known.get(registry._activity_id(record)) != record:
                        registry._import_activity(record)
            for flow_id in flows:
                left, right = Catalog(self.local.space(flow_id)), Catalog(self.remote.space(flow_id))
                local_rows, remote_rows = left.inventory(), right.inventory()
                if remote_rows:
                    left.merge_metadata(remote_rows, right.tables(), self.peer)
                if local_rows:
                    right.merge_metadata(local_rows, left.tables(), self.local.identity)
            state = {
                "checked": utc_now().isoformat(),
                "error": None,
                "conflicts": sorted(set(conflicts)),
                "versions": versions,
            }
        except Exception as error:  # noqa: BLE001 — remote availability never determines run success
            state = state | {"error": str(error), "conflicts": sorted(set(conflicts))}
        self.local.put(self.state_key, json.dumps(state).encode())
        return state

    def runs(self, flow_id: str) -> list[dict]:
        """Known ledger rows with workstation-relative payload availability."""
        cat = Catalog(self.local.space(flow_id))
        presence = cat.availability(self.peer)
        return [r | presence[r["run_id"]] for r in cat.runs()]

    def artifacts(self, kind: str | None = None) -> list[dict]:
        """Known artifacts, including metadata-only records."""
        registry = Artifacts(self.local)
        presence = registry.availability(self.peer)
        return [r | presence[r["id"]] for r in registry.records(kind)]

    def resolve_artifact(self, artifact_id: str, *, prefer: str, local_revision: int, remote_revision: int) -> dict:
        """Explicitly resolve divergent edits; preserve both revision ancestries."""
        if self.remote is None:
            raise ValueError("no remote configured")
        if prefer not in ("local", "s3"):
            raise ValueError("prefer must be local or s3")
        left, right = Artifacts(self.local), Artifacts(self.remote)
        mine, theirs = left.get(artifact_id).snapshot(), right.get(artifact_id).snapshot()
        if (mine["revision"], theirs["revision"]) != (local_revision, remote_revision):
            raise MetadataConflict("metadata changed; review both revisions again")
        chosen = mine if prefer == "local" else theirs
        record = chosen | {
            "revision": max(local_revision, remote_revision) + 1,
            "history": sorted(
                set(
                    mine.get("history", []) + theirs.get("history", []) + [metadata_token(mine), metadata_token(theirs)]
                )
            ),
        }
        for registry in (left, right):
            if registry.merge_metadata([record]):
                raise MetadataConflict("metadata changed during resolution; refresh and review again")
        self.refresh()
        return left.get(artifact_id).snapshot()

    def plan(
        self,
        direction: str,
        flow_id: str | None = None,
        run_id: str | None = None,
        artifact_ids: list[str] | None = None,
        *,
        include_artifacts: list[str] | None = None,
    ) -> TransferPlan:
        """Plan an upload or download; neither direction is implicit."""
        if self.remote is None:
            raise ValueError("no remote configured")
        if direction not in ("up", "down"):
            raise ValueError("direction must be up or down")
        source, target = (self.local, self.remote) if direction == "up" else (self.remote, self.local)
        return plan_transfer(
            source, target, flow_id, run_id, artifact_ids, include_artifacts=include_artifacts, direction=direction
        )

    def preview(
        self,
        direction: str,
        flow_id: str | None = None,
        run_id: str | None = None,
        artifact_ids: list[str] | None = None,
        *,
        include_artifacts: list[str] | None = None,
    ) -> dict:
        """Estimate a transfer from local metadata, without remote access or an approval token."""
        if direction not in ("up", "down"):
            raise ValueError("direction must be up or down")
        if self.peer is None:
            raise ValueError("no remote configured")
        registry, groups, ids = _selection(self.local, flow_id, run_id, artifact_ids, require_present=False)
        records = {r["id"]: r for r in registry.records()}
        chosen = _choose_artifacts(records, ids, include_artifacts, direction)
        payloads = _payload_records(groups, records, ids)
        if self._remote is not None:
            peer = self._remote.uri(self._remote.prefix)
        else:
            cfg = self._remote_config
            assert cfg is not None
            peer = (
                f"{Path(cfg.local_dir).expanduser().resolve()}/{cfg.prefix}"
                if cfg.local_dir
                else f"s3://{cfg.bucket}/{cfg.prefix}"
            )
        local = self.local.uri(self.local.prefix)
        source, target = (local, peer) if direction == "up" else (peer, local)
        files: dict[str, dict] = {}

        def is_missing(path: str, size: int) -> bool:
            if direction == "down":
                info = self.local.stat(f"{self.local.prefix}/{path}")
                if info is not None and info["size"] == size:
                    return False
            return True

        def add(path: str, size: int, digest: str | None, name: str) -> None:
            _safe_path(path)
            if not is_missing(path, size):
                return
            entry = files.setdefault(
                path,
                {
                    "source": source + "/" + path,
                    "target": target + "/" + path,
                    "size": size,
                    "sha256": digest,
                    "names": [],
                },
            )
            if name not in entry["names"]:
                entry["names"].append(name)

        for r in payloads:
            if r["id"] not in chosen:
                continue
            for member in r["content"]["files"]:
                add(
                    f"artifacts/_blobs/sha256/{member['sha256']}",
                    member["size"],
                    member["sha256"],
                    f"{r['name']}/{member['path']}",
                )
        for identifier, group in groups.items():
            for run in group["runs"]:
                for member in run["files"]:
                    path = f"runs/{identifier}/{member['path']}"
                    add(path, member["size"], member["sha256"], path)
        return {
            "preview": True,
            "source": source,
            "target": target,
            "files": len(files),
            "bytes": sum(f["size"] for f in files.values()),
            "file_details": list(files.values()),
            "inputs": [
                {
                    "id": aid,
                    "kind": records[aid]["kind"],
                    "name": records[aid]["name"],
                    "chosen": aid in chosen,
                    "bytes": sum(
                        size
                        for digest, size in {
                            m["sha256"]: m["size"] for p in payloads if p["id"] == aid for m in p["content"]["files"]
                        }.items()
                        if is_missing(f"artifacts/_blobs/sha256/{digest}", size)
                    ),
                }
                for aid in ids
            ],
        }

    def remove_local(
        self,
        *,
        artifact_id: str | None = None,
        flow_id: str | None = None,
        run_id: str | None = None,
    ) -> None:
        """Remove one local payload, preserving metadata, remote copies, and shared data."""
        from .eviction import remove_local

        remove_local(self.local, artifact_id=artifact_id, flow_id=flow_id, run_id=run_id)

    def transfer(self, plan: TransferPlan, progress: Callable[[dict], None] | None = None) -> dict:
        """Execute an approved plan and refresh the metadata indicators."""
        if {plan.source.identity, plan.target.identity} != {self.local.identity, self.peer}:
            raise ValueError("transfer plan belongs to another workspace or remote")
        result = execute_transfer(plan, progress)
        self.refresh()
        return result


_refresh_lock = threading.Lock()
_refreshing: dict[tuple[str, str], bool] = {}


def request_refresh(config: Config) -> None:
    """Coalesce background refreshes; local execution never waits for the network."""
    if not config.remote or not config.local_dir:
        return
    key = (config.local_dir, config.remote)
    with _refresh_lock:
        if key in _refreshing:
            _refreshing[key] = True
            return
        _refreshing[key] = False

    def refresh() -> None:
        while True:
            try:
                Storage(config).refresh()
            except Exception as error:  # noqa: BLE001 — background discovery never determines run success
                logger.warning("metadata refresh failed: %s", error)
            with _refresh_lock:
                if not _refreshing[key]:
                    del _refreshing[key]
                    return
                _refreshing[key] = False

    threading.Thread(target=refresh, name="ionflow-metadata", daemon=True).start()
