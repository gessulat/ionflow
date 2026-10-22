"""Local canonical payload removal; metadata and peer stores are retained."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .artifact import Artifacts
from .lake import Catalog
from .schema import _q
from .storage import _safe_path
from .store import Store, payload_lock


def _paths(store: Store, keys: list[str]) -> list[Path]:
    root = Path(store.local_dir).expanduser().resolve()
    paths = []
    for key in keys:
        _safe_path(key)
        path = Path(store.uri(key))
        if not path.resolve().is_relative_to(root):
            raise ValueError("payload path escapes the local store")
        paths.append(path)
    return paths


def remove_local(
    store: Store,
    *,
    artifact_id: str | None = None,
    flow_id: str | None = None,
    run_id: str | None = None,
) -> None:
    """Remove one payload after checking active runs and references to shared data."""
    if not store.local_dir:
        raise ValueError("payload removal requires a local store")
    with payload_lock(store, exclusive=True):
        _remove_local(store, artifact_id=artifact_id, flow_id=flow_id, run_id=run_id)


def _remove_local(store: Store, *, artifact_id: str | None, flow_id: str | None, run_id: str | None) -> None:
    artifact = artifact_id is not None and flow_id is None and run_id is None
    run = artifact_id is None and flow_id is not None and run_id is not None
    if not (artifact or run):
        raise ValueError("select one artifact_id or one flow_id and run_id")
    for identifier in store.flow_ids():
        if any(r["present"] and r["status"] == "running" for r in Catalog(store.space(identifier)).inventory()):
            raise ValueError("cannot remove local payloads while a local run is running")
    if artifact:
        registry = Artifacts(store)
        record = registry.get(artifact_id).snapshot()
        present = registry.availability()
        shared = {
            m["sha256"]
            for other in registry.records()
            if other["id"] != artifact_id and present.get(other["id"], {}).get("local")
            for payload in registry.payload_records(other["id"])
            for m in payload["content"]["files"]
        }
        paths = _paths(
            store,
            [
                f"{registry.root}/_blobs/sha256/{m['sha256']}"
                for payload in registry.payload_records(record["id"])
                for m in payload["content"]["files"]
                if m["sha256"] not in shared
            ],
        )

        def mark_absent(con: Any) -> None:
            con.execute("UPDATE lake.artifact_presence SET present = false WHERE id = ?", [artifact_id])

        registry.lake.commit(mark_absent)
        registry.evict(artifact_id)
    else:
        assert flow_id is not None and run_id is not None
        _safe_path(f"runs/{flow_id}/run_id={run_id}")
        catalog = Catalog(store.space(flow_id))
        record = next((r for r in catalog.inventory() if r["run_id"] == run_id), None)
        if record is None:
            raise ValueError(f"unknown run: {run_id}")
        prefix = catalog.space.run_dir(run_id) + "/"
        paths = _paths(store, store.list(prefix))

        def unregister(con: Any) -> None:
            if con.execute(
                "SELECT 1 FROM lake.run_versions v JOIN lake.runs r USING (run_id) "
                "WHERE v.present AND r.status = 'running' LIMIT 1"
            ).fetchone():
                raise ValueError("cannot remove local payloads while a local run is running")
            dependents = con.execute(
                "SELECT DISTINCT m.run_id FROM lake.manifest m JOIN lake.run_versions v USING (run_id) "
                "WHERE v.present AND m.run_id <> ? AND starts_with(m.path, ?)",
                [run_id, prefix],
            ).fetchall()
            if dependents:
                raise ValueError("other local runs reuse these results: " + ", ".join(r[0] for r in dependents))
            parts = con.execute(
                "SELECT DISTINCT m.step, m.path FROM lake.manifest m JOIN lake.registered_parts p ON p.path=m.path"
            ).fetchall()
            for step in {name for name, path in parts if path.startswith(prefix)}:
                quoted = _q(step)
                columns = ", ".join(
                    f"{_q(c[0])} {c[1]}" + (" NOT NULL" if c[2] == "NO" else "")
                    for c in con.execute(f"DESCRIBE lake.{quoted}").fetchall()
                )
                con.execute(f"DROP TABLE lake.{quoted}")
                con.execute(f"CREATE TABLE lake.{quoted} ({columns})")
                for name, path in parts:
                    if name == step and not path.startswith(prefix):
                        con.execute(
                            "CALL ducklake_add_data_files('lake', ?, ?, hive_partitioning => true)",
                            [step, store.uri(path)],
                        )
            con.execute("DELETE FROM lake.registered_parts WHERE starts_with(path, ?)", [prefix])
            con.execute("UPDATE lake.run_versions SET present = false WHERE run_id = ?", [run_id])

        catalog.lake.commit(unregister)
        catalog.export_metadata(record)
    for path in paths:
        path.unlink(missing_ok=True)
