"""Generic local browsing over flow declarations and ionflow metadata."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, get_args, get_origin

from .artifact import Artifact, Artifacts, MissingPayload, _spec
from .config import Config
from .flow import Flow
from .query import phase_stats, step_stats
from .schema import Table, _q, _strip_optional
from .storage import Storage
from .views import View


@dataclass(frozen=True)
class Filter:
    """A parameterized table filter; column names must belong to the declared schema.

    >>> Filter("score", "le", 0.01).operator
    'le'
    """

    column: str
    operator: str
    value: Any


def _description(schema: type, fields: dict[str, Any]) -> dict:
    view = getattr(schema, "View", View())
    if not isinstance(view, View):
        raise TypeError(f"{schema.__name__}.View must be a View instance")
    return view.describe({name: _strip_optional(hint)[0] for name, hint in fields.items()}) | {
        "label": view.label or schema.__name__,
    }


def _rows(cursor: Any) -> list[dict]:
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


class Browser:
    """Browse explicitly registered flows and artifact kinds without remote clients."""

    def __init__(
        self, config: Config, *, flows: Sequence[type[Flow]] = (), artifacts: Sequence[type[Artifact]] = ()
    ) -> None:
        self.storage = Storage(config)
        self.flows: dict[str, type[Flow]] = {}
        pending = list(flows)
        while pending:
            flow = pending.pop(0)
            if flow.flow_id() not in self.flows:
                self.flows[flow.flow_id()] = flow
                pending.extend(step.subflow.flow for step in flow._dag.steps.values() if step.subflow)
        self.kinds = {artifact._kind: artifact for artifact in artifacts}

    def describe(self) -> dict:
        """Describe navigation, schemas, references, and the declared execution DAG."""
        kinds = []
        for kind, schema in self.kinds.items():
            fields = {
                "id": str,
                "kind": str,
                "name": str,
                "created": str,
                "revision": int,
                "size": int,
                "local": bool,
                "s3": bool,
                **schema._fields,
            }
            kinds.append(
                {
                    "kind": kind,
                    "payload": schema._payload,
                    "sync": dict(schema._sync),
                    "identity": list(schema._identity),
                    "fields": {name: _spec(hint) for name, hint in fields.items()},
                    "view": _description(schema, fields),
                }
            )
        flows = []
        for identifier, flow in self.flows.items():
            fields = {
                "run_id": str,
                "status": str,
                "started": str,
                "finished": str,
                "commit": str,
                "origin_run_id": str,
                "reuse": str,
                "local": bool,
                "s3": bool,
                **{name: col.dtype for name, col in (flow.params_columns() | flow.ledger_columns()).items()},
            }
            steps = []
            for name, step in flow._dag.steps.items():
                output = step.output_table
                if issubclass(output, Artifact):
                    descriptor = {"kind": output._kind}
                else:
                    assert issubclass(output, Table)
                    columns = output.columns()
                    descriptor = {
                        "columns": [
                            {"name": n, "type": c.persisted_type(), "nullable": c.nullable} for n, c in columns.items()
                        ],
                        "view": _description(output, {n: c.dtype for n, c in columns.items()}),
                    }
                steps.append(
                    {
                        "name": name,
                        "dependencies": step.deps,
                        "fanout": step.is_fanout,
                        "branch": flow._dag.roots.get(name),
                        "output": descriptor,
                        **(
                            {"subflow": {"flow_id": step.subflow.flow.flow_id(), "output": step.subflow.output}}
                            if step.subflow
                            else {}
                        ),
                    }
                )
            inputs = flow.Inputs.declarations() if flow.Inputs else {}
            flows.append(
                {
                    "id": identifier,
                    "view": _description(flow, fields),
                    "fields": list(fields),
                    "inputs": {
                        n: {
                            "kind": (get_args(t)[0] if get_origin(t) is list else t)._kind,
                            "many": get_origin(t) is list,
                        }
                        for n, t in inputs.items()
                    },
                    "steps": steps,
                }
            )
        return {"artifacts": kinds, "flows": flows}

    def runs(self, flow_id: str) -> list[dict]:
        """Return ledger values and payload presence without opening step outputs."""
        self._flow(flow_id)
        rows = self.storage.runs(flow_id)
        return sorted(
            [row.get("params", {}) | row.get("custom", {}) | row for row in rows],
            key=lambda row: row["run_id"],
            reverse=True,
        )

    def children(self, flow_id: str, run_id: str) -> list[dict]:
        """Return child attempts, including children retained through parent resume."""
        flow = self._flow(flow_id)
        runs = {row["run_id"]: row for row in self.storage.runs(flow_id)}
        if run_id not in runs:
            raise ValueError(f"unknown run: {run_id}")
        origins, ancestor = set(), run_id
        while ancestor and ancestor not in origins:
            origins.add(ancestor)
            ancestor = runs.get(ancestor, {}).get("origin_run_id")
        children = {step.subflow.flow.flow_id() for step in flow._dag.steps.values() if step.subflow}
        return sorted(
            [
                row | {"flow_id": child, "reused": row["parent"]["run_id"] != run_id}
                for child in children
                for row in self.storage.runs(child)
                if (link := row.get("parent")) and link["flow_id"] == flow_id and link["run_id"] in origins
            ],
            key=lambda row: (row["parent"]["step"], str(row["parent"]["key"]), row["run_id"]),
        )

    def artifacts(self, kind: str) -> list[dict]:
        """Return typed metadata and standard inventory fields for one artifact kind."""
        if kind not in self.kinds:
            raise ValueError(f"unknown artifact kind: {kind}")
        return [
            row["metadata"] | row | {"size": sum(f["size"] for f in row["content"]["files"])}
            for row in self.storage.artifacts(kind)
        ]

    def lineage(self, artifact_id: str) -> dict:
        """Return recorded producers, consumers, and parents without payload reads."""
        registry = Artifacts(self.storage.local)
        record = registry.get(artifact_id).snapshot()
        if record["kind"] not in self.kinds:
            raise ValueError(f"unknown artifact kind: {record['kind']}")
        return {
            "parents": registry.parents(artifact_id),
            "producers": registry.producers(artifact_id),
            "consumers": registry.consumers(artifact_id),
        }

    def table(
        self,
        flow_id: str,
        run_id: str,
        step: str,
        *,
        columns: Sequence[str] | None = None,
        filters: Sequence[Filter] = (),
        sort: str | None = None,
        descending: bool = False,
        offset: int = 0,
        limit: int = 100,
    ) -> dict:
        """Page a local table with validated identifiers and parameterized filters."""
        flow = self._flow(flow_id)
        self._local_run(flow_id, run_id)
        if step not in flow._dag.steps or flow._dag.steps[step].is_artifact:
            raise ValueError(f"not a table step: {step}")
        schema = flow._dag.steps[step].output_table
        assert issubclass(schema, Table)
        declared = schema.columns()
        selected = list(declared) if columns is None else list(columns)
        if not selected or set(selected) - declared.keys():
            raise ValueError("choose declared table columns")
        if sort is not None and sort not in declared:
            raise ValueError(f"unknown sort column: {sort}")
        if not 1 <= limit <= 1000 or offset < 0:
            raise ValueError("limit must be 1..1000 and offset must be nonnegative")
        where, params = ["run_id = ?"], [run_id]
        operators = {"eq": "=", "ne": "<>", "lt": "<", "le": "<=", "gt": ">", "ge": ">="}
        for item in filters:
            if item.column not in declared:
                raise ValueError(f"unknown filter column: {item.column}")
            column = _q(item.column)
            if item.operator == "contains":
                if declared[item.column].dtype is not str or not isinstance(item.value, str):
                    raise ValueError("contains requires a string column and value")
                where.append(f"contains(lower({column}), lower(?))")
            elif item.operator in operators:
                if item.value is None:
                    if item.operator not in ("eq", "ne"):
                        raise ValueError("null filters require eq or ne")
                    where.append(f"{column} IS {'NOT ' if item.operator == 'ne' else ''}NULL")
                    continue
                where.append(f"{column} {operators[item.operator]} ?")
            else:
                raise ValueError(f"unknown filter operator: {item.operator}")
            params.append(item.value)
        source = f"FROM {_q(step)} WHERE {' AND '.join(where)}"
        order = [sort] if sort else []
        order += [n for n in declared if n not in order and declared[n].primary_key]
        if not order:
            order = list(declared)
        ordering = ", ".join(f"{_q(n)} {'DESC' if descending and n == sort else 'ASC'} NULLS LAST" for n in order)
        with flow.duck(self.storage.config) as con:
            total = con.execute(f"SELECT count(*) {source}", params).fetchone()[0]
            rows = con.execute(
                f"SELECT {', '.join(map(_q, selected))} {source} ORDER BY {ordering} LIMIT ? OFFSET ?",
                [*params, limit, offset],
            ).fetchall()
        return {"columns": selected, "rows": rows, "total": total, "offset": offset, "limit": limit}

    def steps(self, flow_id: str, run_id: str) -> dict:
        """Read local resource summaries and native phases for one run."""
        flow = self._flow(flow_id)
        self._local_run(flow_id, run_id)
        with flow.duck(self.storage.config) as con:
            return {"steps": _rows(step_stats(con, run_id)), "phases": _rows(phase_stats(con, run_id))}

    def _flow(self, flow_id: str) -> type[Flow]:
        if flow_id not in self.flows:
            raise ValueError(f"unknown flow: {flow_id}")
        return self.flows[flow_id]

    def _local_run(self, flow_id: str, run_id: str) -> None:
        row = next((r for r in self.storage.runs(flow_id) if r["run_id"] == run_id), None)
        if row is None:
            raise ValueError(f"unknown run: {run_id}")
        if not row["local"]:
            raise MissingPayload(f"run {run_id}: download this run before reading outputs")
