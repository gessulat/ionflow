import graphlib
import heapq
import inspect
import types
import typing
from dataclasses import dataclass

from .artifact import Artifact, _spec
from .composition import Call, Subflow
from .schema import Table


class Fanout[T]:
    """Return marker: step yields N branch outputs."""


class Branch[T]:
    """Param marker: step runs once per branch of the named fan-out step."""


class Collect[T]:
    """Param marker: step receives all branch outputs of the named step."""


@dataclass
class Step:
    name: str
    method: typing.Callable
    param_kinds: dict[str, str]  # name -> NORMAL | BRANCH | COLLECT
    output_table: type
    is_fanout: bool
    subflow: Subflow | None = None

    @property
    def deps(self):
        return [n for n, k in self.param_kinds.items() if k != "INPUT"]

    @property
    def is_artifact(self):
        return issubclass(self.output_table, Artifact)


@dataclass
class Group:
    """A fan-out's per-item sub-DAG: the branch steps that run inside one branch,
    all in a single worker, independent members concurrently (0015).
    """

    fanout: str
    order: list[str]  # members, topological order within a single item
    deps: dict[str, list[str]]  # member -> the members it waits for, same item


@dataclass
class Dag:
    steps: dict[str, Step]
    _order: list[str]  # global topological order; groups/plan/roots are the public views of it
    groups: dict[str, Group]  # fan-out name -> its per-item sub-DAG (absent if it has none)
    plan: list[str]  # execution order; a fan-out node stands for its whole group
    roots: dict[str, str]  # branch step -> the fan-out at the root of its chain (0014)

    def downstream(self, root):
        """`root` plus every step reachable from it through dependency edges."""
        out = {root}
        for name in self._order:  # topo order: a step appears after all its deps
            if any(d in out for d in self.steps[name].deps):
                out.add(name)
        return out

    def schedule_graph(self):
        """The runner's node graph: every plain/fan-out step by name, plus an
        `("items", f)` node for fan-out f's per-item group. Edges are exact — a
        fan-out's emission does not wait on member-only inputs, and a group's
        dependents wait for its items — and every edge is implied by `plan`'s
        contraction, so the cycle check there covers this graph too.
        """

        def node(s):
            return ("items", self.roots[s]) if s in self.roots else s

        g: dict[str | tuple, set] = {name: set() for name in self.steps if name not in self.roots}
        for f in self.groups:
            g[("items", f)] = {f}
        for name, step in self.steps.items():
            for dep in step.deps:
                if node(dep) != node(name):
                    g[node(name)].add(node(dep))
        return g


def branch_dep(step):
    """The step whose branches this one runs over — its branch index space, or None."""
    return next((d for d, k in step.param_kinds.items() if k == "BRANCH"), None)


def _is_step(name, obj):
    return inspect.isfunction(obj) and not name.startswith("_")


def _toposort(graph):
    """Topological order, lexicographically smallest ready node first (deterministic)."""
    ts = graphlib.TopologicalSorter(graph)
    ts.prepare()  # raises graphlib.CycleError
    order, ready = [], []
    while ts.is_active():
        for n in ts.get_ready():
            heapq.heappush(ready, n)
        n = heapq.heappop(ready)
        order.append(n)
        ts.done(n)
    return order


def _roots(steps, order):
    """Branch step -> the fan-out at the root of its branch chain.

    A chained branch step inherits its upstream's item — branch i is the fan-out's
    i-th item however deep the chain runs — so the chain runs, and resumes, per
    item rather than per stage.
    """
    roots = {}
    for name in order:  # topo: a branch dep is classified before its dependents
        dep = branch_dep(steps[name])
        if dep is None:
            continue
        if steps[dep].is_fanout:
            roots[name] = dep
        elif dep in roots:
            roots[name] = roots[dep]
        else:
            raise ValueError(
                f"step '{name}' branches over '{dep}', which is neither a fan-out nor a "
                f"branch step; drop the Branch[...] marker to read it whole"
            )
        if steps[name].is_fanout:
            raise ValueError(f"step '{name}' fans out inside a branch of '{roots[name]}'; fan-out is one level")
    return roots


def _check_item_inputs(steps, roots):
    """A per-item step may only read a sibling per-item step one item at a time."""
    for name, root in roots.items():
        for dep, kind in steps[name].param_kinds.items():
            if kind == "BRANCH" or roots.get(dep) != root:
                continue
            raise ValueError(
                f"step '{name}' {'collects' if kind == 'COLLECT' else 'takes'} '{dep}', but both run "
                f"per item of fan-out '{root}'; a per-item step reads its own item only (Branch[...])"
            )


def _groups(steps, order, roots):
    """Fan-out name -> its per-item sub-DAG (members in intra-item topological order)."""
    groups = {}
    for name in order:
        if (root := roots.get(name)) is None:
            continue
        group = groups.setdefault(root, Group(root, [], {}))
        group.order.append(name)  # global topo order, restricted: still a valid item order
        group.deps[name] = [d for d in steps[name].deps if roots.get(d) == root]
    return groups


def _plan(steps, roots):
    """Execution order with every group contracted into its fan-out node.

    Contracting is what makes the per-item model checkable: an edge out of a
    group and back into it is a cycle here, and would be a deadlock at run time.
    """
    graph = {}
    for name, step in steps.items():
        node = roots.get(name, name)
        graph.setdefault(node, set()).update(roots.get(d, d) for d in step.deps if roots.get(d, d) != node)
    try:
        return _toposort(graph)
    except graphlib.CycleError as e:
        cycle = " -> ".join(e.args[1])
        raise ValueError(
            f"cycle detected once fan-outs run per item: {cycle}; a step that reads a whole "
            f"fan-out cannot also feed that fan-out's per-item steps"
        ) from None


RESERVED_STEP_NAMES = {
    "phase",
    "phases",
    "ledger",
    "metrics",
    "runs",
    "manifest",
    "artifacts",
    "artifact_files",
    "artifact_activity",
    "artifact_events",
    "artifact_inventory",
    "artifact_members",
    "artifact_event_records",
    "artifact_bindings",
    "artifact_selected_inputs",
}
PATH_COL = "__ionflow_path"  # the step views' filename join column (query.py)
RESERVED_COLUMNS = {
    "run_id": "the catalog's partition column",
    PATH_COL: "the step views' filename column",
}


def build_dag(cls) -> Dag:
    input_schema = getattr(cls, "Inputs", None)
    inputs = input_schema.declarations() if input_schema else {}
    funcs = {n: o for n, o in vars(cls).items() if _is_step(n, o)}
    if overlap := set(inputs) & (set(funcs) | RESERVED_STEP_NAMES):
        raise ValueError(f"artifact inputs collide with steps or system names: {sorted(overlap)}")
    for name in RESERVED_STEP_NAMES & set(funcs):
        raise ValueError(f"step '{name}' is reserved (system view)")
    steps = {}
    for name, fn in funcs.items():
        hints = typing.get_type_hints(fn)
        sig = inspect.signature(fn)
        kinds = {}
        for p in list(sig.parameters)[1:]:  # drop self
            origin = typing.get_origin(hints.get(p))
            if p in inputs:
                if hints.get(p) != inputs[p]:
                    raise TypeError(f"step '{name}': artifact input '{p}' must be annotated {inputs[p]}")
                kinds[p] = "INPUT"
            else:
                kinds[p] = {Branch: "BRANCH", Collect: "COLLECT"}.get(origin, "NORMAL")
        ret = hints.get("return")
        child = getattr(fn, "__ionflow_subflow__", None)
        if child:
            if typing.get_origin(ret) is not Call:
                raise TypeError(f"subflow step '{name}' must return Call[OutputSchema]")
            ret = typing.get_args(ret)[0]
            if ret is not child.flow._dag.steps[child.output].output_table:
                raise TypeError(f"subflow step '{name}': output schema differs from {child.output}")
        elif typing.get_origin(ret) is Call:
            raise TypeError(f"step '{name}': Call requires @subflow")
        if ret is type(None):  # `-> None` counts as untyped
            ret = None
        is_fanout = typing.get_origin(ret) is Fanout
        table = typing.get_args(ret)[0] if is_fanout else ret
        if table is None:
            raise ValueError(f"step '{name}' has no output table annotation")
        if not isinstance(table, type) or not issubclass(table, (Table, Artifact)):
            raise TypeError(f"step '{name}': output annotation must be a Table or Artifact")
        for col, why in RESERVED_COLUMNS.items():
            if not issubclass(table, Artifact) and col in table.columns():
                raise ValueError(f"step '{name}': column '{col}' is reserved ({why})")
        steps[name] = Step(name, fn, kinds, table, is_fanout, child)

    params = cls.Params.model_fields if getattr(cls, "Params", None) else {}
    for step in steps.values():
        schema = step.output_table
        if not issubclass(schema, Artifact) or not schema._identity or step.subflow:
            continue
        if step.is_fanout:
            raise TypeError(f"step '{step.name}': declared identities require one artifact per invocation; use Branch")
        sources = inputs | {n: f.annotation for n, f in params.items()}
        for name in step.deps:
            if name in steps:
                upstream = steps[name]
                if upstream.is_artifact:
                    sources[name] = (
                        types.GenericAlias(list, (upstream.output_table,))
                        if upstream.is_fanout and step.param_kinds[name] != "BRANCH"
                        else upstream.output_table
                    )
        for name in schema._identity:
            if name not in sources:
                raise TypeError(
                    f"step '{step.name}': identity field '{name}' needs a named artifact input, "
                    "parameter, or artifact dependency"
                )
            if _spec(sources[name]) != _spec(schema._fields[name]):
                raise TypeError(f"step '{step.name}': identity field '{name}' has a different source type")

    for s in steps.values():
        for d in s.deps:
            if d not in steps:
                raise ValueError(f"step '{s.name}' depends on unknown step '{d}'")
            if s.subflow:
                hint = typing.get_type_hints(s.method).get(d)
                if typing.get_origin(hint) in (Branch, Collect):
                    hint = typing.get_args(hint)[0]
                upstream = steps[d]
                expected = upstream.output_table
                if upstream.is_artifact and upstream.is_fanout and s.param_kinds[d] == "NORMAL":
                    hint = typing.get_args(hint)[0] if typing.get_origin(hint) is list else None
                if hint != expected:
                    raise TypeError(f"subflow step '{s.name}': dependency '{d}' must be annotated {expected}")
    try:
        order = _toposort({n: s.deps for n, s in steps.items()})
    except graphlib.CycleError as e:
        raise ValueError(f"cycle detected among steps: {' -> '.join(e.args[1])}") from None
    roots = _roots(steps, order)
    _check_item_inputs(steps, roots)
    return Dag(
        steps=steps,
        _order=order,
        groups=_groups(steps, order, roots),
        plan=_plan(steps, roots),
        roots=roots,
    )
