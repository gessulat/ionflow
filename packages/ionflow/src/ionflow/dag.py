import graphlib
import heapq
import inspect
import typing
from dataclasses import dataclass


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

    @property
    def deps(self):
        return list(self.param_kinds)


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


RESERVED_STEP_NAMES = {"ledger", "metrics", "runs", "manifest"}  # duck()'s views + the lake's system tables


def build_dag(cls) -> Dag:
    funcs = {n: o for n, o in vars(cls).items() if _is_step(n, o)}
    for name in RESERVED_STEP_NAMES & set(funcs):
        raise ValueError(f"step '{name}' is reserved (system view)")
    steps = {}
    for name, fn in funcs.items():
        hints = typing.get_type_hints(fn)
        sig = inspect.signature(fn)
        kinds = {}
        for p in list(sig.parameters)[1:]:  # drop self
            origin = typing.get_origin(hints.get(p))
            kinds[p] = {Branch: "BRANCH", Collect: "COLLECT"}.get(origin, "NORMAL")
        ret = hints.get("return")
        if ret is type(None):  # `-> None` counts as untyped
            ret = None
        is_fanout = typing.get_origin(ret) is Fanout
        table = typing.get_args(ret)[0] if is_fanout else ret
        if table is None:
            raise ValueError(f"step '{name}' has no output table annotation")
        if "run_id" in table.columns():
            raise ValueError(f"step '{name}': column 'run_id' is reserved (the catalog's partition column)")
        steps[name] = Step(name, fn, kinds, table, is_fanout)

    for s in steps.values():
        for d in s.deps:
            if d not in steps:
                raise ValueError(f"step '{s.name}' depends on unknown step '{d}'")
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
