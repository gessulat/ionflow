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
class Dag:
    steps: dict[str, Step]
    order: list[str]


def _is_step(name, obj):
    return inspect.isfunction(obj) and not name.startswith("_")


def _toposort(steps):
    indeg = {n: len(s.deps) for n, s in steps.items()}
    ready = sorted(n for n, d in indeg.items() if d == 0)
    order = []
    while ready:
        n = ready.pop(0)
        order.append(n)
        for m, s in steps.items():
            if n in s.deps:
                indeg[m] -= 1
                if indeg[m] == 0:
                    ready.append(m)
        ready.sort()
    if len(order) != len(steps):
        raise ValueError(f"cycle detected among steps: {set(steps) - set(order)}")
    return order


RESERVED_STEP_NAMES = {"ledger", "metrics"}  # taken by duck()'s system views


def build_dag(cls) -> Dag:
    prototype = getattr(cls, "prototype", False)
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
        if table is None and not prototype:
            raise ValueError(
                f"step '{name}' has no output table annotation (only prototype flows may omit it)"
            )
        steps[name] = Step(name, fn, kinds, table, is_fanout)

    for s in steps.values():
        for d in s.deps:
            if d not in steps:
                raise ValueError(
                    f"step '{s.name}' depends on unknown step '{d}'"
                )
    return Dag(steps=steps, order=_toposort(steps))
