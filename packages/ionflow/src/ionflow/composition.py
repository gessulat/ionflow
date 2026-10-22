"""Declared child-flow calls; execution and resume remain runner responsibilities."""

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .flow import Flow


@dataclass
class Call[T]:
    """Arguments for a declared subflow step, with its selected output schema.

    >>> Call[int](params={"count": 2}).params
    {'count': 2}
    """

    params: dict[str, Any] = field(default_factory=dict)
    artifacts: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Subflow:
    flow: type["Flow"]
    output: str


def subflow(flow: type["Flow"], *, output: str) -> Callable:
    """Declare a step that returns Call[Schema] arguments for a child flow.

    The output names one whole table or artifact step in the child. The child
    runs to completion before its output becomes available to the parent.
    """
    if not hasattr(flow, "_dag"):
        raise TypeError("subflow requires a Flow class")
    step = flow._dag.steps.get(output)
    if step is None or step.is_fanout or output in flow._dag.roots:
        raise ValueError("subflow output must name a whole table or artifact step")

    def declare(method: Callable) -> Callable:
        method.__dict__["__ionflow_subflow__"] = Subflow(flow, output)
        return method

    return declare
