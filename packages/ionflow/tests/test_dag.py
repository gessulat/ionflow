import pytest

from ionflow import Artifact, StepInput
from ionflow.dag import Branch, Collect, Fanout, build_dag
from ionflow.schema import Table


class Raw(Table):
    x: int


class Chunk(Table):
    x: int


class Score(Table):
    s: float


class Final(Table):
    s: float


class ETL:
    def raw(self) -> Raw: ...
    def chunks(self, raw: StepInput[Raw]) -> Fanout[Chunk]: ...
    def scored(self, chunks: Branch[Chunk]) -> Score: ...
    def result(self, scored: Collect[Score]) -> Final: ...


def test_dag_shape():
    dag = build_dag(ETL)
    assert dag._order == ["raw", "chunks", "scored", "result"]  # every step, deps first
    assert dag.plan == ["raw", "chunks", "result"]  # the group contracted into its fan-out
    assert dag.groups["chunks"].order == ["scored"]
    assert dag.roots == {"scored": "chunks"}
    assert dag.downstream("chunks") == {"chunks", "scored", "result"}
    assert dag.steps["chunks"].is_fanout
    assert dag.steps["scored"].param_kinds == {"chunks": "BRANCH"}
    assert dag.steps["result"].param_kinds == {"scored": "COLLECT"}
    assert dag.steps["scored"].deps == ["chunks"]
    assert dag.steps["chunks"].output_table is Chunk  # unwrapped from Fanout
    assert dag.steps["result"].output_table is Final


def test_typed_step_input_refuses_the_wrong_schema():
    class Wrong:
        def raw(self) -> Raw: ...
        def result(self, raw: StepInput[Score]) -> Final: ...

    with pytest.raises(TypeError, match="dependency 'raw' must be annotated"):
        build_dag(Wrong)


def test_step_input_requires_a_table_schema():
    class File(Artifact):
        pass

    class WrongArtifact:
        def source(self) -> File: ...
        def result(self, source: StepInput[File]) -> Final: ...

    class Unparameterized:
        def raw(self) -> Raw: ...
        def result(self, raw: StepInput) -> Final: ...

    with pytest.raises(TypeError, match="requires a table dependency"):
        build_dag(WrongArtifact)
    with pytest.raises(TypeError, match=r"needs StepInput\[TableSchema\]"):
        build_dag(Unparameterized)


class Chained:
    """Two branch steps in a row: `polished` branches over `scored`, not the fan-out."""

    def raw(self) -> Raw: ...
    def chunks(self, raw: Raw) -> Fanout[Chunk]: ...
    def scored(self, chunks: Branch[Chunk]) -> Score: ...
    def polished(self, chunks: Branch[Chunk], scored: Branch[Score]) -> Score: ...
    def result(self, polished: Collect[Score]) -> Final: ...


def test_chained_branch_shape():
    dag = build_dag(Chained)
    assert dag.plan == ["raw", "chunks", "result"]
    assert dag.groups["chunks"].order == ["scored", "polished"]
    # branch identity is inherited: the chained step is named by the same fan-out (0014)
    assert dag.roots == {"scored": "chunks", "polished": "chunks"}
    # a branch step's output is a plain table, so branching over it is just BRANCH again
    assert not dag.steps["scored"].is_fanout
    assert dag.steps["polished"].param_kinds == {"chunks": "BRANCH", "scored": "BRANCH"}
    assert dag.steps["polished"].deps == ["chunks", "scored"]
    assert dag.steps["result"].param_kinds == {"polished": "COLLECT"}


class SubDag:
    """A fan-out branch that is itself a DAG: two independent steps after one."""

    def raw(self) -> Raw: ...
    def chunks(self, raw: Raw) -> Fanout[Chunk]: ...
    def scored(self, chunks: Branch[Chunk]) -> Score: ...
    def stored(self, scored: Branch[Score]) -> Score: ...
    def polished(self, scored: Branch[Score]) -> Score: ...
    def result(self, polished: Collect[Score]) -> Final: ...


def test_group_collects_a_fan_outs_per_item_steps():
    dag = build_dag(SubDag)
    group = dag.groups["chunks"]
    assert group.order == ["scored", "polished", "stored"]  # topological within one item
    assert group.deps == {"scored": [], "polished": ["scored"], "stored": ["scored"]}
    # the fan-out node stands for its whole group: members never execute on their own
    assert dag.plan == ["raw", "chunks", "result"]
    assert list(dag.groups) == ["chunks"]


class _BranchOverPlain:
    def raw(self) -> Raw: ...
    def scored(self, raw: Branch[Raw]) -> Score: ...


class _NestedFanout:
    def chunks(self) -> Fanout[Chunk]: ...
    def deeper(self, chunks: Branch[Chunk]) -> Fanout[Chunk]: ...


class _SiblingCollect:
    def chunks(self) -> Fanout[Chunk]: ...
    def scored(self, chunks: Branch[Chunk]) -> Score: ...
    def polished(self, chunks: Branch[Chunk], scored: Collect[Score]) -> Score: ...


class _FanInCycle:
    def chunks(self) -> Fanout[Chunk]: ...
    def scored(self, chunks: Branch[Chunk]) -> Score: ...
    def stats(self, scored: Collect[Score]) -> Final: ...
    def polished(self, scored: Branch[Score], stats: Final) -> Score: ...


class _UnknownParam:
    def a(self) -> Raw: ...
    def b(self, nope: Raw) -> Final: ...


class _Cyclic:
    def a(self, b: Raw) -> Raw: ...
    def b(self, a: Raw) -> Raw: ...


@pytest.mark.parametrize(
    ("bad", "match"),
    [
        (_BranchOverPlain, "neither a fan-out nor a branch step"),
        (_NestedFanout, "fan-out is one level"),
        (_SiblingCollect, "per item of fan-out 'chunks'"),
        (_FanInCycle, "cycle detected once fan-outs run per item"),
        (_UnknownParam, "nope"),
        (_Cyclic, "cycle"),
    ],
    ids=lambda v: v.__name__.lstrip("_") if isinstance(v, type) else "",
)
def test_build_dag_refusals(bad, match):
    with pytest.raises(ValueError, match=match):
        build_dag(bad)


def test_private_methods_ignored():
    class WithHelper:
        def a(self) -> Raw: ...
        def _helper(self): ...

    dag = build_dag(WithHelper)
    assert list(dag.steps) == ["a"]
