import pytest

from miniflow.dag import Branch, Collect, Fanout, build_dag
from miniflow.schema import Table


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
    def chunks(self, raw: Raw) -> Fanout[Chunk]: ...
    def scored(self, chunks: Branch[Chunk]) -> Score: ...
    def result(self, scored: Collect[Score]) -> Final: ...


def test_order_topological():
    dag = build_dag(ETL)
    assert dag.order == ["raw", "chunks", "scored", "result"]


def test_step_kinds():
    dag = build_dag(ETL)
    assert dag.steps["chunks"].is_fanout
    assert dag.steps["scored"].param_kinds == {"chunks": "BRANCH"}
    assert dag.steps["result"].param_kinds == {"scored": "COLLECT"}
    assert dag.steps["raw"].deps == []
    assert dag.steps["scored"].deps == ["chunks"]


def test_output_tables():
    dag = build_dag(ETL)
    assert dag.steps["chunks"].output_table is Chunk  # unwrapped from Fanout
    assert dag.steps["result"].output_table is Final


def test_unknown_param_rejected():
    class Bad:
        def a(self) -> Raw: ...
        def b(self, nope: Raw) -> Final: ...

    with pytest.raises(ValueError, match="nope"):
        build_dag(Bad)


def test_cycle_rejected():
    class Cyclic:
        def a(self, b: Raw) -> Raw: ...
        def b(self, a: Raw) -> Raw: ...

    with pytest.raises(ValueError, match="cycle"):
        build_dag(Cyclic)


def test_private_methods_ignored():
    class WithHelper:
        def a(self) -> Raw: ...
        def _helper(self): ...

    dag = build_dag(WithHelper)
    assert list(dag.steps) == ["a"]
