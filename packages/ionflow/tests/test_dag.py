import pytest

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
    def chunks(self, raw: Raw) -> Fanout[Chunk]: ...
    def scored(self, chunks: Branch[Chunk]) -> Score: ...
    def result(self, scored: Collect[Score]) -> Final: ...


def test_order_topological():
    dag = build_dag(ETL)
    assert dag._order == ["raw", "chunks", "scored", "result"]  # every step, deps first
    assert dag.plan == ["raw", "chunks", "result"]  # the group contracted into its fan-out
    assert dag.groups["chunks"].order == ["scored"]
    assert dag.roots == {"scored": "chunks"}
    assert dag.downstream("chunks") == {"chunks", "scored", "result"}


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


def test_plain_steps_have_no_group():
    dag = build_dag(ETL)
    assert dag.groups["chunks"].order == ["scored"]
    assert dag.plan == ["raw", "chunks", "result"]


def test_branch_over_a_plain_step_rejected():
    class Bad:
        def raw(self) -> Raw: ...
        def scored(self, raw: Branch[Raw]) -> Score: ...

    with pytest.raises(ValueError, match="neither a fan-out nor a branch step"):
        build_dag(Bad)


def test_nested_fan_out_rejected():
    class Bad:
        def chunks(self) -> Fanout[Chunk]: ...
        def deeper(self, chunks: Branch[Chunk]) -> Fanout[Chunk]: ...

    with pytest.raises(ValueError, match="fan-out is one level"):
        build_dag(Bad)


def test_collecting_a_sibling_per_item_step_rejected():
    class Bad:
        def chunks(self) -> Fanout[Chunk]: ...
        def scored(self, chunks: Branch[Chunk]) -> Score: ...
        def polished(self, chunks: Branch[Chunk], scored: Collect[Score]) -> Score: ...

    with pytest.raises(ValueError, match="per item of fan-out 'chunks'"):
        build_dag(Bad)


def test_fan_in_feeding_back_into_its_own_fan_out_rejected():
    class Bad:
        def chunks(self) -> Fanout[Chunk]: ...
        def scored(self, chunks: Branch[Chunk]) -> Score: ...
        def stats(self, scored: Collect[Score]) -> Final: ...
        def polished(self, scored: Branch[Score], stats: Final) -> Score: ...

    with pytest.raises(ValueError, match="cycle detected once fan-outs run per item"):
        build_dag(Bad)


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
