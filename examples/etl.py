"""Runnable miniflow example.

    export MINIFLOW_BUCKET=my-bucket   # + AWS_* / MINIFLOW_ENDPOINT
    miniflow run examples.etl:ETLFlow
    miniflow query examples.etl:ETLFlow "SELECT * FROM summary"

Load rows, fan out by group, score each group in parallel, fan in to a summary.
The DAG is the method signatures: `scored` depends on `parts` because it has a
parameter named `parts`.
"""

import pyarrow as pa

from miniflow import Branch, Collect, Column, Fanout, Flow, LedgerColumns, Table


class Raw(Table):
    id = Column(int, primary_key=True)
    grp = Column(int, nullable=False)
    value = Column(float, nullable=False)


class Part(Table):
    id = Column(int)
    grp = Column(int)
    value = Column(float)


class GroupScore(Table):
    grp = Column(int, nullable=False)
    total = Column(float, check="total >= 0")


class Summary(Table):
    n_groups = Column(int)
    grand_total = Column(float)


class ETLFlow(Flow):
    class Ledger(LedgerColumns):
        n_rows = Column(int)

    def raw(self) -> Raw:
        return pa.table(
            {"id": [1, 2, 3, 4], "grp": [0, 0, 1, 1], "value": [1.0, 2.0, 3.0, 4.0]}
        )

    def parts(self, raw: Raw) -> Fanout[Part]:
        groups = [r[0] for r in raw.project("grp").distinct().order("grp").fetchall()]
        return [raw.filter(f"grp = {g}") for g in groups]

    def scored(self, parts: Branch[Part]) -> GroupScore:
        return parts.aggregate("grp, sum(value) AS total")

    def summary(self, scored: Collect[GroupScore], raw: Raw) -> Summary:
        self.n_rows = raw.aggregate("count(*) AS c").fetchone()[0]
        return scored.aggregate("count(*) AS n_groups, sum(total) AS grand_total")
