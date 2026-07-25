"""Runnable microflow example.

    export MICROFLOW_BUCKET=my-bucket   # + AWS_* / MICROFLOW_ENDPOINT
    microflow run examples.etl:ETLFlow
    microflow query examples.etl:ETLFlow "SELECT * FROM summary"

Load rows, fan out by group, score each group in parallel, fan in to a summary.
The DAG is the method signatures: `scored` depends on `parts` because it has a
parameter named `parts`.
"""

import pyarrow as pa

from microflow import Branch, Collect, Fanout, Flow, LedgerColumns, Table, check, primary_key


class Raw(Table):
    id: int = primary_key
    grp: int
    value: float


class Part(Table):
    id: int
    grp: int
    value: float


class GroupScore(Table):
    grp: int
    total: float = check("total >= 0")


class Summary(Table):
    n_groups: int
    grand_total: float


class ETLFlow(Flow):
    class Ledger(LedgerColumns):
        n_rows: int

    def raw(self) -> Raw:
        return pa.table({"id": [1, 2, 3, 4], "grp": [0, 0, 1, 1], "value": [1.0, 2.0, 3.0, 4.0]})

    def parts(self, raw: Raw) -> Fanout[Part]:
        groups = [r[0] for r in raw.project("grp").distinct().order("grp").fetchall()]
        return [raw.filter(f"grp = {g}") for g in groups]

    def scored(self, parts: Branch[Part]) -> GroupScore:
        return parts.aggregate("grp, sum(value) AS total")

    def summary(self, scored: Collect[GroupScore], raw: Raw) -> Summary:
        self.n_rows = raw.aggregate("count(*) AS c").fetchone()[0]
        return scored.aggregate("count(*) AS n_groups, sum(total) AS grand_total")
