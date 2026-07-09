import pyarrow as pa

from miniflow import Flow, LedgerColumns, Table


class Rows(Table):
    x: int


class Alpha(Flow):
    class Ledger(LedgerColumns):
        note: str

    def data(self) -> Rows:
        self.note = "alpha-note"
        return pa.table({"x": [1, 2, 3]})


class Beta(Flow):
    class Ledger(LedgerColumns):
        count: int

    def data(self) -> Rows:
        self.count = 7
        return pa.table({"x": [10, 20]})
