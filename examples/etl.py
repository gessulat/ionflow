"""Runnable microflow example.

    export MICROFLOW_BUCKET=my-bucket   # + AWS_* / MICROFLOW_ENDPOINT
    microflow run examples.etl:ETLFlow
    microflow query examples.etl:ETLFlow "SELECT * FROM summary"

Load orders, fan out by region, total each region in parallel, fan in to a
summary. The DAG is the method signatures: `region_totals` depends on
`by_region` because it has a parameter named `by_region`.
"""

import pyarrow as pa

from microflow import Branch, Collect, Fanout, Flow, LedgerColumns, Table, check, primary_key


class Order(Table):
    order_id: int = primary_key
    region: str
    amount: float


class RegionTotal(Table):
    region: str
    total: float = check("total >= 0")


class Summary(Table):
    n_regions: int
    grand_total: float


class ETLFlow(Flow):
    class Ledger(LedgerColumns):
        n_orders: int

    def orders(self) -> Order:
        return pa.table(
            {
                "order_id": [1, 2, 3, 4],
                "region": ["eu", "eu", "us", "us"],
                "amount": [1.0, 2.0, 3.0, 4.0],
            }
        )

    def by_region(self, orders: Order) -> Fanout[Order]:  # partitioning reuses the schema
        regions = [r[0] for r in orders.project("region").distinct().order("region").fetchall()]
        return [orders.filter(f"region = '{r}'") for r in regions]

    def region_totals(self, by_region: Branch[Order]) -> RegionTotal:
        return by_region.aggregate("region, sum(amount) AS total")

    def summary(self, region_totals: Collect[RegionTotal], orders: Order) -> Summary:
        self.n_orders = orders.aggregate("count(*) AS c").fetchone()[0]
        return region_totals.aggregate("count(*) AS n_regions, sum(total) AS grand_total")
