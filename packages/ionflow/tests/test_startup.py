"""What a launch does before any flow runs: find flows in code, find flows with data.

The declaration-vs-catalog comparison (`schema_drift`) lives in test_lake_contract.
"""

import textwrap

from ionflow.flow import discover_flows
from ionflow.tui import scan_runs


def _make_pkg(root, name, flows):
    pkg = root / name
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")
    for mod, cls in flows:
        (pkg / f"{mod}.py").write_text(
            textwrap.dedent(f"""
            from ionflow import Flow, Table
            class {cls}Rows(Table):
                x: int
            class {cls}(Flow):
                def data(self) -> {cls}Rows: ...
            """)
        )
    return pkg


def test_discover_flows_walks_submodules(tmp_path, monkeypatch):
    _make_pkg(tmp_path, "mypkg", [("a", "FlowA"), ("b", "FlowB")])
    monkeypatch.syspath_prepend(str(tmp_path))
    flows = discover_flows("mypkg")
    assert set(flows) == {"mypkg.a.FlowA", "mypkg.b.FlowB"}  # keys are flow_ids
    assert flows["mypkg.a.FlowA"].flow_id() == "mypkg.a.FlowA"


def test_discover_flows_isolated_by_package(tmp_path, monkeypatch):
    _make_pkg(tmp_path, "pkgx", [("m", "OnlyX")])
    monkeypatch.syspath_prepend(str(tmp_path))
    flows = discover_flows("pkgx")
    assert set(flows) == {"pkgx.m.OnlyX"}  # other packages' flows are filtered out


def test_scan_runs(s3cfg):
    from ionflow.store import Store

    store = Store.of(s3cfg)
    assert scan_runs(store) == set()
    # key presence is all that matters here; no need for a full flow run
    store.put(f"{s3cfg.prefix}/Alpha/ledger.parquet", b"")
    assert scan_runs(store) == {"Alpha"}
