import json

import pytest
from browser_flows import Bundle, Document, Observe, Rows

from ionflow import Browser, Filter, MissingPayload, Storage, View
from ionflow.space import Space
from ionflow.store import Store


def test_declarations_describe_labels_references_and_dag(localcfg):
    browser = Browser(localcfg, flows=[Observe], artifacts=[Document, Bundle])
    description = browser.describe()
    json.dumps(description)
    document, bundle = description["artifacts"]
    assert document["view"]["plural"] == "Documents"
    assert bundle["view"]["references"] == {"document_id": Document._kind}
    flow = description["flows"][0]
    assert flow["view"]["columns"] == ["run_id", "status", "fail"]
    steps = {s["name"]: s for s in flow["steps"]}
    assert steps["bundle"]["dependencies"] == ["rows"]
    assert steps["bundle"]["output"] == {"kind": Bundle._kind}
    assert steps["rows"]["output"]["view"]["references"] == {"document_id": Document._kind}


@pytest.mark.parametrize(
    "view,error",
    [
        (View(columns=("missing",)), "unknown fields"),
        (View(formats={"text": "html"}), "unknown view formats"),
        (View(references={"score": Document}), "string"),
        (View(references={"text": Rows}), "Artifact"),
    ],
)
def test_invalid_view_hints_fail_at_description(localcfg, monkeypatch, view, error):
    monkeypatch.setattr(Rows, "View", view)
    with pytest.raises((ValueError, TypeError), match=error):
        Browser(localcfg, flows=[Observe]).describe()


def test_inventory_and_lineage_never_open_payloads(observed, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("browsing metadata must not read payloads or open a peer")

    monkeypatch.setattr(Store, "download", forbidden)
    monkeypatch.setattr(Space, "relation", forbidden)
    monkeypatch.setattr(Storage, "remote", property(forbidden))
    rows = observed.browser.runs(Observe.flow_id())
    assert rows[0]["run_id"] == observed.run_id and rows[0]["fail"] is False
    bundle = observed.browser.artifacts(Bundle._kind)[0]
    assert bundle["document_id"] == observed.document.id and bundle["size"] > 0
    lineage = observed.browser.lineage(bundle["id"])
    assert observed.document.id in lineage["parents"]
    assert lineage["producers"][0]["run_id"] == observed.run_id


def test_table_filters_are_bound_and_pagination_is_bounded(observed):
    table = observed.browser.table
    base = (Observe.flow_id(), observed.run_id, "rows")
    page = table(*base, filters=[Filter("text", "contains", "'%_")])
    assert page["total"] == 0  # contains treats percent and underscore literally
    page = table(*base, filters=[Filter("text", "contains", "%_")])
    assert page["total"] == 1 and page["rows"][0][1] == "one's %_content"
    assert table(*base, filters=[Filter("score", "le", 0.5)])["total"] == 2
    assert table(*base, filters=[Filter("text", "eq", None)])["total"] == 1
    assert table(*base, sort="score", descending=True, limit=1, offset=1)["rows"][0][-1] == 0.5
    for options in (
        {"sort": "score; DROP TABLE rows"},
        {"limit": 1001},
        {"columns": ["nope"]},
        {"filters": [Filter("score", "raw_sql", "true")]},
        {"offset": -1},
    ):
        with pytest.raises(ValueError):
            table(*base, **options)


def test_metadata_only_run_rejects_table_reads(observed, tmp_path):
    from ionflow import Config
    from ionflow.lake import Catalog

    config = Config(local_dir=str(tmp_path / "replica"), prefix="mf")
    source = Catalog(observed.storage.local.space(Observe.flow_id()))
    Catalog(Store.of(config).space(Observe.flow_id())).merge_metadata(source.inventory(), source.tables())
    browser = Browser(config, flows=[Observe])
    assert browser.runs(Observe.flow_id())[0]["local"] is False
    with pytest.raises(MissingPayload):
        browser.table(Observe.flow_id(), observed.run_id, "rows")


def test_phase_details_include_exact_nested_spans(observed):
    phases = observed.browser.steps(Observe.flow_id(), observed.run_id)["phases"]
    outer, inner = sorted(phases, key=lambda p: p["started"])
    assert outer["phase"] == "decode" and inner["phase"] == "read"
    assert inner["parent_id"] == outer["span_id"]
    assert outer["status"] == inner["status"] == "complete"
    assert all(p["seconds"] >= 0 and p["peak_rss_mb"] > 0 for p in phases)
