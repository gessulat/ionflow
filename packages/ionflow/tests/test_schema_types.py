"""Golden fixture for schema.py's type rendering, pinned before the arrow-first rewrite.

Every expectation here is a hard-coded literal on purpose: these strings and arrow
types are what the catalog DDL, the write-path cast, the drift check and every
empty view are built from, so a rendering that moves must fail loudly rather than
be re-derived from the code under test.
"""

import datetime as dt

import duckdb
import numpy as np
import pyarrow as pa
import pytest

from ionflow import FlowParams, LedgerColumns, Table, check, fanout_key, field, primary_key, shaped
from ionflow.query import _empty_view_sql

# --- 1. the type grammar, one case per annotation ---

# (label, annotation, duckdb_type(), persisted_type(), arrow_schema() type)
GRAMMAR = [
    # leaves are a lookup in _LEAF/_SQL: spot-check the kinds, do not enumerate the dict
    ("int", int, "BIGINT", "BIGINT", pa.int64()),
    # numpy widths — narrow dtypes must persist, not widen
    ("np.float32", np.float32, "FLOAT", "FLOAT", pa.float32()),
    # list nesting, depth 1
    ("list[int]", list[int], "BIGINT[]", "BIGINT[]", pa.list_(pa.int64())),
    # list nesting, depth 2
    ("list[list[int]]", list[list[int]], "BIGINT[][]", "BIGINT[][]", pa.list_(pa.list_(pa.int64()))),
    # shaped, 1-D
    ("shaped(float,3)", shaped(float, 3), "DOUBLE[3]", "DOUBLE[]", pa.list_(pa.float64(), 3)),
    # shaped, 2-D — DuckDB suffixes read inside-out, arrow nests innermost first
    (
        "shaped(np.int16,4,3)",
        shaped(np.int16, 4, 3),
        "SMALLINT[3][4]",
        "SMALLINT[][]",
        pa.list_(pa.list_(pa.int16(), 3), 4),
    ),
    # mixed nesting: both spellings are representable and distinct
    (
        "list[shaped(float,3)]",
        list[shaped(float, 3)],
        "DOUBLE[3][]",
        "DOUBLE[][]",
        pa.list_(pa.list_(pa.float64(), 3)),
    ),
    (
        "shaped(list[float],3)",
        shaped(list[float], 3),
        "DOUBLE[][3]",
        "DOUBLE[][]",
        pa.list_(pa.list_(pa.float64()), 3),
    ),
]


IDS = [c[0] for c in GRAMMAR]


@pytest.mark.parametrize(("annotation", "duck", "persisted", "arrow"), [c[1:] for c in GRAMMAR], ids=IDS)
def test_type_grammar_renders(annotation, duck, persisted, arrow):
    col = Table.make("G", {"x": annotation}).columns()["x"]
    assert col.duckdb_type() == duck
    assert col.persisted_type() == persisted
    assert Table.make("G", {"x": annotation}).arrow_schema().field("x").type == arrow


@pytest.mark.parametrize("annotation", [complex, list, object, np.float16, np.str_, int | str | None])
def test_unsupported_annotations_rejected(annotation):
    with pytest.raises(TypeError, match="unsupported column dtype"):
        Table.make("Bad", {"x": annotation})


# --- 2. the rendered SQL of one table exercising every constraint kind ---


class Golden(Table):
    id: int = primary_key
    label: str | None
    score: float = check("score >= 0")
    spectrum: shaped(np.float32, 3)


def test_golden_renderings():
    """Every rendering of one table carrying all four constraint kinds."""
    assert Golden.ddl("golden") == (
        "CREATE TABLE golden (\n"
        '  "id" BIGINT NOT NULL PRIMARY KEY,\n'
        '  "label" VARCHAR,\n'
        '  "score" DOUBLE NOT NULL CHECK (score >= 0),\n'
        '  "spectrum" FLOAT[3] NOT NULL\n'
        ")"
    )
    assert Golden.ddl("golden", temp=True).startswith("CREATE TEMP TABLE golden (")
    # what the lakes are created from: persisted types (FLOAT[], not FLOAT[3]) and
    # NOT NULL only — PK/CHECK stay ionflow-side
    assert Golden.persisted_ddl() == (
        '"id" BIGINT NOT NULL, "label" VARCHAR, "score" DOUBLE NOT NULL, "spectrum" FLOAT[] NOT NULL'
    )
    # the FIXED flavour: this cast is the only place a shaped column's width is enforced
    assert Golden.cast_sql("_src") == (
        'SELECT CAST("id" AS BIGINT) AS "id", CAST("label" AS VARCHAR) AS "label", '
        'CAST("score" AS DOUBLE) AS "score", CAST("spectrum" AS FLOAT[3]) AS "spectrum" FROM _src'
    )
    assert Golden.violation_scans() == [
        ("id NOT NULL", 'count(*) FILTER (WHERE "id" IS NULL)'),
        ("score NOT NULL", 'count(*) FILTER (WHERE "score" IS NULL)'),
        ("CHECK (score >= 0)", "count(*) FILTER (WHERE (score >= 0) IS FALSE)"),
        ("spectrum NOT NULL", 'count(*) FILTER (WHERE "spectrum" IS NULL)'),
        ("PRIMARY KEY (id)", 'count(*) - count(DISTINCT ("id"))'),
    ]
    s = Golden.arrow_schema()
    assert s.names == ["id", "label", "score", "spectrum"]
    assert s.types == [pa.int64(), pa.string(), pa.float64(), pa.list_(pa.float32(), 3)]
    assert [f.nullable for f in s] == [False, True, False, False]


def test_column_names_needing_quotes():
    """Scientific schemas carry names SQL cannot take bare: 'b 1 +1', 'z* 5 +1'."""
    Ions = Table.make(
        "Ions",
        {"scan": int, "b 1 +1": float | None, "z* 5 +1": float | None, 'we"ird': float | None},
    )
    con = duckdb.connect()
    con.execute(Ions.ddl("ions"))  # the whole point: it parses
    con.sql(Ions.empty())  # and so does the typed empty stand-in

    assert '"b 1 +1" DOUBLE' in Ions.ddl("ions")
    assert 'CAST("b 1 +1" AS DOUBLE) AS "b 1 +1"' in Ions.cast_sql("_src")
    assert '"we""ird"' in Ions.ddl("ions")  # embedded quote doubled
    assert '"scan" BIGINT NOT NULL' in Ions.persisted_ddl()
    labels = [label for label, _ in Ions.violation_scans()]
    assert labels == ["scan NOT NULL"]  # labels stay human-readable, unquoted


# --- 3. the other column-set classes ---


def test_flow_params_resolve():
    class P(FlowParams):
        dataset: str
        batch_size: int = 32
        lr: float = 1
        flag: bool | None = None

    assert P.resolve({"dataset": "d1"}) == {"dataset": "d1", "batch_size": 32, "lr": 1.0, "flag": None}
    assert P.resolve({"dataset": "d1", "lr": 2})["lr"] == 2.0  # int coerces to float
    assert isinstance(P.resolve({"dataset": "d1"})["lr"], float)
    with pytest.raises(ValueError, match="Field required"):
        P.resolve({})
    with pytest.raises(ValueError, match="Extra inputs"):
        P.resolve({"dataset": "d1", "nope": 1})
    with pytest.raises(ValueError, match="valid string"):
        P.resolve({"dataset": 1})
    with pytest.raises(ValueError, match="valid string"):
        P.resolve({"dataset": None})


@pytest.mark.parametrize("annotation", [dt.datetime, bytes, list])
def test_flow_params_reject_non_json_types(annotation):
    with pytest.raises((TypeError, ValueError)):

        class P(FlowParams):
            x: annotation

        P.columns()  # pragma: no cover — the class body already raised


def test_ledger_columns_always_nullable_and_may_be_empty():
    class L(LedgerColumns):
        auc: float
        tag: str | None

    # every column nullable regardless of spelling: older runs predate any column
    assert [f.nullable for f in L.arrow_fields()] == [True, True]

    class NoCols(LedgerColumns):
        pass

    assert NoCols.columns() == {}  # an empty declaration is legal here, unlike Table


def test_make_carries_constraints():
    cols = {"id": int, "v": shaped(np.float32, 2), "s": str | None, "tags": list[str] | None}
    Wide = Table.make("Wide", cols, id=primary_key)
    assert list(Wide.columns()) == ["id", "v", "s", "tags"]
    assert Wide.columns()["id"].primary_key and not Wide.columns()["id"].nullable
    # NOT NULL is orthogonal to the type: an array can be either
    assert Wide.persisted_ddl() == '"id" BIGINT NOT NULL, "v" FLOAT[] NOT NULL, "s" VARCHAR, "tags" VARCHAR[]'
    with pytest.raises(TypeError, match="unknown columns"):
        Table.make("Bad", {"a": int}, typo=primary_key)


def test_fanout_key_column():
    class Item(Table):
        path: str = fanout_key
        size: int

    assert Item.fanout_key_column() == "path"
    assert Table.make("NoKey", {"a": int}).fanout_key_column() is None
    assert Table.make("Keyed", {"a": np.int32}, a=fanout_key).fanout_key_column() == "a"


@pytest.mark.parametrize(
    ("cols", "keys", "match"),
    [
        ({"a": str, "b": int}, ("a", "b"), "only one column may be the fanout_key"),
        ({"a": float}, ("a",), "str or an integer"),
        ({"a": str | None}, ("a",), "nullable"),
    ],
    ids=["two-keys", "float-key", "nullable-key"],
)
def test_fanout_key_rejections(cols, keys, match):
    with pytest.raises(TypeError, match=match):
        Table.make("K", cols, **{k: field(fanout_key=True) for k in keys})


def test_field_combines_nullability_with_a_check():
    """`field(check=...)` is the spelling that survives an optional annotation."""

    class T(Table):
        d: int | None = field(check="d >= 0")

    col = T.columns()["d"]
    assert col.nullable and col.check == "d >= 0"


def test_table_with_no_columns_rejected():
    with pytest.raises(TypeError, match="declares no columns"):

        class Empty(Table):
            pass


# --- 4. the typed-empty stand-ins ---


class Spectrum(Table):
    frame: np.int32
    spectrum: shaped(np.float32, 3)
    tags: list[str]


def test_empty_relation_and_view_agree():
    con = duckdb.connect()
    rel = Spectrum.empty(con)  # runner's reducer input for an empty fan-out
    view = con.sql(_empty_view_sql(Spectrum))  # query's view for a step with no parts
    declared = list(Spectrum.columns())
    assert rel.columns == declared
    assert view.columns == [*declared, "run_id"]  # run_id last, matching the catalog views
    assert [str(t) for t in rel.types] == [str(t) for t in view.types[: len(declared)]]
    # Both must be the PERSISTED flavour — what a reader of real parts sees.
    # Anchored on literals, not on each other: the two producers share one
    # implementation, so comparing them alone cannot catch a flavour flip.
    assert [str(t) for t in rel.types] == ["INTEGER", "FLOAT[]", "VARCHAR[]"]
    assert [c.persisted_type() for c in Spectrum.columns().values()] == ["INTEGER", "FLOAT[]", "VARCHAR[]"]
