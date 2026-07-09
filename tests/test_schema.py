import datetime as dt

import pyarrow as pa
import pytest

from microflow.schema import Table, check, field, pk


class Features(Table):
    id: int = pk
    ts: dt.datetime
    score: float | None = check("score >= 0")
    label: str | None


def test_columns_ordered():
    assert list(Features.columns()) == ["id", "ts", "score", "label"]


def test_ddl():
    ddl = Features.ddl("features")
    assert ddl == (
        "CREATE TABLE features (\n"
        "  id BIGINT NOT NULL PRIMARY KEY,\n"
        "  ts TIMESTAMP NOT NULL,\n"
        "  score DOUBLE CHECK (score >= 0),\n"
        "  label VARCHAR\n"
        ")"
    )


def test_arrow_schema():
    schema = Features.arrow_schema()
    assert schema.names == ["id", "ts", "score", "label"]
    assert schema.field("id").type == pa.int64()
    assert schema.field("ts").type == pa.timestamp("us")
    assert schema.field("score").type == pa.float64()
    assert schema.field("label").type == pa.string()
    assert not schema.field("id").nullable
    assert not schema.field("ts").nullable
    assert schema.field("score").nullable


def test_constraint_helpers_and_nullability():
    class T(Table):
        a: int = pk
        b: str | None
        c: float = check("c > 0")
        d: int | None = field(check="d >= 0")

    cols = T.columns()
    assert cols["a"].primary_key and not cols["a"].nullable
    assert cols["b"].nullable
    assert not cols["c"].nullable and cols["c"].check == "c > 0"
    assert cols["d"].nullable and cols["d"].check == "d >= 0"


def test_make_builds_table_dynamically():
    cols = {"id": int, **{f"frag_{i}": float | None for i in range(3)}}
    Wide = Table.make("Wide", cols, id=pk)
    assert list(Wide.columns()) == ["id", "frag_0", "frag_1", "frag_2"]
    assert Wide.ddl("wide").count("DOUBLE") == 3
    with pytest.raises(TypeError, match="unknown columns"):
        Table.make("Bad", {"a": int}, typo=pk)


def test_empty_table_rejected():
    with pytest.raises(TypeError):

        class Empty(Table):
            pass


def test_unknown_dtype_rejected():
    with pytest.raises(TypeError):

        class Bad(Table):
            x: complex
