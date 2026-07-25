import datetime as dt

import pyarrow as pa
import pytest

from microflow.schema import Table, check, fanout_key, field, primary_key


class Features(Table):
    id: int = primary_key
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
        a: int = primary_key
        b: str | None
        c: float = check("c > 0")
        d: int | None = field(check="d >= 0")

    cols = T.columns()
    assert cols["a"].primary_key and not cols["a"].nullable
    assert cols["b"].nullable
    assert not cols["c"].nullable and cols["c"].check == "c > 0"
    assert cols["d"].nullable and cols["d"].check == "d >= 0"


def test_list_columns():
    class Emb(Table):
        id: int = primary_key
        vec: list[float]
        tags: list[str] | None

    ddl = Emb.ddl("emb")
    assert "vec DOUBLE[] NOT NULL" in ddl
    assert "tags VARCHAR[]" in ddl and "tags VARCHAR[] NOT NULL" not in ddl
    schema = Emb.arrow_schema()
    assert schema.field("vec").type == pa.list_(pa.float64())
    assert schema.field("tags").nullable
    with pytest.raises(TypeError):

        class Bare(Table):
            x: list  # element type required


def test_make_builds_table_dynamically():
    cols = {"id": int, **{f"frag_{i}": float | None for i in range(3)}}
    Wide = Table.make("Wide", cols, id=primary_key)
    assert list(Wide.columns()) == ["id", "frag_0", "frag_1", "frag_2"]
    assert Wide.ddl("wide").count("DOUBLE") == 3
    with pytest.raises(TypeError, match="unknown columns"):
        Table.make("Bad", {"a": int}, typo=primary_key)


def test_fanout_key_marker():
    class Item(Table):
        path: str = fanout_key  # the shortcut; field(fanout_key=True) below covers combos
        size: int

    assert Item.fanout_key_column() == "path"
    assert Table.make("NoKey", {"a": int}).fanout_key_column() is None


def test_fanout_key_rejects_two_columns():
    with pytest.raises(TypeError, match="only one column"):

        class Two(Table):
            a: str = field(fanout_key=True)
            b: str = field(fanout_key=True)


def test_fanout_key_rejects_unsuitable_columns():
    with pytest.raises(TypeError, match="str or an integer"):

        class Floaty(Table):
            a: float = field(fanout_key=True)

    with pytest.raises(TypeError, match="nullable"):

        class Nully(Table):
            a: str | None = field(fanout_key=True)


def test_empty_table_rejected():
    with pytest.raises(TypeError):

        class Empty(Table):
            pass


def test_unknown_dtype_rejected():
    with pytest.raises(TypeError):

        class Bad(Table):
            x: complex
