import datetime as dt

import pyarrow as pa
import pytest

from miniflow.schema import Column, Table


class Features(Table):
    id = Column(int, primary_key=True)
    ts = Column(dt.datetime, nullable=False)
    score = Column(float, check="score >= 0")
    label = Column(str)


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


def test_empty_table_rejected():
    with pytest.raises(TypeError):

        class Empty(Table):
            pass


def test_unknown_dtype_rejected():
    with pytest.raises(TypeError):

        class Bad(Table):
            x = Column(complex)
