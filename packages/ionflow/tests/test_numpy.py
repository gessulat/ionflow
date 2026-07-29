"""numpy as a first-class citizen: np dtype annotations + auto-converted step returns.

The flows are module-level so branch workers can re-import them by file path.
"""

import duckdb
import numpy as np
import pyarrow as pa
import pytest

from ionflow import Branch, Collect, Fanout, Flow, Table, check, shaped
from ionflow.arrow import StepInput, table_from_bytes, to_arrow
from ionflow.space import ConstraintViolation, Space
from ionflow.store import Store

# --- schema: numpy dtype annotations ---


# --- to_arrow: the conversion matrix ---


class Pair(Table):
    a: np.int32
    b: np.float32


class Vec(Table):
    v: list[np.float32]


def test_dict_of_ndarrays_conversions():
    t = to_arrow({"a": np.arange(3, dtype=np.int16), "b": np.ones(3, dtype=np.float32)})
    assert t.schema.field("a").type == pa.int16()  # narrow dtypes survive
    assert t.schema.field("b").type == pa.float32()

    t = to_arrow({"v": np.arange(6, dtype=np.float32).reshape(2, 3)})  # 2D -> fixed-size list
    assert t.schema.field("v").type == pa.list_(pa.float32(), 3)
    assert t.column("v").to_pylist() == [[0.0, 1.0, 2.0], [3.0, 4.0, 5.0]]

    t = to_arrow({"v": np.zeros((2, 4, 3), dtype=np.int16)})  # 3D nests
    assert t.schema.field("v").type == pa.list_(pa.list_(pa.int16(), 3), 4)

    arr = np.arange(12, dtype=np.float32).reshape(3, 4)[:, ::2]  # strided view
    assert to_arrow({"v": arr}).column("v").to_pylist() == [[0.0, 2.0], [4.0, 6.0], [8.0, 10.0]]


def test_structured_array_carries_names_and_dtypes():
    arr = np.zeros(2, dtype=[("a", np.int32), ("b", np.float32)])
    arr["a"] = [1, 2]
    t = to_arrow(arr)
    assert t.schema.field("a").type == pa.int32()
    assert t.schema.field("b").type == pa.float32()
    assert t.column("a").to_pylist() == [1, 2]


def test_bare_ndarray_maps_onto_declared_columns():
    t = to_arrow(np.arange(3, dtype=np.float32), Table.make("One", {"x": np.float32}))
    assert t.column_names == ["x"]  # 1D -> the single declared column

    t = to_arrow(np.array([[1, 2.5], [3, 4.5]]), Pair)  # (N, k) -> k columns, positionally
    assert t.column_names == ["a", "b"]
    assert t.column("b").to_pylist() == [2.5, 4.5]

    t = to_arrow(np.arange(6, dtype=np.float32).reshape(2, 3), Vec)  # (N, d) -> one vector column
    assert t.schema.field("v").type == pa.list_(pa.float32(), 3)

    with pytest.raises(TypeError, match="dict of named arrays"):
        to_arrow(np.zeros((2, 3)), Pair)  # ambiguous shape is actionable


def test_complex_arrays_rejected():
    with pytest.raises(TypeError, match="complex"):
        to_arrow({"iq": np.zeros(3, dtype=np.complex64)})


# --- write path: dtypes survive to parquet, constraints still enforced ---


class Frame(Table):
    frame: np.int32
    x: np.float32 = check("x >= 0")


def test_constraints_apply_to_numpy_returns(cfg):
    space = Store.of(cfg).space("F")
    bad = {"frame": np.zeros(1, dtype=np.int32), "x": np.array([-1.0], dtype=np.float32)}
    with pytest.raises(ConstraintViolation):
        space.write_part(space.part("r2", "pts"), bad, Frame)


def test_nan_is_a_value_not_null(cfg):
    # NOT NULL means "no NULLs"; NaN is a legitimate float measurement artifact
    store = Store.of(cfg)
    space = store.space("F")
    key = space.part("r3", "pts")
    data = {"frame": np.zeros(1, dtype=np.int32), "x": np.array([np.nan], dtype=np.float32)}
    space.write_part(key, data, Table.make("NanOk", {"frame": np.int32, "x": np.float32}))
    assert np.isnan(table_from_bytes(store.get(key)).column("x").to_pylist()[0])


# --- shaped(...): fixed-shape array columns ---


def test_shaped_rejects_bad_dimensions():
    with pytest.raises(TypeError, match="positive int"):
        shaped(np.float32)
    with pytest.raises(TypeError, match="positive int"):
        shaped(np.float32, 0)


class Shaped3(Table):
    v: shaped(np.float32, 3)


def test_shaped_enforces_shape_at_write(cfg):
    space = Store.of(cfg).space("S")
    good = {"v": np.zeros((2, 3), dtype=np.float32)}
    space.write_part(space.part("r", "v", 0), good, Shaped3)
    with pytest.raises(duckdb.ConversionException, match="size 4 to array of size 3"):
        space.write_part(space.part("r", "v", 1), {"v": np.zeros((2, 4), dtype=np.float32)}, Shaped3)
    # ragged variable lists can't satisfy a fixed shape either
    with pytest.raises(duckdb.ConversionException, match="length"):
        space.write_part(
            space.part("r", "v", 2),
            pa.table({"v": pa.array([[1.0, 2.0]], pa.list_(pa.float32()))}),
            Shaped3,
        )


# --- StepInput: .np() on step inputs ---


def _input(table):
    return StepInput(duckdb.connect().from_arrow(table))


def test_np_no_arg_returns_all_columns():
    got = _input(pa.table({"a": [1], "b": [[1.0, 2.0]]})).np()
    assert set(got) == {"a", "b"} and got["b"].shape == (1, 2)


def test_np_ragged_lists_raise():
    inp = _input(pa.table({"v": [[1.0], [2.0, 3.0]]}))
    with pytest.raises(ValueError, match="ragged"):
        inp.np("v")


def test_np_nulls_raise():
    inp = _input(pa.table({"x": [1.0, None]}))
    with pytest.raises(ValueError, match="NULL"):
        inp.np("x")


def test_step_input_delegates_to_the_relation():
    inp = _input(pa.table({"x": [1, 2, 3]}))
    filtered = inp.filter("x > 1")  # delegated; returns a plain relation
    assert isinstance(filtered, duckdb.DuckDBPyRelation)
    assert inp.aggregate("sum(x) AS s").fetchone()[0] == 6


def test_write_part_accepts_an_input_returned_unchanged(cfg):
    # identity step: `def copy(self, raw: Raw) -> Raw: return raw`
    store = Store.of(cfg)
    space = store.space("F")
    key = space.part("r4", "pts")
    inp = _input(pa.table({"frame": pa.array([1], pa.int32()), "x": pa.array([2.0], pa.float32())}))
    space.write_part(key, inp, Frame)
    assert table_from_bytes(store.get(key)).column("x").to_pylist() == [2.0]


# --- end to end: a numpy-native flow, fan-out included ---


class Spectra(Table):
    frame: np.int32
    spectrum: shaped(np.float32, 16)


class Peak(Table):
    frame: np.int32
    peak: np.float32 = check("peak >= 0")


class Count(Table):
    n: int


class SignalFlow(Flow):
    """Steps return dicts of ndarrays end to end; a fan-out returns one dict per branch."""

    def spectra(self) -> Spectra:
        rng = np.random.default_rng(7)
        return {
            "frame": np.arange(4, dtype=np.int32),
            "spectrum": np.abs(rng.standard_normal((4, 16))).astype(np.float32),
        }

    def frames(self, spectra: Spectra) -> Fanout[Spectra]:
        return [spectra.filter(f"frame = {i}") for i in range(4)]

    def peaks(self, frames: Branch[Spectra]) -> Peak:
        return {"frame": frames.np("frame"), "peak": frames.np("spectrum").max(axis=1)}

    def counted(self, peaks: Collect[Peak]) -> Count:
        return peaks.aggregate("count(*) AS n")


def test_numpy_flow_end_to_end(cfg, repo):
    run_id = SignalFlow.run(cfg, repo=repo, use_processes=False)
    con = SignalFlow.duck(cfg)
    assert con.sql(f"SELECT n FROM counted WHERE run_id = '{run_id}'").fetchone()[0] == 4
    peaks = con.sql(f"SELECT peak FROM peaks WHERE run_id = '{run_id}'").fetchall()
    assert all(p > 0 for (p,) in peaks)
    # narrow dtypes survived the whole pipeline onto S3 (fixed shape persists as list)
    store = Store.of(cfg)
    space = Space.for_flow(store, SignalFlow)
    schema = table_from_bytes(store.get(space.part(run_id, "spectra"))).schema
    assert schema.field("frame").type == pa.int32()
    assert schema.field("spectrum").type == pa.list_(pa.float32())
    # the drift check compares against the persisted (list) form: no false positive
    from ionflow.lake import schema_drift

    assert schema_drift(SignalFlow, space) == []
