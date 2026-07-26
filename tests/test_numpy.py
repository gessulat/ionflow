"""numpy as a first-class citizen: np dtype annotations + auto-converted step returns.

The flows are module-level so branch workers can re-import them by file path.
"""

import duckdb
import numpy as np
import pyarrow as pa
import pytest

from ionflow import Branch, Collect, Fanout, Flow, FlowParams, Table, check, shaped, storage
from ionflow.storage import Layout, get_bytes_or_none, s3_client, table_from_bytes, to_arrow

# --- schema: numpy dtype annotations ---


def test_numpy_dtypes_compile_to_ddl_and_arrow():
    class T(Table):
        a: np.float32
        b: np.int16 | None
        c: list[np.float32]
        d: np.uint8

    cols = T.columns()
    assert cols["a"].duckdb_type() == "FLOAT"
    assert cols["b"].duckdb_type() == "SMALLINT" and cols["b"].nullable
    assert cols["c"].duckdb_type() == "FLOAT[]"
    assert cols["d"].duckdb_type() == "UTINYINT"
    s = T.arrow_schema()
    assert s.field("a").type == pa.float32()
    assert s.field("b").type == pa.int16()
    assert s.field("c").type == pa.list_(pa.float32())


def test_numpy_params_still_rejected():
    # params must round-trip JSON/CLI; np scalars stay excluded
    with pytest.raises(TypeError, match="only int/float/str/bool"):

        class P(FlowParams):
            x: np.float32


# --- to_arrow: the conversion matrix ---


class Pair(Table):
    a: np.int32
    b: np.float32


class Vec(Table):
    v: list[np.float32]


def test_dict_of_ndarrays_preserves_dtypes():
    t = to_arrow({"a": np.arange(3, dtype=np.int16), "b": np.ones(3, dtype=np.float32)})
    assert t.schema.field("a").type == pa.int16()
    assert t.schema.field("b").type == pa.float32()


def test_dict_2d_ndarray_becomes_fixed_size_list():
    t = to_arrow({"v": np.arange(6, dtype=np.float32).reshape(2, 3)})
    assert t.schema.field("v").type == pa.list_(pa.float32(), 3)
    assert t.column("v").to_pylist() == [[0.0, 1.0, 2.0], [3.0, 4.0, 5.0]]


def test_dict_3d_ndarray_nests():
    t = to_arrow({"v": np.zeros((2, 4, 3), dtype=np.int16)})
    assert t.schema.field("v").type == pa.list_(pa.list_(pa.int16(), 3), 4)


def test_dict_2d_noncontiguous_ndarray():
    arr = np.arange(12, dtype=np.float32).reshape(3, 4)[:, ::2]  # strided view
    t = to_arrow({"v": arr})
    assert t.column("v").to_pylist() == [[0.0, 2.0], [4.0, 6.0], [8.0, 10.0]]


def test_structured_array_carries_names_and_dtypes():
    arr = np.zeros(2, dtype=[("a", np.int32), ("b", np.float32)])
    arr["a"] = [1, 2]
    t = to_arrow(arr)
    assert t.schema.field("a").type == pa.int32()
    assert t.schema.field("b").type == pa.float32()
    assert t.column("a").to_pylist() == [1, 2]


def test_bare_1d_maps_to_single_column():
    t = to_arrow(np.arange(3, dtype=np.float32), Table.make("One", {"x": np.float32}))
    assert t.column_names == ["x"]


def test_bare_2d_maps_positionally():
    t = to_arrow(np.array([[1, 2.5], [3, 4.5]]), Pair)
    assert t.column_names == ["a", "b"]
    assert t.column("b").to_pylist() == [2.5, 4.5]


def test_bare_2d_maps_to_single_vector_column():
    t = to_arrow(np.arange(6, dtype=np.float32).reshape(2, 3), Vec)
    assert t.schema.field("v").type == pa.list_(pa.float32(), 3)


def test_bare_ndarray_shape_mismatch_is_actionable():
    with pytest.raises(TypeError, match="dict of named arrays"):
        to_arrow(np.zeros((2, 3)), Pair)


def test_complex_arrays_rejected():
    with pytest.raises(TypeError, match="complex"):
        to_arrow({"iq": np.zeros(3, dtype=np.complex64)})


# --- write path: dtypes survive to parquet, constraints still enforced ---


class Frame(Table):
    frame: np.int32
    x: np.float32 = check("x >= 0")


def test_write_part_persists_narrow_dtypes(s3cfg):
    client = s3_client(s3cfg)
    key = "mf/F/runs/run_id=r/pts/part_0.parquet"
    data = {"frame": np.arange(2, dtype=np.int32), "x": np.ones(2, dtype=np.float32)}
    storage.write_part(client, s3cfg.bucket, key, data, Frame)
    schema = table_from_bytes(get_bytes_or_none(client, s3cfg.bucket, key)).schema
    assert schema.field("frame").type == pa.int32()
    assert schema.field("x").type == pa.float32()


def test_constraints_apply_to_numpy_returns(s3cfg):
    client = s3_client(s3cfg)
    bad = {"frame": np.zeros(1, dtype=np.int32), "x": np.array([-1.0], dtype=np.float32)}
    with pytest.raises(storage.ConstraintViolation):
        storage.write_part(client, s3cfg.bucket, "mf/F/runs/run_id=r2/pts/part_0.parquet", bad, Frame)


def test_nan_is_a_value_not_null(s3cfg):
    # NOT NULL means "no NULLs"; NaN is a legitimate float measurement artifact
    client = s3_client(s3cfg)
    key = "mf/F/runs/run_id=r3/pts/part_0.parquet"
    data = {"frame": np.zeros(1, dtype=np.int32), "x": np.array([np.nan], dtype=np.float32)}
    storage.write_part(client, s3cfg.bucket, key, data, Table.make("NanOk", {"frame": np.int32, "x": np.float32}))
    assert np.isnan(table_from_bytes(get_bytes_or_none(client, s3cfg.bucket, key)).column("x").to_pylist()[0])


# --- shaped(...): fixed-shape array columns ---


def test_shaped_compiles_to_fixed_types():
    class T(Table):
        v: shaped(np.float32, 3)
        m: shaped(np.int16, 4, 3)  # cells are (4, 3) arrays

    cols = T.columns()
    assert cols["v"].duckdb_type() == "FLOAT[3]"
    assert cols["m"].duckdb_type() == "SMALLINT[3][4]"  # DuckDB suffixes read inside-out
    assert cols["v"].persisted_type() == "FLOAT[]"  # parquet round-trips fixed as list
    s = T.arrow_schema()
    assert s.field("v").type == pa.list_(pa.float32(), 3)
    assert s.field("m").type == pa.list_(pa.list_(pa.int16(), 3), 4)


def test_shaped_rejects_bad_dimensions():
    with pytest.raises(TypeError, match="positive int"):
        shaped(np.float32)
    with pytest.raises(TypeError, match="positive int"):
        shaped(np.float32, 0)


class Shaped3(Table):
    v: shaped(np.float32, 3)


def test_shaped_enforces_shape_at_write(s3cfg):
    client = s3_client(s3cfg)
    good = {"v": np.zeros((2, 3), dtype=np.float32)}
    storage.write_part(client, s3cfg.bucket, "mf/S/runs/run_id=r/v/part_0.parquet", good, Shaped3)
    with pytest.raises(duckdb.ConversionException, match="size 4 to array of size 3"):
        storage.write_part(
            client,
            s3cfg.bucket,
            "mf/S/runs/run_id=r/v/part_1.parquet",
            {"v": np.zeros((2, 4), dtype=np.float32)},
            Shaped3,
        )
    # ragged variable lists can't satisfy a fixed shape either
    with pytest.raises(duckdb.ConversionException, match="length"):
        storage.write_part(
            client,
            s3cfg.bucket,
            "mf/S/runs/run_id=r/v/part_2.parquet",
            pa.table({"v": pa.array([[1.0, 2.0]], pa.list_(pa.float32()))}),
            Shaped3,
        )


# --- StepInput: .np() on step inputs ---


def _input(table):
    return storage.StepInput(duckdb.connect().from_arrow(table))


def test_np_scalar_column_keeps_dtype():
    got = _input(pa.table({"x": pa.array([1, 2], pa.int16())})).np("x")
    assert got.dtype == np.int16 and got.tolist() == [1, 2]


def test_np_list_column_stacks():
    inp = _input(pa.table({"v": pa.array([[1.0, 2.0], [3.0, 4.0]], pa.list_(pa.float32()))}))
    got = inp.np("v")
    assert got.shape == (2, 2) and got.dtype == np.float32


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


def test_write_part_accepts_an_input_returned_unchanged(s3cfg):
    # identity step: `def copy(self, raw: Raw) -> Raw: return raw`
    client = s3_client(s3cfg)
    key = "mf/F/runs/run_id=r4/pts/part_0.parquet"
    inp = _input(pa.table({"frame": pa.array([1], pa.int32()), "x": pa.array([2.0], pa.float32())}))
    storage.write_part(client, s3cfg.bucket, key, inp, Frame)
    assert table_from_bytes(get_bytes_or_none(client, s3cfg.bucket, key)).column("x").to_pylist() == [2.0]


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


def test_numpy_flow_end_to_end(s3cfg, repo):
    run_id = SignalFlow.run(s3cfg, repo=repo, use_processes=False)
    con = SignalFlow.duck(s3cfg)
    assert con.sql(f"SELECT n FROM counted WHERE run_id = '{run_id}'").fetchone()[0] == 4
    peaks = con.sql(f"SELECT peak FROM peaks WHERE run_id = '{run_id}'").fetchall()
    assert all(p > 0 for (p,) in peaks)
    # narrow dtypes survived the whole pipeline onto S3 (fixed shape persists as list)
    layout = Layout.for_flow(s3cfg, SignalFlow)
    schema = table_from_bytes(get_bytes_or_none(s3_client(s3cfg), s3cfg.bucket, layout.part(run_id, "spectra"))).schema
    assert schema.field("frame").type == pa.int32()
    assert schema.field("spectrum").type == pa.list_(pa.float32())
    # the drift check compares against the persisted (list) form: no false positive
    from ionflow import discovery

    assert discovery.schema_drift(SignalFlow, s3cfg) == []
