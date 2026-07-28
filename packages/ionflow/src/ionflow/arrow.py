import io

import pyarrow as pa
import pyarrow.parquet as pq

try:  # numpy is optional in core; step returns convert when it's installed
    import numpy as np
except ModuleNotFoundError:  # pragma: no cover
    np = None  # ty: ignore[invalid-assignment]

BATCH_ROWS = 122880  # arrow reader batch = one parquet row group


def _column_array(name, value):
    """Arrow-compatible column from one value; 2D+ ndarrays nest as fixed-size lists.

    NaN is a value, not NULL — a float column full of NaNs passes NOT NULL.
    """
    if np is None or not isinstance(value, np.ndarray):
        return value  # pa.table handles lists, arrow arrays, 1D ndarrays
    if value.dtype.kind == "c":
        raise TypeError(f"column '{name}': complex arrays have no parquet type; store np.real/np.imag as two columns")
    if value.ndim <= 1:
        return value
    arr = pa.array(np.ascontiguousarray(value).reshape(-1))
    for size in value.shape[:0:-1]:  # wrap innermost dimension first
        arr = pa.FixedSizeListArray.from_arrays(arr, size)
    return arr


def _ndarray_table(arr, table):
    """Table from a bare ndarray, taking column names from the declared schema.

    Structured arrays carry their own names/dtypes; a plain array maps onto a
    single declared column, or positionally onto k columns for an (N, k) array.
    """
    if arr.dtype.names:
        return pa.table({n: _column_array(n, arr[n]) for n in arr.dtype.names})
    cols = table.columns()
    names = list(cols)
    if len(names) == 1 and (arr.ndim == 1 or cols[names[0]].is_vector()):
        return pa.table({names[0]: _column_array(names[0], arr)})
    if arr.ndim == 2 and arr.shape[1] == len(names):
        return pa.table({n: _column_array(n, arr[:, i]) for i, n in enumerate(names)})
    raise TypeError(
        f"cannot map a {arr.shape} ndarray onto {table.__name__}'s columns "
        f"({', '.join(names)}); return a dict of named arrays instead"
    )


def to_arrow(obj, table=None) -> pa.Table:
    """Materialize a step return value into a pyarrow Table.

    Accepts a pyarrow Table, a dict of columns (ndarrays — 2D+ become
    fixed-size-list columns — lists, or arrow arrays), a numpy structured or bare
    array (mapped onto `table`'s declared columns), or a polars/pandas DataFrame;
    numpy dtypes are preserved. DuckDB relations never reach here — `write_part`
    streams them.

    >>> import numpy as np
    >>> to_arrow({"x": np.arange(2, dtype=np.int16)}).schema.types
    [DataType(int16)]
    >>> str(to_arrow({"v": np.zeros((2, 3), dtype=np.float32)}).schema.types[0])
    'fixed_size_list<item: float>[3]'
    """
    if isinstance(obj, pa.Table):
        return obj
    if np is not None and isinstance(obj, np.ndarray):
        return _ndarray_table(obj, table)
    if isinstance(obj, dict):
        return pa.table({n: _column_array(n, v) for n, v in obj.items()})
    if hasattr(obj, "to_arrow"):  # polars
        return obj.to_arrow()
    if hasattr(obj, "__dataframe__"):  # pandas
        return pa.Table.from_pandas(obj)
    raise TypeError(f"cannot convert {type(obj)} to a table")


def parquet_bytes(table):
    """Serialize a pyarrow Table to parquet bytes — the repo's one spelling of it."""
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def table_from_bytes(blob):
    return pq.read_table(io.BytesIO(blob))


def _np_column(name, arr):
    """ndarray from an arrow (chunked) array; list columns stack to (N, d, ...)."""
    if isinstance(arr, pa.ChunkedArray):
        arr = arr.combine_chunks()
    if arr.null_count:
        raise ValueError(
            f"column '{name}' has NULLs, which have no ndarray slot; "
            "filter them out first (or materialize with .to_arrow_table())"
        )
    t = arr.type
    if pa.types.is_fixed_size_list(t):
        d = t.list_size
    elif pa.types.is_list(t) or pa.types.is_large_list(t):
        lengths = arr.value_lengths().to_numpy(zero_copy_only=False)
        d = int(lengths[0]) if len(lengths) else 0
        if (lengths != d).any():
            raise ValueError(
                f"column '{name}' is ragged (list lengths {lengths.min()}..{lengths.max()}); "
                ".np() needs a rectangular column"
            )
    else:
        return arr.to_numpy(zero_copy_only=False)
    inner = _np_column(name, arr.flatten())
    return inner.reshape(len(arr), d, *inner.shape[1:])


class StepInput:
    """A step's input: the lazy DuckDB relation plus a numpy accessor.

    Relation methods (`.filter`, `.aggregate`, `.project`, `.df()`, ...) delegate
    through and return plain relations. `.np()` is the one addition, and it
    MATERIALIZES what it touches — filter/project first when the input is large.
    """

    def __init__(self, rel):
        self.rel = rel

    def __getattr__(self, name):
        return getattr(self.rel, name)

    def np(self, column=None):
        """One column as an ndarray, or every column as {name: ndarray}.

        Scalar columns come back 1D with their stored dtype (FLOAT -> float32);
        rectangular list columns stack to (N, d, ...). Ragged or NULL-bearing
        columns raise rather than degrade to object arrays.
        """
        if np is None:
            raise ModuleNotFoundError("numpy is required for .np(); pip install numpy")
        if column is None:  # one scan for all columns, not one pipeline per column
            tbl = self.rel.to_arrow_table()
            return {n: _np_column(n, tbl.column(i)) for i, n in enumerate(tbl.column_names)}
        arr = self.rel.project(f'"{column}"').to_arrow_table().column(0)
        return _np_column(column, arr)


def read_parquet_sql(glob_uri):
    """SELECT over a glob, with hive `run_id=` partitioning surfaced as a column."""
    return f"SELECT * FROM read_parquet('{glob_uri}', hive_partitioning=true, union_by_name=true)"
