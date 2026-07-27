import datetime as dt
import types
import typing

import pyarrow as pa

try:  # numpy is optional in core; when installed, np scalar types work as annotations
    import numpy as np
except ModuleNotFoundError:  # pragma: no cover
    np = None  # ty: ignore[invalid-assignment]

_DUCKDB: dict[type, str] = {
    int: "BIGINT",
    float: "DOUBLE",
    str: "VARCHAR",
    bool: "BOOLEAN",
    bytes: "BLOB",
    dt.datetime: "TIMESTAMP",
    dt.date: "DATE",
}

_ARROW: dict[type, pa.DataType] = {
    int: pa.int64(),
    float: pa.float64(),
    str: pa.string(),
    bool: pa.bool_(),
    bytes: pa.binary(),
    dt.datetime: pa.timestamp("us"),
    dt.date: pa.date32(),
}

if np is not None:
    # Narrow numerics matter for measurement data: a float32 point cloud stored
    # as DOUBLE doubles its size on S3 and in every downstream read.
    _NUMPY = {
        np.bool_: ("BOOLEAN", pa.bool_()),
        np.int8: ("TINYINT", pa.int8()),
        np.int16: ("SMALLINT", pa.int16()),
        np.int32: ("INTEGER", pa.int32()),
        np.int64: ("BIGINT", pa.int64()),
        np.uint8: ("UTINYINT", pa.uint8()),
        np.uint16: ("USMALLINT", pa.uint16()),
        np.uint32: ("UINTEGER", pa.uint32()),
        np.uint64: ("UBIGINT", pa.uint64()),
        np.float32: ("FLOAT", pa.float32()),
        np.float64: ("DOUBLE", pa.float64()),
    }
    _DUCKDB |= {t: d for t, (d, _) in _NUMPY.items()}
    _ARROW |= {t: a for t, (_, a) in _NUMPY.items()}


class Shaped:
    """Marker base for fixed-shape array columns; build annotations with `shaped(...)`."""

    dtype: typing.ClassVar
    shape: typing.ClassVar[tuple]


def shaped(dtype, *shape):
    """Fixed-shape array column: `spectrum: shaped(np.float32, 4096)`.

    Compiles to a DuckDB fixed array / arrow fixed_size_list, so a row with
    the wrong number of elements fails the write instead of persisting.
    Parquet has no fixed-size logical type — the shape is enforced at write,
    and reads (views, `.np()`) see an ordinary list column.

    >>> _duckdb_type(shaped(float, 3))
    'DOUBLE[3]'
    >>> _duckdb_type(shaped(float, 4, 3))  # cells are (4, 3) arrays
    'DOUBLE[3][4]'
    """
    if not shape or not all(isinstance(n, int) and n > 0 for n in shape):
        raise TypeError("shaped(dtype, *shape) needs one or more positive int dimensions")
    name = f"shaped_{getattr(dtype, '__name__', dtype)}_{'x'.join(map(str, shape))}"
    return type(name, (Shaped,), {"dtype": dtype, "shape": shape})


def _is_shaped(dtype):
    return isinstance(dtype, type) and issubclass(dtype, Shaped)


def _duckdb_type(dtype):
    """DuckDB type for a scalar, (nested) list, or fixed-shape annotation.

    List elements are always nullable (DuckDB/Arrow semantics), so
    `list[T | None]` is unsupported rather than redundant.

    >>> _duckdb_type(list[float])
    'DOUBLE[]'
    >>> _duckdb_type(list[list[int]])
    'BIGINT[][]'
    >>> import numpy as np
    >>> _duckdb_type(list[np.float32])
    'FLOAT[]'
    """
    if _is_shaped(dtype):  # DuckDB array suffixes read inside-out: innermost first
        return _duckdb_type(dtype.dtype) + "".join(f"[{n}]" for n in reversed(dtype.shape))
    if typing.get_origin(dtype) is list:
        return _duckdb_type(typing.get_args(dtype)[0]) + "[]"
    return _DUCKDB[dtype]


def _persisted_type(dtype):
    """DuckDB type as parquet round-trips it: fixed arrays come back as plain lists."""
    if _is_shaped(dtype):
        return _persisted_type(dtype.dtype) + "[]" * len(dtype.shape)
    if typing.get_origin(dtype) is list:
        return _persisted_type(typing.get_args(dtype)[0]) + "[]"
    return _DUCKDB[dtype]


def _arrow_type(dtype):
    if _is_shaped(dtype):
        t = _arrow_type(dtype.dtype)
        for n in reversed(dtype.shape):
            t = pa.list_(t, n)
        return t
    if typing.get_origin(dtype) is list:
        return pa.list_(_arrow_type(typing.get_args(dtype)[0]))
    return _ARROW[dtype]


class _Field:
    """Column constraints attached as an annotation default (`id: int = primary_key`)."""

    def __init__(self, *, primary_key=False, check=None, fanout_key=False):
        self.primary_key = primary_key
        self.check = check
        self.fanout_key = fanout_key


def field(*, primary_key=False, check=None, fanout_key=False):
    """Combine constraints when the shortcuts (`primary_key`, `check`, `fanout_key`)
    don't suffice, e.g. `field(primary_key=True, fanout_key=True)`."""
    return _Field(primary_key=primary_key, check=check, fanout_key=fanout_key)


def _is_key_dtype(dtype):
    """Fanout keys must round-trip JSON meta exactly: str or integer types only."""
    if dtype in (str, int):
        return True
    return np is not None and isinstance(dtype, type) and issubclass(dtype, np.integer)


primary_key = field(primary_key=True)

# On a `Fanout[...]` step's output table: marks the column that is each fan-out
# item's identity, enabling keyed resume (`run --resume latest`). See
# `Table.fanout_key_column`.
fanout_key = field(fanout_key=True)


def check(expr):
    """CHECK constraint: `score: float = check("score >= 0")`."""
    return field(check=expr)


class Column:
    """A resolved column, built from a class annotation.

    >>> Column(int, primary_key=True).nullable
    False
    """

    def __init__(self, dtype, *, nullable=False, primary_key=False, check=None, fanout_key=False):
        try:
            _duckdb_type(dtype)
        except KeyError:
            raise TypeError(f"unsupported column dtype: {dtype!r}") from None
        if fanout_key and not _is_key_dtype(dtype):
            raise TypeError(f"fanout_key column must be str or an integer type, not {dtype!r}")
        self.dtype = dtype
        self.primary_key = primary_key
        self.nullable = False if primary_key else nullable
        self.check = check
        self.fanout_key = fanout_key
        if fanout_key and self.nullable:
            raise TypeError("a fanout_key column cannot be nullable")

    def duckdb_type(self):
        return _duckdb_type(self.dtype)

    def persisted_type(self):
        """Type as parquet round-trips it: fixed arrays come back as plain lists.

        The write-path cast uses `duckdb_type()` (shape enforced there); views
        and the drift check must compare against what a reader actually sees.

        >>> Column(shaped(float, 3)).persisted_type()
        'DOUBLE[]'
        """
        return _persisted_type(self.dtype)

    def is_vector(self):
        """True when cells are arrays (`list[T]` / `shaped(...)`), not scalars.

        >>> Column(list[float]).is_vector(), Column(float).is_vector()
        (True, False)
        """
        return typing.get_origin(self.dtype) is list or _is_shaped(self.dtype)

    def ddl(self, name):
        parts = [name, self.duckdb_type()]
        if not self.nullable:
            parts.append("NOT NULL")
        if self.primary_key:
            parts.append("PRIMARY KEY")
        if self.check:
            parts.append(f"CHECK ({self.check})")
        return " ".join(parts)


def _strip_optional(hint):
    if typing.get_origin(hint) in (typing.Union, types.UnionType):
        args = [a for a in typing.get_args(hint) if a is not type(None)]
        if len(args) == 1:
            return args[0], True
    return hint, False


def _arrow_fields(columns, nullable=None):
    return [
        pa.field(n, _arrow_type(c.dtype), nullable=c.nullable if nullable is None else nullable)
        for n, c in columns.items()
    ]


def _columns_of(cls, force_nullable=False):
    cols = {}
    for name, hint in typing.get_type_hints(cls).items():
        if typing.get_origin(hint) is typing.ClassVar:
            continue
        base, optional = _strip_optional(hint)
        spec = getattr(cls, name, None)
        spec = spec if isinstance(spec, _Field) else None
        cols[name] = Column(
            base,
            nullable=force_nullable or optional,
            primary_key=bool(spec and spec.primary_key),
            check=spec.check if spec else None,
            fanout_key=bool(spec and spec.fanout_key),
        )
    return cols


class Table:
    """Declarative table schema; compiles to DuckDB DDL and a pyarrow schema.

    Columns are class annotations: a bare type is `NOT NULL`, `T | None` is
    nullable. Constraints attach as defaults — `= primary_key`, `= check("...")`, `= field(...)`.
    Column types are resolved with `typing.get_type_hints`, so they must be
    importable at the schema class's module scope.

    >>> class T(Table):
    ...     a: int = primary_key
    ...     b: str | None
    >>> list(T.columns())
    ['a', 'b']
    >>> T.arrow_schema().field("a").nullable
    False
    """

    def __init_subclass__(cls, **kw):
        super().__init_subclass__(**kw)
        cls._columns = _columns_of(cls)
        if not cls._columns:
            raise TypeError(f"{cls.__name__} declares no columns")
        if len([n for n, c in cls._columns.items() if c.fanout_key]) > 1:
            raise TypeError(f"{cls.__name__}: only one column may be the fanout_key")

    @classmethod
    def columns(cls):
        return cls._columns

    @classmethod
    def fanout_key_column(cls):
        """Name of the column marked `= fanout_key`, or None.

        On a `Fanout[...]` step's output table, this column is each item's
        stable identity (e.g. source file path + etag): every item must hold
        exactly one distinct value, and resume matches branch outputs across
        runs by that value instead of by list position — added items compute,
        matching items are copied from the origin run.

        >>> class Item(Table):
        ...     path: str = fanout_key
        ...     size: int
        >>> Item.fanout_key_column()
        'path'
        """
        return next((n for n, c in cls._columns.items() if c.fanout_key), None)

    @classmethod
    def persisted_ddl(cls):
        """Column DDL as a reader sees it: persisted types, NOT NULL only.

        What DuckLake tables (catalog and publish lake) are created from —
        PK/CHECK stay ionflow-side (0003).

        >>> Table.make("T", {"x": int, "s": str | None}).persisted_ddl()
        'x BIGINT NOT NULL, s VARCHAR'
        """
        return ", ".join(
            f"{n} {c.persisted_type()}{'' if c.nullable else ' NOT NULL'}" for n, c in cls._columns.items()
        )

    @classmethod
    def ddl(cls, name, temp=False):
        kw = "TEMP TABLE" if temp else "TABLE"
        lines = ",\n".join(f"  {c.ddl(n)}" for n, c in cls._columns.items())
        return f"CREATE {kw} {name} (\n{lines}\n)"

    @classmethod
    def arrow_schema(cls):
        return pa.schema(_arrow_fields(cls._columns))

    @classmethod
    def cast_sql(cls, source):
        """SELECT casting `source` to this schema (extra columns dropped).

        >>> class C(Table):
        ...     a: int
        >>> C.cast_sql("_src")
        'SELECT CAST(a AS BIGINT) AS a FROM _src'
        """
        cols = ", ".join(f"CAST({n} AS {c.duckdb_type()}) AS {n}" for n, c in cls._columns.items())
        return f"SELECT {cols} FROM {source}"

    @classmethod
    def violation_scans(cls):
        """(label, count-of-violations SQL) per constraint, for one scan.

        A CHECK passes on NULL (SQL semantics), so violations are `(expr) IS
        FALSE`; PRIMARY KEY NULLs are caught by its NOT NULL, so DISTINCT-
        counting handles uniqueness alone (the row-value form covers composite
        keys).

        >>> class V(Table):
        ...     id: int = primary_key
        ...     score: float | None = check("score >= 0")
        >>> [label for label, _ in V.violation_scans()]
        ['id NOT NULL', 'CHECK (score >= 0)', 'PRIMARY KEY (id)']
        """
        scans = []
        for n, c in cls._columns.items():
            if not c.nullable:
                scans.append((f"{n} NOT NULL", f"count(*) FILTER (WHERE {n} IS NULL)"))
            if c.check:
                scans.append((f"CHECK ({c.check})", f"count(*) FILTER (WHERE ({c.check}) IS FALSE)"))
        if pk := [n for n, c in cls._columns.items() if c.primary_key]:
            cols = ", ".join(pk)
            scans.append((f"PRIMARY KEY ({cols})", f"count(*) - count(DISTINCT ({cols}))"))
        return scans

    @classmethod
    def make(cls, name, cols, **constraints):
        """Build a Table subclass programmatically (wide/generated schemas).

        `cols` maps column name -> type object (real types, not strings —
        there is no module scope to resolve forward references against);
        `constraints` attach per column the way defaults do in a class body.

        >>> Wide = Table.make("Wide", {"id": int, "f0": float | None}, id=primary_key)
        >>> Wide.columns()["id"].primary_key
        True
        >>> Wide.arrow_schema().field("f0").nullable
        True
        """
        if unknown := set(constraints) - set(cols):
            raise TypeError(f"constraints for unknown columns: {sorted(unknown)}")
        return type(name, (cls,), {"__annotations__": dict(cols), **constraints})


class _ColumnSet:
    """Shared accessors for annotation-declared column sets (LedgerColumns, FlowParams)."""

    _columns: typing.ClassVar[dict] = {}

    @classmethod
    def columns(cls):
        return cls._columns

    @classmethod
    def arrow_fields(cls):
        return _arrow_fields(cls._columns)


class LedgerColumns(_ColumnSet):
    """Per-flow ledger columns, populated during a run (all nullable).

    >>> class L(LedgerColumns):
    ...     auc: float
    >>> L.arrow_fields()[0].name
    'auc'
    """

    def __init_subclass__(cls, **kw):
        super().__init_subclass__(**kw)
        cls._columns = _columns_of(cls, force_nullable=True)


class FlowParams(_ColumnSet):
    """Typed run inputs: annotations declare types, class values are defaults.

    A bare type is required; a default (or `T | None`) makes it optional.
    Only int/float/str/bool — params must round-trip JSON meta, ledger
    parquet, and `-p key=value` CLI flags.

    >>> class P(FlowParams):
    ...     dataset: str
    ...     batch_size: int = 32
    >>> P.resolve({"dataset": "d1"})
    {'dataset': 'd1', 'batch_size': 32}
    >>> P.resolve({})
    Traceback (most recent call last):
        ...
    ValueError: missing required param: dataset
    """

    def __init_subclass__(cls, **kw):
        super().__init_subclass__(**kw)
        cls._columns = _columns_of(cls)
        for name, col in cls._columns.items():
            if col.dtype not in (int, float, str, bool):
                raise TypeError(f"param '{name}': only int/float/str/bool params are supported")

    @classmethod
    def arrow_fields(cls):
        """Ledger fields, all nullable: older runs predate any given param."""
        return _arrow_fields(cls._columns, nullable=True)

    @classmethod
    def resolve(cls, given=None):
        """Merge `given` over the defaults; validate names, presence, and types."""
        given = given or {}
        if unknown := set(given) - set(cls._columns):
            raise ValueError(f"unknown params: {sorted(unknown)}")
        out = {}
        for name, col in cls._columns.items():
            if name in given:
                v = given[name]
            elif hasattr(cls, name):
                v = getattr(cls, name)
            else:
                raise ValueError(f"missing required param: {name}")
            if col.dtype is float and isinstance(v, int):
                v = float(v)
            if v is None and not col.nullable:
                raise TypeError(f"param '{name}' may not be None")
            if v is not None and not isinstance(v, col.dtype):
                raise TypeError(f"param '{name}': expected {col.dtype.__name__}, got {type(v).__name__}")
            out[name] = v
        return out
