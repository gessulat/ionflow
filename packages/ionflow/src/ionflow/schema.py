import datetime as dt
import types
import typing

import pyarrow as pa

try:  # numpy is optional in core; when installed, np scalar types work as annotations
    import numpy as np
except ModuleNotFoundError:  # pragma: no cover
    np = None  # ty: ignore[invalid-assignment]

_LEAF: dict[type, pa.DataType] = {
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
    # as DOUBLE doubles its size on S3 and in every downstream read. Listed
    # explicitly, not scanned off numpy: np.float16 would arrive as arrow
    # halffloat, which has no DuckDB spelling in _SQL.
    _NUMPY = (np.bool_, np.float32, np.float64) + tuple(
        getattr(np, f"{sign}int{bits}") for sign in ("", "u") for bits in (8, 16, 32, 64)
    )
    _LEAF |= {t: pa.from_numpy_dtype(t) for t in _NUMPY}

# Arrow leaf -> DuckDB name. Keyed by arrow type, not by annotation, so the
# numpy widths need no entries of their own (np.float32 -> pa.float32() -> FLOAT).
_SQL: dict[pa.DataType, str] = {
    pa.int64(): "BIGINT",
    pa.float64(): "DOUBLE",
    pa.string(): "VARCHAR",
    pa.bool_(): "BOOLEAN",
    pa.binary(): "BLOB",
    pa.timestamp("us"): "TIMESTAMP",
    pa.date32(): "DATE",
    pa.int8(): "TINYINT",
    pa.int16(): "SMALLINT",
    pa.int32(): "INTEGER",
    pa.uint8(): "UTINYINT",
    pa.uint16(): "USMALLINT",
    pa.uint32(): "UINTEGER",
    pa.uint64(): "UBIGINT",
    pa.float32(): "FLOAT",
}


class Shaped:
    """Marker base for fixed-shape array columns; build annotations with `shaped(...)`."""

    dtype: typing.ClassVar
    shape: typing.ClassVar[tuple]


def shaped(dtype, *shape):
    """Fixed-shape array column: `spectrum: shaped(np.float32, 4096)`.

    Compiles to a DuckDB fixed array, so a row with the wrong number of elements
    fails the write. Parquet has no fixed-size logical type — the shape is
    enforced at write, and reads (views, `.np()`) see an ordinary list column.

    >>> Column(shaped(float, 3)).duckdb_type()
    'DOUBLE[3]'
    >>> Column(shaped(float, 4, 3)).duckdb_type()  # cells are (4, 3) arrays
    'DOUBLE[3][4]'
    """
    if not shape or not all(isinstance(n, int) and n > 0 for n in shape):
        raise TypeError("shaped(dtype, *shape) needs one or more positive int dimensions")
    name = f"shaped_{getattr(dtype, '__name__', dtype)}_{'x'.join(map(str, shape))}"
    return type(name, (Shaped,), {"dtype": dtype, "shape": shape})


def _q(name):
    """Quote a column name so any identifier survives interpolation into SQL.

    Column names are user data — scientific schemas carry things like
    `'b 1 +1'` — so every generated statement quotes them. Embedded quotes are
    doubled, which is how SQL escapes them inside a quoted identifier.

    >>> _q("plain"), _q("b 1 +1")
    ('"plain"', '"b 1 +1"')
    >>> _q('we"ird')
    '"we""ird"'
    """
    return '"' + str(name).replace('"', '""') + '"'


def _is_shaped(dtype):
    return isinstance(dtype, type) and issubclass(dtype, Shaped)


def _arrow(dtype) -> pa.DataType:
    """Arrow type for a scalar, (nested) list, or fixed-shape annotation.

    List elements are always nullable (DuckDB/Arrow semantics), so
    `list[T | None]` is unsupported rather than redundant. Raises KeyError on an
    unknown leaf — `Column` turns that into a TypeError naming the annotation.
    """
    if _is_shaped(dtype):  # arrow nests innermost dimension first
        t = _arrow(dtype.dtype)
        for n in reversed(dtype.shape):
            t = pa.list_(t, n)
        return t
    if typing.get_origin(dtype) is list:
        return pa.list_(_arrow(typing.get_args(dtype)[0]))
    return _LEAF[dtype]


def _sql(t: pa.DataType) -> str:
    """DuckDB type text for an arrow type; array suffixes read inside-out.

    >>> _sql(_arrow(list[list[int]]))
    'BIGINT[][]'
    >>> _sql(_arrow(shaped(float, 4, 3)))
    'DOUBLE[3][4]'
    """
    if pa.types.is_fixed_size_list(t):
        return _sql(t.value_type) + f"[{t.list_size}]"
    if pa.types.is_list(t):
        return _sql(t.value_type) + "[]"
    return _SQL[t]


def _persist(t: pa.DataType) -> pa.DataType:
    """Arrow type as parquet round-trips it: fixed-size lists come back plain,
    at any nesting depth.

    >>> _sql(_persist(_arrow(list[shaped(float, 3)])))
    'DOUBLE[][]'
    """
    if pa.types.is_list(t) or pa.types.is_fixed_size_list(t):
        return pa.list_(_persist(t.value_type))
    return t


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

# On a `Fanout[...]` step's output table: marks each item's identity column,
# enabling keyed resume (`run --resume latest`). See `Table.fanout_key_column`.
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
            _arrow(dtype)  # validate now; the type itself stays derived from .dtype
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

    @property
    def arrow_type(self):
        """Declared arrow type — derived, never cached: the drift check mutates `.dtype`."""
        return _arrow(self.dtype)

    def duckdb_type(self):
        return _sql(self.arrow_type)

    def persisted_type(self):
        """Type as parquet round-trips it: fixed arrays come back as plain lists.

        The write-path cast uses `duckdb_type()` (shape enforced there); views
        and the drift check must compare against what a reader actually sees.

        >>> Column(shaped(float, 3)).persisted_type()
        'DOUBLE[]'
        """
        return _sql(_persist(self.arrow_type))

    def is_vector(self):
        """True when cells are arrays (`list[T]` / `shaped(...)`), not scalars.

        >>> Column(list[float]).is_vector(), Column(float).is_vector()
        (True, False)
        """
        t = self.arrow_type
        return pa.types.is_list(t) or pa.types.is_fixed_size_list(t)

    def ddl(self, name):
        parts = [_q(name), self.duckdb_type()]
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


class _ColumnSet:
    """An ordered set of typed columns declared as class annotations — shared by
    `Table` (a step's output schema) and the flow-level sets that carry no table
    semantics (`LedgerColumns`, `FlowParams`). `base=True` marks the intermediate
    classes themselves, whose bodies are not a user's declaration.
    """

    # Subclass behaviour, read by __init_subclass__ and inherited (so `make`'s
    # generated subclasses get it too). Underscored: these share a namespace
    # with column names.
    _columns: typing.ClassVar[dict] = {}
    _force_nullable: typing.ClassVar[bool] = False  # LedgerColumns: every column nullable
    _scalars_only: typing.ClassVar[bool] = False  # FlowParams: int/float/str/bool only
    _require_columns: typing.ClassVar[bool] = True  # a step output with no columns is a typo

    def __init_subclass__(cls, base=False, **kw):
        super().__init_subclass__(**kw)
        if base:
            return
        cls._columns = _columns_of(cls, force_nullable=cls._force_nullable)
        if cls._require_columns and not cls._columns:
            raise TypeError(f"{cls.__name__} declares no columns")
        if len([n for n, c in cls._columns.items() if c.fanout_key]) > 1:
            raise TypeError(f"{cls.__name__}: only one column may be the fanout_key")
        if cls._scalars_only:
            for name, col in cls._columns.items():
                if col.dtype not in (int, float, str, bool):
                    raise TypeError(f"param '{name}': only int/float/str/bool params are supported")

    @classmethod
    def columns(cls):
        return cls._columns

    @classmethod
    def arrow_fields(cls):
        return [pa.field(n, c.arrow_type, nullable=c.nullable) for n, c in cls._columns.items()]

    @classmethod
    def arrow_schema(cls):
        return pa.schema(cls.arrow_fields())


class Table(_ColumnSet, base=True):
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

    @classmethod
    def fanout_key_column(cls):
        """Name of the column marked `= fanout_key`, or None.

        Resume matches branch outputs across runs by this column's value instead
        of by list position, so every item must hold exactly one distinct value.

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

        What DuckLake tables are created from — PK/CHECK stay ionflow-side (0003).

        >>> Table.make("T", {"x": int, "s": str | None}).persisted_ddl()
        '"x" BIGINT NOT NULL, "s" VARCHAR'
        """
        return ", ".join(
            f"{_q(n)} {c.persisted_type()}{'' if c.nullable else ' NOT NULL'}" for n, c in cls._columns.items()
        )

    @classmethod
    def ddl(cls, name, temp=False):
        kw = "TEMP TABLE" if temp else "TABLE"
        lines = ",\n".join(f"  {c.ddl(n)}" for n, c in cls._columns.items())
        return f"CREATE {kw} {name} (\n{lines}\n)"

    @classmethod
    def empty(cls, con=None):
        """Typed, zero-row stand-in for a table with no parts: SQL text, or a relation on `con`.

        Always the persisted flavour, so an empty fan-out, an empty view and
        every non-empty read agree on types.

        >>> Table.make("T", {"x": int}).empty()
        'SELECT NULL::BIGINT AS "x" WHERE false'
        """
        fields = [pa.field(n, _persist(c.arrow_type), nullable=c.nullable) for n, c in cls._columns.items()]
        if con is not None:
            return con.from_arrow(pa.schema(fields).empty_table())
        cols = ", ".join(f"NULL::{_sql(f.type)} AS {_q(f.name)}" for f in fields)
        return f"SELECT {cols} WHERE false"

    @classmethod
    def cast_sql(cls, source):
        """SELECT casting `source` to this schema (extra columns dropped).

        >>> class C(Table):
        ...     a: int
        >>> C.cast_sql("_src")
        'SELECT CAST("a" AS BIGINT) AS "a" FROM _src'
        """
        cols = ", ".join(f"CAST({_q(n)} AS {c.duckdb_type()}) AS {_q(n)}" for n, c in cls._columns.items())
        return f"SELECT {cols} FROM {source}"

    @classmethod
    def violation_scans(cls):
        """(label, count-of-violations SQL) per constraint, for one scan.

        A CHECK passes on NULL (SQL semantics), so violations are `(expr) IS
        FALSE`; a PRIMARY KEY's NULLs are its NOT NULL's job, leaving DISTINCT-
        counting to cover uniqueness alone (row-value form: composite keys).

        >>> class V(Table):
        ...     id: int = primary_key
        ...     score: float | None = check("score >= 0")
        >>> [label for label, _ in V.violation_scans()]
        ['id NOT NULL', 'CHECK (score >= 0)', 'PRIMARY KEY (id)']
        """
        scans = []
        for n, c in cls._columns.items():
            if not c.nullable:
                scans.append((f"{n} NOT NULL", f"count(*) FILTER (WHERE {_q(n)} IS NULL)"))
            if c.check:
                scans.append((f"CHECK ({c.check})", f"count(*) FILTER (WHERE ({c.check}) IS FALSE)"))
        if pk := [n for n, c in cls._columns.items() if c.primary_key]:
            quoted = ", ".join(_q(n) for n in pk)
            scans.append((f"PRIMARY KEY ({', '.join(pk)})", f"count(*) - count(DISTINCT ({quoted}))"))
        return scans

    @classmethod
    def make(cls, name, cols, **constraints):
        """Build a Table subclass programmatically (wide/generated schemas).

        `cols` maps name -> a real type object, never a string (there is no module
        scope to resolve forward references against); `constraints` attach per
        column the way defaults do in a class body.

        >>> Wide = Table.make("Wide", {"id": int, "f0": float | None}, id=primary_key)
        >>> Wide.columns()["id"].primary_key
        True
        >>> Wide.arrow_schema().field("f0").nullable
        True
        """
        if unknown := set(constraints) - set(cols):
            raise TypeError(f"constraints for unknown columns: {sorted(unknown)}")
        return type(name, (cls,), {"__annotations__": dict(cols), **constraints})


class LedgerColumns(_ColumnSet, base=True):
    """Per-flow ledger columns, populated during a run (all nullable).

    >>> class L(LedgerColumns):
    ...     auc: float
    >>> L.arrow_fields()[0].name
    'auc'
    """

    _force_nullable: typing.ClassVar[bool] = True
    _require_columns: typing.ClassVar[bool] = False  # an empty declaration is legal here


class FlowParams(_ColumnSet, base=True):
    """Typed run inputs: annotations declare types, class values are defaults.

    A bare type is required, a default (or `T | None`) optional. Only
    int/float/str/bool: params round-trip JSON meta, ledger parquet and `-p k=v`.

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

    _scalars_only: typing.ClassVar[bool] = True
    _require_columns: typing.ClassVar[bool] = False  # an empty declaration is legal here

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
