import datetime as dt
import types
import typing

import pyarrow as pa

_DUCKDB = {
    int: "BIGINT",
    float: "DOUBLE",
    str: "VARCHAR",
    bool: "BOOLEAN",
    bytes: "BLOB",
    dt.datetime: "TIMESTAMP",
    dt.date: "DATE",
}

_ARROW = {
    int: pa.int64(),
    float: pa.float64(),
    str: pa.string(),
    bool: pa.bool_(),
    bytes: pa.binary(),
    dt.datetime: pa.timestamp("us"),
    dt.date: pa.date32(),
}


def _duckdb_type(dtype):
    """DuckDB type for a scalar or (nested) list annotation.

    List elements are always nullable (DuckDB/Arrow semantics), so
    `list[T | None]` is unsupported rather than redundant.

    >>> _duckdb_type(list[float])
    'DOUBLE[]'
    >>> _duckdb_type(list[list[int]])
    'BIGINT[][]'
    """
    if typing.get_origin(dtype) is list:
        return _duckdb_type(typing.get_args(dtype)[0]) + "[]"
    return _DUCKDB[dtype]


def _arrow_type(dtype):
    if typing.get_origin(dtype) is list:
        return pa.list_(_arrow_type(typing.get_args(dtype)[0]))
    return _ARROW[dtype]


class _Field:
    """Column constraints attached as an annotation default (`id: int = pk`)."""

    def __init__(self, *, primary_key=False, check=None):
        self.primary_key = primary_key
        self.check = check


def field(*, primary_key=False, check=None):
    """Combine constraints when the shortcuts don't suffice."""
    return _Field(primary_key=primary_key, check=check)


pk = field(primary_key=True)


def check(expr):
    """CHECK constraint: `score: float = check("score >= 0")`."""
    return field(check=expr)


class Column:
    """A resolved column, built from a class annotation.

    >>> Column(int, primary_key=True).nullable
    False
    """

    def __init__(self, dtype, *, nullable=False, primary_key=False, check=None):
        try:
            _duckdb_type(dtype)
        except KeyError:
            raise TypeError(f"unsupported column dtype: {dtype!r}") from None
        self.dtype = dtype
        self.primary_key = primary_key
        self.nullable = False if primary_key else nullable
        self.check = check

    def duckdb_type(self):
        return _duckdb_type(self.dtype)

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
        )
    return cols


class Table:
    """Declarative table schema; compiles to DuckDB DDL and a pyarrow schema.

    Columns are class annotations: a bare type is `NOT NULL`, `T | None` is
    nullable. Constraints attach as defaults — `= pk`, `= check("...")`, `= field(...)`.
    Column types are resolved with `typing.get_type_hints`, so they must be
    importable at the schema class's module scope.

    >>> class T(Table):
    ...     a: int = pk
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

    @classmethod
    def columns(cls):
        return cls._columns

    @classmethod
    def ddl(cls, name, temp=False):
        kw = "TEMP TABLE" if temp else "TABLE"
        lines = ",\n".join(f"  {c.ddl(n)}" for n, c in cls._columns.items())
        return f"CREATE {kw} {name} (\n{lines}\n)"

    @classmethod
    def arrow_schema(cls):
        return pa.schema(_arrow_fields(cls._columns))

    @classmethod
    def make(cls, name, cols, **constraints):
        """Build a Table subclass programmatically (wide/generated schemas).

        `cols` maps column name -> type object (real types, not strings —
        there is no module scope to resolve forward references against);
        `constraints` attach per column the way defaults do in a class body.

        >>> Wide = Table.make("Wide", {"id": int, "f0": float | None}, id=pk)
        >>> Wide.columns()["id"].primary_key
        True
        >>> Wide.arrow_schema().field("f0").nullable
        True
        """
        if unknown := set(constraints) - set(cols):
            raise TypeError(f"constraints for unknown columns: {sorted(unknown)}")
        return type(name, (cls,), {"__annotations__": dict(cols), **constraints})


class LedgerColumns:
    """Per-flow ledger columns, populated during a run (all nullable).

    >>> class L(LedgerColumns):
    ...     auc: float
    >>> L.arrow_fields()[0].name
    'auc'
    """

    def __init_subclass__(cls, **kw):
        super().__init_subclass__(**kw)
        cls._columns = _columns_of(cls, force_nullable=True)

    @classmethod
    def columns(cls):
        return cls._columns

    @classmethod
    def arrow_fields(cls):
        return _arrow_fields(cls._columns)


class FlowParams:
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
    def columns(cls):
        return cls._columns

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
                raise TypeError(
                    f"param '{name}': expected {col.dtype.__name__}, got {type(v).__name__}"
                )
            out[name] = v
        return out
