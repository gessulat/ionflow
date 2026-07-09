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
        if dtype not in _DUCKDB:
            raise TypeError(f"unsupported column dtype: {dtype!r}")
        self.dtype = dtype
        self.primary_key = primary_key
        self.nullable = False if primary_key else nullable
        self.check = check

    def duckdb_type(self):
        return _DUCKDB[self.dtype]

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


def _arrow_fields(columns):
    return [pa.field(n, _ARROW[c.dtype], nullable=c.nullable) for n, c in columns.items()]


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
