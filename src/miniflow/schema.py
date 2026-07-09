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
    """Column metadata attached as an annotation default (`id: int = pk`)."""

    def __init__(self, *, primary_key=False, check=None, nullable=None):
        self.primary_key = primary_key
        self.check = check
        self.nullable = nullable


pk = _Field(primary_key=True)


def check(expr):
    """CHECK constraint: `score: float = check("score >= 0")`."""
    return _Field(check=expr)


def field(*, primary_key=False, check=None, nullable=None):
    """Full column control for the rare case the shortcuts don't cover."""
    return _Field(primary_key=primary_key, check=check, nullable=nullable)


class Column:
    """A resolved column, built from a class annotation.

    >>> Column(int, primary_key=True).nullable
    False
    """

    def __init__(self, dtype, *, nullable=True, primary_key=False, check=None):
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


def _columns_of(cls, force_nullable=False):
    cols = {}
    for name, hint in typing.get_type_hints(cls).items():
        if name.startswith("_"):
            continue
        base, optional = _strip_optional(hint)
        spec = getattr(cls, name, None)
        spec = spec if isinstance(spec, _Field) else None
        nullable = True if force_nullable else optional
        if spec and spec.nullable is not None:
            nullable = spec.nullable
        cols[name] = Column(
            base,
            nullable=nullable,
            primary_key=bool(spec and spec.primary_key),
            check=spec.check if spec else None,
        )
    return cols


class Table:
    """Declarative table schema; compiles to DuckDB DDL and a pyarrow schema.

    Columns are class annotations: a bare type is `NOT NULL`, `T | None` is
    nullable. Constraints attach as defaults — `= pk`, `= check("...")`, `= field(...)`.

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
    def column_ddls(cls):
        return [c.ddl(n) for n, c in cls._columns.items()]

    @classmethod
    def ddl(cls, name, temp=False):
        kw = "TEMP TABLE" if temp else "TABLE"
        lines = ",\n".join(f"  {line}" for line in cls.column_ddls())
        return f"CREATE {kw} {name} (\n{lines}\n)"

    @classmethod
    def arrow_schema(cls):
        return pa.schema(
            pa.field(n, _ARROW[c.dtype], nullable=c.nullable)
            for n, c in cls._columns.items()
        )


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
        return [
            pa.field(n, _ARROW[c.dtype], nullable=True)
            for n, c in cls._columns.items()
        ]
