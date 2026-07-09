import datetime as dt

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


class Column:
    """A typed table column.

    >>> Column(int, primary_key=True).nullable
    False
    >>> Column(str).nullable
    True
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


class Table:
    """Declarative table schema; compiles to DuckDB DDL and a pyarrow schema.

    >>> class T(Table):
    ...     a = Column(int, primary_key=True)
    ...     b = Column(str)
    >>> list(T.columns())
    ['a', 'b']
    >>> T.arrow_schema().names
    ['a', 'b']
    """

    _columns: dict

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


def _columns_of(cls):
    # vars(cls) preserves class-body definition order.
    return {k: v for k, v in vars(cls).items() if isinstance(v, Column)}


class LedgerColumns:
    """Extra, per-flow ledger columns to be populated during a run.

    >>> class L(LedgerColumns):
    ...     auc = Column(float)
    >>> L.arrow_fields()[0].name
    'auc'
    """

    _columns: dict = {}

    def __init_subclass__(cls, **kw):
        super().__init_subclass__(**kw)
        cls._columns = _columns_of(cls)

    @classmethod
    def columns(cls):
        return cls._columns

    @classmethod
    def arrow_fields(cls):
        return [
            pa.field(n, _ARROW[c.dtype], nullable=True)
            for n, c in cls._columns.items()
        ]
