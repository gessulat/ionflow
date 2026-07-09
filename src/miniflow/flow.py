from . import query, runner
from .config import Config
from .dag import build_dag


class Flow:
    """Base class. Subclass it; each public method is a step.

    A step's parameter names are its upstream steps; its return annotation is
    the output table schema. No `self.next()`, no scheduler — just an S3 bucket.

    Every subclass registers itself in `Flow._registry` on definition, so the
    TUI can list all flows in a project once their modules are imported.
    """

    _registry: dict[str, type] = {}

    def __init_subclass__(cls, **kw):
        super().__init_subclass__(**kw)
        cls._dag = build_dag(cls)
        Flow._registry[cls.__name__] = cls

    @classmethod
    def run(cls, config=None, repo=".", **kw):
        return runner.run(cls, config or Config.load(), repo=repo, **kw)

    @classmethod
    def resume(cls, origin_run_id, config=None, repo=".", **kw):
        return runner.run(
            cls, config or Config.load(), repo=repo,
            origin_run_id=origin_run_id, **kw,
        )

    @classmethod
    def duck(cls, config=None):
        return query.duck(cls, config or Config.load())

    @classmethod
    def ledger_columns(cls):
        """The flow's declared extra ledger columns ({} if no inner Ledger class)."""
        ledger = getattr(cls, "Ledger", None)
        return ledger.columns() if ledger else {}

    @classmethod
    def ledger_fields(cls):
        """pyarrow fields for the extra ledger columns ([] if none)."""
        ledger = getattr(cls, "Ledger", None)
        return ledger.arrow_fields() if ledger else []
