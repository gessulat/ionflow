import importlib
import pkgutil

from . import query, runner
from .config import Config
from .dag import build_dag
from .lake import publish as _publish
from .space import BASE_COLS


class Flow:
    """Base class. Subclass it; each public method is a step.

    A step's parameter names are its upstream steps; its return annotation is
    the output table schema. No `self.next()`, no scheduler — just an S3 bucket.

    During a run, `self.config` is the active Config, so steps can reach the
    bucket for side reads (see "side inputs" in the README).

    Every subclass registers itself in `Flow._registry` on definition (keyed
    by `flow_id()`), so the TUI can list all flows in a project once their
    modules are imported.
    """

    _registry: dict[str, type] = {}
    config = None  # bound by the runner (orchestrator and branch workers)
    Ledger = None  # subclass: `class Ledger(LedgerColumns)` — extra ledger columns
    Params = None  # subclass: `class Params(FlowParams)` — typed run inputs

    def __init_subclass__(cls, **kw):
        super().__init_subclass__(**kw)
        cls._dag = build_dag(cls)
        taken = set(cls._dag.steps) | set(cls.ledger_columns()) | BASE_COLS | {"config"}
        if collisions := set(cls.params_columns()) & taken:
            raise TypeError(f"params collide with steps/ledger columns/config: {sorted(collisions)}")
        Flow._registry[cls.flow_id()] = cls

    @classmethod
    def flow_id(cls):
        """Durable identity: module-qualified class name — the S3 namespace and registry key.

        Renaming a flow class OR moving it to another module makes it a new
        flow (old runs stay under the old id). Flows defined in `__main__` or
        a REPL get an unstable id; define flows in importable modules.
        """
        return f"{cls.__module__}.{cls.__name__}"

    @classmethod
    def run(cls, config=None, repo=".", **kw):
        """Execute the flow; `resume=<run_id>|"latest"` reuses an origin run's steps."""
        return runner.run(cls, config or Config.load(), repo=repo, **kw)

    @classmethod
    def duck(cls, config=None):
        return query.duck(cls, config or Config.load())

    @classmethod
    def publish(cls, run_id, step, table, config=None, where=None, dest=None):
        """Bless `step`'s output from `run_id` as `table` in the publish DuckLake."""
        return _publish(cls, config or Config.load(), run_id, step, table, where=where, dest=dest)

    @classmethod
    def ledger_columns(cls):
        """The flow's declared extra ledger columns ({} if no inner Ledger class)."""
        return cls.Ledger.columns() if cls.Ledger else {}

    @classmethod
    def params_columns(cls):
        """The flow's declared params ({} if no inner Params class)."""
        return cls.Params.columns() if cls.Params else {}

    @classmethod
    def custom_ledger_fields(cls):
        """pyarrow ledger fields beyond the base schema: params, then Ledger columns."""
        return (cls.Params.arrow_fields() if cls.Params else []) + (cls.Ledger.arrow_fields() if cls.Ledger else [])

    @classmethod
    def resolve_params(cls, given=None):
        """Full param dict from `given` + defaults; {} for flows without Params."""
        if cls.Params is None:
            if given:
                raise ValueError(f"{cls.__name__} declares no Params")
            return {}
        return cls.Params.resolve(given)


def discover_flows(package):
    """Import `package` and all its submodules, returning its {flow_id: Flow subclass}.

    Registration is a side effect of importing (see Flow.__init_subclass__), so
    this walks the whole package to catch flows split across modules, then keeps
    only the flows defined under `package`. A TUI launch is a fresh process, so
    the registry reflects the current code with no stale entries.
    """
    mod = importlib.import_module(package)
    for info in pkgutil.walk_packages(getattr(mod, "__path__", []), mod.__name__ + "."):
        importlib.import_module(info.name)
    return {
        name: cls
        for name, cls in Flow._registry.items()
        if cls.__module__ == package or cls.__module__.startswith(package + ".")
    }
