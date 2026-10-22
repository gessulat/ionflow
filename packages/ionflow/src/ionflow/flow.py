import importlib
import pkgutil
from contextlib import AbstractContextManager

from . import query, runner
from .config import Config
from .dag import build_dag
from .lake import publish as _publish
from .provenance import ArtifactRuntime
from .state import BASE_COLS


class Flow:
    """Base class. Subclass it; each public method is a step.

    A step's parameter names are its upstream steps; its return annotation is
    the output table or artifact schema. No `self.next()`, no scheduler — just a store,
    which steps can reach through `self.config` (see "side inputs" in the README).
    """

    _registry: dict[str, type] = {}
    config = None  # bound by the runner (orchestrator and branch workers)
    artifacts: ArtifactRuntime  # step-local tracked artifact operations
    Ledger = None  # subclass: `class Ledger(LedgerColumns)` — extra ledger columns
    Params = None  # subclass: `class Params(FlowParams)` — typed run inputs
    Inputs = None  # subclass: `class Inputs(ArtifactInputs)` — typed artifact inputs

    def __init_subclass__(cls, **kw):
        super().__init_subclass__(**kw)
        cls._dag = build_dag(cls)
        if collisions := set(cls.ledger_columns()) & {"local", "s3"}:
            raise TypeError(f"reserved ledger availability columns: {sorted(collisions)}")
        taken = set(cls._dag.steps) | set(cls.ledger_columns()) | BASE_COLS | {"config", "artifacts", "phase"}
        if cls.Inputs:
            if collisions := set(cls.Inputs.declarations()) & (taken | set(cls.params_columns())):
                raise TypeError(f"artifact inputs collide with steps/params/ledger: {sorted(collisions)}")
        if collisions := set(cls.params_columns()) & taken:
            raise TypeError(f"params collide with steps/ledger columns/config: {sorted(collisions)}")
        Flow._registry[cls.flow_id()] = cls

    @classmethod
    def flow_id(cls):
        """Durable identity: module-qualified class name — the S3 namespace and registry key.

        Renaming a flow class OR moving it to another module makes it a new flow
        (old runs stay under the old id); `__main__`/REPL flows have no stable id.
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

    def phase(self, name: str) -> AbstractContextManager[None]:
        """Record a named span inside the current step, including nested spans and failures."""
        from .phases import phase

        return phase(self, name)

    @classmethod
    def ledger_columns(cls):
        """The flow's declared extra ledger columns ({} if no inner Ledger class)."""
        return cls.Ledger.columns() if cls.Ledger else {}

    @classmethod
    def params_columns(cls):
        """The flow's declared params ({} if no inner Params class)."""
        return cls.Params.columns() if cls.Params else {}

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

    Registration is a side effect of importing, so the whole package is walked to
    catch flows split across modules; only those defined under it are returned.
    """
    mod = importlib.import_module(package)
    for info in pkgutil.walk_packages(getattr(mod, "__path__", []), mod.__name__ + "."):
        importlib.import_module(info.name)
    return {
        name: cls
        for name, cls in Flow._registry.items()
        if cls.__module__ == package or cls.__module__.startswith(package + ".")
    }
