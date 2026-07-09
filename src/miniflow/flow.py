from . import query, runner
from .config import Config
from .dag import build_dag


class Flow:
    """Base class. Subclass it; each public method is a step.

    A step's parameter names are its upstream steps; its return annotation is
    the output table schema. No `self.next()`, no scheduler — just an S3 bucket.
    """

    def __init_subclass__(cls, **kw):
        super().__init_subclass__(**kw)
        cls._dag = build_dag(cls)

    @classmethod
    def run(cls, config=None, repo=".", **kw):
        return runner.run(cls, config or Config.from_env(), repo=repo, **kw)

    @classmethod
    def resume(cls, origin_run_id, config=None, repo=".", **kw):
        return runner.run(
            cls, config or Config.from_env(), repo=repo,
            origin_run_id=origin_run_id, **kw,
        )

    @classmethod
    def duck(cls, config=None):
        return query.duck(cls, config or Config.from_env())
