"""Small composable flows, also importable by spawned workers."""

import os
from pathlib import Path

import pyarrow as pa

from ionflow import Artifact, ArtifactInputs, Branch, Call, Fanout, Flow, FlowParams, Table, subflow


class Raw(Artifact, kind="composition-raw"):
    pass


class RawDB(Artifact, kind="composition-rawdb"):
    pass


class Result(Table):
    text: str


class Convert(Flow):
    class Inputs(ArtifactInputs):
        raw: Raw

    class Params(FlowParams):
        work_dir: str

    def rawdb(self, raw: Raw) -> RawDB:
        path = Path(self.work_dir) / (raw.id.rsplit(":", 1)[-1] + ".db")
        path.write_text(raw.name + ":" + raw.local_path().read_text().upper())
        return RawDB.from_path(path)

    def checked(self, rawdb: RawDB) -> Result:
        if os.environ.get("IONFLOW_CHILD_FAIL"):
            raise ValueError("child failed after conversion")
        return pa.table({"text": [rawdb.local_path().read_text()]})


class Pipeline(Flow):
    class Inputs(ArtifactInputs):
        raw: Raw

    class Params(FlowParams):
        work_dir: str

    @subflow(Convert, output="rawdb")
    def converted(self, raw: Raw) -> Call[RawDB]:
        return Call(artifacts={"raw": raw}, params={"work_dir": self.work_dir})

    def result(self, converted: RawDB) -> Result:
        return pa.table({"text": [converted.local_path().read_text()]})


class TablePipeline(Flow):
    class Inputs(ArtifactInputs):
        raw: Raw

    class Params(FlowParams):
        work_dir: str

    @subflow(Convert, output="checked")
    def result(self, raw: Raw) -> Call[Result]:
        return Call(artifacts={"raw": raw}, params={"work_dir": self.work_dir})


class Batch(Flow):
    class Inputs(ArtifactInputs):
        raws: list[Raw]

    class Params(FlowParams):
        work_dir: str

    def files(self, raws: list[Raw]) -> Fanout[Raw]:
        return raws

    @subflow(Convert, output="rawdb")
    def converted(self, files: Branch[Raw]) -> Call[RawDB]:
        return Call(artifacts={"raw": files}, params={"work_dir": self.work_dir})


class Nested(Flow):
    class Inputs(ArtifactInputs):
        raw: Raw

    class Params(FlowParams):
        work_dir: str

    @subflow(Pipeline, output="result")
    def result(self, raw: Raw) -> Call[Result]:
        return Call(artifacts={"raw": raw}, params={"work_dir": self.work_dir})
