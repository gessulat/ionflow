"""Artifact recipes exercised in threads, processes, branches, and child runs."""

import os
from pathlib import Path
from typing import Annotated

from pydantic import AfterValidator

from ionflow import Artifact, ArtifactInputs, Branch, Call, Fanout, Flow, FlowParams, subflow


class Source(Artifact):
    pass


class Index(Artifact, payload="bundle", identity=("library", "method", "nces", "losses")):
    library: Source
    method: str
    nces: list[int]
    losses: list[str]
    platform: str | None = None


class Build(Flow):
    class Inputs(ArtifactInputs):
        library: Source

    class Params(FlowParams):
        work_dir: str
        method: str = "HCD"
        nces: Annotated[list[int], AfterValidator(sorted)] = [30]
        losses: list[str] = []

    def index(self, library: Source) -> Index:
        source = library.local_path().read_bytes()
        root = Path(self.work_dir)
        root.mkdir(parents=True, exist_ok=True)
        with (root / "executed").open("a") as stream:
            stream.write(library.id + "\n")
        (root / "index.arrow").write_bytes(source + os.environ.get("IDENTITY_BUILD", "cpu").encode())
        return Index.from_path(
            root,
            members={"index.arrow": root / "index.arrow"},
            library=library,
            method=self.method,
            nces=self.nces,
            losses=self.losses,
            platform=os.environ.get("IDENTITY_BUILD", "cpu"),
        )


class Parent(Flow):
    Inputs = Build.Inputs
    Params = Build.Params

    @subflow(Build, output="index")
    def prepared(self, library: Source) -> Call[Index]:
        return Call(artifacts={"library": library}, params={k: getattr(self, k) for k in self.params_columns()})


class Mismatch(Flow):
    Inputs = Build.Inputs
    Params = Build.Params

    def index(self, library: Source) -> Index:
        ref = Build.index(self, library)
        ref._metadata["method"] = "CID"
        return ref


class DictionaryBuild(Flow):
    Inputs = Build.Inputs

    class Params(Build.Params):
        digest: dict[str, list[int]]

    index = Build.index


class BranchBuild(Flow):
    class Inputs(ArtifactInputs):
        sources: list[Source]

    Params = Build.Params

    def library(self, sources: list[Source]) -> Fanout[Source]:
        return sources

    def index(self, library: Branch[Source]) -> Index:
        return Build.index(self, library)
