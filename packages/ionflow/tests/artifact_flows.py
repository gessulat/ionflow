from __future__ import annotations

from pathlib import Path

import pyarrow as pa

from ionflow import Artifact, ArtifactInputs, Branch, Collect, Fanout, Flow, FlowParams, Table


class Fasta(Artifact, kind="flow-fasta"):
    organism: str | None


class Raw(Artifact, kind="flow-raw"):
    pass


class Library(Artifact, kind="flow-library", payload="bundle"):
    model: str


class Index(Artifact, kind="flow-index"):
    method: str
    nces: list[int] = [27]


class Numbers(Table):
    n: int


class BuildLibrary(Flow):
    class Inputs(ArtifactInputs):
        fasta: Fasta

    class Params(FlowParams):
        work_dir: str

    def digest(self, fasta: Fasta) -> Numbers:
        return pa.table({"n": [len(fasta.local_path().read_bytes())]})

    def library(self, digest: Numbers) -> Library:
        out = Path(self.work_dir) / "library"
        out.mkdir(exist_ok=True)
        (out / "coefficients.arrow").write_text(str(digest.fetchone()[0]))
        return Library.from_path(out, model="7.2")


class Search(Flow):
    class Inputs(ArtifactInputs):
        raw_files: list[Raw]
        library: Library

    class Params(FlowParams):
        work_dir: str
        fail: bool = False

    def files(self, raw_files: list[Raw]) -> Fanout[Raw]:
        return raw_files

    def score(self, files: Branch[Raw], library: Library) -> Numbers:
        path = Path(self.work_dir) / (files.id.split(":")[1] + ".index")
        path.write_text("shared index")
        self.artifacts.publish(Index.from_path(path, method="HCD"), role="index", parents=[library])
        return pa.table({"n": [len(files.local_path().read_bytes())]})

    def total(self, score: Collect[Numbers]) -> Numbers:
        if self.fail:
            raise RuntimeError("failed after items completed")
        return score.aggregate("sum(n)::BIGINT AS n")


class SelectIndex(Flow):
    class Inputs(ArtifactInputs):
        index: Index

    def selected(self, index: Index) -> Index:
        return index


class PublishFiles(Flow):
    class Inputs(ArtifactInputs):
        fasta: Fasta

    class Params(FlowParams):
        work_dir: str
        fail: bool = False

    def files(self, fasta: Fasta) -> Fanout[Raw]:
        if self.fail:
            raise RuntimeError("failed before emitting items")
        refs = []
        for name in ("first", "second"):
            path = Path(self.work_dir) / (name + ".raw")
            path.write_text(name)
            refs.append(self.artifacts.publish(Raw.from_path(path)))
        return refs
