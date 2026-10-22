"""Small declared application exercising generic browser contracts."""

import tempfile
from pathlib import Path

import pyarrow as pa

from ionflow import Artifact, ArtifactInputs, Flow, FlowParams, Table, View


class Document(Artifact, kind="browser-document"):
    View = View(label="Document", plural="Documents", columns=("name", "size", "local"))


class Bundle(Artifact, kind="browser-bundle", payload="bundle"):
    document_id: str
    View = View(references={"document_id": Document})


class Rows(Table):
    document_id: str
    text: str | None
    score: float
    View = View(columns=("text", "score"), labels={"score": "Confidence"}, references={"document_id": Document})


class Observe(Flow):
    class Inputs(ArtifactInputs):
        document: Document

    class Params(FlowParams):
        fail: bool = False

    View = View(label="Observation", columns=("run_id", "status", "fail"))

    def rows(self, document: Document) -> Rows:
        with self.phase("decode"):
            with self.phase("read"):
                text = document.local_path().read_text()
        if self.fail:
            with self.phase("failing"):
                raise ValueError("deliberate failure")
        return pa.table({"document_id": [document.id] * 3, "text": [text, None, "other"], "score": [0.1, 0.5, 0.9]})

    def bundle(self, rows: Rows) -> Bundle:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "shared").write_text("shared content")
            (root / "unique").write_text(rows.project("text").fetchone()[0])
            return self.artifacts.publish(Bundle.from_path(root, document_id=self.document.id))
