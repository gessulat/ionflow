"""Count shared text artifacts and publish a report artifact.

Use ``ionflow artifacts add examples.artifacts:TextFile file.txt`` to import a
document, then ``ionflow run examples.artifacts:CountWords -a documents=ID``.
Repeat ``-a documents=ID`` to select several documents.
"""

import tempfile
from pathlib import Path

import pyarrow as pa

from ionflow import Artifact, ArtifactInputs, Branch, Collect, Fanout, Flow, Table


class TextFile(Artifact, kind="text-document"):
    language: str = "en"


class WordCount(Table):
    words: int


class Report(Artifact, kind="word-count-report"):
    words: int


class CountWords(Flow):
    class Inputs(ArtifactInputs):
        documents: list[TextFile]

    def files(self, documents: list[TextFile]) -> Fanout[TextFile]:
        return documents

    def count(self, files: Branch[TextFile]) -> WordCount:
        return pa.table({"words": [len(files.local_path().read_text().split())]})

    def summary(self, count: Collect[WordCount]) -> WordCount:
        return count.aggregate("coalesce(sum(words), 0)::BIGINT AS words")

    def report(self, summary: WordCount) -> Report:
        words = summary.fetchone()[0]
        with tempfile.TemporaryDirectory(prefix="ionflow-report-") as tmp:
            path = Path(tmp) / "report.txt"
            path.write_text(f"{words} words\n")
            return self.artifacts.publish(Report.from_path(path, words=words))
