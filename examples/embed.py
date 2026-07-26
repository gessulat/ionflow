"""Runnable ionflow example.

    export IONFLOW_BUCKET=my-bucket   # + AWS_* / IONFLOW_ENDPOINT
    ionflow run examples.embed:EmbedDocs
    ionflow query examples.embed:EmbedDocs "SELECT * FROM summary"

A miniature embedding pipeline: load documents, fan out by source, embed each
source's documents in parallel (numpy in, numpy out — the model is a stand-in
that hashes the text), fan in to a summary. The DAG is the method signatures:
`vectors` depends on `by_source` because it has a parameter named `by_source`.
"""

import hashlib

import numpy as np
import pyarrow as pa

from ionflow import Branch, Collect, Fanout, Flow, LedgerColumns, Table, check, primary_key, shaped


class Doc(Table):
    doc_id: int = primary_key
    source: str
    text: str


class DocVector(Table):
    doc_id: int
    source: str
    embedding: shaped(np.float32, 64)      # fixed shape: a wrong-length row fails the write
    norm: np.float32 = check("norm >= 0")  # narrow dtypes persist as-is (FLOAT, not DOUBLE)


class Summary(Table):
    n_sources: int
    n_docs: int
    avg_norm: float


class EmbedDocs(Flow):
    class Ledger(LedgerColumns):
        n_docs: int

    def docs(self) -> Doc:
        return pa.table(
            {
                "doc_id": [1, 2, 3, 4],
                "source": ["wiki", "wiki", "arxiv", "arxiv"],
                "text": ["ducks type", "tables persist", "vectors search", "flows fan out"],
            }
        )

    def by_source(self, docs: Doc) -> Fanout[Doc]:  # partitioning reuses the schema
        sources = [s for (s,) in docs.project("source").distinct().order("source").fetchall()]
        return [docs.filter(f"source = '{s}'") for s in sources]

    def vectors(self, by_source: Branch[Doc]) -> DocVector:
        texts = by_source.np("text")  # numpy in: one column as ndarray
        vecs = np.stack(  # stand-in for a real model: (n, 64) float32
            [np.frombuffer(hashlib.sha512(t.encode()).digest(), np.uint8) for t in texts]
        ).astype(np.float32)
        return {  # numpy out: dict of ndarrays; the 2D one becomes the fixed-size-list column
            "doc_id": by_source.np("doc_id"),
            "source": by_source.np("source"),
            "embedding": vecs,
            "norm": np.linalg.norm(vecs, axis=1),
        }

    def summary(self, vectors: Collect[DocVector], docs: Doc) -> Summary:
        self.n_docs = docs.aggregate("count(*) AS c").fetchone()[0]
        return vectors.aggregate("count(DISTINCT source) AS n_sources, count(*) AS n_docs, avg(norm) AS avg_norm")
