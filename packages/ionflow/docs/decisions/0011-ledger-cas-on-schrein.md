# 0011 — the ledger's CAS loop delegates to schrein's CasRegister

Status: superseded by 0019 (2026-07-29)

`ledger.parquet` was an append-only parquet object rebuilt inside a schrein
`CasRegister.transact` closure, kept deliberately separate from the catalog
("roles do not merge"). 0019 dissolved the separation the other way: run
bookkeeping moved into the catalog DuckLake's `runs` table, committed
atomically with manifests and data registrations, and the standalone ledger
object is gone. What survives: every ionflow CAS write still goes through
schrein, and the dependency still points one way (schrein never imports
ionflow).
