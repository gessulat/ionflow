# 0013 — DuckDB extensions load through schrein's loader

Status: accepted (2026-07-26)

`storage.connect` loads httpfs via `schrein.ducklake.load_extension`, the
same three-way loader the catalog uses for ducklake: an already-installed
copy first, then the official `duckdb-extension-<name>` PyPI wheel, then
network `INSTALL` (schrein decision 0002). One loader means every
environment story — seeded cache, wheel fallback for proxied sandboxes and
offline CI, plain INSTALL elsewhere — is written once, for every
extension either package ever needs.
