# 0016 — DuckDB extensions load through schrein's loader

`storage.connect` loads httpfs via `schrein.ducklake.load_extension`, the
same three-way loader the catalog uses for ducklake: an already-installed
copy first, then the official `duckdb-extension-<name>` PyPI wheel, then
network `INSTALL` (schrein decision 0002). One loader means every
environment story — seeded cache, wheel fallback for proxied sandboxes and
offline CI, plain INSTALL elsewhere — is written once, and the Iceberg
adapter's extensions will get it for free.
