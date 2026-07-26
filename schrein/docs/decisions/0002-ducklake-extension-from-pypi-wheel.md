# 0002 — load the ducklake extension from the PyPI wheel when INSTALL can't

`INSTALL ducklake` downloads from `extensions.duckdb.org` over plain HTTP —
blocked in proxied/offline environments (including the sandboxes this repo
is developed in), and a cold-start network dependency in any case.

DuckDB publishes official extension binaries to PyPI as
`duckdb-extension-<name>` wheels, versioned in lockstep with `duckdb`
itself (each wheel pins its exact duckdb version). `load_ducklake(con)`
therefore tries, in order:

1. `LOAD ducklake` — an already-installed copy (the normal production path);
2. the binary shipped by an importable `duckdb-extension-ducklake` wheel,
   preferring the directory matching the running duckdb version;
3. `INSTALL ducklake; LOAD ducklake` — the network fallback.

The wheel is a **dev dependency only**: production users on unrestricted
networks need nothing beyond `duckdb`, while the test suite runs without
access to `extensions.duckdb.org`. Because the wheel pins `duckdb==<exact>`,
the workspace lock holds duckdb at the wheel's version — upgrade both
together.
