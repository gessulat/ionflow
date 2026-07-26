# 0004 — DuckLake only; Apache Iceberg is scrapped, not deferred

The spec's original roadmap carried an Iceberg adapter (M3: register as a
JSON pointer map + PyIceberg custom catalog) and thin presets over the
managed Iceberg REST catalogs (M3b: S3 Tables, R2, BigLake). Both are
removed from scope permanently.

Why the adapter goes: schrein's consumer is ionflow, which is
DuckLake-native end to end; the spec's own analysis (§2.3) concedes the
Iceberg adapter answers a narrower question with a worse pitch — the
metadata labyrinth (round-trip floor, per-commit file spray) is the
format itself, so a CAS register only replaces the *service*, not the
pain. Supporting it meant carrying PyIceberg, a REST shim for DuckDB
writers, and a second commit path under the hammer tests, all for
ecosystem compatibility nobody here needs.

Why the presets go with it: they contained no schrein — they were
PyIceberg `RestCatalog` configuration dictionaries with credential
wiring, untestable under moto, valuable only as glue for the adapter
that no longer exists.

What this buys: one format, one adapter, one commit protocol under test;
the ducklake extension is the only DuckDB extension the library manages;
`transact`'s replay contract has a single consumer shape. If Iceberg
ever matters again it re-enters through a new decision, not by default.
