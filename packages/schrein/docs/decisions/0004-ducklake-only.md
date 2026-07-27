# 0004 — DuckLake only; Apache Iceberg is scrapped, not deferred

An Iceberg adapter (register as a JSON pointer map + PyIceberg custom
catalog) and thin presets over the managed Iceberg REST catalogs (S3
Tables, R2, BigLake) were considered for the roadmap. Both are out of
scope permanently — scrapped, not deferred.

Why no adapter: schrein's consumer is ionflow, which is
DuckLake-native end to end; the spec's own analysis (§2.3) concedes the
Iceberg adapter answers a narrower question with a worse pitch — the
metadata labyrinth (round-trip floor, per-commit file spray) is the
format itself, so a CAS register only replaces the *service*, not the
pain. Supporting it meant carrying PyIceberg, a REST shim for DuckDB
writers, and a second commit path under the hammer tests, all for
ecosystem compatibility nobody here needs.

Why the presets go with it: they contain no schrein — they are
PyIceberg `RestCatalog` configuration dictionaries with credential
wiring, untestable under moto, valuable only as glue for an adapter
that doesn't exist.

What this buys: one format, one adapter, one commit protocol under test;
the ducklake extension is the only DuckDB extension the library manages;
`transact`'s replay contract has a single consumer shape. If Iceberg
ever matters it enters through a new decision, not by default.
