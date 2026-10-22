# 0033 — Table step inputs and partitioning

Status: accepted (2026-10-22).

## Context

Table schemas describe persisted columns, while execution passes lazy DuckDB
relations. Applications wrapped relation access to hide that typing mismatch.
They also implemented their own distinct-key queries and SQL literal escaping
to create fan-out partitions.

## Decision

Export the existing runtime StepInput as a generic interface. StepInput[Schema]
annotates a normal table dependency; the DAG checks its schema against its producer.
Schema-only annotations remain valid. Branch and Collect expose the same table
input operations. Artifact dependencies retain their reference and list behavior.

StepInput.partition_by(column) returns one lazy relation per distinct key, in
ascending key order with NULL last. It materializes only keys and retains NULL
rows in a separate partition. Column names use identifier quoting; values use
DuckDB constant expressions. Empty input returns no partitions.

Native relation methods remain delegated to DuckDB and return ordinary relations.
No row or aggregate convenience wrappers are added. Output annotations still
declare persisted schemas, independently of the concrete value returned by a step.

## Consequences

Applications can remove relation-access DSL modules and SQL value interpolation.
Partitioning remains an explicit step operation with the existing fan-out and
resume semantics. This adds no automatic grouping, nested fan-out, dataframe
dependency, or materialization of table payloads in Python.
