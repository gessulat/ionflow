# 0003 — tests serialize moto's conditional PUTs (upstream check is not atomic)

The §10 CAS hammer intermittently lost exactly one update: tracing every
read/write showed two concurrent `If-Match` PUTs against the **same** etag
both being accepted (~16ms apart), the second silently clobbering the
first. Real S3's conditional write is an atomic check-and-swap; moto (as of
5.x) checks the precondition and stores the object non-atomically, so under
its threaded server two writers can both pass the check. Reads were clean —
every GET's body MD5 matched its etag — so this is purely a writer-writer
race in the fake, not a flaw in the commit protocol.

Fix: `schrein.testing` (used by both this suite and ionflow's) wraps
moto's WSGI app and takes a process-wide
lock around any PUT carrying `If-Match`/`If-None-Match`, restoring real-S3
semantics. Everything else (reads, unconditional data uploads) stays
concurrent, so the DuckDB-httpfs starvation lesson from ionflow (server in
a subprocess, never in-process) is unaffected.

Considered and rejected: defending in the library (re-read-and-verify after
write) — that would penalize every real backend to accommodate a test
fake's bug; and running moto single-threaded — that serializes the httpfs
data traffic too and risks starvation. Worth knowing: ionflow's ledger
tests share this exposure in principle, but never contend hard enough on
one key to hit it.
