# 0002 — Resume copies parts; every run folder is self-contained

Status: superseded by 0020 (2026-07-29)

Resume used to server-side-copy every reused part from the origin into the
new run's folder, so one folder = one run and deleting any run folder could
never break another. The price was O(corpus) storage per incremental run.
0020 inverts the trade: a part is stored once by the run that computed it, a
run is its manifest, and resume records references — deleting a run folder
now requires checking `lake.manifest` for references first.
