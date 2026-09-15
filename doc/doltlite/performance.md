# Performance

Nightly DoltLite-versus-SQLite numbers:
[performance-report.md](../../performance-report.md). Per-release comparisons ship on
[GitHub releases](https://github.com/dolthub/doltlite/releases).

PR CI runs paired sysbench-style workloads (int / text / blob / composite PK)
and a short version-control latency suite against the PR base, with automatic
remeasurement on borderline regressions. Details live in
[`.github/workflows/benchmark.yml`](../../.github/workflows/benchmark.yml).

Complexity properties asserted in CI (`test/doltlite_perf.sh`,
`test/doltlite_structural.sh`):

- **O(log n)** point SELECT / UPDATE / DELETE by primary key
- **O(n log n)** bulk INSERT inside an explicit transaction
- **O(changes)** `dolt_diff` between commits (not proportional to table size)
- **Structural sharing** between versions (small edits add little file growth)
- **GC** reclaims unreachable chunks without dropping reachable data
- **Bounded open time** independent of how many commits are in the WAL
  (`test/doltlite_open_perf.sh`)

## Open time

Opening a database replays the chunk WAL from the last checkpoint, so a
checkpoint is what keeps open time flat as history accumulates. One becomes
due whenever the WAL has grown by `DOLTLITE_WAL_CHECKPOINT_THRESHOLD` bytes
or `DOLTLITE_WAL_CHECKPOINT_CHUNKS` chunks since the last one, whichever
comes first — the byte limit catches a few large commits, the chunk limit
catches a stream of small ones, which is the shape that otherwise grows
without bound.

Lowering either limit shortens open time and grows the file between
collections; `dolt_gc()` reclaims that space. Deployments that open a
connection per request and commit often are the ones worth tuning.
