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
