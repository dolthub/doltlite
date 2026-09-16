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

## Hotspot comparisons with stock SQLite

[performance_hotspots.py](../../test/performance_hotspots.py) reports both
PR-base and stock SQLite comparisons for every workload. Each cell is the
median of five elapsed times for the whole workload. `Candidate/stock` is
DoltLite time divided by SQLite time: less than 1 means DoltLite is faster.
The stock ratios describe the current performance gap; the regression gate
compares the candidate with the PR base. Run it with an independently built,
verified stock SQLite shell:

```sh
python3 test/performance_hotspots.py \
  --baseline /path/to/pr-base/doltlite \
  --candidate build/doltlite \
  --stock /path/to/stock/sqlite3
```

### Large Table Scans

The read fixture has 262,144 rows with 1 KiB random BLOB payloads, totaling
256 MiB of payload, with a 64 MiB cache per connection.

| Metric | Work measured |
|---|---|
| `scan_first` | Sums payload lengths and primary keys across the entire table on a newly opened connection. |
| `scan_repeat` | Immediately repeats that full scan on the same connection, showing cache reuse when the dataset exceeds the cache. |
| `point_10000` | Performs 10,000 scattered primary-key lookups after the scans, summing the payload lengths. |

The queries return aggregates, not payload bytes. They measure traversal,
record access, and aggregation, rather than transferring the entire dataset
to an application. Process startup, database open, and fixture setup are
excluded; the OS file cache is not flushed.

### Large Table Appends

These rows use a separate, one-row table with a 2 MiB random BLOB. Each timed
statement is `UPDATE updates SET payload=randomblob(2097152) WHERE id=1;`.
The name "append" refers to DoltLite appending new storage records; the SQL
replaces a large value rather than inserting another row into the scan fixture.
Payload generation, the update, and its autocommit persistence are timed.
Fixture setup, connection open, untimed validation, and close are excluded.

[doltlite_checkpoint_perf.sh](../../test/doltlite_checkpoint_perf.sh) prepares
DoltLite near its 64 MiB checkpoint threshold and verifies the checkpoint
metadata around each timed update. Stock SQLite starts with the same live
schema and payload size, then executes three consecutive updates on one
connection. Each trial uses a fresh SQLite fixture.

| Metric | DoltLite operation | Stock SQLite comparison |
|---|---|---|
| `append_below` | Update immediately below the checkpoint threshold. | First payload replacement. |
| `append_checkpoint` | Update that creates the chunk-index checkpoint. | Second payload replacement. |
| `append_post` | Next update, which reuses that checkpoint. | Third payload replacement. |

SQLite uses `journal_mode=WAL`, `synchronous=FULL`, a 64 MiB cache, and disabled
memory mapping. Automatic WAL checkpointing remains enabled at 1,000 pages;
any checkpoint work triggered by a timed update is included in its time.
The harness verifies these settings, each update's affected-row count and
payload length, and the stored result and integrity after reopening. Missing
stock measurements fail the report instead of producing empty comparison cells.

The row names identify DoltLite's checkpoint phases. SQLite's second update is
not forced to match DoltLite's checkpoint event. Its checkpoints follow its own
page-based policy. The fixtures also have different physical histories:
DoltLite has accumulated immutable chunks near its threshold, while SQLite
can reuse pages for the same current row. These measurements compare the same
SQL operation under each engine's storage policy.

## Architectural performance differences

DoltLite shares SQLite's SQL layer but uses a content-addressed prolly tree
instead of SQLite's page-oriented B-tree. That changes the work below a query,
even when its SQL and results match.

### Reading nodes by hash

SQLite locates B-tree pages by page number, consulting its page cache and WAL
as needed. DoltLite tree links contain content hashes. On a node-cache miss,
DoltLite resolves the hash through the chunk index, reads the chunk, and verifies
its hash before using the node. This adds index lookup and hashing work to
large scans and scattered point reads. The bounded checkpoint-page cache
reduces repeated index reads, but a dataset larger than the node cache still
requires loading and verifying nodes. See
[chunk_store.c](../../src/chunk_store.c) and [chunk_index.c](../../src/chunk_index.c).

The content hashes allow versions to share unchanged subtrees and let diffs
skip equal subtrees. They also change the tradeoff for small SQL operations:
computing a BLOB's length can use record metadata in SQLite, while DoltLite's
storage path may still load and verify the containing node. A length query is
therefore not a measurement of equivalent payload processing in both engines.

### Immutable writes and versioned state

SQLite writes modified pages through its journaling machinery. DoltLite
constructs and hashes changed tree nodes and publishes new roots for the
working state. Unchanged nodes can remain shared with earlier versions. This
supports branches and commits without copying an entire table, but adds node
construction, hash computation, and metadata publication to writes.

Replacing a whole random BLOB provides little opportunity to reuse its previous
contents. Both engines must persist the new payload; DoltLite also performs its
content-addressing work. Small edits and repeated identical data have different
sharing opportunities, so their performance should be measured separately.
A SQL autocommit update persists a working set; it does not create a user-visible
`dolt_commit` on every statement.

### Checkpoints and reclamation

The two checkpoint operations do different work. SQLite copies committed WAL
pages back into the database and can reuse the WAL when readers permit it.
DoltLite writes a checkpoint of chunk locations and publishes a sealed root,
with synchronization steps to make it durable. The checkpoint bounds later
WAL replay; it does not compact the payload data. See
[wal.c](../../src/wal.c), [chunk_wal.c](../../src/chunk_wal.c), and
[chunk_store_commit.c](../../src/chunk_store_commit.c).

For a few large payload replacements, publishing an index of chunk locations
can be cheaper than copying payload pages during SQLite checkpointing. That
can offset other write overheads. Conversely, DoltLite's older immutable chunks
occupy space until GC can reclaim those that are no longer reachable; chunks
needed by retained history remain live. File growth, checkpoint latency, and
GC cost are separate dimensions.

### Measured example

A local macOS arm64 run on 2026-09-16, with optimized DoltLite revision
`3878833834` and the verified stock SQLite reference, produced these five-trial
medians using the default fixtures above:

| Workload | DoltLite ms | Stock SQLite ms | DoltLite/stock |
|---|---:|---:|---:|
| `scan_first` | 647.575 | 109.788 | 5.90× |
| `scan_repeat` | 655.419 | 101.657 | 6.45× |
| `point_10000` | 101.710 | 17.814 | 5.71× |
| `append_below` | 6.659 | 9.123 | 0.73× |
| `append_checkpoint` | 7.109 | 9.951 | 0.71× |
| `append_post` | 6.669 | 6.317 | 1.06× |

The read gap is substantial in this fixture. Large replacements are competitive
here, with DoltLite faster in the first two phases and slightly slower in the
third. Architecture explains the kinds of work each engine performs, not a
fixed slowdown or an attribution of every millisecond. Hardware, cache state,
checkpoint timing, and implementation improvements change the ratios. Use the
current CI report for current numbers.
