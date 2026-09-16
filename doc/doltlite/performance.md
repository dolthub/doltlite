# Performance

DoltLite tests performance in three ways: comparisons with the PR base to catch
new regressions, comparisons with stock SQLite to track the remaining SQL
performance gap, and scaling tests that increase the amount of data or history.
This page describes the workloads and how to read their results. Fixture sizes
and repetition counts below are the defaults unless a CI setting is specified.

Nightly DoltLite-versus-SQLite numbers live in
[performance-report.md](../../performance-report.md). Per-release comparisons
ship on [GitHub releases](https://github.com/dolthub/doltlite/releases).
PR measurements appear in CI artifacts and PR comments. The scheduling and
thresholds are defined in [benchmark.yml](../../.github/workflows/benchmark.yml),
[nightly-performance.yml](../../.github/workflows/nightly-performance.yml), and
[test.yml](../../.github/workflows/test.yml).

- [Reading the reports](#reading-the-reports)
- [Sysbench-style SQL workloads](#sysbench-style-sql-workloads)
- [Performance hotspots](#performance-hotspots)
- [Version-control latency](#version-control-latency)
- [Scaling curves](#scaling-curves)
- [HEAD queries versus history depth](#head-queries-versus-history-depth)
- [Foreign-key verification scaling](#foreign-key-verification-scaling)
- [Open time](#open-time)
- [Checkpoint write latency](#checkpoint-write-latency)
- [Primary-key operations and diff growth](#primary-key-operations-and-diff-growth)
- [COUNT performance](#count-performance)
- [Structural sharing and GC space use](#structural-sharing-and-gc-space-use)
- [Large-scale end-to-end tests](#large-scale-end-to-end-tests)
- [Related scale correctness tests](#related-scale-correctness-tests)
- [Standalone C SQL benchmark](#standalone-c-sql-benchmark)

## Reading the reports

Latency is elapsed time for the **whole named workload**, not time per SQL
statement or per row. Lower is better. `us` means microseconds; `ms` means
milliseconds. Paired benchmarks alternate execution order and report medians to
reduce the effect of runner noise.

| Column | Meaning |
|---|---|
| PR base / baseline | Time using the base revision of DoltLite in a PR comparison. In an absolute SQL comparison, the baseline is stock SQLite. Read the column label. |
| Candidate / DoltLite | Time using the revision under test. |
| Candidate/base or Ratio | Candidate time divided by baseline time: `0.60×` is 40% less time; `1.20×` is 20% more. |
| Stock / Candidate/stock | Stock SQLite time and the DoltLite-to-SQLite ratio. `6×` means DoltLite takes six times as long. |
| Ceiling / Result | The allowed latency or ratio and whether the measurement passed it. |

A PR can improve substantially over its base and still be slower than SQLite.
The two comparisons answer different questions. PR aggregate ratios use summed
workload times, so a long-running workload has more influence than a short one.
They are not per-query averages.

PR SQL and hotspot gates use a 1.50× individual-workload threshold and 1.25×
aggregate threshold, with a 10 ms minimum increase. Version-control individual
workloads use 2.00× and 50 ms; their aggregate gate uses 1.25× and 10 ms.
[benchmark_retry.py](../../test/benchmark_retry.py) allows up to three attempts
when a valid result indicates a regression. Execution failures are errors, not
successful timing samples. Nightly stock comparisons use separate absolute
ceilings specified in their workflow.

## Sysbench-style SQL workloads

These suites generate SQL resembling sysbench OLTP workloads. They cover the
same operations with four primary-key representations, normally on 100,000-row
fixtures with a secondary index on `k`:

| Suite | Key representation | Purpose |
|---|---|---|
| [int](../../test/sysbench_compare.sh) | `INTEGER PRIMARY KEY` | Integer lookup, ordering, and modification paths. |
| [textpk](../../test/sysbench_compare_textpk.sh) | 32-character hexadecimal `TEXT PRIMARY KEY` | Text-key comparison and storage costs. |
| [blobpk](../../test/sysbench_compare_blobpk.sh) | 16-byte `BLOB PRIMARY KEY` | Binary-key comparison and storage costs. |
| [compositepk](../../test/sysbench_compare_compositepk.sh) | Two integer columns, `PRIMARY KEY(a,b) WITHOUT ROWID` | Tuple-key lookup, range ordering, and secondary-index maintenance. |

The report separates `mem_reads`, `mem_writes`, `file_reads`, `file_writes`,
`ac_reads`, and `ac_writes`. Memory sections use `:memory:` databases. File
sections use real files. Ordinary write workloads wrap their modifications in
an explicit transaction; autocommit writes commit each statement separately.
Autocommit reads reuse the read workloads. Stock SQLite uses WAL mode with
`synchronous=FULL` in the autocommit section.

PR CI uses five paired invocations per workload and nine for autocommit writes.
The [timer](../../test/sysbench_timer.c) measures the SQL between benchmark
markers, including preparation and execution, while excluding database open
and fixture setup. Each sample builds a fresh fixture; this suite does not
specifically force the dataset to exceed the cache.

### `oltp_point_select`

Executes 10,000 individual primary-key lookups, retrieving the text column `c`.
This measures repeated point-query preparation, tree seeks, and row retrieval.

### `oltp_range_select`

Executes 1,000 primary-key range queries, each spanning 100 consecutive keys
and returning `c`. This exercises seeking to a range start and advancing through
nearby rows.

### `oltp_sum_range`

Executes 1,000 sums of `k` over 100-key primary-key ranges. This adds aggregation
to the range-read path.

### `oltp_order_range`

Executes 100 queries over 100-key ranges and sorts their results by `c`.
This measures range retrieval plus sorting on a non-indexed column.

### `oltp_distinct_range`

Executes 100 queries over 100-key ranges, returning distinct `c` values in sorted
order. This adds duplicate elimination to range retrieval and sorting.

### `oltp_index_scan`

Executes 1,000 equality searches on the secondary index `k`, returning the
primary key and `c`. The index locates matching rows, and retrieving `c` also
requires the table data.

### `select_random_points`

Executes 1,000 queries with ten randomly selected primary keys in each `IN`
list, retrieving all table columns. This tests multiple point seeks within one
statement rather than one statement per key.

### `select_random_ranges`

Executes 1,000 `count(k)` queries over ten-key primary-key ranges. This tests
short range counts and the count optimizations applicable to them.

### `covering_index_scan`

Executes 1,000 `count(k)` queries over ranges of secondary-index values.
The index contains everything needed to answer the query, so this isolates
index-range traversal and counting without fetching other table columns.

### `groupby_scan`

Executes 100 queries over 1,000-key primary-key ranges, grouping and counting
rows by `k` and ordering the groups. This exercises range reads and grouping.

### `index_join`

Executes 500 joins between two indexed tables on `a.k=b.k`, restricting the
first table to a ten-key primary-key range. It returns the matching primary-key
pairs and exercises indexed join probes and result production.

### `index_join_scan`

Executes 100 joins on the same indexed columns, restricting the second table
to a 50-key primary-key range and counting the matches. This tests a larger
join input and aggregate result instead of returning each pair.

### `types_table_scan`

Executes 100 full scans of a table containing integer, real, and text values,
counting rows whose text contains a generated substring. This exercises record
access and predicate evaluation with mixed column types.

### `table_scan`

Executes 100 full scans, counting rows whose `c` value contains `abc` using
`LIKE '%abc%'`. The predicate requires examining row contents; this is different
from an unfiltered `count(*)` that can use stored row counts.

### `oltp_read_only`

Executes 1,000 groups of ten point lookups plus a range read, a range sum, an
ordered range, and a distinct ordered range. It measures a mixed read workload
rather than a single access pattern.

### `oltp_bulk_insert`

Creates a new table and inserts the configured row count in one transaction.
The new table has a primary key but no secondary `k` index. This measures table
creation, bulk tree construction, and transaction completion.

### `oltp_insert`

Inserts 5,000 additional rows into the existing indexed table in one
transaction. It includes maintaining the secondary index as the table grows.

### `oltp_update_index`

Updates `k` for 10,000 randomly selected primary keys in one transaction.
Each update changes an indexed value, exercising both table and secondary-index
maintenance.

### `oltp_update_non_index`

Updates `c` for 10,000 randomly selected primary keys in one transaction.
This measures row replacement when the modified column is not indexed.

### `oltp_delete_insert`

Performs 5,000 delete-and-reinsert pairs in one transaction. This tests row
removal, replacement, and index maintenance under repeated churn.

### `types_delete_insert`

Performs 5,000 delete-and-reinsert pairs on the mixed-type table, replacing
integer, real, and text values. This exercises serialization and updates for
those column types.

### `oltp_write_only`

Executes 1,000 groups of an indexed update, a non-indexed update, and a
delete-and-reinsert pair, all within one transaction. This measures mixed write
cost with transaction overhead amortized across the workload.

### `oltp_read_write`

Executes 1,000 groups of ten point lookups, a range read, a range sum, two
updates, and a delete-and-reinsert pair within one transaction. Reads must
observe earlier writes in the transaction, so this also exercises access to
pending edits.

### Autocommit write variants: `_ac`

Each of the eight write workloads has an `_ac` variant with 200 modifying
statements, each in its own transaction. Delete-and-reinsert variants use 100
pairs; mixed write variants use 50 groups. `oltp_read_write_ac` also includes
its read statements, and `oltp_bulk_insert_ac` includes creating its new table.
These expose per-transaction persistence costs that bulk transactions amortize.
Compare `_ac` rows with the matching `_ac` baseline, since they do less work
than the wrapped write workloads.

## Performance hotspots

[performance_hotspots.py](../../test/performance_hotspots.py) supplements the
SQL workloads with performance gaps against stock SQLite. Its PR comment has
a **Large Table Scans** table with the five read metrics below and a **Small
Table Updates** table with `bulk_update_text_pk`: one transaction updating a
non-key balance column in 5,000 text-primary-keyed rows. The update fixture fits
within its 64 MiB cache; its timing includes BEGIN, UPDATE, and COMMIT. Each
metric compares with both the PR base and stock SQLite. The harness reports
five-trial medians in milliseconds, verifies results, and gates regressions
against the PR base.

The first three workloads use 262,144 integer-keyed rows with 1 KiB random BLOB
payloads: 256 MiB of payload with a 64 MiB cache per connection. They run in the
order below on each connection. SQL execution is timed; process startup,
database open, and fixture construction are excluded. The OS file cache is not
flushed, so the first scan is not a cold-disk measurement.

### `scan_first`

Runs `SELECT sum(length(payload)),sum(id) FROM t NOT INDEXED;` on a newly opened
connection. It walks the whole table and exercises chunk location, node loading,
and sequential cursor traversal. The result checks the aggregate payload length
and primary-key sum; it does not return all payload bytes to the client.

### `scan_repeat`

Immediately repeats the same full-table query on the same connection.
This shows how caching helps a second pass when the table is larger than the
configured cache. It should not be interpreted as a fully cached in-memory scan.

### `point_10000`

Performs 10,000 primary-key lookups spread across the table by a deterministic
key sequence, summing the retrieved payload lengths. The lookups execute inside
one SQL statement after the two scans. The reported time covers all 10,000
lookups, including tree traversal and chunk lookup.

### `index_scan_row_fetch`

Executes 1,000 customer lookups on a separate table of 262,144 orders with 1 KiB
text descriptions and a secondary index on `customer_id`. Each query matches
128 rows scattered across the primary keys and aggregates amounts, description
lengths, and description characters. This exercises index traversal followed by
table-row retrieval without transferring full descriptions to the client.

### `index_scan`

Executes the same 1,000 customer lookups, selecting `count(*)` and `sum(id)`.
The customer index contains the primary key, so the query uses a covering index
and does not fetch table rows. This measures index traversal and aggregation.

Both index workloads use identical deterministic data across engines, a 64 MiB
cache, and a fresh connection for each workload. Each reported sample sums the
SQL timings of all 1,000 statements. The harness verifies every query result,
database integrity, and the expected index or covering-index query plan.

## Version-control latency

[vc_perf_ceiling.sh](../../test/vc_perf_ceiling.sh) measures version-control
operations on purpose-built fixtures. PR CI uses five paired repetitions
against the PR base. Without a baseline binary, the script reports absolute
latency ceilings, adjusted by a local file-I/O probe. Stock SQLite has no
corresponding branch, diff, or merge operations.

Each sample copies a seed database before timing. Unlike the SQL marker-based
benchmarks, these elapsed times include shell startup, connection open, command
execution, and close; fixture creation and copying are excluded.

### `status_clean_many_tables`

Counts `dolt_status` entries in a clean database with 800 tables and 125 rows
per table. This measures finding that a large catalog has no working changes.

### `status_dirty_many_tables`

Runs the same status query after changing one row in each of 40 tables.
This measures identifying a small dirty subset of a large catalog.

### `diff_regular_working_one_table`

Counts row changes in `dolt_diff_t0001` with `to_commit='WORKING'` in the dirty
fixture. This tests a targeted table diff when many other tables exist.

### `diff_regular_working_many_tables`

Counts entries in `dolt_diff` for `WORKING` with `data_change=1`.
This tests finding tables with data changes across the catalog.

### `diff_stat_working_many_tables`

Counts `WORKING` entries in `dolt_diff` and sums their `data_change` flags.
Despite the metric name, this is a catalog-summary query, not a call to
`dolt_diff_stat` that counts individual changed rows.

### `diff_schema_working_many_tables`

Counts `WORKING` entries with `schema_change=1` after adding a column and
changing one row in each of 20 tables. This exercises schema-change discovery
across a large catalog.

### `branch_list_many_branches`

Counts `dolt_branches` entries in a database with 300 additional branches.
This measures enumerating branch metadata.

### `branch_create_delete`

Creates and deletes a temporary branch in that same fixture. The timing covers
both operations and their ref updates.

### `at_literal_deep_history`

Counts rows from `dolt_at_history_only('zz_history')`. The target table exists on
a branch with 200 history updates, alongside 200 unrelated branches. This tests
resolving a literal branch and accessing its table from another branch.

### `diff_literal_deep_history`

Counts `dolt_diff_history_only('main','zz_history')` rows in that fixture.
This tests resolving both named endpoints and reading the resulting table diff.

### `history_literal_deep_history`

Counts `dolt_history_history_only('zz_history')` rows. This exercises walking
the target table's history while unrelated branches are also present.

### `checkout_branch_clean`

Switches to `feat` and back to `main` in a clean database with 400 tables and
250 rows per table. Both switches are timed, exposing catalog and working-set
installation costs.

### `merge_data_no_conflicts`

Merges branches that each modify a disjoint set of 2,000 rows in a 100,000-row
table. This measures merging a relatively small change set without conflicts
or secondary indexes.

### `merge_data_secondary_index`

Merges a 300,000-row table with a secondary index whose key order differs from
primary-key order. One branch changes even rows and the other changes odd rows;
both change the indexed values. Every row changes across the two branches.
This exposes secondary-index maintenance during a large conflict-free merge.

### `merge_schema_no_conflicts`

Merges branches that add indexes to different tables. This measures combining
compatible schema changes on a small data fixture.

### `merge_data_conflicts`

Merges branches that change the same 2,000 rows differently in a 100,000-row
table, reads the conflict count, and rolls back. The merge runs inside an
explicit transaction so the conflicts remain available for inspection.

### `merge_data_conflicts_with_resolve`

Performs the same conflicted merge, resolves the rows with `--ours`, verifies
that the conflicts are cleared, and rolls back. This includes conflict
construction, enumeration, and resolution; it does not time a durable commit
of the resolved result.

## Scaling curves

[doltlite_scaling.sh](../../test/doltlite_scaling.sh) runs in the PR scaling job
on an optimized build. Its timings include process and connection overhead.
It compares larger fixtures with smaller ones using generous ratio limits and
checks data correctness alongside the measurements. These are regression
indicators for expected complexity, not proofs of asymptotic bounds.

### History depth

Measures blocks of 200 single-row updates plus `dolt_commit`, a full `dolt_log`
walk, a round-trip checkout to an old branch, and merges from disjoint side
branches. It compares shallow history near 200 commits with deeper history
near 2,400 commits. After GC it also measures more commits to detect a lasting
increase in write cost. The log query deliberately reads the whole history;
its work is expected to grow with history length.

### Table size

Tests 100,000, 1 million, 10 million, and 100 million rows. At each size it
reports GC time, open time, 400 point lookups, a 500-row update plus commit,
a predicate scan, the newest commit's 500-row diff, and file size. It checks
ratios between adjacent sizes, verifies row contents and branch isolation, and
bounds bytes per row for the larger fixtures. GC runs before the read/write
measurements so they exercise the compacted storage layout.

### BLOB size

Compares inserting and committing a 1 MiB BLOB with a 16 MiB BLOB, then queries
their lengths and verifies them. This catches excessive growth in write and
read-access cost as values grow. The read query returns a length, so its timing
is not a measurement of transferring the BLOB to an application.

## HEAD queries versus history depth

[vc_head_history_scale.py](../../test/vc_head_history_scale.py) holds the current
1,000-row table and schema constant while adding empty ancestor commits.
CI compares depths 1 and 1,000 using seven samples per query. Connection open
is excluded; first-statement preparation and execution are timed.

| Metric | Operation and purpose |
|---|---|
| `point_lookup` | Reads one current row by primary key; catches unnecessary ancestor work on a point query. |
| `table_scan` | Counts rows and sums text lengths in the current table; checks that identical current data costs the same to read. |
| `schema_lookup` | Looks up the table and index in `sqlite_master`; tests current-schema access. |
| `status_clean` | Counts clean `dolt_status` results; tests working-set comparison against HEAD. |
| `working_diff_clean` | Counts an empty working diff; tests avoiding history traversal when nothing changed. |

A query fails the default gate if it exceeds the depth-1 timing by both 1.25×
and 1 ms. This separates unnecessary ancestor traversal from the intentional
full-history walk in the scaling curves above.

## Foreign-key verification scaling

[fk_verification_scale.py](../../test/fk_verification_scale.py) measures
`dolt_verify_constraints('--all','--output-only')` with parent and orphan-child
counts independently set to 1,000 or 4,000. It repeats the matrix for integer
primary keys, composite keys with `NOCASE`, and unique text indexes with
`NOCASE`.

The output is median elapsed seconds over three invocations, including process
startup and open. Increasing each side independently makes repeated parent
scans per child visible. The script validates violation counts and output-only
behavior. It currently reports timings without a ratio gate; each invocation
has a timeout.

## Open time

[doltlite_open_perf.sh](../../test/doltlite_open_perf.sh) creates small commits
and measures opening the database to execute `SELECT 1`. Defaults are 1,000,
2,000, and 4,000 commits, with 512-byte payloads. Each sample times 40 separate
opens; the reported value is the median of three samples. The gate limits the
increase between the first and last stage to 750 ms for the whole 40-open
batch, and also checks row counts, history length, and integrity.

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

## Checkpoint write latency

[doltlite_checkpoint_perf.sh](../../test/doltlite_checkpoint_perf.sh) measures
three consecutive 2 MiB payload replacements around the 64 MiB checkpoint
threshold. It runs in the native timing suites and reports five-trial medians
in milliseconds, plus checkpoint/below and post/below ratios. Its assertions
validate the checkpoint boundary, reuse of the checkpoint after creation,
positive timings, row count, and database integrity. It does not enforce a
latency ceiling or compare with the PR base or stock SQLite.

Each trial starts from a copy of a fixture just below the checkpoint boundary.
The three updates run on the same connection. Their timings include generating
the random payload, executing the update, and completing its autocommit
transaction; fixture preparation, database open, and close are excluded.

### Below-threshold append

Replaces the single row's payload immediately before the checkpoint threshold
is crossed. This provides the ordinary write latency near that boundary.

### Checkpointing append

Times the next replacement, which crosses the threshold and creates a checkpoint
during the update. Comparing it with the below-threshold timing exposes the cost
of checkpoint creation. The harness checks the file's checkpoint metadata to
confirm that this is the checkpointing operation.

### Post-checkpoint append

Times the next replacement and verifies that it reuses the existing checkpoint.
Comparing it with the below-threshold timing shows whether ordinary writes remain
slow after checkpoint creation.

The SQL updates a single-row table; "append" refers to appending new storage
records. A checkpoint records the chunk index so later opens can replay from
that point. It is separate from a version-control `dolt_commit`. These timings
are reported by the standalone test, outside the performance-hotspot comment
and regression gate.

## Primary-key operations and diff growth

[doltlite_perf.sh](../../test/doltlite_perf.sh) compares point SELECT, single-row
UPDATE and DELETE, and a one-row working diff across 1,000-, 100,000-, and
1-million-row tables. It also compares bulk insertion of 1,000 versus 100,000
rows, and committed diffs with ten versus 1,000 changes in a million-row table.
The ratios are intended to catch full-table work in point or small-diff paths
and excessive growth in bulk operations.

A separate case compares preparing and executing 50 versus 800 indexed
`NOCASE` lookups on 200,000 rows while edits are pending. This catches repeated
validation of the committed index during statement preparation.

These shell-level timings include process startup and open, and some include
SQL generation. Several cases use five-sample medians; the ratio denominator
has a 50 ms floor. Use the paired SQL and hotspot suites for finer latency
comparisons.

## COUNT performance

[doltlite_count_perf.sh](../../test/doltlite_count_perf.sh) times unfiltered
`SELECT count(*) FROM t` on 1,000, 10,000, 100,000, and 1 million rows using the
shell's SQL timer. The million-row count must take at most 5 ms, and the
1,000-to-million-row ratio at most 20×, subject to the script's timing floor.
This checks use of stored row counts rather than walking every row.

The suite also verifies counts after insert, delete, commit, and reopen, plus
range-count correctness with duplicate keys, NULLs, affinity conversions, and
row-dependent bounds. Those cases validate the fast paths; they are not
separate latency measurements.

## Structural sharing and GC space use

[doltlite_structural.sh](../../test/doltlite_structural.sh) measures file growth
and reclamation in bytes. It checks that a one-row insert on 1,000- and
10,000-row tables, a small branch edit, and repeated small commits share
existing storage instead of copying whole versions.

GC cases check reclamation after branch deletion, preservation of shared branch
data, and file size after repeated commits and collections. Repeating GC must
leave the file size within 5% of the first collection. A smaller file is useful
only when the corresponding row checks pass; these are space-use checks, not
GC latency measurements.

## Large-scale end-to-end tests

[large_scale_test.sh](../../test/large_scale_test.sh) combines correctness
checks with absolute wall-clock limits in seconds. PR platform jobs use
`--quick`, with 100,000 rows. [Nightly scale](../../.github/workflows/nightly-scale.yml)
runs the full 1-million-, 10-million-, and 100-million-row workloads on an
optimized build.

The first stage covers bulk insert, add/commit, updating half the table,
diff, deleting a subset, branch/merge, local push/clone, reopening, rapid
commits, and queries. Larger stages measure bulk insert and commit, changing
and diffing ten rows in a large table, local push/clone, and point lookups.
The script applies explicit time ceilings to selected operations and validates
the resulting data. Local clone timings measure storage copying and installation,
not network throughput.

## Related scale correctness tests

Some suites have "scale" in their names but do not publish latency metrics:

- [doltlite_gc_scale.sh](../../test/doltlite_gc_scale.sh) checks 10,000-row GC
  cases with updates, deleted branches, secondary indexes, repeated collections,
  and an attached SQLite database.
- [doltlite_diff_stat_scale.sh](../../test/doltlite_diff_stat_scale.sh) verifies
  whole-table and incremental diff counts on multi-level trees with 2,000–2,500
  rows.
- [vc_oracle_pk_shapes_scale_test.sh](../../test/vc_oracle_pk_shapes_scale_test.sh)
  compares version-control results with Dolt across primary-key shapes on
  1,500-row fixtures.

These provide correctness coverage for operations that are also timed elsewhere.

## Standalone C SQL benchmark

[sysbench_compare.c](../../test/sysbench_compare.c) is a separate C benchmark
covering the same 23 SQL workload families with a default 10,000-row fixture.
It uses the SQLite C API, including prepared-statement reuse in workloads, and
emits JSON timings in milliseconds. Its sample sizes and timing boundaries
differ from the shell-generated suites. The paired PR and nightly reports use
the shell suites and `sysbench_timer.c`, so results from this standalone program
should be identified separately.
