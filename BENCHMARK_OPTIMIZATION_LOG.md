# Sysbench Optimization Progress

Branch base: `origin/master` at `18b188e12`.

Goal: work through each sysbench benchmark, starting with int-key file-backed, compare local results, make one general optimization per PR, then move to the next benchmark without stacking PRs.

## Benchmark Order

### Integer Primary Key, File-Backed

- [x] `oltp_point_select`
- [x] `oltp_range_select`
- [ ] `oltp_sum_range`
- [ ] `oltp_order_range`
- [ ] `oltp_distinct_range`
- [ ] `oltp_index_scan`
- [ ] `select_random_points`
- [ ] `select_random_ranges`
- [ ] `covering_index_scan`
- [ ] `groupby_scan`
- [ ] `index_join`
- [ ] `index_join_scan`
- [ ] `types_table_scan`
- [ ] `table_scan`
- [ ] `oltp_read_only`
- [ ] `oltp_bulk_insert`
- [ ] `oltp_insert`
- [ ] `oltp_update_index`
- [ ] `oltp_update_non_index`
- [ ] `oltp_delete_insert`
- [ ] `oltp_write_only`
- [ ] `types_delete_insert`
- [ ] `oltp_read_write`

### Integer Primary Key, File-Backed Autocommit

- [ ] `oltp_bulk_insert_ac`
- [ ] `oltp_insert_ac`
- [ ] `oltp_update_index_ac`
- [ ] `oltp_update_non_index_ac`
- [ ] `oltp_delete_insert_ac`
- [ ] `oltp_write_only_ac`
- [ ] `types_delete_insert_ac`
- [ ] `oltp_read_write_ac`

### Remaining Suites

- [ ] TEXT primary key, file-backed
- [ ] TEXT primary key, file-backed autocommit
- [ ] BLOB primary key, file-backed
- [ ] BLOB primary key, file-backed autocommit
- [ ] Composite primary key, file-backed
- [ ] Composite primary key, file-backed autocommit

## Completed: `oltp_point_select`

- Branch: `perf/sysbench-int-file`
- Current focus: integer primary key, file-backed `oltp_point_select`.
- Baseline: `BENCH_RUNS=3 BENCH_SECTION_MODE=wrapped bash test/sysbench_compare.sh`
  - File-backed `oltp_point_select`: SQLite 49,322 us, DoltLite 19,953 us, 0.40x.
  - File-backed read average: 1.03x.
  - File-backed write average: 1.34x.
- Optimization: route `prollyCursorSeekInt()` through `prollyNodeSearchInt()` instead of encoding the integer key and using generic blob-key search.
- Result: same benchmark command after the change.
  - File-backed `oltp_point_select`: SQLite 47,871 us, DoltLite 19,071 us, 0.40x.
  - DoltLite `oltp_point_select` delta: -4.4%.
  - File-backed read average: 0.99x.
  - File-backed write average: 1.34x.
- Follow-up 7-sample check: `BENCH_RUNS=7 BENCH_SECTION_MODE=wrapped bash test/sysbench_compare.sh`
  - File-backed `oltp_point_select`: SQLite 47,702 us, DoltLite 19,028 us, 0.40x.
  - File-backed read average: 1.01x.
  - File-backed write average: 1.32x.
- Verification:
  - `DOLTLITE_BUILD_DIR=. bash test/run_doltlite_regression_case.sh prolly_int_cursor_boundary`
  - `DOLTLITE_BUILD_DIR=. bash test/run_doltlite_regression_case.sh prolly_int_cursor_seek_past_max`
  - `DOLTLITE_BUILD_DIR=. bash test/run_doltlite_regression_case.sh prolly_blob_cursor_boundary`
  - `DOLTLITE_BUILD_DIR=. bash test/run_doltlite_regression_case.sh prolly_blob_cursor_seek_past_max`
  - `BENCH_RUNS=7 BENCH_SECTION_MODE=wrapped bash test/sysbench_compare.sh`
- Next benchmark after this PR merges: integer primary key, file-backed `oltp_range_select`.

## Current PR

- Branch: `perf/sysbench-int-range`
- Current focus: integer primary key, file-backed `oltp_range_select`.
- Baseline: `BENCH_RUNS=5 BENCH_SECTION_MODE=wrapped bash test/sysbench_compare.sh`
  - File-backed `oltp_range_select`: SQLite 10,021 us, DoltLite 10,560 us, 1.05x.
  - File-backed read average: 1.01x.
  - File-backed write average: 1.34x.
- Focused profile workload: 400,000 `id BETWEEN ? AND ?` selects over an integer primary key table.
  - Before: 3,772,393 us.
  - After: 3,465,365 us.
- Optimization: stop taking an extra prolly cache reference when caching the current tree payload for integer-key cursors. The cursor level already pins the current leaf until the cursor moves or closes, so the extra `prollyCacheGet()`/`prollyCacheRelease()` pair only adds per-row refcount and LRU churn during range scans.
- Result: same benchmark command after the change.
  - File-backed `oltp_range_select`: SQLite 9,929 us, DoltLite 10,156 us, 1.02x.
  - DoltLite `oltp_range_select` delta: -3.8%.
  - File-backed read average: 1.01x.
  - File-backed write average: 1.32x.
- Verification:
  - `make -j$(sysctl -n hw.ncpu) doltlite doltlite-lib`
  - `DOLTLITE_BUILD_DIR=. bash test/run_doltlite_regression_case.sh prolly_int_cursor_boundary`
  - `DOLTLITE_BUILD_DIR=. bash test/run_doltlite_regression_case.sh prolly_int_cursor_seek_past_max`
  - `DOLTLITE_BUILD_DIR=. bash test/run_doltlite_regression_case.sh prolly_blob_cursor_boundary`
  - `DOLTLITE_BUILD_DIR=. bash test/run_doltlite_regression_case.sh prolly_blob_cursor_seek_past_max`
  - `BENCH_RUNS=5 BENCH_SECTION_MODE=wrapped bash test/sysbench_compare.sh`
- Next benchmark after this PR merges: integer primary key, file-backed `oltp_sum_range`.
