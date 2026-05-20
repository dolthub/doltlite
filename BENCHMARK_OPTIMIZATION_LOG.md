# Sysbench Optimization Progress

Branch base: `origin/master` at `d2f8904ad`.

Goal: work through each sysbench benchmark, starting with int-key file-backed, compare local results, make one general optimization per PR, then move to the next benchmark without stacking PRs.

## Benchmark Order

### Integer Primary Key, File-Backed

- [x] `oltp_point_select`
- [ ] `oltp_range_select`
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

## Current PR

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
