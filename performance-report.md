# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-08-31 11:11 UTC
>
> Commit: [`a0a608dd51db6ce1e1ed3366193b8bf548a33b04`](https://github.com/dolthub/doltlite/commit/a0a608dd51db6ce1e1ed3366193b8bf548a33b04)
>
> Runner: ubuntu24 20260823.283.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/33378680522)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.31s | 9.78s | 1.1× | 1.7% | **PASS** |
| Writes | 1.90s | 3.01s | 1.6× | 1.3% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.67s | 9.79s | 1.0× | 1.7% | **PASS** |
| Writes | 3.50s | 3.88s | 1.1× | 3.9% | **PASS** |
| Autocommit writes | 1.40s | 4.32s | 3.1× | 10.4% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.27s | 2.43s | 1.1× | 1.5% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.00s | 1.84s | 0.9× | 2.8% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.49s | 2.71s | 1.1× | 1.6% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.56s | 2.80s | 1.1× | 1.3% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 346.03ms | 514.96ms | 1.5× | 1.8% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 398.94ms | 629.65ms | 1.6× | 3.1% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 583.25ms | 953.02ms | 1.6× | 1.2% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 573.35ms | 912.77ms | 1.6× | 0.6% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.30s | 2.39s | 1.0× | 1.9% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 1.91s | 1.74s | 0.9× | 1.7% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.73s | 2.79s | 1.0× | 1.4% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.73s | 2.87s | 1.1× | 1.3% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 770.78ms | 779.55ms | 1.0× | 6.2% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.16s | 1.02s | 0.9× | 12.3% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 833.70ms | 1.08s | 1.3× | 2.3% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 741.28ms | 998.84ms | 1.3× | 1.1% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.29s | 2.45s | 1.1× | 2.0% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 1.81s | 1.74s | 1.0× | 1.9% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.77s | 2.86s | 1.0× | 2.0% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.57s | 2.87s | 1.1× | 1.1% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 413.15ms | 1.20s | 2.9× | 22.8% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 567.83ms | 1.64s | 2.9× | 52.2% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 227.96ms | 782.89ms | 3.4× | 6.4% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 190.43ms | 698.08ms | 3.7× | 3.7% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 20.46ms | 22.38ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_range_select` | 8.72ms | 9.38ms | 1.1× | 2.7% | PASS |
| mem_reads | `oltp_sum_range` | 8.24ms | 8.95ms | 1.1× | 1.9% | PASS |
| mem_reads | `oltp_order_range` | 2.50ms | 2.66ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_distinct_range` | 3.42ms | 3.59ms | 1.0× | 1.4% | PASS |
| mem_reads | `oltp_index_scan` | 3.55ms | 4.18ms | 1.2× | 1.0% | PASS |
| mem_reads | `select_random_points` | 10.06ms | 10.12ms | 1.0× | 1.2% | PASS |
| mem_reads | `select_random_ranges` | 2.66ms | 3.27ms | 1.2× | 1.6% | PASS |
| mem_reads | `covering_index_scan` | 3.52ms | 3.44ms | 1.0× | 1.5% | PASS |
| mem_reads | `groupby_scan` | 29.05ms | 29.23ms | 1.0× | 2.2% | PASS |
| mem_reads | `index_join` | 5.18ms | 6.70ms | 1.3× | 1.9% | PASS |
| mem_reads | `index_join_scan` | 2.82ms | 3.90ms | 1.4× | 1.4% | PASS |
| mem_reads | `types_table_scan` | 987.28ms | 1.08s | 1.1× | 0.6% | PASS |
| mem_reads | `table_scan` | 1.09s | 1.15s | 1.1× | 0.7% | PASS |
| mem_reads | `oltp_read_only` | 87.63ms | 95.46ms | 1.1× | 1.2% | PASS |
| mem_writes | `oltp_bulk_insert` | 134.52ms | 176.67ms | 1.3× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 12.26ms | 20.19ms | 1.6× | 1.3% | PASS |
| mem_writes | `oltp_update_index` | 41.44ms | 70.55ms | 1.7× | 1.7% | PASS |
| mem_writes | `oltp_update_non_index` | 27.55ms | 41.88ms | 1.5× | 2.0% | PASS |
| mem_writes | `oltp_delete_insert` | 38.18ms | 56.02ms | 1.5× | 1.9% | PASS |
| mem_writes | `oltp_write_only` | 18.10ms | 34.94ms | 1.9× | 2.1% | PASS |
| mem_writes | `types_delete_insert` | 20.30ms | 28.64ms | 1.4× | 1.9% | PASS |
| mem_writes | `oltp_read_write` | 53.68ms | 86.07ms | 1.6× | 1.4% | PASS |
| file_reads | `oltp_point_select` | 48.41ms | 28.82ms | 0.6× | 1.6% | PASS |
| file_reads | `oltp_range_select` | 12.34ms | 10.78ms | 0.9× | 2.1% | PASS |
| file_reads | `oltp_sum_range` | 11.64ms | 10.12ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_order_range` | 2.88ms | 2.78ms | 1.0× | 1.9% | PASS |
| file_reads | `oltp_distinct_range` | 3.82ms | 3.73ms | 1.0× | 1.2% | PASS |
| file_reads | `oltp_index_scan` | 6.56ms | 4.99ms | 0.8× | 1.8% | PASS |
| file_reads | `select_random_points` | 13.05ms | 11.06ms | 0.8× | 1.9% | PASS |
| file_reads | `select_random_ranges` | 5.45ms | 3.91ms | 0.7× | 2.4% | PASS |
| file_reads | `covering_index_scan` | 6.36ms | 4.17ms | 0.7× | 3.1% | PASS |
| file_reads | `groupby_scan` | 29.25ms | 29.32ms | 1.0× | 2.0% | PASS |
| file_reads | `index_join` | 6.82ms | 7.34ms | 1.1× | 2.1% | PASS |
| file_reads | `index_join_scan` | 3.19ms | 4.13ms | 1.3× | 2.0% | PASS |
| file_reads | `types_table_scan` | 948.14ms | 1.04s | 1.1× | 0.8% | PASS |
| file_reads | `table_scan` | 1.07s | 1.13s | 1.1× | 0.8% | PASS |
| file_reads | `oltp_read_only` | 124.92ms | 102.93ms | 0.8× | 1.5% | PASS |
| file_writes | `oltp_bulk_insert` | 169.52ms | 218.95ms | 1.3× | 2.6% | PASS |
| file_writes | `oltp_insert` | 19.93ms | 29.10ms | 1.5× | 12.4% | PASS |
| file_writes | `oltp_update_index` | 124.13ms | 115.51ms | 0.9× | 6.4% | PASS |
| file_writes | `oltp_update_non_index` | 98.59ms | 79.03ms | 0.8× | 6.1% | PASS |
| file_writes | `oltp_delete_insert` | 107.22ms | 99.03ms | 0.9× | 5.1% | PASS |
| file_writes | `oltp_write_only` | 80.56ms | 69.01ms | 0.9× | 5.6% | PASS |
| file_writes | `types_delete_insert` | 60.21ms | 51.63ms | 0.9× | 16.8% | PASS |
| file_writes | `oltp_read_write` | 110.62ms | 117.30ms | 1.1× | 6.2% | PASS |
| ac_reads | `oltp_point_select` | 28.30ms | 27.53ms | 1.0× | 2.0% | PASS |
| ac_reads | `oltp_range_select` | 9.63ms | 10.11ms | 1.0× | 3.5% | PASS |
| ac_reads | `oltp_sum_range` | 9.10ms | 9.49ms | 1.0× | 3.5% | PASS |
| ac_reads | `oltp_order_range` | 2.48ms | 2.62ms | 1.1× | 3.0% | PASS |
| ac_reads | `oltp_distinct_range` | 3.38ms | 3.51ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_index_scan` | 4.42ms | 4.66ms | 1.1× | 2.7% | PASS |
| ac_reads | `select_random_points` | 10.66ms | 10.40ms | 1.0× | 2.2% | PASS |
| ac_reads | `select_random_ranges` | 3.50ms | 3.75ms | 1.1× | 2.1% | PASS |
| ac_reads | `covering_index_scan` | 4.33ms | 3.96ms | 0.9× | 1.5% | PASS |
| ac_reads | `groupby_scan` | 27.92ms | 28.32ms | 1.0× | 1.0% | PASS |
| ac_reads | `index_join` | 5.79ms | 7.15ms | 1.2× | 1.9% | PASS |
| ac_reads | `index_join_scan` | 2.94ms | 3.96ms | 1.3× | 2.7% | PASS |
| ac_reads | `types_table_scan` | 976.79ms | 1.07s | 1.1× | 0.9% | PASS |
| ac_reads | `table_scan` | 1.10s | 1.16s | 1.1× | 0.5% | PASS |
| ac_reads | `oltp_read_only` | 102.42ms | 107.82ms | 1.1× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 63.79ms | 151.95ms | 2.4× | 30.6% | PASS |
| ac_writes | `oltp_insert_ac` | 61.21ms | 173.47ms | 2.8× | 22.1% | PASS |
| ac_writes | `oltp_update_index_ac` | 56.50ms | 177.31ms | 3.1× | 48.8% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 35.64ms | 103.55ms | 2.9× | 21.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 36.89ms | 103.15ms | 2.8× | 13.0% | PASS |
| ac_writes | `oltp_write_only_ac` | 39.74ms | 146.15ms | 3.7× | 20.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 48.35ms | 152.84ms | 3.2× | 23.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 71.01ms | 191.53ms | 2.7× | 28.2% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 26.58ms | 23.09ms | 0.9× | 2.2% | PASS |
| mem_reads | `oltp_range_select` | 11.45ms | 8.79ms | 0.8× | 2.9% | PASS |
| mem_reads | `oltp_sum_range` | 11.97ms | 9.20ms | 0.8× | 2.8% | PASS |
| mem_reads | `oltp_order_range` | 2.28ms | 1.93ms | 0.8× | 2.9% | PASS |
| mem_reads | `oltp_distinct_range` | 2.60ms | 2.30ms | 0.9× | 2.0% | PASS |
| mem_reads | `oltp_index_scan` | 2.52ms | 3.65ms | 1.4× | 2.7% | PASS |
| mem_reads | `select_random_points` | 16.12ms | 13.61ms | 0.8× | 4.3% | PASS |
| mem_reads | `select_random_ranges` | 2.27ms | 3.27ms | 1.4× | 3.5% | PASS |
| mem_reads | `covering_index_scan` | 2.34ms | 2.80ms | 1.2× | 6.9% | PASS |
| mem_reads | `groupby_scan` | 19.85ms | 18.71ms | 0.9× | 2.3% | PASS |
| mem_reads | `index_join` | 8.30ms | 5.54ms | 0.7× | 7.9% | PASS |
| mem_reads | `index_join_scan` | 2.98ms | 4.78ms | 1.6× | 3.0% | PASS |
| mem_reads | `types_table_scan` | 800.37ms | 761.65ms | 1.0× | 2.5% | PASS |
| mem_reads | `table_scan` | 984.75ms | 896.23ms | 0.9× | 1.6% | PASS |
| mem_reads | `oltp_read_only` | 102.47ms | 82.46ms | 0.8× | 2.2% | PASS |
| mem_writes | `oltp_bulk_insert` | 139.55ms | 211.22ms | 1.5× | 2.0% | PASS |
| mem_writes | `oltp_insert` | 11.28ms | 24.89ms | 2.2× | 2.3% | PASS |
| mem_writes | `oltp_update_index` | 50.73ms | 99.27ms | 2.0× | 2.0% | PASS |
| mem_writes | `oltp_update_non_index` | 39.92ms | 56.83ms | 1.4× | 2.4% | PASS |
| mem_writes | `oltp_delete_insert` | 41.34ms | 74.76ms | 1.8× | 3.8% | PASS |
| mem_writes | `oltp_write_only` | 20.75ms | 42.18ms | 2.0× | 3.9% | PASS |
| mem_writes | `types_delete_insert` | 28.11ms | 34.83ms | 1.2× | 3.8% | PASS |
| mem_writes | `oltp_read_write` | 67.27ms | 85.67ms | 1.3× | 6.4% | PASS |
| file_reads | `oltp_point_select` | 79.67ms | 37.30ms | 0.5× | 1.9% | PASS |
| file_reads | `oltp_range_select` | 15.68ms | 9.60ms | 0.6× | 2.4% | PASS |
| file_reads | `oltp_sum_range` | 16.45ms | 10.01ms | 0.6× | 2.6% | PASS |
| file_reads | `oltp_order_range` | 2.88ms | 2.11ms | 0.7× | 1.7% | PASS |
| file_reads | `oltp_distinct_range` | 3.33ms | 2.53ms | 0.8× | 1.0% | PASS |
| file_reads | `oltp_index_scan` | 8.00ms | 5.51ms | 0.7× | 1.5% | PASS |
| file_reads | `select_random_points` | 20.98ms | 14.50ms | 0.7× | 2.1% | PASS |
| file_reads | `select_random_ranges` | 7.68ms | 4.85ms | 0.6× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 8.03ms | 4.61ms | 0.6× | 1.5% | PASS |
| file_reads | `groupby_scan` | 20.70ms | 18.76ms | 0.9× | 1.3% | PASS |
| file_reads | `index_join` | 11.10ms | 7.15ms | 0.6× | 2.0% | PASS |
| file_reads | `index_join_scan` | 3.42ms | 4.33ms | 1.3× | 1.6% | PASS |
| file_reads | `types_table_scan` | 706.24ms | 705.89ms | 1.0× | 1.2% | PASS |
| file_reads | `table_scan` | 851.76ms | 819.81ms | 1.0× | 2.6% | PASS |
| file_reads | `oltp_read_only` | 153.75ms | 90.87ms | 0.6× | 2.9% | PASS |
| file_writes | `oltp_bulk_insert` | 232.80ms | 277.97ms | 1.2× | 7.4% | PASS |
| file_writes | `oltp_insert` | 22.64ms | 41.15ms | 1.8× | 25.6% | PASS |
| file_writes | `oltp_update_index` | 164.46ms | 163.66ms | 1.0× | 15.2% | PASS |
| file_writes | `oltp_update_non_index` | 131.27ms | 100.80ms | 0.8× | 4.9% | PASS |
| file_writes | `oltp_delete_insert` | 170.90ms | 129.73ms | 0.8× | 5.8% | PASS |
| file_writes | `oltp_write_only` | 129.18ms | 91.04ms | 0.7× | 28.5% | PASS |
| file_writes | `types_delete_insert` | 144.79ms | 80.54ms | 0.6× | 15.0% | PASS |
| file_writes | `oltp_read_write` | 161.23ms | 135.47ms | 0.8× | 9.6% | PASS |
| ac_reads | `oltp_point_select` | 39.95ms | 35.44ms | 0.9× | 1.6% | PASS |
| ac_reads | `oltp_range_select` | 12.41ms | 9.68ms | 0.8× | 1.9% | PASS |
| ac_reads | `oltp_sum_range` | 12.24ms | 9.61ms | 0.8× | 2.2% | PASS |
| ac_reads | `oltp_order_range` | 2.60ms | 2.17ms | 0.8× | 2.0% | PASS |
| ac_reads | `oltp_distinct_range` | 3.11ms | 2.59ms | 0.8× | 1.8% | PASS |
| ac_reads | `oltp_index_scan` | 4.85ms | 5.66ms | 1.2× | 1.8% | PASS |
| ac_reads | `select_random_points` | 19.95ms | 15.33ms | 0.8× | 4.0% | PASS |
| ac_reads | `select_random_ranges` | 4.35ms | 4.93ms | 1.1× | 1.5% | PASS |
| ac_reads | `covering_index_scan` | 4.66ms | 4.65ms | 1.0× | 1.4% | PASS |
| ac_reads | `groupby_scan` | 21.33ms | 19.45ms | 0.9× | 1.9% | PASS |
| ac_reads | `index_join` | 10.01ms | 7.38ms | 0.7× | 2.6% | PASS |
| ac_reads | `index_join_scan` | 3.18ms | 4.33ms | 1.4× | 1.9% | PASS |
| ac_reads | `types_table_scan` | 703.62ms | 706.99ms | 1.0× | 1.7% | PASS |
| ac_reads | `table_scan` | 862.94ms | 820.55ms | 1.0× | 4.4% | PASS |
| ac_reads | `oltp_read_only` | 101.40ms | 89.12ms | 0.9× | 1.5% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 61.05ms | 186.47ms | 3.1× | 57.5% | PASS |
| ac_writes | `oltp_insert_ac` | 61.44ms | 192.04ms | 3.1× | 51.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 50.93ms | 185.10ms | 3.6× | 43.0% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 50.04ms | 158.08ms | 3.2× | 41.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 68.38ms | 203.00ms | 3.0× | 43.4% | PASS |
| ac_writes | `oltp_write_only_ac` | 51.89ms | 192.72ms | 3.7× | 52.5% | PASS |
| ac_writes | `types_delete_insert_ac` | 127.86ms | 274.24ms | 2.1× | 63.6% | PASS |
| ac_writes | `oltp_read_write_ac` | 96.24ms | 246.07ms | 2.6× | 57.7% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.48ms | 37.10ms | 1.1× | 1.7% | PASS |
| mem_reads | `oltp_range_select` | 15.44ms | 13.42ms | 0.9× | 2.1% | PASS |
| mem_reads | `oltp_sum_range` | 14.88ms | 13.27ms | 0.9× | 2.3% | PASS |
| mem_reads | `oltp_order_range` | 3.22ms | 3.08ms | 1.0× | 1.6% | PASS |
| mem_reads | `oltp_distinct_range` | 4.31ms | 4.12ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 4.10ms | 6.32ms | 1.5× | 1.6% | PASS |
| mem_reads | `select_random_points` | 21.69ms | 21.05ms | 1.0× | 2.6% | PASS |
| mem_reads | `select_random_ranges` | 3.38ms | 5.25ms | 1.6× | 2.1% | PASS |
| mem_reads | `covering_index_scan` | 4.26ms | 4.37ms | 1.0× | 1.2% | PASS |
| mem_reads | `groupby_scan` | 34.62ms | 32.42ms | 0.9× | 1.1% | PASS |
| mem_reads | `index_join` | 10.22ms | 8.81ms | 0.9× | 2.2% | PASS |
| mem_reads | `index_join_scan` | 3.82ms | 5.43ms | 1.4× | 1.7% | PASS |
| mem_reads | `types_table_scan` | 1.04s | 1.15s | 1.1× | 0.6% | PASS |
| mem_reads | `table_scan` | 1.16s | 1.27s | 1.1× | 0.7% | PASS |
| mem_reads | `oltp_read_only` | 131.84ms | 131.68ms | 1.0× | 0.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 242.49ms | 359.01ms | 1.5× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 18.68ms | 38.22ms | 2.0× | 1.1% | PASS |
| mem_writes | `oltp_update_index` | 63.21ms | 129.24ms | 2.0× | 1.3% | PASS |
| mem_writes | `oltp_update_non_index` | 46.13ms | 75.45ms | 1.6× | 1.2% | PASS |
| mem_writes | `oltp_delete_insert` | 51.80ms | 100.16ms | 1.9× | 1.4% | PASS |
| mem_writes | `oltp_write_only` | 26.97ms | 58.43ms | 2.2× | 1.2% | PASS |
| mem_writes | `types_delete_insert` | 37.33ms | 51.65ms | 1.4× | 1.1% | PASS |
| mem_writes | `oltp_read_write` | 96.65ms | 140.85ms | 1.5× | 1.8% | PASS |
| file_reads | `oltp_point_select` | 105.75ms | 56.33ms | 0.5× | 1.2% | PASS |
| file_reads | `oltp_range_select` | 23.86ms | 15.61ms | 0.7× | 2.0% | PASS |
| file_reads | `oltp_sum_range` | 22.69ms | 15.44ms | 0.7× | 2.1% | PASS |
| file_reads | `oltp_order_range` | 4.23ms | 3.38ms | 0.8× | 1.8% | PASS |
| file_reads | `oltp_distinct_range` | 5.40ms | 4.51ms | 0.8× | 1.8% | PASS |
| file_reads | `oltp_index_scan` | 11.39ms | 8.33ms | 0.7× | 1.4% | PASS |
| file_reads | `select_random_points` | 30.84ms | 23.89ms | 0.8× | 2.6% | PASS |
| file_reads | `select_random_ranges` | 10.67ms | 7.23ms | 0.7× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 11.58ms | 6.74ms | 0.6× | 1.1% | PASS |
| file_reads | `groupby_scan` | 35.69ms | 33.09ms | 0.9× | 1.4% | PASS |
| file_reads | `index_join` | 14.83ms | 10.95ms | 0.7× | 2.9% | PASS |
| file_reads | `index_join_scan` | 4.90ms | 5.88ms | 1.2× | 2.9% | PASS |
| file_reads | `types_table_scan` | 1.04s | 1.16s | 1.1× | 0.5% | PASS |
| file_reads | `table_scan` | 1.17s | 1.27s | 1.1× | 0.6% | PASS |
| file_reads | `oltp_read_only` | 242.34ms | 161.44ms | 0.7× | 1.2% | PASS |
| file_writes | `oltp_bulk_insert` | 265.19ms | 372.98ms | 1.4× | 1.1% | PASS |
| file_writes | `oltp_insert` | 27.19ms | 47.25ms | 1.7× | 2.3% | PASS |
| file_writes | `oltp_update_index` | 105.19ms | 158.68ms | 1.5× | 2.4% | PASS |
| file_writes | `oltp_update_non_index` | 88.93ms | 93.98ms | 1.1× | 6.8% | PASS |
| file_writes | `oltp_delete_insert` | 91.21ms | 119.39ms | 1.3× | 2.1% | PASS |
| file_writes | `oltp_write_only` | 59.30ms | 71.57ms | 1.2× | 2.4% | PASS |
| file_writes | `types_delete_insert` | 63.80ms | 61.73ms | 1.0× | 2.3% | PASS |
| file_writes | `oltp_read_write` | 132.88ms | 153.87ms | 1.2× | 2.8% | PASS |
| ac_reads | `oltp_point_select` | 59.10ms | 56.19ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_range_select` | 19.51ms | 15.67ms | 0.8× | 2.3% | PASS |
| ac_reads | `oltp_sum_range` | 18.38ms | 15.56ms | 0.8× | 1.8% | PASS |
| ac_reads | `oltp_order_range` | 3.97ms | 3.49ms | 0.9× | 2.0% | PASS |
| ac_reads | `oltp_distinct_range` | 5.03ms | 4.57ms | 0.9× | 2.2% | PASS |
| ac_reads | `oltp_index_scan` | 7.10ms | 8.54ms | 1.2× | 2.0% | PASS |
| ac_reads | `select_random_points` | 26.37ms | 24.34ms | 0.9× | 3.6% | PASS |
| ac_reads | `select_random_ranges` | 6.16ms | 7.31ms | 1.2× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 7.26ms | 6.79ms | 0.9× | 1.9% | PASS |
| ac_reads | `groupby_scan` | 36.32ms | 33.58ms | 0.9× | 0.9% | PASS |
| ac_reads | `index_join` | 13.29ms | 11.04ms | 0.8× | 2.4% | PASS |
| ac_reads | `index_join_scan` | 4.56ms | 5.98ms | 1.3× | 2.3% | PASS |
| ac_reads | `types_table_scan` | 1.11s | 1.19s | 1.1× | 1.8% | PASS |
| ac_reads | `table_scan` | 1.27s | 1.31s | 1.0× | 2.4% | PASS |
| ac_reads | `oltp_read_only` | 178.44ms | 162.53ms | 0.9× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 26.45ms | 83.38ms | 3.2× | 6.9% | PASS |
| ac_writes | `oltp_insert_ac` | 27.92ms | 97.59ms | 3.5× | 5.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 30.03ms | 106.26ms | 3.5× | 5.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 25.18ms | 91.64ms | 3.6× | 6.7% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 27.80ms | 100.10ms | 3.6× | 6.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 28.14ms | 99.10ms | 3.5× | 6.5% | PASS |
| ac_writes | `types_delete_insert_ac` | 26.93ms | 98.98ms | 3.7× | 7.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 35.50ms | 105.85ms | 3.0× | 6.0% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 31.24ms | 38.98ms | 1.2× | 1.9% | PASS |
| mem_reads | `oltp_range_select` | 19.19ms | 20.83ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_sum_range` | 18.12ms | 20.08ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_order_range` | 3.56ms | 3.85ms | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_distinct_range` | 4.74ms | 4.88ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 4.43ms | 5.88ms | 1.3× | 1.8% | PASS |
| mem_reads | `select_random_points` | 27.29ms | 32.04ms | 1.2× | 2.0% | PASS |
| mem_reads | `select_random_ranges` | 7.56ms | 8.85ms | 1.2× | 1.6% | PASS |
| mem_reads | `covering_index_scan` | 4.21ms | 4.08ms | 1.0× | 1.0% | PASS |
| mem_reads | `groupby_scan` | 36.84ms | 37.36ms | 1.0× | 0.8% | PASS |
| mem_reads | `index_join` | 8.11ms | 10.24ms | 1.3× | 1.5% | PASS |
| mem_reads | `index_join_scan` | 4.09ms | 5.48ms | 1.3× | 2.1% | PASS |
| mem_reads | `types_table_scan` | 1.03s | 1.16s | 1.1× | 0.8% | PASS |
| mem_reads | `table_scan` | 1.21s | 1.28s | 1.1× | 2.7% | PASS |
| mem_reads | `oltp_read_only` | 145.33ms | 164.46ms | 1.1× | 0.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 242.80ms | 340.19ms | 1.4× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 18.82ms | 33.97ms | 1.8× | 0.6% | PASS |
| mem_writes | `oltp_update_index` | 63.90ms | 114.28ms | 1.8× | 0.6% | PASS |
| mem_writes | `oltp_update_non_index` | 47.77ms | 73.75ms | 1.5× | 0.8% | PASS |
| mem_writes | `oltp_delete_insert` | 47.26ms | 91.65ms | 1.9× | 0.6% | PASS |
| mem_writes | `oltp_write_only` | 25.25ms | 54.43ms | 2.2× | 0.5% | PASS |
| mem_writes | `types_delete_insert` | 31.32ms | 51.09ms | 1.6× | 0.9% | PASS |
| mem_writes | `oltp_read_write` | 96.22ms | 153.40ms | 1.6× | 0.6% | PASS |
| file_reads | `oltp_point_select` | 101.40ms | 58.35ms | 0.6× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 26.28ms | 22.97ms | 0.9× | 3.0% | PASS |
| file_reads | `oltp_sum_range` | 25.30ms | 22.23ms | 0.9× | 1.2% | PASS |
| file_reads | `oltp_order_range` | 4.41ms | 4.11ms | 0.9× | 1.0% | PASS |
| file_reads | `oltp_distinct_range` | 5.61ms | 5.16ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_index_scan` | 11.83ms | 8.18ms | 0.7× | 2.9% | PASS |
| file_reads | `select_random_points` | 37.05ms | 35.55ms | 1.0× | 1.7% | PASS |
| file_reads | `select_random_ranges` | 15.10ms | 11.02ms | 0.7× | 2.0% | PASS |
| file_reads | `covering_index_scan` | 11.42ms | 6.32ms | 0.6× | 1.7% | PASS |
| file_reads | `groupby_scan` | 38.17ms | 37.75ms | 1.0× | 0.8% | PASS |
| file_reads | `index_join` | 12.10ms | 11.99ms | 1.0× | 1.3% | PASS |
| file_reads | `index_join_scan` | 4.98ms | 5.92ms | 1.2× | 2.4% | PASS |
| file_reads | `types_table_scan` | 1.02s | 1.17s | 1.1× | 0.4% | PASS |
| file_reads | `table_scan` | 1.16s | 1.27s | 1.1× | 0.4% | PASS |
| file_reads | `oltp_read_only` | 249.77ms | 194.07ms | 0.8× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 255.61ms | 349.62ms | 1.4× | 0.8% | PASS |
| file_writes | `oltp_insert` | 25.53ms | 39.92ms | 1.6× | 1.1% | PASS |
| file_writes | `oltp_update_index` | 92.36ms | 128.64ms | 1.4× | 1.1% | PASS |
| file_writes | `oltp_update_non_index` | 73.51ms | 88.46ms | 1.2× | 1.2% | PASS |
| file_writes | `oltp_delete_insert` | 73.78ms | 104.62ms | 1.4× | 1.1% | PASS |
| file_writes | `oltp_write_only` | 48.84ms | 65.61ms | 1.3× | 1.7% | PASS |
| file_writes | `types_delete_insert` | 48.37ms | 57.54ms | 1.2× | 1.1% | PASS |
| file_writes | `oltp_read_write` | 123.28ms | 164.42ms | 1.3× | 0.9% | PASS |
| ac_reads | `oltp_point_select` | 54.60ms | 58.15ms | 1.1× | 0.9% | PASS |
| ac_reads | `oltp_range_select` | 20.92ms | 23.05ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_sum_range` | 20.06ms | 22.30ms | 1.1× | 1.0% | PASS |
| ac_reads | `oltp_order_range` | 3.76ms | 4.13ms | 1.1× | 1.6% | PASS |
| ac_reads | `oltp_distinct_range` | 4.90ms | 5.17ms | 1.1× | 1.5% | PASS |
| ac_reads | `oltp_index_scan` | 6.85ms | 8.21ms | 1.2× | 1.5% | PASS |
| ac_reads | `select_random_points` | 30.40ms | 35.64ms | 1.2× | 1.3% | PASS |
| ac_reads | `select_random_ranges` | 10.00ms | 11.06ms | 1.1× | 1.5% | PASS |
| ac_reads | `covering_index_scan` | 6.56ms | 6.33ms | 1.0× | 1.0% | PASS |
| ac_reads | `groupby_scan` | 37.19ms | 37.86ms | 1.0× | 0.8% | PASS |
| ac_reads | `index_join` | 9.51ms | 12.02ms | 1.3× | 1.6% | PASS |
| ac_reads | `index_join_scan` | 4.37ms | 5.91ms | 1.4× | 1.9% | PASS |
| ac_reads | `types_table_scan` | 1.02s | 1.17s | 1.1× | 0.4% | PASS |
| ac_reads | `table_scan` | 1.16s | 1.27s | 1.1× | 0.3% | PASS |
| ac_reads | `oltp_read_only` | 180.17ms | 195.13ms | 1.1× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.33ms | 71.92ms | 3.4× | 4.1% | PASS |
| ac_writes | `oltp_insert_ac` | 23.25ms | 88.34ms | 3.8× | 2.6% | PASS |
| ac_writes | `oltp_update_index_ac` | 25.48ms | 97.75ms | 3.8× | 3.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.79ms | 80.05ms | 3.7× | 2.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.42ms | 89.92ms | 3.7× | 5.0% | PASS |
| ac_writes | `oltp_write_only_ac` | 23.43ms | 90.81ms | 3.9× | 4.8% | PASS |
| ac_writes | `types_delete_insert_ac` | 20.91ms | 82.22ms | 3.9× | 5.3% | PASS |
| ac_writes | `oltp_read_write_ac` | 29.80ms | 97.07ms | 3.3× | 3.3% | PASS |

</details>

</details>

## Version-control latency

Wall time: 1m 37s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 57.35ms | 130.00ms | 44.1% | 1.5% | PASS |
| `status_dirty_many_tables` | 59.53ms | 130.00ms | 45.8% | 1.4% | PASS |
| `diff_regular_working_one_table` | 53.15ms | 120.00ms | 44.3% | 2.1% | PASS |
| `diff_regular_working_many_tables` | 66.62ms | 140.00ms | 47.6% | 0.9% | PASS |
| `diff_stat_working_many_tables` | 67.41ms | 140.00ms | 48.2% | 1.1% | PASS |
| `diff_schema_working_many_tables` | 68.62ms | 140.00ms | 49.0% | 1.3% | PASS |
| `branch_list_many_branches` | 17.24ms | 35.00ms | 49.3% | 1.6% | PASS |
| `branch_create_delete` | 19.80ms | 40.00ms | 49.5% | 1.9% | PASS |
| `checkout_branch_clean` | 79.70ms | 150.00ms | 53.1% | 1.2% | PASS |
| `merge_data_no_conflicts` | 30.49ms | 50.00ms | 61.0% | 4.6% | PASS |
| `merge_schema_no_conflicts` | 18.82ms | 35.00ms | 53.8% | 1.8% | PASS |
| `merge_data_conflicts` | 24.57ms | 180.00ms | 13.6% | 1.5% | PASS |
| `merge_data_conflicts_with_resolve` | 24.41ms | 180.00ms | 13.6% | 1.9% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
