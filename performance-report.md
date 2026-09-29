# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-09-29 11:11 UTC
>
> Commit: [`8e8c59ab0ed611e9ae734e30fc5842829c9a265c`](https://github.com/dolthub/doltlite/commit/8e8c59ab0ed611e9ae734e30fc5842829c9a265c)
>
> Runner: ubuntu24 20260920.314.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/36550548615)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.35s | 10.55s | 1.1× | 1.3% | **PASS** |
| Writes | 1.79s | 2.82s | 1.6× | 1.2% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.04s | 10.78s | 1.1× | 1.2% | **PASS** |
| Writes | 3.38s | 3.84s | 1.1× | 2.8% | **PASS** |
| Autocommit writes | 919.25ms | 2.61s | 2.8× | 8.1% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.35s | 2.65s | 1.1× | 1.0% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.80s | 3.04s | 1.1× | 2.4% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.09s | 2.43s | 1.2× | 1.3% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.11s | 2.44s | 1.2× | 0.8% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 386.66ms | 608.35ms | 1.6× | 1.1% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 583.63ms | 952.72ms | 1.6× | 2.1% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 416.05ms | 647.84ms | 1.6× | 1.5% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 406.04ms | 609.21ms | 1.5× | 0.9% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.46s | 2.70s | 1.1× | 1.1% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 3.23s | 3.16s | 1.0× | 2.0% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.16s | 2.46s | 1.1× | 1.3% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.19s | 2.46s | 1.1× | 1.1% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 479.74ms | 683.66ms | 1.4× | 1.2% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 936.47ms | 1.07s | 1.1× | 4.0% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 1.00s | 1.09s | 1.1× | 5.3% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 963.57ms | 996.46ms | 1.0× | 3.9% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.39s | 2.70s | 1.1× | 1.1% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.93s | 3.12s | 1.1× | 1.9% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.10s | 2.45s | 1.2× | 1.2% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.14s | 2.47s | 1.2× | 1.2% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 138.64ms | 483.93ms | 3.5× | 5.2% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 349.84ms | 919.86ms | 2.6× | 12.0% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 251.65ms | 620.47ms | 2.5× | 5.7% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 179.12ms | 585.98ms | 3.3× | 21.7% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 21.84ms | 26.17ms | 1.2× | 0.9% | PASS |
| mem_reads | `oltp_range_select` | 9.37ms | 10.91ms | 1.2× | 1.3% | PASS |
| mem_reads | `oltp_sum_range` | 8.84ms | 10.95ms | 1.2× | 1.3% | PASS |
| mem_reads | `oltp_order_range` | 2.39ms | 2.66ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 3.18ms | 3.78ms | 1.2× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 3.50ms | 4.87ms | 1.4× | 1.1% | PASS |
| mem_reads | `select_random_points` | 9.69ms | 10.97ms | 1.1× | 1.2% | PASS |
| mem_reads | `select_random_ranges` | 3.97ms | 4.67ms | 1.2× | 0.8% | PASS |
| mem_reads | `covering_index_scan` | 6.46ms | 9.68ms | 1.5× | 0.9% | PASS |
| mem_reads | `groupby_scan` | 27.25ms | 31.69ms | 1.2× | 0.6% | PASS |
| mem_reads | `index_join` | 4.97ms | 8.21ms | 1.7× | 1.2% | PASS |
| mem_reads | `index_join_scan` | 2.69ms | 4.94ms | 1.8× | 1.6% | PASS |
| mem_reads | `types_table_scan` | 991.32ms | 1.14s | 1.1× | 0.5% | PASS |
| mem_reads | `table_scan` | 1.16s | 1.27s | 1.1× | 0.6% | PASS |
| mem_reads | `oltp_read_only` | 95.80ms | 112.81ms | 1.2× | 0.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 153.45ms | 224.83ms | 1.5× | 1.1% | PASS |
| mem_writes | `oltp_insert` | 13.16ms | 24.41ms | 1.9× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 46.09ms | 86.20ms | 1.9× | 0.9% | PASS |
| mem_writes | `oltp_update_non_index` | 30.75ms | 46.60ms | 1.5× | 1.3% | PASS |
| mem_writes | `oltp_delete_insert` | 40.36ms | 64.40ms | 1.6× | 1.2% | PASS |
| mem_writes | `oltp_write_only` | 19.27ms | 37.34ms | 1.9× | 0.9% | PASS |
| mem_writes | `types_delete_insert` | 21.55ms | 31.22ms | 1.4× | 1.0% | PASS |
| mem_writes | `oltp_read_write` | 62.04ms | 93.36ms | 1.5× | 1.1% | PASS |
| file_reads | `oltp_point_select` | 51.76ms | 34.60ms | 0.7× | 1.1% | PASS |
| file_reads | `oltp_range_select` | 12.62ms | 12.00ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_sum_range` | 11.99ms | 12.09ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_order_range` | 2.79ms | 2.81ms | 1.0× | 1.0% | PASS |
| file_reads | `oltp_distinct_range` | 3.60ms | 3.91ms | 1.1× | 1.5% | PASS |
| file_reads | `oltp_index_scan` | 6.79ms | 5.92ms | 0.9× | 0.9% | PASS |
| file_reads | `select_random_points` | 13.33ms | 12.10ms | 0.9× | 1.9% | PASS |
| file_reads | `select_random_ranges` | 7.12ms | 5.73ms | 0.8× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 9.83ms | 10.93ms | 1.1× | 1.2% | PASS |
| file_reads | `groupby_scan` | 27.63ms | 31.94ms | 1.2× | 0.6% | PASS |
| file_reads | `index_join` | 6.94ms | 8.96ms | 1.3× | 1.1% | PASS |
| file_reads | `index_join_scan` | 3.14ms | 5.11ms | 1.6× | 1.4% | PASS |
| file_reads | `types_table_scan` | 998.62ms | 1.15s | 1.1× | 0.6% | PASS |
| file_reads | `table_scan` | 1.16s | 1.28s | 1.1× | 0.4% | PASS |
| file_reads | `oltp_read_only` | 139.57ms | 126.57ms | 0.9× | 0.6% | PASS |
| file_writes | `oltp_bulk_insert` | 163.52ms | 239.06ms | 1.5× | 1.9% | PASS |
| file_writes | `oltp_insert` | 17.63ms | 27.70ms | 1.6× | 1.5% | PASS |
| file_writes | `oltp_update_index` | 62.53ms | 97.29ms | 1.6× | 1.2% | PASS |
| file_writes | `oltp_update_non_index` | 44.67ms | 56.92ms | 1.3× | 1.2% | PASS |
| file_writes | `oltp_delete_insert` | 52.55ms | 75.37ms | 1.4× | 1.0% | PASS |
| file_writes | `oltp_write_only` | 32.09ms | 47.21ms | 1.5× | 1.8% | PASS |
| file_writes | `types_delete_insert` | 30.89ms | 36.45ms | 1.2× | 1.3% | PASS |
| file_writes | `oltp_read_write` | 75.87ms | 103.67ms | 1.4× | 1.1% | PASS |
| ac_reads | `oltp_point_select` | 31.58ms | 34.76ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_range_select` | 10.52ms | 12.00ms | 1.1× | 1.6% | PASS |
| ac_reads | `oltp_sum_range` | 9.82ms | 12.13ms | 1.2× | 1.1% | PASS |
| ac_reads | `oltp_order_range` | 2.54ms | 2.82ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_distinct_range` | 3.36ms | 3.93ms | 1.2× | 1.3% | PASS |
| ac_reads | `oltp_index_scan` | 4.74ms | 5.92ms | 1.2× | 1.2% | PASS |
| ac_reads | `select_random_points` | 11.26ms | 12.17ms | 1.1× | 1.4% | PASS |
| ac_reads | `select_random_ranges` | 5.04ms | 5.74ms | 1.1× | 0.9% | PASS |
| ac_reads | `covering_index_scan` | 7.64ms | 10.93ms | 1.4× | 1.0% | PASS |
| ac_reads | `groupby_scan` | 27.34ms | 32.11ms | 1.2× | 0.5% | PASS |
| ac_reads | `index_join` | 5.84ms | 8.95ms | 1.5× | 1.3% | PASS |
| ac_reads | `index_join_scan` | 2.92ms | 5.10ms | 1.7× | 1.1% | PASS |
| ac_reads | `types_table_scan` | 997.40ms | 1.15s | 1.1× | 0.8% | PASS |
| ac_reads | `table_scan` | 1.16s | 1.28s | 1.1× | 0.5% | PASS |
| ac_reads | `oltp_read_only` | 109.98ms | 126.31ms | 1.1× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 15.11ms | 46.57ms | 3.1× | 4.4% | PASS |
| ac_writes | `oltp_insert_ac` | 16.94ms | 59.63ms | 3.5× | 6.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 18.96ms | 73.82ms | 3.9× | 5.4% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 16.39ms | 53.62ms | 3.3× | 6.2% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 17.26ms | 64.54ms | 3.7× | 4.8% | PASS |
| ac_writes | `oltp_write_only_ac` | 17.27ms | 62.51ms | 3.6× | 3.4% | PASS |
| ac_writes | `types_delete_insert_ac` | 15.91ms | 54.15ms | 3.4× | 7.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 20.80ms | 69.10ms | 3.3× | 4.9% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 35.29ms | 40.51ms | 1.1× | 4.4% | PASS |
| mem_reads | `oltp_range_select` | 16.80ms | 15.54ms | 0.9× | 2.1% | PASS |
| mem_reads | `oltp_sum_range` | 15.19ms | 15.67ms | 1.0× | 3.2% | PASS |
| mem_reads | `oltp_order_range` | 3.26ms | 3.29ms | 1.0× | 1.3% | PASS |
| mem_reads | `oltp_distinct_range` | 4.32ms | 4.46ms | 1.0× | 1.6% | PASS |
| mem_reads | `oltp_index_scan` | 3.89ms | 6.54ms | 1.7× | 2.4% | PASS |
| mem_reads | `select_random_points` | 21.90ms | 23.36ms | 1.1× | 3.4% | PASS |
| mem_reads | `select_random_ranges` | 6.78ms | 6.94ms | 1.0× | 1.5% | PASS |
| mem_reads | `covering_index_scan` | 7.71ms | 10.71ms | 1.4× | 1.0% | PASS |
| mem_reads | `groupby_scan` | 34.35ms | 36.30ms | 1.1× | 1.0% | PASS |
| mem_reads | `index_join` | 10.18ms | 9.93ms | 1.0× | 2.6% | PASS |
| mem_reads | `index_join_scan` | 3.73ms | 6.53ms | 1.8× | 2.0% | PASS |
| mem_reads | `types_table_scan` | 1.14s | 1.29s | 1.1× | 3.2% | PASS |
| mem_reads | `table_scan` | 1.36s | 1.42s | 1.0× | 2.5% | PASS |
| mem_reads | `oltp_read_only` | 139.86ms | 147.82ms | 1.1× | 2.5% | PASS |
| mem_writes | `oltp_bulk_insert` | 234.29ms | 336.40ms | 1.4× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 17.59ms | 37.83ms | 2.2× | 1.4% | PASS |
| mem_writes | `oltp_update_index` | 65.02ms | 138.21ms | 2.1× | 1.8% | PASS |
| mem_writes | `oltp_update_non_index` | 47.90ms | 83.11ms | 1.7× | 2.4% | PASS |
| mem_writes | `oltp_delete_insert` | 53.95ms | 102.21ms | 1.9× | 2.4% | PASS |
| mem_writes | `oltp_write_only` | 27.43ms | 58.27ms | 2.1× | 1.4% | PASS |
| mem_writes | `types_delete_insert` | 38.38ms | 54.53ms | 1.4× | 2.5% | PASS |
| mem_writes | `oltp_read_write` | 99.06ms | 142.16ms | 1.4× | 3.7% | PASS |
| file_reads | `oltp_point_select` | 105.15ms | 59.49ms | 0.6× | 1.2% | PASS |
| file_reads | `oltp_range_select` | 24.55ms | 17.75ms | 0.7× | 2.7% | PASS |
| file_reads | `oltp_sum_range` | 22.58ms | 18.14ms | 0.8× | 2.4% | PASS |
| file_reads | `oltp_order_range` | 4.15ms | 3.63ms | 0.9× | 2.1% | PASS |
| file_reads | `oltp_distinct_range` | 5.32ms | 4.84ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 11.21ms | 8.84ms | 0.8× | 1.1% | PASS |
| file_reads | `select_random_points` | 32.02ms | 26.95ms | 0.8× | 2.4% | PASS |
| file_reads | `select_random_ranges` | 14.37ms | 9.03ms | 0.6× | 2.2% | PASS |
| file_reads | `covering_index_scan` | 15.27ms | 13.01ms | 0.9× | 1.3% | PASS |
| file_reads | `groupby_scan` | 35.41ms | 36.74ms | 1.0× | 1.0% | PASS |
| file_reads | `index_join` | 15.46ms | 11.74ms | 0.8× | 2.6% | PASS |
| file_reads | `index_join_scan` | 4.78ms | 7.28ms | 1.5× | 2.1% | PASS |
| file_reads | `types_table_scan` | 1.24s | 1.32s | 1.1× | 2.0% | PASS |
| file_reads | `table_scan` | 1.45s | 1.44s | 1.0× | 1.2% | PASS |
| file_reads | `oltp_read_only` | 252.04ms | 178.40ms | 0.7× | 1.3% | PASS |
| file_writes | `oltp_bulk_insert` | 258.91ms | 353.02ms | 1.4× | 0.8% | PASS |
| file_writes | `oltp_insert` | 28.26ms | 44.39ms | 1.6× | 2.2% | PASS |
| file_writes | `oltp_update_index` | 134.41ms | 160.08ms | 1.2× | 11.3% | PASS |
| file_writes | `oltp_update_non_index` | 102.69ms | 99.96ms | 1.0× | 8.4% | PASS |
| file_writes | `oltp_delete_insert` | 99.66ms | 119.01ms | 1.2× | 1.6% | PASS |
| file_writes | `oltp_write_only` | 85.22ms | 72.83ms | 0.9× | 14.6% | PASS |
| file_writes | `types_delete_insert` | 73.21ms | 66.16ms | 0.9× | 1.5% | PASS |
| file_writes | `oltp_read_write` | 154.11ms | 156.35ms | 1.0× | 5.8% | PASS |
| ac_reads | `oltp_point_select` | 59.02ms | 59.23ms | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_range_select` | 20.44ms | 17.86ms | 0.9× | 2.7% | PASS |
| ac_reads | `oltp_sum_range` | 17.98ms | 18.24ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_order_range` | 3.70ms | 3.62ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_distinct_range` | 4.75ms | 4.82ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_index_scan` | 6.69ms | 8.83ms | 1.3× | 0.9% | PASS |
| ac_reads | `select_random_points` | 26.03ms | 27.08ms | 1.0× | 2.0% | PASS |
| ac_reads | `select_random_ranges` | 9.49ms | 9.08ms | 1.0× | 1.5% | PASS |
| ac_reads | `covering_index_scan` | 10.47ms | 12.92ms | 1.2× | 1.7% | PASS |
| ac_reads | `groupby_scan` | 34.46ms | 36.56ms | 1.1× | 0.9% | PASS |
| ac_reads | `index_join` | 12.65ms | 11.78ms | 0.9× | 2.0% | PASS |
| ac_reads | `index_join_scan` | 4.29ms | 7.16ms | 1.7× | 3.3% | PASS |
| ac_reads | `types_table_scan` | 1.16s | 1.31s | 1.1× | 4.6% | PASS |
| ac_reads | `table_scan` | 1.38s | 1.41s | 1.0× | 5.0% | PASS |
| ac_reads | `oltp_read_only` | 179.84ms | 177.38ms | 1.0× | 1.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 41.13ms | 98.79ms | 2.4× | 13.0% | PASS |
| ac_writes | `oltp_insert_ac` | 47.84ms | 112.21ms | 2.3× | 12.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 44.24ms | 125.04ms | 2.8× | 13.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 41.27ms | 112.36ms | 2.7× | 10.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 44.06ms | 124.68ms | 2.8× | 11.2% | PASS |
| ac_writes | `oltp_write_only_ac` | 45.22ms | 118.52ms | 2.6× | 16.0% | PASS |
| ac_writes | `types_delete_insert_ac` | 40.47ms | 108.70ms | 2.7× | 10.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 45.60ms | 119.56ms | 2.6× | 9.2% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 26.56ms | 27.13ms | 1.0× | 1.3% | PASS |
| mem_reads | `oltp_range_select` | 11.60ms | 11.43ms | 1.0× | 1.7% | PASS |
| mem_reads | `oltp_sum_range` | 11.62ms | 11.18ms | 1.0× | 1.6% | PASS |
| mem_reads | `oltp_order_range` | 2.53ms | 2.68ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_distinct_range` | 3.30ms | 3.64ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_index_scan` | 3.14ms | 4.84ms | 1.5× | 1.9% | PASS |
| mem_reads | `select_random_points` | 17.78ms | 18.07ms | 1.0× | 1.4% | PASS |
| mem_reads | `select_random_ranges` | 4.90ms | 4.79ms | 1.0× | 1.3% | PASS |
| mem_reads | `covering_index_scan` | 5.41ms | 7.30ms | 1.3× | 0.9% | PASS |
| mem_reads | `groupby_scan` | 27.25ms | 29.33ms | 1.1× | 0.7% | PASS |
| mem_reads | `index_join` | 9.22ms | 7.75ms | 0.8× | 2.5% | PASS |
| mem_reads | `index_join_scan` | 2.77ms | 5.40ms | 1.9× | 2.0% | PASS |
| mem_reads | `types_table_scan` | 867.77ms | 1.04s | 1.2× | 0.4% | PASS |
| mem_reads | `table_scan` | 990.58ms | 1.15s | 1.2× | 0.8% | PASS |
| mem_reads | `oltp_read_only` | 100.69ms | 108.53ms | 1.1× | 1.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 163.61ms | 228.23ms | 1.4× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 13.23ms | 25.84ms | 2.0× | 1.2% | PASS |
| mem_writes | `oltp_update_index` | 48.33ms | 97.38ms | 2.0× | 1.9% | PASS |
| mem_writes | `oltp_update_non_index` | 35.08ms | 55.47ms | 1.6× | 1.5% | PASS |
| mem_writes | `oltp_delete_insert` | 39.47ms | 69.54ms | 1.8× | 1.6% | PASS |
| mem_writes | `oltp_write_only` | 20.57ms | 39.99ms | 1.9× | 1.4% | PASS |
| mem_writes | `types_delete_insert` | 28.00ms | 35.75ms | 1.3× | 2.0% | PASS |
| mem_writes | `oltp_read_write` | 67.75ms | 95.63ms | 1.4× | 1.5% | PASS |
| file_reads | `oltp_point_select` | 51.62ms | 33.55ms | 0.6× | 1.3% | PASS |
| file_reads | `oltp_range_select` | 14.48ms | 12.35ms | 0.9× | 1.1% | PASS |
| file_reads | `oltp_sum_range` | 14.29ms | 12.07ms | 0.8× | 1.4% | PASS |
| file_reads | `oltp_order_range` | 2.91ms | 2.85ms | 1.0× | 1.8% | PASS |
| file_reads | `oltp_distinct_range` | 3.66ms | 3.77ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 5.82ms | 5.87ms | 1.0× | 1.6% | PASS |
| file_reads | `select_random_points` | 20.96ms | 19.25ms | 0.9× | 1.6% | PASS |
| file_reads | `select_random_ranges` | 7.73ms | 5.66ms | 0.7× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 8.20ms | 8.20ms | 1.0× | 1.6% | PASS |
| file_reads | `groupby_scan` | 27.56ms | 29.77ms | 1.1× | 0.7% | PASS |
| file_reads | `index_join` | 10.98ms | 9.25ms | 0.8× | 1.6% | PASS |
| file_reads | `index_join_scan` | 3.19ms | 5.79ms | 1.8× | 1.0% | PASS |
| file_reads | `types_table_scan` | 866.31ms | 1.05s | 1.2× | 0.9% | PASS |
| file_reads | `table_scan` | 984.03ms | 1.15s | 1.2× | 0.5% | PASS |
| file_reads | `oltp_read_only` | 136.40ms | 117.06ms | 0.9× | 1.1% | PASS |
| file_writes | `oltp_bulk_insert` | 224.09ms | 306.68ms | 1.4× | 5.7% | PASS |
| file_writes | `oltp_insert` | 21.35ms | 49.04ms | 2.3× | 7.9% | PASS |
| file_writes | `oltp_update_index` | 140.55ms | 179.55ms | 1.3× | 3.3% | PASS |
| file_writes | `oltp_update_non_index` | 119.89ms | 113.99ms | 1.0× | 5.8% | PASS |
| file_writes | `oltp_delete_insert` | 142.75ms | 136.88ms | 1.0× | 4.8% | PASS |
| file_writes | `oltp_write_only` | 99.81ms | 87.69ms | 0.9× | 2.9% | PASS |
| file_writes | `types_delete_insert` | 103.41ms | 72.59ms | 0.7× | 9.1% | PASS |
| file_writes | `oltp_read_write` | 149.93ms | 139.75ms | 0.9× | 2.7% | PASS |
| ac_reads | `oltp_point_select` | 34.88ms | 33.29ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_range_select` | 13.01ms | 12.50ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_sum_range` | 12.79ms | 12.18ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_order_range` | 2.75ms | 2.84ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_distinct_range` | 3.49ms | 3.73ms | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_index_scan` | 4.23ms | 5.63ms | 1.3× | 2.1% | PASS |
| ac_reads | `select_random_points` | 19.12ms | 18.75ms | 1.0× | 1.7% | PASS |
| ac_reads | `select_random_ranges` | 6.11ms | 5.60ms | 0.9× | 1.5% | PASS |
| ac_reads | `covering_index_scan` | 6.44ms | 8.11ms | 1.3× | 1.2% | PASS |
| ac_reads | `groupby_scan` | 27.34ms | 29.66ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 10.30ms | 9.13ms | 0.9× | 1.8% | PASS |
| ac_reads | `index_join_scan` | 3.11ms | 5.79ms | 1.9× | 1.5% | PASS |
| ac_reads | `types_table_scan` | 865.27ms | 1.05s | 1.2× | 0.5% | PASS |
| ac_reads | `table_scan` | 981.03ms | 1.14s | 1.2× | 0.9% | PASS |
| ac_reads | `oltp_read_only` | 111.89ms | 115.75ms | 1.0× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 30.81ms | 64.26ms | 2.1× | 3.5% | PASS |
| ac_writes | `oltp_insert_ac` | 32.94ms | 77.58ms | 2.4× | 3.6% | PASS |
| ac_writes | `oltp_update_index_ac` | 33.24ms | 88.98ms | 2.7× | 7.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 28.48ms | 74.04ms | 2.6× | 6.2% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 32.90ms | 80.57ms | 2.4× | 4.9% | PASS |
| ac_writes | `oltp_write_only_ac` | 32.45ms | 81.16ms | 2.5× | 5.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 26.16ms | 70.03ms | 2.7× | 7.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 34.68ms | 83.84ms | 2.4× | 6.4% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 23.80ms | 27.62ms | 1.2× | 0.8% | PASS |
| mem_reads | `oltp_range_select` | 13.60ms | 17.02ms | 1.3× | 2.2% | PASS |
| mem_reads | `oltp_sum_range` | 12.42ms | 16.36ms | 1.3× | 3.0% | PASS |
| mem_reads | `oltp_order_range` | 2.73ms | 3.12ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_distinct_range` | 3.57ms | 4.07ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 3.39ms | 4.25ms | 1.3× | 1.4% | PASS |
| mem_reads | `select_random_points` | 21.81ms | 25.08ms | 1.2× | 0.7% | PASS |
| mem_reads | `select_random_ranges` | 5.40ms | 6.43ms | 1.2× | 1.0% | PASS |
| mem_reads | `covering_index_scan` | 5.36ms | 6.98ms | 1.3× | 0.5% | PASS |
| mem_reads | `groupby_scan` | 28.14ms | 33.67ms | 1.2× | 0.7% | PASS |
| mem_reads | `index_join` | 6.05ms | 8.08ms | 1.3× | 0.6% | PASS |
| mem_reads | `index_join_scan` | 2.91ms | 4.95ms | 1.7× | 1.6% | PASS |
| mem_reads | `types_table_scan` | 867.21ms | 1.02s | 1.2× | 0.2% | PASS |
| mem_reads | `table_scan` | 1.00s | 1.13s | 1.1× | 0.2% | PASS |
| mem_reads | `oltp_read_only` | 107.43ms | 130.63ms | 1.2× | 0.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 167.52ms | 218.63ms | 1.3× | 0.5% | PASS |
| mem_writes | `oltp_insert` | 13.63ms | 22.80ms | 1.7× | 0.5% | PASS |
| mem_writes | `oltp_update_index` | 47.10ms | 83.53ms | 1.8× | 1.2% | PASS |
| mem_writes | `oltp_update_non_index` | 34.28ms | 52.26ms | 1.5× | 1.1% | PASS |
| mem_writes | `oltp_delete_insert` | 34.72ms | 61.24ms | 1.8× | 1.3% | PASS |
| mem_writes | `oltp_write_only` | 18.96ms | 35.39ms | 1.9× | 0.8% | PASS |
| mem_writes | `types_delete_insert` | 22.41ms | 33.62ms | 1.5× | 0.7% | PASS |
| mem_writes | `oltp_read_write` | 67.41ms | 101.74ms | 1.5× | 1.0% | PASS |
| file_reads | `oltp_point_select` | 47.57ms | 33.15ms | 0.7× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 16.50ms | 17.86ms | 1.1× | 1.8% | PASS |
| file_reads | `oltp_sum_range` | 15.74ms | 17.32ms | 1.1× | 1.1% | PASS |
| file_reads | `oltp_order_range` | 3.13ms | 3.28ms | 1.0× | 1.2% | PASS |
| file_reads | `oltp_distinct_range` | 3.85ms | 4.14ms | 1.1× | 1.1% | PASS |
| file_reads | `oltp_index_scan` | 5.92ms | 4.95ms | 0.8× | 1.8% | PASS |
| file_reads | `select_random_points` | 24.30ms | 25.79ms | 1.1× | 1.3% | PASS |
| file_reads | `select_random_ranges` | 7.89ms | 6.96ms | 0.9× | 1.3% | PASS |
| file_reads | `covering_index_scan` | 7.98ms | 7.64ms | 1.0× | 0.6% | PASS |
| file_reads | `groupby_scan` | 27.84ms | 33.60ms | 1.2× | 0.8% | PASS |
| file_reads | `index_join` | 7.40ms | 8.48ms | 1.1× | 1.0% | PASS |
| file_reads | `index_join_scan` | 3.24ms | 5.13ms | 1.6× | 1.9% | PASS |
| file_reads | `types_table_scan` | 868.43ms | 1.03s | 1.2× | 0.3% | PASS |
| file_reads | `table_scan` | 1.00s | 1.13s | 1.1× | 0.2% | PASS |
| file_reads | `oltp_read_only` | 141.66ms | 138.57ms | 1.0× | 0.5% | PASS |
| file_writes | `oltp_bulk_insert` | 219.28ms | 272.96ms | 1.2× | 0.6% | PASS |
| file_writes | `oltp_insert` | 27.78ms | 41.55ms | 1.5× | 3.0% | PASS |
| file_writes | `oltp_update_index` | 149.93ms | 149.66ms | 1.0× | 1.5% | PASS |
| file_writes | `oltp_update_non_index` | 124.47ms | 106.33ms | 0.9× | 4.8% | PASS |
| file_writes | `oltp_delete_insert` | 131.54ms | 118.68ms | 0.9× | 9.1% | PASS |
| file_writes | `oltp_write_only` | 89.80ms | 83.04ms | 0.9× | 2.2% | PASS |
| file_writes | `types_delete_insert` | 82.26ms | 72.97ms | 0.9× | 11.4% | PASS |
| file_writes | `oltp_read_write` | 138.50ms | 151.26ms | 1.1× | 7.6% | PASS |
| ac_reads | `oltp_point_select` | 31.15ms | 33.21ms | 1.1× | 0.9% | PASS |
| ac_reads | `oltp_range_select` | 15.19ms | 17.90ms | 1.2× | 1.3% | PASS |
| ac_reads | `oltp_sum_range` | 13.89ms | 17.23ms | 1.2× | 1.9% | PASS |
| ac_reads | `oltp_order_range` | 2.90ms | 3.24ms | 1.1× | 2.0% | PASS |
| ac_reads | `oltp_distinct_range` | 3.72ms | 4.16ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_index_scan` | 4.35ms | 5.00ms | 1.1× | 1.7% | PASS |
| ac_reads | `select_random_points` | 22.74ms | 25.90ms | 1.1× | 0.9% | PASS |
| ac_reads | `select_random_ranges` | 6.24ms | 6.99ms | 1.1× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 6.23ms | 7.66ms | 1.2× | 0.7% | PASS |
| ac_reads | `groupby_scan` | 28.12ms | 33.63ms | 1.2× | 0.9% | PASS |
| ac_reads | `index_join` | 6.63ms | 8.58ms | 1.3× | 1.2% | PASS |
| ac_reads | `index_join_scan` | 3.14ms | 5.08ms | 1.6× | 1.8% | PASS |
| ac_reads | `types_table_scan` | 868.48ms | 1.03s | 1.2× | 0.3% | PASS |
| ac_reads | `table_scan` | 1.01s | 1.13s | 1.1× | 0.3% | PASS |
| ac_reads | `oltp_read_only` | 118.68ms | 138.81ms | 1.2× | 0.4% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.08ms | 58.94ms | 2.8× | 17.0% | PASS |
| ac_writes | `oltp_insert_ac` | 21.09ms | 65.06ms | 3.1× | 8.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 22.48ms | 79.15ms | 3.5× | 12.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.72ms | 67.39ms | 3.0× | 40.1% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 22.27ms | 80.43ms | 3.6× | 35.0% | PASS |
| ac_writes | `oltp_write_only_ac` | 23.24ms | 78.24ms | 3.4× | 26.4% | PASS |
| ac_writes | `types_delete_insert_ac` | 20.15ms | 71.03ms | 3.5× | 38.2% | PASS |
| ac_writes | `oltp_read_write_ac` | 26.08ms | 85.74ms | 3.3× | 15.2% | PASS |

</details>

</details>

## Version-control latency

Wall time: 3m 27s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 34.24ms | 130.00ms | 26.3% | 0.6% | PASS |
| `status_dirty_many_tables` | 37.83ms | 130.00ms | 29.1% | 0.8% | PASS |
| `diff_regular_working_one_table` | 29.52ms | 120.00ms | 24.6% | 0.6% | PASS |
| `diff_regular_working_many_tables` | 42.70ms | 140.00ms | 30.5% | 0.6% | PASS |
| `diff_stat_working_many_tables` | 43.09ms | 140.00ms | 30.8% | 1.0% | PASS |
| `diff_schema_working_many_tables` | 43.59ms | 140.00ms | 31.1% | 1.0% | PASS |
| `branch_list_many_branches` | 22.05ms | 35.00ms | 63.0% | 1.0% | PASS |
| `branch_create_delete` | 24.10ms | 40.00ms | 60.2% | 1.6% | PASS |
| `at_literal_deep_history` | 25.47ms | 100.00ms | 25.5% | 1.6% | PASS |
| `diff_literal_deep_history` | 24.99ms | 120.00ms | 20.8% | 1.1% | PASS |
| `history_literal_deep_history` | 26.02ms | 150.00ms | 17.3% | 1.0% | PASS |
| `checkout_branch_clean` | 37.50ms | 150.00ms | 25.0% | 1.4% | PASS |
| `merge_data_no_conflicts` | 28.17ms | 50.00ms | 56.3% | 1.1% | PASS |
| `merge_data_secondary_index` | 869.91ms | 2.50s | 34.8% | 0.6% | PASS |
| `merge_schema_no_conflicts` | 21.49ms | 35.00ms | 61.4% | 1.3% | PASS |
| `merge_data_conflicts` | 29.97ms | 180.00ms | 16.6% | 0.9% | PASS |
| `merge_data_conflicts_with_resolve` | 30.61ms | 180.00ms | 17.0% | 0.7% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
