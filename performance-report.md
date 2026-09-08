# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-09-08 11:12 UTC
>
> Commit: [`08afe4525c61c91c9d2e944d3f9a6d1d4b41a312`](https://github.com/dolthub/doltlite/commit/08afe4525c61c91c9d2e944d3f9a6d1d4b41a312)
>
> Runner: ubuntu24 20260831.293.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/34210851256)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.83s | 10.86s | 1.0× | 1.6% | **PASS** |
| Writes | 2.18s | 3.56s | 1.6× | 1.4% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 11.59s | 11.06s | 1.0× | 1.3% | **PASS** |
| Writes | 3.47s | 4.16s | 1.2× | 2.0% | **PASS** |
| Autocommit writes | 856.76ms | 2.95s | 3.4× | 7.0% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.14s | 2.17s | 1.0× | 1.5% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.84s | 2.82s | 1.0× | 2.1% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.69s | 2.81s | 1.0× | 2.0% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 3.16s | 3.07s | 1.0× | 1.6% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 350.53ms | 542.34ms | 1.5× | 1.5% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 617.13ms | 1.03s | 1.7× | 1.5% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 608.40ms | 1.01s | 1.7× | 2.0% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 601.41ms | 969.84ms | 1.6× | 1.2% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.24s | 2.19s | 1.0× | 1.0% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 3.03s | 2.87s | 0.9× | 1.9% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 3.02s | 2.90s | 1.0× | 1.4% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 3.30s | 3.11s | 0.9× | 1.3% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 889.97ms | 867.58ms | 1.0× | 8.2% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 974.81ms | 1.14s | 1.2× | 4.8% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 835.68ms | 1.11s | 1.3× | 1.8% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 764.58ms | 1.04s | 1.4× | 1.5% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.13s | 2.20s | 1.0× | 1.0% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 3.06s | 2.91s | 1.0× | 1.6% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.71s | 2.84s | 1.0× | 2.2% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 3.06s | 3.08s | 1.0× | 1.2% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 264.85ms | 815.53ms | 3.1× | 21.0% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 224.75ms | 776.76ms | 3.5× | 6.8% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 217.66ms | 763.54ms | 3.5× | 6.6% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 149.50ms | 592.79ms | 4.0× | 5.0% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 19.78ms | 22.27ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_range_select` | 8.96ms | 8.80ms | 1.0× | 1.5% | PASS |
| mem_reads | `oltp_sum_range` | 8.00ms | 8.67ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_order_range` | 2.16ms | 2.25ms | 1.0× | 1.5% | PASS |
| mem_reads | `oltp_distinct_range` | 3.00ms | 3.14ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 3.13ms | 3.88ms | 1.2× | 1.6% | PASS |
| mem_reads | `select_random_points` | 8.40ms | 9.03ms | 1.1× | 1.9% | PASS |
| mem_reads | `select_random_ranges` | 2.47ms | 3.15ms | 1.3× | 1.5% | PASS |
| mem_reads | `covering_index_scan` | 3.32ms | 3.17ms | 1.0× | 1.2% | PASS |
| mem_reads | `groupby_scan` | 25.26ms | 27.73ms | 1.1× | 0.8% | PASS |
| mem_reads | `index_join` | 4.60ms | 6.10ms | 1.3× | 1.9% | PASS |
| mem_reads | `index_join_scan` | 2.69ms | 3.89ms | 1.4× | 2.3% | PASS |
| mem_reads | `types_table_scan` | 879.30ms | 931.67ms | 1.1× | 1.0% | PASS |
| mem_reads | `table_scan` | 1.08s | 1.04s | 1.0× | 2.8% | PASS |
| mem_reads | `oltp_read_only` | 85.88ms | 90.80ms | 1.1× | 1.7% | PASS |
| mem_writes | `oltp_bulk_insert` | 142.89ms | 202.04ms | 1.4× | 1.5% | PASS |
| mem_writes | `oltp_insert` | 12.26ms | 21.04ms | 1.7× | 1.5% | PASS |
| mem_writes | `oltp_update_index` | 41.94ms | 75.29ms | 1.8× | 1.5% | PASS |
| mem_writes | `oltp_update_non_index` | 29.98ms | 45.44ms | 1.5× | 1.5% | PASS |
| mem_writes | `oltp_delete_insert` | 35.74ms | 54.86ms | 1.5× | 1.3% | PASS |
| mem_writes | `oltp_write_only` | 17.80ms | 35.17ms | 2.0× | 1.2% | PASS |
| mem_writes | `types_delete_insert` | 19.18ms | 26.94ms | 1.4× | 1.6% | PASS |
| mem_writes | `oltp_read_write` | 50.74ms | 81.58ms | 1.6× | 0.6% | PASS |
| file_reads | `oltp_point_select` | 84.63ms | 38.57ms | 0.5× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 15.77ms | 10.58ms | 0.7× | 1.6% | PASS |
| file_reads | `oltp_sum_range` | 14.95ms | 10.41ms | 0.7× | 1.0% | PASS |
| file_reads | `oltp_order_range` | 2.93ms | 2.50ms | 0.9× | 1.1% | PASS |
| file_reads | `oltp_distinct_range` | 3.74ms | 3.36ms | 0.9× | 1.2% | PASS |
| file_reads | `oltp_index_scan` | 10.05ms | 5.99ms | 0.6× | 0.9% | PASS |
| file_reads | `select_random_points` | 15.53ms | 10.89ms | 0.7× | 1.5% | PASS |
| file_reads | `select_random_ranges` | 9.13ms | 4.86ms | 0.5× | 0.6% | PASS |
| file_reads | `covering_index_scan` | 10.29ms | 5.23ms | 0.5× | 1.0% | PASS |
| file_reads | `groupby_scan` | 25.96ms | 27.93ms | 1.1× | 0.8% | PASS |
| file_reads | `index_join` | 8.33ms | 7.50ms | 0.9× | 1.5% | PASS |
| file_reads | `index_join_scan` | 3.47ms | 4.07ms | 1.2× | 1.2% | PASS |
| file_reads | `types_table_scan` | 868.73ms | 925.65ms | 1.1× | 0.8% | PASS |
| file_reads | `table_scan` | 987.12ms | 1.02s | 1.0× | 0.8% | PASS |
| file_reads | `oltp_read_only` | 175.47ms | 112.73ms | 0.6× | 0.7% | PASS |
| file_writes | `oltp_bulk_insert` | 192.19ms | 250.88ms | 1.3× | 5.6% | PASS |
| file_writes | `oltp_insert` | 27.88ms | 34.97ms | 1.3× | 4.5% | PASS |
| file_writes | `oltp_update_index` | 139.06ms | 127.38ms | 0.9× | 7.0% | PASS |
| file_writes | `oltp_update_non_index` | 112.42ms | 91.14ms | 0.8× | 11.0% | PASS |
| file_writes | `oltp_delete_insert` | 126.50ms | 103.19ms | 0.8× | 15.1% | PASS |
| file_writes | `oltp_write_only` | 86.78ms | 75.01ms | 0.9× | 4.8% | PASS |
| file_writes | `types_delete_insert` | 74.40ms | 58.05ms | 0.8× | 16.7% | PASS |
| file_writes | `oltp_read_write` | 130.75ms | 126.98ms | 1.0× | 9.4% | PASS |
| ac_reads | `oltp_point_select` | 42.90ms | 39.72ms | 0.9× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 12.27ms | 10.79ms | 0.9× | 2.0% | PASS |
| ac_reads | `oltp_sum_range` | 10.96ms | 10.61ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_order_range` | 2.59ms | 2.52ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_distinct_range` | 3.36ms | 3.38ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_index_scan` | 5.80ms | 6.09ms | 1.1× | 1.1% | PASS |
| ac_reads | `select_random_points` | 11.75ms | 11.19ms | 1.0× | 1.9% | PASS |
| ac_reads | `select_random_ranges` | 4.87ms | 4.88ms | 1.0× | 0.9% | PASS |
| ac_reads | `covering_index_scan` | 6.02ms | 5.38ms | 0.9× | 1.0% | PASS |
| ac_reads | `groupby_scan` | 25.86ms | 28.02ms | 1.1× | 1.0% | PASS |
| ac_reads | `index_join` | 6.16ms | 7.55ms | 1.2× | 0.9% | PASS |
| ac_reads | `index_join_scan` | 3.10ms | 4.07ms | 1.3× | 1.4% | PASS |
| ac_reads | `types_table_scan` | 866.17ms | 924.73ms | 1.1× | 0.7% | PASS |
| ac_reads | `table_scan` | 1.01s | 1.02s | 1.0× | 0.5% | PASS |
| ac_reads | `oltp_read_only` | 117.59ms | 114.80ms | 1.0× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 28.99ms | 82.33ms | 2.8× | 19.2% | PASS |
| ac_writes | `oltp_insert_ac` | 32.80ms | 98.31ms | 3.0× | 20.2% | PASS |
| ac_writes | `oltp_update_index_ac` | 36.28ms | 107.07ms | 3.0× | 31.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 32.69ms | 95.68ms | 2.9× | 31.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 31.50ms | 99.53ms | 3.2× | 16.2% | PASS |
| ac_writes | `oltp_write_only_ac` | 32.37ms | 92.46ms | 2.9× | 12.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 32.52ms | 129.68ms | 4.0× | 50.2% | PASS |
| ac_writes | `oltp_read_write_ac` | 37.70ms | 110.48ms | 2.9× | 21.8% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 37.23ms | 38.31ms | 1.0× | 2.4% | PASS |
| mem_reads | `oltp_range_select` | 17.21ms | 14.07ms | 0.8× | 2.3% | PASS |
| mem_reads | `oltp_sum_range` | 16.11ms | 14.08ms | 0.9× | 1.3% | PASS |
| mem_reads | `oltp_order_range` | 3.31ms | 3.15ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 4.36ms | 4.34ms | 1.0× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 4.04ms | 6.23ms | 1.5× | 1.4% | PASS |
| mem_reads | `select_random_points` | 23.45ms | 21.20ms | 0.9× | 2.7% | PASS |
| mem_reads | `select_random_ranges` | 3.59ms | 5.26ms | 1.5× | 1.3% | PASS |
| mem_reads | `covering_index_scan` | 4.24ms | 4.63ms | 1.1× | 2.6% | PASS |
| mem_reads | `groupby_scan` | 35.07ms | 35.30ms | 1.0× | 0.6% | PASS |
| mem_reads | `index_join` | 10.90ms | 9.66ms | 0.9× | 2.1% | PASS |
| mem_reads | `index_join_scan` | 3.91ms | 5.57ms | 1.4× | 2.7% | PASS |
| mem_reads | `types_table_scan` | 1.13s | 1.19s | 1.1× | 2.7% | PASS |
| mem_reads | `table_scan` | 1.40s | 1.32s | 0.9× | 1.6% | PASS |
| mem_reads | `oltp_read_only` | 143.78ms | 140.69ms | 1.0× | 2.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 246.25ms | 377.32ms | 1.5× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 18.32ms | 39.01ms | 2.1× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 69.54ms | 148.99ms | 2.1× | 2.3% | PASS |
| mem_writes | `oltp_update_non_index` | 53.35ms | 89.86ms | 1.7× | 1.7% | PASS |
| mem_writes | `oltp_delete_insert` | 57.01ms | 108.29ms | 1.9× | 1.3% | PASS |
| mem_writes | `oltp_write_only` | 28.88ms | 63.18ms | 2.2× | 1.1% | PASS |
| mem_writes | `types_delete_insert` | 39.89ms | 54.32ms | 1.4× | 2.1% | PASS |
| mem_writes | `oltp_read_write` | 103.89ms | 151.11ms | 1.5× | 2.1% | PASS |
| file_reads | `oltp_point_select` | 112.31ms | 59.41ms | 0.5× | 1.3% | PASS |
| file_reads | `oltp_range_select` | 25.72ms | 16.49ms | 0.6× | 1.9% | PASS |
| file_reads | `oltp_sum_range` | 24.27ms | 16.45ms | 0.7× | 2.1% | PASS |
| file_reads | `oltp_order_range` | 4.37ms | 3.54ms | 0.8× | 2.1% | PASS |
| file_reads | `oltp_distinct_range` | 5.50ms | 4.75ms | 0.9× | 1.9% | PASS |
| file_reads | `oltp_index_scan` | 11.73ms | 8.49ms | 0.7× | 1.3% | PASS |
| file_reads | `select_random_points` | 33.09ms | 24.39ms | 0.7× | 2.3% | PASS |
| file_reads | `select_random_ranges` | 10.89ms | 7.40ms | 0.7× | 1.3% | PASS |
| file_reads | `covering_index_scan` | 11.83ms | 6.85ms | 0.6× | 1.2% | PASS |
| file_reads | `groupby_scan` | 35.79ms | 35.72ms | 1.0× | 0.9% | PASS |
| file_reads | `index_join` | 15.50ms | 11.02ms | 0.7× | 2.6% | PASS |
| file_reads | `index_join_scan` | 4.86ms | 5.91ms | 1.2× | 2.2% | PASS |
| file_reads | `types_table_scan` | 1.12s | 1.19s | 1.1× | 1.8% | PASS |
| file_reads | `table_scan` | 1.37s | 1.31s | 1.0× | 4.3% | PASS |
| file_reads | `oltp_read_only` | 250.61ms | 169.92ms | 0.7× | 1.7% | PASS |
| file_writes | `oltp_bulk_insert` | 271.90ms | 395.45ms | 1.5× | 1.0% | PASS |
| file_writes | `oltp_insert` | 27.15ms | 47.42ms | 1.7× | 2.3% | PASS |
| file_writes | `oltp_update_index` | 135.82ms | 167.81ms | 1.2× | 7.3% | PASS |
| file_writes | `oltp_update_non_index` | 114.16ms | 104.21ms | 0.9× | 13.9% | PASS |
| file_writes | `oltp_delete_insert` | 99.57ms | 123.06ms | 1.2× | 1.9% | PASS |
| file_writes | `oltp_write_only` | 88.22ms | 76.01ms | 0.9× | 9.8% | PASS |
| file_writes | `types_delete_insert` | 73.20ms | 63.97ms | 0.9× | 1.6% | PASS |
| file_writes | `oltp_read_write` | 164.79ms | 162.43ms | 1.0× | 9.2% | PASS |
| ac_reads | `oltp_point_select` | 61.36ms | 58.53ms | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_range_select` | 20.13ms | 16.39ms | 0.8× | 1.5% | PASS |
| ac_reads | `oltp_sum_range` | 19.18ms | 16.40ms | 0.9× | 1.4% | PASS |
| ac_reads | `oltp_order_range` | 3.77ms | 3.46ms | 0.9× | 1.2% | PASS |
| ac_reads | `oltp_distinct_range` | 4.83ms | 4.69ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_index_scan` | 6.82ms | 8.51ms | 1.2× | 2.0% | PASS |
| ac_reads | `select_random_points` | 27.78ms | 24.52ms | 0.9× | 3.2% | PASS |
| ac_reads | `select_random_ranges` | 6.24ms | 7.42ms | 1.2× | 1.5% | PASS |
| ac_reads | `covering_index_scan` | 7.17ms | 6.89ms | 1.0× | 1.7% | PASS |
| ac_reads | `groupby_scan` | 35.43ms | 35.82ms | 1.0× | 0.7% | PASS |
| ac_reads | `index_join` | 13.21ms | 11.18ms | 0.8× | 1.9% | PASS |
| ac_reads | `index_join_scan` | 4.37ms | 5.93ms | 1.4× | 1.8% | PASS |
| ac_reads | `types_table_scan` | 1.23s | 1.21s | 1.0× | 2.7% | PASS |
| ac_reads | `table_scan` | 1.42s | 1.32s | 0.9× | 1.5% | PASS |
| ac_reads | `oltp_read_only` | 190.53ms | 176.87ms | 0.9× | 1.4% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 26.00ms | 83.99ms | 3.2× | 7.2% | PASS |
| ac_writes | `oltp_insert_ac` | 29.11ms | 95.58ms | 3.3× | 6.6% | PASS |
| ac_writes | `oltp_update_index_ac` | 28.96ms | 107.98ms | 3.7× | 6.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 25.16ms | 92.36ms | 3.7× | 9.2% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 27.03ms | 98.16ms | 3.6× | 5.5% | PASS |
| ac_writes | `oltp_write_only_ac` | 28.35ms | 102.21ms | 3.6× | 8.0% | PASS |
| ac_writes | `types_delete_insert_ac` | 26.21ms | 90.42ms | 3.4× | 6.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 33.93ms | 106.06ms | 3.1× | 6.3% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 37.24ms | 38.26ms | 1.0× | 2.0% | PASS |
| mem_reads | `oltp_range_select` | 17.45ms | 13.95ms | 0.8× | 2.5% | PASS |
| mem_reads | `oltp_sum_range` | 15.82ms | 13.65ms | 0.9× | 1.4% | PASS |
| mem_reads | `oltp_order_range` | 3.08ms | 3.09ms | 1.0× | 1.5% | PASS |
| mem_reads | `oltp_distinct_range` | 4.24ms | 4.29ms | 1.0× | 1.6% | PASS |
| mem_reads | `oltp_index_scan` | 4.04ms | 6.20ms | 1.5× | 1.4% | PASS |
| mem_reads | `select_random_points` | 23.28ms | 21.04ms | 0.9× | 4.0% | PASS |
| mem_reads | `select_random_ranges` | 3.57ms | 5.32ms | 1.5× | 2.3% | PASS |
| mem_reads | `covering_index_scan` | 4.22ms | 4.49ms | 1.1× | 2.1% | PASS |
| mem_reads | `groupby_scan` | 34.08ms | 35.01ms | 1.0× | 0.6% | PASS |
| mem_reads | `index_join` | 10.66ms | 9.47ms | 0.9× | 2.3% | PASS |
| mem_reads | `index_join_scan` | 3.94ms | 5.52ms | 1.4× | 3.4% | PASS |
| mem_reads | `types_table_scan` | 1.10s | 1.19s | 1.1× | 1.5% | PASS |
| mem_reads | `table_scan` | 1.28s | 1.31s | 1.0× | 1.3% | PASS |
| mem_reads | `oltp_read_only` | 144.19ms | 139.55ms | 1.0× | 2.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 246.68ms | 375.76ms | 1.5× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 18.84ms | 39.35ms | 2.1× | 1.2% | PASS |
| mem_writes | `oltp_update_index` | 69.00ms | 144.50ms | 2.1× | 2.4% | PASS |
| mem_writes | `oltp_update_non_index` | 48.07ms | 83.17ms | 1.7× | 2.1% | PASS |
| mem_writes | `oltp_delete_insert` | 51.11ms | 100.63ms | 2.0× | 0.8% | PASS |
| mem_writes | `oltp_write_only` | 28.71ms | 62.71ms | 2.2× | 2.4% | PASS |
| mem_writes | `types_delete_insert` | 39.90ms | 54.70ms | 1.4× | 2.0% | PASS |
| mem_writes | `oltp_read_write` | 106.10ms | 150.04ms | 1.4× | 3.0% | PASS |
| file_reads | `oltp_point_select` | 106.31ms | 57.26ms | 0.5× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 25.22ms | 16.24ms | 0.6× | 1.3% | PASS |
| file_reads | `oltp_sum_range` | 23.63ms | 15.95ms | 0.7× | 2.0% | PASS |
| file_reads | `oltp_order_range` | 4.41ms | 3.52ms | 0.8× | 2.1% | PASS |
| file_reads | `oltp_distinct_range` | 5.74ms | 4.92ms | 0.9× | 1.9% | PASS |
| file_reads | `oltp_index_scan` | 11.50ms | 8.43ms | 0.7× | 1.2% | PASS |
| file_reads | `select_random_points` | 31.00ms | 23.79ms | 0.8× | 2.0% | PASS |
| file_reads | `select_random_ranges` | 10.65ms | 7.25ms | 0.7× | 0.8% | PASS |
| file_reads | `covering_index_scan` | 11.66ms | 6.78ms | 0.6× | 1.4% | PASS |
| file_reads | `groupby_scan` | 36.23ms | 35.88ms | 1.0× | 0.7% | PASS |
| file_reads | `index_join` | 15.59ms | 10.98ms | 0.7× | 2.8% | PASS |
| file_reads | `index_join_scan` | 4.91ms | 5.81ms | 1.2× | 2.4% | PASS |
| file_reads | `types_table_scan` | 1.14s | 1.20s | 1.1× | 1.5% | PASS |
| file_reads | `table_scan` | 1.33s | 1.33s | 1.0× | 1.2% | PASS |
| file_reads | `oltp_read_only` | 267.64ms | 176.06ms | 0.7× | 0.7% | PASS |
| file_writes | `oltp_bulk_insert` | 269.84ms | 392.75ms | 1.5× | 1.1% | PASS |
| file_writes | `oltp_insert` | 26.99ms | 47.45ms | 1.8× | 1.3% | PASS |
| file_writes | `oltp_update_index` | 101.74ms | 158.26ms | 1.6× | 1.3% | PASS |
| file_writes | `oltp_update_non_index` | 85.62ms | 97.86ms | 1.1× | 8.3% | PASS |
| file_writes | `oltp_delete_insert` | 89.19ms | 117.64ms | 1.3× | 2.0% | PASS |
| file_writes | `oltp_write_only` | 58.51ms | 72.04ms | 1.2× | 1.5% | PASS |
| file_writes | `types_delete_insert` | 65.99ms | 64.14ms | 1.0× | 2.0% | PASS |
| file_writes | `oltp_read_write` | 137.80ms | 161.00ms | 1.2× | 2.1% | PASS |
| ac_reads | `oltp_point_select` | 61.46ms | 58.09ms | 0.9× | 2.6% | PASS |
| ac_reads | `oltp_range_select` | 19.68ms | 16.13ms | 0.8× | 3.5% | PASS |
| ac_reads | `oltp_sum_range` | 17.53ms | 16.00ms | 0.9× | 2.2% | PASS |
| ac_reads | `oltp_order_range` | 3.69ms | 3.39ms | 0.9× | 2.7% | PASS |
| ac_reads | `oltp_distinct_range` | 4.72ms | 4.58ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_index_scan` | 6.68ms | 8.37ms | 1.3× | 1.4% | PASS |
| ac_reads | `select_random_points` | 26.18ms | 23.47ms | 0.9× | 1.6% | PASS |
| ac_reads | `select_random_ranges` | 5.95ms | 7.27ms | 1.2× | 1.7% | PASS |
| ac_reads | `covering_index_scan` | 6.78ms | 6.80ms | 1.0× | 2.5% | PASS |
| ac_reads | `groupby_scan` | 35.02ms | 35.45ms | 1.0× | 0.8% | PASS |
| ac_reads | `index_join` | 11.99ms | 10.85ms | 0.9× | 4.1% | PASS |
| ac_reads | `index_join_scan` | 4.36ms | 5.77ms | 1.3× | 2.4% | PASS |
| ac_reads | `types_table_scan` | 1.07s | 1.17s | 1.1× | 0.8% | PASS |
| ac_reads | `table_scan` | 1.25s | 1.30s | 1.0× | 3.1% | PASS |
| ac_reads | `oltp_read_only` | 193.76ms | 173.98ms | 0.9× | 2.1% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 24.05ms | 74.55ms | 3.1× | 7.0% | PASS |
| ac_writes | `oltp_insert_ac` | 25.50ms | 92.44ms | 3.6× | 6.2% | PASS |
| ac_writes | `oltp_update_index_ac` | 28.97ms | 107.08ms | 3.7× | 5.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.61ms | 90.73ms | 3.7× | 7.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 27.71ms | 100.11ms | 3.6× | 5.8% | PASS |
| ac_writes | `oltp_write_only_ac` | 28.37ms | 100.44ms | 3.5× | 8.0% | PASS |
| ac_writes | `types_delete_insert_ac` | 24.98ms | 93.87ms | 3.8× | 8.0% | PASS |
| ac_writes | `oltp_read_write_ac` | 33.46ms | 104.33ms | 3.1× | 5.0% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 34.82ms | 39.02ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_range_select` | 21.09ms | 21.67ms | 1.0× | 1.6% | PASS |
| mem_reads | `oltp_sum_range` | 18.76ms | 20.78ms | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_order_range` | 3.76ms | 3.98ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 4.86ms | 5.18ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 4.78ms | 6.10ms | 1.3× | 1.7% | PASS |
| mem_reads | `select_random_points` | 28.05ms | 30.97ms | 1.1× | 2.2% | PASS |
| mem_reads | `select_random_ranges` | 7.61ms | 8.39ms | 1.1× | 1.2% | PASS |
| mem_reads | `covering_index_scan` | 4.35ms | 4.67ms | 1.1× | 1.9% | PASS |
| mem_reads | `groupby_scan` | 39.10ms | 42.57ms | 1.1× | 0.7% | PASS |
| mem_reads | `index_join` | 7.97ms | 10.79ms | 1.4× | 3.2% | PASS |
| mem_reads | `index_join_scan` | 4.12ms | 6.05ms | 1.5× | 2.5% | PASS |
| mem_reads | `types_table_scan` | 1.27s | 1.28s | 1.0× | 1.0% | PASS |
| mem_reads | `table_scan` | 1.55s | 1.41s | 0.9× | 1.4% | PASS |
| mem_reads | `oltp_read_only` | 156.60ms | 170.12ms | 1.1× | 1.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 248.24ms | 341.80ms | 1.4× | 1.3% | PASS |
| mem_writes | `oltp_insert` | 19.56ms | 35.98ms | 1.8× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 69.59ms | 133.85ms | 1.9× | 1.6% | PASS |
| mem_writes | `oltp_update_non_index` | 53.58ms | 86.36ms | 1.6× | 1.2% | PASS |
| mem_writes | `oltp_delete_insert` | 50.68ms | 99.78ms | 2.0× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 27.72ms | 61.11ms | 2.2× | 1.3% | PASS |
| mem_writes | `types_delete_insert` | 32.52ms | 53.13ms | 1.6× | 0.9% | PASS |
| mem_writes | `oltp_read_write` | 99.52ms | 157.83ms | 1.6× | 1.7% | PASS |
| file_reads | `oltp_point_select` | 118.36ms | 59.55ms | 0.5× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 29.09ms | 23.33ms | 0.8× | 1.9% | PASS |
| file_reads | `oltp_sum_range` | 26.98ms | 22.79ms | 0.8× | 1.3% | PASS |
| file_reads | `oltp_order_range` | 4.53ms | 4.22ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_distinct_range` | 5.56ms | 5.40ms | 1.0× | 1.9% | PASS |
| file_reads | `oltp_index_scan` | 13.14ms | 8.27ms | 0.6× | 1.1% | PASS |
| file_reads | `select_random_points` | 36.37ms | 33.29ms | 0.9× | 1.5% | PASS |
| file_reads | `select_random_ranges` | 16.02ms | 10.65ms | 0.7× | 0.9% | PASS |
| file_reads | `covering_index_scan` | 12.94ms | 6.83ms | 0.5× | 1.2% | PASS |
| file_reads | `groupby_scan` | 39.78ms | 42.64ms | 1.1× | 1.3% | PASS |
| file_reads | `index_join` | 12.50ms | 12.18ms | 1.0× | 1.1% | PASS |
| file_reads | `index_join_scan` | 4.92ms | 6.11ms | 1.2× | 1.1% | PASS |
| file_reads | `types_table_scan` | 1.23s | 1.27s | 1.0× | 2.3% | PASS |
| file_reads | `table_scan` | 1.47s | 1.40s | 1.0× | 2.4% | PASS |
| file_reads | `oltp_read_only` | 277.06ms | 201.29ms | 0.7× | 1.2% | PASS |
| file_writes | `oltp_bulk_insert` | 263.89ms | 351.89ms | 1.3× | 1.5% | PASS |
| file_writes | `oltp_insert` | 25.73ms | 41.23ms | 1.6× | 1.3% | PASS |
| file_writes | `oltp_update_index` | 96.78ms | 141.61ms | 1.5× | 1.4% | PASS |
| file_writes | `oltp_update_non_index` | 78.58ms | 99.10ms | 1.3× | 1.5% | PASS |
| file_writes | `oltp_delete_insert` | 75.97ms | 109.41ms | 1.4× | 1.7% | PASS |
| file_writes | `oltp_write_only` | 49.85ms | 69.63ms | 1.4× | 1.7% | PASS |
| file_writes | `types_delete_insert` | 48.96ms | 58.31ms | 1.2× | 1.7% | PASS |
| file_writes | `oltp_read_write` | 124.83ms | 169.32ms | 1.4× | 1.5% | PASS |
| ac_reads | `oltp_point_select` | 61.23ms | 59.30ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_range_select` | 23.34ms | 23.33ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_sum_range` | 21.47ms | 22.77ms | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_order_range` | 4.10ms | 4.22ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_distinct_range` | 5.19ms | 5.41ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 7.73ms | 8.22ms | 1.1× | 0.9% | PASS |
| ac_reads | `select_random_points` | 30.73ms | 33.05ms | 1.1× | 1.4% | PASS |
| ac_reads | `select_random_ranges` | 10.49ms | 10.64ms | 1.0× | 1.1% | PASS |
| ac_reads | `covering_index_scan` | 7.44ms | 6.81ms | 0.9× | 0.9% | PASS |
| ac_reads | `groupby_scan` | 39.19ms | 42.71ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 9.87ms | 12.20ms | 1.2× | 1.2% | PASS |
| ac_reads | `index_join_scan` | 4.54ms | 6.21ms | 1.4× | 2.5% | PASS |
| ac_reads | `types_table_scan` | 1.16s | 1.25s | 1.1× | 1.5% | PASS |
| ac_reads | `table_scan` | 1.49s | 1.40s | 0.9× | 2.6% | PASS |
| ac_reads | `oltp_read_only` | 192.12ms | 199.01ms | 1.0× | 0.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 15.85ms | 55.87ms | 3.5× | 4.6% | PASS |
| ac_writes | `oltp_insert_ac` | 18.51ms | 75.54ms | 4.1× | 4.7% | PASS |
| ac_writes | `oltp_update_index_ac` | 19.44ms | 84.01ms | 4.3× | 3.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 16.63ms | 67.06ms | 4.0× | 5.4% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 17.35ms | 75.32ms | 4.3× | 4.2% | PASS |
| ac_writes | `oltp_write_only_ac` | 18.40ms | 77.53ms | 4.2× | 5.5% | PASS |
| ac_writes | `types_delete_insert_ac` | 15.68ms | 68.66ms | 4.4× | 7.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 27.63ms | 88.79ms | 3.2× | 6.8% | PASS |

</details>

</details>

## Version-control latency

Wall time: 2m 21s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 103.17ms | 130.00ms | 79.4% | 0.4% | PASS |
| `status_dirty_many_tables` | 107.29ms | 130.00ms | 82.5% | 0.4% | PASS |
| `diff_regular_working_one_table` | 98.75ms | 120.00ms | 82.3% | 0.3% | PASS |
| `diff_regular_working_many_tables` | 112.65ms | 140.00ms | 80.5% | 0.3% | PASS |
| `diff_stat_working_many_tables` | 112.54ms | 140.00ms | 80.4% | 0.3% | PASS |
| `diff_schema_working_many_tables` | 113.81ms | 140.00ms | 81.3% | 0.4% | PASS |
| `branch_list_many_branches` | 25.31ms | 35.00ms | 72.3% | 1.6% | PASS |
| `branch_create_delete` | 27.47ms | 40.00ms | 68.7% | 1.5% | PASS |
| `checkout_branch_clean` | 64.32ms | 150.00ms | 42.9% | 0.6% | PASS |
| `merge_data_no_conflicts` | 31.59ms | 50.00ms | 63.2% | 1.1% | PASS |
| `merge_schema_no_conflicts` | 23.63ms | 35.00ms | 67.5% | 1.7% | PASS |
| `merge_data_conflicts` | 36.20ms | 180.00ms | 20.1% | 0.6% | PASS |
| `merge_data_conflicts_with_resolve` | 35.60ms | 180.00ms | 19.8% | 0.8% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
