# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-09-26 11:07 UTC
>
> Commit: [`b4f19b30af06c5217d41ae6e993b3f85af517caf`](https://github.com/dolthub/doltlite/commit/b4f19b30af06c5217d41ae6e993b3f85af517caf)
>
> Runner: ubuntu24 20260920.314.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/36233189891)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.40s | 11.13s | 1.2× | 1.5% | **PASS** |
| Writes | 1.94s | 3.12s | 1.6× | 1.2% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.97s | 11.35s | 1.1× | 1.7% | **PASS** |
| Writes | 3.17s | 3.88s | 1.2× | 2.1% | **PASS** |
| Autocommit writes | 739.76ms | 2.25s | 3.0× | 5.6% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.29s | 2.65s | 1.2× | 0.9% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 1.73s | 1.90s | 1.1× | 3.5% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.78s | 3.29s | 1.2× | 1.5% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.60s | 3.29s | 1.3× | 1.5% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 377.88ms | 594.44ms | 1.6× | 0.9% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 360.39ms | 572.95ms | 1.6× | 2.1% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 582.26ms | 974.22ms | 1.7× | 1.0% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 614.78ms | 983.05ms | 1.6× | 1.2% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.40s | 2.70s | 1.1× | 1.2% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 1.91s | 1.96s | 1.0× | 2.6% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.78s | 3.29s | 1.2× | 1.4% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.87s | 3.39s | 1.2× | 2.5% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 464.74ms | 661.73ms | 1.4× | 1.5% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.12s | 1.05s | 0.9× | 4.6% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 815.61ms | 1.09s | 1.3× | 2.1% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 773.83ms | 1.08s | 1.4× | 1.8% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.28s | 2.68s | 1.2× | 1.3% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 1.82s | 1.99s | 1.1× | 2.5% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.62s | 3.27s | 1.2× | 1.5% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.86s | 3.40s | 1.2× | 1.5% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 139.88ms | 473.94ms | 3.4× | 5.7% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 193.41ms | 528.31ms | 2.7× | 5.0% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 205.15ms | 624.91ms | 3.0× | 5.5% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 201.32ms | 627.38ms | 3.1× | 5.9% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 21.00ms | 24.63ms | 1.2× | 1.2% | PASS |
| mem_reads | `oltp_range_select` | 9.14ms | 10.55ms | 1.2× | 1.4% | PASS |
| mem_reads | `oltp_sum_range` | 8.54ms | 10.50ms | 1.2× | 1.2% | PASS |
| mem_reads | `oltp_order_range` | 2.36ms | 2.63ms | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_distinct_range` | 3.15ms | 3.75ms | 1.2× | 0.6% | PASS |
| mem_reads | `oltp_index_scan` | 3.43ms | 4.37ms | 1.3× | 1.3% | PASS |
| mem_reads | `select_random_points` | 9.15ms | 10.44ms | 1.1× | 1.2% | PASS |
| mem_reads | `select_random_ranges` | 3.93ms | 4.39ms | 1.1× | 0.9% | PASS |
| mem_reads | `covering_index_scan` | 6.43ms | 9.36ms | 1.5× | 0.9% | PASS |
| mem_reads | `groupby_scan` | 27.04ms | 31.72ms | 1.2× | 0.4% | PASS |
| mem_reads | `index_join` | 4.94ms | 7.42ms | 1.5× | 2.4% | PASS |
| mem_reads | `index_join_scan` | 2.67ms | 4.51ms | 1.7× | 0.9% | PASS |
| mem_reads | `types_table_scan` | 952.67ms | 1.14s | 1.2× | 0.8% | PASS |
| mem_reads | `table_scan` | 1.14s | 1.27s | 1.1× | 0.6% | PASS |
| mem_reads | `oltp_read_only` | 93.70ms | 110.28ms | 1.2× | 0.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 152.04ms | 224.59ms | 1.5× | 2.0% | PASS |
| mem_writes | `oltp_insert` | 13.10ms | 24.05ms | 1.8× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 44.95ms | 82.57ms | 1.8× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 30.20ms | 45.32ms | 1.5× | 1.4% | PASS |
| mem_writes | `oltp_delete_insert` | 39.46ms | 62.45ms | 1.6× | 0.9% | PASS |
| mem_writes | `oltp_write_only` | 18.61ms | 35.83ms | 1.9× | 0.9% | PASS |
| mem_writes | `types_delete_insert` | 21.12ms | 30.40ms | 1.4× | 0.9% | PASS |
| mem_writes | `oltp_read_write` | 58.39ms | 89.23ms | 1.5× | 0.7% | PASS |
| file_reads | `oltp_point_select` | 50.78ms | 33.22ms | 0.7× | 1.4% | PASS |
| file_reads | `oltp_range_select` | 12.16ms | 11.67ms | 1.0× | 1.1% | PASS |
| file_reads | `oltp_sum_range` | 11.55ms | 11.73ms | 1.0× | 1.7% | PASS |
| file_reads | `oltp_order_range` | 2.73ms | 2.80ms | 1.0× | 1.7% | PASS |
| file_reads | `oltp_distinct_range` | 3.55ms | 3.90ms | 1.1× | 1.2% | PASS |
| file_reads | `oltp_index_scan` | 6.57ms | 5.68ms | 0.9× | 1.5% | PASS |
| file_reads | `select_random_points` | 12.52ms | 11.62ms | 0.9× | 1.6% | PASS |
| file_reads | `select_random_ranges` | 7.00ms | 5.49ms | 0.8× | 1.7% | PASS |
| file_reads | `covering_index_scan` | 9.69ms | 10.60ms | 1.1× | 1.1% | PASS |
| file_reads | `groupby_scan` | 27.42ms | 32.04ms | 1.2× | 0.6% | PASS |
| file_reads | `index_join` | 6.83ms | 8.54ms | 1.3× | 1.1% | PASS |
| file_reads | `index_join_scan` | 3.12ms | 4.80ms | 1.5× | 1.7% | PASS |
| file_reads | `types_table_scan` | 975.70ms | 1.16s | 1.2× | 0.9% | PASS |
| file_reads | `table_scan` | 1.14s | 1.28s | 1.1× | 1.1% | PASS |
| file_reads | `oltp_read_only` | 135.76ms | 122.45ms | 0.9× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 163.78ms | 236.21ms | 1.4× | 2.0% | PASS |
| file_writes | `oltp_insert` | 17.21ms | 27.12ms | 1.6× | 1.4% | PASS |
| file_writes | `oltp_update_index` | 60.09ms | 93.34ms | 1.6× | 1.3% | PASS |
| file_writes | `oltp_update_non_index` | 42.69ms | 54.90ms | 1.3× | 1.8% | PASS |
| file_writes | `oltp_delete_insert` | 50.58ms | 72.29ms | 1.4× | 1.2% | PASS |
| file_writes | `oltp_write_only` | 30.72ms | 45.16ms | 1.5× | 1.8% | PASS |
| file_writes | `types_delete_insert` | 29.87ms | 35.40ms | 1.2× | 1.4% | PASS |
| file_writes | `oltp_read_write` | 69.80ms | 97.31ms | 1.4× | 1.5% | PASS |
| ac_reads | `oltp_point_select` | 30.50ms | 33.01ms | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_range_select` | 10.12ms | 11.67ms | 1.2× | 1.4% | PASS |
| ac_reads | `oltp_sum_range` | 9.50ms | 11.67ms | 1.2× | 1.1% | PASS |
| ac_reads | `oltp_order_range` | 2.50ms | 2.80ms | 1.1× | 1.0% | PASS |
| ac_reads | `oltp_distinct_range` | 3.31ms | 3.90ms | 1.2× | 1.3% | PASS |
| ac_reads | `oltp_index_scan` | 4.52ms | 5.58ms | 1.2× | 1.7% | PASS |
| ac_reads | `select_random_points` | 10.51ms | 11.59ms | 1.1× | 1.6% | PASS |
| ac_reads | `select_random_ranges` | 4.94ms | 5.46ms | 1.1× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 7.50ms | 10.51ms | 1.4× | 1.3% | PASS |
| ac_reads | `groupby_scan` | 27.07ms | 31.97ms | 1.2× | 0.5% | PASS |
| ac_reads | `index_join` | 5.65ms | 8.18ms | 1.4× | 1.4% | PASS |
| ac_reads | `index_join_scan` | 2.90ms | 4.74ms | 1.6× | 1.1% | PASS |
| ac_reads | `types_table_scan` | 946.96ms | 1.15s | 1.2× | 0.6% | PASS |
| ac_reads | `table_scan` | 1.11s | 1.26s | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_read_only` | 106.22ms | 123.04ms | 1.2× | 0.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 15.74ms | 45.99ms | 2.9× | 8.2% | PASS |
| ac_writes | `oltp_insert_ac` | 18.20ms | 58.08ms | 3.2× | 7.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 18.46ms | 71.81ms | 3.9× | 5.9% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 15.18ms | 52.67ms | 3.5× | 7.2% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 17.14ms | 63.62ms | 3.7× | 5.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 17.23ms | 61.78ms | 3.6× | 5.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 16.26ms | 53.13ms | 3.3× | 5.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 21.66ms | 66.85ms | 3.1× | 3.8% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 23.55ms | 23.21ms | 1.0× | 2.6% | PASS |
| mem_reads | `oltp_range_select` | 11.24ms | 9.66ms | 0.9× | 3.8% | PASS |
| mem_reads | `oltp_sum_range` | 11.13ms | 9.64ms | 0.9× | 4.2% | PASS |
| mem_reads | `oltp_order_range` | 2.34ms | 2.17ms | 0.9× | 2.8% | PASS |
| mem_reads | `oltp_distinct_range` | 2.83ms | 2.67ms | 0.9× | 2.2% | PASS |
| mem_reads | `oltp_index_scan` | 2.58ms | 4.07ms | 1.6× | 3.4% | PASS |
| mem_reads | `select_random_points` | 15.32ms | 13.68ms | 0.9× | 4.4% | PASS |
| mem_reads | `select_random_ranges` | 4.41ms | 3.88ms | 0.9× | 4.1% | PASS |
| mem_reads | `covering_index_scan` | 3.90ms | 5.67ms | 1.5× | 2.9% | PASS |
| mem_reads | `groupby_scan` | 20.81ms | 20.40ms | 1.0× | 2.4% | PASS |
| mem_reads | `index_join` | 7.83ms | 5.60ms | 0.7× | 4.6% | PASS |
| mem_reads | `index_join_scan` | 2.69ms | 4.68ms | 1.7× | 8.1% | PASS |
| mem_reads | `types_table_scan` | 697.98ms | 796.40ms | 1.1× | 3.6% | PASS |
| mem_reads | `table_scan` | 831.23ms | 917.98ms | 1.1× | 3.5% | PASS |
| mem_reads | `oltp_read_only` | 87.28ms | 82.82ms | 0.9× | 3.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 141.32ms | 210.30ms | 1.5× | 1.6% | PASS |
| mem_writes | `oltp_insert` | 10.66ms | 23.57ms | 2.2× | 2.5% | PASS |
| mem_writes | `oltp_update_index` | 43.55ms | 84.60ms | 1.9× | 2.1% | PASS |
| mem_writes | `oltp_update_non_index` | 32.27ms | 50.02ms | 1.6× | 2.1% | PASS |
| mem_writes | `oltp_delete_insert` | 35.30ms | 62.03ms | 1.8× | 2.1% | PASS |
| mem_writes | `oltp_write_only` | 18.39ms | 35.34ms | 1.9× | 2.8% | PASS |
| mem_writes | `types_delete_insert` | 24.67ms | 31.43ms | 1.3× | 3.6% | PASS |
| mem_writes | `oltp_read_write` | 54.22ms | 75.66ms | 1.4× | 1.9% | PASS |
| file_reads | `oltp_point_select` | 76.70ms | 37.34ms | 0.5× | 2.6% | PASS |
| file_reads | `oltp_range_select` | 16.96ms | 10.87ms | 0.6× | 3.6% | PASS |
| file_reads | `oltp_sum_range` | 16.23ms | 10.79ms | 0.7× | 3.1% | PASS |
| file_reads | `oltp_order_range` | 2.92ms | 2.36ms | 0.8× | 2.9% | PASS |
| file_reads | `oltp_distinct_range` | 3.37ms | 2.79ms | 0.8× | 2.8% | PASS |
| file_reads | `oltp_index_scan` | 8.21ms | 5.63ms | 0.7× | 2.2% | PASS |
| file_reads | `select_random_points` | 20.92ms | 15.17ms | 0.7× | 1.5% | PASS |
| file_reads | `select_random_ranges` | 10.10ms | 5.38ms | 0.5× | 1.5% | PASS |
| file_reads | `covering_index_scan` | 9.95ms | 7.37ms | 0.7× | 2.5% | PASS |
| file_reads | `groupby_scan` | 21.44ms | 20.77ms | 1.0× | 2.3% | PASS |
| file_reads | `index_join` | 11.14ms | 7.50ms | 0.7× | 3.1% | PASS |
| file_reads | `index_join_scan` | 3.39ms | 4.86ms | 1.4× | 2.1% | PASS |
| file_reads | `types_table_scan` | 710.73ms | 816.45ms | 1.1× | 2.6% | PASS |
| file_reads | `table_scan` | 840.59ms | 915.73ms | 1.1× | 5.0% | PASS |
| file_reads | `oltp_read_only` | 158.65ms | 100.62ms | 0.6× | 2.3% | PASS |
| file_writes | `oltp_bulk_insert` | 221.44ms | 280.91ms | 1.3× | 3.7% | PASS |
| file_writes | `oltp_insert` | 19.71ms | 47.46ms | 2.4× (paired 2.3×) | 7.8% | PASS |
| file_writes | `oltp_update_index` | 158.05ms | 163.82ms | 1.0× | 7.4% | PASS |
| file_writes | `oltp_update_non_index` | 131.84ms | 110.02ms | 0.8× | 3.8% | PASS |
| file_writes | `oltp_delete_insert` | 166.57ms | 130.42ms | 0.8× | 2.9% | PASS |
| file_writes | `oltp_write_only` | 111.95ms | 91.88ms | 0.8× | 3.5% | PASS |
| file_writes | `types_delete_insert` | 134.22ms | 78.14ms | 0.6× | 8.3% | PASS |
| file_writes | `oltp_read_write` | 172.31ms | 149.89ms | 0.9× | 5.5% | PASS |
| ac_reads | `oltp_point_select` | 45.69ms | 39.66ms | 0.9× | 2.5% | PASS |
| ac_reads | `oltp_range_select` | 13.80ms | 11.40ms | 0.8× | 4.0% | PASS |
| ac_reads | `oltp_sum_range` | 13.20ms | 11.17ms | 0.8× | 3.0% | PASS |
| ac_reads | `oltp_order_range` | 2.66ms | 2.39ms | 0.9× | 3.0% | PASS |
| ac_reads | `oltp_distinct_range` | 3.18ms | 2.92ms | 0.9× | 1.6% | PASS |
| ac_reads | `oltp_index_scan` | 4.89ms | 5.87ms | 1.2× | 1.9% | PASS |
| ac_reads | `select_random_points` | 18.76ms | 15.77ms | 0.8× | 2.7% | PASS |
| ac_reads | `select_random_ranges` | 6.99ms | 5.67ms | 0.8× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 6.49ms | 7.68ms | 1.2× | 1.5% | PASS |
| ac_reads | `groupby_scan` | 21.65ms | 21.32ms | 1.0× | 1.6% | PASS |
| ac_reads | `index_join` | 9.79ms | 7.59ms | 0.8× | 2.0% | PASS |
| ac_reads | `index_join_scan` | 3.16ms | 4.85ms | 1.5× | 2.1% | PASS |
| ac_reads | `types_table_scan` | 706.83ms | 823.14ms | 1.2× | 3.0% | PASS |
| ac_reads | `table_scan` | 842.91ms | 920.74ms | 1.1× | 4.6% | PASS |
| ac_reads | `oltp_read_only` | 121.61ms | 106.20ms | 0.9× | 3.4% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.94ms | 54.31ms | 2.4× | 4.5% | PASS |
| ac_writes | `oltp_insert_ac` | 24.21ms | 62.38ms | 2.6× | 5.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 25.18ms | 77.53ms | 3.1× | 5.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.70ms | 60.49ms | 2.8× | 6.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.51ms | 70.43ms | 2.9× | 5.0% | PASS |
| ac_writes | `oltp_write_only_ac` | 23.58ms | 68.57ms | 2.9× | 6.5% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.99ms | 61.15ms | 2.7× | 4.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 28.30ms | 73.45ms | 2.6× | 4.9% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 36.08ms | 40.51ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_range_select` | 16.90ms | 15.56ms | 0.9× | 1.5% | PASS |
| mem_reads | `oltp_sum_range` | 15.39ms | 15.75ms | 1.0× | 1.5% | PASS |
| mem_reads | `oltp_order_range` | 3.30ms | 3.42ms | 1.0× | 1.4% | PASS |
| mem_reads | `oltp_distinct_range` | 4.32ms | 4.56ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 3.98ms | 6.60ms | 1.7× | 1.5% | PASS |
| mem_reads | `select_random_points` | 21.11ms | 21.93ms | 1.0× | 2.3% | PASS |
| mem_reads | `select_random_ranges` | 6.58ms | 6.96ms | 1.1× | 2.2% | PASS |
| mem_reads | `covering_index_scan` | 7.75ms | 11.48ms | 1.5× | 1.3% | PASS |
| mem_reads | `groupby_scan` | 33.38ms | 36.23ms | 1.1× | 1.0% | PASS |
| mem_reads | `index_join` | 10.00ms | 9.54ms | 1.0× | 1.6% | PASS |
| mem_reads | `index_join_scan` | 3.67ms | 6.01ms | 1.6× | 1.8% | PASS |
| mem_reads | `types_table_scan` | 1.14s | 1.40s | 1.2× | 2.0% | PASS |
| mem_reads | `table_scan` | 1.32s | 1.56s | 1.2× | 0.6% | PASS |
| mem_reads | `oltp_read_only` | 150.47ms | 152.20ms | 1.0× | 1.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 242.36ms | 374.40ms | 1.5× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 18.50ms | 39.61ms | 2.1× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 63.31ms | 132.15ms | 2.1× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 47.09ms | 79.55ms | 1.7× | 1.3% | PASS |
| mem_writes | `oltp_delete_insert` | 51.51ms | 99.55ms | 1.9× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 27.19ms | 57.13ms | 2.1× | 1.0% | PASS |
| mem_writes | `types_delete_insert` | 37.69ms | 52.47ms | 1.4× | 1.3% | PASS |
| mem_writes | `oltp_read_write` | 94.61ms | 139.36ms | 1.5× | 1.0% | PASS |
| file_reads | `oltp_point_select` | 104.17ms | 59.97ms | 0.6× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 23.24ms | 17.89ms | 0.8× | 1.9% | PASS |
| file_reads | `oltp_sum_range` | 22.22ms | 17.85ms | 0.8× | 1.7% | PASS |
| file_reads | `oltp_order_range` | 4.12ms | 3.71ms | 0.9× | 1.8% | PASS |
| file_reads | `oltp_distinct_range` | 5.19ms | 4.87ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_index_scan` | 10.96ms | 8.82ms | 0.8× | 1.3% | PASS |
| file_reads | `select_random_points` | 29.75ms | 25.21ms | 0.8× | 2.0% | PASS |
| file_reads | `select_random_ranges` | 13.68ms | 9.04ms | 0.7× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 14.85ms | 13.68ms | 0.9× | 1.2% | PASS |
| file_reads | `groupby_scan` | 34.48ms | 36.85ms | 1.1× | 1.0% | PASS |
| file_reads | `index_join` | 14.16ms | 11.56ms | 0.8× | 2.1% | PASS |
| file_reads | `index_join_scan` | 4.67ms | 6.54ms | 1.4× | 2.1% | PASS |
| file_reads | `types_table_scan` | 1.06s | 1.38s | 1.3× | 0.9% | PASS |
| file_reads | `table_scan` | 1.19s | 1.52s | 1.3× | 1.3% | PASS |
| file_reads | `oltp_read_only` | 244.87ms | 177.46ms | 0.7× | 1.7% | PASS |
| file_writes | `oltp_bulk_insert` | 264.10ms | 385.65ms | 1.5× | 0.7% | PASS |
| file_writes | `oltp_insert` | 25.54ms | 46.75ms | 1.8× | 1.9% | PASS |
| file_writes | `oltp_update_index` | 97.43ms | 154.30ms | 1.6× | 2.2% | PASS |
| file_writes | `oltp_update_non_index` | 95.99ms | 96.86ms | 1.0× | 11.6% | PASS |
| file_writes | `oltp_delete_insert` | 86.19ms | 116.73ms | 1.4× | 1.6% | PASS |
| file_writes | `oltp_write_only` | 56.03ms | 70.97ms | 1.3× | 2.4% | PASS |
| file_writes | `types_delete_insert` | 62.94ms | 65.58ms | 1.0× | 2.1% | PASS |
| file_writes | `oltp_read_write` | 127.39ms | 153.78ms | 1.2× | 2.1% | PASS |
| ac_reads | `oltp_point_select` | 58.54ms | 59.51ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_range_select` | 19.39ms | 17.79ms | 0.9× | 2.1% | PASS |
| ac_reads | `oltp_sum_range` | 17.93ms | 17.92ms | 1.0× | 2.2% | PASS |
| ac_reads | `oltp_order_range` | 3.65ms | 3.67ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_distinct_range` | 4.88ms | 4.92ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_index_scan` | 6.45ms | 8.88ms | 1.4× | 2.0% | PASS |
| ac_reads | `select_random_points` | 25.13ms | 25.30ms | 1.0× | 2.0% | PASS |
| ac_reads | `select_random_ranges` | 9.14ms | 9.05ms | 1.0× | 1.5% | PASS |
| ac_reads | `covering_index_scan` | 10.36ms | 13.82ms | 1.3× | 2.3% | PASS |
| ac_reads | `groupby_scan` | 33.83ms | 36.86ms | 1.1× | 0.7% | PASS |
| ac_reads | `index_join` | 12.06ms | 11.61ms | 1.0× | 4.1% | PASS |
| ac_reads | `index_join_scan` | 4.25ms | 6.59ms | 1.6× | 2.6% | PASS |
| ac_reads | `types_table_scan` | 1.06s | 1.37s | 1.3× | 0.7% | PASS |
| ac_reads | `table_scan` | 1.19s | 1.51s | 1.3× | 1.1% | PASS |
| ac_reads | `oltp_read_only` | 172.60ms | 176.59ms | 1.0× | 0.7% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.85ms | 61.63ms | 2.7× | 5.5% | PASS |
| ac_writes | `oltp_insert_ac` | 24.99ms | 78.96ms | 3.2× | 3.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.32ms | 92.15ms | 3.5× | 5.3% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.01ms | 71.69ms | 3.1× | 6.4% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.98ms | 80.42ms | 3.1× | 5.5% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.34ms | 79.84ms | 3.0× | 4.9% | PASS |
| ac_writes | `types_delete_insert_ac` | 24.06ms | 73.14ms | 3.0× | 10.1% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.60ms | 87.08ms | 2.8× | 8.1% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.33ms | 42.16ms | 1.3× | 1.8% | PASS |
| mem_reads | `oltp_range_select` | 18.56ms | 23.41ms | 1.3× | 2.9% | PASS |
| mem_reads | `oltp_sum_range` | 17.39ms | 23.35ms | 1.3× | 1.5% | PASS |
| mem_reads | `oltp_order_range` | 3.50ms | 4.05ms | 1.2× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 4.58ms | 5.25ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_index_scan` | 4.45ms | 6.08ms | 1.4× | 2.0% | PASS |
| mem_reads | `select_random_points` | 27.41ms | 32.95ms | 1.2× | 1.9% | PASS |
| mem_reads | `select_random_ranges` | 7.46ms | 9.42ms | 1.3× | 2.1% | PASS |
| mem_reads | `covering_index_scan` | 7.68ms | 11.12ms | 1.4× | 0.6% | PASS |
| mem_reads | `groupby_scan` | 35.15ms | 41.93ms | 1.2× | 0.9% | PASS |
| mem_reads | `index_join` | 7.88ms | 10.61ms | 1.3× | 1.2% | PASS |
| mem_reads | `index_join_scan` | 3.90ms | 5.98ms | 1.5× | 1.7% | PASS |
| mem_reads | `types_table_scan` | 1.05s | 1.37s | 1.3× | 0.5% | PASS |
| mem_reads | `table_scan` | 1.23s | 1.52s | 1.2× | 0.9% | PASS |
| mem_reads | `oltp_read_only` | 150.75ms | 183.02ms | 1.2× | 1.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 244.40ms | 359.08ms | 1.5× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 18.79ms | 36.16ms | 1.9× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 67.97ms | 127.20ms | 1.9× | 1.8% | PASS |
| mem_writes | `oltp_update_non_index` | 52.66ms | 81.37ms | 1.5× | 1.3% | PASS |
| mem_writes | `oltp_delete_insert` | 48.67ms | 94.40ms | 1.9× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 26.72ms | 54.46ms | 2.0× | 1.1% | PASS |
| mem_writes | `types_delete_insert` | 35.62ms | 55.08ms | 1.5× | 3.1% | PASS |
| mem_writes | `oltp_read_write` | 119.95ms | 175.30ms | 1.5× | 2.3% | PASS |
| file_reads | `oltp_point_select` | 110.21ms | 65.86ms | 0.6× | 1.4% | PASS |
| file_reads | `oltp_range_select` | 29.22ms | 27.45ms | 0.9× | 2.5% | PASS |
| file_reads | `oltp_sum_range` | 27.22ms | 27.32ms | 1.0× | 1.4% | PASS |
| file_reads | `oltp_order_range` | 4.96ms | 4.61ms | 0.9× | 3.6% | PASS |
| file_reads | `oltp_distinct_range` | 5.99ms | 6.11ms | 1.0× | 4.7% | PASS |
| file_reads | `oltp_index_scan` | 12.84ms | 9.33ms | 0.7× | 2.8% | PASS |
| file_reads | `select_random_points` | 41.39ms | 40.37ms | 1.0× | 3.2% | PASS |
| file_reads | `select_random_ranges` | 16.26ms | 12.55ms | 0.8× | 2.0% | PASS |
| file_reads | `covering_index_scan` | 16.32ms | 14.26ms | 0.9× | 2.4% | PASS |
| file_reads | `groupby_scan` | 37.50ms | 43.93ms | 1.2× | 1.4% | PASS |
| file_reads | `index_join` | 13.46ms | 14.37ms | 1.1× | 3.9% | PASS |
| file_reads | `index_join_scan` | 5.55ms | 7.79ms | 1.4× | 5.3% | PASS |
| file_reads | `types_table_scan` | 1.12s | 1.39s | 1.2× | 5.0% | PASS |
| file_reads | `table_scan` | 1.19s | 1.51s | 1.3× | 0.4% | PASS |
| file_reads | `oltp_read_only` | 248.42ms | 211.08ms | 0.8× | 0.5% | PASS |
| file_writes | `oltp_bulk_insert` | 259.27ms | 372.34ms | 1.4× | 0.8% | PASS |
| file_writes | `oltp_insert` | 25.36ms | 42.17ms | 1.7× | 1.5% | PASS |
| file_writes | `oltp_update_index` | 93.63ms | 138.75ms | 1.5× | 1.1% | PASS |
| file_writes | `oltp_update_non_index` | 79.71ms | 100.24ms | 1.3× | 2.4% | PASS |
| file_writes | `oltp_delete_insert` | 81.69ms | 116.72ms | 1.4× | 3.0% | PASS |
| file_writes | `oltp_write_only` | 52.27ms | 70.10ms | 1.3× | 1.6% | PASS |
| file_writes | `types_delete_insert` | 53.49ms | 65.02ms | 1.2× | 2.8% | PASS |
| file_writes | `oltp_read_write` | 128.42ms | 173.04ms | 1.3× | 2.0% | PASS |
| ac_reads | `oltp_point_select` | 57.49ms | 61.96ms | 1.1× | 0.8% | PASS |
| ac_reads | `oltp_range_select` | 22.34ms | 25.98ms | 1.2× | 1.1% | PASS |
| ac_reads | `oltp_sum_range` | 20.55ms | 26.07ms | 1.3× | 1.0% | PASS |
| ac_reads | `oltp_order_range` | 4.01ms | 4.42ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_distinct_range` | 5.11ms | 5.64ms | 1.1× | 1.5% | PASS |
| ac_reads | `oltp_index_scan` | 7.28ms | 8.62ms | 1.2× | 1.5% | PASS |
| ac_reads | `select_random_points` | 31.88ms | 37.47ms | 1.2× | 1.4% | PASS |
| ac_reads | `select_random_ranges` | 10.51ms | 11.82ms | 1.1× | 1.5% | PASS |
| ac_reads | `covering_index_scan` | 10.53ms | 13.40ms | 1.3× | 1.7% | PASS |
| ac_reads | `groupby_scan` | 35.48ms | 42.67ms | 1.2× | 1.0% | PASS |
| ac_reads | `index_join` | 9.60ms | 12.72ms | 1.3× | 1.8% | PASS |
| ac_reads | `index_join_scan` | 4.47ms | 6.55ms | 1.5× | 2.1% | PASS |
| ac_reads | `types_table_scan` | 1.10s | 1.38s | 1.3× | 1.8% | PASS |
| ac_reads | `table_scan` | 1.35s | 1.54s | 1.1× | 2.8% | PASS |
| ac_reads | `oltp_read_only` | 194.59ms | 217.26ms | 1.1× | 2.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.79ms | 60.95ms | 2.7× | 6.0% | PASS |
| ac_writes | `oltp_insert_ac` | 23.48ms | 78.22ms | 3.3× | 7.6% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.27ms | 93.66ms | 3.4× | 4.3% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.98ms | 72.10ms | 3.0× | 8.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.96ms | 80.04ms | 3.2× | 5.8% | PASS |
| ac_writes | `oltp_write_only_ac` | 24.36ms | 78.86ms | 3.2× | 5.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 21.78ms | 72.85ms | 3.3× | 7.1% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.71ms | 90.71ms | 2.8× | 4.1% | PASS |

</details>

</details>

## Version-control latency

Wall time: 3m 14s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 32.12ms | 130.00ms | 24.7% | 1.0% | PASS |
| `status_dirty_many_tables` | 34.93ms | 130.00ms | 26.9% | 0.9% | PASS |
| `diff_regular_working_one_table` | 27.31ms | 120.00ms | 22.8% | 1.0% | PASS |
| `diff_regular_working_many_tables` | 38.82ms | 140.00ms | 27.7% | 0.7% | PASS |
| `diff_stat_working_many_tables` | 38.74ms | 140.00ms | 27.7% | 0.6% | PASS |
| `diff_schema_working_many_tables` | 39.41ms | 140.00ms | 28.1% | 0.8% | PASS |
| `branch_list_many_branches` | 19.68ms | 35.00ms | 56.2% | 1.5% | PASS |
| `branch_create_delete` | 21.48ms | 40.00ms | 53.7% | 1.3% | PASS |
| `at_literal_deep_history` | 21.40ms | 100.00ms | 21.4% | 1.1% | PASS |
| `diff_literal_deep_history` | 21.32ms | 120.00ms | 17.8% | 1.0% | PASS |
| `history_literal_deep_history` | 22.67ms | 150.00ms | 15.1% | 1.6% | PASS |
| `checkout_branch_clean` | 33.83ms | 150.00ms | 22.6% | 1.1% | PASS |
| `merge_data_no_conflicts` | 24.41ms | 50.00ms | 48.8% | 1.4% | PASS |
| `merge_data_secondary_index` | 841.21ms | 2.50s | 33.6% | 0.7% | PASS |
| `merge_schema_no_conflicts` | 20.03ms | 35.00ms | 57.2% | 1.4% | PASS |
| `merge_data_conflicts` | 26.87ms | 180.00ms | 14.9% | 0.9% | PASS |
| `merge_data_conflicts_with_resolve` | 27.91ms | 180.00ms | 15.5% | 1.0% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
