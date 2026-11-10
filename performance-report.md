# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-11-10 11:33 UTC
>
> Commit: [`26ca913b126664f6ccb28a3ad2f08d79bfe953b5`](https://github.com/dolthub/doltlite/commit/26ca913b126664f6ccb28a3ad2f08d79bfe953b5)
>
> Runner: ubuntu24 20261101.360.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/42791575240)

The SQL workloads compare optimized DoltLite against stock SQLite on the same GitHub-hosted runner, alternating execution order on each repetition. Version-control sections measure DoltLite latency. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 8.27s | 9.43s | 1.1× | 2.2% | **PASS** |
| Writes | 1.68s | 2.78s | 1.7× | 1.6% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 8.69s | 9.45s | 1.1× | 1.4% | **PASS** |
| Writes | 3.74s | 3.96s | 1.1× | 6.5% | **PASS** |
| Autocommit writes | 858.50ms | 2.50s | 2.9× | 7.3% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.23s | 2.36s | 1.1× | 2.2% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.83s | 3.29s | 1.2× | 1.7% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 1.65s | 1.95s | 1.2× | 2.7% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 1.57s | 1.84s | 1.2× | 1.9% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 367.06ms | 598.03ms | 1.6× | 2.7% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 618.07ms | 1.05s | 1.7× | 1.5% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 362.95ms | 592.03ms | 1.6× | 1.8% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 330.79ms | 544.79ms | 1.6× | 1.3% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.31s | 2.37s | 1.0× | 1.4% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.85s | 3.25s | 1.1× | 1.4% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 1.80s | 1.96s | 1.1× | 2.1% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 1.73s | 1.88s | 1.1× | 1.1% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 935.88ms | 932.44ms | 1.0× | 15.7% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 928.05ms | 1.14s | 1.2× | 4.1% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 1.02s | 1.02s | 1.0× | 8.7% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 861.08ms | 857.75ms | 1.0× | 2.7% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.04s | 2.32s | 1.1× | 1.0% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.81s | 3.30s | 1.2× | 1.8% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 1.82s | 2.08s | 1.1× | 2.7% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 1.65s | 1.89s | 1.1× | 1.7% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 226.56ms | 814.58ms | 3.6× | 49.0% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 217.94ms | 684.16ms | 3.1× | 7.2% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 224.07ms | 537.11ms | 2.4× | 8.1% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 189.93ms | 463.16ms | 2.4× | 4.6% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 21.78ms | 24.71ms | 1.1× | 2.2% | PASS |
| mem_reads | `oltp_range_select` | 9.71ms | 10.12ms | 1.0× | 2.5% | PASS |
| mem_reads | `oltp_sum_range` | 8.67ms | 9.84ms | 1.1× | 2.4% | PASS |
| mem_reads | `oltp_order_range` | 2.25ms | 2.45ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_distinct_range` | 3.11ms | 3.42ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_index_scan` | 3.33ms | 4.56ms | 1.4× | 2.3% | PASS |
| mem_reads | `select_random_points` | 12.27ms | 10.20ms | 0.8× | 5.0% | PASS |
| mem_reads | `select_random_ranges` | 3.94ms | 4.11ms | 1.0× | 1.9% | PASS |
| mem_reads | `covering_index_scan` | 6.02ms | 8.09ms | 1.3× | 2.1% | PASS |
| mem_reads | `groupby_scan` | 25.80ms | 28.05ms | 1.1× | 1.0% | PASS |
| mem_reads | `index_join` | 4.77ms | 7.62ms | 1.6× | 4.6% | PASS |
| mem_reads | `index_join_scan` | 2.91ms | 5.03ms | 1.7× | 3.0% | PASS |
| mem_reads | `types_table_scan` | 946.88ms | 1.02s | 1.1× | 1.3% | PASS |
| mem_reads | `table_scan` | 1.09s | 1.12s | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_read_only` | 89.81ms | 98.86ms | 1.1× | 2.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 141.83ms | 230.70ms | 1.6× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 12.36ms | 20.49ms | 1.7× | 1.1% | PASS |
| mem_writes | `oltp_update_index` | 43.44ms | 80.28ms | 1.8× | 3.3% | PASS |
| mem_writes | `oltp_update_non_index` | 31.90ms | 51.18ms | 1.6× | 2.7% | PASS |
| mem_writes | `oltp_delete_insert` | 39.32ms | 62.30ms | 1.6× | 2.8% | PASS |
| mem_writes | `oltp_write_only` | 19.46ms | 38.28ms | 2.0× | 2.9% | PASS |
| mem_writes | `types_delete_insert` | 21.43ms | 29.00ms | 1.4× | 1.8% | PASS |
| mem_writes | `oltp_read_write` | 57.32ms | 85.80ms | 1.5× | 3.8% | PASS |
| file_reads | `oltp_point_select` | 80.16ms | 39.91ms | 0.5× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 15.54ms | 11.65ms | 0.7× | 2.3% | PASS |
| file_reads | `oltp_sum_range` | 14.31ms | 11.30ms | 0.8× | 2.3% | PASS |
| file_reads | `oltp_order_range` | 2.92ms | 2.68ms | 0.9× | 1.1% | PASS |
| file_reads | `oltp_distinct_range` | 3.79ms | 3.61ms | 1.0× | 1.4% | PASS |
| file_reads | `oltp_index_scan` | 9.70ms | 6.25ms | 0.6× | 1.2% | PASS |
| file_reads | `select_random_points` | 18.54ms | 12.00ms | 0.6× | 2.1% | PASS |
| file_reads | `select_random_ranges` | 10.10ms | 5.82ms | 0.6× | 0.8% | PASS |
| file_reads | `covering_index_scan` | 12.68ms | 9.81ms | 0.8× | 0.9% | PASS |
| file_reads | `groupby_scan` | 26.21ms | 27.97ms | 1.1× | 1.0% | PASS |
| file_reads | `index_join` | 8.26ms | 8.28ms | 1.0× | 2.6% | PASS |
| file_reads | `index_join_scan` | 3.48ms | 5.14ms | 1.5× | 3.8% | PASS |
| file_reads | `types_table_scan` | 878.45ms | 1.00s | 1.1× | 2.3% | PASS |
| file_reads | `table_scan` | 1.05s | 1.10s | 1.0× | 3.3% | PASS |
| file_reads | `oltp_read_only` | 174.77ms | 121.20ms | 0.7× | 1.1% | PASS |
| file_writes | `oltp_bulk_insert` | 195.79ms | 286.89ms | 1.5× | 8.6% | PASS |
| file_writes | `oltp_insert` | 26.64ms | 36.43ms | 1.4× | 16.5% | PASS |
| file_writes | `oltp_update_index` | 144.90ms | 142.20ms | 1.0× | 17.5% | PASS |
| file_writes | `oltp_update_non_index` | 116.67ms | 92.44ms | 0.8× | 8.1% | PASS |
| file_writes | `oltp_delete_insert` | 125.02ms | 109.19ms | 0.9× | 14.8% | PASS |
| file_writes | `oltp_write_only` | 94.65ms | 76.23ms | 0.8× | 14.9% | PASS |
| file_writes | `types_delete_insert` | 77.68ms | 66.14ms | 0.9× | 18.7% | PASS |
| file_writes | `oltp_read_write` | 154.53ms | 122.91ms | 0.8× | 16.5% | PASS |
| ac_reads | `oltp_point_select` | 39.15ms | 39.58ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_range_select` | 11.10ms | 11.54ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_sum_range` | 10.18ms | 11.23ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_order_range` | 2.57ms | 2.68ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_distinct_range` | 3.37ms | 3.59ms | 1.1× | 0.5% | PASS |
| ac_reads | `oltp_index_scan` | 5.61ms | 6.12ms | 1.1× | 0.9% | PASS |
| ac_reads | `select_random_points` | 13.06ms | 11.38ms | 0.9× | 1.3% | PASS |
| ac_reads | `select_random_ranges` | 5.93ms | 5.72ms | 1.0× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 8.44ms | 9.66ms | 1.1× | 0.7% | PASS |
| ac_reads | `groupby_scan` | 25.41ms | 27.68ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 6.09ms | 8.08ms | 1.3× | 1.3% | PASS |
| ac_reads | `index_join_scan` | 3.10ms | 5.09ms | 1.6× | 2.1% | PASS |
| ac_reads | `types_table_scan` | 843.11ms | 980.72ms | 1.2× | 0.9% | PASS |
| ac_reads | `table_scan` | 954.07ms | 1.08s | 1.1× | 0.6% | PASS |
| ac_reads | `oltp_read_only` | 113.64ms | 119.06ms | 1.0× | 1.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 33.82ms | 99.43ms | 2.9× | 54.3% | PASS |
| ac_writes | `oltp_insert_ac` | 23.20ms | 83.43ms | 3.6× | 31.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 30.73ms | 112.25ms | 3.7× | 49.5% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 27.21ms | 114.59ms | 4.2× | 61.7% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 32.13ms | 120.93ms | 3.8× | 45.5% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.17ms | 91.57ms | 3.5× | 48.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 21.59ms | 73.46ms | 3.4× | 24.3% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.71ms | 118.92ms | 3.8× | 52.5% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 36.62ms | 43.81ms | 1.2× | 1.6% | PASS |
| mem_reads | `oltp_range_select` | 17.11ms | 17.34ms | 1.0× | 1.6% | PASS |
| mem_reads | `oltp_sum_range` | 15.32ms | 16.56ms | 1.1× | 1.7% | PASS |
| mem_reads | `oltp_order_range` | 3.34ms | 3.63ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 4.46ms | 4.79ms | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_index_scan` | 4.20ms | 7.33ms | 1.7× | 1.7% | PASS |
| mem_reads | `select_random_points` | 24.17ms | 24.79ms | 1.0× | 3.3% | PASS |
| mem_reads | `select_random_ranges` | 7.08ms | 7.39ms | 1.0× | 2.4% | PASS |
| mem_reads | `covering_index_scan` | 7.99ms | 11.76ms | 1.5× | 1.5% | PASS |
| mem_reads | `groupby_scan` | 35.49ms | 38.17ms | 1.1× | 1.2% | PASS |
| mem_reads | `index_join` | 11.77ms | 12.01ms | 1.0× | 3.0% | PASS |
| mem_reads | `index_join_scan` | 3.72ms | 6.86ms | 1.8× | 2.0% | PASS |
| mem_reads | `types_table_scan` | 1.20s | 1.42s | 1.2× | 3.8% | PASS |
| mem_reads | `table_scan` | 1.31s | 1.52s | 1.2× | 5.5% | PASS |
| mem_reads | `oltp_read_only` | 151.03ms | 160.09ms | 1.1× | 1.9% | PASS |
| mem_writes | `oltp_bulk_insert` | 244.22ms | 383.13ms | 1.6× | 1.1% | PASS |
| mem_writes | `oltp_insert` | 18.09ms | 37.86ms | 2.1× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 69.22ms | 147.96ms | 2.1× | 2.0% | PASS |
| mem_writes | `oltp_update_non_index` | 51.79ms | 89.58ms | 1.7× | 1.6% | PASS |
| mem_writes | `oltp_delete_insert` | 57.41ms | 110.41ms | 1.9× | 1.6% | PASS |
| mem_writes | `oltp_write_only` | 29.77ms | 57.52ms | 1.9× | 1.8% | PASS |
| mem_writes | `types_delete_insert` | 40.99ms | 68.51ms | 1.7× | 1.3% | PASS |
| mem_writes | `oltp_read_write` | 106.59ms | 150.67ms | 1.4× | 1.4% | PASS |
| file_reads | `oltp_point_select` | 105.15ms | 61.78ms | 0.6× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 24.94ms | 19.49ms | 0.8× | 1.4% | PASS |
| file_reads | `oltp_sum_range` | 23.22ms | 18.99ms | 0.8× | 1.8% | PASS |
| file_reads | `oltp_order_range` | 4.31ms | 3.88ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_distinct_range` | 5.36ms | 5.01ms | 0.9× | 1.5% | PASS |
| file_reads | `oltp_index_scan` | 10.94ms | 9.03ms | 0.8× | 1.2% | PASS |
| file_reads | `select_random_points` | 35.19ms | 29.05ms | 0.8× | 3.5% | PASS |
| file_reads | `select_random_ranges` | 14.15ms | 9.28ms | 0.7× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 15.26ms | 13.65ms | 0.9× | 1.0% | PASS |
| file_reads | `groupby_scan` | 35.97ms | 37.82ms | 1.1× | 0.8% | PASS |
| file_reads | `index_join` | 15.94ms | 13.07ms | 0.8× | 2.2% | PASS |
| file_reads | `index_join_scan` | 4.75ms | 7.18ms | 1.5× | 1.7% | PASS |
| file_reads | `types_table_scan` | 1.05s | 1.35s | 1.3× | 1.0% | PASS |
| file_reads | `table_scan` | 1.26s | 1.49s | 1.2× | 1.1% | PASS |
| file_reads | `oltp_read_only` | 238.99ms | 181.14ms | 0.8× | 1.6% | PASS |
| file_writes | `oltp_bulk_insert` | 268.56ms | 399.02ms | 1.5× | 0.7% | PASS |
| file_writes | `oltp_insert` | 25.96ms | 43.70ms | 1.7× | 1.6% | PASS |
| file_writes | `oltp_update_index` | 132.79ms | 165.05ms | 1.2× | 6.3% | PASS |
| file_writes | `oltp_update_non_index` | 106.23ms | 103.67ms | 1.0× | 16.0% | PASS |
| file_writes | `oltp_delete_insert` | 102.09ms | 129.94ms | 1.3× | 1.4% | PASS |
| file_writes | `oltp_write_only` | 77.98ms | 69.32ms | 0.9× | 10.8% | PASS |
| file_writes | `types_delete_insert` | 73.91ms | 77.12ms | 1.0× | 1.9% | PASS |
| file_writes | `oltp_read_write` | 140.54ms | 155.93ms | 1.1× | 6.7% | PASS |
| ac_reads | `oltp_point_select` | 57.15ms | 60.59ms | 1.1× | 1.5% | PASS |
| ac_reads | `oltp_range_select` | 18.98ms | 19.01ms | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_sum_range` | 17.85ms | 18.55ms | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_order_range` | 3.67ms | 3.85ms | 1.1× | 2.8% | PASS |
| ac_reads | `oltp_distinct_range` | 4.75ms | 5.01ms | 1.1× | 2.5% | PASS |
| ac_reads | `oltp_index_scan` | 6.56ms | 9.08ms | 1.4× | 2.4% | PASS |
| ac_reads | `select_random_points` | 25.39ms | 26.78ms | 1.1× | 1.2% | PASS |
| ac_reads | `select_random_ranges` | 9.43ms | 9.23ms | 1.0× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 10.50ms | 13.38ms | 1.3× | 1.1% | PASS |
| ac_reads | `groupby_scan` | 34.82ms | 37.44ms | 1.1× | 0.9% | PASS |
| ac_reads | `index_join` | 12.87ms | 12.66ms | 1.0× | 2.5% | PASS |
| ac_reads | `index_join_scan` | 4.45ms | 8.06ms | 1.8× | 3.1% | PASS |
| ac_reads | `types_table_scan` | 1.24s | 1.43s | 1.2× | 1.9% | PASS |
| ac_reads | `table_scan` | 1.18s | 1.46s | 1.2× | 1.8% | PASS |
| ac_reads | `oltp_read_only` | 183.48ms | 187.65ms | 1.0× | 1.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 24.64ms | 69.16ms | 2.8× | 6.3% | PASS |
| ac_writes | `oltp_insert_ac` | 27.59ms | 83.92ms | 3.0× | 7.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 28.01ms | 97.04ms | 3.5× | 8.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.65ms | 77.94ms | 3.2× | 7.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 27.01ms | 88.61ms | 3.3× | 6.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.70ms | 90.06ms | 3.4× | 7.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 25.26ms | 80.52ms | 3.2× | 9.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 34.08ms | 96.92ms | 2.8× | 8.2% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 23.62ms | 25.41ms | 1.1× | 2.2% | PASS |
| mem_reads | `oltp_range_select` | 9.46ms | 10.25ms | 1.1× | 3.0% | PASS |
| mem_reads | `oltp_sum_range` | 9.94ms | 10.25ms | 1.0× | 2.6% | PASS |
| mem_reads | `oltp_order_range` | 2.16ms | 2.33ms | 1.1× | 3.6% | PASS |
| mem_reads | `oltp_distinct_range` | 2.61ms | 2.81ms | 1.1× | 1.9% | PASS |
| mem_reads | `oltp_index_scan` | 2.57ms | 4.66ms | 1.8× | 3.5% | PASS |
| mem_reads | `select_random_points` | 14.18ms | 14.91ms | 1.1× | 4.0% | PASS |
| mem_reads | `select_random_ranges` | 3.86ms | 4.15ms | 1.1× | 9.1% | PASS |
| mem_reads | `covering_index_scan` | 3.90ms | 6.40ms | 1.6× | 2.3% | PASS |
| mem_reads | `groupby_scan` | 19.70ms | 20.09ms | 1.0× | 1.8% | PASS |
| mem_reads | `index_join` | 8.10ms | 7.96ms | 1.0× | 4.2% | PASS |
| mem_reads | `index_join_scan` | 2.66ms | 5.48ms | 2.1× | 5.4% | PASS |
| mem_reads | `types_table_scan` | 717.70ms | 867.57ms | 1.2× | 2.1% | PASS |
| mem_reads | `table_scan` | 756.08ms | 888.39ms | 1.2× | 2.7% | PASS |
| mem_reads | `oltp_read_only` | 71.22ms | 75.99ms | 1.1× | 1.9% | PASS |
| mem_writes | `oltp_bulk_insert` | 147.70ms | 222.01ms | 1.5× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 11.35ms | 23.01ms | 2.0× | 1.2% | PASS |
| mem_writes | `oltp_update_index` | 40.53ms | 84.37ms | 2.1× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 29.99ms | 48.77ms | 1.6× | 2.3% | PASS |
| mem_writes | `oltp_delete_insert` | 32.17ms | 61.08ms | 1.9× | 2.1% | PASS |
| mem_writes | `oltp_write_only` | 17.92ms | 33.96ms | 1.9× | 1.5% | PASS |
| mem_writes | `types_delete_insert` | 24.87ms | 37.81ms | 1.5× | 5.1% | PASS |
| mem_writes | `oltp_read_write` | 58.41ms | 81.02ms | 1.4× | 4.6% | PASS |
| file_reads | `oltp_point_select` | 71.47ms | 37.21ms | 0.5× | 2.8% | PASS |
| file_reads | `oltp_range_select` | 15.54ms | 11.31ms | 0.7× | 2.4% | PASS |
| file_reads | `oltp_sum_range` | 14.68ms | 10.96ms | 0.7× | 2.1% | PASS |
| file_reads | `oltp_order_range` | 2.64ms | 2.37ms | 0.9× | 1.6% | PASS |
| file_reads | `oltp_distinct_range` | 3.14ms | 2.86ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_index_scan` | 7.84ms | 5.84ms | 0.7× | 1.0% | PASS |
| file_reads | `select_random_points` | 19.57ms | 15.07ms | 0.8× | 2.5% | PASS |
| file_reads | `select_random_ranges` | 9.20ms | 5.38ms | 0.6× | 2.2% | PASS |
| file_reads | `covering_index_scan` | 9.51ms | 7.98ms | 0.8× | 1.9% | PASS |
| file_reads | `groupby_scan` | 20.96ms | 20.86ms | 1.0× | 1.3% | PASS |
| file_reads | `index_join` | 11.05ms | 8.16ms | 0.7× | 3.4% | PASS |
| file_reads | `index_join_scan` | 3.23ms | 5.00ms | 1.5× | 3.0% | PASS |
| file_reads | `types_table_scan` | 675.61ms | 814.23ms | 1.2× | 1.4% | PASS |
| file_reads | `table_scan` | 788.18ms | 917.16ms | 1.2× | 1.6% | PASS |
| file_reads | `oltp_read_only` | 150.03ms | 99.75ms | 0.7× | 2.2% | PASS |
| file_writes | `oltp_bulk_insert` | 208.39ms | 279.45ms | 1.3× | 5.6% | PASS |
| file_writes | `oltp_insert` | 19.33ms | 43.57ms | 2.3× | 9.2% | PASS |
| file_writes | `oltp_update_index` | 142.22ms | 166.99ms | 1.2× | 8.9% | PASS |
| file_writes | `oltp_update_non_index` | 124.88ms | 107.97ms | 0.9× | 4.5% | PASS |
| file_writes | `oltp_delete_insert` | 150.20ms | 130.98ms | 0.9× | 4.0% | PASS |
| file_writes | `oltp_write_only` | 111.09ms | 84.53ms | 0.8× | 9.9% | PASS |
| file_writes | `types_delete_insert` | 110.65ms | 74.66ms | 0.7× | 8.5% | PASS |
| file_writes | `oltp_read_write` | 149.95ms | 136.27ms | 0.9× | 9.3% | PASS |
| ac_reads | `oltp_point_select` | 42.71ms | 39.67ms | 0.9× | 2.9% | PASS |
| ac_reads | `oltp_range_select` | 12.88ms | 11.56ms | 0.9× | 3.0% | PASS |
| ac_reads | `oltp_sum_range` | 12.01ms | 11.22ms | 0.9× | 3.3% | PASS |
| ac_reads | `oltp_order_range` | 2.33ms | 2.47ms | 1.1× | 2.0% | PASS |
| ac_reads | `oltp_distinct_range` | 2.81ms | 2.93ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_index_scan` | 4.30ms | 5.93ms | 1.4× | 2.1% | PASS |
| ac_reads | `select_random_points` | 15.79ms | 15.22ms | 1.0× | 3.0% | PASS |
| ac_reads | `select_random_ranges` | 5.96ms | 5.60ms | 0.9× | 2.4% | PASS |
| ac_reads | `covering_index_scan` | 6.46ms | 8.17ms | 1.3× | 2.7% | PASS |
| ac_reads | `groupby_scan` | 21.61ms | 21.38ms | 1.0× | 1.2% | PASS |
| ac_reads | `index_join` | 10.24ms | 8.66ms | 0.8× | 3.6% | PASS |
| ac_reads | `index_join_scan` | 3.19ms | 5.22ms | 1.6× | 4.4% | PASS |
| ac_reads | `types_table_scan` | 716.38ms | 874.13ms | 1.2× | 1.6% | PASS |
| ac_reads | `table_scan` | 846.89ms | 955.54ms | 1.1× | 2.9% | PASS |
| ac_reads | `oltp_read_only` | 116.33ms | 108.01ms | 0.9× | 2.5% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 26.66ms | 56.39ms | 2.1× | 9.6% | PASS |
| ac_writes | `oltp_insert_ac` | 29.67ms | 65.77ms | 2.2× | 4.2% | PASS |
| ac_writes | `oltp_update_index_ac` | 30.03ms | 75.51ms | 2.5× | 4.8% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 25.31ms | 60.23ms | 2.4× | 5.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 29.44ms | 70.54ms | 2.4× | 8.9% | PASS |
| ac_writes | `oltp_write_only_ac` | 28.89ms | 71.39ms | 2.5× | 11.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.05ms | 63.40ms | 2.9× | 9.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.02ms | 73.89ms | 2.3× | 7.3% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 17.96ms | 22.91ms | 1.3× | 2.2% | PASS |
| mem_reads | `oltp_range_select` | 9.40ms | 11.70ms | 1.2× | 2.8% | PASS |
| mem_reads | `oltp_sum_range` | 8.71ms | 11.59ms | 1.3× | 3.0% | PASS |
| mem_reads | `oltp_order_range` | 1.88ms | 2.24ms | 1.2× | 1.9% | PASS |
| mem_reads | `oltp_distinct_range` | 2.37ms | 2.72ms | 1.1× | 1.8% | PASS |
| mem_reads | `oltp_index_scan` | 2.43ms | 3.60ms | 1.5× | 2.4% | PASS |
| mem_reads | `select_random_points` | 14.56ms | 18.44ms | 1.3× | 3.1% | PASS |
| mem_reads | `select_random_ranges` | 3.83ms | 4.94ms | 1.3× | 2.5% | PASS |
| mem_reads | `covering_index_scan` | 3.83ms | 5.29ms | 1.4× | 1.4% | PASS |
| mem_reads | `groupby_scan` | 19.18ms | 21.63ms | 1.1× | 1.2% | PASS |
| mem_reads | `index_join` | 4.45ms | 6.27ms | 1.4× | 1.5% | PASS |
| mem_reads | `index_join_scan` | 2.14ms | 4.82ms | 2.3× | 5.2% | PASS |
| mem_reads | `types_table_scan` | 657.36ms | 774.04ms | 1.2× | 1.0% | PASS |
| mem_reads | `table_scan` | 743.93ms | 858.30ms | 1.2× | 0.9% | PASS |
| mem_reads | `oltp_read_only` | 74.43ms | 87.05ms | 1.2× | 1.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 139.75ms | 199.94ms | 1.4× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 10.79ms | 19.25ms | 1.8× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 37.86ms | 72.43ms | 1.9× | 1.4% | PASS |
| mem_writes | `oltp_update_non_index` | 29.60ms | 51.65ms | 1.7× | 1.7% | PASS |
| mem_writes | `oltp_delete_insert` | 28.17ms | 53.95ms | 1.9× | 1.3% | PASS |
| mem_writes | `oltp_write_only` | 15.53ms | 33.85ms | 2.2× | 1.1% | PASS |
| mem_writes | `types_delete_insert` | 18.79ms | 32.01ms | 1.7× | 2.2% | PASS |
| mem_writes | `oltp_read_write` | 50.31ms | 81.74ms | 1.6× | 1.3% | PASS |
| file_reads | `oltp_point_select` | 67.17ms | 36.36ms | 0.5× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 15.63ms | 13.62ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_sum_range` | 14.83ms | 13.31ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_order_range` | 2.79ms | 2.51ms | 0.9× | 1.5% | PASS |
| file_reads | `oltp_distinct_range` | 3.15ms | 2.92ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 7.90ms | 5.07ms | 0.6× | 1.1% | PASS |
| file_reads | `select_random_points` | 21.00ms | 19.80ms | 0.9× | 1.5% | PASS |
| file_reads | `select_random_ranges` | 9.23ms | 6.38ms | 0.7× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 9.18ms | 6.83ms | 0.7× | 0.6% | PASS |
| file_reads | `groupby_scan` | 20.27ms | 21.66ms | 1.1× | 0.8% | PASS |
| file_reads | `index_join` | 7.46ms | 7.20ms | 1.0× | 1.3% | PASS |
| file_reads | `index_join_scan` | 3.12ms | 4.54ms | 1.5× | 2.1% | PASS |
| file_reads | `types_table_scan` | 659.10ms | 773.57ms | 1.2× | 0.9% | PASS |
| file_reads | `table_scan` | 741.48ms | 855.61ms | 1.2× | 1.0% | PASS |
| file_reads | `oltp_read_only` | 143.70ms | 106.30ms | 0.7× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 189.61ms | 241.13ms | 1.3× | 0.9% | PASS |
| file_writes | `oltp_insert` | 19.49ms | 30.11ms | 1.5× | 3.6% | PASS |
| file_writes | `oltp_update_index` | 140.59ms | 131.56ms | 0.9× | 1.1% | PASS |
| file_writes | `oltp_update_non_index` | 120.31ms | 91.39ms | 0.8× | 4.1% | PASS |
| file_writes | `oltp_delete_insert` | 120.19ms | 109.75ms | 0.9× | 4.9% | PASS |
| file_writes | `oltp_write_only` | 80.49ms | 71.10ms | 0.9× | 0.5% | PASS |
| file_writes | `types_delete_insert` | 70.17ms | 61.69ms | 0.9× | 6.3% | PASS |
| file_writes | `oltp_read_write` | 120.21ms | 121.03ms | 1.0× | 1.8% | PASS |
| ac_reads | `oltp_point_select` | 34.91ms | 36.56ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_range_select` | 11.90ms | 13.11ms | 1.1× | 1.8% | PASS |
| ac_reads | `oltp_sum_range` | 11.15ms | 13.10ms | 1.2× | 1.6% | PASS |
| ac_reads | `oltp_order_range` | 2.23ms | 2.43ms | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_distinct_range` | 2.75ms | 2.90ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 4.40ms | 5.09ms | 1.2× | 2.1% | PASS |
| ac_reads | `select_random_points` | 17.45ms | 20.15ms | 1.2× | 1.7% | PASS |
| ac_reads | `select_random_ranges` | 5.93ms | 6.44ms | 1.1× | 1.6% | PASS |
| ac_reads | `covering_index_scan` | 5.58ms | 6.85ms | 1.2× | 1.6% | PASS |
| ac_reads | `groupby_scan` | 19.81ms | 21.82ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 5.99ms | 7.99ms | 1.3× | 3.3% | PASS |
| ac_reads | `index_join_scan` | 2.91ms | 4.97ms | 1.7× | 4.5% | PASS |
| ac_reads | `types_table_scan` | 663.08ms | 778.61ms | 1.2× | 0.9% | PASS |
| ac_reads | `table_scan` | 764.75ms | 866.73ms | 1.1× | 1.9% | PASS |
| ac_reads | `oltp_read_only` | 101.36ms | 107.61ms | 1.1× | 2.6% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.06ms | 49.45ms | 2.3× | 4.4% | PASS |
| ac_writes | `oltp_insert_ac` | 24.40ms | 57.52ms | 2.4× | 5.1% | PASS |
| ac_writes | `oltp_update_index_ac` | 25.84ms | 64.56ms | 2.5× | 3.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 20.49ms | 53.89ms | 2.6× | 3.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 23.61ms | 59.08ms | 2.5× | 4.8% | PASS |
| ac_writes | `oltp_write_only_ac` | 23.73ms | 59.55ms | 2.5× | 4.4% | PASS |
| ac_writes | `types_delete_insert_ac` | 24.55ms | 54.16ms | 2.2× | 5.3% | PASS |
| ac_writes | `oltp_read_write_ac` | 26.25ms | 64.97ms | 2.5× | 5.0% | PASS |

</details>

</details>

## Version-control latency

Wall time: 4m 29s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 35.08ms | 38.00ms | 92.3% | 0.8% | PASS |
| `status_dirty_many_tables` | 38.73ms | 42.00ms | 92.2% | 0.6% | PASS |
| `diff_regular_working_one_table` | 30.42ms | 33.00ms | 92.2% | 0.5% | PASS |
| `diff_regular_working_many_tables` | 43.11ms | 50.00ms | 86.2% | 0.5% | PASS |
| `diff_stat_working_many_tables` | 43.05ms | 48.00ms | 89.7% | 0.5% | PASS |
| `diff_schema_working_many_tables` | 43.62ms | 48.00ms | 90.9% | 0.5% | PASS |
| `branch_list_many_branches` | 22.68ms | 25.00ms | 90.7% | 0.6% | PASS |
| `branch_create_delete` | 24.36ms | 27.00ms | 90.2% | 1.2% | PASS |
| `at_literal_deep_history` | 24.68ms | 28.00ms | 88.2% | 0.9% | PASS |
| `diff_literal_deep_history` | 24.72ms | 28.00ms | 88.3% | 0.9% | PASS |
| `history_literal_deep_history` | 25.62ms | 30.00ms | 85.4% | 0.8% | PASS |
| `checkout_branch_clean` | 33.74ms | 43.00ms | 78.5% | 1.0% | PASS |
| `merge_data_no_conflicts` | 27.72ms | 33.00ms | 84.0% | 0.9% | PASS |
| `merge_data_secondary_index` | 880.72ms | 944.00ms | 93.3% | 0.5% | PASS |
| `merge_schema_no_conflicts` | 21.43ms | 24.00ms | 89.3% | 1.2% | PASS |
| `merge_data_conflicts` | 30.65ms | 33.00ms | 92.9% | 0.8% | PASS |
| `merge_data_conflicts_with_resolve` | 31.57ms | 34.00ms | 92.8% | 0.8% | PASS |

Version-control ceiling result: **PASS**.

## File remote performance

Status: passed. Transport: local file-backed remote.

100,000,000 logical payload bytes; 100,000 rows; 1,000 changed rows; 5 repetitions per operation and binary.

SQL timings exclude fixture generation, copies, database open and verification. Each SQL sample uses a fresh process with the OS cache primed by setup verification. SQL client cache: 65,536 KiB. copy_fsync times shutil.copyfile plus fsync.

Throughput uses logical payload bytes; destination growth is file-size change, not transferred bytes.

Nightly median ceilings require three-attempt confirmation; copy_fsync is a control.

| Operation | Ceiling ms | Result |
|---|---:|---|
| push_initial | 2000 | PASS |
| clone_initial | 3000 | PASS |
| push_noop | 10 | PASS |
| fetch_noop | 10 | PASS |
| push_incremental | 750 | PASS |
| fetch_incremental | 300 | PASS |
| pull_fast_forward | 300 | PASS |

Timing gate: PASS after 1 attempt(s).

candidate: 26ca913.

Fixture file bytes: base 107,968,301; delta 115,879,032.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 60.699 | 56.214–62.896 | 38.429 | 1647.47 | 107968301 |
| push_initial | candidate | 588.818 | 580.397–592.043 | 568.674 | 169.83 | 107956807 |
| clone_initial | candidate | 768.526 | 765.098–771.638 | 743.346 | 130.12 | 108475105 |
| push_noop | candidate | 0.685 | 0.624–0.961 | 0.310 | — | 0 |
| fetch_noop | candidate | 0.203 | 0.188–0.234 | 0.205 | — | 0 |
| push_incremental | candidate | 236.848 | 235.731–237.281 | 233.637 | 4.22 | 7909775 |
| fetch_incremental | candidate | 96.687 | 96.329–97.183 | 94.061 | 10.34 | 7910084 |
| pull_fast_forward | candidate | 85.079 | 84.725–89.443 | 82.206 | 11.75 | 7910844 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 90.43 | 175.487 | 108.763 | 18329 | 2066 |
| clone_initial | candidate | 90.80 | 295.295 | 109.784 | 47292 | 2337 |
| push_noop | candidate | 42.84 | 0.073 | 0.000 | 26 | 2 |
| fetch_noop | candidate | 42.84 | 0.135 | 0.000 | 40 | 0 |
| push_incremental | candidate | 42.84 | 96.901 | 7.910 | 22419 | 125 |
| fetch_incremental | candidate | 42.84 | 34.472 | 7.910 | 10808 | 124 |
| pull_fast_forward | candidate | 42.84 | 26.497 | 7.911 | 7031 | 126 |

</details>


<details>
<summary>1 GB file remotes (observational)</summary>

Both database sizes use a fixed 1 MB scattered update and a warm OS cache. 1 GB timings have no ceiling while a baseline is collected.

### 1 GB file remote performance

Status: passed. Transport: local file-backed remote.

1,000,000,000 logical payload bytes; 1,000,000 rows; 1,000 changed rows; 3 repetitions per operation and binary.

SQL timings exclude fixture generation, copies, database open and verification. Each SQL sample uses a fresh process with the OS cache primed by setup verification. SQL client cache: 65,536 KiB. copy_fsync times shutil.copyfile plus fsync.

Throughput uses logical payload bytes; destination growth is file-size change, not transferred bytes.

No speed thresholds are enforced.

candidate: 26ca913.

Fixture file bytes: base 1,087,718,066; delta 1,103,525,194.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 5585.447 | 5506.051–5653.714 | 306.110 | 179.04 | 1087718066 |
| push_initial | candidate | 9320.556 | 9243.517–9323.080 | 4168.392 | 107.29 | 1079566310 |
| clone_initial | candidate | 11420.712 | 11300.286–12219.614 | 6087.925 | 87.56 | 1102407396 |
| push_noop | candidate | 31.322 | 0.585–51.967 | 0.288 | — | 0 |
| fetch_noop | candidate | 0.164 | 0.164–0.169 | 0.166 | — | 0 |
| push_incremental | candidate | 1024.514 | 999.604–1049.698 | 943.380 | 0.98 | 15806003 |
| fetch_incremental | candidate | 294.306 | 287.510–298.150 | 215.738 | 3.40 | 15806513 |
| pull_fast_forward | candidate | 293.191 | 290.866–296.457 | 213.009 | 3.41 | 15807273 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 134.42 | 2091.548 | 1087.622 | 263417 | 20541 |
| clone_initial | candidate | 124.11 | 3514.142 | 1132.819 | 611382 | 32243 |
| push_noop | candidate | 45.51 | 0.078 | 0.000 | 27 | 2 |
| fetch_noop | candidate | 45.51 | 0.137 | 0.000 | 40 | 0 |
| push_incremental | candidate | 45.51 | 615.257 | 15.928 | 149421 | 313 |
| fetch_incremental | candidate | 50.57 | 62.249 | 15.928 | 13899 | 313 |
| pull_fast_forward | candidate | 50.48 | 62.181 | 15.929 | 13895 | 315 |

</details>


</details>

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh`, `test/vc_perf_ceiling.sh`, and `test/remote_perf.py`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
