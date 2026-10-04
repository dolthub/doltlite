# DoltLite Performance Report

> Nightly result: **FAIL**
>
> Generated: 2026-10-04 12:39 UTC
>
> Commit: [`23e97aedc30a6e21691ad9f471e1a19c6325d0de`](https://github.com/dolthub/doltlite/commit/23e97aedc30a6e21691ad9f471e1a19c6325d0de)
>
> Runner: ubuntu24 20260927.320.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/37197622245)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 8.26s | 9.32s | 1.1× | 1.6% | **PASS** |
| Writes | 1.71s | 2.59s | 1.5× | 1.4% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.28s | 9.64s | 1.0× | 1.4% | **PASS** |
| Writes | 4.07s | 3.96s | 1.0× | 4.1% | **PASS** |
| Autocommit writes | 945.11ms | 2.75s | 2.9× | 6.9% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 1.84s | 1.87s | 1.0× | 1.8% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.15s | 2.44s | 1.1× | 1.1% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 1.69s | 1.91s | 1.1× | 2.9% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.59s | 3.10s | 1.2× | 1.3% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 301.13ms | 468.84ms | 1.6× | 2.0% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 458.50ms | 675.72ms | 1.5× | 1.0% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 360.45ms | 540.31ms | 1.5× | 3.1% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 587.29ms | 909.38ms | 1.5× | 1.3% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 1.98s | 1.87s | 0.9× | 1.8% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.39s | 2.50s | 1.0× | 0.9% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.05s | 2.09s | 1.0× | 2.0% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.86s | 3.17s | 1.1× | 1.3% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 753.72ms | 715.29ms | 0.9× | 2.8% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.55s | 1.28s | 0.8× | 28.8% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 1.00s | 962.39ms | 1.0× | 5.7% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 762.28ms | 1.00s | 1.3× | 1.8% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 1.69s | 1.79s | 1.1× | 1.9% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.47s | 2.58s | 1.0× | 0.9% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.01s | 2.19s | 1.1× | 2.3% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.69s | 3.16s | 1.2× | 1.2% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 181.37ms | 499.62ms | 2.8× | 4.4% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 332.31ms | 1.12s | 3.4× | 61.8% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 225.92ms | 508.01ms | 2.2× | 6.1% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 205.51ms | 627.06ms | 3.1× | 7.4% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 17.82ms | 20.41ms | 1.1× | 3.4% | PASS |
| mem_reads | `oltp_range_select` | 7.12ms | 7.86ms | 1.1× | 1.8% | PASS |
| mem_reads | `oltp_sum_range` | 6.66ms | 7.88ms | 1.2× | 2.4% | PASS |
| mem_reads | `oltp_order_range` | 1.81ms | 1.94ms | 1.1× | 1.8% | PASS |
| mem_reads | `oltp_distinct_range` | 2.32ms | 2.49ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 2.65ms | 3.92ms | 1.5× | 1.4% | PASS |
| mem_reads | `select_random_points` | 8.71ms | 9.78ms | 1.1× | 3.8% | PASS |
| mem_reads | `select_random_ranges` | 2.98ms | 3.20ms | 1.1× | 1.6% | PASS |
| mem_reads | `covering_index_scan` | 4.18ms | 6.39ms | 1.5× | 1.6% | PASS |
| mem_reads | `groupby_scan` | 17.65ms | 19.33ms | 1.1× | 1.0% | PASS |
| mem_reads | `index_join` | 3.66ms | 6.19ms | 1.7× | 3.5% | PASS |
| mem_reads | `index_join_scan` | 2.63ms | 4.58ms | 1.7× | 4.1% | PASS |
| mem_reads | `types_table_scan` | 767.45ms | 801.29ms | 1.0× | 1.4% | PASS |
| mem_reads | `table_scan` | 926.42ms | 900.75ms | 1.0× | 2.4% | PASS |
| mem_reads | `oltp_read_only` | 70.66ms | 74.62ms | 1.1× | 1.4% | PASS |
| mem_writes | `oltp_bulk_insert` | 111.05ms | 165.23ms | 1.5× | 1.1% | PASS |
| mem_writes | `oltp_insert` | 9.80ms | 17.41ms | 1.8× | 1.1% | PASS |
| mem_writes | `oltp_update_index` | 37.07ms | 66.41ms | 1.8× | 3.0% | PASS |
| mem_writes | `oltp_update_non_index` | 25.79ms | 39.14ms | 1.5× | 1.9% | PASS |
| mem_writes | `oltp_delete_insert` | 32.80ms | 52.15ms | 1.6× | 2.1% | PASS |
| mem_writes | `oltp_write_only` | 16.23ms | 31.73ms | 2.0× | 2.8% | PASS |
| mem_writes | `types_delete_insert` | 17.98ms | 24.69ms | 1.4× | 1.5% | PASS |
| mem_writes | `oltp_read_write` | 50.42ms | 72.07ms | 1.4× | 2.6% | PASS |
| file_reads | `oltp_point_select` | 75.09ms | 36.40ms | 0.5× | 1.1% | PASS |
| file_reads | `oltp_range_select` | 13.86ms | 9.91ms | 0.7× | 2.1% | PASS |
| file_reads | `oltp_sum_range` | 12.96ms | 9.50ms | 0.7× | 1.8% | PASS |
| file_reads | `oltp_order_range` | 2.52ms | 2.16ms | 0.9× | 1.8% | PASS |
| file_reads | `oltp_distinct_range` | 3.06ms | 2.77ms | 0.9× | 1.6% | PASS |
| file_reads | `oltp_index_scan` | 8.73ms | 5.51ms | 0.6× | 1.9% | PASS |
| file_reads | `select_random_points` | 15.34ms | 11.75ms | 0.8× | 3.7% | PASS |
| file_reads | `select_random_ranges` | 8.75ms | 4.79ms | 0.5× | 0.7% | PASS |
| file_reads | `covering_index_scan` | 10.65ms | 8.04ms | 0.8× | 1.4% | PASS |
| file_reads | `groupby_scan` | 18.95ms | 20.06ms | 1.1× | 1.2% | PASS |
| file_reads | `index_join` | 7.36ms | 7.29ms | 1.0× | 4.3% | PASS |
| file_reads | `index_join_scan` | 3.21ms | 5.00ms | 1.6× | 6.0% | PASS |
| file_reads | `types_table_scan` | 751.38ms | 783.30ms | 1.0× | 1.1% | PASS |
| file_reads | `table_scan` | 903.27ms | 878.79ms | 1.0× | 1.6% | PASS |
| file_reads | `oltp_read_only` | 141.49ms | 89.71ms | 0.6× | 2.8% | PASS |
| file_writes | `oltp_bulk_insert` | 148.22ms | 202.22ms | 1.4× | 3.6% | PASS |
| file_writes | `oltp_insert` | 19.79ms | 28.02ms | 1.4× | 1.2% | PASS |
| file_writes | `oltp_update_index` | 131.03ms | 117.90ms | 0.9× | 4.2% | PASS |
| file_writes | `oltp_update_non_index` | 101.26ms | 79.25ms | 0.8× | 1.8% | PASS |
| file_writes | `oltp_delete_insert` | 101.36ms | 89.73ms | 0.9× | 2.0% | PASS |
| file_writes | `oltp_write_only` | 80.88ms | 60.07ms | 0.7× | 1.6% | PASS |
| file_writes | `types_delete_insert` | 60.81ms | 46.27ms | 0.8× | 11.2% | PASS |
| file_writes | `oltp_read_write` | 110.39ms | 91.83ms | 0.8× | 6.1% | PASS |
| ac_reads | `oltp_point_select` | 33.15ms | 33.45ms | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_range_select` | 8.80ms | 8.81ms | 1.0× | 2.8% | PASS |
| ac_reads | `oltp_sum_range` | 7.92ms | 8.64ms | 1.1× | 1.9% | PASS |
| ac_reads | `oltp_order_range` | 1.92ms | 2.00ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_distinct_range` | 2.46ms | 2.55ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_index_scan` | 4.54ms | 5.23ms | 1.2× | 2.6% | PASS |
| ac_reads | `select_random_points` | 9.71ms | 10.74ms | 1.1× | 4.0% | PASS |
| ac_reads | `select_random_ranges` | 4.71ms | 4.64ms | 1.0× | 1.4% | PASS |
| ac_reads | `covering_index_scan` | 6.12ms | 7.56ms | 1.2× | 1.6% | PASS |
| ac_reads | `groupby_scan` | 17.49ms | 19.10ms | 1.1× | 1.4% | PASS |
| ac_reads | `index_join` | 4.76ms | 6.70ms | 1.4× | 2.5% | PASS |
| ac_reads | `index_join_scan` | 2.53ms | 4.47ms | 1.8× | 3.8% | PASS |
| ac_reads | `types_table_scan` | 686.80ms | 741.92ms | 1.1× | 1.8% | PASS |
| ac_reads | `table_scan` | 814.19ms | 848.58ms | 1.0× | 2.4% | PASS |
| ac_reads | `oltp_read_only` | 87.67ms | 88.99ms | 1.0× | 1.6% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.10ms | 53.30ms | 2.5× | 4.5% | PASS |
| ac_writes | `oltp_insert_ac` | 22.70ms | 60.67ms | 2.7× | 3.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 24.19ms | 70.17ms | 2.9× | 4.9% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.02ms | 57.58ms | 2.7× | 4.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 22.91ms | 65.33ms | 2.9× | 4.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 22.65ms | 63.90ms | 2.8× | 4.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 21.15ms | 59.79ms | 2.8× | 6.8% | PASS |
| ac_writes | `oltp_read_write_ac` | 25.64ms | 68.88ms | 2.7× | 3.5% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 27.54ms | 29.11ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_range_select` | 13.75ms | 12.20ms | 0.9× | 1.2% | PASS |
| mem_reads | `oltp_sum_range` | 12.40ms | 12.05ms | 1.0× | 1.6% | PASS |
| mem_reads | `oltp_order_range` | 2.82ms | 2.79ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 3.65ms | 3.71ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 3.17ms | 5.28ms | 1.7× | 1.3% | PASS |
| mem_reads | `select_random_points` | 17.54ms | 17.76ms | 1.0× | 1.2% | PASS |
| mem_reads | `select_random_ranges` | 5.33ms | 5.02ms | 0.9× | 0.8% | PASS |
| mem_reads | `covering_index_scan` | 5.89ms | 8.03ms | 1.4× | 0.8% | PASS |
| mem_reads | `groupby_scan` | 29.15ms | 30.06ms | 1.0× | 0.4% | PASS |
| mem_reads | `index_join` | 8.81ms | 8.31ms | 0.9× | 0.9% | PASS |
| mem_reads | `index_join_scan` | 3.11ms | 5.55ms | 1.8× | 1.2% | PASS |
| mem_reads | `types_table_scan` | 893.47ms | 1.05s | 1.2× | 0.6% | PASS |
| mem_reads | `table_scan` | 1.01s | 1.14s | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_read_only` | 108.97ms | 110.22ms | 1.0× | 1.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 182.70ms | 239.32ms | 1.3× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 13.93ms | 27.29ms | 2.0× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 51.88ms | 98.09ms | 1.9× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 38.56ms | 58.45ms | 1.5× | 1.0% | PASS |
| mem_writes | `oltp_delete_insert` | 43.54ms | 72.40ms | 1.7× | 0.9% | PASS |
| mem_writes | `oltp_write_only` | 22.65ms | 41.60ms | 1.8× | 1.0% | PASS |
| mem_writes | `types_delete_insert` | 30.89ms | 39.34ms | 1.3× | 1.7% | PASS |
| mem_writes | `oltp_read_write` | 74.35ms | 99.24ms | 1.3× | 1.3% | PASS |
| file_reads | `oltp_point_select` | 93.70ms | 46.35ms | 0.5× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 21.30ms | 13.90ms | 0.7× | 0.8% | PASS |
| file_reads | `oltp_sum_range` | 19.66ms | 13.81ms | 0.7× | 0.9% | PASS |
| file_reads | `oltp_order_range` | 3.69ms | 3.00ms | 0.8× | 1.0% | PASS |
| file_reads | `oltp_distinct_range` | 4.46ms | 3.92ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 10.08ms | 7.09ms | 0.7× | 0.8% | PASS |
| file_reads | `select_random_points` | 25.31ms | 19.49ms | 0.8× | 1.2% | PASS |
| file_reads | `select_random_ranges` | 12.46ms | 6.84ms | 0.5× | 0.8% | PASS |
| file_reads | `covering_index_scan` | 12.88ms | 9.82ms | 0.8× | 0.7% | PASS |
| file_reads | `groupby_scan` | 29.96ms | 30.27ms | 1.0× | 0.7% | PASS |
| file_reads | `index_join` | 13.07ms | 9.25ms | 0.7× | 0.7% | PASS |
| file_reads | `index_join_scan` | 3.98ms | 5.76ms | 1.4× | 1.0% | PASS |
| file_reads | `types_table_scan` | 908.73ms | 1.05s | 1.2× | 0.9% | PASS |
| file_reads | `table_scan` | 1.03s | 1.15s | 1.1× | 1.1% | PASS |
| file_reads | `oltp_read_only` | 204.26ms | 134.62ms | 0.7× | 0.6% | PASS |
| file_writes | `oltp_bulk_insert` | 296.17ms | 333.52ms | 1.1× | 14.0% | PASS |
| file_writes | `oltp_insert` | 42.05ms | 60.68ms | 1.4× | 62.7% | PASS |
| file_writes | `oltp_update_index` | 245.85ms | 206.46ms | 0.8× | 29.4% | PASS |
| file_writes | `oltp_update_non_index` | 164.46ms | 132.21ms | 0.8× | 13.9% | PASS |
| file_writes | `oltp_delete_insert` | 252.15ms | 160.09ms | 0.6× | 29.8% | PASS |
| file_writes | `oltp_write_only` | 161.37ms | 120.63ms | 0.7× | 45.8% | PASS |
| file_writes | `types_delete_insert` | 191.52ms | 109.17ms | 0.6× | 28.3% | PASS |
| file_writes | `oltp_read_write` | 195.30ms | 158.89ms | 0.8× | 16.4% | PASS |
| ac_reads | `oltp_point_select` | 50.57ms | 46.56ms | 0.9× | 1.8% | PASS |
| ac_reads | `oltp_range_select` | 17.14ms | 13.96ms | 0.8× | 1.3% | PASS |
| ac_reads | `oltp_sum_range` | 15.29ms | 13.78ms | 0.9× | 0.9% | PASS |
| ac_reads | `oltp_order_range` | 3.26ms | 3.00ms | 0.9× | 0.7% | PASS |
| ac_reads | `oltp_distinct_range` | 4.02ms | 3.93ms | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_index_scan` | 5.78ms | 7.09ms | 1.2× | 0.9% | PASS |
| ac_reads | `select_random_points` | 20.93ms | 19.43ms | 0.9× | 1.1% | PASS |
| ac_reads | `select_random_ranges` | 8.06ms | 6.83ms | 0.8× | 0.7% | PASS |
| ac_reads | `covering_index_scan` | 8.54ms | 9.82ms | 1.2× | 0.7% | PASS |
| ac_reads | `groupby_scan` | 29.51ms | 30.27ms | 1.0× | 0.8% | PASS |
| ac_reads | `index_join` | 11.03ms | 9.32ms | 0.8× | 1.3% | PASS |
| ac_reads | `index_join_scan` | 3.60ms | 5.77ms | 1.6× | 1.3% | PASS |
| ac_reads | `types_table_scan` | 959.86ms | 1.07s | 1.1× | 2.8% | PASS |
| ac_reads | `table_scan` | 1.19s | 1.20s | 1.0× | 3.5% | PASS |
| ac_reads | `oltp_read_only` | 140.22ms | 134.60ms | 1.0× | 0.6% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 34.63ms | 128.58ms | 3.7× | 48.1% | PASS |
| ac_writes | `oltp_insert_ac` | 36.06ms | 173.30ms | 4.8× | 70.2% | PASS |
| ac_writes | `oltp_update_index_ac` | 39.96ms | 148.51ms | 3.7× | 62.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 32.80ms | 99.68ms | 3.0× | 71.1% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 36.18ms | 133.56ms | 3.7× | 61.0% | PASS |
| ac_writes | `oltp_write_only_ac` | 46.83ms | 130.22ms | 2.8× | 56.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 28.01ms | 90.87ms | 3.2× | 59.8% | PASS |
| ac_writes | `oltp_read_write_ac` | 77.83ms | 214.63ms | 2.8× | 77.6% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 21.70ms | 22.57ms | 1.0× | 2.9% | PASS |
| mem_reads | `oltp_range_select` | 9.41ms | 9.15ms | 1.0× | 4.9% | PASS |
| mem_reads | `oltp_sum_range` | 9.45ms | 9.14ms | 1.0× | 6.6% | PASS |
| mem_reads | `oltp_order_range` | 2.12ms | 2.10ms | 1.0× | 2.8% | PASS |
| mem_reads | `oltp_distinct_range` | 2.65ms | 2.61ms | 1.0× | 2.3% | PASS |
| mem_reads | `oltp_index_scan` | 2.50ms | 4.54ms | 1.8× | 2.5% | PASS |
| mem_reads | `select_random_points` | 14.84ms | 14.99ms | 1.0× | 3.9% | PASS |
| mem_reads | `select_random_ranges` | 4.36ms | 4.14ms | 0.9× | 4.3% | PASS |
| mem_reads | `covering_index_scan` | 4.10ms | 6.45ms | 1.6× | 2.7% | PASS |
| mem_reads | `groupby_scan` | 20.59ms | 21.42ms | 1.0× | 2.0% | PASS |
| mem_reads | `index_join` | 7.43ms | 7.71ms | 1.0× | 4.2% | PASS |
| mem_reads | `index_join_scan` | 2.60ms | 5.18ms | 2.0× | 6.5% | PASS |
| mem_reads | `types_table_scan` | 676.77ms | 795.54ms | 1.2× | 2.2% | PASS |
| mem_reads | `table_scan` | 828.32ms | 923.97ms | 1.1× | 2.2% | PASS |
| mem_reads | `oltp_read_only` | 78.50ms | 77.17ms | 1.0× | 3.5% | PASS |
| mem_writes | `oltp_bulk_insert` | 135.23ms | 181.28ms | 1.3× | 1.3% | PASS |
| mem_writes | `oltp_insert` | 10.45ms | 21.45ms | 2.1× | 1.4% | PASS |
| mem_writes | `oltp_update_index` | 41.83ms | 80.83ms | 1.9× | 4.3% | PASS |
| mem_writes | `oltp_update_non_index` | 30.36ms | 46.92ms | 1.5× | 2.4% | PASS |
| mem_writes | `oltp_delete_insert` | 32.73ms | 59.45ms | 1.8× | 3.3% | PASS |
| mem_writes | `oltp_write_only` | 18.84ms | 35.60ms | 1.9× | 2.9% | PASS |
| mem_writes | `types_delete_insert` | 26.69ms | 34.47ms | 1.3× | 3.2% | PASS |
| mem_writes | `oltp_read_write` | 64.32ms | 80.31ms | 1.2× | 5.0% | PASS |
| file_reads | `oltp_point_select` | 78.00ms | 37.81ms | 0.5× | 2.7% | PASS |
| file_reads | `oltp_range_select` | 16.55ms | 11.04ms | 0.7× | 2.9% | PASS |
| file_reads | `oltp_sum_range` | 16.08ms | 10.91ms | 0.7× | 2.8% | PASS |
| file_reads | `oltp_order_range` | 2.91ms | 2.35ms | 0.8× | 3.6% | PASS |
| file_reads | `oltp_distinct_range` | 3.42ms | 2.88ms | 0.8× | 2.0% | PASS |
| file_reads | `oltp_index_scan` | 8.68ms | 6.26ms | 0.7× | 1.3% | PASS |
| file_reads | `select_random_points` | 23.17ms | 16.79ms | 0.7× | 4.7% | PASS |
| file_reads | `select_random_ranges` | 10.43ms | 5.77ms | 0.6× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 10.31ms | 8.01ms | 0.8× | 1.4% | PASS |
| file_reads | `groupby_scan` | 21.76ms | 21.71ms | 1.0× | 1.8% | PASS |
| file_reads | `index_join` | 11.65ms | 8.31ms | 0.7× | 2.6% | PASS |
| file_reads | `index_join_scan` | 3.43ms | 5.07ms | 1.5× | 1.9% | PASS |
| file_reads | `types_table_scan` | 722.02ms | 842.29ms | 1.2× | 1.7% | PASS |
| file_reads | `table_scan` | 942.06ms | 1.00s | 1.1× | 2.2% | PASS |
| file_reads | `oltp_read_only` | 175.31ms | 107.17ms | 0.6× | 1.9% | PASS |
| file_writes | `oltp_bulk_insert` | 208.83ms | 250.87ms | 1.2× | 2.9% | PASS |
| file_writes | `oltp_insert` | 19.66ms | 46.81ms | 2.4× (paired 2.3×) | 10.6% | PASS |
| file_writes | `oltp_update_index` | 146.39ms | 164.83ms | 1.1× | 5.9% | PASS |
| file_writes | `oltp_update_non_index` | 125.02ms | 103.82ms | 0.8× | 4.0% | PASS |
| file_writes | `oltp_delete_insert` | 149.97ms | 127.34ms | 0.8× | 5.5% | PASS |
| file_writes | `oltp_write_only` | 109.90ms | 79.59ms | 0.7× | 8.6% | PASS |
| file_writes | `types_delete_insert` | 101.66ms | 69.76ms | 0.7× | 14.0% | PASS |
| file_writes | `oltp_read_write` | 140.14ms | 119.37ms | 0.9× | 1.7% | PASS |
| ac_reads | `oltp_point_select` | 41.04ms | 38.06ms | 0.9× | 2.3% | PASS |
| ac_reads | `oltp_range_select` | 12.85ms | 11.00ms | 0.9× | 2.4% | PASS |
| ac_reads | `oltp_sum_range` | 12.52ms | 11.04ms | 0.9× | 3.0% | PASS |
| ac_reads | `oltp_order_range` | 2.57ms | 2.36ms | 0.9× | 1.6% | PASS |
| ac_reads | `oltp_distinct_range` | 3.03ms | 2.84ms | 0.9× | 0.9% | PASS |
| ac_reads | `oltp_index_scan` | 4.84ms | 6.01ms | 1.2× | 1.3% | PASS |
| ac_reads | `select_random_points` | 17.07ms | 15.31ms | 0.9× | 3.8% | PASS |
| ac_reads | `select_random_ranges` | 6.43ms | 5.50ms | 0.9× | 2.0% | PASS |
| ac_reads | `covering_index_scan` | 6.39ms | 7.63ms | 1.2× | 2.1% | PASS |
| ac_reads | `groupby_scan` | 21.16ms | 21.68ms | 1.0× | 1.5% | PASS |
| ac_reads | `index_join` | 9.80ms | 8.28ms | 0.8× | 2.8% | PASS |
| ac_reads | `index_join_scan` | 3.17ms | 5.20ms | 1.6× | 2.9% | PASS |
| ac_reads | `types_table_scan` | 793.19ms | 922.97ms | 1.2× | 3.3% | PASS |
| ac_reads | `table_scan` | 953.78ms | 1.02s | 1.1× | 2.0% | PASS |
| ac_reads | `oltp_read_only` | 126.52ms | 112.27ms | 0.9× | 3.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 27.49ms | 53.34ms | 1.9× | 4.1% | PASS |
| ac_writes | `oltp_insert_ac` | 29.81ms | 64.15ms | 2.2× | 5.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 30.46ms | 74.17ms | 2.4× | 4.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 25.57ms | 58.35ms | 2.3× | 4.1% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 29.70ms | 65.23ms | 2.2× | 6.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 28.73ms | 64.08ms | 2.2× | 6.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.16ms | 60.72ms | 2.7× | 8.2% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.99ms | 67.97ms | 2.1× | 7.0% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.08ms | 41.36ms | 1.3× | 2.1% | PASS |
| mem_reads | `oltp_range_select` | 18.97ms | 23.63ms | 1.2× | 1.7% | PASS |
| mem_reads | `oltp_sum_range` | 17.49ms | 23.20ms | 1.3× | 1.1% | PASS |
| mem_reads | `oltp_order_range` | 3.52ms | 4.26ms | 1.2× | 1.3% | PASS |
| mem_reads | `oltp_distinct_range` | 4.64ms | 5.44ms | 1.2× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 4.53ms | 6.34ms | 1.4× | 1.3% | PASS |
| mem_reads | `select_random_points` | 27.67ms | 34.02ms | 1.2× | 1.7% | PASS |
| mem_reads | `select_random_ranges` | 7.57ms | 9.48ms | 1.3× | 1.1% | PASS |
| mem_reads | `covering_index_scan` | 7.63ms | 10.61ms | 1.4× | 1.1% | PASS |
| mem_reads | `groupby_scan` | 35.51ms | 41.53ms | 1.2× | 0.9% | PASS |
| mem_reads | `index_join` | 7.82ms | 11.62ms | 1.5× | 1.8% | PASS |
| mem_reads | `index_join_scan` | 3.88ms | 6.55ms | 1.7× | 1.5% | PASS |
| mem_reads | `types_table_scan` | 1.05s | 1.29s | 1.2× | 0.6% | PASS |
| mem_reads | `table_scan` | 1.22s | 1.41s | 1.2× | 1.6% | PASS |
| mem_reads | `oltp_read_only` | 149.80ms | 185.43ms | 1.2× | 1.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 243.26ms | 323.26ms | 1.3× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 18.63ms | 33.55ms | 1.8× | 1.1% | PASS |
| mem_writes | `oltp_update_index` | 66.13ms | 119.15ms | 1.8× | 1.3% | PASS |
| mem_writes | `oltp_update_non_index` | 50.83ms | 80.89ms | 1.6× | 1.7% | PASS |
| mem_writes | `oltp_delete_insert` | 48.84ms | 90.89ms | 1.9× | 1.4% | PASS |
| mem_writes | `oltp_write_only` | 26.62ms | 53.54ms | 2.0× | 1.2% | PASS |
| mem_writes | `types_delete_insert` | 32.00ms | 52.57ms | 1.6× | 1.0% | PASS |
| mem_writes | `oltp_read_write` | 100.99ms | 155.53ms | 1.5× | 1.7% | PASS |
| file_reads | `oltp_point_select` | 103.52ms | 60.76ms | 0.6× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 26.26ms | 25.93ms | 1.0× | 1.8% | PASS |
| file_reads | `oltp_sum_range` | 24.79ms | 25.56ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_order_range` | 4.36ms | 4.56ms | 1.0× | 2.1% | PASS |
| file_reads | `oltp_distinct_range` | 5.55ms | 5.74ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 11.72ms | 8.65ms | 0.7× | 1.4% | PASS |
| file_reads | `select_random_points` | 37.44ms | 38.41ms | 1.0× | 1.4% | PASS |
| file_reads | `select_random_ranges` | 15.18ms | 11.67ms | 0.8× | 1.7% | PASS |
| file_reads | `covering_index_scan` | 15.07ms | 12.86ms | 0.9× | 0.9% | PASS |
| file_reads | `groupby_scan` | 36.38ms | 41.70ms | 1.1× | 0.9% | PASS |
| file_reads | `index_join` | 12.10ms | 13.31ms | 1.1× | 1.4% | PASS |
| file_reads | `index_join_scan` | 4.85ms | 6.95ms | 1.4× | 1.2% | PASS |
| file_reads | `types_table_scan` | 1.05s | 1.29s | 1.2× | 1.0% | PASS |
| file_reads | `table_scan` | 1.25s | 1.41s | 1.1× | 3.0% | PASS |
| file_reads | `oltp_read_only` | 258.37ms | 216.78ms | 0.8× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 261.19ms | 338.64ms | 1.3× | 0.9% | PASS |
| file_writes | `oltp_insert` | 25.60ms | 37.56ms | 1.5× | 1.9% | PASS |
| file_writes | `oltp_update_index` | 96.61ms | 135.55ms | 1.4× | 1.8% | PASS |
| file_writes | `oltp_update_non_index` | 76.55ms | 92.58ms | 1.2× | 1.6% | PASS |
| file_writes | `oltp_delete_insert` | 76.45ms | 105.41ms | 1.4× | 1.7% | PASS |
| file_writes | `oltp_write_only` | 50.99ms | 64.61ms | 1.3× | 2.0% | PASS |
| file_writes | `types_delete_insert` | 48.70ms | 61.33ms | 1.3× | 1.9% | PASS |
| file_writes | `oltp_read_write` | 126.19ms | 166.52ms | 1.3× | 1.9% | PASS |
| ac_reads | `oltp_point_select` | 56.39ms | 60.62ms | 1.1× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 21.82ms | 25.96ms | 1.2× | 1.1% | PASS |
| ac_reads | `oltp_sum_range` | 20.15ms | 25.53ms | 1.3× | 0.9% | PASS |
| ac_reads | `oltp_order_range` | 3.98ms | 4.58ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_distinct_range` | 5.07ms | 5.75ms | 1.1× | 1.0% | PASS |
| ac_reads | `oltp_index_scan` | 7.20ms | 8.66ms | 1.2× | 1.3% | PASS |
| ac_reads | `select_random_points` | 31.46ms | 38.49ms | 1.2× | 1.6% | PASS |
| ac_reads | `select_random_ranges` | 10.21ms | 11.70ms | 1.1× | 1.2% | PASS |
| ac_reads | `covering_index_scan` | 10.36ms | 12.90ms | 1.2× | 1.3% | PASS |
| ac_reads | `groupby_scan` | 35.86ms | 41.78ms | 1.2× | 0.7% | PASS |
| ac_reads | `index_join` | 9.56ms | 13.33ms | 1.4× | 1.5% | PASS |
| ac_reads | `index_join_scan` | 4.36ms | 6.96ms | 1.6× | 1.4% | PASS |
| ac_reads | `types_table_scan` | 1.06s | 1.29s | 1.2× | 0.8% | PASS |
| ac_reads | `table_scan` | 1.23s | 1.40s | 1.1× | 1.7% | PASS |
| ac_reads | `oltp_read_only` | 186.41ms | 216.68ms | 1.2× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 23.71ms | 62.96ms | 2.7× | 7.3% | PASS |
| ac_writes | `oltp_insert_ac` | 24.36ms | 78.63ms | 3.2× | 8.2% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.73ms | 89.36ms | 3.3× | 5.9% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.77ms | 70.28ms | 3.0× | 9.1% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.62ms | 79.69ms | 3.1× | 7.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.30ms | 82.03ms | 3.1× | 6.7% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.70ms | 74.18ms | 3.3× | 8.2% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.33ms | 89.94ms | 2.8× | 7.2% | PASS |

</details>

</details>

## Version-control latency

Wall time: 10m 24s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 29.08ms | 38.00ms | 76.5% | 1.1% | PASS |
| `status_dirty_many_tables` | 30.52ms | 42.00ms | 72.7% | 1.3% | PASS |
| `diff_regular_working_one_table` | 24.10ms | 33.00ms | 73.0% | 1.6% | PASS |
| `diff_regular_working_many_tables` | 35.21ms | 50.00ms | 70.4% | 2.5% | PASS |
| `diff_stat_working_many_tables` | 34.94ms | 48.00ms | 72.8% | 1.2% | PASS |
| `diff_schema_working_many_tables` | 36.80ms | 48.00ms | 76.7% | 0.7% | PASS |
| `branch_list_many_branches` | 18.92ms | 25.00ms | 75.7% | 5.4% | PASS |
| `branch_create_delete` | 24.64ms | 27.00ms | 91.3% | 3.5% | PASS |
| `at_literal_deep_history` | 20.13ms | 28.00ms | 71.9% | 1.1% | PASS |
| `diff_literal_deep_history` | 19.78ms | 28.00ms | 70.6% | 0.5% | PASS |
| `history_literal_deep_history` | 20.86ms | 30.00ms | 69.5% | 0.8% | PASS |
| `checkout_branch_clean` | 82.61ms | 43.00ms | 192.1% | 6.8% | FAIL |
| `merge_data_no_conflicts` | 30.97ms | 33.00ms | 93.8% | 4.5% | PASS |
| `merge_data_secondary_index` | 902.94ms | 944.00ms | 95.7% | 1.8% | PASS |
| `merge_schema_no_conflicts` | 17.92ms | 24.00ms | 74.7% | 3.2% | PASS |
| `merge_data_conflicts` | 24.09ms | 33.00ms | 73.0% | 1.2% | PASS |
| `merge_data_conflicts_with_resolve` | 24.87ms | 34.00ms | 73.1% | 1.6% | PASS |

Version-control ceiling result: **FAIL**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
