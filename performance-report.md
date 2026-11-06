# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-11-06 11:25 UTC
>
> Commit: [`6b1d6e8a434ae795e16d06199ed5e2fe20c6f1ed`](https://github.com/dolthub/doltlite/commit/6b1d6e8a434ae795e16d06199ed5e2fe20c6f1ed)
>
> Runner: ubuntu24 20261101.360.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/42195807092)

The SQL workloads compare optimized DoltLite against stock SQLite on the same GitHub-hosted runner, alternating execution order on each repetition. Version-control sections measure DoltLite latency. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 8.45s | 10.01s | 1.2× | 1.2% | **PASS** |
| Writes | 1.77s | 2.88s | 1.6× | 1.1% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 8.89s | 10.17s | 1.1× | 1.2% | **PASS** |
| Writes | 3.83s | 4.10s | 1.1× | 3.6% | **PASS** |
| Autocommit writes | 763.08ms | 2.24s | 2.9× | 4.8% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 1.93s | 2.31s | 1.2× | 0.7% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.32s | 2.55s | 1.1× | 2.0% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 1.55s | 1.83s | 1.2× | 2.0% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.65s | 3.32s | 1.3× | 1.2% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 312.57ms | 533.88ms | 1.7× | 0.9% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 499.25ms | 752.38ms | 1.5× | 2.1% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 359.12ms | 587.27ms | 1.6× | 1.6% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 604.00ms | 1.01s | 1.7× | 1.0% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.00s | 2.32s | 1.2× | 1.0% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.32s | 2.50s | 1.1× | 1.2% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 1.82s | 1.99s | 1.1× | 1.3% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.74s | 3.36s | 1.2× | 1.2% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 754.93ms | 817.72ms | 1.1× | 2.9% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.33s | 1.23s | 0.9× | 13.5% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 990.42ms | 989.61ms | 1.0× | 4.6% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 753.90ms | 1.06s | 1.4× | 1.4% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 1.96s | 2.33s | 1.2× | 0.8% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.20s | 2.51s | 1.1× | 0.9% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 1.75s | 2.03s | 1.2× | 1.9% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.57s | 3.35s | 1.3× | 1.2% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 202.85ms | 585.99ms | 2.9× | 4.6% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 170.37ms | 579.18ms | 3.4× | 21.0% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 213.52ms | 490.13ms | 2.3× | 4.9% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 176.33ms | 589.47ms | 3.3× | 3.5% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 18.21ms | 22.26ms | 1.2× | 0.8% | PASS |
| mem_reads | `oltp_range_select` | 8.40ms | 10.44ms | 1.2× | 0.8% | PASS |
| mem_reads | `oltp_sum_range` | 7.56ms | 9.46ms | 1.3× | 1.1% | PASS |
| mem_reads | `oltp_order_range` | 2.19ms | 2.54ms | 1.2× | 0.7% | PASS |
| mem_reads | `oltp_distinct_range` | 2.94ms | 3.42ms | 1.2× | 0.5% | PASS |
| mem_reads | `oltp_index_scan` | 3.04ms | 3.87ms | 1.3× | 1.0% | PASS |
| mem_reads | `select_random_points` | 10.59ms | 9.85ms | 0.9× | 1.0% | PASS |
| mem_reads | `select_random_ranges` | 3.43ms | 3.75ms | 1.1× | 0.9% | PASS |
| mem_reads | `covering_index_scan` | 5.45ms | 7.79ms | 1.4× | 0.5% | PASS |
| mem_reads | `groupby_scan` | 25.41ms | 27.29ms | 1.1× | 0.6% | PASS |
| mem_reads | `index_join` | 4.42ms | 6.47ms | 1.5× | 0.7% | PASS |
| mem_reads | `index_join_scan` | 2.44ms | 4.58ms | 1.9× | 1.0% | PASS |
| mem_reads | `types_table_scan` | 826.93ms | 999.15ms | 1.2× | 0.4% | PASS |
| mem_reads | `table_scan` | 929.50ms | 1.10s | 1.2× | 0.4% | PASS |
| mem_reads | `oltp_read_only` | 80.42ms | 97.73ms | 1.2× | 0.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 121.85ms | 208.23ms | 1.7× | 0.5% | PASS |
| mem_writes | `oltp_insert` | 10.97ms | 18.23ms | 1.7× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 38.08ms | 71.47ms | 1.9× | 0.9% | PASS |
| mem_writes | `oltp_update_non_index` | 25.40ms | 44.82ms | 1.8× | 1.2% | PASS |
| mem_writes | `oltp_delete_insert` | 34.01ms | 54.30ms | 1.6× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 16.33ms | 33.20ms | 2.0× | 1.1% | PASS |
| mem_writes | `types_delete_insert` | 17.86ms | 25.74ms | 1.4× | 0.8% | PASS |
| mem_writes | `oltp_read_write` | 48.06ms | 77.89ms | 1.6× | 0.8% | PASS |
| file_reads | `oltp_point_select` | 40.47ms | 27.18ms | 0.7× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 10.97ms | 11.26ms | 1.0× | 0.9% | PASS |
| file_reads | `oltp_sum_range` | 9.97ms | 10.20ms | 1.0× | 1.0% | PASS |
| file_reads | `oltp_order_range` | 2.52ms | 2.68ms | 1.1× | 1.3% | PASS |
| file_reads | `oltp_distinct_range` | 3.30ms | 3.58ms | 1.1× | 1.0% | PASS |
| file_reads | `oltp_index_scan` | 5.49ms | 4.71ms | 0.9× | 1.4% | PASS |
| file_reads | `select_random_points` | 13.37ms | 10.88ms | 0.8× | 1.0% | PASS |
| file_reads | `select_random_ranges` | 5.84ms | 4.34ms | 0.7× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 7.94ms | 8.60ms | 1.1× | 0.8% | PASS |
| file_reads | `groupby_scan` | 25.38ms | 27.42ms | 1.1× | 0.7% | PASS |
| file_reads | `index_join` | 5.88ms | 7.14ms | 1.2× | 2.0% | PASS |
| file_reads | `index_join_scan` | 2.80ms | 4.76ms | 1.7× | 1.5% | PASS |
| file_reads | `types_table_scan` | 827.75ms | 998.21ms | 1.2× | 0.4% | PASS |
| file_reads | `table_scan` | 929.58ms | 1.10s | 1.2× | 0.4% | PASS |
| file_reads | `oltp_read_only` | 112.34ms | 104.94ms | 0.9× | 0.5% | PASS |
| file_writes | `oltp_bulk_insert` | 159.98ms | 252.04ms | 1.6× | 1.3% | PASS |
| file_writes | `oltp_insert` | 19.81ms | 27.89ms | 1.4× | 0.9% | PASS |
| file_writes | `oltp_update_index` | 121.85ms | 124.04ms | 1.0× | 3.5% | PASS |
| file_writes | `oltp_update_non_index` | 96.67ms | 85.08ms | 0.9× | 3.2% | PASS |
| file_writes | `oltp_delete_insert` | 104.72ms | 102.18ms | 1.0× | 5.4% | PASS |
| file_writes | `oltp_write_only` | 80.62ms | 66.67ms | 0.8× | 5.6% | PASS |
| file_writes | `types_delete_insert` | 60.40ms | 50.18ms | 0.8× | 2.6% | PASS |
| file_writes | `oltp_read_write` | 110.87ms | 109.65ms | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_point_select` | 25.77ms | 27.36ms | 1.1× | 0.6% | PASS |
| ac_reads | `oltp_range_select` | 9.63ms | 11.33ms | 1.2× | 0.9% | PASS |
| ac_reads | `oltp_sum_range` | 9.10ms | 10.34ms | 1.1× | 0.8% | PASS |
| ac_reads | `oltp_order_range` | 2.42ms | 2.75ms | 1.1× | 1.5% | PASS |
| ac_reads | `oltp_distinct_range` | 3.19ms | 3.64ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_index_scan` | 4.24ms | 4.92ms | 1.2× | 0.9% | PASS |
| ac_reads | `select_random_points` | 12.28ms | 11.08ms | 0.9× | 1.1% | PASS |
| ac_reads | `select_random_ranges` | 4.51ms | 4.47ms | 1.0× | 0.7% | PASS |
| ac_reads | `covering_index_scan` | 6.50ms | 8.73ms | 1.3× | 0.6% | PASS |
| ac_reads | `groupby_scan` | 25.73ms | 27.65ms | 1.1× | 0.4% | PASS |
| ac_reads | `index_join` | 5.30ms | 7.40ms | 1.4× | 1.1% | PASS |
| ac_reads | `index_join_scan` | 2.73ms | 4.86ms | 1.8× | 1.5% | PASS |
| ac_reads | `types_table_scan` | 828.15ms | 999.10ms | 1.2× | 0.4% | PASS |
| ac_reads | `table_scan` | 931.53ms | 1.10s | 1.2× | 0.4% | PASS |
| ac_reads | `oltp_read_only` | 91.79ms | 105.80ms | 1.2× | 0.6% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 24.05ms | 64.32ms | 2.7× | 5.0% | PASS |
| ac_writes | `oltp_insert_ac` | 25.39ms | 72.26ms | 2.8× | 3.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.63ms | 81.68ms | 3.1× | 4.8% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.00ms | 69.29ms | 2.9× | 4.2% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.97ms | 74.03ms | 3.0× | 4.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.96ms | 75.58ms | 2.9× | 4.4% | PASS |
| ac_writes | `types_delete_insert_ac` | 23.63ms | 68.76ms | 2.9× | 4.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 28.21ms | 80.06ms | 2.8× | 4.8% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 30.23ms | 30.97ms | 1.0× | 2.8% | PASS |
| mem_reads | `oltp_range_select` | 14.91ms | 13.27ms | 0.9× | 2.7% | PASS |
| mem_reads | `oltp_sum_range` | 13.38ms | 12.80ms | 1.0× | 2.0% | PASS |
| mem_reads | `oltp_order_range` | 2.92ms | 2.94ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 3.70ms | 3.73ms | 1.0× | 0.8% | PASS |
| mem_reads | `oltp_index_scan` | 3.26ms | 5.46ms | 1.7× | 1.3% | PASS |
| mem_reads | `select_random_points` | 19.79ms | 18.17ms | 0.9× | 4.4% | PASS |
| mem_reads | `select_random_ranges` | 5.32ms | 5.25ms | 1.0× | 1.5% | PASS |
| mem_reads | `covering_index_scan` | 6.02ms | 8.35ms | 1.4× | 0.9% | PASS |
| mem_reads | `groupby_scan` | 29.29ms | 29.82ms | 1.0× | 1.0% | PASS |
| mem_reads | `index_join` | 9.86ms | 8.93ms | 0.9× | 3.0% | PASS |
| mem_reads | `index_join_scan` | 3.33ms | 5.98ms | 1.8× | 2.0% | PASS |
| mem_reads | `types_table_scan` | 944.32ms | 1.10s | 1.2× | 3.2% | PASS |
| mem_reads | `table_scan` | 1.13s | 1.19s | 1.1× | 6.8% | PASS |
| mem_reads | `oltp_read_only` | 107.73ms | 111.83ms | 1.0× | 0.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 190.39ms | 268.20ms | 1.4× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 14.39ms | 27.52ms | 1.9× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 57.79ms | 110.97ms | 1.9× | 2.7% | PASS |
| mem_writes | `oltp_update_non_index` | 44.46ms | 63.40ms | 1.4× | 2.5% | PASS |
| mem_writes | `oltp_delete_insert` | 47.91ms | 82.48ms | 1.7× | 1.7% | PASS |
| mem_writes | `oltp_write_only` | 24.83ms | 42.85ms | 1.7× | 2.1% | PASS |
| mem_writes | `types_delete_insert` | 34.82ms | 49.31ms | 1.4× | 2.1% | PASS |
| mem_writes | `oltp_read_write` | 84.66ms | 107.65ms | 1.3× | 2.2% | PASS |
| file_reads | `oltp_point_select` | 93.25ms | 47.83ms | 0.5× | 1.3% | PASS |
| file_reads | `oltp_range_select` | 22.59ms | 15.21ms | 0.7× | 1.1% | PASS |
| file_reads | `oltp_sum_range` | 20.19ms | 14.50ms | 0.7× | 2.5% | PASS |
| file_reads | `oltp_order_range` | 3.63ms | 3.10ms | 0.9× | 1.2% | PASS |
| file_reads | `oltp_distinct_range` | 4.46ms | 3.94ms | 0.9× | 1.0% | PASS |
| file_reads | `oltp_index_scan` | 9.73ms | 7.16ms | 0.7× | 1.3% | PASS |
| file_reads | `select_random_points` | 25.68ms | 19.61ms | 0.8× | 3.1% | PASS |
| file_reads | `select_random_ranges` | 12.02ms | 6.99ms | 0.6× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 12.62ms | 10.16ms | 0.8× | 1.2% | PASS |
| file_reads | `groupby_scan` | 29.68ms | 29.97ms | 1.0× | 0.8% | PASS |
| file_reads | `index_join` | 12.61ms | 9.57ms | 0.8× | 1.1% | PASS |
| file_reads | `index_join_scan` | 3.92ms | 5.90ms | 1.5× | 1.5% | PASS |
| file_reads | `types_table_scan` | 878.22ms | 1.05s | 1.2× | 0.5% | PASS |
| file_reads | `table_scan` | 998.60ms | 1.14s | 1.1× | 0.6% | PASS |
| file_reads | `oltp_read_only` | 196.56ms | 135.94ms | 0.7× | 0.7% | PASS |
| file_writes | `oltp_bulk_insert` | 280.36ms | 347.19ms | 1.2× | 6.7% | PASS |
| file_writes | `oltp_insert` | 33.64ms | 50.65ms | 1.5× | 14.3% | PASS |
| file_writes | `oltp_update_index` | 192.01ms | 198.99ms | 1.0× | 13.5% | PASS |
| file_writes | `oltp_update_non_index` | 154.96ms | 124.96ms | 0.8× | 13.6% | PASS |
| file_writes | `oltp_delete_insert` | 187.96ms | 149.00ms | 0.8× | 3.4% | PASS |
| file_writes | `oltp_write_only` | 133.92ms | 98.40ms | 0.7× | 16.1% | PASS |
| file_writes | `types_delete_insert` | 160.78ms | 97.39ms | 0.6× | 14.4% | PASS |
| file_writes | `oltp_read_write` | 186.97ms | 158.43ms | 0.8× | 10.8% | PASS |
| ac_reads | `oltp_point_select` | 48.75ms | 46.77ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 16.48ms | 14.59ms | 0.9× | 1.0% | PASS |
| ac_reads | `oltp_sum_range` | 15.07ms | 14.02ms | 0.9× | 1.0% | PASS |
| ac_reads | `oltp_order_range` | 3.22ms | 3.10ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_distinct_range` | 4.01ms | 3.93ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_index_scan` | 5.68ms | 7.08ms | 1.2× | 0.7% | PASS |
| ac_reads | `select_random_points` | 20.40ms | 19.07ms | 0.9× | 0.9% | PASS |
| ac_reads | `select_random_ranges` | 7.93ms | 6.99ms | 0.9× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 8.50ms | 10.09ms | 1.2× | 0.9% | PASS |
| ac_reads | `groupby_scan` | 29.25ms | 29.92ms | 1.0× | 0.8% | PASS |
| ac_reads | `index_join` | 10.74ms | 9.62ms | 0.9× | 1.1% | PASS |
| ac_reads | `index_join_scan` | 3.56ms | 5.88ms | 1.7× | 1.3% | PASS |
| ac_reads | `types_table_scan` | 876.81ms | 1.05s | 1.2× | 0.4% | PASS |
| ac_reads | `table_scan` | 1.01s | 1.15s | 1.1× | 1.0% | PASS |
| ac_reads | `oltp_read_only` | 138.88ms | 135.72ms | 1.0× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 20.11ms | 59.22ms | 2.9× | 25.2% | PASS |
| ac_writes | `oltp_insert_ac` | 21.48ms | 68.95ms | 3.2× | 25.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 21.65ms | 83.03ms | 3.8× | 16.8% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 17.91ms | 66.69ms | 3.7× | 17.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 21.41ms | 73.10ms | 3.4× | 15.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 21.51ms | 81.50ms | 3.8× | 37.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 20.98ms | 73.25ms | 3.5× | 25.2% | PASS |
| ac_writes | `oltp_read_write_ac` | 25.32ms | 73.44ms | 2.9× | 13.3% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 21.66ms | 23.61ms | 1.1× | 2.2% | PASS |
| mem_reads | `oltp_range_select` | 9.41ms | 9.69ms | 1.0× | 2.1% | PASS |
| mem_reads | `oltp_sum_range` | 9.14ms | 9.71ms | 1.1× | 2.1% | PASS |
| mem_reads | `oltp_order_range` | 2.02ms | 2.17ms | 1.1× | 2.5% | PASS |
| mem_reads | `oltp_distinct_range` | 2.54ms | 2.68ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 2.46ms | 4.37ms | 1.8× | 1.8% | PASS |
| mem_reads | `select_random_points` | 14.16ms | 14.00ms | 1.0× | 1.7% | PASS |
| mem_reads | `select_random_ranges` | 4.13ms | 4.07ms | 1.0× | 2.4% | PASS |
| mem_reads | `covering_index_scan` | 3.85ms | 6.34ms | 1.6× | 1.9% | PASS |
| mem_reads | `groupby_scan` | 19.21ms | 20.10ms | 1.0× | 1.2% | PASS |
| mem_reads | `index_join` | 7.40ms | 7.31ms | 1.0× | 3.4% | PASS |
| mem_reads | `index_join_scan` | 2.42ms | 5.26ms | 2.2× | 6.6% | PASS |
| mem_reads | `types_table_scan` | 642.48ms | 776.64ms | 1.2× | 0.6% | PASS |
| mem_reads | `table_scan` | 731.33ms | 865.32ms | 1.2× | 1.5% | PASS |
| mem_reads | `oltp_read_only` | 73.64ms | 77.20ms | 1.0× | 2.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 139.86ms | 213.99ms | 1.5× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 11.21ms | 22.98ms | 2.0× | 1.2% | PASS |
| mem_writes | `oltp_update_index` | 42.40ms | 86.18ms | 2.0× | 2.3% | PASS |
| mem_writes | `oltp_update_non_index` | 32.99ms | 51.05ms | 1.5× | 1.5% | PASS |
| mem_writes | `oltp_delete_insert` | 34.12ms | 63.02ms | 1.8× | 1.3% | PASS |
| mem_writes | `oltp_write_only` | 18.54ms | 34.38ms | 1.9× | 1.6% | PASS |
| mem_writes | `types_delete_insert` | 24.46ms | 36.59ms | 1.5× | 2.2% | PASS |
| mem_writes | `oltp_read_write` | 55.54ms | 79.08ms | 1.4× | 1.8% | PASS |
| file_reads | `oltp_point_select` | 72.65ms | 37.49ms | 0.5× | 1.6% | PASS |
| file_reads | `oltp_range_select` | 15.24ms | 11.34ms | 0.7× | 1.5% | PASS |
| file_reads | `oltp_sum_range` | 15.25ms | 11.52ms | 0.8× | 1.5% | PASS |
| file_reads | `oltp_order_range` | 2.87ms | 2.58ms | 0.9× | 1.0% | PASS |
| file_reads | `oltp_distinct_range` | 3.35ms | 3.05ms | 0.9× | 1.2% | PASS |
| file_reads | `oltp_index_scan` | 7.97ms | 5.92ms | 0.7× | 0.9% | PASS |
| file_reads | `select_random_points` | 20.26ms | 15.40ms | 0.8× | 4.1% | PASS |
| file_reads | `select_random_ranges` | 9.78ms | 5.70ms | 0.6× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 9.64ms | 8.03ms | 0.8× | 1.5% | PASS |
| file_reads | `groupby_scan` | 21.29ms | 21.51ms | 1.0× | 0.7% | PASS |
| file_reads | `index_join` | 11.00ms | 8.33ms | 0.8× | 2.0% | PASS |
| file_reads | `index_join_scan` | 3.42ms | 5.28ms | 1.5× | 1.3% | PASS |
| file_reads | `types_table_scan` | 694.01ms | 838.67ms | 1.2× | 1.3% | PASS |
| file_reads | `table_scan` | 779.80ms | 916.82ms | 1.2× | 1.1% | PASS |
| file_reads | `oltp_read_only` | 156.48ms | 101.83ms | 0.7× | 2.1% | PASS |
| file_writes | `oltp_bulk_insert` | 201.33ms | 269.28ms | 1.3× | 4.3% | PASS |
| file_writes | `oltp_insert` | 19.16ms | 41.66ms | 2.2× | 8.9% | PASS |
| file_writes | `oltp_update_index` | 140.69ms | 162.29ms | 1.2× | 3.4% | PASS |
| file_writes | `oltp_update_non_index` | 120.36ms | 103.35ms | 0.9× | 3.6% | PASS |
| file_writes | `oltp_delete_insert` | 141.03ms | 126.94ms | 0.9× | 3.7% | PASS |
| file_writes | `oltp_write_only` | 110.36ms | 83.62ms | 0.8× | 11.4% | PASS |
| file_writes | `types_delete_insert` | 107.48ms | 74.03ms | 0.7× | 6.0% | PASS |
| file_writes | `oltp_read_write` | 150.03ms | 128.45ms | 0.9× | 4.8% | PASS |
| ac_reads | `oltp_point_select` | 40.88ms | 38.58ms | 0.9× | 2.4% | PASS |
| ac_reads | `oltp_range_select` | 12.37ms | 11.47ms | 0.9× | 1.7% | PASS |
| ac_reads | `oltp_sum_range` | 12.21ms | 11.62ms | 1.0× | 2.2% | PASS |
| ac_reads | `oltp_order_range` | 2.54ms | 2.52ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_distinct_range` | 3.05ms | 3.00ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 4.76ms | 5.93ms | 1.2× | 2.4% | PASS |
| ac_reads | `select_random_points` | 17.19ms | 15.34ms | 0.9× | 2.9% | PASS |
| ac_reads | `select_random_ranges` | 6.33ms | 5.68ms | 0.9× | 1.9% | PASS |
| ac_reads | `covering_index_scan` | 6.32ms | 8.15ms | 1.3× | 2.2% | PASS |
| ac_reads | `groupby_scan` | 20.92ms | 21.37ms | 1.0× | 0.9% | PASS |
| ac_reads | `index_join` | 9.54ms | 8.34ms | 0.9× | 3.0% | PASS |
| ac_reads | `index_join_scan` | 3.10ms | 5.17ms | 1.7× | 2.1% | PASS |
| ac_reads | `types_table_scan` | 707.62ms | 855.60ms | 1.2× | 1.2% | PASS |
| ac_reads | `table_scan` | 801.19ms | 939.11ms | 1.2× | 1.0% | PASS |
| ac_reads | `oltp_read_only` | 97.17ms | 95.82ms | 1.0× | 1.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 26.23ms | 51.67ms | 2.0× | 5.6% | PASS |
| ac_writes | `oltp_insert_ac` | 28.21ms | 60.92ms | 2.2× | 4.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 28.58ms | 71.49ms | 2.5× | 5.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.43ms | 55.99ms | 2.3× | 5.1% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 27.94ms | 63.51ms | 2.3× | 4.7% | PASS |
| ac_writes | `oltp_write_only_ac` | 27.26ms | 60.81ms | 2.2× | 4.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 20.54ms | 58.43ms | 2.8× | 6.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 30.33ms | 67.31ms | 2.2× | 3.3% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 34.38ms | 43.79ms | 1.3× | 1.2% | PASS |
| mem_reads | `oltp_range_select` | 20.15ms | 23.22ms | 1.2× | 1.5% | PASS |
| mem_reads | `oltp_sum_range` | 18.22ms | 22.29ms | 1.2× | 1.4% | PASS |
| mem_reads | `oltp_order_range` | 3.59ms | 4.24ms | 1.2× | 1.3% | PASS |
| mem_reads | `oltp_distinct_range` | 4.71ms | 5.42ms | 1.2× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 4.59ms | 6.59ms | 1.4× | 1.9% | PASS |
| mem_reads | `select_random_points` | 28.90ms | 34.06ms | 1.2× | 0.8% | PASS |
| mem_reads | `select_random_ranges` | 7.85ms | 9.65ms | 1.2× | 1.4% | PASS |
| mem_reads | `covering_index_scan` | 7.69ms | 10.99ms | 1.4× | 1.1% | PASS |
| mem_reads | `groupby_scan` | 36.68ms | 40.50ms | 1.1× | 0.8% | PASS |
| mem_reads | `index_join` | 7.93ms | 11.81ms | 1.5× | 1.7% | PASS |
| mem_reads | `index_join_scan` | 3.97ms | 6.65ms | 1.7× | 2.0% | PASS |
| mem_reads | `types_table_scan` | 1.06s | 1.40s | 1.3× | 0.7% | PASS |
| mem_reads | `table_scan` | 1.26s | 1.51s | 1.2× | 0.8% | PASS |
| mem_reads | `oltp_read_only` | 155.47ms | 187.03ms | 1.2× | 0.7% | PASS |
| mem_writes | `oltp_bulk_insert` | 248.81ms | 369.49ms | 1.5× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 19.02ms | 34.99ms | 1.8× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 67.28ms | 128.90ms | 1.9× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 52.69ms | 92.94ms | 1.8× | 1.3% | PASS |
| mem_writes | `oltp_delete_insert` | 50.25ms | 97.35ms | 1.9× | 0.7% | PASS |
| mem_writes | `oltp_write_only` | 27.44ms | 58.29ms | 2.1× | 1.0% | PASS |
| mem_writes | `types_delete_insert` | 33.73ms | 61.09ms | 1.8× | 0.8% | PASS |
| mem_writes | `oltp_read_write` | 104.78ms | 164.86ms | 1.6× | 1.0% | PASS |
| file_reads | `oltp_point_select` | 100.32ms | 61.34ms | 0.6× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 27.26ms | 25.41ms | 0.9× | 0.9% | PASS |
| file_reads | `oltp_sum_range` | 25.51ms | 24.68ms | 1.0× | 0.9% | PASS |
| file_reads | `oltp_order_range` | 4.40ms | 4.56ms | 1.0× | 1.2% | PASS |
| file_reads | `oltp_distinct_range` | 5.62ms | 5.77ms | 1.0× | 1.1% | PASS |
| file_reads | `oltp_index_scan` | 11.34ms | 8.80ms | 0.8× | 1.6% | PASS |
| file_reads | `select_random_points` | 37.48ms | 37.42ms | 1.0× | 1.3% | PASS |
| file_reads | `select_random_ranges` | 14.86ms | 11.71ms | 0.8× | 1.5% | PASS |
| file_reads | `covering_index_scan` | 14.62ms | 13.36ms | 0.9× | 1.9% | PASS |
| file_reads | `groupby_scan` | 37.56ms | 40.99ms | 1.1× | 0.6% | PASS |
| file_reads | `index_join` | 11.93ms | 13.60ms | 1.1× | 1.7% | PASS |
| file_reads | `index_join_scan` | 5.00ms | 7.11ms | 1.4× | 2.7% | PASS |
| file_reads | `types_table_scan` | 1.04s | 1.40s | 1.3× | 1.2% | PASS |
| file_reads | `table_scan` | 1.15s | 1.49s | 1.3× | 0.6% | PASS |
| file_reads | `oltp_read_only` | 246.58ms | 212.41ms | 0.9× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 263.39ms | 381.47ms | 1.4× | 1.0% | PASS |
| file_writes | `oltp_insert` | 25.71ms | 38.67ms | 1.5× | 1.2% | PASS |
| file_writes | `oltp_update_index` | 93.15ms | 140.38ms | 1.5× | 1.0% | PASS |
| file_writes | `oltp_update_non_index` | 75.28ms | 98.51ms | 1.3× | 1.4% | PASS |
| file_writes | `oltp_delete_insert` | 74.26ms | 108.49ms | 1.5× | 1.8% | PASS |
| file_writes | `oltp_write_only` | 48.97ms | 63.98ms | 1.3× | 1.6% | PASS |
| file_writes | `types_delete_insert` | 49.11ms | 65.49ms | 1.3× | 1.7% | PASS |
| file_writes | `oltp_read_write` | 124.02ms | 167.92ms | 1.4× | 1.3% | PASS |
| ac_reads | `oltp_point_select` | 54.69ms | 60.70ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_range_select` | 21.31ms | 25.24ms | 1.2× | 1.8% | PASS |
| ac_reads | `oltp_sum_range` | 19.82ms | 24.48ms | 1.2× | 1.0% | PASS |
| ac_reads | `oltp_order_range` | 3.75ms | 4.53ms | 1.2× | 0.8% | PASS |
| ac_reads | `oltp_distinct_range` | 4.89ms | 5.73ms | 1.2× | 1.2% | PASS |
| ac_reads | `oltp_index_scan` | 6.67ms | 8.66ms | 1.3× | 2.3% | PASS |
| ac_reads | `select_random_points` | 30.38ms | 37.20ms | 1.2× | 1.4% | PASS |
| ac_reads | `select_random_ranges` | 9.88ms | 11.69ms | 1.2× | 1.7% | PASS |
| ac_reads | `covering_index_scan` | 10.00ms | 13.22ms | 1.3× | 1.2% | PASS |
| ac_reads | `groupby_scan` | 36.25ms | 41.06ms | 1.1× | 0.7% | PASS |
| ac_reads | `index_join` | 9.31ms | 13.37ms | 1.4× | 1.4% | PASS |
| ac_reads | `index_join_scan` | 4.29ms | 6.99ms | 1.6× | 1.9% | PASS |
| ac_reads | `types_table_scan` | 1.03s | 1.39s | 1.4× | 0.5% | PASS |
| ac_reads | `table_scan` | 1.15s | 1.49s | 1.3× | 0.5% | PASS |
| ac_reads | `oltp_read_only` | 181.90ms | 212.66ms | 1.2× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 19.49ms | 59.19ms | 3.0× | 3.7% | PASS |
| ac_writes | `oltp_insert_ac` | 21.62ms | 73.83ms | 3.4× | 3.2% | PASS |
| ac_writes | `oltp_update_index_ac` | 23.86ms | 85.07ms | 3.6× | 2.3% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 20.30ms | 67.61ms | 3.3× | 2.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 21.24ms | 75.06ms | 3.5× | 2.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 21.96ms | 76.09ms | 3.5× | 4.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 19.38ms | 68.96ms | 3.6× | 7.1% | PASS |
| ac_writes | `oltp_read_write_ac` | 28.47ms | 83.66ms | 2.9× | 6.7% | PASS |

</details>

</details>

## Version-control latency

Wall time: 4m 35s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 35.53ms | 38.00ms | 93.5% | 0.7% | PASS |
| `status_dirty_many_tables` | 39.36ms | 42.00ms | 93.7% | 0.8% | PASS |
| `diff_regular_working_one_table` | 30.58ms | 33.00ms | 92.7% | 1.0% | PASS |
| `diff_regular_working_many_tables` | 44.54ms | 50.00ms | 89.1% | 0.6% | PASS |
| `diff_stat_working_many_tables` | 44.67ms | 48.00ms | 93.1% | 0.6% | PASS |
| `diff_schema_working_many_tables` | 45.67ms | 48.00ms | 95.2% | 0.9% | PASS |
| `branch_list_many_branches` | 23.65ms | 25.00ms | 94.6% | 1.5% | PASS |
| `branch_create_delete` | 25.16ms | 27.00ms | 93.2% | 1.2% | PASS |
| `at_literal_deep_history` | 25.67ms | 28.00ms | 91.7% | 1.4% | PASS |
| `diff_literal_deep_history` | 25.78ms | 28.00ms | 92.1% | 1.5% | PASS |
| `history_literal_deep_history` | 27.20ms | 30.00ms | 90.7% | 2.0% | PASS |
| `checkout_branch_clean` | 35.88ms | 43.00ms | 83.5% | 1.8% | PASS |
| `merge_data_no_conflicts` | 29.58ms | 33.00ms | 89.6% | 0.9% | PASS |
| `merge_data_secondary_index` | 910.79ms | 944.00ms | 96.5% | 0.6% | PASS |
| `merge_schema_no_conflicts` | 22.68ms | 24.00ms | 94.5% | 1.1% | PASS |
| `merge_data_conflicts` | 32.47ms | 33.00ms | 98.4% | 0.6% | PASS |
| `merge_data_conflicts_with_resolve` | 33.45ms | 34.00ms | 98.4% | 0.6% | PASS |

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

candidate: 6b1d6e8.

Fixture file bytes: base 107,968,301; delta 115,879,032.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 56.815 | 55.192–73.298 | 39.508 | 1760.10 | 107968301 |
| push_initial | candidate | 597.365 | 594.798–604.507 | 582.019 | 167.40 | 107956807 |
| clone_initial | candidate | 778.871 | 777.965–802.492 | 753.007 | 128.39 | 108475105 |
| push_noop | candidate | 0.619 | 0.610–0.744 | 0.338 | — | 0 |
| fetch_noop | candidate | 0.208 | 0.197–0.250 | 0.210 | — | 0 |
| push_incremental | candidate | 236.886 | 234.594–238.686 | 234.189 | 4.22 | 7909775 |
| fetch_incremental | candidate | 98.687 | 97.676–100.512 | 95.945 | 10.13 | 7910084 |
| pull_fast_forward | candidate | 85.520 | 84.851–86.743 | 82.855 | 11.69 | 7910844 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 90.55 | 175.487 | 108.763 | 18329 | 2066 |
| clone_initial | candidate | 90.80 | 295.117 | 109.784 | 47252 | 2337 |
| push_noop | candidate | 42.49 | 0.075 | 0.000 | 27 | 2 |
| fetch_noop | candidate | 42.49 | 0.135 | 0.000 | 40 | 0 |
| push_incremental | candidate | 42.49 | 96.819 | 7.910 | 22402 | 125 |
| fetch_incremental | candidate | 42.49 | 34.472 | 7.910 | 10808 | 124 |
| pull_fast_forward | candidate | 42.49 | 26.497 | 7.911 | 7031 | 126 |

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

candidate: 6b1d6e8.

Fixture file bytes: base 1,087,718,066; delta 1,103,525,194.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 5847.210 | 5686.341–5847.608 | 334.420 | 171.02 | 1087718066 |
| push_initial | candidate | 7436.715 | 7405.294–7809.721 | 3608.806 | 134.47 | 1079566310 |
| clone_initial | candidate | 10393.207 | 9392.500–10764.217 | 5355.242 | 96.22 | 1102407396 |
| push_noop | candidate | 0.858 | 0.725–0.922 | 0.192 | — | 0 |
| fetch_noop | candidate | 0.118 | 0.104–0.122 | 0.119 | — | 0 |
| push_incremental | candidate | 931.401 | 906.806–986.872 | 822.596 | 1.07 | 15806003 |
| fetch_incremental | candidate | 294.256 | 281.445–362.502 | 196.517 | 3.40 | 15806513 |
| pull_fast_forward | candidate | 315.751 | 281.451–375.741 | 197.235 | 3.17 | 15807273 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 134.84 | 2091.765 | 1087.622 | 263470 | 20541 |
| clone_initial | candidate | 124.14 | 3514.713 | 1132.819 | 611563 | 32243 |
| push_noop | candidate | 45.25 | 0.078 | 0.000 | 27 | 2 |
| fetch_noop | candidate | 45.25 | 0.137 | 0.000 | 40 | 0 |
| push_incremental | candidate | 48.30 | 615.622 | 15.928 | 149534 | 313 |
| fetch_incremental | candidate | 50.48 | 62.266 | 15.928 | 13903 | 313 |
| pull_fast_forward | candidate | 50.42 | 62.199 | 15.929 | 13899 | 315 |

</details>


</details>

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh`, `test/vc_perf_ceiling.sh`, and `test/remote_perf.py`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
