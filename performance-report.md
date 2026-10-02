# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-10-02 11:08 UTC
>
> Commit: [`91ec61c76d0a8bbca0032138df6208865dde9148`](https://github.com/dolthub/doltlite/commit/91ec61c76d0a8bbca0032138df6208865dde9148)
>
> Runner: ubuntu24 20260927.320.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/36990925850)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.85s | 11.36s | 1.2× | 1.2% | **PASS** |
| Writes | 2.04s | 3.24s | 1.6× | 1.1% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.54s | 11.58s | 1.1× | 1.4% | **PASS** |
| Writes | 3.31s | 3.87s | 1.2× | 1.7% | **PASS** |
| Autocommit writes | 837.13ms | 2.47s | 3.0× | 4.9% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 1.97s | 2.28s | 1.2× | 0.8% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.57s | 2.99s | 1.2× | 1.2% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.70s | 3.05s | 1.1× | 1.4% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.61s | 3.04s | 1.2× | 1.3% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 303.20ms | 482.97ms | 1.6× | 0.9% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 572.23ms | 924.27ms | 1.6× | 1.0% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 579.14ms | 926.03ms | 1.6× | 1.5% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 586.13ms | 904.52ms | 1.5× | 1.2% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.05s | 2.31s | 1.1× | 1.2% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.86s | 3.08s | 1.1× | 1.6% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.79s | 3.07s | 1.1× | 1.6% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.84s | 3.12s | 1.1× | 1.4% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 799.38ms | 808.00ms | 1.0× | 1.6% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 910.23ms | 1.03s | 1.1× | 4.5% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 838.15ms | 1.04s | 1.2× | 1.6% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 760.16ms | 987.99ms | 1.3× | 1.6% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.00s | 2.31s | 1.2× | 0.9% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.78s | 3.10s | 1.1× | 1.3% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.72s | 3.11s | 1.1× | 1.5% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.69s | 3.11s | 1.2× | 1.3% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 200.32ms | 552.93ms | 2.8× | 2.8% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 228.05ms | 683.80ms | 3.0× | 7.7% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 205.65ms | 618.68ms | 3.0× | 4.3% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 203.11ms | 619.21ms | 3.0× | 6.4% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 17.77ms | 21.11ms | 1.2× | 0.6% | PASS |
| mem_reads | `oltp_range_select` | 8.34ms | 9.22ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_sum_range` | 7.42ms | 8.72ms | 1.2× | 1.4% | PASS |
| mem_reads | `oltp_order_range` | 2.15ms | 2.36ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_distinct_range` | 2.92ms | 3.28ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 2.99ms | 3.69ms | 1.2× | 1.6% | PASS |
| mem_reads | `select_random_points` | 8.36ms | 9.43ms | 1.1× | 1.6% | PASS |
| mem_reads | `select_random_ranges` | 3.38ms | 3.59ms | 1.1× | 0.8% | PASS |
| mem_reads | `covering_index_scan` | 5.40ms | 7.08ms | 1.3× | 0.6% | PASS |
| mem_reads | `groupby_scan` | 24.69ms | 27.32ms | 1.1× | 0.6% | PASS |
| mem_reads | `index_join` | 4.42ms | 6.11ms | 1.4× | 0.7% | PASS |
| mem_reads | `index_join_scan` | 2.41ms | 4.23ms | 1.8× | 1.1% | PASS |
| mem_reads | `types_table_scan` | 841.58ms | 993.54ms | 1.2× | 0.2% | PASS |
| mem_reads | `table_scan` | 961.84ms | 1.09s | 1.1× | 0.3% | PASS |
| mem_reads | `oltp_read_only` | 77.96ms | 92.58ms | 1.2× | 0.4% | PASS |
| mem_writes | `oltp_bulk_insert` | 118.87ms | 186.36ms | 1.6× | 0.4% | PASS |
| mem_writes | `oltp_insert` | 10.77ms | 18.29ms | 1.7× | 0.5% | PASS |
| mem_writes | `oltp_update_index` | 36.94ms | 64.58ms | 1.7× | 0.9% | PASS |
| mem_writes | `oltp_update_non_index` | 24.36ms | 38.53ms | 1.6× | 1.0% | PASS |
| mem_writes | `oltp_delete_insert` | 32.77ms | 49.70ms | 1.5× | 0.8% | PASS |
| mem_writes | `oltp_write_only` | 15.59ms | 29.79ms | 1.9× | 1.4% | PASS |
| mem_writes | `types_delete_insert` | 17.22ms | 23.79ms | 1.4× | 0.6% | PASS |
| mem_writes | `oltp_read_write` | 46.68ms | 71.95ms | 1.5× | 1.1% | PASS |
| file_reads | `oltp_point_select` | 41.71ms | 26.92ms | 0.6× | 0.6% | PASS |
| file_reads | `oltp_range_select` | 10.98ms | 10.09ms | 0.9× | 1.0% | PASS |
| file_reads | `oltp_sum_range` | 10.00ms | 9.60ms | 1.0× | 2.2% | PASS |
| file_reads | `oltp_order_range` | 2.48ms | 2.49ms | 1.0× | 1.2% | PASS |
| file_reads | `oltp_distinct_range` | 3.26ms | 3.38ms | 1.0× | 1.2% | PASS |
| file_reads | `oltp_index_scan` | 5.55ms | 4.55ms | 0.8× | 1.3% | PASS |
| file_reads | `select_random_points` | 11.22ms | 10.61ms | 0.9× | 1.4% | PASS |
| file_reads | `select_random_ranges` | 5.89ms | 4.29ms | 0.7× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 8.02ms | 7.93ms | 1.0× | 0.6% | PASS |
| file_reads | `groupby_scan` | 24.68ms | 27.12ms | 1.1× | 0.9% | PASS |
| file_reads | `index_join` | 5.88ms | 6.74ms | 1.1× | 1.4% | PASS |
| file_reads | `index_join_scan` | 2.77ms | 4.46ms | 1.6× | 1.3% | PASS |
| file_reads | `types_table_scan` | 843.29ms | 997.59ms | 1.2× | 0.2% | PASS |
| file_reads | `table_scan` | 960.75ms | 1.09s | 1.1× | 0.3% | PASS |
| file_reads | `oltp_read_only` | 112.19ms | 100.84ms | 0.9× | 0.5% | PASS |
| file_writes | `oltp_bulk_insert` | 165.16ms | 235.24ms | 1.4× | 1.7% | PASS |
| file_writes | `oltp_insert` | 22.88ms | 31.83ms | 1.4× | 2.3% | PASS |
| file_writes | `oltp_update_index` | 128.99ms | 120.57ms | 0.9× | 1.7% | PASS |
| file_writes | `oltp_update_non_index` | 103.89ms | 83.56ms | 0.8× | 1.0% | PASS |
| file_writes | `oltp_delete_insert` | 110.87ms | 99.03ms | 0.9× | 1.6% | PASS |
| file_writes | `oltp_write_only` | 82.44ms | 70.81ms | 0.9× | 1.4% | PASS |
| file_writes | `types_delete_insert` | 71.51ms | 53.94ms | 0.8× | 2.2% | PASS |
| file_writes | `oltp_read_write` | 113.65ms | 113.02ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_point_select` | 25.73ms | 27.00ms | 1.0× | 0.5% | PASS |
| ac_reads | `oltp_range_select` | 9.59ms | 10.13ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_sum_range` | 8.94ms | 9.66ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_order_range` | 2.33ms | 2.50ms | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_distinct_range` | 3.13ms | 3.39ms | 1.1× | 0.8% | PASS |
| ac_reads | `oltp_index_scan` | 4.02ms | 4.61ms | 1.1× | 2.0% | PASS |
| ac_reads | `select_random_points` | 9.84ms | 10.68ms | 1.1× | 1.3% | PASS |
| ac_reads | `select_random_ranges` | 4.37ms | 4.32ms | 1.0× | 0.9% | PASS |
| ac_reads | `covering_index_scan` | 6.42ms | 7.96ms | 1.2× | 0.8% | PASS |
| ac_reads | `groupby_scan` | 24.71ms | 27.12ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 5.13ms | 6.72ms | 1.3× | 2.0% | PASS |
| ac_reads | `index_join_scan` | 2.63ms | 4.45ms | 1.7× | 1.9% | PASS |
| ac_reads | `types_table_scan` | 843.86ms | 997.95ms | 1.2× | 0.2% | PASS |
| ac_reads | `table_scan` | 961.50ms | 1.09s | 1.1× | 0.2% | PASS |
| ac_reads | `oltp_read_only` | 89.40ms | 101.00ms | 1.1× | 0.4% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 24.04ms | 60.50ms | 2.5× | 2.7% | PASS |
| ac_writes | `oltp_insert_ac` | 25.16ms | 69.53ms | 2.8× | 3.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.01ms | 76.13ms | 2.9× | 2.8% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.89ms | 64.86ms | 2.7× | 3.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.98ms | 71.71ms | 2.9× | 2.7% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.00ms | 70.53ms | 2.8× | 2.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 23.65ms | 65.45ms | 2.8× | 2.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 27.58ms | 74.22ms | 2.7× | 3.3% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.36ms | 39.91ms | 1.2× | 0.9% | PASS |
| mem_reads | `oltp_range_select` | 16.03ms | 16.47ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_sum_range` | 15.21ms | 15.75ms | 1.0× | 0.9% | PASS |
| mem_reads | `oltp_order_range` | 3.29ms | 3.46ms | 1.0× | 1.4% | PASS |
| mem_reads | `oltp_distinct_range` | 4.39ms | 4.51ms | 1.0× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 3.90ms | 6.95ms | 1.8× | 1.3% | PASS |
| mem_reads | `select_random_points` | 21.32ms | 22.71ms | 1.1× | 2.5% | PASS |
| mem_reads | `select_random_ranges` | 6.53ms | 6.87ms | 1.1× | 1.2% | PASS |
| mem_reads | `covering_index_scan` | 7.62ms | 10.80ms | 1.4× | 0.7% | PASS |
| mem_reads | `groupby_scan` | 34.20ms | 35.65ms | 1.0× | 0.6% | PASS |
| mem_reads | `index_join` | 10.26ms | 10.30ms | 1.0× | 1.6% | PASS |
| mem_reads | `index_join_scan` | 3.65ms | 6.34ms | 1.7× | 1.8% | PASS |
| mem_reads | `types_table_scan` | 1.07s | 1.28s | 1.2× | 0.6% | PASS |
| mem_reads | `table_scan` | 1.20s | 1.38s | 1.1× | 0.6% | PASS |
| mem_reads | `oltp_read_only` | 137.18ms | 147.77ms | 1.1× | 1.2% | PASS |
| mem_writes | `oltp_bulk_insert` | 231.14ms | 327.47ms | 1.4× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 17.49ms | 37.36ms | 2.1× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 64.05ms | 131.81ms | 2.1× | 1.3% | PASS |
| mem_writes | `oltp_update_non_index` | 46.73ms | 80.61ms | 1.7× | 1.2% | PASS |
| mem_writes | `oltp_delete_insert` | 53.14ms | 98.25ms | 1.8× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 27.19ms | 55.59ms | 2.0× | 1.0% | PASS |
| mem_writes | `types_delete_insert` | 37.33ms | 54.82ms | 1.5× | 1.3% | PASS |
| mem_writes | `oltp_read_write` | 95.17ms | 138.37ms | 1.5× | 1.0% | PASS |
| file_reads | `oltp_point_select` | 103.62ms | 58.61ms | 0.6× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 23.57ms | 18.60ms | 0.8× | 1.6% | PASS |
| file_reads | `oltp_sum_range` | 23.04ms | 18.11ms | 0.8× | 1.5% | PASS |
| file_reads | `oltp_order_range` | 4.26ms | 3.74ms | 0.9× | 2.0% | PASS |
| file_reads | `oltp_distinct_range` | 5.28ms | 4.82ms | 0.9× | 1.5% | PASS |
| file_reads | `oltp_index_scan` | 11.09ms | 9.04ms | 0.8× | 1.5% | PASS |
| file_reads | `select_random_points` | 30.37ms | 25.76ms | 0.8× | 2.1% | PASS |
| file_reads | `select_random_ranges` | 13.97ms | 8.99ms | 0.6× | 1.6% | PASS |
| file_reads | `covering_index_scan` | 15.12ms | 12.81ms | 0.8× | 1.3% | PASS |
| file_reads | `groupby_scan` | 35.32ms | 35.89ms | 1.0× | 0.7% | PASS |
| file_reads | `index_join` | 14.62ms | 11.63ms | 0.8× | 2.5% | PASS |
| file_reads | `index_join_scan` | 4.68ms | 6.75ms | 1.4× | 2.0% | PASS |
| file_reads | `types_table_scan` | 1.08s | 1.29s | 1.2× | 1.7% | PASS |
| file_reads | `table_scan` | 1.24s | 1.40s | 1.1× | 2.6% | PASS |
| file_reads | `oltp_read_only` | 244.08ms | 177.43ms | 0.7× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 256.25ms | 343.82ms | 1.3× | 0.9% | PASS |
| file_writes | `oltp_insert` | 25.39ms | 42.82ms | 1.7× | 2.2% | PASS |
| file_writes | `oltp_update_index` | 122.78ms | 150.36ms | 1.2× | 12.8% | PASS |
| file_writes | `oltp_update_non_index` | 102.86ms | 94.13ms | 0.9× | 7.8% | PASS |
| file_writes | `oltp_delete_insert` | 94.39ms | 113.63ms | 1.2× | 1.1% | PASS |
| file_writes | `oltp_write_only` | 87.73ms | 68.68ms | 0.8× | 19.2% | PASS |
| file_writes | `types_delete_insert` | 71.35ms | 66.79ms | 0.9× | 2.4% | PASS |
| file_writes | `oltp_read_write` | 149.48ms | 151.86ms | 1.0× | 6.6% | PASS |
| ac_reads | `oltp_point_select` | 57.79ms | 58.89ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_range_select` | 19.82ms | 18.77ms | 0.9× | 2.1% | PASS |
| ac_reads | `oltp_sum_range` | 18.51ms | 18.00ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_order_range` | 3.77ms | 3.72ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_distinct_range` | 4.78ms | 4.80ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_index_scan` | 6.53ms | 9.06ms | 1.4× | 1.0% | PASS |
| ac_reads | `select_random_points` | 25.00ms | 25.72ms | 1.0× | 1.5% | PASS |
| ac_reads | `select_random_ranges` | 9.35ms | 8.98ms | 1.0× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 10.32ms | 12.77ms | 1.2× | 0.9% | PASS |
| ac_reads | `groupby_scan` | 34.48ms | 35.82ms | 1.0× | 0.5% | PASS |
| ac_reads | `index_join` | 12.47ms | 11.68ms | 0.9× | 1.7% | PASS |
| ac_reads | `index_join_scan` | 4.23ms | 6.88ms | 1.6× | 1.8% | PASS |
| ac_reads | `types_table_scan` | 1.10s | 1.30s | 1.2× | 2.5% | PASS |
| ac_reads | `table_scan` | 1.29s | 1.40s | 1.1× | 4.4% | PASS |
| ac_reads | `oltp_read_only` | 182.16ms | 181.05ms | 1.0× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 27.49ms | 71.89ms | 2.6× | 8.0% | PASS |
| ac_writes | `oltp_insert_ac` | 28.71ms | 84.72ms | 3.0× | 5.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 29.73ms | 100.70ms | 3.4× | 5.9% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 25.91ms | 75.94ms | 2.9× | 9.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 28.80ms | 90.91ms | 3.2× | 7.5% | PASS |
| ac_writes | `oltp_write_only_ac` | 27.33ms | 87.16ms | 3.2× | 8.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 26.61ms | 79.68ms | 3.0× | 9.8% | PASS |
| ac_writes | `oltp_read_write_ac` | 33.48ms | 92.79ms | 2.8× | 6.0% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 34.46ms | 40.36ms | 1.2× | 1.4% | PASS |
| mem_reads | `oltp_range_select` | 15.76ms | 16.32ms | 1.0× | 3.2% | PASS |
| mem_reads | `oltp_sum_range` | 14.41ms | 15.53ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_order_range` | 3.16ms | 3.40ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 4.29ms | 4.49ms | 1.0× | 0.7% | PASS |
| mem_reads | `oltp_index_scan` | 3.99ms | 7.05ms | 1.8× | 1.9% | PASS |
| mem_reads | `select_random_points` | 22.57ms | 23.35ms | 1.0× | 3.3% | PASS |
| mem_reads | `select_random_ranges` | 6.65ms | 6.90ms | 1.0× | 1.7% | PASS |
| mem_reads | `covering_index_scan` | 7.69ms | 10.89ms | 1.4× | 0.9% | PASS |
| mem_reads | `groupby_scan` | 33.74ms | 35.54ms | 1.1× | 0.4% | PASS |
| mem_reads | `index_join` | 10.12ms | 10.46ms | 1.0× | 2.4% | PASS |
| mem_reads | `index_join_scan` | 3.66ms | 6.13ms | 1.7× | 1.4% | PASS |
| mem_reads | `types_table_scan` | 1.09s | 1.29s | 1.2× | 1.9% | PASS |
| mem_reads | `table_scan` | 1.31s | 1.43s | 1.1× | 0.6% | PASS |
| mem_reads | `oltp_read_only` | 134.40ms | 147.03ms | 1.1× | 2.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 239.19ms | 333.25ms | 1.4× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 18.26ms | 37.79ms | 2.1× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 63.93ms | 130.46ms | 2.0× | 1.9% | PASS |
| mem_writes | `oltp_update_non_index` | 46.84ms | 78.97ms | 1.7× | 1.3% | PASS |
| mem_writes | `oltp_delete_insert` | 50.88ms | 96.02ms | 1.9× | 1.5% | PASS |
| mem_writes | `oltp_write_only` | 26.82ms | 55.43ms | 2.1× | 1.5% | PASS |
| mem_writes | `types_delete_insert` | 36.38ms | 53.94ms | 1.5× | 1.5% | PASS |
| mem_writes | `oltp_read_write` | 96.84ms | 140.17ms | 1.4× | 2.0% | PASS |
| file_reads | `oltp_point_select` | 107.32ms | 59.84ms | 0.6× | 1.5% | PASS |
| file_reads | `oltp_range_select` | 23.46ms | 18.33ms | 0.8× | 2.0% | PASS |
| file_reads | `oltp_sum_range` | 21.98ms | 17.67ms | 0.8× | 1.6% | PASS |
| file_reads | `oltp_order_range` | 4.25ms | 3.68ms | 0.9× | 1.8% | PASS |
| file_reads | `oltp_distinct_range` | 5.28ms | 4.75ms | 0.9× | 0.7% | PASS |
| file_reads | `oltp_index_scan` | 11.27ms | 9.03ms | 0.8× | 1.6% | PASS |
| file_reads | `select_random_points` | 31.22ms | 26.42ms | 0.8× | 3.3% | PASS |
| file_reads | `select_random_ranges` | 14.01ms | 8.88ms | 0.6× | 1.4% | PASS |
| file_reads | `covering_index_scan` | 15.22ms | 12.80ms | 0.8× | 1.6% | PASS |
| file_reads | `groupby_scan` | 34.60ms | 35.61ms | 1.0× | 1.0% | PASS |
| file_reads | `index_join` | 14.82ms | 11.78ms | 0.8× | 2.5% | PASS |
| file_reads | `index_join_scan` | 4.85ms | 6.76ms | 1.4× | 2.6% | PASS |
| file_reads | `types_table_scan` | 1.04s | 1.29s | 1.2× | 0.7% | PASS |
| file_reads | `table_scan` | 1.21s | 1.39s | 1.2× | 1.4% | PASS |
| file_reads | `oltp_read_only` | 250.93ms | 179.19ms | 0.7× | 0.7% | PASS |
| file_writes | `oltp_bulk_insert` | 261.11ms | 346.92ms | 1.3× | 0.8% | PASS |
| file_writes | `oltp_insert` | 25.39ms | 42.61ms | 1.7× | 1.0% | PASS |
| file_writes | `oltp_update_index` | 97.84ms | 147.15ms | 1.5× | 1.4% | PASS |
| file_writes | `oltp_update_non_index` | 94.58ms | 93.17ms | 1.0× | 8.9% | PASS |
| file_writes | `oltp_delete_insert` | 89.00ms | 113.99ms | 1.3× | 1.5% | PASS |
| file_writes | `oltp_write_only` | 60.42ms | 71.12ms | 1.2× | 2.3% | PASS |
| file_writes | `types_delete_insert` | 66.32ms | 66.23ms | 1.0× | 1.6% | PASS |
| file_writes | `oltp_read_write` | 143.49ms | 159.00ms | 1.1× | 2.3% | PASS |
| ac_reads | `oltp_point_select` | 62.41ms | 61.69ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_range_select` | 20.02ms | 18.87ms | 0.9× | 1.3% | PASS |
| ac_reads | `oltp_sum_range` | 18.62ms | 18.40ms | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_order_range` | 3.82ms | 3.68ms | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_distinct_range` | 4.88ms | 4.77ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_index_scan` | 6.73ms | 9.06ms | 1.3× | 1.3% | PASS |
| ac_reads | `select_random_points` | 26.14ms | 26.59ms | 1.0× | 3.9% | PASS |
| ac_reads | `select_random_ranges` | 9.25ms | 8.90ms | 1.0× | 1.5% | PASS |
| ac_reads | `covering_index_scan` | 10.40ms | 12.79ms | 1.2× | 1.6% | PASS |
| ac_reads | `groupby_scan` | 33.86ms | 35.64ms | 1.1× | 1.0% | PASS |
| ac_reads | `index_join` | 12.05ms | 11.69ms | 1.0× | 2.9% | PASS |
| ac_reads | `index_join_scan` | 4.30ms | 6.59ms | 1.5× | 2.5% | PASS |
| ac_reads | `types_table_scan` | 1.10s | 1.32s | 1.2× | 2.2% | PASS |
| ac_reads | `table_scan` | 1.22s | 1.39s | 1.1× | 0.9% | PASS |
| ac_reads | `oltp_read_only` | 178.10ms | 178.85ms | 1.0× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.48ms | 61.51ms | 2.7× | 4.5% | PASS |
| ac_writes | `oltp_insert_ac` | 25.09ms | 78.15ms | 3.1× | 4.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 28.15ms | 88.37ms | 3.1× | 5.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.87ms | 70.58ms | 3.1× | 3.9% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.00ms | 80.71ms | 3.1× | 4.2% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.72ms | 80.33ms | 3.1× | 3.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 23.76ms | 74.11ms | 3.1× | 9.6% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.59ms | 84.94ms | 2.7× | 3.5% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.66ms | 41.90ms | 1.2× | 1.6% | PASS |
| mem_reads | `oltp_range_select` | 19.28ms | 23.19ms | 1.2× | 1.3% | PASS |
| mem_reads | `oltp_sum_range` | 17.62ms | 21.87ms | 1.2× | 1.3% | PASS |
| mem_reads | `oltp_order_range` | 3.52ms | 4.08ms | 1.2× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 4.64ms | 5.17ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 4.58ms | 6.49ms | 1.4× | 1.5% | PASS |
| mem_reads | `select_random_points` | 29.46ms | 34.35ms | 1.2× | 2.0% | PASS |
| mem_reads | `select_random_ranges` | 7.65ms | 9.10ms | 1.2× | 0.9% | PASS |
| mem_reads | `covering_index_scan` | 7.60ms | 10.63ms | 1.4× | 1.3% | PASS |
| mem_reads | `groupby_scan` | 35.70ms | 40.14ms | 1.1× | 0.8% | PASS |
| mem_reads | `index_join` | 7.86ms | 11.31ms | 1.4× | 1.4% | PASS |
| mem_reads | `index_join_scan` | 3.93ms | 6.26ms | 1.6× | 2.1% | PASS |
| mem_reads | `types_table_scan` | 1.06s | 1.27s | 1.2× | 1.5% | PASS |
| mem_reads | `table_scan` | 1.23s | 1.38s | 1.1× | 2.8% | PASS |
| mem_reads | `oltp_read_only` | 151.05ms | 179.36ms | 1.2× | 1.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 242.29ms | 323.24ms | 1.3× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 18.59ms | 33.21ms | 1.8× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 65.83ms | 116.50ms | 1.8× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 51.25ms | 81.52ms | 1.6× | 1.4% | PASS |
| mem_writes | `oltp_delete_insert` | 48.62ms | 90.19ms | 1.9× | 1.2% | PASS |
| mem_writes | `oltp_write_only` | 26.59ms | 53.24ms | 2.0× | 1.3% | PASS |
| mem_writes | `types_delete_insert` | 32.47ms | 52.59ms | 1.6× | 1.5% | PASS |
| mem_writes | `oltp_read_write` | 100.50ms | 154.05ms | 1.5× | 1.8% | PASS |
| file_reads | `oltp_point_select` | 102.55ms | 60.62ms | 0.6× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 26.21ms | 25.46ms | 1.0× | 1.7% | PASS |
| file_reads | `oltp_sum_range` | 24.84ms | 24.24ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_order_range` | 4.40ms | 4.37ms | 1.0× | 1.9% | PASS |
| file_reads | `oltp_distinct_range` | 5.51ms | 5.48ms | 1.0× | 1.4% | PASS |
| file_reads | `oltp_index_scan` | 11.59ms | 8.52ms | 0.7× | 1.4% | PASS |
| file_reads | `select_random_points` | 36.76ms | 36.71ms | 1.0× | 2.2% | PASS |
| file_reads | `select_random_ranges` | 15.02ms | 11.27ms | 0.8× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 14.89ms | 12.67ms | 0.9× | 1.4% | PASS |
| file_reads | `groupby_scan` | 36.77ms | 40.61ms | 1.1× | 1.0% | PASS |
| file_reads | `index_join` | 11.91ms | 13.00ms | 1.1× | 1.2% | PASS |
| file_reads | `index_join_scan` | 4.94ms | 6.63ms | 1.3× | 1.9% | PASS |
| file_reads | `types_table_scan` | 1.06s | 1.28s | 1.2× | 1.4% | PASS |
| file_reads | `table_scan` | 1.23s | 1.38s | 1.1× | 2.4% | PASS |
| file_reads | `oltp_read_only` | 258.21ms | 210.68ms | 0.8× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 259.69ms | 336.53ms | 1.3× | 0.9% | PASS |
| file_writes | `oltp_insert` | 25.78ms | 37.27ms | 1.4× | 1.7% | PASS |
| file_writes | `oltp_update_index` | 95.43ms | 132.12ms | 1.4× | 1.4% | PASS |
| file_writes | `oltp_update_non_index` | 76.90ms | 91.41ms | 1.2× | 1.6% | PASS |
| file_writes | `oltp_delete_insert` | 75.41ms | 102.60ms | 1.4× | 2.1% | PASS |
| file_writes | `oltp_write_only` | 50.18ms | 62.63ms | 1.2× | 1.6% | PASS |
| file_writes | `types_delete_insert` | 49.19ms | 60.91ms | 1.2× | 1.9% | PASS |
| file_writes | `oltp_read_write` | 127.57ms | 164.53ms | 1.3× | 1.6% | PASS |
| ac_reads | `oltp_point_select` | 56.62ms | 60.75ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_range_select` | 21.91ms | 25.41ms | 1.2× | 1.9% | PASS |
| ac_reads | `oltp_sum_range` | 20.44ms | 24.09ms | 1.2× | 1.6% | PASS |
| ac_reads | `oltp_order_range` | 3.91ms | 4.37ms | 1.1× | 1.0% | PASS |
| ac_reads | `oltp_distinct_range` | 5.01ms | 5.50ms | 1.1× | 0.7% | PASS |
| ac_reads | `oltp_index_scan` | 7.26ms | 8.53ms | 1.2× | 1.8% | PASS |
| ac_reads | `select_random_points` | 31.80ms | 36.72ms | 1.2× | 1.9% | PASS |
| ac_reads | `select_random_ranges` | 10.10ms | 11.28ms | 1.1× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 10.31ms | 12.59ms | 1.2× | 1.3% | PASS |
| ac_reads | `groupby_scan` | 35.95ms | 40.58ms | 1.1× | 0.6% | PASS |
| ac_reads | `index_join` | 9.46ms | 12.88ms | 1.4× | 1.3% | PASS |
| ac_reads | `index_join_scan` | 4.47ms | 6.70ms | 1.5× | 2.8% | PASS |
| ac_reads | `types_table_scan` | 1.06s | 1.28s | 1.2× | 1.1% | PASS |
| ac_reads | `table_scan` | 1.22s | 1.37s | 1.1× | 2.1% | PASS |
| ac_reads | `oltp_read_only` | 186.08ms | 208.64ms | 1.1× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 23.41ms | 62.57ms | 2.7× | 6.2% | PASS |
| ac_writes | `oltp_insert_ac` | 26.14ms | 78.84ms | 3.0× | 6.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.95ms | 89.19ms | 3.3× | 5.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.09ms | 71.59ms | 3.1× | 7.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.48ms | 79.93ms | 3.1× | 7.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 24.83ms | 79.74ms | 3.2× | 4.5% | PASS |
| ac_writes | `types_delete_insert_ac` | 21.80ms | 71.40ms | 3.3× | 7.1% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.42ms | 85.95ms | 2.7× | 4.3% | PASS |

</details>

</details>

## Version-control latency

Wall time: 3m 18s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 34.30ms | 130.00ms | 26.4% | 0.6% | PASS |
| `status_dirty_many_tables` | 37.81ms | 130.00ms | 29.1% | 0.6% | PASS |
| `diff_regular_working_one_table` | 29.39ms | 120.00ms | 24.5% | 0.7% | PASS |
| `diff_regular_working_many_tables` | 42.66ms | 140.00ms | 30.5% | 0.6% | PASS |
| `diff_stat_working_many_tables` | 42.73ms | 140.00ms | 30.5% | 0.5% | PASS |
| `diff_schema_working_many_tables` | 42.94ms | 140.00ms | 30.7% | 0.6% | PASS |
| `branch_list_many_branches` | 21.65ms | 35.00ms | 61.9% | 0.6% | PASS |
| `branch_create_delete` | 24.09ms | 40.00ms | 60.2% | 1.2% | PASS |
| `at_literal_deep_history` | 24.51ms | 100.00ms | 24.5% | 0.7% | PASS |
| `diff_literal_deep_history` | 24.50ms | 120.00ms | 20.4% | 0.6% | PASS |
| `history_literal_deep_history` | 25.71ms | 150.00ms | 17.1% | 0.6% | PASS |
| `checkout_branch_clean` | 36.73ms | 150.00ms | 24.5% | 0.8% | PASS |
| `merge_data_no_conflicts` | 28.32ms | 50.00ms | 56.6% | 1.3% | PASS |
| `merge_data_secondary_index` | 832.35ms | 2.50s | 33.3% | 0.7% | PASS |
| `merge_schema_no_conflicts` | 21.09ms | 35.00ms | 60.2% | 0.9% | PASS |
| `merge_data_conflicts` | 29.69ms | 180.00ms | 16.5% | 0.6% | PASS |
| `merge_data_conflicts_with_resolve` | 30.52ms | 180.00ms | 17.0% | 0.8% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
