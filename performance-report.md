# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-09-05 11:09 UTC
>
> Commit: [`2bbf2dc953b59922fbd69e1c8d98457cb4a114a4`](https://github.com/dolthub/doltlite/commit/2bbf2dc953b59922fbd69e1c8d98457cb4a114a4)
>
> Runner: ubuntu24 20260831.293.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/33958249554)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.36s | 11.62s | 1.1× | 1.4% | **PASS** |
| Writes | 2.22s | 3.72s | 1.7× | 1.2% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 11.25s | 11.86s | 1.1× | 1.5% | **PASS** |
| Writes | 3.08s | 4.06s | 1.3× | 1.8% | **PASS** |
| Autocommit writes | 810.96ms | 2.93s | 3.6× | 6.3% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.44s | 2.67s | 1.1× | 1.3% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.82s | 3.01s | 1.1× | 1.6% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.54s | 2.93s | 1.2× | 1.9% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.56s | 3.01s | 1.2× | 1.3% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 433.34ms | 705.98ms | 1.6× | 1.2% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 605.48ms | 1.04s | 1.7× | 1.6% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 594.75ms | 1.01s | 1.7× | 1.3% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 585.60ms | 971.34ms | 1.7× | 1.1% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.65s | 2.72s | 1.0× | 1.3% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.97s | 3.04s | 1.0× | 1.5% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.83s | 3.01s | 1.1× | 1.7% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.79s | 3.07s | 1.1× | 1.6% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 576.30ms | 767.65ms | 1.3× | 1.5% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 928.64ms | 1.13s | 1.2× | 2.7% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 824.46ms | 1.12s | 1.4× | 1.9% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 748.88ms | 1.05s | 1.4× | 1.7% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.50s | 2.73s | 1.1× | 1.0% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.77s | 3.04s | 1.1× | 1.5% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.73s | 3.03s | 1.1× | 1.5% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.64s | 3.07s | 1.2× | 1.2% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 179.69ms | 670.84ms | 3.7× | 5.9% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 220.71ms | 773.89ms | 3.5× | 7.1% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 213.32ms | 753.34ms | 3.5× | 6.0% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 197.24ms | 732.24ms | 3.7× | 6.5% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 23.70ms | 29.98ms | 1.3× | 1.9% | PASS |
| mem_reads | `oltp_range_select` | 10.06ms | 12.09ms | 1.2× | 2.1% | PASS |
| mem_reads | `oltp_sum_range` | 9.40ms | 11.15ms | 1.2× | 1.3% | PASS |
| mem_reads | `oltp_order_range` | 2.57ms | 2.81ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 3.66ms | 4.05ms | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_index_scan` | 3.80ms | 5.13ms | 1.4× | 1.7% | PASS |
| mem_reads | `select_random_points` | 9.79ms | 10.86ms | 1.1× | 2.5% | PASS |
| mem_reads | `select_random_ranges` | 2.88ms | 3.94ms | 1.4× | 1.6% | PASS |
| mem_reads | `covering_index_scan` | 4.13ms | 4.17ms | 1.0× | 1.1% | PASS |
| mem_reads | `groupby_scan` | 30.76ms | 33.04ms | 1.1× | 0.7% | PASS |
| mem_reads | `index_join` | 5.74ms | 7.81ms | 1.4× | 1.4% | PASS |
| mem_reads | `index_join_scan` | 3.19ms | 4.28ms | 1.3× | 2.0% | PASS |
| mem_reads | `types_table_scan` | 1.05s | 1.16s | 1.1× | 0.5% | PASS |
| mem_reads | `table_scan` | 1.17s | 1.26s | 1.1× | 0.4% | PASS |
| mem_reads | `oltp_read_only` | 102.57ms | 120.12ms | 1.2× | 1.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 179.41ms | 254.34ms | 1.4× | 1.2% | PASS |
| mem_writes | `oltp_insert` | 15.15ms | 27.34ms | 1.8× | 1.2% | PASS |
| mem_writes | `oltp_update_index` | 49.03ms | 94.70ms | 1.9× | 1.4% | PASS |
| mem_writes | `oltp_update_non_index` | 33.75ms | 59.57ms | 1.8× | 1.9% | PASS |
| mem_writes | `oltp_delete_insert` | 44.28ms | 72.31ms | 1.6× | 1.2% | PASS |
| mem_writes | `oltp_write_only` | 21.57ms | 46.51ms | 2.2× | 1.1% | PASS |
| mem_writes | `types_delete_insert` | 24.03ms | 36.71ms | 1.5× | 1.4% | PASS |
| mem_writes | `oltp_read_write` | 66.12ms | 114.50ms | 1.7× | 1.2% | PASS |
| file_reads | `oltp_point_select` | 92.03ms | 48.40ms | 0.5× | 1.3% | PASS |
| file_reads | `oltp_range_select` | 16.93ms | 13.94ms | 0.8× | 1.9% | PASS |
| file_reads | `oltp_sum_range` | 16.46ms | 13.11ms | 0.8× | 1.6% | PASS |
| file_reads | `oltp_order_range` | 3.34ms | 3.08ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_distinct_range` | 4.45ms | 4.31ms | 1.0× | 1.4% | PASS |
| file_reads | `oltp_index_scan` | 10.88ms | 7.37ms | 0.7× | 1.4% | PASS |
| file_reads | `select_random_points` | 17.09ms | 12.99ms | 0.8× | 2.5% | PASS |
| file_reads | `select_random_ranges` | 9.78ms | 5.85ms | 0.6× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 11.12ms | 6.37ms | 0.6× | 1.2% | PASS |
| file_reads | `groupby_scan` | 31.73ms | 33.40ms | 1.1× | 0.7% | PASS |
| file_reads | `index_join` | 9.66ms | 9.44ms | 1.0× | 1.2% | PASS |
| file_reads | `index_join_scan` | 4.10ms | 4.69ms | 1.1× | 2.4% | PASS |
| file_reads | `types_table_scan` | 1.05s | 1.16s | 1.1× | 0.4% | PASS |
| file_reads | `table_scan` | 1.17s | 1.26s | 1.1× | 0.5% | PASS |
| file_reads | `oltp_read_only` | 204.54ms | 148.26ms | 0.7× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 191.87ms | 262.71ms | 1.4× | 0.8% | PASS |
| file_writes | `oltp_insert` | 21.38ms | 31.18ms | 1.5× | 1.7% | PASS |
| file_writes | `oltp_update_index` | 73.78ms | 105.02ms | 1.4× | 1.7% | PASS |
| file_writes | `oltp_update_non_index` | 55.85ms | 69.72ms | 1.2× | 1.7% | PASS |
| file_writes | `oltp_delete_insert` | 65.27ms | 81.15ms | 1.2× | 1.2% | PASS |
| file_writes | `oltp_write_only` | 42.25ms | 53.54ms | 1.3× | 1.4% | PASS |
| file_writes | `types_delete_insert` | 37.90ms | 42.45ms | 1.1× | 1.5% | PASS |
| file_writes | `oltp_read_write` | 87.99ms | 121.87ms | 1.4× | 1.4% | PASS |
| ac_reads | `oltp_point_select` | 45.75ms | 48.30ms | 1.1× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 12.48ms | 13.98ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_sum_range` | 11.86ms | 13.11ms | 1.1× | 1.0% | PASS |
| ac_reads | `oltp_order_range` | 2.90ms | 3.09ms | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_distinct_range` | 3.98ms | 4.29ms | 1.1× | 0.8% | PASS |
| ac_reads | `oltp_index_scan` | 6.36ms | 7.37ms | 1.2× | 1.5% | PASS |
| ac_reads | `select_random_points` | 12.33ms | 13.00ms | 1.1× | 2.2% | PASS |
| ac_reads | `select_random_ranges` | 5.28ms | 5.88ms | 1.1× | 0.9% | PASS |
| ac_reads | `covering_index_scan` | 6.63ms | 6.36ms | 1.0× | 1.5% | PASS |
| ac_reads | `groupby_scan` | 31.21ms | 33.36ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 7.20ms | 9.39ms | 1.3× | 1.4% | PASS |
| ac_reads | `index_join_scan` | 3.55ms | 4.71ms | 1.3× | 1.7% | PASS |
| ac_reads | `types_table_scan` | 1.05s | 1.16s | 1.1× | 0.4% | PASS |
| ac_reads | `table_scan` | 1.17s | 1.26s | 1.1× | 0.5% | PASS |
| ac_reads | `oltp_read_only` | 137.02ms | 148.00ms | 1.1× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 19.87ms | 69.83ms | 3.5× | 5.8% | PASS |
| ac_writes | `oltp_insert_ac` | 22.51ms | 83.20ms | 3.7× | 6.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 24.32ms | 94.88ms | 3.9× | 3.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 20.60ms | 77.72ms | 3.8× | 4.9% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 23.11ms | 87.70ms | 3.8× | 7.2% | PASS |
| ac_writes | `oltp_write_only_ac` | 22.60ms | 86.59ms | 3.8× | 4.5% | PASS |
| ac_writes | `types_delete_insert_ac` | 19.85ms | 78.39ms | 3.9× | 6.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 26.83ms | 92.52ms | 3.4× | 6.0% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 35.99ms | 38.96ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_range_select` | 16.58ms | 14.06ms | 0.8× | 1.9% | PASS |
| mem_reads | `oltp_sum_range` | 15.32ms | 14.06ms | 0.9× | 1.6% | PASS |
| mem_reads | `oltp_order_range` | 3.30ms | 3.18ms | 1.0× | 1.7% | PASS |
| mem_reads | `oltp_distinct_range` | 4.43ms | 4.42ms | 1.0× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 3.98ms | 6.22ms | 1.6× | 1.6% | PASS |
| mem_reads | `select_random_points` | 21.77ms | 21.28ms | 1.0× | 1.6% | PASS |
| mem_reads | `select_random_ranges` | 3.63ms | 5.36ms | 1.5× | 1.1% | PASS |
| mem_reads | `covering_index_scan` | 4.21ms | 4.71ms | 1.1× | 1.8% | PASS |
| mem_reads | `groupby_scan` | 35.74ms | 35.07ms | 1.0× | 1.0% | PASS |
| mem_reads | `index_join` | 10.54ms | 9.64ms | 0.9× | 2.6% | PASS |
| mem_reads | `index_join_scan` | 3.72ms | 5.28ms | 1.4× | 2.4% | PASS |
| mem_reads | `types_table_scan` | 1.13s | 1.29s | 1.1× | 2.7% | PASS |
| mem_reads | `table_scan` | 1.39s | 1.41s | 1.0× | 2.0% | PASS |
| mem_reads | `oltp_read_only` | 142.73ms | 141.75ms | 1.0× | 1.4% | PASS |
| mem_writes | `oltp_bulk_insert` | 246.33ms | 384.09ms | 1.6× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 18.33ms | 39.69ms | 2.2× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 66.20ms | 146.59ms | 2.2× | 1.4% | PASS |
| mem_writes | `oltp_update_non_index` | 49.39ms | 88.11ms | 1.8× | 1.7% | PASS |
| mem_writes | `oltp_delete_insert` | 54.82ms | 108.10ms | 2.0× | 1.1% | PASS |
| mem_writes | `oltp_write_only` | 28.79ms | 64.40ms | 2.2× | 1.8% | PASS |
| mem_writes | `types_delete_insert` | 39.44ms | 55.26ms | 1.4× | 1.9% | PASS |
| mem_writes | `oltp_read_write` | 102.18ms | 153.07ms | 1.5× | 2.6% | PASS |
| file_reads | `oltp_point_select` | 106.81ms | 57.58ms | 0.5× | 1.7% | PASS |
| file_reads | `oltp_range_select` | 24.17ms | 16.02ms | 0.7× | 1.5% | PASS |
| file_reads | `oltp_sum_range` | 23.19ms | 16.29ms | 0.7× | 1.9% | PASS |
| file_reads | `oltp_order_range` | 4.17ms | 3.46ms | 0.8× | 1.8% | PASS |
| file_reads | `oltp_distinct_range` | 5.34ms | 4.72ms | 0.9× | 1.2% | PASS |
| file_reads | `oltp_index_scan` | 11.33ms | 8.43ms | 0.7× | 1.5% | PASS |
| file_reads | `select_random_points` | 31.17ms | 24.53ms | 0.8× | 2.6% | PASS |
| file_reads | `select_random_ranges` | 10.77ms | 7.35ms | 0.7× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 11.58ms | 6.87ms | 0.6× | 1.4% | PASS |
| file_reads | `groupby_scan` | 36.82ms | 35.51ms | 1.0× | 0.8% | PASS |
| file_reads | `index_join` | 14.79ms | 11.02ms | 0.7× | 2.4% | PASS |
| file_reads | `index_join_scan` | 4.76ms | 5.68ms | 1.2× | 1.9% | PASS |
| file_reads | `types_table_scan` | 1.09s | 1.27s | 1.2× | 0.8% | PASS |
| file_reads | `table_scan` | 1.35s | 1.40s | 1.0× | 2.0% | PASS |
| file_reads | `oltp_read_only` | 252.03ms | 171.55ms | 0.7× | 1.3% | PASS |
| file_writes | `oltp_bulk_insert` | 271.30ms | 397.56ms | 1.5× | 1.4% | PASS |
| file_writes | `oltp_insert` | 25.77ms | 46.49ms | 1.8× | 1.8% | PASS |
| file_writes | `oltp_update_index` | 132.85ms | 163.21ms | 1.2× | 6.1% | PASS |
| file_writes | `oltp_update_non_index` | 100.09ms | 101.63ms | 1.0× | 8.9% | PASS |
| file_writes | `oltp_delete_insert` | 95.88ms | 121.19ms | 1.3× | 2.0% | PASS |
| file_writes | `oltp_write_only` | 87.52ms | 74.36ms | 0.8× | 11.4% | PASS |
| file_writes | `types_delete_insert` | 72.58ms | 63.80ms | 0.9× | 2.3% | PASS |
| file_writes | `oltp_read_write` | 142.64ms | 161.37ms | 1.1× | 3.2% | PASS |
| ac_reads | `oltp_point_select` | 59.47ms | 57.34ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 19.48ms | 16.05ms | 0.8× | 2.4% | PASS |
| ac_reads | `oltp_sum_range` | 18.51ms | 16.23ms | 0.9× | 1.5% | PASS |
| ac_reads | `oltp_order_range` | 3.78ms | 3.50ms | 0.9× | 1.5% | PASS |
| ac_reads | `oltp_distinct_range` | 4.88ms | 4.76ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_index_scan` | 6.67ms | 8.46ms | 1.3× | 1.3% | PASS |
| ac_reads | `select_random_points` | 25.64ms | 24.27ms | 0.9× | 2.9% | PASS |
| ac_reads | `select_random_ranges` | 6.15ms | 7.33ms | 1.2× | 1.4% | PASS |
| ac_reads | `covering_index_scan` | 6.92ms | 6.87ms | 1.0× | 1.4% | PASS |
| ac_reads | `groupby_scan` | 35.87ms | 35.44ms | 1.0× | 0.8% | PASS |
| ac_reads | `index_join` | 12.53ms | 11.03ms | 0.9× | 2.9% | PASS |
| ac_reads | `index_join_scan` | 4.24ms | 5.64ms | 1.3× | 2.4% | PASS |
| ac_reads | `types_table_scan` | 1.10s | 1.28s | 1.2× | 1.6% | PASS |
| ac_reads | `table_scan` | 1.28s | 1.40s | 1.1× | 3.3% | PASS |
| ac_reads | `oltp_read_only` | 177.71ms | 169.92ms | 1.0× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 24.85ms | 77.25ms | 3.1× | 6.2% | PASS |
| ac_writes | `oltp_insert_ac` | 29.13ms | 95.79ms | 3.3× | 8.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 29.29ms | 111.54ms | 3.8× | 8.9% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.74ms | 89.06ms | 3.8× | 4.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 27.59ms | 99.83ms | 3.6× | 7.0% | PASS |
| ac_writes | `oltp_write_only_ac` | 27.32ms | 103.07ms | 3.8× | 7.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 24.62ms | 90.08ms | 3.7× | 7.2% | PASS |
| ac_writes | `oltp_read_write_ac` | 34.17ms | 107.26ms | 3.1× | 5.7% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 35.90ms | 38.22ms | 1.1× | 1.9% | PASS |
| mem_reads | `oltp_range_select` | 16.49ms | 13.83ms | 0.8× | 2.4% | PASS |
| mem_reads | `oltp_sum_range` | 15.38ms | 13.90ms | 0.9× | 2.7% | PASS |
| mem_reads | `oltp_order_range` | 3.27ms | 3.16ms | 1.0× | 1.3% | PASS |
| mem_reads | `oltp_distinct_range` | 4.43ms | 4.40ms | 1.0× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 4.00ms | 6.20ms | 1.6× | 1.4% | PASS |
| mem_reads | `select_random_points` | 22.12ms | 20.90ms | 0.9× | 3.4% | PASS |
| mem_reads | `select_random_ranges` | 3.52ms | 5.32ms | 1.5× | 1.9% | PASS |
| mem_reads | `covering_index_scan` | 4.20ms | 4.81ms | 1.1× | 2.4% | PASS |
| mem_reads | `groupby_scan` | 35.34ms | 34.88ms | 1.0× | 0.8% | PASS |
| mem_reads | `index_join` | 10.61ms | 9.54ms | 0.9× | 2.2% | PASS |
| mem_reads | `index_join_scan` | 3.75ms | 5.26ms | 1.4× | 2.9% | PASS |
| mem_reads | `types_table_scan` | 1.06s | 1.26s | 1.2× | 0.6% | PASS |
| mem_reads | `table_scan` | 1.19s | 1.37s | 1.2× | 0.4% | PASS |
| mem_reads | `oltp_read_only` | 137.42ms | 138.31ms | 1.0× | 0.9% | PASS |
| mem_writes | `oltp_bulk_insert` | 247.63ms | 376.21ms | 1.5× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 18.77ms | 38.97ms | 2.1× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 64.83ms | 140.98ms | 2.2× | 2.1% | PASS |
| mem_writes | `oltp_update_non_index` | 47.84ms | 84.37ms | 1.8× | 1.4% | PASS |
| mem_writes | `oltp_delete_insert` | 53.46ms | 103.88ms | 1.9× | 1.4% | PASS |
| mem_writes | `oltp_write_only` | 28.39ms | 63.12ms | 2.2× | 1.2% | PASS |
| mem_writes | `types_delete_insert` | 38.09ms | 52.90ms | 1.4× | 2.1% | PASS |
| mem_writes | `oltp_read_write` | 95.72ms | 146.95ms | 1.5× | 1.3% | PASS |
| file_reads | `oltp_point_select` | 105.17ms | 57.60ms | 0.5× | 1.3% | PASS |
| file_reads | `oltp_range_select` | 23.44ms | 16.02ms | 0.7× | 2.1% | PASS |
| file_reads | `oltp_sum_range` | 23.11ms | 16.18ms | 0.7× | 2.0% | PASS |
| file_reads | `oltp_order_range` | 4.34ms | 3.55ms | 0.8× | 1.9% | PASS |
| file_reads | `oltp_distinct_range` | 5.51ms | 4.83ms | 0.9× | 1.6% | PASS |
| file_reads | `oltp_index_scan` | 11.25ms | 8.44ms | 0.8× | 1.0% | PASS |
| file_reads | `select_random_points` | 30.28ms | 23.94ms | 0.8× | 3.4% | PASS |
| file_reads | `select_random_ranges` | 10.51ms | 7.34ms | 0.7× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 11.36ms | 6.87ms | 0.6× | 1.7% | PASS |
| file_reads | `groupby_scan` | 36.28ms | 35.31ms | 1.0× | 0.9% | PASS |
| file_reads | `index_join` | 14.25ms | 10.99ms | 0.8× | 2.5% | PASS |
| file_reads | `index_join_scan` | 4.62ms | 5.58ms | 1.2× | 2.0% | PASS |
| file_reads | `types_table_scan` | 1.06s | 1.25s | 1.2× | 0.5% | PASS |
| file_reads | `table_scan` | 1.24s | 1.39s | 1.1× | 1.8% | PASS |
| file_reads | `oltp_read_only` | 259.34ms | 173.06ms | 0.7× | 1.4% | PASS |
| file_writes | `oltp_bulk_insert` | 269.56ms | 392.08ms | 1.5× | 1.1% | PASS |
| file_writes | `oltp_insert` | 26.70ms | 47.09ms | 1.8× | 1.9% | PASS |
| file_writes | `oltp_update_index` | 102.90ms | 164.06ms | 1.6× | 2.3% | PASS |
| file_writes | `oltp_update_non_index` | 86.14ms | 101.61ms | 1.2× | 5.5% | PASS |
| file_writes | `oltp_delete_insert` | 90.63ms | 120.38ms | 1.3× | 1.9% | PASS |
| file_writes | `oltp_write_only` | 57.89ms | 73.00ms | 1.3× | 2.5% | PASS |
| file_writes | `types_delete_insert` | 63.90ms | 62.43ms | 1.0× | 1.8% | PASS |
| file_writes | `oltp_read_write` | 126.74ms | 157.65ms | 1.2× | 1.2% | PASS |
| ac_reads | `oltp_point_select` | 58.54ms | 57.61ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_range_select` | 18.73ms | 16.04ms | 0.9× | 1.6% | PASS |
| ac_reads | `oltp_sum_range` | 17.72ms | 16.18ms | 0.9× | 1.5% | PASS |
| ac_reads | `oltp_order_range` | 3.84ms | 3.50ms | 0.9× | 1.9% | PASS |
| ac_reads | `oltp_distinct_range` | 5.03ms | 4.76ms | 0.9× | 1.7% | PASS |
| ac_reads | `oltp_index_scan` | 6.78ms | 8.58ms | 1.3× | 2.8% | PASS |
| ac_reads | `select_random_points` | 27.31ms | 24.78ms | 0.9× | 2.6% | PASS |
| ac_reads | `select_random_ranges` | 6.06ms | 7.34ms | 1.2× | 1.1% | PASS |
| ac_reads | `covering_index_scan` | 6.93ms | 6.88ms | 1.0× | 1.5% | PASS |
| ac_reads | `groupby_scan` | 36.02ms | 35.43ms | 1.0× | 0.8% | PASS |
| ac_reads | `index_join` | 12.01ms | 11.06ms | 0.9× | 3.0% | PASS |
| ac_reads | `index_join_scan` | 4.26ms | 5.63ms | 1.3× | 1.8% | PASS |
| ac_reads | `types_table_scan` | 1.07s | 1.26s | 1.2× | 1.3% | PASS |
| ac_reads | `table_scan` | 1.26s | 1.40s | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_read_only` | 184.33ms | 171.71ms | 0.9× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.98ms | 76.81ms | 3.3× | 5.0% | PASS |
| ac_writes | `oltp_insert_ac` | 25.43ms | 93.70ms | 3.7× | 5.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 28.63ms | 104.89ms | 3.7× | 5.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.63ms | 87.23ms | 3.7× | 6.1% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 27.31ms | 97.30ms | 3.6× | 5.5% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.75ms | 96.26ms | 3.6× | 7.4% | PASS |
| ac_writes | `types_delete_insert_ac` | 24.93ms | 91.66ms | 3.7× | 7.3% | PASS |
| ac_writes | `oltp_read_write_ac` | 33.66ms | 105.49ms | 3.1× | 6.7% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 32.95ms | 41.41ms | 1.3× | 1.6% | PASS |
| mem_reads | `oltp_range_select` | 19.03ms | 21.11ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_sum_range` | 17.59ms | 20.63ms | 1.2× | 1.5% | PASS |
| mem_reads | `oltp_order_range` | 3.52ms | 3.88ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 4.74ms | 5.20ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 4.48ms | 5.98ms | 1.3× | 1.4% | PASS |
| mem_reads | `select_random_points` | 27.35ms | 32.08ms | 1.2× | 1.4% | PASS |
| mem_reads | `select_random_ranges` | 7.62ms | 8.91ms | 1.2× | 1.2% | PASS |
| mem_reads | `covering_index_scan` | 4.17ms | 4.25ms | 1.0× | 1.3% | PASS |
| mem_reads | `groupby_scan` | 36.99ms | 40.40ms | 1.1× | 0.6% | PASS |
| mem_reads | `index_join` | 7.93ms | 10.27ms | 1.3× | 1.3% | PASS |
| mem_reads | `index_join_scan` | 3.85ms | 5.35ms | 1.4× | 1.6% | PASS |
| mem_reads | `types_table_scan` | 1.05s | 1.27s | 1.2× | 0.4% | PASS |
| mem_reads | `table_scan` | 1.19s | 1.37s | 1.2× | 0.5% | PASS |
| mem_reads | `oltp_read_only` | 147.74ms | 173.42ms | 1.2× | 1.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 246.51ms | 357.00ms | 1.4× | 1.1% | PASS |
| mem_writes | `oltp_insert` | 18.86ms | 35.35ms | 1.9× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 64.53ms | 126.26ms | 2.0× | 1.4% | PASS |
| mem_writes | `oltp_update_non_index` | 49.72ms | 83.85ms | 1.7× | 1.1% | PASS |
| mem_writes | `oltp_delete_insert` | 48.84ms | 95.89ms | 2.0× | 1.1% | PASS |
| mem_writes | `oltp_write_only` | 26.22ms | 57.92ms | 2.2× | 1.3% | PASS |
| mem_writes | `types_delete_insert` | 32.30ms | 53.39ms | 1.7× | 1.5% | PASS |
| mem_writes | `oltp_read_write` | 98.61ms | 161.69ms | 1.6× | 1.1% | PASS |
| file_reads | `oltp_point_select` | 102.37ms | 59.74ms | 0.6× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 26.31ms | 23.20ms | 0.9× | 2.2% | PASS |
| file_reads | `oltp_sum_range` | 24.83ms | 22.70ms | 0.9× | 1.6% | PASS |
| file_reads | `oltp_order_range` | 4.25ms | 4.19ms | 1.0× | 1.7% | PASS |
| file_reads | `oltp_distinct_range` | 5.58ms | 5.47ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 11.50ms | 8.12ms | 0.7× | 1.6% | PASS |
| file_reads | `select_random_points` | 36.19ms | 35.18ms | 1.0× | 1.7% | PASS |
| file_reads | `select_random_ranges` | 14.89ms | 10.99ms | 0.7× | 1.7% | PASS |
| file_reads | `covering_index_scan` | 11.15ms | 6.41ms | 0.6× | 1.8% | PASS |
| file_reads | `groupby_scan` | 37.95ms | 40.62ms | 1.1× | 1.0% | PASS |
| file_reads | `index_join` | 11.92ms | 11.94ms | 1.0× | 1.6% | PASS |
| file_reads | `index_join_scan` | 4.95ms | 5.82ms | 1.2× | 2.5% | PASS |
| file_reads | `types_table_scan` | 1.05s | 1.27s | 1.2× | 0.7% | PASS |
| file_reads | `table_scan` | 1.20s | 1.37s | 1.1× | 0.6% | PASS |
| file_reads | `oltp_read_only` | 251.68ms | 201.87ms | 0.8× | 1.1% | PASS |
| file_writes | `oltp_bulk_insert` | 260.89ms | 366.26ms | 1.4× | 1.4% | PASS |
| file_writes | `oltp_insert` | 25.78ms | 40.98ms | 1.6× | 2.2% | PASS |
| file_writes | `oltp_update_index` | 92.81ms | 138.42ms | 1.5× | 1.3% | PASS |
| file_writes | `oltp_update_non_index` | 74.47ms | 96.97ms | 1.3× | 1.6% | PASS |
| file_writes | `oltp_delete_insert` | 73.52ms | 106.80ms | 1.5× | 1.8% | PASS |
| file_writes | `oltp_write_only` | 48.77ms | 68.17ms | 1.4× | 2.0% | PASS |
| file_writes | `types_delete_insert` | 48.28ms | 59.03ms | 1.2× | 2.1% | PASS |
| file_writes | `oltp_read_write` | 124.38ms | 172.12ms | 1.4× | 1.2% | PASS |
| ac_reads | `oltp_point_select` | 56.04ms | 60.03ms | 1.1× | 0.9% | PASS |
| ac_reads | `oltp_range_select` | 21.81ms | 23.16ms | 1.1× | 1.6% | PASS |
| ac_reads | `oltp_sum_range` | 20.06ms | 22.77ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_order_range` | 3.88ms | 4.15ms | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_distinct_range` | 5.06ms | 5.46ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_index_scan` | 6.94ms | 8.14ms | 1.2× | 2.0% | PASS |
| ac_reads | `select_random_points` | 30.63ms | 35.22ms | 1.1× | 1.1% | PASS |
| ac_reads | `select_random_ranges` | 10.17ms | 11.00ms | 1.1× | 1.1% | PASS |
| ac_reads | `covering_index_scan` | 6.65ms | 6.40ms | 1.0× | 2.1% | PASS |
| ac_reads | `groupby_scan` | 37.24ms | 40.64ms | 1.1× | 0.7% | PASS |
| ac_reads | `index_join` | 9.38ms | 11.95ms | 1.3× | 1.4% | PASS |
| ac_reads | `index_join_scan` | 4.31ms | 5.78ms | 1.3× | 1.8% | PASS |
| ac_reads | `types_table_scan` | 1.05s | 1.26s | 1.2× | 0.7% | PASS |
| ac_reads | `table_scan` | 1.20s | 1.37s | 1.1× | 0.4% | PASS |
| ac_reads | `oltp_read_only` | 183.20ms | 201.91ms | 1.1× | 0.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.37ms | 74.39ms | 3.5× | 6.8% | PASS |
| ac_writes | `oltp_insert_ac` | 24.93ms | 90.48ms | 3.6× | 6.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 25.92ms | 101.61ms | 3.9× | 5.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.89ms | 88.72ms | 3.9× | 6.9% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 23.69ms | 95.44ms | 4.0× | 5.4% | PASS |
| ac_writes | `oltp_write_only_ac` | 24.98ms | 93.13ms | 3.7× | 6.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 21.93ms | 85.62ms | 3.9× | 7.0% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.54ms | 102.85ms | 3.3× | 5.4% | PASS |

</details>

</details>

## Version-control latency

Wall time: 1m 56s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 67.99ms | 130.00ms | 52.3% | 0.4% | PASS |
| `status_dirty_many_tables` | 70.55ms | 130.00ms | 54.3% | 0.4% | PASS |
| `diff_regular_working_one_table` | 62.77ms | 120.00ms | 52.3% | 0.6% | PASS |
| `diff_regular_working_many_tables` | 74.18ms | 140.00ms | 53.0% | 0.3% | PASS |
| `diff_stat_working_many_tables` | 74.19ms | 140.00ms | 53.0% | 0.4% | PASS |
| `diff_schema_working_many_tables` | 74.62ms | 140.00ms | 53.3% | 0.4% | PASS |
| `branch_list_many_branches` | 21.48ms | 35.00ms | 61.4% | 1.3% | PASS |
| `branch_create_delete` | 32.69ms | 40.00ms | 81.7% | 1.5% | PASS |
| `checkout_branch_clean` | 99.92ms | 150.00ms | 66.6% | 0.5% | PASS |
| `merge_data_no_conflicts` | 38.61ms | 50.00ms | 77.2% | 0.8% | PASS |
| `merge_schema_no_conflicts` | 22.62ms | 35.00ms | 64.6% | 1.5% | PASS |
| `merge_data_conflicts` | 29.55ms | 180.00ms | 16.4% | 0.9% | PASS |
| `merge_data_conflicts_with_resolve` | 29.66ms | 180.00ms | 16.5% | 1.2% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
