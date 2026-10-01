# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-10-01 11:09 UTC
>
> Commit: [`561f0c7e5908752804b60e56ebf199b9aadabdb8`](https://github.com/dolthub/doltlite/commit/561f0c7e5908752804b60e56ebf199b9aadabdb8)
>
> Runner: ubuntu24 20260920.314.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/36844164485)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 8.61s | 9.64s | 1.1× | 1.7% | **PASS** |
| Writes | 1.75s | 2.68s | 1.5× | 1.5% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.63s | 10.01s | 1.0× | 1.7% | **PASS** |
| Writes | 3.41s | 3.61s | 1.1× | 2.3% | **PASS** |
| Autocommit writes | 739.11ms | 2.14s | 2.9× | 6.5% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.64s | 2.83s | 1.1× | 1.4% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 1.84s | 1.93s | 1.0× | 2.9% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.50s | 3.05s | 1.2× | 1.4% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 1.62s | 1.82s | 1.1× | 1.5% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 440.68ms | 691.29ms | 1.6× | 1.3% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 390.68ms | 566.28ms | 1.4× | 2.4% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 568.22ms | 904.35ms | 1.6× | 1.2% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 346.87ms | 515.04ms | 1.5× | 1.3% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.93s | 2.91s | 1.0× | 1.4% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 1.89s | 1.93s | 1.0× | 1.9% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.91s | 3.19s | 1.1× | 1.9% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 1.90s | 1.98s | 1.0× | 1.5% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 593.18ms | 753.78ms | 1.3× | 1.5% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.09s | 971.79ms | 0.9× | 4.2% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 834.81ms | 1.02s | 1.2× | 1.4% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 894.63ms | 866.31ms | 1.0× | 4.1% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.74s | 2.91s | 1.1× | 1.0% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 1.82s | 1.97s | 1.1× | 2.1% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.86s | 3.24s | 1.1× | 1.9% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 1.81s | 1.99s | 1.1× | 1.8% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 150.67ms | 525.18ms | 3.5× | 6.8% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 187.19ms | 501.82ms | 2.7× | 5.9% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 199.29ms | 607.46ms | 3.0× | 6.5% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 201.96ms | 502.82ms | 2.5× | 4.8% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.64ms | 29.12ms | 1.2× | 1.5% | PASS |
| mem_reads | `oltp_range_select` | 10.80ms | 11.74ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_sum_range` | 9.41ms | 11.55ms | 1.2× | 1.2% | PASS |
| mem_reads | `oltp_order_range` | 2.73ms | 2.92ms | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_distinct_range` | 3.74ms | 4.16ms | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_index_scan` | 3.94ms | 5.18ms | 1.3× | 1.6% | PASS |
| mem_reads | `select_random_points` | 10.54ms | 12.13ms | 1.2× | 3.0% | PASS |
| mem_reads | `select_random_ranges` | 4.65ms | 4.83ms | 1.0× | 1.6% | PASS |
| mem_reads | `covering_index_scan` | 7.61ms | 10.04ms | 1.3× | 1.8% | PASS |
| mem_reads | `groupby_scan` | 31.47ms | 35.20ms | 1.1× | 0.7% | PASS |
| mem_reads | `index_join` | 5.66ms | 8.32ms | 1.5× | 2.0% | PASS |
| mem_reads | `index_join_scan` | 3.25ms | 5.64ms | 1.7× | 2.1% | PASS |
| mem_reads | `types_table_scan` | 1.12s | 1.22s | 1.1× | 0.5% | PASS |
| mem_reads | `table_scan` | 1.30s | 1.35s | 1.0× | 0.9% | PASS |
| mem_reads | `oltp_read_only` | 104.27ms | 119.09ms | 1.1× | 1.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 179.03ms | 263.25ms | 1.5× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 15.55ms | 26.77ms | 1.7× | 1.2% | PASS |
| mem_writes | `oltp_update_index` | 51.76ms | 92.72ms | 1.8× | 1.2% | PASS |
| mem_writes | `oltp_update_non_index` | 35.63ms | 56.24ms | 1.6× | 1.8% | PASS |
| mem_writes | `oltp_delete_insert` | 45.25ms | 71.03ms | 1.6× | 1.1% | PASS |
| mem_writes | `oltp_write_only` | 22.39ms | 44.44ms | 2.0× | 1.5% | PASS |
| mem_writes | `types_delete_insert` | 24.94ms | 36.51ms | 1.5× | 1.7% | PASS |
| mem_writes | `oltp_read_write` | 66.13ms | 100.33ms | 1.5× | 1.8% | PASS |
| file_reads | `oltp_point_select` | 108.24ms | 50.46ms | 0.5× | 1.3% | PASS |
| file_reads | `oltp_range_select` | 19.14ms | 13.93ms | 0.7× | 2.3% | PASS |
| file_reads | `oltp_sum_range` | 17.65ms | 13.87ms | 0.8× | 2.0% | PASS |
| file_reads | `oltp_order_range` | 3.60ms | 3.20ms | 0.9× | 2.0% | PASS |
| file_reads | `oltp_distinct_range` | 4.62ms | 4.42ms | 1.0× | 1.4% | PASS |
| file_reads | `oltp_index_scan` | 12.54ms | 7.56ms | 0.6× | 1.6% | PASS |
| file_reads | `select_random_points` | 18.78ms | 14.28ms | 0.8× | 2.1% | PASS |
| file_reads | `select_random_ranges` | 12.90ms | 7.11ms | 0.6× | 1.6% | PASS |
| file_reads | `covering_index_scan` | 16.16ms | 12.39ms | 0.8× | 1.3% | PASS |
| file_reads | `groupby_scan` | 32.38ms | 35.47ms | 1.1× | 0.8% | PASS |
| file_reads | `index_join` | 10.25ms | 9.80ms | 1.0× | 1.7% | PASS |
| file_reads | `index_join_scan` | 4.22ms | 5.80ms | 1.4× | 1.3% | PASS |
| file_reads | `types_table_scan` | 1.12s | 1.22s | 1.1× | 0.5% | PASS |
| file_reads | `table_scan` | 1.33s | 1.36s | 1.0× | 1.4% | PASS |
| file_reads | `oltp_read_only` | 223.73ms | 149.92ms | 0.7× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 195.44ms | 273.33ms | 1.4× | 1.3% | PASS |
| file_writes | `oltp_insert` | 21.96ms | 29.62ms | 1.3× | 1.5% | PASS |
| file_writes | `oltp_update_index` | 78.09ms | 102.38ms | 1.3× | 1.6% | PASS |
| file_writes | `oltp_update_non_index` | 58.51ms | 64.76ms | 1.1× | 1.8% | PASS |
| file_writes | `oltp_delete_insert` | 67.25ms | 81.28ms | 1.2× | 1.4% | PASS |
| file_writes | `oltp_write_only` | 43.65ms | 52.29ms | 1.2× | 1.8% | PASS |
| file_writes | `types_delete_insert` | 39.85ms | 41.97ms | 1.1× | 1.2% | PASS |
| file_writes | `oltp_read_write` | 88.43ms | 108.14ms | 1.2× | 1.3% | PASS |
| ac_reads | `oltp_point_select` | 51.54ms | 50.14ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_range_select` | 13.74ms | 13.86ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_sum_range` | 12.36ms | 13.78ms | 1.1× | 1.0% | PASS |
| ac_reads | `oltp_order_range` | 3.10ms | 3.22ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_distinct_range` | 4.07ms | 4.44ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 7.03ms | 7.55ms | 1.1× | 1.5% | PASS |
| ac_reads | `select_random_points` | 13.62ms | 14.11ms | 1.0× | 1.0% | PASS |
| ac_reads | `select_random_ranges` | 7.59ms | 7.13ms | 0.9× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 10.73ms | 12.38ms | 1.2× | 0.8% | PASS |
| ac_reads | `groupby_scan` | 31.88ms | 35.60ms | 1.1× | 1.2% | PASS |
| ac_reads | `index_join` | 7.52ms | 9.89ms | 1.3× | 1.7% | PASS |
| ac_reads | `index_join_scan` | 3.77ms | 5.86ms | 1.6× | 1.3% | PASS |
| ac_reads | `types_table_scan` | 1.13s | 1.22s | 1.1× | 0.7% | PASS |
| ac_reads | `table_scan` | 1.30s | 1.36s | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_read_only` | 144.57ms | 150.65ms | 1.0× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 16.26ms | 52.11ms | 3.2× | 7.4% | PASS |
| ac_writes | `oltp_insert_ac` | 18.47ms | 66.64ms | 3.6× | 6.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 20.57ms | 77.07ms | 3.7× | 6.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 16.74ms | 60.34ms | 3.6× | 7.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 18.98ms | 69.31ms | 3.7× | 6.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 18.75ms | 67.30ms | 3.6× | 6.4% | PASS |
| ac_writes | `types_delete_insert_ac` | 16.35ms | 58.84ms | 3.6× | 7.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 24.55ms | 73.57ms | 3.0× | 4.9% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 25.81ms | 24.35ms | 0.9× | 3.0% | PASS |
| mem_reads | `oltp_range_select` | 11.76ms | 9.84ms | 0.8× | 4.7% | PASS |
| mem_reads | `oltp_sum_range` | 11.27ms | 9.83ms | 0.9× | 3.6% | PASS |
| mem_reads | `oltp_order_range` | 2.32ms | 2.21ms | 1.0× | 2.4% | PASS |
| mem_reads | `oltp_distinct_range` | 2.88ms | 2.72ms | 0.9× | 2.0% | PASS |
| mem_reads | `oltp_index_scan` | 2.51ms | 4.45ms | 1.8× | 2.9% | PASS |
| mem_reads | `select_random_points` | 16.17ms | 14.57ms | 0.9× | 5.3% | PASS |
| mem_reads | `select_random_ranges` | 4.36ms | 4.02ms | 0.9× | 4.2% | PASS |
| mem_reads | `covering_index_scan` | 3.92ms | 6.13ms | 1.6× | 2.5% | PASS |
| mem_reads | `groupby_scan` | 20.32ms | 20.12ms | 1.0× | 1.6% | PASS |
| mem_reads | `index_join` | 7.58ms | 7.31ms | 1.0× | 2.3% | PASS |
| mem_reads | `index_join_scan` | 2.72ms | 5.53ms | 2.0× | 4.0% | PASS |
| mem_reads | `types_table_scan` | 749.39ms | 829.84ms | 1.1× | 2.0% | PASS |
| mem_reads | `table_scan` | 887.33ms | 911.36ms | 1.0× | 2.5% | PASS |
| mem_reads | `oltp_read_only` | 94.96ms | 82.07ms | 0.9× | 3.5% | PASS |
| mem_writes | `oltp_bulk_insert` | 142.02ms | 185.18ms | 1.3× | 1.6% | PASS |
| mem_writes | `oltp_insert` | 10.94ms | 22.59ms | 2.1× | 1.9% | PASS |
| mem_writes | `oltp_update_index` | 50.30ms | 92.39ms | 1.8× | 2.4% | PASS |
| mem_writes | `oltp_update_non_index` | 37.80ms | 52.11ms | 1.4× | 3.0% | PASS |
| mem_writes | `oltp_delete_insert` | 37.21ms | 63.49ms | 1.7× | 2.3% | PASS |
| mem_writes | `oltp_write_only` | 19.80ms | 36.50ms | 1.8× | 2.4% | PASS |
| mem_writes | `types_delete_insert` | 26.81ms | 33.37ms | 1.2× | 1.9% | PASS |
| mem_writes | `oltp_read_write` | 65.79ms | 80.64ms | 1.2× | 4.2% | PASS |
| file_reads | `oltp_point_select` | 76.84ms | 37.62ms | 0.5× | 2.6% | PASS |
| file_reads | `oltp_range_select` | 16.35ms | 10.61ms | 0.6× | 2.0% | PASS |
| file_reads | `oltp_sum_range` | 16.14ms | 10.73ms | 0.7× | 1.8% | PASS |
| file_reads | `oltp_order_range` | 2.95ms | 2.29ms | 0.8× | 2.2% | PASS |
| file_reads | `oltp_distinct_range` | 3.41ms | 2.79ms | 0.8× | 1.7% | PASS |
| file_reads | `oltp_index_scan` | 8.14ms | 5.82ms | 0.7× | 1.9% | PASS |
| file_reads | `select_random_points` | 22.06ms | 15.16ms | 0.7× | 3.3% | PASS |
| file_reads | `select_random_ranges` | 10.16ms | 5.50ms | 0.5× | 1.8% | PASS |
| file_reads | `covering_index_scan` | 9.76ms | 7.55ms | 0.8× | 1.9% | PASS |
| file_reads | `groupby_scan` | 20.76ms | 20.11ms | 1.0× | 1.5% | PASS |
| file_reads | `index_join` | 10.88ms | 7.52ms | 0.7× | 2.0% | PASS |
| file_reads | `index_join_scan` | 3.32ms | 4.90ms | 1.5× | 2.2% | PASS |
| file_reads | `types_table_scan` | 705.89ms | 802.10ms | 1.1× | 1.7% | PASS |
| file_reads | `table_scan` | 816.51ms | 893.60ms | 1.1× | 1.7% | PASS |
| file_reads | `oltp_read_only` | 167.56ms | 101.68ms | 0.6× | 2.4% | PASS |
| file_writes | `oltp_bulk_insert` | 211.62ms | 250.96ms | 1.2× | 4.1% | PASS |
| file_writes | `oltp_insert` | 20.14ms | 38.20ms | 1.9× | 2.7% | PASS |
| file_writes | `oltp_update_index` | 157.86ms | 156.91ms | 1.0× | 2.7% | PASS |
| file_writes | `oltp_update_non_index` | 132.68ms | 104.90ms | 0.8× | 4.4% | PASS |
| file_writes | `oltp_delete_insert` | 169.40ms | 126.38ms | 0.7× | 4.3% | PASS |
| file_writes | `oltp_write_only` | 119.28ms | 89.92ms | 0.8× | 9.4% | PASS |
| file_writes | `types_delete_insert` | 134.16ms | 74.76ms | 0.6× | 7.8% | PASS |
| file_writes | `oltp_read_write` | 144.96ms | 129.78ms | 0.9× | 4.2% | PASS |
| ac_reads | `oltp_point_select` | 40.55ms | 37.92ms | 0.9× | 1.4% | PASS |
| ac_reads | `oltp_range_select` | 12.22ms | 10.83ms | 0.9× | 1.5% | PASS |
| ac_reads | `oltp_sum_range` | 12.37ms | 10.89ms | 0.9× | 2.8% | PASS |
| ac_reads | `oltp_order_range` | 2.61ms | 2.33ms | 0.9× | 2.1% | PASS |
| ac_reads | `oltp_distinct_range` | 3.06ms | 2.86ms | 0.9× | 1.8% | PASS |
| ac_reads | `oltp_index_scan` | 4.76ms | 5.84ms | 1.2× | 2.5% | PASS |
| ac_reads | `select_random_points` | 16.89ms | 15.13ms | 0.9× | 2.7% | PASS |
| ac_reads | `select_random_ranges` | 6.52ms | 5.57ms | 0.9× | 2.9% | PASS |
| ac_reads | `covering_index_scan` | 6.42ms | 7.61ms | 1.2× | 1.9% | PASS |
| ac_reads | `groupby_scan` | 20.84ms | 20.52ms | 1.0× | 2.3% | PASS |
| ac_reads | `index_join` | 9.91ms | 7.79ms | 0.8× | 1.7% | PASS |
| ac_reads | `index_join_scan` | 3.12ms | 4.93ms | 1.6× | 2.1% | PASS |
| ac_reads | `types_table_scan` | 716.60ms | 814.50ms | 1.1× | 1.3% | PASS |
| ac_reads | `table_scan` | 852.93ms | 922.24ms | 1.1× | 4.7% | PASS |
| ac_reads | `oltp_read_only` | 107.11ms | 98.44ms | 0.9× | 4.4% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.12ms | 55.74ms | 2.5× | 7.5% | PASS |
| ac_writes | `oltp_insert_ac` | 23.76ms | 59.84ms | 2.5× | 5.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 23.49ms | 69.66ms | 3.0× | 5.3% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.09ms | 58.08ms | 2.8× | 7.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 23.27ms | 65.39ms | 2.8× | 4.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 23.07ms | 63.42ms | 2.7× | 5.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.50ms | 59.36ms | 2.6× | 6.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 27.88ms | 70.32ms | 2.5× | 6.1% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.35ms | 39.38ms | 1.2× | 2.6% | PASS |
| mem_reads | `oltp_range_select` | 15.85ms | 16.32ms | 1.0× | 3.0% | PASS |
| mem_reads | `oltp_sum_range` | 14.78ms | 15.09ms | 1.0× | 2.2% | PASS |
| mem_reads | `oltp_order_range` | 3.26ms | 3.39ms | 1.0× | 1.4% | PASS |
| mem_reads | `oltp_distinct_range` | 4.31ms | 4.51ms | 1.0× | 1.6% | PASS |
| mem_reads | `oltp_index_scan` | 3.69ms | 6.86ms | 1.9× | 1.4% | PASS |
| mem_reads | `select_random_points` | 19.29ms | 22.23ms | 1.2× | 1.9% | PASS |
| mem_reads | `select_random_ranges` | 6.40ms | 6.86ms | 1.1× | 1.8% | PASS |
| mem_reads | `covering_index_scan` | 7.69ms | 10.81ms | 1.4× | 0.9% | PASS |
| mem_reads | `groupby_scan` | 33.00ms | 35.70ms | 1.1× | 0.9% | PASS |
| mem_reads | `index_join` | 9.92ms | 10.54ms | 1.1× | 1.4% | PASS |
| mem_reads | `index_join_scan` | 3.75ms | 6.38ms | 1.7× | 2.8% | PASS |
| mem_reads | `types_table_scan` | 1.04s | 1.32s | 1.3× | 0.6% | PASS |
| mem_reads | `table_scan` | 1.17s | 1.41s | 1.2× | 0.3% | PASS |
| mem_reads | `oltp_read_only` | 128.43ms | 144.32ms | 1.1× | 0.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 236.34ms | 324.67ms | 1.4× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 18.07ms | 36.50ms | 2.0× | 0.5% | PASS |
| mem_writes | `oltp_update_index` | 59.80ms | 125.41ms | 2.1× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 46.30ms | 77.51ms | 1.7× | 2.2% | PASS |
| mem_writes | `oltp_delete_insert` | 49.76ms | 94.28ms | 1.9× | 0.9% | PASS |
| mem_writes | `oltp_write_only` | 26.39ms | 55.07ms | 2.1× | 1.3% | PASS |
| mem_writes | `types_delete_insert` | 36.46ms | 53.75ms | 1.5× | 2.0% | PASS |
| mem_writes | `oltp_read_write` | 95.10ms | 137.17ms | 1.4× | 1.9% | PASS |
| file_reads | `oltp_point_select` | 102.79ms | 58.12ms | 0.6× | 1.5% | PASS |
| file_reads | `oltp_range_select` | 22.52ms | 18.26ms | 0.8× | 1.9% | PASS |
| file_reads | `oltp_sum_range` | 21.87ms | 17.13ms | 0.8× | 1.9% | PASS |
| file_reads | `oltp_order_range` | 4.27ms | 3.69ms | 0.9× | 2.8% | PASS |
| file_reads | `oltp_distinct_range` | 5.41ms | 4.81ms | 0.9× | 2.0% | PASS |
| file_reads | `oltp_index_scan` | 10.96ms | 8.93ms | 0.8× | 2.0% | PASS |
| file_reads | `select_random_points` | 29.51ms | 25.31ms | 0.9× | 2.9% | PASS |
| file_reads | `select_random_ranges` | 13.43ms | 8.87ms | 0.7× | 2.2% | PASS |
| file_reads | `covering_index_scan` | 15.02ms | 12.74ms | 0.8× | 1.6% | PASS |
| file_reads | `groupby_scan` | 34.96ms | 36.14ms | 1.0× | 1.4% | PASS |
| file_reads | `index_join` | 15.05ms | 11.94ms | 0.8× | 3.1% | PASS |
| file_reads | `index_join_scan` | 4.74ms | 6.83ms | 1.4× | 2.7% | PASS |
| file_reads | `types_table_scan` | 1.09s | 1.36s | 1.2× | 1.3% | PASS |
| file_reads | `table_scan` | 1.29s | 1.45s | 1.1× | 1.7% | PASS |
| file_reads | `oltp_read_only` | 246.85ms | 176.43ms | 0.7× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 257.28ms | 339.19ms | 1.3× | 1.0% | PASS |
| file_writes | `oltp_insert` | 27.74ms | 42.15ms | 1.5× | 1.2% | PASS |
| file_writes | `oltp_update_index` | 102.65ms | 146.69ms | 1.4× | 1.2% | PASS |
| file_writes | `oltp_update_non_index` | 87.64ms | 91.18ms | 1.0× | 5.2% | PASS |
| file_writes | `oltp_delete_insert` | 90.21ms | 111.70ms | 1.2× | 1.8% | PASS |
| file_writes | `oltp_write_only` | 63.52ms | 68.52ms | 1.1× | 1.5% | PASS |
| file_writes | `types_delete_insert` | 65.48ms | 64.84ms | 1.0× | 1.9% | PASS |
| file_writes | `oltp_read_write` | 140.29ms | 152.71ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_point_select` | 58.48ms | 58.40ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 19.90ms | 18.52ms | 0.9× | 2.5% | PASS |
| ac_reads | `oltp_sum_range` | 18.12ms | 17.39ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_order_range` | 4.06ms | 3.77ms | 0.9× | 2.5% | PASS |
| ac_reads | `oltp_distinct_range` | 5.15ms | 4.94ms | 1.0× | 2.2% | PASS |
| ac_reads | `oltp_index_scan` | 6.71ms | 9.05ms | 1.3× | 1.3% | PASS |
| ac_reads | `select_random_points` | 25.86ms | 25.78ms | 1.0× | 2.8% | PASS |
| ac_reads | `select_random_ranges` | 9.07ms | 8.93ms | 1.0× | 1.9% | PASS |
| ac_reads | `covering_index_scan` | 10.32ms | 12.77ms | 1.2× | 1.9% | PASS |
| ac_reads | `groupby_scan` | 34.45ms | 36.21ms | 1.1× | 1.0% | PASS |
| ac_reads | `index_join` | 12.40ms | 11.90ms | 1.0× | 2.7% | PASS |
| ac_reads | `index_join_scan` | 4.30ms | 6.84ms | 1.6× | 2.3% | PASS |
| ac_reads | `types_table_scan` | 1.16s | 1.39s | 1.2× | 1.8% | PASS |
| ac_reads | `table_scan` | 1.32s | 1.46s | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_read_only` | 177.92ms | 176.39ms | 1.0× | 1.5% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.38ms | 59.51ms | 2.7× | 6.7% | PASS |
| ac_writes | `oltp_insert_ac` | 24.81ms | 76.93ms | 3.1× | 7.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.70ms | 88.06ms | 3.3× | 5.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.51ms | 68.93ms | 3.1× | 6.4% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.04ms | 79.54ms | 3.1× | 6.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 24.02ms | 77.34ms | 3.2× | 4.9% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.43ms | 72.08ms | 3.2× | 9.0% | PASS |
| ac_writes | `oltp_read_write_ac` | 30.41ms | 85.06ms | 2.8× | 5.2% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 17.73ms | 21.72ms | 1.2× | 1.3% | PASS |
| mem_reads | `oltp_range_select` | 9.47ms | 11.84ms | 1.3× | 1.9% | PASS |
| mem_reads | `oltp_sum_range` | 8.95ms | 11.88ms | 1.3× | 1.5% | PASS |
| mem_reads | `oltp_order_range` | 1.94ms | 2.31ms | 1.2× | 0.9% | PASS |
| mem_reads | `oltp_distinct_range` | 2.47ms | 2.85ms | 1.2× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 2.83ms | 3.76ms | 1.3× | 4.2% | PASS |
| mem_reads | `select_random_points` | 17.75ms | 19.28ms | 1.1× | 2.0% | PASS |
| mem_reads | `select_random_ranges` | 4.67ms | 5.29ms | 1.1× | 2.3% | PASS |
| mem_reads | `covering_index_scan` | 4.11ms | 5.54ms | 1.3× | 1.7% | PASS |
| mem_reads | `groupby_scan` | 18.72ms | 21.19ms | 1.1× | 1.2% | PASS |
| mem_reads | `index_join` | 4.44ms | 6.00ms | 1.4× | 1.4% | PASS |
| mem_reads | `index_join_scan` | 2.04ms | 4.08ms | 2.0× | 2.4% | PASS |
| mem_reads | `types_table_scan` | 673.58ms | 760.25ms | 1.1× | 0.8% | PASS |
| mem_reads | `table_scan` | 780.00ms | 859.70ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_read_only` | 75.75ms | 86.65ms | 1.1× | 2.7% | PASS |
| mem_writes | `oltp_bulk_insert` | 146.65ms | 184.33ms | 1.3× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 11.35ms | 19.05ms | 1.7× | 1.1% | PASS |
| mem_writes | `oltp_update_index` | 41.16ms | 71.24ms | 1.7× | 1.3% | PASS |
| mem_writes | `oltp_update_non_index` | 31.54ms | 47.57ms | 1.5× | 2.1% | PASS |
| mem_writes | `oltp_delete_insert` | 28.54ms | 50.84ms | 1.8× | 1.4% | PASS |
| mem_writes | `oltp_write_only` | 15.54ms | 30.77ms | 2.0× | 1.2% | PASS |
| mem_writes | `types_delete_insert` | 18.94ms | 29.73ms | 1.6× | 2.1% | PASS |
| mem_writes | `oltp_read_write` | 53.16ms | 81.50ms | 1.5× | 1.1% | PASS |
| file_reads | `oltp_point_select` | 76.80ms | 39.33ms | 0.5× | 1.1% | PASS |
| file_reads | `oltp_range_select` | 17.32ms | 13.97ms | 0.8× | 2.0% | PASS |
| file_reads | `oltp_sum_range` | 16.40ms | 13.86ms | 0.8× | 1.6% | PASS |
| file_reads | `oltp_order_range` | 3.06ms | 2.65ms | 0.9× | 1.1% | PASS |
| file_reads | `oltp_distinct_range` | 3.51ms | 3.14ms | 0.9× | 0.7% | PASS |
| file_reads | `oltp_index_scan` | 9.25ms | 6.03ms | 0.7× | 2.6% | PASS |
| file_reads | `select_random_points` | 24.30ms | 21.92ms | 0.9× | 1.5% | PASS |
| file_reads | `select_random_ranges` | 10.47ms | 6.91ms | 0.7× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 10.30ms | 7.56ms | 0.7× | 1.7% | PASS |
| file_reads | `groupby_scan` | 21.40ms | 22.72ms | 1.1× | 1.9% | PASS |
| file_reads | `index_join` | 7.73ms | 7.77ms | 1.0× | 2.6% | PASS |
| file_reads | `index_join_scan` | 3.25ms | 4.62ms | 1.4× | 3.1% | PASS |
| file_reads | `types_table_scan` | 720.56ms | 815.85ms | 1.1× | 0.9% | PASS |
| file_reads | `table_scan` | 816.15ms | 899.51ms | 1.1× | 0.7% | PASS |
| file_reads | `oltp_read_only` | 159.66ms | 113.66ms | 0.7× | 0.7% | PASS |
| file_writes | `oltp_bulk_insert` | 199.93ms | 234.72ms | 1.2× | 2.8% | PASS |
| file_writes | `oltp_insert` | 19.54ms | 30.15ms | 1.5× | 4.7% | PASS |
| file_writes | `oltp_update_index` | 141.06ms | 128.80ms | 0.9× | 3.8% | PASS |
| file_writes | `oltp_update_non_index` | 120.69ms | 91.55ms | 0.8× | 0.5% | PASS |
| file_writes | `oltp_delete_insert` | 120.67ms | 105.06ms | 0.9× | 3.9% | PASS |
| file_writes | `oltp_write_only` | 82.88ms | 78.31ms | 0.9× | 4.2% | PASS |
| file_writes | `types_delete_insert` | 78.28ms | 67.76ms | 0.9× | 9.2% | PASS |
| file_writes | `oltp_read_write` | 131.59ms | 129.95ms | 1.0× | 6.0% | PASS |
| ac_reads | `oltp_point_select` | 36.01ms | 36.42ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_range_select` | 12.87ms | 13.16ms | 1.0× | 4.0% | PASS |
| ac_reads | `oltp_sum_range` | 12.13ms | 13.04ms | 1.1× | 3.7% | PASS |
| ac_reads | `oltp_order_range` | 2.61ms | 2.54ms | 1.0× | 4.3% | PASS |
| ac_reads | `oltp_distinct_range` | 3.01ms | 3.11ms | 1.0× | 1.8% | PASS |
| ac_reads | `oltp_index_scan` | 4.80ms | 5.50ms | 1.1× | 1.4% | PASS |
| ac_reads | `select_random_points` | 17.47ms | 19.97ms | 1.1× | 1.8% | PASS |
| ac_reads | `select_random_ranges` | 5.96ms | 6.34ms | 1.1× | 0.7% | PASS |
| ac_reads | `covering_index_scan` | 5.64ms | 6.89ms | 1.2× | 1.3% | PASS |
| ac_reads | `groupby_scan` | 19.59ms | 21.47ms | 1.1× | 1.1% | PASS |
| ac_reads | `index_join` | 5.74ms | 7.63ms | 1.3× | 2.5% | PASS |
| ac_reads | `index_join_scan` | 2.91ms | 4.62ms | 1.6× | 2.4% | PASS |
| ac_reads | `types_table_scan` | 728.51ms | 820.04ms | 1.1× | 1.3% | PASS |
| ac_reads | `table_scan` | 841.61ms | 916.75ms | 1.1× | 2.3% | PASS |
| ac_reads | `oltp_read_only` | 113.70ms | 117.13ms | 1.0× | 2.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.74ms | 50.74ms | 2.3× | 4.1% | PASS |
| ac_writes | `oltp_insert_ac` | 25.77ms | 60.77ms | 2.4× | 4.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.81ms | 70.03ms | 2.5× | 5.5% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.51ms | 55.06ms | 2.6× | 3.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.68ms | 61.01ms | 2.5× | 3.9% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.37ms | 66.56ms | 2.6× | 7.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 25.73ms | 61.45ms | 2.4× | 12.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 29.36ms | 77.21ms | 2.6× | 7.9% | PASS |

</details>

</details>

## Version-control latency

Wall time: 3m 24s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 35.21ms | 130.00ms | 27.1% | 0.9% | PASS |
| `status_dirty_many_tables` | 38.58ms | 130.00ms | 29.7% | 1.0% | PASS |
| `diff_regular_working_one_table` | 30.23ms | 120.00ms | 25.2% | 1.1% | PASS |
| `diff_regular_working_many_tables` | 43.71ms | 140.00ms | 31.2% | 0.8% | PASS |
| `diff_stat_working_many_tables` | 43.60ms | 140.00ms | 31.1% | 0.8% | PASS |
| `diff_schema_working_many_tables` | 43.92ms | 140.00ms | 31.4% | 0.8% | PASS |
| `branch_list_many_branches` | 22.70ms | 35.00ms | 64.8% | 0.9% | PASS |
| `branch_create_delete` | 25.19ms | 40.00ms | 63.0% | 1.1% | PASS |
| `at_literal_deep_history` | 25.46ms | 100.00ms | 25.5% | 1.1% | PASS |
| `diff_literal_deep_history` | 25.78ms | 120.00ms | 21.5% | 1.2% | PASS |
| `history_literal_deep_history` | 27.55ms | 150.00ms | 18.4% | 1.0% | PASS |
| `checkout_branch_clean` | 39.02ms | 150.00ms | 26.0% | 0.8% | PASS |
| `merge_data_no_conflicts` | 30.24ms | 50.00ms | 60.5% | 0.8% | PASS |
| `merge_data_secondary_index` | 851.08ms | 2.50s | 34.0% | 0.8% | PASS |
| `merge_schema_no_conflicts` | 22.01ms | 35.00ms | 62.9% | 1.0% | PASS |
| `merge_data_conflicts` | 30.80ms | 180.00ms | 17.1% | 0.7% | PASS |
| `merge_data_conflicts_with_resolve` | 31.54ms | 180.00ms | 17.5% | 1.1% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
