# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-09-30 11:12 UTC
>
> Commit: [`ca03c4a3f9c32e5e6ed4e0e742e218b276c83a2a`](https://github.com/dolthub/doltlite/commit/ca03c4a3f9c32e5e6ed4e0e742e218b276c83a2a)
>
> Runner: ubuntu24 20260920.314.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/36697479876)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.11s | 11.54s | 1.1× | 1.5% | **PASS** |
| Writes | 2.08s | 3.22s | 1.5× | 1.4% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.90s | 11.73s | 1.1× | 1.5% | **PASS** |
| Writes | 3.31s | 3.88s | 1.2× | 2.2% | **PASS** |
| Autocommit writes | 821.94ms | 2.50s | 3.0× | 8.1% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.57s | 2.93s | 1.1× | 1.7% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.83s | 3.11s | 1.1× | 1.7% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.52s | 3.01s | 1.2× | 1.2% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.18s | 2.49s | 1.1× | 1.5% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 440.83ms | 716.03ms | 1.6× | 1.5% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 593.76ms | 933.30ms | 1.6× | 1.2% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 581.72ms | 914.39ms | 1.6× | 1.2% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 465.05ms | 660.31ms | 1.4× | 1.4% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.76s | 2.98s | 1.1× | 1.6% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.85s | 3.10s | 1.1× | 1.4% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.77s | 3.07s | 1.1× | 2.2% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.52s | 2.58s | 1.0× | 1.2% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 591.45ms | 779.65ms | 1.3× | 2.1% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 900.74ms | 1.04s | 1.2× | 4.3% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 813.51ms | 1.02s | 1.3× | 1.7% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 1.01s | 1.04s | 1.0× | 8.9% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.57s | 2.97s | 1.2× | 1.6% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.71s | 3.10s | 1.1× | 1.6% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.61s | 3.07s | 1.2× | 1.5% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.42s | 2.58s | 1.1× | 1.2% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 195.50ms | 615.01ms | 3.1× | 7.8% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 226.03ms | 673.24ms | 3.0× | 8.7% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 203.97ms | 611.44ms | 3.0× | 4.9% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 196.44ms | 603.16ms | 3.1× | 33.0% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 25.95ms | 31.92ms | 1.2× | 1.7% | PASS |
| mem_reads | `oltp_range_select` | 10.97ms | 12.08ms | 1.1× | 2.6% | PASS |
| mem_reads | `oltp_sum_range` | 9.26ms | 12.44ms | 1.3× | 1.9% | PASS |
| mem_reads | `oltp_order_range` | 2.59ms | 2.92ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 3.66ms | 4.07ms | 1.1× | 0.6% | PASS |
| mem_reads | `oltp_index_scan` | 3.91ms | 5.48ms | 1.4× | 1.4% | PASS |
| mem_reads | `select_random_points` | 10.49ms | 11.82ms | 1.1× | 4.5% | PASS |
| mem_reads | `select_random_ranges` | 4.68ms | 5.30ms | 1.1× | 1.8% | PASS |
| mem_reads | `covering_index_scan` | 7.70ms | 10.62ms | 1.4× | 1.0% | PASS |
| mem_reads | `groupby_scan` | 29.49ms | 33.66ms | 1.1× | 1.2% | PASS |
| mem_reads | `index_join` | 5.75ms | 8.65ms | 1.5× | 1.2% | PASS |
| mem_reads | `index_join_scan` | 3.24ms | 5.68ms | 1.8× | 2.9% | PASS |
| mem_reads | `types_table_scan` | 1.09s | 1.29s | 1.2× | 1.7% | PASS |
| mem_reads | `table_scan` | 1.26s | 1.37s | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_read_only` | 107.85ms | 124.64ms | 1.2× | 2.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 179.52ms | 274.50ms | 1.5× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 15.37ms | 27.21ms | 1.8× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 51.86ms | 95.91ms | 1.8× | 1.5% | PASS |
| mem_writes | `oltp_update_non_index` | 34.92ms | 57.75ms | 1.7× | 1.7% | PASS |
| mem_writes | `oltp_delete_insert` | 45.36ms | 72.66ms | 1.6× | 1.7% | PASS |
| mem_writes | `oltp_write_only` | 22.38ms | 44.67ms | 2.0× | 1.6% | PASS |
| mem_writes | `types_delete_insert` | 24.41ms | 37.07ms | 1.5× | 1.7% | PASS |
| mem_writes | `oltp_read_write` | 67.00ms | 106.27ms | 1.6× | 1.5% | PASS |
| file_reads | `oltp_point_select` | 93.62ms | 50.42ms | 0.5× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 18.10ms | 13.97ms | 0.8× | 2.0% | PASS |
| file_reads | `oltp_sum_range` | 17.00ms | 14.73ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_order_range` | 3.48ms | 3.20ms | 0.9× | 2.1% | PASS |
| file_reads | `oltp_distinct_range` | 4.50ms | 4.36ms | 1.0× | 1.7% | PASS |
| file_reads | `oltp_index_scan` | 11.21ms | 7.66ms | 0.7× | 1.5% | PASS |
| file_reads | `select_random_points` | 17.82ms | 14.07ms | 0.8× | 4.3% | PASS |
| file_reads | `select_random_ranges` | 11.82ms | 7.42ms | 0.6× | 0.9% | PASS |
| file_reads | `covering_index_scan` | 15.09ms | 12.71ms | 0.8× | 1.2% | PASS |
| file_reads | `groupby_scan` | 29.98ms | 33.94ms | 1.1× | 1.2% | PASS |
| file_reads | `index_join` | 9.94ms | 10.12ms | 1.0× | 2.3% | PASS |
| file_reads | `index_join_scan` | 4.19ms | 5.79ms | 1.4× | 2.3% | PASS |
| file_reads | `types_table_scan` | 1.07s | 1.28s | 1.2× | 1.6% | PASS |
| file_reads | `table_scan` | 1.24s | 1.36s | 1.1× | 1.6% | PASS |
| file_reads | `oltp_read_only` | 207.78ms | 153.20ms | 0.7× | 0.7% | PASS |
| file_writes | `oltp_bulk_insert` | 193.53ms | 284.35ms | 1.5× | 0.9% | PASS |
| file_writes | `oltp_insert` | 21.89ms | 30.22ms | 1.4× | 2.2% | PASS |
| file_writes | `oltp_update_index` | 77.25ms | 103.60ms | 1.3× | 1.5% | PASS |
| file_writes | `oltp_update_non_index` | 58.76ms | 67.81ms | 1.2× | 2.1% | PASS |
| file_writes | `oltp_delete_insert` | 66.36ms | 83.13ms | 1.3× | 2.2% | PASS |
| file_writes | `oltp_write_only` | 43.11ms | 52.50ms | 1.2× | 2.2% | PASS |
| file_writes | `types_delete_insert` | 39.82ms | 43.39ms | 1.1× | 2.3% | PASS |
| file_writes | `oltp_read_write` | 90.74ms | 114.67ms | 1.3× | 1.6% | PASS |
| ac_reads | `oltp_point_select` | 46.89ms | 50.48ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 13.10ms | 13.88ms | 1.1× | 1.8% | PASS |
| ac_reads | `oltp_sum_range` | 12.09ms | 14.68ms | 1.2× | 2.0% | PASS |
| ac_reads | `oltp_order_range` | 2.99ms | 3.19ms | 1.1× | 1.6% | PASS |
| ac_reads | `oltp_distinct_range` | 4.03ms | 4.38ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 6.55ms | 7.61ms | 1.2× | 2.4% | PASS |
| ac_reads | `select_random_points` | 12.78ms | 13.97ms | 1.1× | 2.8% | PASS |
| ac_reads | `select_random_ranges` | 7.08ms | 7.40ms | 1.0× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 10.45ms | 12.68ms | 1.2× | 1.2% | PASS |
| ac_reads | `groupby_scan` | 29.35ms | 33.87ms | 1.2× | 0.6% | PASS |
| ac_reads | `index_join` | 7.40ms | 9.95ms | 1.3× | 1.8% | PASS |
| ac_reads | `index_join_scan` | 3.57ms | 5.82ms | 1.6× | 2.4% | PASS |
| ac_reads | `types_table_scan` | 1.05s | 1.28s | 1.2× | 1.0% | PASS |
| ac_reads | `table_scan` | 1.23s | 1.36s | 1.1× | 2.1% | PASS |
| ac_reads | `oltp_read_only` | 139.24ms | 153.48ms | 1.1× | 1.4% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.85ms | 62.07ms | 2.8× | 7.6% | PASS |
| ac_writes | `oltp_insert_ac` | 24.17ms | 75.86ms | 3.1× | 6.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 25.90ms | 88.54ms | 3.4× | 6.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.80ms | 69.18ms | 3.2× | 8.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 23.75ms | 79.61ms | 3.4× | 8.2% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.15ms | 80.57ms | 3.1× | 9.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 21.95ms | 73.46ms | 3.3× | 9.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 29.93ms | 85.73ms | 2.9× | 7.2% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 38.95ms | 41.66ms | 1.1× | 1.7% | PASS |
| mem_reads | `oltp_range_select` | 18.08ms | 16.25ms | 0.9× | 2.1% | PASS |
| mem_reads | `oltp_sum_range` | 16.32ms | 15.87ms | 1.0× | 2.3% | PASS |
| mem_reads | `oltp_order_range` | 3.31ms | 3.44ms | 1.0× | 1.6% | PASS |
| mem_reads | `oltp_distinct_range` | 4.37ms | 4.59ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 4.00ms | 6.75ms | 1.7× | 1.2% | PASS |
| mem_reads | `select_random_points` | 23.84ms | 23.42ms | 1.0× | 2.9% | PASS |
| mem_reads | `select_random_ranges` | 6.99ms | 7.00ms | 1.0× | 1.0% | PASS |
| mem_reads | `covering_index_scan` | 7.78ms | 10.85ms | 1.4× | 1.1% | PASS |
| mem_reads | `groupby_scan` | 34.59ms | 36.53ms | 1.1× | 0.9% | PASS |
| mem_reads | `index_join` | 11.01ms | 10.34ms | 0.9× | 1.5% | PASS |
| mem_reads | `index_join_scan` | 3.92ms | 6.49ms | 1.7× | 2.2% | PASS |
| mem_reads | `types_table_scan` | 1.20s | 1.33s | 1.1× | 4.8% | PASS |
| mem_reads | `table_scan` | 1.32s | 1.45s | 1.1× | 4.1% | PASS |
| mem_reads | `oltp_read_only` | 144.12ms | 151.11ms | 1.0× | 2.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 239.76ms | 331.69ms | 1.4× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 17.88ms | 37.31ms | 2.1× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 66.09ms | 133.09ms | 2.0× | 1.3% | PASS |
| mem_writes | `oltp_update_non_index` | 49.20ms | 81.94ms | 1.7× | 1.4% | PASS |
| mem_writes | `oltp_delete_insert` | 54.22ms | 98.24ms | 1.8× | 0.9% | PASS |
| mem_writes | `oltp_write_only` | 27.80ms | 55.80ms | 2.0× | 1.4% | PASS |
| mem_writes | `types_delete_insert` | 38.71ms | 54.96ms | 1.4× | 1.2% | PASS |
| mem_writes | `oltp_read_write` | 100.10ms | 140.27ms | 1.4× | 2.5% | PASS |
| file_reads | `oltp_point_select` | 106.11ms | 59.06ms | 0.6× | 1.1% | PASS |
| file_reads | `oltp_range_select` | 24.13ms | 17.92ms | 0.7× | 1.8% | PASS |
| file_reads | `oltp_sum_range` | 23.30ms | 17.80ms | 0.8× | 1.4% | PASS |
| file_reads | `oltp_order_range` | 4.15ms | 3.67ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_distinct_range` | 5.21ms | 4.85ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 11.30ms | 8.69ms | 0.8× | 1.4% | PASS |
| file_reads | `select_random_points` | 31.31ms | 26.39ms | 0.8× | 2.6% | PASS |
| file_reads | `select_random_ranges` | 14.18ms | 8.98ms | 0.6× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 15.02ms | 12.91ms | 0.9× | 1.5% | PASS |
| file_reads | `groupby_scan` | 34.93ms | 36.62ms | 1.0× | 1.1% | PASS |
| file_reads | `index_join` | 14.79ms | 11.49ms | 0.8× | 2.2% | PASS |
| file_reads | `index_join_scan` | 4.76ms | 6.78ms | 1.4× | 1.9% | PASS |
| file_reads | `types_table_scan` | 1.08s | 1.29s | 1.2× | 0.7% | PASS |
| file_reads | `table_scan` | 1.24s | 1.42s | 1.1× | 0.8% | PASS |
| file_reads | `oltp_read_only` | 241.91ms | 176.44ms | 0.7× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 263.93ms | 346.20ms | 1.3× | 0.9% | PASS |
| file_writes | `oltp_insert` | 25.42ms | 42.57ms | 1.7× | 1.2% | PASS |
| file_writes | `oltp_update_index` | 112.77ms | 150.76ms | 1.3× | 6.7% | PASS |
| file_writes | `oltp_update_non_index` | 96.81ms | 95.54ms | 1.0× | 9.1% | PASS |
| file_writes | `oltp_delete_insert` | 96.12ms | 115.06ms | 1.2× | 1.8% | PASS |
| file_writes | `oltp_write_only` | 86.89ms | 68.65ms | 0.8× | 13.8% | PASS |
| file_writes | `types_delete_insert` | 71.06ms | 67.06ms | 0.9× | 2.0% | PASS |
| file_writes | `oltp_read_write` | 147.74ms | 151.08ms | 1.0× | 6.5% | PASS |
| ac_reads | `oltp_point_select` | 59.36ms | 58.88ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 20.03ms | 18.03ms | 0.9× | 1.6% | PASS |
| ac_reads | `oltp_sum_range` | 18.38ms | 17.73ms | 1.0× | 2.0% | PASS |
| ac_reads | `oltp_order_range` | 3.76ms | 3.68ms | 1.0× | 2.3% | PASS |
| ac_reads | `oltp_distinct_range` | 4.91ms | 4.93ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_index_scan` | 6.53ms | 8.71ms | 1.3× | 1.3% | PASS |
| ac_reads | `select_random_points` | 26.34ms | 26.02ms | 1.0× | 2.2% | PASS |
| ac_reads | `select_random_ranges` | 9.37ms | 8.98ms | 1.0× | 1.6% | PASS |
| ac_reads | `covering_index_scan` | 10.40ms | 12.95ms | 1.2× | 1.2% | PASS |
| ac_reads | `groupby_scan` | 34.48ms | 36.63ms | 1.1× | 1.0% | PASS |
| ac_reads | `index_join` | 12.64ms | 11.49ms | 0.9× | 2.0% | PASS |
| ac_reads | `index_join_scan` | 4.27ms | 6.86ms | 1.6× | 1.7% | PASS |
| ac_reads | `types_table_scan` | 1.08s | 1.29s | 1.2× | 0.8% | PASS |
| ac_reads | `table_scan` | 1.25s | 1.42s | 1.1× | 0.8% | PASS |
| ac_reads | `oltp_read_only` | 174.35ms | 176.89ms | 1.0× | 1.7% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 25.92ms | 67.17ms | 2.6× | 7.2% | PASS |
| ac_writes | `oltp_insert_ac` | 29.63ms | 84.45ms | 2.9× | 10.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 29.60ms | 98.03ms | 3.3× | 7.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 25.52ms | 79.03ms | 3.1× | 9.4% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 27.77ms | 89.33ms | 3.2× | 9.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 27.36ms | 85.77ms | 3.1× | 7.4% | PASS |
| ac_writes | `types_delete_insert_ac` | 24.85ms | 76.74ms | 3.1× | 8.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 35.38ms | 92.74ms | 2.6× | 8.5% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.21ms | 39.69ms | 1.2× | 1.0% | PASS |
| mem_reads | `oltp_range_select` | 15.36ms | 15.70ms | 1.0× | 1.9% | PASS |
| mem_reads | `oltp_sum_range` | 13.97ms | 15.20ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_order_range` | 3.06ms | 3.41ms | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_distinct_range` | 4.13ms | 4.56ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 3.82ms | 6.78ms | 1.8× | 0.9% | PASS |
| mem_reads | `select_random_points` | 20.13ms | 22.59ms | 1.1× | 1.5% | PASS |
| mem_reads | `select_random_ranges` | 6.03ms | 6.88ms | 1.1× | 1.9% | PASS |
| mem_reads | `covering_index_scan` | 7.78ms | 10.82ms | 1.4× | 0.8% | PASS |
| mem_reads | `groupby_scan` | 33.03ms | 36.00ms | 1.1× | 0.9% | PASS |
| mem_reads | `index_join` | 9.73ms | 10.39ms | 1.1× | 1.6% | PASS |
| mem_reads | `index_join_scan` | 3.47ms | 6.29ms | 1.8× | 1.7% | PASS |
| mem_reads | `types_table_scan` | 1.06s | 1.28s | 1.2× | 0.4% | PASS |
| mem_reads | `table_scan` | 1.18s | 1.41s | 1.2× | 0.3% | PASS |
| mem_reads | `oltp_read_only` | 130.96ms | 145.03ms | 1.1× | 0.7% | PASS |
| mem_writes | `oltp_bulk_insert` | 243.15ms | 333.11ms | 1.4× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 18.48ms | 37.00ms | 2.0× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 62.28ms | 126.10ms | 2.0× | 1.2% | PASS |
| mem_writes | `oltp_update_non_index` | 49.29ms | 79.17ms | 1.6× | 2.2% | PASS |
| mem_writes | `oltp_delete_insert` | 51.35ms | 95.25ms | 1.9× | 0.8% | PASS |
| mem_writes | `oltp_write_only` | 26.86ms | 55.21ms | 2.1× | 1.2% | PASS |
| mem_writes | `types_delete_insert` | 37.58ms | 53.53ms | 1.4× | 1.3% | PASS |
| mem_writes | `oltp_read_write` | 92.75ms | 135.02ms | 1.5× | 1.1% | PASS |
| file_reads | `oltp_point_select` | 103.79ms | 58.38ms | 0.6× | 1.4% | PASS |
| file_reads | `oltp_range_select` | 23.07ms | 17.76ms | 0.8× | 2.5% | PASS |
| file_reads | `oltp_sum_range` | 21.81ms | 17.45ms | 0.8× | 2.6% | PASS |
| file_reads | `oltp_order_range` | 4.34ms | 3.73ms | 0.9× | 4.0% | PASS |
| file_reads | `oltp_distinct_range` | 5.42ms | 4.88ms | 0.9× | 2.9% | PASS |
| file_reads | `oltp_index_scan` | 11.10ms | 9.00ms | 0.8× | 2.2% | PASS |
| file_reads | `select_random_points` | 30.14ms | 25.77ms | 0.9× | 3.1% | PASS |
| file_reads | `select_random_ranges` | 13.64ms | 8.95ms | 0.7× | 2.2% | PASS |
| file_reads | `covering_index_scan` | 15.10ms | 13.03ms | 0.9× | 1.7% | PASS |
| file_reads | `groupby_scan` | 34.94ms | 36.40ms | 1.0× | 1.3% | PASS |
| file_reads | `index_join` | 14.14ms | 11.69ms | 0.8× | 1.7% | PASS |
| file_reads | `index_join_scan` | 4.66ms | 6.66ms | 1.4× | 2.8% | PASS |
| file_reads | `types_table_scan` | 1.06s | 1.28s | 1.2× | 0.4% | PASS |
| file_reads | `table_scan` | 1.18s | 1.40s | 1.2× | 0.3% | PASS |
| file_reads | `oltp_read_only` | 241.00ms | 173.46ms | 0.7× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 264.76ms | 347.92ms | 1.3× | 0.9% | PASS |
| file_writes | `oltp_insert` | 25.48ms | 42.42ms | 1.7× | 1.1% | PASS |
| file_writes | `oltp_update_index` | 94.82ms | 145.22ms | 1.5× | 1.8% | PASS |
| file_writes | `oltp_update_non_index` | 98.66ms | 91.89ms | 0.9× | 8.2% | PASS |
| file_writes | `oltp_delete_insert` | 85.66ms | 111.10ms | 1.3× | 1.8% | PASS |
| file_writes | `oltp_write_only` | 56.52ms | 68.70ms | 1.2× | 3.0% | PASS |
| file_writes | `types_delete_insert` | 62.82ms | 64.42ms | 1.0× | 1.6% | PASS |
| file_writes | `oltp_read_write` | 124.80ms | 147.97ms | 1.2× | 1.6% | PASS |
| ac_reads | `oltp_point_select` | 56.51ms | 58.84ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_range_select` | 18.16ms | 17.76ms | 1.0× | 1.8% | PASS |
| ac_reads | `oltp_sum_range` | 16.75ms | 17.53ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_order_range` | 3.68ms | 3.68ms | 1.0× | 2.2% | PASS |
| ac_reads | `oltp_distinct_range` | 4.68ms | 4.84ms | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_index_scan` | 6.22ms | 8.89ms | 1.4× | 1.5% | PASS |
| ac_reads | `select_random_points` | 23.47ms | 25.83ms | 1.1× | 1.7% | PASS |
| ac_reads | `select_random_ranges` | 8.60ms | 8.98ms | 1.0× | 1.5% | PASS |
| ac_reads | `covering_index_scan` | 10.16ms | 13.04ms | 1.3× | 1.1% | PASS |
| ac_reads | `groupby_scan` | 33.47ms | 36.42ms | 1.1× | 0.9% | PASS |
| ac_reads | `index_join` | 11.42ms | 11.64ms | 1.0× | 1.9% | PASS |
| ac_reads | `index_join_scan` | 4.09ms | 6.71ms | 1.6× | 1.9% | PASS |
| ac_reads | `types_table_scan` | 1.06s | 1.28s | 1.2× | 0.4% | PASS |
| ac_reads | `table_scan` | 1.18s | 1.41s | 1.2× | 0.4% | PASS |
| ac_reads | `oltp_read_only` | 169.77ms | 173.81ms | 1.0× | 0.6% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 23.00ms | 60.30ms | 2.6× | 5.8% | PASS |
| ac_writes | `oltp_insert_ac` | 25.78ms | 77.68ms | 3.0× | 7.2% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.57ms | 89.13ms | 3.2× | 6.3% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.16ms | 70.16ms | 3.2× | 4.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.05ms | 79.52ms | 3.1× | 4.2% | PASS |
| ac_writes | `oltp_write_only_ac` | 24.75ms | 79.20ms | 3.2× | 3.8% | PASS |
| ac_writes | `types_delete_insert_ac` | 24.03ms | 70.92ms | 3.0× | 5.6% | PASS |
| ac_writes | `oltp_read_write_ac` | 30.64ms | 84.53ms | 2.8× | 2.4% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 27.12ms | 30.58ms | 1.1× | 2.4% | PASS |
| mem_reads | `oltp_range_select` | 14.92ms | 18.01ms | 1.2× | 1.2% | PASS |
| mem_reads | `oltp_sum_range` | 13.18ms | 17.70ms | 1.3× | 1.5% | PASS |
| mem_reads | `oltp_order_range` | 2.92ms | 3.26ms | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_distinct_range` | 3.67ms | 4.21ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_index_scan` | 3.62ms | 4.72ms | 1.3× | 2.2% | PASS |
| mem_reads | `select_random_points` | 21.83ms | 24.60ms | 1.1× | 1.5% | PASS |
| mem_reads | `select_random_ranges` | 5.97ms | 6.87ms | 1.2× | 2.3% | PASS |
| mem_reads | `covering_index_scan` | 5.86ms | 7.76ms | 1.3× | 1.4% | PASS |
| mem_reads | `groupby_scan` | 29.02ms | 34.36ms | 1.2× | 0.8% | PASS |
| mem_reads | `index_join` | 6.20ms | 8.67ms | 1.4× | 2.8% | PASS |
| mem_reads | `index_join_scan` | 3.33ms | 5.18ms | 1.6× | 2.7% | PASS |
| mem_reads | `types_table_scan` | 883.75ms | 1.04s | 1.2× | 0.9% | PASS |
| mem_reads | `table_scan` | 1.04s | 1.14s | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_read_only` | 117.77ms | 138.51ms | 1.2× | 1.5% | PASS |
| mem_writes | `oltp_bulk_insert` | 190.31ms | 232.24ms | 1.2× | 1.2% | PASS |
| mem_writes | `oltp_insert` | 14.90ms | 24.43ms | 1.6× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 53.63ms | 88.77ms | 1.7× | 1.5% | PASS |
| mem_writes | `oltp_update_non_index` | 42.20ms | 59.22ms | 1.4× | 1.4% | PASS |
| mem_writes | `oltp_delete_insert` | 40.24ms | 67.31ms | 1.7× | 1.4% | PASS |
| mem_writes | `oltp_write_only` | 21.73ms | 39.32ms | 1.8× | 1.5% | PASS |
| mem_writes | `types_delete_insert` | 26.25ms | 37.31ms | 1.4× | 1.8% | PASS |
| mem_writes | `oltp_read_write` | 75.79ms | 111.70ms | 1.5× | 1.4% | PASS |
| file_reads | `oltp_point_select` | 92.69ms | 47.45ms | 0.5× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 22.33ms | 19.76ms | 0.9× | 1.5% | PASS |
| file_reads | `oltp_sum_range` | 20.30ms | 19.44ms | 1.0× | 1.5% | PASS |
| file_reads | `oltp_order_range` | 3.69ms | 3.50ms | 0.9× | 0.9% | PASS |
| file_reads | `oltp_distinct_range` | 4.45ms | 4.45ms | 1.0× | 1.5% | PASS |
| file_reads | `oltp_index_scan` | 10.48ms | 6.72ms | 0.6× | 0.8% | PASS |
| file_reads | `select_random_points` | 28.72ms | 26.28ms | 0.9× | 1.2% | PASS |
| file_reads | `select_random_ranges` | 12.65ms | 8.64ms | 0.7× | 0.9% | PASS |
| file_reads | `covering_index_scan` | 12.86ms | 9.92ms | 0.8× | 1.2% | PASS |
| file_reads | `groupby_scan` | 29.93ms | 34.70ms | 1.2× | 0.7% | PASS |
| file_reads | `index_join` | 10.06ms | 9.99ms | 1.0× | 1.3% | PASS |
| file_reads | `index_join_scan` | 3.96ms | 5.36ms | 1.4× | 0.8% | PASS |
| file_reads | `types_table_scan` | 929.26ms | 1.05s | 1.1× | 2.3% | PASS |
| file_reads | `table_scan` | 1.11s | 1.16s | 1.0× | 5.0% | PASS |
| file_reads | `oltp_read_only` | 219.36ms | 166.64ms | 0.8× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 241.26ms | 291.80ms | 1.2× | 7.4% | PASS |
| file_writes | `oltp_insert` | 29.33ms | 36.11ms | 1.2× | 19.3% | PASS |
| file_writes | `oltp_update_index` | 152.09ms | 169.18ms | 1.1× | 11.9% | PASS |
| file_writes | `oltp_update_non_index` | 132.25ms | 102.83ms | 0.8× | 12.3% | PASS |
| file_writes | `oltp_delete_insert` | 130.87ms | 121.06ms | 0.9× | 3.5% | PASS |
| file_writes | `oltp_write_only` | 90.76ms | 81.69ms | 0.9× | 2.4% | PASS |
| file_writes | `types_delete_insert` | 80.36ms | 78.97ms | 1.0× | 10.3% | PASS |
| file_writes | `oltp_read_write` | 150.62ms | 159.63ms | 1.1× | 6.7% | PASS |
| ac_reads | `oltp_point_select` | 48.96ms | 47.67ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 18.28ms | 19.84ms | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_sum_range` | 16.02ms | 19.59ms | 1.2× | 1.3% | PASS |
| ac_reads | `oltp_order_range` | 3.29ms | 3.50ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_distinct_range` | 4.04ms | 4.45ms | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_index_scan` | 6.20ms | 6.70ms | 1.1× | 0.7% | PASS |
| ac_reads | `select_random_points` | 24.65ms | 26.54ms | 1.1× | 1.3% | PASS |
| ac_reads | `select_random_ranges` | 8.33ms | 8.64ms | 1.0× | 0.9% | PASS |
| ac_reads | `covering_index_scan` | 8.53ms | 9.91ms | 1.2× | 1.2% | PASS |
| ac_reads | `groupby_scan` | 29.55ms | 34.72ms | 1.2× | 0.8% | PASS |
| ac_reads | `index_join` | 7.83ms | 9.93ms | 1.3× | 1.0% | PASS |
| ac_reads | `index_join_scan` | 3.60ms | 5.41ms | 1.5× | 1.5% | PASS |
| ac_reads | `types_table_scan` | 967.54ms | 1.06s | 1.1× | 2.0% | PASS |
| ac_reads | `table_scan` | 1.13s | 1.16s | 1.0× | 3.5% | PASS |
| ac_reads | `oltp_read_only` | 148.14ms | 162.38ms | 1.1× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 20.95ms | 50.30ms | 2.4× | 31.6% | PASS |
| ac_writes | `oltp_insert_ac` | 25.53ms | 84.05ms | 3.3× | 52.2% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.16ms | 94.93ms | 3.6× | 40.8% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.84ms | 66.11ms | 3.0× | 34.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 22.95ms | 78.33ms | 3.4× | 21.0% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.64ms | 70.03ms | 2.7× | 24.9% | PASS |
| ac_writes | `types_delete_insert_ac` | 24.99ms | 70.45ms | 2.8× | 35.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 28.37ms | 88.96ms | 3.1× | 24.4% | PASS |

</details>

</details>

## Version-control latency

Wall time: 3m 27s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 36.62ms | 130.00ms | 28.2% | 1.1% | PASS |
| `status_dirty_many_tables` | 39.47ms | 130.00ms | 30.4% | 1.9% | PASS |
| `diff_regular_working_one_table` | 30.73ms | 120.00ms | 25.6% | 1.3% | PASS |
| `diff_regular_working_many_tables` | 44.99ms | 140.00ms | 32.1% | 1.1% | PASS |
| `diff_stat_working_many_tables` | 45.24ms | 140.00ms | 32.3% | 0.8% | PASS |
| `diff_schema_working_many_tables` | 45.72ms | 140.00ms | 32.7% | 0.8% | PASS |
| `branch_list_many_branches` | 23.43ms | 35.00ms | 66.9% | 1.6% | PASS |
| `branch_create_delete` | 26.26ms | 40.00ms | 65.7% | 1.7% | PASS |
| `at_literal_deep_history` | 25.95ms | 100.00ms | 26.0% | 1.7% | PASS |
| `diff_literal_deep_history` | 25.88ms | 120.00ms | 21.6% | 1.8% | PASS |
| `history_literal_deep_history` | 27.21ms | 150.00ms | 18.1% | 1.9% | PASS |
| `checkout_branch_clean` | 39.24ms | 150.00ms | 26.2% | 0.9% | PASS |
| `merge_data_no_conflicts` | 30.70ms | 50.00ms | 61.4% | 1.0% | PASS |
| `merge_data_secondary_index` | 858.11ms | 2.50s | 34.3% | 1.5% | PASS |
| `merge_schema_no_conflicts` | 21.37ms | 35.00ms | 61.0% | 1.3% | PASS |
| `merge_data_conflicts` | 30.52ms | 180.00ms | 17.0% | 2.0% | PASS |
| `merge_data_conflicts_with_resolve` | 31.24ms | 180.00ms | 17.4% | 1.5% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
