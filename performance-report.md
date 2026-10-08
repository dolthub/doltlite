# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-10-08 11:16 UTC
>
> Commit: [`2a7ff75dd4988273424f7a6a6a91a7bae094b9a0`](https://github.com/dolthub/doltlite/commit/2a7ff75dd4988273424f7a6a6a91a7bae094b9a0)
>
> Runner: ubuntu24 20261004.327.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/37758369118)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 8.73s | 10.12s | 1.2× | 1.4% | **PASS** |
| Writes | 1.82s | 2.84s | 1.6× | 1.3% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.49s | 10.27s | 1.1× | 1.5% | **PASS** |
| Writes | 3.53s | 3.80s | 1.1× | 5.9% | **PASS** |
| Autocommit writes | 847.78ms | 2.54s | 3.0× | 6.9% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.37s | 2.80s | 1.2× | 0.9% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.55s | 2.99s | 1.2× | 1.4% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 1.66s | 1.87s | 1.1× | 2.3% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.14s | 2.46s | 1.1× | 1.5% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 421.71ms | 704.33ms | 1.7× | 0.9% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 587.73ms | 938.98ms | 1.6× | 1.3% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 346.40ms | 531.12ms | 1.5× | 1.7% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 460.12ms | 661.50ms | 1.4× | 1.6% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.60s | 2.85s | 1.1× | 1.8% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.79s | 3.06s | 1.1× | 1.4% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 1.76s | 1.84s | 1.0× | 1.1% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.33s | 2.51s | 1.1× | 1.5% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 569.98ms | 770.29ms | 1.4× | 1.7% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 890.64ms | 1.04s | 1.2× | 4.7% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 972.11ms | 919.26ms | 0.9× | 5.9% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 1.09s | 1.07s | 1.0× | 16.0% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.43s | 2.82s | 1.2× | 1.4% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.64s | 3.06s | 1.2× | 1.7% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 1.64s | 1.83s | 1.1× | 2.2% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.40s | 2.55s | 1.1× | 1.0% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 175.40ms | 559.44ms | 3.2× | 4.0% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 216.27ms | 659.23ms | 3.0× | 7.4% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 215.72ms | 482.43ms | 2.2× | 6.2% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 240.39ms | 837.72ms | 3.5× | 47.8% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 22.20ms | 31.08ms | 1.4× | 1.0% | PASS |
| mem_reads | `oltp_range_select` | 9.00ms | 11.74ms | 1.3× | 0.8% | PASS |
| mem_reads | `oltp_sum_range` | 8.52ms | 11.77ms | 1.4× | 0.8% | PASS |
| mem_reads | `oltp_order_range` | 2.40ms | 2.92ms | 1.2× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 3.43ms | 4.02ms | 1.2× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 3.55ms | 5.37ms | 1.5× | 0.9% | PASS |
| mem_reads | `select_random_points` | 8.45ms | 11.87ms | 1.4× | 1.6% | PASS |
| mem_reads | `select_random_ranges` | 4.14ms | 5.28ms | 1.3× | 1.0% | PASS |
| mem_reads | `covering_index_scan` | 7.56ms | 10.45ms | 1.4× | 0.7% | PASS |
| mem_reads | `groupby_scan` | 28.81ms | 32.09ms | 1.1× | 0.5% | PASS |
| mem_reads | `index_join` | 5.56ms | 8.40ms | 1.5× | 1.1% | PASS |
| mem_reads | `index_join_scan` | 2.78ms | 5.52ms | 2.0× | 1.0% | PASS |
| mem_reads | `types_table_scan` | 1.02s | 1.22s | 1.2× | 0.6% | PASS |
| mem_reads | `table_scan` | 1.14s | 1.32s | 1.2× | 0.2% | PASS |
| mem_reads | `oltp_read_only` | 99.20ms | 122.17ms | 1.2× | 0.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 178.43ms | 277.69ms | 1.6× | 1.1% | PASS |
| mem_writes | `oltp_insert` | 15.10ms | 27.36ms | 1.8× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 47.75ms | 88.91ms | 1.9× | 0.9% | PASS |
| mem_writes | `oltp_update_non_index` | 32.06ms | 55.74ms | 1.7× | 1.0% | PASS |
| mem_writes | `oltp_delete_insert` | 42.34ms | 71.51ms | 1.7× | 0.9% | PASS |
| mem_writes | `oltp_write_only` | 20.37ms | 42.75ms | 2.1× | 0.9% | PASS |
| mem_writes | `types_delete_insert` | 23.05ms | 36.40ms | 1.6× | 1.2% | PASS |
| mem_writes | `oltp_read_write` | 62.61ms | 103.96ms | 1.7× | 0.7% | PASS |
| file_reads | `oltp_point_select` | 91.67ms | 49.90ms | 0.5× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 17.33ms | 13.69ms | 0.8× | 1.5% | PASS |
| file_reads | `oltp_sum_range` | 16.19ms | 13.83ms | 0.9× | 2.9% | PASS |
| file_reads | `oltp_order_range` | 3.34ms | 3.17ms | 0.9× | 2.8% | PASS |
| file_reads | `oltp_distinct_range` | 4.36ms | 4.30ms | 1.0× | 2.6% | PASS |
| file_reads | `oltp_index_scan` | 11.20ms | 7.54ms | 0.7× | 2.3% | PASS |
| file_reads | `select_random_points` | 17.16ms | 13.82ms | 0.8× | 3.1% | PASS |
| file_reads | `select_random_ranges` | 11.63ms | 7.31ms | 0.6× | 2.7% | PASS |
| file_reads | `covering_index_scan` | 15.11ms | 12.45ms | 0.8× | 1.3% | PASS |
| file_reads | `groupby_scan` | 30.07ms | 32.42ms | 1.1× | 0.9% | PASS |
| file_reads | `index_join` | 9.57ms | 10.01ms | 1.0× | 1.8% | PASS |
| file_reads | `index_join_scan` | 3.99ms | 5.93ms | 1.5× | 2.7% | PASS |
| file_reads | `types_table_scan` | 1.02s | 1.21s | 1.2× | 0.5% | PASS |
| file_reads | `table_scan` | 1.15s | 1.32s | 1.1× | 0.4% | PASS |
| file_reads | `oltp_read_only` | 200.57ms | 150.25ms | 0.7× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 190.93ms | 286.02ms | 1.5× | 1.2% | PASS |
| file_writes | `oltp_insert` | 21.28ms | 30.29ms | 1.4× | 1.5% | PASS |
| file_writes | `oltp_update_index` | 72.33ms | 99.20ms | 1.4× | 1.9% | PASS |
| file_writes | `oltp_update_non_index` | 54.14ms | 64.75ms | 1.2× | 2.1% | PASS |
| file_writes | `oltp_delete_insert` | 64.36ms | 82.48ms | 1.3× | 1.8% | PASS |
| file_writes | `oltp_write_only` | 41.68ms | 51.57ms | 1.2× | 2.1% | PASS |
| file_writes | `types_delete_insert` | 38.15ms | 42.55ms | 1.1× | 1.6% | PASS |
| file_writes | `oltp_read_write` | 87.10ms | 113.42ms | 1.3× | 1.3% | PASS |
| ac_reads | `oltp_point_select` | 46.19ms | 49.73ms | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_range_select` | 12.38ms | 13.69ms | 1.1× | 2.0% | PASS |
| ac_reads | `oltp_sum_range` | 11.58ms | 13.83ms | 1.2× | 1.7% | PASS |
| ac_reads | `oltp_order_range` | 2.81ms | 3.18ms | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_distinct_range` | 3.84ms | 4.30ms | 1.1× | 1.0% | PASS |
| ac_reads | `oltp_index_scan` | 6.40ms | 7.69ms | 1.2× | 2.0% | PASS |
| ac_reads | `select_random_points` | 12.55ms | 13.95ms | 1.1× | 2.0% | PASS |
| ac_reads | `select_random_ranges` | 7.10ms | 7.30ms | 1.0× | 1.9% | PASS |
| ac_reads | `covering_index_scan` | 10.01ms | 12.44ms | 1.2× | 1.1% | PASS |
| ac_reads | `groupby_scan` | 29.32ms | 32.47ms | 1.1× | 1.0% | PASS |
| ac_reads | `index_join` | 7.05ms | 9.99ms | 1.4× | 1.7% | PASS |
| ac_reads | `index_join_scan` | 3.46ms | 5.90ms | 1.7× | 1.3% | PASS |
| ac_reads | `types_table_scan` | 1.01s | 1.19s | 1.2× | 0.9% | PASS |
| ac_reads | `table_scan` | 1.13s | 1.30s | 1.1× | 0.9% | PASS |
| ac_reads | `oltp_read_only` | 131.86ms | 149.26ms | 1.1× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 19.68ms | 57.61ms | 2.9× | 2.7% | PASS |
| ac_writes | `oltp_insert_ac` | 21.67ms | 69.55ms | 3.2× | 3.2% | PASS |
| ac_writes | `oltp_update_index_ac` | 23.43ms | 80.71ms | 3.4× | 3.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 20.31ms | 63.62ms | 3.1× | 4.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 21.62ms | 72.76ms | 3.4× | 4.0% | PASS |
| ac_writes | `oltp_write_only_ac` | 22.22ms | 71.71ms | 3.2× | 4.5% | PASS |
| ac_writes | `types_delete_insert_ac` | 19.99ms | 65.25ms | 3.3× | 7.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 26.48ms | 78.23ms | 3.0× | 4.2% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 35.22ms | 40.55ms | 1.2× | 2.3% | PASS |
| mem_reads | `oltp_range_select` | 16.65ms | 15.65ms | 0.9× | 2.5% | PASS |
| mem_reads | `oltp_sum_range` | 15.66ms | 15.42ms | 1.0× | 1.8% | PASS |
| mem_reads | `oltp_order_range` | 3.33ms | 3.43ms | 1.0× | 1.7% | PASS |
| mem_reads | `oltp_distinct_range` | 4.33ms | 4.52ms | 1.0× | 1.3% | PASS |
| mem_reads | `oltp_index_scan` | 3.93ms | 6.79ms | 1.7× | 1.4% | PASS |
| mem_reads | `select_random_points` | 21.64ms | 23.22ms | 1.1× | 2.9% | PASS |
| mem_reads | `select_random_ranges` | 6.55ms | 6.93ms | 1.1× | 1.4% | PASS |
| mem_reads | `covering_index_scan` | 7.64ms | 10.79ms | 1.4× | 1.0% | PASS |
| mem_reads | `groupby_scan` | 34.44ms | 34.98ms | 1.0× | 0.9% | PASS |
| mem_reads | `index_join` | 10.32ms | 10.71ms | 1.0× | 2.0% | PASS |
| mem_reads | `index_join_scan` | 3.73ms | 6.73ms | 1.8× | 2.5% | PASS |
| mem_reads | `types_table_scan` | 1.06s | 1.28s | 1.2× | 0.5% | PASS |
| mem_reads | `table_scan` | 1.20s | 1.39s | 1.2× | 0.7% | PASS |
| mem_reads | `oltp_read_only` | 137.18ms | 145.87ms | 1.1× | 1.2% | PASS |
| mem_writes | `oltp_bulk_insert` | 237.35ms | 334.25ms | 1.4× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 17.73ms | 37.57ms | 2.1× | 0.6% | PASS |
| mem_writes | `oltp_update_index` | 64.59ms | 133.72ms | 2.1× | 1.3% | PASS |
| mem_writes | `oltp_update_non_index` | 49.32ms | 82.25ms | 1.7× | 1.8% | PASS |
| mem_writes | `oltp_delete_insert` | 54.57ms | 100.15ms | 1.8× | 1.3% | PASS |
| mem_writes | `oltp_write_only` | 27.86ms | 56.67ms | 2.0× | 1.2% | PASS |
| mem_writes | `types_delete_insert` | 38.54ms | 55.43ms | 1.4× | 2.5% | PASS |
| mem_writes | `oltp_read_write` | 97.77ms | 138.94ms | 1.4× | 1.6% | PASS |
| file_reads | `oltp_point_select` | 105.11ms | 59.97ms | 0.6× | 1.1% | PASS |
| file_reads | `oltp_range_select` | 24.08ms | 17.67ms | 0.7× | 2.4% | PASS |
| file_reads | `oltp_sum_range` | 23.29ms | 17.64ms | 0.8× | 1.7% | PASS |
| file_reads | `oltp_order_range` | 4.08ms | 3.69ms | 0.9× | 1.5% | PASS |
| file_reads | `oltp_distinct_range` | 5.18ms | 4.80ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_index_scan` | 11.13ms | 8.91ms | 0.8× | 1.3% | PASS |
| file_reads | `select_random_points` | 30.54ms | 26.32ms | 0.9× | 2.0% | PASS |
| file_reads | `select_random_ranges` | 14.09ms | 9.19ms | 0.7× | 1.8% | PASS |
| file_reads | `covering_index_scan` | 14.97ms | 13.20ms | 0.9× | 1.2% | PASS |
| file_reads | `groupby_scan` | 35.16ms | 35.27ms | 1.0× | 0.9% | PASS |
| file_reads | `index_join` | 14.56ms | 11.96ms | 0.8× | 1.5% | PASS |
| file_reads | `index_join_scan` | 4.70ms | 7.05ms | 1.5× | 2.8% | PASS |
| file_reads | `types_table_scan` | 1.06s | 1.28s | 1.2× | 0.4% | PASS |
| file_reads | `table_scan` | 1.20s | 1.39s | 1.2× | 0.7% | PASS |
| file_reads | `oltp_read_only` | 242.01ms | 175.01ms | 0.7× | 1.2% | PASS |
| file_writes | `oltp_bulk_insert` | 261.99ms | 347.88ms | 1.3× | 1.0% | PASS |
| file_writes | `oltp_insert` | 25.15ms | 43.13ms | 1.7× | 1.4% | PASS |
| file_writes | `oltp_update_index` | 115.06ms | 151.69ms | 1.3× | 9.5% | PASS |
| file_writes | `oltp_update_non_index` | 98.91ms | 95.02ms | 1.0× | 9.9% | PASS |
| file_writes | `oltp_delete_insert` | 94.64ms | 115.08ms | 1.2× | 1.8% | PASS |
| file_writes | `oltp_write_only` | 70.66ms | 69.49ms | 1.0× | 14.9% | PASS |
| file_writes | `types_delete_insert` | 71.53ms | 67.09ms | 0.9× | 2.2% | PASS |
| file_writes | `oltp_read_write` | 152.70ms | 151.71ms | 1.0× | 7.2% | PASS |
| ac_reads | `oltp_point_select` | 58.27ms | 60.06ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_range_select` | 19.48ms | 17.68ms | 0.9× | 2.6% | PASS |
| ac_reads | `oltp_sum_range` | 18.59ms | 17.71ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_order_range` | 3.67ms | 3.68ms | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_distinct_range` | 4.69ms | 4.81ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_index_scan` | 6.48ms | 8.89ms | 1.4× | 2.1% | PASS |
| ac_reads | `select_random_points` | 25.80ms | 26.43ms | 1.0× | 3.6% | PASS |
| ac_reads | `select_random_ranges` | 9.16ms | 9.14ms | 1.0× | 1.8% | PASS |
| ac_reads | `covering_index_scan` | 10.22ms | 13.27ms | 1.3× | 1.6% | PASS |
| ac_reads | `groupby_scan` | 34.66ms | 35.24ms | 1.0× | 0.8% | PASS |
| ac_reads | `index_join` | 12.10ms | 11.94ms | 1.0× | 1.7% | PASS |
| ac_reads | `index_join_scan` | 4.21ms | 7.06ms | 1.7× | 1.8% | PASS |
| ac_reads | `types_table_scan` | 1.06s | 1.28s | 1.2× | 0.5% | PASS |
| ac_reads | `table_scan` | 1.20s | 1.39s | 1.2× | 0.5% | PASS |
| ac_reads | `oltp_read_only` | 173.90ms | 176.00ms | 1.0× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 24.48ms | 66.24ms | 2.7× | 4.2% | PASS |
| ac_writes | `oltp_insert_ac` | 29.56ms | 80.07ms | 2.7× | 7.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 28.65ms | 94.83ms | 3.3× | 7.0% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.94ms | 74.33ms | 3.2× | 6.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.25ms | 85.68ms | 3.3× | 8.8% | PASS |
| ac_writes | `oltp_write_only_ac` | 27.21ms | 85.48ms | 3.1× | 9.4% | PASS |
| ac_writes | `types_delete_insert_ac` | 24.65ms | 81.06ms | 3.3× | 9.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.53ms | 91.53ms | 2.8× | 6.7% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 22.90ms | 23.11ms | 1.0× | 3.7% | PASS |
| mem_reads | `oltp_range_select` | 9.79ms | 9.20ms | 0.9× | 3.0% | PASS |
| mem_reads | `oltp_sum_range` | 9.67ms | 9.26ms | 1.0× | 2.4% | PASS |
| mem_reads | `oltp_order_range` | 2.15ms | 2.09ms | 1.0× | 2.1% | PASS |
| mem_reads | `oltp_distinct_range` | 2.62ms | 2.55ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 2.48ms | 4.38ms | 1.8× | 2.3% | PASS |
| mem_reads | `select_random_points` | 14.07ms | 13.78ms | 1.0× | 3.3% | PASS |
| mem_reads | `select_random_ranges` | 4.14ms | 3.94ms | 1.0× | 2.9% | PASS |
| mem_reads | `covering_index_scan` | 3.86ms | 6.20ms | 1.6× | 2.1% | PASS |
| mem_reads | `groupby_scan` | 19.65ms | 20.13ms | 1.0× | 1.2% | PASS |
| mem_reads | `index_join` | 7.93ms | 7.47ms | 0.9× | 3.6% | PASS |
| mem_reads | `index_join_scan` | 2.78ms | 5.58ms | 2.0× | 6.2% | PASS |
| mem_reads | `types_table_scan` | 706.43ms | 823.93ms | 1.2× | 1.5% | PASS |
| mem_reads | `table_scan` | 775.18ms | 861.69ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_read_only` | 73.68ms | 75.32ms | 1.0× | 1.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 141.85ms | 187.32ms | 1.3× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 10.92ms | 22.16ms | 2.0× | 1.4% | PASS |
| mem_writes | `oltp_update_index` | 41.38ms | 81.88ms | 2.0× | 1.7% | PASS |
| mem_writes | `oltp_update_non_index` | 29.28ms | 45.67ms | 1.6× | 2.0% | PASS |
| mem_writes | `oltp_delete_insert` | 31.17ms | 56.52ms | 1.8× | 1.5% | PASS |
| mem_writes | `oltp_write_only` | 17.70ms | 34.18ms | 1.9× | 2.0% | PASS |
| mem_writes | `types_delete_insert` | 22.92ms | 31.23ms | 1.4× | 2.6% | PASS |
| mem_writes | `oltp_read_write` | 51.16ms | 72.17ms | 1.4× | 1.7% | PASS |
| file_reads | `oltp_point_select` | 72.13ms | 35.72ms | 0.5× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 14.70ms | 10.04ms | 0.7× | 1.4% | PASS |
| file_reads | `oltp_sum_range` | 14.24ms | 10.10ms | 0.7× | 1.2% | PASS |
| file_reads | `oltp_order_range` | 2.67ms | 2.21ms | 0.8× | 0.9% | PASS |
| file_reads | `oltp_distinct_range` | 3.13ms | 2.70ms | 0.9× | 0.9% | PASS |
| file_reads | `oltp_index_scan` | 8.13ms | 5.78ms | 0.7× | 1.1% | PASS |
| file_reads | `select_random_points` | 19.95ms | 14.91ms | 0.7× | 2.3% | PASS |
| file_reads | `select_random_ranges` | 9.74ms | 5.41ms | 0.6× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 9.55ms | 7.40ms | 0.8× | 0.8% | PASS |
| file_reads | `groupby_scan` | 19.66ms | 19.89ms | 1.0× | 0.8% | PASS |
| file_reads | `index_join` | 10.64ms | 7.66ms | 0.7× | 2.4% | PASS |
| file_reads | `index_join_scan` | 3.20ms | 4.71ms | 1.5× | 1.5% | PASS |
| file_reads | `types_table_scan` | 662.65ms | 767.46ms | 1.2× | 0.8% | PASS |
| file_reads | `table_scan` | 758.69ms | 853.11ms | 1.1× | 1.3% | PASS |
| file_reads | `oltp_read_only` | 153.67ms | 97.58ms | 0.6× | 2.3% | PASS |
| file_writes | `oltp_bulk_insert` | 199.21ms | 239.35ms | 1.2× | 4.3% | PASS |
| file_writes | `oltp_insert` | 19.17ms | 43.21ms | 2.3× | 8.3% | PASS |
| file_writes | `oltp_update_index` | 140.45ms | 155.72ms | 1.1× | 7.0% | PASS |
| file_writes | `oltp_update_non_index` | 120.18ms | 99.26ms | 0.8× | 6.6% | PASS |
| file_writes | `oltp_delete_insert` | 140.76ms | 120.05ms | 0.9× | 3.2% | PASS |
| file_writes | `oltp_write_only` | 110.04ms | 79.81ms | 0.7× | 8.0% | PASS |
| file_writes | `types_delete_insert` | 101.98ms | 62.97ms | 0.6× | 5.3% | PASS |
| file_writes | `oltp_read_write` | 140.31ms | 118.90ms | 0.8× | 1.3% | PASS |
| ac_reads | `oltp_point_select` | 38.57ms | 36.09ms | 0.9× | 1.8% | PASS |
| ac_reads | `oltp_range_select` | 11.62ms | 10.26ms | 0.9× | 2.6% | PASS |
| ac_reads | `oltp_sum_range` | 10.87ms | 10.05ms | 0.9× | 2.1% | PASS |
| ac_reads | `oltp_order_range` | 2.38ms | 2.20ms | 0.9× | 3.0% | PASS |
| ac_reads | `oltp_distinct_range` | 2.83ms | 2.68ms | 0.9× | 1.8% | PASS |
| ac_reads | `oltp_index_scan` | 4.61ms | 5.67ms | 1.2× | 3.4% | PASS |
| ac_reads | `select_random_points` | 15.89ms | 14.48ms | 0.9× | 3.0% | PASS |
| ac_reads | `select_random_ranges` | 6.19ms | 5.40ms | 0.9× | 2.2% | PASS |
| ac_reads | `covering_index_scan` | 6.01ms | 7.36ms | 1.2× | 3.0% | PASS |
| ac_reads | `groupby_scan` | 19.56ms | 19.80ms | 1.0× | 1.1% | PASS |
| ac_reads | `index_join` | 8.63ms | 7.51ms | 0.9× | 3.3% | PASS |
| ac_reads | `index_join_scan` | 2.85ms | 4.64ms | 1.6× | 2.2% | PASS |
| ac_reads | `types_table_scan` | 660.86ms | 764.90ms | 1.2× | 0.6% | PASS |
| ac_reads | `table_scan` | 754.34ms | 847.63ms | 1.1× | 0.5% | PASS |
| ac_reads | `oltp_read_only` | 97.91ms | 93.35ms | 1.0× | 1.6% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 26.25ms | 49.76ms | 1.9× | 4.4% | PASS |
| ac_writes | `oltp_insert_ac` | 29.10ms | 61.18ms | 2.1× | 6.3% | PASS |
| ac_writes | `oltp_update_index_ac` | 29.05ms | 68.41ms | 2.4× | 4.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.37ms | 57.01ms | 2.3× | 7.9% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 28.27ms | 61.83ms | 2.2× | 4.7% | PASS |
| ac_writes | `oltp_write_only_ac` | 27.25ms | 60.71ms | 2.2× | 7.5% | PASS |
| ac_writes | `types_delete_insert_ac` | 20.78ms | 58.01ms | 2.8× | 6.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 30.65ms | 65.52ms | 2.1× | 6.0% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 28.36ms | 31.39ms | 1.1× | 2.3% | PASS |
| mem_reads | `oltp_range_select` | 15.43ms | 17.13ms | 1.1× | 2.6% | PASS |
| mem_reads | `oltp_sum_range` | 13.96ms | 16.57ms | 1.2× | 1.7% | PASS |
| mem_reads | `oltp_order_range` | 2.95ms | 3.18ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_distinct_range` | 3.74ms | 4.06ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 3.57ms | 4.69ms | 1.3× | 1.1% | PASS |
| mem_reads | `select_random_points` | 21.54ms | 24.39ms | 1.1× | 1.3% | PASS |
| mem_reads | `select_random_ranges` | 5.83ms | 6.67ms | 1.1× | 2.0% | PASS |
| mem_reads | `covering_index_scan` | 5.86ms | 7.92ms | 1.4× | 1.5% | PASS |
| mem_reads | `groupby_scan` | 30.18ms | 33.27ms | 1.1× | 0.7% | PASS |
| mem_reads | `index_join` | 6.17ms | 8.62ms | 1.4× | 2.6% | PASS |
| mem_reads | `index_join_scan` | 3.20ms | 5.15ms | 1.6× | 2.1% | PASS |
| mem_reads | `types_table_scan` | 873.40ms | 1.03s | 1.2× | 0.9% | PASS |
| mem_reads | `table_scan` | 1.01s | 1.14s | 1.1× | 2.2% | PASS |
| mem_reads | `oltp_read_only` | 114.93ms | 132.19ms | 1.2× | 1.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 188.04ms | 235.96ms | 1.3× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 14.68ms | 24.41ms | 1.7× | 1.1% | PASS |
| mem_writes | `oltp_update_index` | 52.53ms | 86.42ms | 1.6× | 1.0% | PASS |
| mem_writes | `oltp_update_non_index` | 40.20ms | 57.96ms | 1.4× | 1.5% | PASS |
| mem_writes | `oltp_delete_insert` | 39.37ms | 65.93ms | 1.7× | 2.0% | PASS |
| mem_writes | `oltp_write_only` | 22.74ms | 40.57ms | 1.8× | 2.2% | PASS |
| mem_writes | `types_delete_insert` | 26.57ms | 38.07ms | 1.4× | 2.5% | PASS |
| mem_writes | `oltp_read_write` | 75.98ms | 112.18ms | 1.5× | 1.6% | PASS |
| file_reads | `oltp_point_select` | 92.55ms | 47.90ms | 0.5× | 1.5% | PASS |
| file_reads | `oltp_range_select` | 22.57ms | 19.09ms | 0.8× | 1.3% | PASS |
| file_reads | `oltp_sum_range` | 21.30ms | 18.92ms | 0.9× | 2.1% | PASS |
| file_reads | `oltp_order_range` | 3.90ms | 3.57ms | 0.9× | 1.9% | PASS |
| file_reads | `oltp_distinct_range` | 4.67ms | 4.46ms | 1.0× | 1.5% | PASS |
| file_reads | `oltp_index_scan` | 10.66ms | 7.09ms | 0.7× | 1.6% | PASS |
| file_reads | `select_random_points` | 29.24ms | 27.24ms | 0.9× | 1.9% | PASS |
| file_reads | `select_random_ranges` | 12.99ms | 9.05ms | 0.7× | 1.9% | PASS |
| file_reads | `covering_index_scan` | 13.15ms | 10.27ms | 0.8× | 1.3% | PASS |
| file_reads | `groupby_scan` | 31.03ms | 34.35ms | 1.1× | 1.7% | PASS |
| file_reads | `index_join` | 10.30ms | 10.52ms | 1.0× | 1.7% | PASS |
| file_reads | `index_join_scan` | 4.04ms | 5.57ms | 1.4× | 1.5% | PASS |
| file_reads | `types_table_scan` | 862.09ms | 1.02s | 1.2× | 0.5% | PASS |
| file_reads | `table_scan` | 992.91ms | 1.13s | 1.1× | 0.9% | PASS |
| file_reads | `oltp_read_only` | 218.50ms | 161.15ms | 0.7× | 1.2% | PASS |
| file_writes | `oltp_bulk_insert` | 259.02ms | 301.48ms | 1.2× | 12.7% | PASS |
| file_writes | `oltp_insert` | 29.25ms | 44.30ms | 1.5× | 31.8% | PASS |
| file_writes | `oltp_update_index` | 159.08ms | 153.61ms | 1.0× | 11.7% | PASS |
| file_writes | `oltp_update_non_index` | 132.30ms | 104.68ms | 0.8× | 9.3% | PASS |
| file_writes | `oltp_delete_insert` | 160.14ms | 133.99ms | 0.8× | 19.3% | PASS |
| file_writes | `oltp_write_only` | 120.22ms | 87.33ms | 0.7× | 35.2% | PASS |
| file_writes | `types_delete_insert` | 81.83ms | 91.23ms | 1.1× | 26.5% | PASS |
| file_writes | `oltp_read_write` | 150.83ms | 156.88ms | 1.0× | 7.8% | PASS |
| ac_reads | `oltp_point_select` | 47.77ms | 47.05ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 18.14ms | 19.05ms | 1.1× | 0.6% | PASS |
| ac_reads | `oltp_sum_range` | 16.52ms | 18.59ms | 1.1× | 0.8% | PASS |
| ac_reads | `oltp_order_range` | 3.32ms | 3.43ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_distinct_range` | 4.13ms | 4.32ms | 1.0× | 0.6% | PASS |
| ac_reads | `oltp_index_scan` | 6.26ms | 6.90ms | 1.1× | 0.9% | PASS |
| ac_reads | `select_random_points` | 25.00ms | 27.33ms | 1.1× | 1.4% | PASS |
| ac_reads | `select_random_ranges` | 8.44ms | 8.79ms | 1.0× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 8.62ms | 9.94ms | 1.2× | 1.0% | PASS |
| ac_reads | `groupby_scan` | 30.56ms | 33.79ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 7.77ms | 10.15ms | 1.3× | 1.2% | PASS |
| ac_reads | `index_join_scan` | 3.64ms | 5.61ms | 1.5× | 1.2% | PASS |
| ac_reads | `types_table_scan` | 934.29ms | 1.04s | 1.1× | 1.9% | PASS |
| ac_reads | `table_scan` | 1.13s | 1.16s | 1.0× | 4.0% | PASS |
| ac_reads | `oltp_read_only` | 151.86ms | 158.72ms | 1.0× | 1.6% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.22ms | 77.68ms | 3.7× | 38.1% | PASS |
| ac_writes | `oltp_insert_ac` | 28.45ms | 75.29ms | 2.6× | 58.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 30.97ms | 108.66ms | 3.5× | 49.0% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.18ms | 78.89ms | 3.6× | 34.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.84ms | 78.56ms | 3.0× | 31.0% | PASS |
| ac_writes | `oltp_write_only_ac` | 33.58ms | 86.45ms | 2.6× | 46.7% | PASS |
| ac_writes | `types_delete_insert_ac` | 32.35ms | 140.05ms | 4.3× | 65.2% | PASS |
| ac_writes | `oltp_read_write_ac` | 45.79ms | 192.15ms | 4.2× | 61.3% | PASS |

</details>

</details>

## Version-control latency

Wall time: 7m 42s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 28.41ms | 38.00ms | 74.8% | 1.5% | PASS |
| `status_dirty_many_tables` | 31.28ms | 42.00ms | 74.5% | 1.3% | PASS |
| `diff_regular_working_one_table` | 30.77ms | 33.00ms | 93.3% | 1.5% | PASS |
| `diff_regular_working_many_tables` | 35.40ms | 50.00ms | 70.8% | 1.3% | PASS |
| `diff_stat_working_many_tables` | 35.39ms | 48.00ms | 73.7% | 1.6% | PASS |
| `diff_schema_working_many_tables` | 34.99ms | 48.00ms | 72.9% | 1.1% | PASS |
| `branch_list_many_branches` | 18.84ms | 25.00ms | 75.4% | 2.7% | PASS |
| `branch_create_delete` | 20.18ms | 27.00ms | 74.7% | 3.4% | PASS |
| `at_literal_deep_history` | 20.32ms | 28.00ms | 72.6% | 0.9% | PASS |
| `diff_literal_deep_history` | 20.30ms | 28.00ms | 72.5% | 1.0% | PASS |
| `history_literal_deep_history` | 21.10ms | 30.00ms | 70.3% | 0.6% | PASS |
| `checkout_branch_clean` | 27.32ms | 43.00ms | 63.5% | 3.2% | PASS |
| `merge_data_no_conflicts` | 22.00ms | 33.00ms | 66.7% | 1.3% | PASS |
| `merge_data_secondary_index` | 739.89ms | 944.00ms | 78.4% | 3.4% | PASS |
| `merge_schema_no_conflicts` | 19.20ms | 24.00ms | 80.0% | 9.0% | PASS |
| `merge_data_conflicts` | 24.40ms | 33.00ms | 74.0% | 1.6% | PASS |
| `merge_data_conflicts_with_resolve` | 25.12ms | 34.00ms | 73.9% | 1.6% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
