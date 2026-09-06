# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-09-06 11:07 UTC
>
> Commit: [`37a390eb7b021962d9d287a465a2da3c9f59c3cf`](https://github.com/dolthub/doltlite/commit/37a390eb7b021962d9d287a465a2da3c9f59c3cf)
>
> Runner: ubuntu24 20260831.293.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/34024916235)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.21s | 10.73s | 1.1× | 1.4% | **PASS** |
| Writes | 2.13s | 3.46s | 1.6× | 1.5% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 11.33s | 11.06s | 1.0× | 1.4% | **PASS** |
| Writes | 3.44s | 4.09s | 1.2× | 2.2% | **PASS** |
| Autocommit writes | 813.02ms | 2.76s | 3.4× | 5.8% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.52s | 2.69s | 1.1× | 1.6% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.83s | 2.89s | 1.0× | 1.5% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.70s | 2.86s | 1.1× | 2.0% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.17s | 2.29s | 1.1× | 0.9% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 452.83ms | 727.21ms | 1.6× | 1.8% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 604.91ms | 1.00s | 1.7× | 1.5% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 606.06ms | 1.02s | 1.7× | 1.6% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 466.05ms | 702.28ms | 1.5× | 0.9% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.76s | 2.76s | 1.0× | 1.8% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 3.21s | 3.03s | 0.9× | 1.3% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.99s | 2.94s | 1.0× | 1.7% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.36s | 2.34s | 1.0× | 0.9% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 599.38ms | 788.43ms | 1.3× | 1.8% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 930.98ms | 1.10s | 1.2× | 5.2% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 862.10ms | 1.12s | 1.3× | 2.1% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 1.05s | 1.09s | 1.0× | 4.5% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.61s | 2.76s | 1.1× | 1.9% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.96s | 3.00s | 1.0× | 1.6% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.84s | 2.94s | 1.0× | 1.8% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.39s | 2.39s | 1.0× | 1.0% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 184.36ms | 684.98ms | 3.7× | 3.5% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 154.55ms | 575.67ms | 3.7× | 5.1% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 223.33ms | 786.33ms | 3.5× | 7.2% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 250.78ms | 712.34ms | 2.8× | 9.4% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.33ms | 30.69ms | 1.3× | 1.6% | PASS |
| mem_reads | `oltp_range_select` | 10.47ms | 12.42ms | 1.2× | 1.0% | PASS |
| mem_reads | `oltp_sum_range` | 9.66ms | 11.42ms | 1.2× | 1.9% | PASS |
| mem_reads | `oltp_order_range` | 2.62ms | 2.86ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_distinct_range` | 3.76ms | 4.11ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 3.91ms | 5.31ms | 1.4× | 2.8% | PASS |
| mem_reads | `select_random_points` | 10.01ms | 11.26ms | 1.1× | 3.0% | PASS |
| mem_reads | `select_random_ranges` | 3.00ms | 4.12ms | 1.4× | 2.7% | PASS |
| mem_reads | `covering_index_scan` | 4.17ms | 4.23ms | 1.0× | 1.2% | PASS |
| mem_reads | `groupby_scan` | 31.08ms | 32.94ms | 1.1× | 0.7% | PASS |
| mem_reads | `index_join` | 5.83ms | 8.19ms | 1.4× | 1.7% | PASS |
| mem_reads | `index_join_scan` | 3.26ms | 4.65ms | 1.4× | 3.0% | PASS |
| mem_reads | `types_table_scan` | 1.06s | 1.14s | 1.1× | 0.6% | PASS |
| mem_reads | `table_scan` | 1.24s | 1.29s | 1.0× | 1.6% | PASS |
| mem_reads | `oltp_read_only` | 108.77ms | 123.28ms | 1.1× | 1.2% | PASS |
| mem_writes | `oltp_bulk_insert` | 186.11ms | 258.93ms | 1.4× | 1.2% | PASS |
| mem_writes | `oltp_insert` | 15.47ms | 27.91ms | 1.8× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 51.21ms | 99.29ms | 1.9× | 1.9% | PASS |
| mem_writes | `oltp_update_non_index` | 36.32ms | 61.92ms | 1.7× | 2.9% | PASS |
| mem_writes | `oltp_delete_insert` | 46.56ms | 75.60ms | 1.6× | 2.0% | PASS |
| mem_writes | `oltp_write_only` | 22.93ms | 48.53ms | 2.1× | 2.5% | PASS |
| mem_writes | `types_delete_insert` | 25.13ms | 38.02ms | 1.5× | 1.8% | PASS |
| mem_writes | `oltp_read_write` | 69.09ms | 117.01ms | 1.7× | 1.5% | PASS |
| file_reads | `oltp_point_select` | 94.43ms | 49.78ms | 0.5× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 18.13ms | 14.41ms | 0.8× | 3.0% | PASS |
| file_reads | `oltp_sum_range` | 17.25ms | 13.66ms | 0.8× | 2.6% | PASS |
| file_reads | `oltp_order_range` | 3.55ms | 3.31ms | 0.9× | 3.7% | PASS |
| file_reads | `oltp_distinct_range` | 4.70ms | 4.53ms | 1.0× | 2.3% | PASS |
| file_reads | `oltp_index_scan` | 11.10ms | 7.78ms | 0.7× | 2.2% | PASS |
| file_reads | `select_random_points` | 17.40ms | 13.62ms | 0.8× | 4.5% | PASS |
| file_reads | `select_random_ranges` | 10.01ms | 6.10ms | 0.6× | 1.8% | PASS |
| file_reads | `covering_index_scan` | 11.36ms | 6.64ms | 0.6× | 1.8% | PASS |
| file_reads | `groupby_scan` | 32.22ms | 33.66ms | 1.0× | 0.9% | PASS |
| file_reads | `index_join` | 9.81ms | 10.19ms | 1.0× | 1.7% | PASS |
| file_reads | `index_join_scan` | 4.17ms | 4.95ms | 1.2× | 3.0% | PASS |
| file_reads | `types_table_scan` | 1.05s | 1.14s | 1.1× | 0.6% | PASS |
| file_reads | `table_scan` | 1.27s | 1.30s | 1.0× | 1.2% | PASS |
| file_reads | `oltp_read_only` | 209.44ms | 150.22ms | 0.7× | 1.1% | PASS |
| file_writes | `oltp_bulk_insert` | 198.57ms | 267.78ms | 1.3× | 1.3% | PASS |
| file_writes | `oltp_insert` | 21.93ms | 31.80ms | 1.5× | 1.5% | PASS |
| file_writes | `oltp_update_index` | 77.27ms | 110.10ms | 1.4× | 2.1% | PASS |
| file_writes | `oltp_update_non_index` | 58.29ms | 72.15ms | 1.2× | 2.0% | PASS |
| file_writes | `oltp_delete_insert` | 68.10ms | 83.83ms | 1.2× | 1.7% | PASS |
| file_writes | `oltp_write_only` | 43.33ms | 55.35ms | 1.3× | 1.6% | PASS |
| file_writes | `types_delete_insert` | 39.17ms | 43.43ms | 1.1× | 1.8% | PASS |
| file_writes | `oltp_read_write` | 92.74ms | 123.99ms | 1.3× | 1.8% | PASS |
| ac_reads | `oltp_point_select` | 46.92ms | 49.42ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_range_select` | 13.29ms | 14.28ms | 1.1× | 2.0% | PASS |
| ac_reads | `oltp_sum_range` | 12.27ms | 13.58ms | 1.1× | 2.3% | PASS |
| ac_reads | `oltp_order_range` | 3.10ms | 3.34ms | 1.1× | 3.4% | PASS |
| ac_reads | `oltp_distinct_range` | 4.18ms | 4.52ms | 1.1× | 2.4% | PASS |
| ac_reads | `oltp_index_scan` | 6.56ms | 7.66ms | 1.2× | 2.6% | PASS |
| ac_reads | `select_random_points` | 12.80ms | 13.48ms | 1.1× | 2.2% | PASS |
| ac_reads | `select_random_ranges` | 5.47ms | 6.08ms | 1.1× | 1.8% | PASS |
| ac_reads | `covering_index_scan` | 6.97ms | 6.65ms | 1.0× | 1.8% | PASS |
| ac_reads | `groupby_scan` | 31.70ms | 33.75ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 7.46ms | 10.02ms | 1.3× | 1.9% | PASS |
| ac_reads | `index_join_scan` | 3.75ms | 4.98ms | 1.3× | 3.3% | PASS |
| ac_reads | `types_table_scan` | 1.05s | 1.14s | 1.1× | 0.7% | PASS |
| ac_reads | `table_scan` | 1.26s | 1.29s | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_read_only` | 144.08ms | 152.47ms | 1.1× | 0.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 20.55ms | 71.54ms | 3.5× | 2.7% | PASS |
| ac_writes | `oltp_insert_ac` | 22.95ms | 85.29ms | 3.7× | 2.3% | PASS |
| ac_writes | `oltp_update_index_ac` | 25.00ms | 97.10ms | 3.9× | 3.4% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.46ms | 80.05ms | 3.7× | 5.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 22.89ms | 90.07ms | 3.9× | 3.7% | PASS |
| ac_writes | `oltp_write_only_ac` | 23.12ms | 87.23ms | 3.8× | 4.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 20.60ms | 80.37ms | 3.9× | 4.6% | PASS |
| ac_writes | `oltp_read_write_ac` | 27.79ms | 93.34ms | 3.4× | 3.2% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 40.78ms | 37.74ms | 0.9× | 2.9% | PASS |
| mem_reads | `oltp_range_select` | 17.93ms | 13.81ms | 0.8× | 2.0% | PASS |
| mem_reads | `oltp_sum_range` | 16.37ms | 13.59ms | 0.8× | 1.6% | PASS |
| mem_reads | `oltp_order_range` | 3.60ms | 3.26ms | 0.9× | 0.9% | PASS |
| mem_reads | `oltp_distinct_range` | 4.68ms | 4.39ms | 0.9× | 0.7% | PASS |
| mem_reads | `oltp_index_scan` | 4.19ms | 6.24ms | 1.5× | 1.6% | PASS |
| mem_reads | `select_random_points` | 22.73ms | 20.79ms | 0.9× | 3.2% | PASS |
| mem_reads | `select_random_ranges` | 3.63ms | 5.35ms | 1.5× | 0.9% | PASS |
| mem_reads | `covering_index_scan` | 4.28ms | 4.52ms | 1.1× | 2.0% | PASS |
| mem_reads | `groupby_scan` | 37.75ms | 36.16ms | 1.0× | 0.8% | PASS |
| mem_reads | `index_join` | 10.73ms | 8.76ms | 0.8× | 1.5% | PASS |
| mem_reads | `index_join_scan` | 3.97ms | 5.70ms | 1.4× | 2.2% | PASS |
| mem_reads | `types_table_scan` | 1.17s | 1.23s | 1.1× | 0.5% | PASS |
| mem_reads | `table_scan` | 1.34s | 1.37s | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_read_only` | 147.38ms | 134.50ms | 0.9× | 1.4% | PASS |
| mem_writes | `oltp_bulk_insert` | 245.89ms | 359.64ms | 1.5× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 18.50ms | 39.15ms | 2.1× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 65.95ms | 145.27ms | 2.2× | 1.2% | PASS |
| mem_writes | `oltp_update_non_index` | 49.84ms | 86.73ms | 1.7× | 1.6% | PASS |
| mem_writes | `oltp_delete_insert` | 55.44ms | 108.20ms | 2.0× | 1.5% | PASS |
| mem_writes | `oltp_write_only` | 30.14ms | 66.81ms | 2.2× | 1.9% | PASS |
| mem_writes | `types_delete_insert` | 40.28ms | 54.47ms | 1.4× | 1.9% | PASS |
| mem_writes | `oltp_read_write` | 98.87ms | 144.49ms | 1.5× | 2.3% | PASS |
| file_reads | `oltp_point_select` | 122.06ms | 57.35ms | 0.5× | 1.2% | PASS |
| file_reads | `oltp_range_select` | 27.59ms | 16.29ms | 0.6× | 1.4% | PASS |
| file_reads | `oltp_sum_range` | 26.14ms | 16.20ms | 0.6× | 0.8% | PASS |
| file_reads | `oltp_order_range` | 4.58ms | 3.53ms | 0.8× | 1.0% | PASS |
| file_reads | `oltp_distinct_range` | 5.59ms | 4.70ms | 0.8× | 1.0% | PASS |
| file_reads | `oltp_index_scan` | 12.73ms | 8.39ms | 0.7× | 1.5% | PASS |
| file_reads | `select_random_points` | 31.82ms | 23.08ms | 0.7× | 0.9% | PASS |
| file_reads | `select_random_ranges` | 12.17ms | 7.63ms | 0.6× | 1.4% | PASS |
| file_reads | `covering_index_scan` | 13.20ms | 7.10ms | 0.5× | 1.2% | PASS |
| file_reads | `groupby_scan` | 39.72ms | 37.23ms | 0.9× | 1.3% | PASS |
| file_reads | `index_join` | 16.33ms | 11.05ms | 0.7× | 2.0% | PASS |
| file_reads | `index_join_scan` | 5.00ms | 6.24ms | 1.2× | 2.1% | PASS |
| file_reads | `types_table_scan` | 1.22s | 1.26s | 1.0× | 1.5% | PASS |
| file_reads | `table_scan` | 1.40s | 1.39s | 1.0× | 2.6% | PASS |
| file_reads | `oltp_read_only` | 274.98ms | 167.72ms | 0.6× | 1.1% | PASS |
| file_writes | `oltp_bulk_insert` | 270.00ms | 371.31ms | 1.4× | 1.2% | PASS |
| file_writes | `oltp_insert` | 25.78ms | 46.27ms | 1.8× | 2.0% | PASS |
| file_writes | `oltp_update_index` | 128.43ms | 163.55ms | 1.3× | 8.1% | PASS |
| file_writes | `oltp_update_non_index` | 97.09ms | 101.25ms | 1.0× | 9.8% | PASS |
| file_writes | `oltp_delete_insert` | 95.15ms | 122.49ms | 1.3× | 1.0% | PASS |
| file_writes | `oltp_write_only` | 84.85ms | 75.59ms | 0.9× | 15.1% | PASS |
| file_writes | `types_delete_insert` | 71.89ms | 62.45ms | 0.9× | 2.2% | PASS |
| file_writes | `oltp_read_write` | 157.79ms | 158.68ms | 1.0× | 9.4% | PASS |
| ac_reads | `oltp_point_select` | 68.44ms | 58.96ms | 0.9× | 1.7% | PASS |
| ac_reads | `oltp_range_select` | 22.03ms | 16.32ms | 0.7× | 2.0% | PASS |
| ac_reads | `oltp_sum_range` | 19.38ms | 15.96ms | 0.8× | 2.3% | PASS |
| ac_reads | `oltp_order_range` | 4.04ms | 3.52ms | 0.9× | 1.1% | PASS |
| ac_reads | `oltp_distinct_range` | 5.03ms | 4.68ms | 0.9× | 1.0% | PASS |
| ac_reads | `oltp_index_scan` | 7.12ms | 8.34ms | 1.2× | 1.6% | PASS |
| ac_reads | `select_random_points` | 26.82ms | 23.32ms | 0.9× | 1.7% | PASS |
| ac_reads | `select_random_ranges` | 6.65ms | 7.64ms | 1.1× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 7.47ms | 7.11ms | 1.0× | 1.2% | PASS |
| ac_reads | `groupby_scan` | 38.62ms | 36.77ms | 1.0× | 0.8% | PASS |
| ac_reads | `index_join` | 13.36ms | 11.04ms | 0.8× | 2.2% | PASS |
| ac_reads | `index_join_scan` | 4.45ms | 6.07ms | 1.4× | 1.1% | PASS |
| ac_reads | `types_table_scan` | 1.18s | 1.24s | 1.1× | 0.9% | PASS |
| ac_reads | `table_scan` | 1.37s | 1.39s | 1.0× | 3.0% | PASS |
| ac_reads | `oltp_read_only` | 189.89ms | 165.86ms | 0.9× | 1.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 16.80ms | 56.89ms | 3.4× | 4.3% | PASS |
| ac_writes | `oltp_insert_ac` | 19.39ms | 69.46ms | 3.6× | 5.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 19.68ms | 82.70ms | 4.2× | 5.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 16.26ms | 65.88ms | 4.1× | 5.5% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 19.12ms | 75.16ms | 3.9× | 3.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 18.97ms | 75.06ms | 4.0× | 5.4% | PASS |
| ac_writes | `types_delete_insert_ac` | 17.60ms | 67.91ms | 3.9× | 6.1% | PASS |
| ac_writes | `oltp_read_write_ac` | 26.72ms | 82.61ms | 3.1× | 5.0% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 35.71ms | 39.00ms | 1.1× | 2.0% | PASS |
| mem_reads | `oltp_range_select` | 16.86ms | 14.13ms | 0.8× | 2.9% | PASS |
| mem_reads | `oltp_sum_range` | 15.39ms | 13.84ms | 0.9× | 1.4% | PASS |
| mem_reads | `oltp_order_range` | 3.32ms | 3.22ms | 1.0× | 1.4% | PASS |
| mem_reads | `oltp_distinct_range` | 4.41ms | 4.45ms | 1.0× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 4.06ms | 6.49ms | 1.6× | 2.3% | PASS |
| mem_reads | `select_random_points` | 21.97ms | 21.63ms | 1.0× | 3.1% | PASS |
| mem_reads | `select_random_ranges` | 3.55ms | 5.44ms | 1.5× | 1.6% | PASS |
| mem_reads | `covering_index_scan` | 4.18ms | 4.93ms | 1.2× | 1.4% | PASS |
| mem_reads | `groupby_scan` | 35.40ms | 34.94ms | 1.0× | 0.8% | PASS |
| mem_reads | `index_join` | 10.56ms | 9.96ms | 0.9× | 2.9% | PASS |
| mem_reads | `index_join_scan` | 3.76ms | 5.62ms | 1.5× | 2.6% | PASS |
| mem_reads | `types_table_scan` | 1.14s | 1.22s | 1.1× | 2.0% | PASS |
| mem_reads | `table_scan` | 1.26s | 1.33s | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_read_only` | 145.17ms | 142.11ms | 1.0× | 2.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 247.28ms | 374.63ms | 1.5× | 1.3% | PASS |
| mem_writes | `oltp_insert` | 18.84ms | 39.31ms | 2.1× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 66.16ms | 145.39ms | 2.2× | 1.5% | PASS |
| mem_writes | `oltp_update_non_index` | 48.97ms | 86.85ms | 1.8× | 1.6% | PASS |
| mem_writes | `oltp_delete_insert` | 54.74ms | 107.37ms | 2.0× | 2.0% | PASS |
| mem_writes | `oltp_write_only` | 28.78ms | 63.86ms | 2.2× | 2.2% | PASS |
| mem_writes | `types_delete_insert` | 38.38ms | 54.71ms | 1.4× | 1.6% | PASS |
| mem_writes | `oltp_read_write` | 102.92ms | 152.47ms | 1.5× | 1.9% | PASS |
| file_reads | `oltp_point_select` | 107.56ms | 59.11ms | 0.5× | 1.4% | PASS |
| file_reads | `oltp_range_select` | 24.34ms | 16.41ms | 0.7× | 1.8% | PASS |
| file_reads | `oltp_sum_range` | 23.45ms | 16.29ms | 0.7× | 1.8% | PASS |
| file_reads | `oltp_order_range` | 4.30ms | 3.52ms | 0.8× | 1.7% | PASS |
| file_reads | `oltp_distinct_range` | 5.62ms | 4.95ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_index_scan` | 11.32ms | 8.65ms | 0.8× | 1.7% | PASS |
| file_reads | `select_random_points` | 31.64ms | 24.96ms | 0.8× | 2.7% | PASS |
| file_reads | `select_random_ranges` | 10.57ms | 7.46ms | 0.7× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 11.73ms | 7.03ms | 0.6× | 1.2% | PASS |
| file_reads | `groupby_scan` | 36.58ms | 35.25ms | 1.0× | 0.8% | PASS |
| file_reads | `index_join` | 15.03ms | 11.43ms | 0.8× | 2.4% | PASS |
| file_reads | `index_join_scan` | 4.82ms | 5.97ms | 1.2× | 3.6% | PASS |
| file_reads | `types_table_scan` | 1.14s | 1.22s | 1.1× | 2.0% | PASS |
| file_reads | `table_scan` | 1.30s | 1.34s | 1.0× | 1.3% | PASS |
| file_reads | `oltp_read_only` | 254.10ms | 173.94ms | 0.7× | 1.6% | PASS |
| file_writes | `oltp_bulk_insert` | 269.29ms | 386.48ms | 1.4× | 0.9% | PASS |
| file_writes | `oltp_insert` | 28.60ms | 46.58ms | 1.6× | 2.3% | PASS |
| file_writes | `oltp_update_index` | 105.13ms | 161.01ms | 1.5× | 1.7% | PASS |
| file_writes | `oltp_update_non_index` | 90.45ms | 100.53ms | 1.1× | 5.9% | PASS |
| file_writes | `oltp_delete_insert` | 93.83ms | 120.84ms | 1.3× | 2.1% | PASS |
| file_writes | `oltp_write_only` | 66.51ms | 74.40ms | 1.1× | 2.3% | PASS |
| file_writes | `types_delete_insert` | 66.60ms | 63.33ms | 1.0× | 1.6% | PASS |
| file_writes | `oltp_read_write` | 141.68ms | 163.51ms | 1.2× | 2.1% | PASS |
| ac_reads | `oltp_point_select` | 60.37ms | 59.54ms | 1.0× | 1.8% | PASS |
| ac_reads | `oltp_range_select` | 19.45ms | 16.24ms | 0.8× | 2.2% | PASS |
| ac_reads | `oltp_sum_range` | 19.00ms | 16.51ms | 0.9× | 2.5% | PASS |
| ac_reads | `oltp_order_range` | 3.87ms | 3.55ms | 0.9× | 1.4% | PASS |
| ac_reads | `oltp_distinct_range` | 5.36ms | 5.06ms | 0.9× | 2.1% | PASS |
| ac_reads | `oltp_index_scan` | 6.80ms | 8.67ms | 1.3× | 1.3% | PASS |
| ac_reads | `select_random_points` | 25.72ms | 25.00ms | 1.0× | 2.7% | PASS |
| ac_reads | `select_random_ranges` | 6.03ms | 7.46ms | 1.2× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 7.11ms | 7.00ms | 1.0× | 2.2% | PASS |
| ac_reads | `groupby_scan` | 36.25ms | 35.59ms | 1.0× | 0.8% | PASS |
| ac_reads | `index_join` | 12.66ms | 11.53ms | 0.9× | 2.5% | PASS |
| ac_reads | `index_join_scan` | 4.29ms | 5.89ms | 1.4× | 1.7% | PASS |
| ac_reads | `types_table_scan` | 1.15s | 1.22s | 1.1× | 2.4% | PASS |
| ac_reads | `table_scan` | 1.30s | 1.34s | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_read_only` | 184.16ms | 173.75ms | 0.9× | 1.3% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 25.01ms | 82.48ms | 3.3× | 6.0% | PASS |
| ac_writes | `oltp_insert_ac` | 26.97ms | 96.52ms | 3.6× | 4.7% | PASS |
| ac_writes | `oltp_update_index_ac` | 29.34ms | 109.73ms | 3.7× | 9.5% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.49ms | 90.95ms | 3.7× | 9.4% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 28.91ms | 106.18ms | 3.7× | 7.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 28.46ms | 97.65ms | 3.4× | 7.0% | PASS |
| ac_writes | `types_delete_insert_ac` | 25.07ms | 93.19ms | 3.7× | 8.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 35.07ms | 109.63ms | 3.1× | 7.0% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 25.99ms | 29.36ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_range_select` | 15.73ms | 16.22ms | 1.0× | 1.5% | PASS |
| mem_reads | `oltp_sum_range` | 14.20ms | 15.30ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_order_range` | 2.96ms | 3.08ms | 1.0× | 0.9% | PASS |
| mem_reads | `oltp_distinct_range` | 3.84ms | 4.03ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 3.59ms | 4.41ms | 1.2× | 1.1% | PASS |
| mem_reads | `select_random_points` | 21.10ms | 24.06ms | 1.1× | 0.8% | PASS |
| mem_reads | `select_random_ranges` | 5.87ms | 6.42ms | 1.1× | 1.0% | PASS |
| mem_reads | `covering_index_scan` | 3.32ms | 3.21ms | 1.0× | 0.8% | PASS |
| mem_reads | `groupby_scan` | 30.61ms | 31.62ms | 1.0× | 0.7% | PASS |
| mem_reads | `index_join` | 6.18ms | 7.73ms | 1.2× | 0.7% | PASS |
| mem_reads | `index_join_scan` | 3.21ms | 4.44ms | 1.4× | 0.8% | PASS |
| mem_reads | `types_table_scan` | 882.87ms | 954.47ms | 1.1× | 0.4% | PASS |
| mem_reads | `table_scan` | 1.03s | 1.05s | 1.0× | 0.6% | PASS |
| mem_reads | `oltp_read_only` | 118.49ms | 129.81ms | 1.1× | 1.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 194.90ms | 256.67ms | 1.3× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 15.17ms | 25.90ms | 1.7× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 52.73ms | 93.52ms | 1.8× | 0.9% | PASS |
| mem_writes | `oltp_update_non_index` | 41.21ms | 60.21ms | 1.5× | 1.3% | PASS |
| mem_writes | `oltp_delete_insert` | 39.02ms | 70.22ms | 1.8× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 21.40ms | 42.50ms | 2.0× | 1.0% | PASS |
| mem_writes | `types_delete_insert` | 25.28ms | 37.27ms | 1.5× | 0.9% | PASS |
| mem_writes | `oltp_read_write` | 76.34ms | 115.98ms | 1.5× | 0.6% | PASS |
| file_reads | `oltp_point_select` | 91.22ms | 46.23ms | 0.5× | 0.6% | PASS |
| file_reads | `oltp_range_select` | 22.74ms | 18.21ms | 0.8× | 1.4% | PASS |
| file_reads | `oltp_sum_range` | 21.16ms | 17.20ms | 0.8× | 1.1% | PASS |
| file_reads | `oltp_order_range` | 3.67ms | 3.36ms | 0.9× | 1.6% | PASS |
| file_reads | `oltp_distinct_range` | 4.53ms | 4.30ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_index_scan` | 10.41ms | 6.65ms | 0.6× | 1.4% | PASS |
| file_reads | `select_random_points` | 28.28ms | 26.21ms | 0.9× | 1.2% | PASS |
| file_reads | `select_random_ranges` | 12.59ms | 8.38ms | 0.7× | 0.9% | PASS |
| file_reads | `covering_index_scan` | 10.15ms | 5.44ms | 0.5× | 0.9% | PASS |
| file_reads | `groupby_scan` | 31.27ms | 32.01ms | 1.0× | 0.6% | PASS |
| file_reads | `index_join` | 9.89ms | 9.55ms | 1.0× | 1.1% | PASS |
| file_reads | `index_join_scan` | 3.96ms | 4.84ms | 1.2× | 0.6% | PASS |
| file_reads | `types_table_scan` | 881.15ms | 952.59ms | 1.1× | 0.4% | PASS |
| file_reads | `table_scan` | 1.02s | 1.05s | 1.0× | 0.4% | PASS |
| file_reads | `oltp_read_only` | 212.56ms | 154.46ms | 0.7× | 0.6% | PASS |
| file_writes | `oltp_bulk_insert` | 254.37ms | 315.27ms | 1.2× | 12.1% | PASS |
| file_writes | `oltp_insert` | 32.25ms | 45.00ms | 1.4× | 6.0% | PASS |
| file_writes | `oltp_update_index` | 160.16ms | 157.59ms | 1.0× | 2.4% | PASS |
| file_writes | `oltp_update_non_index` | 136.70ms | 113.52ms | 0.8× | 3.9% | PASS |
| file_writes | `oltp_delete_insert` | 133.03ms | 127.95ms | 1.0× | 4.5% | PASS |
| file_writes | `oltp_write_only` | 94.89ms | 89.08ms | 0.9× | 4.5% | PASS |
| file_writes | `types_delete_insert` | 86.12ms | 69.81ms | 0.8× | 6.0% | PASS |
| file_writes | `oltp_read_write` | 154.96ms | 167.29ms | 1.1× | 2.7% | PASS |
| ac_reads | `oltp_point_select` | 47.65ms | 46.50ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 18.54ms | 18.18ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_sum_range` | 16.87ms | 17.21ms | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_order_range` | 3.34ms | 3.36ms | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_distinct_range` | 4.19ms | 4.30ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_index_scan` | 6.27ms | 6.71ms | 1.1× | 0.7% | PASS |
| ac_reads | `select_random_points` | 24.82ms | 26.71ms | 1.1× | 1.7% | PASS |
| ac_reads | `select_random_ranges` | 8.50ms | 8.41ms | 1.0× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 5.98ms | 5.48ms | 0.9× | 0.8% | PASS |
| ac_reads | `groupby_scan` | 31.04ms | 32.07ms | 1.0× | 0.7% | PASS |
| ac_reads | `index_join` | 7.84ms | 9.62ms | 1.2× | 1.1% | PASS |
| ac_reads | `index_join_scan` | 3.64ms | 4.84ms | 1.3× | 1.4% | PASS |
| ac_reads | `types_table_scan` | 909.34ms | 966.45ms | 1.1× | 1.4% | PASS |
| ac_reads | `table_scan` | 1.16s | 1.09s | 0.9× | 4.6% | PASS |
| ac_reads | `oltp_read_only` | 149.96ms | 154.63ms | 1.0× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 28.69ms | 73.78ms | 2.6× | 8.2% | PASS |
| ac_writes | `oltp_insert_ac` | 30.98ms | 97.10ms | 3.1× | 52.3% | PASS |
| ac_writes | `oltp_update_index_ac` | 32.90ms | 102.70ms | 3.1× | 20.5% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 28.77ms | 80.38ms | 2.8× | 6.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 30.51ms | 85.06ms | 2.8× | 5.8% | PASS |
| ac_writes | `oltp_write_only_ac` | 32.36ms | 92.32ms | 2.9× | 9.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 28.84ms | 79.38ms | 2.8× | 9.1% | PASS |
| ac_writes | `oltp_read_write_ac` | 37.74ms | 101.61ms | 2.7× | 13.3% | PASS |

</details>

</details>

## Version-control latency

Wall time: 1m 59s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 81.85ms | 130.00ms | 63.0% | 0.4% | PASS |
| `status_dirty_many_tables` | 85.03ms | 130.00ms | 65.4% | 0.3% | PASS |
| `diff_regular_working_one_table` | 77.20ms | 120.00ms | 64.3% | 0.5% | PASS |
| `diff_regular_working_many_tables` | 90.58ms | 140.00ms | 64.7% | 0.5% | PASS |
| `diff_stat_working_many_tables` | 90.41ms | 140.00ms | 64.6% | 0.3% | PASS |
| `diff_schema_working_many_tables` | 91.50ms | 140.00ms | 65.4% | 0.4% | PASS |
| `branch_list_many_branches` | 22.27ms | 35.00ms | 63.6% | 1.1% | PASS |
| `branch_create_delete` | 25.06ms | 40.00ms | 62.6% | 1.1% | PASS |
| `checkout_branch_clean` | 55.25ms | 150.00ms | 36.8% | 0.6% | PASS |
| `merge_data_no_conflicts` | 28.12ms | 50.00ms | 56.2% | 1.1% | PASS |
| `merge_schema_no_conflicts` | 21.06ms | 35.00ms | 60.2% | 1.1% | PASS |
| `merge_data_conflicts` | 31.72ms | 180.00ms | 17.6% | 0.8% | PASS |
| `merge_data_conflicts_with_resolve` | 31.69ms | 180.00ms | 17.6% | 0.9% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
