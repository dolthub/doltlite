# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-09-12 11:06 UTC
>
> Commit: [`c12a90604fb508a47465a9502079c4753df29082`](https://github.com/dolthub/doltlite/commit/c12a90604fb508a47465a9502079c4753df29082)
>
> Runner: ubuntu24 20260907.300.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/34686118287)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.91s | 10.64s | 1.1× | 1.5% | **PASS** |
| Writes | 2.09s | 3.45s | 1.7× | 1.3% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.70s | 10.82s | 1.0× | 1.2% | **PASS** |
| Writes | 3.53s | 4.15s | 1.2× | 2.3% | **PASS** |
| Autocommit writes | 1.06s | 3.57s | 3.4× | 7.0% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.45s | 2.64s | 1.1× | 1.6% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.67s | 2.84s | 1.1× | 1.4% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.54s | 2.80s | 1.1× | 1.3% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.26s | 2.36s | 1.0× | 1.5% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 443.52ms | 738.15ms | 1.7× | 1.5% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 585.18ms | 1.00s | 1.7× | 1.0% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 582.42ms | 983.32ms | 1.7× | 1.3% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 474.43ms | 726.74ms | 1.5× | 1.2% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.70s | 2.70s | 1.0× | 1.3% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.85s | 2.88s | 1.0× | 1.3% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.81s | 2.87s | 1.0× | 1.5% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.35s | 2.37s | 1.0× | 0.9% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 607.01ms | 810.23ms | 1.3× | 2.2% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 913.44ms | 1.11s | 1.2× | 2.5% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 809.17ms | 1.08s | 1.3× | 1.8% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 1.20s | 1.15s | 1.0× | 19.9% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.57s | 2.71s | 1.1× | 1.6% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.71s | 2.88s | 1.1× | 1.5% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.63s | 2.86s | 1.1× | 1.3% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.22s | 2.37s | 1.1× | 0.8% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 208.79ms | 772.49ms | 3.7× | 9.3% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 212.25ms | 736.63ms | 3.5× | 5.0% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 199.50ms | 704.53ms | 3.5× | 5.9% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 441.87ms | 1.36s | 3.1× | 50.1% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.08ms | 30.22ms | 1.3× | 1.8% | PASS |
| mem_reads | `oltp_range_select` | 10.54ms | 11.71ms | 1.1× | 2.5% | PASS |
| mem_reads | `oltp_sum_range` | 9.74ms | 11.46ms | 1.2× | 1.8% | PASS |
| mem_reads | `oltp_order_range` | 2.58ms | 2.84ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_distinct_range` | 3.65ms | 3.96ms | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_index_scan` | 3.86ms | 5.25ms | 1.4× | 2.3% | PASS |
| mem_reads | `select_random_points` | 9.99ms | 11.09ms | 1.1× | 2.3% | PASS |
| mem_reads | `select_random_ranges` | 4.61ms | 4.02ms | 0.9× | 1.6% | PASS |
| mem_reads | `covering_index_scan` | 7.82ms | 4.36ms | 0.6× | 1.7% | PASS |
| mem_reads | `groupby_scan` | 29.39ms | 32.92ms | 1.1× | 0.8% | PASS |
| mem_reads | `index_join` | 5.83ms | 7.92ms | 1.4× | 1.6% | PASS |
| mem_reads | `index_join_scan` | 3.24ms | 4.63ms | 1.4× | 2.6% | PASS |
| mem_reads | `types_table_scan` | 1.04s | 1.14s | 1.1× | 0.6% | PASS |
| mem_reads | `table_scan` | 1.19s | 1.24s | 1.0× | 1.0% | PASS |
| mem_reads | `oltp_read_only` | 103.11ms | 118.74ms | 1.2× | 1.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 182.35ms | 273.72ms | 1.5× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 15.37ms | 28.59ms | 1.9× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 50.53ms | 97.38ms | 1.9× | 1.7% | PASS |
| mem_writes | `oltp_update_non_index` | 35.01ms | 61.37ms | 1.8× | 1.6% | PASS |
| mem_writes | `oltp_delete_insert` | 44.91ms | 74.89ms | 1.7× | 1.3% | PASS |
| mem_writes | `oltp_write_only` | 22.21ms | 48.48ms | 2.2× | 1.6% | PASS |
| mem_writes | `types_delete_insert` | 24.81ms | 38.83ms | 1.6× | 1.7% | PASS |
| mem_writes | `oltp_read_write` | 68.32ms | 114.89ms | 1.7× | 1.4% | PASS |
| file_reads | `oltp_point_select` | 93.67ms | 48.87ms | 0.5× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 17.88ms | 13.75ms | 0.8× | 2.0% | PASS |
| file_reads | `oltp_sum_range` | 17.01ms | 13.42ms | 0.8× | 1.3% | PASS |
| file_reads | `oltp_order_range` | 3.48ms | 3.17ms | 0.9× | 3.2% | PASS |
| file_reads | `oltp_distinct_range` | 4.59ms | 4.29ms | 0.9× | 2.2% | PASS |
| file_reads | `oltp_index_scan` | 11.40ms | 7.56ms | 0.7× | 1.3% | PASS |
| file_reads | `select_random_points` | 17.66ms | 13.10ms | 0.7× | 2.0% | PASS |
| file_reads | `select_random_ranges` | 11.89ms | 5.92ms | 0.5× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 15.43ms | 6.42ms | 0.4× | 1.2% | PASS |
| file_reads | `groupby_scan` | 30.57ms | 33.25ms | 1.1× | 0.9% | PASS |
| file_reads | `index_join` | 10.12ms | 9.74ms | 1.0× | 1.9% | PASS |
| file_reads | `index_join_scan` | 4.24ms | 4.83ms | 1.1× | 2.5% | PASS |
| file_reads | `types_table_scan` | 1.05s | 1.15s | 1.1× | 0.5% | PASS |
| file_reads | `table_scan` | 1.20s | 1.25s | 1.0× | 1.2% | PASS |
| file_reads | `oltp_read_only` | 207.37ms | 147.54ms | 0.7× | 1.2% | PASS |
| file_writes | `oltp_bulk_insert` | 197.01ms | 284.61ms | 1.4× | 1.3% | PASS |
| file_writes | `oltp_insert` | 22.54ms | 32.51ms | 1.4× | 2.3% | PASS |
| file_writes | `oltp_update_index` | 80.45ms | 110.38ms | 1.4× | 2.2% | PASS |
| file_writes | `oltp_update_non_index` | 59.24ms | 73.57ms | 1.2× | 2.3% | PASS |
| file_writes | `oltp_delete_insert` | 68.56ms | 85.36ms | 1.2× | 2.0% | PASS |
| file_writes | `oltp_write_only` | 45.00ms | 55.37ms | 1.2× | 2.6% | PASS |
| file_writes | `types_delete_insert` | 40.03ms | 44.77ms | 1.1× | 1.4% | PASS |
| file_writes | `oltp_read_write` | 94.18ms | 123.66ms | 1.3× | 2.3% | PASS |
| ac_reads | `oltp_point_select` | 47.63ms | 48.83ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 13.10ms | 13.67ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_sum_range` | 12.08ms | 13.37ms | 1.1× | 1.7% | PASS |
| ac_reads | `oltp_order_range` | 2.93ms | 3.13ms | 1.1× | 1.9% | PASS |
| ac_reads | `oltp_distinct_range` | 4.14ms | 4.29ms | 1.0× | 3.2% | PASS |
| ac_reads | `oltp_index_scan` | 6.55ms | 7.55ms | 1.2× | 1.6% | PASS |
| ac_reads | `select_random_points` | 12.72ms | 13.05ms | 1.0× | 2.4% | PASS |
| ac_reads | `select_random_ranges` | 7.10ms | 5.93ms | 0.8× | 0.9% | PASS |
| ac_reads | `covering_index_scan` | 10.58ms | 6.51ms | 0.6× | 1.4% | PASS |
| ac_reads | `groupby_scan` | 29.61ms | 33.22ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 7.44ms | 9.62ms | 1.3× | 1.6% | PASS |
| ac_reads | `index_join_scan` | 3.74ms | 4.86ms | 1.3× | 2.5% | PASS |
| ac_reads | `types_table_scan` | 1.07s | 1.15s | 1.1× | 1.2% | PASS |
| ac_reads | `table_scan` | 1.21s | 1.25s | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_read_only` | 136.68ms | 146.68ms | 1.1× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.16ms | 78.61ms | 3.5× | 6.8% | PASS |
| ac_writes | `oltp_insert_ac` | 26.30ms | 96.12ms | 3.7× | 9.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 28.83ms | 112.38ms | 3.9× | 7.5% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.53ms | 91.65ms | 3.7× | 9.2% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.95ms | 98.67ms | 3.8× | 7.7% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.87ms | 100.03ms | 3.9× | 9.8% | PASS |
| ac_writes | `types_delete_insert_ac` | 23.72ms | 89.02ms | 3.8× | 9.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.43ms | 106.00ms | 3.4× | 11.8% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 34.65ms | 38.87ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_range_select` | 16.27ms | 14.00ms | 0.9× | 1.7% | PASS |
| mem_reads | `oltp_sum_range` | 14.71ms | 13.83ms | 0.9× | 1.4% | PASS |
| mem_reads | `oltp_order_range` | 3.21ms | 3.20ms | 1.0× | 0.9% | PASS |
| mem_reads | `oltp_distinct_range` | 4.37ms | 4.38ms | 1.0× | 2.0% | PASS |
| mem_reads | `oltp_index_scan` | 3.95ms | 6.17ms | 1.6× | 1.1% | PASS |
| mem_reads | `select_random_points` | 21.54ms | 21.49ms | 1.0× | 2.1% | PASS |
| mem_reads | `select_random_ranges` | 6.70ms | 5.37ms | 0.8× | 1.0% | PASS |
| mem_reads | `covering_index_scan` | 7.84ms | 4.78ms | 0.6× | 1.9% | PASS |
| mem_reads | `groupby_scan` | 33.83ms | 34.56ms | 1.0× | 0.8% | PASS |
| mem_reads | `index_join` | 10.16ms | 9.23ms | 0.9× | 2.5% | PASS |
| mem_reads | `index_join_scan` | 3.72ms | 5.45ms | 1.5× | 1.8% | PASS |
| mem_reads | `types_table_scan` | 1.10s | 1.20s | 1.1× | 1.2% | PASS |
| mem_reads | `table_scan` | 1.27s | 1.34s | 1.1× | 2.5% | PASS |
| mem_reads | `oltp_read_only` | 135.97ms | 139.16ms | 1.0× | 1.2% | PASS |
| mem_writes | `oltp_bulk_insert` | 242.07ms | 370.24ms | 1.5× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 17.98ms | 38.95ms | 2.2× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 63.62ms | 141.00ms | 2.2× | 0.6% | PASS |
| mem_writes | `oltp_update_non_index` | 46.50ms | 85.87ms | 1.8× | 1.0% | PASS |
| mem_writes | `oltp_delete_insert` | 52.88ms | 104.52ms | 2.0× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 27.25ms | 61.96ms | 2.3× | 1.2% | PASS |
| mem_writes | `types_delete_insert` | 37.42ms | 54.10ms | 1.4× | 1.5% | PASS |
| mem_writes | `oltp_read_write` | 97.47ms | 147.67ms | 1.5× | 2.2% | PASS |
| file_reads | `oltp_point_select` | 105.55ms | 57.45ms | 0.5× | 1.1% | PASS |
| file_reads | `oltp_range_select` | 24.14ms | 15.96ms | 0.7× | 1.6% | PASS |
| file_reads | `oltp_sum_range` | 22.59ms | 15.91ms | 0.7× | 1.4% | PASS |
| file_reads | `oltp_order_range` | 4.17ms | 3.50ms | 0.8× | 1.3% | PASS |
| file_reads | `oltp_distinct_range` | 5.32ms | 4.71ms | 0.9× | 2.0% | PASS |
| file_reads | `oltp_index_scan` | 11.34ms | 8.46ms | 0.7× | 1.0% | PASS |
| file_reads | `select_random_points` | 31.19ms | 24.79ms | 0.8× | 1.8% | PASS |
| file_reads | `select_random_ranges` | 14.17ms | 7.30ms | 0.5× | 0.9% | PASS |
| file_reads | `covering_index_scan` | 15.49ms | 6.83ms | 0.4× | 0.8% | PASS |
| file_reads | `groupby_scan` | 34.95ms | 35.07ms | 1.0× | 0.8% | PASS |
| file_reads | `index_join` | 15.07ms | 11.11ms | 0.7× | 1.7% | PASS |
| file_reads | `index_join_scan` | 4.75ms | 5.90ms | 1.2× | 2.1% | PASS |
| file_reads | `types_table_scan` | 1.08s | 1.19s | 1.1× | 0.7% | PASS |
| file_reads | `table_scan` | 1.24s | 1.32s | 1.1× | 0.7% | PASS |
| file_reads | `oltp_read_only` | 240.48ms | 167.77ms | 0.7× | 1.3% | PASS |
| file_writes | `oltp_bulk_insert` | 266.38ms | 384.67ms | 1.4× | 0.7% | PASS |
| file_writes | `oltp_insert` | 25.74ms | 45.54ms | 1.8× | 1.4% | PASS |
| file_writes | `oltp_update_index` | 124.46ms | 159.98ms | 1.3× | 12.7% | PASS |
| file_writes | `oltp_update_non_index` | 93.50ms | 100.73ms | 1.1× | 8.8% | PASS |
| file_writes | `oltp_delete_insert` | 95.04ms | 119.87ms | 1.3× | 1.6% | PASS |
| file_writes | `oltp_write_only` | 90.89ms | 73.79ms | 0.8× | 10.3% | PASS |
| file_writes | `types_delete_insert` | 71.00ms | 63.70ms | 0.9× | 1.5% | PASS |
| file_writes | `oltp_read_write` | 146.44ms | 160.14ms | 1.1× | 3.4% | PASS |
| ac_reads | `oltp_point_select` | 59.61ms | 57.90ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_range_select` | 19.13ms | 16.09ms | 0.8× | 2.0% | PASS |
| ac_reads | `oltp_sum_range` | 18.05ms | 16.11ms | 0.9× | 1.5% | PASS |
| ac_reads | `oltp_order_range` | 3.65ms | 3.49ms | 1.0× | 1.8% | PASS |
| ac_reads | `oltp_distinct_range` | 4.74ms | 4.71ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_index_scan` | 6.57ms | 8.46ms | 1.3× | 1.6% | PASS |
| ac_reads | `select_random_points` | 25.17ms | 24.69ms | 1.0× | 2.3% | PASS |
| ac_reads | `select_random_ranges` | 9.31ms | 7.30ms | 0.8× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 10.57ms | 6.82ms | 0.6× | 1.2% | PASS |
| ac_reads | `groupby_scan` | 34.09ms | 34.92ms | 1.0× | 0.7% | PASS |
| ac_reads | `index_join` | 12.30ms | 11.02ms | 0.9× | 1.4% | PASS |
| ac_reads | `index_join_scan` | 4.21ms | 5.97ms | 1.4× | 2.1% | PASS |
| ac_reads | `types_table_scan` | 1.08s | 1.19s | 1.1× | 0.7% | PASS |
| ac_reads | `table_scan` | 1.24s | 1.33s | 1.1× | 1.0% | PASS |
| ac_reads | `oltp_read_only` | 169.27ms | 167.73ms | 1.0× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 24.26ms | 78.47ms | 3.2× | 5.1% | PASS |
| ac_writes | `oltp_insert_ac` | 26.63ms | 88.77ms | 3.3× | 4.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.97ms | 101.91ms | 3.8× | 4.0% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.72ms | 86.57ms | 3.6× | 5.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.20ms | 96.12ms | 3.7× | 4.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.59ms | 95.15ms | 3.6× | 5.0% | PASS |
| ac_writes | `types_delete_insert_ac` | 24.01ms | 87.62ms | 3.6× | 6.3% | PASS |
| ac_writes | `oltp_read_write_ac` | 33.88ms | 102.02ms | 3.0× | 5.6% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 34.15ms | 37.58ms | 1.1× | 1.8% | PASS |
| mem_reads | `oltp_range_select` | 15.76ms | 13.69ms | 0.9× | 1.6% | PASS |
| mem_reads | `oltp_sum_range` | 14.42ms | 13.51ms | 0.9× | 1.1% | PASS |
| mem_reads | `oltp_order_range` | 3.12ms | 3.11ms | 1.0× | 1.3% | PASS |
| mem_reads | `oltp_distinct_range` | 4.34ms | 4.28ms | 1.0× | 2.2% | PASS |
| mem_reads | `oltp_index_scan` | 3.95ms | 6.08ms | 1.5× | 1.5% | PASS |
| mem_reads | `select_random_points` | 20.62ms | 20.77ms | 1.0× | 1.3% | PASS |
| mem_reads | `select_random_ranges` | 6.40ms | 5.25ms | 0.8× | 1.3% | PASS |
| mem_reads | `covering_index_scan` | 7.86ms | 4.56ms | 0.6× | 1.5% | PASS |
| mem_reads | `groupby_scan` | 33.26ms | 34.26ms | 1.0× | 0.9% | PASS |
| mem_reads | `index_join` | 9.87ms | 9.01ms | 0.9× | 2.4% | PASS |
| mem_reads | `index_join_scan` | 3.65ms | 5.38ms | 1.5× | 1.7% | PASS |
| mem_reads | `types_table_scan` | 1.05s | 1.18s | 1.1× | 0.4% | PASS |
| mem_reads | `table_scan` | 1.19s | 1.32s | 1.1× | 0.5% | PASS |
| mem_reads | `oltp_read_only` | 135.66ms | 135.87ms | 1.0× | 1.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 244.76ms | 366.11ms | 1.5× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 18.53ms | 38.56ms | 2.1× | 0.6% | PASS |
| mem_writes | `oltp_update_index` | 62.58ms | 136.61ms | 2.2× | 1.2% | PASS |
| mem_writes | `oltp_update_non_index` | 46.20ms | 82.90ms | 1.8× | 1.5% | PASS |
| mem_writes | `oltp_delete_insert` | 51.58ms | 101.97ms | 2.0× | 1.3% | PASS |
| mem_writes | `oltp_write_only` | 26.99ms | 60.85ms | 2.3× | 1.3% | PASS |
| mem_writes | `types_delete_insert` | 37.16ms | 52.57ms | 1.4× | 1.7% | PASS |
| mem_writes | `oltp_read_write` | 94.63ms | 143.75ms | 1.5× | 1.6% | PASS |
| file_reads | `oltp_point_select` | 105.12ms | 56.93ms | 0.5× | 1.2% | PASS |
| file_reads | `oltp_range_select` | 23.56ms | 15.84ms | 0.7× | 3.5% | PASS |
| file_reads | `oltp_sum_range` | 22.15ms | 15.71ms | 0.7× | 1.9% | PASS |
| file_reads | `oltp_order_range` | 4.12ms | 3.38ms | 0.8× | 1.5% | PASS |
| file_reads | `oltp_distinct_range` | 5.33ms | 4.59ms | 0.9× | 2.2% | PASS |
| file_reads | `oltp_index_scan` | 11.28ms | 8.35ms | 0.7× | 1.2% | PASS |
| file_reads | `select_random_points` | 29.91ms | 23.74ms | 0.8× | 2.5% | PASS |
| file_reads | `select_random_ranges` | 13.94ms | 7.23ms | 0.5× | 1.8% | PASS |
| file_reads | `covering_index_scan` | 15.32ms | 6.73ms | 0.4× | 1.0% | PASS |
| file_reads | `groupby_scan` | 34.42ms | 34.75ms | 1.0× | 0.9% | PASS |
| file_reads | `index_join` | 14.36ms | 10.79ms | 0.8× | 2.6% | PASS |
| file_reads | `index_join_scan` | 4.62ms | 5.73ms | 1.2× | 2.0% | PASS |
| file_reads | `types_table_scan` | 1.06s | 1.19s | 1.1× | 0.4% | PASS |
| file_reads | `table_scan` | 1.21s | 1.33s | 1.1× | 0.9% | PASS |
| file_reads | `oltp_read_only` | 251.04ms | 166.87ms | 0.7× | 1.3% | PASS |
| file_writes | `oltp_bulk_insert` | 266.35ms | 378.01ms | 1.4× | 0.8% | PASS |
| file_writes | `oltp_insert` | 25.59ms | 45.61ms | 1.8× | 1.8% | PASS |
| file_writes | `oltp_update_index` | 96.69ms | 153.94ms | 1.6× | 2.2% | PASS |
| file_writes | `oltp_update_non_index` | 88.41ms | 96.69ms | 1.1× | 12.3% | PASS |
| file_writes | `oltp_delete_insert` | 85.86ms | 116.05ms | 1.4× | 1.3% | PASS |
| file_writes | `oltp_write_only` | 55.97ms | 71.56ms | 1.3× | 1.8% | PASS |
| file_writes | `types_delete_insert` | 63.44ms | 61.77ms | 1.0× | 1.9% | PASS |
| file_writes | `oltp_read_write` | 126.85ms | 154.86ms | 1.2× | 1.6% | PASS |
| ac_reads | `oltp_point_select` | 58.06ms | 57.13ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_range_select` | 18.68ms | 15.84ms | 0.8× | 1.1% | PASS |
| ac_reads | `oltp_sum_range` | 17.41ms | 15.65ms | 0.9× | 1.6% | PASS |
| ac_reads | `oltp_order_range` | 3.70ms | 3.37ms | 0.9× | 1.0% | PASS |
| ac_reads | `oltp_distinct_range` | 4.81ms | 4.58ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_index_scan` | 6.59ms | 8.30ms | 1.3× | 1.4% | PASS |
| ac_reads | `select_random_points` | 24.38ms | 23.68ms | 1.0× | 1.3% | PASS |
| ac_reads | `select_random_ranges` | 9.07ms | 7.26ms | 0.8× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 10.58ms | 6.71ms | 0.6× | 1.4% | PASS |
| ac_reads | `groupby_scan` | 33.85ms | 34.72ms | 1.0× | 0.6% | PASS |
| ac_reads | `index_join` | 11.84ms | 10.75ms | 0.9× | 1.7% | PASS |
| ac_reads | `index_join_scan` | 4.15ms | 5.75ms | 1.4× | 1.8% | PASS |
| ac_reads | `types_table_scan` | 1.06s | 1.18s | 1.1× | 0.5% | PASS |
| ac_reads | `table_scan` | 1.20s | 1.32s | 1.1× | 0.8% | PASS |
| ac_reads | `oltp_read_only` | 174.49ms | 165.71ms | 0.9× | 1.3% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.25ms | 73.01ms | 3.3× | 4.2% | PASS |
| ac_writes | `oltp_insert_ac` | 25.62ms | 88.41ms | 3.5× | 6.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 25.64ms | 98.58ms | 3.8× | 7.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.16ms | 83.30ms | 3.8× | 5.4% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.32ms | 90.45ms | 3.4× | 6.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 24.03ms | 89.37ms | 3.7× | 3.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.52ms | 85.02ms | 3.8× | 6.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 30.96ms | 96.38ms | 3.1× | 4.3% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 25.90ms | 29.68ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_range_select` | 15.57ms | 16.27ms | 1.0× | 1.5% | PASS |
| mem_reads | `oltp_sum_range` | 13.97ms | 15.52ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_order_range` | 2.92ms | 3.07ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 3.83ms | 3.98ms | 1.0× | 2.8% | PASS |
| mem_reads | `oltp_index_scan` | 3.59ms | 4.41ms | 1.2× | 1.1% | PASS |
| mem_reads | `select_random_points` | 21.41ms | 23.91ms | 1.1× | 2.1% | PASS |
| mem_reads | `select_random_ranges` | 5.93ms | 6.57ms | 1.1× | 1.8% | PASS |
| mem_reads | `covering_index_scan` | 5.86ms | 3.29ms | 0.6× | 1.7% | PASS |
| mem_reads | `groupby_scan` | 29.61ms | 32.65ms | 1.1× | 0.8% | PASS |
| mem_reads | `index_join` | 6.23ms | 7.97ms | 1.3× | 1.1% | PASS |
| mem_reads | `index_join_scan` | 3.24ms | 4.76ms | 1.5× | 2.4% | PASS |
| mem_reads | `types_table_scan` | 883.28ms | 980.42ms | 1.1× | 0.7% | PASS |
| mem_reads | `table_scan` | 1.11s | 1.10s | 1.0× | 4.4% | PASS |
| mem_reads | `oltp_read_only` | 124.76ms | 130.94ms | 1.0× | 1.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 192.68ms | 263.60ms | 1.4× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 15.06ms | 26.22ms | 1.7× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 54.44ms | 98.51ms | 1.8× | 1.0% | PASS |
| mem_writes | `oltp_update_non_index` | 42.74ms | 62.70ms | 1.5× | 1.8% | PASS |
| mem_writes | `oltp_delete_insert` | 40.23ms | 73.37ms | 1.8× | 1.1% | PASS |
| mem_writes | `oltp_write_only` | 22.46ms | 44.33ms | 2.0× | 2.0% | PASS |
| mem_writes | `types_delete_insert` | 26.40ms | 39.08ms | 1.5× | 1.3% | PASS |
| mem_writes | `oltp_read_write` | 80.43ms | 118.92ms | 1.5× | 1.7% | PASS |
| file_reads | `oltp_point_select` | 91.56ms | 46.60ms | 0.5× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 22.93ms | 18.08ms | 0.8× | 1.2% | PASS |
| file_reads | `oltp_sum_range` | 21.01ms | 17.45ms | 0.8× | 0.9% | PASS |
| file_reads | `oltp_order_range` | 3.72ms | 3.33ms | 0.9× | 1.1% | PASS |
| file_reads | `oltp_distinct_range` | 4.62ms | 4.24ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 10.50ms | 6.64ms | 0.6× | 0.9% | PASS |
| file_reads | `select_random_points` | 28.50ms | 25.84ms | 0.9× | 0.7% | PASS |
| file_reads | `select_random_ranges` | 12.71ms | 8.46ms | 0.7× | 0.9% | PASS |
| file_reads | `covering_index_scan` | 12.85ms | 5.45ms | 0.4× | 0.9% | PASS |
| file_reads | `groupby_scan` | 30.44ms | 33.01ms | 1.1× | 1.0% | PASS |
| file_reads | `index_join` | 9.99ms | 9.70ms | 1.0× | 0.8% | PASS |
| file_reads | `index_join_scan` | 4.02ms | 4.94ms | 1.2× | 0.9% | PASS |
| file_reads | `types_table_scan` | 878.80ms | 964.15ms | 1.1× | 0.6% | PASS |
| file_reads | `table_scan` | 1.01s | 1.07s | 1.1× | 0.3% | PASS |
| file_reads | `oltp_read_only` | 210.89ms | 152.39ms | 0.7× | 0.6% | PASS |
| file_writes | `oltp_bulk_insert` | 255.31ms | 323.31ms | 1.3× | 9.8% | PASS |
| file_writes | `oltp_insert` | 34.07ms | 47.67ms | 1.4× | 24.9% | PASS |
| file_writes | `oltp_update_index` | 177.09ms | 180.41ms | 1.0× | 16.8% | PASS |
| file_writes | `oltp_update_non_index` | 171.09ms | 117.25ms | 0.7× | 20.4% | PASS |
| file_writes | `oltp_delete_insert` | 167.83ms | 130.82ms | 0.8× | 19.6% | PASS |
| file_writes | `oltp_write_only` | 122.73ms | 105.02ms | 0.9× | 20.2% | PASS |
| file_writes | `types_delete_insert` | 87.91ms | 83.47ms | 0.9× | 21.0% | PASS |
| file_writes | `oltp_read_write` | 185.58ms | 165.50ms | 0.9× | 15.4% | PASS |
| ac_reads | `oltp_point_select` | 47.60ms | 46.27ms | 1.0× | 0.5% | PASS |
| ac_reads | `oltp_range_select` | 18.41ms | 18.04ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_sum_range` | 16.70ms | 17.54ms | 1.1× | 0.8% | PASS |
| ac_reads | `oltp_order_range` | 3.36ms | 3.34ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_distinct_range` | 4.22ms | 4.25ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 6.25ms | 6.69ms | 1.1× | 1.2% | PASS |
| ac_reads | `select_random_points` | 24.19ms | 25.94ms | 1.1× | 0.7% | PASS |
| ac_reads | `select_random_ranges` | 8.39ms | 8.45ms | 1.0× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 8.54ms | 5.47ms | 0.6× | 0.9% | PASS |
| ac_reads | `groupby_scan` | 30.07ms | 33.00ms | 1.1× | 0.7% | PASS |
| ac_reads | `index_join` | 7.86ms | 9.75ms | 1.2× | 0.8% | PASS |
| ac_reads | `index_join_scan` | 3.66ms | 4.98ms | 1.4× | 1.1% | PASS |
| ac_reads | `types_table_scan` | 878.99ms | 964.62ms | 1.1× | 0.4% | PASS |
| ac_reads | `table_scan` | 1.01s | 1.07s | 1.1× | 0.5% | PASS |
| ac_reads | `oltp_read_only` | 149.89ms | 153.42ms | 1.0× | 1.1% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 38.37ms | 106.92ms | 2.8× | 50.2% | PASS |
| ac_writes | `oltp_insert_ac` | 44.93ms | 129.67ms | 2.9× | 30.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 40.21ms | 118.29ms | 2.9× | 59.4% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 53.40ms | 155.83ms | 2.9× | 59.2% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 102.46ms | 247.00ms | 2.4× | 66.5% | PASS |
| ac_writes | `oltp_write_only_ac` | 56.75ms | 189.82ms | 3.3× | 49.5% | PASS |
| ac_writes | `types_delete_insert_ac` | 52.18ms | 195.26ms | 3.7× | 47.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 53.57ms | 214.33ms | 4.0× | 49.9% | PASS |

</details>

</details>

## Version-control latency

Wall time: 2m 25s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 86.86ms | 130.00ms | 66.8% | 0.2% | PASS |
| `status_dirty_many_tables` | 90.58ms | 130.00ms | 69.7% | 0.3% | PASS |
| `diff_regular_working_one_table` | 82.28ms | 120.00ms | 68.6% | 0.3% | PASS |
| `diff_regular_working_many_tables` | 95.58ms | 140.00ms | 68.3% | 0.3% | PASS |
| `diff_stat_working_many_tables` | 95.70ms | 140.00ms | 68.4% | 0.3% | PASS |
| `diff_schema_working_many_tables` | 96.66ms | 140.00ms | 69.0% | 0.3% | PASS |
| `branch_list_many_branches` | 22.07ms | 35.00ms | 63.1% | 0.6% | PASS |
| `branch_create_delete` | 24.38ms | 40.00ms | 61.0% | 1.0% | PASS |
| `at_literal_deep_history` | 35.59ms | 100.00ms | 35.6% | 0.6% | PASS |
| `diff_literal_deep_history` | 36.01ms | 120.00ms | 30.0% | 1.1% | PASS |
| `history_literal_deep_history` | 36.62ms | 150.00ms | 24.4% | 0.7% | PASS |
| `checkout_branch_clean` | 55.81ms | 150.00ms | 37.2% | 0.8% | PASS |
| `merge_data_no_conflicts` | 28.70ms | 50.00ms | 57.4% | 1.2% | PASS |
| `merge_schema_no_conflicts` | 22.05ms | 35.00ms | 63.0% | 1.7% | PASS |
| `merge_data_conflicts` | 31.58ms | 180.00ms | 17.5% | 1.4% | PASS |
| `merge_data_conflicts_with_resolve` | 32.09ms | 180.00ms | 17.8% | 1.4% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
