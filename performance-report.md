# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-09-15 11:20 UTC
>
> Commit: [`aa13252d5c1ea436e92cfe4697c7656f9ac485a3`](https://github.com/dolthub/doltlite/commit/aa13252d5c1ea436e92cfe4697c7656f9ac485a3)
>
> Runner: ubuntu24 20260907.300.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/34953697138)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.89s | 10.34s | 1.0× | 1.3% | **PASS** |
| Writes | 1.93s | 3.15s | 1.6× | 1.3% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.78s | 11.00s | 1.0× | 1.4% | **PASS** |
| Writes | 3.70s | 4.32s | 1.2× | 2.2% | **PASS** |
| Autocommit writes | 823.94ms | 2.94s | 3.6× | 8.6% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.34s | 2.55s | 1.1× | 1.5% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.27s | 2.32s | 1.0× | 4.0% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.51s | 2.73s | 1.1× | 0.9% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.76s | 2.75s | 1.0× | 1.2% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 383.78ms | 635.09ms | 1.7× | 1.5% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 524.00ms | 823.26ms | 1.6× | 3.8% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 527.81ms | 901.00ms | 1.7× | 1.2% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 496.01ms | 790.62ms | 1.6× | 0.9% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.43s | 2.56s | 1.1× | 1.3% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 3.08s | 2.94s | 1.0× | 2.3% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.66s | 2.79s | 1.1× | 1.4% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.60s | 2.71s | 1.0× | 1.0% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 473.40ms | 700.59ms | 1.5× | 1.9% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.44s | 1.40s | 1.0× | 5.0% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 656.27ms | 980.82ms | 1.5× | 1.7% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 1.13s | 1.24s | 1.1× | 2.8% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.39s | 2.58s | 1.1× | 1.4% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.71s | 2.73s | 1.0× | 2.1% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.58s | 2.79s | 1.1× | 1.4% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.71s | 2.76s | 1.0× | 1.1% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 137.77ms | 562.54ms | 4.1× | 5.8% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 328.42ms | 921.22ms | 2.8× | 19.2% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 139.81ms | 578.00ms | 4.1× | 7.6% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 217.94ms | 878.00ms | 4.0× | 13.9% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 22.13ms | 25.60ms | 1.2× | 1.6% | PASS |
| mem_reads | `oltp_range_select` | 10.20ms | 10.92ms | 1.1× | 2.6% | PASS |
| mem_reads | `oltp_sum_range` | 8.93ms | 10.61ms | 1.2× | 3.2% | PASS |
| mem_reads | `oltp_order_range` | 2.36ms | 2.61ms | 1.1× | 0.6% | PASS |
| mem_reads | `oltp_distinct_range` | 3.17ms | 3.67ms | 1.2× | 0.8% | PASS |
| mem_reads | `oltp_index_scan` | 3.40ms | 4.58ms | 1.3× | 1.9% | PASS |
| mem_reads | `select_random_points` | 9.28ms | 10.55ms | 1.1× | 1.5% | PASS |
| mem_reads | `select_random_ranges` | 3.94ms | 3.39ms | 0.9× | 0.8% | PASS |
| mem_reads | `covering_index_scan` | 6.47ms | 3.73ms | 0.6× | 1.9% | PASS |
| mem_reads | `groupby_scan` | 27.40ms | 31.63ms | 1.2× | 0.5% | PASS |
| mem_reads | `index_join` | 4.89ms | 7.40ms | 1.5× | 1.5% | PASS |
| mem_reads | `index_join_scan` | 2.67ms | 4.20ms | 1.6× | 1.6% | PASS |
| mem_reads | `types_table_scan` | 983.75ms | 1.10s | 1.1× | 0.9% | PASS |
| mem_reads | `table_scan` | 1.16s | 1.22s | 1.1× | 0.7% | PASS |
| mem_reads | `oltp_read_only` | 94.02ms | 109.42ms | 1.2× | 1.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 155.55ms | 232.49ms | 1.5× | 1.8% | PASS |
| mem_writes | `oltp_insert` | 13.19ms | 24.30ms | 1.8× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 44.90ms | 88.79ms | 2.0× | 1.6% | PASS |
| mem_writes | `oltp_update_non_index` | 31.36ms | 52.07ms | 1.7× | 1.5% | PASS |
| mem_writes | `oltp_delete_insert` | 39.42ms | 64.69ms | 1.6× | 1.3% | PASS |
| mem_writes | `oltp_write_only` | 18.46ms | 39.67ms | 2.1× | 1.1% | PASS |
| mem_writes | `types_delete_insert` | 21.11ms | 32.68ms | 1.5× | 1.5% | PASS |
| mem_writes | `oltp_read_write` | 59.79ms | 100.41ms | 1.7× | 1.4% | PASS |
| file_reads | `oltp_point_select` | 51.38ms | 34.55ms | 0.7× | 1.6% | PASS |
| file_reads | `oltp_range_select` | 12.26ms | 11.54ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_sum_range` | 11.96ms | 11.56ms | 1.0× | 1.6% | PASS |
| file_reads | `oltp_order_range` | 2.77ms | 2.77ms | 1.0× | 1.4% | PASS |
| file_reads | `oltp_distinct_range` | 3.59ms | 3.84ms | 1.1× | 1.0% | PASS |
| file_reads | `oltp_index_scan` | 6.56ms | 5.83ms | 0.9× | 1.7% | PASS |
| file_reads | `select_random_points` | 12.70ms | 11.72ms | 0.9× | 2.1% | PASS |
| file_reads | `select_random_ranges` | 7.21ms | 4.33ms | 0.6× | 1.6% | PASS |
| file_reads | `covering_index_scan` | 9.82ms | 4.90ms | 0.5× | 1.2% | PASS |
| file_reads | `groupby_scan` | 27.77ms | 31.86ms | 1.1× | 0.6% | PASS |
| file_reads | `index_join` | 6.78ms | 8.41ms | 1.2× | 0.9% | PASS |
| file_reads | `index_join_scan` | 3.11ms | 4.32ms | 1.4× | 1.0% | PASS |
| file_reads | `types_table_scan` | 975.02ms | 1.09s | 1.1× | 1.1% | PASS |
| file_reads | `table_scan` | 1.16s | 1.21s | 1.0× | 1.0% | PASS |
| file_reads | `oltp_read_only` | 137.54ms | 123.67ms | 0.9× | 1.3% | PASS |
| file_writes | `oltp_bulk_insert` | 167.06ms | 242.81ms | 1.5× | 1.8% | PASS |
| file_writes | `oltp_insert` | 17.63ms | 27.55ms | 1.6× | 1.8% | PASS |
| file_writes | `oltp_update_index` | 59.73ms | 98.91ms | 1.7× | 1.9% | PASS |
| file_writes | `oltp_update_non_index` | 42.76ms | 60.88ms | 1.4× | 1.4% | PASS |
| file_writes | `oltp_delete_insert` | 51.25ms | 75.11ms | 1.5× | 2.1% | PASS |
| file_writes | `oltp_write_only` | 31.05ms | 47.95ms | 1.5× | 2.8% | PASS |
| file_writes | `types_delete_insert` | 30.49ms | 37.79ms | 1.2× | 1.5% | PASS |
| file_writes | `oltp_read_write` | 73.44ms | 109.58ms | 1.5× | 2.0% | PASS |
| ac_reads | `oltp_point_select` | 31.00ms | 34.90ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_range_select` | 10.41ms | 11.63ms | 1.1× | 2.1% | PASS |
| ac_reads | `oltp_sum_range` | 9.73ms | 11.58ms | 1.2× | 1.4% | PASS |
| ac_reads | `oltp_order_range` | 2.54ms | 2.77ms | 1.1× | 1.6% | PASS |
| ac_reads | `oltp_distinct_range` | 3.37ms | 3.86ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 4.80ms | 6.04ms | 1.3× | 1.5% | PASS |
| ac_reads | `select_random_points` | 10.76ms | 11.78ms | 1.1× | 1.9% | PASS |
| ac_reads | `select_random_ranges` | 5.05ms | 4.34ms | 0.9× | 1.6% | PASS |
| ac_reads | `covering_index_scan` | 7.69ms | 4.95ms | 0.6× | 1.5% | PASS |
| ac_reads | `groupby_scan` | 27.55ms | 31.94ms | 1.2× | 0.8% | PASS |
| ac_reads | `index_join` | 5.76ms | 8.57ms | 1.5× | 1.6% | PASS |
| ac_reads | `index_join_scan` | 2.88ms | 4.32ms | 1.5× | 1.2% | PASS |
| ac_reads | `types_table_scan` | 1.00s | 1.10s | 1.1× | 0.8% | PASS |
| ac_reads | `table_scan` | 1.16s | 1.22s | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_read_only` | 108.98ms | 124.73ms | 1.1× | 0.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 15.11ms | 55.45ms | 3.7× | 7.2% | PASS |
| ac_writes | `oltp_insert_ac` | 16.79ms | 68.70ms | 4.1× | 4.7% | PASS |
| ac_writes | `oltp_update_index_ac` | 19.32ms | 83.36ms | 4.3× | 5.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 17.11ms | 66.04ms | 3.9× | 5.2% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 17.51ms | 75.06ms | 4.3× | 5.9% | PASS |
| ac_writes | `oltp_write_only_ac` | 16.66ms | 72.28ms | 4.3× | 7.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 15.10ms | 63.79ms | 4.2× | 7.1% | PASS |
| ac_writes | `oltp_read_write_ac` | 20.18ms | 77.86ms | 3.9× | 4.7% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 28.74ms | 29.41ms | 1.0× | 5.2% | PASS |
| mem_reads | `oltp_range_select` | 14.20ms | 11.51ms | 0.8× | 3.2% | PASS |
| mem_reads | `oltp_sum_range` | 12.35ms | 11.06ms | 0.9× | 2.6% | PASS |
| mem_reads | `oltp_order_range` | 2.80ms | 2.51ms | 0.9× | 2.0% | PASS |
| mem_reads | `oltp_distinct_range` | 3.88ms | 3.49ms | 0.9× | 7.1% | PASS |
| mem_reads | `oltp_index_scan` | 3.47ms | 5.09ms | 1.5× | 5.5% | PASS |
| mem_reads | `select_random_points` | 18.45ms | 17.21ms | 0.9× | 4.8% | PASS |
| mem_reads | `select_random_ranges` | 5.89ms | 4.53ms | 0.8× | 4.7% | PASS |
| mem_reads | `covering_index_scan` | 6.95ms | 4.11ms | 0.6× | 4.0% | PASS |
| mem_reads | `groupby_scan` | 34.40ms | 34.03ms | 1.0× | 7.7% | PASS |
| mem_reads | `index_join` | 10.06ms | 8.28ms | 0.8× | 4.8% | PASS |
| mem_reads | `index_join_scan` | 3.36ms | 5.32ms | 1.6× | 3.6% | PASS |
| mem_reads | `types_table_scan` | 960.63ms | 1.01s | 1.0× | 0.8% | PASS |
| mem_reads | `table_scan` | 1.05s | 1.07s | 1.0× | 0.8% | PASS |
| mem_reads | `oltp_read_only` | 109.87ms | 103.76ms | 0.9× | 1.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 193.56ms | 271.92ms | 1.4× | 2.9% | PASS |
| mem_writes | `oltp_insert` | 15.83ms | 30.01ms | 1.9× | 7.0% | PASS |
| mem_writes | `oltp_update_index` | 57.80ms | 118.44ms | 2.0× | 8.6% | PASS |
| mem_writes | `oltp_update_non_index` | 40.74ms | 65.18ms | 1.6× | 4.7% | PASS |
| mem_writes | `oltp_delete_insert` | 48.84ms | 90.01ms | 1.8× | 5.2% | PASS |
| mem_writes | `oltp_write_only` | 28.09ms | 57.47ms | 2.0× | 1.9% | PASS |
| mem_writes | `types_delete_insert` | 39.73ms | 49.41ms | 1.2× | 2.5% | PASS |
| mem_writes | `oltp_read_write` | 99.42ms | 140.82ms | 1.4× | 2.3% | PASS |
| file_reads | `oltp_point_select` | 119.37ms | 56.53ms | 0.5× | 1.4% | PASS |
| file_reads | `oltp_range_select` | 26.60ms | 16.23ms | 0.6× | 3.7% | PASS |
| file_reads | `oltp_sum_range` | 24.79ms | 15.80ms | 0.6× | 2.1% | PASS |
| file_reads | `oltp_order_range` | 4.54ms | 3.48ms | 0.8× | 2.3% | PASS |
| file_reads | `oltp_distinct_range` | 5.76ms | 4.70ms | 0.8× | 2.3% | PASS |
| file_reads | `oltp_index_scan` | 12.79ms | 8.61ms | 0.7× | 2.3% | PASS |
| file_reads | `select_random_points` | 32.21ms | 23.35ms | 0.7× | 2.0% | PASS |
| file_reads | `select_random_ranges` | 15.67ms | 7.64ms | 0.5× | 2.0% | PASS |
| file_reads | `covering_index_scan` | 16.44ms | 7.22ms | 0.4× | 2.6% | PASS |
| file_reads | `groupby_scan` | 37.90ms | 37.28ms | 1.0× | 2.0% | PASS |
| file_reads | `index_join` | 16.43ms | 11.51ms | 0.7× | 2.3% | PASS |
| file_reads | `index_join_scan` | 5.17ms | 6.95ms | 1.3× | 2.9% | PASS |
| file_reads | `types_table_scan` | 1.17s | 1.23s | 1.1× | 0.9% | PASS |
| file_reads | `table_scan` | 1.34s | 1.35s | 1.0× | 0.8% | PASS |
| file_reads | `oltp_read_only` | 260.34ms | 161.68ms | 0.6× | 2.3% | PASS |
| file_writes | `oltp_bulk_insert` | 320.93ms | 389.23ms | 1.2× | 5.7% | PASS |
| file_writes | `oltp_insert` | 39.65ms | 59.53ms | 1.5× | 6.8% | PASS |
| file_writes | `oltp_update_index` | 193.87ms | 225.08ms | 1.2× | 3.2% | PASS |
| file_writes | `oltp_update_non_index` | 169.35ms | 146.19ms | 0.9× | 2.6% | PASS |
| file_writes | `oltp_delete_insert` | 206.15ms | 173.84ms | 0.8× | 3.9% | PASS |
| file_writes | `oltp_write_only` | 135.43ms | 112.95ms | 0.8× | 6.3% | PASS |
| file_writes | `types_delete_insert` | 164.03ms | 95.25ms | 0.6× | 9.3% | PASS |
| file_writes | `oltp_read_write` | 209.32ms | 197.13ms | 0.9× | 4.3% | PASS |
| ac_reads | `oltp_point_select` | 63.61ms | 56.56ms | 0.9× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 21.16ms | 16.07ms | 0.8× | 1.3% | PASS |
| ac_reads | `oltp_sum_range` | 18.94ms | 15.60ms | 0.8× | 2.1% | PASS |
| ac_reads | `oltp_order_range` | 4.00ms | 3.47ms | 0.9× | 2.6% | PASS |
| ac_reads | `oltp_distinct_range` | 5.08ms | 4.62ms | 0.9× | 3.3% | PASS |
| ac_reads | `oltp_index_scan` | 7.22ms | 8.42ms | 1.2× | 2.5% | PASS |
| ac_reads | `select_random_points` | 26.57ms | 22.93ms | 0.9× | 1.9% | PASS |
| ac_reads | `select_random_ranges` | 10.05ms | 7.46ms | 0.7× | 1.6% | PASS |
| ac_reads | `covering_index_scan` | 10.60ms | 6.86ms | 0.6× | 5.5% | PASS |
| ac_reads | `groupby_scan` | 35.10ms | 34.07ms | 1.0× | 6.5% | PASS |
| ac_reads | `index_join` | 13.31ms | 10.97ms | 0.8× | 2.2% | PASS |
| ac_reads | `index_join_scan` | 4.53ms | 6.58ms | 1.5× | 2.4% | PASS |
| ac_reads | `types_table_scan` | 1.06s | 1.12s | 1.1× | 1.4% | PASS |
| ac_reads | `table_scan` | 1.25s | 1.26s | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_read_only` | 178.67ms | 159.13ms | 0.9× | 2.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 40.48ms | 106.72ms | 2.6× | 38.0% | PASS |
| ac_writes | `oltp_insert_ac` | 38.28ms | 102.95ms | 2.7× | 7.2% | PASS |
| ac_writes | `oltp_update_index_ac` | 42.65ms | 127.94ms | 3.0× | 19.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 37.41ms | 114.15ms | 3.1× | 24.1% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 41.90ms | 116.23ms | 2.8× | 18.7% | PASS |
| ac_writes | `oltp_write_only_ac` | 40.57ms | 115.00ms | 2.8× | 13.5% | PASS |
| ac_writes | `types_delete_insert_ac` | 37.61ms | 109.14ms | 2.9× | 14.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 49.52ms | 129.10ms | 2.6× | 21.8% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 32.01ms | 33.99ms | 1.1× | 1.7% | PASS |
| mem_reads | `oltp_range_select` | 13.98ms | 12.73ms | 0.9× | 1.4% | PASS |
| mem_reads | `oltp_sum_range` | 13.21ms | 12.68ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_order_range` | 2.83ms | 2.91ms | 1.0× | 0.6% | PASS |
| mem_reads | `oltp_distinct_range` | 3.66ms | 3.99ms | 1.1× | 0.5% | PASS |
| mem_reads | `oltp_index_scan` | 3.48ms | 5.45ms | 1.6× | 0.7% | PASS |
| mem_reads | `select_random_points` | 19.68ms | 20.29ms | 1.0× | 1.4% | PASS |
| mem_reads | `select_random_ranges` | 5.58ms | 4.57ms | 0.8× | 0.9% | PASS |
| mem_reads | `covering_index_scan` | 6.56ms | 4.17ms | 0.6× | 1.0% | PASS |
| mem_reads | `groupby_scan` | 30.88ms | 33.12ms | 1.1× | 0.5% | PASS |
| mem_reads | `index_join` | 9.50ms | 8.93ms | 0.9× | 2.1% | PASS |
| mem_reads | `index_join_scan` | 3.06ms | 5.12ms | 1.7× | 1.3% | PASS |
| mem_reads | `types_table_scan` | 1.03s | 1.16s | 1.1× | 0.6% | PASS |
| mem_reads | `table_scan` | 1.21s | 1.29s | 1.1× | 0.7% | PASS |
| mem_reads | `oltp_read_only` | 128.58ms | 132.53ms | 1.0× | 0.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 206.23ms | 321.70ms | 1.6× | 1.3% | PASS |
| mem_writes | `oltp_insert` | 16.33ms | 35.14ms | 2.2× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 61.75ms | 135.42ms | 2.2× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 45.13ms | 76.20ms | 1.7× | 1.3% | PASS |
| mem_writes | `oltp_delete_insert` | 48.94ms | 96.81ms | 2.0× | 1.4% | PASS |
| mem_writes | `oltp_write_only` | 25.94ms | 56.25ms | 2.2× | 1.4% | PASS |
| mem_writes | `types_delete_insert` | 34.47ms | 47.58ms | 1.4× | 1.2% | PASS |
| mem_writes | `oltp_read_write` | 89.00ms | 131.91ms | 1.5× | 1.1% | PASS |
| file_reads | `oltp_point_select` | 65.42ms | 44.93ms | 0.7× | 1.1% | PASS |
| file_reads | `oltp_range_select` | 17.72ms | 14.30ms | 0.8× | 1.4% | PASS |
| file_reads | `oltp_sum_range` | 17.17ms | 14.31ms | 0.8× | 0.9% | PASS |
| file_reads | `oltp_order_range` | 3.29ms | 3.11ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_distinct_range` | 4.09ms | 4.24ms | 1.0× | 1.9% | PASS |
| file_reads | `oltp_index_scan` | 6.91ms | 6.78ms | 1.0× | 1.6% | PASS |
| file_reads | `select_random_points` | 24.19ms | 22.03ms | 0.9× | 2.0% | PASS |
| file_reads | `select_random_ranges` | 9.01ms | 5.71ms | 0.6× | 1.3% | PASS |
| file_reads | `covering_index_scan` | 9.98ms | 5.23ms | 0.5× | 1.7% | PASS |
| file_reads | `groupby_scan` | 31.31ms | 33.42ms | 1.1× | 0.7% | PASS |
| file_reads | `index_join` | 11.79ms | 9.75ms | 0.8× | 2.4% | PASS |
| file_reads | `index_join_scan` | 3.59ms | 5.54ms | 1.5× | 3.2% | PASS |
| file_reads | `types_table_scan` | 1.05s | 1.17s | 1.1× | 0.4% | PASS |
| file_reads | `table_scan` | 1.23s | 1.30s | 1.1× | 0.7% | PASS |
| file_reads | `oltp_read_only` | 176.56ms | 149.88ms | 0.8× | 1.6% | PASS |
| file_writes | `oltp_bulk_insert` | 222.61ms | 333.42ms | 1.5× | 1.4% | PASS |
| file_writes | `oltp_insert` | 21.42ms | 41.66ms | 1.9× | 2.2% | PASS |
| file_writes | `oltp_update_index` | 82.12ms | 152.03ms | 1.9× | 2.2% | PASS |
| file_writes | `oltp_update_non_index` | 66.07ms | 87.16ms | 1.3× | 9.0% | PASS |
| file_writes | `oltp_delete_insert` | 67.60ms | 105.60ms | 1.6× | 1.6% | PASS |
| file_writes | `oltp_write_only` | 40.21ms | 63.50ms | 1.6× | 1.8% | PASS |
| file_writes | `types_delete_insert` | 49.21ms | 55.27ms | 1.1× | 1.6% | PASS |
| file_writes | `oltp_read_write` | 107.03ms | 142.18ms | 1.3× | 1.5% | PASS |
| ac_reads | `oltp_point_select` | 43.66ms | 45.26ms | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_range_select` | 15.00ms | 14.14ms | 0.9× | 2.3% | PASS |
| ac_reads | `oltp_sum_range` | 14.34ms | 14.08ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_order_range` | 3.00ms | 3.12ms | 1.0× | 2.0% | PASS |
| ac_reads | `oltp_distinct_range` | 3.82ms | 4.22ms | 1.1× | 1.5% | PASS |
| ac_reads | `oltp_index_scan` | 4.76ms | 6.68ms | 1.4× | 1.0% | PASS |
| ac_reads | `select_random_points` | 21.56ms | 22.02ms | 1.0× | 1.8% | PASS |
| ac_reads | `select_random_ranges` | 6.89ms | 5.73ms | 0.8× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 7.80ms | 5.30ms | 0.7× | 1.3% | PASS |
| ac_reads | `groupby_scan` | 31.18ms | 33.51ms | 1.1× | 0.5% | PASS |
| ac_reads | `index_join` | 11.26ms | 10.19ms | 0.9× | 2.0% | PASS |
| ac_reads | `index_join_scan` | 3.42ms | 5.71ms | 1.7× | 2.7% | PASS |
| ac_reads | `types_table_scan` | 1.05s | 1.17s | 1.1× | 0.6% | PASS |
| ac_reads | `table_scan` | 1.22s | 1.30s | 1.1× | 0.5% | PASS |
| ac_reads | `oltp_read_only` | 143.21ms | 148.82ms | 1.0× | 0.7% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 16.26ms | 61.39ms | 3.8× | 9.0% | PASS |
| ac_writes | `oltp_insert_ac` | 18.48ms | 76.19ms | 4.1× | 8.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 18.55ms | 86.18ms | 4.6× | 8.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 15.43ms | 65.32ms | 4.2× | 5.7% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 17.34ms | 70.47ms | 4.1× | 7.0% | PASS |
| ac_writes | `oltp_write_only_ac` | 16.44ms | 71.46ms | 4.3× | 6.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 15.30ms | 64.78ms | 4.2× | 7.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 21.99ms | 82.20ms | 3.7× | 7.7% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 28.77ms | 33.67ms | 1.2× | 0.9% | PASS |
| mem_reads | `oltp_range_select` | 17.06ms | 18.63ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_sum_range` | 16.09ms | 18.16ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_order_range` | 3.34ms | 3.53ms | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_distinct_range` | 4.29ms | 4.60ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_index_scan` | 4.06ms | 5.12ms | 1.3× | 1.0% | PASS |
| mem_reads | `select_random_points` | 26.47ms | 29.77ms | 1.1× | 0.8% | PASS |
| mem_reads | `select_random_ranges` | 6.68ms | 7.76ms | 1.2× | 1.2% | PASS |
| mem_reads | `covering_index_scan` | 6.23ms | 3.55ms | 0.6× | 1.2% | PASS |
| mem_reads | `groupby_scan` | 34.87ms | 37.36ms | 1.1× | 0.9% | PASS |
| mem_reads | `index_join` | 7.12ms | 9.37ms | 1.3× | 1.2% | PASS |
| mem_reads | `index_join_scan` | 3.47ms | 5.12ms | 1.5× | 1.6% | PASS |
| mem_reads | `types_table_scan` | 1.03s | 1.12s | 1.1× | 0.6% | PASS |
| mem_reads | `table_scan` | 1.44s | 1.31s | 0.9× | 1.8% | PASS |
| mem_reads | `oltp_read_only` | 131.80ms | 149.07ms | 1.1× | 0.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 203.92ms | 278.40ms | 1.4× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 16.50ms | 28.41ms | 1.7× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 58.31ms | 111.57ms | 1.9× | 0.9% | PASS |
| mem_writes | `oltp_update_non_index` | 42.46ms | 70.58ms | 1.7× | 0.9% | PASS |
| mem_writes | `oltp_delete_insert` | 42.56ms | 80.22ms | 1.9× | 1.2% | PASS |
| mem_writes | `oltp_write_only` | 22.87ms | 48.41ms | 2.1× | 1.0% | PASS |
| mem_writes | `types_delete_insert` | 26.45ms | 42.87ms | 1.6× | 1.3% | PASS |
| mem_writes | `oltp_read_write` | 82.94ms | 130.16ms | 1.6× | 0.9% | PASS |
| file_reads | `oltp_point_select` | 58.64ms | 40.69ms | 0.7× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 20.49ms | 19.90ms | 1.0× | 0.9% | PASS |
| file_reads | `oltp_sum_range` | 19.79ms | 19.11ms | 1.0× | 1.2% | PASS |
| file_reads | `oltp_order_range` | 3.75ms | 3.64ms | 1.0× | 1.5% | PASS |
| file_reads | `oltp_distinct_range` | 4.69ms | 4.69ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 7.60ms | 6.43ms | 0.8× | 1.1% | PASS |
| file_reads | `select_random_points` | 33.03ms | 33.37ms | 1.0× | 1.4% | PASS |
| file_reads | `select_random_ranges` | 10.11ms | 8.78ms | 0.9× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 10.07ms | 4.92ms | 0.5× | 0.9% | PASS |
| file_reads | `groupby_scan` | 35.46ms | 37.39ms | 1.1× | 0.9% | PASS |
| file_reads | `index_join` | 9.18ms | 10.69ms | 1.2× | 1.2% | PASS |
| file_reads | `index_join_scan` | 4.00ms | 5.42ms | 1.4× | 1.3% | PASS |
| file_reads | `types_table_scan` | 1.04s | 1.14s | 1.1× | 0.8% | PASS |
| file_reads | `table_scan` | 1.18s | 1.22s | 1.0× | 0.5% | PASS |
| file_reads | `oltp_read_only` | 171.22ms | 156.63ms | 0.9× | 0.7% | PASS |
| file_writes | `oltp_bulk_insert` | 260.15ms | 337.27ms | 1.3× | 2.7% | PASS |
| file_writes | `oltp_insert` | 33.71ms | 52.67ms | 1.6× | 2.9% | PASS |
| file_writes | `oltp_update_index` | 179.55ms | 193.73ms | 1.1× | 2.8% | PASS |
| file_writes | `oltp_update_non_index` | 146.20ms | 129.90ms | 0.9× | 1.9% | PASS |
| file_writes | `oltp_delete_insert` | 144.94ms | 145.78ms | 1.0× | 1.5% | PASS |
| file_writes | `oltp_write_only` | 104.38ms | 101.23ms | 1.0× | 1.2% | PASS |
| file_writes | `types_delete_insert` | 94.33ms | 86.88ms | 0.9× | 5.6% | PASS |
| file_writes | `oltp_read_write` | 168.50ms | 188.27ms | 1.1× | 2.9% | PASS |
| ac_reads | `oltp_point_select` | 36.88ms | 40.42ms | 1.1× | 0.6% | PASS |
| ac_reads | `oltp_range_select` | 17.98ms | 19.70ms | 1.1× | 0.8% | PASS |
| ac_reads | `oltp_sum_range` | 17.32ms | 19.14ms | 1.1× | 1.0% | PASS |
| ac_reads | `oltp_order_range` | 3.50ms | 3.65ms | 1.0× | 2.0% | PASS |
| ac_reads | `oltp_distinct_range` | 4.41ms | 4.64ms | 1.1× | 1.9% | PASS |
| ac_reads | `oltp_index_scan` | 5.32ms | 6.20ms | 1.2× | 1.7% | PASS |
| ac_reads | `select_random_points` | 27.73ms | 30.70ms | 1.1× | 1.1% | PASS |
| ac_reads | `select_random_ranges` | 7.72ms | 8.61ms | 1.1× | 1.1% | PASS |
| ac_reads | `covering_index_scan` | 7.63ms | 4.80ms | 0.6× | 1.6% | PASS |
| ac_reads | `groupby_scan` | 34.99ms | 37.21ms | 1.1× | 0.7% | PASS |
| ac_reads | `index_join` | 8.04ms | 10.56ms | 1.3× | 1.2% | PASS |
| ac_reads | `index_join_scan` | 3.77ms | 5.34ms | 1.4× | 1.2% | PASS |
| ac_reads | `types_table_scan` | 1.02s | 1.12s | 1.1× | 0.6% | PASS |
| ac_reads | `table_scan` | 1.37s | 1.29s | 0.9× | 4.3% | PASS |
| ac_reads | `oltp_read_only` | 142.50ms | 158.27ms | 1.1× | 0.7% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 23.80ms | 88.20ms | 3.7× | 15.0% | PASS |
| ac_writes | `oltp_insert_ac` | 26.79ms | 107.66ms | 4.0× | 8.6% | PASS |
| ac_writes | `oltp_update_index_ac` | 29.48ms | 118.41ms | 4.0× | 20.4% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 25.14ms | 105.80ms | 4.2× | 12.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.56ms | 116.53ms | 4.4× | 15.4% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.80ms | 114.29ms | 4.3× | 13.4% | PASS |
| ac_writes | `types_delete_insert_ac` | 26.32ms | 108.62ms | 4.1× | 13.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 33.06ms | 118.49ms | 3.6× | 14.2% | PASS |

</details>

</details>

## Version-control latency

Wall time: 2m 13s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 64.07ms | 130.00ms | 49.3% | 0.7% | PASS |
| `status_dirty_many_tables` | 67.48ms | 130.00ms | 51.9% | 0.6% | PASS |
| `diff_regular_working_one_table` | 60.24ms | 120.00ms | 50.2% | 0.4% | PASS |
| `diff_regular_working_many_tables` | 70.88ms | 140.00ms | 50.6% | 0.5% | PASS |
| `diff_stat_working_many_tables` | 71.34ms | 140.00ms | 51.0% | 0.4% | PASS |
| `diff_schema_working_many_tables` | 72.06ms | 140.00ms | 51.5% | 0.3% | PASS |
| `branch_list_many_branches` | 19.55ms | 35.00ms | 55.9% | 2.1% | PASS |
| `branch_create_delete` | 26.98ms | 40.00ms | 67.4% | 3.7% | PASS |
| `at_literal_deep_history` | 29.88ms | 100.00ms | 29.9% | 1.1% | PASS |
| `diff_literal_deep_history` | 29.67ms | 120.00ms | 24.7% | 1.4% | PASS |
| `history_literal_deep_history` | 29.71ms | 150.00ms | 19.8% | 1.3% | PASS |
| `checkout_branch_clean` | 96.51ms | 150.00ms | 64.3% | 5.2% | PASS |
| `merge_data_no_conflicts` | 32.83ms | 50.00ms | 65.7% | 4.7% | PASS |
| `merge_schema_no_conflicts` | 18.85ms | 35.00ms | 53.9% | 2.9% | PASS |
| `merge_data_conflicts` | 25.50ms | 180.00ms | 14.2% | 2.1% | PASS |
| `merge_data_conflicts_with_resolve` | 25.61ms | 180.00ms | 14.2% | 1.5% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
