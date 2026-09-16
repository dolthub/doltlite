# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-09-16 11:12 UTC
>
> Commit: [`5b71c6259626f4d4b19e4470c497efbd6726967d`](https://github.com/dolthub/doltlite/commit/5b71c6259626f4d4b19e4470c497efbd6726967d)
>
> Runner: ubuntu24 20260907.300.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/35080349151)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.27s | 10.91s | 1.1× | 1.3% | **PASS** |
| Writes | 2.07s | 3.41s | 1.6× | 1.2% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 11.27s | 11.24s | 1.0× | 1.4% | **PASS** |
| Writes | 3.33s | 4.16s | 1.2× | 2.0% | **PASS** |
| Autocommit writes | 766.52ms | 2.87s | 3.7× | 6.8% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.30s | 2.48s | 1.1× | 0.8% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.77s | 2.80s | 1.0× | 2.4% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.51s | 2.79s | 1.1× | 1.1% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.69s | 2.84s | 1.1× | 1.6% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 355.10ms | 561.41ms | 1.6× | 0.7% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 608.51ms | 1.02s | 1.7× | 1.7% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 512.62ms | 882.26ms | 1.7× | 1.4% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 593.49ms | 954.59ms | 1.6× | 1.2% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.40s | 2.51s | 1.0× | 0.8% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 3.36s | 2.98s | 0.9× | 1.4% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.65s | 2.86s | 1.1× | 1.5% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.87s | 2.90s | 1.0× | 1.4% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 922.73ms | 955.33ms | 1.0× | 2.4% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.01s | 1.18s | 1.2× | 3.0% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 643.90ms | 980.43ms | 1.5× | 1.9% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 759.52ms | 1.05s | 1.4× | 1.8% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.34s | 2.51s | 1.1× | 0.9% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.91s | 2.88s | 1.0× | 2.0% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.57s | 2.85s | 1.1× | 1.3% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.69s | 2.88s | 1.1× | 1.6% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 202.68ms | 802.98ms | 4.0× | 11.1% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 215.33ms | 750.73ms | 3.5× | 6.5% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 142.71ms | 557.32ms | 3.9× | 6.2% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 205.81ms | 758.91ms | 3.7× | 5.5% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 20.19ms | 22.84ms | 1.1× | 0.5% | PASS |
| mem_reads | `oltp_range_select` | 9.06ms | 9.46ms | 1.0× | 0.9% | PASS |
| mem_reads | `oltp_sum_range` | 8.29ms | 9.25ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_order_range` | 2.40ms | 2.52ms | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_distinct_range` | 3.28ms | 3.62ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 3.37ms | 4.06ms | 1.2× | 1.0% | PASS |
| mem_reads | `select_random_points` | 9.61ms | 9.92ms | 1.0× | 0.9% | PASS |
| mem_reads | `select_random_ranges` | 3.86ms | 3.93ms | 1.0× | 0.5% | PASS |
| mem_reads | `covering_index_scan` | 6.20ms | 7.37ms | 1.2× | 0.6% | PASS |
| mem_reads | `groupby_scan` | 28.73ms | 31.38ms | 1.1× | 0.5% | PASS |
| mem_reads | `index_join` | 5.04ms | 6.99ms | 1.4× | 0.8% | PASS |
| mem_reads | `index_join_scan` | 2.74ms | 4.04ms | 1.5× | 0.9% | PASS |
| mem_reads | `types_table_scan` | 984.06ms | 1.07s | 1.1× | 0.2% | PASS |
| mem_reads | `table_scan` | 1.13s | 1.19s | 1.1× | 0.5% | PASS |
| mem_reads | `oltp_read_only` | 89.97ms | 101.13ms | 1.1× | 0.7% | PASS |
| mem_writes | `oltp_bulk_insert` | 141.62ms | 205.68ms | 1.5× | 0.5% | PASS |
| mem_writes | `oltp_insert` | 12.56ms | 22.16ms | 1.8× | 0.4% | PASS |
| mem_writes | `oltp_update_index` | 42.51ms | 76.03ms | 1.8× | 0.8% | PASS |
| mem_writes | `oltp_update_non_index` | 28.40ms | 44.19ms | 1.6× | 0.9% | PASS |
| mem_writes | `oltp_delete_insert` | 38.07ms | 58.90ms | 1.5× | 0.8% | PASS |
| mem_writes | `oltp_write_only` | 18.05ms | 36.17ms | 2.0× | 0.7% | PASS |
| mem_writes | `types_delete_insert` | 20.18ms | 29.65ms | 1.5× | 0.6% | PASS |
| mem_writes | `oltp_read_write` | 53.72ms | 88.63ms | 1.6× | 0.8% | PASS |
| file_reads | `oltp_point_select` | 50.07ms | 30.49ms | 0.6× | 0.5% | PASS |
| file_reads | `oltp_range_select` | 12.48ms | 10.70ms | 0.9× | 0.6% | PASS |
| file_reads | `oltp_sum_range` | 12.05ms | 10.49ms | 0.9× | 0.8% | PASS |
| file_reads | `oltp_order_range` | 2.81ms | 2.71ms | 1.0× | 1.1% | PASS |
| file_reads | `oltp_distinct_range` | 3.69ms | 3.80ms | 1.0× | 1.0% | PASS |
| file_reads | `oltp_index_scan` | 6.70ms | 5.36ms | 0.8× | 1.5% | PASS |
| file_reads | `select_random_points` | 13.36ms | 11.39ms | 0.9× | 1.0% | PASS |
| file_reads | `select_random_ranges` | 7.05ms | 4.86ms | 0.7× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 9.65ms | 8.51ms | 0.9× | 0.5% | PASS |
| file_reads | `groupby_scan` | 29.08ms | 31.83ms | 1.1× | 0.6% | PASS |
| file_reads | `index_join` | 6.93ms | 8.21ms | 1.2× | 1.9% | PASS |
| file_reads | `index_join_scan` | 3.15ms | 4.40ms | 1.4× | 1.3% | PASS |
| file_reads | `types_table_scan` | 987.67ms | 1.07s | 1.1× | 0.3% | PASS |
| file_reads | `table_scan` | 1.12s | 1.19s | 1.1× | 0.5% | PASS |
| file_reads | `oltp_read_only` | 132.54ms | 111.98ms | 0.8× | 0.6% | PASS |
| file_writes | `oltp_bulk_insert` | 194.70ms | 266.71ms | 1.4× | 1.5% | PASS |
| file_writes | `oltp_insert` | 26.89ms | 38.59ms | 1.4× | 3.9% | PASS |
| file_writes | `oltp_update_index` | 147.00ms | 143.58ms | 1.0× | 7.9% | PASS |
| file_writes | `oltp_update_non_index` | 119.19ms | 97.95ms | 0.8× | 2.0% | PASS |
| file_writes | `oltp_delete_insert` | 127.80ms | 118.14ms | 0.9× | 1.3% | PASS |
| file_writes | `oltp_write_only` | 94.70ms | 85.86ms | 0.9× | 2.9% | PASS |
| file_writes | `types_delete_insert` | 81.05ms | 65.61ms | 0.8× | 7.9% | PASS |
| file_writes | `oltp_read_write` | 131.39ms | 138.89ms | 1.1× | 1.6% | PASS |
| ac_reads | `oltp_point_select` | 29.55ms | 30.42ms | 1.0× | 0.4% | PASS |
| ac_reads | `oltp_range_select` | 10.35ms | 10.68ms | 1.0× | 0.5% | PASS |
| ac_reads | `oltp_sum_range` | 9.91ms | 10.49ms | 1.1× | 0.5% | PASS |
| ac_reads | `oltp_order_range` | 2.61ms | 2.73ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_distinct_range` | 3.48ms | 3.78ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_index_scan` | 4.56ms | 5.14ms | 1.1× | 1.6% | PASS |
| ac_reads | `select_random_points` | 11.08ms | 11.26ms | 1.0× | 1.1% | PASS |
| ac_reads | `select_random_ranges` | 4.98ms | 4.81ms | 1.0× | 1.6% | PASS |
| ac_reads | `covering_index_scan` | 7.38ms | 8.42ms | 1.1× | 1.0% | PASS |
| ac_reads | `groupby_scan` | 28.81ms | 31.73ms | 1.1× | 0.5% | PASS |
| ac_reads | `index_join` | 5.92ms | 8.17ms | 1.4× | 2.1% | PASS |
| ac_reads | `index_join_scan` | 3.02ms | 4.43ms | 1.5× | 1.0% | PASS |
| ac_reads | `types_table_scan` | 986.92ms | 1.07s | 1.1× | 0.3% | PASS |
| ac_reads | `table_scan` | 1.13s | 1.19s | 1.1× | 0.7% | PASS |
| ac_reads | `oltp_read_only` | 103.86ms | 112.46ms | 1.1× | 0.4% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 23.15ms | 89.54ms | 3.9× | 13.1% | PASS |
| ac_writes | `oltp_insert_ac` | 25.36ms | 98.48ms | 3.9× | 8.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.15ms | 109.62ms | 4.0× | 9.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.16ms | 104.19ms | 4.3× | 19.5% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.03ms | 102.79ms | 3.9× | 14.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.73ms | 101.17ms | 3.9× | 11.5% | PASS |
| ac_writes | `types_delete_insert_ac` | 23.37ms | 91.39ms | 3.9× | 9.3% | PASS |
| ac_writes | `oltp_read_write_ac` | 27.73ms | 105.78ms | 3.8× | 10.7% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 37.76ms | 39.72ms | 1.1× | 2.6% | PASS |
| mem_reads | `oltp_range_select` | 17.26ms | 13.95ms | 0.8× | 2.4% | PASS |
| mem_reads | `oltp_sum_range` | 15.90ms | 14.21ms | 0.9× | 2.2% | PASS |
| mem_reads | `oltp_order_range` | 3.42ms | 3.16ms | 0.9× | 1.3% | PASS |
| mem_reads | `oltp_distinct_range` | 4.52ms | 4.35ms | 1.0× | 2.7% | PASS |
| mem_reads | `oltp_index_scan` | 4.07ms | 6.44ms | 1.6× | 2.5% | PASS |
| mem_reads | `select_random_points` | 23.53ms | 21.67ms | 0.9× | 4.1% | PASS |
| mem_reads | `select_random_ranges` | 7.07ms | 6.73ms | 1.0× | 2.3% | PASS |
| mem_reads | `covering_index_scan` | 7.99ms | 9.85ms | 1.2× | 1.5% | PASS |
| mem_reads | `groupby_scan` | 34.02ms | 34.27ms | 1.0× | 0.8% | PASS |
| mem_reads | `index_join` | 10.98ms | 9.67ms | 0.9× | 2.8% | PASS |
| mem_reads | `index_join_scan` | 3.87ms | 5.73ms | 1.5× | 2.4% | PASS |
| mem_reads | `types_table_scan` | 1.11s | 1.18s | 1.1× | 2.5% | PASS |
| mem_reads | `table_scan` | 1.34s | 1.31s | 1.0× | 1.9% | PASS |
| mem_reads | `oltp_read_only` | 145.00ms | 138.83ms | 1.0× | 3.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 242.21ms | 372.47ms | 1.5× | 1.2% | PASS |
| mem_writes | `oltp_insert` | 18.19ms | 39.32ms | 2.2× | 1.1% | PASS |
| mem_writes | `oltp_update_index` | 70.65ms | 146.86ms | 2.1× | 2.2% | PASS |
| mem_writes | `oltp_update_non_index` | 50.81ms | 83.67ms | 1.6× | 2.6% | PASS |
| mem_writes | `oltp_delete_insert` | 56.54ms | 108.18ms | 1.9× | 1.2% | PASS |
| mem_writes | `oltp_write_only` | 28.87ms | 62.45ms | 2.2× | 1.6% | PASS |
| mem_writes | `types_delete_insert` | 40.03ms | 55.93ms | 1.4× | 1.8% | PASS |
| mem_writes | `oltp_read_write` | 101.21ms | 147.61ms | 1.5× | 2.1% | PASS |
| file_reads | `oltp_point_select` | 108.12ms | 58.22ms | 0.5× | 1.4% | PASS |
| file_reads | `oltp_range_select` | 25.44ms | 16.06ms | 0.6× | 1.1% | PASS |
| file_reads | `oltp_sum_range` | 23.50ms | 16.36ms | 0.7× | 1.8% | PASS |
| file_reads | `oltp_order_range` | 4.39ms | 3.46ms | 0.8× | 1.6% | PASS |
| file_reads | `oltp_distinct_range` | 5.51ms | 4.66ms | 0.8× | 2.9% | PASS |
| file_reads | `oltp_index_scan` | 11.66ms | 8.66ms | 0.7× | 1.4% | PASS |
| file_reads | `select_random_points` | 35.31ms | 25.82ms | 0.7× | 2.7% | PASS |
| file_reads | `select_random_ranges` | 14.75ms | 8.84ms | 0.6× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 15.88ms | 12.39ms | 0.8× | 1.1% | PASS |
| file_reads | `groupby_scan` | 35.93ms | 35.44ms | 1.0× | 0.9% | PASS |
| file_reads | `index_join` | 16.43ms | 11.84ms | 0.7× | 2.5% | PASS |
| file_reads | `index_join_scan` | 5.10ms | 6.70ms | 1.3× | 4.2% | PASS |
| file_reads | `types_table_scan` | 1.30s | 1.23s | 0.9× | 0.8% | PASS |
| file_reads | `table_scan` | 1.48s | 1.36s | 0.9× | 0.6% | PASS |
| file_reads | `oltp_read_only` | 270.42ms | 179.69ms | 0.7× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 270.29ms | 396.17ms | 1.5× | 0.8% | PASS |
| file_writes | `oltp_insert` | 27.73ms | 47.97ms | 1.7× | 2.4% | PASS |
| file_writes | `oltp_update_index` | 135.96ms | 177.47ms | 1.3× | 5.2% | PASS |
| file_writes | `oltp_update_non_index` | 123.03ms | 108.87ms | 0.9× | 8.9% | PASS |
| file_writes | `oltp_delete_insert` | 103.98ms | 130.53ms | 1.3× | 2.0% | PASS |
| file_writes | `oltp_write_only` | 89.97ms | 77.74ms | 0.9× | 10.3% | PASS |
| file_writes | `types_delete_insert` | 76.41ms | 70.16ms | 0.9× | 1.3% | PASS |
| file_writes | `oltp_read_write` | 180.89ms | 168.35ms | 0.9× | 3.6% | PASS |
| ac_reads | `oltp_point_select` | 60.77ms | 57.93ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_range_select` | 20.48ms | 16.03ms | 0.8× | 2.3% | PASS |
| ac_reads | `oltp_sum_range` | 18.13ms | 16.27ms | 0.9× | 2.3% | PASS |
| ac_reads | `oltp_order_range` | 3.80ms | 3.45ms | 0.9× | 2.7% | PASS |
| ac_reads | `oltp_distinct_range` | 4.95ms | 4.64ms | 0.9× | 2.8% | PASS |
| ac_reads | `oltp_index_scan` | 6.69ms | 8.50ms | 1.3× | 1.7% | PASS |
| ac_reads | `select_random_points` | 26.59ms | 24.42ms | 0.9× | 2.7% | PASS |
| ac_reads | `select_random_ranges` | 9.25ms | 8.64ms | 0.9× | 2.0% | PASS |
| ac_reads | `covering_index_scan` | 10.45ms | 12.18ms | 1.2× | 1.8% | PASS |
| ac_reads | `groupby_scan` | 33.95ms | 34.70ms | 1.0× | 0.9% | PASS |
| ac_reads | `index_join` | 12.04ms | 11.23ms | 0.9× | 2.0% | PASS |
| ac_reads | `index_join_scan` | 4.16ms | 6.22ms | 1.5× | 3.6% | PASS |
| ac_reads | `types_table_scan` | 1.08s | 1.17s | 1.1× | 1.1% | PASS |
| ac_reads | `table_scan` | 1.43s | 1.33s | 0.9× | 0.7% | PASS |
| ac_reads | `oltp_read_only` | 185.71ms | 170.12ms | 0.9× | 1.5% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 23.50ms | 76.70ms | 3.3× | 6.2% | PASS |
| ac_writes | `oltp_insert_ac` | 26.66ms | 89.11ms | 3.3× | 5.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.83ms | 107.80ms | 3.9× | 8.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.18ms | 87.90ms | 3.6× | 7.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 27.79ms | 101.39ms | 3.6× | 6.8% | PASS |
| ac_writes | `oltp_write_only_ac` | 27.56ms | 95.61ms | 3.5× | 6.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 24.30ms | 87.39ms | 3.6× | 6.8% | PASS |
| ac_writes | `oltp_read_write_ac` | 33.52ms | 104.83ms | 3.1× | 3.5% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 31.13ms | 33.24ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_range_select` | 13.70ms | 12.74ms | 0.9× | 1.0% | PASS |
| mem_reads | `oltp_sum_range` | 13.09ms | 12.81ms | 1.0× | 1.5% | PASS |
| mem_reads | `oltp_order_range` | 2.83ms | 2.94ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 3.64ms | 4.01ms | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_index_scan` | 3.46ms | 5.37ms | 1.6× | 1.2% | PASS |
| mem_reads | `select_random_points` | 19.57ms | 20.62ms | 1.1× | 2.3% | PASS |
| mem_reads | `select_random_ranges` | 5.59ms | 5.90ms | 1.1× | 1.1% | PASS |
| mem_reads | `covering_index_scan` | 6.53ms | 8.81ms | 1.3× | 0.9% | PASS |
| mem_reads | `groupby_scan` | 30.83ms | 33.18ms | 1.1× | 0.6% | PASS |
| mem_reads | `index_join` | 9.54ms | 9.04ms | 0.9× | 2.2% | PASS |
| mem_reads | `index_join_scan` | 3.07ms | 5.60ms | 1.8× | 2.6% | PASS |
| mem_reads | `types_table_scan` | 1.04s | 1.20s | 1.2× | 0.6% | PASS |
| mem_reads | `table_scan` | 1.20s | 1.30s | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_read_only` | 122.79ms | 128.63ms | 1.0× | 1.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 204.55ms | 321.31ms | 1.6× | 1.2% | PASS |
| mem_writes | `oltp_insert` | 15.93ms | 33.80ms | 2.1× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 61.26ms | 133.93ms | 2.2× | 2.0% | PASS |
| mem_writes | `oltp_update_non_index` | 43.37ms | 71.62ms | 1.7× | 1.5% | PASS |
| mem_writes | `oltp_delete_insert` | 45.11ms | 91.25ms | 2.0× | 1.1% | PASS |
| mem_writes | `oltp_write_only` | 23.59ms | 52.80ms | 2.2× | 1.3% | PASS |
| mem_writes | `types_delete_insert` | 33.05ms | 46.86ms | 1.4× | 1.9% | PASS |
| mem_writes | `oltp_read_write` | 85.76ms | 130.69ms | 1.5× | 1.9% | PASS |
| file_reads | `oltp_point_select` | 62.73ms | 43.32ms | 0.7× | 1.5% | PASS |
| file_reads | `oltp_range_select` | 17.16ms | 14.14ms | 0.8× | 1.7% | PASS |
| file_reads | `oltp_sum_range` | 16.71ms | 14.27ms | 0.9× | 1.5% | PASS |
| file_reads | `oltp_order_range` | 3.29ms | 3.13ms | 1.0× | 2.4% | PASS |
| file_reads | `oltp_distinct_range` | 4.13ms | 4.26ms | 1.0× | 1.4% | PASS |
| file_reads | `oltp_index_scan` | 7.00ms | 6.73ms | 1.0× | 1.6% | PASS |
| file_reads | `select_random_points` | 25.74ms | 23.09ms | 0.9× | 1.9% | PASS |
| file_reads | `select_random_ranges` | 9.29ms | 7.22ms | 0.8× | 1.4% | PASS |
| file_reads | `covering_index_scan` | 10.12ms | 10.15ms | 1.0× | 2.1% | PASS |
| file_reads | `groupby_scan` | 31.80ms | 34.04ms | 1.1× | 1.3% | PASS |
| file_reads | `index_join` | 12.58ms | 10.22ms | 0.8× | 2.1% | PASS |
| file_reads | `index_join_scan` | 3.61ms | 5.94ms | 1.6× | 2.9% | PASS |
| file_reads | `types_table_scan` | 1.05s | 1.21s | 1.2× | 0.7% | PASS |
| file_reads | `table_scan` | 1.22s | 1.32s | 1.1× | 0.6% | PASS |
| file_reads | `oltp_read_only` | 171.68ms | 145.44ms | 0.8× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 222.27ms | 339.25ms | 1.5× | 1.3% | PASS |
| file_writes | `oltp_insert` | 20.96ms | 41.47ms | 2.0× | 2.4% | PASS |
| file_writes | `oltp_update_index` | 76.48ms | 142.19ms | 1.9× | 1.7% | PASS |
| file_writes | `oltp_update_non_index` | 62.82ms | 85.22ms | 1.4× | 6.6% | PASS |
| file_writes | `oltp_delete_insert` | 67.99ms | 108.38ms | 1.6× | 1.7% | PASS |
| file_writes | `oltp_write_only` | 40.83ms | 65.24ms | 1.6× | 2.2% | PASS |
| file_writes | `types_delete_insert` | 48.85ms | 58.11ms | 1.2× | 2.1% | PASS |
| file_writes | `oltp_read_write` | 103.68ms | 140.55ms | 1.4× | 1.3% | PASS |
| ac_reads | `oltp_point_select` | 41.62ms | 43.56ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_range_select` | 15.15ms | 14.29ms | 0.9× | 1.2% | PASS |
| ac_reads | `oltp_sum_range` | 14.85ms | 14.54ms | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_order_range` | 3.07ms | 3.17ms | 1.0× | 2.3% | PASS |
| ac_reads | `oltp_distinct_range` | 3.84ms | 4.22ms | 1.1× | 1.5% | PASS |
| ac_reads | `oltp_index_scan` | 4.80ms | 6.63ms | 1.4× | 1.1% | PASS |
| ac_reads | `select_random_points` | 22.39ms | 22.80ms | 1.0× | 1.9% | PASS |
| ac_reads | `select_random_ranges` | 6.95ms | 7.17ms | 1.0× | 1.1% | PASS |
| ac_reads | `covering_index_scan` | 7.75ms | 10.01ms | 1.3× | 1.3% | PASS |
| ac_reads | `groupby_scan` | 31.09ms | 33.67ms | 1.1× | 0.6% | PASS |
| ac_reads | `index_join` | 11.20ms | 10.15ms | 0.9× | 1.7% | PASS |
| ac_reads | `index_join_scan` | 3.37ms | 5.97ms | 1.8× | 2.3% | PASS |
| ac_reads | `types_table_scan` | 1.04s | 1.20s | 1.2× | 0.5% | PASS |
| ac_reads | `table_scan` | 1.22s | 1.32s | 1.1× | 0.9% | PASS |
| ac_reads | `oltp_read_only` | 142.02ms | 146.27ms | 1.0× | 0.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 16.92ms | 57.38ms | 3.4× | 5.9% | PASS |
| ac_writes | `oltp_insert_ac` | 18.08ms | 71.35ms | 3.9× | 5.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 18.55ms | 83.92ms | 4.5× | 4.4% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 15.20ms | 62.10ms | 4.1× | 6.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 18.02ms | 71.08ms | 3.9× | 7.4% | PASS |
| ac_writes | `oltp_write_only_ac` | 16.90ms | 71.37ms | 4.2× | 6.4% | PASS |
| ac_writes | `types_delete_insert_ac` | 16.56ms | 64.15ms | 3.9× | 7.1% | PASS |
| ac_writes | `oltp_read_write_ac` | 22.46ms | 75.97ms | 3.4× | 4.6% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.62ms | 41.04ms | 1.2× | 1.1% | PASS |
| mem_reads | `oltp_range_select` | 20.03ms | 20.83ms | 1.0× | 1.6% | PASS |
| mem_reads | `oltp_sum_range` | 18.15ms | 20.39ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_order_range` | 3.58ms | 3.82ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_distinct_range` | 4.86ms | 5.04ms | 1.0× | 1.7% | PASS |
| mem_reads | `oltp_index_scan` | 4.58ms | 6.19ms | 1.4× | 3.5% | PASS |
| mem_reads | `select_random_points` | 28.29ms | 32.65ms | 1.2× | 3.1% | PASS |
| mem_reads | `select_random_ranges` | 7.80ms | 9.02ms | 1.2× | 1.4% | PASS |
| mem_reads | `covering_index_scan` | 7.82ms | 9.69ms | 1.2× | 1.4% | PASS |
| mem_reads | `groupby_scan` | 36.46ms | 38.77ms | 1.1× | 0.7% | PASS |
| mem_reads | `index_join` | 8.11ms | 10.96ms | 1.4× | 2.5% | PASS |
| mem_reads | `index_join_scan` | 3.90ms | 5.49ms | 1.4× | 2.3% | PASS |
| mem_reads | `types_table_scan` | 1.08s | 1.17s | 1.1× | 1.6% | PASS |
| mem_reads | `table_scan` | 1.28s | 1.30s | 1.0× | 5.1% | PASS |
| mem_reads | `oltp_read_only` | 151.72ms | 170.92ms | 1.1× | 1.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 248.08ms | 348.38ms | 1.4× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 18.98ms | 34.84ms | 1.8× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 65.61ms | 121.43ms | 1.9× | 0.8% | PASS |
| mem_writes | `oltp_update_non_index` | 50.49ms | 80.01ms | 1.6× | 1.3% | PASS |
| mem_writes | `oltp_delete_insert` | 49.24ms | 95.79ms | 1.9× | 1.2% | PASS |
| mem_writes | `oltp_write_only` | 26.53ms | 57.51ms | 2.2× | 1.6% | PASS |
| mem_writes | `types_delete_insert` | 32.06ms | 53.07ms | 1.7× | 1.7% | PASS |
| mem_writes | `oltp_read_write` | 102.51ms | 163.56ms | 1.6× | 1.7% | PASS |
| file_reads | `oltp_point_select` | 105.88ms | 61.12ms | 0.6× | 1.3% | PASS |
| file_reads | `oltp_range_select` | 27.67ms | 23.20ms | 0.8× | 1.2% | PASS |
| file_reads | `oltp_sum_range` | 26.32ms | 23.04ms | 0.9× | 1.2% | PASS |
| file_reads | `oltp_order_range` | 4.49ms | 4.18ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_distinct_range` | 5.70ms | 5.34ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_index_scan` | 11.64ms | 8.38ms | 0.7× | 1.6% | PASS |
| file_reads | `select_random_points` | 36.95ms | 36.22ms | 1.0× | 1.6% | PASS |
| file_reads | `select_random_ranges` | 15.36ms | 11.41ms | 0.7× | 1.4% | PASS |
| file_reads | `covering_index_scan` | 15.11ms | 11.77ms | 0.8× | 1.4% | PASS |
| file_reads | `groupby_scan` | 36.86ms | 38.86ms | 1.1× | 0.6% | PASS |
| file_reads | `index_join` | 11.96ms | 12.29ms | 1.0× | 1.3% | PASS |
| file_reads | `index_join_scan` | 4.88ms | 5.96ms | 1.2× | 2.3% | PASS |
| file_reads | `types_table_scan` | 1.06s | 1.17s | 1.1× | 1.7% | PASS |
| file_reads | `table_scan` | 1.24s | 1.29s | 1.0× | 2.3% | PASS |
| file_reads | `oltp_read_only` | 261.49ms | 201.62ms | 0.8× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 265.25ms | 361.89ms | 1.4× | 0.9% | PASS |
| file_writes | `oltp_insert` | 25.92ms | 40.85ms | 1.6× | 2.2% | PASS |
| file_writes | `oltp_update_index` | 93.72ms | 136.96ms | 1.5× | 1.8% | PASS |
| file_writes | `oltp_update_non_index` | 76.02ms | 95.89ms | 1.3× | 1.8% | PASS |
| file_writes | `oltp_delete_insert` | 75.01ms | 109.77ms | 1.5× | 2.2% | PASS |
| file_writes | `oltp_write_only` | 49.51ms | 69.22ms | 1.4× | 2.4% | PASS |
| file_writes | `types_delete_insert` | 48.60ms | 62.03ms | 1.3× | 1.5% | PASS |
| file_writes | `oltp_read_write` | 125.48ms | 174.38ms | 1.4× | 1.8% | PASS |
| ac_reads | `oltp_point_select` | 56.27ms | 60.64ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_range_select` | 22.71ms | 23.20ms | 1.0× | 2.2% | PASS |
| ac_reads | `oltp_sum_range` | 20.89ms | 22.89ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_order_range` | 3.87ms | 4.11ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_distinct_range` | 5.17ms | 5.34ms | 1.0× | 2.2% | PASS |
| ac_reads | `oltp_index_scan` | 7.16ms | 8.48ms | 1.2× | 2.1% | PASS |
| ac_reads | `select_random_points` | 32.01ms | 36.53ms | 1.1× | 1.6% | PASS |
| ac_reads | `select_random_ranges` | 10.52ms | 11.40ms | 1.1× | 1.2% | PASS |
| ac_reads | `covering_index_scan` | 10.79ms | 11.90ms | 1.1× | 1.6% | PASS |
| ac_reads | `groupby_scan` | 36.87ms | 39.29ms | 1.1× | 1.2% | PASS |
| ac_reads | `index_join` | 9.91ms | 12.67ms | 1.3× | 2.4% | PASS |
| ac_reads | `index_join_scan` | 4.41ms | 6.06ms | 1.4× | 3.4% | PASS |
| ac_reads | `types_table_scan` | 1.05s | 1.16s | 1.1× | 0.7% | PASS |
| ac_reads | `table_scan` | 1.22s | 1.28s | 1.0× | 1.8% | PASS |
| ac_reads | `oltp_read_only` | 187.38ms | 201.15ms | 1.1× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.08ms | 75.98ms | 3.4× | 4.1% | PASS |
| ac_writes | `oltp_insert_ac` | 25.07ms | 94.85ms | 3.8× | 6.3% | PASS |
| ac_writes | `oltp_update_index_ac` | 28.32ms | 109.02ms | 3.8× | 5.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.13ms | 86.56ms | 3.7× | 4.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.64ms | 98.27ms | 3.8× | 7.7% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.36ms | 97.04ms | 3.7× | 4.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 23.03ms | 92.50ms | 4.0× | 7.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.17ms | 104.69ms | 3.3× | 5.7% | PASS |

</details>

</details>

## Version-control latency

Wall time: 4m 35s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 36.07ms | 130.00ms | 27.7% | 0.9% | PASS |
| `status_dirty_many_tables` | 41.03ms | 130.00ms | 31.6% | 0.5% | PASS |
| `diff_regular_working_one_table` | 32.59ms | 120.00ms | 27.2% | 0.6% | PASS |
| `diff_regular_working_many_tables` | 46.48ms | 140.00ms | 33.2% | 0.8% | PASS |
| `diff_stat_working_many_tables` | 46.54ms | 140.00ms | 33.2% | 0.7% | PASS |
| `diff_schema_working_many_tables` | 47.41ms | 140.00ms | 33.9% | 0.5% | PASS |
| `branch_list_many_branches` | 21.96ms | 35.00ms | 62.8% | 0.8% | PASS |
| `branch_create_delete` | 24.78ms | 40.00ms | 62.0% | 1.8% | PASS |
| `at_literal_deep_history` | 35.23ms | 100.00ms | 35.2% | 0.8% | PASS |
| `diff_literal_deep_history` | 35.27ms | 120.00ms | 29.4% | 0.9% | PASS |
| `history_literal_deep_history` | 39.09ms | 150.00ms | 26.1% | 0.7% | PASS |
| `checkout_branch_clean` | 40.75ms | 150.00ms | 27.2% | 1.3% | PASS |
| `merge_data_no_conflicts` | 28.41ms | 50.00ms | 56.8% | 1.3% | PASS |
| `merge_data_secondary_index` | 1.50s | 2.50s | 59.9% | 0.3% | PASS |
| `merge_schema_no_conflicts` | 21.63ms | 35.00ms | 61.8% | 1.6% | PASS |
| `merge_data_conflicts` | 29.69ms | 180.00ms | 16.5% | 0.6% | PASS |
| `merge_data_conflicts_with_resolve` | 30.77ms | 180.00ms | 17.1% | 1.2% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
