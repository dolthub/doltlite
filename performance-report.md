# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-09-19 11:05 UTC
>
> Commit: [`827f32f8e9916006353e87e7ac098869fb0c7897`](https://github.com/dolthub/doltlite/commit/827f32f8e9916006353e87e7ac098869fb0c7897)
>
> Runner: ubuntu24 20260907.300.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/35435014061)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 8.09s | 8.85s | 1.1× | 1.3% | **PASS** |
| Writes | 1.57s | 2.57s | 1.6× | 1.2% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 8.62s | 9.07s | 1.1× | 1.3% | **PASS** |
| Writes | 3.58s | 3.86s | 1.1× | 2.4% | **PASS** |
| Autocommit writes | 889.60ms | 2.90s | 3.3× | 6.4% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 1.52s | 1.60s | 1.1× | 1.8% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.45s | 2.71s | 1.1× | 1.0% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.50s | 2.80s | 1.1× | 1.2% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 1.62s | 1.73s | 1.1× | 2.0% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 241.84ms | 393.98ms | 1.6× | 1.3% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 486.03ms | 798.58ms | 1.6× | 1.1% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 508.41ms | 851.05ms | 1.7× | 1.1% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 329.00ms | 522.97ms | 1.6× | 1.1% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 1.71s | 1.66s | 1.0× | 1.1% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.56s | 2.75s | 1.1× | 1.2% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.49s | 2.79s | 1.1× | 1.3% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 1.86s | 1.86s | 1.0× | 1.6% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 714.39ms | 684.17ms | 1.0× | 0.9% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.35s | 1.36s | 1.0× | 2.8% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 638.25ms | 933.01ms | 1.5× | 1.3% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 874.38ms | 883.45ms | 1.0× | 3.5% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 1.74s | 1.81s | 1.0× | 1.7% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.47s | 2.74s | 1.1× | 1.1% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.44s | 2.80s | 1.1× | 1.1% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 1.81s | 1.82s | 1.0× | 1.9% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 258.90ms | 733.40ms | 2.8× | 5.7% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 236.34ms | 904.45ms | 3.8× | 15.3% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 136.56ms | 540.64ms | 4.0× | 5.5% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 257.80ms | 720.91ms | 2.8× | 6.4% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 13.33ms | 16.41ms | 1.2× | 1.9% | PASS |
| mem_reads | `oltp_range_select` | 5.11ms | 5.92ms | 1.2× | 3.4% | PASS |
| mem_reads | `oltp_sum_range` | 4.70ms | 5.92ms | 1.3× | 3.1% | PASS |
| mem_reads | `oltp_order_range` | 1.43ms | 1.56ms | 1.1× | 2.4% | PASS |
| mem_reads | `oltp_distinct_range` | 1.92ms | 2.06ms | 1.1× | 1.8% | PASS |
| mem_reads | `oltp_index_scan` | 2.08ms | 2.70ms | 1.3× | 2.8% | PASS |
| mem_reads | `select_random_points` | 5.33ms | 6.84ms | 1.3× | 2.7% | PASS |
| mem_reads | `select_random_ranges` | 2.41ms | 2.61ms | 1.1× | 3.1% | PASS |
| mem_reads | `covering_index_scan` | 3.80ms | 4.64ms | 1.2× | 1.1% | PASS |
| mem_reads | `groupby_scan` | 15.93ms | 17.18ms | 1.1× | 0.7% | PASS |
| mem_reads | `index_join` | 3.27ms | 4.05ms | 1.2× | 1.1% | PASS |
| mem_reads | `index_join_scan` | 1.66ms | 3.15ms | 1.9× | 1.8% | PASS |
| mem_reads | `types_table_scan` | 662.56ms | 693.23ms | 1.0× | 0.9% | PASS |
| mem_reads | `table_scan` | 748.64ms | 777.93ms | 1.0× | 0.8% | PASS |
| mem_reads | `oltp_read_only` | 51.87ms | 59.12ms | 1.1× | 0.5% | PASS |
| mem_writes | `oltp_bulk_insert` | 101.64ms | 147.72ms | 1.5× | 1.1% | PASS |
| mem_writes | `oltp_insert` | 8.69ms | 15.62ms | 1.8× | 1.1% | PASS |
| mem_writes | `oltp_update_index` | 28.49ms | 52.47ms | 1.8× | 1.4% | PASS |
| mem_writes | `oltp_update_non_index` | 19.20ms | 31.20ms | 1.6× | 1.3% | PASS |
| mem_writes | `oltp_delete_insert` | 24.28ms | 40.75ms | 1.7× | 1.2% | PASS |
| mem_writes | `oltp_write_only` | 11.99ms | 26.27ms | 2.2× | 1.5% | PASS |
| mem_writes | `types_delete_insert` | 13.77ms | 20.87ms | 1.5× | 1.5% | PASS |
| mem_writes | `oltp_read_write` | 33.77ms | 59.09ms | 1.8× | 1.3% | PASS |
| file_reads | `oltp_point_select` | 68.67ms | 32.08ms | 0.5× | 1.2% | PASS |
| file_reads | `oltp_range_select` | 11.57ms | 7.82ms | 0.7× | 1.0% | PASS |
| file_reads | `oltp_sum_range` | 11.19ms | 7.91ms | 0.7× | 0.9% | PASS |
| file_reads | `oltp_order_range` | 2.23ms | 1.83ms | 0.8× | 1.0% | PASS |
| file_reads | `oltp_distinct_range` | 2.59ms | 2.25ms | 0.9× | 1.1% | PASS |
| file_reads | `oltp_index_scan` | 7.76ms | 4.29ms | 0.6× | 0.9% | PASS |
| file_reads | `select_random_points` | 12.47ms | 8.80ms | 0.7× | 2.2% | PASS |
| file_reads | `select_random_ranges` | 7.83ms | 4.08ms | 0.5× | 1.3% | PASS |
| file_reads | `covering_index_scan` | 9.39ms | 6.20ms | 0.7× | 1.2% | PASS |
| file_reads | `groupby_scan` | 16.89ms | 17.47ms | 1.0× | 1.0% | PASS |
| file_reads | `index_join` | 6.36ms | 5.26ms | 0.8× | 3.8% | PASS |
| file_reads | `index_join_scan` | 2.75ms | 3.49ms | 1.3× | 2.0% | PASS |
| file_reads | `types_table_scan` | 666.42ms | 699.52ms | 1.0× | 0.9% | PASS |
| file_reads | `table_scan` | 755.62ms | 780.30ms | 1.0× | 0.9% | PASS |
| file_reads | `oltp_read_only` | 126.83ms | 80.22ms | 0.6× | 1.7% | PASS |
| file_writes | `oltp_bulk_insert` | 140.43ms | 192.91ms | 1.4× | 3.0% | PASS |
| file_writes | `oltp_insert` | 19.80ms | 27.60ms | 1.4× | 0.5% | PASS |
| file_writes | `oltp_update_index` | 120.95ms | 107.88ms | 0.9× | 5.7% | PASS |
| file_writes | `oltp_update_non_index` | 90.68ms | 78.86ms | 0.9× | 0.7% | PASS |
| file_writes | `oltp_delete_insert` | 100.89ms | 88.43ms | 0.9× | 1.0% | PASS |
| file_writes | `oltp_write_only` | 80.68ms | 59.54ms | 0.7× | 0.4% | PASS |
| file_writes | `types_delete_insert` | 60.41ms | 39.27ms | 0.6× | 1.5% | PASS |
| file_writes | `oltp_read_write` | 100.53ms | 89.68ms | 0.9× | 0.7% | PASS |
| ac_reads | `oltp_point_select` | 30.04ms | 30.05ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_range_select` | 7.18ms | 7.39ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_sum_range` | 7.11ms | 7.61ms | 1.1× | 1.9% | PASS |
| ac_reads | `oltp_order_range` | 1.73ms | 1.77ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_distinct_range` | 2.21ms | 2.26ms | 1.0× | 1.8% | PASS |
| ac_reads | `oltp_index_scan` | 4.50ms | 4.83ms | 1.1× | 2.0% | PASS |
| ac_reads | `select_random_points` | 9.36ms | 9.42ms | 1.0× | 2.5% | PASS |
| ac_reads | `select_random_ranges` | 4.78ms | 4.49ms | 0.9× | 1.9% | PASS |
| ac_reads | `covering_index_scan` | 6.39ms | 7.24ms | 1.1× | 1.7% | PASS |
| ac_reads | `groupby_scan` | 17.96ms | 19.12ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 5.38ms | 7.23ms | 1.3× | 4.7% | PASS |
| ac_reads | `index_join_scan` | 2.73ms | 4.22ms | 1.5× | 5.4% | PASS |
| ac_reads | `types_table_scan` | 739.34ms | 766.43ms | 1.0× | 1.4% | PASS |
| ac_reads | `table_scan` | 824.01ms | 848.70ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_read_only` | 81.99ms | 84.75ms | 1.0× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 30.33ms | 79.99ms | 2.6× | 5.8% | PASS |
| ac_writes | `oltp_insert_ac` | 32.85ms | 92.58ms | 2.8× | 5.7% | PASS |
| ac_writes | `oltp_update_index_ac` | 34.30ms | 102.03ms | 3.0× | 7.3% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 31.18ms | 86.33ms | 2.8× | 8.1% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 32.45ms | 96.88ms | 3.0× | 5.4% | PASS |
| ac_writes | `oltp_write_only_ac` | 32.98ms | 93.62ms | 2.8× | 5.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 30.88ms | 85.63ms | 2.8× | 5.8% | PASS |
| ac_writes | `oltp_read_write_ac` | 33.94ms | 96.33ms | 2.8× | 4.6% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 32.16ms | 31.57ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_range_select` | 13.60ms | 12.12ms | 0.9× | 1.0% | PASS |
| mem_reads | `oltp_sum_range` | 13.25ms | 11.73ms | 0.9× | 1.6% | PASS |
| mem_reads | `oltp_order_range` | 2.89ms | 2.80ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 3.74ms | 3.88ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 3.46ms | 4.90ms | 1.4× | 1.0% | PASS |
| mem_reads | `select_random_points` | 19.98ms | 18.79ms | 0.9× | 1.0% | PASS |
| mem_reads | `select_random_ranges` | 5.54ms | 5.09ms | 0.9× | 1.0% | PASS |
| mem_reads | `covering_index_scan` | 6.26ms | 7.58ms | 1.2× | 0.4% | PASS |
| mem_reads | `groupby_scan` | 31.57ms | 32.97ms | 1.0× | 0.5% | PASS |
| mem_reads | `index_join` | 10.74ms | 9.09ms | 0.8× | 1.2% | PASS |
| mem_reads | `index_join_scan` | 3.12ms | 5.53ms | 1.8× | 1.9% | PASS |
| mem_reads | `types_table_scan` | 1.02s | 1.17s | 1.1× | 0.3% | PASS |
| mem_reads | `table_scan` | 1.17s | 1.27s | 1.1× | 0.4% | PASS |
| mem_reads | `oltp_read_only` | 119.52ms | 122.42ms | 1.0× | 2.5% | PASS |
| mem_writes | `oltp_bulk_insert` | 193.33ms | 282.37ms | 1.5× | 0.4% | PASS |
| mem_writes | `oltp_insert` | 15.09ms | 31.61ms | 2.1× | 1.1% | PASS |
| mem_writes | `oltp_update_index` | 56.05ms | 118.38ms | 2.1× | 1.2% | PASS |
| mem_writes | `oltp_update_non_index` | 40.19ms | 66.65ms | 1.7× | 1.3% | PASS |
| mem_writes | `oltp_delete_insert` | 45.43ms | 85.86ms | 1.9× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 23.27ms | 49.01ms | 2.1× | 1.3% | PASS |
| mem_writes | `types_delete_insert` | 32.74ms | 43.44ms | 1.3× | 1.0% | PASS |
| mem_writes | `oltp_read_write` | 79.93ms | 121.25ms | 1.5× | 2.5% | PASS |
| file_reads | `oltp_point_select` | 67.25ms | 41.90ms | 0.6× | 1.4% | PASS |
| file_reads | `oltp_range_select` | 17.45ms | 13.73ms | 0.8× | 0.9% | PASS |
| file_reads | `oltp_sum_range` | 17.12ms | 13.35ms | 0.8× | 1.5% | PASS |
| file_reads | `oltp_order_range` | 3.32ms | 3.08ms | 0.9× | 1.9% | PASS |
| file_reads | `oltp_distinct_range` | 4.22ms | 4.24ms | 1.0× | 1.0% | PASS |
| file_reads | `oltp_index_scan` | 6.76ms | 6.38ms | 0.9× | 1.7% | PASS |
| file_reads | `select_random_points` | 24.36ms | 20.98ms | 0.9× | 1.0% | PASS |
| file_reads | `select_random_ranges` | 9.05ms | 6.28ms | 0.7× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 10.07ms | 9.23ms | 0.9× | 0.9% | PASS |
| file_reads | `groupby_scan` | 32.91ms | 34.21ms | 1.0× | 0.8% | PASS |
| file_reads | `index_join` | 12.50ms | 9.69ms | 0.8× | 1.5% | PASS |
| file_reads | `index_join_scan` | 3.63ms | 6.07ms | 1.7× | 1.3% | PASS |
| file_reads | `types_table_scan` | 1.02s | 1.17s | 1.2× | 0.4% | PASS |
| file_reads | `table_scan` | 1.17s | 1.28s | 1.1× | 1.2% | PASS |
| file_reads | `oltp_read_only` | 157.50ms | 130.26ms | 0.8× | 0.6% | PASS |
| file_writes | `oltp_bulk_insert` | 280.08ms | 359.07ms | 1.3× | 3.6% | PASS |
| file_writes | `oltp_insert` | 32.55ms | 61.29ms | 1.9× | 6.2% | PASS |
| file_writes | `oltp_update_index` | 185.50ms | 216.28ms | 1.2× | 2.1% | PASS |
| file_writes | `oltp_update_non_index` | 174.18ms | 154.46ms | 0.9× | 7.8% | PASS |
| file_writes | `oltp_delete_insert` | 201.78ms | 174.31ms | 0.9× | 2.0% | PASS |
| file_writes | `oltp_write_only` | 132.85ms | 116.09ms | 0.9× | 2.5% | PASS |
| file_writes | `types_delete_insert` | 157.18ms | 97.18ms | 0.6× | 3.1% | PASS |
| file_writes | `oltp_read_write` | 187.24ms | 185.26ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_point_select` | 39.92ms | 38.70ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_range_select` | 16.22ms | 14.23ms | 0.9× | 1.4% | PASS |
| ac_reads | `oltp_sum_range` | 14.80ms | 13.16ms | 0.9× | 1.4% | PASS |
| ac_reads | `oltp_order_range` | 3.08ms | 3.04ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_distinct_range` | 3.96ms | 4.18ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 4.67ms | 6.15ms | 1.3× | 1.7% | PASS |
| ac_reads | `select_random_points` | 22.11ms | 20.78ms | 0.9× | 1.1% | PASS |
| ac_reads | `select_random_ranges` | 6.93ms | 6.25ms | 0.9× | 1.1% | PASS |
| ac_reads | `covering_index_scan` | 7.41ms | 8.87ms | 1.2× | 1.1% | PASS |
| ac_reads | `groupby_scan` | 33.86ms | 35.36ms | 1.0× | 0.9% | PASS |
| ac_reads | `index_join` | 11.58ms | 9.73ms | 0.8× | 1.3% | PASS |
| ac_reads | `index_join_scan` | 3.49ms | 6.03ms | 1.7× | 1.6% | PASS |
| ac_reads | `types_table_scan` | 1.01s | 1.17s | 1.2× | 0.4% | PASS |
| ac_reads | `table_scan` | 1.16s | 1.28s | 1.1× | 0.4% | PASS |
| ac_reads | `oltp_read_only` | 128.13ms | 130.27ms | 1.0× | 0.3% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 26.18ms | 89.94ms | 3.4× | 7.8% | PASS |
| ac_writes | `oltp_insert_ac` | 27.23ms | 107.98ms | 4.0× | 16.3% | PASS |
| ac_writes | `oltp_update_index_ac` | 29.27ms | 124.80ms | 4.3× | 15.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 28.61ms | 113.60ms | 4.0× | 13.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 30.01ms | 122.04ms | 4.1× | 16.5% | PASS |
| ac_writes | `oltp_write_only_ac` | 33.83ms | 118.47ms | 3.5× | 16.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 28.41ms | 109.19ms | 3.8× | 15.6% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.80ms | 118.43ms | 3.6× | 7.9% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 31.79ms | 33.15ms | 1.0× | 1.3% | PASS |
| mem_reads | `oltp_range_select` | 13.80ms | 12.94ms | 0.9× | 1.5% | PASS |
| mem_reads | `oltp_sum_range` | 13.03ms | 12.96ms | 1.0× | 1.3% | PASS |
| mem_reads | `oltp_order_range` | 2.84ms | 2.95ms | 1.0× | 0.6% | PASS |
| mem_reads | `oltp_distinct_range` | 3.64ms | 4.04ms | 1.1× | 0.5% | PASS |
| mem_reads | `oltp_index_scan` | 3.51ms | 5.69ms | 1.6× | 1.2% | PASS |
| mem_reads | `select_random_points` | 19.88ms | 20.80ms | 1.0× | 1.5% | PASS |
| mem_reads | `select_random_ranges` | 5.55ms | 5.73ms | 1.0× | 1.0% | PASS |
| mem_reads | `covering_index_scan` | 6.47ms | 8.62ms | 1.3× | 1.0% | PASS |
| mem_reads | `groupby_scan` | 30.57ms | 33.21ms | 1.1× | 0.5% | PASS |
| mem_reads | `index_join` | 9.57ms | 9.28ms | 1.0× | 1.5% | PASS |
| mem_reads | `index_join_scan` | 3.05ms | 5.64ms | 1.8× | 1.6% | PASS |
| mem_reads | `types_table_scan` | 1.03s | 1.21s | 1.2× | 0.5% | PASS |
| mem_reads | `table_scan` | 1.20s | 1.31s | 1.1× | 0.5% | PASS |
| mem_reads | `oltp_read_only` | 122.40ms | 128.66ms | 1.1× | 1.2% | PASS |
| mem_writes | `oltp_bulk_insert` | 206.12ms | 304.83ms | 1.5× | 1.3% | PASS |
| mem_writes | `oltp_insert` | 15.97ms | 33.53ms | 2.1× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 57.60ms | 125.30ms | 2.2× | 1.3% | PASS |
| mem_writes | `oltp_update_non_index` | 42.64ms | 70.76ms | 1.7× | 1.2% | PASS |
| mem_writes | `oltp_delete_insert` | 45.11ms | 90.22ms | 2.0× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 23.24ms | 51.60ms | 2.2× | 1.0% | PASS |
| mem_writes | `types_delete_insert` | 32.39ms | 45.55ms | 1.4× | 1.1% | PASS |
| mem_writes | `oltp_read_write` | 85.34ms | 129.27ms | 1.5× | 1.1% | PASS |
| file_reads | `oltp_point_select` | 62.86ms | 42.72ms | 0.7× | 1.2% | PASS |
| file_reads | `oltp_range_select` | 16.81ms | 14.03ms | 0.8× | 1.1% | PASS |
| file_reads | `oltp_sum_range` | 16.17ms | 14.22ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_order_range` | 3.21ms | 3.12ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_distinct_range` | 4.03ms | 4.23ms | 1.1× | 1.1% | PASS |
| file_reads | `oltp_index_scan` | 6.68ms | 6.75ms | 1.0× | 1.3% | PASS |
| file_reads | `select_random_points` | 23.32ms | 21.89ms | 0.9× | 1.9% | PASS |
| file_reads | `select_random_ranges` | 8.83ms | 6.88ms | 0.8× | 1.4% | PASS |
| file_reads | `covering_index_scan` | 9.81ms | 9.81ms | 1.0× | 1.3% | PASS |
| file_reads | `groupby_scan` | 30.60ms | 33.35ms | 1.1× | 0.5% | PASS |
| file_reads | `index_join` | 11.29ms | 9.74ms | 0.9× | 1.8% | PASS |
| file_reads | `index_join_scan` | 3.50ms | 5.73ms | 1.6× | 1.3% | PASS |
| file_reads | `types_table_scan` | 988.03ms | 1.19s | 1.2× | 1.3% | PASS |
| file_reads | `table_scan` | 1.14s | 1.29s | 1.1× | 1.7% | PASS |
| file_reads | `oltp_read_only` | 167.08ms | 142.77ms | 0.9× | 0.7% | PASS |
| file_writes | `oltp_bulk_insert` | 221.34ms | 316.90ms | 1.4× | 1.3% | PASS |
| file_writes | `oltp_insert` | 20.25ms | 38.92ms | 1.9× | 1.2% | PASS |
| file_writes | `oltp_update_index` | 73.59ms | 136.88ms | 1.9× | 1.5% | PASS |
| file_writes | `oltp_update_non_index` | 75.34ms | 81.89ms | 1.1× | 13.8% | PASS |
| file_writes | `oltp_delete_insert` | 64.25ms | 103.67ms | 1.6× | 1.2% | PASS |
| file_writes | `oltp_write_only` | 38.77ms | 62.42ms | 1.6× | 1.3% | PASS |
| file_writes | `types_delete_insert` | 46.62ms | 55.74ms | 1.2× | 2.3% | PASS |
| file_writes | `oltp_read_write` | 98.09ms | 136.59ms | 1.4× | 1.3% | PASS |
| ac_reads | `oltp_point_select` | 40.54ms | 42.16ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_range_select` | 14.56ms | 14.01ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_sum_range` | 13.89ms | 14.09ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_order_range` | 2.98ms | 3.11ms | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_distinct_range` | 3.78ms | 4.22ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 4.69ms | 6.72ms | 1.4× | 1.0% | PASS |
| ac_reads | `select_random_points` | 21.02ms | 21.68ms | 1.0× | 1.4% | PASS |
| ac_reads | `select_random_ranges` | 6.75ms | 6.85ms | 1.0× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 7.60ms | 9.77ms | 1.3× | 1.1% | PASS |
| ac_reads | `groupby_scan` | 30.43ms | 33.38ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 10.27ms | 9.78ms | 1.0× | 1.6% | PASS |
| ac_reads | `index_join_scan` | 3.29ms | 5.71ms | 1.7× | 1.3% | PASS |
| ac_reads | `types_table_scan` | 980.39ms | 1.19s | 1.2× | 1.2% | PASS |
| ac_reads | `table_scan` | 1.17s | 1.30s | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_read_only` | 134.43ms | 141.79ms | 1.1× | 1.1% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 15.01ms | 53.37ms | 3.6× | 4.8% | PASS |
| ac_writes | `oltp_insert_ac` | 16.45ms | 66.07ms | 4.0× | 4.7% | PASS |
| ac_writes | `oltp_update_index_ac` | 17.53ms | 81.09ms | 4.6× | 6.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 15.24ms | 61.86ms | 4.1× | 5.4% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 17.30ms | 70.81ms | 4.1× | 6.4% | PASS |
| ac_writes | `oltp_write_only_ac` | 17.54ms | 69.77ms | 4.0× | 5.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 16.03ms | 62.38ms | 3.9× | 6.6% | PASS |
| ac_writes | `oltp_read_write_ac` | 21.47ms | 75.30ms | 3.5× | 5.1% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 20.48ms | 22.61ms | 1.1× | 2.7% | PASS |
| mem_reads | `oltp_range_select` | 10.48ms | 10.85ms | 1.0× | 2.2% | PASS |
| mem_reads | `oltp_sum_range` | 9.40ms | 10.63ms | 1.1× | 3.5% | PASS |
| mem_reads | `oltp_order_range` | 2.15ms | 2.17ms | 1.0× | 3.1% | PASS |
| mem_reads | `oltp_distinct_range` | 3.04ms | 3.12ms | 1.0× | 2.0% | PASS |
| mem_reads | `oltp_index_scan` | 3.17ms | 3.92ms | 1.2× | 3.0% | PASS |
| mem_reads | `select_random_points` | 18.54ms | 21.64ms | 1.2× | 3.5% | PASS |
| mem_reads | `select_random_ranges` | 5.06ms | 5.66ms | 1.1× | 1.9% | PASS |
| mem_reads | `covering_index_scan` | 4.14ms | 5.13ms | 1.2× | 1.5% | PASS |
| mem_reads | `groupby_scan` | 19.59ms | 20.79ms | 1.1× | 1.2% | PASS |
| mem_reads | `index_join` | 4.60ms | 5.63ms | 1.2× | 2.0% | PASS |
| mem_reads | `index_join_scan` | 2.06ms | 3.86ms | 1.9× | 2.1% | PASS |
| mem_reads | `types_table_scan` | 672.21ms | 723.50ms | 1.1× | 0.7% | PASS |
| mem_reads | `table_scan` | 767.90ms | 806.92ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_read_only` | 75.82ms | 83.71ms | 1.1× | 1.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 138.68ms | 186.97ms | 1.3× | 1.2% | PASS |
| mem_writes | `oltp_insert` | 10.76ms | 19.87ms | 1.8× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 41.42ms | 76.06ms | 1.8× | 1.8% | PASS |
| mem_writes | `oltp_update_non_index` | 28.55ms | 44.54ms | 1.6× | 1.9% | PASS |
| mem_writes | `oltp_delete_insert` | 27.10ms | 53.01ms | 2.0× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 15.04ms | 32.57ms | 2.2× | 0.7% | PASS |
| mem_writes | `types_delete_insert` | 17.97ms | 28.63ms | 1.6× | 2.1% | PASS |
| mem_writes | `oltp_read_write` | 49.46ms | 81.31ms | 1.6× | 0.9% | PASS |
| file_reads | `oltp_point_select` | 70.84ms | 36.51ms | 0.5× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 16.00ms | 12.43ms | 0.8× | 1.6% | PASS |
| file_reads | `oltp_sum_range` | 15.33ms | 12.33ms | 0.8× | 1.5% | PASS |
| file_reads | `oltp_order_range` | 2.75ms | 2.40ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_distinct_range` | 3.39ms | 3.00ms | 0.9× | 1.1% | PASS |
| file_reads | `oltp_index_scan` | 8.59ms | 5.34ms | 0.6× | 3.7% | PASS |
| file_reads | `select_random_points` | 21.87ms | 20.00ms | 0.9× | 1.9% | PASS |
| file_reads | `select_random_ranges` | 9.76ms | 6.51ms | 0.7× | 0.8% | PASS |
| file_reads | `covering_index_scan` | 9.51ms | 6.76ms | 0.7× | 2.6% | PASS |
| file_reads | `groupby_scan` | 20.39ms | 21.16ms | 1.0× | 1.0% | PASS |
| file_reads | `index_join` | 7.93ms | 7.37ms | 0.9× | 3.3% | PASS |
| file_reads | `index_join_scan` | 3.43ms | 4.35ms | 1.3× | 2.3% | PASS |
| file_reads | `types_table_scan` | 702.02ms | 755.94ms | 1.1× | 1.5% | PASS |
| file_reads | `table_scan` | 813.25ms | 857.15ms | 1.1× | 1.5% | PASS |
| file_reads | `oltp_read_only` | 157.87ms | 110.13ms | 0.7× | 2.3% | PASS |
| file_writes | `oltp_bulk_insert` | 196.23ms | 242.11ms | 1.2× | 3.4% | PASS |
| file_writes | `oltp_insert` | 19.48ms | 28.03ms | 1.4× | 4.1% | PASS |
| file_writes | `oltp_update_index` | 140.62ms | 129.65ms | 0.9× | 6.6% | PASS |
| file_writes | `oltp_update_non_index` | 121.04ms | 96.19ms | 0.8× | 7.6% | PASS |
| file_writes | `oltp_delete_insert` | 120.68ms | 110.69ms | 0.9× | 2.6% | PASS |
| file_writes | `oltp_write_only` | 81.40ms | 79.70ms | 1.0× | 2.5% | PASS |
| file_writes | `types_delete_insert` | 70.86ms | 64.35ms | 0.9× | 3.6% | PASS |
| file_writes | `oltp_read_write` | 124.06ms | 132.73ms | 1.1× | 3.1% | PASS |
| ac_reads | `oltp_point_select` | 38.02ms | 37.12ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_range_select` | 12.46ms | 12.65ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_sum_range` | 11.98ms | 12.47ms | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_order_range` | 2.45ms | 2.39ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_distinct_range` | 2.96ms | 2.90ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_index_scan` | 5.06ms | 5.51ms | 1.1× | 2.5% | PASS |
| ac_reads | `select_random_points` | 18.64ms | 20.46ms | 1.1× | 2.2% | PASS |
| ac_reads | `select_random_ranges` | 6.40ms | 6.51ms | 1.0× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 5.84ms | 6.86ms | 1.2× | 1.4% | PASS |
| ac_reads | `groupby_scan` | 20.14ms | 21.36ms | 1.1× | 1.1% | PASS |
| ac_reads | `index_join` | 6.07ms | 7.52ms | 1.2× | 3.6% | PASS |
| ac_reads | `index_join_scan` | 2.96ms | 4.29ms | 1.4× | 2.0% | PASS |
| ac_reads | `types_table_scan` | 709.27ms | 743.34ms | 1.0× | 3.1% | PASS |
| ac_reads | `table_scan` | 862.98ms | 832.05ms | 1.0× | 6.4% | PASS |
| ac_reads | `oltp_read_only` | 109.66ms | 109.02ms | 1.0× | 3.3% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 28.79ms | 78.39ms | 2.7× | 4.4% | PASS |
| ac_writes | `oltp_insert_ac` | 32.23ms | 90.06ms | 2.8× | 6.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 36.06ms | 101.27ms | 2.8× | 11.3% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 29.90ms | 86.79ms | 2.9× | 9.1% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 31.33ms | 90.83ms | 2.9× | 3.7% | PASS |
| ac_writes | `oltp_write_only_ac` | 31.96ms | 92.51ms | 2.9× | 6.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 33.12ms | 84.56ms | 2.6× | 9.2% | PASS |
| ac_writes | `oltp_read_write_ac` | 34.41ms | 96.50ms | 2.8× | 6.3% | PASS |

</details>

</details>

## Version-control latency

Wall time: 3m 58s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 36.55ms | 130.00ms | 28.1% | 2.1% | PASS |
| `status_dirty_many_tables` | 39.67ms | 130.00ms | 30.5% | 0.8% | PASS |
| `diff_regular_working_one_table` | 31.98ms | 120.00ms | 26.7% | 1.3% | PASS |
| `diff_regular_working_many_tables` | 45.57ms | 140.00ms | 32.6% | 1.7% | PASS |
| `diff_stat_working_many_tables` | 45.43ms | 140.00ms | 32.5% | 1.3% | PASS |
| `diff_schema_working_many_tables` | 46.76ms | 140.00ms | 33.4% | 1.0% | PASS |
| `branch_list_many_branches` | 22.69ms | 35.00ms | 64.8% | 2.2% | PASS |
| `branch_create_delete` | 25.42ms | 40.00ms | 63.5% | 2.0% | PASS |
| `at_literal_deep_history` | 28.26ms | 100.00ms | 28.3% | 1.8% | PASS |
| `diff_literal_deep_history` | 28.18ms | 120.00ms | 23.5% | 1.6% | PASS |
| `history_literal_deep_history` | 29.36ms | 150.00ms | 19.6% | 2.0% | PASS |
| `checkout_branch_clean` | 39.01ms | 150.00ms | 26.0% | 2.5% | PASS |
| `merge_data_no_conflicts` | 28.31ms | 50.00ms | 56.6% | 1.5% | PASS |
| `merge_data_secondary_index` | 1.14s | 2.50s | 45.8% | 0.8% | PASS |
| `merge_schema_no_conflicts` | 22.57ms | 35.00ms | 64.5% | 2.1% | PASS |
| `merge_data_conflicts` | 30.76ms | 180.00ms | 17.1% | 1.2% | PASS |
| `merge_data_conflicts_with_resolve` | 32.10ms | 180.00ms | 17.8% | 1.0% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
