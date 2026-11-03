# DoltLite Performance Report

> Nightly result: **FAIL**
>
> Generated: 2026-11-03 06:46 UTC
>
> Commit: [`6da36b0f12f54bd0dbaf99b70d940eeb9350c4d7`](https://github.com/dolthub/doltlite/commit/6da36b0f12f54bd0dbaf99b70d940eeb9350c4d7)
>
> Runner: ubuntu24 20261026.354.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/41580613337)

The SQL workloads compare optimized DoltLite against stock SQLite on the same GitHub-hosted runner, alternating execution order on each repetition. Version-control sections measure DoltLite latency. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 8.54s | 10.38s | 1.2× | 2.0% | **PASS** |
| Writes | 1.77s | 2.93s | 1.7× | 1.8% | **FAIL** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.34s | 10.90s | 1.2× | 1.5% | **PASS** |
| Writes | 4.01s | 4.41s | 1.1× | 3.3% | **PASS** |
| Autocommit writes | 788.66ms | 2.47s | 3.1× | 5.1% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 1.57s | 1.77s | 1.1× | 2.3% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.41s | 2.60s | 1.1× | 2.0% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.06s | 2.63s | 1.3× | 2.7% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.51s | 3.38s | 1.3× | 1.3% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 267.19ms | 469.29ms | 1.8× | 2.4% | **FAIL** |
| In-memory | Writes | textpk | 8 | 55 | 483.28ms | 752.81ms | 1.6× | 1.7% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 420.20ms | 687.28ms | 1.6× | 2.4% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 594.80ms | 1.02s | 1.7× | 1.0% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 1.75s | 1.83s | 1.0× | 2.2% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.43s | 2.59s | 1.1× | 1.0% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.44s | 3.04s | 1.2× | 3.3% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.73s | 3.44s | 1.3× | 1.2% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 739.75ms | 749.14ms | 1.0× | 4.2% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.33s | 1.27s | 1.0× | 15.8% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 1.18s | 1.29s | 1.1× | 2.5% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 763.03ms | 1.10s | 1.4× | 1.4% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 1.79s | 1.88s | 1.1× | 2.6% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.59s | 2.69s | 1.0× | 1.1% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.53s | 3.18s | 1.3× | 1.1% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.59s | 3.44s | 1.3× | 1.3% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 178.02ms | 512.71ms | 2.9× | 4.2% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 195.00ms | 698.55ms | 3.6× | 36.2% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 232.82ms | 658.62ms | 2.8× | 4.0% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 182.81ms | 601.21ms | 3.3× | 5.3% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 18.40ms | 21.61ms | 1.2× | 1.7% | PASS |
| mem_reads | `oltp_range_select` | 7.68ms | 8.47ms | 1.1× | 1.9% | PASS |
| mem_reads | `oltp_sum_range` | 7.09ms | 8.36ms | 1.2× | 2.3% | PASS |
| mem_reads | `oltp_order_range` | 1.81ms | 2.02ms | 1.1× | 3.3% | PASS |
| mem_reads | `oltp_distinct_range` | 2.17ms | 2.37ms | 1.1× | 2.2% | PASS |
| mem_reads | `oltp_index_scan` | 2.45ms | 3.50ms | 1.4× | 3.4% | PASS |
| mem_reads | `select_random_points` | 7.05ms | 7.98ms | 1.1× | 3.2% | PASS |
| mem_reads | `select_random_ranges` | 2.60ms | 2.96ms | 1.1× | 5.7% | PASS |
| mem_reads | `covering_index_scan` | 3.90ms | 5.46ms | 1.4× | 2.4% | PASS |
| mem_reads | `groupby_scan` | 16.43ms | 19.06ms | 1.2× | 2.0% | PASS |
| mem_reads | `index_join` | 3.43ms | 5.17ms | 1.5× | 2.8% | PASS |
| mem_reads | `index_join_scan` | 1.72ms | 3.94ms | 2.3× | 3.5% | PASS |
| mem_reads | `types_table_scan` | 683.56ms | 770.42ms | 1.1× | 1.0% | PASS |
| mem_reads | `table_scan` | 755.89ms | 838.77ms | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_read_only` | 54.99ms | 66.48ms | 1.2× | 1.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 109.21ms | 181.06ms | 1.7× | 1.3% | PASS |
| mem_writes | `oltp_insert` | 9.31ms | 16.10ms | 1.7× | 1.1% | PASS |
| mem_writes | `oltp_update_index` | 30.73ms | 62.31ms | 2.0× | 2.5% | PASS |
| mem_writes | `oltp_update_non_index` | 21.09ms | 40.01ms | 1.9× | 1.8% | PASS |
| mem_writes | `oltp_delete_insert` | 26.56ms | 47.84ms | 1.8× | 2.4% | PASS |
| mem_writes | `oltp_write_only` | 13.27ms | 30.69ms | 2.3× | 2.5% | FAIL |
| mem_writes | `types_delete_insert` | 15.57ms | 22.40ms | 1.4× | 3.4% | PASS |
| mem_writes | `oltp_read_write` | 41.47ms | 68.87ms | 1.7× | 4.2% | PASS |
| file_reads | `oltp_point_select` | 67.19ms | 33.58ms | 0.5× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 11.51ms | 8.70ms | 0.8× | 2.0% | PASS |
| file_reads | `oltp_sum_range` | 10.98ms | 8.52ms | 0.8× | 2.0% | PASS |
| file_reads | `oltp_order_range` | 2.17ms | 1.95ms | 0.9× | 2.1% | PASS |
| file_reads | `oltp_distinct_range` | 2.61ms | 2.42ms | 0.9× | 2.3% | PASS |
| file_reads | `oltp_index_scan` | 7.51ms | 4.72ms | 0.6× | 2.6% | PASS |
| file_reads | `select_random_points` | 13.62ms | 10.13ms | 0.7× | 3.6% | PASS |
| file_reads | `select_random_ranges` | 7.60ms | 4.36ms | 0.6× | 2.5% | PASS |
| file_reads | `covering_index_scan` | 9.15ms | 6.91ms | 0.8× | 3.3% | PASS |
| file_reads | `groupby_scan` | 17.55ms | 19.54ms | 1.1× | 1.9% | PASS |
| file_reads | `index_join` | 6.54ms | 6.21ms | 0.9× | 2.2% | PASS |
| file_reads | `index_join_scan` | 2.85ms | 4.48ms | 1.6× | 4.9% | PASS |
| file_reads | `types_table_scan` | 689.83ms | 776.91ms | 1.1× | 1.0% | PASS |
| file_reads | `table_scan` | 776.53ms | 855.02ms | 1.1× | 1.1% | PASS |
| file_reads | `oltp_read_only` | 125.11ms | 84.65ms | 0.7× | 2.2% | PASS |
| file_writes | `oltp_bulk_insert` | 147.87ms | 225.18ms | 1.5× | 5.3% | PASS |
| file_writes | `oltp_insert` | 19.84ms | 27.48ms | 1.4× | 2.2% | PASS |
| file_writes | `oltp_update_index` | 121.70ms | 116.47ms | 1.0× | 6.1% | PASS |
| file_writes | `oltp_update_non_index` | 101.26ms | 81.16ms | 0.8× | 3.4% | PASS |
| file_writes | `oltp_delete_insert` | 105.37ms | 99.46ms | 0.9× | 4.4% | PASS |
| file_writes | `oltp_write_only` | 81.24ms | 60.40ms | 0.7× | 4.0% | PASS |
| file_writes | `types_delete_insert` | 61.48ms | 45.48ms | 0.7× | 8.5% | PASS |
| file_writes | `oltp_read_write` | 100.99ms | 93.52ms | 0.9× | 3.1% | PASS |
| ac_reads | `oltp_point_select` | 31.20ms | 31.87ms | 1.0× | 2.9% | PASS |
| ac_reads | `oltp_range_select` | 8.60ms | 8.89ms | 1.0× | 2.6% | PASS |
| ac_reads | `oltp_sum_range` | 8.12ms | 8.82ms | 1.1× | 2.7% | PASS |
| ac_reads | `oltp_order_range` | 1.89ms | 1.99ms | 1.1× | 3.9% | PASS |
| ac_reads | `oltp_distinct_range` | 2.22ms | 2.42ms | 1.1× | 1.5% | PASS |
| ac_reads | `oltp_index_scan` | 4.04ms | 4.82ms | 1.2× | 2.6% | PASS |
| ac_reads | `select_random_points` | 11.14ms | 10.20ms | 0.9× | 3.2% | PASS |
| ac_reads | `select_random_ranges` | 4.61ms | 4.42ms | 1.0× | 2.3% | PASS |
| ac_reads | `covering_index_scan` | 6.08ms | 7.28ms | 1.2× | 3.3% | PASS |
| ac_reads | `groupby_scan` | 17.84ms | 20.33ms | 1.1× | 1.4% | PASS |
| ac_reads | `index_join` | 5.16ms | 7.21ms | 1.4× | 3.5% | PASS |
| ac_reads | `index_join_scan` | 2.75ms | 4.87ms | 1.8× | 6.7% | PASS |
| ac_reads | `types_table_scan` | 700.20ms | 779.50ms | 1.1× | 1.6% | PASS |
| ac_reads | `table_scan` | 887.73ms | 894.01ms | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_read_only` | 96.42ms | 97.13ms | 1.0× | 1.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 20.49ms | 55.39ms | 2.7× | 3.8% | PASS |
| ac_writes | `oltp_insert_ac` | 22.11ms | 61.96ms | 2.8× | 3.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 23.20ms | 70.44ms | 3.0× | 4.8% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 20.87ms | 59.75ms | 2.9× | 6.2% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 22.60ms | 66.69ms | 3.0× | 3.9% | PASS |
| ac_writes | `oltp_write_only_ac` | 22.76ms | 65.79ms | 2.9× | 4.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 20.51ms | 61.66ms | 3.0× | 5.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 25.49ms | 71.04ms | 2.8× | 4.4% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 31.28ms | 30.72ms | 1.0× | 2.0% | PASS |
| mem_reads | `oltp_range_select` | 15.14ms | 12.51ms | 0.8× | 3.0% | PASS |
| mem_reads | `oltp_sum_range` | 13.67ms | 12.49ms | 0.9× | 2.1% | PASS |
| mem_reads | `oltp_order_range` | 2.88ms | 2.84ms | 1.0× | 1.5% | PASS |
| mem_reads | `oltp_distinct_range` | 3.69ms | 3.70ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 3.25ms | 5.28ms | 1.6× | 1.6% | PASS |
| mem_reads | `select_random_points` | 20.68ms | 17.83ms | 0.9× | 5.0% | PASS |
| mem_reads | `select_random_ranges` | 5.78ms | 5.36ms | 0.9× | 1.9% | PASS |
| mem_reads | `covering_index_scan` | 6.11ms | 8.38ms | 1.4× | 1.2% | PASS |
| mem_reads | `groupby_scan` | 29.68ms | 30.17ms | 1.0× | 0.8% | PASS |
| mem_reads | `index_join` | 9.51ms | 8.64ms | 0.9× | 4.3% | PASS |
| mem_reads | `index_join_scan` | 3.24ms | 5.73ms | 1.8× | 3.2% | PASS |
| mem_reads | `types_table_scan` | 959.77ms | 1.12s | 1.2× | 2.6% | PASS |
| mem_reads | `table_scan` | 1.18s | 1.22s | 1.0× | 1.5% | PASS |
| mem_reads | `oltp_read_only` | 121.64ms | 116.55ms | 1.0× | 2.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 190.68ms | 274.69ms | 1.4× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 14.56ms | 28.41ms | 2.0× | 1.1% | PASS |
| mem_writes | `oltp_update_index` | 54.83ms | 110.70ms | 2.0× | 1.8% | PASS |
| mem_writes | `oltp_update_non_index` | 42.15ms | 62.50ms | 1.5× | 2.2% | PASS |
| mem_writes | `oltp_delete_insert` | 46.86ms | 82.73ms | 1.8× | 1.9% | PASS |
| mem_writes | `oltp_write_only` | 25.07ms | 43.61ms | 1.7× | 1.6% | PASS |
| mem_writes | `types_delete_insert` | 33.23ms | 48.53ms | 1.5× | 2.7% | PASS |
| mem_writes | `oltp_read_write` | 75.89ms | 101.62ms | 1.3× | 1.3% | PASS |
| file_reads | `oltp_point_select` | 90.42ms | 46.67ms | 0.5× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 21.36ms | 14.39ms | 0.7× | 1.2% | PASS |
| file_reads | `oltp_sum_range` | 19.39ms | 13.98ms | 0.7× | 1.5% | PASS |
| file_reads | `oltp_order_range` | 3.54ms | 3.00ms | 0.8× | 0.9% | PASS |
| file_reads | `oltp_distinct_range` | 4.35ms | 3.86ms | 0.9× | 1.0% | PASS |
| file_reads | `oltp_index_scan` | 9.59ms | 6.89ms | 0.7× | 0.8% | PASS |
| file_reads | `select_random_points` | 24.96ms | 19.12ms | 0.8× | 1.9% | PASS |
| file_reads | `select_random_ranges` | 11.89ms | 7.00ms | 0.6× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 12.49ms | 9.95ms | 0.8× | 0.6% | PASS |
| file_reads | `groupby_scan` | 29.74ms | 29.99ms | 1.0× | 0.8% | PASS |
| file_reads | `index_join` | 12.52ms | 9.36ms | 0.7× | 1.3% | PASS |
| file_reads | `index_join_scan` | 3.86ms | 5.77ms | 1.5× | 1.6% | PASS |
| file_reads | `types_table_scan` | 986.88ms | 1.12s | 1.1× | 4.2% | PASS |
| file_reads | `table_scan` | 1.00s | 1.16s | 1.2× | 1.3% | PASS |
| file_reads | `oltp_read_only` | 197.57ms | 135.38ms | 0.7× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 270.38ms | 351.64ms | 1.3× | 8.8% | PASS |
| file_writes | `oltp_insert` | 27.75ms | 49.27ms | 1.8× | 22.4% | PASS |
| file_writes | `oltp_update_index` | 167.90ms | 189.80ms | 1.1× | 2.6% | PASS |
| file_writes | `oltp_update_non_index` | 141.71ms | 126.62ms | 0.9× | 12.5% | PASS |
| file_writes | `oltp_delete_insert` | 201.50ms | 171.94ms | 0.9× | 18.6% | PASS |
| file_writes | `oltp_write_only` | 151.10ms | 105.50ms | 0.7× | 45.8% | PASS |
| file_writes | `types_delete_insert` | 158.14ms | 104.65ms | 0.7× | 18.4% | PASS |
| file_writes | `oltp_read_write` | 210.67ms | 166.94ms | 0.8× | 13.2% | PASS |
| ac_reads | `oltp_point_select` | 51.33ms | 47.32ms | 0.9× | 1.4% | PASS |
| ac_reads | `oltp_range_select` | 17.20ms | 14.34ms | 0.8× | 1.4% | PASS |
| ac_reads | `oltp_sum_range` | 15.60ms | 14.15ms | 0.9× | 0.9% | PASS |
| ac_reads | `oltp_order_range` | 3.18ms | 2.99ms | 0.9× | 1.1% | PASS |
| ac_reads | `oltp_distinct_range` | 3.96ms | 3.86ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_index_scan` | 5.61ms | 6.83ms | 1.2× | 0.7% | PASS |
| ac_reads | `select_random_points` | 21.48ms | 19.38ms | 0.9× | 2.9% | PASS |
| ac_reads | `select_random_ranges` | 7.92ms | 6.97ms | 0.9× | 0.8% | PASS |
| ac_reads | `covering_index_scan` | 8.48ms | 9.95ms | 1.2× | 0.8% | PASS |
| ac_reads | `groupby_scan` | 29.74ms | 30.36ms | 1.0× | 0.8% | PASS |
| ac_reads | `index_join` | 10.69ms | 9.40ms | 0.9× | 1.7% | PASS |
| ac_reads | `index_join_scan` | 3.55ms | 5.79ms | 1.6× | 1.5% | PASS |
| ac_reads | `types_table_scan` | 1.03s | 1.14s | 1.1× | 3.3% | PASS |
| ac_reads | `table_scan` | 1.23s | 1.24s | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_read_only` | 143.85ms | 136.95ms | 1.0× | 2.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 20.98ms | 60.46ms | 2.9× | 18.2% | PASS |
| ac_writes | `oltp_insert_ac` | 24.33ms | 82.30ms | 3.4× | 45.7% | PASS |
| ac_writes | `oltp_update_index_ac` | 22.97ms | 93.58ms | 4.1× | 21.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 20.21ms | 74.90ms | 3.7× | 26.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.41ms | 86.89ms | 3.4× | 50.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 20.93ms | 73.94ms | 3.5× | 21.8% | PASS |
| ac_writes | `types_delete_insert_ac` | 23.32ms | 88.59ms | 3.8× | 62.2% | PASS |
| ac_writes | `oltp_read_write_ac` | 36.86ms | 137.90ms | 3.7× | 51.9% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 26.57ms | 28.46ms | 1.1× | 3.4% | PASS |
| mem_reads | `oltp_range_select` | 10.93ms | 12.60ms | 1.2× | 3.5% | PASS |
| mem_reads | `oltp_sum_range` | 10.50ms | 11.83ms | 1.1× | 4.3% | PASS |
| mem_reads | `oltp_order_range` | 2.51ms | 2.89ms | 1.2× | 2.5% | PASS |
| mem_reads | `oltp_distinct_range` | 3.30ms | 3.77ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_index_scan` | 3.08ms | 4.91ms | 1.6× | 2.7% | PASS |
| mem_reads | `select_random_points` | 17.13ms | 17.74ms | 1.0× | 2.9% | PASS |
| mem_reads | `select_random_ranges` | 4.83ms | 5.05ms | 1.0× | 2.6% | PASS |
| mem_reads | `covering_index_scan` | 5.58ms | 8.27ms | 1.5× | 4.8% | PASS |
| mem_reads | `groupby_scan` | 28.15ms | 29.91ms | 1.1× | 2.3% | PASS |
| mem_reads | `index_join` | 9.10ms | 8.59ms | 0.9× | 2.8% | PASS |
| mem_reads | `index_join_scan` | 2.64ms | 5.26ms | 2.0× | 3.6% | PASS |
| mem_reads | `types_table_scan` | 859.67ms | 1.14s | 1.3× | 0.8% | PASS |
| mem_reads | `table_scan` | 970.14ms | 1.24s | 1.3× | 0.6% | PASS |
| mem_reads | `oltp_read_only` | 102.73ms | 117.36ms | 1.1× | 1.7% | PASS |
| mem_writes | `oltp_bulk_insert` | 169.43ms | 257.01ms | 1.5× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 13.26ms | 26.40ms | 2.0× | 2.4% | PASS |
| mem_writes | `oltp_update_index` | 47.98ms | 98.64ms | 2.1× | 3.0% | PASS |
| mem_writes | `oltp_update_non_index` | 34.78ms | 55.47ms | 1.6× | 2.4% | PASS |
| mem_writes | `oltp_delete_insert` | 38.93ms | 72.21ms | 1.9× | 1.9% | PASS |
| mem_writes | `oltp_write_only` | 20.23ms | 37.82ms | 1.9× | 3.0% | PASS |
| mem_writes | `types_delete_insert` | 27.79ms | 42.77ms | 1.5× | 2.8% | PASS |
| mem_writes | `oltp_read_write` | 67.81ms | 96.97ms | 1.4× | 1.9% | PASS |
| file_reads | `oltp_point_select` | 50.13ms | 34.66ms | 0.7× | 2.8% | PASS |
| file_reads | `oltp_range_select` | 14.09ms | 13.88ms | 1.0× | 3.8% | PASS |
| file_reads | `oltp_sum_range` | 13.93ms | 13.20ms | 0.9× | 2.9% | PASS |
| file_reads | `oltp_order_range` | 2.95ms | 3.09ms | 1.0× | 3.3% | PASS |
| file_reads | `oltp_distinct_range` | 3.76ms | 3.95ms | 1.1× | 4.0% | PASS |
| file_reads | `oltp_index_scan` | 5.73ms | 5.91ms | 1.0× | 4.5% | PASS |
| file_reads | `select_random_points` | 20.84ms | 19.34ms | 0.9× | 3.3% | PASS |
| file_reads | `select_random_ranges` | 7.44ms | 5.80ms | 0.8× | 3.5% | PASS |
| file_reads | `covering_index_scan` | 8.11ms | 8.89ms | 1.1× | 4.0% | PASS |
| file_reads | `groupby_scan` | 28.25ms | 30.11ms | 1.1× | 2.1% | PASS |
| file_reads | `index_join` | 10.74ms | 9.46ms | 0.9× | 3.6% | PASS |
| file_reads | `index_join_scan` | 3.10ms | 5.74ms | 1.9× | 3.8% | PASS |
| file_reads | `types_table_scan` | 985.21ms | 1.31s | 1.3× | 0.5% | PASS |
| file_reads | `table_scan` | 1.12s | 1.44s | 1.3× | 0.3% | PASS |
| file_reads | `oltp_read_only` | 161.59ms | 145.87ms | 0.9× | 0.5% | PASS |
| file_writes | `oltp_bulk_insert` | 265.80ms | 368.71ms | 1.4× | 2.2% | PASS |
| file_writes | `oltp_insert` | 30.26ms | 56.77ms | 1.9× | 2.8% | PASS |
| file_writes | `oltp_update_index` | 167.79ms | 208.41ms | 1.2× | 5.8% | PASS |
| file_writes | `oltp_update_non_index` | 137.35ms | 127.66ms | 0.9× | 1.3% | PASS |
| file_writes | `oltp_delete_insert` | 164.87ms | 159.96ms | 1.0× | 3.7% | PASS |
| file_writes | `oltp_write_only` | 115.68ms | 102.83ms | 0.9× | 0.9% | PASS |
| file_writes | `types_delete_insert` | 123.51ms | 92.90ms | 0.8× | 11.4% | PASS |
| file_writes | `oltp_read_write` | 176.42ms | 176.01ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_point_select` | 41.55ms | 40.97ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 15.12ms | 16.29ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_sum_range` | 14.68ms | 15.18ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_order_range` | 3.25ms | 3.58ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_distinct_range` | 4.18ms | 4.63ms | 1.1× | 0.6% | PASS |
| ac_reads | `oltp_index_scan` | 4.98ms | 6.80ms | 1.4× | 1.2% | PASS |
| ac_reads | `select_random_points` | 22.95ms | 22.31ms | 1.0× | 1.1% | PASS |
| ac_reads | `select_random_ranges` | 7.09ms | 6.80ms | 1.0× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 7.78ms | 10.64ms | 1.4× | 0.8% | PASS |
| ac_reads | `groupby_scan` | 33.27ms | 35.53ms | 1.1× | 0.7% | PASS |
| ac_reads | `index_join` | 11.74ms | 11.09ms | 0.9× | 1.0% | PASS |
| ac_reads | `index_join_scan` | 3.54ms | 6.67ms | 1.9× | 1.1% | PASS |
| ac_reads | `types_table_scan` | 1.02s | 1.37s | 1.3× | 0.7% | PASS |
| ac_reads | `table_scan` | 1.20s | 1.48s | 1.2× | 1.3% | PASS |
| ac_reads | `oltp_read_only` | 138.14ms | 148.66ms | 1.1× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 27.80ms | 70.73ms | 2.5× | 4.2% | PASS |
| ac_writes | `oltp_insert_ac` | 29.83ms | 83.53ms | 2.8× | 3.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 29.64ms | 92.03ms | 3.1× | 2.8% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 26.52ms | 74.86ms | 2.8× | 3.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 29.45ms | 86.23ms | 2.9× | 5.0% | PASS |
| ac_writes | `oltp_write_only_ac` | 28.63ms | 83.27ms | 2.9× | 4.8% | PASS |
| ac_writes | `types_delete_insert_ac` | 28.01ms | 78.69ms | 2.8× | 5.6% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.96ms | 89.28ms | 2.7× | 3.8% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.13ms | 44.31ms | 1.3× | 1.3% | PASS |
| mem_reads | `oltp_range_select` | 19.39ms | 24.96ms | 1.3× | 1.7% | PASS |
| mem_reads | `oltp_sum_range` | 17.76ms | 23.65ms | 1.3× | 1.0% | PASS |
| mem_reads | `oltp_order_range` | 3.52ms | 4.28ms | 1.2× | 2.0% | PASS |
| mem_reads | `oltp_distinct_range` | 4.62ms | 5.50ms | 1.2× | 1.3% | PASS |
| mem_reads | `oltp_index_scan` | 4.43ms | 6.63ms | 1.5× | 1.7% | PASS |
| mem_reads | `select_random_points` | 27.85ms | 34.38ms | 1.2× | 1.6% | PASS |
| mem_reads | `select_random_ranges` | 7.66ms | 9.69ms | 1.3× | 1.2% | PASS |
| mem_reads | `covering_index_scan` | 7.70ms | 11.65ms | 1.5× | 1.1% | PASS |
| mem_reads | `groupby_scan` | 36.41ms | 43.73ms | 1.2× | 0.8% | PASS |
| mem_reads | `index_join` | 7.88ms | 11.61ms | 1.5× | 1.3% | PASS |
| mem_reads | `index_join_scan` | 3.93ms | 6.68ms | 1.7× | 2.2% | PASS |
| mem_reads | `types_table_scan` | 1.03s | 1.43s | 1.4× | 0.4% | PASS |
| mem_reads | `table_scan` | 1.16s | 1.53s | 1.3× | 0.5% | PASS |
| mem_reads | `oltp_read_only` | 149.66ms | 191.28ms | 1.3× | 1.2% | PASS |
| mem_writes | `oltp_bulk_insert` | 249.27ms | 374.36ms | 1.5× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 18.94ms | 35.63ms | 1.9× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 65.83ms | 131.44ms | 2.0× | 0.8% | PASS |
| mem_writes | `oltp_update_non_index` | 51.02ms | 92.25ms | 1.8× | 1.1% | PASS |
| mem_writes | `oltp_delete_insert` | 49.23ms | 99.83ms | 2.0× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 26.86ms | 60.65ms | 2.3× | 1.0% | PASS |
| mem_writes | `types_delete_insert` | 32.37ms | 60.25ms | 1.9× | 1.4% | PASS |
| mem_writes | `oltp_read_write` | 101.27ms | 168.23ms | 1.7× | 0.8% | PASS |
| file_reads | `oltp_point_select` | 98.77ms | 61.77ms | 0.6× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 26.59ms | 27.01ms | 1.0× | 1.6% | PASS |
| file_reads | `oltp_sum_range` | 24.97ms | 25.90ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_order_range` | 4.35ms | 4.58ms | 1.1× | 1.5% | PASS |
| file_reads | `oltp_distinct_range` | 5.52ms | 5.80ms | 1.1× | 1.4% | PASS |
| file_reads | `oltp_index_scan` | 11.09ms | 8.82ms | 0.8× | 1.7% | PASS |
| file_reads | `select_random_points` | 36.13ms | 38.26ms | 1.1× | 1.4% | PASS |
| file_reads | `select_random_ranges` | 14.54ms | 11.90ms | 0.8× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 14.57ms | 13.81ms | 0.9× | 1.2% | PASS |
| file_reads | `groupby_scan` | 37.35ms | 44.18ms | 1.2× | 0.8% | PASS |
| file_reads | `index_join` | 11.69ms | 13.60ms | 1.2× | 1.1% | PASS |
| file_reads | `index_join_scan` | 4.91ms | 7.13ms | 1.5× | 2.0% | PASS |
| file_reads | `types_table_scan` | 1.03s | 1.43s | 1.4× | 0.5% | PASS |
| file_reads | `table_scan` | 1.16s | 1.52s | 1.3× | 0.4% | PASS |
| file_reads | `oltp_read_only` | 248.82ms | 219.25ms | 0.9× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 263.98ms | 388.46ms | 1.5× | 0.8% | PASS |
| file_writes | `oltp_insert` | 25.86ms | 39.71ms | 1.5× | 1.3% | PASS |
| file_writes | `oltp_update_index` | 94.91ms | 146.79ms | 1.5× | 1.1% | PASS |
| file_writes | `oltp_update_non_index` | 76.00ms | 99.73ms | 1.3× | 1.4% | PASS |
| file_writes | `oltp_delete_insert` | 75.56ms | 112.91ms | 1.5× | 1.8% | PASS |
| file_writes | `oltp_write_only` | 50.16ms | 67.60ms | 1.3× | 1.8% | PASS |
| file_writes | `types_delete_insert` | 49.45ms | 66.09ms | 1.3× | 1.6% | PASS |
| file_writes | `oltp_read_write` | 127.12ms | 175.63ms | 1.4× | 1.1% | PASS |
| ac_reads | `oltp_point_select` | 55.49ms | 61.72ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_range_select` | 22.29ms | 27.03ms | 1.2× | 1.5% | PASS |
| ac_reads | `oltp_sum_range` | 20.34ms | 25.90ms | 1.3× | 1.2% | PASS |
| ac_reads | `oltp_order_range` | 3.83ms | 4.58ms | 1.2× | 1.5% | PASS |
| ac_reads | `oltp_distinct_range` | 4.99ms | 5.84ms | 1.2× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 6.79ms | 8.80ms | 1.3× | 1.6% | PASS |
| ac_reads | `select_random_points` | 31.07ms | 38.29ms | 1.2× | 1.8% | PASS |
| ac_reads | `select_random_ranges` | 10.02ms | 11.91ms | 1.2× | 1.5% | PASS |
| ac_reads | `covering_index_scan` | 10.07ms | 13.85ms | 1.4× | 1.3% | PASS |
| ac_reads | `groupby_scan` | 36.77ms | 44.09ms | 1.2× | 0.9% | PASS |
| ac_reads | `index_join` | 9.40ms | 13.60ms | 1.4× | 1.6% | PASS |
| ac_reads | `index_join_scan` | 4.40ms | 7.16ms | 1.6× | 1.6% | PASS |
| ac_reads | `types_table_scan` | 1.03s | 1.43s | 1.4× | 0.4% | PASS |
| ac_reads | `table_scan` | 1.16s | 1.52s | 1.3× | 0.5% | PASS |
| ac_reads | `oltp_read_only` | 184.15ms | 220.21ms | 1.2× | 0.7% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 19.64ms | 59.11ms | 3.0× | 6.2% | PASS |
| ac_writes | `oltp_insert_ac` | 23.20ms | 76.43ms | 3.3× | 5.3% | PASS |
| ac_writes | `oltp_update_index_ac` | 24.06ms | 86.72ms | 3.6× | 5.5% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.02ms | 68.96ms | 3.3× | 3.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 22.74ms | 76.64ms | 3.4× | 5.4% | PASS |
| ac_writes | `oltp_write_only_ac` | 22.83ms | 76.67ms | 3.4× | 4.9% | PASS |
| ac_writes | `types_delete_insert_ac` | 20.05ms | 71.64ms | 3.6× | 6.1% | PASS |
| ac_writes | `oltp_read_write_ac` | 29.28ms | 85.03ms | 2.9× | 4.9% | PASS |

</details>

</details>

## Version-control latency

Wall time: 7m 5s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 33.59ms | 38.00ms | 88.4% | 0.9% | PASS |
| `status_dirty_many_tables` | 36.71ms | 42.00ms | 87.4% | 0.8% | PASS |
| `diff_regular_working_one_table` | 28.39ms | 33.00ms | 86.0% | 0.8% | PASS |
| `diff_regular_working_many_tables` | 42.21ms | 50.00ms | 84.4% | 0.7% | PASS |
| `diff_stat_working_many_tables` | 42.21ms | 48.00ms | 87.9% | 0.8% | PASS |
| `diff_schema_working_many_tables` | 43.17ms | 48.00ms | 89.9% | 0.6% | PASS |
| `branch_list_many_branches` | 20.85ms | 25.00ms | 83.4% | 1.2% | PASS |
| `branch_create_delete` | 22.66ms | 27.00ms | 83.9% | 1.2% | PASS |
| `at_literal_deep_history` | 22.28ms | 28.00ms | 79.6% | 0.9% | PASS |
| `diff_literal_deep_history` | 22.20ms | 28.00ms | 79.3% | 1.1% | PASS |
| `history_literal_deep_history` | 23.33ms | 30.00ms | 77.8% | 0.9% | PASS |
| `checkout_branch_clean` | 32.63ms | 43.00ms | 75.9% | 0.7% | PASS |
| `merge_data_no_conflicts` | 25.20ms | 33.00ms | 76.3% | 1.2% | PASS |
| `merge_data_secondary_index` | 867.85ms | 944.00ms | 91.9% | 0.7% | PASS |
| `merge_schema_no_conflicts` | 20.02ms | 24.00ms | 83.4% | 1.5% | PASS |
| `merge_data_conflicts` | 27.63ms | 33.00ms | 83.7% | 1.1% | PASS |
| `merge_data_conflicts_with_resolve` | 28.48ms | 34.00ms | 83.8% | 0.9% | PASS |

Version-control ceiling result: **PASS**.

## File remote performance

Status: passed. Transport: local file-backed remote.

100,000,000 logical payload bytes; 100,000 rows; 1,000 changed rows; 5 repetitions per operation and binary.

SQL timings exclude fixture generation, copies, database open and verification. Each SQL sample uses a fresh process with the OS cache primed by setup verification. SQL client cache: 65,536 KiB. copy_fsync times shutil.copyfile plus fsync.

Throughput uses logical payload bytes; destination growth is file-size change, not transferred bytes.

Nightly median ceilings require three-attempt confirmation; copy_fsync is a control.

| Operation | Ceiling ms | Result |
|---|---:|---|
| push_initial | 2000 | PASS |
| clone_initial | 3000 | PASS |
| push_noop | 10 | PASS |
| fetch_noop | 10 | PASS |
| push_incremental | 750 | PASS |
| fetch_incremental | 300 | PASS |
| pull_fast_forward | 300 | PASS |

Timing gate: PASS after 1 attempt(s).

candidate: 6da36b0.

Fixture file bytes: base 107,968,301; delta 115,879,032.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 499.717 | 455.794–557.975 | 37.778 | 200.11 | 107968301 |
| push_initial | candidate | 835.586 | 834.861–844.611 | 422.654 | 119.68 | 107956807 |
| clone_initial | candidate | 962.465 | 956.777–962.916 | 548.054 | 103.90 | 108475105 |
| push_noop | candidate | 0.796 | 0.757–0.867 | 0.296 | — | 0 |
| fetch_noop | candidate | 0.138 | 0.120–0.142 | 0.140 | — | 0 |
| push_incremental | candidate | 203.822 | 192.188–212.108 | 170.266 | 4.91 | 7909775 |
| fetch_incremental | candidate | 106.606 | 106.202–109.207 | 75.058 | 9.38 | 7910084 |
| pull_fast_forward | candidate | 93.557 | 89.841–98.602 | 66.595 | 10.69 | 7910844 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 90.59 | 175.487 | 108.763 | 18329 | 2066 |
| clone_initial | candidate | 90.94 | 295.138 | 109.784 | 47266 | 2337 |
| push_noop | candidate | 42.47 | 0.073 | 0.000 | 26 | 2 |
| fetch_noop | candidate | 42.47 | 0.135 | 0.000 | 40 | 0 |
| push_incremental | candidate | 42.47 | 96.798 | 7.910 | 22408 | 125 |
| fetch_incremental | candidate | 42.47 | 34.472 | 7.910 | 10808 | 124 |
| pull_fast_forward | candidate | 42.47 | 26.497 | 7.911 | 7031 | 126 |

</details>


<details>
<summary>1 GB file remotes (observational)</summary>

Both database sizes use a fixed 1 MB scattered update and a warm OS cache. 1 GB timings have no ceiling while a baseline is collected.

### 1 GB file remote performance

Status: passed. Transport: local file-backed remote.

1,000,000,000 logical payload bytes; 1,000,000 rows; 1,000 changed rows; 3 repetitions per operation and binary.

SQL timings exclude fixture generation, copies, database open and verification. Each SQL sample uses a fresh process with the OS cache primed by setup verification. SQL client cache: 65,536 KiB. copy_fsync times shutil.copyfile plus fsync.

Throughput uses logical payload bytes; destination growth is file-size change, not transferred bytes.

No speed thresholds are enforced.

candidate: 6da36b0.

Fixture file bytes: base 1,087,718,066; delta 1,103,525,194.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 5413.096 | 5327.449–5679.024 | 306.836 | 184.74 | 1087718066 |
| push_initial | candidate | 7871.362 | 7864.317–7877.354 | 4110.895 | 127.04 | 1079566310 |
| clone_initial | candidate | 9880.840 | 9662.155–10286.002 | 5954.010 | 101.21 | 1102407396 |
| push_noop | candidate | 0.627 | 0.577–2.531 | 0.278 | — | 0 |
| fetch_noop | candidate | 0.163 | 0.163–0.165 | 0.164 | — | 0 |
| push_incremental | candidate | 1077.999 | 1033.825–1199.795 | 954.186 | 0.93 | 15806003 |
| fetch_incremental | candidate | 285.399 | 280.898–289.021 | 217.976 | 3.50 | 15806513 |
| pull_fast_forward | candidate | 288.694 | 282.068–339.186 | 213.263 | 3.46 | 15807273 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 134.86 | 2092.034 | 1087.622 | 263536 | 20541 |
| clone_initial | candidate | 123.94 | 3518.553 | 1132.819 | 612484 | 32243 |
| push_noop | candidate | 45.34 | 0.078 | 0.000 | 27 | 2 |
| fetch_noop | candidate | 45.34 | 0.137 | 0.000 | 40 | 0 |
| push_incremental | candidate | 45.34 | 616.975 | 15.928 | 149866 | 313 |
| fetch_incremental | candidate | 50.55 | 62.105 | 15.928 | 13864 | 313 |
| pull_fast_forward | candidate | 50.57 | 62.038 | 15.929 | 13860 | 315 |

</details>


</details>

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh`, `test/vc_perf_ceiling.sh`, and `test/remote_perf.py`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
