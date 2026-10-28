# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-10-28 11:29 UTC
>
> Commit: [`80a9ec5b57e24c77b2486462ad924fe66b9481f8`](https://github.com/dolthub/doltlite/commit/80a9ec5b57e24c77b2486462ad924fe66b9481f8)
>
> Runner: ubuntu24 20261026.354.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/40664957165)

The SQL workloads compare optimized DoltLite against stock SQLite on the same GitHub-hosted runner, alternating execution order on each repetition. Version-control sections measure DoltLite latency. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.48s | 11.25s | 1.1× | 1.7% | **PASS** |
| Writes | 2.02s | 3.40s | 1.7× | 1.7% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 11.15s | 11.41s | 1.0× | 1.6% | **PASS** |
| Writes | 3.31s | 4.00s | 1.2× | 1.9% | **PASS** |
| Autocommit writes | 817.31ms | 2.48s | 3.0× | 6.5% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.53s | 2.77s | 1.1× | 1.3% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 3.06s | 3.23s | 1.1× | 1.8% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.75s | 3.16s | 1.2× | 1.2% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.14s | 2.10s | 1.0× | 2.4% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 438.67ms | 793.76ms | 1.8× | 1.8% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 604.05ms | 997.00ms | 1.7× | 1.3% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 588.63ms | 978.35ms | 1.7× | 1.5% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 387.97ms | 630.32ms | 1.6× | 2.2% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.61s | 2.78s | 1.1× | 1.6% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 3.29s | 3.28s | 1.0× | 1.3% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.90s | 3.18s | 1.1× | 1.5% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.35s | 2.17s | 0.9× | 1.8% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 597.51ms | 837.16ms | 1.4× | 1.5% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 982.14ms | 1.13s | 1.1× | 3.9% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 816.86ms | 1.08s | 1.3× | 1.8% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 913.40ms | 954.10ms | 1.0× | 3.7% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.48s | 2.78s | 1.1× | 1.7% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 3.14s | 3.29s | 1.0× | 1.6% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.70s | 3.17s | 1.2× | 1.7% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 1.85s | 2.03s | 1.1× | 2.0% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 163.94ms | 563.84ms | 3.4× | 6.4% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 222.99ms | 691.24ms | 3.1× | 6.2% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 227.28ms | 710.95ms | 3.1× | 8.1% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 203.10ms | 513.53ms | 2.5× | 5.0% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.02ms | 32.11ms | 1.3× | 1.7% | PASS |
| mem_reads | `oltp_range_select` | 10.56ms | 12.48ms | 1.2× | 1.4% | PASS |
| mem_reads | `oltp_sum_range` | 9.60ms | 12.19ms | 1.3× | 1.3% | PASS |
| mem_reads | `oltp_order_range` | 2.61ms | 2.99ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_distinct_range` | 3.65ms | 4.14ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 3.87ms | 5.83ms | 1.5× | 1.3% | PASS |
| mem_reads | `select_random_points` | 10.03ms | 12.11ms | 1.2× | 2.1% | PASS |
| mem_reads | `select_random_ranges` | 4.66ms | 5.46ms | 1.2× | 1.2% | PASS |
| mem_reads | `covering_index_scan` | 7.53ms | 10.74ms | 1.4× | 1.2% | PASS |
| mem_reads | `groupby_scan` | 29.54ms | 33.29ms | 1.1× | 0.8% | PASS |
| mem_reads | `index_join` | 5.84ms | 9.50ms | 1.6× | 1.5% | PASS |
| mem_reads | `index_join_scan` | 3.15ms | 5.94ms | 1.9× | 2.0% | PASS |
| mem_reads | `types_table_scan` | 1.07s | 1.20s | 1.1× | 1.2% | PASS |
| mem_reads | `table_scan` | 1.24s | 1.30s | 1.0× | 0.6% | PASS |
| mem_reads | `oltp_read_only` | 105.12ms | 126.35ms | 1.2× | 2.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 172.92ms | 305.87ms | 1.8× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 14.84ms | 27.74ms | 1.9× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 50.38ms | 102.98ms | 2.0× | 2.4% | PASS |
| mem_writes | `oltp_update_non_index` | 35.17ms | 65.54ms | 1.9× | 1.3% | PASS |
| mem_writes | `oltp_delete_insert` | 46.10ms | 81.70ms | 1.8× | 1.7% | PASS |
| mem_writes | `oltp_write_only` | 22.52ms | 51.13ms | 2.3× | 2.0% | PASS |
| mem_writes | `types_delete_insert` | 24.76ms | 39.53ms | 1.6× | 1.8% | PASS |
| mem_writes | `oltp_read_write` | 71.97ms | 119.27ms | 1.7× | 2.0% | PASS |
| file_reads | `oltp_point_select` | 89.49ms | 49.60ms | 0.6× | 1.5% | PASS |
| file_reads | `oltp_range_select` | 18.04ms | 14.61ms | 0.8× | 1.6% | PASS |
| file_reads | `oltp_sum_range` | 16.92ms | 14.44ms | 0.9× | 1.6% | PASS |
| file_reads | `oltp_order_range` | 3.52ms | 3.32ms | 0.9× | 3.0% | PASS |
| file_reads | `oltp_distinct_range` | 4.49ms | 4.43ms | 1.0× | 1.6% | PASS |
| file_reads | `oltp_index_scan` | 10.71ms | 7.75ms | 0.7× | 1.1% | PASS |
| file_reads | `select_random_points` | 17.18ms | 14.19ms | 0.8× | 2.4% | PASS |
| file_reads | `select_random_ranges` | 10.94ms | 7.30ms | 0.7× | 2.1% | PASS |
| file_reads | `covering_index_scan` | 14.29ms | 12.86ms | 0.9× | 1.1% | PASS |
| file_reads | `groupby_scan` | 30.24ms | 33.52ms | 1.1× | 0.9% | PASS |
| file_reads | `index_join` | 9.74ms | 10.80ms | 1.1× | 1.8% | PASS |
| file_reads | `index_join_scan` | 4.09ms | 6.34ms | 1.5× | 2.5% | PASS |
| file_reads | `types_table_scan` | 1.03s | 1.18s | 1.1× | 0.7% | PASS |
| file_reads | `table_scan` | 1.16s | 1.27s | 1.1× | 0.7% | PASS |
| file_reads | `oltp_read_only` | 192.66ms | 149.89ms | 0.8× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 186.38ms | 315.18ms | 1.7× | 1.4% | PASS |
| file_writes | `oltp_insert` | 23.39ms | 30.51ms | 1.3× | 1.0% | PASS |
| file_writes | `oltp_update_index` | 74.55ms | 108.17ms | 1.5× | 1.6% | PASS |
| file_writes | `oltp_update_non_index` | 59.34ms | 71.07ms | 1.2× | 1.5% | PASS |
| file_writes | `oltp_delete_insert` | 69.51ms | 91.23ms | 1.3× | 1.3% | PASS |
| file_writes | `oltp_write_only` | 48.45ms | 55.44ms | 1.1× | 1.6% | PASS |
| file_writes | `types_delete_insert` | 40.43ms | 44.93ms | 1.1× | 1.8% | PASS |
| file_writes | `oltp_read_write` | 95.45ms | 120.63ms | 1.3× | 1.8% | PASS |
| ac_reads | `oltp_point_select` | 45.37ms | 48.93ms | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_range_select` | 13.58ms | 14.55ms | 1.1× | 2.2% | PASS |
| ac_reads | `oltp_sum_range` | 12.36ms | 14.23ms | 1.2× | 2.1% | PASS |
| ac_reads | `oltp_order_range` | 2.97ms | 3.24ms | 1.1× | 1.5% | PASS |
| ac_reads | `oltp_distinct_range` | 3.94ms | 4.38ms | 1.1× | 1.6% | PASS |
| ac_reads | `oltp_index_scan` | 5.97ms | 7.54ms | 1.3× | 2.7% | PASS |
| ac_reads | `select_random_points` | 11.36ms | 13.81ms | 1.2× | 2.4% | PASS |
| ac_reads | `select_random_ranges` | 6.81ms | 7.30ms | 1.1× | 2.1% | PASS |
| ac_reads | `covering_index_scan` | 10.18ms | 12.90ms | 1.3× | 2.0% | PASS |
| ac_reads | `groupby_scan` | 29.83ms | 33.58ms | 1.1× | 0.5% | PASS |
| ac_reads | `index_join` | 7.20ms | 10.53ms | 1.5× | 1.7% | PASS |
| ac_reads | `index_join_scan` | 3.54ms | 6.21ms | 1.8× | 1.8% | PASS |
| ac_reads | `types_table_scan` | 1.03s | 1.18s | 1.1× | 0.7% | PASS |
| ac_reads | `table_scan` | 1.16s | 1.27s | 1.1× | 0.8% | PASS |
| ac_reads | `oltp_read_only` | 131.62ms | 150.19ms | 1.1× | 1.5% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 17.78ms | 57.74ms | 3.2× | 7.2% | PASS |
| ac_writes | `oltp_insert_ac` | 20.00ms | 70.07ms | 3.5× | 3.6% | PASS |
| ac_writes | `oltp_update_index_ac` | 23.32ms | 82.35ms | 3.5× | 6.5% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 18.69ms | 63.85ms | 3.4× | 6.9% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 19.77ms | 72.00ms | 3.6× | 6.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 21.50ms | 73.19ms | 3.4× | 5.9% | PASS |
| ac_writes | `types_delete_insert_ac` | 18.09ms | 65.12ms | 3.6× | 7.0% | PASS |
| ac_writes | `oltp_read_write_ac` | 24.78ms | 79.51ms | 3.2× | 5.0% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 38.13ms | 43.15ms | 1.1× | 2.0% | PASS |
| mem_reads | `oltp_range_select` | 18.22ms | 16.83ms | 0.9× | 2.5% | PASS |
| mem_reads | `oltp_sum_range` | 16.75ms | 16.20ms | 1.0× | 2.3% | PASS |
| mem_reads | `oltp_order_range` | 3.48ms | 3.53ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 4.53ms | 4.69ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 4.14ms | 7.06ms | 1.7× | 2.4% | PASS |
| mem_reads | `select_random_points` | 24.96ms | 24.94ms | 1.0× | 2.7% | PASS |
| mem_reads | `select_random_ranges` | 7.11ms | 7.29ms | 1.0× | 1.8% | PASS |
| mem_reads | `covering_index_scan` | 7.93ms | 11.39ms | 1.4× | 1.7% | PASS |
| mem_reads | `groupby_scan` | 35.34ms | 36.54ms | 1.0× | 0.8% | PASS |
| mem_reads | `index_join` | 11.47ms | 11.52ms | 1.0× | 3.5% | PASS |
| mem_reads | `index_join_scan` | 3.95ms | 7.58ms | 1.9× | 4.8% | PASS |
| mem_reads | `types_table_scan` | 1.29s | 1.40s | 1.1× | 0.6% | PASS |
| mem_reads | `table_scan` | 1.44s | 1.48s | 1.0× | 0.4% | PASS |
| mem_reads | `oltp_read_only` | 158.86ms | 160.63ms | 1.0× | 1.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 238.49ms | 358.84ms | 1.5× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 18.00ms | 38.86ms | 2.2× | 1.2% | PASS |
| mem_writes | `oltp_update_index` | 68.69ms | 142.41ms | 2.1× | 1.4% | PASS |
| mem_writes | `oltp_update_non_index` | 50.30ms | 87.11ms | 1.7× | 1.8% | PASS |
| mem_writes | `oltp_delete_insert` | 55.95ms | 104.31ms | 1.9× | 1.2% | PASS |
| mem_writes | `oltp_write_only` | 29.97ms | 59.91ms | 2.0× | 2.0% | PASS |
| mem_writes | `types_delete_insert` | 40.33ms | 60.15ms | 1.5× | 1.0% | PASS |
| mem_writes | `oltp_read_write` | 102.33ms | 145.41ms | 1.4× | 3.1% | PASS |
| file_reads | `oltp_point_select` | 105.22ms | 61.28ms | 0.6× | 1.2% | PASS |
| file_reads | `oltp_range_select` | 25.08ms | 18.97ms | 0.8× | 1.4% | PASS |
| file_reads | `oltp_sum_range` | 24.05ms | 18.28ms | 0.8× | 1.3% | PASS |
| file_reads | `oltp_order_range` | 4.33ms | 3.80ms | 0.9× | 1.9% | PASS |
| file_reads | `oltp_distinct_range` | 5.40ms | 4.96ms | 0.9× | 1.1% | PASS |
| file_reads | `oltp_index_scan` | 11.47ms | 9.24ms | 0.8× | 1.7% | PASS |
| file_reads | `select_random_points` | 32.76ms | 27.38ms | 0.8× | 2.7% | PASS |
| file_reads | `select_random_ranges` | 14.66ms | 9.52ms | 0.6× | 1.6% | PASS |
| file_reads | `covering_index_scan` | 15.55ms | 13.61ms | 0.9× | 1.3% | PASS |
| file_reads | `groupby_scan` | 36.29ms | 36.80ms | 1.0× | 0.6% | PASS |
| file_reads | `index_join` | 15.98ms | 12.89ms | 0.8× | 2.8% | PASS |
| file_reads | `index_join_scan` | 4.90ms | 7.57ms | 1.5× | 2.2% | PASS |
| file_reads | `types_table_scan` | 1.29s | 1.39s | 1.1× | 0.9% | PASS |
| file_reads | `table_scan` | 1.44s | 1.47s | 1.0× | 0.7% | PASS |
| file_reads | `oltp_read_only` | 264.25ms | 191.56ms | 0.7× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 265.11ms | 377.13ms | 1.4× | 1.3% | PASS |
| file_writes | `oltp_insert` | 26.46ms | 45.22ms | 1.7× | 1.9% | PASS |
| file_writes | `oltp_update_index` | 140.77ms | 166.97ms | 1.2× | 5.9% | PASS |
| file_writes | `oltp_update_non_index` | 104.92ms | 103.40ms | 1.0× | 13.1% | PASS |
| file_writes | `oltp_delete_insert` | 101.48ms | 123.80ms | 1.2× | 1.8% | PASS |
| file_writes | `oltp_write_only` | 88.84ms | 72.98ms | 0.8× | 11.9% | PASS |
| file_writes | `types_delete_insert` | 75.59ms | 73.21ms | 1.0× | 1.5% | PASS |
| file_writes | `oltp_read_write` | 178.97ms | 166.38ms | 0.9× | 6.4% | PASS |
| ac_reads | `oltp_point_select` | 62.91ms | 64.02ms | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_range_select` | 20.90ms | 19.19ms | 0.9× | 2.4% | PASS |
| ac_reads | `oltp_sum_range` | 20.12ms | 18.63ms | 0.9× | 1.9% | PASS |
| ac_reads | `oltp_order_range` | 3.90ms | 3.82ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_distinct_range` | 4.91ms | 5.00ms | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_index_scan` | 6.91ms | 9.38ms | 1.4× | 1.6% | PASS |
| ac_reads | `select_random_points` | 28.25ms | 28.05ms | 1.0× | 2.9% | PASS |
| ac_reads | `select_random_ranges` | 9.74ms | 9.49ms | 1.0× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 10.63ms | 13.47ms | 1.3× | 1.5% | PASS |
| ac_reads | `groupby_scan` | 35.46ms | 36.90ms | 1.0× | 0.7% | PASS |
| ac_reads | `index_join` | 13.62ms | 13.15ms | 1.0× | 3.8% | PASS |
| ac_reads | `index_join_scan` | 4.44ms | 7.77ms | 1.8× | 3.1% | PASS |
| ac_reads | `types_table_scan` | 1.30s | 1.40s | 1.1× | 0.7% | PASS |
| ac_reads | `table_scan` | 1.43s | 1.47s | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_read_only` | 191.31ms | 190.72ms | 1.0× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 24.96ms | 69.83ms | 2.8× | 6.5% | PASS |
| ac_writes | `oltp_insert_ac` | 28.36ms | 84.28ms | 3.0× | 6.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 29.67ms | 100.20ms | 3.4× | 9.0% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.42ms | 79.09ms | 3.2× | 6.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 27.94ms | 91.59ms | 3.3× | 5.4% | PASS |
| ac_writes | `oltp_write_only_ac` | 27.23ms | 88.50ms | 3.3× | 5.4% | PASS |
| ac_writes | `types_delete_insert_ac` | 26.05ms | 82.05ms | 3.1× | 7.2% | PASS |
| ac_writes | `oltp_read_write_ac` | 34.36ms | 95.70ms | 2.8× | 7.0% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 35.22ms | 41.32ms | 1.2× | 2.0% | PASS |
| mem_reads | `oltp_range_select` | 15.71ms | 16.13ms | 1.0× | 1.3% | PASS |
| mem_reads | `oltp_sum_range` | 14.56ms | 15.34ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_order_range` | 3.26ms | 3.49ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_distinct_range` | 4.32ms | 4.62ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 4.00ms | 6.88ms | 1.7× | 1.2% | PASS |
| mem_reads | `select_random_points` | 22.10ms | 23.09ms | 1.0× | 1.6% | PASS |
| mem_reads | `select_random_ranges` | 6.83ms | 7.14ms | 1.0× | 1.2% | PASS |
| mem_reads | `covering_index_scan` | 7.73ms | 11.20ms | 1.4× | 1.0% | PASS |
| mem_reads | `groupby_scan` | 33.98ms | 35.86ms | 1.1× | 0.7% | PASS |
| mem_reads | `index_join` | 10.47ms | 11.07ms | 1.1× | 2.1% | PASS |
| mem_reads | `index_join_scan` | 3.73ms | 6.67ms | 1.8× | 2.0% | PASS |
| mem_reads | `types_table_scan` | 1.16s | 1.38s | 1.2× | 0.8% | PASS |
| mem_reads | `table_scan` | 1.29s | 1.45s | 1.1× | 0.6% | PASS |
| mem_reads | `oltp_read_only` | 139.45ms | 150.92ms | 1.1× | 2.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 239.44ms | 361.81ms | 1.5× | 1.2% | PASS |
| mem_writes | `oltp_insert` | 18.33ms | 38.70ms | 2.1× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 65.47ms | 135.91ms | 2.1× | 2.3% | PASS |
| mem_writes | `oltp_update_non_index` | 47.70ms | 83.59ms | 1.8× | 1.9% | PASS |
| mem_writes | `oltp_delete_insert` | 52.51ms | 99.86ms | 1.9× | 1.1% | PASS |
| mem_writes | `oltp_write_only` | 28.16ms | 57.58ms | 2.0× | 1.6% | PASS |
| mem_writes | `types_delete_insert` | 37.79ms | 57.65ms | 1.5× | 1.4% | PASS |
| mem_writes | `oltp_read_write` | 99.22ms | 143.23ms | 1.4× | 1.7% | PASS |
| file_reads | `oltp_point_select` | 107.52ms | 60.81ms | 0.6× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 23.60ms | 18.28ms | 0.8× | 2.2% | PASS |
| file_reads | `oltp_sum_range` | 22.56ms | 17.59ms | 0.8× | 1.7% | PASS |
| file_reads | `oltp_order_range` | 4.29ms | 3.74ms | 0.9× | 2.3% | PASS |
| file_reads | `oltp_distinct_range` | 5.36ms | 4.88ms | 0.9× | 2.0% | PASS |
| file_reads | `oltp_index_scan` | 11.53ms | 8.93ms | 0.8× | 1.4% | PASS |
| file_reads | `select_random_points` | 30.64ms | 25.69ms | 0.8× | 2.3% | PASS |
| file_reads | `select_random_ranges` | 14.17ms | 9.14ms | 0.6× | 1.5% | PASS |
| file_reads | `covering_index_scan` | 15.31ms | 13.17ms | 0.9× | 1.4% | PASS |
| file_reads | `groupby_scan` | 34.80ms | 36.08ms | 1.0× | 1.0% | PASS |
| file_reads | `index_join` | 14.66ms | 12.25ms | 0.8× | 2.0% | PASS |
| file_reads | `index_join_scan` | 4.71ms | 7.03ms | 1.5× | 1.5% | PASS |
| file_reads | `types_table_scan` | 1.12s | 1.36s | 1.2× | 1.9% | PASS |
| file_reads | `table_scan` | 1.24s | 1.42s | 1.1× | 0.8% | PASS |
| file_reads | `oltp_read_only` | 252.87ms | 182.11ms | 0.7× | 1.2% | PASS |
| file_writes | `oltp_bulk_insert` | 262.04ms | 377.28ms | 1.4× | 1.3% | PASS |
| file_writes | `oltp_insert` | 25.82ms | 43.88ms | 1.7× | 1.9% | PASS |
| file_writes | `oltp_update_index` | 98.79ms | 152.27ms | 1.5× | 1.8% | PASS |
| file_writes | `oltp_update_non_index` | 88.52ms | 96.70ms | 1.1× | 10.6% | PASS |
| file_writes | `oltp_delete_insert` | 88.20ms | 115.48ms | 1.3× | 1.6% | PASS |
| file_writes | `oltp_write_only` | 58.42ms | 69.95ms | 1.2× | 2.4% | PASS |
| file_writes | `types_delete_insert` | 63.85ms | 67.92ms | 1.1× | 2.2% | PASS |
| file_writes | `oltp_read_write` | 131.21ms | 155.37ms | 1.2× | 1.2% | PASS |
| ac_reads | `oltp_point_select` | 58.59ms | 60.76ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_range_select` | 18.93ms | 18.25ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_sum_range` | 18.12ms | 17.69ms | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_order_range` | 3.86ms | 3.75ms | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_distinct_range` | 4.90ms | 4.89ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 6.75ms | 9.01ms | 1.3× | 2.0% | PASS |
| ac_reads | `select_random_points` | 25.03ms | 25.93ms | 1.0× | 2.2% | PASS |
| ac_reads | `select_random_ranges` | 9.39ms | 9.19ms | 1.0× | 1.7% | PASS |
| ac_reads | `covering_index_scan` | 10.59ms | 13.27ms | 1.3× | 1.2% | PASS |
| ac_reads | `groupby_scan` | 34.16ms | 36.17ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 12.33ms | 12.23ms | 1.0× | 2.2% | PASS |
| ac_reads | `index_join_scan` | 4.23ms | 7.06ms | 1.7× | 1.8% | PASS |
| ac_reads | `types_table_scan` | 1.09s | 1.35s | 1.2× | 2.0% | PASS |
| ac_reads | `table_scan` | 1.23s | 1.42s | 1.2× | 1.2% | PASS |
| ac_reads | `oltp_read_only` | 177.35ms | 181.03ms | 1.0× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 26.46ms | 71.70ms | 2.7× | 9.5% | PASS |
| ac_writes | `oltp_insert_ac` | 28.32ms | 92.53ms | 3.3× | 8.1% | PASS |
| ac_writes | `oltp_update_index_ac` | 30.43ms | 104.43ms | 3.4× | 6.5% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.36ms | 83.67ms | 3.4× | 9.1% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 28.17ms | 90.44ms | 3.2× | 8.0% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.57ms | 86.90ms | 3.3× | 7.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 28.03ms | 84.47ms | 3.0× | 9.6% | PASS |
| ac_writes | `oltp_read_write_ac` | 34.95ms | 96.80ms | 2.8× | 7.2% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 22.20ms | 25.61ms | 1.2× | 1.9% | PASS |
| mem_reads | `oltp_range_select` | 11.83ms | 12.36ms | 1.0× | 2.8% | PASS |
| mem_reads | `oltp_sum_range` | 10.48ms | 12.15ms | 1.2× | 2.1% | PASS |
| mem_reads | `oltp_order_range` | 2.26ms | 2.33ms | 1.0× | 2.4% | PASS |
| mem_reads | `oltp_distinct_range` | 2.91ms | 2.89ms | 1.0× | 2.0% | PASS |
| mem_reads | `oltp_index_scan` | 3.15ms | 4.47ms | 1.4× | 2.6% | PASS |
| mem_reads | `select_random_points` | 19.40ms | 21.14ms | 1.1× | 3.3% | PASS |
| mem_reads | `select_random_ranges` | 4.73ms | 5.41ms | 1.1× | 2.2% | PASS |
| mem_reads | `covering_index_scan` | 4.13ms | 5.95ms | 1.4× | 4.2% | PASS |
| mem_reads | `groupby_scan` | 21.40ms | 23.00ms | 1.1× | 1.8% | PASS |
| mem_reads | `index_join` | 5.07ms | 8.11ms | 1.6× | 4.8% | PASS |
| mem_reads | `index_join_scan` | 3.12ms | 5.13ms | 1.6× | 5.3% | PASS |
| mem_reads | `types_table_scan` | 846.18ms | 883.16ms | 1.0× | 2.3% | PASS |
| mem_reads | `table_scan` | 1.09s | 989.03ms | 0.9× | 2.3% | PASS |
| mem_reads | `oltp_read_only` | 92.31ms | 96.91ms | 1.0× | 3.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 147.58ms | 212.15ms | 1.4× | 1.1% | PASS |
| mem_writes | `oltp_insert` | 11.63ms | 21.82ms | 1.9× | 1.2% | PASS |
| mem_writes | `oltp_update_index` | 47.27ms | 93.03ms | 2.0× | 3.3% | PASS |
| mem_writes | `oltp_update_non_index` | 36.20ms | 59.66ms | 1.6× | 2.8% | PASS |
| mem_writes | `oltp_delete_insert` | 33.06ms | 64.88ms | 2.0× | 2.8% | PASS |
| mem_writes | `oltp_write_only` | 18.96ms | 41.05ms | 2.2× | 2.2% | PASS |
| mem_writes | `types_delete_insert` | 23.20ms | 36.77ms | 1.6× | 2.2% | PASS |
| mem_writes | `oltp_read_write` | 70.06ms | 100.96ms | 1.4× | 2.2% | PASS |
| file_reads | `oltp_point_select` | 73.97ms | 39.50ms | 0.5× | 2.2% | PASS |
| file_reads | `oltp_range_select` | 17.59ms | 13.99ms | 0.8× | 1.8% | PASS |
| file_reads | `oltp_sum_range` | 16.57ms | 13.82ms | 0.8× | 1.9% | PASS |
| file_reads | `oltp_order_range` | 3.00ms | 2.62ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_distinct_range` | 3.56ms | 3.14ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 8.91ms | 6.27ms | 0.7× | 1.8% | PASS |
| file_reads | `select_random_points` | 24.78ms | 22.36ms | 0.9× | 2.6% | PASS |
| file_reads | `select_random_ranges` | 10.25ms | 7.15ms | 0.7× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 9.80ms | 8.04ms | 0.8× | 3.4% | PASS |
| file_reads | `groupby_scan` | 21.86ms | 22.97ms | 1.1× | 1.4% | PASS |
| file_reads | `index_join` | 8.42ms | 9.26ms | 1.1× | 6.2% | PASS |
| file_reads | `index_join_scan` | 3.61ms | 5.57ms | 1.5× | 8.3% | PASS |
| file_reads | `types_table_scan` | 872.91ms | 894.85ms | 1.0× | 2.9% | PASS |
| file_reads | `table_scan` | 1.10s | 1.00s | 0.9× | 1.8% | PASS |
| file_reads | `oltp_read_only` | 176.04ms | 123.76ms | 0.7× | 1.2% | PASS |
| file_writes | `oltp_bulk_insert` | 204.44ms | 268.78ms | 1.3× | 3.6% | PASS |
| file_writes | `oltp_insert` | 19.47ms | 30.29ms | 1.6× | 8.0% | PASS |
| file_writes | `oltp_update_index` | 149.77ms | 147.86ms | 1.0× | 3.7% | PASS |
| file_writes | `oltp_update_non_index` | 121.10ms | 101.62ms | 0.8× | 3.1% | PASS |
| file_writes | `oltp_delete_insert` | 120.61ms | 119.69ms | 1.0× | 2.9% | PASS |
| file_writes | `oltp_write_only` | 89.57ms | 81.33ms | 0.9× | 8.3% | PASS |
| file_writes | `types_delete_insert` | 77.05ms | 68.30ms | 0.9× | 7.9% | PASS |
| file_writes | `oltp_read_write` | 131.38ms | 136.22ms | 1.0× | 3.8% | PASS |
| ac_reads | `oltp_point_select` | 40.70ms | 39.84ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_range_select` | 14.71ms | 14.42ms | 1.0× | 2.5% | PASS |
| ac_reads | `oltp_sum_range` | 13.63ms | 14.39ms | 1.1× | 2.0% | PASS |
| ac_reads | `oltp_order_range` | 2.71ms | 2.64ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_distinct_range` | 3.19ms | 3.08ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_index_scan` | 5.45ms | 6.13ms | 1.1× | 1.7% | PASS |
| ac_reads | `select_random_points` | 21.90ms | 23.27ms | 1.1× | 2.9% | PASS |
| ac_reads | `select_random_ranges` | 6.77ms | 7.04ms | 1.0× | 1.7% | PASS |
| ac_reads | `covering_index_scan` | 6.61ms | 8.03ms | 1.2× | 3.0% | PASS |
| ac_reads | `groupby_scan` | 22.03ms | 23.41ms | 1.1× | 1.7% | PASS |
| ac_reads | `index_join` | 6.48ms | 8.71ms | 1.3× | 5.3% | PASS |
| ac_reads | `index_join_scan` | 3.43ms | 5.88ms | 1.7× | 7.5% | PASS |
| ac_reads | `types_table_scan` | 761.78ms | 849.06ms | 1.1× | 3.7% | PASS |
| ac_reads | `table_scan` | 839.19ms | 913.05ms | 1.1× | 2.9% | PASS |
| ac_reads | `oltp_read_only` | 102.19ms | 107.60ms | 1.1× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.66ms | 51.29ms | 2.4× | 3.3% | PASS |
| ac_writes | `oltp_insert_ac` | 25.11ms | 60.25ms | 2.4× | 4.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.65ms | 72.95ms | 2.6× | 9.5% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.06ms | 61.61ms | 2.8× | 4.7% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.76ms | 67.87ms | 2.6× | 5.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.71ms | 66.15ms | 2.6× | 6.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 26.86ms | 62.31ms | 2.3× | 11.0% | PASS |
| ac_writes | `oltp_read_write_ac` | 28.29ms | 71.10ms | 2.5× | 4.5% | PASS |

</details>

</details>

## Version-control latency

Wall time: 5m 35s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 21.33ms | 38.00ms | 56.1% | 0.4% | PASS |
| `status_dirty_many_tables` | 23.47ms | 42.00ms | 55.9% | 0.3% | PASS |
| `diff_regular_working_one_table` | 18.70ms | 33.00ms | 56.7% | 0.4% | PASS |
| `diff_regular_working_many_tables` | 25.58ms | 50.00ms | 51.2% | 0.4% | PASS |
| `diff_stat_working_many_tables` | 25.69ms | 48.00ms | 53.5% | 0.9% | PASS |
| `diff_schema_working_many_tables` | 26.27ms | 48.00ms | 54.7% | 1.2% | PASS |
| `branch_list_many_branches` | 14.77ms | 25.00ms | 59.1% | 2.8% | PASS |
| `branch_create_delete` | 19.31ms | 27.00ms | 71.5% | 2.7% | PASS |
| `at_literal_deep_history` | 18.82ms | 28.00ms | 67.2% | 1.6% | PASS |
| `diff_literal_deep_history` | 18.73ms | 28.00ms | 66.9% | 1.3% | PASS |
| `history_literal_deep_history` | 19.57ms | 30.00ms | 65.2% | 1.3% | PASS |
| `checkout_branch_clean` | 23.26ms | 43.00ms | 54.1% | 2.2% | PASS |
| `merge_data_no_conflicts` | 19.89ms | 33.00ms | 60.3% | 2.9% | PASS |
| `merge_data_secondary_index` | 600.66ms | 944.00ms | 63.6% | 1.3% | PASS |
| `merge_schema_no_conflicts` | 14.60ms | 24.00ms | 60.8% | 1.4% | PASS |
| `merge_data_conflicts` | 21.14ms | 33.00ms | 64.1% | 0.6% | PASS |
| `merge_data_conflicts_with_resolve` | 21.75ms | 34.00ms | 64.0% | 0.7% | PASS |

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

candidate: 80a9ec5.

Fixture file bytes: base 107,968,266; delta 115,878,997.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 524.876 | 510.394–534.952 | 30.193 | 190.52 | 107968266 |
| push_initial | candidate | 799.866 | 780.949–817.589 | 332.857 | 125.02 | 107956772 |
| clone_initial | candidate | 956.220 | 944.833–1018.120 | 452.363 | 104.58 | 108475035 |
| push_noop | candidate | 0.594 | 0.574–0.647 | 0.300 | — | 0 |
| fetch_noop | candidate | 0.139 | 0.137–0.154 | 0.140 | — | 0 |
| push_incremental | candidate | 163.702 | 160.271–176.160 | 145.025 | 6.11 | 7909775 |
| fetch_incremental | candidate | 80.343 | 72.556–81.110 | 60.903 | 12.45 | 7910084 |
| pull_fast_forward | candidate | 72.747 | 69.309–75.472 | 52.773 | 13.75 | 7910844 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 90.60 | 175.487 | 108.763 | 18329 | 2066 |
| clone_initial | candidate | 90.78 | 294.955 | 109.784 | 47211 | 2337 |
| push_noop | candidate | 42.36 | 0.073 | 0.000 | 26 | 2 |
| fetch_noop | candidate | 42.36 | 0.135 | 0.000 | 40 | 0 |
| push_incremental | candidate | 42.36 | 96.704 | 7.910 | 22371 | 125 |
| fetch_incremental | candidate | 42.36 | 34.472 | 7.910 | 10808 | 124 |
| pull_fast_forward | candidate | 42.36 | 26.497 | 7.911 | 7031 | 126 |

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

candidate: 80a9ec5.

Fixture file bytes: base 1,087,718,031; delta 1,103,525,159.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 5446.834 | 5282.132–5526.021 | 298.795 | 183.59 | 1087718031 |
| push_initial | candidate | 7512.607 | 7393.138–7538.154 | 3616.947 | 133.11 | 1079566275 |
| clone_initial | candidate | 9468.448 | 9438.436–9490.529 | 5492.434 | 105.61 | 1102407326 |
| push_noop | candidate | 0.627 | 0.605–0.712 | 0.310 | — | 0 |
| fetch_noop | candidate | 0.165 | 0.162–0.192 | 0.166 | — | 0 |
| push_incremental | candidate | 1018.328 | 1014.670–1033.031 | 925.334 | 0.98 | 15806003 |
| fetch_incremental | candidate | 325.625 | 294.281–428.990 | 195.651 | 3.07 | 15806513 |
| pull_fast_forward | candidate | 327.208 | 293.667–366.972 | 193.892 | 3.06 | 15807273 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 135.11 | 2091.789 | 1087.622 | 263476 | 20541 |
| clone_initial | candidate | 125.23 | 3521.769 | 1132.819 | 613275 | 32243 |
| push_noop | candidate | 44.71 | 0.078 | 0.000 | 27 | 2 |
| fetch_noop | candidate | 44.71 | 0.137 | 0.000 | 40 | 0 |
| push_incremental | candidate | 48.35 | 619.075 | 15.928 | 150361 | 313 |
| fetch_incremental | candidate | 50.43 | 61.672 | 15.928 | 13758 | 313 |
| pull_fast_forward | candidate | 51.34 | 61.605 | 15.929 | 13754 | 315 |

</details>


</details>

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh`, `test/vc_perf_ceiling.sh`, and `test/remote_perf.py`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
