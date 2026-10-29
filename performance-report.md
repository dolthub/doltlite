# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-10-29 11:34 UTC
>
> Commit: [`00d4dd3d954e5e1d7339872a3cbb9bd986918934`](https://github.com/dolthub/doltlite/commit/00d4dd3d954e5e1d7339872a3cbb9bd986918934)
>
> Runner: ubuntu24 20261026.354.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/40850402540)

The SQL workloads compare optimized DoltLite against stock SQLite on the same GitHub-hosted runner, alternating execution order on each repetition. Version-control sections measure DoltLite latency. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 8.95s | 10.74s | 1.2× | 1.2% | **PASS** |
| Writes | 1.79s | 2.99s | 1.7× | 1.2% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.65s | 10.79s | 1.1× | 1.5% | **PASS** |
| Writes | 3.89s | 4.23s | 1.1× | 2.1% | **PASS** |
| Autocommit writes | 778.47ms | 2.39s | 3.1× | 5.9% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.31s | 2.65s | 1.1× | 1.2% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 1.80s | 2.12s | 1.2× | 2.2% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.73s | 3.52s | 1.3× | 2.0% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.10s | 2.45s | 1.2× | 1.0% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 360.55ms | 633.55ms | 1.8× | 0.9% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 369.09ms | 592.96ms | 1.6× | 2.1% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 607.45ms | 1.03s | 1.7× | 2.3% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 457.85ms | 732.55ms | 1.6× | 1.1% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.40s | 2.68s | 1.1× | 1.1% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 1.98s | 2.00s | 1.0× | 2.2% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.98s | 3.61s | 1.2× | 1.8% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.29s | 2.50s | 1.1× | 1.2% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 926.35ms | 980.28ms | 1.1× | 1.1% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.09s | 1.04s | 0.9× | 3.7% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 837.79ms | 1.12s | 1.3× | 2.1% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 1.03s | 1.09s | 1.1× | 2.6% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.37s | 2.71s | 1.1× | 1.3% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 1.77s | 1.91s | 1.1× | 1.7% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.91s | 3.64s | 1.3× | 1.5% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.17s | 2.50s | 1.2× | 0.8% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 235.26ms | 690.32ms | 2.9× | 3.6% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 175.97ms | 490.35ms | 2.8× | 5.3% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 217.14ms | 695.82ms | 3.2× | 11.6% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 150.10ms | 510.98ms | 3.4× | 9.3% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 20.97ms | 25.33ms | 1.2× | 1.3% | PASS |
| mem_reads | `oltp_range_select` | 9.57ms | 11.86ms | 1.2× | 0.9% | PASS |
| mem_reads | `oltp_sum_range` | 8.66ms | 10.84ms | 1.3× | 1.5% | PASS |
| mem_reads | `oltp_order_range` | 2.44ms | 2.89ms | 1.2× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 3.32ms | 3.88ms | 1.2× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 3.47ms | 4.54ms | 1.3× | 1.4% | PASS |
| mem_reads | `select_random_points` | 9.89ms | 11.46ms | 1.2× | 1.1% | PASS |
| mem_reads | `select_random_ranges` | 3.95ms | 4.37ms | 1.1× | 1.4% | PASS |
| mem_reads | `covering_index_scan` | 6.43ms | 9.39ms | 1.5× | 1.2% | PASS |
| mem_reads | `groupby_scan` | 29.30ms | 32.14ms | 1.1× | 0.8% | PASS |
| mem_reads | `index_join` | 5.21ms | 7.95ms | 1.5× | 1.2% | PASS |
| mem_reads | `index_join_scan` | 2.80ms | 5.20ms | 1.9× | 1.4% | PASS |
| mem_reads | `types_table_scan` | 986.33ms | 1.15s | 1.2× | 0.3% | PASS |
| mem_reads | `table_scan` | 1.13s | 1.26s | 1.1× | 0.7% | PASS |
| mem_reads | `oltp_read_only` | 92.45ms | 112.57ms | 1.2× | 0.5% | PASS |
| mem_writes | `oltp_bulk_insert` | 140.40ms | 241.00ms | 1.7× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 12.72ms | 22.02ms | 1.7× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 44.27ms | 87.05ms | 2.0× | 0.9% | PASS |
| mem_writes | `oltp_update_non_index` | 28.95ms | 52.77ms | 1.8× | 0.9% | PASS |
| mem_writes | `oltp_delete_insert` | 38.92ms | 65.61ms | 1.7× | 0.9% | PASS |
| mem_writes | `oltp_write_only` | 18.65ms | 40.82ms | 2.2× | 1.0% | PASS |
| mem_writes | `types_delete_insert` | 20.59ms | 30.35ms | 1.5× | 1.2% | PASS |
| mem_writes | `oltp_read_write` | 56.05ms | 93.93ms | 1.7× | 0.7% | PASS |
| file_reads | `oltp_point_select` | 47.99ms | 31.55ms | 0.7× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 12.90ms | 12.91ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_sum_range` | 12.19ms | 11.77ms | 1.0× | 1.1% | PASS |
| file_reads | `oltp_order_range` | 2.86ms | 3.06ms | 1.1× | 1.7% | PASS |
| file_reads | `oltp_distinct_range` | 3.75ms | 4.04ms | 1.1× | 1.5% | PASS |
| file_reads | `oltp_index_scan` | 6.50ms | 5.51ms | 0.8× | 1.7% | PASS |
| file_reads | `select_random_points` | 13.21ms | 12.39ms | 0.9× | 0.9% | PASS |
| file_reads | `select_random_ranges` | 6.94ms | 5.14ms | 0.7× | 1.3% | PASS |
| file_reads | `covering_index_scan` | 9.51ms | 10.22ms | 1.1× | 1.2% | PASS |
| file_reads | `groupby_scan` | 29.47ms | 32.03ms | 1.1× | 0.7% | PASS |
| file_reads | `index_join` | 6.82ms | 8.33ms | 1.2× | 1.1% | PASS |
| file_reads | `index_join_scan` | 3.20ms | 5.41ms | 1.7× | 1.7% | PASS |
| file_reads | `types_table_scan` | 993.38ms | 1.16s | 1.2× | 0.4% | PASS |
| file_reads | `table_scan` | 1.13s | 1.26s | 1.1× | 0.3% | PASS |
| file_reads | `oltp_read_only` | 128.84ms | 119.79ms | 0.9× | 0.3% | PASS |
| file_writes | `oltp_bulk_insert` | 192.48ms | 293.06ms | 1.5× | 1.1% | PASS |
| file_writes | `oltp_insert` | 27.28ms | 36.68ms | 1.3× | 2.1% | PASS |
| file_writes | `oltp_update_index` | 147.29ms | 144.53ms | 1.0× | 0.7% | PASS |
| file_writes | `oltp_update_non_index` | 120.57ms | 100.29ms | 0.8× | 0.9% | PASS |
| file_writes | `oltp_delete_insert` | 128.17ms | 121.52ms | 0.9× | 1.1% | PASS |
| file_writes | `oltp_write_only` | 95.03ms | 83.58ms | 0.9× | 1.4% | PASS |
| file_writes | `types_delete_insert` | 82.19ms | 64.31ms | 0.8× | 2.1% | PASS |
| file_writes | `oltp_read_write` | 133.34ms | 136.32ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_point_select` | 29.79ms | 31.39ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_range_select` | 10.93ms | 12.87ms | 1.2× | 1.3% | PASS |
| ac_reads | `oltp_sum_range` | 10.28ms | 11.65ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_order_range` | 2.68ms | 3.06ms | 1.1× | 1.6% | PASS |
| ac_reads | `oltp_distinct_range` | 3.57ms | 4.10ms | 1.1× | 1.7% | PASS |
| ac_reads | `oltp_index_scan` | 4.77ms | 5.52ms | 1.2× | 1.4% | PASS |
| ac_reads | `select_random_points` | 11.62ms | 12.45ms | 1.1× | 0.8% | PASS |
| ac_reads | `select_random_ranges` | 5.15ms | 5.09ms | 1.0× | 1.7% | PASS |
| ac_reads | `covering_index_scan` | 7.65ms | 10.21ms | 1.3× | 1.0% | PASS |
| ac_reads | `groupby_scan` | 29.77ms | 32.52ms | 1.1× | 0.5% | PASS |
| ac_reads | `index_join` | 6.21ms | 8.66ms | 1.4× | 1.4% | PASS |
| ac_reads | `index_join_scan` | 3.11ms | 5.51ms | 1.8× | 1.6% | PASS |
| ac_reads | `types_table_scan` | 1.00s | 1.17s | 1.2× | 0.6% | PASS |
| ac_reads | `table_scan` | 1.14s | 1.27s | 1.1× | 0.7% | PASS |
| ac_reads | `oltp_read_only` | 107.33ms | 123.29ms | 1.1× | 0.6% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 27.80ms | 74.09ms | 2.7× | 3.3% | PASS |
| ac_writes | `oltp_insert_ac` | 29.39ms | 84.49ms | 2.9× | 2.7% | PASS |
| ac_writes | `oltp_update_index_ac` | 30.66ms | 94.69ms | 3.1× | 3.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 27.90ms | 79.35ms | 2.8× | 3.7% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 29.18ms | 89.59ms | 3.1× | 3.5% | PASS |
| ac_writes | `oltp_write_only_ac` | 29.57ms | 88.98ms | 3.0× | 3.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 27.71ms | 83.87ms | 3.0× | 3.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 33.04ms | 95.25ms | 2.9× | 3.6% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 22.64ms | 23.48ms | 1.0× | 3.3% | PASS |
| mem_reads | `oltp_range_select` | 10.77ms | 9.77ms | 0.9× | 2.7% | PASS |
| mem_reads | `oltp_sum_range` | 10.76ms | 9.90ms | 0.9× | 2.3% | PASS |
| mem_reads | `oltp_order_range` | 2.30ms | 2.14ms | 0.9× | 1.7% | PASS |
| mem_reads | `oltp_distinct_range` | 2.80ms | 2.63ms | 0.9× | 1.8% | PASS |
| mem_reads | `oltp_index_scan` | 2.60ms | 4.70ms | 1.8× | 2.1% | PASS |
| mem_reads | `select_random_points` | 18.99ms | 15.90ms | 0.8× | 3.0% | PASS |
| mem_reads | `select_random_ranges` | 4.75ms | 4.28ms | 0.9× | 1.3% | PASS |
| mem_reads | `covering_index_scan` | 3.88ms | 6.34ms | 1.6× | 1.8% | PASS |
| mem_reads | `groupby_scan` | 21.59ms | 21.21ms | 1.0× | 0.6% | PASS |
| mem_reads | `index_join` | 8.41ms | 8.31ms | 1.0× | 3.3% | PASS |
| mem_reads | `index_join_scan` | 2.90ms | 5.75ms | 2.0× | 3.9% | PASS |
| mem_reads | `types_table_scan` | 765.98ms | 941.87ms | 1.2× | 2.2% | PASS |
| mem_reads | `table_scan` | 835.36ms | 979.33ms | 1.2× | 1.1% | PASS |
| mem_reads | `oltp_read_only` | 83.67ms | 80.02ms | 1.0× | 2.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 141.25ms | 207.05ms | 1.5× | 1.1% | PASS |
| mem_writes | `oltp_insert` | 10.44ms | 22.30ms | 2.1× | 1.5% | PASS |
| mem_writes | `oltp_update_index` | 46.31ms | 95.76ms | 2.1× | 2.2% | PASS |
| mem_writes | `oltp_update_non_index` | 36.54ms | 53.59ms | 1.5× | 2.2% | PASS |
| mem_writes | `oltp_delete_insert` | 34.67ms | 65.14ms | 1.9× | 1.7% | PASS |
| mem_writes | `oltp_write_only` | 18.23ms | 34.84ms | 1.9× | 2.1% | PASS |
| mem_writes | `types_delete_insert` | 26.69ms | 37.25ms | 1.4× | 2.2% | PASS |
| mem_writes | `oltp_read_write` | 54.97ms | 77.03ms | 1.4× | 3.1% | PASS |
| file_reads | `oltp_point_select` | 72.44ms | 36.74ms | 0.5× | 2.4% | PASS |
| file_reads | `oltp_range_select` | 16.15ms | 10.84ms | 0.7× | 2.8% | PASS |
| file_reads | `oltp_sum_range` | 15.70ms | 10.60ms | 0.7× | 2.2% | PASS |
| file_reads | `oltp_order_range` | 2.87ms | 2.25ms | 0.8× | 1.4% | PASS |
| file_reads | `oltp_distinct_range` | 3.33ms | 2.71ms | 0.8× | 1.1% | PASS |
| file_reads | `oltp_index_scan` | 7.74ms | 5.72ms | 0.7× | 2.2% | PASS |
| file_reads | `select_random_points` | 21.41ms | 15.55ms | 0.7× | 3.3% | PASS |
| file_reads | `select_random_ranges` | 9.89ms | 5.62ms | 0.6× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 9.56ms | 7.61ms | 0.8× | 1.7% | PASS |
| file_reads | `groupby_scan` | 21.30ms | 20.81ms | 1.0× | 1.7% | PASS |
| file_reads | `index_join` | 11.04ms | 7.83ms | 0.7× | 2.2% | PASS |
| file_reads | `index_join_scan` | 3.25ms | 4.96ms | 1.5× | 2.1% | PASS |
| file_reads | `types_table_scan` | 722.65ms | 851.02ms | 1.2× | 4.2% | PASS |
| file_reads | `table_scan` | 904.05ms | 922.63ms | 1.0× | 4.2% | PASS |
| file_reads | `oltp_read_only` | 154.20ms | 98.58ms | 0.6× | 2.4% | PASS |
| file_writes | `oltp_bulk_insert` | 212.00ms | 270.28ms | 1.3× | 2.0% | PASS |
| file_writes | `oltp_insert` | 20.07ms | 43.55ms | 2.2× | 9.8% | PASS |
| file_writes | `oltp_update_index` | 157.36ms | 170.96ms | 1.1× | 3.2% | PASS |
| file_writes | `oltp_update_non_index` | 131.92ms | 108.55ms | 0.8× | 3.3% | PASS |
| file_writes | `oltp_delete_insert` | 172.35ms | 139.63ms | 0.8× | 1.2% | PASS |
| file_writes | `oltp_write_only` | 116.87ms | 93.10ms | 0.8× | 4.1% | PASS |
| file_writes | `types_delete_insert` | 131.88ms | 77.64ms | 0.6× | 6.6% | PASS |
| file_writes | `oltp_read_write` | 151.85ms | 134.14ms | 0.9× | 4.8% | PASS |
| ac_reads | `oltp_point_select` | 39.38ms | 36.60ms | 0.9× | 2.5% | PASS |
| ac_reads | `oltp_range_select` | 12.53ms | 10.91ms | 0.9× | 2.5% | PASS |
| ac_reads | `oltp_sum_range` | 12.68ms | 10.93ms | 0.9× | 1.5% | PASS |
| ac_reads | `oltp_order_range` | 2.73ms | 2.36ms | 0.9× | 1.6% | PASS |
| ac_reads | `oltp_distinct_range` | 3.19ms | 2.84ms | 0.9× | 1.3% | PASS |
| ac_reads | `oltp_index_scan` | 4.73ms | 5.94ms | 1.3× | 1.8% | PASS |
| ac_reads | `select_random_points` | 18.71ms | 15.67ms | 0.8× | 3.8% | PASS |
| ac_reads | `select_random_ranges` | 6.37ms | 5.41ms | 0.8× | 1.7% | PASS |
| ac_reads | `covering_index_scan` | 6.07ms | 7.37ms | 1.2× | 1.1% | PASS |
| ac_reads | `groupby_scan` | 20.44ms | 19.86ms | 1.0× | 0.9% | PASS |
| ac_reads | `index_join` | 9.32ms | 7.62ms | 0.8× | 1.5% | PASS |
| ac_reads | `index_join_scan` | 3.06ms | 5.12ms | 1.7× | 2.1% | PASS |
| ac_reads | `types_table_scan` | 751.78ms | 808.47ms | 1.1× | 5.7% | PASS |
| ac_reads | `table_scan` | 782.86ms | 874.53ms | 1.1× | 1.9% | PASS |
| ac_reads | `oltp_read_only` | 96.65ms | 92.75ms | 1.0× | 1.3% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.05ms | 52.18ms | 2.5× | 5.8% | PASS |
| ac_writes | `oltp_insert_ac` | 22.18ms | 58.30ms | 2.6× | 5.7% | PASS |
| ac_writes | `oltp_update_index_ac` | 22.94ms | 70.85ms | 3.1× | 5.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 20.22ms | 57.59ms | 2.8× | 4.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 22.18ms | 63.86ms | 2.9× | 4.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 21.68ms | 62.42ms | 2.9× | 5.4% | PASS |
| ac_writes | `types_delete_insert_ac` | 20.79ms | 57.34ms | 2.8× | 6.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 24.93ms | 67.82ms | 2.7× | 3.9% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 34.12ms | 41.15ms | 1.2× | 2.0% | PASS |
| mem_reads | `oltp_range_select` | 16.21ms | 17.10ms | 1.1× | 2.2% | PASS |
| mem_reads | `oltp_sum_range` | 14.83ms | 16.77ms | 1.1× | 2.0% | PASS |
| mem_reads | `oltp_order_range` | 3.27ms | 3.56ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_distinct_range` | 4.35ms | 4.69ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 3.96ms | 6.84ms | 1.7× | 1.2% | PASS |
| mem_reads | `select_random_points` | 21.92ms | 23.12ms | 1.1× | 2.6% | PASS |
| mem_reads | `select_random_ranges` | 6.54ms | 7.09ms | 1.1× | 1.2% | PASS |
| mem_reads | `covering_index_scan` | 7.83ms | 11.33ms | 1.4× | 1.5% | PASS |
| mem_reads | `groupby_scan` | 34.81ms | 38.32ms | 1.1× | 1.0% | PASS |
| mem_reads | `index_join` | 10.57ms | 11.35ms | 1.1× | 3.5% | PASS |
| mem_reads | `index_join_scan` | 3.69ms | 6.67ms | 1.8× | 2.5% | PASS |
| mem_reads | `types_table_scan` | 1.11s | 1.50s | 1.3× | 2.3% | PASS |
| mem_reads | `table_scan` | 1.32s | 1.68s | 1.3× | 0.8% | PASS |
| mem_reads | `oltp_read_only` | 138.82ms | 154.79ms | 1.1× | 3.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 233.53ms | 370.92ms | 1.6× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 18.09ms | 38.82ms | 2.1× | 1.2% | PASS |
| mem_writes | `oltp_update_index` | 66.13ms | 144.69ms | 2.2× | 2.9% | PASS |
| mem_writes | `oltp_update_non_index` | 49.36ms | 84.15ms | 1.7× | 2.3% | PASS |
| mem_writes | `oltp_delete_insert` | 55.18ms | 109.72ms | 2.0× | 3.6% | PASS |
| mem_writes | `oltp_write_only` | 29.27ms | 58.65ms | 2.0× | 2.0% | PASS |
| mem_writes | `types_delete_insert` | 41.71ms | 63.93ms | 1.5× | 2.3% | PASS |
| mem_writes | `oltp_read_write` | 114.19ms | 156.17ms | 1.4× | 2.3% | PASS |
| file_reads | `oltp_point_select` | 105.97ms | 61.18ms | 0.6× | 1.1% | PASS |
| file_reads | `oltp_range_select` | 24.68ms | 19.39ms | 0.8× | 2.5% | PASS |
| file_reads | `oltp_sum_range` | 22.61ms | 18.98ms | 0.8× | 2.1% | PASS |
| file_reads | `oltp_order_range` | 4.33ms | 3.83ms | 0.9× | 2.4% | PASS |
| file_reads | `oltp_distinct_range` | 5.41ms | 4.97ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_index_scan` | 10.91ms | 8.86ms | 0.8× | 1.8% | PASS |
| file_reads | `select_random_points` | 31.61ms | 26.27ms | 0.8× | 3.3% | PASS |
| file_reads | `select_random_ranges` | 13.62ms | 9.19ms | 0.7× | 1.4% | PASS |
| file_reads | `covering_index_scan` | 14.92ms | 13.22ms | 0.9× | 1.7% | PASS |
| file_reads | `groupby_scan` | 35.85ms | 38.34ms | 1.1× | 1.1% | PASS |
| file_reads | `index_join` | 16.08ms | 13.05ms | 0.8× | 4.4% | PASS |
| file_reads | `index_join_scan` | 5.13ms | 7.91ms | 1.5× | 4.8% | PASS |
| file_reads | `types_table_scan` | 1.19s | 1.56s | 1.3× | 2.8% | PASS |
| file_reads | `table_scan` | 1.25s | 1.64s | 1.3× | 1.5% | PASS |
| file_reads | `oltp_read_only` | 240.22ms | 181.35ms | 0.8× | 1.7% | PASS |
| file_writes | `oltp_bulk_insert` | 256.09ms | 387.88ms | 1.5× | 0.9% | PASS |
| file_writes | `oltp_insert` | 25.61ms | 44.26ms | 1.7× | 2.1% | PASS |
| file_writes | `oltp_update_index` | 99.82ms | 160.65ms | 1.6× | 1.9% | PASS |
| file_writes | `oltp_update_non_index` | 92.11ms | 96.31ms | 1.0× | 9.1% | PASS |
| file_writes | `oltp_delete_insert` | 92.93ms | 126.69ms | 1.4× | 2.2% | PASS |
| file_writes | `oltp_write_only` | 60.42ms | 71.70ms | 1.2× | 2.8% | PASS |
| file_writes | `types_delete_insert` | 67.23ms | 70.32ms | 1.0× | 1.8% | PASS |
| file_writes | `oltp_read_write` | 143.58ms | 163.99ms | 1.1× | 2.2% | PASS |
| ac_reads | `oltp_point_select` | 58.01ms | 58.88ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_range_select` | 20.13ms | 19.74ms | 1.0× | 2.1% | PASS |
| ac_reads | `oltp_sum_range` | 18.67ms | 19.37ms | 1.0× | 2.7% | PASS |
| ac_reads | `oltp_order_range` | 3.87ms | 3.84ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_distinct_range` | 4.86ms | 4.96ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_index_scan` | 6.63ms | 9.05ms | 1.4× | 1.5% | PASS |
| ac_reads | `select_random_points` | 28.36ms | 27.81ms | 1.0× | 4.2% | PASS |
| ac_reads | `select_random_ranges` | 9.31ms | 9.22ms | 1.0× | 1.7% | PASS |
| ac_reads | `covering_index_scan` | 10.42ms | 13.30ms | 1.3× | 1.3% | PASS |
| ac_reads | `groupby_scan` | 35.11ms | 38.47ms | 1.1× | 1.1% | PASS |
| ac_reads | `index_join` | 13.17ms | 13.21ms | 1.0× | 3.4% | PASS |
| ac_reads | `index_join_scan` | 4.40ms | 7.57ms | 1.7× | 3.2% | PASS |
| ac_reads | `types_table_scan` | 1.17s | 1.54s | 1.3× | 1.2% | PASS |
| ac_reads | `table_scan` | 1.34s | 1.68s | 1.3× | 0.8% | PASS |
| ac_reads | `oltp_read_only` | 191.59ms | 192.86ms | 1.0× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 24.87ms | 70.25ms | 2.8× | 11.3% | PASS |
| ac_writes | `oltp_insert_ac` | 26.22ms | 94.01ms | 3.6× | 11.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 29.01ms | 99.32ms | 3.4× | 9.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.84ms | 79.27ms | 3.3× | 12.5% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.96ms | 88.79ms | 3.3× | 6.9% | PASS |
| ac_writes | `oltp_write_only_ac` | 28.21ms | 89.08ms | 3.2× | 11.8% | PASS |
| ac_writes | `types_delete_insert_ac` | 24.47ms | 78.04ms | 3.2× | 13.1% | PASS |
| ac_writes | `oltp_read_write_ac` | 33.57ms | 97.06ms | 2.9× | 11.7% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 25.48ms | 30.13ms | 1.2× | 1.2% | PASS |
| mem_reads | `oltp_range_select` | 15.09ms | 16.59ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_sum_range` | 13.58ms | 15.98ms | 1.2× | 0.8% | PASS |
| mem_reads | `oltp_order_range` | 2.91ms | 3.08ms | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_distinct_range` | 3.73ms | 3.89ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 3.53ms | 4.79ms | 1.4× | 1.3% | PASS |
| mem_reads | `select_random_points` | 21.08ms | 24.36ms | 1.2× | 1.1% | PASS |
| mem_reads | `select_random_ranges` | 5.74ms | 6.77ms | 1.2× | 1.0% | PASS |
| mem_reads | `covering_index_scan` | 5.83ms | 7.68ms | 1.3× | 1.1% | PASS |
| mem_reads | `groupby_scan` | 29.76ms | 32.61ms | 1.1× | 0.8% | PASS |
| mem_reads | `index_join` | 6.14ms | 8.65ms | 1.4× | 0.9% | PASS |
| mem_reads | `index_join_scan` | 3.14ms | 5.19ms | 1.7× | 1.0% | PASS |
| mem_reads | `types_table_scan` | 860.24ms | 1.04s | 1.2× | 0.5% | PASS |
| mem_reads | `table_scan` | 989.27ms | 1.13s | 1.1× | 0.6% | PASS |
| mem_reads | `oltp_read_only` | 113.63ms | 128.66ms | 1.1× | 0.9% | PASS |
| mem_writes | `oltp_bulk_insert` | 188.66ms | 271.58ms | 1.4× | 1.1% | PASS |
| mem_writes | `oltp_insert` | 14.70ms | 25.96ms | 1.8× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 52.42ms | 97.02ms | 1.9× | 1.0% | PASS |
| mem_writes | `oltp_update_non_index` | 40.87ms | 66.21ms | 1.6× | 1.2% | PASS |
| mem_writes | `oltp_delete_insert` | 39.17ms | 72.88ms | 1.9× | 0.9% | PASS |
| mem_writes | `oltp_write_only` | 21.76ms | 44.69ms | 2.1× | 0.9% | PASS |
| mem_writes | `types_delete_insert` | 25.54ms | 40.32ms | 1.6× | 1.3% | PASS |
| mem_writes | `oltp_read_write` | 74.74ms | 113.89ms | 1.5× | 1.4% | PASS |
| file_reads | `oltp_point_select` | 86.34ms | 46.58ms | 0.5× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 21.69ms | 18.49ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_sum_range` | 19.85ms | 17.84ms | 0.9× | 1.2% | PASS |
| file_reads | `oltp_order_range` | 3.58ms | 3.35ms | 0.9× | 1.9% | PASS |
| file_reads | `oltp_distinct_range` | 4.40ms | 4.15ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_index_scan` | 9.85ms | 6.83ms | 0.7× | 1.6% | PASS |
| file_reads | `select_random_points` | 27.54ms | 26.34ms | 1.0× | 1.5% | PASS |
| file_reads | `select_random_ranges` | 12.03ms | 8.63ms | 0.7× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 12.40ms | 9.80ms | 0.8× | 1.0% | PASS |
| file_reads | `groupby_scan` | 30.51ms | 32.92ms | 1.1× | 0.8% | PASS |
| file_reads | `index_join` | 9.61ms | 10.17ms | 1.1× | 1.3% | PASS |
| file_reads | `index_join_scan` | 3.87ms | 5.54ms | 1.4× | 1.3% | PASS |
| file_reads | `types_table_scan` | 859.50ms | 1.03s | 1.2× | 0.5% | PASS |
| file_reads | `table_scan` | 988.73ms | 1.13s | 1.1× | 0.3% | PASS |
| file_reads | `oltp_read_only` | 202.24ms | 152.81ms | 0.8× | 0.7% | PASS |
| file_writes | `oltp_bulk_insert` | 242.29ms | 327.19ms | 1.4× | 3.3% | PASS |
| file_writes | `oltp_insert` | 31.64ms | 39.62ms | 1.3× | 2.7% | PASS |
| file_writes | `oltp_update_index` | 159.14ms | 161.16ms | 1.0× | 1.4% | PASS |
| file_writes | `oltp_update_non_index` | 134.84ms | 112.51ms | 0.8× | 2.4% | PASS |
| file_writes | `oltp_delete_insert` | 132.66ms | 128.56ms | 1.0× | 1.6% | PASS |
| file_writes | `oltp_write_only` | 94.90ms | 87.94ms | 0.9× | 2.7% | PASS |
| file_writes | `types_delete_insert` | 86.46ms | 73.98ms | 0.9× | 9.2% | PASS |
| file_writes | `oltp_read_write` | 150.85ms | 157.76ms | 1.0× | 2.4% | PASS |
| ac_reads | `oltp_point_select` | 46.31ms | 46.86ms | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_range_select` | 17.92ms | 18.53ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_sum_range` | 16.26ms | 17.91ms | 1.1× | 0.6% | PASS |
| ac_reads | `oltp_order_range` | 3.33ms | 3.36ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_distinct_range` | 4.13ms | 4.16ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_index_scan` | 6.07ms | 6.82ms | 1.1× | 1.0% | PASS |
| ac_reads | `select_random_points` | 23.91ms | 26.32ms | 1.1× | 0.7% | PASS |
| ac_reads | `select_random_ranges` | 8.15ms | 8.64ms | 1.1× | 1.1% | PASS |
| ac_reads | `covering_index_scan` | 8.45ms | 9.80ms | 1.2× | 0.9% | PASS |
| ac_reads | `groupby_scan` | 30.17ms | 32.98ms | 1.1× | 0.7% | PASS |
| ac_reads | `index_join` | 7.70ms | 10.24ms | 1.3× | 1.1% | PASS |
| ac_reads | `index_join_scan` | 3.62ms | 5.53ms | 1.5× | 1.3% | PASS |
| ac_reads | `types_table_scan` | 859.99ms | 1.03s | 1.2× | 0.6% | PASS |
| ac_reads | `table_scan` | 992.25ms | 1.13s | 1.1× | 0.5% | PASS |
| ac_reads | `oltp_read_only` | 144.01ms | 152.45ms | 1.1× | 0.7% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 17.10ms | 52.16ms | 3.1× | 9.2% | PASS |
| ac_writes | `oltp_insert_ac` | 19.11ms | 67.65ms | 3.5× | 13.1% | PASS |
| ac_writes | `oltp_update_index_ac` | 20.03ms | 76.24ms | 3.8× | 12.0% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 17.14ms | 59.95ms | 3.5× | 10.4% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 18.54ms | 65.11ms | 3.5× | 9.4% | PASS |
| ac_writes | `oltp_write_only_ac` | 18.43ms | 62.30ms | 3.4× | 4.4% | PASS |
| ac_writes | `types_delete_insert_ac` | 16.02ms | 57.87ms | 3.6× | 6.0% | PASS |
| ac_writes | `oltp_read_write_ac` | 23.72ms | 69.70ms | 2.9× | 6.9% | PASS |

</details>

</details>

## Version-control latency

Wall time: 5m 58s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 25.90ms | 38.00ms | 68.2% | 2.0% | PASS |
| `status_dirty_many_tables` | 28.80ms | 42.00ms | 68.6% | 2.0% | PASS |
| `diff_regular_working_one_table` | 23.37ms | 33.00ms | 70.8% | 2.6% | PASS |
| `diff_regular_working_many_tables` | 30.26ms | 50.00ms | 60.5% | 1.7% | PASS |
| `diff_stat_working_many_tables` | 30.23ms | 48.00ms | 63.0% | 1.6% | PASS |
| `diff_schema_working_many_tables` | 30.15ms | 48.00ms | 62.8% | 1.2% | PASS |
| `branch_list_many_branches` | 17.72ms | 25.00ms | 70.9% | 1.6% | PASS |
| `branch_create_delete` | 19.40ms | 27.00ms | 71.8% | 1.2% | PASS |
| `at_literal_deep_history` | 19.01ms | 28.00ms | 67.9% | 1.7% | PASS |
| `diff_literal_deep_history` | 18.92ms | 28.00ms | 67.6% | 1.7% | PASS |
| `history_literal_deep_history` | 19.80ms | 30.00ms | 66.0% | 1.0% | PASS |
| `checkout_branch_clean` | 24.87ms | 43.00ms | 57.8% | 1.8% | PASS |
| `merge_data_no_conflicts` | 21.64ms | 33.00ms | 65.6% | 1.5% | PASS |
| `merge_data_secondary_index` | 707.81ms | 944.00ms | 75.0% | 1.3% | PASS |
| `merge_schema_no_conflicts` | 16.63ms | 24.00ms | 69.3% | 1.5% | PASS |
| `merge_data_conflicts` | 23.37ms | 33.00ms | 70.8% | 1.8% | PASS |
| `merge_data_conflicts_with_resolve` | 24.31ms | 34.00ms | 71.5% | 1.5% | PASS |

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

candidate: 00d4dd3.

Fixture file bytes: base 107,968,266; delta 115,878,997.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 407.827 | 404.331–445.926 | 32.302 | 245.20 | 107968266 |
| push_initial | candidate | 781.990 | 770.227–800.376 | 410.351 | 127.88 | 107956772 |
| clone_initial | candidate | 1025.846 | 990.521–1111.033 | 513.093 | 97.48 | 108475035 |
| push_noop | candidate | 0.607 | 0.575–0.716 | 0.309 | — | 0 |
| fetch_noop | candidate | 0.154 | 0.143–0.189 | 0.156 | — | 0 |
| push_incremental | candidate | 179.299 | 175.674–197.176 | 161.882 | 5.58 | 7909775 |
| fetch_incremental | candidate | 92.479 | 84.209–117.923 | 71.211 | 10.81 | 7910084 |
| pull_fast_forward | candidate | 77.769 | 75.190–88.940 | 60.453 | 12.86 | 7910844 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 90.49 | 175.487 | 108.763 | 18329 | 2066 |
| clone_initial | candidate | 90.82 | 295.232 | 109.784 | 47277 | 2337 |
| push_noop | candidate | 42.18 | 0.075 | 0.000 | 27 | 2 |
| fetch_noop | candidate | 42.18 | 0.135 | 0.000 | 40 | 0 |
| push_incremental | candidate | 42.18 | 96.636 | 7.910 | 22354 | 125 |
| fetch_incremental | candidate | 42.18 | 34.472 | 7.910 | 10808 | 124 |
| pull_fast_forward | candidate | 42.18 | 26.497 | 7.911 | 7031 | 126 |

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

candidate: 00d4dd3.

Fixture file bytes: base 1,087,718,031; delta 1,103,525,159.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 2652.674 | 2649.542–2665.133 | 388.112 | 376.98 | 1087718031 |
| push_initial | candidate | 8013.995 | 8004.078–8040.523 | 5793.780 | 124.78 | 1079566275 |
| clone_initial | candidate | 8817.750 | 8810.750–8822.356 | 8570.946 | 113.41 | 1102407326 |
| push_noop | candidate | 0.821 | 0.768–0.843 | 0.382 | — | 0 |
| fetch_noop | candidate | 0.203 | 0.189–0.203 | 0.204 | — | 0 |
| push_incremental | candidate | 1329.539 | 1327.469–1334.127 | 1324.575 | 0.75 | 15806003 |
| fetch_incremental | candidate | 272.538 | 271.979–278.428 | 266.592 | 3.67 | 15806513 |
| pull_fast_forward | candidate | 273.264 | 271.249–275.084 | 267.582 | 3.66 | 15807273 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 134.78 | 2091.203 | 1087.622 | 263333 | 20541 |
| clone_initial | candidate | 123.87 | 3516.371 | 1132.819 | 611952 | 32243 |
| push_noop | candidate | 44.75 | 0.078 | 0.000 | 27 | 2 |
| fetch_noop | candidate | 44.75 | 0.137 | 0.000 | 40 | 0 |
| push_incremental | candidate | 48.18 | 616.426 | 15.928 | 149725 | 313 |
| fetch_incremental | candidate | 50.27 | 62.070 | 15.928 | 13855 | 313 |
| pull_fast_forward | candidate | 50.32 | 62.002 | 15.929 | 13851 | 315 |

</details>


</details>

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh`, `test/vc_perf_ceiling.sh`, and `test/remote_perf.py`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
