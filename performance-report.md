# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-11-19 11:30 UTC
>
> Commit: [`9e78b8b6f273fa53252a51197a945ce0e8cbb98d`](https://github.com/dolthub/doltlite/commit/9e78b8b6f273fa53252a51197a945ce0e8cbb98d)
>
> Runner: ubuntu24 20261111.370.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/44497330101)

The SQL workloads compare optimized DoltLite against stock SQLite on the same GitHub-hosted runner, alternating execution order on each repetition. Version-control sections measure DoltLite latency. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 8.46s | 10.11s | 1.20× | 1.2% | **PASS** |
| Writes | 1.79s | 2.79s | 1.56× | 1.0% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.18s | 10.39s | 1.13× | 1.3% | **PASS** |
| Writes | 2.96s | 3.39s | 1.15× | 2.2% | **PASS** |
| Autocommit writes | 661.38ms | 1.24s | 1.88× | 3.8% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.8× for a section average. Durable autocommit writes use 3.6× and 2.6× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.38s | 2.79s | 1.17× | 1.1% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.48s | 3.19s | 1.29× | 1.3% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 1.71s | 1.93s | 1.13× | 2.5% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 1.89s | 2.20s | 1.16× | 0.8% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 437.32ms | 708.09ms | 1.62× | 1.0% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 579.11ms | 916.99ms | 1.58× | 1.0% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 399.22ms | 601.72ms | 1.51× | 2.6% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 378.38ms | 567.29ms | 1.50× | 1.0% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.59s | 2.83s | 1.09× | 1.0% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.70s | 3.28s | 1.22× | 1.2% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 1.94s | 2.06s | 1.07× | 1.9% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 1.95s | 2.21s | 1.13× | 0.9% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 595.38ms | 770.00ms | 1.29× | 1.6% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 907.87ms | 1.02s | 1.12× | 2.8% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 993.16ms | 979.00ms | 0.99× | 4.7% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 462.48ms | 624.09ms | 1.35× | 1.9% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.49s | 2.85s | 1.14× | 1.2% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.57s | 3.29s | 1.28× | 1.6% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 1.74s | 1.96s | 1.12× | 3.2% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 1.90s | 2.20s | 1.16× | 1.2% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 186.34ms | 370.90ms | 1.99× | 6.1% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 190.62ms | 375.02ms | 1.97× | 3.3% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 202.99ms | 328.74ms | 1.62× | 4.0% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 81.42ms | 169.38ms | 2.08× | 3.6% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 23.71ms | 29.28ms | 1.2× | 1.7% | PASS |
| mem_reads | `oltp_range_select` | 10.14ms | 11.89ms | 1.2× | 1.3% | PASS |
| mem_reads | `oltp_sum_range` | 9.26ms | 11.30ms | 1.2× | 1.1% | PASS |
| mem_reads | `oltp_order_range` | 2.55ms | 2.86ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 3.68ms | 4.02ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_index_scan` | 3.83ms | 5.14ms | 1.3× | 1.0% | PASS |
| mem_reads | `select_random_points` | 12.65ms | 11.37ms | 0.9× | 1.5% | PASS |
| mem_reads | `select_random_ranges` | 4.57ms | 5.01ms | 1.1× | 1.7% | PASS |
| mem_reads | `covering_index_scan` | 7.65ms | 10.54ms | 1.4× | 1.1% | PASS |
| mem_reads | `groupby_scan` | 29.87ms | 32.54ms | 1.1× | 0.8% | PASS |
| mem_reads | `index_join` | 5.74ms | 8.16ms | 1.4× | 1.9% | PASS |
| mem_reads | `index_join_scan` | 3.15ms | 5.36ms | 1.7× | 1.6% | PASS |
| mem_reads | `types_table_scan` | 1.02s | 1.20s | 1.2× | 0.4% | PASS |
| mem_reads | `table_scan` | 1.14s | 1.34s | 1.2× | 0.8% | PASS |
| mem_reads | `oltp_read_only` | 102.57ms | 118.54ms | 1.2× | 1.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 182.44ms | 280.06ms | 1.5× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 15.20ms | 24.89ms | 1.6× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 49.13ms | 91.45ms | 1.9× | 0.9% | PASS |
| mem_writes | `oltp_update_non_index` | 34.45ms | 57.27ms | 1.7× | 1.2% | PASS |
| mem_writes | `oltp_delete_insert` | 44.42ms | 72.42ms | 1.6× | 0.8% | PASS |
| mem_writes | `oltp_write_only` | 21.68ms | 45.05ms | 2.1× | 1.0% | PASS |
| mem_writes | `types_delete_insert` | 24.23ms | 33.44ms | 1.4× | 1.2% | PASS |
| mem_writes | `oltp_read_write` | 65.78ms | 103.52ms | 1.6× | 0.8% | PASS |
| file_reads | `oltp_point_select` | 88.11ms | 45.69ms | 0.5× | 0.7% | PASS |
| file_reads | `oltp_range_select` | 17.03ms | 13.79ms | 0.8× | 1.4% | PASS |
| file_reads | `oltp_sum_range` | 16.16ms | 13.21ms | 0.8× | 1.5% | PASS |
| file_reads | `oltp_order_range` | 3.42ms | 3.13ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_distinct_range` | 4.52ms | 4.29ms | 1.0× | 0.9% | PASS |
| file_reads | `oltp_index_scan` | 10.57ms | 7.28ms | 0.7× | 1.6% | PASS |
| file_reads | `select_random_points` | 20.15ms | 13.16ms | 0.7× | 2.0% | PASS |
| file_reads | `select_random_ranges` | 11.41ms | 6.84ms | 0.6× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 14.67ms | 12.74ms | 0.9× | 1.0% | PASS |
| file_reads | `groupby_scan` | 30.77ms | 32.87ms | 1.1× | 0.6% | PASS |
| file_reads | `index_join` | 9.63ms | 9.77ms | 1.0× | 1.9% | PASS |
| file_reads | `index_join_scan` | 4.09ms | 5.73ms | 1.4× | 1.6% | PASS |
| file_reads | `types_table_scan` | 1.02s | 1.18s | 1.2× | 0.5% | PASS |
| file_reads | `table_scan` | 1.14s | 1.33s | 1.2× | 0.7% | PASS |
| file_reads | `oltp_read_only` | 199.04ms | 143.75ms | 0.7× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 196.74ms | 292.10ms | 1.5× | 0.9% | PASS |
| file_writes | `oltp_insert` | 22.14ms | 28.32ms | 1.3× | 2.3% | PASS |
| file_writes | `oltp_update_index` | 76.11ms | 101.22ms | 1.3× | 2.1% | PASS |
| file_writes | `oltp_update_non_index` | 57.51ms | 63.69ms | 1.1× | 1.3% | PASS |
| file_writes | `oltp_delete_insert` | 67.55ms | 84.19ms | 1.2× | 1.7% | PASS |
| file_writes | `oltp_write_only` | 44.66ms | 50.79ms | 1.1× | 2.4% | PASS |
| file_writes | `types_delete_insert` | 39.76ms | 40.30ms | 1.0× | 1.6% | PASS |
| file_writes | `oltp_read_write` | 90.91ms | 109.40ms | 1.2× | 1.6% | PASS |
| ac_reads | `oltp_point_select` | 45.26ms | 45.74ms | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_range_select` | 12.59ms | 13.76ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_sum_range` | 11.75ms | 13.23ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_order_range` | 2.94ms | 3.13ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_distinct_range` | 4.01ms | 4.28ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 6.25ms | 7.25ms | 1.2× | 1.5% | PASS |
| ac_reads | `select_random_points` | 15.30ms | 13.16ms | 0.9× | 1.5% | PASS |
| ac_reads | `select_random_ranges` | 6.93ms | 6.84ms | 1.0× | 1.4% | PASS |
| ac_reads | `covering_index_scan` | 10.11ms | 12.70ms | 1.3× | 1.0% | PASS |
| ac_reads | `groupby_scan` | 30.25ms | 32.86ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 7.23ms | 9.79ms | 1.4× | 1.1% | PASS |
| ac_reads | `index_join_scan` | 3.59ms | 5.78ms | 1.6× | 2.1% | PASS |
| ac_reads | `types_table_scan` | 1.02s | 1.19s | 1.2× | 0.7% | PASS |
| ac_reads | `table_scan` | 1.17s | 1.34s | 1.1× | 0.9% | PASS |
| ac_reads | `oltp_read_only` | 137.30ms | 145.23ms | 1.1× | 1.3% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 20.77ms | 32.87ms | 1.6× | 5.2% | PASS |
| ac_writes | `oltp_insert_ac` | 23.97ms | 46.22ms | 1.9× | 7.1% | PASS |
| ac_writes | `oltp_update_index_ac` | 24.52ms | 59.08ms | 2.4× | 5.5% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.88ms | 40.75ms | 1.9× | 6.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 23.52ms | 48.61ms | 2.1× | 7.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 22.66ms | 48.62ms | 2.1× | 3.8% | PASS |
| ac_writes | `types_delete_insert_ac` | 20.41ms | 39.16ms | 1.9× | 7.1% | PASS |
| ac_writes | `oltp_read_write_ac` | 28.62ms | 55.59ms | 1.9× | 6.0% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 32.85ms | 37.66ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_range_select` | 15.13ms | 15.61ms | 1.0× | 1.4% | PASS |
| mem_reads | `oltp_sum_range` | 13.76ms | 14.94ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_order_range` | 3.03ms | 3.44ms | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_distinct_range` | 4.25ms | 4.59ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 3.80ms | 6.32ms | 1.7× | 1.5% | PASS |
| mem_reads | `select_random_points` | 19.74ms | 21.41ms | 1.1× | 1.4% | PASS |
| mem_reads | `select_random_ranges` | 6.10ms | 6.62ms | 1.1× | 1.5% | PASS |
| mem_reads | `covering_index_scan` | 7.73ms | 11.19ms | 1.4× | 0.8% | PASS |
| mem_reads | `groupby_scan` | 34.05ms | 35.90ms | 1.1× | 0.8% | PASS |
| mem_reads | `index_join` | 9.66ms | 9.90ms | 1.0× | 1.0% | PASS |
| mem_reads | `index_join_scan` | 3.46ms | 6.25ms | 1.8× | 1.6% | PASS |
| mem_reads | `types_table_scan` | 1.04s | 1.35s | 1.3× | 0.4% | PASS |
| mem_reads | `table_scan` | 1.15s | 1.53s | 1.3× | 0.4% | PASS |
| mem_reads | `oltp_read_only` | 132.34ms | 141.33ms | 1.1× | 0.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 243.24ms | 341.63ms | 1.4× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 17.91ms | 34.43ms | 1.9× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 63.14ms | 131.36ms | 2.1× | 1.3% | PASS |
| mem_writes | `oltp_update_non_index` | 46.27ms | 74.51ms | 1.6× | 1.6% | PASS |
| mem_writes | `oltp_delete_insert` | 52.28ms | 96.41ms | 1.8× | 0.8% | PASS |
| mem_writes | `oltp_write_only` | 26.83ms | 51.22ms | 1.9× | 1.5% | PASS |
| mem_writes | `types_delete_insert` | 36.88ms | 57.65ms | 1.6× | 0.8% | PASS |
| mem_writes | `oltp_read_write` | 92.56ms | 129.78ms | 1.4× | 1.0% | PASS |
| file_reads | `oltp_point_select` | 99.43ms | 54.25ms | 0.5× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 22.57ms | 17.36ms | 0.8× | 1.2% | PASS |
| file_reads | `oltp_sum_range` | 21.18ms | 16.79ms | 0.8× | 1.4% | PASS |
| file_reads | `oltp_order_range` | 3.87ms | 3.69ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_distinct_range` | 5.13ms | 4.88ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 10.57ms | 8.22ms | 0.8× | 0.9% | PASS |
| file_reads | `select_random_points` | 28.60ms | 23.90ms | 0.8× | 1.6% | PASS |
| file_reads | `select_random_ranges` | 13.28ms | 8.49ms | 0.6× | 1.4% | PASS |
| file_reads | `covering_index_scan` | 14.67ms | 13.09ms | 0.9× | 0.8% | PASS |
| file_reads | `groupby_scan` | 35.14ms | 36.25ms | 1.0× | 0.9% | PASS |
| file_reads | `index_join` | 13.79ms | 11.26ms | 0.8× | 1.3% | PASS |
| file_reads | `index_join_scan` | 4.53ms | 6.50ms | 1.4× | 1.9% | PASS |
| file_reads | `types_table_scan` | 1.04s | 1.38s | 1.3× | 0.5% | PASS |
| file_reads | `table_scan` | 1.16s | 1.53s | 1.3× | 0.3% | PASS |
| file_reads | `oltp_read_only` | 232.05ms | 167.20ms | 0.7× | 0.6% | PASS |
| file_writes | `oltp_bulk_insert` | 267.51ms | 354.85ms | 1.3× | 0.8% | PASS |
| file_writes | `oltp_insert` | 25.57ms | 39.65ms | 1.6× | 1.1% | PASS |
| file_writes | `oltp_update_index` | 131.78ms | 151.07ms | 1.1× | 7.8% | PASS |
| file_writes | `oltp_update_non_index` | 96.73ms | 88.35ms | 0.9× | 13.5% | PASS |
| file_writes | `oltp_delete_insert` | 91.94ms | 112.83ms | 1.2× | 1.2% | PASS |
| file_writes | `oltp_write_only` | 81.91ms | 63.93ms | 0.8× | 11.5% | PASS |
| file_writes | `types_delete_insert` | 68.53ms | 67.32ms | 1.0× | 1.0% | PASS |
| file_writes | `oltp_read_write` | 143.89ms | 141.86ms | 1.0× | 4.5% | PASS |
| ac_reads | `oltp_point_select` | 56.86ms | 54.26ms | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_range_select` | 18.91ms | 17.42ms | 0.9× | 3.3% | PASS |
| ac_reads | `oltp_sum_range` | 17.33ms | 16.79ms | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_order_range` | 3.50ms | 3.68ms | 1.1× | 2.0% | PASS |
| ac_reads | `oltp_distinct_range` | 4.73ms | 4.87ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_index_scan` | 6.31ms | 8.21ms | 1.3× | 2.0% | PASS |
| ac_reads | `select_random_points` | 24.58ms | 23.99ms | 1.0× | 2.4% | PASS |
| ac_reads | `select_random_ranges` | 9.09ms | 8.53ms | 0.9× | 2.5% | PASS |
| ac_reads | `covering_index_scan` | 10.38ms | 13.08ms | 1.3× | 1.4% | PASS |
| ac_reads | `groupby_scan` | 34.89ms | 36.28ms | 1.0× | 0.9% | PASS |
| ac_reads | `index_join` | 12.07ms | 11.28ms | 0.9× | 2.0% | PASS |
| ac_reads | `index_join_scan` | 4.14ms | 6.55ms | 1.6× | 1.2% | PASS |
| ac_reads | `types_table_scan` | 1.04s | 1.38s | 1.3× | 0.4% | PASS |
| ac_reads | `table_scan` | 1.16s | 1.53s | 1.3× | 0.5% | PASS |
| ac_reads | `oltp_read_only` | 169.68ms | 167.19ms | 1.0× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.25ms | 31.29ms | 1.5× | 3.6% | PASS |
| ac_writes | `oltp_insert_ac` | 23.90ms | 42.04ms | 1.8× | 2.7% | PASS |
| ac_writes | `oltp_update_index_ac` | 24.64ms | 61.91ms | 2.5× | 2.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 20.82ms | 38.72ms | 1.9× | 3.5% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.55ms | 53.31ms | 2.2× | 3.5% | PASS |
| ac_writes | `oltp_write_only_ac` | 23.99ms | 50.04ms | 2.1× | 2.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 21.67ms | 39.42ms | 1.8× | 3.2% | PASS |
| ac_writes | `oltp_read_write_ac` | 29.80ms | 58.29ms | 2.0× | 5.0% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 25.11ms | 24.49ms | 1.0× | 4.2% | PASS |
| mem_reads | `oltp_range_select` | 10.50ms | 10.20ms | 1.0× | 1.8% | PASS |
| mem_reads | `oltp_sum_range` | 10.21ms | 9.76ms | 1.0× | 2.3% | PASS |
| mem_reads | `oltp_order_range` | 2.21ms | 2.25ms | 1.0× | 2.1% | PASS |
| mem_reads | `oltp_distinct_range` | 2.70ms | 2.77ms | 1.0× | 2.8% | PASS |
| mem_reads | `oltp_index_scan` | 2.68ms | 4.72ms | 1.8× | 2.7% | PASS |
| mem_reads | `select_random_points` | 17.40ms | 15.79ms | 0.9× | 3.3% | PASS |
| mem_reads | `select_random_ranges` | 4.78ms | 4.29ms | 0.9× | 2.5% | PASS |
| mem_reads | `covering_index_scan` | 4.22ms | 7.08ms | 1.7× | 2.4% | PASS |
| mem_reads | `groupby_scan` | 21.62ms | 21.47ms | 1.0× | 1.0% | PASS |
| mem_reads | `index_join` | 8.54ms | 8.18ms | 1.0× | 3.4% | PASS |
| mem_reads | `index_join_scan` | 2.97ms | 5.79ms | 2.0× | 3.9% | PASS |
| mem_reads | `types_table_scan` | 698.62ms | 815.86ms | 1.2× | 1.2% | PASS |
| mem_reads | `table_scan` | 817.00ms | 918.35ms | 1.1× | 2.5% | PASS |
| mem_reads | `oltp_read_only` | 83.81ms | 80.58ms | 1.0× | 3.2% | PASS |
| mem_writes | `oltp_bulk_insert` | 144.67ms | 203.33ms | 1.4× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 11.31ms | 22.31ms | 2.0× | 1.1% | PASS |
| mem_writes | `oltp_update_index` | 45.61ms | 90.17ms | 2.0× | 2.9% | PASS |
| mem_writes | `oltp_update_non_index` | 36.66ms | 51.30ms | 1.4× | 2.4% | PASS |
| mem_writes | `oltp_delete_insert` | 37.40ms | 67.18ms | 1.8× | 2.4% | PASS |
| mem_writes | `oltp_write_only` | 20.39ms | 36.54ms | 1.8× | 3.2% | PASS |
| mem_writes | `types_delete_insert` | 27.83ms | 38.56ms | 1.4× | 2.8% | PASS |
| mem_writes | `oltp_read_write` | 75.35ms | 92.34ms | 1.2× | 3.4% | PASS |
| file_reads | `oltp_point_select` | 80.38ms | 39.32ms | 0.5× | 1.7% | PASS |
| file_reads | `oltp_range_select` | 17.34ms | 12.45ms | 0.7× | 2.3% | PASS |
| file_reads | `oltp_sum_range` | 16.91ms | 12.13ms | 0.7× | 2.8% | PASS |
| file_reads | `oltp_order_range` | 2.91ms | 2.52ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_distinct_range` | 3.37ms | 2.98ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 8.09ms | 5.94ms | 0.7× | 2.1% | PASS |
| file_reads | `select_random_points` | 23.17ms | 16.21ms | 0.7× | 5.3% | PASS |
| file_reads | `select_random_ranges` | 10.34ms | 5.76ms | 0.6× | 1.5% | PASS |
| file_reads | `covering_index_scan` | 10.01ms | 8.29ms | 0.8× | 1.6% | PASS |
| file_reads | `groupby_scan` | 22.23ms | 21.52ms | 1.0× | 1.2% | PASS |
| file_reads | `index_join` | 11.93ms | 8.58ms | 0.7× | 4.6% | PASS |
| file_reads | `index_join_scan` | 3.55ms | 5.66ms | 1.6× | 5.1% | PASS |
| file_reads | `types_table_scan` | 780.81ms | 913.28ms | 1.2× | 1.9% | PASS |
| file_reads | `table_scan` | 793.76ms | 911.80ms | 1.1× | 1.9% | PASS |
| file_reads | `oltp_read_only` | 153.49ms | 98.36ms | 0.6× | 2.2% | PASS |
| file_writes | `oltp_bulk_insert` | 214.97ms | 269.75ms | 1.3× | 3.1% | PASS |
| file_writes | `oltp_insert` | 19.11ms | 44.69ms | 2.3× (paired 2.2×) | 11.2% | PASS |
| file_writes | `oltp_update_index` | 141.35ms | 160.88ms | 1.1× | 4.1% | PASS |
| file_writes | `oltp_update_non_index` | 123.48ms | 104.74ms | 0.8× | 5.8% | PASS |
| file_writes | `oltp_delete_insert` | 148.99ms | 123.80ms | 0.8× | 5.7% | PASS |
| file_writes | `oltp_write_only` | 100.82ms | 79.07ms | 0.8× | 4.7% | PASS |
| file_writes | `types_delete_insert` | 103.24ms | 69.47ms | 0.7× | 4.7% | PASS |
| file_writes | `oltp_read_write` | 141.21ms | 126.61ms | 0.9× | 3.9% | PASS |
| ac_reads | `oltp_point_select` | 41.29ms | 36.65ms | 0.9× | 3.8% | PASS |
| ac_reads | `oltp_range_select` | 12.58ms | 11.28ms | 0.9× | 3.2% | PASS |
| ac_reads | `oltp_sum_range` | 13.32ms | 11.75ms | 0.9× | 3.8% | PASS |
| ac_reads | `oltp_order_range` | 2.62ms | 2.51ms | 1.0× | 2.4% | PASS |
| ac_reads | `oltp_distinct_range` | 3.10ms | 3.02ms | 1.0× | 1.8% | PASS |
| ac_reads | `oltp_index_scan` | 5.09ms | 6.30ms | 1.2× | 3.4% | PASS |
| ac_reads | `select_random_points` | 20.05ms | 17.11ms | 0.9× | 10.7% | PASS |
| ac_reads | `select_random_ranges` | 6.94ms | 5.76ms | 0.8× | 2.3% | PASS |
| ac_reads | `covering_index_scan` | 6.82ms | 8.70ms | 1.3× | 4.4% | PASS |
| ac_reads | `groupby_scan` | 22.13ms | 21.60ms | 1.0× | 1.7% | PASS |
| ac_reads | `index_join` | 9.64ms | 8.30ms | 0.9× | 5.1% | PASS |
| ac_reads | `index_join_scan` | 3.06ms | 5.00ms | 1.6× | 3.2% | PASS |
| ac_reads | `types_table_scan` | 698.43ms | 812.84ms | 1.2× | 2.6% | PASS |
| ac_reads | `table_scan` | 794.22ms | 907.32ms | 1.1× | 1.7% | PASS |
| ac_reads | `oltp_read_only` | 102.51ms | 97.32ms | 0.9× | 1.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 24.56ms | 27.19ms | 1.1× | 6.1% | PASS |
| ac_writes | `oltp_insert_ac` | 27.19ms | 40.91ms | 1.5× | 3.7% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.89ms | 56.81ms | 2.1× | 3.8% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 20.64ms | 33.22ms | 1.6× | 3.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.75ms | 44.29ms | 1.7× | 6.0% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.19ms | 43.31ms | 1.7× | 4.4% | PASS |
| ac_writes | `types_delete_insert_ac` | 21.74ms | 36.49ms | 1.7× | 4.1% | PASS |
| ac_writes | `oltp_read_write_ac` | 29.03ms | 46.53ms | 1.6× | 3.2% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 21.45ms | 23.85ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_range_select` | 14.03ms | 15.66ms | 1.1× | 0.6% | PASS |
| mem_reads | `oltp_sum_range` | 13.37ms | 14.40ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_order_range` | 2.69ms | 2.83ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 3.37ms | 3.55ms | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_index_scan` | 3.22ms | 4.13ms | 1.3× | 1.5% | PASS |
| mem_reads | `select_random_points` | 21.01ms | 21.64ms | 1.0× | 0.5% | PASS |
| mem_reads | `select_random_ranges` | 5.11ms | 5.84ms | 1.1× | 0.9% | PASS |
| mem_reads | `covering_index_scan` | 4.80ms | 6.81ms | 1.4× | 0.9% | PASS |
| mem_reads | `groupby_scan` | 25.12ms | 28.04ms | 1.1× | 0.3% | PASS |
| mem_reads | `index_join` | 5.79ms | 7.78ms | 1.3× | 0.8% | PASS |
| mem_reads | `index_join_scan` | 2.93ms | 4.89ms | 1.7× | 2.3% | PASS |
| mem_reads | `types_table_scan` | 775.67ms | 923.00ms | 1.2× | 0.4% | PASS |
| mem_reads | `table_scan` | 888.87ms | 1.02s | 1.1× | 0.7% | PASS |
| mem_reads | `oltp_read_only` | 99.78ms | 113.26ms | 1.1× | 0.7% | PASS |
| mem_writes | `oltp_bulk_insert` | 151.05ms | 193.28ms | 1.3× | 0.5% | PASS |
| mem_writes | `oltp_insert` | 12.34ms | 19.78ms | 1.6× | 0.6% | PASS |
| mem_writes | `oltp_update_index` | 45.50ms | 81.36ms | 1.8× | 0.9% | PASS |
| mem_writes | `oltp_update_non_index` | 33.05ms | 53.62ms | 1.6× | 0.9% | PASS |
| mem_writes | `oltp_delete_insert` | 33.58ms | 57.88ms | 1.7× | 1.2% | PASS |
| mem_writes | `oltp_write_only` | 18.05ms | 34.38ms | 1.9× | 1.1% | PASS |
| mem_writes | `types_delete_insert` | 20.81ms | 33.37ms | 1.6× | 1.2% | PASS |
| mem_writes | `oltp_read_write` | 64.00ms | 93.63ms | 1.5× | 1.1% | PASS |
| file_reads | `oltp_point_select` | 41.34ms | 28.34ms | 0.7× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 16.14ms | 16.30ms | 1.0× | 0.9% | PASS |
| file_reads | `oltp_sum_range` | 15.65ms | 15.12ms | 1.0× | 0.9% | PASS |
| file_reads | `oltp_order_range` | 2.90ms | 2.95ms | 1.0× | 1.6% | PASS |
| file_reads | `oltp_distinct_range` | 3.46ms | 3.65ms | 1.1× | 2.2% | PASS |
| file_reads | `oltp_index_scan` | 5.42ms | 4.95ms | 0.9× | 2.0% | PASS |
| file_reads | `select_random_points` | 23.12ms | 22.40ms | 1.0× | 0.8% | PASS |
| file_reads | `select_random_ranges` | 7.21ms | 6.36ms | 0.9× | 0.7% | PASS |
| file_reads | `covering_index_scan` | 6.92ms | 7.29ms | 1.1× | 0.9% | PASS |
| file_reads | `groupby_scan` | 25.34ms | 28.13ms | 1.1× | 0.4% | PASS |
| file_reads | `index_join` | 7.04ms | 8.29ms | 1.2× | 1.0% | PASS |
| file_reads | `index_join_scan` | 3.22ms | 5.14ms | 1.6× | 3.0% | PASS |
| file_reads | `types_table_scan` | 777.83ms | 922.47ms | 1.2× | 0.5% | PASS |
| file_reads | `table_scan` | 880.70ms | 1.02s | 1.2× | 1.6% | PASS |
| file_reads | `oltp_read_only` | 131.43ms | 120.92ms | 0.9× | 1.5% | PASS |
| file_writes | `oltp_bulk_insert` | 161.36ms | 203.05ms | 1.3× | 0.7% | PASS |
| file_writes | `oltp_insert` | 15.62ms | 23.28ms | 1.5× | 1.5% | PASS |
| file_writes | `oltp_update_index` | 59.49ms | 92.87ms | 1.6× | 2.3% | PASS |
| file_writes | `oltp_update_non_index` | 45.17ms | 58.44ms | 1.3× | 2.0% | PASS |
| file_writes | `oltp_delete_insert` | 46.07ms | 68.10ms | 1.5× | 1.8% | PASS |
| file_writes | `oltp_write_only` | 29.16ms | 40.58ms | 1.4× | 2.9% | PASS |
| file_writes | `types_delete_insert` | 29.07ms | 37.81ms | 1.3× | 2.1% | PASS |
| file_writes | `oltp_read_write` | 76.53ms | 99.96ms | 1.3× | 1.3% | PASS |
| ac_reads | `oltp_point_select` | 27.67ms | 28.17ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_range_select` | 14.56ms | 16.19ms | 1.1× | 1.7% | PASS |
| ac_reads | `oltp_sum_range` | 14.10ms | 15.03ms | 1.1× | 1.6% | PASS |
| ac_reads | `oltp_order_range` | 2.74ms | 2.89ms | 1.1× | 1.5% | PASS |
| ac_reads | `oltp_distinct_range` | 3.42ms | 3.61ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_index_scan` | 3.96ms | 4.61ms | 1.2× | 1.4% | PASS |
| ac_reads | `select_random_points` | 21.60ms | 22.15ms | 1.0× | 0.5% | PASS |
| ac_reads | `select_random_ranges` | 5.86ms | 6.43ms | 1.1× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 5.55ms | 7.42ms | 1.3× | 0.9% | PASS |
| ac_reads | `groupby_scan` | 25.28ms | 28.23ms | 1.1× | 0.4% | PASS |
| ac_reads | `index_join` | 6.38ms | 8.43ms | 1.3× | 1.2% | PASS |
| ac_reads | `index_join_scan` | 3.09ms | 4.95ms | 1.6× | 2.2% | PASS |
| ac_reads | `types_table_scan` | 773.38ms | 916.23ms | 1.2× | 0.4% | PASS |
| ac_reads | `table_scan` | 873.72ms | 1.01s | 1.2× | 1.2% | PASS |
| ac_reads | `oltp_read_only` | 115.12ms | 121.77ms | 1.1× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 8.73ms | 12.67ms | 1.5× | 1.1% | PASS |
| ac_writes | `oltp_insert_ac` | 9.90ms | 20.43ms | 2.1× | 1.3% | PASS |
| ac_writes | `oltp_update_index_ac` | 11.09ms | 29.27ms | 2.6× | 3.5% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 9.21ms | 16.81ms | 1.8× | 3.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 10.32ms | 23.33ms | 2.3× | 3.9% | PASS |
| ac_writes | `oltp_write_only_ac` | 10.13ms | 22.09ms | 2.2× | 4.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 8.87ms | 18.05ms | 2.0× | 5.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 13.17ms | 26.73ms | 2.0× | 1.4% | PASS |

</details>

</details>

## Version-control latency

Wall time: 4m 47s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 22.38ms | 38.00ms | 58.9% | 1.0% | PASS |
| `status_dirty_many_tables` | 24.79ms | 42.00ms | 59.0% | 0.9% | PASS |
| `diff_regular_working_one_table` | 21.12ms | 33.00ms | 64.0% | 0.6% | PASS |
| `diff_regular_working_many_tables` | 27.66ms | 50.00ms | 55.3% | 0.8% | PASS |
| `diff_stat_working_many_tables` | 27.83ms | 48.00ms | 58.0% | 0.8% | PASS |
| `diff_schema_working_many_tables` | 28.45ms | 48.00ms | 59.3% | 0.7% | PASS |
| `branch_list_many_branches` | 15.17ms | 25.00ms | 60.7% | 0.6% | PASS |
| `branch_create_delete` | 16.85ms | 27.00ms | 62.4% | 1.0% | PASS |
| `at_literal_deep_history` | 16.46ms | 28.00ms | 58.8% | 0.7% | PASS |
| `diff_literal_deep_history` | 16.50ms | 28.00ms | 58.9% | 0.7% | PASS |
| `history_literal_deep_history` | 17.18ms | 30.00ms | 57.3% | 1.0% | PASS |
| `checkout_branch_clean` | 22.45ms | 43.00ms | 52.2% | 0.8% | PASS |
| `merge_data_no_conflicts` | 18.02ms | 33.00ms | 54.6% | 0.8% | PASS |
| `merge_data_secondary_index` | 613.15ms | 944.00ms | 65.0% | 1.4% | PASS |
| `merge_schema_no_conflicts` | 14.18ms | 24.00ms | 59.1% | 0.7% | PASS |
| `merge_data_conflicts` | 19.92ms | 33.00ms | 60.4% | 0.8% | PASS |
| `merge_data_conflicts_with_resolve` | 20.47ms | 34.00ms | 60.2% | 0.4% | PASS |

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

candidate: 9e78b8b.

Fixture file bytes: base 107,968,301; delta 115,879,032.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 45.398 | 44.149–61.764 | 43.987 | 2202.74 | 107968301 |
| push_initial | candidate | 333.130 | 330.081–337.266 | 332.128 | 300.18 | 107956807 |
| clone_initial | candidate | 456.189 | 434.779–459.956 | 444.512 | 219.21 | 108475105 |
| push_noop | candidate | 0.349 | 0.322–0.389 | 0.214 | — | 0 |
| fetch_noop | candidate | 0.106 | 0.092–0.118 | 0.107 | — | 0 |
| push_incremental | candidate | 135.338 | 135.103–136.674 | 134.430 | 7.39 | 7909775 |
| fetch_incremental | candidate | 59.730 | 58.657–61.178 | 58.826 | 16.74 | 7910084 |
| pull_fast_forward | candidate | 53.406 | 52.289–53.772 | 51.831 | 18.72 | 7910844 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 90.46 | 175.487 | 108.763 | 18329 | 2066 |
| clone_initial | candidate | 90.93 | 294.967 | 110.833 | 47216 | 2353 |
| push_noop | candidate | 41.38 | 0.073 | 0.000 | 27 | 2 |
| fetch_noop | candidate | 41.38 | 0.135 | 0.000 | 42 | 0 |
| push_incremental | candidate | 41.38 | 96.689 | 7.910 | 22370 | 125 |
| fetch_incremental | candidate | 41.38 | 34.472 | 7.910 | 10810 | 124 |
| pull_fast_forward | candidate | 41.38 | 26.497 | 8.959 | 7032 | 142 |

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

candidate: 9e78b8b.

Fixture file bytes: base 1,087,718,066; delta 1,103,525,194.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 2663.214 | 2633.986–2669.673 | 378.851 | 375.49 | 1087718066 |
| push_initial | candidate | 8038.394 | 7981.489–8063.974 | 5798.318 | 124.40 | 1079566310 |
| clone_initial | candidate | 8729.609 | 8709.556–8761.956 | 8458.192 | 114.55 | 1102407396 |
| push_noop | candidate | 0.725 | 0.714–0.741 | 0.316 | — | 0 |
| fetch_noop | candidate | 0.220 | 0.194–0.228 | 0.223 | — | 0 |
| push_incremental | candidate | 1305.950 | 1291.057–1306.682 | 1300.194 | 0.77 | 15806003 |
| fetch_incremental | candidate | 276.732 | 269.414–277.219 | 270.432 | 3.61 | 15806513 |
| pull_fast_forward | candidate | 270.794 | 269.968–274.631 | 263.086 | 3.69 | 15807273 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 134.69 | 2091.523 | 1087.622 | 263411 | 20541 |
| clone_initial | candidate | 123.97 | 3522.929 | 1133.868 | 613593 | 32259 |
| push_noop | candidate | 44.44 | 0.078 | 0.000 | 28 | 2 |
| fetch_noop | candidate | 44.44 | 0.137 | 0.000 | 42 | 0 |
| push_incremental | candidate | 48.16 | 618.863 | 15.928 | 150335 | 313 |
| fetch_incremental | candidate | 50.24 | 62.320 | 15.928 | 13918 | 313 |
| pull_fast_forward | candidate | 50.14 | 62.252 | 16.978 | 13913 | 331 |

</details>


</details>

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh`, `test/vc_perf_ceiling.sh`, and `test/remote_perf.py`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
