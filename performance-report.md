# DoltLite Performance Report

> Nightly result: **FAIL**
>
> Generated: 2026-11-14 11:25 UTC
>
> Commit: [`cf881243361e047aa96da88c8dd2305a314f1bf7`](https://github.com/dolthub/doltlite/commit/cf881243361e047aa96da88c8dd2305a314f1bf7)
>
> Runner: ubuntu24 20261111.370.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/43580992193)

The SQL workloads compare optimized DoltLite against stock SQLite on the same GitHub-hosted runner, alternating execution order on each repetition. Version-control sections measure DoltLite latency. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 8.98s | 11.05s | 1.23× | 1.4% | **PASS** |
| Writes | 1.82s | 2.78s | 1.52× | 1.1% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.60s | 11.22s | 1.17× | 1.1% | **PASS** |
| Writes | 3.51s | 3.76s | 1.07× | 2.7% | **PASS** |
| Autocommit writes | 704.43ms | 1.40s | 1.99× | 4.1% | **FAIL** |

The absolute ceiling is 2.3× per ordinary workload and 1.8× for a section average. Durable autocommit writes use 3.0× and 2.4× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 1.74s | 2.03s | 1.16× | 1.6% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.76s | 3.48s | 1.26× | 2.2% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.07s | 2.58s | 1.25× | 1.2% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.40s | 2.95s | 1.23× | 1.1% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 269.12ms | 421.86ms | 1.57× | 1.1% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 602.80ms | 920.65ms | 1.53× | 1.4% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 458.64ms | 686.87ms | 1.50× | 1.1% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 492.06ms | 749.58ms | 1.52× | 0.9% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 1.81s | 2.04s | 1.13× | 1.3% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.95s | 3.54s | 1.20× | 1.7% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.25s | 2.61s | 1.16× | 0.8% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.58s | 3.03s | 1.17× | 0.9% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 335.40ms | 454.81ms | 1.36× | 2.6% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 942.83ms | 1.03s | 1.09× | 3.1% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 1.17s | 1.14s | 0.98× | 6.6% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 1.06s | 1.13s | 1.06× | 2.1% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 1.77s | 2.04s | 1.15× | 1.9% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.74s | 3.52s | 1.28× | 1.7% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.16s | 2.63s | 1.22× | 1.2% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.45s | 2.97s | 1.21× | 0.9% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 75.62ms | 156.40ms | 2.07× | 1.8% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 196.69ms | 389.52ms | 1.98× | 5.0% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 176.15ms | 434.95ms | 2.47× | 27.4% | **FAIL** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 255.97ms | 419.77ms | 1.64× | 3.4% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 14.69ms | 16.27ms | 1.1× | 2.4% | PASS |
| mem_reads | `oltp_range_select` | 6.79ms | 8.24ms | 1.2× | 6.4% | PASS |
| mem_reads | `oltp_sum_range` | 5.92ms | 7.50ms | 1.3× | 5.8% | PASS |
| mem_reads | `oltp_order_range` | 1.79ms | 2.02ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_distinct_range` | 2.45ms | 2.78ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_index_scan` | 2.37ms | 3.03ms | 1.3× | 2.8% | PASS |
| mem_reads | `select_random_points` | 8.79ms | 7.69ms | 0.9× | 2.3% | PASS |
| mem_reads | `select_random_ranges` | 2.69ms | 2.94ms | 1.1× | 2.6% | PASS |
| mem_reads | `covering_index_scan` | 4.74ms | 6.51ms | 1.4× | 0.5% | PASS |
| mem_reads | `groupby_scan` | 20.85ms | 22.88ms | 1.1× | 1.2% | PASS |
| mem_reads | `index_join` | 3.92ms | 5.57ms | 1.4× | 1.5% | PASS |
| mem_reads | `index_join_scan` | 2.11ms | 3.74ms | 1.8× | 4.9% | PASS |
| mem_reads | `types_table_scan` | 751.84ms | 878.30ms | 1.2× | 0.2% | PASS |
| mem_reads | `table_scan` | 847.08ms | 981.96ms | 1.2× | 0.5% | PASS |
| mem_reads | `oltp_read_only` | 68.75ms | 77.41ms | 1.1× | 0.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 107.48ms | 158.73ms | 1.5× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 9.51ms | 14.87ms | 1.6× | 0.6% | PASS |
| mem_writes | `oltp_update_index` | 31.69ms | 57.71ms | 1.8× | 1.2% | PASS |
| mem_writes | `oltp_update_non_index` | 20.91ms | 34.58ms | 1.7× | 3.3% | PASS |
| mem_writes | `oltp_delete_insert` | 29.63ms | 44.92ms | 1.5× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 13.96ms | 27.83ms | 2.0× | 1.8% | PASS |
| mem_writes | `types_delete_insert` | 14.99ms | 20.20ms | 1.3× | 1.5% | PASS |
| mem_writes | `oltp_read_write` | 40.95ms | 63.03ms | 1.5× | 0.9% | PASS |
| file_reads | `oltp_point_select` | 34.52ms | 20.85ms | 0.6× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 8.84ms | 8.81ms | 1.0× | 4.4% | PASS |
| file_reads | `oltp_sum_range` | 8.62ms | 8.21ms | 1.0× | 2.8% | PASS |
| file_reads | `oltp_order_range` | 2.06ms | 2.11ms | 1.0× | 1.6% | PASS |
| file_reads | `oltp_distinct_range` | 2.67ms | 2.82ms | 1.1× | 0.9% | PASS |
| file_reads | `oltp_index_scan` | 4.63ms | 3.72ms | 0.8× | 1.5% | PASS |
| file_reads | `select_random_points` | 11.99ms | 9.38ms | 0.8× | 2.8% | PASS |
| file_reads | `select_random_ranges` | 4.92ms | 3.42ms | 0.7× | 2.1% | PASS |
| file_reads | `covering_index_scan` | 6.82ms | 7.02ms | 1.0× | 0.4% | PASS |
| file_reads | `groupby_scan` | 21.61ms | 22.94ms | 1.1× | 1.3% | PASS |
| file_reads | `index_join` | 5.17ms | 5.96ms | 1.2× | 1.0% | PASS |
| file_reads | `index_join_scan` | 2.31ms | 3.78ms | 1.6× | 3.2% | PASS |
| file_reads | `types_table_scan` | 752.68ms | 878.15ms | 1.2× | 0.2% | PASS |
| file_reads | `table_scan` | 848.19ms | 982.93ms | 1.2× | 0.4% | PASS |
| file_reads | `oltp_read_only` | 96.80ms | 84.21ms | 0.9× | 0.6% | PASS |
| file_writes | `oltp_bulk_insert` | 116.24ms | 166.18ms | 1.4× | 1.3% | PASS |
| file_writes | `oltp_insert` | 12.44ms | 16.67ms | 1.3× | 1.9% | PASS |
| file_writes | `oltp_update_index` | 44.35ms | 62.57ms | 1.4× | 2.7% | PASS |
| file_writes | `oltp_update_non_index` | 30.32ms | 37.81ms | 1.2× | 2.1% | PASS |
| file_writes | `oltp_delete_insert` | 36.89ms | 50.53ms | 1.4× | 2.7% | PASS |
| file_writes | `oltp_write_only` | 22.56ms | 30.67ms | 1.4× | 3.8% | PASS |
| file_writes | `types_delete_insert` | 21.97ms | 23.90ms | 1.1× | 2.5% | PASS |
| file_writes | `oltp_read_write` | 50.64ms | 66.48ms | 1.3× | 2.6% | PASS |
| ac_reads | `oltp_point_select` | 21.04ms | 20.78ms | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_range_select` | 8.03ms | 8.93ms | 1.1× | 4.2% | PASS |
| ac_reads | `oltp_sum_range` | 6.72ms | 8.00ms | 1.2× | 3.7% | PASS |
| ac_reads | `oltp_order_range` | 1.87ms | 2.08ms | 1.1× | 2.4% | PASS |
| ac_reads | `oltp_distinct_range` | 2.59ms | 2.80ms | 1.1× | 2.7% | PASS |
| ac_reads | `oltp_index_scan` | 3.15ms | 3.62ms | 1.1× | 1.8% | PASS |
| ac_reads | `select_random_points` | 9.78ms | 8.66ms | 0.9× | 5.1% | PASS |
| ac_reads | `select_random_ranges` | 3.66ms | 3.54ms | 1.0× | 2.4% | PASS |
| ac_reads | `covering_index_scan` | 5.43ms | 7.02ms | 1.3× | 0.4% | PASS |
| ac_reads | `groupby_scan` | 21.12ms | 22.92ms | 1.1× | 1.7% | PASS |
| ac_reads | `index_join` | 4.32ms | 5.70ms | 1.3× | 1.5% | PASS |
| ac_reads | `index_join_scan` | 2.19ms | 3.69ms | 1.7× | 4.5% | PASS |
| ac_reads | `types_table_scan` | 752.97ms | 878.67ms | 1.2× | 0.2% | PASS |
| ac_reads | `table_scan` | 852.72ms | 981.72ms | 1.2× | 0.4% | PASS |
| ac_reads | `oltp_read_only` | 78.11ms | 84.47ms | 1.1× | 0.6% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 8.44ms | 12.92ms | 1.5× | 1.3% | PASS |
| ac_writes | `oltp_insert_ac` | 9.45ms | 18.78ms | 2.0× | 1.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 10.16ms | 26.09ms | 2.6× | 1.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 8.54ms | 15.69ms | 1.8× | 1.4% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 9.22ms | 20.91ms | 2.3× | 2.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 9.35ms | 20.41ms | 2.2× | 1.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 8.77ms | 17.36ms | 2.0× | 4.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 11.69ms | 24.25ms | 2.1× | 2.0% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 36.12ms | 37.64ms | 1.0× | 2.7% | PASS |
| mem_reads | `oltp_range_select` | 17.55ms | 17.05ms | 1.0× | 2.2% | PASS |
| mem_reads | `oltp_sum_range` | 15.38ms | 15.61ms | 1.0× | 2.2% | PASS |
| mem_reads | `oltp_order_range` | 3.33ms | 3.56ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_distinct_range` | 4.46ms | 4.67ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 3.99ms | 6.44ms | 1.6× | 1.6% | PASS |
| mem_reads | `select_random_points` | 22.92ms | 21.49ms | 0.9× | 3.5% | PASS |
| mem_reads | `select_random_ranges` | 6.85ms | 6.71ms | 1.0× | 2.4% | PASS |
| mem_reads | `covering_index_scan` | 7.70ms | 11.59ms | 1.5× | 0.8% | PASS |
| mem_reads | `groupby_scan` | 35.06ms | 36.33ms | 1.0× | 0.5% | PASS |
| mem_reads | `index_join` | 10.69ms | 10.29ms | 1.0× | 2.3% | PASS |
| mem_reads | `index_join_scan` | 3.78ms | 6.36ms | 1.7× | 2.2% | PASS |
| mem_reads | `types_table_scan` | 1.10s | 1.50s | 1.4× | 2.2% | PASS |
| mem_reads | `table_scan` | 1.35s | 1.65s | 1.2× | 1.4% | PASS |
| mem_reads | `oltp_read_only` | 151.27ms | 150.87ms | 1.0× | 1.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 238.27ms | 332.10ms | 1.4× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 17.83ms | 34.46ms | 1.9× | 1.1% | PASS |
| mem_writes | `oltp_update_index` | 66.81ms | 133.22ms | 2.0× | 1.5% | PASS |
| mem_writes | `oltp_update_non_index` | 49.52ms | 75.00ms | 1.5× | 1.3% | PASS |
| mem_writes | `oltp_delete_insert` | 54.73ms | 97.95ms | 1.8× | 1.2% | PASS |
| mem_writes | `oltp_write_only` | 28.25ms | 51.97ms | 1.8× | 1.9% | PASS |
| mem_writes | `types_delete_insert` | 40.11ms | 59.01ms | 1.5× | 2.2% | PASS |
| mem_writes | `oltp_read_write` | 107.28ms | 136.96ms | 1.3× | 1.6% | PASS |
| file_reads | `oltp_point_select` | 103.70ms | 54.97ms | 0.5× | 1.1% | PASS |
| file_reads | `oltp_range_select` | 24.94ms | 19.02ms | 0.8× | 2.0% | PASS |
| file_reads | `oltp_sum_range` | 22.70ms | 17.54ms | 0.8× | 1.6% | PASS |
| file_reads | `oltp_order_range` | 4.21ms | 3.82ms | 0.9× | 0.9% | PASS |
| file_reads | `oltp_distinct_range` | 5.36ms | 4.94ms | 0.9× | 1.1% | PASS |
| file_reads | `oltp_index_scan` | 10.82ms | 8.28ms | 0.8× | 1.8% | PASS |
| file_reads | `select_random_points` | 31.86ms | 24.43ms | 0.8× | 2.8% | PASS |
| file_reads | `select_random_ranges` | 13.84ms | 8.52ms | 0.6× | 1.7% | PASS |
| file_reads | `covering_index_scan` | 14.78ms | 13.63ms | 0.9× | 1.6% | PASS |
| file_reads | `groupby_scan` | 35.63ms | 36.45ms | 1.0× | 1.2% | PASS |
| file_reads | `index_join` | 14.57ms | 11.45ms | 0.8× | 2.2% | PASS |
| file_reads | `index_join_scan` | 4.76ms | 6.73ms | 1.4× | 2.4% | PASS |
| file_reads | `types_table_scan` | 1.14s | 1.52s | 1.3× | 3.5% | PASS |
| file_reads | `table_scan` | 1.28s | 1.64s | 1.3× | 3.2% | PASS |
| file_reads | `oltp_read_only` | 241.28ms | 173.56ms | 0.7× | 1.3% | PASS |
| file_writes | `oltp_bulk_insert` | 262.87ms | 347.26ms | 1.3× | 0.9% | PASS |
| file_writes | `oltp_insert` | 25.82ms | 40.12ms | 1.6× | 1.5% | PASS |
| file_writes | `oltp_update_index` | 138.46ms | 157.79ms | 1.1× | 5.1% | PASS |
| file_writes | `oltp_update_non_index` | 105.76ms | 90.56ms | 0.9× | 11.9% | PASS |
| file_writes | `oltp_delete_insert` | 97.16ms | 116.73ms | 1.2× | 1.5% | PASS |
| file_writes | `oltp_write_only` | 92.77ms | 66.07ms | 0.7× | 10.4% | PASS |
| file_writes | `types_delete_insert` | 72.59ms | 67.91ms | 0.9× | 1.5% | PASS |
| file_writes | `oltp_read_write` | 147.41ms | 145.09ms | 1.0× | 4.8% | PASS |
| ac_reads | `oltp_point_select` | 58.15ms | 54.12ms | 0.9× | 1.4% | PASS |
| ac_reads | `oltp_range_select` | 20.38ms | 18.88ms | 0.9× | 1.8% | PASS |
| ac_reads | `oltp_sum_range` | 18.03ms | 17.62ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_order_range` | 3.71ms | 3.82ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_distinct_range` | 4.84ms | 4.94ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_index_scan` | 6.43ms | 8.20ms | 1.3× | 2.0% | PASS |
| ac_reads | `select_random_points` | 26.31ms | 24.16ms | 0.9× | 2.5% | PASS |
| ac_reads | `select_random_ranges` | 9.27ms | 8.54ms | 0.9× | 1.6% | PASS |
| ac_reads | `covering_index_scan` | 10.35ms | 13.52ms | 1.3× | 1.8% | PASS |
| ac_reads | `groupby_scan` | 35.14ms | 36.44ms | 1.0× | 0.7% | PASS |
| ac_reads | `index_join` | 12.84ms | 11.56ms | 0.9× | 2.9% | PASS |
| ac_reads | `index_join_scan` | 4.30ms | 6.79ms | 1.6× | 1.5% | PASS |
| ac_reads | `types_table_scan` | 1.12s | 1.51s | 1.4× | 3.2% | PASS |
| ac_reads | `table_scan` | 1.24s | 1.63s | 1.3× | 2.3% | PASS |
| ac_reads | `oltp_read_only` | 177.28ms | 173.70ms | 1.0× | 1.5% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.32ms | 32.84ms | 1.5× | 5.7% | PASS |
| ac_writes | `oltp_insert_ac` | 24.01ms | 42.60ms | 1.8× | 4.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 25.77ms | 65.04ms | 2.5× | 3.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.21ms | 40.87ms | 1.9× | 5.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.34ms | 54.36ms | 2.1× | 5.8% | PASS |
| ac_writes | `oltp_write_only_ac` | 24.94ms | 52.22ms | 2.1× | 5.0% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.38ms | 41.02ms | 1.8× | 3.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 30.72ms | 60.56ms | 2.0× | 5.0% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 27.20ms | 28.08ms | 1.0× | 1.5% | PASS |
| mem_reads | `oltp_range_select` | 12.88ms | 12.09ms | 0.9× | 1.0% | PASS |
| mem_reads | `oltp_sum_range` | 11.83ms | 11.48ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_order_range` | 2.72ms | 2.79ms | 1.0× | 0.8% | PASS |
| mem_reads | `oltp_distinct_range` | 3.59ms | 3.66ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 3.22ms | 5.14ms | 1.6× | 1.2% | PASS |
| mem_reads | `select_random_points` | 17.16ms | 15.93ms | 0.9× | 2.0% | PASS |
| mem_reads | `select_random_ranges` | 5.29ms | 4.95ms | 0.9× | 1.3% | PASS |
| mem_reads | `covering_index_scan` | 6.01ms | 8.19ms | 1.4× | 1.3% | PASS |
| mem_reads | `groupby_scan` | 28.69ms | 28.90ms | 1.0× | 1.0% | PASS |
| mem_reads | `index_join` | 8.44ms | 8.33ms | 1.0× | 1.4% | PASS |
| mem_reads | `index_join_scan` | 2.99ms | 5.33ms | 1.8× | 1.4% | PASS |
| mem_reads | `types_table_scan` | 875.52ms | 1.12s | 1.3× | 1.0% | PASS |
| mem_reads | `table_scan` | 958.10ms | 1.22s | 1.3× | 0.4% | PASS |
| mem_reads | `oltp_read_only` | 108.43ms | 107.07ms | 1.0× | 1.9% | PASS |
| mem_writes | `oltp_bulk_insert` | 187.77ms | 255.09ms | 1.4× | 1.2% | PASS |
| mem_writes | `oltp_insert` | 14.66ms | 26.90ms | 1.8× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 50.64ms | 99.40ms | 2.0× | 1.4% | PASS |
| mem_writes | `oltp_update_non_index` | 38.73ms | 56.06ms | 1.4× | 1.2% | PASS |
| mem_writes | `oltp_delete_insert` | 41.89ms | 72.76ms | 1.7× | 0.8% | PASS |
| mem_writes | `oltp_write_only` | 22.54ms | 39.33ms | 1.7× | 1.0% | PASS |
| mem_writes | `types_delete_insert` | 30.10ms | 42.87ms | 1.4× | 1.3% | PASS |
| mem_writes | `oltp_read_write` | 72.31ms | 94.46ms | 1.3× | 0.9% | PASS |
| file_reads | `oltp_point_select` | 88.25ms | 43.96ms | 0.5× | 0.7% | PASS |
| file_reads | `oltp_range_select` | 19.70ms | 13.65ms | 0.7× | 1.0% | PASS |
| file_reads | `oltp_sum_range` | 18.63ms | 13.06ms | 0.7× | 0.8% | PASS |
| file_reads | `oltp_order_range` | 3.49ms | 2.99ms | 0.9× | 0.7% | PASS |
| file_reads | `oltp_distinct_range` | 4.30ms | 3.83ms | 0.9× | 0.6% | PASS |
| file_reads | `oltp_index_scan` | 9.57ms | 6.77ms | 0.7× | 0.8% | PASS |
| file_reads | `select_random_points` | 23.86ms | 17.54ms | 0.7× | 1.2% | PASS |
| file_reads | `select_random_ranges` | 11.74ms | 6.65ms | 0.6× | 0.9% | PASS |
| file_reads | `covering_index_scan` | 12.48ms | 9.93ms | 0.8× | 0.7% | PASS |
| file_reads | `groupby_scan` | 29.66ms | 29.19ms | 1.0× | 0.9% | PASS |
| file_reads | `index_join` | 12.54ms | 9.43ms | 0.8× | 1.6% | PASS |
| file_reads | `index_join_scan` | 3.90ms | 5.58ms | 1.4× | 1.7% | PASS |
| file_reads | `types_table_scan` | 859.37ms | 1.10s | 1.3× | 0.5% | PASS |
| file_reads | `table_scan` | 962.34ms | 1.22s | 1.3× | 0.5% | PASS |
| file_reads | `oltp_read_only` | 194.44ms | 129.22ms | 0.7× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 257.66ms | 322.48ms | 1.3× | 3.2% | PASS |
| file_writes | `oltp_insert` | 32.94ms | 48.81ms | 1.5× | 7.0% | PASS |
| file_writes | `oltp_update_index` | 170.79ms | 189.55ms | 1.1× | 9.6% | PASS |
| file_writes | `oltp_update_non_index` | 136.96ms | 113.00ms | 0.8× | 6.3% | PASS |
| file_writes | `oltp_delete_insert` | 162.79ms | 142.12ms | 0.9× | 9.4% | PASS |
| file_writes | `oltp_write_only` | 116.57ms | 94.62ms | 0.8× | 9.6% | PASS |
| file_writes | `types_delete_insert` | 121.32ms | 82.87ms | 0.7× | 6.1% | PASS |
| file_writes | `oltp_read_write` | 170.55ms | 150.29ms | 0.9× | 3.4% | PASS |
| ac_reads | `oltp_point_select` | 48.94ms | 44.33ms | 0.9× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 16.13ms | 13.89ms | 0.9× | 2.4% | PASS |
| ac_reads | `oltp_sum_range` | 15.22ms | 13.34ms | 0.9× | 2.3% | PASS |
| ac_reads | `oltp_order_range` | 3.13ms | 3.00ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_distinct_range` | 3.91ms | 3.84ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_index_scan` | 5.65ms | 6.72ms | 1.2× | 1.2% | PASS |
| ac_reads | `select_random_points` | 19.59ms | 17.44ms | 0.9× | 1.5% | PASS |
| ac_reads | `select_random_ranges` | 7.69ms | 6.61ms | 0.9× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 8.50ms | 9.93ms | 1.2× | 0.9% | PASS |
| ac_reads | `groupby_scan` | 29.13ms | 29.11ms | 1.0× | 0.6% | PASS |
| ac_reads | `index_join` | 10.48ms | 9.37ms | 0.9× | 1.4% | PASS |
| ac_reads | `index_join_scan` | 3.56ms | 5.55ms | 1.6× | 1.8% | PASS |
| ac_reads | `types_table_scan` | 861.87ms | 1.11s | 1.3× | 0.6% | PASS |
| ac_reads | `table_scan` | 976.02ms | 1.23s | 1.3× | 2.5% | PASS |
| ac_reads | `oltp_read_only` | 148.18ms | 134.75ms | 0.9× | 2.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.30ms | 32.30ms | 1.5× | 53.0% | PASS |
| ac_writes | `oltp_insert_ac` | 23.56ms | 53.99ms | 2.3× | 27.7% | PASS |
| ac_writes | `oltp_update_index_ac` | 23.90ms | 82.28ms | 3.4× | 28.7% | FAIL |
| ac_writes | `oltp_update_non_index_ac` | 18.02ms | 39.71ms | 2.2× | 9.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 21.88ms | 54.80ms | 2.5× | 28.2% | PASS |
| ac_writes | `oltp_write_only_ac` | 22.01ms | 62.20ms | 2.8× | 22.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 18.84ms | 41.49ms | 2.2× | 19.6% | PASS |
| ac_writes | `oltp_read_write_ac` | 26.64ms | 68.16ms | 2.6× | 27.0% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 28.48ms | 31.31ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_range_select` | 17.19ms | 20.63ms | 1.2× | 1.0% | PASS |
| mem_reads | `oltp_sum_range` | 16.17ms | 18.95ms | 1.2× | 0.8% | PASS |
| mem_reads | `oltp_order_range` | 3.34ms | 3.72ms | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_distinct_range` | 4.29ms | 4.79ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_index_scan` | 4.03ms | 5.14ms | 1.3× | 1.4% | PASS |
| mem_reads | `select_random_points` | 25.85ms | 26.47ms | 1.0× | 0.8% | PASS |
| mem_reads | `select_random_ranges` | 6.36ms | 7.29ms | 1.1× | 1.2% | PASS |
| mem_reads | `covering_index_scan` | 6.45ms | 9.20ms | 1.4× | 1.5% | PASS |
| mem_reads | `groupby_scan` | 34.97ms | 38.12ms | 1.1× | 0.8% | PASS |
| mem_reads | `index_join` | 7.07ms | 9.85ms | 1.4× | 1.5% | PASS |
| mem_reads | `index_join_scan` | 3.42ms | 5.66ms | 1.7× | 1.9% | PASS |
| mem_reads | `types_table_scan` | 983.49ms | 1.25s | 1.3× | 0.2% | PASS |
| mem_reads | `table_scan` | 1.13s | 1.37s | 1.2× | 0.4% | PASS |
| mem_reads | `oltp_read_only` | 130.44ms | 151.34ms | 1.2× | 0.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 205.73ms | 262.42ms | 1.3× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 16.21ms | 26.49ms | 1.6× | 1.1% | PASS |
| mem_writes | `oltp_update_index` | 55.90ms | 103.02ms | 1.8× | 0.8% | PASS |
| mem_writes | `oltp_update_non_index` | 41.27ms | 67.42ms | 1.6× | 1.1% | PASS |
| mem_writes | `oltp_delete_insert` | 41.70ms | 76.07ms | 1.8× | 0.9% | PASS |
| mem_writes | `oltp_write_only` | 22.48ms | 46.37ms | 2.1× | 1.2% | PASS |
| mem_writes | `types_delete_insert` | 26.49ms | 43.77ms | 1.7× | 0.8% | PASS |
| mem_writes | `oltp_read_write` | 82.28ms | 124.02ms | 1.5× | 0.6% | PASS |
| file_reads | `oltp_point_select` | 54.23ms | 37.12ms | 0.7× | 0.7% | PASS |
| file_reads | `oltp_range_select` | 20.22ms | 21.39ms | 1.1× | 0.9% | PASS |
| file_reads | `oltp_sum_range` | 19.19ms | 19.81ms | 1.0× | 0.8% | PASS |
| file_reads | `oltp_order_range` | 3.75ms | 3.90ms | 1.0× | 1.7% | PASS |
| file_reads | `oltp_distinct_range` | 4.69ms | 4.96ms | 1.1× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 7.08ms | 6.30ms | 0.9× | 1.3% | PASS |
| file_reads | `select_random_points` | 29.40ms | 27.58ms | 0.9× | 0.9% | PASS |
| file_reads | `select_random_ranges` | 9.47ms | 8.15ms | 0.9× | 0.9% | PASS |
| file_reads | `covering_index_scan` | 9.60ms | 10.38ms | 1.1× | 1.2% | PASS |
| file_reads | `groupby_scan` | 36.09ms | 38.93ms | 1.1× | 0.8% | PASS |
| file_reads | `index_join` | 9.06ms | 10.96ms | 1.2× | 1.0% | PASS |
| file_reads | `index_join_scan` | 3.93ms | 6.10ms | 1.6× | 1.6% | PASS |
| file_reads | `types_table_scan` | 1.02s | 1.27s | 1.3× | 0.8% | PASS |
| file_reads | `table_scan` | 1.18s | 1.40s | 1.2× | 1.0% | PASS |
| file_reads | `oltp_read_only` | 176.04ms | 163.89ms | 0.9× | 0.7% | PASS |
| file_writes | `oltp_bulk_insert` | 259.83ms | 324.64ms | 1.2× | 2.7% | PASS |
| file_writes | `oltp_insert` | 30.30ms | 40.44ms | 1.3× | 2.2% | PASS |
| file_writes | `oltp_update_index` | 164.38ms | 174.92ms | 1.1× | 1.5% | PASS |
| file_writes | `oltp_update_non_index` | 133.10ms | 119.63ms | 0.9× | 2.2% | PASS |
| file_writes | `oltp_delete_insert` | 130.82ms | 131.16ms | 1.0× | 2.0% | PASS |
| file_writes | `oltp_write_only` | 91.35ms | 90.64ms | 1.0× | 1.6% | PASS |
| file_writes | `types_delete_insert` | 90.80ms | 78.34ms | 0.9× | 7.9% | PASS |
| file_writes | `oltp_read_write` | 162.05ms | 171.29ms | 1.1× | 1.6% | PASS |
| ac_reads | `oltp_point_select` | 37.21ms | 37.92ms | 1.0× | 0.6% | PASS |
| ac_reads | `oltp_range_select` | 18.68ms | 21.84ms | 1.2× | 0.9% | PASS |
| ac_reads | `oltp_sum_range` | 17.74ms | 20.19ms | 1.1× | 0.7% | PASS |
| ac_reads | `oltp_order_range` | 3.63ms | 3.98ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_distinct_range` | 4.57ms | 5.02ms | 1.1× | 0.9% | PASS |
| ac_reads | `oltp_index_scan` | 5.43ms | 6.32ms | 1.2× | 1.1% | PASS |
| ac_reads | `select_random_points` | 28.13ms | 28.13ms | 1.0× | 1.0% | PASS |
| ac_reads | `select_random_ranges` | 7.66ms | 8.22ms | 1.1× | 1.2% | PASS |
| ac_reads | `covering_index_scan` | 7.73ms | 10.37ms | 1.3× | 1.1% | PASS |
| ac_reads | `groupby_scan` | 35.61ms | 38.71ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 8.03ms | 10.82ms | 1.3× | 1.5% | PASS |
| ac_reads | `index_join_scan` | 3.73ms | 5.95ms | 1.6× | 1.5% | PASS |
| ac_reads | `types_table_scan` | 984.05ms | 1.24s | 1.3× | 0.3% | PASS |
| ac_reads | `table_scan` | 1.14s | 1.37s | 1.2× | 0.7% | PASS |
| ac_reads | `oltp_read_only` | 151.82ms | 165.91ms | 1.1× | 0.6% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 30.28ms | 40.79ms | 1.3× | 4.1% | PASS |
| ac_writes | `oltp_insert_ac` | 32.86ms | 53.15ms | 1.6× | 3.7% | PASS |
| ac_writes | `oltp_update_index_ac` | 33.93ms | 65.56ms | 1.9× | 2.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 30.55ms | 46.70ms | 1.5× | 3.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 31.78ms | 55.02ms | 1.7× | 2.5% | PASS |
| ac_writes | `oltp_write_only_ac` | 31.79ms | 54.34ms | 1.7× | 3.4% | PASS |
| ac_writes | `types_delete_insert_ac` | 29.29ms | 42.90ms | 1.5× | 4.1% | PASS |
| ac_writes | `oltp_read_write_ac` | 35.48ms | 61.32ms | 1.7× | 2.8% | PASS |

</details>

</details>

## Version-control latency

Wall time: 5m 44s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 22.92ms | 38.00ms | 60.3% | 0.9% | PASS |
| `status_dirty_many_tables` | 25.20ms | 42.00ms | 60.0% | 1.1% | PASS |
| `diff_regular_working_one_table` | 20.22ms | 33.00ms | 61.3% | 0.9% | PASS |
| `diff_regular_working_many_tables` | 27.57ms | 50.00ms | 55.1% | 1.2% | PASS |
| `diff_stat_working_many_tables` | 27.24ms | 48.00ms | 56.8% | 1.5% | PASS |
| `diff_schema_working_many_tables` | 27.23ms | 48.00ms | 56.7% | 1.7% | PASS |
| `branch_list_many_branches` | 14.90ms | 25.00ms | 59.6% | 0.8% | PASS |
| `branch_create_delete` | 17.00ms | 27.00ms | 63.0% | 0.9% | PASS |
| `at_literal_deep_history` | 16.14ms | 28.00ms | 57.6% | 0.9% | PASS |
| `diff_literal_deep_history` | 16.56ms | 28.00ms | 59.2% | 2.1% | PASS |
| `history_literal_deep_history` | 16.93ms | 30.00ms | 56.4% | 1.0% | PASS |
| `checkout_branch_clean` | 21.56ms | 43.00ms | 50.1% | 1.6% | PASS |
| `merge_data_no_conflicts` | 19.14ms | 33.00ms | 58.0% | 2.3% | PASS |
| `merge_data_secondary_index` | 633.08ms | 944.00ms | 67.1% | 1.5% | PASS |
| `merge_schema_no_conflicts` | 14.91ms | 24.00ms | 62.1% | 1.5% | PASS |
| `merge_data_conflicts` | 21.08ms | 33.00ms | 63.9% | 1.5% | PASS |
| `merge_data_conflicts_with_resolve` | 22.00ms | 34.00ms | 64.7% | 1.2% | PASS |

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

candidate: cf88124.

Fixture file bytes: base 107,968,301; delta 115,879,032.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 402.303 | 397.995–548.578 | 31.362 | 248.57 | 107968301 |
| push_initial | candidate | 734.506 | 730.530–739.906 | 367.936 | 136.15 | 107956807 |
| clone_initial | candidate | 969.021 | 880.412–984.836 | 464.489 | 103.20 | 108475105 |
| push_noop | candidate | 0.537 | 0.503–0.556 | 0.230 | — | 0 |
| fetch_noop | candidate | 0.155 | 0.140–0.161 | 0.158 | — | 0 |
| push_incremental | candidate | 169.090 | 161.590–170.743 | 146.032 | 5.91 | 7909775 |
| fetch_incremental | candidate | 81.988 | 78.038–85.872 | 64.254 | 12.20 | 7910084 |
| pull_fast_forward | candidate | 73.460 | 71.844–77.234 | 55.564 | 13.61 | 7910844 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 90.53 | 175.487 | 108.763 | 18329 | 2066 |
| clone_initial | candidate | 90.96 | 295.002 | 110.833 | 47223 | 2353 |
| push_noop | candidate | 41.34 | 0.073 | 0.000 | 27 | 2 |
| fetch_noop | candidate | 41.34 | 0.135 | 0.000 | 42 | 0 |
| push_incremental | candidate | 41.34 | 96.736 | 7.910 | 22380 | 125 |
| fetch_incremental | candidate | 41.34 | 34.472 | 7.910 | 10810 | 124 |
| pull_fast_forward | candidate | 41.34 | 26.497 | 8.959 | 7032 | 142 |

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

candidate: cf88124.

Fixture file bytes: base 1,087,718,066; delta 1,103,525,194.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 4151.688 | 4149.615–4158.236 | 573.334 | 240.87 | 1087718066 |
| push_initial | candidate | 6330.262 | 6327.893–6390.622 | 3668.585 | 157.97 | 1079566310 |
| clone_initial | candidate | 5332.201 | 5303.971–5400.563 | 5283.045 | 187.54 | 1102407396 |
| push_noop | candidate | 0.463 | 0.391–0.482 | 0.206 | — | 0 |
| fetch_noop | candidate | 0.093 | 0.090–0.096 | 0.094 | — | 0 |
| push_incremental | candidate | 773.343 | 771.572–795.884 | 770.145 | 1.29 | 15806003 |
| fetch_incremental | candidate | 191.045 | 187.470–194.301 | 187.385 | 5.23 | 15806513 |
| pull_fast_forward | candidate | 199.644 | 196.376–206.994 | 195.045 | 5.01 | 15807273 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 134.73 | 2092.060 | 1087.622 | 263542 | 20541 |
| clone_initial | candidate | 124.04 | 3517.584 | 1133.868 | 612289 | 32259 |
| push_noop | candidate | 43.90 | 0.078 | 0.000 | 28 | 2 |
| fetch_noop | candidate | 43.90 | 0.137 | 0.000 | 42 | 0 |
| push_incremental | candidate | 44.89 | 616.368 | 15.928 | 149724 | 313 |
| fetch_incremental | candidate | 50.41 | 62.762 | 15.928 | 14026 | 313 |
| pull_fast_forward | candidate | 50.19 | 62.695 | 16.978 | 14021 | 331 |

</details>


</details>

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh`, `test/vc_perf_ceiling.sh`, and `test/remote_perf.py`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
