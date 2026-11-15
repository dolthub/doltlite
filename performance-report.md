# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-11-15 11:24 UTC
>
> Commit: [`ea46cdf6517c6e3bb1f7485d02a457227ef99c28`](https://github.com/dolthub/doltlite/commit/ea46cdf6517c6e3bb1f7485d02a457227ef99c28)
>
> Runner: ubuntu24 20261111.127.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/43699731421)

The SQL workloads compare optimized DoltLite against stock SQLite on the same GitHub-hosted runner, alternating execution order on each repetition. Version-control sections measure DoltLite latency. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 7.94s | 9.35s | 1.18× | 1.1% | **PASS** |
| Writes | 1.55s | 2.39s | 1.54× | 1.1% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 8.18s | 9.33s | 1.14× | 1.2% | **PASS** |
| Writes | 3.16s | 3.45s | 1.09× | 2.5% | **PASS** |
| Autocommit writes | 688.91ms | 1.14s | 1.65× | 2.8% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.8× for a section average. Durable autocommit writes use 3.6× and 2.6× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 1.80s | 2.03s | 1.13× | 1.4% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.49s | 2.96s | 1.19× | 0.9% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 1.77s | 2.10s | 1.19× | 2.7% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 1.89s | 2.26s | 1.20× | 0.9% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 289.82ms | 463.38ms | 1.60× | 1.2% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 507.94ms | 779.86ms | 1.54× | 0.9% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 368.11ms | 567.04ms | 1.54× | 1.8% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 384.77ms | 582.28ms | 1.51× | 1.0% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 1.83s | 2.02s | 1.10× | 1.2% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.59s | 2.99s | 1.15× | 0.9% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 1.79s | 2.06s | 1.15× | 1.8% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 1.96s | 2.27s | 1.15× | 1.1% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 358.13ms | 488.26ms | 1.36× | 1.4% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.35s | 1.31s | 0.97× | 3.6% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 977.03ms | 1.02s | 1.05× | 5.3% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 468.88ms | 630.82ms | 1.35× | 1.6% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 1.80s | 2.02s | 1.12× | 1.4% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.56s | 2.99s | 1.17× | 0.8% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 1.90s | 2.23s | 1.17× | 2.2% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 1.93s | 2.26s | 1.17× | 1.2% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 75.69ms | 160.53ms | 2.12× | 2.6% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 251.25ms | 405.86ms | 1.62× | 2.6% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 280.62ms | 402.74ms | 1.44× | 3.6% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 81.34ms | 167.64ms | 2.06× | 2.4% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 16.10ms | 17.86ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_range_select` | 8.01ms | 8.71ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_sum_range` | 7.32ms | 8.17ms | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_order_range` | 2.00ms | 2.14ms | 1.1× | 1.8% | PASS |
| mem_reads | `oltp_distinct_range` | 2.64ms | 2.87ms | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_index_scan` | 2.81ms | 3.51ms | 1.2× | 1.8% | PASS |
| mem_reads | `select_random_points` | 10.60ms | 9.51ms | 0.9× | 1.4% | PASS |
| mem_reads | `select_random_ranges` | 3.16ms | 3.23ms | 1.0× | 1.6% | PASS |
| mem_reads | `covering_index_scan` | 4.79ms | 6.64ms | 1.4× | 0.5% | PASS |
| mem_reads | `groupby_scan` | 21.25ms | 23.34ms | 1.1× | 0.7% | PASS |
| mem_reads | `index_join` | 4.25ms | 6.19ms | 1.5× | 1.8% | PASS |
| mem_reads | `index_join_scan` | 2.41ms | 4.38ms | 1.8× | 3.4% | PASS |
| mem_reads | `types_table_scan` | 761.77ms | 870.41ms | 1.1× | 0.4% | PASS |
| mem_reads | `table_scan` | 874.81ms | 976.67ms | 1.1× | 1.8% | PASS |
| mem_reads | `oltp_read_only` | 76.35ms | 83.76ms | 1.1× | 1.7% | PASS |
| mem_writes | `oltp_bulk_insert` | 109.15ms | 166.63ms | 1.5× | 0.5% | PASS |
| mem_writes | `oltp_insert` | 9.82ms | 15.96ms | 1.6× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 36.08ms | 67.42ms | 1.9× | 1.7% | PASS |
| mem_writes | `oltp_update_non_index` | 23.93ms | 39.65ms | 1.7× | 1.4% | PASS |
| mem_writes | `oltp_delete_insert` | 32.55ms | 50.22ms | 1.5× | 1.1% | PASS |
| mem_writes | `oltp_write_only` | 15.74ms | 30.97ms | 2.0× | 1.6% | PASS |
| mem_writes | `types_delete_insert` | 16.84ms | 21.89ms | 1.3× | 1.3% | PASS |
| mem_writes | `oltp_read_write` | 45.71ms | 70.64ms | 1.5× | 1.1% | PASS |
| file_reads | `oltp_point_select` | 36.23ms | 22.31ms | 0.6× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 10.38ms | 9.42ms | 0.9× | 1.2% | PASS |
| file_reads | `oltp_sum_range` | 9.55ms | 9.13ms | 1.0× | 1.6% | PASS |
| file_reads | `oltp_order_range` | 2.23ms | 2.26ms | 1.0× | 2.1% | PASS |
| file_reads | `oltp_distinct_range` | 2.88ms | 2.98ms | 1.0× | 2.6% | PASS |
| file_reads | `oltp_index_scan` | 4.93ms | 4.21ms | 0.9× | 2.3% | PASS |
| file_reads | `select_random_points` | 12.83ms | 10.17ms | 0.8× | 1.4% | PASS |
| file_reads | `select_random_ranges` | 5.23ms | 3.79ms | 0.7× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 6.95ms | 7.27ms | 1.0× | 0.8% | PASS |
| file_reads | `groupby_scan` | 21.68ms | 23.46ms | 1.1× | 0.6% | PASS |
| file_reads | `index_join` | 5.47ms | 6.76ms | 1.2× | 2.4% | PASS |
| file_reads | `index_join_scan` | 2.67ms | 4.54ms | 1.7× | 2.8% | PASS |
| file_reads | `types_table_scan` | 762.75ms | 864.89ms | 1.1× | 0.3% | PASS |
| file_reads | `table_scan` | 850.79ms | 959.88ms | 1.1× | 0.5% | PASS |
| file_reads | `oltp_read_only` | 98.33ms | 85.82ms | 0.9× | 0.7% | PASS |
| file_writes | `oltp_bulk_insert` | 117.97ms | 172.19ms | 1.5× | 1.0% | PASS |
| file_writes | `oltp_insert` | 13.18ms | 18.09ms | 1.4× | 1.5% | PASS |
| file_writes | `oltp_update_index` | 48.20ms | 69.89ms | 1.4× | 1.6% | PASS |
| file_writes | `oltp_update_non_index` | 33.83ms | 42.06ms | 1.2× | 1.4% | PASS |
| file_writes | `oltp_delete_insert` | 40.94ms | 55.85ms | 1.4× | 1.7% | PASS |
| file_writes | `oltp_write_only` | 25.29ms | 33.18ms | 1.3× | 1.5% | PASS |
| file_writes | `types_delete_insert` | 23.45ms | 25.51ms | 1.1× | 1.3% | PASS |
| file_writes | `oltp_read_write` | 55.28ms | 71.50ms | 1.3× | 1.3% | PASS |
| ac_reads | `oltp_point_select` | 22.18ms | 21.85ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 8.76ms | 9.17ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_sum_range` | 7.91ms | 8.85ms | 1.1× | 1.8% | PASS |
| ac_reads | `oltp_order_range` | 2.09ms | 2.19ms | 1.1× | 2.1% | PASS |
| ac_reads | `oltp_distinct_range` | 2.75ms | 2.91ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_index_scan` | 3.56ms | 3.94ms | 1.1× | 1.7% | PASS |
| ac_reads | `select_random_points` | 11.30ms | 9.81ms | 0.9× | 1.6% | PASS |
| ac_reads | `select_random_ranges` | 3.87ms | 3.69ms | 1.0× | 1.6% | PASS |
| ac_reads | `covering_index_scan` | 5.48ms | 7.04ms | 1.3× | 0.4% | PASS |
| ac_reads | `groupby_scan` | 21.75ms | 23.01ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 4.77ms | 6.37ms | 1.3× | 1.5% | PASS |
| ac_reads | `index_join_scan` | 2.56ms | 4.36ms | 1.7× | 3.2% | PASS |
| ac_reads | `types_table_scan` | 758.52ms | 861.36ms | 1.1× | 0.2% | PASS |
| ac_reads | `table_scan` | 862.30ms | 967.21ms | 1.1× | 0.7% | PASS |
| ac_reads | `oltp_read_only` | 81.62ms | 87.05ms | 1.1× | 0.6% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 8.37ms | 13.09ms | 1.6× | 2.4% | PASS |
| ac_writes | `oltp_insert_ac` | 9.40ms | 18.98ms | 2.0× | 2.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 10.30ms | 27.47ms | 2.7× | 2.0% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 8.61ms | 16.37ms | 1.9× | 2.7% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 9.37ms | 21.81ms | 2.3× | 3.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 9.72ms | 21.92ms | 2.3× | 3.7% | PASS |
| ac_writes | `types_delete_insert_ac` | 8.36ms | 16.39ms | 2.0× | 2.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 11.56ms | 24.50ms | 2.1× | 2.0% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 32.04ms | 31.92ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_range_select` | 14.39ms | 14.84ms | 1.0× | 0.8% | PASS |
| mem_reads | `oltp_sum_range` | 13.99ms | 13.60ms | 1.0× | 1.0% | PASS |
| mem_reads | `oltp_order_range` | 3.07ms | 3.27ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_distinct_range` | 3.97ms | 4.30ms | 1.1× | 0.6% | PASS |
| mem_reads | `oltp_index_scan` | 3.66ms | 5.54ms | 1.5× | 1.1% | PASS |
| mem_reads | `select_random_points` | 21.07ms | 19.78ms | 0.9× | 0.9% | PASS |
| mem_reads | `select_random_ranges` | 5.79ms | 5.58ms | 1.0× | 1.2% | PASS |
| mem_reads | `covering_index_scan` | 6.57ms | 9.59ms | 1.5× | 0.7% | PASS |
| mem_reads | `groupby_scan` | 33.74ms | 33.78ms | 1.0× | 0.4% | PASS |
| mem_reads | `index_join` | 10.57ms | 9.38ms | 0.9× | 1.1% | PASS |
| mem_reads | `index_join_scan` | 3.27ms | 6.15ms | 1.9× | 1.1% | PASS |
| mem_reads | `types_table_scan` | 1.03s | 1.28s | 1.2× | 0.6% | PASS |
| mem_reads | `table_scan` | 1.18s | 1.40s | 1.2× | 0.8% | PASS |
| mem_reads | `oltp_read_only` | 126.18ms | 131.24ms | 1.0× | 0.7% | PASS |
| mem_writes | `oltp_bulk_insert` | 196.98ms | 268.14ms | 1.4× | 0.4% | PASS |
| mem_writes | `oltp_insert` | 15.52ms | 29.19ms | 1.9× | 0.6% | PASS |
| mem_writes | `oltp_update_index` | 58.53ms | 120.16ms | 2.1× | 1.2% | PASS |
| mem_writes | `oltp_update_non_index` | 42.95ms | 64.81ms | 1.5× | 1.1% | PASS |
| mem_writes | `oltp_delete_insert` | 48.71ms | 86.86ms | 1.8× | 0.8% | PASS |
| mem_writes | `oltp_write_only` | 24.72ms | 44.92ms | 1.8× | 0.8% | PASS |
| mem_writes | `types_delete_insert` | 34.69ms | 50.10ms | 1.4× | 1.0% | PASS |
| mem_writes | `oltp_read_write` | 85.85ms | 115.68ms | 1.3× | 1.1% | PASS |
| file_reads | `oltp_point_select` | 60.17ms | 38.11ms | 0.6× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 17.56ms | 15.71ms | 0.9× | 0.8% | PASS |
| file_reads | `oltp_sum_range` | 17.22ms | 14.55ms | 0.8× | 0.8% | PASS |
| file_reads | `oltp_order_range` | 3.47ms | 3.45ms | 1.0× | 0.9% | PASS |
| file_reads | `oltp_distinct_range` | 4.42ms | 4.47ms | 1.0× | 0.9% | PASS |
| file_reads | `oltp_index_scan` | 6.76ms | 6.42ms | 0.9× | 0.6% | PASS |
| file_reads | `select_random_points` | 25.14ms | 20.93ms | 0.8× | 0.9% | PASS |
| file_reads | `select_random_ranges` | 8.99ms | 6.39ms | 0.7× | 0.9% | PASS |
| file_reads | `covering_index_scan` | 9.67ms | 10.47ms | 1.1× | 0.7% | PASS |
| file_reads | `groupby_scan` | 34.35ms | 34.05ms | 1.0× | 0.6% | PASS |
| file_reads | `index_join` | 12.71ms | 10.13ms | 0.8× | 1.1% | PASS |
| file_reads | `index_join_scan` | 3.77ms | 6.46ms | 1.7× | 1.5% | PASS |
| file_reads | `types_table_scan` | 1.03s | 1.28s | 1.2× | 0.4% | PASS |
| file_reads | `table_scan` | 1.19s | 1.40s | 1.2× | 0.8% | PASS |
| file_reads | `oltp_read_only` | 167.31ms | 140.49ms | 0.8× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 281.02ms | 351.48ms | 1.3× | 2.4% | PASS |
| file_writes | `oltp_insert` | 30.42ms | 53.89ms | 1.8× | 2.7% | PASS |
| file_writes | `oltp_update_index` | 189.18ms | 214.98ms | 1.1× | 3.2% | PASS |
| file_writes | `oltp_update_non_index` | 171.77ms | 135.14ms | 0.8× | 4.9% | PASS |
| file_writes | `oltp_delete_insert` | 189.84ms | 167.89ms | 0.9× | 4.4% | PASS |
| file_writes | `oltp_write_only` | 142.37ms | 105.21ms | 0.7× | 2.8% | PASS |
| file_writes | `types_delete_insert` | 153.85ms | 97.34ms | 0.6× | 6.3% | PASS |
| file_writes | `oltp_read_write` | 195.53ms | 180.74ms | 0.9× | 4.0% | PASS |
| ac_reads | `oltp_point_select` | 41.27ms | 38.07ms | 0.9× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 15.58ms | 15.68ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_sum_range` | 15.42ms | 14.48ms | 0.9× | 0.8% | PASS |
| ac_reads | `oltp_order_range` | 3.28ms | 3.44ms | 1.1× | 0.8% | PASS |
| ac_reads | `oltp_distinct_range` | 4.21ms | 4.45ms | 1.1× | 0.8% | PASS |
| ac_reads | `oltp_index_scan` | 4.98ms | 6.42ms | 1.3× | 0.8% | PASS |
| ac_reads | `select_random_points` | 23.43ms | 20.90ms | 0.9× | 0.8% | PASS |
| ac_reads | `select_random_ranges` | 7.20ms | 6.39ms | 0.9× | 0.9% | PASS |
| ac_reads | `covering_index_scan` | 7.78ms | 10.50ms | 1.4× | 0.7% | PASS |
| ac_reads | `groupby_scan` | 34.14ms | 34.12ms | 1.0× | 0.4% | PASS |
| ac_reads | `index_join` | 11.86ms | 10.17ms | 0.9× | 1.0% | PASS |
| ac_reads | `index_join_scan` | 3.62ms | 6.48ms | 1.8× | 1.1% | PASS |
| ac_reads | `types_table_scan` | 1.03s | 1.28s | 1.2× | 0.7% | PASS |
| ac_reads | `table_scan` | 1.21s | 1.40s | 1.2× | 1.2% | PASS |
| ac_reads | `oltp_read_only` | 140.55ms | 140.61ms | 1.0× | 0.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 29.75ms | 34.88ms | 1.2× | 2.3% | PASS |
| ac_writes | `oltp_insert_ac` | 31.56ms | 45.25ms | 1.4× | 2.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 32.10ms | 68.80ms | 2.1× | 4.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 28.79ms | 41.66ms | 1.4× | 2.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 31.26ms | 55.92ms | 1.8× | 3.8% | PASS |
| ac_writes | `oltp_write_only_ac` | 32.49ms | 53.70ms | 1.7× | 2.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 29.84ms | 45.85ms | 1.5× | 4.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 35.46ms | 59.79ms | 1.7× | 2.6% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 26.57ms | 25.72ms | 1.0× | 1.6% | PASS |
| mem_reads | `oltp_range_select` | 12.10ms | 12.14ms | 1.0× | 2.6% | PASS |
| mem_reads | `oltp_sum_range` | 12.31ms | 11.74ms | 1.0× | 1.7% | PASS |
| mem_reads | `oltp_order_range` | 2.54ms | 2.70ms | 1.1× | 2.9% | PASS |
| mem_reads | `oltp_distinct_range` | 3.17ms | 3.37ms | 1.1× | 3.6% | PASS |
| mem_reads | `oltp_index_scan` | 3.06ms | 4.93ms | 1.6× | 2.9% | PASS |
| mem_reads | `select_random_points` | 18.10ms | 16.94ms | 0.9× | 3.0% | PASS |
| mem_reads | `select_random_ranges` | 4.74ms | 4.42ms | 0.9× | 3.1% | PASS |
| mem_reads | `covering_index_scan` | 4.73ms | 7.34ms | 1.6× | 2.6% | PASS |
| mem_reads | `groupby_scan` | 23.64ms | 25.26ms | 1.1× | 2.2% | PASS |
| mem_reads | `index_join` | 9.87ms | 8.73ms | 0.9× | 2.7% | PASS |
| mem_reads | `index_join_scan` | 2.71ms | 5.64ms | 2.1× | 3.9% | PASS |
| mem_reads | `types_table_scan` | 731.55ms | 899.09ms | 1.2× | 2.0% | PASS |
| mem_reads | `table_scan` | 824.85ms | 981.40ms | 1.2× | 3.1% | PASS |
| mem_reads | `oltp_read_only` | 91.39ms | 93.77ms | 1.0× | 1.7% | PASS |
| mem_writes | `oltp_bulk_insert` | 136.97ms | 186.84ms | 1.4× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 11.16ms | 22.49ms | 2.0× | 3.1% | PASS |
| mem_writes | `oltp_update_index` | 44.90ms | 91.81ms | 2.0× | 1.8% | PASS |
| mem_writes | `oltp_update_non_index` | 32.49ms | 48.17ms | 1.5× | 1.7% | PASS |
| mem_writes | `oltp_delete_insert` | 36.22ms | 64.08ms | 1.8× | 2.1% | PASS |
| mem_writes | `oltp_write_only` | 19.67ms | 33.55ms | 1.7× | 2.4% | PASS |
| mem_writes | `types_delete_insert` | 25.27ms | 36.77ms | 1.5× | 1.7% | PASS |
| mem_writes | `oltp_read_write` | 61.41ms | 83.36ms | 1.4× | 1.7% | PASS |
| file_reads | `oltp_point_select` | 43.10ms | 27.41ms | 0.6× | 1.1% | PASS |
| file_reads | `oltp_range_select` | 13.30ms | 11.39ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_sum_range` | 13.18ms | 10.94ms | 0.8× | 2.9% | PASS |
| file_reads | `oltp_order_range` | 2.53ms | 2.50ms | 1.0× | 2.5% | PASS |
| file_reads | `oltp_distinct_range` | 3.16ms | 3.14ms | 1.0× | 1.8% | PASS |
| file_reads | `oltp_index_scan` | 4.86ms | 5.12ms | 1.1× | 2.8% | PASS |
| file_reads | `select_random_points` | 19.40ms | 16.24ms | 0.8× | 2.4% | PASS |
| file_reads | `select_random_ranges` | 6.50ms | 4.74ms | 0.7× | 2.4% | PASS |
| file_reads | `covering_index_scan` | 6.59ms | 7.23ms | 1.1× | 1.0% | PASS |
| file_reads | `groupby_scan` | 21.88ms | 23.16ms | 1.1× | 1.2% | PASS |
| file_reads | `index_join` | 10.28ms | 8.06ms | 0.8× | 3.4% | PASS |
| file_reads | `index_join_scan` | 2.91ms | 5.57ms | 1.9× | 5.1% | PASS |
| file_reads | `types_table_scan` | 713.13ms | 875.89ms | 1.2× | 1.2% | PASS |
| file_reads | `table_scan` | 815.34ms | 957.55ms | 1.2× | 1.8% | PASS |
| file_reads | `oltp_read_only` | 116.52ms | 96.50ms | 0.8× | 1.3% | PASS |
| file_writes | `oltp_bulk_insert` | 208.72ms | 260.67ms | 1.2× | 5.3% | PASS |
| file_writes | `oltp_insert` | 19.29ms | 46.07ms | 2.4× (paired 2.2×) | 8.5% | PASS |
| file_writes | `oltp_update_index` | 142.06ms | 176.30ms | 1.2× | 4.4% | PASS |
| file_writes | `oltp_update_non_index` | 121.01ms | 108.34ms | 0.9× | 4.3% | PASS |
| file_writes | `oltp_delete_insert` | 142.57ms | 130.54ms | 0.9× | 5.3% | PASS |
| file_writes | `oltp_write_only` | 94.55ms | 84.28ms | 0.9× | 6.6% | PASS |
| file_writes | `types_delete_insert` | 101.84ms | 74.73ms | 0.7× | 8.3% | PASS |
| file_writes | `oltp_read_write` | 146.99ms | 141.35ms | 1.0× | 3.9% | PASS |
| ac_reads | `oltp_point_select` | 34.51ms | 31.01ms | 0.9× | 1.5% | PASS |
| ac_reads | `oltp_range_select` | 13.26ms | 12.60ms | 1.0× | 2.1% | PASS |
| ac_reads | `oltp_sum_range` | 13.37ms | 12.23ms | 0.9× | 2.2% | PASS |
| ac_reads | `oltp_order_range` | 2.71ms | 2.81ms | 1.0× | 2.0% | PASS |
| ac_reads | `oltp_distinct_range` | 3.45ms | 3.60ms | 1.0× | 2.0% | PASS |
| ac_reads | `oltp_index_scan` | 4.19ms | 5.66ms | 1.4× | 2.2% | PASS |
| ac_reads | `select_random_points` | 20.69ms | 18.09ms | 0.9× | 2.2% | PASS |
| ac_reads | `select_random_ranges` | 6.15ms | 5.28ms | 0.9× | 3.5% | PASS |
| ac_reads | `covering_index_scan` | 6.09ms | 8.13ms | 1.3× | 2.1% | PASS |
| ac_reads | `groupby_scan` | 24.87ms | 26.34ms | 1.1× | 1.8% | PASS |
| ac_reads | `index_join` | 11.13ms | 9.18ms | 0.8× | 3.5% | PASS |
| ac_reads | `index_join_scan` | 3.01ms | 5.91ms | 2.0× | 5.1% | PASS |
| ac_reads | `types_table_scan` | 760.27ms | 940.92ms | 1.2× | 2.4% | PASS |
| ac_reads | `table_scan` | 888.55ms | 1.04s | 1.2× | 2.4% | PASS |
| ac_reads | `oltp_read_only` | 106.79ms | 106.91ms | 1.0× | 3.5% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 33.39ms | 36.36ms | 1.1× | 3.4% | PASS |
| ac_writes | `oltp_insert_ac` | 37.25ms | 51.59ms | 1.4× | 4.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 36.60ms | 64.42ms | 1.8× | 5.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 31.75ms | 42.64ms | 1.3× | 3.1% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 36.05ms | 53.32ms | 1.5× | 3.7% | PASS |
| ac_writes | `oltp_write_only_ac` | 34.73ms | 51.95ms | 1.5× | 2.5% | PASS |
| ac_writes | `types_delete_insert_ac` | 33.13ms | 45.66ms | 1.4× | 4.1% | PASS |
| ac_writes | `oltp_read_write_ac` | 37.72ms | 56.80ms | 1.5× | 3.5% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 22.34ms | 24.70ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_range_select` | 14.59ms | 15.92ms | 1.1× | 0.7% | PASS |
| mem_reads | `oltp_sum_range` | 13.94ms | 14.89ms | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_order_range` | 2.71ms | 2.93ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 3.40ms | 3.64ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 3.28ms | 4.33ms | 1.3× | 2.2% | PASS |
| mem_reads | `select_random_points` | 21.21ms | 21.85ms | 1.0× | 0.7% | PASS |
| mem_reads | `select_random_ranges` | 5.14ms | 5.86ms | 1.1× | 1.0% | PASS |
| mem_reads | `covering_index_scan` | 4.82ms | 6.86ms | 1.4× | 0.9% | PASS |
| mem_reads | `groupby_scan` | 25.15ms | 28.43ms | 1.1× | 0.4% | PASS |
| mem_reads | `index_join` | 5.80ms | 8.20ms | 1.4× | 1.1% | PASS |
| mem_reads | `index_join_scan` | 2.93ms | 4.98ms | 1.7× | 2.2% | PASS |
| mem_reads | `types_table_scan` | 777.94ms | 956.10ms | 1.2× | 0.8% | PASS |
| mem_reads | `table_scan` | 885.83ms | 1.05s | 1.2× | 1.0% | PASS |
| mem_reads | `oltp_read_only` | 100.14ms | 114.42ms | 1.1× | 0.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 151.55ms | 193.99ms | 1.3× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 12.36ms | 20.04ms | 1.6× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 46.19ms | 83.54ms | 1.8× | 1.0% | PASS |
| mem_writes | `oltp_update_non_index` | 33.07ms | 53.34ms | 1.6× | 1.0% | PASS |
| mem_writes | `oltp_delete_insert` | 33.91ms | 59.41ms | 1.8× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 18.61ms | 36.67ms | 2.0× | 1.4% | PASS |
| mem_writes | `types_delete_insert` | 21.53ms | 35.19ms | 1.6× | 1.1% | PASS |
| mem_writes | `oltp_read_write` | 67.54ms | 100.11ms | 1.5× | 1.2% | PASS |
| file_reads | `oltp_point_select` | 42.53ms | 29.13ms | 0.7× | 0.7% | PASS |
| file_reads | `oltp_range_select` | 16.55ms | 16.43ms | 1.0× | 0.9% | PASS |
| file_reads | `oltp_sum_range` | 15.80ms | 15.28ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_order_range` | 2.90ms | 3.00ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_distinct_range` | 3.62ms | 3.75ms | 1.0× | 2.3% | PASS |
| file_reads | `oltp_index_scan` | 5.47ms | 5.08ms | 0.9× | 1.3% | PASS |
| file_reads | `select_random_points` | 23.75ms | 23.00ms | 1.0× | 0.9% | PASS |
| file_reads | `select_random_ranges` | 7.40ms | 6.60ms | 0.9× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 6.96ms | 7.65ms | 1.1× | 0.7% | PASS |
| file_reads | `groupby_scan` | 25.58ms | 28.81ms | 1.1× | 0.6% | PASS |
| file_reads | `index_join` | 7.10ms | 8.94ms | 1.3× | 1.6% | PASS |
| file_reads | `index_join_scan` | 3.27ms | 5.31ms | 1.6× | 3.3% | PASS |
| file_reads | `types_table_scan` | 783.65ms | 945.07ms | 1.2× | 0.8% | PASS |
| file_reads | `table_scan` | 890.39ms | 1.05s | 1.2× | 2.4% | PASS |
| file_reads | `oltp_read_only` | 128.47ms | 121.14ms | 0.9× | 0.7% | PASS |
| file_writes | `oltp_bulk_insert` | 161.70ms | 201.65ms | 1.2× | 0.7% | PASS |
| file_writes | `oltp_insert` | 15.90ms | 23.44ms | 1.5× | 1.6% | PASS |
| file_writes | `oltp_update_index` | 59.58ms | 92.80ms | 1.6× | 1.6% | PASS |
| file_writes | `oltp_update_non_index` | 45.63ms | 59.12ms | 1.3× | 1.4% | PASS |
| file_writes | `oltp_delete_insert` | 47.74ms | 70.62ms | 1.5× | 2.0% | PASS |
| file_writes | `oltp_write_only` | 29.70ms | 41.24ms | 1.4× | 2.0% | PASS |
| file_writes | `types_delete_insert` | 29.38ms | 38.45ms | 1.3× | 2.6% | PASS |
| file_writes | `oltp_read_write` | 79.25ms | 103.51ms | 1.3× | 1.5% | PASS |
| ac_reads | `oltp_point_select` | 28.42ms | 28.84ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 14.97ms | 16.30ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_sum_range` | 14.63ms | 15.28ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_order_range` | 2.82ms | 2.98ms | 1.1× | 2.4% | PASS |
| ac_reads | `oltp_distinct_range` | 3.49ms | 3.70ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 4.16ms | 4.93ms | 1.2× | 1.8% | PASS |
| ac_reads | `select_random_points` | 21.96ms | 22.45ms | 1.0× | 0.8% | PASS |
| ac_reads | `select_random_ranges` | 5.90ms | 6.46ms | 1.1× | 0.9% | PASS |
| ac_reads | `covering_index_scan` | 5.62ms | 7.44ms | 1.3× | 1.2% | PASS |
| ac_reads | `groupby_scan` | 25.29ms | 28.69ms | 1.1× | 0.5% | PASS |
| ac_reads | `index_join` | 6.48ms | 8.80ms | 1.4× | 1.8% | PASS |
| ac_reads | `index_join_scan` | 3.20ms | 5.28ms | 1.7× | 1.8% | PASS |
| ac_reads | `types_table_scan` | 787.86ms | 946.55ms | 1.2× | 1.9% | PASS |
| ac_reads | `table_scan` | 896.57ms | 1.04s | 1.2× | 2.4% | PASS |
| ac_reads | `oltp_read_only` | 111.34ms | 121.54ms | 1.1× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 8.61ms | 12.73ms | 1.5× | 1.9% | PASS |
| ac_writes | `oltp_insert_ac` | 9.70ms | 20.23ms | 2.1× | 2.3% | PASS |
| ac_writes | `oltp_update_index_ac` | 10.90ms | 28.04ms | 2.6× | 2.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 8.92ms | 16.27ms | 1.8× | 2.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 9.78ms | 22.58ms | 2.3× | 3.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 9.81ms | 21.12ms | 2.2× | 2.0% | PASS |
| ac_writes | `types_delete_insert_ac` | 8.60ms | 16.89ms | 2.0× | 3.0% | PASS |
| ac_writes | `oltp_read_write_ac` | 15.02ms | 29.78ms | 2.0× | 6.7% | PASS |

</details>

</details>

## Version-control latency

Wall time: 4m 25s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 34.81ms | 38.00ms | 91.6% | 1.9% | PASS |
| `status_dirty_many_tables` | 38.18ms | 42.00ms | 90.9% | 0.6% | PASS |
| `diff_regular_working_one_table` | 30.01ms | 33.00ms | 90.9% | 0.7% | PASS |
| `diff_regular_working_many_tables` | 43.00ms | 50.00ms | 86.0% | 0.6% | PASS |
| `diff_stat_working_many_tables` | 42.92ms | 48.00ms | 89.4% | 0.6% | PASS |
| `diff_schema_working_many_tables` | 43.43ms | 48.00ms | 90.5% | 0.6% | PASS |
| `branch_list_many_branches` | 22.30ms | 25.00ms | 89.2% | 0.6% | PASS |
| `branch_create_delete` | 24.90ms | 27.00ms | 92.2% | 0.8% | PASS |
| `at_literal_deep_history` | 24.12ms | 28.00ms | 86.1% | 0.8% | PASS |
| `diff_literal_deep_history` | 24.18ms | 28.00ms | 86.4% | 0.8% | PASS |
| `history_literal_deep_history` | 25.27ms | 30.00ms | 84.2% | 0.7% | PASS |
| `checkout_branch_clean` | 32.80ms | 43.00ms | 76.3% | 0.5% | PASS |
| `merge_data_no_conflicts` | 27.41ms | 33.00ms | 83.1% | 1.1% | PASS |
| `merge_data_secondary_index` | 865.96ms | 944.00ms | 91.7% | 1.3% | PASS |
| `merge_schema_no_conflicts` | 21.39ms | 24.00ms | 89.1% | 1.3% | PASS |
| `merge_data_conflicts` | 30.22ms | 33.00ms | 91.6% | 1.2% | PASS |
| `merge_data_conflicts_with_resolve` | 31.50ms | 34.00ms | 92.7% | 2.1% | PASS |

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

candidate: ea46cdf.

Fixture file bytes: base 107,968,301; delta 115,879,032.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 58.148 | 57.514–61.207 | 37.717 | 1719.75 | 107968301 |
| push_initial | candidate | 587.801 | 584.591–590.042 | 566.855 | 170.13 | 107956807 |
| clone_initial | candidate | 758.602 | 753.514–761.234 | 732.403 | 131.82 | 108475105 |
| push_noop | candidate | 0.657 | 0.599–0.676 | 0.315 | — | 0 |
| fetch_noop | candidate | 0.204 | 0.193–0.210 | 0.206 | — | 0 |
| push_incremental | candidate | 232.946 | 232.363–233.558 | 229.646 | 4.29 | 7909775 |
| fetch_incremental | candidate | 95.903 | 95.355–98.119 | 93.490 | 10.43 | 7910084 |
| pull_fast_forward | candidate | 84.302 | 83.500–84.568 | 80.990 | 11.86 | 7910844 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 90.42 | 175.487 | 108.763 | 18329 | 2066 |
| clone_initial | candidate | 90.67 | 295.241 | 110.833 | 47281 | 2353 |
| push_noop | candidate | 41.66 | 0.073 | 0.000 | 27 | 2 |
| fetch_noop | candidate | 41.66 | 0.135 | 0.000 | 42 | 0 |
| push_incremental | candidate | 41.66 | 96.904 | 7.910 | 22422 | 125 |
| fetch_incremental | candidate | 41.66 | 34.472 | 7.910 | 10810 | 124 |
| pull_fast_forward | candidate | 41.66 | 26.497 | 8.959 | 7032 | 142 |

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

candidate: ea46cdf.

Fixture file bytes: base 1,087,718,066; delta 1,103,525,194.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 3355.374 | 3207.148–4147.224 | 504.843 | 298.03 | 1087718066 |
| push_initial | candidate | 6068.820 | 6026.673–6612.636 | 3401.226 | 164.78 | 1079566310 |
| clone_initial | candidate | 5041.148 | 4992.845–5171.907 | 5004.502 | 198.37 | 1102407396 |
| push_noop | candidate | 0.383 | 0.357–0.401 | 0.175 | — | 0 |
| fetch_noop | candidate | 0.091 | 0.087–0.094 | 0.092 | — | 0 |
| push_incremental | candidate | 763.604 | 753.827–767.264 | 759.929 | 1.31 | 15806003 |
| fetch_incremental | candidate | 183.513 | 179.795–187.336 | 180.901 | 5.45 | 15806513 |
| pull_fast_forward | candidate | 191.611 | 189.693–193.153 | 188.461 | 5.22 | 15807273 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 134.75 | 2091.801 | 1087.622 | 263479 | 20541 |
| clone_initial | candidate | 124.06 | 3515.194 | 1133.868 | 611657 | 32259 |
| push_noop | candidate | 44.00 | 0.078 | 0.000 | 28 | 2 |
| fetch_noop | candidate | 44.00 | 0.137 | 0.000 | 42 | 0 |
| push_incremental | candidate | 48.43 | 615.569 | 15.928 | 149502 | 313 |
| fetch_incremental | candidate | 50.31 | 62.057 | 15.928 | 13854 | 313 |
| pull_fast_forward | candidate | 50.46 | 61.990 | 16.978 | 13849 | 331 |

</details>


</details>

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh`, `test/vc_perf_ceiling.sh`, and `test/remote_perf.py`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
