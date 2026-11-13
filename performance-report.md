# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-11-13 11:30 UTC
>
> Commit: [`219715dd09997f8bd4e8c1e1c541a88dca042043`](https://github.com/dolthub/doltlite/commit/219715dd09997f8bd4e8c1e1c541a88dca042043)
>
> Runner: ubuntu24 20261101.360.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/43402150547)

The SQL workloads compare optimized DoltLite against stock SQLite on the same GitHub-hosted runner, alternating execution order on each repetition. Version-control sections measure DoltLite latency. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.55s | 11.29s | 1.18× | 1.4% | **PASS** |
| Writes | 2.02s | 3.12s | 1.55× | 1.2% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.42s | 11.53s | 1.11× | 1.5% | **PASS** |
| Writes | 3.37s | 3.86s | 1.15× | 1.5% | **PASS** |
| Autocommit writes | 764.62ms | 1.45s | 1.90× | 5.0% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.42s | 2.78s | 1.15× | 1.3% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.14s | 2.22s | 1.04× | 2.9% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.49s | 3.14s | 1.26× | 1.6% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.51s | 3.15s | 1.26× | 1.2% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 445.21ms | 719.03ms | 1.62× | 1.0% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 391.20ms | 587.59ms | 1.50× | 2.0% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 586.74ms | 899.85ms | 1.53× | 1.3% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 596.51ms | 915.27ms | 1.53× | 0.9% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.67s | 2.85s | 1.07× | 1.4% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.30s | 2.26s | 0.98× | 2.1% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.73s | 3.21s | 1.18× | 1.7% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.71s | 3.21s | 1.18× | 1.0% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 606.07ms | 781.86ms | 1.29× | 1.3% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.16s | 1.07s | 0.92× | 5.6% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 841.13ms | 1.03s | 1.23× | 1.7% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 757.05ms | 981.01ms | 1.30× | 1.2% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.61s | 2.88s | 1.10× | 1.4% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 1.91s | 2.03s | 1.06× | 2.8% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.63s | 3.22s | 1.23× | 1.6% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.58s | 3.21s | 1.25× | 1.2% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 185.08ms | 359.92ms | 1.94× | 3.9% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 189.51ms | 320.95ms | 1.69× | 4.5% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 201.89ms | 399.84ms | 1.98× | 6.3% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 188.13ms | 372.97ms | 1.98× | 5.2% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 25.10ms | 29.44ms | 1.2× | 2.0% | PASS |
| mem_reads | `oltp_range_select` | 10.53ms | 12.36ms | 1.2× | 1.5% | PASS |
| mem_reads | `oltp_sum_range` | 9.62ms | 11.13ms | 1.2× | 1.3% | PASS |
| mem_reads | `oltp_order_range` | 2.60ms | 2.87ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 3.71ms | 3.94ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 3.90ms | 5.19ms | 1.3× | 1.4% | PASS |
| mem_reads | `select_random_points` | 12.87ms | 11.54ms | 0.9× | 1.5% | PASS |
| mem_reads | `select_random_ranges` | 4.69ms | 4.98ms | 1.1× | 1.2% | PASS |
| mem_reads | `covering_index_scan` | 7.69ms | 10.66ms | 1.4× | 0.9% | PASS |
| mem_reads | `groupby_scan` | 30.01ms | 31.62ms | 1.1× | 0.9% | PASS |
| mem_reads | `index_join` | 5.77ms | 8.29ms | 1.4× | 1.4% | PASS |
| mem_reads | `index_join_scan` | 3.22ms | 5.51ms | 1.7× | 2.2% | PASS |
| mem_reads | `types_table_scan` | 1.02s | 1.20s | 1.2× | 0.5% | PASS |
| mem_reads | `table_scan` | 1.17s | 1.33s | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_read_only` | 106.12ms | 118.73ms | 1.1× | 1.4% | PASS |
| mem_writes | `oltp_bulk_insert` | 182.43ms | 280.80ms | 1.5× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 15.26ms | 25.13ms | 1.6× | 1.1% | PASS |
| mem_writes | `oltp_update_index` | 50.85ms | 94.59ms | 1.9× | 1.2% | PASS |
| mem_writes | `oltp_update_non_index` | 35.81ms | 58.68ms | 1.6× | 1.0% | PASS |
| mem_writes | `oltp_delete_insert` | 45.59ms | 73.56ms | 1.6× | 0.9% | PASS |
| mem_writes | `oltp_write_only` | 22.23ms | 45.92ms | 2.1× | 0.8% | PASS |
| mem_writes | `types_delete_insert` | 24.89ms | 33.99ms | 1.4× | 1.0% | PASS |
| mem_writes | `oltp_read_write` | 68.14ms | 106.36ms | 1.6× | 1.5% | PASS |
| file_reads | `oltp_point_select` | 88.70ms | 46.14ms | 0.5× | 0.7% | PASS |
| file_reads | `oltp_range_select` | 17.21ms | 14.08ms | 0.8× | 2.0% | PASS |
| file_reads | `oltp_sum_range` | 16.54ms | 12.94ms | 0.8× | 1.4% | PASS |
| file_reads | `oltp_order_range` | 3.44ms | 3.12ms | 0.9× | 1.9% | PASS |
| file_reads | `oltp_distinct_range` | 4.51ms | 4.23ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 10.62ms | 7.20ms | 0.7× | 1.8% | PASS |
| file_reads | `select_random_points` | 20.53ms | 13.48ms | 0.7× | 1.7% | PASS |
| file_reads | `select_random_ranges` | 11.33ms | 6.85ms | 0.6× | 1.6% | PASS |
| file_reads | `covering_index_scan` | 14.72ms | 12.49ms | 0.8× | 1.3% | PASS |
| file_reads | `groupby_scan` | 31.03ms | 32.01ms | 1.0× | 0.9% | PASS |
| file_reads | `index_join` | 9.58ms | 9.80ms | 1.0× | 1.7% | PASS |
| file_reads | `index_join_scan` | 4.16ms | 5.71ms | 1.4× | 1.8% | PASS |
| file_reads | `types_table_scan` | 1.04s | 1.20s | 1.2× | 1.0% | PASS |
| file_reads | `table_scan` | 1.20s | 1.33s | 1.1× | 1.1% | PASS |
| file_reads | `oltp_read_only` | 203.26ms | 144.50ms | 0.7× | 1.2% | PASS |
| file_writes | `oltp_bulk_insert` | 197.95ms | 292.51ms | 1.5× | 0.8% | PASS |
| file_writes | `oltp_insert` | 22.08ms | 28.63ms | 1.3× | 1.3% | PASS |
| file_writes | `oltp_update_index` | 78.13ms | 103.53ms | 1.3× | 1.3% | PASS |
| file_writes | `oltp_update_non_index` | 58.96ms | 65.09ms | 1.1× | 1.6% | PASS |
| file_writes | `oltp_delete_insert` | 68.24ms | 85.15ms | 1.2× | 1.3% | PASS |
| file_writes | `oltp_write_only` | 44.02ms | 51.53ms | 1.2× | 1.4% | PASS |
| file_writes | `types_delete_insert` | 40.32ms | 41.03ms | 1.0× | 1.4% | PASS |
| file_writes | `oltp_read_write` | 96.37ms | 114.40ms | 1.2× | 1.3% | PASS |
| ac_reads | `oltp_point_select` | 46.64ms | 46.41ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_range_select` | 13.00ms | 14.02ms | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_sum_range` | 12.08ms | 12.87ms | 1.1× | 0.6% | PASS |
| ac_reads | `oltp_order_range` | 3.05ms | 3.15ms | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_distinct_range` | 4.10ms | 4.20ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 6.57ms | 7.23ms | 1.1× | 1.2% | PASS |
| ac_reads | `select_random_points` | 16.03ms | 13.54ms | 0.8× | 1.6% | PASS |
| ac_reads | `select_random_ranges` | 7.17ms | 6.82ms | 1.0× | 0.8% | PASS |
| ac_reads | `covering_index_scan` | 10.54ms | 12.59ms | 1.2× | 1.3% | PASS |
| ac_reads | `groupby_scan` | 30.69ms | 32.22ms | 1.0× | 1.0% | PASS |
| ac_reads | `index_join` | 7.57ms | 10.01ms | 1.3× | 1.6% | PASS |
| ac_reads | `index_join_scan` | 3.74ms | 5.86ms | 1.6× | 2.0% | PASS |
| ac_reads | `types_table_scan` | 1.09s | 1.22s | 1.1× | 1.0% | PASS |
| ac_reads | `table_scan` | 1.21s | 1.34s | 1.1× | 2.5% | PASS |
| ac_reads | `oltp_read_only` | 140.30ms | 145.63ms | 1.0× | 2.1% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 19.86ms | 31.19ms | 1.6× | 3.8% | PASS |
| ac_writes | `oltp_insert_ac` | 22.62ms | 43.33ms | 1.9× | 3.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 24.47ms | 57.06ms | 2.3× | 4.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.93ms | 38.93ms | 1.8× | 6.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 23.21ms | 47.72ms | 2.1× | 4.0% | PASS |
| ac_writes | `oltp_write_only_ac` | 23.32ms | 47.39ms | 2.0× | 4.8% | PASS |
| ac_writes | `types_delete_insert_ac` | 20.28ms | 38.53ms | 1.9× | 3.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 29.39ms | 55.76ms | 1.9× | 3.3% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 27.55ms | 25.06ms | 0.9× | 3.7% | PASS |
| mem_reads | `oltp_range_select` | 12.13ms | 10.50ms | 0.9× | 3.5% | PASS |
| mem_reads | `oltp_sum_range` | 10.32ms | 10.22ms | 1.0× | 4.6% | PASS |
| mem_reads | `oltp_order_range` | 2.15ms | 2.27ms | 1.1× | 6.7% | PASS |
| mem_reads | `oltp_distinct_range` | 2.69ms | 2.73ms | 1.0× | 2.9% | PASS |
| mem_reads | `oltp_index_scan` | 2.62ms | 4.46ms | 1.7× | 2.8% | PASS |
| mem_reads | `select_random_points` | 16.48ms | 14.71ms | 0.9× | 4.1% | PASS |
| mem_reads | `select_random_ranges` | 4.74ms | 4.19ms | 0.9× | 3.1% | PASS |
| mem_reads | `covering_index_scan` | 4.16ms | 6.87ms | 1.7× | 2.4% | PASS |
| mem_reads | `groupby_scan` | 22.36ms | 21.17ms | 0.9× | 1.2% | PASS |
| mem_reads | `index_join` | 8.86ms | 7.96ms | 0.9× | 1.9% | PASS |
| mem_reads | `index_join_scan` | 3.30ms | 6.31ms | 1.9× | 5.5% | PASS |
| mem_reads | `types_table_scan` | 884.18ms | 950.03ms | 1.1× | 1.3% | PASS |
| mem_reads | `table_scan` | 1.03s | 1.06s | 1.0× | 1.5% | PASS |
| mem_reads | `oltp_read_only` | 103.55ms | 89.69ms | 0.9× | 1.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 144.80ms | 201.89ms | 1.4× | 1.4% | PASS |
| mem_writes | `oltp_insert` | 10.99ms | 21.92ms | 2.0× | 1.4% | PASS |
| mem_writes | `oltp_update_index` | 46.84ms | 91.05ms | 1.9× | 2.4% | PASS |
| mem_writes | `oltp_update_non_index` | 35.87ms | 50.22ms | 1.4× | 2.3% | PASS |
| mem_writes | `oltp_delete_insert` | 38.06ms | 66.11ms | 1.7× | 1.6% | PASS |
| mem_writes | `oltp_write_only` | 20.40ms | 36.02ms | 1.8× | 1.6% | PASS |
| mem_writes | `types_delete_insert` | 28.32ms | 38.90ms | 1.4× | 2.5% | PASS |
| mem_writes | `oltp_read_write` | 65.91ms | 81.48ms | 1.2× | 3.4% | PASS |
| file_reads | `oltp_point_select` | 74.30ms | 36.14ms | 0.5× | 3.3% | PASS |
| file_reads | `oltp_range_select` | 16.09ms | 10.94ms | 0.7× | 2.1% | PASS |
| file_reads | `oltp_sum_range` | 15.57ms | 10.81ms | 0.7× | 1.7% | PASS |
| file_reads | `oltp_order_range` | 2.94ms | 2.42ms | 0.8× | 1.8% | PASS |
| file_reads | `oltp_distinct_range` | 3.52ms | 2.95ms | 0.8× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 8.28ms | 5.94ms | 0.7× | 1.5% | PASS |
| file_reads | `select_random_points` | 26.93ms | 17.37ms | 0.6× | 4.6% | PASS |
| file_reads | `select_random_ranges` | 10.62ms | 5.69ms | 0.5× | 1.6% | PASS |
| file_reads | `covering_index_scan` | 10.20ms | 8.68ms | 0.9× | 2.5% | PASS |
| file_reads | `groupby_scan` | 23.86ms | 22.23ms | 0.9× | 1.5% | PASS |
| file_reads | `index_join` | 11.56ms | 7.83ms | 0.7× | 3.9% | PASS |
| file_reads | `index_join_scan` | 3.50ms | 5.08ms | 1.4× | 4.2% | PASS |
| file_reads | `types_table_scan` | 888.45ms | 954.36ms | 1.1× | 2.1% | PASS |
| file_reads | `table_scan` | 1.05s | 1.08s | 1.0× | 1.9% | PASS |
| file_reads | `oltp_read_only` | 160.56ms | 98.19ms | 0.6× | 2.3% | PASS |
| file_writes | `oltp_bulk_insert` | 222.41ms | 268.66ms | 1.2× | 1.9% | PASS |
| file_writes | `oltp_insert` | 19.77ms | 42.22ms | 2.1× | 10.3% | PASS |
| file_writes | `oltp_update_index` | 167.20ms | 179.88ms | 1.1× | 6.5% | PASS |
| file_writes | `oltp_update_non_index` | 142.74ms | 108.61ms | 0.8× | 3.9% | PASS |
| file_writes | `oltp_delete_insert` | 178.26ms | 139.66ms | 0.8× | 3.7% | PASS |
| file_writes | `oltp_write_only` | 116.40ms | 97.13ms | 0.8× | 6.5% | PASS |
| file_writes | `types_delete_insert` | 138.08ms | 83.02ms | 0.6× | 6.8% | PASS |
| file_writes | `oltp_read_write` | 178.43ms | 147.52ms | 0.8× | 4.8% | PASS |
| ac_reads | `oltp_point_select` | 46.38ms | 39.22ms | 0.8× | 3.2% | PASS |
| ac_reads | `oltp_range_select` | 14.68ms | 12.00ms | 0.8× | 4.4% | PASS |
| ac_reads | `oltp_sum_range` | 14.53ms | 12.43ms | 0.9× | 2.8% | PASS |
| ac_reads | `oltp_order_range` | 2.81ms | 2.56ms | 0.9× | 2.7% | PASS |
| ac_reads | `oltp_distinct_range` | 3.34ms | 3.11ms | 0.9× | 3.2% | PASS |
| ac_reads | `oltp_index_scan` | 4.90ms | 5.86ms | 1.2× | 2.8% | PASS |
| ac_reads | `select_random_points` | 18.48ms | 15.06ms | 0.8× | 2.6% | PASS |
| ac_reads | `select_random_ranges` | 6.64ms | 5.32ms | 0.8× | 1.9% | PASS |
| ac_reads | `covering_index_scan` | 6.32ms | 7.79ms | 1.2× | 2.2% | PASS |
| ac_reads | `groupby_scan` | 21.99ms | 20.63ms | 0.9× | 2.8% | PASS |
| ac_reads | `index_join` | 10.11ms | 8.32ms | 0.8× | 6.4% | PASS |
| ac_reads | `index_join_scan` | 3.36ms | 5.70ms | 1.7× | 8.4% | PASS |
| ac_reads | `types_table_scan` | 741.86ms | 836.31ms | 1.1× | 2.8% | PASS |
| ac_reads | `table_scan` | 904.39ms | 961.17ms | 1.1× | 3.3% | PASS |
| ac_reads | `oltp_read_only` | 110.04ms | 98.39ms | 0.9× | 3.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.89ms | 28.00ms | 1.2× | 6.7% | PASS |
| ac_writes | `oltp_insert_ac` | 23.92ms | 34.53ms | 1.4× | 3.8% | PASS |
| ac_writes | `oltp_update_index_ac` | 24.80ms | 58.81ms | 2.4× | 4.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.33ms | 32.97ms | 1.5× | 4.5% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.57ms | 45.34ms | 1.8× | 4.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 22.95ms | 41.88ms | 1.8× | 3.9% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.00ms | 33.18ms | 1.5× | 5.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 27.04ms | 46.26ms | 1.7× | 5.5% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 35.23ms | 36.98ms | 1.0× | 1.9% | PASS |
| mem_reads | `oltp_range_select` | 16.14ms | 15.48ms | 1.0× | 2.2% | PASS |
| mem_reads | `oltp_sum_range` | 14.64ms | 14.60ms | 1.0× | 2.2% | PASS |
| mem_reads | `oltp_order_range` | 3.18ms | 3.38ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_distinct_range` | 4.27ms | 4.39ms | 1.0× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 3.97ms | 6.35ms | 1.6× | 1.1% | PASS |
| mem_reads | `select_random_points` | 21.56ms | 20.85ms | 1.0× | 3.9% | PASS |
| mem_reads | `select_random_ranges` | 6.50ms | 6.49ms | 1.0× | 1.9% | PASS |
| mem_reads | `covering_index_scan` | 7.73ms | 10.89ms | 1.4× | 1.1% | PASS |
| mem_reads | `groupby_scan` | 34.21ms | 34.15ms | 1.0× | 0.9% | PASS |
| mem_reads | `index_join` | 10.20ms | 10.39ms | 1.0× | 2.2% | PASS |
| mem_reads | `index_join_scan` | 3.73ms | 6.08ms | 1.6× | 2.5% | PASS |
| mem_reads | `types_table_scan` | 1.04s | 1.35s | 1.3× | 1.4% | PASS |
| mem_reads | `table_scan` | 1.15s | 1.47s | 1.3× | 0.9% | PASS |
| mem_reads | `oltp_read_only` | 135.54ms | 139.46ms | 1.0× | 1.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 245.56ms | 333.59ms | 1.4× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 18.56ms | 34.70ms | 1.9× | 0.5% | PASS |
| mem_writes | `oltp_update_index` | 63.09ms | 127.25ms | 2.0× | 1.2% | PASS |
| mem_writes | `oltp_update_non_index` | 47.53ms | 72.60ms | 1.5× | 2.1% | PASS |
| mem_writes | `oltp_delete_insert` | 51.68ms | 94.68ms | 1.8× | 1.3% | PASS |
| mem_writes | `oltp_write_only` | 27.41ms | 51.20ms | 1.9× | 1.4% | PASS |
| mem_writes | `types_delete_insert` | 37.23ms | 56.27ms | 1.5× | 1.5% | PASS |
| mem_writes | `oltp_read_write` | 95.68ms | 129.56ms | 1.4× | 1.5% | PASS |
| file_reads | `oltp_point_select` | 102.48ms | 53.84ms | 0.5× | 1.3% | PASS |
| file_reads | `oltp_range_select` | 23.51ms | 17.38ms | 0.7× | 2.5% | PASS |
| file_reads | `oltp_sum_range` | 21.98ms | 16.57ms | 0.8× | 2.4% | PASS |
| file_reads | `oltp_order_range` | 4.15ms | 3.60ms | 0.9× | 2.4% | PASS |
| file_reads | `oltp_distinct_range` | 5.33ms | 4.66ms | 0.9× | 1.9% | PASS |
| file_reads | `oltp_index_scan` | 11.00ms | 8.29ms | 0.8× | 1.2% | PASS |
| file_reads | `select_random_points` | 31.51ms | 24.29ms | 0.8× | 2.1% | PASS |
| file_reads | `select_random_ranges` | 13.84ms | 8.47ms | 0.6× | 1.7% | PASS |
| file_reads | `covering_index_scan` | 15.00ms | 12.95ms | 0.9× | 1.2% | PASS |
| file_reads | `groupby_scan` | 35.26ms | 34.56ms | 1.0× | 1.0% | PASS |
| file_reads | `index_join` | 14.05ms | 11.51ms | 0.8× | 1.9% | PASS |
| file_reads | `index_join_scan` | 4.63ms | 6.36ms | 1.4× | 1.8% | PASS |
| file_reads | `types_table_scan` | 1.04s | 1.37s | 1.3× | 1.1% | PASS |
| file_reads | `table_scan` | 1.16s | 1.48s | 1.3× | 1.0% | PASS |
| file_reads | `oltp_read_only` | 239.98ms | 167.06ms | 0.7× | 1.4% | PASS |
| file_writes | `oltp_bulk_insert` | 267.74ms | 350.69ms | 1.3× | 0.8% | PASS |
| file_writes | `oltp_insert` | 25.99ms | 40.65ms | 1.6× | 1.4% | PASS |
| file_writes | `oltp_update_index` | 102.94ms | 155.17ms | 1.5× | 1.7% | PASS |
| file_writes | `oltp_update_non_index` | 89.36ms | 89.34ms | 1.0× | 6.7% | PASS |
| file_writes | `oltp_delete_insert` | 90.72ms | 115.34ms | 1.3× | 1.8% | PASS |
| file_writes | `oltp_write_only` | 60.08ms | 66.36ms | 1.1× | 1.8% | PASS |
| file_writes | `types_delete_insert` | 66.52ms | 66.27ms | 1.0× | 1.7% | PASS |
| file_writes | `oltp_read_write` | 137.78ms | 149.13ms | 1.1× | 2.0% | PASS |
| ac_reads | `oltp_point_select` | 60.14ms | 55.04ms | 0.9× | 1.6% | PASS |
| ac_reads | `oltp_range_select` | 19.32ms | 17.48ms | 0.9× | 2.1% | PASS |
| ac_reads | `oltp_sum_range` | 17.50ms | 16.61ms | 0.9× | 1.9% | PASS |
| ac_reads | `oltp_order_range` | 3.74ms | 3.58ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_distinct_range` | 4.80ms | 4.63ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_index_scan` | 6.55ms | 8.20ms | 1.3× | 1.6% | PASS |
| ac_reads | `select_random_points` | 24.98ms | 23.39ms | 0.9× | 2.9% | PASS |
| ac_reads | `select_random_ranges` | 9.17ms | 8.42ms | 0.9× | 1.1% | PASS |
| ac_reads | `covering_index_scan` | 10.39ms | 12.86ms | 1.2× | 1.1% | PASS |
| ac_reads | `groupby_scan` | 34.70ms | 34.52ms | 1.0× | 0.6% | PASS |
| ac_reads | `index_join` | 11.89ms | 11.42ms | 1.0× | 1.4% | PASS |
| ac_reads | `index_join_scan` | 4.24ms | 6.36ms | 1.5× | 1.7% | PASS |
| ac_reads | `types_table_scan` | 1.07s | 1.37s | 1.3× | 1.8% | PASS |
| ac_reads | `table_scan` | 1.18s | 1.48s | 1.3× | 2.2% | PASS |
| ac_reads | `oltp_read_only` | 172.05ms | 166.12ms | 1.0× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.81ms | 33.32ms | 1.5× | 5.0% | PASS |
| ac_writes | `oltp_insert_ac` | 25.35ms | 50.77ms | 2.0× | 6.6% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.64ms | 66.43ms | 2.5× | 5.8% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.83ms | 40.69ms | 1.9× | 6.9% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.77ms | 54.61ms | 2.1× | 6.0% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.55ms | 52.45ms | 2.1× | 7.0% | PASS |
| ac_writes | `types_delete_insert_ac` | 23.09ms | 42.51ms | 1.8× | 8.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 30.84ms | 59.07ms | 1.9× | 5.9% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 32.24ms | 37.91ms | 1.2× | 1.3% | PASS |
| mem_reads | `oltp_range_select` | 18.76ms | 22.76ms | 1.2× | 1.5% | PASS |
| mem_reads | `oltp_sum_range` | 17.31ms | 20.80ms | 1.2× | 1.1% | PASS |
| mem_reads | `oltp_order_range` | 3.39ms | 3.94ms | 1.2× | 1.6% | PASS |
| mem_reads | `oltp_distinct_range` | 4.55ms | 5.02ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 4.31ms | 5.80ms | 1.3× | 1.3% | PASS |
| mem_reads | `select_random_points` | 26.79ms | 29.29ms | 1.1× | 1.1% | PASS |
| mem_reads | `select_random_ranges` | 7.36ms | 8.46ms | 1.1× | 1.5% | PASS |
| mem_reads | `covering_index_scan` | 7.71ms | 10.33ms | 1.3× | 1.0% | PASS |
| mem_reads | `groupby_scan` | 36.07ms | 39.21ms | 1.1× | 1.1% | PASS |
| mem_reads | `index_join` | 7.88ms | 10.65ms | 1.4× | 1.8% | PASS |
| mem_reads | `index_join_scan` | 3.86ms | 5.88ms | 1.5× | 1.4% | PASS |
| mem_reads | `types_table_scan` | 1.03s | 1.33s | 1.3× | 0.5% | PASS |
| mem_reads | `table_scan` | 1.16s | 1.45s | 1.3× | 1.0% | PASS |
| mem_reads | `oltp_read_only` | 150.14ms | 170.91ms | 1.1× | 0.9% | PASS |
| mem_writes | `oltp_bulk_insert` | 248.27ms | 325.47ms | 1.3× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 18.98ms | 32.06ms | 1.7× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 66.51ms | 123.16ms | 1.9× | 0.7% | PASS |
| mem_writes | `oltp_update_non_index` | 51.95ms | 83.25ms | 1.6× | 0.9% | PASS |
| mem_writes | `oltp_delete_insert` | 49.46ms | 90.58ms | 1.8× | 0.9% | PASS |
| mem_writes | `oltp_write_only` | 27.17ms | 55.76ms | 2.1× | 0.8% | PASS |
| mem_writes | `types_delete_insert` | 33.15ms | 53.53ms | 1.6× | 1.3% | PASS |
| mem_writes | `oltp_read_write` | 101.02ms | 151.46ms | 1.5× | 1.1% | PASS |
| file_reads | `oltp_point_select` | 98.75ms | 55.16ms | 0.6× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 25.86ms | 25.01ms | 1.0× | 1.9% | PASS |
| file_reads | `oltp_sum_range` | 24.43ms | 23.06ms | 0.9× | 1.0% | PASS |
| file_reads | `oltp_order_range` | 4.28ms | 4.27ms | 1.0× | 1.5% | PASS |
| file_reads | `oltp_distinct_range` | 5.49ms | 5.36ms | 1.0× | 1.1% | PASS |
| file_reads | `oltp_index_scan` | 11.18ms | 8.06ms | 0.7× | 1.0% | PASS |
| file_reads | `select_random_points` | 36.14ms | 32.53ms | 0.9× | 1.3% | PASS |
| file_reads | `select_random_ranges` | 14.49ms | 10.65ms | 0.7× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 14.47ms | 12.67ms | 0.9× | 1.0% | PASS |
| file_reads | `groupby_scan` | 37.06ms | 39.73ms | 1.1× | 0.7% | PASS |
| file_reads | `index_join` | 11.71ms | 12.14ms | 1.0× | 0.6% | PASS |
| file_reads | `index_join_scan` | 4.78ms | 6.27ms | 1.3× | 1.3% | PASS |
| file_reads | `types_table_scan` | 1.03s | 1.33s | 1.3× | 0.4% | PASS |
| file_reads | `table_scan` | 1.15s | 1.45s | 1.3× | 0.3% | PASS |
| file_reads | `oltp_read_only` | 247.26ms | 198.33ms | 0.8× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 263.04ms | 338.31ms | 1.3× | 0.6% | PASS |
| file_writes | `oltp_insert` | 25.82ms | 35.96ms | 1.4× | 1.2% | PASS |
| file_writes | `oltp_update_index` | 93.95ms | 135.01ms | 1.4× | 1.2% | PASS |
| file_writes | `oltp_update_non_index` | 76.17ms | 89.71ms | 1.2× | 1.4% | PASS |
| file_writes | `oltp_delete_insert` | 74.44ms | 103.10ms | 1.4× | 1.0% | PASS |
| file_writes | `oltp_write_only` | 49.47ms | 61.90ms | 1.3× | 1.5% | PASS |
| file_writes | `types_delete_insert` | 48.74ms | 58.80ms | 1.2× | 1.5% | PASS |
| file_writes | `oltp_read_write` | 125.43ms | 158.22ms | 1.3× | 1.1% | PASS |
| ac_reads | `oltp_point_select` | 55.29ms | 55.25ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 21.76ms | 25.01ms | 1.1× | 1.7% | PASS |
| ac_reads | `oltp_sum_range` | 20.01ms | 23.15ms | 1.2× | 1.2% | PASS |
| ac_reads | `oltp_order_range` | 3.80ms | 4.26ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_distinct_range` | 4.97ms | 5.37ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_index_scan` | 6.77ms | 8.03ms | 1.2× | 1.8% | PASS |
| ac_reads | `select_random_points` | 30.35ms | 32.45ms | 1.1× | 1.4% | PASS |
| ac_reads | `select_random_ranges` | 9.96ms | 10.59ms | 1.1× | 1.8% | PASS |
| ac_reads | `covering_index_scan` | 10.06ms | 12.69ms | 1.3× | 1.0% | PASS |
| ac_reads | `groupby_scan` | 36.56ms | 39.67ms | 1.1× | 0.9% | PASS |
| ac_reads | `index_join` | 9.30ms | 12.12ms | 1.3× | 1.5% | PASS |
| ac_reads | `index_join_scan` | 4.30ms | 6.25ms | 1.5× | 1.1% | PASS |
| ac_reads | `types_table_scan` | 1.03s | 1.33s | 1.3× | 0.5% | PASS |
| ac_reads | `table_scan` | 1.15s | 1.45s | 1.3× | 0.4% | PASS |
| ac_reads | `oltp_read_only` | 183.56ms | 198.12ms | 1.1× | 0.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 20.41ms | 31.36ms | 1.5× | 3.0% | PASS |
| ac_writes | `oltp_insert_ac` | 23.93ms | 46.75ms | 2.0× | 5.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 25.01ms | 58.40ms | 2.3× | 4.9% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.86ms | 40.99ms | 1.8× | 6.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 22.58ms | 49.17ms | 2.2× | 6.7% | PASS |
| ac_writes | `oltp_write_only_ac` | 23.23ms | 48.47ms | 2.1× | 4.8% | PASS |
| ac_writes | `types_delete_insert_ac` | 20.51ms | 39.93ms | 1.9× | 5.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 29.59ms | 57.91ms | 2.0× | 3.9% | PASS |

</details>

</details>

## Version-control latency

Wall time: 6m 29s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 29.07ms | 38.00ms | 76.5% | 1.6% | PASS |
| `status_dirty_many_tables` | 31.14ms | 42.00ms | 74.1% | 1.1% | PASS |
| `diff_regular_working_one_table` | 24.26ms | 33.00ms | 73.5% | 0.9% | PASS |
| `diff_regular_working_many_tables` | 35.67ms | 50.00ms | 71.3% | 1.4% | PASS |
| `diff_stat_working_many_tables` | 35.55ms | 48.00ms | 74.1% | 1.4% | PASS |
| `diff_schema_working_many_tables` | 35.53ms | 48.00ms | 74.0% | 1.4% | PASS |
| `branch_list_many_branches` | 19.23ms | 25.00ms | 76.9% | 2.5% | PASS |
| `branch_create_delete` | 21.66ms | 27.00ms | 80.2% | 2.7% | PASS |
| `at_literal_deep_history` | 20.73ms | 28.00ms | 74.0% | 1.4% | PASS |
| `diff_literal_deep_history` | 19.92ms | 28.00ms | 71.2% | 0.9% | PASS |
| `history_literal_deep_history` | 21.07ms | 30.00ms | 70.2% | 1.7% | PASS |
| `checkout_branch_clean` | 27.55ms | 43.00ms | 64.1% | 1.6% | PASS |
| `merge_data_no_conflicts` | 23.54ms | 33.00ms | 71.3% | 1.6% | PASS |
| `merge_data_secondary_index` | 760.30ms | 944.00ms | 80.5% | 1.9% | PASS |
| `merge_schema_no_conflicts` | 17.65ms | 24.00ms | 73.6% | 1.4% | PASS |
| `merge_data_conflicts` | 24.97ms | 33.00ms | 75.7% | 1.4% | PASS |
| `merge_data_conflicts_with_resolve` | 25.04ms | 34.00ms | 73.6% | 1.2% | PASS |

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

candidate: 219715d.

Fixture file bytes: base 107,968,301; delta 115,879,032.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 505.435 | 469.555–532.014 | 31.750 | 197.85 | 107968301 |
| push_initial | candidate | 771.189 | 763.531–773.510 | 401.393 | 129.67 | 107956807 |
| clone_initial | candidate | 1029.271 | 1020.025–1096.313 | 525.047 | 97.16 | 108475105 |
| push_noop | candidate | 0.497 | 0.482–0.511 | 0.264 | — | 0 |
| fetch_noop | candidate | 0.179 | 0.166–0.187 | 0.180 | — | 0 |
| push_incremental | candidate | 185.584 | 180.626–195.962 | 165.183 | 5.39 | 7909775 |
| fetch_incremental | candidate | 90.544 | 88.934–115.003 | 70.786 | 11.04 | 7910084 |
| pull_fast_forward | candidate | 81.240 | 76.788–84.852 | 60.690 | 12.31 | 7910844 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 90.44 | 175.487 | 108.763 | 18329 | 2066 |
| clone_initial | candidate | 90.80 | 295.305 | 110.833 | 47296 | 2353 |
| push_noop | candidate | 41.46 | 0.075 | 0.000 | 28 | 2 |
| fetch_noop | candidate | 41.46 | 0.135 | 0.000 | 42 | 0 |
| push_incremental | candidate | 41.46 | 97.036 | 7.910 | 22453 | 125 |
| fetch_incremental | candidate | 41.46 | 34.472 | 7.910 | 10810 | 124 |
| pull_fast_forward | candidate | 41.46 | 26.497 | 8.959 | 7032 | 142 |

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

candidate: 219715d.

Fixture file bytes: base 1,087,718,066; delta 1,103,525,194.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 2662.665 | 2642.921–2679.130 | 359.683 | 375.56 | 1087718066 |
| push_initial | candidate | 6532.558 | 6511.797–6578.639 | 4300.543 | 153.08 | 1079566310 |
| clone_initial | candidate | 6728.431 | 6652.123–6757.544 | 6290.470 | 148.62 | 1102407396 |
| push_noop | candidate | 0.587 | 0.549–0.700 | 0.238 | — | 0 |
| fetch_noop | candidate | 0.133 | 0.120–0.144 | 0.136 | — | 0 |
| push_incremental | candidate | 927.703 | 919.823–932.643 | 921.253 | 1.08 | 15806003 |
| fetch_incremental | candidate | 246.905 | 244.085–251.109 | 235.545 | 4.05 | 15806513 |
| pull_fast_forward | candidate | 242.281 | 241.252–247.800 | 234.207 | 4.13 | 15807273 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 134.88 | 2091.585 | 1087.622 | 263426 | 20541 |
| clone_initial | candidate | 124.00 | 3517.332 | 1133.868 | 612185 | 32259 |
| push_noop | candidate | 44.20 | 0.078 | 0.000 | 28 | 2 |
| fetch_noop | candidate | 44.20 | 0.137 | 0.000 | 42 | 0 |
| push_incremental | candidate | 44.95 | 617.115 | 15.928 | 149889 | 313 |
| fetch_incremental | candidate | 50.42 | 62.234 | 15.928 | 13897 | 313 |
| pull_fast_forward | candidate | 50.36 | 62.166 | 16.978 | 13892 | 331 |

</details>


</details>

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh`, `test/vc_perf_ceiling.sh`, and `test/remote_perf.py`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
