# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-11-23 11:24 UTC
>
> Commit: [`97f35b1cb80cc7511221c680444f36e578bf2c5e`](https://github.com/dolthub/doltlite/commit/97f35b1cb80cc7511221c680444f36e578bf2c5e)
>
> Runner: ubuntu24 20261116.133.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/45148206112)

The SQL workloads compare optimized DoltLite against stock SQLite on the same GitHub-hosted runner, alternating execution order on each repetition. Version-control sections measure DoltLite latency. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class. Mean multiple is the average over workloads of each workload's paired DoltLite/SQLite ratio, so every workload counts equally whatever its run time.

### In-memory

| Operation | Mean multiple | Paired-ratio noise | Result |
|---|---:|---:|---|
| Reads | 1.13× | 1.5% | **PASS** |
| Writes | 1.62× | 1.3% | **PASS** |

### File-backed

| Operation | Mean multiple | Paired-ratio noise | Result |
|---|---:|---:|---|
| Reads | 0.90× | 1.4% | **PASS** |
| Writes | 1.12× | 2.0% | **PASS** |
| Autocommit writes | 1.85× | 3.4% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.8× for a section average. Durable autocommit writes use 3.6× and 2.6× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | Mean multiple | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 1.15× | 1.4% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 1.09× | 0.8% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 1.11× | 3.0% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 1.19× | 1.5% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 1.62× | 1.3% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 1.61× | 0.8% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 1.60× | 2.2% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 1.64× | 1.3% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 0.83× | 1.4% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 0.92× | 1.0% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 0.92× | 2.0% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 0.93× | 1.7% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 1.20× | 2.1% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 0.99× | 3.8% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 1.03× | 1.7% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 1.27× | 1.4% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 1.05× | 1.5% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 1.04× | 0.7% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 1.03× | 1.8% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 1.15× | 1.8% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 1.92× | 7.2% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 1.63× | 2.9% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 1.84× | 2.8% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 1.99× | 3.3% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 25.33ms | 29.49ms | 1.2× | 1.9% | PASS |
| mem_reads | `oltp_range_select` | 10.96ms | 12.21ms | 1.1× | 2.2% | PASS |
| mem_reads | `oltp_sum_range` | 9.76ms | 11.41ms | 1.2× | 2.3% | PASS |
| mem_reads | `oltp_order_range` | 2.66ms | 2.95ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 3.74ms | 4.10ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_index_scan` | 3.98ms | 5.41ms | 1.4× | 2.0% | PASS |
| mem_reads | `select_random_points` | 13.53ms | 11.87ms | 0.9× | 2.9% | PASS |
| mem_reads | `select_random_ranges` | 4.73ms | 5.10ms | 1.1× | 1.4% | PASS |
| mem_reads | `covering_index_scan` | 7.71ms | 10.66ms | 1.4× | 1.0% | PASS |
| mem_reads | `groupby_scan` | 30.46ms | 33.51ms | 1.1× | 0.7% | PASS |
| mem_reads | `index_join` | 5.85ms | 8.72ms | 1.5× | 1.9% | PASS |
| mem_reads | `index_join_scan` | 3.20ms | 5.39ms | 1.7× | 1.7% | PASS |
| mem_reads | `types_table_scan` | 1.06s | 788.45ms | 0.7× | 0.9% | PASS |
| mem_reads | `table_scan` | 1.21s | 902.59ms | 0.7× | 0.5% | PASS |
| mem_reads | `oltp_read_only` | 110.08ms | 122.06ms | 1.1× | 1.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 183.01ms | 279.61ms | 1.5× | 1.1% | PASS |
| mem_writes | `oltp_insert` | 15.42ms | 24.75ms | 1.6× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 51.79ms | 93.74ms | 1.8× | 1.9% | PASS |
| mem_writes | `oltp_update_non_index` | 36.23ms | 57.50ms | 1.6× | 1.2% | PASS |
| mem_writes | `oltp_delete_insert` | 46.68ms | 73.33ms | 1.6× | 1.1% | PASS |
| mem_writes | `oltp_write_only` | 22.87ms | 45.30ms | 2.0× | 1.4% | PASS |
| mem_writes | `types_delete_insert` | 25.24ms | 33.73ms | 1.3× | 1.7% | PASS |
| mem_writes | `oltp_read_write` | 70.89ms | 107.33ms | 1.5× | 1.4% | PASS |
| file_reads | `oltp_point_select` | 90.38ms | 46.02ms | 0.5× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 17.52ms | 14.05ms | 0.8× | 1.9% | PASS |
| file_reads | `oltp_sum_range` | 16.83ms | 13.36ms | 0.8× | 1.7% | PASS |
| file_reads | `oltp_order_range` | 3.55ms | 3.22ms | 0.9× | 1.8% | PASS |
| file_reads | `oltp_distinct_range` | 4.67ms | 4.41ms | 0.9× | 1.2% | PASS |
| file_reads | `oltp_index_scan` | 10.96ms | 7.45ms | 0.7× | 1.5% | PASS |
| file_reads | `select_random_points` | 21.15ms | 13.68ms | 0.6× | 1.7% | PASS |
| file_reads | `select_random_ranges` | 11.63ms | 6.96ms | 0.6× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 14.96ms | 12.89ms | 0.9× | 1.4% | PASS |
| file_reads | `groupby_scan` | 31.48ms | 33.97ms | 1.1× | 0.9% | PASS |
| file_reads | `index_join` | 9.69ms | 9.97ms | 1.0× | 1.8% | PASS |
| file_reads | `index_join_scan` | 4.20ms | 5.70ms | 1.4× | 2.8% | PASS |
| file_reads | `types_table_scan` | 1.05s | 780.33ms | 0.7× | 1.2% | PASS |
| file_reads | `table_scan` | 1.20s | 898.04ms | 0.7× | 1.1% | PASS |
| file_reads | `oltp_read_only` | 203.92ms | 146.88ms | 0.7× | 1.3% | PASS |
| file_writes | `oltp_bulk_insert` | 198.31ms | 292.18ms | 1.5× | 0.8% | PASS |
| file_writes | `oltp_insert` | 22.33ms | 28.26ms | 1.3× | 1.2% | PASS |
| file_writes | `oltp_update_index` | 78.14ms | 102.59ms | 1.3× | 1.7% | PASS |
| file_writes | `oltp_update_non_index` | 60.26ms | 63.64ms | 1.1× | 2.3% | PASS |
| file_writes | `oltp_delete_insert` | 69.22ms | 84.58ms | 1.2× | 2.0% | PASS |
| file_writes | `oltp_write_only` | 45.57ms | 50.88ms | 1.1× | 2.7% | PASS |
| file_writes | `types_delete_insert` | 41.02ms | 39.84ms | 1.0× | 2.7% | PASS |
| file_writes | `oltp_read_write` | 95.06ms | 111.61ms | 1.2× | 2.3% | PASS |
| ac_reads | `oltp_point_select` | 46.22ms | 45.94ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_range_select` | 13.51ms | 14.10ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_sum_range` | 12.59ms | 13.63ms | 1.1× | 2.2% | PASS |
| ac_reads | `oltp_order_range` | 3.19ms | 3.28ms | 1.0× | 2.5% | PASS |
| ac_reads | `oltp_distinct_range` | 4.21ms | 4.39ms | 1.0× | 2.0% | PASS |
| ac_reads | `oltp_index_scan` | 6.55ms | 7.36ms | 1.1× | 1.6% | PASS |
| ac_reads | `select_random_points` | 16.38ms | 13.63ms | 0.8× | 2.0% | PASS |
| ac_reads | `select_random_ranges` | 7.13ms | 6.95ms | 1.0× | 1.4% | PASS |
| ac_reads | `covering_index_scan` | 10.50ms | 12.89ms | 1.2× | 1.2% | PASS |
| ac_reads | `groupby_scan` | 30.97ms | 33.98ms | 1.1× | 0.7% | PASS |
| ac_reads | `index_join` | 7.67ms | 10.14ms | 1.3× | 2.0% | PASS |
| ac_reads | `index_join_scan` | 3.80ms | 5.95ms | 1.6× | 3.2% | PASS |
| ac_reads | `types_table_scan` | 1.13s | 809.70ms | 0.7× | 0.6% | PASS |
| ac_reads | `table_scan` | 1.27s | 927.00ms | 0.7× | 0.6% | PASS |
| ac_reads | `oltp_read_only` | 146.12ms | 151.68ms | 1.0× | 0.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 23.44ms | 36.05ms | 1.5× | 11.6% | PASS |
| ac_writes | `oltp_insert_ac` | 25.27ms | 47.73ms | 1.9× | 7.7% | PASS |
| ac_writes | `oltp_update_index_ac` | 28.27ms | 62.96ms | 2.2× | 7.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.45ms | 42.31ms | 1.8× | 6.7% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.87ms | 54.03ms | 2.1× | 7.4% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.58ms | 52.36ms | 2.0× | 8.8% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.16ms | 42.14ms | 1.9× | 6.6% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.21ms | 58.42ms | 1.9× | 6.4% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 32.73ms | 31.86ms | 1.0× | 0.8% | PASS |
| mem_reads | `oltp_range_select` | 14.94ms | 15.54ms | 1.0× | 1.0% | PASS |
| mem_reads | `oltp_sum_range` | 14.76ms | 14.23ms | 1.0× | 1.0% | PASS |
| mem_reads | `oltp_order_range` | 3.18ms | 3.34ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 4.08ms | 4.32ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 3.72ms | 5.69ms | 1.5× | 0.8% | PASS |
| mem_reads | `select_random_points` | 21.72ms | 19.88ms | 0.9× | 0.7% | PASS |
| mem_reads | `select_random_ranges` | 6.14ms | 5.71ms | 0.9× | 1.5% | PASS |
| mem_reads | `covering_index_scan` | 6.59ms | 9.63ms | 1.5× | 0.8% | PASS |
| mem_reads | `groupby_scan` | 34.77ms | 35.11ms | 1.0× | 0.5% | PASS |
| mem_reads | `index_join` | 10.74ms | 9.69ms | 0.9× | 0.9% | PASS |
| mem_reads | `index_join_scan` | 3.37ms | 6.27ms | 1.9× | 1.1% | PASS |
| mem_reads | `types_table_scan` | 1.02s | 812.29ms | 0.8× | 0.5% | PASS |
| mem_reads | `table_scan` | 1.16s | 898.60ms | 0.8× | 0.4% | PASS |
| mem_reads | `oltp_read_only` | 127.09ms | 131.06ms | 1.0× | 0.7% | PASS |
| mem_writes | `oltp_bulk_insert` | 197.65ms | 274.29ms | 1.4× | 0.4% | PASS |
| mem_writes | `oltp_insert` | 15.60ms | 28.72ms | 1.8× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 59.61ms | 118.02ms | 2.0× | 0.7% | PASS |
| mem_writes | `oltp_update_non_index` | 43.43ms | 62.84ms | 1.4× | 0.8% | PASS |
| mem_writes | `oltp_delete_insert` | 49.04ms | 84.98ms | 1.7× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 25.27ms | 43.58ms | 1.7× | 0.9% | PASS |
| mem_writes | `types_delete_insert` | 35.46ms | 49.80ms | 1.4× | 0.8% | PASS |
| mem_writes | `oltp_read_write` | 84.87ms | 114.60ms | 1.4× | 1.0% | PASS |
| file_reads | `oltp_point_select` | 59.99ms | 38.20ms | 0.6× | 0.7% | PASS |
| file_reads | `oltp_range_select` | 17.83ms | 16.12ms | 0.9× | 1.0% | PASS |
| file_reads | `oltp_sum_range` | 17.62ms | 14.87ms | 0.8× | 0.7% | PASS |
| file_reads | `oltp_order_range` | 3.53ms | 3.46ms | 1.0× | 1.1% | PASS |
| file_reads | `oltp_distinct_range` | 4.50ms | 4.45ms | 1.0× | 1.0% | PASS |
| file_reads | `oltp_index_scan` | 6.76ms | 6.53ms | 1.0× | 1.2% | PASS |
| file_reads | `select_random_points` | 25.38ms | 21.02ms | 0.8× | 1.0% | PASS |
| file_reads | `select_random_ranges` | 9.26ms | 6.50ms | 0.7× | 0.7% | PASS |
| file_reads | `covering_index_scan` | 9.71ms | 10.43ms | 1.1× | 0.5% | PASS |
| file_reads | `groupby_scan` | 35.04ms | 35.08ms | 1.0× | 0.5% | PASS |
| file_reads | `index_join` | 12.86ms | 10.39ms | 0.8× | 1.2% | PASS |
| file_reads | `index_join_scan` | 3.87ms | 6.59ms | 1.7× | 1.2% | PASS |
| file_reads | `types_table_scan` | 1.02s | 808.58ms | 0.8× | 0.6% | PASS |
| file_reads | `table_scan` | 1.18s | 902.13ms | 0.8× | 0.9% | PASS |
| file_reads | `oltp_read_only` | 167.88ms | 140.63ms | 0.8× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 285.27ms | 355.79ms | 1.2× | 3.1% | PASS |
| file_writes | `oltp_insert` | 30.29ms | 51.07ms | 1.7× | 5.6% | PASS |
| file_writes | `oltp_update_index` | 190.91ms | 210.07ms | 1.1× | 4.9% | PASS |
| file_writes | `oltp_update_non_index` | 176.83ms | 129.29ms | 0.7× | 3.0% | PASS |
| file_writes | `oltp_delete_insert` | 187.06ms | 169.16ms | 0.9× | 6.4% | PASS |
| file_writes | `oltp_write_only` | 140.72ms | 102.01ms | 0.7× | 2.8% | PASS |
| file_writes | `types_delete_insert` | 154.29ms | 96.16ms | 0.6× | 3.9% | PASS |
| file_writes | `oltp_read_write` | 191.56ms | 175.11ms | 0.9× | 3.7% | PASS |
| ac_reads | `oltp_point_select` | 41.68ms | 38.25ms | 0.9× | 0.6% | PASS |
| ac_reads | `oltp_range_select` | 16.01ms | 16.25ms | 1.0× | 0.6% | PASS |
| ac_reads | `oltp_sum_range` | 15.90ms | 14.96ms | 0.9× | 0.7% | PASS |
| ac_reads | `oltp_order_range` | 3.35ms | 3.48ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_distinct_range` | 4.30ms | 4.45ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 5.05ms | 6.53ms | 1.3× | 0.9% | PASS |
| ac_reads | `select_random_points` | 23.60ms | 21.01ms | 0.9× | 0.6% | PASS |
| ac_reads | `select_random_ranges` | 7.47ms | 6.50ms | 0.9× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 7.81ms | 10.46ms | 1.3× | 0.6% | PASS |
| ac_reads | `groupby_scan` | 34.83ms | 35.18ms | 1.0× | 0.5% | PASS |
| ac_reads | `index_join` | 12.03ms | 10.39ms | 0.9× | 0.9% | PASS |
| ac_reads | `index_join_scan` | 3.71ms | 6.57ms | 1.8× | 1.4% | PASS |
| ac_reads | `types_table_scan` | 1.02s | 806.78ms | 0.8× | 0.7% | PASS |
| ac_reads | `table_scan` | 1.16s | 898.75ms | 0.8× | 0.4% | PASS |
| ac_reads | `oltp_read_only` | 139.70ms | 140.12ms | 1.0× | 0.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 28.14ms | 33.75ms | 1.2× | 4.5% | PASS |
| ac_writes | `oltp_insert_ac` | 30.23ms | 42.99ms | 1.4× | 2.1% | PASS |
| ac_writes | `oltp_update_index_ac` | 30.71ms | 68.47ms | 2.2× | 3.8% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 27.41ms | 40.26ms | 1.5× | 3.5% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 29.88ms | 53.82ms | 1.8× | 2.8% | PASS |
| ac_writes | `oltp_write_only_ac` | 29.71ms | 50.48ms | 1.7× | 3.0% | PASS |
| ac_writes | `types_delete_insert_ac` | 28.50ms | 44.10ms | 1.5× | 2.3% | PASS |
| ac_writes | `oltp_read_write_ac` | 34.55ms | 57.73ms | 1.7× | 2.5% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 26.65ms | 25.50ms | 1.0× | 2.0% | PASS |
| mem_reads | `oltp_range_select` | 12.35ms | 11.93ms | 1.0× | 1.5% | PASS |
| mem_reads | `oltp_sum_range` | 12.29ms | 11.66ms | 0.9× | 2.3% | PASS |
| mem_reads | `oltp_order_range` | 2.43ms | 2.60ms | 1.1× | 3.9% | PASS |
| mem_reads | `oltp_distinct_range` | 3.21ms | 3.44ms | 1.1× | 3.1% | PASS |
| mem_reads | `oltp_index_scan` | 2.99ms | 5.00ms | 1.7× | 4.2% | PASS |
| mem_reads | `select_random_points` | 17.62ms | 16.50ms | 0.9× | 2.2% | PASS |
| mem_reads | `select_random_ranges` | 4.71ms | 4.45ms | 0.9× | 3.8% | PASS |
| mem_reads | `covering_index_scan` | 4.79ms | 7.54ms | 1.6× | 4.0% | PASS |
| mem_reads | `groupby_scan` | 24.40ms | 26.05ms | 1.1× | 1.4% | PASS |
| mem_reads | `index_join` | 10.49ms | 9.25ms | 0.9× | 2.1% | PASS |
| mem_reads | `index_join_scan` | 2.91ms | 6.13ms | 2.1× | 3.0% | PASS |
| mem_reads | `types_table_scan` | 798.11ms | 597.93ms | 0.7× | 3.4% | PASS |
| mem_reads | `table_scan` | 951.71ms | 672.51ms | 0.7× | 7.4% | PASS |
| mem_reads | `oltp_read_only` | 100.59ms | 100.27ms | 1.0× | 2.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 150.30ms | 204.42ms | 1.4× | 2.0% | PASS |
| mem_writes | `oltp_insert` | 12.22ms | 23.63ms | 1.9× | 2.3% | PASS |
| mem_writes | `oltp_update_index` | 49.83ms | 99.18ms | 2.0× | 2.3% | PASS |
| mem_writes | `oltp_update_non_index` | 35.38ms | 50.02ms | 1.4× | 1.8% | PASS |
| mem_writes | `oltp_delete_insert` | 40.40ms | 70.41ms | 1.7× | 1.7% | PASS |
| mem_writes | `oltp_write_only` | 21.12ms | 35.52ms | 1.7× | 2.9% | PASS |
| mem_writes | `types_delete_insert` | 28.14ms | 39.47ms | 1.4× | 2.1% | PASS |
| mem_writes | `oltp_read_write` | 73.85ms | 94.94ms | 1.3× | 2.3% | PASS |
| file_reads | `oltp_point_select` | 48.49ms | 30.27ms | 0.6× | 3.4% | PASS |
| file_reads | `oltp_range_select` | 15.10ms | 12.50ms | 0.8× | 1.8% | PASS |
| file_reads | `oltp_sum_range` | 14.86ms | 12.00ms | 0.8× | 2.4% | PASS |
| file_reads | `oltp_order_range` | 2.72ms | 2.59ms | 1.0× | 3.0% | PASS |
| file_reads | `oltp_distinct_range` | 3.38ms | 3.28ms | 1.0× | 2.4% | PASS |
| file_reads | `oltp_index_scan` | 5.32ms | 5.47ms | 1.0× | 2.0% | PASS |
| file_reads | `select_random_points` | 21.47ms | 17.54ms | 0.8× | 2.0% | PASS |
| file_reads | `select_random_ranges` | 6.94ms | 5.07ms | 0.7× | 2.0% | PASS |
| file_reads | `covering_index_scan` | 7.33ms | 7.90ms | 1.1× | 3.6% | PASS |
| file_reads | `groupby_scan` | 25.29ms | 26.88ms | 1.1× | 1.1% | PASS |
| file_reads | `index_join` | 12.35ms | 9.48ms | 0.8× | 1.8% | PASS |
| file_reads | `index_join_scan` | 3.33ms | 6.16ms | 1.8× | 3.1% | PASS |
| file_reads | `types_table_scan` | 828.70ms | 631.37ms | 0.8× | 1.5% | PASS |
| file_reads | `table_scan` | 911.92ms | 664.01ms | 0.7× | 1.8% | PASS |
| file_reads | `oltp_read_only` | 126.49ms | 101.31ms | 0.8× | 1.1% | PASS |
| file_writes | `oltp_bulk_insert` | 221.09ms | 270.11ms | 1.2× | 5.8% | PASS |
| file_writes | `oltp_insert` | 26.68ms | 45.87ms | 1.7× | 1.8% | PASS |
| file_writes | `oltp_update_index` | 151.72ms | 172.27ms | 1.1× | 1.5% | PASS |
| file_writes | `oltp_update_non_index` | 127.76ms | 107.08ms | 0.8× | 1.5% | PASS |
| file_writes | `oltp_delete_insert` | 152.71ms | 133.91ms | 0.9× | 1.6% | PASS |
| file_writes | `oltp_write_only` | 111.73ms | 92.30ms | 0.8× | 2.4% | PASS |
| file_writes | `types_delete_insert` | 119.47ms | 82.10ms | 0.7× | 2.6% | PASS |
| file_writes | `oltp_read_write` | 161.50ms | 147.66ms | 0.9× | 1.6% | PASS |
| ac_reads | `oltp_point_select` | 34.38ms | 30.77ms | 0.9× | 2.1% | PASS |
| ac_reads | `oltp_range_select` | 13.88ms | 12.86ms | 0.9× | 1.8% | PASS |
| ac_reads | `oltp_sum_range` | 14.15ms | 12.78ms | 0.9× | 1.8% | PASS |
| ac_reads | `oltp_order_range` | 2.81ms | 2.82ms | 1.0× | 2.0% | PASS |
| ac_reads | `oltp_distinct_range` | 3.55ms | 3.62ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_index_scan` | 4.31ms | 5.93ms | 1.4× | 1.6% | PASS |
| ac_reads | `select_random_points` | 22.32ms | 19.66ms | 0.9× | 1.7% | PASS |
| ac_reads | `select_random_ranges` | 6.26ms | 5.52ms | 0.9× | 1.7% | PASS |
| ac_reads | `covering_index_scan` | 6.17ms | 8.27ms | 1.3× | 1.5% | PASS |
| ac_reads | `groupby_scan` | 25.50ms | 27.10ms | 1.1× | 1.8% | PASS |
| ac_reads | `index_join` | 11.76ms | 9.64ms | 0.8× | 2.0% | PASS |
| ac_reads | `index_join_scan` | 3.20ms | 6.16ms | 1.9× | 4.2% | PASS |
| ac_reads | `types_table_scan` | 735.51ms | 571.05ms | 0.8× | 1.9% | PASS |
| ac_reads | `table_scan` | 824.09ms | 615.13ms | 0.7× | 1.8% | PASS |
| ac_reads | `oltp_read_only` | 100.85ms | 98.51ms | 1.0× | 1.3% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 32.20ms | 38.41ms | 1.2× | 2.8% | PASS |
| ac_writes | `oltp_insert_ac` | 34.13ms | 65.17ms | 1.9× | 2.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 34.57ms | 85.54ms | 2.5× | 3.0% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 31.54ms | 52.06ms | 1.7× | 2.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 33.95ms | 67.50ms | 2.0× | 2.9% | PASS |
| ac_writes | `oltp_write_only_ac` | 33.67ms | 66.53ms | 2.0× | 3.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 32.77ms | 52.71ms | 1.6× | 5.1% | PASS |
| ac_writes | `oltp_read_write_ac` | 37.99ms | 71.30ms | 1.9× | 2.7% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 35.58ms | 40.19ms | 1.1× | 1.7% | PASS |
| mem_reads | `oltp_range_select` | 21.18ms | 24.01ms | 1.1× | 2.0% | PASS |
| mem_reads | `oltp_sum_range` | 18.38ms | 21.99ms | 1.2× | 1.7% | PASS |
| mem_reads | `oltp_order_range` | 3.64ms | 4.55ms | 1.2× | 2.2% | PASS |
| mem_reads | `oltp_distinct_range` | 4.77ms | 5.73ms | 1.2× | 1.5% | PASS |
| mem_reads | `oltp_index_scan` | 4.53ms | 6.17ms | 1.4× | 1.5% | PASS |
| mem_reads | `select_random_points` | 28.50ms | 30.39ms | 1.1× | 1.5% | PASS |
| mem_reads | `select_random_ranges` | 7.80ms | 8.88ms | 1.1× | 1.8% | PASS |
| mem_reads | `covering_index_scan` | 7.76ms | 10.91ms | 1.4× | 1.1% | PASS |
| mem_reads | `groupby_scan` | 36.80ms | 41.66ms | 1.1× | 0.6% | PASS |
| mem_reads | `index_join` | 8.00ms | 11.12ms | 1.4× | 2.0% | PASS |
| mem_reads | `index_join_scan` | 4.08ms | 6.24ms | 1.5× | 2.2% | PASS |
| mem_reads | `types_table_scan` | 1.04s | 884.48ms | 0.9× | 0.7% | PASS |
| mem_reads | `table_scan` | 1.21s | 987.87ms | 0.8× | 0.9% | PASS |
| mem_reads | `oltp_read_only` | 157.50ms | 188.13ms | 1.2× | 1.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 249.94ms | 330.27ms | 1.3× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 19.06ms | 31.51ms | 1.7× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 65.23ms | 116.73ms | 1.8× | 0.9% | PASS |
| mem_writes | `oltp_update_non_index` | 52.20ms | 81.38ms | 1.6× | 1.7% | PASS |
| mem_writes | `oltp_delete_insert` | 50.10ms | 88.42ms | 1.8× | 1.5% | PASS |
| mem_writes | `oltp_write_only` | 26.82ms | 52.98ms | 2.0× | 1.6% | PASS |
| mem_writes | `types_delete_insert` | 33.04ms | 51.45ms | 1.6× | 1.2% | PASS |
| mem_writes | `oltp_read_write` | 99.77ms | 151.03ms | 1.5× | 1.6% | PASS |
| file_reads | `oltp_point_select` | 98.90ms | 56.27ms | 0.6× | 1.2% | PASS |
| file_reads | `oltp_range_select` | 27.39ms | 26.69ms | 1.0× | 2.3% | PASS |
| file_reads | `oltp_sum_range` | 25.46ms | 23.99ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_order_range` | 4.42ms | 4.76ms | 1.1× | 4.5% | PASS |
| file_reads | `oltp_distinct_range` | 5.58ms | 5.99ms | 1.1× | 2.7% | PASS |
| file_reads | `oltp_index_scan` | 11.14ms | 8.12ms | 0.7× | 2.1% | PASS |
| file_reads | `select_random_points` | 36.45ms | 32.83ms | 0.9× | 2.2% | PASS |
| file_reads | `select_random_ranges` | 14.72ms | 10.84ms | 0.7× | 1.8% | PASS |
| file_reads | `covering_index_scan` | 14.63ms | 12.85ms | 0.9× | 1.4% | PASS |
| file_reads | `groupby_scan` | 37.27ms | 41.67ms | 1.1× | 1.0% | PASS |
| file_reads | `index_join` | 11.74ms | 12.38ms | 1.1× | 1.7% | PASS |
| file_reads | `index_join_scan` | 4.88ms | 6.44ms | 1.3× | 1.6% | PASS |
| file_reads | `types_table_scan` | 1.03s | 876.12ms | 0.8× | 0.5% | PASS |
| file_reads | `table_scan` | 1.15s | 977.33ms | 0.9× | 0.4% | PASS |
| file_reads | `oltp_read_only` | 247.50ms | 210.99ms | 0.9× | 1.1% | PASS |
| file_writes | `oltp_bulk_insert` | 264.01ms | 341.06ms | 1.3× | 0.9% | PASS |
| file_writes | `oltp_insert` | 26.03ms | 35.85ms | 1.4× | 1.5% | PASS |
| file_writes | `oltp_update_index` | 93.91ms | 131.36ms | 1.4× | 1.1% | PASS |
| file_writes | `oltp_update_non_index` | 75.56ms | 88.08ms | 1.2× | 1.5% | PASS |
| file_writes | `oltp_delete_insert` | 75.19ms | 99.83ms | 1.3× | 1.2% | PASS |
| file_writes | `oltp_write_only` | 49.55ms | 59.86ms | 1.2× | 2.1% | PASS |
| file_writes | `types_delete_insert` | 49.28ms | 57.47ms | 1.2× | 1.6% | PASS |
| file_writes | `oltp_read_write` | 124.89ms | 157.16ms | 1.3× | 1.1% | PASS |
| ac_reads | `oltp_point_select` | 54.84ms | 55.91ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 22.03ms | 26.55ms | 1.2× | 1.9% | PASS |
| ac_reads | `oltp_sum_range` | 19.93ms | 23.82ms | 1.2× | 1.3% | PASS |
| ac_reads | `oltp_order_range` | 3.79ms | 4.77ms | 1.3× | 2.1% | PASS |
| ac_reads | `oltp_distinct_range` | 4.92ms | 5.99ms | 1.2× | 1.2% | PASS |
| ac_reads | `oltp_index_scan` | 6.66ms | 8.06ms | 1.2× | 1.3% | PASS |
| ac_reads | `select_random_points` | 30.34ms | 32.74ms | 1.1× | 1.9% | PASS |
| ac_reads | `select_random_ranges` | 10.35ms | 10.92ms | 1.1× | 1.8% | PASS |
| ac_reads | `covering_index_scan` | 10.19ms | 12.94ms | 1.3× | 1.9% | PASS |
| ac_reads | `groupby_scan` | 37.07ms | 42.04ms | 1.1× | 1.0% | PASS |
| ac_reads | `index_join` | 9.41ms | 12.52ms | 1.3× | 1.8% | PASS |
| ac_reads | `index_join_scan` | 4.53ms | 6.60ms | 1.5× | 3.2% | PASS |
| ac_reads | `types_table_scan` | 1.08s | 896.64ms | 0.8× | 3.3% | PASS |
| ac_reads | `table_scan` | 1.25s | 998.54ms | 0.8× | 4.4% | PASS |
| ac_reads | `oltp_read_only` | 200.05ms | 221.77ms | 1.1× | 1.4% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 19.45ms | 30.71ms | 1.6× | 3.1% | PASS |
| ac_writes | `oltp_insert_ac` | 22.07ms | 44.78ms | 2.0× | 3.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 25.58ms | 58.22ms | 2.3× | 5.3% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 20.52ms | 37.94ms | 1.8× | 4.1% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 21.89ms | 47.44ms | 2.2× | 3.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 22.15ms | 46.17ms | 2.1× | 2.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 19.25ms | 38.14ms | 2.0× | 3.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 29.48ms | 56.62ms | 1.9× | 3.8% | PASS |

</details>

</details>

## Version-control latency

Wall time: 4m 43s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 22.31ms | 38.00ms | 58.7% | 1.7% | PASS |
| `status_dirty_many_tables` | 24.43ms | 42.00ms | 58.2% | 1.1% | PASS |
| `diff_regular_working_one_table` | 20.90ms | 33.00ms | 63.3% | 0.9% | PASS |
| `diff_regular_working_many_tables` | 27.17ms | 50.00ms | 54.3% | 1.7% | PASS |
| `diff_stat_working_many_tables` | 26.88ms | 48.00ms | 56.0% | 1.2% | PASS |
| `diff_schema_working_many_tables` | 27.27ms | 48.00ms | 56.8% | 1.1% | PASS |
| `branch_list_many_branches` | 14.78ms | 25.00ms | 59.1% | 0.3% | PASS |
| `branch_create_delete` | 16.03ms | 27.00ms | 59.4% | 0.4% | PASS |
| `at_literal_deep_history` | 15.69ms | 28.00ms | 56.0% | 0.4% | PASS |
| `diff_literal_deep_history` | 15.78ms | 28.00ms | 56.4% | 0.3% | PASS |
| `history_literal_deep_history` | 16.40ms | 30.00ms | 54.7% | 0.4% | PASS |
| `checkout_branch_clean` | 21.62ms | 43.00ms | 50.3% | 0.8% | PASS |
| `merge_data_no_conflicts` | 17.53ms | 33.00ms | 53.1% | 0.4% | PASS |
| `merge_data_secondary_index` | 588.37ms | 944.00ms | 62.3% | 1.1% | PASS |
| `merge_schema_no_conflicts` | 14.49ms | 24.00ms | 60.4% | 1.2% | PASS |
| `merge_data_conflicts` | 20.21ms | 33.00ms | 61.3% | 0.7% | PASS |
| `merge_data_conflicts_with_resolve` | 20.69ms | 34.00ms | 60.8% | 0.5% | PASS |

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

candidate: 97f35b1.

Fixture file bytes: base 107,968,301; delta 115,879,032.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 47.721 | 46.277–62.344 | 44.338 | 2095.51 | 107968301 |
| push_initial | candidate | 339.610 | 337.935–344.726 | 338.168 | 294.46 | 107956807 |
| clone_initial | candidate | 460.358 | 454.928–462.232 | 447.060 | 217.22 | 108475105 |
| push_noop | candidate | 0.378 | 0.368–0.422 | 0.182 | — | 0 |
| fetch_noop | candidate | 0.102 | 0.092–0.111 | 0.104 | — | 0 |
| push_incremental | candidate | 136.046 | 135.791–138.446 | 134.949 | 7.35 | 7909775 |
| fetch_incremental | candidate | 59.062 | 57.866–59.338 | 57.760 | 16.93 | 7910084 |
| pull_fast_forward | candidate | 52.386 | 52.089–52.887 | 51.403 | 19.09 | 7910844 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 90.50 | 175.487 | 108.763 | 18329 | 2066 |
| clone_initial | candidate | 90.81 | 295.111 | 110.833 | 47250 | 2353 |
| push_noop | candidate | 41.54 | 0.073 | 0.000 | 27 | 2 |
| fetch_noop | candidate | 41.54 | 0.135 | 0.000 | 42 | 0 |
| push_incremental | candidate | 41.54 | 96.887 | 7.910 | 22418 | 125 |
| fetch_incremental | candidate | 41.54 | 34.472 | 7.910 | 10810 | 124 |
| pull_fast_forward | candidate | 41.54 | 26.497 | 8.959 | 7032 | 142 |

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

candidate: 97f35b1.

Fixture file bytes: base 1,087,718,066; delta 1,103,525,194.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 3354.358 | 3204.416–4113.783 | 506.402 | 298.12 | 1087718066 |
| push_initial | candidate | 6383.953 | 6354.374–6454.536 | 3720.019 | 156.64 | 1079566310 |
| clone_initial | candidate | 5277.412 | 5260.412–5357.519 | 5247.781 | 189.49 | 1102407396 |
| push_noop | candidate | 0.466 | 0.427–0.472 | 0.198 | — | 0 |
| fetch_noop | candidate | 0.106 | 0.095–0.121 | 0.107 | — | 0 |
| push_incremental | candidate | 769.854 | 769.485–773.610 | 767.164 | 1.30 | 15806003 |
| fetch_incremental | candidate | 197.477 | 192.991–197.862 | 193.859 | 5.06 | 15806513 |
| pull_fast_forward | candidate | 199.989 | 198.444–204.577 | 195.914 | 5.00 | 15807273 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 134.60 | 2091.816 | 1087.622 | 263483 | 20541 |
| clone_initial | candidate | 123.99 | 3516.535 | 1133.868 | 611945 | 32259 |
| push_noop | candidate | 44.13 | 0.078 | 0.000 | 28 | 2 |
| fetch_noop | candidate | 44.13 | 0.137 | 0.000 | 42 | 0 |
| push_incremental | candidate | 44.86 | 616.125 | 15.928 | 149625 | 313 |
| fetch_incremental | candidate | 50.30 | 62.917 | 15.928 | 14064 | 313 |
| pull_fast_forward | candidate | 50.30 | 62.849 | 16.978 | 14059 | 331 |

</details>


</details>

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh`, `test/vc_perf_ceiling.sh`, and `test/remote_perf.py`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
