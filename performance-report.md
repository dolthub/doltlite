# DoltLite Performance Report

> Nightly result: **FAIL**
>
> Generated: 2026-11-28 11:18 UTC
>
> Commit: [`6f464ee2be559e9889ca7cad64795bde223df84c`](https://github.com/dolthub/doltlite/commit/6f464ee2be559e9889ca7cad64795bde223df84c)
>
> Runner: ubuntu24 20261122.23.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/46130909493)

The SQL workloads compare optimized DoltLite against stock SQLite on the same GitHub-hosted runner, alternating execution order on each repetition. Version-control sections measure DoltLite latency. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class. Mean multiple is the average over workloads of each workload's paired DoltLite/SQLite ratio, so every workload counts equally whatever its run time.

### In-memory

| Operation | Mean multiple | Paired-ratio noise | Result |
|---|---:|---:|---|
| Reads | 1.13× | 1.2% | **PASS** |
| Writes | 1.64× | 1.1% | **PASS** |

### File-backed

| Operation | Mean multiple | Paired-ratio noise | Result |
|---|---:|---:|---|
| Reads | 0.90× | 1.1% | **PASS** |
| Writes | 1.25× | 1.8% | **PASS** |
| Autocommit writes | 2.17× | 4.5% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.8× for a section average. Durable autocommit writes use 3.6× and 2.6× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | Mean multiple | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 1.15× | 1.3% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 1.07× | 1.2% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 1.12× | 1.2% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 1.18× | 1.0% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 1.64× | 1.2% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 1.66× | 1.4% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 1.68× | 1.1% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 1.59× | 1.0% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 0.83× | 1.3% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 0.80× | 1.1% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 0.95× | 1.1% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 1.01× | 0.9% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 1.24× | 1.7% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 0.98× | 2.9% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 1.43× | 1.3% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 1.37× | 1.5% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 1.07× | 1.4% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 0.98× | 1.3% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 1.06× | 1.0% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 1.13× | 1.0% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 1.98× | 4.8% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 2.47× | 5.3% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 2.14× | 1.8% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 2.10× | 2.7% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 23.66ms | 28.00ms | 1.2× | 1.2% | PASS |
| mem_reads | `oltp_range_select` | 10.28ms | 11.72ms | 1.1× | 1.8% | PASS |
| mem_reads | `oltp_sum_range` | 9.34ms | 11.03ms | 1.2× | 1.3% | PASS |
| mem_reads | `oltp_order_range` | 2.54ms | 2.80ms | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_distinct_range` | 3.67ms | 3.94ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 3.83ms | 5.06ms | 1.3× | 1.3% | PASS |
| mem_reads | `select_random_points` | 12.92ms | 11.34ms | 0.9× | 2.3% | PASS |
| mem_reads | `select_random_ranges` | 4.50ms | 4.88ms | 1.1× | 1.5% | PASS |
| mem_reads | `covering_index_scan` | 7.59ms | 10.96ms | 1.4× | 1.2% | PASS |
| mem_reads | `groupby_scan` | 29.96ms | 32.04ms | 1.1× | 0.7% | PASS |
| mem_reads | `index_join` | 5.71ms | 8.14ms | 1.4× | 1.7% | PASS |
| mem_reads | `index_join_scan` | 3.21ms | 5.37ms | 1.7× | 1.9% | PASS |
| mem_reads | `types_table_scan` | 1.02s | 773.69ms | 0.8× | 0.5% | PASS |
| mem_reads | `table_scan` | 1.14s | 890.59ms | 0.8× | 0.6% | PASS |
| mem_reads | `oltp_read_only` | 103.30ms | 115.89ms | 1.1× | 1.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 176.46ms | 265.60ms | 1.5× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 15.00ms | 26.23ms | 1.7× | 1.2% | PASS |
| mem_writes | `oltp_update_index` | 49.35ms | 90.85ms | 1.8× | 1.3% | PASS |
| mem_writes | `oltp_update_non_index` | 34.91ms | 55.05ms | 1.6× | 1.5% | PASS |
| mem_writes | `oltp_delete_insert` | 44.56ms | 73.11ms | 1.6× | 1.3% | PASS |
| mem_writes | `oltp_write_only` | 21.99ms | 43.73ms | 2.0× | 1.0% | PASS |
| mem_writes | `types_delete_insert` | 24.70ms | 31.71ms | 1.3× | 0.8% | PASS |
| mem_writes | `oltp_read_write` | 66.49ms | 102.29ms | 1.5× | 1.4% | PASS |
| file_reads | `oltp_point_select` | 87.67ms | 44.68ms | 0.5× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 17.14ms | 13.50ms | 0.8× | 1.4% | PASS |
| file_reads | `oltp_sum_range` | 16.23ms | 12.85ms | 0.8× | 1.3% | PASS |
| file_reads | `oltp_order_range` | 3.39ms | 3.08ms | 0.9× | 1.8% | PASS |
| file_reads | `oltp_distinct_range` | 4.48ms | 4.22ms | 0.9× | 0.9% | PASS |
| file_reads | `oltp_index_scan` | 10.56ms | 7.05ms | 0.7× | 2.1% | PASS |
| file_reads | `select_random_points` | 20.60ms | 13.23ms | 0.6× | 1.7% | PASS |
| file_reads | `select_random_ranges` | 11.20ms | 6.73ms | 0.6× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 14.55ms | 13.04ms | 0.9× | 1.5% | PASS |
| file_reads | `groupby_scan` | 30.92ms | 32.39ms | 1.0× | 0.8% | PASS |
| file_reads | `index_join` | 9.57ms | 9.64ms | 1.0× | 2.2% | PASS |
| file_reads | `index_join_scan` | 4.15ms | 5.66ms | 1.4× | 1.4% | PASS |
| file_reads | `types_table_scan` | 1.02s | 777.32ms | 0.8× | 0.5% | PASS |
| file_reads | `table_scan` | 1.14s | 888.69ms | 0.8× | 0.5% | PASS |
| file_reads | `oltp_read_only` | 198.11ms | 140.97ms | 0.7× | 1.3% | PASS |
| file_writes | `oltp_bulk_insert` | 192.32ms | 279.31ms | 1.5× | 0.9% | PASS |
| file_writes | `oltp_insert` | 22.00ms | 29.88ms | 1.4× | 1.4% | PASS |
| file_writes | `oltp_update_index` | 76.62ms | 104.63ms | 1.4× | 1.8% | PASS |
| file_writes | `oltp_update_non_index` | 57.77ms | 61.85ms | 1.1× | 2.3% | PASS |
| file_writes | `oltp_delete_insert` | 68.04ms | 82.61ms | 1.2× | 1.8% | PASS |
| file_writes | `oltp_write_only` | 44.60ms | 54.56ms | 1.2× | 1.6% | PASS |
| file_writes | `types_delete_insert` | 40.08ms | 38.77ms | 1.0× | 1.6% | PASS |
| file_writes | `oltp_read_write` | 92.10ms | 112.75ms | 1.2× | 2.2% | PASS |
| ac_reads | `oltp_point_select` | 45.18ms | 44.61ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_range_select` | 12.71ms | 13.49ms | 1.1× | 1.7% | PASS |
| ac_reads | `oltp_sum_range` | 11.86ms | 12.95ms | 1.1× | 1.8% | PASS |
| ac_reads | `oltp_order_range` | 2.92ms | 3.08ms | 1.1× | 2.2% | PASS |
| ac_reads | `oltp_distinct_range` | 4.00ms | 4.22ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_index_scan` | 6.18ms | 7.07ms | 1.1× | 1.4% | PASS |
| ac_reads | `select_random_points` | 15.60ms | 13.18ms | 0.8× | 1.8% | PASS |
| ac_reads | `select_random_ranges` | 6.85ms | 6.72ms | 1.0× | 1.1% | PASS |
| ac_reads | `covering_index_scan` | 10.03ms | 13.00ms | 1.3× | 1.4% | PASS |
| ac_reads | `groupby_scan` | 30.25ms | 32.38ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 7.16ms | 9.65ms | 1.3× | 1.4% | PASS |
| ac_reads | `index_join_scan` | 3.67ms | 5.68ms | 1.5× | 1.4% | PASS |
| ac_reads | `types_table_scan` | 1.02s | 776.09ms | 0.8× | 0.6% | PASS |
| ac_reads | `table_scan` | 1.14s | 890.00ms | 0.8× | 0.8% | PASS |
| ac_reads | `oltp_read_only` | 134.72ms | 140.99ms | 1.0× | 1.1% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 20.81ms | 33.30ms | 1.6× | 5.3% | PASS |
| ac_writes | `oltp_insert_ac` | 23.26ms | 44.09ms | 1.9× | 4.8% | PASS |
| ac_writes | `oltp_update_index_ac` | 25.48ms | 61.15ms | 2.4× | 5.9% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.26ms | 40.08ms | 1.8× | 4.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 23.18ms | 49.54ms | 2.1× | 4.5% | PASS |
| ac_writes | `oltp_write_only_ac` | 24.19ms | 49.56ms | 2.0× | 4.7% | PASS |
| ac_writes | `types_delete_insert_ac` | 20.72ms | 39.62ms | 1.9× | 4.8% | PASS |
| ac_writes | `oltp_read_write_ac` | 28.95ms | 56.08ms | 1.9× | 6.0% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 28.01ms | 28.82ms | 1.0× | 1.5% | PASS |
| mem_reads | `oltp_range_select` | 13.44ms | 12.69ms | 0.9× | 1.8% | PASS |
| mem_reads | `oltp_sum_range` | 12.25ms | 12.23ms | 1.0× | 0.9% | PASS |
| mem_reads | `oltp_order_range` | 2.88ms | 2.83ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 3.74ms | 3.70ms | 1.0× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 3.23ms | 5.04ms | 1.6× | 1.4% | PASS |
| mem_reads | `select_random_points` | 18.09ms | 16.84ms | 0.9× | 1.8% | PASS |
| mem_reads | `select_random_ranges` | 5.32ms | 4.99ms | 0.9× | 0.9% | PASS |
| mem_reads | `covering_index_scan` | 5.99ms | 8.29ms | 1.4× | 0.7% | PASS |
| mem_reads | `groupby_scan` | 28.78ms | 29.04ms | 1.0× | 0.6% | PASS |
| mem_reads | `index_join` | 8.69ms | 7.47ms | 0.9× | 1.2% | PASS |
| mem_reads | `index_join_scan` | 3.07ms | 5.33ms | 1.7× | 1.2% | PASS |
| mem_reads | `types_table_scan` | 907.87ms | 751.66ms | 0.8× | 3.0% | PASS |
| mem_reads | `table_scan` | 990.76ms | 813.94ms | 0.8× | 1.2% | PASS |
| mem_reads | `oltp_read_only` | 107.81ms | 108.73ms | 1.0× | 0.7% | PASS |
| mem_writes | `oltp_bulk_insert` | 190.65ms | 257.79ms | 1.4× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 14.32ms | 29.96ms | 2.1× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 57.12ms | 113.38ms | 2.0× | 1.5% | PASS |
| mem_writes | `oltp_update_non_index` | 41.43ms | 67.20ms | 1.6× | 2.7% | PASS |
| mem_writes | `oltp_delete_insert` | 46.83ms | 80.28ms | 1.7× | 1.4% | PASS |
| mem_writes | `oltp_write_only` | 24.83ms | 48.62ms | 2.0× | 1.4% | PASS |
| mem_writes | `types_delete_insert` | 31.37ms | 37.93ms | 1.2× | 1.5% | PASS |
| mem_writes | `oltp_read_write` | 73.72ms | 104.22ms | 1.4× | 0.8% | PASS |
| file_reads | `oltp_point_select` | 91.35ms | 45.06ms | 0.5× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 20.63ms | 14.38ms | 0.7× | 1.2% | PASS |
| file_reads | `oltp_sum_range` | 19.13ms | 13.95ms | 0.7× | 0.7% | PASS |
| file_reads | `oltp_order_range` | 3.56ms | 3.05ms | 0.9× | 1.1% | PASS |
| file_reads | `oltp_distinct_range` | 4.42ms | 3.90ms | 0.9× | 1.1% | PASS |
| file_reads | `oltp_index_scan` | 9.64ms | 6.78ms | 0.7× | 0.8% | PASS |
| file_reads | `select_random_points` | 24.82ms | 18.48ms | 0.7× | 1.0% | PASS |
| file_reads | `select_random_ranges` | 11.95ms | 6.79ms | 0.6× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 12.56ms | 10.34ms | 0.8× | 0.5% | PASS |
| file_reads | `groupby_scan` | 29.77ms | 29.37ms | 1.0× | 0.9% | PASS |
| file_reads | `index_join` | 12.83ms | 9.13ms | 0.7× | 1.3% | PASS |
| file_reads | `index_join_scan` | 3.92ms | 5.69ms | 1.5× | 1.1% | PASS |
| file_reads | `types_table_scan` | 881.29ms | 734.95ms | 0.8× | 0.8% | PASS |
| file_reads | `table_scan` | 1.02s | 814.94ms | 0.8× | 2.3% | PASS |
| file_reads | `oltp_read_only` | 200.12ms | 132.05ms | 0.7× | 1.1% | PASS |
| file_writes | `oltp_bulk_insert` | 277.11ms | 336.07ms | 1.2× | 2.8% | PASS |
| file_writes | `oltp_insert` | 33.67ms | 52.60ms | 1.6× | 6.8% | PASS |
| file_writes | `oltp_update_index` | 177.66ms | 193.47ms | 1.1× | 2.3% | PASS |
| file_writes | `oltp_update_non_index` | 153.25ms | 130.19ms | 0.8× | 2.8% | PASS |
| file_writes | `oltp_delete_insert` | 187.97ms | 149.81ms | 0.8× | 1.9% | PASS |
| file_writes | `oltp_write_only` | 125.22ms | 105.41ms | 0.8× | 3.1% | PASS |
| file_writes | `types_delete_insert` | 151.62ms | 83.42ms | 0.6× | 5.4% | PASS |
| file_writes | `oltp_read_write` | 191.26ms | 167.42ms | 0.9× | 4.7% | PASS |
| ac_reads | `oltp_point_select` | 49.83ms | 45.02ms | 0.9× | 2.0% | PASS |
| ac_reads | `oltp_range_select` | 17.39ms | 14.77ms | 0.8× | 2.5% | PASS |
| ac_reads | `oltp_sum_range` | 15.33ms | 14.08ms | 0.9× | 1.4% | PASS |
| ac_reads | `oltp_order_range` | 3.24ms | 3.06ms | 0.9× | 0.7% | PASS |
| ac_reads | `oltp_distinct_range` | 4.06ms | 3.90ms | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_index_scan` | 5.66ms | 6.81ms | 1.2× | 1.0% | PASS |
| ac_reads | `select_random_points` | 21.05ms | 18.61ms | 0.9× | 1.3% | PASS |
| ac_reads | `select_random_ranges` | 8.05ms | 6.84ms | 0.8× | 1.1% | PASS |
| ac_reads | `covering_index_scan` | 8.57ms | 10.44ms | 1.2× | 0.8% | PASS |
| ac_reads | `groupby_scan` | 29.82ms | 29.50ms | 1.0× | 0.7% | PASS |
| ac_reads | `index_join` | 11.33ms | 9.47ms | 0.8× | 1.3% | PASS |
| ac_reads | `index_join_scan` | 3.64ms | 5.83ms | 1.6× | 1.6% | PASS |
| ac_reads | `types_table_scan` | 949.42ms | 767.60ms | 0.8× | 4.9% | PASS |
| ac_reads | `table_scan` | 1.16s | 856.10ms | 0.7× | 3.5% | PASS |
| ac_reads | `oltp_read_only` | 143.69ms | 135.01ms | 0.9× | 2.4% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 17.80ms | 26.56ms | 1.5× | 4.6% | PASS |
| ac_writes | `oltp_insert_ac` | 19.54ms | 40.67ms | 2.1× | 4.1% | PASS |
| ac_writes | `oltp_update_index_ac` | 20.36ms | 72.95ms | 3.6× | 5.0% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 16.97ms | 38.58ms | 2.3× | 5.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 19.69ms | 55.63ms | 2.8× | 6.0% | PASS |
| ac_writes | `oltp_write_only_ac` | 19.04ms | 52.47ms | 2.8× | 6.0% | PASS |
| ac_writes | `types_delete_insert_ac` | 17.56ms | 38.88ms | 2.2× | 7.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 24.26ms | 60.46ms | 2.5× | 4.5% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 25.49ms | 24.60ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_range_select` | 11.75ms | 12.21ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_sum_range` | 11.57ms | 11.18ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_order_range` | 2.44ms | 2.56ms | 1.0× | 1.3% | PASS |
| mem_reads | `oltp_distinct_range` | 3.10ms | 3.31ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 2.96ms | 4.78ms | 1.6× | 1.3% | PASS |
| mem_reads | `select_random_points` | 18.39ms | 17.04ms | 0.9× | 1.6% | PASS |
| mem_reads | `select_random_ranges` | 4.75ms | 4.55ms | 1.0× | 1.7% | PASS |
| mem_reads | `covering_index_scan` | 4.85ms | 7.55ms | 1.6× | 0.6% | PASS |
| mem_reads | `groupby_scan` | 23.40ms | 25.25ms | 1.1× | 0.4% | PASS |
| mem_reads | `index_join` | 9.92ms | 8.62ms | 0.9× | 1.5% | PASS |
| mem_reads | `index_join_scan` | 2.78ms | 5.68ms | 2.0× | 1.4% | PASS |
| mem_reads | `types_table_scan` | 773.35ms | 617.77ms | 0.8× | 0.5% | PASS |
| mem_reads | `table_scan` | 871.72ms | 706.64ms | 0.8× | 1.7% | PASS |
| mem_reads | `oltp_read_only` | 93.25ms | 97.98ms | 1.1× | 1.2% | PASS |
| mem_writes | `oltp_bulk_insert` | 146.23ms | 200.07ms | 1.4× | 0.5% | PASS |
| mem_writes | `oltp_insert` | 11.88ms | 24.48ms | 2.1× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 46.91ms | 94.10ms | 2.0× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 34.78ms | 55.90ms | 1.6× | 1.1% | PASS |
| mem_writes | `oltp_delete_insert` | 38.50ms | 69.51ms | 1.8× | 1.1% | PASS |
| mem_writes | `oltp_write_only` | 20.50ms | 40.58ms | 2.0× | 0.9% | PASS |
| mem_writes | `types_delete_insert` | 27.48ms | 33.97ms | 1.2× | 1.1% | PASS |
| mem_writes | `oltp_read_write` | 71.44ms | 97.42ms | 1.4× | 1.3% | PASS |
| file_reads | `oltp_point_select` | 46.16ms | 29.20ms | 0.6× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 13.42ms | 12.59ms | 0.9× | 1.1% | PASS |
| file_reads | `oltp_sum_range` | 13.20ms | 11.70ms | 0.9× | 0.9% | PASS |
| file_reads | `oltp_order_range` | 2.62ms | 2.56ms | 1.0× | 1.5% | PASS |
| file_reads | `oltp_distinct_range` | 3.25ms | 3.27ms | 1.0× | 1.0% | PASS |
| file_reads | `oltp_index_scan` | 4.95ms | 4.99ms | 1.0× | 1.1% | PASS |
| file_reads | `select_random_points` | 20.00ms | 16.57ms | 0.8× | 1.6% | PASS |
| file_reads | `select_random_ranges` | 6.75ms | 4.84ms | 0.7× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 6.97ms | 7.96ms | 1.1× | 0.7% | PASS |
| file_reads | `groupby_scan` | 23.44ms | 25.14ms | 1.1× | 0.5% | PASS |
| file_reads | `index_join` | 10.60ms | 8.53ms | 0.8× | 1.7% | PASS |
| file_reads | `index_join_scan` | 3.04ms | 5.57ms | 1.8× | 2.9% | PASS |
| file_reads | `types_table_scan` | 772.51ms | 617.09ms | 0.8× | 0.4% | PASS |
| file_reads | `table_scan` | 882.33ms | 709.80ms | 0.8× | 1.6% | PASS |
| file_reads | `oltp_read_only` | 129.70ms | 108.53ms | 0.8× | 0.7% | PASS |
| file_writes | `oltp_bulk_insert` | 158.74ms | 212.33ms | 1.3× | 0.9% | PASS |
| file_writes | `oltp_insert` | 15.45ms | 28.97ms | 1.9× | 1.7% | PASS |
| file_writes | `oltp_update_index` | 63.86ms | 108.47ms | 1.7× | 1.4% | PASS |
| file_writes | `oltp_update_non_index` | 48.56ms | 66.26ms | 1.4× | 1.8% | PASS |
| file_writes | `oltp_delete_insert` | 55.01ms | 80.77ms | 1.5× | 1.1% | PASS |
| file_writes | `oltp_write_only` | 34.45ms | 50.14ms | 1.5× | 1.3% | PASS |
| file_writes | `types_delete_insert` | 40.35ms | 40.49ms | 1.0× | 2.4% | PASS |
| file_writes | `oltp_read_write` | 83.21ms | 102.80ms | 1.2× | 1.2% | PASS |
| ac_reads | `oltp_point_select` | 32.21ms | 29.30ms | 0.9× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 12.82ms | 12.83ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_sum_range` | 12.71ms | 12.00ms | 0.9× | 0.7% | PASS |
| ac_reads | `oltp_order_range` | 2.60ms | 2.65ms | 1.0× | 1.8% | PASS |
| ac_reads | `oltp_distinct_range` | 3.23ms | 3.35ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_index_scan` | 3.78ms | 5.16ms | 1.4× | 1.4% | PASS |
| ac_reads | `select_random_points` | 19.61ms | 17.09ms | 0.9× | 1.5% | PASS |
| ac_reads | `select_random_ranges` | 5.67ms | 5.00ms | 0.9× | 1.5% | PASS |
| ac_reads | `covering_index_scan` | 5.63ms | 8.06ms | 1.4× | 1.0% | PASS |
| ac_reads | `groupby_scan` | 23.52ms | 25.41ms | 1.1× | 0.4% | PASS |
| ac_reads | `index_join` | 10.36ms | 8.68ms | 0.8× | 1.2% | PASS |
| ac_reads | `index_join_scan` | 3.02ms | 5.68ms | 1.9× | 2.5% | PASS |
| ac_reads | `types_table_scan` | 776.19ms | 618.55ms | 0.8× | 0.6% | PASS |
| ac_reads | `table_scan` | 878.54ms | 705.45ms | 0.8× | 0.6% | PASS |
| ac_reads | `oltp_read_only` | 103.59ms | 104.05ms | 1.0× | 0.5% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 9.07ms | 12.54ms | 1.4× | 1.1% | PASS |
| ac_writes | `oltp_insert_ac` | 10.13ms | 22.52ms | 2.2× | 1.7% | PASS |
| ac_writes | `oltp_update_index_ac` | 10.51ms | 31.78ms | 3.0× | 1.9% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 8.74ms | 16.33ms | 1.9× | 1.2% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 10.01ms | 23.86ms | 2.4× | 1.7% | PASS |
| ac_writes | `oltp_write_only_ac` | 9.88ms | 23.40ms | 2.4× | 2.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 9.41ms | 17.04ms | 1.8× | 2.3% | PASS |
| ac_writes | `oltp_read_write_ac` | 14.51ms | 29.43ms | 2.0× | 4.9% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 21.20ms | 23.81ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_range_select` | 14.04ms | 16.25ms | 1.2× | 0.8% | PASS |
| mem_reads | `oltp_sum_range` | 13.77ms | 15.26ms | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_order_range` | 2.74ms | 2.98ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_distinct_range` | 3.43ms | 3.71ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 3.28ms | 4.51ms | 1.4× | 1.3% | PASS |
| mem_reads | `select_random_points` | 21.40ms | 22.05ms | 1.0× | 0.6% | PASS |
| mem_reads | `select_random_ranges` | 5.20ms | 6.03ms | 1.2× | 0.8% | PASS |
| mem_reads | `covering_index_scan` | 4.80ms | 7.40ms | 1.5× | 0.9% | PASS |
| mem_reads | `groupby_scan` | 25.33ms | 28.81ms | 1.1× | 0.3% | PASS |
| mem_reads | `index_join` | 5.87ms | 8.67ms | 1.5× | 1.2% | PASS |
| mem_reads | `index_join_scan` | 2.96ms | 5.07ms | 1.7× | 3.0% | PASS |
| mem_reads | `types_table_scan` | 780.38ms | 587.44ms | 0.8× | 0.6% | PASS |
| mem_reads | `table_scan` | 869.86ms | 692.21ms | 0.8× | 1.0% | PASS |
| mem_reads | `oltp_read_only` | 107.74ms | 119.78ms | 1.1× | 1.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 151.29ms | 193.32ms | 1.3× | 0.4% | PASS |
| mem_writes | `oltp_insert` | 12.34ms | 20.64ms | 1.7× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 44.08ms | 76.45ms | 1.7× | 1.5% | PASS |
| mem_writes | `oltp_update_non_index` | 31.87ms | 48.96ms | 1.5× | 0.8% | PASS |
| mem_writes | `oltp_delete_insert` | 32.58ms | 56.21ms | 1.7× | 1.1% | PASS |
| mem_writes | `oltp_write_only` | 18.17ms | 33.31ms | 1.8× | 1.5% | PASS |
| mem_writes | `types_delete_insert` | 20.80ms | 29.84ms | 1.4× | 1.1% | PASS |
| mem_writes | `oltp_read_write` | 64.82ms | 94.56ms | 1.5× | 1.0% | PASS |
| file_reads | `oltp_point_select` | 41.88ms | 29.04ms | 0.7× | 0.7% | PASS |
| file_reads | `oltp_range_select` | 16.43ms | 16.96ms | 1.0× | 0.9% | PASS |
| file_reads | `oltp_sum_range` | 15.84ms | 15.79ms | 1.0× | 1.2% | PASS |
| file_reads | `oltp_order_range` | 2.90ms | 3.03ms | 1.0× | 1.4% | PASS |
| file_reads | `oltp_distinct_range` | 3.59ms | 3.74ms | 1.0× | 2.6% | PASS |
| file_reads | `oltp_index_scan` | 5.33ms | 4.92ms | 0.9× | 1.4% | PASS |
| file_reads | `select_random_points` | 22.82ms | 21.99ms | 1.0× | 0.7% | PASS |
| file_reads | `select_random_ranges` | 7.21ms | 6.44ms | 0.9× | 0.7% | PASS |
| file_reads | `covering_index_scan` | 6.93ms | 7.87ms | 1.1× | 0.8% | PASS |
| file_reads | `groupby_scan` | 25.38ms | 28.69ms | 1.1× | 0.4% | PASS |
| file_reads | `index_join` | 6.99ms | 8.49ms | 1.2× | 1.3% | PASS |
| file_reads | `index_join_scan` | 3.16ms | 5.11ms | 1.6× | 2.7% | PASS |
| file_reads | `types_table_scan` | 771.81ms | 584.90ms | 0.8× | 0.3% | PASS |
| file_reads | `table_scan` | 883.72ms | 686.12ms | 0.8× | 1.1% | PASS |
| file_reads | `oltp_read_only` | 127.50ms | 122.21ms | 1.0× | 0.5% | PASS |
| file_writes | `oltp_bulk_insert` | 160.07ms | 199.95ms | 1.2× | 0.7% | PASS |
| file_writes | `oltp_insert` | 15.41ms | 23.51ms | 1.5× | 1.2% | PASS |
| file_writes | `oltp_update_index` | 59.00ms | 87.54ms | 1.5× | 1.6% | PASS |
| file_writes | `oltp_update_non_index` | 45.63ms | 56.90ms | 1.2× | 2.0% | PASS |
| file_writes | `oltp_delete_insert` | 46.98ms | 67.14ms | 1.4× | 1.4% | PASS |
| file_writes | `oltp_write_only` | 29.63ms | 42.75ms | 1.4× | 2.2% | PASS |
| file_writes | `types_delete_insert` | 29.12ms | 36.56ms | 1.3× | 1.7% | PASS |
| file_writes | `oltp_read_write` | 77.00ms | 103.01ms | 1.3× | 1.2% | PASS |
| ac_reads | `oltp_point_select` | 28.38ms | 29.05ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 14.91ms | 16.86ms | 1.1× | 1.0% | PASS |
| ac_reads | `oltp_sum_range` | 14.38ms | 15.63ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_order_range` | 2.79ms | 3.00ms | 1.1× | 1.8% | PASS |
| ac_reads | `oltp_distinct_range` | 3.49ms | 3.73ms | 1.1× | 0.7% | PASS |
| ac_reads | `oltp_index_scan` | 3.99ms | 4.84ms | 1.2× | 1.7% | PASS |
| ac_reads | `select_random_points` | 21.48ms | 22.05ms | 1.0× | 0.8% | PASS |
| ac_reads | `select_random_ranges` | 5.93ms | 6.58ms | 1.1× | 0.9% | PASS |
| ac_reads | `covering_index_scan` | 5.59ms | 7.96ms | 1.4× | 0.6% | PASS |
| ac_reads | `groupby_scan` | 25.23ms | 28.61ms | 1.1× | 0.3% | PASS |
| ac_reads | `index_join` | 6.41ms | 8.85ms | 1.4× | 1.0% | PASS |
| ac_reads | `index_join_scan` | 3.16ms | 5.22ms | 1.7× | 2.0% | PASS |
| ac_reads | `types_table_scan` | 777.62ms | 587.47ms | 0.8× | 0.5% | PASS |
| ac_reads | `table_scan` | 899.56ms | 695.41ms | 0.8× | 3.7% | PASS |
| ac_reads | `oltp_read_only` | 113.81ms | 124.11ms | 1.1× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 8.85ms | 12.96ms | 1.5× | 2.0% | PASS |
| ac_writes | `oltp_insert_ac` | 9.79ms | 20.65ms | 2.1× | 1.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 10.84ms | 29.34ms | 2.7× | 2.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 9.18ms | 17.09ms | 1.9× | 3.2% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 10.06ms | 23.94ms | 2.4× | 4.5% | PASS |
| ac_writes | `oltp_write_only_ac` | 9.95ms | 22.95ms | 2.3× | 3.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 8.77ms | 17.87ms | 2.0× | 5.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 13.37ms | 28.11ms | 2.1× | 1.7% | PASS |

</details>

</details>

## Version-control latency

Wall time: 21m 29s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 30.69ms | 38.00ms | 80.8% | 1.2% | PASS |
| `status_dirty_many_tables` | 33.95ms | 42.00ms | 80.8% | 0.9% | PASS |
| `diff_regular_working_one_table` | 28.89ms | 33.00ms | 87.6% | 1.1% | PASS |
| `diff_regular_working_many_tables` | 38.03ms | 50.00ms | 76.1% | 0.8% | PASS |
| `diff_stat_working_many_tables` | 38.25ms | 48.00ms | 79.7% | 1.0% | PASS |
| `diff_schema_working_many_tables` | 38.85ms | 48.00ms | 80.9% | 0.8% | PASS |
| `branch_list_many_branches` | 21.25ms | 25.00ms | 85.0% | 1.3% | PASS |
| `branch_create_delete` | 27.22ms | 27.00ms | 100.8% | 1.3% | FAIL |
| `at_literal_deep_history` | 22.89ms | 28.00ms | 81.7% | 1.2% | PASS |
| `diff_literal_deep_history` | 23.00ms | 28.00ms | 82.1% | 1.2% | PASS |
| `history_literal_deep_history` | 23.96ms | 30.00ms | 79.9% | 0.9% | PASS |
| `checkout_branch_clean` | 31.84ms | 43.00ms | 74.1% | 1.0% | PASS |
| `merge_data_no_conflicts` | 26.46ms | 33.00ms | 80.2% | 1.5% | PASS |
| `merge_data_secondary_index` | 880.16ms | 944.00ms | 93.2% | 0.6% | PASS |
| `merge_schema_no_conflicts` | 20.99ms | 24.00ms | 87.5% | 1.5% | PASS |
| `merge_data_conflicts` | 28.53ms | 33.00ms | 86.5% | 0.9% | PASS |
| `merge_data_conflicts_with_resolve` | 29.54ms | 34.00ms | 86.9% | 1.0% | PASS |

Version-control ceiling result: **FAIL**.

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

candidate: 6f464ee.

Fixture file bytes: base 107,968,301; delta 115,879,032.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 595.088 | 467.736–601.917 | 39.398 | 168.04 | 107968301 |
| push_initial | candidate | 800.906 | 798.062–802.508 | 375.161 | 124.86 | 107956807 |
| clone_initial | candidate | 731.969 | 730.358–733.668 | 307.864 | 136.62 | 108475105 |
| push_noop | candidate | 0.834 | 0.736–0.870 | 0.334 | — | 0 |
| fetch_noop | candidate | 0.103 | 0.097–0.130 | 0.105 | — | 0 |
| push_incremental | candidate | 83.663 | 82.048–93.835 | 52.167 | 11.95 | 7909775 |
| fetch_incremental | candidate | 93.456 | 91.169–94.589 | 61.911 | 10.70 | 7910084 |
| pull_fast_forward | candidate | 97.090 | 92.497–98.465 | 62.079 | 10.30 | 7910844 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 90.42 | 175.487 | 108.763 | 18329 | 2066 |
| clone_initial | candidate | 92.35 | 110.421 | 110.833 | 3037 | 2353 |
| push_noop | candidate | 41.73 | 0.073 | 0.000 | 27 | 2 |
| fetch_noop | candidate | 41.73 | 0.068 | 0.000 | 22 | 0 |
| push_incremental | candidate | 41.73 | 10.738 | 7.910 | 1374 | 125 |
| fetch_incremental | candidate | 41.73 | 26.494 | 7.910 | 7016 | 124 |
| pull_fast_forward | candidate | 41.73 | 26.497 | 8.959 | 7032 | 142 |

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

candidate: 6f464ee.

Fixture file bytes: base 1,087,718,066; delta 1,103,525,194.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 7676.210 | 7528.738–7901.996 | 307.423 | 130.27 | 1087718066 |
| push_initial | candidate | 8925.941 | 8373.351–12337.714 | 3637.541 | 112.03 | 1079566310 |
| clone_initial | candidate | 7023.994 | 6985.916–8132.276 | 3103.363 | 142.37 | 1102407396 |
| push_noop | candidate | 0.739 | 0.721–0.803 | 0.304 | — | 0 |
| fetch_noop | candidate | 0.129 | 0.123–0.135 | 0.130 | — | 0 |
| push_incremental | candidate | 274.543 | 270.189–276.446 | 199.027 | 3.64 | 15806003 |
| fetch_incremental | candidate | 279.081 | 277.415–282.017 | 202.592 | 3.58 | 15806513 |
| pull_fast_forward | candidate | 286.509 | 285.851–287.620 | 204.453 | 3.49 | 15807273 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 134.80 | 2091.523 | 1087.622 | 263411 | 20541 |
| clone_initial | candidate | 142.05 | 1123.473 | 1133.868 | 35325 | 32259 |
| push_noop | candidate | 44.47 | 0.078 | 0.000 | 28 | 2 |
| fetch_noop | candidate | 44.47 | 0.069 | 0.000 | 22 | 0 |
| push_incremental | candidate | 56.23 | 46.387 | 15.928 | 10029 | 313 |
| fetch_incremental | candidate | 50.56 | 61.971 | 15.928 | 13829 | 313 |
| pull_fast_forward | candidate | 50.57 | 61.974 | 16.978 | 13845 | 331 |

</details>


</details>

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh`, `test/vc_perf_ceiling.sh`, and `test/remote_perf.py`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
