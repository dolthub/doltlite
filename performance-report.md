# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-11-26 10:51 UTC
>
> Commit: [`e0ceed7d3069842e270a99781328357918afca5d`](https://github.com/dolthub/doltlite/commit/e0ceed7d3069842e270a99781328357918afca5d)
>
> Runner: ubuntu24 20261116.133.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/45792823149)

The SQL workloads compare optimized DoltLite against stock SQLite on the same GitHub-hosted runner, alternating execution order on each repetition. Version-control sections measure DoltLite latency. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class. Mean multiple is the average over workloads of each workload's paired DoltLite/SQLite ratio, so every workload counts equally whatever its run time.

### In-memory

| Operation | Mean multiple | Paired-ratio noise | Result |
|---|---:|---:|---|
| Reads | 1.11× | 1.3% | **PASS** |
| Writes | 1.63× | 1.0% | **PASS** |

### File-backed

| Operation | Mean multiple | Paired-ratio noise | Result |
|---|---:|---:|---|
| Reads | 0.95× | 1.2% | **PASS** |
| Writes | 1.38× | 1.6% | **PASS** |
| Autocommit writes | 2.09× | 1.4% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.8× for a section average. Durable autocommit writes use 3.6× and 2.6× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | Mean multiple | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 1.11× | 1.6% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 1.09× | 1.1% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 1.10× | 1.3% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 1.16× | 1.7% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 1.58× | 1.6% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 1.69× | 1.0% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 1.67× | 1.2% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 1.60× | 1.0% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 0.93× | 1.2% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 0.94× | 1.2% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 0.95× | 1.0% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 1.00× | 1.4% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 1.33× | 2.1% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.39× | 1.4% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 1.43× | 1.5% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 1.38× | 1.9% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 1.08× | 1.7% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 1.04× | 1.1% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 1.06× | 1.1% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 1.11× | 1.4% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 2.09× | 1.2% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 2.08× | 1.7% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 2.10× | 1.6% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 2.09× | 1.2% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 15.34ms | 17.37ms | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_range_select` | 7.58ms | 8.34ms | 1.1× | 2.0% | PASS |
| mem_reads | `oltp_sum_range` | 6.71ms | 7.55ms | 1.1× | 4.0% | PASS |
| mem_reads | `oltp_order_range` | 1.92ms | 2.01ms | 1.0× | 1.7% | PASS |
| mem_reads | `oltp_distinct_range` | 2.45ms | 2.62ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 2.58ms | 3.22ms | 1.2× | 2.1% | PASS |
| mem_reads | `select_random_points` | 8.89ms | 7.96ms | 0.9× | 4.3% | PASS |
| mem_reads | `select_random_ranges` | 2.87ms | 2.99ms | 1.0× | 3.1% | PASS |
| mem_reads | `covering_index_scan` | 4.76ms | 6.90ms | 1.4× | 0.3% | PASS |
| mem_reads | `groupby_scan` | 20.94ms | 21.70ms | 1.0× | 1.0% | PASS |
| mem_reads | `index_join` | 3.98ms | 5.53ms | 1.4× | 1.6% | PASS |
| mem_reads | `index_join_scan` | 2.05ms | 3.48ms | 1.7× | 2.6% | PASS |
| mem_reads | `types_table_scan` | 750.56ms | 501.76ms | 0.7× | 0.2% | PASS |
| mem_reads | `table_scan` | 851.99ms | 572.70ms | 0.7× | 0.3% | PASS |
| mem_reads | `oltp_read_only` | 67.93ms | 75.90ms | 1.1× | 0.7% | PASS |
| mem_writes | `oltp_bulk_insert` | 107.92ms | 157.43ms | 1.5× | 0.4% | PASS |
| mem_writes | `oltp_insert` | 9.63ms | 15.89ms | 1.7× | 0.5% | PASS |
| mem_writes | `oltp_update_index` | 32.44ms | 57.77ms | 1.8× | 2.0% | PASS |
| mem_writes | `oltp_update_non_index` | 20.82ms | 32.85ms | 1.6× | 1.8% | PASS |
| mem_writes | `oltp_delete_insert` | 29.13ms | 44.75ms | 1.5× | 0.9% | PASS |
| mem_writes | `oltp_write_only` | 13.91ms | 26.34ms | 1.9× | 2.9% | PASS |
| mem_writes | `types_delete_insert` | 15.52ms | 19.26ms | 1.2× | 1.4% | PASS |
| mem_writes | `oltp_read_write` | 41.68ms | 62.32ms | 1.5× | 2.0% | PASS |
| file_reads | `oltp_point_select` | 34.78ms | 21.30ms | 0.6× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 9.82ms | 8.98ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_sum_range` | 8.98ms | 8.45ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_order_range` | 2.18ms | 2.09ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_distinct_range` | 2.86ms | 2.77ms | 1.0× | 1.2% | PASS |
| file_reads | `oltp_index_scan` | 4.82ms | 4.01ms | 0.8× | 1.3% | PASS |
| file_reads | `select_random_points` | 12.42ms | 9.84ms | 0.8× | 1.3% | PASS |
| file_reads | `select_random_ranges` | 5.11ms | 3.61ms | 0.7× | 1.3% | PASS |
| file_reads | `covering_index_scan` | 6.88ms | 7.50ms | 1.1× | 0.4% | PASS |
| file_reads | `groupby_scan` | 21.98ms | 21.91ms | 1.0× | 0.7% | PASS |
| file_reads | `index_join` | 5.38ms | 6.41ms | 1.2× | 1.1% | PASS |
| file_reads | `index_join_scan` | 2.50ms | 4.23ms | 1.7× | 2.2% | PASS |
| file_reads | `types_table_scan` | 753.78ms | 503.69ms | 0.7× | 0.3% | PASS |
| file_reads | `table_scan` | 850.28ms | 571.96ms | 0.7× | 0.4% | PASS |
| file_reads | `oltp_read_only` | 97.32ms | 82.92ms | 0.9× | 0.5% | PASS |
| file_writes | `oltp_bulk_insert` | 116.67ms | 168.47ms | 1.4× | 0.7% | PASS |
| file_writes | `oltp_insert` | 12.79ms | 18.13ms | 1.4× | 1.5% | PASS |
| file_writes | `oltp_update_index` | 44.66ms | 65.63ms | 1.5× | 2.0% | PASS |
| file_writes | `oltp_update_non_index` | 31.30ms | 36.79ms | 1.2× | 3.2% | PASS |
| file_writes | `oltp_delete_insert` | 36.79ms | 48.74ms | 1.3× | 2.6% | PASS |
| file_writes | `oltp_write_only` | 22.66ms | 32.10ms | 1.4× | 2.8% | PASS |
| file_writes | `types_delete_insert` | 22.31ms | 23.14ms | 1.0× | 1.5% | PASS |
| file_writes | `oltp_read_write` | 50.88ms | 67.37ms | 1.3× | 2.3% | PASS |
| ac_reads | `oltp_point_select` | 20.99ms | 21.00ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_range_select` | 7.39ms | 8.63ms | 1.2× | 4.2% | PASS |
| ac_reads | `oltp_sum_range` | 6.69ms | 7.94ms | 1.2× | 3.8% | PASS |
| ac_reads | `oltp_order_range` | 1.86ms | 1.99ms | 1.1× | 2.0% | PASS |
| ac_reads | `oltp_distinct_range` | 2.50ms | 2.65ms | 1.1× | 1.5% | PASS |
| ac_reads | `oltp_index_scan` | 3.22ms | 3.63ms | 1.1× | 2.3% | PASS |
| ac_reads | `select_random_points` | 9.95ms | 8.70ms | 0.9× | 4.9% | PASS |
| ac_reads | `select_random_ranges` | 3.50ms | 3.39ms | 1.0× | 3.9% | PASS |
| ac_reads | `covering_index_scan` | 5.43ms | 7.33ms | 1.4× | 0.3% | PASS |
| ac_reads | `groupby_scan` | 21.41ms | 21.70ms | 1.0× | 1.7% | PASS |
| ac_reads | `index_join` | 4.37ms | 5.80ms | 1.3× | 1.7% | PASS |
| ac_reads | `index_join_scan` | 2.13ms | 3.48ms | 1.6× | 1.7% | PASS |
| ac_reads | `types_table_scan` | 752.93ms | 502.95ms | 0.7× | 0.2% | PASS |
| ac_reads | `table_scan` | 852.14ms | 571.72ms | 0.7× | 0.4% | PASS |
| ac_reads | `oltp_read_only` | 76.55ms | 81.80ms | 1.1× | 0.7% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 8.23ms | 12.38ms | 1.5× | 0.6% | PASS |
| ac_writes | `oltp_insert_ac` | 9.13ms | 18.43ms | 2.0× | 0.7% | PASS |
| ac_writes | `oltp_update_index_ac` | 9.73ms | 26.59ms | 2.7× | 1.4% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 8.33ms | 15.34ms | 1.8× | 1.4% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 8.95ms | 20.79ms | 2.3× | 0.9% | PASS |
| ac_writes | `oltp_write_only_ac` | 9.05ms | 20.34ms | 2.2× | 1.0% | PASS |
| ac_writes | `types_delete_insert_ac` | 8.12ms | 15.60ms | 1.9× | 1.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 11.26ms | 24.00ms | 2.1× | 1.8% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 25.40ms | 24.84ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_range_select` | 12.49ms | 12.65ms | 1.0× | 1.0% | PASS |
| mem_reads | `oltp_sum_range` | 12.44ms | 11.64ms | 0.9× | 1.1% | PASS |
| mem_reads | `oltp_order_range` | 2.51ms | 2.55ms | 1.0× | 1.0% | PASS |
| mem_reads | `oltp_distinct_range` | 3.19ms | 3.20ms | 1.0× | 0.7% | PASS |
| mem_reads | `oltp_index_scan` | 2.92ms | 4.68ms | 1.6× | 1.2% | PASS |
| mem_reads | `select_random_points` | 18.41ms | 17.11ms | 0.9× | 1.5% | PASS |
| mem_reads | `select_random_ranges` | 4.92ms | 4.61ms | 0.9× | 1.3% | PASS |
| mem_reads | `covering_index_scan` | 4.82ms | 7.27ms | 1.5× | 1.0% | PASS |
| mem_reads | `groupby_scan` | 23.92ms | 24.35ms | 1.0× | 0.4% | PASS |
| mem_reads | `index_join` | 9.57ms | 7.84ms | 0.8× | 1.7% | PASS |
| mem_reads | `index_join_scan` | 2.84ms | 5.73ms | 2.0× | 1.7% | PASS |
| mem_reads | `types_table_scan` | 780.85ms | 586.66ms | 0.8× | 0.6% | PASS |
| mem_reads | `table_scan` | 896.89ms | 667.59ms | 0.7× | 1.3% | PASS |
| mem_reads | `oltp_read_only` | 95.98ms | 98.58ms | 1.0× | 0.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 147.88ms | 204.03ms | 1.4× | 0.3% | PASS |
| mem_writes | `oltp_insert` | 11.69ms | 24.35ms | 2.1× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 48.38ms | 97.83ms | 2.0× | 1.3% | PASS |
| mem_writes | `oltp_update_non_index` | 35.32ms | 58.41ms | 1.7× | 1.1% | PASS |
| mem_writes | `oltp_delete_insert` | 40.28ms | 71.53ms | 1.8× | 1.2% | PASS |
| mem_writes | `oltp_write_only` | 21.14ms | 42.10ms | 2.0× | 1.1% | PASS |
| mem_writes | `types_delete_insert` | 28.93ms | 35.04ms | 1.2× | 0.6% | PASS |
| mem_writes | `oltp_read_write` | 69.65ms | 97.84ms | 1.4× | 0.9% | PASS |
| file_reads | `oltp_point_select` | 46.80ms | 30.47ms | 0.7× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 15.11ms | 13.54ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_sum_range` | 15.01ms | 12.43ms | 0.8× | 1.8% | PASS |
| file_reads | `oltp_order_range` | 2.78ms | 2.68ms | 1.0× | 1.8% | PASS |
| file_reads | `oltp_distinct_range` | 3.47ms | 3.31ms | 1.0× | 1.1% | PASS |
| file_reads | `oltp_index_scan` | 5.06ms | 5.29ms | 1.0× | 1.2% | PASS |
| file_reads | `select_random_points` | 21.08ms | 17.92ms | 0.9× | 0.7% | PASS |
| file_reads | `select_random_ranges` | 6.96ms | 5.11ms | 0.7× | 1.3% | PASS |
| file_reads | `covering_index_scan` | 6.96ms | 7.90ms | 1.1× | 0.7% | PASS |
| file_reads | `groupby_scan` | 23.91ms | 24.52ms | 1.0× | 0.5% | PASS |
| file_reads | `index_join` | 10.88ms | 8.31ms | 0.8× | 3.0% | PASS |
| file_reads | `index_join_scan` | 3.09ms | 5.71ms | 1.9× | 3.7% | PASS |
| file_reads | `types_table_scan` | 786.38ms | 587.82ms | 0.7× | 0.7% | PASS |
| file_reads | `table_scan` | 886.91ms | 669.86ms | 0.8× | 1.9% | PASS |
| file_reads | `oltp_read_only` | 138.16ms | 112.41ms | 0.8× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 164.17ms | 219.17ms | 1.3× | 0.7% | PASS |
| file_writes | `oltp_insert` | 15.64ms | 29.66ms | 1.9× | 1.2% | PASS |
| file_writes | `oltp_update_index` | 69.97ms | 115.29ms | 1.6× | 1.4% | PASS |
| file_writes | `oltp_update_non_index` | 54.26ms | 69.96ms | 1.3× | 2.4% | PASS |
| file_writes | `oltp_delete_insert` | 61.55ms | 84.28ms | 1.4× | 1.0% | PASS |
| file_writes | `oltp_write_only` | 37.51ms | 52.05ms | 1.4× | 1.5% | PASS |
| file_writes | `types_delete_insert` | 45.58ms | 43.06ms | 0.9× | 2.5% | PASS |
| file_writes | `oltp_read_write` | 87.33ms | 108.22ms | 1.2× | 1.4% | PASS |
| ac_reads | `oltp_point_select` | 33.48ms | 30.18ms | 0.9× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 13.51ms | 13.28ms | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_sum_range` | 13.76ms | 12.36ms | 0.9× | 1.0% | PASS |
| ac_reads | `oltp_order_range` | 2.71ms | 2.69ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_distinct_range` | 3.38ms | 3.31ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 3.89ms | 5.36ms | 1.4× | 1.7% | PASS |
| ac_reads | `select_random_points` | 20.87ms | 18.48ms | 0.9× | 1.1% | PASS |
| ac_reads | `select_random_ranges` | 5.94ms | 5.21ms | 0.9× | 1.5% | PASS |
| ac_reads | `covering_index_scan` | 5.69ms | 8.08ms | 1.4× | 0.5% | PASS |
| ac_reads | `groupby_scan` | 24.09ms | 24.66ms | 1.0× | 0.5% | PASS |
| ac_reads | `index_join` | 10.19ms | 8.16ms | 0.8× | 1.7% | PASS |
| ac_reads | `index_join_scan` | 3.07ms | 5.94ms | 1.9× | 2.0% | PASS |
| ac_reads | `types_table_scan` | 785.70ms | 589.39ms | 0.8× | 1.2% | PASS |
| ac_reads | `table_scan` | 874.81ms | 668.50ms | 0.8× | 0.9% | PASS |
| ac_reads | `oltp_read_only` | 106.21ms | 105.85ms | 1.0× | 1.4% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 9.30ms | 12.90ms | 1.4× | 1.0% | PASS |
| ac_writes | `oltp_insert_ac` | 10.41ms | 18.87ms | 1.8× | 1.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 10.91ms | 31.67ms | 2.9× | 3.3% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 9.06ms | 16.66ms | 1.8× | 1.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 10.21ms | 25.23ms | 2.5× | 1.9% | PASS |
| ac_writes | `oltp_write_only_ac` | 10.09ms | 23.57ms | 2.3× | 1.9% | PASS |
| ac_writes | `types_delete_insert_ac` | 9.49ms | 17.32ms | 1.8× | 1.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 14.09ms | 29.56ms | 2.1× | 2.0% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.61ms | 24.25ms | 1.0× | 0.9% | PASS |
| mem_reads | `oltp_range_select` | 11.32ms | 12.04ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_sum_range` | 11.18ms | 11.20ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_order_range` | 2.40ms | 2.50ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 3.04ms | 3.10ms | 1.0× | 1.4% | PASS |
| mem_reads | `oltp_index_scan` | 2.82ms | 4.28ms | 1.5× | 2.4% | PASS |
| mem_reads | `select_random_points` | 16.98ms | 15.96ms | 0.9× | 1.6% | PASS |
| mem_reads | `select_random_ranges` | 4.69ms | 4.51ms | 1.0× | 1.3% | PASS |
| mem_reads | `covering_index_scan` | 4.84ms | 7.43ms | 1.5× | 0.8% | PASS |
| mem_reads | `groupby_scan` | 23.46ms | 24.24ms | 1.0× | 0.4% | PASS |
| mem_reads | `index_join` | 9.49ms | 8.20ms | 0.9× | 1.3% | PASS |
| mem_reads | `index_join_scan` | 2.75ms | 5.53ms | 2.0× | 2.5% | PASS |
| mem_reads | `types_table_scan` | 770.77ms | 588.87ms | 0.8× | 0.5% | PASS |
| mem_reads | `table_scan` | 876.36ms | 659.00ms | 0.8× | 1.3% | PASS |
| mem_reads | `oltp_read_only` | 94.15ms | 97.73ms | 1.0× | 0.7% | PASS |
| mem_writes | `oltp_bulk_insert` | 145.66ms | 203.27ms | 1.4× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 11.88ms | 24.27ms | 2.0× | 0.6% | PASS |
| mem_writes | `oltp_update_index` | 44.72ms | 86.64ms | 1.9× | 1.7% | PASS |
| mem_writes | `oltp_update_non_index` | 31.48ms | 51.16ms | 1.6× | 1.6% | PASS |
| mem_writes | `oltp_delete_insert` | 36.27ms | 63.22ms | 1.7× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 19.54ms | 37.26ms | 1.9× | 1.7% | PASS |
| mem_writes | `types_delete_insert` | 26.05ms | 31.81ms | 1.2× | 1.4% | PASS |
| mem_writes | `oltp_read_write` | 61.78ms | 86.84ms | 1.4× | 1.0% | PASS |
| file_reads | `oltp_point_select` | 44.65ms | 28.32ms | 0.6× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 13.29ms | 12.57ms | 0.9× | 0.9% | PASS |
| file_reads | `oltp_sum_range` | 13.09ms | 11.73ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_order_range` | 2.62ms | 2.57ms | 1.0× | 1.1% | PASS |
| file_reads | `oltp_distinct_range` | 3.31ms | 3.25ms | 1.0× | 1.2% | PASS |
| file_reads | `oltp_index_scan` | 5.01ms | 5.15ms | 1.0× | 1.5% | PASS |
| file_reads | `select_random_points` | 20.16ms | 17.04ms | 0.8× | 1.0% | PASS |
| file_reads | `select_random_ranges` | 6.81ms | 4.94ms | 0.7× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 7.00ms | 7.91ms | 1.1× | 0.9% | PASS |
| file_reads | `groupby_scan` | 23.56ms | 24.36ms | 1.0× | 0.5% | PASS |
| file_reads | `index_join` | 10.69ms | 8.56ms | 0.8× | 2.0% | PASS |
| file_reads | `index_join_scan` | 3.02ms | 5.64ms | 1.9× | 2.9% | PASS |
| file_reads | `types_table_scan` | 772.22ms | 589.92ms | 0.8× | 0.4% | PASS |
| file_reads | `table_scan` | 878.24ms | 663.70ms | 0.8× | 0.5% | PASS |
| file_reads | `oltp_read_only` | 121.03ms | 102.38ms | 0.8× | 0.7% | PASS |
| file_writes | `oltp_bulk_insert` | 158.50ms | 214.49ms | 1.4× | 0.9% | PASS |
| file_writes | `oltp_insert` | 15.17ms | 28.07ms | 1.8× | 1.7% | PASS |
| file_writes | `oltp_update_index` | 61.70ms | 104.86ms | 1.7× | 2.0% | PASS |
| file_writes | `oltp_update_non_index` | 46.04ms | 63.41ms | 1.4× | 1.4% | PASS |
| file_writes | `oltp_delete_insert` | 53.16ms | 77.19ms | 1.5× | 1.6% | PASS |
| file_writes | `oltp_write_only` | 33.54ms | 49.09ms | 1.5× | 1.2% | PASS |
| file_writes | `types_delete_insert` | 38.88ms | 39.94ms | 1.0× | 2.1% | PASS |
| file_writes | `oltp_read_write` | 80.70ms | 102.46ms | 1.3× | 1.3% | PASS |
| ac_reads | `oltp_point_select` | 31.47ms | 28.92ms | 0.9× | 0.7% | PASS |
| ac_reads | `oltp_range_select` | 12.29ms | 12.62ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_sum_range` | 12.07ms | 11.65ms | 1.0× | 1.8% | PASS |
| ac_reads | `oltp_order_range` | 2.50ms | 2.56ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_distinct_range` | 3.18ms | 3.20ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 3.69ms | 5.08ms | 1.4× | 1.6% | PASS |
| ac_reads | `select_random_points` | 18.80ms | 17.10ms | 0.9× | 1.3% | PASS |
| ac_reads | `select_random_ranges` | 5.56ms | 4.95ms | 0.9× | 1.6% | PASS |
| ac_reads | `covering_index_scan` | 5.58ms | 7.95ms | 1.4× | 0.6% | PASS |
| ac_reads | `groupby_scan` | 23.40ms | 24.37ms | 1.0× | 0.5% | PASS |
| ac_reads | `index_join` | 10.19ms | 8.45ms | 0.8× | 1.5% | PASS |
| ac_reads | `index_join_scan` | 2.94ms | 5.62ms | 1.9× | 2.1% | PASS |
| ac_reads | `types_table_scan` | 769.70ms | 590.58ms | 0.8× | 0.3% | PASS |
| ac_reads | `table_scan` | 875.12ms | 662.44ms | 0.8× | 0.3% | PASS |
| ac_reads | `oltp_read_only` | 102.22ms | 102.46ms | 1.0× | 0.7% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 9.19ms | 12.67ms | 1.4× | 0.5% | PASS |
| ac_writes | `oltp_insert_ac` | 10.26ms | 21.90ms | 2.1× | 1.2% | PASS |
| ac_writes | `oltp_update_index_ac` | 10.51ms | 30.49ms | 2.9× | 1.9% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 8.83ms | 16.19ms | 1.8× | 1.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 10.11ms | 23.88ms | 2.4× | 2.2% | PASS |
| ac_writes | `oltp_write_only_ac` | 10.00ms | 22.96ms | 2.3× | 1.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 9.36ms | 17.03ms | 1.8× | 1.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 13.69ms | 28.30ms | 2.1× | 2.2% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 20.63ms | 22.65ms | 1.1× | 1.7% | PASS |
| mem_reads | `oltp_range_select` | 12.80ms | 15.46ms | 1.2× | 3.7% | PASS |
| mem_reads | `oltp_sum_range` | 12.18ms | 14.23ms | 1.2× | 4.1% | PASS |
| mem_reads | `oltp_order_range` | 2.51ms | 2.75ms | 1.1× | 1.8% | PASS |
| mem_reads | `oltp_distinct_range` | 3.07ms | 3.39ms | 1.1× | 2.0% | PASS |
| mem_reads | `oltp_index_scan` | 3.10ms | 3.96ms | 1.3× | 1.8% | PASS |
| mem_reads | `select_random_points` | 20.35ms | 20.94ms | 1.0× | 1.1% | PASS |
| mem_reads | `select_random_ranges` | 4.99ms | 5.65ms | 1.1× | 1.3% | PASS |
| mem_reads | `covering_index_scan` | 4.79ms | 7.12ms | 1.5× | 0.7% | PASS |
| mem_reads | `groupby_scan` | 24.93ms | 27.23ms | 1.1× | 0.4% | PASS |
| mem_reads | `index_join` | 5.50ms | 7.54ms | 1.4× | 2.2% | PASS |
| mem_reads | `index_join_scan` | 2.50ms | 4.06ms | 1.6× | 2.6% | PASS |
| mem_reads | `types_table_scan` | 769.96ms | 564.08ms | 0.7× | 0.4% | PASS |
| mem_reads | `table_scan` | 871.18ms | 648.08ms | 0.7× | 1.3% | PASS |
| mem_reads | `oltp_read_only` | 95.64ms | 110.91ms | 1.2× | 0.5% | PASS |
| mem_writes | `oltp_bulk_insert` | 149.78ms | 190.84ms | 1.3× | 0.4% | PASS |
| mem_writes | `oltp_insert` | 12.17ms | 20.32ms | 1.7× | 0.6% | PASS |
| mem_writes | `oltp_update_index` | 43.07ms | 76.13ms | 1.8× | 1.7% | PASS |
| mem_writes | `oltp_update_non_index` | 31.50ms | 48.82ms | 1.6× | 1.7% | PASS |
| mem_writes | `oltp_delete_insert` | 33.24ms | 57.37ms | 1.7× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 18.59ms | 34.30ms | 1.8× | 0.9% | PASS |
| mem_writes | `types_delete_insert` | 21.20ms | 30.97ms | 1.5× | 0.8% | PASS |
| mem_writes | `oltp_read_write` | 63.22ms | 90.80ms | 1.4× | 1.2% | PASS |
| file_reads | `oltp_point_select` | 40.30ms | 27.64ms | 0.7× | 1.1% | PASS |
| file_reads | `oltp_range_select` | 15.05ms | 16.01ms | 1.1× | 3.0% | PASS |
| file_reads | `oltp_sum_range` | 13.82ms | 14.69ms | 1.1× | 3.2% | PASS |
| file_reads | `oltp_order_range` | 2.65ms | 2.79ms | 1.1× | 2.6% | PASS |
| file_reads | `oltp_distinct_range` | 3.29ms | 3.46ms | 1.1× | 1.4% | PASS |
| file_reads | `oltp_index_scan` | 5.06ms | 4.39ms | 0.9× | 2.0% | PASS |
| file_reads | `select_random_points` | 21.75ms | 20.80ms | 1.0× | 1.8% | PASS |
| file_reads | `select_random_ranges` | 6.91ms | 5.98ms | 0.9× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 6.85ms | 7.53ms | 1.1× | 0.4% | PASS |
| file_reads | `groupby_scan` | 25.21ms | 27.30ms | 1.1× | 0.4% | PASS |
| file_reads | `index_join` | 6.66ms | 7.89ms | 1.2× | 1.4% | PASS |
| file_reads | `index_join_scan` | 2.87ms | 4.75ms | 1.7× | 2.7% | PASS |
| file_reads | `types_table_scan` | 770.91ms | 565.92ms | 0.7× | 0.4% | PASS |
| file_reads | `table_scan` | 866.46ms | 648.47ms | 0.7× | 1.5% | PASS |
| file_reads | `oltp_read_only` | 126.06ms | 118.45ms | 0.9× | 0.5% | PASS |
| file_writes | `oltp_bulk_insert` | 160.29ms | 203.16ms | 1.3× | 0.7% | PASS |
| file_writes | `oltp_insert` | 15.62ms | 24.19ms | 1.5× | 1.2% | PASS |
| file_writes | `oltp_update_index` | 58.17ms | 87.17ms | 1.5× | 1.8% | PASS |
| file_writes | `oltp_update_non_index` | 43.87ms | 55.45ms | 1.3× | 2.4% | PASS |
| file_writes | `oltp_delete_insert` | 44.78ms | 63.02ms | 1.4× | 2.0% | PASS |
| file_writes | `oltp_write_only` | 28.12ms | 40.62ms | 1.4× | 2.4% | PASS |
| file_writes | `types_delete_insert` | 28.86ms | 35.89ms | 1.2× | 2.3% | PASS |
| file_writes | `oltp_read_write` | 75.18ms | 99.83ms | 1.3× | 1.3% | PASS |
| ac_reads | `oltp_point_select` | 27.37ms | 28.19ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_range_select` | 14.46ms | 16.42ms | 1.1× | 1.5% | PASS |
| ac_reads | `oltp_sum_range` | 13.63ms | 15.12ms | 1.1× | 1.8% | PASS |
| ac_reads | `oltp_order_range` | 2.67ms | 2.82ms | 1.1× | 1.7% | PASS |
| ac_reads | `oltp_distinct_range` | 3.37ms | 3.52ms | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_index_scan` | 3.91ms | 4.62ms | 1.2× | 2.1% | PASS |
| ac_reads | `select_random_points` | 21.18ms | 21.75ms | 1.0× | 1.4% | PASS |
| ac_reads | `select_random_ranges` | 5.70ms | 6.23ms | 1.1× | 1.6% | PASS |
| ac_reads | `covering_index_scan` | 5.49ms | 7.65ms | 1.4× | 0.6% | PASS |
| ac_reads | `groupby_scan` | 25.05ms | 27.31ms | 1.1× | 0.4% | PASS |
| ac_reads | `index_join` | 5.89ms | 7.73ms | 1.3× | 1.0% | PASS |
| ac_reads | `index_join_scan` | 2.65ms | 4.21ms | 1.6× | 2.7% | PASS |
| ac_reads | `types_table_scan` | 770.15ms | 564.90ms | 0.7× | 0.4% | PASS |
| ac_reads | `table_scan` | 885.00ms | 647.79ms | 0.7× | 1.0% | PASS |
| ac_reads | `oltp_read_only` | 105.80ms | 117.78ms | 1.1× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 8.46ms | 12.26ms | 1.4× | 1.0% | PASS |
| ac_writes | `oltp_insert_ac` | 9.44ms | 19.96ms | 2.1× | 1.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 10.09ms | 27.57ms | 2.7× | 1.8% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 8.62ms | 15.71ms | 1.8× | 1.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 9.23ms | 21.51ms | 2.3× | 1.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 9.34ms | 20.84ms | 2.2× | 0.9% | PASS |
| ac_writes | `types_delete_insert_ac` | 8.29ms | 16.21ms | 2.0× | 1.3% | PASS |
| ac_writes | `oltp_read_write_ac` | 12.44ms | 26.17ms | 2.1× | 1.5% | PASS |

</details>

</details>

## Version-control latency

Wall time: 6m 40s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 26.24ms | 38.00ms | 69.1% | 1.8% | PASS |
| `status_dirty_many_tables` | 29.20ms | 42.00ms | 69.5% | 1.2% | PASS |
| `diff_regular_working_one_table` | 25.44ms | 33.00ms | 77.1% | 1.7% | PASS |
| `diff_regular_working_many_tables` | 34.08ms | 50.00ms | 68.2% | 1.5% | PASS |
| `diff_stat_working_many_tables` | 32.98ms | 48.00ms | 68.7% | 1.1% | PASS |
| `diff_schema_working_many_tables` | 33.27ms | 48.00ms | 69.3% | 1.5% | PASS |
| `branch_list_many_branches` | 19.90ms | 25.00ms | 79.6% | 1.8% | PASS |
| `branch_create_delete` | 22.82ms | 27.00ms | 84.5% | 1.7% | PASS |
| `at_literal_deep_history` | 21.60ms | 28.00ms | 77.1% | 1.4% | PASS |
| `diff_literal_deep_history` | 21.58ms | 28.00ms | 77.1% | 2.3% | PASS |
| `history_literal_deep_history` | 22.72ms | 30.00ms | 75.7% | 2.0% | PASS |
| `checkout_branch_clean` | 27.73ms | 43.00ms | 64.5% | 2.6% | PASS |
| `merge_data_no_conflicts` | 24.54ms | 33.00ms | 74.4% | 2.9% | PASS |
| `merge_data_secondary_index` | 783.37ms | 944.00ms | 83.0% | 1.2% | PASS |
| `merge_schema_no_conflicts` | 20.24ms | 24.00ms | 84.3% | 4.1% | PASS |
| `merge_data_conflicts` | 26.43ms | 33.00ms | 80.1% | 1.5% | PASS |
| `merge_data_conflicts_with_resolve` | 26.44ms | 34.00ms | 77.8% | 1.9% | PASS |

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

candidate: e0ceed7.

Fixture file bytes: base 107,968,301; delta 115,879,032.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 432.729 | 398.589–533.021 | 31.765 | 231.09 | 107968301 |
| push_initial | candidate | 721.556 | 719.263–726.018 | 352.685 | 138.59 | 107956807 |
| clone_initial | candidate | 802.168 | 794.623–813.397 | 300.329 | 124.66 | 108475105 |
| push_noop | candidate | 0.571 | 0.527–0.624 | 0.274 | — | 0 |
| fetch_noop | candidate | 0.137 | 0.121–0.161 | 0.138 | — | 0 |
| push_incremental | candidate | 69.809 | 61.913–74.235 | 49.120 | 14.32 | 7909775 |
| fetch_incremental | candidate | 76.706 | 73.195–89.385 | 59.002 | 13.04 | 7910084 |
| pull_fast_forward | candidate | 73.687 | 71.172–80.464 | 58.065 | 13.57 | 7910844 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 90.48 | 175.487 | 108.763 | 18329 | 2066 |
| clone_initial | candidate | 92.38 | 110.421 | 110.833 | 3037 | 2353 |
| push_noop | candidate | 41.85 | 0.073 | 0.000 | 27 | 2 |
| fetch_noop | candidate | 41.85 | 0.068 | 0.000 | 22 | 0 |
| push_incremental | candidate | 41.85 | 10.738 | 7.910 | 1374 | 125 |
| fetch_incremental | candidate | 41.85 | 26.494 | 7.910 | 7016 | 124 |
| pull_fast_forward | candidate | 41.85 | 26.497 | 8.959 | 7032 | 142 |

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

candidate: e0ceed7.

Fixture file bytes: base 1,087,718,066; delta 1,103,525,194.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 5838.251 | 5814.560–5844.434 | 341.517 | 171.28 | 1087718066 |
| push_initial | candidate | 7043.019 | 6941.878–7170.101 | 3170.529 | 141.98 | 1079566310 |
| clone_initial | candidate | 7326.900 | 6917.573–8245.325 | 2822.319 | 136.48 | 1102407396 |
| push_noop | candidate | 0.796 | 0.762–0.895 | 0.220 | — | 0 |
| fetch_noop | candidate | 0.087 | 0.084–0.097 | 0.088 | — | 0 |
| push_incremental | candidate | 253.605 | 246.439–259.706 | 174.436 | 3.94 | 15806003 |
| fetch_incremental | candidate | 259.077 | 257.263–272.743 | 185.068 | 3.86 | 15806513 |
| pull_fast_forward | candidate | 271.945 | 270.325–272.541 | 183.466 | 3.68 | 15807273 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 134.87 | 2091.986 | 1087.622 | 263524 | 20541 |
| clone_initial | candidate | 141.87 | 1123.936 | 1133.868 | 35438 | 32259 |
| push_noop | candidate | 44.61 | 0.078 | 0.000 | 28 | 2 |
| fetch_noop | candidate | 44.61 | 0.069 | 0.000 | 22 | 0 |
| push_incremental | candidate | 56.40 | 46.817 | 15.928 | 10134 | 313 |
| fetch_incremental | candidate | 50.54 | 62.315 | 15.928 | 13913 | 313 |
| pull_fast_forward | candidate | 50.47 | 62.318 | 16.978 | 13929 | 331 |

</details>


</details>

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh`, `test/vc_perf_ceiling.sh`, and `test/remote_perf.py`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
