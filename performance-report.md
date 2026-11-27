# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-11-27 11:22 UTC
>
> Commit: [`449c5dc96cca5c49fef1c83901be3e954221ca6e`](https://github.com/dolthub/doltlite/commit/449c5dc96cca5c49fef1c83901be3e954221ca6e)
>
> Runner: ubuntu24 20261122.23.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/45974510721)

The SQL workloads compare optimized DoltLite against stock SQLite on the same GitHub-hosted runner, alternating execution order on each repetition. Version-control sections measure DoltLite latency. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class. Mean multiple is the average over workloads of each workload's paired DoltLite/SQLite ratio, so every workload counts equally whatever its run time.

### In-memory

| Operation | Mean multiple | Paired-ratio noise | Result |
|---|---:|---:|---|
| Reads | 1.10× | 1.6% | **PASS** |
| Writes | 1.63× | 1.4% | **PASS** |

### File-backed

| Operation | Mean multiple | Paired-ratio noise | Result |
|---|---:|---:|---|
| Reads | 0.85× | 1.2% | **PASS** |
| Writes | 1.12× | 8.2% | **PASS** |
| Autocommit writes | 2.16× | 25.1% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.8× for a section average. Durable autocommit writes use 3.6× and 2.6× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | Mean multiple | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 1.10× | 1.4% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 1.07× | 1.6% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 1.10× | 1.4% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 1.12× | 1.6% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 1.54× | 1.4% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 1.75× | 1.5% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 1.69× | 1.0% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 1.54× | 1.5% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 0.79× | 1.5% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 0.83× | 1.2% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 0.94× | 1.3% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 0.84× | 1.2% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 0.93× | 12.6% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.17× | 4.4% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 1.43× | 1.9% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 0.97× | 14.5% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 1.02× | 0.9% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 1.00× | 1.7% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 1.04× | 1.4% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 1.04× | 1.1% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 2.34× | 39.5% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 1.98× | 5.6% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 2.06× | 6.1% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 2.24× | 48.4% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 18.99ms | 21.61ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_range_select` | 8.56ms | 9.17ms | 1.1× | 1.7% | PASS |
| mem_reads | `oltp_sum_range` | 7.66ms | 8.51ms | 1.1× | 2.0% | PASS |
| mem_reads | `oltp_order_range` | 2.18ms | 2.28ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 3.03ms | 3.04ms | 1.0× | 1.4% | PASS |
| mem_reads | `oltp_index_scan` | 3.08ms | 3.82ms | 1.2× | 1.7% | PASS |
| mem_reads | `select_random_points` | 10.13ms | 9.08ms | 0.9× | 2.2% | PASS |
| mem_reads | `select_random_ranges` | 3.65ms | 3.63ms | 1.0× | 1.8% | PASS |
| mem_reads | `covering_index_scan` | 5.88ms | 7.66ms | 1.3× | 1.0% | PASS |
| mem_reads | `groupby_scan` | 25.22ms | 26.00ms | 1.0× | 1.1% | PASS |
| mem_reads | `index_join` | 4.49ms | 6.14ms | 1.4× | 1.4% | PASS |
| mem_reads | `index_join_scan` | 2.63ms | 4.41ms | 1.7× | 2.2% | PASS |
| mem_reads | `types_table_scan` | 836.49ms | 642.27ms | 0.8× | 0.6% | PASS |
| mem_reads | `table_scan` | 950.75ms | 723.84ms | 0.8× | 0.7% | PASS |
| mem_reads | `oltp_read_only` | 81.60ms | 86.69ms | 1.1× | 0.9% | PASS |
| mem_writes | `oltp_bulk_insert` | 140.69ms | 205.87ms | 1.5× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 12.17ms | 19.95ms | 1.6× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 40.41ms | 69.61ms | 1.7× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 28.77ms | 42.09ms | 1.5× | 1.6% | PASS |
| mem_writes | `oltp_delete_insert` | 35.81ms | 55.43ms | 1.5× | 1.3% | PASS |
| mem_writes | `oltp_write_only` | 18.09ms | 33.23ms | 1.8× | 1.5% | PASS |
| mem_writes | `types_delete_insert` | 19.76ms | 23.14ms | 1.2× | 1.5% | PASS |
| mem_writes | `oltp_read_write` | 51.04ms | 74.59ms | 1.5× | 1.6% | PASS |
| file_reads | `oltp_point_select` | 80.42ms | 37.81ms | 0.5× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 15.04ms | 10.99ms | 0.7× | 2.6% | PASS |
| file_reads | `oltp_sum_range` | 14.11ms | 10.27ms | 0.7× | 1.8% | PASS |
| file_reads | `oltp_order_range` | 2.88ms | 2.54ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_distinct_range` | 3.74ms | 3.28ms | 0.9× | 1.8% | PASS |
| file_reads | `oltp_index_scan` | 9.56ms | 5.88ms | 0.6× | 1.3% | PASS |
| file_reads | `select_random_points` | 16.93ms | 11.05ms | 0.7× | 1.9% | PASS |
| file_reads | `select_random_ranges` | 9.92ms | 5.42ms | 0.5× | 1.6% | PASS |
| file_reads | `covering_index_scan` | 12.48ms | 9.71ms | 0.8× | 1.0% | PASS |
| file_reads | `groupby_scan` | 25.88ms | 26.35ms | 1.0× | 1.1% | PASS |
| file_reads | `index_join` | 7.97ms | 7.64ms | 1.0× | 1.5% | PASS |
| file_reads | `index_join_scan` | 3.40ms | 4.72ms | 1.4× | 1.6% | PASS |
| file_reads | `types_table_scan` | 839.70ms | 644.42ms | 0.8× | 0.6% | PASS |
| file_reads | `table_scan` | 951.75ms | 722.84ms | 0.8× | 0.7% | PASS |
| file_reads | `oltp_read_only` | 169.38ms | 109.44ms | 0.6× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 203.29ms | 268.25ms | 1.3× | 8.5% | PASS |
| file_writes | `oltp_insert` | 29.67ms | 32.10ms | 1.1× | 8.4% | PASS |
| file_writes | `oltp_update_index` | 138.31ms | 133.68ms | 1.0× | 14.5% | PASS |
| file_writes | `oltp_update_non_index` | 113.52ms | 83.42ms | 0.7× | 12.3% | PASS |
| file_writes | `oltp_delete_insert` | 118.81ms | 103.21ms | 0.9× | 7.9% | PASS |
| file_writes | `oltp_write_only` | 108.79ms | 81.37ms | 0.7× | 28.9% | PASS |
| file_writes | `types_delete_insert` | 74.60ms | 54.62ms | 0.7× | 12.8% | PASS |
| file_writes | `oltp_read_write` | 132.12ms | 129.52ms | 1.0× | 16.2% | PASS |
| ac_reads | `oltp_point_select` | 39.20ms | 37.83ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_range_select` | 11.14ms | 11.03ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_sum_range` | 10.22ms | 10.29ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_order_range` | 2.57ms | 2.55ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_distinct_range` | 3.38ms | 3.29ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_index_scan` | 5.58ms | 5.92ms | 1.1× | 1.3% | PASS |
| ac_reads | `select_random_points` | 13.07ms | 11.12ms | 0.9× | 0.9% | PASS |
| ac_reads | `select_random_ranges` | 5.96ms | 5.44ms | 0.9× | 0.9% | PASS |
| ac_reads | `covering_index_scan` | 8.41ms | 9.75ms | 1.2× | 0.6% | PASS |
| ac_reads | `groupby_scan` | 25.60ms | 26.33ms | 1.0× | 0.6% | PASS |
| ac_reads | `index_join` | 6.07ms | 7.71ms | 1.3× | 1.4% | PASS |
| ac_reads | `index_join_scan` | 3.08ms | 4.76ms | 1.5× | 1.7% | PASS |
| ac_reads | `types_table_scan` | 837.93ms | 645.37ms | 0.8× | 0.4% | PASS |
| ac_reads | `table_scan` | 951.16ms | 722.86ms | 0.8× | 0.4% | PASS |
| ac_reads | `oltp_read_only` | 110.64ms | 109.70ms | 1.0× | 0.7% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.59ms | 33.46ms | 1.5× | 38.8% | PASS |
| ac_writes | `oltp_insert_ac` | 24.54ms | 63.75ms | 2.6× | 55.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.71ms | 79.89ms | 3.0× | 28.8% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.47ms | 45.23ms | 2.1× | 40.2% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 22.62ms | 57.23ms | 2.5× | 29.4% | PASS |
| ac_writes | `oltp_write_only_ac` | 22.88ms | 58.82ms | 2.6× | 25.5% | PASS |
| ac_writes | `types_delete_insert_ac` | 31.53ms | 52.56ms | 1.7× | 61.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 30.23ms | 81.61ms | 2.7× | 45.7% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 37.02ms | 39.04ms | 1.1× | 2.3% | PASS |
| mem_reads | `oltp_range_select` | 17.54ms | 16.28ms | 0.9× | 1.6% | PASS |
| mem_reads | `oltp_sum_range` | 15.50ms | 15.75ms | 1.0× | 2.0% | PASS |
| mem_reads | `oltp_order_range` | 3.32ms | 3.44ms | 1.0× | 1.6% | PASS |
| mem_reads | `oltp_distinct_range` | 4.43ms | 4.49ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 4.08ms | 6.38ms | 1.6× | 1.8% | PASS |
| mem_reads | `select_random_points` | 23.90ms | 21.95ms | 0.9× | 2.8% | PASS |
| mem_reads | `select_random_ranges` | 7.07ms | 6.84ms | 1.0× | 1.7% | PASS |
| mem_reads | `covering_index_scan` | 7.80ms | 11.71ms | 1.5× | 1.3% | PASS |
| mem_reads | `groupby_scan` | 35.13ms | 34.94ms | 1.0× | 0.9% | PASS |
| mem_reads | `index_join` | 11.29ms | 10.43ms | 0.9× | 2.7% | PASS |
| mem_reads | `index_join_scan` | 3.95ms | 6.37ms | 1.6× | 2.5% | PASS |
| mem_reads | `types_table_scan` | 1.21s | 936.01ms | 0.8× | 0.9% | PASS |
| mem_reads | `table_scan` | 1.36s | 1.04s | 0.8× | 0.8% | PASS |
| mem_reads | `oltp_read_only` | 151.79ms | 151.96ms | 1.0× | 1.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 245.44ms | 347.22ms | 1.4× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 18.39ms | 39.76ms | 2.2× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 71.76ms | 147.39ms | 2.1× | 1.7% | PASS |
| mem_writes | `oltp_update_non_index` | 52.95ms | 87.97ms | 1.7× | 1.5% | PASS |
| mem_writes | `oltp_delete_insert` | 55.74ms | 103.83ms | 1.9× | 1.5% | PASS |
| mem_writes | `oltp_write_only` | 29.41ms | 61.62ms | 2.1× | 1.6% | PASS |
| mem_writes | `types_delete_insert` | 40.36ms | 53.02ms | 1.3× | 1.4% | PASS |
| mem_writes | `oltp_read_write` | 98.12ms | 141.49ms | 1.4× | 1.8% | PASS |
| file_reads | `oltp_point_select` | 102.62ms | 55.84ms | 0.5× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 24.69ms | 18.13ms | 0.7× | 1.1% | PASS |
| file_reads | `oltp_sum_range` | 22.67ms | 17.61ms | 0.8× | 0.9% | PASS |
| file_reads | `oltp_order_range` | 4.17ms | 3.68ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_distinct_range` | 5.25ms | 4.76ms | 0.9× | 1.0% | PASS |
| file_reads | `oltp_index_scan` | 10.94ms | 8.28ms | 0.8× | 1.4% | PASS |
| file_reads | `select_random_points` | 31.43ms | 24.21ms | 0.8× | 2.0% | PASS |
| file_reads | `select_random_ranges` | 13.78ms | 8.63ms | 0.6× | 1.4% | PASS |
| file_reads | `covering_index_scan` | 14.99ms | 13.55ms | 0.9× | 1.1% | PASS |
| file_reads | `groupby_scan` | 35.73ms | 35.02ms | 1.0× | 1.0% | PASS |
| file_reads | `index_join` | 15.06ms | 11.43ms | 0.8× | 2.1% | PASS |
| file_reads | `index_join_scan` | 4.75ms | 6.58ms | 1.4× | 1.2% | PASS |
| file_reads | `types_table_scan` | 1.06s | 893.70ms | 0.8× | 1.2% | PASS |
| file_reads | `table_scan` | 1.21s | 992.27ms | 0.8× | 1.4% | PASS |
| file_reads | `oltp_read_only` | 239.62ms | 171.28ms | 0.7× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 268.95ms | 361.82ms | 1.3× | 0.7% | PASS |
| file_writes | `oltp_insert` | 26.17ms | 44.37ms | 1.7× | 2.4% | PASS |
| file_writes | `oltp_update_index` | 130.84ms | 159.06ms | 1.2× | 6.5% | PASS |
| file_writes | `oltp_update_non_index` | 102.97ms | 102.55ms | 1.0× | 12.5% | PASS |
| file_writes | `oltp_delete_insert` | 96.91ms | 120.33ms | 1.2× | 1.4% | PASS |
| file_writes | `oltp_write_only` | 78.62ms | 74.97ms | 1.0× | 12.4% | PASS |
| file_writes | `types_delete_insert` | 73.27ms | 65.01ms | 0.9× | 1.4% | PASS |
| file_writes | `oltp_read_write` | 153.16ms | 157.87ms | 1.0× | 11.0% | PASS |
| ac_reads | `oltp_point_select` | 59.38ms | 56.45ms | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_range_select` | 21.02ms | 18.54ms | 0.9× | 2.1% | PASS |
| ac_reads | `oltp_sum_range` | 18.88ms | 18.07ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_order_range` | 3.88ms | 3.73ms | 1.0× | 2.2% | PASS |
| ac_reads | `oltp_distinct_range` | 4.92ms | 4.81ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_index_scan` | 6.59ms | 8.29ms | 1.3× | 1.5% | PASS |
| ac_reads | `select_random_points` | 26.63ms | 24.46ms | 0.9× | 2.2% | PASS |
| ac_reads | `select_random_ranges` | 9.28ms | 8.65ms | 0.9× | 1.6% | PASS |
| ac_reads | `covering_index_scan` | 10.40ms | 13.55ms | 1.3× | 1.4% | PASS |
| ac_reads | `groupby_scan` | 35.17ms | 35.06ms | 1.0× | 1.0% | PASS |
| ac_reads | `index_join` | 13.41ms | 11.68ms | 0.9× | 1.7% | PASS |
| ac_reads | `index_join_scan` | 4.45ms | 6.84ms | 1.5× | 2.1% | PASS |
| ac_reads | `types_table_scan` | 1.24s | 955.22ms | 0.8× | 0.9% | PASS |
| ac_reads | `table_scan` | 1.36s | 1.03s | 0.8× | 2.0% | PASS |
| ac_reads | `oltp_read_only` | 184.84ms | 176.75ms | 1.0× | 1.4% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 23.91ms | 34.29ms | 1.4× | 7.2% | PASS |
| ac_writes | `oltp_insert_ac` | 26.05ms | 46.45ms | 1.8× | 5.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 28.25ms | 71.31ms | 2.5× | 5.0% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.22ms | 43.83ms | 1.9× | 5.5% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.40ms | 58.29ms | 2.2× | 5.8% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.25ms | 54.91ms | 2.2× | 3.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 24.27ms | 43.01ms | 1.8× | 6.2% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.38ms | 63.25ms | 2.0× | 5.6% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 26.38ms | 25.27ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_range_select` | 12.03ms | 12.05ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_sum_range` | 11.83ms | 11.48ms | 1.0× | 1.4% | PASS |
| mem_reads | `oltp_order_range` | 2.44ms | 2.55ms | 1.0× | 1.0% | PASS |
| mem_reads | `oltp_distinct_range` | 3.11ms | 3.21ms | 1.0× | 0.7% | PASS |
| mem_reads | `oltp_index_scan` | 2.97ms | 4.83ms | 1.6× | 0.8% | PASS |
| mem_reads | `select_random_points` | 18.69ms | 17.36ms | 0.9× | 2.1% | PASS |
| mem_reads | `select_random_ranges` | 4.82ms | 4.55ms | 0.9× | 1.5% | PASS |
| mem_reads | `covering_index_scan` | 4.83ms | 7.31ms | 1.5× | 0.4% | PASS |
| mem_reads | `groupby_scan` | 23.53ms | 24.18ms | 1.0× | 0.5% | PASS |
| mem_reads | `index_join` | 10.19ms | 8.88ms | 0.9× | 2.0% | PASS |
| mem_reads | `index_join_scan` | 2.80ms | 5.93ms | 2.1× | 2.9% | PASS |
| mem_reads | `types_table_scan` | 772.98ms | 585.63ms | 0.8× | 2.2% | PASS |
| mem_reads | `table_scan` | 956.11ms | 653.02ms | 0.7× | 9.1% | PASS |
| mem_reads | `oltp_read_only` | 99.08ms | 100.14ms | 1.0× | 1.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 145.96ms | 202.30ms | 1.4× | 0.5% | PASS |
| mem_writes | `oltp_insert` | 11.88ms | 25.41ms | 2.1× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 48.32ms | 98.42ms | 2.0× | 1.3% | PASS |
| mem_writes | `oltp_update_non_index` | 34.31ms | 55.48ms | 1.6× | 1.1% | PASS |
| mem_writes | `oltp_delete_insert` | 38.17ms | 68.55ms | 1.8× | 0.9% | PASS |
| mem_writes | `oltp_write_only` | 19.82ms | 38.24ms | 1.9× | 1.3% | PASS |
| mem_writes | `types_delete_insert` | 26.15ms | 32.03ms | 1.2× | 1.5% | PASS |
| mem_writes | `oltp_read_write` | 63.38ms | 88.45ms | 1.4× | 1.0% | PASS |
| file_reads | `oltp_point_select` | 44.81ms | 28.49ms | 0.6× | 0.7% | PASS |
| file_reads | `oltp_range_select` | 12.99ms | 11.97ms | 0.9× | 1.8% | PASS |
| file_reads | `oltp_sum_range` | 13.15ms | 11.67ms | 0.9× | 1.9% | PASS |
| file_reads | `oltp_order_range` | 2.64ms | 2.53ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_distinct_range` | 3.31ms | 3.26ms | 1.0× | 1.2% | PASS |
| file_reads | `oltp_index_scan` | 5.01ms | 5.24ms | 1.0× | 1.6% | PASS |
| file_reads | `select_random_points` | 20.81ms | 17.26ms | 0.8× | 1.4% | PASS |
| file_reads | `select_random_ranges` | 6.87ms | 4.95ms | 0.7× | 1.7% | PASS |
| file_reads | `covering_index_scan` | 6.94ms | 7.71ms | 1.1× | 0.7% | PASS |
| file_reads | `groupby_scan` | 23.44ms | 24.07ms | 1.0× | 0.4% | PASS |
| file_reads | `index_join` | 10.76ms | 8.63ms | 0.8× | 2.3% | PASS |
| file_reads | `index_join_scan` | 3.02ms | 5.59ms | 1.9× | 3.0% | PASS |
| file_reads | `types_table_scan` | 766.95ms | 578.33ms | 0.8× | 0.4% | PASS |
| file_reads | `table_scan` | 871.48ms | 642.88ms | 0.7× | 0.3% | PASS |
| file_reads | `oltp_read_only` | 122.38ms | 101.77ms | 0.8× | 1.1% | PASS |
| file_writes | `oltp_bulk_insert` | 157.97ms | 210.99ms | 1.3× | 0.7% | PASS |
| file_writes | `oltp_insert` | 15.31ms | 28.32ms | 1.8× | 1.6% | PASS |
| file_writes | `oltp_update_index` | 60.81ms | 104.78ms | 1.7× | 1.9% | PASS |
| file_writes | `oltp_update_non_index` | 46.62ms | 64.19ms | 1.4× | 2.2% | PASS |
| file_writes | `oltp_delete_insert` | 53.16ms | 78.20ms | 1.5× | 1.9% | PASS |
| file_writes | `oltp_write_only` | 33.05ms | 48.16ms | 1.5× | 2.3% | PASS |
| file_writes | `types_delete_insert` | 39.23ms | 39.52ms | 1.0× | 2.8% | PASS |
| file_writes | `oltp_read_write` | 80.17ms | 101.63ms | 1.3× | 1.3% | PASS |
| ac_reads | `oltp_point_select` | 31.65ms | 28.81ms | 0.9× | 0.7% | PASS |
| ac_reads | `oltp_range_select` | 12.63ms | 12.34ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_sum_range` | 12.47ms | 11.85ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_order_range` | 2.57ms | 2.60ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_distinct_range` | 3.21ms | 3.24ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_index_scan` | 3.77ms | 5.22ms | 1.4× | 1.4% | PASS |
| ac_reads | `select_random_points` | 19.71ms | 17.39ms | 0.9× | 1.6% | PASS |
| ac_reads | `select_random_ranges` | 5.66ms | 4.96ms | 0.9× | 2.3% | PASS |
| ac_reads | `covering_index_scan` | 5.60ms | 7.74ms | 1.4× | 0.6% | PASS |
| ac_reads | `groupby_scan` | 23.38ms | 24.14ms | 1.0× | 0.4% | PASS |
| ac_reads | `index_join` | 10.44ms | 8.79ms | 0.8× | 2.1% | PASS |
| ac_reads | `index_join_scan` | 3.01ms | 5.76ms | 1.9× | 2.3% | PASS |
| ac_reads | `types_table_scan` | 764.27ms | 578.41ms | 0.8× | 0.3% | PASS |
| ac_reads | `table_scan` | 872.58ms | 643.36ms | 0.7× | 0.4% | PASS |
| ac_reads | `oltp_read_only` | 102.59ms | 101.62ms | 1.0× | 0.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 9.29ms | 12.79ms | 1.4× | 0.9% | PASS |
| ac_writes | `oltp_insert_ac` | 12.07ms | 28.37ms | 2.4× | 4.7% | PASS |
| ac_writes | `oltp_update_index_ac` | 15.00ms | 38.45ms | 2.6× | 14.9% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 9.16ms | 17.07ms | 1.9× | 2.7% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 17.33ms | 33.88ms | 2.0× | 24.7% | PASS |
| ac_writes | `oltp_write_only_ac` | 10.66ms | 26.11ms | 2.4× | 4.8% | PASS |
| ac_writes | `types_delete_insert_ac` | 9.88ms | 18.56ms | 1.9× | 7.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 15.93ms | 31.35ms | 2.0× | 12.0% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 28.98ms | 30.61ms | 1.1× | 1.7% | PASS |
| mem_reads | `oltp_range_select` | 17.40ms | 17.46ms | 1.0× | 1.8% | PASS |
| mem_reads | `oltp_sum_range` | 14.90ms | 16.35ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_order_range` | 3.09ms | 3.17ms | 1.0× | 0.8% | PASS |
| mem_reads | `oltp_distinct_range` | 3.96ms | 3.98ms | 1.0× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 3.86ms | 5.10ms | 1.3× | 1.7% | PASS |
| mem_reads | `select_random_points` | 23.11ms | 23.00ms | 1.0× | 2.1% | PASS |
| mem_reads | `select_random_ranges` | 5.97ms | 6.45ms | 1.1× | 1.3% | PASS |
| mem_reads | `covering_index_scan` | 6.02ms | 8.46ms | 1.4× | 1.2% | PASS |
| mem_reads | `groupby_scan` | 31.27ms | 32.38ms | 1.0× | 0.9% | PASS |
| mem_reads | `index_join` | 6.42ms | 9.50ms | 1.5× | 1.7% | PASS |
| mem_reads | `index_join_scan` | 3.30ms | 5.65ms | 1.7× | 2.4% | PASS |
| mem_reads | `types_table_scan` | 978.03ms | 779.24ms | 0.8× | 1.1% | PASS |
| mem_reads | `table_scan` | 1.18s | 844.06ms | 0.7× | 1.9% | PASS |
| mem_reads | `oltp_read_only` | 131.85ms | 132.01ms | 1.0× | 1.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 195.04ms | 247.89ms | 1.3× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 15.32ms | 26.85ms | 1.8× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 58.57ms | 100.64ms | 1.7× | 1.9% | PASS |
| mem_writes | `oltp_update_non_index` | 43.68ms | 63.13ms | 1.4× | 1.6% | PASS |
| mem_writes | `oltp_delete_insert` | 40.22ms | 68.58ms | 1.7× | 1.4% | PASS |
| mem_writes | `oltp_write_only` | 22.34ms | 40.96ms | 1.8× | 1.9% | PASS |
| mem_writes | `types_delete_insert` | 27.59ms | 35.64ms | 1.3× | 2.0% | PASS |
| mem_writes | `oltp_read_write` | 88.06ms | 115.90ms | 1.3× | 1.5% | PASS |
| file_reads | `oltp_point_select` | 91.23ms | 46.99ms | 0.5× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 24.20ms | 19.13ms | 0.8× | 0.9% | PASS |
| file_reads | `oltp_sum_range` | 21.73ms | 18.16ms | 0.8× | 1.4% | PASS |
| file_reads | `oltp_order_range` | 3.79ms | 3.39ms | 0.9× | 1.2% | PASS |
| file_reads | `oltp_distinct_range` | 4.63ms | 4.15ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_index_scan` | 10.24ms | 6.77ms | 0.7× | 0.9% | PASS |
| file_reads | `select_random_points` | 29.19ms | 24.31ms | 0.8× | 1.1% | PASS |
| file_reads | `select_random_ranges` | 12.43ms | 8.26ms | 0.7× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 12.59ms | 10.08ms | 0.8× | 0.8% | PASS |
| file_reads | `groupby_scan` | 31.24ms | 32.16ms | 1.0× | 1.1% | PASS |
| file_reads | `index_join` | 9.86ms | 10.00ms | 1.0× | 1.8% | PASS |
| file_reads | `index_join_scan` | 4.03ms | 5.49ms | 1.4× | 1.6% | PASS |
| file_reads | `types_table_scan` | 900.37ms | 738.21ms | 0.8× | 2.5% | PASS |
| file_reads | `table_scan` | 1.09s | 809.49ms | 0.7× | 3.4% | PASS |
| file_reads | `oltp_read_only` | 210.33ms | 149.65ms | 0.7× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 250.53ms | 307.14ms | 1.2× | 5.3% | PASS |
| file_writes | `oltp_insert` | 38.33ms | 48.68ms | 1.3× | 19.5% | PASS |
| file_writes | `oltp_update_index` | 166.32ms | 160.79ms | 1.0× | 5.8% | PASS |
| file_writes | `oltp_update_non_index` | 144.71ms | 123.53ms | 0.9× | 13.6% | PASS |
| file_writes | `oltp_delete_insert` | 139.10ms | 126.72ms | 0.9× | 8.4% | PASS |
| file_writes | `oltp_write_only` | 108.67ms | 95.01ms | 0.9× | 15.3% | PASS |
| file_writes | `types_delete_insert` | 91.93ms | 78.99ms | 0.9× | 22.4% | PASS |
| file_writes | `oltp_read_write` | 192.17ms | 174.03ms | 0.9× | 18.4% | PASS |
| ac_reads | `oltp_point_select` | 47.45ms | 45.53ms | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_range_select` | 18.83ms | 18.67ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_sum_range` | 16.73ms | 17.74ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_order_range` | 3.43ms | 3.39ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_distinct_range` | 4.29ms | 4.15ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_index_scan` | 6.28ms | 6.82ms | 1.1× | 1.5% | PASS |
| ac_reads | `select_random_points` | 24.39ms | 24.17ms | 1.0× | 1.0% | PASS |
| ac_reads | `select_random_ranges` | 8.31ms | 8.25ms | 1.0× | 0.8% | PASS |
| ac_reads | `covering_index_scan` | 8.51ms | 10.11ms | 1.2× | 0.9% | PASS |
| ac_reads | `groupby_scan` | 31.05ms | 32.22ms | 1.0× | 0.9% | PASS |
| ac_reads | `index_join` | 7.95ms | 10.10ms | 1.3× | 1.4% | PASS |
| ac_reads | `index_join_scan` | 3.72ms | 5.64ms | 1.5× | 2.0% | PASS |
| ac_reads | `types_table_scan` | 942.07ms | 754.85ms | 0.8× | 2.5% | PASS |
| ac_reads | `table_scan` | 1.15s | 823.35ms | 0.7× | 3.3% | PASS |
| ac_reads | `oltp_read_only` | 152.38ms | 150.85ms | 1.0× | 1.6% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 47.30ms | 63.37ms | 1.3× | 77.1% | PASS |
| ac_writes | `oltp_insert_ac` | 26.58ms | 63.74ms | 2.4× | 58.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 37.35ms | 119.95ms | 3.2× | 67.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 26.17ms | 64.63ms | 2.5× | 54.4% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 29.67ms | 71.61ms | 2.4× | 38.5% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.21ms | 74.53ms | 2.8× | 42.4% | PASS |
| ac_writes | `types_delete_insert_ac` | 31.70ms | 54.19ms | 1.7× | 40.3% | PASS |
| ac_writes | `oltp_read_write_ac` | 39.66ms | 80.50ms | 2.0× | 32.9% | PASS |

</details>

</details>

## Version-control latency

Wall time: 4m 49s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 22.77ms | 38.00ms | 59.9% | 0.8% | PASS |
| `status_dirty_many_tables` | 24.96ms | 42.00ms | 59.4% | 0.8% | PASS |
| `diff_regular_working_one_table` | 21.47ms | 33.00ms | 65.1% | 0.9% | PASS |
| `diff_regular_working_many_tables` | 28.31ms | 50.00ms | 56.6% | 0.9% | PASS |
| `diff_stat_working_many_tables` | 28.41ms | 48.00ms | 59.2% | 0.9% | PASS |
| `diff_schema_working_many_tables` | 29.10ms | 48.00ms | 60.6% | 1.1% | PASS |
| `branch_list_many_branches` | 15.25ms | 25.00ms | 61.0% | 1.4% | PASS |
| `branch_create_delete` | 16.32ms | 27.00ms | 60.4% | 1.0% | PASS |
| `at_literal_deep_history` | 16.27ms | 28.00ms | 58.1% | 1.0% | PASS |
| `diff_literal_deep_history` | 16.18ms | 28.00ms | 57.8% | 0.9% | PASS |
| `history_literal_deep_history` | 16.71ms | 30.00ms | 55.7% | 0.6% | PASS |
| `checkout_branch_clean` | 22.03ms | 43.00ms | 51.2% | 0.9% | PASS |
| `merge_data_no_conflicts` | 17.95ms | 33.00ms | 54.4% | 0.8% | PASS |
| `merge_data_secondary_index` | 626.09ms | 944.00ms | 66.3% | 0.7% | PASS |
| `merge_schema_no_conflicts` | 14.46ms | 24.00ms | 60.2% | 0.6% | PASS |
| `merge_data_conflicts` | 20.46ms | 33.00ms | 62.0% | 0.7% | PASS |
| `merge_data_conflicts_with_resolve` | 21.16ms | 34.00ms | 62.2% | 0.8% | PASS |

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

candidate: 449c5dc.

Fixture file bytes: base 107,968,301; delta 115,879,032.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 50.974 | 48.369–63.340 | 46.048 | 1961.78 | 107968301 |
| push_initial | candidate | 299.385 | 293.557–304.288 | 297.716 | 334.02 | 107956807 |
| clone_initial | candidate | 265.237 | 255.255–272.166 | 255.333 | 377.02 | 108475105 |
| push_noop | candidate | 0.383 | 0.348–0.391 | 0.182 | — | 0 |
| fetch_noop | candidate | 0.078 | 0.076–0.084 | 0.079 | — | 0 |
| push_incremental | candidate | 43.652 | 42.188–45.483 | 42.294 | 22.91 | 7909775 |
| fetch_incremental | candidate | 49.234 | 48.236–49.869 | 47.945 | 20.31 | 7910084 |
| pull_fast_forward | candidate | 49.242 | 48.939–50.264 | 47.858 | 20.31 | 7910844 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 90.56 | 175.487 | 108.763 | 18329 | 2066 |
| clone_initial | candidate | 92.26 | 110.421 | 110.833 | 3037 | 2353 |
| push_noop | candidate | 41.77 | 0.073 | 0.000 | 27 | 2 |
| fetch_noop | candidate | 41.77 | 0.068 | 0.000 | 22 | 0 |
| push_incremental | candidate | 41.77 | 10.741 | 7.910 | 1375 | 125 |
| fetch_incremental | candidate | 41.77 | 26.494 | 7.910 | 7016 | 124 |
| pull_fast_forward | candidate | 41.77 | 26.497 | 8.959 | 7032 | 142 |

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

candidate: 449c5dc.

Fixture file bytes: base 1,087,718,066; delta 1,103,525,194.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 4157.447 | 4149.064–4162.698 | 593.749 | 240.53 | 1087718066 |
| push_initial | candidate | 5960.864 | 5777.787–6840.933 | 3112.356 | 167.76 | 1079566310 |
| clone_initial | candidate | 3985.021 | 3981.421–4186.908 | 2657.875 | 250.94 | 1102407396 |
| push_noop | candidate | 0.482 | 0.412–0.494 | 0.186 | — | 0 |
| fetch_noop | candidate | 0.086 | 0.081–0.095 | 0.087 | — | 0 |
| push_incremental | candidate | 182.999 | 181.747–187.593 | 180.492 | 5.46 | 15806003 |
| fetch_incremental | candidate | 185.869 | 185.523–185.906 | 183.198 | 5.38 | 15806513 |
| pull_fast_forward | candidate | 190.138 | 187.968–196.989 | 187.385 | 5.26 | 15807273 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 136.06 | 2091.400 | 1087.622 | 263381 | 20541 |
| clone_initial | candidate | 142.11 | 1123.350 | 1133.868 | 35295 | 32259 |
| push_noop | candidate | 44.45 | 0.078 | 0.000 | 28 | 2 |
| fetch_noop | candidate | 44.45 | 0.069 | 0.000 | 22 | 0 |
| push_incremental | candidate | 56.42 | 46.055 | 15.928 | 9948 | 313 |
| fetch_incremental | candidate | 50.50 | 61.709 | 15.928 | 13765 | 313 |
| pull_fast_forward | candidate | 50.55 | 61.712 | 16.978 | 13781 | 331 |

</details>


</details>

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh`, `test/vc_perf_ceiling.sh`, and `test/remote_perf.py`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
