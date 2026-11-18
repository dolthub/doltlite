# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-11-18 11:08 UTC
>
> Commit: [`0c9ea2ed17ccfeb9dc26a9abbb3b238e444b7edd`](https://github.com/dolthub/doltlite/commit/0c9ea2ed17ccfeb9dc26a9abbb3b238e444b7edd)
>
> Runner: ubuntu24 20261111.370.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/44276875097)

The SQL workloads compare optimized DoltLite against stock SQLite on the same GitHub-hosted runner, alternating execution order on each repetition. Version-control sections measure DoltLite latency. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 8.18s | 9.35s | 1.14× | 1.1% | **PASS** |
| Writes | 1.57s | 2.38s | 1.51× | 1.2% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 8.78s | 9.49s | 1.08× | 1.1% | **PASS** |
| Writes | 2.08s | 2.71s | 1.30× | 1.8% | **PASS** |
| Autocommit writes | 382.38ms | 820.10ms | 2.14× | 2.2% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.8× for a section average. Durable autocommit writes use 3.6× and 2.6× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.58s | 2.80s | 1.08× | 1.2% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 1.87s | 2.18s | 1.16× | 1.3% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 1.84s | 2.17s | 1.18× | 1.0% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 1.89s | 2.20s | 1.17× | 1.0% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 448.73ms | 705.82ms | 1.57× | 1.0% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 371.08ms | 554.86ms | 1.50× | 1.9% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 371.66ms | 550.41ms | 1.48× | 1.5% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 380.83ms | 568.56ms | 1.49× | 1.1% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.98s | 2.91s | 0.98× | 1.1% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 1.95s | 2.19s | 1.13× | 1.4% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 1.90s | 2.17s | 1.14× | 0.8% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 1.95s | 2.20s | 1.13× | 1.0% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 592.68ms | 754.24ms | 1.27× | 1.3% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 519.93ms | 665.87ms | 1.28× | 1.9% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 490.06ms | 649.37ms | 1.33× | 1.8% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 477.39ms | 639.82ms | 1.34× | 1.6% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.63s | 2.85s | 1.08× | 1.1% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 1.89s | 2.20s | 1.17× | 1.1% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 1.87s | 2.18s | 1.17× | 1.0% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 1.90s | 2.21s | 1.17× | 1.1% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 136.36ms | 311.99ms | 2.29× | 4.4% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 82.18ms | 170.75ms | 2.08× | 1.7% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 81.88ms | 169.81ms | 2.07× | 1.6% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 81.96ms | 167.56ms | 2.04× | 2.1% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.08ms | 27.50ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_range_select` | 10.31ms | 11.44ms | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_sum_range` | 9.41ms | 10.55ms | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_order_range` | 2.67ms | 2.85ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_distinct_range` | 3.78ms | 3.85ms | 1.0× | 0.7% | PASS |
| mem_reads | `oltp_index_scan` | 3.90ms | 4.87ms | 1.2× | 1.2% | PASS |
| mem_reads | `select_random_points` | 13.00ms | 11.40ms | 0.9× | 1.6% | PASS |
| mem_reads | `select_random_ranges` | 4.56ms | 4.61ms | 1.0× | 1.1% | PASS |
| mem_reads | `covering_index_scan` | 7.57ms | 9.50ms | 1.3× | 0.9% | PASS |
| mem_reads | `groupby_scan` | 31.88ms | 33.44ms | 1.0× | 0.5% | PASS |
| mem_reads | `index_join` | 5.65ms | 7.64ms | 1.4× | 1.2% | PASS |
| mem_reads | `index_join_scan` | 3.21ms | 5.22ms | 1.6× | 0.9% | PASS |
| mem_reads | `types_table_scan` | 1.08s | 1.20s | 1.1× | 0.6% | PASS |
| mem_reads | `table_scan` | 1.27s | 1.36s | 1.1× | 2.0% | PASS |
| mem_reads | `oltp_read_only` | 106.41ms | 111.99ms | 1.1× | 1.5% | PASS |
| mem_writes | `oltp_bulk_insert` | 181.60ms | 273.10ms | 1.5× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 15.68ms | 24.69ms | 1.6× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 52.09ms | 94.14ms | 1.8× | 0.9% | PASS |
| mem_writes | `oltp_update_non_index` | 37.34ms | 59.35ms | 1.6× | 1.3% | PASS |
| mem_writes | `oltp_delete_insert` | 46.74ms | 73.85ms | 1.6× | 1.1% | PASS |
| mem_writes | `oltp_write_only` | 23.31ms | 47.19ms | 2.0× | 1.1% | PASS |
| mem_writes | `types_delete_insert` | 25.78ms | 34.10ms | 1.3× | 0.9% | PASS |
| mem_writes | `oltp_read_write` | 66.21ms | 99.41ms | 1.5× | 1.6% | PASS |
| file_reads | `oltp_point_select` | 102.18ms | 47.47ms | 0.5× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 18.70ms | 13.75ms | 0.7× | 1.4% | PASS |
| file_reads | `oltp_sum_range` | 17.44ms | 12.74ms | 0.7× | 1.1% | PASS |
| file_reads | `oltp_order_range` | 3.62ms | 3.16ms | 0.9× | 0.8% | PASS |
| file_reads | `oltp_distinct_range` | 4.70ms | 4.14ms | 0.9× | 0.7% | PASS |
| file_reads | `oltp_index_scan` | 11.93ms | 7.20ms | 0.6× | 1.1% | PASS |
| file_reads | `select_random_points` | 21.00ms | 13.37ms | 0.6× | 0.7% | PASS |
| file_reads | `select_random_ranges` | 12.44ms | 6.74ms | 0.5× | 0.6% | PASS |
| file_reads | `covering_index_scan` | 15.77ms | 12.03ms | 0.8× | 0.8% | PASS |
| file_reads | `groupby_scan` | 32.86ms | 33.79ms | 1.0× | 0.7% | PASS |
| file_reads | `index_join` | 10.02ms | 9.24ms | 0.9× | 1.3% | PASS |
| file_reads | `index_join_scan` | 4.21ms | 5.66ms | 1.3× | 1.7% | PASS |
| file_reads | `types_table_scan` | 1.16s | 1.23s | 1.1× | 1.3% | PASS |
| file_reads | `table_scan` | 1.34s | 1.37s | 1.0× | 1.2% | PASS |
| file_reads | `oltp_read_only` | 220.68ms | 142.10ms | 0.6× | 1.3% | PASS |
| file_writes | `oltp_bulk_insert` | 197.54ms | 284.94ms | 1.4× | 0.7% | PASS |
| file_writes | `oltp_insert` | 21.98ms | 27.70ms | 1.3× | 1.3% | PASS |
| file_writes | `oltp_update_index` | 77.22ms | 101.20ms | 1.3× | 1.0% | PASS |
| file_writes | `oltp_update_non_index` | 57.86ms | 63.98ms | 1.1× | 0.9% | PASS |
| file_writes | `oltp_delete_insert` | 67.31ms | 82.90ms | 1.2× | 1.8% | PASS |
| file_writes | `oltp_write_only` | 43.34ms | 50.75ms | 1.2× | 1.9% | PASS |
| file_writes | `types_delete_insert` | 39.94ms | 39.24ms | 1.0× | 1.3% | PASS |
| file_writes | `oltp_read_write` | 87.49ms | 103.54ms | 1.2× | 1.8% | PASS |
| ac_reads | `oltp_point_select` | 50.55ms | 47.69ms | 0.9× | 0.8% | PASS |
| ac_reads | `oltp_range_select` | 13.56ms | 13.74ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_sum_range` | 12.49ms | 12.69ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_order_range` | 3.10ms | 3.15ms | 1.0× | 1.8% | PASS |
| ac_reads | `oltp_distinct_range` | 4.17ms | 4.12ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 6.92ms | 7.25ms | 1.0× | 1.2% | PASS |
| ac_reads | `select_random_points` | 16.10ms | 13.46ms | 0.8× | 1.1% | PASS |
| ac_reads | `select_random_ranges` | 7.40ms | 6.76ms | 0.9× | 0.7% | PASS |
| ac_reads | `covering_index_scan` | 10.69ms | 12.07ms | 1.1× | 1.1% | PASS |
| ac_reads | `groupby_scan` | 32.38ms | 33.88ms | 1.0× | 0.6% | PASS |
| ac_reads | `index_join` | 7.38ms | 9.34ms | 1.3× | 1.0% | PASS |
| ac_reads | `index_join_scan` | 3.67ms | 5.60ms | 1.5× | 1.1% | PASS |
| ac_reads | `types_table_scan` | 1.08s | 1.20s | 1.1× | 0.4% | PASS |
| ac_reads | `table_scan` | 1.24s | 1.34s | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_read_only` | 141.31ms | 139.58ms | 1.0× | 0.7% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 15.27ms | 28.08ms | 1.8× | 8.3% | PASS |
| ac_writes | `oltp_insert_ac` | 16.66ms | 37.31ms | 2.2× | 4.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 18.33ms | 50.77ms | 2.8× | 3.0% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 15.09ms | 32.12ms | 2.1× | 3.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 15.80ms | 40.73ms | 2.6× | 2.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 17.66ms | 42.30ms | 2.4× | 6.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 15.38ms | 32.20ms | 2.1× | 6.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 22.16ms | 48.48ms | 2.2× | 4.8% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.37ms | 24.05ms | 1.0× | 1.0% | PASS |
| mem_reads | `oltp_range_select` | 11.82ms | 11.44ms | 1.0× | 1.7% | PASS |
| mem_reads | `oltp_sum_range` | 12.09ms | 11.04ms | 0.9× | 1.3% | PASS |
| mem_reads | `oltp_order_range` | 2.27ms | 2.35ms | 1.0× | 2.6% | PASS |
| mem_reads | `oltp_distinct_range` | 3.05ms | 3.02ms | 1.0× | 2.4% | PASS |
| mem_reads | `oltp_index_scan` | 2.80ms | 4.43ms | 1.6× | 1.7% | PASS |
| mem_reads | `select_random_points` | 17.93ms | 16.59ms | 0.9× | 0.8% | PASS |
| mem_reads | `select_random_ranges` | 4.67ms | 4.32ms | 0.9× | 2.5% | PASS |
| mem_reads | `covering_index_scan` | 4.82ms | 6.94ms | 1.4× | 1.1% | PASS |
| mem_reads | `groupby_scan` | 23.72ms | 24.05ms | 1.0× | 0.6% | PASS |
| mem_reads | `index_join` | 9.39ms | 7.77ms | 0.8× | 1.5% | PASS |
| mem_reads | `index_join_scan` | 2.67ms | 5.45ms | 2.0× | 2.6% | PASS |
| mem_reads | `types_table_scan` | 774.90ms | 938.80ms | 1.2× | 0.8% | PASS |
| mem_reads | `table_scan` | 889.57ms | 1.03s | 1.2× | 0.5% | PASS |
| mem_reads | `oltp_read_only` | 88.80ms | 91.39ms | 1.0× | 0.7% | PASS |
| mem_writes | `oltp_bulk_insert` | 145.12ms | 187.75ms | 1.3× | 0.4% | PASS |
| mem_writes | `oltp_insert` | 11.41ms | 20.92ms | 1.8× | 0.5% | PASS |
| mem_writes | `oltp_update_index` | 41.51ms | 84.82ms | 2.0× | 1.5% | PASS |
| mem_writes | `oltp_update_non_index` | 29.58ms | 47.77ms | 1.6× | 2.1% | PASS |
| mem_writes | `oltp_delete_insert` | 36.10ms | 60.98ms | 1.7× | 2.3% | PASS |
| mem_writes | `oltp_write_only` | 18.78ms | 33.34ms | 1.8× | 3.2% | PASS |
| mem_writes | `types_delete_insert` | 25.93ms | 35.41ms | 1.4× | 2.2% | PASS |
| mem_writes | `oltp_read_write` | 62.65ms | 83.89ms | 1.3× | 1.8% | PASS |
| file_reads | `oltp_point_select` | 44.18ms | 28.98ms | 0.7× | 1.3% | PASS |
| file_reads | `oltp_range_select` | 13.66ms | 12.07ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_sum_range` | 13.54ms | 11.47ms | 0.8× | 2.1% | PASS |
| file_reads | `oltp_order_range` | 2.65ms | 2.48ms | 0.9× | 1.5% | PASS |
| file_reads | `oltp_distinct_range` | 3.35ms | 3.15ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_index_scan` | 4.89ms | 4.96ms | 1.0× | 1.7% | PASS |
| file_reads | `select_random_points` | 20.36ms | 17.42ms | 0.9× | 2.4% | PASS |
| file_reads | `select_random_ranges` | 6.77ms | 4.88ms | 0.7× | 1.4% | PASS |
| file_reads | `covering_index_scan` | 6.93ms | 7.53ms | 1.1× | 0.9% | PASS |
| file_reads | `groupby_scan` | 23.66ms | 24.14ms | 1.0× | 0.5% | PASS |
| file_reads | `index_join` | 10.52ms | 7.92ms | 0.8× | 3.1% | PASS |
| file_reads | `index_join_scan` | 3.04ms | 5.70ms | 1.9× | 4.3% | PASS |
| file_reads | `types_table_scan` | 777.80ms | 934.78ms | 1.2× | 0.9% | PASS |
| file_reads | `table_scan` | 894.91ms | 1.03s | 1.1× | 0.5% | PASS |
| file_reads | `oltp_read_only` | 123.23ms | 101.72ms | 0.8× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 162.43ms | 204.03ms | 1.3× | 0.9% | PASS |
| file_writes | `oltp_insert` | 15.67ms | 26.51ms | 1.7× | 2.1% | PASS |
| file_writes | `oltp_update_index` | 69.71ms | 112.97ms | 1.6× | 3.3% | PASS |
| file_writes | `oltp_update_non_index` | 49.84ms | 60.04ms | 1.2× | 1.7% | PASS |
| file_writes | `oltp_delete_insert` | 57.21ms | 74.95ms | 1.3× | 1.8% | PASS |
| file_writes | `oltp_write_only` | 34.83ms | 42.48ms | 1.2× | 1.8% | PASS |
| file_writes | `types_delete_insert` | 43.89ms | 44.47ms | 1.0× | 2.9% | PASS |
| file_writes | `oltp_read_write` | 86.34ms | 100.42ms | 1.2× | 2.1% | PASS |
| ac_reads | `oltp_point_select` | 32.58ms | 30.19ms | 0.9× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 13.28ms | 12.27ms | 0.9× | 0.9% | PASS |
| ac_reads | `oltp_sum_range` | 12.62ms | 11.58ms | 0.9× | 1.6% | PASS |
| ac_reads | `oltp_order_range` | 2.58ms | 2.49ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_distinct_range` | 3.24ms | 3.12ms | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_index_scan` | 3.62ms | 5.04ms | 1.4× | 1.4% | PASS |
| ac_reads | `select_random_points` | 19.20ms | 17.40ms | 0.9× | 1.6% | PASS |
| ac_reads | `select_random_ranges` | 5.59ms | 4.84ms | 0.9× | 1.5% | PASS |
| ac_reads | `covering_index_scan` | 5.53ms | 7.36ms | 1.3× | 1.2% | PASS |
| ac_reads | `groupby_scan` | 23.56ms | 24.09ms | 1.0× | 0.7% | PASS |
| ac_reads | `index_join` | 10.05ms | 8.01ms | 0.8× | 2.0% | PASS |
| ac_reads | `index_join_scan` | 2.90ms | 5.52ms | 1.9× | 3.0% | PASS |
| ac_reads | `types_table_scan` | 772.28ms | 936.03ms | 1.2× | 0.8% | PASS |
| ac_reads | `table_scan` | 871.75ms | 1.03s | 1.2× | 0.9% | PASS |
| ac_reads | `oltp_read_only` | 106.51ms | 103.38ms | 1.0× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 9.11ms | 12.66ms | 1.4× | 1.0% | PASS |
| ac_writes | `oltp_insert_ac` | 10.33ms | 18.62ms | 1.8× | 2.2% | PASS |
| ac_writes | `oltp_update_index_ac` | 10.91ms | 30.73ms | 2.8× | 2.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 8.91ms | 16.27ms | 1.8× | 1.5% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 10.14ms | 24.86ms | 2.5× | 1.4% | PASS |
| ac_writes | `oltp_write_only_ac` | 10.03ms | 22.79ms | 2.3× | 1.5% | PASS |
| ac_writes | `types_delete_insert_ac` | 9.26ms | 16.92ms | 1.8× | 1.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 13.49ms | 27.89ms | 2.1× | 2.3% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 23.80ms | 23.66ms | 1.0× | 1.3% | PASS |
| mem_reads | `oltp_range_select` | 10.88ms | 11.05ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_sum_range` | 10.84ms | 10.56ms | 1.0× | 1.7% | PASS |
| mem_reads | `oltp_order_range` | 2.33ms | 2.38ms | 1.0× | 1.4% | PASS |
| mem_reads | `oltp_distinct_range` | 3.00ms | 3.01ms | 1.0× | 0.8% | PASS |
| mem_reads | `oltp_index_scan` | 2.85ms | 4.46ms | 1.6× | 1.5% | PASS |
| mem_reads | `select_random_points` | 16.80ms | 15.94ms | 0.9× | 1.0% | PASS |
| mem_reads | `select_random_ranges` | 4.59ms | 4.37ms | 1.0× | 1.0% | PASS |
| mem_reads | `covering_index_scan` | 4.81ms | 6.93ms | 1.4× | 1.0% | PASS |
| mem_reads | `groupby_scan` | 23.03ms | 23.64ms | 1.0× | 0.6% | PASS |
| mem_reads | `index_join` | 9.05ms | 7.71ms | 0.9× | 1.3% | PASS |
| mem_reads | `index_join_scan` | 2.61ms | 5.17ms | 2.0× | 1.9% | PASS |
| mem_reads | `types_table_scan` | 763.56ms | 933.46ms | 1.2× | 0.3% | PASS |
| mem_reads | `table_scan` | 868.96ms | 1.02s | 1.2× | 0.7% | PASS |
| mem_reads | `oltp_read_only` | 90.01ms | 91.39ms | 1.0× | 0.7% | PASS |
| mem_writes | `oltp_bulk_insert` | 144.12ms | 188.21ms | 1.3× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 11.68ms | 21.27ms | 1.8× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 43.74ms | 83.44ms | 1.9× | 1.8% | PASS |
| mem_writes | `oltp_update_non_index` | 31.65ms | 47.25ms | 1.5× | 1.2% | PASS |
| mem_writes | `oltp_delete_insert` | 34.36ms | 59.13ms | 1.7× | 2.4% | PASS |
| mem_writes | `oltp_write_only` | 18.50ms | 31.87ms | 1.7× | 1.9% | PASS |
| mem_writes | `types_delete_insert` | 25.74ms | 36.80ms | 1.4× | 1.7% | PASS |
| mem_writes | `oltp_read_write` | 61.87ms | 82.44ms | 1.3× | 1.1% | PASS |
| file_reads | `oltp_point_select` | 44.52ms | 28.72ms | 0.6× | 0.6% | PASS |
| file_reads | `oltp_range_select` | 12.96ms | 11.68ms | 0.9× | 0.8% | PASS |
| file_reads | `oltp_sum_range` | 12.94ms | 11.23ms | 0.9× | 1.1% | PASS |
| file_reads | `oltp_order_range` | 2.59ms | 2.46ms | 0.9× | 0.9% | PASS |
| file_reads | `oltp_distinct_range` | 3.25ms | 3.08ms | 0.9× | 0.8% | PASS |
| file_reads | `oltp_index_scan` | 4.90ms | 5.00ms | 1.0× | 1.2% | PASS |
| file_reads | `select_random_points` | 19.52ms | 16.73ms | 0.9× | 1.5% | PASS |
| file_reads | `select_random_ranges` | 6.74ms | 4.89ms | 0.7× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 6.92ms | 7.47ms | 1.1× | 0.6% | PASS |
| file_reads | `groupby_scan` | 23.30ms | 23.80ms | 1.0× | 0.7% | PASS |
| file_reads | `index_join` | 10.12ms | 8.08ms | 0.8× | 2.0% | PASS |
| file_reads | `index_join_scan` | 2.93ms | 5.40ms | 1.8× | 2.7% | PASS |
| file_reads | `types_table_scan` | 763.04ms | 927.53ms | 1.2× | 0.3% | PASS |
| file_reads | `table_scan` | 866.58ms | 1.02s | 1.2× | 0.7% | PASS |
| file_reads | `oltp_read_only` | 119.87ms | 98.98ms | 0.8× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 157.00ms | 202.11ms | 1.3× | 0.7% | PASS |
| file_writes | `oltp_insert` | 15.18ms | 25.37ms | 1.7× | 1.2% | PASS |
| file_writes | `oltp_update_index` | 60.49ms | 100.98ms | 1.7× | 1.9% | PASS |
| file_writes | `oltp_update_non_index` | 46.81ms | 57.84ms | 1.2× | 1.8% | PASS |
| file_writes | `oltp_delete_insert` | 54.08ms | 76.96ms | 1.4× | 1.9% | PASS |
| file_writes | `oltp_write_only` | 33.56ms | 43.05ms | 1.3× | 1.5% | PASS |
| file_writes | `types_delete_insert` | 39.74ms | 43.73ms | 1.1× | 2.5% | PASS |
| file_writes | `oltp_read_write` | 83.20ms | 99.32ms | 1.2× | 2.4% | PASS |
| ac_reads | `oltp_point_select` | 32.07ms | 29.67ms | 0.9× | 0.9% | PASS |
| ac_reads | `oltp_range_select` | 12.49ms | 11.95ms | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_sum_range` | 12.31ms | 11.43ms | 0.9× | 1.0% | PASS |
| ac_reads | `oltp_order_range` | 2.53ms | 2.52ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_distinct_range` | 3.21ms | 3.15ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 3.81ms | 5.27ms | 1.4× | 1.3% | PASS |
| ac_reads | `select_random_points` | 19.31ms | 17.32ms | 0.9× | 1.4% | PASS |
| ac_reads | `select_random_ranges` | 5.67ms | 5.02ms | 0.9× | 1.7% | PASS |
| ac_reads | `covering_index_scan` | 5.61ms | 7.63ms | 1.4× | 0.5% | PASS |
| ac_reads | `groupby_scan` | 23.33ms | 24.11ms | 1.0× | 0.3% | PASS |
| ac_reads | `index_join` | 10.35ms | 8.74ms | 0.8× | 1.4% | PASS |
| ac_reads | `index_join_scan` | 2.95ms | 5.56ms | 1.9× | 2.2% | PASS |
| ac_reads | `types_table_scan` | 764.97ms | 930.69ms | 1.2× | 0.4% | PASS |
| ac_reads | `table_scan` | 867.46ms | 1.02s | 1.2× | 0.6% | PASS |
| ac_reads | `oltp_read_only` | 101.09ms | 99.09ms | 1.0× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 9.19ms | 12.53ms | 1.4× | 1.4% | PASS |
| ac_writes | `oltp_insert_ac` | 10.21ms | 21.67ms | 2.1× | 1.2% | PASS |
| ac_writes | `oltp_update_index_ac` | 10.59ms | 29.55ms | 2.8× | 1.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 8.94ms | 16.21ms | 1.8× | 2.1% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 10.09ms | 22.76ms | 2.3× | 2.2% | PASS |
| ac_writes | `oltp_write_only_ac` | 10.24ms | 23.19ms | 2.3× | 2.9% | PASS |
| ac_writes | `types_delete_insert_ac` | 9.30ms | 16.78ms | 1.8× | 1.2% | PASS |
| ac_writes | `oltp_read_write_ac` | 13.32ms | 27.11ms | 2.0× | 1.5% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 21.49ms | 24.12ms | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_range_select` | 14.02ms | 14.93ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_sum_range` | 13.42ms | 13.78ms | 1.0× | 0.7% | PASS |
| mem_reads | `oltp_order_range` | 2.69ms | 2.75ms | 1.0× | 1.3% | PASS |
| mem_reads | `oltp_distinct_range` | 3.40ms | 3.42ms | 1.0× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 3.29ms | 4.33ms | 1.3× | 1.4% | PASS |
| mem_reads | `select_random_points` | 21.02ms | 21.55ms | 1.0× | 0.5% | PASS |
| mem_reads | `select_random_ranges` | 5.16ms | 5.88ms | 1.1× | 1.3% | PASS |
| mem_reads | `covering_index_scan` | 4.79ms | 6.71ms | 1.4× | 0.7% | PASS |
| mem_reads | `groupby_scan` | 25.01ms | 26.74ms | 1.1× | 0.5% | PASS |
| mem_reads | `index_join` | 5.79ms | 8.06ms | 1.4× | 1.4% | PASS |
| mem_reads | `index_join_scan` | 2.92ms | 4.90ms | 1.7× | 4.0% | PASS |
| mem_reads | `types_table_scan` | 773.71ms | 923.42ms | 1.2× | 0.4% | PASS |
| mem_reads | `table_scan` | 890.82ms | 1.03s | 1.2× | 1.6% | PASS |
| mem_reads | `oltp_read_only` | 99.39ms | 108.90ms | 1.1× | 1.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 151.56ms | 191.78ms | 1.3× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 12.39ms | 20.00ms | 1.6× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 45.78ms | 81.22ms | 1.8× | 1.6% | PASS |
| mem_writes | `oltp_update_non_index` | 33.10ms | 53.27ms | 1.6× | 2.2% | PASS |
| mem_writes | `oltp_delete_insert` | 33.99ms | 59.59ms | 1.8× | 1.8% | PASS |
| mem_writes | `oltp_write_only` | 18.58ms | 35.86ms | 1.9× | 1.3% | PASS |
| mem_writes | `types_delete_insert` | 20.96ms | 33.99ms | 1.6× | 1.0% | PASS |
| mem_writes | `oltp_read_write` | 64.48ms | 92.86ms | 1.4× | 1.0% | PASS |
| file_reads | `oltp_point_select` | 41.17ms | 28.68ms | 0.7× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 15.93ms | 15.44ms | 1.0× | 0.8% | PASS |
| file_reads | `oltp_sum_range` | 15.55ms | 14.42ms | 0.9× | 1.0% | PASS |
| file_reads | `oltp_order_range` | 2.87ms | 2.83ms | 1.0× | 1.2% | PASS |
| file_reads | `oltp_distinct_range` | 3.56ms | 3.49ms | 1.0× | 2.4% | PASS |
| file_reads | `oltp_index_scan` | 5.42ms | 4.99ms | 0.9× | 1.3% | PASS |
| file_reads | `select_random_points` | 23.21ms | 22.35ms | 1.0× | 0.7% | PASS |
| file_reads | `select_random_ranges` | 7.23ms | 6.36ms | 0.9× | 1.3% | PASS |
| file_reads | `covering_index_scan` | 6.90ms | 7.32ms | 1.1× | 0.6% | PASS |
| file_reads | `groupby_scan` | 25.31ms | 26.84ms | 1.1× | 0.4% | PASS |
| file_reads | `index_join` | 7.00ms | 8.51ms | 1.2× | 1.7% | PASS |
| file_reads | `index_join_scan` | 3.15ms | 5.05ms | 1.6× | 1.8% | PASS |
| file_reads | `types_table_scan` | 776.44ms | 918.26ms | 1.2× | 0.5% | PASS |
| file_reads | `table_scan` | 889.54ms | 1.02s | 1.2× | 1.2% | PASS |
| file_reads | `oltp_read_only` | 128.37ms | 115.92ms | 0.9× | 0.5% | PASS |
| file_writes | `oltp_bulk_insert` | 161.54ms | 202.21ms | 1.3× | 0.8% | PASS |
| file_writes | `oltp_insert` | 15.80ms | 23.63ms | 1.5× | 1.3% | PASS |
| file_writes | `oltp_update_index` | 61.22ms | 95.88ms | 1.6× | 1.9% | PASS |
| file_writes | `oltp_update_non_index` | 46.65ms | 60.45ms | 1.3× | 2.0% | PASS |
| file_writes | `oltp_delete_insert` | 47.97ms | 71.00ms | 1.5× | 1.6% | PASS |
| file_writes | `oltp_write_only` | 30.65ms | 42.19ms | 1.4× | 2.1% | PASS |
| file_writes | `types_delete_insert` | 30.26ms | 39.41ms | 1.3× | 1.6% | PASS |
| file_writes | `oltp_read_write` | 83.30ms | 105.06ms | 1.3× | 1.6% | PASS |
| ac_reads | `oltp_point_select` | 29.32ms | 30.24ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_range_select` | 15.53ms | 15.81ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_sum_range` | 14.96ms | 14.68ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_order_range` | 2.87ms | 2.88ms | 1.0× | 2.0% | PASS |
| ac_reads | `oltp_distinct_range` | 3.56ms | 3.53ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_index_scan` | 4.24ms | 5.16ms | 1.2× | 2.0% | PASS |
| ac_reads | `select_random_points` | 22.31ms | 22.89ms | 1.0× | 1.0% | PASS |
| ac_reads | `select_random_ranges` | 5.99ms | 6.55ms | 1.1× | 1.2% | PASS |
| ac_reads | `covering_index_scan` | 5.62ms | 7.56ms | 1.3× | 0.7% | PASS |
| ac_reads | `groupby_scan` | 25.36ms | 27.05ms | 1.1× | 0.4% | PASS |
| ac_reads | `index_join` | 6.40ms | 8.50ms | 1.3× | 1.4% | PASS |
| ac_reads | `index_join_scan` | 3.12ms | 5.08ms | 1.6× | 1.5% | PASS |
| ac_reads | `types_table_scan` | 774.50ms | 918.94ms | 1.2× | 0.5% | PASS |
| ac_reads | `table_scan` | 871.33ms | 1.02s | 1.2× | 1.2% | PASS |
| ac_reads | `oltp_read_only` | 110.65ms | 116.11ms | 1.0× | 1.1% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 8.75ms | 12.62ms | 1.4× | 1.3% | PASS |
| ac_writes | `oltp_insert_ac` | 9.96ms | 20.56ms | 2.1× | 2.6% | PASS |
| ac_writes | `oltp_update_index_ac` | 12.39ms | 30.07ms | 2.4× | 7.4% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 9.22ms | 16.85ms | 1.8× | 3.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 9.84ms | 23.04ms | 2.3× | 3.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 9.81ms | 21.15ms | 2.2× | 1.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 8.68ms | 16.80ms | 1.9× | 1.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 13.31ms | 26.45ms | 2.0× | 1.2% | PASS |

</details>

</details>

## Version-control latency

Wall time: 4m 49s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 25.16ms | 38.00ms | 66.2% | 1.2% | PASS |
| `status_dirty_many_tables` | 27.73ms | 42.00ms | 66.0% | 0.7% | PASS |
| `diff_regular_working_one_table` | 21.68ms | 33.00ms | 65.7% | 0.6% | PASS |
| `diff_regular_working_many_tables` | 30.78ms | 50.00ms | 61.6% | 1.2% | PASS |
| `diff_stat_working_many_tables` | 30.28ms | 48.00ms | 63.1% | 0.8% | PASS |
| `diff_schema_working_many_tables` | 30.58ms | 48.00ms | 63.7% | 0.7% | PASS |
| `branch_list_many_branches` | 15.34ms | 25.00ms | 61.4% | 0.9% | PASS |
| `branch_create_delete` | 17.06ms | 27.00ms | 63.2% | 1.3% | PASS |
| `at_literal_deep_history` | 16.84ms | 28.00ms | 60.2% | 0.8% | PASS |
| `diff_literal_deep_history` | 16.52ms | 28.00ms | 59.0% | 1.0% | PASS |
| `history_literal_deep_history` | 17.01ms | 30.00ms | 56.7% | 0.9% | PASS |
| `checkout_branch_clean` | 22.93ms | 43.00ms | 53.3% | 1.1% | PASS |
| `merge_data_no_conflicts` | 17.94ms | 33.00ms | 54.4% | 0.9% | PASS |
| `merge_data_secondary_index` | 619.63ms | 944.00ms | 65.6% | 1.3% | PASS |
| `merge_schema_no_conflicts` | 14.55ms | 24.00ms | 60.6% | 1.2% | PASS |
| `merge_data_conflicts` | 19.99ms | 33.00ms | 60.6% | 1.0% | PASS |
| `merge_data_conflicts_with_resolve` | 21.02ms | 34.00ms | 61.8% | 0.9% | PASS |

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

candidate: 0c9ea2e.

Fixture file bytes: base 107,968,301; delta 115,879,032.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 51.732 | 49.991–57.999 | 46.662 | 1933.04 | 107968301 |
| push_initial | candidate | 345.226 | 341.342–352.411 | 344.244 | 289.67 | 107956807 |
| clone_initial | candidate | 467.837 | 465.763–469.238 | 456.492 | 213.75 | 108475105 |
| push_noop | candidate | 0.357 | 0.309–0.374 | 0.182 | — | 0 |
| fetch_noop | candidate | 0.099 | 0.094–0.113 | 0.100 | — | 0 |
| push_incremental | candidate | 139.796 | 136.813–140.577 | 137.876 | 7.15 | 7909775 |
| fetch_incremental | candidate | 60.910 | 60.560–61.823 | 59.569 | 16.42 | 7910084 |
| pull_fast_forward | candidate | 55.671 | 54.769–56.110 | 54.235 | 17.96 | 7910844 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 90.40 | 175.487 | 108.763 | 18329 | 2066 |
| clone_initial | candidate | 90.90 | 295.154 | 110.833 | 47260 | 2353 |
| push_noop | candidate | 41.41 | 0.073 | 0.000 | 27 | 2 |
| fetch_noop | candidate | 41.41 | 0.135 | 0.000 | 42 | 0 |
| push_incremental | candidate | 41.41 | 96.786 | 7.910 | 22394 | 125 |
| fetch_incremental | candidate | 41.41 | 34.472 | 7.910 | 10810 | 124 |
| pull_fast_forward | candidate | 41.41 | 26.497 | 8.959 | 7032 | 142 |

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

candidate: 0c9ea2e.

Fixture file bytes: base 1,087,718,066; delta 1,103,525,194.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 4103.742 | 3163.234–4147.656 | 420.659 | 243.68 | 1087718066 |
| push_initial | candidate | 7007.819 | 6055.574–7043.965 | 3390.409 | 142.70 | 1079566310 |
| clone_initial | candidate | 5022.212 | 5007.020–5056.488 | 4996.132 | 199.12 | 1102407396 |
| push_noop | candidate | 0.423 | 0.358–0.424 | 0.184 | — | 0 |
| fetch_noop | candidate | 0.096 | 0.093–0.099 | 0.097 | — | 0 |
| push_incremental | candidate | 767.375 | 761.157–769.096 | 763.475 | 1.30 | 15806003 |
| fetch_incremental | candidate | 186.790 | 185.067–191.920 | 183.532 | 5.35 | 15806513 |
| pull_fast_forward | candidate | 191.035 | 190.174–191.476 | 186.306 | 5.23 | 15807273 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 134.81 | 2091.855 | 1087.622 | 263492 | 20541 |
| clone_initial | candidate | 123.96 | 3517.805 | 1133.868 | 612277 | 32259 |
| push_noop | candidate | 43.88 | 0.078 | 0.000 | 28 | 2 |
| fetch_noop | candidate | 43.88 | 0.137 | 0.000 | 42 | 0 |
| push_incremental | candidate | 44.83 | 617.242 | 15.928 | 149902 | 313 |
| fetch_incremental | candidate | 50.49 | 62.175 | 15.928 | 13883 | 313 |
| pull_fast_forward | candidate | 50.21 | 62.108 | 16.978 | 13878 | 331 |

</details>


</details>

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh`, `test/vc_perf_ceiling.sh`, and `test/remote_perf.py`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
