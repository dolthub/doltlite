# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-11-12 11:31 UTC
>
> Commit: [`cb73254873f352fa374bb20522663cfbdaa0bc74`](https://github.com/dolthub/doltlite/commit/cb73254873f352fa374bb20522663cfbdaa0bc74)
>
> Runner: ubuntu24 20261101.360.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/43192315071)

The SQL workloads compare optimized DoltLite against stock SQLite on the same GitHub-hosted runner, alternating execution order on each repetition. Version-control sections measure DoltLite latency. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 8.22s | 9.15s | 1.1× | 1.8% | **PASS** |
| Writes | 1.73s | 2.64s | 1.5× | 1.5% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 8.97s | 9.34s | 1.0× | 1.6% | **PASS** |
| Writes | 3.77s | 3.78s | 1.0× | 4.5% | **PASS** |
| Autocommit writes | 795.40ms | 2.36s | 3.0× | 7.4% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 1.74s | 1.82s | 1.0× | 2.6% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.66s | 3.06s | 1.2× | 1.3% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 1.63s | 1.84s | 1.1× | 2.4% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.20s | 2.43s | 1.1× | 1.3% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 280.77ms | 449.39ms | 1.6× | 1.8% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 604.93ms | 930.88ms | 1.5× | 1.2% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 373.28ms | 564.44ms | 1.5× | 2.5% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 467.63ms | 690.46ms | 1.5× | 1.0% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 1.76s | 1.79s | 1.0× | 1.7% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 3.00s | 3.16s | 1.1× | 1.5% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 1.90s | 1.95s | 1.0× | 2.3% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.30s | 2.45s | 1.1× | 0.9% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 747.12ms | 717.03ms | 1.0× | 3.8% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 968.92ms | 1.04s | 1.1× | 4.5% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 993.24ms | 946.53ms | 1.0× | 4.5% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 1.07s | 1.07s | 1.0× | 7.2% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 1.80s | 1.85s | 1.0× | 2.3% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 3.05s | 3.21s | 1.1× | 2.3% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 1.62s | 1.80s | 1.1× | 2.6% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.30s | 2.49s | 1.1× | 1.1% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 179.99ms | 519.30ms | 2.9× | 4.9% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 200.60ms | 646.40ms | 3.2× | 7.6% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 219.06ms | 509.69ms | 2.3× | 6.9% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 195.75ms | 688.13ms | 3.5× | 31.4% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 18.05ms | 20.18ms | 1.1× | 2.7% | PASS |
| mem_reads | `oltp_range_select` | 7.31ms | 8.32ms | 1.1× | 3.5% | PASS |
| mem_reads | `oltp_sum_range` | 6.90ms | 8.10ms | 1.2× | 2.1% | PASS |
| mem_reads | `oltp_order_range` | 1.82ms | 1.99ms | 1.1× | 2.2% | PASS |
| mem_reads | `oltp_distinct_range` | 2.37ms | 2.58ms | 1.1× | 1.9% | PASS |
| mem_reads | `oltp_index_scan` | 2.84ms | 4.13ms | 1.5× | 2.5% | PASS |
| mem_reads | `select_random_points` | 11.83ms | 10.77ms | 0.9× | 4.6% | PASS |
| mem_reads | `select_random_ranges` | 3.19ms | 3.27ms | 1.0× | 2.1% | PASS |
| mem_reads | `covering_index_scan` | 4.50ms | 6.95ms | 1.5× | 3.3% | PASS |
| mem_reads | `groupby_scan` | 19.17ms | 20.11ms | 1.0× | 1.8% | PASS |
| mem_reads | `index_join` | 3.77ms | 6.24ms | 1.7× | 5.2% | PASS |
| mem_reads | `index_join_scan` | 2.57ms | 4.44ms | 1.7× | 5.5% | PASS |
| mem_reads | `types_table_scan` | 734.73ms | 785.21ms | 1.1× | 2.1% | PASS |
| mem_reads | `table_scan` | 859.75ms | 875.00ms | 1.0× | 3.1% | PASS |
| mem_reads | `oltp_read_only` | 61.80ms | 66.13ms | 1.1× | 2.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 110.56ms | 170.55ms | 1.5× | 1.2% | PASS |
| mem_writes | `oltp_insert` | 9.56ms | 15.48ms | 1.6× | 1.8% | PASS |
| mem_writes | `oltp_update_index` | 34.19ms | 63.80ms | 1.9× | 1.8% | PASS |
| mem_writes | `oltp_update_non_index` | 24.65ms | 39.45ms | 1.6× | 2.1% | PASS |
| mem_writes | `oltp_delete_insert` | 29.60ms | 46.82ms | 1.6× | 1.6% | PASS |
| mem_writes | `oltp_write_only` | 14.79ms | 30.04ms | 2.0× | 1.8% | PASS |
| mem_writes | `types_delete_insert` | 17.03ms | 21.40ms | 1.3× | 2.0% | PASS |
| mem_writes | `oltp_read_write` | 40.38ms | 61.85ms | 1.5× | 2.6% | PASS |
| file_reads | `oltp_point_select` | 66.92ms | 31.77ms | 0.5× | 1.8% | PASS |
| file_reads | `oltp_range_select` | 11.94ms | 8.71ms | 0.7× | 1.9% | PASS |
| file_reads | `oltp_sum_range` | 11.70ms | 8.63ms | 0.7× | 1.9% | PASS |
| file_reads | `oltp_order_range` | 2.34ms | 2.05ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_distinct_range` | 2.86ms | 2.59ms | 0.9× | 1.9% | PASS |
| file_reads | `oltp_index_scan` | 8.31ms | 5.18ms | 0.6× | 1.6% | PASS |
| file_reads | `select_random_points` | 15.21ms | 10.38ms | 0.7× | 3.2% | PASS |
| file_reads | `select_random_ranges` | 8.17ms | 4.39ms | 0.5× | 1.3% | PASS |
| file_reads | `covering_index_scan` | 9.73ms | 7.14ms | 0.7× | 1.7% | PASS |
| file_reads | `groupby_scan` | 18.39ms | 18.56ms | 1.0× | 1.3% | PASS |
| file_reads | `index_join` | 6.73ms | 5.98ms | 0.9× | 4.0% | PASS |
| file_reads | `index_join_scan` | 3.00ms | 4.25ms | 1.4× | 4.0% | PASS |
| file_reads | `types_table_scan` | 690.09ms | 746.69ms | 1.1× | 1.0% | PASS |
| file_reads | `table_scan` | 776.77ms | 845.56ms | 1.1× | 1.4% | PASS |
| file_reads | `oltp_read_only` | 128.92ms | 83.18ms | 0.6× | 1.2% | PASS |
| file_writes | `oltp_bulk_insert` | 150.41ms | 217.89ms | 1.4× | 3.2% | PASS |
| file_writes | `oltp_insert` | 19.75ms | 26.50ms | 1.3× | 5.7% | PASS |
| file_writes | `oltp_update_index` | 121.15ms | 107.80ms | 0.9× | 1.7% | PASS |
| file_writes | `oltp_update_non_index` | 101.01ms | 78.87ms | 0.8× | 1.1% | PASS |
| file_writes | `oltp_delete_insert` | 105.88ms | 89.40ms | 0.8× | 4.5% | PASS |
| file_writes | `oltp_write_only` | 81.10ms | 60.14ms | 0.7× | 3.7% | PASS |
| file_writes | `types_delete_insert` | 66.24ms | 45.23ms | 0.7× | 14.7% | PASS |
| file_writes | `oltp_read_write` | 101.58ms | 91.19ms | 0.9× | 3.9% | PASS |
| ac_reads | `oltp_point_select` | 31.12ms | 30.90ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_range_select` | 7.76ms | 8.46ms | 1.1× | 1.8% | PASS |
| ac_reads | `oltp_sum_range` | 7.34ms | 8.39ms | 1.1× | 1.5% | PASS |
| ac_reads | `oltp_order_range` | 1.82ms | 1.96ms | 1.1× | 3.0% | PASS |
| ac_reads | `oltp_distinct_range` | 2.40ms | 2.53ms | 1.1× | 1.8% | PASS |
| ac_reads | `oltp_index_scan` | 4.17ms | 4.70ms | 1.1× | 2.2% | PASS |
| ac_reads | `select_random_points` | 11.43ms | 10.52ms | 0.9× | 7.0% | PASS |
| ac_reads | `select_random_ranges` | 5.16ms | 4.75ms | 0.9× | 2.3% | PASS |
| ac_reads | `covering_index_scan` | 6.03ms | 7.42ms | 1.2× | 2.7% | PASS |
| ac_reads | `groupby_scan` | 17.73ms | 18.59ms | 1.0× | 1.8% | PASS |
| ac_reads | `index_join` | 5.12ms | 6.47ms | 1.3× | 3.7% | PASS |
| ac_reads | `index_join_scan` | 2.56ms | 4.12ms | 1.6× | 3.8% | PASS |
| ac_reads | `types_table_scan` | 694.98ms | 744.40ms | 1.1× | 1.6% | PASS |
| ac_reads | `table_scan` | 914.04ms | 911.58ms | 1.0× | 2.5% | PASS |
| ac_reads | `oltp_read_only` | 84.50ms | 83.65ms | 1.0× | 2.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 20.98ms | 56.25ms | 2.7× | 13.4% | PASS |
| ac_writes | `oltp_insert_ac` | 22.32ms | 62.86ms | 2.8× | 3.7% | PASS |
| ac_writes | `oltp_update_index_ac` | 23.99ms | 71.99ms | 3.0× | 5.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 20.47ms | 57.92ms | 2.8× | 3.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 22.68ms | 66.48ms | 2.9× | 4.2% | PASS |
| ac_writes | `oltp_write_only_ac` | 22.09ms | 63.71ms | 2.9× | 2.7% | PASS |
| ac_writes | `types_delete_insert_ac` | 20.55ms | 64.31ms | 3.1× | 5.6% | PASS |
| ac_writes | `oltp_read_write_ac` | 26.91ms | 75.78ms | 2.8× | 5.6% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 35.48ms | 38.23ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_range_select` | 16.82ms | 16.66ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_sum_range` | 15.20ms | 14.98ms | 1.0× | 1.0% | PASS |
| mem_reads | `oltp_order_range` | 3.33ms | 3.40ms | 1.0× | 1.5% | PASS |
| mem_reads | `oltp_distinct_range` | 4.46ms | 4.52ms | 1.0× | 1.3% | PASS |
| mem_reads | `oltp_index_scan` | 4.03ms | 6.53ms | 1.6× | 2.1% | PASS |
| mem_reads | `select_random_points` | 21.91ms | 21.61ms | 1.0× | 1.5% | PASS |
| mem_reads | `select_random_ranges` | 6.90ms | 6.71ms | 1.0× | 1.3% | PASS |
| mem_reads | `covering_index_scan` | 7.71ms | 11.29ms | 1.5× | 0.7% | PASS |
| mem_reads | `groupby_scan` | 34.77ms | 35.53ms | 1.0× | 1.0% | PASS |
| mem_reads | `index_join` | 10.42ms | 10.27ms | 1.0× | 1.9% | PASS |
| mem_reads | `index_join_scan` | 3.79ms | 6.46ms | 1.7× | 1.8% | PASS |
| mem_reads | `types_table_scan` | 1.08s | 1.30s | 1.2× | 1.3% | PASS |
| mem_reads | `table_scan` | 1.26s | 1.44s | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_read_only` | 142.10ms | 143.74ms | 1.0× | 1.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 244.63ms | 336.69ms | 1.4× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 18.08ms | 34.87ms | 1.9× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 67.25ms | 136.15ms | 2.0× | 1.4% | PASS |
| mem_writes | `oltp_update_non_index` | 49.69ms | 76.45ms | 1.5× | 1.3% | PASS |
| mem_writes | `oltp_delete_insert` | 54.86ms | 98.88ms | 1.8× | 1.2% | PASS |
| mem_writes | `oltp_write_only` | 28.92ms | 52.97ms | 1.8× | 1.2% | PASS |
| mem_writes | `types_delete_insert` | 39.35ms | 59.23ms | 1.5× | 1.5% | PASS |
| mem_writes | `oltp_read_write` | 102.16ms | 135.64ms | 1.3× | 1.2% | PASS |
| file_reads | `oltp_point_select` | 102.53ms | 55.24ms | 0.5× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 24.15ms | 18.49ms | 0.8× | 1.3% | PASS |
| file_reads | `oltp_sum_range` | 22.95ms | 16.98ms | 0.7× | 1.4% | PASS |
| file_reads | `oltp_order_range` | 4.17ms | 3.61ms | 0.9× | 2.4% | PASS |
| file_reads | `oltp_distinct_range` | 5.32ms | 4.79ms | 0.9× | 1.5% | PASS |
| file_reads | `oltp_index_scan` | 10.92ms | 8.34ms | 0.8× | 1.5% | PASS |
| file_reads | `select_random_points` | 30.86ms | 24.38ms | 0.8× | 2.4% | PASS |
| file_reads | `select_random_ranges` | 13.94ms | 8.60ms | 0.6× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 14.98ms | 13.18ms | 0.9× | 1.0% | PASS |
| file_reads | `groupby_scan` | 35.64ms | 35.65ms | 1.0× | 1.0% | PASS |
| file_reads | `index_join` | 14.62ms | 11.52ms | 0.8× | 2.1% | PASS |
| file_reads | `index_join_scan` | 4.71ms | 6.65ms | 1.4× | 1.8% | PASS |
| file_reads | `types_table_scan` | 1.10s | 1.31s | 1.2× | 1.9% | PASS |
| file_reads | `table_scan` | 1.37s | 1.46s | 1.1× | 1.4% | PASS |
| file_reads | `oltp_read_only` | 250.86ms | 174.75ms | 0.7× | 1.9% | PASS |
| file_writes | `oltp_bulk_insert` | 268.94ms | 352.06ms | 1.3× | 1.0% | PASS |
| file_writes | `oltp_insert` | 26.18ms | 40.33ms | 1.5× | 1.4% | PASS |
| file_writes | `oltp_update_index` | 133.23ms | 157.16ms | 1.2× | 7.3% | PASS |
| file_writes | `oltp_update_non_index` | 117.64ms | 91.13ms | 0.8× | 10.5% | PASS |
| file_writes | `oltp_delete_insert` | 96.81ms | 116.65ms | 1.2× | 1.6% | PASS |
| file_writes | `oltp_write_only` | 89.05ms | 65.99ms | 0.7× | 8.1% | PASS |
| file_writes | `types_delete_insert` | 74.76ms | 68.93ms | 0.9× | 1.7% | PASS |
| file_writes | `oltp_read_write` | 162.31ms | 151.03ms | 0.9× | 9.4% | PASS |
| ac_reads | `oltp_point_select` | 61.25ms | 56.77ms | 0.9× | 2.0% | PASS |
| ac_reads | `oltp_range_select` | 21.12ms | 19.04ms | 0.9× | 2.3% | PASS |
| ac_reads | `oltp_sum_range` | 19.52ms | 17.56ms | 0.9× | 2.7% | PASS |
| ac_reads | `oltp_order_range` | 3.91ms | 3.68ms | 0.9× | 2.4% | PASS |
| ac_reads | `oltp_distinct_range` | 4.98ms | 4.82ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_index_scan` | 6.76ms | 8.44ms | 1.2× | 2.3% | PASS |
| ac_reads | `select_random_points` | 27.24ms | 25.43ms | 0.9× | 3.2% | PASS |
| ac_reads | `select_random_ranges` | 9.75ms | 8.70ms | 0.9× | 1.8% | PASS |
| ac_reads | `covering_index_scan` | 10.82ms | 13.36ms | 1.2× | 2.4% | PASS |
| ac_reads | `groupby_scan` | 35.80ms | 36.19ms | 1.0× | 1.2% | PASS |
| ac_reads | `index_join` | 13.14ms | 11.93ms | 0.9× | 4.2% | PASS |
| ac_reads | `index_join_scan` | 4.42ms | 6.94ms | 1.6× | 2.6% | PASS |
| ac_reads | `types_table_scan` | 1.23s | 1.35s | 1.1× | 1.1% | PASS |
| ac_reads | `table_scan` | 1.42s | 1.48s | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_read_only` | 186.30ms | 176.73ms | 0.9× | 1.3% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.15ms | 63.98ms | 2.9× | 6.5% | PASS |
| ac_writes | `oltp_insert_ac` | 24.01ms | 76.37ms | 3.2× | 5.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.49ms | 94.62ms | 3.6× | 8.3% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.38ms | 75.57ms | 3.2× | 11.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.96ms | 85.77ms | 3.4× | 6.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 24.64ms | 82.14ms | 3.3× | 7.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 23.16ms | 76.21ms | 3.3× | 9.1% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.80ms | 91.75ms | 2.9× | 7.9% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.24ms | 23.50ms | 1.0× | 5.2% | PASS |
| mem_reads | `oltp_range_select` | 10.86ms | 9.81ms | 0.9× | 3.6% | PASS |
| mem_reads | `oltp_sum_range` | 10.29ms | 9.65ms | 0.9× | 1.7% | PASS |
| mem_reads | `oltp_order_range` | 2.14ms | 2.17ms | 1.0× | 1.5% | PASS |
| mem_reads | `oltp_distinct_range` | 2.66ms | 2.63ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 2.57ms | 4.43ms | 1.7× | 2.3% | PASS |
| mem_reads | `select_random_points` | 16.17ms | 14.54ms | 0.9× | 7.9% | PASS |
| mem_reads | `select_random_ranges` | 4.40ms | 3.94ms | 0.9× | 2.4% | PASS |
| mem_reads | `covering_index_scan` | 3.88ms | 6.52ms | 1.7× | 2.4% | PASS |
| mem_reads | `groupby_scan` | 20.18ms | 19.60ms | 1.0× | 1.6% | PASS |
| mem_reads | `index_join` | 8.07ms | 7.63ms | 0.9× | 3.3% | PASS |
| mem_reads | `index_join_scan` | 2.86ms | 5.68ms | 2.0× | 4.7% | PASS |
| mem_reads | `types_table_scan` | 672.21ms | 788.74ms | 1.2× | 2.4% | PASS |
| mem_reads | `table_scan` | 775.47ms | 862.69ms | 1.1× | 2.4% | PASS |
| mem_reads | `oltp_read_only` | 77.00ms | 75.49ms | 1.0× | 2.4% | PASS |
| mem_writes | `oltp_bulk_insert` | 136.99ms | 189.46ms | 1.4× | 1.2% | PASS |
| mem_writes | `oltp_insert` | 10.85ms | 21.34ms | 2.0× | 1.6% | PASS |
| mem_writes | `oltp_update_index` | 45.18ms | 88.54ms | 2.0× | 2.4% | PASS |
| mem_writes | `oltp_update_non_index` | 35.19ms | 49.12ms | 1.4× | 2.1% | PASS |
| mem_writes | `oltp_delete_insert` | 36.22ms | 64.37ms | 1.8× | 2.6% | PASS |
| mem_writes | `oltp_write_only` | 19.46ms | 35.11ms | 1.8× | 2.8% | PASS |
| mem_writes | `types_delete_insert` | 25.34ms | 36.06ms | 1.4× | 3.1% | PASS |
| mem_writes | `oltp_read_write` | 64.05ms | 80.44ms | 1.3× | 4.4% | PASS |
| file_reads | `oltp_point_select` | 69.13ms | 33.59ms | 0.5× | 2.3% | PASS |
| file_reads | `oltp_range_select` | 14.81ms | 10.29ms | 0.7× | 2.5% | PASS |
| file_reads | `oltp_sum_range` | 14.39ms | 10.06ms | 0.7× | 2.3% | PASS |
| file_reads | `oltp_order_range` | 2.66ms | 2.26ms | 0.8× | 2.0% | PASS |
| file_reads | `oltp_distinct_range` | 3.15ms | 2.73ms | 0.9× | 2.1% | PASS |
| file_reads | `oltp_index_scan` | 7.59ms | 5.47ms | 0.7× | 2.8% | PASS |
| file_reads | `select_random_points` | 19.34ms | 13.88ms | 0.7× | 2.8% | PASS |
| file_reads | `select_random_ranges` | 9.17ms | 5.12ms | 0.6× | 1.8% | PASS |
| file_reads | `covering_index_scan` | 9.16ms | 7.46ms | 0.8× | 1.8% | PASS |
| file_reads | `groupby_scan` | 20.28ms | 19.47ms | 1.0× | 1.9% | PASS |
| file_reads | `index_join` | 11.32ms | 7.90ms | 0.7× | 4.3% | PASS |
| file_reads | `index_join_scan` | 3.39ms | 4.92ms | 1.5× | 3.1% | PASS |
| file_reads | `types_table_scan` | 701.08ms | 808.98ms | 1.2× | 1.9% | PASS |
| file_reads | `table_scan` | 859.17ms | 925.27ms | 1.1× | 2.9% | PASS |
| file_reads | `oltp_read_only` | 157.73ms | 96.15ms | 0.6× | 2.4% | PASS |
| file_writes | `oltp_bulk_insert` | 200.22ms | 250.55ms | 1.3× | 2.4% | PASS |
| file_writes | `oltp_insert` | 22.10ms | 42.40ms | 1.9× | 11.7% | PASS |
| file_writes | `oltp_update_index` | 141.10ms | 160.19ms | 1.1× | 4.8% | PASS |
| file_writes | `oltp_update_non_index` | 120.71ms | 99.02ms | 0.8× | 3.5% | PASS |
| file_writes | `oltp_delete_insert` | 151.08ms | 126.89ms | 0.8× | 3.7% | PASS |
| file_writes | `oltp_write_only` | 110.69ms | 79.33ms | 0.7× | 5.0% | PASS |
| file_writes | `types_delete_insert` | 104.34ms | 69.57ms | 0.7× | 8.6% | PASS |
| file_writes | `oltp_read_write` | 143.00ms | 118.58ms | 0.8× | 4.1% | PASS |
| ac_reads | `oltp_point_select` | 36.88ms | 33.62ms | 0.9× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 11.02ms | 10.12ms | 0.9× | 3.6% | PASS |
| ac_reads | `oltp_sum_range` | 10.43ms | 9.99ms | 1.0× | 2.6% | PASS |
| ac_reads | `oltp_order_range` | 2.39ms | 2.25ms | 0.9× | 1.9% | PASS |
| ac_reads | `oltp_distinct_range` | 2.88ms | 2.72ms | 0.9× | 2.1% | PASS |
| ac_reads | `oltp_index_scan` | 4.55ms | 5.50ms | 1.2× | 2.8% | PASS |
| ac_reads | `select_random_points` | 15.53ms | 13.86ms | 0.9× | 4.2% | PASS |
| ac_reads | `select_random_ranges` | 5.66ms | 5.11ms | 0.9× | 2.3% | PASS |
| ac_reads | `covering_index_scan` | 5.66ms | 7.38ms | 1.3× | 3.3% | PASS |
| ac_reads | `groupby_scan` | 19.68ms | 19.25ms | 1.0× | 2.2% | PASS |
| ac_reads | `index_join` | 8.64ms | 7.52ms | 0.9× | 3.9% | PASS |
| ac_reads | `index_join_scan` | 2.83ms | 4.68ms | 1.7× | 2.8% | PASS |
| ac_reads | `types_table_scan` | 653.33ms | 751.97ms | 1.2× | 1.6% | PASS |
| ac_reads | `table_scan` | 744.47ms | 839.92ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_read_only` | 95.45ms | 90.15ms | 0.9× | 3.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 26.88ms | 51.60ms | 1.9× | 6.7% | PASS |
| ac_writes | `oltp_insert_ac` | 28.96ms | 64.16ms | 2.2× | 9.1% | PASS |
| ac_writes | `oltp_update_index_ac` | 29.39ms | 73.13ms | 2.5× | 5.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.76ms | 59.02ms | 2.4× | 7.7% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 28.43ms | 64.48ms | 2.3× | 6.2% | PASS |
| ac_writes | `oltp_write_only_ac` | 27.86ms | 66.13ms | 2.4× | 7.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 21.53ms | 61.46ms | 2.9× | 7.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.23ms | 69.72ms | 2.2× | 6.5% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 27.40ms | 29.98ms | 1.1× | 1.7% | PASS |
| mem_reads | `oltp_range_select` | 16.75ms | 17.53ms | 1.0× | 2.1% | PASS |
| mem_reads | `oltp_sum_range` | 14.40ms | 16.47ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_order_range` | 2.98ms | 3.27ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_distinct_range` | 3.76ms | 4.12ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 3.56ms | 4.51ms | 1.3× | 1.3% | PASS |
| mem_reads | `select_random_points` | 20.95ms | 21.59ms | 1.0× | 0.9% | PASS |
| mem_reads | `select_random_ranges` | 5.79ms | 6.28ms | 1.1× | 1.3% | PASS |
| mem_reads | `covering_index_scan` | 5.97ms | 7.95ms | 1.3× | 0.9% | PASS |
| mem_reads | `groupby_scan` | 30.06ms | 32.71ms | 1.1× | 0.8% | PASS |
| mem_reads | `index_join` | 6.14ms | 8.00ms | 1.3× | 1.8% | PASS |
| mem_reads | `index_join_scan` | 3.17ms | 5.08ms | 1.6× | 2.1% | PASS |
| mem_reads | `types_table_scan` | 867.82ms | 1.01s | 1.2× | 1.0% | PASS |
| mem_reads | `table_scan` | 1.06s | 1.12s | 1.1× | 6.1% | PASS |
| mem_reads | `oltp_read_only` | 124.30ms | 134.44ms | 1.1× | 1.2% | PASS |
| mem_writes | `oltp_bulk_insert` | 194.52ms | 249.76ms | 1.3× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 15.14ms | 24.74ms | 1.6× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 52.70ms | 92.44ms | 1.8× | 1.2% | PASS |
| mem_writes | `oltp_update_non_index` | 41.37ms | 62.34ms | 1.5× | 1.1% | PASS |
| mem_writes | `oltp_delete_insert` | 39.73ms | 68.65ms | 1.7× | 0.7% | PASS |
| mem_writes | `oltp_write_only` | 22.59ms | 43.38ms | 1.9× | 1.5% | PASS |
| mem_writes | `types_delete_insert` | 26.33ms | 39.61ms | 1.5× | 1.6% | PASS |
| mem_writes | `oltp_read_write` | 75.25ms | 109.55ms | 1.5× | 0.8% | PASS |
| file_reads | `oltp_point_select` | 86.82ms | 45.11ms | 0.5× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 22.27ms | 19.08ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_sum_range` | 20.68ms | 18.00ms | 0.9× | 1.2% | PASS |
| file_reads | `oltp_order_range` | 3.71ms | 3.53ms | 1.0× | 1.1% | PASS |
| file_reads | `oltp_distinct_range` | 4.56ms | 4.39ms | 1.0× | 1.0% | PASS |
| file_reads | `oltp_index_scan` | 10.04ms | 6.65ms | 0.7× | 1.2% | PASS |
| file_reads | `select_random_points` | 28.35ms | 23.71ms | 0.8× | 0.9% | PASS |
| file_reads | `select_random_ranges` | 12.18ms | 8.10ms | 0.7× | 0.9% | PASS |
| file_reads | `covering_index_scan` | 12.46ms | 10.08ms | 0.8× | 0.9% | PASS |
| file_reads | `groupby_scan` | 31.00ms | 33.26ms | 1.1× | 0.9% | PASS |
| file_reads | `index_join` | 9.81ms | 9.76ms | 1.0× | 1.0% | PASS |
| file_reads | `index_join_scan` | 4.00ms | 5.30ms | 1.3× | 1.3% | PASS |
| file_reads | `types_table_scan` | 859.49ms | 1.00s | 1.2× | 0.6% | PASS |
| file_reads | `table_scan` | 988.02ms | 1.11s | 1.1× | 0.9% | PASS |
| file_reads | `oltp_read_only` | 204.34ms | 153.56ms | 0.8× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 254.25ms | 305.61ms | 1.2× | 8.5% | PASS |
| file_writes | `oltp_insert` | 33.47ms | 40.57ms | 1.2× | 9.2% | PASS |
| file_writes | `oltp_update_index` | 160.01ms | 159.20ms | 1.0× | 3.4% | PASS |
| file_writes | `oltp_update_non_index` | 136.66ms | 108.98ms | 0.8× | 4.6% | PASS |
| file_writes | `oltp_delete_insert` | 132.94ms | 125.06ms | 0.9× | 5.9% | PASS |
| file_writes | `oltp_write_only` | 100.47ms | 85.34ms | 0.8× | 11.8% | PASS |
| file_writes | `types_delete_insert` | 88.37ms | 82.27ms | 0.9× | 9.0% | PASS |
| file_writes | `oltp_read_write` | 159.10ms | 162.59ms | 1.0× | 4.3% | PASS |
| ac_reads | `oltp_point_select` | 47.91ms | 46.01ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_range_select` | 18.86ms | 19.33ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_sum_range` | 16.53ms | 18.04ms | 1.1× | 0.9% | PASS |
| ac_reads | `oltp_order_range` | 3.32ms | 3.52ms | 1.1× | 0.9% | PASS |
| ac_reads | `oltp_distinct_range` | 4.17ms | 4.38ms | 1.1× | 0.7% | PASS |
| ac_reads | `oltp_index_scan` | 6.09ms | 6.63ms | 1.1× | 1.2% | PASS |
| ac_reads | `select_random_points` | 24.08ms | 23.71ms | 1.0× | 1.0% | PASS |
| ac_reads | `select_random_ranges` | 8.22ms | 8.11ms | 1.0× | 1.1% | PASS |
| ac_reads | `covering_index_scan` | 8.51ms | 10.05ms | 1.2× | 0.7% | PASS |
| ac_reads | `groupby_scan` | 30.45ms | 33.20ms | 1.1× | 0.5% | PASS |
| ac_reads | `index_join` | 7.78ms | 9.59ms | 1.2× | 1.1% | PASS |
| ac_reads | `index_join_scan` | 3.64ms | 5.24ms | 1.4× | 2.0% | PASS |
| ac_reads | `types_table_scan` | 876.85ms | 1.01s | 1.2× | 1.7% | PASS |
| ac_reads | `table_scan` | 1.09s | 1.13s | 1.0× | 4.0% | PASS |
| ac_reads | `oltp_read_only` | 155.56ms | 157.62ms | 1.0× | 1.4% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 23.50ms | 76.52ms | 3.3× | 38.2% | PASS |
| ac_writes | `oltp_insert_ac` | 25.05ms | 82.11ms | 3.3× | 38.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 32.89ms | 122.25ms | 3.7× | 61.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.23ms | 74.09ms | 3.5× | 38.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 23.99ms | 92.31ms | 3.8× | 24.9% | PASS |
| ac_writes | `oltp_write_only_ac` | 21.78ms | 79.86ms | 3.7× | 22.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 19.27ms | 74.39ms | 3.9× | 18.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 28.04ms | 86.61ms | 3.1× | 20.9% | PASS |

</details>

</details>

## Version-control latency

Wall time: 6m 47s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 28.21ms | 38.00ms | 74.2% | 1.3% | PASS |
| `status_dirty_many_tables` | 31.07ms | 42.00ms | 74.0% | 1.6% | PASS |
| `diff_regular_working_one_table` | 24.64ms | 33.00ms | 74.7% | 1.6% | PASS |
| `diff_regular_working_many_tables` | 35.27ms | 50.00ms | 70.5% | 1.0% | PASS |
| `diff_stat_working_many_tables` | 35.45ms | 48.00ms | 73.8% | 2.0% | PASS |
| `diff_schema_working_many_tables` | 36.12ms | 48.00ms | 75.2% | 1.6% | PASS |
| `branch_list_many_branches` | 19.31ms | 25.00ms | 77.2% | 1.5% | PASS |
| `branch_create_delete` | 20.51ms | 27.00ms | 76.0% | 3.0% | PASS |
| `at_literal_deep_history` | 20.44ms | 28.00ms | 73.0% | 1.5% | PASS |
| `diff_literal_deep_history` | 20.89ms | 28.00ms | 74.6% | 1.9% | PASS |
| `history_literal_deep_history` | 21.82ms | 30.00ms | 72.7% | 2.2% | PASS |
| `checkout_branch_clean` | 28.20ms | 43.00ms | 65.6% | 2.0% | PASS |
| `merge_data_no_conflicts` | 22.98ms | 33.00ms | 69.6% | 1.8% | PASS |
| `merge_data_secondary_index` | 770.33ms | 944.00ms | 81.6% | 1.3% | PASS |
| `merge_schema_no_conflicts` | 18.71ms | 24.00ms | 78.0% | 1.6% | PASS |
| `merge_data_conflicts` | 25.69ms | 33.00ms | 77.8% | 0.6% | PASS |
| `merge_data_conflicts_with_resolve` | 26.25ms | 34.00ms | 77.2% | 0.6% | PASS |

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

candidate: cb73254.

Fixture file bytes: base 107,968,301; delta 115,879,032.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 443.255 | 413.533–545.140 | 31.690 | 225.60 | 107968301 |
| push_initial | candidate | 782.756 | 780.205–783.931 | 406.016 | 127.75 | 107956807 |
| clone_initial | candidate | 1056.619 | 1025.081–1170.076 | 530.199 | 94.64 | 108475105 |
| push_noop | candidate | 0.570 | 0.543–0.665 | 0.264 | — | 0 |
| fetch_noop | candidate | 0.166 | 0.165–0.189 | 0.168 | — | 0 |
| push_incremental | candidate | 196.167 | 194.553–208.398 | 166.690 | 5.10 | 7909775 |
| fetch_incremental | candidate | 100.629 | 97.924–130.362 | 71.362 | 9.94 | 7910084 |
| pull_fast_forward | candidate | 90.306 | 87.975–193.727 | 61.871 | 11.07 | 7910844 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 90.58 | 175.487 | 108.763 | 18329 | 2066 |
| clone_initial | candidate | 90.78 | 295.236 | 109.784 | 47279 | 2337 |
| push_noop | candidate | 41.23 | 0.073 | 0.000 | 27 | 2 |
| fetch_noop | candidate | 41.23 | 0.135 | 0.000 | 42 | 0 |
| push_incremental | candidate | 41.23 | 96.927 | 7.910 | 22426 | 125 |
| fetch_incremental | candidate | 41.23 | 34.472 | 7.910 | 10810 | 124 |
| pull_fast_forward | candidate | 41.23 | 26.497 | 7.911 | 7032 | 126 |

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

candidate: cb73254.

Fixture file bytes: base 1,087,718,066; delta 1,103,525,194.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 5569.090 | 5394.248–5643.042 | 315.227 | 179.56 | 1087718066 |
| push_initial | candidate | 8587.959 | 8161.718–9527.781 | 4392.402 | 116.44 | 1079566310 |
| clone_initial | candidate | 11321.539 | 10412.871–11764.969 | 6190.364 | 88.33 | 1102407396 |
| push_noop | candidate | 0.596 | 0.593–0.673 | 0.270 | — | 0 |
| fetch_noop | candidate | 0.163 | 0.163–0.199 | 0.164 | — | 0 |
| push_incremental | candidate | 1021.900 | 1016.638–1028.837 | 943.346 | 0.98 | 15806003 |
| fetch_incremental | candidate | 277.006 | 274.173–295.668 | 218.384 | 3.61 | 15806513 |
| pull_fast_forward | candidate | 363.811 | 341.743–364.809 | 227.453 | 2.75 | 15807273 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 134.62 | 2092.418 | 1087.622 | 263630 | 20541 |
| clone_initial | candidate | 124.06 | 3522.084 | 1132.819 | 613354 | 32243 |
| push_noop | candidate | 44.12 | 0.076 | 0.000 | 28 | 2 |
| fetch_noop | candidate | 44.12 | 0.137 | 0.000 | 42 | 0 |
| push_incremental | candidate | 44.85 | 618.071 | 15.928 | 150121 | 313 |
| fetch_incremental | candidate | 50.32 | 62.945 | 15.928 | 14071 | 313 |
| pull_fast_forward | candidate | 50.51 | 62.878 | 15.929 | 14066 | 315 |

</details>


</details>

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh`, `test/vc_perf_ceiling.sh`, and `test/remote_perf.py`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
