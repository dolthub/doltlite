# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-09-28 11:23 UTC
>
> Commit: [`43690f84d08bb57d9dd8e56fa76282fffb81d2e2`](https://github.com/dolthub/doltlite/commit/43690f84d08bb57d9dd8e56fa76282fffb81d2e2)
>
> Runner: ubuntu24 20260920.314.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/36405626607)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.17s | 11.66s | 1.1× | 1.3% | **PASS** |
| Writes | 1.97s | 3.18s | 1.6× | 1.2% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.73s | 11.80s | 1.1× | 1.3% | **PASS** |
| Writes | 3.76s | 4.31s | 1.1× | 2.3% | **PASS** |
| Autocommit writes | 901.73ms | 2.65s | 2.9× | 5.3% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.62s | 2.82s | 1.1× | 1.3% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.83s | 3.07s | 1.1× | 1.0% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.05s | 2.44s | 1.2× | 2.1% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.67s | 3.32s | 1.2× | 1.4% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 458.02ms | 747.23ms | 1.6× | 1.1% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 516.60ms | 835.54ms | 1.6× | 1.0% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 396.54ms | 639.41ms | 1.6× | 2.3% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 595.52ms | 962.58ms | 1.6× | 1.2% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.82s | 2.87s | 1.0× | 1.3% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.80s | 3.07s | 1.1× | 1.1% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.13s | 2.47s | 1.2× | 1.4% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.98s | 3.39s | 1.1× | 1.4% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 585.34ms | 794.43ms | 1.4× | 1.5% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.35s | 1.35s | 1.0× | 3.8% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 1.06s | 1.11s | 1.0× | 2.7% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 764.46ms | 1.06s | 1.4× | 2.2% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.60s | 2.85s | 1.1× | 1.4% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.69s | 3.05s | 1.1× | 1.1% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.08s | 2.47s | 1.2× | 2.1% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.71s | 3.37s | 1.2× | 1.3% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 195.91ms | 609.88ms | 3.1× | 6.1% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 269.90ms | 766.00ms | 2.8× | 5.2% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 212.70ms | 587.05ms | 2.8× | 4.7% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 223.22ms | 690.11ms | 3.1× | 8.2% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.31ms | 31.84ms | 1.3× | 1.5% | PASS |
| mem_reads | `oltp_range_select` | 10.62ms | 11.84ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_sum_range` | 9.62ms | 11.80ms | 1.2× | 1.7% | PASS |
| mem_reads | `oltp_order_range` | 2.60ms | 2.95ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 3.68ms | 4.14ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 3.98ms | 5.79ms | 1.5× | 1.3% | PASS |
| mem_reads | `select_random_points` | 10.48ms | 11.96ms | 1.1× | 1.7% | PASS |
| mem_reads | `select_random_ranges` | 4.67ms | 5.31ms | 1.1× | 1.1% | PASS |
| mem_reads | `covering_index_scan` | 7.72ms | 11.36ms | 1.5× | 1.0% | PASS |
| mem_reads | `groupby_scan` | 29.38ms | 33.53ms | 1.1× | 1.0% | PASS |
| mem_reads | `index_join` | 5.94ms | 9.03ms | 1.5× | 1.7% | PASS |
| mem_reads | `index_join_scan` | 3.31ms | 5.17ms | 1.6× | 2.1% | PASS |
| mem_reads | `types_table_scan` | 1.10s | 1.19s | 1.1× | 1.4% | PASS |
| mem_reads | `table_scan` | 1.29s | 1.36s | 1.1× | 0.4% | PASS |
| mem_reads | `oltp_read_only` | 110.40ms | 127.48ms | 1.2× | 1.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 180.54ms | 281.31ms | 1.6× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 15.35ms | 29.50ms | 1.9× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 52.98ms | 99.90ms | 1.9× | 1.3% | PASS |
| mem_writes | `oltp_update_non_index` | 37.07ms | 58.47ms | 1.6× | 1.3% | PASS |
| mem_writes | `oltp_delete_insert` | 48.37ms | 79.73ms | 1.6× | 0.9% | PASS |
| mem_writes | `oltp_write_only` | 23.18ms | 46.49ms | 2.0× | 1.1% | PASS |
| mem_writes | `types_delete_insert` | 25.37ms | 38.37ms | 1.5× | 1.1% | PASS |
| mem_writes | `oltp_read_write` | 75.16ms | 113.46ms | 1.5× | 1.5% | PASS |
| file_reads | `oltp_point_select` | 95.47ms | 51.79ms | 0.5× | 1.4% | PASS |
| file_reads | `oltp_range_select` | 18.69ms | 14.07ms | 0.8× | 1.3% | PASS |
| file_reads | `oltp_sum_range` | 17.38ms | 14.04ms | 0.8× | 0.6% | PASS |
| file_reads | `oltp_order_range` | 3.53ms | 3.23ms | 0.9× | 1.6% | PASS |
| file_reads | `oltp_distinct_range` | 4.60ms | 4.42ms | 1.0× | 1.0% | PASS |
| file_reads | `oltp_index_scan` | 11.57ms | 7.93ms | 0.7× | 1.1% | PASS |
| file_reads | `select_random_points` | 18.61ms | 14.32ms | 0.8× | 1.7% | PASS |
| file_reads | `select_random_ranges` | 11.99ms | 7.37ms | 0.6× | 1.3% | PASS |
| file_reads | `covering_index_scan` | 15.52ms | 13.56ms | 0.9× | 1.0% | PASS |
| file_reads | `groupby_scan` | 30.44ms | 34.14ms | 1.1× | 0.9% | PASS |
| file_reads | `index_join` | 10.02ms | 10.05ms | 1.0× | 2.1% | PASS |
| file_reads | `index_join_scan` | 4.13ms | 5.27ms | 1.3× | 1.6% | PASS |
| file_reads | `types_table_scan` | 1.11s | 1.20s | 1.1× | 0.8% | PASS |
| file_reads | `table_scan` | 1.26s | 1.34s | 1.1× | 2.4% | PASS |
| file_reads | `oltp_read_only` | 203.99ms | 152.26ms | 0.7× | 1.2% | PASS |
| file_writes | `oltp_bulk_insert` | 194.06ms | 291.43ms | 1.5× | 1.0% | PASS |
| file_writes | `oltp_insert` | 21.49ms | 32.59ms | 1.5× | 1.5% | PASS |
| file_writes | `oltp_update_index` | 74.02ms | 102.86ms | 1.4× | 1.4% | PASS |
| file_writes | `oltp_update_non_index` | 57.24ms | 67.71ms | 1.2× | 1.4% | PASS |
| file_writes | `oltp_delete_insert` | 65.63ms | 84.59ms | 1.3× | 2.1% | PASS |
| file_writes | `oltp_write_only` | 43.10ms | 53.97ms | 1.3× | 2.0% | PASS |
| file_writes | `types_delete_insert` | 39.19ms | 43.53ms | 1.1× | 1.3% | PASS |
| file_writes | `oltp_read_write` | 90.61ms | 117.73ms | 1.3× | 2.5% | PASS |
| ac_reads | `oltp_point_select` | 46.22ms | 50.27ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_range_select` | 12.59ms | 13.67ms | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_sum_range` | 11.47ms | 13.61ms | 1.2× | 1.2% | PASS |
| ac_reads | `oltp_order_range` | 2.88ms | 3.15ms | 1.1× | 1.7% | PASS |
| ac_reads | `oltp_distinct_range` | 3.97ms | 4.35ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 6.42ms | 7.59ms | 1.2× | 1.2% | PASS |
| ac_reads | `select_random_points` | 12.66ms | 13.71ms | 1.1× | 1.5% | PASS |
| ac_reads | `select_random_ranges` | 7.02ms | 7.27ms | 1.0× | 1.7% | PASS |
| ac_reads | `covering_index_scan` | 10.37ms | 13.29ms | 1.3× | 1.2% | PASS |
| ac_reads | `groupby_scan` | 29.47ms | 33.84ms | 1.1× | 0.6% | PASS |
| ac_reads | `index_join` | 7.55ms | 9.97ms | 1.3× | 1.7% | PASS |
| ac_reads | `index_join_scan` | 3.64ms | 5.34ms | 1.5× | 2.0% | PASS |
| ac_reads | `types_table_scan` | 1.06s | 1.19s | 1.1× | 1.1% | PASS |
| ac_reads | `table_scan` | 1.25s | 1.34s | 1.1× | 1.6% | PASS |
| ac_reads | `oltp_read_only` | 136.90ms | 153.34ms | 1.1× | 1.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.82ms | 63.62ms | 2.8× | 8.7% | PASS |
| ac_writes | `oltp_insert_ac` | 24.07ms | 75.54ms | 3.1× | 5.1% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.28ms | 89.21ms | 3.4× | 3.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.09ms | 69.33ms | 3.0× | 6.9% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 23.45ms | 79.44ms | 3.4× | 5.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.04ms | 78.74ms | 3.1× | 8.9% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.06ms | 69.61ms | 3.2× | 6.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 29.11ms | 84.38ms | 2.9× | 5.5% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.08ms | 34.04ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_range_select` | 14.62ms | 14.13ms | 1.0× | 1.0% | PASS |
| mem_reads | `oltp_sum_range` | 14.41ms | 13.61ms | 0.9× | 1.0% | PASS |
| mem_reads | `oltp_order_range` | 3.03ms | 3.18ms | 1.0× | 0.9% | PASS |
| mem_reads | `oltp_distinct_range` | 3.93ms | 4.29ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 3.68ms | 5.63ms | 1.5× | 1.4% | PASS |
| mem_reads | `select_random_points` | 22.02ms | 21.37ms | 1.0× | 1.3% | PASS |
| mem_reads | `select_random_ranges` | 5.91ms | 5.89ms | 1.0× | 0.9% | PASS |
| mem_reads | `covering_index_scan` | 6.50ms | 9.13ms | 1.4× | 0.7% | PASS |
| mem_reads | `groupby_scan` | 33.31ms | 35.13ms | 1.1× | 0.7% | PASS |
| mem_reads | `index_join` | 10.99ms | 9.46ms | 0.9× | 1.2% | PASS |
| mem_reads | `index_join_scan` | 3.32ms | 6.16ms | 1.9× | 1.2% | PASS |
| mem_reads | `types_table_scan` | 1.16s | 1.32s | 1.1× | 1.7% | PASS |
| mem_reads | `table_scan` | 1.38s | 1.45s | 1.0× | 0.8% | PASS |
| mem_reads | `oltp_read_only` | 130.62ms | 135.94ms | 1.0× | 1.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 194.46ms | 294.16ms | 1.5× | 0.4% | PASS |
| mem_writes | `oltp_insert` | 15.54ms | 33.92ms | 2.2× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 62.17ms | 130.62ms | 2.1× | 0.9% | PASS |
| mem_writes | `oltp_update_non_index` | 44.80ms | 70.74ms | 1.6× | 1.3% | PASS |
| mem_writes | `oltp_delete_insert` | 49.41ms | 89.99ms | 1.8× | 0.9% | PASS |
| mem_writes | `oltp_write_only` | 25.14ms | 50.18ms | 2.0× | 1.6% | PASS |
| mem_writes | `types_delete_insert` | 35.38ms | 44.02ms | 1.2× | 1.0% | PASS |
| mem_writes | `oltp_read_write` | 89.69ms | 121.91ms | 1.4× | 1.7% | PASS |
| file_reads | `oltp_point_select` | 65.27ms | 42.06ms | 0.6× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 18.25ms | 15.36ms | 0.8× | 0.8% | PASS |
| file_reads | `oltp_sum_range` | 18.19ms | 14.88ms | 0.8× | 1.1% | PASS |
| file_reads | `oltp_order_range` | 3.46ms | 3.40ms | 1.0× | 1.4% | PASS |
| file_reads | `oltp_distinct_range` | 4.38ms | 4.47ms | 1.0× | 1.2% | PASS |
| file_reads | `oltp_index_scan` | 7.09ms | 6.75ms | 1.0× | 1.2% | PASS |
| file_reads | `select_random_points` | 25.80ms | 22.59ms | 0.9× | 1.2% | PASS |
| file_reads | `select_random_ranges` | 9.40ms | 6.81ms | 0.7× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 10.05ms | 10.22ms | 1.0× | 1.0% | PASS |
| file_reads | `groupby_scan` | 33.89ms | 35.14ms | 1.0× | 0.6% | PASS |
| file_reads | `index_join` | 13.10ms | 10.27ms | 0.8× | 1.2% | PASS |
| file_reads | `index_join_scan` | 3.77ms | 6.43ms | 1.7× | 1.1% | PASS |
| file_reads | `types_table_scan` | 1.12s | 1.32s | 1.2× | 1.4% | PASS |
| file_reads | `table_scan` | 1.30s | 1.43s | 1.1× | 1.6% | PASS |
| file_reads | `oltp_read_only` | 174.46ms | 145.09ms | 0.8× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 280.37ms | 376.30ms | 1.3× | 4.6% | PASS |
| file_writes | `oltp_insert` | 30.52ms | 59.30ms | 1.9× | 2.4% | PASS |
| file_writes | `oltp_update_index` | 189.44ms | 220.43ms | 1.2× | 5.2% | PASS |
| file_writes | `oltp_update_non_index` | 172.04ms | 141.93ms | 0.8× | 2.8% | PASS |
| file_writes | `oltp_delete_insert` | 188.84ms | 170.61ms | 0.9× | 3.2% | PASS |
| file_writes | `oltp_write_only` | 141.24ms | 110.31ms | 0.8× | 2.2% | PASS |
| file_writes | `types_delete_insert` | 152.52ms | 93.78ms | 0.6× | 5.2% | PASS |
| file_writes | `oltp_read_write` | 193.44ms | 180.52ms | 0.9× | 4.3% | PASS |
| ac_reads | `oltp_point_select` | 42.47ms | 41.72ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 15.84ms | 15.38ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_sum_range` | 15.62ms | 14.82ms | 0.9× | 0.9% | PASS |
| ac_reads | `oltp_order_range` | 3.24ms | 3.39ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_distinct_range` | 4.15ms | 4.48ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 5.04ms | 6.79ms | 1.3× | 1.2% | PASS |
| ac_reads | `select_random_points` | 23.71ms | 22.62ms | 1.0× | 1.4% | PASS |
| ac_reads | `select_random_ranges` | 7.24ms | 6.81ms | 0.9× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 7.69ms | 10.17ms | 1.3× | 1.1% | PASS |
| ac_reads | `groupby_scan` | 33.39ms | 35.03ms | 1.0× | 0.6% | PASS |
| ac_reads | `index_join` | 12.21ms | 10.37ms | 0.8× | 1.4% | PASS |
| ac_reads | `index_join_scan` | 3.64ms | 6.41ms | 1.8× | 1.9% | PASS |
| ac_reads | `types_table_scan` | 1.10s | 1.31s | 1.2× | 1.4% | PASS |
| ac_reads | `table_scan` | 1.27s | 1.42s | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_read_only` | 138.88ms | 143.58ms | 1.0× | 0.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 31.77ms | 81.15ms | 2.6× | 5.0% | PASS |
| ac_writes | `oltp_insert_ac` | 34.09ms | 94.61ms | 2.8× | 5.3% | PASS |
| ac_writes | `oltp_update_index_ac` | 34.55ms | 106.07ms | 3.1× | 5.0% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 30.79ms | 87.60ms | 2.8× | 3.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 33.86ms | 99.35ms | 2.9× | 3.4% | PASS |
| ac_writes | `oltp_write_only_ac` | 33.77ms | 97.26ms | 2.9× | 6.9% | PASS |
| ac_writes | `types_delete_insert_ac` | 32.37ms | 94.54ms | 2.9× | 6.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 38.71ms | 105.42ms | 2.7× | 7.2% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 25.75ms | 26.43ms | 1.0× | 2.5% | PASS |
| mem_reads | `oltp_range_select` | 10.30ms | 11.19ms | 1.1× | 2.7% | PASS |
| mem_reads | `oltp_sum_range` | 10.45ms | 10.62ms | 1.0× | 4.1% | PASS |
| mem_reads | `oltp_order_range` | 2.35ms | 2.56ms | 1.1× | 2.6% | PASS |
| mem_reads | `oltp_distinct_range` | 3.07ms | 3.50ms | 1.1× | 2.1% | PASS |
| mem_reads | `oltp_index_scan` | 2.93ms | 4.34ms | 1.5× | 2.1% | PASS |
| mem_reads | `select_random_points` | 16.85ms | 16.25ms | 1.0× | 2.9% | PASS |
| mem_reads | `select_random_ranges` | 4.59ms | 4.55ms | 1.0× | 3.9% | PASS |
| mem_reads | `covering_index_scan` | 5.37ms | 7.41ms | 1.4× | 1.2% | PASS |
| mem_reads | `groupby_scan` | 25.88ms | 28.52ms | 1.1× | 1.8% | PASS |
| mem_reads | `index_join` | 8.73ms | 7.22ms | 0.8× | 1.3% | PASS |
| mem_reads | `index_join_scan` | 2.49ms | 4.28ms | 1.7× | 3.7% | PASS |
| mem_reads | `types_table_scan` | 859.69ms | 1.05s | 1.2× | 0.3% | PASS |
| mem_reads | `table_scan` | 974.46ms | 1.16s | 1.2× | 0.5% | PASS |
| mem_reads | `oltp_read_only` | 97.29ms | 106.28ms | 1.1× | 0.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 163.19ms | 242.01ms | 1.5× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 12.87ms | 26.24ms | 2.0× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 44.62ms | 90.37ms | 2.0× | 2.4% | PASS |
| mem_writes | `oltp_update_non_index` | 31.48ms | 52.50ms | 1.7× | 2.4% | PASS |
| mem_writes | `oltp_delete_insert` | 34.99ms | 66.19ms | 1.9× | 2.3% | PASS |
| mem_writes | `oltp_write_only` | 18.93ms | 37.58ms | 2.0× | 2.8% | PASS |
| mem_writes | `types_delete_insert` | 26.39ms | 33.59ms | 1.3× | 3.2% | PASS |
| mem_writes | `oltp_read_write` | 64.06ms | 90.93ms | 1.4× | 1.4% | PASS |
| file_reads | `oltp_point_select` | 50.23ms | 31.77ms | 0.6× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 13.56ms | 12.12ms | 0.9× | 3.0% | PASS |
| file_reads | `oltp_sum_range` | 13.13ms | 11.51ms | 0.9× | 2.4% | PASS |
| file_reads | `oltp_order_range` | 2.70ms | 2.67ms | 1.0× | 2.1% | PASS |
| file_reads | `oltp_distinct_range` | 3.51ms | 3.60ms | 1.0× | 1.0% | PASS |
| file_reads | `oltp_index_scan` | 5.46ms | 4.95ms | 0.9× | 1.4% | PASS |
| file_reads | `select_random_points` | 20.04ms | 17.62ms | 0.9× | 1.6% | PASS |
| file_reads | `select_random_ranges` | 7.46ms | 5.40ms | 0.7× | 2.5% | PASS |
| file_reads | `covering_index_scan` | 8.05ms | 8.06ms | 1.0× | 0.9% | PASS |
| file_reads | `groupby_scan` | 26.19ms | 28.41ms | 1.1× | 1.0% | PASS |
| file_reads | `index_join` | 9.92ms | 7.79ms | 0.8× | 2.3% | PASS |
| file_reads | `index_join_scan` | 2.87ms | 4.87ms | 1.7× | 4.1% | PASS |
| file_reads | `types_table_scan` | 861.22ms | 1.06s | 1.2× | 0.3% | PASS |
| file_reads | `table_scan` | 977.18ms | 1.16s | 1.2× | 0.6% | PASS |
| file_reads | `oltp_read_only` | 131.75ms | 113.89ms | 0.9× | 0.5% | PASS |
| file_writes | `oltp_bulk_insert` | 228.84ms | 306.32ms | 1.3× | 2.1% | PASS |
| file_writes | `oltp_insert` | 26.07ms | 51.23ms | 2.0× | 2.7% | PASS |
| file_writes | `oltp_update_index` | 151.38ms | 175.90ms | 1.2× | 2.7% | PASS |
| file_writes | `oltp_update_non_index` | 126.42ms | 115.99ms | 0.9× | 3.2% | PASS |
| file_writes | `oltp_delete_insert` | 152.92ms | 139.06ms | 0.9× | 2.6% | PASS |
| file_writes | `oltp_write_only` | 108.44ms | 94.14ms | 0.9× | 2.9% | PASS |
| file_writes | `types_delete_insert` | 115.15ms | 79.03ms | 0.7× | 7.5% | PASS |
| file_writes | `oltp_read_write` | 154.45ms | 146.86ms | 1.0× | 2.3% | PASS |
| ac_reads | `oltp_point_select` | 33.23ms | 31.57ms | 1.0× | 1.8% | PASS |
| ac_reads | `oltp_range_select` | 11.62ms | 11.98ms | 1.0× | 3.5% | PASS |
| ac_reads | `oltp_sum_range` | 11.12ms | 11.22ms | 1.0× | 3.3% | PASS |
| ac_reads | `oltp_order_range` | 2.58ms | 2.69ms | 1.0× | 2.3% | PASS |
| ac_reads | `oltp_distinct_range` | 3.31ms | 3.59ms | 1.1× | 1.5% | PASS |
| ac_reads | `oltp_index_scan` | 3.72ms | 4.89ms | 1.3× | 2.1% | PASS |
| ac_reads | `select_random_points` | 18.41ms | 17.48ms | 0.9× | 2.5% | PASS |
| ac_reads | `select_random_ranges` | 5.60ms | 5.22ms | 0.9× | 3.3% | PASS |
| ac_reads | `covering_index_scan` | 6.22ms | 8.01ms | 1.3× | 0.6% | PASS |
| ac_reads | `groupby_scan` | 25.79ms | 28.29ms | 1.1× | 1.1% | PASS |
| ac_reads | `index_join` | 8.87ms | 7.67ms | 0.9× | 2.8% | PASS |
| ac_reads | `index_join_scan` | 2.91ms | 5.08ms | 1.7× | 2.5% | PASS |
| ac_reads | `types_table_scan` | 861.00ms | 1.06s | 1.2× | 0.4% | PASS |
| ac_reads | `table_scan` | 975.48ms | 1.16s | 1.2× | 0.5% | PASS |
| ac_reads | `oltp_read_only` | 108.46ms | 114.04ms | 1.1× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 25.37ms | 62.97ms | 2.5× | 4.6% | PASS |
| ac_writes | `oltp_insert_ac` | 26.82ms | 72.97ms | 2.7× | 4.3% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.02ms | 81.68ms | 3.0× | 3.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.74ms | 68.74ms | 2.8× | 3.9% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.39ms | 75.24ms | 2.9× | 4.8% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.00ms | 73.97ms | 2.8× | 4.7% | PASS |
| ac_writes | `types_delete_insert_ac` | 25.86ms | 70.79ms | 2.7× | 5.2% | PASS |
| ac_writes | `oltp_read_write_ac` | 30.51ms | 80.69ms | 2.6× | 5.3% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.65ms | 42.05ms | 1.2× | 1.6% | PASS |
| mem_reads | `oltp_range_select` | 20.57ms | 23.99ms | 1.2× | 2.2% | PASS |
| mem_reads | `oltp_sum_range` | 18.13ms | 23.70ms | 1.3× | 1.3% | PASS |
| mem_reads | `oltp_order_range` | 3.61ms | 4.14ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_distinct_range` | 4.72ms | 5.37ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 4.74ms | 6.57ms | 1.4× | 2.2% | PASS |
| mem_reads | `select_random_points` | 29.13ms | 34.70ms | 1.2× | 2.3% | PASS |
| mem_reads | `select_random_ranges` | 7.65ms | 9.47ms | 1.2× | 1.7% | PASS |
| mem_reads | `covering_index_scan` | 7.71ms | 11.14ms | 1.4× | 0.9% | PASS |
| mem_reads | `groupby_scan` | 35.27ms | 41.86ms | 1.2× | 1.0% | PASS |
| mem_reads | `index_join` | 7.89ms | 10.52ms | 1.3× | 1.1% | PASS |
| mem_reads | `index_join_scan` | 3.87ms | 5.92ms | 1.5× | 1.8% | PASS |
| mem_reads | `types_table_scan` | 1.07s | 1.39s | 1.3× | 1.4% | PASS |
| mem_reads | `table_scan` | 1.27s | 1.54s | 1.2× | 3.9% | PASS |
| mem_reads | `oltp_read_only` | 147.76ms | 183.45ms | 1.2× | 1.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 246.27ms | 358.15ms | 1.5× | 1.3% | PASS |
| mem_writes | `oltp_insert` | 18.90ms | 36.35ms | 1.9× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 67.28ms | 126.05ms | 1.9× | 1.6% | PASS |
| mem_writes | `oltp_update_non_index` | 51.03ms | 80.71ms | 1.6× | 1.0% | PASS |
| mem_writes | `oltp_delete_insert` | 49.25ms | 95.16ms | 1.9× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 26.71ms | 54.79ms | 2.1× | 1.2% | PASS |
| mem_writes | `types_delete_insert` | 32.19ms | 52.53ms | 1.6× | 1.2% | PASS |
| mem_writes | `oltp_read_write` | 103.88ms | 158.83ms | 1.5× | 1.5% | PASS |
| file_reads | `oltp_point_select` | 103.78ms | 60.89ms | 0.6× | 1.1% | PASS |
| file_reads | `oltp_range_select` | 26.68ms | 25.92ms | 1.0× | 1.4% | PASS |
| file_reads | `oltp_sum_range` | 24.85ms | 25.82ms | 1.0× | 1.4% | PASS |
| file_reads | `oltp_order_range` | 4.35ms | 4.41ms | 1.0× | 2.1% | PASS |
| file_reads | `oltp_distinct_range` | 5.48ms | 5.65ms | 1.0× | 1.8% | PASS |
| file_reads | `oltp_index_scan` | 11.54ms | 8.42ms | 0.7× | 1.4% | PASS |
| file_reads | `select_random_points` | 37.00ms | 36.95ms | 1.0× | 1.6% | PASS |
| file_reads | `select_random_ranges` | 14.94ms | 11.75ms | 0.8× | 1.4% | PASS |
| file_reads | `covering_index_scan` | 15.02ms | 13.35ms | 0.9× | 1.4% | PASS |
| file_reads | `groupby_scan` | 35.73ms | 42.55ms | 1.2× | 1.0% | PASS |
| file_reads | `index_join` | 12.04ms | 12.63ms | 1.0× | 1.6% | PASS |
| file_reads | `index_join_scan` | 4.82ms | 6.40ms | 1.3× | 2.1% | PASS |
| file_reads | `types_table_scan` | 1.11s | 1.39s | 1.2× | 2.3% | PASS |
| file_reads | `table_scan` | 1.32s | 1.53s | 1.2× | 4.2% | PASS |
| file_reads | `oltp_read_only` | 251.11ms | 212.52ms | 0.8× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 261.63ms | 371.86ms | 1.4× | 1.0% | PASS |
| file_writes | `oltp_insert` | 26.43ms | 42.48ms | 1.6× | 2.8% | PASS |
| file_writes | `oltp_update_index` | 95.35ms | 139.38ms | 1.5× | 2.2% | PASS |
| file_writes | `oltp_update_non_index` | 76.90ms | 95.94ms | 1.2× | 1.9% | PASS |
| file_writes | `oltp_delete_insert` | 76.31ms | 108.61ms | 1.4× | 2.2% | PASS |
| file_writes | `oltp_write_only` | 50.57ms | 67.28ms | 1.3× | 2.7% | PASS |
| file_writes | `types_delete_insert` | 49.31ms | 61.59ms | 1.2× | 2.3% | PASS |
| file_writes | `oltp_read_write` | 127.96ms | 171.12ms | 1.3× | 1.7% | PASS |
| ac_reads | `oltp_point_select` | 56.92ms | 61.01ms | 1.1× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 21.88ms | 25.91ms | 1.2× | 1.2% | PASS |
| ac_reads | `oltp_sum_range` | 19.99ms | 25.80ms | 1.3× | 1.0% | PASS |
| ac_reads | `oltp_order_range` | 3.97ms | 4.41ms | 1.1× | 2.2% | PASS |
| ac_reads | `oltp_distinct_range` | 5.00ms | 5.66ms | 1.1× | 1.6% | PASS |
| ac_reads | `oltp_index_scan` | 7.11ms | 8.43ms | 1.2× | 1.4% | PASS |
| ac_reads | `select_random_points` | 31.09ms | 36.52ms | 1.2× | 1.2% | PASS |
| ac_reads | `select_random_ranges` | 10.21ms | 11.68ms | 1.1× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 10.39ms | 13.40ms | 1.3× | 1.0% | PASS |
| ac_reads | `groupby_scan` | 35.21ms | 42.46ms | 1.2× | 1.0% | PASS |
| ac_reads | `index_join` | 9.64ms | 12.64ms | 1.3× | 1.4% | PASS |
| ac_reads | `index_join_scan` | 4.37ms | 6.42ms | 1.5× | 1.4% | PASS |
| ac_reads | `types_table_scan` | 1.08s | 1.38s | 1.3× | 1.5% | PASS |
| ac_reads | `table_scan` | 1.24s | 1.52s | 1.2× | 1.9% | PASS |
| ac_reads | `oltp_read_only` | 183.69ms | 213.88ms | 1.2× | 1.3% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 26.40ms | 71.32ms | 2.7× | 9.9% | PASS |
| ac_writes | `oltp_insert_ac` | 26.65ms | 85.47ms | 3.2× | 6.8% | PASS |
| ac_writes | `oltp_update_index_ac` | 29.23ms | 98.60ms | 3.4× | 7.5% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 26.23ms | 80.22ms | 3.1× | 9.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.22ms | 90.82ms | 3.5× | 12.0% | PASS |
| ac_writes | `oltp_write_only_ac` | 28.32ms | 87.08ms | 3.1× | 7.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 25.16ms | 78.88ms | 3.1× | 9.0% | PASS |
| ac_writes | `oltp_read_write_ac` | 35.01ms | 97.71ms | 2.8× | 4.9% | PASS |

</details>

</details>

## Version-control latency

Wall time: 4m 5s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 27.38ms | 130.00ms | 21.1% | 0.6% | PASS |
| `status_dirty_many_tables` | 29.71ms | 130.00ms | 22.9% | 0.4% | PASS |
| `diff_regular_working_one_table` | 22.94ms | 120.00ms | 19.1% | 0.4% | PASS |
| `diff_regular_working_many_tables` | 33.97ms | 140.00ms | 24.3% | 0.2% | PASS |
| `diff_stat_working_many_tables` | 34.00ms | 140.00ms | 24.3% | 0.4% | PASS |
| `diff_schema_working_many_tables` | 34.32ms | 140.00ms | 24.5% | 0.4% | PASS |
| `branch_list_many_branches` | 17.42ms | 35.00ms | 49.8% | 0.4% | PASS |
| `branch_create_delete` | 24.63ms | 40.00ms | 61.6% | 4.2% | PASS |
| `at_literal_deep_history` | 19.64ms | 100.00ms | 19.6% | 0.5% | PASS |
| `diff_literal_deep_history` | 19.67ms | 120.00ms | 16.4% | 0.4% | PASS |
| `history_literal_deep_history` | 20.65ms | 150.00ms | 13.8% | 0.4% | PASS |
| `checkout_branch_clean` | 96.38ms | 150.00ms | 64.2% | 14.5% | PASS |
| `merge_data_no_conflicts` | 29.91ms | 50.00ms | 59.8% | 5.6% | PASS |
| `merge_data_secondary_index` | 1.29s | 2.50s | 51.6% | 0.8% | PASS |
| `merge_schema_no_conflicts` | 17.22ms | 35.00ms | 49.2% | 1.9% | PASS |
| `merge_data_conflicts` | 23.34ms | 180.00ms | 13.0% | 0.4% | PASS |
| `merge_data_conflicts_with_resolve` | 23.96ms | 180.00ms | 13.3% | 0.4% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
