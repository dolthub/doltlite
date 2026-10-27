# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-10-27 11:38 UTC
>
> Commit: [`7297937755d14b94b8cb55b25ca885034fa78b31`](https://github.com/dolthub/doltlite/commit/7297937755d14b94b8cb55b25ca885034fa78b31)
>
> Runner: ubuntu24 20261023.334.3
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/40482490801)

The SQL workloads compare optimized DoltLite against stock SQLite on the same GitHub-hosted runner, alternating execution order on each repetition. Version-control sections measure DoltLite latency. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 8.66s | 10.06s | 1.2× | 1.8% | **PASS** |
| Writes | 1.78s | 2.90s | 1.6× | 1.5% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.55s | 10.34s | 1.1× | 1.8% | **PASS** |
| Writes | 3.43s | 3.80s | 1.1× | 2.7% | **PASS** |
| Autocommit writes | 842.54ms | 2.44s | 2.9× | 6.1% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 1.56s | 1.69s | 1.1× | 2.3% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.61s | 3.12s | 1.2× | 1.6% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 1.88s | 2.08s | 1.1× | 2.5% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.61s | 3.17s | 1.2× | 1.1% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 255.03ms | 411.69ms | 1.6× | 2.0% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 570.82ms | 965.08ms | 1.7× | 1.3% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 372.28ms | 579.32ms | 1.6× | 2.7% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 585.90ms | 943.96ms | 1.6× | 1.2% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 1.76s | 1.77s | 1.0× | 2.8% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.86s | 3.20s | 1.1× | 1.3% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.08s | 2.14s | 1.0× | 2.2% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.85s | 3.24s | 1.1× | 1.4% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 732.79ms | 704.86ms | 1.0× | 3.2% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 915.08ms | 1.07s | 1.2× | 3.6% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 1.02s | 991.70ms | 1.0× | 5.2% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 763.17ms | 1.03s | 1.3× | 1.7% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 1.61s | 1.74s | 1.1× | 2.3% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.82s | 3.21s | 1.1× | 1.8% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.00s | 2.21s | 1.1× | 2.3% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.68s | 3.24s | 1.2× | 1.3% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 173.84ms | 500.45ms | 2.9× | 5.1% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 223.74ms | 698.17ms | 3.1× | 7.4% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 231.09ms | 547.97ms | 2.4× | 5.7% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 213.88ms | 691.42ms | 3.2× | 6.7% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 16.34ms | 18.87ms | 1.2× | 2.2% | PASS |
| mem_reads | `oltp_range_select` | 6.57ms | 7.18ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_sum_range` | 5.92ms | 7.14ms | 1.2× | 2.3% | PASS |
| mem_reads | `oltp_order_range` | 1.56ms | 1.74ms | 1.1× | 3.3% | PASS |
| mem_reads | `oltp_distinct_range` | 1.97ms | 2.22ms | 1.1× | 2.5% | PASS |
| mem_reads | `oltp_index_scan` | 2.21ms | 3.13ms | 1.4× | 4.1% | PASS |
| mem_reads | `select_random_points` | 6.32ms | 8.07ms | 1.3× | 6.8% | PASS |
| mem_reads | `select_random_ranges` | 2.48ms | 2.91ms | 1.2× | 4.3% | PASS |
| mem_reads | `covering_index_scan` | 3.85ms | 5.18ms | 1.3× | 1.9% | PASS |
| mem_reads | `groupby_scan` | 16.20ms | 18.14ms | 1.1× | 1.3% | PASS |
| mem_reads | `index_join` | 3.40ms | 4.88ms | 1.4× | 2.7% | PASS |
| mem_reads | `index_join_scan` | 2.23ms | 4.26ms | 1.9× | 4.7% | PASS |
| mem_reads | `types_table_scan` | 667.17ms | 729.51ms | 1.1× | 1.2% | PASS |
| mem_reads | `table_scan` | 771.27ms | 812.72ms | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_read_only` | 54.29ms | 62.86ms | 1.2× | 2.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 105.05ms | 162.06ms | 1.5× | 1.7% | PASS |
| mem_writes | `oltp_insert` | 8.94ms | 15.53ms | 1.7× | 1.9% | PASS |
| mem_writes | `oltp_update_index` | 30.53ms | 54.16ms | 1.8× | 1.4% | PASS |
| mem_writes | `oltp_update_non_index` | 21.10ms | 34.39ms | 1.6× | 3.5% | PASS |
| mem_writes | `oltp_delete_insert` | 26.22ms | 41.71ms | 1.6× | 2.0% | PASS |
| mem_writes | `oltp_write_only` | 12.93ms | 25.04ms | 1.9× | 2.1% | PASS |
| mem_writes | `types_delete_insert` | 14.49ms | 21.50ms | 1.5× | 2.0% | PASS |
| mem_writes | `oltp_read_write` | 35.77ms | 57.31ms | 1.6× | 2.2% | PASS |
| file_reads | `oltp_point_select` | 67.96ms | 32.77ms | 0.5× | 2.0% | PASS |
| file_reads | `oltp_range_select` | 11.65ms | 8.53ms | 0.7× | 3.1% | PASS |
| file_reads | `oltp_sum_range` | 11.42ms | 8.56ms | 0.7× | 2.8% | PASS |
| file_reads | `oltp_order_range` | 2.27ms | 2.01ms | 0.9× | 1.6% | PASS |
| file_reads | `oltp_distinct_range` | 2.73ms | 2.46ms | 0.9× | 2.8% | PASS |
| file_reads | `oltp_index_scan` | 7.94ms | 4.96ms | 0.6× | 3.8% | PASS |
| file_reads | `select_random_points` | 12.72ms | 10.01ms | 0.8× | 4.3% | PASS |
| file_reads | `select_random_ranges` | 8.05ms | 4.46ms | 0.6× | 2.0% | PASS |
| file_reads | `covering_index_scan` | 9.53ms | 7.23ms | 0.8× | 3.3% | PASS |
| file_reads | `groupby_scan` | 17.20ms | 18.71ms | 1.1× | 1.9% | PASS |
| file_reads | `index_join` | 6.58ms | 6.42ms | 1.0× | 3.6% | PASS |
| file_reads | `index_join_scan` | 2.85ms | 4.47ms | 1.6× | 4.1% | PASS |
| file_reads | `types_table_scan` | 686.14ms | 749.83ms | 1.1× | 1.7% | PASS |
| file_reads | `table_scan` | 779.28ms | 824.79ms | 1.1× | 1.2% | PASS |
| file_reads | `oltp_read_only` | 133.60ms | 86.55ms | 0.6× | 1.2% | PASS |
| file_writes | `oltp_bulk_insert` | 147.73ms | 209.05ms | 1.4× | 2.3% | PASS |
| file_writes | `oltp_insert` | 19.82ms | 27.07ms | 1.4× | 4.0% | PASS |
| file_writes | `oltp_update_index` | 120.95ms | 108.01ms | 0.9× | 3.1% | PASS |
| file_writes | `oltp_update_non_index` | 100.58ms | 79.08ms | 0.8× | 1.5% | PASS |
| file_writes | `oltp_delete_insert` | 101.15ms | 89.56ms | 0.9× | 6.0% | PASS |
| file_writes | `oltp_write_only` | 80.89ms | 59.94ms | 0.7× | 2.3% | PASS |
| file_writes | `types_delete_insert` | 60.50ms | 42.25ms | 0.7× | 6.9% | PASS |
| file_writes | `oltp_read_write` | 101.17ms | 89.90ms | 0.9× | 3.3% | PASS |
| ac_reads | `oltp_point_select` | 31.05ms | 31.95ms | 1.0× | 2.4% | PASS |
| ac_reads | `oltp_range_select` | 7.79ms | 8.47ms | 1.1× | 2.3% | PASS |
| ac_reads | `oltp_sum_range` | 7.41ms | 8.46ms | 1.1× | 2.0% | PASS |
| ac_reads | `oltp_order_range` | 1.81ms | 1.94ms | 1.1× | 2.9% | PASS |
| ac_reads | `oltp_distinct_range` | 2.31ms | 2.41ms | 1.0× | 2.4% | PASS |
| ac_reads | `oltp_index_scan` | 4.19ms | 4.94ms | 1.2× | 2.5% | PASS |
| ac_reads | `select_random_points` | 8.70ms | 10.10ms | 1.2× | 3.9% | PASS |
| ac_reads | `select_random_ranges` | 4.47ms | 4.44ms | 1.0× | 2.2% | PASS |
| ac_reads | `covering_index_scan` | 5.75ms | 7.16ms | 1.2× | 1.9% | PASS |
| ac_reads | `groupby_scan` | 17.01ms | 18.93ms | 1.1× | 2.3% | PASS |
| ac_reads | `index_join` | 4.58ms | 6.46ms | 1.4× | 3.9% | PASS |
| ac_reads | `index_join_scan` | 2.49ms | 4.52ms | 1.8× | 3.7% | PASS |
| ac_reads | `types_table_scan` | 677.12ms | 747.86ms | 1.1× | 1.4% | PASS |
| ac_reads | `table_scan` | 759.11ms | 803.00ms | 1.1× | 1.0% | PASS |
| ac_reads | `oltp_read_only` | 78.40ms | 82.54ms | 1.1× | 1.1% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 19.81ms | 52.30ms | 2.6× | 3.5% | PASS |
| ac_writes | `oltp_insert_ac` | 21.60ms | 61.97ms | 2.9× | 5.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 23.33ms | 70.60ms | 3.0× | 4.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 20.66ms | 58.24ms | 2.8× | 6.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 21.97ms | 64.53ms | 2.9× | 5.8% | PASS |
| ac_writes | `oltp_write_only_ac` | 21.74ms | 64.92ms | 3.0× | 5.7% | PASS |
| ac_writes | `types_delete_insert_ac` | 20.30ms | 60.30ms | 3.0× | 4.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 24.43ms | 67.58ms | 2.8× | 5.3% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.50ms | 40.78ms | 1.2× | 1.2% | PASS |
| mem_reads | `oltp_range_select` | 16.89ms | 16.82ms | 1.0× | 1.8% | PASS |
| mem_reads | `oltp_sum_range` | 15.67ms | 15.93ms | 1.0× | 1.9% | PASS |
| mem_reads | `oltp_order_range` | 3.30ms | 3.51ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_distinct_range` | 4.35ms | 4.73ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_index_scan` | 3.89ms | 6.84ms | 1.8× | 1.4% | PASS |
| mem_reads | `select_random_points` | 21.45ms | 23.03ms | 1.1× | 2.3% | PASS |
| mem_reads | `select_random_ranges` | 6.44ms | 6.98ms | 1.1× | 1.7% | PASS |
| mem_reads | `covering_index_scan` | 7.61ms | 11.12ms | 1.5× | 1.1% | PASS |
| mem_reads | `groupby_scan` | 34.39ms | 35.94ms | 1.0× | 1.0% | PASS |
| mem_reads | `index_join` | 10.20ms | 10.61ms | 1.0× | 1.9% | PASS |
| mem_reads | `index_join_scan` | 3.69ms | 6.58ms | 1.8× | 1.6% | PASS |
| mem_reads | `types_table_scan` | 1.07s | 1.32s | 1.2× | 1.2% | PASS |
| mem_reads | `table_scan` | 1.24s | 1.46s | 1.2× | 2.3% | PASS |
| mem_reads | `oltp_read_only` | 139.62ms | 152.76ms | 1.1× | 1.9% | PASS |
| mem_writes | `oltp_bulk_insert` | 231.80ms | 350.30ms | 1.5× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 17.49ms | 37.66ms | 2.2× | 0.6% | PASS |
| mem_writes | `oltp_update_index` | 63.87ms | 136.60ms | 2.1× | 1.5% | PASS |
| mem_writes | `oltp_update_non_index` | 45.84ms | 83.79ms | 1.8× | 1.2% | PASS |
| mem_writes | `oltp_delete_insert` | 52.24ms | 100.68ms | 1.9× | 0.7% | PASS |
| mem_writes | `oltp_write_only` | 27.18ms | 56.49ms | 2.1× | 1.4% | PASS |
| mem_writes | `types_delete_insert` | 37.00ms | 57.37ms | 1.6× | 1.5% | PASS |
| mem_writes | `oltp_read_write` | 95.39ms | 142.19ms | 1.5× | 1.3% | PASS |
| file_reads | `oltp_point_select` | 104.20ms | 60.33ms | 0.6× | 1.2% | PASS |
| file_reads | `oltp_range_select` | 24.00ms | 18.86ms | 0.8× | 1.4% | PASS |
| file_reads | `oltp_sum_range` | 23.14ms | 18.08ms | 0.8× | 1.0% | PASS |
| file_reads | `oltp_order_range` | 4.29ms | 3.80ms | 0.9× | 2.0% | PASS |
| file_reads | `oltp_distinct_range` | 5.32ms | 5.01ms | 0.9× | 1.2% | PASS |
| file_reads | `oltp_index_scan` | 11.23ms | 8.91ms | 0.8× | 1.3% | PASS |
| file_reads | `select_random_points` | 31.16ms | 26.80ms | 0.9× | 2.3% | PASS |
| file_reads | `select_random_ranges` | 14.11ms | 9.13ms | 0.6× | 1.3% | PASS |
| file_reads | `covering_index_scan` | 15.27ms | 13.30ms | 0.9× | 1.1% | PASS |
| file_reads | `groupby_scan` | 35.51ms | 36.41ms | 1.0× | 0.7% | PASS |
| file_reads | `index_join` | 14.86ms | 12.00ms | 0.8× | 2.5% | PASS |
| file_reads | `index_join_scan` | 4.79ms | 7.00ms | 1.5× | 1.5% | PASS |
| file_reads | `types_table_scan` | 1.07s | 1.33s | 1.2× | 1.4% | PASS |
| file_reads | `table_scan` | 1.25s | 1.47s | 1.2× | 1.7% | PASS |
| file_reads | `oltp_read_only` | 248.20ms | 184.25ms | 0.7× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 257.27ms | 365.96ms | 1.4× | 0.9% | PASS |
| file_writes | `oltp_insert` | 25.57ms | 42.82ms | 1.7× | 1.4% | PASS |
| file_writes | `oltp_update_index` | 111.66ms | 156.24ms | 1.4× | 5.2% | PASS |
| file_writes | `oltp_update_non_index` | 107.31ms | 98.71ms | 0.9× | 11.0% | PASS |
| file_writes | `oltp_delete_insert` | 95.43ms | 116.10ms | 1.2× | 2.0% | PASS |
| file_writes | `oltp_write_only` | 93.41ms | 69.03ms | 0.7× | 13.3% | PASS |
| file_writes | `types_delete_insert` | 71.19ms | 69.51ms | 1.0× | 2.1% | PASS |
| file_writes | `oltp_read_write` | 153.23ms | 155.33ms | 1.0× | 5.1% | PASS |
| ac_reads | `oltp_point_select` | 57.62ms | 60.40ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 19.51ms | 18.90ms | 1.0× | 2.4% | PASS |
| ac_reads | `oltp_sum_range` | 18.32ms | 18.07ms | 1.0× | 1.8% | PASS |
| ac_reads | `oltp_order_range` | 3.77ms | 3.77ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_distinct_range` | 4.82ms | 4.99ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_index_scan` | 6.55ms | 8.90ms | 1.4× | 1.8% | PASS |
| ac_reads | `select_random_points` | 25.48ms | 26.78ms | 1.1× | 2.5% | PASS |
| ac_reads | `select_random_ranges` | 9.18ms | 9.15ms | 1.0× | 1.2% | PASS |
| ac_reads | `covering_index_scan` | 10.32ms | 13.26ms | 1.3× | 1.2% | PASS |
| ac_reads | `groupby_scan` | 34.92ms | 36.39ms | 1.0× | 0.8% | PASS |
| ac_reads | `index_join` | 12.49ms | 12.03ms | 1.0× | 2.5% | PASS |
| ac_reads | `index_join_scan` | 4.32ms | 7.12ms | 1.6× | 1.8% | PASS |
| ac_reads | `types_table_scan` | 1.10s | 1.34s | 1.2× | 2.7% | PASS |
| ac_reads | `table_scan` | 1.33s | 1.47s | 1.1× | 2.3% | PASS |
| ac_reads | `oltp_read_only` | 180.00ms | 184.79ms | 1.0× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 25.77ms | 71.78ms | 2.8× | 5.9% | PASS |
| ac_writes | `oltp_insert_ac` | 29.25ms | 84.23ms | 2.9× | 7.2% | PASS |
| ac_writes | `oltp_update_index_ac` | 30.20ms | 100.02ms | 3.3× | 7.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.34ms | 79.12ms | 3.3× | 7.5% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 28.44ms | 92.33ms | 3.2× | 7.7% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.86ms | 89.54ms | 3.3× | 8.5% | PASS |
| ac_writes | `types_delete_insert_ac` | 25.74ms | 86.01ms | 3.3× | 6.2% | PASS |
| ac_writes | `oltp_read_write_ac` | 33.13ms | 95.16ms | 2.9× | 6.4% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.11ms | 24.62ms | 1.0× | 4.3% | PASS |
| mem_reads | `oltp_range_select` | 10.74ms | 9.90ms | 0.9× | 2.1% | PASS |
| mem_reads | `oltp_sum_range` | 10.39ms | 9.87ms | 0.9× | 2.2% | PASS |
| mem_reads | `oltp_order_range` | 2.24ms | 2.26ms | 1.0× | 2.0% | PASS |
| mem_reads | `oltp_distinct_range` | 2.79ms | 2.77ms | 1.0× | 1.6% | PASS |
| mem_reads | `oltp_index_scan` | 2.69ms | 4.79ms | 1.8× | 2.4% | PASS |
| mem_reads | `select_random_points` | 16.02ms | 15.69ms | 1.0× | 4.1% | PASS |
| mem_reads | `select_random_ranges` | 4.68ms | 4.29ms | 0.9× | 2.5% | PASS |
| mem_reads | `covering_index_scan` | 4.20ms | 6.65ms | 1.6× | 2.6% | PASS |
| mem_reads | `groupby_scan` | 22.02ms | 22.18ms | 1.0× | 1.6% | PASS |
| mem_reads | `index_join` | 8.40ms | 8.45ms | 1.0× | 3.9% | PASS |
| mem_reads | `index_join_scan` | 3.00ms | 5.94ms | 2.0× | 4.4% | PASS |
| mem_reads | `types_table_scan` | 751.51ms | 881.68ms | 1.2× | 2.3% | PASS |
| mem_reads | `table_scan` | 923.55ms | 990.26ms | 1.1× | 3.1% | PASS |
| mem_reads | `oltp_read_only` | 95.31ms | 87.93ms | 0.9× | 2.9% | PASS |
| mem_writes | `oltp_bulk_insert` | 144.07ms | 203.93ms | 1.4× | 1.1% | PASS |
| mem_writes | `oltp_insert` | 11.03ms | 22.91ms | 2.1× | 1.4% | PASS |
| mem_writes | `oltp_update_index` | 42.51ms | 85.02ms | 2.0× | 2.6% | PASS |
| mem_writes | `oltp_update_non_index` | 31.91ms | 50.00ms | 1.6× | 2.7% | PASS |
| mem_writes | `oltp_delete_insert` | 34.39ms | 63.54ms | 1.8× | 3.2% | PASS |
| mem_writes | `oltp_write_only` | 18.84ms | 36.56ms | 1.9× | 2.7% | PASS |
| mem_writes | `types_delete_insert` | 24.94ms | 34.57ms | 1.4× | 2.6% | PASS |
| mem_writes | `oltp_read_write` | 64.58ms | 82.80ms | 1.3× | 6.0% | PASS |
| file_reads | `oltp_point_select` | 81.34ms | 39.51ms | 0.5× | 2.6% | PASS |
| file_reads | `oltp_range_select` | 16.67ms | 11.25ms | 0.7× | 2.8% | PASS |
| file_reads | `oltp_sum_range` | 16.15ms | 11.22ms | 0.7× | 2.1% | PASS |
| file_reads | `oltp_order_range` | 2.91ms | 2.40ms | 0.8× | 2.0% | PASS |
| file_reads | `oltp_distinct_range` | 3.44ms | 2.89ms | 0.8× | 2.5% | PASS |
| file_reads | `oltp_index_scan` | 8.61ms | 6.12ms | 0.7× | 2.2% | PASS |
| file_reads | `select_random_points` | 25.07ms | 17.96ms | 0.7× | 4.6% | PASS |
| file_reads | `select_random_ranges` | 10.65ms | 5.90ms | 0.6× | 1.9% | PASS |
| file_reads | `covering_index_scan` | 10.41ms | 8.29ms | 0.8× | 1.9% | PASS |
| file_reads | `groupby_scan` | 22.37ms | 22.14ms | 1.0× | 1.5% | PASS |
| file_reads | `index_join` | 12.96ms | 9.17ms | 0.7× | 4.6% | PASS |
| file_reads | `index_join_scan` | 3.94ms | 6.32ms | 1.6× | 7.0% | PASS |
| file_reads | `types_table_scan` | 822.02ms | 944.66ms | 1.1× | 1.9% | PASS |
| file_reads | `table_scan` | 860.45ms | 940.72ms | 1.1× | 3.0% | PASS |
| file_reads | `oltp_read_only` | 179.31ms | 110.08ms | 0.6× | 1.3% | PASS |
| file_writes | `oltp_bulk_insert` | 208.51ms | 263.71ms | 1.3× | 2.5% | PASS |
| file_writes | `oltp_insert` | 19.01ms | 44.24ms | 2.3× (paired 2.2×) | 11.4% | PASS |
| file_writes | `oltp_update_index` | 147.98ms | 168.94ms | 1.1× | 3.5% | PASS |
| file_writes | `oltp_update_non_index` | 129.77ms | 108.97ms | 0.8× | 8.4% | PASS |
| file_writes | `oltp_delete_insert` | 150.59ms | 127.82ms | 0.8× | 3.7% | PASS |
| file_writes | `oltp_write_only` | 110.06ms | 79.77ms | 0.7× | 2.8% | PASS |
| file_writes | `types_delete_insert` | 104.43ms | 69.85ms | 0.7× | 8.9% | PASS |
| file_writes | `oltp_read_write` | 149.74ms | 128.39ms | 0.9× | 6.6% | PASS |
| ac_reads | `oltp_point_select` | 40.78ms | 38.70ms | 0.9× | 3.3% | PASS |
| ac_reads | `oltp_range_select` | 13.31ms | 11.83ms | 0.9× | 3.3% | PASS |
| ac_reads | `oltp_sum_range` | 13.89ms | 12.35ms | 0.9× | 5.5% | PASS |
| ac_reads | `oltp_order_range` | 2.66ms | 2.48ms | 0.9× | 2.3% | PASS |
| ac_reads | `oltp_distinct_range` | 3.12ms | 2.94ms | 0.9× | 1.2% | PASS |
| ac_reads | `oltp_index_scan` | 4.95ms | 6.05ms | 1.2× | 1.5% | PASS |
| ac_reads | `select_random_points` | 19.34ms | 17.03ms | 0.9× | 4.7% | PASS |
| ac_reads | `select_random_ranges` | 6.86ms | 5.89ms | 0.9× | 1.5% | PASS |
| ac_reads | `covering_index_scan` | 6.76ms | 8.20ms | 1.2× | 2.3% | PASS |
| ac_reads | `groupby_scan` | 21.91ms | 22.08ms | 1.0× | 1.5% | PASS |
| ac_reads | `index_join` | 10.35ms | 8.65ms | 0.8× | 4.4% | PASS |
| ac_reads | `index_join_scan` | 3.27ms | 5.44ms | 1.7× | 2.5% | PASS |
| ac_reads | `types_table_scan` | 815.33ms | 950.48ms | 1.2× | 1.9% | PASS |
| ac_reads | `table_scan` | 917.96ms | 1.01s | 1.1× | 1.7% | PASS |
| ac_reads | `oltp_read_only` | 119.01ms | 109.23ms | 0.9× | 2.7% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 28.30ms | 58.48ms | 2.1× | 5.7% | PASS |
| ac_writes | `oltp_insert_ac` | 30.40ms | 67.71ms | 2.2× | 4.8% | PASS |
| ac_writes | `oltp_update_index_ac` | 30.54ms | 75.78ms | 2.5× | 4.4% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 26.16ms | 64.70ms | 2.5× | 6.1% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 30.33ms | 72.73ms | 2.4× | 6.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 29.65ms | 68.88ms | 2.3× | 3.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.71ms | 63.87ms | 2.8× | 6.0% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.99ms | 75.81ms | 2.3× | 5.8% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.09ms | 42.85ms | 1.3× | 1.6% | PASS |
| mem_reads | `oltp_range_select` | 19.21ms | 23.91ms | 1.2× | 1.4% | PASS |
| mem_reads | `oltp_sum_range` | 17.46ms | 23.24ms | 1.3× | 1.4% | PASS |
| mem_reads | `oltp_order_range` | 3.52ms | 4.12ms | 1.2× | 0.9% | PASS |
| mem_reads | `oltp_distinct_range` | 4.63ms | 5.18ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 4.52ms | 6.55ms | 1.4× | 1.4% | PASS |
| mem_reads | `select_random_points` | 27.94ms | 33.88ms | 1.2× | 1.4% | PASS |
| mem_reads | `select_random_ranges` | 7.55ms | 9.43ms | 1.2× | 1.0% | PASS |
| mem_reads | `covering_index_scan` | 7.60ms | 10.98ms | 1.4× | 1.1% | PASS |
| mem_reads | `groupby_scan` | 35.69ms | 40.93ms | 1.1× | 0.6% | PASS |
| mem_reads | `index_join` | 7.81ms | 11.80ms | 1.5× | 1.9% | PASS |
| mem_reads | `index_join_scan` | 3.86ms | 6.48ms | 1.7× | 1.5% | PASS |
| mem_reads | `types_table_scan` | 1.05s | 1.31s | 1.2× | 0.8% | PASS |
| mem_reads | `table_scan` | 1.23s | 1.45s | 1.2× | 1.1% | PASS |
| mem_reads | `oltp_read_only` | 150.64ms | 184.59ms | 1.2× | 1.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 242.64ms | 346.07ms | 1.4× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 18.59ms | 33.96ms | 1.8× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 65.87ms | 120.88ms | 1.8× | 1.0% | PASS |
| mem_writes | `oltp_update_non_index` | 50.02ms | 83.39ms | 1.7× | 1.2% | PASS |
| mem_writes | `oltp_delete_insert` | 48.77ms | 91.80ms | 1.9× | 1.1% | PASS |
| mem_writes | `oltp_write_only` | 26.65ms | 53.89ms | 2.0× | 1.5% | PASS |
| mem_writes | `types_delete_insert` | 32.44ms | 54.71ms | 1.7× | 1.2% | PASS |
| mem_writes | `oltp_read_write` | 100.93ms | 159.26ms | 1.6× | 1.7% | PASS |
| file_reads | `oltp_point_select` | 103.55ms | 62.69ms | 0.6× | 1.5% | PASS |
| file_reads | `oltp_range_select` | 26.91ms | 26.22ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_sum_range` | 25.28ms | 25.74ms | 1.0× | 1.1% | PASS |
| file_reads | `oltp_order_range` | 4.39ms | 4.44ms | 1.0× | 1.6% | PASS |
| file_reads | `oltp_distinct_range` | 5.52ms | 5.53ms | 1.0× | 1.5% | PASS |
| file_reads | `oltp_index_scan` | 11.62ms | 8.88ms | 0.8× | 1.6% | PASS |
| file_reads | `select_random_points` | 37.12ms | 37.29ms | 1.0× | 2.1% | PASS |
| file_reads | `select_random_ranges` | 15.12ms | 11.64ms | 0.8× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 14.94ms | 13.33ms | 0.9× | 1.4% | PASS |
| file_reads | `groupby_scan` | 36.64ms | 41.36ms | 1.1× | 0.7% | PASS |
| file_reads | `index_join` | 11.98ms | 13.61ms | 1.1× | 1.4% | PASS |
| file_reads | `index_join_scan` | 4.90ms | 6.98ms | 1.4× | 1.9% | PASS |
| file_reads | `types_table_scan` | 1.06s | 1.31s | 1.2× | 1.2% | PASS |
| file_reads | `table_scan` | 1.24s | 1.46s | 1.2× | 2.0% | PASS |
| file_reads | `oltp_read_only` | 256.62ms | 215.39ms | 0.8× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 259.57ms | 358.49ms | 1.4× | 0.9% | PASS |
| file_writes | `oltp_insert` | 25.98ms | 37.71ms | 1.5× | 1.8% | PASS |
| file_writes | `oltp_update_index` | 95.61ms | 136.47ms | 1.4× | 1.3% | PASS |
| file_writes | `oltp_update_non_index` | 77.29ms | 94.66ms | 1.2× | 1.9% | PASS |
| file_writes | `oltp_delete_insert` | 76.24ms | 105.38ms | 1.4× | 1.6% | PASS |
| file_writes | `oltp_write_only` | 51.29ms | 64.12ms | 1.3× | 2.1% | PASS |
| file_writes | `types_delete_insert` | 49.81ms | 63.10ms | 1.3× | 2.3% | PASS |
| file_writes | `oltp_read_write` | 127.37ms | 170.13ms | 1.3× | 1.3% | PASS |
| ac_reads | `oltp_point_select` | 56.61ms | 62.60ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_range_select` | 22.02ms | 26.19ms | 1.2× | 1.5% | PASS |
| ac_reads | `oltp_sum_range` | 20.34ms | 25.74ms | 1.3× | 1.3% | PASS |
| ac_reads | `oltp_order_range` | 3.93ms | 4.45ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_distinct_range` | 5.03ms | 5.53ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_index_scan` | 7.16ms | 8.89ms | 1.2× | 1.7% | PASS |
| ac_reads | `select_random_points` | 31.51ms | 37.28ms | 1.2× | 1.0% | PASS |
| ac_reads | `select_random_ranges` | 10.12ms | 11.65ms | 1.2× | 0.8% | PASS |
| ac_reads | `covering_index_scan` | 10.36ms | 13.32ms | 1.3× | 1.3% | PASS |
| ac_reads | `groupby_scan` | 36.04ms | 41.47ms | 1.2× | 0.8% | PASS |
| ac_reads | `index_join` | 9.51ms | 13.62ms | 1.4× | 1.8% | PASS |
| ac_reads | `index_join_scan` | 4.36ms | 6.97ms | 1.6× | 1.6% | PASS |
| ac_reads | `types_table_scan` | 1.05s | 1.31s | 1.2× | 1.2% | PASS |
| ac_reads | `table_scan` | 1.22s | 1.45s | 1.2× | 1.1% | PASS |
| ac_reads | `oltp_read_only` | 185.95ms | 215.35ms | 1.2× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 24.46ms | 71.18ms | 2.9× | 10.2% | PASS |
| ac_writes | `oltp_insert_ac` | 25.96ms | 86.98ms | 3.4× | 6.2% | PASS |
| ac_writes | `oltp_update_index_ac` | 29.11ms | 100.68ms | 3.5× | 6.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 25.29ms | 79.18ms | 3.1× | 8.1% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.03ms | 87.25ms | 3.4× | 6.4% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.02ms | 87.04ms | 3.3× | 7.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 23.05ms | 83.65ms | 3.6× | 9.2% | PASS |
| ac_writes | `oltp_read_write_ac` | 33.95ms | 95.46ms | 2.8× | 6.2% | PASS |

</details>

</details>

## Version-control latency

Wall time: 5m 59s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 27.10ms | 38.00ms | 71.3% | 0.4% | PASS |
| `status_dirty_many_tables` | 29.67ms | 42.00ms | 70.6% | 0.4% | PASS |
| `diff_regular_working_one_table` | 23.13ms | 33.00ms | 70.1% | 0.4% | PASS |
| `diff_regular_working_many_tables` | 34.03ms | 50.00ms | 68.1% | 0.4% | PASS |
| `diff_stat_working_many_tables` | 34.01ms | 48.00ms | 70.9% | 0.4% | PASS |
| `diff_schema_working_many_tables` | 34.38ms | 48.00ms | 71.6% | 0.4% | PASS |
| `branch_list_many_branches` | 17.49ms | 25.00ms | 70.0% | 0.5% | PASS |
| `branch_create_delete` | 18.93ms | 27.00ms | 70.1% | 0.8% | PASS |
| `at_literal_deep_history` | 18.97ms | 28.00ms | 67.8% | 0.7% | PASS |
| `diff_literal_deep_history` | 18.99ms | 28.00ms | 67.8% | 0.5% | PASS |
| `history_literal_deep_history` | 19.97ms | 30.00ms | 66.6% | 0.5% | PASS |
| `checkout_branch_clean` | 26.13ms | 43.00ms | 60.8% | 0.5% | PASS |
| `merge_data_no_conflicts` | 21.12ms | 33.00ms | 64.0% | 0.5% | PASS |
| `merge_data_secondary_index` | 668.52ms | 944.00ms | 70.8% | 0.0% | PASS |
| `merge_schema_no_conflicts` | 16.64ms | 24.00ms | 69.3% | 0.6% | PASS |
| `merge_data_conflicts` | 23.47ms | 33.00ms | 71.1% | 0.5% | PASS |
| `merge_data_conflicts_with_resolve` | 24.05ms | 34.00ms | 70.7% | 0.4% | PASS |

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

candidate: 7297937.

Fixture file bytes: base 107,968,266; delta 115,878,997.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 444.026 | 404.380–512.946 | 30.202 | 225.21 | 107968266 |
| push_initial | candidate | 716.412 | 712.791–741.513 | 346.367 | 139.58 | 107956772 |
| clone_initial | candidate | 972.796 | 833.845–981.856 | 464.706 | 102.80 | 108475035 |
| push_noop | candidate | 0.546 | 0.529–0.675 | 0.306 | — | 0 |
| fetch_noop | candidate | 0.179 | 0.178–0.270 | 0.181 | — | 0 |
| push_incremental | candidate | 184.618 | 183.448–195.048 | 164.152 | 5.42 | 7909775 |
| fetch_incremental | candidate | 83.196 | 82.253–87.528 | 66.429 | 12.02 | 7910084 |
| pull_fast_forward | candidate | 72.733 | 71.053–81.143 | 57.547 | 13.75 | 7910844 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 90.60 | 175.487 | 108.763 | 18329 | 2066 |
| clone_initial | candidate | 90.84 | 295.107 | 109.784 | 47245 | 2337 |
| push_noop | candidate | 42.09 | 0.073 | 0.000 | 26 | 2 |
| fetch_noop | candidate | 42.09 | 0.135 | 0.000 | 40 | 0 |
| push_incremental | candidate | 42.09 | 96.849 | 7.910 | 22408 | 125 |
| fetch_incremental | candidate | 42.09 | 34.472 | 7.910 | 10808 | 124 |
| pull_fast_forward | candidate | 42.09 | 26.497 | 7.911 | 7031 | 126 |

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

candidate: 7297937.

Fixture file bytes: base 1,087,718,031; delta 1,103,525,159.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 2638.365 | 2638.056–2653.236 | 381.195 | 379.02 | 1087718031 |
| push_initial | candidate | 8170.936 | 8141.002–8180.535 | 5971.126 | 122.38 | 1079566275 |
| clone_initial | candidate | 8793.594 | 8745.347–8801.865 | 8596.593 | 113.72 | 1102407326 |
| push_noop | candidate | 0.827 | 0.775–1.701 | 0.356 | — | 0 |
| fetch_noop | candidate | 0.189 | 0.188–0.216 | 0.192 | — | 0 |
| push_incremental | candidate | 1327.412 | 1324.797–1345.211 | 1322.491 | 0.75 | 15806003 |
| fetch_incremental | candidate | 280.700 | 275.760–290.453 | 275.409 | 3.56 | 15806513 |
| pull_fast_forward | candidate | 277.683 | 275.529–291.950 | 271.921 | 3.60 | 15807273 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 134.65 | 2092.133 | 1087.622 | 263560 | 20541 |
| clone_initial | candidate | 123.88 | 3520.280 | 1132.819 | 612894 | 32243 |
| push_noop | candidate | 44.91 | 0.078 | 0.000 | 27 | 2 |
| fetch_noop | candidate | 44.91 | 0.137 | 0.000 | 40 | 0 |
| push_incremental | candidate | 48.24 | 618.007 | 15.928 | 150091 | 313 |
| fetch_incremental | candidate | 50.29 | 62.262 | 15.928 | 13902 | 313 |
| pull_fast_forward | candidate | 50.32 | 62.195 | 15.929 | 13898 | 315 |

</details>


</details>

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh`, `test/vc_perf_ceiling.sh`, and `test/remote_perf.py`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
