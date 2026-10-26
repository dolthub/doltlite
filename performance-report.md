# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-10-26 11:48 UTC
>
> Commit: [`3dd9504feb5c1d0b4595f4dfd923c94d30ab2534`](https://github.com/dolthub/doltlite/commit/3dd9504feb5c1d0b4595f4dfd923c94d30ab2534)
>
> Runner: ubuntu24 20261023.334.3
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/40302459606)

The SQL workloads compare optimized DoltLite against stock SQLite on the same GitHub-hosted runner, alternating execution order on each repetition. Version-control sections measure DoltLite latency. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.03s | 12.37s | 1.2× | 1.5% | **PASS** |
| Writes | 2.07s | 3.37s | 1.6× | 1.6% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.97s | 12.66s | 1.2× | 1.5% | **PASS** |
| Writes | 3.45s | 4.15s | 1.2× | 1.9% | **PASS** |
| Autocommit writes | 928.44ms | 2.79s | 3.0× | 6.1% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.60s | 3.01s | 1.2× | 1.4% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.36s | 2.89s | 1.2× | 3.3% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.53s | 3.20s | 1.3× | 1.9% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.54s | 3.27s | 1.3× | 1.3% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 444.08ms | 749.71ms | 1.7× | 1.6% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 461.55ms | 732.13ms | 1.6× | 2.6% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 593.38ms | 963.66ms | 1.6× | 1.6% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 572.43ms | 928.85ms | 1.6× | 0.9% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.84s | 3.09s | 1.1× | 1.3% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.48s | 2.92s | 1.2× | 3.0% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.89s | 3.32s | 1.1× | 1.4% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.76s | 3.33s | 1.2× | 1.4% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 630.57ms | 825.10ms | 1.3× | 1.9% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.23s | 1.23s | 1.0× | 2.6% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 821.00ms | 1.07s | 1.3× | 1.8% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 763.17ms | 1.02s | 1.3× | 1.4% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.70s | 3.09s | 1.1× | 1.5% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.41s | 2.90s | 1.2× | 2.6% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.72s | 3.32s | 1.2× | 1.7% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.61s | 3.34s | 1.3× | 1.3% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 251.79ms | 781.21ms | 3.1× | 8.3% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 218.03ms | 621.51ms | 2.9× | 4.5% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 210.92ms | 664.59ms | 3.2× | 5.7% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 247.70ms | 725.25ms | 2.9× | 5.4% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 25.09ms | 32.01ms | 1.3× | 2.5% | PASS |
| mem_reads | `oltp_range_select` | 11.00ms | 13.23ms | 1.2× | 1.8% | PASS |
| mem_reads | `oltp_sum_range` | 9.96ms | 12.44ms | 1.2× | 1.8% | PASS |
| mem_reads | `oltp_order_range` | 2.66ms | 3.06ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_distinct_range` | 3.71ms | 4.20ms | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_index_scan` | 3.97ms | 5.85ms | 1.5× | 1.5% | PASS |
| mem_reads | `select_random_points` | 11.54ms | 12.53ms | 1.1× | 3.8% | PASS |
| mem_reads | `select_random_ranges` | 4.75ms | 5.46ms | 1.2× | 1.0% | PASS |
| mem_reads | `covering_index_scan` | 7.72ms | 11.22ms | 1.5× | 1.4% | PASS |
| mem_reads | `groupby_scan` | 29.86ms | 34.52ms | 1.2× | 0.9% | PASS |
| mem_reads | `index_join` | 5.83ms | 9.39ms | 1.6× | 1.5% | PASS |
| mem_reads | `index_join_scan` | 3.33ms | 5.91ms | 1.8× | 2.1% | PASS |
| mem_reads | `types_table_scan` | 1.10s | 1.33s | 1.2× | 1.2% | PASS |
| mem_reads | `table_scan` | 1.27s | 1.40s | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_read_only` | 108.14ms | 129.57ms | 1.2× | 1.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 173.41ms | 292.61ms | 1.7× | 1.1% | PASS |
| mem_writes | `oltp_insert` | 15.04ms | 28.03ms | 1.9× | 1.2% | PASS |
| mem_writes | `oltp_update_index` | 51.27ms | 95.31ms | 1.9× | 1.3% | PASS |
| mem_writes | `oltp_update_non_index` | 35.65ms | 59.03ms | 1.7× | 1.8% | PASS |
| mem_writes | `oltp_delete_insert` | 46.36ms | 75.92ms | 1.6× | 1.6% | PASS |
| mem_writes | `oltp_write_only` | 22.87ms | 45.35ms | 2.0× | 2.1% | PASS |
| mem_writes | `types_delete_insert` | 25.18ms | 39.38ms | 1.6× | 1.5% | PASS |
| mem_writes | `oltp_read_write` | 74.30ms | 114.10ms | 1.5× | 1.8% | PASS |
| file_reads | `oltp_point_select` | 94.38ms | 51.91ms | 0.6× | 1.3% | PASS |
| file_reads | `oltp_range_select` | 18.64ms | 15.35ms | 0.8× | 0.9% | PASS |
| file_reads | `oltp_sum_range` | 17.71ms | 14.78ms | 0.8× | 1.3% | PASS |
| file_reads | `oltp_order_range` | 3.54ms | 3.33ms | 0.9× | 1.8% | PASS |
| file_reads | `oltp_distinct_range` | 4.65ms | 4.55ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 11.59ms | 8.02ms | 0.7× | 1.1% | PASS |
| file_reads | `select_random_points` | 19.35ms | 14.92ms | 0.8× | 1.9% | PASS |
| file_reads | `select_random_ranges` | 12.03ms | 7.58ms | 0.6× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 15.67ms | 13.39ms | 0.9× | 1.5% | PASS |
| file_reads | `groupby_scan` | 30.91ms | 34.99ms | 1.1× | 1.0% | PASS |
| file_reads | `index_join` | 10.30ms | 10.92ms | 1.1× | 1.7% | PASS |
| file_reads | `index_join_scan` | 4.46ms | 6.51ms | 1.5× | 2.5% | PASS |
| file_reads | `types_table_scan` | 1.11s | 1.34s | 1.2× | 0.5% | PASS |
| file_reads | `table_scan` | 1.28s | 1.41s | 1.1× | 0.6% | PASS |
| file_reads | `oltp_read_only` | 209.27ms | 158.75ms | 0.8× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 187.78ms | 303.81ms | 1.6× | 1.3% | PASS |
| file_writes | `oltp_insert` | 24.90ms | 31.37ms | 1.3× | 2.4% | PASS |
| file_writes | `oltp_update_index` | 82.20ms | 109.37ms | 1.3× | 2.5% | PASS |
| file_writes | `oltp_update_non_index` | 64.05ms | 70.92ms | 1.1× | 2.2% | PASS |
| file_writes | `oltp_delete_insert` | 73.36ms | 87.53ms | 1.2× | 1.8% | PASS |
| file_writes | `oltp_write_only` | 51.26ms | 53.89ms | 1.1× | 1.9% | PASS |
| file_writes | `types_delete_insert` | 42.38ms | 45.31ms | 1.1× | 1.4% | PASS |
| file_writes | `oltp_read_write` | 104.65ms | 122.90ms | 1.2× | 2.0% | PASS |
| ac_reads | `oltp_point_select` | 48.28ms | 52.10ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 14.19ms | 15.49ms | 1.1× | 1.5% | PASS |
| ac_reads | `oltp_sum_range` | 12.94ms | 14.72ms | 1.1× | 1.5% | PASS |
| ac_reads | `oltp_order_range` | 3.11ms | 3.35ms | 1.1× | 2.2% | PASS |
| ac_reads | `oltp_distinct_range` | 4.12ms | 4.52ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_index_scan` | 6.78ms | 7.90ms | 1.2× | 1.8% | PASS |
| ac_reads | `select_random_points` | 14.32ms | 14.92ms | 1.0× | 2.2% | PASS |
| ac_reads | `select_random_ranges` | 7.25ms | 7.56ms | 1.0× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 10.83ms | 13.36ms | 1.2× | 1.5% | PASS |
| ac_reads | `groupby_scan` | 30.18ms | 34.80ms | 1.2× | 0.9% | PASS |
| ac_reads | `index_join` | 7.88ms | 10.96ms | 1.4× | 2.6% | PASS |
| ac_reads | `index_join_scan` | 3.92ms | 6.47ms | 1.7× | 2.5% | PASS |
| ac_reads | `types_table_scan` | 1.11s | 1.34s | 1.2× | 0.5% | PASS |
| ac_reads | `table_scan` | 1.28s | 1.41s | 1.1× | 0.7% | PASS |
| ac_reads | `oltp_read_only` | 141.50ms | 158.33ms | 1.1× | 0.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 28.93ms | 82.25ms | 2.8× | 10.0% | PASS |
| ac_writes | `oltp_insert_ac` | 31.85ms | 98.33ms | 3.1× | 8.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 34.67ms | 110.37ms | 3.2× | 8.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 28.70ms | 89.89ms | 3.1× | 8.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 30.96ms | 99.03ms | 3.2× | 7.0% | PASS |
| ac_writes | `oltp_write_only_ac` | 32.99ms | 101.58ms | 3.1× | 8.8% | PASS |
| ac_writes | `types_delete_insert_ac` | 28.11ms | 92.59ms | 3.3× | 10.1% | PASS |
| ac_writes | `oltp_read_write_ac` | 35.58ms | 107.17ms | 3.0× | 8.3% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 28.81ms | 30.91ms | 1.1× | 4.0% | PASS |
| mem_reads | `oltp_range_select` | 12.73ms | 14.44ms | 1.1× | 6.0% | PASS |
| mem_reads | `oltp_sum_range` | 12.62ms | 13.23ms | 1.0× | 4.4% | PASS |
| mem_reads | `oltp_order_range` | 2.91ms | 3.26ms | 1.1× | 2.6% | PASS |
| mem_reads | `oltp_distinct_range` | 3.80ms | 4.22ms | 1.1× | 4.5% | PASS |
| mem_reads | `oltp_index_scan` | 3.51ms | 5.08ms | 1.4× | 3.0% | PASS |
| mem_reads | `select_random_points` | 18.23ms | 19.40ms | 1.1× | 3.6% | PASS |
| mem_reads | `select_random_ranges` | 5.38ms | 5.47ms | 1.0× | 3.3% | PASS |
| mem_reads | `covering_index_scan` | 6.44ms | 9.18ms | 1.4× | 2.7% | PASS |
| mem_reads | `groupby_scan` | 31.50ms | 34.04ms | 1.1× | 2.2% | PASS |
| mem_reads | `index_join` | 9.81ms | 8.53ms | 0.9× | 3.7% | PASS |
| mem_reads | `index_join_scan` | 2.98ms | 5.78ms | 1.9× | 3.6% | PASS |
| mem_reads | `types_table_scan` | 988.30ms | 1.26s | 1.3× | 0.9% | PASS |
| mem_reads | `table_scan` | 1.12s | 1.35s | 1.2× | 1.2% | PASS |
| mem_reads | `oltp_read_only` | 114.09ms | 130.51ms | 1.1× | 1.9% | PASS |
| mem_writes | `oltp_bulk_insert` | 188.16ms | 271.02ms | 1.4× | 1.3% | PASS |
| mem_writes | `oltp_insert` | 14.88ms | 29.43ms | 2.0× | 2.5% | PASS |
| mem_writes | `oltp_update_index` | 52.76ms | 105.70ms | 2.0× | 3.3% | PASS |
| mem_writes | `oltp_update_non_index` | 37.01ms | 62.17ms | 1.7× | 3.7% | PASS |
| mem_writes | `oltp_delete_insert` | 42.56ms | 75.87ms | 1.8× | 2.4% | PASS |
| mem_writes | `oltp_write_only` | 21.84ms | 41.49ms | 1.9× | 3.6% | PASS |
| mem_writes | `types_delete_insert` | 30.61ms | 41.61ms | 1.4× | 2.6% | PASS |
| mem_writes | `oltp_read_write` | 73.73ms | 104.84ms | 1.4× | 1.9% | PASS |
| file_reads | `oltp_point_select` | 57.58ms | 37.68ms | 0.7× | 3.1% | PASS |
| file_reads | `oltp_range_select` | 15.97ms | 15.29ms | 1.0× | 3.4% | PASS |
| file_reads | `oltp_sum_range` | 15.65ms | 13.92ms | 0.9× | 4.6% | PASS |
| file_reads | `oltp_order_range` | 3.19ms | 3.38ms | 1.1× | 4.0% | PASS |
| file_reads | `oltp_distinct_range` | 4.16ms | 4.50ms | 1.1× | 2.8% | PASS |
| file_reads | `oltp_index_scan` | 6.35ms | 6.11ms | 1.0× | 3.3% | PASS |
| file_reads | `select_random_points` | 22.94ms | 20.79ms | 0.9× | 2.5% | PASS |
| file_reads | `select_random_ranges` | 8.47ms | 6.37ms | 0.8× | 3.1% | PASS |
| file_reads | `covering_index_scan` | 9.47ms | 10.16ms | 1.1× | 2.4% | PASS |
| file_reads | `groupby_scan` | 31.86ms | 34.73ms | 1.1× | 2.7% | PASS |
| file_reads | `index_join` | 11.61ms | 9.28ms | 0.8× | 3.0% | PASS |
| file_reads | `index_join_scan` | 3.43ms | 6.22ms | 1.8× | 3.1% | PASS |
| file_reads | `types_table_scan` | 994.75ms | 1.25s | 1.3× | 0.9% | PASS |
| file_reads | `table_scan` | 1.14s | 1.36s | 1.2× | 1.0% | PASS |
| file_reads | `oltp_read_only` | 152.97ms | 141.42ms | 0.9× | 1.7% | PASS |
| file_writes | `oltp_bulk_insert` | 272.01ms | 345.95ms | 1.3× | 2.7% | PASS |
| file_writes | `oltp_insert` | 28.94ms | 55.12ms | 1.9× | 2.9% | PASS |
| file_writes | `oltp_update_index` | 170.66ms | 194.12ms | 1.1× | 3.0% | PASS |
| file_writes | `oltp_update_non_index` | 143.86ms | 127.16ms | 0.9× | 2.3% | PASS |
| file_writes | `oltp_delete_insert` | 180.47ms | 151.97ms | 0.8× | 2.2% | PASS |
| file_writes | `oltp_write_only` | 121.03ms | 101.93ms | 0.8× | 1.9% | PASS |
| file_writes | `types_delete_insert` | 143.18ms | 89.01ms | 0.6× | 7.5% | PASS |
| file_writes | `oltp_read_write` | 173.84ms | 168.29ms | 1.0× | 2.5% | PASS |
| ac_reads | `oltp_point_select` | 38.96ms | 38.70ms | 1.0× | 2.9% | PASS |
| ac_reads | `oltp_range_select` | 14.98ms | 15.95ms | 1.1× | 1.7% | PASS |
| ac_reads | `oltp_sum_range` | 14.22ms | 14.29ms | 1.0× | 3.9% | PASS |
| ac_reads | `oltp_order_range` | 3.02ms | 3.34ms | 1.1× | 3.9% | PASS |
| ac_reads | `oltp_distinct_range` | 4.05ms | 4.49ms | 1.1× | 2.8% | PASS |
| ac_reads | `oltp_index_scan` | 4.59ms | 6.21ms | 1.4× | 2.6% | PASS |
| ac_reads | `select_random_points` | 21.88ms | 21.39ms | 1.0× | 1.9% | PASS |
| ac_reads | `select_random_ranges` | 6.73ms | 6.42ms | 1.0× | 3.1% | PASS |
| ac_reads | `covering_index_scan` | 7.55ms | 10.15ms | 1.3× | 2.0% | PASS |
| ac_reads | `groupby_scan` | 32.42ms | 34.89ms | 1.1× | 1.9% | PASS |
| ac_reads | `index_join` | 11.02ms | 9.43ms | 0.9× | 2.6% | PASS |
| ac_reads | `index_join_scan` | 3.34ms | 6.17ms | 1.8× | 3.4% | PASS |
| ac_reads | `types_table_scan` | 983.07ms | 1.24s | 1.3× | 1.0% | PASS |
| ac_reads | `table_scan` | 1.14s | 1.35s | 1.2× | 1.2% | PASS |
| ac_reads | `oltp_read_only` | 125.87ms | 138.16ms | 1.1× | 2.3% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 25.77ms | 65.96ms | 2.6× | 4.4% | PASS |
| ac_writes | `oltp_insert_ac` | 27.29ms | 75.33ms | 2.8× | 4.3% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.74ms | 86.75ms | 3.1× | 4.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 25.49ms | 72.39ms | 2.8× | 4.7% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 27.41ms | 82.99ms | 3.0× | 4.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 27.18ms | 79.37ms | 2.9× | 4.8% | PASS |
| ac_writes | `types_delete_insert_ac` | 26.22ms | 73.76ms | 2.8× | 6.3% | PASS |
| ac_writes | `oltp_read_write_ac` | 30.93ms | 84.97ms | 2.7× | 6.5% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 34.67ms | 40.20ms | 1.2× | 2.0% | PASS |
| mem_reads | `oltp_range_select` | 16.16ms | 16.46ms | 1.0× | 2.1% | PASS |
| mem_reads | `oltp_sum_range` | 14.81ms | 15.31ms | 1.0× | 2.0% | PASS |
| mem_reads | `oltp_order_range` | 3.20ms | 3.42ms | 1.1× | 1.7% | PASS |
| mem_reads | `oltp_distinct_range` | 4.29ms | 4.55ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 3.93ms | 6.75ms | 1.7× | 1.9% | PASS |
| mem_reads | `select_random_points` | 22.03ms | 22.86ms | 1.0× | 3.0% | PASS |
| mem_reads | `select_random_ranges` | 6.53ms | 6.93ms | 1.1× | 2.0% | PASS |
| mem_reads | `covering_index_scan` | 7.66ms | 11.44ms | 1.5× | 0.8% | PASS |
| mem_reads | `groupby_scan` | 33.43ms | 36.20ms | 1.1× | 0.8% | PASS |
| mem_reads | `index_join` | 10.07ms | 10.94ms | 1.1× | 1.5% | PASS |
| mem_reads | `index_join_scan` | 3.73ms | 6.55ms | 1.8× | 2.3% | PASS |
| mem_reads | `types_table_scan` | 1.04s | 1.40s | 1.3× | 0.7% | PASS |
| mem_reads | `table_scan` | 1.20s | 1.46s | 1.2× | 1.5% | PASS |
| mem_reads | `oltp_read_only` | 133.91ms | 148.32ms | 1.1× | 1.9% | PASS |
| mem_writes | `oltp_bulk_insert` | 239.21ms | 361.71ms | 1.5× | 1.1% | PASS |
| mem_writes | `oltp_insert` | 18.31ms | 38.27ms | 2.1× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 64.86ms | 129.55ms | 2.0× | 1.6% | PASS |
| mem_writes | `oltp_update_non_index` | 47.47ms | 80.88ms | 1.7× | 1.5% | PASS |
| mem_writes | `oltp_delete_insert` | 52.77ms | 98.49ms | 1.9× | 1.5% | PASS |
| mem_writes | `oltp_write_only` | 27.91ms | 55.67ms | 2.0× | 1.8% | PASS |
| mem_writes | `types_delete_insert` | 38.28ms | 55.79ms | 1.5× | 1.6% | PASS |
| mem_writes | `oltp_read_write` | 104.58ms | 143.30ms | 1.4× | 1.9% | PASS |
| file_reads | `oltp_point_select` | 107.74ms | 60.33ms | 0.6× | 1.3% | PASS |
| file_reads | `oltp_range_select` | 24.60ms | 18.82ms | 0.8× | 1.5% | PASS |
| file_reads | `oltp_sum_range` | 23.09ms | 17.60ms | 0.8× | 1.1% | PASS |
| file_reads | `oltp_order_range` | 4.38ms | 3.70ms | 0.8× | 1.8% | PASS |
| file_reads | `oltp_distinct_range` | 5.58ms | 4.91ms | 0.9× | 2.1% | PASS |
| file_reads | `oltp_index_scan` | 11.56ms | 8.97ms | 0.8× | 1.4% | PASS |
| file_reads | `select_random_points` | 31.37ms | 25.93ms | 0.8× | 2.4% | PASS |
| file_reads | `select_random_ranges` | 14.28ms | 9.08ms | 0.6× | 1.3% | PASS |
| file_reads | `covering_index_scan` | 15.39ms | 13.53ms | 0.9× | 1.0% | PASS |
| file_reads | `groupby_scan` | 34.91ms | 36.64ms | 1.0× | 0.7% | PASS |
| file_reads | `index_join` | 15.20ms | 12.38ms | 0.8× | 1.6% | PASS |
| file_reads | `index_join_scan` | 4.84ms | 6.94ms | 1.4× | 1.4% | PASS |
| file_reads | `types_table_scan` | 1.13s | 1.44s | 1.3× | 2.1% | PASS |
| file_reads | `table_scan` | 1.22s | 1.48s | 1.2× | 1.4% | PASS |
| file_reads | `oltp_read_only` | 251.83ms | 181.54ms | 0.7× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 261.02ms | 377.89ms | 1.4× | 0.9% | PASS |
| file_writes | `oltp_insert` | 25.94ms | 43.66ms | 1.7× | 1.7% | PASS |
| file_writes | `oltp_update_index` | 98.35ms | 149.53ms | 1.5× | 1.8% | PASS |
| file_writes | `oltp_update_non_index` | 92.75ms | 95.22ms | 1.0× | 10.2% | PASS |
| file_writes | `oltp_delete_insert` | 87.77ms | 113.64ms | 1.3× | 1.5% | PASS |
| file_writes | `oltp_write_only` | 57.58ms | 68.81ms | 1.2× | 2.0% | PASS |
| file_writes | `types_delete_insert` | 64.09ms | 67.29ms | 1.0× | 1.3% | PASS |
| file_writes | `oltp_read_write` | 133.50ms | 154.56ms | 1.2× | 2.2% | PASS |
| ac_reads | `oltp_point_select` | 60.50ms | 60.80ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_range_select` | 19.69ms | 18.77ms | 1.0× | 2.0% | PASS |
| ac_reads | `oltp_sum_range` | 18.38ms | 17.75ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_order_range` | 3.94ms | 3.75ms | 1.0× | 2.9% | PASS |
| ac_reads | `oltp_distinct_range` | 4.91ms | 4.88ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_index_scan` | 6.67ms | 8.93ms | 1.3× | 1.6% | PASS |
| ac_reads | `select_random_points` | 26.48ms | 26.56ms | 1.0× | 2.6% | PASS |
| ac_reads | `select_random_ranges` | 9.30ms | 9.04ms | 1.0× | 1.5% | PASS |
| ac_reads | `covering_index_scan` | 10.78ms | 13.68ms | 1.3× | 1.9% | PASS |
| ac_reads | `groupby_scan` | 34.94ms | 37.08ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 12.83ms | 12.55ms | 1.0× | 2.8% | PASS |
| ac_reads | `index_join_scan` | 4.36ms | 7.05ms | 1.6× | 2.3% | PASS |
| ac_reads | `types_table_scan` | 1.09s | 1.43s | 1.3× | 1.7% | PASS |
| ac_reads | `table_scan` | 1.24s | 1.48s | 1.2× | 1.7% | PASS |
| ac_reads | `oltp_read_only` | 179.72ms | 181.04ms | 1.0× | 1.6% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 24.33ms | 65.69ms | 2.7× | 5.3% | PASS |
| ac_writes | `oltp_insert_ac` | 26.97ms | 84.68ms | 3.1× | 6.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.24ms | 96.45ms | 3.5× | 4.9% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.34ms | 76.61ms | 3.4× | 6.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.20ms | 86.52ms | 3.3× | 4.9% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.49ms | 85.31ms | 3.3× | 6.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 24.99ms | 77.86ms | 3.1× | 6.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 33.36ms | 91.47ms | 2.7× | 5.4% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 31.79ms | 41.48ms | 1.3× | 1.1% | PASS |
| mem_reads | `oltp_range_select` | 18.58ms | 24.94ms | 1.3× | 1.4% | PASS |
| mem_reads | `oltp_sum_range` | 17.16ms | 22.27ms | 1.3× | 1.4% | PASS |
| mem_reads | `oltp_order_range` | 3.42ms | 4.13ms | 1.2× | 1.3% | PASS |
| mem_reads | `oltp_distinct_range` | 4.54ms | 5.29ms | 1.2× | 1.3% | PASS |
| mem_reads | `oltp_index_scan` | 4.29ms | 6.21ms | 1.4× | 1.3% | PASS |
| mem_reads | `select_random_points` | 27.15ms | 32.67ms | 1.2× | 1.5% | PASS |
| mem_reads | `select_random_ranges` | 7.30ms | 9.33ms | 1.3× | 1.7% | PASS |
| mem_reads | `covering_index_scan` | 7.54ms | 10.91ms | 1.4× | 0.8% | PASS |
| mem_reads | `groupby_scan` | 35.20ms | 41.91ms | 1.2× | 0.8% | PASS |
| mem_reads | `index_join` | 7.77ms | 11.39ms | 1.5× | 1.2% | PASS |
| mem_reads | `index_join_scan` | 3.85ms | 6.31ms | 1.6× | 1.6% | PASS |
| mem_reads | `types_table_scan` | 1.04s | 1.40s | 1.3× | 0.5% | PASS |
| mem_reads | `table_scan` | 1.18s | 1.47s | 1.2× | 0.7% | PASS |
| mem_reads | `oltp_read_only` | 145.55ms | 183.82ms | 1.3× | 1.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 238.43ms | 346.17ms | 1.5× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 18.38ms | 33.51ms | 1.8× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 63.91ms | 117.31ms | 1.8× | 0.9% | PASS |
| mem_writes | `oltp_update_non_index` | 48.90ms | 81.58ms | 1.7× | 1.7% | PASS |
| mem_writes | `oltp_delete_insert` | 47.65ms | 89.79ms | 1.9× | 0.9% | PASS |
| mem_writes | `oltp_write_only` | 25.75ms | 52.12ms | 2.0× | 0.8% | PASS |
| mem_writes | `types_delete_insert` | 31.77ms | 53.24ms | 1.7× | 1.1% | PASS |
| mem_writes | `oltp_read_write` | 97.66ms | 155.13ms | 1.6× | 1.4% | PASS |
| file_reads | `oltp_point_select` | 101.17ms | 60.50ms | 0.6× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 26.11ms | 27.36ms | 1.0× | 2.2% | PASS |
| file_reads | `oltp_sum_range` | 24.75ms | 24.81ms | 1.0× | 1.4% | PASS |
| file_reads | `oltp_order_range` | 4.29ms | 4.45ms | 1.0× | 1.8% | PASS |
| file_reads | `oltp_distinct_range` | 5.45ms | 5.64ms | 1.0× | 1.6% | PASS |
| file_reads | `oltp_index_scan` | 11.40ms | 8.51ms | 0.7× | 2.0% | PASS |
| file_reads | `select_random_points` | 36.27ms | 36.91ms | 1.0× | 2.0% | PASS |
| file_reads | `select_random_ranges` | 14.85ms | 11.68ms | 0.8× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 14.86ms | 13.42ms | 0.9× | 1.2% | PASS |
| file_reads | `groupby_scan` | 36.32ms | 42.58ms | 1.2× | 1.0% | PASS |
| file_reads | `index_join` | 11.80ms | 13.20ms | 1.1× | 1.6% | PASS |
| file_reads | `index_join_scan` | 4.79ms | 6.88ms | 1.4× | 2.0% | PASS |
| file_reads | `types_table_scan` | 1.04s | 1.40s | 1.3× | 0.5% | PASS |
| file_reads | `table_scan` | 1.18s | 1.46s | 1.2× | 0.6% | PASS |
| file_reads | `oltp_read_only` | 250.22ms | 215.21ms | 0.9× | 0.7% | PASS |
| file_writes | `oltp_bulk_insert` | 254.34ms | 358.64ms | 1.4× | 0.8% | PASS |
| file_writes | `oltp_insert` | 28.26ms | 37.55ms | 1.3× | 1.5% | PASS |
| file_writes | `oltp_update_index` | 93.66ms | 134.22ms | 1.4× | 1.8% | PASS |
| file_writes | `oltp_update_non_index` | 77.37ms | 92.75ms | 1.2× | 1.7% | PASS |
| file_writes | `oltp_delete_insert` | 77.14ms | 102.87ms | 1.3× | 1.2% | PASS |
| file_writes | `oltp_write_only` | 55.08ms | 62.50ms | 1.1× | 1.8% | PASS |
| file_writes | `types_delete_insert` | 49.62ms | 61.68ms | 1.2× | 1.4% | PASS |
| file_writes | `oltp_read_write` | 127.72ms | 167.28ms | 1.3× | 1.1% | PASS |
| ac_reads | `oltp_point_select` | 54.84ms | 60.51ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 21.18ms | 27.34ms | 1.3× | 1.6% | PASS |
| ac_reads | `oltp_sum_range` | 19.79ms | 24.93ms | 1.3× | 1.5% | PASS |
| ac_reads | `oltp_order_range` | 3.77ms | 4.44ms | 1.2× | 1.4% | PASS |
| ac_reads | `oltp_distinct_range` | 4.89ms | 5.66ms | 1.2× | 1.0% | PASS |
| ac_reads | `oltp_index_scan` | 6.72ms | 8.50ms | 1.3× | 1.6% | PASS |
| ac_reads | `select_random_points` | 30.26ms | 36.52ms | 1.2× | 1.7% | PASS |
| ac_reads | `select_random_ranges` | 9.76ms | 11.68ms | 1.2× | 1.4% | PASS |
| ac_reads | `covering_index_scan` | 9.94ms | 13.42ms | 1.3× | 0.9% | PASS |
| ac_reads | `groupby_scan` | 35.67ms | 42.58ms | 1.2× | 0.9% | PASS |
| ac_reads | `index_join` | 9.22ms | 13.24ms | 1.4× | 1.3% | PASS |
| ac_reads | `index_join_scan` | 4.25ms | 6.92ms | 1.6× | 2.0% | PASS |
| ac_reads | `types_table_scan` | 1.04s | 1.40s | 1.3× | 0.5% | PASS |
| ac_reads | `table_scan` | 1.18s | 1.46s | 1.2× | 0.7% | PASS |
| ac_reads | `oltp_read_only` | 181.30ms | 215.23ms | 1.2× | 1.1% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 27.40ms | 74.93ms | 2.7× | 6.1% | PASS |
| ac_writes | `oltp_insert_ac` | 31.26ms | 92.49ms | 3.0× | 6.8% | PASS |
| ac_writes | `oltp_update_index_ac` | 33.76ms | 102.23ms | 3.0× | 4.0% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 29.03ms | 83.68ms | 2.9× | 4.7% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 29.87ms | 92.98ms | 3.1× | 7.2% | PASS |
| ac_writes | `oltp_write_only_ac` | 31.19ms | 91.39ms | 2.9× | 5.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 28.20ms | 86.30ms | 3.1× | 5.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 37.00ms | 101.27ms | 2.7× | 5.0% | PASS |

</details>

</details>

## Version-control latency

Wall time: 5m 39s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 24.18ms | 38.00ms | 63.6% | 1.9% | PASS |
| `status_dirty_many_tables` | 26.35ms | 42.00ms | 62.7% | 1.6% | PASS |
| `diff_regular_working_one_table` | 21.24ms | 33.00ms | 64.4% | 2.1% | PASS |
| `diff_regular_working_many_tables` | 29.23ms | 50.00ms | 58.5% | 2.1% | PASS |
| `diff_stat_working_many_tables` | 28.25ms | 48.00ms | 58.8% | 1.3% | PASS |
| `diff_schema_working_many_tables` | 29.11ms | 48.00ms | 60.6% | 2.2% | PASS |
| `branch_list_many_branches` | 16.72ms | 25.00ms | 66.9% | 1.9% | PASS |
| `branch_create_delete` | 17.95ms | 27.00ms | 66.5% | 2.1% | PASS |
| `at_literal_deep_history` | 18.04ms | 28.00ms | 64.4% | 2.7% | PASS |
| `diff_literal_deep_history` | 18.18ms | 28.00ms | 64.9% | 2.4% | PASS |
| `history_literal_deep_history` | 19.10ms | 30.00ms | 63.7% | 1.9% | PASS |
| `checkout_branch_clean` | 25.20ms | 43.00ms | 58.6% | 2.7% | PASS |
| `merge_data_no_conflicts` | 20.83ms | 33.00ms | 63.1% | 2.3% | PASS |
| `merge_data_secondary_index` | 656.80ms | 944.00ms | 69.6% | 1.4% | PASS |
| `merge_schema_no_conflicts` | 16.35ms | 24.00ms | 68.1% | 2.8% | PASS |
| `merge_data_conflicts` | 23.32ms | 33.00ms | 70.7% | 2.2% | PASS |
| `merge_data_conflicts_with_resolve` | 24.48ms | 34.00ms | 72.0% | 3.0% | PASS |

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

candidate: 3dd9504.

Fixture file bytes: base 107,968,266; delta 115,878,997.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 504.962 | 502.318–530.475 | 33.390 | 198.03 | 107968266 |
| push_initial | candidate | 835.259 | 761.284–867.849 | 370.398 | 119.72 | 107956772 |
| clone_initial | candidate | 985.413 | 887.639–996.822 | 475.383 | 101.48 | 108475035 |
| push_noop | candidate | 0.621 | 0.589–1.003 | 0.327 | — | 0 |
| fetch_noop | candidate | 0.158 | 0.151–0.171 | 0.160 | — | 0 |
| push_incremental | candidate | 179.246 | 176.606–189.880 | 160.836 | 5.58 | 7909775 |
| fetch_incremental | candidate | 82.908 | 79.152–86.272 | 67.145 | 12.06 | 7910084 |
| pull_fast_forward | candidate | 77.291 | 70.379–78.562 | 58.542 | 12.94 | 7910844 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 90.59 | 175.487 | 108.763 | 18329 | 2066 |
| clone_initial | candidate | 90.75 | 295.004 | 109.784 | 47218 | 2337 |
| push_noop | candidate | 41.96 | 0.073 | 0.000 | 26 | 2 |
| fetch_noop | candidate | 41.96 | 0.135 | 0.000 | 40 | 0 |
| push_incremental | candidate | 41.96 | 96.774 | 7.910 | 22388 | 125 |
| fetch_incremental | candidate | 41.96 | 34.472 | 7.910 | 10808 | 124 |
| pull_fast_forward | candidate | 41.96 | 26.497 | 7.911 | 7031 | 126 |

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

candidate: 3dd9504.

Fixture file bytes: base 1,087,718,031; delta 1,103,525,159.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 2648.606 | 2635.008–2669.264 | 368.259 | 377.56 | 1087718031 |
| push_initial | candidate | 7509.913 | 7499.614–7519.348 | 5294.467 | 133.16 | 1079566275 |
| clone_initial | candidate | 8193.343 | 8185.140–8238.932 | 7943.184 | 122.05 | 1102407326 |
| push_noop | candidate | 0.765 | 0.737–1.589 | 0.355 | — | 0 |
| fetch_noop | candidate | 0.221 | 0.189–0.226 | 0.222 | — | 0 |
| push_incremental | candidate | 1279.588 | 1274.695–1279.826 | 1274.205 | 0.78 | 15806003 |
| fetch_incremental | candidate | 247.861 | 246.029–248.132 | 241.599 | 4.03 | 15806513 |
| pull_fast_forward | candidate | 255.264 | 254.384–259.848 | 250.203 | 3.92 | 15807273 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 135.02 | 2092.047 | 1087.622 | 263539 | 20541 |
| clone_initial | candidate | 125.20 | 3515.743 | 1132.819 | 611745 | 32243 |
| push_noop | candidate | 44.81 | 0.078 | 0.000 | 27 | 2 |
| fetch_noop | candidate | 44.81 | 0.137 | 0.000 | 40 | 0 |
| push_incremental | candidate | 44.86 | 615.666 | 15.928 | 149513 | 313 |
| fetch_incremental | candidate | 51.25 | 62.885 | 15.928 | 14054 | 313 |
| pull_fast_forward | candidate | 51.19 | 62.818 | 15.929 | 14050 | 315 |

</details>


</details>

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh`, `test/vc_perf_ceiling.sh`, and `test/remote_perf.py`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
