# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-11-09 11:27 UTC
>
> Commit: [`d7d4e93cc46861ba54dda3e94675e2379ec9633a`](https://github.com/dolthub/doltlite/commit/d7d4e93cc46861ba54dda3e94675e2379ec9633a)
>
> Runner: ubuntu24 20261101.360.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/42594149690)

The SQL workloads compare optimized DoltLite against stock SQLite on the same GitHub-hosted runner, alternating execution order on each repetition. Version-control sections measure DoltLite latency. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.15s | 11.23s | 1.2× | 1.2% | **PASS** |
| Writes | 1.95s | 3.32s | 1.7× | 1.3% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.72s | 11.38s | 1.2× | 1.5% | **PASS** |
| Writes | 3.17s | 3.97s | 1.3× | 1.6% | **PASS** |
| Autocommit writes | 816.00ms | 2.55s | 3.1× | 5.8% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.15s | 2.57s | 1.2× | 1.1% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.50s | 3.11s | 1.2× | 1.5% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.47s | 3.11s | 1.3× | 1.3% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.03s | 2.45s | 1.2× | 1.3% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 379.02ms | 674.26ms | 1.8× | 1.1% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 579.30ms | 994.00ms | 1.7× | 1.1% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 573.74ms | 978.58ms | 1.7× | 1.6% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 414.44ms | 677.72ms | 1.6× | 1.5% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.25s | 2.60s | 1.2× | 1.3% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.72s | 3.16s | 1.2× | 1.5% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.68s | 3.17s | 1.2× | 1.6% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.07s | 2.44s | 1.2× | 1.9% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 473.63ms | 722.10ms | 1.5× | 1.5% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 921.49ms | 1.12s | 1.2× | 4.3% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 794.15ms | 1.08s | 1.4× | 1.5% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 978.30ms | 1.05s | 1.1× | 1.9% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.20s | 2.61s | 1.2× | 1.2% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.58s | 3.16s | 1.2× | 1.9% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.54s | 3.16s | 1.2× | 1.4% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.04s | 2.45s | 1.2× | 2.0% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 134.27ms | 480.98ms | 3.6× | 6.2% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 204.29ms | 649.27ms | 3.2× | 5.8% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 198.55ms | 629.53ms | 3.2× | 5.8% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 278.88ms | 789.34ms | 2.8× | 5.7% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 21.97ms | 28.09ms | 1.3× | 1.7% | PASS |
| mem_reads | `oltp_range_select` | 9.12ms | 11.45ms | 1.3× | 0.7% | PASS |
| mem_reads | `oltp_sum_range` | 8.49ms | 11.26ms | 1.3× | 1.1% | PASS |
| mem_reads | `oltp_order_range` | 2.36ms | 2.73ms | 1.2× | 0.9% | PASS |
| mem_reads | `oltp_distinct_range` | 3.18ms | 3.73ms | 1.2× | 0.5% | PASS |
| mem_reads | `oltp_index_scan` | 3.51ms | 4.99ms | 1.4× | 2.1% | PASS |
| mem_reads | `select_random_points` | 11.71ms | 11.35ms | 1.0× | 1.2% | PASS |
| mem_reads | `select_random_ranges` | 3.99ms | 5.15ms | 1.3× | 0.9% | PASS |
| mem_reads | `covering_index_scan` | 6.53ms | 9.70ms | 1.5× | 1.4% | PASS |
| mem_reads | `groupby_scan` | 27.36ms | 30.85ms | 1.1× | 0.5% | PASS |
| mem_reads | `index_join` | 5.01ms | 8.15ms | 1.6× | 1.1% | PASS |
| mem_reads | `index_join_scan` | 2.70ms | 5.44ms | 2.0× | 1.3% | PASS |
| mem_reads | `types_table_scan` | 910.67ms | 1.09s | 1.2× | 0.8% | PASS |
| mem_reads | `table_scan` | 1.04s | 1.23s | 1.2× | 1.2% | PASS |
| mem_reads | `oltp_read_only` | 92.12ms | 113.11ms | 1.2× | 1.5% | PASS |
| mem_writes | `oltp_bulk_insert` | 155.77ms | 265.42ms | 1.7× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 13.31ms | 23.83ms | 1.8× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 43.62ms | 88.55ms | 2.0× | 1.0% | PASS |
| mem_writes | `oltp_update_non_index` | 29.42ms | 54.89ms | 1.9× | 1.1% | PASS |
| mem_writes | `oltp_delete_insert` | 39.23ms | 69.10ms | 1.8× | 0.9% | PASS |
| mem_writes | `oltp_write_only` | 18.60ms | 41.04ms | 2.2× | 1.1% | PASS |
| mem_writes | `types_delete_insert` | 20.86ms | 33.01ms | 1.6× | 1.1% | PASS |
| mem_writes | `oltp_read_write` | 58.22ms | 98.43ms | 1.7× | 1.3% | PASS |
| file_reads | `oltp_point_select` | 47.94ms | 36.16ms | 0.8× | 2.0% | PASS |
| file_reads | `oltp_range_select` | 11.86ms | 12.53ms | 1.1× | 1.4% | PASS |
| file_reads | `oltp_sum_range` | 11.37ms | 12.39ms | 1.1× | 1.9% | PASS |
| file_reads | `oltp_order_range` | 2.70ms | 2.90ms | 1.1× | 1.5% | PASS |
| file_reads | `oltp_distinct_range` | 3.54ms | 3.90ms | 1.1× | 1.2% | PASS |
| file_reads | `oltp_index_scan` | 6.30ms | 6.14ms | 1.0× | 2.5% | PASS |
| file_reads | `select_random_points` | 14.89ms | 12.43ms | 0.8× | 2.3% | PASS |
| file_reads | `select_random_ranges` | 6.76ms | 6.15ms | 0.9× | 1.5% | PASS |
| file_reads | `covering_index_scan` | 9.52ms | 10.80ms | 1.1× | 1.2% | PASS |
| file_reads | `groupby_scan` | 27.62ms | 31.06ms | 1.1× | 0.8% | PASS |
| file_reads | `index_join` | 6.75ms | 9.09ms | 1.3× | 1.3% | PASS |
| file_reads | `index_join_scan` | 3.11ms | 5.60ms | 1.8× | 1.2% | PASS |
| file_reads | `types_table_scan` | 915.02ms | 1.09s | 1.2× | 0.5% | PASS |
| file_reads | `table_scan` | 1.05s | 1.24s | 1.2× | 0.9% | PASS |
| file_reads | `oltp_read_only` | 131.16ms | 126.10ms | 1.0× | 1.1% | PASS |
| file_writes | `oltp_bulk_insert` | 167.82ms | 274.40ms | 1.6× | 0.8% | PASS |
| file_writes | `oltp_insert` | 17.64ms | 26.78ms | 1.5× | 1.6% | PASS |
| file_writes | `oltp_update_index` | 60.28ms | 95.36ms | 1.6× | 1.4% | PASS |
| file_writes | `oltp_update_non_index` | 42.96ms | 59.91ms | 1.4× | 1.4% | PASS |
| file_writes | `oltp_delete_insert` | 50.90ms | 78.20ms | 1.5× | 1.2% | PASS |
| file_writes | `oltp_write_only` | 31.38ms | 46.09ms | 1.5× | 1.7% | PASS |
| file_writes | `types_delete_insert` | 30.33ms | 38.44ms | 1.3× | 1.7% | PASS |
| file_writes | `oltp_read_write` | 72.32ms | 102.92ms | 1.4× | 1.6% | PASS |
| ac_reads | `oltp_point_select` | 30.82ms | 36.55ms | 1.2× | 1.7% | PASS |
| ac_reads | `oltp_range_select` | 10.18ms | 12.49ms | 1.2× | 0.8% | PASS |
| ac_reads | `oltp_sum_range` | 9.59ms | 12.36ms | 1.3× | 0.9% | PASS |
| ac_reads | `oltp_order_range` | 2.52ms | 2.89ms | 1.2× | 0.9% | PASS |
| ac_reads | `oltp_distinct_range` | 3.36ms | 3.89ms | 1.2× | 1.2% | PASS |
| ac_reads | `oltp_index_scan` | 4.68ms | 6.08ms | 1.3× | 2.2% | PASS |
| ac_reads | `select_random_points` | 13.16ms | 12.45ms | 0.9× | 1.7% | PASS |
| ac_reads | `select_random_ranges` | 5.06ms | 6.15ms | 1.2× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 7.71ms | 10.75ms | 1.4× | 0.9% | PASS |
| ac_reads | `groupby_scan` | 27.54ms | 31.07ms | 1.1× | 0.6% | PASS |
| ac_reads | `index_join` | 5.78ms | 8.98ms | 1.6× | 1.8% | PASS |
| ac_reads | `index_join_scan` | 2.94ms | 5.54ms | 1.9× | 1.2% | PASS |
| ac_reads | `types_table_scan` | 918.56ms | 1.10s | 1.2× | 0.9% | PASS |
| ac_reads | `table_scan` | 1.05s | 1.24s | 1.2× | 1.0% | PASS |
| ac_reads | `oltp_read_only` | 107.12ms | 127.07ms | 1.2× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 14.87ms | 46.26ms | 3.1× | 5.8% | PASS |
| ac_writes | `oltp_insert_ac` | 16.72ms | 59.48ms | 3.6× | 5.6% | PASS |
| ac_writes | `oltp_update_index_ac` | 18.29ms | 71.75ms | 3.9× | 7.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 15.17ms | 53.73ms | 3.5× | 6.7% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 16.92ms | 63.75ms | 3.8× | 5.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 17.14ms | 63.54ms | 3.7× | 7.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 15.25ms | 54.22ms | 3.6× | 6.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 19.91ms | 68.24ms | 3.4× | 5.3% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.80ms | 42.41ms | 1.3× | 1.7% | PASS |
| mem_reads | `oltp_range_select` | 15.90ms | 17.30ms | 1.1× | 1.7% | PASS |
| mem_reads | `oltp_sum_range` | 14.43ms | 16.84ms | 1.2× | 1.8% | PASS |
| mem_reads | `oltp_order_range` | 3.20ms | 3.68ms | 1.2× | 1.5% | PASS |
| mem_reads | `oltp_distinct_range` | 4.35ms | 4.89ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 3.83ms | 7.07ms | 1.8× | 1.0% | PASS |
| mem_reads | `select_random_points` | 20.64ms | 23.53ms | 1.1× | 2.4% | PASS |
| mem_reads | `select_random_ranges` | 6.29ms | 7.30ms | 1.2× | 1.8% | PASS |
| mem_reads | `covering_index_scan` | 7.67ms | 11.72ms | 1.5× | 0.8% | PASS |
| mem_reads | `groupby_scan` | 33.93ms | 38.03ms | 1.1× | 0.6% | PASS |
| mem_reads | `index_join` | 10.12ms | 10.92ms | 1.1× | 1.8% | PASS |
| mem_reads | `index_join_scan` | 3.58ms | 6.86ms | 1.9× | 1.7% | PASS |
| mem_reads | `types_table_scan` | 1.04s | 1.33s | 1.3× | 0.6% | PASS |
| mem_reads | `table_scan` | 1.16s | 1.43s | 1.2× | 0.6% | PASS |
| mem_reads | `oltp_read_only` | 135.29ms | 154.90ms | 1.1× | 1.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 238.03ms | 369.55ms | 1.6× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 17.73ms | 36.67ms | 2.1× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 63.69ms | 139.50ms | 2.2× | 1.0% | PASS |
| mem_writes | `oltp_update_non_index` | 47.07ms | 85.12ms | 1.8× | 2.0% | PASS |
| mem_writes | `oltp_delete_insert` | 53.16ms | 103.77ms | 2.0× | 1.3% | PASS |
| mem_writes | `oltp_write_only` | 27.29ms | 53.76ms | 2.0× | 1.2% | PASS |
| mem_writes | `types_delete_insert` | 38.39ms | 64.72ms | 1.7× | 1.3% | PASS |
| mem_writes | `oltp_read_write` | 93.94ms | 140.93ms | 1.5× | 1.0% | PASS |
| file_reads | `oltp_point_select` | 99.60ms | 59.52ms | 0.6× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 23.35ms | 19.14ms | 0.8× | 2.4% | PASS |
| file_reads | `oltp_sum_range` | 21.70ms | 18.67ms | 0.9× | 1.5% | PASS |
| file_reads | `oltp_order_range` | 4.13ms | 3.95ms | 1.0× | 2.6% | PASS |
| file_reads | `oltp_distinct_range` | 5.20ms | 5.16ms | 1.0× | 1.7% | PASS |
| file_reads | `oltp_index_scan` | 10.53ms | 8.91ms | 0.8× | 1.3% | PASS |
| file_reads | `select_random_points` | 29.65ms | 26.19ms | 0.9× | 2.9% | PASS |
| file_reads | `select_random_ranges` | 13.45ms | 9.25ms | 0.7× | 1.8% | PASS |
| file_reads | `covering_index_scan` | 14.55ms | 13.65ms | 0.9× | 1.3% | PASS |
| file_reads | `groupby_scan` | 35.14ms | 38.40ms | 1.1× | 1.2% | PASS |
| file_reads | `index_join` | 14.13ms | 12.27ms | 0.9× | 2.2% | PASS |
| file_reads | `index_join_scan` | 4.59ms | 7.19ms | 1.6× | 1.8% | PASS |
| file_reads | `types_table_scan` | 1.04s | 1.33s | 1.3× | 0.5% | PASS |
| file_reads | `table_scan` | 1.17s | 1.43s | 1.2× | 0.6% | PASS |
| file_reads | `oltp_read_only` | 233.79ms | 181.94ms | 0.8× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 264.28ms | 389.73ms | 1.5× | 1.0% | PASS |
| file_writes | `oltp_insert` | 26.21ms | 42.83ms | 1.6× | 1.6% | PASS |
| file_writes | `oltp_update_index` | 131.41ms | 161.70ms | 1.2× | 9.6% | PASS |
| file_writes | `oltp_update_non_index` | 98.38ms | 101.02ms | 1.0× | 7.6% | PASS |
| file_writes | `oltp_delete_insert` | 95.75ms | 121.49ms | 1.3× | 1.5% | PASS |
| file_writes | `oltp_write_only` | 85.23ms | 67.56ms | 0.8× | 10.0% | PASS |
| file_writes | `types_delete_insert` | 72.07ms | 75.71ms | 1.1× | 2.0% | PASS |
| file_writes | `oltp_read_write` | 148.16ms | 155.00ms | 1.0× | 6.5% | PASS |
| ac_reads | `oltp_point_select` | 56.46ms | 59.30ms | 1.1× | 1.6% | PASS |
| ac_reads | `oltp_range_select` | 19.06ms | 19.20ms | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_sum_range` | 17.24ms | 18.73ms | 1.1× | 2.2% | PASS |
| ac_reads | `oltp_order_range` | 3.64ms | 3.95ms | 1.1× | 2.2% | PASS |
| ac_reads | `oltp_distinct_range` | 4.74ms | 5.16ms | 1.1× | 1.9% | PASS |
| ac_reads | `oltp_index_scan` | 6.24ms | 8.99ms | 1.4× | 1.8% | PASS |
| ac_reads | `select_random_points` | 24.95ms | 26.17ms | 1.0× | 2.4% | PASS |
| ac_reads | `select_random_ranges` | 8.92ms | 9.27ms | 1.0× | 2.3% | PASS |
| ac_reads | `covering_index_scan` | 10.12ms | 13.70ms | 1.4× | 1.1% | PASS |
| ac_reads | `groupby_scan` | 34.43ms | 38.33ms | 1.1× | 1.0% | PASS |
| ac_reads | `index_join` | 12.00ms | 12.24ms | 1.0× | 2.0% | PASS |
| ac_reads | `index_join_scan` | 4.12ms | 7.17ms | 1.7× | 2.3% | PASS |
| ac_reads | `types_table_scan` | 1.04s | 1.33s | 1.3× | 0.5% | PASS |
| ac_reads | `table_scan` | 1.17s | 1.43s | 1.2× | 0.5% | PASS |
| ac_reads | `oltp_read_only` | 169.72ms | 182.09ms | 1.1× | 0.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.54ms | 65.79ms | 2.9× | 5.5% | PASS |
| ac_writes | `oltp_insert_ac` | 25.25ms | 76.47ms | 3.0× | 4.8% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.02ms | 94.82ms | 3.6× | 4.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.47ms | 74.45ms | 3.3× | 6.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.79ms | 87.59ms | 3.3× | 6.0% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.10ms | 84.87ms | 3.4× | 7.8% | PASS |
| ac_writes | `types_delete_insert_ac` | 23.93ms | 74.85ms | 3.1× | 7.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.20ms | 90.43ms | 2.8× | 5.3% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.17ms | 41.63ms | 1.3× | 1.5% | PASS |
| mem_reads | `oltp_range_select` | 15.28ms | 16.93ms | 1.1× | 1.7% | PASS |
| mem_reads | `oltp_sum_range` | 13.99ms | 16.25ms | 1.2× | 1.2% | PASS |
| mem_reads | `oltp_order_range` | 3.08ms | 3.64ms | 1.2× | 1.3% | PASS |
| mem_reads | `oltp_distinct_range` | 4.21ms | 4.83ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 3.84ms | 6.91ms | 1.8× | 1.3% | PASS |
| mem_reads | `select_random_points` | 20.98ms | 22.93ms | 1.1× | 3.1% | PASS |
| mem_reads | `select_random_ranges` | 6.42ms | 7.16ms | 1.1× | 1.9% | PASS |
| mem_reads | `covering_index_scan` | 7.67ms | 11.60ms | 1.5× | 1.1% | PASS |
| mem_reads | `groupby_scan` | 33.85ms | 37.72ms | 1.1× | 0.9% | PASS |
| mem_reads | `index_join` | 9.68ms | 10.93ms | 1.1× | 1.6% | PASS |
| mem_reads | `index_join_scan` | 3.57ms | 6.58ms | 1.8× | 2.0% | PASS |
| mem_reads | `types_table_scan` | 1.03s | 1.33s | 1.3× | 0.5% | PASS |
| mem_reads | `table_scan` | 1.15s | 1.43s | 1.2× | 1.0% | PASS |
| mem_reads | `oltp_read_only` | 132.59ms | 154.28ms | 1.2× | 1.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 238.92ms | 371.88ms | 1.6× | 0.5% | PASS |
| mem_writes | `oltp_insert` | 18.27ms | 36.78ms | 2.0× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 61.99ms | 132.81ms | 2.1× | 1.3% | PASS |
| mem_writes | `oltp_update_non_index` | 46.22ms | 81.67ms | 1.8× | 1.7% | PASS |
| mem_writes | `oltp_delete_insert` | 51.24ms | 100.39ms | 2.0× | 1.6% | PASS |
| mem_writes | `oltp_write_only` | 27.59ms | 53.08ms | 1.9× | 1.6% | PASS |
| mem_writes | `types_delete_insert` | 36.95ms | 63.14ms | 1.7× | 2.1% | PASS |
| mem_writes | `oltp_read_write` | 92.57ms | 138.82ms | 1.5× | 1.6% | PASS |
| file_reads | `oltp_point_select` | 98.59ms | 58.89ms | 0.6× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 22.41ms | 18.77ms | 0.8× | 2.0% | PASS |
| file_reads | `oltp_sum_range` | 20.95ms | 18.18ms | 0.9× | 1.6% | PASS |
| file_reads | `oltp_order_range` | 4.11ms | 3.88ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_distinct_range` | 5.24ms | 5.12ms | 1.0× | 1.5% | PASS |
| file_reads | `oltp_index_scan` | 10.53ms | 8.83ms | 0.8× | 1.8% | PASS |
| file_reads | `select_random_points` | 28.45ms | 25.66ms | 0.9× | 2.4% | PASS |
| file_reads | `select_random_ranges` | 13.27ms | 9.06ms | 0.7× | 1.6% | PASS |
| file_reads | `covering_index_scan` | 14.70ms | 13.44ms | 0.9× | 1.1% | PASS |
| file_reads | `groupby_scan` | 35.10ms | 38.13ms | 1.1× | 1.2% | PASS |
| file_reads | `index_join` | 13.80ms | 12.37ms | 0.9× | 2.8% | PASS |
| file_reads | `index_join_scan` | 4.57ms | 6.88ms | 1.5× | 1.6% | PASS |
| file_reads | `types_table_scan` | 1.03s | 1.33s | 1.3× | 0.8% | PASS |
| file_reads | `table_scan` | 1.15s | 1.43s | 1.2× | 0.8% | PASS |
| file_reads | `oltp_read_only` | 232.71ms | 180.93ms | 0.8× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 260.52ms | 387.00ms | 1.5× | 0.9% | PASS |
| file_writes | `oltp_insert` | 25.27ms | 42.12ms | 1.7× | 1.4% | PASS |
| file_writes | `oltp_update_index` | 93.80ms | 151.84ms | 1.6× | 1.4% | PASS |
| file_writes | `oltp_update_non_index` | 87.57ms | 95.27ms | 1.1× | 14.0% | PASS |
| file_writes | `oltp_delete_insert` | 84.45ms | 116.68ms | 1.4× | 1.5% | PASS |
| file_writes | `oltp_write_only` | 56.04ms | 66.05ms | 1.2× | 1.5% | PASS |
| file_writes | `types_delete_insert` | 62.48ms | 70.92ms | 1.1× | 1.5% | PASS |
| file_writes | `oltp_read_write` | 124.02ms | 151.26ms | 1.2× | 1.5% | PASS |
| ac_reads | `oltp_point_select` | 55.71ms | 58.78ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 18.51ms | 18.81ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_sum_range` | 17.11ms | 18.27ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_order_range` | 3.73ms | 3.87ms | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_distinct_range` | 4.81ms | 5.10ms | 1.1× | 1.5% | PASS |
| ac_reads | `oltp_index_scan` | 6.31ms | 8.82ms | 1.4× | 1.4% | PASS |
| ac_reads | `select_random_points` | 25.04ms | 25.62ms | 1.0× | 2.4% | PASS |
| ac_reads | `select_random_ranges` | 9.01ms | 9.02ms | 1.0× | 1.7% | PASS |
| ac_reads | `covering_index_scan` | 10.30ms | 13.49ms | 1.3× | 1.0% | PASS |
| ac_reads | `groupby_scan` | 34.42ms | 38.14ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 11.77ms | 12.44ms | 1.1× | 1.9% | PASS |
| ac_reads | `index_join_scan` | 4.16ms | 6.96ms | 1.7× | 2.0% | PASS |
| ac_reads | `types_table_scan` | 1.03s | 1.33s | 1.3× | 0.9% | PASS |
| ac_reads | `table_scan` | 1.14s | 1.43s | 1.3× | 0.7% | PASS |
| ac_reads | `oltp_read_only` | 169.42ms | 180.96ms | 1.1× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.86ms | 62.62ms | 2.9× | 6.6% | PASS |
| ac_writes | `oltp_insert_ac` | 25.92ms | 79.78ms | 3.1× | 4.3% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.57ms | 91.98ms | 3.5× | 6.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.82ms | 72.44ms | 3.3× | 5.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.38ms | 81.94ms | 3.4× | 6.9% | PASS |
| ac_writes | `oltp_write_only_ac` | 23.69ms | 80.57ms | 3.4× | 4.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.84ms | 72.73ms | 3.2× | 6.1% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.47ms | 87.47ms | 2.8× | 4.9% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 23.33ms | 29.02ms | 1.2× | 2.1% | PASS |
| mem_reads | `oltp_range_select` | 15.35ms | 17.40ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_sum_range` | 14.87ms | 16.71ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_order_range` | 2.92ms | 3.26ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 3.69ms | 4.12ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 3.54ms | 5.06ms | 1.4× | 2.7% | PASS |
| mem_reads | `select_random_points` | 22.74ms | 26.57ms | 1.2× | 1.0% | PASS |
| mem_reads | `select_random_ranges` | 5.59ms | 7.06ms | 1.3× | 1.6% | PASS |
| mem_reads | `covering_index_scan` | 5.18ms | 7.65ms | 1.5× | 1.3% | PASS |
| mem_reads | `groupby_scan` | 27.14ms | 31.42ms | 1.2× | 0.9% | PASS |
| mem_reads | `index_join` | 6.17ms | 9.44ms | 1.5× | 2.0% | PASS |
| mem_reads | `index_join_scan` | 3.14ms | 5.66ms | 1.8× | 2.4% | PASS |
| mem_reads | `types_table_scan` | 833.01ms | 1.03s | 1.2× | 0.9% | PASS |
| mem_reads | `table_scan` | 955.60ms | 1.13s | 1.2× | 2.6% | PASS |
| mem_reads | `oltp_read_only` | 109.39ms | 130.98ms | 1.2× | 2.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 163.40ms | 232.86ms | 1.4× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 13.39ms | 23.34ms | 1.7× | 1.2% | PASS |
| mem_writes | `oltp_update_index` | 49.80ms | 93.87ms | 1.9× | 2.5% | PASS |
| mem_writes | `oltp_update_non_index` | 35.93ms | 63.01ms | 1.8× | 1.6% | PASS |
| mem_writes | `oltp_delete_insert` | 36.95ms | 69.28ms | 1.9× | 1.3% | PASS |
| mem_writes | `oltp_write_only` | 20.36ms | 41.45ms | 2.0× | 1.4% | PASS |
| mem_writes | `types_delete_insert` | 22.99ms | 41.10ms | 1.8× | 1.9% | PASS |
| mem_writes | `oltp_read_write` | 71.62ms | 112.80ms | 1.6× | 1.7% | PASS |
| file_reads | `oltp_point_select` | 46.50ms | 34.59ms | 0.7× | 1.9% | PASS |
| file_reads | `oltp_range_select` | 17.82ms | 18.01ms | 1.0× | 1.6% | PASS |
| file_reads | `oltp_sum_range` | 17.21ms | 17.24ms | 1.0× | 1.6% | PASS |
| file_reads | `oltp_order_range` | 3.16ms | 3.37ms | 1.1× | 2.0% | PASS |
| file_reads | `oltp_distinct_range` | 3.95ms | 4.19ms | 1.1× | 1.9% | PASS |
| file_reads | `oltp_index_scan` | 6.00ms | 5.71ms | 1.0× | 2.3% | PASS |
| file_reads | `select_random_points` | 24.82ms | 27.14ms | 1.1× | 2.0% | PASS |
| file_reads | `select_random_ranges` | 7.83ms | 7.51ms | 1.0× | 2.1% | PASS |
| file_reads | `covering_index_scan` | 7.60ms | 8.46ms | 1.1× | 2.2% | PASS |
| file_reads | `groupby_scan` | 26.97ms | 31.10ms | 1.2× | 1.5% | PASS |
| file_reads | `index_join` | 7.64ms | 9.96ms | 1.3× | 1.4% | PASS |
| file_reads | `index_join_scan` | 3.42ms | 5.89ms | 1.7× | 3.0% | PASS |
| file_reads | `types_table_scan` | 815.37ms | 1.01s | 1.2× | 1.1% | PASS |
| file_reads | `table_scan` | 943.81ms | 1.12s | 1.2× | 2.1% | PASS |
| file_reads | `oltp_read_only` | 139.95ms | 138.16ms | 1.0× | 1.4% | PASS |
| file_writes | `oltp_bulk_insert` | 216.57ms | 287.61ms | 1.3× | 1.4% | PASS |
| file_writes | `oltp_insert` | 29.21ms | 41.74ms | 1.4× | 2.2% | PASS |
| file_writes | `oltp_update_index` | 154.91ms | 163.21ms | 1.1× | 1.6% | PASS |
| file_writes | `oltp_update_non_index` | 128.57ms | 112.11ms | 0.9× | 1.0% | PASS |
| file_writes | `oltp_delete_insert` | 128.64ms | 128.08ms | 1.0× | 2.3% | PASS |
| file_writes | `oltp_write_only` | 93.46ms | 86.87ms | 0.9× | 1.4% | PASS |
| file_writes | `types_delete_insert` | 83.63ms | 74.79ms | 0.9× | 4.1% | PASS |
| file_writes | `oltp_read_write` | 143.30ms | 155.63ms | 1.1× | 2.7% | PASS |
| ac_reads | `oltp_point_select` | 30.28ms | 33.93ms | 1.1× | 1.8% | PASS |
| ac_reads | `oltp_range_select` | 15.98ms | 17.76ms | 1.1× | 2.0% | PASS |
| ac_reads | `oltp_sum_range` | 15.56ms | 17.06ms | 1.1× | 1.6% | PASS |
| ac_reads | `oltp_order_range` | 3.01ms | 3.33ms | 1.1× | 2.1% | PASS |
| ac_reads | `oltp_distinct_range` | 3.73ms | 4.13ms | 1.1× | 2.3% | PASS |
| ac_reads | `oltp_index_scan` | 4.57ms | 5.68ms | 1.2× | 2.7% | PASS |
| ac_reads | `select_random_points` | 23.50ms | 27.09ms | 1.2× | 1.6% | PASS |
| ac_reads | `select_random_ranges` | 6.36ms | 7.51ms | 1.2× | 2.1% | PASS |
| ac_reads | `covering_index_scan` | 6.13ms | 8.46ms | 1.4× | 2.0% | PASS |
| ac_reads | `groupby_scan` | 26.81ms | 31.16ms | 1.2× | 1.3% | PASS |
| ac_reads | `index_join` | 6.93ms | 9.99ms | 1.4× | 2.1% | PASS |
| ac_reads | `index_join_scan` | 3.29ms | 5.80ms | 1.8× | 3.6% | PASS |
| ac_reads | `types_table_scan` | 825.37ms | 1.02s | 1.2× | 0.9% | PASS |
| ac_reads | `table_scan` | 947.75ms | 1.12s | 1.2× | 2.3% | PASS |
| ac_reads | `oltp_read_only` | 119.57ms | 137.93ms | 1.2× | 2.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 32.98ms | 86.17ms | 2.6× | 4.2% | PASS |
| ac_writes | `oltp_insert_ac` | 34.69ms | 99.06ms | 2.9× | 6.1% | PASS |
| ac_writes | `oltp_update_index_ac` | 36.89ms | 111.09ms | 3.0× | 5.8% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 32.88ms | 94.38ms | 2.9× | 6.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 34.52ms | 100.31ms | 2.9× | 5.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 34.36ms | 98.23ms | 2.9× | 5.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 33.05ms | 93.50ms | 2.8× | 5.2% | PASS |
| ac_writes | `oltp_read_write_ac` | 39.52ms | 106.59ms | 2.7× | 5.8% | PASS |

</details>

</details>

## Version-control latency

Wall time: 5m 42s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 22.12ms | 38.00ms | 58.2% | 0.4% | PASS |
| `status_dirty_many_tables` | 24.23ms | 42.00ms | 57.7% | 0.4% | PASS |
| `diff_regular_working_one_table` | 19.46ms | 33.00ms | 59.0% | 0.7% | PASS |
| `diff_regular_working_many_tables` | 26.31ms | 50.00ms | 52.6% | 0.7% | PASS |
| `diff_stat_working_many_tables` | 26.32ms | 48.00ms | 54.8% | 0.6% | PASS |
| `diff_schema_working_many_tables` | 26.62ms | 48.00ms | 55.5% | 0.6% | PASS |
| `branch_list_many_branches` | 14.87ms | 25.00ms | 59.5% | 0.9% | PASS |
| `branch_create_delete` | 15.89ms | 27.00ms | 58.9% | 0.6% | PASS |
| `at_literal_deep_history` | 16.09ms | 28.00ms | 57.5% | 1.0% | PASS |
| `diff_literal_deep_history` | 16.05ms | 28.00ms | 57.3% | 0.9% | PASS |
| `history_literal_deep_history` | 16.82ms | 30.00ms | 56.1% | 0.9% | PASS |
| `checkout_branch_clean` | 21.52ms | 43.00ms | 50.0% | 0.9% | PASS |
| `merge_data_no_conflicts` | 18.35ms | 33.00ms | 55.6% | 1.1% | PASS |
| `merge_data_secondary_index` | 621.26ms | 944.00ms | 65.8% | 0.8% | PASS |
| `merge_schema_no_conflicts` | 13.95ms | 24.00ms | 58.1% | 0.6% | PASS |
| `merge_data_conflicts` | 20.05ms | 33.00ms | 60.8% | 0.6% | PASS |
| `merge_data_conflicts_with_resolve` | 20.71ms | 34.00ms | 60.9% | 0.6% | PASS |

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

candidate: d7d4e93.

Fixture file bytes: base 107,968,301; delta 115,879,032.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 414.234 | 403.658–505.638 | 30.708 | 241.41 | 107968301 |
| push_initial | candidate | 724.960 | 722.178–729.297 | 356.523 | 137.94 | 107956807 |
| clone_initial | candidate | 981.660 | 979.339–993.329 | 464.433 | 101.87 | 108475105 |
| push_noop | candidate | 0.517 | 0.481–0.541 | 0.230 | — | 0 |
| fetch_noop | candidate | 0.146 | 0.135–0.172 | 0.148 | — | 0 |
| push_incremental | candidate | 163.237 | 160.775–175.594 | 146.255 | 6.13 | 7909775 |
| fetch_incremental | candidate | 81.816 | 75.335–83.971 | 61.856 | 12.22 | 7910084 |
| pull_fast_forward | candidate | 73.234 | 67.557–77.130 | 54.024 | 13.65 | 7910844 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 90.69 | 175.487 | 108.763 | 18329 | 2066 |
| clone_initial | candidate | 90.94 | 294.992 | 109.784 | 47218 | 2337 |
| push_noop | candidate | 42.87 | 0.073 | 0.000 | 26 | 2 |
| fetch_noop | candidate | 42.87 | 0.135 | 0.000 | 40 | 0 |
| push_incremental | candidate | 42.87 | 96.585 | 7.910 | 22342 | 125 |
| fetch_incremental | candidate | 42.87 | 34.472 | 7.910 | 10808 | 124 |
| pull_fast_forward | candidate | 42.87 | 26.497 | 7.911 | 7031 | 126 |

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

candidate: d7d4e93.

Fixture file bytes: base 1,087,718,066; delta 1,103,525,194.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 5659.766 | 5629.022–5694.596 | 308.858 | 176.69 | 1087718066 |
| push_initial | candidate | 8113.511 | 7973.164–8229.968 | 4328.933 | 123.25 | 1079566310 |
| clone_initial | candidate | 11026.990 | 10742.001–11241.517 | 6134.932 | 90.69 | 1102407396 |
| push_noop | candidate | 0.591 | 0.590–50.494 | 0.295 | — | 0 |
| fetch_noop | candidate | 0.175 | 0.165–0.178 | 0.176 | — | 0 |
| push_incremental | candidate | 1042.570 | 1028.712–1046.702 | 965.052 | 0.96 | 15806003 |
| fetch_incremental | candidate | 300.547 | 299.315–369.455 | 223.082 | 3.33 | 15806513 |
| pull_fast_forward | candidate | 315.102 | 301.836–530.091 | 226.438 | 3.17 | 15807273 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 134.86 | 2091.929 | 1087.622 | 263510 | 20541 |
| clone_initial | candidate | 124.19 | 3514.015 | 1132.819 | 611383 | 32243 |
| push_noop | candidate | 45.59 | 0.078 | 0.000 | 27 | 2 |
| fetch_noop | candidate | 45.59 | 0.137 | 0.000 | 40 | 0 |
| push_incremental | candidate | 48.29 | 614.762 | 15.928 | 149324 | 313 |
| fetch_incremental | candidate | 50.45 | 62.922 | 15.928 | 14063 | 313 |
| pull_fast_forward | candidate | 50.44 | 62.855 | 15.929 | 14059 | 315 |

</details>


</details>

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh`, `test/vc_perf_ceiling.sh`, and `test/remote_perf.py`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
