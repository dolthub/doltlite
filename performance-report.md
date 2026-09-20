# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-09-20 11:05 UTC
>
> Commit: [`25226fad248553ea25d28ed2c53310f55808e32b`](https://github.com/dolthub/doltlite/commit/25226fad248553ea25d28ed2c53310f55808e32b)
>
> Runner: ubuntu24 20260907.300.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/35502636264)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.33s | 10.76s | 1.0× | 1.7% | **PASS** |
| Writes | 2.08s | 3.38s | 1.6× | 1.2% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.90s | 10.90s | 1.0× | 1.3% | **PASS** |
| Writes | 3.39s | 4.15s | 1.2× | 2.0% | **PASS** |
| Autocommit writes | 869.97ms | 2.94s | 3.4× | 6.4% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.41s | 2.62s | 1.1× | 1.7% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.42s | 2.40s | 1.0× | 2.0% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.54s | 2.79s | 1.1× | 1.4% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.96s | 2.96s | 1.0× | 1.4% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 427.52ms | 712.96ms | 1.7× | 0.9% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 477.36ms | 739.55ms | 1.5× | 1.7% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 581.29ms | 965.21ms | 1.7× | 1.3% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 592.77ms | 957.77ms | 1.6× | 1.1% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.65s | 2.69s | 1.0× | 1.5% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.58s | 2.41s | 0.9× | 1.2% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.80s | 2.86s | 1.0× | 1.3% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.88s | 2.95s | 1.0× | 1.7% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 571.59ms | 784.83ms | 1.4× | 1.5% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.22s | 1.22s | 1.0× | 6.0% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 820.69ms | 1.08s | 1.3× | 1.8% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 772.43ms | 1.07s | 1.4× | 2.2% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.48s | 2.67s | 1.1× | 1.2% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.49s | 2.43s | 1.0× | 1.3% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.64s | 2.87s | 1.1× | 1.6% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.88s | 2.98s | 1.0× | 1.6% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 195.90ms | 715.74ms | 3.7× | 5.8% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 257.99ms | 717.86ms | 2.8× | 11.4% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 212.17ms | 764.78ms | 3.6× | 7.5% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 203.91ms | 740.77ms | 3.6× | 5.9% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 23.03ms | 29.26ms | 1.3× | 2.1% | PASS |
| mem_reads | `oltp_range_select` | 9.72ms | 11.28ms | 1.2× | 3.3% | PASS |
| mem_reads | `oltp_sum_range` | 8.91ms | 10.99ms | 1.2× | 2.8% | PASS |
| mem_reads | `oltp_order_range` | 2.45ms | 2.82ms | 1.2× | 2.0% | PASS |
| mem_reads | `oltp_distinct_range` | 3.57ms | 3.95ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 3.79ms | 5.00ms | 1.3× | 1.8% | PASS |
| mem_reads | `select_random_points` | 9.56ms | 10.62ms | 1.1× | 3.2% | PASS |
| mem_reads | `select_random_ranges` | 4.32ms | 4.93ms | 1.1× | 3.3% | PASS |
| mem_reads | `covering_index_scan` | 7.65ms | 9.46ms | 1.2× | 0.8% | PASS |
| mem_reads | `groupby_scan` | 28.71ms | 32.09ms | 1.1× | 1.0% | PASS |
| mem_reads | `index_join` | 5.55ms | 7.39ms | 1.3× | 1.1% | PASS |
| mem_reads | `index_join_scan` | 2.80ms | 4.49ms | 1.6× | 1.7% | PASS |
| mem_reads | `types_table_scan` | 1.04s | 1.13s | 1.1× | 0.6% | PASS |
| mem_reads | `table_scan` | 1.16s | 1.24s | 1.1× | 0.4% | PASS |
| mem_reads | `oltp_read_only` | 97.55ms | 118.23ms | 1.2× | 0.5% | PASS |
| mem_writes | `oltp_bulk_insert` | 179.88ms | 269.73ms | 1.5× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 15.00ms | 28.15ms | 1.9× | 0.6% | PASS |
| mem_writes | `oltp_update_index` | 47.07ms | 89.98ms | 1.9× | 0.8% | PASS |
| mem_writes | `oltp_update_non_index` | 32.24ms | 55.03ms | 1.7× | 1.2% | PASS |
| mem_writes | `oltp_delete_insert` | 42.47ms | 72.13ms | 1.7× | 0.9% | PASS |
| mem_writes | `oltp_write_only` | 20.27ms | 45.34ms | 2.2× | 1.0% | PASS |
| mem_writes | `types_delete_insert` | 24.03ms | 37.87ms | 1.6× | 1.6% | PASS |
| mem_writes | `oltp_read_write` | 66.56ms | 114.74ms | 1.7× | 2.7% | PASS |
| file_reads | `oltp_point_select` | 91.62ms | 48.30ms | 0.5× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 17.36ms | 13.29ms | 0.8× | 2.9% | PASS |
| file_reads | `oltp_sum_range` | 16.84ms | 13.30ms | 0.8× | 1.5% | PASS |
| file_reads | `oltp_order_range` | 3.35ms | 3.15ms | 0.9× | 2.3% | PASS |
| file_reads | `oltp_distinct_range` | 4.46ms | 4.26ms | 1.0× | 1.7% | PASS |
| file_reads | `oltp_index_scan` | 11.02ms | 7.36ms | 0.7× | 1.5% | PASS |
| file_reads | `select_random_points` | 16.70ms | 12.90ms | 0.8× | 3.2% | PASS |
| file_reads | `select_random_ranges` | 11.35ms | 6.97ms | 0.6× | 1.3% | PASS |
| file_reads | `covering_index_scan` | 14.95ms | 11.48ms | 0.8× | 1.0% | PASS |
| file_reads | `groupby_scan` | 29.62ms | 32.51ms | 1.1× | 1.1% | PASS |
| file_reads | `index_join` | 9.63ms | 9.09ms | 0.9× | 1.8% | PASS |
| file_reads | `index_join_scan` | 4.07ms | 4.96ms | 1.2× | 1.9% | PASS |
| file_reads | `types_table_scan` | 1.05s | 1.14s | 1.1× | 1.2% | PASS |
| file_reads | `table_scan` | 1.17s | 1.24s | 1.1× | 0.7% | PASS |
| file_reads | `oltp_read_only` | 198.42ms | 146.55ms | 0.7× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 193.47ms | 280.92ms | 1.5× | 0.8% | PASS |
| file_writes | `oltp_insert` | 21.40ms | 32.00ms | 1.5× | 1.2% | PASS |
| file_writes | `oltp_update_index` | 72.46ms | 102.25ms | 1.4× | 1.4% | PASS |
| file_writes | `oltp_update_non_index` | 55.19ms | 67.34ms | 1.2× | 2.2% | PASS |
| file_writes | `oltp_delete_insert` | 63.98ms | 83.11ms | 1.3× | 1.5% | PASS |
| file_writes | `oltp_write_only` | 42.05ms | 54.59ms | 1.3× | 1.7% | PASS |
| file_writes | `types_delete_insert` | 37.70ms | 43.20ms | 1.1× | 1.4% | PASS |
| file_writes | `oltp_read_write` | 85.34ms | 121.41ms | 1.4× | 1.6% | PASS |
| ac_reads | `oltp_point_select` | 45.21ms | 48.05ms | 1.1× | 0.8% | PASS |
| ac_reads | `oltp_range_select` | 12.12ms | 13.20ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_sum_range` | 11.05ms | 12.96ms | 1.2× | 1.2% | PASS |
| ac_reads | `oltp_order_range` | 2.75ms | 3.09ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_distinct_range` | 3.80ms | 4.20ms | 1.1× | 1.0% | PASS |
| ac_reads | `oltp_index_scan` | 6.08ms | 7.09ms | 1.2× | 1.4% | PASS |
| ac_reads | `select_random_points` | 11.50ms | 12.71ms | 1.1× | 2.6% | PASS |
| ac_reads | `select_random_ranges` | 6.67ms | 6.95ms | 1.0× | 1.6% | PASS |
| ac_reads | `covering_index_scan` | 10.07ms | 11.38ms | 1.1× | 0.8% | PASS |
| ac_reads | `groupby_scan` | 28.75ms | 32.38ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 7.05ms | 9.08ms | 1.3× | 1.4% | PASS |
| ac_reads | `index_join_scan` | 3.42ms | 5.01ms | 1.5× | 2.7% | PASS |
| ac_reads | `types_table_scan` | 1.03s | 1.13s | 1.1× | 0.4% | PASS |
| ac_reads | `table_scan` | 1.16s | 1.23s | 1.1× | 0.5% | PASS |
| ac_reads | `oltp_read_only` | 130.92ms | 146.18ms | 1.1× | 0.6% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.13ms | 75.18ms | 3.6× | 5.9% | PASS |
| ac_writes | `oltp_insert_ac` | 24.21ms | 90.16ms | 3.7× | 6.1% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.28ms | 104.01ms | 3.8× | 6.8% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.82ms | 80.34ms | 3.7× | 4.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.47ms | 90.75ms | 3.7× | 5.8% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.37ms | 92.42ms | 3.6× | 6.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.48ms | 84.28ms | 3.7× | 5.6% | PASS |
| ac_writes | `oltp_read_write_ac` | 29.13ms | 98.60ms | 3.4× | 4.2% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 29.96ms | 28.68ms | 1.0× | 2.4% | PASS |
| mem_reads | `oltp_range_select` | 14.85ms | 11.28ms | 0.8× | 1.7% | PASS |
| mem_reads | `oltp_sum_range` | 13.01ms | 11.15ms | 0.9× | 2.0% | PASS |
| mem_reads | `oltp_order_range` | 2.90ms | 2.60ms | 0.9× | 0.8% | PASS |
| mem_reads | `oltp_distinct_range` | 3.65ms | 3.43ms | 0.9× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 3.33ms | 5.14ms | 1.5× | 2.2% | PASS |
| mem_reads | `select_random_points` | 20.57ms | 17.55ms | 0.9× | 3.9% | PASS |
| mem_reads | `select_random_ranges` | 5.68ms | 4.85ms | 0.9× | 1.5% | PASS |
| mem_reads | `covering_index_scan` | 5.96ms | 7.43ms | 1.2× | 1.3% | PASS |
| mem_reads | `groupby_scan` | 29.06ms | 29.51ms | 1.0× | 0.7% | PASS |
| mem_reads | `index_join` | 9.33ms | 7.99ms | 0.9× | 2.2% | PASS |
| mem_reads | `index_join_scan` | 3.31ms | 5.27ms | 1.6× | 3.3% | PASS |
| mem_reads | `types_table_scan` | 1.00s | 1.03s | 1.0× | 2.9% | PASS |
| mem_reads | `table_scan` | 1.16s | 1.13s | 1.0× | 2.1% | PASS |
| mem_reads | `oltp_read_only` | 114.83ms | 105.99ms | 0.9× | 2.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 185.59ms | 258.72ms | 1.4× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 14.23ms | 28.58ms | 2.0× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 54.59ms | 110.16ms | 2.0× | 1.7% | PASS |
| mem_writes | `oltp_update_non_index` | 41.06ms | 61.53ms | 1.5× | 1.9% | PASS |
| mem_writes | `oltp_delete_insert` | 45.40ms | 81.38ms | 1.8× | 1.9% | PASS |
| mem_writes | `oltp_write_only` | 23.61ms | 47.61ms | 2.0× | 1.6% | PASS |
| mem_writes | `types_delete_insert` | 32.22ms | 40.13ms | 1.2× | 1.4% | PASS |
| mem_writes | `oltp_read_write` | 80.65ms | 111.44ms | 1.4× | 2.2% | PASS |
| file_reads | `oltp_point_select` | 95.39ms | 45.77ms | 0.5× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 21.89ms | 13.13ms | 0.6× | 1.6% | PASS |
| file_reads | `oltp_sum_range` | 19.80ms | 12.94ms | 0.7× | 1.3% | PASS |
| file_reads | `oltp_order_range` | 3.61ms | 2.85ms | 0.8× | 0.8% | PASS |
| file_reads | `oltp_distinct_range` | 4.36ms | 3.69ms | 0.8× | 0.8% | PASS |
| file_reads | `oltp_index_scan` | 10.02ms | 6.85ms | 0.7× | 0.8% | PASS |
| file_reads | `select_random_points` | 26.16ms | 18.55ms | 0.7× | 1.3% | PASS |
| file_reads | `select_random_ranges` | 12.31ms | 6.67ms | 0.5× | 1.3% | PASS |
| file_reads | `covering_index_scan` | 12.90ms | 9.24ms | 0.7× | 0.8% | PASS |
| file_reads | `groupby_scan` | 29.47ms | 29.39ms | 1.0× | 0.7% | PASS |
| file_reads | `index_join` | 13.02ms | 8.74ms | 0.7× | 1.1% | PASS |
| file_reads | `index_join_scan` | 3.92ms | 5.36ms | 1.4× | 1.2% | PASS |
| file_reads | `types_table_scan` | 981.97ms | 1.01s | 1.0× | 2.2% | PASS |
| file_reads | `table_scan` | 1.13s | 1.10s | 1.0× | 3.2% | PASS |
| file_reads | `oltp_read_only` | 210.72ms | 130.22ms | 0.6× | 1.2% | PASS |
| file_writes | `oltp_bulk_insert` | 270.17ms | 340.66ms | 1.3× | 6.0% | PASS |
| file_writes | `oltp_insert` | 26.91ms | 50.16ms | 1.9× | 11.0% | PASS |
| file_writes | `oltp_update_index` | 175.23ms | 195.93ms | 1.1× | 3.5% | PASS |
| file_writes | `oltp_update_non_index` | 141.66ms | 121.91ms | 0.9× | 6.0% | PASS |
| file_writes | `oltp_delete_insert` | 181.59ms | 150.54ms | 0.8× | 5.6% | PASS |
| file_writes | `oltp_write_only` | 112.29ms | 102.08ms | 0.9× | 7.8% | PASS |
| file_writes | `types_delete_insert` | 143.25ms | 87.39ms | 0.6× | 15.7% | PASS |
| file_writes | `oltp_read_write` | 171.34ms | 169.33ms | 1.0× | 1.8% | PASS |
| ac_reads | `oltp_point_select` | 52.01ms | 46.05ms | 0.9× | 1.8% | PASS |
| ac_reads | `oltp_range_select` | 17.46ms | 13.25ms | 0.8× | 1.7% | PASS |
| ac_reads | `oltp_sum_range` | 15.57ms | 12.99ms | 0.8× | 1.7% | PASS |
| ac_reads | `oltp_order_range` | 3.24ms | 2.86ms | 0.9× | 0.8% | PASS |
| ac_reads | `oltp_distinct_range` | 3.99ms | 3.70ms | 0.9× | 0.9% | PASS |
| ac_reads | `oltp_index_scan` | 5.76ms | 6.84ms | 1.2× | 1.0% | PASS |
| ac_reads | `select_random_points` | 21.34ms | 18.51ms | 0.9× | 2.0% | PASS |
| ac_reads | `select_random_ranges` | 8.04ms | 6.68ms | 0.8× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 8.53ms | 9.22ms | 1.1× | 0.9% | PASS |
| ac_reads | `groupby_scan` | 28.89ms | 29.42ms | 1.0× | 0.8% | PASS |
| ac_reads | `index_join` | 10.84ms | 8.73ms | 0.8× | 1.3% | PASS |
| ac_reads | `index_join_scan` | 3.58ms | 5.35ms | 1.5× | 1.0% | PASS |
| ac_reads | `types_table_scan` | 994.24ms | 1.01s | 1.0× | 3.3% | PASS |
| ac_reads | `table_scan` | 1.17s | 1.12s | 1.0× | 3.3% | PASS |
| ac_reads | `oltp_read_only` | 147.28ms | 130.68ms | 0.9× | 2.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 29.60ms | 74.83ms | 2.5× | 9.2% | PASS |
| ac_writes | `oltp_insert_ac` | 32.02ms | 86.92ms | 2.7× | 9.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 33.61ms | 105.66ms | 3.1× | 16.3% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 31.38ms | 84.20ms | 2.7× | 17.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 32.53ms | 94.55ms | 2.9× | 11.2% | PASS |
| ac_writes | `oltp_write_only_ac` | 31.67ms | 91.59ms | 2.9× | 12.5% | PASS |
| ac_writes | `types_delete_insert_ac` | 30.13ms | 85.22ms | 2.8× | 11.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 37.04ms | 94.91ms | 2.6× | 8.9% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 34.83ms | 38.24ms | 1.1× | 2.0% | PASS |
| mem_reads | `oltp_range_select` | 16.09ms | 14.09ms | 0.9× | 2.3% | PASS |
| mem_reads | `oltp_sum_range` | 14.72ms | 13.85ms | 0.9× | 2.0% | PASS |
| mem_reads | `oltp_order_range` | 3.19ms | 3.19ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 4.25ms | 4.29ms | 1.0× | 1.3% | PASS |
| mem_reads | `oltp_index_scan` | 3.91ms | 6.17ms | 1.6× | 1.3% | PASS |
| mem_reads | `select_random_points` | 21.26ms | 20.91ms | 1.0× | 2.3% | PASS |
| mem_reads | `select_random_ranges` | 6.49ms | 6.40ms | 1.0× | 2.1% | PASS |
| mem_reads | `covering_index_scan` | 7.72ms | 9.59ms | 1.2× | 1.1% | PASS |
| mem_reads | `groupby_scan` | 33.34ms | 34.14ms | 1.0× | 1.1% | PASS |
| mem_reads | `index_join` | 10.08ms | 9.13ms | 0.9× | 1.6% | PASS |
| mem_reads | `index_join_scan` | 3.70ms | 5.51ms | 1.5× | 2.3% | PASS |
| mem_reads | `types_table_scan` | 1.05s | 1.19s | 1.1× | 0.8% | PASS |
| mem_reads | `table_scan` | 1.19s | 1.29s | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_read_only` | 133.38ms | 138.42ms | 1.0× | 1.4% | PASS |
| mem_writes | `oltp_bulk_insert` | 243.07ms | 357.88ms | 1.5× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 18.44ms | 38.02ms | 2.1× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 62.64ms | 131.93ms | 2.1× | 1.5% | PASS |
| mem_writes | `oltp_update_non_index` | 46.45ms | 78.59ms | 1.7× | 1.3% | PASS |
| mem_writes | `oltp_delete_insert` | 51.14ms | 100.64ms | 2.0× | 1.3% | PASS |
| mem_writes | `oltp_write_only` | 27.06ms | 59.96ms | 2.2× | 1.2% | PASS |
| mem_writes | `types_delete_insert` | 37.46ms | 51.97ms | 1.4× | 1.3% | PASS |
| mem_writes | `oltp_read_write` | 95.03ms | 146.23ms | 1.5× | 2.3% | PASS |
| file_reads | `oltp_point_select` | 104.81ms | 57.78ms | 0.6× | 1.1% | PASS |
| file_reads | `oltp_range_select` | 23.41ms | 16.29ms | 0.7× | 2.3% | PASS |
| file_reads | `oltp_sum_range` | 22.33ms | 16.23ms | 0.7× | 1.9% | PASS |
| file_reads | `oltp_order_range` | 4.19ms | 3.50ms | 0.8× | 1.9% | PASS |
| file_reads | `oltp_distinct_range` | 5.25ms | 4.64ms | 0.9× | 1.2% | PASS |
| file_reads | `oltp_index_scan` | 11.17ms | 8.43ms | 0.8× | 1.3% | PASS |
| file_reads | `select_random_points` | 30.52ms | 24.15ms | 0.8× | 1.9% | PASS |
| file_reads | `select_random_ranges` | 13.94ms | 8.59ms | 0.6× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 15.16ms | 11.84ms | 0.8× | 1.4% | PASS |
| file_reads | `groupby_scan` | 34.57ms | 35.16ms | 1.0× | 0.8% | PASS |
| file_reads | `index_join` | 14.66ms | 10.94ms | 0.7× | 2.4% | PASS |
| file_reads | `index_join_scan` | 4.74ms | 6.04ms | 1.3× | 2.8% | PASS |
| file_reads | `types_table_scan` | 1.06s | 1.19s | 1.1× | 0.7% | PASS |
| file_reads | `table_scan` | 1.21s | 1.30s | 1.1× | 0.8% | PASS |
| file_reads | `oltp_read_only` | 243.91ms | 169.63ms | 0.7× | 1.3% | PASS |
| file_writes | `oltp_bulk_insert` | 264.88ms | 373.00ms | 1.4× | 0.8% | PASS |
| file_writes | `oltp_insert` | 25.98ms | 44.79ms | 1.7× | 1.9% | PASS |
| file_writes | `oltp_update_index` | 97.05ms | 152.43ms | 1.6× | 1.7% | PASS |
| file_writes | `oltp_update_non_index` | 95.44ms | 95.63ms | 1.0× | 7.0% | PASS |
| file_writes | `oltp_delete_insert` | 87.00ms | 117.70ms | 1.4× | 1.8% | PASS |
| file_writes | `oltp_write_only` | 57.29ms | 73.83ms | 1.3× | 2.5% | PASS |
| file_writes | `types_delete_insert` | 64.12ms | 64.98ms | 1.0× | 1.5% | PASS |
| file_writes | `oltp_read_write` | 128.94ms | 160.00ms | 1.2× | 1.4% | PASS |
| ac_reads | `oltp_point_select` | 59.25ms | 58.22ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_range_select` | 19.15ms | 16.32ms | 0.9× | 1.9% | PASS |
| ac_reads | `oltp_sum_range` | 18.02ms | 16.28ms | 0.9× | 1.5% | PASS |
| ac_reads | `oltp_order_range` | 3.72ms | 3.51ms | 0.9× | 2.7% | PASS |
| ac_reads | `oltp_distinct_range` | 4.77ms | 4.66ms | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_index_scan` | 6.49ms | 8.58ms | 1.3× | 1.9% | PASS |
| ac_reads | `select_random_points` | 25.27ms | 24.54ms | 1.0× | 2.6% | PASS |
| ac_reads | `select_random_ranges` | 8.96ms | 8.66ms | 1.0× | 1.6% | PASS |
| ac_reads | `covering_index_scan` | 10.19ms | 11.91ms | 1.2× | 1.4% | PASS |
| ac_reads | `groupby_scan` | 33.62ms | 35.21ms | 1.0× | 0.9% | PASS |
| ac_reads | `index_join` | 11.74ms | 11.15ms | 1.0× | 1.8% | PASS |
| ac_reads | `index_join_scan` | 4.22ms | 6.06ms | 1.4× | 2.3% | PASS |
| ac_reads | `types_table_scan` | 1.06s | 1.19s | 1.1× | 0.8% | PASS |
| ac_reads | `table_scan` | 1.20s | 1.30s | 1.1× | 1.0% | PASS |
| ac_reads | `oltp_read_only` | 171.92ms | 168.94ms | 1.0× | 1.1% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 24.05ms | 78.45ms | 3.3× | 5.7% | PASS |
| ac_writes | `oltp_insert_ac` | 26.73ms | 96.09ms | 3.6× | 7.1% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.93ms | 108.17ms | 4.0× | 6.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.12ms | 87.02ms | 3.8× | 6.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.08ms | 96.79ms | 3.7× | 7.9% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.42ms | 98.98ms | 3.7× | 7.9% | PASS |
| ac_writes | `types_delete_insert_ac` | 25.45ms | 95.24ms | 3.7× | 8.6% | PASS |
| ac_writes | `oltp_read_write_ac` | 33.38ms | 104.04ms | 3.1× | 8.2% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.96ms | 41.93ms | 1.2× | 1.7% | PASS |
| mem_reads | `oltp_range_select` | 19.15ms | 21.37ms | 1.1× | 2.5% | PASS |
| mem_reads | `oltp_sum_range` | 17.44ms | 21.07ms | 1.2× | 1.3% | PASS |
| mem_reads | `oltp_order_range` | 3.50ms | 3.90ms | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_distinct_range` | 4.66ms | 5.02ms | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_index_scan` | 4.55ms | 6.10ms | 1.3× | 2.2% | PASS |
| mem_reads | `select_random_points` | 28.01ms | 32.76ms | 1.2× | 2.0% | PASS |
| mem_reads | `select_random_ranges` | 7.65ms | 8.93ms | 1.2× | 1.2% | PASS |
| mem_reads | `covering_index_scan` | 7.72ms | 9.34ms | 1.2× | 1.1% | PASS |
| mem_reads | `groupby_scan` | 35.75ms | 39.41ms | 1.1× | 0.9% | PASS |
| mem_reads | `index_join` | 7.93ms | 10.73ms | 1.4× | 2.2% | PASS |
| mem_reads | `index_join_scan` | 3.90ms | 5.64ms | 1.4× | 2.3% | PASS |
| mem_reads | `types_table_scan` | 1.19s | 1.23s | 1.0× | 0.7% | PASS |
| mem_reads | `table_scan` | 1.44s | 1.35s | 0.9× | 0.6% | PASS |
| mem_reads | `oltp_read_only` | 157.59ms | 177.91ms | 1.1× | 1.4% | PASS |
| mem_writes | `oltp_bulk_insert` | 246.62ms | 346.02ms | 1.4× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 19.25ms | 35.64ms | 1.9× | 1.1% | PASS |
| mem_writes | `oltp_update_index` | 66.44ms | 125.34ms | 1.9× | 1.5% | PASS |
| mem_writes | `oltp_update_non_index` | 50.17ms | 79.94ms | 1.6× | 1.1% | PASS |
| mem_writes | `oltp_delete_insert` | 48.34ms | 95.13ms | 2.0× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 26.41ms | 57.04ms | 2.2× | 1.2% | PASS |
| mem_writes | `types_delete_insert` | 33.17ms | 53.68ms | 1.6× | 1.7% | PASS |
| mem_writes | `oltp_read_write` | 102.38ms | 164.98ms | 1.6× | 2.4% | PASS |
| file_reads | `oltp_point_select` | 104.82ms | 61.70ms | 0.6× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 27.14ms | 23.82ms | 0.9× | 2.3% | PASS |
| file_reads | `oltp_sum_range` | 25.46ms | 23.68ms | 0.9× | 1.9% | PASS |
| file_reads | `oltp_order_range` | 4.36ms | 4.21ms | 1.0× | 1.7% | PASS |
| file_reads | `oltp_distinct_range` | 5.55ms | 5.37ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 11.99ms | 8.45ms | 0.7× | 1.4% | PASS |
| file_reads | `select_random_points` | 38.66ms | 37.23ms | 1.0× | 1.8% | PASS |
| file_reads | `select_random_ranges` | 15.55ms | 11.48ms | 0.7× | 1.3% | PASS |
| file_reads | `covering_index_scan` | 15.55ms | 12.07ms | 0.8× | 1.5% | PASS |
| file_reads | `groupby_scan` | 36.25ms | 39.76ms | 1.1× | 1.0% | PASS |
| file_reads | `index_join` | 12.07ms | 12.04ms | 1.0× | 1.7% | PASS |
| file_reads | `index_join_scan` | 4.87ms | 6.10ms | 1.3× | 2.5% | PASS |
| file_reads | `types_table_scan` | 1.10s | 1.19s | 1.1× | 2.8% | PASS |
| file_reads | `table_scan` | 1.22s | 1.31s | 1.1× | 2.7% | PASS |
| file_reads | `oltp_read_only` | 252.29ms | 202.23ms | 0.8× | 1.2% | PASS |
| file_writes | `oltp_bulk_insert` | 262.40ms | 358.12ms | 1.4× | 0.8% | PASS |
| file_writes | `oltp_insert` | 26.30ms | 41.54ms | 1.6× | 2.0% | PASS |
| file_writes | `oltp_update_index` | 98.25ms | 144.86ms | 1.5× | 2.4% | PASS |
| file_writes | `oltp_update_non_index` | 78.52ms | 97.56ms | 1.2× | 2.4% | PASS |
| file_writes | `oltp_delete_insert` | 76.28ms | 111.87ms | 1.5× | 2.0% | PASS |
| file_writes | `oltp_write_only` | 50.70ms | 70.66ms | 1.4× | 2.2% | PASS |
| file_writes | `types_delete_insert` | 49.88ms | 62.35ms | 1.2× | 2.4% | PASS |
| file_writes | `oltp_read_write` | 130.08ms | 178.10ms | 1.4× | 2.1% | PASS |
| ac_reads | `oltp_point_select` | 57.02ms | 60.86ms | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_range_select` | 22.03ms | 23.76ms | 1.1× | 1.8% | PASS |
| ac_reads | `oltp_sum_range` | 20.21ms | 23.63ms | 1.2× | 1.6% | PASS |
| ac_reads | `oltp_order_range` | 3.98ms | 4.25ms | 1.1× | 2.4% | PASS |
| ac_reads | `oltp_distinct_range` | 5.12ms | 5.45ms | 1.1× | 1.6% | PASS |
| ac_reads | `oltp_index_scan` | 7.16ms | 8.36ms | 1.2× | 1.2% | PASS |
| ac_reads | `select_random_points` | 31.19ms | 36.43ms | 1.2× | 1.6% | PASS |
| ac_reads | `select_random_ranges` | 10.16ms | 11.33ms | 1.1× | 1.4% | PASS |
| ac_reads | `covering_index_scan` | 10.60ms | 11.92ms | 1.1× | 1.3% | PASS |
| ac_reads | `groupby_scan` | 35.67ms | 39.75ms | 1.1× | 0.7% | PASS |
| ac_reads | `index_join` | 9.81ms | 12.27ms | 1.3× | 2.6% | PASS |
| ac_reads | `index_join_scan` | 4.36ms | 6.08ms | 1.4× | 2.2% | PASS |
| ac_reads | `types_table_scan` | 1.10s | 1.20s | 1.1× | 2.8% | PASS |
| ac_reads | `table_scan` | 1.37s | 1.33s | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_read_only` | 188.04ms | 205.78ms | 1.1× | 2.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.36ms | 75.82ms | 3.4× | 6.0% | PASS |
| ac_writes | `oltp_insert_ac` | 25.77ms | 93.25ms | 3.6× | 6.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 28.04ms | 106.69ms | 3.8× | 5.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.18ms | 85.91ms | 3.7× | 6.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.79ms | 95.56ms | 3.7× | 5.8% | PASS |
| ac_writes | `oltp_write_only_ac` | 24.79ms | 92.98ms | 3.8× | 6.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 21.66ms | 86.71ms | 4.0× | 4.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.32ms | 103.85ms | 3.2× | 5.7% | PASS |

</details>

</details>

## Version-control latency

Wall time: 3m 54s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 35.24ms | 130.00ms | 27.1% | 0.7% | PASS |
| `status_dirty_many_tables` | 39.02ms | 130.00ms | 30.0% | 0.9% | PASS |
| `diff_regular_working_one_table` | 31.05ms | 120.00ms | 25.9% | 1.1% | PASS |
| `diff_regular_working_many_tables` | 44.80ms | 140.00ms | 32.0% | 1.2% | PASS |
| `diff_stat_working_many_tables` | 44.79ms | 140.00ms | 32.0% | 0.7% | PASS |
| `diff_schema_working_many_tables` | 45.41ms | 140.00ms | 32.4% | 0.7% | PASS |
| `branch_list_many_branches` | 21.83ms | 35.00ms | 62.4% | 0.8% | PASS |
| `branch_create_delete` | 24.38ms | 40.00ms | 61.0% | 1.5% | PASS |
| `at_literal_deep_history` | 26.54ms | 100.00ms | 26.5% | 0.6% | PASS |
| `diff_literal_deep_history` | 26.86ms | 120.00ms | 22.4% | 0.7% | PASS |
| `history_literal_deep_history` | 28.41ms | 150.00ms | 18.9% | 1.4% | PASS |
| `checkout_branch_clean` | 38.82ms | 150.00ms | 25.9% | 1.6% | PASS |
| `merge_data_no_conflicts` | 28.23ms | 50.00ms | 56.5% | 1.0% | PASS |
| `merge_data_secondary_index` | 1.13s | 2.50s | 45.3% | 0.9% | PASS |
| `merge_schema_no_conflicts` | 21.93ms | 35.00ms | 62.7% | 1.0% | PASS |
| `merge_data_conflicts` | 30.50ms | 180.00ms | 16.9% | 1.2% | PASS |
| `merge_data_conflicts_with_resolve` | 31.51ms | 180.00ms | 17.5% | 0.8% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
