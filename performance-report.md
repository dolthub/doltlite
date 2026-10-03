# DoltLite Performance Report

> Nightly result: **FAIL**
>
> Generated: 2026-10-03 11:06 UTC
>
> Commit: [`f297bf785a4b7d8c45fd16b0ebedffda7822f717`](https://github.com/dolthub/doltlite/commit/f297bf785a4b7d8c45fd16b0ebedffda7822f717)
>
> Runner: ubuntu24 20260927.320.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/37113604704)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.81s | 11.15s | 1.1× | 1.6% | **PASS** |
| Writes | 2.02s | 3.21s | 1.6× | 1.5% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.48s | 11.36s | 1.1× | 1.7% | **PASS** |
| Writes | 3.24s | 3.90s | 1.2× | 2.1% | **FAIL** |
| Autocommit writes | 902.17ms | 2.67s | 3.0× | 5.7% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.49s | 2.77s | 1.1× | 1.4% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 1.94s | 2.23s | 1.1× | 2.6% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.51s | 3.00s | 1.2× | 1.5% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.87s | 3.15s | 1.1× | 1.4% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 429.69ms | 713.84ms | 1.7× | 1.2% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 402.38ms | 617.17ms | 1.5× | 2.8% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 583.81ms | 932.87ms | 1.6× | 1.4% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 604.35ms | 943.04ms | 1.6× | 1.5% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.62s | 2.82s | 1.1× | 1.2% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.06s | 2.28s | 1.1× | 3.2% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.79s | 3.09s | 1.1× | 1.6% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 3.01s | 3.18s | 1.1× | 1.7% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 585.79ms | 788.17ms | 1.3× | 1.8% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.07s | 1.06s | 1.0× | 4.7% | **FAIL** |
| File-backed | Writes | blobpk | 8 | 55 | 804.29ms | 1.03s | 1.3× | 1.6% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 780.21ms | 1.02s | 1.3× | 2.1% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.51s | 2.82s | 1.1× | 1.1% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 1.99s | 2.26s | 1.1× | 5.0% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.79s | 3.15s | 1.1× | 1.4% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.92s | 3.19s | 1.1× | 1.4% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 194.95ms | 613.78ms | 3.1× | 5.7% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 285.35ms | 778.29ms | 2.7× | 5.5% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 209.20ms | 636.43ms | 3.0× | 5.7% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 212.68ms | 641.91ms | 3.0× | 7.5% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 22.98ms | 31.04ms | 1.4× | 1.5% | PASS |
| mem_reads | `oltp_range_select` | 9.96ms | 11.86ms | 1.2× | 2.6% | PASS |
| mem_reads | `oltp_sum_range` | 9.11ms | 11.84ms | 1.3× | 1.7% | PASS |
| mem_reads | `oltp_order_range` | 2.50ms | 2.93ms | 1.2× | 0.8% | PASS |
| mem_reads | `oltp_distinct_range` | 3.60ms | 4.15ms | 1.2× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 3.85ms | 5.53ms | 1.4× | 1.4% | PASS |
| mem_reads | `select_random_points` | 9.89ms | 11.90ms | 1.2× | 2.3% | PASS |
| mem_reads | `select_random_ranges` | 4.51ms | 5.30ms | 1.2× | 1.7% | PASS |
| mem_reads | `covering_index_scan` | 7.56ms | 10.59ms | 1.4× | 0.9% | PASS |
| mem_reads | `groupby_scan` | 29.33ms | 32.82ms | 1.1× | 0.6% | PASS |
| mem_reads | `index_join` | 5.72ms | 8.72ms | 1.5× | 1.1% | PASS |
| mem_reads | `index_join_scan` | 3.21ms | 5.64ms | 1.8× | 2.3% | PASS |
| mem_reads | `types_table_scan` | 1.03s | 1.20s | 1.2× | 0.8% | PASS |
| mem_reads | `table_scan` | 1.24s | 1.31s | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_read_only` | 106.47ms | 126.63ms | 1.2× | 0.9% | PASS |
| mem_writes | `oltp_bulk_insert` | 178.03ms | 280.34ms | 1.6× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 15.07ms | 27.46ms | 1.8× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 48.52ms | 89.41ms | 1.8× | 0.9% | PASS |
| mem_writes | `oltp_update_non_index` | 33.17ms | 56.23ms | 1.7× | 1.3% | PASS |
| mem_writes | `oltp_delete_insert` | 44.43ms | 73.48ms | 1.7× | 1.2% | PASS |
| mem_writes | `oltp_write_only` | 21.28ms | 43.45ms | 2.0× | 1.7% | PASS |
| mem_writes | `types_delete_insert` | 24.47ms | 36.79ms | 1.5× | 1.7% | PASS |
| mem_writes | `oltp_read_write` | 64.72ms | 106.69ms | 1.6× | 1.3% | PASS |
| file_reads | `oltp_point_select` | 92.19ms | 49.91ms | 0.5× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 16.68ms | 13.91ms | 0.8× | 1.5% | PASS |
| file_reads | `oltp_sum_range` | 16.32ms | 14.15ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_order_range` | 3.28ms | 3.21ms | 1.0× | 1.7% | PASS |
| file_reads | `oltp_distinct_range` | 4.33ms | 4.37ms | 1.0× | 1.2% | PASS |
| file_reads | `oltp_index_scan` | 10.96ms | 7.62ms | 0.7× | 1.2% | PASS |
| file_reads | `select_random_points` | 17.04ms | 14.10ms | 0.8× | 2.7% | PASS |
| file_reads | `select_random_ranges` | 11.72ms | 7.42ms | 0.6× | 1.7% | PASS |
| file_reads | `covering_index_scan` | 14.85ms | 12.66ms | 0.9× | 0.8% | PASS |
| file_reads | `groupby_scan` | 30.07ms | 33.13ms | 1.1× | 0.7% | PASS |
| file_reads | `index_join` | 9.69ms | 10.12ms | 1.0× | 0.9% | PASS |
| file_reads | `index_join_scan` | 4.30ms | 6.03ms | 1.4× | 2.5% | PASS |
| file_reads | `types_table_scan` | 1.03s | 1.19s | 1.2× | 0.7% | PASS |
| file_reads | `table_scan` | 1.15s | 1.29s | 1.1× | 0.5% | PASS |
| file_reads | `oltp_read_only` | 208.21ms | 154.33ms | 0.7× | 1.1% | PASS |
| file_writes | `oltp_bulk_insert` | 193.74ms | 291.16ms | 1.5× | 1.4% | PASS |
| file_writes | `oltp_insert` | 21.91ms | 30.94ms | 1.4× | 1.6% | PASS |
| file_writes | `oltp_update_index` | 78.91ms | 107.15ms | 1.4× | 2.2% | PASS |
| file_writes | `oltp_update_non_index` | 57.14ms | 66.06ms | 1.2× | 2.7% | PASS |
| file_writes | `oltp_delete_insert` | 65.39ms | 83.09ms | 1.3× | 2.1% | PASS |
| file_writes | `oltp_write_only` | 42.42ms | 52.04ms | 1.2× | 1.5% | PASS |
| file_writes | `types_delete_insert` | 39.02ms | 42.84ms | 1.1× | 2.0% | PASS |
| file_writes | `oltp_read_write` | 87.27ms | 114.89ms | 1.3× | 1.1% | PASS |
| ac_reads | `oltp_point_select` | 46.31ms | 49.93ms | 1.1× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 12.38ms | 13.90ms | 1.1× | 1.6% | PASS |
| ac_reads | `oltp_sum_range` | 11.65ms | 14.01ms | 1.2× | 1.3% | PASS |
| ac_reads | `oltp_order_range` | 2.81ms | 3.20ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_distinct_range` | 3.87ms | 4.40ms | 1.1× | 1.0% | PASS |
| ac_reads | `oltp_index_scan` | 6.18ms | 7.59ms | 1.2× | 1.1% | PASS |
| ac_reads | `select_random_points` | 11.97ms | 14.11ms | 1.2× | 1.4% | PASS |
| ac_reads | `select_random_ranges` | 6.90ms | 7.35ms | 1.1× | 1.9% | PASS |
| ac_reads | `covering_index_scan` | 10.07ms | 12.68ms | 1.3× | 1.1% | PASS |
| ac_reads | `groupby_scan` | 29.37ms | 33.00ms | 1.1× | 0.6% | PASS |
| ac_reads | `index_join` | 7.11ms | 10.03ms | 1.4× | 1.5% | PASS |
| ac_reads | `index_join_scan` | 3.60ms | 5.96ms | 1.7× | 2.0% | PASS |
| ac_reads | `types_table_scan` | 1.05s | 1.20s | 1.1× | 0.7% | PASS |
| ac_reads | `table_scan` | 1.17s | 1.30s | 1.1× | 0.9% | PASS |
| ac_reads | `oltp_read_only` | 136.24ms | 153.17ms | 1.1× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.43ms | 62.86ms | 2.8× | 7.0% | PASS |
| ac_writes | `oltp_insert_ac` | 24.11ms | 76.28ms | 3.2× | 6.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.18ms | 88.86ms | 3.3× | 5.8% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.24ms | 70.81ms | 3.2× | 5.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.92ms | 79.83ms | 3.2× | 7.4% | PASS |
| ac_writes | `oltp_write_only_ac` | 24.03ms | 79.34ms | 3.3× | 5.7% | PASS |
| ac_writes | `types_delete_insert_ac` | 21.79ms | 71.79ms | 3.3× | 5.6% | PASS |
| ac_writes | `oltp_read_write_ac` | 28.25ms | 84.01ms | 3.0× | 5.1% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.37ms | 25.14ms | 1.0× | 2.0% | PASS |
| mem_reads | `oltp_range_select` | 12.29ms | 10.77ms | 0.9× | 2.4% | PASS |
| mem_reads | `oltp_sum_range` | 12.45ms | 10.55ms | 0.8× | 2.9% | PASS |
| mem_reads | `oltp_order_range` | 2.40ms | 2.40ms | 1.0× | 2.7% | PASS |
| mem_reads | `oltp_distinct_range` | 3.01ms | 3.27ms | 1.1× | 2.3% | PASS |
| mem_reads | `oltp_index_scan` | 2.79ms | 4.58ms | 1.6× | 2.6% | PASS |
| mem_reads | `select_random_points` | 17.42ms | 17.60ms | 1.0× | 2.2% | PASS |
| mem_reads | `select_random_ranges` | 4.44ms | 4.28ms | 1.0× | 4.2% | PASS |
| mem_reads | `covering_index_scan` | 4.54ms | 6.30ms | 1.4× | 2.6% | PASS |
| mem_reads | `groupby_scan` | 22.52ms | 25.69ms | 1.1× | 2.9% | PASS |
| mem_reads | `index_join` | 9.28ms | 7.22ms | 0.8× | 3.1% | PASS |
| mem_reads | `index_join_scan` | 2.64ms | 5.56ms | 2.1× | 4.1% | PASS |
| mem_reads | `types_table_scan` | 801.83ms | 949.11ms | 1.2× | 1.4% | PASS |
| mem_reads | `table_scan` | 926.68ms | 1.05s | 1.1× | 2.0% | PASS |
| mem_reads | `oltp_read_only` | 94.59ms | 102.19ms | 1.1× | 3.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 151.03ms | 208.43ms | 1.4× | 2.2% | PASS |
| mem_writes | `oltp_insert` | 12.38ms | 25.05ms | 2.0× | 1.9% | PASS |
| mem_writes | `oltp_update_index` | 48.88ms | 96.34ms | 2.0× | 3.9% | PASS |
| mem_writes | `oltp_update_non_index` | 34.99ms | 54.77ms | 1.6× | 2.3% | PASS |
| mem_writes | `oltp_delete_insert` | 40.62ms | 70.10ms | 1.7× | 3.8% | PASS |
| mem_writes | `oltp_write_only` | 20.67ms | 36.77ms | 1.8× | 2.6% | PASS |
| mem_writes | `types_delete_insert` | 28.37ms | 35.52ms | 1.3× | 4.0% | PASS |
| mem_writes | `oltp_read_write` | 65.44ms | 90.17ms | 1.4× | 3.1% | PASS |
| file_reads | `oltp_point_select` | 48.09ms | 32.31ms | 0.7× | 3.9% | PASS |
| file_reads | `oltp_range_select` | 14.47ms | 11.93ms | 0.8× | 5.0% | PASS |
| file_reads | `oltp_sum_range` | 15.53ms | 12.23ms | 0.8× | 4.7% | PASS |
| file_reads | `oltp_order_range` | 2.89ms | 2.76ms | 1.0× | 3.2% | PASS |
| file_reads | `oltp_distinct_range` | 3.65ms | 3.60ms | 1.0× | 2.8% | PASS |
| file_reads | `oltp_index_scan` | 5.61ms | 5.84ms | 1.0× | 5.7% | PASS |
| file_reads | `select_random_points` | 22.76ms | 20.34ms | 0.9× | 2.4% | PASS |
| file_reads | `select_random_ranges` | 7.76ms | 5.77ms | 0.7× | 2.5% | PASS |
| file_reads | `covering_index_scan` | 7.87ms | 8.07ms | 1.0× | 4.5% | PASS |
| file_reads | `groupby_scan` | 24.84ms | 27.68ms | 1.1× | 3.8% | PASS |
| file_reads | `index_join` | 12.18ms | 9.33ms | 0.8× | 3.1% | PASS |
| file_reads | `index_join_scan` | 3.37ms | 6.39ms | 1.9× | 4.1% | PASS |
| file_reads | `types_table_scan` | 810.22ms | 955.46ms | 1.2× | 1.3% | PASS |
| file_reads | `table_scan` | 946.76ms | 1.07s | 1.1× | 2.1% | PASS |
| file_reads | `oltp_read_only` | 134.69ms | 112.07ms | 0.8× | 2.6% | PASS |
| file_writes | `oltp_bulk_insert` | 221.19ms | 275.10ms | 1.2× | 4.2% | PASS |
| file_writes | `oltp_insert` | 20.00ms | 47.77ms | 2.4× | 4.8% | FAIL |
| file_writes | `oltp_update_index` | 153.99ms | 175.06ms | 1.1× | 5.1% | PASS |
| file_writes | `oltp_update_non_index` | 130.95ms | 110.52ms | 0.8× | 3.3% | PASS |
| file_writes | `oltp_delete_insert` | 159.09ms | 136.37ms | 0.9× | 3.7% | PASS |
| file_writes | `oltp_write_only` | 106.33ms | 89.52ms | 0.8× | 4.6% | PASS |
| file_writes | `types_delete_insert` | 123.26ms | 79.45ms | 0.6× | 8.5% | PASS |
| file_writes | `oltp_read_write` | 152.02ms | 147.06ms | 1.0× | 5.1% | PASS |
| ac_reads | `oltp_point_select` | 32.77ms | 32.23ms | 1.0× | 4.0% | PASS |
| ac_reads | `oltp_range_select` | 13.60ms | 11.79ms | 0.9× | 5.8% | PASS |
| ac_reads | `oltp_sum_range` | 13.65ms | 11.96ms | 0.9× | 6.0% | PASS |
| ac_reads | `oltp_order_range` | 2.66ms | 2.68ms | 1.0× | 5.9% | PASS |
| ac_reads | `oltp_distinct_range` | 3.37ms | 3.50ms | 1.0× | 6.6% | PASS |
| ac_reads | `oltp_index_scan` | 3.82ms | 5.59ms | 1.5× | 5.3% | PASS |
| ac_reads | `select_random_points` | 19.30ms | 19.59ms | 1.0× | 5.0% | PASS |
| ac_reads | `select_random_ranges` | 5.85ms | 5.54ms | 0.9× | 4.3% | PASS |
| ac_reads | `covering_index_scan` | 5.76ms | 7.86ms | 1.4× | 7.4% | PASS |
| ac_reads | `groupby_scan` | 23.35ms | 27.12ms | 1.2× | 8.4% | PASS |
| ac_reads | `index_join` | 10.51ms | 8.53ms | 0.8× | 3.1% | PASS |
| ac_reads | `index_join_scan` | 2.99ms | 6.09ms | 2.0× | 4.2% | PASS |
| ac_reads | `types_table_scan` | 819.72ms | 963.59ms | 1.2× | 2.1% | PASS |
| ac_reads | `table_scan` | 932.20ms | 1.04s | 1.1× | 2.5% | PASS |
| ac_reads | `oltp_read_only` | 103.69ms | 107.32ms | 1.0× | 3.6% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 34.23ms | 87.09ms | 2.5× | 4.6% | PASS |
| ac_writes | `oltp_insert_ac` | 35.93ms | 95.37ms | 2.7× | 5.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 37.16ms | 107.95ms | 2.9× | 5.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 34.04ms | 92.54ms | 2.7× | 6.9% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 36.27ms | 101.32ms | 2.8× | 4.4% | PASS |
| ac_writes | `oltp_write_only_ac` | 34.96ms | 99.63ms | 2.8× | 5.5% | PASS |
| ac_writes | `types_delete_insert_ac` | 34.52ms | 93.44ms | 2.7× | 6.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 38.22ms | 100.95ms | 2.6× | 2.7% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.87ms | 39.99ms | 1.2× | 1.5% | PASS |
| mem_reads | `oltp_range_select` | 15.43ms | 15.20ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_sum_range` | 14.18ms | 15.11ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_order_range` | 3.20ms | 3.40ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 4.28ms | 4.50ms | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_index_scan` | 3.88ms | 7.05ms | 1.8× | 1.6% | PASS |
| mem_reads | `select_random_points` | 20.36ms | 22.83ms | 1.1× | 1.9% | PASS |
| mem_reads | `select_random_ranges` | 6.23ms | 6.90ms | 1.1× | 1.8% | PASS |
| mem_reads | `covering_index_scan` | 7.65ms | 10.93ms | 1.4× | 0.9% | PASS |
| mem_reads | `groupby_scan` | 33.27ms | 35.13ms | 1.1× | 0.8% | PASS |
| mem_reads | `index_join` | 10.03ms | 10.81ms | 1.1× | 1.9% | PASS |
| mem_reads | `index_join_scan` | 3.65ms | 6.58ms | 1.8× | 1.6% | PASS |
| mem_reads | `types_table_scan` | 1.05s | 1.30s | 1.2× | 0.8% | PASS |
| mem_reads | `table_scan` | 1.17s | 1.38s | 1.2× | 0.9% | PASS |
| mem_reads | `oltp_read_only` | 133.05ms | 145.47ms | 1.1× | 1.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 239.61ms | 335.46ms | 1.4× | 1.1% | PASS |
| mem_writes | `oltp_insert` | 18.27ms | 37.87ms | 2.1× | 1.1% | PASS |
| mem_writes | `oltp_update_index` | 62.86ms | 129.75ms | 2.1× | 1.3% | PASS |
| mem_writes | `oltp_update_non_index` | 47.62ms | 79.81ms | 1.7× | 2.2% | PASS |
| mem_writes | `oltp_delete_insert` | 51.74ms | 97.99ms | 1.9× | 1.3% | PASS |
| mem_writes | `oltp_write_only` | 27.73ms | 56.69ms | 2.0× | 1.5% | PASS |
| mem_writes | `types_delete_insert` | 37.67ms | 55.17ms | 1.5× | 1.7% | PASS |
| mem_writes | `oltp_read_write` | 98.31ms | 140.12ms | 1.4× | 1.9% | PASS |
| file_reads | `oltp_point_select` | 105.56ms | 59.73ms | 0.6× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 22.87ms | 17.30ms | 0.8× | 1.8% | PASS |
| file_reads | `oltp_sum_range` | 21.85ms | 17.34ms | 0.8× | 2.1% | PASS |
| file_reads | `oltp_order_range` | 4.16ms | 3.66ms | 0.9× | 1.9% | PASS |
| file_reads | `oltp_distinct_range` | 5.18ms | 4.76ms | 0.9× | 0.8% | PASS |
| file_reads | `oltp_index_scan` | 11.08ms | 9.12ms | 0.8× | 1.2% | PASS |
| file_reads | `select_random_points` | 29.70ms | 25.80ms | 0.9× | 2.6% | PASS |
| file_reads | `select_random_ranges` | 13.79ms | 9.04ms | 0.7× | 1.7% | PASS |
| file_reads | `covering_index_scan` | 14.97ms | 13.13ms | 0.9× | 0.6% | PASS |
| file_reads | `groupby_scan` | 34.20ms | 35.42ms | 1.0× | 0.6% | PASS |
| file_reads | `index_join` | 14.23ms | 12.00ms | 0.8× | 2.7% | PASS |
| file_reads | `index_join_scan` | 4.69ms | 6.85ms | 1.5× | 1.6% | PASS |
| file_reads | `types_table_scan` | 1.05s | 1.30s | 1.2× | 0.6% | PASS |
| file_reads | `table_scan` | 1.22s | 1.40s | 1.1× | 1.7% | PASS |
| file_reads | `oltp_read_only` | 242.89ms | 174.99ms | 0.7× | 1.2% | PASS |
| file_writes | `oltp_bulk_insert` | 261.90ms | 350.02ms | 1.3× | 0.9% | PASS |
| file_writes | `oltp_insert` | 25.14ms | 42.99ms | 1.7× | 1.3% | PASS |
| file_writes | `oltp_update_index` | 96.86ms | 148.33ms | 1.5× | 1.8% | PASS |
| file_writes | `oltp_update_non_index` | 90.64ms | 92.40ms | 1.0× | 10.4% | PASS |
| file_writes | `oltp_delete_insert` | 84.78ms | 112.07ms | 1.3× | 1.4% | PASS |
| file_writes | `oltp_write_only` | 55.98ms | 68.73ms | 1.2× | 1.9% | PASS |
| file_writes | `types_delete_insert` | 62.48ms | 65.45ms | 1.0× | 2.5% | PASS |
| file_writes | `oltp_read_write` | 126.51ms | 151.19ms | 1.2× | 1.5% | PASS |
| ac_reads | `oltp_point_select` | 58.46ms | 59.57ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_range_select` | 18.60ms | 17.32ms | 0.9× | 2.4% | PASS |
| ac_reads | `oltp_sum_range` | 17.59ms | 17.39ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_order_range` | 3.79ms | 3.67ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_distinct_range` | 4.80ms | 4.78ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_index_scan` | 6.68ms | 9.21ms | 1.4× | 1.1% | PASS |
| ac_reads | `select_random_points` | 25.95ms | 26.27ms | 1.0× | 2.3% | PASS |
| ac_reads | `select_random_ranges` | 9.31ms | 9.07ms | 1.0× | 1.4% | PASS |
| ac_reads | `covering_index_scan` | 10.33ms | 13.20ms | 1.3× | 1.5% | PASS |
| ac_reads | `groupby_scan` | 33.75ms | 35.49ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 11.98ms | 12.04ms | 1.0× | 2.8% | PASS |
| ac_reads | `index_join_scan` | 4.22ms | 6.85ms | 1.6× | 1.8% | PASS |
| ac_reads | `types_table_scan` | 1.13s | 1.34s | 1.2× | 1.7% | PASS |
| ac_reads | `table_scan` | 1.28s | 1.42s | 1.1× | 0.6% | PASS |
| ac_reads | `oltp_read_only` | 177.78ms | 177.15ms | 1.0× | 1.3% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 23.04ms | 63.30ms | 2.7× | 4.6% | PASS |
| ac_writes | `oltp_insert_ac` | 24.99ms | 79.23ms | 3.2× | 6.7% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.65ms | 92.47ms | 3.5× | 5.8% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.73ms | 72.81ms | 3.1× | 5.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.67ms | 82.44ms | 3.1× | 5.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.87ms | 81.69ms | 3.0× | 7.8% | PASS |
| ac_writes | `types_delete_insert_ac` | 24.06ms | 75.35ms | 3.1× | 10.1% | PASS |
| ac_writes | `oltp_read_write_ac` | 33.18ms | 89.14ms | 2.7× | 5.6% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 35.94ms | 43.90ms | 1.2× | 1.4% | PASS |
| mem_reads | `oltp_range_select` | 20.62ms | 23.58ms | 1.1× | 1.8% | PASS |
| mem_reads | `oltp_sum_range` | 18.51ms | 22.65ms | 1.2× | 1.6% | PASS |
| mem_reads | `oltp_order_range` | 3.62ms | 4.25ms | 1.2× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 4.74ms | 5.42ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 4.69ms | 6.76ms | 1.4× | 1.5% | PASS |
| mem_reads | `select_random_points` | 29.21ms | 35.57ms | 1.2× | 1.6% | PASS |
| mem_reads | `select_random_ranges` | 7.76ms | 9.52ms | 1.2× | 1.3% | PASS |
| mem_reads | `covering_index_scan` | 7.70ms | 11.04ms | 1.4× | 1.4% | PASS |
| mem_reads | `groupby_scan` | 36.11ms | 41.78ms | 1.2× | 0.8% | PASS |
| mem_reads | `index_join` | 8.19ms | 12.65ms | 1.5× | 2.4% | PASS |
| mem_reads | `index_join_scan` | 3.98ms | 6.87ms | 1.7× | 3.3% | PASS |
| mem_reads | `types_table_scan` | 1.16s | 1.31s | 1.1× | 0.4% | PASS |
| mem_reads | `table_scan` | 1.37s | 1.42s | 1.0× | 0.7% | PASS |
| mem_reads | `oltp_read_only` | 159.08ms | 188.39ms | 1.2× | 1.2% | PASS |
| mem_writes | `oltp_bulk_insert` | 243.40ms | 330.02ms | 1.4× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 18.91ms | 34.33ms | 1.8× | 1.5% | PASS |
| mem_writes | `oltp_update_index` | 71.97ms | 130.84ms | 1.8× | 2.1% | PASS |
| mem_writes | `oltp_update_non_index` | 53.35ms | 84.11ms | 1.6× | 1.1% | PASS |
| mem_writes | `oltp_delete_insert` | 50.93ms | 94.98ms | 1.9× | 1.5% | PASS |
| mem_writes | `oltp_write_only` | 27.35ms | 55.60ms | 2.0× | 1.5% | PASS |
| mem_writes | `types_delete_insert` | 32.90ms | 54.42ms | 1.7× | 1.6% | PASS |
| mem_writes | `oltp_read_write` | 105.55ms | 158.74ms | 1.5× | 1.6% | PASS |
| file_reads | `oltp_point_select` | 104.57ms | 61.60ms | 0.6× | 1.1% | PASS |
| file_reads | `oltp_range_select` | 26.84ms | 25.41ms | 0.9× | 1.9% | PASS |
| file_reads | `oltp_sum_range` | 25.37ms | 24.81ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_order_range` | 4.49ms | 4.54ms | 1.0× | 2.5% | PASS |
| file_reads | `oltp_distinct_range` | 5.67ms | 5.72ms | 1.0× | 2.2% | PASS |
| file_reads | `oltp_index_scan` | 11.88ms | 8.77ms | 0.7× | 1.7% | PASS |
| file_reads | `select_random_points` | 38.24ms | 38.74ms | 1.0× | 1.8% | PASS |
| file_reads | `select_random_ranges` | 15.19ms | 11.60ms | 0.8× | 1.6% | PASS |
| file_reads | `covering_index_scan` | 15.10ms | 13.24ms | 0.9× | 0.9% | PASS |
| file_reads | `groupby_scan` | 36.84ms | 42.06ms | 1.1× | 1.0% | PASS |
| file_reads | `index_join` | 11.94ms | 13.40ms | 1.1× | 1.7% | PASS |
| file_reads | `index_join_scan` | 4.83ms | 6.86ms | 1.4× | 2.6% | PASS |
| file_reads | `types_table_scan` | 1.09s | 1.30s | 1.2× | 1.7% | PASS |
| file_reads | `table_scan` | 1.35s | 1.41s | 1.0× | 1.5% | PASS |
| file_reads | `oltp_read_only` | 262.95ms | 215.74ms | 0.8× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 259.52ms | 341.56ms | 1.3× | 0.7% | PASS |
| file_writes | `oltp_insert` | 26.64ms | 38.37ms | 1.4× | 2.0% | PASS |
| file_writes | `oltp_update_index` | 100.83ms | 141.19ms | 1.4× | 2.7% | PASS |
| file_writes | `oltp_update_non_index` | 79.37ms | 94.13ms | 1.2× | 2.1% | PASS |
| file_writes | `oltp_delete_insert` | 77.96ms | 106.96ms | 1.4× | 2.3% | PASS |
| file_writes | `oltp_write_only` | 52.56ms | 65.35ms | 1.2× | 2.3% | PASS |
| file_writes | `types_delete_insert` | 50.37ms | 63.17ms | 1.3× | 1.8% | PASS |
| file_writes | `oltp_read_write` | 132.96ms | 169.29ms | 1.3× | 1.8% | PASS |
| ac_reads | `oltp_point_select` | 57.63ms | 61.33ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_range_select` | 22.28ms | 25.50ms | 1.1× | 1.6% | PASS |
| ac_reads | `oltp_sum_range` | 20.52ms | 24.70ms | 1.2× | 1.0% | PASS |
| ac_reads | `oltp_order_range` | 4.14ms | 4.54ms | 1.1× | 2.9% | PASS |
| ac_reads | `oltp_distinct_range` | 5.21ms | 5.72ms | 1.1× | 2.3% | PASS |
| ac_reads | `oltp_index_scan` | 7.29ms | 8.75ms | 1.2× | 1.5% | PASS |
| ac_reads | `select_random_points` | 32.13ms | 38.77ms | 1.2× | 1.6% | PASS |
| ac_reads | `select_random_ranges` | 10.38ms | 11.64ms | 1.1× | 1.4% | PASS |
| ac_reads | `covering_index_scan` | 10.58ms | 13.22ms | 1.2× | 1.8% | PASS |
| ac_reads | `groupby_scan` | 36.33ms | 42.10ms | 1.2× | 0.7% | PASS |
| ac_reads | `index_join` | 9.70ms | 13.54ms | 1.4× | 0.9% | PASS |
| ac_reads | `index_join_scan` | 4.37ms | 7.00ms | 1.6× | 2.6% | PASS |
| ac_reads | `types_table_scan` | 1.13s | 1.31s | 1.2× | 0.9% | PASS |
| ac_reads | `table_scan` | 1.38s | 1.41s | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_read_only` | 192.60ms | 216.39ms | 1.1× | 1.1% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 23.20ms | 65.08ms | 2.8× | 7.5% | PASS |
| ac_writes | `oltp_insert_ac` | 25.94ms | 80.25ms | 3.1× | 7.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 29.70ms | 93.31ms | 3.1× | 5.4% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 25.34ms | 74.29ms | 2.9× | 8.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.46ms | 82.66ms | 3.2× | 7.5% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.97ms | 82.50ms | 3.1× | 7.4% | PASS |
| ac_writes | `types_delete_insert_ac` | 23.32ms | 73.84ms | 3.2× | 9.0% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.75ms | 89.97ms | 2.7× | 4.9% | PASS |

</details>

</details>

## Version-control latency

Wall time: 3m 28s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 36.28ms | 38.00ms | 95.5% | 1.0% | PASS |
| `status_dirty_many_tables` | 40.26ms | 42.00ms | 95.9% | 1.5% | PASS |
| `diff_regular_working_one_table` | 30.94ms | 33.00ms | 93.8% | 1.2% | PASS |
| `diff_regular_working_many_tables` | 44.59ms | 50.00ms | 89.2% | 1.3% | PASS |
| `diff_stat_working_many_tables` | 44.79ms | 48.00ms | 93.3% | 1.3% | PASS |
| `diff_schema_working_many_tables` | 44.83ms | 48.00ms | 93.4% | 0.9% | PASS |
| `branch_list_many_branches` | 23.14ms | 25.00ms | 92.6% | 1.7% | PASS |
| `branch_create_delete` | 25.26ms | 27.00ms | 93.5% | 1.8% | PASS |
| `at_literal_deep_history` | 25.73ms | 28.00ms | 91.9% | 1.3% | PASS |
| `diff_literal_deep_history` | 25.84ms | 28.00ms | 92.3% | 1.5% | PASS |
| `history_literal_deep_history` | 27.05ms | 30.00ms | 90.2% | 1.1% | PASS |
| `checkout_branch_clean` | 38.78ms | 43.00ms | 90.2% | 1.3% | PASS |
| `merge_data_no_conflicts` | 29.53ms | 33.00ms | 89.5% | 1.1% | PASS |
| `merge_data_secondary_index` | 868.26ms | 944.00ms | 92.0% | 0.7% | PASS |
| `merge_schema_no_conflicts` | 23.18ms | 24.00ms | 96.6% | 1.3% | PASS |
| `merge_data_conflicts` | 31.53ms | 33.00ms | 95.5% | 0.9% | PASS |
| `merge_data_conflicts_with_resolve` | 31.45ms | 34.00ms | 92.5% | 1.3% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
