# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-11-03 11:31 UTC
>
> Commit: [`f3b36a8c3fd42f45948ef8ef2ab43bd737565ecc`](https://github.com/dolthub/doltlite/commit/f3b36a8c3fd42f45948ef8ef2ab43bd737565ecc)
>
> Runner: ubuntu24 20261026.354.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/41611771861)

The SQL workloads compare optimized DoltLite against stock SQLite on the same GitHub-hosted runner, alternating execution order on each repetition. Version-control sections measure DoltLite latency. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.66s | 11.34s | 1.2× | 1.6% | **PASS** |
| Writes | 2.04s | 3.47s | 1.7× | 1.2% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.59s | 11.62s | 1.1× | 1.5% | **PASS** |
| Writes | 3.33s | 4.07s | 1.2× | 2.0% | **PASS** |
| Autocommit writes | 744.13ms | 2.32s | 3.1× | 5.8% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.54s | 2.92s | 1.1× | 1.2% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.89s | 3.27s | 1.1× | 1.8% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.49s | 3.17s | 1.3× | 1.3% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 1.73s | 1.98s | 1.1× | 3.1% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 445.40ms | 779.65ms | 1.8× | 0.7% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 630.41ms | 1.05s | 1.7× | 1.8% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 586.95ms | 1.01s | 1.7× | 1.1% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 379.19ms | 620.23ms | 1.6× | 2.1% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.78s | 2.98s | 1.1× | 0.7% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 3.14s | 3.34s | 1.1× | 1.5% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.71s | 3.23s | 1.2× | 1.5% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 1.96s | 2.07s | 1.1× | 1.8% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 610.23ms | 844.91ms | 1.4× | 1.4% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 995.07ms | 1.17s | 1.2× | 3.5% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 822.80ms | 1.12s | 1.4× | 1.9% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 899.30ms | 938.99ms | 1.0× | 3.3% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.70s | 3.01s | 1.1× | 1.5% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.83s | 3.29s | 1.2× | 1.7% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.57s | 3.23s | 1.3× | 1.2% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.03s | 2.11s | 1.0× | 2.0% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 146.49ms | 538.83ms | 3.7× | 4.8% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 206.87ms | 647.82ms | 3.1× | 8.1% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 187.61ms | 616.46ms | 3.3× | 5.9% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 203.16ms | 518.77ms | 2.6× | 5.7% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.29ms | 29.77ms | 1.2× | 1.1% | PASS |
| mem_reads | `oltp_range_select` | 10.54ms | 12.38ms | 1.2× | 1.3% | PASS |
| mem_reads | `oltp_sum_range` | 9.57ms | 11.89ms | 1.2× | 1.0% | PASS |
| mem_reads | `oltp_order_range` | 2.74ms | 3.09ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 3.82ms | 4.19ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 3.96ms | 5.27ms | 1.3× | 1.7% | PASS |
| mem_reads | `select_random_points` | 13.64ms | 12.28ms | 0.9× | 2.7% | PASS |
| mem_reads | `select_random_ranges` | 4.74ms | 5.04ms | 1.1× | 2.1% | PASS |
| mem_reads | `covering_index_scan` | 7.61ms | 10.13ms | 1.3× | 1.5% | PASS |
| mem_reads | `groupby_scan` | 32.41ms | 35.59ms | 1.1× | 0.8% | PASS |
| mem_reads | `index_join` | 5.74ms | 8.69ms | 1.5× | 2.8% | PASS |
| mem_reads | `index_join_scan` | 3.34ms | 6.25ms | 1.9× | 3.0% | PASS |
| mem_reads | `types_table_scan` | 1.09s | 1.27s | 1.2× | 0.8% | PASS |
| mem_reads | `table_scan` | 1.22s | 1.38s | 1.1× | 0.5% | PASS |
| mem_reads | `oltp_read_only` | 104.46ms | 122.64ms | 1.2× | 1.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 183.60ms | 303.49ms | 1.7× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 15.76ms | 27.34ms | 1.7× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 51.50ms | 103.09ms | 2.0× | 0.6% | PASS |
| mem_writes | `oltp_update_non_index` | 36.43ms | 66.08ms | 1.8× | 0.7% | PASS |
| mem_writes | `oltp_delete_insert` | 45.38ms | 81.16ms | 1.8× | 1.1% | PASS |
| mem_writes | `oltp_write_only` | 22.67ms | 51.30ms | 2.3× | 0.7% | PASS |
| mem_writes | `types_delete_insert` | 25.05ms | 37.80ms | 1.5× | 0.7% | PASS |
| mem_writes | `oltp_read_write` | 65.01ms | 109.39ms | 1.7× | 0.7% | PASS |
| file_reads | `oltp_point_select` | 102.03ms | 50.39ms | 0.5× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 18.74ms | 14.48ms | 0.8× | 0.6% | PASS |
| file_reads | `oltp_sum_range` | 17.67ms | 14.10ms | 0.8× | 0.6% | PASS |
| file_reads | `oltp_order_range` | 3.65ms | 3.34ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_distinct_range` | 4.73ms | 4.50ms | 1.0× | 0.6% | PASS |
| file_reads | `oltp_index_scan` | 12.08ms | 7.62ms | 0.6× | 0.9% | PASS |
| file_reads | `select_random_points` | 21.49ms | 14.32ms | 0.7× | 0.7% | PASS |
| file_reads | `select_random_ranges` | 12.58ms | 7.18ms | 0.6× | 0.7% | PASS |
| file_reads | `covering_index_scan` | 15.88ms | 12.41ms | 0.8× | 0.7% | PASS |
| file_reads | `groupby_scan` | 33.01ms | 35.76ms | 1.1× | 0.7% | PASS |
| file_reads | `index_join` | 10.23ms | 10.20ms | 1.0× | 1.4% | PASS |
| file_reads | `index_join_scan` | 4.29ms | 6.34ms | 1.5× | 2.1% | PASS |
| file_reads | `types_table_scan` | 1.08s | 1.27s | 1.2× | 0.7% | PASS |
| file_reads | `table_scan` | 1.22s | 1.38s | 1.1× | 0.9% | PASS |
| file_reads | `oltp_read_only` | 215.15ms | 151.99ms | 0.7× | 0.5% | PASS |
| file_writes | `oltp_bulk_insert` | 200.15ms | 315.00ms | 1.6× | 0.8% | PASS |
| file_writes | `oltp_insert` | 22.09ms | 30.70ms | 1.4× | 1.4% | PASS |
| file_writes | `oltp_update_index` | 80.36ms | 114.06ms | 1.4× | 1.3% | PASS |
| file_writes | `oltp_update_non_index` | 60.38ms | 72.82ms | 1.2× | 1.9% | PASS |
| file_writes | `oltp_delete_insert` | 69.42ms | 93.09ms | 1.3× | 1.4% | PASS |
| file_writes | `oltp_write_only` | 45.29ms | 57.98ms | 1.3× | 1.1% | PASS |
| file_writes | `types_delete_insert` | 41.40ms | 45.05ms | 1.1× | 1.7% | PASS |
| file_writes | `oltp_read_write` | 91.14ms | 116.20ms | 1.3× | 2.2% | PASS |
| ac_reads | `oltp_point_select` | 50.60ms | 50.35ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_range_select` | 14.01ms | 14.59ms | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_sum_range` | 12.54ms | 14.16ms | 1.1× | 2.1% | PASS |
| ac_reads | `oltp_order_range` | 3.19ms | 3.36ms | 1.1× | 1.5% | PASS |
| ac_reads | `oltp_distinct_range` | 4.19ms | 4.51ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 7.05ms | 7.74ms | 1.1× | 1.9% | PASS |
| ac_reads | `select_random_points` | 16.12ms | 14.38ms | 0.9× | 1.5% | PASS |
| ac_reads | `select_random_ranges` | 7.40ms | 7.19ms | 1.0× | 1.8% | PASS |
| ac_reads | `covering_index_scan` | 10.65ms | 12.48ms | 1.2× | 1.3% | PASS |
| ac_reads | `groupby_scan` | 32.58ms | 35.83ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 7.65ms | 10.32ms | 1.4× | 1.6% | PASS |
| ac_reads | `index_join_scan` | 3.85ms | 6.41ms | 1.7× | 1.5% | PASS |
| ac_reads | `types_table_scan` | 1.11s | 1.28s | 1.2× | 0.9% | PASS |
| ac_reads | `table_scan` | 1.27s | 1.39s | 1.1× | 0.9% | PASS |
| ac_reads | `oltp_read_only` | 147.89ms | 154.53ms | 1.0× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 15.99ms | 53.17ms | 3.3× | 3.9% | PASS |
| ac_writes | `oltp_insert_ac` | 18.55ms | 67.10ms | 3.6× | 4.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 20.25ms | 81.05ms | 4.0× | 5.5% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 16.24ms | 60.97ms | 3.8× | 4.7% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 18.24ms | 71.46ms | 3.9× | 6.2% | PASS |
| ac_writes | `oltp_write_only_ac` | 18.59ms | 70.38ms | 3.8× | 3.9% | PASS |
| ac_writes | `types_delete_insert_ac` | 15.62ms | 59.63ms | 3.8× | 4.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 23.02ms | 75.08ms | 3.3× | 5.1% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 37.52ms | 42.83ms | 1.1× | 2.4% | PASS |
| mem_reads | `oltp_range_select` | 17.85ms | 16.95ms | 0.9× | 2.0% | PASS |
| mem_reads | `oltp_sum_range` | 16.07ms | 16.11ms | 1.0× | 1.8% | PASS |
| mem_reads | `oltp_order_range` | 3.38ms | 3.58ms | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_distinct_range` | 4.47ms | 4.72ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 4.10ms | 7.17ms | 1.7× | 2.0% | PASS |
| mem_reads | `select_random_points` | 23.83ms | 23.76ms | 1.0× | 3.1% | PASS |
| mem_reads | `select_random_ranges` | 6.96ms | 7.36ms | 1.1× | 1.7% | PASS |
| mem_reads | `covering_index_scan` | 7.82ms | 11.55ms | 1.5× | 1.2% | PASS |
| mem_reads | `groupby_scan` | 35.40ms | 37.79ms | 1.1× | 0.9% | PASS |
| mem_reads | `index_join` | 11.63ms | 11.47ms | 1.0× | 2.4% | PASS |
| mem_reads | `index_join_scan` | 4.06ms | 7.23ms | 1.8× | 2.7% | PASS |
| mem_reads | `types_table_scan` | 1.17s | 1.41s | 1.2× | 2.0% | PASS |
| mem_reads | `table_scan` | 1.40s | 1.52s | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_read_only` | 151.50ms | 157.51ms | 1.0× | 1.5% | PASS |
| mem_writes | `oltp_bulk_insert` | 245.69ms | 386.28ms | 1.6× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 18.35ms | 39.43ms | 2.1× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 71.73ms | 151.68ms | 2.1× | 1.9% | PASS |
| mem_writes | `oltp_update_non_index` | 54.26ms | 88.15ms | 1.6× | 1.9% | PASS |
| mem_writes | `oltp_delete_insert` | 58.23ms | 112.06ms | 1.9× | 1.3% | PASS |
| mem_writes | `oltp_write_only` | 30.08ms | 59.22ms | 2.0× | 1.7% | PASS |
| mem_writes | `types_delete_insert` | 40.97ms | 67.59ms | 1.6× | 2.3% | PASS |
| mem_writes | `oltp_read_write` | 111.10ms | 149.72ms | 1.3× | 2.0% | PASS |
| file_reads | `oltp_point_select` | 103.72ms | 60.38ms | 0.6× | 1.6% | PASS |
| file_reads | `oltp_range_select` | 25.21ms | 18.97ms | 0.8× | 1.0% | PASS |
| file_reads | `oltp_sum_range` | 23.13ms | 18.04ms | 0.8× | 1.5% | PASS |
| file_reads | `oltp_order_range` | 4.26ms | 3.77ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_distinct_range` | 5.33ms | 4.99ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_index_scan` | 10.91ms | 8.96ms | 0.8× | 1.5% | PASS |
| file_reads | `select_random_points` | 33.16ms | 26.76ms | 0.8× | 2.7% | PASS |
| file_reads | `select_random_ranges` | 14.31ms | 9.33ms | 0.7× | 1.5% | PASS |
| file_reads | `covering_index_scan` | 15.30ms | 13.42ms | 0.9× | 0.9% | PASS |
| file_reads | `groupby_scan` | 36.19ms | 38.02ms | 1.1× | 1.0% | PASS |
| file_reads | `index_join` | 16.30ms | 12.80ms | 0.8× | 2.5% | PASS |
| file_reads | `index_join_scan` | 5.07ms | 7.44ms | 1.5× | 2.6% | PASS |
| file_reads | `types_table_scan` | 1.20s | 1.42s | 1.2× | 2.0% | PASS |
| file_reads | `table_scan` | 1.39s | 1.52s | 1.1× | 1.3% | PASS |
| file_reads | `oltp_read_only` | 252.73ms | 186.56ms | 0.7× | 1.3% | PASS |
| file_writes | `oltp_bulk_insert` | 271.14ms | 402.30ms | 1.5× | 0.9% | PASS |
| file_writes | `oltp_insert` | 26.82ms | 45.46ms | 1.7× | 1.4% | PASS |
| file_writes | `oltp_update_index` | 129.94ms | 175.41ms | 1.3× | 7.6% | PASS |
| file_writes | `oltp_update_non_index` | 126.39ms | 102.82ms | 0.8× | 6.2% | PASS |
| file_writes | `oltp_delete_insert` | 103.37ms | 131.33ms | 1.3× | 2.1% | PASS |
| file_writes | `oltp_write_only` | 89.80ms | 72.40ms | 0.8× | 4.9% | PASS |
| file_writes | `types_delete_insert` | 75.20ms | 77.31ms | 1.0× | 1.5% | PASS |
| file_writes | `oltp_read_write` | 172.42ms | 159.03ms | 0.9× | 9.4% | PASS |
| ac_reads | `oltp_point_select` | 60.90ms | 61.01ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_range_select` | 20.93ms | 19.18ms | 0.9× | 1.5% | PASS |
| ac_reads | `oltp_sum_range` | 19.21ms | 18.56ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_order_range` | 3.89ms | 3.85ms | 1.0× | 2.0% | PASS |
| ac_reads | `oltp_distinct_range` | 4.97ms | 5.03ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_index_scan` | 6.86ms | 9.17ms | 1.3× | 1.2% | PASS |
| ac_reads | `select_random_points` | 28.68ms | 27.87ms | 1.0× | 3.0% | PASS |
| ac_reads | `select_random_ranges` | 9.65ms | 9.37ms | 1.0× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 10.61ms | 13.38ms | 1.3× | 1.3% | PASS |
| ac_reads | `groupby_scan` | 35.61ms | 38.10ms | 1.1× | 0.9% | PASS |
| ac_reads | `index_join` | 13.64ms | 12.77ms | 0.9× | 2.7% | PASS |
| ac_reads | `index_join_scan` | 4.53ms | 7.61ms | 1.7× | 2.6% | PASS |
| ac_reads | `types_table_scan` | 1.12s | 1.40s | 1.2× | 2.9% | PASS |
| ac_reads | `table_scan` | 1.32s | 1.49s | 1.1× | 2.3% | PASS |
| ac_reads | `oltp_read_only` | 176.43ms | 182.01ms | 1.0× | 2.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.63ms | 64.84ms | 2.9× | 10.0% | PASS |
| ac_writes | `oltp_insert_ac` | 25.87ms | 76.27ms | 2.9× | 7.7% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.98ms | 94.06ms | 3.5× | 5.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.23ms | 73.87ms | 3.2× | 8.9% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.05ms | 86.51ms | 3.3× | 8.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.59ms | 83.32ms | 3.3× | 7.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 24.09ms | 76.63ms | 3.2× | 10.2% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.43ms | 92.34ms | 2.8× | 5.6% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 34.38ms | 42.23ms | 1.2× | 1.8% | PASS |
| mem_reads | `oltp_range_select` | 15.92ms | 16.63ms | 1.0× | 1.5% | PASS |
| mem_reads | `oltp_sum_range` | 14.49ms | 15.43ms | 1.1× | 2.1% | PASS |
| mem_reads | `oltp_order_range` | 3.17ms | 3.48ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_distinct_range` | 4.33ms | 4.64ms | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_index_scan` | 3.96ms | 7.05ms | 1.8× | 1.2% | PASS |
| mem_reads | `select_random_points` | 20.97ms | 23.15ms | 1.1× | 1.4% | PASS |
| mem_reads | `select_random_ranges` | 6.43ms | 7.18ms | 1.1× | 1.3% | PASS |
| mem_reads | `covering_index_scan` | 7.73ms | 11.37ms | 1.5× | 1.0% | PASS |
| mem_reads | `groupby_scan` | 33.95ms | 37.02ms | 1.1× | 0.9% | PASS |
| mem_reads | `index_join` | 9.99ms | 11.27ms | 1.1× | 1.7% | PASS |
| mem_reads | `index_join_scan` | 3.65ms | 6.67ms | 1.8× | 1.7% | PASS |
| mem_reads | `types_table_scan` | 1.04s | 1.37s | 1.3× | 0.7% | PASS |
| mem_reads | `table_scan` | 1.16s | 1.47s | 1.3× | 0.8% | PASS |
| mem_reads | `oltp_read_only` | 136.64ms | 152.30ms | 1.1× | 1.2% | PASS |
| mem_writes | `oltp_bulk_insert` | 245.09ms | 384.41ms | 1.6× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 18.52ms | 38.71ms | 2.1× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 63.79ms | 141.22ms | 2.2× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 47.58ms | 82.08ms | 1.7× | 1.6% | PASS |
| mem_writes | `oltp_delete_insert` | 52.04ms | 105.89ms | 2.0× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 27.65ms | 56.76ms | 2.1× | 1.3% | PASS |
| mem_writes | `types_delete_insert` | 37.71ms | 63.45ms | 1.7× | 1.2% | PASS |
| mem_writes | `oltp_read_write` | 94.56ms | 141.37ms | 1.5× | 1.3% | PASS |
| file_reads | `oltp_point_select` | 101.57ms | 59.54ms | 0.6× | 1.4% | PASS |
| file_reads | `oltp_range_select` | 23.38ms | 18.53ms | 0.8× | 1.5% | PASS |
| file_reads | `oltp_sum_range` | 21.94ms | 17.55ms | 0.8× | 2.1% | PASS |
| file_reads | `oltp_order_range` | 4.26ms | 3.75ms | 0.9× | 1.5% | PASS |
| file_reads | `oltp_distinct_range` | 5.38ms | 4.94ms | 0.9× | 1.9% | PASS |
| file_reads | `oltp_index_scan` | 10.84ms | 8.88ms | 0.8× | 1.5% | PASS |
| file_reads | `select_random_points` | 29.69ms | 25.87ms | 0.9× | 1.6% | PASS |
| file_reads | `select_random_ranges` | 13.65ms | 9.13ms | 0.7× | 1.3% | PASS |
| file_reads | `covering_index_scan` | 14.90ms | 13.28ms | 0.9× | 1.2% | PASS |
| file_reads | `groupby_scan` | 35.06ms | 37.40ms | 1.1× | 1.0% | PASS |
| file_reads | `index_join` | 14.39ms | 12.45ms | 0.9× | 2.0% | PASS |
| file_reads | `index_join_scan` | 4.72ms | 7.11ms | 1.5× | 1.9% | PASS |
| file_reads | `types_table_scan` | 1.03s | 1.37s | 1.3× | 1.0% | PASS |
| file_reads | `table_scan` | 1.16s | 1.47s | 1.3× | 1.1% | PASS |
| file_reads | `oltp_read_only` | 238.22ms | 179.48ms | 0.8× | 1.1% | PASS |
| file_writes | `oltp_bulk_insert` | 266.93ms | 399.60ms | 1.5× | 1.2% | PASS |
| file_writes | `oltp_insert` | 25.89ms | 45.94ms | 1.8× | 2.0% | PASS |
| file_writes | `oltp_update_index` | 97.10ms | 159.31ms | 1.6× | 1.3% | PASS |
| file_writes | `oltp_update_non_index` | 94.10ms | 95.78ms | 1.0× | 8.3% | PASS |
| file_writes | `oltp_delete_insert` | 87.59ms | 122.20ms | 1.4× | 1.6% | PASS |
| file_writes | `oltp_write_only` | 58.07ms | 69.87ms | 1.2× | 2.2% | PASS |
| file_writes | `types_delete_insert` | 63.53ms | 71.44ms | 1.1× | 1.9% | PASS |
| file_writes | `oltp_read_write` | 129.59ms | 155.21ms | 1.2× | 1.8% | PASS |
| ac_reads | `oltp_point_select` | 57.21ms | 59.54ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_range_select` | 18.84ms | 18.61ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_sum_range` | 17.46ms | 17.57ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_order_range` | 3.76ms | 3.76ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_distinct_range` | 4.88ms | 4.94ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 6.51ms | 8.94ms | 1.4× | 1.3% | PASS |
| ac_reads | `select_random_points` | 24.66ms | 25.86ms | 1.0× | 2.1% | PASS |
| ac_reads | `select_random_ranges` | 9.16ms | 9.17ms | 1.0× | 2.1% | PASS |
| ac_reads | `covering_index_scan` | 10.35ms | 13.29ms | 1.3× | 1.2% | PASS |
| ac_reads | `groupby_scan` | 34.64ms | 37.46ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 12.40ms | 12.43ms | 1.0× | 1.9% | PASS |
| ac_reads | `index_join_scan` | 4.26ms | 7.25ms | 1.7× | 1.5% | PASS |
| ac_reads | `types_table_scan` | 1.04s | 1.37s | 1.3× | 1.0% | PASS |
| ac_reads | `table_scan` | 1.16s | 1.46s | 1.3× | 0.7% | PASS |
| ac_reads | `oltp_read_only` | 172.18ms | 179.87ms | 1.0× | 1.1% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 20.41ms | 61.11ms | 3.0× | 5.4% | PASS |
| ac_writes | `oltp_insert_ac` | 23.52ms | 78.71ms | 3.3× | 6.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 25.64ms | 89.18ms | 3.5× | 6.8% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 20.15ms | 70.17ms | 3.5× | 4.9% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 23.20ms | 80.28ms | 3.5× | 5.9% | PASS |
| ac_writes | `oltp_write_only_ac` | 22.68ms | 78.63ms | 3.5× | 4.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 21.41ms | 71.24ms | 3.3× | 8.6% | PASS |
| ac_writes | `oltp_read_write_ac` | 30.61ms | 87.14ms | 2.8× | 7.0% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 23.15ms | 26.98ms | 1.2× | 3.1% | PASS |
| mem_reads | `oltp_range_select` | 12.52ms | 12.88ms | 1.0× | 3.1% | PASS |
| mem_reads | `oltp_sum_range` | 11.26ms | 12.72ms | 1.1× | 3.2% | PASS |
| mem_reads | `oltp_order_range` | 2.33ms | 2.41ms | 1.0× | 1.6% | PASS |
| mem_reads | `oltp_distinct_range` | 2.81ms | 2.87ms | 1.0× | 1.6% | PASS |
| mem_reads | `oltp_index_scan` | 3.06ms | 4.27ms | 1.4× | 4.6% | PASS |
| mem_reads | `select_random_points` | 18.36ms | 20.11ms | 1.1× | 4.4% | PASS |
| mem_reads | `select_random_ranges` | 4.73ms | 5.42ms | 1.1× | 1.8% | PASS |
| mem_reads | `covering_index_scan` | 4.11ms | 6.10ms | 1.5× | 4.0% | PASS |
| mem_reads | `groupby_scan` | 21.71ms | 23.20ms | 1.1× | 1.9% | PASS |
| mem_reads | `index_join` | 5.02ms | 8.10ms | 1.6× | 2.2% | PASS |
| mem_reads | `index_join_scan` | 2.96ms | 5.07ms | 1.7× | 5.9% | PASS |
| mem_reads | `types_table_scan` | 707.20ms | 831.96ms | 1.2× | 1.5% | PASS |
| mem_reads | `table_scan` | 827.25ms | 921.78ms | 1.1× | 2.3% | PASS |
| mem_reads | `oltp_read_only` | 87.21ms | 94.80ms | 1.1× | 3.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 148.70ms | 214.90ms | 1.4× | 1.3% | PASS |
| mem_writes | `oltp_insert` | 11.70ms | 22.05ms | 1.9× | 1.2% | PASS |
| mem_writes | `oltp_update_index` | 45.48ms | 88.03ms | 1.9× | 2.1% | PASS |
| mem_writes | `oltp_update_non_index` | 34.90ms | 57.78ms | 1.7× | 1.4% | PASS |
| mem_writes | `oltp_delete_insert` | 32.96ms | 63.91ms | 1.9× | 2.1% | PASS |
| mem_writes | `oltp_write_only` | 18.92ms | 41.54ms | 2.2× | 2.0% | PASS |
| mem_writes | `types_delete_insert` | 22.86ms | 37.83ms | 1.7× | 2.1% | PASS |
| mem_writes | `oltp_read_write` | 63.67ms | 94.19ms | 1.5× | 2.6% | PASS |
| file_reads | `oltp_point_select` | 73.07ms | 40.11ms | 0.5× | 2.0% | PASS |
| file_reads | `oltp_range_select` | 17.60ms | 14.16ms | 0.8× | 1.6% | PASS |
| file_reads | `oltp_sum_range` | 16.47ms | 13.86ms | 0.8× | 2.9% | PASS |
| file_reads | `oltp_order_range` | 2.94ms | 2.59ms | 0.9× | 1.2% | PASS |
| file_reads | `oltp_distinct_range` | 3.42ms | 3.07ms | 0.9× | 1.5% | PASS |
| file_reads | `oltp_index_scan` | 8.47ms | 5.99ms | 0.7× | 1.8% | PASS |
| file_reads | `select_random_points` | 24.06ms | 21.93ms | 0.9× | 3.0% | PASS |
| file_reads | `select_random_ranges` | 9.91ms | 6.89ms | 0.7× | 1.6% | PASS |
| file_reads | `covering_index_scan` | 9.78ms | 7.83ms | 0.8× | 1.9% | PASS |
| file_reads | `groupby_scan` | 21.82ms | 22.97ms | 1.1× | 1.7% | PASS |
| file_reads | `index_join` | 8.13ms | 8.55ms | 1.1× | 1.8% | PASS |
| file_reads | `index_join_scan` | 3.54ms | 5.11ms | 1.4× | 3.3% | PASS |
| file_reads | `types_table_scan` | 726.57ms | 853.88ms | 1.2× | 1.6% | PASS |
| file_reads | `table_scan` | 858.22ms | 938.79ms | 1.1× | 5.6% | PASS |
| file_reads | `oltp_read_only` | 171.79ms | 123.01ms | 0.7× | 1.9% | PASS |
| file_writes | `oltp_bulk_insert` | 200.57ms | 262.58ms | 1.3× | 2.2% | PASS |
| file_writes | `oltp_insert` | 19.58ms | 30.32ms | 1.5× | 7.6% | PASS |
| file_writes | `oltp_update_index` | 145.34ms | 142.81ms | 1.0× | 3.5% | PASS |
| file_writes | `oltp_update_non_index` | 120.96ms | 101.86ms | 0.8× | 8.9% | PASS |
| file_writes | `oltp_delete_insert` | 120.94ms | 111.20ms | 0.9× | 2.4% | PASS |
| file_writes | `oltp_write_only` | 81.14ms | 81.50ms | 1.0× | 1.7% | PASS |
| file_writes | `types_delete_insert` | 79.84ms | 69.99ms | 0.9× | 3.1% | PASS |
| file_writes | `oltp_read_write` | 130.93ms | 138.72ms | 1.1× | 5.9% | PASS |
| ac_reads | `oltp_point_select` | 40.02ms | 40.45ms | 1.0× | 1.8% | PASS |
| ac_reads | `oltp_range_select` | 14.07ms | 14.01ms | 1.0× | 1.8% | PASS |
| ac_reads | `oltp_sum_range` | 13.20ms | 14.03ms | 1.1× | 1.7% | PASS |
| ac_reads | `oltp_order_range` | 2.69ms | 2.66ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_distinct_range` | 3.23ms | 3.20ms | 1.0× | 2.0% | PASS |
| ac_reads | `oltp_index_scan` | 5.50ms | 6.24ms | 1.1× | 2.8% | PASS |
| ac_reads | `select_random_points` | 21.95ms | 23.65ms | 1.1× | 4.6% | PASS |
| ac_reads | `select_random_ranges` | 6.90ms | 7.12ms | 1.0× | 1.2% | PASS |
| ac_reads | `covering_index_scan` | 6.82ms | 8.34ms | 1.2× | 2.8% | PASS |
| ac_reads | `groupby_scan` | 22.05ms | 23.09ms | 1.0× | 2.0% | PASS |
| ac_reads | `index_join` | 6.89ms | 9.05ms | 1.3× | 4.7% | PASS |
| ac_reads | `index_join_scan` | 3.52ms | 5.76ms | 1.6× | 6.9% | PASS |
| ac_reads | `types_table_scan` | 826.56ms | 888.07ms | 1.1× | 2.9% | PASS |
| ac_reads | `table_scan` | 947.75ms | 950.51ms | 1.0× | 4.4% | PASS |
| ac_reads | `oltp_read_only` | 106.06ms | 111.68ms | 1.1× | 1.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.97ms | 55.42ms | 2.5× | 4.9% | PASS |
| ac_writes | `oltp_insert_ac` | 26.55ms | 65.55ms | 2.5× | 6.1% | PASS |
| ac_writes | `oltp_update_index_ac` | 28.22ms | 73.49ms | 2.6× | 8.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.93ms | 59.38ms | 2.7× | 4.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.79ms | 65.52ms | 2.6× | 7.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.36ms | 65.25ms | 2.6× | 5.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 26.20ms | 61.61ms | 2.4× | 12.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 28.13ms | 72.56ms | 2.6× | 5.3% | PASS |

</details>

</details>

## Version-control latency

Wall time: 6m 36s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 29.54ms | 38.00ms | 77.7% | 0.7% | PASS |
| `status_dirty_many_tables` | 32.25ms | 42.00ms | 76.8% | 0.6% | PASS |
| `diff_regular_working_one_table` | 25.34ms | 33.00ms | 76.8% | 1.0% | PASS |
| `diff_regular_working_many_tables` | 36.54ms | 50.00ms | 73.1% | 1.4% | PASS |
| `diff_stat_working_many_tables` | 36.89ms | 48.00ms | 76.9% | 1.6% | PASS |
| `diff_schema_working_many_tables` | 37.53ms | 48.00ms | 78.2% | 1.0% | PASS |
| `branch_list_many_branches` | 19.41ms | 25.00ms | 77.6% | 1.2% | PASS |
| `branch_create_delete` | 21.18ms | 27.00ms | 78.5% | 1.1% | PASS |
| `at_literal_deep_history` | 21.09ms | 28.00ms | 75.3% | 1.1% | PASS |
| `diff_literal_deep_history` | 20.96ms | 28.00ms | 74.9% | 1.1% | PASS |
| `history_literal_deep_history` | 22.21ms | 30.00ms | 74.0% | 1.2% | PASS |
| `checkout_branch_clean` | 28.71ms | 43.00ms | 66.8% | 1.0% | PASS |
| `merge_data_no_conflicts` | 23.56ms | 33.00ms | 71.4% | 1.0% | PASS |
| `merge_data_secondary_index` | 798.44ms | 944.00ms | 84.6% | 0.7% | PASS |
| `merge_schema_no_conflicts` | 18.48ms | 24.00ms | 77.0% | 0.9% | PASS |
| `merge_data_conflicts` | 25.67ms | 33.00ms | 77.8% | 0.5% | PASS |
| `merge_data_conflicts_with_resolve` | 26.39ms | 34.00ms | 77.6% | 0.6% | PASS |

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

candidate: f3b36a8.

Fixture file bytes: base 107,968,301; delta 115,879,032.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 475.398 | 412.732–534.417 | 32.856 | 210.35 | 107968301 |
| push_initial | candidate | 785.976 | 784.436–791.780 | 408.806 | 127.23 | 107956807 |
| clone_initial | candidate | 1041.815 | 1037.072–1054.309 | 528.569 | 95.99 | 108475105 |
| push_noop | candidate | 0.547 | 0.529–0.560 | 0.276 | — | 0 |
| fetch_noop | candidate | 0.165 | 0.163–0.167 | 0.166 | — | 0 |
| push_incremental | candidate | 196.578 | 194.926–205.693 | 167.048 | 5.09 | 7909775 |
| fetch_incremental | candidate | 100.348 | 98.275–102.505 | 71.358 | 9.97 | 7910084 |
| pull_fast_forward | candidate | 91.992 | 90.463–180.320 | 63.026 | 10.87 | 7910844 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 90.54 | 175.487 | 108.763 | 18329 | 2066 |
| clone_initial | candidate | 90.89 | 295.025 | 109.784 | 47230 | 2337 |
| push_noop | candidate | 42.62 | 0.073 | 0.000 | 26 | 2 |
| fetch_noop | candidate | 42.62 | 0.135 | 0.000 | 40 | 0 |
| push_incremental | candidate | 42.62 | 96.722 | 7.910 | 22377 | 125 |
| fetch_incremental | candidate | 42.62 | 34.472 | 7.910 | 10808 | 124 |
| pull_fast_forward | candidate | 42.62 | 26.497 | 7.911 | 7031 | 126 |

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

candidate: f3b36a8.

Fixture file bytes: base 1,087,718,066; delta 1,103,525,194.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 5512.081 | 5477.629–5668.914 | 312.674 | 181.42 | 1087718066 |
| push_initial | candidate | 7881.070 | 7826.871–7886.512 | 4149.629 | 126.89 | 1079566310 |
| clone_initial | candidate | 9606.768 | 9408.906–9608.686 | 5871.124 | 104.09 | 1102407396 |
| push_noop | candidate | 0.721 | 0.668–0.723 | 0.257 | — | 0 |
| fetch_noop | candidate | 0.139 | 0.137–0.163 | 0.141 | — | 0 |
| push_incremental | candidate | 947.785 | 934.588–1040.498 | 877.441 | 1.06 | 15806003 |
| fetch_incremental | candidate | 272.319 | 271.325–307.302 | 203.003 | 3.67 | 15806513 |
| pull_fast_forward | candidate | 269.099 | 257.519–276.841 | 196.180 | 3.72 | 15807273 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 134.36 | 2091.445 | 1087.622 | 263392 | 20541 |
| clone_initial | candidate | 124.15 | 3531.027 | 1132.819 | 615478 | 32243 |
| push_noop | candidate | 45.30 | 0.078 | 0.000 | 27 | 2 |
| fetch_noop | candidate | 45.30 | 0.137 | 0.000 | 40 | 0 |
| push_incremental | candidate | 48.35 | 622.629 | 15.928 | 151214 | 313 |
| fetch_incremental | candidate | 50.47 | 61.717 | 15.928 | 13769 | 313 |
| pull_fast_forward | candidate | 50.52 | 61.650 | 15.929 | 13765 | 315 |

</details>


</details>

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh`, `test/vc_perf_ceiling.sh`, and `test/remote_perf.py`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
