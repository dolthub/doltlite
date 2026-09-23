# DoltLite Performance Report

> Nightly result: **FAIL**
>
> Generated: 2026-09-23 11:10 UTC
>
> Commit: [`1717d2082d11ce551737f236c218d8aa7a861bbd`](https://github.com/dolthub/doltlite/commit/1717d2082d11ce551737f236c218d8aa7a861bbd)
>
> Runner: ubuntu24 20260920.314.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/35843932366)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.51s | 10.69s | 1.1× | 1.1% | **PASS** |
| Writes | 1.89s | 3.08s | 1.6× | 0.9% | **FAIL** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.45s | 11.02s | 1.1× | 1.4% | **PASS** |
| Writes | 4.24s | 4.63s | 1.1× | 3.0% | **PASS** |
| Autocommit writes | 2.40s | 6.49s | 2.7× | 12.0% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.29s | 2.52s | 1.1× | 0.8% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.54s | 2.97s | 1.2× | 1.1% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 1.81s | 2.03s | 1.1× | 2.2% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.87s | 3.17s | 1.1× | 0.9% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 348.03ms | 554.93ms | 1.6× | 0.9% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 570.93ms | 983.29ms | 1.7× | 0.7% | **FAIL** |
| In-memory | Writes | blobpk | 8 | 55 | 360.00ms | 583.38ms | 1.6× | 2.2% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 606.19ms | 954.54ms | 1.6× | 1.2% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.40s | 2.58s | 1.1× | 1.0% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.76s | 3.04s | 1.1× | 1.2% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 1.87s | 2.05s | 1.1× | 2.0% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 3.43s | 3.35s | 1.0× | 1.0% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 916.60ms | 953.01ms | 1.0× | 3.0% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 923.65ms | 1.12s | 1.2× | 5.4% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 1.62s | 1.50s | 0.9× | 33.5% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 781.75ms | 1.06s | 1.4× | 1.7% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.33s | 2.58s | 1.1× | 1.1% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.65s | 3.05s | 1.2× | 1.5% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 1.81s | 2.05s | 1.1× | 2.0% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 3.23s | 3.33s | 1.0× | 0.9% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 234.62ms | 981.45ms | 4.2× | 18.7% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 243.74ms | 755.41ms | 3.1× | 6.6% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 1.77s | 4.17s | 2.3× | 55.8% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 147.20ms | 583.38ms | 4.0× | 4.1% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 20.47ms | 23.07ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_range_select` | 9.23ms | 10.10ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_sum_range` | 8.31ms | 9.46ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_order_range` | 2.39ms | 2.55ms | 1.1× | 0.6% | PASS |
| mem_reads | `oltp_distinct_range` | 3.23ms | 3.56ms | 1.1× | 1.8% | PASS |
| mem_reads | `oltp_index_scan` | 3.39ms | 4.05ms | 1.2× | 0.9% | PASS |
| mem_reads | `select_random_points` | 9.45ms | 9.74ms | 1.0× | 0.8% | PASS |
| mem_reads | `select_random_ranges` | 3.77ms | 3.91ms | 1.0× | 0.9% | PASS |
| mem_reads | `covering_index_scan` | 6.21ms | 8.05ms | 1.3× | 0.6% | PASS |
| mem_reads | `groupby_scan` | 28.01ms | 30.18ms | 1.1× | 0.7% | PASS |
| mem_reads | `index_join` | 4.99ms | 6.76ms | 1.4× | 0.6% | PASS |
| mem_reads | `index_join_scan` | 2.68ms | 4.17ms | 1.6× | 1.3% | PASS |
| mem_reads | `types_table_scan` | 985.36ms | 1.10s | 1.1× | 0.3% | PASS |
| mem_reads | `table_scan` | 1.11s | 1.21s | 1.1× | 0.3% | PASS |
| mem_reads | `oltp_read_only` | 88.16ms | 101.73ms | 1.2× | 0.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 138.67ms | 207.58ms | 1.5× | 0.5% | PASS |
| mem_writes | `oltp_insert` | 12.31ms | 22.07ms | 1.8× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 41.72ms | 75.12ms | 1.8× | 1.2% | PASS |
| mem_writes | `oltp_update_non_index` | 27.50ms | 43.23ms | 1.6× | 0.9% | PASS |
| mem_writes | `oltp_delete_insert` | 37.29ms | 58.65ms | 1.6× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 17.70ms | 35.87ms | 2.0× | 1.2% | PASS |
| mem_writes | `types_delete_insert` | 19.70ms | 29.78ms | 1.5× | 0.8% | PASS |
| mem_writes | `oltp_read_write` | 53.13ms | 82.62ms | 1.6× | 0.8% | PASS |
| file_reads | `oltp_point_select` | 49.66ms | 30.77ms | 0.6× | 0.6% | PASS |
| file_reads | `oltp_range_select` | 12.50ms | 11.22ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_sum_range` | 11.91ms | 10.65ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_order_range` | 2.81ms | 2.77ms | 1.0× | 1.4% | PASS |
| file_reads | `oltp_distinct_range` | 3.68ms | 3.68ms | 1.0× | 1.0% | PASS |
| file_reads | `oltp_index_scan` | 6.69ms | 5.34ms | 0.8× | 1.5% | PASS |
| file_reads | `select_random_points` | 13.28ms | 11.49ms | 0.9× | 0.6% | PASS |
| file_reads | `select_random_ranges` | 7.02ms | 4.90ms | 0.7× | 0.8% | PASS |
| file_reads | `covering_index_scan` | 9.55ms | 9.23ms | 1.0× | 1.2% | PASS |
| file_reads | `groupby_scan` | 28.40ms | 29.84ms | 1.1× | 0.7% | PASS |
| file_reads | `index_join` | 6.89ms | 7.99ms | 1.2× | 1.2% | PASS |
| file_reads | `index_join_scan` | 3.16ms | 4.62ms | 1.5× | 1.5% | PASS |
| file_reads | `types_table_scan` | 991.27ms | 1.11s | 1.1× | 0.3% | PASS |
| file_reads | `table_scan` | 1.12s | 1.22s | 1.1× | 0.3% | PASS |
| file_reads | `oltp_read_only` | 130.64ms | 112.95ms | 0.9× | 0.6% | PASS |
| file_writes | `oltp_bulk_insert` | 193.16ms | 271.26ms | 1.4× | 4.5% | PASS |
| file_writes | `oltp_insert` | 27.88ms | 38.14ms | 1.4× | 5.5% | PASS |
| file_writes | `oltp_update_index` | 146.11ms | 142.70ms | 1.0× | 2.9% | PASS |
| file_writes | `oltp_update_non_index` | 118.64ms | 98.49ms | 0.8× | 1.4% | PASS |
| file_writes | `oltp_delete_insert` | 126.63ms | 119.37ms | 0.9× | 2.7% | PASS |
| file_writes | `oltp_write_only` | 93.49ms | 84.97ms | 0.9× | 2.6% | PASS |
| file_writes | `types_delete_insert` | 80.09ms | 66.14ms | 0.8× | 6.5% | PASS |
| file_writes | `oltp_read_write` | 130.60ms | 131.94ms | 1.0× | 3.1% | PASS |
| ac_reads | `oltp_point_select` | 29.20ms | 30.70ms | 1.1× | 0.7% | PASS |
| ac_reads | `oltp_range_select` | 10.41ms | 11.12ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_sum_range` | 9.78ms | 10.59ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_order_range` | 2.58ms | 2.75ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_distinct_range` | 3.46ms | 3.65ms | 1.1× | 0.9% | PASS |
| ac_reads | `oltp_index_scan` | 4.68ms | 5.30ms | 1.1× | 1.9% | PASS |
| ac_reads | `select_random_points` | 11.15ms | 11.38ms | 1.0× | 1.3% | PASS |
| ac_reads | `select_random_ranges` | 4.93ms | 4.86ms | 1.0× | 1.2% | PASS |
| ac_reads | `covering_index_scan` | 7.37ms | 9.19ms | 1.2× | 0.9% | PASS |
| ac_reads | `groupby_scan` | 28.37ms | 29.85ms | 1.1× | 0.6% | PASS |
| ac_reads | `index_join` | 5.86ms | 7.85ms | 1.3× | 2.6% | PASS |
| ac_reads | `index_join_scan` | 2.98ms | 4.59ms | 1.5× | 1.4% | PASS |
| ac_reads | `types_table_scan` | 991.45ms | 1.12s | 1.1× | 0.3% | PASS |
| ac_reads | `table_scan` | 1.12s | 1.22s | 1.1× | 0.4% | PASS |
| ac_reads | `oltp_read_only` | 101.42ms | 112.96ms | 1.1× | 0.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 30.10ms | 116.73ms | 3.9× | 25.1% | PASS |
| ac_writes | `oltp_insert_ac` | 26.43ms | 120.03ms | 4.5× | 16.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 30.68ms | 129.25ms | 4.2× | 18.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 27.05ms | 110.25ms | 4.1× | 18.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 28.30ms | 116.00ms | 4.1× | 19.0% | PASS |
| ac_writes | `oltp_write_only_ac` | 28.96ms | 114.32ms | 3.9× | 16.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 30.40ms | 139.71ms | 4.6× | 20.3% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.70ms | 135.16ms | 4.1× | 21.0% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.17ms | 38.99ms | 1.2× | 1.1% | PASS |
| mem_reads | `oltp_range_select` | 15.43ms | 15.55ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_sum_range` | 14.04ms | 14.95ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_order_range` | 3.10ms | 3.28ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_distinct_range` | 4.21ms | 4.39ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 3.83ms | 6.25ms | 1.6× | 1.2% | PASS |
| mem_reads | `select_random_points` | 20.43ms | 22.16ms | 1.1× | 1.9% | PASS |
| mem_reads | `select_random_ranges` | 6.33ms | 6.72ms | 1.1× | 1.7% | PASS |
| mem_reads | `covering_index_scan` | 7.74ms | 9.91ms | 1.3× | 1.0% | PASS |
| mem_reads | `groupby_scan` | 33.07ms | 34.69ms | 1.0× | 0.7% | PASS |
| mem_reads | `index_join` | 9.82ms | 9.10ms | 0.9× | 1.5% | PASS |
| mem_reads | `index_join_scan` | 3.51ms | 5.86ms | 1.7× | 1.7% | PASS |
| mem_reads | `types_table_scan` | 1.06s | 1.27s | 1.2× | 0.4% | PASS |
| mem_reads | `table_scan` | 1.19s | 1.39s | 1.2× | 0.4% | PASS |
| mem_reads | `oltp_read_only` | 129.41ms | 143.70ms | 1.1× | 0.5% | PASS |
| mem_writes | `oltp_bulk_insert` | 239.58ms | 362.88ms | 1.5× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 17.72ms | 38.69ms | 2.2× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 62.50ms | 138.13ms | 2.2× | 0.5% | PASS |
| mem_writes | `oltp_update_non_index` | 45.39ms | 82.05ms | 1.8× | 0.6% | PASS |
| mem_writes | `oltp_delete_insert` | 51.80ms | 104.36ms | 2.0× | 0.5% | PASS |
| mem_writes | `oltp_write_only` | 26.58ms | 61.15ms | 2.3× | 0.9% | FAIL |
| mem_writes | `types_delete_insert` | 36.17ms | 54.37ms | 1.5× | 0.8% | PASS |
| mem_writes | `oltp_read_write` | 91.21ms | 141.66ms | 1.6× | 0.6% | PASS |
| file_reads | `oltp_point_select` | 102.52ms | 58.87ms | 0.6× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 22.44ms | 18.12ms | 0.8× | 1.5% | PASS |
| file_reads | `oltp_sum_range` | 21.34ms | 17.68ms | 0.8× | 1.6% | PASS |
| file_reads | `oltp_order_range` | 3.95ms | 3.64ms | 0.9× | 1.8% | PASS |
| file_reads | `oltp_distinct_range` | 5.04ms | 4.76ms | 0.9× | 1.2% | PASS |
| file_reads | `oltp_index_scan` | 10.93ms | 8.66ms | 0.8× | 0.9% | PASS |
| file_reads | `select_random_points` | 28.74ms | 25.67ms | 0.9× | 1.6% | PASS |
| file_reads | `select_random_ranges` | 13.49ms | 8.93ms | 0.7× | 1.6% | PASS |
| file_reads | `covering_index_scan` | 14.93ms | 12.43ms | 0.8× | 1.1% | PASS |
| file_reads | `groupby_scan` | 33.83ms | 35.47ms | 1.0× | 0.7% | PASS |
| file_reads | `index_join` | 13.75ms | 11.48ms | 0.8× | 2.4% | PASS |
| file_reads | `index_join_scan` | 4.43ms | 6.59ms | 1.5× | 3.8% | PASS |
| file_reads | `types_table_scan` | 1.06s | 1.26s | 1.2× | 0.4% | PASS |
| file_reads | `table_scan` | 1.20s | 1.39s | 1.2× | 0.4% | PASS |
| file_reads | `oltp_read_only` | 232.68ms | 173.21ms | 0.7× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 263.29ms | 378.37ms | 1.4× | 1.0% | PASS |
| file_writes | `oltp_insert` | 25.06ms | 45.39ms | 1.8× | 1.8% | PASS |
| file_writes | `oltp_update_index` | 127.76ms | 161.17ms | 1.3× | 14.5% | PASS |
| file_writes | `oltp_update_non_index` | 105.30ms | 100.04ms | 1.0× | 11.4% | PASS |
| file_writes | `oltp_delete_insert` | 91.69ms | 122.61ms | 1.3× | 1.4% | PASS |
| file_writes | `oltp_write_only` | 87.54ms | 77.00ms | 0.9× | 12.4% | PASS |
| file_writes | `types_delete_insert` | 71.70ms | 69.58ms | 1.0× | 2.5% | PASS |
| file_writes | `oltp_read_write` | 151.32ms | 164.68ms | 1.1× | 8.2% | PASS |
| ac_reads | `oltp_point_select` | 60.34ms | 59.58ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_range_select` | 19.79ms | 18.05ms | 0.9× | 1.0% | PASS |
| ac_reads | `oltp_sum_range` | 18.32ms | 17.69ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_order_range` | 3.81ms | 3.67ms | 1.0× | 2.5% | PASS |
| ac_reads | `oltp_distinct_range` | 4.95ms | 4.83ms | 1.0× | 2.0% | PASS |
| ac_reads | `oltp_index_scan` | 6.64ms | 8.69ms | 1.3× | 1.5% | PASS |
| ac_reads | `select_random_points` | 24.82ms | 25.56ms | 1.0× | 2.1% | PASS |
| ac_reads | `select_random_ranges` | 9.28ms | 8.95ms | 1.0× | 1.5% | PASS |
| ac_reads | `covering_index_scan` | 10.41ms | 12.46ms | 1.2× | 1.4% | PASS |
| ac_reads | `groupby_scan` | 33.78ms | 35.47ms | 1.1× | 1.0% | PASS |
| ac_reads | `index_join` | 12.15ms | 11.31ms | 0.9× | 2.8% | PASS |
| ac_reads | `index_join_scan` | 4.11ms | 6.57ms | 1.6× | 2.6% | PASS |
| ac_reads | `types_table_scan` | 1.06s | 1.26s | 1.2× | 0.4% | PASS |
| ac_reads | `table_scan` | 1.20s | 1.40s | 1.2× | 1.0% | PASS |
| ac_reads | `oltp_read_only` | 182.89ms | 181.06ms | 1.0× | 3.3% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 28.38ms | 77.35ms | 2.7× | 7.8% | PASS |
| ac_writes | `oltp_insert_ac` | 30.53ms | 90.76ms | 3.0× | 6.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 32.21ms | 112.03ms | 3.5× | 6.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 28.13ms | 88.55ms | 3.1× | 7.1% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 30.13ms | 99.78ms | 3.3× | 6.0% | PASS |
| ac_writes | `oltp_write_only_ac` | 28.86ms | 92.79ms | 3.2× | 7.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 26.95ms | 88.77ms | 3.3× | 6.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 38.54ms | 105.38ms | 2.7× | 6.8% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 23.47ms | 23.61ms | 1.0× | 2.1% | PASS |
| mem_reads | `oltp_range_select` | 10.99ms | 10.01ms | 0.9× | 2.2% | PASS |
| mem_reads | `oltp_sum_range` | 10.96ms | 9.90ms | 0.9× | 1.9% | PASS |
| mem_reads | `oltp_order_range` | 2.23ms | 2.19ms | 1.0× | 3.6% | PASS |
| mem_reads | `oltp_distinct_range` | 2.86ms | 3.00ms | 1.0× | 2.6% | PASS |
| mem_reads | `oltp_index_scan` | 2.76ms | 4.20ms | 1.5× | 3.2% | PASS |
| mem_reads | `select_random_points` | 16.50ms | 15.85ms | 1.0× | 3.9% | PASS |
| mem_reads | `select_random_ranges` | 4.61ms | 4.11ms | 0.9× | 2.9% | PASS |
| mem_reads | `covering_index_scan` | 4.43ms | 5.84ms | 1.3× | 1.6% | PASS |
| mem_reads | `groupby_scan` | 21.50ms | 23.99ms | 1.1× | 1.6% | PASS |
| mem_reads | `index_join` | 9.25ms | 6.98ms | 0.8× | 1.6% | PASS |
| mem_reads | `index_join_scan` | 2.60ms | 5.19ms | 2.0× | 2.9% | PASS |
| mem_reads | `types_table_scan` | 746.59ms | 866.88ms | 1.2× | 1.7% | PASS |
| mem_reads | `table_scan` | 863.43ms | 955.24ms | 1.1× | 2.8% | PASS |
| mem_reads | `oltp_read_only` | 85.18ms | 89.19ms | 1.0× | 2.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 134.59ms | 198.04ms | 1.5× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 10.96ms | 22.75ms | 2.1× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 43.09ms | 88.41ms | 2.1× | 2.5% | PASS |
| mem_writes | `oltp_update_non_index` | 31.61ms | 51.07ms | 1.6× | 2.3% | PASS |
| mem_writes | `oltp_delete_insert` | 35.60ms | 66.31ms | 1.9× | 2.5% | PASS |
| mem_writes | `oltp_write_only` | 19.00ms | 38.02ms | 2.0× | 2.2% | PASS |
| mem_writes | `types_delete_insert` | 25.05ms | 32.81ms | 1.3× | 1.8% | PASS |
| mem_writes | `oltp_read_write` | 60.09ms | 85.99ms | 1.4× | 2.3% | PASS |
| file_reads | `oltp_point_select` | 44.25ms | 29.17ms | 0.7× | 1.5% | PASS |
| file_reads | `oltp_range_select` | 13.48ms | 10.91ms | 0.8× | 1.7% | PASS |
| file_reads | `oltp_sum_range` | 13.28ms | 10.76ms | 0.8× | 1.8% | PASS |
| file_reads | `oltp_order_range` | 2.50ms | 2.35ms | 0.9× | 3.0% | PASS |
| file_reads | `oltp_distinct_range` | 3.06ms | 3.06ms | 1.0× | 2.0% | PASS |
| file_reads | `oltp_index_scan` | 4.95ms | 5.10ms | 1.0× | 3.3% | PASS |
| file_reads | `select_random_points` | 19.52ms | 17.14ms | 0.9× | 2.5% | PASS |
| file_reads | `select_random_ranges` | 6.78ms | 4.88ms | 0.7× | 2.8% | PASS |
| file_reads | `covering_index_scan` | 6.71ms | 6.74ms | 1.0× | 1.6% | PASS |
| file_reads | `groupby_scan` | 21.29ms | 23.64ms | 1.1× | 1.2% | PASS |
| file_reads | `index_join` | 10.59ms | 8.06ms | 0.8× | 3.0% | PASS |
| file_reads | `index_join_scan` | 2.98ms | 5.49ms | 1.8× | 3.3% | PASS |
| file_reads | `types_table_scan` | 745.43ms | 871.49ms | 1.2× | 1.6% | PASS |
| file_reads | `table_scan` | 853.13ms | 955.68ms | 1.1× | 0.8% | PASS |
| file_reads | `oltp_read_only` | 117.98ms | 98.23ms | 0.8× | 2.0% | PASS |
| file_writes | `oltp_bulk_insert` | 278.48ms | 332.80ms | 1.2× | 22.2% | PASS |
| file_writes | `oltp_insert` | 51.42ms | 67.78ms | 1.3× | 74.5% | PASS |
| file_writes | `oltp_update_index` | 315.58ms | 232.78ms | 0.7× | 27.6% | PASS |
| file_writes | `oltp_update_non_index` | 189.19ms | 195.21ms | 1.0× | 32.0% | PASS |
| file_writes | `oltp_delete_insert` | 219.30ms | 175.04ms | 0.8× | 31.5% | PASS |
| file_writes | `oltp_write_only` | 215.11ms | 134.50ms | 0.6× | 37.7% | PASS |
| file_writes | `types_delete_insert` | 134.58ms | 150.43ms | 1.1× | 35.1% | PASS |
| file_writes | `oltp_read_write` | 217.63ms | 212.18ms | 1.0× | 37.5% | PASS |
| ac_reads | `oltp_point_select` | 30.35ms | 29.21ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_range_select` | 11.90ms | 10.81ms | 0.9× | 1.9% | PASS |
| ac_reads | `oltp_sum_range` | 12.07ms | 10.85ms | 0.9× | 2.0% | PASS |
| ac_reads | `oltp_order_range` | 2.39ms | 2.36ms | 1.0× | 3.1% | PASS |
| ac_reads | `oltp_distinct_range` | 3.00ms | 3.15ms | 1.1× | 2.3% | PASS |
| ac_reads | `oltp_index_scan` | 3.76ms | 5.26ms | 1.4× | 2.8% | PASS |
| ac_reads | `select_random_points` | 17.99ms | 16.94ms | 0.9× | 2.1% | PASS |
| ac_reads | `select_random_ranges` | 5.31ms | 4.74ms | 0.9× | 3.0% | PASS |
| ac_reads | `covering_index_scan` | 5.27ms | 6.60ms | 1.3× | 1.8% | PASS |
| ac_reads | `groupby_scan` | 21.17ms | 23.68ms | 1.1× | 1.0% | PASS |
| ac_reads | `index_join` | 10.15ms | 8.04ms | 0.8× | 4.1% | PASS |
| ac_reads | `index_join_scan` | 2.87ms | 5.30ms | 1.8× | 5.3% | PASS |
| ac_reads | `types_table_scan` | 740.69ms | 869.11ms | 1.2× | 1.1% | PASS |
| ac_reads | `table_scan` | 852.62ms | 955.42ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_read_only` | 95.04ms | 96.79ms | 1.0× | 1.5% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 139.93ms | 398.31ms | 2.8× | 54.5% | PASS |
| ac_writes | `oltp_insert_ac` | 197.60ms | 496.60ms | 2.5× | 40.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 229.68ms | 559.49ms | 2.4× | 57.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 215.78ms | 536.27ms | 2.5× | 48.4% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 272.83ms | 498.36ms | 1.8× | 57.4% | PASS |
| ac_writes | `oltp_write_only_ac` | 223.41ms | 613.61ms | 2.7× | 77.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 221.39ms | 461.94ms | 2.1× | 65.3% | PASS |
| ac_writes | `oltp_read_write_ac` | 274.20ms | 602.31ms | 2.2× | 52.4% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 32.60ms | 38.22ms | 1.2× | 1.1% | PASS |
| mem_reads | `oltp_range_select` | 18.83ms | 21.75ms | 1.2× | 0.8% | PASS |
| mem_reads | `oltp_sum_range` | 16.40ms | 20.61ms | 1.3× | 0.8% | PASS |
| mem_reads | `oltp_order_range` | 3.58ms | 3.98ms | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_distinct_range` | 4.58ms | 5.11ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 4.47ms | 5.58ms | 1.2× | 1.0% | PASS |
| mem_reads | `select_random_points` | 26.54ms | 30.81ms | 1.2× | 0.7% | PASS |
| mem_reads | `select_random_ranges` | 7.20ms | 8.25ms | 1.1× | 0.9% | PASS |
| mem_reads | `covering_index_scan` | 7.55ms | 9.48ms | 1.3× | 0.9% | PASS |
| mem_reads | `groupby_scan` | 36.94ms | 42.82ms | 1.2× | 0.5% | PASS |
| mem_reads | `index_join` | 7.71ms | 10.05ms | 1.3× | 0.7% | PASS |
| mem_reads | `index_join_scan` | 3.88ms | 5.89ms | 1.5× | 0.6% | PASS |
| mem_reads | `types_table_scan` | 1.14s | 1.32s | 1.2× | 0.9% | PASS |
| mem_reads | `table_scan` | 1.41s | 1.48s | 1.0× | 2.5% | PASS |
| mem_reads | `oltp_read_only` | 150.13ms | 173.23ms | 1.2× | 1.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 244.25ms | 333.79ms | 1.4× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 19.05ms | 35.60ms | 1.9× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 67.37ms | 126.76ms | 1.9× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 55.17ms | 82.38ms | 1.5× | 0.9% | PASS |
| mem_writes | `oltp_delete_insert` | 52.45ms | 102.58ms | 2.0× | 1.6% | PASS |
| mem_writes | `oltp_write_only` | 28.85ms | 61.41ms | 2.1× | 1.4% | PASS |
| mem_writes | `types_delete_insert` | 34.04ms | 54.59ms | 1.6× | 2.1% | PASS |
| mem_writes | `oltp_read_write` | 105.01ms | 157.44ms | 1.5× | 2.4% | PASS |
| file_reads | `oltp_point_select` | 116.99ms | 59.85ms | 0.5× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 28.28ms | 24.43ms | 0.9× | 2.5% | PASS |
| file_reads | `oltp_sum_range` | 25.74ms | 23.32ms | 0.9× | 1.0% | PASS |
| file_reads | `oltp_order_range` | 4.50ms | 4.34ms | 1.0× | 1.4% | PASS |
| file_reads | `oltp_distinct_range` | 5.54ms | 5.51ms | 1.0× | 1.4% | PASS |
| file_reads | `oltp_index_scan` | 13.31ms | 8.46ms | 0.6× | 1.6% | PASS |
| file_reads | `select_random_points` | 37.95ms | 34.82ms | 0.9× | 1.5% | PASS |
| file_reads | `select_random_ranges` | 16.34ms | 10.92ms | 0.7× | 0.9% | PASS |
| file_reads | `covering_index_scan` | 16.75ms | 12.33ms | 0.7× | 1.0% | PASS |
| file_reads | `groupby_scan` | 38.86ms | 43.89ms | 1.1× | 0.6% | PASS |
| file_reads | `index_join` | 13.29ms | 12.63ms | 1.0× | 1.6% | PASS |
| file_reads | `index_join_scan` | 5.33ms | 6.74ms | 1.3× | 3.3% | PASS |
| file_reads | `types_table_scan` | 1.26s | 1.36s | 1.1× | 0.4% | PASS |
| file_reads | `table_scan` | 1.56s | 1.53s | 1.0× | 0.4% | PASS |
| file_reads | `oltp_read_only` | 282.21ms | 209.25ms | 0.7× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 261.86ms | 347.38ms | 1.3× | 0.7% | PASS |
| file_writes | `oltp_insert` | 26.75ms | 42.83ms | 1.6× | 1.6% | PASS |
| file_writes | `oltp_update_index` | 103.06ms | 148.50ms | 1.4× | 2.2% | PASS |
| file_writes | `oltp_update_non_index` | 80.19ms | 98.55ms | 1.2× | 1.6% | PASS |
| file_writes | `oltp_delete_insert` | 78.95ms | 115.45ms | 1.5× | 1.7% | PASS |
| file_writes | `oltp_write_only` | 51.61ms | 73.37ms | 1.4× | 1.9% | PASS |
| file_writes | `types_delete_insert` | 50.76ms | 61.93ms | 1.2× | 1.2% | PASS |
| file_writes | `oltp_read_write` | 128.57ms | 169.12ms | 1.3× | 1.9% | PASS |
| ac_reads | `oltp_point_select` | 62.99ms | 61.40ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_range_select` | 23.82ms | 24.65ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_sum_range` | 20.83ms | 23.58ms | 1.1× | 1.6% | PASS |
| ac_reads | `oltp_order_range` | 4.12ms | 4.34ms | 1.1× | 0.8% | PASS |
| ac_reads | `oltp_distinct_range` | 5.13ms | 5.51ms | 1.1× | 0.8% | PASS |
| ac_reads | `oltp_index_scan` | 7.79ms | 8.43ms | 1.1× | 0.8% | PASS |
| ac_reads | `select_random_points` | 32.01ms | 35.14ms | 1.1× | 1.7% | PASS |
| ac_reads | `select_random_ranges` | 10.47ms | 10.86ms | 1.0× | 0.9% | PASS |
| ac_reads | `covering_index_scan` | 10.89ms | 12.24ms | 1.1× | 0.7% | PASS |
| ac_reads | `groupby_scan` | 37.98ms | 43.61ms | 1.1× | 0.3% | PASS |
| ac_reads | `index_join` | 10.16ms | 12.43ms | 1.2× | 2.2% | PASS |
| ac_reads | `index_join_scan` | 4.63ms | 6.59ms | 1.4× | 2.9% | PASS |
| ac_reads | `types_table_scan` | 1.26s | 1.36s | 1.1× | 0.8% | PASS |
| ac_reads | `table_scan` | 1.54s | 1.52s | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_read_only` | 198.75ms | 207.71ms | 1.0× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 15.66ms | 56.10ms | 3.6× | 4.1% | PASS |
| ac_writes | `oltp_insert_ac` | 17.72ms | 72.76ms | 4.1× | 4.2% | PASS |
| ac_writes | `oltp_update_index_ac` | 19.41ms | 86.59ms | 4.5× | 3.4% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 17.25ms | 67.84ms | 3.9× | 4.4% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 18.02ms | 74.59ms | 4.1× | 3.9% | PASS |
| ac_writes | `oltp_write_only_ac` | 18.59ms | 76.24ms | 4.1× | 4.5% | PASS |
| ac_writes | `types_delete_insert_ac` | 15.43ms | 65.34ms | 4.2× | 4.8% | PASS |
| ac_writes | `oltp_read_write_ac` | 25.12ms | 83.91ms | 3.3× | 3.1% | PASS |

</details>

</details>

## Version-control latency

Wall time: 4m 30s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 33.47ms | 130.00ms | 25.7% | 1.0% | PASS |
| `status_dirty_many_tables` | 36.80ms | 130.00ms | 28.3% | 0.9% | PASS |
| `diff_regular_working_one_table` | 28.61ms | 120.00ms | 23.8% | 0.9% | PASS |
| `diff_regular_working_many_tables` | 41.21ms | 140.00ms | 29.4% | 0.6% | PASS |
| `diff_stat_working_many_tables` | 41.00ms | 140.00ms | 29.3% | 0.7% | PASS |
| `diff_schema_working_many_tables` | 41.46ms | 140.00ms | 29.6% | 0.6% | PASS |
| `branch_list_many_branches` | 21.10ms | 35.00ms | 60.3% | 1.3% | PASS |
| `branch_create_delete` | 23.23ms | 40.00ms | 58.1% | 1.1% | PASS |
| `at_literal_deep_history` | 23.06ms | 100.00ms | 23.1% | 1.0% | PASS |
| `diff_literal_deep_history` | 23.20ms | 120.00ms | 19.3% | 1.0% | PASS |
| `history_literal_deep_history` | 24.13ms | 150.00ms | 16.1% | 1.2% | PASS |
| `checkout_branch_clean` | 82.16ms | 150.00ms | 54.8% | 0.4% | PASS |
| `merge_data_no_conflicts` | 33.25ms | 50.00ms | 66.5% | 2.7% | PASS |
| `merge_data_secondary_index` | 1.47s | 2.50s | 58.9% | 0.4% | PASS |
| `merge_schema_no_conflicts` | 20.98ms | 35.00ms | 60.0% | 1.1% | PASS |
| `merge_data_conflicts` | 28.25ms | 180.00ms | 15.7% | 0.9% | PASS |
| `merge_data_conflicts_with_resolve` | 29.16ms | 180.00ms | 16.2% | 0.9% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
