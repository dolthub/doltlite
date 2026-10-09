# DoltLite Performance Report

> Nightly result: **FAIL**
>
> Generated: 2026-10-09 11:13 UTC
>
> Commit: [`168a34d7f770e327f9100b83a027f655947b6986`](https://github.com/dolthub/doltlite/commit/168a34d7f770e327f9100b83a027f655947b6986)
>
> Runner: ubuntu24 20261004.327.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/37912871169)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.71s | 11.93s | 1.1× | 2.3% | **PASS** |
| Writes | 2.19s | 3.49s | 1.6× | 2.1% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 11.29s | 12.13s | 1.1× | 1.7% | **PASS** |
| Writes | 3.07s | 3.82s | 1.2× | 2.1% | **PASS** |
| Autocommit writes | 740.35ms | 2.34s | 3.2× | 6.1% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.27s | 2.60s | 1.1× | 2.9% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 3.13s | 3.19s | 1.0× | 2.4% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.55s | 3.03s | 1.2× | 1.3% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.76s | 3.11s | 1.1× | 1.9% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 408.54ms | 683.99ms | 1.7× | 2.5% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 601.59ms | 930.35ms | 1.5× | 1.8% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 584.10ms | 940.11ms | 1.6× | 1.5% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 597.94ms | 932.75ms | 1.6× | 2.2% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.52s | 2.68s | 1.1× | 3.0% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 3.09s | 3.18s | 1.0× | 1.0% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.79s | 3.11s | 1.1× | 1.7% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.90s | 3.16s | 1.1× | 1.7% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 553.77ms | 749.27ms | 1.4× | 2.3% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 935.33ms | 1.02s | 1.1× | 3.8% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 818.43ms | 1.04s | 1.3× | 1.8% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 764.04ms | 1.01s | 1.3× | 1.9% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.35s | 2.67s | 1.1× | 2.5% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.86s | 3.16s | 1.1× | 1.2% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.65s | 3.11s | 1.2× | 1.4% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.60s | 3.13s | 1.2× | 1.5% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 172.84ms | 548.98ms | 3.2× | 5.3% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 155.60ms | 520.70ms | 3.3× | 5.8% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 216.87ms | 656.84ms | 3.0× | 6.9% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 195.03ms | 611.22ms | 3.1× | 5.5% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 22.07ms | 30.86ms | 1.4× | 1.7% | PASS |
| mem_reads | `oltp_range_select` | 8.71ms | 11.98ms | 1.4× | 2.9% | PASS |
| mem_reads | `oltp_sum_range` | 8.11ms | 11.09ms | 1.4× | 2.9% | PASS |
| mem_reads | `oltp_order_range` | 2.28ms | 2.78ms | 1.2× | 3.0% | PASS |
| mem_reads | `oltp_distinct_range` | 3.33ms | 3.82ms | 1.1× | 3.3% | PASS |
| mem_reads | `oltp_index_scan` | 3.51ms | 5.32ms | 1.5× | 2.5% | PASS |
| mem_reads | `select_random_points` | 8.43ms | 11.53ms | 1.4× | 3.2% | PASS |
| mem_reads | `select_random_ranges` | 4.10ms | 5.25ms | 1.3× | 3.1% | PASS |
| mem_reads | `covering_index_scan` | 7.41ms | 10.22ms | 1.4× | 2.5% | PASS |
| mem_reads | `groupby_scan` | 28.72ms | 32.64ms | 1.1× | 1.4% | PASS |
| mem_reads | `index_join` | 5.43ms | 8.14ms | 1.5× | 3.6% | PASS |
| mem_reads | `index_join_scan` | 2.61ms | 5.37ms | 2.1× | 3.7% | PASS |
| mem_reads | `types_table_scan` | 976.47ms | 1.11s | 1.1× | 0.8% | PASS |
| mem_reads | `table_scan` | 1.10s | 1.23s | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_read_only` | 96.23ms | 120.54ms | 1.3× | 2.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 172.83ms | 273.65ms | 1.6× | 1.6% | PASS |
| mem_writes | `oltp_insert` | 14.83ms | 26.75ms | 1.8× | 2.5% | PASS |
| mem_writes | `oltp_update_index` | 47.10ms | 86.00ms | 1.8× | 2.3% | PASS |
| mem_writes | `oltp_update_non_index` | 30.86ms | 52.87ms | 1.7× | 2.9% | PASS |
| mem_writes | `oltp_delete_insert` | 40.62ms | 68.64ms | 1.7× | 2.4% | PASS |
| mem_writes | `oltp_write_only` | 19.65ms | 40.59ms | 2.1× | 3.2% | PASS |
| mem_writes | `types_delete_insert` | 22.53ms | 34.96ms | 1.6× | 2.4% | PASS |
| mem_writes | `oltp_read_write` | 60.12ms | 100.53ms | 1.7× | 3.2% | PASS |
| file_reads | `oltp_point_select` | 88.11ms | 48.96ms | 0.6× | 2.0% | PASS |
| file_reads | `oltp_range_select` | 16.34ms | 14.52ms | 0.9× | 3.1% | PASS |
| file_reads | `oltp_sum_range` | 15.78ms | 13.81ms | 0.9× | 3.2% | PASS |
| file_reads | `oltp_order_range` | 3.19ms | 3.18ms | 1.0× | 3.5% | PASS |
| file_reads | `oltp_distinct_range` | 4.11ms | 4.24ms | 1.0× | 3.7% | PASS |
| file_reads | `oltp_index_scan` | 10.46ms | 7.44ms | 0.7× | 3.0% | PASS |
| file_reads | `select_random_points` | 15.65ms | 13.58ms | 0.9× | 4.5% | PASS |
| file_reads | `select_random_ranges` | 11.30ms | 7.28ms | 0.6× | 3.6% | PASS |
| file_reads | `covering_index_scan` | 14.81ms | 12.48ms | 0.8× | 1.9% | PASS |
| file_reads | `groupby_scan` | 29.39ms | 32.63ms | 1.1× | 2.0% | PASS |
| file_reads | `index_join` | 8.97ms | 9.70ms | 1.1× | 2.7% | PASS |
| file_reads | `index_join_scan` | 3.79ms | 5.91ms | 1.6× | 3.6% | PASS |
| file_reads | `types_table_scan` | 988.84ms | 1.13s | 1.1× | 1.4% | PASS |
| file_reads | `table_scan` | 1.11s | 1.23s | 1.1× | 1.1% | PASS |
| file_reads | `oltp_read_only` | 190.44ms | 145.82ms | 0.8× | 2.3% | PASS |
| file_writes | `oltp_bulk_insert` | 184.09ms | 276.61ms | 1.5× | 2.3% | PASS |
| file_writes | `oltp_insert` | 20.90ms | 29.61ms | 1.4× | 3.5% | PASS |
| file_writes | `oltp_update_index` | 69.91ms | 96.28ms | 1.4× | 2.7% | PASS |
| file_writes | `oltp_update_non_index` | 54.35ms | 64.33ms | 1.2× | 3.0% | PASS |
| file_writes | `oltp_delete_insert` | 62.56ms | 80.22ms | 1.3× | 2.2% | PASS |
| file_writes | `oltp_write_only` | 41.84ms | 50.16ms | 1.2× | 1.9% | PASS |
| file_writes | `types_delete_insert` | 36.05ms | 40.76ms | 1.1× | 2.3% | PASS |
| file_writes | `oltp_read_write` | 84.07ms | 111.30ms | 1.3× | 2.2% | PASS |
| ac_reads | `oltp_point_select` | 42.91ms | 48.25ms | 1.1× | 3.0% | PASS |
| ac_reads | `oltp_range_select` | 11.44ms | 14.38ms | 1.3× | 4.4% | PASS |
| ac_reads | `oltp_sum_range` | 10.87ms | 13.28ms | 1.2× | 3.1% | PASS |
| ac_reads | `oltp_order_range` | 2.76ms | 3.20ms | 1.2× | 2.0% | PASS |
| ac_reads | `oltp_distinct_range` | 3.76ms | 4.32ms | 1.1× | 3.4% | PASS |
| ac_reads | `oltp_index_scan` | 6.05ms | 7.61ms | 1.3× | 2.5% | PASS |
| ac_reads | `select_random_points` | 11.32ms | 13.56ms | 1.2× | 2.8% | PASS |
| ac_reads | `select_random_ranges` | 6.62ms | 7.34ms | 1.1× | 2.0% | PASS |
| ac_reads | `covering_index_scan` | 9.71ms | 12.35ms | 1.3× | 3.7% | PASS |
| ac_reads | `groupby_scan` | 27.86ms | 32.25ms | 1.2× | 2.0% | PASS |
| ac_reads | `index_join` | 6.62ms | 9.69ms | 1.5× | 2.5% | PASS |
| ac_reads | `index_join_scan` | 3.26ms | 5.79ms | 1.8× | 3.5% | PASS |
| ac_reads | `types_table_scan` | 974.20ms | 1.12s | 1.2× | 1.0% | PASS |
| ac_reads | `table_scan` | 1.11s | 1.22s | 1.1× | 0.9% | PASS |
| ac_reads | `oltp_read_only` | 128.94ms | 147.39ms | 1.1× | 2.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 19.52ms | 56.74ms | 2.9× | 3.7% | PASS |
| ac_writes | `oltp_insert_ac` | 21.56ms | 68.02ms | 3.2× | 6.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 23.30ms | 79.75ms | 3.4× | 5.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 20.06ms | 63.66ms | 3.2× | 5.1% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 21.64ms | 72.04ms | 3.3× | 4.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 21.52ms | 70.73ms | 3.3× | 3.7% | PASS |
| ac_writes | `types_delete_insert_ac` | 19.62ms | 62.44ms | 3.2× | 6.2% | PASS |
| ac_writes | `oltp_read_write_ac` | 25.64ms | 75.60ms | 2.9× | 6.2% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 35.93ms | 37.11ms | 1.0× | 2.4% | PASS |
| mem_reads | `oltp_range_select` | 17.51ms | 15.46ms | 0.9× | 2.2% | PASS |
| mem_reads | `oltp_sum_range` | 16.21ms | 14.77ms | 0.9× | 3.3% | PASS |
| mem_reads | `oltp_order_range` | 3.56ms | 3.49ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 4.61ms | 4.61ms | 1.0× | 0.8% | PASS |
| mem_reads | `oltp_index_scan` | 4.06ms | 6.44ms | 1.6× | 2.3% | PASS |
| mem_reads | `select_random_points` | 22.89ms | 21.85ms | 1.0× | 5.6% | PASS |
| mem_reads | `select_random_ranges` | 6.84ms | 6.33ms | 0.9× | 2.6% | PASS |
| mem_reads | `covering_index_scan` | 7.61ms | 10.22ms | 1.3× | 1.3% | PASS |
| mem_reads | `groupby_scan` | 37.48ms | 37.44ms | 1.0× | 1.1% | PASS |
| mem_reads | `index_join` | 11.08ms | 10.31ms | 0.9× | 3.1% | PASS |
| mem_reads | `index_join_scan` | 3.86ms | 6.80ms | 1.8× | 4.1% | PASS |
| mem_reads | `types_table_scan` | 1.32s | 1.39s | 1.1× | 3.6% | PASS |
| mem_reads | `table_scan` | 1.49s | 1.49s | 1.0× | 3.2% | PASS |
| mem_reads | `oltp_read_only` | 144.01ms | 142.58ms | 1.0× | 2.4% | PASS |
| mem_writes | `oltp_bulk_insert` | 234.67ms | 319.42ms | 1.4× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 17.99ms | 38.01ms | 2.1× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 68.09ms | 137.40ms | 2.0× | 1.9% | PASS |
| mem_writes | `oltp_update_non_index` | 52.04ms | 82.48ms | 1.6× | 2.4% | PASS |
| mem_writes | `oltp_delete_insert` | 55.64ms | 100.76ms | 1.8× | 1.6% | PASS |
| mem_writes | `oltp_write_only` | 29.44ms | 59.21ms | 2.0× | 1.6% | PASS |
| mem_writes | `types_delete_insert` | 40.34ms | 55.50ms | 1.4× | 2.1% | PASS |
| mem_writes | `oltp_read_write` | 103.40ms | 137.58ms | 1.3× | 2.9% | PASS |
| file_reads | `oltp_point_select` | 123.42ms | 60.11ms | 0.5× | 1.3% | PASS |
| file_reads | `oltp_range_select` | 27.10ms | 17.71ms | 0.7× | 2.4% | PASS |
| file_reads | `oltp_sum_range` | 25.19ms | 17.38ms | 0.7× | 2.3% | PASS |
| file_reads | `oltp_order_range` | 4.44ms | 3.74ms | 0.8× | 0.9% | PASS |
| file_reads | `oltp_distinct_range` | 5.49ms | 4.84ms | 0.9× | 0.8% | PASS |
| file_reads | `oltp_index_scan` | 12.80ms | 8.74ms | 0.7× | 1.0% | PASS |
| file_reads | `select_random_points` | 33.07ms | 24.82ms | 0.8× | 2.1% | PASS |
| file_reads | `select_random_ranges` | 15.41ms | 8.68ms | 0.6× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 16.41ms | 12.51ms | 0.8× | 1.3% | PASS |
| file_reads | `groupby_scan` | 37.94ms | 37.34ms | 1.0× | 1.0% | PASS |
| file_reads | `index_join` | 15.93ms | 11.63ms | 0.7× | 2.3% | PASS |
| file_reads | `index_join_scan` | 4.87ms | 7.01ms | 1.4× | 2.3% | PASS |
| file_reads | `types_table_scan` | 1.16s | 1.34s | 1.2× | 0.5% | PASS |
| file_reads | `table_scan` | 1.34s | 1.46s | 1.1× | 0.7% | PASS |
| file_reads | `oltp_read_only` | 261.62ms | 173.02ms | 0.7× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 263.34ms | 332.50ms | 1.3× | 0.8% | PASS |
| file_writes | `oltp_insert` | 25.38ms | 42.92ms | 1.7× | 1.8% | PASS |
| file_writes | `oltp_update_index` | 131.36ms | 153.80ms | 1.2× | 13.8% | PASS |
| file_writes | `oltp_update_non_index` | 102.07ms | 95.13ms | 0.9× | 7.3% | PASS |
| file_writes | `oltp_delete_insert` | 98.49ms | 115.56ms | 1.2× | 2.1% | PASS |
| file_writes | `oltp_write_only` | 90.53ms | 70.49ms | 0.8× | 10.1% | PASS |
| file_writes | `types_delete_insert` | 73.21ms | 66.25ms | 0.9× | 1.5% | PASS |
| file_writes | `oltp_read_write` | 150.96ms | 145.53ms | 1.0× | 5.4% | PASS |
| ac_reads | `oltp_point_select` | 63.87ms | 59.22ms | 0.9× | 1.4% | PASS |
| ac_reads | `oltp_range_select` | 21.14ms | 17.67ms | 0.8× | 1.8% | PASS |
| ac_reads | `oltp_sum_range` | 19.38ms | 17.23ms | 0.9× | 2.0% | PASS |
| ac_reads | `oltp_order_range` | 3.92ms | 3.73ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_distinct_range` | 4.88ms | 4.85ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_index_scan` | 7.13ms | 8.67ms | 1.2× | 1.4% | PASS |
| ac_reads | `select_random_points` | 25.52ms | 23.99ms | 0.9× | 1.7% | PASS |
| ac_reads | `select_random_ranges` | 9.68ms | 8.65ms | 0.9× | 0.9% | PASS |
| ac_reads | `covering_index_scan` | 10.82ms | 12.49ms | 1.2× | 1.0% | PASS |
| ac_reads | `groupby_scan` | 37.30ms | 37.30ms | 1.0× | 0.7% | PASS |
| ac_reads | `index_join` | 13.28ms | 11.64ms | 0.9× | 1.9% | PASS |
| ac_reads | `index_join_scan` | 4.32ms | 7.05ms | 1.6× | 1.4% | PASS |
| ac_reads | `types_table_scan` | 1.16s | 1.34s | 1.2× | 0.9% | PASS |
| ac_reads | `table_scan` | 1.30s | 1.44s | 1.1× | 0.7% | PASS |
| ac_reads | `oltp_read_only` | 178.75ms | 172.38ms | 1.0× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 17.25ms | 48.42ms | 2.8× | 6.9% | PASS |
| ac_writes | `oltp_insert_ac` | 19.47ms | 62.29ms | 3.2× | 4.7% | PASS |
| ac_writes | `oltp_update_index_ac` | 20.52ms | 79.39ms | 3.9× | 5.4% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 17.46ms | 59.36ms | 3.4× | 6.5% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 19.70ms | 70.56ms | 3.6× | 5.8% | PASS |
| ac_writes | `oltp_write_only_ac` | 19.35ms | 67.78ms | 3.5× | 5.8% | PASS |
| ac_writes | `types_delete_insert_ac` | 16.91ms | 59.14ms | 3.5× | 6.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 24.95ms | 73.75ms | 3.0× | 3.7% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 34.26ms | 40.39ms | 1.2× | 2.1% | PASS |
| mem_reads | `oltp_range_select` | 15.58ms | 15.91ms | 1.0× | 2.5% | PASS |
| mem_reads | `oltp_sum_range` | 14.76ms | 15.40ms | 1.0× | 3.0% | PASS |
| mem_reads | `oltp_order_range` | 3.23ms | 3.47ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_distinct_range` | 4.30ms | 4.60ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 3.95ms | 6.82ms | 1.7× | 1.3% | PASS |
| mem_reads | `select_random_points` | 21.55ms | 22.65ms | 1.1× | 3.3% | PASS |
| mem_reads | `select_random_ranges` | 6.50ms | 6.95ms | 1.1× | 1.2% | PASS |
| mem_reads | `covering_index_scan` | 7.64ms | 10.89ms | 1.4× | 0.9% | PASS |
| mem_reads | `groupby_scan` | 33.40ms | 36.60ms | 1.1× | 0.7% | PASS |
| mem_reads | `index_join` | 10.09ms | 10.84ms | 1.1× | 2.2% | PASS |
| mem_reads | `index_join_scan` | 3.72ms | 6.50ms | 1.7× | 2.5% | PASS |
| mem_reads | `types_table_scan` | 1.06s | 1.30s | 1.2× | 0.7% | PASS |
| mem_reads | `table_scan` | 1.20s | 1.40s | 1.2× | 1.3% | PASS |
| mem_reads | `oltp_read_only` | 135.59ms | 148.87ms | 1.1× | 1.4% | PASS |
| mem_writes | `oltp_bulk_insert` | 239.14ms | 341.28ms | 1.4× | 1.1% | PASS |
| mem_writes | `oltp_insert` | 18.22ms | 37.91ms | 2.1× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 63.91ms | 129.91ms | 2.0× | 1.9% | PASS |
| mem_writes | `oltp_update_non_index` | 47.20ms | 79.26ms | 1.7× | 1.7% | PASS |
| mem_writes | `oltp_delete_insert` | 52.64ms | 97.61ms | 1.9× | 1.3% | PASS |
| mem_writes | `oltp_write_only` | 27.98ms | 56.52ms | 2.0× | 1.4% | PASS |
| mem_writes | `types_delete_insert` | 38.64ms | 55.49ms | 1.4× | 3.0% | PASS |
| mem_writes | `oltp_read_write` | 96.38ms | 142.14ms | 1.5× | 2.2% | PASS |
| file_reads | `oltp_point_select` | 105.74ms | 59.59ms | 0.6× | 1.3% | PASS |
| file_reads | `oltp_range_select` | 23.64ms | 17.96ms | 0.8× | 2.4% | PASS |
| file_reads | `oltp_sum_range` | 22.76ms | 17.66ms | 0.8× | 2.3% | PASS |
| file_reads | `oltp_order_range` | 4.30ms | 3.77ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_distinct_range` | 5.48ms | 4.92ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_index_scan` | 11.23ms | 8.85ms | 0.8× | 1.5% | PASS |
| file_reads | `select_random_points` | 30.68ms | 25.79ms | 0.8× | 2.5% | PASS |
| file_reads | `select_random_ranges` | 14.09ms | 9.09ms | 0.6× | 2.0% | PASS |
| file_reads | `covering_index_scan` | 15.03ms | 13.26ms | 0.9× | 0.9% | PASS |
| file_reads | `groupby_scan` | 34.75ms | 37.04ms | 1.1× | 1.1% | PASS |
| file_reads | `index_join` | 14.47ms | 12.03ms | 0.8× | 1.7% | PASS |
| file_reads | `index_join_scan` | 4.77ms | 6.82ms | 1.4× | 2.4% | PASS |
| file_reads | `types_table_scan` | 1.05s | 1.32s | 1.3× | 1.2% | PASS |
| file_reads | `table_scan` | 1.20s | 1.40s | 1.2× | 1.0% | PASS |
| file_reads | `oltp_read_only` | 245.85ms | 178.68ms | 0.7× | 1.2% | PASS |
| file_writes | `oltp_bulk_insert` | 261.16ms | 354.16ms | 1.4× | 1.1% | PASS |
| file_writes | `oltp_insert` | 25.52ms | 43.04ms | 1.7× | 1.8% | PASS |
| file_writes | `oltp_update_index` | 97.70ms | 147.20ms | 1.5× | 1.6% | PASS |
| file_writes | `oltp_update_non_index` | 95.31ms | 92.87ms | 1.0× | 8.8% | PASS |
| file_writes | `oltp_delete_insert` | 87.35ms | 112.51ms | 1.3× | 1.2% | PASS |
| file_writes | `oltp_write_only` | 57.62ms | 68.89ms | 1.2× | 2.0% | PASS |
| file_writes | `types_delete_insert` | 63.64ms | 65.77ms | 1.0× | 1.7% | PASS |
| file_writes | `oltp_read_write` | 130.13ms | 155.48ms | 1.2× | 2.1% | PASS |
| ac_reads | `oltp_point_select` | 59.22ms | 59.81ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_range_select` | 19.01ms | 18.13ms | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_sum_range` | 17.89ms | 17.73ms | 1.0× | 1.8% | PASS |
| ac_reads | `oltp_order_range` | 3.84ms | 3.73ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_distinct_range` | 4.92ms | 4.90ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_index_scan` | 6.69ms | 8.88ms | 1.3× | 1.9% | PASS |
| ac_reads | `select_random_points` | 25.51ms | 25.80ms | 1.0× | 2.9% | PASS |
| ac_reads | `select_random_ranges` | 9.30ms | 9.10ms | 1.0× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 10.59ms | 13.35ms | 1.3× | 1.0% | PASS |
| ac_reads | `groupby_scan` | 34.32ms | 36.97ms | 1.1× | 0.9% | PASS |
| ac_reads | `index_join` | 12.29ms | 12.16ms | 1.0× | 2.4% | PASS |
| ac_reads | `index_join_scan` | 4.26ms | 6.90ms | 1.6× | 2.1% | PASS |
| ac_reads | `types_table_scan` | 1.06s | 1.32s | 1.2× | 1.0% | PASS |
| ac_reads | `table_scan` | 1.21s | 1.40s | 1.2× | 0.9% | PASS |
| ac_reads | `oltp_read_only` | 174.69ms | 178.83ms | 1.0× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 24.13ms | 62.95ms | 2.6× | 6.9% | PASS |
| ac_writes | `oltp_insert_ac` | 26.58ms | 83.91ms | 3.2× | 6.8% | PASS |
| ac_writes | `oltp_update_index_ac` | 28.83ms | 96.90ms | 3.4× | 6.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.99ms | 73.38ms | 3.2× | 5.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 27.31ms | 84.48ms | 3.1× | 9.5% | PASS |
| ac_writes | `oltp_write_only_ac` | 27.47ms | 84.02ms | 3.1× | 7.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 25.79ms | 81.90ms | 3.2× | 9.3% | PASS |
| ac_writes | `oltp_read_write_ac` | 33.77ms | 89.29ms | 2.6× | 6.9% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.27ms | 42.44ms | 1.3× | 2.3% | PASS |
| mem_reads | `oltp_range_select` | 19.24ms | 23.46ms | 1.2× | 1.7% | PASS |
| mem_reads | `oltp_sum_range` | 17.75ms | 22.03ms | 1.2× | 1.7% | PASS |
| mem_reads | `oltp_order_range` | 3.56ms | 4.17ms | 1.2× | 1.5% | PASS |
| mem_reads | `oltp_distinct_range` | 4.67ms | 5.31ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_index_scan` | 4.53ms | 6.55ms | 1.4× | 2.4% | PASS |
| mem_reads | `select_random_points` | 29.38ms | 34.30ms | 1.2× | 2.6% | PASS |
| mem_reads | `select_random_ranges` | 7.56ms | 9.37ms | 1.2× | 1.9% | PASS |
| mem_reads | `covering_index_scan` | 7.63ms | 10.71ms | 1.4× | 1.8% | PASS |
| mem_reads | `groupby_scan` | 36.15ms | 41.13ms | 1.1× | 1.2% | PASS |
| mem_reads | `index_join` | 7.98ms | 11.97ms | 1.5× | 3.2% | PASS |
| mem_reads | `index_join_scan` | 4.02ms | 6.66ms | 1.7× | 4.1% | PASS |
| mem_reads | `types_table_scan` | 1.11s | 1.29s | 1.2× | 2.6% | PASS |
| mem_reads | `table_scan` | 1.32s | 1.41s | 1.1× | 3.1% | PASS |
| mem_reads | `oltp_read_only` | 155.59ms | 186.78ms | 1.2× | 1.7% | PASS |
| mem_writes | `oltp_bulk_insert` | 242.50ms | 328.19ms | 1.4× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 18.79ms | 34.02ms | 1.8× | 1.5% | PASS |
| mem_writes | `oltp_update_index` | 68.43ms | 123.29ms | 1.8× | 2.3% | PASS |
| mem_writes | `oltp_update_non_index` | 52.41ms | 82.87ms | 1.6× | 2.0% | PASS |
| mem_writes | `oltp_delete_insert` | 50.57ms | 93.34ms | 1.8× | 2.2% | PASS |
| mem_writes | `oltp_write_only` | 28.23ms | 55.38ms | 2.0× | 2.3% | PASS |
| mem_writes | `types_delete_insert` | 33.13ms | 54.27ms | 1.6× | 2.5% | PASS |
| mem_writes | `oltp_read_write` | 103.87ms | 161.38ms | 1.6× | 2.5% | PASS |
| file_reads | `oltp_point_select` | 105.49ms | 63.17ms | 0.6× | 1.1% | PASS |
| file_reads | `oltp_range_select` | 27.64ms | 26.06ms | 0.9× | 2.0% | PASS |
| file_reads | `oltp_sum_range` | 25.71ms | 24.46ms | 1.0× | 1.7% | PASS |
| file_reads | `oltp_order_range` | 4.46ms | 4.50ms | 1.0× | 2.1% | PASS |
| file_reads | `oltp_distinct_range` | 5.66ms | 5.64ms | 1.0× | 1.4% | PASS |
| file_reads | `oltp_index_scan` | 11.88ms | 8.82ms | 0.7× | 1.6% | PASS |
| file_reads | `select_random_points` | 38.38ms | 38.20ms | 1.0× | 1.8% | PASS |
| file_reads | `select_random_ranges` | 15.46ms | 11.75ms | 0.8× | 1.7% | PASS |
| file_reads | `covering_index_scan` | 15.16ms | 13.07ms | 0.9× | 1.3% | PASS |
| file_reads | `groupby_scan` | 37.01ms | 41.71ms | 1.1× | 1.0% | PASS |
| file_reads | `index_join` | 12.33ms | 13.64ms | 1.1× | 2.8% | PASS |
| file_reads | `index_join_scan` | 4.96ms | 6.83ms | 1.4× | 3.0% | PASS |
| file_reads | `types_table_scan` | 1.05s | 1.29s | 1.2× | 1.0% | PASS |
| file_reads | `table_scan` | 1.29s | 1.40s | 1.1× | 2.9% | PASS |
| file_reads | `oltp_read_only` | 256.96ms | 213.45ms | 0.8× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 260.00ms | 338.78ms | 1.3× | 0.8% | PASS |
| file_writes | `oltp_insert` | 25.62ms | 37.40ms | 1.5× | 1.8% | PASS |
| file_writes | `oltp_update_index` | 96.42ms | 135.19ms | 1.4× | 2.1% | PASS |
| file_writes | `oltp_update_non_index` | 78.87ms | 93.87ms | 1.2× | 1.8% | PASS |
| file_writes | `oltp_delete_insert` | 80.34ms | 108.41ms | 1.3× | 1.9% | PASS |
| file_writes | `oltp_write_only` | 52.75ms | 66.03ms | 1.3× | 2.3% | PASS |
| file_writes | `types_delete_insert` | 48.40ms | 61.44ms | 1.3× | 2.1% | PASS |
| file_writes | `oltp_read_write` | 121.64ms | 165.57ms | 1.4× | 1.5% | PASS |
| ac_reads | `oltp_point_select` | 55.93ms | 62.05ms | 1.1× | 2.1% | PASS |
| ac_reads | `oltp_range_select` | 21.64ms | 25.80ms | 1.2× | 2.2% | PASS |
| ac_reads | `oltp_sum_range` | 20.39ms | 24.41ms | 1.2× | 1.5% | PASS |
| ac_reads | `oltp_order_range` | 3.95ms | 4.49ms | 1.1× | 1.5% | PASS |
| ac_reads | `oltp_distinct_range` | 5.05ms | 5.64ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 6.68ms | 8.61ms | 1.3× | 1.7% | PASS |
| ac_reads | `select_random_points` | 29.85ms | 36.87ms | 1.2× | 1.4% | PASS |
| ac_reads | `select_random_ranges` | 9.62ms | 11.54ms | 1.2× | 1.5% | PASS |
| ac_reads | `covering_index_scan` | 10.01ms | 12.82ms | 1.3× | 1.2% | PASS |
| ac_reads | `groupby_scan` | 35.50ms | 41.35ms | 1.2× | 0.8% | PASS |
| ac_reads | `index_join` | 9.16ms | 13.13ms | 1.4× | 1.5% | PASS |
| ac_reads | `index_join_scan` | 4.12ms | 6.76ms | 1.6× | 1.9% | PASS |
| ac_reads | `types_table_scan` | 1.04s | 1.28s | 1.2× | 0.5% | PASS |
| ac_reads | `table_scan` | 1.17s | 1.38s | 1.2× | 0.5% | PASS |
| ac_reads | `oltp_read_only` | 184.65ms | 213.18ms | 1.2× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.10ms | 63.31ms | 2.9× | 7.3% | PASS |
| ac_writes | `oltp_insert_ac` | 24.76ms | 76.33ms | 3.1× | 6.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 25.34ms | 87.03ms | 3.4× | 4.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.52ms | 70.44ms | 3.1× | 4.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 23.63ms | 77.65ms | 3.3× | 5.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 24.12ms | 79.41ms | 3.3× | 6.5% | PASS |
| ac_writes | `types_delete_insert_ac` | 21.34ms | 71.19ms | 3.3× | 8.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.20ms | 85.85ms | 2.8× | 4.6% | PASS |

</details>

</details>

## Version-control latency

Wall time: 18m 56s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 31.07ms | 38.00ms | 81.8% | 1.3% | PASS |
| `status_dirty_many_tables` | 34.28ms | 42.00ms | 81.6% | 1.4% | PASS |
| `diff_regular_working_one_table` | 33.01ms | 33.00ms | 100.0% | 1.3% | FAIL |
| `diff_regular_working_many_tables` | 38.19ms | 50.00ms | 76.4% | 2.3% | PASS |
| `diff_stat_working_many_tables` | 37.87ms | 48.00ms | 78.9% | 2.3% | PASS |
| `diff_schema_working_many_tables` | 38.33ms | 48.00ms | 79.9% | 2.1% | PASS |
| `branch_list_many_branches` | 18.93ms | 25.00ms | 75.7% | 2.0% | PASS |
| `branch_create_delete` | 20.50ms | 27.00ms | 75.9% | 1.8% | PASS |
| `at_literal_deep_history` | 20.05ms | 28.00ms | 71.6% | 2.2% | PASS |
| `diff_literal_deep_history` | 20.02ms | 28.00ms | 71.5% | 2.2% | PASS |
| `history_literal_deep_history` | 20.97ms | 30.00ms | 69.9% | 1.7% | PASS |
| `checkout_branch_clean` | 29.65ms | 43.00ms | 69.0% | 1.5% | PASS |
| `merge_data_no_conflicts` | 22.95ms | 33.00ms | 69.5% | 1.8% | PASS |
| `merge_data_secondary_index` | 765.80ms | 944.00ms | 81.1% | 1.4% | PASS |
| `merge_schema_no_conflicts` | 19.49ms | 24.00ms | 81.2% | 1.5% | PASS |
| `merge_data_conflicts` | 25.67ms | 33.00ms | 77.8% | 1.5% | PASS |
| `merge_data_conflicts_with_resolve` | 26.40ms | 34.00ms | 77.7% | 1.8% | PASS |

Version-control ceiling result: **FAIL**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
