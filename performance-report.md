# DoltLite Performance Report

> Nightly result: **FAIL**
>
> Generated: 2026-10-05 11:23 UTC
>
> Commit: [`309ef0b2b67e9db8b9e25b83692e31f649cd22fd`](https://github.com/dolthub/doltlite/commit/309ef0b2b67e9db8b9e25b83692e31f649cd22fd)
>
> Runner: ubuntu24 20260927.320.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/37292582430)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.13s | 11.55s | 1.1× | 1.7% | **PASS** |
| Writes | 2.11s | 3.28s | 1.6× | 1.5% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 11.36s | 11.90s | 1.0× | 1.5% | **PASS** |
| Writes | 3.55s | 4.06s | 1.1× | 2.1% | **PASS** |
| Autocommit writes | 832.26ms | 2.58s | 3.1× | 7.9% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.57s | 2.83s | 1.1× | 1.9% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.75s | 3.11s | 1.1× | 2.0% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.15s | 2.48s | 1.2× | 1.6% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.67s | 3.14s | 1.2× | 1.7% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 442.62ms | 724.66ms | 1.6× | 1.5% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 612.58ms | 964.07ms | 1.6× | 1.5% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 458.89ms | 675.14ms | 1.5× | 1.4% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 596.55ms | 919.52ms | 1.5× | 1.4% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.81s | 2.89s | 1.0× | 1.7% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 3.28s | 3.26s | 1.0× | 1.7% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.37s | 2.54s | 1.1× | 1.1% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.90s | 3.20s | 1.1× | 1.5% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 604.03ms | 797.65ms | 1.3× | 1.9% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 971.16ms | 1.08s | 1.1× | 4.0% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 1.20s | 1.17s | 1.0× | 12.0% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 776.48ms | 1.01s | 1.3× | 1.7% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.69s | 2.90s | 1.1× | 1.3% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.93s | 3.19s | 1.1× | 1.7% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.24s | 2.55s | 1.1× | 1.2% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.82s | 3.21s | 1.1× | 1.4% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 202.00ms | 639.65ms | 3.2× | 7.1% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 209.82ms | 630.54ms | 3.0× | 7.8% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 209.94ms | 681.24ms | 3.2× | 29.1% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 210.50ms | 629.49ms | 3.0× | 7.2% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 25.08ms | 31.77ms | 1.3× | 2.4% | PASS |
| mem_reads | `oltp_range_select` | 10.75ms | 12.21ms | 1.1× | 2.9% | PASS |
| mem_reads | `oltp_sum_range` | 9.97ms | 12.11ms | 1.2× | 2.3% | PASS |
| mem_reads | `oltp_order_range` | 2.60ms | 2.99ms | 1.2× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 3.69ms | 4.22ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_index_scan` | 4.01ms | 5.83ms | 1.5× | 1.9% | PASS |
| mem_reads | `select_random_points` | 10.63ms | 12.36ms | 1.2× | 3.8% | PASS |
| mem_reads | `select_random_ranges` | 4.69ms | 5.38ms | 1.1× | 1.9% | PASS |
| mem_reads | `covering_index_scan` | 7.62ms | 10.70ms | 1.4× | 0.8% | PASS |
| mem_reads | `groupby_scan` | 29.84ms | 33.24ms | 1.1× | 0.7% | PASS |
| mem_reads | `index_join` | 5.78ms | 9.10ms | 1.6× | 1.9% | PASS |
| mem_reads | `index_join_scan` | 3.41ms | 5.85ms | 1.7× | 3.3% | PASS |
| mem_reads | `types_table_scan` | 1.08s | 1.22s | 1.1× | 1.7% | PASS |
| mem_reads | `table_scan` | 1.26s | 1.33s | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_read_only` | 108.88ms | 127.39ms | 1.2× | 1.9% | PASS |
| mem_writes | `oltp_bulk_insert` | 177.64ms | 279.11ms | 1.6× | 1.2% | PASS |
| mem_writes | `oltp_insert` | 15.22ms | 28.12ms | 1.8× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 50.47ms | 92.78ms | 1.8× | 1.4% | PASS |
| mem_writes | `oltp_update_non_index` | 35.79ms | 58.23ms | 1.6× | 1.9% | PASS |
| mem_writes | `oltp_delete_insert` | 46.66ms | 75.46ms | 1.6× | 1.7% | PASS |
| mem_writes | `oltp_write_only` | 21.95ms | 44.38ms | 2.0× | 1.4% | PASS |
| mem_writes | `types_delete_insert` | 25.28ms | 38.05ms | 1.5× | 2.0% | PASS |
| mem_writes | `oltp_read_write` | 69.61ms | 108.52ms | 1.6× | 2.6% | PASS |
| file_reads | `oltp_point_select` | 95.43ms | 50.63ms | 0.5× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 18.70ms | 14.29ms | 0.8× | 1.8% | PASS |
| file_reads | `oltp_sum_range` | 17.35ms | 14.08ms | 0.8× | 2.5% | PASS |
| file_reads | `oltp_order_range` | 3.52ms | 3.28ms | 0.9× | 2.1% | PASS |
| file_reads | `oltp_distinct_range` | 4.56ms | 4.48ms | 1.0× | 1.2% | PASS |
| file_reads | `oltp_index_scan` | 11.43ms | 7.83ms | 0.7× | 1.9% | PASS |
| file_reads | `select_random_points` | 18.95ms | 14.40ms | 0.8× | 3.7% | PASS |
| file_reads | `select_random_ranges` | 12.05ms | 7.45ms | 0.6× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 15.30ms | 12.68ms | 0.8× | 1.3% | PASS |
| file_reads | `groupby_scan` | 30.86ms | 33.55ms | 1.1× | 0.8% | PASS |
| file_reads | `index_join` | 9.97ms | 10.33ms | 1.0× | 1.7% | PASS |
| file_reads | `index_join_scan` | 4.26ms | 6.08ms | 1.4× | 2.8% | PASS |
| file_reads | `types_table_scan` | 1.10s | 1.22s | 1.1× | 1.5% | PASS |
| file_reads | `table_scan` | 1.26s | 1.33s | 1.1× | 2.0% | PASS |
| file_reads | `oltp_read_only` | 214.31ms | 157.22ms | 0.7× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 192.58ms | 289.30ms | 1.5× | 1.1% | PASS |
| file_writes | `oltp_insert` | 22.15ms | 31.36ms | 1.4× | 1.5% | PASS |
| file_writes | `oltp_update_index` | 78.36ms | 105.15ms | 1.3× | 1.7% | PASS |
| file_writes | `oltp_update_non_index` | 60.20ms | 68.11ms | 1.1× | 2.1% | PASS |
| file_writes | `oltp_delete_insert` | 67.25ms | 85.15ms | 1.3× | 1.4% | PASS |
| file_writes | `oltp_write_only` | 46.01ms | 54.64ms | 1.2× | 2.4% | PASS |
| file_writes | `types_delete_insert` | 41.04ms | 44.14ms | 1.1× | 2.1% | PASS |
| file_writes | `oltp_read_write` | 96.45ms | 119.81ms | 1.2× | 2.1% | PASS |
| ac_reads | `oltp_point_select` | 48.31ms | 50.83ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 13.35ms | 14.11ms | 1.1× | 2.9% | PASS |
| ac_reads | `oltp_sum_range` | 12.75ms | 14.12ms | 1.1× | 2.2% | PASS |
| ac_reads | `oltp_order_range` | 2.98ms | 3.25ms | 1.1× | 1.6% | PASS |
| ac_reads | `oltp_distinct_range` | 4.06ms | 4.49ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_index_scan` | 6.81ms | 7.82ms | 1.1× | 1.4% | PASS |
| ac_reads | `select_random_points` | 13.55ms | 14.07ms | 1.0× | 3.9% | PASS |
| ac_reads | `select_random_ranges` | 7.33ms | 7.45ms | 1.0× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 10.67ms | 12.66ms | 1.2× | 1.3% | PASS |
| ac_reads | `groupby_scan` | 30.00ms | 33.45ms | 1.1× | 0.9% | PASS |
| ac_reads | `index_join` | 7.73ms | 10.49ms | 1.4× | 2.2% | PASS |
| ac_reads | `index_join_scan` | 3.84ms | 6.20ms | 1.6× | 4.3% | PASS |
| ac_reads | `types_table_scan` | 1.11s | 1.23s | 1.1× | 0.8% | PASS |
| ac_reads | `table_scan` | 1.28s | 1.34s | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_read_only` | 142.06ms | 155.80ms | 1.1× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 23.60ms | 67.27ms | 2.9× | 8.4% | PASS |
| ac_writes | `oltp_insert_ac` | 25.02ms | 77.51ms | 3.1× | 4.6% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.89ms | 92.37ms | 3.4× | 7.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.61ms | 72.67ms | 3.2× | 8.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.61ms | 81.62ms | 3.3× | 5.7% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.72ms | 82.41ms | 3.2× | 7.0% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.56ms | 75.54ms | 3.3× | 9.0% | PASS |
| ac_writes | `oltp_read_write_ac` | 30.99ms | 90.27ms | 2.9× | 4.6% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 36.91ms | 40.76ms | 1.1× | 2.8% | PASS |
| mem_reads | `oltp_range_select` | 18.12ms | 16.11ms | 0.9× | 2.2% | PASS |
| mem_reads | `oltp_sum_range` | 16.66ms | 15.67ms | 0.9× | 2.2% | PASS |
| mem_reads | `oltp_order_range` | 3.40ms | 3.46ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 4.45ms | 4.61ms | 1.0× | 0.8% | PASS |
| mem_reads | `oltp_index_scan` | 4.03ms | 7.08ms | 1.8× | 1.0% | PASS |
| mem_reads | `select_random_points` | 23.59ms | 23.65ms | 1.0× | 3.2% | PASS |
| mem_reads | `select_random_ranges` | 6.97ms | 6.99ms | 1.0× | 1.8% | PASS |
| mem_reads | `covering_index_scan` | 7.76ms | 10.85ms | 1.4× | 1.3% | PASS |
| mem_reads | `groupby_scan` | 35.26ms | 36.48ms | 1.0× | 0.8% | PASS |
| mem_reads | `index_join` | 11.20ms | 10.80ms | 1.0× | 2.6% | PASS |
| mem_reads | `index_join_scan` | 3.76ms | 6.52ms | 1.7× | 1.9% | PASS |
| mem_reads | `types_table_scan` | 1.13s | 1.34s | 1.2× | 3.5% | PASS |
| mem_reads | `table_scan` | 1.30s | 1.44s | 1.1× | 2.2% | PASS |
| mem_reads | `oltp_read_only` | 144.85ms | 150.39ms | 1.0× | 2.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 237.40ms | 336.60ms | 1.4× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 17.93ms | 38.85ms | 2.2× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 66.55ms | 137.04ms | 2.1× | 1.3% | PASS |
| mem_writes | `oltp_update_non_index` | 50.30ms | 82.28ms | 1.6× | 2.7% | PASS |
| mem_writes | `oltp_delete_insert` | 57.25ms | 104.46ms | 1.8× | 1.5% | PASS |
| mem_writes | `oltp_write_only` | 29.20ms | 58.57ms | 2.0× | 1.4% | PASS |
| mem_writes | `types_delete_insert` | 41.02ms | 57.76ms | 1.4× | 2.1% | PASS |
| mem_writes | `oltp_read_write` | 112.94ms | 148.51ms | 1.3× | 2.6% | PASS |
| file_reads | `oltp_point_select` | 109.96ms | 62.17ms | 0.6× | 1.4% | PASS |
| file_reads | `oltp_range_select` | 25.96ms | 18.79ms | 0.7× | 1.7% | PASS |
| file_reads | `oltp_sum_range` | 25.02ms | 18.34ms | 0.7× | 1.9% | PASS |
| file_reads | `oltp_order_range` | 4.49ms | 3.76ms | 0.8× | 2.3% | PASS |
| file_reads | `oltp_distinct_range` | 5.62ms | 5.03ms | 0.9× | 2.8% | PASS |
| file_reads | `oltp_index_scan` | 11.63ms | 9.41ms | 0.8× | 1.6% | PASS |
| file_reads | `select_random_points` | 34.91ms | 27.54ms | 0.8× | 3.6% | PASS |
| file_reads | `select_random_ranges` | 14.83ms | 9.25ms | 0.6× | 1.3% | PASS |
| file_reads | `covering_index_scan` | 15.91ms | 13.41ms | 0.8× | 2.4% | PASS |
| file_reads | `groupby_scan` | 36.56ms | 36.80ms | 1.0× | 1.0% | PASS |
| file_reads | `index_join` | 16.30ms | 12.32ms | 0.8× | 2.2% | PASS |
| file_reads | `index_join_scan` | 4.96ms | 7.10ms | 1.4× | 2.8% | PASS |
| file_reads | `types_table_scan` | 1.27s | 1.38s | 1.1× | 0.8% | PASS |
| file_reads | `table_scan` | 1.44s | 1.47s | 1.0× | 0.6% | PASS |
| file_reads | `oltp_read_only` | 260.85ms | 187.01ms | 0.7× | 1.2% | PASS |
| file_writes | `oltp_bulk_insert` | 262.37ms | 352.14ms | 1.3× | 1.4% | PASS |
| file_writes | `oltp_insert` | 26.09ms | 44.19ms | 1.7× | 1.4% | PASS |
| file_writes | `oltp_update_index` | 138.49ms | 158.31ms | 1.1× | 5.9% | PASS |
| file_writes | `oltp_update_non_index` | 102.41ms | 96.41ms | 0.9× | 9.9% | PASS |
| file_writes | `oltp_delete_insert` | 100.42ms | 120.19ms | 1.2× | 1.9% | PASS |
| file_writes | `oltp_write_only` | 90.57ms | 72.28ms | 0.8× | 9.2% | PASS |
| file_writes | `types_delete_insert` | 77.06ms | 69.84ms | 0.9× | 2.1% | PASS |
| file_writes | `oltp_read_write` | 173.75ms | 163.43ms | 0.9× | 8.1% | PASS |
| ac_reads | `oltp_point_select` | 62.52ms | 62.71ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_range_select` | 21.53ms | 18.73ms | 0.9× | 2.4% | PASS |
| ac_reads | `oltp_sum_range` | 19.62ms | 18.14ms | 0.9× | 1.5% | PASS |
| ac_reads | `oltp_order_range` | 4.03ms | 3.83ms | 1.0× | 2.2% | PASS |
| ac_reads | `oltp_distinct_range` | 5.17ms | 5.07ms | 1.0× | 2.8% | PASS |
| ac_reads | `oltp_index_scan` | 6.91ms | 9.36ms | 1.4× | 1.5% | PASS |
| ac_reads | `select_random_points` | 28.11ms | 27.43ms | 1.0× | 2.7% | PASS |
| ac_reads | `select_random_ranges` | 9.64ms | 9.17ms | 1.0× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 10.90ms | 13.10ms | 1.2× | 1.3% | PASS |
| ac_reads | `groupby_scan` | 35.64ms | 36.68ms | 1.0× | 0.7% | PASS |
| ac_reads | `index_join` | 13.60ms | 12.31ms | 0.9× | 3.1% | PASS |
| ac_reads | `index_join_scan` | 4.37ms | 6.92ms | 1.6× | 1.7% | PASS |
| ac_reads | `types_table_scan` | 1.14s | 1.34s | 1.2× | 4.5% | PASS |
| ac_reads | `table_scan` | 1.38s | 1.45s | 1.0× | 3.1% | PASS |
| ac_reads | `oltp_read_only` | 185.17ms | 184.02ms | 1.0× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 23.23ms | 62.17ms | 2.7× | 7.7% | PASS |
| ac_writes | `oltp_insert_ac` | 26.73ms | 76.77ms | 2.9× | 7.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.98ms | 92.96ms | 3.3× | 6.8% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.56ms | 73.42ms | 3.1× | 8.5% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.11ms | 82.96ms | 3.3× | 4.2% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.90ms | 80.08ms | 3.1× | 6.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 24.15ms | 72.96ms | 3.0× | 9.0% | PASS |
| ac_writes | `oltp_read_write_ac` | 33.17ms | 89.22ms | 2.7× | 7.8% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 27.60ms | 28.86ms | 1.0× | 1.4% | PASS |
| mem_reads | `oltp_range_select` | 13.98ms | 12.11ms | 0.9× | 1.7% | PASS |
| mem_reads | `oltp_sum_range` | 12.32ms | 11.92ms | 1.0× | 1.8% | PASS |
| mem_reads | `oltp_order_range` | 2.86ms | 2.79ms | 1.0× | 1.6% | PASS |
| mem_reads | `oltp_distinct_range` | 3.67ms | 3.69ms | 1.0× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 3.21ms | 5.38ms | 1.7× | 1.8% | PASS |
| mem_reads | `select_random_points` | 17.08ms | 17.21ms | 1.0× | 1.9% | PASS |
| mem_reads | `select_random_ranges` | 5.34ms | 5.07ms | 0.9× | 1.6% | PASS |
| mem_reads | `covering_index_scan` | 5.87ms | 8.15ms | 1.4× | 0.7% | PASS |
| mem_reads | `groupby_scan` | 29.24ms | 29.58ms | 1.0× | 0.8% | PASS |
| mem_reads | `index_join` | 8.67ms | 8.71ms | 1.0× | 1.1% | PASS |
| mem_reads | `index_join_scan` | 3.16ms | 5.67ms | 1.8× | 2.8% | PASS |
| mem_reads | `types_table_scan` | 887.95ms | 1.07s | 1.2× | 1.0% | PASS |
| mem_reads | `table_scan` | 1.01s | 1.16s | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_read_only` | 110.30ms | 110.68ms | 1.0× | 1.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 184.37ms | 239.49ms | 1.3× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 14.34ms | 27.84ms | 1.9× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 51.17ms | 97.07ms | 1.9× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 38.58ms | 57.56ms | 1.5× | 1.8% | PASS |
| mem_writes | `oltp_delete_insert` | 41.88ms | 72.37ms | 1.7× | 1.7% | PASS |
| mem_writes | `oltp_write_only` | 22.62ms | 41.82ms | 1.8× | 1.2% | PASS |
| mem_writes | `types_delete_insert` | 30.35ms | 39.15ms | 1.3× | 1.6% | PASS |
| mem_writes | `oltp_read_write` | 75.58ms | 99.86ms | 1.3× | 2.3% | PASS |
| file_reads | `oltp_point_select` | 93.66ms | 45.95ms | 0.5× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 21.25ms | 13.97ms | 0.7× | 1.7% | PASS |
| file_reads | `oltp_sum_range` | 19.50ms | 13.72ms | 0.7× | 1.2% | PASS |
| file_reads | `oltp_order_range` | 3.64ms | 2.99ms | 0.8× | 0.7% | PASS |
| file_reads | `oltp_distinct_range` | 4.41ms | 3.90ms | 0.9× | 1.1% | PASS |
| file_reads | `oltp_index_scan` | 10.09ms | 7.18ms | 0.7× | 0.8% | PASS |
| file_reads | `select_random_points` | 24.70ms | 19.11ms | 0.8× | 1.5% | PASS |
| file_reads | `select_random_ranges` | 12.17ms | 6.89ms | 0.6× | 0.9% | PASS |
| file_reads | `covering_index_scan` | 12.89ms | 9.93ms | 0.8× | 0.8% | PASS |
| file_reads | `groupby_scan` | 30.10ms | 29.88ms | 1.0× | 0.8% | PASS |
| file_reads | `index_join` | 12.92ms | 9.51ms | 0.7× | 1.8% | PASS |
| file_reads | `index_join_scan` | 4.01ms | 5.71ms | 1.4× | 1.8% | PASS |
| file_reads | `types_table_scan` | 885.81ms | 1.07s | 1.2× | 1.0% | PASS |
| file_reads | `table_scan` | 1.03s | 1.16s | 1.1× | 1.5% | PASS |
| file_reads | `oltp_read_only` | 208.36ms | 135.21ms | 0.6× | 1.4% | PASS |
| file_writes | `oltp_bulk_insert` | 253.73ms | 310.36ms | 1.2× | 6.8% | PASS |
| file_writes | `oltp_insert` | 31.28ms | 49.74ms | 1.6× | 7.8% | PASS |
| file_writes | `oltp_update_index` | 183.44ms | 189.60ms | 1.0× | 16.2% | PASS |
| file_writes | `oltp_update_non_index` | 148.19ms | 131.64ms | 0.9× | 17.3% | PASS |
| file_writes | `oltp_delete_insert` | 165.05ms | 146.02ms | 0.9× | 11.2% | PASS |
| file_writes | `oltp_write_only` | 118.72ms | 103.30ms | 0.9× | 12.9% | PASS |
| file_writes | `types_delete_insert` | 123.13ms | 89.88ms | 0.7× | 15.7% | PASS |
| file_writes | `oltp_read_write` | 174.97ms | 154.19ms | 0.9× | 3.6% | PASS |
| ac_reads | `oltp_point_select` | 49.73ms | 45.95ms | 0.9× | 1.3% | PASS |
| ac_reads | `oltp_range_select` | 17.03ms | 13.92ms | 0.8× | 1.6% | PASS |
| ac_reads | `oltp_sum_range` | 15.25ms | 13.67ms | 0.9× | 1.2% | PASS |
| ac_reads | `oltp_order_range` | 3.24ms | 2.98ms | 0.9× | 0.9% | PASS |
| ac_reads | `oltp_distinct_range` | 4.01ms | 3.89ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_index_scan` | 5.77ms | 7.13ms | 1.2× | 1.0% | PASS |
| ac_reads | `select_random_points` | 20.34ms | 19.07ms | 0.9× | 1.2% | PASS |
| ac_reads | `select_random_ranges` | 7.84ms | 6.87ms | 0.9× | 0.9% | PASS |
| ac_reads | `covering_index_scan` | 8.58ms | 10.04ms | 1.2× | 0.8% | PASS |
| ac_reads | `groupby_scan` | 29.49ms | 30.00ms | 1.0× | 0.6% | PASS |
| ac_reads | `index_join` | 10.74ms | 9.48ms | 0.9× | 1.7% | PASS |
| ac_reads | `index_join_scan` | 3.59ms | 5.77ms | 1.6× | 1.7% | PASS |
| ac_reads | `types_table_scan` | 892.93ms | 1.08s | 1.2× | 1.2% | PASS |
| ac_reads | `table_scan` | 1.02s | 1.16s | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_read_only` | 143.14ms | 135.35ms | 0.9× | 1.3% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 25.83ms | 67.72ms | 2.6× | 43.1% | PASS |
| ac_writes | `oltp_insert_ac` | 31.62ms | 92.25ms | 2.9× | 48.3% | PASS |
| ac_writes | `oltp_update_index_ac` | 24.80ms | 84.29ms | 3.4× | 27.5% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 18.84ms | 63.71ms | 3.4× | 17.9% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.29ms | 76.03ms | 3.0× | 29.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 23.51ms | 86.31ms | 3.7× | 27.9% | PASS |
| ac_writes | `types_delete_insert_ac` | 23.28ms | 74.07ms | 3.2× | 28.6% | PASS |
| ac_writes | `oltp_read_write_ac` | 36.78ms | 136.85ms | 3.7× | 44.5% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.22ms | 41.77ms | 1.3× | 1.8% | PASS |
| mem_reads | `oltp_range_select` | 19.31ms | 23.45ms | 1.2× | 2.7% | PASS |
| mem_reads | `oltp_sum_range` | 17.77ms | 22.46ms | 1.3× | 1.1% | PASS |
| mem_reads | `oltp_order_range` | 3.67ms | 4.21ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 4.75ms | 5.40ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 4.57ms | 6.54ms | 1.4× | 2.2% | PASS |
| mem_reads | `select_random_points` | 27.91ms | 34.15ms | 1.2× | 2.0% | PASS |
| mem_reads | `select_random_ranges` | 7.66ms | 9.40ms | 1.2× | 1.7% | PASS |
| mem_reads | `covering_index_scan` | 7.63ms | 10.48ms | 1.4× | 0.8% | PASS |
| mem_reads | `groupby_scan` | 35.68ms | 41.52ms | 1.2× | 1.0% | PASS |
| mem_reads | `index_join` | 7.88ms | 11.70ms | 1.5× | 2.3% | PASS |
| mem_reads | `index_join_scan` | 3.97ms | 6.56ms | 1.7× | 2.5% | PASS |
| mem_reads | `types_table_scan` | 1.04s | 1.31s | 1.3× | 0.6% | PASS |
| mem_reads | `table_scan` | 1.29s | 1.42s | 1.1× | 3.7% | PASS |
| mem_reads | `oltp_read_only` | 156.07ms | 185.09ms | 1.2× | 1.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 242.68ms | 324.20ms | 1.3× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 18.64ms | 34.07ms | 1.8× | 1.2% | PASS |
| mem_writes | `oltp_update_index` | 67.10ms | 120.59ms | 1.8× | 1.5% | PASS |
| mem_writes | `oltp_update_non_index` | 50.88ms | 81.53ms | 1.6× | 1.9% | PASS |
| mem_writes | `oltp_delete_insert` | 48.56ms | 90.95ms | 1.9× | 1.3% | PASS |
| mem_writes | `oltp_write_only` | 26.91ms | 54.15ms | 2.0× | 1.8% | PASS |
| mem_writes | `types_delete_insert` | 32.62ms | 53.09ms | 1.6× | 2.2% | PASS |
| mem_writes | `oltp_read_write` | 109.17ms | 160.95ms | 1.5× | 1.2% | PASS |
| file_reads | `oltp_point_select` | 105.09ms | 62.76ms | 0.6× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 27.67ms | 25.82ms | 0.9× | 1.8% | PASS |
| file_reads | `oltp_sum_range` | 25.46ms | 24.88ms | 1.0× | 1.2% | PASS |
| file_reads | `oltp_order_range` | 4.53ms | 4.53ms | 1.0× | 2.0% | PASS |
| file_reads | `oltp_distinct_range` | 5.64ms | 5.73ms | 1.0× | 1.5% | PASS |
| file_reads | `oltp_index_scan` | 11.78ms | 8.79ms | 0.7× | 1.6% | PASS |
| file_reads | `select_random_points` | 37.06ms | 37.97ms | 1.0× | 2.0% | PASS |
| file_reads | `select_random_ranges` | 15.28ms | 11.69ms | 0.8× | 1.5% | PASS |
| file_reads | `covering_index_scan` | 15.04ms | 12.87ms | 0.9× | 1.5% | PASS |
| file_reads | `groupby_scan` | 36.56ms | 41.95ms | 1.1× | 0.9% | PASS |
| file_reads | `index_join` | 11.92ms | 13.24ms | 1.1× | 1.7% | PASS |
| file_reads | `index_join_scan` | 4.89ms | 6.80ms | 1.4× | 2.5% | PASS |
| file_reads | `types_table_scan` | 1.05s | 1.31s | 1.2× | 0.6% | PASS |
| file_reads | `table_scan` | 1.29s | 1.42s | 1.1× | 4.5% | PASS |
| file_reads | `oltp_read_only` | 258.89ms | 215.37ms | 0.8× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 259.63ms | 337.74ms | 1.3× | 0.9% | PASS |
| file_writes | `oltp_insert` | 26.18ms | 38.27ms | 1.5× | 2.2% | PASS |
| file_writes | `oltp_update_index` | 98.44ms | 136.77ms | 1.4× | 1.4% | PASS |
| file_writes | `oltp_update_non_index` | 79.55ms | 93.20ms | 1.2× | 2.1% | PASS |
| file_writes | `oltp_delete_insert` | 77.05ms | 105.68ms | 1.4× | 1.5% | PASS |
| file_writes | `oltp_write_only` | 51.81ms | 64.69ms | 1.2× | 1.9% | PASS |
| file_writes | `types_delete_insert` | 50.04ms | 61.59ms | 1.2× | 1.9% | PASS |
| file_writes | `oltp_read_write` | 133.78ms | 169.81ms | 1.3× | 1.4% | PASS |
| ac_reads | `oltp_point_select` | 56.84ms | 61.78ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 21.48ms | 25.72ms | 1.2× | 1.4% | PASS |
| ac_reads | `oltp_sum_range` | 20.16ms | 24.91ms | 1.2× | 1.5% | PASS |
| ac_reads | `oltp_order_range` | 4.00ms | 4.51ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_distinct_range` | 5.09ms | 5.74ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_index_scan` | 6.97ms | 8.79ms | 1.3× | 2.1% | PASS |
| ac_reads | `select_random_points` | 30.63ms | 37.98ms | 1.2× | 1.8% | PASS |
| ac_reads | `select_random_ranges` | 9.97ms | 11.61ms | 1.2× | 1.2% | PASS |
| ac_reads | `covering_index_scan` | 10.18ms | 12.85ms | 1.3× | 1.4% | PASS |
| ac_reads | `groupby_scan` | 35.76ms | 41.94ms | 1.2× | 1.0% | PASS |
| ac_reads | `index_join` | 9.35ms | 13.31ms | 1.4× | 1.1% | PASS |
| ac_reads | `index_join_scan` | 4.34ms | 6.85ms | 1.6× | 1.8% | PASS |
| ac_reads | `types_table_scan` | 1.08s | 1.32s | 1.2× | 1.8% | PASS |
| ac_reads | `table_scan` | 1.33s | 1.43s | 1.1× | 3.6% | PASS |
| ac_reads | `oltp_read_only` | 190.18ms | 216.09ms | 1.1× | 0.7% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 23.24ms | 64.14ms | 2.8× | 6.5% | PASS |
| ac_writes | `oltp_insert_ac` | 25.58ms | 78.57ms | 3.1× | 6.8% | PASS |
| ac_writes | `oltp_update_index_ac` | 28.19ms | 89.48ms | 3.2× | 6.9% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.24ms | 72.13ms | 3.0× | 7.9% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.75ms | 81.24ms | 3.2× | 7.4% | PASS |
| ac_writes | `oltp_write_only_ac` | 27.17ms | 81.26ms | 3.0× | 8.9% | PASS |
| ac_writes | `types_delete_insert_ac` | 23.20ms | 73.19ms | 3.2× | 8.0% | PASS |
| ac_writes | `oltp_read_write_ac` | 33.13ms | 89.49ms | 2.7× | 7.0% | PASS |

</details>

</details>

## Version-control latency

Wall time: 8m 36s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 22.51ms | 38.00ms | 59.2% | 1.4% | PASS |
| `status_dirty_many_tables` | 24.82ms | 42.00ms | 59.1% | 0.7% | PASS |
| `diff_regular_working_one_table` | 19.80ms | 33.00ms | 60.0% | 0.8% | PASS |
| `diff_regular_working_many_tables` | 27.07ms | 50.00ms | 54.1% | 0.5% | PASS |
| `diff_stat_working_many_tables` | 26.95ms | 48.00ms | 56.1% | 0.6% | PASS |
| `diff_schema_working_many_tables` | 27.75ms | 48.00ms | 57.8% | 1.0% | PASS |
| `branch_list_many_branches` | 15.13ms | 25.00ms | 60.5% | 0.9% | PASS |
| `branch_create_delete` | 16.75ms | 27.00ms | 62.0% | 1.4% | PASS |
| `at_literal_deep_history` | 17.12ms | 28.00ms | 61.1% | 0.9% | PASS |
| `diff_literal_deep_history` | 17.17ms | 28.00ms | 61.3% | 0.7% | PASS |
| `history_literal_deep_history` | 18.02ms | 30.00ms | 60.1% | 2.8% | PASS |
| `checkout_branch_clean` | 70.05ms | 43.00ms | 162.9% | 1.2% | FAIL |
| `merge_data_no_conflicts` | 24.53ms | 33.00ms | 74.3% | 3.3% | PASS |
| `merge_data_secondary_index` | 843.93ms | 944.00ms | 89.4% | 3.7% | PASS |
| `merge_schema_no_conflicts` | 16.32ms | 24.00ms | 68.0% | 2.3% | PASS |
| `merge_data_conflicts` | 22.67ms | 33.00ms | 68.7% | 2.0% | PASS |
| `merge_data_conflicts_with_resolve` | 23.23ms | 34.00ms | 68.3% | 1.9% | PASS |

Version-control ceiling result: **FAIL**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
