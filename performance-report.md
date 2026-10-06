# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-10-06 11:12 UTC
>
> Commit: [`3e4ff818578818250b94fc6b0a4ab562187d2f2b`](https://github.com/dolthub/doltlite/commit/3e4ff818578818250b94fc6b0a4ab562187d2f2b)
>
> Runner: ubuntu24 20260927.320.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/37444427839)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.57s | 11.12s | 1.2× | 1.4% | **PASS** |
| Writes | 1.94s | 3.03s | 1.6× | 1.4% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.16s | 11.24s | 1.1× | 1.2% | **PASS** |
| Writes | 3.66s | 4.03s | 1.1× | 1.8% | **PASS** |
| Autocommit writes | 816.91ms | 2.35s | 2.9× | 4.7% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 1.51s | 1.64s | 1.1× | 1.2% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.86s | 3.11s | 1.1× | 1.3% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.56s | 3.15s | 1.2× | 1.5% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.65s | 3.22s | 1.2× | 1.4% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 246.71ms | 384.36ms | 1.6× | 1.3% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 532.79ms | 813.89ms | 1.5× | 1.8% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 564.29ms | 914.54ms | 1.6× | 0.9% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 591.67ms | 918.26ms | 1.6× | 1.2% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 1.67s | 1.68s | 1.0× | 0.7% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.94s | 3.11s | 1.1× | 1.6% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.71s | 3.17s | 1.2× | 1.5% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.83s | 3.28s | 1.2× | 1.5% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 723.29ms | 668.02ms | 0.9× | 2.4% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.38s | 1.36s | 1.0× | 3.3% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 789.83ms | 1.02s | 1.3× | 1.3% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 763.75ms | 992.60ms | 1.3× | 1.5% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 1.56s | 1.68s | 1.1× | 0.8% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.47s | 2.89s | 1.2× | 0.8% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.66s | 3.22s | 1.2× | 1.7% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.80s | 3.30s | 1.2× | 1.5% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 157.48ms | 435.98ms | 2.8× | 2.2% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 246.88ms | 685.45ms | 2.8× | 4.3% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 206.09ms | 617.13ms | 3.0× | 5.4% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 206.46ms | 608.33ms | 2.9× | 6.5% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 13.09ms | 17.28ms | 1.3× | 2.4% | PASS |
| mem_reads | `oltp_range_select` | 5.00ms | 6.45ms | 1.3× | 3.0% | PASS |
| mem_reads | `oltp_sum_range` | 4.85ms | 6.48ms | 1.3× | 2.0% | PASS |
| mem_reads | `oltp_order_range` | 1.43ms | 1.67ms | 1.2× | 2.1% | PASS |
| mem_reads | `oltp_distinct_range` | 1.92ms | 2.19ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 2.10ms | 3.17ms | 1.5× | 1.4% | PASS |
| mem_reads | `select_random_points` | 5.53ms | 7.93ms | 1.4× | 5.8% | PASS |
| mem_reads | `select_random_ranges` | 2.37ms | 2.88ms | 1.2× | 2.7% | PASS |
| mem_reads | `covering_index_scan` | 3.82ms | 5.24ms | 1.4× | 0.9% | PASS |
| mem_reads | `groupby_scan` | 15.78ms | 17.83ms | 1.1× | 0.8% | PASS |
| mem_reads | `index_join` | 3.29ms | 4.61ms | 1.4× | 1.0% | PASS |
| mem_reads | `index_join_scan` | 1.63ms | 3.63ms | 2.2× | 1.2% | PASS |
| mem_reads | `types_table_scan` | 651.00ms | 706.98ms | 1.1× | 0.5% | PASS |
| mem_reads | `table_scan` | 742.05ms | 792.38ms | 1.1× | 0.5% | PASS |
| mem_reads | `oltp_read_only` | 52.97ms | 61.83ms | 1.2× | 0.7% | PASS |
| mem_writes | `oltp_bulk_insert` | 105.29ms | 148.46ms | 1.4× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 8.79ms | 14.73ms | 1.7× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 28.36ms | 50.66ms | 1.8× | 1.4% | PASS |
| mem_writes | `oltp_update_non_index` | 19.70ms | 32.05ms | 1.6× | 2.0% | PASS |
| mem_writes | `oltp_delete_insert` | 24.68ms | 39.55ms | 1.6× | 1.9% | PASS |
| mem_writes | `oltp_write_only` | 11.99ms | 24.60ms | 2.1× | 1.8% | PASS |
| mem_writes | `types_delete_insert` | 14.55ms | 20.03ms | 1.4× | 1.2% | PASS |
| mem_writes | `oltp_read_write` | 33.34ms | 54.28ms | 1.6× | 1.2% | PASS |
| file_reads | `oltp_point_select` | 64.75ms | 30.79ms | 0.5× | 0.7% | PASS |
| file_reads | `oltp_range_select` | 10.92ms | 7.89ms | 0.7× | 0.7% | PASS |
| file_reads | `oltp_sum_range` | 10.51ms | 7.89ms | 0.8× | 0.6% | PASS |
| file_reads | `oltp_order_range` | 2.12ms | 1.84ms | 0.9× | 1.0% | PASS |
| file_reads | `oltp_distinct_range` | 2.60ms | 2.35ms | 0.9× | 0.8% | PASS |
| file_reads | `oltp_index_scan` | 7.78ms | 4.68ms | 0.6× | 0.9% | PASS |
| file_reads | `select_random_points` | 12.13ms | 9.29ms | 0.8× | 0.6% | PASS |
| file_reads | `select_random_ranges` | 7.90ms | 4.29ms | 0.5× | 0.7% | PASS |
| file_reads | `covering_index_scan` | 9.39ms | 6.76ms | 0.7× | 0.6% | PASS |
| file_reads | `groupby_scan` | 16.71ms | 17.97ms | 1.1× | 0.4% | PASS |
| file_reads | `index_join` | 6.37ms | 5.74ms | 0.9× | 0.8% | PASS |
| file_reads | `index_join_scan` | 2.67ms | 3.84ms | 1.4× | 0.7% | PASS |
| file_reads | `types_table_scan` | 652.29ms | 707.18ms | 1.1× | 0.6% | PASS |
| file_reads | `table_scan` | 741.63ms | 791.52ms | 1.1× | 0.9% | PASS |
| file_reads | `oltp_read_only` | 126.32ms | 81.03ms | 0.6× | 0.6% | PASS |
| file_writes | `oltp_bulk_insert` | 144.44ms | 193.49ms | 1.3× | 2.9% | PASS |
| file_writes | `oltp_insert` | 19.72ms | 25.18ms | 1.3× | 1.1% | PASS |
| file_writes | `oltp_update_index` | 120.42ms | 104.96ms | 0.9× | 3.8% | PASS |
| file_writes | `oltp_update_non_index` | 97.31ms | 76.00ms | 0.8× | 1.3% | PASS |
| file_writes | `oltp_delete_insert` | 100.53ms | 79.35ms | 0.8× | 2.0% | PASS |
| file_writes | `oltp_write_only` | 80.38ms | 59.68ms | 0.7× | 3.3% | PASS |
| file_writes | `types_delete_insert` | 60.35ms | 39.96ms | 0.7× | 13.0% | PASS |
| file_writes | `oltp_read_write` | 100.14ms | 89.40ms | 0.9× | 0.3% | PASS |
| ac_reads | `oltp_point_select` | 30.19ms | 30.89ms | 1.0× | 0.6% | PASS |
| ac_reads | `oltp_range_select` | 7.09ms | 7.88ms | 1.1× | 0.7% | PASS |
| ac_reads | `oltp_sum_range` | 6.78ms | 7.90ms | 1.2× | 0.8% | PASS |
| ac_reads | `oltp_order_range` | 1.70ms | 1.84ms | 1.1× | 0.8% | PASS |
| ac_reads | `oltp_distinct_range` | 2.20ms | 2.35ms | 1.1× | 0.8% | PASS |
| ac_reads | `oltp_index_scan` | 3.96ms | 4.65ms | 1.2× | 0.8% | PASS |
| ac_reads | `select_random_points` | 8.06ms | 9.33ms | 1.2× | 0.9% | PASS |
| ac_reads | `select_random_ranges` | 4.27ms | 4.29ms | 1.0× | 0.8% | PASS |
| ac_reads | `covering_index_scan` | 5.58ms | 6.78ms | 1.2× | 0.9% | PASS |
| ac_reads | `groupby_scan` | 16.24ms | 18.02ms | 1.1× | 0.5% | PASS |
| ac_reads | `index_join` | 4.32ms | 5.73ms | 1.3× | 0.9% | PASS |
| ac_reads | `index_join_scan` | 2.21ms | 3.85ms | 1.7× | 1.3% | PASS |
| ac_reads | `types_table_scan` | 653.26ms | 706.19ms | 1.1× | 0.5% | PASS |
| ac_reads | `table_scan` | 741.58ms | 790.54ms | 1.1× | 0.7% | PASS |
| ac_reads | `oltp_read_only` | 77.16ms | 80.98ms | 1.0× | 0.5% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 18.53ms | 46.61ms | 2.5× | 2.2% | PASS |
| ac_writes | `oltp_insert_ac` | 19.76ms | 54.15ms | 2.7× | 2.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 20.80ms | 61.39ms | 3.0× | 2.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 18.55ms | 50.39ms | 2.7× | 1.7% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 19.63ms | 56.57ms | 2.9× | 2.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 19.61ms | 56.09ms | 2.9× | 2.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 18.39ms | 50.61ms | 2.8× | 2.2% | PASS |
| ac_writes | `oltp_read_write_ac` | 22.20ms | 60.18ms | 2.7× | 2.3% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 31.53ms | 32.29ms | 1.0× | 1.9% | PASS |
| mem_reads | `oltp_range_select` | 14.15ms | 13.86ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_sum_range` | 14.01ms | 13.28ms | 0.9× | 1.0% | PASS |
| mem_reads | `oltp_order_range` | 2.94ms | 3.18ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_distinct_range` | 3.86ms | 4.33ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_index_scan` | 3.62ms | 5.83ms | 1.6× | 1.2% | PASS |
| mem_reads | `select_random_points` | 22.45ms | 22.16ms | 1.0× | 3.3% | PASS |
| mem_reads | `select_random_ranges` | 5.93ms | 5.93ms | 1.0× | 1.3% | PASS |
| mem_reads | `covering_index_scan` | 6.58ms | 9.11ms | 1.4× | 0.9% | PASS |
| mem_reads | `groupby_scan` | 33.93ms | 36.21ms | 1.1× | 0.7% | PASS |
| mem_reads | `index_join` | 11.33ms | 10.27ms | 0.9× | 2.2% | PASS |
| mem_reads | `index_join_scan` | 3.34ms | 6.84ms | 2.0× | 2.3% | PASS |
| mem_reads | `types_table_scan` | 1.18s | 1.35s | 1.1× | 1.3% | PASS |
| mem_reads | `table_scan` | 1.39s | 1.46s | 1.1× | 1.7% | PASS |
| mem_reads | `oltp_read_only` | 139.75ms | 143.57ms | 1.0× | 1.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 192.96ms | 264.98ms | 1.4× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 15.46ms | 32.30ms | 2.1× | 1.5% | PASS |
| mem_writes | `oltp_update_index` | 64.59ms | 131.58ms | 2.0× | 2.1% | PASS |
| mem_writes | `oltp_update_non_index` | 48.18ms | 70.44ms | 1.5× | 2.0% | PASS |
| mem_writes | `oltp_delete_insert` | 50.75ms | 91.25ms | 1.8× | 1.4% | PASS |
| mem_writes | `oltp_write_only` | 26.60ms | 50.16ms | 1.9× | 2.1% | PASS |
| mem_writes | `types_delete_insert` | 37.45ms | 48.51ms | 1.3× | 1.8% | PASS |
| mem_writes | `oltp_read_write` | 96.81ms | 124.67ms | 1.3× | 1.9% | PASS |
| file_reads | `oltp_point_select` | 68.00ms | 43.56ms | 0.6× | 1.2% | PASS |
| file_reads | `oltp_range_select` | 19.09ms | 15.89ms | 0.8× | 1.8% | PASS |
| file_reads | `oltp_sum_range` | 18.73ms | 15.30ms | 0.8× | 1.6% | PASS |
| file_reads | `oltp_order_range` | 3.50ms | 3.48ms | 1.0× | 1.6% | PASS |
| file_reads | `oltp_distinct_range` | 4.47ms | 4.65ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 7.23ms | 7.10ms | 1.0× | 1.5% | PASS |
| file_reads | `select_random_points` | 27.29ms | 23.69ms | 0.9× | 2.4% | PASS |
| file_reads | `select_random_ranges` | 9.47ms | 6.89ms | 0.7× | 1.8% | PASS |
| file_reads | `covering_index_scan` | 10.07ms | 10.20ms | 1.0× | 1.1% | PASS |
| file_reads | `groupby_scan` | 34.26ms | 36.40ms | 1.1× | 0.9% | PASS |
| file_reads | `index_join` | 13.27ms | 10.88ms | 0.8× | 2.4% | PASS |
| file_reads | `index_join_scan` | 3.83ms | 7.02ms | 1.8× | 2.6% | PASS |
| file_reads | `types_table_scan` | 1.16s | 1.34s | 1.2× | 2.3% | PASS |
| file_reads | `table_scan` | 1.37s | 1.43s | 1.0× | 2.7% | PASS |
| file_reads | `oltp_read_only` | 181.82ms | 153.58ms | 0.8× | 1.6% | PASS |
| file_writes | `oltp_bulk_insert` | 286.34ms | 350.58ms | 1.2× | 4.9% | PASS |
| file_writes | `oltp_insert` | 33.44ms | 61.02ms | 1.8× | 2.4% | PASS |
| file_writes | `oltp_update_index` | 197.38ms | 228.60ms | 1.2× | 3.3% | PASS |
| file_writes | `oltp_update_non_index` | 169.49ms | 142.07ms | 0.8× | 3.2% | PASS |
| file_writes | `oltp_delete_insert` | 203.69ms | 172.91ms | 0.8× | 3.5% | PASS |
| file_writes | `oltp_write_only` | 137.93ms | 115.11ms | 0.8× | 1.7% | PASS |
| file_writes | `types_delete_insert` | 162.43ms | 104.98ms | 0.6× | 9.1% | PASS |
| file_writes | `oltp_read_write` | 193.48ms | 182.19ms | 0.9× | 2.5% | PASS |
| ac_reads | `oltp_point_select` | 40.29ms | 40.26ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 15.22ms | 14.84ms | 1.0× | 0.6% | PASS |
| ac_reads | `oltp_sum_range` | 15.08ms | 14.39ms | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_order_range` | 3.16ms | 3.37ms | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_distinct_range` | 4.06ms | 4.47ms | 1.1× | 1.0% | PASS |
| ac_reads | `oltp_index_scan` | 4.82ms | 6.77ms | 1.4× | 0.6% | PASS |
| ac_reads | `select_random_points` | 22.65ms | 22.15ms | 1.0× | 0.9% | PASS |
| ac_reads | `select_random_ranges` | 6.92ms | 6.59ms | 1.0× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 7.53ms | 9.77ms | 1.3× | 0.7% | PASS |
| ac_reads | `groupby_scan` | 32.52ms | 35.01ms | 1.1× | 0.4% | PASS |
| ac_reads | `index_join` | 11.54ms | 10.25ms | 0.9× | 0.7% | PASS |
| ac_reads | `index_join_scan` | 3.47ms | 6.47ms | 1.9× | 1.2% | PASS |
| ac_reads | `types_table_scan` | 1.01s | 1.24s | 1.2× | 0.6% | PASS |
| ac_reads | `table_scan` | 1.17s | 1.34s | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_read_only` | 131.87ms | 140.84ms | 1.1× | 0.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 29.34ms | 71.53ms | 2.4× | 3.6% | PASS |
| ac_writes | `oltp_insert_ac` | 31.16ms | 83.44ms | 2.7× | 3.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 31.64ms | 98.05ms | 3.1× | 6.0% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 28.82ms | 80.48ms | 2.8× | 4.9% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 30.81ms | 90.60ms | 2.9× | 4.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 30.54ms | 87.81ms | 2.9× | 3.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 29.59ms | 80.36ms | 2.7× | 5.0% | PASS |
| ac_writes | `oltp_read_write_ac` | 34.98ms | 93.17ms | 2.7× | 4.2% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.68ms | 39.58ms | 1.2× | 2.0% | PASS |
| mem_reads | `oltp_range_select` | 14.99ms | 15.87ms | 1.1× | 2.8% | PASS |
| mem_reads | `oltp_sum_range` | 14.22ms | 15.68ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_order_range` | 3.19ms | 3.48ms | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_distinct_range` | 4.26ms | 4.62ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 3.91ms | 6.96ms | 1.8× | 1.3% | PASS |
| mem_reads | `select_random_points` | 21.39ms | 22.79ms | 1.1× | 1.8% | PASS |
| mem_reads | `select_random_ranges` | 6.53ms | 6.96ms | 1.1× | 1.5% | PASS |
| mem_reads | `covering_index_scan` | 7.61ms | 11.50ms | 1.5× | 0.9% | PASS |
| mem_reads | `groupby_scan` | 33.20ms | 35.85ms | 1.1× | 0.7% | PASS |
| mem_reads | `index_join` | 9.83ms | 10.59ms | 1.1× | 1.7% | PASS |
| mem_reads | `index_join_scan` | 3.59ms | 6.30ms | 1.8× | 2.4% | PASS |
| mem_reads | `types_table_scan` | 1.05s | 1.35s | 1.3× | 0.5% | PASS |
| mem_reads | `table_scan` | 1.21s | 1.46s | 1.2× | 2.7% | PASS |
| mem_reads | `oltp_read_only` | 146.24ms | 155.41ms | 1.1× | 1.2% | PASS |
| mem_writes | `oltp_bulk_insert` | 238.62ms | 334.24ms | 1.4× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 18.20ms | 37.76ms | 2.1× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 62.71ms | 127.71ms | 2.0× | 2.0% | PASS |
| mem_writes | `oltp_update_non_index` | 44.89ms | 76.74ms | 1.7× | 1.6% | PASS |
| mem_writes | `oltp_delete_insert` | 49.39ms | 94.64ms | 1.9× | 0.7% | PASS |
| mem_writes | `oltp_write_only` | 26.02ms | 55.15ms | 2.1× | 0.8% | PASS |
| mem_writes | `types_delete_insert` | 35.66ms | 53.12ms | 1.5× | 1.3% | PASS |
| mem_writes | `oltp_read_write` | 88.82ms | 135.18ms | 1.5× | 0.6% | PASS |
| file_reads | `oltp_point_select` | 103.29ms | 58.55ms | 0.6× | 1.1% | PASS |
| file_reads | `oltp_range_select` | 22.85ms | 17.90ms | 0.8× | 2.7% | PASS |
| file_reads | `oltp_sum_range` | 21.71ms | 17.82ms | 0.8× | 2.0% | PASS |
| file_reads | `oltp_order_range` | 4.19ms | 3.73ms | 0.9× | 2.0% | PASS |
| file_reads | `oltp_distinct_range` | 5.18ms | 4.91ms | 0.9× | 2.4% | PASS |
| file_reads | `oltp_index_scan` | 11.04ms | 9.04ms | 0.8× | 1.5% | PASS |
| file_reads | `select_random_points` | 30.27ms | 25.61ms | 0.8× | 2.5% | PASS |
| file_reads | `select_random_ranges` | 13.52ms | 9.00ms | 0.7× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 15.03ms | 13.56ms | 0.9× | 1.0% | PASS |
| file_reads | `groupby_scan` | 34.05ms | 36.14ms | 1.1× | 0.9% | PASS |
| file_reads | `index_join` | 13.89ms | 11.76ms | 0.8× | 2.4% | PASS |
| file_reads | `index_join_scan` | 4.47ms | 6.58ms | 1.5× | 1.8% | PASS |
| file_reads | `types_table_scan` | 1.03s | 1.34s | 1.3× | 0.4% | PASS |
| file_reads | `table_scan` | 1.16s | 1.43s | 1.2× | 0.3% | PASS |
| file_reads | `oltp_read_only` | 236.03ms | 177.10ms | 0.8× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 260.08ms | 347.40ms | 1.3× | 1.0% | PASS |
| file_writes | `oltp_insert` | 25.18ms | 42.79ms | 1.7× | 1.0% | PASS |
| file_writes | `oltp_update_index` | 93.30ms | 145.20ms | 1.6× | 1.4% | PASS |
| file_writes | `oltp_update_non_index` | 88.22ms | 90.24ms | 1.0× | 14.7% | PASS |
| file_writes | `oltp_delete_insert` | 84.02ms | 110.45ms | 1.3× | 1.4% | PASS |
| file_writes | `oltp_write_only` | 55.55ms | 68.08ms | 1.2× | 1.3% | PASS |
| file_writes | `types_delete_insert` | 61.72ms | 64.19ms | 1.0× | 1.8% | PASS |
| file_writes | `oltp_read_write` | 121.77ms | 147.51ms | 1.2× | 0.7% | PASS |
| ac_reads | `oltp_point_select` | 57.83ms | 58.69ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_range_select` | 18.08ms | 17.89ms | 1.0× | 1.8% | PASS |
| ac_reads | `oltp_sum_range` | 17.00ms | 17.90ms | 1.1× | 1.7% | PASS |
| ac_reads | `oltp_order_range` | 3.75ms | 3.74ms | 1.0× | 2.2% | PASS |
| ac_reads | `oltp_distinct_range` | 4.71ms | 4.89ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_index_scan` | 6.42ms | 9.01ms | 1.4× | 1.5% | PASS |
| ac_reads | `select_random_points` | 23.85ms | 25.49ms | 1.1× | 2.2% | PASS |
| ac_reads | `select_random_ranges` | 8.91ms | 8.99ms | 1.0× | 1.9% | PASS |
| ac_reads | `covering_index_scan` | 10.27ms | 13.54ms | 1.3× | 1.0% | PASS |
| ac_reads | `groupby_scan` | 33.74ms | 36.15ms | 1.1× | 0.6% | PASS |
| ac_reads | `index_join` | 11.65ms | 11.79ms | 1.0× | 1.9% | PASS |
| ac_reads | `index_join_scan` | 4.19ms | 6.71ms | 1.6× | 2.1% | PASS |
| ac_reads | `types_table_scan` | 1.13s | 1.39s | 1.2× | 0.8% | PASS |
| ac_reads | `table_scan` | 1.16s | 1.43s | 1.2× | 0.5% | PASS |
| ac_reads | `oltp_read_only` | 168.11ms | 177.05ms | 1.1× | 0.6% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 23.96ms | 61.27ms | 2.6× | 6.5% | PASS |
| ac_writes | `oltp_insert_ac` | 24.75ms | 77.02ms | 3.1× | 4.1% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.76ms | 90.42ms | 3.4× | 4.4% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.82ms | 71.67ms | 3.1× | 5.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.85ms | 80.77ms | 3.1× | 5.2% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.62ms | 80.04ms | 3.1× | 5.9% | PASS |
| ac_writes | `types_delete_insert_ac` | 24.58ms | 70.71ms | 2.9× | 5.6% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.76ms | 85.24ms | 2.7× | 3.6% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 35.20ms | 42.85ms | 1.2× | 1.7% | PASS |
| mem_reads | `oltp_range_select` | 20.27ms | 24.24ms | 1.2× | 1.7% | PASS |
| mem_reads | `oltp_sum_range` | 18.41ms | 24.10ms | 1.3× | 1.9% | PASS |
| mem_reads | `oltp_order_range` | 3.63ms | 4.25ms | 1.2× | 1.4% | PASS |
| mem_reads | `oltp_distinct_range` | 4.72ms | 5.42ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 4.61ms | 6.67ms | 1.4× | 1.3% | PASS |
| mem_reads | `select_random_points` | 29.30ms | 34.59ms | 1.2× | 2.6% | PASS |
| mem_reads | `select_random_ranges` | 7.85ms | 9.41ms | 1.2× | 1.5% | PASS |
| mem_reads | `covering_index_scan` | 7.64ms | 11.55ms | 1.5× | 1.1% | PASS |
| mem_reads | `groupby_scan` | 36.12ms | 42.39ms | 1.2× | 1.0% | PASS |
| mem_reads | `index_join` | 7.91ms | 11.95ms | 1.5× | 3.1% | PASS |
| mem_reads | `index_join_scan` | 3.88ms | 6.36ms | 1.6× | 1.3% | PASS |
| mem_reads | `types_table_scan` | 1.05s | 1.35s | 1.3× | 0.9% | PASS |
| mem_reads | `table_scan` | 1.26s | 1.46s | 1.2× | 3.7% | PASS |
| mem_reads | `oltp_read_only` | 161.55ms | 191.01ms | 1.2× | 1.2% | PASS |
| mem_writes | `oltp_bulk_insert` | 244.02ms | 326.33ms | 1.3× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 18.72ms | 34.05ms | 1.8× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 68.29ms | 122.29ms | 1.8× | 1.9% | PASS |
| mem_writes | `oltp_update_non_index` | 51.79ms | 81.64ms | 1.6× | 1.8% | PASS |
| mem_writes | `oltp_delete_insert` | 49.84ms | 92.58ms | 1.9× | 1.2% | PASS |
| mem_writes | `oltp_write_only` | 27.98ms | 55.48ms | 2.0× | 2.4% | PASS |
| mem_writes | `types_delete_insert` | 32.46ms | 52.73ms | 1.6× | 1.2% | PASS |
| mem_writes | `oltp_read_write` | 98.57ms | 153.17ms | 1.6× | 1.2% | PASS |
| file_reads | `oltp_point_select` | 103.47ms | 60.84ms | 0.6× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 26.30ms | 26.17ms | 1.0× | 2.4% | PASS |
| file_reads | `oltp_sum_range` | 25.15ms | 26.02ms | 1.0× | 1.7% | PASS |
| file_reads | `oltp_order_range` | 4.40ms | 4.55ms | 1.0× | 2.5% | PASS |
| file_reads | `oltp_distinct_range` | 5.52ms | 5.73ms | 1.0× | 1.2% | PASS |
| file_reads | `oltp_index_scan` | 11.96ms | 8.87ms | 0.7× | 1.5% | PASS |
| file_reads | `select_random_points` | 38.52ms | 38.35ms | 1.0× | 3.0% | PASS |
| file_reads | `select_random_ranges` | 15.56ms | 11.75ms | 0.8× | 1.9% | PASS |
| file_reads | `covering_index_scan` | 15.84ms | 13.96ms | 0.9× | 1.6% | PASS |
| file_reads | `groupby_scan` | 36.70ms | 42.87ms | 1.2× | 0.9% | PASS |
| file_reads | `index_join` | 11.96ms | 13.28ms | 1.1× | 1.4% | PASS |
| file_reads | `index_join_scan` | 4.79ms | 6.84ms | 1.4× | 2.0% | PASS |
| file_reads | `types_table_scan` | 1.05s | 1.35s | 1.3× | 1.0% | PASS |
| file_reads | `table_scan` | 1.22s | 1.45s | 1.2× | 0.9% | PASS |
| file_reads | `oltp_read_only` | 258.21ms | 217.86ms | 0.8× | 1.2% | PASS |
| file_writes | `oltp_bulk_insert` | 259.79ms | 336.71ms | 1.3× | 1.1% | PASS |
| file_writes | `oltp_insert` | 25.62ms | 37.74ms | 1.5× | 1.3% | PASS |
| file_writes | `oltp_update_index` | 94.57ms | 132.83ms | 1.4× | 1.2% | PASS |
| file_writes | `oltp_update_non_index` | 77.29ms | 90.91ms | 1.2× | 1.7% | PASS |
| file_writes | `oltp_delete_insert` | 76.64ms | 103.88ms | 1.4× | 2.3% | PASS |
| file_writes | `oltp_write_only` | 50.03ms | 63.28ms | 1.3× | 1.2% | PASS |
| file_writes | `types_delete_insert` | 49.30ms | 60.85ms | 1.2× | 1.8% | PASS |
| file_writes | `oltp_read_write` | 130.50ms | 166.40ms | 1.3× | 1.8% | PASS |
| ac_reads | `oltp_point_select` | 57.27ms | 61.29ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 22.44ms | 26.34ms | 1.2× | 1.5% | PASS |
| ac_reads | `oltp_sum_range` | 20.72ms | 26.13ms | 1.3× | 1.0% | PASS |
| ac_reads | `oltp_order_range` | 3.97ms | 4.53ms | 1.1× | 0.9% | PASS |
| ac_reads | `oltp_distinct_range` | 5.09ms | 5.77ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 7.22ms | 8.91ms | 1.2× | 1.9% | PASS |
| ac_reads | `select_random_points` | 32.59ms | 38.18ms | 1.2× | 1.8% | PASS |
| ac_reads | `select_random_ranges` | 10.35ms | 11.71ms | 1.1× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 10.45ms | 13.73ms | 1.3× | 1.8% | PASS |
| ac_reads | `groupby_scan` | 36.22ms | 42.82ms | 1.2× | 0.6% | PASS |
| ac_reads | `index_join` | 9.66ms | 13.33ms | 1.4× | 2.0% | PASS |
| ac_reads | `index_join_scan` | 4.41ms | 6.82ms | 1.5× | 1.8% | PASS |
| ac_reads | `types_table_scan` | 1.07s | 1.35s | 1.3× | 1.5% | PASS |
| ac_reads | `table_scan` | 1.32s | 1.47s | 1.1× | 1.9% | PASS |
| ac_reads | `oltp_read_only` | 193.70ms | 221.21ms | 1.1× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.91ms | 59.51ms | 2.6× | 6.1% | PASS |
| ac_writes | `oltp_insert_ac` | 24.95ms | 75.36ms | 3.0× | 5.8% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.94ms | 87.13ms | 3.1× | 8.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.41ms | 71.93ms | 3.1× | 5.5% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 27.09ms | 80.18ms | 3.0× | 8.0% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.56ms | 78.26ms | 3.1× | 7.0% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.56ms | 71.39ms | 3.2× | 7.3% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.04ms | 84.56ms | 2.6× | 6.0% | PASS |

</details>

</details>

## Version-control latency

Wall time: 6m 59s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 27.57ms | 38.00ms | 72.5% | 1.1% | PASS |
| `status_dirty_many_tables` | 29.84ms | 42.00ms | 71.0% | 0.6% | PASS |
| `diff_regular_working_one_table` | 23.26ms | 33.00ms | 70.5% | 0.6% | PASS |
| `diff_regular_working_many_tables` | 33.69ms | 50.00ms | 67.4% | 0.6% | PASS |
| `diff_stat_working_many_tables` | 33.67ms | 48.00ms | 70.2% | 0.4% | PASS |
| `diff_schema_working_many_tables` | 34.21ms | 48.00ms | 71.3% | 0.6% | PASS |
| `branch_list_many_branches` | 17.70ms | 25.00ms | 70.8% | 0.8% | PASS |
| `branch_create_delete` | 19.39ms | 27.00ms | 71.8% | 2.2% | PASS |
| `at_literal_deep_history` | 20.17ms | 28.00ms | 72.0% | 1.8% | PASS |
| `diff_literal_deep_history` | 20.08ms | 28.00ms | 71.7% | 1.4% | PASS |
| `history_literal_deep_history` | 20.98ms | 30.00ms | 70.0% | 1.1% | PASS |
| `checkout_branch_clean` | 28.90ms | 43.00ms | 67.2% | 9.1% | PASS |
| `merge_data_no_conflicts` | 22.52ms | 33.00ms | 68.3% | 3.9% | PASS |
| `merge_data_secondary_index` | 714.00ms | 944.00ms | 75.6% | 2.2% | PASS |
| `merge_schema_no_conflicts` | 20.32ms | 24.00ms | 84.6% | 15.0% | PASS |
| `merge_data_conflicts` | 24.17ms | 33.00ms | 73.2% | 1.2% | PASS |
| `merge_data_conflicts_with_resolve` | 24.29ms | 34.00ms | 71.5% | 0.8% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
