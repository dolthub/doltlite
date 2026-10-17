# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-10-17 11:04 UTC
>
> Commit: [`b92803229f229125795dd6bee0a6f704e7f841f5`](https://github.com/dolthub/doltlite/commit/b92803229f229125795dd6bee0a6f704e7f841f5)
>
> Runner: ubuntu24 20261012.334.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/39021654696)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.13s | 10.49s | 1.1× | 1.0% | **PASS** |
| Writes | 1.90s | 3.01s | 1.6× | 1.1% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.99s | 10.78s | 1.1× | 1.4% | **PASS** |
| Writes | 3.00s | 3.54s | 1.2× | 1.3% | **PASS** |
| Autocommit writes | 645.18ms | 2.11s | 3.3× | 4.6% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 1.61s | 1.69s | 1.0× | 2.5% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.50s | 2.99s | 1.2× | 1.0% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.71s | 3.07s | 1.1× | 1.0% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.30s | 2.74s | 1.2× | 0.7% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 261.30ms | 409.50ms | 1.6× | 1.9% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 559.40ms | 924.43ms | 1.7× | 0.9% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 579.01ms | 897.89ms | 1.6× | 1.5% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 500.12ms | 776.60ms | 1.6× | 1.0% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 1.73s | 1.72s | 1.0× | 1.7% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.74s | 3.06s | 1.1× | 2.2% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 3.11s | 3.22s | 1.0× | 1.7% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.40s | 2.79s | 1.2× | 1.0% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 731.08ms | 690.78ms | 0.9× | 4.5% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 876.82ms | 1.02s | 1.2× | 3.8% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 795.40ms | 988.22ms | 1.2× | 1.2% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 596.68ms | 839.41ms | 1.4× | 1.1% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 1.76s | 1.76s | 1.0× | 2.4% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.60s | 3.07s | 1.2× | 1.1% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.80s | 3.15s | 1.1× | 0.9% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.34s | 2.79s | 1.2× | 0.9% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 170.90ms | 496.52ms | 2.9× | 4.1% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 194.16ms | 618.29ms | 3.2× | 5.6% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 147.62ms | 530.21ms | 3.6× | 3.8% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 132.51ms | 462.13ms | 3.5× | 6.2% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 14.97ms | 17.63ms | 1.2× | 2.7% | PASS |
| mem_reads | `oltp_range_select` | 6.10ms | 6.83ms | 1.1× | 2.7% | PASS |
| mem_reads | `oltp_sum_range` | 5.65ms | 6.87ms | 1.2× | 3.3% | PASS |
| mem_reads | `oltp_order_range` | 1.63ms | 1.79ms | 1.1× | 2.0% | PASS |
| mem_reads | `oltp_distinct_range` | 2.19ms | 2.35ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_index_scan` | 2.46ms | 3.65ms | 1.5× | 2.5% | PASS |
| mem_reads | `select_random_points` | 7.88ms | 8.33ms | 1.1× | 2.8% | PASS |
| mem_reads | `select_random_ranges` | 2.81ms | 3.00ms | 1.1× | 1.9% | PASS |
| mem_reads | `covering_index_scan` | 3.87ms | 5.41ms | 1.4× | 3.2% | PASS |
| mem_reads | `groupby_scan` | 16.32ms | 17.96ms | 1.1× | 2.1% | PASS |
| mem_reads | `index_join` | 3.34ms | 4.83ms | 1.4× | 1.3% | PASS |
| mem_reads | `index_join_scan` | 2.24ms | 3.83ms | 1.7× | 4.0% | PASS |
| mem_reads | `types_table_scan` | 667.53ms | 713.16ms | 1.1× | 1.3% | PASS |
| mem_reads | `table_scan` | 806.78ms | 819.25ms | 1.0× | 3.3% | PASS |
| mem_reads | `oltp_read_only` | 67.94ms | 71.20ms | 1.0× | 1.7% | PASS |
| mem_writes | `oltp_bulk_insert` | 108.39ms | 158.48ms | 1.5× | 2.0% | PASS |
| mem_writes | `oltp_insert` | 9.06ms | 15.90ms | 1.8× | 1.5% | PASS |
| mem_writes | `oltp_update_index` | 30.34ms | 54.08ms | 1.8× | 1.9% | PASS |
| mem_writes | `oltp_update_non_index` | 21.29ms | 32.93ms | 1.5× | 1.8% | PASS |
| mem_writes | `oltp_delete_insert` | 26.98ms | 42.06ms | 1.6× | 1.9% | PASS |
| mem_writes | `oltp_write_only` | 13.12ms | 26.20ms | 2.0× | 1.3% | PASS |
| mem_writes | `types_delete_insert` | 14.82ms | 21.29ms | 1.4× | 2.0% | PASS |
| mem_writes | `oltp_read_write` | 37.31ms | 58.55ms | 1.6× | 2.5% | PASS |
| file_reads | `oltp_point_select` | 67.18ms | 31.40ms | 0.5× | 1.2% | PASS |
| file_reads | `oltp_range_select` | 11.65ms | 8.27ms | 0.7× | 1.5% | PASS |
| file_reads | `oltp_sum_range` | 11.56ms | 8.44ms | 0.7× | 2.3% | PASS |
| file_reads | `oltp_order_range` | 2.32ms | 2.02ms | 0.9× | 2.1% | PASS |
| file_reads | `oltp_distinct_range` | 2.83ms | 2.54ms | 0.9× | 1.5% | PASS |
| file_reads | `oltp_index_scan` | 8.36ms | 5.19ms | 0.6× | 1.3% | PASS |
| file_reads | `select_random_points` | 13.70ms | 10.19ms | 0.7× | 2.6% | PASS |
| file_reads | `select_random_ranges` | 8.42ms | 4.58ms | 0.5× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 9.92ms | 7.37ms | 0.7× | 1.7% | PASS |
| file_reads | `groupby_scan` | 17.41ms | 18.31ms | 1.1× | 1.4% | PASS |
| file_reads | `index_join` | 6.85ms | 6.71ms | 1.0× | 3.0% | PASS |
| file_reads | `index_join_scan` | 2.93ms | 4.29ms | 1.5× | 3.9% | PASS |
| file_reads | `types_table_scan` | 678.00ms | 721.02ms | 1.1× | 2.2% | PASS |
| file_reads | `table_scan` | 762.35ms | 803.56ms | 1.1× | 1.9% | PASS |
| file_reads | `oltp_read_only` | 131.02ms | 83.36ms | 0.6× | 1.2% | PASS |
| file_writes | `oltp_bulk_insert` | 140.82ms | 198.30ms | 1.4× | 2.6% | PASS |
| file_writes | `oltp_insert` | 19.86ms | 25.29ms | 1.3× | 17.9% | PASS |
| file_writes | `oltp_update_index` | 121.60ms | 108.66ms | 0.9× | 7.0% | PASS |
| file_writes | `oltp_update_non_index` | 101.04ms | 77.78ms | 0.8× | 4.9% | PASS |
| file_writes | `oltp_delete_insert` | 101.47ms | 89.85ms | 0.9× | 2.5% | PASS |
| file_writes | `oltp_write_only` | 81.19ms | 60.19ms | 0.7× | 0.5% | PASS |
| file_writes | `types_delete_insert` | 60.91ms | 40.48ms | 0.7× | 8.6% | PASS |
| file_writes | `oltp_read_write` | 104.18ms | 90.23ms | 0.9× | 4.1% | PASS |
| ac_reads | `oltp_point_select` | 31.57ms | 31.54ms | 1.0× | 2.0% | PASS |
| ac_reads | `oltp_range_select` | 7.93ms | 8.12ms | 1.0× | 2.7% | PASS |
| ac_reads | `oltp_sum_range` | 7.22ms | 8.00ms | 1.1× | 2.4% | PASS |
| ac_reads | `oltp_order_range` | 1.82ms | 1.91ms | 1.0× | 2.1% | PASS |
| ac_reads | `oltp_distinct_range` | 2.37ms | 2.43ms | 1.0× | 2.8% | PASS |
| ac_reads | `oltp_index_scan` | 4.37ms | 4.95ms | 1.1× | 3.2% | PASS |
| ac_reads | `select_random_points` | 8.98ms | 9.62ms | 1.1× | 4.4% | PASS |
| ac_reads | `select_random_ranges` | 4.63ms | 4.40ms | 1.0× | 2.0% | PASS |
| ac_reads | `covering_index_scan` | 6.02ms | 7.13ms | 1.2× | 2.8% | PASS |
| ac_reads | `groupby_scan` | 16.70ms | 18.11ms | 1.1× | 1.4% | PASS |
| ac_reads | `index_join` | 4.85ms | 6.55ms | 1.4× | 1.9% | PASS |
| ac_reads | `index_join_scan` | 2.56ms | 4.34ms | 1.7× | 4.3% | PASS |
| ac_reads | `types_table_scan` | 697.54ms | 726.39ms | 1.0× | 2.5% | PASS |
| ac_reads | `table_scan` | 872.77ms | 836.21ms | 1.0× | 1.8% | PASS |
| ac_reads | `oltp_read_only` | 90.50ms | 89.50ms | 1.0× | 1.6% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 19.54ms | 51.93ms | 2.7× | 3.7% | PASS |
| ac_writes | `oltp_insert_ac` | 21.64ms | 61.23ms | 2.8× | 4.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 22.84ms | 69.63ms | 3.0× | 5.0% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 19.86ms | 56.91ms | 2.9× | 4.2% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 21.21ms | 63.91ms | 3.0× | 5.2% | PASS |
| ac_writes | `oltp_write_only_ac` | 21.88ms | 64.44ms | 2.9× | 3.0% | PASS |
| ac_writes | `types_delete_insert_ac` | 19.80ms | 60.79ms | 3.1× | 6.3% | PASS |
| ac_writes | `oltp_read_write_ac` | 24.12ms | 67.67ms | 2.8× | 2.5% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 32.08ms | 40.42ms | 1.3× | 0.8% | PASS |
| mem_reads | `oltp_range_select` | 15.04ms | 16.18ms | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_sum_range` | 14.45ms | 15.31ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_order_range` | 3.07ms | 3.68ms | 1.2× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 4.13ms | 4.78ms | 1.2× | 0.8% | PASS |
| mem_reads | `oltp_index_scan` | 3.71ms | 6.69ms | 1.8× | 1.3% | PASS |
| mem_reads | `select_random_points` | 19.89ms | 22.56ms | 1.1× | 1.3% | PASS |
| mem_reads | `select_random_ranges` | 5.93ms | 6.97ms | 1.2× | 1.2% | PASS |
| mem_reads | `covering_index_scan` | 7.57ms | 10.97ms | 1.5× | 0.9% | PASS |
| mem_reads | `groupby_scan` | 33.58ms | 36.39ms | 1.1× | 0.5% | PASS |
| mem_reads | `index_join` | 9.46ms | 10.39ms | 1.1× | 1.0% | PASS |
| mem_reads | `index_join_scan` | 3.28ms | 6.37ms | 1.9× | 1.0% | PASS |
| mem_reads | `types_table_scan` | 1.05s | 1.28s | 1.2× | 0.7% | PASS |
| mem_reads | `table_scan` | 1.17s | 1.38s | 1.2× | 0.5% | PASS |
| mem_reads | `oltp_read_only` | 131.57ms | 151.37ms | 1.2× | 0.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 230.66ms | 332.13ms | 1.4× | 1.2% | PASS |
| mem_writes | `oltp_insert` | 17.34ms | 36.98ms | 2.1× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 61.71ms | 130.35ms | 2.1× | 0.9% | PASS |
| mem_writes | `oltp_update_non_index` | 44.38ms | 80.02ms | 1.8× | 0.9% | PASS |
| mem_writes | `oltp_delete_insert` | 50.74ms | 97.05ms | 1.9× | 0.7% | PASS |
| mem_writes | `oltp_write_only` | 25.93ms | 55.21ms | 2.1× | 0.7% | PASS |
| mem_writes | `types_delete_insert` | 36.99ms | 54.56ms | 1.5× | 1.1% | PASS |
| mem_writes | `oltp_read_write` | 91.65ms | 138.13ms | 1.5× | 0.8% | PASS |
| file_reads | `oltp_point_select` | 102.92ms | 59.42ms | 0.6× | 1.3% | PASS |
| file_reads | `oltp_range_select` | 22.47ms | 18.29ms | 0.8× | 2.8% | PASS |
| file_reads | `oltp_sum_range` | 22.39ms | 17.52ms | 0.8× | 3.3% | PASS |
| file_reads | `oltp_order_range` | 4.07ms | 3.80ms | 0.9× | 2.9% | PASS |
| file_reads | `oltp_distinct_range` | 5.02ms | 4.92ms | 1.0× | 2.2% | PASS |
| file_reads | `oltp_index_scan` | 11.00ms | 8.78ms | 0.8× | 2.2% | PASS |
| file_reads | `select_random_points` | 28.55ms | 25.65ms | 0.9× | 2.5% | PASS |
| file_reads | `select_random_ranges` | 13.85ms | 9.08ms | 0.7× | 3.0% | PASS |
| file_reads | `covering_index_scan` | 14.96ms | 13.04ms | 0.9× | 1.5% | PASS |
| file_reads | `groupby_scan` | 34.74ms | 36.65ms | 1.1× | 1.3% | PASS |
| file_reads | `index_join` | 13.73ms | 11.77ms | 0.9× | 3.4% | PASS |
| file_reads | `index_join_scan` | 4.53ms | 6.76ms | 1.5× | 3.0% | PASS |
| file_reads | `types_table_scan` | 1.05s | 1.28s | 1.2× | 0.6% | PASS |
| file_reads | `table_scan` | 1.18s | 1.39s | 1.2× | 0.8% | PASS |
| file_reads | `oltp_read_only` | 234.39ms | 177.97ms | 0.8× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 254.53ms | 347.69ms | 1.4× | 1.0% | PASS |
| file_writes | `oltp_insert` | 24.65ms | 42.04ms | 1.7× | 1.1% | PASS |
| file_writes | `oltp_update_index` | 112.38ms | 147.28ms | 1.3× | 9.5% | PASS |
| file_writes | `oltp_update_non_index` | 110.83ms | 92.33ms | 0.8× | 5.9% | PASS |
| file_writes | `oltp_delete_insert` | 89.54ms | 111.71ms | 1.2× | 1.6% | PASS |
| file_writes | `oltp_write_only` | 67.41ms | 66.76ms | 1.0× | 14.2% | PASS |
| file_writes | `types_delete_insert` | 67.53ms | 64.86ms | 1.0× | 1.3% | PASS |
| file_writes | `oltp_read_write` | 149.95ms | 149.74ms | 1.0× | 7.8% | PASS |
| ac_reads | `oltp_point_select` | 55.42ms | 59.60ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 17.96ms | 18.39ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_sum_range` | 17.18ms | 17.49ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_order_range` | 3.41ms | 3.81ms | 1.1× | 1.0% | PASS |
| ac_reads | `oltp_distinct_range` | 4.44ms | 4.93ms | 1.1× | 0.9% | PASS |
| ac_reads | `oltp_index_scan` | 6.11ms | 8.81ms | 1.4× | 1.3% | PASS |
| ac_reads | `select_random_points` | 23.29ms | 25.74ms | 1.1× | 1.3% | PASS |
| ac_reads | `select_random_ranges` | 8.49ms | 9.07ms | 1.1× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 9.94ms | 12.96ms | 1.3× | 1.2% | PASS |
| ac_reads | `groupby_scan` | 33.83ms | 36.71ms | 1.1× | 0.5% | PASS |
| ac_reads | `index_join` | 12.40ms | 11.91ms | 1.0× | 3.5% | PASS |
| ac_reads | `index_join_scan` | 4.14ms | 6.92ms | 1.7× | 3.0% | PASS |
| ac_reads | `types_table_scan` | 1.05s | 1.29s | 1.2× | 0.9% | PASS |
| ac_reads | `table_scan` | 1.18s | 1.39s | 1.2× | 0.9% | PASS |
| ac_reads | `oltp_read_only` | 168.24ms | 178.61ms | 1.1× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.76ms | 62.76ms | 2.9× | 5.5% | PASS |
| ac_writes | `oltp_insert_ac` | 25.00ms | 75.34ms | 3.0× | 7.3% | PASS |
| ac_writes | `oltp_update_index_ac` | 25.30ms | 90.53ms | 3.6× | 7.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.44ms | 70.18ms | 3.1× | 5.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 23.86ms | 82.12ms | 3.4× | 4.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 23.65ms | 80.61ms | 3.4× | 4.7% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.76ms | 70.55ms | 3.1× | 6.8% | PASS |
| ac_writes | `oltp_read_write_ac` | 29.39ms | 86.19ms | 2.9× | 4.1% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 34.13ms | 37.09ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_range_select` | 16.89ms | 15.04ms | 0.9× | 1.4% | PASS |
| mem_reads | `oltp_sum_range` | 14.81ms | 14.40ms | 1.0× | 1.0% | PASS |
| mem_reads | `oltp_order_range` | 3.65ms | 3.46ms | 0.9× | 1.0% | PASS |
| mem_reads | `oltp_distinct_range` | 4.73ms | 4.51ms | 1.0× | 0.7% | PASS |
| mem_reads | `oltp_index_scan` | 3.94ms | 6.38ms | 1.6× | 1.0% | PASS |
| mem_reads | `select_random_points` | 21.14ms | 21.32ms | 1.0× | 3.0% | PASS |
| mem_reads | `select_random_ranges` | 6.41ms | 6.36ms | 1.0× | 1.4% | PASS |
| mem_reads | `covering_index_scan` | 7.54ms | 10.27ms | 1.4× | 0.7% | PASS |
| mem_reads | `groupby_scan` | 36.78ms | 37.78ms | 1.0× | 0.5% | PASS |
| mem_reads | `index_join` | 10.30ms | 10.62ms | 1.0× | 1.8% | PASS |
| mem_reads | `index_join_scan` | 3.75ms | 6.56ms | 1.8× | 1.8% | PASS |
| mem_reads | `types_table_scan` | 1.13s | 1.32s | 1.2× | 0.8% | PASS |
| mem_reads | `table_scan` | 1.28s | 1.43s | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_read_only` | 141.85ms | 140.23ms | 1.0× | 1.5% | PASS |
| mem_writes | `oltp_bulk_insert` | 238.10ms | 315.75ms | 1.3× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 18.44ms | 37.89ms | 2.1× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 63.84ms | 127.44ms | 2.0× | 1.0% | PASS |
| mem_writes | `oltp_update_non_index` | 48.05ms | 78.25ms | 1.6× | 2.1% | PASS |
| mem_writes | `oltp_delete_insert` | 53.01ms | 97.37ms | 1.8× | 1.7% | PASS |
| mem_writes | `oltp_write_only` | 27.56ms | 57.34ms | 2.1× | 1.3% | PASS |
| mem_writes | `types_delete_insert` | 37.06ms | 53.10ms | 1.4× | 1.6% | PASS |
| mem_writes | `oltp_read_write` | 92.95ms | 130.75ms | 1.4× | 1.7% | PASS |
| file_reads | `oltp_point_select` | 118.62ms | 59.66ms | 0.5× | 0.7% | PASS |
| file_reads | `oltp_range_select` | 25.37ms | 17.56ms | 0.7× | 2.6% | PASS |
| file_reads | `oltp_sum_range` | 23.60ms | 16.84ms | 0.7× | 1.7% | PASS |
| file_reads | `oltp_order_range` | 4.62ms | 3.73ms | 0.8× | 2.6% | PASS |
| file_reads | `oltp_distinct_range` | 5.73ms | 4.81ms | 0.8× | 1.0% | PASS |
| file_reads | `oltp_index_scan` | 12.48ms | 8.76ms | 0.7× | 1.4% | PASS |
| file_reads | `select_random_points` | 30.63ms | 23.68ms | 0.8× | 1.8% | PASS |
| file_reads | `select_random_ranges` | 15.06ms | 8.72ms | 0.6× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 16.22ms | 12.47ms | 0.8× | 1.2% | PASS |
| file_reads | `groupby_scan` | 38.23ms | 38.43ms | 1.0× | 0.8% | PASS |
| file_reads | `index_join` | 15.69ms | 11.97ms | 0.8× | 1.7% | PASS |
| file_reads | `index_join_scan` | 4.89ms | 7.02ms | 1.4× | 2.3% | PASS |
| file_reads | `types_table_scan` | 1.23s | 1.38s | 1.1× | 2.2% | PASS |
| file_reads | `table_scan` | 1.31s | 1.46s | 1.1× | 2.6% | PASS |
| file_reads | `oltp_read_only` | 262.46ms | 172.66ms | 0.7× | 0.7% | PASS |
| file_writes | `oltp_bulk_insert` | 262.06ms | 330.04ms | 1.3× | 0.8% | PASS |
| file_writes | `oltp_insert` | 24.75ms | 42.18ms | 1.7× | 0.7% | PASS |
| file_writes | `oltp_update_index` | 93.48ms | 143.27ms | 1.5× | 1.2% | PASS |
| file_writes | `oltp_update_non_index` | 91.16ms | 90.33ms | 1.0× | 13.5% | PASS |
| file_writes | `oltp_delete_insert` | 83.82ms | 108.98ms | 1.3× | 1.1% | PASS |
| file_writes | `oltp_write_only` | 56.42ms | 68.86ms | 1.2× | 2.7% | PASS |
| file_writes | `types_delete_insert` | 62.05ms | 62.28ms | 1.0× | 1.2% | PASS |
| file_writes | `oltp_read_write` | 121.66ms | 142.27ms | 1.2× | 1.4% | PASS |
| ac_reads | `oltp_point_select` | 62.23ms | 59.08ms | 0.9× | 0.9% | PASS |
| ac_reads | `oltp_range_select` | 20.39ms | 17.58ms | 0.9× | 1.6% | PASS |
| ac_reads | `oltp_sum_range` | 18.36ms | 16.77ms | 0.9× | 1.6% | PASS |
| ac_reads | `oltp_order_range` | 4.14ms | 3.73ms | 0.9× | 0.7% | PASS |
| ac_reads | `oltp_distinct_range` | 5.07ms | 4.78ms | 0.9× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 6.98ms | 8.63ms | 1.2× | 0.9% | PASS |
| ac_reads | `select_random_points` | 24.67ms | 23.25ms | 0.9× | 1.2% | PASS |
| ac_reads | `select_random_ranges` | 9.48ms | 8.63ms | 0.9× | 0.8% | PASS |
| ac_reads | `covering_index_scan` | 10.74ms | 12.43ms | 1.2× | 1.1% | PASS |
| ac_reads | `groupby_scan` | 37.16ms | 38.03ms | 1.0× | 0.5% | PASS |
| ac_reads | `index_join` | 12.64ms | 11.73ms | 0.9× | 1.4% | PASS |
| ac_reads | `index_join_scan` | 4.26ms | 6.79ms | 1.6× | 1.6% | PASS |
| ac_reads | `types_table_scan` | 1.12s | 1.32s | 1.2× | 0.6% | PASS |
| ac_reads | `table_scan` | 1.28s | 1.44s | 1.1× | 0.6% | PASS |
| ac_reads | `oltp_read_only` | 182.42ms | 172.66ms | 0.9× | 0.7% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 16.84ms | 51.30ms | 3.0× | 5.6% | PASS |
| ac_writes | `oltp_insert_ac` | 18.50ms | 67.80ms | 3.7× | 4.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 19.00ms | 78.41ms | 4.1× | 3.5% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 15.60ms | 59.24ms | 3.8× | 3.7% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 18.30ms | 69.30ms | 3.8× | 3.9% | PASS |
| ac_writes | `oltp_write_only_ac` | 18.00ms | 68.68ms | 3.8× | 3.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 16.73ms | 60.69ms | 3.6× | 4.1% | PASS |
| ac_writes | `oltp_read_write_ac` | 24.64ms | 74.79ms | 3.0× | 1.9% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 28.83ms | 35.68ms | 1.2× | 0.9% | PASS |
| mem_reads | `oltp_range_select` | 15.80ms | 19.96ms | 1.3× | 0.5% | PASS |
| mem_reads | `oltp_sum_range` | 14.98ms | 18.48ms | 1.2× | 0.4% | PASS |
| mem_reads | `oltp_order_range` | 3.08ms | 3.62ms | 1.2× | 0.7% | PASS |
| mem_reads | `oltp_distinct_range` | 3.87ms | 4.54ms | 1.2× | 0.7% | PASS |
| mem_reads | `oltp_index_scan` | 3.90ms | 5.41ms | 1.4× | 0.9% | PASS |
| mem_reads | `select_random_points` | 24.39ms | 29.64ms | 1.2× | 0.6% | PASS |
| mem_reads | `select_random_ranges` | 6.39ms | 8.01ms | 1.3× | 0.9% | PASS |
| mem_reads | `covering_index_scan` | 6.54ms | 9.10ms | 1.4× | 0.7% | PASS |
| mem_reads | `groupby_scan` | 31.52ms | 37.08ms | 1.2× | 0.4% | PASS |
| mem_reads | `index_join` | 6.75ms | 10.19ms | 1.5× | 1.0% | PASS |
| mem_reads | `index_join_scan` | 3.25ms | 5.85ms | 1.8× | 1.0% | PASS |
| mem_reads | `types_table_scan` | 944.92ms | 1.14s | 1.2× | 0.4% | PASS |
| mem_reads | `table_scan` | 1.08s | 1.26s | 1.2× | 0.5% | PASS |
| mem_reads | `oltp_read_only` | 124.61ms | 153.07ms | 1.2× | 0.7% | PASS |
| mem_writes | `oltp_bulk_insert` | 208.87ms | 282.81ms | 1.4× | 1.3% | PASS |
| mem_writes | `oltp_insert` | 16.34ms | 29.10ms | 1.8× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 58.05ms | 103.88ms | 1.8× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 42.90ms | 67.47ms | 1.6× | 0.9% | PASS |
| mem_writes | `oltp_delete_insert` | 42.11ms | 76.94ms | 1.8× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 22.43ms | 44.93ms | 2.0× | 1.1% | PASS |
| mem_writes | `types_delete_insert` | 26.85ms | 44.04ms | 1.6× | 0.8% | PASS |
| mem_writes | `oltp_read_write` | 82.58ms | 127.43ms | 1.5× | 0.9% | PASS |
| file_reads | `oltp_point_select` | 58.59ms | 45.26ms | 0.8× | 1.1% | PASS |
| file_reads | `oltp_range_select` | 18.88ms | 21.34ms | 1.1× | 1.1% | PASS |
| file_reads | `oltp_sum_range` | 18.15ms | 19.88ms | 1.1× | 1.0% | PASS |
| file_reads | `oltp_order_range` | 3.41ms | 3.82ms | 1.1× | 0.9% | PASS |
| file_reads | `oltp_distinct_range` | 4.24ms | 4.74ms | 1.1× | 0.8% | PASS |
| file_reads | `oltp_index_scan` | 6.94ms | 6.71ms | 1.0× | 1.2% | PASS |
| file_reads | `select_random_points` | 27.86ms | 31.41ms | 1.1× | 0.7% | PASS |
| file_reads | `select_random_ranges` | 9.67ms | 9.34ms | 1.0× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 9.84ms | 10.31ms | 1.0× | 1.1% | PASS |
| file_reads | `groupby_scan` | 31.76ms | 37.43ms | 1.2× | 0.6% | PASS |
| file_reads | `index_join` | 8.61ms | 11.25ms | 1.3× | 1.0% | PASS |
| file_reads | `index_join_scan` | 3.66ms | 6.07ms | 1.7× | 1.1% | PASS |
| file_reads | `types_table_scan` | 947.03ms | 1.15s | 1.2× | 0.5% | PASS |
| file_reads | `table_scan` | 1.08s | 1.26s | 1.2× | 0.4% | PASS |
| file_reads | `oltp_read_only` | 170.75ms | 169.36ms | 1.0× | 0.5% | PASS |
| file_writes | `oltp_bulk_insert` | 222.50ms | 293.09ms | 1.3× | 1.2% | PASS |
| file_writes | `oltp_insert` | 20.53ms | 32.41ms | 1.6× | 1.2% | PASS |
| file_writes | `oltp_update_index` | 73.84ms | 115.41ms | 1.6× | 0.8% | PASS |
| file_writes | `oltp_update_non_index` | 56.86ms | 74.74ms | 1.3× | 0.9% | PASS |
| file_writes | `oltp_delete_insert` | 56.42ms | 87.11ms | 1.5× | 1.0% | PASS |
| file_writes | `oltp_write_only` | 34.67ms | 52.45ms | 1.5× | 1.6% | PASS |
| file_writes | `types_delete_insert` | 36.31ms | 49.70ms | 1.4× | 1.2% | PASS |
| file_writes | `oltp_read_write` | 95.55ms | 134.51ms | 1.4× | 0.9% | PASS |
| ac_reads | `oltp_point_select` | 38.40ms | 45.32ms | 1.2× | 0.9% | PASS |
| ac_reads | `oltp_range_select` | 16.88ms | 21.30ms | 1.3× | 0.7% | PASS |
| ac_reads | `oltp_sum_range` | 16.09ms | 19.86ms | 1.2× | 1.0% | PASS |
| ac_reads | `oltp_order_range` | 3.21ms | 3.83ms | 1.2× | 1.1% | PASS |
| ac_reads | `oltp_distinct_range` | 4.02ms | 4.74ms | 1.2× | 0.9% | PASS |
| ac_reads | `oltp_index_scan` | 4.96ms | 6.70ms | 1.4× | 1.5% | PASS |
| ac_reads | `select_random_points` | 25.78ms | 31.33ms | 1.2× | 1.0% | PASS |
| ac_reads | `select_random_ranges` | 7.58ms | 9.34ms | 1.2× | 0.8% | PASS |
| ac_reads | `covering_index_scan` | 7.63ms | 10.31ms | 1.4× | 1.4% | PASS |
| ac_reads | `groupby_scan` | 31.73ms | 37.39ms | 1.2× | 0.3% | PASS |
| ac_reads | `index_join` | 7.51ms | 11.29ms | 1.5× | 1.0% | PASS |
| ac_reads | `index_join_scan` | 3.47ms | 6.10ms | 1.8× | 1.1% | PASS |
| ac_reads | `types_table_scan` | 948.42ms | 1.15s | 1.2× | 0.4% | PASS |
| ac_reads | `table_scan` | 1.08s | 1.26s | 1.2× | 0.4% | PASS |
| ac_reads | `oltp_read_only` | 138.95ms | 168.92ms | 1.2× | 0.6% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 14.59ms | 45.81ms | 3.1× | 7.5% | PASS |
| ac_writes | `oltp_insert_ac` | 16.26ms | 57.63ms | 3.5× | 7.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 17.64ms | 67.39ms | 3.8× | 4.3% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 14.62ms | 52.44ms | 3.6× | 5.9% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 15.96ms | 59.90ms | 3.8× | 6.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 16.28ms | 59.52ms | 3.7× | 6.4% | PASS |
| ac_writes | `types_delete_insert_ac` | 16.15ms | 52.95ms | 3.3× | 7.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 21.01ms | 66.49ms | 3.2× | 3.5% | PASS |

</details>

</details>

## Version-control latency

Wall time: 4m 22s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 34.20ms | 38.00ms | 90.0% | 0.4% | PASS |
| `status_dirty_many_tables` | 37.83ms | 42.00ms | 90.1% | 0.6% | PASS |
| `diff_regular_working_one_table` | 29.50ms | 33.00ms | 89.4% | 0.5% | PASS |
| `diff_regular_working_many_tables` | 42.89ms | 50.00ms | 85.8% | 0.6% | PASS |
| `diff_stat_working_many_tables` | 43.48ms | 48.00ms | 90.6% | 1.0% | PASS |
| `diff_schema_working_many_tables` | 44.31ms | 48.00ms | 92.3% | 1.5% | PASS |
| `branch_list_many_branches` | 22.87ms | 25.00ms | 91.5% | 2.5% | PASS |
| `branch_create_delete` | 24.93ms | 27.00ms | 92.3% | 1.2% | PASS |
| `at_literal_deep_history` | 25.20ms | 28.00ms | 90.0% | 1.6% | PASS |
| `diff_literal_deep_history` | 24.78ms | 28.00ms | 88.5% | 1.3% | PASS |
| `history_literal_deep_history` | 25.74ms | 30.00ms | 85.8% | 1.5% | PASS |
| `checkout_branch_clean` | 32.84ms | 43.00ms | 76.4% | 1.5% | PASS |
| `merge_data_no_conflicts` | 27.37ms | 33.00ms | 83.0% | 1.2% | PASS |
| `merge_data_secondary_index` | 829.47ms | 944.00ms | 87.9% | 0.9% | PASS |
| `merge_schema_no_conflicts` | 20.99ms | 24.00ms | 87.5% | 0.7% | PASS |
| `merge_data_conflicts` | 29.90ms | 33.00ms | 90.6% | 0.4% | PASS |
| `merge_data_conflicts_with_resolve` | 30.72ms | 34.00ms | 90.3% | 0.6% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
