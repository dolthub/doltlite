# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-09-22 11:09 UTC
>
> Commit: [`6eea51a61989e4b51ab45105e9b0af6103ca479a`](https://github.com/dolthub/doltlite/commit/6eea51a61989e4b51ab45105e9b0af6103ca479a)
>
> Runner: ubuntu24 20260907.300.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/35711260924)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.70s | 10.63s | 1.1× | 1.5% | **PASS** |
| Writes | 2.01s | 3.24s | 1.6× | 1.3% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.29s | 10.84s | 1.1× | 1.2% | **PASS** |
| Writes | 3.85s | 4.31s | 1.1× | 10.1% | **PASS** |
| Autocommit writes | 1.20s | 3.99s | 3.3× | 16.0% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.20s | 2.19s | 1.0× | 1.3% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.62s | 3.05s | 1.2× | 1.3% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.58s | 3.03s | 1.2× | 1.5% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.29s | 2.36s | 1.0× | 1.8% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 352.45ms | 545.42ms | 1.5× | 1.9% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 584.02ms | 992.01ms | 1.7× | 1.0% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 584.76ms | 980.87ms | 1.7× | 1.5% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 488.82ms | 722.24ms | 1.5× | 1.7% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.22s | 2.20s | 1.0× | 0.9% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.82s | 3.11s | 1.1× | 1.5% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.81s | 3.11s | 1.1× | 1.4% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.45s | 2.42s | 1.0× | 1.0% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 1.03s | 968.13ms | 0.9× | 24.7% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 916.65ms | 1.12s | 1.2× | 3.7% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 806.45ms | 1.10s | 1.4× | 1.8% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 1.10s | 1.13s | 1.0× | 14.0% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.13s | 2.22s | 1.0× | 1.1% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.78s | 3.14s | 1.1× | 1.7% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.65s | 3.11s | 1.2× | 1.7% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.50s | 2.45s | 1.0× | 1.2% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 451.02ms | 1.45s | 3.2× | 57.0% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 207.87ms | 736.00ms | 3.5× | 5.7% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 217.09ms | 776.93ms | 3.6× | 6.3% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 320.36ms | 1.03s | 3.2× | 39.5% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 19.38ms | 21.97ms | 1.1× | 1.7% | PASS |
| mem_reads | `oltp_range_select` | 9.08ms | 8.84ms | 1.0× | 3.3% | PASS |
| mem_reads | `oltp_sum_range` | 7.64ms | 8.54ms | 1.1× | 2.5% | PASS |
| mem_reads | `oltp_order_range` | 2.15ms | 2.26ms | 1.0× | 1.7% | PASS |
| mem_reads | `oltp_distinct_range` | 2.95ms | 3.05ms | 1.0× | 0.8% | PASS |
| mem_reads | `oltp_index_scan` | 3.06ms | 3.71ms | 1.2× | 2.1% | PASS |
| mem_reads | `select_random_points` | 8.59ms | 8.94ms | 1.0× | 2.8% | PASS |
| mem_reads | `select_random_ranges` | 3.79ms | 3.66ms | 1.0× | 2.0% | PASS |
| mem_reads | `covering_index_scan` | 5.86ms | 6.88ms | 1.2× | 1.2% | PASS |
| mem_reads | `groupby_scan` | 24.74ms | 26.29ms | 1.1× | 0.9% | PASS |
| mem_reads | `index_join` | 4.49ms | 5.60ms | 1.2× | 1.2% | PASS |
| mem_reads | `index_join_scan` | 2.57ms | 3.70ms | 1.4× | 1.1% | PASS |
| mem_reads | `types_table_scan` | 882.87ms | 934.82ms | 1.1× | 1.3% | PASS |
| mem_reads | `table_scan` | 1.13s | 1.06s | 0.9× | 0.9% | PASS |
| mem_reads | `oltp_read_only` | 89.52ms | 93.50ms | 1.0× | 1.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 139.22ms | 199.08ms | 1.4× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 12.18ms | 21.62ms | 1.8× | 1.2% | PASS |
| mem_writes | `oltp_update_index` | 43.96ms | 78.86ms | 1.8× | 2.5% | PASS |
| mem_writes | `oltp_update_non_index` | 29.29ms | 41.94ms | 1.4× | 1.8% | PASS |
| mem_writes | `oltp_delete_insert` | 36.13ms | 55.93ms | 1.5× | 2.6% | PASS |
| mem_writes | `oltp_write_only` | 18.14ms | 35.46ms | 2.0× | 2.1% | PASS |
| mem_writes | `types_delete_insert` | 19.90ms | 27.46ms | 1.4× | 2.1% | PASS |
| mem_writes | `oltp_read_write` | 53.63ms | 85.07ms | 1.6× | 1.8% | PASS |
| file_reads | `oltp_point_select` | 85.48ms | 39.35ms | 0.5× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 16.30ms | 10.86ms | 0.7× | 1.4% | PASS |
| file_reads | `oltp_sum_range` | 14.11ms | 10.31ms | 0.7× | 0.9% | PASS |
| file_reads | `oltp_order_range` | 2.92ms | 2.54ms | 0.9× | 1.1% | PASS |
| file_reads | `oltp_distinct_range` | 3.72ms | 3.35ms | 0.9× | 1.1% | PASS |
| file_reads | `oltp_index_scan` | 9.96ms | 5.90ms | 0.6× | 1.1% | PASS |
| file_reads | `select_random_points` | 15.34ms | 10.70ms | 0.7× | 0.7% | PASS |
| file_reads | `select_random_ranges` | 10.31ms | 5.47ms | 0.5× | 0.8% | PASS |
| file_reads | `covering_index_scan` | 12.82ms | 9.04ms | 0.7× | 0.8% | PASS |
| file_reads | `groupby_scan` | 25.24ms | 26.60ms | 1.1× | 0.8% | PASS |
| file_reads | `index_join` | 8.26ms | 7.37ms | 0.9× | 1.2% | PASS |
| file_reads | `index_join_scan` | 3.40ms | 4.15ms | 1.2× | 1.0% | PASS |
| file_reads | `types_table_scan` | 861.74ms | 926.23ms | 1.1× | 0.3% | PASS |
| file_reads | `table_scan` | 975.39ms | 1.03s | 1.1× | 0.4% | PASS |
| file_reads | `oltp_read_only` | 171.55ms | 112.47ms | 0.7× | 0.3% | PASS |
| file_writes | `oltp_bulk_insert` | 214.48ms | 277.98ms | 1.3× | 21.9% | PASS |
| file_writes | `oltp_insert` | 27.83ms | 36.55ms | 1.3× | 9.3% | PASS |
| file_writes | `oltp_update_index` | 139.81ms | 137.81ms | 1.0× | 16.8% | PASS |
| file_writes | `oltp_update_non_index` | 131.70ms | 87.85ms | 0.7× | 25.7% | PASS |
| file_writes | `oltp_delete_insert` | 133.46ms | 116.03ms | 0.9× | 23.6% | PASS |
| file_writes | `oltp_write_only` | 157.52ms | 102.34ms | 0.6× | 47.6% | PASS |
| file_writes | `types_delete_insert` | 75.64ms | 66.24ms | 0.9× | 28.2% | PASS |
| file_writes | `oltp_read_write` | 148.37ms | 143.34ms | 1.0× | 26.9% | PASS |
| ac_reads | `oltp_point_select` | 40.14ms | 38.63ms | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_range_select` | 10.94ms | 10.62ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_sum_range` | 9.69ms | 10.29ms | 1.1× | 0.9% | PASS |
| ac_reads | `oltp_order_range` | 2.51ms | 2.52ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_distinct_range` | 3.27ms | 3.34ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 5.62ms | 5.84ms | 1.0× | 1.1% | PASS |
| ac_reads | `select_random_points` | 10.87ms | 10.70ms | 1.0× | 0.9% | PASS |
| ac_reads | `select_random_ranges` | 5.96ms | 5.45ms | 0.9× | 1.4% | PASS |
| ac_reads | `covering_index_scan` | 8.42ms | 9.04ms | 1.1× | 1.1% | PASS |
| ac_reads | `groupby_scan` | 24.70ms | 26.56ms | 1.1× | 0.6% | PASS |
| ac_reads | `index_join` | 6.05ms | 7.32ms | 1.2× | 1.3% | PASS |
| ac_reads | `index_join_scan` | 3.05ms | 4.19ms | 1.4× | 1.8% | PASS |
| ac_reads | `types_table_scan` | 911.63ms | 943.77ms | 1.0× | 3.2% | PASS |
| ac_reads | `table_scan` | 977.61ms | 1.03s | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_read_only` | 109.91ms | 112.52ms | 1.0× | 0.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 58.25ms | 172.07ms | 3.0× | 57.3% | PASS |
| ac_writes | `oltp_insert_ac` | 48.13ms | 143.48ms | 3.0× | 55.3% | PASS |
| ac_writes | `oltp_update_index_ac` | 81.96ms | 201.44ms | 2.5× | 70.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 38.08ms | 130.08ms | 3.4× | 39.2% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 37.47ms | 137.72ms | 3.7× | 36.2% | PASS |
| ac_writes | `oltp_write_only_ac` | 85.72ms | 241.01ms | 2.8× | 67.0% | PASS |
| ac_writes | `types_delete_insert_ac` | 33.89ms | 132.52ms | 3.9× | 56.8% | PASS |
| ac_writes | `oltp_read_write_ac` | 67.52ms | 291.54ms | 4.3× | 72.8% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 36.16ms | 39.37ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_range_select` | 17.04ms | 14.53ms | 0.9× | 1.6% | PASS |
| mem_reads | `oltp_sum_range` | 15.51ms | 14.62ms | 0.9× | 1.2% | PASS |
| mem_reads | `oltp_order_range` | 3.35ms | 3.31ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 4.44ms | 4.47ms | 1.0× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 4.04ms | 6.49ms | 1.6× | 1.3% | PASS |
| mem_reads | `select_random_points` | 23.30ms | 22.46ms | 1.0× | 2.3% | PASS |
| mem_reads | `select_random_ranges` | 6.94ms | 6.64ms | 1.0× | 1.3% | PASS |
| mem_reads | `covering_index_scan` | 7.79ms | 9.88ms | 1.3× | 0.7% | PASS |
| mem_reads | `groupby_scan` | 34.18ms | 35.43ms | 1.0× | 1.0% | PASS |
| mem_reads | `index_join` | 10.62ms | 9.81ms | 0.9× | 2.1% | PASS |
| mem_reads | `index_join_scan` | 3.76ms | 5.98ms | 1.6× | 1.8% | PASS |
| mem_reads | `types_table_scan` | 1.09s | 1.36s | 1.2× | 2.3% | PASS |
| mem_reads | `table_scan` | 1.23s | 1.38s | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_read_only` | 136.40ms | 141.18ms | 1.0× | 1.7% | PASS |
| mem_writes | `oltp_bulk_insert` | 241.55ms | 363.58ms | 1.5× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 17.80ms | 38.48ms | 2.2× | 0.6% | PASS |
| mem_writes | `oltp_update_index` | 64.82ms | 139.04ms | 2.1× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 46.76ms | 82.20ms | 1.8× | 1.1% | PASS |
| mem_writes | `oltp_delete_insert` | 52.90ms | 104.48ms | 2.0× | 0.9% | PASS |
| mem_writes | `oltp_write_only` | 27.57ms | 61.35ms | 2.2× | 1.2% | PASS |
| mem_writes | `types_delete_insert` | 37.44ms | 54.63ms | 1.5× | 1.1% | PASS |
| mem_writes | `oltp_read_write` | 95.18ms | 148.25ms | 1.6× | 0.9% | PASS |
| file_reads | `oltp_point_select` | 105.10ms | 58.69ms | 0.6× | 1.5% | PASS |
| file_reads | `oltp_range_select` | 23.46ms | 16.86ms | 0.7× | 1.4% | PASS |
| file_reads | `oltp_sum_range` | 21.99ms | 17.03ms | 0.8× | 2.7% | PASS |
| file_reads | `oltp_order_range` | 4.15ms | 3.62ms | 0.9× | 2.0% | PASS |
| file_reads | `oltp_distinct_range` | 5.18ms | 4.76ms | 0.9× | 1.5% | PASS |
| file_reads | `oltp_index_scan` | 11.09ms | 8.69ms | 0.8× | 1.4% | PASS |
| file_reads | `select_random_points` | 30.09ms | 25.36ms | 0.8× | 2.7% | PASS |
| file_reads | `select_random_ranges` | 13.88ms | 8.82ms | 0.6× | 1.7% | PASS |
| file_reads | `covering_index_scan` | 15.13ms | 12.47ms | 0.8× | 1.3% | PASS |
| file_reads | `groupby_scan` | 34.39ms | 35.85ms | 1.0× | 1.2% | PASS |
| file_reads | `index_join` | 14.40ms | 11.40ms | 0.8× | 2.0% | PASS |
| file_reads | `index_join_scan` | 4.59ms | 6.46ms | 1.4× | 4.6% | PASS |
| file_reads | `types_table_scan` | 1.07s | 1.36s | 1.3× | 0.5% | PASS |
| file_reads | `table_scan` | 1.22s | 1.37s | 1.1× | 0.6% | PASS |
| file_reads | `oltp_read_only` | 241.60ms | 171.32ms | 0.7× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 265.46ms | 379.73ms | 1.4× | 1.0% | PASS |
| file_writes | `oltp_insert` | 25.42ms | 45.41ms | 1.8× | 2.1% | PASS |
| file_writes | `oltp_update_index` | 127.08ms | 161.04ms | 1.3× | 10.8% | PASS |
| file_writes | `oltp_update_non_index` | 101.73ms | 101.10ms | 1.0× | 8.0% | PASS |
| file_writes | `oltp_delete_insert` | 94.26ms | 122.94ms | 1.3× | 1.6% | PASS |
| file_writes | `oltp_write_only` | 83.83ms | 76.06ms | 0.9× | 19.8% | PASS |
| file_writes | `types_delete_insert` | 69.74ms | 67.28ms | 1.0× | 1.4% | PASS |
| file_writes | `oltp_read_write` | 149.14ms | 163.26ms | 1.1× | 5.2% | PASS |
| ac_reads | `oltp_point_select` | 59.87ms | 58.51ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_range_select` | 19.58ms | 16.63ms | 0.8× | 2.2% | PASS |
| ac_reads | `oltp_sum_range` | 18.12ms | 16.81ms | 0.9× | 1.7% | PASS |
| ac_reads | `oltp_order_range` | 3.77ms | 3.62ms | 1.0× | 2.0% | PASS |
| ac_reads | `oltp_distinct_range` | 4.82ms | 4.76ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 6.73ms | 8.70ms | 1.3× | 1.7% | PASS |
| ac_reads | `select_random_points` | 26.25ms | 25.53ms | 1.0× | 3.6% | PASS |
| ac_reads | `select_random_ranges` | 9.68ms | 8.80ms | 0.9× | 1.7% | PASS |
| ac_reads | `covering_index_scan` | 10.61ms | 12.40ms | 1.2× | 1.1% | PASS |
| ac_reads | `groupby_scan` | 34.52ms | 35.88ms | 1.0× | 0.9% | PASS |
| ac_reads | `index_join` | 12.94ms | 11.24ms | 0.9× | 2.8% | PASS |
| ac_reads | `index_join_scan` | 4.25ms | 6.37ms | 1.5× | 2.3% | PASS |
| ac_reads | `types_table_scan` | 1.10s | 1.37s | 1.2× | 1.2% | PASS |
| ac_reads | `table_scan` | 1.29s | 1.39s | 1.1× | 2.2% | PASS |
| ac_reads | `oltp_read_only` | 177.64ms | 172.64ms | 1.0× | 1.7% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.85ms | 76.94ms | 3.4× | 5.4% | PASS |
| ac_writes | `oltp_insert_ac` | 25.72ms | 88.50ms | 3.4× | 4.6% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.48ms | 104.39ms | 3.8× | 4.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.95ms | 84.16ms | 3.7× | 5.9% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.82ms | 99.13ms | 3.7× | 6.0% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.67ms | 95.12ms | 3.7× | 8.9% | PASS |
| ac_writes | `types_delete_insert_ac` | 24.12ms | 86.23ms | 3.6× | 6.3% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.27ms | 101.53ms | 3.1× | 5.0% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 34.18ms | 38.49ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_range_select` | 15.95ms | 14.17ms | 0.9× | 1.9% | PASS |
| mem_reads | `oltp_sum_range` | 14.77ms | 14.10ms | 1.0× | 1.3% | PASS |
| mem_reads | `oltp_order_range` | 3.14ms | 3.25ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 4.25ms | 4.38ms | 1.0× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 3.93ms | 6.35ms | 1.6× | 1.5% | PASS |
| mem_reads | `select_random_points` | 21.44ms | 21.60ms | 1.0× | 2.7% | PASS |
| mem_reads | `select_random_ranges` | 6.49ms | 6.54ms | 1.0× | 1.8% | PASS |
| mem_reads | `covering_index_scan` | 7.80ms | 9.70ms | 1.2× | 1.1% | PASS |
| mem_reads | `groupby_scan` | 33.29ms | 34.55ms | 1.0× | 0.7% | PASS |
| mem_reads | `index_join` | 10.19ms | 9.25ms | 0.9× | 2.6% | PASS |
| mem_reads | `index_join_scan` | 3.71ms | 5.70ms | 1.5× | 2.0% | PASS |
| mem_reads | `types_table_scan` | 1.06s | 1.34s | 1.3× | 0.3% | PASS |
| mem_reads | `table_scan` | 1.23s | 1.38s | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_read_only` | 134.86ms | 140.75ms | 1.0× | 1.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 243.26ms | 363.80ms | 1.5× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 18.49ms | 38.45ms | 2.1× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 63.58ms | 135.55ms | 2.1× | 1.8% | PASS |
| mem_writes | `oltp_update_non_index` | 47.09ms | 79.88ms | 1.7× | 1.8% | PASS |
| mem_writes | `oltp_delete_insert` | 51.84ms | 102.73ms | 2.0× | 1.2% | PASS |
| mem_writes | `oltp_write_only` | 27.42ms | 60.64ms | 2.2× | 1.4% | PASS |
| mem_writes | `types_delete_insert` | 37.71ms | 53.24ms | 1.4× | 1.6% | PASS |
| mem_writes | `oltp_read_write` | 95.36ms | 146.58ms | 1.5× | 1.8% | PASS |
| file_reads | `oltp_point_select` | 104.57ms | 58.29ms | 0.6× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 23.45ms | 16.57ms | 0.7× | 2.2% | PASS |
| file_reads | `oltp_sum_range` | 22.32ms | 16.49ms | 0.7× | 1.6% | PASS |
| file_reads | `oltp_order_range` | 4.18ms | 3.61ms | 0.9× | 3.1% | PASS |
| file_reads | `oltp_distinct_range` | 5.25ms | 4.73ms | 0.9× | 1.2% | PASS |
| file_reads | `oltp_index_scan` | 11.08ms | 8.71ms | 0.8× | 1.3% | PASS |
| file_reads | `select_random_points` | 30.24ms | 24.88ms | 0.8× | 2.7% | PASS |
| file_reads | `select_random_ranges` | 13.86ms | 8.78ms | 0.6× | 1.4% | PASS |
| file_reads | `covering_index_scan` | 15.16ms | 12.07ms | 0.8× | 1.3% | PASS |
| file_reads | `groupby_scan` | 34.61ms | 35.54ms | 1.0× | 1.1% | PASS |
| file_reads | `index_join` | 14.34ms | 11.22ms | 0.8× | 2.4% | PASS |
| file_reads | `index_join_scan` | 4.66ms | 6.19ms | 1.3× | 3.2% | PASS |
| file_reads | `types_table_scan` | 1.06s | 1.36s | 1.3× | 0.5% | PASS |
| file_reads | `table_scan` | 1.22s | 1.38s | 1.1× | 1.5% | PASS |
| file_reads | `oltp_read_only` | 241.57ms | 170.50ms | 0.7× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 265.12ms | 379.56ms | 1.4× | 0.9% | PASS |
| file_writes | `oltp_insert` | 25.62ms | 45.23ms | 1.8× | 2.1% | PASS |
| file_writes | `oltp_update_index` | 95.34ms | 154.46ms | 1.6× | 1.5% | PASS |
| file_writes | `oltp_update_non_index` | 87.00ms | 96.45ms | 1.1× | 8.9% | PASS |
| file_writes | `oltp_delete_insert` | 85.62ms | 118.11ms | 1.4× | 1.4% | PASS |
| file_writes | `oltp_write_only` | 56.78ms | 74.77ms | 1.3× | 2.3% | PASS |
| file_writes | `types_delete_insert` | 62.96ms | 66.06ms | 1.0× | 2.1% | PASS |
| file_writes | `oltp_read_write` | 128.01ms | 160.62ms | 1.3× | 1.6% | PASS |
| ac_reads | `oltp_point_select` | 58.62ms | 58.48ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_range_select` | 19.01ms | 16.43ms | 0.9× | 2.1% | PASS |
| ac_reads | `oltp_sum_range` | 17.71ms | 16.46ms | 0.9× | 1.7% | PASS |
| ac_reads | `oltp_order_range` | 3.80ms | 3.58ms | 0.9× | 2.1% | PASS |
| ac_reads | `oltp_distinct_range` | 4.83ms | 4.73ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_index_scan` | 6.51ms | 8.66ms | 1.3× | 1.5% | PASS |
| ac_reads | `select_random_points` | 24.70ms | 24.90ms | 1.0× | 2.7% | PASS |
| ac_reads | `select_random_ranges` | 9.07ms | 8.79ms | 1.0× | 1.8% | PASS |
| ac_reads | `covering_index_scan` | 10.42ms | 12.07ms | 1.2× | 1.3% | PASS |
| ac_reads | `groupby_scan` | 33.98ms | 35.61ms | 1.0× | 0.9% | PASS |
| ac_reads | `index_join` | 12.04ms | 11.18ms | 0.9× | 2.0% | PASS |
| ac_reads | `index_join_scan` | 4.23ms | 6.16ms | 1.5× | 2.1% | PASS |
| ac_reads | `types_table_scan` | 1.06s | 1.36s | 1.3× | 0.6% | PASS |
| ac_reads | `table_scan` | 1.21s | 1.38s | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_read_only` | 172.13ms | 170.84ms | 1.0× | 1.1% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 24.81ms | 82.26ms | 3.3× | 6.1% | PASS |
| ac_writes | `oltp_insert_ac` | 27.18ms | 102.14ms | 3.8× | 8.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 28.54ms | 109.62ms | 3.8× | 5.9% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.69ms | 90.05ms | 3.6× | 8.2% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.90ms | 98.47ms | 3.7× | 5.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.73ms | 97.92ms | 3.7× | 5.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 24.97ms | 93.13ms | 3.7× | 7.2% | PASS |
| ac_writes | `oltp_read_write_ac` | 33.27ms | 103.33ms | 3.1× | 6.5% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 27.58ms | 30.51ms | 1.1× | 1.8% | PASS |
| mem_reads | `oltp_range_select` | 16.50ms | 16.21ms | 1.0× | 2.0% | PASS |
| mem_reads | `oltp_sum_range` | 14.10ms | 15.56ms | 1.1× | 1.8% | PASS |
| mem_reads | `oltp_order_range` | 2.95ms | 3.08ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 3.72ms | 3.92ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_index_scan` | 3.73ms | 4.66ms | 1.2× | 1.8% | PASS |
| mem_reads | `select_random_points` | 22.95ms | 24.39ms | 1.1× | 3.3% | PASS |
| mem_reads | `select_random_ranges` | 6.10ms | 6.63ms | 1.1× | 1.5% | PASS |
| mem_reads | `covering_index_scan` | 5.94ms | 7.28ms | 1.2× | 2.8% | PASS |
| mem_reads | `groupby_scan` | 29.77ms | 33.02ms | 1.1× | 0.8% | PASS |
| mem_reads | `index_join` | 6.34ms | 7.98ms | 1.3× | 2.5% | PASS |
| mem_reads | `index_join_scan` | 3.38ms | 4.71ms | 1.4× | 2.4% | PASS |
| mem_reads | `types_table_scan` | 924.76ms | 990.49ms | 1.1× | 2.9% | PASS |
| mem_reads | `table_scan` | 1.10s | 1.09s | 1.0× | 5.2% | PASS |
| mem_reads | `oltp_read_only` | 123.60ms | 130.00ms | 1.1× | 1.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 188.72ms | 250.65ms | 1.3× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 15.01ms | 26.10ms | 1.7× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 56.17ms | 96.59ms | 1.7× | 1.7% | PASS |
| mem_writes | `oltp_update_non_index` | 43.73ms | 60.12ms | 1.4× | 1.8% | PASS |
| mem_writes | `oltp_delete_insert` | 42.42ms | 75.30ms | 1.8× | 2.3% | PASS |
| mem_writes | `oltp_write_only` | 22.74ms | 44.31ms | 1.9× | 1.8% | PASS |
| mem_writes | `types_delete_insert` | 27.65ms | 40.08ms | 1.4× | 2.2% | PASS |
| mem_writes | `oltp_read_write` | 92.38ms | 129.10ms | 1.4× | 1.2% | PASS |
| file_reads | `oltp_point_select` | 95.31ms | 49.69ms | 0.5× | 0.7% | PASS |
| file_reads | `oltp_range_select` | 23.71ms | 18.23ms | 0.8× | 1.3% | PASS |
| file_reads | `oltp_sum_range` | 21.19ms | 17.60ms | 0.8× | 1.4% | PASS |
| file_reads | `oltp_order_range` | 3.73ms | 3.33ms | 0.9× | 0.9% | PASS |
| file_reads | `oltp_distinct_range` | 4.48ms | 4.18ms | 0.9× | 0.6% | PASS |
| file_reads | `oltp_index_scan` | 10.55ms | 6.70ms | 0.6× | 0.8% | PASS |
| file_reads | `select_random_points` | 29.10ms | 26.25ms | 0.9× | 1.4% | PASS |
| file_reads | `select_random_ranges` | 12.76ms | 8.56ms | 0.7× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 13.04ms | 9.09ms | 0.7× | 0.9% | PASS |
| file_reads | `groupby_scan` | 30.59ms | 33.38ms | 1.1× | 0.6% | PASS |
| file_reads | `index_join` | 10.29ms | 9.73ms | 0.9× | 2.0% | PASS |
| file_reads | `index_join_scan` | 4.03ms | 5.07ms | 1.3× | 2.4% | PASS |
| file_reads | `types_table_scan` | 894.16ms | 979.21ms | 1.1× | 2.7% | PASS |
| file_reads | `table_scan` | 1.08s | 1.09s | 1.0× | 3.5% | PASS |
| file_reads | `oltp_read_only` | 218.63ms | 157.30ms | 0.7× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 273.28ms | 312.53ms | 1.1× | 18.1% | PASS |
| file_writes | `oltp_insert` | 29.51ms | 48.91ms | 1.7× | 41.1% | PASS |
| file_writes | `oltp_update_index` | 178.91ms | 171.04ms | 1.0× | 13.6% | PASS |
| file_writes | `oltp_update_non_index` | 135.68ms | 111.34ms | 0.8× | 14.2% | PASS |
| file_writes | `oltp_delete_insert` | 137.70ms | 133.82ms | 1.0× | 13.9% | PASS |
| file_writes | `oltp_write_only` | 96.44ms | 92.26ms | 1.0× | 7.7% | PASS |
| file_writes | `types_delete_insert` | 81.83ms | 80.62ms | 1.0× | 12.1% | PASS |
| file_writes | `oltp_read_write` | 168.54ms | 180.40ms | 1.1× | 19.9% | PASS |
| ac_reads | `oltp_point_select` | 49.79ms | 48.78ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_range_select` | 18.93ms | 18.15ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_sum_range` | 16.29ms | 17.49ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_order_range` | 3.33ms | 3.34ms | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_distinct_range` | 4.11ms | 4.21ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_index_scan` | 6.38ms | 6.87ms | 1.1× | 1.1% | PASS |
| ac_reads | `select_random_points` | 25.96ms | 27.41ms | 1.1× | 2.1% | PASS |
| ac_reads | `select_random_ranges` | 8.45ms | 8.61ms | 1.0× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 8.67ms | 9.14ms | 1.1× | 0.9% | PASS |
| ac_reads | `groupby_scan` | 30.02ms | 33.44ms | 1.1× | 0.5% | PASS |
| ac_reads | `index_join` | 7.99ms | 9.75ms | 1.2× | 1.7% | PASS |
| ac_reads | `index_join_scan` | 3.66ms | 5.23ms | 1.4× | 1.9% | PASS |
| ac_reads | `types_table_scan` | 1.00s | 1.01s | 1.0× | 1.9% | PASS |
| ac_reads | `table_scan` | 1.16s | 1.09s | 0.9× | 5.0% | PASS |
| ac_reads | `oltp_read_only` | 155.28ms | 156.22ms | 1.0× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 30.60ms | 84.42ms | 2.8× | 23.2% | PASS |
| ac_writes | `oltp_insert_ac` | 34.91ms | 110.02ms | 3.2× | 39.8% | PASS |
| ac_writes | `oltp_update_index_ac` | 44.82ms | 139.48ms | 3.1× | 39.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 49.31ms | 137.15ms | 2.8× | 60.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 38.34ms | 192.28ms | 5.0× | 69.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 46.81ms | 145.49ms | 3.1× | 52.7% | PASS |
| ac_writes | `types_delete_insert_ac` | 34.56ms | 106.75ms | 3.1× | 36.6% | PASS |
| ac_writes | `oltp_read_write_ac` | 41.00ms | 110.80ms | 2.7× | 26.3% | PASS |

</details>

</details>

## Version-control latency

Wall time: 3m 28s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 34.97ms | 130.00ms | 26.9% | 0.7% | PASS |
| `status_dirty_many_tables` | 38.49ms | 130.00ms | 29.6% | 0.4% | PASS |
| `diff_regular_working_one_table` | 30.27ms | 120.00ms | 25.2% | 0.4% | PASS |
| `diff_regular_working_many_tables` | 43.37ms | 140.00ms | 31.0% | 0.7% | PASS |
| `diff_stat_working_many_tables` | 43.19ms | 140.00ms | 30.8% | 0.4% | PASS |
| `diff_schema_working_many_tables` | 43.84ms | 140.00ms | 31.3% | 0.6% | PASS |
| `branch_list_many_branches` | 21.95ms | 35.00ms | 62.7% | 1.4% | PASS |
| `branch_create_delete` | 24.48ms | 40.00ms | 61.2% | 1.9% | PASS |
| `at_literal_deep_history` | 24.95ms | 100.00ms | 25.0% | 0.9% | PASS |
| `diff_literal_deep_history` | 25.09ms | 120.00ms | 20.9% | 1.0% | PASS |
| `history_literal_deep_history` | 26.48ms | 150.00ms | 17.6% | 1.6% | PASS |
| `checkout_branch_clean` | 37.61ms | 150.00ms | 25.1% | 2.0% | PASS |
| `merge_data_no_conflicts` | 29.24ms | 50.00ms | 58.5% | 2.8% | PASS |
| `merge_data_secondary_index` | 878.11ms | 2.50s | 35.1% | 0.7% | PASS |
| `merge_schema_no_conflicts` | 21.19ms | 35.00ms | 60.5% | 1.0% | PASS |
| `merge_data_conflicts` | 29.64ms | 180.00ms | 16.5% | 0.7% | PASS |
| `merge_data_conflicts_with_resolve` | 30.41ms | 180.00ms | 16.9% | 0.7% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
