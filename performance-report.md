# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-09-04 11:10 UTC
>
> Commit: [`f0be7ec23d0f66f8113ed8189cb19fb1da87d56a`](https://github.com/dolthub/doltlite/commit/f0be7ec23d0f66f8113ed8189cb19fb1da87d56a)
>
> Runner: ubuntu24 20260831.293.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/33859091064)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.18s | 10.81s | 1.1× | 1.8% | **PASS** |
| Writes | 2.17s | 3.50s | 1.6× | 1.3% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 11.22s | 11.23s | 1.0× | 1.6% | **PASS** |
| Writes | 3.47s | 4.17s | 1.2× | 1.9% | **PASS** |
| Autocommit writes | 895.45ms | 3.00s | 3.3× | 6.1% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.17s | 2.26s | 1.0× | 4.6% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.70s | 2.83s | 1.1× | 1.7% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.52s | 2.78s | 1.1× | 1.5% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.78s | 2.93s | 1.1× | 1.6% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 396.71ms | 583.45ms | 1.5× | 4.6% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 597.99ms | 1.00s | 1.7× | 1.5% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 586.26ms | 968.21ms | 1.7× | 1.3% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 593.98ms | 945.76ms | 1.6× | 1.0% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.54s | 2.48s | 1.0× | 3.1% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.91s | 2.90s | 1.0× | 1.5% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.75s | 2.86s | 1.0× | 1.2% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 3.02s | 2.99s | 1.0× | 1.4% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 941.35ms | 939.10ms | 1.0× | 6.7% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 937.87ms | 1.11s | 1.2× | 4.0% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 803.35ms | 1.06s | 1.3× | 1.5% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 786.15ms | 1.06s | 1.3× | 1.6% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.51s | 2.62s | 1.0× | 1.9% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.80s | 2.91s | 1.0× | 1.8% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.60s | 2.86s | 1.1× | 1.4% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.77s | 2.97s | 1.1× | 1.7% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 285.26ms | 839.34ms | 2.9× | 11.0% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 207.68ms | 736.76ms | 3.5× | 6.7% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 202.93ms | 711.53ms | 3.5× | 5.4% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 199.58ms | 708.59ms | 3.6× | 4.8% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 23.00ms | 26.50ms | 1.2× | 1.9% | PASS |
| mem_reads | `oltp_range_select` | 10.20ms | 10.40ms | 1.0× | 6.2% | PASS |
| mem_reads | `oltp_sum_range` | 9.11ms | 9.96ms | 1.1× | 5.0% | PASS |
| mem_reads | `oltp_order_range` | 2.45ms | 2.46ms | 1.0× | 5.6% | PASS |
| mem_reads | `oltp_distinct_range` | 3.29ms | 3.46ms | 1.0× | 4.3% | PASS |
| mem_reads | `oltp_index_scan` | 3.72ms | 4.52ms | 1.2× | 4.6% | PASS |
| mem_reads | `select_random_points` | 10.02ms | 10.37ms | 1.0× | 4.5% | PASS |
| mem_reads | `select_random_ranges` | 2.78ms | 3.49ms | 1.3× | 4.0% | PASS |
| mem_reads | `covering_index_scan` | 3.64ms | 3.57ms | 1.0× | 3.9% | PASS |
| mem_reads | `groupby_scan` | 27.77ms | 29.17ms | 1.1× | 5.8% | PASS |
| mem_reads | `index_join` | 4.99ms | 6.50ms | 1.3× | 6.6% | PASS |
| mem_reads | `index_join_scan` | 3.00ms | 4.26ms | 1.4× | 7.9% | PASS |
| mem_reads | `types_table_scan` | 933.82ms | 982.33ms | 1.1× | 0.9% | PASS |
| mem_reads | `table_scan` | 1.04s | 1.06s | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_read_only` | 97.00ms | 105.42ms | 1.1× | 5.5% | PASS |
| mem_writes | `oltp_bulk_insert` | 165.53ms | 223.40ms | 1.3× | 3.3% | PASS |
| mem_writes | `oltp_insert` | 14.56ms | 23.85ms | 1.6× | 2.8% | PASS |
| mem_writes | `oltp_update_index` | 46.83ms | 78.74ms | 1.7× | 3.2% | PASS |
| mem_writes | `oltp_update_non_index` | 33.12ms | 45.41ms | 1.4× | 3.9% | PASS |
| mem_writes | `oltp_delete_insert` | 38.39ms | 56.51ms | 1.5× | 6.6% | PASS |
| mem_writes | `oltp_write_only` | 19.06ms | 36.23ms | 1.9× | 5.3% | PASS |
| mem_writes | `types_delete_insert` | 21.20ms | 28.58ms | 1.3× | 9.3% | PASS |
| mem_writes | `oltp_read_write` | 58.01ms | 90.73ms | 1.6× | 7.3% | PASS |
| file_reads | `oltp_point_select` | 97.93ms | 45.64ms | 0.5× | 4.8% | PASS |
| file_reads | `oltp_range_select` | 19.37ms | 13.03ms | 0.7× | 10.4% | PASS |
| file_reads | `oltp_sum_range` | 19.50ms | 13.85ms | 0.7× | 5.5% | PASS |
| file_reads | `oltp_order_range` | 3.47ms | 2.92ms | 0.8× | 6.6% | PASS |
| file_reads | `oltp_distinct_range` | 4.10ms | 3.73ms | 0.9× | 6.5% | PASS |
| file_reads | `oltp_index_scan` | 10.38ms | 6.24ms | 0.6× | 7.7% | PASS |
| file_reads | `select_random_points` | 16.09ms | 11.22ms | 0.7× | 3.1% | PASS |
| file_reads | `select_random_ranges` | 10.68ms | 5.68ms | 0.5× | 2.8% | PASS |
| file_reads | `covering_index_scan` | 12.06ms | 6.17ms | 0.5× | 1.8% | PASS |
| file_reads | `groupby_scan` | 31.93ms | 32.84ms | 1.0× | 1.3% | PASS |
| file_reads | `index_join` | 10.22ms | 9.15ms | 0.9× | 2.1% | PASS |
| file_reads | `index_join_scan` | 3.96ms | 4.66ms | 1.2× | 2.8% | PASS |
| file_reads | `types_table_scan` | 940.39ms | 995.09ms | 1.1× | 1.2% | PASS |
| file_reads | `table_scan` | 1.15s | 1.19s | 1.0× | 1.0% | PASS |
| file_reads | `oltp_read_only` | 208.19ms | 133.83ms | 0.6× | 3.6% | PASS |
| file_writes | `oltp_bulk_insert` | 210.92ms | 274.72ms | 1.3× | 6.5% | PASS |
| file_writes | `oltp_insert` | 26.92ms | 38.13ms | 1.4× | 12.9% | PASS |
| file_writes | `oltp_update_index` | 150.19ms | 136.54ms | 0.9× | 14.9% | PASS |
| file_writes | `oltp_update_non_index` | 113.98ms | 98.36ms | 0.9× | 4.5% | PASS |
| file_writes | `oltp_delete_insert` | 121.44ms | 111.47ms | 0.9× | 2.5% | PASS |
| file_writes | `oltp_write_only` | 94.12ms | 78.98ms | 0.8× | 4.5% | PASS |
| file_writes | `types_delete_insert` | 70.74ms | 57.92ms | 0.8× | 7.0% | PASS |
| file_writes | `oltp_read_write` | 153.05ms | 142.98ms | 0.9× | 16.0% | PASS |
| ac_reads | `oltp_point_select` | 51.71ms | 49.56ms | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_range_select` | 14.70ms | 13.07ms | 0.9× | 3.6% | PASS |
| ac_reads | `oltp_sum_range` | 12.83ms | 12.70ms | 1.0× | 3.1% | PASS |
| ac_reads | `oltp_order_range` | 3.04ms | 2.96ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_distinct_range` | 4.02ms | 4.09ms | 1.0× | 2.8% | PASS |
| ac_reads | `oltp_index_scan` | 6.79ms | 7.08ms | 1.0× | 2.1% | PASS |
| ac_reads | `select_random_points` | 13.39ms | 13.27ms | 1.0× | 2.0% | PASS |
| ac_reads | `select_random_ranges` | 5.74ms | 5.88ms | 1.0× | 1.9% | PASS |
| ac_reads | `covering_index_scan` | 7.11ms | 6.32ms | 0.9× | 1.7% | PASS |
| ac_reads | `groupby_scan` | 31.03ms | 32.69ms | 1.1× | 1.2% | PASS |
| ac_reads | `index_join` | 7.31ms | 9.00ms | 1.2× | 1.9% | PASS |
| ac_reads | `index_join_scan` | 3.67ms | 5.19ms | 1.4× | 4.0% | PASS |
| ac_reads | `types_table_scan` | 1.02s | 1.09s | 1.1× | 0.7% | PASS |
| ac_reads | `table_scan` | 1.19s | 1.23s | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_read_only` | 133.92ms | 135.10ms | 1.0× | 1.7% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 34.20ms | 86.29ms | 2.5× | 16.2% | PASS |
| ac_writes | `oltp_insert_ac` | 35.48ms | 105.46ms | 3.0× | 10.7% | PASS |
| ac_writes | `oltp_update_index_ac` | 39.71ms | 128.27ms | 3.2× | 34.9% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 32.55ms | 94.90ms | 2.9× | 9.2% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 34.44ms | 111.27ms | 3.2× | 15.5% | PASS |
| ac_writes | `oltp_write_only_ac` | 34.87ms | 102.26ms | 2.9× | 9.9% | PASS |
| ac_writes | `types_delete_insert_ac` | 32.04ms | 98.15ms | 3.1× | 11.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 41.97ms | 112.74ms | 2.7× | 8.3% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 34.59ms | 38.07ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_range_select` | 16.72ms | 15.14ms | 0.9× | 3.4% | PASS |
| mem_reads | `oltp_sum_range` | 16.08ms | 14.06ms | 0.9× | 2.2% | PASS |
| mem_reads | `oltp_order_range` | 3.35ms | 3.24ms | 1.0× | 0.9% | PASS |
| mem_reads | `oltp_distinct_range` | 4.51ms | 4.54ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 4.02ms | 6.19ms | 1.5× | 1.7% | PASS |
| mem_reads | `select_random_points` | 22.38ms | 21.30ms | 1.0× | 3.3% | PASS |
| mem_reads | `select_random_ranges` | 3.61ms | 5.36ms | 1.5× | 1.5% | PASS |
| mem_reads | `covering_index_scan` | 4.18ms | 4.77ms | 1.1× | 2.6% | PASS |
| mem_reads | `groupby_scan` | 35.78ms | 34.94ms | 1.0× | 0.9% | PASS |
| mem_reads | `index_join` | 10.46ms | 9.21ms | 0.9× | 3.2% | PASS |
| mem_reads | `index_join_scan` | 3.86ms | 5.59ms | 1.5× | 3.0% | PASS |
| mem_reads | `types_table_scan` | 1.09s | 1.18s | 1.1× | 0.9% | PASS |
| mem_reads | `table_scan` | 1.29s | 1.34s | 1.0× | 2.9% | PASS |
| mem_reads | `oltp_read_only` | 159.81ms | 150.28ms | 0.9× | 1.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 245.79ms | 374.73ms | 1.5× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 18.23ms | 39.16ms | 2.1× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 65.15ms | 137.53ms | 2.1× | 1.0% | PASS |
| mem_writes | `oltp_update_non_index` | 48.17ms | 80.82ms | 1.7× | 1.8% | PASS |
| mem_writes | `oltp_delete_insert` | 53.86ms | 104.89ms | 1.9× | 1.3% | PASS |
| mem_writes | `oltp_write_only` | 28.20ms | 60.99ms | 2.2× | 1.9% | PASS |
| mem_writes | `types_delete_insert` | 39.22ms | 54.78ms | 1.4× | 2.3% | PASS |
| mem_writes | `oltp_read_write` | 99.38ms | 147.22ms | 1.5× | 2.1% | PASS |
| file_reads | `oltp_point_select` | 107.77ms | 58.32ms | 0.5× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 24.46ms | 17.40ms | 0.7× | 3.1% | PASS |
| file_reads | `oltp_sum_range` | 23.52ms | 16.17ms | 0.7× | 1.7% | PASS |
| file_reads | `oltp_order_range` | 4.21ms | 3.55ms | 0.8× | 2.2% | PASS |
| file_reads | `oltp_distinct_range` | 5.40ms | 4.84ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 11.27ms | 8.58ms | 0.8× | 1.5% | PASS |
| file_reads | `select_random_points` | 32.00ms | 24.23ms | 0.8× | 3.4% | PASS |
| file_reads | `select_random_ranges` | 10.75ms | 7.40ms | 0.7× | 1.3% | PASS |
| file_reads | `covering_index_scan` | 11.26ms | 6.91ms | 0.6× | 0.9% | PASS |
| file_reads | `groupby_scan` | 36.78ms | 35.49ms | 1.0× | 1.5% | PASS |
| file_reads | `index_join` | 14.86ms | 11.06ms | 0.7× | 3.2% | PASS |
| file_reads | `index_join_scan` | 4.77ms | 6.04ms | 1.3× | 2.8% | PASS |
| file_reads | `types_table_scan` | 1.10s | 1.20s | 1.1× | 1.1% | PASS |
| file_reads | `table_scan` | 1.28s | 1.33s | 1.0× | 1.3% | PASS |
| file_reads | `oltp_read_only` | 246.16ms | 170.96ms | 0.7× | 1.4% | PASS |
| file_writes | `oltp_bulk_insert` | 271.91ms | 395.25ms | 1.5× | 1.4% | PASS |
| file_writes | `oltp_insert` | 25.82ms | 46.46ms | 1.8× | 1.7% | PASS |
| file_writes | `oltp_update_index` | 137.52ms | 157.11ms | 1.1× | 5.7% | PASS |
| file_writes | `oltp_update_non_index` | 107.08ms | 97.06ms | 0.9× | 13.6% | PASS |
| file_writes | `oltp_delete_insert` | 95.49ms | 121.49ms | 1.3× | 2.3% | PASS |
| file_writes | `oltp_write_only` | 84.84ms | 73.37ms | 0.9× | 10.9% | PASS |
| file_writes | `types_delete_insert` | 70.87ms | 63.85ms | 0.9× | 1.5% | PASS |
| file_writes | `oltp_read_write` | 144.33ms | 159.14ms | 1.1× | 5.9% | PASS |
| ac_reads | `oltp_point_select` | 59.20ms | 58.06ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_range_select` | 19.43ms | 17.42ms | 0.9× | 1.7% | PASS |
| ac_reads | `oltp_sum_range` | 18.39ms | 16.14ms | 0.9× | 1.5% | PASS |
| ac_reads | `oltp_order_range` | 3.76ms | 3.54ms | 0.9× | 1.8% | PASS |
| ac_reads | `oltp_distinct_range` | 4.81ms | 4.83ms | 1.0× | 2.1% | PASS |
| ac_reads | `oltp_index_scan` | 6.59ms | 8.48ms | 1.3× | 2.3% | PASS |
| ac_reads | `select_random_points` | 25.90ms | 24.86ms | 1.0× | 1.9% | PASS |
| ac_reads | `select_random_ranges` | 6.08ms | 7.45ms | 1.2× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 6.70ms | 6.95ms | 1.0× | 1.8% | PASS |
| ac_reads | `groupby_scan` | 35.91ms | 35.60ms | 1.0× | 0.8% | PASS |
| ac_reads | `index_join` | 12.26ms | 11.14ms | 0.9× | 2.9% | PASS |
| ac_reads | `index_join_scan` | 4.29ms | 6.04ms | 1.4× | 3.4% | PASS |
| ac_reads | `types_table_scan` | 1.10s | 1.20s | 1.1× | 1.7% | PASS |
| ac_reads | `table_scan` | 1.32s | 1.34s | 1.0× | 4.2% | PASS |
| ac_reads | `oltp_read_only` | 176.40ms | 171.87ms | 1.0× | 1.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 23.79ms | 77.86ms | 3.3× | 7.0% | PASS |
| ac_writes | `oltp_insert_ac` | 27.82ms | 89.77ms | 3.2× | 7.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.67ms | 104.83ms | 3.9× | 5.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.11ms | 85.56ms | 3.7× | 6.4% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.48ms | 95.72ms | 3.8× | 6.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.09ms | 94.26ms | 3.8× | 7.4% | PASS |
| ac_writes | `types_delete_insert_ac` | 23.63ms | 87.05ms | 3.7× | 5.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.09ms | 101.71ms | 3.2× | 7.4% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 34.02ms | 37.88ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_range_select` | 15.44ms | 14.82ms | 1.0× | 1.5% | PASS |
| mem_reads | `oltp_sum_range` | 14.70ms | 13.37ms | 0.9× | 2.2% | PASS |
| mem_reads | `oltp_order_range` | 3.10ms | 3.16ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 4.26ms | 4.45ms | 1.0× | 1.4% | PASS |
| mem_reads | `oltp_index_scan` | 3.90ms | 5.95ms | 1.5× | 1.3% | PASS |
| mem_reads | `select_random_points` | 20.36ms | 20.55ms | 1.0× | 2.6% | PASS |
| mem_reads | `select_random_ranges` | 3.36ms | 5.26ms | 1.6× | 1.7% | PASS |
| mem_reads | `covering_index_scan` | 4.18ms | 4.48ms | 1.1× | 1.5% | PASS |
| mem_reads | `groupby_scan` | 35.06ms | 34.48ms | 1.0× | 0.7% | PASS |
| mem_reads | `index_join` | 9.91ms | 8.86ms | 0.9× | 1.8% | PASS |
| mem_reads | `index_join_scan` | 3.61ms | 5.23ms | 1.4× | 1.5% | PASS |
| mem_reads | `types_table_scan` | 1.06s | 1.17s | 1.1× | 0.4% | PASS |
| mem_reads | `table_scan` | 1.18s | 1.31s | 1.1× | 0.4% | PASS |
| mem_reads | `oltp_read_only` | 134.02ms | 139.37ms | 1.0× | 1.2% | PASS |
| mem_writes | `oltp_bulk_insert` | 246.77ms | 368.61ms | 1.5× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 18.67ms | 38.34ms | 2.1× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 62.97ms | 129.54ms | 2.1× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 47.10ms | 76.72ms | 1.6× | 1.4% | PASS |
| mem_writes | `oltp_delete_insert` | 51.45ms | 101.06ms | 2.0× | 1.7% | PASS |
| mem_writes | `oltp_write_only` | 27.27ms | 59.18ms | 2.2× | 1.4% | PASS |
| mem_writes | `types_delete_insert` | 37.38ms | 52.25ms | 1.4× | 1.3% | PASS |
| mem_writes | `oltp_read_write` | 94.65ms | 142.50ms | 1.5× | 1.7% | PASS |
| file_reads | `oltp_point_select` | 104.54ms | 57.11ms | 0.5× | 1.2% | PASS |
| file_reads | `oltp_range_select` | 22.60ms | 17.00ms | 0.8× | 2.2% | PASS |
| file_reads | `oltp_sum_range` | 22.50ms | 15.65ms | 0.7× | 2.1% | PASS |
| file_reads | `oltp_order_range` | 4.10ms | 3.47ms | 0.8× | 1.7% | PASS |
| file_reads | `oltp_distinct_range` | 5.28ms | 4.76ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 11.05ms | 8.29ms | 0.8× | 1.2% | PASS |
| file_reads | `select_random_points` | 29.40ms | 23.28ms | 0.8× | 2.7% | PASS |
| file_reads | `select_random_ranges` | 10.42ms | 7.25ms | 0.7× | 0.9% | PASS |
| file_reads | `covering_index_scan` | 11.23ms | 6.77ms | 0.6× | 0.9% | PASS |
| file_reads | `groupby_scan` | 36.08ms | 35.08ms | 1.0× | 0.7% | PASS |
| file_reads | `index_join` | 14.13ms | 10.92ms | 0.8× | 1.8% | PASS |
| file_reads | `index_join_scan` | 4.64ms | 5.69ms | 1.2× | 2.5% | PASS |
| file_reads | `types_table_scan` | 1.05s | 1.18s | 1.1× | 0.4% | PASS |
| file_reads | `table_scan` | 1.18s | 1.31s | 1.1× | 0.4% | PASS |
| file_reads | `oltp_read_only` | 241.40ms | 168.13ms | 0.7× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 266.27ms | 380.48ms | 1.4× | 0.7% | PASS |
| file_writes | `oltp_insert` | 25.64ms | 45.40ms | 1.8× | 2.1% | PASS |
| file_writes | `oltp_update_index` | 94.51ms | 147.62ms | 1.6× | 1.5% | PASS |
| file_writes | `oltp_update_non_index` | 88.07ms | 90.98ms | 1.0× | 11.2% | PASS |
| file_writes | `oltp_delete_insert` | 84.58ms | 114.22ms | 1.4× | 1.2% | PASS |
| file_writes | `oltp_write_only` | 55.32ms | 70.03ms | 1.3× | 1.4% | PASS |
| file_writes | `types_delete_insert` | 62.66ms | 61.82ms | 1.0× | 1.5% | PASS |
| file_writes | `oltp_read_write` | 126.30ms | 153.83ms | 1.2× | 1.5% | PASS |
| ac_reads | `oltp_point_select` | 58.05ms | 57.30ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_range_select` | 18.46ms | 17.19ms | 0.9× | 1.1% | PASS |
| ac_reads | `oltp_sum_range` | 17.87ms | 15.71ms | 0.9× | 1.7% | PASS |
| ac_reads | `oltp_order_range` | 3.71ms | 3.49ms | 0.9× | 2.6% | PASS |
| ac_reads | `oltp_distinct_range` | 4.81ms | 4.79ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 6.42ms | 8.37ms | 1.3× | 1.6% | PASS |
| ac_reads | `select_random_points` | 24.24ms | 23.74ms | 1.0× | 2.6% | PASS |
| ac_reads | `select_random_ranges` | 5.84ms | 7.30ms | 1.3× | 1.4% | PASS |
| ac_reads | `covering_index_scan` | 6.77ms | 6.78ms | 1.0× | 1.8% | PASS |
| ac_reads | `groupby_scan` | 35.74ms | 35.02ms | 1.0× | 0.9% | PASS |
| ac_reads | `index_join` | 11.86ms | 10.97ms | 0.9× | 1.4% | PASS |
| ac_reads | `index_join_scan` | 4.12ms | 5.69ms | 1.4× | 2.2% | PASS |
| ac_reads | `types_table_scan` | 1.05s | 1.18s | 1.1× | 0.6% | PASS |
| ac_reads | `table_scan` | 1.18s | 1.32s | 1.1× | 0.4% | PASS |
| ac_reads | `oltp_read_only` | 173.42ms | 168.52ms | 1.0× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 24.48ms | 73.70ms | 3.0× | 5.7% | PASS |
| ac_writes | `oltp_insert_ac` | 24.95ms | 90.62ms | 3.6× | 5.7% | PASS |
| ac_writes | `oltp_update_index_ac` | 25.85ms | 98.93ms | 3.8× | 4.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.21ms | 82.47ms | 3.6× | 5.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.36ms | 92.19ms | 3.6× | 4.7% | PASS |
| ac_writes | `oltp_write_only_ac` | 24.59ms | 90.52ms | 3.7× | 3.8% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.88ms | 83.77ms | 3.7× | 6.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.62ms | 99.33ms | 3.1× | 5.1% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 35.01ms | 41.96ms | 1.2× | 1.6% | PASS |
| mem_reads | `oltp_range_select` | 19.90ms | 22.81ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_sum_range` | 18.33ms | 20.69ms | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_order_range` | 3.57ms | 3.93ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_distinct_range` | 4.80ms | 5.27ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_index_scan` | 4.65ms | 6.35ms | 1.4× | 2.2% | PASS |
| mem_reads | `select_random_points` | 29.62ms | 33.48ms | 1.1× | 2.2% | PASS |
| mem_reads | `select_random_ranges` | 7.99ms | 9.04ms | 1.1× | 1.4% | PASS |
| mem_reads | `covering_index_scan` | 4.22ms | 4.61ms | 1.1× | 3.1% | PASS |
| mem_reads | `groupby_scan` | 37.60ms | 40.41ms | 1.1× | 0.6% | PASS |
| mem_reads | `index_join` | 8.18ms | 10.92ms | 1.3× | 3.1% | PASS |
| mem_reads | `index_join_scan` | 4.04ms | 5.41ms | 1.3× | 2.4% | PASS |
| mem_reads | `types_table_scan` | 1.13s | 1.20s | 1.1× | 1.9% | PASS |
| mem_reads | `table_scan` | 1.33s | 1.34s | 1.0× | 2.3% | PASS |
| mem_reads | `oltp_read_only` | 148.70ms | 174.55ms | 1.2× | 1.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 251.32ms | 353.21ms | 1.4× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 19.13ms | 35.09ms | 1.8× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 65.19ms | 117.65ms | 1.8× | 0.9% | PASS |
| mem_writes | `oltp_update_non_index` | 50.35ms | 76.65ms | 1.5× | 1.3% | PASS |
| mem_writes | `oltp_delete_insert` | 48.69ms | 94.55ms | 1.9× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 26.38ms | 55.83ms | 2.1× | 1.1% | PASS |
| mem_writes | `types_delete_insert` | 32.61ms | 53.02ms | 1.6× | 1.2% | PASS |
| mem_writes | `oltp_read_write` | 100.31ms | 159.76ms | 1.6× | 1.1% | PASS |
| file_reads | `oltp_point_select` | 103.30ms | 60.38ms | 0.6× | 1.3% | PASS |
| file_reads | `oltp_range_select` | 26.45ms | 24.60ms | 0.9× | 1.5% | PASS |
| file_reads | `oltp_sum_range` | 25.24ms | 22.51ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_order_range` | 4.39ms | 4.26ms | 1.0× | 2.5% | PASS |
| file_reads | `oltp_distinct_range` | 5.54ms | 5.57ms | 1.0× | 1.6% | PASS |
| file_reads | `oltp_index_scan` | 11.52ms | 8.12ms | 0.7× | 1.9% | PASS |
| file_reads | `select_random_points` | 36.62ms | 35.41ms | 1.0× | 1.2% | PASS |
| file_reads | `select_random_ranges` | 15.16ms | 11.09ms | 0.7× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 11.35ms | 6.59ms | 0.6× | 1.2% | PASS |
| file_reads | `groupby_scan` | 38.32ms | 40.52ms | 1.1× | 0.9% | PASS |
| file_reads | `index_join` | 11.97ms | 12.20ms | 1.0× | 1.4% | PASS |
| file_reads | `index_join_scan` | 4.89ms | 5.74ms | 1.2× | 1.8% | PASS |
| file_reads | `types_table_scan` | 1.09s | 1.20s | 1.1× | 2.2% | PASS |
| file_reads | `table_scan` | 1.38s | 1.35s | 1.0× | 1.9% | PASS |
| file_reads | `oltp_read_only` | 262.84ms | 207.33ms | 0.8× | 1.3% | PASS |
| file_writes | `oltp_bulk_insert` | 266.70ms | 368.30ms | 1.4× | 0.8% | PASS |
| file_writes | `oltp_insert` | 25.97ms | 41.01ms | 1.6× | 1.7% | PASS |
| file_writes | `oltp_update_index` | 95.25ms | 133.26ms | 1.4× | 1.7% | PASS |
| file_writes | `oltp_update_non_index` | 76.87ms | 91.97ms | 1.2× | 1.5% | PASS |
| file_writes | `oltp_delete_insert` | 79.17ms | 111.04ms | 1.4× | 1.5% | PASS |
| file_writes | `oltp_write_only` | 53.97ms | 69.95ms | 1.3× | 1.9% | PASS |
| file_writes | `types_delete_insert` | 52.52ms | 62.78ms | 1.2× | 1.9% | PASS |
| file_writes | `oltp_read_write` | 135.69ms | 177.59ms | 1.3× | 1.3% | PASS |
| ac_reads | `oltp_point_select` | 58.01ms | 60.55ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 22.49ms | 24.59ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_sum_range` | 20.45ms | 22.55ms | 1.1× | 1.6% | PASS |
| ac_reads | `oltp_order_range` | 4.02ms | 4.29ms | 1.1× | 2.9% | PASS |
| ac_reads | `oltp_distinct_range` | 5.16ms | 5.58ms | 1.1× | 1.9% | PASS |
| ac_reads | `oltp_index_scan` | 7.32ms | 8.27ms | 1.1× | 2.1% | PASS |
| ac_reads | `select_random_points` | 31.56ms | 35.65ms | 1.1× | 1.7% | PASS |
| ac_reads | `select_random_ranges` | 10.26ms | 11.11ms | 1.1× | 1.4% | PASS |
| ac_reads | `covering_index_scan` | 6.82ms | 6.57ms | 1.0× | 2.2% | PASS |
| ac_reads | `groupby_scan` | 37.35ms | 40.35ms | 1.1× | 0.9% | PASS |
| ac_reads | `index_join` | 9.58ms | 12.05ms | 1.3× | 1.7% | PASS |
| ac_reads | `index_join_scan` | 4.45ms | 5.79ms | 1.3× | 2.2% | PASS |
| ac_reads | `types_table_scan` | 1.07s | 1.19s | 1.1× | 1.2% | PASS |
| ac_reads | `table_scan` | 1.30s | 1.34s | 1.0× | 3.0% | PASS |
| ac_reads | `oltp_read_only` | 189.32ms | 204.96ms | 1.1× | 1.6% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 23.43ms | 74.64ms | 3.2× | 4.6% | PASS |
| ac_writes | `oltp_insert_ac` | 24.07ms | 88.84ms | 3.7× | 4.6% | PASS |
| ac_writes | `oltp_update_index_ac` | 25.59ms | 96.69ms | 3.8× | 4.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.46ms | 82.09ms | 3.7× | 6.2% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.14ms | 90.84ms | 3.8× | 5.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 24.54ms | 91.13ms | 3.7× | 4.8% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.86ms | 85.21ms | 3.7× | 7.8% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.49ms | 99.15ms | 3.1× | 4.7% | PASS |

</details>

</details>

## Version-control latency

Wall time: 2m 2s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 83.90ms | 130.00ms | 64.5% | 0.3% | PASS |
| `status_dirty_many_tables` | 87.02ms | 130.00ms | 66.9% | 0.4% | PASS |
| `diff_regular_working_one_table` | 78.67ms | 120.00ms | 65.6% | 0.4% | PASS |
| `diff_regular_working_many_tables` | 92.38ms | 140.00ms | 66.0% | 0.3% | PASS |
| `diff_stat_working_many_tables` | 92.50ms | 140.00ms | 66.1% | 0.3% | PASS |
| `diff_schema_working_many_tables` | 92.49ms | 140.00ms | 66.1% | 0.2% | PASS |
| `branch_list_many_branches` | 22.39ms | 35.00ms | 64.0% | 0.7% | PASS |
| `branch_create_delete` | 25.52ms | 40.00ms | 63.8% | 1.4% | PASS |
| `checkout_branch_clean` | 56.85ms | 150.00ms | 37.9% | 1.1% | PASS |
| `merge_data_no_conflicts` | 29.39ms | 50.00ms | 58.8% | 1.2% | PASS |
| `merge_schema_no_conflicts` | 22.43ms | 35.00ms | 64.1% | 1.3% | PASS |
| `merge_data_conflicts` | 32.78ms | 180.00ms | 18.2% | 0.8% | PASS |
| `merge_data_conflicts_with_resolve` | 33.09ms | 180.00ms | 18.4% | 1.2% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
