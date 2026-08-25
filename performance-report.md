# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-08-25 11:17 UTC
>
> Commit: [`b8f9f72c9b43d9d1e75e3bc51f0380d3f41f56ba`](https://github.com/dolthub/doltlite/commit/b8f9f72c9b43d9d1e75e3bc51f0380d3f41f56ba)
>
> Runner: ubuntu24 20260816.277.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/32833485217)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.73s | 10.78s | 1.1× | 1.4% | **PASS** |
| Writes | 2.12s | 3.36s | 1.6× | 1.2% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.72s | 11.03s | 1.0× | 1.1% | **PASS** |
| Writes | 3.73s | 4.08s | 1.1× | 1.8% | **PASS** |
| Autocommit writes | 1.49s | 4.72s | 3.2× | 6.5% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.04s | 2.09s | 1.0× | 1.4% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.61s | 2.87s | 1.1× | 1.8% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.52s | 2.86s | 1.1× | 1.4% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.56s | 2.96s | 1.2× | 1.0% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 350.97ms | 505.18ms | 1.4× | 1.4% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 591.08ms | 971.45ms | 1.6× | 1.5% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 586.75ms | 957.87ms | 1.6× | 1.1% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 590.14ms | 922.98ms | 1.6× | 0.9% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.24s | 2.15s | 1.0× | 1.0% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.88s | 2.94s | 1.0× | 1.4% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.81s | 2.93s | 1.0× | 1.2% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.79s | 3.02s | 1.1× | 1.1% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 1.20s | 946.68ms | 0.8× | 26.9% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 935.56ms | 1.09s | 1.2× | 3.9% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 814.04ms | 1.04s | 1.3× | 1.6% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 782.66ms | 1.01s | 1.3× | 1.5% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.10s | 2.14s | 1.0× | 0.8% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.80s | 2.98s | 1.1× | 1.7% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.65s | 2.93s | 1.1× | 1.6% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.64s | 3.02s | 1.1× | 1.1% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 810.92ms | 2.35s | 2.9× | 57.8% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 229.56ms | 791.22ms | 3.4× | 6.9% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 210.93ms | 735.65ms | 3.5× | 5.9% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 240.95ms | 842.87ms | 3.5× | 6.3% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 19.84ms | 21.30ms | 1.1× | 1.7% | PASS |
| mem_reads | `oltp_range_select` | 8.90ms | 8.50ms | 1.0× | 2.7% | PASS |
| mem_reads | `oltp_sum_range` | 7.95ms | 8.20ms | 1.0× | 1.7% | PASS |
| mem_reads | `oltp_order_range` | 2.16ms | 2.20ms | 1.0× | 1.0% | PASS |
| mem_reads | `oltp_distinct_range` | 3.02ms | 2.96ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 3.21ms | 3.84ms | 1.2× | 1.4% | PASS |
| mem_reads | `select_random_points` | 9.15ms | 9.01ms | 1.0× | 1.7% | PASS |
| mem_reads | `select_random_ranges` | 2.61ms | 3.15ms | 1.2× | 1.2% | PASS |
| mem_reads | `covering_index_scan` | 3.35ms | 3.30ms | 1.0× | 1.9% | PASS |
| mem_reads | `groupby_scan` | 25.73ms | 26.16ms | 1.0× | 0.8% | PASS |
| mem_reads | `index_join` | 4.74ms | 6.08ms | 1.3× | 1.6% | PASS |
| mem_reads | `index_join_scan` | 2.83ms | 3.77ms | 1.3× | 1.2% | PASS |
| mem_reads | `types_table_scan` | 864.55ms | 905.21ms | 1.0× | 0.8% | PASS |
| mem_reads | `table_scan` | 995.35ms | 999.33ms | 1.0× | 1.5% | PASS |
| mem_reads | `oltp_read_only` | 83.10ms | 85.46ms | 1.0× | 1.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 140.24ms | 183.89ms | 1.3× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 12.30ms | 20.14ms | 1.6× | 1.3% | PASS |
| mem_writes | `oltp_update_index` | 41.86ms | 67.73ms | 1.6× | 1.9% | PASS |
| mem_writes | `oltp_update_non_index` | 28.53ms | 38.33ms | 1.3× | 1.8% | PASS |
| mem_writes | `oltp_delete_insert` | 37.16ms | 54.38ms | 1.5× | 1.2% | PASS |
| mem_writes | `oltp_write_only` | 18.42ms | 34.65ms | 1.9× | 1.2% | PASS |
| mem_writes | `types_delete_insert` | 20.30ms | 26.06ms | 1.3× | 1.4% | PASS |
| mem_writes | `oltp_read_write` | 52.17ms | 80.00ms | 1.5× | 1.5% | PASS |
| file_reads | `oltp_point_select` | 84.69ms | 37.44ms | 0.4× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 15.84ms | 10.29ms | 0.7× | 1.3% | PASS |
| file_reads | `oltp_sum_range` | 14.92ms | 10.04ms | 0.7× | 1.1% | PASS |
| file_reads | `oltp_order_range` | 2.96ms | 2.43ms | 0.8× | 0.6% | PASS |
| file_reads | `oltp_distinct_range` | 3.77ms | 3.19ms | 0.8× | 1.0% | PASS |
| file_reads | `oltp_index_scan` | 10.15ms | 5.92ms | 0.6× | 1.0% | PASS |
| file_reads | `select_random_points` | 16.17ms | 10.76ms | 0.7× | 1.7% | PASS |
| file_reads | `select_random_ranges` | 9.22ms | 4.82ms | 0.5× | 0.6% | PASS |
| file_reads | `covering_index_scan` | 10.38ms | 5.27ms | 0.5× | 0.7% | PASS |
| file_reads | `groupby_scan` | 26.63ms | 26.36ms | 1.0× | 0.6% | PASS |
| file_reads | `index_join` | 8.53ms | 7.63ms | 0.9× | 1.1% | PASS |
| file_reads | `index_join_scan` | 3.60ms | 4.05ms | 1.1× | 1.0% | PASS |
| file_reads | `types_table_scan` | 872.98ms | 910.47ms | 1.0× | 1.1% | PASS |
| file_reads | `table_scan` | 982.48ms | 998.58ms | 1.0× | 0.5% | PASS |
| file_reads | `oltp_read_only` | 177.02ms | 108.86ms | 0.6× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 246.40ms | 250.30ms | 1.0× | 19.3% | PASS |
| file_writes | `oltp_insert` | 41.01ms | 54.73ms | 1.3× | 58.9% | PASS |
| file_writes | `oltp_update_index` | 162.96ms | 119.94ms | 0.7× | 19.6% | PASS |
| file_writes | `oltp_update_non_index` | 148.18ms | 98.47ms | 0.7× | 26.5% | PASS |
| file_writes | `oltp_delete_insert` | 208.15ms | 151.66ms | 0.7× | 34.6% | PASS |
| file_writes | `oltp_write_only` | 131.71ms | 80.83ms | 0.6× | 35.1% | PASS |
| file_writes | `types_delete_insert` | 76.15ms | 60.52ms | 0.8× | 26.1% | PASS |
| file_writes | `oltp_read_write` | 183.06ms | 130.24ms | 0.7× | 27.3% | PASS |
| ac_reads | `oltp_point_select` | 40.90ms | 37.32ms | 0.9× | 0.5% | PASS |
| ac_reads | `oltp_range_select` | 11.58ms | 10.25ms | 0.9× | 1.2% | PASS |
| ac_reads | `oltp_sum_range` | 10.65ms | 9.99ms | 0.9× | 0.6% | PASS |
| ac_reads | `oltp_order_range` | 2.55ms | 2.41ms | 0.9× | 0.8% | PASS |
| ac_reads | `oltp_distinct_range` | 3.33ms | 3.18ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_index_scan` | 5.79ms | 5.83ms | 1.0× | 1.0% | PASS |
| ac_reads | `select_random_points` | 11.48ms | 10.60ms | 0.9× | 0.6% | PASS |
| ac_reads | `select_random_ranges` | 4.92ms | 4.82ms | 1.0× | 0.7% | PASS |
| ac_reads | `covering_index_scan` | 6.01ms | 5.26ms | 0.9× | 1.0% | PASS |
| ac_reads | `groupby_scan` | 25.91ms | 26.24ms | 1.0× | 0.9% | PASS |
| ac_reads | `index_join` | 6.30ms | 7.48ms | 1.2× | 0.7% | PASS |
| ac_reads | `index_join_scan` | 3.21ms | 3.99ms | 1.2× | 0.8% | PASS |
| ac_reads | `types_table_scan` | 859.31ms | 902.58ms | 1.1× | 0.8% | PASS |
| ac_reads | `table_scan` | 994.48ms | 1.00s | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_read_only` | 114.91ms | 109.05ms | 0.9× | 1.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 95.28ms | 240.90ms | 2.5× | 64.8% | PASS |
| ac_writes | `oltp_insert_ac` | 88.38ms | 254.06ms | 2.9× | 53.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 95.93ms | 325.16ms | 3.4× | 66.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 88.10ms | 235.04ms | 2.7× | 48.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 69.18ms | 274.39ms | 4.0× | 45.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 131.40ms | 366.94ms | 2.8× | 56.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 130.33ms | 380.41ms | 2.9× | 71.1% | PASS |
| ac_writes | `oltp_read_write_ac` | 112.33ms | 275.04ms | 2.4× | 59.4% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 34.58ms | 36.36ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_range_select` | 16.16ms | 13.71ms | 0.8× | 1.9% | PASS |
| mem_reads | `oltp_sum_range` | 14.99ms | 13.29ms | 0.9× | 2.1% | PASS |
| mem_reads | `oltp_order_range` | 3.23ms | 3.08ms | 1.0× | 1.6% | PASS |
| mem_reads | `oltp_distinct_range` | 4.36ms | 4.15ms | 1.0× | 0.7% | PASS |
| mem_reads | `oltp_index_scan` | 4.00ms | 5.86ms | 1.5× | 1.7% | PASS |
| mem_reads | `select_random_points` | 21.61ms | 20.53ms | 1.0× | 2.9% | PASS |
| mem_reads | `select_random_ranges` | 3.55ms | 5.12ms | 1.4× | 1.8% | PASS |
| mem_reads | `covering_index_scan` | 4.30ms | 4.44ms | 1.0× | 1.9% | PASS |
| mem_reads | `groupby_scan` | 35.14ms | 33.57ms | 1.0× | 0.8% | PASS |
| mem_reads | `index_join` | 10.48ms | 9.11ms | 0.9× | 2.5% | PASS |
| mem_reads | `index_join_scan` | 3.87ms | 5.33ms | 1.4× | 1.9% | PASS |
| mem_reads | `types_table_scan` | 1.08s | 1.21s | 1.1× | 1.5% | PASS |
| mem_reads | `table_scan` | 1.23s | 1.37s | 1.1× | 2.0% | PASS |
| mem_reads | `oltp_read_only` | 137.53ms | 133.00ms | 1.0× | 1.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 240.25ms | 362.53ms | 1.5× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 18.08ms | 38.22ms | 2.1× | 0.6% | PASS |
| mem_writes | `oltp_update_index` | 64.99ms | 132.94ms | 2.0× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 47.58ms | 78.05ms | 1.6× | 1.7% | PASS |
| mem_writes | `oltp_delete_insert` | 54.12ms | 103.26ms | 1.9× | 1.5% | PASS |
| mem_writes | `oltp_write_only` | 28.16ms | 60.54ms | 2.2× | 1.4% | PASS |
| mem_writes | `types_delete_insert` | 39.03ms | 53.15ms | 1.4× | 2.4% | PASS |
| mem_writes | `oltp_read_write` | 98.88ms | 142.76ms | 1.4× | 2.0% | PASS |
| file_reads | `oltp_point_select` | 105.09ms | 55.86ms | 0.5× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 23.79ms | 15.96ms | 0.7× | 2.2% | PASS |
| file_reads | `oltp_sum_range` | 22.99ms | 15.65ms | 0.7× | 1.3% | PASS |
| file_reads | `oltp_order_range` | 4.15ms | 3.43ms | 0.8× | 2.5% | PASS |
| file_reads | `oltp_distinct_range` | 5.33ms | 4.54ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 11.37ms | 8.15ms | 0.7× | 1.4% | PASS |
| file_reads | `select_random_points` | 30.55ms | 23.49ms | 0.8× | 2.3% | PASS |
| file_reads | `select_random_ranges` | 10.62ms | 7.13ms | 0.7× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 11.52ms | 6.66ms | 0.6× | 1.2% | PASS |
| file_reads | `groupby_scan` | 36.20ms | 34.19ms | 0.9× | 1.1% | PASS |
| file_reads | `index_join` | 14.82ms | 10.73ms | 0.7× | 2.1% | PASS |
| file_reads | `index_join_scan` | 4.87ms | 5.80ms | 1.2× | 2.3% | PASS |
| file_reads | `types_table_scan` | 1.07s | 1.21s | 1.1× | 0.9% | PASS |
| file_reads | `table_scan` | 1.28s | 1.38s | 1.1× | 2.9% | PASS |
| file_reads | `oltp_read_only` | 247.81ms | 163.15ms | 0.7× | 1.4% | PASS |
| file_writes | `oltp_bulk_insert` | 264.67ms | 379.27ms | 1.4× | 1.0% | PASS |
| file_writes | `oltp_insert` | 26.39ms | 45.62ms | 1.7× | 2.6% | PASS |
| file_writes | `oltp_update_index` | 134.68ms | 153.63ms | 1.1× | 7.4% | PASS |
| file_writes | `oltp_update_non_index` | 103.96ms | 93.38ms | 0.9× | 10.3% | PASS |
| file_writes | `oltp_delete_insert` | 96.66ms | 119.76ms | 1.2× | 1.6% | PASS |
| file_writes | `oltp_write_only` | 91.70ms | 73.20ms | 0.8× | 8.8% | PASS |
| file_writes | `types_delete_insert` | 71.64ms | 65.97ms | 0.9× | 1.6% | PASS |
| file_writes | `oltp_read_write` | 145.88ms | 156.41ms | 1.1× | 5.2% | PASS |
| ac_reads | `oltp_point_select` | 59.34ms | 55.83ms | 0.9× | 1.4% | PASS |
| ac_reads | `oltp_range_select` | 19.61ms | 15.96ms | 0.8× | 1.7% | PASS |
| ac_reads | `oltp_sum_range` | 18.14ms | 15.50ms | 0.9× | 1.7% | PASS |
| ac_reads | `oltp_order_range` | 3.79ms | 3.45ms | 0.9× | 2.3% | PASS |
| ac_reads | `oltp_distinct_range` | 4.87ms | 4.55ms | 0.9× | 2.2% | PASS |
| ac_reads | `oltp_index_scan` | 6.75ms | 8.18ms | 1.2× | 0.8% | PASS |
| ac_reads | `select_random_points` | 25.92ms | 23.49ms | 0.9× | 2.4% | PASS |
| ac_reads | `select_random_ranges` | 6.08ms | 7.15ms | 1.2× | 1.2% | PASS |
| ac_reads | `covering_index_scan` | 7.07ms | 6.67ms | 0.9× | 1.2% | PASS |
| ac_reads | `groupby_scan` | 36.03ms | 34.13ms | 0.9× | 0.7% | PASS |
| ac_reads | `index_join` | 12.98ms | 10.88ms | 0.8× | 1.9% | PASS |
| ac_reads | `index_join_scan` | 4.45ms | 5.84ms | 1.3× | 1.6% | PASS |
| ac_reads | `types_table_scan` | 1.13s | 1.23s | 1.1× | 2.2% | PASS |
| ac_reads | `table_scan` | 1.28s | 1.39s | 1.1× | 2.4% | PASS |
| ac_reads | `oltp_read_only` | 179.19ms | 162.99ms | 0.9× | 2.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 25.78ms | 82.75ms | 3.2× | 6.0% | PASS |
| ac_writes | `oltp_insert_ac` | 28.90ms | 92.82ms | 3.2× | 8.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 28.88ms | 110.48ms | 3.8× | 6.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.84ms | 91.08ms | 3.7× | 5.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 29.23ms | 104.33ms | 3.6× | 7.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 29.29ms | 103.12ms | 3.5× | 6.8% | PASS |
| ac_writes | `types_delete_insert_ac` | 27.05ms | 97.13ms | 3.6× | 7.3% | PASS |
| ac_writes | `oltp_read_write_ac` | 35.60ms | 109.52ms | 3.1× | 7.3% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.16ms | 35.83ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_range_select` | 15.21ms | 13.64ms | 0.9× | 1.4% | PASS |
| mem_reads | `oltp_sum_range` | 14.51ms | 13.03ms | 0.9× | 1.3% | PASS |
| mem_reads | `oltp_order_range` | 3.10ms | 3.06ms | 1.0× | 1.9% | PASS |
| mem_reads | `oltp_distinct_range` | 4.31ms | 4.14ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 4.00ms | 6.02ms | 1.5× | 1.4% | PASS |
| mem_reads | `select_random_points` | 20.38ms | 20.08ms | 1.0× | 1.9% | PASS |
| mem_reads | `select_random_ranges` | 3.33ms | 5.10ms | 1.5× | 1.9% | PASS |
| mem_reads | `covering_index_scan` | 4.20ms | 4.38ms | 1.0× | 2.0% | PASS |
| mem_reads | `groupby_scan` | 34.98ms | 33.67ms | 1.0× | 0.8% | PASS |
| mem_reads | `index_join` | 10.26ms | 8.97ms | 0.9× | 1.9% | PASS |
| mem_reads | `index_join_scan` | 3.80ms | 5.29ms | 1.4× | 1.5% | PASS |
| mem_reads | `types_table_scan` | 1.05s | 1.21s | 1.2× | 1.2% | PASS |
| mem_reads | `table_scan` | 1.18s | 1.36s | 1.2× | 1.4% | PASS |
| mem_reads | `oltp_read_only` | 137.82ms | 133.44ms | 1.0× | 1.7% | PASS |
| mem_writes | `oltp_bulk_insert` | 236.69ms | 354.02ms | 1.5× | 1.1% | PASS |
| mem_writes | `oltp_insert` | 18.46ms | 38.17ms | 2.1× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 64.71ms | 130.11ms | 2.0× | 1.4% | PASS |
| mem_writes | `oltp_update_non_index` | 46.08ms | 75.51ms | 1.6× | 1.5% | PASS |
| mem_writes | `oltp_delete_insert` | 52.56ms | 102.46ms | 1.9× | 1.2% | PASS |
| mem_writes | `oltp_write_only` | 28.13ms | 60.35ms | 2.1× | 0.8% | PASS |
| mem_writes | `types_delete_insert` | 38.54ms | 53.04ms | 1.4× | 1.0% | PASS |
| mem_writes | `oltp_read_write` | 101.58ms | 144.21ms | 1.4× | 2.8% | PASS |
| file_reads | `oltp_point_select` | 105.97ms | 55.28ms | 0.5× | 1.6% | PASS |
| file_reads | `oltp_range_select` | 23.48ms | 15.82ms | 0.7× | 1.9% | PASS |
| file_reads | `oltp_sum_range` | 22.47ms | 15.17ms | 0.7× | 1.1% | PASS |
| file_reads | `oltp_order_range` | 4.21ms | 3.38ms | 0.8× | 1.6% | PASS |
| file_reads | `oltp_distinct_range` | 5.37ms | 4.45ms | 0.8× | 1.2% | PASS |
| file_reads | `oltp_index_scan` | 11.25ms | 7.96ms | 0.7× | 1.0% | PASS |
| file_reads | `select_random_points` | 29.98ms | 23.00ms | 0.8× | 1.2% | PASS |
| file_reads | `select_random_ranges` | 10.50ms | 7.04ms | 0.7× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 11.57ms | 6.64ms | 0.6× | 1.2% | PASS |
| file_reads | `groupby_scan` | 36.54ms | 34.22ms | 0.9× | 1.0% | PASS |
| file_reads | `index_join` | 15.31ms | 10.98ms | 0.7× | 1.9% | PASS |
| file_reads | `index_join_scan` | 4.90ms | 5.73ms | 1.2× | 1.2% | PASS |
| file_reads | `types_table_scan` | 1.06s | 1.21s | 1.1× | 1.6% | PASS |
| file_reads | `table_scan` | 1.23s | 1.37s | 1.1× | 2.2% | PASS |
| file_reads | `oltp_read_only` | 244.34ms | 161.26ms | 0.7× | 1.1% | PASS |
| file_writes | `oltp_bulk_insert` | 258.54ms | 364.87ms | 1.4× | 0.9% | PASS |
| file_writes | `oltp_insert` | 25.71ms | 45.06ms | 1.8× | 1.5% | PASS |
| file_writes | `oltp_update_index` | 97.03ms | 146.24ms | 1.5× | 1.8% | PASS |
| file_writes | `oltp_update_non_index` | 95.71ms | 88.40ms | 0.9× | 9.0% | PASS |
| file_writes | `oltp_delete_insert` | 85.62ms | 113.67ms | 1.3× | 1.4% | PASS |
| file_writes | `oltp_write_only` | 57.05ms | 69.06ms | 1.2× | 1.8% | PASS |
| file_writes | `types_delete_insert` | 62.82ms | 60.18ms | 1.0× | 1.6% | PASS |
| file_writes | `oltp_read_write` | 131.58ms | 152.87ms | 1.2× | 1.7% | PASS |
| ac_reads | `oltp_point_select` | 58.52ms | 54.93ms | 0.9× | 1.4% | PASS |
| ac_reads | `oltp_range_select` | 18.84ms | 15.76ms | 0.8× | 1.8% | PASS |
| ac_reads | `oltp_sum_range` | 17.75ms | 15.23ms | 0.9× | 2.2% | PASS |
| ac_reads | `oltp_order_range` | 3.77ms | 3.36ms | 0.9× | 1.9% | PASS |
| ac_reads | `oltp_distinct_range` | 4.87ms | 4.44ms | 0.9× | 1.4% | PASS |
| ac_reads | `oltp_index_scan` | 6.66ms | 8.04ms | 1.2× | 1.3% | PASS |
| ac_reads | `select_random_points` | 24.76ms | 23.09ms | 0.9× | 2.3% | PASS |
| ac_reads | `select_random_ranges` | 5.95ms | 7.09ms | 1.2× | 1.1% | PASS |
| ac_reads | `covering_index_scan` | 6.88ms | 6.60ms | 1.0× | 1.6% | PASS |
| ac_reads | `groupby_scan` | 35.73ms | 34.08ms | 1.0× | 0.5% | PASS |
| ac_reads | `index_join` | 12.21ms | 10.66ms | 0.9× | 2.3% | PASS |
| ac_reads | `index_join_scan` | 4.34ms | 5.65ms | 1.3× | 1.7% | PASS |
| ac_reads | `types_table_scan` | 1.08s | 1.22s | 1.1× | 1.5% | PASS |
| ac_reads | `table_scan` | 1.20s | 1.36s | 1.1× | 1.5% | PASS |
| ac_reads | `oltp_read_only` | 173.40ms | 160.67ms | 0.9× | 1.7% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 24.26ms | 77.92ms | 3.2× | 6.3% | PASS |
| ac_writes | `oltp_insert_ac` | 26.58ms | 93.25ms | 3.5× | 5.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.81ms | 101.78ms | 3.7× | 6.3% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.44ms | 85.28ms | 3.5× | 5.7% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.09ms | 96.08ms | 3.7× | 3.7% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.95ms | 94.31ms | 3.6× | 6.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 23.96ms | 89.62ms | 3.7× | 6.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.84ms | 97.41ms | 3.1× | 5.1% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.08ms | 38.53ms | 1.2× | 1.3% | PASS |
| mem_reads | `oltp_range_select` | 19.23ms | 20.73ms | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_sum_range` | 17.96ms | 19.96ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_order_range` | 3.55ms | 3.81ms | 1.1× | 0.7% | PASS |
| mem_reads | `oltp_distinct_range` | 4.71ms | 4.88ms | 1.0× | 0.7% | PASS |
| mem_reads | `oltp_index_scan` | 4.54ms | 5.83ms | 1.3× | 1.0% | PASS |
| mem_reads | `select_random_points` | 27.85ms | 31.83ms | 1.1× | 1.5% | PASS |
| mem_reads | `select_random_ranges` | 7.76ms | 8.76ms | 1.1× | 0.8% | PASS |
| mem_reads | `covering_index_scan` | 4.22ms | 4.11ms | 1.0× | 1.4% | PASS |
| mem_reads | `groupby_scan` | 37.30ms | 38.66ms | 1.0× | 0.6% | PASS |
| mem_reads | `index_join` | 8.09ms | 10.24ms | 1.3× | 1.2% | PASS |
| mem_reads | `index_join_scan` | 4.07ms | 5.30ms | 1.3× | 1.9% | PASS |
| mem_reads | `types_table_scan` | 1.04s | 1.23s | 1.2× | 0.5% | PASS |
| mem_reads | `table_scan` | 1.20s | 1.37s | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_read_only` | 151.43ms | 165.02ms | 1.1× | 0.9% | PASS |
| mem_writes | `oltp_bulk_insert` | 243.38ms | 339.63ms | 1.4× | 1.2% | PASS |
| mem_writes | `oltp_insert` | 18.81ms | 34.21ms | 1.8× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 66.77ms | 117.33ms | 1.8× | 0.8% | PASS |
| mem_writes | `oltp_update_non_index` | 50.57ms | 75.17ms | 1.5× | 1.2% | PASS |
| mem_writes | `oltp_delete_insert` | 49.40ms | 93.39ms | 1.9× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 26.96ms | 55.67ms | 2.1× | 0.9% | PASS |
| mem_writes | `types_delete_insert` | 32.27ms | 51.80ms | 1.6× | 0.9% | PASS |
| mem_writes | `oltp_read_write` | 101.98ms | 155.77ms | 1.5× | 1.0% | PASS |
| file_reads | `oltp_point_select` | 103.83ms | 56.98ms | 0.5× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 26.39ms | 22.84ms | 0.9× | 1.6% | PASS |
| file_reads | `oltp_sum_range` | 25.55ms | 22.18ms | 0.9× | 1.0% | PASS |
| file_reads | `oltp_order_range` | 4.37ms | 4.13ms | 0.9× | 1.9% | PASS |
| file_reads | `oltp_distinct_range` | 5.60ms | 5.22ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 11.77ms | 8.04ms | 0.7× | 1.1% | PASS |
| file_reads | `select_random_points` | 37.04ms | 35.09ms | 0.9× | 1.2% | PASS |
| file_reads | `select_random_ranges` | 15.29ms | 10.83ms | 0.7× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 11.27ms | 6.39ms | 0.6× | 1.0% | PASS |
| file_reads | `groupby_scan` | 38.15ms | 39.04ms | 1.0× | 0.7% | PASS |
| file_reads | `index_join` | 12.20ms | 12.16ms | 1.0× | 1.4% | PASS |
| file_reads | `index_join_scan` | 5.01ms | 5.71ms | 1.1× | 1.4% | PASS |
| file_reads | `types_table_scan` | 1.03s | 1.22s | 1.2× | 0.4% | PASS |
| file_reads | `table_scan` | 1.20s | 1.37s | 1.1× | 0.4% | PASS |
| file_reads | `oltp_read_only` | 257.43ms | 194.02ms | 0.8× | 0.7% | PASS |
| file_writes | `oltp_bulk_insert` | 256.90ms | 350.40ms | 1.4× | 1.1% | PASS |
| file_writes | `oltp_insert` | 28.61ms | 40.01ms | 1.4× | 1.6% | PASS |
| file_writes | `oltp_update_index` | 96.69ms | 130.84ms | 1.4× | 1.2% | PASS |
| file_writes | `oltp_update_non_index` | 79.92ms | 88.94ms | 1.1× | 2.0% | PASS |
| file_writes | `oltp_delete_insert` | 80.36ms | 105.93ms | 1.3× | 1.5% | PASS |
| file_writes | `oltp_write_only` | 56.28ms | 65.71ms | 1.2× | 1.4% | PASS |
| file_writes | `types_delete_insert` | 50.27ms | 58.42ms | 1.2× | 1.6% | PASS |
| file_writes | `oltp_read_write` | 133.63ms | 166.35ms | 1.2× | 1.2% | PASS |
| ac_reads | `oltp_point_select` | 56.20ms | 57.09ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 21.88ms | 22.82ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_sum_range` | 20.67ms | 22.12ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_order_range` | 3.92ms | 4.11ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_distinct_range` | 5.10ms | 5.20ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_index_scan` | 7.18ms | 8.05ms | 1.1× | 1.1% | PASS |
| ac_reads | `select_random_points` | 31.35ms | 35.09ms | 1.1× | 1.2% | PASS |
| ac_reads | `select_random_ranges` | 10.36ms | 10.92ms | 1.1× | 1.2% | PASS |
| ac_reads | `covering_index_scan` | 6.86ms | 6.43ms | 0.9× | 1.3% | PASS |
| ac_reads | `groupby_scan` | 37.78ms | 39.12ms | 1.0× | 0.6% | PASS |
| ac_reads | `index_join` | 9.76ms | 12.14ms | 1.2× | 1.2% | PASS |
| ac_reads | `index_join_scan` | 4.56ms | 5.80ms | 1.3× | 2.1% | PASS |
| ac_reads | `types_table_scan` | 1.04s | 1.23s | 1.2× | 0.5% | PASS |
| ac_reads | `table_scan` | 1.20s | 1.37s | 1.1× | 0.5% | PASS |
| ac_reads | `oltp_read_only` | 187.32ms | 194.25ms | 1.0× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 26.93ms | 89.77ms | 3.3× | 6.0% | PASS |
| ac_writes | `oltp_insert_ac` | 29.38ms | 105.52ms | 3.6× | 6.2% | PASS |
| ac_writes | `oltp_update_index_ac` | 32.08ms | 114.55ms | 3.6× | 5.9% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 28.71ms | 99.94ms | 3.5× | 6.1% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 29.91ms | 108.62ms | 3.6× | 6.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 29.69ms | 109.82ms | 3.7× | 6.4% | PASS |
| ac_writes | `types_delete_insert_ac` | 27.36ms | 99.76ms | 3.6× | 7.3% | PASS |
| ac_writes | `oltp_read_write_ac` | 36.90ms | 114.88ms | 3.1× | 7.2% | PASS |

</details>

</details>

## Version-control latency

Wall time: 2m 26s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 85.99ms | 130.00ms | 66.1% | 1.0% | PASS |
| `status_dirty_many_tables` | 89.34ms | 130.00ms | 68.7% | 0.7% | PASS |
| `diff_regular_working_one_table` | 80.51ms | 120.00ms | 67.1% | 0.6% | PASS |
| `diff_regular_working_many_tables` | 95.37ms | 140.00ms | 68.1% | 0.7% | PASS |
| `diff_stat_working_many_tables` | 95.54ms | 140.00ms | 68.2% | 0.7% | PASS |
| `diff_schema_working_many_tables` | 96.42ms | 140.00ms | 68.9% | 0.7% | PASS |
| `branch_list_many_branches` | 24.94ms | 35.00ms | 71.3% | 1.3% | PASS |
| `branch_create_delete` | 27.44ms | 40.00ms | 68.6% | 1.6% | PASS |
| `checkout_branch_clean` | 57.53ms | 150.00ms | 38.4% | 1.0% | PASS |
| `merge_data_no_conflicts` | 30.42ms | 50.00ms | 60.8% | 1.3% | PASS |
| `merge_schema_no_conflicts` | 23.37ms | 35.00ms | 66.8% | 1.5% | PASS |
| `merge_data_conflicts` | 129.23ms | 180.00ms | 71.8% | 0.2% | PASS |
| `merge_data_conflicts_with_resolve` | 129.00ms | 180.00ms | 71.7% | 0.3% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
