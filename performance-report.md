# DoltLite Performance Report

> Nightly result: **FAIL**
>
> Generated: 2026-08-13 11:24 UTC
>
> Commit: [`cbc758f4fefb418275bae27af8267cc0f8f05d7f`](https://github.com/dolthub/doltlite/commit/cbc758f4fefb418275bae27af8267cc0f8f05d7f)
>
> Runner: ubuntu24 20260720.247.2
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/31688351087)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 11.11s | 11.81s | 1.1× | 1.7% | **PASS** |
| Writes | 2.23s | 3.67s | 1.6× | 1.3% | **FAIL** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 12.23s | 12.08s | 1.0× | 1.5% | **PASS** |
| Writes | 3.11s | 4.01s | 1.3× | 1.8% | **PASS** |
| Autocommit writes | 770.50ms | 2.89s | 3.8× | 6.1% | **PASS** |

The absolute ceiling is 2.4× per ordinary workload and 1.95× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.60s | 2.77s | 1.1× | 1.3% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.59s | 2.97s | 1.1× | 2.0% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.89s | 2.98s | 1.0× | 1.8% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 3.03s | 3.09s | 1.0× | 1.4% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 447.44ms | 686.45ms | 1.5× | 1.4% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 586.90ms | 1.00s | 1.7× | 1.3% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 582.32ms | 984.51ms | 1.7× | 1.7% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 610.68ms | 992.03ms | 1.6× | 1.0% | **FAIL** |
| File-backed | Reads | int | 15 | 55 | 2.91s | 2.86s | 1.0× | 1.4% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 3.08s | 3.09s | 1.0× | 1.8% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 3.24s | 3.06s | 0.9× | 1.8% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.99s | 3.07s | 1.0× | 1.1% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 596.27ms | 755.22ms | 1.3× | 1.7% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 925.21ms | 1.12s | 1.2× | 3.7% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 825.80ms | 1.07s | 1.3× | 1.8% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 760.88ms | 1.06s | 1.4× | 1.7% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.72s | 2.86s | 1.1× | 1.3% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.91s | 3.10s | 1.1× | 2.7% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.98s | 3.03s | 1.0× | 1.7% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 3.15s | 3.16s | 1.0× | 1.1% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 146.03ms | 587.59ms | 4.0× | 5.4% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 212.00ms | 754.12ms | 3.6× | 4.8% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 254.91ms | 904.48ms | 3.5× | 7.4% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 157.57ms | 645.75ms | 4.1× | 6.8% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 25.99ms | 26.78ms | 1.0× | 1.9% | PASS |
| mem_reads | `oltp_range_select` | 10.75ms | 10.70ms | 1.0× | 2.3% | PASS |
| mem_reads | `oltp_sum_range` | 9.71ms | 10.64ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_order_range` | 2.63ms | 2.72ms | 1.0× | 0.7% | PASS |
| mem_reads | `oltp_distinct_range` | 3.77ms | 3.83ms | 1.0× | 0.8% | PASS |
| mem_reads | `oltp_index_scan` | 3.96ms | 4.63ms | 1.2× | 1.0% | PASS |
| mem_reads | `select_random_points` | 11.62ms | 10.80ms | 0.9× | 5.2% | PASS |
| mem_reads | `select_random_ranges` | 3.21ms | 3.92ms | 1.2× | 0.9% | PASS |
| mem_reads | `covering_index_scan` | 4.34ms | 4.06ms | 0.9× | 1.6% | PASS |
| mem_reads | `groupby_scan` | 32.13ms | 34.63ms | 1.1× | 0.5% | PASS |
| mem_reads | `index_join` | 6.06ms | 7.57ms | 1.2× | 1.7% | PASS |
| mem_reads | `index_join_scan` | 3.72ms | 4.57ms | 1.2× | 3.3% | PASS |
| mem_reads | `types_table_scan` | 1.12s | 1.22s | 1.1× | 0.6% | PASS |
| mem_reads | `table_scan` | 1.26s | 1.31s | 1.0× | 0.7% | PASS |
| mem_reads | `oltp_read_only` | 105.56ms | 110.25ms | 1.0× | 1.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 179.12ms | 240.86ms | 1.3× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 15.80ms | 27.84ms | 1.8× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 51.67ms | 102.46ms | 2.0× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 35.93ms | 54.67ms | 1.5× | 1.7% | PASS |
| mem_writes | `oltp_delete_insert` | 46.14ms | 76.85ms | 1.7× | 1.8% | PASS |
| mem_writes | `oltp_write_only` | 22.49ms | 43.51ms | 1.9× | 2.1% | PASS |
| mem_writes | `types_delete_insert` | 26.03ms | 37.04ms | 1.4× | 1.1% | PASS |
| mem_writes | `oltp_read_write` | 70.26ms | 103.23ms | 1.5× | 1.6% | PASS |
| file_reads | `oltp_point_select` | 107.11ms | 48.10ms | 0.4× | 1.4% | PASS |
| file_reads | `oltp_range_select` | 18.63ms | 12.97ms | 0.7× | 2.6% | PASS |
| file_reads | `oltp_sum_range` | 17.43ms | 12.97ms | 0.7× | 1.1% | PASS |
| file_reads | `oltp_order_range` | 3.39ms | 3.04ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_distinct_range` | 4.51ms | 4.11ms | 0.9× | 1.1% | PASS |
| file_reads | `oltp_index_scan` | 12.12ms | 6.91ms | 0.6× | 1.2% | PASS |
| file_reads | `select_random_points` | 17.98ms | 12.78ms | 0.7× | 1.8% | PASS |
| file_reads | `select_random_ranges` | 11.09ms | 6.01ms | 0.5× | 0.9% | PASS |
| file_reads | `covering_index_scan` | 12.74ms | 6.33ms | 0.5× | 2.0% | PASS |
| file_reads | `groupby_scan` | 32.92ms | 34.82ms | 1.1× | 0.7% | PASS |
| file_reads | `index_join` | 10.42ms | 8.95ms | 0.9× | 1.4% | PASS |
| file_reads | `index_join_scan` | 4.74ms | 5.01ms | 1.1× | 1.5% | PASS |
| file_reads | `types_table_scan` | 1.18s | 1.24s | 1.1× | 2.4% | PASS |
| file_reads | `table_scan` | 1.25s | 1.31s | 1.0× | 0.5% | PASS |
| file_reads | `oltp_read_only` | 222.00ms | 140.84ms | 0.6× | 0.5% | PASS |
| file_writes | `oltp_bulk_insert` | 193.34ms | 249.46ms | 1.3× | 1.1% | PASS |
| file_writes | `oltp_insert` | 21.86ms | 31.97ms | 1.5× | 1.7% | PASS |
| file_writes | `oltp_update_index` | 77.94ms | 111.98ms | 1.4× | 1.7% | PASS |
| file_writes | `oltp_update_non_index` | 60.20ms | 68.29ms | 1.1× | 1.9% | PASS |
| file_writes | `oltp_delete_insert` | 67.68ms | 84.81ms | 1.3× | 2.1% | PASS |
| file_writes | `oltp_write_only` | 45.04ms | 54.06ms | 1.2× | 2.1% | PASS |
| file_writes | `types_delete_insert` | 40.53ms | 42.86ms | 1.1× | 1.5% | PASS |
| file_writes | `oltp_read_write` | 89.69ms | 111.80ms | 1.2× | 1.2% | PASS |
| ac_reads | `oltp_point_select` | 53.34ms | 48.35ms | 0.9× | 0.9% | PASS |
| ac_reads | `oltp_range_select` | 14.91ms | 13.13ms | 0.9× | 2.0% | PASS |
| ac_reads | `oltp_sum_range` | 13.26ms | 13.02ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_order_range` | 3.09ms | 3.08ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_distinct_range` | 4.19ms | 4.13ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_index_scan` | 7.36ms | 7.33ms | 1.0× | 1.0% | PASS |
| ac_reads | `select_random_points` | 14.61ms | 13.09ms | 0.9× | 3.3% | PASS |
| ac_reads | `select_random_ranges` | 6.08ms | 6.12ms | 1.0× | 0.8% | PASS |
| ac_reads | `covering_index_scan` | 7.66ms | 6.66ms | 0.9× | 1.4% | PASS |
| ac_reads | `groupby_scan` | 32.84ms | 34.95ms | 1.1× | 0.6% | PASS |
| ac_reads | `index_join` | 7.94ms | 9.36ms | 1.2× | 1.5% | PASS |
| ac_reads | `index_join_scan` | 4.14ms | 4.88ms | 1.2× | 1.5% | PASS |
| ac_reads | `types_table_scan` | 1.12s | 1.22s | 1.1× | 1.0% | PASS |
| ac_reads | `table_scan` | 1.28s | 1.33s | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_read_only` | 150.12ms | 142.86ms | 1.0× | 1.1% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 16.00ms | 58.02ms | 3.6× | 5.4% | PASS |
| ac_writes | `oltp_insert_ac` | 18.32ms | 72.21ms | 3.9× | 5.3% | PASS |
| ac_writes | `oltp_update_index_ac` | 20.19ms | 84.85ms | 4.2× | 6.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 16.13ms | 66.02ms | 4.1× | 6.4% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 17.72ms | 78.97ms | 4.5× | 4.4% | PASS |
| ac_writes | `oltp_write_only_ac` | 18.20ms | 77.67ms | 4.3× | 4.0% | PASS |
| ac_writes | `types_delete_insert_ac` | 15.93ms | 66.27ms | 4.2× | 6.8% | PASS |
| ac_writes | `oltp_read_write_ac` | 23.54ms | 83.57ms | 3.6× | 5.1% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 29.80ms | 36.71ms | 1.2× | 1.5% | PASS |
| mem_reads | `oltp_range_select` | 13.81ms | 13.63ms | 1.0× | 2.9% | PASS |
| mem_reads | `oltp_sum_range` | 12.22ms | 13.63ms | 1.1× | 3.4% | PASS |
| mem_reads | `oltp_order_range` | 2.85ms | 3.18ms | 1.1× | 1.9% | PASS |
| mem_reads | `oltp_distinct_range` | 3.94ms | 4.25ms | 1.1× | 1.9% | PASS |
| mem_reads | `oltp_index_scan` | 4.38ms | 5.86ms | 1.3× | 2.6% | PASS |
| mem_reads | `select_random_points` | 18.07ms | 21.32ms | 1.2× | 2.0% | PASS |
| mem_reads | `select_random_ranges` | 4.00ms | 5.18ms | 1.3× | 2.0% | PASS |
| mem_reads | `covering_index_scan` | 4.55ms | 4.43ms | 1.0× | 2.7% | PASS |
| mem_reads | `groupby_scan` | 32.56ms | 34.07ms | 1.0× | 0.8% | PASS |
| mem_reads | `index_join` | 7.01ms | 8.93ms | 1.3× | 2.1% | PASS |
| mem_reads | `index_join_scan` | 4.34ms | 5.25ms | 1.2× | 4.1% | PASS |
| mem_reads | `types_table_scan` | 1.09s | 1.26s | 1.2× | 0.9% | PASS |
| mem_reads | `table_scan` | 1.24s | 1.42s | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_read_only` | 118.70ms | 135.07ms | 1.1× | 0.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 234.81ms | 355.06ms | 1.5× | 1.3% | PASS |
| mem_writes | `oltp_insert` | 21.86ms | 38.49ms | 1.8× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 72.24ms | 149.56ms | 2.1× | 1.3% | PASS |
| mem_writes | `oltp_update_non_index` | 50.47ms | 80.92ms | 1.6× | 1.5% | PASS |
| mem_writes | `oltp_delete_insert` | 54.14ms | 110.28ms | 2.0× | 1.2% | PASS |
| mem_writes | `oltp_write_only` | 30.51ms | 57.51ms | 1.9× | 2.8% | PASS |
| mem_writes | `types_delete_insert` | 34.40ms | 54.40ms | 1.6× | 1.3% | PASS |
| mem_writes | `oltp_read_write` | 88.47ms | 155.79ms | 1.8× | 2.1% | PASS |
| file_reads | `oltp_point_select` | 100.04ms | 56.55ms | 0.6× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 22.38ms | 16.10ms | 0.7× | 3.3% | PASS |
| file_reads | `oltp_sum_range` | 20.79ms | 16.10ms | 0.8× | 2.2% | PASS |
| file_reads | `oltp_order_range` | 3.98ms | 3.58ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_distinct_range` | 5.09ms | 4.70ms | 0.9× | 1.8% | PASS |
| file_reads | `oltp_index_scan` | 12.08ms | 8.35ms | 0.7× | 1.7% | PASS |
| file_reads | `select_random_points` | 27.70ms | 25.03ms | 0.9× | 2.3% | PASS |
| file_reads | `select_random_ranges` | 11.22ms | 7.28ms | 0.6× | 1.3% | PASS |
| file_reads | `covering_index_scan` | 12.98ms | 6.79ms | 0.5× | 3.0% | PASS |
| file_reads | `groupby_scan` | 33.95ms | 34.73ms | 1.0× | 1.4% | PASS |
| file_reads | `index_join` | 12.16ms | 11.22ms | 0.9× | 3.2% | PASS |
| file_reads | `index_join_scan` | 6.01ms | 6.18ms | 1.0× | 2.9% | PASS |
| file_reads | `types_table_scan` | 1.15s | 1.27s | 1.1× | 1.7% | PASS |
| file_reads | `table_scan` | 1.45s | 1.46s | 1.0× | 7.9% | PASS |
| file_reads | `oltp_read_only` | 223.91ms | 164.62ms | 0.7× | 1.1% | PASS |
| file_writes | `oltp_bulk_insert` | 254.41ms | 370.77ms | 1.5× | 1.1% | PASS |
| file_writes | `oltp_insert` | 58.73ms | 46.43ms | 0.8× | 21.9% | PASS |
| file_writes | `oltp_update_index` | 115.49ms | 168.31ms | 1.5× | 2.0% | PASS |
| file_writes | `oltp_update_non_index` | 95.93ms | 96.97ms | 1.0× | 9.3% | PASS |
| file_writes | `oltp_delete_insert` | 92.39ms | 122.71ms | 1.3× | 1.2% | PASS |
| file_writes | `oltp_write_only` | 87.29ms | 69.51ms | 0.8× | 10.4% | PASS |
| file_writes | `types_delete_insert` | 59.56ms | 66.67ms | 1.1× | 1.8% | PASS |
| file_writes | `oltp_read_write` | 161.40ms | 180.79ms | 1.1× | 5.4% | PASS |
| ac_reads | `oltp_point_select` | 54.84ms | 57.03ms | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_range_select` | 17.93ms | 16.03ms | 0.9× | 3.1% | PASS |
| ac_reads | `oltp_sum_range` | 16.16ms | 16.15ms | 1.0× | 3.6% | PASS |
| ac_reads | `oltp_order_range` | 3.43ms | 3.56ms | 1.0× | 2.8% | PASS |
| ac_reads | `oltp_distinct_range` | 4.52ms | 4.65ms | 1.0× | 2.0% | PASS |
| ac_reads | `oltp_index_scan` | 7.31ms | 8.37ms | 1.1× | 2.5% | PASS |
| ac_reads | `select_random_points` | 21.10ms | 24.88ms | 1.2× | 1.7% | PASS |
| ac_reads | `select_random_ranges` | 6.35ms | 7.24ms | 1.1× | 1.6% | PASS |
| ac_reads | `covering_index_scan` | 7.39ms | 6.73ms | 0.9× | 5.1% | PASS |
| ac_reads | `groupby_scan` | 33.52ms | 34.71ms | 1.0× | 1.3% | PASS |
| ac_reads | `index_join` | 10.27ms | 11.21ms | 1.1× | 2.7% | PASS |
| ac_reads | `index_join_scan` | 5.78ms | 5.84ms | 1.0× | 3.3% | PASS |
| ac_reads | `types_table_scan` | 1.11s | 1.28s | 1.2× | 2.9% | PASS |
| ac_reads | `table_scan` | 1.46s | 1.46s | 1.0× | 8.8% | PASS |
| ac_reads | `oltp_read_only` | 153.73ms | 164.25ms | 1.1× | 1.1% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 23.47ms | 78.42ms | 3.3× | 4.2% | PASS |
| ac_writes | `oltp_insert_ac` | 26.69ms | 93.20ms | 3.5× | 4.3% | PASS |
| ac_writes | `oltp_update_index_ac` | 29.07ms | 104.65ms | 3.6× | 4.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.79ms | 88.19ms | 3.7× | 6.1% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.47ms | 101.96ms | 3.9× | 8.8% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.57ms | 95.94ms | 3.6× | 4.4% | PASS |
| ac_writes | `types_delete_insert_ac` | 23.32ms | 89.23ms | 3.8× | 7.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.63ms | 102.53ms | 3.1× | 5.0% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 31.38ms | 37.44ms | 1.2× | 1.4% | PASS |
| mem_reads | `oltp_range_select` | 13.98ms | 13.86ms | 1.0× | 2.1% | PASS |
| mem_reads | `oltp_sum_range` | 12.67ms | 13.77ms | 1.1× | 3.4% | PASS |
| mem_reads | `oltp_order_range` | 3.09ms | 3.17ms | 1.0× | 1.4% | PASS |
| mem_reads | `oltp_distinct_range` | 4.08ms | 4.28ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 4.77ms | 6.34ms | 1.3× | 2.4% | PASS |
| mem_reads | `select_random_points` | 18.91ms | 21.91ms | 1.2× | 2.1% | PASS |
| mem_reads | `select_random_ranges` | 4.24ms | 5.31ms | 1.3× | 1.7% | PASS |
| mem_reads | `covering_index_scan` | 4.51ms | 4.69ms | 1.0× | 2.2% | PASS |
| mem_reads | `groupby_scan` | 32.43ms | 34.17ms | 1.1× | 0.6% | PASS |
| mem_reads | `index_join` | 6.95ms | 9.61ms | 1.4× | 1.8% | PASS |
| mem_reads | `index_join_scan` | 4.43ms | 5.57ms | 1.3× | 2.7% | PASS |
| mem_reads | `types_table_scan` | 1.16s | 1.26s | 1.1× | 2.3% | PASS |
| mem_reads | `table_scan` | 1.47s | 1.42s | 1.0× | 1.3% | PASS |
| mem_reads | `oltp_read_only` | 123.93ms | 136.22ms | 1.1× | 1.4% | PASS |
| mem_writes | `oltp_bulk_insert` | 236.52ms | 352.80ms | 1.5× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 20.09ms | 38.91ms | 1.9× | 1.1% | PASS |
| mem_writes | `oltp_update_index` | 71.67ms | 145.88ms | 2.0× | 2.1% | PASS |
| mem_writes | `oltp_update_non_index` | 49.33ms | 80.50ms | 1.6× | 1.4% | PASS |
| mem_writes | `oltp_delete_insert` | 50.73ms | 111.04ms | 2.2× | 1.3% | PASS |
| mem_writes | `oltp_write_only` | 29.90ms | 58.85ms | 2.0× | 1.9% | PASS |
| mem_writes | `types_delete_insert` | 33.59ms | 54.30ms | 1.6× | 2.0% | PASS |
| mem_writes | `oltp_read_write` | 90.50ms | 142.22ms | 1.6× | 2.7% | PASS |
| file_reads | `oltp_point_select` | 102.01ms | 55.03ms | 0.5× | 1.1% | PASS |
| file_reads | `oltp_range_select` | 21.37ms | 15.67ms | 0.7× | 2.2% | PASS |
| file_reads | `oltp_sum_range` | 20.37ms | 15.71ms | 0.8× | 1.8% | PASS |
| file_reads | `oltp_order_range` | 4.08ms | 3.63ms | 0.9× | 3.1% | PASS |
| file_reads | `oltp_distinct_range` | 5.16ms | 4.78ms | 0.9× | 2.3% | PASS |
| file_reads | `oltp_index_scan` | 12.16ms | 8.15ms | 0.7× | 1.7% | PASS |
| file_reads | `select_random_points` | 27.57ms | 24.03ms | 0.9× | 2.2% | PASS |
| file_reads | `select_random_ranges` | 11.39ms | 7.10ms | 0.6× | 0.8% | PASS |
| file_reads | `covering_index_scan` | 12.51ms | 6.63ms | 0.5× | 1.4% | PASS |
| file_reads | `groupby_scan` | 33.79ms | 34.75ms | 1.0× | 0.9% | PASS |
| file_reads | `index_join` | 12.06ms | 11.31ms | 0.9× | 3.0% | PASS |
| file_reads | `index_join_scan` | 5.56ms | 6.15ms | 1.1× | 3.1% | PASS |
| file_reads | `types_table_scan` | 1.25s | 1.28s | 1.0× | 1.2% | PASS |
| file_reads | `table_scan` | 1.49s | 1.42s | 1.0× | 2.0% | PASS |
| file_reads | `oltp_read_only` | 232.19ms | 164.25ms | 0.7× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 255.08ms | 363.23ms | 1.4× | 1.1% | PASS |
| file_writes | `oltp_insert` | 37.19ms | 45.92ms | 1.2× | 2.6% | PASS |
| file_writes | `oltp_update_index` | 109.04ms | 158.59ms | 1.5× | 1.7% | PASS |
| file_writes | `oltp_update_non_index` | 85.08ms | 93.58ms | 1.1× | 1.9% | PASS |
| file_writes | `oltp_delete_insert` | 88.06ms | 123.32ms | 1.4× | 1.3% | PASS |
| file_writes | `oltp_write_only` | 65.00ms | 70.31ms | 1.1× | 2.3% | PASS |
| file_writes | `types_delete_insert` | 55.76ms | 63.84ms | 1.1× | 1.8% | PASS |
| file_writes | `oltp_read_write` | 130.60ms | 154.45ms | 1.2× | 2.4% | PASS |
| ac_reads | `oltp_point_select` | 54.23ms | 55.15ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_range_select` | 16.45ms | 15.52ms | 0.9× | 2.9% | PASS |
| ac_reads | `oltp_sum_range` | 15.57ms | 15.57ms | 1.0× | 1.8% | PASS |
| ac_reads | `oltp_order_range` | 3.45ms | 3.46ms | 1.0× | 2.2% | PASS |
| ac_reads | `oltp_distinct_range` | 4.43ms | 4.56ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 7.48ms | 8.15ms | 1.1× | 1.2% | PASS |
| ac_reads | `select_random_points` | 21.84ms | 23.91ms | 1.1× | 2.2% | PASS |
| ac_reads | `select_random_ranges` | 6.83ms | 7.12ms | 1.0× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 7.93ms | 6.64ms | 0.8× | 2.2% | PASS |
| ac_reads | `groupby_scan` | 33.05ms | 34.81ms | 1.1× | 0.7% | PASS |
| ac_reads | `index_join` | 9.21ms | 11.05ms | 1.2× | 2.1% | PASS |
| ac_reads | `index_join_scan` | 4.89ms | 6.01ms | 1.2× | 2.5% | PASS |
| ac_reads | `types_table_scan` | 1.18s | 1.26s | 1.1× | 1.7% | PASS |
| ac_reads | `table_scan` | 1.46s | 1.42s | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_read_only` | 161.66ms | 164.39ms | 1.0× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 29.63ms | 97.14ms | 3.3× | 5.5% | PASS |
| ac_writes | `oltp_insert_ac` | 31.80ms | 115.69ms | 3.6× | 10.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 33.81ms | 121.03ms | 3.6× | 5.0% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 28.99ms | 105.75ms | 3.6× | 7.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 31.47ms | 120.50ms | 3.8× | 7.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 33.63ms | 116.36ms | 3.5× | 12.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 28.62ms | 110.39ms | 3.9× | 8.6% | PASS |
| ac_writes | `oltp_read_write_ac` | 36.96ms | 117.62ms | 3.2× | 6.8% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.71ms | 36.26ms | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_range_select` | 20.34ms | 20.98ms | 1.0× | 1.4% | PASS |
| mem_reads | `oltp_sum_range` | 18.55ms | 20.33ms | 1.1× | 1.7% | PASS |
| mem_reads | `oltp_order_range` | 3.78ms | 3.91ms | 1.0× | 0.8% | PASS |
| mem_reads | `oltp_distinct_range` | 4.84ms | 5.01ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 4.82ms | 5.99ms | 1.2× | 2.2% | PASS |
| mem_reads | `select_random_points` | 29.46ms | 32.44ms | 1.1× | 1.9% | PASS |
| mem_reads | `select_random_ranges` | 7.62ms | 8.23ms | 1.1× | 1.1% | PASS |
| mem_reads | `covering_index_scan` | 4.35ms | 4.13ms | 1.0× | 2.8% | PASS |
| mem_reads | `groupby_scan` | 39.10ms | 42.66ms | 1.1× | 0.7% | PASS |
| mem_reads | `index_join` | 8.14ms | 9.99ms | 1.2× | 1.9% | PASS |
| mem_reads | `index_join_scan` | 4.25ms | 5.54ms | 1.3× | 1.3% | PASS |
| mem_reads | `types_table_scan` | 1.20s | 1.31s | 1.1× | 2.3% | PASS |
| mem_reads | `table_scan` | 1.50s | 1.42s | 0.9× | 1.3% | PASS |
| mem_reads | `oltp_read_only` | 156.91ms | 166.87ms | 1.1× | 1.4% | PASS |
| mem_writes | `oltp_bulk_insert` | 246.10ms | 334.12ms | 1.4× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 19.52ms | 35.46ms | 1.8× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 70.66ms | 133.16ms | 1.9× | 0.9% | PASS |
| mem_writes | `oltp_update_non_index` | 52.79ms | 78.59ms | 1.5× | 1.2% | PASS |
| mem_writes | `oltp_delete_insert` | 51.38ms | 104.86ms | 2.0× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 28.65ms | 76.50ms | 2.7× | 1.3% | FAIL |
| mem_writes | `types_delete_insert` | 34.06ms | 54.50ms | 1.6× | 1.0% | PASS |
| mem_writes | `oltp_read_write` | 107.53ms | 174.84ms | 1.6× | 1.3% | PASS |
| file_reads | `oltp_point_select` | 119.05ms | 59.78ms | 0.5× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 29.72ms | 23.39ms | 0.8× | 0.7% | PASS |
| file_reads | `oltp_sum_range` | 27.30ms | 22.56ms | 0.8× | 1.1% | PASS |
| file_reads | `oltp_order_range` | 4.54ms | 4.16ms | 0.9× | 2.6% | PASS |
| file_reads | `oltp_distinct_range` | 5.47ms | 5.30ms | 1.0× | 1.1% | PASS |
| file_reads | `oltp_index_scan` | 12.86ms | 7.99ms | 0.6× | 2.1% | PASS |
| file_reads | `select_random_points` | 35.73ms | 33.10ms | 0.9× | 2.2% | PASS |
| file_reads | `select_random_ranges` | 15.90ms | 10.46ms | 0.7× | 1.5% | PASS |
| file_reads | `covering_index_scan` | 12.81ms | 6.70ms | 0.5× | 1.4% | PASS |
| file_reads | `groupby_scan` | 39.61ms | 42.75ms | 1.1× | 0.8% | PASS |
| file_reads | `index_join` | 12.56ms | 11.86ms | 0.9× | 1.3% | PASS |
| file_reads | `index_join_scan` | 5.19ms | 5.94ms | 1.1× | 1.7% | PASS |
| file_reads | `types_table_scan` | 1.12s | 1.27s | 1.1× | 0.8% | PASS |
| file_reads | `table_scan` | 1.28s | 1.37s | 1.1× | 0.4% | PASS |
| file_reads | `oltp_read_only` | 273.79ms | 196.60ms | 0.7× | 1.1% | PASS |
| file_writes | `oltp_bulk_insert` | 262.63ms | 346.23ms | 1.3× | 1.2% | PASS |
| file_writes | `oltp_insert` | 25.86ms | 40.93ms | 1.6× | 1.8% | PASS |
| file_writes | `oltp_update_index` | 97.61ms | 142.03ms | 1.5× | 1.9% | PASS |
| file_writes | `oltp_update_non_index` | 76.40ms | 91.37ms | 1.2× | 1.7% | PASS |
| file_writes | `oltp_delete_insert` | 76.25ms | 114.81ms | 1.5× | 1.3% | PASS |
| file_writes | `oltp_write_only` | 51.22ms | 85.47ms | 1.7× | 1.8% | PASS |
| file_writes | `types_delete_insert` | 49.01ms | 58.58ms | 1.2× | 1.6% | PASS |
| file_writes | `oltp_read_write` | 121.90ms | 177.90ms | 1.5× | 1.9% | PASS |
| ac_reads | `oltp_point_select` | 61.65ms | 58.53ms | 0.9× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 23.13ms | 23.09ms | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_sum_range` | 21.52ms | 22.32ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_order_range` | 4.21ms | 4.16ms | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_distinct_range` | 5.25ms | 5.31ms | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_index_scan` | 8.02ms | 8.33ms | 1.0× | 0.7% | PASS |
| ac_reads | `select_random_points` | 32.21ms | 34.17ms | 1.1× | 1.8% | PASS |
| ac_reads | `select_random_ranges` | 10.71ms | 10.53ms | 1.0× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 7.65ms | 6.78ms | 0.9× | 1.1% | PASS |
| ac_reads | `groupby_scan` | 39.30ms | 42.91ms | 1.1× | 1.0% | PASS |
| ac_reads | `index_join` | 10.35ms | 12.42ms | 1.2× | 2.2% | PASS |
| ac_reads | `index_join_scan` | 4.87ms | 6.20ms | 1.3× | 2.9% | PASS |
| ac_reads | `types_table_scan` | 1.22s | 1.31s | 1.1× | 2.1% | PASS |
| ac_reads | `table_scan` | 1.50s | 1.42s | 0.9× | 2.1% | PASS |
| ac_reads | `oltp_read_only` | 202.96ms | 201.01ms | 1.0× | 1.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 16.50ms | 58.88ms | 3.6× | 5.2% | PASS |
| ac_writes | `oltp_insert_ac` | 18.72ms | 76.69ms | 4.1× | 4.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 20.18ms | 89.54ms | 4.4× | 5.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 16.70ms | 68.25ms | 4.1× | 6.9% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 18.55ms | 82.67ms | 4.5× | 6.7% | PASS |
| ac_writes | `oltp_write_only_ac` | 20.76ms | 89.91ms | 4.3× | 7.8% | PASS |
| ac_writes | `types_delete_insert_ac` | 18.82ms | 83.13ms | 4.4× | 13.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 27.35ms | 96.67ms | 3.5× | 7.7% | PASS |

</details>

</details>

## Version-control latency

Wall time: 2m 20s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 81.00ms | 130.00ms | 62.3% | 0.5% | PASS |
| `status_dirty_many_tables` | 84.62ms | 130.00ms | 65.1% | 0.5% | PASS |
| `diff_regular_working_one_table` | 78.61ms | 120.00ms | 65.5% | 0.6% | PASS |
| `diff_regular_working_many_tables` | 89.57ms | 140.00ms | 64.0% | 0.8% | PASS |
| `diff_stat_working_many_tables` | 89.25ms | 140.00ms | 63.7% | 0.7% | PASS |
| `diff_schema_working_many_tables` | 89.53ms | 140.00ms | 63.9% | 0.8% | PASS |
| `branch_list_many_branches` | 23.41ms | 35.00ms | 66.9% | 1.8% | PASS |
| `branch_create_delete` | 25.77ms | 40.00ms | 64.4% | 1.9% | PASS |
| `checkout_branch_clean` | 54.87ms | 150.00ms | 36.6% | 1.2% | PASS |
| `merge_data_no_conflicts` | 30.34ms | 50.00ms | 60.7% | 2.4% | PASS |
| `merge_schema_no_conflicts` | 23.83ms | 35.00ms | 68.1% | 2.4% | PASS |
| `merge_data_conflicts` | 128.07ms | 180.00ms | 71.2% | 0.5% | PASS |
| `merge_data_conflicts_with_resolve` | 127.53ms | 180.00ms | 70.8% | 0.4% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
