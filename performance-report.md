# DoltLite Performance Report

> Nightly result: **FAIL**
>
> Generated: 2026-09-21 11:06 UTC
>
> Commit: [`83693647b6c7c0e4a898d1b96fbe0f23913b9156`](https://github.com/dolthub/doltlite/commit/83693647b6c7c0e4a898d1b96fbe0f23913b9156)
>
> Runner: ubuntu24 20260907.300.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/35584579929)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 7.20s | 7.62s | 1.1× | 1.6% | **PASS** |
| Writes | 1.39s | 2.21s | 1.6× | 1.5% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 7.82s | 7.82s | 1.0× | 1.2% | **PASS** |
| Writes | 4.80s | 4.38s | 0.9× | 19.2% | **PASS** |
| Autocommit writes | 3.33s | 9.09s | 2.7× | 45.8% | **FAIL** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.04s | 2.15s | 1.1× | 1.6% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 1.74s | 1.78s | 1.0× | 2.3% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 1.58s | 1.70s | 1.1× | 1.5% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 1.84s | 1.99s | 1.1× | 1.5% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 353.09ms | 534.52ms | 1.5× | 1.4% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 357.46ms | 575.55ms | 1.6× | 1.9% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 327.78ms | 537.40ms | 1.6× | 1.2% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 355.12ms | 558.34ms | 1.6× | 1.3% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.30s | 2.23s | 1.0× | 1.1% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 1.85s | 1.82s | 1.0× | 1.5% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 1.75s | 1.76s | 1.0× | 0.9% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 1.92s | 2.02s | 1.1× | 1.5% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 1.14s | 977.86ms | 0.9× | 27.6% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.17s | 1.09s | 0.9× | 13.0% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 977.27ms | 975.70ms | 1.0× | 8.9% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 1.51s | 1.34s | 0.9× | 38.3% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.09s | 2.21s | 1.1× | 1.0% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 1.75s | 1.81s | 1.0× | 1.3% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 1.63s | 1.75s | 1.1× | 1.1% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 1.87s | 2.02s | 1.1× | 1.8% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 488.68ms | 1.58s | 3.2× | 52.0% | **FAIL** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 495.29ms | 1.50s | 3.0× | 45.4% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 308.17ms | 866.24ms | 2.8× | 21.2% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 2.04s | 5.14s | 2.5× | 52.1% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 20.16ms | 21.84ms | 1.1× | 2.6% | PASS |
| mem_reads | `oltp_range_select` | 8.56ms | 8.65ms | 1.0× | 2.9% | PASS |
| mem_reads | `oltp_sum_range` | 7.43ms | 8.35ms | 1.1× | 2.2% | PASS |
| mem_reads | `oltp_order_range` | 2.13ms | 2.21ms | 1.0× | 1.5% | PASS |
| mem_reads | `oltp_distinct_range` | 2.90ms | 3.15ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 3.13ms | 3.75ms | 1.2× | 1.8% | PASS |
| mem_reads | `select_random_points` | 8.80ms | 8.80ms | 1.0× | 4.8% | PASS |
| mem_reads | `select_random_ranges` | 3.65ms | 3.55ms | 1.0× | 1.6% | PASS |
| mem_reads | `covering_index_scan` | 5.84ms | 6.84ms | 1.2× | 0.9% | PASS |
| mem_reads | `groupby_scan` | 24.61ms | 27.14ms | 1.1× | 1.3% | PASS |
| mem_reads | `index_join` | 4.50ms | 5.68ms | 1.3× | 1.3% | PASS |
| mem_reads | `index_join_scan` | 2.64ms | 3.79ms | 1.4× | 2.1% | PASS |
| mem_reads | `types_table_scan` | 869.91ms | 927.41ms | 1.1× | 0.8% | PASS |
| mem_reads | `table_scan` | 988.06ms | 1.03s | 1.0× | 1.5% | PASS |
| mem_reads | `oltp_read_only` | 85.82ms | 91.02ms | 1.1× | 2.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 139.28ms | 196.64ms | 1.4× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 12.13ms | 21.30ms | 1.8× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 41.92ms | 71.59ms | 1.7× | 1.5% | PASS |
| mem_writes | `oltp_update_non_index` | 29.32ms | 41.10ms | 1.4× | 1.2% | PASS |
| mem_writes | `oltp_delete_insert` | 37.17ms | 56.38ms | 1.5× | 1.8% | PASS |
| mem_writes | `oltp_write_only` | 18.46ms | 35.81ms | 1.9× | 1.7% | PASS |
| mem_writes | `types_delete_insert` | 20.15ms | 27.02ms | 1.3× | 1.3% | PASS |
| mem_writes | `oltp_read_write` | 54.65ms | 84.68ms | 1.5× | 3.5% | PASS |
| file_reads | `oltp_point_select` | 85.54ms | 39.02ms | 0.5× | 1.1% | PASS |
| file_reads | `oltp_range_select` | 15.82ms | 10.56ms | 0.7× | 1.4% | PASS |
| file_reads | `oltp_sum_range` | 14.83ms | 10.38ms | 0.7× | 2.1% | PASS |
| file_reads | `oltp_order_range` | 2.94ms | 2.49ms | 0.8× | 1.1% | PASS |
| file_reads | `oltp_distinct_range` | 3.71ms | 3.45ms | 0.9× | 0.8% | PASS |
| file_reads | `oltp_index_scan` | 10.00ms | 5.94ms | 0.6× | 0.8% | PASS |
| file_reads | `select_random_points` | 16.46ms | 10.80ms | 0.7× | 2.5% | PASS |
| file_reads | `select_random_ranges` | 10.43ms | 5.49ms | 0.5× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 12.77ms | 9.09ms | 0.7× | 1.4% | PASS |
| file_reads | `groupby_scan` | 25.11ms | 27.39ms | 1.1× | 0.8% | PASS |
| file_reads | `index_join` | 8.22ms | 7.42ms | 0.9× | 0.8% | PASS |
| file_reads | `index_join_scan` | 3.50ms | 4.29ms | 1.2× | 2.6% | PASS |
| file_reads | `types_table_scan` | 941.93ms | 952.23ms | 1.0× | 2.3% | PASS |
| file_reads | `table_scan` | 979.49ms | 1.03s | 1.0× | 0.6% | PASS |
| file_reads | `oltp_read_only` | 171.37ms | 113.52ms | 0.7× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 262.14ms | 273.53ms | 1.0× | 21.4% | PASS |
| file_writes | `oltp_insert` | 30.47ms | 36.09ms | 1.2× | 32.1% | PASS |
| file_writes | `oltp_update_index` | 152.85ms | 145.48ms | 1.0× | 18.9% | PASS |
| file_writes | `oltp_update_non_index` | 160.91ms | 109.17ms | 0.7× | 33.2% | PASS |
| file_writes | `oltp_delete_insert` | 177.17ms | 123.34ms | 0.7× | 25.3% | PASS |
| file_writes | `oltp_write_only` | 106.70ms | 85.31ms | 0.8× | 30.0% | PASS |
| file_writes | `types_delete_insert` | 80.48ms | 62.97ms | 0.8× | 19.6% | PASS |
| file_writes | `oltp_read_write` | 168.74ms | 141.97ms | 0.8× | 31.5% | PASS |
| ac_reads | `oltp_point_select` | 41.10ms | 38.76ms | 0.9× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 11.57ms | 10.53ms | 0.9× | 2.5% | PASS |
| ac_reads | `oltp_sum_range` | 10.04ms | 10.26ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_order_range` | 2.54ms | 2.47ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_distinct_range` | 3.28ms | 3.45ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 5.77ms | 6.02ms | 1.0× | 0.9% | PASS |
| ac_reads | `select_random_points` | 11.52ms | 10.79ms | 0.9× | 3.1% | PASS |
| ac_reads | `select_random_ranges` | 6.11ms | 5.46ms | 0.9× | 1.4% | PASS |
| ac_reads | `covering_index_scan` | 8.52ms | 9.13ms | 1.1× | 0.7% | PASS |
| ac_reads | `groupby_scan` | 24.82ms | 27.36ms | 1.1× | 1.1% | PASS |
| ac_reads | `index_join` | 6.16ms | 7.47ms | 1.2× | 1.0% | PASS |
| ac_reads | `index_join_scan` | 3.05ms | 4.21ms | 1.4× | 1.0% | PASS |
| ac_reads | `types_table_scan` | 864.59ms | 928.78ms | 1.1× | 0.8% | PASS |
| ac_reads | `table_scan` | 980.99ms | 1.03s | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_read_only` | 114.62ms | 114.74ms | 1.0× | 1.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 42.52ms | 164.35ms | 3.9× | 33.5% | PASS |
| ac_writes | `oltp_insert_ac` | 71.75ms | 199.13ms | 2.8× | 46.8% | PASS |
| ac_writes | `oltp_update_index_ac` | 58.09ms | 239.42ms | 4.1× | 78.3% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 76.38ms | 157.41ms | 2.1× | 50.2% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 47.45ms | 172.08ms | 3.6× | 46.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 79.99ms | 168.13ms | 2.1× | 61.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 45.76ms | 285.96ms | 6.2× | 77.1% | FAIL |
| ac_writes | `oltp_read_write_ac` | 66.74ms | 192.87ms | 2.9× | 53.9% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 23.42ms | 22.17ms | 0.9× | 2.1% | PASS |
| mem_reads | `oltp_range_select` | 10.59ms | 8.50ms | 0.8× | 2.4% | PASS |
| mem_reads | `oltp_sum_range` | 10.39ms | 8.76ms | 0.8× | 2.3% | PASS |
| mem_reads | `oltp_order_range` | 2.21ms | 1.99ms | 0.9× | 2.8% | PASS |
| mem_reads | `oltp_distinct_range` | 2.69ms | 2.46ms | 0.9× | 1.7% | PASS |
| mem_reads | `oltp_index_scan` | 2.50ms | 3.80ms | 1.5× | 2.2% | PASS |
| mem_reads | `select_random_points` | 16.57ms | 13.74ms | 0.8× | 2.6% | PASS |
| mem_reads | `select_random_ranges` | 4.46ms | 3.70ms | 0.8× | 2.7% | PASS |
| mem_reads | `covering_index_scan` | 3.87ms | 5.14ms | 1.3× | 1.6% | PASS |
| mem_reads | `groupby_scan` | 19.91ms | 19.20ms | 1.0× | 1.1% | PASS |
| mem_reads | `index_join` | 7.94ms | 5.76ms | 0.7× | 4.9% | PASS |
| mem_reads | `index_join_scan` | 2.78ms | 4.75ms | 1.7× | 2.9% | PASS |
| mem_reads | `types_table_scan` | 696.75ms | 751.60ms | 1.1× | 1.2% | PASS |
| mem_reads | `table_scan` | 847.64ms | 852.22ms | 1.0× | 3.4% | PASS |
| mem_reads | `oltp_read_only` | 91.08ms | 77.90ms | 0.9× | 2.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 135.16ms | 198.56ms | 1.5× | 1.4% | PASS |
| mem_writes | `oltp_insert` | 10.21ms | 21.91ms | 2.1× | 1.7% | PASS |
| mem_writes | `oltp_update_index` | 42.17ms | 86.04ms | 2.0× | 2.0% | PASS |
| mem_writes | `oltp_update_non_index` | 34.56ms | 51.85ms | 1.5× | 2.9% | PASS |
| mem_writes | `oltp_delete_insert` | 35.13ms | 65.57ms | 1.9× | 2.0% | PASS |
| mem_writes | `oltp_write_only` | 18.34ms | 38.05ms | 2.1× | 1.7% | PASS |
| mem_writes | `types_delete_insert` | 24.78ms | 31.88ms | 1.3× | 2.1% | PASS |
| mem_writes | `oltp_read_write` | 57.09ms | 81.69ms | 1.4× | 1.8% | PASS |
| file_reads | `oltp_point_select` | 73.53ms | 35.64ms | 0.5× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 15.78ms | 9.99ms | 0.6× | 2.0% | PASS |
| file_reads | `oltp_sum_range` | 15.58ms | 10.13ms | 0.7× | 1.4% | PASS |
| file_reads | `oltp_order_range` | 2.90ms | 2.26ms | 0.8× | 1.1% | PASS |
| file_reads | `oltp_distinct_range` | 3.35ms | 2.73ms | 0.8× | 0.9% | PASS |
| file_reads | `oltp_index_scan` | 8.11ms | 5.68ms | 0.7× | 1.2% | PASS |
| file_reads | `select_random_points` | 22.22ms | 15.13ms | 0.7× | 2.5% | PASS |
| file_reads | `select_random_ranges` | 10.07ms | 5.32ms | 0.5× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 9.67ms | 7.13ms | 0.7× | 1.5% | PASS |
| file_reads | `groupby_scan` | 20.99ms | 19.73ms | 0.9× | 1.6% | PASS |
| file_reads | `index_join` | 11.45ms | 7.36ms | 0.6× | 2.5% | PASS |
| file_reads | `index_join_scan` | 3.29ms | 4.56ms | 1.4× | 1.3% | PASS |
| file_reads | `types_table_scan` | 697.08ms | 759.02ms | 1.1× | 1.8% | PASS |
| file_reads | `table_scan` | 803.70ms | 837.67ms | 1.0× | 1.8% | PASS |
| file_reads | `oltp_read_only` | 155.40ms | 93.78ms | 0.6× | 1.7% | PASS |
| file_writes | `oltp_bulk_insert` | 233.35ms | 276.49ms | 1.2× | 12.2% | PASS |
| file_writes | `oltp_insert` | 21.74ms | 45.18ms | 2.1× | 20.0% | PASS |
| file_writes | `oltp_update_index` | 160.33ms | 174.65ms | 1.1× | 12.9% | PASS |
| file_writes | `oltp_update_non_index` | 140.91ms | 113.07ms | 0.8× | 16.6% | PASS |
| file_writes | `oltp_delete_insert` | 180.21ms | 135.36ms | 0.8× | 12.6% | PASS |
| file_writes | `oltp_write_only` | 124.35ms | 104.78ms | 0.8× | 13.2% | PASS |
| file_writes | `types_delete_insert` | 150.14ms | 87.46ms | 0.6× | 18.2% | PASS |
| file_writes | `oltp_read_write` | 160.82ms | 150.69ms | 0.9× | 6.1% | PASS |
| ac_reads | `oltp_point_select` | 39.22ms | 35.71ms | 0.9× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 12.14ms | 9.81ms | 0.8× | 1.4% | PASS |
| ac_reads | `oltp_sum_range` | 12.12ms | 9.98ms | 0.8× | 1.3% | PASS |
| ac_reads | `oltp_order_range` | 2.58ms | 2.24ms | 0.9× | 1.1% | PASS |
| ac_reads | `oltp_distinct_range` | 3.04ms | 2.73ms | 0.9× | 1.6% | PASS |
| ac_reads | `oltp_index_scan` | 4.69ms | 5.62ms | 1.2× | 1.3% | PASS |
| ac_reads | `select_random_points` | 17.31ms | 14.53ms | 0.8× | 1.1% | PASS |
| ac_reads | `select_random_ranges` | 6.50ms | 5.26ms | 0.8× | 1.4% | PASS |
| ac_reads | `covering_index_scan` | 6.18ms | 6.98ms | 1.1× | 1.1% | PASS |
| ac_reads | `groupby_scan` | 20.14ms | 19.33ms | 1.0× | 1.0% | PASS |
| ac_reads | `index_join` | 9.52ms | 7.15ms | 0.8× | 1.8% | PASS |
| ac_reads | `index_join_scan` | 3.02ms | 4.59ms | 1.5× | 1.0% | PASS |
| ac_reads | `types_table_scan` | 688.98ms | 744.07ms | 1.1× | 1.3% | PASS |
| ac_reads | `table_scan` | 819.40ms | 845.67ms | 1.0× | 2.2% | PASS |
| ac_reads | `oltp_read_only` | 103.53ms | 92.90ms | 0.9× | 2.5% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 63.12ms | 158.58ms | 2.5× | 48.4% | PASS |
| ac_writes | `oltp_insert_ac` | 68.13ms | 189.50ms | 2.8× | 44.6% | PASS |
| ac_writes | `oltp_update_index_ac` | 60.11ms | 224.79ms | 3.7× | 40.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 59.18ms | 175.18ms | 3.0× | 56.4% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 58.64ms | 225.84ms | 3.9× | 46.2% | PASS |
| ac_writes | `oltp_write_only_ac` | 69.73ms | 180.62ms | 2.6× | 46.5% | PASS |
| ac_writes | `types_delete_insert_ac` | 58.55ms | 171.79ms | 2.9× | 44.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 57.84ms | 178.30ms | 3.1× | 36.5% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 18.73ms | 20.59ms | 1.1× | 2.5% | PASS |
| mem_reads | `oltp_range_select` | 8.05ms | 7.86ms | 1.0× | 1.9% | PASS |
| mem_reads | `oltp_sum_range` | 7.74ms | 7.83ms | 1.0× | 2.4% | PASS |
| mem_reads | `oltp_order_range` | 1.80ms | 1.80ms | 1.0× | 2.1% | PASS |
| mem_reads | `oltp_distinct_range` | 2.29ms | 2.33ms | 1.0× | 1.5% | PASS |
| mem_reads | `oltp_index_scan` | 2.17ms | 3.54ms | 1.6× | 2.2% | PASS |
| mem_reads | `select_random_points` | 12.05ms | 12.37ms | 1.0× | 3.3% | PASS |
| mem_reads | `select_random_ranges` | 3.45ms | 3.53ms | 1.0× | 4.4% | PASS |
| mem_reads | `covering_index_scan` | 3.82ms | 4.89ms | 1.3× | 1.1% | PASS |
| mem_reads | `groupby_scan` | 18.35ms | 18.74ms | 1.0× | 0.8% | PASS |
| mem_reads | `index_join` | 6.72ms | 5.24ms | 0.8× | 1.5% | PASS |
| mem_reads | `index_join_scan` | 1.99ms | 4.12ms | 2.1× | 1.4% | PASS |
| mem_reads | `types_table_scan` | 664.05ms | 723.80ms | 1.1× | 0.5% | PASS |
| mem_reads | `table_scan` | 756.54ms | 811.95ms | 1.1× | 0.6% | PASS |
| mem_reads | `oltp_read_only` | 68.72ms | 69.63ms | 1.0× | 0.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 136.84ms | 194.53ms | 1.4× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 10.40ms | 21.21ms | 2.0× | 0.5% | PASS |
| mem_writes | `oltp_update_index` | 36.63ms | 77.32ms | 2.1× | 1.0% | PASS |
| mem_writes | `oltp_update_non_index` | 27.51ms | 45.38ms | 1.7× | 1.7% | PASS |
| mem_writes | `oltp_delete_insert` | 29.85ms | 58.43ms | 2.0× | 1.2% | PASS |
| mem_writes | `oltp_write_only` | 15.97ms | 35.40ms | 2.2× | 1.1% | PASS |
| mem_writes | `types_delete_insert` | 21.78ms | 29.03ms | 1.3× | 3.2% | PASS |
| mem_writes | `oltp_read_write` | 48.78ms | 76.10ms | 1.6× | 1.3% | PASS |
| file_reads | `oltp_point_select` | 71.63ms | 35.33ms | 0.5× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 14.52ms | 9.56ms | 0.7× | 0.8% | PASS |
| file_reads | `oltp_sum_range` | 14.16ms | 9.54ms | 0.7× | 0.8% | PASS |
| file_reads | `oltp_order_range` | 2.62ms | 2.07ms | 0.8× | 1.6% | PASS |
| file_reads | `oltp_distinct_range` | 3.06ms | 2.57ms | 0.8× | 1.4% | PASS |
| file_reads | `oltp_index_scan` | 7.89ms | 5.30ms | 0.7× | 1.5% | PASS |
| file_reads | `select_random_points` | 18.79ms | 13.99ms | 0.7× | 0.8% | PASS |
| file_reads | `select_random_ranges` | 9.49ms | 5.18ms | 0.5× | 0.9% | PASS |
| file_reads | `covering_index_scan` | 9.49ms | 6.61ms | 0.7× | 1.8% | PASS |
| file_reads | `groupby_scan` | 19.58ms | 19.07ms | 1.0× | 0.9% | PASS |
| file_reads | `index_join` | 10.20ms | 7.06ms | 0.7× | 1.0% | PASS |
| file_reads | `index_join_scan` | 3.08ms | 4.31ms | 1.4× | 0.9% | PASS |
| file_reads | `types_table_scan` | 664.79ms | 732.57ms | 1.1× | 0.8% | PASS |
| file_reads | `table_scan` | 756.58ms | 811.84ms | 1.1× | 0.8% | PASS |
| file_reads | `oltp_read_only` | 143.61ms | 90.30ms | 0.6× | 0.7% | PASS |
| file_writes | `oltp_bulk_insert` | 207.31ms | 256.47ms | 1.2× | 9.1% | PASS |
| file_writes | `oltp_insert` | 19.19ms | 44.09ms | 2.3× | 11.2% | PASS |
| file_writes | `oltp_update_index` | 140.33ms | 166.72ms | 1.2× | 7.1% | PASS |
| file_writes | `oltp_update_non_index` | 118.92ms | 102.43ms | 0.9× | 8.7% | PASS |
| file_writes | `oltp_delete_insert` | 147.96ms | 126.44ms | 0.9× | 13.4% | PASS |
| file_writes | `oltp_write_only` | 100.47ms | 90.47ms | 0.9× | 3.6% | PASS |
| file_writes | `types_delete_insert` | 102.56ms | 67.46ms | 0.7× | 9.5% | PASS |
| file_writes | `oltp_read_write` | 140.54ms | 121.61ms | 0.9× | 4.1% | PASS |
| ac_reads | `oltp_point_select` | 36.72ms | 35.33ms | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_range_select` | 10.81ms | 9.47ms | 0.9× | 1.3% | PASS |
| ac_reads | `oltp_sum_range` | 10.24ms | 9.50ms | 0.9× | 0.8% | PASS |
| ac_reads | `oltp_order_range` | 2.14ms | 2.09ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_distinct_range` | 2.60ms | 2.58ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_index_scan` | 4.14ms | 5.26ms | 1.3× | 1.9% | PASS |
| ac_reads | `select_random_points` | 14.53ms | 13.96ms | 1.0× | 0.6% | PASS |
| ac_reads | `select_random_ranges` | 5.60ms | 5.10ms | 0.9× | 1.5% | PASS |
| ac_reads | `covering_index_scan` | 5.64ms | 6.62ms | 1.2× | 2.2% | PASS |
| ac_reads | `groupby_scan` | 18.77ms | 18.98ms | 1.0× | 0.8% | PASS |
| ac_reads | `index_join` | 7.83ms | 7.05ms | 0.9× | 1.7% | PASS |
| ac_reads | `index_join_scan` | 2.63ms | 4.33ms | 1.6× | 1.3% | PASS |
| ac_reads | `types_table_scan` | 662.64ms | 729.24ms | 1.1× | 0.5% | PASS |
| ac_reads | `table_scan` | 754.64ms | 811.07ms | 1.1× | 0.5% | PASS |
| ac_reads | `oltp_read_only` | 94.27ms | 90.35ms | 1.0× | 0.5% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 51.27ms | 121.86ms | 2.4× | 44.7% | PASS |
| ac_writes | `oltp_insert_ac` | 37.03ms | 109.06ms | 2.9× | 24.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 41.16ms | 116.78ms | 2.8× | 27.8% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 33.02ms | 94.37ms | 2.9× | 19.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 36.55ms | 107.73ms | 2.9× | 14.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 39.14ms | 115.57ms | 3.0× | 23.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 31.39ms | 89.86ms | 2.9× | 16.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 38.60ms | 111.02ms | 2.9× | 18.7% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 20.42ms | 24.56ms | 1.2× | 1.1% | PASS |
| mem_reads | `oltp_range_select` | 13.31ms | 14.13ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_sum_range` | 12.82ms | 13.99ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_order_range` | 2.48ms | 2.57ms | 1.0× | 2.0% | PASS |
| mem_reads | `oltp_distinct_range` | 3.06ms | 3.34ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_index_scan` | 3.07ms | 3.94ms | 1.3× | 2.9% | PASS |
| mem_reads | `select_random_points` | 19.71ms | 22.21ms | 1.1× | 1.3% | PASS |
| mem_reads | `select_random_ranges` | 4.84ms | 5.60ms | 1.2× | 1.1% | PASS |
| mem_reads | `covering_index_scan` | 4.39ms | 5.32ms | 1.2× | 0.7% | PASS |
| mem_reads | `groupby_scan` | 22.17ms | 28.45ms | 1.3× | 0.7% | PASS |
| mem_reads | `index_join` | 5.47ms | 7.17ms | 1.3× | 1.6% | PASS |
| mem_reads | `index_join_scan` | 2.83ms | 4.74ms | 1.7× | 1.5% | PASS |
| mem_reads | `types_table_scan` | 752.81ms | 828.99ms | 1.1× | 1.1% | PASS |
| mem_reads | `table_scan` | 880.35ms | 917.99ms | 1.0× | 2.6% | PASS |
| mem_reads | `oltp_read_only` | 90.81ms | 107.42ms | 1.2× | 0.9% | PASS |
| mem_writes | `oltp_bulk_insert` | 138.39ms | 187.27ms | 1.4× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 11.49ms | 20.07ms | 1.7× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 43.19ms | 79.28ms | 1.8× | 1.4% | PASS |
| mem_writes | `oltp_update_non_index` | 31.14ms | 48.83ms | 1.6× | 1.7% | PASS |
| mem_writes | `oltp_delete_insert` | 31.78ms | 59.60ms | 1.9× | 1.2% | PASS |
| mem_writes | `oltp_write_only` | 17.59ms | 34.68ms | 2.0× | 2.1% | PASS |
| mem_writes | `types_delete_insert` | 20.13ms | 31.28ms | 1.6× | 2.0% | PASS |
| mem_writes | `oltp_read_write` | 61.40ms | 97.33ms | 1.6× | 1.2% | PASS |
| file_reads | `oltp_point_select` | 41.59ms | 30.05ms | 0.7× | 0.7% | PASS |
| file_reads | `oltp_range_select` | 15.84ms | 15.08ms | 1.0× | 1.5% | PASS |
| file_reads | `oltp_sum_range` | 15.14ms | 14.77ms | 1.0× | 2.3% | PASS |
| file_reads | `oltp_order_range` | 2.78ms | 2.73ms | 1.0× | 2.5% | PASS |
| file_reads | `oltp_distinct_range` | 3.44ms | 3.56ms | 1.0× | 1.4% | PASS |
| file_reads | `oltp_index_scan` | 5.53ms | 5.05ms | 0.9× | 2.3% | PASS |
| file_reads | `select_random_points` | 22.30ms | 23.62ms | 1.1× | 1.0% | PASS |
| file_reads | `select_random_ranges` | 7.17ms | 6.46ms | 0.9× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 6.81ms | 6.30ms | 0.9× | 1.2% | PASS |
| file_reads | `groupby_scan` | 22.35ms | 28.21ms | 1.3× | 1.0% | PASS |
| file_reads | `index_join` | 6.83ms | 8.11ms | 1.2× | 2.2% | PASS |
| file_reads | `index_join_scan` | 3.17ms | 4.96ms | 1.6× | 4.6% | PASS |
| file_reads | `types_table_scan` | 754.98ms | 833.89ms | 1.1× | 0.8% | PASS |
| file_reads | `table_scan` | 886.43ms | 923.87ms | 1.0× | 3.5% | PASS |
| file_reads | `oltp_read_only` | 123.01ms | 115.94ms | 0.9× | 1.9% | PASS |
| file_writes | `oltp_bulk_insert` | 291.01ms | 315.95ms | 1.1× | 36.5% | PASS |
| file_writes | `oltp_insert` | 121.90ms | 69.58ms | 0.6× | 71.5% | PASS |
| file_writes | `oltp_update_index` | 179.24ms | 196.55ms | 1.1× | 36.3% | PASS |
| file_writes | `oltp_update_non_index` | 159.47ms | 145.95ms | 0.9× | 35.1% | PASS |
| file_writes | `oltp_delete_insert` | 219.08ms | 180.94ms | 0.8× | 43.0% | PASS |
| file_writes | `oltp_write_only` | 206.48ms | 119.33ms | 0.6× | 46.7% | PASS |
| file_writes | `types_delete_insert` | 106.67ms | 121.26ms | 1.1× | 35.4% | PASS |
| file_writes | `oltp_read_write` | 228.19ms | 192.12ms | 0.8× | 40.0% | PASS |
| ac_reads | `oltp_point_select` | 27.12ms | 29.82ms | 1.1× | 1.8% | PASS |
| ac_reads | `oltp_range_select` | 14.56ms | 15.08ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_sum_range` | 14.19ms | 14.96ms | 1.1× | 1.5% | PASS |
| ac_reads | `oltp_order_range` | 2.65ms | 2.76ms | 1.0× | 3.0% | PASS |
| ac_reads | `oltp_distinct_range` | 3.27ms | 3.54ms | 1.1× | 2.7% | PASS |
| ac_reads | `oltp_index_scan` | 4.01ms | 4.78ms | 1.2× | 4.0% | PASS |
| ac_reads | `select_random_points` | 20.50ms | 23.04ms | 1.1× | 1.7% | PASS |
| ac_reads | `select_random_ranges` | 5.66ms | 6.34ms | 1.1× | 2.9% | PASS |
| ac_reads | `covering_index_scan` | 5.37ms | 6.25ms | 1.2× | 1.1% | PASS |
| ac_reads | `groupby_scan` | 22.36ms | 28.37ms | 1.3× | 1.7% | PASS |
| ac_reads | `index_join` | 6.23ms | 8.11ms | 1.3× | 3.0% | PASS |
| ac_reads | `index_join_scan` | 3.08ms | 4.92ms | 1.6× | 4.1% | PASS |
| ac_reads | `types_table_scan` | 751.92ms | 835.97ms | 1.1× | 1.0% | PASS |
| ac_reads | `table_scan` | 891.22ms | 923.48ms | 1.0× | 3.6% | PASS |
| ac_reads | `oltp_read_only` | 101.80ms | 115.32ms | 1.1× | 1.4% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 266.77ms | 650.93ms | 2.4× | 43.9% | PASS |
| ac_writes | `oltp_insert_ac` | 304.52ms | 800.25ms | 2.6× | 61.8% | PASS |
| ac_writes | `oltp_update_index_ac` | 344.59ms | 724.11ms | 2.1× | 63.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 231.32ms | 543.12ms | 2.3× | 49.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 187.91ms | 441.78ms | 2.4× | 45.4% | PASS |
| ac_writes | `oltp_write_only_ac` | 209.66ms | 769.80ms | 3.7× | 55.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 250.76ms | 549.28ms | 2.2× | 43.2% | PASS |
| ac_writes | `oltp_read_write_ac` | 243.45ms | 661.17ms | 2.7× | 65.9% | PASS |

</details>

</details>

## Version-control latency

Wall time: 4m 8s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 27.03ms | 130.00ms | 20.8% | 0.8% | PASS |
| `status_dirty_many_tables` | 29.90ms | 130.00ms | 23.0% | 0.6% | PASS |
| `diff_regular_working_one_table` | 22.84ms | 120.00ms | 19.0% | 0.9% | PASS |
| `diff_regular_working_many_tables` | 33.74ms | 140.00ms | 24.1% | 0.7% | PASS |
| `diff_stat_working_many_tables` | 33.78ms | 140.00ms | 24.1% | 0.5% | PASS |
| `diff_schema_working_many_tables` | 34.33ms | 140.00ms | 24.5% | 0.5% | PASS |
| `branch_list_many_branches` | 16.32ms | 35.00ms | 46.6% | 0.4% | PASS |
| `branch_create_delete` | 25.68ms | 40.00ms | 64.2% | 1.2% | PASS |
| `at_literal_deep_history` | 19.73ms | 100.00ms | 19.7% | 0.5% | PASS |
| `diff_literal_deep_history` | 19.74ms | 120.00ms | 16.4% | 0.5% | PASS |
| `history_literal_deep_history` | 20.78ms | 150.00ms | 13.9% | 0.4% | PASS |
| `checkout_branch_clean` | 83.79ms | 150.00ms | 55.9% | 0.4% | PASS |
| `merge_data_no_conflicts` | 30.65ms | 50.00ms | 61.3% | 0.8% | PASS |
| `merge_data_secondary_index` | 1.47s | 2.50s | 59.0% | 0.4% | PASS |
| `merge_schema_no_conflicts` | 17.11ms | 35.00ms | 48.9% | 1.2% | PASS |
| `merge_data_conflicts` | 21.94ms | 180.00ms | 12.2% | 0.5% | PASS |
| `merge_data_conflicts_with_resolve` | 22.70ms | 180.00ms | 12.6% | 0.3% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
