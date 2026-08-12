# DoltLite Performance Report

> Nightly result: **FAIL**
>
> Generated: 2026-08-12 11:23 UTC
>
> Commit: [`6d04aeea7a2d8ec64d254de9de229873c76857d2`](https://github.com/dolthub/doltlite/commit/6d04aeea7a2d8ec64d254de9de229873c76857d2)
>
> Runner: ubuntu24 20260810.271.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/31584663956)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.71s | 11.19s | 1.0× | 1.5% | **PASS** |
| Writes | 2.19s | 3.74s | 1.7× | 1.1% | **FAIL** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 11.72s | 11.48s | 1.0× | 1.5% | **PASS** |
| Writes | 3.04s | 4.11s | 1.4× | 1.7% | **PASS** |
| Autocommit writes | 700.13ms | 2.59s | 3.7× | 5.7% | **PASS** |

The absolute ceiling is 2.4× per ordinary workload and 1.95× for a section average. Durable autocommit writes use 10.0× and 5.0× ceilings respectively.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.58s | 2.68s | 1.0× | 0.9% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.70s | 2.80s | 1.0× | 1.9% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.82s | 2.86s | 1.0× | 2.2% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.61s | 2.85s | 1.1× | 1.5% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 437.53ms | 686.33ms | 1.6× | 1.0% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 581.35ms | 1.04s | 1.8× | 1.1% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 578.70ms | 994.56ms | 1.7× | 1.8% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 593.40ms | 1.01s | 1.7× | 1.0% | **FAIL** |
| File-backed | Reads | int | 15 | 55 | 2.84s | 2.75s | 1.0× | 1.3% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.93s | 2.87s | 1.0× | 2.7% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 3.07s | 2.93s | 1.0× | 1.9% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.88s | 2.93s | 1.0× | 1.3% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 585.89ms | 758.52ms | 1.3× | 1.4% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 908.56ms | 1.17s | 1.3× | 4.3% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 779.08ms | 1.08s | 1.4× | 1.7% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 767.52ms | 1.11s | 1.4× | 1.7% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.72s | 2.77s | 1.0× | 1.3% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.71s | 2.85s | 1.1× | 1.9% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.87s | 2.92s | 1.0× | 1.3% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.68s | 2.92s | 1.1× | 1.4% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 146.13ms | 576.04ms | 3.9× | 5.8% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 215.55ms | 748.37ms | 3.5× | 6.7% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 149.13ms | 583.72ms | 3.9× | 4.4% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 189.32ms | 678.63ms | 3.6× | 4.5% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.19ms | 26.76ms | 1.1× | 0.7% | PASS |
| mem_reads | `oltp_range_select` | 10.43ms | 10.75ms | 1.0× | 1.4% | PASS |
| mem_reads | `oltp_sum_range` | 9.63ms | 10.76ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_order_range` | 2.62ms | 2.78ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 3.70ms | 3.85ms | 1.0× | 0.5% | PASS |
| mem_reads | `oltp_index_scan` | 3.94ms | 4.65ms | 1.2× | 1.3% | PASS |
| mem_reads | `select_random_points` | 10.35ms | 10.56ms | 1.0× | 1.0% | PASS |
| mem_reads | `select_random_ranges` | 3.07ms | 3.84ms | 1.3× | 1.2% | PASS |
| mem_reads | `covering_index_scan` | 4.33ms | 4.01ms | 0.9× | 0.7% | PASS |
| mem_reads | `groupby_scan` | 31.73ms | 34.14ms | 1.1× | 0.5% | PASS |
| mem_reads | `index_join` | 5.93ms | 7.36ms | 1.2× | 0.9% | PASS |
| mem_reads | `index_join_scan` | 3.49ms | 4.51ms | 1.3× | 1.5% | PASS |
| mem_reads | `types_table_scan` | 1.11s | 1.16s | 1.0× | 0.6% | PASS |
| mem_reads | `table_scan` | 1.25s | 1.28s | 1.0× | 0.4% | PASS |
| mem_reads | `oltp_read_only` | 102.58ms | 111.06ms | 1.1× | 0.7% | PASS |
| mem_writes | `oltp_bulk_insert` | 180.99ms | 243.53ms | 1.3× | 1.7% | PASS |
| mem_writes | `oltp_insert` | 15.74ms | 27.57ms | 1.8× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 50.90ms | 101.69ms | 2.0× | 0.8% | PASS |
| mem_writes | `oltp_update_non_index` | 34.68ms | 54.71ms | 1.6× | 1.1% | PASS |
| mem_writes | `oltp_delete_insert` | 44.53ms | 76.03ms | 1.7× | 0.8% | PASS |
| mem_writes | `oltp_write_only` | 21.77ms | 42.88ms | 2.0× | 1.0% | PASS |
| mem_writes | `types_delete_insert` | 24.50ms | 37.35ms | 1.5× | 1.3% | PASS |
| mem_writes | `oltp_read_write` | 64.42ms | 102.57ms | 1.6× | 0.7% | PASS |
| file_reads | `oltp_point_select` | 106.94ms | 48.14ms | 0.5× | 0.6% | PASS |
| file_reads | `oltp_range_select` | 18.41ms | 13.00ms | 0.7× | 1.8% | PASS |
| file_reads | `oltp_sum_range` | 17.63ms | 13.16ms | 0.7× | 1.1% | PASS |
| file_reads | `oltp_order_range` | 3.43ms | 3.06ms | 0.9× | 2.2% | PASS |
| file_reads | `oltp_distinct_range` | 4.46ms | 4.14ms | 0.9× | 1.0% | PASS |
| file_reads | `oltp_index_scan` | 12.21ms | 7.09ms | 0.6× | 2.0% | PASS |
| file_reads | `select_random_points` | 18.11ms | 13.20ms | 0.7× | 1.4% | PASS |
| file_reads | `select_random_ranges` | 11.15ms | 6.05ms | 0.5× | 1.7% | PASS |
| file_reads | `covering_index_scan` | 12.73ms | 6.33ms | 0.5× | 1.4% | PASS |
| file_reads | `groupby_scan` | 32.45ms | 34.50ms | 1.1× | 0.8% | PASS |
| file_reads | `index_join` | 10.41ms | 9.10ms | 0.9× | 1.3% | PASS |
| file_reads | `index_join_scan` | 4.40ms | 4.88ms | 1.1× | 1.5% | PASS |
| file_reads | `types_table_scan` | 1.10s | 1.16s | 1.1× | 0.5% | PASS |
| file_reads | `table_scan` | 1.26s | 1.29s | 1.0× | 0.6% | PASS |
| file_reads | `oltp_read_only` | 222.02ms | 141.67ms | 0.6× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 193.99ms | 251.22ms | 1.3× | 1.6% | PASS |
| file_writes | `oltp_insert` | 21.80ms | 31.89ms | 1.5× | 1.2% | PASS |
| file_writes | `oltp_update_index` | 77.96ms | 113.37ms | 1.5× | 1.2% | PASS |
| file_writes | `oltp_update_non_index` | 57.59ms | 68.02ms | 1.2× | 1.8% | PASS |
| file_writes | `oltp_delete_insert` | 66.78ms | 84.96ms | 1.3× | 1.5% | PASS |
| file_writes | `oltp_write_only` | 42.56ms | 52.95ms | 1.2× | 2.1% | PASS |
| file_writes | `types_delete_insert` | 39.37ms | 43.23ms | 1.1× | 1.2% | PASS |
| file_writes | `oltp_read_write` | 85.84ms | 112.88ms | 1.3× | 1.1% | PASS |
| ac_reads | `oltp_point_select` | 52.21ms | 48.34ms | 0.9× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 14.05ms | 13.05ms | 0.9× | 1.5% | PASS |
| ac_reads | `oltp_sum_range` | 12.95ms | 13.24ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_order_range` | 3.15ms | 3.10ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_distinct_range` | 4.07ms | 4.14ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_index_scan` | 7.20ms | 7.30ms | 1.0× | 1.7% | PASS |
| ac_reads | `select_random_points` | 14.00ms | 13.38ms | 1.0× | 2.3% | PASS |
| ac_reads | `select_random_ranges` | 6.05ms | 6.08ms | 1.0× | 1.5% | PASS |
| ac_reads | `covering_index_scan` | 7.45ms | 6.50ms | 0.9× | 1.3% | PASS |
| ac_reads | `groupby_scan` | 32.41ms | 34.54ms | 1.1× | 0.7% | PASS |
| ac_reads | `index_join` | 7.78ms | 9.18ms | 1.2× | 1.4% | PASS |
| ac_reads | `index_join_scan` | 4.01ms | 4.98ms | 1.2× | 1.6% | PASS |
| ac_reads | `types_table_scan` | 1.11s | 1.17s | 1.1× | 0.7% | PASS |
| ac_reads | `table_scan` | 1.30s | 1.30s | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_read_only` | 145.28ms | 142.53ms | 1.0× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 15.88ms | 56.01ms | 3.5× | 5.9% | PASS |
| ac_writes | `oltp_insert_ac` | 17.70ms | 69.69ms | 3.9× | 4.6% | PASS |
| ac_writes | `oltp_update_index_ac` | 20.28ms | 84.56ms | 4.2× | 5.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 16.62ms | 65.49ms | 3.9× | 6.7% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 17.80ms | 74.00ms | 4.2× | 3.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 17.83ms | 75.44ms | 4.2× | 5.7% | PASS |
| ac_writes | `types_delete_insert_ac` | 16.04ms | 68.18ms | 4.3× | 7.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 24.00ms | 82.69ms | 3.4× | 6.5% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 31.90ms | 38.22ms | 1.2× | 1.9% | PASS |
| mem_reads | `oltp_range_select` | 14.98ms | 14.13ms | 0.9× | 3.5% | PASS |
| mem_reads | `oltp_sum_range` | 13.20ms | 13.87ms | 1.1× | 1.9% | PASS |
| mem_reads | `oltp_order_range` | 3.12ms | 3.20ms | 1.0× | 1.4% | PASS |
| mem_reads | `oltp_distinct_range` | 4.24ms | 4.34ms | 1.0× | 1.5% | PASS |
| mem_reads | `oltp_index_scan` | 5.22ms | 6.65ms | 1.3× | 3.5% | PASS |
| mem_reads | `select_random_points` | 21.05ms | 23.17ms | 1.1× | 2.8% | PASS |
| mem_reads | `select_random_ranges` | 4.32ms | 5.35ms | 1.2× | 1.8% | PASS |
| mem_reads | `covering_index_scan` | 5.49ms | 4.89ms | 0.9× | 4.5% | PASS |
| mem_reads | `groupby_scan` | 33.22ms | 34.48ms | 1.0× | 0.9% | PASS |
| mem_reads | `index_join` | 7.24ms | 9.59ms | 1.3× | 2.4% | PASS |
| mem_reads | `index_join_scan` | 5.10ms | 5.55ms | 1.1× | 3.4% | PASS |
| mem_reads | `types_table_scan` | 1.11s | 1.18s | 1.1× | 0.7% | PASS |
| mem_reads | `table_scan` | 1.33s | 1.32s | 1.0× | 1.3% | PASS |
| mem_reads | `oltp_read_only` | 122.72ms | 136.60ms | 1.1× | 2.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 235.87ms | 353.04ms | 1.5× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 21.75ms | 38.22ms | 1.8× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 72.87ms | 166.89ms | 2.3× | 1.6% | PASS |
| mem_writes | `oltp_update_non_index` | 48.33ms | 96.46ms | 2.0× | 0.6% | PASS |
| mem_writes | `oltp_delete_insert` | 50.64ms | 110.82ms | 2.2× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 29.10ms | 57.90ms | 2.0× | 1.3% | PASS |
| mem_writes | `types_delete_insert` | 33.51ms | 58.04ms | 1.7× | 1.3% | PASS |
| mem_writes | `oltp_read_write` | 89.29ms | 159.18ms | 1.8× | 3.1% | PASS |
| file_reads | `oltp_point_select` | 100.89ms | 57.05ms | 0.6× | 1.5% | PASS |
| file_reads | `oltp_range_select` | 21.62ms | 16.19ms | 0.7× | 3.9% | PASS |
| file_reads | `oltp_sum_range` | 20.38ms | 16.30ms | 0.8× | 3.1% | PASS |
| file_reads | `oltp_order_range` | 3.89ms | 3.52ms | 0.9× | 2.6% | PASS |
| file_reads | `oltp_distinct_range` | 5.01ms | 4.61ms | 0.9× | 2.7% | PASS |
| file_reads | `oltp_index_scan` | 11.48ms | 8.39ms | 0.7× | 2.8% | PASS |
| file_reads | `select_random_points` | 25.98ms | 24.59ms | 0.9× | 2.7% | PASS |
| file_reads | `select_random_ranges` | 11.34ms | 7.21ms | 0.6× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 12.19ms | 6.85ms | 0.6× | 4.8% | PASS |
| file_reads | `groupby_scan` | 33.55ms | 34.57ms | 1.0× | 1.0% | PASS |
| file_reads | `index_join` | 12.63ms | 11.17ms | 0.9× | 3.4% | PASS |
| file_reads | `index_join_scan` | 6.32ms | 6.15ms | 1.0× | 2.8% | PASS |
| file_reads | `types_table_scan` | 1.21s | 1.20s | 1.0× | 5.5% | PASS |
| file_reads | `table_scan` | 1.23s | 1.31s | 1.1× | 1.4% | PASS |
| file_reads | `oltp_read_only` | 225.05ms | 164.96ms | 0.7× | 1.8% | PASS |
| file_writes | `oltp_bulk_insert` | 254.79ms | 367.63ms | 1.4× | 1.0% | PASS |
| file_writes | `oltp_insert` | 46.87ms | 45.26ms | 1.0× | 18.7% | PASS |
| file_writes | `oltp_update_index` | 115.57ms | 187.02ms | 1.6× | 1.7% | PASS |
| file_writes | `oltp_update_non_index` | 92.52ms | 113.75ms | 1.2× | 6.6% | PASS |
| file_writes | `oltp_delete_insert` | 93.24ms | 129.69ms | 1.4× | 2.0% | PASS |
| file_writes | `oltp_write_only` | 83.56ms | 73.40ms | 0.9× | 12.1% | PASS |
| file_writes | `types_delete_insert` | 58.67ms | 70.23ms | 1.2× | 1.8% | PASS |
| file_writes | `oltp_read_write` | 163.35ms | 179.94ms | 1.1× | 7.9% | PASS |
| ac_reads | `oltp_point_select` | 56.12ms | 57.78ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_range_select` | 19.30ms | 16.50ms | 0.9× | 2.5% | PASS |
| ac_reads | `oltp_sum_range` | 16.05ms | 16.15ms | 1.0× | 3.7% | PASS |
| ac_reads | `oltp_order_range` | 3.65ms | 3.67ms | 1.0× | 3.0% | PASS |
| ac_reads | `oltp_distinct_range` | 4.60ms | 4.70ms | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_index_scan` | 7.12ms | 8.53ms | 1.2× | 1.7% | PASS |
| ac_reads | `select_random_points` | 21.62ms | 24.54ms | 1.1× | 2.2% | PASS |
| ac_reads | `select_random_ranges` | 6.49ms | 7.21ms | 1.1× | 1.1% | PASS |
| ac_reads | `covering_index_scan` | 8.31ms | 6.79ms | 0.8× | 7.5% | PASS |
| ac_reads | `groupby_scan` | 32.91ms | 34.55ms | 1.0× | 1.0% | PASS |
| ac_reads | `index_join` | 9.46ms | 11.19ms | 1.2× | 2.8% | PASS |
| ac_reads | `index_join_scan` | 5.58ms | 6.06ms | 1.1× | 4.2% | PASS |
| ac_reads | `types_table_scan` | 1.09s | 1.17s | 1.1× | 1.1% | PASS |
| ac_reads | `table_scan` | 1.27s | 1.32s | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_read_only` | 155.97ms | 165.29ms | 1.1× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.65ms | 76.87ms | 3.4× | 6.8% | PASS |
| ac_writes | `oltp_insert_ac` | 27.38ms | 89.01ms | 3.3× | 6.8% | PASS |
| ac_writes | `oltp_update_index_ac` | 30.37ms | 107.64ms | 3.5× | 4.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 25.12ms | 88.78ms | 3.5× | 6.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.61ms | 96.28ms | 3.6× | 7.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.42ms | 96.01ms | 3.6× | 5.5% | PASS |
| ac_writes | `types_delete_insert_ac` | 23.57ms | 90.91ms | 3.9× | 5.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 33.42ms | 102.87ms | 3.1× | 6.9% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 30.51ms | 34.40ms | 1.1× | 2.0% | PASS |
| mem_reads | `oltp_range_select` | 13.77ms | 13.22ms | 1.0× | 2.3% | PASS |
| mem_reads | `oltp_sum_range` | 12.46ms | 13.07ms | 1.0× | 2.0% | PASS |
| mem_reads | `oltp_order_range` | 3.07ms | 3.15ms | 1.0× | 1.3% | PASS |
| mem_reads | `oltp_distinct_range` | 4.14ms | 4.20ms | 1.0× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 4.65ms | 5.75ms | 1.2× | 2.2% | PASS |
| mem_reads | `select_random_points` | 18.00ms | 19.96ms | 1.1× | 2.7% | PASS |
| mem_reads | `select_random_ranges` | 4.26ms | 5.20ms | 1.2× | 1.8% | PASS |
| mem_reads | `covering_index_scan` | 4.61ms | 4.52ms | 1.0× | 2.8% | PASS |
| mem_reads | `groupby_scan` | 34.26ms | 35.56ms | 1.0× | 0.8% | PASS |
| mem_reads | `index_join` | 6.99ms | 9.08ms | 1.3× | 2.8% | PASS |
| mem_reads | `index_join_scan` | 4.51ms | 5.71ms | 1.3× | 4.1% | PASS |
| mem_reads | `types_table_scan` | 1.18s | 1.23s | 1.0× | 2.5% | PASS |
| mem_reads | `table_scan` | 1.38s | 1.35s | 1.0× | 2.7% | PASS |
| mem_reads | `oltp_read_only` | 121.18ms | 128.85ms | 1.1× | 1.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 238.07ms | 339.12ms | 1.4× | 1.6% | PASS |
| mem_writes | `oltp_insert` | 20.59ms | 38.65ms | 1.9× | 1.1% | PASS |
| mem_writes | `oltp_update_index` | 71.23ms | 156.64ms | 2.2× | 2.0% | PASS |
| mem_writes | `oltp_update_non_index` | 50.66ms | 93.21ms | 1.8× | 1.9% | PASS |
| mem_writes | `oltp_delete_insert` | 51.33ms | 114.64ms | 2.2× | 1.7% | PASS |
| mem_writes | `oltp_write_only` | 29.20ms | 59.89ms | 2.1× | 1.6% | PASS |
| mem_writes | `types_delete_insert` | 32.89ms | 56.80ms | 1.7× | 1.9% | PASS |
| mem_writes | `oltp_read_write` | 84.73ms | 135.60ms | 1.6× | 1.9% | PASS |
| file_reads | `oltp_point_select` | 114.45ms | 55.75ms | 0.5× | 1.1% | PASS |
| file_reads | `oltp_range_select` | 22.06ms | 15.49ms | 0.7× | 2.7% | PASS |
| file_reads | `oltp_sum_range` | 20.90ms | 15.38ms | 0.7× | 1.9% | PASS |
| file_reads | `oltp_order_range` | 3.87ms | 3.49ms | 0.9× | 2.3% | PASS |
| file_reads | `oltp_distinct_range` | 4.93ms | 4.50ms | 0.9× | 1.5% | PASS |
| file_reads | `oltp_index_scan` | 13.23ms | 8.15ms | 0.6× | 1.2% | PASS |
| file_reads | `select_random_points` | 27.07ms | 22.35ms | 0.8× | 2.5% | PASS |
| file_reads | `select_random_ranges` | 12.60ms | 7.38ms | 0.6× | 1.5% | PASS |
| file_reads | `covering_index_scan` | 13.33ms | 6.87ms | 0.5× | 1.3% | PASS |
| file_reads | `groupby_scan` | 35.16ms | 35.80ms | 1.0× | 1.1% | PASS |
| file_reads | `index_join` | 11.62ms | 10.75ms | 0.9× | 2.1% | PASS |
| file_reads | `index_join_scan` | 5.41ms | 6.05ms | 1.1× | 4.0% | PASS |
| file_reads | `types_table_scan` | 1.18s | 1.23s | 1.0× | 1.9% | PASS |
| file_reads | `table_scan` | 1.36s | 1.34s | 1.0× | 2.5% | PASS |
| file_reads | `oltp_read_only` | 242.56ms | 159.89ms | 0.7× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 260.28ms | 348.05ms | 1.3× | 1.6% | PASS |
| file_writes | `oltp_insert` | 31.34ms | 45.38ms | 1.4× | 2.4% | PASS |
| file_writes | `oltp_update_index` | 105.81ms | 172.88ms | 1.6× | 1.7% | PASS |
| file_writes | `oltp_update_non_index` | 79.40ms | 106.03ms | 1.3× | 1.7% | PASS |
| file_writes | `oltp_delete_insert` | 82.80ms | 126.77ms | 1.5× | 1.8% | PASS |
| file_writes | `oltp_write_only` | 54.75ms | 70.26ms | 1.3× | 2.4% | PASS |
| file_writes | `types_delete_insert` | 51.78ms | 64.96ms | 1.3× | 1.6% | PASS |
| file_writes | `oltp_read_write` | 112.91ms | 146.12ms | 1.3× | 2.4% | PASS |
| ac_reads | `oltp_point_select` | 59.73ms | 55.92ms | 0.9× | 1.3% | PASS |
| ac_reads | `oltp_range_select` | 17.38ms | 15.50ms | 0.9× | 1.9% | PASS |
| ac_reads | `oltp_sum_range` | 16.05ms | 15.31ms | 1.0× | 2.0% | PASS |
| ac_reads | `oltp_order_range` | 3.56ms | 3.51ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_distinct_range` | 4.58ms | 4.52ms | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_index_scan` | 8.13ms | 8.17ms | 1.0× | 1.1% | PASS |
| ac_reads | `select_random_points` | 22.05ms | 22.25ms | 1.0× | 1.6% | PASS |
| ac_reads | `select_random_ranges` | 7.34ms | 7.40ms | 1.0× | 1.1% | PASS |
| ac_reads | `covering_index_scan` | 8.49ms | 6.87ms | 0.8× | 2.2% | PASS |
| ac_reads | `groupby_scan` | 34.78ms | 35.80ms | 1.0× | 1.1% | PASS |
| ac_reads | `index_join` | 9.40ms | 10.96ms | 1.2× | 2.5% | PASS |
| ac_reads | `index_join_scan` | 5.15ms | 6.11ms | 1.2× | 4.4% | PASS |
| ac_reads | `types_table_scan` | 1.15s | 1.22s | 1.1× | 1.3% | PASS |
| ac_reads | `table_scan` | 1.37s | 1.34s | 1.0× | 2.0% | PASS |
| ac_reads | `oltp_read_only` | 164.72ms | 159.98ms | 1.0× | 1.3% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 15.65ms | 55.93ms | 3.6× | 3.3% | PASS |
| ac_writes | `oltp_insert_ac` | 18.16ms | 75.41ms | 4.2× | 4.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 20.50ms | 83.96ms | 4.1× | 5.0% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 16.57ms | 66.93ms | 4.0× | 4.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 18.53ms | 76.40ms | 4.1× | 5.9% | PASS |
| ac_writes | `oltp_write_only_ac` | 18.13ms | 73.78ms | 4.1× | 3.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 16.66ms | 69.10ms | 4.1× | 7.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 24.93ms | 82.21ms | 3.3× | 3.4% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 32.92ms | 39.80ms | 1.2× | 2.3% | PASS |
| mem_reads | `oltp_range_select` | 20.20ms | 21.45ms | 1.1× | 1.9% | PASS |
| mem_reads | `oltp_sum_range` | 18.30ms | 21.14ms | 1.2× | 1.4% | PASS |
| mem_reads | `oltp_order_range` | 3.60ms | 3.91ms | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_distinct_range` | 4.60ms | 5.02ms | 1.1× | 2.3% | PASS |
| mem_reads | `oltp_index_scan` | 4.43ms | 5.71ms | 1.3× | 1.6% | PASS |
| mem_reads | `select_random_points` | 26.38ms | 31.42ms | 1.2× | 0.9% | PASS |
| mem_reads | `select_random_ranges` | 7.68ms | 8.84ms | 1.2× | 2.4% | PASS |
| mem_reads | `covering_index_scan` | 4.23ms | 4.01ms | 0.9× | 1.0% | PASS |
| mem_reads | `groupby_scan` | 35.61ms | 39.61ms | 1.1× | 0.5% | PASS |
| mem_reads | `index_join` | 7.98ms | 10.03ms | 1.3× | 1.2% | PASS |
| mem_reads | `index_join_scan` | 3.83ms | 5.18ms | 1.4× | 1.8% | PASS |
| mem_reads | `types_table_scan` | 1.08s | 1.16s | 1.1× | 0.5% | PASS |
| mem_reads | `table_scan` | 1.21s | 1.33s | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_read_only` | 149.25ms | 171.47ms | 1.1× | 1.9% | PASS |
| mem_writes | `oltp_bulk_insert` | 248.19ms | 346.48ms | 1.4× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 19.24ms | 34.90ms | 1.8× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 66.06ms | 138.89ms | 2.1× | 1.0% | PASS |
| mem_writes | `oltp_update_non_index` | 49.05ms | 91.98ms | 1.9× | 1.1% | PASS |
| mem_writes | `oltp_delete_insert` | 48.62ms | 101.76ms | 2.1× | 0.9% | PASS |
| mem_writes | `oltp_write_only` | 26.24ms | 69.89ms | 2.7× | 1.1% | FAIL |
| mem_writes | `types_delete_insert` | 32.80ms | 56.82ms | 1.7× | 1.2% | PASS |
| mem_writes | `oltp_read_write` | 103.21ms | 173.01ms | 1.7× | 1.1% | PASS |
| file_reads | `oltp_point_select` | 103.74ms | 59.08ms | 0.6× | 1.1% | PASS |
| file_reads | `oltp_range_select` | 26.58ms | 23.52ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_sum_range` | 25.28ms | 23.35ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_order_range` | 4.40ms | 4.18ms | 1.0× | 1.9% | PASS |
| file_reads | `oltp_distinct_range` | 5.53ms | 5.32ms | 1.0× | 0.9% | PASS |
| file_reads | `oltp_index_scan` | 11.99ms | 8.29ms | 0.7× | 1.3% | PASS |
| file_reads | `select_random_points` | 36.94ms | 35.35ms | 1.0× | 2.2% | PASS |
| file_reads | `select_random_ranges` | 15.05ms | 11.06ms | 0.7× | 1.7% | PASS |
| file_reads | `covering_index_scan` | 11.34ms | 6.51ms | 0.6× | 1.3% | PASS |
| file_reads | `groupby_scan` | 37.40ms | 40.35ms | 1.1× | 0.7% | PASS |
| file_reads | `index_join` | 12.35ms | 12.30ms | 1.0× | 1.2% | PASS |
| file_reads | `index_join_scan` | 5.21ms | 5.68ms | 1.1× | 2.7% | PASS |
| file_reads | `types_table_scan` | 1.07s | 1.15s | 1.1× | 0.4% | PASS |
| file_reads | `table_scan` | 1.26s | 1.33s | 1.1× | 1.1% | PASS |
| file_reads | `oltp_read_only` | 260.38ms | 201.50ms | 0.8× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 263.42ms | 356.61ms | 1.4× | 1.2% | PASS |
| file_writes | `oltp_insert` | 26.31ms | 40.83ms | 1.6× | 1.3% | PASS |
| file_writes | `oltp_update_index` | 95.11ms | 153.27ms | 1.6× | 1.7% | PASS |
| file_writes | `oltp_update_non_index` | 75.87ms | 106.94ms | 1.4× | 1.7% | PASS |
| file_writes | `oltp_delete_insert` | 75.83ms | 115.08ms | 1.5× | 1.8% | PASS |
| file_writes | `oltp_write_only` | 50.93ms | 82.64ms | 1.6× | 2.6% | PASS |
| file_writes | `types_delete_insert` | 49.68ms | 64.03ms | 1.3× | 1.6% | PASS |
| file_writes | `oltp_read_write` | 130.38ms | 185.97ms | 1.4× | 1.7% | PASS |
| ac_reads | `oltp_point_select` | 57.08ms | 58.98ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_range_select` | 21.95ms | 23.61ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_sum_range` | 20.24ms | 23.27ms | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_order_range` | 3.81ms | 4.16ms | 1.1× | 1.6% | PASS |
| ac_reads | `oltp_distinct_range` | 4.96ms | 5.31ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_index_scan` | 7.07ms | 8.19ms | 1.2× | 1.8% | PASS |
| ac_reads | `select_random_points` | 30.57ms | 35.38ms | 1.2× | 1.7% | PASS |
| ac_reads | `select_random_ranges` | 10.09ms | 10.95ms | 1.1× | 2.0% | PASS |
| ac_reads | `covering_index_scan` | 6.59ms | 6.29ms | 1.0× | 1.5% | PASS |
| ac_reads | `groupby_scan` | 35.86ms | 40.10ms | 1.1× | 0.6% | PASS |
| ac_reads | `index_join` | 9.76ms | 11.99ms | 1.2× | 1.3% | PASS |
| ac_reads | `index_join_scan` | 4.61ms | 5.68ms | 1.2× | 2.4% | PASS |
| ac_reads | `types_table_scan` | 1.08s | 1.16s | 1.1× | 0.7% | PASS |
| ac_reads | `table_scan` | 1.20s | 1.33s | 1.1× | 0.7% | PASS |
| ac_reads | `oltp_read_only` | 182.58ms | 199.38ms | 1.1× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 20.87ms | 69.98ms | 3.4× | 5.9% | PASS |
| ac_writes | `oltp_insert_ac` | 22.91ms | 84.86ms | 3.7× | 3.6% | PASS |
| ac_writes | `oltp_update_index_ac` | 25.30ms | 95.19ms | 3.8× | 4.9% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.83ms | 79.30ms | 3.6× | 4.1% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 22.62ms | 87.39ms | 3.9× | 3.2% | PASS |
| ac_writes | `oltp_write_only_ac` | 24.70ms | 87.54ms | 3.5× | 6.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 21.80ms | 79.08ms | 3.6× | 7.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 29.26ms | 95.29ms | 3.3× | 3.5% | PASS |

</details>

</details>

## Version-control latency

Wall time: 2m 25s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 88.00ms | 130.00ms | 67.7% | 0.2% | PASS |
| `status_dirty_many_tables` | 91.57ms | 130.00ms | 70.4% | 0.4% | PASS |
| `diff_regular_working_one_table` | 85.70ms | 120.00ms | 71.4% | 0.3% | PASS |
| `diff_regular_working_many_tables` | 96.32ms | 140.00ms | 68.8% | 0.3% | PASS |
| `diff_stat_working_many_tables` | 96.14ms | 140.00ms | 68.7% | 0.3% | PASS |
| `diff_schema_working_many_tables` | 96.84ms | 140.00ms | 69.2% | 0.2% | PASS |
| `branch_list_many_branches` | 23.39ms | 35.00ms | 66.8% | 0.4% | PASS |
| `branch_create_delete` | 25.38ms | 40.00ms | 63.5% | 0.7% | PASS |
| `checkout_branch_clean` | 56.31ms | 150.00ms | 37.5% | 0.4% | PASS |
| `merge_data_no_conflicts` | 29.90ms | 50.00ms | 59.8% | 0.6% | PASS |
| `merge_schema_no_conflicts` | 22.38ms | 35.00ms | 63.9% | 0.9% | PASS |
| `merge_data_conflicts` | 129.33ms | 180.00ms | 71.8% | 0.1% | PASS |
| `merge_data_conflicts_with_resolve` | 129.39ms | 180.00ms | 71.9% | 0.1% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
