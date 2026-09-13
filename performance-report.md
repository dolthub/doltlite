# DoltLite Performance Report

> Nightly result: **FAIL**
>
> Generated: 2026-09-13 11:29 UTC
>
> Commit: [`2f6dee8ce0d3eaaa0c190ef42f82c1c723eca635`](https://github.com/dolthub/doltlite/commit/2f6dee8ce0d3eaaa0c190ef42f82c1c723eca635)
>
> Runner: ubuntu24 20260907.300.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/34750749556)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.40s | 9.76s | 1.0× | 1.8% | **PASS** |
| Writes | 1.83s | 3.01s | 1.6× | 1.6% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.73s | 9.71s | 1.0× | 1.7% | **PASS** |
| Writes | 3.70s | 4.11s | 1.1× | 1.9% | **FAIL** |
| Autocommit writes | 1.24s | 4.06s | 3.3× | 14.2% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.50s | 2.63s | 1.1× | 1.3% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 1.97s | 1.90s | 1.0× | 2.5% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.10s | 2.30s | 1.1× | 2.5% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.84s | 2.93s | 1.0× | 1.5% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 442.29ms | 746.09ms | 1.7× | 1.2% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 371.75ms | 599.50ms | 1.6× | 2.5% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 413.97ms | 674.58ms | 1.6× | 2.9% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 601.42ms | 988.73ms | 1.6× | 0.9% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.66s | 2.66s | 1.0× | 1.1% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 1.83s | 1.76s | 1.0× | 2.2% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.19s | 2.34s | 1.1× | 3.3% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 3.05s | 2.96s | 1.0× | 1.2% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 587.92ms | 798.25ms | 1.4× | 1.7% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.25s | 1.11s | 0.9× | 12.5% | **FAIL** |
| File-backed | Writes | blobpk | 8 | 55 | 1.12s | 1.16s | 1.0× | 10.0% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 746.34ms | 1.05s | 1.4× | 1.1% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.51s | 2.65s | 1.1× | 1.2% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 1.77s | 1.79s | 1.0× | 1.9% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.10s | 2.30s | 1.1× | 2.7% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.70s | 2.93s | 1.1× | 1.3% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 196.09ms | 712.16ms | 3.6× | 5.6% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 461.36ms | 1.45s | 3.1× | 46.0% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 377.49ms | 1.17s | 3.1× | 39.0% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 207.15ms | 729.64ms | 3.5× | 5.6% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 23.70ms | 30.10ms | 1.3× | 1.2% | PASS |
| mem_reads | `oltp_range_select` | 10.27ms | 11.31ms | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_sum_range` | 9.37ms | 11.12ms | 1.2× | 1.4% | PASS |
| mem_reads | `oltp_order_range` | 2.51ms | 2.77ms | 1.1× | 2.2% | PASS |
| mem_reads | `oltp_distinct_range` | 3.54ms | 3.84ms | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_index_scan` | 3.71ms | 5.16ms | 1.4× | 1.7% | PASS |
| mem_reads | `select_random_points` | 8.90ms | 10.77ms | 1.2× | 2.4% | PASS |
| mem_reads | `select_random_ranges` | 4.30ms | 3.98ms | 0.9× | 1.3% | PASS |
| mem_reads | `covering_index_scan` | 7.77ms | 4.20ms | 0.5× | 1.0% | PASS |
| mem_reads | `groupby_scan` | 29.06ms | 32.21ms | 1.1× | 1.1% | PASS |
| mem_reads | `index_join` | 5.80ms | 7.84ms | 1.4× | 1.2% | PASS |
| mem_reads | `index_join_scan` | 3.02ms | 4.49ms | 1.5× | 1.8% | PASS |
| mem_reads | `types_table_scan` | 1.04s | 1.13s | 1.1× | 0.5% | PASS |
| mem_reads | `table_scan` | 1.24s | 1.25s | 1.0× | 1.4% | PASS |
| mem_reads | `oltp_read_only` | 108.89ms | 120.81ms | 1.1× | 1.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 181.83ms | 278.20ms | 1.5× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 15.35ms | 28.56ms | 1.9× | 1.2% | PASS |
| mem_writes | `oltp_update_index` | 50.52ms | 99.51ms | 2.0× | 1.7% | PASS |
| mem_writes | `oltp_update_non_index` | 34.88ms | 61.78ms | 1.8× | 1.6% | PASS |
| mem_writes | `oltp_delete_insert` | 45.39ms | 75.87ms | 1.7× | 1.2% | PASS |
| mem_writes | `oltp_write_only` | 21.44ms | 47.21ms | 2.2× | 1.2% | PASS |
| mem_writes | `types_delete_insert` | 24.37ms | 39.14ms | 1.6× | 1.1% | PASS |
| mem_writes | `oltp_read_write` | 68.51ms | 115.82ms | 1.7× | 1.6% | PASS |
| file_reads | `oltp_point_select` | 92.82ms | 48.86ms | 0.5× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 17.55ms | 13.25ms | 0.8× | 1.2% | PASS |
| file_reads | `oltp_sum_range` | 16.62ms | 13.15ms | 0.8× | 1.3% | PASS |
| file_reads | `oltp_order_range` | 3.42ms | 3.05ms | 0.9× | 1.1% | PASS |
| file_reads | `oltp_distinct_range` | 4.57ms | 4.15ms | 0.9× | 2.2% | PASS |
| file_reads | `oltp_index_scan` | 11.21ms | 7.51ms | 0.7× | 2.1% | PASS |
| file_reads | `select_random_points` | 17.53ms | 13.20ms | 0.8× | 1.7% | PASS |
| file_reads | `select_random_ranges` | 11.94ms | 5.98ms | 0.5× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 15.56ms | 6.59ms | 0.4× | 1.1% | PASS |
| file_reads | `groupby_scan` | 30.36ms | 32.52ms | 1.1× | 1.1% | PASS |
| file_reads | `index_join` | 9.90ms | 9.54ms | 1.0× | 1.8% | PASS |
| file_reads | `index_join_scan` | 4.12ms | 4.88ms | 1.2× | 1.9% | PASS |
| file_reads | `types_table_scan` | 1.04s | 1.12s | 1.1× | 0.4% | PASS |
| file_reads | `table_scan` | 1.18s | 1.23s | 1.0× | 0.5% | PASS |
| file_reads | `oltp_read_only` | 202.73ms | 144.55ms | 0.7× | 0.7% | PASS |
| file_writes | `oltp_bulk_insert` | 195.66ms | 283.66ms | 1.4× | 0.9% | PASS |
| file_writes | `oltp_insert` | 21.95ms | 32.22ms | 1.5× | 1.7% | PASS |
| file_writes | `oltp_update_index` | 75.35ms | 105.87ms | 1.4× | 1.5% | PASS |
| file_writes | `oltp_update_non_index` | 56.34ms | 72.44ms | 1.3× | 1.6% | PASS |
| file_writes | `oltp_delete_insert` | 66.43ms | 83.13ms | 1.3× | 1.6% | PASS |
| file_writes | `oltp_write_only` | 43.10ms | 54.37ms | 1.3× | 1.8% | PASS |
| file_writes | `types_delete_insert` | 38.49ms | 44.69ms | 1.2× | 1.8% | PASS |
| file_writes | `oltp_read_write` | 90.59ms | 121.86ms | 1.3× | 1.8% | PASS |
| ac_reads | `oltp_point_select` | 46.06ms | 48.61ms | 1.1× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 12.70ms | 13.16ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_sum_range` | 11.78ms | 13.10ms | 1.1× | 1.5% | PASS |
| ac_reads | `oltp_order_range` | 2.85ms | 3.01ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_distinct_range` | 3.99ms | 4.12ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_index_scan` | 6.33ms | 7.39ms | 1.2× | 1.9% | PASS |
| ac_reads | `select_random_points` | 12.32ms | 13.02ms | 1.1× | 1.3% | PASS |
| ac_reads | `select_random_ranges` | 6.99ms | 5.96ms | 0.9× | 1.1% | PASS |
| ac_reads | `covering_index_scan` | 10.31ms | 6.42ms | 0.6× | 1.8% | PASS |
| ac_reads | `groupby_scan` | 29.51ms | 32.48ms | 1.1× | 1.0% | PASS |
| ac_reads | `index_join` | 7.31ms | 9.44ms | 1.3× | 1.2% | PASS |
| ac_reads | `index_join_scan` | 3.60ms | 4.88ms | 1.4× | 2.2% | PASS |
| ac_reads | `types_table_scan` | 1.04s | 1.12s | 1.1× | 0.3% | PASS |
| ac_reads | `table_scan` | 1.18s | 1.23s | 1.0× | 0.6% | PASS |
| ac_reads | `oltp_read_only` | 136.16ms | 144.18ms | 1.1× | 1.3% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.23ms | 75.60ms | 3.4× | 5.3% | PASS |
| ac_writes | `oltp_insert_ac` | 23.51ms | 89.06ms | 3.8× | 6.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.51ms | 99.55ms | 3.8× | 5.5% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.78ms | 84.66ms | 3.6× | 6.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.76ms | 92.28ms | 3.7× | 5.2% | PASS |
| ac_writes | `oltp_write_only_ac` | 24.31ms | 92.15ms | 3.8× | 5.7% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.20ms | 82.72ms | 3.7× | 6.3% | PASS |
| ac_writes | `oltp_read_write_ac` | 28.79ms | 96.15ms | 3.3× | 4.0% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 23.79ms | 22.98ms | 1.0× | 3.7% | PASS |
| mem_reads | `oltp_range_select` | 10.96ms | 8.67ms | 0.8× | 2.9% | PASS |
| mem_reads | `oltp_sum_range` | 10.36ms | 8.70ms | 0.8× | 2.9% | PASS |
| mem_reads | `oltp_order_range` | 2.27ms | 2.03ms | 0.9× | 2.7% | PASS |
| mem_reads | `oltp_distinct_range` | 2.81ms | 2.52ms | 0.9× | 2.4% | PASS |
| mem_reads | `oltp_index_scan` | 2.54ms | 3.97ms | 1.6× | 3.8% | PASS |
| mem_reads | `select_random_points` | 16.47ms | 13.92ms | 0.8× | 2.4% | PASS |
| mem_reads | `select_random_ranges` | 4.51ms | 3.47ms | 0.8× | 2.5% | PASS |
| mem_reads | `covering_index_scan` | 3.93ms | 2.95ms | 0.8× | 5.3% | PASS |
| mem_reads | `groupby_scan` | 20.76ms | 19.47ms | 0.9× | 2.4% | PASS |
| mem_reads | `index_join` | 8.05ms | 5.65ms | 0.7× | 3.8% | PASS |
| mem_reads | `index_join_scan` | 2.82ms | 4.67ms | 1.7× | 2.4% | PASS |
| mem_reads | `types_table_scan` | 798.36ms | 809.09ms | 1.0× | 1.6% | PASS |
| mem_reads | `table_scan` | 962.11ms | 915.62ms | 1.0× | 1.5% | PASS |
| mem_reads | `oltp_read_only` | 96.05ms | 81.25ms | 0.8× | 2.5% | PASS |
| mem_writes | `oltp_bulk_insert` | 138.17ms | 207.02ms | 1.5× | 1.5% | PASS |
| mem_writes | `oltp_insert` | 10.70ms | 23.07ms | 2.2× | 2.6% | PASS |
| mem_writes | `oltp_update_index` | 45.27ms | 92.74ms | 2.0× | 3.4% | PASS |
| mem_writes | `oltp_update_non_index` | 33.65ms | 53.75ms | 1.6× | 2.3% | PASS |
| mem_writes | `oltp_delete_insert` | 34.28ms | 63.91ms | 1.9× | 2.9% | PASS |
| mem_writes | `oltp_write_only` | 18.21ms | 37.92ms | 2.1× | 2.3% | PASS |
| mem_writes | `types_delete_insert` | 25.44ms | 32.36ms | 1.3× | 2.3% | PASS |
| mem_writes | `oltp_read_write` | 66.04ms | 88.73ms | 1.3× | 4.0% | PASS |
| file_reads | `oltp_point_select` | 79.51ms | 37.73ms | 0.5× | 1.6% | PASS |
| file_reads | `oltp_range_select` | 16.61ms | 10.14ms | 0.6× | 2.3% | PASS |
| file_reads | `oltp_sum_range` | 16.09ms | 10.10ms | 0.6× | 2.6% | PASS |
| file_reads | `oltp_order_range` | 2.89ms | 2.16ms | 0.7× | 1.8% | PASS |
| file_reads | `oltp_distinct_range` | 3.34ms | 2.66ms | 0.8× | 1.7% | PASS |
| file_reads | `oltp_index_scan` | 8.17ms | 5.67ms | 0.7× | 2.4% | PASS |
| file_reads | `select_random_points` | 21.41ms | 15.02ms | 0.7× | 2.2% | PASS |
| file_reads | `select_random_ranges` | 10.00ms | 4.95ms | 0.5× | 1.8% | PASS |
| file_reads | `covering_index_scan` | 9.66ms | 4.50ms | 0.5× | 3.7% | PASS |
| file_reads | `groupby_scan` | 21.34ms | 19.96ms | 0.9× | 1.6% | PASS |
| file_reads | `index_join` | 11.82ms | 7.73ms | 0.7× | 2.3% | PASS |
| file_reads | `index_join_scan` | 3.41ms | 4.60ms | 1.3× | 2.2% | PASS |
| file_reads | `types_table_scan` | 682.88ms | 725.79ms | 1.1× | 1.5% | PASS |
| file_reads | `table_scan` | 790.05ms | 818.31ms | 1.0× | 1.6% | PASS |
| file_reads | `oltp_read_only` | 153.64ms | 92.30ms | 0.6× | 2.6% | PASS |
| file_writes | `oltp_bulk_insert` | 241.11ms | 290.14ms | 1.2× | 10.6% | PASS |
| file_writes | `oltp_insert` | 23.11ms | 53.69ms | 2.3× | 65.9% | FAIL |
| file_writes | `oltp_update_index` | 165.56ms | 169.41ms | 1.0× | 10.1% | PASS |
| file_writes | `oltp_update_non_index` | 159.60ms | 121.40ms | 0.8× | 17.6% | PASS |
| file_writes | `oltp_delete_insert` | 176.55ms | 140.14ms | 0.8× | 9.3% | PASS |
| file_writes | `oltp_write_only` | 121.81ms | 99.54ms | 0.8× | 10.2% | PASS |
| file_writes | `types_delete_insert` | 162.61ms | 77.14ms | 0.5× | 28.2% | PASS |
| file_writes | `oltp_read_write` | 202.82ms | 157.28ms | 0.8× | 14.4% | PASS |
| ac_reads | `oltp_point_select` | 40.98ms | 37.30ms | 0.9× | 2.1% | PASS |
| ac_reads | `oltp_range_select` | 12.48ms | 10.10ms | 0.8× | 1.5% | PASS |
| ac_reads | `oltp_sum_range` | 12.17ms | 10.20ms | 0.8× | 1.7% | PASS |
| ac_reads | `oltp_order_range` | 2.62ms | 2.23ms | 0.9× | 2.1% | PASS |
| ac_reads | `oltp_distinct_range` | 3.03ms | 2.72ms | 0.9× | 2.0% | PASS |
| ac_reads | `oltp_index_scan` | 4.50ms | 5.44ms | 1.2× | 2.3% | PASS |
| ac_reads | `select_random_points` | 16.81ms | 14.51ms | 0.9× | 1.3% | PASS |
| ac_reads | `select_random_ranges` | 6.62ms | 5.02ms | 0.8× | 1.9% | PASS |
| ac_reads | `covering_index_scan` | 6.45ms | 4.79ms | 0.7× | 1.9% | PASS |
| ac_reads | `groupby_scan` | 21.29ms | 20.10ms | 0.9× | 1.3% | PASS |
| ac_reads | `index_join` | 9.80ms | 7.46ms | 0.8× | 1.8% | PASS |
| ac_reads | `index_join_scan` | 3.10ms | 4.49ms | 1.4× | 1.8% | PASS |
| ac_reads | `types_table_scan` | 696.90ms | 741.67ms | 1.1× | 1.9% | PASS |
| ac_reads | `table_scan` | 827.40ms | 833.65ms | 1.0× | 3.1% | PASS |
| ac_reads | `oltp_read_only` | 101.93ms | 91.84ms | 0.9× | 1.6% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 44.51ms | 128.83ms | 2.9× | 47.7% | PASS |
| ac_writes | `oltp_insert_ac` | 43.05ms | 150.03ms | 3.5× | 42.6% | PASS |
| ac_writes | `oltp_update_index_ac` | 47.99ms | 172.17ms | 3.6× | 41.5% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 62.16ms | 224.64ms | 3.6× | 46.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 53.78ms | 150.91ms | 2.8× | 38.5% | PASS |
| ac_writes | `oltp_write_only_ac` | 66.70ms | 219.45ms | 3.3× | 46.0% | PASS |
| ac_writes | `types_delete_insert_ac` | 53.71ms | 192.66ms | 3.6× | 75.3% | PASS |
| ac_writes | `oltp_read_write_ac` | 89.46ms | 207.74ms | 2.3× | 58.7% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 25.60ms | 24.92ms | 1.0× | 2.0% | PASS |
| mem_reads | `oltp_range_select` | 11.20ms | 9.80ms | 0.9× | 2.8% | PASS |
| mem_reads | `oltp_sum_range` | 10.82ms | 9.44ms | 0.9× | 3.4% | PASS |
| mem_reads | `oltp_order_range` | 2.42ms | 2.42ms | 1.0× | 3.3% | PASS |
| mem_reads | `oltp_distinct_range` | 3.24ms | 3.32ms | 1.0× | 3.0% | PASS |
| mem_reads | `oltp_index_scan` | 2.97ms | 4.04ms | 1.4× | 3.2% | PASS |
| mem_reads | `select_random_points` | 16.93ms | 15.56ms | 0.9× | 2.4% | PASS |
| mem_reads | `select_random_ranges` | 4.63ms | 3.79ms | 0.8× | 4.6% | PASS |
| mem_reads | `covering_index_scan` | 5.39ms | 3.03ms | 0.6× | 1.9% | PASS |
| mem_reads | `groupby_scan` | 26.98ms | 27.43ms | 1.0× | 1.7% | PASS |
| mem_reads | `index_join` | 8.80ms | 6.70ms | 0.8× | 2.3% | PASS |
| mem_reads | `index_join_scan` | 2.62ms | 4.33ms | 1.6× | 3.0% | PASS |
| mem_reads | `types_table_scan` | 867.55ms | 985.13ms | 1.1× | 0.4% | PASS |
| mem_reads | `table_scan` | 1.01s | 1.10s | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_read_only` | 101.42ms | 101.88ms | 1.0× | 2.5% | PASS |
| mem_writes | `oltp_bulk_insert` | 172.80ms | 249.91ms | 1.4× | 1.2% | PASS |
| mem_writes | `oltp_insert` | 13.43ms | 26.63ms | 2.0× | 2.5% | PASS |
| mem_writes | `oltp_update_index` | 48.28ms | 98.92ms | 2.0× | 2.8% | PASS |
| mem_writes | `oltp_update_non_index` | 34.21ms | 58.25ms | 1.7× | 2.9% | PASS |
| mem_writes | `oltp_delete_insert` | 37.52ms | 70.43ms | 1.9× | 3.1% | PASS |
| mem_writes | `oltp_write_only` | 17.82ms | 40.09ms | 2.2× | 3.7% | PASS |
| mem_writes | `types_delete_insert` | 26.80ms | 34.85ms | 1.3× | 3.8% | PASS |
| mem_writes | `oltp_read_write` | 63.11ms | 95.50ms | 1.5× | 1.5% | PASS |
| file_reads | `oltp_point_select` | 50.29ms | 31.11ms | 0.6× | 1.3% | PASS |
| file_reads | `oltp_range_select` | 13.61ms | 10.57ms | 0.8× | 1.8% | PASS |
| file_reads | `oltp_sum_range` | 13.44ms | 10.40ms | 0.8× | 4.0% | PASS |
| file_reads | `oltp_order_range` | 2.73ms | 2.52ms | 0.9× | 3.5% | PASS |
| file_reads | `oltp_distinct_range` | 3.52ms | 3.44ms | 1.0× | 4.7% | PASS |
| file_reads | `oltp_index_scan` | 5.60ms | 4.83ms | 0.9× | 3.3% | PASS |
| file_reads | `select_random_points` | 19.85ms | 16.93ms | 0.9× | 4.1% | PASS |
| file_reads | `select_random_ranges` | 7.15ms | 4.37ms | 0.6× | 4.0% | PASS |
| file_reads | `covering_index_scan` | 8.12ms | 3.76ms | 0.5× | 2.6% | PASS |
| file_reads | `groupby_scan` | 27.15ms | 27.64ms | 1.0× | 2.9% | PASS |
| file_reads | `index_join` | 10.17ms | 7.29ms | 0.7× | 3.6% | PASS |
| file_reads | `index_join_scan` | 2.90ms | 4.28ms | 1.5× | 5.5% | PASS |
| file_reads | `types_table_scan` | 884.44ms | 1.00s | 1.1× | 1.0% | PASS |
| file_reads | `table_scan` | 1.00s | 1.10s | 1.1× | 0.8% | PASS |
| file_reads | `oltp_read_only` | 138.60ms | 110.32ms | 0.8× | 2.4% | PASS |
| file_writes | `oltp_bulk_insert` | 245.19ms | 329.61ms | 1.3× | 6.9% | PASS |
| file_writes | `oltp_insert` | 28.51ms | 54.15ms | 1.9× | 17.9% | PASS |
| file_writes | `oltp_update_index` | 163.66ms | 185.26ms | 1.1× | 11.6% | PASS |
| file_writes | `oltp_update_non_index` | 129.14ms | 120.38ms | 0.9× | 8.5% | PASS |
| file_writes | `oltp_delete_insert` | 156.52ms | 139.71ms | 0.9× | 4.6% | PASS |
| file_writes | `oltp_write_only` | 120.31ms | 99.66ms | 0.8× | 20.1% | PASS |
| file_writes | `types_delete_insert` | 117.11ms | 78.57ms | 0.7× | 13.4% | PASS |
| file_writes | `oltp_read_write` | 155.52ms | 149.54ms | 1.0× | 2.0% | PASS |
| ac_reads | `oltp_point_select` | 33.36ms | 30.77ms | 0.9× | 1.6% | PASS |
| ac_reads | `oltp_range_select` | 11.80ms | 10.61ms | 0.9× | 2.8% | PASS |
| ac_reads | `oltp_sum_range` | 11.58ms | 10.20ms | 0.9× | 3.1% | PASS |
| ac_reads | `oltp_order_range` | 2.55ms | 2.52ms | 1.0× | 2.6% | PASS |
| ac_reads | `oltp_distinct_range` | 3.33ms | 3.37ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_index_scan` | 3.83ms | 4.72ms | 1.2× | 3.4% | PASS |
| ac_reads | `select_random_points` | 17.56ms | 15.84ms | 0.9× | 2.7% | PASS |
| ac_reads | `select_random_ranges` | 5.38ms | 4.31ms | 0.8× | 3.3% | PASS |
| ac_reads | `covering_index_scan` | 6.29ms | 3.75ms | 0.6× | 1.9% | PASS |
| ac_reads | `groupby_scan` | 27.22ms | 27.32ms | 1.0× | 1.1% | PASS |
| ac_reads | `index_join` | 9.56ms | 7.28ms | 0.8× | 3.7% | PASS |
| ac_reads | `index_join_scan` | 2.93ms | 4.54ms | 1.6× | 3.1% | PASS |
| ac_reads | `types_table_scan` | 870.88ms | 984.04ms | 1.1× | 0.6% | PASS |
| ac_reads | `table_scan` | 983.68ms | 1.08s | 1.1× | 0.7% | PASS |
| ac_reads | `oltp_read_only` | 113.70ms | 111.47ms | 1.0× | 2.7% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 42.78ms | 110.60ms | 2.6× | 32.0% | PASS |
| ac_writes | `oltp_insert_ac` | 53.63ms | 154.85ms | 2.9× | 37.2% | PASS |
| ac_writes | `oltp_update_index_ac` | 48.33ms | 166.93ms | 3.5× | 41.3% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 38.13ms | 115.73ms | 3.0× | 21.5% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 50.69ms | 163.77ms | 3.2× | 47.5% | PASS |
| ac_writes | `oltp_write_only_ac` | 38.62ms | 127.85ms | 3.3× | 27.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 46.05ms | 144.18ms | 3.1× | 45.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 59.27ms | 187.55ms | 3.2× | 40.9% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 34.11ms | 41.39ms | 1.2× | 1.5% | PASS |
| mem_reads | `oltp_range_select` | 20.22ms | 21.73ms | 1.1× | 0.7% | PASS |
| mem_reads | `oltp_sum_range` | 18.03ms | 20.10ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_order_range` | 3.55ms | 4.05ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 4.71ms | 5.29ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_index_scan` | 4.55ms | 6.24ms | 1.4× | 1.6% | PASS |
| mem_reads | `select_random_points` | 28.22ms | 33.10ms | 1.2× | 1.0% | PASS |
| mem_reads | `select_random_ranges` | 7.81ms | 8.98ms | 1.1× | 0.8% | PASS |
| mem_reads | `covering_index_scan` | 7.82ms | 4.58ms | 0.6× | 1.7% | PASS |
| mem_reads | `groupby_scan` | 36.02ms | 38.65ms | 1.1× | 0.8% | PASS |
| mem_reads | `index_join` | 8.10ms | 11.40ms | 1.4× | 1.7% | PASS |
| mem_reads | `index_join_scan` | 4.09ms | 5.94ms | 1.5× | 3.0% | PASS |
| mem_reads | `types_table_scan` | 1.19s | 1.23s | 1.0× | 1.0% | PASS |
| mem_reads | `table_scan` | 1.32s | 1.32s | 1.0× | 3.3% | PASS |
| mem_reads | `oltp_read_only` | 150.12ms | 175.32ms | 1.2× | 1.5% | PASS |
| mem_writes | `oltp_bulk_insert` | 249.05ms | 354.97ms | 1.4× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 19.09ms | 35.74ms | 1.9× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 68.51ms | 131.73ms | 1.9× | 1.3% | PASS |
| mem_writes | `oltp_update_non_index` | 50.99ms | 85.75ms | 1.7× | 0.9% | PASS |
| mem_writes | `oltp_delete_insert` | 49.16ms | 96.54ms | 2.0× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 26.68ms | 59.12ms | 2.2× | 0.8% | PASS |
| mem_writes | `types_delete_insert` | 32.26ms | 53.85ms | 1.7× | 1.2% | PASS |
| mem_writes | `oltp_read_write` | 105.68ms | 171.04ms | 1.6× | 1.4% | PASS |
| file_reads | `oltp_point_select` | 103.07ms | 60.18ms | 0.6× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 26.36ms | 23.87ms | 0.9× | 0.9% | PASS |
| file_reads | `oltp_sum_range` | 24.90ms | 22.40ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_order_range` | 4.18ms | 4.32ms | 1.0× | 1.0% | PASS |
| file_reads | `oltp_distinct_range` | 5.37ms | 5.56ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 11.36ms | 8.24ms | 0.7× | 1.3% | PASS |
| file_reads | `select_random_points` | 35.33ms | 35.69ms | 1.0× | 1.1% | PASS |
| file_reads | `select_random_ranges` | 14.88ms | 11.21ms | 0.8× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 15.09ms | 6.52ms | 0.4× | 1.4% | PASS |
| file_reads | `groupby_scan` | 36.28ms | 38.95ms | 1.1× | 0.9% | PASS |
| file_reads | `index_join` | 12.03ms | 12.23ms | 1.0× | 2.0% | PASS |
| file_reads | `index_join_scan` | 4.81ms | 5.77ms | 1.2× | 1.7% | PASS |
| file_reads | `types_table_scan` | 1.10s | 1.19s | 1.1× | 2.8% | PASS |
| file_reads | `table_scan` | 1.39s | 1.33s | 1.0× | 1.1% | PASS |
| file_reads | `oltp_read_only` | 261.44ms | 206.83ms | 0.8× | 1.2% | PASS |
| file_writes | `oltp_bulk_insert` | 264.67ms | 362.31ms | 1.4× | 0.9% | PASS |
| file_writes | `oltp_insert` | 25.67ms | 40.85ms | 1.6× | 1.0% | PASS |
| file_writes | `oltp_update_index` | 92.04ms | 138.28ms | 1.5× | 1.1% | PASS |
| file_writes | `oltp_update_non_index` | 74.22ms | 96.92ms | 1.3× | 1.5% | PASS |
| file_writes | `oltp_delete_insert` | 73.81ms | 107.20ms | 1.5× | 0.9% | PASS |
| file_writes | `oltp_write_only` | 47.92ms | 67.20ms | 1.4× | 1.2% | PASS |
| file_writes | `types_delete_insert` | 46.88ms | 58.71ms | 1.3× | 1.5% | PASS |
| file_writes | `oltp_read_write` | 121.13ms | 174.09ms | 1.4× | 0.6% | PASS |
| ac_reads | `oltp_point_select` | 55.72ms | 59.68ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_range_select` | 22.43ms | 23.92ms | 1.1× | 1.8% | PASS |
| ac_reads | `oltp_sum_range` | 20.75ms | 22.51ms | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_order_range` | 3.95ms | 4.33ms | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_distinct_range` | 5.11ms | 5.55ms | 1.1× | 2.0% | PASS |
| ac_reads | `oltp_index_scan` | 7.17ms | 8.38ms | 1.2× | 1.3% | PASS |
| ac_reads | `select_random_points` | 31.26ms | 35.97ms | 1.2× | 1.2% | PASS |
| ac_reads | `select_random_ranges` | 10.39ms | 11.23ms | 1.1× | 0.8% | PASS |
| ac_reads | `covering_index_scan` | 10.49ms | 6.59ms | 0.6× | 1.2% | PASS |
| ac_reads | `groupby_scan` | 35.86ms | 39.03ms | 1.1× | 0.7% | PASS |
| ac_reads | `index_join` | 9.54ms | 12.27ms | 1.3× | 1.5% | PASS |
| ac_reads | `index_join_scan` | 4.25ms | 5.87ms | 1.4× | 1.7% | PASS |
| ac_reads | `types_table_scan` | 1.05s | 1.18s | 1.1× | 0.4% | PASS |
| ac_reads | `table_scan` | 1.24s | 1.31s | 1.1× | 2.5% | PASS |
| ac_reads | `oltp_read_only` | 192.50ms | 207.23ms | 1.1× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.47ms | 74.89ms | 3.3× | 4.1% | PASS |
| ac_writes | `oltp_insert_ac` | 25.47ms | 93.20ms | 3.7× | 6.6% | PASS |
| ac_writes | `oltp_update_index_ac` | 28.10ms | 101.56ms | 3.6× | 5.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.28ms | 85.49ms | 3.5× | 4.7% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.72ms | 92.73ms | 3.8× | 5.0% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.45ms | 95.55ms | 3.6× | 6.8% | PASS |
| ac_writes | `types_delete_insert_ac` | 23.22ms | 84.84ms | 3.7× | 5.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.44ms | 101.39ms | 3.1× | 5.9% | PASS |

</details>

</details>

## Version-control latency

Wall time: 2m 38s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 89.91ms | 130.00ms | 69.2% | 1.0% | PASS |
| `status_dirty_many_tables` | 94.30ms | 130.00ms | 72.5% | 0.5% | PASS |
| `diff_regular_working_one_table` | 86.16ms | 120.00ms | 71.8% | 0.6% | PASS |
| `diff_regular_working_many_tables` | 99.45ms | 140.00ms | 71.0% | 0.5% | PASS |
| `diff_stat_working_many_tables` | 99.53ms | 140.00ms | 71.1% | 0.5% | PASS |
| `diff_schema_working_many_tables` | 100.45ms | 140.00ms | 71.7% | 0.6% | PASS |
| `branch_list_many_branches` | 25.68ms | 35.00ms | 73.4% | 2.2% | PASS |
| `branch_create_delete` | 28.37ms | 40.00ms | 70.9% | 2.0% | PASS |
| `at_literal_deep_history` | 39.19ms | 100.00ms | 39.2% | 1.1% | PASS |
| `diff_literal_deep_history` | 39.26ms | 120.00ms | 32.7% | 1.1% | PASS |
| `history_literal_deep_history` | 40.16ms | 150.00ms | 26.8% | 1.0% | PASS |
| `checkout_branch_clean` | 59.94ms | 150.00ms | 40.0% | 1.2% | PASS |
| `merge_data_no_conflicts` | 31.96ms | 50.00ms | 63.9% | 1.5% | PASS |
| `merge_schema_no_conflicts` | 25.02ms | 35.00ms | 71.5% | 1.4% | PASS |
| `merge_data_conflicts` | 34.74ms | 180.00ms | 19.3% | 1.0% | PASS |
| `merge_data_conflicts_with_resolve` | 34.84ms | 180.00ms | 19.4% | 1.2% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
