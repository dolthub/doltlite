# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-09-03 11:15 UTC
>
> Commit: [`5c67114c037485ef4d0e4653abf260dc64125298`](https://github.com/dolthub/doltlite/commit/5c67114c037485ef4d0e4653abf260dc64125298)
>
> Runner: ubuntu24 20260831.293.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/33739724590)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.68s | 11.16s | 1.0× | 1.5% | **PASS** |
| Writes | 2.17s | 3.46s | 1.6× | 1.1% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 11.87s | 11.57s | 1.0× | 1.3% | **PASS** |
| Writes | 3.52s | 4.18s | 1.2× | 1.8% | **PASS** |
| Autocommit writes | 802.79ms | 2.96s | 3.7× | 5.7% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.53s | 2.68s | 1.1× | 2.1% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.89s | 2.87s | 1.0× | 1.6% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.77s | 2.91s | 1.1× | 1.4% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.50s | 2.70s | 1.1× | 1.1% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 459.52ms | 713.00ms | 1.6× | 1.6% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 622.21ms | 1.03s | 1.6× | 1.4% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 598.65ms | 957.67ms | 1.6× | 1.3% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 489.97ms | 762.57ms | 1.6× | 0.8% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.94s | 2.82s | 1.0× | 1.6% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 3.16s | 2.95s | 0.9× | 1.5% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 3.02s | 2.99s | 1.0× | 1.2% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.76s | 2.81s | 1.0× | 1.2% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 608.89ms | 782.68ms | 1.3× | 2.0% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 962.38ms | 1.13s | 1.2× | 3.5% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 816.30ms | 1.05s | 1.3× | 1.3% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 1.14s | 1.22s | 1.1× | 1.7% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.76s | 2.81s | 1.0× | 1.7% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 3.13s | 3.01s | 1.0× | 1.2% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 3.06s | 3.10s | 1.0× | 1.3% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.64s | 2.79s | 1.1× | 1.0% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 194.36ms | 718.52ms | 3.7× | 4.9% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 223.81ms | 784.71ms | 3.5× | 6.5% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 151.83ms | 573.00ms | 3.8× | 4.5% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 232.80ms | 886.57ms | 3.8× | 15.3% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 25.57ms | 30.59ms | 1.2× | 2.5% | PASS |
| mem_reads | `oltp_range_select` | 10.88ms | 12.28ms | 1.1× | 2.6% | PASS |
| mem_reads | `oltp_sum_range` | 10.04ms | 11.36ms | 1.1× | 2.1% | PASS |
| mem_reads | `oltp_order_range` | 2.64ms | 2.83ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 3.73ms | 4.05ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 3.95ms | 5.33ms | 1.3× | 2.6% | PASS |
| mem_reads | `select_random_points` | 10.86ms | 11.19ms | 1.0× | 4.4% | PASS |
| mem_reads | `select_random_ranges` | 3.01ms | 4.01ms | 1.3× | 1.6% | PASS |
| mem_reads | `covering_index_scan` | 4.13ms | 4.25ms | 1.0× | 2.1% | PASS |
| mem_reads | `groupby_scan` | 30.88ms | 32.97ms | 1.1× | 0.8% | PASS |
| mem_reads | `index_join` | 5.81ms | 7.82ms | 1.3× | 1.5% | PASS |
| mem_reads | `index_join_scan` | 3.31ms | 4.57ms | 1.4× | 3.3% | PASS |
| mem_reads | `types_table_scan` | 1.06s | 1.14s | 1.1× | 0.8% | PASS |
| mem_reads | `table_scan` | 1.24s | 1.29s | 1.0× | 2.4% | PASS |
| mem_reads | `oltp_read_only` | 111.31ms | 122.41ms | 1.1× | 1.7% | PASS |
| mem_writes | `oltp_bulk_insert` | 180.71ms | 252.02ms | 1.4× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 15.20ms | 26.98ms | 1.8× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 50.34ms | 90.89ms | 1.8× | 1.6% | PASS |
| mem_writes | `oltp_update_non_index` | 35.86ms | 55.34ms | 1.5× | 1.8% | PASS |
| mem_writes | `oltp_delete_insert` | 46.09ms | 73.62ms | 1.6× | 1.7% | PASS |
| mem_writes | `oltp_write_only` | 23.50ms | 47.97ms | 2.0× | 2.4% | PASS |
| mem_writes | `types_delete_insert` | 25.95ms | 38.63ms | 1.5× | 1.1% | PASS |
| mem_writes | `oltp_read_write` | 81.87ms | 127.55ms | 1.6× | 2.8% | PASS |
| file_reads | `oltp_point_select` | 96.59ms | 50.52ms | 0.5× | 1.5% | PASS |
| file_reads | `oltp_range_select` | 19.38ms | 14.18ms | 0.7× | 2.8% | PASS |
| file_reads | `oltp_sum_range` | 18.11ms | 13.87ms | 0.8× | 2.4% | PASS |
| file_reads | `oltp_order_range` | 3.64ms | 3.25ms | 0.9× | 2.2% | PASS |
| file_reads | `oltp_distinct_range` | 4.91ms | 4.62ms | 0.9× | 3.2% | PASS |
| file_reads | `oltp_index_scan` | 11.69ms | 7.80ms | 0.7× | 1.3% | PASS |
| file_reads | `select_random_points` | 19.44ms | 13.85ms | 0.7× | 3.6% | PASS |
| file_reads | `select_random_ranges` | 10.22ms | 6.00ms | 0.6× | 1.4% | PASS |
| file_reads | `covering_index_scan` | 11.66ms | 6.60ms | 0.6× | 1.6% | PASS |
| file_reads | `groupby_scan` | 32.52ms | 33.70ms | 1.0× | 0.7% | PASS |
| file_reads | `index_join` | 10.18ms | 10.07ms | 1.0× | 2.2% | PASS |
| file_reads | `index_join_scan` | 4.62ms | 5.25ms | 1.1× | 4.8% | PASS |
| file_reads | `types_table_scan` | 1.15s | 1.18s | 1.0× | 0.4% | PASS |
| file_reads | `table_scan` | 1.32s | 1.32s | 1.0× | 0.5% | PASS |
| file_reads | `oltp_read_only` | 215.52ms | 152.45ms | 0.7× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 194.12ms | 264.29ms | 1.4× | 1.4% | PASS |
| file_writes | `oltp_insert` | 22.84ms | 31.87ms | 1.4× | 2.1% | PASS |
| file_writes | `oltp_update_index` | 82.06ms | 109.52ms | 1.3× | 2.9% | PASS |
| file_writes | `oltp_update_non_index` | 59.81ms | 68.59ms | 1.1× | 2.1% | PASS |
| file_writes | `oltp_delete_insert` | 69.50ms | 84.14ms | 1.2× | 1.8% | PASS |
| file_writes | `oltp_write_only` | 44.95ms | 55.14ms | 1.2× | 2.4% | PASS |
| file_writes | `types_delete_insert` | 40.23ms | 43.43ms | 1.1× | 1.6% | PASS |
| file_writes | `oltp_read_write` | 95.37ms | 125.71ms | 1.3× | 1.8% | PASS |
| ac_reads | `oltp_point_select` | 48.84ms | 49.90ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_range_select` | 14.30ms | 14.17ms | 1.0× | 1.8% | PASS |
| ac_reads | `oltp_sum_range` | 13.20ms | 13.73ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_order_range` | 3.14ms | 3.19ms | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_distinct_range` | 4.28ms | 4.44ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_index_scan` | 7.00ms | 7.83ms | 1.1× | 1.8% | PASS |
| ac_reads | `select_random_points` | 15.06ms | 13.98ms | 0.9× | 2.5% | PASS |
| ac_reads | `select_random_ranges` | 5.64ms | 6.03ms | 1.1× | 0.8% | PASS |
| ac_reads | `covering_index_scan` | 7.35ms | 6.74ms | 0.9× | 2.2% | PASS |
| ac_reads | `groupby_scan` | 31.83ms | 33.69ms | 1.1× | 0.6% | PASS |
| ac_reads | `index_join` | 8.18ms | 10.15ms | 1.2× | 2.2% | PASS |
| ac_reads | `index_join_scan` | 4.10ms | 5.10ms | 1.2× | 2.8% | PASS |
| ac_reads | `types_table_scan` | 1.14s | 1.17s | 1.0× | 0.4% | PASS |
| ac_reads | `table_scan` | 1.31s | 1.32s | 1.0× | 0.5% | PASS |
| ac_reads | `oltp_read_only` | 148.77ms | 152.75ms | 1.0× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.19ms | 78.09ms | 3.5× | 7.3% | PASS |
| ac_writes | `oltp_insert_ac` | 24.44ms | 89.39ms | 3.7× | 4.6% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.31ms | 100.88ms | 3.8× | 5.0% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.44ms | 82.83ms | 3.7× | 4.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 23.58ms | 91.04ms | 3.9× | 4.9% | PASS |
| ac_writes | `oltp_write_only_ac` | 24.17ms | 92.85ms | 3.8× | 4.8% | PASS |
| ac_writes | `types_delete_insert_ac` | 21.79ms | 84.88ms | 3.9× | 5.6% | PASS |
| ac_writes | `oltp_read_write_ac` | 29.44ms | 98.55ms | 3.3× | 4.5% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 35.43ms | 38.86ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_range_select` | 16.34ms | 14.95ms | 0.9× | 1.7% | PASS |
| mem_reads | `oltp_sum_range` | 15.78ms | 13.81ms | 0.9× | 1.6% | PASS |
| mem_reads | `oltp_order_range` | 3.36ms | 3.19ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 4.50ms | 4.43ms | 1.0× | 0.7% | PASS |
| mem_reads | `oltp_index_scan` | 4.08ms | 6.36ms | 1.6× | 1.9% | PASS |
| mem_reads | `select_random_points` | 22.96ms | 21.65ms | 0.9× | 2.3% | PASS |
| mem_reads | `select_random_ranges` | 3.71ms | 5.38ms | 1.4× | 1.4% | PASS |
| mem_reads | `covering_index_scan` | 4.23ms | 4.84ms | 1.1× | 2.0% | PASS |
| mem_reads | `groupby_scan` | 35.89ms | 34.59ms | 1.0× | 0.6% | PASS |
| mem_reads | `index_join` | 10.48ms | 9.61ms | 0.9× | 1.5% | PASS |
| mem_reads | `index_join_scan` | 3.69ms | 5.35ms | 1.4× | 1.4% | PASS |
| mem_reads | `types_table_scan` | 1.21s | 1.22s | 1.0× | 2.3% | PASS |
| mem_reads | `table_scan` | 1.38s | 1.35s | 1.0× | 1.6% | PASS |
| mem_reads | `oltp_read_only` | 143.46ms | 141.85ms | 1.0× | 1.9% | PASS |
| mem_writes | `oltp_bulk_insert` | 247.58ms | 377.55ms | 1.5× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 18.55ms | 40.29ms | 2.2× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 69.61ms | 144.18ms | 2.1× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 52.23ms | 84.14ms | 1.6× | 1.8% | PASS |
| mem_writes | `oltp_delete_insert` | 56.38ms | 108.44ms | 1.9× | 1.3% | PASS |
| mem_writes | `oltp_write_only` | 28.94ms | 62.85ms | 2.2× | 1.6% | PASS |
| mem_writes | `types_delete_insert` | 40.83ms | 56.50ms | 1.4× | 1.4% | PASS |
| mem_writes | `oltp_read_write` | 108.10ms | 152.14ms | 1.4× | 2.3% | PASS |
| file_reads | `oltp_point_select` | 108.90ms | 58.99ms | 0.5× | 1.5% | PASS |
| file_reads | `oltp_range_select` | 24.16ms | 17.09ms | 0.7× | 0.9% | PASS |
| file_reads | `oltp_sum_range` | 23.30ms | 15.91ms | 0.7× | 1.6% | PASS |
| file_reads | `oltp_order_range` | 4.22ms | 3.46ms | 0.8× | 1.7% | PASS |
| file_reads | `oltp_distinct_range` | 5.34ms | 4.71ms | 0.9× | 0.7% | PASS |
| file_reads | `oltp_index_scan` | 11.37ms | 8.38ms | 0.7× | 1.0% | PASS |
| file_reads | `select_random_points` | 31.53ms | 24.42ms | 0.8× | 2.3% | PASS |
| file_reads | `select_random_ranges` | 10.71ms | 7.30ms | 0.7× | 0.9% | PASS |
| file_reads | `covering_index_scan` | 11.66ms | 6.83ms | 0.6× | 1.0% | PASS |
| file_reads | `groupby_scan` | 37.00ms | 35.01ms | 0.9× | 0.9% | PASS |
| file_reads | `index_join` | 15.14ms | 10.92ms | 0.7× | 2.5% | PASS |
| file_reads | `index_join_scan` | 4.66ms | 5.69ms | 1.2× | 1.3% | PASS |
| file_reads | `types_table_scan` | 1.24s | 1.23s | 1.0× | 2.9% | PASS |
| file_reads | `table_scan` | 1.38s | 1.35s | 1.0× | 2.7% | PASS |
| file_reads | `oltp_read_only` | 251.79ms | 172.20ms | 0.7× | 1.5% | PASS |
| file_writes | `oltp_bulk_insert` | 270.10ms | 389.94ms | 1.4× | 0.8% | PASS |
| file_writes | `oltp_insert` | 26.47ms | 46.83ms | 1.8× | 2.1% | PASS |
| file_writes | `oltp_update_index` | 132.42ms | 160.90ms | 1.2× | 6.5% | PASS |
| file_writes | `oltp_update_non_index` | 94.17ms | 97.67ms | 1.0× | 6.5% | PASS |
| file_writes | `oltp_delete_insert` | 98.69ms | 122.72ms | 1.2× | 1.5% | PASS |
| file_writes | `oltp_write_only` | 85.47ms | 75.86ms | 0.9× | 9.1% | PASS |
| file_writes | `types_delete_insert` | 76.58ms | 66.81ms | 0.9× | 1.9% | PASS |
| file_writes | `oltp_read_write` | 178.49ms | 168.76ms | 0.9× | 4.8% | PASS |
| ac_reads | `oltp_point_select` | 63.50ms | 60.50ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_range_select` | 20.52ms | 17.55ms | 0.9× | 1.4% | PASS |
| ac_reads | `oltp_sum_range` | 18.46ms | 15.92ms | 0.9× | 1.7% | PASS |
| ac_reads | `oltp_order_range` | 3.70ms | 3.46ms | 0.9× | 1.6% | PASS |
| ac_reads | `oltp_distinct_range` | 4.82ms | 4.71ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_index_scan` | 6.70ms | 8.48ms | 1.3× | 1.1% | PASS |
| ac_reads | `select_random_points` | 26.07ms | 24.73ms | 0.9× | 2.2% | PASS |
| ac_reads | `select_random_ranges` | 6.23ms | 7.40ms | 1.2× | 1.1% | PASS |
| ac_reads | `covering_index_scan` | 7.07ms | 6.96ms | 1.0× | 1.2% | PASS |
| ac_reads | `groupby_scan` | 36.75ms | 35.42ms | 1.0× | 0.7% | PASS |
| ac_reads | `index_join` | 13.34ms | 11.55ms | 0.9× | 1.7% | PASS |
| ac_reads | `index_join_scan` | 4.29ms | 6.11ms | 1.4× | 2.7% | PASS |
| ac_reads | `types_table_scan` | 1.28s | 1.25s | 1.0× | 0.7% | PASS |
| ac_reads | `table_scan` | 1.45s | 1.38s | 1.0× | 0.6% | PASS |
| ac_reads | `oltp_read_only` | 193.25ms | 178.04ms | 0.9× | 1.1% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 24.87ms | 81.07ms | 3.3× | 5.9% | PASS |
| ac_writes | `oltp_insert_ac` | 28.98ms | 94.66ms | 3.3× | 7.2% | PASS |
| ac_writes | `oltp_update_index_ac` | 30.54ms | 111.20ms | 3.6× | 7.3% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.38ms | 89.18ms | 3.7× | 6.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 28.08ms | 102.48ms | 3.6× | 7.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 27.29ms | 101.23ms | 3.7× | 5.0% | PASS |
| ac_writes | `types_delete_insert_ac` | 25.29ms | 98.31ms | 3.9× | 5.6% | PASS |
| ac_writes | `oltp_read_write_ac` | 34.38ms | 106.59ms | 3.1× | 6.3% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 34.86ms | 35.20ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_range_select` | 16.57ms | 13.49ms | 0.8× | 1.7% | PASS |
| mem_reads | `oltp_sum_range` | 15.74ms | 13.24ms | 0.8× | 2.6% | PASS |
| mem_reads | `oltp_order_range` | 3.57ms | 3.21ms | 0.9× | 1.3% | PASS |
| mem_reads | `oltp_distinct_range` | 4.63ms | 4.35ms | 0.9× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 4.38ms | 6.22ms | 1.4× | 2.3% | PASS |
| mem_reads | `select_random_points` | 22.98ms | 20.60ms | 0.9× | 3.7% | PASS |
| mem_reads | `select_random_ranges` | 3.51ms | 5.35ms | 1.5× | 1.2% | PASS |
| mem_reads | `covering_index_scan` | 4.28ms | 4.46ms | 1.0× | 1.4% | PASS |
| mem_reads | `groupby_scan` | 37.47ms | 36.23ms | 1.0× | 0.8% | PASS |
| mem_reads | `index_join` | 11.74ms | 9.89ms | 0.8× | 2.6% | PASS |
| mem_reads | `index_join_scan` | 4.15ms | 5.74ms | 1.4× | 3.4% | PASS |
| mem_reads | `types_table_scan` | 1.16s | 1.25s | 1.1× | 1.9% | PASS |
| mem_reads | `table_scan` | 1.30s | 1.38s | 1.1× | 0.7% | PASS |
| mem_reads | `oltp_read_only` | 140.14ms | 131.52ms | 0.9× | 1.2% | PASS |
| mem_writes | `oltp_bulk_insert` | 248.31ms | 349.41ms | 1.4× | 1.5% | PASS |
| mem_writes | `oltp_insert` | 19.07ms | 38.67ms | 2.0× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 65.45ms | 133.49ms | 2.0× | 1.2% | PASS |
| mem_writes | `oltp_update_non_index` | 49.93ms | 78.06ms | 1.6× | 1.4% | PASS |
| mem_writes | `oltp_delete_insert` | 53.49ms | 104.59ms | 2.0× | 1.1% | PASS |
| mem_writes | `oltp_write_only` | 28.33ms | 62.17ms | 2.2× | 1.1% | PASS |
| mem_writes | `types_delete_insert` | 38.61ms | 51.93ms | 1.3× | 1.4% | PASS |
| mem_writes | `oltp_read_write` | 95.47ms | 139.35ms | 1.5× | 1.5% | PASS |
| file_reads | `oltp_point_select` | 119.81ms | 56.96ms | 0.5× | 1.2% | PASS |
| file_reads | `oltp_range_select` | 25.93ms | 15.77ms | 0.6× | 1.9% | PASS |
| file_reads | `oltp_sum_range` | 24.20ms | 15.43ms | 0.6× | 1.7% | PASS |
| file_reads | `oltp_order_range` | 4.39ms | 3.47ms | 0.8× | 1.7% | PASS |
| file_reads | `oltp_distinct_range` | 5.45ms | 4.60ms | 0.8× | 1.1% | PASS |
| file_reads | `oltp_index_scan` | 12.73ms | 8.23ms | 0.6× | 1.3% | PASS |
| file_reads | `select_random_points` | 30.34ms | 22.16ms | 0.7× | 1.9% | PASS |
| file_reads | `select_random_ranges` | 11.98ms | 7.54ms | 0.6× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 13.03ms | 7.02ms | 0.5× | 1.1% | PASS |
| file_reads | `groupby_scan` | 38.34ms | 36.54ms | 1.0× | 1.0% | PASS |
| file_reads | `index_join` | 15.56ms | 10.79ms | 0.7× | 1.9% | PASS |
| file_reads | `index_join_scan` | 4.86ms | 5.87ms | 1.2× | 1.3% | PASS |
| file_reads | `types_table_scan` | 1.15s | 1.25s | 1.1× | 0.5% | PASS |
| file_reads | `table_scan` | 1.30s | 1.38s | 1.1× | 0.7% | PASS |
| file_reads | `oltp_read_only` | 262.39ms | 163.00ms | 0.6× | 0.7% | PASS |
| file_writes | `oltp_bulk_insert` | 268.81ms | 359.73ms | 1.3× | 1.0% | PASS |
| file_writes | `oltp_insert` | 25.59ms | 45.28ms | 1.8× | 1.4% | PASS |
| file_writes | `oltp_update_index` | 95.81ms | 148.22ms | 1.5× | 1.3% | PASS |
| file_writes | `oltp_update_non_index` | 89.22ms | 90.50ms | 1.0× | 10.9% | PASS |
| file_writes | `oltp_delete_insert` | 85.61ms | 116.85ms | 1.4× | 1.2% | PASS |
| file_writes | `oltp_write_only` | 57.32ms | 72.89ms | 1.3× | 2.4% | PASS |
| file_writes | `types_delete_insert` | 64.26ms | 61.51ms | 1.0× | 1.6% | PASS |
| file_writes | `oltp_read_write` | 129.67ms | 151.97ms | 1.2× | 1.2% | PASS |
| ac_reads | `oltp_point_select` | 64.76ms | 57.74ms | 0.9× | 1.4% | PASS |
| ac_reads | `oltp_range_select` | 20.87ms | 15.88ms | 0.8× | 2.0% | PASS |
| ac_reads | `oltp_sum_range` | 19.24ms | 15.59ms | 0.8× | 1.3% | PASS |
| ac_reads | `oltp_order_range` | 3.94ms | 3.48ms | 0.9× | 0.8% | PASS |
| ac_reads | `oltp_distinct_range` | 4.96ms | 4.61ms | 0.9× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 7.23ms | 8.27ms | 1.1× | 1.1% | PASS |
| ac_reads | `select_random_points` | 25.68ms | 22.45ms | 0.9× | 3.0% | PASS |
| ac_reads | `select_random_ranges` | 6.50ms | 7.59ms | 1.2× | 1.1% | PASS |
| ac_reads | `covering_index_scan` | 7.50ms | 7.07ms | 0.9× | 1.5% | PASS |
| ac_reads | `groupby_scan` | 37.96ms | 36.59ms | 1.0× | 0.7% | PASS |
| ac_reads | `index_join` | 12.98ms | 10.91ms | 0.8× | 2.1% | PASS |
| ac_reads | `index_join_scan` | 4.39ms | 5.89ms | 1.3× | 2.3% | PASS |
| ac_reads | `types_table_scan` | 1.24s | 1.30s | 1.0× | 1.3% | PASS |
| ac_reads | `table_scan` | 1.42s | 1.44s | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_read_only` | 187.76ms | 166.54ms | 0.9× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 16.76ms | 57.09ms | 3.4× | 4.6% | PASS |
| ac_writes | `oltp_insert_ac` | 19.03ms | 72.37ms | 3.8× | 4.6% | PASS |
| ac_writes | `oltp_update_index_ac` | 19.99ms | 83.63ms | 4.2× | 4.3% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 16.02ms | 65.07ms | 4.1× | 5.1% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 18.70ms | 73.27ms | 3.9× | 3.2% | PASS |
| ac_writes | `oltp_write_only_ac` | 18.51ms | 74.58ms | 4.0× | 3.7% | PASS |
| ac_writes | `types_delete_insert_ac` | 17.34ms | 65.94ms | 3.8× | 6.8% | PASS |
| ac_writes | `oltp_read_write_ac` | 25.48ms | 81.05ms | 3.2× | 3.5% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 28.50ms | 32.64ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_range_select` | 17.17ms | 18.80ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_sum_range` | 16.16ms | 18.04ms | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_order_range` | 3.33ms | 3.51ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_distinct_range` | 4.29ms | 4.61ms | 1.1× | 1.7% | PASS |
| mem_reads | `oltp_index_scan` | 4.05ms | 5.00ms | 1.2× | 1.5% | PASS |
| mem_reads | `select_random_points` | 26.07ms | 29.33ms | 1.1× | 1.1% | PASS |
| mem_reads | `select_random_ranges` | 6.50ms | 7.56ms | 1.2× | 1.5% | PASS |
| mem_reads | `covering_index_scan` | 3.46ms | 3.60ms | 1.0× | 1.6% | PASS |
| mem_reads | `groupby_scan` | 34.62ms | 38.09ms | 1.1× | 1.1% | PASS |
| mem_reads | `index_join` | 7.09ms | 9.30ms | 1.3× | 1.6% | PASS |
| mem_reads | `index_join_scan` | 3.44ms | 5.01ms | 1.5× | 1.1% | PASS |
| mem_reads | `types_table_scan` | 1.03s | 1.13s | 1.1× | 0.4% | PASS |
| mem_reads | `table_scan` | 1.19s | 1.24s | 1.0× | 0.5% | PASS |
| mem_reads | `oltp_read_only` | 129.23ms | 146.75ms | 1.1× | 0.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 203.33ms | 276.56ms | 1.4× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 16.30ms | 28.05ms | 1.7× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 56.31ms | 101.55ms | 1.8× | 0.7% | PASS |
| mem_writes | `oltp_update_non_index` | 41.43ms | 62.99ms | 1.5× | 1.1% | PASS |
| mem_writes | `oltp_delete_insert` | 41.70ms | 78.35ms | 1.9× | 0.8% | PASS |
| mem_writes | `oltp_write_only` | 22.31ms | 45.35ms | 2.0× | 0.9% | PASS |
| mem_writes | `types_delete_insert` | 26.26ms | 42.17ms | 1.6× | 0.8% | PASS |
| mem_writes | `oltp_read_write` | 82.34ms | 127.54ms | 1.5× | 0.8% | PASS |
| file_reads | `oltp_point_select` | 58.70ms | 40.71ms | 0.7× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 20.66ms | 20.10ms | 1.0× | 1.2% | PASS |
| file_reads | `oltp_sum_range` | 19.84ms | 19.22ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_order_range` | 3.77ms | 3.71ms | 1.0× | 1.7% | PASS |
| file_reads | `oltp_distinct_range` | 4.72ms | 4.76ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 7.41ms | 6.29ms | 0.8× | 1.5% | PASS |
| file_reads | `select_random_points` | 29.54ms | 30.42ms | 1.0× | 1.1% | PASS |
| file_reads | `select_random_ranges` | 9.90ms | 8.54ms | 0.9× | 1.5% | PASS |
| file_reads | `covering_index_scan` | 6.81ms | 4.91ms | 0.7× | 1.9% | PASS |
| file_reads | `groupby_scan` | 35.34ms | 38.15ms | 1.1× | 0.6% | PASS |
| file_reads | `index_join` | 9.19ms | 10.68ms | 1.2× | 1.0% | PASS |
| file_reads | `index_join_scan` | 3.94ms | 5.36ms | 1.4× | 1.7% | PASS |
| file_reads | `types_table_scan` | 1.05s | 1.15s | 1.1× | 0.7% | PASS |
| file_reads | `table_scan` | 1.32s | 1.30s | 1.0× | 0.9% | PASS |
| file_reads | `oltp_read_only` | 183.76ms | 164.41ms | 0.9× | 0.4% | PASS |
| file_writes | `oltp_bulk_insert` | 265.96ms | 347.19ms | 1.3× | 3.4% | PASS |
| file_writes | `oltp_insert` | 33.37ms | 50.86ms | 1.5× | 5.1% | PASS |
| file_writes | `oltp_update_index` | 176.45ms | 180.19ms | 1.0× | 1.7% | PASS |
| file_writes | `oltp_update_non_index` | 146.23ms | 123.99ms | 0.8× | 0.9% | PASS |
| file_writes | `oltp_delete_insert` | 145.57ms | 145.03ms | 1.0× | 1.8% | PASS |
| file_writes | `oltp_write_only` | 104.60ms | 100.60ms | 1.0× | 1.5% | PASS |
| file_writes | `types_delete_insert` | 95.00ms | 82.41ms | 0.9× | 3.3% | PASS |
| file_writes | `oltp_read_write` | 168.78ms | 185.71ms | 1.1× | 1.6% | PASS |
| ac_reads | `oltp_point_select` | 38.68ms | 41.67ms | 1.1× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 18.78ms | 20.33ms | 1.1× | 0.7% | PASS |
| ac_reads | `oltp_sum_range` | 18.03ms | 19.52ms | 1.1× | 0.9% | PASS |
| ac_reads | `oltp_order_range` | 3.58ms | 3.74ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_distinct_range` | 4.58ms | 4.80ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_index_scan` | 5.49ms | 6.42ms | 1.2× | 0.7% | PASS |
| ac_reads | `select_random_points` | 28.49ms | 31.27ms | 1.1× | 1.0% | PASS |
| ac_reads | `select_random_ranges` | 7.93ms | 8.70ms | 1.1× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 4.77ms | 4.95ms | 1.0× | 1.2% | PASS |
| ac_reads | `groupby_scan` | 35.55ms | 38.35ms | 1.1× | 0.5% | PASS |
| ac_reads | `index_join` | 8.15ms | 10.74ms | 1.3× | 1.1% | PASS |
| ac_reads | `index_join_scan` | 3.80ms | 5.37ms | 1.4× | 1.2% | PASS |
| ac_reads | `types_table_scan` | 1.05s | 1.16s | 1.1× | 0.7% | PASS |
| ac_reads | `table_scan` | 1.26s | 1.28s | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_read_only` | 148.37ms | 162.09ms | 1.1× | 0.7% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 26.04ms | 92.66ms | 3.6× | 13.5% | PASS |
| ac_writes | `oltp_insert_ac` | 29.05ms | 110.75ms | 3.8× | 16.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 31.28ms | 116.58ms | 3.7× | 16.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 27.10ms | 106.80ms | 3.9× | 13.5% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 27.51ms | 113.75ms | 4.1× | 19.5% | PASS |
| ac_writes | `oltp_write_only_ac` | 27.44ms | 102.64ms | 3.7× | 11.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 28.29ms | 110.62ms | 3.9× | 18.6% | PASS |
| ac_writes | `oltp_read_write_ac` | 36.09ms | 132.78ms | 3.7× | 14.6% | PASS |

</details>

</details>

## Version-control latency

Wall time: 1m 58s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 68.06ms | 130.00ms | 52.3% | 0.6% | PASS |
| `status_dirty_many_tables` | 70.38ms | 130.00ms | 54.1% | 0.3% | PASS |
| `diff_regular_working_one_table` | 62.30ms | 120.00ms | 51.9% | 0.4% | PASS |
| `diff_regular_working_many_tables` | 74.99ms | 140.00ms | 53.6% | 0.4% | PASS |
| `diff_stat_working_many_tables` | 75.29ms | 140.00ms | 53.8% | 0.3% | PASS |
| `diff_schema_working_many_tables` | 75.43ms | 140.00ms | 53.9% | 0.3% | PASS |
| `branch_list_many_branches` | 21.25ms | 35.00ms | 60.7% | 0.8% | PASS |
| `branch_create_delete` | 32.42ms | 40.00ms | 81.1% | 1.7% | PASS |
| `checkout_branch_clean` | 116.69ms | 150.00ms | 77.8% | 8.6% | PASS |
| `merge_data_no_conflicts` | 38.23ms | 50.00ms | 76.5% | 2.0% | PASS |
| `merge_schema_no_conflicts` | 22.32ms | 35.00ms | 63.8% | 3.1% | PASS |
| `merge_data_conflicts` | 28.64ms | 180.00ms | 15.9% | 0.6% | PASS |
| `merge_data_conflicts_with_resolve` | 28.81ms | 180.00ms | 16.0% | 0.8% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
