# DoltLite Performance Report

> Nightly result: **FAIL**
>
> Generated: 2026-09-10 11:12 UTC
>
> Commit: [`5b9f983f8df63a11e2214cc95b9b1125b83ffcd6`](https://github.com/dolthub/doltlite/commit/5b9f983f8df63a11e2214cc95b9b1125b83ffcd6)
>
> Runner: ubuntu24 20260831.293.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/34461522768)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.72s | 10.22s | 1.1× | 1.6% | **PASS** |
| Writes | 2.04s | 3.37s | 1.7× | 1.8% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.51s | 10.42s | 1.0× | 1.5% | **PASS** |
| Writes | 3.64s | 4.16s | 1.1× | 2.6% | **PASS** |
| Autocommit writes | 2.80s | 8.20s | 2.9× | 6.7% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.57s | 2.66s | 1.0× | 1.8% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.73s | 2.82s | 1.0× | 1.7% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.60s | 2.79s | 1.1× | 1.6% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 1.82s | 1.96s | 1.1× | 1.2% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 443.38ms | 746.42ms | 1.7× | 2.1% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 608.47ms | 1.03s | 1.7× | 2.0% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 627.48ms | 1.02s | 1.6× | 1.9% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 359.87ms | 569.76ms | 1.6× | 0.9% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.80s | 2.72s | 1.0× | 1.7% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.98s | 2.88s | 1.0× | 1.9% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.84s | 2.85s | 1.0× | 1.4% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 1.89s | 1.97s | 1.0× | 1.6% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 619.72ms | 815.29ms | 1.3× | 2.2% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 937.60ms | 1.12s | 1.2× | 3.8% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 836.21ms | 1.11s | 1.3× | 2.1% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 1.25s | 1.11s | 0.9× | 34.2% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.64s | 2.72s | 1.0× | 2.1% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.84s | 2.88s | 1.0× | 1.7% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.64s | 2.83s | 1.1× | 1.6% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 1.83s | 1.96s | 1.1× | 1.6% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 201.63ms | 761.92ms | 3.8× | 5.9% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 225.50ms | 791.11ms | 3.5× | 6.7% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 214.92ms | 741.00ms | 3.4× | 6.5% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 2.16s | 5.91s | 2.7× | 60.3% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 23.63ms | 29.61ms | 1.3× | 1.2% | PASS |
| mem_reads | `oltp_range_select` | 10.32ms | 11.23ms | 1.1× | 2.6% | PASS |
| mem_reads | `oltp_sum_range` | 9.64ms | 11.30ms | 1.2× | 1.6% | PASS |
| mem_reads | `oltp_order_range` | 2.66ms | 2.86ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 3.71ms | 3.98ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 3.87ms | 5.31ms | 1.4× | 2.2% | PASS |
| mem_reads | `select_random_points` | 10.17ms | 11.14ms | 1.1× | 2.4% | PASS |
| mem_reads | `select_random_ranges` | 2.98ms | 3.97ms | 1.3× | 1.6% | PASS |
| mem_reads | `covering_index_scan` | 4.17ms | 4.26ms | 1.0× | 2.9% | PASS |
| mem_reads | `groupby_scan` | 30.57ms | 33.03ms | 1.1× | 0.7% | PASS |
| mem_reads | `index_join` | 5.82ms | 8.25ms | 1.4× | 3.1% | PASS |
| mem_reads | `index_join_scan` | 3.20ms | 4.61ms | 1.4× | 2.7% | PASS |
| mem_reads | `types_table_scan` | 1.10s | 1.16s | 1.1× | 1.3% | PASS |
| mem_reads | `table_scan` | 1.25s | 1.25s | 1.0× | 1.8% | PASS |
| mem_reads | `oltp_read_only` | 105.82ms | 118.69ms | 1.1× | 2.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 179.87ms | 274.28ms | 1.5× | 1.3% | PASS |
| mem_writes | `oltp_insert` | 15.21ms | 28.55ms | 1.9× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 50.34ms | 100.54ms | 2.0× | 2.5% | PASS |
| mem_writes | `oltp_update_non_index` | 34.52ms | 61.23ms | 1.8× | 2.4% | PASS |
| mem_writes | `oltp_delete_insert` | 45.31ms | 76.01ms | 1.7× | 2.6% | PASS |
| mem_writes | `oltp_write_only` | 22.15ms | 48.04ms | 2.2× | 2.3% | PASS |
| mem_writes | `types_delete_insert` | 24.43ms | 39.01ms | 1.6× | 1.9% | PASS |
| mem_writes | `oltp_read_write` | 71.54ms | 118.76ms | 1.7× | 1.8% | PASS |
| file_reads | `oltp_point_select` | 92.79ms | 48.17ms | 0.5× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 18.11ms | 13.46ms | 0.7× | 3.1% | PASS |
| file_reads | `oltp_sum_range` | 17.00ms | 13.37ms | 0.8× | 1.4% | PASS |
| file_reads | `oltp_order_range` | 3.58ms | 3.31ms | 0.9× | 4.2% | PASS |
| file_reads | `oltp_distinct_range` | 4.69ms | 4.48ms | 1.0× | 2.4% | PASS |
| file_reads | `oltp_index_scan` | 11.22ms | 7.51ms | 0.7× | 1.9% | PASS |
| file_reads | `select_random_points` | 18.02ms | 13.29ms | 0.7× | 2.4% | PASS |
| file_reads | `select_random_ranges` | 9.95ms | 5.88ms | 0.6× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 11.61ms | 6.48ms | 0.6× | 1.5% | PASS |
| file_reads | `groupby_scan` | 31.87ms | 33.63ms | 1.1× | 1.1% | PASS |
| file_reads | `index_join` | 9.86ms | 9.66ms | 1.0× | 2.3% | PASS |
| file_reads | `index_join_scan` | 4.18ms | 4.98ms | 1.2× | 2.5% | PASS |
| file_reads | `types_table_scan` | 1.09s | 1.15s | 1.1× | 1.5% | PASS |
| file_reads | `table_scan` | 1.27s | 1.25s | 1.0× | 1.7% | PASS |
| file_reads | `oltp_read_only` | 211.52ms | 148.24ms | 0.7× | 1.3% | PASS |
| file_writes | `oltp_bulk_insert` | 192.09ms | 282.69ms | 1.5× | 1.0% | PASS |
| file_writes | `oltp_insert` | 23.99ms | 32.28ms | 1.3× | 1.9% | PASS |
| file_writes | `oltp_update_index` | 79.19ms | 112.04ms | 1.4× | 2.6% | PASS |
| file_writes | `oltp_update_non_index` | 61.47ms | 73.47ms | 1.2× | 2.1% | PASS |
| file_writes | `oltp_delete_insert` | 70.87ms | 86.09ms | 1.2× | 2.4% | PASS |
| file_writes | `oltp_write_only` | 49.70ms | 56.77ms | 1.1× | 2.5% | PASS |
| file_writes | `types_delete_insert` | 40.01ms | 44.95ms | 1.1× | 1.8% | PASS |
| file_writes | `oltp_read_write` | 102.40ms | 127.01ms | 1.2× | 2.4% | PASS |
| ac_reads | `oltp_point_select` | 47.78ms | 48.95ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 13.66ms | 13.45ms | 1.0× | 2.1% | PASS |
| ac_reads | `oltp_sum_range` | 12.41ms | 13.35ms | 1.1× | 2.0% | PASS |
| ac_reads | `oltp_order_range` | 3.23ms | 3.34ms | 1.0× | 2.7% | PASS |
| ac_reads | `oltp_distinct_range` | 4.23ms | 4.48ms | 1.1× | 3.0% | PASS |
| ac_reads | `oltp_index_scan` | 6.58ms | 7.49ms | 1.1× | 2.7% | PASS |
| ac_reads | `select_random_points` | 13.31ms | 13.24ms | 1.0× | 3.1% | PASS |
| ac_reads | `select_random_ranges` | 5.41ms | 5.89ms | 1.1× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 7.04ms | 6.44ms | 0.9× | 2.4% | PASS |
| ac_reads | `groupby_scan` | 31.33ms | 33.71ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 7.45ms | 9.69ms | 1.3× | 2.5% | PASS |
| ac_reads | `index_join_scan` | 3.70ms | 4.92ms | 1.3× | 2.8% | PASS |
| ac_reads | `types_table_scan` | 1.09s | 1.15s | 1.1× | 1.3% | PASS |
| ac_reads | `table_scan` | 1.26s | 1.25s | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_read_only` | 142.03ms | 146.76ms | 1.0× | 1.6% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.15ms | 81.62ms | 3.7× | 7.1% | PASS |
| ac_writes | `oltp_insert_ac` | 24.90ms | 94.10ms | 3.8× | 6.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.18ms | 108.36ms | 4.0× | 5.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.20ms | 87.33ms | 3.8× | 5.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.21ms | 100.08ms | 3.8× | 7.2% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.35ms | 98.37ms | 3.9× | 5.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.50ms | 89.18ms | 4.0× | 6.0% | PASS |
| ac_writes | `oltp_read_write_ac` | 30.14ms | 102.88ms | 3.4× | 5.3% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 35.25ms | 38.91ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_range_select` | 16.93ms | 15.18ms | 0.9× | 2.2% | PASS |
| mem_reads | `oltp_sum_range` | 15.56ms | 14.10ms | 0.9× | 1.6% | PASS |
| mem_reads | `oltp_order_range` | 3.27ms | 3.22ms | 1.0× | 1.5% | PASS |
| mem_reads | `oltp_distinct_range` | 4.35ms | 4.39ms | 1.0× | 0.7% | PASS |
| mem_reads | `oltp_index_scan` | 4.00ms | 6.32ms | 1.6× | 1.6% | PASS |
| mem_reads | `select_random_points` | 21.97ms | 21.21ms | 1.0× | 3.3% | PASS |
| mem_reads | `select_random_ranges` | 3.60ms | 5.30ms | 1.5× | 1.3% | PASS |
| mem_reads | `covering_index_scan` | 4.22ms | 4.67ms | 1.1× | 1.7% | PASS |
| mem_reads | `groupby_scan` | 34.82ms | 34.80ms | 1.0× | 0.8% | PASS |
| mem_reads | `index_join` | 10.53ms | 9.53ms | 0.9× | 2.7% | PASS |
| mem_reads | `index_join_scan` | 3.74ms | 5.48ms | 1.5× | 2.0% | PASS |
| mem_reads | `types_table_scan` | 1.13s | 1.21s | 1.1× | 2.0% | PASS |
| mem_reads | `table_scan` | 1.29s | 1.30s | 1.0× | 2.2% | PASS |
| mem_reads | `oltp_read_only` | 144.57ms | 142.72ms | 1.0× | 2.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 245.14ms | 374.82ms | 1.5× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 18.23ms | 39.50ms | 2.2× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 68.67ms | 151.36ms | 2.2× | 2.4% | PASS |
| mem_writes | `oltp_update_non_index` | 50.05ms | 88.14ms | 1.8× | 3.2% | PASS |
| mem_writes | `oltp_delete_insert` | 55.22ms | 107.24ms | 1.9× | 2.0% | PASS |
| mem_writes | `oltp_write_only` | 28.96ms | 64.57ms | 2.2× | 1.5% | PASS |
| mem_writes | `types_delete_insert` | 40.98ms | 56.13ms | 1.4× | 2.1% | PASS |
| mem_writes | `oltp_read_write` | 101.23ms | 150.04ms | 1.5× | 2.2% | PASS |
| file_reads | `oltp_point_select` | 106.23ms | 57.87ms | 0.5× | 1.1% | PASS |
| file_reads | `oltp_range_select` | 24.11ms | 17.10ms | 0.7× | 2.6% | PASS |
| file_reads | `oltp_sum_range` | 23.71ms | 16.24ms | 0.7× | 1.9% | PASS |
| file_reads | `oltp_order_range` | 4.14ms | 3.55ms | 0.9× | 2.2% | PASS |
| file_reads | `oltp_distinct_range` | 5.22ms | 4.74ms | 0.9× | 1.5% | PASS |
| file_reads | `oltp_index_scan` | 11.37ms | 8.51ms | 0.7× | 1.5% | PASS |
| file_reads | `select_random_points` | 33.65ms | 24.55ms | 0.7× | 2.9% | PASS |
| file_reads | `select_random_ranges` | 10.91ms | 7.25ms | 0.7× | 1.5% | PASS |
| file_reads | `covering_index_scan` | 11.91ms | 6.90ms | 0.6× | 1.4% | PASS |
| file_reads | `groupby_scan` | 36.67ms | 35.60ms | 1.0× | 0.8% | PASS |
| file_reads | `index_join` | 15.87ms | 11.47ms | 0.7× | 2.4% | PASS |
| file_reads | `index_join_scan` | 4.77ms | 5.90ms | 1.2× | 2.5% | PASS |
| file_reads | `types_table_scan` | 1.14s | 1.20s | 1.1× | 2.5% | PASS |
| file_reads | `table_scan` | 1.30s | 1.30s | 1.0× | 2.8% | PASS |
| file_reads | `oltp_read_only` | 252.46ms | 171.47ms | 0.7× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 270.66ms | 389.04ms | 1.4× | 0.8% | PASS |
| file_writes | `oltp_insert` | 25.94ms | 45.77ms | 1.8× | 1.8% | PASS |
| file_writes | `oltp_update_index` | 136.84ms | 164.61ms | 1.2× | 5.2% | PASS |
| file_writes | `oltp_update_non_index` | 98.99ms | 102.77ms | 1.0× | 10.7% | PASS |
| file_writes | `oltp_delete_insert` | 96.93ms | 121.67ms | 1.3× | 1.7% | PASS |
| file_writes | `oltp_write_only` | 91.62ms | 75.27ms | 0.8× | 8.6% | PASS |
| file_writes | `types_delete_insert` | 71.87ms | 64.18ms | 0.9× | 2.3% | PASS |
| file_writes | `oltp_read_write` | 144.75ms | 160.68ms | 1.1× | 5.9% | PASS |
| ac_reads | `oltp_point_select` | 59.99ms | 57.66ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_range_select` | 19.82ms | 17.19ms | 0.9× | 2.1% | PASS |
| ac_reads | `oltp_sum_range` | 18.73ms | 16.30ms | 0.9× | 1.9% | PASS |
| ac_reads | `oltp_order_range` | 3.81ms | 3.57ms | 0.9× | 2.0% | PASS |
| ac_reads | `oltp_distinct_range` | 4.83ms | 4.76ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_index_scan` | 6.73ms | 8.57ms | 1.3× | 1.2% | PASS |
| ac_reads | `select_random_points` | 26.52ms | 24.36ms | 0.9× | 2.7% | PASS |
| ac_reads | `select_random_ranges` | 6.14ms | 7.23ms | 1.2× | 1.1% | PASS |
| ac_reads | `covering_index_scan` | 6.99ms | 6.78ms | 1.0× | 1.3% | PASS |
| ac_reads | `groupby_scan` | 35.16ms | 35.07ms | 1.0× | 0.7% | PASS |
| ac_reads | `index_join` | 12.65ms | 11.06ms | 0.9× | 1.9% | PASS |
| ac_reads | `index_join_scan` | 4.28ms | 5.91ms | 1.4× | 2.2% | PASS |
| ac_reads | `types_table_scan` | 1.16s | 1.21s | 1.0× | 2.0% | PASS |
| ac_reads | `table_scan` | 1.30s | 1.30s | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_read_only` | 177.86ms | 170.91ms | 1.0× | 1.7% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 26.20ms | 83.04ms | 3.2× | 8.0% | PASS |
| ac_writes | `oltp_insert_ac` | 28.13ms | 94.75ms | 3.4× | 6.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 28.90ms | 108.59ms | 3.8× | 6.3% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.71ms | 94.31ms | 3.8× | 6.9% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 29.29ms | 102.76ms | 3.5× | 6.4% | PASS |
| ac_writes | `oltp_write_only_ac` | 27.50ms | 100.39ms | 3.7× | 6.7% | PASS |
| ac_writes | `types_delete_insert_ac` | 26.56ms | 100.53ms | 3.8× | 6.8% | PASS |
| ac_writes | `oltp_read_write_ac` | 34.22ms | 106.74ms | 3.1× | 5.4% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 38.42ms | 38.66ms | 1.0× | 1.8% | PASS |
| mem_reads | `oltp_range_select` | 17.51ms | 15.02ms | 0.9× | 1.8% | PASS |
| mem_reads | `oltp_sum_range` | 16.26ms | 14.09ms | 0.9× | 1.5% | PASS |
| mem_reads | `oltp_order_range` | 3.19ms | 3.14ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 4.34ms | 4.32ms | 1.0× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 4.11ms | 6.36ms | 1.5× | 1.6% | PASS |
| mem_reads | `select_random_points` | 22.37ms | 20.94ms | 0.9× | 1.6% | PASS |
| mem_reads | `select_random_ranges` | 3.68ms | 5.36ms | 1.5× | 1.5% | PASS |
| mem_reads | `covering_index_scan` | 4.25ms | 4.80ms | 1.1× | 2.8% | PASS |
| mem_reads | `groupby_scan` | 35.12ms | 34.95ms | 1.0× | 0.7% | PASS |
| mem_reads | `index_join` | 11.16ms | 9.97ms | 0.9× | 3.3% | PASS |
| mem_reads | `index_join_scan` | 3.98ms | 5.61ms | 1.4× | 3.1% | PASS |
| mem_reads | `types_table_scan` | 1.10s | 1.19s | 1.1× | 2.1% | PASS |
| mem_reads | `table_scan` | 1.20s | 1.29s | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_read_only` | 142.04ms | 140.29ms | 1.0× | 2.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 247.75ms | 373.44ms | 1.5× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 19.06ms | 39.78ms | 2.1× | 1.3% | PASS |
| mem_writes | `oltp_update_index` | 70.97ms | 145.51ms | 2.1× | 1.8% | PASS |
| mem_writes | `oltp_update_non_index` | 54.03ms | 87.86ms | 1.6× | 1.9% | PASS |
| mem_writes | `oltp_delete_insert` | 56.63ms | 107.49ms | 1.9× | 2.1% | PASS |
| mem_writes | `oltp_write_only` | 28.73ms | 62.38ms | 2.2× | 1.9% | PASS |
| mem_writes | `types_delete_insert` | 39.05ms | 52.96ms | 1.4× | 1.9% | PASS |
| mem_writes | `oltp_read_write` | 111.27ms | 152.87ms | 1.4× | 2.9% | PASS |
| file_reads | `oltp_point_select` | 107.46ms | 57.83ms | 0.5× | 1.4% | PASS |
| file_reads | `oltp_range_select` | 24.36ms | 16.86ms | 0.7× | 2.0% | PASS |
| file_reads | `oltp_sum_range` | 22.76ms | 15.98ms | 0.7× | 1.8% | PASS |
| file_reads | `oltp_order_range` | 4.25ms | 3.46ms | 0.8× | 1.7% | PASS |
| file_reads | `oltp_distinct_range` | 5.33ms | 4.68ms | 0.9× | 1.5% | PASS |
| file_reads | `oltp_index_scan` | 11.17ms | 8.36ms | 0.7× | 1.2% | PASS |
| file_reads | `select_random_points` | 30.53ms | 23.90ms | 0.8× | 2.1% | PASS |
| file_reads | `select_random_ranges` | 10.61ms | 7.29ms | 0.7× | 0.7% | PASS |
| file_reads | `covering_index_scan` | 11.33ms | 6.78ms | 0.6× | 1.1% | PASS |
| file_reads | `groupby_scan` | 36.13ms | 35.20ms | 1.0× | 1.1% | PASS |
| file_reads | `index_join` | 14.74ms | 11.12ms | 0.8× | 3.0% | PASS |
| file_reads | `index_join_scan` | 4.84ms | 5.87ms | 1.2× | 2.2% | PASS |
| file_reads | `types_table_scan` | 1.08s | 1.19s | 1.1× | 0.8% | PASS |
| file_reads | `table_scan` | 1.22s | 1.30s | 1.1× | 1.1% | PASS |
| file_reads | `oltp_read_only` | 255.71ms | 170.82ms | 0.7× | 1.4% | PASS |
| file_writes | `oltp_bulk_insert` | 270.52ms | 384.11ms | 1.4× | 0.9% | PASS |
| file_writes | `oltp_insert` | 26.11ms | 46.39ms | 1.8× | 1.5% | PASS |
| file_writes | `oltp_update_index` | 99.60ms | 156.85ms | 1.6× | 2.0% | PASS |
| file_writes | `oltp_update_non_index` | 87.79ms | 99.53ms | 1.1× | 7.8% | PASS |
| file_writes | `oltp_delete_insert` | 93.94ms | 122.13ms | 1.3× | 2.3% | PASS |
| file_writes | `oltp_write_only` | 57.84ms | 73.60ms | 1.3× | 1.8% | PASS |
| file_writes | `types_delete_insert` | 65.87ms | 63.79ms | 1.0× | 3.1% | PASS |
| file_writes | `oltp_read_write` | 134.54ms | 159.55ms | 1.2× | 3.5% | PASS |
| ac_reads | `oltp_point_select` | 59.03ms | 57.51ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_range_select` | 19.32ms | 16.89ms | 0.9× | 1.5% | PASS |
| ac_reads | `oltp_sum_range` | 18.07ms | 16.01ms | 0.9× | 1.7% | PASS |
| ac_reads | `oltp_order_range` | 3.75ms | 3.49ms | 0.9× | 2.2% | PASS |
| ac_reads | `oltp_distinct_range` | 4.87ms | 4.70ms | 1.0× | 2.0% | PASS |
| ac_reads | `oltp_index_scan` | 6.44ms | 8.36ms | 1.3× | 1.6% | PASS |
| ac_reads | `select_random_points` | 25.16ms | 23.94ms | 1.0× | 2.8% | PASS |
| ac_reads | `select_random_ranges` | 5.88ms | 7.27ms | 1.2× | 1.7% | PASS |
| ac_reads | `covering_index_scan` | 6.74ms | 6.75ms | 1.0× | 1.4% | PASS |
| ac_reads | `groupby_scan` | 34.84ms | 35.02ms | 1.0× | 0.4% | PASS |
| ac_reads | `index_join` | 11.93ms | 10.97ms | 0.9× | 2.4% | PASS |
| ac_reads | `index_join_scan` | 4.38ms | 5.89ms | 1.3× | 2.0% | PASS |
| ac_reads | `types_table_scan` | 1.07s | 1.18s | 1.1× | 0.4% | PASS |
| ac_reads | `table_scan` | 1.19s | 1.28s | 1.1× | 0.5% | PASS |
| ac_reads | `oltp_read_only` | 178.08ms | 168.12ms | 0.9× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 24.79ms | 76.87ms | 3.1× | 5.9% | PASS |
| ac_writes | `oltp_insert_ac` | 26.51ms | 93.43ms | 3.5× | 6.7% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.80ms | 102.58ms | 3.7× | 5.9% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.07ms | 85.35ms | 3.5× | 7.1% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.76ms | 95.92ms | 3.6× | 6.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 27.17ms | 96.11ms | 3.5× | 4.5% | PASS |
| ac_writes | `types_delete_insert_ac` | 24.57ms | 85.95ms | 3.5× | 6.6% | PASS |
| ac_writes | `oltp_read_write_ac` | 33.25ms | 104.78ms | 3.2× | 7.1% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 20.51ms | 24.22ms | 1.2× | 1.2% | PASS |
| mem_reads | `oltp_range_select` | 13.42ms | 13.53ms | 1.0× | 1.4% | PASS |
| mem_reads | `oltp_sum_range` | 12.97ms | 12.94ms | 1.0× | 1.6% | PASS |
| mem_reads | `oltp_order_range` | 2.52ms | 2.55ms | 1.0× | 2.8% | PASS |
| mem_reads | `oltp_distinct_range` | 3.14ms | 3.35ms | 1.1× | 2.0% | PASS |
| mem_reads | `oltp_index_scan` | 3.07ms | 3.97ms | 1.3× | 1.6% | PASS |
| mem_reads | `select_random_points` | 19.81ms | 22.07ms | 1.1× | 0.9% | PASS |
| mem_reads | `select_random_ranges` | 4.89ms | 5.69ms | 1.2× | 1.1% | PASS |
| mem_reads | `covering_index_scan` | 2.47ms | 2.55ms | 1.0× | 1.0% | PASS |
| mem_reads | `groupby_scan` | 22.84ms | 26.63ms | 1.2× | 1.0% | PASS |
| mem_reads | `index_join` | 5.41ms | 7.01ms | 1.3× | 0.8% | PASS |
| mem_reads | `index_join_scan` | 2.78ms | 4.35ms | 1.6× | 2.5% | PASS |
| mem_reads | `types_table_scan` | 751.03ms | 820.68ms | 1.1× | 0.6% | PASS |
| mem_reads | `table_scan` | 867.39ms | 904.10ms | 1.0× | 2.1% | PASS |
| mem_reads | `oltp_read_only` | 91.73ms | 103.88ms | 1.1× | 1.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 141.98ms | 193.95ms | 1.4× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 11.73ms | 20.53ms | 1.8× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 43.53ms | 82.24ms | 1.9× | 0.8% | PASS |
| mem_writes | `oltp_update_non_index` | 31.32ms | 51.48ms | 1.6× | 0.9% | PASS |
| mem_writes | `oltp_delete_insert` | 32.16ms | 59.41ms | 1.8× | 1.2% | PASS |
| mem_writes | `oltp_write_only` | 17.73ms | 35.22ms | 2.0× | 1.6% | PASS |
| mem_writes | `types_delete_insert` | 19.91ms | 31.55ms | 1.6× | 0.9% | PASS |
| mem_writes | `oltp_read_write` | 61.52ms | 95.38ms | 1.6× | 1.4% | PASS |
| file_reads | `oltp_point_select` | 42.01ms | 29.62ms | 0.7× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 16.17ms | 14.64ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_sum_range` | 15.59ms | 13.93ms | 0.9× | 1.6% | PASS |
| file_reads | `oltp_order_range` | 2.76ms | 2.63ms | 1.0× | 1.6% | PASS |
| file_reads | `oltp_distinct_range` | 3.43ms | 3.37ms | 1.0× | 1.4% | PASS |
| file_reads | `oltp_index_scan` | 5.52ms | 4.95ms | 0.9× | 2.7% | PASS |
| file_reads | `select_random_points` | 22.25ms | 23.02ms | 1.0× | 1.1% | PASS |
| file_reads | `select_random_ranges` | 7.25ms | 6.41ms | 0.9× | 1.6% | PASS |
| file_reads | `covering_index_scan` | 4.67ms | 3.28ms | 0.7× | 5.5% | PASS |
| file_reads | `groupby_scan` | 23.20ms | 26.17ms | 1.1× | 1.8% | PASS |
| file_reads | `index_join` | 6.76ms | 8.03ms | 1.2× | 3.3% | PASS |
| file_reads | `index_join_scan` | 3.06ms | 4.51ms | 1.5× | 3.8% | PASS |
| file_reads | `types_table_scan` | 749.95ms | 818.55ms | 1.1× | 0.8% | PASS |
| file_reads | `table_scan` | 864.99ms | 898.75ms | 1.0× | 1.4% | PASS |
| file_reads | `oltp_read_only` | 120.89ms | 111.32ms | 0.9× | 1.2% | PASS |
| file_writes | `oltp_bulk_insert` | 265.22ms | 329.15ms | 1.2× | 31.6% | PASS |
| file_writes | `oltp_insert` | 94.95ms | 76.17ms | 0.8× | 70.9% | PASS |
| file_writes | `oltp_update_index` | 172.97ms | 158.06ms | 0.9× | 22.6% | PASS |
| file_writes | `oltp_update_non_index` | 177.22ms | 105.25ms | 0.6× | 38.2% | PASS |
| file_writes | `oltp_delete_insert` | 172.90ms | 136.50ms | 0.8× | 40.4% | PASS |
| file_writes | `oltp_write_only` | 163.22ms | 93.85ms | 0.6× | 36.9% | PASS |
| file_writes | `types_delete_insert` | 73.24ms | 68.41ms | 0.9× | 27.9% | PASS |
| file_writes | `oltp_read_write` | 130.36ms | 146.17ms | 1.1× | 11.9% | PASS |
| ac_reads | `oltp_point_select` | 26.78ms | 28.40ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_range_select` | 13.61ms | 14.12ms | 1.0× | 2.5% | PASS |
| ac_reads | `oltp_sum_range` | 13.67ms | 13.72ms | 1.0× | 2.4% | PASS |
| ac_reads | `oltp_order_range` | 2.55ms | 2.58ms | 1.0× | 2.5% | PASS |
| ac_reads | `oltp_distinct_range` | 3.07ms | 3.23ms | 1.1× | 1.6% | PASS |
| ac_reads | `oltp_index_scan` | 3.70ms | 4.24ms | 1.1× | 2.5% | PASS |
| ac_reads | `select_random_points` | 19.89ms | 21.75ms | 1.1× | 1.9% | PASS |
| ac_reads | `select_random_ranges` | 5.48ms | 5.97ms | 1.1× | 1.6% | PASS |
| ac_reads | `covering_index_scan` | 3.15ms | 3.02ms | 1.0× | 1.3% | PASS |
| ac_reads | `groupby_scan` | 22.60ms | 26.04ms | 1.2× | 0.8% | PASS |
| ac_reads | `index_join` | 5.82ms | 7.48ms | 1.3× | 2.3% | PASS |
| ac_reads | `index_join_scan` | 2.85ms | 4.31ms | 1.5× | 2.8% | PASS |
| ac_reads | `types_table_scan` | 749.77ms | 819.54ms | 1.1× | 0.7% | PASS |
| ac_reads | `table_scan` | 861.24ms | 899.13ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_read_only` | 100.22ms | 111.00ms | 1.1× | 1.1% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 366.98ms | 766.79ms | 2.1× | 68.0% | PASS |
| ac_writes | `oltp_insert_ac` | 190.68ms | 522.68ms | 2.7× | 59.8% | PASS |
| ac_writes | `oltp_update_index_ac` | 289.84ms | 905.06ms | 3.1× | 63.5% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 342.80ms | 833.93ms | 2.4× | 57.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 233.22ms | 708.47ms | 3.0× | 60.8% | PASS |
| ac_writes | `oltp_write_only_ac` | 318.20ms | 702.90ms | 2.2× | 38.7% | PASS |
| ac_writes | `types_delete_insert_ac` | 181.05ms | 782.48ms | 4.3× | 44.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 233.14ms | 687.15ms | 2.9× | 62.3% | PASS |

</details>

</details>

## Version-control latency

Wall time: 2m 5s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 49.94ms | 130.00ms | 38.4% | 0.3% | PASS |
| `status_dirty_many_tables` | 52.17ms | 130.00ms | 40.1% | 0.4% | PASS |
| `diff_regular_working_one_table` | 47.15ms | 120.00ms | 39.3% | 0.4% | PASS |
| `diff_regular_working_many_tables` | 55.18ms | 140.00ms | 39.4% | 0.4% | PASS |
| `diff_stat_working_many_tables` | 56.59ms | 140.00ms | 40.4% | 0.7% | PASS |
| `diff_schema_working_many_tables` | 56.53ms | 140.00ms | 40.4% | 1.3% | PASS |
| `branch_list_many_branches` | 14.57ms | 35.00ms | 41.6% | 1.3% | PASS |
| `branch_create_delete` | 18.52ms | 40.00ms | 46.3% | 6.5% | PASS |
| `at_literal_deep_history` | 22.06ms | 100.00ms | 22.1% | 1.2% | PASS |
| `diff_literal_deep_history` | 21.60ms | 120.00ms | 18.0% | 0.9% | PASS |
| `history_literal_deep_history` | 22.71ms | 150.00ms | 15.1% | 1.4% | PASS |
| `checkout_branch_clean` | 105.25ms | 150.00ms | 70.2% | 18.7% | PASS |
| `merge_data_no_conflicts` | 66.84ms | 50.00ms | 133.7% | 54.0% | FAIL |
| `merge_schema_no_conflicts` | 82.89ms | 35.00ms | 236.8% | 59.7% | FAIL |
| `merge_data_conflicts` | 19.95ms | 180.00ms | 11.1% | 0.5% | PASS |
| `merge_data_conflicts_with_resolve` | 19.95ms | 180.00ms | 11.1% | 0.8% | PASS |

Version-control ceiling result: **FAIL**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
