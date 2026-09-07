# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-09-07 11:13 UTC
>
> Commit: [`d8296828cef8027739e4c77df6a68340368030e1`](https://github.com/dolthub/doltlite/commit/d8296828cef8027739e4c77df6a68340368030e1)
>
> Runner: ubuntu24 20260831.293.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/34107286110)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.77s | 11.28s | 1.0× | 1.4% | **PASS** |
| Writes | 2.16s | 3.59s | 1.7× | 1.3% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 11.72s | 11.54s | 1.0× | 1.5% | **PASS** |
| Writes | 2.95s | 3.93s | 1.3× | 1.7% | **PASS** |
| Autocommit writes | 653.50ms | 2.46s | 3.8× | 5.7% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.64s | 2.74s | 1.0× | 1.2% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.58s | 2.75s | 1.1× | 1.1% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.74s | 2.88s | 1.1× | 1.5% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.82s | 2.91s | 1.0× | 1.9% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 450.99ms | 719.44ms | 1.6× | 1.5% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 520.15ms | 894.08ms | 1.7× | 1.3% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 584.91ms | 973.65ms | 1.7× | 1.1% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 607.76ms | 1.00s | 1.6× | 1.7% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.90s | 2.81s | 1.0× | 1.3% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.69s | 2.78s | 1.0× | 1.5% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 3.07s | 2.97s | 1.0× | 1.5% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 3.06s | 2.97s | 1.0× | 1.8% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 590.07ms | 770.90ms | 1.3× | 1.6% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 751.33ms | 976.02ms | 1.3× | 4.7% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 818.44ms | 1.08s | 1.3× | 1.6% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 793.94ms | 1.10s | 1.4× | 1.7% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.74s | 2.82s | 1.0× | 1.2% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.60s | 2.77s | 1.1× | 1.5% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.98s | 3.04s | 1.0× | 1.2% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.95s | 2.99s | 1.0× | 1.4% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 146.04ms | 574.92ms | 3.9× | 5.7% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 147.58ms | 559.41ms | 3.8× | 5.7% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 157.83ms | 586.99ms | 3.7× | 5.2% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 202.05ms | 734.41ms | 3.6× | 6.3% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.62ms | 27.91ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_range_select` | 10.71ms | 11.15ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_sum_range` | 9.85ms | 11.07ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_order_range` | 2.74ms | 2.84ms | 1.0× | 1.4% | PASS |
| mem_reads | `oltp_distinct_range` | 3.77ms | 3.99ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 3.89ms | 4.87ms | 1.3× | 1.2% | PASS |
| mem_reads | `select_random_points` | 10.40ms | 11.03ms | 1.1× | 1.4% | PASS |
| mem_reads | `select_random_ranges` | 3.05ms | 3.98ms | 1.3× | 1.0% | PASS |
| mem_reads | `covering_index_scan` | 4.30ms | 4.18ms | 1.0× | 1.6% | PASS |
| mem_reads | `groupby_scan` | 32.34ms | 34.41ms | 1.1× | 0.8% | PASS |
| mem_reads | `index_join` | 5.77ms | 7.73ms | 1.3× | 1.6% | PASS |
| mem_reads | `index_join_scan` | 3.27ms | 4.61ms | 1.4× | 2.1% | PASS |
| mem_reads | `types_table_scan` | 1.14s | 1.19s | 1.0× | 0.9% | PASS |
| mem_reads | `table_scan` | 1.28s | 1.31s | 1.0× | 0.9% | PASS |
| mem_reads | `oltp_read_only` | 103.05ms | 114.01ms | 1.1× | 0.9% | PASS |
| mem_writes | `oltp_bulk_insert` | 182.72ms | 252.22ms | 1.4× | 1.7% | PASS |
| mem_writes | `oltp_insert` | 15.87ms | 28.75ms | 1.8× | 1.2% | PASS |
| mem_writes | `oltp_update_index` | 52.03ms | 100.80ms | 1.9× | 1.5% | PASS |
| mem_writes | `oltp_update_non_index` | 36.68ms | 61.40ms | 1.7× | 1.4% | PASS |
| mem_writes | `oltp_delete_insert` | 45.24ms | 75.13ms | 1.7× | 1.7% | PASS |
| mem_writes | `oltp_write_only` | 22.36ms | 49.59ms | 2.2× | 1.5% | PASS |
| mem_writes | `types_delete_insert` | 25.19ms | 38.08ms | 1.5× | 1.3% | PASS |
| mem_writes | `oltp_read_write` | 70.89ms | 113.47ms | 1.6× | 1.4% | PASS |
| file_reads | `oltp_point_select` | 107.81ms | 49.20ms | 0.5× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 18.91ms | 13.35ms | 0.7× | 2.3% | PASS |
| file_reads | `oltp_sum_range` | 17.76ms | 13.26ms | 0.7× | 1.3% | PASS |
| file_reads | `oltp_order_range` | 3.48ms | 3.13ms | 0.9× | 2.0% | PASS |
| file_reads | `oltp_distinct_range` | 4.59ms | 4.25ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 12.47ms | 7.42ms | 0.6× | 1.3% | PASS |
| file_reads | `select_random_points` | 19.01ms | 13.51ms | 0.7× | 2.6% | PASS |
| file_reads | `select_random_ranges` | 11.39ms | 6.14ms | 0.5× | 0.8% | PASS |
| file_reads | `covering_index_scan` | 12.90ms | 6.65ms | 0.5× | 1.0% | PASS |
| file_reads | `groupby_scan` | 33.25ms | 34.77ms | 1.0× | 1.3% | PASS |
| file_reads | `index_join` | 10.35ms | 9.48ms | 0.9× | 1.2% | PASS |
| file_reads | `index_join_scan` | 4.25ms | 4.95ms | 1.2× | 1.4% | PASS |
| file_reads | `types_table_scan` | 1.13s | 1.19s | 1.1× | 0.9% | PASS |
| file_reads | `table_scan` | 1.29s | 1.31s | 1.0× | 1.3% | PASS |
| file_reads | `oltp_read_only` | 224.35ms | 145.83ms | 0.7× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 197.31ms | 261.82ms | 1.3× | 2.0% | PASS |
| file_writes | `oltp_insert` | 21.86ms | 32.26ms | 1.5× | 1.8% | PASS |
| file_writes | `oltp_update_index` | 77.29ms | 107.68ms | 1.4× | 1.1% | PASS |
| file_writes | `oltp_update_non_index` | 58.10ms | 71.28ms | 1.2× | 1.8% | PASS |
| file_writes | `oltp_delete_insert` | 66.66ms | 82.77ms | 1.2× | 1.3% | PASS |
| file_writes | `oltp_write_only` | 42.88ms | 55.64ms | 1.3× | 1.5% | PASS |
| file_writes | `types_delete_insert` | 38.81ms | 42.82ms | 1.1× | 1.4% | PASS |
| file_writes | `oltp_read_write` | 87.16ms | 116.63ms | 1.3× | 2.0% | PASS |
| ac_reads | `oltp_point_select` | 52.69ms | 49.73ms | 0.9× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 14.12ms | 13.40ms | 0.9× | 1.3% | PASS |
| ac_reads | `oltp_sum_range` | 12.94ms | 13.30ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_order_range` | 3.13ms | 3.14ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_distinct_range` | 4.08ms | 4.24ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_index_scan` | 7.00ms | 7.35ms | 1.0× | 1.4% | PASS |
| ac_reads | `select_random_points` | 13.52ms | 13.32ms | 1.0× | 1.2% | PASS |
| ac_reads | `select_random_ranges` | 5.93ms | 6.13ms | 1.0× | 1.1% | PASS |
| ac_reads | `covering_index_scan` | 7.36ms | 6.53ms | 0.9× | 1.6% | PASS |
| ac_reads | `groupby_scan` | 32.59ms | 35.29ms | 1.1× | 1.0% | PASS |
| ac_reads | `index_join` | 7.57ms | 9.33ms | 1.2× | 1.0% | PASS |
| ac_reads | `index_join_scan` | 3.76ms | 4.95ms | 1.3× | 1.8% | PASS |
| ac_reads | `types_table_scan` | 1.12s | 1.19s | 1.1× | 0.7% | PASS |
| ac_reads | `table_scan` | 1.31s | 1.32s | 1.0× | 2.0% | PASS |
| ac_reads | `oltp_read_only` | 145.21ms | 145.59ms | 1.0× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 16.46ms | 57.84ms | 3.5× | 4.5% | PASS |
| ac_writes | `oltp_insert_ac` | 18.30ms | 71.01ms | 3.9× | 6.2% | PASS |
| ac_writes | `oltp_update_index_ac` | 19.55ms | 82.69ms | 4.2× | 3.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 16.88ms | 64.99ms | 3.9× | 5.9% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 17.68ms | 74.94ms | 4.2× | 5.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 18.28ms | 76.97ms | 4.2× | 7.4% | PASS |
| ac_writes | `types_delete_insert_ac` | 15.39ms | 64.81ms | 4.2× | 6.8% | PASS |
| ac_writes | `oltp_read_write_ac` | 23.50ms | 81.67ms | 3.5× | 4.4% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 31.75ms | 33.33ms | 1.0× | 1.9% | PASS |
| mem_reads | `oltp_range_select` | 14.22ms | 12.57ms | 0.9× | 1.3% | PASS |
| mem_reads | `oltp_sum_range` | 13.71ms | 12.67ms | 0.9× | 1.5% | PASS |
| mem_reads | `oltp_order_range` | 2.92ms | 2.91ms | 1.0× | 0.8% | PASS |
| mem_reads | `oltp_distinct_range` | 3.67ms | 4.00ms | 1.1× | 0.6% | PASS |
| mem_reads | `oltp_index_scan` | 3.50ms | 5.46ms | 1.6× | 1.0% | PASS |
| mem_reads | `select_random_points` | 19.83ms | 20.49ms | 1.0× | 1.5% | PASS |
| mem_reads | `select_random_ranges` | 3.06ms | 4.56ms | 1.5× | 0.6% | PASS |
| mem_reads | `covering_index_scan` | 3.64ms | 4.07ms | 1.1× | 1.7% | PASS |
| mem_reads | `groupby_scan` | 30.76ms | 32.80ms | 1.1× | 0.3% | PASS |
| mem_reads | `index_join` | 9.49ms | 8.83ms | 0.9× | 1.6% | PASS |
| mem_reads | `index_join_scan` | 3.12ms | 5.20ms | 1.7× | 1.3% | PASS |
| mem_reads | `types_table_scan` | 1.05s | 1.18s | 1.1× | 0.5% | PASS |
| mem_reads | `table_scan` | 1.26s | 1.29s | 1.0× | 0.7% | PASS |
| mem_reads | `oltp_read_only` | 126.28ms | 129.48ms | 1.0× | 1.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 204.09ms | 325.35ms | 1.6× | 1.3% | PASS |
| mem_writes | `oltp_insert` | 15.84ms | 34.77ms | 2.2× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 60.04ms | 132.59ms | 2.2× | 1.2% | PASS |
| mem_writes | `oltp_update_non_index` | 43.30ms | 74.85ms | 1.7× | 1.1% | PASS |
| mem_writes | `oltp_delete_insert` | 47.82ms | 93.92ms | 2.0× | 1.3% | PASS |
| mem_writes | `oltp_write_only` | 24.39ms | 54.28ms | 2.2× | 1.0% | PASS |
| mem_writes | `types_delete_insert` | 35.05ms | 47.57ms | 1.4× | 1.3% | PASS |
| mem_writes | `oltp_read_write` | 89.64ms | 130.74ms | 1.5× | 1.3% | PASS |
| file_reads | `oltp_point_select` | 63.80ms | 42.89ms | 0.7× | 2.2% | PASS |
| file_reads | `oltp_range_select` | 17.56ms | 13.85ms | 0.8× | 1.4% | PASS |
| file_reads | `oltp_sum_range` | 17.18ms | 14.02ms | 0.8× | 1.6% | PASS |
| file_reads | `oltp_order_range` | 3.32ms | 3.13ms | 0.9× | 2.4% | PASS |
| file_reads | `oltp_distinct_range` | 4.09ms | 4.24ms | 1.0× | 1.5% | PASS |
| file_reads | `oltp_index_scan` | 6.78ms | 6.72ms | 1.0× | 1.7% | PASS |
| file_reads | `select_random_points` | 24.45ms | 22.08ms | 0.9× | 1.9% | PASS |
| file_reads | `select_random_ranges` | 6.17ms | 5.67ms | 0.9× | 1.7% | PASS |
| file_reads | `covering_index_scan` | 6.88ms | 5.20ms | 0.8× | 1.3% | PASS |
| file_reads | `groupby_scan` | 31.44ms | 33.19ms | 1.1× | 0.7% | PASS |
| file_reads | `index_join` | 11.75ms | 9.84ms | 0.8× | 2.0% | PASS |
| file_reads | `index_join_scan` | 3.59ms | 5.47ms | 1.5× | 1.2% | PASS |
| file_reads | `types_table_scan` | 1.06s | 1.19s | 1.1× | 0.8% | PASS |
| file_reads | `table_scan` | 1.26s | 1.29s | 1.0× | 0.6% | PASS |
| file_reads | `oltp_read_only` | 172.10ms | 144.32ms | 0.8× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 222.71ms | 339.19ms | 1.5× | 1.2% | PASS |
| file_writes | `oltp_insert` | 20.80ms | 41.05ms | 2.0× | 1.5% | PASS |
| file_writes | `oltp_update_index` | 103.61ms | 144.34ms | 1.4× | 7.8% | PASS |
| file_writes | `oltp_update_non_index` | 98.68ms | 86.50ms | 0.9× | 9.5% | PASS |
| file_writes | `oltp_delete_insert` | 71.15ms | 105.64ms | 1.5× | 1.6% | PASS |
| file_writes | `oltp_write_only` | 58.85ms | 64.03ms | 1.1× | 18.0% | PASS |
| file_writes | `types_delete_insert` | 52.86ms | 55.54ms | 1.1× | 1.7% | PASS |
| file_writes | `oltp_read_write` | 122.66ms | 139.72ms | 1.1× | 8.9% | PASS |
| ac_reads | `oltp_point_select` | 41.42ms | 42.80ms | 1.0× | 2.5% | PASS |
| ac_reads | `oltp_range_select` | 15.31ms | 13.90ms | 0.9× | 1.4% | PASS |
| ac_reads | `oltp_sum_range` | 14.81ms | 14.06ms | 0.9× | 1.3% | PASS |
| ac_reads | `oltp_order_range` | 3.08ms | 3.12ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_distinct_range` | 3.88ms | 4.26ms | 1.1× | 1.8% | PASS |
| ac_reads | `oltp_index_scan` | 4.75ms | 6.71ms | 1.4× | 1.3% | PASS |
| ac_reads | `select_random_points` | 22.06ms | 22.16ms | 1.0× | 2.0% | PASS |
| ac_reads | `select_random_ranges` | 4.15ms | 5.64ms | 1.4× | 1.9% | PASS |
| ac_reads | `covering_index_scan` | 4.79ms | 5.20ms | 1.1× | 1.5% | PASS |
| ac_reads | `groupby_scan` | 30.90ms | 33.01ms | 1.1× | 0.6% | PASS |
| ac_reads | `index_join` | 10.68ms | 9.78ms | 0.9× | 2.0% | PASS |
| ac_reads | `index_join_scan` | 3.37ms | 5.36ms | 1.6× | 1.7% | PASS |
| ac_reads | `types_table_scan` | 1.05s | 1.17s | 1.1× | 1.0% | PASS |
| ac_reads | `table_scan` | 1.26s | 1.28s | 1.0× | 0.5% | PASS |
| ac_reads | `oltp_read_only` | 140.96ms | 144.14ms | 1.0× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 16.35ms | 56.85ms | 3.5× | 4.6% | PASS |
| ac_writes | `oltp_insert_ac` | 19.56ms | 68.80ms | 3.5× | 7.1% | PASS |
| ac_writes | `oltp_update_index_ac` | 19.38ms | 80.02ms | 4.1× | 5.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 16.08ms | 64.47ms | 4.0× | 8.1% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 17.77ms | 71.12ms | 4.0× | 7.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 18.11ms | 72.86ms | 4.0× | 5.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 17.25ms | 67.02ms | 3.9× | 5.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 23.07ms | 78.27ms | 3.4× | 5.7% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 34.41ms | 34.60ms | 1.0× | 1.7% | PASS |
| mem_reads | `oltp_range_select` | 16.48ms | 13.48ms | 0.8× | 1.5% | PASS |
| mem_reads | `oltp_sum_range` | 15.26ms | 13.21ms | 0.9× | 1.6% | PASS |
| mem_reads | `oltp_order_range` | 3.38ms | 3.11ms | 0.9× | 1.3% | PASS |
| mem_reads | `oltp_distinct_range` | 4.41ms | 4.27ms | 1.0× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 3.97ms | 5.65ms | 1.4× | 2.0% | PASS |
| mem_reads | `select_random_points` | 21.40ms | 19.89ms | 0.9× | 2.2% | PASS |
| mem_reads | `select_random_ranges` | 3.50ms | 5.23ms | 1.5× | 1.0% | PASS |
| mem_reads | `covering_index_scan` | 4.30ms | 4.75ms | 1.1× | 1.7% | PASS |
| mem_reads | `groupby_scan` | 37.33ms | 37.41ms | 1.0× | 1.0% | PASS |
| mem_reads | `index_join` | 10.53ms | 9.08ms | 0.9× | 2.8% | PASS |
| mem_reads | `index_join_scan` | 3.80ms | 5.46ms | 1.4× | 1.5% | PASS |
| mem_reads | `types_table_scan` | 1.14s | 1.23s | 1.1× | 0.9% | PASS |
| mem_reads | `table_scan` | 1.30s | 1.36s | 1.0× | 1.0% | PASS |
| mem_reads | `oltp_read_only` | 138.73ms | 131.21ms | 0.9× | 1.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 246.36ms | 356.01ms | 1.4× | 1.3% | PASS |
| mem_writes | `oltp_insert` | 18.85ms | 38.77ms | 2.1× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 63.05ms | 137.45ms | 2.2× | 0.7% | PASS |
| mem_writes | `oltp_update_non_index` | 47.34ms | 82.71ms | 1.7× | 1.3% | PASS |
| mem_writes | `oltp_delete_insert` | 53.27ms | 105.83ms | 2.0× | 1.5% | PASS |
| mem_writes | `oltp_write_only` | 27.65ms | 62.85ms | 2.3× | 0.6% | PASS |
| mem_writes | `types_delete_insert` | 36.89ms | 51.38ms | 1.4× | 1.2% | PASS |
| mem_writes | `oltp_read_write` | 91.50ms | 138.63ms | 1.5× | 0.6% | PASS |
| file_reads | `oltp_point_select` | 118.69ms | 57.10ms | 0.5× | 0.6% | PASS |
| file_reads | `oltp_range_select` | 24.56ms | 15.94ms | 0.6× | 1.9% | PASS |
| file_reads | `oltp_sum_range` | 23.55ms | 15.77ms | 0.7× | 1.5% | PASS |
| file_reads | `oltp_order_range` | 4.21ms | 3.45ms | 0.8× | 2.0% | PASS |
| file_reads | `oltp_distinct_range` | 5.27ms | 4.58ms | 0.9× | 1.5% | PASS |
| file_reads | `oltp_index_scan` | 12.49ms | 8.31ms | 0.7× | 1.8% | PASS |
| file_reads | `select_random_points` | 30.23ms | 22.51ms | 0.7× | 2.4% | PASS |
| file_reads | `select_random_ranges` | 11.87ms | 7.53ms | 0.6× | 0.6% | PASS |
| file_reads | `covering_index_scan` | 12.93ms | 7.01ms | 0.5× | 1.2% | PASS |
| file_reads | `groupby_scan` | 37.27ms | 37.43ms | 1.0× | 1.1% | PASS |
| file_reads | `index_join` | 14.88ms | 10.53ms | 0.7× | 2.4% | PASS |
| file_reads | `index_join_scan` | 4.71ms | 5.78ms | 1.2× | 2.2% | PASS |
| file_reads | `types_table_scan` | 1.14s | 1.23s | 1.1× | 0.9% | PASS |
| file_reads | `table_scan` | 1.37s | 1.38s | 1.0× | 3.3% | PASS |
| file_reads | `oltp_read_only` | 258.82ms | 163.57ms | 0.6× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 267.08ms | 364.23ms | 1.4× | 1.4% | PASS |
| file_writes | `oltp_insert` | 25.59ms | 46.13ms | 1.8× | 1.7% | PASS |
| file_writes | `oltp_update_index` | 97.94ms | 159.23ms | 1.6× | 1.7% | PASS |
| file_writes | `oltp_update_non_index` | 91.38ms | 100.59ms | 1.1× | 8.6% | PASS |
| file_writes | `oltp_delete_insert` | 91.64ms | 123.82ms | 1.4× | 1.4% | PASS |
| file_writes | `oltp_write_only` | 59.81ms | 76.46ms | 1.3× | 2.3% | PASS |
| file_writes | `types_delete_insert` | 64.20ms | 61.84ms | 1.0× | 1.5% | PASS |
| file_writes | `oltp_read_write` | 120.80ms | 149.57ms | 1.2× | 1.3% | PASS |
| ac_reads | `oltp_point_select` | 63.25ms | 57.46ms | 0.9× | 0.8% | PASS |
| ac_reads | `oltp_range_select` | 20.03ms | 16.16ms | 0.8× | 2.1% | PASS |
| ac_reads | `oltp_sum_range` | 18.72ms | 15.76ms | 0.8× | 1.1% | PASS |
| ac_reads | `oltp_order_range` | 3.78ms | 3.47ms | 0.9× | 1.5% | PASS |
| ac_reads | `oltp_distinct_range` | 4.83ms | 4.61ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_index_scan` | 7.09ms | 8.44ms | 1.2× | 1.0% | PASS |
| ac_reads | `select_random_points` | 25.00ms | 22.59ms | 0.9× | 1.2% | PASS |
| ac_reads | `select_random_ranges` | 6.42ms | 7.56ms | 1.2× | 0.7% | PASS |
| ac_reads | `covering_index_scan` | 7.41ms | 7.02ms | 0.9× | 1.0% | PASS |
| ac_reads | `groupby_scan` | 37.15ms | 37.39ms | 1.0× | 1.0% | PASS |
| ac_reads | `index_join` | 12.86ms | 10.62ms | 0.8× | 2.0% | PASS |
| ac_reads | `index_join_scan` | 4.47ms | 6.02ms | 1.3× | 1.7% | PASS |
| ac_reads | `types_table_scan` | 1.21s | 1.27s | 1.1× | 3.0% | PASS |
| ac_reads | `table_scan` | 1.38s | 1.40s | 1.0× | 2.3% | PASS |
| ac_reads | `oltp_read_only` | 186.24ms | 166.35ms | 0.9× | 1.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 17.49ms | 56.92ms | 3.3× | 4.2% | PASS |
| ac_writes | `oltp_insert_ac` | 19.96ms | 75.39ms | 3.8× | 6.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 20.26ms | 84.53ms | 4.2× | 5.0% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 17.28ms | 66.18ms | 3.8× | 4.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 19.40ms | 76.99ms | 4.0× | 5.0% | PASS |
| ac_writes | `oltp_write_only_ac` | 19.34ms | 74.66ms | 3.9× | 5.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 17.95ms | 69.41ms | 3.9× | 6.6% | PASS |
| ac_writes | `oltp_read_write_ac` | 26.15ms | 82.90ms | 3.2× | 5.4% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 34.44ms | 41.89ms | 1.2× | 1.8% | PASS |
| mem_reads | `oltp_range_select` | 20.84ms | 22.30ms | 1.1× | 2.0% | PASS |
| mem_reads | `oltp_sum_range` | 18.06ms | 21.15ms | 1.2× | 1.4% | PASS |
| mem_reads | `oltp_order_range` | 3.58ms | 3.98ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_distinct_range` | 4.75ms | 5.17ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_index_scan` | 4.66ms | 6.47ms | 1.4× | 2.6% | PASS |
| mem_reads | `select_random_points` | 28.58ms | 33.09ms | 1.2× | 2.4% | PASS |
| mem_reads | `select_random_ranges` | 7.87ms | 9.08ms | 1.2× | 1.4% | PASS |
| mem_reads | `covering_index_scan` | 4.26ms | 4.49ms | 1.1× | 2.3% | PASS |
| mem_reads | `groupby_scan` | 36.59ms | 40.40ms | 1.1× | 1.0% | PASS |
| mem_reads | `index_join` | 7.99ms | 10.75ms | 1.3× | 2.4% | PASS |
| mem_reads | `index_join_scan` | 3.93ms | 5.59ms | 1.4× | 2.8% | PASS |
| mem_reads | `types_table_scan` | 1.11s | 1.20s | 1.1× | 1.8% | PASS |
| mem_reads | `table_scan` | 1.37s | 1.33s | 1.0× | 2.1% | PASS |
| mem_reads | `oltp_read_only` | 157.68ms | 180.03ms | 1.1× | 1.9% | PASS |
| mem_writes | `oltp_bulk_insert` | 251.16ms | 367.01ms | 1.5× | 1.3% | PASS |
| mem_writes | `oltp_insert` | 19.19ms | 36.02ms | 1.9× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 66.91ms | 129.94ms | 1.9× | 2.4% | PASS |
| mem_writes | `oltp_update_non_index` | 51.55ms | 85.61ms | 1.7× | 1.8% | PASS |
| mem_writes | `oltp_delete_insert` | 50.48ms | 98.43ms | 1.9× | 2.0% | PASS |
| mem_writes | `oltp_write_only` | 27.32ms | 59.40ms | 2.2× | 1.7% | PASS |
| mem_writes | `types_delete_insert` | 33.16ms | 53.96ms | 1.6× | 1.5% | PASS |
| mem_writes | `oltp_read_write` | 107.99ms | 170.04ms | 1.6× | 1.7% | PASS |
| file_reads | `oltp_point_select` | 105.75ms | 61.22ms | 0.6× | 1.4% | PASS |
| file_reads | `oltp_range_select` | 28.70ms | 24.32ms | 0.8× | 1.5% | PASS |
| file_reads | `oltp_sum_range` | 26.05ms | 23.63ms | 0.9× | 1.2% | PASS |
| file_reads | `oltp_order_range` | 4.83ms | 4.59ms | 0.9× | 3.4% | PASS |
| file_reads | `oltp_distinct_range` | 5.96ms | 5.82ms | 1.0× | 2.6% | PASS |
| file_reads | `oltp_index_scan` | 12.28ms | 8.60ms | 0.7× | 2.1% | PASS |
| file_reads | `select_random_points` | 39.78ms | 38.04ms | 1.0× | 1.8% | PASS |
| file_reads | `select_random_ranges` | 15.77ms | 11.41ms | 0.7× | 1.4% | PASS |
| file_reads | `covering_index_scan` | 12.08ms | 6.82ms | 0.6× | 2.4% | PASS |
| file_reads | `groupby_scan` | 38.26ms | 41.24ms | 1.1× | 0.7% | PASS |
| file_reads | `index_join` | 12.59ms | 12.83ms | 1.0× | 2.2% | PASS |
| file_reads | `index_join_scan` | 4.97ms | 6.01ms | 1.2× | 3.0% | PASS |
| file_reads | `types_table_scan` | 1.11s | 1.20s | 1.1× | 1.8% | PASS |
| file_reads | `table_scan` | 1.38s | 1.33s | 1.0× | 1.5% | PASS |
| file_reads | `oltp_read_only` | 264.92ms | 208.61ms | 0.8× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 265.71ms | 376.85ms | 1.4× | 1.3% | PASS |
| file_writes | `oltp_insert` | 26.49ms | 42.12ms | 1.6× | 1.3% | PASS |
| file_writes | `oltp_update_index` | 102.59ms | 149.99ms | 1.5× | 2.7% | PASS |
| file_writes | `oltp_update_non_index` | 80.12ms | 102.15ms | 1.3× | 1.2% | PASS |
| file_writes | `oltp_delete_insert` | 79.31ms | 112.52ms | 1.4× | 1.8% | PASS |
| file_writes | `oltp_write_only` | 52.51ms | 71.13ms | 1.4× | 1.8% | PASS |
| file_writes | `types_delete_insert` | 51.31ms | 62.38ms | 1.2× | 1.7% | PASS |
| file_writes | `oltp_read_write` | 135.92ms | 182.08ms | 1.3× | 1.8% | PASS |
| ac_reads | `oltp_point_select` | 59.13ms | 61.89ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_range_select` | 24.10ms | 24.62ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_sum_range` | 21.00ms | 23.53ms | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_order_range` | 4.20ms | 4.48ms | 1.1× | 2.3% | PASS |
| ac_reads | `oltp_distinct_range` | 5.28ms | 5.67ms | 1.1× | 2.2% | PASS |
| ac_reads | `oltp_index_scan` | 7.27ms | 8.45ms | 1.2× | 2.5% | PASS |
| ac_reads | `select_random_points` | 32.31ms | 36.03ms | 1.1× | 1.8% | PASS |
| ac_reads | `select_random_ranges` | 10.40ms | 11.23ms | 1.1× | 1.2% | PASS |
| ac_reads | `covering_index_scan` | 7.08ms | 6.67ms | 0.9× | 1.8% | PASS |
| ac_reads | `groupby_scan` | 37.31ms | 41.00ms | 1.1× | 0.6% | PASS |
| ac_reads | `index_join` | 9.78ms | 12.49ms | 1.3× | 2.2% | PASS |
| ac_reads | `index_join_scan` | 4.54ms | 5.96ms | 1.3× | 2.8% | PASS |
| ac_reads | `types_table_scan` | 1.12s | 1.20s | 1.1× | 1.4% | PASS |
| ac_reads | `table_scan` | 1.41s | 1.34s | 0.9× | 1.1% | PASS |
| ac_reads | `oltp_read_only` | 198.03ms | 210.92ms | 1.1× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.75ms | 74.81ms | 3.4× | 4.4% | PASS |
| ac_writes | `oltp_insert_ac` | 23.64ms | 89.89ms | 3.8× | 6.2% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.52ms | 101.87ms | 3.7× | 6.4% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.58ms | 83.89ms | 3.7× | 4.9% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.48ms | 93.97ms | 3.7× | 6.9% | PASS |
| ac_writes | `oltp_write_only_ac` | 24.86ms | 94.83ms | 3.8× | 5.0% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.96ms | 89.39ms | 3.9× | 7.0% | PASS |
| ac_writes | `oltp_read_write_ac` | 33.25ms | 105.76ms | 3.2× | 6.4% | PASS |

</details>

</details>

## Version-control latency

Wall time: 2m 0s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 82.47ms | 130.00ms | 63.4% | 0.3% | PASS |
| `status_dirty_many_tables` | 85.89ms | 130.00ms | 66.1% | 0.6% | PASS |
| `diff_regular_working_one_table` | 78.25ms | 120.00ms | 65.2% | 0.4% | PASS |
| `diff_regular_working_many_tables` | 90.99ms | 140.00ms | 65.0% | 0.6% | PASS |
| `diff_stat_working_many_tables` | 91.20ms | 140.00ms | 65.1% | 0.5% | PASS |
| `diff_schema_working_many_tables` | 92.27ms | 140.00ms | 65.9% | 0.3% | PASS |
| `branch_list_many_branches` | 22.20ms | 35.00ms | 63.4% | 0.8% | PASS |
| `branch_create_delete` | 25.19ms | 40.00ms | 63.0% | 1.1% | PASS |
| `checkout_branch_clean` | 54.44ms | 150.00ms | 36.3% | 0.4% | PASS |
| `merge_data_no_conflicts` | 28.16ms | 50.00ms | 56.3% | 0.8% | PASS |
| `merge_schema_no_conflicts` | 21.16ms | 35.00ms | 60.5% | 0.8% | PASS |
| `merge_data_conflicts` | 31.51ms | 180.00ms | 17.5% | 0.4% | PASS |
| `merge_data_conflicts_with_resolve` | 31.51ms | 180.00ms | 17.5% | 0.3% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
