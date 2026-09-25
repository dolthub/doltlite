# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-09-25 11:10 UTC
>
> Commit: [`c0799675eebac9ff8c4914e4604245a1b29dd8c3`](https://github.com/dolthub/doltlite/commit/c0799675eebac9ff8c4914e4604245a1b29dd8c3)
>
> Runner: ubuntu24 20260920.314.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/36119423203)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.24s | 11.01s | 1.2× | 1.5% | **PASS** |
| Writes | 1.97s | 3.23s | 1.6× | 1.2% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.33s | 11.36s | 1.1× | 1.3% | **PASS** |
| Writes | 3.30s | 4.03s | 1.2× | 1.6% | **PASS** |
| Autocommit writes | 997.38ms | 2.93s | 2.9× | 6.6% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.42s | 2.77s | 1.1× | 1.2% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 1.74s | 1.87s | 1.1× | 3.1% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.53s | 3.15s | 1.2× | 1.3% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.55s | 3.23s | 1.3× | 1.1% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 435.99ms | 712.17ms | 1.6× | 1.1% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 384.18ms | 601.30ms | 1.6× | 1.9% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 575.20ms | 974.57ms | 1.7× | 1.2% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 575.53ms | 938.42ms | 1.6× | 1.2% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.64s | 2.83s | 1.1× | 1.2% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.13s | 2.03s | 1.0× | 1.9% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.77s | 3.21s | 1.2× | 1.3% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.79s | 3.30s | 1.2× | 1.3% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 575.89ms | 783.75ms | 1.4× | 1.1% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.17s | 1.12s | 1.0× | 11.3% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 795.15ms | 1.08s | 1.4× | 1.5% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 753.51ms | 1.05s | 1.4× | 1.8% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.49s | 2.83s | 1.1× | 1.2% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 1.78s | 1.91s | 1.1× | 1.8% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.62s | 3.21s | 1.2× | 1.3% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.64s | 3.30s | 1.2× | 1.2% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 190.57ms | 607.15ms | 3.2× | 5.4% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 386.15ms | 1.06s | 2.7× | 43.5% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 211.46ms | 628.20ms | 3.0× | 6.2% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 209.20ms | 638.22ms | 3.1× | 6.0% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 23.38ms | 30.54ms | 1.3× | 1.4% | PASS |
| mem_reads | `oltp_range_select` | 10.03ms | 11.45ms | 1.1× | 2.9% | PASS |
| mem_reads | `oltp_sum_range` | 9.06ms | 11.33ms | 1.3× | 2.0% | PASS |
| mem_reads | `oltp_order_range` | 2.47ms | 2.86ms | 1.2× | 1.6% | PASS |
| mem_reads | `oltp_distinct_range` | 3.51ms | 3.96ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 3.71ms | 5.18ms | 1.4× | 1.1% | PASS |
| mem_reads | `select_random_points` | 9.12ms | 11.19ms | 1.2× | 2.6% | PASS |
| mem_reads | `select_random_ranges` | 4.35ms | 5.11ms | 1.2× | 1.7% | PASS |
| mem_reads | `covering_index_scan` | 7.66ms | 10.18ms | 1.3× | 0.7% | PASS |
| mem_reads | `groupby_scan` | 28.86ms | 32.68ms | 1.1× | 1.0% | PASS |
| mem_reads | `index_join` | 5.69ms | 7.66ms | 1.3× | 0.8% | PASS |
| mem_reads | `index_join_scan` | 3.04ms | 4.57ms | 1.5× | 2.3% | PASS |
| mem_reads | `types_table_scan` | 1.04s | 1.21s | 1.2× | 0.4% | PASS |
| mem_reads | `table_scan` | 1.17s | 1.30s | 1.1× | 0.6% | PASS |
| mem_reads | `oltp_read_only` | 100.25ms | 121.28ms | 1.2× | 1.2% | PASS |
| mem_writes | `oltp_bulk_insert` | 180.06ms | 275.89ms | 1.5× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 15.20ms | 28.68ms | 1.9× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 48.72ms | 90.38ms | 1.9× | 1.2% | PASS |
| mem_writes | `oltp_update_non_index` | 34.17ms | 56.17ms | 1.6× | 1.4% | PASS |
| mem_writes | `oltp_delete_insert` | 44.45ms | 73.48ms | 1.7× | 0.9% | PASS |
| mem_writes | `oltp_write_only` | 21.70ms | 43.30ms | 2.0× | 1.3% | PASS |
| mem_writes | `types_delete_insert` | 24.27ms | 37.87ms | 1.6× | 0.8% | PASS |
| mem_writes | `oltp_read_write` | 67.42ms | 106.39ms | 1.6× | 1.2% | PASS |
| file_reads | `oltp_point_select` | 93.31ms | 49.99ms | 0.5× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 17.49ms | 13.59ms | 0.8× | 1.9% | PASS |
| file_reads | `oltp_sum_range` | 16.54ms | 13.56ms | 0.8× | 2.1% | PASS |
| file_reads | `oltp_order_range` | 3.37ms | 3.18ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_distinct_range` | 4.38ms | 4.27ms | 1.0× | 1.2% | PASS |
| file_reads | `oltp_index_scan` | 10.96ms | 7.66ms | 0.7× | 1.3% | PASS |
| file_reads | `select_random_points` | 17.11ms | 13.64ms | 0.8× | 2.0% | PASS |
| file_reads | `select_random_ranges` | 11.70ms | 7.27ms | 0.6× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 15.03ms | 12.61ms | 0.8× | 1.4% | PASS |
| file_reads | `groupby_scan` | 29.60ms | 33.22ms | 1.1× | 1.0% | PASS |
| file_reads | `index_join` | 9.58ms | 9.37ms | 1.0× | 1.0% | PASS |
| file_reads | `index_join_scan` | 3.97ms | 5.09ms | 1.3× | 1.4% | PASS |
| file_reads | `types_table_scan` | 1.04s | 1.21s | 1.2× | 0.3% | PASS |
| file_reads | `table_scan` | 1.17s | 1.30s | 1.1× | 0.4% | PASS |
| file_reads | `oltp_read_only` | 198.63ms | 149.83ms | 0.8× | 0.6% | PASS |
| file_writes | `oltp_bulk_insert` | 193.19ms | 286.27ms | 1.5× | 0.8% | PASS |
| file_writes | `oltp_insert` | 21.39ms | 32.20ms | 1.5× | 0.9% | PASS |
| file_writes | `oltp_update_index` | 73.95ms | 102.48ms | 1.4× | 1.1% | PASS |
| file_writes | `oltp_update_non_index` | 55.58ms | 67.74ms | 1.2× | 1.2% | PASS |
| file_writes | `oltp_delete_insert` | 64.99ms | 83.80ms | 1.3× | 1.5% | PASS |
| file_writes | `oltp_write_only` | 42.44ms | 53.52ms | 1.3× | 1.6% | PASS |
| file_writes | `types_delete_insert` | 37.96ms | 43.49ms | 1.1× | 1.2% | PASS |
| file_writes | `oltp_read_write` | 86.40ms | 114.25ms | 1.3× | 0.8% | PASS |
| ac_reads | `oltp_point_select` | 45.43ms | 49.30ms | 1.1× | 0.8% | PASS |
| ac_reads | `oltp_range_select` | 12.28ms | 13.42ms | 1.1× | 1.0% | PASS |
| ac_reads | `oltp_sum_range` | 11.25ms | 13.44ms | 1.2× | 1.4% | PASS |
| ac_reads | `oltp_order_range` | 2.81ms | 3.15ms | 1.1× | 2.2% | PASS |
| ac_reads | `oltp_distinct_range` | 3.85ms | 4.26ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_index_scan` | 6.21ms | 7.54ms | 1.2× | 1.6% | PASS |
| ac_reads | `select_random_points` | 11.92ms | 13.47ms | 1.1× | 1.3% | PASS |
| ac_reads | `select_random_ranges` | 6.79ms | 7.25ms | 1.1× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 10.15ms | 12.42ms | 1.2× | 1.2% | PASS |
| ac_reads | `groupby_scan` | 29.09ms | 33.12ms | 1.1× | 0.9% | PASS |
| ac_reads | `index_join` | 7.14ms | 9.45ms | 1.3× | 1.6% | PASS |
| ac_reads | `index_join_scan` | 3.53ms | 5.18ms | 1.5× | 1.6% | PASS |
| ac_reads | `types_table_scan` | 1.04s | 1.21s | 1.2× | 0.4% | PASS |
| ac_reads | `table_scan` | 1.17s | 1.30s | 1.1× | 0.4% | PASS |
| ac_reads | `oltp_read_only` | 132.18ms | 149.45ms | 1.1× | 0.6% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 20.96ms | 60.83ms | 2.9× | 4.4% | PASS |
| ac_writes | `oltp_insert_ac` | 23.56ms | 75.96ms | 3.2× | 6.6% | PASS |
| ac_writes | `oltp_update_index_ac` | 25.63ms | 90.21ms | 3.5× | 4.9% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.92ms | 68.89ms | 3.1× | 7.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 23.27ms | 78.28ms | 3.4× | 5.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 23.61ms | 79.65ms | 3.4× | 5.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.80ms | 69.61ms | 3.1× | 8.3% | PASS |
| ac_writes | `oltp_read_write_ac` | 28.82ms | 83.73ms | 2.9× | 5.4% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 25.68ms | 24.56ms | 1.0× | 3.1% | PASS |
| mem_reads | `oltp_range_select` | 11.38ms | 9.59ms | 0.8× | 2.3% | PASS |
| mem_reads | `oltp_sum_range` | 11.10ms | 9.79ms | 0.9× | 3.3% | PASS |
| mem_reads | `oltp_order_range` | 2.22ms | 2.05ms | 0.9× | 2.8% | PASS |
| mem_reads | `oltp_distinct_range` | 2.67ms | 2.49ms | 0.9× | 2.2% | PASS |
| mem_reads | `oltp_index_scan` | 2.47ms | 3.85ms | 1.6× | 3.7% | PASS |
| mem_reads | `select_random_points` | 16.53ms | 13.95ms | 0.8× | 3.9% | PASS |
| mem_reads | `select_random_ranges` | 4.43ms | 3.83ms | 0.9× | 2.3% | PASS |
| mem_reads | `covering_index_scan` | 3.89ms | 5.59ms | 1.4× | 3.6% | PASS |
| mem_reads | `groupby_scan` | 20.22ms | 19.67ms | 1.0× | 2.0% | PASS |
| mem_reads | `index_join` | 7.69ms | 5.37ms | 0.7× | 5.5% | PASS |
| mem_reads | `index_join_scan` | 2.69ms | 4.64ms | 1.7× | 5.1% | PASS |
| mem_reads | `types_table_scan` | 689.62ms | 777.27ms | 1.1× | 1.5% | PASS |
| mem_reads | `table_scan` | 847.32ms | 902.56ms | 1.1× | 3.2% | PASS |
| mem_reads | `oltp_read_only` | 94.73ms | 82.03ms | 0.9× | 1.7% | PASS |
| mem_writes | `oltp_bulk_insert` | 135.91ms | 202.24ms | 1.5× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 10.41ms | 23.92ms | 2.3× | 1.9% | PASS |
| mem_writes | `oltp_update_index` | 46.72ms | 93.31ms | 2.0× | 2.0% | PASS |
| mem_writes | `oltp_update_non_index` | 35.80ms | 55.12ms | 1.5× | 2.2% | PASS |
| mem_writes | `oltp_delete_insert` | 37.41ms | 66.65ms | 1.8× | 1.7% | PASS |
| mem_writes | `oltp_write_only` | 19.73ms | 38.41ms | 1.9× | 1.8% | PASS |
| mem_writes | `types_delete_insert` | 27.70ms | 35.27ms | 1.3× | 2.4% | PASS |
| mem_writes | `oltp_read_write` | 70.48ms | 86.39ms | 1.2× | 3.0% | PASS |
| file_reads | `oltp_point_select` | 78.52ms | 37.91ms | 0.5× | 1.3% | PASS |
| file_reads | `oltp_range_select` | 16.67ms | 10.64ms | 0.6× | 2.0% | PASS |
| file_reads | `oltp_sum_range` | 16.56ms | 10.88ms | 0.7× | 1.5% | PASS |
| file_reads | `oltp_order_range` | 3.09ms | 2.49ms | 0.8× | 4.6% | PASS |
| file_reads | `oltp_distinct_range` | 3.54ms | 2.96ms | 0.8× | 4.9% | PASS |
| file_reads | `oltp_index_scan` | 8.44ms | 5.97ms | 0.7× | 2.4% | PASS |
| file_reads | `select_random_points` | 22.29ms | 15.41ms | 0.7× | 3.6% | PASS |
| file_reads | `select_random_ranges` | 10.00ms | 5.42ms | 0.5× | 1.6% | PASS |
| file_reads | `covering_index_scan` | 9.64ms | 7.52ms | 0.8× | 1.3% | PASS |
| file_reads | `groupby_scan` | 21.29ms | 20.48ms | 1.0× | 2.0% | PASS |
| file_reads | `index_join` | 11.47ms | 7.55ms | 0.7× | 2.1% | PASS |
| file_reads | `index_join_scan` | 3.45ms | 5.00ms | 1.5× | 1.9% | PASS |
| file_reads | `types_table_scan` | 790.64ms | 838.87ms | 1.1× | 1.6% | PASS |
| file_reads | `table_scan` | 959.79ms | 954.93ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_read_only` | 171.24ms | 102.31ms | 0.6× | 1.2% | PASS |
| file_writes | `oltp_bulk_insert` | 230.38ms | 291.94ms | 1.3× | 10.9% | PASS |
| file_writes | `oltp_insert` | 22.96ms | 47.67ms | 2.1× | 21.6% | PASS |
| file_writes | `oltp_update_index` | 172.03ms | 186.80ms | 1.1× | 11.8% | PASS |
| file_writes | `oltp_update_non_index` | 142.22ms | 119.89ms | 0.8× | 13.1% | PASS |
| file_writes | `oltp_delete_insert` | 172.17ms | 137.35ms | 0.8× | 9.8% | PASS |
| file_writes | `oltp_write_only` | 120.02ms | 96.44ms | 0.8× | 7.6% | PASS |
| file_writes | `types_delete_insert` | 146.09ms | 89.37ms | 0.6× | 18.8% | PASS |
| file_writes | `oltp_read_write` | 168.13ms | 146.72ms | 0.9× | 8.1% | PASS |
| ac_reads | `oltp_point_select` | 43.28ms | 38.16ms | 0.9× | 2.6% | PASS |
| ac_reads | `oltp_range_select` | 13.26ms | 10.84ms | 0.8× | 2.3% | PASS |
| ac_reads | `oltp_sum_range` | 13.07ms | 10.99ms | 0.8× | 2.7% | PASS |
| ac_reads | `oltp_order_range` | 2.65ms | 2.33ms | 0.9× | 1.3% | PASS |
| ac_reads | `oltp_distinct_range` | 3.07ms | 2.78ms | 0.9× | 1.3% | PASS |
| ac_reads | `oltp_index_scan` | 4.80ms | 5.79ms | 1.2× | 1.7% | PASS |
| ac_reads | `select_random_points` | 19.06ms | 15.39ms | 0.8× | 2.7% | PASS |
| ac_reads | `select_random_ranges` | 6.70ms | 5.45ms | 0.8× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 6.26ms | 7.54ms | 1.2× | 1.1% | PASS |
| ac_reads | `groupby_scan` | 20.85ms | 20.10ms | 1.0× | 1.2% | PASS |
| ac_reads | `index_join` | 9.70ms | 7.50ms | 0.8× | 2.0% | PASS |
| ac_reads | `index_join_scan` | 3.08ms | 4.82ms | 1.6× | 2.4% | PASS |
| ac_reads | `types_table_scan` | 723.58ms | 805.55ms | 1.1× | 3.3% | PASS |
| ac_reads | `table_scan` | 812.00ms | 880.00ms | 1.1× | 1.7% | PASS |
| ac_reads | `oltp_read_only` | 101.74ms | 94.81ms | 0.9× | 1.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 53.10ms | 116.60ms | 2.2× | 42.0% | PASS |
| ac_writes | `oltp_insert_ac` | 42.37ms | 134.68ms | 3.2× | 45.3% | PASS |
| ac_writes | `oltp_update_index_ac` | 43.16ms | 127.00ms | 2.9× | 37.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 28.23ms | 75.23ms | 2.7× | 40.5% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 46.52ms | 131.44ms | 2.8× | 45.0% | PASS |
| ac_writes | `oltp_write_only_ac` | 46.69ms | 127.49ms | 2.7× | 42.0% | PASS |
| ac_writes | `types_delete_insert_ac` | 55.74ms | 186.84ms | 3.4× | 54.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 70.33ms | 158.80ms | 2.3× | 52.0% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.53ms | 39.34ms | 1.2× | 1.8% | PASS |
| mem_reads | `oltp_range_select` | 15.44ms | 15.86ms | 1.0× | 1.3% | PASS |
| mem_reads | `oltp_sum_range` | 14.19ms | 14.74ms | 1.0× | 2.4% | PASS |
| mem_reads | `oltp_order_range` | 3.22ms | 3.33ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 4.33ms | 4.46ms | 1.0× | 1.3% | PASS |
| mem_reads | `oltp_index_scan` | 3.90ms | 6.35ms | 1.6× | 1.6% | PASS |
| mem_reads | `select_random_points` | 20.21ms | 21.70ms | 1.1× | 1.4% | PASS |
| mem_reads | `select_random_ranges` | 6.41ms | 6.68ms | 1.0× | 1.8% | PASS |
| mem_reads | `covering_index_scan` | 7.72ms | 10.35ms | 1.3× | 0.8% | PASS |
| mem_reads | `groupby_scan` | 33.14ms | 35.76ms | 1.1× | 1.0% | PASS |
| mem_reads | `index_join` | 9.82ms | 9.43ms | 1.0× | 1.8% | PASS |
| mem_reads | `index_join_scan` | 3.58ms | 5.76ms | 1.6× | 1.3% | PASS |
| mem_reads | `types_table_scan` | 1.05s | 1.36s | 1.3× | 0.4% | PASS |
| mem_reads | `table_scan` | 1.19s | 1.46s | 1.2× | 0.5% | PASS |
| mem_reads | `oltp_read_only` | 133.78ms | 146.16ms | 1.1× | 1.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 241.89ms | 371.79ms | 1.5× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 18.30ms | 39.44ms | 2.2× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 62.51ms | 132.82ms | 2.1× | 1.2% | PASS |
| mem_writes | `oltp_update_non_index` | 45.81ms | 81.42ms | 1.8× | 1.3% | PASS |
| mem_writes | `oltp_delete_insert` | 50.80ms | 99.97ms | 2.0× | 1.2% | PASS |
| mem_writes | `oltp_write_only` | 26.81ms | 57.10ms | 2.1× | 1.3% | PASS |
| mem_writes | `types_delete_insert` | 36.64ms | 53.06ms | 1.4× | 0.9% | PASS |
| mem_writes | `oltp_read_write` | 92.45ms | 138.97ms | 1.5× | 1.3% | PASS |
| file_reads | `oltp_point_select` | 104.36ms | 58.77ms | 0.6× | 1.2% | PASS |
| file_reads | `oltp_range_select` | 22.61ms | 18.10ms | 0.8× | 2.6% | PASS |
| file_reads | `oltp_sum_range` | 21.42ms | 17.20ms | 0.8× | 1.6% | PASS |
| file_reads | `oltp_order_range` | 4.12ms | 3.67ms | 0.9× | 1.6% | PASS |
| file_reads | `oltp_distinct_range` | 5.25ms | 4.81ms | 0.9× | 0.9% | PASS |
| file_reads | `oltp_index_scan` | 11.04ms | 8.69ms | 0.8× | 1.3% | PASS |
| file_reads | `select_random_points` | 29.48ms | 24.98ms | 0.8× | 2.0% | PASS |
| file_reads | `select_random_ranges` | 13.75ms | 8.90ms | 0.6× | 1.7% | PASS |
| file_reads | `covering_index_scan` | 14.92ms | 12.61ms | 0.8× | 1.2% | PASS |
| file_reads | `groupby_scan` | 34.23ms | 36.32ms | 1.1× | 1.1% | PASS |
| file_reads | `index_join` | 13.97ms | 11.25ms | 0.8× | 2.5% | PASS |
| file_reads | `index_join_scan` | 4.54ms | 6.29ms | 1.4× | 2.7% | PASS |
| file_reads | `types_table_scan` | 1.05s | 1.36s | 1.3× | 0.3% | PASS |
| file_reads | `table_scan` | 1.19s | 1.46s | 1.2× | 0.6% | PASS |
| file_reads | `oltp_read_only` | 244.49ms | 175.60ms | 0.7× | 1.2% | PASS |
| file_writes | `oltp_bulk_insert` | 263.81ms | 383.86ms | 1.5× | 0.9% | PASS |
| file_writes | `oltp_insert` | 25.31ms | 46.11ms | 1.8× | 1.1% | PASS |
| file_writes | `oltp_update_index` | 94.25ms | 152.24ms | 1.6× | 1.3% | PASS |
| file_writes | `oltp_update_non_index` | 83.37ms | 96.92ms | 1.2× | 8.8% | PASS |
| file_writes | `oltp_delete_insert` | 85.48ms | 115.79ms | 1.4× | 1.5% | PASS |
| file_writes | `oltp_write_only` | 56.14ms | 70.70ms | 1.3× | 1.9% | PASS |
| file_writes | `types_delete_insert` | 62.16ms | 65.86ms | 1.1× | 1.6% | PASS |
| file_writes | `oltp_read_write` | 124.63ms | 153.20ms | 1.2× | 1.5% | PASS |
| ac_reads | `oltp_point_select` | 57.77ms | 58.88ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_range_select` | 18.77ms | 18.07ms | 1.0× | 1.8% | PASS |
| ac_reads | `oltp_sum_range` | 17.29ms | 17.13ms | 1.0× | 2.1% | PASS |
| ac_reads | `oltp_order_range` | 3.77ms | 3.67ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_distinct_range` | 4.85ms | 4.80ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_index_scan` | 6.59ms | 8.71ms | 1.3× | 1.7% | PASS |
| ac_reads | `select_random_points` | 24.20ms | 24.82ms | 1.0× | 3.0% | PASS |
| ac_reads | `select_random_ranges` | 9.18ms | 8.89ms | 1.0× | 1.2% | PASS |
| ac_reads | `covering_index_scan` | 10.35ms | 12.65ms | 1.2× | 1.3% | PASS |
| ac_reads | `groupby_scan` | 33.78ms | 36.33ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 11.90ms | 11.29ms | 0.9× | 1.9% | PASS |
| ac_reads | `index_join_scan` | 4.13ms | 6.20ms | 1.5× | 1.4% | PASS |
| ac_reads | `types_table_scan` | 1.05s | 1.36s | 1.3× | 0.5% | PASS |
| ac_reads | `table_scan` | 1.19s | 1.46s | 1.2× | 0.7% | PASS |
| ac_reads | `oltp_read_only` | 173.22ms | 176.11ms | 1.0× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 24.99ms | 63.73ms | 2.5× | 6.7% | PASS |
| ac_writes | `oltp_insert_ac` | 26.64ms | 78.62ms | 3.0× | 6.6% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.87ms | 90.28ms | 3.4× | 4.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.00ms | 71.02ms | 3.0× | 6.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.89ms | 80.19ms | 3.1× | 5.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.82ms | 79.75ms | 3.1× | 6.4% | PASS |
| ac_writes | `types_delete_insert_ac` | 24.94ms | 79.12ms | 3.2× | 7.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.31ms | 85.46ms | 2.6× | 5.5% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 32.76ms | 41.25ms | 1.3× | 1.8% | PASS |
| mem_reads | `oltp_range_select` | 19.02ms | 22.73ms | 1.2× | 1.2% | PASS |
| mem_reads | `oltp_sum_range` | 17.65ms | 21.61ms | 1.2× | 1.5% | PASS |
| mem_reads | `oltp_order_range` | 3.50ms | 4.11ms | 1.2× | 1.0% | PASS |
| mem_reads | `oltp_distinct_range` | 4.61ms | 5.29ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 4.34ms | 6.02ms | 1.4× | 1.7% | PASS |
| mem_reads | `select_random_points` | 27.30ms | 32.34ms | 1.2× | 2.1% | PASS |
| mem_reads | `select_random_ranges` | 7.60ms | 9.11ms | 1.2× | 1.1% | PASS |
| mem_reads | `covering_index_scan` | 7.63ms | 10.23ms | 1.3× | 0.9% | PASS |
| mem_reads | `groupby_scan` | 35.47ms | 41.77ms | 1.2× | 0.8% | PASS |
| mem_reads | `index_join` | 7.81ms | 10.30ms | 1.3× | 1.1% | PASS |
| mem_reads | `index_join_scan` | 3.81ms | 5.75ms | 1.5× | 2.0% | PASS |
| mem_reads | `types_table_scan` | 1.05s | 1.37s | 1.3× | 0.5% | PASS |
| mem_reads | `table_scan` | 1.19s | 1.47s | 1.2× | 0.6% | PASS |
| mem_reads | `oltp_read_only` | 145.59ms | 179.91ms | 1.2× | 1.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 240.97ms | 349.83ms | 1.5× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 18.66ms | 35.86ms | 1.9× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 64.06ms | 120.50ms | 1.9× | 1.2% | PASS |
| mem_writes | `oltp_update_non_index` | 49.82ms | 81.00ms | 1.6× | 1.7% | PASS |
| mem_writes | `oltp_delete_insert` | 48.70ms | 93.42ms | 1.9× | 1.2% | PASS |
| mem_writes | `oltp_write_only` | 26.19ms | 53.52ms | 2.0× | 1.1% | PASS |
| mem_writes | `types_delete_insert` | 31.39ms | 51.92ms | 1.7× | 1.4% | PASS |
| mem_writes | `oltp_read_write` | 95.73ms | 152.37ms | 1.6× | 1.0% | PASS |
| file_reads | `oltp_point_select` | 101.84ms | 59.99ms | 0.6× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 25.99ms | 24.94ms | 1.0× | 1.4% | PASS |
| file_reads | `oltp_sum_range` | 24.63ms | 23.69ms | 1.0× | 1.0% | PASS |
| file_reads | `oltp_order_range` | 4.37ms | 4.38ms | 1.0× | 1.5% | PASS |
| file_reads | `oltp_distinct_range` | 5.51ms | 5.62ms | 1.0× | 1.7% | PASS |
| file_reads | `oltp_index_scan` | 11.86ms | 8.46ms | 0.7× | 1.5% | PASS |
| file_reads | `select_random_points` | 37.16ms | 36.06ms | 1.0× | 1.3% | PASS |
| file_reads | `select_random_ranges` | 15.00ms | 11.30ms | 0.8× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 15.15ms | 12.40ms | 0.8× | 1.5% | PASS |
| file_reads | `groupby_scan` | 36.43ms | 42.27ms | 1.2× | 0.8% | PASS |
| file_reads | `index_join` | 12.02ms | 12.14ms | 1.0× | 1.9% | PASS |
| file_reads | `index_join_scan` | 4.78ms | 6.28ms | 1.3× | 2.0% | PASS |
| file_reads | `types_table_scan` | 1.05s | 1.37s | 1.3× | 0.8% | PASS |
| file_reads | `table_scan` | 1.20s | 1.47s | 1.2× | 0.7% | PASS |
| file_reads | `oltp_read_only` | 249.10ms | 209.26ms | 0.8× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 254.74ms | 361.61ms | 1.4× | 1.0% | PASS |
| file_writes | `oltp_insert` | 25.73ms | 41.77ms | 1.6× | 1.8% | PASS |
| file_writes | `oltp_update_index` | 93.55ms | 137.01ms | 1.5× | 1.6% | PASS |
| file_writes | `oltp_update_non_index` | 76.48ms | 97.51ms | 1.3× | 2.1% | PASS |
| file_writes | `oltp_delete_insert` | 75.66ms | 108.92ms | 1.4× | 1.8% | PASS |
| file_writes | `oltp_write_only` | 50.99ms | 67.73ms | 1.3× | 1.9% | PASS |
| file_writes | `types_delete_insert` | 49.58ms | 62.38ms | 1.3× | 2.5% | PASS |
| file_writes | `oltp_read_write` | 126.78ms | 168.38ms | 1.3× | 1.6% | PASS |
| ac_reads | `oltp_point_select` | 56.09ms | 60.68ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_range_select` | 21.72ms | 25.09ms | 1.2× | 2.2% | PASS |
| ac_reads | `oltp_sum_range` | 20.09ms | 23.92ms | 1.2× | 1.2% | PASS |
| ac_reads | `oltp_order_range` | 3.92ms | 4.44ms | 1.1× | 1.5% | PASS |
| ac_reads | `oltp_distinct_range` | 4.99ms | 5.61ms | 1.1× | 0.9% | PASS |
| ac_reads | `oltp_index_scan` | 7.19ms | 8.49ms | 1.2× | 1.8% | PASS |
| ac_reads | `select_random_points` | 31.45ms | 36.43ms | 1.2× | 1.3% | PASS |
| ac_reads | `select_random_ranges` | 10.23ms | 11.37ms | 1.1× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 10.41ms | 12.50ms | 1.2× | 1.0% | PASS |
| ac_reads | `groupby_scan` | 35.91ms | 42.46ms | 1.2× | 1.1% | PASS |
| ac_reads | `index_join` | 9.63ms | 12.41ms | 1.3× | 1.2% | PASS |
| ac_reads | `index_join_scan` | 4.30ms | 6.33ms | 1.5× | 1.5% | PASS |
| ac_reads | `types_table_scan` | 1.06s | 1.37s | 1.3× | 1.0% | PASS |
| ac_reads | `table_scan` | 1.19s | 1.47s | 1.2× | 0.7% | PASS |
| ac_reads | `oltp_read_only` | 178.53ms | 208.95ms | 1.2× | 1.1% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 23.45ms | 62.73ms | 2.7× | 6.3% | PASS |
| ac_writes | `oltp_insert_ac` | 25.71ms | 79.49ms | 3.1× | 6.6% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.99ms | 92.11ms | 3.3× | 5.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.83ms | 72.57ms | 3.0× | 5.1% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.37ms | 80.75ms | 3.2× | 5.7% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.40ms | 82.30ms | 3.1× | 8.4% | PASS |
| ac_writes | `types_delete_insert_ac` | 23.19ms | 75.92ms | 3.3× | 8.6% | PASS |
| ac_writes | `oltp_read_write_ac` | 33.26ms | 92.35ms | 2.8× | 5.6% | PASS |

</details>

</details>

## Version-control latency

Wall time: 3m 26s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 34.99ms | 130.00ms | 26.9% | 0.9% | PASS |
| `status_dirty_many_tables` | 38.72ms | 130.00ms | 29.8% | 0.8% | PASS |
| `diff_regular_working_one_table` | 30.25ms | 120.00ms | 25.2% | 1.0% | PASS |
| `diff_regular_working_many_tables` | 43.80ms | 140.00ms | 31.3% | 0.9% | PASS |
| `diff_stat_working_many_tables` | 43.72ms | 140.00ms | 31.2% | 0.8% | PASS |
| `diff_schema_working_many_tables` | 44.42ms | 140.00ms | 31.7% | 0.9% | PASS |
| `branch_list_many_branches` | 22.20ms | 35.00ms | 63.4% | 1.3% | PASS |
| `branch_create_delete` | 24.64ms | 40.00ms | 61.6% | 1.4% | PASS |
| `at_literal_deep_history` | 24.90ms | 100.00ms | 24.9% | 1.6% | PASS |
| `diff_literal_deep_history` | 24.65ms | 120.00ms | 20.5% | 0.7% | PASS |
| `history_literal_deep_history` | 26.10ms | 150.00ms | 17.4% | 1.5% | PASS |
| `checkout_branch_clean` | 36.97ms | 150.00ms | 24.6% | 1.2% | PASS |
| `merge_data_no_conflicts` | 27.71ms | 50.00ms | 55.4% | 1.0% | PASS |
| `merge_data_secondary_index` | 870.85ms | 2.50s | 34.8% | 0.8% | PASS |
| `merge_schema_no_conflicts` | 21.38ms | 35.00ms | 61.1% | 1.5% | PASS |
| `merge_data_conflicts` | 29.95ms | 180.00ms | 16.6% | 1.0% | PASS |
| `merge_data_conflicts_with_resolve` | 31.25ms | 180.00ms | 17.4% | 1.6% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
