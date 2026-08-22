# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-08-22 11:10 UTC
>
> Commit: [`7d694509838082c482a95f3499595118ff2aac93`](https://github.com/dolthub/doltlite/commit/7d694509838082c482a95f3499595118ff2aac93)
>
> Runner: ubuntu24 20260816.277.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/32565515939)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.97s | 10.74s | 1.1× | 1.4% | **PASS** |
| Writes | 2.00s | 3.21s | 1.6× | 1.2% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.53s | 10.89s | 1.0× | 1.3% | **PASS** |
| Writes | 3.22s | 3.89s | 1.2× | 1.5% | **PASS** |
| Autocommit writes | 849.89ms | 2.86s | 3.4× | 5.5% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.40s | 2.55s | 1.1× | 1.4% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.11s | 2.23s | 1.1× | 1.7% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.49s | 2.89s | 1.2× | 1.3% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.97s | 3.07s | 1.0× | 1.3% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 430.00ms | 667.66ms | 1.6× | 1.0% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 388.15ms | 640.91ms | 1.7× | 1.5% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 562.37ms | 936.58ms | 1.7× | 1.1% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 617.17ms | 965.51ms | 1.6× | 1.3% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.62s | 2.61s | 1.0× | 1.3% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.19s | 2.25s | 1.0× | 1.7% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.73s | 2.96s | 1.1× | 1.4% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.99s | 3.07s | 1.0× | 1.4% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 580.76ms | 734.20ms | 1.3× | 1.4% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.10s | 1.09s | 1.0× | 1.5% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 771.23ms | 1.04s | 1.3× | 2.0% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 775.99ms | 1.03s | 1.3× | 1.3% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.48s | 2.61s | 1.1× | 1.5% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.14s | 2.26s | 1.1× | 1.6% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.81s | 3.01s | 1.1× | 1.7% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.94s | 3.10s | 1.1× | 1.7% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 183.18ms | 675.86ms | 3.7× | 4.5% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 257.31ms | 724.62ms | 2.8× | 5.8% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 207.96ms | 726.60ms | 3.5× | 5.7% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 201.44ms | 736.55ms | 3.7× | 5.8% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 22.76ms | 28.50ms | 1.3× | 1.5% | PASS |
| mem_reads | `oltp_range_select` | 9.83ms | 11.48ms | 1.2× | 3.0% | PASS |
| mem_reads | `oltp_sum_range` | 9.02ms | 10.88ms | 1.2× | 1.6% | PASS |
| mem_reads | `oltp_order_range` | 2.44ms | 2.70ms | 1.1× | 1.9% | PASS |
| mem_reads | `oltp_distinct_range` | 3.48ms | 3.79ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 3.72ms | 4.95ms | 1.3× | 1.4% | PASS |
| mem_reads | `select_random_points` | 9.07ms | 10.54ms | 1.2× | 2.3% | PASS |
| mem_reads | `select_random_ranges` | 2.72ms | 3.82ms | 1.4× | 2.2% | PASS |
| mem_reads | `covering_index_scan` | 4.23ms | 3.99ms | 0.9× | 1.1% | PASS |
| mem_reads | `groupby_scan` | 29.70ms | 32.07ms | 1.1× | 0.6% | PASS |
| mem_reads | `index_join` | 5.98ms | 7.59ms | 1.3× | 1.2% | PASS |
| mem_reads | `index_join_scan` | 3.08ms | 4.36ms | 1.4× | 2.2% | PASS |
| mem_reads | `types_table_scan` | 1.03s | 1.11s | 1.1× | 0.4% | PASS |
| mem_reads | `table_scan` | 1.16s | 1.20s | 1.0× | 0.4% | PASS |
| mem_reads | `oltp_read_only` | 98.57ms | 113.56ms | 1.2× | 0.5% | PASS |
| mem_writes | `oltp_bulk_insert` | 182.05ms | 245.70ms | 1.3× | 1.2% | PASS |
| mem_writes | `oltp_insert` | 15.43ms | 26.60ms | 1.7× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 48.28ms | 86.83ms | 1.8× | 0.7% | PASS |
| mem_writes | `oltp_update_non_index` | 32.60ms | 51.79ms | 1.6× | 0.8% | PASS |
| mem_writes | `oltp_delete_insert` | 43.22ms | 69.68ms | 1.6× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 20.68ms | 43.97ms | 2.1× | 0.9% | PASS |
| mem_writes | `types_delete_insert` | 23.86ms | 35.52ms | 1.5× | 1.9% | PASS |
| mem_writes | `oltp_read_write` | 63.89ms | 107.57ms | 1.7× | 1.2% | PASS |
| file_reads | `oltp_point_select` | 91.49ms | 47.26ms | 0.5× | 1.1% | PASS |
| file_reads | `oltp_range_select` | 16.61ms | 13.50ms | 0.8× | 1.1% | PASS |
| file_reads | `oltp_sum_range` | 16.09ms | 13.03ms | 0.8× | 1.2% | PASS |
| file_reads | `oltp_order_range` | 3.29ms | 3.13ms | 0.9× | 2.3% | PASS |
| file_reads | `oltp_distinct_range` | 4.31ms | 4.19ms | 1.0× | 2.0% | PASS |
| file_reads | `oltp_index_scan` | 10.81ms | 7.16ms | 0.7× | 1.7% | PASS |
| file_reads | `select_random_points` | 16.13ms | 12.93ms | 0.8× | 2.9% | PASS |
| file_reads | `select_random_ranges` | 9.67ms | 5.78ms | 0.6× | 1.3% | PASS |
| file_reads | `covering_index_scan` | 11.30ms | 6.10ms | 0.5× | 1.5% | PASS |
| file_reads | `groupby_scan` | 30.62ms | 32.57ms | 1.1× | 1.0% | PASS |
| file_reads | `index_join` | 9.86ms | 9.14ms | 0.9× | 1.3% | PASS |
| file_reads | `index_join_scan` | 4.20ms | 4.90ms | 1.2× | 3.1% | PASS |
| file_reads | `types_table_scan` | 1.03s | 1.10s | 1.1× | 0.2% | PASS |
| file_reads | `table_scan` | 1.16s | 1.20s | 1.0× | 0.3% | PASS |
| file_reads | `oltp_read_only` | 198.93ms | 141.50ms | 0.7× | 0.4% | PASS |
| file_writes | `oltp_bulk_insert` | 195.60ms | 253.48ms | 1.3× | 1.5% | PASS |
| file_writes | `oltp_insert` | 21.82ms | 30.63ms | 1.4× | 1.4% | PASS |
| file_writes | `oltp_update_index` | 73.89ms | 97.20ms | 1.3× | 1.1% | PASS |
| file_writes | `oltp_update_non_index` | 55.59ms | 64.70ms | 1.2× | 1.8% | PASS |
| file_writes | `oltp_delete_insert` | 65.12ms | 79.29ms | 1.2× | 1.3% | PASS |
| file_writes | `oltp_write_only` | 42.46ms | 51.73ms | 1.2× | 1.6% | PASS |
| file_writes | `types_delete_insert` | 38.54ms | 41.68ms | 1.1× | 1.8% | PASS |
| file_writes | `oltp_read_write` | 87.73ms | 115.48ms | 1.3× | 1.3% | PASS |
| ac_reads | `oltp_point_select` | 45.55ms | 47.39ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_range_select` | 12.14ms | 13.51ms | 1.1× | 1.6% | PASS |
| ac_reads | `oltp_sum_range` | 11.71ms | 13.05ms | 1.1× | 1.0% | PASS |
| ac_reads | `oltp_order_range` | 2.88ms | 3.10ms | 1.1× | 3.0% | PASS |
| ac_reads | `oltp_distinct_range` | 3.88ms | 4.17ms | 1.1× | 1.7% | PASS |
| ac_reads | `oltp_index_scan` | 6.13ms | 7.16ms | 1.2× | 1.9% | PASS |
| ac_reads | `select_random_points` | 11.71ms | 12.85ms | 1.1× | 2.3% | PASS |
| ac_reads | `select_random_ranges` | 5.16ms | 5.79ms | 1.1× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 6.68ms | 6.06ms | 0.9× | 1.5% | PASS |
| ac_reads | `groupby_scan` | 30.10ms | 32.64ms | 1.1× | 0.7% | PASS |
| ac_reads | `index_join` | 7.38ms | 9.20ms | 1.2× | 1.7% | PASS |
| ac_reads | `index_join_scan` | 3.80ms | 4.91ms | 1.3× | 3.3% | PASS |
| ac_reads | `types_table_scan` | 1.04s | 1.11s | 1.1× | 0.3% | PASS |
| ac_reads | `table_scan` | 1.16s | 1.20s | 1.0× | 0.3% | PASS |
| ac_reads | `oltp_read_only` | 132.82ms | 142.00ms | 1.1× | 0.6% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 20.83ms | 73.08ms | 3.5× | 4.3% | PASS |
| ac_writes | `oltp_insert_ac` | 22.98ms | 84.60ms | 3.7× | 4.6% | PASS |
| ac_writes | `oltp_update_index_ac` | 24.39ms | 95.25ms | 3.9× | 4.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.23ms | 77.32ms | 3.6× | 4.2% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 22.60ms | 87.16ms | 3.9× | 4.2% | PASS |
| ac_writes | `oltp_write_only_ac` | 22.50ms | 86.04ms | 3.8× | 3.7% | PASS |
| ac_writes | `types_delete_insert_ac` | 20.91ms | 79.75ms | 3.8× | 6.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 27.73ms | 92.65ms | 3.3× | 4.7% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 21.32ms | 24.40ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_range_select` | 8.76ms | 9.81ms | 1.1× | 2.4% | PASS |
| mem_reads | `oltp_sum_range` | 8.63ms | 9.31ms | 1.1× | 5.3% | PASS |
| mem_reads | `oltp_order_range` | 2.29ms | 2.36ms | 1.0× | 2.5% | PASS |
| mem_reads | `oltp_distinct_range` | 2.97ms | 3.22ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_index_scan` | 3.21ms | 3.85ms | 1.2× | 2.2% | PASS |
| mem_reads | `select_random_points` | 12.70ms | 14.89ms | 1.2× | 2.7% | PASS |
| mem_reads | `select_random_ranges` | 2.69ms | 3.49ms | 1.3× | 4.0% | PASS |
| mem_reads | `covering_index_scan` | 3.28ms | 2.90ms | 0.9× | 0.9% | PASS |
| mem_reads | `groupby_scan` | 25.97ms | 26.64ms | 1.0× | 1.7% | PASS |
| mem_reads | `index_join` | 5.35ms | 6.51ms | 1.2× | 1.0% | PASS |
| mem_reads | `index_join_scan` | 2.93ms | 3.61ms | 1.2× | 2.0% | PASS |
| mem_reads | `types_table_scan` | 888.45ms | 965.42ms | 1.1× | 0.1% | PASS |
| mem_reads | `table_scan` | 1.04s | 1.06s | 1.0× | 0.3% | PASS |
| mem_reads | `oltp_read_only` | 87.23ms | 97.42ms | 1.1× | 0.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 157.56ms | 231.25ms | 1.5× | 0.5% | PASS |
| mem_writes | `oltp_insert` | 15.38ms | 25.47ms | 1.7× | 1.4% | PASS |
| mem_writes | `oltp_update_index` | 51.16ms | 92.61ms | 1.8× | 1.6% | PASS |
| mem_writes | `oltp_update_non_index` | 32.41ms | 53.27ms | 1.6× | 1.5% | PASS |
| mem_writes | `oltp_delete_insert` | 34.64ms | 69.17ms | 2.0× | 1.9% | PASS |
| mem_writes | `oltp_write_only` | 18.35ms | 39.13ms | 2.1× | 2.0% | PASS |
| mem_writes | `types_delete_insert` | 22.32ms | 35.05ms | 1.6× | 2.4% | PASS |
| mem_writes | `oltp_read_write` | 56.34ms | 94.96ms | 1.7× | 1.4% | PASS |
| file_reads | `oltp_point_select` | 45.13ms | 30.39ms | 0.7× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 12.28ms | 10.73ms | 0.9× | 2.9% | PASS |
| file_reads | `oltp_sum_range` | 11.43ms | 10.24ms | 0.9× | 2.6% | PASS |
| file_reads | `oltp_order_range` | 2.61ms | 2.54ms | 1.0× | 2.3% | PASS |
| file_reads | `oltp_distinct_range` | 3.36ms | 3.38ms | 1.0× | 1.7% | PASS |
| file_reads | `oltp_index_scan` | 5.79ms | 4.60ms | 0.8× | 2.0% | PASS |
| file_reads | `select_random_points` | 15.85ms | 15.77ms | 1.0× | 2.0% | PASS |
| file_reads | `select_random_ranges` | 5.22ms | 4.20ms | 0.8× | 2.2% | PASS |
| file_reads | `covering_index_scan` | 5.96ms | 3.60ms | 0.6× | 1.2% | PASS |
| file_reads | `groupby_scan` | 26.16ms | 26.79ms | 1.0× | 1.0% | PASS |
| file_reads | `index_join` | 6.87ms | 7.10ms | 1.0× | 0.7% | PASS |
| file_reads | `index_join_scan` | 3.39ms | 4.15ms | 1.2× | 4.8% | PASS |
| file_reads | `types_table_scan` | 889.50ms | 964.60ms | 1.1× | 0.2% | PASS |
| file_reads | `table_scan` | 1.04s | 1.06s | 1.0× | 0.2% | PASS |
| file_reads | `oltp_read_only` | 122.50ms | 106.39ms | 0.9× | 0.5% | PASS |
| file_writes | `oltp_bulk_insert` | 210.19ms | 301.26ms | 1.4× | 1.5% | PASS |
| file_writes | `oltp_insert` | 50.45ms | 49.91ms | 1.0× | 1.8% | PASS |
| file_writes | `oltp_update_index` | 189.03ms | 173.88ms | 0.9× | 5.7% | PASS |
| file_writes | `oltp_update_non_index` | 138.75ms | 111.72ms | 0.8× | 1.5% | PASS |
| file_writes | `oltp_delete_insert` | 152.37ms | 137.02ms | 0.9× | 1.3% | PASS |
| file_writes | `oltp_write_only` | 110.82ms | 95.59ms | 0.9× | 0.7% | PASS |
| file_writes | `types_delete_insert` | 90.08ms | 69.91ms | 0.8× | 2.2% | PASS |
| file_writes | `oltp_read_write` | 154.05ms | 152.10ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_point_select` | 29.11ms | 30.64ms | 1.1× | 1.6% | PASS |
| ac_reads | `oltp_range_select` | 10.67ms | 10.77ms | 1.0× | 3.0% | PASS |
| ac_reads | `oltp_sum_range` | 9.98ms | 10.28ms | 1.0× | 3.5% | PASS |
| ac_reads | `oltp_order_range` | 2.43ms | 2.53ms | 1.0× | 1.8% | PASS |
| ac_reads | `oltp_distinct_range` | 3.24ms | 3.40ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_index_scan` | 4.11ms | 4.56ms | 1.1× | 1.5% | PASS |
| ac_reads | `select_random_points` | 14.10ms | 16.07ms | 1.1× | 2.8% | PASS |
| ac_reads | `select_random_ranges` | 3.57ms | 4.16ms | 1.2× | 2.1% | PASS |
| ac_reads | `covering_index_scan` | 4.22ms | 3.62ms | 0.9× | 1.6% | PASS |
| ac_reads | `groupby_scan` | 25.76ms | 26.72ms | 1.0× | 1.4% | PASS |
| ac_reads | `index_join` | 6.09ms | 7.13ms | 1.2× | 1.6% | PASS |
| ac_reads | `index_join_scan` | 3.42ms | 4.06ms | 1.2× | 4.2% | PASS |
| ac_reads | `types_table_scan` | 889.55ms | 964.41ms | 1.1× | 0.2% | PASS |
| ac_reads | `table_scan` | 1.04s | 1.06s | 1.0× | 0.2% | PASS |
| ac_reads | `oltp_read_only` | 99.44ms | 106.20ms | 1.1× | 0.7% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 30.68ms | 81.91ms | 2.7× | 4.8% | PASS |
| ac_writes | `oltp_insert_ac` | 32.65ms | 89.17ms | 2.7× | 4.8% | PASS |
| ac_writes | `oltp_update_index_ac` | 33.32ms | 99.12ms | 3.0× | 6.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 30.78ms | 85.51ms | 2.8× | 6.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 32.13ms | 91.67ms | 2.9× | 5.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 32.05ms | 91.49ms | 2.9× | 6.8% | PASS |
| ac_writes | `types_delete_insert_ac` | 30.24ms | 89.65ms | 3.0× | 6.1% | PASS |
| ac_writes | `oltp_read_write_ac` | 35.46ms | 96.11ms | 2.7× | 4.9% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 28.99ms | 36.11ms | 1.2× | 1.3% | PASS |
| mem_reads | `oltp_range_select` | 11.64ms | 14.45ms | 1.2× | 1.5% | PASS |
| mem_reads | `oltp_sum_range` | 10.99ms | 13.61ms | 1.2× | 1.8% | PASS |
| mem_reads | `oltp_order_range` | 2.74ms | 3.08ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_distinct_range` | 3.81ms | 4.17ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 4.26ms | 5.85ms | 1.4× | 1.6% | PASS |
| mem_reads | `select_random_points` | 16.52ms | 20.14ms | 1.2× | 1.8% | PASS |
| mem_reads | `select_random_ranges` | 3.68ms | 5.01ms | 1.4× | 1.8% | PASS |
| mem_reads | `covering_index_scan` | 4.34ms | 4.17ms | 1.0× | 1.2% | PASS |
| mem_reads | `groupby_scan` | 31.27ms | 33.75ms | 1.1× | 0.8% | PASS |
| mem_reads | `index_join` | 6.66ms | 8.65ms | 1.3× | 1.0% | PASS |
| mem_reads | `index_join_scan` | 3.89ms | 5.02ms | 1.3× | 2.9% | PASS |
| mem_reads | `types_table_scan` | 1.05s | 1.25s | 1.2× | 0.5% | PASS |
| mem_reads | `table_scan` | 1.20s | 1.35s | 1.1× | 0.4% | PASS |
| mem_reads | `oltp_read_only` | 112.75ms | 133.52ms | 1.2× | 0.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 240.88ms | 345.04ms | 1.4× | 1.2% | PASS |
| mem_writes | `oltp_insert` | 19.67ms | 36.84ms | 1.9× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 67.02ms | 129.09ms | 1.9× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 47.25ms | 75.21ms | 1.6× | 0.9% | PASS |
| mem_writes | `oltp_delete_insert` | 47.47ms | 99.09ms | 2.1× | 0.7% | PASS |
| mem_writes | `oltp_write_only` | 27.34ms | 58.94ms | 2.2× | 1.3% | PASS |
| mem_writes | `types_delete_insert` | 32.05ms | 51.38ms | 1.6× | 1.3% | PASS |
| mem_writes | `oltp_read_write` | 80.69ms | 140.99ms | 1.7× | 1.2% | PASS |
| file_reads | `oltp_point_select` | 99.00ms | 55.62ms | 0.6× | 1.4% | PASS |
| file_reads | `oltp_range_select` | 18.98ms | 16.65ms | 0.9× | 1.2% | PASS |
| file_reads | `oltp_sum_range` | 18.48ms | 15.90ms | 0.9× | 1.2% | PASS |
| file_reads | `oltp_order_range` | 3.58ms | 3.46ms | 1.0× | 1.8% | PASS |
| file_reads | `oltp_distinct_range` | 4.67ms | 4.54ms | 1.0× | 1.4% | PASS |
| file_reads | `oltp_index_scan` | 11.43ms | 8.22ms | 0.7× | 1.5% | PASS |
| file_reads | `select_random_points` | 25.17ms | 23.14ms | 0.9× | 2.2% | PASS |
| file_reads | `select_random_ranges` | 10.65ms | 7.07ms | 0.7× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 11.30ms | 6.51ms | 0.6× | 1.2% | PASS |
| file_reads | `groupby_scan` | 32.52ms | 34.35ms | 1.1× | 1.3% | PASS |
| file_reads | `index_join` | 10.64ms | 11.08ms | 1.0× | 2.3% | PASS |
| file_reads | `index_join_scan` | 5.04ms | 5.58ms | 1.1× | 3.2% | PASS |
| file_reads | `types_table_scan` | 1.05s | 1.25s | 1.2× | 0.4% | PASS |
| file_reads | `table_scan` | 1.21s | 1.35s | 1.1× | 0.8% | PASS |
| file_reads | `oltp_read_only` | 218.64ms | 163.84ms | 0.7× | 1.6% | PASS |
| file_writes | `oltp_bulk_insert` | 260.12ms | 357.59ms | 1.4× | 1.3% | PASS |
| file_writes | `oltp_insert` | 31.37ms | 44.31ms | 1.4× | 2.5% | PASS |
| file_writes | `oltp_update_index` | 108.17ms | 151.59ms | 1.4× | 2.9% | PASS |
| file_writes | `oltp_update_non_index` | 77.99ms | 90.84ms | 1.2× | 2.3% | PASS |
| file_writes | `oltp_delete_insert` | 78.59ms | 112.78ms | 1.4× | 1.4% | PASS |
| file_writes | `oltp_write_only` | 54.01ms | 69.84ms | 1.3× | 2.6% | PASS |
| file_writes | `types_delete_insert` | 50.00ms | 59.81ms | 1.2× | 1.3% | PASS |
| file_writes | `oltp_read_write` | 110.99ms | 152.34ms | 1.4× | 1.8% | PASS |
| ac_reads | `oltp_point_select` | 53.34ms | 55.16ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_range_select` | 15.09ms | 16.61ms | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_sum_range` | 14.28ms | 15.82ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_order_range` | 3.43ms | 3.44ms | 1.0× | 2.7% | PASS |
| ac_reads | `oltp_distinct_range` | 4.50ms | 4.58ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_index_scan` | 7.63ms | 8.47ms | 1.1× | 1.3% | PASS |
| ac_reads | `select_random_points` | 21.74ms | 23.50ms | 1.1× | 2.4% | PASS |
| ac_reads | `select_random_ranges` | 6.72ms | 7.07ms | 1.1× | 1.7% | PASS |
| ac_reads | `covering_index_scan` | 7.63ms | 6.53ms | 0.9× | 1.3% | PASS |
| ac_reads | `groupby_scan` | 32.66ms | 34.54ms | 1.1× | 0.9% | PASS |
| ac_reads | `index_join` | 9.20ms | 11.41ms | 1.2× | 1.7% | PASS |
| ac_reads | `index_join_scan` | 4.81ms | 5.71ms | 1.2× | 1.6% | PASS |
| ac_reads | `types_table_scan` | 1.10s | 1.26s | 1.1× | 2.6% | PASS |
| ac_reads | `table_scan` | 1.38s | 1.39s | 1.0× | 6.9% | PASS |
| ac_reads | `oltp_read_only` | 157.36ms | 165.13ms | 1.0× | 2.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.83ms | 75.27ms | 3.3× | 5.7% | PASS |
| ac_writes | `oltp_insert_ac` | 25.61ms | 90.69ms | 3.5× | 5.6% | PASS |
| ac_writes | `oltp_update_index_ac` | 28.91ms | 103.47ms | 3.6× | 7.8% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.97ms | 86.52ms | 3.5× | 5.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.04ms | 91.97ms | 3.7× | 5.7% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.06ms | 91.41ms | 3.6× | 4.7% | PASS |
| ac_writes | `types_delete_insert_ac` | 23.57ms | 87.92ms | 3.7× | 7.2% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.97ms | 99.35ms | 3.1× | 6.2% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.94ms | 39.68ms | 1.2× | 1.8% | PASS |
| mem_reads | `oltp_range_select` | 19.74ms | 21.56ms | 1.1× | 1.8% | PASS |
| mem_reads | `oltp_sum_range` | 18.31ms | 20.78ms | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_order_range` | 3.62ms | 3.85ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_distinct_range` | 4.74ms | 4.98ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 4.73ms | 6.18ms | 1.3× | 1.3% | PASS |
| mem_reads | `select_random_points` | 29.66ms | 33.31ms | 1.1× | 1.5% | PASS |
| mem_reads | `select_random_ranges` | 8.03ms | 8.98ms | 1.1× | 1.2% | PASS |
| mem_reads | `covering_index_scan` | 4.28ms | 4.39ms | 1.0× | 2.3% | PASS |
| mem_reads | `groupby_scan` | 37.31ms | 39.68ms | 1.1× | 0.5% | PASS |
| mem_reads | `index_join` | 8.38ms | 10.81ms | 1.3× | 1.8% | PASS |
| mem_reads | `index_join_scan` | 4.26ms | 5.53ms | 1.3× | 2.3% | PASS |
| mem_reads | `types_table_scan` | 1.16s | 1.29s | 1.1× | 1.0% | PASS |
| mem_reads | `table_scan` | 1.46s | 1.40s | 1.0× | 0.8% | PASS |
| mem_reads | `oltp_read_only` | 165.66ms | 175.49ms | 1.1× | 1.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 249.33ms | 352.43ms | 1.4× | 1.3% | PASS |
| mem_writes | `oltp_insert` | 19.65ms | 35.38ms | 1.8× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 71.92ms | 125.58ms | 1.7× | 1.5% | PASS |
| mem_writes | `oltp_update_non_index` | 52.37ms | 78.20ms | 1.5× | 1.2% | PASS |
| mem_writes | `oltp_delete_insert` | 51.18ms | 96.89ms | 1.9× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 27.80ms | 58.05ms | 2.1× | 1.5% | PASS |
| mem_writes | `types_delete_insert` | 33.95ms | 54.01ms | 1.6× | 1.3% | PASS |
| mem_writes | `oltp_read_write` | 110.98ms | 164.97ms | 1.5× | 1.4% | PASS |
| file_reads | `oltp_point_select` | 106.08ms | 60.19ms | 0.6× | 1.2% | PASS |
| file_reads | `oltp_range_select` | 27.95ms | 24.16ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_sum_range` | 26.15ms | 23.35ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_order_range` | 4.53ms | 4.28ms | 0.9× | 2.0% | PASS |
| file_reads | `oltp_distinct_range` | 5.70ms | 5.42ms | 1.0× | 1.8% | PASS |
| file_reads | `oltp_index_scan` | 11.94ms | 8.36ms | 0.7× | 1.7% | PASS |
| file_reads | `select_random_points` | 38.12ms | 36.11ms | 0.9× | 1.8% | PASS |
| file_reads | `select_random_ranges` | 15.32ms | 11.19ms | 0.7× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 11.53ms | 6.49ms | 0.6× | 1.2% | PASS |
| file_reads | `groupby_scan` | 37.97ms | 40.13ms | 1.1× | 0.8% | PASS |
| file_reads | `index_join` | 12.35ms | 12.18ms | 1.0× | 1.6% | PASS |
| file_reads | `index_join_scan` | 5.18ms | 5.83ms | 1.1× | 1.4% | PASS |
| file_reads | `types_table_scan` | 1.09s | 1.27s | 1.2× | 1.2% | PASS |
| file_reads | `table_scan` | 1.33s | 1.37s | 1.0× | 2.9% | PASS |
| file_reads | `oltp_read_only` | 260.17ms | 200.01ms | 0.8× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 263.85ms | 358.88ms | 1.4× | 1.1% | PASS |
| file_writes | `oltp_insert` | 26.12ms | 40.62ms | 1.6× | 1.5% | PASS |
| file_writes | `oltp_update_index` | 98.35ms | 133.15ms | 1.4× | 1.7% | PASS |
| file_writes | `oltp_update_non_index` | 77.17ms | 90.50ms | 1.2× | 1.0% | PASS |
| file_writes | `oltp_delete_insert` | 77.40ms | 107.80ms | 1.4× | 1.1% | PASS |
| file_writes | `oltp_write_only` | 51.12ms | 67.52ms | 1.3× | 1.5% | PASS |
| file_writes | `types_delete_insert` | 49.45ms | 59.18ms | 1.2× | 1.3% | PASS |
| file_writes | `oltp_read_write` | 132.52ms | 171.96ms | 1.3× | 1.4% | PASS |
| ac_reads | `oltp_point_select` | 57.49ms | 59.81ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_range_select` | 22.85ms | 23.96ms | 1.0× | 1.8% | PASS |
| ac_reads | `oltp_sum_range` | 21.42ms | 23.35ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_order_range` | 4.20ms | 4.33ms | 1.0× | 2.5% | PASS |
| ac_reads | `oltp_distinct_range` | 5.31ms | 5.48ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_index_scan` | 7.46ms | 8.41ms | 1.1× | 1.9% | PASS |
| ac_reads | `select_random_points` | 32.14ms | 36.13ms | 1.1× | 1.9% | PASS |
| ac_reads | `select_random_ranges` | 10.56ms | 11.21ms | 1.1× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 7.18ms | 6.47ms | 0.9× | 1.7% | PASS |
| ac_reads | `groupby_scan` | 38.05ms | 40.67ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 10.31ms | 12.53ms | 1.2× | 2.1% | PASS |
| ac_reads | `index_join_scan` | 4.78ms | 5.91ms | 1.2× | 1.7% | PASS |
| ac_reads | `types_table_scan` | 1.13s | 1.27s | 1.1× | 1.4% | PASS |
| ac_reads | `table_scan` | 1.39s | 1.38s | 1.0× | 2.4% | PASS |
| ac_reads | `oltp_read_only` | 197.69ms | 204.50ms | 1.0× | 1.1% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.55ms | 73.57ms | 3.4× | 4.8% | PASS |
| ac_writes | `oltp_insert_ac` | 24.30ms | 92.97ms | 3.8× | 3.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.20ms | 101.12ms | 3.7× | 5.4% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.07ms | 86.09ms | 3.7× | 7.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.88ms | 97.99ms | 3.9× | 6.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.57ms | 95.39ms | 3.7× | 6.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 21.53ms | 85.80ms | 4.0× | 4.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 33.35ms | 103.61ms | 3.1× | 7.2% | PASS |

</details>

</details>

## Version-control latency

Wall time: 1m 57s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 59.75ms | 130.00ms | 46.0% | 0.6% | PASS |
| `status_dirty_many_tables` | 61.81ms | 130.00ms | 47.5% | 0.4% | PASS |
| `diff_regular_working_one_table` | 57.54ms | 120.00ms | 47.9% | 0.6% | PASS |
| `diff_regular_working_many_tables` | 65.98ms | 140.00ms | 47.1% | 0.6% | PASS |
| `diff_stat_working_many_tables` | 65.68ms | 140.00ms | 46.9% | 0.3% | PASS |
| `diff_schema_working_many_tables` | 65.88ms | 140.00ms | 47.1% | 0.3% | PASS |
| `branch_list_many_branches` | 17.80ms | 35.00ms | 50.9% | 0.8% | PASS |
| `branch_create_delete` | 26.09ms | 40.00ms | 65.2% | 3.1% | PASS |
| `checkout_branch_clean` | 100.09ms | 150.00ms | 66.7% | 10.0% | PASS |
| `merge_data_no_conflicts` | 32.30ms | 50.00ms | 64.6% | 5.6% | PASS |
| `merge_schema_no_conflicts` | 18.65ms | 35.00ms | 53.3% | 3.6% | PASS |
| `merge_data_conflicts` | 98.17ms | 180.00ms | 54.5% | 0.2% | PASS |
| `merge_data_conflicts_with_resolve` | 98.34ms | 180.00ms | 54.6% | 0.3% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
