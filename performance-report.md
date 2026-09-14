# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-09-14 11:10 UTC
>
> Commit: [`0edd17c8e6756aeff55ef7cfc640a4a73fde79e1`](https://github.com/dolthub/doltlite/commit/0edd17c8e6756aeff55ef7cfc640a4a73fde79e1)
>
> Runner: ubuntu24 20260907.300.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/34829155005)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 8.80s | 9.34s | 1.1× | 1.3% | **PASS** |
| Writes | 1.78s | 2.90s | 1.6× | 1.1% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.36s | 9.47s | 1.0× | 1.1% | **PASS** |
| Writes | 4.24s | 4.28s | 1.0× | 3.2% | **PASS** |
| Autocommit writes | 1.63s | 5.07s | 3.1× | 15.5% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 1.98s | 2.11s | 1.1× | 1.5% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 1.76s | 1.81s | 1.0× | 2.5% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.41s | 2.58s | 1.1× | 0.8% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.66s | 2.84s | 1.1× | 1.0% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 307.97ms | 493.53ms | 1.6× | 1.1% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 394.39ms | 633.35ms | 1.6× | 1.7% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 479.47ms | 789.21ms | 1.6× | 0.9% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 597.15ms | 982.21ms | 1.6× | 1.0% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.07s | 2.13s | 1.0× | 1.4% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 1.91s | 1.82s | 1.0× | 1.4% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.50s | 2.60s | 1.0× | 0.9% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.88s | 2.91s | 1.0× | 1.2% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 759.32ms | 804.00ms | 1.1× | 5.1% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.47s | 1.12s | 0.8× | 22.1% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 1.22s | 1.30s | 1.1× | 2.0% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 792.98ms | 1.06s | 1.3× | 1.1% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.03s | 2.15s | 1.1× | 1.4% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 1.72s | 1.76s | 1.0× | 1.6% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.43s | 2.61s | 1.1× | 0.9% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.73s | 2.91s | 1.1× | 1.0% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 297.01ms | 910.34ms | 3.1× | 19.7% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 847.71ms | 2.37s | 2.8× | 52.9% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 219.68ms | 842.14ms | 3.8× | 12.5% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 262.86ms | 940.53ms | 3.6× | 6.9% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 17.66ms | 20.31ms | 1.1× | 2.1% | PASS |
| mem_reads | `oltp_range_select` | 8.00ms | 8.42ms | 1.1× | 2.1% | PASS |
| mem_reads | `oltp_sum_range` | 7.22ms | 8.09ms | 1.1× | 1.7% | PASS |
| mem_reads | `oltp_order_range` | 2.11ms | 2.23ms | 1.1× | 1.7% | PASS |
| mem_reads | `oltp_distinct_range` | 2.88ms | 3.14ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_index_scan` | 2.94ms | 3.51ms | 1.2× | 2.0% | PASS |
| mem_reads | `select_random_points` | 8.25ms | 8.55ms | 1.0× | 3.1% | PASS |
| mem_reads | `select_random_ranges` | 3.27ms | 2.78ms | 0.9× | 2.4% | PASS |
| mem_reads | `covering_index_scan` | 5.35ms | 2.92ms | 0.5× | 0.6% | PASS |
| mem_reads | `groupby_scan` | 24.47ms | 26.43ms | 1.1× | 1.5% | PASS |
| mem_reads | `index_join` | 4.38ms | 5.90ms | 1.3× | 1.0% | PASS |
| mem_reads | `index_join_scan` | 2.40ms | 3.54ms | 1.5× | 1.4% | PASS |
| mem_reads | `types_table_scan` | 849.31ms | 915.08ms | 1.1× | 0.2% | PASS |
| mem_reads | `table_scan` | 962.41ms | 1.01s | 1.1× | 0.4% | PASS |
| mem_reads | `oltp_read_only` | 77.48ms | 87.24ms | 1.1× | 0.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 122.40ms | 180.09ms | 1.5× | 0.4% | PASS |
| mem_writes | `oltp_insert` | 10.86ms | 18.93ms | 1.7× | 1.1% | PASS |
| mem_writes | `oltp_update_index` | 37.22ms | 68.87ms | 1.9× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 24.69ms | 41.42ms | 1.7× | 1.2% | PASS |
| mem_writes | `oltp_delete_insert` | 33.04ms | 49.96ms | 1.5× | 1.2% | PASS |
| mem_writes | `oltp_write_only` | 15.46ms | 31.37ms | 2.0× | 1.3% | PASS |
| mem_writes | `types_delete_insert` | 17.58ms | 26.01ms | 1.5× | 0.9% | PASS |
| mem_writes | `oltp_read_write` | 46.72ms | 76.88ms | 1.6× | 0.7% | PASS |
| file_reads | `oltp_point_select` | 41.94ms | 26.00ms | 0.6× | 0.7% | PASS |
| file_reads | `oltp_range_select` | 10.70ms | 9.32ms | 0.9× | 1.8% | PASS |
| file_reads | `oltp_sum_range` | 9.88ms | 8.98ms | 0.9× | 1.9% | PASS |
| file_reads | `oltp_order_range` | 2.49ms | 2.44ms | 1.0× | 1.7% | PASS |
| file_reads | `oltp_distinct_range` | 3.27ms | 3.24ms | 1.0× | 0.9% | PASS |
| file_reads | `oltp_index_scan` | 5.67ms | 4.50ms | 0.8× | 2.2% | PASS |
| file_reads | `select_random_points` | 11.59ms | 10.12ms | 0.9× | 1.4% | PASS |
| file_reads | `select_random_ranges` | 6.03ms | 3.61ms | 0.6× | 1.6% | PASS |
| file_reads | `covering_index_scan` | 8.21ms | 3.80ms | 0.5× | 2.1% | PASS |
| file_reads | `groupby_scan` | 25.22ms | 26.31ms | 1.0× | 0.8% | PASS |
| file_reads | `index_join` | 5.98ms | 6.91ms | 1.2× | 2.1% | PASS |
| file_reads | `index_join_scan` | 2.82ms | 3.82ms | 1.4× | 1.4% | PASS |
| file_reads | `types_table_scan` | 852.48ms | 916.61ms | 1.1× | 0.3% | PASS |
| file_reads | `table_scan` | 965.43ms | 1.01s | 1.0× | 0.2% | PASS |
| file_reads | `oltp_read_only` | 113.47ms | 96.05ms | 0.8× | 0.6% | PASS |
| file_writes | `oltp_bulk_insert` | 162.28ms | 231.45ms | 1.4× | 5.2% | PASS |
| file_writes | `oltp_insert` | 19.92ms | 29.50ms | 1.5× | 7.3% | PASS |
| file_writes | `oltp_update_index` | 124.28ms | 119.86ms | 1.0× | 4.9% | PASS |
| file_writes | `oltp_update_non_index` | 98.13ms | 89.36ms | 0.9× | 2.7% | PASS |
| file_writes | `oltp_delete_insert` | 106.36ms | 99.77ms | 0.9× | 5.1% | PASS |
| file_writes | `oltp_write_only` | 76.46ms | 69.14ms | 0.9× | 7.0% | PASS |
| file_writes | `types_delete_insert` | 60.55ms | 51.77ms | 0.9× | 12.4% | PASS |
| file_writes | `oltp_read_write` | 111.35ms | 113.15ms | 1.0× | 2.8% | PASS |
| ac_reads | `oltp_point_select` | 25.94ms | 26.33ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_range_select` | 9.45ms | 9.56ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_sum_range` | 9.04ms | 9.23ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_order_range` | 2.38ms | 2.46ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_distinct_range` | 3.17ms | 3.30ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_index_scan` | 4.20ms | 4.55ms | 1.1× | 2.3% | PASS |
| ac_reads | `select_random_points` | 9.98ms | 10.06ms | 1.0× | 2.2% | PASS |
| ac_reads | `select_random_ranges` | 4.44ms | 3.63ms | 0.8× | 2.0% | PASS |
| ac_reads | `covering_index_scan` | 6.38ms | 3.75ms | 0.6× | 1.5% | PASS |
| ac_reads | `groupby_scan` | 25.30ms | 26.43ms | 1.0× | 1.3% | PASS |
| ac_reads | `index_join` | 5.19ms | 6.65ms | 1.3× | 2.4% | PASS |
| ac_reads | `index_join_scan` | 2.72ms | 3.82ms | 1.4× | 1.9% | PASS |
| ac_reads | `types_table_scan` | 861.93ms | 924.82ms | 1.1× | 0.5% | PASS |
| ac_reads | `table_scan` | 969.39ms | 1.01s | 1.0× | 0.5% | PASS |
| ac_reads | `oltp_read_only` | 90.31ms | 96.70ms | 1.1× | 0.7% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 36.06ms | 109.47ms | 3.0× | 55.9% | PASS |
| ac_writes | `oltp_insert_ac` | 36.80ms | 105.53ms | 2.9× | 19.7% | PASS |
| ac_writes | `oltp_update_index_ac` | 38.65ms | 134.60ms | 3.5× | 19.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 35.96ms | 123.49ms | 3.4× | 31.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 36.98ms | 104.92ms | 2.8× | 13.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 34.45ms | 100.57ms | 2.9× | 9.5% | PASS |
| ac_writes | `types_delete_insert_ac` | 38.20ms | 109.90ms | 2.9× | 19.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 39.90ms | 121.86ms | 3.1× | 27.0% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.34ms | 23.85ms | 1.0× | 3.0% | PASS |
| mem_reads | `oltp_range_select` | 11.40ms | 8.74ms | 0.8× | 2.5% | PASS |
| mem_reads | `oltp_sum_range` | 10.53ms | 8.86ms | 0.8× | 2.0% | PASS |
| mem_reads | `oltp_order_range` | 2.25ms | 1.99ms | 0.9× | 2.7% | PASS |
| mem_reads | `oltp_distinct_range` | 2.77ms | 2.52ms | 0.9× | 1.3% | PASS |
| mem_reads | `oltp_index_scan` | 2.60ms | 3.94ms | 1.5× | 2.5% | PASS |
| mem_reads | `select_random_points` | 16.65ms | 14.07ms | 0.8× | 4.4% | PASS |
| mem_reads | `select_random_ranges` | 4.62ms | 3.59ms | 0.8× | 3.3% | PASS |
| mem_reads | `covering_index_scan` | 4.09ms | 2.95ms | 0.7× | 3.2% | PASS |
| mem_reads | `groupby_scan` | 21.39ms | 20.42ms | 1.0× | 0.8% | PASS |
| mem_reads | `index_join` | 8.22ms | 5.70ms | 0.7× | 3.4% | PASS |
| mem_reads | `index_join_scan` | 2.81ms | 4.57ms | 1.6× | 3.0% | PASS |
| mem_reads | `types_table_scan` | 720.68ms | 769.28ms | 1.1× | 1.3% | PASS |
| mem_reads | `table_scan` | 844.75ms | 862.83ms | 1.0× | 1.4% | PASS |
| mem_reads | `oltp_read_only` | 85.03ms | 78.80ms | 0.9× | 2.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 151.43ms | 222.62ms | 1.5× | 1.2% | PASS |
| mem_writes | `oltp_insert` | 11.06ms | 24.00ms | 2.2× | 1.4% | PASS |
| mem_writes | `oltp_update_index` | 47.08ms | 96.21ms | 2.0× | 2.8% | PASS |
| mem_writes | `oltp_update_non_index` | 35.85ms | 57.02ms | 1.6× | 2.0% | PASS |
| mem_writes | `oltp_delete_insert` | 37.64ms | 70.18ms | 1.9× | 1.4% | PASS |
| mem_writes | `oltp_write_only` | 19.21ms | 40.46ms | 2.1× | 1.1% | PASS |
| mem_writes | `types_delete_insert` | 26.52ms | 34.09ms | 1.3× | 2.6% | PASS |
| mem_writes | `oltp_read_write` | 65.59ms | 88.78ms | 1.4× | 3.6% | PASS |
| file_reads | `oltp_point_select` | 79.89ms | 38.16ms | 0.5× | 1.4% | PASS |
| file_reads | `oltp_range_select` | 17.45ms | 10.43ms | 0.6× | 2.0% | PASS |
| file_reads | `oltp_sum_range` | 16.44ms | 10.33ms | 0.6× | 1.5% | PASS |
| file_reads | `oltp_order_range` | 3.01ms | 2.26ms | 0.8× | 1.2% | PASS |
| file_reads | `oltp_distinct_range` | 3.55ms | 2.79ms | 0.8× | 1.0% | PASS |
| file_reads | `oltp_index_scan` | 8.51ms | 5.90ms | 0.7× | 1.1% | PASS |
| file_reads | `select_random_points` | 21.51ms | 15.04ms | 0.7× | 1.6% | PASS |
| file_reads | `select_random_ranges` | 10.37ms | 5.07ms | 0.5× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 10.08ms | 4.84ms | 0.5× | 2.6% | PASS |
| file_reads | `groupby_scan` | 21.68ms | 20.28ms | 0.9× | 1.1% | PASS |
| file_reads | `index_join` | 11.25ms | 7.50ms | 0.7× | 1.9% | PASS |
| file_reads | `index_join_scan` | 3.41ms | 4.50ms | 1.3× | 1.7% | PASS |
| file_reads | `types_table_scan` | 712.99ms | 753.10ms | 1.1× | 0.6% | PASS |
| file_reads | `table_scan` | 826.87ms | 844.49ms | 1.0× | 0.9% | PASS |
| file_reads | `oltp_read_only` | 167.25ms | 98.28ms | 0.6× | 2.1% | PASS |
| file_writes | `oltp_bulk_insert` | 246.37ms | 288.71ms | 1.2× | 15.0% | PASS |
| file_writes | `oltp_insert` | 32.98ms | 48.74ms | 1.5× | 51.3% | PASS |
| file_writes | `oltp_update_index` | 219.80ms | 187.23ms | 0.9× | 21.7% | PASS |
| file_writes | `oltp_update_non_index` | 243.51ms | 130.83ms | 0.5× | 37.5% | PASS |
| file_writes | `oltp_delete_insert` | 243.23ms | 139.56ms | 0.6× | 22.5% | PASS |
| file_writes | `oltp_write_only` | 128.47ms | 95.71ms | 0.7× | 17.9% | PASS |
| file_writes | `types_delete_insert` | 179.99ms | 87.04ms | 0.5× | 30.2% | PASS |
| file_writes | `oltp_read_write` | 176.27ms | 139.06ms | 0.8× | 18.1% | PASS |
| ac_reads | `oltp_point_select` | 40.80ms | 36.96ms | 0.9× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 13.02ms | 10.12ms | 0.8× | 1.6% | PASS |
| ac_reads | `oltp_sum_range` | 12.51ms | 10.20ms | 0.8× | 1.2% | PASS |
| ac_reads | `oltp_order_range` | 2.65ms | 2.22ms | 0.8× | 1.4% | PASS |
| ac_reads | `oltp_distinct_range` | 3.14ms | 2.77ms | 0.9× | 1.2% | PASS |
| ac_reads | `oltp_index_scan` | 4.81ms | 5.75ms | 1.2× | 2.2% | PASS |
| ac_reads | `select_random_points` | 18.08ms | 15.01ms | 0.8× | 1.7% | PASS |
| ac_reads | `select_random_ranges` | 6.68ms | 5.00ms | 0.7× | 1.1% | PASS |
| ac_reads | `covering_index_scan` | 6.31ms | 4.66ms | 0.7× | 2.1% | PASS |
| ac_reads | `groupby_scan` | 20.93ms | 20.11ms | 1.0× | 1.1% | PASS |
| ac_reads | `index_join` | 9.46ms | 7.34ms | 0.8× | 2.3% | PASS |
| ac_reads | `index_join_scan` | 3.05ms | 4.41ms | 1.4× | 1.7% | PASS |
| ac_reads | `types_table_scan` | 685.12ms | 723.78ms | 1.1× | 1.0% | PASS |
| ac_reads | `table_scan` | 796.94ms | 821.01ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_read_only` | 101.04ms | 91.04ms | 0.9× | 1.7% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 115.24ms | 245.14ms | 2.1× | 45.2% | PASS |
| ac_writes | `oltp_insert_ac` | 76.66ms | 311.27ms | 4.1× | 66.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 95.06ms | 250.63ms | 2.6× | 49.4% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 114.70ms | 253.14ms | 2.2× | 60.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 116.32ms | 329.94ms | 2.8× | 59.4% | PASS |
| ac_writes | `oltp_write_only_ac` | 122.83ms | 362.35ms | 3.0× | 52.9% | PASS |
| ac_writes | `types_delete_insert_ac` | 88.37ms | 271.88ms | 3.1× | 48.0% | PASS |
| ac_writes | `oltp_read_write_ac` | 118.53ms | 349.24ms | 2.9× | 52.8% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 30.05ms | 30.05ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_range_select` | 12.96ms | 11.45ms | 0.9× | 0.8% | PASS |
| mem_reads | `oltp_sum_range` | 12.78ms | 11.08ms | 0.9× | 0.6% | PASS |
| mem_reads | `oltp_order_range` | 2.82ms | 2.79ms | 1.0× | 0.8% | PASS |
| mem_reads | `oltp_distinct_range` | 3.72ms | 3.82ms | 1.0× | 0.8% | PASS |
| mem_reads | `oltp_index_scan` | 3.48ms | 4.91ms | 1.4× | 1.3% | PASS |
| mem_reads | `select_random_points` | 19.77ms | 18.80ms | 1.0× | 1.2% | PASS |
| mem_reads | `select_random_ranges` | 5.49ms | 4.43ms | 0.8× | 1.1% | PASS |
| mem_reads | `covering_index_scan` | 6.24ms | 3.70ms | 0.6× | 2.1% | PASS |
| mem_reads | `groupby_scan` | 31.45ms | 31.88ms | 1.0× | 0.7% | PASS |
| mem_reads | `index_join` | 10.06ms | 7.96ms | 0.8× | 1.3% | PASS |
| mem_reads | `index_join_scan` | 3.10ms | 4.98ms | 1.6× | 0.7% | PASS |
| mem_reads | `types_table_scan` | 1.00s | 1.10s | 1.1× | 0.3% | PASS |
| mem_reads | `table_scan` | 1.15s | 1.22s | 1.1× | 0.5% | PASS |
| mem_reads | `oltp_read_only` | 114.57ms | 114.81ms | 1.0× | 1.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 192.25ms | 278.76ms | 1.5× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 15.38ms | 30.58ms | 2.0× | 1.1% | PASS |
| mem_writes | `oltp_update_index` | 55.09ms | 118.08ms | 2.1× | 0.7% | PASS |
| mem_writes | `oltp_update_non_index` | 40.16ms | 69.43ms | 1.7× | 0.9% | PASS |
| mem_writes | `oltp_delete_insert` | 44.96ms | 85.17ms | 1.9× | 1.2% | PASS |
| mem_writes | `oltp_write_only` | 23.10ms | 49.36ms | 2.1× | 1.0% | PASS |
| mem_writes | `types_delete_insert` | 31.49ms | 42.16ms | 1.3× | 0.9% | PASS |
| mem_writes | `oltp_read_write` | 77.05ms | 115.67ms | 1.5× | 0.9% | PASS |
| file_reads | `oltp_point_select` | 60.88ms | 38.07ms | 0.6× | 0.7% | PASS |
| file_reads | `oltp_range_select` | 16.31ms | 12.58ms | 0.8× | 0.9% | PASS |
| file_reads | `oltp_sum_range` | 16.28ms | 12.31ms | 0.8× | 0.8% | PASS |
| file_reads | `oltp_order_range` | 3.22ms | 2.95ms | 0.9× | 0.9% | PASS |
| file_reads | `oltp_distinct_range` | 4.14ms | 3.98ms | 1.0× | 0.9% | PASS |
| file_reads | `oltp_index_scan` | 6.80ms | 6.19ms | 0.9× | 1.2% | PASS |
| file_reads | `select_random_points` | 23.68ms | 20.05ms | 0.8× | 1.0% | PASS |
| file_reads | `select_random_ranges` | 8.91ms | 5.44ms | 0.6× | 0.8% | PASS |
| file_reads | `covering_index_scan` | 9.74ms | 4.95ms | 0.5× | 1.0% | PASS |
| file_reads | `groupby_scan` | 32.01ms | 32.02ms | 1.0× | 0.6% | PASS |
| file_reads | `index_join` | 12.28ms | 9.51ms | 0.8× | 1.1% | PASS |
| file_reads | `index_join_scan` | 3.56ms | 5.32ms | 1.5× | 1.1% | PASS |
| file_reads | `types_table_scan` | 1.00s | 1.10s | 1.1× | 0.3% | PASS |
| file_reads | `table_scan` | 1.14s | 1.22s | 1.1× | 0.3% | PASS |
| file_reads | `oltp_read_only` | 158.39ms | 126.43ms | 0.8× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 265.14ms | 355.21ms | 1.3× | 3.5% | PASS |
| file_writes | `oltp_insert` | 31.28ms | 60.38ms | 1.9× | 3.0% | PASS |
| file_writes | `oltp_update_index` | 172.95ms | 207.26ms | 1.2× | 1.8% | PASS |
| file_writes | `oltp_update_non_index` | 144.82ms | 135.67ms | 0.9× | 1.0% | PASS |
| file_writes | `oltp_delete_insert` | 172.76ms | 159.71ms | 0.9× | 1.8% | PASS |
| file_writes | `oltp_write_only` | 123.41ms | 110.90ms | 0.9× | 1.0% | PASS |
| file_writes | `types_delete_insert` | 131.09ms | 89.42ms | 0.7× | 5.2% | PASS |
| file_writes | `oltp_read_write` | 180.57ms | 178.65ms | 1.0× | 2.2% | PASS |
| ac_reads | `oltp_point_select` | 39.64ms | 37.99ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_range_select` | 14.14ms | 12.52ms | 0.9× | 0.7% | PASS |
| ac_reads | `oltp_sum_range` | 14.06ms | 12.28ms | 0.9× | 0.9% | PASS |
| ac_reads | `oltp_order_range` | 3.02ms | 2.97ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_distinct_range` | 3.93ms | 4.00ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_index_scan` | 4.77ms | 6.20ms | 1.3× | 1.3% | PASS |
| ac_reads | `select_random_points` | 21.76ms | 20.11ms | 0.9× | 0.8% | PASS |
| ac_reads | `select_random_ranges` | 6.84ms | 5.43ms | 0.8× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 7.47ms | 4.92ms | 0.7× | 0.9% | PASS |
| ac_reads | `groupby_scan` | 31.77ms | 32.17ms | 1.0× | 1.0% | PASS |
| ac_reads | `index_join` | 11.27ms | 9.42ms | 0.8× | 1.1% | PASS |
| ac_reads | `index_join_scan` | 3.42ms | 5.31ms | 1.6× | 1.2% | PASS |
| ac_reads | `types_table_scan` | 1.00s | 1.10s | 1.1× | 0.4% | PASS |
| ac_reads | `table_scan` | 1.15s | 1.23s | 1.1× | 0.6% | PASS |
| ac_reads | `oltp_read_only` | 127.54ms | 126.00ms | 1.0× | 0.5% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 25.08ms | 84.28ms | 3.4× | 6.1% | PASS |
| ac_writes | `oltp_insert_ac` | 27.02ms | 109.10ms | 4.0× | 12.8% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.76ms | 110.17ms | 4.0× | 12.0% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 26.20ms | 98.53ms | 3.8× | 16.2% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 27.09ms | 115.42ms | 4.3× | 15.4% | PASS |
| ac_writes | `oltp_write_only_ac` | 27.27ms | 110.85ms | 4.1× | 12.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 26.27ms | 97.63ms | 3.7× | 10.3% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.99ms | 116.17ms | 3.5× | 15.6% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.65ms | 42.32ms | 1.3× | 1.6% | PASS |
| mem_reads | `oltp_range_select` | 20.10ms | 21.58ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_sum_range` | 18.01ms | 20.20ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_order_range` | 3.55ms | 3.87ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_distinct_range` | 4.71ms | 5.08ms | 1.1× | 1.7% | PASS |
| mem_reads | `oltp_index_scan` | 4.56ms | 6.27ms | 1.4× | 1.4% | PASS |
| mem_reads | `select_random_points` | 27.94ms | 32.74ms | 1.2× | 1.5% | PASS |
| mem_reads | `select_random_ranges` | 7.74ms | 9.14ms | 1.2× | 0.9% | PASS |
| mem_reads | `covering_index_scan` | 7.80ms | 4.50ms | 0.6× | 1.6% | PASS |
| mem_reads | `groupby_scan` | 35.90ms | 38.55ms | 1.1× | 1.0% | PASS |
| mem_reads | `index_join` | 7.92ms | 10.75ms | 1.4× | 1.0% | PASS |
| mem_reads | `index_join_scan` | 3.89ms | 5.71ms | 1.5× | 1.8% | PASS |
| mem_reads | `types_table_scan` | 1.07s | 1.18s | 1.1× | 0.8% | PASS |
| mem_reads | `table_scan` | 1.26s | 1.29s | 1.0× | 0.9% | PASS |
| mem_reads | `oltp_read_only` | 152.37ms | 173.19ms | 1.1× | 1.2% | PASS |
| mem_writes | `oltp_bulk_insert` | 248.96ms | 355.63ms | 1.4× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 19.07ms | 35.46ms | 1.9× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 66.68ms | 128.25ms | 1.9× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 51.21ms | 86.34ms | 1.7× | 1.0% | PASS |
| mem_writes | `oltp_delete_insert` | 49.18ms | 96.73ms | 2.0× | 0.7% | PASS |
| mem_writes | `oltp_write_only` | 26.73ms | 59.12ms | 2.2× | 0.9% | PASS |
| mem_writes | `types_delete_insert` | 32.30ms | 54.28ms | 1.7× | 0.8% | PASS |
| mem_writes | `oltp_read_write` | 103.03ms | 166.40ms | 1.6× | 1.3% | PASS |
| file_reads | `oltp_point_select` | 103.71ms | 61.37ms | 0.6× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 27.45ms | 23.99ms | 0.9× | 1.8% | PASS |
| file_reads | `oltp_sum_range` | 25.64ms | 22.72ms | 0.9× | 1.2% | PASS |
| file_reads | `oltp_order_range` | 4.51ms | 4.25ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_distinct_range` | 5.67ms | 5.47ms | 1.0× | 2.2% | PASS |
| file_reads | `oltp_index_scan` | 11.95ms | 8.54ms | 0.7× | 1.8% | PASS |
| file_reads | `select_random_points` | 37.07ms | 36.52ms | 1.0× | 1.1% | PASS |
| file_reads | `select_random_ranges` | 15.28ms | 11.42ms | 0.7× | 1.3% | PASS |
| file_reads | `covering_index_scan` | 15.47ms | 6.68ms | 0.4× | 1.0% | PASS |
| file_reads | `groupby_scan` | 36.98ms | 39.15ms | 1.1× | 0.7% | PASS |
| file_reads | `index_join` | 12.17ms | 12.61ms | 1.0× | 1.6% | PASS |
| file_reads | `index_join_scan` | 4.85ms | 6.06ms | 1.3× | 1.2% | PASS |
| file_reads | `types_table_scan` | 1.07s | 1.18s | 1.1× | 0.4% | PASS |
| file_reads | `table_scan` | 1.26s | 1.28s | 1.0× | 0.9% | PASS |
| file_reads | `oltp_read_only` | 259.00ms | 203.88ms | 0.8× | 0.7% | PASS |
| file_writes | `oltp_bulk_insert` | 265.56ms | 365.26ms | 1.4× | 0.9% | PASS |
| file_writes | `oltp_insert` | 28.84ms | 41.01ms | 1.4× | 1.8% | PASS |
| file_writes | `oltp_update_index` | 96.98ms | 141.86ms | 1.5× | 1.2% | PASS |
| file_writes | `oltp_update_non_index` | 80.15ms | 98.87ms | 1.2× | 1.0% | PASS |
| file_writes | `oltp_delete_insert` | 80.27ms | 108.67ms | 1.4× | 1.0% | PASS |
| file_writes | `oltp_write_only` | 56.18ms | 69.33ms | 1.2× | 1.8% | PASS |
| file_writes | `types_delete_insert` | 50.46ms | 60.57ms | 1.2× | 0.9% | PASS |
| file_writes | `oltp_read_write` | 134.54ms | 176.54ms | 1.3× | 1.4% | PASS |
| ac_reads | `oltp_point_select` | 56.97ms | 61.57ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_range_select` | 22.79ms | 23.99ms | 1.1× | 1.0% | PASS |
| ac_reads | `oltp_sum_range` | 20.72ms | 22.74ms | 1.1× | 0.9% | PASS |
| ac_reads | `oltp_order_range` | 3.97ms | 4.25ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_distinct_range` | 5.15ms | 5.44ms | 1.1× | 1.9% | PASS |
| ac_reads | `oltp_index_scan` | 7.22ms | 8.57ms | 1.2× | 1.3% | PASS |
| ac_reads | `select_random_points` | 31.32ms | 36.64ms | 1.2× | 1.2% | PASS |
| ac_reads | `select_random_ranges` | 10.40ms | 11.37ms | 1.1× | 0.8% | PASS |
| ac_reads | `covering_index_scan` | 10.52ms | 6.64ms | 0.6× | 1.0% | PASS |
| ac_reads | `groupby_scan` | 36.35ms | 39.20ms | 1.1× | 0.9% | PASS |
| ac_reads | `index_join` | 9.65ms | 12.61ms | 1.3× | 1.3% | PASS |
| ac_reads | `index_join_scan` | 4.39ms | 6.12ms | 1.4× | 1.3% | PASS |
| ac_reads | `types_table_scan` | 1.07s | 1.18s | 1.1× | 0.5% | PASS |
| ac_reads | `table_scan` | 1.26s | 1.28s | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_read_only` | 188.13ms | 204.37ms | 1.1× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 29.78ms | 102.59ms | 3.4× | 5.4% | PASS |
| ac_writes | `oltp_insert_ac` | 31.95ms | 119.79ms | 3.7× | 6.1% | PASS |
| ac_writes | `oltp_update_index_ac` | 35.20ms | 127.67ms | 3.6× | 7.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 30.36ms | 111.95ms | 3.7× | 6.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 32.46ms | 120.33ms | 3.7× | 7.0% | PASS |
| ac_writes | `oltp_write_only_ac` | 32.97ms | 120.23ms | 3.6× | 8.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 29.94ms | 109.57ms | 3.7× | 8.8% | PASS |
| ac_writes | `oltp_read_write_ac` | 40.19ms | 128.40ms | 3.2× | 6.8% | PASS |

</details>

</details>

## Version-control latency

Wall time: 2m 26s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 87.26ms | 130.00ms | 67.1% | 0.5% | PASS |
| `status_dirty_many_tables` | 91.03ms | 130.00ms | 70.0% | 0.6% | PASS |
| `diff_regular_working_one_table` | 83.20ms | 120.00ms | 69.3% | 0.4% | PASS |
| `diff_regular_working_many_tables` | 95.50ms | 140.00ms | 68.2% | 0.5% | PASS |
| `diff_stat_working_many_tables` | 95.26ms | 140.00ms | 68.0% | 0.4% | PASS |
| `diff_schema_working_many_tables` | 95.88ms | 140.00ms | 68.5% | 0.4% | PASS |
| `branch_list_many_branches` | 22.11ms | 35.00ms | 63.2% | 1.6% | PASS |
| `branch_create_delete` | 24.30ms | 40.00ms | 60.7% | 1.1% | PASS |
| `at_literal_deep_history` | 35.53ms | 100.00ms | 35.5% | 1.3% | PASS |
| `diff_literal_deep_history` | 35.56ms | 120.00ms | 29.6% | 1.4% | PASS |
| `history_literal_deep_history` | 37.21ms | 150.00ms | 24.8% | 1.7% | PASS |
| `checkout_branch_clean` | 55.43ms | 150.00ms | 37.0% | 0.8% | PASS |
| `merge_data_no_conflicts` | 28.41ms | 50.00ms | 56.8% | 1.1% | PASS |
| `merge_schema_no_conflicts` | 21.52ms | 35.00ms | 61.5% | 1.3% | PASS |
| `merge_data_conflicts` | 30.83ms | 180.00ms | 17.1% | 0.6% | PASS |
| `merge_data_conflicts_with_resolve` | 31.11ms | 180.00ms | 17.3% | 1.3% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
