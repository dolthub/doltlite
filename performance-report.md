# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-08-21 11:16 UTC
>
> Commit: [`dba2a968f19cda67c2a64151b31778557d29e6fa`](https://github.com/dolthub/doltlite/commit/dba2a968f19cda67c2a64151b31778557d29e6fa)
>
> Runner: ubuntu24 20260816.277.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/32469256508)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 8.55s | 8.74s | 1.0× | 2.3% | **PASS** |
| Writes | 1.69s | 2.62s | 1.5× | 1.8% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.59s | 9.03s | 0.9× | 2.2% | **PASS** |
| Writes | 5.41s | 4.31s | 0.8× | 19.5% | **PASS** |
| Autocommit writes | 4.17s | 12.55s | 3.0× | 50.3% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.08s | 2.11s | 1.0× | 1.6% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 1.86s | 1.87s | 1.0× | 2.2% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 1.78s | 1.77s | 1.0× | 3.3% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.83s | 3.00s | 1.1× | 2.5% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 355.30ms | 506.85ms | 1.4× | 1.3% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 363.55ms | 586.27ms | 1.6× | 1.6% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 357.19ms | 570.90ms | 1.6× | 3.6% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 616.34ms | 958.68ms | 1.6× | 2.6% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.44s | 2.20s | 0.9× | 1.1% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 1.93s | 1.89s | 1.0× | 2.3% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 1.96s | 1.82s | 0.9× | 2.8% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 3.27s | 3.12s | 1.0× | 2.3% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 942.14ms | 860.52ms | 0.9× | 12.9% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 2.16s | 1.31s | 0.6× | 35.9% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 1.51s | 1.10s | 0.7× | 36.4% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 803.26ms | 1.05s | 1.3× | 3.0% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.09s | 2.13s | 1.0× | 0.7% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 1.81s | 1.88s | 1.0× | 2.1% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 1.86s | 1.83s | 1.0× | 2.6% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.96s | 3.09s | 1.0× | 2.6% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 272.57ms | 941.12ms | 3.5× | 40.7% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 1.64s | 4.93s | 3.0× | 52.9% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 2.04s | 5.87s | 2.9× | 56.7% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 221.47ms | 800.12ms | 3.6× | 7.9% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 19.96ms | 21.16ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_range_select` | 8.88ms | 8.60ms | 1.0× | 2.3% | PASS |
| mem_reads | `oltp_sum_range` | 8.18ms | 8.27ms | 1.0× | 1.7% | PASS |
| mem_reads | `oltp_order_range` | 2.19ms | 2.22ms | 1.0× | 1.3% | PASS |
| mem_reads | `oltp_distinct_range` | 3.01ms | 3.00ms | 1.0× | 0.8% | PASS |
| mem_reads | `oltp_index_scan` | 3.21ms | 3.79ms | 1.2× | 2.2% | PASS |
| mem_reads | `select_random_points` | 8.65ms | 8.77ms | 1.0× | 1.5% | PASS |
| mem_reads | `select_random_ranges` | 2.56ms | 3.10ms | 1.2× | 1.5% | PASS |
| mem_reads | `covering_index_scan` | 3.35ms | 3.15ms | 0.9× | 1.4% | PASS |
| mem_reads | `groupby_scan` | 25.02ms | 26.04ms | 1.0× | 0.8% | PASS |
| mem_reads | `index_join` | 4.73ms | 6.14ms | 1.3× | 2.5% | PASS |
| mem_reads | `index_join_scan` | 2.86ms | 3.78ms | 1.3× | 2.3% | PASS |
| mem_reads | `types_table_scan` | 908.07ms | 921.56ms | 1.0× | 1.8% | PASS |
| mem_reads | `table_scan` | 994.44ms | 1.00s | 1.0× | 1.9% | PASS |
| mem_reads | `oltp_read_only` | 82.23ms | 85.48ms | 1.0× | 1.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 139.58ms | 181.66ms | 1.3× | 1.3% | PASS |
| mem_writes | `oltp_insert` | 12.45ms | 20.25ms | 1.6× | 1.1% | PASS |
| mem_writes | `oltp_update_index` | 40.37ms | 66.69ms | 1.7× | 1.3% | PASS |
| mem_writes | `oltp_update_non_index` | 27.95ms | 38.12ms | 1.4× | 1.3% | PASS |
| mem_writes | `oltp_delete_insert` | 36.64ms | 53.90ms | 1.5× | 1.2% | PASS |
| mem_writes | `oltp_write_only` | 17.87ms | 34.35ms | 1.9× | 1.6% | PASS |
| mem_writes | `types_delete_insert` | 20.97ms | 26.94ms | 1.3× | 1.3% | PASS |
| mem_writes | `oltp_read_write` | 59.48ms | 84.95ms | 1.4× | 1.7% | PASS |
| file_reads | `oltp_point_select` | 86.11ms | 38.66ms | 0.4× | 0.6% | PASS |
| file_reads | `oltp_range_select` | 16.23ms | 10.40ms | 0.6× | 1.7% | PASS |
| file_reads | `oltp_sum_range` | 15.15ms | 10.09ms | 0.7× | 1.1% | PASS |
| file_reads | `oltp_order_range` | 2.95ms | 2.42ms | 0.8× | 1.3% | PASS |
| file_reads | `oltp_distinct_range` | 3.79ms | 3.21ms | 0.8× | 0.7% | PASS |
| file_reads | `oltp_index_scan` | 10.15ms | 5.95ms | 0.6× | 1.0% | PASS |
| file_reads | `select_random_points` | 16.41ms | 10.87ms | 0.7× | 2.3% | PASS |
| file_reads | `select_random_ranges` | 9.09ms | 4.81ms | 0.5× | 1.4% | PASS |
| file_reads | `covering_index_scan` | 10.27ms | 5.23ms | 0.5× | 1.1% | PASS |
| file_reads | `groupby_scan` | 26.04ms | 26.39ms | 1.0× | 0.8% | PASS |
| file_reads | `index_join` | 8.54ms | 7.62ms | 0.9× | 1.1% | PASS |
| file_reads | `index_join_scan` | 3.64ms | 4.07ms | 1.1× | 1.7% | PASS |
| file_reads | `types_table_scan` | 915.83ms | 921.72ms | 1.0× | 1.1% | PASS |
| file_reads | `table_scan` | 1.13s | 1.04s | 0.9× | 0.8% | PASS |
| file_reads | `oltp_read_only` | 184.27ms | 112.82ms | 0.6× | 0.7% | PASS |
| file_writes | `oltp_bulk_insert` | 204.51ms | 240.90ms | 1.2× | 10.9% | PASS |
| file_writes | `oltp_insert` | 29.81ms | 35.61ms | 1.2× | 11.5% | PASS |
| file_writes | `oltp_update_index` | 141.19ms | 127.18ms | 0.9× | 6.3% | PASS |
| file_writes | `oltp_update_non_index` | 136.61ms | 89.91ms | 0.7× | 24.0% | PASS |
| file_writes | `oltp_delete_insert` | 121.69ms | 103.84ms | 0.9× | 18.1% | PASS |
| file_writes | `oltp_write_only` | 90.62ms | 73.32ms | 0.8× | 7.2% | PASS |
| file_writes | `types_delete_insert` | 80.37ms | 56.17ms | 0.7× | 14.3% | PASS |
| file_writes | `oltp_read_write` | 137.34ms | 133.58ms | 1.0× | 14.5% | PASS |
| ac_reads | `oltp_point_select` | 41.21ms | 37.74ms | 0.9× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 11.22ms | 10.26ms | 0.9× | 1.2% | PASS |
| ac_reads | `oltp_sum_range` | 10.54ms | 10.01ms | 0.9× | 0.7% | PASS |
| ac_reads | `oltp_order_range` | 2.52ms | 2.42ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_distinct_range` | 3.34ms | 3.22ms | 1.0× | 0.6% | PASS |
| ac_reads | `oltp_index_scan` | 5.75ms | 5.84ms | 1.0× | 0.7% | PASS |
| ac_reads | `select_random_points` | 11.38ms | 10.58ms | 0.9× | 0.9% | PASS |
| ac_reads | `select_random_ranges` | 4.87ms | 4.81ms | 1.0× | 0.8% | PASS |
| ac_reads | `covering_index_scan` | 5.98ms | 5.24ms | 0.9× | 0.7% | PASS |
| ac_reads | `groupby_scan` | 25.55ms | 26.41ms | 1.0× | 0.6% | PASS |
| ac_reads | `index_join` | 6.40ms | 7.61ms | 1.2× | 1.5% | PASS |
| ac_reads | `index_join_scan` | 3.31ms | 4.17ms | 1.3× | 2.1% | PASS |
| ac_reads | `types_table_scan` | 864.53ms | 901.86ms | 1.0× | 0.6% | PASS |
| ac_reads | `table_scan` | 985.25ms | 994.70ms | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_read_only` | 112.07ms | 108.82ms | 1.0× | 0.7% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 30.83ms | 90.48ms | 2.9× | 28.2% | PASS |
| ac_writes | `oltp_insert_ac` | 36.49ms | 143.30ms | 3.9× | 52.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 35.96ms | 131.73ms | 3.7× | 49.3% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 29.85ms | 125.24ms | 4.2× | 39.9% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 35.36ms | 147.44ms | 4.2× | 41.5% | PASS |
| ac_writes | `oltp_write_only_ac` | 33.70ms | 94.69ms | 2.8× | 19.9% | PASS |
| ac_writes | `types_delete_insert_ac` | 32.05ms | 111.89ms | 3.5× | 54.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 38.33ms | 96.34ms | 2.5× | 25.3% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 20.27ms | 23.65ms | 1.2× | 2.2% | PASS |
| mem_reads | `oltp_range_select` | 10.70ms | 9.51ms | 0.9× | 2.6% | PASS |
| mem_reads | `oltp_sum_range` | 10.58ms | 9.28ms | 0.9× | 1.6% | PASS |
| mem_reads | `oltp_order_range` | 2.23ms | 2.13ms | 1.0× | 1.5% | PASS |
| mem_reads | `oltp_distinct_range` | 2.83ms | 2.82ms | 1.0× | 1.6% | PASS |
| mem_reads | `oltp_index_scan` | 3.19ms | 3.82ms | 1.2× | 2.8% | PASS |
| mem_reads | `select_random_points` | 13.65ms | 15.18ms | 1.1× | 2.5% | PASS |
| mem_reads | `select_random_ranges` | 2.83ms | 3.28ms | 1.2× | 3.3% | PASS |
| mem_reads | `covering_index_scan` | 2.89ms | 2.58ms | 0.9× | 1.7% | PASS |
| mem_reads | `groupby_scan` | 20.94ms | 21.75ms | 1.0× | 0.6% | PASS |
| mem_reads | `index_join` | 5.30ms | 6.29ms | 1.2× | 1.2% | PASS |
| mem_reads | `index_join_scan` | 3.65ms | 4.60ms | 1.3× | 5.0% | PASS |
| mem_reads | `types_table_scan` | 767.38ms | 797.42ms | 1.0× | 2.2% | PASS |
| mem_reads | `table_scan` | 918.62ms | 879.25ms | 1.0× | 4.1% | PASS |
| mem_reads | `oltp_read_only` | 79.19ms | 85.00ms | 1.1× | 1.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 132.53ms | 194.45ms | 1.5× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 14.40ms | 22.42ms | 1.6× | 1.8% | PASS |
| mem_writes | `oltp_update_index` | 49.23ms | 88.65ms | 1.8× | 1.3% | PASS |
| mem_writes | `oltp_update_non_index` | 31.87ms | 49.65ms | 1.6× | 1.4% | PASS |
| mem_writes | `oltp_delete_insert` | 35.79ms | 67.60ms | 1.9× | 0.8% | PASS |
| mem_writes | `oltp_write_only` | 20.80ms | 39.71ms | 1.9× | 2.0% | PASS |
| mem_writes | `types_delete_insert` | 21.79ms | 33.31ms | 1.5× | 2.6% | PASS |
| mem_writes | `oltp_read_write` | 57.16ms | 90.49ms | 1.6× | 2.9% | PASS |
| file_reads | `oltp_point_select` | 40.42ms | 28.02ms | 0.7× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 13.08ms | 10.08ms | 0.8× | 2.0% | PASS |
| file_reads | `oltp_sum_range` | 13.01ms | 9.88ms | 0.8× | 2.3% | PASS |
| file_reads | `oltp_order_range` | 2.56ms | 2.29ms | 0.9× | 3.1% | PASS |
| file_reads | `oltp_distinct_range` | 3.14ms | 2.95ms | 0.9× | 2.4% | PASS |
| file_reads | `oltp_index_scan` | 5.96ms | 5.17ms | 0.9× | 2.7% | PASS |
| file_reads | `select_random_points` | 17.37ms | 17.53ms | 1.0× | 1.4% | PASS |
| file_reads | `select_random_ranges` | 5.04ms | 4.03ms | 0.8× | 1.7% | PASS |
| file_reads | `covering_index_scan` | 5.81ms | 3.64ms | 0.6× | 5.5% | PASS |
| file_reads | `groupby_scan` | 21.70ms | 22.35ms | 1.0× | 0.7% | PASS |
| file_reads | `index_join` | 7.53ms | 7.86ms | 1.0× | 5.1% | PASS |
| file_reads | `index_join_scan` | 4.39ms | 4.90ms | 1.1× | 4.0% | PASS |
| file_reads | `types_table_scan` | 769.58ms | 797.59ms | 1.0× | 2.3% | PASS |
| file_reads | `table_scan` | 909.30ms | 884.50ms | 1.0× | 4.4% | PASS |
| file_reads | `oltp_read_only` | 110.47ms | 92.41ms | 0.8× | 1.2% | PASS |
| file_writes | `oltp_bulk_insert` | 228.52ms | 275.21ms | 1.2× | 21.3% | PASS |
| file_writes | `oltp_insert` | 206.33ms | 62.82ms | 0.3× | 64.3% | PASS |
| file_writes | `oltp_update_index` | 278.54ms | 189.16ms | 0.7× | 23.7% | PASS |
| file_writes | `oltp_update_non_index` | 269.75ms | 138.63ms | 0.5× | 37.8% | PASS |
| file_writes | `oltp_delete_insert` | 351.19ms | 173.20ms | 0.5× | 34.0% | PASS |
| file_writes | `oltp_write_only` | 298.38ms | 150.82ms | 0.5× | 40.1% | PASS |
| file_writes | `types_delete_insert` | 198.81ms | 129.47ms | 0.7× | 29.1% | PASS |
| file_writes | `oltp_read_write` | 329.68ms | 189.20ms | 0.6× | 39.3% | PASS |
| ac_reads | `oltp_point_select` | 26.55ms | 28.55ms | 1.1× | 2.2% | PASS |
| ac_reads | `oltp_range_select` | 11.85ms | 10.17ms | 0.9× | 1.5% | PASS |
| ac_reads | `oltp_sum_range` | 11.82ms | 10.03ms | 0.8× | 2.8% | PASS |
| ac_reads | `oltp_order_range` | 2.48ms | 2.30ms | 0.9× | 2.4% | PASS |
| ac_reads | `oltp_distinct_range` | 3.05ms | 2.95ms | 1.0× | 2.1% | PASS |
| ac_reads | `oltp_index_scan` | 4.65ms | 5.06ms | 1.1× | 2.2% | PASS |
| ac_reads | `select_random_points` | 15.82ms | 17.19ms | 1.1× | 1.7% | PASS |
| ac_reads | `select_random_ranges` | 3.96ms | 4.13ms | 1.0× | 2.1% | PASS |
| ac_reads | `covering_index_scan` | 4.98ms | 3.79ms | 0.8× | 2.5% | PASS |
| ac_reads | `groupby_scan` | 21.48ms | 22.25ms | 1.0× | 1.0% | PASS |
| ac_reads | `index_join` | 7.13ms | 7.91ms | 1.1× | 2.3% | PASS |
| ac_reads | `index_join_scan` | 4.27ms | 4.86ms | 1.1× | 3.8% | PASS |
| ac_reads | `types_table_scan` | 746.70ms | 794.86ms | 1.1× | 1.0% | PASS |
| ac_reads | `table_scan` | 854.63ms | 878.74ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_read_only` | 88.74ms | 91.33ms | 1.0× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 187.57ms | 551.54ms | 2.9× | 51.2% | PASS |
| ac_writes | `oltp_insert_ac` | 192.83ms | 746.67ms | 3.9× | 44.6% | PASS |
| ac_writes | `oltp_update_index_ac` | 255.32ms | 718.38ms | 2.8× | 54.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 196.73ms | 533.97ms | 2.7× | 52.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 208.88ms | 745.31ms | 3.6× | 60.2% | PASS |
| ac_writes | `oltp_write_only_ac` | 266.23ms | 849.53ms | 3.2× | 53.5% | PASS |
| ac_writes | `types_delete_insert_ac` | 141.42ms | 301.43ms | 2.1× | 49.2% | PASS |
| ac_writes | `oltp_read_write_ac` | 189.36ms | 485.33ms | 2.6× | 58.6% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 21.63ms | 23.77ms | 1.1× | 2.7% | PASS |
| mem_reads | `oltp_range_select` | 9.91ms | 8.92ms | 0.9× | 3.3% | PASS |
| mem_reads | `oltp_sum_range` | 9.88ms | 8.91ms | 0.9× | 3.1% | PASS |
| mem_reads | `oltp_order_range` | 2.09ms | 1.98ms | 0.9× | 2.7% | PASS |
| mem_reads | `oltp_distinct_range` | 2.56ms | 2.39ms | 0.9× | 2.1% | PASS |
| mem_reads | `oltp_index_scan` | 3.06ms | 4.22ms | 1.4× | 5.1% | PASS |
| mem_reads | `select_random_points` | 13.04ms | 14.25ms | 1.1× | 3.5% | PASS |
| mem_reads | `select_random_ranges` | 2.92ms | 3.39ms | 1.2× | 4.3% | PASS |
| mem_reads | `covering_index_scan` | 2.43ms | 2.63ms | 1.1× | 4.6% | PASS |
| mem_reads | `groupby_scan` | 18.25ms | 18.53ms | 1.0× | 2.2% | PASS |
| mem_reads | `index_join` | 4.49ms | 5.90ms | 1.3× | 5.0% | PASS |
| mem_reads | `index_join_scan` | 3.56ms | 4.74ms | 1.3× | 6.1% | PASS |
| mem_reads | `types_table_scan` | 719.96ms | 743.30ms | 1.0× | 2.0% | PASS |
| mem_reads | `table_scan` | 901.96ms | 856.37ms | 0.9× | 4.8% | PASS |
| mem_reads | `oltp_read_only` | 66.36ms | 68.70ms | 1.0× | 3.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 136.65ms | 187.35ms | 1.4× | 1.3% | PASS |
| mem_writes | `oltp_insert` | 12.01ms | 22.16ms | 1.8× | 2.8% | PASS |
| mem_writes | `oltp_update_index` | 43.77ms | 83.21ms | 1.9× | 3.4% | PASS |
| mem_writes | `oltp_update_non_index` | 32.44ms | 47.24ms | 1.5× | 2.9% | PASS |
| mem_writes | `oltp_delete_insert` | 33.80ms | 68.23ms | 2.0× | 5.4% | PASS |
| mem_writes | `oltp_write_only` | 19.29ms | 40.33ms | 2.1× | 5.1% | PASS |
| mem_writes | `types_delete_insert` | 22.56ms | 34.13ms | 1.5× | 3.7% | PASS |
| mem_writes | `oltp_read_write` | 56.68ms | 88.26ms | 1.6× | 7.1% | PASS |
| file_reads | `oltp_point_select` | 76.59ms | 37.66ms | 0.5× | 2.7% | PASS |
| file_reads | `oltp_range_select` | 15.01ms | 10.12ms | 0.7× | 2.4% | PASS |
| file_reads | `oltp_sum_range` | 15.36ms | 10.19ms | 0.7× | 3.8% | PASS |
| file_reads | `oltp_order_range` | 2.82ms | 2.19ms | 0.8× | 2.5% | PASS |
| file_reads | `oltp_distinct_range` | 3.38ms | 2.72ms | 0.8× | 3.6% | PASS |
| file_reads | `oltp_index_scan` | 9.22ms | 5.80ms | 0.6× | 2.0% | PASS |
| file_reads | `select_random_points` | 20.50ms | 16.09ms | 0.8× | 4.5% | PASS |
| file_reads | `select_random_ranges` | 8.99ms | 5.16ms | 0.6× | 2.6% | PASS |
| file_reads | `covering_index_scan` | 9.52ms | 4.79ms | 0.5× | 3.2% | PASS |
| file_reads | `groupby_scan` | 21.53ms | 19.84ms | 0.9× | 3.2% | PASS |
| file_reads | `index_join` | 8.39ms | 7.67ms | 0.9× | 2.8% | PASS |
| file_reads | `index_join_scan` | 3.98ms | 4.45ms | 1.1× | 1.7% | PASS |
| file_reads | `types_table_scan` | 742.93ms | 754.58ms | 1.0× | 4.4% | PASS |
| file_reads | `table_scan` | 862.13ms | 839.85ms | 1.0× | 4.5% | PASS |
| file_reads | `oltp_read_only` | 157.45ms | 96.42ms | 0.6× | 2.0% | PASS |
| file_writes | `oltp_bulk_insert` | 264.91ms | 268.92ms | 1.0× | 20.8% | PASS |
| file_writes | `oltp_insert` | 74.63ms | 49.62ms | 0.7× | 57.7% | PASS |
| file_writes | `oltp_update_index` | 218.00ms | 179.87ms | 0.8× | 18.3% | PASS |
| file_writes | `oltp_update_non_index` | 250.63ms | 121.63ms | 0.5× | 39.3% | PASS |
| file_writes | `oltp_delete_insert` | 208.74ms | 139.15ms | 0.7× | 23.7% | PASS |
| file_writes | `oltp_write_only` | 138.57ms | 100.94ms | 0.7× | 39.5% | PASS |
| file_writes | `types_delete_insert` | 114.04ms | 79.39ms | 0.7× | 35.5% | PASS |
| file_writes | `oltp_read_write` | 235.72ms | 158.09ms | 0.7× | 37.3% | PASS |
| ac_reads | `oltp_point_select` | 39.52ms | 37.56ms | 1.0× | 2.1% | PASS |
| ac_reads | `oltp_range_select` | 11.78ms | 10.21ms | 0.9× | 2.8% | PASS |
| ac_reads | `oltp_sum_range` | 12.27ms | 10.57ms | 0.9× | 4.2% | PASS |
| ac_reads | `oltp_order_range` | 2.52ms | 2.21ms | 0.9× | 2.6% | PASS |
| ac_reads | `oltp_distinct_range` | 3.04ms | 2.68ms | 0.9× | 2.4% | PASS |
| ac_reads | `oltp_index_scan` | 5.76ms | 5.92ms | 1.0× | 2.9% | PASS |
| ac_reads | `select_random_points` | 16.70ms | 16.64ms | 1.0× | 4.4% | PASS |
| ac_reads | `select_random_ranges` | 5.31ms | 5.16ms | 1.0× | 2.0% | PASS |
| ac_reads | `covering_index_scan` | 5.83ms | 4.82ms | 0.8× | 1.6% | PASS |
| ac_reads | `groupby_scan` | 21.22ms | 19.89ms | 0.9× | 2.1% | PASS |
| ac_reads | `index_join` | 6.68ms | 7.67ms | 1.1× | 2.1% | PASS |
| ac_reads | `index_join_scan` | 3.75ms | 4.58ms | 1.2× | 2.3% | PASS |
| ac_reads | `types_table_scan` | 715.56ms | 744.52ms | 1.0× | 3.2% | PASS |
| ac_reads | `table_scan` | 895.87ms | 859.29ms | 1.0× | 5.8% | PASS |
| ac_reads | `oltp_read_only` | 112.36ms | 101.70ms | 0.9× | 3.4% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 223.25ms | 437.34ms | 2.0× | 65.3% | PASS |
| ac_writes | `oltp_insert_ac` | 233.19ms | 660.89ms | 2.8× | 56.7% | PASS |
| ac_writes | `oltp_update_index_ac` | 240.06ms | 958.04ms | 4.0× | 54.8% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 246.62ms | 723.58ms | 2.9× | 52.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 281.59ms | 811.62ms | 2.9× | 56.7% | PASS |
| ac_writes | `oltp_write_only_ac` | 263.42ms | 793.20ms | 3.0× | 74.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 263.20ms | 732.45ms | 2.8× | 69.1% | PASS |
| ac_writes | `oltp_read_write_ac` | 286.84ms | 755.55ms | 2.6× | 51.5% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 36.40ms | 40.71ms | 1.1× | 3.3% | PASS |
| mem_reads | `oltp_range_select` | 19.23ms | 21.46ms | 1.1× | 3.0% | PASS |
| mem_reads | `oltp_sum_range` | 17.79ms | 20.55ms | 1.2× | 1.2% | PASS |
| mem_reads | `oltp_order_range` | 3.52ms | 3.81ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_distinct_range` | 4.80ms | 5.04ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_index_scan` | 4.86ms | 6.31ms | 1.3× | 2.6% | PASS |
| mem_reads | `select_random_points` | 30.14ms | 32.86ms | 1.1× | 2.5% | PASS |
| mem_reads | `select_random_ranges` | 8.14ms | 9.01ms | 1.1× | 1.7% | PASS |
| mem_reads | `covering_index_scan` | 4.22ms | 4.19ms | 1.0× | 2.4% | PASS |
| mem_reads | `groupby_scan` | 37.56ms | 39.79ms | 1.1× | 0.9% | PASS |
| mem_reads | `index_join` | 8.69ms | 11.05ms | 1.3× | 1.8% | PASS |
| mem_reads | `index_join_scan` | 4.21ms | 5.38ms | 1.3× | 2.8% | PASS |
| mem_reads | `types_table_scan` | 1.12s | 1.28s | 1.1× | 4.2% | PASS |
| mem_reads | `table_scan` | 1.37s | 1.35s | 1.0× | 7.4% | PASS |
| mem_reads | `oltp_read_only` | 163.02ms | 174.23ms | 1.1× | 2.9% | PASS |
| mem_writes | `oltp_bulk_insert` | 244.68ms | 345.57ms | 1.4× | 1.5% | PASS |
| mem_writes | `oltp_insert` | 19.16ms | 34.54ms | 1.8× | 1.2% | PASS |
| mem_writes | `oltp_update_index` | 73.22ms | 127.47ms | 1.7× | 2.3% | PASS |
| mem_writes | `oltp_update_non_index` | 54.30ms | 79.10ms | 1.5× | 3.0% | PASS |
| mem_writes | `oltp_delete_insert` | 51.33ms | 94.52ms | 1.8× | 1.7% | PASS |
| mem_writes | `oltp_write_only` | 29.99ms | 60.27ms | 2.0× | 3.1% | PASS |
| mem_writes | `types_delete_insert` | 32.78ms | 52.34ms | 1.6× | 2.9% | PASS |
| mem_writes | `oltp_read_write` | 110.88ms | 164.86ms | 1.5× | 3.3% | PASS |
| file_reads | `oltp_point_select` | 107.22ms | 60.00ms | 0.6× | 2.2% | PASS |
| file_reads | `oltp_range_select` | 28.93ms | 24.48ms | 0.8× | 2.7% | PASS |
| file_reads | `oltp_sum_range` | 25.54ms | 23.02ms | 0.9× | 2.0% | PASS |
| file_reads | `oltp_order_range` | 4.73ms | 4.24ms | 0.9× | 3.0% | PASS |
| file_reads | `oltp_distinct_range` | 5.75ms | 5.43ms | 0.9× | 1.6% | PASS |
| file_reads | `oltp_index_scan` | 12.20ms | 8.43ms | 0.7× | 2.5% | PASS |
| file_reads | `select_random_points` | 37.43ms | 35.84ms | 1.0× | 2.8% | PASS |
| file_reads | `select_random_ranges` | 15.71ms | 11.27ms | 0.7× | 2.3% | PASS |
| file_reads | `covering_index_scan` | 11.78ms | 6.69ms | 0.6× | 3.8% | PASS |
| file_reads | `groupby_scan` | 38.15ms | 40.49ms | 1.1× | 1.1% | PASS |
| file_reads | `index_join` | 12.78ms | 12.47ms | 1.0× | 3.5% | PASS |
| file_reads | `index_join_scan` | 5.32ms | 5.97ms | 1.1× | 3.5% | PASS |
| file_reads | `types_table_scan` | 1.20s | 1.29s | 1.1× | 1.8% | PASS |
| file_reads | `table_scan` | 1.51s | 1.39s | 0.9× | 2.0% | PASS |
| file_reads | `oltp_read_only` | 254.34ms | 196.81ms | 0.8× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 259.80ms | 354.38ms | 1.4× | 1.9% | PASS |
| file_writes | `oltp_insert` | 26.35ms | 40.96ms | 1.6× | 2.4% | PASS |
| file_writes | `oltp_update_index` | 104.52ms | 139.15ms | 1.3× | 2.3% | PASS |
| file_writes | `oltp_update_non_index` | 78.26ms | 90.86ms | 1.2× | 3.2% | PASS |
| file_writes | `oltp_delete_insert` | 83.45ms | 114.77ms | 1.4× | 3.0% | PASS |
| file_writes | `oltp_write_only` | 54.64ms | 68.44ms | 1.3× | 3.0% | PASS |
| file_writes | `types_delete_insert` | 53.00ms | 61.40ms | 1.2× | 3.1% | PASS |
| file_writes | `oltp_read_write` | 143.24ms | 177.74ms | 1.2× | 3.4% | PASS |
| ac_reads | `oltp_point_select` | 57.58ms | 58.20ms | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_range_select` | 24.27ms | 24.38ms | 1.0× | 3.7% | PASS |
| ac_reads | `oltp_sum_range` | 21.38ms | 23.35ms | 1.1× | 2.6% | PASS |
| ac_reads | `oltp_order_range` | 4.21ms | 4.37ms | 1.0× | 2.9% | PASS |
| ac_reads | `oltp_distinct_range` | 5.43ms | 5.60ms | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_index_scan` | 7.87ms | 8.60ms | 1.1× | 2.6% | PASS |
| ac_reads | `select_random_points` | 34.51ms | 38.50ms | 1.1× | 2.5% | PASS |
| ac_reads | `select_random_ranges` | 10.38ms | 11.15ms | 1.1× | 1.7% | PASS |
| ac_reads | `covering_index_scan` | 7.63ms | 6.87ms | 0.9× | 4.6% | PASS |
| ac_reads | `groupby_scan` | 37.55ms | 40.35ms | 1.1× | 0.5% | PASS |
| ac_reads | `index_join` | 10.74ms | 12.71ms | 1.2× | 4.0% | PASS |
| ac_reads | `index_join_scan` | 5.17ms | 6.28ms | 1.2× | 5.9% | PASS |
| ac_reads | `types_table_scan` | 1.20s | 1.29s | 1.1× | 1.8% | PASS |
| ac_reads | `table_scan` | 1.35s | 1.35s | 1.0× | 7.9% | PASS |
| ac_reads | `oltp_read_only` | 187.33ms | 198.78ms | 1.1× | 1.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 25.18ms | 82.47ms | 3.3× | 7.6% | PASS |
| ac_writes | `oltp_insert_ac` | 26.54ms | 98.90ms | 3.7× | 7.3% | PASS |
| ac_writes | `oltp_update_index_ac` | 29.83ms | 114.35ms | 3.8× | 7.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.62ms | 92.26ms | 3.7× | 7.4% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 27.04ms | 103.31ms | 3.8× | 9.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 29.30ms | 105.64ms | 3.6× | 10.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 24.79ms | 91.41ms | 3.7× | 9.1% | PASS |
| ac_writes | `oltp_read_write_ac` | 34.18ms | 111.78ms | 3.3× | 8.2% | PASS |

</details>

</details>

## Version-control latency

Wall time: 2m 17s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 80.48ms | 130.00ms | 61.9% | 0.6% | PASS |
| `status_dirty_many_tables` | 83.09ms | 130.00ms | 63.9% | 0.7% | PASS |
| `diff_regular_working_one_table` | 77.83ms | 120.00ms | 64.9% | 0.7% | PASS |
| `diff_regular_working_many_tables` | 88.03ms | 140.00ms | 62.9% | 0.7% | PASS |
| `diff_stat_working_many_tables` | 88.67ms | 140.00ms | 63.3% | 0.6% | PASS |
| `diff_schema_working_many_tables` | 89.35ms | 140.00ms | 63.8% | 0.5% | PASS |
| `branch_list_many_branches` | 22.47ms | 35.00ms | 64.2% | 1.5% | PASS |
| `branch_create_delete` | 25.02ms | 40.00ms | 62.6% | 1.5% | PASS |
| `checkout_branch_clean` | 54.53ms | 150.00ms | 36.4% | 0.9% | PASS |
| `merge_data_no_conflicts` | 29.13ms | 50.00ms | 58.3% | 1.0% | PASS |
| `merge_schema_no_conflicts` | 22.46ms | 35.00ms | 64.2% | 1.7% | PASS |
| `merge_data_conflicts` | 127.30ms | 180.00ms | 70.7% | 0.3% | PASS |
| `merge_data_conflicts_with_resolve` | 127.19ms | 180.00ms | 70.7% | 0.3% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
