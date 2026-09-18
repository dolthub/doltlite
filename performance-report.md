# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-09-18 11:09 UTC
>
> Commit: [`7a14cc7017026e54e8911229e6c014d3c611da5a`](https://github.com/dolthub/doltlite/commit/7a14cc7017026e54e8911229e6c014d3c611da5a)
>
> Runner: ubuntu24 20260907.300.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/35330348389)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.26s | 9.83s | 1.1× | 1.8% | **PASS** |
| Writes | 1.92s | 3.11s | 1.6× | 1.6% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.03s | 9.98s | 1.0× | 1.4% | **PASS** |
| Writes | 4.38s | 4.50s | 1.0× | 8.4% | **PASS** |
| Autocommit writes | 1.85s | 5.23s | 2.8× | 15.5% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.10s | 2.17s | 1.0× | 1.7% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 1.72s | 1.81s | 1.1× | 2.5% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.68s | 2.89s | 1.1× | 1.3% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.77s | 2.96s | 1.1× | 1.7% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 353.22ms | 538.73ms | 1.5× | 1.7% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 369.86ms | 597.53ms | 1.6× | 2.2% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 589.93ms | 985.37ms | 1.7× | 1.3% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 607.96ms | 985.34ms | 1.6× | 1.5% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.23s | 2.20s | 1.0× | 1.2% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 1.84s | 1.81s | 1.0× | 1.7% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.89s | 2.94s | 1.0× | 1.3% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 3.06s | 3.03s | 1.0× | 1.5% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 1.06s | 986.96ms | 0.9× | 24.8% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.71s | 1.33s | 0.8× | 36.0% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 831.92ms | 1.10s | 1.3× | 2.1% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 779.85ms | 1.08s | 1.4× | 1.6% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.11s | 2.22s | 1.1× | 1.1% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 1.88s | 1.90s | 1.0× | 2.5% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.89s | 3.00s | 1.0× | 1.4% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.87s | 3.02s | 1.1× | 1.4% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 308.01ms | 937.50ms | 3.0× | 45.1% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 1.12s | 2.76s | 2.5× | 60.0% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 211.83ms | 760.51ms | 3.6× | 6.6% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 212.52ms | 769.68ms | 3.6× | 7.2% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 19.95ms | 22.09ms | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_range_select` | 9.11ms | 8.81ms | 1.0× | 1.8% | PASS |
| mem_reads | `oltp_sum_range` | 7.69ms | 8.60ms | 1.1× | 1.9% | PASS |
| mem_reads | `oltp_order_range` | 2.21ms | 2.21ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 2.97ms | 3.08ms | 1.0× | 0.8% | PASS |
| mem_reads | `oltp_index_scan` | 3.19ms | 3.98ms | 1.2× | 2.2% | PASS |
| mem_reads | `select_random_points` | 9.14ms | 9.04ms | 1.0× | 2.0% | PASS |
| mem_reads | `select_random_ranges` | 3.79ms | 3.69ms | 1.0× | 1.1% | PASS |
| mem_reads | `covering_index_scan` | 5.89ms | 7.09ms | 1.2× | 1.7% | PASS |
| mem_reads | `groupby_scan` | 24.82ms | 27.21ms | 1.1× | 0.7% | PASS |
| mem_reads | `index_join` | 4.53ms | 6.14ms | 1.4× | 3.0% | PASS |
| mem_reads | `index_join_scan` | 2.67ms | 3.90ms | 1.5× | 1.7% | PASS |
| mem_reads | `types_table_scan` | 913.86ms | 942.55ms | 1.0× | 1.2% | PASS |
| mem_reads | `table_scan` | 1.01s | 1.04s | 1.0× | 2.3% | PASS |
| mem_reads | `oltp_read_only` | 79.59ms | 88.09ms | 1.1× | 1.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 138.72ms | 196.72ms | 1.4× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 12.02ms | 21.12ms | 1.8× | 1.2% | PASS |
| mem_writes | `oltp_update_index` | 41.63ms | 72.38ms | 1.7× | 2.6% | PASS |
| mem_writes | `oltp_update_non_index` | 29.67ms | 42.34ms | 1.4× | 2.9% | PASS |
| mem_writes | `oltp_delete_insert` | 37.61ms | 57.54ms | 1.5× | 1.6% | PASS |
| mem_writes | `oltp_write_only` | 17.96ms | 35.20ms | 2.0× | 1.0% | PASS |
| mem_writes | `types_delete_insert` | 19.89ms | 27.45ms | 1.4× | 1.7% | PASS |
| mem_writes | `oltp_read_write` | 55.73ms | 85.99ms | 1.5× | 3.1% | PASS |
| file_reads | `oltp_point_select` | 84.86ms | 38.82ms | 0.5× | 1.2% | PASS |
| file_reads | `oltp_range_select` | 15.81ms | 10.62ms | 0.7× | 1.5% | PASS |
| file_reads | `oltp_sum_range` | 14.54ms | 10.36ms | 0.7× | 1.3% | PASS |
| file_reads | `oltp_order_range` | 2.94ms | 2.48ms | 0.8× | 0.8% | PASS |
| file_reads | `oltp_distinct_range` | 3.70ms | 3.32ms | 0.9× | 1.1% | PASS |
| file_reads | `oltp_index_scan` | 9.95ms | 5.92ms | 0.6× | 1.0% | PASS |
| file_reads | `select_random_points` | 15.53ms | 10.71ms | 0.7× | 2.2% | PASS |
| file_reads | `select_random_ranges` | 10.32ms | 5.48ms | 0.5× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 12.76ms | 9.03ms | 0.7× | 1.0% | PASS |
| file_reads | `groupby_scan` | 25.21ms | 27.25ms | 1.1× | 0.6% | PASS |
| file_reads | `index_join` | 8.31ms | 7.51ms | 0.9× | 1.2% | PASS |
| file_reads | `index_join_scan` | 3.45ms | 4.19ms | 1.2× | 1.5% | PASS |
| file_reads | `types_table_scan` | 860.70ms | 926.90ms | 1.1× | 0.5% | PASS |
| file_reads | `table_scan` | 984.96ms | 1.03s | 1.0× | 1.1% | PASS |
| file_reads | `oltp_read_only` | 176.79ms | 112.93ms | 0.6× | 1.3% | PASS |
| file_writes | `oltp_bulk_insert` | 203.34ms | 253.64ms | 1.2× | 21.7% | PASS |
| file_writes | `oltp_insert` | 29.89ms | 33.86ms | 1.1× | 8.7% | PASS |
| file_writes | `oltp_update_index` | 137.13ms | 127.47ms | 0.9× | 8.1% | PASS |
| file_writes | `oltp_update_non_index` | 116.64ms | 89.98ms | 0.8× | 12.7% | PASS |
| file_writes | `oltp_delete_insert` | 154.99ms | 123.00ms | 0.8× | 29.3% | PASS |
| file_writes | `oltp_write_only` | 113.48ms | 91.33ms | 0.8× | 38.0% | PASS |
| file_writes | `types_delete_insert` | 84.59ms | 65.31ms | 0.8× | 27.9% | PASS |
| file_writes | `oltp_read_write` | 216.80ms | 202.36ms | 0.9× | 41.2% | PASS |
| ac_reads | `oltp_point_select` | 40.79ms | 38.58ms | 0.9× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 11.51ms | 10.66ms | 0.9× | 1.4% | PASS |
| ac_reads | `oltp_sum_range` | 10.18ms | 10.34ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_order_range` | 2.55ms | 2.48ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_distinct_range` | 3.30ms | 3.33ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 5.74ms | 5.97ms | 1.0× | 0.9% | PASS |
| ac_reads | `select_random_points` | 11.53ms | 10.83ms | 0.9× | 1.9% | PASS |
| ac_reads | `select_random_ranges` | 6.15ms | 5.52ms | 0.9× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 8.62ms | 9.19ms | 1.1× | 1.0% | PASS |
| ac_reads | `groupby_scan` | 24.87ms | 27.25ms | 1.1× | 0.6% | PASS |
| ac_reads | `index_join` | 6.17ms | 7.45ms | 1.2× | 1.1% | PASS |
| ac_reads | `index_join_scan` | 3.08ms | 4.31ms | 1.4× | 2.5% | PASS |
| ac_reads | `types_table_scan` | 872.20ms | 934.40ms | 1.1× | 1.7% | PASS |
| ac_reads | `table_scan` | 990.29ms | 1.03s | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_read_only` | 112.74ms | 112.38ms | 1.0× | 1.7% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 31.07ms | 111.45ms | 3.6× | 48.3% | PASS |
| ac_writes | `oltp_insert_ac` | 33.04ms | 91.19ms | 2.8× | 21.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 39.80ms | 141.08ms | 3.5× | 44.3% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 35.83ms | 94.60ms | 2.6× | 45.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 39.40ms | 114.49ms | 2.9× | 23.4% | PASS |
| ac_writes | `oltp_write_only_ac` | 39.74ms | 144.75ms | 3.6× | 54.7% | PASS |
| ac_writes | `types_delete_insert_ac` | 52.50ms | 120.06ms | 2.3× | 61.3% | PASS |
| ac_writes | `oltp_read_write_ac` | 36.63ms | 119.87ms | 3.3× | 27.6% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.04ms | 23.06ms | 1.0× | 2.8% | PASS |
| mem_reads | `oltp_range_select` | 11.30ms | 9.58ms | 0.8× | 2.4% | PASS |
| mem_reads | `oltp_sum_range` | 10.65ms | 9.15ms | 0.9× | 3.3% | PASS |
| mem_reads | `oltp_order_range` | 2.25ms | 2.03ms | 0.9× | 2.4% | PASS |
| mem_reads | `oltp_distinct_range` | 2.77ms | 2.51ms | 0.9× | 2.1% | PASS |
| mem_reads | `oltp_index_scan` | 2.54ms | 3.84ms | 1.5× | 2.5% | PASS |
| mem_reads | `select_random_points` | 16.82ms | 14.31ms | 0.9× | 5.1% | PASS |
| mem_reads | `select_random_ranges` | 4.48ms | 3.75ms | 0.8× | 4.1% | PASS |
| mem_reads | `covering_index_scan` | 3.89ms | 5.16ms | 1.3× | 2.3% | PASS |
| mem_reads | `groupby_scan` | 20.16ms | 19.59ms | 1.0× | 2.2% | PASS |
| mem_reads | `index_join` | 7.84ms | 5.47ms | 0.7× | 4.7% | PASS |
| mem_reads | `index_join_scan` | 2.56ms | 4.50ms | 1.8× | 4.9% | PASS |
| mem_reads | `types_table_scan` | 706.95ms | 768.62ms | 1.1× | 1.8% | PASS |
| mem_reads | `table_scan` | 819.33ms | 859.65ms | 1.0× | 2.1% | PASS |
| mem_reads | `oltp_read_only` | 81.58ms | 76.47ms | 0.9× | 3.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 139.60ms | 201.91ms | 1.4× | 2.0% | PASS |
| mem_writes | `oltp_insert` | 10.69ms | 23.06ms | 2.2× | 1.6% | PASS |
| mem_writes | `oltp_update_index` | 45.23ms | 92.38ms | 2.0× | 2.0% | PASS |
| mem_writes | `oltp_update_non_index` | 36.48ms | 55.36ms | 1.5× | 2.3% | PASS |
| mem_writes | `oltp_delete_insert` | 36.02ms | 67.26ms | 1.9× | 2.4% | PASS |
| mem_writes | `oltp_write_only` | 19.26ms | 40.51ms | 2.1× | 1.7% | PASS |
| mem_writes | `types_delete_insert` | 26.28ms | 34.26ms | 1.3× | 2.9% | PASS |
| mem_writes | `oltp_read_write` | 56.32ms | 82.79ms | 1.5× | 3.0% | PASS |
| file_reads | `oltp_point_select` | 74.30ms | 36.45ms | 0.5× | 1.4% | PASS |
| file_reads | `oltp_range_select` | 15.79ms | 10.66ms | 0.7× | 1.7% | PASS |
| file_reads | `oltp_sum_range` | 15.70ms | 10.44ms | 0.7× | 2.4% | PASS |
| file_reads | `oltp_order_range` | 2.87ms | 2.25ms | 0.8× | 2.3% | PASS |
| file_reads | `oltp_distinct_range` | 3.37ms | 2.75ms | 0.8× | 1.9% | PASS |
| file_reads | `oltp_index_scan` | 8.18ms | 5.72ms | 0.7× | 1.6% | PASS |
| file_reads | `select_random_points` | 20.85ms | 14.90ms | 0.7× | 1.7% | PASS |
| file_reads | `select_random_ranges` | 10.19ms | 5.36ms | 0.5× | 2.1% | PASS |
| file_reads | `covering_index_scan` | 9.77ms | 7.17ms | 0.7× | 2.4% | PASS |
| file_reads | `groupby_scan` | 21.27ms | 20.34ms | 1.0× | 2.0% | PASS |
| file_reads | `index_join` | 11.26ms | 7.37ms | 0.7× | 1.7% | PASS |
| file_reads | `index_join_scan` | 3.37ms | 4.74ms | 1.4× | 1.4% | PASS |
| file_reads | `types_table_scan` | 697.86ms | 744.49ms | 1.1× | 2.8% | PASS |
| file_reads | `table_scan` | 795.04ms | 844.59ms | 1.1× | 1.6% | PASS |
| file_reads | `oltp_read_only` | 149.45ms | 92.91ms | 0.6× | 1.5% | PASS |
| file_writes | `oltp_bulk_insert` | 305.11ms | 294.05ms | 1.0× | 19.8% | PASS |
| file_writes | `oltp_insert` | 68.30ms | 56.95ms | 0.8× | 64.6% | PASS |
| file_writes | `oltp_update_index` | 231.48ms | 217.77ms | 0.9× | 36.5% | PASS |
| file_writes | `oltp_update_non_index` | 201.29ms | 127.22ms | 0.6× | 30.4% | PASS |
| file_writes | `oltp_delete_insert` | 294.01ms | 198.23ms | 0.7× | 36.7% | PASS |
| file_writes | `oltp_write_only` | 206.46ms | 132.10ms | 0.6× | 47.0% | PASS |
| file_writes | `types_delete_insert` | 181.74ms | 92.29ms | 0.5× | 26.9% | PASS |
| file_writes | `oltp_read_write` | 223.91ms | 209.97ms | 0.9× | 35.5% | PASS |
| ac_reads | `oltp_point_select` | 41.06ms | 36.69ms | 0.9× | 2.5% | PASS |
| ac_reads | `oltp_range_select` | 13.07ms | 10.94ms | 0.8× | 3.1% | PASS |
| ac_reads | `oltp_sum_range` | 12.66ms | 10.59ms | 0.8× | 2.9% | PASS |
| ac_reads | `oltp_order_range` | 2.63ms | 2.27ms | 0.9× | 1.5% | PASS |
| ac_reads | `oltp_distinct_range` | 3.10ms | 2.79ms | 0.9× | 1.9% | PASS |
| ac_reads | `oltp_index_scan` | 4.92ms | 5.87ms | 1.2× | 2.8% | PASS |
| ac_reads | `select_random_points` | 18.68ms | 15.60ms | 0.8× | 3.3% | PASS |
| ac_reads | `select_random_ranges` | 6.66ms | 5.34ms | 0.8× | 1.6% | PASS |
| ac_reads | `covering_index_scan` | 6.30ms | 7.17ms | 1.1× | 2.1% | PASS |
| ac_reads | `groupby_scan` | 21.04ms | 20.20ms | 1.0× | 2.0% | PASS |
| ac_reads | `index_join` | 9.81ms | 7.43ms | 0.8× | 2.2% | PASS |
| ac_reads | `index_join_scan` | 3.17ms | 4.87ms | 1.5× | 2.7% | PASS |
| ac_reads | `types_table_scan` | 731.55ms | 784.51ms | 1.1× | 2.4% | PASS |
| ac_reads | `table_scan` | 893.86ms | 890.33ms | 1.0× | 5.0% | PASS |
| ac_reads | `oltp_read_only` | 110.10ms | 98.43ms | 0.9× | 3.6% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 205.25ms | 361.93ms | 1.8× | 62.4% | PASS |
| ac_writes | `oltp_insert_ac` | 114.31ms | 245.14ms | 2.1× | 44.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 160.50ms | 410.56ms | 2.6× | 50.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 91.46ms | 346.44ms | 3.8× | 59.4% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 122.62ms | 385.87ms | 3.1× | 60.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 121.55ms | 308.19ms | 2.5× | 68.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 130.24ms | 322.10ms | 2.5× | 65.6% | PASS |
| ac_writes | `oltp_read_write_ac` | 170.95ms | 379.16ms | 2.2× | 54.2% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 35.42ms | 38.72ms | 1.1× | 3.1% | PASS |
| mem_reads | `oltp_range_select` | 15.81ms | 15.50ms | 1.0× | 2.2% | PASS |
| mem_reads | `oltp_sum_range` | 14.36ms | 14.00ms | 1.0× | 1.5% | PASS |
| mem_reads | `oltp_order_range` | 3.19ms | 3.27ms | 1.0× | 1.3% | PASS |
| mem_reads | `oltp_distinct_range` | 4.27ms | 4.40ms | 1.0× | 0.8% | PASS |
| mem_reads | `oltp_index_scan` | 3.95ms | 6.31ms | 1.6× | 1.0% | PASS |
| mem_reads | `select_random_points` | 21.03ms | 21.14ms | 1.0× | 1.8% | PASS |
| mem_reads | `select_random_ranges` | 6.56ms | 6.50ms | 1.0× | 1.4% | PASS |
| mem_reads | `covering_index_scan` | 7.76ms | 10.52ms | 1.4× | 1.1% | PASS |
| mem_reads | `groupby_scan` | 33.36ms | 34.28ms | 1.0× | 0.8% | PASS |
| mem_reads | `index_join` | 9.98ms | 9.38ms | 0.9× | 2.0% | PASS |
| mem_reads | `index_join_scan` | 3.63ms | 5.56ms | 1.5× | 2.1% | PASS |
| mem_reads | `types_table_scan` | 1.10s | 1.24s | 1.1× | 0.9% | PASS |
| mem_reads | `table_scan` | 1.29s | 1.34s | 1.0× | 1.0% | PASS |
| mem_reads | `oltp_read_only` | 137.57ms | 143.85ms | 1.0× | 1.2% | PASS |
| mem_writes | `oltp_bulk_insert` | 243.16ms | 360.76ms | 1.5× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 18.50ms | 38.63ms | 2.1× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 63.58ms | 135.71ms | 2.1× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 46.94ms | 80.06ms | 1.7× | 1.6% | PASS |
| mem_writes | `oltp_delete_insert` | 52.58ms | 104.35ms | 2.0× | 1.3% | PASS |
| mem_writes | `oltp_write_only` | 27.21ms | 60.83ms | 2.2× | 1.2% | PASS |
| mem_writes | `types_delete_insert` | 37.74ms | 53.68ms | 1.4× | 1.7% | PASS |
| mem_writes | `oltp_read_write` | 100.21ms | 151.33ms | 1.5× | 1.4% | PASS |
| file_reads | `oltp_point_select` | 107.34ms | 58.37ms | 0.5× | 1.2% | PASS |
| file_reads | `oltp_range_select` | 24.72ms | 17.83ms | 0.7× | 2.0% | PASS |
| file_reads | `oltp_sum_range` | 22.91ms | 16.34ms | 0.7× | 1.3% | PASS |
| file_reads | `oltp_order_range` | 4.26ms | 3.58ms | 0.8× | 1.2% | PASS |
| file_reads | `oltp_distinct_range` | 5.37ms | 4.73ms | 0.9× | 0.9% | PASS |
| file_reads | `oltp_index_scan` | 11.27ms | 8.54ms | 0.8× | 1.2% | PASS |
| file_reads | `select_random_points` | 30.76ms | 24.42ms | 0.8× | 1.6% | PASS |
| file_reads | `select_random_ranges` | 14.24ms | 8.67ms | 0.6× | 1.3% | PASS |
| file_reads | `covering_index_scan` | 15.38ms | 12.71ms | 0.8× | 0.9% | PASS |
| file_reads | `groupby_scan` | 34.76ms | 35.38ms | 1.0× | 1.2% | PASS |
| file_reads | `index_join` | 14.83ms | 11.17ms | 0.8× | 2.2% | PASS |
| file_reads | `index_join_scan` | 4.72ms | 6.10ms | 1.3× | 2.4% | PASS |
| file_reads | `types_table_scan` | 1.11s | 1.23s | 1.1× | 1.8% | PASS |
| file_reads | `table_scan` | 1.25s | 1.33s | 1.1× | 2.1% | PASS |
| file_reads | `oltp_read_only` | 243.81ms | 173.02ms | 0.7× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 265.10ms | 375.54ms | 1.4× | 1.1% | PASS |
| file_writes | `oltp_insert` | 26.05ms | 45.83ms | 1.8× | 2.1% | PASS |
| file_writes | `oltp_update_index` | 96.56ms | 155.59ms | 1.6× | 1.8% | PASS |
| file_writes | `oltp_update_non_index` | 94.44ms | 97.06ms | 1.0× | 11.7% | PASS |
| file_writes | `oltp_delete_insert` | 89.38ms | 120.83ms | 1.4× | 2.3% | PASS |
| file_writes | `oltp_write_only` | 58.75ms | 75.89ms | 1.3× | 2.1% | PASS |
| file_writes | `types_delete_insert` | 65.31ms | 67.90ms | 1.0× | 1.8% | PASS |
| file_writes | `oltp_read_write` | 136.32ms | 166.18ms | 1.2× | 2.1% | PASS |
| ac_reads | `oltp_point_select` | 60.17ms | 58.47ms | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_range_select` | 19.62ms | 18.00ms | 0.9× | 1.7% | PASS |
| ac_reads | `oltp_sum_range` | 18.30ms | 16.64ms | 0.9× | 1.6% | PASS |
| ac_reads | `oltp_order_range` | 3.82ms | 3.61ms | 0.9× | 1.4% | PASS |
| ac_reads | `oltp_distinct_range` | 4.82ms | 4.76ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_index_scan` | 6.73ms | 8.66ms | 1.3× | 1.5% | PASS |
| ac_reads | `select_random_points` | 26.62ms | 25.32ms | 1.0× | 2.9% | PASS |
| ac_reads | `select_random_ranges` | 9.43ms | 8.71ms | 0.9× | 1.4% | PASS |
| ac_reads | `covering_index_scan` | 10.65ms | 12.92ms | 1.2× | 1.1% | PASS |
| ac_reads | `groupby_scan` | 34.18ms | 35.54ms | 1.0× | 0.7% | PASS |
| ac_reads | `index_join` | 12.63ms | 11.43ms | 0.9× | 2.0% | PASS |
| ac_reads | `index_join_scan` | 4.29ms | 6.28ms | 1.5× | 2.4% | PASS |
| ac_reads | `types_table_scan` | 1.18s | 1.26s | 1.1× | 0.6% | PASS |
| ac_reads | `table_scan` | 1.32s | 1.35s | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_read_only` | 184.37ms | 177.34ms | 1.0× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 24.21ms | 78.57ms | 3.2× | 6.2% | PASS |
| ac_writes | `oltp_insert_ac` | 26.16ms | 97.91ms | 3.7× | 8.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 28.42ms | 108.18ms | 3.8× | 5.4% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.63ms | 88.70ms | 3.8× | 9.5% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.87ms | 98.40ms | 3.7× | 7.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.86ms | 96.08ms | 3.7× | 5.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 23.71ms | 89.43ms | 3.8× | 7.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.97ms | 103.26ms | 3.1× | 4.0% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 34.55ms | 41.56ms | 1.2× | 2.0% | PASS |
| mem_reads | `oltp_range_select` | 20.06ms | 23.26ms | 1.2× | 2.4% | PASS |
| mem_reads | `oltp_sum_range` | 18.09ms | 21.85ms | 1.2× | 1.3% | PASS |
| mem_reads | `oltp_order_range` | 3.57ms | 4.02ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_distinct_range` | 4.73ms | 5.15ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 4.67ms | 6.22ms | 1.3× | 1.7% | PASS |
| mem_reads | `select_random_points` | 28.63ms | 32.97ms | 1.2× | 2.2% | PASS |
| mem_reads | `select_random_ranges` | 7.80ms | 9.11ms | 1.2× | 1.3% | PASS |
| mem_reads | `covering_index_scan` | 7.79ms | 10.60ms | 1.4× | 1.1% | PASS |
| mem_reads | `groupby_scan` | 35.65ms | 39.91ms | 1.1× | 0.8% | PASS |
| mem_reads | `index_join` | 8.04ms | 10.80ms | 1.3× | 1.7% | PASS |
| mem_reads | `index_join_scan` | 4.10ms | 5.79ms | 1.4× | 3.2% | PASS |
| mem_reads | `types_table_scan` | 1.11s | 1.23s | 1.1× | 1.8% | PASS |
| mem_reads | `table_scan` | 1.33s | 1.33s | 1.0× | 2.6% | PASS |
| mem_reads | `oltp_read_only` | 154.32ms | 178.27ms | 1.2× | 1.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 247.17ms | 348.00ms | 1.4× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 18.98ms | 35.72ms | 1.9× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 67.05ms | 126.82ms | 1.9× | 1.5% | PASS |
| mem_writes | `oltp_update_non_index` | 52.76ms | 82.49ms | 1.6× | 1.4% | PASS |
| mem_writes | `oltp_delete_insert` | 51.52ms | 101.96ms | 2.0× | 2.2% | PASS |
| mem_writes | `oltp_write_only` | 28.28ms | 61.62ms | 2.2× | 1.7% | PASS |
| mem_writes | `types_delete_insert` | 34.03ms | 55.02ms | 1.6× | 1.5% | PASS |
| mem_writes | `oltp_read_write` | 108.18ms | 173.70ms | 1.6× | 1.4% | PASS |
| file_reads | `oltp_point_select` | 105.42ms | 60.92ms | 0.6× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 27.50ms | 25.07ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_sum_range` | 25.74ms | 23.66ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_order_range` | 4.56ms | 4.34ms | 1.0× | 2.1% | PASS |
| file_reads | `oltp_distinct_range` | 5.65ms | 5.46ms | 1.0× | 1.2% | PASS |
| file_reads | `oltp_index_scan` | 12.06ms | 8.35ms | 0.7× | 1.8% | PASS |
| file_reads | `select_random_points` | 38.67ms | 36.65ms | 0.9× | 1.3% | PASS |
| file_reads | `select_random_ranges` | 15.49ms | 11.25ms | 0.7× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 15.38ms | 12.64ms | 0.8× | 1.5% | PASS |
| file_reads | `groupby_scan` | 36.74ms | 40.62ms | 1.1× | 1.1% | PASS |
| file_reads | `index_join` | 12.50ms | 12.35ms | 1.0× | 1.5% | PASS |
| file_reads | `index_join_scan` | 4.97ms | 6.14ms | 1.2× | 2.2% | PASS |
| file_reads | `types_table_scan` | 1.13s | 1.23s | 1.1× | 2.1% | PASS |
| file_reads | `table_scan` | 1.37s | 1.34s | 1.0× | 1.6% | PASS |
| file_reads | `oltp_read_only` | 263.26ms | 208.58ms | 0.8× | 1.1% | PASS |
| file_writes | `oltp_bulk_insert` | 262.04ms | 360.17ms | 1.4× | 0.7% | PASS |
| file_writes | `oltp_insert` | 26.73ms | 42.65ms | 1.6× | 1.9% | PASS |
| file_writes | `oltp_update_index` | 97.49ms | 142.69ms | 1.5× | 2.0% | PASS |
| file_writes | `oltp_update_non_index` | 77.87ms | 97.88ms | 1.3× | 1.5% | PASS |
| file_writes | `oltp_delete_insert` | 78.96ms | 115.15ms | 1.5× | 1.6% | PASS |
| file_writes | `oltp_write_only` | 52.51ms | 72.90ms | 1.4× | 1.4% | PASS |
| file_writes | `types_delete_insert` | 50.29ms | 63.39ms | 1.3× | 1.2% | PASS |
| file_writes | `oltp_read_write` | 133.96ms | 184.32ms | 1.4× | 1.8% | PASS |
| ac_reads | `oltp_point_select` | 59.86ms | 62.49ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 23.65ms | 25.48ms | 1.1× | 1.8% | PASS |
| ac_reads | `oltp_sum_range` | 21.20ms | 23.98ms | 1.1× | 2.3% | PASS |
| ac_reads | `oltp_order_range` | 4.00ms | 4.31ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_distinct_range` | 5.01ms | 5.40ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 7.41ms | 8.34ms | 1.1× | 1.7% | PASS |
| ac_reads | `select_random_points` | 32.13ms | 36.52ms | 1.1× | 2.1% | PASS |
| ac_reads | `select_random_ranges` | 10.46ms | 11.24ms | 1.1× | 1.4% | PASS |
| ac_reads | `covering_index_scan` | 10.64ms | 12.62ms | 1.2× | 1.7% | PASS |
| ac_reads | `groupby_scan` | 35.66ms | 40.31ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 9.87ms | 12.35ms | 1.3× | 1.7% | PASS |
| ac_reads | `index_join_scan` | 4.48ms | 6.10ms | 1.4× | 1.8% | PASS |
| ac_reads | `types_table_scan` | 1.10s | 1.22s | 1.1× | 1.1% | PASS |
| ac_reads | `table_scan` | 1.35s | 1.34s | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_read_only` | 191.51ms | 208.57ms | 1.1× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 24.09ms | 82.42ms | 3.4× | 6.1% | PASS |
| ac_writes | `oltp_insert_ac` | 25.22ms | 95.97ms | 3.8× | 5.8% | PASS |
| ac_writes | `oltp_update_index_ac` | 28.10ms | 107.82ms | 3.8× | 7.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.24ms | 89.23ms | 3.7× | 7.2% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.73ms | 99.32ms | 3.7× | 8.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.91ms | 97.04ms | 3.6× | 8.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 23.13ms | 92.42ms | 4.0× | 9.0% | PASS |
| ac_writes | `oltp_read_write_ac` | 34.09ms | 105.45ms | 3.1× | 7.1% | PASS |

</details>

</details>

## Version-control latency

Wall time: 3m 54s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 34.21ms | 130.00ms | 26.3% | 1.3% | PASS |
| `status_dirty_many_tables` | 37.29ms | 130.00ms | 28.7% | 0.4% | PASS |
| `diff_regular_working_one_table` | 29.50ms | 120.00ms | 24.6% | 0.7% | PASS |
| `diff_regular_working_many_tables` | 42.59ms | 140.00ms | 30.4% | 0.5% | PASS |
| `diff_stat_working_many_tables` | 42.78ms | 140.00ms | 30.6% | 0.7% | PASS |
| `diff_schema_working_many_tables` | 44.21ms | 140.00ms | 31.6% | 0.8% | PASS |
| `branch_list_many_branches` | 22.22ms | 35.00ms | 63.5% | 1.0% | PASS |
| `branch_create_delete` | 24.51ms | 40.00ms | 61.3% | 0.7% | PASS |
| `at_literal_deep_history` | 27.03ms | 100.00ms | 27.0% | 1.0% | PASS |
| `diff_literal_deep_history` | 26.79ms | 120.00ms | 22.3% | 0.6% | PASS |
| `history_literal_deep_history` | 28.50ms | 150.00ms | 19.0% | 1.1% | PASS |
| `checkout_branch_clean` | 37.22ms | 150.00ms | 24.8% | 0.9% | PASS |
| `merge_data_no_conflicts` | 28.12ms | 50.00ms | 56.2% | 0.9% | PASS |
| `merge_data_secondary_index` | 1.13s | 2.50s | 45.2% | 1.1% | PASS |
| `merge_schema_no_conflicts` | 21.40ms | 35.00ms | 61.1% | 2.0% | PASS |
| `merge_data_conflicts` | 29.56ms | 180.00ms | 16.4% | 0.8% | PASS |
| `merge_data_conflicts_with_resolve` | 30.22ms | 180.00ms | 16.8% | 0.9% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
