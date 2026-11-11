# DoltLite Performance Report

> Nightly result: **FAIL**
>
> Generated: 2026-11-11 11:21 UTC
>
> Commit: [`39f0fa0983ebc18619d04b265188c9ece95deb03`](https://github.com/dolthub/doltlite/commit/39f0fa0983ebc18619d04b265188c9ece95deb03)
>
> Runner: ubuntu24 20261101.360.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/42995941558)

The SQL workloads compare optimized DoltLite against stock SQLite on the same GitHub-hosted runner, alternating execution order on each repetition. Version-control sections measure DoltLite latency. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.36s | 11.61s | 1.2× | 1.6% | **FAIL** |
| Writes | 1.96s | 3.35s | 1.7× | 1.4% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.88s | 11.70s | 1.2× | 1.3% | **PASS** |
| Writes | 3.15s | 4.01s | 1.3× | 1.7% | **PASS** |
| Autocommit writes | 714.93ms | 2.28s | 3.2× | 6.2% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.52s | 2.97s | 1.2× | 1.6% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 1.79s | 1.99s | 1.1× | 2.3% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.44s | 3.16s | 1.3× | 1.1% | **FAIL** |
| In-memory | Reads | compositepk | 15 | 55 | 2.61s | 3.49s | 1.3× | 1.6% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 461.50ms | 814.81ms | 1.8× | 1.4% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 378.15ms | 604.89ms | 1.6× | 1.8% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 526.03ms | 908.92ms | 1.7× | 1.2% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 592.65ms | 1.03s | 1.7× | 1.6% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.74s | 3.05s | 1.1× | 1.4% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 1.74s | 1.88s | 1.1× | 1.3% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.55s | 3.20s | 1.3× | 1.1% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.85s | 3.58s | 1.3× | 1.6% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 618.31ms | 875.07ms | 1.4× | 1.9% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.07s | 1.02s | 0.9× | 3.8% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 695.98ms | 1.02s | 1.5× | 1.4% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 762.10ms | 1.09s | 1.4× | 1.4% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.52s | 3.03s | 1.2× | 1.3% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 1.67s | 1.91s | 1.1× | 2.0% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.47s | 3.17s | 1.3× | 1.3% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.68s | 3.57s | 1.3× | 1.3% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 199.54ms | 652.72ms | 3.3× | 7.4% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 174.53ms | 491.95ms | 2.8× | 4.4% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 152.14ms | 527.46ms | 3.5× | 6.4% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 188.73ms | 608.90ms | 3.2× | 5.9% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 25.26ms | 34.27ms | 1.4× | 1.5% | PASS |
| mem_reads | `oltp_range_select` | 10.74ms | 14.45ms | 1.3× | 1.4% | PASS |
| mem_reads | `oltp_sum_range` | 9.84ms | 13.12ms | 1.3× | 1.5% | PASS |
| mem_reads | `oltp_order_range` | 2.65ms | 3.17ms | 1.2× | 1.6% | PASS |
| mem_reads | `oltp_distinct_range` | 3.70ms | 4.33ms | 1.2× | 1.4% | PASS |
| mem_reads | `oltp_index_scan` | 4.00ms | 6.01ms | 1.5× | 1.8% | PASS |
| mem_reads | `select_random_points` | 13.61ms | 12.70ms | 0.9× | 2.6% | PASS |
| mem_reads | `select_random_ranges` | 4.77ms | 5.79ms | 1.2× | 1.6% | PASS |
| mem_reads | `covering_index_scan` | 7.75ms | 11.49ms | 1.5× | 1.2% | PASS |
| mem_reads | `groupby_scan` | 30.55ms | 34.83ms | 1.1× | 0.7% | PASS |
| mem_reads | `index_join` | 5.85ms | 9.09ms | 1.6× | 1.6% | PASS |
| mem_reads | `index_join_scan` | 3.32ms | 5.89ms | 1.8× | 1.4% | PASS |
| mem_reads | `types_table_scan` | 1.07s | 1.28s | 1.2× | 2.3% | PASS |
| mem_reads | `table_scan` | 1.22s | 1.39s | 1.1× | 1.9% | PASS |
| mem_reads | `oltp_read_only` | 108.70ms | 134.91ms | 1.2× | 1.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 184.54ms | 322.13ms | 1.7× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 15.49ms | 27.88ms | 1.8× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 51.58ms | 101.15ms | 2.0× | 0.9% | PASS |
| mem_writes | `oltp_update_non_index` | 36.81ms | 68.60ms | 1.9× | 1.4% | PASS |
| mem_writes | `oltp_delete_insert` | 48.58ms | 82.84ms | 1.7× | 2.2% | PASS |
| mem_writes | `oltp_write_only` | 24.11ms | 50.58ms | 2.1× | 2.1% | PASS |
| mem_writes | `types_delete_insert` | 25.64ms | 40.37ms | 1.6× | 1.3% | PASS |
| mem_writes | `oltp_read_write` | 74.73ms | 121.28ms | 1.6× | 3.0% | PASS |
| file_reads | `oltp_point_select` | 89.36ms | 51.08ms | 0.6× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 17.93ms | 16.41ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_sum_range` | 16.77ms | 15.24ms | 0.9× | 1.2% | PASS |
| file_reads | `oltp_order_range` | 3.45ms | 3.43ms | 1.0× | 1.6% | PASS |
| file_reads | `oltp_distinct_range` | 4.58ms | 4.60ms | 1.0× | 1.1% | PASS |
| file_reads | `oltp_index_scan` | 10.79ms | 8.11ms | 0.8× | 1.4% | PASS |
| file_reads | `select_random_points` | 21.17ms | 14.48ms | 0.7× | 1.6% | PASS |
| file_reads | `select_random_ranges` | 11.56ms | 7.71ms | 0.7× | 1.3% | PASS |
| file_reads | `covering_index_scan` | 15.04ms | 13.61ms | 0.9× | 1.7% | PASS |
| file_reads | `groupby_scan` | 31.18ms | 34.90ms | 1.1× | 1.2% | PASS |
| file_reads | `index_join` | 9.72ms | 10.83ms | 1.1× | 1.6% | PASS |
| file_reads | `index_join_scan` | 4.37ms | 6.32ms | 1.4× | 1.8% | PASS |
| file_reads | `types_table_scan` | 1.06s | 1.29s | 1.2× | 1.8% | PASS |
| file_reads | `table_scan` | 1.25s | 1.41s | 1.1× | 1.2% | PASS |
| file_reads | `oltp_read_only` | 204.85ms | 161.12ms | 0.8× | 0.6% | PASS |
| file_writes | `oltp_bulk_insert` | 201.23ms | 336.05ms | 1.7× | 1.0% | PASS |
| file_writes | `oltp_insert` | 22.41ms | 31.48ms | 1.4× | 1.5% | PASS |
| file_writes | `oltp_update_index` | 79.87ms | 112.30ms | 1.4× | 1.9% | PASS |
| file_writes | `oltp_update_non_index` | 60.55ms | 75.42ms | 1.2× | 2.5% | PASS |
| file_writes | `oltp_delete_insert` | 70.30ms | 92.79ms | 1.3× | 1.9% | PASS |
| file_writes | `oltp_write_only` | 46.54ms | 55.06ms | 1.2× | 2.4% | PASS |
| file_writes | `types_delete_insert` | 41.41ms | 47.40ms | 1.1× | 1.9% | PASS |
| file_writes | `oltp_read_write` | 96.00ms | 124.58ms | 1.3× | 1.9% | PASS |
| ac_reads | `oltp_point_select` | 46.29ms | 51.02ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_range_select` | 13.37ms | 16.25ms | 1.2× | 1.2% | PASS |
| ac_reads | `oltp_sum_range` | 12.22ms | 15.12ms | 1.2× | 0.9% | PASS |
| ac_reads | `oltp_order_range` | 2.96ms | 3.41ms | 1.2× | 1.3% | PASS |
| ac_reads | `oltp_distinct_range` | 4.05ms | 4.62ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_index_scan` | 6.48ms | 8.11ms | 1.3× | 1.9% | PASS |
| ac_reads | `select_random_points` | 16.81ms | 14.66ms | 0.9× | 2.0% | PASS |
| ac_reads | `select_random_ranges` | 7.26ms | 7.75ms | 1.1× | 1.2% | PASS |
| ac_reads | `covering_index_scan` | 10.60ms | 13.55ms | 1.3× | 1.5% | PASS |
| ac_reads | `groupby_scan` | 30.59ms | 34.95ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 7.63ms | 10.87ms | 1.4× | 2.1% | PASS |
| ac_reads | `index_join_scan` | 3.88ms | 6.37ms | 1.6× | 1.9% | PASS |
| ac_reads | `types_table_scan` | 1.05s | 1.30s | 1.2× | 1.3% | PASS |
| ac_reads | `table_scan` | 1.16s | 1.39s | 1.2× | 0.9% | PASS |
| ac_reads | `oltp_read_only` | 142.98ms | 163.13ms | 1.1× | 0.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.93ms | 67.40ms | 2.9× | 7.6% | PASS |
| ac_writes | `oltp_insert_ac` | 24.49ms | 82.40ms | 3.4× | 7.1% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.28ms | 93.62ms | 3.4× | 6.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.40ms | 74.68ms | 3.2× | 7.5% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 23.52ms | 83.50ms | 3.6× | 5.5% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.26ms | 83.54ms | 3.3× | 8.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.76ms | 76.47ms | 3.4× | 7.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 29.90ms | 91.11ms | 3.0× | 7.9% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 26.65ms | 25.77ms | 1.0× | 2.3% | PASS |
| mem_reads | `oltp_range_select` | 11.47ms | 10.73ms | 0.9× | 2.2% | PASS |
| mem_reads | `oltp_sum_range` | 11.44ms | 11.27ms | 1.0× | 2.9% | PASS |
| mem_reads | `oltp_order_range` | 2.33ms | 2.30ms | 1.0× | 1.6% | PASS |
| mem_reads | `oltp_distinct_range` | 2.75ms | 2.75ms | 1.0× | 2.1% | PASS |
| mem_reads | `oltp_index_scan` | 2.60ms | 4.72ms | 1.8× | 1.6% | PASS |
| mem_reads | `select_random_points` | 17.15ms | 15.20ms | 0.9× | 2.6% | PASS |
| mem_reads | `select_random_ranges` | 4.59ms | 4.26ms | 0.9× | 1.6% | PASS |
| mem_reads | `covering_index_scan` | 3.91ms | 6.91ms | 1.8× | 3.3% | PASS |
| mem_reads | `groupby_scan` | 21.22ms | 21.08ms | 1.0× | 1.8% | PASS |
| mem_reads | `index_join` | 8.37ms | 8.24ms | 1.0× | 3.3% | PASS |
| mem_reads | `index_join_scan` | 3.01ms | 6.20ms | 2.1× | 4.1% | PASS |
| mem_reads | `types_table_scan` | 762.18ms | 870.39ms | 1.1× | 1.3% | PASS |
| mem_reads | `table_scan` | 832.28ms | 919.33ms | 1.1× | 4.9% | PASS |
| mem_reads | `oltp_read_only` | 83.65ms | 83.28ms | 1.0× | 5.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 146.33ms | 218.61ms | 1.5× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 10.93ms | 22.82ms | 2.1× | 1.3% | PASS |
| mem_writes | `oltp_update_index` | 41.91ms | 87.70ms | 2.1× | 2.6% | PASS |
| mem_writes | `oltp_update_non_index` | 31.39ms | 51.31ms | 1.6× | 1.4% | PASS |
| mem_writes | `oltp_delete_insert` | 34.01ms | 61.90ms | 1.8× | 1.7% | PASS |
| mem_writes | `oltp_write_only` | 17.68ms | 32.98ms | 1.9× | 1.9% | PASS |
| mem_writes | `types_delete_insert` | 25.16ms | 37.52ms | 1.5× | 3.0% | PASS |
| mem_writes | `oltp_read_write` | 70.75ms | 92.04ms | 1.3× | 5.2% | PASS |
| file_reads | `oltp_point_select` | 72.19ms | 36.75ms | 0.5× | 1.8% | PASS |
| file_reads | `oltp_range_select` | 14.92ms | 10.82ms | 0.7× | 1.5% | PASS |
| file_reads | `oltp_sum_range` | 14.83ms | 10.77ms | 0.7× | 1.8% | PASS |
| file_reads | `oltp_order_range` | 2.75ms | 2.33ms | 0.8× | 0.7% | PASS |
| file_reads | `oltp_distinct_range` | 3.20ms | 2.81ms | 0.9× | 1.1% | PASS |
| file_reads | `oltp_index_scan` | 7.50ms | 5.61ms | 0.7× | 0.9% | PASS |
| file_reads | `select_random_points` | 19.86ms | 14.78ms | 0.7× | 2.8% | PASS |
| file_reads | `select_random_ranges` | 9.46ms | 5.43ms | 0.6× | 1.3% | PASS |
| file_reads | `covering_index_scan` | 9.10ms | 7.61ms | 0.8× | 0.7% | PASS |
| file_reads | `groupby_scan` | 20.30ms | 20.00ms | 1.0× | 1.3% | PASS |
| file_reads | `index_join` | 10.37ms | 7.52ms | 0.7× | 1.7% | PASS |
| file_reads | `index_join_scan` | 3.25ms | 4.91ms | 1.5× | 1.6% | PASS |
| file_reads | `types_table_scan` | 655.21ms | 778.97ms | 1.2× | 0.9% | PASS |
| file_reads | `table_scan` | 753.53ms | 873.45ms | 1.2× | 1.1% | PASS |
| file_reads | `oltp_read_only` | 141.47ms | 95.00ms | 0.7× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 211.87ms | 271.20ms | 1.3× | 1.7% | PASS |
| file_writes | `oltp_insert` | 19.85ms | 43.02ms | 2.2× | 6.4% | PASS |
| file_writes | `oltp_update_index` | 159.56ms | 167.37ms | 1.0× | 3.6% | PASS |
| file_writes | `oltp_update_non_index` | 131.85ms | 108.41ms | 0.8× | 4.3% | PASS |
| file_writes | `oltp_delete_insert` | 166.66ms | 130.37ms | 0.8× | 3.1% | PASS |
| file_writes | `oltp_write_only` | 111.50ms | 88.66ms | 0.8× | 5.1% | PASS |
| file_writes | `types_delete_insert` | 126.07ms | 77.56ms | 0.6× | 3.9% | PASS |
| file_writes | `oltp_read_write` | 145.46ms | 130.49ms | 0.9× | 3.6% | PASS |
| ac_reads | `oltp_point_select` | 43.01ms | 38.28ms | 0.9× | 2.9% | PASS |
| ac_reads | `oltp_range_select` | 13.18ms | 11.63ms | 0.9× | 4.4% | PASS |
| ac_reads | `oltp_sum_range` | 12.36ms | 11.33ms | 0.9× | 2.5% | PASS |
| ac_reads | `oltp_order_range` | 2.42ms | 2.34ms | 1.0× | 2.1% | PASS |
| ac_reads | `oltp_distinct_range` | 2.95ms | 2.81ms | 0.9× | 2.0% | PASS |
| ac_reads | `oltp_index_scan` | 4.60ms | 5.70ms | 1.2× | 1.4% | PASS |
| ac_reads | `select_random_points` | 21.81ms | 17.69ms | 0.8× | 4.2% | PASS |
| ac_reads | `select_random_ranges` | 6.36ms | 5.42ms | 0.9× | 0.9% | PASS |
| ac_reads | `covering_index_scan` | 6.09ms | 7.75ms | 1.3× | 1.1% | PASS |
| ac_reads | `groupby_scan` | 20.40ms | 20.23ms | 1.0× | 1.0% | PASS |
| ac_reads | `index_join` | 9.25ms | 7.84ms | 0.8× | 2.7% | PASS |
| ac_reads | `index_join_scan` | 3.04ms | 4.95ms | 1.6× | 3.7% | PASS |
| ac_reads | `types_table_scan` | 670.27ms | 801.44ms | 1.2× | 1.1% | PASS |
| ac_reads | `table_scan` | 752.85ms | 874.21ms | 1.2× | 1.0% | PASS |
| ac_reads | `oltp_read_only` | 96.69ms | 95.32ms | 1.0× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 20.37ms | 50.33ms | 2.5× | 2.9% | PASS |
| ac_writes | `oltp_insert_ac` | 21.98ms | 57.91ms | 2.6× | 3.8% | PASS |
| ac_writes | `oltp_update_index_ac` | 22.20ms | 69.12ms | 3.1× | 4.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 19.60ms | 56.68ms | 2.9× | 4.5% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 22.26ms | 65.58ms | 2.9× | 4.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 21.40ms | 62.45ms | 2.9× | 4.7% | PASS |
| ac_writes | `types_delete_insert_ac` | 20.64ms | 59.03ms | 2.9× | 7.3% | PASS |
| ac_writes | `oltp_read_write_ac` | 26.08ms | 70.86ms | 2.7× | 6.5% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.10ms | 39.05ms | 1.2× | 1.5% | PASS |
| mem_reads | `oltp_range_select` | 14.29ms | 16.03ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_sum_range` | 13.61ms | 15.36ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_order_range` | 2.92ms | 3.29ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_distinct_range` | 3.74ms | 4.21ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 3.71ms | 6.48ms | 1.7× | 1.5% | PASS |
| mem_reads | `select_random_points` | 21.77ms | 23.90ms | 1.1× | 1.7% | PASS |
| mem_reads | `select_random_ranges` | 5.68ms | 6.66ms | 1.2× | 0.9% | PASS |
| mem_reads | `covering_index_scan` | 6.64ms | 9.95ms | 1.5× | 1.2% | PASS |
| mem_reads | `groupby_scan` | 31.09ms | 33.35ms | 1.1× | 0.4% | PASS |
| mem_reads | `index_join` | 10.48ms | 11.48ms | 1.1× | 1.7% | PASS |
| mem_reads | `index_join_scan` | 3.16ms | 7.29ms | 2.3× | 2.2% | FAIL |
| mem_reads | `types_table_scan` | 1.00s | 1.37s | 1.4× | 0.7% | PASS |
| mem_reads | `table_scan` | 1.16s | 1.46s | 1.3× | 0.5% | PASS |
| mem_reads | `oltp_read_only` | 129.35ms | 146.65ms | 1.1× | 1.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 209.92ms | 327.75ms | 1.6× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 16.30ms | 34.44ms | 2.1× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 58.65ms | 133.79ms | 2.3× | 1.2% | PASS |
| mem_writes | `oltp_update_non_index` | 43.28ms | 75.50ms | 1.7× | 1.2% | PASS |
| mem_writes | `oltp_delete_insert` | 46.53ms | 95.75ms | 2.1× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 24.25ms | 49.10ms | 2.0× | 1.5% | PASS |
| mem_writes | `types_delete_insert` | 34.29ms | 58.23ms | 1.7× | 1.2% | PASS |
| mem_writes | `oltp_read_write` | 92.81ms | 134.36ms | 1.4× | 1.5% | PASS |
| file_reads | `oltp_point_select` | 61.28ms | 47.01ms | 0.8× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 17.46ms | 17.20ms | 1.0× | 0.8% | PASS |
| file_reads | `oltp_sum_range` | 16.93ms | 16.43ms | 1.0× | 1.1% | PASS |
| file_reads | `oltp_order_range` | 3.32ms | 3.42ms | 1.0× | 1.4% | PASS |
| file_reads | `oltp_distinct_range` | 4.19ms | 4.39ms | 1.0× | 1.2% | PASS |
| file_reads | `oltp_index_scan` | 6.79ms | 7.47ms | 1.1× | 1.3% | PASS |
| file_reads | `select_random_points` | 25.78ms | 25.25ms | 1.0× | 1.7% | PASS |
| file_reads | `select_random_ranges` | 8.86ms | 7.68ms | 0.9× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 9.86ms | 11.04ms | 1.1× | 1.3% | PASS |
| file_reads | `groupby_scan` | 31.58ms | 33.50ms | 1.1× | 0.6% | PASS |
| file_reads | `index_join` | 12.21ms | 11.73ms | 1.0× | 1.3% | PASS |
| file_reads | `index_join_scan` | 3.66ms | 7.31ms | 2.0× | 3.0% | PASS |
| file_reads | `types_table_scan` | 1.01s | 1.38s | 1.4× | 0.5% | PASS |
| file_reads | `table_scan` | 1.17s | 1.47s | 1.3× | 0.7% | PASS |
| file_reads | `oltp_read_only` | 171.68ms | 159.98ms | 0.9× | 0.7% | PASS |
| file_writes | `oltp_bulk_insert` | 227.01ms | 342.28ms | 1.5× | 0.9% | PASS |
| file_writes | `oltp_insert` | 21.34ms | 39.14ms | 1.8× | 1.2% | PASS |
| file_writes | `oltp_update_index` | 83.19ms | 157.46ms | 1.9× | 1.6% | PASS |
| file_writes | `oltp_update_non_index` | 83.41ms | 90.67ms | 1.1× | 18.3% | PASS |
| file_writes | `oltp_delete_insert` | 72.22ms | 115.30ms | 1.6× | 1.2% | PASS |
| file_writes | `oltp_write_only` | 44.48ms | 62.10ms | 1.4× | 1.8% | PASS |
| file_writes | `types_delete_insert` | 51.75ms | 65.88ms | 1.3× | 0.9% | PASS |
| file_writes | `oltp_read_write` | 112.58ms | 145.34ms | 1.3× | 1.6% | PASS |
| ac_reads | `oltp_point_select` | 43.15ms | 47.22ms | 1.1× | 1.9% | PASS |
| ac_reads | `oltp_range_select` | 15.35ms | 17.05ms | 1.1× | 1.5% | PASS |
| ac_reads | `oltp_sum_range` | 14.58ms | 16.19ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_order_range` | 3.10ms | 3.42ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_distinct_range` | 3.94ms | 4.36ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 4.95ms | 7.47ms | 1.5× | 1.3% | PASS |
| ac_reads | `select_random_points` | 23.62ms | 25.23ms | 1.1× | 1.8% | PASS |
| ac_reads | `select_random_ranges` | 6.99ms | 7.72ms | 1.1× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 7.84ms | 11.05ms | 1.4× | 1.4% | PASS |
| ac_reads | `groupby_scan` | 31.45ms | 33.54ms | 1.1× | 0.6% | PASS |
| ac_reads | `index_join` | 11.39ms | 11.84ms | 1.0× | 2.0% | PASS |
| ac_reads | `index_join_scan` | 3.46ms | 7.29ms | 2.1× | 2.5% | PASS |
| ac_reads | `types_table_scan` | 993.38ms | 1.36s | 1.4× | 0.7% | PASS |
| ac_reads | `table_scan` | 1.16s | 1.46s | 1.3× | 0.4% | PASS |
| ac_reads | `oltp_read_only` | 145.11ms | 159.54ms | 1.1× | 0.7% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 16.45ms | 49.10ms | 3.0× | 6.9% | PASS |
| ac_writes | `oltp_insert_ac` | 18.56ms | 64.81ms | 3.5× | 5.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 20.73ms | 82.45ms | 4.0× | 5.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 17.18ms | 60.41ms | 3.5× | 6.2% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 20.01ms | 71.36ms | 3.6× | 4.9% | PASS |
| ac_writes | `oltp_write_only_ac` | 18.56ms | 65.57ms | 3.5× | 7.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 17.25ms | 60.09ms | 3.5× | 9.3% | PASS |
| ac_writes | `oltp_read_write_ac` | 23.39ms | 73.66ms | 3.1× | 6.6% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.20ms | 44.65ms | 1.3× | 1.9% | PASS |
| mem_reads | `oltp_range_select` | 19.66ms | 26.18ms | 1.3× | 1.8% | PASS |
| mem_reads | `oltp_sum_range` | 17.93ms | 23.78ms | 1.3× | 1.7% | PASS |
| mem_reads | `oltp_order_range` | 3.53ms | 4.32ms | 1.2× | 2.0% | PASS |
| mem_reads | `oltp_distinct_range` | 4.69ms | 5.57ms | 1.2× | 1.4% | PASS |
| mem_reads | `oltp_index_scan` | 4.50ms | 6.66ms | 1.5× | 1.7% | PASS |
| mem_reads | `select_random_points` | 28.00ms | 34.42ms | 1.2× | 1.6% | PASS |
| mem_reads | `select_random_ranges` | 7.70ms | 9.77ms | 1.3× | 1.4% | PASS |
| mem_reads | `covering_index_scan` | 7.68ms | 11.29ms | 1.5× | 1.2% | PASS |
| mem_reads | `groupby_scan` | 36.65ms | 43.49ms | 1.2× | 1.0% | PASS |
| mem_reads | `index_join` | 7.87ms | 12.08ms | 1.5× | 2.2% | PASS |
| mem_reads | `index_join_scan` | 3.95ms | 6.86ms | 1.7× | 2.8% | PASS |
| mem_reads | `types_table_scan` | 1.05s | 1.48s | 1.4× | 0.9% | PASS |
| mem_reads | `table_scan` | 1.23s | 1.59s | 1.3× | 1.6% | PASS |
| mem_reads | `oltp_read_only` | 154.25ms | 195.72ms | 1.3× | 1.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 244.07ms | 374.73ms | 1.5× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 18.81ms | 35.49ms | 1.9× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 65.73ms | 131.70ms | 2.0× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 51.00ms | 94.13ms | 1.8× | 1.6% | PASS |
| mem_writes | `oltp_delete_insert` | 49.48ms | 98.11ms | 2.0× | 1.6% | PASS |
| mem_writes | `oltp_write_only` | 27.22ms | 59.44ms | 2.2× | 1.6% | PASS |
| mem_writes | `types_delete_insert` | 32.45ms | 61.78ms | 1.9× | 1.8% | PASS |
| mem_writes | `oltp_read_write` | 103.89ms | 170.25ms | 1.6× | 2.1% | PASS |
| file_reads | `oltp_point_select` | 98.86ms | 61.50ms | 0.6× | 1.3% | PASS |
| file_reads | `oltp_range_select` | 26.96ms | 28.30ms | 1.0× | 1.9% | PASS |
| file_reads | `oltp_sum_range` | 25.09ms | 25.78ms | 1.0× | 1.8% | PASS |
| file_reads | `oltp_order_range` | 4.38ms | 4.63ms | 1.1× | 2.5% | PASS |
| file_reads | `oltp_distinct_range` | 5.60ms | 5.90ms | 1.1× | 1.6% | PASS |
| file_reads | `oltp_index_scan` | 11.53ms | 8.97ms | 0.8× | 1.9% | PASS |
| file_reads | `select_random_points` | 36.52ms | 37.88ms | 1.0× | 1.6% | PASS |
| file_reads | `select_random_ranges` | 14.79ms | 11.79ms | 0.8× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 14.61ms | 13.49ms | 0.9× | 1.0% | PASS |
| file_reads | `groupby_scan` | 37.71ms | 43.72ms | 1.2× | 0.8% | PASS |
| file_reads | `index_join` | 11.85ms | 13.66ms | 1.2× | 2.1% | PASS |
| file_reads | `index_join_scan` | 4.79ms | 7.07ms | 1.5× | 1.8% | PASS |
| file_reads | `types_table_scan` | 1.06s | 1.50s | 1.4× | 1.0% | PASS |
| file_reads | `table_scan` | 1.25s | 1.59s | 1.3× | 2.0% | PASS |
| file_reads | `oltp_read_only` | 252.39ms | 222.39ms | 0.9× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 259.18ms | 386.32ms | 1.5× | 0.9% | PASS |
| file_writes | `oltp_insert` | 25.84ms | 39.56ms | 1.5× | 1.2% | PASS |
| file_writes | `oltp_update_index` | 95.86ms | 145.86ms | 1.5× | 1.4% | PASS |
| file_writes | `oltp_update_non_index` | 76.45ms | 102.38ms | 1.3× | 1.7% | PASS |
| file_writes | `oltp_delete_insert` | 76.84ms | 112.01ms | 1.5× | 1.5% | PASS |
| file_writes | `oltp_write_only` | 50.50ms | 66.19ms | 1.3× | 1.5% | PASS |
| file_writes | `types_delete_insert` | 49.26ms | 67.41ms | 1.4× | 1.5% | PASS |
| file_writes | `oltp_read_write` | 128.16ms | 175.17ms | 1.4× | 1.4% | PASS |
| ac_reads | `oltp_point_select` | 55.20ms | 61.72ms | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_range_select` | 22.44ms | 28.17ms | 1.3× | 2.1% | PASS |
| ac_reads | `oltp_sum_range` | 20.49ms | 25.66ms | 1.3× | 1.6% | PASS |
| ac_reads | `oltp_order_range` | 3.92ms | 4.65ms | 1.2× | 1.2% | PASS |
| ac_reads | `oltp_distinct_range` | 5.11ms | 5.92ms | 1.2× | 1.0% | PASS |
| ac_reads | `oltp_index_scan` | 7.06ms | 8.86ms | 1.3× | 2.2% | PASS |
| ac_reads | `select_random_points` | 31.23ms | 38.07ms | 1.2× | 1.3% | PASS |
| ac_reads | `select_random_ranges` | 10.19ms | 11.78ms | 1.2× | 1.6% | PASS |
| ac_reads | `covering_index_scan` | 10.31ms | 13.54ms | 1.3× | 1.1% | PASS |
| ac_reads | `groupby_scan` | 37.23ms | 43.65ms | 1.2× | 0.6% | PASS |
| ac_reads | `index_join` | 9.56ms | 13.63ms | 1.4× | 2.3% | PASS |
| ac_reads | `index_join_scan` | 4.36ms | 7.10ms | 1.6× | 2.6% | PASS |
| ac_reads | `types_table_scan` | 1.06s | 1.50s | 1.4× | 1.2% | PASS |
| ac_reads | `table_scan` | 1.22s | 1.59s | 1.3× | 0.7% | PASS |
| ac_reads | `oltp_read_only` | 187.33ms | 222.43ms | 1.2× | 1.3% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.03ms | 60.88ms | 2.9× | 5.4% | PASS |
| ac_writes | `oltp_insert_ac` | 23.85ms | 75.81ms | 3.2× | 3.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 25.04ms | 88.06ms | 3.5× | 5.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.27ms | 69.56ms | 3.3× | 5.2% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 22.65ms | 78.04ms | 3.4× | 6.2% | PASS |
| ac_writes | `oltp_write_only_ac` | 23.20ms | 77.93ms | 3.4× | 6.5% | PASS |
| ac_writes | `types_delete_insert_ac` | 20.84ms | 71.38ms | 3.4× | 9.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 30.85ms | 87.25ms | 2.8× | 6.2% | PASS |

</details>

</details>

## Version-control latency

Wall time: 6m 29s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 32.10ms | 38.00ms | 84.5% | 1.5% | PASS |
| `status_dirty_many_tables` | 35.42ms | 42.00ms | 84.3% | 1.2% | PASS |
| `diff_regular_working_one_table` | 27.37ms | 33.00ms | 82.9% | 1.6% | PASS |
| `diff_regular_working_many_tables` | 39.41ms | 50.00ms | 78.8% | 1.4% | PASS |
| `diff_stat_working_many_tables` | 39.73ms | 48.00ms | 82.8% | 1.5% | PASS |
| `diff_schema_working_many_tables` | 40.07ms | 48.00ms | 83.5% | 1.4% | PASS |
| `branch_list_many_branches` | 19.60ms | 25.00ms | 78.4% | 1.3% | PASS |
| `branch_create_delete` | 21.25ms | 27.00ms | 78.7% | 1.5% | PASS |
| `at_literal_deep_history` | 21.17ms | 28.00ms | 75.6% | 0.9% | PASS |
| `diff_literal_deep_history` | 21.01ms | 28.00ms | 75.0% | 1.1% | PASS |
| `history_literal_deep_history` | 21.66ms | 30.00ms | 72.2% | 1.3% | PASS |
| `checkout_branch_clean` | 30.56ms | 43.00ms | 71.1% | 1.4% | PASS |
| `merge_data_no_conflicts` | 23.16ms | 33.00ms | 70.2% | 1.7% | PASS |
| `merge_data_secondary_index` | 808.63ms | 944.00ms | 85.7% | 1.3% | PASS |
| `merge_schema_no_conflicts` | 19.02ms | 24.00ms | 79.3% | 2.0% | PASS |
| `merge_data_conflicts` | 26.20ms | 33.00ms | 79.4% | 1.9% | PASS |
| `merge_data_conflicts_with_resolve` | 27.04ms | 34.00ms | 79.5% | 1.9% | PASS |

Version-control ceiling result: **PASS**.

## File remote performance

Status: passed. Transport: local file-backed remote.

100,000,000 logical payload bytes; 100,000 rows; 1,000 changed rows; 5 repetitions per operation and binary.

SQL timings exclude fixture generation, copies, database open and verification. Each SQL sample uses a fresh process with the OS cache primed by setup verification. SQL client cache: 65,536 KiB. copy_fsync times shutil.copyfile plus fsync.

Throughput uses logical payload bytes; destination growth is file-size change, not transferred bytes.

Nightly median ceilings require three-attempt confirmation; copy_fsync is a control.

| Operation | Ceiling ms | Result |
|---|---:|---|
| push_initial | 2000 | PASS |
| clone_initial | 3000 | PASS |
| push_noop | 10 | PASS |
| fetch_noop | 10 | PASS |
| push_incremental | 750 | PASS |
| fetch_incremental | 300 | PASS |
| pull_fast_forward | 300 | PASS |

Timing gate: PASS after 1 attempt(s).

candidate: 39f0fa0.

Fixture file bytes: base 107,968,301; delta 115,879,032.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 488.161 | 409.983–537.642 | 36.665 | 204.85 | 107968301 |
| push_initial | candidate | 777.697 | 773.783–798.266 | 406.878 | 128.58 | 107956807 |
| clone_initial | candidate | 1057.011 | 1050.233–1068.854 | 533.024 | 94.61 | 108475105 |
| push_noop | candidate | 0.634 | 0.548–0.697 | 0.208 | — | 0 |
| fetch_noop | candidate | 0.125 | 0.116–0.137 | 0.126 | — | 0 |
| push_incremental | candidate | 189.889 | 184.198–202.481 | 172.415 | 5.27 | 7909775 |
| fetch_incremental | candidate | 91.417 | 85.292–94.519 | 71.985 | 10.94 | 7910084 |
| pull_fast_forward | candidate | 83.086 | 78.568–84.603 | 63.885 | 12.04 | 7910844 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 90.66 | 175.487 | 108.763 | 18329 | 2066 |
| clone_initial | candidate | 91.04 | 295.204 | 109.784 | 47271 | 2337 |
| push_noop | candidate | 43.09 | 0.073 | 0.000 | 26 | 2 |
| fetch_noop | candidate | 43.09 | 0.135 | 0.000 | 40 | 0 |
| push_incremental | candidate | 43.09 | 96.926 | 7.910 | 22425 | 125 |
| fetch_incremental | candidate | 43.09 | 34.472 | 7.910 | 10808 | 124 |
| pull_fast_forward | candidate | 43.09 | 26.497 | 7.911 | 7031 | 126 |

</details>


<details>
<summary>1 GB file remotes (observational)</summary>

Both database sizes use a fixed 1 MB scattered update and a warm OS cache. 1 GB timings have no ceiling while a baseline is collected.

### 1 GB file remote performance

Status: passed. Transport: local file-backed remote.

1,000,000,000 logical payload bytes; 1,000,000 rows; 1,000 changed rows; 3 repetitions per operation and binary.

SQL timings exclude fixture generation, copies, database open and verification. Each SQL sample uses a fresh process with the OS cache primed by setup verification. SQL client cache: 65,536 KiB. copy_fsync times shutil.copyfile plus fsync.

Throughput uses logical payload bytes; destination growth is file-size change, not transferred bytes.

No speed thresholds are enforced.

candidate: 39f0fa0.

Fixture file bytes: base 1,087,718,066; delta 1,103,525,194.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 5689.334 | 5677.411–5763.394 | 301.617 | 175.77 | 1087718066 |
| push_initial | candidate | 7544.891 | 7485.126–7590.842 | 3873.997 | 132.54 | 1079566310 |
| clone_initial | candidate | 10635.184 | 9718.094–10786.739 | 5571.767 | 94.03 | 1102407396 |
| push_noop | candidate | 0.668 | 0.641–0.700 | 0.248 | — | 0 |
| fetch_noop | candidate | 0.146 | 0.131–0.150 | 0.149 | — | 0 |
| push_incremental | candidate | 899.397 | 889.351–907.535 | 828.318 | 1.11 | 15806003 |
| fetch_incremental | candidate | 224.075 | 217.951–259.633 | 175.268 | 4.46 | 15806513 |
| pull_fast_forward | candidate | 247.054 | 244.550–266.831 | 173.116 | 4.05 | 15807273 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 134.56 | 2091.527 | 1087.622 | 263412 | 20541 |
| clone_initial | candidate | 124.16 | 3522.194 | 1132.819 | 613414 | 32243 |
| push_noop | candidate | 45.68 | 0.078 | 0.000 | 27 | 2 |
| fetch_noop | candidate | 45.68 | 0.137 | 0.000 | 40 | 0 |
| push_incremental | candidate | 48.36 | 618.801 | 15.928 | 150305 | 313 |
| fetch_incremental | candidate | 50.48 | 62.396 | 15.928 | 13935 | 313 |
| pull_fast_forward | candidate | 50.59 | 62.329 | 15.929 | 13931 | 315 |

</details>


</details>

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh`, `test/vc_perf_ceiling.sh`, and `test/remote_perf.py`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
