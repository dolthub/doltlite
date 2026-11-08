# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-11-08 11:30 UTC
>
> Commit: [`0ac59cb6a3b60dde9d22406fd55ee797745d6ebc`](https://github.com/dolthub/doltlite/commit/0ac59cb6a3b60dde9d22406fd55ee797745d6ebc)
>
> Runner: ubuntu24 20261101.360.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/42467584108)

The SQL workloads compare optimized DoltLite against stock SQLite on the same GitHub-hosted runner, alternating execution order on each repetition. Version-control sections measure DoltLite latency. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.68s | 11.74s | 1.2× | 1.2% | **PASS** |
| Writes | 2.10s | 3.57s | 1.7× | 1.2% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.52s | 12.00s | 1.1× | 1.3% | **PASS** |
| Writes | 3.49s | 4.29s | 1.2× | 1.9% | **PASS** |
| Autocommit writes | 735.51ms | 2.43s | 3.3× | 6.9% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.47s | 2.89s | 1.2× | 1.5% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.60s | 3.17s | 1.2× | 1.2% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.09s | 2.48s | 1.2× | 1.0% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.52s | 3.20s | 1.3× | 1.5% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 445.24ms | 806.32ms | 1.8× | 1.3% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 596.11ms | 1.03s | 1.7× | 1.3% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 462.72ms | 729.92ms | 1.6× | 0.8% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 596.40ms | 1.01s | 1.7× | 1.3% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.59s | 2.93s | 1.1× | 1.5% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.82s | 3.24s | 1.1× | 1.3% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.30s | 2.55s | 1.1× | 0.8% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.81s | 3.28s | 1.2× | 1.3% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 593.67ms | 856.75ms | 1.4× | 1.6% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 938.67ms | 1.14s | 1.2× | 3.5% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 1.17s | 1.19s | 1.0× | 5.4% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 788.39ms | 1.10s | 1.4× | 1.6% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.45s | 2.92s | 1.2× | 1.4% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.68s | 3.24s | 1.2× | 1.4% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.15s | 2.52s | 1.2× | 1.0% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.70s | 3.29s | 1.2× | 1.6% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 181.28ms | 609.63ms | 3.4× | 7.0% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 196.04ms | 643.26ms | 3.3× | 6.6% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 171.05ms | 565.87ms | 3.3× | 18.2% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 187.15ms | 615.84ms | 3.3× | 5.7% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 23.34ms | 33.51ms | 1.4× | 1.2% | PASS |
| mem_reads | `oltp_range_select` | 9.70ms | 13.04ms | 1.3× | 2.0% | PASS |
| mem_reads | `oltp_sum_range` | 8.95ms | 12.56ms | 1.4× | 1.5% | PASS |
| mem_reads | `oltp_order_range` | 2.51ms | 3.05ms | 1.2× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 3.60ms | 4.22ms | 1.2× | 1.4% | PASS |
| mem_reads | `oltp_index_scan` | 3.78ms | 5.65ms | 1.5× | 1.9% | PASS |
| mem_reads | `select_random_points` | 12.30ms | 11.96ms | 1.0× | 2.1% | PASS |
| mem_reads | `select_random_ranges` | 4.53ms | 5.64ms | 1.2× | 1.7% | PASS |
| mem_reads | `covering_index_scan` | 7.63ms | 10.83ms | 1.4× | 0.9% | PASS |
| mem_reads | `groupby_scan` | 30.03ms | 33.20ms | 1.1× | 0.8% | PASS |
| mem_reads | `index_join` | 5.74ms | 8.84ms | 1.5× | 1.5% | PASS |
| mem_reads | `index_join_scan` | 3.17ms | 5.99ms | 1.9× | 1.9% | PASS |
| mem_reads | `types_table_scan` | 1.04s | 1.25s | 1.2× | 1.3% | PASS |
| mem_reads | `table_scan` | 1.21s | 1.36s | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_read_only` | 105.31ms | 131.61ms | 1.2× | 1.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 182.28ms | 321.87ms | 1.8× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 15.38ms | 27.84ms | 1.8× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 50.20ms | 100.88ms | 2.0× | 1.3% | PASS |
| mem_writes | `oltp_update_non_index` | 35.32ms | 67.93ms | 1.9× | 1.3% | PASS |
| mem_writes | `oltp_delete_insert` | 45.60ms | 79.86ms | 1.8× | 1.2% | PASS |
| mem_writes | `oltp_write_only` | 22.33ms | 49.11ms | 2.2× | 1.4% | PASS |
| mem_writes | `types_delete_insert` | 24.79ms | 40.89ms | 1.6× | 1.7% | PASS |
| mem_writes | `oltp_read_write` | 69.34ms | 117.92ms | 1.7× | 2.1% | PASS |
| file_reads | `oltp_point_select` | 89.09ms | 50.80ms | 0.6× | 1.1% | PASS |
| file_reads | `oltp_range_select` | 17.09ms | 15.08ms | 0.9× | 2.1% | PASS |
| file_reads | `oltp_sum_range` | 16.54ms | 14.77ms | 0.9× | 1.5% | PASS |
| file_reads | `oltp_order_range` | 3.41ms | 3.34ms | 1.0× | 1.5% | PASS |
| file_reads | `oltp_distinct_range` | 4.52ms | 4.52ms | 1.0× | 1.7% | PASS |
| file_reads | `oltp_index_scan` | 10.55ms | 7.83ms | 0.7× | 1.4% | PASS |
| file_reads | `select_random_points` | 20.36ms | 14.23ms | 0.7× | 1.9% | PASS |
| file_reads | `select_random_ranges` | 11.30ms | 7.56ms | 0.7× | 1.7% | PASS |
| file_reads | `covering_index_scan` | 14.62ms | 13.02ms | 0.9× | 1.0% | PASS |
| file_reads | `groupby_scan` | 30.99ms | 33.74ms | 1.1× | 1.0% | PASS |
| file_reads | `index_join` | 9.59ms | 10.64ms | 1.1× | 1.6% | PASS |
| file_reads | `index_join_scan` | 4.04ms | 6.32ms | 1.6× | 2.5% | PASS |
| file_reads | `types_table_scan` | 1.02s | 1.24s | 1.2× | 0.7% | PASS |
| file_reads | `table_scan` | 1.14s | 1.35s | 1.2× | 1.0% | PASS |
| file_reads | `oltp_read_only` | 197.78ms | 155.65ms | 0.8× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 197.28ms | 331.62ms | 1.7× | 1.1% | PASS |
| file_writes | `oltp_insert` | 22.01ms | 30.94ms | 1.4× | 1.5% | PASS |
| file_writes | `oltp_update_index` | 76.51ms | 108.57ms | 1.4× | 1.7% | PASS |
| file_writes | `oltp_update_non_index` | 58.10ms | 73.78ms | 1.3× | 2.6% | PASS |
| file_writes | `oltp_delete_insert` | 66.65ms | 89.93ms | 1.3× | 1.3% | PASS |
| file_writes | `oltp_write_only` | 44.30ms | 53.84ms | 1.2× | 2.9% | PASS |
| file_writes | `types_delete_insert` | 39.35ms | 46.85ms | 1.2× | 2.2% | PASS |
| file_writes | `oltp_read_write` | 89.46ms | 121.23ms | 1.4× | 1.5% | PASS |
| ac_reads | `oltp_point_select` | 45.29ms | 50.58ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 12.70ms | 15.05ms | 1.2× | 1.7% | PASS |
| ac_reads | `oltp_sum_range` | 11.74ms | 14.69ms | 1.3× | 1.5% | PASS |
| ac_reads | `oltp_order_range` | 2.91ms | 3.34ms | 1.1× | 1.5% | PASS |
| ac_reads | `oltp_distinct_range` | 4.00ms | 4.52ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 6.33ms | 7.82ms | 1.2× | 1.8% | PASS |
| ac_reads | `select_random_points` | 15.36ms | 14.13ms | 0.9× | 1.6% | PASS |
| ac_reads | `select_random_ranges` | 6.94ms | 7.55ms | 1.1× | 1.4% | PASS |
| ac_reads | `covering_index_scan` | 10.24ms | 13.07ms | 1.3× | 1.2% | PASS |
| ac_reads | `groupby_scan` | 30.45ms | 33.70ms | 1.1× | 1.0% | PASS |
| ac_reads | `index_join` | 7.26ms | 10.55ms | 1.5× | 2.0% | PASS |
| ac_reads | `index_join_scan` | 3.58ms | 6.33ms | 1.8× | 1.8% | PASS |
| ac_reads | `types_table_scan` | 1.02s | 1.24s | 1.2× | 0.7% | PASS |
| ac_reads | `table_scan` | 1.14s | 1.35s | 1.2× | 0.8% | PASS |
| ac_reads | `oltp_read_only` | 135.47ms | 156.17ms | 1.2× | 1.4% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 19.34ms | 63.53ms | 3.3× | 6.9% | PASS |
| ac_writes | `oltp_insert_ac` | 22.14ms | 76.43ms | 3.5× | 5.6% | PASS |
| ac_writes | `oltp_update_index_ac` | 24.18ms | 87.20ms | 3.6× | 4.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 20.31ms | 69.78ms | 3.4× | 7.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 22.75ms | 78.99ms | 3.5× | 6.8% | PASS |
| ac_writes | `oltp_write_only_ac` | 23.66ms | 78.20ms | 3.3× | 8.5% | PASS |
| ac_writes | `types_delete_insert_ac` | 20.44ms | 70.93ms | 3.5× | 7.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 28.46ms | 84.58ms | 3.0× | 7.1% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 35.03ms | 43.69ms | 1.2× | 2.3% | PASS |
| mem_reads | `oltp_range_select` | 16.33ms | 17.15ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_sum_range` | 14.79ms | 16.66ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_order_range` | 3.23ms | 3.73ms | 1.2× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 4.38ms | 4.89ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 3.99ms | 7.26ms | 1.8× | 1.1% | PASS |
| mem_reads | `select_random_points` | 21.24ms | 24.39ms | 1.1× | 2.0% | PASS |
| mem_reads | `select_random_ranges` | 6.64ms | 7.41ms | 1.1× | 1.2% | PASS |
| mem_reads | `covering_index_scan` | 7.75ms | 11.57ms | 1.5× | 1.3% | PASS |
| mem_reads | `groupby_scan` | 34.34ms | 36.00ms | 1.0× | 1.0% | PASS |
| mem_reads | `index_join` | 10.15ms | 11.19ms | 1.1× | 1.7% | PASS |
| mem_reads | `index_join_scan` | 3.67ms | 6.85ms | 1.9× | 1.9% | PASS |
| mem_reads | `types_table_scan` | 1.07s | 1.36s | 1.3× | 1.0% | PASS |
| mem_reads | `table_scan` | 1.23s | 1.46s | 1.2× | 1.0% | PASS |
| mem_reads | `oltp_read_only` | 140.54ms | 159.24ms | 1.1× | 1.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 245.60ms | 379.75ms | 1.5× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 18.03ms | 37.31ms | 2.1× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 65.22ms | 143.80ms | 2.2× | 1.5% | PASS |
| mem_writes | `oltp_update_non_index` | 49.16ms | 88.00ms | 1.8× | 1.7% | PASS |
| mem_writes | `oltp_delete_insert` | 54.04ms | 107.02ms | 2.0× | 1.3% | PASS |
| mem_writes | `oltp_write_only` | 27.80ms | 55.35ms | 2.0× | 1.3% | PASS |
| mem_writes | `types_delete_insert` | 38.56ms | 67.17ms | 1.7× | 1.0% | PASS |
| mem_writes | `oltp_read_write` | 97.70ms | 146.63ms | 1.5× | 1.4% | PASS |
| file_reads | `oltp_point_select` | 101.19ms | 62.11ms | 0.6× | 1.3% | PASS |
| file_reads | `oltp_range_select` | 23.74ms | 19.22ms | 0.8× | 1.6% | PASS |
| file_reads | `oltp_sum_range` | 22.05ms | 18.81ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_order_range` | 4.15ms | 3.96ms | 1.0× | 1.7% | PASS |
| file_reads | `oltp_distinct_range` | 5.31ms | 5.16ms | 1.0× | 1.2% | PASS |
| file_reads | `oltp_index_scan` | 10.85ms | 9.21ms | 0.8× | 1.3% | PASS |
| file_reads | `select_random_points` | 29.81ms | 27.55ms | 0.9× | 1.8% | PASS |
| file_reads | `select_random_ranges` | 13.77ms | 9.46ms | 0.7× | 1.6% | PASS |
| file_reads | `covering_index_scan` | 14.94ms | 13.66ms | 0.9× | 1.3% | PASS |
| file_reads | `groupby_scan` | 35.36ms | 36.46ms | 1.0× | 1.0% | PASS |
| file_reads | `index_join` | 14.43ms | 12.58ms | 0.9× | 1.8% | PASS |
| file_reads | `index_join_scan` | 4.63ms | 7.19ms | 1.6× | 1.4% | PASS |
| file_reads | `types_table_scan` | 1.06s | 1.36s | 1.3× | 1.0% | PASS |
| file_reads | `table_scan` | 1.24s | 1.47s | 1.2× | 1.1% | PASS |
| file_reads | `oltp_read_only` | 239.74ms | 189.06ms | 0.8× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 268.97ms | 396.66ms | 1.5× | 0.8% | PASS |
| file_writes | `oltp_insert` | 25.88ms | 43.12ms | 1.7× | 1.8% | PASS |
| file_writes | `oltp_update_index` | 135.75ms | 166.08ms | 1.2× | 5.2% | PASS |
| file_writes | `oltp_update_non_index` | 97.13ms | 102.91ms | 1.1× | 15.3% | PASS |
| file_writes | `oltp_delete_insert` | 95.75ms | 124.09ms | 1.3× | 1.7% | PASS |
| file_writes | `oltp_write_only` | 88.01ms | 68.81ms | 0.8× | 14.1% | PASS |
| file_writes | `types_delete_insert` | 72.53ms | 77.55ms | 1.1× | 1.8% | PASS |
| file_writes | `oltp_read_write` | 154.64ms | 161.19ms | 1.0× | 8.2% | PASS |
| ac_reads | `oltp_point_select` | 58.25ms | 62.23ms | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_range_select` | 19.50ms | 19.28ms | 1.0× | 2.4% | PASS |
| ac_reads | `oltp_sum_range` | 17.71ms | 19.08ms | 1.1× | 1.7% | PASS |
| ac_reads | `oltp_order_range` | 3.81ms | 3.99ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_distinct_range` | 4.84ms | 5.17ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_index_scan` | 6.60ms | 9.30ms | 1.4× | 1.7% | PASS |
| ac_reads | `select_random_points` | 25.41ms | 27.46ms | 1.1× | 2.7% | PASS |
| ac_reads | `select_random_ranges` | 9.27ms | 9.48ms | 1.0× | 1.6% | PASS |
| ac_reads | `covering_index_scan` | 10.42ms | 13.65ms | 1.3× | 1.2% | PASS |
| ac_reads | `groupby_scan` | 34.88ms | 36.37ms | 1.0× | 0.6% | PASS |
| ac_reads | `index_join` | 12.31ms | 12.59ms | 1.0× | 2.0% | PASS |
| ac_reads | `index_join_scan` | 4.20ms | 7.21ms | 1.7× | 1.6% | PASS |
| ac_reads | `types_table_scan` | 1.07s | 1.36s | 1.3× | 1.2% | PASS |
| ac_reads | `table_scan` | 1.23s | 1.46s | 1.2× | 1.1% | PASS |
| ac_reads | `oltp_read_only` | 174.50ms | 188.87ms | 1.1× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.31ms | 63.71ms | 3.0× | 4.9% | PASS |
| ac_writes | `oltp_insert_ac` | 24.93ms | 75.72ms | 3.0× | 4.8% | PASS |
| ac_writes | `oltp_update_index_ac` | 25.80ms | 93.10ms | 3.6× | 6.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.71ms | 74.67ms | 3.4× | 8.9% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.59ms | 87.36ms | 3.6× | 7.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 24.18ms | 82.24ms | 3.4× | 6.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.37ms | 74.98ms | 3.4× | 6.6% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.16ms | 91.49ms | 2.9× | 5.3% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 27.09ms | 29.92ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_range_select` | 13.05ms | 12.57ms | 1.0× | 1.0% | PASS |
| mem_reads | `oltp_sum_range` | 11.81ms | 12.07ms | 1.0× | 0.8% | PASS |
| mem_reads | `oltp_order_range` | 2.69ms | 2.83ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 3.55ms | 3.65ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 3.13ms | 5.24ms | 1.7× | 1.2% | PASS |
| mem_reads | `select_random_points` | 16.24ms | 16.86ms | 1.0× | 0.6% | PASS |
| mem_reads | `select_random_ranges` | 5.13ms | 5.25ms | 1.0× | 0.8% | PASS |
| mem_reads | `covering_index_scan` | 5.99ms | 8.23ms | 1.4× | 0.9% | PASS |
| mem_reads | `groupby_scan` | 28.44ms | 29.32ms | 1.0× | 0.6% | PASS |
| mem_reads | `index_join` | 8.31ms | 8.58ms | 1.0× | 1.0% | PASS |
| mem_reads | `index_join_scan` | 2.98ms | 5.54ms | 1.9× | 1.3% | PASS |
| mem_reads | `types_table_scan` | 858.08ms | 1.06s | 1.2× | 0.5% | PASS |
| mem_reads | `table_scan` | 989.63ms | 1.16s | 1.2× | 1.1% | PASS |
| mem_reads | `oltp_read_only` | 112.08ms | 112.61ms | 1.0× | 1.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 188.12ms | 273.01ms | 1.5× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 14.60ms | 27.71ms | 1.9× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 50.42ms | 101.62ms | 2.0× | 0.5% | PASS |
| mem_writes | `oltp_update_non_index` | 39.67ms | 60.53ms | 1.5× | 1.6% | PASS |
| mem_writes | `oltp_delete_insert` | 43.10ms | 78.04ms | 1.8× | 0.7% | PASS |
| mem_writes | `oltp_write_only` | 23.40ms | 41.62ms | 1.8× | 0.9% | PASS |
| mem_writes | `types_delete_insert` | 31.08ms | 46.99ms | 1.5× | 1.1% | PASS |
| mem_writes | `oltp_read_write` | 72.33ms | 100.38ms | 1.4× | 0.8% | PASS |
| file_reads | `oltp_point_select` | 88.00ms | 46.49ms | 0.5× | 0.5% | PASS |
| file_reads | `oltp_range_select` | 19.95ms | 14.32ms | 0.7× | 0.8% | PASS |
| file_reads | `oltp_sum_range` | 18.66ms | 13.81ms | 0.7× | 0.7% | PASS |
| file_reads | `oltp_order_range` | 3.49ms | 3.06ms | 0.9× | 1.0% | PASS |
| file_reads | `oltp_distinct_range` | 4.34ms | 3.87ms | 0.9× | 1.0% | PASS |
| file_reads | `oltp_index_scan` | 9.58ms | 7.07ms | 0.7× | 0.5% | PASS |
| file_reads | `select_random_points` | 23.67ms | 18.74ms | 0.8× | 0.8% | PASS |
| file_reads | `select_random_ranges` | 11.70ms | 7.06ms | 0.6× | 0.9% | PASS |
| file_reads | `covering_index_scan` | 12.44ms | 10.07ms | 0.8× | 0.8% | PASS |
| file_reads | `groupby_scan` | 29.39ms | 29.57ms | 1.0× | 0.8% | PASS |
| file_reads | `index_join` | 12.35ms | 9.81ms | 0.8× | 1.3% | PASS |
| file_reads | `index_join_scan` | 3.91ms | 5.87ms | 1.5× | 1.3% | PASS |
| file_reads | `types_table_scan` | 900.86ms | 1.09s | 1.2× | 0.8% | PASS |
| file_reads | `table_scan` | 971.10ms | 1.15s | 1.2× | 1.3% | PASS |
| file_reads | `oltp_read_only` | 194.50ms | 135.06ms | 0.7× | 0.6% | PASS |
| file_writes | `oltp_bulk_insert` | 256.96ms | 344.32ms | 1.3× | 5.2% | PASS |
| file_writes | `oltp_insert` | 31.35ms | 48.91ms | 1.6× | 5.7% | PASS |
| file_writes | `oltp_update_index` | 162.73ms | 188.79ms | 1.2× | 15.0% | PASS |
| file_writes | `oltp_update_non_index` | 146.38ms | 119.27ms | 0.8× | 9.1% | PASS |
| file_writes | `oltp_delete_insert` | 163.17ms | 145.79ms | 0.9× | 2.5% | PASS |
| file_writes | `oltp_write_only` | 116.80ms | 96.03ms | 0.8× | 3.3% | PASS |
| file_writes | `types_delete_insert` | 122.60ms | 88.73ms | 0.7× | 6.2% | PASS |
| file_writes | `oltp_read_write` | 168.62ms | 156.81ms | 0.9× | 3.6% | PASS |
| ac_reads | `oltp_point_select` | 48.28ms | 46.49ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 16.47ms | 14.52ms | 0.9× | 1.1% | PASS |
| ac_reads | `oltp_sum_range` | 14.88ms | 13.93ms | 0.9× | 1.0% | PASS |
| ac_reads | `oltp_order_range` | 3.14ms | 3.06ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_distinct_range` | 3.93ms | 3.89ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_index_scan` | 5.65ms | 7.11ms | 1.3× | 0.7% | PASS |
| ac_reads | `select_random_points` | 19.50ms | 18.77ms | 1.0× | 0.7% | PASS |
| ac_reads | `select_random_ranges` | 7.67ms | 7.03ms | 0.9× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 8.45ms | 10.06ms | 1.2× | 0.5% | PASS |
| ac_reads | `groupby_scan` | 29.16ms | 29.67ms | 1.0× | 0.7% | PASS |
| ac_reads | `index_join` | 10.46ms | 9.83ms | 0.9× | 1.3% | PASS |
| ac_reads | `index_join_scan` | 3.56ms | 5.92ms | 1.7× | 2.2% | PASS |
| ac_reads | `types_table_scan` | 859.02ms | 1.06s | 1.2× | 0.7% | PASS |
| ac_reads | `table_scan` | 968.62ms | 1.15s | 1.2× | 1.4% | PASS |
| ac_reads | `oltp_read_only` | 148.67ms | 140.84ms | 0.9× | 1.1% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.25ms | 60.70ms | 2.7× | 45.2% | PASS |
| ac_writes | `oltp_insert_ac` | 20.34ms | 66.99ms | 3.3× | 9.1% | PASS |
| ac_writes | `oltp_update_index_ac` | 21.76ms | 82.11ms | 3.8× | 17.5% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 20.87ms | 70.52ms | 3.4× | 33.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 21.00ms | 72.45ms | 3.4× | 16.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 20.56ms | 73.42ms | 3.6× | 18.8% | PASS |
| ac_writes | `types_delete_insert_ac` | 18.37ms | 62.03ms | 3.4× | 14.2% | PASS |
| ac_writes | `oltp_read_write_ac` | 25.90ms | 77.65ms | 3.0× | 24.5% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 34.55ms | 45.18ms | 1.3× | 2.2% | PASS |
| mem_reads | `oltp_range_select` | 20.28ms | 24.21ms | 1.2× | 2.0% | PASS |
| mem_reads | `oltp_sum_range` | 18.20ms | 22.99ms | 1.3× | 1.5% | PASS |
| mem_reads | `oltp_order_range` | 3.71ms | 4.40ms | 1.2× | 1.0% | PASS |
| mem_reads | `oltp_distinct_range` | 4.80ms | 5.50ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_index_scan` | 4.44ms | 6.66ms | 1.5× | 2.4% | PASS |
| mem_reads | `select_random_points` | 27.47ms | 34.03ms | 1.2× | 2.0% | PASS |
| mem_reads | `select_random_ranges` | 7.80ms | 9.60ms | 1.2× | 1.4% | PASS |
| mem_reads | `covering_index_scan` | 7.71ms | 11.13ms | 1.4× | 1.2% | PASS |
| mem_reads | `groupby_scan` | 36.50ms | 40.94ms | 1.1× | 0.9% | PASS |
| mem_reads | `index_join` | 7.91ms | 11.74ms | 1.5× | 2.0% | PASS |
| mem_reads | `index_join_scan` | 3.92ms | 6.63ms | 1.7× | 2.4% | PASS |
| mem_reads | `types_table_scan` | 1.03s | 1.35s | 1.3× | 0.9% | PASS |
| mem_reads | `table_scan` | 1.16s | 1.44s | 1.2× | 0.7% | PASS |
| mem_reads | `oltp_read_only` | 153.48ms | 190.38ms | 1.2× | 1.7% | PASS |
| mem_writes | `oltp_bulk_insert` | 247.96ms | 372.29ms | 1.5× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 18.99ms | 34.91ms | 1.8× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 66.67ms | 129.29ms | 1.9× | 1.3% | PASS |
| mem_writes | `oltp_update_non_index` | 51.24ms | 93.08ms | 1.8× | 1.8% | PASS |
| mem_writes | `oltp_delete_insert` | 48.77ms | 97.45ms | 2.0× | 1.2% | PASS |
| mem_writes | `oltp_write_only` | 27.08ms | 58.78ms | 2.2× | 1.3% | PASS |
| mem_writes | `types_delete_insert` | 33.08ms | 61.65ms | 1.9× | 1.4% | PASS |
| mem_writes | `oltp_read_write` | 102.63ms | 166.05ms | 1.6× | 1.9% | PASS |
| file_reads | `oltp_point_select` | 99.86ms | 62.25ms | 0.6× | 1.2% | PASS |
| file_reads | `oltp_range_select` | 27.73ms | 26.45ms | 1.0× | 1.7% | PASS |
| file_reads | `oltp_sum_range` | 25.86ms | 25.31ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_order_range` | 4.59ms | 4.69ms | 1.0× | 1.1% | PASS |
| file_reads | `oltp_distinct_range` | 5.78ms | 5.87ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 11.79ms | 9.02ms | 0.8× | 1.1% | PASS |
| file_reads | `select_random_points` | 37.73ms | 38.12ms | 1.0× | 1.6% | PASS |
| file_reads | `select_random_ranges` | 14.94ms | 11.78ms | 0.8× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 15.04ms | 13.46ms | 0.9× | 1.0% | PASS |
| file_reads | `groupby_scan` | 37.66ms | 41.45ms | 1.1× | 1.0% | PASS |
| file_reads | `index_join` | 12.21ms | 13.69ms | 1.1× | 2.1% | PASS |
| file_reads | `index_join_scan` | 4.96ms | 7.05ms | 1.4× | 1.9% | PASS |
| file_reads | `types_table_scan` | 1.08s | 1.36s | 1.3× | 1.6% | PASS |
| file_reads | `table_scan` | 1.17s | 1.44s | 1.2× | 0.9% | PASS |
| file_reads | `oltp_read_only` | 254.09ms | 217.29ms | 0.9× | 1.4% | PASS |
| file_writes | `oltp_bulk_insert` | 262.89ms | 384.65ms | 1.5× | 0.9% | PASS |
| file_writes | `oltp_insert` | 26.28ms | 39.27ms | 1.5× | 1.7% | PASS |
| file_writes | `oltp_update_index` | 100.32ms | 149.73ms | 1.5× | 1.4% | PASS |
| file_writes | `oltp_update_non_index` | 81.36ms | 103.77ms | 1.3× | 1.2% | PASS |
| file_writes | `oltp_delete_insert` | 80.12ms | 113.69ms | 1.4× | 1.5% | PASS |
| file_writes | `oltp_write_only` | 52.06ms | 66.49ms | 1.3× | 1.9% | PASS |
| file_writes | `types_delete_insert` | 51.75ms | 68.08ms | 1.3× | 1.7% | PASS |
| file_writes | `oltp_read_write` | 133.60ms | 176.73ms | 1.3× | 1.8% | PASS |
| ac_reads | `oltp_point_select` | 56.84ms | 62.92ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 23.11ms | 26.60ms | 1.2× | 1.7% | PASS |
| ac_reads | `oltp_sum_range` | 20.93ms | 25.33ms | 1.2× | 1.2% | PASS |
| ac_reads | `oltp_order_range` | 4.11ms | 4.70ms | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_distinct_range` | 5.18ms | 5.86ms | 1.1× | 1.0% | PASS |
| ac_reads | `oltp_index_scan` | 7.10ms | 8.96ms | 1.3× | 1.3% | PASS |
| ac_reads | `select_random_points` | 30.75ms | 37.70ms | 1.2× | 2.2% | PASS |
| ac_reads | `select_random_ranges` | 10.27ms | 11.73ms | 1.1× | 1.8% | PASS |
| ac_reads | `covering_index_scan` | 10.31ms | 13.49ms | 1.3× | 1.6% | PASS |
| ac_reads | `groupby_scan` | 37.14ms | 41.57ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 9.70ms | 13.58ms | 1.4× | 2.0% | PASS |
| ac_reads | `index_join_scan` | 4.53ms | 7.05ms | 1.6× | 1.5% | PASS |
| ac_reads | `types_table_scan` | 1.10s | 1.37s | 1.2× | 1.7% | PASS |
| ac_reads | `table_scan` | 1.20s | 1.45s | 1.2× | 2.2% | PASS |
| ac_reads | `oltp_read_only` | 189.39ms | 219.01ms | 1.2× | 1.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 20.79ms | 62.84ms | 3.0× | 5.6% | PASS |
| ac_writes | `oltp_insert_ac` | 22.66ms | 78.27ms | 3.5× | 5.8% | PASS |
| ac_writes | `oltp_update_index_ac` | 25.09ms | 87.25ms | 3.5× | 5.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.87ms | 69.98ms | 3.2× | 5.9% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 22.38ms | 79.57ms | 3.6× | 6.8% | PASS |
| ac_writes | `oltp_write_only_ac` | 22.90ms | 79.65ms | 3.5× | 5.4% | PASS |
| ac_writes | `types_delete_insert_ac` | 20.96ms | 71.36ms | 3.4× | 8.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 30.49ms | 86.91ms | 2.9× | 5.2% | PASS |

</details>

</details>

## Version-control latency

Wall time: 5m 30s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 22.18ms | 38.00ms | 58.4% | 0.4% | PASS |
| `status_dirty_many_tables` | 24.26ms | 42.00ms | 57.8% | 0.4% | PASS |
| `diff_regular_working_one_table` | 19.57ms | 33.00ms | 59.3% | 1.0% | PASS |
| `diff_regular_working_many_tables` | 26.57ms | 50.00ms | 53.1% | 0.9% | PASS |
| `diff_stat_working_many_tables` | 26.40ms | 48.00ms | 55.0% | 0.6% | PASS |
| `diff_schema_working_many_tables` | 26.67ms | 48.00ms | 55.6% | 0.4% | PASS |
| `branch_list_many_branches` | 14.85ms | 25.00ms | 59.4% | 0.8% | PASS |
| `branch_create_delete` | 16.08ms | 27.00ms | 59.5% | 0.8% | PASS |
| `at_literal_deep_history` | 16.20ms | 28.00ms | 57.8% | 0.7% | PASS |
| `diff_literal_deep_history` | 16.24ms | 28.00ms | 58.0% | 1.0% | PASS |
| `history_literal_deep_history` | 16.98ms | 30.00ms | 56.6% | 0.9% | PASS |
| `checkout_branch_clean` | 21.63ms | 43.00ms | 50.3% | 0.6% | PASS |
| `merge_data_no_conflicts` | 18.33ms | 33.00ms | 55.5% | 0.6% | PASS |
| `merge_data_secondary_index` | 618.33ms | 944.00ms | 65.5% | 0.5% | PASS |
| `merge_schema_no_conflicts` | 13.94ms | 24.00ms | 58.1% | 0.8% | PASS |
| `merge_data_conflicts` | 20.41ms | 33.00ms | 61.9% | 0.8% | PASS |
| `merge_data_conflicts_with_resolve` | 20.98ms | 34.00ms | 61.7% | 0.7% | PASS |

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

candidate: 0ac59cb.

Fixture file bytes: base 107,968,301; delta 115,879,032.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 423.094 | 391.593–536.659 | 30.346 | 236.35 | 107968301 |
| push_initial | candidate | 722.766 | 717.739–728.559 | 360.198 | 138.36 | 107956807 |
| clone_initial | candidate | 973.821 | 957.556–985.930 | 466.528 | 102.69 | 108475105 |
| push_noop | candidate | 0.487 | 0.476–0.541 | 0.230 | — | 0 |
| fetch_noop | candidate | 0.141 | 0.133–0.144 | 0.143 | — | 0 |
| push_incremental | candidate | 163.472 | 163.153–180.559 | 146.384 | 6.12 | 7909775 |
| fetch_incremental | candidate | 79.124 | 75.046–81.026 | 61.462 | 12.64 | 7910084 |
| pull_fast_forward | candidate | 70.973 | 65.448–73.975 | 52.565 | 14.09 | 7910844 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 90.55 | 175.487 | 108.763 | 18329 | 2066 |
| clone_initial | candidate | 90.87 | 295.268 | 109.784 | 47286 | 2337 |
| push_noop | candidate | 42.76 | 0.073 | 0.000 | 26 | 2 |
| fetch_noop | candidate | 42.76 | 0.135 | 0.000 | 40 | 0 |
| push_incremental | candidate | 42.76 | 96.954 | 7.910 | 22431 | 125 |
| fetch_incremental | candidate | 42.76 | 34.472 | 7.910 | 10808 | 124 |
| pull_fast_forward | candidate | 42.76 | 26.497 | 7.911 | 7031 | 126 |

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

candidate: 0ac59cb.

Fixture file bytes: base 1,087,718,066; delta 1,103,525,194.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 5580.112 | 5523.082–5782.367 | 314.729 | 179.21 | 1087718066 |
| push_initial | candidate | 8102.807 | 8063.156–8187.335 | 4276.144 | 123.41 | 1079566310 |
| clone_initial | candidate | 11315.450 | 10440.824–11353.659 | 6240.046 | 88.37 | 1102407396 |
| push_noop | candidate | 0.717 | 0.631–15.982 | 0.280 | — | 0 |
| fetch_noop | candidate | 0.172 | 0.160–0.185 | 0.174 | — | 0 |
| push_incremental | candidate | 1039.927 | 1021.109–1046.142 | 963.153 | 0.96 | 15806003 |
| fetch_incremental | candidate | 400.346 | 284.996–430.836 | 222.220 | 2.50 | 15806513 |
| pull_fast_forward | candidate | 346.201 | 299.395–427.372 | 225.045 | 2.89 | 15807273 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 136.18 | 2090.868 | 1087.622 | 263251 | 20541 |
| clone_initial | candidate | 124.16 | 3517.162 | 1132.819 | 612119 | 32243 |
| push_noop | candidate | 45.66 | 0.078 | 0.000 | 27 | 2 |
| fetch_noop | candidate | 45.66 | 0.137 | 0.000 | 40 | 0 |
| push_incremental | candidate | 48.43 | 617.194 | 15.928 | 149896 | 313 |
| fetch_incremental | candidate | 50.49 | 61.685 | 15.928 | 13761 | 313 |
| pull_fast_forward | candidate | 50.56 | 61.617 | 15.929 | 13757 | 315 |

</details>


</details>

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh`, `test/vc_perf_ceiling.sh`, and `test/remote_perf.py`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
