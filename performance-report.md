# DoltLite Performance Report

> Nightly result: **FAIL**
>
> Generated: 2026-11-01 11:33 UTC
>
> Commit: [`89a2cdf5e26870ae1aa54f547d743e754c9bb742`](https://github.com/dolthub/doltlite/commit/89a2cdf5e26870ae1aa54f547d743e754c9bb742)
>
> Runner: ubuntu24 20261026.354.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/41293528208)

The SQL workloads compare optimized DoltLite against stock SQLite on the same GitHub-hosted runner, alternating execution order on each repetition. Version-control sections measure DoltLite latency. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 8.81s | 10.28s | 1.2× | 1.6% | **FAIL** |
| Writes | 1.77s | 3.12s | 1.8× | 1.3% | **FAIL** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.16s | 10.34s | 1.1× | 1.5% | **PASS** |
| Writes | 3.33s | 4.02s | 1.2× | 2.7% | **PASS** |
| Autocommit writes | 841.58ms | 2.54s | 3.0× | 5.8% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.40s | 2.84s | 1.2× | 1.2% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 1.98s | 2.08s | 1.0× | 3.4% | **FAIL** |
| In-memory | Reads | blobpk | 15 | 55 | 2.60s | 3.23s | 1.2× | 1.6% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 1.83s | 2.13s | 1.2× | 1.2% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 432.53ms | 823.64ms | 1.9× | 1.1% | **FAIL** |
| In-memory | Writes | textpk | 8 | 55 | 396.31ms | 644.09ms | 1.6× | 1.8% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 588.91ms | 1.04s | 1.8× | 1.7% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 357.14ms | 612.21ms | 1.7× | 1.2% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.61s | 2.90s | 1.1× | 1.2% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 1.93s | 2.01s | 1.0× | 1.8% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.73s | 3.27s | 1.2× | 1.7% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 1.89s | 2.16s | 1.1× | 1.4% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 585.55ms | 883.67ms | 1.5× | 1.4% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.11s | 1.08s | 1.0× | 5.0% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 805.04ms | 1.13s | 1.4× | 1.7% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 834.23ms | 922.76ms | 1.1× | 4.2% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.48s | 2.90s | 1.2× | 1.3% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 1.76s | 1.96s | 1.1× | 2.3% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.59s | 3.26s | 1.3× | 1.6% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 1.85s | 2.15s | 1.2× | 1.4% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 184.41ms | 604.22ms | 3.3× | 5.7% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 191.27ms | 537.31ms | 2.8× | 6.1% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 202.02ms | 639.17ms | 3.2× | 7.0% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 263.88ms | 764.09ms | 2.9× | 3.8% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 23.25ms | 33.46ms | 1.4× | 1.3% | PASS |
| mem_reads | `oltp_range_select` | 10.14ms | 13.20ms | 1.3× | 1.6% | PASS |
| mem_reads | `oltp_sum_range` | 9.41ms | 12.46ms | 1.3× | 1.3% | PASS |
| mem_reads | `oltp_order_range` | 2.57ms | 3.19ms | 1.2× | 0.9% | PASS |
| mem_reads | `oltp_distinct_range` | 3.60ms | 4.35ms | 1.2× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 3.81ms | 5.82ms | 1.5× | 1.7% | PASS |
| mem_reads | `select_random_points` | 9.62ms | 12.12ms | 1.3× | 2.0% | PASS |
| mem_reads | `select_random_ranges` | 4.56ms | 5.61ms | 1.2× | 1.2% | PASS |
| mem_reads | `covering_index_scan` | 7.55ms | 11.16ms | 1.5× | 1.1% | PASS |
| mem_reads | `groupby_scan` | 29.48ms | 34.58ms | 1.2× | 0.6% | PASS |
| mem_reads | `index_join` | 5.67ms | 8.95ms | 1.6× | 1.6% | PASS |
| mem_reads | `index_join_scan` | 3.13ms | 5.78ms | 1.8× | 1.6% | PASS |
| mem_reads | `types_table_scan` | 1.03s | 1.22s | 1.2× | 0.5% | PASS |
| mem_reads | `table_scan` | 1.16s | 1.33s | 1.2× | 0.4% | PASS |
| mem_reads | `oltp_read_only` | 100.94ms | 132.58ms | 1.3× | 1.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 178.86ms | 334.64ms | 1.9× | 1.2% | PASS |
| mem_writes | `oltp_insert` | 15.11ms | 28.78ms | 1.9× | 1.1% | PASS |
| mem_writes | `oltp_update_index` | 49.55ms | 102.33ms | 2.1× | 1.2% | PASS |
| mem_writes | `oltp_update_non_index` | 33.66ms | 66.80ms | 2.0× | 1.1% | PASS |
| mem_writes | `oltp_delete_insert` | 44.39ms | 81.91ms | 1.8× | 1.1% | PASS |
| mem_writes | `oltp_write_only` | 21.83ms | 50.46ms | 2.3× | 1.3% | FAIL |
| mem_writes | `types_delete_insert` | 24.20ms | 39.90ms | 1.6× | 1.2% | PASS |
| mem_writes | `oltp_read_write` | 64.92ms | 118.83ms | 1.8× | 1.0% | PASS |
| file_reads | `oltp_point_select` | 86.91ms | 50.37ms | 0.6× | 1.1% | PASS |
| file_reads | `oltp_range_select` | 16.34ms | 14.96ms | 0.9× | 1.9% | PASS |
| file_reads | `oltp_sum_range` | 15.77ms | 14.30ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_order_range` | 3.29ms | 3.44ms | 1.0× | 1.7% | PASS |
| file_reads | `oltp_distinct_range` | 4.34ms | 4.61ms | 1.1× | 1.2% | PASS |
| file_reads | `oltp_index_scan` | 10.30ms | 7.79ms | 0.8× | 1.2% | PASS |
| file_reads | `select_random_points` | 16.29ms | 14.14ms | 0.9× | 2.9% | PASS |
| file_reads | `select_random_ranges` | 11.12ms | 7.44ms | 0.7× | 1.5% | PASS |
| file_reads | `covering_index_scan` | 14.38ms | 13.09ms | 0.9× | 1.1% | PASS |
| file_reads | `groupby_scan` | 30.27ms | 34.87ms | 1.2× | 0.6% | PASS |
| file_reads | `index_join` | 9.31ms | 10.41ms | 1.1× | 1.4% | PASS |
| file_reads | `index_join_scan` | 3.98ms | 6.02ms | 1.5× | 1.5% | PASS |
| file_reads | `types_table_scan` | 1.03s | 1.22s | 1.2× | 0.5% | PASS |
| file_reads | `table_scan` | 1.16s | 1.33s | 1.1× | 0.5% | PASS |
| file_reads | `oltp_read_only` | 194.72ms | 158.63ms | 0.8× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 193.32ms | 344.87ms | 1.8× | 1.5% | PASS |
| file_writes | `oltp_insert` | 21.53ms | 32.15ms | 1.5× | 1.3% | PASS |
| file_writes | `oltp_update_index` | 75.51ms | 111.68ms | 1.5× | 1.0% | PASS |
| file_writes | `oltp_update_non_index` | 56.66ms | 73.69ms | 1.3× | 1.3% | PASS |
| file_writes | `oltp_delete_insert` | 67.09ms | 94.11ms | 1.4× | 1.4% | PASS |
| file_writes | `oltp_write_only` | 43.53ms | 56.19ms | 1.3× | 2.3% | PASS |
| file_writes | `types_delete_insert` | 38.72ms | 46.32ms | 1.2× | 1.5% | PASS |
| file_writes | `oltp_read_write` | 89.20ms | 124.65ms | 1.4× | 1.4% | PASS |
| ac_reads | `oltp_point_select` | 44.88ms | 50.30ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_range_select` | 12.45ms | 14.97ms | 1.2× | 1.7% | PASS |
| ac_reads | `oltp_sum_range` | 11.69ms | 14.32ms | 1.2× | 1.1% | PASS |
| ac_reads | `oltp_order_range` | 2.86ms | 3.45ms | 1.2× | 1.5% | PASS |
| ac_reads | `oltp_distinct_range` | 3.92ms | 4.61ms | 1.2× | 1.0% | PASS |
| ac_reads | `oltp_index_scan` | 6.22ms | 7.82ms | 1.3× | 2.1% | PASS |
| ac_reads | `select_random_points` | 12.22ms | 14.16ms | 1.2× | 1.9% | PASS |
| ac_reads | `select_random_ranges` | 6.83ms | 7.45ms | 1.1× | 1.7% | PASS |
| ac_reads | `covering_index_scan` | 10.09ms | 13.13ms | 1.3× | 1.7% | PASS |
| ac_reads | `groupby_scan` | 29.70ms | 34.81ms | 1.2× | 0.8% | PASS |
| ac_reads | `index_join` | 7.14ms | 10.37ms | 1.5× | 1.8% | PASS |
| ac_reads | `index_join_scan` | 3.58ms | 6.06ms | 1.7× | 1.2% | PASS |
| ac_reads | `types_table_scan` | 1.03s | 1.22s | 1.2× | 0.5% | PASS |
| ac_reads | `table_scan` | 1.16s | 1.33s | 1.1× | 0.9% | PASS |
| ac_reads | `oltp_read_only` | 134.17ms | 158.60ms | 1.2× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 19.80ms | 62.20ms | 3.1× | 3.7% | PASS |
| ac_writes | `oltp_insert_ac` | 23.84ms | 75.54ms | 3.2× | 5.8% | PASS |
| ac_writes | `oltp_update_index_ac` | 24.16ms | 88.02ms | 3.6× | 5.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.62ms | 69.15ms | 3.2× | 6.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 23.01ms | 77.77ms | 3.4× | 5.7% | PASS |
| ac_writes | `oltp_write_only_ac` | 22.70ms | 77.88ms | 3.4× | 5.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 21.01ms | 70.51ms | 3.4× | 5.6% | PASS |
| ac_writes | `oltp_read_write_ac` | 28.26ms | 83.15ms | 2.9× | 5.9% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 27.46ms | 26.95ms | 1.0× | 4.1% | PASS |
| mem_reads | `oltp_range_select` | 12.45ms | 10.83ms | 0.9× | 4.0% | PASS |
| mem_reads | `oltp_sum_range` | 11.82ms | 10.71ms | 0.9× | 3.1% | PASS |
| mem_reads | `oltp_order_range` | 2.41ms | 2.34ms | 1.0× | 3.2% | PASS |
| mem_reads | `oltp_distinct_range` | 2.96ms | 2.87ms | 1.0× | 2.6% | PASS |
| mem_reads | `oltp_index_scan` | 2.73ms | 5.02ms | 1.8× | 2.7% | PASS |
| mem_reads | `select_random_points` | 19.38ms | 17.02ms | 0.9× | 4.7% | PASS |
| mem_reads | `select_random_ranges` | 5.06ms | 4.69ms | 0.9× | 2.3% | PASS |
| mem_reads | `covering_index_scan` | 4.18ms | 7.20ms | 1.7× | 3.5% | PASS |
| mem_reads | `groupby_scan` | 22.40ms | 22.65ms | 1.0× | 1.4% | PASS |
| mem_reads | `index_join` | 8.84ms | 8.63ms | 1.0× | 3.6% | PASS |
| mem_reads | `index_join_scan` | 2.20ms | 5.28ms | 2.4× | 4.0% | FAIL |
| mem_reads | `types_table_scan` | 715.69ms | 827.44ms | 1.2× | 1.4% | PASS |
| mem_reads | `table_scan` | 1.04s | 1.03s | 1.0× | 4.0% | PASS |
| mem_reads | `oltp_read_only` | 108.82ms | 97.93ms | 0.9× | 3.4% | PASS |
| mem_writes | `oltp_bulk_insert` | 152.24ms | 235.06ms | 1.5× | 1.1% | PASS |
| mem_writes | `oltp_insert` | 11.33ms | 24.39ms | 2.2× | 2.0% | PASS |
| mem_writes | `oltp_update_index` | 45.06ms | 94.56ms | 2.1× | 2.7% | PASS |
| mem_writes | `oltp_update_non_index` | 34.02ms | 54.54ms | 1.6× | 2.5% | PASS |
| mem_writes | `oltp_delete_insert` | 37.11ms | 69.98ms | 1.9× | 1.7% | PASS |
| mem_writes | `oltp_write_only` | 20.49ms | 38.01ms | 1.9× | 1.6% | PASS |
| mem_writes | `types_delete_insert` | 27.92ms | 39.83ms | 1.4× | 1.7% | PASS |
| mem_writes | `oltp_read_write` | 68.14ms | 87.73ms | 1.3× | 2.8% | PASS |
| file_reads | `oltp_point_select` | 77.98ms | 39.93ms | 0.5× | 1.6% | PASS |
| file_reads | `oltp_range_select` | 17.09ms | 11.74ms | 0.7× | 2.6% | PASS |
| file_reads | `oltp_sum_range` | 16.49ms | 11.72ms | 0.7× | 1.5% | PASS |
| file_reads | `oltp_order_range` | 3.12ms | 2.54ms | 0.8× | 1.5% | PASS |
| file_reads | `oltp_distinct_range` | 3.65ms | 3.09ms | 0.8× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 8.33ms | 6.21ms | 0.7× | 1.8% | PASS |
| file_reads | `select_random_points` | 22.09ms | 16.28ms | 0.7× | 2.9% | PASS |
| file_reads | `select_random_ranges` | 10.34ms | 5.91ms | 0.6× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 9.95ms | 8.05ms | 0.8× | 1.2% | PASS |
| file_reads | `groupby_scan` | 22.52ms | 22.20ms | 1.0× | 1.9% | PASS |
| file_reads | `index_join` | 11.49ms | 8.26ms | 0.7× | 2.1% | PASS |
| file_reads | `index_join_scan` | 3.42ms | 5.25ms | 1.5× | 1.0% | PASS |
| file_reads | `types_table_scan` | 725.68ms | 837.17ms | 1.2× | 2.6% | PASS |
| file_reads | `table_scan` | 845.50ms | 931.64ms | 1.1× | 2.8% | PASS |
| file_reads | `oltp_read_only` | 156.64ms | 103.88ms | 0.7× | 2.3% | PASS |
| file_writes | `oltp_bulk_insert` | 220.98ms | 294.51ms | 1.3× | 3.7% | PASS |
| file_writes | `oltp_insert` | 20.04ms | 46.01ms | 2.3× | 6.6% | PASS |
| file_writes | `oltp_update_index` | 152.37ms | 170.68ms | 1.1× | 5.9% | PASS |
| file_writes | `oltp_update_non_index` | 131.77ms | 110.11ms | 0.8× | 3.8% | PASS |
| file_writes | `oltp_delete_insert` | 172.03ms | 139.21ms | 0.8× | 3.5% | PASS |
| file_writes | `oltp_write_only` | 116.77ms | 96.67ms | 0.8× | 4.3% | PASS |
| file_writes | `types_delete_insert` | 136.56ms | 83.83ms | 0.6× | 5.7% | PASS |
| file_writes | `oltp_read_write` | 155.45ms | 137.67ms | 0.9× | 5.7% | PASS |
| ac_reads | `oltp_point_select` | 42.81ms | 39.23ms | 0.9× | 2.6% | PASS |
| ac_reads | `oltp_range_select` | 13.45ms | 11.62ms | 0.9× | 3.1% | PASS |
| ac_reads | `oltp_sum_range` | 12.68ms | 11.39ms | 0.9× | 3.1% | PASS |
| ac_reads | `oltp_order_range` | 2.71ms | 2.49ms | 0.9× | 2.2% | PASS |
| ac_reads | `oltp_distinct_range` | 3.19ms | 3.00ms | 0.9× | 1.8% | PASS |
| ac_reads | `oltp_index_scan` | 4.83ms | 6.05ms | 1.3× | 3.3% | PASS |
| ac_reads | `select_random_points` | 18.41ms | 16.17ms | 0.9× | 3.2% | PASS |
| ac_reads | `select_random_ranges` | 6.68ms | 5.87ms | 0.9× | 2.6% | PASS |
| ac_reads | `covering_index_scan` | 6.33ms | 8.12ms | 1.3× | 2.4% | PASS |
| ac_reads | `groupby_scan` | 21.24ms | 21.79ms | 1.0× | 1.5% | PASS |
| ac_reads | `index_join` | 9.23ms | 8.01ms | 0.9× | 2.0% | PASS |
| ac_reads | `index_join_scan` | 2.98ms | 5.12ms | 1.7× | 2.3% | PASS |
| ac_reads | `types_table_scan` | 698.13ms | 811.53ms | 1.2× | 0.7% | PASS |
| ac_reads | `table_scan` | 814.91ms | 910.30ms | 1.1× | 1.5% | PASS |
| ac_reads | `oltp_read_only` | 104.93ms | 101.70ms | 1.0× | 1.1% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.42ms | 57.08ms | 2.5× | 4.7% | PASS |
| ac_writes | `oltp_insert_ac` | 24.11ms | 64.25ms | 2.7× | 8.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 24.43ms | 75.27ms | 3.1× | 5.3% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.59ms | 62.52ms | 2.9× | 6.4% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.53ms | 70.31ms | 2.9× | 5.9% | PASS |
| ac_writes | `oltp_write_only_ac` | 23.89ms | 71.18ms | 3.0× | 5.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.71ms | 62.49ms | 2.8× | 9.6% | PASS |
| ac_writes | `oltp_read_write_ac` | 27.59ms | 74.22ms | 2.7× | 6.6% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 35.22ms | 42.45ms | 1.2× | 2.4% | PASS |
| mem_reads | `oltp_range_select` | 16.09ms | 17.24ms | 1.1× | 2.2% | PASS |
| mem_reads | `oltp_sum_range` | 14.59ms | 15.62ms | 1.1× | 2.2% | PASS |
| mem_reads | `oltp_order_range` | 3.15ms | 3.72ms | 1.2× | 1.3% | PASS |
| mem_reads | `oltp_distinct_range` | 4.15ms | 4.82ms | 1.2× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 3.86ms | 6.94ms | 1.8× | 1.5% | PASS |
| mem_reads | `select_random_points` | 20.61ms | 22.69ms | 1.1× | 2.4% | PASS |
| mem_reads | `select_random_ranges` | 6.27ms | 7.19ms | 1.1× | 2.1% | PASS |
| mem_reads | `covering_index_scan` | 7.61ms | 11.39ms | 1.5× | 0.8% | PASS |
| mem_reads | `groupby_scan` | 33.20ms | 37.38ms | 1.1× | 1.0% | PASS |
| mem_reads | `index_join` | 10.11ms | 10.95ms | 1.1× | 2.1% | PASS |
| mem_reads | `index_join_scan` | 3.65ms | 6.43ms | 1.8× | 1.6% | PASS |
| mem_reads | `types_table_scan` | 1.04s | 1.33s | 1.3× | 0.6% | PASS |
| mem_reads | `table_scan` | 1.26s | 1.56s | 1.2× | 1.7% | PASS |
| mem_reads | `oltp_read_only` | 137.10ms | 159.55ms | 1.2× | 1.5% | PASS |
| mem_writes | `oltp_bulk_insert` | 240.06ms | 403.33ms | 1.7× | 1.3% | PASS |
| mem_writes | `oltp_insert` | 18.34ms | 39.96ms | 2.2× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 66.00ms | 143.54ms | 2.2× | 2.0% | PASS |
| mem_writes | `oltp_update_non_index` | 48.67ms | 84.08ms | 1.7× | 1.6% | PASS |
| mem_writes | `oltp_delete_insert` | 52.73ms | 106.20ms | 2.0× | 1.4% | PASS |
| mem_writes | `oltp_write_only` | 27.75ms | 56.90ms | 2.1× | 1.7% | PASS |
| mem_writes | `types_delete_insert` | 38.91ms | 61.30ms | 1.6× | 1.7% | PASS |
| mem_writes | `oltp_read_write` | 96.45ms | 142.97ms | 1.5× | 2.0% | PASS |
| file_reads | `oltp_point_select` | 99.92ms | 59.64ms | 0.6× | 1.3% | PASS |
| file_reads | `oltp_range_select` | 22.37ms | 19.15ms | 0.9× | 1.8% | PASS |
| file_reads | `oltp_sum_range` | 21.69ms | 17.53ms | 0.8× | 1.7% | PASS |
| file_reads | `oltp_order_range` | 4.10ms | 3.97ms | 1.0× | 1.7% | PASS |
| file_reads | `oltp_distinct_range` | 5.23ms | 5.12ms | 1.0× | 2.2% | PASS |
| file_reads | `oltp_index_scan` | 10.65ms | 8.91ms | 0.8× | 1.5% | PASS |
| file_reads | `select_random_points` | 29.59ms | 25.84ms | 0.9× | 2.0% | PASS |
| file_reads | `select_random_ranges` | 13.40ms | 9.14ms | 0.7× | 1.8% | PASS |
| file_reads | `covering_index_scan` | 14.55ms | 13.65ms | 0.9× | 1.0% | PASS |
| file_reads | `groupby_scan` | 34.29ms | 37.75ms | 1.1× | 0.7% | PASS |
| file_reads | `index_join` | 13.97ms | 12.24ms | 0.9× | 2.9% | PASS |
| file_reads | `index_join_scan` | 4.70ms | 6.89ms | 1.5× | 2.4% | PASS |
| file_reads | `types_table_scan` | 1.05s | 1.33s | 1.3× | 0.9% | PASS |
| file_reads | `table_scan` | 1.18s | 1.54s | 1.3× | 0.9% | PASS |
| file_reads | `oltp_read_only` | 233.51ms | 184.99ms | 0.8× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 261.61ms | 413.81ms | 1.6× | 1.6% | PASS |
| file_writes | `oltp_insert` | 25.28ms | 44.74ms | 1.8× | 1.5% | PASS |
| file_writes | `oltp_update_index` | 96.00ms | 159.63ms | 1.7× | 1.7% | PASS |
| file_writes | `oltp_update_non_index` | 89.36ms | 96.28ms | 1.1× | 9.3% | PASS |
| file_writes | `oltp_delete_insert` | 86.78ms | 122.59ms | 1.4× | 1.6% | PASS |
| file_writes | `oltp_write_only` | 56.91ms | 69.72ms | 1.2× | 2.0% | PASS |
| file_writes | `types_delete_insert` | 63.86ms | 68.72ms | 1.1× | 2.1% | PASS |
| file_writes | `oltp_read_write` | 125.25ms | 155.05ms | 1.2× | 1.7% | PASS |
| ac_reads | `oltp_point_select` | 56.90ms | 59.63ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_range_select` | 18.54ms | 19.13ms | 1.0× | 1.8% | PASS |
| ac_reads | `oltp_sum_range` | 17.30ms | 17.55ms | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_order_range` | 3.70ms | 3.96ms | 1.1× | 2.1% | PASS |
| ac_reads | `oltp_distinct_range` | 4.75ms | 5.10ms | 1.1× | 1.5% | PASS |
| ac_reads | `oltp_index_scan` | 6.45ms | 8.82ms | 1.4× | 2.1% | PASS |
| ac_reads | `select_random_points` | 25.75ms | 26.16ms | 1.0× | 3.3% | PASS |
| ac_reads | `select_random_ranges` | 8.90ms | 9.09ms | 1.0× | 1.9% | PASS |
| ac_reads | `covering_index_scan` | 10.17ms | 13.54ms | 1.3× | 1.5% | PASS |
| ac_reads | `groupby_scan` | 33.93ms | 37.86ms | 1.1× | 1.0% | PASS |
| ac_reads | `index_join` | 11.83ms | 12.22ms | 1.0× | 2.4% | PASS |
| ac_reads | `index_join_scan` | 4.17ms | 6.93ms | 1.7× | 2.1% | PASS |
| ac_reads | `types_table_scan` | 1.05s | 1.33s | 1.3× | 0.6% | PASS |
| ac_reads | `table_scan` | 1.17s | 1.53s | 1.3× | 0.8% | PASS |
| ac_reads | `oltp_read_only` | 169.44ms | 184.29ms | 1.1× | 1.3% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.08ms | 62.82ms | 2.8× | 10.3% | PASS |
| ac_writes | `oltp_insert_ac` | 25.48ms | 78.66ms | 3.1× | 6.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 25.38ms | 92.28ms | 3.6× | 5.8% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.32ms | 70.95ms | 3.2× | 5.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.70ms | 82.02ms | 3.2× | 9.2% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.35ms | 82.84ms | 3.3× | 7.0% | PASS |
| ac_writes | `types_delete_insert_ac` | 24.32ms | 78.82ms | 3.2× | 8.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.39ms | 90.77ms | 2.9× | 4.9% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 20.04ms | 25.76ms | 1.3× | 0.9% | PASS |
| mem_reads | `oltp_range_select` | 13.79ms | 15.38ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_sum_range` | 12.93ms | 14.44ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_order_range` | 2.50ms | 2.81ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_distinct_range` | 3.15ms | 3.49ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 2.97ms | 4.27ms | 1.4× | 1.7% | PASS |
| mem_reads | `select_random_points` | 19.14ms | 22.31ms | 1.2× | 1.0% | PASS |
| mem_reads | `select_random_ranges` | 4.63ms | 6.11ms | 1.3× | 1.7% | PASS |
| mem_reads | `covering_index_scan` | 4.41ms | 6.65ms | 1.5× | 0.8% | PASS |
| mem_reads | `groupby_scan` | 22.19ms | 27.04ms | 1.2× | 0.7% | PASS |
| mem_reads | `index_join` | 5.41ms | 8.45ms | 1.6× | 1.7% | PASS |
| mem_reads | `index_join_scan` | 2.81ms | 5.08ms | 1.8× | 3.0% | PASS |
| mem_reads | `types_table_scan` | 748.37ms | 908.40ms | 1.2× | 0.5% | PASS |
| mem_reads | `table_scan` | 872.22ms | 967.53ms | 1.1× | 2.4% | PASS |
| mem_reads | `oltp_read_only` | 94.17ms | 113.98ms | 1.2× | 1.4% | PASS |
| mem_writes | `oltp_bulk_insert` | 137.26ms | 209.82ms | 1.5× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 11.48ms | 20.92ms | 1.8× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 43.61ms | 85.42ms | 2.0× | 1.3% | PASS |
| mem_writes | `oltp_update_non_index` | 31.81ms | 57.00ms | 1.8× | 1.2% | PASS |
| mem_writes | `oltp_delete_insert` | 32.65ms | 64.22ms | 2.0× | 0.9% | PASS |
| mem_writes | `oltp_write_only` | 18.23ms | 38.69ms | 2.1× | 1.2% | PASS |
| mem_writes | `types_delete_insert` | 20.34ms | 35.19ms | 1.7× | 1.5% | PASS |
| mem_writes | `oltp_read_write` | 61.75ms | 100.96ms | 1.6× | 1.3% | PASS |
| file_reads | `oltp_point_select` | 39.93ms | 30.46ms | 0.8× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 16.11ms | 16.10ms | 1.0× | 1.0% | PASS |
| file_reads | `oltp_sum_range` | 15.15ms | 15.12ms | 1.0× | 1.4% | PASS |
| file_reads | `oltp_order_range` | 2.75ms | 2.93ms | 1.1× | 1.8% | PASS |
| file_reads | `oltp_distinct_range` | 3.41ms | 3.60ms | 1.1× | 1.4% | PASS |
| file_reads | `oltp_index_scan` | 5.21ms | 5.14ms | 1.0× | 2.1% | PASS |
| file_reads | `select_random_points` | 21.77ms | 23.90ms | 1.1× | 0.9% | PASS |
| file_reads | `select_random_ranges` | 6.91ms | 6.82ms | 1.0× | 1.3% | PASS |
| file_reads | `covering_index_scan` | 6.51ms | 7.38ms | 1.1× | 2.1% | PASS |
| file_reads | `groupby_scan` | 22.42ms | 27.12ms | 1.2× | 0.6% | PASS |
| file_reads | `index_join` | 6.54ms | 8.79ms | 1.3× | 2.3% | PASS |
| file_reads | `index_join_scan` | 3.04ms | 5.26ms | 1.7× | 4.2% | PASS |
| file_reads | `types_table_scan` | 754.21ms | 917.35ms | 1.2× | 1.4% | PASS |
| file_reads | `table_scan` | 866.00ms | 965.28ms | 1.1× | 1.9% | PASS |
| file_reads | `oltp_read_only` | 122.75ms | 120.79ms | 1.0× | 1.4% | PASS |
| file_writes | `oltp_bulk_insert` | 187.03ms | 258.63ms | 1.4× | 3.0% | PASS |
| file_writes | `oltp_insert` | 19.86ms | 30.94ms | 1.6× | 11.0% | PASS |
| file_writes | `oltp_update_index` | 137.43ms | 142.69ms | 1.0× | 3.4% | PASS |
| file_writes | `oltp_update_non_index` | 112.52ms | 99.00ms | 0.9× | 5.0% | PASS |
| file_writes | `oltp_delete_insert` | 104.56ms | 109.20ms | 1.0× | 7.9% | PASS |
| file_writes | `oltp_write_only` | 81.05ms | 79.16ms | 1.0× | 1.6% | PASS |
| file_writes | `types_delete_insert` | 70.26ms | 64.07ms | 0.9× | 8.3% | PASS |
| file_writes | `oltp_read_write` | 121.52ms | 139.08ms | 1.1× | 3.4% | PASS |
| ac_reads | `oltp_point_select` | 26.70ms | 30.62ms | 1.1× | 0.8% | PASS |
| ac_reads | `oltp_range_select` | 15.09ms | 16.19ms | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_sum_range` | 13.92ms | 15.15ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_order_range` | 2.64ms | 2.94ms | 1.1× | 1.8% | PASS |
| ac_reads | `oltp_distinct_range` | 3.31ms | 3.63ms | 1.1× | 2.0% | PASS |
| ac_reads | `oltp_index_scan` | 4.03ms | 5.11ms | 1.3× | 2.2% | PASS |
| ac_reads | `select_random_points` | 20.41ms | 23.91ms | 1.2× | 1.1% | PASS |
| ac_reads | `select_random_ranges` | 5.75ms | 6.93ms | 1.2× | 1.4% | PASS |
| ac_reads | `covering_index_scan` | 5.42ms | 7.62ms | 1.4× | 1.4% | PASS |
| ac_reads | `groupby_scan` | 22.51ms | 27.41ms | 1.2× | 0.7% | PASS |
| ac_reads | `index_join` | 6.05ms | 8.94ms | 1.5× | 2.5% | PASS |
| ac_reads | `index_join_scan` | 2.99ms | 5.34ms | 1.8× | 3.8% | PASS |
| ac_reads | `types_table_scan` | 750.80ms | 914.43ms | 1.2× | 0.7% | PASS |
| ac_reads | `table_scan` | 870.89ms | 965.94ms | 1.1× | 2.3% | PASS |
| ac_reads | `oltp_read_only` | 102.26ms | 120.45ms | 1.2× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 31.27ms | 84.33ms | 2.7× | 6.5% | PASS |
| ac_writes | `oltp_insert_ac` | 33.08ms | 97.61ms | 3.0× | 3.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 34.56ms | 104.60ms | 3.0× | 3.8% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 32.25ms | 93.56ms | 2.9× | 3.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 33.45ms | 99.56ms | 3.0× | 3.4% | PASS |
| ac_writes | `oltp_write_only_ac` | 33.16ms | 98.58ms | 3.0× | 3.4% | PASS |
| ac_writes | `types_delete_insert_ac` | 30.71ms | 84.71ms | 2.8× | 5.2% | PASS |
| ac_writes | `oltp_read_write_ac` | 35.40ms | 101.12ms | 2.9× | 4.4% | PASS |

</details>

</details>

## Version-control latency

Wall time: 6m 52s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 28.48ms | 38.00ms | 75.0% | 1.0% | PASS |
| `status_dirty_many_tables` | 31.45ms | 42.00ms | 74.9% | 1.3% | PASS |
| `diff_regular_working_one_table` | 24.44ms | 33.00ms | 74.1% | 1.0% | PASS |
| `diff_regular_working_many_tables` | 36.00ms | 50.00ms | 72.0% | 1.4% | PASS |
| `diff_stat_working_many_tables` | 36.70ms | 48.00ms | 76.5% | 0.9% | PASS |
| `diff_schema_working_many_tables` | 37.29ms | 48.00ms | 77.7% | 1.5% | PASS |
| `branch_list_many_branches` | 20.14ms | 25.00ms | 80.6% | 1.0% | PASS |
| `branch_create_delete` | 21.65ms | 27.00ms | 80.2% | 0.8% | PASS |
| `at_literal_deep_history` | 21.49ms | 28.00ms | 76.7% | 0.7% | PASS |
| `diff_literal_deep_history` | 21.61ms | 28.00ms | 77.2% | 0.6% | PASS |
| `history_literal_deep_history` | 22.73ms | 30.00ms | 75.8% | 0.6% | PASS |
| `checkout_branch_clean` | 29.44ms | 43.00ms | 68.5% | 0.9% | PASS |
| `merge_data_no_conflicts` | 23.82ms | 33.00ms | 72.2% | 2.1% | PASS |
| `merge_data_secondary_index` | 826.77ms | 944.00ms | 87.6% | 1.3% | PASS |
| `merge_schema_no_conflicts` | 19.82ms | 24.00ms | 82.6% | 1.9% | PASS |
| `merge_data_conflicts` | 26.44ms | 33.00ms | 80.1% | 1.3% | PASS |
| `merge_data_conflicts_with_resolve` | 26.85ms | 34.00ms | 79.0% | 1.8% | PASS |

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

candidate: 89a2cdf.

Fixture file bytes: base 107,968,266; delta 115,878,997.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 413.519 | 412.857–526.436 | 32.410 | 241.83 | 107968266 |
| push_initial | candidate | 792.792 | 787.188–831.344 | 412.369 | 126.14 | 107956772 |
| clone_initial | candidate | 964.703 | 952.672–1061.389 | 534.626 | 103.66 | 108475035 |
| push_noop | candidate | 0.614 | 0.550–0.668 | 0.314 | — | 0 |
| fetch_noop | candidate | 0.166 | 0.162–0.179 | 0.168 | — | 0 |
| push_incremental | candidate | 196.960 | 195.586–207.838 | 168.163 | 5.08 | 7909775 |
| fetch_incremental | candidate | 101.627 | 99.016–102.454 | 73.199 | 9.84 | 7910084 |
| pull_fast_forward | candidate | 91.199 | 89.666–151.021 | 61.570 | 10.97 | 7910844 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 90.54 | 175.487 | 108.763 | 18329 | 2066 |
| clone_initial | candidate | 90.90 | 295.073 | 109.784 | 47240 | 2337 |
| push_noop | candidate | 42.52 | 0.075 | 0.000 | 27 | 2 |
| fetch_noop | candidate | 42.52 | 0.135 | 0.000 | 40 | 0 |
| push_incremental | candidate | 42.52 | 96.707 | 7.910 | 22372 | 125 |
| fetch_incremental | candidate | 42.52 | 34.472 | 7.910 | 10808 | 124 |
| pull_fast_forward | candidate | 42.52 | 26.497 | 7.911 | 7031 | 126 |

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

candidate: 89a2cdf.

Fixture file bytes: base 1,087,718,031; delta 1,103,525,159.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 2659.803 | 2659.170–2666.460 | 382.794 | 375.97 | 1087718031 |
| push_initial | candidate | 8085.166 | 8057.201–8113.262 | 5872.843 | 123.68 | 1079566275 |
| clone_initial | candidate | 8874.113 | 8857.727–8933.557 | 8632.565 | 112.69 | 1102407326 |
| push_noop | candidate | 0.792 | 0.742–1.001 | 0.358 | — | 0 |
| fetch_noop | candidate | 0.216 | 0.200–0.239 | 0.220 | — | 0 |
| push_incremental | candidate | 1323.923 | 1316.691–1324.159 | 1315.755 | 0.76 | 15806003 |
| fetch_incremental | candidate | 284.965 | 280.352–296.988 | 278.132 | 3.51 | 15806513 |
| pull_fast_forward | candidate | 279.814 | 278.570–291.202 | 272.849 | 3.57 | 15807273 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 134.77 | 2091.666 | 1087.622 | 263446 | 20541 |
| clone_initial | candidate | 123.93 | 3515.681 | 1132.819 | 611833 | 32243 |
| push_noop | candidate | 45.26 | 0.078 | 0.000 | 27 | 2 |
| fetch_noop | candidate | 45.26 | 0.137 | 0.000 | 40 | 0 |
| push_incremental | candidate | 48.25 | 615.596 | 15.928 | 149536 | 313 |
| fetch_incremental | candidate | 50.41 | 61.896 | 15.928 | 13813 | 313 |
| pull_fast_forward | candidate | 50.40 | 61.829 | 15.929 | 13809 | 315 |

</details>


</details>

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh`, `test/vc_perf_ceiling.sh`, and `test/remote_perf.py`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
