# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-10-07 11:13 UTC
>
> Commit: [`b9126544b1ebbaeb955c4da173aab1d3acf6b35b`](https://github.com/dolthub/doltlite/commit/b9126544b1ebbaeb955c4da173aab1d3acf6b35b)
>
> Runner: ubuntu24 20260927.320.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/37602220858)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.21s | 10.24s | 1.1× | 1.7% | **PASS** |
| Writes | 1.91s | 2.97s | 1.6× | 1.8% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.05s | 10.47s | 1.0× | 1.5% | **PASS** |
| Writes | 3.53s | 3.83s | 1.1× | 2.5% | **PASS** |
| Autocommit writes | 757.12ms | 2.28s | 3.0× | 6.5% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 1.66s | 1.71s | 1.0× | 2.4% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.88s | 3.08s | 1.1× | 2.4% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.51s | 3.00s | 1.2× | 1.4% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.15s | 2.45s | 1.1× | 1.2% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 271.44ms | 419.56ms | 1.5× | 2.3% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 609.62ms | 964.92ms | 1.6× | 2.0% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 575.55ms | 929.31ms | 1.6× | 1.6% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 453.45ms | 656.81ms | 1.4× | 1.1% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 1.74s | 1.72s | 1.0× | 1.9% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 3.16s | 3.16s | 1.0× | 2.0% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.84s | 3.10s | 1.1× | 1.3% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.31s | 2.49s | 1.1× | 0.9% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 740.34ms | 698.02ms | 0.9× | 2.3% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 940.13ms | 1.06s | 1.1× | 3.3% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 811.70ms | 1.04s | 1.3× | 1.5% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 1.04s | 1.03s | 1.0× | 4.0% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 1.67s | 1.77s | 1.1× | 2.1% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.89s | 3.13s | 1.1× | 2.2% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.64s | 3.07s | 1.2× | 1.4% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.18s | 2.50s | 1.1× | 0.8% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 176.03ms | 486.88ms | 2.8× | 3.8% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 212.41ms | 642.84ms | 3.0× | 6.3% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 204.04ms | 611.48ms | 3.0× | 6.2% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 164.64ms | 541.66ms | 3.3× | 11.5% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 15.03ms | 17.62ms | 1.2× | 2.3% | PASS |
| mem_reads | `oltp_range_select` | 5.96ms | 6.57ms | 1.1× | 2.2% | PASS |
| mem_reads | `oltp_sum_range` | 5.56ms | 6.66ms | 1.2× | 3.1% | PASS |
| mem_reads | `oltp_order_range` | 1.60ms | 1.72ms | 1.1× | 2.0% | PASS |
| mem_reads | `oltp_distinct_range` | 2.12ms | 2.24ms | 1.1× | 1.8% | PASS |
| mem_reads | `oltp_index_scan` | 2.44ms | 3.28ms | 1.3× | 4.5% | PASS |
| mem_reads | `select_random_points` | 7.63ms | 8.25ms | 1.1× | 4.3% | PASS |
| mem_reads | `select_random_ranges` | 2.84ms | 3.02ms | 1.1× | 1.8% | PASS |
| mem_reads | `covering_index_scan` | 4.06ms | 5.46ms | 1.3× | 2.6% | PASS |
| mem_reads | `groupby_scan` | 17.38ms | 19.01ms | 1.1× | 1.0% | PASS |
| mem_reads | `index_join` | 3.56ms | 5.45ms | 1.5× | 4.6% | PASS |
| mem_reads | `index_join_scan` | 2.30ms | 3.95ms | 1.7× | 3.1% | PASS |
| mem_reads | `types_table_scan` | 682.23ms | 730.93ms | 1.1× | 1.4% | PASS |
| mem_reads | `table_scan` | 841.80ms | 825.76ms | 1.0× | 2.4% | PASS |
| mem_reads | `oltp_read_only` | 62.67ms | 66.25ms | 1.1× | 3.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 108.13ms | 156.27ms | 1.4× | 1.8% | PASS |
| mem_writes | `oltp_insert` | 9.10ms | 15.87ms | 1.7× | 1.4% | PASS |
| mem_writes | `oltp_update_index` | 33.12ms | 58.97ms | 1.8× | 2.1% | PASS |
| mem_writes | `oltp_update_non_index` | 23.11ms | 34.83ms | 1.5× | 2.4% | PASS |
| mem_writes | `oltp_delete_insert` | 28.52ms | 44.26ms | 1.6× | 3.1% | PASS |
| mem_writes | `oltp_write_only` | 14.37ms | 28.48ms | 2.0× | 2.8% | PASS |
| mem_writes | `types_delete_insert` | 16.57ms | 22.27ms | 1.3× | 2.4% | PASS |
| mem_writes | `oltp_read_write` | 38.53ms | 58.63ms | 1.5× | 2.2% | PASS |
| file_reads | `oltp_point_select` | 69.66ms | 32.35ms | 0.5× | 1.1% | PASS |
| file_reads | `oltp_range_select` | 12.22ms | 8.36ms | 0.7× | 2.1% | PASS |
| file_reads | `oltp_sum_range` | 11.57ms | 8.37ms | 0.7× | 1.7% | PASS |
| file_reads | `oltp_order_range` | 2.29ms | 1.94ms | 0.8× | 1.5% | PASS |
| file_reads | `oltp_distinct_range` | 2.81ms | 2.53ms | 0.9× | 1.5% | PASS |
| file_reads | `oltp_index_scan` | 8.33ms | 5.13ms | 0.6× | 1.9% | PASS |
| file_reads | `select_random_points` | 13.46ms | 9.73ms | 0.7× | 2.1% | PASS |
| file_reads | `select_random_ranges` | 8.26ms | 4.47ms | 0.5× | 1.8% | PASS |
| file_reads | `covering_index_scan` | 9.88ms | 7.31ms | 0.7× | 2.2% | PASS |
| file_reads | `groupby_scan` | 17.56ms | 18.55ms | 1.1× | 1.3% | PASS |
| file_reads | `index_join` | 6.68ms | 6.48ms | 1.0× | 2.7% | PASS |
| file_reads | `index_join_scan` | 2.89ms | 4.18ms | 1.4× | 3.9% | PASS |
| file_reads | `types_table_scan` | 672.00ms | 725.75ms | 1.1× | 1.5% | PASS |
| file_reads | `table_scan` | 766.02ms | 803.23ms | 1.0× | 2.0% | PASS |
| file_reads | `oltp_read_only` | 133.80ms | 82.46ms | 0.6× | 1.9% | PASS |
| file_writes | `oltp_bulk_insert` | 142.68ms | 199.81ms | 1.4× | 3.2% | PASS |
| file_writes | `oltp_insert` | 19.82ms | 26.18ms | 1.3× | 9.9% | PASS |
| file_writes | `oltp_update_index` | 121.61ms | 108.46ms | 0.9× | 1.4% | PASS |
| file_writes | `oltp_update_non_index` | 101.20ms | 79.36ms | 0.8× | 0.8% | PASS |
| file_writes | `oltp_delete_insert` | 101.47ms | 90.00ms | 0.9× | 0.5% | PASS |
| file_writes | `oltp_write_only` | 81.23ms | 60.27ms | 0.7× | 0.8% | PASS |
| file_writes | `types_delete_insert` | 61.55ms | 41.65ms | 0.7× | 17.2% | PASS |
| file_writes | `oltp_read_write` | 110.78ms | 92.28ms | 0.8× | 6.0% | PASS |
| ac_reads | `oltp_point_select` | 32.49ms | 31.70ms | 1.0× | 1.8% | PASS |
| ac_reads | `oltp_range_select` | 8.38ms | 8.16ms | 1.0× | 2.1% | PASS |
| ac_reads | `oltp_sum_range` | 7.86ms | 8.31ms | 1.1× | 1.9% | PASS |
| ac_reads | `oltp_order_range` | 1.94ms | 1.93ms | 1.0× | 2.3% | PASS |
| ac_reads | `oltp_distinct_range` | 2.46ms | 2.49ms | 1.0× | 2.0% | PASS |
| ac_reads | `oltp_index_scan` | 4.62ms | 4.98ms | 1.1× | 2.7% | PASS |
| ac_reads | `select_random_points` | 9.69ms | 9.66ms | 1.0× | 4.5% | PASS |
| ac_reads | `select_random_ranges` | 4.75ms | 4.48ms | 0.9× | 2.1% | PASS |
| ac_reads | `covering_index_scan` | 6.38ms | 7.41ms | 1.2× | 2.3% | PASS |
| ac_reads | `groupby_scan` | 17.70ms | 19.07ms | 1.1× | 1.4% | PASS |
| ac_reads | `index_join` | 4.75ms | 6.37ms | 1.3× | 3.8% | PASS |
| ac_reads | `index_join_scan` | 2.39ms | 4.19ms | 1.8× | 2.3% | PASS |
| ac_reads | `types_table_scan` | 681.19ms | 741.51ms | 1.1× | 1.2% | PASS |
| ac_reads | `table_scan` | 794.12ms | 829.20ms | 1.0× | 2.0% | PASS |
| ac_reads | `oltp_read_only` | 88.37ms | 87.14ms | 1.0× | 1.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 20.14ms | 51.80ms | 2.6× | 2.8% | PASS |
| ac_writes | `oltp_insert_ac` | 22.16ms | 59.64ms | 2.7× | 3.7% | PASS |
| ac_writes | `oltp_update_index_ac` | 23.21ms | 66.78ms | 2.9× | 3.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 20.31ms | 54.48ms | 2.7× | 3.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 21.80ms | 62.15ms | 2.9× | 4.0% | PASS |
| ac_writes | `oltp_write_only_ac` | 22.15ms | 62.83ms | 2.8× | 5.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 20.82ms | 60.83ms | 2.9× | 6.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 25.44ms | 68.38ms | 2.7× | 4.8% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 37.18ms | 41.02ms | 1.1× | 4.0% | PASS |
| mem_reads | `oltp_range_select` | 18.55ms | 15.79ms | 0.9× | 2.8% | PASS |
| mem_reads | `oltp_sum_range` | 16.14ms | 15.45ms | 1.0× | 2.8% | PASS |
| mem_reads | `oltp_order_range` | 3.39ms | 3.40ms | 1.0× | 1.6% | PASS |
| mem_reads | `oltp_distinct_range` | 4.44ms | 4.50ms | 1.0× | 1.4% | PASS |
| mem_reads | `oltp_index_scan` | 4.00ms | 6.79ms | 1.7× | 1.8% | PASS |
| mem_reads | `select_random_points` | 23.82ms | 24.13ms | 1.0× | 4.6% | PASS |
| mem_reads | `select_random_ranges` | 7.03ms | 7.04ms | 1.0× | 1.5% | PASS |
| mem_reads | `covering_index_scan` | 7.70ms | 10.83ms | 1.4× | 0.8% | PASS |
| mem_reads | `groupby_scan` | 34.86ms | 35.73ms | 1.0× | 0.8% | PASS |
| mem_reads | `index_join` | 10.96ms | 10.71ms | 1.0× | 3.3% | PASS |
| mem_reads | `index_join_scan` | 3.87ms | 6.74ms | 1.7× | 2.4% | PASS |
| mem_reads | `types_table_scan` | 1.18s | 1.32s | 1.1× | 3.6% | PASS |
| mem_reads | `table_scan` | 1.39s | 1.42s | 1.0× | 1.7% | PASS |
| mem_reads | `oltp_read_only` | 148.73ms | 149.21ms | 1.0× | 2.5% | PASS |
| mem_writes | `oltp_bulk_insert` | 238.31ms | 334.85ms | 1.4× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 17.89ms | 38.68ms | 2.2× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 67.19ms | 140.17ms | 2.1× | 2.0% | PASS |
| mem_writes | `oltp_update_non_index` | 51.50ms | 84.49ms | 1.6× | 2.1% | PASS |
| mem_writes | `oltp_delete_insert` | 57.07ms | 104.03ms | 1.8× | 1.9% | PASS |
| mem_writes | `oltp_write_only` | 29.00ms | 59.58ms | 2.1× | 2.7% | PASS |
| mem_writes | `types_delete_insert` | 40.39ms | 58.08ms | 1.4× | 2.6% | PASS |
| mem_writes | `oltp_read_write` | 108.28ms | 145.05ms | 1.3× | 3.4% | PASS |
| file_reads | `oltp_point_select` | 108.18ms | 60.73ms | 0.6× | 1.4% | PASS |
| file_reads | `oltp_range_select` | 25.67ms | 17.88ms | 0.7× | 2.8% | PASS |
| file_reads | `oltp_sum_range` | 24.09ms | 18.02ms | 0.7× | 2.2% | PASS |
| file_reads | `oltp_order_range` | 4.27ms | 3.66ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_distinct_range` | 5.41ms | 4.78ms | 0.9× | 1.1% | PASS |
| file_reads | `oltp_index_scan` | 11.42ms | 8.88ms | 0.8× | 1.6% | PASS |
| file_reads | `select_random_points` | 33.29ms | 27.63ms | 0.8× | 2.7% | PASS |
| file_reads | `select_random_ranges` | 14.57ms | 9.20ms | 0.6× | 2.0% | PASS |
| file_reads | `covering_index_scan` | 15.45ms | 13.46ms | 0.9× | 2.0% | PASS |
| file_reads | `groupby_scan` | 35.76ms | 36.13ms | 1.0× | 0.8% | PASS |
| file_reads | `index_join` | 14.99ms | 11.92ms | 0.8× | 3.0% | PASS |
| file_reads | `index_join_scan` | 4.84ms | 7.01ms | 1.5× | 3.4% | PASS |
| file_reads | `types_table_scan` | 1.24s | 1.34s | 1.1× | 2.8% | PASS |
| file_reads | `table_scan` | 1.37s | 1.42s | 1.0× | 2.3% | PASS |
| file_reads | `oltp_read_only` | 256.03ms | 179.92ms | 0.7× | 1.8% | PASS |
| file_writes | `oltp_bulk_insert` | 263.66ms | 349.03ms | 1.3× | 1.1% | PASS |
| file_writes | `oltp_insert` | 25.77ms | 44.30ms | 1.7× | 1.7% | PASS |
| file_writes | `oltp_update_index` | 140.51ms | 157.53ms | 1.1× | 6.4% | PASS |
| file_writes | `oltp_update_non_index` | 102.71ms | 97.52ms | 0.9× | 10.1% | PASS |
| file_writes | `oltp_delete_insert` | 96.55ms | 118.32ms | 1.2× | 1.8% | PASS |
| file_writes | `oltp_write_only` | 86.11ms | 71.53ms | 0.8× | 16.7% | PASS |
| file_writes | `types_delete_insert` | 73.89ms | 68.52ms | 0.9× | 1.6% | PASS |
| file_writes | `oltp_read_write` | 150.93ms | 157.28ms | 1.0× | 4.9% | PASS |
| ac_reads | `oltp_point_select` | 60.49ms | 60.78ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_range_select` | 21.04ms | 17.91ms | 0.9× | 3.2% | PASS |
| ac_reads | `oltp_sum_range` | 19.16ms | 17.82ms | 0.9× | 2.4% | PASS |
| ac_reads | `oltp_order_range` | 3.84ms | 3.68ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_distinct_range` | 4.86ms | 4.80ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 6.87ms | 9.02ms | 1.3× | 1.8% | PASS |
| ac_reads | `select_random_points` | 27.83ms | 27.42ms | 1.0× | 3.7% | PASS |
| ac_reads | `select_random_ranges` | 9.71ms | 9.24ms | 1.0× | 2.0% | PASS |
| ac_reads | `covering_index_scan` | 10.60ms | 13.33ms | 1.3× | 2.3% | PASS |
| ac_reads | `groupby_scan` | 35.01ms | 36.13ms | 1.0× | 1.0% | PASS |
| ac_reads | `index_join` | 13.02ms | 12.02ms | 0.9× | 3.7% | PASS |
| ac_reads | `index_join_scan` | 4.32ms | 7.01ms | 1.6× | 2.2% | PASS |
| ac_reads | `types_table_scan` | 1.16s | 1.31s | 1.1× | 3.0% | PASS |
| ac_reads | `table_scan` | 1.34s | 1.42s | 1.1× | 3.3% | PASS |
| ac_reads | `oltp_read_only` | 182.43ms | 178.85ms | 1.0× | 1.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 23.62ms | 63.03ms | 2.7× | 9.4% | PASS |
| ac_writes | `oltp_insert_ac` | 27.02ms | 78.31ms | 2.9× | 5.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 29.37ms | 92.59ms | 3.2× | 7.8% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.07ms | 73.76ms | 3.2× | 5.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.70ms | 83.61ms | 3.1× | 5.7% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.48ms | 84.32ms | 3.3× | 6.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 24.73ms | 76.11ms | 3.1× | 8.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.42ms | 91.10ms | 2.8× | 6.4% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 34.09ms | 39.90ms | 1.2× | 1.5% | PASS |
| mem_reads | `oltp_range_select` | 15.46ms | 15.31ms | 1.0× | 2.1% | PASS |
| mem_reads | `oltp_sum_range` | 14.06ms | 15.22ms | 1.1× | 2.2% | PASS |
| mem_reads | `oltp_order_range` | 3.13ms | 3.36ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_distinct_range` | 4.17ms | 4.44ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 3.88ms | 6.71ms | 1.7× | 1.4% | PASS |
| mem_reads | `select_random_points` | 21.14ms | 22.98ms | 1.1× | 2.1% | PASS |
| mem_reads | `select_random_ranges` | 6.46ms | 6.91ms | 1.1× | 2.1% | PASS |
| mem_reads | `covering_index_scan` | 7.59ms | 10.81ms | 1.4× | 0.9% | PASS |
| mem_reads | `groupby_scan` | 33.46ms | 35.52ms | 1.1× | 0.9% | PASS |
| mem_reads | `index_join` | 9.98ms | 10.80ms | 1.1× | 1.6% | PASS |
| mem_reads | `index_join_scan` | 3.67ms | 6.60ms | 1.8× | 1.4% | PASS |
| mem_reads | `types_table_scan` | 1.05s | 1.30s | 1.2× | 0.6% | PASS |
| mem_reads | `table_scan` | 1.18s | 1.38s | 1.2× | 1.1% | PASS |
| mem_reads | `oltp_read_only` | 130.21ms | 143.47ms | 1.1× | 0.9% | PASS |
| mem_writes | `oltp_bulk_insert` | 232.25ms | 328.28ms | 1.4× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 17.91ms | 37.85ms | 2.1× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 61.11ms | 129.08ms | 2.1× | 0.6% | PASS |
| mem_writes | `oltp_update_non_index` | 45.93ms | 78.40ms | 1.7× | 1.4% | PASS |
| mem_writes | `oltp_delete_insert` | 53.84ms | 101.06ms | 1.9× | 2.1% | PASS |
| mem_writes | `oltp_write_only` | 29.77ms | 59.81ms | 2.0× | 1.9% | PASS |
| mem_writes | `types_delete_insert` | 37.93ms | 55.36ms | 1.5× | 1.9% | PASS |
| mem_writes | `oltp_read_write` | 96.80ms | 139.47ms | 1.4× | 2.1% | PASS |
| file_reads | `oltp_point_select` | 104.31ms | 58.89ms | 0.6× | 1.3% | PASS |
| file_reads | `oltp_range_select` | 23.53ms | 17.50ms | 0.7× | 1.1% | PASS |
| file_reads | `oltp_sum_range` | 22.46ms | 17.43ms | 0.8× | 1.4% | PASS |
| file_reads | `oltp_order_range` | 4.26ms | 3.65ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_distinct_range` | 5.32ms | 4.77ms | 0.9× | 0.7% | PASS |
| file_reads | `oltp_index_scan` | 11.30ms | 8.80ms | 0.8× | 1.2% | PASS |
| file_reads | `select_random_points` | 31.16ms | 26.44ms | 0.8× | 2.7% | PASS |
| file_reads | `select_random_ranges` | 14.06ms | 8.97ms | 0.6× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 15.16ms | 13.17ms | 0.9× | 1.1% | PASS |
| file_reads | `groupby_scan` | 34.85ms | 35.93ms | 1.0× | 0.8% | PASS |
| file_reads | `index_join` | 14.71ms | 12.23ms | 0.8× | 2.1% | PASS |
| file_reads | `index_join_scan` | 4.71ms | 6.97ms | 1.5× | 1.8% | PASS |
| file_reads | `types_table_scan` | 1.08s | 1.31s | 1.2× | 1.8% | PASS |
| file_reads | `table_scan` | 1.23s | 1.40s | 1.1× | 1.7% | PASS |
| file_reads | `oltp_read_only` | 245.04ms | 174.43ms | 0.7× | 0.7% | PASS |
| file_writes | `oltp_bulk_insert` | 254.66ms | 342.90ms | 1.3× | 1.3% | PASS |
| file_writes | `oltp_insert` | 25.02ms | 42.94ms | 1.7× | 1.1% | PASS |
| file_writes | `oltp_update_index` | 102.55ms | 157.88ms | 1.5× | 2.2% | PASS |
| file_writes | `oltp_update_non_index` | 92.95ms | 92.42ms | 1.0× | 12.3% | PASS |
| file_writes | `oltp_delete_insert` | 87.63ms | 114.30ms | 1.3× | 1.6% | PASS |
| file_writes | `oltp_write_only` | 57.23ms | 70.34ms | 1.2× | 1.4% | PASS |
| file_writes | `types_delete_insert` | 64.85ms | 66.25ms | 1.0× | 3.4% | PASS |
| file_writes | `oltp_read_write` | 126.81ms | 150.54ms | 1.2× | 1.2% | PASS |
| ac_reads | `oltp_point_select` | 57.17ms | 58.74ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 18.65ms | 17.40ms | 0.9× | 2.0% | PASS |
| ac_reads | `oltp_sum_range` | 17.37ms | 17.26ms | 1.0× | 1.8% | PASS |
| ac_reads | `oltp_order_range` | 3.75ms | 3.61ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_distinct_range` | 4.78ms | 4.73ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_index_scan` | 6.50ms | 8.64ms | 1.3× | 1.4% | PASS |
| ac_reads | `select_random_points` | 24.41ms | 25.75ms | 1.1× | 2.1% | PASS |
| ac_reads | `select_random_ranges` | 9.10ms | 8.93ms | 1.0× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 10.24ms | 13.11ms | 1.3× | 1.1% | PASS |
| ac_reads | `groupby_scan` | 34.04ms | 35.87ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 12.09ms | 12.11ms | 1.0× | 2.0% | PASS |
| ac_reads | `index_join_scan` | 4.13ms | 6.82ms | 1.7× | 1.3% | PASS |
| ac_reads | `types_table_scan` | 1.05s | 1.29s | 1.2× | 0.8% | PASS |
| ac_reads | `table_scan` | 1.21s | 1.39s | 1.2× | 2.7% | PASS |
| ac_reads | `oltp_read_only` | 174.56ms | 174.88ms | 1.0× | 3.4% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 23.21ms | 62.02ms | 2.7× | 7.4% | PASS |
| ac_writes | `oltp_insert_ac` | 25.56ms | 77.46ms | 3.0× | 8.1% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.90ms | 88.62ms | 3.3× | 5.8% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.66ms | 70.09ms | 3.0× | 6.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.79ms | 78.35ms | 3.2× | 3.7% | PASS |
| ac_writes | `oltp_write_only_ac` | 24.60ms | 78.06ms | 3.2× | 3.8% | PASS |
| ac_writes | `types_delete_insert_ac` | 23.54ms | 72.69ms | 3.1× | 6.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.77ms | 84.19ms | 2.6× | 5.8% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 25.71ms | 29.52ms | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_range_select` | 14.98ms | 16.95ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_sum_range` | 13.51ms | 16.39ms | 1.2× | 1.2% | PASS |
| mem_reads | `oltp_order_range` | 2.87ms | 3.15ms | 1.1× | 0.7% | PASS |
| mem_reads | `oltp_distinct_range` | 3.70ms | 4.07ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 3.57ms | 4.73ms | 1.3× | 1.3% | PASS |
| mem_reads | `select_random_points` | 21.21ms | 24.43ms | 1.2× | 1.7% | PASS |
| mem_reads | `select_random_ranges` | 5.76ms | 6.61ms | 1.1× | 1.0% | PASS |
| mem_reads | `covering_index_scan` | 5.84ms | 7.67ms | 1.3× | 1.0% | PASS |
| mem_reads | `groupby_scan` | 29.86ms | 33.84ms | 1.1× | 0.9% | PASS |
| mem_reads | `index_join` | 6.12ms | 8.69ms | 1.4× | 3.0% | PASS |
| mem_reads | `index_join_scan` | 3.12ms | 5.15ms | 1.6× | 1.6% | PASS |
| mem_reads | `types_table_scan` | 878.01ms | 1.03s | 1.2× | 1.6% | PASS |
| mem_reads | `table_scan` | 1.03s | 1.13s | 1.1× | 2.4% | PASS |
| mem_reads | `oltp_read_only` | 113.55ms | 132.62ms | 1.2× | 0.5% | PASS |
| mem_writes | `oltp_bulk_insert` | 187.50ms | 237.18ms | 1.3× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 14.70ms | 24.51ms | 1.7× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 52.25ms | 87.21ms | 1.7× | 1.2% | PASS |
| mem_writes | `oltp_update_non_index` | 39.54ms | 57.97ms | 1.5× | 1.3% | PASS |
| mem_writes | `oltp_delete_insert` | 38.39ms | 65.39ms | 1.7× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 21.16ms | 38.31ms | 1.8× | 1.0% | PASS |
| mem_writes | `types_delete_insert` | 25.00ms | 37.15ms | 1.5× | 1.4% | PASS |
| mem_writes | `oltp_read_write` | 74.91ms | 109.10ms | 1.5× | 1.5% | PASS |
| file_reads | `oltp_point_select` | 91.41ms | 46.52ms | 0.5× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 22.16ms | 18.95ms | 0.9× | 0.7% | PASS |
| file_reads | `oltp_sum_range` | 20.50ms | 18.44ms | 0.9× | 0.9% | PASS |
| file_reads | `oltp_order_range` | 3.69ms | 3.43ms | 0.9× | 0.7% | PASS |
| file_reads | `oltp_distinct_range` | 4.50ms | 4.32ms | 1.0× | 0.6% | PASS |
| file_reads | `oltp_index_scan` | 10.51ms | 6.81ms | 0.6× | 0.9% | PASS |
| file_reads | `select_random_points` | 29.37ms | 26.83ms | 0.9× | 2.0% | PASS |
| file_reads | `select_random_ranges` | 12.72ms | 8.67ms | 0.7× | 0.9% | PASS |
| file_reads | `covering_index_scan` | 13.01ms | 9.86ms | 0.8× | 1.0% | PASS |
| file_reads | `groupby_scan` | 31.01ms | 34.30ms | 1.1× | 0.7% | PASS |
| file_reads | `index_join` | 9.88ms | 10.05ms | 1.0× | 1.2% | PASS |
| file_reads | `index_join_scan` | 3.89ms | 5.43ms | 1.4× | 1.0% | PASS |
| file_reads | `types_table_scan` | 862.89ms | 1.02s | 1.2× | 0.4% | PASS |
| file_reads | `table_scan` | 990.38ms | 1.12s | 1.1× | 0.5% | PASS |
| file_reads | `oltp_read_only` | 208.44ms | 156.66ms | 0.8× | 0.5% | PASS |
| file_writes | `oltp_bulk_insert` | 238.40ms | 287.78ms | 1.2× | 2.3% | PASS |
| file_writes | `oltp_insert` | 31.71ms | 40.00ms | 1.3× | 6.8% | PASS |
| file_writes | `oltp_update_index` | 161.66ms | 153.88ms | 1.0× | 2.6% | PASS |
| file_writes | `oltp_update_non_index` | 134.91ms | 107.64ms | 0.8× | 2.9% | PASS |
| file_writes | `oltp_delete_insert` | 133.12ms | 122.73ms | 0.9× | 7.9% | PASS |
| file_writes | `oltp_write_only` | 96.87ms | 83.98ms | 0.9× | 5.1% | PASS |
| file_writes | `types_delete_insert` | 86.51ms | 72.28ms | 0.8× | 5.6% | PASS |
| file_writes | `oltp_read_write` | 156.88ms | 159.48ms | 1.0× | 2.3% | PASS |
| ac_reads | `oltp_point_select` | 48.92ms | 47.76ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_range_select` | 17.87ms | 18.93ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_sum_range` | 16.15ms | 18.47ms | 1.1× | 0.7% | PASS |
| ac_reads | `oltp_order_range` | 3.28ms | 3.42ms | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_distinct_range` | 4.08ms | 4.32ms | 1.1× | 0.8% | PASS |
| ac_reads | `oltp_index_scan` | 6.18ms | 6.78ms | 1.1× | 1.0% | PASS |
| ac_reads | `select_random_points` | 24.04ms | 26.28ms | 1.1× | 0.8% | PASS |
| ac_reads | `select_random_ranges` | 8.23ms | 8.60ms | 1.0× | 0.9% | PASS |
| ac_reads | `covering_index_scan` | 8.56ms | 9.83ms | 1.1× | 0.7% | PASS |
| ac_reads | `groupby_scan` | 30.10ms | 34.04ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 7.69ms | 10.01ms | 1.3× | 0.8% | PASS |
| ac_reads | `index_join_scan` | 3.54ms | 5.43ms | 1.5× | 1.2% | PASS |
| ac_reads | `types_table_scan` | 865.60ms | 1.02s | 1.2× | 0.5% | PASS |
| ac_reads | `table_scan` | 990.76ms | 1.12s | 1.1× | 0.5% | PASS |
| ac_reads | `oltp_read_only` | 145.99ms | 157.18ms | 1.1× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 18.65ms | 52.13ms | 2.8× | 8.8% | PASS |
| ac_writes | `oltp_insert_ac` | 19.87ms | 65.67ms | 3.3× | 9.2% | PASS |
| ac_writes | `oltp_update_index_ac` | 20.93ms | 71.58ms | 3.4× | 7.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 18.91ms | 59.64ms | 3.2× | 12.2% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 20.51ms | 66.01ms | 3.2× | 13.5% | PASS |
| ac_writes | `oltp_write_only_ac` | 20.90ms | 77.37ms | 3.7× | 33.0% | PASS |
| ac_writes | `types_delete_insert_ac` | 18.43ms | 65.02ms | 3.5× | 13.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 26.43ms | 84.25ms | 3.2× | 10.7% | PASS |

</details>

</details>

## Version-control latency

Wall time: 5m 34s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 22.78ms | 38.00ms | 59.9% | 2.2% | PASS |
| `status_dirty_many_tables` | 25.38ms | 42.00ms | 60.4% | 2.0% | PASS |
| `diff_regular_working_one_table` | 24.68ms | 33.00ms | 74.8% | 2.3% | PASS |
| `diff_regular_working_many_tables` | 26.98ms | 50.00ms | 54.0% | 1.4% | PASS |
| `diff_stat_working_many_tables` | 26.89ms | 48.00ms | 56.0% | 2.0% | PASS |
| `diff_schema_working_many_tables` | 26.50ms | 48.00ms | 55.2% | 1.4% | PASS |
| `branch_list_many_branches` | 15.53ms | 25.00ms | 62.1% | 2.6% | PASS |
| `branch_create_delete` | 16.28ms | 27.00ms | 60.3% | 2.5% | PASS |
| `at_literal_deep_history` | 17.89ms | 28.00ms | 63.9% | 1.9% | PASS |
| `diff_literal_deep_history` | 18.30ms | 28.00ms | 65.3% | 3.4% | PASS |
| `history_literal_deep_history` | 19.21ms | 30.00ms | 64.0% | 3.3% | PASS |
| `checkout_branch_clean` | 24.11ms | 43.00ms | 56.1% | 4.0% | PASS |
| `merge_data_no_conflicts` | 21.08ms | 33.00ms | 63.9% | 2.2% | PASS |
| `merge_data_secondary_index` | 636.18ms | 944.00ms | 67.4% | 3.9% | PASS |
| `merge_schema_no_conflicts` | 15.10ms | 24.00ms | 62.9% | 2.4% | PASS |
| `merge_data_conflicts` | 21.41ms | 33.00ms | 64.9% | 1.3% | PASS |
| `merge_data_conflicts_with_resolve` | 22.04ms | 34.00ms | 64.8% | 1.2% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
