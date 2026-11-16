# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-11-16 11:34 UTC
>
> Commit: [`456a2a09b61c23760d216d5db1295df0805b5834`](https://github.com/dolthub/doltlite/commit/456a2a09b61c23760d216d5db1295df0805b5834)
>
> Runner: ubuntu24 20261111.370.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/43841789904)

The SQL workloads compare optimized DoltLite against stock SQLite on the same GitHub-hosted runner, alternating execution order on each repetition. Version-control sections measure DoltLite latency. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 8.06s | 9.09s | 1.13× | 1.3% | **PASS** |
| Writes | 1.63s | 2.46s | 1.51× | 1.0% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 8.68s | 9.18s | 1.06× | 1.0% | **PASS** |
| Writes | 3.55s | 3.62s | 1.02× | 5.3% | **PASS** |
| Autocommit writes | 750.60ms | 1.48s | 1.97× | 20.9% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.8× for a section average. Durable autocommit writes use 3.6× and 2.6× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 1.81s | 2.02s | 1.12× | 1.2% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.12s | 2.41s | 1.14× | 1.4% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 1.82s | 2.16s | 1.19× | 1.2% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.31s | 2.49s | 1.08× | 2.0% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 291.46ms | 468.33ms | 1.61× | 1.0% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 470.91ms | 695.32ms | 1.48× | 1.0% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 379.68ms | 571.23ms | 1.50× | 0.7% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 490.48ms | 724.50ms | 1.48× | 1.6% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 1.93s | 2.02s | 1.05× | 1.5% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.31s | 2.44s | 1.06× | 0.9% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 1.90s | 2.18s | 1.14× | 0.8% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.55s | 2.55s | 1.00× | 1.3% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 362.92ms | 501.67ms | 1.38× | 1.9% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.48s | 1.29s | 0.87× | 26.8% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 489.63ms | 653.48ms | 1.33× | 1.8% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 1.22s | 1.17s | 0.96× | 11.5% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 1.83s | 2.02s | 1.10× | 1.3% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.18s | 2.44s | 1.12× | 0.9% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 1.87s | 2.19s | 1.17× | 1.0% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.60s | 2.63s | 1.01× | 1.3% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 76.66ms | 162.34ms | 2.12× | 2.2% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 304.48ms | 587.97ms | 1.93× | 61.7% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 83.12ms | 172.40ms | 2.07× | 2.2% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 286.34ms | 556.14ms | 1.94× | 46.1% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 15.85ms | 17.87ms | 1.1× | 0.7% | PASS |
| mem_reads | `oltp_range_select` | 7.95ms | 8.68ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_sum_range` | 7.29ms | 8.15ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_order_range` | 1.98ms | 2.16ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_distinct_range` | 2.64ms | 2.87ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 2.78ms | 3.46ms | 1.2× | 1.6% | PASS |
| mem_reads | `select_random_points` | 10.47ms | 9.46ms | 0.9× | 1.3% | PASS |
| mem_reads | `select_random_ranges` | 3.13ms | 3.25ms | 1.0× | 1.0% | PASS |
| mem_reads | `covering_index_scan` | 4.78ms | 6.77ms | 1.4× | 0.5% | PASS |
| mem_reads | `groupby_scan` | 21.20ms | 23.00ms | 1.1× | 0.7% | PASS |
| mem_reads | `index_join` | 4.22ms | 6.09ms | 1.4× | 1.4% | PASS |
| mem_reads | `index_join_scan` | 2.39ms | 4.42ms | 1.8× | 2.6% | PASS |
| mem_reads | `types_table_scan` | 781.83ms | 867.45ms | 1.1× | 2.0% | PASS |
| mem_reads | `table_scan` | 869.01ms | 973.20ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_read_only` | 76.67ms | 84.46ms | 1.1× | 0.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 109.18ms | 168.72ms | 1.5× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 9.79ms | 16.08ms | 1.6× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 36.35ms | 68.16ms | 1.9× | 1.3% | PASS |
| mem_writes | `oltp_update_non_index` | 23.34ms | 38.72ms | 1.7× | 1.1% | PASS |
| mem_writes | `oltp_delete_insert` | 32.34ms | 49.80ms | 1.5× | 0.8% | PASS |
| mem_writes | `oltp_write_only` | 15.73ms | 31.41ms | 2.0× | 1.4% | PASS |
| mem_writes | `types_delete_insert` | 17.05ms | 22.50ms | 1.3× | 1.0% | PASS |
| mem_writes | `oltp_read_write` | 47.67ms | 72.95ms | 1.5× | 1.2% | PASS |
| file_reads | `oltp_point_select` | 37.00ms | 22.87ms | 0.6× | 0.7% | PASS |
| file_reads | `oltp_range_select` | 10.64ms | 9.48ms | 0.9× | 1.1% | PASS |
| file_reads | `oltp_sum_range` | 9.73ms | 9.34ms | 1.0× | 0.9% | PASS |
| file_reads | `oltp_order_range` | 2.23ms | 2.29ms | 1.0× | 1.8% | PASS |
| file_reads | `oltp_distinct_range` | 2.86ms | 3.02ms | 1.1× | 2.7% | PASS |
| file_reads | `oltp_index_scan` | 5.00ms | 4.40ms | 0.9× | 2.2% | PASS |
| file_reads | `select_random_points` | 13.19ms | 10.55ms | 0.8× | 1.5% | PASS |
| file_reads | `select_random_ranges` | 5.30ms | 3.85ms | 0.7× | 1.5% | PASS |
| file_reads | `covering_index_scan` | 6.93ms | 7.43ms | 1.1× | 0.7% | PASS |
| file_reads | `groupby_scan` | 21.51ms | 23.25ms | 1.1× | 0.6% | PASS |
| file_reads | `index_join` | 5.48ms | 6.87ms | 1.3× | 1.6% | PASS |
| file_reads | `index_join_scan` | 2.68ms | 4.62ms | 1.7× | 3.5% | PASS |
| file_reads | `types_table_scan` | 770.84ms | 864.45ms | 1.1× | 0.8% | PASS |
| file_reads | `table_scan` | 929.92ms | 958.20ms | 1.0× | 7.0% | PASS |
| file_reads | `oltp_read_only` | 102.66ms | 90.51ms | 0.9× | 1.6% | PASS |
| file_writes | `oltp_bulk_insert` | 118.17ms | 176.09ms | 1.5× | 1.1% | PASS |
| file_writes | `oltp_insert` | 12.89ms | 18.12ms | 1.4× | 1.4% | PASS |
| file_writes | `oltp_update_index` | 48.13ms | 70.29ms | 1.5× | 1.7% | PASS |
| file_writes | `oltp_update_non_index` | 34.06ms | 42.53ms | 1.2× | 2.1% | PASS |
| file_writes | `oltp_delete_insert` | 41.53ms | 57.32ms | 1.4× | 1.7% | PASS |
| file_writes | `oltp_write_only` | 25.68ms | 34.14ms | 1.3× | 2.2% | PASS |
| file_writes | `types_delete_insert` | 24.04ms | 26.77ms | 1.1× | 2.0% | PASS |
| file_writes | `oltp_read_write` | 58.42ms | 76.41ms | 1.3× | 2.1% | PASS |
| ac_reads | `oltp_point_select` | 22.83ms | 22.68ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 9.28ms | 9.46ms | 1.0× | 1.8% | PASS |
| ac_reads | `oltp_sum_range` | 8.33ms | 9.31ms | 1.1× | 1.0% | PASS |
| ac_reads | `oltp_order_range` | 2.16ms | 2.29ms | 1.1× | 2.6% | PASS |
| ac_reads | `oltp_distinct_range` | 2.85ms | 2.99ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_index_scan` | 3.67ms | 4.24ms | 1.2× | 1.6% | PASS |
| ac_reads | `select_random_points` | 11.91ms | 10.29ms | 0.9× | 1.3% | PASS |
| ac_reads | `select_random_ranges` | 3.94ms | 3.85ms | 1.0× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 5.59ms | 7.37ms | 1.3× | 0.8% | PASS |
| ac_reads | `groupby_scan` | 21.55ms | 23.31ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 4.81ms | 6.57ms | 1.4× | 1.5% | PASS |
| ac_reads | `index_join_scan` | 2.63ms | 4.59ms | 1.7× | 2.6% | PASS |
| ac_reads | `types_table_scan` | 762.76ms | 862.21ms | 1.1× | 0.4% | PASS |
| ac_reads | `table_scan` | 881.66ms | 956.99ms | 1.1× | 1.6% | PASS |
| ac_reads | `oltp_read_only` | 84.08ms | 89.25ms | 1.1× | 0.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 8.54ms | 13.24ms | 1.6× | 2.3% | PASS |
| ac_writes | `oltp_insert_ac` | 9.55ms | 19.74ms | 2.1× | 2.2% | PASS |
| ac_writes | `oltp_update_index_ac` | 10.57ms | 27.76ms | 2.6× | 2.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 8.65ms | 16.15ms | 1.9× | 2.7% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 9.46ms | 21.91ms | 2.3× | 1.9% | PASS |
| ac_writes | `oltp_write_only_ac` | 9.58ms | 21.70ms | 2.3× | 2.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 8.35ms | 16.33ms | 2.0× | 1.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 11.96ms | 25.50ms | 2.1× | 1.5% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 28.22ms | 29.10ms | 1.0× | 1.3% | PASS |
| mem_reads | `oltp_range_select` | 13.86ms | 12.37ms | 0.9× | 3.3% | PASS |
| mem_reads | `oltp_sum_range` | 12.37ms | 12.04ms | 1.0× | 1.8% | PASS |
| mem_reads | `oltp_order_range` | 2.93ms | 2.85ms | 1.0× | 1.5% | PASS |
| mem_reads | `oltp_distinct_range` | 3.78ms | 3.78ms | 1.0× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 3.29ms | 5.19ms | 1.6× | 1.2% | PASS |
| mem_reads | `select_random_points` | 19.22ms | 16.81ms | 0.9× | 5.0% | PASS |
| mem_reads | `select_random_ranges` | 5.79ms | 5.06ms | 0.9× | 1.4% | PASS |
| mem_reads | `covering_index_scan` | 6.03ms | 8.58ms | 1.4× | 0.8% | PASS |
| mem_reads | `groupby_scan` | 29.55ms | 29.63ms | 1.0× | 0.9% | PASS |
| mem_reads | `index_join` | 9.75ms | 8.21ms | 0.8× | 2.1% | PASS |
| mem_reads | `index_join_scan` | 3.24ms | 5.43ms | 1.7× | 3.4% | PASS |
| mem_reads | `types_table_scan` | 889.47ms | 1.04s | 1.2× | 1.4% | PASS |
| mem_reads | `table_scan` | 983.94ms | 1.12s | 1.1× | 0.4% | PASS |
| mem_reads | `oltp_read_only` | 109.66ms | 108.76ms | 1.0× | 0.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 190.85ms | 253.57ms | 1.3× | 1.1% | PASS |
| mem_writes | `oltp_insert` | 14.31ms | 26.42ms | 1.8× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 52.57ms | 102.63ms | 2.0× | 0.9% | PASS |
| mem_writes | `oltp_update_non_index` | 39.87ms | 57.69ms | 1.4× | 1.1% | PASS |
| mem_writes | `oltp_delete_insert` | 44.42ms | 75.25ms | 1.7× | 0.9% | PASS |
| mem_writes | `oltp_write_only` | 23.06ms | 39.43ms | 1.7× | 0.8% | PASS |
| mem_writes | `types_delete_insert` | 31.33ms | 43.78ms | 1.4× | 1.2% | PASS |
| mem_writes | `oltp_read_write` | 74.49ms | 96.55ms | 1.3× | 0.8% | PASS |
| file_reads | `oltp_point_select` | 89.45ms | 45.24ms | 0.5× | 0.7% | PASS |
| file_reads | `oltp_range_select` | 20.47ms | 14.03ms | 0.7× | 1.0% | PASS |
| file_reads | `oltp_sum_range` | 18.89ms | 13.78ms | 0.7× | 0.9% | PASS |
| file_reads | `oltp_order_range` | 3.63ms | 3.04ms | 0.8× | 1.5% | PASS |
| file_reads | `oltp_distinct_range` | 4.46ms | 3.97ms | 0.9× | 1.0% | PASS |
| file_reads | `oltp_index_scan` | 9.59ms | 6.91ms | 0.7× | 0.9% | PASS |
| file_reads | `select_random_points` | 24.48ms | 18.28ms | 0.7× | 0.7% | PASS |
| file_reads | `select_random_ranges` | 11.94ms | 6.70ms | 0.6× | 0.9% | PASS |
| file_reads | `covering_index_scan` | 12.48ms | 10.15ms | 0.8× | 0.6% | PASS |
| file_reads | `groupby_scan` | 29.66ms | 29.64ms | 1.0× | 0.8% | PASS |
| file_reads | `index_join` | 12.73ms | 9.05ms | 0.7× | 1.1% | PASS |
| file_reads | `index_join_scan` | 3.91ms | 5.54ms | 1.4× | 1.4% | PASS |
| file_reads | `types_table_scan` | 873.14ms | 1.02s | 1.2× | 0.5% | PASS |
| file_reads | `table_scan` | 993.14ms | 1.12s | 1.1× | 0.4% | PASS |
| file_reads | `oltp_read_only` | 198.95ms | 132.40ms | 0.7× | 0.7% | PASS |
| file_writes | `oltp_bulk_insert` | 283.76ms | 351.87ms | 1.2× | 8.5% | PASS |
| file_writes | `oltp_insert` | 36.38ms | 55.35ms | 1.5× | 26.8% | PASS |
| file_writes | `oltp_update_index` | 202.42ms | 198.79ms | 1.0× | 27.9% | PASS |
| file_writes | `oltp_update_non_index` | 171.21ms | 141.39ms | 0.8× | 19.5% | PASS |
| file_writes | `oltp_delete_insert` | 200.53ms | 162.88ms | 0.8× | 17.7% | PASS |
| file_writes | `oltp_write_only` | 175.31ms | 98.01ms | 0.6× | 31.2% | PASS |
| file_writes | `types_delete_insert` | 171.83ms | 98.08ms | 0.6× | 32.5% | PASS |
| file_writes | `oltp_read_write` | 238.21ms | 184.51ms | 0.8× | 26.7% | PASS |
| ac_reads | `oltp_point_select` | 49.19ms | 45.35ms | 0.9× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 16.64ms | 14.10ms | 0.8× | 1.0% | PASS |
| ac_reads | `oltp_sum_range` | 15.17ms | 13.91ms | 0.9× | 0.9% | PASS |
| ac_reads | `oltp_order_range` | 3.32ms | 3.05ms | 0.9× | 0.7% | PASS |
| ac_reads | `oltp_distinct_range` | 4.11ms | 3.97ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_index_scan` | 5.75ms | 6.94ms | 1.2× | 0.9% | PASS |
| ac_reads | `select_random_points` | 20.86ms | 18.43ms | 0.9× | 1.4% | PASS |
| ac_reads | `select_random_ranges` | 8.06ms | 6.72ms | 0.8× | 0.9% | PASS |
| ac_reads | `covering_index_scan` | 8.51ms | 10.18ms | 1.2× | 1.0% | PASS |
| ac_reads | `groupby_scan` | 29.35ms | 29.64ms | 1.0× | 0.7% | PASS |
| ac_reads | `index_join` | 10.91ms | 9.07ms | 0.8× | 1.1% | PASS |
| ac_reads | `index_join_scan` | 3.61ms | 5.63ms | 1.6× | 2.1% | PASS |
| ac_reads | `types_table_scan` | 873.90ms | 1.02s | 1.2× | 0.4% | PASS |
| ac_reads | `table_scan` | 992.58ms | 1.12s | 1.1× | 0.4% | PASS |
| ac_reads | `oltp_read_only` | 140.63ms | 132.33ms | 0.9× | 0.6% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 41.43ms | 33.98ms | 0.8× | 65.5% | PASS |
| ac_writes | `oltp_insert_ac` | 38.67ms | 64.53ms | 1.7× | 62.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 36.20ms | 110.29ms | 3.0× | 47.0% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 37.64ms | 56.82ms | 1.5× | 60.5% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 30.64ms | 70.07ms | 2.3× | 36.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 33.88ms | 71.45ms | 2.1× | 46.7% | PASS |
| ac_writes | `types_delete_insert_ac` | 36.03ms | 58.42ms | 1.6× | 67.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 49.99ms | 122.41ms | 2.4× | 66.5% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 23.19ms | 22.55ms | 1.0× | 2.6% | PASS |
| mem_reads | `oltp_range_select` | 10.88ms | 10.81ms | 1.0× | 1.7% | PASS |
| mem_reads | `oltp_sum_range` | 10.93ms | 10.53ms | 1.0× | 1.6% | PASS |
| mem_reads | `oltp_order_range` | 2.35ms | 2.45ms | 1.0× | 1.4% | PASS |
| mem_reads | `oltp_distinct_range` | 3.04ms | 3.20ms | 1.1× | 0.7% | PASS |
| mem_reads | `oltp_index_scan` | 2.89ms | 4.60ms | 1.6× | 1.2% | PASS |
| mem_reads | `select_random_points` | 17.59ms | 16.52ms | 0.9× | 1.0% | PASS |
| mem_reads | `select_random_ranges` | 4.65ms | 4.41ms | 0.9× | 1.2% | PASS |
| mem_reads | `covering_index_scan` | 4.80ms | 7.21ms | 1.5× | 0.4% | PASS |
| mem_reads | `groupby_scan` | 23.18ms | 24.59ms | 1.1× | 0.3% | PASS |
| mem_reads | `index_join` | 9.33ms | 8.25ms | 0.9× | 1.6% | PASS |
| mem_reads | `index_join_scan` | 2.71ms | 5.36ms | 2.0× | 1.5% | PASS |
| mem_reads | `types_table_scan` | 764.90ms | 927.47ms | 1.2× | 0.4% | PASS |
| mem_reads | `table_scan` | 849.98ms | 1.02s | 1.2× | 0.6% | PASS |
| mem_reads | `oltp_read_only` | 91.65ms | 94.58ms | 1.0× | 0.4% | PASS |
| mem_writes | `oltp_bulk_insert` | 144.98ms | 195.55ms | 1.3× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 11.77ms | 22.07ms | 1.9× | 0.4% | PASS |
| mem_writes | `oltp_update_index` | 44.98ms | 86.96ms | 1.9× | 1.0% | PASS |
| mem_writes | `oltp_update_non_index` | 31.32ms | 47.12ms | 1.5× | 2.1% | PASS |
| mem_writes | `oltp_delete_insert` | 36.74ms | 62.69ms | 1.7× | 0.6% | PASS |
| mem_writes | `oltp_write_only` | 19.53ms | 33.57ms | 1.7× | 1.0% | PASS |
| mem_writes | `types_delete_insert` | 26.00ms | 36.96ms | 1.4× | 0.8% | PASS |
| mem_writes | `oltp_read_write` | 64.36ms | 86.33ms | 1.3× | 0.7% | PASS |
| file_reads | `oltp_point_select` | 44.93ms | 28.63ms | 0.6× | 0.7% | PASS |
| file_reads | `oltp_range_select` | 13.31ms | 11.71ms | 0.9× | 0.7% | PASS |
| file_reads | `oltp_sum_range` | 13.10ms | 11.21ms | 0.9× | 0.6% | PASS |
| file_reads | `oltp_order_range` | 2.62ms | 2.56ms | 1.0× | 1.0% | PASS |
| file_reads | `oltp_distinct_range` | 3.27ms | 3.26ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 4.97ms | 5.17ms | 1.0× | 1.1% | PASS |
| file_reads | `select_random_points` | 20.15ms | 17.07ms | 0.8× | 0.9% | PASS |
| file_reads | `select_random_ranges` | 6.80ms | 4.89ms | 0.7× | 1.3% | PASS |
| file_reads | `covering_index_scan` | 6.92ms | 7.72ms | 1.1× | 0.5% | PASS |
| file_reads | `groupby_scan` | 23.18ms | 24.55ms | 1.1× | 0.5% | PASS |
| file_reads | `index_join` | 9.82ms | 7.78ms | 0.8× | 2.1% | PASS |
| file_reads | `index_join_scan` | 2.77ms | 5.27ms | 1.9× | 2.6% | PASS |
| file_reads | `types_table_scan` | 764.27ms | 928.51ms | 1.2× | 0.3% | PASS |
| file_reads | `table_scan` | 866.47ms | 1.02s | 1.2× | 0.5% | PASS |
| file_reads | `oltp_read_only` | 117.98ms | 99.98ms | 0.8× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 156.93ms | 205.46ms | 1.3× | 0.8% | PASS |
| file_writes | `oltp_insert` | 15.29ms | 26.09ms | 1.7× | 2.0% | PASS |
| file_writes | `oltp_update_index` | 61.25ms | 102.62ms | 1.7× | 2.3% | PASS |
| file_writes | `oltp_update_non_index` | 46.86ms | 57.46ms | 1.2× | 1.8% | PASS |
| file_writes | `oltp_delete_insert` | 55.03ms | 78.41ms | 1.4× | 2.3% | PASS |
| file_writes | `oltp_write_only` | 34.37ms | 43.55ms | 1.3× | 1.8% | PASS |
| file_writes | `types_delete_insert` | 39.29ms | 42.94ms | 1.1× | 1.7% | PASS |
| file_writes | `oltp_read_write` | 80.61ms | 96.95ms | 1.2× | 1.0% | PASS |
| ac_reads | `oltp_point_select` | 31.66ms | 28.89ms | 0.9× | 0.8% | PASS |
| ac_reads | `oltp_range_select` | 12.31ms | 11.74ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_sum_range` | 11.94ms | 11.16ms | 0.9× | 1.6% | PASS |
| ac_reads | `oltp_order_range` | 2.51ms | 2.57ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_distinct_range` | 3.19ms | 3.27ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 3.73ms | 5.16ms | 1.4× | 1.4% | PASS |
| ac_reads | `select_random_points` | 19.17ms | 17.27ms | 0.9× | 1.0% | PASS |
| ac_reads | `select_random_ranges` | 5.61ms | 4.89ms | 0.9× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 5.56ms | 7.73ms | 1.4× | 0.6% | PASS |
| ac_reads | `groupby_scan` | 23.26ms | 24.80ms | 1.1× | 0.3% | PASS |
| ac_reads | `index_join` | 9.96ms | 8.22ms | 0.8× | 1.8% | PASS |
| ac_reads | `index_join_scan` | 2.90ms | 5.40ms | 1.9× | 2.1% | PASS |
| ac_reads | `types_table_scan` | 767.63ms | 931.75ms | 1.2× | 0.4% | PASS |
| ac_reads | `table_scan` | 871.96ms | 1.02s | 1.2× | 0.5% | PASS |
| ac_reads | `oltp_read_only` | 102.00ms | 101.87ms | 1.0× | 0.6% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 9.35ms | 12.95ms | 1.4× | 2.5% | PASS |
| ac_writes | `oltp_insert_ac` | 10.43ms | 22.63ms | 2.2× | 2.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 10.69ms | 30.64ms | 2.9× | 2.4% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 8.90ms | 16.19ms | 1.8× | 1.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 10.08ms | 22.69ms | 2.3× | 1.0% | PASS |
| ac_writes | `oltp_write_only_ac` | 10.53ms | 23.05ms | 2.2× | 5.5% | PASS |
| ac_writes | `types_delete_insert_ac` | 9.42ms | 16.99ms | 1.8× | 1.2% | PASS |
| ac_writes | `oltp_read_write_ac` | 13.72ms | 27.26ms | 2.0× | 2.4% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 26.91ms | 29.73ms | 1.1× | 2.2% | PASS |
| mem_reads | `oltp_range_select` | 16.08ms | 18.33ms | 1.1× | 2.4% | PASS |
| mem_reads | `oltp_sum_range` | 14.53ms | 17.68ms | 1.2× | 2.4% | PASS |
| mem_reads | `oltp_order_range` | 3.03ms | 3.34ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 3.86ms | 4.36ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 3.77ms | 4.99ms | 1.3× | 2.2% | PASS |
| mem_reads | `select_random_points` | 22.28ms | 23.03ms | 1.0× | 1.6% | PASS |
| mem_reads | `select_random_ranges` | 6.00ms | 6.67ms | 1.1× | 1.1% | PASS |
| mem_reads | `covering_index_scan` | 5.93ms | 8.11ms | 1.4× | 1.2% | PASS |
| mem_reads | `groupby_scan` | 30.77ms | 33.69ms | 1.1× | 0.9% | PASS |
| mem_reads | `index_join` | 6.21ms | 8.55ms | 1.4× | 2.6% | PASS |
| mem_reads | `index_join_scan` | 3.22ms | 5.14ms | 1.6× | 2.0% | PASS |
| mem_reads | `types_table_scan` | 872.94ms | 1.02s | 1.2× | 1.0% | PASS |
| mem_reads | `table_scan` | 1.16s | 1.17s | 1.0× | 3.7% | PASS |
| mem_reads | `oltp_read_only` | 126.63ms | 140.38ms | 1.1× | 2.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 195.63ms | 253.09ms | 1.3× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 15.29ms | 25.62ms | 1.7× | 1.3% | PASS |
| mem_writes | `oltp_update_index` | 57.03ms | 99.98ms | 1.8× | 1.8% | PASS |
| mem_writes | `oltp_update_non_index` | 44.35ms | 66.16ms | 1.5× | 2.0% | PASS |
| mem_writes | `oltp_delete_insert` | 41.99ms | 73.15ms | 1.7× | 1.4% | PASS |
| mem_writes | `oltp_write_only` | 23.40ms | 45.19ms | 1.9× | 1.3% | PASS |
| mem_writes | `types_delete_insert` | 27.82ms | 41.50ms | 1.5× | 2.3% | PASS |
| mem_writes | `oltp_read_write` | 84.97ms | 119.81ms | 1.4× | 2.2% | PASS |
| file_reads | `oltp_point_select` | 88.12ms | 46.08ms | 0.5× | 1.5% | PASS |
| file_reads | `oltp_range_select` | 23.11ms | 20.73ms | 0.9× | 1.6% | PASS |
| file_reads | `oltp_sum_range` | 21.02ms | 19.48ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_order_range` | 3.75ms | 3.56ms | 0.9× | 0.9% | PASS |
| file_reads | `oltp_distinct_range` | 4.59ms | 4.53ms | 1.0× | 1.2% | PASS |
| file_reads | `oltp_index_scan` | 10.14ms | 6.80ms | 0.7× | 0.7% | PASS |
| file_reads | `select_random_points` | 29.41ms | 24.90ms | 0.8× | 2.2% | PASS |
| file_reads | `select_random_ranges` | 12.30ms | 8.43ms | 0.7× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 12.51ms | 10.07ms | 0.8× | 1.1% | PASS |
| file_reads | `groupby_scan` | 31.05ms | 33.77ms | 1.1× | 0.7% | PASS |
| file_reads | `index_join` | 9.80ms | 9.87ms | 1.0× | 1.4% | PASS |
| file_reads | `index_join_scan` | 3.99ms | 5.47ms | 1.4× | 2.0% | PASS |
| file_reads | `types_table_scan` | 888.00ms | 1.03s | 1.2× | 1.3% | PASS |
| file_reads | `table_scan` | 1.19s | 1.16s | 1.0× | 2.5% | PASS |
| file_reads | `oltp_read_only` | 217.99ms | 167.63ms | 0.8× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 262.93ms | 326.92ms | 1.2× | 8.3% | PASS |
| file_writes | `oltp_insert` | 45.66ms | 44.88ms | 1.0× | 30.2% | PASS |
| file_writes | `oltp_update_index` | 175.66ms | 181.70ms | 1.0× | 11.8% | PASS |
| file_writes | `oltp_update_non_index` | 185.66ms | 122.75ms | 0.7× | 23.2% | PASS |
| file_writes | `oltp_delete_insert` | 153.80ms | 137.15ms | 0.9× | 11.0% | PASS |
| file_writes | `oltp_write_only` | 118.96ms | 94.67ms | 0.8× | 21.4% | PASS |
| file_writes | `types_delete_insert` | 91.72ms | 84.05ms | 0.9× | 9.8% | PASS |
| file_writes | `oltp_read_write` | 187.10ms | 177.19ms | 0.9× | 11.2% | PASS |
| ac_reads | `oltp_point_select` | 49.83ms | 47.76ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_range_select` | 19.99ms | 21.51ms | 1.1× | 2.4% | PASS |
| ac_reads | `oltp_sum_range` | 17.73ms | 20.13ms | 1.1× | 2.0% | PASS |
| ac_reads | `oltp_order_range` | 3.42ms | 3.59ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_distinct_range` | 4.28ms | 4.60ms | 1.1× | 1.7% | PASS |
| ac_reads | `oltp_index_scan` | 6.43ms | 7.05ms | 1.1× | 2.1% | PASS |
| ac_reads | `select_random_points` | 26.74ms | 26.06ms | 1.0× | 1.9% | PASS |
| ac_reads | `select_random_ranges` | 8.50ms | 8.60ms | 1.0× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 8.75ms | 10.36ms | 1.2× | 1.3% | PASS |
| ac_reads | `groupby_scan` | 31.38ms | 34.59ms | 1.1× | 1.0% | PASS |
| ac_reads | `index_join` | 8.04ms | 10.67ms | 1.3× | 2.6% | PASS |
| ac_reads | `index_join_scan` | 3.75ms | 5.81ms | 1.6× | 4.1% | PASS |
| ac_reads | `types_table_scan` | 996.00ms | 1.07s | 1.1× | 1.2% | PASS |
| ac_reads | `table_scan` | 1.25s | 1.19s | 0.9× | 0.7% | PASS |
| ac_reads | `oltp_read_only` | 162.44ms | 170.45ms | 1.0× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 30.41ms | 32.70ms | 1.1× | 70.6% | PASS |
| ac_writes | `oltp_insert_ac` | 30.66ms | 59.46ms | 1.9× | 37.6% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.62ms | 87.89ms | 3.2× | 42.4% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 25.17ms | 54.02ms | 2.1× | 41.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 45.99ms | 109.86ms | 2.4× | 58.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 58.82ms | 78.34ms | 1.3× | 49.8% | PASS |
| ac_writes | `types_delete_insert_ac` | 26.04ms | 64.56ms | 2.5× | 76.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 41.62ms | 69.32ms | 1.7× | 39.2% | PASS |

</details>

</details>

## Version-control latency

Wall time: 4m 48s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 23.96ms | 38.00ms | 63.1% | 1.0% | PASS |
| `status_dirty_many_tables` | 26.03ms | 42.00ms | 62.0% | 1.2% | PASS |
| `diff_regular_working_one_table` | 20.59ms | 33.00ms | 62.4% | 1.2% | PASS |
| `diff_regular_working_many_tables` | 29.85ms | 50.00ms | 59.7% | 0.8% | PASS |
| `diff_stat_working_many_tables` | 30.34ms | 48.00ms | 63.2% | 1.1% | PASS |
| `diff_schema_working_many_tables` | 31.45ms | 48.00ms | 65.5% | 0.9% | PASS |
| `branch_list_many_branches` | 15.10ms | 25.00ms | 60.4% | 0.8% | PASS |
| `branch_create_delete` | 16.48ms | 27.00ms | 61.1% | 1.0% | PASS |
| `at_literal_deep_history` | 16.33ms | 28.00ms | 58.3% | 0.7% | PASS |
| `diff_literal_deep_history` | 16.43ms | 28.00ms | 58.7% | 0.7% | PASS |
| `history_literal_deep_history` | 16.76ms | 30.00ms | 55.9% | 0.6% | PASS |
| `checkout_branch_clean` | 23.02ms | 43.00ms | 53.5% | 0.9% | PASS |
| `merge_data_no_conflicts` | 18.06ms | 33.00ms | 54.7% | 0.7% | PASS |
| `merge_data_secondary_index` | 620.89ms | 944.00ms | 65.8% | 1.3% | PASS |
| `merge_schema_no_conflicts` | 14.62ms | 24.00ms | 60.9% | 1.3% | PASS |
| `merge_data_conflicts` | 20.69ms | 33.00ms | 62.7% | 1.0% | PASS |
| `merge_data_conflicts_with_resolve` | 21.36ms | 34.00ms | 62.8% | 1.0% | PASS |

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

candidate: 456a2a0.

Fixture file bytes: base 107,968,301; delta 115,879,032.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 47.813 | 47.016–55.671 | 45.646 | 2091.48 | 107968301 |
| push_initial | candidate | 346.711 | 344.588–347.516 | 345.516 | 288.42 | 107956807 |
| clone_initial | candidate | 462.842 | 451.679–471.607 | 451.638 | 216.06 | 108475105 |
| push_noop | candidate | 0.363 | 0.358–0.368 | 0.196 | — | 0 |
| fetch_noop | candidate | 0.093 | 0.092–0.097 | 0.094 | — | 0 |
| push_incremental | candidate | 138.210 | 137.684–138.871 | 136.056 | 7.24 | 7909775 |
| fetch_incremental | candidate | 60.586 | 60.075–61.114 | 59.358 | 16.51 | 7910084 |
| pull_fast_forward | candidate | 53.773 | 52.664–54.021 | 52.274 | 18.60 | 7910844 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 90.50 | 175.487 | 108.763 | 18329 | 2066 |
| clone_initial | candidate | 90.86 | 295.110 | 110.833 | 47250 | 2353 |
| push_noop | candidate | 41.38 | 0.073 | 0.000 | 27 | 2 |
| fetch_noop | candidate | 41.38 | 0.135 | 0.000 | 42 | 0 |
| push_incremental | candidate | 41.38 | 96.795 | 7.910 | 22395 | 125 |
| fetch_incremental | candidate | 41.38 | 34.472 | 7.910 | 10810 | 124 |
| pull_fast_forward | candidate | 41.38 | 26.497 | 8.959 | 7032 | 142 |

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

candidate: 456a2a0.

Fixture file bytes: base 1,087,718,066; delta 1,103,525,194.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 5667.184 | 5650.541–5693.216 | 322.824 | 176.45 | 1087718066 |
| push_initial | candidate | 7683.399 | 7680.137–7733.867 | 4004.995 | 130.15 | 1079566310 |
| clone_initial | candidate | 10824.546 | 10364.086–10881.022 | 5793.028 | 92.38 | 1102407396 |
| push_noop | candidate | 0.715 | 0.612–0.829 | 0.280 | — | 0 |
| fetch_noop | candidate | 0.152 | 0.148–0.174 | 0.154 | — | 0 |
| push_incremental | candidate | 959.980 | 937.099–975.713 | 891.294 | 1.04 | 15806003 |
| fetch_incremental | candidate | 253.040 | 240.305–255.791 | 182.674 | 3.95 | 15806513 |
| pull_fast_forward | candidate | 274.686 | 261.194–277.924 | 198.499 | 3.64 | 15807273 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 134.76 | 2091.875 | 1087.622 | 263497 | 20541 |
| clone_initial | candidate | 123.91 | 3512.865 | 1133.868 | 611071 | 32259 |
| push_noop | candidate | 44.21 | 0.078 | 0.000 | 28 | 2 |
| fetch_noop | candidate | 44.21 | 0.137 | 0.000 | 42 | 0 |
| push_incremental | candidate | 48.27 | 614.176 | 15.928 | 149164 | 313 |
| fetch_incremental | candidate | 50.32 | 62.060 | 15.928 | 13855 | 313 |
| pull_fast_forward | candidate | 50.32 | 61.993 | 16.978 | 13850 | 331 |

</details>


</details>

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh`, `test/vc_perf_ceiling.sh`, and `test/remote_perf.py`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
