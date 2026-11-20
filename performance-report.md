# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-11-20 11:09 UTC
>
> Commit: [`da5a4e503963fc7ca40fcad9003b0800f1be9db0`](https://github.com/dolthub/doltlite/commit/da5a4e503963fc7ca40fcad9003b0800f1be9db0)
>
> Runner: ubuntu24 20261111.127.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/44711532923)

The SQL workloads compare optimized DoltLite against stock SQLite on the same GitHub-hosted runner, alternating execution order on each repetition. Version-control sections measure DoltLite latency. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 7.42s | 8.47s | 1.14× | 1.4% | **PASS** |
| Writes | 1.50s | 2.27s | 1.52× | 1.1% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 8.10s | 8.60s | 1.06× | 1.4% | **PASS** |
| Writes | 2.86s | 3.12s | 1.09× | 2.4% | **PASS** |
| Autocommit writes | 518.29ms | 1.04s | 2.00× | 3.4% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.8× for a section average. Durable autocommit writes use 3.6× and 2.6× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 1.99s | 2.17s | 1.09× | 1.9% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 1.88s | 2.20s | 1.17× | 1.1% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 1.66s | 1.89s | 1.14× | 2.2% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 1.89s | 2.21s | 1.17× | 1.1% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 356.51ms | 546.12ms | 1.53× | 1.5% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 390.01ms | 586.11ms | 1.50× | 0.8% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 361.70ms | 547.85ms | 1.51× | 1.7% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 387.93ms | 592.88ms | 1.53× | 0.7% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.19s | 2.22s | 1.01× | 1.4% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 1.99s | 2.23s | 1.12× | 1.3% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 1.81s | 1.90s | 1.05× | 1.9% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.11s | 2.25s | 1.07× | 1.0% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 879.03ms | 855.64ms | 0.97× | 4.5% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 517.40ms | 670.70ms | 1.30× | 1.7% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 990.31ms | 951.85ms | 0.96× | 5.0% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 472.73ms | 641.00ms | 1.36× | 1.4% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.08s | 2.22s | 1.07× | 1.0% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 1.94s | 2.23s | 1.15× | 1.2% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 1.71s | 1.91s | 1.11× | 3.0% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 1.90s | 2.21s | 1.16× | 1.1% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 154.39ms | 375.65ms | 2.43× | 13.3% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 84.13ms | 173.16ms | 2.06× | 1.6% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 200.71ms | 323.33ms | 1.61× | 4.8% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 79.06ms | 162.92ms | 2.06× | 1.8% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 19.36ms | 21.78ms | 1.1× | 1.9% | PASS |
| mem_reads | `oltp_range_select` | 8.85ms | 9.32ms | 1.1× | 2.2% | PASS |
| mem_reads | `oltp_sum_range` | 7.89ms | 8.51ms | 1.1× | 2.0% | PASS |
| mem_reads | `oltp_order_range` | 2.18ms | 2.29ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 3.03ms | 3.08ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 3.15ms | 3.93ms | 1.3× | 2.3% | PASS |
| mem_reads | `select_random_points` | 10.65ms | 9.34ms | 0.9× | 2.6% | PASS |
| mem_reads | `select_random_ranges` | 3.73ms | 3.69ms | 1.0× | 1.4% | PASS |
| mem_reads | `covering_index_scan` | 5.89ms | 7.44ms | 1.3× | 1.6% | PASS |
| mem_reads | `groupby_scan` | 25.19ms | 26.14ms | 1.0× | 0.7% | PASS |
| mem_reads | `index_join` | 4.62ms | 6.22ms | 1.3× | 2.1% | PASS |
| mem_reads | `index_join_scan` | 2.71ms | 4.40ms | 1.6× | 2.5% | PASS |
| mem_reads | `types_table_scan` | 842.88ms | 933.53ms | 1.1× | 0.6% | PASS |
| mem_reads | `table_scan` | 964.74ms | 1.05s | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_read_only` | 83.15ms | 87.10ms | 1.0× | 1.9% | PASS |
| mem_writes | `oltp_bulk_insert` | 142.21ms | 217.43ms | 1.5× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 12.34ms | 19.13ms | 1.5× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 41.98ms | 72.12ms | 1.7× | 1.5% | PASS |
| mem_writes | `oltp_update_non_index` | 30.00ms | 45.07ms | 1.5× | 1.3% | PASS |
| mem_writes | `oltp_delete_insert` | 37.98ms | 56.02ms | 1.5× | 1.5% | PASS |
| mem_writes | `oltp_write_only` | 18.95ms | 35.53ms | 1.9× | 2.2% | PASS |
| mem_writes | `types_delete_insert` | 20.51ms | 25.09ms | 1.2× | 1.4% | PASS |
| mem_writes | `oltp_read_write` | 52.53ms | 75.74ms | 1.4× | 1.7% | PASS |
| file_reads | `oltp_point_select` | 79.34ms | 37.31ms | 0.5× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 15.04ms | 11.12ms | 0.7× | 1.6% | PASS |
| file_reads | `oltp_sum_range` | 14.20ms | 10.36ms | 0.7× | 1.9% | PASS |
| file_reads | `oltp_order_range` | 2.93ms | 2.57ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_distinct_range` | 3.71ms | 3.32ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_index_scan` | 9.35ms | 5.92ms | 0.6× | 1.4% | PASS |
| file_reads | `select_random_points` | 17.26ms | 11.09ms | 0.6× | 2.2% | PASS |
| file_reads | `select_random_ranges` | 9.79ms | 5.41ms | 0.6× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 12.31ms | 9.44ms | 0.8× | 0.8% | PASS |
| file_reads | `groupby_scan` | 25.89ms | 26.50ms | 1.0× | 0.8% | PASS |
| file_reads | `index_join` | 8.06ms | 7.79ms | 1.0× | 2.2% | PASS |
| file_reads | `index_join_scan` | 3.48ms | 4.77ms | 1.4× | 3.0% | PASS |
| file_reads | `types_table_scan` | 844.02ms | 931.75ms | 1.1× | 0.6% | PASS |
| file_reads | `table_scan` | 969.14ms | 1.04s | 1.1× | 1.3% | PASS |
| file_reads | `oltp_read_only` | 177.66ms | 112.72ms | 0.6× | 1.1% | PASS |
| file_writes | `oltp_bulk_insert` | 190.11ms | 265.08ms | 1.4× | 4.0% | PASS |
| file_writes | `oltp_insert` | 27.79ms | 29.75ms | 1.1× | 5.0% | PASS |
| file_writes | `oltp_update_index` | 139.08ms | 127.28ms | 0.9× | 4.3% | PASS |
| file_writes | `oltp_update_non_index` | 112.92ms | 85.59ms | 0.8× | 4.0% | PASS |
| file_writes | `oltp_delete_insert` | 121.18ms | 106.66ms | 0.9× | 5.2% | PASS |
| file_writes | `oltp_write_only` | 88.79ms | 73.25ms | 0.8× | 3.2% | PASS |
| file_writes | `types_delete_insert` | 74.53ms | 54.62ms | 0.7× | 5.0% | PASS |
| file_writes | `oltp_read_write` | 124.65ms | 113.41ms | 0.9× | 4.7% | PASS |
| ac_reads | `oltp_point_select` | 39.52ms | 37.30ms | 0.9× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 11.27ms | 11.12ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_sum_range` | 10.36ms | 10.36ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_order_range` | 2.59ms | 2.58ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_distinct_range` | 3.38ms | 3.32ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_index_scan` | 5.72ms | 5.98ms | 1.0× | 1.5% | PASS |
| ac_reads | `select_random_points` | 13.61ms | 11.24ms | 0.8× | 2.8% | PASS |
| ac_reads | `select_random_ranges` | 5.95ms | 5.43ms | 0.9× | 0.8% | PASS |
| ac_reads | `covering_index_scan` | 8.47ms | 9.45ms | 1.1× | 0.9% | PASS |
| ac_reads | `groupby_scan` | 25.48ms | 26.50ms | 1.0× | 0.5% | PASS |
| ac_reads | `index_join` | 6.13ms | 7.83ms | 1.3× | 1.9% | PASS |
| ac_reads | `index_join_scan` | 3.10ms | 4.83ms | 1.6× | 2.5% | PASS |
| ac_reads | `types_table_scan` | 845.34ms | 933.17ms | 1.1× | 0.5% | PASS |
| ac_reads | `table_scan` | 984.64ms | 1.04s | 1.1× | 2.6% | PASS |
| ac_reads | `oltp_read_only` | 117.99ms | 111.46ms | 0.9× | 1.4% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 17.39ms | 29.20ms | 1.7× | 9.5% | PASS |
| ac_writes | `oltp_insert_ac` | 19.56ms | 44.76ms | 2.3× | 14.1% | PASS |
| ac_writes | `oltp_update_index_ac` | 20.97ms | 65.19ms | 3.1× | 9.9% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 16.76ms | 39.24ms | 2.3× | 13.2% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 19.36ms | 48.74ms | 2.5× | 13.4% | PASS |
| ac_writes | `oltp_write_only_ac` | 19.26ms | 49.75ms | 2.6× | 8.8% | PASS |
| ac_writes | `types_delete_insert_ac` | 17.41ms | 40.33ms | 2.3× | 17.1% | PASS |
| ac_writes | `oltp_read_write_ac` | 23.68ms | 58.44ms | 2.5× | 24.5% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 25.69ms | 25.15ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_range_select` | 12.46ms | 11.64ms | 0.9× | 1.0% | PASS |
| mem_reads | `oltp_sum_range` | 12.54ms | 11.19ms | 0.9× | 1.2% | PASS |
| mem_reads | `oltp_order_range` | 2.51ms | 2.52ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 3.15ms | 3.14ms | 1.0× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 2.90ms | 4.60ms | 1.6× | 1.1% | PASS |
| mem_reads | `select_random_points` | 18.19ms | 16.99ms | 0.9× | 1.1% | PASS |
| mem_reads | `select_random_ranges` | 4.83ms | 4.51ms | 0.9× | 1.7% | PASS |
| mem_reads | `covering_index_scan` | 4.80ms | 6.84ms | 1.4× | 0.9% | PASS |
| mem_reads | `groupby_scan` | 23.58ms | 23.90ms | 1.0× | 0.6% | PASS |
| mem_reads | `index_join` | 9.34ms | 7.71ms | 0.8× | 1.9% | PASS |
| mem_reads | `index_join_scan` | 2.62ms | 5.39ms | 2.1× | 3.0% | PASS |
| mem_reads | `types_table_scan` | 770.98ms | 936.44ms | 1.2× | 0.7% | PASS |
| mem_reads | `table_scan` | 892.20ms | 1.04s | 1.2× | 2.1% | PASS |
| mem_reads | `oltp_read_only` | 97.74ms | 98.16ms | 1.0× | 1.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 147.49ms | 194.96ms | 1.3× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 11.66ms | 22.03ms | 1.9× | 0.6% | PASS |
| mem_writes | `oltp_update_index` | 46.77ms | 93.05ms | 2.0× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 33.85ms | 50.98ms | 1.5× | 0.8% | PASS |
| mem_writes | `oltp_delete_insert` | 38.20ms | 65.32ms | 1.7× | 1.1% | PASS |
| mem_writes | `oltp_write_only` | 19.89ms | 34.93ms | 1.8× | 0.8% | PASS |
| mem_writes | `types_delete_insert` | 27.31ms | 38.40ms | 1.4× | 1.1% | PASS |
| mem_writes | `oltp_read_write` | 64.85ms | 86.44ms | 1.3× | 0.8% | PASS |
| file_reads | `oltp_point_select` | 44.75ms | 29.30ms | 0.7× | 0.7% | PASS |
| file_reads | `oltp_range_select` | 13.87ms | 11.97ms | 0.9× | 0.8% | PASS |
| file_reads | `oltp_sum_range` | 13.87ms | 11.55ms | 0.8× | 1.6% | PASS |
| file_reads | `oltp_order_range` | 2.64ms | 2.54ms | 1.0× | 1.6% | PASS |
| file_reads | `oltp_distinct_range` | 3.38ms | 3.21ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 4.96ms | 5.17ms | 1.0× | 1.3% | PASS |
| file_reads | `select_random_points` | 20.52ms | 17.45ms | 0.9× | 1.7% | PASS |
| file_reads | `select_random_ranges` | 6.83ms | 4.99ms | 0.7× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 6.94ms | 7.56ms | 1.1× | 0.8% | PASS |
| file_reads | `groupby_scan` | 23.82ms | 24.17ms | 1.0× | 0.5% | PASS |
| file_reads | `index_join` | 10.55ms | 8.04ms | 0.8× | 2.0% | PASS |
| file_reads | `index_join_scan` | 3.12ms | 5.75ms | 1.8× | 2.9% | PASS |
| file_reads | `types_table_scan` | 782.85ms | 939.95ms | 1.2× | 0.9% | PASS |
| file_reads | `table_scan` | 913.32ms | 1.05s | 1.2× | 5.0% | PASS |
| file_reads | `oltp_read_only` | 135.74ms | 108.03ms | 0.8× | 1.9% | PASS |
| file_writes | `oltp_bulk_insert` | 163.43ms | 210.71ms | 1.3× | 0.8% | PASS |
| file_writes | `oltp_insert` | 15.46ms | 26.04ms | 1.7× | 1.9% | PASS |
| file_writes | `oltp_update_index` | 65.53ms | 106.73ms | 1.6× | 1.5% | PASS |
| file_writes | `oltp_update_non_index` | 50.05ms | 60.39ms | 1.2× | 1.8% | PASS |
| file_writes | `oltp_delete_insert` | 59.66ms | 79.52ms | 1.3× | 2.3% | PASS |
| file_writes | `oltp_write_only` | 36.11ms | 44.80ms | 1.2× | 1.6% | PASS |
| file_writes | `types_delete_insert` | 44.63ms | 45.69ms | 1.0× | 2.3% | PASS |
| file_writes | `oltp_read_write` | 82.53ms | 96.82ms | 1.2× | 1.7% | PASS |
| ac_reads | `oltp_point_select` | 33.13ms | 30.14ms | 0.9× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 13.53ms | 12.18ms | 0.9× | 0.9% | PASS |
| ac_reads | `oltp_sum_range` | 13.41ms | 11.71ms | 0.9× | 0.9% | PASS |
| ac_reads | `oltp_order_range` | 2.62ms | 2.56ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_distinct_range` | 3.31ms | 3.22ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_index_scan` | 3.72ms | 5.18ms | 1.4× | 1.2% | PASS |
| ac_reads | `select_random_points` | 19.91ms | 17.73ms | 0.9× | 1.4% | PASS |
| ac_reads | `select_random_ranges` | 5.79ms | 5.07ms | 0.9× | 1.6% | PASS |
| ac_reads | `covering_index_scan` | 5.59ms | 7.64ms | 1.4× | 0.7% | PASS |
| ac_reads | `groupby_scan` | 23.74ms | 24.13ms | 1.0× | 0.5% | PASS |
| ac_reads | `index_join` | 10.19ms | 8.04ms | 0.8× | 2.3% | PASS |
| ac_reads | `index_join_scan` | 3.02ms | 5.70ms | 1.9× | 1.5% | PASS |
| ac_reads | `types_table_scan` | 789.32ms | 946.45ms | 1.2× | 1.2% | PASS |
| ac_reads | `table_scan` | 898.49ms | 1.04s | 1.2× | 2.5% | PASS |
| ac_reads | `oltp_read_only` | 111.92ms | 106.08ms | 0.9× | 1.4% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 9.33ms | 12.93ms | 1.4× | 1.0% | PASS |
| ac_writes | `oltp_insert_ac` | 10.63ms | 19.11ms | 1.8× | 2.8% | PASS |
| ac_writes | `oltp_update_index_ac` | 10.85ms | 30.65ms | 2.8× | 1.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 9.13ms | 16.59ms | 1.8× | 2.1% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 10.55ms | 25.37ms | 2.4× | 1.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 10.29ms | 23.06ms | 2.2× | 1.5% | PASS |
| ac_writes | `types_delete_insert_ac` | 9.52ms | 17.25ms | 1.8× | 1.6% | PASS |
| ac_writes | `oltp_read_write_ac` | 13.83ms | 28.22ms | 2.0× | 1.9% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.57ms | 23.60ms | 1.0× | 3.0% | PASS |
| mem_reads | `oltp_range_select` | 11.08ms | 10.07ms | 0.9× | 3.6% | PASS |
| mem_reads | `oltp_sum_range` | 10.55ms | 9.75ms | 0.9× | 2.2% | PASS |
| mem_reads | `oltp_order_range` | 2.19ms | 2.21ms | 1.0× | 1.9% | PASS |
| mem_reads | `oltp_distinct_range` | 2.80ms | 2.80ms | 1.0× | 2.1% | PASS |
| mem_reads | `oltp_index_scan` | 2.86ms | 4.90ms | 1.7× | 1.4% | PASS |
| mem_reads | `select_random_points` | 18.15ms | 15.68ms | 0.9× | 3.2% | PASS |
| mem_reads | `select_random_ranges` | 4.85ms | 4.27ms | 0.9× | 1.6% | PASS |
| mem_reads | `covering_index_scan` | 4.17ms | 6.97ms | 1.7× | 2.2% | PASS |
| mem_reads | `groupby_scan` | 20.91ms | 20.59ms | 1.0× | 2.0% | PASS |
| mem_reads | `index_join` | 7.94ms | 7.57ms | 1.0× | 3.4% | PASS |
| mem_reads | `index_join_scan` | 2.87ms | 5.39ms | 1.9× | 3.0% | PASS |
| mem_reads | `types_table_scan` | 694.06ms | 807.17ms | 1.2× | 2.3% | PASS |
| mem_reads | `table_scan` | 773.09ms | 889.63ms | 1.2× | 1.3% | PASS |
| mem_reads | `oltp_read_only` | 77.70ms | 75.86ms | 1.0× | 1.7% | PASS |
| mem_writes | `oltp_bulk_insert` | 145.08ms | 197.16ms | 1.4× | 1.5% | PASS |
| mem_writes | `oltp_insert` | 11.06ms | 21.14ms | 1.9× | 1.6% | PASS |
| mem_writes | `oltp_update_index` | 41.38ms | 81.64ms | 2.0× | 1.7% | PASS |
| mem_writes | `oltp_update_non_index` | 31.97ms | 46.52ms | 1.5× | 1.2% | PASS |
| mem_writes | `oltp_delete_insert` | 33.65ms | 60.01ms | 1.8× | 1.7% | PASS |
| mem_writes | `oltp_write_only` | 18.29ms | 33.32ms | 1.8× | 1.7% | PASS |
| mem_writes | `types_delete_insert` | 25.04ms | 34.81ms | 1.4× | 2.5% | PASS |
| mem_writes | `oltp_read_write` | 55.23ms | 73.25ms | 1.3× | 2.0% | PASS |
| file_reads | `oltp_point_select` | 72.21ms | 35.42ms | 0.5× | 2.3% | PASS |
| file_reads | `oltp_range_select` | 15.48ms | 10.90ms | 0.7× | 2.5% | PASS |
| file_reads | `oltp_sum_range` | 15.51ms | 10.71ms | 0.7× | 2.2% | PASS |
| file_reads | `oltp_order_range` | 2.85ms | 2.43ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_distinct_range` | 3.34ms | 2.88ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 8.24ms | 5.82ms | 0.7× | 1.6% | PASS |
| file_reads | `select_random_points` | 21.89ms | 15.38ms | 0.7× | 3.8% | PASS |
| file_reads | `select_random_ranges` | 10.07ms | 5.52ms | 0.5× | 1.5% | PASS |
| file_reads | `covering_index_scan` | 9.87ms | 8.02ms | 0.8× | 1.0% | PASS |
| file_reads | `groupby_scan` | 22.46ms | 21.38ms | 1.0× | 1.6% | PASS |
| file_reads | `index_join` | 11.70ms | 8.15ms | 0.7× | 4.0% | PASS |
| file_reads | `index_join_scan` | 3.41ms | 4.92ms | 1.4× | 3.1% | PASS |
| file_reads | `types_table_scan` | 675.86ms | 786.93ms | 1.2× | 1.2% | PASS |
| file_reads | `table_scan` | 777.56ms | 882.23ms | 1.1× | 1.9% | PASS |
| file_reads | `oltp_read_only` | 156.95ms | 97.06ms | 0.6× | 2.1% | PASS |
| file_writes | `oltp_bulk_insert` | 209.86ms | 260.37ms | 1.2× | 2.9% | PASS |
| file_writes | `oltp_insert` | 19.10ms | 42.80ms | 2.2× | 11.6% | PASS |
| file_writes | `oltp_update_index` | 142.78ms | 157.47ms | 1.1× | 2.4% | PASS |
| file_writes | `oltp_update_non_index` | 120.72ms | 98.89ms | 0.8× | 5.5% | PASS |
| file_writes | `oltp_delete_insert` | 148.84ms | 120.95ms | 0.8× | 3.8% | PASS |
| file_writes | `oltp_write_only` | 101.05ms | 79.40ms | 0.8× | 6.0% | PASS |
| file_writes | `types_delete_insert` | 106.50ms | 69.38ms | 0.7× | 6.1% | PASS |
| file_writes | `oltp_read_write` | 141.44ms | 122.60ms | 0.9× | 4.5% | PASS |
| ac_reads | `oltp_point_select` | 40.21ms | 35.78ms | 0.9× | 2.5% | PASS |
| ac_reads | `oltp_range_select` | 12.23ms | 10.83ms | 0.9× | 3.0% | PASS |
| ac_reads | `oltp_sum_range` | 11.79ms | 10.54ms | 0.9× | 3.3% | PASS |
| ac_reads | `oltp_order_range` | 2.39ms | 2.35ms | 1.0× | 2.2% | PASS |
| ac_reads | `oltp_distinct_range` | 2.86ms | 2.81ms | 1.0× | 2.4% | PASS |
| ac_reads | `oltp_index_scan` | 4.55ms | 5.68ms | 1.2× | 3.0% | PASS |
| ac_reads | `select_random_points` | 16.91ms | 14.54ms | 0.9× | 3.0% | PASS |
| ac_reads | `select_random_ranges` | 6.40ms | 5.42ms | 0.8× | 2.2% | PASS |
| ac_reads | `covering_index_scan` | 5.98ms | 7.83ms | 1.3× | 2.5% | PASS |
| ac_reads | `groupby_scan` | 20.55ms | 20.28ms | 1.0× | 1.9% | PASS |
| ac_reads | `index_join` | 9.27ms | 7.78ms | 0.8× | 4.8% | PASS |
| ac_reads | `index_join_scan` | 3.05ms | 4.87ms | 1.6× | 5.6% | PASS |
| ac_reads | `types_table_scan` | 683.22ms | 782.44ms | 1.1× | 1.7% | PASS |
| ac_reads | `table_scan` | 786.04ms | 899.68ms | 1.1× | 3.4% | PASS |
| ac_reads | `oltp_read_only` | 108.94ms | 96.81ms | 0.9× | 4.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 24.02ms | 26.79ms | 1.1× | 4.4% | PASS |
| ac_writes | `oltp_insert_ac` | 27.29ms | 40.79ms | 1.5× | 3.8% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.33ms | 57.27ms | 2.1× | 5.0% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 20.86ms | 33.22ms | 1.6× | 5.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.09ms | 42.95ms | 1.6× | 5.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.43ms | 41.91ms | 1.6× | 3.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 21.07ms | 34.98ms | 1.7× | 5.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 28.62ms | 45.41ms | 1.6× | 4.6% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 21.24ms | 23.98ms | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_range_select` | 13.96ms | 15.07ms | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_sum_range` | 13.38ms | 13.66ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_order_range` | 2.67ms | 2.78ms | 1.0× | 1.4% | PASS |
| mem_reads | `oltp_distinct_range` | 3.39ms | 3.46ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 3.23ms | 4.18ms | 1.3× | 1.2% | PASS |
| mem_reads | `select_random_points` | 20.81ms | 21.46ms | 1.0× | 0.7% | PASS |
| mem_reads | `select_random_ranges` | 5.09ms | 5.81ms | 1.1× | 1.1% | PASS |
| mem_reads | `covering_index_scan` | 4.82ms | 6.78ms | 1.4× | 1.1% | PASS |
| mem_reads | `groupby_scan` | 25.33ms | 26.86ms | 1.1× | 0.5% | PASS |
| mem_reads | `index_join` | 5.81ms | 8.32ms | 1.4× | 1.1% | PASS |
| mem_reads | `index_join_scan` | 2.94ms | 4.92ms | 1.7× | 3.4% | PASS |
| mem_reads | `types_table_scan` | 778.84ms | 929.32ms | 1.2× | 0.6% | PASS |
| mem_reads | `table_scan` | 880.48ms | 1.03s | 1.2× | 2.0% | PASS |
| mem_reads | `oltp_read_only` | 104.12ms | 111.06ms | 1.1× | 0.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 151.47ms | 195.50ms | 1.3× | 0.4% | PASS |
| mem_writes | `oltp_insert` | 12.42ms | 20.61ms | 1.7× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 46.66ms | 85.88ms | 1.8× | 0.7% | PASS |
| mem_writes | `oltp_update_non_index` | 34.01ms | 55.85ms | 1.6× | 0.8% | PASS |
| mem_writes | `oltp_delete_insert` | 34.68ms | 62.57ms | 1.8× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 19.14ms | 38.34ms | 2.0× | 0.7% | PASS |
| mem_writes | `types_delete_insert` | 21.62ms | 35.96ms | 1.7× | 0.8% | PASS |
| mem_writes | `oltp_read_write` | 67.93ms | 98.17ms | 1.4× | 0.7% | PASS |
| file_reads | `oltp_point_select` | 42.47ms | 29.77ms | 0.7× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 16.54ms | 15.88ms | 1.0× | 0.7% | PASS |
| file_reads | `oltp_sum_range` | 15.94ms | 14.52ms | 0.9× | 1.1% | PASS |
| file_reads | `oltp_order_range` | 2.93ms | 2.92ms | 1.0× | 1.4% | PASS |
| file_reads | `oltp_distinct_range` | 3.60ms | 3.57ms | 1.0× | 2.2% | PASS |
| file_reads | `oltp_index_scan` | 5.39ms | 5.02ms | 0.9× | 1.4% | PASS |
| file_reads | `select_random_points` | 23.26ms | 22.61ms | 1.0× | 0.8% | PASS |
| file_reads | `select_random_ranges` | 7.29ms | 6.52ms | 0.9× | 0.8% | PASS |
| file_reads | `covering_index_scan` | 6.96ms | 7.55ms | 1.1× | 0.6% | PASS |
| file_reads | `groupby_scan` | 25.50ms | 26.97ms | 1.1× | 0.4% | PASS |
| file_reads | `index_join` | 7.04ms | 8.55ms | 1.2× | 1.4% | PASS |
| file_reads | `index_join_scan` | 3.22ms | 5.16ms | 1.6× | 2.6% | PASS |
| file_reads | `types_table_scan` | 789.36ms | 936.23ms | 1.2× | 1.0% | PASS |
| file_reads | `table_scan` | 1.03s | 1.05s | 1.0× | 12.8% | PASS |
| file_reads | `oltp_read_only` | 138.47ms | 122.35ms | 0.9× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 162.80ms | 206.49ms | 1.3× | 0.8% | PASS |
| file_writes | `oltp_insert` | 15.86ms | 24.07ms | 1.5× | 1.9% | PASS |
| file_writes | `oltp_update_index` | 60.64ms | 95.90ms | 1.6× | 1.1% | PASS |
| file_writes | `oltp_update_non_index` | 46.27ms | 60.33ms | 1.3× | 1.0% | PASS |
| file_writes | `oltp_delete_insert` | 47.49ms | 70.88ms | 1.5× | 1.8% | PASS |
| file_writes | `oltp_write_only` | 30.51ms | 42.48ms | 1.4× | 2.0% | PASS |
| file_writes | `types_delete_insert` | 29.68ms | 38.95ms | 1.3× | 2.0% | PASS |
| file_writes | `oltp_read_write` | 79.47ms | 101.89ms | 1.3× | 1.1% | PASS |
| ac_reads | `oltp_point_select` | 28.80ms | 29.72ms | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_range_select` | 15.31ms | 15.92ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_sum_range` | 14.57ms | 14.44ms | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_order_range` | 2.82ms | 2.91ms | 1.0× | 2.3% | PASS |
| ac_reads | `oltp_distinct_range` | 3.55ms | 3.58ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_index_scan` | 4.21ms | 5.13ms | 1.2× | 1.2% | PASS |
| ac_reads | `select_random_points` | 21.69ms | 22.39ms | 1.0× | 0.6% | PASS |
| ac_reads | `select_random_ranges` | 5.82ms | 6.37ms | 1.1× | 1.1% | PASS |
| ac_reads | `covering_index_scan` | 5.53ms | 7.32ms | 1.3× | 0.6% | PASS |
| ac_reads | `groupby_scan` | 25.18ms | 26.69ms | 1.1× | 0.3% | PASS |
| ac_reads | `index_join` | 6.34ms | 8.29ms | 1.3× | 1.3% | PASS |
| ac_reads | `index_join_scan` | 3.10ms | 5.03ms | 1.6× | 1.3% | PASS |
| ac_reads | `types_table_scan` | 773.26ms | 920.89ms | 1.2× | 0.6% | PASS |
| ac_reads | `table_scan` | 882.06ms | 1.03s | 1.2× | 2.1% | PASS |
| ac_reads | `oltp_read_only` | 110.03ms | 116.51ms | 1.1× | 0.6% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 8.82ms | 12.83ms | 1.5× | 2.7% | PASS |
| ac_writes | `oltp_insert_ac` | 9.84ms | 20.25ms | 2.1× | 1.8% | PASS |
| ac_writes | `oltp_update_index_ac` | 10.58ms | 27.45ms | 2.6× | 1.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 8.97ms | 16.04ms | 1.8× | 1.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 9.71ms | 22.22ms | 2.3× | 2.4% | PASS |
| ac_writes | `oltp_write_only_ac` | 9.79ms | 21.64ms | 2.2× | 3.7% | PASS |
| ac_writes | `types_delete_insert_ac` | 8.53ms | 16.35ms | 1.9× | 1.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 12.82ms | 26.13ms | 2.0× | 1.6% | PASS |

</details>

</details>

## Version-control latency

Wall time: 4m 48s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 22.80ms | 38.00ms | 60.0% | 1.0% | PASS |
| `status_dirty_many_tables` | 24.86ms | 42.00ms | 59.2% | 0.5% | PASS |
| `diff_regular_working_one_table` | 21.39ms | 33.00ms | 64.8% | 0.7% | PASS |
| `diff_regular_working_many_tables` | 28.43ms | 50.00ms | 56.9% | 0.9% | PASS |
| `diff_stat_working_many_tables` | 28.42ms | 48.00ms | 59.2% | 0.6% | PASS |
| `diff_schema_working_many_tables` | 28.82ms | 48.00ms | 60.0% | 0.9% | PASS |
| `branch_list_many_branches` | 15.48ms | 25.00ms | 61.9% | 0.6% | PASS |
| `branch_create_delete` | 16.75ms | 27.00ms | 62.0% | 0.8% | PASS |
| `at_literal_deep_history` | 16.29ms | 28.00ms | 58.2% | 0.5% | PASS |
| `diff_literal_deep_history` | 16.28ms | 28.00ms | 58.1% | 0.6% | PASS |
| `history_literal_deep_history` | 16.98ms | 30.00ms | 56.6% | 0.6% | PASS |
| `checkout_branch_clean` | 22.10ms | 43.00ms | 51.4% | 1.0% | PASS |
| `merge_data_no_conflicts` | 18.14ms | 33.00ms | 55.0% | 0.7% | PASS |
| `merge_data_secondary_index` | 620.03ms | 944.00ms | 65.7% | 1.0% | PASS |
| `merge_schema_no_conflicts` | 14.59ms | 24.00ms | 60.8% | 0.9% | PASS |
| `merge_data_conflicts` | 20.41ms | 33.00ms | 61.9% | 0.7% | PASS |
| `merge_data_conflicts_with_resolve` | 21.03ms | 34.00ms | 61.9% | 0.4% | PASS |

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

candidate: da5a4e5.

Fixture file bytes: base 107,968,301; delta 115,879,032.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 47.896 | 47.572–61.368 | 45.017 | 2087.86 | 107968301 |
| push_initial | candidate | 347.441 | 343.615–350.995 | 344.916 | 287.82 | 107956807 |
| clone_initial | candidate | 464.691 | 450.047–467.743 | 449.827 | 215.20 | 108475105 |
| push_noop | candidate | 0.383 | 0.347–0.469 | 0.184 | — | 0 |
| fetch_noop | candidate | 0.097 | 0.092–0.098 | 0.098 | — | 0 |
| push_incremental | candidate | 138.972 | 136.144–139.049 | 136.734 | 7.20 | 7909775 |
| fetch_incremental | candidate | 61.304 | 60.683–63.479 | 59.620 | 16.31 | 7910084 |
| pull_fast_forward | candidate | 54.039 | 53.672–54.613 | 52.648 | 18.51 | 7910844 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 90.50 | 175.487 | 108.763 | 18329 | 2066 |
| clone_initial | candidate | 90.77 | 295.112 | 110.833 | 47249 | 2353 |
| push_noop | candidate | 41.46 | 0.073 | 0.000 | 27 | 2 |
| fetch_noop | candidate | 41.46 | 0.135 | 0.000 | 42 | 0 |
| push_incremental | candidate | 41.46 | 96.748 | 7.910 | 22384 | 125 |
| fetch_incremental | candidate | 41.46 | 34.472 | 7.910 | 10810 | 124 |
| pull_fast_forward | candidate | 41.46 | 26.497 | 8.959 | 7032 | 142 |

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

candidate: da5a4e5.

Fixture file bytes: base 1,087,718,066; delta 1,103,525,194.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 2651.158 | 2636.863–2671.336 | 373.626 | 377.19 | 1087718066 |
| push_initial | candidate | 9018.299 | 8977.643–9023.562 | 6761.856 | 110.89 | 1079566310 |
| clone_initial | candidate | 9941.467 | 9939.857–9967.168 | 9735.357 | 100.59 | 1102407396 |
| push_noop | candidate | 0.671 | 0.652–0.730 | 0.339 | — | 0 |
| fetch_noop | candidate | 0.214 | 0.206–0.243 | 0.216 | — | 0 |
| push_incremental | candidate | 1480.165 | 1479.582–1489.813 | 1475.621 | 0.68 | 15806003 |
| fetch_incremental | candidate | 312.399 | 310.915–319.374 | 308.445 | 3.20 | 15806513 |
| pull_fast_forward | candidate | 314.509 | 310.543–315.610 | 309.878 | 3.18 | 15807273 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 134.66 | 2091.789 | 1087.622 | 263476 | 20541 |
| clone_initial | candidate | 123.84 | 3518.647 | 1133.868 | 612501 | 32259 |
| push_noop | candidate | 44.38 | 0.078 | 0.000 | 28 | 2 |
| fetch_noop | candidate | 44.38 | 0.137 | 0.000 | 42 | 0 |
| push_incremental | candidate | 48.08 | 617.385 | 15.928 | 149955 | 313 |
| fetch_incremental | candidate | 50.38 | 62.373 | 15.928 | 13931 | 313 |
| pull_fast_forward | candidate | 50.24 | 62.306 | 16.978 | 13926 | 331 |

</details>


</details>

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh`, `test/vc_perf_ceiling.sh`, and `test/remote_perf.py`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
