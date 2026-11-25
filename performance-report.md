# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-11-25 11:19 UTC
>
> Commit: [`c2c24139d4b48d7815d4ce4ddc43bc59033fc924`](https://github.com/dolthub/doltlite/commit/c2c24139d4b48d7815d4ce4ddc43bc59033fc924)
>
> Runner: ubuntu24 20261116.133.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/45586387772)

The SQL workloads compare optimized DoltLite against stock SQLite on the same GitHub-hosted runner, alternating execution order on each repetition. Version-control sections measure DoltLite latency. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class. Mean multiple is the average over workloads of each workload's paired DoltLite/SQLite ratio, so every workload counts equally whatever its run time.

### In-memory

| Operation | Mean multiple | Paired-ratio noise | Result |
|---|---:|---:|---|
| Reads | 1.11× | 1.3% | **PASS** |
| Writes | 1.63× | 1.1% | **PASS** |

### File-backed

| Operation | Mean multiple | Paired-ratio noise | Result |
|---|---:|---:|---|
| Reads | 0.92× | 1.3% | **PASS** |
| Writes | 1.35× | 1.9% | **PASS** |
| Autocommit writes | 2.10× | 1.9% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.8× for a section average. Durable autocommit writes use 3.6× and 2.6× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | Mean multiple | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 1.12× | 1.3% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 1.07× | 1.3% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 1.10× | 1.0% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 1.17× | 1.7% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 1.56× | 1.2% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 1.64× | 1.0% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 1.68× | 0.9% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 1.66× | 1.3% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 0.92× | 1.4% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 0.92× | 1.0% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 0.94× | 1.1% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 0.90× | 1.7% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 1.31× | 1.9% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.37× | 2.2% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 1.42× | 1.5% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 1.29× | 1.8% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 1.07× | 1.5% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 1.03× | 1.3% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 1.04× | 1.1% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 1.11× | 1.4% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 2.13× | 1.7% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 2.12× | 1.8% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 2.12× | 1.6% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 2.05× | 6.6% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 15.53ms | 17.25ms | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_range_select` | 7.62ms | 8.37ms | 1.1× | 2.6% | PASS |
| mem_reads | `oltp_sum_range` | 6.68ms | 7.80ms | 1.2× | 2.7% | PASS |
| mem_reads | `oltp_order_range` | 1.88ms | 2.03ms | 1.1× | 2.8% | PASS |
| mem_reads | `oltp_distinct_range` | 2.59ms | 2.75ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 2.76ms | 3.44ms | 1.2× | 1.8% | PASS |
| mem_reads | `select_random_points` | 10.40ms | 9.36ms | 0.9× | 1.3% | PASS |
| mem_reads | `select_random_ranges` | 3.14ms | 3.21ms | 1.0× | 1.1% | PASS |
| mem_reads | `covering_index_scan` | 4.77ms | 6.71ms | 1.4× | 0.5% | PASS |
| mem_reads | `groupby_scan` | 21.29ms | 22.44ms | 1.1× | 0.5% | PASS |
| mem_reads | `index_join` | 4.14ms | 5.94ms | 1.4× | 1.5% | PASS |
| mem_reads | `index_join_scan` | 2.16ms | 3.88ms | 1.8× | 3.1% | PASS |
| mem_reads | `types_table_scan` | 752.49ms | 506.12ms | 0.7× | 0.2% | PASS |
| mem_reads | `table_scan` | 847.92ms | 577.50ms | 0.7× | 0.4% | PASS |
| mem_reads | `oltp_read_only` | 70.61ms | 77.79ms | 1.1× | 0.4% | PASS |
| mem_writes | `oltp_bulk_insert` | 108.53ms | 157.86ms | 1.5× | 0.4% | PASS |
| mem_writes | `oltp_insert` | 9.66ms | 15.73ms | 1.6× | 0.4% | PASS |
| mem_writes | `oltp_update_index` | 33.18ms | 58.18ms | 1.8× | 1.4% | PASS |
| mem_writes | `oltp_update_non_index` | 22.10ms | 34.24ms | 1.5× | 1.1% | PASS |
| mem_writes | `oltp_delete_insert` | 30.43ms | 46.12ms | 1.5× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 14.84ms | 27.38ms | 1.8× | 1.4% | PASS |
| mem_writes | `types_delete_insert` | 15.95ms | 19.41ms | 1.2× | 1.3% | PASS |
| mem_writes | `oltp_read_write` | 42.25ms | 62.56ms | 1.5× | 1.6% | PASS |
| file_reads | `oltp_point_select` | 34.84ms | 21.21ms | 0.6× | 1.3% | PASS |
| file_reads | `oltp_range_select` | 9.56ms | 8.86ms | 0.9× | 2.4% | PASS |
| file_reads | `oltp_sum_range` | 8.77ms | 8.42ms | 1.0× | 2.2% | PASS |
| file_reads | `oltp_order_range` | 2.16ms | 2.10ms | 1.0× | 2.0% | PASS |
| file_reads | `oltp_distinct_range` | 2.86ms | 2.85ms | 1.0× | 1.7% | PASS |
| file_reads | `oltp_index_scan` | 4.83ms | 3.95ms | 0.8× | 1.8% | PASS |
| file_reads | `select_random_points` | 12.09ms | 9.35ms | 0.8× | 2.3% | PASS |
| file_reads | `select_random_ranges` | 5.10ms | 3.61ms | 0.7× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 6.87ms | 7.18ms | 1.0× | 0.4% | PASS |
| file_reads | `groupby_scan` | 22.02ms | 22.31ms | 1.0× | 0.6% | PASS |
| file_reads | `index_join` | 5.20ms | 6.06ms | 1.2× | 1.4% | PASS |
| file_reads | `index_join_scan` | 2.56ms | 4.23ms | 1.6× | 3.5% | PASS |
| file_reads | `types_table_scan` | 756.33ms | 507.01ms | 0.7× | 0.4% | PASS |
| file_reads | `table_scan` | 853.59ms | 578.55ms | 0.7× | 0.6% | PASS |
| file_reads | `oltp_read_only` | 98.71ms | 84.81ms | 0.9× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 117.50ms | 167.93ms | 1.4× | 0.7% | PASS |
| file_writes | `oltp_insert` | 12.71ms | 17.66ms | 1.4× | 1.3% | PASS |
| file_writes | `oltp_update_index` | 45.30ms | 66.10ms | 1.5× | 2.4% | PASS |
| file_writes | `oltp_update_non_index` | 32.56ms | 38.21ms | 1.2× | 1.9% | PASS |
| file_writes | `oltp_delete_insert` | 39.60ms | 50.78ms | 1.3× | 2.2% | PASS |
| file_writes | `oltp_write_only` | 23.81ms | 33.14ms | 1.4× | 2.4% | PASS |
| file_writes | `types_delete_insert` | 22.95ms | 23.37ms | 1.0× | 1.9% | PASS |
| file_writes | `oltp_read_write` | 53.87ms | 70.08ms | 1.3× | 1.6% | PASS |
| ac_reads | `oltp_point_select` | 21.97ms | 21.70ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 8.67ms | 9.06ms | 1.0× | 1.8% | PASS |
| ac_reads | `oltp_sum_range` | 7.32ms | 8.29ms | 1.1× | 3.1% | PASS |
| ac_reads | `oltp_order_range` | 2.00ms | 2.11ms | 1.1× | 1.5% | PASS |
| ac_reads | `oltp_distinct_range` | 2.73ms | 2.80ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_index_scan` | 3.48ms | 3.89ms | 1.1× | 2.3% | PASS |
| ac_reads | `select_random_points` | 11.21ms | 9.90ms | 0.9× | 1.3% | PASS |
| ac_reads | `select_random_ranges` | 3.85ms | 3.68ms | 1.0× | 1.7% | PASS |
| ac_reads | `covering_index_scan` | 5.54ms | 7.22ms | 1.3× | 0.5% | PASS |
| ac_reads | `groupby_scan` | 21.80ms | 22.45ms | 1.0× | 0.7% | PASS |
| ac_reads | `index_join` | 4.62ms | 6.21ms | 1.3× | 1.7% | PASS |
| ac_reads | `index_join_scan` | 2.45ms | 4.21ms | 1.7× | 3.0% | PASS |
| ac_reads | `types_table_scan` | 756.12ms | 507.79ms | 0.7× | 0.3% | PASS |
| ac_reads | `table_scan` | 852.97ms | 577.16ms | 0.7× | 0.5% | PASS |
| ac_reads | `oltp_read_only` | 78.73ms | 83.68ms | 1.1× | 0.5% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 8.23ms | 12.88ms | 1.6× | 1.4% | PASS |
| ac_writes | `oltp_insert_ac` | 9.33ms | 19.23ms | 2.1× | 1.8% | PASS |
| ac_writes | `oltp_update_index_ac` | 10.22ms | 28.18ms | 2.8× | 3.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 8.45ms | 16.06ms | 1.9× | 1.7% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 9.29ms | 22.00ms | 2.4× | 1.4% | PASS |
| ac_writes | `oltp_write_only_ac` | 9.34ms | 21.71ms | 2.3× | 1.9% | PASS |
| ac_writes | `types_delete_insert_ac` | 8.24ms | 16.25ms | 2.0× | 1.6% | PASS |
| ac_writes | `oltp_read_write_ac` | 11.97ms | 26.04ms | 2.2× | 2.4% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.71ms | 23.88ms | 1.0× | 1.3% | PASS |
| mem_reads | `oltp_range_select` | 12.13ms | 11.43ms | 0.9× | 1.4% | PASS |
| mem_reads | `oltp_sum_range` | 12.16ms | 10.98ms | 0.9× | 0.9% | PASS |
| mem_reads | `oltp_order_range` | 2.47ms | 2.45ms | 1.0× | 1.4% | PASS |
| mem_reads | `oltp_distinct_range` | 3.15ms | 3.15ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 2.92ms | 4.60ms | 1.6× | 1.8% | PASS |
| mem_reads | `select_random_points` | 18.18ms | 16.59ms | 0.9× | 1.7% | PASS |
| mem_reads | `select_random_ranges` | 4.79ms | 4.40ms | 0.9× | 2.1% | PASS |
| mem_reads | `covering_index_scan` | 4.81ms | 7.06ms | 1.5× | 1.0% | PASS |
| mem_reads | `groupby_scan` | 23.77ms | 24.49ms | 1.0× | 0.6% | PASS |
| mem_reads | `index_join` | 9.22ms | 7.26ms | 0.8× | 2.4% | PASS |
| mem_reads | `index_join_scan` | 2.72ms | 5.33ms | 2.0× | 2.9% | PASS |
| mem_reads | `types_table_scan` | 769.47ms | 603.42ms | 0.8× | 0.7% | PASS |
| mem_reads | `table_scan` | 858.68ms | 670.97ms | 0.8× | 1.2% | PASS |
| mem_reads | `oltp_read_only` | 90.03ms | 93.12ms | 1.0× | 0.9% | PASS |
| mem_writes | `oltp_bulk_insert` | 145.47ms | 193.75ms | 1.3× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 11.38ms | 22.74ms | 2.0× | 0.4% | PASS |
| mem_writes | `oltp_update_index` | 45.51ms | 88.35ms | 1.9× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 33.23ms | 53.97ms | 1.6× | 0.9% | PASS |
| mem_writes | `oltp_delete_insert` | 37.69ms | 65.31ms | 1.7× | 1.2% | PASS |
| mem_writes | `oltp_write_only` | 19.11ms | 36.80ms | 1.9× | 1.7% | PASS |
| mem_writes | `types_delete_insert` | 27.11ms | 32.36ms | 1.2× | 1.3% | PASS |
| mem_writes | `oltp_read_write` | 65.00ms | 89.86ms | 1.4× | 0.9% | PASS |
| file_reads | `oltp_point_select` | 44.80ms | 29.12ms | 0.7× | 0.6% | PASS |
| file_reads | `oltp_range_select` | 14.04ms | 12.07ms | 0.9× | 1.0% | PASS |
| file_reads | `oltp_sum_range` | 14.04ms | 11.67ms | 0.8× | 1.0% | PASS |
| file_reads | `oltp_order_range` | 2.70ms | 2.57ms | 1.0× | 2.1% | PASS |
| file_reads | `oltp_distinct_range` | 3.39ms | 3.21ms | 0.9× | 1.5% | PASS |
| file_reads | `oltp_index_scan` | 4.95ms | 4.98ms | 1.0× | 1.0% | PASS |
| file_reads | `select_random_points` | 20.40ms | 17.20ms | 0.8× | 1.3% | PASS |
| file_reads | `select_random_ranges` | 6.82ms | 4.94ms | 0.7× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 6.92ms | 7.55ms | 1.1× | 0.7% | PASS |
| file_reads | `groupby_scan` | 23.78ms | 24.52ms | 1.0× | 0.5% | PASS |
| file_reads | `index_join` | 10.56ms | 7.96ms | 0.8× | 1.6% | PASS |
| file_reads | `index_join_scan` | 3.07ms | 5.51ms | 1.8× | 3.9% | PASS |
| file_reads | `types_table_scan` | 775.32ms | 604.53ms | 0.8× | 0.8% | PASS |
| file_reads | `table_scan` | 880.66ms | 673.80ms | 0.8× | 1.7% | PASS |
| file_reads | `oltp_read_only` | 126.76ms | 103.79ms | 0.8× | 1.5% | PASS |
| file_writes | `oltp_bulk_insert` | 164.07ms | 213.21ms | 1.3× | 1.6% | PASS |
| file_writes | `oltp_insert` | 15.43ms | 28.57ms | 1.9× | 1.6% | PASS |
| file_writes | `oltp_update_index` | 66.15ms | 106.29ms | 1.6× | 2.3% | PASS |
| file_writes | `oltp_update_non_index` | 50.54ms | 65.40ms | 1.3× | 2.5% | PASS |
| file_writes | `oltp_delete_insert` | 59.44ms | 80.27ms | 1.4× | 2.7% | PASS |
| file_writes | `oltp_write_only` | 33.82ms | 46.69ms | 1.4× | 2.4% | PASS |
| file_writes | `types_delete_insert` | 43.13ms | 39.57ms | 0.9× | 2.0% | PASS |
| file_writes | `oltp_read_write` | 76.94ms | 95.13ms | 1.2× | 1.8% | PASS |
| ac_reads | `oltp_point_select` | 30.71ms | 28.16ms | 0.9× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 12.51ms | 11.93ms | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_sum_range` | 12.56ms | 11.49ms | 0.9× | 1.5% | PASS |
| ac_reads | `oltp_order_range` | 2.60ms | 2.52ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_distinct_range` | 3.31ms | 3.24ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_index_scan` | 3.68ms | 5.07ms | 1.4× | 1.3% | PASS |
| ac_reads | `select_random_points` | 18.91ms | 16.95ms | 0.9× | 1.3% | PASS |
| ac_reads | `select_random_ranges` | 5.38ms | 4.71ms | 0.9× | 2.0% | PASS |
| ac_reads | `covering_index_scan` | 5.55ms | 7.52ms | 1.4× | 1.2% | PASS |
| ac_reads | `groupby_scan` | 24.11ms | 24.84ms | 1.0× | 0.5% | PASS |
| ac_reads | `index_join` | 10.40ms | 8.21ms | 0.8× | 2.3% | PASS |
| ac_reads | `index_join_scan` | 2.99ms | 5.56ms | 1.9× | 3.5% | PASS |
| ac_reads | `types_table_scan` | 782.22ms | 608.15ms | 0.8× | 1.2% | PASS |
| ac_reads | `table_scan` | 883.34ms | 679.06ms | 0.8× | 1.2% | PASS |
| ac_reads | `oltp_read_only` | 109.95ms | 105.54ms | 1.0× | 1.3% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 9.16ms | 12.90ms | 1.4× | 1.6% | PASS |
| ac_writes | `oltp_insert_ac` | 10.37ms | 19.07ms | 1.8× | 1.3% | PASS |
| ac_writes | `oltp_update_index_ac` | 10.68ms | 32.20ms | 3.0× | 1.8% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 8.87ms | 16.64ms | 1.9× | 1.5% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 10.13ms | 25.66ms | 2.5× | 2.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 10.10ms | 23.72ms | 2.3× | 2.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 9.26ms | 17.19ms | 1.9× | 1.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 13.91ms | 30.30ms | 2.2× | 2.5% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 25.36ms | 24.38ms | 1.0× | 1.4% | PASS |
| mem_reads | `oltp_range_select` | 11.67ms | 11.45ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_sum_range` | 11.65ms | 11.04ms | 0.9× | 1.0% | PASS |
| mem_reads | `oltp_order_range` | 2.42ms | 2.49ms | 1.0× | 1.0% | PASS |
| mem_reads | `oltp_distinct_range` | 3.11ms | 3.19ms | 1.0× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 2.94ms | 4.79ms | 1.6× | 1.1% | PASS |
| mem_reads | `select_random_points` | 18.00ms | 16.78ms | 0.9× | 1.3% | PASS |
| mem_reads | `select_random_ranges` | 4.76ms | 4.49ms | 0.9× | 1.4% | PASS |
| mem_reads | `covering_index_scan` | 4.83ms | 7.17ms | 1.5× | 0.5% | PASS |
| mem_reads | `groupby_scan` | 23.48ms | 24.52ms | 1.0× | 0.5% | PASS |
| mem_reads | `index_join` | 9.61ms | 8.42ms | 0.9× | 0.7% | PASS |
| mem_reads | `index_join_scan` | 2.74ms | 5.47ms | 2.0× | 1.3% | PASS |
| mem_reads | `types_table_scan` | 770.33ms | 605.98ms | 0.8× | 0.4% | PASS |
| mem_reads | `table_scan` | 860.91ms | 669.41ms | 0.8× | 0.8% | PASS |
| mem_reads | `oltp_read_only` | 95.18ms | 96.63ms | 1.0× | 0.9% | PASS |
| mem_writes | `oltp_bulk_insert` | 145.67ms | 197.22ms | 1.4× | 0.5% | PASS |
| mem_writes | `oltp_insert` | 11.83ms | 24.42ms | 2.1× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 46.27ms | 92.75ms | 2.0× | 1.0% | PASS |
| mem_writes | `oltp_update_non_index` | 33.69ms | 54.29ms | 1.6× | 0.9% | PASS |
| mem_writes | `oltp_delete_insert` | 38.17ms | 69.01ms | 1.8× | 0.6% | PASS |
| mem_writes | `oltp_write_only` | 19.88ms | 38.79ms | 2.0× | 1.0% | PASS |
| mem_writes | `types_delete_insert` | 26.57ms | 32.59ms | 1.2× | 1.2% | PASS |
| mem_writes | `oltp_read_write` | 64.63ms | 89.64ms | 1.4× | 0.9% | PASS |
| file_reads | `oltp_point_select` | 45.12ms | 28.69ms | 0.6× | 0.7% | PASS |
| file_reads | `oltp_range_select` | 13.55ms | 11.85ms | 0.9× | 0.9% | PASS |
| file_reads | `oltp_sum_range` | 13.36ms | 11.43ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_order_range` | 2.61ms | 2.51ms | 1.0× | 1.2% | PASS |
| file_reads | `oltp_distinct_range` | 3.28ms | 3.18ms | 1.0× | 1.4% | PASS |
| file_reads | `oltp_index_scan` | 5.02ms | 5.19ms | 1.0× | 1.0% | PASS |
| file_reads | `select_random_points` | 21.08ms | 17.56ms | 0.8× | 1.3% | PASS |
| file_reads | `select_random_ranges` | 7.01ms | 5.06ms | 0.7× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 6.97ms | 7.78ms | 1.1× | 0.7% | PASS |
| file_reads | `groupby_scan` | 23.70ms | 24.68ms | 1.0× | 0.4% | PASS |
| file_reads | `index_join` | 10.94ms | 8.70ms | 0.8× | 1.3% | PASS |
| file_reads | `index_join_scan` | 3.03ms | 5.56ms | 1.8× | 2.3% | PASS |
| file_reads | `types_table_scan` | 767.82ms | 605.01ms | 0.8× | 0.4% | PASS |
| file_reads | `table_scan` | 872.22ms | 668.84ms | 0.8× | 0.6% | PASS |
| file_reads | `oltp_read_only` | 124.03ms | 101.61ms | 0.8× | 1.1% | PASS |
| file_writes | `oltp_bulk_insert` | 158.85ms | 208.43ms | 1.3× | 0.9% | PASS |
| file_writes | `oltp_insert` | 15.46ms | 28.85ms | 1.9× | 1.8% | PASS |
| file_writes | `oltp_update_index` | 64.47ms | 109.29ms | 1.7× | 1.3% | PASS |
| file_writes | `oltp_update_non_index` | 48.03ms | 64.72ms | 1.3× | 1.3% | PASS |
| file_writes | `oltp_delete_insert` | 53.82ms | 78.36ms | 1.5× | 1.8% | PASS |
| file_writes | `oltp_write_only` | 34.10ms | 48.99ms | 1.4× | 2.0% | PASS |
| file_writes | `types_delete_insert` | 40.10ms | 40.20ms | 1.0× | 2.3% | PASS |
| file_writes | `oltp_read_write` | 80.95ms | 101.22ms | 1.3× | 1.1% | PASS |
| ac_reads | `oltp_point_select` | 31.49ms | 28.77ms | 0.9× | 0.8% | PASS |
| ac_reads | `oltp_range_select` | 12.36ms | 11.86ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_sum_range` | 12.08ms | 11.31ms | 0.9× | 1.2% | PASS |
| ac_reads | `oltp_order_range` | 2.54ms | 2.55ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_distinct_range` | 3.21ms | 3.24ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_index_scan` | 3.73ms | 5.11ms | 1.4× | 1.3% | PASS |
| ac_reads | `select_random_points` | 19.16ms | 17.04ms | 0.9× | 1.0% | PASS |
| ac_reads | `select_random_ranges` | 5.68ms | 5.02ms | 0.9× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 5.58ms | 7.67ms | 1.4× | 0.5% | PASS |
| ac_reads | `groupby_scan` | 23.35ms | 24.37ms | 1.0× | 0.3% | PASS |
| ac_reads | `index_join` | 10.10ms | 8.46ms | 0.8× | 1.3% | PASS |
| ac_reads | `index_join_scan` | 2.97ms | 5.49ms | 1.8× | 2.1% | PASS |
| ac_reads | `types_table_scan` | 768.82ms | 606.03ms | 0.8× | 0.3% | PASS |
| ac_reads | `table_scan` | 879.99ms | 673.41ms | 0.8× | 1.1% | PASS |
| ac_reads | `oltp_read_only` | 107.50ms | 104.35ms | 1.0× | 1.1% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 9.30ms | 12.96ms | 1.4× | 1.1% | PASS |
| ac_writes | `oltp_insert_ac` | 10.39ms | 22.78ms | 2.2× | 1.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 10.97ms | 31.97ms | 2.9× | 1.8% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 9.21ms | 17.47ms | 1.9× | 1.9% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 10.66ms | 25.73ms | 2.4× | 2.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 10.38ms | 24.07ms | 2.3× | 1.4% | PASS |
| ac_writes | `types_delete_insert_ac` | 9.54ms | 17.47ms | 1.8× | 1.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 13.95ms | 28.79ms | 2.1× | 1.6% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.91ms | 39.06ms | 1.2× | 1.8% | PASS |
| mem_reads | `oltp_range_select` | 21.32ms | 22.68ms | 1.1× | 1.7% | PASS |
| mem_reads | `oltp_sum_range` | 18.02ms | 21.44ms | 1.2× | 1.6% | PASS |
| mem_reads | `oltp_order_range` | 3.60ms | 3.96ms | 1.1× | 1.7% | PASS |
| mem_reads | `oltp_distinct_range` | 4.88ms | 5.13ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_index_scan` | 4.62ms | 6.09ms | 1.3× | 1.9% | PASS |
| mem_reads | `select_random_points` | 29.14ms | 30.59ms | 1.0× | 2.8% | PASS |
| mem_reads | `select_random_ranges` | 7.92ms | 8.93ms | 1.1× | 1.9% | PASS |
| mem_reads | `covering_index_scan` | 7.76ms | 11.24ms | 1.4× | 2.0% | PASS |
| mem_reads | `groupby_scan` | 37.12ms | 40.28ms | 1.1× | 1.0% | PASS |
| mem_reads | `index_join` | 8.00ms | 10.82ms | 1.4× | 1.8% | PASS |
| mem_reads | `index_join_scan` | 4.02ms | 6.28ms | 1.6× | 2.6% | PASS |
| mem_reads | `types_table_scan` | 1.04s | 986.14ms | 0.9× | 1.1% | PASS |
| mem_reads | `table_scan` | 1.19s | 1.07s | 0.9× | 1.7% | PASS |
| mem_reads | `oltp_read_only` | 156.31ms | 173.30ms | 1.1× | 1.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 250.91ms | 328.86ms | 1.3× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 19.02ms | 34.18ms | 1.8× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 66.79ms | 121.79ms | 1.8× | 1.6% | PASS |
| mem_writes | `oltp_update_non_index` | 52.50ms | 82.39ms | 1.6× | 1.1% | PASS |
| mem_writes | `oltp_delete_insert` | 49.97ms | 92.61ms | 1.9× | 1.4% | PASS |
| mem_writes | `oltp_write_only` | 27.21ms | 54.54ms | 2.0× | 1.1% | PASS |
| mem_writes | `types_delete_insert` | 33.74ms | 49.64ms | 1.5× | 1.5% | PASS |
| mem_writes | `oltp_read_write` | 107.08ms | 153.92ms | 1.4× | 2.4% | PASS |
| file_reads | `oltp_point_select` | 100.16ms | 56.65ms | 0.6× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 28.85ms | 25.20ms | 0.9× | 1.9% | PASS |
| file_reads | `oltp_sum_range` | 25.82ms | 24.03ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_order_range` | 4.69ms | 4.37ms | 0.9× | 1.8% | PASS |
| file_reads | `oltp_distinct_range` | 6.12ms | 5.59ms | 0.9× | 2.4% | PASS |
| file_reads | `oltp_index_scan` | 11.66ms | 8.39ms | 0.7× | 1.9% | PASS |
| file_reads | `select_random_points` | 37.77ms | 34.07ms | 0.9× | 1.7% | PASS |
| file_reads | `select_random_ranges` | 15.19ms | 11.07ms | 0.7× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 15.09ms | 13.62ms | 0.9× | 1.6% | PASS |
| file_reads | `groupby_scan` | 38.52ms | 40.91ms | 1.1× | 0.8% | PASS |
| file_reads | `index_join` | 12.61ms | 13.01ms | 1.0× | 3.4% | PASS |
| file_reads | `index_join_scan` | 5.13ms | 6.72ms | 1.3× | 3.1% | PASS |
| file_reads | `types_table_scan` | 1.06s | 990.80ms | 0.9× | 0.8% | PASS |
| file_reads | `table_scan` | 1.20s | 1.08s | 0.9× | 1.8% | PASS |
| file_reads | `oltp_read_only` | 257.00ms | 201.59ms | 0.8× | 1.3% | PASS |
| file_writes | `oltp_bulk_insert` | 266.11ms | 342.46ms | 1.3× | 1.1% | PASS |
| file_writes | `oltp_insert` | 26.69ms | 38.85ms | 1.5× | 2.2% | PASS |
| file_writes | `oltp_update_index` | 99.35ms | 142.03ms | 1.4× | 1.8% | PASS |
| file_writes | `oltp_update_non_index` | 80.44ms | 91.70ms | 1.1× | 1.6% | PASS |
| file_writes | `oltp_delete_insert` | 80.01ms | 106.12ms | 1.3× | 1.7% | PASS |
| file_writes | `oltp_write_only` | 53.15ms | 67.50ms | 1.3× | 2.7% | PASS |
| file_writes | `types_delete_insert` | 50.52ms | 59.50ms | 1.2× | 2.5% | PASS |
| file_writes | `oltp_read_write` | 135.60ms | 166.88ms | 1.2× | 1.9% | PASS |
| ac_reads | `oltp_point_select` | 57.45ms | 57.04ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_range_select` | 24.73ms | 25.23ms | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_sum_range` | 21.23ms | 23.95ms | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_order_range` | 4.17ms | 4.38ms | 1.1× | 2.4% | PASS |
| ac_reads | `oltp_distinct_range` | 5.49ms | 5.64ms | 1.0× | 2.1% | PASS |
| ac_reads | `oltp_index_scan` | 6.94ms | 8.28ms | 1.2× | 1.8% | PASS |
| ac_reads | `select_random_points` | 31.49ms | 33.48ms | 1.1× | 2.1% | PASS |
| ac_reads | `select_random_ranges` | 10.19ms | 10.96ms | 1.1× | 1.4% | PASS |
| ac_reads | `covering_index_scan` | 10.23ms | 13.49ms | 1.3× | 1.3% | PASS |
| ac_reads | `groupby_scan` | 37.63ms | 40.78ms | 1.1× | 1.0% | PASS |
| ac_reads | `index_join` | 9.47ms | 12.66ms | 1.3× | 1.9% | PASS |
| ac_reads | `index_join_scan` | 4.47ms | 6.57ms | 1.5× | 2.4% | PASS |
| ac_reads | `types_table_scan` | 1.03s | 980.66ms | 1.0× | 0.4% | PASS |
| ac_reads | `table_scan` | 1.17s | 1.07s | 0.9× | 1.0% | PASS |
| ac_reads | `oltp_read_only` | 189.92ms | 201.11ms | 1.1× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.32ms | 33.84ms | 1.5× | 9.0% | PASS |
| ac_writes | `oltp_insert_ac` | 24.02ms | 49.27ms | 2.1× | 6.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.30ms | 64.41ms | 2.4× | 4.5% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.96ms | 41.88ms | 1.8× | 5.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 22.99ms | 52.42ms | 2.3× | 6.8% | PASS |
| ac_writes | `oltp_write_only_ac` | 23.82ms | 51.65ms | 2.2× | 5.0% | PASS |
| ac_writes | `types_delete_insert_ac` | 20.97ms | 41.48ms | 2.0× | 8.0% | PASS |
| ac_writes | `oltp_read_write_ac` | 30.57ms | 62.38ms | 2.0× | 7.3% | PASS |

</details>

</details>

## Version-control latency

Wall time: 4m 29s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 32.45ms | 38.00ms | 85.4% | 1.5% | PASS |
| `status_dirty_many_tables` | 35.51ms | 42.00ms | 84.5% | 1.0% | PASS |
| `diff_regular_working_one_table` | 31.70ms | 33.00ms | 96.0% | 1.2% | PASS |
| `diff_regular_working_many_tables` | 40.76ms | 50.00ms | 81.5% | 0.7% | PASS |
| `diff_stat_working_many_tables` | 41.02ms | 48.00ms | 85.4% | 0.6% | PASS |
| `diff_schema_working_many_tables` | 41.61ms | 48.00ms | 86.7% | 1.0% | PASS |
| `branch_list_many_branches` | 23.98ms | 25.00ms | 95.9% | 1.9% | PASS |
| `branch_create_delete` | 26.46ms | 27.00ms | 98.0% | 1.3% | PASS |
| `at_literal_deep_history` | 25.39ms | 28.00ms | 90.7% | 1.5% | PASS |
| `diff_literal_deep_history` | 25.13ms | 28.00ms | 89.8% | 0.8% | PASS |
| `history_literal_deep_history` | 26.33ms | 30.00ms | 87.8% | 1.2% | PASS |
| `checkout_branch_clean` | 32.02ms | 43.00ms | 74.5% | 1.0% | PASS |
| `merge_data_no_conflicts` | 27.79ms | 33.00ms | 84.2% | 1.5% | PASS |
| `merge_data_secondary_index` | 883.28ms | 944.00ms | 93.6% | 0.8% | PASS |
| `merge_schema_no_conflicts` | 21.66ms | 24.00ms | 90.2% | 1.8% | PASS |
| `merge_data_conflicts` | 30.72ms | 33.00ms | 93.1% | 1.2% | PASS |
| `merge_data_conflicts_with_resolve` | 31.27ms | 34.00ms | 92.0% | 0.9% | PASS |

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

candidate: c2c2413.

Fixture file bytes: base 107,968,301; delta 115,879,032.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 58.574 | 57.815–61.604 | 38.310 | 1707.24 | 107968301 |
| push_initial | candidate | 502.923 | 494.212–508.863 | 485.727 | 198.84 | 107956807 |
| clone_initial | candidate | 530.326 | 523.496–543.135 | 503.089 | 188.56 | 108475105 |
| push_noop | candidate | 0.647 | 0.607–0.682 | 0.324 | — | 0 |
| fetch_noop | candidate | 0.144 | 0.143–0.152 | 0.146 | — | 0 |
| push_incremental | candidate | 64.503 | 64.198–65.518 | 61.759 | 15.50 | 7909775 |
| fetch_incremental | candidate | 78.222 | 75.646–89.694 | 75.696 | 12.78 | 7910084 |
| pull_fast_forward | candidate | 77.490 | 76.499–78.923 | 74.570 | 12.90 | 7910844 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 90.41 | 175.487 | 108.763 | 18329 | 2066 |
| clone_initial | candidate | 91.29 | 215.906 | 110.833 | 27898 | 2353 |
| push_noop | candidate | 41.83 | 0.073 | 0.000 | 27 | 2 |
| fetch_noop | candidate | 41.83 | 0.068 | 0.000 | 22 | 0 |
| push_incremental | candidate | 41.83 | 10.741 | 7.910 | 1375 | 125 |
| fetch_incremental | candidate | 41.83 | 26.494 | 7.910 | 7016 | 124 |
| pull_fast_forward | candidate | 41.83 | 26.497 | 8.959 | 7032 | 142 |

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

candidate: c2c2413.

Fixture file bytes: base 1,087,718,066; delta 1,103,525,194.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 4147.424 | 3213.703–4149.749 | 503.905 | 241.11 | 1087718066 |
| push_initial | candidate | 5676.348 | 5668.744–5686.607 | 3029.457 | 176.17 | 1079566310 |
| clone_initial | candidate | 4586.306 | 4311.887–4663.811 | 3247.475 | 218.04 | 1102407396 |
| push_noop | candidate | 0.419 | 0.388–0.423 | 0.200 | — | 0 |
| fetch_noop | candidate | 0.082 | 0.082–0.082 | 0.082 | — | 0 |
| push_incremental | candidate | 175.551 | 175.092–177.739 | 173.038 | 5.70 | 15806003 |
| fetch_incremental | candidate | 180.071 | 179.971–181.392 | 176.592 | 5.55 | 15806513 |
| pull_fast_forward | candidate | 180.666 | 179.684–183.016 | 177.241 | 5.54 | 15807273 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 134.86 | 2091.978 | 1087.622 | 263522 | 20541 |
| clone_initial | candidate | 132.03 | 2178.776 | 1133.868 | 284188 | 32259 |
| push_noop | candidate | 44.10 | 0.078 | 0.000 | 28 | 2 |
| fetch_noop | candidate | 44.10 | 0.069 | 0.000 | 22 | 0 |
| push_incremental | candidate | 56.29 | 47.374 | 15.928 | 10270 | 313 |
| fetch_incremental | candidate | 50.34 | 62.954 | 15.928 | 14069 | 313 |
| pull_fast_forward | candidate | 50.40 | 62.957 | 16.978 | 14085 | 331 |

</details>


</details>

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh`, `test/vc_perf_ceiling.sh`, and `test/remote_perf.py`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
