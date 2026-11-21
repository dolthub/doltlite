# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-11-21 11:04 UTC
>
> Commit: [`1bac204e1cc730625d005f6fd4f5a47f8a07d8b9`](https://github.com/dolthub/doltlite/commit/1bac204e1cc730625d005f6fd4f5a47f8a07d8b9)
>
> Runner: ubuntu24 20261116.133.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/44890014854)

The SQL workloads compare optimized DoltLite against stock SQLite on the same GitHub-hosted runner, alternating execution order on each repetition. Version-control sections measure DoltLite latency. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class. Mean multiple is the average over workloads of each workload's paired DoltLite/SQLite ratio, so every workload counts equally whatever its run time.

### In-memory

| Operation | Mean multiple | Paired-ratio noise | Result |
|---|---:|---:|---|
| Reads | 1.12× | 1.6% | **PASS** |
| Writes | 1.56× | 1.5% | **PASS** |

### File-backed

| Operation | Mean multiple | Paired-ratio noise | Result |
|---|---:|---:|---|
| Reads | 0.86× | 1.3% | **PASS** |
| Writes | 1.13× | 3.4% | **PASS** |
| Autocommit writes | 1.87× | 3.5% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.8× for a section average. Durable autocommit writes use 3.6× and 2.6× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | Mean multiple | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 1.11× | 1.5% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 1.07× | 1.2% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 1.10× | 3.8% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 1.20× | 1.6% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 1.49× | 1.6% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 1.58× | 1.2% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 1.58× | 1.5% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 1.59× | 1.8% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 0.79× | 1.3% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 0.91× | 1.8% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 0.93× | 1.5% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 0.83× | 0.9% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 0.95× | 14.9% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.28× | 2.4% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 1.33× | 1.8% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 0.98× | 4.5% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 1.02× | 1.1% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 1.03× | 2.1% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 1.04× | 1.2% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 1.06× | 2.2% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 1.80× | 44.1% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 2.05× | 1.6% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 2.06× | 1.7% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 1.57× | 4.9% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 18.86ms | 22.02ms | 1.2× | 1.6% | PASS |
| mem_reads | `oltp_range_select` | 8.80ms | 9.36ms | 1.1× | 2.9% | PASS |
| mem_reads | `oltp_sum_range` | 7.59ms | 8.59ms | 1.1× | 1.7% | PASS |
| mem_reads | `oltp_order_range` | 2.20ms | 2.38ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_distinct_range` | 3.08ms | 3.25ms | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_index_scan` | 3.25ms | 4.31ms | 1.3× | 1.5% | PASS |
| mem_reads | `select_random_points` | 11.40ms | 9.71ms | 0.9× | 2.3% | PASS |
| mem_reads | `select_random_ranges` | 3.77ms | 3.78ms | 1.0× | 1.0% | PASS |
| mem_reads | `covering_index_scan` | 5.91ms | 7.83ms | 1.3× | 1.3% | PASS |
| mem_reads | `groupby_scan` | 25.65ms | 27.31ms | 1.1× | 1.5% | PASS |
| mem_reads | `index_join` | 4.51ms | 6.27ms | 1.4× | 2.6% | PASS |
| mem_reads | `index_join_scan` | 2.65ms | 4.43ms | 1.7× | 1.9% | PASS |
| mem_reads | `types_table_scan` | 855.38ms | 660.49ms | 0.8× | 1.4% | PASS |
| mem_reads | `table_scan` | 1.02s | 738.52ms | 0.7× | 3.3% | PASS |
| mem_reads | `oltp_read_only` | 87.10ms | 91.95ms | 1.1× | 1.4% | PASS |
| mem_writes | `oltp_bulk_insert` | 140.88ms | 210.77ms | 1.5× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 12.37ms | 18.60ms | 1.5× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 44.15ms | 74.79ms | 1.7× | 1.4% | PASS |
| mem_writes | `oltp_update_non_index` | 31.66ms | 44.44ms | 1.4× | 2.1% | PASS |
| mem_writes | `oltp_delete_insert` | 38.02ms | 54.63ms | 1.4× | 1.8% | PASS |
| mem_writes | `oltp_write_only` | 18.85ms | 33.41ms | 1.8× | 2.1% | PASS |
| mem_writes | `types_delete_insert` | 20.37ms | 23.91ms | 1.2× | 1.4% | PASS |
| mem_writes | `oltp_read_write` | 52.78ms | 75.28ms | 1.4× | 3.3% | PASS |
| file_reads | `oltp_point_select` | 79.83ms | 37.96ms | 0.5× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 15.47ms | 11.26ms | 0.7× | 0.9% | PASS |
| file_reads | `oltp_sum_range` | 14.37ms | 10.46ms | 0.7× | 1.6% | PASS |
| file_reads | `oltp_order_range` | 2.95ms | 2.63ms | 0.9× | 1.1% | PASS |
| file_reads | `oltp_distinct_range` | 3.80ms | 3.48ms | 0.9× | 0.9% | PASS |
| file_reads | `oltp_index_scan` | 9.69ms | 5.99ms | 0.6× | 1.0% | PASS |
| file_reads | `select_random_points` | 17.26ms | 11.13ms | 0.6× | 1.6% | PASS |
| file_reads | `select_random_ranges` | 9.97ms | 5.46ms | 0.5× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 12.54ms | 9.67ms | 0.8× | 1.0% | PASS |
| file_reads | `groupby_scan` | 26.41ms | 27.73ms | 1.1× | 1.3% | PASS |
| file_reads | `index_join` | 8.05ms | 7.62ms | 0.9× | 1.3% | PASS |
| file_reads | `index_join_scan` | 3.44ms | 4.71ms | 1.4× | 2.0% | PASS |
| file_reads | `types_table_scan` | 877.99ms | 662.71ms | 0.8× | 2.3% | PASS |
| file_reads | `table_scan` | 1.03s | 744.34ms | 0.7× | 3.4% | PASS |
| file_reads | `oltp_read_only` | 173.70ms | 114.05ms | 0.7× | 1.4% | PASS |
| file_writes | `oltp_bulk_insert` | 200.78ms | 259.84ms | 1.3× | 17.4% | PASS |
| file_writes | `oltp_insert` | 26.51ms | 29.80ms | 1.1× | 29.6% | PASS |
| file_writes | `oltp_update_index` | 141.07ms | 127.57ms | 0.9× | 10.3% | PASS |
| file_writes | `oltp_update_non_index` | 110.03ms | 88.32ms | 0.8× | 5.7% | PASS |
| file_writes | `oltp_delete_insert` | 116.72ms | 103.33ms | 0.9× | 8.1% | PASS |
| file_writes | `oltp_write_only` | 91.34ms | 70.67ms | 0.8× | 20.3% | PASS |
| file_writes | `types_delete_insert` | 65.54ms | 49.78ms | 0.8× | 12.4% | PASS |
| file_writes | `oltp_read_write` | 130.12ms | 112.75ms | 0.9× | 18.1% | PASS |
| ac_reads | `oltp_point_select` | 39.31ms | 37.85ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_range_select` | 11.45ms | 11.23ms | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_sum_range` | 10.39ms | 10.50ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_order_range` | 2.55ms | 2.61ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_distinct_range` | 3.38ms | 3.45ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_index_scan` | 5.61ms | 5.94ms | 1.1× | 0.9% | PASS |
| ac_reads | `select_random_points` | 13.18ms | 11.14ms | 0.8× | 1.7% | PASS |
| ac_reads | `select_random_ranges` | 6.00ms | 5.46ms | 0.9× | 1.1% | PASS |
| ac_reads | `covering_index_scan` | 8.41ms | 9.57ms | 1.1× | 0.8% | PASS |
| ac_reads | `groupby_scan` | 25.73ms | 27.56ms | 1.1× | 1.0% | PASS |
| ac_reads | `index_join` | 6.14ms | 7.80ms | 1.3× | 1.7% | PASS |
| ac_reads | `index_join_scan` | 3.09ms | 4.85ms | 1.6× | 2.9% | PASS |
| ac_reads | `types_table_scan` | 877.70ms | 667.96ms | 0.8× | 2.7% | PASS |
| ac_reads | `table_scan` | 1.07s | 755.05ms | 0.7× | 2.5% | PASS |
| ac_reads | `oltp_read_only` | 113.08ms | 112.89ms | 1.0× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 28.51ms | 41.18ms | 1.4× | 59.6% | PASS |
| ac_writes | `oltp_insert_ac` | 23.87ms | 40.38ms | 1.7× | 33.7% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.15ms | 71.10ms | 2.7× | 42.4% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 31.13ms | 41.16ms | 1.3× | 30.9% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 27.23ms | 54.15ms | 2.0× | 40.7% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.02ms | 47.45ms | 1.9× | 47.5% | PASS |
| ac_writes | `types_delete_insert_ac` | 27.46ms | 47.78ms | 1.7× | 72.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 34.84ms | 62.76ms | 1.8× | 45.8% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 25.01ms | 24.05ms | 1.0× | 0.7% | PASS |
| mem_reads | `oltp_range_select` | 12.11ms | 11.14ms | 0.9× | 0.9% | PASS |
| mem_reads | `oltp_sum_range` | 12.10ms | 10.81ms | 0.9× | 1.5% | PASS |
| mem_reads | `oltp_order_range` | 2.45ms | 2.44ms | 1.0× | 1.8% | PASS |
| mem_reads | `oltp_distinct_range` | 2.96ms | 3.07ms | 1.0× | 3.4% | PASS |
| mem_reads | `oltp_index_scan` | 2.83ms | 4.36ms | 1.5× | 2.0% | PASS |
| mem_reads | `select_random_points` | 18.08ms | 16.50ms | 0.9× | 1.2% | PASS |
| mem_reads | `select_random_ranges` | 4.77ms | 4.38ms | 0.9× | 1.7% | PASS |
| mem_reads | `covering_index_scan` | 4.81ms | 6.96ms | 1.4× | 0.5% | PASS |
| mem_reads | `groupby_scan` | 23.73ms | 24.95ms | 1.1× | 0.3% | PASS |
| mem_reads | `index_join` | 9.31ms | 7.54ms | 0.8× | 2.2% | PASS |
| mem_reads | `index_join_scan` | 2.45ms | 4.91ms | 2.0× | 2.8% | PASS |
| mem_reads | `types_table_scan` | 768.00ms | 587.27ms | 0.8× | 0.7% | PASS |
| mem_reads | `table_scan` | 859.68ms | 651.10ms | 0.8× | 0.7% | PASS |
| mem_reads | `oltp_read_only` | 91.43ms | 93.38ms | 1.0× | 0.5% | PASS |
| mem_writes | `oltp_bulk_insert` | 145.71ms | 190.52ms | 1.3× | 0.4% | PASS |
| mem_writes | `oltp_insert` | 11.38ms | 20.29ms | 1.8× | 0.6% | PASS |
| mem_writes | `oltp_update_index` | 43.48ms | 83.70ms | 1.9× | 0.9% | PASS |
| mem_writes | `oltp_update_non_index` | 30.57ms | 45.92ms | 1.5× | 1.3% | PASS |
| mem_writes | `oltp_delete_insert` | 35.34ms | 58.89ms | 1.7× | 1.7% | PASS |
| mem_writes | `oltp_write_only` | 17.47ms | 31.02ms | 1.8× | 2.6% | PASS |
| mem_writes | `types_delete_insert` | 25.74ms | 34.41ms | 1.3× | 1.5% | PASS |
| mem_writes | `oltp_read_write` | 62.81ms | 81.70ms | 1.3× | 1.1% | PASS |
| file_reads | `oltp_point_select` | 43.98ms | 28.15ms | 0.6× | 0.7% | PASS |
| file_reads | `oltp_range_select` | 13.41ms | 11.49ms | 0.9× | 2.5% | PASS |
| file_reads | `oltp_sum_range` | 13.23ms | 11.08ms | 0.8× | 2.6% | PASS |
| file_reads | `oltp_order_range` | 2.61ms | 2.44ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_distinct_range` | 3.27ms | 3.15ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 4.73ms | 4.47ms | 0.9× | 2.9% | PASS |
| file_reads | `select_random_points` | 17.77ms | 15.41ms | 0.9× | 3.3% | PASS |
| file_reads | `select_random_ranges` | 6.07ms | 4.50ms | 0.7× | 3.8% | PASS |
| file_reads | `covering_index_scan` | 6.87ms | 7.13ms | 1.0× | 0.5% | PASS |
| file_reads | `groupby_scan` | 23.24ms | 24.51ms | 1.1× | 0.5% | PASS |
| file_reads | `index_join` | 8.81ms | 6.83ms | 0.8× | 3.2% | PASS |
| file_reads | `index_join_scan` | 2.63ms | 4.47ms | 1.7× | 3.2% | PASS |
| file_reads | `types_table_scan` | 776.88ms | 586.04ms | 0.8× | 0.8% | PASS |
| file_reads | `table_scan` | 891.77ms | 652.17ms | 0.7× | 1.2% | PASS |
| file_reads | `oltp_read_only` | 117.94ms | 98.33ms | 0.8× | 1.8% | PASS |
| file_writes | `oltp_bulk_insert` | 157.77ms | 198.03ms | 1.3× | 1.0% | PASS |
| file_writes | `oltp_insert` | 14.86ms | 23.63ms | 1.6× | 1.4% | PASS |
| file_writes | `oltp_update_index` | 58.63ms | 92.39ms | 1.6× | 3.4% | PASS |
| file_writes | `oltp_update_non_index` | 44.87ms | 53.15ms | 1.2× | 2.6% | PASS |
| file_writes | `oltp_delete_insert` | 55.92ms | 70.83ms | 1.3× | 2.3% | PASS |
| file_writes | `oltp_write_only` | 32.75ms | 38.33ms | 1.2× | 3.2% | PASS |
| file_writes | `types_delete_insert` | 41.14ms | 39.27ms | 1.0× | 4.1% | PASS |
| file_writes | `oltp_read_write` | 73.47ms | 87.34ms | 1.2× | 2.1% | PASS |
| ac_reads | `oltp_point_select` | 30.97ms | 28.34ms | 0.9× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 12.01ms | 11.38ms | 0.9× | 3.4% | PASS |
| ac_reads | `oltp_sum_range` | 12.18ms | 11.14ms | 0.9× | 2.4% | PASS |
| ac_reads | `oltp_order_range` | 2.50ms | 2.46ms | 1.0× | 2.1% | PASS |
| ac_reads | `oltp_distinct_range` | 3.15ms | 3.16ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_index_scan` | 3.47ms | 4.68ms | 1.3× | 2.5% | PASS |
| ac_reads | `select_random_points` | 17.92ms | 16.67ms | 0.9× | 2.4% | PASS |
| ac_reads | `select_random_ranges` | 5.46ms | 4.74ms | 0.9× | 2.2% | PASS |
| ac_reads | `covering_index_scan` | 5.52ms | 7.30ms | 1.3× | 0.9% | PASS |
| ac_reads | `groupby_scan` | 23.44ms | 24.89ms | 1.1× | 0.6% | PASS |
| ac_reads | `index_join` | 9.77ms | 7.60ms | 0.8× | 2.6% | PASS |
| ac_reads | `index_join_scan` | 2.93ms | 5.51ms | 1.9× | 2.4% | PASS |
| ac_reads | `types_table_scan` | 778.52ms | 587.10ms | 0.8× | 0.8% | PASS |
| ac_reads | `table_scan` | 866.88ms | 652.29ms | 0.8× | 1.0% | PASS |
| ac_reads | `oltp_read_only` | 99.54ms | 98.03ms | 1.0× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 9.04ms | 12.59ms | 1.4× | 0.6% | PASS |
| ac_writes | `oltp_insert_ac` | 10.09ms | 18.10ms | 1.8× | 1.3% | PASS |
| ac_writes | `oltp_update_index_ac` | 10.33ms | 28.99ms | 2.8× | 1.8% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 8.76ms | 15.95ms | 1.8× | 1.4% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 9.86ms | 23.67ms | 2.4× | 1.9% | PASS |
| ac_writes | `oltp_write_only_ac` | 9.68ms | 21.90ms | 2.3× | 1.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 9.02ms | 16.42ms | 1.8× | 1.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 12.74ms | 26.61ms | 2.1× | 2.6% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 23.12ms | 22.13ms | 1.0× | 4.0% | PASS |
| mem_reads | `oltp_range_select` | 10.27ms | 10.29ms | 1.0× | 4.5% | PASS |
| mem_reads | `oltp_sum_range` | 9.72ms | 9.63ms | 1.0× | 9.4% | PASS |
| mem_reads | `oltp_order_range` | 2.07ms | 2.22ms | 1.1× | 4.3% | PASS |
| mem_reads | `oltp_distinct_range` | 2.77ms | 2.92ms | 1.1× | 3.0% | PASS |
| mem_reads | `oltp_index_scan` | 2.51ms | 3.90ms | 1.6× | 3.8% | PASS |
| mem_reads | `select_random_points` | 14.85ms | 13.88ms | 0.9× | 2.3% | PASS |
| mem_reads | `select_random_ranges` | 3.74ms | 3.90ms | 1.0× | 6.6% | PASS |
| mem_reads | `covering_index_scan` | 4.77ms | 6.60ms | 1.4× | 0.5% | PASS |
| mem_reads | `groupby_scan` | 22.96ms | 24.28ms | 1.1× | 0.5% | PASS |
| mem_reads | `index_join` | 8.29ms | 7.42ms | 0.9× | 5.0% | PASS |
| mem_reads | `index_join_scan` | 2.48ms | 4.93ms | 2.0× | 4.3% | PASS |
| mem_reads | `types_table_scan` | 762.99ms | 584.70ms | 0.8× | 0.3% | PASS |
| mem_reads | `table_scan` | 866.29ms | 645.46ms | 0.7× | 1.3% | PASS |
| mem_reads | `oltp_read_only` | 90.41ms | 92.01ms | 1.0× | 0.9% | PASS |
| mem_writes | `oltp_bulk_insert` | 143.21ms | 188.36ms | 1.3× | 0.5% | PASS |
| mem_writes | `oltp_insert` | 11.53ms | 20.58ms | 1.8× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 41.36ms | 79.50ms | 1.9× | 1.4% | PASS |
| mem_writes | `oltp_update_non_index` | 28.87ms | 44.14ms | 1.5× | 1.5% | PASS |
| mem_writes | `oltp_delete_insert` | 34.35ms | 57.86ms | 1.7× | 1.8% | PASS |
| mem_writes | `oltp_write_only` | 18.50ms | 30.77ms | 1.7× | 2.8% | PASS |
| mem_writes | `types_delete_insert` | 23.15ms | 33.28ms | 1.4× | 2.6% | PASS |
| mem_writes | `oltp_read_write` | 57.01ms | 76.37ms | 1.3× | 0.6% | PASS |
| file_reads | `oltp_point_select` | 42.92ms | 26.61ms | 0.6× | 1.5% | PASS |
| file_reads | `oltp_range_select` | 12.34ms | 10.93ms | 0.9× | 4.3% | PASS |
| file_reads | `oltp_sum_range` | 12.74ms | 10.95ms | 0.9× | 1.2% | PASS |
| file_reads | `oltp_order_range` | 2.61ms | 2.50ms | 1.0× | 1.5% | PASS |
| file_reads | `oltp_distinct_range` | 3.26ms | 3.18ms | 1.0× | 1.1% | PASS |
| file_reads | `oltp_index_scan` | 4.78ms | 4.54ms | 0.9× | 2.4% | PASS |
| file_reads | `select_random_points` | 17.84ms | 15.21ms | 0.9× | 4.4% | PASS |
| file_reads | `select_random_ranges` | 6.02ms | 4.43ms | 0.7× | 4.4% | PASS |
| file_reads | `covering_index_scan` | 6.86ms | 7.12ms | 1.0× | 0.5% | PASS |
| file_reads | `groupby_scan` | 23.22ms | 24.44ms | 1.1× | 0.6% | PASS |
| file_reads | `index_join` | 9.88ms | 7.80ms | 0.8× | 2.9% | PASS |
| file_reads | `index_join_scan` | 2.96ms | 5.39ms | 1.8× | 2.6% | PASS |
| file_reads | `types_table_scan` | 770.64ms | 592.53ms | 0.8× | 0.6% | PASS |
| file_reads | `table_scan` | 877.09ms | 652.76ms | 0.7× | 1.0% | PASS |
| file_reads | `oltp_read_only` | 125.83ms | 102.44ms | 0.8× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 157.79ms | 205.36ms | 1.3× | 1.1% | PASS |
| file_writes | `oltp_insert` | 15.17ms | 25.28ms | 1.7× | 1.8% | PASS |
| file_writes | `oltp_update_index` | 62.91ms | 103.57ms | 1.6× | 1.7% | PASS |
| file_writes | `oltp_update_non_index` | 47.86ms | 57.31ms | 1.2× | 1.5% | PASS |
| file_writes | `oltp_delete_insert` | 54.10ms | 75.97ms | 1.4× | 1.5% | PASS |
| file_writes | `oltp_write_only` | 34.23ms | 41.82ms | 1.2× | 1.9% | PASS |
| file_writes | `types_delete_insert` | 39.78ms | 42.18ms | 1.1× | 3.3% | PASS |
| file_writes | `oltp_read_write` | 82.18ms | 94.64ms | 1.2× | 3.0% | PASS |
| ac_reads | `oltp_point_select` | 32.25ms | 29.23ms | 0.9× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 12.46ms | 11.61ms | 0.9× | 2.0% | PASS |
| ac_reads | `oltp_sum_range` | 12.07ms | 11.14ms | 0.9× | 1.5% | PASS |
| ac_reads | `oltp_order_range` | 2.54ms | 2.54ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_distinct_range` | 3.21ms | 3.25ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_index_scan` | 3.78ms | 5.26ms | 1.4× | 1.8% | PASS |
| ac_reads | `select_random_points` | 18.93ms | 16.98ms | 0.9× | 1.1% | PASS |
| ac_reads | `select_random_ranges` | 5.58ms | 4.89ms | 0.9× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 5.57ms | 7.50ms | 1.3× | 0.4% | PASS |
| ac_reads | `groupby_scan` | 23.24ms | 24.86ms | 1.1× | 0.3% | PASS |
| ac_reads | `index_join` | 10.15ms | 8.41ms | 0.8× | 1.6% | PASS |
| ac_reads | `index_join_scan` | 2.98ms | 5.57ms | 1.9× | 1.7% | PASS |
| ac_reads | `types_table_scan` | 767.56ms | 592.15ms | 0.8× | 0.5% | PASS |
| ac_reads | `table_scan` | 873.40ms | 649.79ms | 0.7× | 0.9% | PASS |
| ac_reads | `oltp_read_only` | 103.06ms | 100.61ms | 1.0× | 0.7% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 9.16ms | 12.63ms | 1.4× | 0.8% | PASS |
| ac_writes | `oltp_insert_ac` | 10.25ms | 21.61ms | 2.1× | 1.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 10.78ms | 30.19ms | 2.8× | 1.5% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 8.93ms | 16.31ms | 1.8× | 1.9% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 10.22ms | 23.32ms | 2.3× | 1.9% | PASS |
| ac_writes | `oltp_write_only_ac` | 10.09ms | 22.79ms | 2.3× | 1.4% | PASS |
| ac_writes | `types_delete_insert_ac` | 9.34ms | 17.06ms | 1.8× | 1.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 13.73ms | 27.49ms | 2.0× | 1.9% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 17.98ms | 21.57ms | 1.2× | 2.2% | PASS |
| mem_reads | `oltp_range_select` | 9.59ms | 11.49ms | 1.2× | 4.0% | PASS |
| mem_reads | `oltp_sum_range` | 8.67ms | 11.29ms | 1.3× | 3.0% | PASS |
| mem_reads | `oltp_order_range` | 1.91ms | 2.22ms | 1.2× | 1.3% | PASS |
| mem_reads | `oltp_distinct_range` | 2.40ms | 2.71ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_index_scan` | 2.43ms | 3.45ms | 1.4× | 2.8% | PASS |
| mem_reads | `select_random_points` | 14.41ms | 16.46ms | 1.1× | 2.6% | PASS |
| mem_reads | `select_random_ranges` | 3.86ms | 4.69ms | 1.2× | 3.1% | PASS |
| mem_reads | `covering_index_scan` | 3.80ms | 5.21ms | 1.4× | 0.9% | PASS |
| mem_reads | `groupby_scan` | 19.17ms | 21.22ms | 1.1× | 1.1% | PASS |
| mem_reads | `index_join` | 4.43ms | 5.66ms | 1.3× | 1.8% | PASS |
| mem_reads | `index_join_scan` | 2.01ms | 3.96ms | 2.0× | 1.6% | PASS |
| mem_reads | `types_table_scan` | 652.99ms | 462.31ms | 0.7× | 0.5% | PASS |
| mem_reads | `table_scan` | 739.01ms | 521.20ms | 0.7× | 0.5% | PASS |
| mem_reads | `oltp_read_only` | 76.67ms | 85.10ms | 1.1× | 1.4% | PASS |
| mem_writes | `oltp_bulk_insert` | 139.89ms | 186.92ms | 1.3× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 10.82ms | 17.47ms | 1.6× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 38.82ms | 66.35ms | 1.7× | 1.9% | PASS |
| mem_writes | `oltp_update_non_index` | 29.97ms | 45.71ms | 1.5× | 2.4% | PASS |
| mem_writes | `oltp_delete_insert` | 28.95ms | 49.05ms | 1.7× | 2.2% | PASS |
| mem_writes | `oltp_write_only` | 16.02ms | 30.14ms | 1.9× | 2.1% | PASS |
| mem_writes | `types_delete_insert` | 19.68ms | 27.96ms | 1.4× | 1.6% | PASS |
| mem_writes | `oltp_read_write` | 50.56ms | 75.91ms | 1.5× | 1.8% | PASS |
| file_reads | `oltp_point_select` | 66.83ms | 34.30ms | 0.5× | 1.1% | PASS |
| file_reads | `oltp_range_select` | 15.44ms | 12.77ms | 0.8× | 1.0% | PASS |
| file_reads | `oltp_sum_range` | 14.65ms | 12.57ms | 0.9× | 0.9% | PASS |
| file_reads | `oltp_order_range` | 2.66ms | 2.40ms | 0.9× | 0.6% | PASS |
| file_reads | `oltp_distinct_range` | 3.15ms | 2.90ms | 0.9× | 0.8% | PASS |
| file_reads | `oltp_index_scan` | 8.06ms | 5.07ms | 0.6× | 1.8% | PASS |
| file_reads | `select_random_points` | 21.31ms | 17.68ms | 0.8× | 1.4% | PASS |
| file_reads | `select_random_ranges` | 9.23ms | 6.12ms | 0.7× | 0.8% | PASS |
| file_reads | `covering_index_scan` | 9.07ms | 6.88ms | 0.8× | 1.6% | PASS |
| file_reads | `groupby_scan` | 20.56ms | 21.50ms | 1.0× | 0.5% | PASS |
| file_reads | `index_join` | 7.51ms | 6.80ms | 0.9× | 1.2% | PASS |
| file_reads | `index_join_scan` | 3.14ms | 4.47ms | 1.4× | 2.4% | PASS |
| file_reads | `types_table_scan` | 655.89ms | 463.58ms | 0.7× | 0.6% | PASS |
| file_reads | `table_scan` | 739.90ms | 522.48ms | 0.7× | 0.8% | PASS |
| file_reads | `oltp_read_only` | 143.09ms | 102.92ms | 0.7× | 0.4% | PASS |
| file_writes | `oltp_bulk_insert` | 190.42ms | 232.00ms | 1.2× | 2.6% | PASS |
| file_writes | `oltp_insert` | 19.55ms | 29.41ms | 1.5× | 3.8% | PASS |
| file_writes | `oltp_update_index` | 139.47ms | 122.02ms | 0.9× | 1.9% | PASS |
| file_writes | `oltp_update_non_index` | 120.66ms | 89.33ms | 0.7× | 6.4% | PASS |
| file_writes | `oltp_delete_insert` | 120.60ms | 102.13ms | 0.8× | 4.6% | PASS |
| file_writes | `oltp_write_only` | 81.02ms | 71.44ms | 0.9× | 4.3% | PASS |
| file_writes | `types_delete_insert` | 71.75ms | 60.15ms | 0.8× | 6.8% | PASS |
| file_writes | `oltp_read_write` | 120.65ms | 112.39ms | 0.9× | 7.3% | PASS |
| ac_reads | `oltp_point_select` | 34.81ms | 34.30ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 12.28ms | 12.85ms | 1.0× | 2.2% | PASS |
| ac_reads | `oltp_sum_range` | 11.83ms | 12.75ms | 1.1× | 4.0% | PASS |
| ac_reads | `oltp_order_range` | 2.29ms | 2.45ms | 1.1× | 2.3% | PASS |
| ac_reads | `oltp_distinct_range` | 3.55ms | 3.58ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_index_scan` | 4.57ms | 5.31ms | 1.2× | 3.0% | PASS |
| ac_reads | `select_random_points` | 17.57ms | 17.81ms | 1.0× | 2.7% | PASS |
| ac_reads | `select_random_ranges` | 5.96ms | 6.16ms | 1.0× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 5.55ms | 6.95ms | 1.3× | 1.9% | PASS |
| ac_reads | `groupby_scan` | 20.00ms | 21.54ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 5.68ms | 6.79ms | 1.2× | 1.4% | PASS |
| ac_reads | `index_join_scan` | 2.74ms | 4.23ms | 1.5× | 2.2% | PASS |
| ac_reads | `types_table_scan` | 657.19ms | 464.70ms | 0.7× | 0.6% | PASS |
| ac_reads | `table_scan` | 899.68ms | 582.86ms | 0.6× | 7.5% | PASS |
| ac_reads | `oltp_read_only` | 130.25ms | 129.19ms | 1.0× | 4.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.29ms | 28.61ms | 1.3× | 4.4% | PASS |
| ac_writes | `oltp_insert_ac` | 26.40ms | 39.77ms | 1.5× | 5.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 28.17ms | 51.38ms | 1.8× | 5.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.59ms | 33.60ms | 1.6× | 4.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.73ms | 43.14ms | 1.7× | 6.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.74ms | 40.49ms | 1.6× | 4.8% | PASS |
| ac_writes | `types_delete_insert_ac` | 26.39ms | 39.65ms | 1.5× | 11.6% | PASS |
| ac_writes | `oltp_read_write_ac` | 28.59ms | 47.83ms | 1.7× | 4.6% | PASS |

</details>

</details>

## Version-control latency

Wall time: 5m 45s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 24.66ms | 38.00ms | 64.9% | 3.6% | PASS |
| `status_dirty_many_tables` | 26.48ms | 42.00ms | 63.0% | 2.8% | PASS |
| `diff_regular_working_one_table` | 23.29ms | 33.00ms | 70.6% | 2.1% | PASS |
| `diff_regular_working_many_tables` | 28.32ms | 50.00ms | 56.6% | 2.6% | PASS |
| `diff_stat_working_many_tables` | 27.90ms | 48.00ms | 58.1% | 2.4% | PASS |
| `diff_schema_working_many_tables` | 28.46ms | 48.00ms | 59.3% | 2.5% | PASS |
| `branch_list_many_branches` | 16.48ms | 25.00ms | 65.9% | 2.7% | PASS |
| `branch_create_delete` | 19.19ms | 27.00ms | 71.1% | 4.5% | PASS |
| `at_literal_deep_history` | 18.07ms | 28.00ms | 64.5% | 2.8% | PASS |
| `diff_literal_deep_history` | 18.56ms | 28.00ms | 66.3% | 2.3% | PASS |
| `history_literal_deep_history` | 19.09ms | 30.00ms | 63.6% | 2.0% | PASS |
| `checkout_branch_clean` | 23.08ms | 43.00ms | 53.7% | 2.2% | PASS |
| `merge_data_no_conflicts` | 21.49ms | 33.00ms | 65.1% | 3.6% | PASS |
| `merge_data_secondary_index` | 692.42ms | 944.00ms | 73.3% | 2.4% | PASS |
| `merge_schema_no_conflicts` | 17.98ms | 24.00ms | 74.9% | 3.0% | PASS |
| `merge_data_conflicts` | 24.98ms | 33.00ms | 75.7% | 2.3% | PASS |
| `merge_data_conflicts_with_resolve` | 25.52ms | 34.00ms | 75.0% | 2.7% | PASS |

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

candidate: 1bac204.

Fixture file bytes: base 107,968,301; delta 115,879,032.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 434.728 | 394.396–493.341 | 33.170 | 230.03 | 107968301 |
| push_initial | candidate | 763.781 | 760.209–782.072 | 404.887 | 130.93 | 107956807 |
| clone_initial | candidate | 1003.199 | 937.700–1027.939 | 511.090 | 99.68 | 108475105 |
| push_noop | candidate | 0.567 | 0.536–0.611 | 0.270 | — | 0 |
| fetch_noop | candidate | 0.149 | 0.145–0.183 | 0.152 | — | 0 |
| push_incremental | candidate | 175.306 | 173.213–188.528 | 157.836 | 5.70 | 7909775 |
| fetch_incremental | candidate | 89.198 | 84.387–103.276 | 68.348 | 11.21 | 7910084 |
| pull_fast_forward | candidate | 76.370 | 75.004–79.918 | 57.781 | 13.09 | 7910844 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 90.42 | 175.487 | 108.763 | 18329 | 2066 |
| clone_initial | candidate | 90.86 | 295.055 | 110.833 | 47236 | 2353 |
| push_noop | candidate | 41.74 | 0.073 | 0.000 | 27 | 2 |
| fetch_noop | candidate | 41.74 | 0.135 | 0.000 | 42 | 0 |
| push_incremental | candidate | 41.74 | 96.856 | 7.910 | 22410 | 125 |
| fetch_incremental | candidate | 41.74 | 34.472 | 7.910 | 10810 | 124 |
| pull_fast_forward | candidate | 41.74 | 26.497 | 8.959 | 7032 | 142 |

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

candidate: 1bac204.

Fixture file bytes: base 1,087,718,066; delta 1,103,525,194.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 3888.623 | 3641.431–4147.112 | 521.925 | 257.16 | 1087718066 |
| push_initial | candidate | 7146.233 | 6238.685–7161.796 | 3524.086 | 139.93 | 1079566310 |
| clone_initial | candidate | 5155.429 | 5139.965–5201.330 | 5129.304 | 193.97 | 1102407396 |
| push_noop | candidate | 0.417 | 0.365–0.482 | 0.195 | — | 0 |
| fetch_noop | candidate | 0.106 | 0.098–0.106 | 0.107 | — | 0 |
| push_incremental | candidate | 771.792 | 766.492–772.156 | 768.330 | 1.30 | 15806003 |
| fetch_incremental | candidate | 193.861 | 192.393–196.514 | 190.355 | 5.16 | 15806513 |
| pull_fast_forward | candidate | 197.353 | 195.676–202.555 | 192.898 | 5.07 | 15807273 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 134.66 | 2091.400 | 1087.622 | 263381 | 20541 |
| clone_initial | candidate | 123.91 | 3528.872 | 1133.868 | 614949 | 32259 |
| push_noop | candidate | 44.11 | 0.078 | 0.000 | 28 | 2 |
| fetch_noop | candidate | 44.11 | 0.137 | 0.000 | 42 | 0 |
| push_incremental | candidate | 48.09 | 621.566 | 15.928 | 150950 | 313 |
| fetch_incremental | candidate | 50.39 | 61.840 | 15.928 | 13801 | 313 |
| pull_fast_forward | candidate | 50.36 | 61.773 | 16.978 | 13796 | 331 |

</details>


</details>

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh`, `test/vc_perf_ceiling.sh`, and `test/remote_perf.py`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
