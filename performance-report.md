# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-10-31 11:35 UTC
>
> Commit: [`8f8e80d745524cabb1b2625da7c14451b5255ab9`](https://github.com/dolthub/doltlite/commit/8f8e80d745524cabb1b2625da7c14451b5255ab9)
>
> Runner: ubuntu24 20261026.354.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/41189094005)

The SQL workloads compare optimized DoltLite against stock SQLite on the same GitHub-hosted runner, alternating execution order on each repetition. Version-control sections measure DoltLite latency. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.27s | 12.72s | 1.2× | 1.2% | **PASS** |
| Writes | 2.06s | 3.63s | 1.8× | 1.0% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.81s | 12.90s | 1.2× | 1.3% | **PASS** |
| Writes | 3.28s | 4.25s | 1.3× | 1.8% | **PASS** |
| Autocommit writes | 772.35ms | 2.47s | 3.2× | 5.4% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.38s | 2.79s | 1.2× | 1.0% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.63s | 3.41s | 1.3× | 1.5% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.48s | 3.01s | 1.2× | 1.0% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.78s | 3.51s | 1.3× | 1.8% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 360.48ms | 654.57ms | 1.8× | 0.8% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 586.54ms | 1.04s | 1.8× | 1.8% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 522.93ms | 892.68ms | 1.7× | 0.9% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 586.76ms | 1.04s | 1.8× | 1.3% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.44s | 2.81s | 1.2× | 0.9% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.86s | 3.48s | 1.2× | 1.7% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.69s | 3.08s | 1.1× | 1.3% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.82s | 3.53s | 1.3× | 1.6% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 928.23ms | 1.00s | 1.1× | 1.4% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 917.18ms | 1.15s | 1.3× | 2.9% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 670.57ms | 976.56ms | 1.5× | 1.9% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 765.89ms | 1.12s | 1.5× | 1.7% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.34s | 2.79s | 1.2× | 0.9% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.72s | 3.47s | 1.3× | 1.7% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.51s | 3.03s | 1.2× | 1.5% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.74s | 3.55s | 1.3× | 1.5% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 232.62ms | 686.52ms | 3.0× | 3.6% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 189.88ms | 613.07ms | 3.2× | 5.0% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 148.58ms | 534.79ms | 3.6× | 7.9% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 201.27ms | 635.00ms | 3.2× | 5.8% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 20.84ms | 26.32ms | 1.3× | 0.7% | PASS |
| mem_reads | `oltp_range_select` | 9.53ms | 12.47ms | 1.3× | 1.3% | PASS |
| mem_reads | `oltp_sum_range` | 8.69ms | 11.24ms | 1.3× | 1.0% | PASS |
| mem_reads | `oltp_order_range` | 2.47ms | 2.99ms | 1.2× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 3.35ms | 4.04ms | 1.2× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 3.46ms | 4.73ms | 1.4× | 1.1% | PASS |
| mem_reads | `select_random_points` | 9.81ms | 11.61ms | 1.2× | 1.0% | PASS |
| mem_reads | `select_random_ranges` | 3.90ms | 4.43ms | 1.1× | 0.6% | PASS |
| mem_reads | `covering_index_scan` | 6.28ms | 9.34ms | 1.5× | 0.7% | PASS |
| mem_reads | `groupby_scan` | 28.66ms | 32.88ms | 1.1× | 0.4% | PASS |
| mem_reads | `index_join` | 5.05ms | 7.77ms | 1.5× | 1.1% | PASS |
| mem_reads | `index_join_scan` | 2.75ms | 5.07ms | 1.8× | 1.3% | PASS |
| mem_reads | `types_table_scan` | 983.76ms | 1.19s | 1.2× | 0.4% | PASS |
| mem_reads | `table_scan` | 1.19s | 1.35s | 1.1× | 1.9% | PASS |
| mem_reads | `oltp_read_only` | 94.90ms | 118.74ms | 1.3× | 0.9% | PASS |
| mem_writes | `oltp_bulk_insert` | 140.26ms | 253.14ms | 1.8× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 12.71ms | 22.70ms | 1.8× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 44.64ms | 89.20ms | 2.0× | 0.9% | PASS |
| mem_writes | `oltp_update_non_index` | 29.19ms | 54.22ms | 1.9× | 0.9% | PASS |
| mem_writes | `oltp_delete_insert` | 39.17ms | 67.08ms | 1.7× | 0.9% | PASS |
| mem_writes | `oltp_write_only` | 18.78ms | 41.90ms | 2.2× | 0.8% | PASS |
| mem_writes | `types_delete_insert` | 20.33ms | 31.02ms | 1.5× | 1.1% | PASS |
| mem_writes | `oltp_read_write` | 55.38ms | 95.30ms | 1.7× | 0.7% | PASS |
| file_reads | `oltp_point_select` | 46.99ms | 31.93ms | 0.7× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 12.54ms | 13.30ms | 1.1× | 1.0% | PASS |
| file_reads | `oltp_sum_range` | 11.85ms | 11.97ms | 1.0× | 0.9% | PASS |
| file_reads | `oltp_order_range` | 2.84ms | 3.15ms | 1.1× | 1.2% | PASS |
| file_reads | `oltp_distinct_range` | 3.71ms | 4.18ms | 1.1× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 6.38ms | 5.49ms | 0.9× | 1.6% | PASS |
| file_reads | `select_random_points` | 13.15ms | 12.46ms | 0.9× | 0.8% | PASS |
| file_reads | `select_random_ranges` | 6.86ms | 5.14ms | 0.7× | 0.7% | PASS |
| file_reads | `covering_index_scan` | 9.41ms | 10.36ms | 1.1× | 0.8% | PASS |
| file_reads | `groupby_scan` | 29.41ms | 33.48ms | 1.1× | 0.6% | PASS |
| file_reads | `index_join` | 6.89ms | 8.48ms | 1.2× | 1.0% | PASS |
| file_reads | `index_join_scan` | 3.19ms | 5.46ms | 1.7× | 1.8% | PASS |
| file_reads | `types_table_scan` | 998.27ms | 1.21s | 1.2× | 0.6% | PASS |
| file_reads | `table_scan` | 1.16s | 1.34s | 1.1× | 1.0% | PASS |
| file_reads | `oltp_read_only` | 129.28ms | 124.70ms | 1.0× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 192.74ms | 306.74ms | 1.6× | 3.0% | PASS |
| file_writes | `oltp_insert` | 27.59ms | 37.62ms | 1.4× | 2.6% | PASS |
| file_writes | `oltp_update_index` | 148.18ms | 146.58ms | 1.0× | 1.4% | PASS |
| file_writes | `oltp_update_non_index` | 120.56ms | 101.31ms | 0.8× | 0.9% | PASS |
| file_writes | `oltp_delete_insert` | 129.04ms | 123.39ms | 1.0× | 1.2% | PASS |
| file_writes | `oltp_write_only` | 95.59ms | 84.49ms | 0.9× | 0.6% | PASS |
| file_writes | `types_delete_insert` | 82.07ms | 65.46ms | 0.8× | 3.0% | PASS |
| file_writes | `oltp_read_write` | 132.47ms | 137.48ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_point_select` | 29.21ms | 31.62ms | 1.1× | 0.6% | PASS |
| ac_reads | `oltp_range_select` | 10.75ms | 13.25ms | 1.2× | 0.9% | PASS |
| ac_reads | `oltp_sum_range` | 10.13ms | 11.96ms | 1.2× | 0.7% | PASS |
| ac_reads | `oltp_order_range` | 2.64ms | 3.13ms | 1.2× | 1.5% | PASS |
| ac_reads | `oltp_distinct_range` | 3.54ms | 4.18ms | 1.2× | 0.9% | PASS |
| ac_reads | `oltp_index_scan` | 4.77ms | 5.60ms | 1.2× | 1.0% | PASS |
| ac_reads | `select_random_points` | 11.71ms | 12.68ms | 1.1× | 0.8% | PASS |
| ac_reads | `select_random_ranges` | 5.13ms | 5.17ms | 1.0× | 1.1% | PASS |
| ac_reads | `covering_index_scan` | 7.53ms | 10.41ms | 1.4× | 1.2% | PASS |
| ac_reads | `groupby_scan` | 29.09ms | 33.43ms | 1.1× | 0.9% | PASS |
| ac_reads | `index_join` | 6.00ms | 8.52ms | 1.4× | 1.1% | PASS |
| ac_reads | `index_join_scan` | 3.04ms | 5.41ms | 1.8× | 1.8% | PASS |
| ac_reads | `types_table_scan` | 984.08ms | 1.20s | 1.2× | 0.3% | PASS |
| ac_reads | `table_scan` | 1.13s | 1.32s | 1.2× | 0.7% | PASS |
| ac_reads | `oltp_read_only` | 105.07ms | 125.40ms | 1.2× | 0.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 27.26ms | 73.95ms | 2.7× | 3.4% | PASS |
| ac_writes | `oltp_insert_ac` | 29.49ms | 84.69ms | 2.9× | 3.6% | PASS |
| ac_writes | `oltp_update_index_ac` | 30.63ms | 97.04ms | 3.2× | 5.5% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 27.52ms | 79.55ms | 2.9× | 3.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 29.21ms | 88.84ms | 3.0× | 3.5% | PASS |
| ac_writes | `oltp_write_only_ac` | 29.09ms | 87.66ms | 3.0× | 3.7% | PASS |
| ac_writes | `types_delete_insert_ac` | 27.24ms | 80.31ms | 2.9× | 5.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.17ms | 94.48ms | 2.9× | 3.3% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 35.24ms | 43.52ms | 1.2× | 2.0% | PASS |
| mem_reads | `oltp_range_select` | 16.67ms | 17.54ms | 1.1× | 2.5% | PASS |
| mem_reads | `oltp_sum_range` | 15.67ms | 16.09ms | 1.0× | 1.4% | PASS |
| mem_reads | `oltp_order_range` | 3.31ms | 3.61ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_distinct_range` | 4.41ms | 4.82ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 3.92ms | 7.20ms | 1.8× | 1.0% | PASS |
| mem_reads | `select_random_points` | 21.83ms | 23.41ms | 1.1× | 2.6% | PASS |
| mem_reads | `select_random_ranges` | 6.68ms | 7.43ms | 1.1× | 1.3% | PASS |
| mem_reads | `covering_index_scan` | 7.66ms | 11.67ms | 1.5× | 1.1% | PASS |
| mem_reads | `groupby_scan` | 34.55ms | 36.83ms | 1.1× | 0.6% | PASS |
| mem_reads | `index_join` | 10.39ms | 11.04ms | 1.1× | 2.1% | PASS |
| mem_reads | `index_join_scan` | 3.72ms | 6.79ms | 1.8× | 1.8% | PASS |
| mem_reads | `types_table_scan` | 1.08s | 1.47s | 1.4× | 1.5% | PASS |
| mem_reads | `table_scan` | 1.24s | 1.59s | 1.3× | 1.8% | PASS |
| mem_reads | `oltp_read_only` | 140.44ms | 157.63ms | 1.1× | 2.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 237.00ms | 392.44ms | 1.7× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 17.70ms | 39.52ms | 2.2× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 65.51ms | 146.60ms | 2.2× | 1.8% | PASS |
| mem_writes | `oltp_update_non_index` | 47.71ms | 86.12ms | 1.8× | 1.9% | PASS |
| mem_writes | `oltp_delete_insert` | 54.08ms | 109.06ms | 2.0× | 1.9% | PASS |
| mem_writes | `oltp_write_only` | 28.18ms | 57.94ms | 2.1× | 1.5% | PASS |
| mem_writes | `types_delete_insert` | 38.10ms | 63.09ms | 1.7× | 2.1% | PASS |
| mem_writes | `oltp_read_write` | 98.26ms | 145.76ms | 1.5× | 2.6% | PASS |
| file_reads | `oltp_point_select` | 101.30ms | 61.00ms | 0.6× | 1.2% | PASS |
| file_reads | `oltp_range_select` | 23.92ms | 19.45ms | 0.8× | 2.3% | PASS |
| file_reads | `oltp_sum_range` | 22.43ms | 18.15ms | 0.8× | 1.7% | PASS |
| file_reads | `oltp_order_range` | 4.15ms | 3.83ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_distinct_range` | 5.22ms | 5.07ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 10.71ms | 9.07ms | 0.8× | 1.2% | PASS |
| file_reads | `select_random_points` | 30.35ms | 26.40ms | 0.9× | 2.2% | PASS |
| file_reads | `select_random_ranges` | 13.78ms | 9.39ms | 0.7× | 1.5% | PASS |
| file_reads | `covering_index_scan` | 14.64ms | 13.75ms | 0.9× | 1.2% | PASS |
| file_reads | `groupby_scan` | 35.57ms | 37.17ms | 1.0× | 1.1% | PASS |
| file_reads | `index_join` | 14.55ms | 12.36ms | 0.8× | 1.7% | PASS |
| file_reads | `index_join_scan` | 4.72ms | 7.19ms | 1.5× | 2.4% | PASS |
| file_reads | `types_table_scan` | 1.08s | 1.48s | 1.4× | 1.0% | PASS |
| file_reads | `table_scan` | 1.26s | 1.59s | 1.3× | 2.1% | PASS |
| file_reads | `oltp_read_only` | 240.31ms | 184.39ms | 0.8× | 1.8% | PASS |
| file_writes | `oltp_bulk_insert` | 262.41ms | 410.97ms | 1.6× | 0.9% | PASS |
| file_writes | `oltp_insert` | 25.35ms | 45.20ms | 1.8× | 1.3% | PASS |
| file_writes | `oltp_update_index` | 132.80ms | 167.70ms | 1.3× | 7.8% | PASS |
| file_writes | `oltp_update_non_index` | 99.03ms | 100.66ms | 1.0× | 7.9% | PASS |
| file_writes | `oltp_delete_insert` | 94.93ms | 125.70ms | 1.3× | 1.4% | PASS |
| file_writes | `oltp_write_only` | 87.81ms | 70.75ms | 0.8× | 10.1% | PASS |
| file_writes | `types_delete_insert` | 72.13ms | 73.14ms | 1.0× | 2.0% | PASS |
| file_writes | `oltp_read_write` | 142.74ms | 158.69ms | 1.1× | 3.8% | PASS |
| ac_reads | `oltp_point_select` | 57.72ms | 61.05ms | 1.1× | 1.6% | PASS |
| ac_reads | `oltp_range_select` | 19.91ms | 19.43ms | 1.0× | 2.9% | PASS |
| ac_reads | `oltp_sum_range` | 18.70ms | 18.14ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_order_range` | 3.76ms | 3.85ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_distinct_range` | 4.79ms | 5.08ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_index_scan` | 6.55ms | 9.05ms | 1.4× | 1.8% | PASS |
| ac_reads | `select_random_points` | 26.05ms | 26.44ms | 1.0× | 3.3% | PASS |
| ac_reads | `select_random_ranges` | 9.27ms | 9.41ms | 1.0× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 10.34ms | 13.69ms | 1.3× | 1.5% | PASS |
| ac_reads | `groupby_scan` | 35.22ms | 37.13ms | 1.1× | 0.7% | PASS |
| ac_reads | `index_join` | 12.54ms | 12.30ms | 1.0× | 2.5% | PASS |
| ac_reads | `index_join_scan` | 4.32ms | 7.12ms | 1.6× | 2.4% | PASS |
| ac_reads | `types_table_scan` | 1.08s | 1.48s | 1.4× | 1.7% | PASS |
| ac_reads | `table_scan` | 1.26s | 1.59s | 1.3× | 2.2% | PASS |
| ac_reads | `oltp_read_only` | 174.55ms | 183.72ms | 1.1× | 1.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.66ms | 61.51ms | 2.8× | 5.8% | PASS |
| ac_writes | `oltp_insert_ac` | 23.68ms | 73.25ms | 3.1× | 4.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 24.70ms | 89.10ms | 3.6× | 4.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.01ms | 71.37ms | 3.4× | 5.4% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.02ms | 81.71ms | 3.4× | 5.7% | PASS |
| ac_writes | `oltp_write_only_ac` | 23.39ms | 78.88ms | 3.4× | 3.7% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.07ms | 70.68ms | 3.2× | 7.0% | PASS |
| ac_writes | `oltp_read_write_ac` | 29.34ms | 86.58ms | 3.0× | 4.6% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 30.64ms | 36.93ms | 1.2× | 1.1% | PASS |
| mem_reads | `oltp_range_select` | 13.27ms | 14.60ms | 1.1× | 0.7% | PASS |
| mem_reads | `oltp_sum_range` | 12.67ms | 14.02ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_order_range` | 2.81ms | 3.18ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_distinct_range` | 3.59ms | 4.10ms | 1.1× | 0.7% | PASS |
| mem_reads | `oltp_index_scan` | 3.48ms | 6.11ms | 1.8× | 1.1% | PASS |
| mem_reads | `select_random_points` | 19.39ms | 22.03ms | 1.1× | 2.1% | PASS |
| mem_reads | `select_random_ranges` | 5.48ms | 6.56ms | 1.2× | 0.9% | PASS |
| mem_reads | `covering_index_scan` | 6.56ms | 9.77ms | 1.5× | 1.0% | PASS |
| mem_reads | `groupby_scan` | 30.44ms | 33.30ms | 1.1× | 0.5% | PASS |
| mem_reads | `index_join` | 9.73ms | 10.76ms | 1.1× | 1.4% | PASS |
| mem_reads | `index_join_scan` | 3.03ms | 6.63ms | 2.2× | 1.4% | PASS |
| mem_reads | `types_table_scan` | 1.03s | 1.30s | 1.3× | 0.7% | PASS |
| mem_reads | `table_scan` | 1.19s | 1.40s | 1.2× | 0.6% | PASS |
| mem_reads | `oltp_read_only` | 126.89ms | 141.98ms | 1.1× | 0.4% | PASS |
| mem_writes | `oltp_bulk_insert` | 205.09ms | 322.87ms | 1.6× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 16.01ms | 33.90ms | 2.1× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 61.23ms | 135.90ms | 2.2× | 0.8% | PASS |
| mem_writes | `oltp_update_non_index` | 43.72ms | 72.84ms | 1.7× | 1.1% | PASS |
| mem_writes | `oltp_delete_insert` | 48.47ms | 97.86ms | 2.0× | 2.0% | PASS |
| mem_writes | `oltp_write_only` | 24.69ms | 48.88ms | 2.0× | 0.9% | PASS |
| mem_writes | `types_delete_insert` | 34.37ms | 52.70ms | 1.5× | 0.8% | PASS |
| mem_writes | `oltp_read_write` | 89.35ms | 127.71ms | 1.4× | 2.8% | PASS |
| file_reads | `oltp_point_select` | 61.04ms | 46.68ms | 0.8× | 1.1% | PASS |
| file_reads | `oltp_range_select` | 17.02ms | 16.10ms | 0.9× | 0.8% | PASS |
| file_reads | `oltp_sum_range` | 16.54ms | 15.48ms | 0.9× | 1.6% | PASS |
| file_reads | `oltp_order_range` | 3.18ms | 3.32ms | 1.0× | 2.7% | PASS |
| file_reads | `oltp_distinct_range` | 4.03ms | 4.31ms | 1.1× | 0.8% | PASS |
| file_reads | `oltp_index_scan` | 6.49ms | 7.20ms | 1.1× | 1.8% | PASS |
| file_reads | `select_random_points` | 23.03ms | 23.19ms | 1.0× | 2.1% | PASS |
| file_reads | `select_random_ranges` | 8.43ms | 7.51ms | 0.9× | 1.3% | PASS |
| file_reads | `covering_index_scan` | 9.71ms | 10.91ms | 1.1× | 0.9% | PASS |
| file_reads | `groupby_scan` | 30.81ms | 33.43ms | 1.1× | 0.4% | PASS |
| file_reads | `index_join` | 11.76ms | 11.34ms | 1.0× | 1.9% | PASS |
| file_reads | `index_join_scan` | 3.49ms | 6.85ms | 2.0× | 3.6% | PASS |
| file_reads | `types_table_scan` | 1.10s | 1.32s | 1.2× | 2.9% | PASS |
| file_reads | `table_scan` | 1.23s | 1.42s | 1.2× | 1.0% | PASS |
| file_reads | `oltp_read_only` | 166.64ms | 152.21ms | 0.9× | 1.3% | PASS |
| file_writes | `oltp_bulk_insert` | 220.97ms | 335.84ms | 1.5× | 0.7% | PASS |
| file_writes | `oltp_insert` | 20.98ms | 38.56ms | 1.8× | 2.0% | PASS |
| file_writes | `oltp_update_index` | 79.34ms | 149.24ms | 1.9× | 1.3% | PASS |
| file_writes | `oltp_update_non_index` | 84.61ms | 86.02ms | 1.0× | 16.3% | PASS |
| file_writes | `oltp_delete_insert` | 69.95ms | 111.73ms | 1.6× | 1.5% | PASS |
| file_writes | `oltp_write_only` | 41.97ms | 60.23ms | 1.4× | 1.8% | PASS |
| file_writes | `types_delete_insert` | 51.22ms | 59.46ms | 1.2× | 2.6% | PASS |
| file_writes | `oltp_read_write` | 101.54ms | 135.49ms | 1.3× | 2.5% | PASS |
| ac_reads | `oltp_point_select` | 40.53ms | 45.97ms | 1.1× | 1.5% | PASS |
| ac_reads | `oltp_range_select` | 15.76ms | 16.43ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_sum_range` | 14.44ms | 15.45ms | 1.1× | 1.9% | PASS |
| ac_reads | `oltp_order_range` | 3.09ms | 3.39ms | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_distinct_range` | 3.84ms | 4.32ms | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_index_scan` | 4.85ms | 7.29ms | 1.5× | 1.4% | PASS |
| ac_reads | `select_random_points` | 23.36ms | 24.80ms | 1.1× | 1.5% | PASS |
| ac_reads | `select_random_ranges` | 6.76ms | 7.55ms | 1.1× | 1.6% | PASS |
| ac_reads | `covering_index_scan` | 7.87ms | 10.95ms | 1.4× | 1.3% | PASS |
| ac_reads | `groupby_scan` | 30.45ms | 33.27ms | 1.1× | 0.5% | PASS |
| ac_reads | `index_join` | 10.93ms | 11.37ms | 1.0× | 1.8% | PASS |
| ac_reads | `index_join_scan` | 3.32ms | 6.71ms | 2.0× | 2.3% | PASS |
| ac_reads | `types_table_scan` | 1.00s | 1.28s | 1.3× | 2.1% | PASS |
| ac_reads | `table_scan` | 1.21s | 1.41s | 1.2× | 1.9% | PASS |
| ac_reads | `oltp_read_only` | 133.22ms | 149.26ms | 1.1× | 1.1% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 16.25ms | 48.30ms | 3.0× | 5.1% | PASS |
| ac_writes | `oltp_insert_ac` | 18.98ms | 68.86ms | 3.6× | 8.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 20.14ms | 79.97ms | 4.0× | 5.4% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 16.54ms | 59.12ms | 3.6× | 8.9% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 19.00ms | 70.74ms | 3.7× | 10.5% | PASS |
| ac_writes | `oltp_write_only_ac` | 18.62ms | 71.04ms | 3.8× | 8.4% | PASS |
| ac_writes | `types_delete_insert_ac` | 16.74ms | 61.84ms | 3.7× | 7.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 22.31ms | 74.91ms | 3.4× | 6.2% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.01ms | 43.83ms | 1.3× | 1.9% | PASS |
| mem_reads | `oltp_range_select` | 20.18ms | 25.68ms | 1.3× | 1.9% | PASS |
| mem_reads | `oltp_sum_range` | 18.61ms | 23.77ms | 1.3× | 1.1% | PASS |
| mem_reads | `oltp_order_range` | 3.56ms | 4.38ms | 1.2× | 1.8% | PASS |
| mem_reads | `oltp_distinct_range` | 4.64ms | 5.60ms | 1.2× | 1.6% | PASS |
| mem_reads | `oltp_index_scan` | 4.61ms | 6.95ms | 1.5× | 2.5% | PASS |
| mem_reads | `select_random_points` | 29.42ms | 34.38ms | 1.2× | 2.2% | PASS |
| mem_reads | `select_random_ranges` | 7.77ms | 9.78ms | 1.3× | 1.5% | PASS |
| mem_reads | `covering_index_scan` | 7.61ms | 11.54ms | 1.5× | 1.6% | PASS |
| mem_reads | `groupby_scan` | 36.16ms | 43.22ms | 1.2× | 1.0% | PASS |
| mem_reads | `index_join` | 7.90ms | 12.52ms | 1.6× | 3.0% | PASS |
| mem_reads | `index_join_scan` | 3.95ms | 6.79ms | 1.7× | 3.6% | PASS |
| mem_reads | `types_table_scan` | 1.09s | 1.48s | 1.4× | 1.8% | PASS |
| mem_reads | `table_scan` | 1.36s | 1.61s | 1.2× | 1.1% | PASS |
| mem_reads | `oltp_read_only` | 152.85ms | 194.66ms | 1.3× | 2.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 240.04ms | 378.85ms | 1.6× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 18.50ms | 36.24ms | 2.0× | 0.6% | PASS |
| mem_writes | `oltp_update_index` | 66.01ms | 133.41ms | 2.0× | 1.3% | PASS |
| mem_writes | `oltp_update_non_index` | 51.76ms | 94.40ms | 1.8× | 1.3% | PASS |
| mem_writes | `oltp_delete_insert` | 50.29ms | 103.68ms | 2.1× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 27.75ms | 62.76ms | 2.3× | 1.7% | PASS |
| mem_writes | `types_delete_insert` | 32.56ms | 59.17ms | 1.8× | 1.7% | PASS |
| mem_writes | `oltp_read_write` | 99.84ms | 169.27ms | 1.7× | 1.2% | PASS |
| file_reads | `oltp_point_select` | 97.80ms | 61.13ms | 0.6× | 1.3% | PASS |
| file_reads | `oltp_range_select` | 26.60ms | 27.31ms | 1.0× | 1.7% | PASS |
| file_reads | `oltp_sum_range` | 24.86ms | 25.61ms | 1.0× | 1.5% | PASS |
| file_reads | `oltp_order_range` | 4.45ms | 4.68ms | 1.1× | 1.6% | PASS |
| file_reads | `oltp_distinct_range` | 5.53ms | 5.92ms | 1.1× | 1.8% | PASS |
| file_reads | `oltp_index_scan` | 11.20ms | 8.82ms | 0.8× | 1.8% | PASS |
| file_reads | `select_random_points` | 37.37ms | 37.03ms | 1.0× | 1.5% | PASS |
| file_reads | `select_random_ranges` | 14.80ms | 11.92ms | 0.8× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 14.78ms | 13.59ms | 0.9× | 1.7% | PASS |
| file_reads | `groupby_scan` | 36.74ms | 43.29ms | 1.2× | 0.8% | PASS |
| file_reads | `index_join` | 11.80ms | 13.64ms | 1.2× | 1.9% | PASS |
| file_reads | `index_join_scan` | 4.84ms | 7.15ms | 1.5× | 2.0% | PASS |
| file_reads | `types_table_scan` | 1.06s | 1.46s | 1.4× | 1.2% | PASS |
| file_reads | `table_scan` | 1.22s | 1.59s | 1.3× | 2.6% | PASS |
| file_reads | `oltp_read_only` | 253.75ms | 224.36ms | 0.9× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 257.05ms | 393.72ms | 1.5× | 0.9% | PASS |
| file_writes | `oltp_insert` | 26.81ms | 41.13ms | 1.5× | 1.4% | PASS |
| file_writes | `oltp_update_index` | 97.25ms | 152.14ms | 1.6× | 2.0% | PASS |
| file_writes | `oltp_update_non_index` | 77.48ms | 100.65ms | 1.3× | 1.8% | PASS |
| file_writes | `oltp_delete_insert` | 78.07ms | 115.70ms | 1.5× | 1.4% | PASS |
| file_writes | `oltp_write_only` | 52.04ms | 69.25ms | 1.3× | 1.7% | PASS |
| file_writes | `types_delete_insert` | 50.29ms | 65.87ms | 1.3× | 2.0% | PASS |
| file_writes | `oltp_read_write` | 126.90ms | 176.60ms | 1.4× | 1.8% | PASS |
| ac_reads | `oltp_point_select` | 55.28ms | 61.55ms | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_range_select` | 22.59ms | 27.44ms | 1.2× | 1.2% | PASS |
| ac_reads | `oltp_sum_range` | 20.74ms | 25.76ms | 1.2× | 1.4% | PASS |
| ac_reads | `oltp_order_range` | 3.97ms | 4.67ms | 1.2× | 1.8% | PASS |
| ac_reads | `oltp_distinct_range` | 5.13ms | 5.95ms | 1.2× | 1.5% | PASS |
| ac_reads | `oltp_index_scan` | 7.23ms | 8.87ms | 1.2× | 2.4% | PASS |
| ac_reads | `select_random_points` | 32.38ms | 37.71ms | 1.2× | 1.7% | PASS |
| ac_reads | `select_random_ranges` | 10.26ms | 11.95ms | 1.2× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 10.41ms | 13.63ms | 1.3× | 1.6% | PASS |
| ac_reads | `groupby_scan` | 36.46ms | 43.67ms | 1.2× | 0.7% | PASS |
| ac_reads | `index_join` | 9.69ms | 13.77ms | 1.4× | 1.9% | PASS |
| ac_reads | `index_join_scan` | 4.51ms | 7.26ms | 1.6× | 3.0% | PASS |
| ac_reads | `types_table_scan` | 1.05s | 1.47s | 1.4× | 1.0% | PASS |
| ac_reads | `table_scan` | 1.29s | 1.60s | 1.2× | 2.5% | PASS |
| ac_reads | `oltp_read_only` | 185.93ms | 222.88ms | 1.2× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.85ms | 63.49ms | 2.8× | 7.3% | PASS |
| ac_writes | `oltp_insert_ac` | 24.85ms | 79.34ms | 3.2× | 5.3% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.49ms | 91.59ms | 3.5× | 6.0% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.09ms | 72.85ms | 3.3× | 5.4% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.95ms | 81.55ms | 3.3× | 8.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.31ms | 80.66ms | 3.2× | 5.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.75ms | 73.97ms | 3.3× | 12.0% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.96ms | 91.56ms | 2.9× | 5.1% | PASS |

</details>

</details>

## Version-control latency

Wall time: 5m 51s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 24.41ms | 38.00ms | 64.2% | 1.3% | PASS |
| `status_dirty_many_tables` | 26.81ms | 42.00ms | 63.8% | 1.3% | PASS |
| `diff_regular_working_one_table` | 21.29ms | 33.00ms | 64.5% | 1.4% | PASS |
| `diff_regular_working_many_tables` | 29.00ms | 50.00ms | 58.0% | 2.0% | PASS |
| `diff_stat_working_many_tables` | 29.17ms | 48.00ms | 60.8% | 2.0% | PASS |
| `diff_schema_working_many_tables` | 29.69ms | 48.00ms | 61.9% | 1.7% | PASS |
| `branch_list_many_branches` | 17.27ms | 25.00ms | 69.1% | 2.4% | PASS |
| `branch_create_delete` | 19.01ms | 27.00ms | 70.4% | 2.2% | PASS |
| `at_literal_deep_history` | 19.01ms | 28.00ms | 67.9% | 2.4% | PASS |
| `diff_literal_deep_history` | 18.47ms | 28.00ms | 66.0% | 1.9% | PASS |
| `history_literal_deep_history` | 19.44ms | 30.00ms | 64.8% | 1.7% | PASS |
| `checkout_branch_clean` | 24.23ms | 43.00ms | 56.4% | 1.6% | PASS |
| `merge_data_no_conflicts` | 20.41ms | 33.00ms | 61.9% | 2.4% | PASS |
| `merge_data_secondary_index` | 687.92ms | 944.00ms | 72.9% | 1.7% | PASS |
| `merge_schema_no_conflicts` | 16.37ms | 24.00ms | 68.2% | 2.0% | PASS |
| `merge_data_conflicts` | 22.88ms | 33.00ms | 69.3% | 3.2% | PASS |
| `merge_data_conflicts_with_resolve` | 23.81ms | 34.00ms | 70.0% | 2.8% | PASS |

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

candidate: 8f8e80d.

Fixture file bytes: base 107,968,266; delta 115,878,997.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 408.350 | 401.617–498.082 | 32.572 | 244.89 | 107968266 |
| push_initial | candidate | 750.574 | 735.229–762.075 | 376.901 | 133.23 | 107956772 |
| clone_initial | candidate | 1012.910 | 866.890–1267.135 | 494.248 | 98.73 | 108475035 |
| push_noop | candidate | 0.558 | 0.538–0.608 | 0.287 | — | 0 |
| fetch_noop | candidate | 0.162 | 0.146–0.167 | 0.163 | — | 0 |
| push_incremental | candidate | 182.446 | 181.772–198.447 | 161.670 | 5.48 | 7909775 |
| fetch_incremental | candidate | 90.796 | 86.625–97.447 | 70.610 | 11.01 | 7910084 |
| pull_fast_forward | candidate | 82.849 | 77.230–84.158 | 61.745 | 12.07 | 7910844 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 90.57 | 175.487 | 108.763 | 18329 | 2066 |
| clone_initial | candidate | 90.83 | 295.222 | 109.784 | 47275 | 2337 |
| push_noop | candidate | 42.42 | 0.073 | 0.000 | 26 | 2 |
| fetch_noop | candidate | 42.42 | 0.135 | 0.000 | 40 | 0 |
| push_incremental | candidate | 42.42 | 96.932 | 7.910 | 22427 | 125 |
| fetch_incremental | candidate | 42.42 | 34.472 | 7.910 | 10808 | 124 |
| pull_fast_forward | candidate | 42.42 | 26.497 | 7.911 | 7031 | 126 |

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

candidate: 8f8e80d.

Fixture file bytes: base 1,087,718,031; delta 1,103,525,159.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 2674.862 | 2638.610–2699.566 | 394.111 | 373.85 | 1087718031 |
| push_initial | candidate | 8145.813 | 8133.464–8182.264 | 5935.477 | 122.76 | 1079566275 |
| clone_initial | candidate | 8929.595 | 8863.631–9062.672 | 8690.178 | 111.99 | 1102407326 |
| push_noop | candidate | 0.846 | 0.780–0.854 | 0.393 | — | 0 |
| fetch_noop | candidate | 0.205 | 0.191–0.222 | 0.208 | — | 0 |
| push_incremental | candidate | 1333.786 | 1332.756–1353.080 | 1328.230 | 0.75 | 15806003 |
| fetch_incremental | candidate | 272.973 | 270.994–279.552 | 267.351 | 3.66 | 15806513 |
| pull_fast_forward | candidate | 278.837 | 275.335–282.310 | 272.499 | 3.59 | 15807273 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 134.76 | 2092.011 | 1087.622 | 263530 | 20541 |
| clone_initial | candidate | 123.98 | 3521.445 | 1132.819 | 613235 | 32243 |
| push_noop | candidate | 45.07 | 0.078 | 0.000 | 27 | 2 |
| fetch_noop | candidate | 45.07 | 0.137 | 0.000 | 40 | 0 |
| push_incremental | candidate | 48.16 | 618.515 | 15.928 | 150245 | 313 |
| fetch_incremental | candidate | 50.36 | 61.971 | 15.928 | 13831 | 313 |
| pull_fast_forward | candidate | 50.38 | 61.904 | 15.929 | 13827 | 315 |

</details>


</details>

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh`, `test/vc_perf_ceiling.sh`, and `test/remote_perf.py`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
