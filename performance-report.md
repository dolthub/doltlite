# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-11-05 11:27 UTC
>
> Commit: [`b422859f0136c5475fcde91b00054aeb1a945e8a`](https://github.com/dolthub/doltlite/commit/b422859f0136c5475fcde91b00054aeb1a945e8a)
>
> Runner: ubuntu24 20261026.354.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/42003175597)

The SQL workloads compare optimized DoltLite against stock SQLite on the same GitHub-hosted runner, alternating execution order on each repetition. Version-control sections measure DoltLite latency. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.22s | 10.78s | 1.2× | 1.7% | **PASS** |
| Writes | 1.91s | 3.15s | 1.6× | 1.5% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.67s | 10.68s | 1.1× | 1.2% | **PASS** |
| Writes | 3.56s | 4.07s | 1.1× | 3.0% | **PASS** |
| Autocommit writes | 758.68ms | 2.37s | 3.1× | 9.3% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.48s | 2.86s | 1.2× | 1.7% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.01s | 2.19s | 1.1× | 2.4% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.64s | 3.25s | 1.2× | 2.1% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.09s | 2.48s | 1.2× | 1.0% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 447.22ms | 798.27ms | 1.8× | 1.3% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 376.16ms | 597.51ms | 1.6× | 2.3% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 617.86ms | 1.02s | 1.7× | 2.7% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 470.37ms | 730.92ms | 1.6× | 1.1% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.65s | 2.91s | 1.1× | 1.2% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 1.80s | 1.93s | 1.1× | 1.1% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.90s | 3.30s | 1.1× | 1.7% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.32s | 2.54s | 1.1× | 0.9% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 612.59ms | 860.28ms | 1.4× | 2.0% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.07s | 1.01s | 0.9× | 4.2% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 853.49ms | 1.15s | 1.3× | 2.0% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 1.02s | 1.06s | 1.0× | 9.1% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.52s | 2.91s | 1.2× | 1.7% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 1.88s | 2.05s | 1.1× | 2.4% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.69s | 3.28s | 1.2× | 2.2% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.17s | 2.54s | 1.2× | 0.9% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 198.44ms | 682.64ms | 3.4× | 9.8% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 165.44ms | 469.21ms | 2.8× | 4.0% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 207.38ms | 656.82ms | 3.2× | 6.8% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 187.41ms | 556.67ms | 3.0× | 22.6% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.53ms | 33.48ms | 1.4× | 1.8% | PASS |
| mem_reads | `oltp_range_select` | 10.50ms | 13.24ms | 1.3× | 2.1% | PASS |
| mem_reads | `oltp_sum_range` | 9.66ms | 12.77ms | 1.3× | 2.1% | PASS |
| mem_reads | `oltp_order_range` | 2.60ms | 3.10ms | 1.2× | 0.8% | PASS |
| mem_reads | `oltp_distinct_range` | 3.72ms | 4.28ms | 1.2× | 1.4% | PASS |
| mem_reads | `oltp_index_scan` | 3.97ms | 5.98ms | 1.5× | 1.7% | PASS |
| mem_reads | `select_random_points` | 13.49ms | 12.49ms | 0.9× | 2.5% | PASS |
| mem_reads | `select_random_ranges` | 4.73ms | 5.72ms | 1.2× | 1.7% | PASS |
| mem_reads | `covering_index_scan` | 7.73ms | 11.39ms | 1.5× | 0.7% | PASS |
| mem_reads | `groupby_scan` | 30.30ms | 33.96ms | 1.1× | 1.0% | PASS |
| mem_reads | `index_join` | 5.86ms | 9.34ms | 1.6× | 1.8% | PASS |
| mem_reads | `index_join_scan` | 3.28ms | 6.07ms | 1.9× | 2.0% | PASS |
| mem_reads | `types_table_scan` | 1.05s | 1.24s | 1.2× | 1.7% | PASS |
| mem_reads | `table_scan` | 1.21s | 1.34s | 1.1× | 1.7% | PASS |
| mem_reads | `oltp_read_only` | 105.01ms | 131.14ms | 1.2× | 1.2% | PASS |
| mem_writes | `oltp_bulk_insert` | 182.74ms | 317.11ms | 1.7× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 15.39ms | 27.46ms | 1.8× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 50.69ms | 99.86ms | 2.0× | 1.5% | PASS |
| mem_writes | `oltp_update_non_index` | 35.59ms | 67.19ms | 1.9× | 1.5% | PASS |
| mem_writes | `oltp_delete_insert` | 46.19ms | 79.98ms | 1.7× | 1.1% | PASS |
| mem_writes | `oltp_write_only` | 22.41ms | 48.53ms | 2.2× | 1.4% | PASS |
| mem_writes | `types_delete_insert` | 24.86ms | 40.25ms | 1.6× | 1.3% | PASS |
| mem_writes | `oltp_read_write` | 69.34ms | 117.89ms | 1.7× | 2.1% | PASS |
| file_reads | `oltp_point_select` | 89.67ms | 50.81ms | 0.6× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 17.57ms | 15.16ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_sum_range` | 16.63ms | 14.80ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_order_range` | 3.53ms | 3.41ms | 1.0× | 2.6% | PASS |
| file_reads | `oltp_distinct_range` | 4.61ms | 4.57ms | 1.0× | 1.1% | PASS |
| file_reads | `oltp_index_scan` | 11.09ms | 8.01ms | 0.7× | 1.5% | PASS |
| file_reads | `select_random_points` | 20.78ms | 14.19ms | 0.7× | 1.2% | PASS |
| file_reads | `select_random_ranges` | 11.71ms | 7.58ms | 0.6× | 0.9% | PASS |
| file_reads | `covering_index_scan` | 15.15ms | 13.41ms | 0.9× | 1.6% | PASS |
| file_reads | `groupby_scan` | 31.19ms | 34.13ms | 1.1× | 1.1% | PASS |
| file_reads | `index_join` | 9.89ms | 10.65ms | 1.1× | 1.4% | PASS |
| file_reads | `index_join_scan` | 4.15ms | 6.27ms | 1.5× | 1.7% | PASS |
| file_reads | `types_table_scan` | 1.03s | 1.23s | 1.2× | 0.8% | PASS |
| file_reads | `table_scan` | 1.18s | 1.34s | 1.1× | 1.1% | PASS |
| file_reads | `oltp_read_only` | 203.94ms | 157.67ms | 0.8× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 197.55ms | 328.38ms | 1.7× | 0.8% | PASS |
| file_writes | `oltp_insert` | 22.59ms | 30.94ms | 1.4× | 1.5% | PASS |
| file_writes | `oltp_update_index` | 78.34ms | 110.61ms | 1.4× | 2.5% | PASS |
| file_writes | `oltp_update_non_index` | 59.77ms | 74.20ms | 1.2× | 2.0% | PASS |
| file_writes | `oltp_delete_insert` | 70.16ms | 91.26ms | 1.3× | 1.8% | PASS |
| file_writes | `oltp_write_only` | 46.60ms | 54.33ms | 1.2× | 2.8% | PASS |
| file_writes | `types_delete_insert` | 41.26ms | 46.88ms | 1.1× | 2.5% | PASS |
| file_writes | `oltp_read_write` | 96.32ms | 123.68ms | 1.3× | 2.0% | PASS |
| ac_reads | `oltp_point_select` | 46.30ms | 50.92ms | 1.1× | 0.9% | PASS |
| ac_reads | `oltp_range_select` | 12.87ms | 15.14ms | 1.2× | 2.1% | PASS |
| ac_reads | `oltp_sum_range` | 12.10ms | 14.77ms | 1.2× | 2.1% | PASS |
| ac_reads | `oltp_order_range` | 3.02ms | 3.38ms | 1.1× | 2.1% | PASS |
| ac_reads | `oltp_distinct_range` | 4.09ms | 4.53ms | 1.1× | 2.0% | PASS |
| ac_reads | `oltp_index_scan` | 6.49ms | 8.03ms | 1.2× | 1.4% | PASS |
| ac_reads | `select_random_points` | 16.24ms | 14.36ms | 0.9× | 1.9% | PASS |
| ac_reads | `select_random_ranges` | 7.17ms | 7.70ms | 1.1× | 1.7% | PASS |
| ac_reads | `covering_index_scan` | 10.59ms | 13.64ms | 1.3× | 1.5% | PASS |
| ac_reads | `groupby_scan` | 30.70ms | 34.28ms | 1.1× | 1.0% | PASS |
| ac_reads | `index_join` | 7.54ms | 10.86ms | 1.4× | 2.0% | PASS |
| ac_reads | `index_join_scan` | 3.80ms | 6.35ms | 1.7× | 2.2% | PASS |
| ac_reads | `types_table_scan` | 1.04s | 1.23s | 1.2× | 0.9% | PASS |
| ac_reads | `table_scan` | 1.18s | 1.34s | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_read_only` | 140.00ms | 158.41ms | 1.1× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.11ms | 69.68ms | 3.2× | 9.2% | PASS |
| ac_writes | `oltp_insert_ac` | 24.90ms | 87.42ms | 3.5× | 7.2% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.55ms | 99.60ms | 3.6× | 10.0% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.02ms | 77.75ms | 3.5× | 12.2% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.63ms | 86.58ms | 3.5× | 9.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.17ms | 86.49ms | 3.4× | 11.4% | PASS |
| ac_writes | `types_delete_insert_ac` | 21.70ms | 79.14ms | 3.6× | 9.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 30.36ms | 95.99ms | 3.2× | 9.4% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 21.97ms | 23.33ms | 1.1× | 3.5% | PASS |
| mem_reads | `oltp_range_select` | 10.19ms | 9.84ms | 1.0× | 4.9% | PASS |
| mem_reads | `oltp_sum_range` | 9.92ms | 9.82ms | 1.0× | 3.3% | PASS |
| mem_reads | `oltp_order_range` | 2.09ms | 2.18ms | 1.0× | 2.4% | PASS |
| mem_reads | `oltp_distinct_range` | 2.68ms | 2.69ms | 1.0× | 1.9% | PASS |
| mem_reads | `oltp_index_scan` | 2.60ms | 4.62ms | 1.8× | 1.8% | PASS |
| mem_reads | `select_random_points` | 18.35ms | 16.09ms | 0.9× | 4.0% | PASS |
| mem_reads | `select_random_ranges` | 4.72ms | 4.29ms | 0.9× | 1.7% | PASS |
| mem_reads | `covering_index_scan` | 4.09ms | 6.90ms | 1.7× | 2.7% | PASS |
| mem_reads | `groupby_scan` | 22.28ms | 21.98ms | 1.0× | 2.0% | PASS |
| mem_reads | `index_join` | 8.66ms | 8.36ms | 1.0× | 2.7% | PASS |
| mem_reads | `index_join_scan` | 2.91ms | 5.94ms | 2.0× | 2.9% | PASS |
| mem_reads | `types_table_scan` | 829.93ms | 948.57ms | 1.1× | 1.0% | PASS |
| mem_reads | `table_scan` | 959.84ms | 1.03s | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_read_only` | 106.88ms | 96.21ms | 0.9× | 1.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 144.20ms | 213.87ms | 1.5× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 10.79ms | 22.95ms | 2.1× | 2.2% | PASS |
| mem_writes | `oltp_update_index` | 44.11ms | 87.69ms | 2.0× | 2.9% | PASS |
| mem_writes | `oltp_update_non_index` | 35.19ms | 52.02ms | 1.5× | 2.7% | PASS |
| mem_writes | `oltp_delete_insert` | 39.81ms | 70.67ms | 1.8× | 2.3% | PASS |
| mem_writes | `oltp_write_only` | 20.84ms | 37.06ms | 1.8× | 1.7% | PASS |
| mem_writes | `types_delete_insert` | 25.75ms | 38.12ms | 1.5× | 2.0% | PASS |
| mem_writes | `oltp_read_write` | 55.47ms | 75.13ms | 1.4× | 3.1% | PASS |
| file_reads | `oltp_point_select` | 69.31ms | 35.83ms | 0.5× | 1.3% | PASS |
| file_reads | `oltp_range_select` | 15.64ms | 11.00ms | 0.7× | 2.1% | PASS |
| file_reads | `oltp_sum_range` | 15.39ms | 10.87ms | 0.7× | 2.2% | PASS |
| file_reads | `oltp_order_range` | 2.83ms | 2.37ms | 0.8× | 1.0% | PASS |
| file_reads | `oltp_distinct_range` | 3.25ms | 2.84ms | 0.9× | 1.2% | PASS |
| file_reads | `oltp_index_scan` | 7.67ms | 5.65ms | 0.7× | 0.8% | PASS |
| file_reads | `select_random_points` | 19.30ms | 14.74ms | 0.8× | 1.1% | PASS |
| file_reads | `select_random_ranges` | 9.31ms | 5.29ms | 0.6× | 0.4% | PASS |
| file_reads | `covering_index_scan` | 9.14ms | 7.58ms | 0.8× | 1.0% | PASS |
| file_reads | `groupby_scan` | 20.38ms | 20.05ms | 1.0× | 0.5% | PASS |
| file_reads | `index_join` | 10.33ms | 7.61ms | 0.7× | 0.9% | PASS |
| file_reads | `index_join_scan` | 3.27ms | 5.08ms | 1.6× | 2.0% | PASS |
| file_reads | `types_table_scan` | 710.99ms | 840.48ms | 1.2× | 2.2% | PASS |
| file_reads | `table_scan` | 757.22ms | 863.24ms | 1.1× | 1.3% | PASS |
| file_reads | `oltp_read_only` | 141.47ms | 94.75ms | 0.7× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 211.87ms | 270.57ms | 1.3× | 3.1% | PASS |
| file_writes | `oltp_insert` | 19.97ms | 42.22ms | 2.1× | 11.4% | PASS |
| file_writes | `oltp_update_index` | 154.10ms | 165.71ms | 1.1× | 3.9% | PASS |
| file_writes | `oltp_update_non_index` | 131.46ms | 103.29ms | 0.8× | 4.4% | PASS |
| file_writes | `oltp_delete_insert` | 162.09ms | 130.17ms | 0.8× | 4.1% | PASS |
| file_writes | `oltp_write_only` | 112.38ms | 87.14ms | 0.8× | 4.2% | PASS |
| file_writes | `types_delete_insert` | 128.94ms | 76.95ms | 0.6× | 11.3% | PASS |
| file_writes | `oltp_read_write` | 151.75ms | 131.19ms | 0.9× | 3.8% | PASS |
| ac_reads | `oltp_point_select` | 42.03ms | 38.22ms | 0.9× | 2.4% | PASS |
| ac_reads | `oltp_range_select` | 13.51ms | 11.81ms | 0.9× | 2.5% | PASS |
| ac_reads | `oltp_sum_range` | 13.81ms | 11.96ms | 0.9× | 2.5% | PASS |
| ac_reads | `oltp_order_range` | 2.73ms | 2.55ms | 0.9× | 2.2% | PASS |
| ac_reads | `oltp_distinct_range` | 3.16ms | 3.04ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_index_scan` | 4.64ms | 5.77ms | 1.2× | 1.5% | PASS |
| ac_reads | `select_random_points` | 18.32ms | 15.55ms | 0.8× | 4.1% | PASS |
| ac_reads | `select_random_ranges` | 6.52ms | 5.40ms | 0.8× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 6.56ms | 8.27ms | 1.3× | 1.6% | PASS |
| ac_reads | `groupby_scan` | 22.43ms | 21.93ms | 1.0× | 1.4% | PASS |
| ac_reads | `index_join` | 10.25ms | 8.42ms | 0.8× | 2.8% | PASS |
| ac_reads | `index_join_scan` | 3.03ms | 5.25ms | 1.7× | 1.9% | PASS |
| ac_reads | `types_table_scan` | 742.46ms | 871.97ms | 1.2× | 4.0% | PASS |
| ac_reads | `table_scan` | 876.28ms | 940.67ms | 1.1× | 2.8% | PASS |
| ac_reads | `oltp_read_only` | 118.04ms | 103.41ms | 0.9× | 2.5% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 19.58ms | 48.36ms | 2.5× | 3.4% | PASS |
| ac_writes | `oltp_insert_ac` | 21.23ms | 57.49ms | 2.7× | 5.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 21.34ms | 67.87ms | 3.2× | 4.5% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 18.49ms | 54.45ms | 2.9× | 3.9% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 20.76ms | 62.60ms | 3.0× | 3.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 20.37ms | 59.68ms | 2.9× | 2.9% | PASS |
| ac_writes | `types_delete_insert_ac` | 19.42ms | 54.18ms | 2.8× | 5.0% | PASS |
| ac_writes | `oltp_read_write_ac` | 24.25ms | 64.58ms | 2.7× | 4.1% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 37.52ms | 43.26ms | 1.2× | 4.1% | PASS |
| mem_reads | `oltp_range_select` | 17.06ms | 17.42ms | 1.0× | 2.6% | PASS |
| mem_reads | `oltp_sum_range` | 15.28ms | 16.85ms | 1.1× | 3.0% | PASS |
| mem_reads | `oltp_order_range` | 3.25ms | 3.68ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_distinct_range` | 4.38ms | 4.81ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 4.04ms | 7.12ms | 1.8× | 2.1% | PASS |
| mem_reads | `select_random_points` | 23.29ms | 25.07ms | 1.1× | 4.3% | PASS |
| mem_reads | `select_random_ranges` | 6.80ms | 7.34ms | 1.1× | 2.4% | PASS |
| mem_reads | `covering_index_scan` | 7.82ms | 11.71ms | 1.5× | 1.3% | PASS |
| mem_reads | `groupby_scan` | 34.49ms | 37.01ms | 1.1× | 1.0% | PASS |
| mem_reads | `index_join` | 10.66ms | 11.65ms | 1.1× | 2.9% | PASS |
| mem_reads | `index_join_scan` | 3.85ms | 7.02ms | 1.8× | 3.3% | PASS |
| mem_reads | `types_table_scan` | 1.09s | 1.40s | 1.3× | 1.7% | PASS |
| mem_reads | `table_scan` | 1.23s | 1.49s | 1.2× | 1.2% | PASS |
| mem_reads | `oltp_read_only` | 148.62ms | 160.84ms | 1.1× | 2.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 245.95ms | 377.68ms | 1.5× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 18.79ms | 37.70ms | 2.0× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 67.64ms | 141.87ms | 2.1× | 2.7% | PASS |
| mem_writes | `oltp_update_non_index` | 50.28ms | 85.66ms | 1.7× | 2.7% | PASS |
| mem_writes | `oltp_delete_insert` | 55.95ms | 106.04ms | 1.9× | 2.8% | PASS |
| mem_writes | `oltp_write_only` | 29.66ms | 56.26ms | 1.9× | 3.6% | PASS |
| mem_writes | `types_delete_insert` | 40.52ms | 66.26ms | 1.6× | 3.0% | PASS |
| mem_writes | `oltp_read_write` | 109.07ms | 148.95ms | 1.4× | 3.7% | PASS |
| file_reads | `oltp_point_select` | 103.65ms | 60.11ms | 0.6× | 1.7% | PASS |
| file_reads | `oltp_range_select` | 24.27ms | 19.11ms | 0.8× | 1.8% | PASS |
| file_reads | `oltp_sum_range` | 22.88ms | 18.76ms | 0.8× | 2.2% | PASS |
| file_reads | `oltp_order_range` | 4.25ms | 3.90ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_distinct_range` | 5.37ms | 5.07ms | 0.9× | 1.5% | PASS |
| file_reads | `oltp_index_scan` | 10.97ms | 9.00ms | 0.8× | 2.1% | PASS |
| file_reads | `select_random_points` | 30.84ms | 27.59ms | 0.9× | 2.8% | PASS |
| file_reads | `select_random_ranges` | 13.62ms | 9.23ms | 0.7× | 1.7% | PASS |
| file_reads | `covering_index_scan` | 14.88ms | 13.47ms | 0.9× | 1.3% | PASS |
| file_reads | `groupby_scan` | 35.55ms | 37.41ms | 1.1× | 0.8% | PASS |
| file_reads | `index_join` | 14.90ms | 12.93ms | 0.9× | 2.5% | PASS |
| file_reads | `index_join_scan` | 4.93ms | 7.49ms | 1.5× | 2.8% | PASS |
| file_reads | `types_table_scan` | 1.11s | 1.40s | 1.3× | 1.5% | PASS |
| file_reads | `table_scan` | 1.24s | 1.49s | 1.2× | 1.6% | PASS |
| file_reads | `oltp_read_only` | 253.96ms | 190.05ms | 0.7× | 1.1% | PASS |
| file_writes | `oltp_bulk_insert` | 268.71ms | 394.35ms | 1.5× | 1.1% | PASS |
| file_writes | `oltp_insert` | 26.80ms | 43.72ms | 1.6× | 1.8% | PASS |
| file_writes | `oltp_update_index` | 108.95ms | 173.66ms | 1.6× | 1.8% | PASS |
| file_writes | `oltp_update_non_index` | 89.79ms | 103.56ms | 1.2× | 5.3% | PASS |
| file_writes | `oltp_delete_insert` | 93.53ms | 126.66ms | 1.4× | 1.9% | PASS |
| file_writes | `oltp_write_only` | 61.94ms | 70.69ms | 1.1× | 2.2% | PASS |
| file_writes | `types_delete_insert` | 68.62ms | 75.52ms | 1.1× | 2.7% | PASS |
| file_writes | `oltp_read_write` | 135.16ms | 159.75ms | 1.2× | 2.1% | PASS |
| ac_reads | `oltp_point_select` | 59.23ms | 60.85ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_range_select` | 19.90ms | 19.18ms | 1.0× | 2.2% | PASS |
| ac_reads | `oltp_sum_range` | 17.95ms | 18.71ms | 1.0× | 2.7% | PASS |
| ac_reads | `oltp_order_range` | 3.86ms | 3.94ms | 1.0× | 2.2% | PASS |
| ac_reads | `oltp_distinct_range` | 4.92ms | 5.08ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_index_scan` | 6.65ms | 9.01ms | 1.4× | 1.4% | PASS |
| ac_reads | `select_random_points` | 26.54ms | 27.73ms | 1.0× | 2.5% | PASS |
| ac_reads | `select_random_ranges` | 9.37ms | 9.33ms | 1.0× | 2.0% | PASS |
| ac_reads | `covering_index_scan` | 10.49ms | 13.52ms | 1.3× | 2.1% | PASS |
| ac_reads | `groupby_scan` | 34.79ms | 37.32ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 12.59ms | 12.94ms | 1.0× | 3.9% | PASS |
| ac_reads | `index_join_scan` | 4.36ms | 7.42ms | 1.7× | 3.0% | PASS |
| ac_reads | `types_table_scan` | 1.09s | 1.38s | 1.3× | 2.3% | PASS |
| ac_reads | `table_scan` | 1.20s | 1.48s | 1.2× | 3.0% | PASS |
| ac_reads | `oltp_read_only` | 183.33ms | 188.29ms | 1.0× | 1.7% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 23.45ms | 65.55ms | 2.8× | 6.9% | PASS |
| ac_writes | `oltp_insert_ac` | 27.75ms | 86.09ms | 3.1× | 7.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.03ms | 94.91ms | 3.6× | 6.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.95ms | 74.38ms | 3.2× | 6.5% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.95ms | 86.12ms | 3.2× | 11.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.56ms | 84.40ms | 3.3× | 6.4% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.85ms | 74.43ms | 3.3× | 9.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.84ms | 90.93ms | 2.9× | 5.7% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 25.75ms | 30.57ms | 1.2× | 1.2% | PASS |
| mem_reads | `oltp_range_select` | 15.60ms | 17.05ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_sum_range` | 14.07ms | 16.40ms | 1.2× | 1.3% | PASS |
| mem_reads | `oltp_order_range` | 2.95ms | 3.18ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_distinct_range` | 3.83ms | 4.02ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 3.61ms | 4.73ms | 1.3× | 0.9% | PASS |
| mem_reads | `select_random_points` | 21.01ms | 24.46ms | 1.2× | 0.9% | PASS |
| mem_reads | `select_random_ranges` | 5.82ms | 6.79ms | 1.2× | 1.0% | PASS |
| mem_reads | `covering_index_scan` | 5.92ms | 7.71ms | 1.3× | 1.3% | PASS |
| mem_reads | `groupby_scan` | 30.39ms | 32.62ms | 1.1× | 1.0% | PASS |
| mem_reads | `index_join` | 6.18ms | 8.74ms | 1.4× | 1.9% | PASS |
| mem_reads | `index_join_scan` | 3.19ms | 5.29ms | 1.7× | 2.7% | PASS |
| mem_reads | `types_table_scan` | 854.76ms | 1.05s | 1.2× | 0.7% | PASS |
| mem_reads | `table_scan` | 981.15ms | 1.14s | 1.2× | 1.0% | PASS |
| mem_reads | `oltp_read_only` | 116.56ms | 132.35ms | 1.1× | 0.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 194.98ms | 268.65ms | 1.4× | 1.1% | PASS |
| mem_writes | `oltp_insert` | 15.12ms | 25.83ms | 1.7× | 0.6% | PASS |
| mem_writes | `oltp_update_index` | 53.27ms | 95.98ms | 1.8× | 1.2% | PASS |
| mem_writes | `oltp_update_non_index` | 41.69ms | 67.45ms | 1.6× | 1.6% | PASS |
| mem_writes | `oltp_delete_insert` | 40.56ms | 72.10ms | 1.8× | 1.5% | PASS |
| mem_writes | `oltp_write_only` | 22.31ms | 43.59ms | 2.0× | 1.5% | PASS |
| mem_writes | `types_delete_insert` | 25.95ms | 42.52ms | 1.6× | 1.1% | PASS |
| mem_writes | `oltp_read_write` | 76.50ms | 114.81ms | 1.5× | 0.9% | PASS |
| file_reads | `oltp_point_select` | 87.60ms | 47.44ms | 0.5× | 0.6% | PASS |
| file_reads | `oltp_range_select` | 22.62ms | 19.19ms | 0.8× | 0.9% | PASS |
| file_reads | `oltp_sum_range` | 20.77ms | 18.34ms | 0.9× | 0.9% | PASS |
| file_reads | `oltp_order_range` | 3.72ms | 3.45ms | 0.9× | 1.0% | PASS |
| file_reads | `oltp_distinct_range` | 4.61ms | 4.32ms | 0.9× | 0.9% | PASS |
| file_reads | `oltp_index_scan` | 10.11ms | 6.95ms | 0.7× | 0.9% | PASS |
| file_reads | `select_random_points` | 28.70ms | 27.06ms | 0.9× | 1.4% | PASS |
| file_reads | `select_random_ranges` | 12.27ms | 8.77ms | 0.7× | 0.7% | PASS |
| file_reads | `covering_index_scan` | 12.52ms | 10.01ms | 0.8× | 0.9% | PASS |
| file_reads | `groupby_scan` | 31.11ms | 33.04ms | 1.1× | 0.7% | PASS |
| file_reads | `index_join` | 9.80ms | 10.33ms | 1.1× | 1.1% | PASS |
| file_reads | `index_join_scan` | 4.04ms | 5.66ms | 1.4× | 2.5% | PASS |
| file_reads | `types_table_scan` | 858.19ms | 1.05s | 1.2× | 0.5% | PASS |
| file_reads | `table_scan` | 1.01s | 1.14s | 1.1× | 2.4% | PASS |
| file_reads | `oltp_read_only` | 205.86ms | 156.81ms | 0.8× | 0.4% | PASS |
| file_writes | `oltp_bulk_insert` | 246.61ms | 312.18ms | 1.3× | 2.9% | PASS |
| file_writes | `oltp_insert` | 28.43ms | 36.05ms | 1.3× | 22.5% | PASS |
| file_writes | `oltp_update_index` | 155.75ms | 154.41ms | 1.0× | 6.3% | PASS |
| file_writes | `oltp_update_non_index` | 134.46ms | 115.05ms | 0.9× | 13.8% | PASS |
| file_writes | `oltp_delete_insert` | 139.71ms | 128.60ms | 0.9× | 16.2% | PASS |
| file_writes | `oltp_write_only` | 93.03ms | 83.24ms | 0.9× | 10.1% | PASS |
| file_writes | `types_delete_insert` | 80.14ms | 71.12ms | 0.9× | 8.1% | PASS |
| file_writes | `oltp_read_write` | 141.13ms | 157.91ms | 1.1× | 6.6% | PASS |
| ac_reads | `oltp_point_select` | 46.62ms | 47.24ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_range_select` | 18.58ms | 19.18ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_sum_range` | 16.74ms | 18.33ms | 1.1× | 1.0% | PASS |
| ac_reads | `oltp_order_range` | 3.34ms | 3.45ms | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_distinct_range` | 4.22ms | 4.32ms | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_index_scan` | 6.11ms | 6.95ms | 1.1× | 0.9% | PASS |
| ac_reads | `select_random_points` | 24.02ms | 26.73ms | 1.1× | 1.0% | PASS |
| ac_reads | `select_random_ranges` | 8.24ms | 8.78ms | 1.1× | 0.8% | PASS |
| ac_reads | `covering_index_scan` | 8.51ms | 10.01ms | 1.2× | 1.0% | PASS |
| ac_reads | `groupby_scan` | 30.52ms | 33.02ms | 1.1× | 0.7% | PASS |
| ac_reads | `index_join` | 7.79ms | 10.32ms | 1.3× | 0.9% | PASS |
| ac_reads | `index_join_scan` | 3.62ms | 5.62ms | 1.6× | 2.0% | PASS |
| ac_reads | `types_table_scan` | 860.44ms | 1.06s | 1.2× | 0.6% | PASS |
| ac_reads | `table_scan` | 984.70ms | 1.14s | 1.2× | 1.0% | PASS |
| ac_reads | `oltp_read_only` | 147.50ms | 156.62ms | 1.1× | 0.7% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.14ms | 58.55ms | 2.8× | 32.0% | PASS |
| ac_writes | `oltp_insert_ac` | 23.80ms | 71.35ms | 3.0× | 25.1% | PASS |
| ac_writes | `oltp_update_index_ac` | 25.08ms | 77.06ms | 3.1× | 20.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 19.41ms | 59.98ms | 3.1× | 14.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 23.02ms | 67.98ms | 3.0× | 16.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 22.88ms | 67.64ms | 3.0× | 9.5% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.75ms | 66.93ms | 2.9× | 26.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 29.33ms | 87.20ms | 3.0× | 28.4% | PASS |

</details>

</details>

## Version-control latency

Wall time: 7m 35s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 28.66ms | 38.00ms | 75.4% | 0.8% | PASS |
| `status_dirty_many_tables` | 31.68ms | 42.00ms | 75.4% | 0.6% | PASS |
| `diff_regular_working_one_table` | 24.76ms | 33.00ms | 75.0% | 0.8% | PASS |
| `diff_regular_working_many_tables` | 35.27ms | 50.00ms | 70.5% | 1.0% | PASS |
| `diff_stat_working_many_tables` | 35.71ms | 48.00ms | 74.4% | 1.1% | PASS |
| `diff_schema_working_many_tables` | 36.50ms | 48.00ms | 76.0% | 1.0% | PASS |
| `branch_list_many_branches` | 18.93ms | 25.00ms | 75.7% | 1.6% | PASS |
| `branch_create_delete` | 20.49ms | 27.00ms | 75.9% | 2.7% | PASS |
| `at_literal_deep_history` | 21.11ms | 28.00ms | 75.4% | 1.9% | PASS |
| `diff_literal_deep_history` | 19.81ms | 28.00ms | 70.7% | 0.9% | PASS |
| `history_literal_deep_history` | 20.79ms | 30.00ms | 69.3% | 0.6% | PASS |
| `checkout_branch_clean` | 27.09ms | 43.00ms | 63.0% | 0.8% | PASS |
| `merge_data_no_conflicts` | 22.93ms | 33.00ms | 69.5% | 4.9% | PASS |
| `merge_data_secondary_index` | 773.39ms | 944.00ms | 81.9% | 2.1% | PASS |
| `merge_schema_no_conflicts` | 17.50ms | 24.00ms | 72.9% | 1.5% | PASS |
| `merge_data_conflicts` | 24.22ms | 33.00ms | 73.4% | 0.4% | PASS |
| `merge_data_conflicts_with_resolve` | 25.02ms | 34.00ms | 73.6% | 0.4% | PASS |

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

candidate: b422859.

Fixture file bytes: base 107,968,301; delta 115,879,032.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 426.755 | 412.967–538.990 | 32.276 | 234.33 | 107968301 |
| push_initial | candidate | 893.506 | 801.346–992.231 | 398.013 | 111.92 | 107956807 |
| clone_initial | candidate | 1128.015 | 1040.757–1275.127 | 523.207 | 88.65 | 108475105 |
| push_noop | candidate | 0.512 | 0.482–0.560 | 0.270 | — | 0 |
| fetch_noop | candidate | 0.166 | 0.163–0.188 | 0.168 | — | 0 |
| push_incremental | candidate | 198.293 | 193.775–266.070 | 164.964 | 5.04 | 7909775 |
| fetch_incremental | candidate | 101.091 | 96.402–174.011 | 70.575 | 9.89 | 7910084 |
| pull_fast_forward | candidate | 90.263 | 83.093–121.937 | 60.392 | 11.08 | 7910844 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 90.57 | 175.487 | 108.763 | 18329 | 2066 |
| clone_initial | candidate | 90.95 | 295.177 | 109.784 | 47265 | 2337 |
| push_noop | candidate | 42.50 | 0.073 | 0.000 | 26 | 2 |
| fetch_noop | candidate | 42.50 | 0.135 | 0.000 | 40 | 0 |
| push_incremental | candidate | 42.50 | 96.868 | 7.910 | 22411 | 125 |
| fetch_incremental | candidate | 42.50 | 34.472 | 7.910 | 10808 | 124 |
| pull_fast_forward | candidate | 42.50 | 26.497 | 7.911 | 7031 | 126 |

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

candidate: b422859.

Fixture file bytes: base 1,087,718,066; delta 1,103,525,194.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 5651.648 | 5643.945–5688.625 | 309.860 | 176.94 | 1087718066 |
| push_initial | candidate | 7941.741 | 7489.141–7947.073 | 4162.071 | 125.92 | 1079566310 |
| clone_initial | candidate | 10030.054 | 9394.962–10546.502 | 5685.705 | 99.70 | 1102407396 |
| push_noop | candidate | 0.629 | 0.585–0.709 | 0.245 | — | 0 |
| fetch_noop | candidate | 0.139 | 0.137–0.142 | 0.140 | — | 0 |
| push_incremental | candidate | 921.518 | 919.579–940.711 | 870.463 | 1.09 | 15806003 |
| fetch_incremental | candidate | 263.687 | 253.052–270.183 | 192.351 | 3.79 | 15806513 |
| pull_fast_forward | candidate | 281.983 | 267.946–288.917 | 209.126 | 3.55 | 15807273 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 134.84 | 2092.407 | 1087.622 | 263628 | 20541 |
| clone_initial | candidate | 124.16 | 3516.857 | 1132.819 | 612094 | 32243 |
| push_noop | candidate | 45.25 | 0.078 | 0.000 | 27 | 2 |
| fetch_noop | candidate | 45.25 | 0.137 | 0.000 | 40 | 0 |
| push_incremental | candidate | 48.39 | 616.902 | 15.928 | 149840 | 313 |
| fetch_incremental | candidate | 50.52 | 61.828 | 15.928 | 13796 | 313 |
| pull_fast_forward | candidate | 50.50 | 61.761 | 15.929 | 13792 | 315 |

</details>


</details>

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh`, `test/vc_perf_ceiling.sh`, and `test/remote_perf.py`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
