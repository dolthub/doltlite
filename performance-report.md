# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-11-07 11:29 UTC
>
> Commit: [`2d20c96872b59508ddf37caa5b1394cb94977607`](https://github.com/dolthub/doltlite/commit/2d20c96872b59508ddf37caa5b1394cb94977607)
>
> Runner: ubuntu24 20261101.360.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/42360122493)

The SQL workloads compare optimized DoltLite against stock SQLite on the same GitHub-hosted runner, alternating execution order on each repetition. Version-control sections measure DoltLite latency. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.39s | 11.28s | 1.2× | 1.5% | **PASS** |
| Writes | 1.99s | 3.38s | 1.7× | 1.2% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.38s | 11.59s | 1.1× | 1.3% | **PASS** |
| Writes | 3.30s | 4.07s | 1.2× | 2.2% | **PASS** |
| Autocommit writes | 789.40ms | 2.44s | 3.1× | 6.4% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.34s | 2.88s | 1.2× | 0.9% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.66s | 3.21s | 1.2× | 1.6% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 1.63s | 1.91s | 1.2× | 2.0% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.75s | 3.28s | 1.2× | 1.5% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 426.22ms | 780.93ms | 1.8× | 0.8% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 610.47ms | 1.03s | 1.7× | 1.4% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 344.42ms | 559.11ms | 1.6× | 1.6% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 606.62ms | 1.02s | 1.7× | 1.6% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.55s | 2.93s | 1.1× | 1.1% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.93s | 3.29s | 1.1× | 1.5% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 1.92s | 2.04s | 1.1× | 1.5% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.97s | 3.33s | 1.1× | 1.4% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 578.60ms | 844.32ms | 1.5× | 1.3% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 954.72ms | 1.14s | 1.2× | 3.2% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 978.92ms | 992.00ms | 1.0× | 4.1% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 790.51ms | 1.09s | 1.4× | 2.0% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.45s | 2.94s | 1.2× | 1.3% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.73s | 3.26s | 1.2× | 1.5% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 1.72s | 1.98s | 1.2× | 2.2% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.83s | 3.34s | 1.2× | 1.3% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 184.53ms | 617.01ms | 3.3× | 6.6% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 208.26ms | 664.42ms | 3.2× | 7.8% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 196.69ms | 498.44ms | 2.5× | 4.8% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 199.91ms | 659.48ms | 3.3× | 6.1% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 22.34ms | 32.88ms | 1.5× | 1.0% | PASS |
| mem_reads | `oltp_range_select` | 9.04ms | 12.75ms | 1.4× | 1.4% | PASS |
| mem_reads | `oltp_sum_range` | 8.47ms | 12.28ms | 1.4× | 0.9% | PASS |
| mem_reads | `oltp_order_range` | 2.40ms | 2.98ms | 1.2× | 0.9% | PASS |
| mem_reads | `oltp_distinct_range` | 3.46ms | 4.14ms | 1.2× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 3.58ms | 5.59ms | 1.6× | 1.0% | PASS |
| mem_reads | `select_random_points` | 11.27ms | 11.73ms | 1.0× | 1.0% | PASS |
| mem_reads | `select_random_ranges` | 4.15ms | 5.52ms | 1.3× | 1.1% | PASS |
| mem_reads | `covering_index_scan` | 7.67ms | 11.00ms | 1.4× | 1.1% | PASS |
| mem_reads | `groupby_scan` | 29.27ms | 32.89ms | 1.1× | 0.5% | PASS |
| mem_reads | `index_join` | 5.53ms | 8.62ms | 1.6× | 0.8% | PASS |
| mem_reads | `index_join_scan` | 2.80ms | 5.71ms | 2.0× | 0.9% | PASS |
| mem_reads | `types_table_scan` | 1.01s | 1.25s | 1.2× | 0.5% | PASS |
| mem_reads | `table_scan` | 1.12s | 1.35s | 1.2× | 0.5% | PASS |
| mem_reads | `oltp_read_only` | 99.25ms | 128.00ms | 1.3× | 0.7% | PASS |
| mem_writes | `oltp_bulk_insert` | 182.20ms | 319.22ms | 1.8× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 15.17ms | 27.07ms | 1.8× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 47.24ms | 95.31ms | 2.0× | 0.8% | PASS |
| mem_writes | `oltp_update_non_index` | 32.75ms | 65.03ms | 2.0× | 0.8% | PASS |
| mem_writes | `oltp_delete_insert` | 42.35ms | 76.09ms | 1.8× | 0.6% | PASS |
| mem_writes | `oltp_write_only` | 20.44ms | 46.19ms | 2.3× | 0.7% | PASS |
| mem_writes | `types_delete_insert` | 23.45ms | 39.05ms | 1.7× | 0.7% | PASS |
| mem_writes | `oltp_read_write` | 62.63ms | 112.97ms | 1.8× | 0.5% | PASS |
| file_reads | `oltp_point_select` | 86.71ms | 50.13ms | 0.6× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 15.74ms | 14.62ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_sum_range` | 15.28ms | 14.35ms | 0.9× | 1.2% | PASS |
| file_reads | `oltp_order_range` | 3.16ms | 3.28ms | 1.0× | 1.2% | PASS |
| file_reads | `oltp_distinct_range` | 4.29ms | 4.43ms | 1.0× | 1.1% | PASS |
| file_reads | `oltp_index_scan` | 10.23ms | 7.72ms | 0.8× | 1.3% | PASS |
| file_reads | `select_random_points` | 18.61ms | 13.81ms | 0.7× | 0.8% | PASS |
| file_reads | `select_random_ranges` | 10.86ms | 7.48ms | 0.7× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 14.46ms | 13.01ms | 0.9× | 1.2% | PASS |
| file_reads | `groupby_scan` | 30.17ms | 33.26ms | 1.1× | 0.8% | PASS |
| file_reads | `index_join` | 9.35ms | 10.17ms | 1.1× | 0.8% | PASS |
| file_reads | `index_join_scan` | 3.92ms | 6.06ms | 1.5× | 1.2% | PASS |
| file_reads | `types_table_scan` | 1.01s | 1.24s | 1.2× | 0.5% | PASS |
| file_reads | `table_scan` | 1.13s | 1.35s | 1.2× | 0.6% | PASS |
| file_reads | `oltp_read_only` | 193.42ms | 154.33ms | 0.8× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 196.94ms | 330.57ms | 1.7× | 1.1% | PASS |
| file_writes | `oltp_insert` | 21.69ms | 30.60ms | 1.4× | 1.3% | PASS |
| file_writes | `oltp_update_index` | 73.17ms | 105.33ms | 1.4× | 1.4% | PASS |
| file_writes | `oltp_update_non_index` | 54.98ms | 72.17ms | 1.3× | 1.3% | PASS |
| file_writes | `oltp_delete_insert` | 64.67ms | 87.96ms | 1.4× | 0.8% | PASS |
| file_writes | `oltp_write_only` | 42.03ms | 52.66ms | 1.3× | 1.3% | PASS |
| file_writes | `types_delete_insert` | 38.32ms | 45.58ms | 1.2× | 1.2% | PASS |
| file_writes | `oltp_read_write` | 86.80ms | 119.44ms | 1.4× | 0.8% | PASS |
| ac_reads | `oltp_point_select` | 44.23ms | 50.38ms | 1.1× | 0.9% | PASS |
| ac_reads | `oltp_range_select` | 11.80ms | 14.69ms | 1.2× | 1.5% | PASS |
| ac_reads | `oltp_sum_range` | 11.20ms | 14.31ms | 1.3× | 1.3% | PASS |
| ac_reads | `oltp_order_range` | 2.78ms | 3.29ms | 1.2× | 1.5% | PASS |
| ac_reads | `oltp_distinct_range` | 3.87ms | 4.43ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_index_scan` | 6.00ms | 7.78ms | 1.3× | 1.0% | PASS |
| ac_reads | `select_random_points` | 14.42ms | 13.84ms | 1.0× | 1.0% | PASS |
| ac_reads | `select_random_ranges` | 6.64ms | 7.49ms | 1.1× | 1.4% | PASS |
| ac_reads | `covering_index_scan` | 10.02ms | 13.02ms | 1.3× | 0.9% | PASS |
| ac_reads | `groupby_scan` | 29.80ms | 33.29ms | 1.1× | 0.5% | PASS |
| ac_reads | `index_join` | 7.13ms | 10.31ms | 1.4× | 1.3% | PASS |
| ac_reads | `index_join_scan` | 3.58ms | 6.19ms | 1.7× | 2.2% | PASS |
| ac_reads | `types_table_scan` | 1.02s | 1.25s | 1.2× | 0.8% | PASS |
| ac_reads | `table_scan` | 1.15s | 1.36s | 1.2× | 0.9% | PASS |
| ac_reads | `oltp_read_only` | 136.01ms | 156.15ms | 1.1× | 1.4% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.07ms | 63.06ms | 3.0× | 6.8% | PASS |
| ac_writes | `oltp_insert_ac` | 23.24ms | 76.35ms | 3.3× | 7.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 24.51ms | 88.56ms | 3.6× | 4.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 20.65ms | 71.04ms | 3.4× | 6.4% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 23.10ms | 80.70ms | 3.5× | 6.2% | PASS |
| ac_writes | `oltp_write_only_ac` | 23.55ms | 79.83ms | 3.4× | 6.7% | PASS |
| ac_writes | `types_delete_insert_ac` | 20.82ms | 71.12ms | 3.4× | 6.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 27.59ms | 86.34ms | 3.1× | 9.4% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 37.18ms | 43.54ms | 1.2× | 1.6% | PASS |
| mem_reads | `oltp_range_select` | 17.60ms | 17.04ms | 1.0× | 1.9% | PASS |
| mem_reads | `oltp_sum_range` | 15.73ms | 16.82ms | 1.1× | 2.1% | PASS |
| mem_reads | `oltp_order_range` | 3.34ms | 3.62ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_distinct_range` | 4.47ms | 4.78ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 4.05ms | 7.23ms | 1.8× | 1.8% | PASS |
| mem_reads | `select_random_points` | 23.16ms | 23.64ms | 1.0× | 2.6% | PASS |
| mem_reads | `select_random_ranges` | 6.82ms | 7.41ms | 1.1× | 1.7% | PASS |
| mem_reads | `covering_index_scan` | 7.76ms | 11.69ms | 1.5× | 1.2% | PASS |
| mem_reads | `groupby_scan` | 35.06ms | 35.82ms | 1.0× | 0.8% | PASS |
| mem_reads | `index_join` | 10.68ms | 11.19ms | 1.0× | 1.9% | PASS |
| mem_reads | `index_join_scan` | 3.83ms | 6.81ms | 1.8× | 2.7% | PASS |
| mem_reads | `types_table_scan` | 1.09s | 1.38s | 1.3× | 1.2% | PASS |
| mem_reads | `table_scan` | 1.26s | 1.48s | 1.2× | 1.1% | PASS |
| mem_reads | `oltp_read_only` | 143.63ms | 156.91ms | 1.1× | 1.5% | PASS |
| mem_writes | `oltp_bulk_insert` | 245.10ms | 379.40ms | 1.5× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 18.14ms | 37.73ms | 2.1× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 67.60ms | 146.32ms | 2.2× | 1.2% | PASS |
| mem_writes | `oltp_update_non_index` | 50.51ms | 86.63ms | 1.7× | 1.5% | PASS |
| mem_writes | `oltp_delete_insert` | 55.73ms | 106.83ms | 1.9× | 1.2% | PASS |
| mem_writes | `oltp_write_only` | 29.17ms | 56.13ms | 1.9× | 1.7% | PASS |
| mem_writes | `types_delete_insert` | 40.62ms | 67.28ms | 1.7× | 1.6% | PASS |
| mem_writes | `oltp_read_write` | 103.58ms | 147.56ms | 1.4× | 2.2% | PASS |
| file_reads | `oltp_point_select` | 103.98ms | 61.07ms | 0.6× | 1.3% | PASS |
| file_reads | `oltp_range_select` | 24.84ms | 18.73ms | 0.8× | 1.9% | PASS |
| file_reads | `oltp_sum_range` | 23.10ms | 18.61ms | 0.8× | 1.9% | PASS |
| file_reads | `oltp_order_range` | 4.21ms | 3.86ms | 0.9× | 2.2% | PASS |
| file_reads | `oltp_distinct_range` | 5.39ms | 5.03ms | 0.9× | 1.5% | PASS |
| file_reads | `oltp_index_scan` | 10.86ms | 9.06ms | 0.8× | 1.4% | PASS |
| file_reads | `select_random_points` | 31.41ms | 26.46ms | 0.8× | 2.0% | PASS |
| file_reads | `select_random_ranges` | 13.96ms | 9.39ms | 0.7× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 14.88ms | 13.64ms | 0.9× | 1.4% | PASS |
| file_reads | `groupby_scan` | 35.99ms | 36.12ms | 1.0× | 1.0% | PASS |
| file_reads | `index_join` | 14.91ms | 12.53ms | 0.8× | 1.8% | PASS |
| file_reads | `index_join_scan` | 4.87ms | 7.26ms | 1.5× | 2.7% | PASS |
| file_reads | `types_table_scan` | 1.12s | 1.39s | 1.2× | 2.4% | PASS |
| file_reads | `table_scan` | 1.28s | 1.49s | 1.2× | 1.3% | PASS |
| file_reads | `oltp_read_only` | 246.25ms | 185.79ms | 0.8× | 1.5% | PASS |
| file_writes | `oltp_bulk_insert` | 270.61ms | 394.63ms | 1.5× | 0.8% | PASS |
| file_writes | `oltp_insert` | 26.37ms | 43.48ms | 1.6× | 2.3% | PASS |
| file_writes | `oltp_update_index` | 135.56ms | 166.92ms | 1.2× | 4.1% | PASS |
| file_writes | `oltp_update_non_index` | 104.36ms | 101.60ms | 1.0× | 16.1% | PASS |
| file_writes | `oltp_delete_insert` | 97.33ms | 123.69ms | 1.3× | 1.4% | PASS |
| file_writes | `oltp_write_only` | 89.42ms | 68.78ms | 0.8× | 7.1% | PASS |
| file_writes | `types_delete_insert` | 74.10ms | 77.15ms | 1.0× | 2.1% | PASS |
| file_writes | `oltp_read_write` | 156.97ms | 160.91ms | 1.0× | 10.4% | PASS |
| ac_reads | `oltp_point_select` | 59.06ms | 60.70ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_range_select` | 20.32ms | 18.71ms | 0.9× | 1.7% | PASS |
| ac_reads | `oltp_sum_range` | 18.35ms | 18.57ms | 1.0× | 2.2% | PASS |
| ac_reads | `oltp_order_range` | 3.76ms | 3.86ms | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_distinct_range` | 4.86ms | 5.01ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_index_scan` | 6.63ms | 9.06ms | 1.4× | 1.4% | PASS |
| ac_reads | `select_random_points` | 26.44ms | 26.45ms | 1.0× | 2.9% | PASS |
| ac_reads | `select_random_ranges` | 9.38ms | 9.33ms | 1.0× | 1.4% | PASS |
| ac_reads | `covering_index_scan` | 10.55ms | 13.65ms | 1.3× | 1.0% | PASS |
| ac_reads | `groupby_scan` | 35.39ms | 36.13ms | 1.0× | 0.8% | PASS |
| ac_reads | `index_join` | 12.69ms | 12.49ms | 1.0× | 1.5% | PASS |
| ac_reads | `index_join_scan` | 4.33ms | 7.28ms | 1.7× | 1.6% | PASS |
| ac_reads | `types_table_scan` | 1.08s | 1.38s | 1.3× | 1.3% | PASS |
| ac_reads | `table_scan` | 1.26s | 1.48s | 1.2× | 1.4% | PASS |
| ac_reads | `oltp_read_only` | 178.37ms | 183.97ms | 1.0× | 1.6% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 23.92ms | 66.10ms | 2.8× | 8.3% | PASS |
| ac_writes | `oltp_insert_ac` | 26.53ms | 82.44ms | 3.1× | 8.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.41ms | 97.88ms | 3.6× | 7.5% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.35ms | 74.53ms | 3.3× | 7.2% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.47ms | 88.98ms | 3.4× | 6.9% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.53ms | 86.38ms | 3.4× | 6.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 23.81ms | 77.74ms | 3.3× | 8.1% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.26ms | 90.36ms | 2.8× | 8.2% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 25.52ms | 25.89ms | 1.0× | 3.1% | PASS |
| mem_reads | `oltp_range_select` | 11.03ms | 10.72ms | 1.0× | 1.6% | PASS |
| mem_reads | `oltp_sum_range` | 10.46ms | 10.46ms | 1.0× | 2.8% | PASS |
| mem_reads | `oltp_order_range` | 2.18ms | 2.27ms | 1.0× | 2.1% | PASS |
| mem_reads | `oltp_distinct_range` | 2.71ms | 2.78ms | 1.0× | 2.0% | PASS |
| mem_reads | `oltp_index_scan` | 2.67ms | 4.75ms | 1.8× | 1.6% | PASS |
| mem_reads | `select_random_points` | 16.85ms | 15.86ms | 0.9× | 2.8% | PASS |
| mem_reads | `select_random_ranges` | 4.71ms | 4.43ms | 0.9× | 1.8% | PASS |
| mem_reads | `covering_index_scan` | 4.17ms | 7.12ms | 1.7× | 2.0% | PASS |
| mem_reads | `groupby_scan` | 21.32ms | 21.47ms | 1.0× | 1.5% | PASS |
| mem_reads | `index_join` | 8.45ms | 8.38ms | 1.0× | 3.6% | PASS |
| mem_reads | `index_join_scan` | 2.86ms | 5.81ms | 2.0× | 2.1% | PASS |
| mem_reads | `types_table_scan` | 681.59ms | 816.69ms | 1.2× | 1.4% | PASS |
| mem_reads | `table_scan` | 769.79ms | 894.41ms | 1.2× | 2.5% | PASS |
| mem_reads | `oltp_read_only` | 70.39ms | 75.53ms | 1.1× | 1.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 137.94ms | 204.86ms | 1.5× | 1.1% | PASS |
| mem_writes | `oltp_insert` | 10.56ms | 21.37ms | 2.0× | 1.2% | PASS |
| mem_writes | `oltp_update_index` | 38.92ms | 79.23ms | 2.0× | 1.7% | PASS |
| mem_writes | `oltp_update_non_index` | 30.64ms | 48.83ms | 1.6× | 1.8% | PASS |
| mem_writes | `oltp_delete_insert` | 33.43ms | 62.81ms | 1.9× | 1.2% | PASS |
| mem_writes | `oltp_write_only` | 17.25ms | 32.72ms | 1.9× | 1.9% | PASS |
| mem_writes | `types_delete_insert` | 24.25ms | 35.80ms | 1.5× | 2.7% | PASS |
| mem_writes | `oltp_read_write` | 51.43ms | 73.47ms | 1.4× | 1.4% | PASS |
| file_reads | `oltp_point_select` | 70.00ms | 36.11ms | 0.5× | 1.5% | PASS |
| file_reads | `oltp_range_select` | 15.58ms | 11.50ms | 0.7× | 2.0% | PASS |
| file_reads | `oltp_sum_range` | 14.63ms | 10.63ms | 0.7× | 2.7% | PASS |
| file_reads | `oltp_order_range` | 2.79ms | 2.47ms | 0.9× | 1.1% | PASS |
| file_reads | `oltp_distinct_range` | 3.31ms | 2.98ms | 0.9× | 0.7% | PASS |
| file_reads | `oltp_index_scan` | 8.26ms | 6.03ms | 0.7× | 1.2% | PASS |
| file_reads | `select_random_points` | 21.03ms | 15.71ms | 0.7× | 2.7% | PASS |
| file_reads | `select_random_ranges` | 10.03ms | 5.80ms | 0.6× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 10.05ms | 8.29ms | 0.8× | 1.1% | PASS |
| file_reads | `groupby_scan` | 21.80ms | 21.62ms | 1.0× | 1.1% | PASS |
| file_reads | `index_join` | 11.59ms | 8.41ms | 0.7× | 4.1% | PASS |
| file_reads | `index_join_scan` | 3.32ms | 5.08ms | 1.5× | 1.8% | PASS |
| file_reads | `types_table_scan` | 693.19ms | 833.97ms | 1.2× | 1.2% | PASS |
| file_reads | `table_scan` | 869.87ms | 963.80ms | 1.1× | 2.5% | PASS |
| file_reads | `oltp_read_only` | 167.83ms | 109.05ms | 0.6× | 2.1% | PASS |
| file_writes | `oltp_bulk_insert` | 205.65ms | 269.92ms | 1.3× | 3.0% | PASS |
| file_writes | `oltp_insert` | 19.16ms | 44.45ms | 2.3× (paired 2.2×) | 10.4% | PASS |
| file_writes | `oltp_update_index` | 141.09ms | 166.17ms | 1.2× | 5.6% | PASS |
| file_writes | `oltp_update_non_index` | 120.60ms | 107.30ms | 0.9× | 3.8% | PASS |
| file_writes | `oltp_delete_insert` | 145.54ms | 122.15ms | 0.8× | 4.3% | PASS |
| file_writes | `oltp_write_only` | 100.82ms | 79.34ms | 0.8× | 3.8% | PASS |
| file_writes | `types_delete_insert` | 104.62ms | 73.78ms | 0.7× | 7.0% | PASS |
| file_writes | `oltp_read_write` | 141.44ms | 128.89ms | 0.9× | 3.6% | PASS |
| ac_reads | `oltp_point_select` | 40.57ms | 37.51ms | 0.9× | 1.7% | PASS |
| ac_reads | `oltp_range_select` | 12.57ms | 11.32ms | 0.9× | 2.2% | PASS |
| ac_reads | `oltp_sum_range` | 11.73ms | 10.87ms | 0.9× | 3.3% | PASS |
| ac_reads | `oltp_order_range` | 2.43ms | 2.39ms | 1.0× | 3.2% | PASS |
| ac_reads | `oltp_distinct_range` | 2.95ms | 2.89ms | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_index_scan` | 4.81ms | 5.85ms | 1.2× | 2.0% | PASS |
| ac_reads | `select_random_points` | 16.59ms | 15.15ms | 0.9× | 4.9% | PASS |
| ac_reads | `select_random_ranges` | 6.11ms | 5.40ms | 0.9× | 2.1% | PASS |
| ac_reads | `covering_index_scan` | 6.29ms | 7.88ms | 1.3× | 2.0% | PASS |
| ac_reads | `groupby_scan` | 20.87ms | 20.63ms | 1.0× | 2.0% | PASS |
| ac_reads | `index_join` | 9.18ms | 7.93ms | 0.9× | 2.4% | PASS |
| ac_reads | `index_join_scan` | 3.05ms | 4.97ms | 1.6× | 2.4% | PASS |
| ac_reads | `types_table_scan` | 680.68ms | 830.44ms | 1.2× | 2.3% | PASS |
| ac_reads | `table_scan` | 786.43ms | 909.34ms | 1.2× | 2.6% | PASS |
| ac_reads | `oltp_read_only` | 112.56ms | 103.67ms | 0.9× | 2.1% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 23.49ms | 51.36ms | 2.2× | 4.5% | PASS |
| ac_writes | `oltp_insert_ac` | 26.62ms | 62.81ms | 2.4× | 4.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.39ms | 69.39ms | 2.6× | 4.3% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 20.51ms | 58.69ms | 2.9× | 5.2% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.82ms | 65.41ms | 2.5× | 5.8% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.00ms | 64.92ms | 2.6× | 4.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 20.81ms | 58.52ms | 2.8× | 4.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 28.05ms | 67.36ms | 2.4× | 7.2% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 34.47ms | 44.42ms | 1.3× | 1.9% | PASS |
| mem_reads | `oltp_range_select` | 20.84ms | 23.44ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_sum_range` | 18.53ms | 22.60ms | 1.2× | 1.3% | PASS |
| mem_reads | `oltp_order_range` | 3.61ms | 4.22ms | 1.2× | 1.3% | PASS |
| mem_reads | `oltp_distinct_range` | 4.87ms | 5.38ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 4.64ms | 6.84ms | 1.5× | 2.4% | PASS |
| mem_reads | `select_random_points` | 29.43ms | 34.63ms | 1.2× | 1.6% | PASS |
| mem_reads | `select_random_ranges` | 7.94ms | 9.66ms | 1.2× | 1.5% | PASS |
| mem_reads | `covering_index_scan` | 7.80ms | 11.24ms | 1.4× | 1.7% | PASS |
| mem_reads | `groupby_scan` | 36.89ms | 40.36ms | 1.1× | 0.8% | PASS |
| mem_reads | `index_join` | 7.98ms | 12.12ms | 1.5× | 1.5% | PASS |
| mem_reads | `index_join_scan` | 4.12ms | 6.81ms | 1.7× | 3.6% | PASS |
| mem_reads | `types_table_scan` | 1.09s | 1.39s | 1.3× | 0.9% | PASS |
| mem_reads | `table_scan` | 1.31s | 1.48s | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_read_only` | 161.47ms | 187.88ms | 1.2× | 1.5% | PASS |
| mem_writes | `oltp_bulk_insert` | 244.57ms | 366.27ms | 1.5× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 19.05ms | 35.35ms | 1.9× | 1.5% | PASS |
| mem_writes | `oltp_update_index` | 68.57ms | 133.14ms | 1.9× | 1.8% | PASS |
| mem_writes | `oltp_update_non_index` | 53.29ms | 94.19ms | 1.8× | 1.7% | PASS |
| mem_writes | `oltp_delete_insert` | 51.08ms | 99.31ms | 1.9× | 1.6% | PASS |
| mem_writes | `oltp_write_only` | 28.06ms | 59.35ms | 2.1× | 1.8% | PASS |
| mem_writes | `types_delete_insert` | 33.86ms | 62.07ms | 1.8× | 1.0% | PASS |
| mem_writes | `oltp_read_write` | 108.14ms | 166.51ms | 1.5× | 1.7% | PASS |
| file_reads | `oltp_point_select` | 100.21ms | 61.93ms | 0.6× | 1.4% | PASS |
| file_reads | `oltp_range_select` | 27.68ms | 25.92ms | 0.9× | 1.9% | PASS |
| file_reads | `oltp_sum_range` | 25.79ms | 24.91ms | 1.0× | 1.1% | PASS |
| file_reads | `oltp_order_range` | 4.49ms | 4.56ms | 1.0× | 1.6% | PASS |
| file_reads | `oltp_distinct_range` | 5.79ms | 5.70ms | 1.0× | 1.4% | PASS |
| file_reads | `oltp_index_scan` | 11.65ms | 8.99ms | 0.8× | 1.7% | PASS |
| file_reads | `select_random_points` | 38.32ms | 38.32ms | 1.0× | 1.8% | PASS |
| file_reads | `select_random_ranges` | 14.96ms | 11.86ms | 0.8× | 1.4% | PASS |
| file_reads | `covering_index_scan` | 15.15ms | 13.59ms | 0.9× | 2.5% | PASS |
| file_reads | `groupby_scan` | 38.05ms | 40.97ms | 1.1× | 0.9% | PASS |
| file_reads | `index_join` | 12.37ms | 13.83ms | 1.1× | 2.3% | PASS |
| file_reads | `index_join_scan` | 5.09ms | 7.19ms | 1.4× | 2.8% | PASS |
| file_reads | `types_table_scan` | 1.09s | 1.37s | 1.3× | 1.2% | PASS |
| file_reads | `table_scan` | 1.32s | 1.48s | 1.1× | 0.9% | PASS |
| file_reads | `oltp_read_only` | 259.64ms | 216.23ms | 0.8× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 261.39ms | 378.64ms | 1.4× | 0.8% | PASS |
| file_writes | `oltp_insert` | 26.59ms | 39.63ms | 1.5× | 1.8% | PASS |
| file_writes | `oltp_update_index` | 101.09ms | 148.29ms | 1.5× | 2.3% | PASS |
| file_writes | `oltp_update_non_index` | 80.74ms | 102.51ms | 1.3× | 1.7% | PASS |
| file_writes | `oltp_delete_insert` | 80.30ms | 113.48ms | 1.4× | 2.7% | PASS |
| file_writes | `oltp_write_only` | 52.81ms | 66.44ms | 1.3× | 2.0% | PASS |
| file_writes | `types_delete_insert` | 52.04ms | 68.43ms | 1.3× | 2.3% | PASS |
| file_writes | `oltp_read_write` | 135.54ms | 174.26ms | 1.3× | 1.9% | PASS |
| ac_reads | `oltp_point_select` | 56.60ms | 61.86ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_range_select` | 23.21ms | 25.64ms | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_sum_range` | 21.10ms | 24.87ms | 1.2× | 1.5% | PASS |
| ac_reads | `oltp_order_range` | 3.98ms | 4.54ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_distinct_range` | 5.26ms | 5.72ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_index_scan` | 7.30ms | 9.00ms | 1.2× | 1.8% | PASS |
| ac_reads | `select_random_points` | 33.01ms | 38.57ms | 1.2× | 2.0% | PASS |
| ac_reads | `select_random_ranges` | 10.39ms | 11.83ms | 1.1× | 1.2% | PASS |
| ac_reads | `covering_index_scan` | 10.52ms | 13.61ms | 1.3× | 1.8% | PASS |
| ac_reads | `groupby_scan` | 37.33ms | 40.80ms | 1.1× | 0.9% | PASS |
| ac_reads | `index_join` | 9.94ms | 13.90ms | 1.4× | 1.7% | PASS |
| ac_reads | `index_join_scan` | 4.65ms | 7.27ms | 1.6× | 3.6% | PASS |
| ac_reads | `types_table_scan` | 1.09s | 1.38s | 1.3× | 1.1% | PASS |
| ac_reads | `table_scan` | 1.33s | 1.48s | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_read_only` | 194.43ms | 215.71ms | 1.1× | 1.1% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.71ms | 66.31ms | 2.9× | 5.4% | PASS |
| ac_writes | `oltp_insert_ac` | 23.86ms | 82.90ms | 3.5× | 6.7% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.12ms | 94.13ms | 3.5× | 6.3% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.83ms | 76.15ms | 3.3× | 5.7% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 23.45ms | 84.20ms | 3.6× | 5.9% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.61ms | 84.88ms | 3.3× | 5.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.32ms | 78.09ms | 3.5× | 9.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.01ms | 92.81ms | 2.9× | 7.0% | PASS |

</details>

</details>

## Version-control latency

Wall time: 6m 18s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 27.89ms | 38.00ms | 73.4% | 0.3% | PASS |
| `status_dirty_many_tables` | 30.47ms | 42.00ms | 72.6% | 0.2% | PASS |
| `diff_regular_working_one_table` | 23.77ms | 33.00ms | 72.0% | 0.3% | PASS |
| `diff_regular_working_many_tables` | 35.41ms | 50.00ms | 70.8% | 0.2% | PASS |
| `diff_stat_working_many_tables` | 35.46ms | 48.00ms | 73.9% | 0.2% | PASS |
| `diff_schema_working_many_tables` | 35.78ms | 48.00ms | 74.5% | 0.3% | PASS |
| `branch_list_many_branches` | 17.98ms | 25.00ms | 71.9% | 0.3% | PASS |
| `branch_create_delete` | 19.44ms | 27.00ms | 72.0% | 0.4% | PASS |
| `at_literal_deep_history` | 19.51ms | 28.00ms | 69.7% | 0.5% | PASS |
| `diff_literal_deep_history` | 19.54ms | 28.00ms | 69.8% | 0.4% | PASS |
| `history_literal_deep_history` | 20.53ms | 30.00ms | 68.4% | 0.5% | PASS |
| `checkout_branch_clean` | 26.81ms | 43.00ms | 62.3% | 0.3% | PASS |
| `merge_data_no_conflicts` | 21.79ms | 33.00ms | 66.0% | 0.4% | PASS |
| `merge_data_secondary_index` | 755.26ms | 944.00ms | 80.0% | 0.5% | PASS |
| `merge_schema_no_conflicts` | 17.23ms | 24.00ms | 71.8% | 0.6% | PASS |
| `merge_data_conflicts` | 24.09ms | 33.00ms | 73.0% | 0.3% | PASS |
| `merge_data_conflicts_with_resolve` | 24.66ms | 34.00ms | 72.5% | 0.4% | PASS |

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

candidate: 2d20c96.

Fixture file bytes: base 107,968,301; delta 115,879,032.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 442.481 | 411.661–544.072 | 31.885 | 226.00 | 107968301 |
| push_initial | candidate | 773.915 | 772.775–777.163 | 396.633 | 129.21 | 107956807 |
| clone_initial | candidate | 1039.048 | 1026.799–1052.270 | 521.215 | 96.24 | 108475105 |
| push_noop | candidate | 0.607 | 0.542–1.521 | 0.264 | — | 0 |
| fetch_noop | candidate | 0.165 | 0.162–0.179 | 0.168 | — | 0 |
| push_incremental | candidate | 193.039 | 191.979–203.860 | 164.208 | 5.18 | 7909775 |
| fetch_incremental | candidate | 96.604 | 95.085–98.988 | 69.139 | 10.35 | 7910084 |
| pull_fast_forward | candidate | 90.277 | 89.333–91.972 | 61.404 | 11.08 | 7910844 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 90.62 | 175.487 | 108.763 | 18329 | 2066 |
| clone_initial | candidate | 90.97 | 295.172 | 109.784 | 47262 | 2337 |
| push_noop | candidate | 42.59 | 0.073 | 0.000 | 26 | 2 |
| fetch_noop | candidate | 42.59 | 0.135 | 0.000 | 40 | 0 |
| push_incremental | candidate | 42.59 | 96.670 | 7.910 | 22363 | 125 |
| fetch_incremental | candidate | 42.59 | 34.472 | 7.910 | 10808 | 124 |
| pull_fast_forward | candidate | 42.59 | 26.497 | 7.911 | 7031 | 126 |

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

candidate: 2d20c96.

Fixture file bytes: base 1,087,718,066; delta 1,103,525,194.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 5424.533 | 5407.702–5624.823 | 308.461 | 184.35 | 1087718066 |
| push_initial | candidate | 7892.839 | 7889.538–8027.307 | 4119.839 | 126.70 | 1079566310 |
| clone_initial | candidate | 9831.906 | 9781.784–10741.443 | 6046.543 | 101.71 | 1102407396 |
| push_noop | candidate | 0.655 | 0.633–12.751 | 0.260 | — | 0 |
| fetch_noop | candidate | 0.161 | 0.160–0.163 | 0.165 | — | 0 |
| push_incremental | candidate | 1008.366 | 977.842–1010.458 | 931.747 | 0.99 | 15806003 |
| fetch_incremental | candidate | 287.019 | 282.215–301.292 | 212.335 | 3.48 | 15806513 |
| pull_fast_forward | candidate | 279.504 | 277.621–280.263 | 209.031 | 3.58 | 15807273 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 136.64 | 2092.248 | 1087.622 | 263588 | 20541 |
| clone_initial | candidate | 124.06 | 3516.936 | 1132.819 | 612079 | 32243 |
| push_noop | candidate | 45.57 | 0.078 | 0.000 | 27 | 2 |
| fetch_noop | candidate | 45.57 | 0.137 | 0.000 | 40 | 0 |
| push_incremental | candidate | 48.31 | 616.651 | 15.928 | 149769 | 313 |
| fetch_incremental | candidate | 50.46 | 62.316 | 15.928 | 13915 | 313 |
| pull_fast_forward | candidate | 50.49 | 62.248 | 15.929 | 13911 | 315 |

</details>


</details>

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh`, `test/vc_perf_ceiling.sh`, and `test/remote_perf.py`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
