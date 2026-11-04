# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-11-04 11:33 UTC
>
> Commit: [`7bcf12581892705976702ddf1dac2474b4c128c6`](https://github.com/dolthub/doltlite/commit/7bcf12581892705976702ddf1dac2474b4c128c6)
>
> Runner: ubuntu24 20261026.354.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/41808292684)

The SQL workloads compare optimized DoltLite against stock SQLite on the same GitHub-hosted runner, alternating execution order on each repetition. Version-control sections measure DoltLite latency. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.13s | 12.32s | 1.2× | 1.5% | **PASS** |
| Writes | 2.17s | 3.68s | 1.7× | 1.3% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.81s | 12.52s | 1.2× | 1.4% | **PASS** |
| Writes | 3.00s | 4.00s | 1.3× | 2.0% | **PASS** |
| Autocommit writes | 789.46ms | 2.52s | 3.2× | 6.7% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.51s | 2.87s | 1.1× | 1.6% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.49s | 2.99s | 1.2× | 0.9% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.64s | 3.23s | 1.2× | 2.1% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.50s | 3.23s | 1.3× | 1.5% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 445.14ms | 794.82ms | 1.8× | 1.5% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 528.25ms | 889.85ms | 1.7× | 1.1% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 609.78ms | 1.00s | 1.6× | 2.5% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 584.58ms | 993.35ms | 1.7× | 1.2% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.60s | 2.89s | 1.1× | 1.4% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.60s | 3.03s | 1.2× | 1.0% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.88s | 3.30s | 1.1× | 1.7% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.72s | 3.29s | 1.2× | 1.8% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 601.57ms | 850.38ms | 1.4× | 1.9% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 775.80ms | 984.20ms | 1.3× | 3.7% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 861.69ms | 1.10s | 1.3× | 2.4% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 759.58ms | 1.07s | 1.4× | 1.9% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.54s | 2.91s | 1.1× | 1.6% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.54s | 3.04s | 1.2× | 1.1% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.70s | 3.29s | 1.2× | 2.2% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.59s | 3.29s | 1.3× | 1.2% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 196.61ms | 640.86ms | 3.3× | 6.6% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 150.65ms | 498.01ms | 3.3× | 6.6% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 247.41ms | 743.47ms | 3.0× | 7.0% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 194.79ms | 639.22ms | 3.3× | 6.5% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.31ms | 33.20ms | 1.4× | 1.8% | PASS |
| mem_reads | `oltp_range_select` | 10.39ms | 13.07ms | 1.3× | 2.1% | PASS |
| mem_reads | `oltp_sum_range` | 9.65ms | 12.65ms | 1.3× | 2.1% | PASS |
| mem_reads | `oltp_order_range` | 2.62ms | 3.10ms | 1.2× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 3.74ms | 4.33ms | 1.2× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 3.94ms | 5.80ms | 1.5× | 1.9% | PASS |
| mem_reads | `select_random_points` | 12.85ms | 12.08ms | 0.9× | 2.4% | PASS |
| mem_reads | `select_random_ranges` | 4.74ms | 5.61ms | 1.2× | 1.6% | PASS |
| mem_reads | `covering_index_scan` | 7.67ms | 11.03ms | 1.4× | 1.1% | PASS |
| mem_reads | `groupby_scan` | 30.10ms | 33.78ms | 1.1× | 0.9% | PASS |
| mem_reads | `index_join` | 5.79ms | 8.91ms | 1.5× | 1.8% | PASS |
| mem_reads | `index_join_scan` | 3.20ms | 5.90ms | 1.8× | 2.3% | PASS |
| mem_reads | `types_table_scan` | 1.07s | 1.24s | 1.2× | 1.4% | PASS |
| mem_reads | `table_scan` | 1.21s | 1.35s | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_read_only` | 107.17ms | 131.73ms | 1.2× | 1.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 181.70ms | 317.07ms | 1.7× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 15.28ms | 27.19ms | 1.8× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 50.50ms | 99.23ms | 2.0× | 1.3% | PASS |
| mem_writes | `oltp_update_non_index` | 34.78ms | 66.72ms | 1.9× | 1.7% | PASS |
| mem_writes | `oltp_delete_insert` | 44.99ms | 78.07ms | 1.7× | 1.1% | PASS |
| mem_writes | `oltp_write_only` | 22.37ms | 48.46ms | 2.2× | 1.7% | PASS |
| mem_writes | `types_delete_insert` | 25.41ms | 40.67ms | 1.6× | 1.8% | PASS |
| mem_writes | `oltp_read_write` | 70.13ms | 117.41ms | 1.7× | 2.2% | PASS |
| file_reads | `oltp_point_select` | 90.57ms | 50.67ms | 0.6× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 17.64ms | 15.07ms | 0.9× | 2.1% | PASS |
| file_reads | `oltp_sum_range` | 16.38ms | 14.68ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_order_range` | 3.48ms | 3.37ms | 1.0× | 1.6% | PASS |
| file_reads | `oltp_distinct_range` | 4.56ms | 4.61ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 10.83ms | 7.82ms | 0.7× | 1.3% | PASS |
| file_reads | `select_random_points` | 20.71ms | 14.09ms | 0.7× | 1.4% | PASS |
| file_reads | `select_random_ranges` | 11.49ms | 7.53ms | 0.7× | 1.5% | PASS |
| file_reads | `covering_index_scan` | 15.06ms | 13.23ms | 0.9× | 1.8% | PASS |
| file_reads | `groupby_scan` | 31.06ms | 34.08ms | 1.1× | 0.7% | PASS |
| file_reads | `index_join` | 9.78ms | 10.56ms | 1.1× | 1.9% | PASS |
| file_reads | `index_join_scan` | 4.20ms | 6.26ms | 1.5× | 2.0% | PASS |
| file_reads | `types_table_scan` | 1.02s | 1.22s | 1.2× | 0.8% | PASS |
| file_reads | `table_scan` | 1.15s | 1.33s | 1.2× | 0.9% | PASS |
| file_reads | `oltp_read_only` | 198.68ms | 155.74ms | 0.8× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 197.05ms | 326.72ms | 1.7× | 1.6% | PASS |
| file_writes | `oltp_insert` | 22.29ms | 30.91ms | 1.4× | 1.2% | PASS |
| file_writes | `oltp_update_index` | 78.86ms | 109.21ms | 1.4× | 1.4% | PASS |
| file_writes | `oltp_update_non_index` | 58.01ms | 72.94ms | 1.3× | 1.9% | PASS |
| file_writes | `oltp_delete_insert` | 68.58ms | 89.98ms | 1.3× | 2.4% | PASS |
| file_writes | `oltp_write_only` | 45.19ms | 54.25ms | 1.2× | 2.4% | PASS |
| file_writes | `types_delete_insert` | 40.63ms | 46.03ms | 1.1× | 2.2% | PASS |
| file_writes | `oltp_read_write` | 90.95ms | 120.33ms | 1.3× | 1.9% | PASS |
| ac_reads | `oltp_point_select` | 45.38ms | 49.88ms | 1.1× | 0.8% | PASS |
| ac_reads | `oltp_range_select` | 12.74ms | 14.83ms | 1.2× | 2.1% | PASS |
| ac_reads | `oltp_sum_range` | 11.81ms | 14.52ms | 1.2× | 1.6% | PASS |
| ac_reads | `oltp_order_range` | 2.91ms | 3.32ms | 1.1× | 1.6% | PASS |
| ac_reads | `oltp_distinct_range` | 4.01ms | 4.55ms | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_index_scan` | 6.28ms | 7.71ms | 1.2× | 1.7% | PASS |
| ac_reads | `select_random_points` | 15.52ms | 13.88ms | 0.9× | 2.2% | PASS |
| ac_reads | `select_random_ranges` | 7.02ms | 7.46ms | 1.1× | 1.7% | PASS |
| ac_reads | `covering_index_scan` | 10.37ms | 13.14ms | 1.3× | 1.6% | PASS |
| ac_reads | `groupby_scan` | 30.22ms | 33.84ms | 1.1× | 1.0% | PASS |
| ac_reads | `index_join` | 7.47ms | 10.56ms | 1.4× | 2.3% | PASS |
| ac_reads | `index_join_scan` | 3.75ms | 6.28ms | 1.7× | 2.2% | PASS |
| ac_reads | `types_table_scan` | 1.02s | 1.22s | 1.2× | 0.7% | PASS |
| ac_reads | `table_scan` | 1.22s | 1.35s | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_read_only` | 140.43ms | 158.69ms | 1.1× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.20ms | 68.12ms | 3.1× | 9.6% | PASS |
| ac_writes | `oltp_insert_ac` | 24.89ms | 82.68ms | 3.3× | 5.8% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.84ms | 92.52ms | 3.3× | 8.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.49ms | 72.38ms | 3.2× | 7.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.04ms | 83.13ms | 3.3× | 8.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 23.74ms | 81.78ms | 3.4× | 5.7% | PASS |
| ac_writes | `types_delete_insert_ac` | 21.38ms | 73.74ms | 3.4× | 6.1% | PASS |
| ac_writes | `oltp_read_write_ac` | 29.02ms | 86.52ms | 3.0× | 5.6% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.03ms | 38.92ms | 1.2× | 1.3% | PASS |
| mem_reads | `oltp_range_select` | 14.65ms | 15.84ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_sum_range` | 14.23ms | 15.32ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_order_range` | 2.98ms | 3.29ms | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_distinct_range` | 3.81ms | 4.25ms | 1.1× | 0.4% | PASS |
| mem_reads | `oltp_index_scan` | 3.65ms | 6.47ms | 1.8× | 0.6% | PASS |
| mem_reads | `select_random_points` | 21.01ms | 23.37ms | 1.1× | 1.5% | PASS |
| mem_reads | `select_random_ranges` | 5.72ms | 6.65ms | 1.2× | 1.2% | PASS |
| mem_reads | `covering_index_scan` | 6.60ms | 9.95ms | 1.5× | 1.6% | PASS |
| mem_reads | `groupby_scan` | 31.64ms | 33.52ms | 1.1× | 0.4% | PASS |
| mem_reads | `index_join` | 9.78ms | 10.59ms | 1.1× | 1.8% | PASS |
| mem_reads | `index_join_scan` | 3.15ms | 6.80ms | 2.2× | 1.1% | PASS |
| mem_reads | `types_table_scan` | 1.02s | 1.29s | 1.3× | 0.5% | PASS |
| mem_reads | `table_scan` | 1.19s | 1.38s | 1.2× | 0.7% | PASS |
| mem_reads | `oltp_read_only` | 129.69ms | 144.39ms | 1.1× | 0.7% | PASS |
| mem_writes | `oltp_bulk_insert` | 207.15ms | 317.30ms | 1.5× | 0.5% | PASS |
| mem_writes | `oltp_insert` | 15.88ms | 33.33ms | 2.1× | 0.6% | PASS |
| mem_writes | `oltp_update_index` | 59.51ms | 131.82ms | 2.2× | 1.3% | PASS |
| mem_writes | `oltp_update_non_index` | 44.31ms | 73.55ms | 1.7× | 1.4% | PASS |
| mem_writes | `oltp_delete_insert` | 48.31ms | 95.64ms | 2.0× | 0.9% | PASS |
| mem_writes | `oltp_write_only` | 24.55ms | 48.59ms | 2.0× | 1.2% | PASS |
| mem_writes | `types_delete_insert` | 34.52ms | 56.32ms | 1.6× | 1.0% | PASS |
| mem_writes | `oltp_read_write` | 94.02ms | 133.29ms | 1.4× | 1.3% | PASS |
| file_reads | `oltp_point_select` | 60.67ms | 46.98ms | 0.8× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 17.87ms | 16.92ms | 0.9× | 1.2% | PASS |
| file_reads | `oltp_sum_range` | 17.28ms | 16.29ms | 0.9× | 1.2% | PASS |
| file_reads | `oltp_order_range` | 3.39ms | 3.43ms | 1.0× | 1.1% | PASS |
| file_reads | `oltp_distinct_range` | 4.24ms | 4.39ms | 1.0× | 1.0% | PASS |
| file_reads | `oltp_index_scan` | 6.70ms | 7.45ms | 1.1× | 1.0% | PASS |
| file_reads | `select_random_points` | 25.31ms | 24.88ms | 1.0× | 1.3% | PASS |
| file_reads | `select_random_ranges` | 8.86ms | 7.62ms | 0.9× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 9.80ms | 10.96ms | 1.1× | 1.0% | PASS |
| file_reads | `groupby_scan` | 32.10ms | 33.62ms | 1.0× | 0.6% | PASS |
| file_reads | `index_join` | 11.83ms | 11.13ms | 0.9× | 2.1% | PASS |
| file_reads | `index_join_scan` | 3.67ms | 7.06ms | 1.9× | 1.6% | PASS |
| file_reads | `types_table_scan` | 1.02s | 1.29s | 1.3× | 0.5% | PASS |
| file_reads | `table_scan` | 1.21s | 1.39s | 1.2× | 0.6% | PASS |
| file_reads | `oltp_read_only` | 173.85ms | 160.22ms | 0.9× | 0.7% | PASS |
| file_writes | `oltp_bulk_insert` | 226.80ms | 332.64ms | 1.5× | 0.8% | PASS |
| file_writes | `oltp_insert` | 21.53ms | 38.49ms | 1.8× | 1.4% | PASS |
| file_writes | `oltp_update_index` | 108.60ms | 151.74ms | 1.4× | 6.0% | PASS |
| file_writes | `oltp_update_non_index` | 95.20ms | 86.50ms | 0.9× | 7.3% | PASS |
| file_writes | `oltp_delete_insert` | 74.55ms | 110.67ms | 1.5× | 1.1% | PASS |
| file_writes | `oltp_write_only` | 52.97ms | 59.54ms | 1.1× | 12.8% | PASS |
| file_writes | `types_delete_insert` | 54.37ms | 64.09ms | 1.2× | 1.3% | PASS |
| file_writes | `oltp_read_write` | 141.77ms | 140.53ms | 1.0× | 8.4% | PASS |
| ac_reads | `oltp_point_select` | 42.45ms | 47.05ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_range_select` | 16.17ms | 17.14ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_sum_range` | 15.18ms | 16.29ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_order_range` | 3.19ms | 3.44ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_distinct_range` | 4.03ms | 4.41ms | 1.1× | 0.7% | PASS |
| ac_reads | `oltp_index_scan` | 4.95ms | 7.43ms | 1.5× | 1.0% | PASS |
| ac_reads | `select_random_points` | 23.28ms | 24.91ms | 1.1× | 1.5% | PASS |
| ac_reads | `select_random_ranges` | 7.01ms | 7.61ms | 1.1× | 0.9% | PASS |
| ac_reads | `covering_index_scan` | 7.85ms | 10.96ms | 1.4× | 1.3% | PASS |
| ac_reads | `groupby_scan` | 32.04ms | 33.78ms | 1.1× | 0.3% | PASS |
| ac_reads | `index_join` | 11.32ms | 11.53ms | 1.0× | 1.7% | PASS |
| ac_reads | `index_join_scan` | 3.45ms | 7.00ms | 2.0× | 2.2% | PASS |
| ac_reads | `types_table_scan` | 1.02s | 1.29s | 1.3× | 0.7% | PASS |
| ac_reads | `table_scan` | 1.21s | 1.39s | 1.2× | 0.6% | PASS |
| ac_reads | `oltp_read_only` | 146.72ms | 159.77ms | 1.1× | 0.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 16.89ms | 49.01ms | 2.9× | 5.9% | PASS |
| ac_writes | `oltp_insert_ac` | 19.49ms | 60.58ms | 3.1× | 6.8% | PASS |
| ac_writes | `oltp_update_index_ac` | 19.77ms | 73.41ms | 3.7× | 8.0% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 15.77ms | 55.63ms | 3.5× | 8.2% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 17.87ms | 64.41ms | 3.6× | 6.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 18.64ms | 64.31ms | 3.4× | 6.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 17.73ms | 58.79ms | 3.3× | 7.8% | PASS |
| ac_writes | `oltp_read_write_ac` | 24.48ms | 71.88ms | 2.9× | 6.2% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 36.75ms | 42.56ms | 1.2× | 2.8% | PASS |
| mem_reads | `oltp_range_select` | 17.43ms | 17.14ms | 1.0× | 2.4% | PASS |
| mem_reads | `oltp_sum_range` | 15.45ms | 16.73ms | 1.1× | 1.9% | PASS |
| mem_reads | `oltp_order_range` | 3.32ms | 3.69ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_distinct_range` | 4.45ms | 4.83ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_index_scan` | 4.04ms | 7.13ms | 1.8× | 1.6% | PASS |
| mem_reads | `select_random_points` | 23.44ms | 23.91ms | 1.0× | 4.6% | PASS |
| mem_reads | `select_random_ranges` | 6.75ms | 7.31ms | 1.1× | 2.1% | PASS |
| mem_reads | `covering_index_scan` | 7.72ms | 11.53ms | 1.5× | 0.9% | PASS |
| mem_reads | `groupby_scan` | 34.59ms | 37.84ms | 1.1× | 0.8% | PASS |
| mem_reads | `index_join` | 10.75ms | 11.31ms | 1.1× | 2.1% | PASS |
| mem_reads | `index_join_scan` | 3.93ms | 6.89ms | 1.8× | 3.0% | PASS |
| mem_reads | `types_table_scan` | 1.10s | 1.39s | 1.3× | 2.1% | PASS |
| mem_reads | `table_scan` | 1.22s | 1.49s | 1.2× | 2.4% | PASS |
| mem_reads | `oltp_read_only` | 146.10ms | 158.46ms | 1.1× | 3.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 240.50ms | 372.04ms | 1.5× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 18.50ms | 37.71ms | 2.0× | 1.1% | PASS |
| mem_writes | `oltp_update_index` | 69.26ms | 139.84ms | 2.0× | 3.4% | PASS |
| mem_writes | `oltp_update_non_index` | 51.85ms | 84.97ms | 1.6× | 3.6% | PASS |
| mem_writes | `oltp_delete_insert` | 54.08ms | 102.78ms | 1.9× | 2.4% | PASS |
| mem_writes | `oltp_write_only` | 29.29ms | 54.82ms | 1.9× | 2.3% | PASS |
| mem_writes | `types_delete_insert` | 40.06ms | 64.60ms | 1.6× | 2.6% | PASS |
| mem_writes | `oltp_read_write` | 106.25ms | 145.08ms | 1.4× | 3.3% | PASS |
| file_reads | `oltp_point_select` | 102.89ms | 59.96ms | 0.6× | 1.6% | PASS |
| file_reads | `oltp_range_select` | 24.83ms | 19.12ms | 0.8× | 1.7% | PASS |
| file_reads | `oltp_sum_range` | 22.93ms | 18.69ms | 0.8× | 2.2% | PASS |
| file_reads | `oltp_order_range` | 4.39ms | 3.95ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_distinct_range` | 5.53ms | 5.08ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 10.99ms | 9.02ms | 0.8× | 1.4% | PASS |
| file_reads | `select_random_points` | 32.36ms | 26.56ms | 0.8× | 2.8% | PASS |
| file_reads | `select_random_ranges` | 13.86ms | 9.17ms | 0.7× | 1.6% | PASS |
| file_reads | `covering_index_scan` | 15.05ms | 13.50ms | 0.9× | 1.7% | PASS |
| file_reads | `groupby_scan` | 35.81ms | 38.43ms | 1.1× | 1.0% | PASS |
| file_reads | `index_join` | 15.50ms | 12.70ms | 0.8× | 2.9% | PASS |
| file_reads | `index_join_scan` | 4.91ms | 7.30ms | 1.5× | 1.9% | PASS |
| file_reads | `types_table_scan` | 1.09s | 1.39s | 1.3× | 1.9% | PASS |
| file_reads | `table_scan` | 1.25s | 1.50s | 1.2× | 2.2% | PASS |
| file_reads | `oltp_read_only` | 249.94ms | 185.87ms | 0.7× | 1.9% | PASS |
| file_writes | `oltp_bulk_insert` | 261.13ms | 385.94ms | 1.5× | 1.1% | PASS |
| file_writes | `oltp_insert` | 28.82ms | 42.73ms | 1.5× | 2.2% | PASS |
| file_writes | `oltp_update_index` | 109.28ms | 159.75ms | 1.5× | 2.1% | PASS |
| file_writes | `oltp_update_non_index` | 90.75ms | 97.11ms | 1.1× | 4.4% | PASS |
| file_writes | `oltp_delete_insert` | 93.98ms | 120.47ms | 1.3× | 2.7% | PASS |
| file_writes | `oltp_write_only` | 66.63ms | 67.64ms | 1.0× | 2.4% | PASS |
| file_writes | `types_delete_insert` | 68.05ms | 72.56ms | 1.1× | 2.3% | PASS |
| file_writes | `oltp_read_write` | 143.05ms | 156.69ms | 1.1× | 3.5% | PASS |
| ac_reads | `oltp_point_select` | 58.90ms | 60.30ms | 1.0× | 2.4% | PASS |
| ac_reads | `oltp_range_select` | 20.38ms | 19.21ms | 0.9× | 2.0% | PASS |
| ac_reads | `oltp_sum_range` | 18.57ms | 19.04ms | 1.0× | 2.5% | PASS |
| ac_reads | `oltp_order_range` | 3.88ms | 3.94ms | 1.0× | 2.0% | PASS |
| ac_reads | `oltp_distinct_range` | 4.99ms | 5.09ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_index_scan` | 6.68ms | 9.00ms | 1.3× | 1.9% | PASS |
| ac_reads | `select_random_points` | 26.69ms | 26.50ms | 1.0× | 4.2% | PASS |
| ac_reads | `select_random_ranges` | 9.37ms | 9.21ms | 1.0× | 1.8% | PASS |
| ac_reads | `covering_index_scan` | 10.72ms | 13.52ms | 1.3× | 2.0% | PASS |
| ac_reads | `groupby_scan` | 34.92ms | 38.18ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 13.09ms | 12.73ms | 1.0× | 3.7% | PASS |
| ac_reads | `index_join_scan` | 4.40ms | 7.35ms | 1.7× | 2.5% | PASS |
| ac_reads | `types_table_scan` | 1.09s | 1.39s | 1.3× | 2.2% | PASS |
| ac_reads | `table_scan` | 1.22s | 1.49s | 1.2× | 2.2% | PASS |
| ac_reads | `oltp_read_only` | 183.38ms | 185.46ms | 1.0× | 3.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 28.38ms | 76.20ms | 2.7× | 6.4% | PASS |
| ac_writes | `oltp_insert_ac` | 31.23ms | 91.32ms | 2.9× | 7.7% | PASS |
| ac_writes | `oltp_update_index_ac` | 32.62ms | 107.90ms | 3.3× | 7.0% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 27.99ms | 84.59ms | 3.0× | 8.1% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 29.69ms | 94.91ms | 3.2× | 7.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 31.44ms | 95.64ms | 3.0× | 7.0% | PASS |
| ac_writes | `types_delete_insert_ac` | 28.81ms | 89.91ms | 3.1× | 6.6% | PASS |
| ac_writes | `oltp_read_write_ac` | 37.25ms | 103.00ms | 2.8× | 5.7% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 32.00ms | 43.84ms | 1.4× | 1.2% | PASS |
| mem_reads | `oltp_range_select` | 18.82ms | 23.70ms | 1.3× | 1.6% | PASS |
| mem_reads | `oltp_sum_range` | 17.39ms | 22.95ms | 1.3× | 1.0% | PASS |
| mem_reads | `oltp_order_range` | 3.44ms | 4.25ms | 1.2× | 1.5% | PASS |
| mem_reads | `oltp_distinct_range` | 4.62ms | 5.42ms | 1.2× | 1.9% | PASS |
| mem_reads | `oltp_index_scan` | 4.37ms | 6.56ms | 1.5× | 1.9% | PASS |
| mem_reads | `select_random_points` | 26.97ms | 33.85ms | 1.3× | 1.5% | PASS |
| mem_reads | `select_random_ranges` | 7.45ms | 9.65ms | 1.3× | 1.9% | PASS |
| mem_reads | `covering_index_scan` | 7.64ms | 11.12ms | 1.5× | 1.0% | PASS |
| mem_reads | `groupby_scan` | 36.13ms | 41.49ms | 1.1× | 1.1% | PASS |
| mem_reads | `index_join` | 7.86ms | 11.62ms | 1.5× | 1.6% | PASS |
| mem_reads | `index_join_scan` | 3.79ms | 6.68ms | 1.8× | 1.6% | PASS |
| mem_reads | `types_table_scan` | 1.03s | 1.36s | 1.3× | 0.7% | PASS |
| mem_reads | `table_scan` | 1.15s | 1.46s | 1.3× | 0.7% | PASS |
| mem_reads | `oltp_read_only` | 147.21ms | 186.85ms | 1.3× | 0.7% | PASS |
| mem_writes | `oltp_bulk_insert` | 244.16ms | 365.07ms | 1.5× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 18.76ms | 34.56ms | 1.8× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 64.73ms | 125.45ms | 1.9× | 1.4% | PASS |
| mem_writes | `oltp_update_non_index` | 50.16ms | 91.61ms | 1.8× | 1.5% | PASS |
| mem_writes | `oltp_delete_insert` | 48.38ms | 95.52ms | 2.0× | 0.8% | PASS |
| mem_writes | `oltp_write_only` | 26.46ms | 57.55ms | 2.2× | 1.0% | PASS |
| mem_writes | `types_delete_insert` | 32.25ms | 60.43ms | 1.9× | 1.3% | PASS |
| mem_writes | `oltp_read_write` | 99.69ms | 163.15ms | 1.6× | 1.7% | PASS |
| file_reads | `oltp_point_select` | 97.43ms | 60.69ms | 0.6× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 26.47ms | 25.81ms | 1.0× | 2.5% | PASS |
| file_reads | `oltp_sum_range` | 24.80ms | 25.35ms | 1.0× | 1.9% | PASS |
| file_reads | `oltp_order_range` | 4.38ms | 4.59ms | 1.0× | 2.6% | PASS |
| file_reads | `oltp_distinct_range` | 5.59ms | 5.79ms | 1.0× | 1.9% | PASS |
| file_reads | `oltp_index_scan` | 11.32ms | 8.76ms | 0.8× | 1.9% | PASS |
| file_reads | `select_random_points` | 36.31ms | 37.29ms | 1.0× | 1.8% | PASS |
| file_reads | `select_random_ranges` | 14.64ms | 11.81ms | 0.8× | 1.9% | PASS |
| file_reads | `covering_index_scan` | 14.62ms | 13.45ms | 0.9× | 1.1% | PASS |
| file_reads | `groupby_scan` | 37.44ms | 42.13ms | 1.1× | 1.1% | PASS |
| file_reads | `index_join` | 11.90ms | 13.52ms | 1.1× | 1.8% | PASS |
| file_reads | `index_join_scan` | 4.85ms | 7.07ms | 1.5× | 2.1% | PASS |
| file_reads | `types_table_scan` | 1.03s | 1.36s | 1.3× | 0.8% | PASS |
| file_reads | `table_scan` | 1.16s | 1.46s | 1.3× | 0.6% | PASS |
| file_reads | `oltp_read_only` | 246.74ms | 214.70ms | 0.9× | 1.2% | PASS |
| file_writes | `oltp_bulk_insert` | 258.30ms | 375.88ms | 1.5× | 0.7% | PASS |
| file_writes | `oltp_insert` | 26.00ms | 38.66ms | 1.5× | 1.8% | PASS |
| file_writes | `oltp_update_index` | 94.93ms | 141.81ms | 1.5× | 1.3% | PASS |
| file_writes | `oltp_update_non_index` | 76.28ms | 99.27ms | 1.3× | 2.0% | PASS |
| file_writes | `oltp_delete_insert` | 76.34ms | 109.73ms | 1.4× | 1.5% | PASS |
| file_writes | `oltp_write_only` | 51.21ms | 64.97ms | 1.3× | 2.2% | PASS |
| file_writes | `types_delete_insert` | 49.39ms | 66.16ms | 1.3× | 2.1% | PASS |
| file_writes | `oltp_read_write` | 127.13ms | 171.01ms | 1.3× | 2.0% | PASS |
| ac_reads | `oltp_point_select` | 54.80ms | 60.63ms | 1.1× | 0.8% | PASS |
| ac_reads | `oltp_range_select` | 22.24ms | 26.00ms | 1.2× | 1.6% | PASS |
| ac_reads | `oltp_sum_range` | 20.32ms | 25.27ms | 1.2× | 1.7% | PASS |
| ac_reads | `oltp_order_range` | 3.94ms | 4.60ms | 1.2× | 1.4% | PASS |
| ac_reads | `oltp_distinct_range` | 5.17ms | 5.79ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_index_scan` | 6.89ms | 8.79ms | 1.3× | 2.0% | PASS |
| ac_reads | `select_random_points` | 31.46ms | 37.63ms | 1.2× | 1.2% | PASS |
| ac_reads | `select_random_ranges` | 10.08ms | 11.81ms | 1.2× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 10.18ms | 13.47ms | 1.3× | 1.5% | PASS |
| ac_reads | `groupby_scan` | 36.85ms | 42.07ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 9.50ms | 13.56ms | 1.4× | 1.0% | PASS |
| ac_reads | `index_join_scan` | 4.37ms | 7.07ms | 1.6× | 1.8% | PASS |
| ac_reads | `types_table_scan` | 1.03s | 1.36s | 1.3× | 1.0% | PASS |
| ac_reads | `table_scan` | 1.16s | 1.46s | 1.3× | 1.1% | PASS |
| ac_reads | `oltp_read_only` | 183.03ms | 214.74ms | 1.2× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.48ms | 65.97ms | 2.9× | 9.2% | PASS |
| ac_writes | `oltp_insert_ac` | 23.65ms | 79.91ms | 3.4× | 5.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.21ms | 91.87ms | 3.5× | 3.9% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.90ms | 73.78ms | 3.4× | 6.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 23.59ms | 83.01ms | 3.5× | 6.4% | PASS |
| ac_writes | `oltp_write_only_ac` | 24.61ms | 82.14ms | 3.3× | 7.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 20.60ms | 73.78ms | 3.6× | 7.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.74ms | 88.76ms | 2.8× | 5.9% | PASS |

</details>

</details>

## Version-control latency

Wall time: 7m 18s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 34.33ms | 38.00ms | 90.3% | 0.8% | PASS |
| `status_dirty_many_tables` | 37.68ms | 42.00ms | 89.7% | 0.7% | PASS |
| `diff_regular_working_one_table` | 29.41ms | 33.00ms | 89.1% | 1.0% | PASS |
| `diff_regular_working_many_tables` | 42.86ms | 50.00ms | 85.7% | 0.7% | PASS |
| `diff_stat_working_many_tables` | 43.21ms | 48.00ms | 90.0% | 0.8% | PASS |
| `diff_schema_working_many_tables` | 43.44ms | 48.00ms | 90.5% | 0.8% | PASS |
| `branch_list_many_branches` | 21.14ms | 25.00ms | 84.6% | 1.4% | PASS |
| `branch_create_delete` | 23.34ms | 27.00ms | 86.5% | 1.3% | PASS |
| `at_literal_deep_history` | 22.93ms | 28.00ms | 81.9% | 1.4% | PASS |
| `diff_literal_deep_history` | 22.82ms | 28.00ms | 81.5% | 1.4% | PASS |
| `history_literal_deep_history` | 23.93ms | 30.00ms | 79.8% | 1.0% | PASS |
| `checkout_branch_clean` | 33.87ms | 43.00ms | 78.8% | 1.0% | PASS |
| `merge_data_no_conflicts` | 26.55ms | 33.00ms | 80.5% | 1.0% | PASS |
| `merge_data_secondary_index` | 894.55ms | 944.00ms | 94.8% | 0.5% | PASS |
| `merge_schema_no_conflicts` | 21.56ms | 24.00ms | 89.8% | 1.6% | PASS |
| `merge_data_conflicts` | 29.06ms | 33.00ms | 88.1% | 0.7% | PASS |
| `merge_data_conflicts_with_resolve` | 30.12ms | 34.00ms | 88.6% | 0.9% | PASS |

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

candidate: 7bcf125.

Fixture file bytes: base 107,968,301; delta 115,879,032.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 496.996 | 465.293–607.671 | 39.689 | 201.21 | 107968301 |
| push_initial | candidate | 853.741 | 849.095–971.411 | 433.134 | 117.13 | 107956807 |
| clone_initial | candidate | 984.743 | 979.109–987.458 | 563.081 | 101.55 | 108475105 |
| push_noop | candidate | 0.866 | 0.851–1.422 | 0.347 | — | 0 |
| fetch_noop | candidate | 0.131 | 0.128–0.165 | 0.132 | — | 0 |
| push_incremental | candidate | 202.965 | 198.188–217.926 | 175.250 | 4.93 | 7909775 |
| fetch_incremental | candidate | 104.613 | 101.411–107.867 | 76.279 | 9.56 | 7910084 |
| pull_fast_forward | candidate | 99.671 | 95.352–100.573 | 68.031 | 10.03 | 7910844 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 90.55 | 175.487 | 108.763 | 18329 | 2066 |
| clone_initial | candidate | 90.93 | 295.057 | 109.784 | 47235 | 2337 |
| push_noop | candidate | 42.55 | 0.073 | 0.000 | 26 | 2 |
| fetch_noop | candidate | 42.55 | 0.135 | 0.000 | 40 | 0 |
| push_incremental | candidate | 42.55 | 96.801 | 7.910 | 22395 | 125 |
| fetch_incremental | candidate | 42.55 | 34.472 | 7.910 | 10808 | 124 |
| pull_fast_forward | candidate | 42.55 | 26.497 | 7.911 | 7031 | 126 |

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

candidate: 7bcf125.

Fixture file bytes: base 1,087,718,066; delta 1,103,525,194.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 2637.326 | 2635.309–2658.911 | 380.341 | 379.17 | 1087718066 |
| push_initial | candidate | 8108.300 | 8052.979–8232.401 | 5877.977 | 123.33 | 1079566310 |
| clone_initial | candidate | 8868.378 | 8840.516–8919.097 | 8633.077 | 112.76 | 1102407396 |
| push_noop | candidate | 0.876 | 0.671–0.903 | 0.318 | — | 0 |
| fetch_noop | candidate | 0.203 | 0.202–0.208 | 0.205 | — | 0 |
| push_incremental | candidate | 1299.536 | 1296.197–1300.087 | 1294.338 | 0.77 | 15806003 |
| fetch_incremental | candidate | 272.122 | 271.367–278.349 | 265.550 | 3.67 | 15806513 |
| pull_fast_forward | candidate | 276.400 | 275.387–276.476 | 269.928 | 3.62 | 15807273 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 134.86 | 2091.260 | 1087.622 | 263347 | 20541 |
| clone_initial | candidate | 123.93 | 3519.127 | 1132.819 | 612602 | 32243 |
| push_noop | candidate | 45.33 | 0.078 | 0.000 | 27 | 2 |
| fetch_noop | candidate | 45.33 | 0.137 | 0.000 | 40 | 0 |
| push_incremental | candidate | 48.26 | 617.599 | 15.928 | 150000 | 313 |
| fetch_incremental | candidate | 50.41 | 62.057 | 15.928 | 13852 | 313 |
| pull_fast_forward | candidate | 50.33 | 61.990 | 15.929 | 13848 | 315 |

</details>


</details>

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh`, `test/vc_perf_ceiling.sh`, and `test/remote_perf.py`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
