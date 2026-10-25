# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-10-25 11:08 UTC
>
> Commit: [`d8cffc9d8f069f39303f069a991faf5b4dca83e8`](https://github.com/dolthub/doltlite/commit/d8cffc9d8f069f39303f069a991faf5b4dca83e8)
>
> Runner: ubuntu24 20261023.334.3
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/40181324508)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.41s | 12.56s | 1.2× | 1.2% | **PASS** |
| Writes | 2.13s | 3.38s | 1.6× | 1.1% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 11.04s | 12.66s | 1.1× | 1.3% | **PASS** |
| Writes | 3.46s | 4.09s | 1.2× | 1.7% | **PASS** |
| Autocommit writes | 881.91ms | 2.74s | 3.1× | 6.3% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.43s | 2.93s | 1.2× | 1.9% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.77s | 3.23s | 1.2× | 1.5% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.63s | 3.15s | 1.2× | 1.1% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.58s | 3.24s | 1.3× | 1.2% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 449.47ms | 734.22ms | 1.6× | 1.7% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 593.12ms | 955.71ms | 1.6× | 1.2% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 498.12ms | 770.72ms | 1.5× | 0.9% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 587.65ms | 920.07ms | 1.6× | 1.2% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.65s | 3.00s | 1.1× | 1.3% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 3.00s | 3.29s | 1.1× | 1.1% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.54s | 3.05s | 1.2× | 1.3% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.85s | 3.33s | 1.2× | 1.3% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 599.53ms | 797.84ms | 1.3× | 1.9% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 930.68ms | 1.05s | 1.1× | 4.2% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 1.17s | 1.23s | 1.1× | 1.3% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 768.22ms | 1.01s | 1.3× | 1.6% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.50s | 3.00s | 1.2× | 1.5% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.97s | 3.32s | 1.1× | 1.6% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.49s | 3.02s | 1.2× | 0.8% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.69s | 3.32s | 1.2× | 1.3% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 199.72ms | 656.16ms | 3.3× | 6.5% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 213.66ms | 677.79ms | 3.2× | 6.1% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 250.90ms | 705.57ms | 2.8× | 4.2% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 217.62ms | 702.23ms | 3.2× | 8.3% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 23.87ms | 31.96ms | 1.3× | 2.1% | PASS |
| mem_reads | `oltp_range_select` | 10.19ms | 13.00ms | 1.3× | 3.2% | PASS |
| mem_reads | `oltp_sum_range` | 9.35ms | 12.75ms | 1.4× | 2.4% | PASS |
| mem_reads | `oltp_order_range` | 2.61ms | 3.08ms | 1.2× | 1.9% | PASS |
| mem_reads | `oltp_distinct_range` | 3.68ms | 4.24ms | 1.2× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 3.90ms | 5.83ms | 1.5× | 2.2% | PASS |
| mem_reads | `select_random_points` | 9.88ms | 12.04ms | 1.2× | 2.6% | PASS |
| mem_reads | `select_random_ranges` | 4.61ms | 5.48ms | 1.2× | 1.8% | PASS |
| mem_reads | `covering_index_scan` | 7.59ms | 11.11ms | 1.5× | 1.0% | PASS |
| mem_reads | `groupby_scan` | 29.60ms | 34.80ms | 1.2× | 0.8% | PASS |
| mem_reads | `index_join` | 5.74ms | 8.89ms | 1.5× | 2.2% | PASS |
| mem_reads | `index_join_scan` | 3.15ms | 5.76ms | 1.8× | 3.4% | PASS |
| mem_reads | `types_table_scan` | 1.03s | 1.26s | 1.2× | 0.8% | PASS |
| mem_reads | `table_scan` | 1.18s | 1.39s | 1.2× | 1.6% | PASS |
| mem_reads | `oltp_read_only` | 104.22ms | 128.80ms | 1.2× | 1.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 177.33ms | 283.72ms | 1.6× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 15.14ms | 26.79ms | 1.8× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 50.19ms | 90.37ms | 1.8× | 1.6% | PASS |
| mem_writes | `oltp_update_non_index` | 35.20ms | 58.38ms | 1.7× | 2.1% | PASS |
| mem_writes | `oltp_delete_insert` | 47.60ms | 76.25ms | 1.6× | 2.2% | PASS |
| mem_writes | `oltp_write_only` | 24.36ms | 46.08ms | 1.9× | 1.7% | PASS |
| mem_writes | `types_delete_insert` | 26.14ms | 39.61ms | 1.5× | 2.0% | PASS |
| mem_writes | `oltp_read_write` | 73.51ms | 113.01ms | 1.5× | 1.6% | PASS |
| file_reads | `oltp_point_select` | 94.36ms | 51.83ms | 0.5× | 0.7% | PASS |
| file_reads | `oltp_range_select` | 18.44ms | 15.35ms | 0.8× | 1.3% | PASS |
| file_reads | `oltp_sum_range` | 17.34ms | 15.14ms | 0.9× | 1.2% | PASS |
| file_reads | `oltp_order_range` | 3.56ms | 3.39ms | 1.0× | 1.9% | PASS |
| file_reads | `oltp_distinct_range` | 4.63ms | 4.55ms | 1.0× | 1.8% | PASS |
| file_reads | `oltp_index_scan` | 11.28ms | 7.97ms | 0.7× | 1.5% | PASS |
| file_reads | `select_random_points` | 17.22ms | 14.26ms | 0.8× | 2.5% | PASS |
| file_reads | `select_random_ranges` | 11.86ms | 7.49ms | 0.6× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 15.17ms | 13.25ms | 0.9× | 0.7% | PASS |
| file_reads | `groupby_scan` | 30.28ms | 34.98ms | 1.2× | 0.8% | PASS |
| file_reads | `index_join` | 9.94ms | 10.46ms | 1.1× | 1.6% | PASS |
| file_reads | `index_join_scan` | 4.28ms | 6.43ms | 1.5× | 3.1% | PASS |
| file_reads | `types_table_scan` | 1.04s | 1.27s | 1.2× | 0.9% | PASS |
| file_reads | `table_scan` | 1.17s | 1.39s | 1.2× | 1.5% | PASS |
| file_reads | `oltp_read_only` | 203.98ms | 156.52ms | 0.8× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 192.34ms | 293.00ms | 1.5× | 1.2% | PASS |
| file_writes | `oltp_insert` | 22.04ms | 30.04ms | 1.4× | 2.1% | PASS |
| file_writes | `oltp_update_index` | 79.22ms | 105.36ms | 1.3× | 1.9% | PASS |
| file_writes | `oltp_update_non_index` | 57.99ms | 67.77ms | 1.2× | 1.7% | PASS |
| file_writes | `oltp_delete_insert` | 68.09ms | 84.01ms | 1.2× | 1.9% | PASS |
| file_writes | `oltp_write_only` | 45.20ms | 52.88ms | 1.2× | 1.9% | PASS |
| file_writes | `types_delete_insert` | 40.53ms | 44.80ms | 1.1× | 2.1% | PASS |
| file_writes | `oltp_read_write` | 94.12ms | 119.99ms | 1.3× | 1.3% | PASS |
| ac_reads | `oltp_point_select` | 47.03ms | 51.69ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_range_select` | 13.19ms | 15.34ms | 1.2× | 2.0% | PASS |
| ac_reads | `oltp_sum_range` | 12.37ms | 15.06ms | 1.2× | 2.1% | PASS |
| ac_reads | `oltp_order_range` | 3.10ms | 3.45ms | 1.1× | 2.7% | PASS |
| ac_reads | `oltp_distinct_range` | 4.05ms | 4.55ms | 1.1× | 2.0% | PASS |
| ac_reads | `oltp_index_scan` | 6.30ms | 7.91ms | 1.3× | 1.5% | PASS |
| ac_reads | `select_random_points` | 12.13ms | 14.18ms | 1.2× | 1.3% | PASS |
| ac_reads | `select_random_ranges` | 6.98ms | 7.48ms | 1.1× | 1.4% | PASS |
| ac_reads | `covering_index_scan` | 10.10ms | 13.22ms | 1.3× | 1.1% | PASS |
| ac_reads | `groupby_scan` | 29.52ms | 35.04ms | 1.2× | 1.0% | PASS |
| ac_reads | `index_join` | 7.12ms | 10.32ms | 1.4× | 1.4% | PASS |
| ac_reads | `index_join_scan` | 3.56ms | 6.11ms | 1.7× | 2.5% | PASS |
| ac_reads | `types_table_scan` | 1.03s | 1.26s | 1.2× | 0.8% | PASS |
| ac_reads | `table_scan` | 1.17s | 1.39s | 1.2× | 2.2% | PASS |
| ac_reads | `oltp_read_only` | 140.80ms | 160.12ms | 1.1× | 1.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.59ms | 69.48ms | 3.1× | 6.3% | PASS |
| ac_writes | `oltp_insert_ac` | 25.48ms | 83.07ms | 3.3× | 7.1% | PASS |
| ac_writes | `oltp_update_index_ac` | 25.86ms | 92.40ms | 3.6× | 4.9% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.63ms | 78.96ms | 3.3× | 10.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.56ms | 83.84ms | 3.4× | 6.9% | PASS |
| ac_writes | `oltp_write_only_ac` | 24.75ms | 83.59ms | 3.4× | 6.7% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.07ms | 75.45ms | 3.4× | 4.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 30.80ms | 89.37ms | 2.9× | 6.2% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 35.49ms | 41.27ms | 1.2× | 2.2% | PASS |
| mem_reads | `oltp_range_select` | 16.64ms | 16.52ms | 1.0× | 1.6% | PASS |
| mem_reads | `oltp_sum_range` | 15.59ms | 15.91ms | 1.0× | 1.6% | PASS |
| mem_reads | `oltp_order_range` | 3.40ms | 3.50ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 4.46ms | 4.64ms | 1.0× | 0.6% | PASS |
| mem_reads | `oltp_index_scan` | 4.00ms | 6.92ms | 1.7× | 1.2% | PASS |
| mem_reads | `select_random_points` | 23.07ms | 23.41ms | 1.0× | 3.1% | PASS |
| mem_reads | `select_random_ranges` | 6.92ms | 7.14ms | 1.0× | 1.1% | PASS |
| mem_reads | `covering_index_scan` | 7.72ms | 11.50ms | 1.5× | 1.1% | PASS |
| mem_reads | `groupby_scan` | 35.18ms | 37.24ms | 1.1× | 0.7% | PASS |
| mem_reads | `index_join` | 10.15ms | 10.66ms | 1.1× | 1.5% | PASS |
| mem_reads | `index_join_scan` | 3.75ms | 6.66ms | 1.8× | 1.8% | PASS |
| mem_reads | `types_table_scan` | 1.17s | 1.40s | 1.2× | 3.4% | PASS |
| mem_reads | `table_scan` | 1.29s | 1.50s | 1.2× | 4.6% | PASS |
| mem_reads | `oltp_read_only` | 145.44ms | 152.19ms | 1.0× | 1.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 238.05ms | 342.68ms | 1.4× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 17.75ms | 37.07ms | 2.1× | 1.1% | PASS |
| mem_writes | `oltp_update_index` | 65.87ms | 135.11ms | 2.1× | 1.5% | PASS |
| mem_writes | `oltp_update_non_index` | 47.95ms | 82.20ms | 1.7× | 1.7% | PASS |
| mem_writes | `oltp_delete_insert` | 55.37ms | 101.83ms | 1.8× | 1.1% | PASS |
| mem_writes | `oltp_write_only` | 28.69ms | 57.32ms | 2.0× | 0.8% | PASS |
| mem_writes | `types_delete_insert` | 38.55ms | 56.91ms | 1.5× | 1.2% | PASS |
| mem_writes | `oltp_read_write` | 100.90ms | 142.60ms | 1.4× | 1.7% | PASS |
| file_reads | `oltp_point_select` | 107.33ms | 60.86ms | 0.6× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 24.48ms | 18.50ms | 0.8× | 1.7% | PASS |
| file_reads | `oltp_sum_range` | 24.06ms | 18.41ms | 0.8× | 1.1% | PASS |
| file_reads | `oltp_order_range` | 4.33ms | 3.73ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_distinct_range` | 5.31ms | 4.91ms | 0.9× | 1.1% | PASS |
| file_reads | `oltp_index_scan` | 11.37ms | 8.87ms | 0.8× | 1.0% | PASS |
| file_reads | `select_random_points` | 32.06ms | 26.46ms | 0.8× | 1.8% | PASS |
| file_reads | `select_random_ranges` | 14.32ms | 9.13ms | 0.6× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 15.38ms | 13.42ms | 0.9× | 1.1% | PASS |
| file_reads | `groupby_scan` | 35.82ms | 37.08ms | 1.0× | 0.9% | PASS |
| file_reads | `index_join` | 14.95ms | 12.05ms | 0.8× | 2.1% | PASS |
| file_reads | `index_join_scan` | 4.74ms | 7.05ms | 1.5× | 1.8% | PASS |
| file_reads | `types_table_scan` | 1.11s | 1.38s | 1.3× | 2.5% | PASS |
| file_reads | `table_scan` | 1.35s | 1.50s | 1.1× | 3.1% | PASS |
| file_reads | `oltp_read_only` | 251.81ms | 182.39ms | 0.7× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 262.64ms | 358.69ms | 1.4× | 1.3% | PASS |
| file_writes | `oltp_insert` | 25.51ms | 42.08ms | 1.6× | 1.7% | PASS |
| file_writes | `oltp_update_index` | 131.14ms | 151.47ms | 1.2× | 13.2% | PASS |
| file_writes | `oltp_update_non_index` | 104.18ms | 95.01ms | 0.9× | 9.4% | PASS |
| file_writes | `oltp_delete_insert` | 94.77ms | 114.01ms | 1.2× | 1.5% | PASS |
| file_writes | `oltp_write_only` | 89.80ms | 68.50ms | 0.8× | 12.3% | PASS |
| file_writes | `types_delete_insert` | 70.20ms | 67.11ms | 1.0× | 1.6% | PASS |
| file_writes | `oltp_read_write` | 152.44ms | 154.78ms | 1.0× | 6.7% | PASS |
| ac_reads | `oltp_point_select` | 58.24ms | 60.16ms | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_range_select` | 19.00ms | 18.40ms | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_sum_range` | 18.22ms | 18.05ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_order_range` | 3.73ms | 3.72ms | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_distinct_range` | 4.72ms | 4.91ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_index_scan` | 6.49ms | 8.80ms | 1.4× | 1.9% | PASS |
| ac_reads | `select_random_points` | 24.66ms | 25.68ms | 1.0× | 2.6% | PASS |
| ac_reads | `select_random_ranges` | 9.26ms | 9.05ms | 1.0× | 1.2% | PASS |
| ac_reads | `covering_index_scan` | 10.36ms | 13.28ms | 1.3× | 0.9% | PASS |
| ac_reads | `groupby_scan` | 34.57ms | 36.97ms | 1.1× | 0.6% | PASS |
| ac_reads | `index_join` | 12.14ms | 11.91ms | 1.0× | 1.6% | PASS |
| ac_reads | `index_join_scan` | 4.18ms | 6.95ms | 1.7× | 1.4% | PASS |
| ac_reads | `types_table_scan` | 1.20s | 1.41s | 1.2× | 2.4% | PASS |
| ac_reads | `table_scan` | 1.38s | 1.51s | 1.1× | 2.4% | PASS |
| ac_reads | `oltp_read_only` | 186.04ms | 184.25ms | 1.0× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 24.22ms | 67.43ms | 2.8× | 6.2% | PASS |
| ac_writes | `oltp_insert_ac` | 27.24ms | 80.89ms | 3.0× | 5.3% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.55ms | 96.83ms | 3.5× | 6.0% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.98ms | 77.90ms | 3.4× | 6.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.15ms | 91.52ms | 3.5× | 7.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.83ms | 86.53ms | 3.2× | 6.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 25.67ms | 82.53ms | 3.2× | 9.0% | PASS |
| ac_writes | `oltp_read_write_ac` | 33.03ms | 94.15ms | 2.9× | 6.3% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 32.98ms | 33.98ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_range_select` | 14.13ms | 15.66ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_sum_range` | 14.07ms | 14.32ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_order_range` | 3.00ms | 3.42ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_distinct_range` | 3.92ms | 4.49ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 3.69ms | 5.91ms | 1.6× | 1.1% | PASS |
| mem_reads | `select_random_points` | 21.30ms | 21.45ms | 1.0× | 1.0% | PASS |
| mem_reads | `select_random_ranges` | 5.66ms | 5.87ms | 1.0× | 1.1% | PASS |
| mem_reads | `covering_index_scan` | 6.55ms | 9.61ms | 1.5× | 0.6% | PASS |
| mem_reads | `groupby_scan` | 32.51ms | 35.25ms | 1.1× | 0.4% | PASS |
| mem_reads | `index_join` | 10.49ms | 10.25ms | 1.0× | 1.0% | PASS |
| mem_reads | `index_join_scan` | 3.20ms | 6.33ms | 2.0× | 0.9% | PASS |
| mem_reads | `types_table_scan` | 1.06s | 1.37s | 1.3× | 1.3% | PASS |
| mem_reads | `table_scan` | 1.29s | 1.48s | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_read_only` | 123.58ms | 137.54ms | 1.1× | 0.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 194.22ms | 265.32ms | 1.4× | 0.5% | PASS |
| mem_writes | `oltp_insert` | 15.59ms | 30.56ms | 2.0× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 57.26ms | 115.16ms | 2.0× | 0.9% | PASS |
| mem_writes | `oltp_update_non_index` | 41.72ms | 65.37ms | 1.6× | 1.2% | PASS |
| mem_writes | `oltp_delete_insert` | 46.84ms | 84.87ms | 1.8× | 0.9% | PASS |
| mem_writes | `oltp_write_only` | 24.27ms | 46.23ms | 1.9× | 0.9% | PASS |
| mem_writes | `types_delete_insert` | 33.33ms | 46.53ms | 1.4× | 1.1% | PASS |
| mem_writes | `oltp_read_write` | 84.89ms | 116.68ms | 1.4× | 1.1% | PASS |
| file_reads | `oltp_point_select` | 63.69ms | 41.31ms | 0.6× | 1.1% | PASS |
| file_reads | `oltp_range_select` | 17.37ms | 16.59ms | 1.0× | 1.0% | PASS |
| file_reads | `oltp_sum_range` | 16.83ms | 15.06ms | 0.9× | 1.2% | PASS |
| file_reads | `oltp_order_range` | 3.32ms | 3.53ms | 1.1× | 1.7% | PASS |
| file_reads | `oltp_distinct_range` | 4.25ms | 4.58ms | 1.1× | 1.6% | PASS |
| file_reads | `oltp_index_scan` | 6.96ms | 6.68ms | 1.0× | 1.3% | PASS |
| file_reads | `select_random_points` | 24.69ms | 22.16ms | 0.9× | 1.6% | PASS |
| file_reads | `select_random_ranges` | 9.02ms | 6.73ms | 0.7× | 1.4% | PASS |
| file_reads | `covering_index_scan` | 9.86ms | 10.63ms | 1.1× | 1.5% | PASS |
| file_reads | `groupby_scan` | 32.06ms | 34.58ms | 1.1× | 0.6% | PASS |
| file_reads | `index_join` | 12.46ms | 10.69ms | 0.9× | 1.1% | PASS |
| file_reads | `index_join_scan` | 3.60ms | 6.49ms | 1.8× | 1.8% | PASS |
| file_reads | `types_table_scan` | 1.01s | 1.30s | 1.3× | 0.6% | PASS |
| file_reads | `table_scan` | 1.17s | 1.42s | 1.2× | 1.3% | PASS |
| file_reads | `oltp_read_only` | 162.83ms | 144.72ms | 0.9× | 0.7% | PASS |
| file_writes | `oltp_bulk_insert` | 262.79ms | 335.10ms | 1.3× | 2.3% | PASS |
| file_writes | `oltp_insert` | 29.69ms | 53.74ms | 1.8× | 1.3% | PASS |
| file_writes | `oltp_update_index` | 162.84ms | 200.76ms | 1.2× | 1.3% | PASS |
| file_writes | `oltp_update_non_index` | 146.22ms | 122.97ms | 0.8× | 1.1% | PASS |
| file_writes | `oltp_delete_insert` | 161.85ms | 150.00ms | 0.9× | 0.9% | PASS |
| file_writes | `oltp_write_only` | 110.81ms | 102.96ms | 0.9× | 0.7% | PASS |
| file_writes | `types_delete_insert` | 117.75ms | 91.25ms | 0.8× | 4.2% | PASS |
| file_writes | `oltp_read_write` | 173.94ms | 168.70ms | 1.0× | 2.1% | PASS |
| ac_reads | `oltp_point_select` | 39.49ms | 39.48ms | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_range_select` | 14.36ms | 15.99ms | 1.1× | 0.9% | PASS |
| ac_reads | `oltp_sum_range` | 14.08ms | 14.62ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_order_range` | 3.04ms | 3.45ms | 1.1× | 0.8% | PASS |
| ac_reads | `oltp_distinct_range` | 3.95ms | 4.47ms | 1.1× | 0.8% | PASS |
| ac_reads | `oltp_index_scan` | 4.78ms | 6.52ms | 1.4× | 1.1% | PASS |
| ac_reads | `select_random_points` | 21.69ms | 21.38ms | 1.0× | 0.6% | PASS |
| ac_reads | `select_random_ranges` | 6.73ms | 6.58ms | 1.0× | 0.7% | PASS |
| ac_reads | `covering_index_scan` | 7.51ms | 10.40ms | 1.4× | 0.8% | PASS |
| ac_reads | `groupby_scan` | 31.60ms | 34.34ms | 1.1× | 0.4% | PASS |
| ac_reads | `index_join` | 11.36ms | 10.62ms | 0.9× | 0.7% | PASS |
| ac_reads | `index_join_scan` | 3.41ms | 6.39ms | 1.9× | 1.4% | PASS |
| ac_reads | `types_table_scan` | 987.44ms | 1.28s | 1.3× | 0.2% | PASS |
| ac_reads | `table_scan` | 1.21s | 1.43s | 1.2× | 1.6% | PASS |
| ac_reads | `oltp_read_only` | 127.03ms | 141.43ms | 1.1× | 0.6% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 29.44ms | 74.45ms | 2.5× | 5.9% | PASS |
| ac_writes | `oltp_insert_ac` | 31.48ms | 86.93ms | 2.8× | 3.6% | PASS |
| ac_writes | `oltp_update_index_ac` | 32.03ms | 97.70ms | 3.0× | 3.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 28.91ms | 82.02ms | 2.8× | 3.7% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 31.40ms | 91.18ms | 2.9× | 4.7% | PASS |
| ac_writes | `oltp_write_only_ac` | 31.34ms | 90.54ms | 2.9× | 3.0% | PASS |
| ac_writes | `types_delete_insert_ac` | 30.62ms | 85.91ms | 2.8× | 9.0% | PASS |
| ac_writes | `oltp_read_write_ac` | 35.68ms | 96.83ms | 2.7× | 5.6% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.06ms | 42.74ms | 1.3× | 1.5% | PASS |
| mem_reads | `oltp_range_select` | 19.02ms | 24.00ms | 1.3× | 0.9% | PASS |
| mem_reads | `oltp_sum_range` | 17.51ms | 22.68ms | 1.3× | 0.7% | PASS |
| mem_reads | `oltp_order_range` | 3.48ms | 4.14ms | 1.2× | 0.9% | PASS |
| mem_reads | `oltp_distinct_range` | 4.68ms | 5.37ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 4.50ms | 6.51ms | 1.4× | 1.9% | PASS |
| mem_reads | `select_random_points` | 27.41ms | 33.34ms | 1.2× | 1.2% | PASS |
| mem_reads | `select_random_ranges` | 7.54ms | 9.42ms | 1.2× | 1.3% | PASS |
| mem_reads | `covering_index_scan` | 7.62ms | 11.21ms | 1.5× | 0.8% | PASS |
| mem_reads | `groupby_scan` | 35.48ms | 42.14ms | 1.2× | 0.6% | PASS |
| mem_reads | `index_join` | 7.82ms | 11.64ms | 1.5× | 1.7% | PASS |
| mem_reads | `index_join_scan` | 3.90ms | 6.59ms | 1.7× | 1.5% | PASS |
| mem_reads | `types_table_scan` | 1.04s | 1.36s | 1.3× | 0.7% | PASS |
| mem_reads | `table_scan` | 1.21s | 1.48s | 1.2× | 1.5% | PASS |
| mem_reads | `oltp_read_only` | 151.44ms | 186.40ms | 1.2× | 1.2% | PASS |
| mem_writes | `oltp_bulk_insert` | 243.53ms | 329.96ms | 1.4× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 18.63ms | 32.84ms | 1.8× | 1.1% | PASS |
| mem_writes | `oltp_update_index` | 65.32ms | 118.41ms | 1.8× | 1.3% | PASS |
| mem_writes | `oltp_update_non_index` | 49.95ms | 81.66ms | 1.6× | 1.7% | PASS |
| mem_writes | `oltp_delete_insert` | 48.64ms | 90.41ms | 1.9× | 1.3% | PASS |
| mem_writes | `oltp_write_only` | 26.54ms | 53.11ms | 2.0× | 1.3% | PASS |
| mem_writes | `types_delete_insert` | 32.37ms | 54.09ms | 1.7× | 1.0% | PASS |
| mem_writes | `oltp_read_write` | 102.67ms | 159.60ms | 1.6× | 1.1% | PASS |
| file_reads | `oltp_point_select` | 103.60ms | 62.60ms | 0.6× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 26.72ms | 26.77ms | 1.0× | 1.9% | PASS |
| file_reads | `oltp_sum_range` | 25.29ms | 25.58ms | 1.0× | 1.2% | PASS |
| file_reads | `oltp_order_range` | 4.43ms | 4.52ms | 1.0× | 1.5% | PASS |
| file_reads | `oltp_distinct_range` | 5.61ms | 5.78ms | 1.0× | 1.5% | PASS |
| file_reads | `oltp_index_scan` | 11.67ms | 8.85ms | 0.8× | 1.3% | PASS |
| file_reads | `select_random_points` | 36.97ms | 37.20ms | 1.0× | 1.6% | PASS |
| file_reads | `select_random_ranges` | 15.24ms | 11.79ms | 0.8× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 15.02ms | 13.62ms | 0.9× | 0.6% | PASS |
| file_reads | `groupby_scan` | 36.58ms | 42.87ms | 1.2× | 1.0% | PASS |
| file_reads | `index_join` | 12.17ms | 13.70ms | 1.1× | 1.8% | PASS |
| file_reads | `index_join_scan` | 4.87ms | 7.15ms | 1.5× | 1.8% | PASS |
| file_reads | `types_table_scan` | 1.06s | 1.36s | 1.3× | 1.1% | PASS |
| file_reads | `table_scan` | 1.23s | 1.48s | 1.2× | 3.4% | PASS |
| file_reads | `oltp_read_only` | 260.09ms | 220.55ms | 0.8× | 0.6% | PASS |
| file_writes | `oltp_bulk_insert` | 260.12ms | 343.07ms | 1.3× | 1.1% | PASS |
| file_writes | `oltp_insert` | 25.93ms | 37.02ms | 1.4× | 1.7% | PASS |
| file_writes | `oltp_update_index` | 95.18ms | 133.90ms | 1.4× | 1.2% | PASS |
| file_writes | `oltp_update_non_index` | 76.18ms | 92.82ms | 1.2× | 1.5% | PASS |
| file_writes | `oltp_delete_insert` | 77.64ms | 105.63ms | 1.4× | 2.5% | PASS |
| file_writes | `oltp_write_only` | 51.79ms | 64.45ms | 1.2× | 2.4% | PASS |
| file_writes | `types_delete_insert` | 51.50ms | 63.35ms | 1.2× | 1.7% | PASS |
| file_writes | `oltp_read_write` | 129.88ms | 169.87ms | 1.3× | 1.4% | PASS |
| ac_reads | `oltp_point_select` | 56.34ms | 62.20ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 21.94ms | 26.66ms | 1.2× | 1.4% | PASS |
| ac_reads | `oltp_sum_range` | 20.34ms | 25.44ms | 1.3× | 1.2% | PASS |
| ac_reads | `oltp_order_range` | 4.01ms | 4.54ms | 1.1× | 2.1% | PASS |
| ac_reads | `oltp_distinct_range` | 5.18ms | 5.79ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_index_scan` | 7.37ms | 9.06ms | 1.2× | 1.5% | PASS |
| ac_reads | `select_random_points` | 32.32ms | 38.48ms | 1.2× | 1.8% | PASS |
| ac_reads | `select_random_ranges` | 10.41ms | 12.01ms | 1.2× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 10.66ms | 13.86ms | 1.3× | 1.0% | PASS |
| ac_reads | `groupby_scan` | 36.44ms | 43.13ms | 1.2× | 0.6% | PASS |
| ac_reads | `index_join` | 9.85ms | 13.97ms | 1.4× | 1.7% | PASS |
| ac_reads | `index_join_scan` | 4.49ms | 7.33ms | 1.6× | 1.3% | PASS |
| ac_reads | `types_table_scan` | 1.07s | 1.36s | 1.3× | 2.1% | PASS |
| ac_reads | `table_scan` | 1.21s | 1.48s | 1.2× | 1.4% | PASS |
| ac_reads | `oltp_read_only` | 186.84ms | 219.15ms | 1.2× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 24.19ms | 72.16ms | 3.0× | 10.9% | PASS |
| ac_writes | `oltp_insert_ac` | 26.68ms | 87.53ms | 3.3× | 7.2% | PASS |
| ac_writes | `oltp_update_index_ac` | 29.70ms | 98.52ms | 3.3× | 9.3% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.53ms | 81.75ms | 3.3× | 6.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.99ms | 89.29ms | 3.4× | 8.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.61ms | 89.73ms | 3.4× | 8.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 24.86ms | 84.45ms | 3.4× | 12.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 35.07ms | 98.80ms | 2.8× | 6.6% | PASS |

</details>

</details>

## Version-control latency

Wall time: 4m 23s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 34.37ms | 38.00ms | 90.4% | 0.8% | PASS |
| `status_dirty_many_tables` | 37.65ms | 42.00ms | 89.6% | 0.6% | PASS |
| `diff_regular_working_one_table` | 29.23ms | 33.00ms | 88.6% | 0.4% | PASS |
| `diff_regular_working_many_tables` | 42.68ms | 50.00ms | 85.4% | 0.5% | PASS |
| `diff_stat_working_many_tables` | 43.93ms | 48.00ms | 91.5% | 1.4% | PASS |
| `diff_schema_working_many_tables` | 43.22ms | 48.00ms | 90.0% | 0.5% | PASS |
| `branch_list_many_branches` | 21.74ms | 25.00ms | 86.9% | 0.8% | PASS |
| `branch_create_delete` | 23.22ms | 27.00ms | 86.0% | 0.6% | PASS |
| `at_literal_deep_history` | 23.66ms | 28.00ms | 84.5% | 1.0% | PASS |
| `diff_literal_deep_history` | 23.77ms | 28.00ms | 84.9% | 1.2% | PASS |
| `history_literal_deep_history` | 24.84ms | 30.00ms | 82.8% | 0.6% | PASS |
| `checkout_branch_clean` | 33.20ms | 43.00ms | 77.2% | 1.4% | PASS |
| `merge_data_no_conflicts` | 27.55ms | 33.00ms | 83.5% | 1.5% | PASS |
| `merge_data_secondary_index` | 842.27ms | 944.00ms | 89.2% | 0.9% | PASS |
| `merge_schema_no_conflicts` | 22.63ms | 24.00ms | 94.3% | 1.1% | PASS |
| `merge_data_conflicts` | 31.86ms | 33.00ms | 96.5% | 0.7% | PASS |
| `merge_data_conflicts_with_resolve` | 32.62ms | 34.00ms | 95.9% | 0.7% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
