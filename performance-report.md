# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-08-24 11:20 UTC
>
> Commit: [`6b7478d8b0610dac86f6979324a2fe7be191aa0a`](https://github.com/dolthub/doltlite/commit/6b7478d8b0610dac86f6979324a2fe7be191aa0a)
>
> Runner: ubuntu24 20260816.277.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/32713225684)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.81s | 10.46s | 1.1× | 1.8% | **PASS** |
| Writes | 2.04s | 3.19s | 1.6× | 1.2% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.80s | 10.77s | 1.0× | 1.6% | **PASS** |
| Writes | 3.88s | 4.01s | 1.0× | 1.7% | **PASS** |
| Autocommit writes | 2.17s | 6.35s | 2.9× | 6.1% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.66s | 2.78s | 1.0× | 1.6% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.92s | 2.97s | 1.0× | 1.5% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 1.70s | 1.77s | 1.0× | 2.2% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.52s | 2.95s | 1.2× | 1.5% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 435.75ms | 661.04ms | 1.5× | 0.7% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 622.61ms | 1.00s | 1.6× | 1.4% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 396.23ms | 616.29ms | 1.6× | 2.4% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 587.76ms | 912.23ms | 1.6× | 0.8% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.86s | 2.84s | 1.0× | 1.4% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 3.18s | 3.03s | 1.0× | 1.3% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.00s | 1.89s | 0.9× | 2.8% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.76s | 3.01s | 1.1× | 1.2% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 592.17ms | 727.84ms | 1.2× | 1.4% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 938.62ms | 1.08s | 1.2× | 4.3% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 1.58s | 1.19s | 0.8× | 34.4% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 773.56ms | 1.01s | 1.3× | 1.4% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.76s | 2.85s | 1.0× | 1.2% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.62s | 2.92s | 1.1× | 1.3% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 1.87s | 1.91s | 1.0× | 2.5% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.64s | 3.02s | 1.1× | 1.3% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 143.93ms | 565.15ms | 3.9× | 5.3% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 210.18ms | 731.46ms | 3.5× | 5.9% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 1.62s | 4.33s | 2.7× | 51.4% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 196.42ms | 722.44ms | 3.7× | 5.3% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.54ms | 26.80ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_range_select` | 10.79ms | 10.73ms | 1.0× | 1.9% | PASS |
| mem_reads | `oltp_sum_range` | 9.89ms | 10.31ms | 1.0× | 1.5% | PASS |
| mem_reads | `oltp_order_range` | 2.68ms | 2.77ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 3.78ms | 3.76ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 4.09ms | 4.97ms | 1.2× | 1.9% | PASS |
| mem_reads | `select_random_points` | 11.24ms | 11.08ms | 1.0× | 1.5% | PASS |
| mem_reads | `select_random_ranges` | 3.19ms | 3.93ms | 1.2× | 2.2% | PASS |
| mem_reads | `covering_index_scan` | 4.35ms | 4.22ms | 1.0× | 1.9% | PASS |
| mem_reads | `groupby_scan` | 32.08ms | 33.69ms | 1.1× | 0.5% | PASS |
| mem_reads | `index_join` | 5.96ms | 7.73ms | 1.3× | 1.9% | PASS |
| mem_reads | `index_join_scan` | 3.60ms | 4.66ms | 1.3× | 1.7% | PASS |
| mem_reads | `types_table_scan` | 1.12s | 1.21s | 1.1× | 0.9% | PASS |
| mem_reads | `table_scan` | 1.32s | 1.34s | 1.0× | 2.1% | PASS |
| mem_reads | `oltp_read_only` | 104.74ms | 108.61ms | 1.0× | 1.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 179.17ms | 235.15ms | 1.3× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 15.78ms | 27.18ms | 1.7× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 51.23ms | 89.58ms | 1.7× | 0.9% | PASS |
| mem_writes | `oltp_update_non_index` | 34.63ms | 52.12ms | 1.5× | 1.1% | PASS |
| mem_writes | `oltp_delete_insert` | 44.73ms | 71.27ms | 1.6× | 1.2% | PASS |
| mem_writes | `oltp_write_only` | 21.69ms | 46.08ms | 2.1× | 0.7% | PASS |
| mem_writes | `types_delete_insert` | 24.34ms | 35.54ms | 1.5× | 0.7% | PASS |
| mem_writes | `oltp_read_write` | 64.18ms | 104.11ms | 1.6× | 0.7% | PASS |
| file_reads | `oltp_point_select` | 107.26ms | 47.68ms | 0.4× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 19.16ms | 12.89ms | 0.7× | 3.1% | PASS |
| file_reads | `oltp_sum_range` | 18.43ms | 12.55ms | 0.7× | 1.4% | PASS |
| file_reads | `oltp_order_range` | 3.42ms | 3.03ms | 0.9× | 2.4% | PASS |
| file_reads | `oltp_distinct_range` | 4.59ms | 4.03ms | 0.9× | 1.5% | PASS |
| file_reads | `oltp_index_scan` | 12.29ms | 7.24ms | 0.6× | 1.6% | PASS |
| file_reads | `select_random_points` | 18.67ms | 13.13ms | 0.7× | 2.4% | PASS |
| file_reads | `select_random_ranges` | 11.25ms | 6.00ms | 0.5× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 13.07ms | 6.32ms | 0.5× | 1.7% | PASS |
| file_reads | `groupby_scan` | 33.09ms | 33.92ms | 1.0× | 0.7% | PASS |
| file_reads | `index_join` | 10.93ms | 9.73ms | 0.9× | 1.3% | PASS |
| file_reads | `index_join_scan` | 4.64ms | 5.18ms | 1.1× | 1.3% | PASS |
| file_reads | `types_table_scan` | 1.11s | 1.22s | 1.1× | 1.9% | PASS |
| file_reads | `table_scan` | 1.27s | 1.32s | 1.0× | 1.0% | PASS |
| file_reads | `oltp_read_only` | 222.19ms | 138.87ms | 0.6× | 0.7% | PASS |
| file_writes | `oltp_bulk_insert` | 193.44ms | 242.38ms | 1.3× | 1.1% | PASS |
| file_writes | `oltp_insert` | 21.81ms | 30.95ms | 1.4× | 1.5% | PASS |
| file_writes | `oltp_update_index` | 78.67ms | 99.88ms | 1.3× | 1.3% | PASS |
| file_writes | `oltp_update_non_index` | 58.14ms | 65.14ms | 1.1× | 1.4% | PASS |
| file_writes | `oltp_delete_insert` | 66.53ms | 80.13ms | 1.2× | 1.6% | PASS |
| file_writes | `oltp_write_only` | 42.87ms | 53.63ms | 1.3× | 1.8% | PASS |
| file_writes | `types_delete_insert` | 39.65ms | 41.66ms | 1.1× | 1.0% | PASS |
| file_writes | `oltp_read_write` | 91.06ms | 114.06ms | 1.3× | 2.3% | PASS |
| ac_reads | `oltp_point_select` | 52.84ms | 47.87ms | 0.9× | 1.2% | PASS |
| ac_reads | `oltp_range_select` | 14.55ms | 13.14ms | 0.9× | 1.2% | PASS |
| ac_reads | `oltp_sum_range` | 13.10ms | 12.67ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_order_range` | 3.11ms | 3.06ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_distinct_range` | 4.14ms | 4.03ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 7.15ms | 7.26ms | 1.0× | 1.1% | PASS |
| ac_reads | `select_random_points` | 14.07ms | 13.13ms | 0.9× | 1.3% | PASS |
| ac_reads | `select_random_ranges` | 6.03ms | 6.06ms | 1.0× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 7.50ms | 6.46ms | 0.9× | 0.9% | PASS |
| ac_reads | `groupby_scan` | 32.45ms | 33.94ms | 1.0× | 0.8% | PASS |
| ac_reads | `index_join` | 7.97ms | 9.40ms | 1.2× | 1.4% | PASS |
| ac_reads | `index_join_scan` | 4.02ms | 4.85ms | 1.2× | 1.4% | PASS |
| ac_reads | `types_table_scan` | 1.12s | 1.21s | 1.1× | 1.3% | PASS |
| ac_reads | `table_scan` | 1.32s | 1.34s | 1.0× | 2.4% | PASS |
| ac_reads | `oltp_read_only` | 145.86ms | 139.88ms | 1.0× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 15.08ms | 55.40ms | 3.7× | 3.0% | PASS |
| ac_writes | `oltp_insert_ac` | 17.65ms | 70.14ms | 4.0× | 5.3% | PASS |
| ac_writes | `oltp_update_index_ac` | 19.65ms | 82.45ms | 4.2× | 3.5% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 16.29ms | 63.45ms | 3.9× | 5.2% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 18.46ms | 74.58ms | 4.0× | 5.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 17.95ms | 73.13ms | 4.1× | 5.7% | PASS |
| ac_writes | `types_delete_insert_ac` | 15.40ms | 64.65ms | 4.2× | 7.1% | PASS |
| ac_writes | `oltp_read_write_ac` | 23.45ms | 81.35ms | 3.5× | 6.7% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 35.85ms | 36.32ms | 1.0× | 2.5% | PASS |
| mem_reads | `oltp_range_select` | 16.69ms | 13.21ms | 0.8× | 2.4% | PASS |
| mem_reads | `oltp_sum_range` | 15.65ms | 13.19ms | 0.8× | 1.8% | PASS |
| mem_reads | `oltp_order_range` | 3.25ms | 3.13ms | 1.0× | 1.4% | PASS |
| mem_reads | `oltp_distinct_range` | 4.35ms | 4.24ms | 1.0× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 4.15ms | 6.09ms | 1.5× | 1.4% | PASS |
| mem_reads | `select_random_points` | 23.23ms | 20.87ms | 0.9× | 2.0% | PASS |
| mem_reads | `select_random_ranges` | 3.66ms | 5.09ms | 1.4× | 1.5% | PASS |
| mem_reads | `covering_index_scan` | 4.33ms | 4.62ms | 1.1× | 1.6% | PASS |
| mem_reads | `groupby_scan` | 35.76ms | 34.11ms | 1.0× | 0.6% | PASS |
| mem_reads | `index_join` | 11.28ms | 9.46ms | 0.8× | 3.7% | PASS |
| mem_reads | `index_join_scan` | 4.00ms | 5.24ms | 1.3× | 2.2% | PASS |
| mem_reads | `types_table_scan` | 1.22s | 1.29s | 1.1× | 1.2% | PASS |
| mem_reads | `table_scan` | 1.40s | 1.39s | 1.0× | 0.6% | PASS |
| mem_reads | `oltp_read_only` | 148.05ms | 137.17ms | 0.9× | 1.2% | PASS |
| mem_writes | `oltp_bulk_insert` | 240.64ms | 364.64ms | 1.5× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 18.54ms | 39.29ms | 2.1× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 73.37ms | 144.89ms | 2.0× | 1.4% | PASS |
| mem_writes | `oltp_update_non_index` | 52.35ms | 81.35ms | 1.6× | 2.5% | PASS |
| mem_writes | `oltp_delete_insert` | 58.43ms | 108.17ms | 1.9× | 1.7% | PASS |
| mem_writes | `oltp_write_only` | 29.77ms | 62.41ms | 2.1× | 1.3% | PASS |
| mem_writes | `types_delete_insert` | 41.57ms | 55.22ms | 1.3× | 1.8% | PASS |
| mem_writes | `oltp_read_write` | 107.94ms | 146.08ms | 1.4× | 1.4% | PASS |
| file_reads | `oltp_point_select` | 110.88ms | 56.40ms | 0.5× | 1.3% | PASS |
| file_reads | `oltp_range_select` | 24.82ms | 15.38ms | 0.6× | 2.6% | PASS |
| file_reads | `oltp_sum_range` | 24.20ms | 15.51ms | 0.6× | 1.8% | PASS |
| file_reads | `oltp_order_range` | 4.19ms | 3.39ms | 0.8× | 1.0% | PASS |
| file_reads | `oltp_distinct_range` | 5.29ms | 4.53ms | 0.9× | 1.1% | PASS |
| file_reads | `oltp_index_scan` | 11.68ms | 8.15ms | 0.7× | 1.4% | PASS |
| file_reads | `select_random_points` | 32.51ms | 23.65ms | 0.7× | 1.7% | PASS |
| file_reads | `select_random_ranges` | 10.88ms | 7.05ms | 0.6× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 12.04ms | 6.63ms | 0.6× | 1.7% | PASS |
| file_reads | `groupby_scan` | 36.89ms | 34.68ms | 0.9× | 0.9% | PASS |
| file_reads | `index_join` | 16.50ms | 11.05ms | 0.7× | 3.8% | PASS |
| file_reads | `index_join_scan` | 5.15ms | 5.74ms | 1.1× | 2.2% | PASS |
| file_reads | `types_table_scan` | 1.23s | 1.28s | 1.0× | 1.3% | PASS |
| file_reads | `table_scan` | 1.40s | 1.39s | 1.0× | 0.4% | PASS |
| file_reads | `oltp_read_only` | 254.05ms | 164.82ms | 0.6× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 266.34ms | 374.56ms | 1.4× | 1.1% | PASS |
| file_writes | `oltp_insert` | 26.18ms | 45.55ms | 1.7× | 1.7% | PASS |
| file_writes | `oltp_update_index` | 138.03ms | 155.29ms | 1.1× | 7.1% | PASS |
| file_writes | `oltp_update_non_index` | 99.82ms | 94.93ms | 1.0× | 7.8% | PASS |
| file_writes | `oltp_delete_insert` | 98.41ms | 120.07ms | 1.2× | 1.4% | PASS |
| file_writes | `oltp_write_only` | 89.90ms | 72.72ms | 0.8× | 9.5% | PASS |
| file_writes | `types_delete_insert` | 72.48ms | 66.42ms | 0.9× | 1.7% | PASS |
| file_writes | `oltp_read_write` | 147.45ms | 153.59ms | 1.0× | 6.9% | PASS |
| ac_reads | `oltp_point_select` | 59.10ms | 55.35ms | 0.9× | 1.3% | PASS |
| ac_reads | `oltp_range_select` | 19.06ms | 15.25ms | 0.8× | 2.0% | PASS |
| ac_reads | `oltp_sum_range` | 18.60ms | 15.44ms | 0.8× | 2.0% | PASS |
| ac_reads | `oltp_order_range` | 3.71ms | 3.40ms | 0.9× | 1.4% | PASS |
| ac_reads | `oltp_distinct_range` | 4.77ms | 4.51ms | 0.9× | 0.9% | PASS |
| ac_reads | `oltp_index_scan` | 6.88ms | 8.23ms | 1.2× | 1.1% | PASS |
| ac_reads | `select_random_points` | 26.28ms | 23.65ms | 0.9× | 1.5% | PASS |
| ac_reads | `select_random_ranges` | 6.10ms | 7.03ms | 1.2× | 1.2% | PASS |
| ac_reads | `covering_index_scan` | 7.03ms | 6.56ms | 0.9× | 1.0% | PASS |
| ac_reads | `groupby_scan` | 35.60ms | 34.33ms | 1.0× | 0.8% | PASS |
| ac_reads | `index_join` | 13.15ms | 10.68ms | 0.8× | 2.6% | PASS |
| ac_reads | `index_join_scan` | 4.60ms | 5.72ms | 1.2× | 3.2% | PASS |
| ac_reads | `types_table_scan` | 1.06s | 1.24s | 1.2× | 0.6% | PASS |
| ac_reads | `table_scan` | 1.18s | 1.33s | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_read_only` | 175.74ms | 163.37ms | 0.9× | 2.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 23.68ms | 78.74ms | 3.3× | 4.4% | PASS |
| ac_writes | `oltp_insert_ac` | 27.11ms | 91.36ms | 3.4× | 5.3% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.84ms | 104.63ms | 3.8× | 5.4% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.61ms | 87.96ms | 3.7× | 6.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.92ms | 94.16ms | 3.5× | 6.5% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.34ms | 92.11ms | 3.6× | 7.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 23.85ms | 83.77ms | 3.5× | 7.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.83ms | 98.74ms | 3.1× | 4.9% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.20ms | 22.24ms | 0.9× | 3.2% | PASS |
| mem_reads | `oltp_range_select` | 10.31ms | 8.33ms | 0.8× | 2.2% | PASS |
| mem_reads | `oltp_sum_range` | 10.21ms | 8.29ms | 0.8× | 2.2% | PASS |
| mem_reads | `oltp_order_range` | 2.12ms | 1.92ms | 0.9× | 1.8% | PASS |
| mem_reads | `oltp_distinct_range` | 2.65ms | 2.40ms | 0.9× | 1.6% | PASS |
| mem_reads | `oltp_index_scan` | 2.62ms | 4.07ms | 1.6× | 2.1% | PASS |
| mem_reads | `select_random_points` | 16.12ms | 13.93ms | 0.9× | 3.7% | PASS |
| mem_reads | `select_random_ranges` | 2.35ms | 3.37ms | 1.4× | 2.1% | PASS |
| mem_reads | `covering_index_scan` | 2.46ms | 3.08ms | 1.3× | 3.9% | PASS |
| mem_reads | `groupby_scan` | 20.25ms | 19.33ms | 1.0× | 2.0% | PASS |
| mem_reads | `index_join` | 8.20ms | 6.19ms | 0.8× | 4.5% | PASS |
| mem_reads | `index_join_scan` | 2.90ms | 4.63ms | 1.6× | 3.7% | PASS |
| mem_reads | `types_table_scan` | 692.79ms | 741.26ms | 1.1× | 1.9% | PASS |
| mem_reads | `table_scan` | 816.82ms | 851.70ms | 1.0× | 2.3% | PASS |
| mem_reads | `oltp_read_only` | 86.86ms | 75.20ms | 0.9× | 2.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 146.06ms | 208.28ms | 1.4× | 1.2% | PASS |
| mem_writes | `oltp_insert` | 11.45ms | 23.45ms | 2.0× | 1.8% | PASS |
| mem_writes | `oltp_update_index` | 46.11ms | 91.75ms | 2.0× | 3.9% | PASS |
| mem_writes | `oltp_update_non_index` | 38.47ms | 54.11ms | 1.4× | 2.9% | PASS |
| mem_writes | `oltp_delete_insert` | 40.67ms | 73.53ms | 1.8× | 2.0% | PASS |
| mem_writes | `oltp_write_only` | 21.13ms | 43.15ms | 2.0× | 2.0% | PASS |
| mem_writes | `types_delete_insert` | 26.88ms | 34.51ms | 1.3× | 3.1% | PASS |
| mem_writes | `oltp_read_write` | 65.46ms | 87.51ms | 1.3× | 3.7% | PASS |
| file_reads | `oltp_point_select` | 79.80ms | 37.65ms | 0.5× | 2.8% | PASS |
| file_reads | `oltp_range_select` | 16.08ms | 10.03ms | 0.6× | 2.2% | PASS |
| file_reads | `oltp_sum_range` | 15.96ms | 9.89ms | 0.6× | 2.9% | PASS |
| file_reads | `oltp_order_range` | 2.85ms | 2.17ms | 0.8× | 2.8% | PASS |
| file_reads | `oltp_distinct_range` | 3.49ms | 2.76ms | 0.8× | 2.7% | PASS |
| file_reads | `oltp_index_scan` | 8.32ms | 5.67ms | 0.7× | 2.6% | PASS |
| file_reads | `select_random_points` | 20.92ms | 14.75ms | 0.7× | 4.5% | PASS |
| file_reads | `select_random_ranges` | 8.01ms | 5.00ms | 0.6× | 2.1% | PASS |
| file_reads | `covering_index_scan` | 8.60ms | 4.73ms | 0.6× | 3.3% | PASS |
| file_reads | `groupby_scan` | 21.98ms | 20.49ms | 0.9× | 3.7% | PASS |
| file_reads | `index_join` | 12.02ms | 7.77ms | 0.6× | 4.6% | PASS |
| file_reads | `index_join_scan` | 3.57ms | 4.52ms | 1.3× | 3.7% | PASS |
| file_reads | `types_table_scan` | 774.09ms | 805.58ms | 1.0× | 2.3% | PASS |
| file_reads | `table_scan` | 852.17ms | 860.64ms | 1.0× | 2.8% | PASS |
| file_reads | `oltp_read_only` | 176.13ms | 101.90ms | 0.6× | 2.7% | PASS |
| file_writes | `oltp_bulk_insert` | 302.44ms | 341.31ms | 1.1× | 21.1% | PASS |
| file_writes | `oltp_insert` | 44.87ms | 60.46ms | 1.3× | 70.5% | PASS |
| file_writes | `oltp_update_index` | 214.19ms | 182.07ms | 0.9× | 34.4% | PASS |
| file_writes | `oltp_update_non_index` | 215.13ms | 116.97ms | 0.5× | 31.9% | PASS |
| file_writes | `oltp_delete_insert` | 209.19ms | 150.58ms | 0.7× | 35.1% | PASS |
| file_writes | `oltp_write_only` | 215.31ms | 102.84ms | 0.5× | 34.4% | PASS |
| file_writes | `types_delete_insert` | 163.41ms | 86.99ms | 0.5× | 39.5% | PASS |
| file_writes | `oltp_read_write` | 214.78ms | 153.01ms | 0.7× | 26.5% | PASS |
| ac_reads | `oltp_point_select` | 39.53ms | 35.65ms | 0.9× | 2.5% | PASS |
| ac_reads | `oltp_range_select` | 11.74ms | 9.67ms | 0.8× | 2.5% | PASS |
| ac_reads | `oltp_sum_range` | 11.69ms | 9.48ms | 0.8× | 3.4% | PASS |
| ac_reads | `oltp_order_range` | 2.41ms | 2.10ms | 0.9× | 2.7% | PASS |
| ac_reads | `oltp_distinct_range` | 2.88ms | 2.56ms | 0.9× | 2.2% | PASS |
| ac_reads | `oltp_index_scan` | 4.58ms | 5.42ms | 1.2× | 2.3% | PASS |
| ac_reads | `select_random_points` | 17.09ms | 14.65ms | 0.9× | 3.0% | PASS |
| ac_reads | `select_random_ranges` | 4.25ms | 4.90ms | 1.2× | 2.0% | PASS |
| ac_reads | `covering_index_scan` | 4.47ms | 4.52ms | 1.0× | 3.9% | PASS |
| ac_reads | `groupby_scan` | 20.81ms | 19.68ms | 0.9× | 2.2% | PASS |
| ac_reads | `index_join` | 9.61ms | 7.37ms | 0.8× | 2.5% | PASS |
| ac_reads | `index_join_scan` | 3.14ms | 4.34ms | 1.4× | 2.1% | PASS |
| ac_reads | `types_table_scan` | 742.50ms | 781.71ms | 1.1× | 2.1% | PASS |
| ac_reads | `table_scan` | 874.87ms | 908.51ms | 1.0× | 3.3% | PASS |
| ac_reads | `oltp_read_only` | 118.71ms | 100.33ms | 0.8× | 3.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 218.09ms | 781.67ms | 3.6× | 67.4% | PASS |
| ac_writes | `oltp_insert_ac` | 121.26ms | 283.01ms | 2.3× | 43.3% | PASS |
| ac_writes | `oltp_update_index_ac` | 48.39ms | 135.84ms | 2.8× | 16.9% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 181.26ms | 484.87ms | 2.7× | 50.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 305.31ms | 742.25ms | 2.4× | 64.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 226.96ms | 701.50ms | 3.1× | 62.4% | PASS |
| ac_writes | `types_delete_insert_ac` | 196.93ms | 457.07ms | 2.3× | 51.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 318.81ms | 745.75ms | 2.3× | 47.1% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.41ms | 39.07ms | 1.2× | 1.8% | PASS |
| mem_reads | `oltp_range_select` | 19.22ms | 20.45ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_sum_range` | 18.01ms | 19.75ms | 1.1× | 1.7% | PASS |
| mem_reads | `oltp_order_range` | 3.39ms | 3.79ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 4.60ms | 4.92ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 4.55ms | 5.78ms | 1.3× | 2.5% | PASS |
| mem_reads | `select_random_points` | 28.24ms | 31.56ms | 1.1× | 2.0% | PASS |
| mem_reads | `select_random_ranges` | 7.56ms | 8.61ms | 1.1× | 2.3% | PASS |
| mem_reads | `covering_index_scan` | 4.25ms | 4.10ms | 1.0× | 1.6% | PASS |
| mem_reads | `groupby_scan` | 37.62ms | 38.92ms | 1.0× | 0.8% | PASS |
| mem_reads | `index_join` | 8.13ms | 10.04ms | 1.2× | 1.2% | PASS |
| mem_reads | `index_join_scan` | 3.85ms | 5.24ms | 1.4× | 1.7% | PASS |
| mem_reads | `types_table_scan` | 1.03s | 1.25s | 1.2× | 0.7% | PASS |
| mem_reads | `table_scan` | 1.16s | 1.34s | 1.2× | 0.5% | PASS |
| mem_reads | `oltp_read_only` | 148.45ms | 165.44ms | 1.1× | 1.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 247.59ms | 338.54ms | 1.4× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 19.13ms | 34.02ms | 1.8× | 0.6% | PASS |
| mem_writes | `oltp_update_index` | 66.29ms | 114.79ms | 1.7× | 1.6% | PASS |
| mem_writes | `oltp_update_non_index` | 49.02ms | 73.78ms | 1.5× | 0.8% | PASS |
| mem_writes | `oltp_delete_insert` | 48.31ms | 92.15ms | 1.9× | 0.7% | PASS |
| mem_writes | `oltp_write_only` | 26.15ms | 54.69ms | 2.1× | 0.7% | PASS |
| mem_writes | `types_delete_insert` | 32.67ms | 51.15ms | 1.6× | 1.0% | PASS |
| mem_writes | `oltp_read_write` | 98.60ms | 153.11ms | 1.6× | 1.0% | PASS |
| file_reads | `oltp_point_select` | 104.49ms | 58.28ms | 0.6× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 28.14ms | 23.14ms | 0.8× | 1.6% | PASS |
| file_reads | `oltp_sum_range` | 27.05ms | 22.66ms | 0.8× | 1.1% | PASS |
| file_reads | `oltp_order_range` | 4.49ms | 4.13ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_distinct_range` | 5.58ms | 5.23ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 12.07ms | 7.89ms | 0.7× | 1.2% | PASS |
| file_reads | `select_random_points` | 37.65ms | 34.96ms | 0.9× | 1.8% | PASS |
| file_reads | `select_random_ranges` | 15.30ms | 10.80ms | 0.7× | 0.9% | PASS |
| file_reads | `covering_index_scan` | 11.64ms | 6.08ms | 0.5× | 1.4% | PASS |
| file_reads | `groupby_scan` | 38.61ms | 39.27ms | 1.0× | 0.7% | PASS |
| file_reads | `index_join` | 12.59ms | 11.83ms | 0.9× | 2.1% | PASS |
| file_reads | `index_join_scan` | 5.25ms | 5.74ms | 1.1× | 1.9% | PASS |
| file_reads | `types_table_scan` | 1.03s | 1.24s | 1.2× | 0.3% | PASS |
| file_reads | `table_scan` | 1.17s | 1.34s | 1.1× | 0.5% | PASS |
| file_reads | `oltp_read_only` | 257.83ms | 194.88ms | 0.8× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 262.07ms | 350.79ms | 1.3× | 0.8% | PASS |
| file_writes | `oltp_insert` | 26.85ms | 40.13ms | 1.5× | 1.4% | PASS |
| file_writes | `oltp_update_index` | 98.94ms | 131.05ms | 1.3× | 1.4% | PASS |
| file_writes | `oltp_update_non_index` | 76.50ms | 88.18ms | 1.2× | 1.2% | PASS |
| file_writes | `oltp_delete_insert` | 77.65ms | 106.47ms | 1.4× | 1.5% | PASS |
| file_writes | `oltp_write_only` | 51.80ms | 66.27ms | 1.3× | 1.4% | PASS |
| file_writes | `types_delete_insert` | 50.11ms | 58.36ms | 1.2× | 1.7% | PASS |
| file_writes | `oltp_read_write` | 129.64ms | 165.60ms | 1.3× | 1.1% | PASS |
| ac_reads | `oltp_point_select` | 56.80ms | 58.41ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 22.25ms | 23.01ms | 1.0× | 1.8% | PASS |
| ac_reads | `oltp_sum_range` | 20.91ms | 22.29ms | 1.1× | 0.9% | PASS |
| ac_reads | `oltp_order_range` | 3.98ms | 4.18ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_distinct_range` | 5.01ms | 5.29ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_index_scan` | 7.17ms | 8.18ms | 1.1× | 1.2% | PASS |
| ac_reads | `select_random_points` | 31.75ms | 35.30ms | 1.1× | 1.9% | PASS |
| ac_reads | `select_random_ranges` | 10.31ms | 10.93ms | 1.1× | 1.7% | PASS |
| ac_reads | `covering_index_scan` | 6.81ms | 6.36ms | 0.9× | 1.6% | PASS |
| ac_reads | `groupby_scan` | 37.82ms | 39.43ms | 1.0× | 0.7% | PASS |
| ac_reads | `index_join` | 10.15ms | 12.50ms | 1.2× | 2.4% | PASS |
| ac_reads | `index_join_scan` | 4.71ms | 5.79ms | 1.2× | 2.1% | PASS |
| ac_reads | `types_table_scan` | 1.04s | 1.25s | 1.2× | 0.5% | PASS |
| ac_reads | `table_scan` | 1.20s | 1.35s | 1.1× | 0.5% | PASS |
| ac_reads | `oltp_read_only` | 184.31ms | 195.41ms | 1.1× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.84ms | 73.48ms | 3.4× | 3.1% | PASS |
| ac_writes | `oltp_insert_ac` | 23.87ms | 90.17ms | 3.8× | 4.7% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.78ms | 101.18ms | 3.8× | 5.5% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.61ms | 84.54ms | 3.7× | 5.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.16ms | 93.38ms | 3.9× | 9.0% | PASS |
| ac_writes | `oltp_write_only_ac` | 24.52ms | 93.74ms | 3.8× | 5.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 21.94ms | 87.48ms | 4.0× | 7.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 30.70ms | 98.47ms | 3.2× | 4.6% | PASS |

</details>

</details>

## Version-control latency

Wall time: 2m 20s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 83.12ms | 130.00ms | 63.9% | 0.8% | PASS |
| `status_dirty_many_tables` | 86.23ms | 130.00ms | 66.3% | 0.7% | PASS |
| `diff_regular_working_one_table` | 78.05ms | 120.00ms | 65.0% | 0.8% | PASS |
| `diff_regular_working_many_tables` | 91.38ms | 140.00ms | 65.3% | 0.8% | PASS |
| `diff_stat_working_many_tables` | 91.52ms | 140.00ms | 65.4% | 0.8% | PASS |
| `diff_schema_working_many_tables` | 91.86ms | 140.00ms | 65.6% | 0.7% | PASS |
| `branch_list_many_branches` | 22.33ms | 35.00ms | 63.8% | 1.7% | PASS |
| `branch_create_delete` | 25.21ms | 40.00ms | 63.0% | 1.6% | PASS |
| `checkout_branch_clean` | 55.75ms | 150.00ms | 37.2% | 1.2% | PASS |
| `merge_data_no_conflicts` | 29.20ms | 50.00ms | 58.4% | 2.1% | PASS |
| `merge_schema_no_conflicts` | 22.53ms | 35.00ms | 64.4% | 2.0% | PASS |
| `merge_data_conflicts` | 127.88ms | 180.00ms | 71.0% | 0.3% | PASS |
| `merge_data_conflicts_with_resolve` | 127.95ms | 180.00ms | 71.1% | 0.3% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
