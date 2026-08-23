# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-08-23 11:10 UTC
>
> Commit: [`0841e0ca9c6e5aa6fdd0273bbefed26e3e1eb9cd`](https://github.com/dolthub/doltlite/commit/0841e0ca9c6e5aa6fdd0273bbefed26e3e1eb9cd)
>
> Runner: ubuntu24 20260816.277.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/32631596323)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.94s | 10.53s | 1.1× | 1.4% | **PASS** |
| Writes | 2.12s | 3.29s | 1.6× | 1.1% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.94s | 10.78s | 1.0× | 1.2% | **PASS** |
| Writes | 3.41s | 3.97s | 1.2× | 1.8% | **PASS** |
| Autocommit writes | 894.09ms | 2.97s | 3.3× | 6.4% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.40s | 2.62s | 1.1× | 1.2% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.53s | 2.33s | 0.9× | 2.1% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.47s | 2.73s | 1.1× | 1.3% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.54s | 2.85s | 1.1× | 1.3% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 441.89ms | 676.65ms | 1.5× | 1.2% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 505.08ms | 754.40ms | 1.5× | 1.8% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 576.21ms | 934.67ms | 1.6× | 0.9% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 593.45ms | 921.71ms | 1.6× | 1.1% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.66s | 2.68s | 1.0× | 1.5% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.81s | 2.40s | 0.9× | 1.0% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.71s | 2.79s | 1.0× | 1.4% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.77s | 2.90s | 1.1× | 1.6% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 585.88ms | 732.13ms | 1.2× | 1.8% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.26s | 1.20s | 1.0× | 5.4% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 797.60ms | 1.03s | 1.3× | 1.5% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 762.46ms | 1.01s | 1.3× | 1.8% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.54s | 2.68s | 1.1× | 1.7% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.65s | 2.41s | 0.9× | 1.0% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.56s | 2.79s | 1.1× | 1.2% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.62s | 2.91s | 1.1× | 1.1% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 192.56ms | 711.28ms | 3.7× | 5.4% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 280.48ms | 757.94ms | 2.7× | 17.0% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 201.51ms | 711.57ms | 3.5× | 4.0% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 219.54ms | 787.77ms | 3.6× | 6.8% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 23.01ms | 27.69ms | 1.2× | 1.3% | PASS |
| mem_reads | `oltp_range_select` | 9.62ms | 10.77ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_sum_range` | 9.09ms | 10.98ms | 1.2× | 1.2% | PASS |
| mem_reads | `oltp_order_range` | 2.47ms | 2.72ms | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_distinct_range` | 3.50ms | 3.78ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 3.74ms | 4.88ms | 1.3× | 1.4% | PASS |
| mem_reads | `select_random_points` | 9.29ms | 10.33ms | 1.1× | 2.1% | PASS |
| mem_reads | `select_random_ranges` | 2.77ms | 3.75ms | 1.4× | 1.7% | PASS |
| mem_reads | `covering_index_scan` | 4.19ms | 3.93ms | 0.9× | 0.9% | PASS |
| mem_reads | `groupby_scan` | 30.10ms | 31.74ms | 1.1× | 0.7% | PASS |
| mem_reads | `index_join` | 5.97ms | 7.57ms | 1.3× | 1.1% | PASS |
| mem_reads | `index_join_scan` | 3.42ms | 4.29ms | 1.3× | 1.2% | PASS |
| mem_reads | `types_table_scan` | 1.03s | 1.14s | 1.1× | 0.9% | PASS |
| mem_reads | `table_scan` | 1.16s | 1.24s | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_read_only` | 102.60ms | 114.29ms | 1.1× | 1.2% | PASS |
| mem_writes | `oltp_bulk_insert` | 177.92ms | 239.67ms | 1.3× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 15.23ms | 26.58ms | 1.7× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 50.39ms | 89.77ms | 1.8× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 33.84ms | 52.54ms | 1.6× | 1.3% | PASS |
| mem_writes | `oltp_delete_insert` | 45.45ms | 72.10ms | 1.6× | 1.7% | PASS |
| mem_writes | `oltp_write_only` | 22.21ms | 45.93ms | 2.1× | 1.1% | PASS |
| mem_writes | `types_delete_insert` | 25.14ms | 36.70ms | 1.5× | 1.4% | PASS |
| mem_writes | `oltp_read_write` | 71.72ms | 113.37ms | 1.6× | 1.3% | PASS |
| file_reads | `oltp_point_select` | 94.33ms | 47.16ms | 0.5× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 18.38ms | 13.16ms | 0.7× | 1.2% | PASS |
| file_reads | `oltp_sum_range` | 17.51ms | 13.43ms | 0.8× | 1.7% | PASS |
| file_reads | `oltp_order_range` | 3.63ms | 3.24ms | 0.9× | 2.6% | PASS |
| file_reads | `oltp_distinct_range` | 4.54ms | 4.29ms | 0.9× | 2.1% | PASS |
| file_reads | `oltp_index_scan` | 11.30ms | 7.28ms | 0.6× | 1.7% | PASS |
| file_reads | `select_random_points` | 17.58ms | 12.48ms | 0.7× | 1.5% | PASS |
| file_reads | `select_random_ranges` | 9.96ms | 5.72ms | 0.6× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 11.56ms | 6.24ms | 0.5× | 1.5% | PASS |
| file_reads | `groupby_scan` | 31.75ms | 32.50ms | 1.0× | 0.8% | PASS |
| file_reads | `index_join` | 10.07ms | 9.37ms | 0.9× | 1.8% | PASS |
| file_reads | `index_join_scan` | 4.49ms | 4.76ms | 1.1× | 2.1% | PASS |
| file_reads | `types_table_scan` | 1.04s | 1.14s | 1.1× | 0.9% | PASS |
| file_reads | `table_scan` | 1.18s | 1.24s | 1.1× | 1.1% | PASS |
| file_reads | `oltp_read_only` | 203.31ms | 140.71ms | 0.7× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 190.50ms | 247.35ms | 1.3× | 1.4% | PASS |
| file_writes | `oltp_insert` | 21.84ms | 30.40ms | 1.4× | 2.1% | PASS |
| file_writes | `oltp_update_index` | 76.13ms | 98.44ms | 1.3× | 1.8% | PASS |
| file_writes | `oltp_update_non_index` | 56.15ms | 64.24ms | 1.1× | 1.2% | PASS |
| file_writes | `oltp_delete_insert` | 67.21ms | 80.06ms | 1.2× | 1.7% | PASS |
| file_writes | `oltp_write_only` | 44.04ms | 52.54ms | 1.2× | 2.2% | PASS |
| file_writes | `types_delete_insert` | 39.16ms | 41.78ms | 1.1× | 2.1% | PASS |
| file_writes | `oltp_read_write` | 90.86ms | 117.32ms | 1.3× | 1.5% | PASS |
| ac_reads | `oltp_point_select` | 46.60ms | 46.09ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_range_select` | 12.94ms | 12.96ms | 1.0× | 2.2% | PASS |
| ac_reads | `oltp_sum_range` | 12.17ms | 13.16ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_order_range` | 3.04ms | 3.16ms | 1.0× | 3.1% | PASS |
| ac_reads | `oltp_distinct_range` | 4.00ms | 4.23ms | 1.1× | 2.4% | PASS |
| ac_reads | `oltp_index_scan` | 6.46ms | 7.24ms | 1.1× | 2.0% | PASS |
| ac_reads | `select_random_points` | 13.27ms | 12.69ms | 1.0× | 2.3% | PASS |
| ac_reads | `select_random_ranges` | 5.38ms | 5.74ms | 1.1× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 6.97ms | 6.21ms | 0.9× | 1.7% | PASS |
| ac_reads | `groupby_scan` | 30.82ms | 32.49ms | 1.1× | 1.0% | PASS |
| ac_reads | `index_join` | 7.52ms | 9.14ms | 1.2× | 1.7% | PASS |
| ac_reads | `index_join_scan` | 3.94ms | 4.84ms | 1.2× | 2.9% | PASS |
| ac_reads | `types_table_scan` | 1.04s | 1.13s | 1.1× | 0.7% | PASS |
| ac_reads | `table_scan` | 1.21s | 1.25s | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_read_only` | 136.45ms | 141.44ms | 1.0× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.57ms | 73.77ms | 3.3× | 5.1% | PASS |
| ac_writes | `oltp_insert_ac` | 24.03ms | 88.51ms | 3.7× | 6.8% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.05ms | 100.02ms | 3.8× | 5.9% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.38ms | 81.88ms | 3.8× | 4.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 23.89ms | 93.42ms | 3.9× | 5.7% | PASS |
| ac_writes | `oltp_write_only_ac` | 24.37ms | 92.52ms | 3.8× | 8.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 20.91ms | 83.59ms | 4.0× | 4.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 29.36ms | 97.57ms | 3.3× | 2.9% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 30.73ms | 27.78ms | 0.9× | 2.5% | PASS |
| mem_reads | `oltp_range_select` | 15.25ms | 10.90ms | 0.7× | 4.1% | PASS |
| mem_reads | `oltp_sum_range` | 14.33ms | 10.64ms | 0.7× | 4.0% | PASS |
| mem_reads | `oltp_order_range` | 2.93ms | 2.53ms | 0.9× | 1.5% | PASS |
| mem_reads | `oltp_distinct_range` | 3.75ms | 3.29ms | 0.9× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 3.47ms | 4.92ms | 1.4× | 2.0% | PASS |
| mem_reads | `select_random_points` | 20.86ms | 16.89ms | 0.8× | 5.7% | PASS |
| mem_reads | `select_random_ranges` | 3.11ms | 4.25ms | 1.4× | 2.1% | PASS |
| mem_reads | `covering_index_scan` | 3.49ms | 3.82ms | 1.1× | 1.4% | PASS |
| mem_reads | `groupby_scan` | 29.92ms | 28.29ms | 0.9× | 1.0% | PASS |
| mem_reads | `index_join` | 10.56ms | 7.68ms | 0.7× | 2.9% | PASS |
| mem_reads | `index_join_scan` | 3.92ms | 4.91ms | 1.3× | 3.2% | PASS |
| mem_reads | `types_table_scan` | 1.04s | 987.32ms | 1.0× | 3.2% | PASS |
| mem_reads | `table_scan` | 1.22s | 1.11s | 0.9× | 0.9% | PASS |
| mem_reads | `oltp_read_only` | 128.72ms | 103.76ms | 0.8× | 1.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 185.16ms | 267.12ms | 1.4× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 14.55ms | 29.00ms | 2.0× | 1.2% | PASS |
| mem_writes | `oltp_update_index` | 59.55ms | 111.52ms | 1.9× | 2.7% | PASS |
| mem_writes | `oltp_update_non_index` | 44.50ms | 61.70ms | 1.4× | 2.8% | PASS |
| mem_writes | `oltp_delete_insert` | 48.14ms | 84.28ms | 1.8× | 1.7% | PASS |
| mem_writes | `oltp_write_only` | 25.73ms | 48.52ms | 1.9× | 1.3% | PASS |
| mem_writes | `types_delete_insert` | 35.58ms | 40.19ms | 1.1× | 1.9% | PASS |
| mem_writes | `oltp_read_write` | 91.89ms | 112.07ms | 1.2× | 3.6% | PASS |
| file_reads | `oltp_point_select` | 98.37ms | 44.84ms | 0.5× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 22.77ms | 12.70ms | 0.6× | 1.1% | PASS |
| file_reads | `oltp_sum_range` | 21.77ms | 12.61ms | 0.6× | 0.7% | PASS |
| file_reads | `oltp_order_range` | 3.70ms | 2.74ms | 0.7× | 0.6% | PASS |
| file_reads | `oltp_distinct_range` | 4.55ms | 3.53ms | 0.8× | 0.8% | PASS |
| file_reads | `oltp_index_scan` | 10.28ms | 6.78ms | 0.7× | 0.5% | PASS |
| file_reads | `select_random_points` | 27.97ms | 18.79ms | 0.7× | 1.6% | PASS |
| file_reads | `select_random_ranges` | 9.63ms | 5.92ms | 0.6× | 0.7% | PASS |
| file_reads | `covering_index_scan` | 10.42ms | 5.64ms | 0.5× | 0.6% | PASS |
| file_reads | `groupby_scan` | 30.49ms | 28.62ms | 0.9× | 0.7% | PASS |
| file_reads | `index_join` | 14.00ms | 8.92ms | 0.6× | 1.2% | PASS |
| file_reads | `index_join_scan` | 4.31ms | 5.14ms | 1.2× | 1.0% | PASS |
| file_reads | `types_table_scan` | 1.07s | 996.55ms | 0.9× | 1.5% | PASS |
| file_reads | `table_scan` | 1.25s | 1.12s | 0.9× | 1.2% | PASS |
| file_reads | `oltp_read_only` | 222.18ms | 129.08ms | 0.6× | 1.3% | PASS |
| file_writes | `oltp_bulk_insert` | 268.00ms | 339.10ms | 1.3× | 3.3% | PASS |
| file_writes | `oltp_insert` | 27.85ms | 53.69ms | 1.9× | 10.5% | PASS |
| file_writes | `oltp_update_index` | 186.84ms | 190.98ms | 1.0× | 9.6% | PASS |
| file_writes | `oltp_update_non_index` | 144.01ms | 119.60ms | 0.8× | 4.5% | PASS |
| file_writes | `oltp_delete_insert` | 181.79ms | 149.51ms | 0.8× | 1.7% | PASS |
| file_writes | `oltp_write_only` | 120.25ms | 99.23ms | 0.8× | 5.9% | PASS |
| file_writes | `types_delete_insert` | 151.46ms | 81.55ms | 0.5× | 9.5% | PASS |
| file_writes | `oltp_read_write` | 181.37ms | 168.27ms | 0.9× | 4.9% | PASS |
| ac_reads | `oltp_point_select` | 53.62ms | 44.85ms | 0.8× | 1.4% | PASS |
| ac_reads | `oltp_range_select` | 18.27ms | 12.71ms | 0.7× | 1.1% | PASS |
| ac_reads | `oltp_sum_range` | 17.30ms | 12.61ms | 0.7× | 1.2% | PASS |
| ac_reads | `oltp_order_range` | 3.30ms | 2.75ms | 0.8× | 0.9% | PASS |
| ac_reads | `oltp_distinct_range` | 4.13ms | 3.53ms | 0.9× | 1.0% | PASS |
| ac_reads | `oltp_index_scan` | 6.00ms | 6.77ms | 1.1× | 0.7% | PASS |
| ac_reads | `select_random_points` | 23.24ms | 18.75ms | 0.8× | 1.9% | PASS |
| ac_reads | `select_random_ranges` | 5.35ms | 5.93ms | 1.1× | 0.8% | PASS |
| ac_reads | `covering_index_scan` | 6.05ms | 5.65ms | 0.9× | 0.7% | PASS |
| ac_reads | `groupby_scan` | 30.05ms | 28.61ms | 1.0× | 0.7% | PASS |
| ac_reads | `index_join` | 11.90ms | 8.94ms | 0.8× | 1.1% | PASS |
| ac_reads | `index_join_scan` | 3.95ms | 5.16ms | 1.3× | 0.9% | PASS |
| ac_reads | `types_table_scan` | 1.08s | 997.18ms | 0.9× | 1.0% | PASS |
| ac_reads | `table_scan` | 1.24s | 1.12s | 0.9× | 1.1% | PASS |
| ac_reads | `oltp_read_only` | 157.86ms | 129.00ms | 0.8× | 1.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 33.29ms | 83.15ms | 2.5× | 19.9% | PASS |
| ac_writes | `oltp_insert_ac` | 32.84ms | 90.30ms | 2.7× | 14.6% | PASS |
| ac_writes | `oltp_update_index_ac` | 33.95ms | 104.19ms | 3.1× | 14.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 29.84ms | 90.37ms | 3.0× | 16.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 42.04ms | 106.27ms | 2.5× | 33.2% | PASS |
| ac_writes | `oltp_write_only_ac` | 35.26ms | 90.89ms | 2.6× | 21.0% | PASS |
| ac_writes | `types_delete_insert_ac` | 31.21ms | 90.16ms | 2.9× | 7.2% | PASS |
| ac_writes | `oltp_read_write_ac` | 42.03ms | 102.61ms | 2.4× | 17.1% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.19ms | 35.91ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_range_select` | 15.41ms | 13.23ms | 0.9× | 1.5% | PASS |
| mem_reads | `oltp_sum_range` | 14.22ms | 13.07ms | 0.9× | 1.5% | PASS |
| mem_reads | `oltp_order_range` | 3.15ms | 3.08ms | 1.0× | 1.3% | PASS |
| mem_reads | `oltp_distinct_range` | 4.20ms | 4.14ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 3.88ms | 5.71ms | 1.5× | 1.2% | PASS |
| mem_reads | `select_random_points` | 19.82ms | 19.79ms | 1.0× | 1.7% | PASS |
| mem_reads | `select_random_ranges` | 3.29ms | 5.00ms | 1.5× | 1.1% | PASS |
| mem_reads | `covering_index_scan` | 4.24ms | 4.23ms | 1.0× | 1.3% | PASS |
| mem_reads | `groupby_scan` | 34.04ms | 33.64ms | 1.0× | 1.0% | PASS |
| mem_reads | `index_join` | 9.76ms | 8.69ms | 0.9× | 1.4% | PASS |
| mem_reads | `index_join_scan` | 3.71ms | 5.05ms | 1.4× | 1.4% | PASS |
| mem_reads | `types_table_scan` | 1.03s | 1.17s | 1.1× | 0.5% | PASS |
| mem_reads | `table_scan` | 1.16s | 1.27s | 1.1× | 0.5% | PASS |
| mem_reads | `oltp_read_only` | 132.60ms | 132.48ms | 1.0× | 0.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 243.38ms | 352.88ms | 1.4× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 18.68ms | 37.37ms | 2.0× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 61.96ms | 125.73ms | 2.0× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 45.28ms | 73.80ms | 1.6× | 0.9% | PASS |
| mem_writes | `oltp_delete_insert` | 50.80ms | 98.40ms | 1.9× | 0.8% | PASS |
| mem_writes | `oltp_write_only` | 26.55ms | 57.68ms | 2.2× | 1.1% | PASS |
| mem_writes | `types_delete_insert` | 36.43ms | 50.50ms | 1.4× | 1.0% | PASS |
| mem_writes | `oltp_read_write` | 93.12ms | 138.32ms | 1.5× | 0.9% | PASS |
| file_reads | `oltp_point_select` | 103.72ms | 54.98ms | 0.5× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 23.27ms | 15.39ms | 0.7× | 2.9% | PASS |
| file_reads | `oltp_sum_range` | 21.95ms | 15.46ms | 0.7× | 1.4% | PASS |
| file_reads | `oltp_order_range` | 4.21ms | 3.38ms | 0.8× | 2.5% | PASS |
| file_reads | `oltp_distinct_range` | 5.33ms | 4.47ms | 0.8× | 2.0% | PASS |
| file_reads | `oltp_index_scan` | 11.32ms | 8.03ms | 0.7× | 1.6% | PASS |
| file_reads | `select_random_points` | 28.67ms | 23.06ms | 0.8× | 2.1% | PASS |
| file_reads | `select_random_ranges` | 10.43ms | 7.00ms | 0.7× | 1.3% | PASS |
| file_reads | `covering_index_scan` | 11.53ms | 6.42ms | 0.6× | 1.4% | PASS |
| file_reads | `groupby_scan` | 35.34ms | 34.05ms | 1.0× | 0.6% | PASS |
| file_reads | `index_join` | 14.16ms | 10.49ms | 0.7× | 3.3% | PASS |
| file_reads | `index_join_scan` | 4.89ms | 5.38ms | 1.1× | 1.9% | PASS |
| file_reads | `types_table_scan` | 1.04s | 1.17s | 1.1× | 0.5% | PASS |
| file_reads | `table_scan` | 1.16s | 1.27s | 1.1× | 0.3% | PASS |
| file_reads | `oltp_read_only` | 240.87ms | 161.43ms | 0.7× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 264.71ms | 364.37ms | 1.4× | 1.1% | PASS |
| file_writes | `oltp_insert` | 25.80ms | 44.33ms | 1.7× | 1.4% | PASS |
| file_writes | `oltp_update_index` | 94.17ms | 141.69ms | 1.5× | 1.1% | PASS |
| file_writes | `oltp_update_non_index` | 85.86ms | 87.02ms | 1.0× | 7.9% | PASS |
| file_writes | `oltp_delete_insert` | 84.56ms | 111.56ms | 1.3× | 1.5% | PASS |
| file_writes | `oltp_write_only` | 55.63ms | 67.98ms | 1.2× | 1.5% | PASS |
| file_writes | `types_delete_insert` | 61.82ms | 59.38ms | 1.0× | 1.5% | PASS |
| file_writes | `oltp_read_write` | 125.05ms | 149.67ms | 1.2× | 1.5% | PASS |
| ac_reads | `oltp_point_select` | 57.18ms | 54.94ms | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_range_select` | 18.76ms | 15.30ms | 0.8× | 1.3% | PASS |
| ac_reads | `oltp_sum_range` | 17.32ms | 15.39ms | 0.9× | 1.2% | PASS |
| ac_reads | `oltp_order_range` | 3.74ms | 3.36ms | 0.9× | 1.7% | PASS |
| ac_reads | `oltp_distinct_range` | 4.76ms | 4.44ms | 0.9× | 1.0% | PASS |
| ac_reads | `oltp_index_scan` | 6.51ms | 7.97ms | 1.2× | 1.7% | PASS |
| ac_reads | `select_random_points` | 24.05ms | 23.01ms | 1.0× | 1.3% | PASS |
| ac_reads | `select_random_ranges` | 5.83ms | 7.03ms | 1.2× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 6.71ms | 6.42ms | 1.0× | 1.4% | PASS |
| ac_reads | `groupby_scan` | 34.68ms | 34.01ms | 1.0× | 1.0% | PASS |
| ac_reads | `index_join` | 11.98ms | 10.52ms | 0.9× | 1.8% | PASS |
| ac_reads | `index_join_scan` | 4.31ms | 5.40ms | 1.3× | 1.0% | PASS |
| ac_reads | `types_table_scan` | 1.03s | 1.17s | 1.1× | 0.4% | PASS |
| ac_reads | `table_scan` | 1.16s | 1.27s | 1.1× | 0.3% | PASS |
| ac_reads | `oltp_read_only` | 172.25ms | 161.66ms | 0.9× | 1.1% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 23.16ms | 74.88ms | 3.2× | 4.3% | PASS |
| ac_writes | `oltp_insert_ac` | 24.63ms | 90.25ms | 3.7× | 3.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.21ms | 98.20ms | 3.7× | 3.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.66ms | 81.87ms | 3.6× | 4.1% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.05ms | 91.25ms | 3.6× | 3.9% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.43ms | 90.30ms | 3.6× | 4.5% | PASS |
| ac_writes | `types_delete_insert_ac` | 23.30ms | 88.14ms | 3.8× | 6.8% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.07ms | 96.68ms | 3.1× | 2.3% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.05ms | 38.77ms | 1.2× | 1.7% | PASS |
| mem_reads | `oltp_range_select` | 18.75ms | 21.17ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_sum_range` | 17.91ms | 20.58ms | 1.1× | 1.9% | PASS |
| mem_reads | `oltp_order_range` | 3.52ms | 3.87ms | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_distinct_range` | 4.58ms | 4.91ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 4.53ms | 5.77ms | 1.3× | 1.4% | PASS |
| mem_reads | `select_random_points` | 27.66ms | 31.82ms | 1.2× | 1.3% | PASS |
| mem_reads | `select_random_ranges` | 7.60ms | 8.69ms | 1.1× | 1.7% | PASS |
| mem_reads | `covering_index_scan` | 4.25ms | 4.00ms | 0.9× | 1.6% | PASS |
| mem_reads | `groupby_scan` | 37.58ms | 39.21ms | 1.0× | 0.8% | PASS |
| mem_reads | `index_join` | 8.24ms | 10.31ms | 1.3× | 1.3% | PASS |
| mem_reads | `index_join_scan` | 4.13ms | 5.30ms | 1.3× | 1.9% | PASS |
| mem_reads | `types_table_scan` | 1.04s | 1.19s | 1.1× | 0.5% | PASS |
| mem_reads | `table_scan` | 1.18s | 1.29s | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_read_only` | 149.67ms | 168.00ms | 1.1× | 1.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 247.92ms | 339.60ms | 1.4× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 19.16ms | 34.13ms | 1.8× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 66.92ms | 116.36ms | 1.7× | 1.3% | PASS |
| mem_writes | `oltp_update_non_index` | 50.38ms | 75.24ms | 1.5× | 1.5% | PASS |
| mem_writes | `oltp_delete_insert` | 49.11ms | 92.94ms | 1.9× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 26.68ms | 55.30ms | 2.1× | 1.1% | PASS |
| mem_writes | `types_delete_insert` | 32.81ms | 52.13ms | 1.6× | 1.7% | PASS |
| mem_writes | `oltp_read_write` | 100.45ms | 156.03ms | 1.6× | 1.0% | PASS |
| file_reads | `oltp_point_select` | 103.59ms | 58.03ms | 0.6× | 1.2% | PASS |
| file_reads | `oltp_range_select` | 26.53ms | 23.76ms | 0.9× | 1.6% | PASS |
| file_reads | `oltp_sum_range` | 25.50ms | 22.99ms | 0.9× | 1.8% | PASS |
| file_reads | `oltp_order_range` | 4.54ms | 4.43ms | 1.0× | 2.7% | PASS |
| file_reads | `oltp_distinct_range` | 5.67ms | 5.48ms | 1.0× | 2.5% | PASS |
| file_reads | `oltp_index_scan` | 11.75ms | 8.13ms | 0.7× | 1.9% | PASS |
| file_reads | `select_random_points` | 36.09ms | 35.34ms | 1.0× | 1.3% | PASS |
| file_reads | `select_random_ranges` | 15.19ms | 10.93ms | 0.7× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 11.60ms | 6.33ms | 0.5× | 1.7% | PASS |
| file_reads | `groupby_scan` | 38.25ms | 39.72ms | 1.0× | 0.9% | PASS |
| file_reads | `index_join` | 12.19ms | 12.11ms | 1.0× | 1.6% | PASS |
| file_reads | `index_join_scan` | 5.06ms | 5.68ms | 1.1× | 2.0% | PASS |
| file_reads | `types_table_scan` | 1.03s | 1.18s | 1.1× | 0.6% | PASS |
| file_reads | `table_scan` | 1.18s | 1.29s | 1.1× | 0.8% | PASS |
| file_reads | `oltp_read_only` | 255.12ms | 196.24ms | 0.8× | 1.1% | PASS |
| file_writes | `oltp_bulk_insert` | 262.23ms | 350.14ms | 1.3× | 1.0% | PASS |
| file_writes | `oltp_insert` | 26.09ms | 39.98ms | 1.5× | 2.1% | PASS |
| file_writes | `oltp_update_index` | 96.22ms | 130.75ms | 1.4× | 1.6% | PASS |
| file_writes | `oltp_update_non_index` | 75.60ms | 90.02ms | 1.2× | 1.8% | PASS |
| file_writes | `oltp_delete_insert` | 75.36ms | 105.38ms | 1.4× | 1.9% | PASS |
| file_writes | `oltp_write_only` | 50.41ms | 66.48ms | 1.3× | 1.5% | PASS |
| file_writes | `types_delete_insert` | 48.82ms | 58.78ms | 1.2× | 2.1% | PASS |
| file_writes | `oltp_read_write` | 127.72ms | 167.98ms | 1.3× | 1.7% | PASS |
| ac_reads | `oltp_point_select` | 56.11ms | 58.02ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 21.69ms | 23.60ms | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_sum_range` | 20.64ms | 22.87ms | 1.1× | 0.9% | PASS |
| ac_reads | `oltp_order_range` | 4.06ms | 4.35ms | 1.1× | 2.6% | PASS |
| ac_reads | `oltp_distinct_range` | 5.14ms | 5.43ms | 1.1× | 2.2% | PASS |
| ac_reads | `oltp_index_scan` | 7.20ms | 8.17ms | 1.1× | 1.6% | PASS |
| ac_reads | `select_random_points` | 31.07ms | 35.20ms | 1.1× | 1.1% | PASS |
| ac_reads | `select_random_ranges` | 10.27ms | 10.92ms | 1.1× | 1.1% | PASS |
| ac_reads | `covering_index_scan` | 6.96ms | 6.34ms | 0.9× | 1.9% | PASS |
| ac_reads | `groupby_scan` | 38.04ms | 40.01ms | 1.1× | 1.0% | PASS |
| ac_reads | `index_join` | 9.87ms | 12.20ms | 1.2× | 1.4% | PASS |
| ac_reads | `index_join_scan` | 4.61ms | 5.72ms | 1.2× | 2.0% | PASS |
| ac_reads | `types_table_scan` | 1.03s | 1.18s | 1.1× | 0.5% | PASS |
| ac_reads | `table_scan` | 1.19s | 1.29s | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_read_only` | 189.96ms | 199.30ms | 1.0× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 23.53ms | 79.68ms | 3.4× | 5.8% | PASS |
| ac_writes | `oltp_insert_ac` | 27.26ms | 98.22ms | 3.6× | 8.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 29.89ms | 108.60ms | 3.6× | 6.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.89ms | 91.30ms | 3.7× | 6.2% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 27.53ms | 101.69ms | 3.7× | 6.9% | PASS |
| ac_writes | `oltp_write_only_ac` | 27.15ms | 102.26ms | 3.8× | 7.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 24.96ms | 97.34ms | 3.9× | 8.8% | PASS |
| ac_writes | `oltp_read_write_ac` | 34.34ms | 108.68ms | 3.2× | 5.8% | PASS |

</details>

</details>

## Version-control latency

Wall time: 2m 18s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 82.47ms | 130.00ms | 63.4% | 0.5% | PASS |
| `status_dirty_many_tables` | 85.54ms | 130.00ms | 65.8% | 0.5% | PASS |
| `diff_regular_working_one_table` | 77.80ms | 120.00ms | 64.8% | 0.5% | PASS |
| `diff_regular_working_many_tables` | 91.00ms | 140.00ms | 65.0% | 0.4% | PASS |
| `diff_stat_working_many_tables` | 90.64ms | 140.00ms | 64.7% | 0.5% | PASS |
| `diff_schema_working_many_tables` | 90.95ms | 140.00ms | 65.0% | 0.5% | PASS |
| `branch_list_many_branches` | 21.74ms | 35.00ms | 62.1% | 0.5% | PASS |
| `branch_create_delete` | 24.37ms | 40.00ms | 60.9% | 0.8% | PASS |
| `checkout_branch_clean` | 55.19ms | 150.00ms | 36.8% | 0.8% | PASS |
| `merge_data_no_conflicts` | 28.45ms | 50.00ms | 56.9% | 0.9% | PASS |
| `merge_schema_no_conflicts` | 21.28ms | 35.00ms | 60.8% | 0.7% | PASS |
| `merge_data_conflicts` | 125.80ms | 180.00ms | 69.9% | 0.3% | PASS |
| `merge_data_conflicts_with_resolve` | 126.18ms | 180.00ms | 70.1% | 0.2% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
