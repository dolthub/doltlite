# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-08-28 15:00 UTC
>
> Commit: [`2abe368cc8d1f515728a8aa1377bd111e4191400`](https://github.com/dolthub/doltlite/commit/2abe368cc8d1f515728a8aa1377bd111e4191400)
>
> Runner: ubuntu24 20260823.283.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/33175036111)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.53s | 10.90s | 1.0× | 1.4% | **PASS** |
| Writes | 2.07s | 3.19s | 1.5× | 1.6% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 11.40s | 11.04s | 1.0× | 1.4% | **PASS** |
| Writes | 4.26s | 4.62s | 1.1× | 4.2% | **PASS** |
| Autocommit writes | 967.28ms | 3.84s | 4.0× | 7.3% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.63s | 2.69s | 1.0× | 1.4% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.16s | 2.22s | 1.0× | 1.2% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.99s | 3.09s | 1.0× | 4.0% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.75s | 2.91s | 1.1× | 0.9% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 459.67ms | 695.93ms | 1.5× | 1.6% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 477.22ms | 726.25ms | 1.5× | 1.2% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 527.38ms | 841.17ms | 1.6× | 3.6% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 602.39ms | 927.27ms | 1.5× | 1.4% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 3.05s | 2.81s | 0.9× | 1.1% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.42s | 2.28s | 0.9× | 1.1% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.93s | 2.98s | 1.0× | 3.4% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 3.01s | 2.98s | 1.0× | 1.4% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 619.58ms | 759.48ms | 1.2× | 1.9% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.54s | 1.31s | 0.9× | 19.8% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 1.35s | 1.53s | 1.1× | 6.9% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 758.34ms | 1.01s | 1.3× | 1.2% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.87s | 2.80s | 1.0× | 1.6% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.27s | 2.28s | 1.0× | 0.9% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.84s | 2.94s | 1.0× | 3.2% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.85s | 2.99s | 1.0× | 1.3% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 144.43ms | 572.42ms | 4.0× | 4.6% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 430.96ms | 1.73s | 4.0× | 62.9% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 237.56ms | 931.29ms | 3.9× | 10.4% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 154.33ms | 600.37ms | 3.9× | 5.9% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 25.32ms | 26.82ms | 1.1× | 2.3% | PASS |
| mem_reads | `oltp_range_select` | 11.23ms | 10.89ms | 1.0× | 2.2% | PASS |
| mem_reads | `oltp_sum_range` | 10.13ms | 10.45ms | 1.0× | 2.1% | PASS |
| mem_reads | `oltp_order_range` | 2.68ms | 2.73ms | 1.0× | 1.6% | PASS |
| mem_reads | `oltp_distinct_range` | 3.79ms | 3.81ms | 1.0× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 4.08ms | 4.86ms | 1.2× | 1.2% | PASS |
| mem_reads | `select_random_points` | 11.22ms | 11.08ms | 1.0× | 2.9% | PASS |
| mem_reads | `select_random_ranges` | 3.23ms | 3.93ms | 1.2× | 0.8% | PASS |
| mem_reads | `covering_index_scan` | 4.36ms | 4.14ms | 0.9× | 1.4% | PASS |
| mem_reads | `groupby_scan` | 32.96ms | 33.39ms | 1.0× | 0.8% | PASS |
| mem_reads | `index_join` | 6.00ms | 7.51ms | 1.3× | 1.3% | PASS |
| mem_reads | `index_join_scan` | 3.62ms | 4.60ms | 1.3× | 2.3% | PASS |
| mem_reads | `types_table_scan` | 1.12s | 1.16s | 1.0× | 0.7% | PASS |
| mem_reads | `table_scan` | 1.28s | 1.29s | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_read_only` | 108.89ms | 110.80ms | 1.0× | 1.4% | PASS |
| mem_writes | `oltp_bulk_insert` | 180.49ms | 241.55ms | 1.3× | 1.1% | PASS |
| mem_writes | `oltp_insert` | 15.87ms | 27.74ms | 1.7× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 54.62ms | 96.91ms | 1.8× | 2.0% | PASS |
| mem_writes | `oltp_update_non_index` | 39.27ms | 57.39ms | 1.5× | 1.8% | PASS |
| mem_writes | `oltp_delete_insert` | 47.85ms | 76.37ms | 1.6× | 1.5% | PASS |
| mem_writes | `oltp_write_only` | 23.36ms | 48.66ms | 2.1× | 1.3% | PASS |
| mem_writes | `types_delete_insert` | 26.17ms | 37.24ms | 1.4× | 1.9% | PASS |
| mem_writes | `oltp_read_write` | 72.03ms | 110.08ms | 1.5× | 2.4% | PASS |
| file_reads | `oltp_point_select` | 111.17ms | 49.38ms | 0.4× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 21.12ms | 13.48ms | 0.6× | 1.4% | PASS |
| file_reads | `oltp_sum_range` | 19.78ms | 13.01ms | 0.7× | 1.5% | PASS |
| file_reads | `oltp_order_range` | 3.70ms | 3.09ms | 0.8× | 0.7% | PASS |
| file_reads | `oltp_distinct_range` | 4.79ms | 4.11ms | 0.9× | 0.8% | PASS |
| file_reads | `oltp_index_scan` | 13.02ms | 7.54ms | 0.6× | 1.1% | PASS |
| file_reads | `select_random_points` | 21.10ms | 13.84ms | 0.7× | 2.5% | PASS |
| file_reads | `select_random_ranges` | 11.70ms | 6.14ms | 0.5× | 0.8% | PASS |
| file_reads | `covering_index_scan` | 13.28ms | 6.68ms | 0.5× | 0.8% | PASS |
| file_reads | `groupby_scan` | 34.47ms | 33.97ms | 1.0× | 0.6% | PASS |
| file_reads | `index_join` | 11.13ms | 9.60ms | 0.9× | 1.9% | PASS |
| file_reads | `index_join_scan` | 4.56ms | 5.13ms | 1.1× | 3.9% | PASS |
| file_reads | `types_table_scan` | 1.20s | 1.20s | 1.0× | 1.6% | PASS |
| file_reads | `table_scan` | 1.34s | 1.30s | 1.0× | 2.8% | PASS |
| file_reads | `oltp_read_only` | 231.53ms | 142.95ms | 0.6× | 1.1% | PASS |
| file_writes | `oltp_bulk_insert` | 195.14ms | 249.81ms | 1.3× | 0.9% | PASS |
| file_writes | `oltp_insert` | 22.42ms | 31.86ms | 1.4× | 2.1% | PASS |
| file_writes | `oltp_update_index` | 82.35ms | 105.55ms | 1.3× | 1.9% | PASS |
| file_writes | `oltp_update_non_index` | 62.46ms | 69.19ms | 1.1× | 2.0% | PASS |
| file_writes | `oltp_delete_insert` | 72.50ms | 85.49ms | 1.2× | 1.7% | PASS |
| file_writes | `oltp_write_only` | 47.07ms | 56.67ms | 1.2× | 2.4% | PASS |
| file_writes | `types_delete_insert` | 42.47ms | 44.07ms | 1.0× | 1.9% | PASS |
| file_writes | `oltp_read_write` | 95.16ms | 116.84ms | 1.2× | 1.8% | PASS |
| ac_reads | `oltp_point_select` | 53.83ms | 48.66ms | 0.9× | 0.8% | PASS |
| ac_reads | `oltp_range_select` | 14.53ms | 13.23ms | 0.9× | 1.5% | PASS |
| ac_reads | `oltp_sum_range` | 13.51ms | 12.79ms | 0.9× | 2.4% | PASS |
| ac_reads | `oltp_order_range` | 3.17ms | 3.09ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_distinct_range` | 4.18ms | 4.09ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_index_scan` | 7.30ms | 7.41ms | 1.0× | 1.5% | PASS |
| ac_reads | `select_random_points` | 14.65ms | 13.46ms | 0.9× | 1.7% | PASS |
| ac_reads | `select_random_ranges` | 6.17ms | 6.12ms | 1.0× | 1.2% | PASS |
| ac_reads | `covering_index_scan` | 7.74ms | 6.70ms | 0.9× | 1.6% | PASS |
| ac_reads | `groupby_scan` | 34.04ms | 33.97ms | 1.0× | 1.0% | PASS |
| ac_reads | `index_join` | 8.09ms | 9.70ms | 1.2× | 1.7% | PASS |
| ac_reads | `index_join_scan` | 4.03ms | 4.92ms | 1.2× | 2.3% | PASS |
| ac_reads | `types_table_scan` | 1.17s | 1.19s | 1.0× | 2.0% | PASS |
| ac_reads | `table_scan` | 1.38s | 1.31s | 0.9× | 2.0% | PASS |
| ac_reads | `oltp_read_only` | 148.57ms | 141.27ms | 1.0× | 1.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 15.86ms | 57.46ms | 3.6× | 5.1% | PASS |
| ac_writes | `oltp_insert_ac` | 17.48ms | 69.98ms | 4.0× | 4.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 19.96ms | 83.69ms | 4.2× | 3.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 15.97ms | 64.13ms | 4.0× | 3.4% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 17.50ms | 74.78ms | 4.3× | 3.7% | PASS |
| ac_writes | `oltp_write_only_ac` | 18.20ms | 76.11ms | 4.2× | 5.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 15.51ms | 65.28ms | 4.2× | 5.2% | PASS |
| ac_writes | `oltp_read_write_ac` | 23.96ms | 80.99ms | 3.4× | 4.3% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 28.31ms | 27.05ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_range_select` | 14.63ms | 10.67ms | 0.7× | 1.2% | PASS |
| mem_reads | `oltp_sum_range` | 12.66ms | 10.42ms | 0.8× | 0.8% | PASS |
| mem_reads | `oltp_order_range` | 2.73ms | 2.48ms | 0.9× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 3.57ms | 3.28ms | 0.9× | 0.7% | PASS |
| mem_reads | `oltp_index_scan` | 3.27ms | 4.55ms | 1.4× | 1.5% | PASS |
| mem_reads | `select_random_points` | 18.03ms | 16.06ms | 0.9× | 2.3% | PASS |
| mem_reads | `select_random_ranges` | 2.92ms | 4.16ms | 1.4× | 1.1% | PASS |
| mem_reads | `covering_index_scan` | 3.40ms | 3.59ms | 1.1× | 3.3% | PASS |
| mem_reads | `groupby_scan` | 29.15ms | 27.49ms | 0.9× | 0.5% | PASS |
| mem_reads | `index_join` | 9.03ms | 6.83ms | 0.8× | 1.7% | PASS |
| mem_reads | `index_join_scan` | 3.40ms | 4.74ms | 1.4× | 2.0% | PASS |
| mem_reads | `types_table_scan` | 910.82ms | 953.23ms | 1.0× | 1.2% | PASS |
| mem_reads | `table_scan` | 1.01s | 1.05s | 1.0× | 0.8% | PASS |
| mem_reads | `oltp_read_only` | 110.15ms | 99.44ms | 0.9× | 1.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 185.29ms | 267.56ms | 1.4× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 14.38ms | 28.02ms | 1.9× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 53.67ms | 102.83ms | 1.9× | 1.6% | PASS |
| mem_writes | `oltp_update_non_index` | 40.06ms | 58.06ms | 1.4× | 1.4% | PASS |
| mem_writes | `oltp_delete_insert` | 44.43ms | 77.72ms | 1.7× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 23.08ms | 44.91ms | 1.9× | 0.9% | PASS |
| mem_writes | `types_delete_insert` | 32.12ms | 38.56ms | 1.2× | 2.3% | PASS |
| mem_writes | `oltp_read_write` | 84.19ms | 108.58ms | 1.3× | 1.9% | PASS |
| file_reads | `oltp_point_select` | 96.80ms | 45.02ms | 0.5× | 1.1% | PASS |
| file_reads | `oltp_range_select` | 22.70ms | 12.61ms | 0.6× | 1.7% | PASS |
| file_reads | `oltp_sum_range` | 20.42ms | 12.32ms | 0.6× | 1.2% | PASS |
| file_reads | `oltp_order_range` | 3.65ms | 2.76ms | 0.8× | 1.1% | PASS |
| file_reads | `oltp_distinct_range` | 4.49ms | 3.54ms | 0.8× | 1.2% | PASS |
| file_reads | `oltp_index_scan` | 10.18ms | 6.73ms | 0.7× | 0.5% | PASS |
| file_reads | `select_random_points` | 25.81ms | 17.96ms | 0.7× | 1.1% | PASS |
| file_reads | `select_random_ranges` | 9.59ms | 5.95ms | 0.6× | 0.6% | PASS |
| file_reads | `covering_index_scan` | 10.36ms | 5.67ms | 0.5× | 0.5% | PASS |
| file_reads | `groupby_scan` | 30.41ms | 27.95ms | 0.9× | 0.9% | PASS |
| file_reads | `index_join` | 13.35ms | 8.72ms | 0.7× | 0.9% | PASS |
| file_reads | `index_join_scan` | 4.19ms | 5.06ms | 1.2× | 0.8% | PASS |
| file_reads | `types_table_scan` | 895.15ms | 942.42ms | 1.1× | 1.0% | PASS |
| file_reads | `table_scan` | 1.07s | 1.06s | 1.0× | 2.4% | PASS |
| file_reads | `oltp_read_only` | 207.98ms | 124.88ms | 0.6× | 1.2% | PASS |
| file_writes | `oltp_bulk_insert` | 322.16ms | 367.47ms | 1.1× | 19.2% | PASS |
| file_writes | `oltp_insert` | 37.88ms | 60.35ms | 1.6× | 22.8% | PASS |
| file_writes | `oltp_update_index` | 230.60ms | 217.78ms | 0.9× | 34.2% | PASS |
| file_writes | `oltp_update_non_index` | 235.63ms | 141.53ms | 0.6× | 36.5% | PASS |
| file_writes | `oltp_delete_insert` | 233.67ms | 169.47ms | 0.7× | 19.9% | PASS |
| file_writes | `oltp_write_only` | 129.16ms | 103.76ms | 0.8× | 19.7% | PASS |
| file_writes | `types_delete_insert` | 153.60ms | 86.66ms | 0.6× | 11.9% | PASS |
| file_writes | `oltp_read_write` | 193.02ms | 163.78ms | 0.8× | 16.5% | PASS |
| ac_reads | `oltp_point_select` | 50.73ms | 44.21ms | 0.9× | 0.9% | PASS |
| ac_reads | `oltp_range_select` | 17.15ms | 12.55ms | 0.7× | 1.0% | PASS |
| ac_reads | `oltp_sum_range` | 15.75ms | 12.28ms | 0.8× | 0.7% | PASS |
| ac_reads | `oltp_order_range` | 3.26ms | 2.76ms | 0.8× | 0.9% | PASS |
| ac_reads | `oltp_distinct_range` | 4.06ms | 3.54ms | 0.9× | 0.7% | PASS |
| ac_reads | `oltp_index_scan` | 5.90ms | 6.74ms | 1.1× | 0.9% | PASS |
| ac_reads | `select_random_points` | 21.59ms | 18.07ms | 0.8× | 1.2% | PASS |
| ac_reads | `select_random_ranges` | 5.36ms | 5.97ms | 1.1× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 6.07ms | 5.66ms | 0.9× | 0.8% | PASS |
| ac_reads | `groupby_scan` | 30.42ms | 28.12ms | 0.9× | 0.8% | PASS |
| ac_reads | `index_join` | 11.40ms | 8.79ms | 0.8× | 1.1% | PASS |
| ac_reads | `index_join_scan` | 3.85ms | 5.05ms | 1.3× | 0.7% | PASS |
| ac_reads | `types_table_scan` | 921.96ms | 954.82ms | 1.0× | 2.1% | PASS |
| ac_reads | `table_scan` | 1.03s | 1.05s | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_read_only` | 143.41ms | 124.39ms | 0.9× | 1.4% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 32.65ms | 128.36ms | 3.9× | 63.0% | PASS |
| ac_writes | `oltp_insert_ac` | 48.54ms | 172.91ms | 3.6× | 58.3% | PASS |
| ac_writes | `oltp_update_index_ac` | 45.38ms | 260.71ms | 5.7× | 62.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 48.30ms | 228.17ms | 4.7× | 75.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 77.00ms | 346.12ms | 4.5× | 62.7% | PASS |
| ac_writes | `oltp_write_only_ac` | 80.05ms | 250.40ms | 3.1× | 78.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 44.52ms | 225.94ms | 5.1× | 64.2% | PASS |
| ac_writes | `oltp_read_write_ac` | 54.52ms | 120.89ms | 2.2× | 53.4% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 35.98ms | 33.28ms | 0.9× | 4.3% | PASS |
| mem_reads | `oltp_range_select` | 14.85ms | 12.65ms | 0.9× | 2.5% | PASS |
| mem_reads | `oltp_sum_range` | 16.60ms | 13.35ms | 0.8× | 4.0% | PASS |
| mem_reads | `oltp_order_range` | 3.29ms | 3.16ms | 1.0× | 5.1% | PASS |
| mem_reads | `oltp_distinct_range` | 4.10ms | 3.99ms | 1.0× | 3.1% | PASS |
| mem_reads | `oltp_index_scan` | 4.20ms | 6.23ms | 1.5× | 4.3% | PASS |
| mem_reads | `select_random_points` | 25.09ms | 21.82ms | 0.9× | 6.1% | PASS |
| mem_reads | `select_random_ranges` | 3.06ms | 4.69ms | 1.5× | 2.8% | PASS |
| mem_reads | `covering_index_scan` | 3.57ms | 4.28ms | 1.2× | 2.9% | PASS |
| mem_reads | `groupby_scan` | 34.25ms | 32.51ms | 0.9× | 2.1% | PASS |
| mem_reads | `index_join` | 11.63ms | 9.57ms | 0.8× | 4.9% | PASS |
| mem_reads | `index_join_scan` | 3.49ms | 5.60ms | 1.6× | 4.3% | PASS |
| mem_reads | `types_table_scan` | 1.29s | 1.39s | 1.1× | 3.5% | PASS |
| mem_reads | `table_scan` | 1.40s | 1.42s | 1.0× | 4.0% | PASS |
| mem_reads | `oltp_read_only` | 136.07ms | 126.33ms | 0.9× | 3.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 193.96ms | 287.68ms | 1.5× | 1.2% | PASS |
| mem_writes | `oltp_insert` | 16.19ms | 32.80ms | 2.0× | 3.3% | PASS |
| mem_writes | `oltp_update_index` | 64.36ms | 131.47ms | 2.0× | 3.8% | PASS |
| mem_writes | `oltp_update_non_index` | 47.16ms | 70.88ms | 1.5× | 3.8% | PASS |
| mem_writes | `oltp_delete_insert` | 49.77ms | 93.47ms | 1.9× | 4.8% | PASS |
| mem_writes | `oltp_write_only` | 24.75ms | 50.60ms | 2.0× | 3.0% | PASS |
| mem_writes | `types_delete_insert` | 34.74ms | 45.17ms | 1.3× | 3.1% | PASS |
| mem_writes | `oltp_read_write` | 96.45ms | 129.11ms | 1.3× | 5.4% | PASS |
| file_reads | `oltp_point_select` | 67.51ms | 39.90ms | 0.6× | 2.7% | PASS |
| file_reads | `oltp_range_select` | 19.94ms | 14.70ms | 0.7× | 4.4% | PASS |
| file_reads | `oltp_sum_range` | 18.46ms | 13.72ms | 0.7× | 2.9% | PASS |
| file_reads | `oltp_order_range` | 3.60ms | 3.24ms | 0.9× | 4.0% | PASS |
| file_reads | `oltp_distinct_range` | 4.51ms | 4.14ms | 0.9× | 2.6% | PASS |
| file_reads | `oltp_index_scan` | 7.45ms | 6.72ms | 0.9× | 4.2% | PASS |
| file_reads | `select_random_points` | 28.80ms | 23.29ms | 0.8× | 4.8% | PASS |
| file_reads | `select_random_ranges` | 7.11ms | 6.21ms | 0.9× | 4.8% | PASS |
| file_reads | `covering_index_scan` | 7.07ms | 5.21ms | 0.7× | 2.2% | PASS |
| file_reads | `groupby_scan` | 34.61ms | 32.59ms | 0.9× | 2.1% | PASS |
| file_reads | `index_join` | 13.69ms | 10.01ms | 0.7× | 4.7% | PASS |
| file_reads | `index_join_scan` | 3.83ms | 5.54ms | 1.4× | 2.2% | PASS |
| file_reads | `types_table_scan` | 1.16s | 1.25s | 1.1× | 2.6% | PASS |
| file_reads | `table_scan` | 1.37s | 1.42s | 1.0× | 3.4% | PASS |
| file_reads | `oltp_read_only` | 186.57ms | 140.96ms | 0.8× | 4.2% | PASS |
| file_writes | `oltp_bulk_insert` | 301.59ms | 438.44ms | 1.5× | 6.0% | PASS |
| file_writes | `oltp_insert` | 34.01ms | 71.38ms | 2.1× | 6.6% | PASS |
| file_writes | `oltp_update_index` | 195.17ms | 246.25ms | 1.3× | 7.0% | PASS |
| file_writes | `oltp_update_non_index` | 153.86ms | 156.90ms | 1.0× | 8.3% | PASS |
| file_writes | `oltp_delete_insert` | 183.23ms | 182.47ms | 1.0× | 7.2% | PASS |
| file_writes | `oltp_write_only` | 131.43ms | 129.65ms | 1.0× | 8.1% | PASS |
| file_writes | `types_delete_insert` | 139.49ms | 105.20ms | 0.8× | 6.9% | PASS |
| file_writes | `oltp_read_write` | 207.42ms | 204.67ms | 1.0× | 6.6% | PASS |
| ac_reads | `oltp_point_select` | 46.23ms | 41.57ms | 0.9× | 4.2% | PASS |
| ac_reads | `oltp_range_select` | 18.17ms | 14.53ms | 0.8× | 6.2% | PASS |
| ac_reads | `oltp_sum_range` | 16.19ms | 14.15ms | 0.9× | 3.4% | PASS |
| ac_reads | `oltp_order_range` | 3.48ms | 3.26ms | 0.9× | 3.2% | PASS |
| ac_reads | `oltp_distinct_range` | 4.35ms | 4.26ms | 1.0× | 3.4% | PASS |
| ac_reads | `oltp_index_scan` | 5.56ms | 6.80ms | 1.2× | 4.6% | PASS |
| ac_reads | `select_random_points` | 27.81ms | 23.43ms | 0.8× | 9.1% | PASS |
| ac_reads | `select_random_ranges` | 4.13ms | 5.50ms | 1.3× | 2.0% | PASS |
| ac_reads | `covering_index_scan` | 4.66ms | 4.93ms | 1.1× | 1.1% | PASS |
| ac_reads | `groupby_scan` | 32.91ms | 31.53ms | 1.0× | 0.7% | PASS |
| ac_reads | `index_join` | 12.29ms | 9.72ms | 0.8× | 2.9% | PASS |
| ac_reads | `index_join_scan` | 3.62ms | 5.34ms | 1.5× | 1.8% | PASS |
| ac_reads | `types_table_scan` | 1.11s | 1.22s | 1.1× | 1.9% | PASS |
| ac_reads | `table_scan` | 1.38s | 1.40s | 1.0× | 2.3% | PASS |
| ac_reads | `oltp_read_only` | 164.97ms | 145.03ms | 0.9× | 5.5% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 26.42ms | 92.75ms | 3.5× | 7.2% | PASS |
| ac_writes | `oltp_insert_ac` | 28.80ms | 109.54ms | 3.8× | 7.3% | PASS |
| ac_writes | `oltp_update_index_ac` | 31.36ms | 128.09ms | 4.1× | 11.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 26.80ms | 105.02ms | 3.9× | 9.1% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 29.68ms | 124.14ms | 4.2× | 11.0% | PASS |
| ac_writes | `oltp_write_only_ac` | 29.34ms | 117.33ms | 4.0× | 10.7% | PASS |
| ac_writes | `types_delete_insert_ac` | 29.04ms | 118.75ms | 4.1× | 12.1% | PASS |
| ac_writes | `oltp_read_write_ac` | 36.13ms | 135.68ms | 3.8× | 10.1% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.18ms | 36.38ms | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_range_select` | 19.44ms | 19.62ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_sum_range` | 18.15ms | 18.89ms | 1.0× | 0.9% | PASS |
| mem_reads | `oltp_order_range` | 3.70ms | 3.75ms | 1.0× | 0.9% | PASS |
| mem_reads | `oltp_distinct_range` | 4.74ms | 4.84ms | 1.0× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 4.73ms | 5.63ms | 1.2× | 1.2% | PASS |
| mem_reads | `select_random_points` | 28.36ms | 30.95ms | 1.1× | 1.3% | PASS |
| mem_reads | `select_random_ranges` | 7.59ms | 8.26ms | 1.1× | 0.9% | PASS |
| mem_reads | `covering_index_scan` | 4.37ms | 4.15ms | 1.0× | 2.0% | PASS |
| mem_reads | `groupby_scan` | 39.29ms | 39.86ms | 1.0× | 0.8% | PASS |
| mem_reads | `index_join` | 8.18ms | 10.07ms | 1.2× | 1.4% | PASS |
| mem_reads | `index_join_scan` | 4.26ms | 5.62ms | 1.3× | 1.6% | PASS |
| mem_reads | `types_table_scan` | 1.12s | 1.22s | 1.1× | 0.5% | PASS |
| mem_reads | `table_scan` | 1.30s | 1.34s | 1.0× | 0.9% | PASS |
| mem_reads | `oltp_read_only` | 150.83ms | 158.13ms | 1.0× | 0.9% | PASS |
| mem_writes | `oltp_bulk_insert` | 246.34ms | 333.81ms | 1.4× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 19.47ms | 34.73ms | 1.8× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 69.73ms | 120.72ms | 1.7× | 1.5% | PASS |
| mem_writes | `oltp_update_non_index` | 52.97ms | 76.74ms | 1.4× | 1.8% | PASS |
| mem_writes | `oltp_delete_insert` | 51.13ms | 96.77ms | 1.9× | 1.5% | PASS |
| mem_writes | `oltp_write_only` | 28.09ms | 58.63ms | 2.1× | 1.0% | PASS |
| mem_writes | `types_delete_insert` | 33.63ms | 52.23ms | 1.6× | 1.4% | PASS |
| mem_writes | `oltp_read_write` | 101.03ms | 153.65ms | 1.5× | 1.7% | PASS |
| file_reads | `oltp_point_select` | 118.80ms | 58.95ms | 0.5× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 28.59ms | 22.25ms | 0.8× | 1.8% | PASS |
| file_reads | `oltp_sum_range` | 26.67ms | 21.48ms | 0.8× | 1.6% | PASS |
| file_reads | `oltp_order_range` | 4.43ms | 4.05ms | 0.9× | 1.6% | PASS |
| file_reads | `oltp_distinct_range` | 5.53ms | 5.16ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_index_scan` | 12.99ms | 8.16ms | 0.6× | 2.3% | PASS |
| file_reads | `select_random_points` | 35.55ms | 33.15ms | 0.9× | 1.3% | PASS |
| file_reads | `select_random_ranges` | 15.83ms | 10.70ms | 0.7× | 1.8% | PASS |
| file_reads | `covering_index_scan` | 12.80ms | 6.69ms | 0.5× | 1.1% | PASS |
| file_reads | `groupby_scan` | 39.83ms | 40.26ms | 1.0× | 1.1% | PASS |
| file_reads | `index_join` | 12.83ms | 12.27ms | 1.0× | 2.0% | PASS |
| file_reads | `index_join_scan` | 5.18ms | 6.03ms | 1.2× | 1.7% | PASS |
| file_reads | `types_table_scan` | 1.12s | 1.22s | 1.1× | 0.7% | PASS |
| file_reads | `table_scan` | 1.30s | 1.34s | 1.0× | 0.6% | PASS |
| file_reads | `oltp_read_only` | 270.31ms | 189.95ms | 0.7× | 1.1% | PASS |
| file_writes | `oltp_bulk_insert` | 262.14ms | 345.05ms | 1.3× | 0.9% | PASS |
| file_writes | `oltp_insert` | 25.73ms | 41.52ms | 1.6× | 1.9% | PASS |
| file_writes | `oltp_update_index` | 97.81ms | 135.66ms | 1.4× | 1.3% | PASS |
| file_writes | `oltp_update_non_index` | 77.54ms | 90.74ms | 1.2× | 1.1% | PASS |
| file_writes | `oltp_delete_insert` | 76.39ms | 109.60ms | 1.4× | 1.1% | PASS |
| file_writes | `oltp_write_only` | 49.82ms | 69.95ms | 1.4× | 1.5% | PASS |
| file_writes | `types_delete_insert` | 48.96ms | 57.32ms | 1.2× | 1.5% | PASS |
| file_writes | `oltp_read_write` | 119.95ms | 163.09ms | 1.4× | 1.0% | PASS |
| ac_reads | `oltp_point_select` | 60.91ms | 58.16ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_range_select` | 22.66ms | 22.22ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_sum_range` | 21.53ms | 21.42ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_order_range` | 4.20ms | 4.07ms | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_distinct_range` | 5.24ms | 5.17ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_index_scan` | 8.03ms | 8.38ms | 1.0× | 1.7% | PASS |
| ac_reads | `select_random_points` | 32.53ms | 34.13ms | 1.0× | 0.9% | PASS |
| ac_reads | `select_random_ranges` | 10.83ms | 10.76ms | 1.0× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 7.42ms | 6.76ms | 0.9× | 1.9% | PASS |
| ac_reads | `groupby_scan` | 39.28ms | 40.29ms | 1.0× | 1.1% | PASS |
| ac_reads | `index_join` | 10.04ms | 12.12ms | 1.2× | 1.7% | PASS |
| ac_reads | `index_join_scan` | 4.83ms | 6.09ms | 1.3× | 1.7% | PASS |
| ac_reads | `types_table_scan` | 1.12s | 1.22s | 1.1× | 0.6% | PASS |
| ac_reads | `table_scan` | 1.31s | 1.35s | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_read_only` | 189.89ms | 189.25ms | 1.0× | 0.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 17.04ms | 58.64ms | 3.4× | 6.1% | PASS |
| ac_writes | `oltp_insert_ac` | 18.92ms | 75.71ms | 4.0× | 7.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 20.70ms | 85.93ms | 4.2× | 7.3% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 17.53ms | 68.41ms | 3.9× | 5.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 18.57ms | 79.59ms | 4.3× | 7.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 19.27ms | 78.61ms | 4.1× | 5.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 16.55ms | 68.26ms | 4.1× | 5.3% | PASS |
| ac_writes | `oltp_read_write_ac` | 25.76ms | 85.21ms | 3.3× | 3.9% | PASS |

</details>

</details>

## Version-control latency

Wall time: 2m 19s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 82.80ms | 130.00ms | 63.7% | 0.4% | PASS |
| `status_dirty_many_tables` | 85.71ms | 130.00ms | 65.9% | 0.3% | PASS |
| `diff_regular_working_one_table` | 77.70ms | 120.00ms | 64.8% | 0.3% | PASS |
| `diff_regular_working_many_tables` | 90.75ms | 140.00ms | 64.8% | 0.3% | PASS |
| `diff_stat_working_many_tables` | 90.67ms | 140.00ms | 64.8% | 0.4% | PASS |
| `diff_schema_working_many_tables` | 92.92ms | 140.00ms | 66.4% | 0.5% | PASS |
| `branch_list_many_branches` | 22.83ms | 35.00ms | 65.2% | 1.4% | PASS |
| `branch_create_delete` | 25.19ms | 40.00ms | 63.0% | 1.3% | PASS |
| `checkout_branch_clean` | 54.87ms | 150.00ms | 36.6% | 1.0% | PASS |
| `merge_data_no_conflicts` | 29.14ms | 50.00ms | 58.3% | 1.1% | PASS |
| `merge_schema_no_conflicts` | 22.02ms | 35.00ms | 62.9% | 1.3% | PASS |
| `merge_data_conflicts` | 126.69ms | 180.00ms | 70.4% | 0.6% | PASS |
| `merge_data_conflicts_with_resolve` | 128.03ms | 180.00ms | 71.1% | 0.3% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
