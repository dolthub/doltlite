# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-08-19 11:17 UTC
>
> Commit: [`58bdad6f52493182fb4772842f7650effbc0a0b2`](https://github.com/dolthub/doltlite/commit/58bdad6f52493182fb4772842f7650effbc0a0b2)
>
> Runner: ubuntu24 20260816.277.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/32238978933)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.66s | 11.11s | 1.0× | 1.2% | **PASS** |
| Writes | 2.20s | 3.52s | 1.6× | 1.0% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 11.71s | 11.38s | 1.0× | 1.3% | **PASS** |
| Writes | 3.06s | 3.83s | 1.3× | 1.6% | **PASS** |
| Autocommit writes | 808.74ms | 2.95s | 3.6× | 5.0% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.44s | 2.60s | 1.1× | 1.3% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.99s | 2.83s | 0.9× | 2.0% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.49s | 2.73s | 1.1× | 0.9% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.74s | 2.96s | 1.1× | 1.0% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 443.80ms | 688.20ms | 1.6× | 1.3% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 612.41ms | 995.88ms | 1.6× | 1.4% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 557.17ms | 923.46ms | 1.7× | 0.7% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 591.47ms | 913.10ms | 1.5× | 0.8% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.72s | 2.68s | 1.0× | 1.0% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 3.21s | 2.88s | 0.9× | 1.6% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.70s | 2.78s | 1.0× | 1.9% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 3.08s | 3.05s | 1.0× | 1.0% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 591.76ms | 748.89ms | 1.3× | 1.8% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 948.36ms | 1.08s | 1.1× | 4.7% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 757.70ms | 1.01s | 1.3× | 1.3% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 758.75ms | 993.47ms | 1.3× | 1.3% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.51s | 2.66s | 1.1× | 1.4% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 3.08s | 2.88s | 0.9× | 1.7% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.55s | 2.77s | 1.1× | 1.5% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.83s | 3.03s | 1.1× | 1.0% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 189.17ms | 704.06ms | 3.7× | 4.7% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 284.99ms | 979.43ms | 3.4× | 8.9% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 186.57ms | 678.62ms | 3.6× | 3.2% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 148.00ms | 583.75ms | 3.9× | 5.0% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.46ms | 28.88ms | 1.2× | 1.7% | PASS |
| mem_reads | `oltp_range_select` | 10.56ms | 11.13ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_sum_range` | 9.82ms | 11.04ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_order_range` | 2.58ms | 2.78ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_distinct_range` | 3.63ms | 3.87ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 3.92ms | 4.96ms | 1.3× | 2.0% | PASS |
| mem_reads | `select_random_points` | 9.95ms | 10.53ms | 1.1× | 3.9% | PASS |
| mem_reads | `select_random_ranges` | 2.98ms | 3.88ms | 1.3× | 1.8% | PASS |
| mem_reads | `covering_index_scan` | 4.22ms | 4.04ms | 1.0× | 0.8% | PASS |
| mem_reads | `groupby_scan` | 29.98ms | 32.22ms | 1.1× | 0.5% | PASS |
| mem_reads | `index_join` | 6.07ms | 7.59ms | 1.3× | 1.4% | PASS |
| mem_reads | `index_join_scan` | 3.47ms | 4.36ms | 1.3× | 2.0% | PASS |
| mem_reads | `types_table_scan` | 1.04s | 1.13s | 1.1× | 0.4% | PASS |
| mem_reads | `table_scan` | 1.18s | 1.23s | 1.0× | 0.6% | PASS |
| mem_reads | `oltp_read_only` | 101.45ms | 115.37ms | 1.1× | 0.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 181.59ms | 250.33ms | 1.4× | 1.1% | PASS |
| mem_writes | `oltp_insert` | 15.45ms | 27.47ms | 1.8× | 1.1% | PASS |
| mem_writes | `oltp_update_index` | 49.59ms | 87.92ms | 1.8× | 1.0% | PASS |
| mem_writes | `oltp_update_non_index` | 33.97ms | 53.13ms | 1.6× | 1.5% | PASS |
| mem_writes | `oltp_delete_insert` | 45.72ms | 72.96ms | 1.6× | 1.3% | PASS |
| mem_writes | `oltp_write_only` | 22.31ms | 46.16ms | 2.1× | 1.8% | PASS |
| mem_writes | `types_delete_insert` | 25.06ms | 37.29ms | 1.5× | 1.3% | PASS |
| mem_writes | `oltp_read_write` | 70.11ms | 112.93ms | 1.6× | 1.5% | PASS |
| file_reads | `oltp_point_select` | 94.27ms | 47.81ms | 0.5× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 18.14ms | 13.17ms | 0.7× | 1.6% | PASS |
| file_reads | `oltp_sum_range` | 17.24ms | 13.12ms | 0.8× | 1.4% | PASS |
| file_reads | `oltp_order_range` | 3.39ms | 3.04ms | 0.9× | 1.5% | PASS |
| file_reads | `oltp_distinct_range` | 4.46ms | 4.13ms | 0.9× | 0.9% | PASS |
| file_reads | `oltp_index_scan` | 11.33ms | 7.38ms | 0.7× | 0.9% | PASS |
| file_reads | `select_random_points` | 18.43ms | 12.89ms | 0.7× | 2.8% | PASS |
| file_reads | `select_random_ranges` | 10.12ms | 5.84ms | 0.6× | 0.7% | PASS |
| file_reads | `covering_index_scan` | 11.62ms | 6.45ms | 0.6× | 1.9% | PASS |
| file_reads | `groupby_scan` | 31.49ms | 32.82ms | 1.0× | 0.6% | PASS |
| file_reads | `index_join` | 10.13ms | 9.61ms | 0.9× | 2.6% | PASS |
| file_reads | `index_join_scan` | 4.51ms | 4.75ms | 1.1× | 2.5% | PASS |
| file_reads | `types_table_scan` | 1.05s | 1.14s | 1.1× | 1.0% | PASS |
| file_reads | `table_scan` | 1.22s | 1.24s | 1.0× | 1.0% | PASS |
| file_reads | `oltp_read_only` | 206.88ms | 144.30ms | 0.7× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 195.29ms | 259.09ms | 1.3× | 1.3% | PASS |
| file_writes | `oltp_insert` | 21.81ms | 31.27ms | 1.4× | 1.3% | PASS |
| file_writes | `oltp_update_index` | 76.42ms | 98.83ms | 1.3× | 1.4% | PASS |
| file_writes | `oltp_update_non_index` | 57.12ms | 65.41ms | 1.1× | 2.4% | PASS |
| file_writes | `oltp_delete_insert` | 66.84ms | 80.89ms | 1.2× | 1.8% | PASS |
| file_writes | `oltp_write_only` | 43.99ms | 52.77ms | 1.2× | 1.7% | PASS |
| file_writes | `types_delete_insert` | 39.84ms | 42.89ms | 1.1× | 1.8% | PASS |
| file_writes | `oltp_read_write` | 90.44ms | 117.74ms | 1.3× | 1.8% | PASS |
| ac_reads | `oltp_point_select` | 47.40ms | 47.27ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_range_select` | 12.73ms | 13.01ms | 1.0× | 2.2% | PASS |
| ac_reads | `oltp_sum_range` | 12.18ms | 12.93ms | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_order_range` | 2.94ms | 3.04ms | 1.0× | 1.8% | PASS |
| ac_reads | `oltp_distinct_range` | 3.96ms | 4.12ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_index_scan` | 6.75ms | 7.39ms | 1.1× | 1.5% | PASS |
| ac_reads | `select_random_points` | 13.34ms | 12.79ms | 1.0× | 2.6% | PASS |
| ac_reads | `select_random_ranges` | 5.48ms | 5.84ms | 1.1× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 6.96ms | 6.35ms | 0.9× | 2.3% | PASS |
| ac_reads | `groupby_scan` | 30.48ms | 32.69ms | 1.1× | 1.0% | PASS |
| ac_reads | `index_join` | 7.86ms | 9.48ms | 1.2× | 2.0% | PASS |
| ac_reads | `index_join_scan` | 3.91ms | 4.71ms | 1.2× | 1.9% | PASS |
| ac_reads | `types_table_scan` | 1.04s | 1.13s | 1.1× | 0.4% | PASS |
| ac_reads | `table_scan` | 1.18s | 1.22s | 1.0× | 0.6% | PASS |
| ac_reads | `oltp_read_only` | 135.08ms | 142.82ms | 1.1× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.20ms | 76.93ms | 3.5× | 5.5% | PASS |
| ac_writes | `oltp_insert_ac` | 24.20ms | 88.19ms | 3.6× | 5.1% | PASS |
| ac_writes | `oltp_update_index_ac` | 25.31ms | 98.51ms | 3.9× | 5.9% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.75ms | 81.27ms | 3.7× | 3.7% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 23.38ms | 91.31ms | 3.9× | 4.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 23.27ms | 90.18ms | 3.9× | 3.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 20.76ms | 82.17ms | 4.0× | 5.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 28.30ms | 95.49ms | 3.4× | 4.0% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 30.88ms | 36.82ms | 1.2× | 1.7% | PASS |
| mem_reads | `oltp_range_select` | 14.85ms | 14.18ms | 1.0× | 2.2% | PASS |
| mem_reads | `oltp_sum_range` | 12.96ms | 13.60ms | 1.0× | 2.0% | PASS |
| mem_reads | `oltp_order_range` | 3.07ms | 3.09ms | 1.0× | 1.3% | PASS |
| mem_reads | `oltp_distinct_range` | 4.11ms | 4.18ms | 1.0× | 0.8% | PASS |
| mem_reads | `oltp_index_scan` | 4.72ms | 6.17ms | 1.3× | 2.2% | PASS |
| mem_reads | `select_random_points` | 18.68ms | 21.02ms | 1.1× | 2.3% | PASS |
| mem_reads | `select_random_ranges` | 4.09ms | 5.21ms | 1.3× | 1.5% | PASS |
| mem_reads | `covering_index_scan` | 4.83ms | 4.56ms | 0.9× | 3.9% | PASS |
| mem_reads | `groupby_scan` | 32.65ms | 33.76ms | 1.0× | 0.8% | PASS |
| mem_reads | `index_join` | 7.28ms | 9.23ms | 1.3× | 3.5% | PASS |
| mem_reads | `index_join_scan` | 5.05ms | 5.37ms | 1.1× | 2.9% | PASS |
| mem_reads | `types_table_scan` | 1.20s | 1.21s | 1.0× | 2.5% | PASS |
| mem_reads | `table_scan` | 1.52s | 1.32s | 0.9× | 2.0% | PASS |
| mem_reads | `oltp_read_only` | 128.76ms | 137.02ms | 1.1× | 1.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 230.16ms | 353.14ms | 1.5× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 23.44ms | 38.85ms | 1.7× | 2.4% | PASS |
| mem_writes | `oltp_update_index` | 81.94ms | 141.60ms | 1.7× | 2.7% | PASS |
| mem_writes | `oltp_update_non_index` | 53.47ms | 85.12ms | 1.6× | 1.4% | PASS |
| mem_writes | `oltp_delete_insert` | 59.57ms | 109.52ms | 1.8× | 1.3% | PASS |
| mem_writes | `oltp_write_only` | 32.65ms | 63.05ms | 1.9× | 1.3% | PASS |
| mem_writes | `types_delete_insert` | 35.88ms | 56.35ms | 1.6× | 1.4% | PASS |
| mem_writes | `oltp_read_write` | 95.30ms | 148.24ms | 1.6× | 2.3% | PASS |
| file_reads | `oltp_point_select` | 101.85ms | 56.78ms | 0.6× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 23.02ms | 16.32ms | 0.7× | 1.7% | PASS |
| file_reads | `oltp_sum_range` | 21.23ms | 15.87ms | 0.7× | 1.6% | PASS |
| file_reads | `oltp_order_range` | 4.04ms | 3.39ms | 0.8× | 1.2% | PASS |
| file_reads | `oltp_distinct_range` | 5.11ms | 4.51ms | 0.9× | 1.6% | PASS |
| file_reads | `oltp_index_scan` | 12.41ms | 8.32ms | 0.7× | 1.2% | PASS |
| file_reads | `select_random_points` | 27.44ms | 24.21ms | 0.9× | 2.0% | PASS |
| file_reads | `select_random_ranges` | 11.33ms | 7.09ms | 0.6× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 13.35ms | 6.62ms | 0.5× | 2.5% | PASS |
| file_reads | `groupby_scan` | 34.23ms | 34.43ms | 1.0× | 0.6% | PASS |
| file_reads | `index_join` | 12.10ms | 10.71ms | 0.9× | 4.8% | PASS |
| file_reads | `index_join_scan` | 6.01ms | 5.64ms | 0.9× | 2.9% | PASS |
| file_reads | `types_table_scan` | 1.21s | 1.21s | 1.0× | 4.4% | PASS |
| file_reads | `table_scan` | 1.50s | 1.31s | 0.9× | 3.6% | PASS |
| file_reads | `oltp_read_only` | 234.52ms | 164.86ms | 0.7× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 249.53ms | 366.95ms | 1.5× | 1.1% | PASS |
| file_writes | `oltp_insert` | 66.78ms | 45.96ms | 0.7× | 19.8% | PASS |
| file_writes | `oltp_update_index` | 125.47ms | 159.16ms | 1.3× | 1.7% | PASS |
| file_writes | `oltp_update_non_index` | 113.84ms | 95.45ms | 0.8× | 12.0% | PASS |
| file_writes | `oltp_delete_insert` | 100.82ms | 121.79ms | 1.2× | 2.3% | PASS |
| file_writes | `oltp_write_only` | 87.02ms | 72.95ms | 0.8× | 9.3% | PASS |
| file_writes | `types_delete_insert` | 59.50ms | 62.66ms | 1.1× | 2.2% | PASS |
| file_writes | `oltp_read_write` | 145.41ms | 156.80ms | 1.1× | 7.1% | PASS |
| ac_reads | `oltp_point_select` | 54.38ms | 55.95ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_range_select` | 17.57ms | 16.15ms | 0.9× | 2.9% | PASS |
| ac_reads | `oltp_sum_range` | 16.05ms | 15.61ms | 1.0× | 2.0% | PASS |
| ac_reads | `oltp_order_range` | 3.45ms | 3.37ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_distinct_range` | 4.48ms | 4.49ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_index_scan` | 7.56ms | 8.21ms | 1.1× | 1.2% | PASS |
| ac_reads | `select_random_points` | 22.11ms | 23.48ms | 1.1× | 1.7% | PASS |
| ac_reads | `select_random_ranges` | 6.70ms | 7.10ms | 1.1× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 8.63ms | 6.68ms | 0.8× | 2.5% | PASS |
| ac_reads | `groupby_scan` | 33.41ms | 34.41ms | 1.0× | 0.7% | PASS |
| ac_reads | `index_join` | 10.04ms | 10.92ms | 1.1× | 2.1% | PASS |
| ac_reads | `index_join_scan` | 5.68ms | 5.82ms | 1.0× | 2.7% | PASS |
| ac_reads | `types_table_scan` | 1.23s | 1.21s | 1.0× | 1.9% | PASS |
| ac_reads | `table_scan` | 1.50s | 1.31s | 0.9× | 2.6% | PASS |
| ac_reads | `oltp_read_only` | 162.91ms | 165.50ms | 1.0× | 1.1% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 32.94ms | 110.16ms | 3.3× | 10.7% | PASS |
| ac_writes | `oltp_insert_ac` | 34.78ms | 122.80ms | 3.5× | 7.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 38.98ms | 133.09ms | 3.4× | 8.5% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 34.63ms | 115.26ms | 3.3× | 14.1% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 35.16ms | 129.95ms | 3.7× | 14.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 35.20ms | 123.61ms | 3.5× | 8.0% | PASS |
| ac_writes | `types_delete_insert_ac` | 30.24ms | 117.34ms | 3.9× | 9.2% | PASS |
| ac_writes | `oltp_read_write_ac` | 43.08ms | 127.20ms | 3.0× | 8.4% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 28.79ms | 35.84ms | 1.2× | 1.1% | PASS |
| mem_reads | `oltp_range_select` | 11.45ms | 13.62ms | 1.2× | 1.2% | PASS |
| mem_reads | `oltp_sum_range` | 10.79ms | 12.89ms | 1.2× | 0.9% | PASS |
| mem_reads | `oltp_order_range` | 2.69ms | 3.02ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_distinct_range` | 3.85ms | 4.13ms | 1.1× | 1.8% | PASS |
| mem_reads | `oltp_index_scan` | 4.36ms | 5.73ms | 1.3× | 2.1% | PASS |
| mem_reads | `select_random_points` | 17.35ms | 20.10ms | 1.2× | 1.6% | PASS |
| mem_reads | `select_random_ranges` | 3.59ms | 5.02ms | 1.4× | 1.2% | PASS |
| mem_reads | `covering_index_scan` | 4.35ms | 4.17ms | 1.0× | 0.8% | PASS |
| mem_reads | `groupby_scan` | 31.92ms | 33.34ms | 1.0× | 0.4% | PASS |
| mem_reads | `index_join` | 6.59ms | 8.41ms | 1.3× | 0.9% | PASS |
| mem_reads | `index_join_scan` | 3.59ms | 5.05ms | 1.4× | 2.0% | PASS |
| mem_reads | `types_table_scan` | 1.05s | 1.18s | 1.1× | 0.3% | PASS |
| mem_reads | `table_scan` | 1.20s | 1.27s | 1.1× | 0.3% | PASS |
| mem_reads | `oltp_read_only` | 112.71ms | 131.28ms | 1.2× | 0.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 241.58ms | 344.70ms | 1.4× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 19.80ms | 37.05ms | 1.9× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 65.48ms | 124.98ms | 1.9× | 0.7% | PASS |
| mem_writes | `oltp_update_non_index` | 46.52ms | 74.37ms | 1.6× | 0.7% | PASS |
| mem_writes | `oltp_delete_insert` | 46.88ms | 97.80ms | 2.1× | 0.7% | PASS |
| mem_writes | `oltp_write_only` | 26.44ms | 57.32ms | 2.2× | 0.7% | PASS |
| mem_writes | `types_delete_insert` | 31.62ms | 49.98ms | 1.6× | 1.8% | PASS |
| mem_writes | `oltp_read_write` | 78.83ms | 137.26ms | 1.7× | 0.7% | PASS |
| file_reads | `oltp_point_select` | 99.75ms | 54.72ms | 0.5× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 20.15ms | 15.82ms | 0.8× | 2.8% | PASS |
| file_reads | `oltp_sum_range` | 19.13ms | 15.24ms | 0.8× | 1.9% | PASS |
| file_reads | `oltp_order_range` | 3.51ms | 3.28ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_distinct_range` | 4.81ms | 4.39ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 11.52ms | 7.94ms | 0.7× | 3.6% | PASS |
| file_reads | `select_random_points` | 25.99ms | 22.92ms | 0.9× | 3.0% | PASS |
| file_reads | `select_random_ranges` | 10.71ms | 6.99ms | 0.7× | 2.0% | PASS |
| file_reads | `covering_index_scan` | 12.10ms | 6.39ms | 0.5× | 2.5% | PASS |
| file_reads | `groupby_scan` | 33.47ms | 33.97ms | 1.0× | 0.8% | PASS |
| file_reads | `index_join` | 10.92ms | 10.68ms | 1.0× | 3.2% | PASS |
| file_reads | `index_join_scan` | 4.96ms | 5.82ms | 1.2× | 3.4% | PASS |
| file_reads | `types_table_scan` | 1.03s | 1.17s | 1.1× | 0.7% | PASS |
| file_reads | `table_scan` | 1.20s | 1.27s | 1.1× | 0.7% | PASS |
| file_reads | `oltp_read_only` | 216.15ms | 159.43ms | 0.7× | 1.3% | PASS |
| file_writes | `oltp_bulk_insert` | 259.96ms | 353.27ms | 1.4× | 1.3% | PASS |
| file_writes | `oltp_insert` | 31.23ms | 43.76ms | 1.4× | 1.3% | PASS |
| file_writes | `oltp_update_index` | 99.26ms | 141.91ms | 1.4× | 1.1% | PASS |
| file_writes | `oltp_update_non_index` | 76.95ms | 87.55ms | 1.1× | 1.2% | PASS |
| file_writes | `oltp_delete_insert` | 77.45ms | 110.99ms | 1.4× | 0.9% | PASS |
| file_writes | `oltp_write_only` | 53.49ms | 67.66ms | 1.3× | 1.8% | PASS |
| file_writes | `types_delete_insert` | 50.49ms | 58.87ms | 1.2× | 1.7% | PASS |
| file_writes | `oltp_read_write` | 108.87ms | 146.74ms | 1.3× | 1.9% | PASS |
| ac_reads | `oltp_point_select` | 52.98ms | 54.52ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 15.08ms | 15.81ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_sum_range` | 14.27ms | 15.16ms | 1.1× | 1.7% | PASS |
| ac_reads | `oltp_order_range` | 3.15ms | 3.28ms | 1.0× | 2.7% | PASS |
| ac_reads | `oltp_distinct_range` | 4.23ms | 4.40ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_index_scan` | 6.93ms | 7.96ms | 1.1× | 1.5% | PASS |
| ac_reads | `select_random_points` | 20.57ms | 22.95ms | 1.1× | 1.9% | PASS |
| ac_reads | `select_random_ranges` | 6.37ms | 6.96ms | 1.1× | 1.2% | PASS |
| ac_reads | `covering_index_scan` | 7.01ms | 6.42ms | 0.9× | 2.3% | PASS |
| ac_reads | `groupby_scan` | 32.35ms | 33.86ms | 1.0× | 0.8% | PASS |
| ac_reads | `index_join` | 8.31ms | 10.57ms | 1.3× | 1.9% | PASS |
| ac_reads | `index_join_scan` | 4.53ms | 5.78ms | 1.3× | 3.4% | PASS |
| ac_reads | `types_table_scan` | 1.04s | 1.16s | 1.1× | 0.9% | PASS |
| ac_reads | `table_scan` | 1.19s | 1.26s | 1.1× | 0.9% | PASS |
| ac_reads | `oltp_read_only` | 148.85ms | 158.91ms | 1.1× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 20.98ms | 70.45ms | 3.4× | 2.4% | PASS |
| ac_writes | `oltp_insert_ac` | 23.33ms | 85.82ms | 3.7× | 2.1% | PASS |
| ac_writes | `oltp_update_index_ac` | 25.84ms | 96.53ms | 3.7× | 3.4% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.52ms | 78.45ms | 3.6× | 3.4% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 22.68ms | 87.66ms | 3.9× | 1.9% | PASS |
| ac_writes | `oltp_write_only_ac` | 23.02ms | 87.39ms | 3.8× | 3.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 20.36ms | 78.99ms | 3.9× | 4.0% | PASS |
| ac_writes | `oltp_read_write_ac` | 28.84ms | 93.33ms | 3.2× | 4.0% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 34.23ms | 36.07ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_range_select` | 20.28ms | 20.93ms | 1.0× | 1.7% | PASS |
| mem_reads | `oltp_sum_range` | 18.22ms | 19.98ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_order_range` | 3.69ms | 3.86ms | 1.0× | 0.9% | PASS |
| mem_reads | `oltp_distinct_range` | 4.76ms | 5.05ms | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_index_scan` | 4.68ms | 5.65ms | 1.2× | 1.4% | PASS |
| mem_reads | `select_random_points` | 27.21ms | 30.01ms | 1.1× | 0.7% | PASS |
| mem_reads | `select_random_ranges` | 7.46ms | 8.16ms | 1.1× | 1.1% | PASS |
| mem_reads | `covering_index_scan` | 4.33ms | 4.10ms | 0.9× | 1.2% | PASS |
| mem_reads | `groupby_scan` | 38.44ms | 41.79ms | 1.1× | 0.9% | PASS |
| mem_reads | `index_join` | 8.11ms | 10.02ms | 1.2× | 1.0% | PASS |
| mem_reads | `index_join_scan` | 4.26ms | 5.48ms | 1.3× | 1.5% | PASS |
| mem_reads | `types_table_scan` | 1.12s | 1.24s | 1.1× | 0.8% | PASS |
| mem_reads | `table_scan` | 1.29s | 1.36s | 1.1× | 0.7% | PASS |
| mem_reads | `oltp_read_only` | 149.00ms | 162.26ms | 1.1× | 0.9% | PASS |
| mem_writes | `oltp_bulk_insert` | 244.48ms | 331.69ms | 1.4× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 19.48ms | 34.49ms | 1.8× | 0.5% | PASS |
| mem_writes | `oltp_update_index` | 68.19ms | 118.48ms | 1.7× | 0.8% | PASS |
| mem_writes | `oltp_update_non_index` | 51.57ms | 75.12ms | 1.5× | 0.8% | PASS |
| mem_writes | `oltp_delete_insert` | 50.29ms | 94.52ms | 1.9× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 27.27ms | 57.20ms | 2.1× | 0.9% | PASS |
| mem_writes | `types_delete_insert` | 32.42ms | 50.99ms | 1.6× | 0.9% | PASS |
| mem_writes | `oltp_read_write` | 97.77ms | 150.61ms | 1.5× | 1.0% | PASS |
| file_reads | `oltp_point_select` | 117.27ms | 57.41ms | 0.5× | 0.5% | PASS |
| file_reads | `oltp_range_select` | 28.67ms | 23.10ms | 0.8× | 1.1% | PASS |
| file_reads | `oltp_sum_range` | 27.08ms | 22.39ms | 0.8× | 0.9% | PASS |
| file_reads | `oltp_order_range` | 4.65ms | 4.14ms | 0.9× | 1.1% | PASS |
| file_reads | `oltp_distinct_range` | 5.68ms | 5.33ms | 0.9× | 0.8% | PASS |
| file_reads | `oltp_index_scan` | 13.41ms | 7.96ms | 0.6× | 1.3% | PASS |
| file_reads | `select_random_points` | 36.44ms | 32.93ms | 0.9× | 1.0% | PASS |
| file_reads | `select_random_ranges` | 16.21ms | 10.59ms | 0.7× | 0.8% | PASS |
| file_reads | `covering_index_scan` | 13.09ms | 6.47ms | 0.5× | 1.4% | PASS |
| file_reads | `groupby_scan` | 39.49ms | 42.17ms | 1.1× | 0.7% | PASS |
| file_reads | `index_join` | 12.97ms | 11.80ms | 0.9× | 1.4% | PASS |
| file_reads | `index_join_scan` | 5.30ms | 5.93ms | 1.1× | 1.5% | PASS |
| file_reads | `types_table_scan` | 1.13s | 1.25s | 1.1× | 0.8% | PASS |
| file_reads | `table_scan` | 1.35s | 1.38s | 1.0× | 1.8% | PASS |
| file_reads | `oltp_read_only` | 276.82ms | 195.59ms | 0.7× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 260.57ms | 341.32ms | 1.3× | 0.8% | PASS |
| file_writes | `oltp_insert` | 26.24ms | 40.09ms | 1.5× | 1.2% | PASS |
| file_writes | `oltp_update_index` | 97.38ms | 131.53ms | 1.4× | 1.3% | PASS |
| file_writes | `oltp_update_non_index` | 77.16ms | 89.67ms | 1.2× | 1.2% | PASS |
| file_writes | `oltp_delete_insert` | 76.90ms | 106.09ms | 1.4× | 1.6% | PASS |
| file_writes | `oltp_write_only` | 49.86ms | 67.72ms | 1.4× | 2.0% | PASS |
| file_writes | `types_delete_insert` | 49.05ms | 56.66ms | 1.2× | 1.3% | PASS |
| file_writes | `oltp_read_write` | 121.60ms | 160.40ms | 1.3× | 1.0% | PASS |
| ac_reads | `oltp_point_select` | 61.18ms | 57.61ms | 0.9× | 0.9% | PASS |
| ac_reads | `oltp_range_select` | 22.65ms | 23.06ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_sum_range` | 21.12ms | 22.38ms | 1.1× | 1.0% | PASS |
| ac_reads | `oltp_order_range` | 4.04ms | 4.11ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_distinct_range` | 5.08ms | 5.31ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_index_scan` | 7.64ms | 7.92ms | 1.0× | 0.9% | PASS |
| ac_reads | `select_random_points` | 30.69ms | 33.05ms | 1.1× | 1.1% | PASS |
| ac_reads | `select_random_ranges` | 10.46ms | 10.55ms | 1.0× | 1.2% | PASS |
| ac_reads | `covering_index_scan` | 7.36ms | 6.46ms | 0.9× | 1.4% | PASS |
| ac_reads | `groupby_scan` | 38.61ms | 42.25ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 9.99ms | 11.76ms | 1.2× | 1.5% | PASS |
| ac_reads | `index_join_scan` | 4.72ms | 6.04ms | 1.3× | 2.3% | PASS |
| ac_reads | `types_table_scan` | 1.12s | 1.25s | 1.1× | 0.6% | PASS |
| ac_reads | `table_scan` | 1.30s | 1.36s | 1.0× | 0.4% | PASS |
| ac_reads | `oltp_read_only` | 190.20ms | 193.69ms | 1.0× | 0.6% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 15.74ms | 57.88ms | 3.7× | 5.2% | PASS |
| ac_writes | `oltp_insert_ac` | 17.93ms | 73.23ms | 4.1× | 4.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 20.16ms | 83.07ms | 4.1× | 4.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 16.15ms | 65.84ms | 4.1× | 5.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 17.71ms | 78.02ms | 4.4× | 4.4% | PASS |
| ac_writes | `oltp_write_only_ac` | 18.57ms | 76.50ms | 4.1× | 5.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 15.76ms | 64.82ms | 4.1× | 5.1% | PASS |
| ac_writes | `oltp_read_write_ac` | 25.97ms | 84.38ms | 3.2× | 5.0% | PASS |

</details>

</details>

## Version-control latency

Wall time: 1m 32s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 48.96ms | 130.00ms | 37.7% | 1.7% | PASS |
| `status_dirty_many_tables` | 50.18ms | 130.00ms | 38.6% | 0.9% | PASS |
| `diff_regular_working_one_table` | 46.40ms | 120.00ms | 38.7% | 0.4% | PASS |
| `diff_regular_working_many_tables` | 51.86ms | 140.00ms | 37.0% | 0.5% | PASS |
| `diff_stat_working_many_tables` | 51.80ms | 140.00ms | 37.0% | 0.4% | PASS |
| `diff_schema_working_many_tables` | 52.20ms | 140.00ms | 37.3% | 0.5% | PASS |
| `branch_list_many_branches` | 14.39ms | 35.00ms | 41.1% | 0.6% | PASS |
| `branch_create_delete` | 16.71ms | 40.00ms | 41.8% | 2.1% | PASS |
| `checkout_branch_clean` | 72.25ms | 150.00ms | 48.2% | 1.6% | PASS |
| `merge_data_no_conflicts` | 26.75ms | 50.00ms | 53.5% | 8.5% | PASS |
| `merge_schema_no_conflicts` | 16.10ms | 35.00ms | 46.0% | 8.5% | PASS |
| `merge_data_conflicts` | 69.53ms | 180.00ms | 38.6% | 1.3% | PASS |
| `merge_data_conflicts_with_resolve` | 69.88ms | 180.00ms | 38.8% | 0.9% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
