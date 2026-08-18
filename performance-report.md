# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-08-18 11:15 UTC
>
> Commit: [`34b4712c56fef33841f0dbcccc21684265f04225`](https://github.com/dolthub/doltlite/commit/34b4712c56fef33841f0dbcccc21684265f04225)
>
> Runner: ubuntu24 20260810.271.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/32122998638)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.24s | 10.56s | 1.0× | 1.6% | **PASS** |
| Writes | 1.96s | 3.06s | 1.6× | 1.4% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.95s | 10.77s | 1.0× | 1.3% | **PASS** |
| Writes | 3.61s | 3.98s | 1.1× | 2.5% | **PASS** |
| Autocommit writes | 956.99ms | 3.16s | 3.3× | 9.0% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.65s | 2.73s | 1.0× | 1.4% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.41s | 2.56s | 1.1× | 1.3% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.81s | 2.92s | 1.0× | 2.4% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.36s | 2.36s | 1.0× | 1.6% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 450.59ms | 686.15ms | 1.5× | 1.3% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 450.13ms | 733.63ms | 1.6× | 1.6% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 583.52ms | 948.14ms | 1.6× | 1.9% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 474.94ms | 694.53ms | 1.5× | 1.3% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.87s | 2.79s | 1.0× | 1.1% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.50s | 2.59s | 1.0× | 2.4% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 3.14s | 3.00s | 1.0× | 2.3% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.44s | 2.39s | 1.0× | 1.0% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 596.87ms | 730.07ms | 1.2× | 1.4% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.21s | 1.19s | 1.0× | 7.7% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 788.79ms | 1.03s | 1.3× | 1.8% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 1.02s | 1.03s | 1.0× | 7.8% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.71s | 2.79s | 1.0× | 1.3% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.41s | 2.56s | 1.1× | 2.6% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 3.16s | 3.06s | 1.0× | 1.7% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.23s | 2.36s | 1.1× | 0.9% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 140.07ms | 553.31ms | 4.0× | 3.5% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 374.56ms | 1.16s | 3.1× | 28.7% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 158.84ms | 603.43ms | 3.8× | 3.5% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 283.52ms | 840.15ms | 3.0× | 31.3% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.42ms | 26.63ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_range_select` | 10.58ms | 10.89ms | 1.0× | 1.7% | PASS |
| mem_reads | `oltp_sum_range` | 9.65ms | 10.61ms | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_order_range` | 2.64ms | 2.81ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 3.74ms | 3.92ms | 1.0× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 3.98ms | 4.67ms | 1.2× | 1.2% | PASS |
| mem_reads | `select_random_points` | 10.47ms | 10.69ms | 1.0× | 1.4% | PASS |
| mem_reads | `select_random_ranges` | 3.13ms | 3.94ms | 1.3× | 1.5% | PASS |
| mem_reads | `covering_index_scan` | 4.34ms | 4.04ms | 0.9× | 1.5% | PASS |
| mem_reads | `groupby_scan` | 31.93ms | 34.15ms | 1.1× | 0.9% | PASS |
| mem_reads | `index_join` | 5.95ms | 7.70ms | 1.3× | 1.7% | PASS |
| mem_reads | `index_join_scan` | 3.50ms | 4.53ms | 1.3× | 1.5% | PASS |
| mem_reads | `types_table_scan` | 1.11s | 1.18s | 1.1× | 0.7% | PASS |
| mem_reads | `table_scan` | 1.32s | 1.31s | 1.0× | 2.2% | PASS |
| mem_reads | `oltp_read_only` | 108.23ms | 113.26ms | 1.0× | 1.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 180.82ms | 239.78ms | 1.3× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 15.85ms | 27.41ms | 1.7× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 56.40ms | 99.02ms | 1.8× | 2.2% | PASS |
| mem_writes | `oltp_update_non_index` | 39.22ms | 57.88ms | 1.5× | 1.9% | PASS |
| mem_writes | `oltp_delete_insert` | 46.28ms | 74.07ms | 1.6× | 2.3% | PASS |
| mem_writes | `oltp_write_only` | 22.08ms | 46.85ms | 2.1× | 1.0% | PASS |
| mem_writes | `types_delete_insert` | 24.67ms | 35.87ms | 1.5× | 1.2% | PASS |
| mem_writes | `oltp_read_write` | 65.27ms | 105.27ms | 1.6× | 1.4% | PASS |
| file_reads | `oltp_point_select` | 108.13ms | 47.82ms | 0.4× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 19.18ms | 13.04ms | 0.7× | 1.1% | PASS |
| file_reads | `oltp_sum_range` | 18.34ms | 12.90ms | 0.7× | 1.0% | PASS |
| file_reads | `oltp_order_range` | 3.58ms | 3.08ms | 0.9× | 0.9% | PASS |
| file_reads | `oltp_distinct_range` | 4.65ms | 4.14ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 12.63ms | 7.04ms | 0.6× | 1.3% | PASS |
| file_reads | `select_random_points` | 19.46ms | 13.04ms | 0.7× | 1.2% | PASS |
| file_reads | `select_random_ranges` | 11.48ms | 6.08ms | 0.5× | 0.6% | PASS |
| file_reads | `covering_index_scan` | 13.01ms | 6.31ms | 0.5× | 1.3% | PASS |
| file_reads | `groupby_scan` | 32.96ms | 34.48ms | 1.0× | 0.7% | PASS |
| file_reads | `index_join` | 10.63ms | 9.08ms | 0.9× | 1.7% | PASS |
| file_reads | `index_join_scan` | 4.50ms | 4.84ms | 1.1× | 1.5% | PASS |
| file_reads | `types_table_scan` | 1.12s | 1.19s | 1.1× | 1.0% | PASS |
| file_reads | `table_scan` | 1.27s | 1.30s | 1.0× | 1.1% | PASS |
| file_reads | `oltp_read_only` | 221.86ms | 141.91ms | 0.6× | 0.6% | PASS |
| file_writes | `oltp_bulk_insert` | 195.30ms | 247.35ms | 1.3× | 1.0% | PASS |
| file_writes | `oltp_insert` | 22.00ms | 31.09ms | 1.4× | 1.2% | PASS |
| file_writes | `oltp_update_index` | 78.77ms | 98.93ms | 1.3× | 1.1% | PASS |
| file_writes | `oltp_update_non_index` | 59.69ms | 65.82ms | 1.1× | 2.2% | PASS |
| file_writes | `oltp_delete_insert` | 67.39ms | 79.91ms | 1.2× | 2.0% | PASS |
| file_writes | `oltp_write_only` | 43.98ms | 53.32ms | 1.2× | 2.4% | PASS |
| file_writes | `types_delete_insert` | 40.38ms | 41.38ms | 1.0× | 1.3% | PASS |
| file_writes | `oltp_read_write` | 89.35ms | 112.28ms | 1.3× | 1.5% | PASS |
| ac_reads | `oltp_point_select` | 52.71ms | 47.86ms | 0.9× | 0.9% | PASS |
| ac_reads | `oltp_range_select` | 14.24ms | 13.09ms | 0.9× | 1.4% | PASS |
| ac_reads | `oltp_sum_range` | 13.22ms | 12.96ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_order_range` | 3.09ms | 3.11ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_distinct_range` | 4.11ms | 4.17ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_index_scan` | 7.02ms | 7.14ms | 1.0× | 1.3% | PASS |
| ac_reads | `select_random_points` | 13.66ms | 13.02ms | 1.0× | 1.9% | PASS |
| ac_reads | `select_random_ranges` | 5.95ms | 6.13ms | 1.0× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 7.41ms | 6.59ms | 0.9× | 1.6% | PASS |
| ac_reads | `groupby_scan` | 32.46ms | 34.41ms | 1.1× | 0.7% | PASS |
| ac_reads | `index_join` | 7.85ms | 9.17ms | 1.2× | 1.5% | PASS |
| ac_reads | `index_join_scan` | 4.02ms | 4.84ms | 1.2× | 1.7% | PASS |
| ac_reads | `types_table_scan` | 1.11s | 1.18s | 1.1× | 0.9% | PASS |
| ac_reads | `table_scan` | 1.29s | 1.30s | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_read_only` | 143.38ms | 142.36ms | 1.0× | 0.7% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 14.91ms | 55.38ms | 3.7× | 4.1% | PASS |
| ac_writes | `oltp_insert_ac` | 17.39ms | 68.41ms | 3.9× | 3.2% | PASS |
| ac_writes | `oltp_update_index_ac` | 18.72ms | 80.09ms | 4.3× | 1.8% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 16.19ms | 63.07ms | 3.9× | 3.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 17.40ms | 72.34ms | 4.2× | 3.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 17.07ms | 71.41ms | 4.2× | 3.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 15.12ms | 63.08ms | 4.2× | 4.2% | PASS |
| ac_writes | `oltp_read_write_ac` | 23.27ms | 79.53ms | 3.4× | 3.6% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.66ms | 28.11ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_range_select` | 11.50ms | 11.69ms | 1.0× | 1.7% | PASS |
| mem_reads | `oltp_sum_range` | 11.03ms | 11.04ms | 1.0× | 1.3% | PASS |
| mem_reads | `oltp_order_range` | 2.68ms | 2.78ms | 1.0× | 1.3% | PASS |
| mem_reads | `oltp_distinct_range` | 3.50ms | 3.70ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 3.76ms | 4.46ms | 1.2× | 1.6% | PASS |
| mem_reads | `select_random_points` | 15.17ms | 17.33ms | 1.1× | 1.6% | PASS |
| mem_reads | `select_random_ranges` | 3.18ms | 4.05ms | 1.3× | 1.7% | PASS |
| mem_reads | `covering_index_scan` | 3.68ms | 3.36ms | 0.9× | 1.0% | PASS |
| mem_reads | `groupby_scan` | 30.10ms | 30.90ms | 1.0× | 1.8% | PASS |
| mem_reads | `index_join` | 6.29ms | 7.71ms | 1.2× | 1.9% | PASS |
| mem_reads | `index_join_scan` | 3.99ms | 4.84ms | 1.2× | 3.5% | PASS |
| mem_reads | `types_table_scan` | 1.01s | 1.10s | 1.1× | 0.5% | PASS |
| mem_reads | `table_scan` | 1.18s | 1.22s | 1.0× | 0.4% | PASS |
| mem_reads | `oltp_read_only` | 103.45ms | 114.96ms | 1.1× | 1.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 178.92ms | 263.34ms | 1.5× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 17.86ms | 29.03ms | 1.6× | 1.8% | PASS |
| mem_writes | `oltp_update_index` | 58.12ms | 105.84ms | 1.8× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 38.72ms | 61.02ms | 1.6× | 1.5% | PASS |
| mem_writes | `oltp_delete_insert` | 40.77ms | 79.36ms | 1.9× | 1.6% | PASS |
| mem_writes | `oltp_write_only` | 23.00ms | 45.05ms | 2.0× | 2.0% | PASS |
| mem_writes | `types_delete_insert` | 26.02ms | 39.52ms | 1.5× | 1.6% | PASS |
| mem_writes | `oltp_read_write` | 66.73ms | 110.48ms | 1.7× | 1.0% | PASS |
| file_reads | `oltp_point_select` | 52.42ms | 35.20ms | 0.7× | 1.5% | PASS |
| file_reads | `oltp_range_select` | 14.48ms | 12.69ms | 0.9× | 2.7% | PASS |
| file_reads | `oltp_sum_range` | 14.30ms | 12.31ms | 0.9× | 2.4% | PASS |
| file_reads | `oltp_order_range` | 3.15ms | 3.02ms | 1.0× | 2.4% | PASS |
| file_reads | `oltp_distinct_range` | 4.07ms | 4.01ms | 1.0× | 2.1% | PASS |
| file_reads | `oltp_index_scan` | 7.03ms | 5.70ms | 0.8× | 3.3% | PASS |
| file_reads | `select_random_points` | 19.35ms | 19.60ms | 1.0× | 2.1% | PASS |
| file_reads | `select_random_ranges` | 6.38ms | 5.14ms | 0.8× | 2.6% | PASS |
| file_reads | `covering_index_scan` | 6.90ms | 4.47ms | 0.6× | 3.4% | PASS |
| file_reads | `groupby_scan` | 31.17ms | 31.60ms | 1.0× | 1.5% | PASS |
| file_reads | `index_join` | 8.04ms | 8.55ms | 1.1× | 3.3% | PASS |
| file_reads | `index_join_scan` | 4.53ms | 5.16ms | 1.1× | 2.8% | PASS |
| file_reads | `types_table_scan` | 1.01s | 1.11s | 1.1× | 0.7% | PASS |
| file_reads | `table_scan` | 1.18s | 1.21s | 1.0× | 0.5% | PASS |
| file_reads | `oltp_read_only` | 141.54ms | 123.65ms | 0.9× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 249.74ms | 339.27ms | 1.4× | 4.0% | PASS |
| file_writes | `oltp_insert` | 60.68ms | 54.45ms | 0.9× | 7.2% | PASS |
| file_writes | `oltp_update_index` | 201.26ms | 189.80ms | 0.9× | 4.3% | PASS |
| file_writes | `oltp_update_non_index` | 155.01ms | 120.78ms | 0.8× | 10.5% | PASS |
| file_writes | `oltp_delete_insert` | 158.02ms | 148.15ms | 0.9× | 8.1% | PASS |
| file_writes | `oltp_write_only` | 121.08ms | 100.09ms | 0.8× | 11.9% | PASS |
| file_writes | `types_delete_insert` | 99.93ms | 73.27ms | 0.7× | 14.6% | PASS |
| file_writes | `oltp_read_write` | 160.76ms | 159.37ms | 1.0× | 5.4% | PASS |
| ac_reads | `oltp_point_select` | 33.01ms | 34.60ms | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_range_select` | 12.09ms | 12.41ms | 1.0× | 2.9% | PASS |
| ac_reads | `oltp_sum_range` | 11.93ms | 11.87ms | 1.0× | 3.4% | PASS |
| ac_reads | `oltp_order_range` | 2.82ms | 2.93ms | 1.0× | 2.3% | PASS |
| ac_reads | `oltp_distinct_range` | 3.83ms | 3.96ms | 1.0× | 2.0% | PASS |
| ac_reads | `oltp_index_scan` | 5.10ms | 5.57ms | 1.1× | 3.5% | PASS |
| ac_reads | `select_random_points` | 17.48ms | 19.32ms | 1.1× | 2.7% | PASS |
| ac_reads | `select_random_ranges` | 4.44ms | 5.09ms | 1.1× | 2.6% | PASS |
| ac_reads | `covering_index_scan` | 5.05ms | 4.33ms | 0.9× | 5.3% | PASS |
| ac_reads | `groupby_scan` | 30.47ms | 31.16ms | 1.0× | 1.0% | PASS |
| ac_reads | `index_join` | 7.24ms | 8.41ms | 1.2× | 3.1% | PASS |
| ac_reads | `index_join_scan` | 4.31ms | 5.04ms | 1.2× | 2.8% | PASS |
| ac_reads | `types_table_scan` | 991.65ms | 1.09s | 1.1× | 0.6% | PASS |
| ac_reads | `table_scan` | 1.17s | 1.20s | 1.0× | 0.5% | PASS |
| ac_reads | `oltp_read_only` | 114.15ms | 122.32ms | 1.1× | 0.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 38.68ms | 100.84ms | 2.6× | 24.6% | PASS |
| ac_writes | `oltp_insert_ac` | 57.89ms | 181.56ms | 3.1× | 45.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 40.74ms | 111.57ms | 2.7× | 12.4% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 38.52ms | 105.74ms | 2.7× | 15.5% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 58.83ms | 238.40ms | 4.1× | 50.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 41.79ms | 126.19ms | 3.0× | 23.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 39.64ms | 127.35ms | 3.2× | 32.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 58.45ms | 166.80ms | 2.9× | 48.7% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 32.01ms | 35.20ms | 1.1× | 1.8% | PASS |
| mem_reads | `oltp_range_select` | 14.39ms | 13.73ms | 1.0× | 3.2% | PASS |
| mem_reads | `oltp_sum_range` | 13.13ms | 13.29ms | 1.0× | 2.8% | PASS |
| mem_reads | `oltp_order_range` | 3.16ms | 3.21ms | 1.0× | 1.6% | PASS |
| mem_reads | `oltp_distinct_range` | 4.24ms | 4.32ms | 1.0× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 4.79ms | 5.99ms | 1.3× | 2.4% | PASS |
| mem_reads | `select_random_points` | 18.53ms | 20.76ms | 1.1× | 2.5% | PASS |
| mem_reads | `select_random_ranges` | 4.43ms | 5.38ms | 1.2× | 1.4% | PASS |
| mem_reads | `covering_index_scan` | 4.62ms | 4.43ms | 1.0× | 3.0% | PASS |
| mem_reads | `groupby_scan` | 34.01ms | 35.92ms | 1.1× | 1.0% | PASS |
| mem_reads | `index_join` | 7.01ms | 8.91ms | 1.3× | 3.2% | PASS |
| mem_reads | `index_join_scan` | 4.72ms | 5.87ms | 1.2× | 3.9% | PASS |
| mem_reads | `types_table_scan` | 1.16s | 1.26s | 1.1× | 2.0% | PASS |
| mem_reads | `table_scan` | 1.38s | 1.37s | 1.0× | 3.1% | PASS |
| mem_reads | `oltp_read_only` | 120.57ms | 129.79ms | 1.1× | 1.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 237.19ms | 333.57ms | 1.4× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 20.71ms | 38.10ms | 1.8× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 74.50ms | 137.97ms | 1.9× | 2.1% | PASS |
| mem_writes | `oltp_update_non_index` | 50.57ms | 78.34ms | 1.5× | 1.0% | PASS |
| mem_writes | `oltp_delete_insert` | 52.45ms | 106.27ms | 2.0× | 1.9% | PASS |
| mem_writes | `oltp_write_only` | 29.49ms | 63.10ms | 2.1× | 2.1% | PASS |
| mem_writes | `types_delete_insert` | 32.89ms | 51.79ms | 1.6× | 1.8% | PASS |
| mem_writes | `oltp_read_write` | 85.72ms | 138.99ms | 1.6× | 2.9% | PASS |
| file_reads | `oltp_point_select` | 115.16ms | 56.02ms | 0.5× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 23.62ms | 16.05ms | 0.7× | 2.3% | PASS |
| file_reads | `oltp_sum_range` | 22.12ms | 15.64ms | 0.7× | 2.5% | PASS |
| file_reads | `oltp_order_range` | 4.05ms | 3.47ms | 0.9× | 2.2% | PASS |
| file_reads | `oltp_distinct_range` | 5.16ms | 4.54ms | 0.9× | 1.2% | PASS |
| file_reads | `oltp_index_scan` | 13.70ms | 8.36ms | 0.6× | 1.3% | PASS |
| file_reads | `select_random_points` | 28.86ms | 23.43ms | 0.8× | 2.3% | PASS |
| file_reads | `select_random_ranges` | 13.00ms | 7.56ms | 0.6× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 14.12ms | 7.01ms | 0.5× | 2.3% | PASS |
| file_reads | `groupby_scan` | 35.67ms | 36.28ms | 1.0× | 0.9% | PASS |
| file_reads | `index_join` | 12.35ms | 11.28ms | 0.9× | 2.6% | PASS |
| file_reads | `index_join_scan` | 5.77ms | 6.27ms | 1.1× | 3.6% | PASS |
| file_reads | `types_table_scan` | 1.19s | 1.26s | 1.1× | 3.3% | PASS |
| file_reads | `table_scan` | 1.41s | 1.38s | 1.0× | 3.8% | PASS |
| file_reads | `oltp_read_only` | 249.35ms | 163.89ms | 0.7× | 1.4% | PASS |
| file_writes | `oltp_bulk_insert` | 260.30ms | 342.89ms | 1.3× | 1.2% | PASS |
| file_writes | `oltp_insert` | 31.78ms | 45.12ms | 1.4× | 1.7% | PASS |
| file_writes | `oltp_update_index` | 108.14ms | 152.94ms | 1.4× | 1.8% | PASS |
| file_writes | `oltp_update_non_index` | 80.57ms | 91.22ms | 1.1× | 2.0% | PASS |
| file_writes | `oltp_delete_insert` | 82.44ms | 117.99ms | 1.4× | 1.6% | PASS |
| file_writes | `oltp_write_only` | 56.80ms | 73.28ms | 1.3× | 2.4% | PASS |
| file_writes | `types_delete_insert` | 53.04ms | 61.08ms | 1.2× | 1.8% | PASS |
| file_writes | `oltp_read_write` | 115.73ms | 149.93ms | 1.3× | 2.3% | PASS |
| ac_reads | `oltp_point_select` | 59.18ms | 56.05ms | 0.9× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 18.17ms | 16.03ms | 0.9× | 2.8% | PASS |
| ac_reads | `oltp_sum_range` | 16.92ms | 15.67ms | 0.9× | 1.7% | PASS |
| ac_reads | `oltp_order_range` | 3.64ms | 3.50ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_distinct_range` | 4.68ms | 4.54ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_index_scan` | 8.15ms | 8.33ms | 1.0× | 1.6% | PASS |
| ac_reads | `select_random_points` | 22.41ms | 22.78ms | 1.0× | 1.9% | PASS |
| ac_reads | `select_random_ranges` | 7.48ms | 7.59ms | 1.0× | 1.2% | PASS |
| ac_reads | `covering_index_scan` | 8.67ms | 7.02ms | 0.8× | 1.5% | PASS |
| ac_reads | `groupby_scan` | 35.12ms | 36.23ms | 1.0× | 0.8% | PASS |
| ac_reads | `index_join` | 9.73ms | 11.37ms | 1.2× | 1.8% | PASS |
| ac_reads | `index_join_scan` | 5.24ms | 6.26ms | 1.2× | 2.3% | PASS |
| ac_reads | `types_table_scan` | 1.24s | 1.29s | 1.0× | 3.6% | PASS |
| ac_reads | `table_scan` | 1.55s | 1.41s | 0.9× | 2.2% | PASS |
| ac_reads | `oltp_read_only` | 165.08ms | 161.15ms | 1.0× | 2.1% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 17.36ms | 59.49ms | 3.4× | 3.4% | PASS |
| ac_writes | `oltp_insert_ac` | 19.78ms | 75.87ms | 3.8× | 4.7% | PASS |
| ac_writes | `oltp_update_index_ac` | 21.38ms | 85.24ms | 4.0× | 3.5% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 17.80ms | 68.66ms | 3.9× | 3.9% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 19.46ms | 78.67ms | 4.0× | 2.8% | PASS |
| ac_writes | `oltp_write_only_ac` | 19.20ms | 76.66ms | 4.0× | 3.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 17.49ms | 72.54ms | 4.1× | 5.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 26.37ms | 86.30ms | 3.3× | 3.5% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 26.87ms | 28.79ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_range_select` | 15.58ms | 16.09ms | 1.0× | 1.6% | PASS |
| mem_reads | `oltp_sum_range` | 14.42ms | 15.55ms | 1.1× | 1.7% | PASS |
| mem_reads | `oltp_order_range` | 2.97ms | 3.03ms | 1.0× | 1.6% | PASS |
| mem_reads | `oltp_distinct_range` | 3.86ms | 3.95ms | 1.0× | 0.7% | PASS |
| mem_reads | `oltp_index_scan` | 3.85ms | 4.72ms | 1.2× | 2.5% | PASS |
| mem_reads | `select_random_points` | 22.21ms | 24.18ms | 1.1× | 2.4% | PASS |
| mem_reads | `select_random_ranges` | 6.03ms | 6.50ms | 1.1× | 1.6% | PASS |
| mem_reads | `covering_index_scan` | 3.39ms | 3.21ms | 0.9× | 2.0% | PASS |
| mem_reads | `groupby_scan` | 30.91ms | 32.25ms | 1.0× | 0.8% | PASS |
| mem_reads | `index_join` | 6.44ms | 7.91ms | 1.2× | 1.9% | PASS |
| mem_reads | `index_join_scan` | 3.46ms | 4.53ms | 1.3× | 1.6% | PASS |
| mem_reads | `types_table_scan` | 916.13ms | 980.85ms | 1.1× | 1.6% | PASS |
| mem_reads | `table_scan` | 1.18s | 1.10s | 0.9× | 2.8% | PASS |
| mem_reads | `oltp_read_only` | 123.96ms | 128.03ms | 1.0× | 2.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 191.14ms | 253.18ms | 1.3× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 15.38ms | 25.87ms | 1.7× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 56.60ms | 92.72ms | 1.6× | 1.4% | PASS |
| mem_writes | `oltp_update_non_index` | 43.59ms | 57.87ms | 1.3× | 1.2% | PASS |
| mem_writes | `oltp_delete_insert` | 40.63ms | 70.80ms | 1.7× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 22.21ms | 41.65ms | 1.9× | 1.7% | PASS |
| mem_writes | `types_delete_insert` | 26.28ms | 37.66ms | 1.4× | 1.4% | PASS |
| mem_writes | `oltp_read_write` | 79.11ms | 114.80ms | 1.5× | 1.6% | PASS |
| file_reads | `oltp_point_select` | 92.23ms | 45.41ms | 0.5× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 22.95ms | 17.90ms | 0.8× | 1.4% | PASS |
| file_reads | `oltp_sum_range` | 21.53ms | 17.39ms | 0.8× | 1.4% | PASS |
| file_reads | `oltp_order_range` | 3.72ms | 3.27ms | 0.9× | 1.2% | PASS |
| file_reads | `oltp_distinct_range` | 4.58ms | 4.14ms | 0.9× | 0.8% | PASS |
| file_reads | `oltp_index_scan` | 10.63ms | 6.55ms | 0.6× | 0.8% | PASS |
| file_reads | `select_random_points` | 29.20ms | 25.94ms | 0.9× | 0.9% | PASS |
| file_reads | `select_random_ranges` | 12.89ms | 8.32ms | 0.6× | 0.8% | PASS |
| file_reads | `covering_index_scan` | 10.41ms | 5.43ms | 0.5× | 0.7% | PASS |
| file_reads | `groupby_scan` | 31.75ms | 32.46ms | 1.0× | 0.8% | PASS |
| file_reads | `index_join` | 10.25ms | 9.62ms | 0.9× | 1.0% | PASS |
| file_reads | `index_join_scan` | 4.21ms | 4.89ms | 1.2× | 1.2% | PASS |
| file_reads | `types_table_scan` | 927.24ms | 991.77ms | 1.1× | 3.5% | PASS |
| file_reads | `table_scan` | 1.04s | 1.06s | 1.0× | 2.0% | PASS |
| file_reads | `oltp_read_only` | 216.37ms | 151.03ms | 0.7× | 1.3% | PASS |
| file_writes | `oltp_bulk_insert` | 238.98ms | 298.16ms | 1.2× | 2.6% | PASS |
| file_writes | `oltp_insert` | 27.54ms | 38.96ms | 1.4× | 6.7% | PASS |
| file_writes | `oltp_update_index` | 159.40ms | 148.44ms | 0.9× | 14.7% | PASS |
| file_writes | `oltp_update_non_index` | 135.81ms | 105.68ms | 0.8× | 5.3% | PASS |
| file_writes | `oltp_delete_insert` | 138.42ms | 118.94ms | 0.9× | 8.8% | PASS |
| file_writes | `oltp_write_only` | 91.94ms | 88.75ms | 1.0× | 9.8% | PASS |
| file_writes | `types_delete_insert` | 79.85ms | 71.09ms | 0.9× | 11.0% | PASS |
| file_writes | `oltp_read_write` | 149.70ms | 158.51ms | 1.1× | 5.9% | PASS |
| ac_reads | `oltp_point_select` | 48.49ms | 45.25ms | 0.9× | 0.7% | PASS |
| ac_reads | `oltp_range_select` | 18.32ms | 17.92ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_sum_range` | 17.00ms | 17.29ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_order_range` | 3.35ms | 3.26ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_distinct_range` | 4.19ms | 4.13ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_index_scan` | 6.34ms | 6.55ms | 1.0× | 0.8% | PASS |
| ac_reads | `select_random_points` | 24.54ms | 25.78ms | 1.1× | 0.9% | PASS |
| ac_reads | `select_random_ranges` | 8.45ms | 8.29ms | 1.0× | 0.9% | PASS |
| ac_reads | `covering_index_scan` | 6.06ms | 5.44ms | 0.9× | 1.1% | PASS |
| ac_reads | `groupby_scan` | 31.25ms | 32.60ms | 1.0× | 1.0% | PASS |
| ac_reads | `index_join` | 8.12ms | 9.72ms | 1.2× | 1.0% | PASS |
| ac_reads | `index_join_scan` | 3.83ms | 4.87ms | 1.3× | 0.8% | PASS |
| ac_reads | `types_table_scan` | 877.03ms | 968.35ms | 1.1× | 1.0% | PASS |
| ac_reads | `table_scan` | 1.02s | 1.06s | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_read_only` | 150.50ms | 150.50ms | 1.0× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 29.47ms | 78.76ms | 2.7× | 20.1% | PASS |
| ac_writes | `oltp_insert_ac` | 39.02ms | 120.27ms | 3.1× | 24.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 37.22ms | 114.31ms | 3.1× | 34.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 32.23ms | 117.20ms | 3.6× | 34.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 31.72ms | 95.42ms | 3.0× | 12.4% | PASS |
| ac_writes | `oltp_write_only_ac` | 36.07ms | 106.03ms | 2.9× | 48.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 34.59ms | 96.97ms | 2.8× | 32.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 43.21ms | 111.20ms | 2.6× | 29.7% | PASS |

</details>

</details>

## Version-control latency

Wall time: 2m 17s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 80.25ms | 130.00ms | 61.7% | 0.4% | PASS |
| `status_dirty_many_tables` | 83.58ms | 130.00ms | 64.3% | 0.6% | PASS |
| `diff_regular_working_one_table` | 78.03ms | 120.00ms | 65.0% | 0.5% | PASS |
| `diff_regular_working_many_tables` | 89.00ms | 140.00ms | 63.6% | 0.6% | PASS |
| `diff_stat_working_many_tables` | 88.30ms | 140.00ms | 63.1% | 0.4% | PASS |
| `diff_schema_working_many_tables` | 88.59ms | 140.00ms | 63.3% | 0.3% | PASS |
| `branch_list_many_branches` | 22.00ms | 35.00ms | 62.8% | 0.6% | PASS |
| `branch_create_delete` | 25.06ms | 40.00ms | 62.6% | 1.5% | PASS |
| `checkout_branch_clean` | 55.36ms | 150.00ms | 36.9% | 1.6% | PASS |
| `merge_data_no_conflicts` | 28.98ms | 50.00ms | 58.0% | 1.5% | PASS |
| `merge_schema_no_conflicts` | 21.77ms | 35.00ms | 62.2% | 1.5% | PASS |
| `merge_data_conflicts` | 126.72ms | 180.00ms | 70.4% | 0.2% | PASS |
| `merge_data_conflicts_with_resolve` | 126.61ms | 180.00ms | 70.3% | 0.2% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
