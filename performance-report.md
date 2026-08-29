# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-08-29 11:08 UTC
>
> Commit: [`7ef767f6e28dcc0cc4c78e64bae8e2ffe3c4e643`](https://github.com/dolthub/doltlite/commit/7ef767f6e28dcc0cc4c78e64bae8e2ffe3c4e643)
>
> Runner: ubuntu24 20260823.283.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/33245709477)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.70s | 11.65s | 1.1× | 1.5% | **PASS** |
| Writes | 2.24s | 3.56s | 1.6× | 1.2% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 11.74s | 11.92s | 1.0× | 1.3% | **PASS** |
| Writes | 3.12s | 3.89s | 1.2× | 1.3% | **PASS** |
| Autocommit writes | 726.63ms | 2.65s | 3.6× | 4.8% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.70s | 2.72s | 1.0× | 1.4% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.64s | 3.01s | 1.1× | 1.6% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.71s | 2.82s | 1.0× | 1.2% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.65s | 3.10s | 1.2× | 1.7% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 457.31ms | 701.22ms | 1.5× | 1.3% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 601.54ms | 982.40ms | 1.6× | 1.9% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 586.19ms | 940.20ms | 1.6× | 1.0% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 594.34ms | 932.06ms | 1.6× | 1.0% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.87s | 2.76s | 1.0× | 0.9% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 3.13s | 3.13s | 1.0× | 1.8% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.97s | 2.89s | 1.0× | 1.5% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.77s | 3.13s | 1.1× | 1.5% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 591.94ms | 738.67ms | 1.2× | 1.1% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 979.54ms | 1.13s | 1.2× | 3.2% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 802.96ms | 1.03s | 1.3× | 1.2% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 745.61ms | 1.00s | 1.3× | 1.4% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.70s | 2.76s | 1.0× | 1.1% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 3.15s | 3.19s | 1.0× | 1.4% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.80s | 2.89s | 1.0× | 1.0% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.59s | 3.12s | 1.2× | 1.4% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 156.93ms | 615.18ms | 3.9× | 4.1% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 222.75ms | 766.23ms | 3.4× | 6.7% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 149.59ms | 559.74ms | 3.7× | 3.2% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 197.36ms | 705.63ms | 3.6× | 5.2% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 25.31ms | 27.49ms | 1.1× | 1.7% | PASS |
| mem_reads | `oltp_range_select` | 11.33ms | 11.16ms | 1.0× | 1.0% | PASS |
| mem_reads | `oltp_sum_range` | 10.48ms | 10.76ms | 1.0× | 1.7% | PASS |
| mem_reads | `oltp_order_range` | 2.76ms | 2.79ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 3.84ms | 3.83ms | 1.0× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 4.15ms | 5.23ms | 1.3× | 1.8% | PASS |
| mem_reads | `select_random_points` | 11.76ms | 11.63ms | 1.0× | 1.8% | PASS |
| mem_reads | `select_random_ranges` | 3.24ms | 4.00ms | 1.2× | 1.2% | PASS |
| mem_reads | `covering_index_scan` | 4.33ms | 4.24ms | 1.0× | 2.4% | PASS |
| mem_reads | `groupby_scan` | 32.80ms | 33.38ms | 1.0× | 0.7% | PASS |
| mem_reads | `index_join` | 5.98ms | 8.02ms | 1.3× | 2.1% | PASS |
| mem_reads | `index_join_scan` | 3.61ms | 5.01ms | 1.4× | 1.1% | PASS |
| mem_reads | `types_table_scan` | 1.12s | 1.17s | 1.0× | 1.4% | PASS |
| mem_reads | `table_scan` | 1.35s | 1.31s | 1.0× | 2.3% | PASS |
| mem_reads | `oltp_read_only` | 107.91ms | 111.21ms | 1.0× | 1.2% | PASS |
| mem_writes | `oltp_bulk_insert` | 179.19ms | 243.47ms | 1.4× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 15.72ms | 27.84ms | 1.8× | 1.1% | PASS |
| mem_writes | `oltp_update_index` | 52.97ms | 95.04ms | 1.8× | 1.3% | PASS |
| mem_writes | `oltp_update_non_index` | 36.38ms | 54.60ms | 1.5× | 1.3% | PASS |
| mem_writes | `oltp_delete_insert` | 47.66ms | 77.23ms | 1.6× | 1.2% | PASS |
| mem_writes | `oltp_write_only` | 24.27ms | 51.08ms | 2.1× | 1.2% | PASS |
| mem_writes | `types_delete_insert` | 26.59ms | 38.59ms | 1.5× | 1.7% | PASS |
| mem_writes | `oltp_read_write` | 74.53ms | 113.37ms | 1.5× | 1.9% | PASS |
| file_reads | `oltp_point_select` | 110.74ms | 49.50ms | 0.4× | 0.6% | PASS |
| file_reads | `oltp_range_select` | 20.07ms | 13.24ms | 0.7× | 1.2% | PASS |
| file_reads | `oltp_sum_range` | 18.83ms | 12.82ms | 0.7× | 1.3% | PASS |
| file_reads | `oltp_order_range` | 3.70ms | 3.07ms | 0.8× | 0.6% | PASS |
| file_reads | `oltp_distinct_range` | 4.75ms | 4.08ms | 0.9× | 0.9% | PASS |
| file_reads | `oltp_index_scan` | 12.80ms | 7.49ms | 0.6× | 0.8% | PASS |
| file_reads | `select_random_points` | 20.33ms | 13.75ms | 0.7× | 1.1% | PASS |
| file_reads | `select_random_ranges` | 11.68ms | 6.13ms | 0.5× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 13.25ms | 6.63ms | 0.5× | 0.9% | PASS |
| file_reads | `groupby_scan` | 33.69ms | 33.58ms | 1.0× | 0.8% | PASS |
| file_reads | `index_join` | 10.79ms | 9.54ms | 0.9× | 2.0% | PASS |
| file_reads | `index_join_scan` | 4.55ms | 5.18ms | 1.1× | 1.0% | PASS |
| file_reads | `types_table_scan` | 1.11s | 1.17s | 1.1× | 0.7% | PASS |
| file_reads | `table_scan` | 1.27s | 1.29s | 1.0× | 1.1% | PASS |
| file_reads | `oltp_read_only` | 224.09ms | 139.89ms | 0.6× | 0.7% | PASS |
| file_writes | `oltp_bulk_insert` | 192.62ms | 250.42ms | 1.3× | 0.7% | PASS |
| file_writes | `oltp_insert` | 21.83ms | 31.57ms | 1.4× | 1.2% | PASS |
| file_writes | `oltp_update_index` | 78.76ms | 99.99ms | 1.3× | 1.1% | PASS |
| file_writes | `oltp_update_non_index` | 58.35ms | 65.83ms | 1.1× | 1.1% | PASS |
| file_writes | `oltp_delete_insert` | 67.11ms | 80.97ms | 1.2× | 1.1% | PASS |
| file_writes | `oltp_write_only` | 44.15ms | 54.02ms | 1.2× | 2.0% | PASS |
| file_writes | `types_delete_insert` | 40.52ms | 42.67ms | 1.1× | 0.9% | PASS |
| file_writes | `oltp_read_write` | 88.61ms | 113.22ms | 1.3× | 1.3% | PASS |
| ac_reads | `oltp_point_select` | 52.12ms | 48.16ms | 0.9× | 0.8% | PASS |
| ac_reads | `oltp_range_select` | 13.75ms | 13.10ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_sum_range` | 12.83ms | 12.68ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_order_range` | 3.07ms | 3.03ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_distinct_range` | 4.10ms | 4.07ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_index_scan` | 6.99ms | 7.16ms | 1.0× | 1.2% | PASS |
| ac_reads | `select_random_points` | 14.08ms | 13.46ms | 1.0× | 2.3% | PASS |
| ac_reads | `select_random_ranges` | 6.12ms | 6.08ms | 1.0× | 1.4% | PASS |
| ac_reads | `covering_index_scan` | 7.45ms | 6.44ms | 0.9× | 1.6% | PASS |
| ac_reads | `groupby_scan` | 32.88ms | 33.49ms | 1.0× | 0.6% | PASS |
| ac_reads | `index_join` | 7.65ms | 9.05ms | 1.2× | 1.4% | PASS |
| ac_reads | `index_join_scan` | 3.98ms | 5.11ms | 1.3× | 1.7% | PASS |
| ac_reads | `types_table_scan` | 1.11s | 1.17s | 1.1× | 0.8% | PASS |
| ac_reads | `table_scan` | 1.28s | 1.29s | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_read_only` | 146.52ms | 141.07ms | 1.0× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 17.01ms | 61.92ms | 3.6× | 4.2% | PASS |
| ac_writes | `oltp_insert_ac` | 19.75ms | 76.55ms | 3.9× | 4.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 21.43ms | 87.22ms | 4.1× | 3.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 17.28ms | 69.88ms | 4.0× | 5.5% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 19.61ms | 84.27ms | 4.3× | 5.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 19.74ms | 79.12ms | 4.0× | 3.8% | PASS |
| ac_writes | `types_delete_insert_ac` | 16.97ms | 70.18ms | 4.1× | 4.2% | PASS |
| ac_writes | `oltp_read_write_ac` | 25.13ms | 86.04ms | 3.4× | 2.7% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 37.81ms | 37.41ms | 1.0× | 2.3% | PASS |
| mem_reads | `oltp_range_select` | 17.91ms | 13.84ms | 0.8× | 2.2% | PASS |
| mem_reads | `oltp_sum_range` | 16.49ms | 14.05ms | 0.9× | 1.5% | PASS |
| mem_reads | `oltp_order_range` | 3.40ms | 3.19ms | 0.9× | 1.4% | PASS |
| mem_reads | `oltp_distinct_range` | 4.51ms | 4.25ms | 0.9× | 0.8% | PASS |
| mem_reads | `oltp_index_scan` | 4.16ms | 6.25ms | 1.5× | 1.5% | PASS |
| mem_reads | `select_random_points` | 21.70ms | 20.41ms | 0.9× | 2.5% | PASS |
| mem_reads | `select_random_ranges` | 3.45ms | 5.11ms | 1.5× | 2.2% | PASS |
| mem_reads | `covering_index_scan` | 4.26ms | 4.29ms | 1.0× | 1.1% | PASS |
| mem_reads | `groupby_scan` | 34.99ms | 32.66ms | 0.9× | 0.9% | PASS |
| mem_reads | `index_join` | 10.65ms | 9.14ms | 0.9× | 3.5% | PASS |
| mem_reads | `index_join_scan` | 3.88ms | 5.22ms | 1.3× | 2.0% | PASS |
| mem_reads | `types_table_scan` | 1.09s | 1.31s | 1.2× | 1.3% | PASS |
| mem_reads | `table_scan` | 1.25s | 1.42s | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_read_only` | 142.51ms | 135.45ms | 1.0× | 2.7% | PASS |
| mem_writes | `oltp_bulk_insert` | 239.25ms | 362.68ms | 1.5× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 18.05ms | 38.41ms | 2.1× | 1.4% | PASS |
| mem_writes | `oltp_update_index` | 66.49ms | 136.42ms | 2.1× | 2.0% | PASS |
| mem_writes | `oltp_update_non_index` | 49.08ms | 79.12ms | 1.6× | 2.1% | PASS |
| mem_writes | `oltp_delete_insert` | 55.82ms | 105.45ms | 1.9× | 1.8% | PASS |
| mem_writes | `oltp_write_only` | 28.68ms | 60.63ms | 2.1× | 1.4% | PASS |
| mem_writes | `types_delete_insert` | 40.48ms | 54.36ms | 1.3× | 2.2% | PASS |
| mem_writes | `oltp_read_write` | 103.68ms | 145.34ms | 1.4× | 3.1% | PASS |
| file_reads | `oltp_point_select` | 109.08ms | 56.09ms | 0.5× | 1.2% | PASS |
| file_reads | `oltp_range_select` | 25.34ms | 15.84ms | 0.6× | 1.9% | PASS |
| file_reads | `oltp_sum_range` | 24.39ms | 16.09ms | 0.7× | 2.4% | PASS |
| file_reads | `oltp_order_range` | 4.51ms | 3.66ms | 0.8× | 2.1% | PASS |
| file_reads | `oltp_distinct_range` | 5.49ms | 4.66ms | 0.8× | 2.0% | PASS |
| file_reads | `oltp_index_scan` | 11.61ms | 8.22ms | 0.7× | 1.0% | PASS |
| file_reads | `select_random_points` | 31.84ms | 23.68ms | 0.7× | 2.6% | PASS |
| file_reads | `select_random_ranges` | 10.80ms | 7.18ms | 0.7× | 0.9% | PASS |
| file_reads | `covering_index_scan` | 11.96ms | 6.73ms | 0.6× | 1.1% | PASS |
| file_reads | `groupby_scan` | 36.74ms | 33.54ms | 0.9× | 1.3% | PASS |
| file_reads | `index_join` | 15.33ms | 10.73ms | 0.7× | 2.9% | PASS |
| file_reads | `index_join_scan` | 4.97ms | 5.78ms | 1.2× | 1.8% | PASS |
| file_reads | `types_table_scan` | 1.26s | 1.34s | 1.1× | 1.3% | PASS |
| file_reads | `table_scan` | 1.31s | 1.42s | 1.1× | 4.8% | PASS |
| file_reads | `oltp_read_only` | 268.46ms | 173.38ms | 0.6× | 1.2% | PASS |
| file_writes | `oltp_bulk_insert` | 268.23ms | 389.45ms | 1.5× | 1.3% | PASS |
| file_writes | `oltp_insert` | 26.25ms | 46.02ms | 1.8× | 2.1% | PASS |
| file_writes | `oltp_update_index` | 138.88ms | 159.49ms | 1.1× | 6.0% | PASS |
| file_writes | `oltp_update_non_index` | 97.55ms | 97.11ms | 1.0× | 7.5% | PASS |
| file_writes | `oltp_delete_insert` | 104.11ms | 126.86ms | 1.2× | 2.1% | PASS |
| file_writes | `oltp_write_only` | 86.62ms | 76.36ms | 0.9× | 12.3% | PASS |
| file_writes | `types_delete_insert` | 77.39ms | 66.53ms | 0.9× | 2.1% | PASS |
| file_writes | `oltp_read_write` | 180.50ms | 165.10ms | 0.9× | 4.3% | PASS |
| ac_reads | `oltp_point_select` | 64.70ms | 58.11ms | 0.9× | 1.5% | PASS |
| ac_reads | `oltp_range_select` | 21.68ms | 16.42ms | 0.8× | 1.4% | PASS |
| ac_reads | `oltp_sum_range` | 20.15ms | 16.67ms | 0.8× | 2.0% | PASS |
| ac_reads | `oltp_order_range` | 3.99ms | 3.61ms | 0.9× | 2.5% | PASS |
| ac_reads | `oltp_distinct_range` | 5.14ms | 4.74ms | 0.9× | 2.0% | PASS |
| ac_reads | `oltp_index_scan` | 6.98ms | 8.49ms | 1.2× | 1.3% | PASS |
| ac_reads | `select_random_points` | 29.67ms | 25.45ms | 0.9× | 2.7% | PASS |
| ac_reads | `select_random_ranges` | 6.25ms | 7.29ms | 1.2× | 1.4% | PASS |
| ac_reads | `covering_index_scan` | 7.27ms | 6.93ms | 1.0× | 1.3% | PASS |
| ac_reads | `groupby_scan` | 37.02ms | 34.08ms | 0.9× | 0.5% | PASS |
| ac_reads | `index_join` | 13.86ms | 11.40ms | 0.8× | 2.9% | PASS |
| ac_reads | `index_join_scan` | 4.62ms | 6.12ms | 1.3× | 2.5% | PASS |
| ac_reads | `types_table_scan` | 1.28s | 1.36s | 1.1× | 0.7% | PASS |
| ac_reads | `table_scan` | 1.45s | 1.46s | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_read_only` | 195.23ms | 172.49ms | 0.9× | 1.4% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 25.29ms | 79.75ms | 3.2× | 6.4% | PASS |
| ac_writes | `oltp_insert_ac` | 28.00ms | 91.51ms | 3.3× | 6.3% | PASS |
| ac_writes | `oltp_update_index_ac` | 28.71ms | 108.35ms | 3.8× | 7.0% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 25.24ms | 90.16ms | 3.6× | 8.5% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 27.94ms | 100.45ms | 3.6× | 7.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 27.30ms | 99.02ms | 3.6× | 7.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 25.35ms | 93.10ms | 3.7× | 5.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 34.91ms | 103.90ms | 3.0× | 5.8% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 35.22ms | 34.02ms | 1.0× | 1.3% | PASS |
| mem_reads | `oltp_range_select` | 16.84ms | 13.25ms | 0.8× | 1.2% | PASS |
| mem_reads | `oltp_sum_range` | 15.55ms | 12.52ms | 0.8× | 1.1% | PASS |
| mem_reads | `oltp_order_range` | 3.42ms | 3.07ms | 0.9× | 1.4% | PASS |
| mem_reads | `oltp_distinct_range` | 4.47ms | 4.18ms | 0.9× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 4.10ms | 5.57ms | 1.4× | 2.3% | PASS |
| mem_reads | `select_random_points` | 21.31ms | 19.44ms | 0.9× | 1.6% | PASS |
| mem_reads | `select_random_ranges` | 3.54ms | 5.12ms | 1.4× | 1.1% | PASS |
| mem_reads | `covering_index_scan` | 4.39ms | 4.34ms | 1.0× | 1.5% | PASS |
| mem_reads | `groupby_scan` | 36.83ms | 34.85ms | 0.9× | 0.5% | PASS |
| mem_reads | `index_join` | 10.83ms | 8.58ms | 0.8× | 1.9% | PASS |
| mem_reads | `index_join_scan` | 3.99ms | 5.42ms | 1.4× | 1.4% | PASS |
| mem_reads | `types_table_scan` | 1.12s | 1.21s | 1.1× | 0.7% | PASS |
| mem_reads | `table_scan` | 1.28s | 1.33s | 1.0× | 0.6% | PASS |
| mem_reads | `oltp_read_only` | 138.56ms | 126.71ms | 0.9× | 1.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 240.29ms | 344.49ms | 1.4× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 19.06ms | 38.21ms | 2.0× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 65.77ms | 132.05ms | 2.0× | 1.0% | PASS |
| mem_writes | `oltp_update_non_index` | 48.09ms | 75.77ms | 1.6× | 1.3% | PASS |
| mem_writes | `oltp_delete_insert` | 52.72ms | 101.95ms | 1.9× | 0.8% | PASS |
| mem_writes | `oltp_write_only` | 27.95ms | 61.22ms | 2.2× | 1.0% | PASS |
| mem_writes | `types_delete_insert` | 37.99ms | 50.94ms | 1.3× | 1.3% | PASS |
| mem_writes | `oltp_read_write` | 94.31ms | 135.57ms | 1.4× | 1.0% | PASS |
| file_reads | `oltp_point_select` | 119.86ms | 55.72ms | 0.5× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 25.72ms | 15.43ms | 0.6× | 1.5% | PASS |
| file_reads | `oltp_sum_range` | 24.15ms | 14.86ms | 0.6× | 1.4% | PASS |
| file_reads | `oltp_order_range` | 4.32ms | 3.38ms | 0.8× | 2.3% | PASS |
| file_reads | `oltp_distinct_range` | 5.29ms | 4.45ms | 0.8× | 2.2% | PASS |
| file_reads | `oltp_index_scan` | 12.59ms | 8.16ms | 0.6× | 1.7% | PASS |
| file_reads | `select_random_points` | 30.59ms | 21.92ms | 0.7× | 2.0% | PASS |
| file_reads | `select_random_ranges` | 11.97ms | 7.37ms | 0.6× | 0.9% | PASS |
| file_reads | `covering_index_scan` | 12.90ms | 6.83ms | 0.5× | 1.7% | PASS |
| file_reads | `groupby_scan` | 37.57ms | 35.22ms | 0.9× | 0.7% | PASS |
| file_reads | `index_join` | 15.09ms | 10.49ms | 0.7× | 2.6% | PASS |
| file_reads | `index_join_scan` | 4.91ms | 5.85ms | 1.2× | 1.6% | PASS |
| file_reads | `types_table_scan` | 1.13s | 1.21s | 1.1× | 0.6% | PASS |
| file_reads | `table_scan` | 1.28s | 1.34s | 1.0× | 0.5% | PASS |
| file_reads | `oltp_read_only` | 258.30ms | 158.03ms | 0.6× | 0.6% | PASS |
| file_writes | `oltp_bulk_insert` | 263.50ms | 352.85ms | 1.3× | 0.8% | PASS |
| file_writes | `oltp_insert` | 25.52ms | 44.62ms | 1.7× | 1.2% | PASS |
| file_writes | `oltp_update_index` | 96.17ms | 147.63ms | 1.5× | 1.3% | PASS |
| file_writes | `oltp_update_non_index` | 87.30ms | 88.67ms | 1.0× | 9.3% | PASS |
| file_writes | `oltp_delete_insert` | 86.52ms | 115.64ms | 1.3× | 1.3% | PASS |
| file_writes | `oltp_write_only` | 55.55ms | 71.31ms | 1.3× | 1.6% | PASS |
| file_writes | `types_delete_insert` | 63.19ms | 60.01ms | 0.9× | 1.2% | PASS |
| file_writes | `oltp_read_write` | 125.22ms | 147.04ms | 1.2× | 1.1% | PASS |
| ac_reads | `oltp_point_select` | 63.81ms | 55.81ms | 0.9× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 20.51ms | 15.53ms | 0.8× | 1.3% | PASS |
| ac_reads | `oltp_sum_range` | 18.94ms | 14.90ms | 0.8× | 1.0% | PASS |
| ac_reads | `oltp_order_range` | 3.84ms | 3.38ms | 0.9× | 0.8% | PASS |
| ac_reads | `oltp_distinct_range` | 4.87ms | 4.46ms | 0.9× | 0.8% | PASS |
| ac_reads | `oltp_index_scan` | 7.25ms | 8.17ms | 1.1× | 1.3% | PASS |
| ac_reads | `select_random_points` | 25.42ms | 22.16ms | 0.9× | 1.5% | PASS |
| ac_reads | `select_random_ranges` | 6.54ms | 7.38ms | 1.1× | 0.9% | PASS |
| ac_reads | `covering_index_scan` | 7.48ms | 6.82ms | 0.9× | 1.0% | PASS |
| ac_reads | `groupby_scan` | 37.12ms | 35.19ms | 0.9× | 0.6% | PASS |
| ac_reads | `index_join` | 12.91ms | 10.45ms | 0.8× | 1.2% | PASS |
| ac_reads | `index_join_scan` | 4.55ms | 5.81ms | 1.3× | 1.7% | PASS |
| ac_reads | `types_table_scan` | 1.13s | 1.21s | 1.1× | 0.6% | PASS |
| ac_reads | `table_scan` | 1.28s | 1.34s | 1.0× | 0.6% | PASS |
| ac_reads | `oltp_read_only` | 178.85ms | 158.22ms | 0.9× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 16.84ms | 55.45ms | 3.3× | 3.2% | PASS |
| ac_writes | `oltp_insert_ac` | 18.50ms | 70.87ms | 3.8× | 3.2% | PASS |
| ac_writes | `oltp_update_index_ac` | 19.66ms | 81.16ms | 4.1× | 4.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 15.90ms | 63.39ms | 4.0× | 3.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 18.64ms | 73.33ms | 3.9× | 3.0% | PASS |
| ac_writes | `oltp_write_only_ac` | 18.30ms | 72.22ms | 3.9× | 3.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 16.75ms | 64.51ms | 3.9× | 2.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 25.01ms | 78.81ms | 3.2× | 3.4% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 32.45ms | 38.73ms | 1.2× | 2.3% | PASS |
| mem_reads | `oltp_range_select` | 19.31ms | 21.58ms | 1.1× | 1.9% | PASS |
| mem_reads | `oltp_sum_range` | 17.78ms | 20.27ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_order_range` | 3.57ms | 3.82ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_distinct_range` | 4.75ms | 4.90ms | 1.0× | 1.6% | PASS |
| mem_reads | `oltp_index_scan` | 4.61ms | 6.04ms | 1.3× | 1.6% | PASS |
| mem_reads | `select_random_points` | 29.79ms | 33.21ms | 1.1× | 2.0% | PASS |
| mem_reads | `select_random_ranges` | 7.98ms | 8.95ms | 1.1× | 0.9% | PASS |
| mem_reads | `covering_index_scan` | 4.30ms | 4.64ms | 1.1× | 2.4% | PASS |
| mem_reads | `groupby_scan` | 38.10ms | 38.26ms | 1.0× | 0.8% | PASS |
| mem_reads | `index_join` | 8.20ms | 10.79ms | 1.3× | 1.7% | PASS |
| mem_reads | `index_join_scan` | 4.18ms | 5.37ms | 1.3× | 1.8% | PASS |
| mem_reads | `types_table_scan` | 1.07s | 1.32s | 1.2× | 1.8% | PASS |
| mem_reads | `table_scan` | 1.25s | 1.41s | 1.1× | 4.3% | PASS |
| mem_reads | `oltp_read_only` | 154.26ms | 169.03ms | 1.1× | 1.5% | PASS |
| mem_writes | `oltp_bulk_insert` | 243.40ms | 340.50ms | 1.4× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 18.86ms | 34.41ms | 1.8× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 67.93ms | 119.02ms | 1.8× | 1.0% | PASS |
| mem_writes | `oltp_update_non_index` | 50.55ms | 75.33ms | 1.5× | 1.4% | PASS |
| mem_writes | `oltp_delete_insert` | 50.41ms | 95.99ms | 1.9× | 1.1% | PASS |
| mem_writes | `oltp_write_only` | 26.82ms | 55.71ms | 2.1× | 1.0% | PASS |
| mem_writes | `types_delete_insert` | 32.68ms | 52.39ms | 1.6× | 0.8% | PASS |
| mem_writes | `oltp_read_write` | 103.70ms | 158.70ms | 1.5× | 1.3% | PASS |
| file_reads | `oltp_point_select` | 103.99ms | 58.38ms | 0.6× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 26.81ms | 23.75ms | 0.9× | 1.9% | PASS |
| file_reads | `oltp_sum_range` | 25.69ms | 22.75ms | 0.9× | 1.6% | PASS |
| file_reads | `oltp_order_range` | 4.37ms | 4.16ms | 1.0× | 1.7% | PASS |
| file_reads | `oltp_distinct_range` | 5.60ms | 5.21ms | 0.9× | 1.5% | PASS |
| file_reads | `oltp_index_scan` | 11.96ms | 8.16ms | 0.7× | 1.6% | PASS |
| file_reads | `select_random_points` | 38.40ms | 35.49ms | 0.9× | 2.2% | PASS |
| file_reads | `select_random_ranges` | 15.27ms | 10.96ms | 0.7× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 11.31ms | 6.41ms | 0.6× | 1.2% | PASS |
| file_reads | `groupby_scan` | 38.58ms | 38.21ms | 1.0× | 0.7% | PASS |
| file_reads | `index_join` | 12.28ms | 12.14ms | 1.0× | 1.8% | PASS |
| file_reads | `index_join_scan` | 4.99ms | 5.60ms | 1.1× | 1.5% | PASS |
| file_reads | `types_table_scan` | 1.03s | 1.30s | 1.3× | 0.7% | PASS |
| file_reads | `table_scan` | 1.18s | 1.40s | 1.2× | 1.2% | PASS |
| file_reads | `oltp_read_only` | 259.16ms | 197.48ms | 0.8× | 1.3% | PASS |
| file_writes | `oltp_bulk_insert` | 257.21ms | 349.29ms | 1.4× | 1.0% | PASS |
| file_writes | `oltp_insert` | 25.25ms | 39.59ms | 1.6× | 1.3% | PASS |
| file_writes | `oltp_update_index` | 92.07ms | 128.96ms | 1.4× | 1.2% | PASS |
| file_writes | `oltp_update_non_index` | 74.44ms | 87.95ms | 1.2× | 1.4% | PASS |
| file_writes | `oltp_delete_insert` | 75.18ms | 105.22ms | 1.4× | 1.5% | PASS |
| file_writes | `oltp_write_only` | 49.71ms | 65.73ms | 1.3× | 2.4% | PASS |
| file_writes | `types_delete_insert` | 48.09ms | 58.30ms | 1.2× | 1.6% | PASS |
| file_writes | `oltp_read_write` | 123.66ms | 166.00ms | 1.3× | 1.3% | PASS |
| ac_reads | `oltp_point_select` | 55.28ms | 57.25ms | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_range_select` | 21.60ms | 23.72ms | 1.1× | 1.7% | PASS |
| ac_reads | `oltp_sum_range` | 20.68ms | 22.69ms | 1.1× | 1.7% | PASS |
| ac_reads | `oltp_order_range` | 4.12ms | 4.19ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_distinct_range` | 5.29ms | 5.29ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_index_scan` | 7.44ms | 8.24ms | 1.1× | 1.4% | PASS |
| ac_reads | `select_random_points` | 32.61ms | 35.66ms | 1.1× | 1.7% | PASS |
| ac_reads | `select_random_ranges` | 10.55ms | 11.00ms | 1.0× | 1.6% | PASS |
| ac_reads | `covering_index_scan` | 6.76ms | 6.30ms | 0.9× | 1.8% | PASS |
| ac_reads | `groupby_scan` | 37.61ms | 38.09ms | 1.0× | 0.9% | PASS |
| ac_reads | `index_join` | 9.69ms | 12.08ms | 1.2× | 1.1% | PASS |
| ac_reads | `index_join_scan` | 4.46ms | 5.61ms | 1.3× | 2.1% | PASS |
| ac_reads | `types_table_scan` | 1.03s | 1.30s | 1.3× | 0.4% | PASS |
| ac_reads | `table_scan` | 1.17s | 1.40s | 1.2× | 0.5% | PASS |
| ac_reads | `oltp_read_only` | 184.73ms | 196.24ms | 1.1× | 1.4% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.92ms | 73.56ms | 3.2× | 5.1% | PASS |
| ac_writes | `oltp_insert_ac` | 24.42ms | 89.74ms | 3.7× | 7.6% | PASS |
| ac_writes | `oltp_update_index_ac` | 25.71ms | 98.58ms | 3.8× | 3.8% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.72ms | 80.61ms | 3.5× | 5.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.97ms | 90.27ms | 3.6× | 6.5% | PASS |
| ac_writes | `oltp_write_only_ac` | 24.16ms | 88.47ms | 3.7× | 4.9% | PASS |
| ac_writes | `types_delete_insert_ac` | 21.07ms | 87.69ms | 4.2× | 6.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.41ms | 96.72ms | 3.1× | 4.7% | PASS |

</details>

</details>

## Version-control latency

Wall time: 2m 20s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 83.71ms | 130.00ms | 64.4% | 0.4% | PASS |
| `status_dirty_many_tables` | 86.62ms | 130.00ms | 66.6% | 0.3% | PASS |
| `diff_regular_working_one_table` | 78.73ms | 120.00ms | 65.6% | 0.5% | PASS |
| `diff_regular_working_many_tables` | 92.11ms | 140.00ms | 65.8% | 0.4% | PASS |
| `diff_stat_working_many_tables` | 92.04ms | 140.00ms | 65.7% | 0.4% | PASS |
| `diff_schema_working_many_tables` | 92.41ms | 140.00ms | 66.0% | 0.6% | PASS |
| `branch_list_many_branches` | 22.17ms | 35.00ms | 63.3% | 0.9% | PASS |
| `branch_create_delete` | 24.86ms | 40.00ms | 62.1% | 1.1% | PASS |
| `checkout_branch_clean` | 55.88ms | 150.00ms | 37.3% | 1.4% | PASS |
| `merge_data_no_conflicts` | 28.86ms | 50.00ms | 57.7% | 1.2% | PASS |
| `merge_schema_no_conflicts` | 21.93ms | 35.00ms | 62.6% | 1.2% | PASS |
| `merge_data_conflicts` | 126.96ms | 180.00ms | 70.5% | 0.3% | PASS |
| `merge_data_conflicts_with_resolve` | 127.24ms | 180.00ms | 70.7% | 0.4% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
