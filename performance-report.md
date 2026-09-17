# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-09-17 11:10 UTC
>
> Commit: [`af5b4bbe494f4826f8f14bd3664f2cf8eb81db31`](https://github.com/dolthub/doltlite/commit/af5b4bbe494f4826f8f14bd3664f2cf8eb81db31)
>
> Runner: ubuntu24 20260907.300.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/35206094007)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.86s | 10.41s | 1.1× | 1.4% | **PASS** |
| Writes | 2.06s | 3.39s | 1.6× | 1.0% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.68s | 10.61s | 1.0× | 1.4% | **PASS** |
| Writes | 3.33s | 4.06s | 1.2× | 2.0% | **PASS** |
| Autocommit writes | 916.39ms | 3.08s | 3.4× | 7.3% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.46s | 2.57s | 1.0× | 1.3% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.59s | 2.76s | 1.1× | 1.7% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.65s | 2.79s | 1.1× | 1.7% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.16s | 2.30s | 1.1× | 1.0% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 431.86ms | 721.71ms | 1.7× | 1.1% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 579.71ms | 981.85ms | 1.7× | 1.0% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 588.12ms | 986.89ms | 1.7× | 1.1% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 462.87ms | 695.84ms | 1.5× | 0.9% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.65s | 2.62s | 1.0× | 2.0% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.85s | 2.82s | 1.0× | 1.5% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.80s | 2.81s | 1.0× | 1.7% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.38s | 2.36s | 1.0× | 0.9% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 577.79ms | 793.27ms | 1.4× | 1.5% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 926.58ms | 1.11s | 1.2× | 3.3% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 812.91ms | 1.10s | 1.4× | 1.7% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 1.01s | 1.06s | 1.0× | 3.5% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.52s | 2.63s | 1.0× | 1.5% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.67s | 2.82s | 1.1× | 1.7% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.75s | 2.85s | 1.0× | 1.4% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.23s | 2.35s | 1.1× | 0.8% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 218.28ms | 803.74ms | 3.7× | 8.7% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 214.06ms | 756.52ms | 3.5× | 6.1% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 227.52ms | 796.85ms | 3.5× | 8.3% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 256.53ms | 720.77ms | 2.8× | 6.8% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 23.47ms | 29.60ms | 1.3× | 1.3% | PASS |
| mem_reads | `oltp_range_select` | 11.11ms | 11.02ms | 1.0× | 2.1% | PASS |
| mem_reads | `oltp_sum_range` | 9.72ms | 11.00ms | 1.1× | 2.1% | PASS |
| mem_reads | `oltp_order_range` | 2.58ms | 2.73ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 3.61ms | 3.89ms | 1.1× | 2.3% | PASS |
| mem_reads | `oltp_index_scan` | 3.77ms | 5.11ms | 1.4× | 1.5% | PASS |
| mem_reads | `select_random_points` | 10.24ms | 10.84ms | 1.1× | 4.2% | PASS |
| mem_reads | `select_random_ranges` | 4.65ms | 5.02ms | 1.1× | 1.5% | PASS |
| mem_reads | `covering_index_scan` | 7.72ms | 9.37ms | 1.2× | 0.7% | PASS |
| mem_reads | `groupby_scan` | 29.31ms | 31.99ms | 1.1× | 0.9% | PASS |
| mem_reads | `index_join` | 5.75ms | 8.01ms | 1.4× | 1.3% | PASS |
| mem_reads | `index_join_scan` | 3.18ms | 4.38ms | 1.4× | 2.4% | PASS |
| mem_reads | `types_table_scan` | 1.05s | 1.11s | 1.1× | 0.5% | PASS |
| mem_reads | `table_scan` | 1.19s | 1.21s | 1.0× | 0.7% | PASS |
| mem_reads | `oltp_read_only` | 107.54ms | 118.83ms | 1.1× | 1.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 177.32ms | 273.38ms | 1.5× | 1.2% | PASS |
| mem_writes | `oltp_insert` | 15.10ms | 28.53ms | 1.9× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 49.64ms | 92.54ms | 1.9× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 34.00ms | 55.92ms | 1.6× | 1.1% | PASS |
| mem_writes | `oltp_delete_insert` | 43.76ms | 73.69ms | 1.7× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 21.08ms | 45.77ms | 2.2× | 1.1% | PASS |
| mem_writes | `types_delete_insert` | 23.91ms | 38.35ms | 1.6× | 1.9% | PASS |
| mem_writes | `oltp_read_write` | 67.04ms | 113.54ms | 1.7× | 2.3% | PASS |
| file_reads | `oltp_point_select` | 94.38ms | 49.88ms | 0.5× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 18.15ms | 13.11ms | 0.7× | 2.5% | PASS |
| file_reads | `oltp_sum_range` | 16.74ms | 12.99ms | 0.8× | 2.0% | PASS |
| file_reads | `oltp_order_range` | 3.36ms | 3.00ms | 0.9× | 3.1% | PASS |
| file_reads | `oltp_distinct_range` | 4.44ms | 4.16ms | 0.9× | 2.5% | PASS |
| file_reads | `oltp_index_scan` | 10.69ms | 7.16ms | 0.7× | 2.1% | PASS |
| file_reads | `select_random_points` | 16.20ms | 12.62ms | 0.8× | 2.3% | PASS |
| file_reads | `select_random_ranges` | 11.28ms | 6.90ms | 0.6× | 1.3% | PASS |
| file_reads | `covering_index_scan` | 14.89ms | 11.34ms | 0.8× | 1.3% | PASS |
| file_reads | `groupby_scan` | 30.10ms | 32.39ms | 1.1× | 0.8% | PASS |
| file_reads | `index_join` | 9.52ms | 9.32ms | 1.0× | 2.0% | PASS |
| file_reads | `index_join_scan` | 4.06ms | 4.93ms | 1.2× | 2.8% | PASS |
| file_reads | `types_table_scan` | 1.04s | 1.10s | 1.1× | 0.6% | PASS |
| file_reads | `table_scan` | 1.17s | 1.21s | 1.0× | 0.6% | PASS |
| file_reads | `oltp_read_only` | 200.64ms | 143.80ms | 0.7× | 1.3% | PASS |
| file_writes | `oltp_bulk_insert` | 190.95ms | 283.87ms | 1.5× | 1.2% | PASS |
| file_writes | `oltp_insert` | 21.74ms | 32.14ms | 1.5× | 2.1% | PASS |
| file_writes | `oltp_update_index` | 73.87ms | 103.23ms | 1.4× | 1.3% | PASS |
| file_writes | `oltp_update_non_index` | 56.53ms | 67.94ms | 1.2× | 1.5% | PASS |
| file_writes | `oltp_delete_insert` | 66.09ms | 85.61ms | 1.3× | 1.5% | PASS |
| file_writes | `oltp_write_only` | 42.84ms | 55.15ms | 1.3× | 2.4% | PASS |
| file_writes | `types_delete_insert` | 38.74ms | 44.20ms | 1.1× | 1.5% | PASS |
| file_writes | `oltp_read_write` | 87.02ms | 121.13ms | 1.4× | 1.5% | PASS |
| ac_reads | `oltp_point_select` | 44.83ms | 47.99ms | 1.1× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 12.29ms | 12.83ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_sum_range` | 11.39ms | 12.88ms | 1.1× | 1.5% | PASS |
| ac_reads | `oltp_order_range` | 2.82ms | 2.97ms | 1.1× | 1.9% | PASS |
| ac_reads | `oltp_distinct_range` | 3.97ms | 4.17ms | 1.1× | 3.3% | PASS |
| ac_reads | `oltp_index_scan` | 6.06ms | 7.16ms | 1.2× | 1.3% | PASS |
| ac_reads | `select_random_points` | 11.68ms | 12.75ms | 1.1× | 1.9% | PASS |
| ac_reads | `select_random_ranges` | 6.71ms | 6.90ms | 1.0× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 10.09ms | 11.29ms | 1.1× | 0.8% | PASS |
| ac_reads | `groupby_scan` | 29.34ms | 32.40ms | 1.1× | 1.1% | PASS |
| ac_reads | `index_join` | 7.07ms | 9.48ms | 1.3× | 1.7% | PASS |
| ac_reads | `index_join_scan` | 3.60ms | 4.97ms | 1.4× | 3.7% | PASS |
| ac_reads | `types_table_scan` | 1.04s | 1.10s | 1.1× | 0.5% | PASS |
| ac_reads | `table_scan` | 1.19s | 1.22s | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_read_only` | 137.50ms | 144.93ms | 1.1× | 2.3% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 25.34ms | 86.75ms | 3.4× | 17.6% | PASS |
| ac_writes | `oltp_insert_ac` | 29.41ms | 105.92ms | 3.6× | 12.7% | PASS |
| ac_writes | `oltp_update_index_ac` | 30.98ms | 121.52ms | 3.9× | 6.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 25.03ms | 86.74ms | 3.5× | 5.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.55ms | 100.11ms | 3.8× | 9.4% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.06ms | 99.05ms | 4.0× | 6.4% | PASS |
| ac_writes | `types_delete_insert_ac` | 23.31ms | 91.59ms | 3.9× | 7.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.60ms | 112.06ms | 3.4× | 9.9% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 38.13ms | 40.35ms | 1.1× | 2.3% | PASS |
| mem_reads | `oltp_range_select` | 17.54ms | 14.32ms | 0.8× | 2.5% | PASS |
| mem_reads | `oltp_sum_range` | 15.54ms | 14.09ms | 0.9× | 2.3% | PASS |
| mem_reads | `oltp_order_range` | 3.31ms | 3.21ms | 1.0× | 1.0% | PASS |
| mem_reads | `oltp_distinct_range` | 4.42ms | 4.38ms | 1.0× | 1.3% | PASS |
| mem_reads | `oltp_index_scan` | 4.15ms | 6.55ms | 1.6× | 1.7% | PASS |
| mem_reads | `select_random_points` | 22.86ms | 21.69ms | 0.9× | 3.8% | PASS |
| mem_reads | `select_random_ranges` | 6.87ms | 6.61ms | 1.0× | 1.3% | PASS |
| mem_reads | `covering_index_scan` | 7.92ms | 9.86ms | 1.2× | 1.4% | PASS |
| mem_reads | `groupby_scan` | 34.13ms | 34.53ms | 1.0× | 0.7% | PASS |
| mem_reads | `index_join` | 10.54ms | 9.47ms | 0.9× | 3.1% | PASS |
| mem_reads | `index_join_scan` | 3.65ms | 5.56ms | 1.5× | 2.5% | PASS |
| mem_reads | `types_table_scan` | 1.07s | 1.17s | 1.1× | 1.2% | PASS |
| mem_reads | `table_scan` | 1.21s | 1.28s | 1.1× | 0.6% | PASS |
| mem_reads | `oltp_read_only` | 137.76ms | 138.95ms | 1.0× | 2.2% | PASS |
| mem_writes | `oltp_bulk_insert` | 241.28ms | 362.66ms | 1.5× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 17.94ms | 38.66ms | 2.2× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 63.28ms | 137.06ms | 2.2× | 1.0% | PASS |
| mem_writes | `oltp_update_non_index` | 46.50ms | 80.76ms | 1.7× | 1.4% | PASS |
| mem_writes | `oltp_delete_insert` | 52.61ms | 103.93ms | 2.0× | 0.8% | PASS |
| mem_writes | `oltp_write_only` | 27.01ms | 60.69ms | 2.2× | 0.9% | PASS |
| mem_writes | `types_delete_insert` | 37.06ms | 53.98ms | 1.5× | 1.1% | PASS |
| mem_writes | `oltp_read_write` | 94.03ms | 144.10ms | 1.5× | 1.2% | PASS |
| file_reads | `oltp_point_select` | 104.70ms | 58.30ms | 0.6× | 1.1% | PASS |
| file_reads | `oltp_range_select` | 23.56ms | 16.31ms | 0.7× | 1.6% | PASS |
| file_reads | `oltp_sum_range` | 22.58ms | 16.42ms | 0.7× | 1.5% | PASS |
| file_reads | `oltp_order_range` | 4.08ms | 3.43ms | 0.8× | 1.4% | PASS |
| file_reads | `oltp_distinct_range` | 5.18ms | 4.63ms | 0.9× | 1.8% | PASS |
| file_reads | `oltp_index_scan` | 11.28ms | 8.44ms | 0.7× | 1.5% | PASS |
| file_reads | `select_random_points` | 30.55ms | 24.50ms | 0.8× | 2.1% | PASS |
| file_reads | `select_random_ranges` | 14.15ms | 8.73ms | 0.6× | 1.6% | PASS |
| file_reads | `covering_index_scan` | 15.39ms | 12.13ms | 0.8× | 1.1% | PASS |
| file_reads | `groupby_scan` | 34.65ms | 34.69ms | 1.0× | 0.8% | PASS |
| file_reads | `index_join` | 14.69ms | 11.15ms | 0.8× | 2.5% | PASS |
| file_reads | `index_join_scan` | 4.71ms | 6.24ms | 1.3× | 1.9% | PASS |
| file_reads | `types_table_scan` | 1.11s | 1.17s | 1.1× | 2.6% | PASS |
| file_reads | `table_scan` | 1.22s | 1.28s | 1.1× | 0.6% | PASS |
| file_reads | `oltp_read_only` | 238.65ms | 166.84ms | 0.7× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 266.46ms | 379.50ms | 1.4× | 0.9% | PASS |
| file_writes | `oltp_insert` | 25.77ms | 45.83ms | 1.8× | 2.1% | PASS |
| file_writes | `oltp_update_index` | 130.77ms | 160.77ms | 1.2× | 8.4% | PASS |
| file_writes | `oltp_update_non_index` | 101.51ms | 99.26ms | 1.0× | 10.1% | PASS |
| file_writes | `oltp_delete_insert` | 94.39ms | 122.72ms | 1.3× | 1.6% | PASS |
| file_writes | `oltp_write_only` | 92.06ms | 76.02ms | 0.8× | 8.9% | PASS |
| file_writes | `types_delete_insert` | 69.92ms | 66.39ms | 0.9× | 1.7% | PASS |
| file_writes | `oltp_read_write` | 145.71ms | 159.34ms | 1.1× | 4.5% | PASS |
| ac_reads | `oltp_point_select` | 58.77ms | 58.20ms | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_range_select` | 19.55ms | 16.04ms | 0.8× | 1.6% | PASS |
| ac_reads | `oltp_sum_range` | 17.98ms | 16.32ms | 0.9× | 1.7% | PASS |
| ac_reads | `oltp_order_range` | 3.66ms | 3.46ms | 0.9× | 1.7% | PASS |
| ac_reads | `oltp_distinct_range` | 4.74ms | 4.65ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_index_scan` | 6.54ms | 8.50ms | 1.3× | 1.5% | PASS |
| ac_reads | `select_random_points` | 25.13ms | 24.67ms | 1.0× | 2.0% | PASS |
| ac_reads | `select_random_ranges` | 9.27ms | 8.76ms | 0.9× | 2.0% | PASS |
| ac_reads | `covering_index_scan` | 10.46ms | 12.18ms | 1.2× | 1.7% | PASS |
| ac_reads | `groupby_scan` | 33.95ms | 34.65ms | 1.0× | 0.9% | PASS |
| ac_reads | `index_join` | 12.32ms | 10.94ms | 0.9× | 2.5% | PASS |
| ac_reads | `index_join_scan` | 4.21ms | 6.15ms | 1.5× | 2.7% | PASS |
| ac_reads | `types_table_scan` | 1.08s | 1.17s | 1.1× | 0.9% | PASS |
| ac_reads | `table_scan` | 1.22s | 1.28s | 1.1× | 0.8% | PASS |
| ac_reads | `oltp_read_only` | 169.43ms | 166.71ms | 1.0× | 0.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 24.46ms | 77.35ms | 3.2× | 6.1% | PASS |
| ac_writes | `oltp_insert_ac` | 26.46ms | 90.63ms | 3.4× | 7.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 28.02ms | 108.45ms | 3.9× | 5.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.27ms | 88.86ms | 3.7× | 6.1% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.27ms | 100.36ms | 3.8× | 7.4% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.67ms | 97.65ms | 3.8× | 5.0% | PASS |
| ac_writes | `types_delete_insert_ac` | 26.11ms | 89.01ms | 3.4× | 7.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.80ms | 104.21ms | 3.2× | 4.3% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 37.02ms | 39.97ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_range_select` | 16.77ms | 14.10ms | 0.8× | 2.2% | PASS |
| mem_reads | `oltp_sum_range` | 15.35ms | 14.02ms | 0.9× | 1.7% | PASS |
| mem_reads | `oltp_order_range` | 3.28ms | 3.17ms | 1.0× | 1.3% | PASS |
| mem_reads | `oltp_distinct_range` | 4.40ms | 4.37ms | 1.0× | 1.6% | PASS |
| mem_reads | `oltp_index_scan` | 4.11ms | 6.61ms | 1.6× | 1.8% | PASS |
| mem_reads | `select_random_points` | 22.59ms | 22.00ms | 1.0× | 2.7% | PASS |
| mem_reads | `select_random_ranges` | 6.87ms | 6.74ms | 1.0× | 2.0% | PASS |
| mem_reads | `covering_index_scan` | 7.92ms | 9.88ms | 1.2× | 2.0% | PASS |
| mem_reads | `groupby_scan` | 33.07ms | 33.79ms | 1.0× | 1.1% | PASS |
| mem_reads | `index_join` | 9.85ms | 9.03ms | 0.9× | 1.6% | PASS |
| mem_reads | `index_join_scan` | 3.63ms | 5.63ms | 1.5× | 1.6% | PASS |
| mem_reads | `types_table_scan` | 1.06s | 1.17s | 1.1× | 0.8% | PASS |
| mem_reads | `table_scan` | 1.29s | 1.31s | 1.0× | 2.7% | PASS |
| mem_reads | `oltp_read_only` | 136.52ms | 138.11ms | 1.0× | 1.7% | PASS |
| mem_writes | `oltp_bulk_insert` | 245.62ms | 365.30ms | 1.5× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 18.55ms | 38.56ms | 2.1× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 62.75ms | 134.99ms | 2.2× | 0.7% | PASS |
| mem_writes | `oltp_update_non_index` | 47.78ms | 81.72ms | 1.7× | 2.3% | PASS |
| mem_writes | `oltp_delete_insert` | 52.14ms | 104.76ms | 2.0× | 1.4% | PASS |
| mem_writes | `oltp_write_only` | 27.52ms | 62.14ms | 2.3× | 0.9% | PASS |
| mem_writes | `types_delete_insert` | 37.73ms | 54.22ms | 1.4× | 1.3% | PASS |
| mem_writes | `oltp_read_write` | 96.03ms | 145.22ms | 1.5× | 2.8% | PASS |
| file_reads | `oltp_point_select` | 104.77ms | 58.23ms | 0.6× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 23.13ms | 15.97ms | 0.7× | 1.7% | PASS |
| file_reads | `oltp_sum_range` | 21.78ms | 15.86ms | 0.7× | 2.0% | PASS |
| file_reads | `oltp_order_range` | 4.05ms | 3.42ms | 0.8× | 2.0% | PASS |
| file_reads | `oltp_distinct_range` | 5.34ms | 4.65ms | 0.9× | 2.6% | PASS |
| file_reads | `oltp_index_scan` | 11.03ms | 8.53ms | 0.8× | 1.6% | PASS |
| file_reads | `select_random_points` | 28.88ms | 23.96ms | 0.8× | 2.2% | PASS |
| file_reads | `select_random_ranges` | 13.72ms | 8.73ms | 0.6× | 1.5% | PASS |
| file_reads | `covering_index_scan` | 15.50ms | 12.02ms | 0.8× | 2.0% | PASS |
| file_reads | `groupby_scan` | 34.40ms | 34.95ms | 1.0× | 1.7% | PASS |
| file_reads | `index_join` | 14.17ms | 10.93ms | 0.8× | 2.9% | PASS |
| file_reads | `index_join_scan` | 4.66ms | 6.01ms | 1.3× | 2.0% | PASS |
| file_reads | `types_table_scan` | 1.06s | 1.16s | 1.1× | 0.7% | PASS |
| file_reads | `table_scan` | 1.21s | 1.28s | 1.1× | 1.2% | PASS |
| file_reads | `oltp_read_only` | 243.75ms | 167.62ms | 0.7× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 266.86ms | 377.54ms | 1.4× | 1.0% | PASS |
| file_writes | `oltp_insert` | 25.65ms | 45.02ms | 1.8× | 1.9% | PASS |
| file_writes | `oltp_update_index` | 98.15ms | 155.63ms | 1.6× | 1.5% | PASS |
| file_writes | `oltp_update_non_index` | 84.07ms | 96.69ms | 1.2× | 8.6% | PASS |
| file_writes | `oltp_delete_insert` | 87.28ms | 120.89ms | 1.4× | 1.2% | PASS |
| file_writes | `oltp_write_only` | 57.93ms | 75.36ms | 1.3× | 2.8% | PASS |
| file_writes | `types_delete_insert` | 64.13ms | 67.27ms | 1.0× | 1.4% | PASS |
| file_writes | `oltp_read_write` | 128.84ms | 159.07ms | 1.2× | 1.8% | PASS |
| ac_reads | `oltp_point_select` | 58.96ms | 58.69ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_range_select` | 18.83ms | 15.95ms | 0.8× | 1.3% | PASS |
| ac_reads | `oltp_sum_range` | 17.72ms | 15.94ms | 0.9× | 1.1% | PASS |
| ac_reads | `oltp_order_range` | 3.80ms | 3.44ms | 0.9× | 1.5% | PASS |
| ac_reads | `oltp_distinct_range` | 4.97ms | 4.64ms | 0.9× | 2.3% | PASS |
| ac_reads | `oltp_index_scan` | 6.71ms | 8.50ms | 1.3× | 1.2% | PASS |
| ac_reads | `select_random_points` | 25.38ms | 24.56ms | 1.0× | 2.3% | PASS |
| ac_reads | `select_random_ranges` | 9.37ms | 8.74ms | 0.9× | 1.2% | PASS |
| ac_reads | `covering_index_scan` | 10.71ms | 11.97ms | 1.1× | 1.1% | PASS |
| ac_reads | `groupby_scan` | 34.04ms | 34.97ms | 1.0× | 0.7% | PASS |
| ac_reads | `index_join` | 12.18ms | 10.95ms | 0.9× | 1.8% | PASS |
| ac_reads | `index_join_scan` | 4.22ms | 6.06ms | 1.4× | 1.8% | PASS |
| ac_reads | `types_table_scan` | 1.11s | 1.18s | 1.1× | 2.7% | PASS |
| ac_reads | `table_scan` | 1.26s | 1.29s | 1.0× | 1.8% | PASS |
| ac_reads | `oltp_read_only` | 178.66ms | 168.95ms | 0.9× | 1.4% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 26.00ms | 80.81ms | 3.1× | 7.2% | PASS |
| ac_writes | `oltp_insert_ac` | 30.13ms | 107.40ms | 3.6× | 8.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 29.79ms | 111.92ms | 3.8× | 8.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.33ms | 88.88ms | 3.8× | 7.9% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 28.29ms | 99.97ms | 3.5× | 8.9% | PASS |
| ac_writes | `oltp_write_only_ac` | 28.67ms | 102.09ms | 3.6× | 8.7% | PASS |
| ac_writes | `types_delete_insert_ac` | 26.49ms | 97.28ms | 3.7× | 8.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 34.81ms | 108.50ms | 3.1× | 6.6% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 26.05ms | 30.02ms | 1.2× | 1.3% | PASS |
| mem_reads | `oltp_range_select` | 15.33ms | 15.86ms | 1.0× | 1.0% | PASS |
| mem_reads | `oltp_sum_range` | 14.01ms | 15.33ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_order_range` | 2.91ms | 2.99ms | 1.0× | 1.0% | PASS |
| mem_reads | `oltp_distinct_range` | 3.77ms | 3.86ms | 1.0× | 2.0% | PASS |
| mem_reads | `oltp_index_scan` | 3.60ms | 4.45ms | 1.2× | 1.5% | PASS |
| mem_reads | `select_random_points` | 21.34ms | 24.21ms | 1.1× | 1.1% | PASS |
| mem_reads | `select_random_ranges` | 5.88ms | 6.53ms | 1.1× | 0.8% | PASS |
| mem_reads | `covering_index_scan` | 5.87ms | 6.92ms | 1.2× | 1.0% | PASS |
| mem_reads | `groupby_scan` | 29.54ms | 31.96ms | 1.1× | 0.7% | PASS |
| mem_reads | `index_join` | 6.21ms | 7.71ms | 1.2× | 1.2% | PASS |
| mem_reads | `index_join_scan` | 3.23ms | 4.58ms | 1.4× | 1.0% | PASS |
| mem_reads | `types_table_scan` | 881.34ms | 957.30ms | 1.1× | 0.4% | PASS |
| mem_reads | `table_scan` | 1.02s | 1.06s | 1.0× | 0.6% | PASS |
| mem_reads | `oltp_read_only` | 118.25ms | 127.21ms | 1.1× | 1.4% | PASS |
| mem_writes | `oltp_bulk_insert` | 191.75ms | 253.31ms | 1.3× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 14.92ms | 25.82ms | 1.7× | 0.5% | PASS |
| mem_writes | `oltp_update_index` | 52.41ms | 92.23ms | 1.8× | 1.2% | PASS |
| mem_writes | `oltp_update_non_index` | 40.71ms | 57.78ms | 1.4× | 0.9% | PASS |
| mem_writes | `oltp_delete_insert` | 38.85ms | 70.80ms | 1.8× | 0.8% | PASS |
| mem_writes | `oltp_write_only` | 21.51ms | 42.43ms | 2.0× | 0.9% | PASS |
| mem_writes | `types_delete_insert` | 25.62ms | 37.69ms | 1.5× | 1.2% | PASS |
| mem_writes | `oltp_read_write` | 77.11ms | 115.78ms | 1.5× | 1.4% | PASS |
| file_reads | `oltp_point_select` | 91.83ms | 46.75ms | 0.5× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 22.68ms | 17.86ms | 0.8× | 1.0% | PASS |
| file_reads | `oltp_sum_range` | 20.89ms | 17.24ms | 0.8× | 0.8% | PASS |
| file_reads | `oltp_order_range` | 3.67ms | 3.29ms | 0.9× | 1.5% | PASS |
| file_reads | `oltp_distinct_range` | 4.63ms | 4.14ms | 0.9× | 1.9% | PASS |
| file_reads | `oltp_index_scan` | 10.35ms | 6.62ms | 0.6× | 1.2% | PASS |
| file_reads | `select_random_points` | 28.29ms | 26.01ms | 0.9× | 1.2% | PASS |
| file_reads | `select_random_ranges` | 12.66ms | 8.43ms | 0.7× | 0.9% | PASS |
| file_reads | `covering_index_scan` | 12.73ms | 9.06ms | 0.7× | 0.7% | PASS |
| file_reads | `groupby_scan` | 30.43ms | 32.44ms | 1.1× | 0.8% | PASS |
| file_reads | `index_join` | 9.96ms | 9.48ms | 1.0× | 0.9% | PASS |
| file_reads | `index_join_scan` | 4.00ms | 4.98ms | 1.2× | 1.0% | PASS |
| file_reads | `types_table_scan` | 884.07ms | 956.89ms | 1.1× | 0.7% | PASS |
| file_reads | `table_scan` | 1.03s | 1.06s | 1.0× | 0.6% | PASS |
| file_reads | `oltp_read_only` | 213.61ms | 151.97ms | 0.7× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 242.17ms | 301.62ms | 1.2× | 1.1% | PASS |
| file_writes | `oltp_insert` | 25.63ms | 37.80ms | 1.5× | 11.1% | PASS |
| file_writes | `oltp_update_index` | 155.32ms | 150.73ms | 1.0× | 2.5% | PASS |
| file_writes | `oltp_update_non_index` | 134.69ms | 109.86ms | 0.8× | 3.7% | PASS |
| file_writes | `oltp_delete_insert` | 130.95ms | 127.99ms | 1.0× | 3.9% | PASS |
| file_writes | `oltp_write_only` | 90.85ms | 89.73ms | 1.0× | 2.8% | PASS |
| file_writes | `types_delete_insert` | 80.32ms | 74.12ms | 0.9× | 10.8% | PASS |
| file_writes | `oltp_read_write` | 150.84ms | 164.78ms | 1.1× | 3.3% | PASS |
| ac_reads | `oltp_point_select` | 48.06ms | 47.12ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 18.31ms | 17.95ms | 1.0× | 0.6% | PASS |
| ac_reads | `oltp_sum_range` | 16.82ms | 17.21ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_order_range` | 3.33ms | 3.29ms | 1.0× | 0.5% | PASS |
| ac_reads | `oltp_distinct_range` | 4.17ms | 4.15ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_index_scan` | 6.24ms | 6.70ms | 1.1× | 0.8% | PASS |
| ac_reads | `select_random_points` | 24.26ms | 26.26ms | 1.1× | 1.0% | PASS |
| ac_reads | `select_random_ranges` | 8.41ms | 8.48ms | 1.0× | 0.8% | PASS |
| ac_reads | `covering_index_scan` | 8.53ms | 9.07ms | 1.1× | 0.9% | PASS |
| ac_reads | `groupby_scan` | 30.08ms | 32.38ms | 1.1× | 0.7% | PASS |
| ac_reads | `index_join` | 7.87ms | 9.55ms | 1.2× | 0.6% | PASS |
| ac_reads | `index_join_scan` | 3.63ms | 4.98ms | 1.4× | 0.5% | PASS |
| ac_reads | `types_table_scan` | 881.51ms | 956.60ms | 1.1× | 0.5% | PASS |
| ac_reads | `table_scan` | 1.02s | 1.06s | 1.0× | 0.6% | PASS |
| ac_reads | `oltp_read_only` | 149.48ms | 151.14ms | 1.0× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 28.68ms | 74.53ms | 2.6× | 6.5% | PASS |
| ac_writes | `oltp_insert_ac` | 32.43ms | 90.33ms | 2.8× | 6.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 34.53ms | 101.77ms | 2.9× | 5.4% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 29.20ms | 86.01ms | 2.9× | 8.1% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 31.12ms | 94.20ms | 3.0× | 7.0% | PASS |
| ac_writes | `oltp_write_only_ac` | 31.49ms | 92.58ms | 2.9× | 7.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 32.83ms | 81.36ms | 2.5× | 13.0% | PASS |
| ac_writes | `oltp_read_write_ac` | 36.24ms | 99.99ms | 2.8× | 6.2% | PASS |

</details>

</details>

## Version-control latency

Wall time: 5m 10s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 32.51ms | 130.00ms | 25.0% | 1.0% | PASS |
| `status_dirty_many_tables` | 36.31ms | 130.00ms | 27.9% | 1.3% | PASS |
| `diff_regular_working_one_table` | 29.27ms | 120.00ms | 24.4% | 0.9% | PASS |
| `diff_regular_working_many_tables` | 42.47ms | 140.00ms | 30.3% | 0.8% | PASS |
| `diff_stat_working_many_tables` | 42.54ms | 140.00ms | 30.4% | 0.6% | PASS |
| `diff_schema_working_many_tables` | 42.90ms | 140.00ms | 30.6% | 1.0% | PASS |
| `branch_list_many_branches` | 20.59ms | 35.00ms | 58.8% | 1.6% | PASS |
| `branch_create_delete` | 31.42ms | 40.00ms | 78.6% | 1.7% | PASS |
| `at_literal_deep_history` | 24.53ms | 100.00ms | 24.5% | 1.0% | PASS |
| `diff_literal_deep_history` | 24.56ms | 120.00ms | 20.5% | 1.5% | PASS |
| `history_literal_deep_history` | 25.86ms | 150.00ms | 17.2% | 0.7% | PASS |
| `checkout_branch_clean` | 97.14ms | 150.00ms | 64.8% | 0.5% | PASS |
| `merge_data_no_conflicts` | 38.05ms | 50.00ms | 76.1% | 0.9% | PASS |
| `merge_data_secondary_index` | 1.78s | 2.50s | 71.0% | 0.5% | PASS |
| `merge_schema_no_conflicts` | 21.10ms | 35.00ms | 60.3% | 2.2% | PASS |
| `merge_data_conflicts` | 26.90ms | 180.00ms | 14.9% | 1.2% | PASS |
| `merge_data_conflicts_with_resolve` | 28.04ms | 180.00ms | 15.6% | 0.9% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
