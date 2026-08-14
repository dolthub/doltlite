# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-08-14 11:24 UTC
>
> Commit: [`b29e5ff7b968c3d770beaf941ced83b87fa9a573`](https://github.com/dolthub/doltlite/commit/b29e5ff7b968c3d770beaf941ced83b87fa9a573)
>
> Runner: ubuntu24 20260810.271.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/31789554476)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 11.51s | 11.98s | 1.0× | 1.5% | **PASS** |
| Writes | 2.28s | 3.62s | 1.6× | 1.4% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 12.40s | 12.18s | 1.0× | 1.4% | **PASS** |
| Writes | 3.09s | 3.90s | 1.3× | 1.8% | **PASS** |
| Autocommit writes | 716.04ms | 2.67s | 3.7× | 5.5% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.68s | 2.88s | 1.1× | 1.8% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.74s | 2.96s | 1.1× | 1.4% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 3.25s | 3.12s | 1.0× | 1.7% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.84s | 3.03s | 1.1× | 1.3% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 470.02ms | 717.83ms | 1.5× | 1.6% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 571.17ms | 952.93ms | 1.7× | 1.2% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 615.66ms | 994.19ms | 1.6× | 2.0% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 618.43ms | 955.24ms | 1.5× | 1.4% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.83s | 2.88s | 1.0× | 1.5% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.92s | 3.00s | 1.0× | 1.4% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 3.56s | 3.19s | 0.9× | 1.5% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 3.08s | 3.11s | 1.0× | 1.2% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 615.43ms | 765.47ms | 1.2× | 2.4% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 890.06ms | 1.05s | 1.2× | 3.8% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 818.17ms | 1.07s | 1.3× | 1.6% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 764.48ms | 1.01s | 1.3× | 1.7% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.66s | 2.88s | 1.1× | 1.6% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.78s | 3.00s | 1.1× | 1.1% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 3.36s | 3.18s | 0.9× | 2.0% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.82s | 3.06s | 1.1× | 0.9% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 207.81ms | 779.30ms | 3.8× | 7.8% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 194.85ms | 682.26ms | 3.5× | 4.3% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 162.22ms | 618.76ms | 3.8× | 5.0% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 151.16ms | 587.35ms | 3.9× | 5.6% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 25.27ms | 28.68ms | 1.1× | 2.4% | PASS |
| mem_reads | `oltp_range_select` | 11.33ms | 11.39ms | 1.0× | 2.1% | PASS |
| mem_reads | `oltp_sum_range` | 10.18ms | 11.37ms | 1.1× | 1.8% | PASS |
| mem_reads | `oltp_order_range` | 2.63ms | 2.85ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 3.72ms | 3.95ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 4.06ms | 5.37ms | 1.3× | 1.8% | PASS |
| mem_reads | `select_random_points` | 11.36ms | 11.15ms | 1.0× | 4.4% | PASS |
| mem_reads | `select_random_ranges` | 3.10ms | 3.88ms | 1.3× | 1.4% | PASS |
| mem_reads | `covering_index_scan` | 4.25ms | 4.39ms | 1.0× | 1.9% | PASS |
| mem_reads | `groupby_scan` | 30.51ms | 32.55ms | 1.1× | 0.6% | PASS |
| mem_reads | `index_join` | 6.17ms | 8.39ms | 1.4× | 2.2% | PASS |
| mem_reads | `index_join_scan` | 3.85ms | 4.62ms | 1.2× | 3.4% | PASS |
| mem_reads | `types_table_scan` | 1.14s | 1.24s | 1.1× | 0.5% | PASS |
| mem_reads | `table_scan` | 1.31s | 1.38s | 1.1× | 0.5% | PASS |
| mem_reads | `oltp_read_only` | 113.31ms | 121.47ms | 1.1× | 1.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 179.25ms | 248.53ms | 1.4× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 16.17ms | 27.75ms | 1.7× | 1.1% | PASS |
| mem_writes | `oltp_update_index` | 56.26ms | 99.11ms | 1.8× | 1.9% | PASS |
| mem_writes | `oltp_update_non_index` | 37.75ms | 56.98ms | 1.5× | 2.2% | PASS |
| mem_writes | `oltp_delete_insert` | 49.88ms | 77.22ms | 1.5× | 1.9% | PASS |
| mem_writes | `oltp_write_only` | 24.26ms | 48.31ms | 2.0× | 2.1% | PASS |
| mem_writes | `types_delete_insert` | 26.47ms | 38.28ms | 1.4× | 1.4% | PASS |
| mem_writes | `oltp_read_write` | 79.99ms | 121.65ms | 1.5× | 1.3% | PASS |
| file_reads | `oltp_point_select` | 94.70ms | 48.95ms | 0.5× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 19.11ms | 13.69ms | 0.7× | 1.4% | PASS |
| file_reads | `oltp_sum_range` | 17.89ms | 13.67ms | 0.8× | 1.9% | PASS |
| file_reads | `oltp_order_range` | 3.53ms | 3.15ms | 0.9× | 1.6% | PASS |
| file_reads | `oltp_distinct_range` | 4.61ms | 4.26ms | 0.9× | 1.6% | PASS |
| file_reads | `oltp_index_scan` | 11.54ms | 7.55ms | 0.7× | 1.5% | PASS |
| file_reads | `select_random_points` | 19.25ms | 13.35ms | 0.7× | 2.0% | PASS |
| file_reads | `select_random_ranges` | 9.93ms | 5.74ms | 0.6× | 0.9% | PASS |
| file_reads | `covering_index_scan` | 11.39ms | 6.33ms | 0.6× | 1.8% | PASS |
| file_reads | `groupby_scan` | 31.51ms | 32.96ms | 1.0× | 0.8% | PASS |
| file_reads | `index_join` | 10.64ms | 9.97ms | 0.9× | 2.9% | PASS |
| file_reads | `index_join_scan` | 4.65ms | 4.94ms | 1.1× | 3.8% | PASS |
| file_reads | `types_table_scan` | 1.10s | 1.21s | 1.1× | 1.3% | PASS |
| file_reads | `table_scan` | 1.28s | 1.37s | 1.1× | 0.8% | PASS |
| file_reads | `oltp_read_only` | 209.34ms | 147.26ms | 0.7× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 192.64ms | 255.02ms | 1.3× | 1.4% | PASS |
| file_writes | `oltp_insert` | 22.98ms | 31.36ms | 1.4× | 3.5% | PASS |
| file_writes | `oltp_update_index` | 82.28ms | 105.83ms | 1.3× | 1.9% | PASS |
| file_writes | `oltp_update_non_index` | 60.72ms | 68.19ms | 1.1× | 2.5% | PASS |
| file_writes | `oltp_delete_insert` | 70.82ms | 84.06ms | 1.2× | 2.4% | PASS |
| file_writes | `oltp_write_only` | 46.70ms | 54.87ms | 1.2× | 2.6% | PASS |
| file_writes | `types_delete_insert` | 41.54ms | 43.38ms | 1.0× | 2.6% | PASS |
| file_writes | `oltp_read_write` | 97.74ms | 122.75ms | 1.3× | 1.8% | PASS |
| ac_reads | `oltp_point_select` | 47.30ms | 47.03ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_range_select` | 13.78ms | 13.30ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_sum_range` | 12.68ms | 13.27ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_order_range` | 2.98ms | 3.12ms | 1.1× | 1.6% | PASS |
| ac_reads | `oltp_distinct_range` | 4.01ms | 4.23ms | 1.1× | 2.0% | PASS |
| ac_reads | `oltp_index_scan` | 6.68ms | 7.44ms | 1.1× | 2.0% | PASS |
| ac_reads | `select_random_points` | 13.86ms | 12.96ms | 0.9× | 3.3% | PASS |
| ac_reads | `select_random_ranges` | 5.46ms | 5.77ms | 1.1× | 1.7% | PASS |
| ac_reads | `covering_index_scan` | 7.06ms | 6.36ms | 0.9× | 1.5% | PASS |
| ac_reads | `groupby_scan` | 30.83ms | 32.84ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 7.91ms | 9.59ms | 1.2× | 2.5% | PASS |
| ac_reads | `index_join_scan` | 4.13ms | 4.83ms | 1.2× | 3.6% | PASS |
| ac_reads | `types_table_scan` | 1.10s | 1.21s | 1.1× | 1.3% | PASS |
| ac_reads | `table_scan` | 1.25s | 1.36s | 1.1× | 0.9% | PASS |
| ac_reads | `oltp_read_only` | 142.85ms | 147.08ms | 1.0× | 1.6% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 24.02ms | 86.19ms | 3.6× | 8.4% | PASS |
| ac_writes | `oltp_insert_ac` | 26.15ms | 98.10ms | 3.8× | 8.2% | PASS |
| ac_writes | `oltp_update_index_ac` | 28.01ms | 107.09ms | 3.8× | 8.0% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.56ms | 88.02ms | 3.7× | 10.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.61ms | 99.65ms | 4.0× | 7.2% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.28ms | 102.65ms | 3.9× | 7.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 23.64ms | 90.98ms | 3.8× | 5.6% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.55ms | 106.62ms | 3.4× | 7.5% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 29.61ms | 36.79ms | 1.2× | 1.4% | PASS |
| mem_reads | `oltp_range_select` | 13.29ms | 13.39ms | 1.0× | 1.4% | PASS |
| mem_reads | `oltp_sum_range` | 11.98ms | 13.58ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_order_range` | 3.00ms | 3.12ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 4.01ms | 4.21ms | 1.0× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 4.46ms | 6.06ms | 1.4× | 1.8% | PASS |
| mem_reads | `select_random_points` | 17.68ms | 20.81ms | 1.2× | 1.3% | PASS |
| mem_reads | `select_random_ranges` | 3.98ms | 5.07ms | 1.3× | 1.5% | PASS |
| mem_reads | `covering_index_scan` | 4.51ms | 4.44ms | 1.0× | 2.3% | PASS |
| mem_reads | `groupby_scan` | 32.33ms | 34.08ms | 1.1× | 0.7% | PASS |
| mem_reads | `index_join` | 6.91ms | 8.96ms | 1.3× | 1.9% | PASS |
| mem_reads | `index_join_scan` | 4.60ms | 5.21ms | 1.1× | 2.0% | PASS |
| mem_reads | `types_table_scan` | 1.12s | 1.23s | 1.1× | 1.1% | PASS |
| mem_reads | `table_scan` | 1.36s | 1.44s | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_read_only` | 119.81ms | 134.35ms | 1.1× | 1.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 230.44ms | 347.85ms | 1.5× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 21.32ms | 37.53ms | 1.8× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 71.33ms | 133.49ms | 1.9× | 1.8% | PASS |
| mem_writes | `oltp_update_non_index` | 48.21ms | 78.24ms | 1.6× | 1.3% | PASS |
| mem_writes | `oltp_delete_insert` | 51.12ms | 102.37ms | 2.0× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 28.78ms | 59.74ms | 2.1× | 1.5% | PASS |
| mem_writes | `types_delete_insert` | 32.74ms | 52.31ms | 1.6× | 1.1% | PASS |
| mem_writes | `oltp_read_write` | 87.23ms | 141.37ms | 1.6× | 1.2% | PASS |
| file_reads | `oltp_point_select` | 99.02ms | 55.25ms | 0.6× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 20.67ms | 15.35ms | 0.7× | 2.3% | PASS |
| file_reads | `oltp_sum_range` | 19.66ms | 15.59ms | 0.8× | 1.3% | PASS |
| file_reads | `oltp_order_range` | 3.77ms | 3.38ms | 0.9× | 1.9% | PASS |
| file_reads | `oltp_distinct_range` | 4.86ms | 4.49ms | 0.9× | 1.2% | PASS |
| file_reads | `oltp_index_scan` | 11.82ms | 8.18ms | 0.7× | 1.5% | PASS |
| file_reads | `select_random_points` | 26.10ms | 23.22ms | 0.9× | 2.0% | PASS |
| file_reads | `select_random_ranges` | 11.04ms | 6.99ms | 0.6× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 12.38ms | 6.56ms | 0.5× | 2.8% | PASS |
| file_reads | `groupby_scan` | 33.34ms | 34.52ms | 1.0× | 0.9% | PASS |
| file_reads | `index_join` | 11.52ms | 10.67ms | 0.9× | 1.9% | PASS |
| file_reads | `index_join_scan` | 5.57ms | 5.62ms | 1.0× | 1.4% | PASS |
| file_reads | `types_table_scan` | 1.10s | 1.21s | 1.1× | 1.1% | PASS |
| file_reads | `table_scan` | 1.34s | 1.43s | 1.1× | 1.6% | PASS |
| file_reads | `oltp_read_only` | 223.24ms | 163.01ms | 0.7× | 1.1% | PASS |
| file_writes | `oltp_bulk_insert` | 248.38ms | 360.91ms | 1.5× | 0.7% | PASS |
| file_writes | `oltp_insert` | 48.07ms | 44.54ms | 0.9× | 15.7% | PASS |
| file_writes | `oltp_update_index` | 112.26ms | 151.41ms | 1.3× | 1.5% | PASS |
| file_writes | `oltp_update_non_index` | 100.56ms | 92.74ms | 0.9× | 5.9% | PASS |
| file_writes | `oltp_delete_insert` | 89.41ms | 116.45ms | 1.3× | 2.0% | PASS |
| file_writes | `oltp_write_only` | 90.50ms | 70.99ms | 0.8× | 10.5% | PASS |
| file_writes | `types_delete_insert` | 54.36ms | 61.45ms | 1.1× | 1.6% | PASS |
| file_writes | `oltp_read_write` | 146.52ms | 152.86ms | 1.0× | 5.7% | PASS |
| ac_reads | `oltp_point_select` | 52.64ms | 55.19ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_range_select` | 16.09ms | 15.36ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_sum_range` | 14.98ms | 15.68ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_order_range` | 3.40ms | 3.37ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_distinct_range` | 4.42ms | 4.50ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_index_scan` | 7.34ms | 8.18ms | 1.1× | 1.3% | PASS |
| ac_reads | `select_random_points` | 20.98ms | 23.37ms | 1.1× | 1.0% | PASS |
| ac_reads | `select_random_ranges` | 6.59ms | 6.99ms | 1.1× | 0.6% | PASS |
| ac_reads | `covering_index_scan` | 8.07ms | 6.55ms | 0.8× | 1.2% | PASS |
| ac_reads | `groupby_scan` | 32.79ms | 34.45ms | 1.1× | 0.7% | PASS |
| ac_reads | `index_join` | 9.28ms | 10.64ms | 1.1× | 1.5% | PASS |
| ac_reads | `index_join_scan` | 5.12ms | 5.65ms | 1.1× | 1.9% | PASS |
| ac_reads | `types_table_scan` | 1.10s | 1.22s | 1.1× | 0.9% | PASS |
| ac_reads | `table_scan` | 1.34s | 1.43s | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_read_only` | 154.22ms | 162.83ms | 1.1× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.16ms | 70.71ms | 3.3× | 3.2% | PASS |
| ac_writes | `oltp_insert_ac` | 24.29ms | 82.58ms | 3.4× | 3.1% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.62ms | 96.22ms | 3.6× | 3.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.59ms | 80.36ms | 3.7× | 5.4% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.44ms | 88.70ms | 3.6× | 4.8% | PASS |
| ac_writes | `oltp_write_only_ac` | 24.27ms | 87.89ms | 3.6× | 3.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 20.79ms | 81.70ms | 3.9× | 5.8% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.68ms | 94.11ms | 3.0× | 5.4% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.08ms | 36.58ms | 1.1× | 1.7% | PASS |
| mem_reads | `oltp_range_select` | 15.25ms | 14.19ms | 0.9× | 2.2% | PASS |
| mem_reads | `oltp_sum_range` | 13.45ms | 13.71ms | 1.0× | 1.6% | PASS |
| mem_reads | `oltp_order_range` | 3.24ms | 3.20ms | 1.0× | 1.6% | PASS |
| mem_reads | `oltp_distinct_range` | 4.32ms | 4.30ms | 1.0× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 4.86ms | 6.29ms | 1.3× | 2.7% | PASS |
| mem_reads | `select_random_points` | 20.18ms | 22.55ms | 1.1× | 2.6% | PASS |
| mem_reads | `select_random_ranges` | 4.58ms | 5.38ms | 1.2× | 1.7% | PASS |
| mem_reads | `covering_index_scan` | 5.38ms | 4.94ms | 0.9× | 2.5% | PASS |
| mem_reads | `groupby_scan` | 35.33ms | 36.97ms | 1.0× | 0.9% | PASS |
| mem_reads | `index_join` | 7.71ms | 10.67ms | 1.4× | 3.0% | PASS |
| mem_reads | `index_join_scan` | 4.87ms | 6.54ms | 1.3× | 3.0% | PASS |
| mem_reads | `types_table_scan` | 1.32s | 1.35s | 1.0× | 1.7% | PASS |
| mem_reads | `table_scan` | 1.64s | 1.47s | 0.9× | 1.0% | PASS |
| mem_reads | `oltp_read_only` | 135.85ms | 137.97ms | 1.0× | 1.4% | PASS |
| mem_writes | `oltp_bulk_insert` | 238.37ms | 342.87ms | 1.4× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 22.41ms | 40.05ms | 1.8× | 2.8% | PASS |
| mem_writes | `oltp_update_index` | 80.46ms | 147.10ms | 1.8× | 2.1% | PASS |
| mem_writes | `oltp_update_non_index` | 54.23ms | 83.30ms | 1.5× | 1.8% | PASS |
| mem_writes | `oltp_delete_insert` | 55.62ms | 110.87ms | 2.0× | 2.6% | PASS |
| mem_writes | `oltp_write_only` | 32.11ms | 66.40ms | 2.1× | 2.0% | PASS |
| mem_writes | `types_delete_insert` | 34.62ms | 54.20ms | 1.6× | 1.6% | PASS |
| mem_writes | `oltp_read_write` | 97.83ms | 149.39ms | 1.5× | 3.1% | PASS |
| file_reads | `oltp_point_select` | 117.64ms | 57.56ms | 0.5× | 1.7% | PASS |
| file_reads | `oltp_range_select` | 24.27ms | 16.24ms | 0.7× | 2.7% | PASS |
| file_reads | `oltp_sum_range` | 23.18ms | 16.11ms | 0.7× | 1.9% | PASS |
| file_reads | `oltp_order_range` | 4.20ms | 3.49ms | 0.8× | 1.2% | PASS |
| file_reads | `oltp_distinct_range` | 5.30ms | 4.56ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_index_scan` | 13.90ms | 8.55ms | 0.6× | 1.3% | PASS |
| file_reads | `select_random_points` | 29.37ms | 24.12ms | 0.8× | 1.8% | PASS |
| file_reads | `select_random_ranges` | 13.17ms | 7.50ms | 0.6× | 1.3% | PASS |
| file_reads | `covering_index_scan` | 14.76ms | 7.05ms | 0.5× | 1.8% | PASS |
| file_reads | `groupby_scan` | 36.33ms | 37.12ms | 1.0× | 1.2% | PASS |
| file_reads | `index_join` | 12.69ms | 11.54ms | 0.9× | 2.5% | PASS |
| file_reads | `index_join_scan` | 5.92ms | 6.52ms | 1.1× | 3.7% | PASS |
| file_reads | `types_table_scan` | 1.34s | 1.35s | 1.0× | 1.4% | PASS |
| file_reads | `table_scan` | 1.66s | 1.47s | 0.9× | 1.3% | PASS |
| file_reads | `oltp_read_only` | 261.83ms | 168.89ms | 0.6× | 1.5% | PASS |
| file_writes | `oltp_bulk_insert` | 261.13ms | 351.73ms | 1.3× | 0.8% | PASS |
| file_writes | `oltp_insert` | 32.81ms | 46.31ms | 1.4× | 2.4% | PASS |
| file_writes | `oltp_update_index` | 113.46ms | 159.81ms | 1.4× | 1.7% | PASS |
| file_writes | `oltp_update_non_index` | 83.74ms | 96.06ms | 1.1× | 1.1% | PASS |
| file_writes | `oltp_delete_insert` | 87.71ms | 122.75ms | 1.4× | 1.7% | PASS |
| file_writes | `oltp_write_only` | 58.41ms | 75.89ms | 1.3× | 1.6% | PASS |
| file_writes | `types_delete_insert` | 54.80ms | 62.85ms | 1.1× | 1.5% | PASS |
| file_writes | `oltp_read_write` | 126.11ms | 158.35ms | 1.3× | 2.4% | PASS |
| ac_reads | `oltp_point_select` | 61.21ms | 58.28ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 18.92ms | 16.41ms | 0.9× | 2.1% | PASS |
| ac_reads | `oltp_sum_range` | 17.08ms | 15.94ms | 0.9× | 2.0% | PASS |
| ac_reads | `oltp_order_range` | 3.75ms | 3.54ms | 0.9× | 1.4% | PASS |
| ac_reads | `oltp_distinct_range` | 4.74ms | 4.56ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_index_scan` | 8.30ms | 8.53ms | 1.0× | 2.1% | PASS |
| ac_reads | `select_random_points` | 22.65ms | 23.20ms | 1.0× | 2.0% | PASS |
| ac_reads | `select_random_ranges` | 7.60ms | 7.52ms | 1.0× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 8.82ms | 7.07ms | 0.8× | 2.3% | PASS |
| ac_reads | `groupby_scan` | 35.45ms | 37.24ms | 1.1× | 1.2% | PASS |
| ac_reads | `index_join` | 10.17ms | 11.80ms | 1.2× | 3.4% | PASS |
| ac_reads | `index_join_scan` | 5.46ms | 6.58ms | 1.2× | 3.4% | PASS |
| ac_reads | `types_table_scan` | 1.34s | 1.35s | 1.0× | 1.9% | PASS |
| ac_reads | `table_scan` | 1.64s | 1.46s | 0.9× | 2.6% | PASS |
| ac_reads | `oltp_read_only` | 181.97ms | 170.81ms | 0.9× | 1.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 17.43ms | 62.26ms | 3.6× | 3.9% | PASS |
| ac_writes | `oltp_insert_ac` | 20.03ms | 77.68ms | 3.9× | 4.3% | PASS |
| ac_writes | `oltp_update_index_ac` | 22.30ms | 86.92ms | 3.9× | 4.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 18.35ms | 70.69ms | 3.9× | 4.4% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 20.33ms | 82.99ms | 4.1× | 6.5% | PASS |
| ac_writes | `oltp_write_only_ac` | 20.38ms | 80.36ms | 3.9× | 5.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 17.67ms | 71.14ms | 4.0× | 6.0% | PASS |
| ac_writes | `oltp_read_write_ac` | 25.72ms | 86.72ms | 3.4× | 5.5% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 34.03ms | 36.56ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_range_select` | 20.01ms | 20.99ms | 1.0× | 1.3% | PASS |
| mem_reads | `oltp_sum_range` | 18.45ms | 20.15ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_order_range` | 3.77ms | 3.89ms | 1.0× | 0.6% | PASS |
| mem_reads | `oltp_distinct_range` | 4.80ms | 5.01ms | 1.0× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 4.71ms | 5.67ms | 1.2× | 1.8% | PASS |
| mem_reads | `select_random_points` | 27.77ms | 31.01ms | 1.1× | 1.3% | PASS |
| mem_reads | `select_random_ranges` | 7.60ms | 8.14ms | 1.1× | 0.7% | PASS |
| mem_reads | `covering_index_scan` | 4.36ms | 4.03ms | 0.9× | 1.4% | PASS |
| mem_reads | `groupby_scan` | 38.51ms | 42.01ms | 1.1× | 0.9% | PASS |
| mem_reads | `index_join` | 8.18ms | 10.14ms | 1.2× | 1.3% | PASS |
| mem_reads | `index_join_scan` | 4.35ms | 5.61ms | 1.3× | 1.4% | PASS |
| mem_reads | `types_table_scan` | 1.13s | 1.28s | 1.1× | 0.7% | PASS |
| mem_reads | `table_scan` | 1.38s | 1.40s | 1.0× | 3.2% | PASS |
| mem_reads | `oltp_read_only` | 152.67ms | 163.61ms | 1.1× | 1.7% | PASS |
| mem_writes | `oltp_bulk_insert` | 244.85ms | 336.19ms | 1.4× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 19.85ms | 35.43ms | 1.8× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 74.64ms | 131.50ms | 1.8× | 1.6% | PASS |
| mem_writes | `oltp_update_non_index` | 55.26ms | 80.86ms | 1.5× | 1.2% | PASS |
| mem_writes | `oltp_delete_insert` | 52.02ms | 97.95ms | 1.9× | 1.3% | PASS |
| mem_writes | `oltp_write_only` | 29.08ms | 60.48ms | 2.1× | 1.9% | PASS |
| mem_writes | `types_delete_insert` | 34.11ms | 53.51ms | 1.6× | 1.4% | PASS |
| mem_writes | `oltp_read_write` | 108.62ms | 159.32ms | 1.5× | 2.3% | PASS |
| file_reads | `oltp_point_select` | 119.53ms | 59.97ms | 0.5× | 0.7% | PASS |
| file_reads | `oltp_range_select` | 29.19ms | 23.21ms | 0.8× | 2.0% | PASS |
| file_reads | `oltp_sum_range` | 26.76ms | 22.20ms | 0.8× | 1.1% | PASS |
| file_reads | `oltp_order_range` | 4.60ms | 4.13ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_distinct_range` | 5.70ms | 5.29ms | 0.9× | 1.2% | PASS |
| file_reads | `oltp_index_scan` | 13.33ms | 8.21ms | 0.6× | 1.5% | PASS |
| file_reads | `select_random_points` | 37.02ms | 33.58ms | 0.9× | 1.2% | PASS |
| file_reads | `select_random_ranges` | 16.28ms | 10.54ms | 0.6× | 1.4% | PASS |
| file_reads | `covering_index_scan` | 13.15ms | 6.71ms | 0.5× | 1.2% | PASS |
| file_reads | `groupby_scan` | 39.56ms | 42.28ms | 1.1× | 1.0% | PASS |
| file_reads | `index_join` | 13.01ms | 12.17ms | 0.9× | 1.9% | PASS |
| file_reads | `index_join_scan` | 5.32ms | 6.03ms | 1.1× | 1.7% | PASS |
| file_reads | `types_table_scan` | 1.17s | 1.30s | 1.1× | 2.0% | PASS |
| file_reads | `table_scan` | 1.31s | 1.38s | 1.0× | 1.2% | PASS |
| file_reads | `oltp_read_only` | 273.14ms | 195.77ms | 0.7× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 260.89ms | 345.58ms | 1.3× | 1.2% | PASS |
| file_writes | `oltp_insert` | 26.25ms | 40.95ms | 1.6× | 1.9% | PASS |
| file_writes | `oltp_update_index` | 99.67ms | 135.15ms | 1.4× | 1.1% | PASS |
| file_writes | `oltp_update_non_index` | 78.64ms | 90.91ms | 1.2× | 1.4% | PASS |
| file_writes | `oltp_delete_insert` | 77.25ms | 106.70ms | 1.4× | 1.7% | PASS |
| file_writes | `oltp_write_only` | 51.24ms | 68.98ms | 1.3× | 2.2% | PASS |
| file_writes | `types_delete_insert` | 49.72ms | 57.35ms | 1.2× | 2.0% | PASS |
| file_writes | `oltp_read_write` | 120.81ms | 160.97ms | 1.3× | 1.6% | PASS |
| ac_reads | `oltp_point_select` | 60.69ms | 58.15ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_range_select` | 22.26ms | 22.92ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_sum_range` | 20.90ms | 22.19ms | 1.1× | 0.8% | PASS |
| ac_reads | `oltp_order_range` | 4.02ms | 4.12ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_distinct_range` | 5.07ms | 5.27ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_index_scan` | 7.56ms | 7.96ms | 1.1× | 1.7% | PASS |
| ac_reads | `select_random_points` | 30.14ms | 33.35ms | 1.1× | 1.1% | PASS |
| ac_reads | `select_random_ranges` | 10.36ms | 10.45ms | 1.0× | 0.9% | PASS |
| ac_reads | `covering_index_scan` | 7.30ms | 6.36ms | 0.9× | 1.1% | PASS |
| ac_reads | `groupby_scan` | 38.38ms | 42.27ms | 1.1× | 1.0% | PASS |
| ac_reads | `index_join` | 9.81ms | 11.62ms | 1.2× | 1.2% | PASS |
| ac_reads | `index_join_scan` | 4.61ms | 5.87ms | 1.3× | 1.0% | PASS |
| ac_reads | `types_table_scan` | 1.12s | 1.27s | 1.1× | 0.8% | PASS |
| ac_reads | `table_scan` | 1.29s | 1.37s | 1.1× | 0.5% | PASS |
| ac_reads | `oltp_read_only` | 191.69ms | 195.22ms | 1.0× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 16.09ms | 57.92ms | 3.6× | 4.8% | PASS |
| ac_writes | `oltp_insert_ac` | 19.30ms | 74.74ms | 3.9× | 5.2% | PASS |
| ac_writes | `oltp_update_index_ac` | 20.37ms | 84.42ms | 4.1× | 5.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 16.57ms | 66.29ms | 4.0× | 5.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 18.53ms | 76.34ms | 4.1× | 5.7% | PASS |
| ac_writes | `oltp_write_only_ac` | 18.58ms | 75.96ms | 4.1× | 5.5% | PASS |
| ac_writes | `types_delete_insert_ac` | 16.06ms | 66.18ms | 4.1× | 6.0% | PASS |
| ac_writes | `oltp_read_write_ac` | 25.67ms | 85.49ms | 3.3× | 4.2% | PASS |

</details>

</details>

## Version-control latency

Wall time: 2m 16s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 80.08ms | 130.00ms | 61.6% | 0.4% | PASS |
| `status_dirty_many_tables` | 83.52ms | 130.00ms | 64.2% | 0.4% | PASS |
| `diff_regular_working_one_table` | 77.93ms | 120.00ms | 64.9% | 0.3% | PASS |
| `diff_regular_working_many_tables` | 88.10ms | 140.00ms | 62.9% | 0.4% | PASS |
| `diff_stat_working_many_tables` | 88.06ms | 140.00ms | 62.9% | 0.4% | PASS |
| `diff_schema_working_many_tables` | 88.76ms | 140.00ms | 63.4% | 0.5% | PASS |
| `branch_list_many_branches` | 21.89ms | 35.00ms | 62.5% | 0.5% | PASS |
| `branch_create_delete` | 24.82ms | 40.00ms | 62.0% | 1.2% | PASS |
| `checkout_branch_clean` | 53.58ms | 150.00ms | 35.7% | 0.8% | PASS |
| `merge_data_no_conflicts` | 28.36ms | 50.00ms | 56.7% | 0.8% | PASS |
| `merge_schema_no_conflicts` | 21.52ms | 35.00ms | 61.5% | 1.1% | PASS |
| `merge_data_conflicts` | 125.90ms | 180.00ms | 69.9% | 0.3% | PASS |
| `merge_data_conflicts_with_resolve` | 126.06ms | 180.00ms | 70.0% | 0.3% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
