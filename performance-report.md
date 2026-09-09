# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-09-09 11:15 UTC
>
> Commit: [`4e828ca6620a937154852d079d7a2af2d45b6bf0`](https://github.com/dolthub/doltlite/commit/4e828ca6620a937154852d079d7a2af2d45b6bf0)
>
> Runner: ubuntu24 20260907.300.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/34335615498)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.88s | 11.67s | 1.1× | 1.4% | **PASS** |
| Writes | 2.26s | 3.68s | 1.6× | 1.4% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 12.04s | 11.94s | 1.0× | 1.5% | **PASS** |
| Writes | 3.17s | 4.04s | 1.3× | 1.7% | **PASS** |
| Autocommit writes | 837.40ms | 3.07s | 3.7× | 5.4% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.59s | 2.75s | 1.1× | 0.8% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.72s | 2.94s | 1.1× | 1.9% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.76s | 2.94s | 1.1× | 1.9% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.81s | 3.03s | 1.1× | 1.4% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 437.44ms | 705.94ms | 1.6× | 0.9% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 601.43ms | 1.01s | 1.7× | 1.6% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 614.72ms | 984.03ms | 1.6× | 1.5% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 604.16ms | 981.52ms | 1.6× | 1.6% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.91s | 2.84s | 1.0× | 1.2% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 3.02s | 3.00s | 1.0× | 2.0% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 3.03s | 2.99s | 1.0× | 1.6% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 3.08s | 3.10s | 1.0× | 1.4% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 582.28ms | 769.96ms | 1.3× | 1.3% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 958.10ms | 1.12s | 1.2× | 4.0% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 824.08ms | 1.08s | 1.3× | 2.2% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 804.05ms | 1.07s | 1.3× | 1.8% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.80s | 2.85s | 1.0× | 1.4% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.72s | 2.99s | 1.1× | 2.2% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.83s | 2.98s | 1.1× | 1.3% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.96s | 3.12s | 1.1× | 1.4% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 137.85ms | 553.75ms | 4.0× | 3.5% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 211.79ms | 761.29ms | 3.6× | 5.4% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 161.14ms | 606.12ms | 3.8× | 5.2% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 326.62ms | 1.15s | 3.5× | 9.8% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.15ms | 27.09ms | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_range_select` | 10.54ms | 10.83ms | 1.0× | 0.6% | PASS |
| mem_reads | `oltp_sum_range` | 9.57ms | 10.77ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_order_range` | 2.64ms | 2.78ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_distinct_range` | 3.73ms | 3.94ms | 1.1× | 0.6% | PASS |
| mem_reads | `oltp_index_scan` | 3.87ms | 4.66ms | 1.2× | 0.6% | PASS |
| mem_reads | `select_random_points` | 10.04ms | 10.51ms | 1.0× | 0.8% | PASS |
| mem_reads | `select_random_ranges` | 3.01ms | 3.87ms | 1.3× | 1.4% | PASS |
| mem_reads | `covering_index_scan` | 4.28ms | 4.07ms | 1.0× | 0.8% | PASS |
| mem_reads | `groupby_scan` | 32.28ms | 34.83ms | 1.1× | 1.1% | PASS |
| mem_reads | `index_join` | 5.72ms | 7.47ms | 1.3× | 0.9% | PASS |
| mem_reads | `index_join_scan` | 3.23ms | 4.54ms | 1.4× | 1.4% | PASS |
| mem_reads | `types_table_scan` | 1.11s | 1.20s | 1.1× | 0.6% | PASS |
| mem_reads | `table_scan` | 1.26s | 1.31s | 1.0× | 0.4% | PASS |
| mem_reads | `oltp_read_only` | 102.79ms | 113.71ms | 1.1× | 0.5% | PASS |
| mem_writes | `oltp_bulk_insert` | 183.80ms | 259.74ms | 1.4× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 15.63ms | 27.81ms | 1.8× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 49.57ms | 94.44ms | 1.9× | 0.8% | PASS |
| mem_writes | `oltp_update_non_index` | 34.95ms | 58.37ms | 1.7× | 0.9% | PASS |
| mem_writes | `oltp_delete_insert` | 43.85ms | 72.58ms | 1.7× | 0.8% | PASS |
| mem_writes | `oltp_write_only` | 21.47ms | 47.46ms | 2.2× | 0.8% | PASS |
| mem_writes | `types_delete_insert` | 24.06ms | 37.42ms | 1.6× | 1.1% | PASS |
| mem_writes | `oltp_read_write` | 64.11ms | 108.12ms | 1.7× | 0.8% | PASS |
| file_reads | `oltp_point_select` | 106.95ms | 48.80ms | 0.5× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 19.23ms | 13.25ms | 0.7× | 1.6% | PASS |
| file_reads | `oltp_sum_range` | 18.59ms | 13.25ms | 0.7× | 1.1% | PASS |
| file_reads | `oltp_order_range` | 3.63ms | 3.16ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_distinct_range` | 4.71ms | 4.25ms | 0.9× | 1.1% | PASS |
| file_reads | `oltp_index_scan` | 12.76ms | 7.50ms | 0.6× | 0.7% | PASS |
| file_reads | `select_random_points` | 18.43ms | 13.42ms | 0.7× | 2.9% | PASS |
| file_reads | `select_random_ranges` | 11.21ms | 6.10ms | 0.5× | 1.3% | PASS |
| file_reads | `covering_index_scan` | 12.74ms | 6.49ms | 0.5× | 1.2% | PASS |
| file_reads | `groupby_scan` | 33.17ms | 35.35ms | 1.1× | 1.1% | PASS |
| file_reads | `index_join` | 10.29ms | 9.36ms | 0.9× | 2.0% | PASS |
| file_reads | `index_join_scan` | 4.32ms | 5.12ms | 1.2× | 2.6% | PASS |
| file_reads | `types_table_scan` | 1.15s | 1.22s | 1.1× | 1.7% | PASS |
| file_reads | `table_scan` | 1.28s | 1.32s | 1.0× | 0.9% | PASS |
| file_reads | `oltp_read_only` | 224.48ms | 146.14ms | 0.7× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 196.13ms | 269.45ms | 1.4× | 1.0% | PASS |
| file_writes | `oltp_insert` | 21.75ms | 31.97ms | 1.5× | 1.8% | PASS |
| file_writes | `oltp_update_index` | 76.33ms | 104.52ms | 1.4× | 1.2% | PASS |
| file_writes | `oltp_update_non_index` | 57.39ms | 69.91ms | 1.2× | 1.1% | PASS |
| file_writes | `oltp_delete_insert` | 65.45ms | 81.20ms | 1.2× | 1.3% | PASS |
| file_writes | `oltp_write_only` | 41.83ms | 54.84ms | 1.3× | 1.4% | PASS |
| file_writes | `types_delete_insert` | 38.57ms | 43.28ms | 1.1× | 1.3% | PASS |
| file_writes | `oltp_read_write` | 84.82ms | 114.81ms | 1.4× | 1.1% | PASS |
| ac_reads | `oltp_point_select` | 51.30ms | 48.90ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_range_select` | 13.47ms | 13.22ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_sum_range` | 12.36ms | 13.05ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_order_range` | 3.05ms | 3.13ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_distinct_range` | 4.07ms | 4.24ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 6.96ms | 7.24ms | 1.0× | 1.5% | PASS |
| ac_reads | `select_random_points` | 13.83ms | 13.59ms | 1.0× | 2.0% | PASS |
| ac_reads | `select_random_ranges` | 5.87ms | 6.11ms | 1.0× | 1.9% | PASS |
| ac_reads | `covering_index_scan` | 7.32ms | 6.51ms | 0.9× | 1.5% | PASS |
| ac_reads | `groupby_scan` | 32.55ms | 35.22ms | 1.1× | 1.1% | PASS |
| ac_reads | `index_join` | 7.55ms | 9.29ms | 1.2× | 1.6% | PASS |
| ac_reads | `index_join_scan` | 3.75ms | 4.93ms | 1.3× | 1.7% | PASS |
| ac_reads | `types_table_scan` | 1.11s | 1.20s | 1.1× | 0.5% | PASS |
| ac_reads | `table_scan` | 1.38s | 1.34s | 1.0× | 1.8% | PASS |
| ac_reads | `oltp_read_only` | 142.29ms | 144.78ms | 1.0× | 1.3% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 14.85ms | 54.95ms | 3.7× | 2.3% | PASS |
| ac_writes | `oltp_insert_ac` | 16.85ms | 68.08ms | 4.0× | 3.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 18.65ms | 81.38ms | 4.4× | 3.9% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 15.19ms | 63.33ms | 4.2× | 4.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 16.89ms | 72.44ms | 4.3× | 3.0% | PASS |
| ac_writes | `oltp_write_only_ac` | 17.55ms | 72.13ms | 4.1× | 5.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 15.75ms | 63.57ms | 4.0× | 5.2% | PASS |
| ac_writes | `oltp_read_write_ac` | 22.11ms | 77.88ms | 3.5× | 3.1% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 34.19ms | 37.67ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_range_select` | 16.07ms | 14.66ms | 0.9× | 2.1% | PASS |
| mem_reads | `oltp_sum_range` | 15.18ms | 13.86ms | 0.9× | 2.4% | PASS |
| mem_reads | `oltp_order_range` | 3.29ms | 3.18ms | 1.0× | 1.9% | PASS |
| mem_reads | `oltp_distinct_range` | 4.35ms | 4.30ms | 1.0× | 1.8% | PASS |
| mem_reads | `oltp_index_scan` | 3.92ms | 6.08ms | 1.6× | 1.4% | PASS |
| mem_reads | `select_random_points` | 21.17ms | 21.03ms | 1.0× | 2.5% | PASS |
| mem_reads | `select_random_ranges` | 3.50ms | 5.29ms | 1.5× | 1.8% | PASS |
| mem_reads | `covering_index_scan` | 4.25ms | 4.60ms | 1.1× | 1.8% | PASS |
| mem_reads | `groupby_scan` | 34.98ms | 38.13ms | 1.1× | 0.8% | PASS |
| mem_reads | `index_join` | 10.89ms | 9.71ms | 0.9× | 3.5% | PASS |
| mem_reads | `index_join_scan` | 3.95ms | 5.72ms | 1.4× | 3.6% | PASS |
| mem_reads | `types_table_scan` | 1.10s | 1.27s | 1.2× | 1.5% | PASS |
| mem_reads | `table_scan` | 1.32s | 1.37s | 1.0× | 5.6% | PASS |
| mem_reads | `oltp_read_only` | 143.90ms | 138.07ms | 1.0× | 2.9% | PASS |
| mem_writes | `oltp_bulk_insert` | 244.97ms | 370.98ms | 1.5× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 18.31ms | 38.76ms | 2.1× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 64.28ms | 140.85ms | 2.2× | 1.7% | PASS |
| mem_writes | `oltp_update_non_index` | 47.69ms | 85.03ms | 1.8× | 2.1% | PASS |
| mem_writes | `oltp_delete_insert` | 54.96ms | 105.32ms | 1.9× | 1.5% | PASS |
| mem_writes | `oltp_write_only` | 28.83ms | 62.64ms | 2.2× | 1.6% | PASS |
| mem_writes | `types_delete_insert` | 40.24ms | 54.45ms | 1.4× | 2.4% | PASS |
| mem_writes | `oltp_read_write` | 102.14ms | 147.97ms | 1.4× | 1.6% | PASS |
| file_reads | `oltp_point_select` | 112.00ms | 58.62ms | 0.5× | 1.1% | PASS |
| file_reads | `oltp_range_select` | 27.04ms | 17.46ms | 0.6× | 2.2% | PASS |
| file_reads | `oltp_sum_range` | 24.87ms | 16.53ms | 0.7× | 2.7% | PASS |
| file_reads | `oltp_order_range` | 4.62ms | 3.73ms | 0.8× | 3.8% | PASS |
| file_reads | `oltp_distinct_range` | 5.55ms | 4.90ms | 0.9× | 2.5% | PASS |
| file_reads | `oltp_index_scan` | 11.32ms | 8.62ms | 0.8× | 1.9% | PASS |
| file_reads | `select_random_points` | 31.14ms | 24.36ms | 0.8× | 2.7% | PASS |
| file_reads | `select_random_ranges` | 10.72ms | 7.44ms | 0.7× | 1.5% | PASS |
| file_reads | `covering_index_scan` | 11.75ms | 6.90ms | 0.6× | 1.4% | PASS |
| file_reads | `groupby_scan` | 36.45ms | 38.59ms | 1.1× | 1.2% | PASS |
| file_reads | `index_join` | 15.66ms | 11.29ms | 0.7× | 2.6% | PASS |
| file_reads | `index_join_scan` | 4.97ms | 5.97ms | 1.2× | 2.6% | PASS |
| file_reads | `types_table_scan` | 1.12s | 1.27s | 1.1× | 1.1% | PASS |
| file_reads | `table_scan` | 1.35s | 1.36s | 1.0× | 2.0% | PASS |
| file_reads | `oltp_read_only` | 253.66ms | 168.85ms | 0.7× | 1.8% | PASS |
| file_writes | `oltp_bulk_insert` | 270.42ms | 386.13ms | 1.4× | 1.0% | PASS |
| file_writes | `oltp_insert` | 26.32ms | 46.59ms | 1.8× | 1.9% | PASS |
| file_writes | `oltp_update_index` | 134.37ms | 165.13ms | 1.2× | 6.2% | PASS |
| file_writes | `oltp_update_non_index` | 123.17ms | 106.56ms | 0.9× | 9.6% | PASS |
| file_writes | `oltp_delete_insert` | 96.54ms | 120.87ms | 1.3× | 1.8% | PASS |
| file_writes | `oltp_write_only` | 87.16ms | 76.19ms | 0.9× | 8.9% | PASS |
| file_writes | `types_delete_insert` | 72.67ms | 62.96ms | 0.9× | 1.3% | PASS |
| file_writes | `oltp_read_write` | 147.47ms | 158.64ms | 1.1× | 8.0% | PASS |
| ac_reads | `oltp_point_select` | 61.15ms | 58.27ms | 1.0× | 2.2% | PASS |
| ac_reads | `oltp_range_select` | 21.14ms | 17.16ms | 0.8× | 2.9% | PASS |
| ac_reads | `oltp_sum_range` | 19.64ms | 16.31ms | 0.8× | 2.0% | PASS |
| ac_reads | `oltp_order_range` | 4.21ms | 3.85ms | 0.9× | 3.5% | PASS |
| ac_reads | `oltp_distinct_range` | 5.28ms | 5.05ms | 1.0× | 2.3% | PASS |
| ac_reads | `oltp_index_scan` | 6.88ms | 8.73ms | 1.3× | 1.9% | PASS |
| ac_reads | `select_random_points` | 27.75ms | 24.62ms | 0.9× | 3.5% | PASS |
| ac_reads | `select_random_ranges` | 6.25ms | 7.45ms | 1.2× | 1.7% | PASS |
| ac_reads | `covering_index_scan` | 7.49ms | 7.00ms | 0.9× | 1.8% | PASS |
| ac_reads | `groupby_scan` | 35.98ms | 38.85ms | 1.1× | 1.0% | PASS |
| ac_reads | `index_join` | 12.64ms | 11.55ms | 0.9× | 3.9% | PASS |
| ac_reads | `index_join_scan` | 4.41ms | 6.08ms | 1.4× | 2.6% | PASS |
| ac_reads | `types_table_scan` | 1.08s | 1.27s | 1.2× | 0.6% | PASS |
| ac_reads | `table_scan` | 1.24s | 1.34s | 1.1× | 1.5% | PASS |
| ac_reads | `oltp_read_only` | 185.15ms | 170.46ms | 0.9× | 2.4% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 23.77ms | 78.42ms | 3.3× | 4.6% | PASS |
| ac_writes | `oltp_insert_ac` | 27.37ms | 94.23ms | 3.4× | 6.8% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.87ms | 108.50ms | 3.9× | 5.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.05ms | 88.41ms | 3.8× | 5.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.97ms | 97.94ms | 3.6× | 5.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.81ms | 99.39ms | 3.9× | 5.8% | PASS |
| ac_writes | `types_delete_insert_ac` | 24.29ms | 91.25ms | 3.8× | 7.2% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.66ms | 103.14ms | 3.2× | 3.6% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 37.76ms | 35.86ms | 0.9× | 2.8% | PASS |
| mem_reads | `oltp_range_select` | 18.70ms | 13.74ms | 0.7× | 2.3% | PASS |
| mem_reads | `oltp_sum_range` | 16.90ms | 13.43ms | 0.8× | 2.1% | PASS |
| mem_reads | `oltp_order_range` | 3.40ms | 3.20ms | 0.9× | 1.3% | PASS |
| mem_reads | `oltp_distinct_range` | 4.48ms | 4.29ms | 1.0× | 0.7% | PASS |
| mem_reads | `oltp_index_scan` | 4.03ms | 5.68ms | 1.4× | 1.4% | PASS |
| mem_reads | `select_random_points` | 21.51ms | 20.08ms | 0.9× | 2.6% | PASS |
| mem_reads | `select_random_ranges` | 3.53ms | 5.25ms | 1.5× | 0.9% | PASS |
| mem_reads | `covering_index_scan` | 4.29ms | 4.68ms | 1.1× | 2.0% | PASS |
| mem_reads | `groupby_scan` | 37.33ms | 36.91ms | 1.0× | 0.9% | PASS |
| mem_reads | `index_join` | 11.76ms | 9.88ms | 0.8× | 1.9% | PASS |
| mem_reads | `index_join_scan` | 3.91ms | 5.89ms | 1.5× | 2.9% | PASS |
| mem_reads | `types_table_scan` | 1.14s | 1.26s | 1.1× | 1.1% | PASS |
| mem_reads | `table_scan` | 1.30s | 1.39s | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_read_only` | 142.92ms | 132.11ms | 0.9× | 2.2% | PASS |
| mem_writes | `oltp_bulk_insert` | 247.38ms | 347.95ms | 1.4× | 1.3% | PASS |
| mem_writes | `oltp_insert` | 19.10ms | 38.72ms | 2.0× | 1.2% | PASS |
| mem_writes | `oltp_update_index` | 69.06ms | 145.28ms | 2.1× | 1.9% | PASS |
| mem_writes | `oltp_update_non_index` | 51.19ms | 84.60ms | 1.7× | 1.5% | PASS |
| mem_writes | `oltp_delete_insert` | 55.16ms | 105.95ms | 1.9× | 1.3% | PASS |
| mem_writes | `oltp_write_only` | 29.26ms | 64.42ms | 2.2× | 1.5% | PASS |
| mem_writes | `types_delete_insert` | 40.21ms | 53.09ms | 1.3× | 1.5% | PASS |
| mem_writes | `oltp_read_write` | 103.37ms | 144.03ms | 1.4× | 2.5% | PASS |
| file_reads | `oltp_point_select` | 123.71ms | 58.28ms | 0.5× | 1.1% | PASS |
| file_reads | `oltp_range_select` | 26.88ms | 16.07ms | 0.6× | 2.3% | PASS |
| file_reads | `oltp_sum_range` | 25.16ms | 15.72ms | 0.6× | 1.4% | PASS |
| file_reads | `oltp_order_range` | 4.39ms | 3.51ms | 0.8× | 1.2% | PASS |
| file_reads | `oltp_distinct_range` | 5.44ms | 4.59ms | 0.8× | 1.8% | PASS |
| file_reads | `oltp_index_scan` | 12.75ms | 8.36ms | 0.7× | 1.6% | PASS |
| file_reads | `select_random_points` | 30.71ms | 22.56ms | 0.7× | 1.7% | PASS |
| file_reads | `select_random_ranges` | 11.99ms | 7.58ms | 0.6× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 13.08ms | 7.03ms | 0.5× | 1.8% | PASS |
| file_reads | `groupby_scan` | 37.84ms | 37.34ms | 1.0× | 1.2% | PASS |
| file_reads | `index_join` | 15.76ms | 10.96ms | 0.7× | 3.1% | PASS |
| file_reads | `index_join_scan` | 4.85ms | 6.10ms | 1.3× | 2.2% | PASS |
| file_reads | `types_table_scan` | 1.17s | 1.25s | 1.1× | 2.0% | PASS |
| file_reads | `table_scan` | 1.29s | 1.38s | 1.1× | 0.9% | PASS |
| file_reads | `oltp_read_only` | 264.15ms | 164.07ms | 0.6× | 1.3% | PASS |
| file_writes | `oltp_bulk_insert` | 270.30ms | 364.48ms | 1.3× | 1.3% | PASS |
| file_writes | `oltp_insert` | 26.46ms | 46.63ms | 1.8× | 2.6% | PASS |
| file_writes | `oltp_update_index` | 101.56ms | 160.84ms | 1.6× | 1.7% | PASS |
| file_writes | `oltp_update_non_index` | 86.20ms | 99.01ms | 1.1× | 6.1% | PASS |
| file_writes | `oltp_delete_insert` | 89.97ms | 119.64ms | 1.3× | 1.5% | PASS |
| file_writes | `oltp_write_only` | 56.51ms | 73.69ms | 1.3× | 2.1% | PASS |
| file_writes | `types_delete_insert` | 65.00ms | 61.60ms | 0.9× | 2.2% | PASS |
| file_writes | `oltp_read_write` | 128.08ms | 152.80ms | 1.2× | 2.4% | PASS |
| ac_reads | `oltp_point_select` | 68.34ms | 59.97ms | 0.9× | 2.4% | PASS |
| ac_reads | `oltp_range_select` | 22.09ms | 16.35ms | 0.7× | 3.5% | PASS |
| ac_reads | `oltp_sum_range` | 19.54ms | 15.80ms | 0.8× | 2.4% | PASS |
| ac_reads | `oltp_order_range` | 3.93ms | 3.53ms | 0.9× | 1.1% | PASS |
| ac_reads | `oltp_distinct_range` | 4.97ms | 4.62ms | 0.9× | 0.9% | PASS |
| ac_reads | `oltp_index_scan` | 7.26ms | 8.48ms | 1.2× | 1.1% | PASS |
| ac_reads | `select_random_points` | 25.58ms | 22.93ms | 0.9× | 2.0% | PASS |
| ac_reads | `select_random_ranges` | 6.38ms | 7.61ms | 1.2× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 7.34ms | 7.00ms | 1.0× | 1.1% | PASS |
| ac_reads | `groupby_scan` | 37.44ms | 37.36ms | 1.0× | 1.1% | PASS |
| ac_reads | `index_join` | 12.89ms | 10.92ms | 0.8× | 1.5% | PASS |
| ac_reads | `index_join_scan` | 4.62ms | 6.28ms | 1.4× | 1.8% | PASS |
| ac_reads | `types_table_scan` | 1.14s | 1.24s | 1.1× | 0.5% | PASS |
| ac_reads | `table_scan` | 1.28s | 1.37s | 1.1× | 0.6% | PASS |
| ac_reads | `oltp_read_only` | 181.76ms | 164.79ms | 0.9× | 1.3% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 18.34ms | 63.17ms | 3.4× | 6.4% | PASS |
| ac_writes | `oltp_insert_ac` | 19.77ms | 76.96ms | 3.9× | 3.8% | PASS |
| ac_writes | `oltp_update_index_ac` | 21.21ms | 85.47ms | 4.0× | 6.8% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 17.16ms | 69.29ms | 4.0× | 5.5% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 19.84ms | 77.92ms | 3.9× | 4.5% | PASS |
| ac_writes | `oltp_write_only_ac` | 20.28ms | 79.55ms | 3.9× | 5.8% | PASS |
| ac_writes | `types_delete_insert_ac` | 17.71ms | 68.73ms | 3.9× | 4.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 26.81ms | 85.02ms | 3.2× | 4.9% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.99ms | 41.03ms | 1.2× | 2.1% | PASS |
| mem_reads | `oltp_range_select` | 20.31ms | 21.62ms | 1.1× | 1.8% | PASS |
| mem_reads | `oltp_sum_range` | 18.11ms | 20.33ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_order_range` | 3.56ms | 3.84ms | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_distinct_range` | 4.74ms | 5.04ms | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_index_scan` | 4.53ms | 6.19ms | 1.4× | 2.1% | PASS |
| mem_reads | `select_random_points` | 28.38ms | 32.49ms | 1.1× | 1.7% | PASS |
| mem_reads | `select_random_ranges` | 7.81ms | 8.87ms | 1.1× | 1.1% | PASS |
| mem_reads | `covering_index_scan` | 4.20ms | 4.53ms | 1.1× | 2.0% | PASS |
| mem_reads | `groupby_scan` | 36.75ms | 39.66ms | 1.1× | 0.6% | PASS |
| mem_reads | `index_join` | 7.95ms | 10.71ms | 1.3× | 2.1% | PASS |
| mem_reads | `index_join_scan` | 3.97ms | 5.38ms | 1.4× | 2.8% | PASS |
| mem_reads | `types_table_scan` | 1.12s | 1.29s | 1.1× | 1.3% | PASS |
| mem_reads | `table_scan` | 1.36s | 1.37s | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_read_only` | 157.25ms | 173.69ms | 1.1× | 1.4% | PASS |
| mem_writes | `oltp_bulk_insert` | 246.91ms | 350.47ms | 1.4× | 1.2% | PASS |
| mem_writes | `oltp_insert` | 19.07ms | 35.25ms | 1.8× | 1.1% | PASS |
| mem_writes | `oltp_update_index` | 68.32ms | 132.83ms | 1.9× | 2.0% | PASS |
| mem_writes | `oltp_update_non_index` | 52.35ms | 85.80ms | 1.6× | 1.8% | PASS |
| mem_writes | `oltp_delete_insert` | 50.37ms | 97.78ms | 1.9× | 1.6% | PASS |
| mem_writes | `oltp_write_only` | 27.09ms | 59.11ms | 2.2× | 1.4% | PASS |
| mem_writes | `types_delete_insert` | 33.01ms | 53.90ms | 1.6× | 1.6% | PASS |
| mem_writes | `oltp_read_write` | 107.04ms | 166.37ms | 1.6× | 2.0% | PASS |
| file_reads | `oltp_point_select` | 104.92ms | 60.07ms | 0.6× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 28.12ms | 23.74ms | 0.8× | 1.5% | PASS |
| file_reads | `oltp_sum_range` | 26.01ms | 22.62ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_order_range` | 4.53ms | 4.23ms | 0.9× | 2.0% | PASS |
| file_reads | `oltp_distinct_range` | 5.73ms | 5.45ms | 1.0× | 1.6% | PASS |
| file_reads | `oltp_index_scan` | 11.91ms | 8.37ms | 0.7× | 2.1% | PASS |
| file_reads | `select_random_points` | 37.90ms | 36.22ms | 1.0× | 1.3% | PASS |
| file_reads | `select_random_ranges` | 15.28ms | 11.04ms | 0.7× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 11.70ms | 6.67ms | 0.6× | 1.4% | PASS |
| file_reads | `groupby_scan` | 38.00ms | 40.27ms | 1.1× | 1.0% | PASS |
| file_reads | `index_join` | 12.28ms | 12.47ms | 1.0× | 1.8% | PASS |
| file_reads | `index_join_scan` | 4.91ms | 5.87ms | 1.2× | 2.5% | PASS |
| file_reads | `types_table_scan` | 1.13s | 1.29s | 1.1× | 1.1% | PASS |
| file_reads | `table_scan` | 1.39s | 1.37s | 1.0× | 0.8% | PASS |
| file_reads | `oltp_read_only` | 265.19ms | 203.70ms | 0.8× | 1.3% | PASS |
| file_writes | `oltp_bulk_insert` | 263.10ms | 362.29ms | 1.4× | 1.2% | PASS |
| file_writes | `oltp_insert` | 29.34ms | 41.39ms | 1.4× | 2.5% | PASS |
| file_writes | `oltp_update_index` | 98.76ms | 144.78ms | 1.5× | 2.1% | PASS |
| file_writes | `oltp_update_non_index` | 81.63ms | 100.17ms | 1.2× | 2.1% | PASS |
| file_writes | `oltp_delete_insert` | 81.69ms | 110.53ms | 1.4× | 1.4% | PASS |
| file_writes | `oltp_write_only` | 57.46ms | 69.69ms | 1.2× | 2.0% | PASS |
| file_writes | `types_delete_insert` | 52.16ms | 60.81ms | 1.2× | 1.5% | PASS |
| file_writes | `oltp_read_write` | 139.91ms | 176.78ms | 1.3× | 1.6% | PASS |
| ac_reads | `oltp_point_select` | 57.65ms | 59.96ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_range_select` | 23.14ms | 23.77ms | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_sum_range` | 20.92ms | 22.61ms | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_order_range` | 4.01ms | 4.22ms | 1.1× | 1.8% | PASS |
| ac_reads | `oltp_distinct_range` | 5.18ms | 5.42ms | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_index_scan` | 7.39ms | 8.43ms | 1.1× | 1.6% | PASS |
| ac_reads | `select_random_points` | 32.71ms | 36.06ms | 1.1× | 1.3% | PASS |
| ac_reads | `select_random_ranges` | 10.51ms | 11.08ms | 1.1× | 1.1% | PASS |
| ac_reads | `covering_index_scan` | 7.12ms | 6.60ms | 0.9× | 1.4% | PASS |
| ac_reads | `groupby_scan` | 37.31ms | 40.06ms | 1.1× | 0.7% | PASS |
| ac_reads | `index_join` | 9.83ms | 12.39ms | 1.3× | 1.9% | PASS |
| ac_reads | `index_join_scan` | 4.46ms | 5.87ms | 1.3× | 2.6% | PASS |
| ac_reads | `types_table_scan` | 1.15s | 1.30s | 1.1× | 1.2% | PASS |
| ac_reads | `table_scan` | 1.40s | 1.38s | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_read_only` | 196.53ms | 204.98ms | 1.0× | 0.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 38.06ms | 122.12ms | 3.2× | 13.6% | PASS |
| ac_writes | `oltp_insert_ac` | 41.90ms | 149.70ms | 3.6× | 9.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 43.01ms | 154.67ms | 3.6× | 9.4% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 40.06ms | 142.15ms | 3.5× | 9.5% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 40.17ms | 149.72ms | 3.7× | 11.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 37.37ms | 135.21ms | 3.6× | 10.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 37.45ms | 135.87ms | 3.6× | 10.0% | PASS |
| ac_writes | `oltp_read_write_ac` | 48.60ms | 158.26ms | 3.3× | 9.0% | PASS |

</details>

</details>

## Version-control latency

Wall time: 2m 11s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 94.76ms | 130.00ms | 72.9% | 0.6% | PASS |
| `status_dirty_many_tables` | 97.78ms | 130.00ms | 75.2% | 0.6% | PASS |
| `diff_regular_working_one_table` | 89.22ms | 120.00ms | 74.3% | 0.4% | PASS |
| `diff_regular_working_many_tables` | 102.13ms | 140.00ms | 73.0% | 0.5% | PASS |
| `diff_stat_working_many_tables` | 101.96ms | 140.00ms | 72.8% | 0.3% | PASS |
| `diff_schema_working_many_tables` | 102.72ms | 140.00ms | 73.4% | 0.3% | PASS |
| `branch_list_many_branches` | 22.35ms | 35.00ms | 63.9% | 0.9% | PASS |
| `branch_create_delete` | 25.28ms | 40.00ms | 63.2% | 1.1% | PASS |
| `checkout_branch_clean` | 60.39ms | 150.00ms | 40.3% | 1.0% | PASS |
| `merge_data_no_conflicts` | 28.77ms | 50.00ms | 57.5% | 1.4% | PASS |
| `merge_schema_no_conflicts` | 21.82ms | 35.00ms | 62.3% | 1.6% | PASS |
| `merge_data_conflicts` | 31.98ms | 180.00ms | 17.8% | 0.5% | PASS |
| `merge_data_conflicts_with_resolve` | 32.02ms | 180.00ms | 17.8% | 0.6% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
