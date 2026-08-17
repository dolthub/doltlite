# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-08-15 11:13 UTC
>
> Commit: [`db6fe5bad83f3557e32f363d9906a467330c95de`](https://github.com/dolthub/doltlite/commit/db6fe5bad83f3557e32f363d9906a467330c95de)
>
> Runner: ubuntu24 20260810.271.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/31877504010)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.66s | 11.73s | 1.1× | 1.4% | **PASS** |
| Writes | 2.22s | 3.55s | 1.6× | 1.4% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 11.61s | 11.94s | 1.0× | 1.3% | **PASS** |
| Writes | 3.07s | 3.90s | 1.3× | 1.7% | **PASS** |
| Autocommit writes | 815.14ms | 2.91s | 3.6× | 6.0% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.46s | 2.74s | 1.1× | 1.4% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.85s | 3.00s | 1.1× | 1.7% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.71s | 2.96s | 1.1× | 1.7% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.64s | 3.03s | 1.1× | 1.3% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 444.11ms | 685.45ms | 1.5× | 1.5% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 591.17ms | 975.01ms | 1.6× | 1.5% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 589.52ms | 958.66ms | 1.6× | 1.3% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 596.26ms | 926.34ms | 1.6× | 1.1% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.69s | 2.78s | 1.0× | 1.3% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 3.11s | 3.06s | 1.0× | 1.8% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.95s | 3.02s | 1.0× | 1.3% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.86s | 3.09s | 1.1× | 1.3% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 599.09ms | 751.74ms | 1.3× | 1.5% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 896.65ms | 1.08s | 1.2× | 4.0% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 802.36ms | 1.05s | 1.3× | 1.5% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 774.77ms | 1.02s | 1.3× | 1.8% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.57s | 2.79s | 1.1× | 1.4% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.99s | 3.06s | 1.0× | 1.6% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.80s | 3.01s | 1.1× | 1.5% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 3.03s | 3.18s | 1.1× | 1.3% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 196.59ms | 710.76ms | 3.6× | 5.3% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 206.97ms | 736.29ms | 3.6× | 4.8% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 211.47ms | 750.74ms | 3.6× | 6.8% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 200.12ms | 712.44ms | 3.6× | 6.8% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.56ms | 28.53ms | 1.2× | 1.9% | PASS |
| mem_reads | `oltp_range_select` | 10.60ms | 11.01ms | 1.0× | 2.3% | PASS |
| mem_reads | `oltp_sum_range` | 9.70ms | 11.18ms | 1.2× | 1.4% | PASS |
| mem_reads | `oltp_order_range` | 2.57ms | 2.82ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 3.59ms | 3.90ms | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_index_scan` | 3.96ms | 5.09ms | 1.3× | 2.1% | PASS |
| mem_reads | `select_random_points` | 10.15ms | 10.62ms | 1.0× | 3.2% | PASS |
| mem_reads | `select_random_ranges` | 3.01ms | 3.81ms | 1.3× | 1.5% | PASS |
| mem_reads | `covering_index_scan` | 4.23ms | 4.06ms | 1.0× | 1.1% | PASS |
| mem_reads | `groupby_scan` | 30.21ms | 32.91ms | 1.1× | 0.6% | PASS |
| mem_reads | `index_join` | 6.07ms | 7.70ms | 1.3× | 1.2% | PASS |
| mem_reads | `index_join_scan` | 3.51ms | 4.26ms | 1.2× | 2.0% | PASS |
| mem_reads | `types_table_scan` | 1.05s | 1.17s | 1.1× | 0.7% | PASS |
| mem_reads | `table_scan` | 1.19s | 1.32s | 1.1× | 0.7% | PASS |
| mem_reads | `oltp_read_only` | 103.03ms | 116.90ms | 1.1× | 1.4% | PASS |
| mem_writes | `oltp_bulk_insert` | 183.01ms | 248.54ms | 1.4× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 15.60ms | 27.15ms | 1.7× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 50.67ms | 89.83ms | 1.8× | 1.6% | PASS |
| mem_writes | `oltp_update_non_index` | 34.34ms | 53.75ms | 1.6× | 2.0% | PASS |
| mem_writes | `oltp_delete_insert` | 45.45ms | 71.91ms | 1.6× | 1.6% | PASS |
| mem_writes | `oltp_write_only` | 21.96ms | 45.36ms | 2.1× | 1.4% | PASS |
| mem_writes | `types_delete_insert` | 24.68ms | 36.38ms | 1.5× | 2.0% | PASS |
| mem_writes | `oltp_read_write` | 68.40ms | 112.53ms | 1.6× | 1.3% | PASS |
| file_reads | `oltp_point_select` | 93.70ms | 47.18ms | 0.5× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 18.01ms | 12.95ms | 0.7× | 2.5% | PASS |
| file_reads | `oltp_sum_range` | 17.08ms | 13.23ms | 0.8× | 2.0% | PASS |
| file_reads | `oltp_order_range` | 3.39ms | 3.08ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_distinct_range` | 4.41ms | 4.17ms | 0.9× | 1.1% | PASS |
| file_reads | `oltp_index_scan` | 11.21ms | 7.23ms | 0.6× | 1.3% | PASS |
| file_reads | `select_random_points` | 17.91ms | 12.68ms | 0.7× | 2.2% | PASS |
| file_reads | `select_random_ranges` | 10.05ms | 5.75ms | 0.6× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 11.53ms | 6.27ms | 0.5× | 1.7% | PASS |
| file_reads | `groupby_scan` | 31.12ms | 33.30ms | 1.1× | 0.9% | PASS |
| file_reads | `index_join` | 10.04ms | 9.29ms | 0.9× | 2.0% | PASS |
| file_reads | `index_join_scan` | 4.47ms | 4.61ms | 1.0× | 1.4% | PASS |
| file_reads | `types_table_scan` | 1.05s | 1.16s | 1.1× | 0.6% | PASS |
| file_reads | `table_scan` | 1.20s | 1.32s | 1.1× | 1.1% | PASS |
| file_reads | `oltp_read_only` | 206.11ms | 145.61ms | 0.7× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 195.64ms | 257.41ms | 1.3× | 1.1% | PASS |
| file_writes | `oltp_insert` | 22.31ms | 31.11ms | 1.4× | 1.4% | PASS |
| file_writes | `oltp_update_index` | 77.57ms | 99.72ms | 1.3× | 1.4% | PASS |
| file_writes | `oltp_update_non_index` | 57.76ms | 65.51ms | 1.1× | 2.1% | PASS |
| file_writes | `oltp_delete_insert` | 67.47ms | 80.93ms | 1.2× | 1.5% | PASS |
| file_writes | `oltp_write_only` | 44.51ms | 53.24ms | 1.2× | 2.0% | PASS |
| file_writes | `types_delete_insert` | 40.40ms | 42.59ms | 1.1× | 1.8% | PASS |
| file_writes | `oltp_read_write` | 93.44ms | 121.24ms | 1.3× | 1.4% | PASS |
| ac_reads | `oltp_point_select` | 47.75ms | 47.19ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 13.45ms | 12.94ms | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_sum_range` | 12.44ms | 13.14ms | 1.1× | 1.7% | PASS |
| ac_reads | `oltp_order_range` | 2.92ms | 3.08ms | 1.1× | 1.7% | PASS |
| ac_reads | `oltp_distinct_range` | 3.94ms | 4.15ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_index_scan` | 6.59ms | 7.22ms | 1.1× | 1.5% | PASS |
| ac_reads | `select_random_points` | 13.18ms | 12.70ms | 1.0× | 2.8% | PASS |
| ac_reads | `select_random_ranges` | 5.46ms | 5.75ms | 1.1× | 1.1% | PASS |
| ac_reads | `covering_index_scan` | 6.97ms | 6.33ms | 0.9× | 1.6% | PASS |
| ac_reads | `groupby_scan` | 30.55ms | 33.27ms | 1.1× | 0.7% | PASS |
| ac_reads | `index_join` | 7.71ms | 9.33ms | 1.2× | 1.8% | PASS |
| ac_reads | `index_join_scan` | 3.96ms | 4.63ms | 1.2× | 1.4% | PASS |
| ac_reads | `types_table_scan` | 1.05s | 1.17s | 1.1× | 0.9% | PASS |
| ac_reads | `table_scan` | 1.22s | 1.32s | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_read_only` | 139.80ms | 145.71ms | 1.0× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.26ms | 75.87ms | 3.4× | 6.6% | PASS |
| ac_writes | `oltp_insert_ac` | 24.48ms | 89.62ms | 3.7× | 6.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.30ms | 98.98ms | 3.8× | 5.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.67ms | 82.56ms | 3.6× | 5.4% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.83ms | 92.45ms | 3.7× | 5.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 24.64ms | 91.12ms | 3.7× | 6.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 21.71ms | 82.74ms | 3.8× | 5.0% | PASS |
| ac_writes | `oltp_read_write_ac` | 29.71ms | 97.41ms | 3.3× | 4.7% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 30.40ms | 37.13ms | 1.2× | 1.7% | PASS |
| mem_reads | `oltp_range_select` | 14.28ms | 13.76ms | 1.0× | 3.2% | PASS |
| mem_reads | `oltp_sum_range` | 12.51ms | 13.91ms | 1.1× | 2.3% | PASS |
| mem_reads | `oltp_order_range` | 3.05ms | 3.17ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 4.11ms | 4.30ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 4.65ms | 6.23ms | 1.3× | 1.8% | PASS |
| mem_reads | `select_random_points` | 18.05ms | 21.20ms | 1.2× | 2.2% | PASS |
| mem_reads | `select_random_ranges` | 4.05ms | 5.13ms | 1.3× | 1.2% | PASS |
| mem_reads | `covering_index_scan` | 4.72ms | 4.55ms | 1.0× | 1.9% | PASS |
| mem_reads | `groupby_scan` | 32.55ms | 34.15ms | 1.0× | 0.8% | PASS |
| mem_reads | `index_join` | 7.09ms | 9.42ms | 1.3× | 2.7% | PASS |
| mem_reads | `index_join_scan` | 4.69ms | 5.36ms | 1.1× | 4.0% | PASS |
| mem_reads | `types_table_scan` | 1.15s | 1.25s | 1.1× | 1.0% | PASS |
| mem_reads | `table_scan` | 1.43s | 1.46s | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_read_only` | 125.25ms | 137.81ms | 1.1× | 1.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 235.53ms | 355.56ms | 1.5× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 22.43ms | 38.50ms | 1.7× | 1.5% | PASS |
| mem_writes | `oltp_update_index` | 74.24ms | 137.19ms | 1.8× | 2.2% | PASS |
| mem_writes | `oltp_update_non_index` | 49.71ms | 79.78ms | 1.6× | 1.3% | PASS |
| mem_writes | `oltp_delete_insert` | 53.29ms | 105.10ms | 2.0× | 1.6% | PASS |
| mem_writes | `oltp_write_only` | 30.18ms | 61.04ms | 2.0× | 1.6% | PASS |
| mem_writes | `types_delete_insert` | 33.94ms | 53.39ms | 1.6× | 1.5% | PASS |
| mem_writes | `oltp_read_write` | 91.84ms | 144.46ms | 1.6× | 1.9% | PASS |
| file_reads | `oltp_point_select` | 101.74ms | 56.75ms | 0.6× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 21.82ms | 15.77ms | 0.7× | 2.7% | PASS |
| file_reads | `oltp_sum_range` | 20.41ms | 15.95ms | 0.8× | 1.8% | PASS |
| file_reads | `oltp_order_range` | 3.95ms | 3.46ms | 0.9× | 1.9% | PASS |
| file_reads | `oltp_distinct_range` | 5.04ms | 4.58ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 12.19ms | 8.32ms | 0.7× | 1.3% | PASS |
| file_reads | `select_random_points` | 27.34ms | 23.75ms | 0.9× | 2.9% | PASS |
| file_reads | `select_random_ranges` | 11.36ms | 7.10ms | 0.6× | 1.3% | PASS |
| file_reads | `covering_index_scan` | 13.01ms | 6.60ms | 0.5× | 2.0% | PASS |
| file_reads | `groupby_scan` | 33.81ms | 34.75ms | 1.0× | 0.7% | PASS |
| file_reads | `index_join` | 12.09ms | 10.88ms | 0.9× | 2.0% | PASS |
| file_reads | `index_join_scan` | 5.77ms | 5.75ms | 1.0× | 3.4% | PASS |
| file_reads | `types_table_scan` | 1.14s | 1.23s | 1.1× | 1.3% | PASS |
| file_reads | `table_scan` | 1.46s | 1.46s | 1.0× | 2.0% | PASS |
| file_reads | `oltp_read_only` | 235.76ms | 169.13ms | 0.7× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 256.16ms | 373.00ms | 1.5× | 1.2% | PASS |
| file_writes | `oltp_insert` | 57.04ms | 45.53ms | 0.8× | 20.8% | PASS |
| file_writes | `oltp_update_index` | 116.75ms | 154.70ms | 1.3× | 1.7% | PASS |
| file_writes | `oltp_update_non_index` | 93.46ms | 95.07ms | 1.0× | 11.2% | PASS |
| file_writes | `oltp_delete_insert` | 92.57ms | 119.46ms | 1.3× | 1.8% | PASS |
| file_writes | `oltp_write_only` | 85.17ms | 72.70ms | 0.9× | 10.3% | PASS |
| file_writes | `types_delete_insert` | 56.27ms | 62.92ms | 1.1× | 1.7% | PASS |
| file_writes | `oltp_read_write` | 139.24ms | 156.65ms | 1.1× | 6.1% | PASS |
| ac_reads | `oltp_point_select` | 54.30ms | 56.56ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_range_select` | 17.28ms | 15.83ms | 0.9× | 2.9% | PASS |
| ac_reads | `oltp_sum_range` | 15.38ms | 15.81ms | 1.0× | 1.8% | PASS |
| ac_reads | `oltp_order_range` | 3.49ms | 3.48ms | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_distinct_range` | 4.51ms | 4.59ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 7.51ms | 8.35ms | 1.1× | 1.6% | PASS |
| ac_reads | `select_random_points` | 22.36ms | 24.06ms | 1.1× | 2.5% | PASS |
| ac_reads | `select_random_ranges` | 6.79ms | 7.12ms | 1.0× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 8.38ms | 6.64ms | 0.8× | 2.2% | PASS |
| ac_reads | `groupby_scan` | 33.17ms | 34.55ms | 1.0× | 0.7% | PASS |
| ac_reads | `index_join` | 9.60ms | 10.87ms | 1.1× | 2.3% | PASS |
| ac_reads | `index_join_scan` | 5.42ms | 5.85ms | 1.1× | 4.0% | PASS |
| ac_reads | `types_table_scan` | 1.15s | 1.24s | 1.1× | 1.1% | PASS |
| ac_reads | `table_scan` | 1.49s | 1.46s | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_read_only` | 160.98ms | 166.87ms | 1.0× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.85ms | 77.23ms | 3.4× | 4.3% | PASS |
| ac_writes | `oltp_insert_ac` | 25.91ms | 89.24ms | 3.4× | 5.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 28.11ms | 102.50ms | 3.6× | 4.3% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.69ms | 88.03ms | 3.7× | 5.9% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.34ms | 95.72ms | 3.8× | 4.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.98ms | 94.68ms | 3.6× | 6.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.70ms | 87.18ms | 3.8× | 6.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.39ms | 101.72ms | 3.1× | 4.0% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 31.83ms | 36.99ms | 1.2× | 2.1% | PASS |
| mem_reads | `oltp_range_select` | 13.81ms | 13.53ms | 1.0× | 2.1% | PASS |
| mem_reads | `oltp_sum_range` | 12.55ms | 13.65ms | 1.1× | 2.3% | PASS |
| mem_reads | `oltp_order_range` | 2.97ms | 3.15ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_distinct_range` | 4.03ms | 4.27ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 4.68ms | 6.20ms | 1.3× | 1.9% | PASS |
| mem_reads | `select_random_points` | 19.35ms | 21.25ms | 1.1× | 2.1% | PASS |
| mem_reads | `select_random_ranges` | 4.33ms | 5.21ms | 1.2× | 1.7% | PASS |
| mem_reads | `covering_index_scan` | 4.47ms | 4.61ms | 1.0× | 1.4% | PASS |
| mem_reads | `groupby_scan` | 32.51ms | 34.20ms | 1.1× | 0.8% | PASS |
| mem_reads | `index_join` | 6.99ms | 9.59ms | 1.4× | 1.9% | PASS |
| mem_reads | `index_join_scan` | 4.78ms | 5.47ms | 1.1× | 1.4% | PASS |
| mem_reads | `types_table_scan` | 1.10s | 1.23s | 1.1× | 1.1% | PASS |
| mem_reads | `table_scan` | 1.35s | 1.44s | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_read_only` | 124.02ms | 135.96ms | 1.1× | 1.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 242.76ms | 349.42ms | 1.4× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 20.24ms | 38.11ms | 1.9× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 71.18ms | 133.61ms | 1.9× | 1.3% | PASS |
| mem_writes | `oltp_update_non_index` | 50.48ms | 77.90ms | 1.5× | 1.6% | PASS |
| mem_writes | `oltp_delete_insert` | 50.85ms | 103.19ms | 2.0× | 1.3% | PASS |
| mem_writes | `oltp_write_only` | 28.97ms | 60.50ms | 2.1× | 1.7% | PASS |
| mem_writes | `types_delete_insert` | 33.90ms | 51.75ms | 1.5× | 1.6% | PASS |
| mem_writes | `oltp_read_write` | 91.13ms | 144.18ms | 1.6× | 1.4% | PASS |
| file_reads | `oltp_point_select` | 102.79ms | 55.73ms | 0.5× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 21.95ms | 15.55ms | 0.7× | 1.3% | PASS |
| file_reads | `oltp_sum_range` | 20.71ms | 15.67ms | 0.8× | 1.9% | PASS |
| file_reads | `oltp_order_range` | 3.88ms | 3.42ms | 0.9× | 1.6% | PASS |
| file_reads | `oltp_distinct_range` | 4.97ms | 4.55ms | 0.9× | 1.2% | PASS |
| file_reads | `oltp_index_scan` | 12.37ms | 8.25ms | 0.7× | 1.4% | PASS |
| file_reads | `select_random_points` | 28.30ms | 23.78ms | 0.8× | 2.3% | PASS |
| file_reads | `select_random_ranges` | 11.55ms | 7.07ms | 0.6× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 12.83ms | 6.58ms | 0.5× | 2.3% | PASS |
| file_reads | `groupby_scan` | 33.80ms | 34.62ms | 1.0× | 0.7% | PASS |
| file_reads | `index_join` | 12.12ms | 10.98ms | 0.9× | 3.3% | PASS |
| file_reads | `index_join_scan` | 5.81ms | 5.84ms | 1.0× | 2.9% | PASS |
| file_reads | `types_table_scan` | 1.10s | 1.22s | 1.1× | 1.0% | PASS |
| file_reads | `table_scan` | 1.34s | 1.44s | 1.1× | 0.8% | PASS |
| file_reads | `oltp_read_only` | 232.66ms | 165.71ms | 0.7× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 261.68ms | 360.60ms | 1.4× | 0.8% | PASS |
| file_writes | `oltp_insert` | 33.00ms | 44.98ms | 1.4× | 2.6% | PASS |
| file_writes | `oltp_update_index` | 108.09ms | 148.27ms | 1.4× | 1.3% | PASS |
| file_writes | `oltp_update_non_index` | 81.06ms | 90.58ms | 1.1× | 1.0% | PASS |
| file_writes | `oltp_delete_insert` | 84.13ms | 115.39ms | 1.4× | 2.0% | PASS |
| file_writes | `oltp_write_only` | 57.21ms | 71.03ms | 1.2× | 1.5% | PASS |
| file_writes | `types_delete_insert` | 53.83ms | 61.01ms | 1.1× | 1.5% | PASS |
| file_writes | `oltp_read_write` | 123.35ms | 155.03ms | 1.3× | 1.8% | PASS |
| ac_reads | `oltp_point_select` | 55.82ms | 55.77ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_range_select` | 17.31ms | 15.66ms | 0.9× | 1.4% | PASS |
| ac_reads | `oltp_sum_range` | 15.98ms | 15.75ms | 1.0× | 1.8% | PASS |
| ac_reads | `oltp_order_range` | 3.38ms | 3.41ms | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_distinct_range` | 4.42ms | 4.54ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_index_scan` | 7.56ms | 8.27ms | 1.1× | 1.7% | PASS |
| ac_reads | `select_random_points` | 23.20ms | 23.83ms | 1.0× | 2.3% | PASS |
| ac_reads | `select_random_ranges` | 6.87ms | 7.06ms | 1.0× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 8.01ms | 6.60ms | 0.8× | 2.6% | PASS |
| ac_reads | `groupby_scan` | 33.19ms | 34.61ms | 1.0× | 0.6% | PASS |
| ac_reads | `index_join` | 9.48ms | 11.07ms | 1.2× | 3.8% | PASS |
| ac_reads | `index_join_scan` | 5.30ms | 5.82ms | 1.1× | 1.5% | PASS |
| ac_reads | `types_table_scan` | 1.10s | 1.22s | 1.1× | 1.2% | PASS |
| ac_reads | `table_scan` | 1.35s | 1.44s | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_read_only` | 161.79ms | 166.09ms | 1.0× | 1.5% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 24.32ms | 77.56ms | 3.2× | 7.3% | PASS |
| ac_writes | `oltp_insert_ac` | 26.40ms | 93.62ms | 3.5× | 7.2% | PASS |
| ac_writes | `oltp_update_index_ac` | 28.00ms | 102.37ms | 3.7× | 5.4% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 25.10ms | 90.21ms | 3.6× | 8.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.81ms | 95.38ms | 3.8× | 5.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.62ms | 93.62ms | 3.5× | 5.5% | PASS |
| ac_writes | `types_delete_insert_ac` | 23.54ms | 94.28ms | 4.0× | 8.6% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.68ms | 103.70ms | 3.2× | 6.4% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 32.17ms | 38.23ms | 1.2× | 1.2% | PASS |
| mem_reads | `oltp_range_select` | 18.62ms | 21.36ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_sum_range` | 17.56ms | 20.88ms | 1.2× | 0.9% | PASS |
| mem_reads | `oltp_order_range` | 3.44ms | 3.89ms | 1.1× | 1.8% | PASS |
| mem_reads | `oltp_distinct_range` | 4.63ms | 5.05ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 4.50ms | 5.79ms | 1.3× | 1.8% | PASS |
| mem_reads | `select_random_points` | 29.07ms | 32.54ms | 1.1× | 0.9% | PASS |
| mem_reads | `select_random_ranges` | 7.93ms | 8.96ms | 1.1× | 1.3% | PASS |
| mem_reads | `covering_index_scan` | 4.28ms | 4.41ms | 1.0× | 1.7% | PASS |
| mem_reads | `groupby_scan` | 37.31ms | 40.55ms | 1.1× | 0.7% | PASS |
| mem_reads | `index_join` | 8.28ms | 11.02ms | 1.3× | 1.9% | PASS |
| mem_reads | `index_join_scan` | 4.22ms | 5.42ms | 1.3× | 1.5% | PASS |
| mem_reads | `types_table_scan` | 1.07s | 1.23s | 1.2× | 0.8% | PASS |
| mem_reads | `table_scan` | 1.25s | 1.43s | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_read_only` | 152.24ms | 171.48ms | 1.1× | 1.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 248.31ms | 342.88ms | 1.4× | 1.1% | PASS |
| mem_writes | `oltp_insert` | 19.19ms | 34.46ms | 1.8× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 68.06ms | 116.23ms | 1.7× | 1.8% | PASS |
| mem_writes | `oltp_update_non_index` | 50.29ms | 75.05ms | 1.5× | 0.8% | PASS |
| mem_writes | `oltp_delete_insert` | 49.88ms | 93.10ms | 1.9× | 0.9% | PASS |
| mem_writes | `oltp_write_only` | 26.97ms | 55.32ms | 2.1× | 1.2% | PASS |
| mem_writes | `types_delete_insert` | 32.52ms | 51.42ms | 1.6× | 1.2% | PASS |
| mem_writes | `oltp_read_write` | 101.04ms | 157.87ms | 1.6× | 1.6% | PASS |
| file_reads | `oltp_point_select` | 102.71ms | 57.56ms | 0.6× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 26.23ms | 23.58ms | 0.9× | 1.5% | PASS |
| file_reads | `oltp_sum_range` | 25.13ms | 23.27ms | 0.9× | 1.2% | PASS |
| file_reads | `oltp_order_range` | 4.37ms | 4.34ms | 1.0× | 2.4% | PASS |
| file_reads | `oltp_distinct_range` | 5.56ms | 5.49ms | 1.0× | 1.6% | PASS |
| file_reads | `oltp_index_scan` | 11.68ms | 8.16ms | 0.7× | 1.3% | PASS |
| file_reads | `select_random_points` | 36.82ms | 35.05ms | 1.0× | 1.7% | PASS |
| file_reads | `select_random_ranges` | 15.05ms | 10.97ms | 0.7× | 1.9% | PASS |
| file_reads | `covering_index_scan` | 11.32ms | 6.38ms | 0.6× | 1.0% | PASS |
| file_reads | `groupby_scan` | 37.66ms | 40.82ms | 1.1× | 0.9% | PASS |
| file_reads | `index_join` | 12.46ms | 12.29ms | 1.0× | 1.9% | PASS |
| file_reads | `index_join_scan` | 5.10ms | 5.71ms | 1.1× | 1.9% | PASS |
| file_reads | `types_table_scan` | 1.07s | 1.22s | 1.1× | 0.7% | PASS |
| file_reads | `table_scan` | 1.24s | 1.43s | 1.2× | 0.8% | PASS |
| file_reads | `oltp_read_only` | 256.99ms | 199.81ms | 0.8× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 262.21ms | 354.37ms | 1.4× | 0.8% | PASS |
| file_writes | `oltp_insert` | 26.09ms | 40.25ms | 1.5× | 1.8% | PASS |
| file_writes | `oltp_update_index` | 96.49ms | 131.44ms | 1.4× | 1.8% | PASS |
| file_writes | `oltp_update_non_index` | 77.00ms | 90.18ms | 1.2× | 1.7% | PASS |
| file_writes | `oltp_delete_insert` | 80.37ms | 109.80ms | 1.4× | 1.4% | PASS |
| file_writes | `oltp_write_only` | 52.29ms | 68.20ms | 1.3× | 2.4% | PASS |
| file_writes | `types_delete_insert` | 50.58ms | 59.65ms | 1.2× | 1.9% | PASS |
| file_writes | `oltp_read_write` | 129.76ms | 170.32ms | 1.3× | 1.8% | PASS |
| ac_reads | `oltp_point_select` | 56.04ms | 57.69ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_range_select` | 22.20ms | 23.46ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_sum_range` | 20.44ms | 23.24ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_order_range` | 3.94ms | 4.29ms | 1.1× | 2.0% | PASS |
| ac_reads | `oltp_distinct_range` | 5.07ms | 5.46ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_index_scan` | 7.33ms | 8.17ms | 1.1× | 1.6% | PASS |
| ac_reads | `select_random_points` | 31.48ms | 35.19ms | 1.1× | 1.9% | PASS |
| ac_reads | `select_random_ranges` | 10.25ms | 10.96ms | 1.1× | 0.9% | PASS |
| ac_reads | `covering_index_scan` | 6.94ms | 6.40ms | 0.9× | 1.9% | PASS |
| ac_reads | `groupby_scan` | 37.31ms | 40.72ms | 1.1× | 1.1% | PASS |
| ac_reads | `index_join` | 9.91ms | 12.17ms | 1.2× | 1.6% | PASS |
| ac_reads | `index_join_scan` | 4.70ms | 5.71ms | 1.2× | 1.8% | PASS |
| ac_reads | `types_table_scan` | 1.17s | 1.26s | 1.1× | 1.5% | PASS |
| ac_reads | `table_scan` | 1.45s | 1.49s | 1.0× | 0.4% | PASS |
| ac_reads | `oltp_read_only` | 198.92ms | 207.82ms | 1.0× | 0.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.89ms | 74.33ms | 3.4× | 7.9% | PASS |
| ac_writes | `oltp_insert_ac` | 25.19ms | 89.53ms | 3.6× | 6.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.24ms | 98.80ms | 3.8× | 4.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.92ms | 82.39ms | 3.6× | 7.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.89ms | 92.41ms | 3.7× | 8.5% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.20ms | 93.14ms | 3.7× | 6.7% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.30ms | 81.75ms | 3.7× | 6.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.51ms | 100.09ms | 3.2× | 4.9% | PASS |

</details>

</details>

## Version-control latency

Wall time: 2m 19s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 80.79ms | 130.00ms | 62.1% | 0.3% | PASS |
| `status_dirty_many_tables` | 83.96ms | 130.00ms | 64.6% | 0.4% | PASS |
| `diff_regular_working_one_table` | 78.35ms | 120.00ms | 65.3% | 0.3% | PASS |
| `diff_regular_working_many_tables` | 88.84ms | 140.00ms | 63.5% | 0.4% | PASS |
| `diff_stat_working_many_tables` | 88.97ms | 140.00ms | 63.5% | 0.4% | PASS |
| `diff_schema_working_many_tables` | 89.35ms | 140.00ms | 63.8% | 0.3% | PASS |
| `branch_list_many_branches` | 22.73ms | 35.00ms | 64.9% | 1.0% | PASS |
| `branch_create_delete` | 25.88ms | 40.00ms | 64.7% | 1.2% | PASS |
| `checkout_branch_clean` | 54.95ms | 150.00ms | 36.6% | 0.9% | PASS |
| `merge_data_no_conflicts` | 29.79ms | 50.00ms | 59.6% | 1.1% | PASS |
| `merge_schema_no_conflicts` | 22.86ms | 35.00ms | 65.3% | 1.5% | PASS |
| `merge_data_conflicts` | 127.04ms | 180.00ms | 70.6% | 0.2% | PASS |
| `merge_data_conflicts_with_resolve` | 126.92ms | 180.00ms | 70.5% | 0.2% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
