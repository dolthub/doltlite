# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-08-26 11:18 UTC
>
> Commit: [`10170ed82c1b12414db8d1b29d2fe9ea2a72fd88`](https://github.com/dolthub/doltlite/commit/10170ed82c1b12414db8d1b29d2fe9ea2a72fd88)
>
> Runner: ubuntu24 20260823.283.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/32954600052)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.17s | 10.69s | 1.1× | 1.4% | **PASS** |
| Writes | 2.14s | 3.34s | 1.6× | 1.5% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 11.04s | 10.93s | 1.0× | 1.3% | **PASS** |
| Writes | 3.56s | 4.03s | 1.1× | 2.2% | **PASS** |
| Autocommit writes | 1.04s | 3.86s | 3.7× | 6.6% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.46s | 2.65s | 1.1× | 1.4% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.60s | 2.82s | 1.1× | 1.5% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.74s | 2.87s | 1.0× | 1.1% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.37s | 2.35s | 1.0× | 2.2% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 450.10ms | 689.66ms | 1.5× | 1.5% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 596.73ms | 975.38ms | 1.6× | 1.3% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 604.18ms | 965.95ms | 1.6× | 1.2% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 489.48ms | 709.22ms | 1.4× | 2.0% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.73s | 2.71s | 1.0× | 1.3% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.82s | 2.87s | 1.0× | 1.3% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 3.06s | 2.97s | 1.0× | 1.2% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.44s | 2.37s | 1.0× | 1.3% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 602.52ms | 755.29ms | 1.3× | 1.8% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 933.30ms | 1.08s | 1.2× | 3.5% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 823.89ms | 1.04s | 1.3× | 1.9% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 1.20s | 1.14s | 1.0× | 26.1% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.54s | 2.71s | 1.1× | 1.4% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.70s | 2.89s | 1.1× | 1.5% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 3.00s | 3.02s | 1.0× | 1.2% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.29s | 2.38s | 1.0× | 1.2% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 204.68ms | 753.09ms | 3.7× | 6.6% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 219.02ms | 764.02ms | 3.5× | 6.0% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 164.47ms | 618.17ms | 3.8× | 6.1% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 452.59ms | 1.73s | 3.8× | 56.5% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 25.19ms | 29.00ms | 1.2× | 1.5% | PASS |
| mem_reads | `oltp_range_select` | 10.92ms | 11.19ms | 1.0× | 1.6% | PASS |
| mem_reads | `oltp_sum_range` | 10.06ms | 11.24ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_order_range` | 2.61ms | 2.79ms | 1.1× | 0.7% | PASS |
| mem_reads | `oltp_distinct_range` | 3.67ms | 3.88ms | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_index_scan` | 4.02ms | 5.31ms | 1.3× | 1.8% | PASS |
| mem_reads | `select_random_points` | 11.09ms | 10.96ms | 1.0× | 3.3% | PASS |
| mem_reads | `select_random_ranges` | 3.08ms | 3.88ms | 1.3× | 1.4% | PASS |
| mem_reads | `covering_index_scan` | 4.25ms | 4.17ms | 1.0× | 1.4% | PASS |
| mem_reads | `groupby_scan` | 30.95ms | 32.91ms | 1.1× | 0.6% | PASS |
| mem_reads | `index_join` | 5.97ms | 7.87ms | 1.3× | 1.3% | PASS |
| mem_reads | `index_join_scan` | 3.54ms | 4.40ms | 1.2× | 2.4% | PASS |
| mem_reads | `types_table_scan` | 1.05s | 1.17s | 1.1× | 0.8% | PASS |
| mem_reads | `table_scan` | 1.20s | 1.24s | 1.0× | 0.6% | PASS |
| mem_reads | `oltp_read_only` | 105.86ms | 116.37ms | 1.1× | 1.5% | PASS |
| mem_writes | `oltp_bulk_insert` | 181.04ms | 247.83ms | 1.4× | 1.5% | PASS |
| mem_writes | `oltp_insert` | 15.45ms | 27.14ms | 1.8× | 1.1% | PASS |
| mem_writes | `oltp_update_index` | 51.59ms | 90.79ms | 1.8× | 1.8% | PASS |
| mem_writes | `oltp_update_non_index` | 36.14ms | 55.17ms | 1.5× | 1.5% | PASS |
| mem_writes | `oltp_delete_insert` | 46.64ms | 73.53ms | 1.6× | 1.5% | PASS |
| mem_writes | `oltp_write_only` | 22.50ms | 45.73ms | 2.0× | 1.6% | PASS |
| mem_writes | `types_delete_insert` | 25.26ms | 36.66ms | 1.5× | 1.3% | PASS |
| mem_writes | `oltp_read_write` | 71.48ms | 112.81ms | 1.6× | 1.8% | PASS |
| file_reads | `oltp_point_select` | 94.55ms | 47.95ms | 0.5× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 18.34ms | 13.24ms | 0.7× | 1.5% | PASS |
| file_reads | `oltp_sum_range` | 17.56ms | 13.41ms | 0.8× | 1.1% | PASS |
| file_reads | `oltp_order_range` | 3.47ms | 3.10ms | 0.9× | 2.1% | PASS |
| file_reads | `oltp_distinct_range` | 4.57ms | 4.24ms | 0.9× | 1.5% | PASS |
| file_reads | `oltp_index_scan` | 11.39ms | 7.42ms | 0.7× | 1.2% | PASS |
| file_reads | `select_random_points` | 18.64ms | 13.10ms | 0.7× | 2.4% | PASS |
| file_reads | `select_random_ranges` | 10.05ms | 5.80ms | 0.6× | 0.9% | PASS |
| file_reads | `covering_index_scan` | 11.40ms | 6.38ms | 0.6× | 1.3% | PASS |
| file_reads | `groupby_scan` | 32.19ms | 33.47ms | 1.0× | 0.8% | PASS |
| file_reads | `index_join` | 9.96ms | 9.53ms | 1.0× | 1.3% | PASS |
| file_reads | `index_join_scan` | 4.40ms | 4.69ms | 1.1× | 2.6% | PASS |
| file_reads | `types_table_scan` | 1.05s | 1.16s | 1.1× | 0.8% | PASS |
| file_reads | `table_scan` | 1.24s | 1.25s | 1.0× | 1.6% | PASS |
| file_reads | `oltp_read_only` | 208.05ms | 144.78ms | 0.7× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 194.27ms | 256.61ms | 1.3× | 1.2% | PASS |
| file_writes | `oltp_insert` | 22.26ms | 31.13ms | 1.4× | 2.0% | PASS |
| file_writes | `oltp_update_index` | 80.11ms | 103.38ms | 1.3× | 1.9% | PASS |
| file_writes | `oltp_update_non_index` | 58.89ms | 66.29ms | 1.1× | 2.1% | PASS |
| file_writes | `oltp_delete_insert` | 68.40ms | 81.52ms | 1.2× | 1.8% | PASS |
| file_writes | `oltp_write_only` | 44.80ms | 53.50ms | 1.2× | 2.9% | PASS |
| file_writes | `types_delete_insert` | 40.48ms | 42.82ms | 1.1× | 1.7% | PASS |
| file_writes | `oltp_read_write` | 93.30ms | 120.04ms | 1.3× | 1.4% | PASS |
| ac_reads | `oltp_point_select` | 47.71ms | 47.88ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_range_select` | 13.46ms | 13.22ms | 1.0× | 1.8% | PASS |
| ac_reads | `oltp_sum_range` | 12.99ms | 13.54ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_order_range` | 3.06ms | 3.15ms | 1.0× | 2.5% | PASS |
| ac_reads | `oltp_distinct_range` | 4.04ms | 4.21ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_index_scan` | 6.59ms | 7.43ms | 1.1× | 1.8% | PASS |
| ac_reads | `select_random_points` | 13.37ms | 12.92ms | 1.0× | 1.5% | PASS |
| ac_reads | `select_random_ranges` | 5.42ms | 5.80ms | 1.1× | 1.2% | PASS |
| ac_reads | `covering_index_scan` | 6.93ms | 6.40ms | 0.9× | 1.8% | PASS |
| ac_reads | `groupby_scan` | 31.43ms | 33.46ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 7.66ms | 9.55ms | 1.2× | 2.0% | PASS |
| ac_reads | `index_join_scan` | 3.90ms | 4.77ms | 1.2× | 1.9% | PASS |
| ac_reads | `types_table_scan` | 1.04s | 1.16s | 1.1× | 0.7% | PASS |
| ac_reads | `table_scan` | 1.20s | 1.24s | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_read_only` | 140.58ms | 145.88ms | 1.0× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.74ms | 78.11ms | 3.4× | 4.7% | PASS |
| ac_writes | `oltp_insert_ac` | 24.85ms | 94.63ms | 3.8× | 5.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 28.99ms | 108.82ms | 3.8× | 9.3% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.90ms | 86.48ms | 3.8× | 6.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.15ms | 97.23ms | 3.9× | 7.2% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.79ms | 96.34ms | 3.7× | 6.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 23.77ms | 91.11ms | 3.8× | 8.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 30.50ms | 100.37ms | 3.3× | 6.7% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 36.30ms | 37.31ms | 1.0× | 2.2% | PASS |
| mem_reads | `oltp_range_select` | 17.07ms | 13.57ms | 0.8× | 2.2% | PASS |
| mem_reads | `oltp_sum_range` | 15.90ms | 13.78ms | 0.9× | 1.6% | PASS |
| mem_reads | `oltp_order_range` | 3.28ms | 3.11ms | 0.9× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 4.42ms | 4.23ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 4.07ms | 6.08ms | 1.5× | 1.2% | PASS |
| mem_reads | `select_random_points` | 22.69ms | 21.05ms | 0.9× | 2.2% | PASS |
| mem_reads | `select_random_ranges` | 3.58ms | 5.17ms | 1.4× | 1.3% | PASS |
| mem_reads | `covering_index_scan` | 4.30ms | 4.42ms | 1.0× | 1.5% | PASS |
| mem_reads | `groupby_scan` | 35.84ms | 34.52ms | 1.0× | 1.0% | PASS |
| mem_reads | `index_join` | 10.82ms | 8.84ms | 0.8× | 3.0% | PASS |
| mem_reads | `index_join_scan` | 4.01ms | 5.29ms | 1.3× | 1.9% | PASS |
| mem_reads | `types_table_scan` | 1.07s | 1.22s | 1.1× | 1.4% | PASS |
| mem_reads | `table_scan` | 1.23s | 1.31s | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_read_only` | 139.32ms | 133.85ms | 1.0× | 1.2% | PASS |
| mem_writes | `oltp_bulk_insert` | 239.87ms | 362.48ms | 1.5× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 18.18ms | 38.23ms | 2.1× | 1.1% | PASS |
| mem_writes | `oltp_update_index` | 67.28ms | 135.00ms | 2.0× | 1.0% | PASS |
| mem_writes | `oltp_update_non_index` | 49.03ms | 78.81ms | 1.6× | 1.3% | PASS |
| mem_writes | `oltp_delete_insert` | 55.49ms | 103.67ms | 1.9× | 1.2% | PASS |
| mem_writes | `oltp_write_only` | 28.29ms | 60.22ms | 2.1× | 1.7% | PASS |
| mem_writes | `types_delete_insert` | 39.04ms | 53.51ms | 1.4× | 1.7% | PASS |
| mem_writes | `oltp_read_write` | 99.56ms | 143.47ms | 1.4× | 1.3% | PASS |
| file_reads | `oltp_point_select` | 105.35ms | 56.15ms | 0.5× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 24.19ms | 15.68ms | 0.6× | 1.8% | PASS |
| file_reads | `oltp_sum_range` | 23.23ms | 15.90ms | 0.7× | 2.7% | PASS |
| file_reads | `oltp_order_range` | 4.25ms | 3.43ms | 0.8× | 1.8% | PASS |
| file_reads | `oltp_distinct_range` | 5.38ms | 4.54ms | 0.8× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 11.41ms | 8.38ms | 0.7× | 1.3% | PASS |
| file_reads | `select_random_points` | 31.37ms | 23.97ms | 0.8× | 2.1% | PASS |
| file_reads | `select_random_ranges` | 10.71ms | 7.15ms | 0.7× | 0.7% | PASS |
| file_reads | `covering_index_scan` | 11.68ms | 6.69ms | 0.6× | 1.5% | PASS |
| file_reads | `groupby_scan` | 37.01ms | 34.96ms | 0.9× | 1.0% | PASS |
| file_reads | `index_join` | 15.24ms | 10.75ms | 0.7× | 2.2% | PASS |
| file_reads | `index_join_scan` | 5.07ms | 5.81ms | 1.1× | 1.9% | PASS |
| file_reads | `types_table_scan` | 1.07s | 1.21s | 1.1× | 0.6% | PASS |
| file_reads | `table_scan` | 1.22s | 1.31s | 1.1× | 1.3% | PASS |
| file_reads | `oltp_read_only` | 245.22ms | 163.79ms | 0.7× | 1.1% | PASS |
| file_writes | `oltp_bulk_insert` | 265.03ms | 377.76ms | 1.4× | 1.2% | PASS |
| file_writes | `oltp_insert` | 25.97ms | 45.26ms | 1.7× | 1.5% | PASS |
| file_writes | `oltp_update_index` | 137.95ms | 153.94ms | 1.1× | 5.6% | PASS |
| file_writes | `oltp_update_non_index` | 105.81ms | 93.44ms | 0.9× | 12.5% | PASS |
| file_writes | `oltp_delete_insert` | 96.36ms | 118.88ms | 1.2× | 1.1% | PASS |
| file_writes | `oltp_write_only` | 85.22ms | 72.55ms | 0.9× | 14.6% | PASS |
| file_writes | `types_delete_insert` | 71.69ms | 66.77ms | 0.9× | 1.6% | PASS |
| file_writes | `oltp_read_write` | 145.26ms | 156.03ms | 1.1× | 5.4% | PASS |
| ac_reads | `oltp_point_select` | 59.36ms | 56.33ms | 0.9× | 1.3% | PASS |
| ac_reads | `oltp_range_select` | 20.18ms | 15.93ms | 0.8× | 2.4% | PASS |
| ac_reads | `oltp_sum_range` | 19.25ms | 16.15ms | 0.8× | 2.9% | PASS |
| ac_reads | `oltp_order_range` | 3.80ms | 3.44ms | 0.9× | 2.1% | PASS |
| ac_reads | `oltp_distinct_range` | 4.97ms | 4.61ms | 0.9× | 1.5% | PASS |
| ac_reads | `oltp_index_scan` | 6.69ms | 8.39ms | 1.3× | 1.3% | PASS |
| ac_reads | `select_random_points` | 26.16ms | 24.10ms | 0.9× | 3.0% | PASS |
| ac_reads | `select_random_ranges` | 6.11ms | 7.17ms | 1.2× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 6.90ms | 6.70ms | 1.0× | 1.6% | PASS |
| ac_reads | `groupby_scan` | 36.25ms | 34.98ms | 1.0× | 1.0% | PASS |
| ac_reads | `index_join` | 12.85ms | 10.79ms | 0.8× | 2.0% | PASS |
| ac_reads | `index_join_scan` | 4.50ms | 5.78ms | 1.3× | 1.6% | PASS |
| ac_reads | `types_table_scan` | 1.08s | 1.22s | 1.1× | 1.3% | PASS |
| ac_reads | `table_scan` | 1.22s | 1.31s | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_read_only` | 182.35ms | 165.17ms | 0.9× | 1.4% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 24.78ms | 80.41ms | 3.2× | 5.4% | PASS |
| ac_writes | `oltp_insert_ac` | 27.48ms | 94.74ms | 3.4× | 7.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 28.52ms | 105.98ms | 3.7× | 6.0% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.28ms | 89.28ms | 3.7× | 7.9% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 27.38ms | 98.59ms | 3.6× | 6.7% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.77ms | 98.00ms | 3.7× | 5.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 26.10ms | 92.19ms | 3.5× | 4.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 33.70ms | 104.83ms | 3.1× | 6.0% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 36.44ms | 34.56ms | 0.9× | 2.1% | PASS |
| mem_reads | `oltp_range_select` | 17.07ms | 13.27ms | 0.8× | 1.4% | PASS |
| mem_reads | `oltp_sum_range` | 15.57ms | 13.15ms | 0.8× | 1.0% | PASS |
| mem_reads | `oltp_order_range` | 3.44ms | 3.15ms | 0.9× | 1.0% | PASS |
| mem_reads | `oltp_distinct_range` | 4.47ms | 4.27ms | 1.0× | 0.8% | PASS |
| mem_reads | `oltp_index_scan` | 4.08ms | 5.51ms | 1.3× | 1.1% | PASS |
| mem_reads | `select_random_points` | 21.45ms | 19.55ms | 0.9× | 1.2% | PASS |
| mem_reads | `select_random_ranges` | 3.58ms | 5.18ms | 1.4× | 1.1% | PASS |
| mem_reads | `covering_index_scan` | 4.40ms | 4.62ms | 1.0× | 1.7% | PASS |
| mem_reads | `groupby_scan` | 37.41ms | 35.62ms | 1.0× | 0.7% | PASS |
| mem_reads | `index_join` | 10.93ms | 8.99ms | 0.8× | 2.1% | PASS |
| mem_reads | `index_join_scan` | 4.00ms | 5.48ms | 1.4× | 1.1% | PASS |
| mem_reads | `types_table_scan` | 1.16s | 1.24s | 1.1× | 2.2% | PASS |
| mem_reads | `table_scan` | 1.28s | 1.34s | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_read_only` | 136.80ms | 128.88ms | 0.9× | 0.5% | PASS |
| mem_writes | `oltp_bulk_insert` | 240.14ms | 347.18ms | 1.4× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 19.05ms | 38.59ms | 2.0× | 0.6% | PASS |
| mem_writes | `oltp_update_index` | 67.28ms | 135.18ms | 2.0× | 1.0% | PASS |
| mem_writes | `oltp_update_non_index` | 50.16ms | 78.66ms | 1.6× | 1.4% | PASS |
| mem_writes | `oltp_delete_insert` | 54.18ms | 105.30ms | 1.9× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 28.86ms | 63.78ms | 2.2× | 1.7% | PASS |
| mem_writes | `types_delete_insert` | 40.10ms | 53.39ms | 1.3× | 1.7% | PASS |
| mem_writes | `oltp_read_write` | 104.41ms | 143.86ms | 1.4× | 1.8% | PASS |
| file_reads | `oltp_point_select` | 122.98ms | 56.55ms | 0.5× | 0.7% | PASS |
| file_reads | `oltp_range_select` | 26.91ms | 15.67ms | 0.6× | 2.3% | PASS |
| file_reads | `oltp_sum_range` | 25.53ms | 15.86ms | 0.6× | 1.1% | PASS |
| file_reads | `oltp_order_range` | 4.41ms | 3.45ms | 0.8× | 1.2% | PASS |
| file_reads | `oltp_distinct_range` | 5.51ms | 4.57ms | 0.8× | 0.9% | PASS |
| file_reads | `oltp_index_scan` | 12.79ms | 8.09ms | 0.6× | 1.0% | PASS |
| file_reads | `select_random_points` | 31.22ms | 22.12ms | 0.7× | 1.7% | PASS |
| file_reads | `select_random_ranges` | 11.99ms | 7.34ms | 0.6× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 13.07ms | 6.87ms | 0.5× | 1.3% | PASS |
| file_reads | `groupby_scan` | 38.09ms | 35.89ms | 0.9× | 0.8% | PASS |
| file_reads | `index_join` | 15.71ms | 10.54ms | 0.7× | 3.0% | PASS |
| file_reads | `index_join_scan` | 5.03ms | 5.82ms | 1.2× | 1.2% | PASS |
| file_reads | `types_table_scan` | 1.19s | 1.26s | 1.1× | 2.0% | PASS |
| file_reads | `table_scan` | 1.29s | 1.36s | 1.0× | 1.7% | PASS |
| file_reads | `oltp_read_only` | 261.88ms | 160.87ms | 0.6× | 1.2% | PASS |
| file_writes | `oltp_bulk_insert` | 263.55ms | 356.13ms | 1.4× | 1.0% | PASS |
| file_writes | `oltp_insert` | 25.83ms | 44.87ms | 1.7× | 1.2% | PASS |
| file_writes | `oltp_update_index` | 98.36ms | 148.81ms | 1.5× | 2.6% | PASS |
| file_writes | `oltp_update_non_index` | 92.30ms | 90.85ms | 1.0× | 7.0% | PASS |
| file_writes | `oltp_delete_insert` | 88.69ms | 118.77ms | 1.3× | 2.0% | PASS |
| file_writes | `oltp_write_only` | 57.66ms | 72.59ms | 1.3× | 1.9% | PASS |
| file_writes | `types_delete_insert` | 65.12ms | 61.56ms | 0.9× | 1.2% | PASS |
| file_writes | `oltp_read_write` | 132.38ms | 151.36ms | 1.1× | 2.2% | PASS |
| ac_reads | `oltp_point_select` | 64.61ms | 56.13ms | 0.9× | 1.3% | PASS |
| ac_reads | `oltp_range_select` | 21.30ms | 15.77ms | 0.7× | 1.4% | PASS |
| ac_reads | `oltp_sum_range` | 19.51ms | 15.72ms | 0.8× | 1.2% | PASS |
| ac_reads | `oltp_order_range` | 3.93ms | 3.46ms | 0.9× | 0.9% | PASS |
| ac_reads | `oltp_distinct_range` | 4.96ms | 4.56ms | 0.9× | 0.6% | PASS |
| ac_reads | `oltp_index_scan` | 7.29ms | 8.09ms | 1.1× | 0.6% | PASS |
| ac_reads | `select_random_points` | 25.47ms | 22.11ms | 0.9× | 1.4% | PASS |
| ac_reads | `select_random_ranges` | 6.50ms | 7.39ms | 1.1× | 1.5% | PASS |
| ac_reads | `covering_index_scan` | 7.39ms | 6.80ms | 0.9× | 1.2% | PASS |
| ac_reads | `groupby_scan` | 37.38ms | 35.87ms | 1.0× | 0.7% | PASS |
| ac_reads | `index_join` | 13.28ms | 10.60ms | 0.8× | 1.3% | PASS |
| ac_reads | `index_join_scan` | 4.56ms | 5.82ms | 1.3× | 1.2% | PASS |
| ac_reads | `types_table_scan` | 1.21s | 1.27s | 1.1× | 2.3% | PASS |
| ac_reads | `table_scan` | 1.39s | 1.39s | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_read_only` | 185.45ms | 162.46ms | 0.9× | 0.7% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 18.73ms | 60.95ms | 3.3× | 6.2% | PASS |
| ac_writes | `oltp_insert_ac` | 21.50ms | 78.21ms | 3.6× | 6.3% | PASS |
| ac_writes | `oltp_update_index_ac` | 22.43ms | 91.01ms | 4.1× | 6.0% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 17.17ms | 68.35ms | 4.0× | 5.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 19.67ms | 79.23ms | 4.0× | 4.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 19.86ms | 80.22ms | 4.0× | 6.5% | PASS |
| ac_writes | `types_delete_insert_ac` | 18.22ms | 74.43ms | 4.1× | 6.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 26.89ms | 85.78ms | 3.2× | 3.5% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 28.25ms | 29.76ms | 1.1× | 2.2% | PASS |
| mem_reads | `oltp_range_select` | 16.71ms | 16.56ms | 1.0× | 2.1% | PASS |
| mem_reads | `oltp_sum_range` | 15.33ms | 16.02ms | 1.0× | 2.4% | PASS |
| mem_reads | `oltp_order_range` | 3.07ms | 3.12ms | 1.0× | 1.0% | PASS |
| mem_reads | `oltp_distinct_range` | 3.87ms | 4.01ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 3.88ms | 4.72ms | 1.2× | 2.4% | PASS |
| mem_reads | `select_random_points` | 22.59ms | 24.04ms | 1.1× | 2.3% | PASS |
| mem_reads | `select_random_ranges` | 6.19ms | 6.56ms | 1.1× | 1.6% | PASS |
| mem_reads | `covering_index_scan` | 3.39ms | 3.26ms | 1.0× | 2.4% | PASS |
| mem_reads | `groupby_scan` | 30.99ms | 32.20ms | 1.0× | 1.0% | PASS |
| mem_reads | `index_join` | 6.55ms | 8.14ms | 1.2× | 2.3% | PASS |
| mem_reads | `index_join_scan` | 3.58ms | 4.58ms | 1.3× | 2.8% | PASS |
| mem_reads | `types_table_scan` | 902.93ms | 974.20ms | 1.1× | 1.8% | PASS |
| mem_reads | `table_scan` | 1.20s | 1.09s | 0.9× | 2.4% | PASS |
| mem_reads | `oltp_read_only` | 125.53ms | 131.10ms | 1.0× | 1.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 191.20ms | 254.69ms | 1.3× | 1.3% | PASS |
| mem_writes | `oltp_insert` | 15.19ms | 25.91ms | 1.7× | 1.2% | PASS |
| mem_writes | `oltp_update_index` | 56.36ms | 91.45ms | 1.6× | 2.4% | PASS |
| mem_writes | `oltp_update_non_index` | 43.45ms | 57.76ms | 1.3× | 1.9% | PASS |
| mem_writes | `oltp_delete_insert` | 41.63ms | 71.92ms | 1.7× | 2.0% | PASS |
| mem_writes | `oltp_write_only` | 23.18ms | 43.22ms | 1.9× | 1.9% | PASS |
| mem_writes | `types_delete_insert` | 27.78ms | 39.29ms | 1.4× | 2.3% | PASS |
| mem_writes | `oltp_read_write` | 90.70ms | 124.98ms | 1.4× | 2.2% | PASS |
| file_reads | `oltp_point_select` | 93.58ms | 46.05ms | 0.5× | 1.5% | PASS |
| file_reads | `oltp_range_select` | 23.73ms | 18.38ms | 0.8× | 1.3% | PASS |
| file_reads | `oltp_sum_range` | 22.09ms | 17.84ms | 0.8× | 1.5% | PASS |
| file_reads | `oltp_order_range` | 3.79ms | 3.32ms | 0.9× | 1.1% | PASS |
| file_reads | `oltp_distinct_range` | 4.60ms | 4.24ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 10.84ms | 6.69ms | 0.6× | 1.4% | PASS |
| file_reads | `select_random_points` | 30.65ms | 26.66ms | 0.9× | 1.8% | PASS |
| file_reads | `select_random_ranges` | 12.98ms | 8.31ms | 0.6× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 10.35ms | 5.36ms | 0.5× | 1.0% | PASS |
| file_reads | `groupby_scan` | 31.52ms | 32.39ms | 1.0× | 0.7% | PASS |
| file_reads | `index_join` | 10.34ms | 9.69ms | 0.9× | 1.1% | PASS |
| file_reads | `index_join_scan` | 4.22ms | 4.92ms | 1.2× | 1.8% | PASS |
| file_reads | `types_table_scan` | 880.34ms | 964.99ms | 1.1× | 1.3% | PASS |
| file_reads | `table_scan` | 1.08s | 1.07s | 1.0× | 3.8% | PASS |
| file_reads | `oltp_read_only` | 218.52ms | 155.02ms | 0.7× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 281.57ms | 317.68ms | 1.1× | 15.9% | PASS |
| file_writes | `oltp_insert` | 31.67ms | 49.05ms | 1.5× | 58.8% | PASS |
| file_writes | `oltp_update_index` | 161.46ms | 169.69ms | 1.1× | 13.3% | PASS |
| file_writes | `oltp_update_non_index` | 160.02ms | 108.88ms | 0.7× | 19.6% | PASS |
| file_writes | `oltp_delete_insert` | 159.25ms | 139.77ms | 0.9× | 32.6% | PASS |
| file_writes | `oltp_write_only` | 122.85ms | 100.19ms | 0.8× | 40.5% | PASS |
| file_writes | `types_delete_insert` | 107.46ms | 89.27ms | 0.8× | 36.2% | PASS |
| file_writes | `oltp_read_write` | 178.39ms | 168.46ms | 0.9× | 13.1% | PASS |
| ac_reads | `oltp_point_select` | 48.57ms | 45.81ms | 0.9× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 18.61ms | 18.19ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_sum_range` | 17.36ms | 17.69ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_order_range` | 3.37ms | 3.31ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_distinct_range` | 4.17ms | 4.24ms | 1.0× | 0.5% | PASS |
| ac_reads | `oltp_index_scan` | 6.52ms | 6.71ms | 1.0× | 1.3% | PASS |
| ac_reads | `select_random_points` | 26.15ms | 26.89ms | 1.0× | 2.3% | PASS |
| ac_reads | `select_random_ranges` | 8.62ms | 8.33ms | 1.0× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 6.12ms | 5.39ms | 0.9× | 1.2% | PASS |
| ac_reads | `groupby_scan` | 31.16ms | 32.57ms | 1.0× | 1.2% | PASS |
| ac_reads | `index_join` | 8.21ms | 9.86ms | 1.2× | 1.3% | PASS |
| ac_reads | `index_join_scan` | 3.88ms | 4.92ms | 1.3× | 2.4% | PASS |
| ac_reads | `types_table_scan` | 927.28ms | 986.78ms | 1.1× | 3.5% | PASS |
| ac_reads | `table_scan` | 1.03s | 1.05s | 1.0× | 2.7% | PASS |
| ac_reads | `oltp_read_only` | 148.61ms | 151.52ms | 1.0× | 0.7% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 38.71ms | 124.18ms | 3.2× | 62.5% | PASS |
| ac_writes | `oltp_insert_ac` | 46.24ms | 170.42ms | 3.7× | 48.2% | PASS |
| ac_writes | `oltp_update_index_ac` | 56.91ms | 185.81ms | 3.3× | 55.5% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 51.21ms | 218.12ms | 4.3× | 48.2% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 71.53ms | 269.41ms | 3.8× | 51.0% | PASS |
| ac_writes | `oltp_write_only_ac` | 62.14ms | 261.90ms | 4.2× | 72.4% | PASS |
| ac_writes | `types_delete_insert_ac` | 51.82ms | 240.97ms | 4.7× | 61.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 74.04ms | 257.96ms | 3.5× | 57.6% | PASS |

</details>

</details>

## Version-control latency

Wall time: 2m 20s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 82.26ms | 130.00ms | 63.3% | 0.2% | PASS |
| `status_dirty_many_tables` | 85.33ms | 130.00ms | 65.6% | 0.2% | PASS |
| `diff_regular_working_one_table` | 76.98ms | 120.00ms | 64.2% | 0.2% | PASS |
| `diff_regular_working_many_tables` | 90.08ms | 140.00ms | 64.3% | 0.2% | PASS |
| `diff_stat_working_many_tables` | 89.99ms | 140.00ms | 64.3% | 0.2% | PASS |
| `diff_schema_working_many_tables` | 90.52ms | 140.00ms | 64.7% | 0.2% | PASS |
| `branch_list_many_branches` | 21.62ms | 35.00ms | 61.8% | 0.4% | PASS |
| `branch_create_delete` | 24.34ms | 40.00ms | 60.8% | 0.7% | PASS |
| `checkout_branch_clean` | 54.36ms | 150.00ms | 36.2% | 0.7% | PASS |
| `merge_data_no_conflicts` | 28.42ms | 50.00ms | 56.8% | 1.0% | PASS |
| `merge_schema_no_conflicts` | 21.49ms | 35.00ms | 61.4% | 1.1% | PASS |
| `merge_data_conflicts` | 126.92ms | 180.00ms | 70.5% | 0.3% | PASS |
| `merge_data_conflicts_with_resolve` | 127.62ms | 180.00ms | 70.9% | 0.4% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
