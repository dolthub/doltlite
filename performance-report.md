# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-08-20 11:16 UTC
>
> Commit: [`6f74fb38789b2e5f699da7a0add14f5a2e66c8fd`](https://github.com/dolthub/doltlite/commit/6f74fb38789b2e5f699da7a0add14f5a2e66c8fd)
>
> Runner: ubuntu24 20260816.277.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/32355281912)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.95s | 11.14s | 1.0× | 1.3% | **PASS** |
| Writes | 2.15s | 3.41s | 1.6× | 1.3% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 11.56s | 11.29s | 1.0× | 1.3% | **PASS** |
| Writes | 2.89s | 3.70s | 1.3× | 1.7% | **PASS** |
| Autocommit writes | 638.41ms | 2.45s | 3.8× | 5.6% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.43s | 2.60s | 1.1× | 1.4% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.68s | 2.59s | 1.0× | 1.3% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 3.09s | 3.00s | 1.0× | 1.5% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.75s | 2.95s | 1.1× | 1.1% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 447.02ms | 688.84ms | 1.5× | 1.4% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 513.11ms | 845.41ms | 1.6× | 1.3% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 595.39ms | 961.90ms | 1.6× | 1.4% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 594.49ms | 916.36ms | 1.5× | 0.9% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.66s | 2.66s | 1.0× | 1.4% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.79s | 2.62s | 0.9× | 1.5% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 3.12s | 2.99s | 1.0× | 0.9% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 3.00s | 3.02s | 1.0× | 1.3% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 600.14ms | 748.78ms | 1.2× | 1.5% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 752.67ms | 935.12ms | 1.2× | 4.7% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 785.90ms | 1.03s | 1.3× | 1.8% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 755.00ms | 988.45ms | 1.3× | 1.5% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.57s | 2.68s | 1.0× | 1.5% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.70s | 2.60s | 1.0× | 1.4% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 3.05s | 3.03s | 1.0× | 1.8% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.89s | 3.05s | 1.1× | 1.2% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 200.82ms | 742.32ms | 3.7× | 5.9% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 141.55ms | 538.79ms | 3.8× | 6.3% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 151.46ms | 593.80ms | 3.9× | 5.5% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 144.58ms | 571.38ms | 4.0× | 3.7% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.40ms | 28.93ms | 1.2× | 1.5% | PASS |
| mem_reads | `oltp_range_select` | 10.11ms | 11.79ms | 1.2× | 1.2% | PASS |
| mem_reads | `oltp_sum_range` | 9.54ms | 11.00ms | 1.2× | 1.5% | PASS |
| mem_reads | `oltp_order_range` | 2.58ms | 2.85ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_distinct_range` | 3.65ms | 3.96ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 3.93ms | 5.08ms | 1.3× | 1.4% | PASS |
| mem_reads | `select_random_points` | 10.08ms | 10.78ms | 1.1× | 2.1% | PASS |
| mem_reads | `select_random_ranges` | 3.04ms | 3.93ms | 1.3× | 2.0% | PASS |
| mem_reads | `covering_index_scan` | 4.26ms | 4.17ms | 1.0× | 1.9% | PASS |
| mem_reads | `groupby_scan` | 30.33ms | 32.24ms | 1.1× | 0.6% | PASS |
| mem_reads | `index_join` | 6.08ms | 8.05ms | 1.3× | 1.9% | PASS |
| mem_reads | `index_join_scan` | 3.49ms | 4.42ms | 1.3× | 2.2% | PASS |
| mem_reads | `types_table_scan` | 1.04s | 1.13s | 1.1× | 0.4% | PASS |
| mem_reads | `table_scan` | 1.18s | 1.23s | 1.0× | 0.8% | PASS |
| mem_reads | `oltp_read_only` | 102.51ms | 117.48ms | 1.1× | 1.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 182.37ms | 248.53ms | 1.4× | 1.2% | PASS |
| mem_writes | `oltp_insert` | 15.53ms | 27.50ms | 1.8× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 50.34ms | 89.22ms | 1.8× | 1.3% | PASS |
| mem_writes | `oltp_update_non_index` | 34.59ms | 54.04ms | 1.6× | 1.7% | PASS |
| mem_writes | `oltp_delete_insert` | 45.48ms | 73.10ms | 1.6× | 1.6% | PASS |
| mem_writes | `oltp_write_only` | 22.40ms | 46.27ms | 2.1× | 1.3% | PASS |
| mem_writes | `types_delete_insert` | 25.11ms | 36.95ms | 1.5× | 1.8% | PASS |
| mem_writes | `oltp_read_write` | 71.19ms | 113.24ms | 1.6× | 1.5% | PASS |
| file_reads | `oltp_point_select` | 94.41ms | 47.92ms | 0.5× | 0.7% | PASS |
| file_reads | `oltp_range_select` | 17.59ms | 13.75ms | 0.8× | 2.4% | PASS |
| file_reads | `oltp_sum_range` | 16.97ms | 13.09ms | 0.8× | 2.5% | PASS |
| file_reads | `oltp_order_range` | 3.38ms | 3.13ms | 0.9× | 1.5% | PASS |
| file_reads | `oltp_distinct_range` | 4.46ms | 4.21ms | 0.9× | 2.0% | PASS |
| file_reads | `oltp_index_scan` | 11.31ms | 7.36ms | 0.7× | 1.4% | PASS |
| file_reads | `select_random_points` | 17.41ms | 12.82ms | 0.7× | 1.6% | PASS |
| file_reads | `select_random_ranges` | 9.94ms | 5.82ms | 0.6× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 11.34ms | 6.31ms | 0.6× | 0.9% | PASS |
| file_reads | `groupby_scan` | 30.87ms | 32.45ms | 1.1× | 0.6% | PASS |
| file_reads | `index_join` | 9.95ms | 9.41ms | 0.9× | 1.7% | PASS |
| file_reads | `index_join_scan` | 4.36ms | 4.69ms | 1.1× | 2.1% | PASS |
| file_reads | `types_table_scan` | 1.05s | 1.13s | 1.1× | 0.7% | PASS |
| file_reads | `table_scan` | 1.17s | 1.22s | 1.0× | 0.5% | PASS |
| file_reads | `oltp_read_only` | 204.26ms | 145.13ms | 0.7× | 1.1% | PASS |
| file_writes | `oltp_bulk_insert` | 197.01ms | 257.10ms | 1.3× | 0.9% | PASS |
| file_writes | `oltp_insert` | 21.83ms | 31.26ms | 1.4× | 1.5% | PASS |
| file_writes | `oltp_update_index` | 75.95ms | 98.35ms | 1.3× | 1.4% | PASS |
| file_writes | `oltp_update_non_index` | 56.60ms | 64.87ms | 1.1× | 1.3% | PASS |
| file_writes | `oltp_delete_insert` | 68.45ms | 81.42ms | 1.2× | 1.5% | PASS |
| file_writes | `oltp_write_only` | 45.03ms | 53.48ms | 1.2× | 1.7% | PASS |
| file_writes | `types_delete_insert` | 40.69ms | 43.06ms | 1.1× | 2.7% | PASS |
| file_writes | `oltp_read_write` | 94.57ms | 119.24ms | 1.3× | 1.9% | PASS |
| ac_reads | `oltp_point_select` | 47.49ms | 47.58ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 13.76ms | 13.79ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_sum_range` | 12.82ms | 13.17ms | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_order_range` | 3.03ms | 3.12ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_distinct_range` | 4.01ms | 4.21ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_index_scan` | 6.47ms | 7.29ms | 1.1× | 1.7% | PASS |
| ac_reads | `select_random_points` | 12.71ms | 12.74ms | 1.0× | 1.5% | PASS |
| ac_reads | `select_random_ranges` | 5.30ms | 5.81ms | 1.1× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 6.80ms | 6.32ms | 0.9× | 1.9% | PASS |
| ac_reads | `groupby_scan` | 30.72ms | 32.72ms | 1.1× | 1.1% | PASS |
| ac_reads | `index_join` | 7.78ms | 9.62ms | 1.2× | 1.5% | PASS |
| ac_reads | `index_join_scan` | 4.01ms | 4.70ms | 1.2× | 2.2% | PASS |
| ac_reads | `types_table_scan` | 1.07s | 1.14s | 1.1× | 1.3% | PASS |
| ac_reads | `table_scan` | 1.20s | 1.23s | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_read_only` | 141.87ms | 146.69ms | 1.0× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.28ms | 78.27ms | 3.5× | 6.1% | PASS |
| ac_writes | `oltp_insert_ac` | 26.29ms | 93.21ms | 3.5× | 5.1% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.84ms | 102.78ms | 3.8× | 4.9% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.06ms | 86.06ms | 3.7× | 7.1% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.30ms | 94.92ms | 3.9× | 8.4% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.18ms | 97.43ms | 3.7× | 7.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.10ms | 87.29ms | 4.0× | 5.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 29.78ms | 102.36ms | 3.4× | 4.9% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 27.10ms | 32.88ms | 1.2× | 1.3% | PASS |
| mem_reads | `oltp_range_select` | 12.06ms | 12.50ms | 1.0× | 1.5% | PASS |
| mem_reads | `oltp_sum_range` | 11.70ms | 12.19ms | 1.0× | 1.3% | PASS |
| mem_reads | `oltp_order_range` | 2.67ms | 2.84ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 3.39ms | 3.66ms | 1.1× | 0.5% | PASS |
| mem_reads | `oltp_index_scan` | 4.12ms | 5.42ms | 1.3× | 1.5% | PASS |
| mem_reads | `select_random_points` | 17.40ms | 20.99ms | 1.2× | 1.5% | PASS |
| mem_reads | `select_random_ranges` | 3.49ms | 4.55ms | 1.3× | 1.5% | PASS |
| mem_reads | `covering_index_scan` | 4.57ms | 4.11ms | 0.9× | 1.9% | PASS |
| mem_reads | `groupby_scan` | 29.57ms | 30.54ms | 1.0× | 0.5% | PASS |
| mem_reads | `index_join` | 6.60ms | 8.99ms | 1.4× | 2.5% | PASS |
| mem_reads | `index_join_scan` | 4.05ms | 5.27ms | 1.3× | 2.2% | PASS |
| mem_reads | `types_table_scan` | 1.09s | 1.11s | 1.0× | 0.5% | PASS |
| mem_reads | `table_scan` | 1.35s | 1.22s | 0.9× | 0.5% | PASS |
| mem_reads | `oltp_read_only` | 110.56ms | 123.05ms | 1.1× | 1.2% | PASS |
| mem_writes | `oltp_bulk_insert` | 198.43ms | 293.44ms | 1.5× | 1.3% | PASS |
| mem_writes | `oltp_insert` | 19.96ms | 33.04ms | 1.7× | 1.6% | PASS |
| mem_writes | `oltp_update_index` | 67.63ms | 128.71ms | 1.9× | 1.8% | PASS |
| mem_writes | `oltp_update_non_index` | 44.05ms | 70.49ms | 1.6× | 0.9% | PASS |
| mem_writes | `oltp_delete_insert` | 47.67ms | 93.61ms | 2.0× | 1.8% | PASS |
| mem_writes | `oltp_write_only` | 25.93ms | 52.16ms | 2.0× | 1.0% | PASS |
| mem_writes | `types_delete_insert` | 28.71ms | 45.81ms | 1.6× | 0.9% | PASS |
| mem_writes | `oltp_read_write` | 80.73ms | 128.16ms | 1.6× | 1.2% | PASS |
| file_reads | `oltp_point_select` | 58.66ms | 42.05ms | 0.7× | 1.5% | PASS |
| file_reads | `oltp_range_select` | 15.64ms | 13.66ms | 0.9× | 1.2% | PASS |
| file_reads | `oltp_sum_range` | 15.21ms | 13.45ms | 0.9× | 1.5% | PASS |
| file_reads | `oltp_order_range` | 3.09ms | 3.03ms | 1.0× | 2.4% | PASS |
| file_reads | `oltp_distinct_range` | 3.85ms | 3.87ms | 1.0× | 1.5% | PASS |
| file_reads | `oltp_index_scan` | 7.58ms | 6.45ms | 0.9× | 1.7% | PASS |
| file_reads | `select_random_points` | 21.07ms | 22.07ms | 1.0× | 1.9% | PASS |
| file_reads | `select_random_ranges` | 6.67ms | 5.50ms | 0.8× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 8.18ms | 5.11ms | 0.6× | 1.8% | PASS |
| file_reads | `groupby_scan` | 30.30ms | 30.84ms | 1.0× | 0.8% | PASS |
| file_reads | `index_join` | 8.86ms | 9.60ms | 1.1× | 2.0% | PASS |
| file_reads | `index_join_scan` | 4.52ms | 5.32ms | 1.2× | 2.4% | PASS |
| file_reads | `types_table_scan` | 1.10s | 1.10s | 1.0× | 0.6% | PASS |
| file_reads | `table_scan` | 1.35s | 1.22s | 0.9× | 0.5% | PASS |
| file_reads | `oltp_read_only` | 158.50ms | 138.09ms | 0.9× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 214.66ms | 307.31ms | 1.4× | 1.6% | PASS |
| file_writes | `oltp_insert` | 45.53ms | 39.64ms | 0.9× | 23.3% | PASS |
| file_writes | `oltp_update_index` | 93.29ms | 145.14ms | 1.6× | 1.6% | PASS |
| file_writes | `oltp_update_non_index` | 93.98ms | 82.58ms | 0.9× | 10.3% | PASS |
| file_writes | `oltp_delete_insert` | 68.57ms | 104.62ms | 1.5× | 2.0% | PASS |
| file_writes | `oltp_write_only` | 63.85ms | 62.58ms | 1.0× | 25.7% | PASS |
| file_writes | `types_delete_insert` | 42.13ms | 54.86ms | 1.3× | 1.4% | PASS |
| file_writes | `oltp_read_write` | 130.67ms | 138.39ms | 1.1× | 7.4% | PASS |
| ac_reads | `oltp_point_select` | 36.83ms | 41.52ms | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_range_select` | 13.16ms | 13.55ms | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_sum_range` | 12.70ms | 13.30ms | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_order_range` | 2.84ms | 3.06ms | 1.1× | 1.8% | PASS |
| ac_reads | `oltp_distinct_range` | 3.58ms | 3.88ms | 1.1× | 1.9% | PASS |
| ac_reads | `oltp_index_scan` | 5.42ms | 6.39ms | 1.2× | 1.3% | PASS |
| ac_reads | `select_random_points` | 18.10ms | 21.53ms | 1.2× | 2.0% | PASS |
| ac_reads | `select_random_ranges` | 4.56ms | 5.49ms | 1.2× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 5.87ms | 5.03ms | 0.9× | 1.2% | PASS |
| ac_reads | `groupby_scan` | 29.61ms | 30.66ms | 1.0× | 0.5% | PASS |
| ac_reads | `index_join` | 7.74ms | 9.51ms | 1.2× | 1.8% | PASS |
| ac_reads | `index_join_scan` | 4.33ms | 5.30ms | 1.2× | 1.9% | PASS |
| ac_reads | `types_table_scan` | 1.09s | 1.10s | 1.0× | 0.7% | PASS |
| ac_reads | `table_scan` | 1.35s | 1.21s | 0.9× | 0.4% | PASS |
| ac_reads | `oltp_read_only` | 127.46ms | 137.62ms | 1.1× | 0.7% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 15.97ms | 57.38ms | 3.6× | 8.8% | PASS |
| ac_writes | `oltp_insert_ac` | 18.87ms | 66.39ms | 3.5× | 6.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 19.53ms | 74.69ms | 3.8× | 6.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 15.61ms | 60.70ms | 3.9× | 6.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 17.14ms | 68.73ms | 4.0× | 6.0% | PASS |
| ac_writes | `oltp_write_only_ac` | 17.33ms | 69.66ms | 4.0× | 5.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 15.01ms | 65.41ms | 4.4× | 6.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 22.09ms | 75.83ms | 3.4× | 6.4% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 31.32ms | 34.59ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_range_select` | 13.90ms | 13.39ms | 1.0× | 2.6% | PASS |
| mem_reads | `oltp_sum_range` | 12.17ms | 12.91ms | 1.1× | 1.9% | PASS |
| mem_reads | `oltp_order_range` | 3.12ms | 3.15ms | 1.0× | 1.0% | PASS |
| mem_reads | `oltp_distinct_range` | 4.23ms | 4.28ms | 1.0× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 4.80ms | 5.97ms | 1.2× | 1.3% | PASS |
| mem_reads | `select_random_points` | 19.29ms | 20.45ms | 1.1× | 3.8% | PASS |
| mem_reads | `select_random_ranges` | 4.35ms | 5.25ms | 1.2× | 1.5% | PASS |
| mem_reads | `covering_index_scan` | 4.96ms | 4.73ms | 1.0× | 2.5% | PASS |
| mem_reads | `groupby_scan` | 34.72ms | 35.84ms | 1.0× | 0.8% | PASS |
| mem_reads | `index_join` | 7.25ms | 9.65ms | 1.3× | 3.7% | PASS |
| mem_reads | `index_join_scan` | 5.02ms | 5.77ms | 1.1× | 3.4% | PASS |
| mem_reads | `types_table_scan` | 1.22s | 1.27s | 1.0× | 3.4% | PASS |
| mem_reads | `table_scan` | 1.59s | 1.43s | 0.9× | 0.5% | PASS |
| mem_reads | `oltp_read_only` | 134.81ms | 135.84ms | 1.0× | 1.5% | PASS |
| mem_writes | `oltp_bulk_insert` | 237.90ms | 337.04ms | 1.4× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 21.65ms | 38.85ms | 1.8× | 1.7% | PASS |
| mem_writes | `oltp_update_index` | 75.66ms | 140.94ms | 1.9× | 2.1% | PASS |
| mem_writes | `oltp_update_non_index` | 51.58ms | 79.20ms | 1.5× | 1.4% | PASS |
| mem_writes | `oltp_delete_insert` | 52.45ms | 105.82ms | 2.0× | 1.3% | PASS |
| mem_writes | `oltp_write_only` | 29.96ms | 63.36ms | 2.1× | 1.4% | PASS |
| mem_writes | `types_delete_insert` | 34.02ms | 52.74ms | 1.6× | 1.4% | PASS |
| mem_writes | `oltp_read_write` | 92.18ms | 143.96ms | 1.6× | 1.4% | PASS |
| file_reads | `oltp_point_select` | 116.67ms | 56.87ms | 0.5× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 23.73ms | 15.79ms | 0.7× | 1.5% | PASS |
| file_reads | `oltp_sum_range` | 22.18ms | 15.52ms | 0.7× | 1.5% | PASS |
| file_reads | `oltp_order_range` | 4.10ms | 3.44ms | 0.8× | 0.8% | PASS |
| file_reads | `oltp_distinct_range` | 5.23ms | 4.53ms | 0.9× | 0.9% | PASS |
| file_reads | `oltp_index_scan` | 13.72ms | 8.30ms | 0.6× | 0.6% | PASS |
| file_reads | `select_random_points` | 28.66ms | 23.25ms | 0.8× | 1.1% | PASS |
| file_reads | `select_random_ranges` | 12.96ms | 7.44ms | 0.6× | 0.7% | PASS |
| file_reads | `covering_index_scan` | 14.06ms | 6.90ms | 0.5× | 0.9% | PASS |
| file_reads | `groupby_scan` | 35.32ms | 35.86ms | 1.0× | 0.7% | PASS |
| file_reads | `index_join` | 12.05ms | 10.62ms | 0.9× | 1.3% | PASS |
| file_reads | `index_join_scan` | 5.47ms | 5.76ms | 1.1× | 1.8% | PASS |
| file_reads | `types_table_scan` | 1.13s | 1.24s | 1.1× | 0.7% | PASS |
| file_reads | `table_scan` | 1.45s | 1.40s | 1.0× | 5.1% | PASS |
| file_reads | `oltp_read_only` | 246.44ms | 161.78ms | 0.7× | 1.1% | PASS |
| file_writes | `oltp_bulk_insert` | 260.69ms | 344.31ms | 1.3× | 1.0% | PASS |
| file_writes | `oltp_insert` | 31.73ms | 44.87ms | 1.4× | 1.7% | PASS |
| file_writes | `oltp_update_index` | 108.03ms | 150.23ms | 1.4× | 1.5% | PASS |
| file_writes | `oltp_update_non_index` | 79.92ms | 90.86ms | 1.1× | 2.2% | PASS |
| file_writes | `oltp_delete_insert` | 83.64ms | 116.61ms | 1.4× | 1.8% | PASS |
| file_writes | `oltp_write_only` | 55.69ms | 71.27ms | 1.3× | 2.5% | PASS |
| file_writes | `types_delete_insert` | 51.57ms | 59.60ms | 1.2× | 1.7% | PASS |
| file_writes | `oltp_read_write` | 114.64ms | 148.23ms | 1.3× | 2.0% | PASS |
| ac_reads | `oltp_point_select` | 58.96ms | 56.08ms | 1.0× | 2.2% | PASS |
| ac_reads | `oltp_range_select` | 17.35ms | 15.55ms | 0.9× | 3.0% | PASS |
| ac_reads | `oltp_sum_range` | 16.12ms | 15.27ms | 0.9× | 2.7% | PASS |
| ac_reads | `oltp_order_range` | 3.57ms | 3.43ms | 1.0× | 2.1% | PASS |
| ac_reads | `oltp_distinct_range` | 4.68ms | 4.51ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_index_scan` | 8.02ms | 8.15ms | 1.0× | 1.0% | PASS |
| ac_reads | `select_random_points` | 22.27ms | 22.54ms | 1.0× | 2.0% | PASS |
| ac_reads | `select_random_ranges` | 7.26ms | 7.37ms | 1.0× | 1.1% | PASS |
| ac_reads | `covering_index_scan` | 8.45ms | 6.88ms | 0.8× | 1.2% | PASS |
| ac_reads | `groupby_scan` | 34.69ms | 35.68ms | 1.0× | 0.8% | PASS |
| ac_reads | `index_join` | 9.43ms | 10.87ms | 1.2× | 1.9% | PASS |
| ac_reads | `index_join_scan` | 5.02ms | 5.89ms | 1.2× | 1.8% | PASS |
| ac_reads | `types_table_scan` | 1.19s | 1.27s | 1.1× | 1.6% | PASS |
| ac_reads | `table_scan` | 1.50s | 1.40s | 0.9× | 3.3% | PASS |
| ac_reads | `oltp_read_only` | 166.03ms | 161.36ms | 1.0× | 1.6% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 16.26ms | 57.57ms | 3.5× | 4.4% | PASS |
| ac_writes | `oltp_insert_ac` | 18.62ms | 73.98ms | 4.0× | 5.6% | PASS |
| ac_writes | `oltp_update_index_ac` | 21.19ms | 89.13ms | 4.2× | 6.3% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 16.64ms | 67.41ms | 4.1× | 5.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 18.56ms | 78.95ms | 4.3× | 4.5% | PASS |
| ac_writes | `oltp_write_only_ac` | 19.05ms | 76.16ms | 4.0× | 5.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 16.38ms | 69.44ms | 4.2× | 5.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 24.76ms | 81.17ms | 3.3× | 4.1% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.25ms | 36.00ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_range_select` | 19.57ms | 20.51ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_sum_range` | 18.09ms | 19.89ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_order_range` | 3.68ms | 3.83ms | 1.0× | 0.9% | PASS |
| mem_reads | `oltp_distinct_range` | 4.75ms | 5.00ms | 1.1× | 0.7% | PASS |
| mem_reads | `oltp_index_scan` | 4.61ms | 5.54ms | 1.2× | 1.7% | PASS |
| mem_reads | `select_random_points` | 27.22ms | 29.92ms | 1.1× | 0.8% | PASS |
| mem_reads | `select_random_ranges` | 7.51ms | 8.06ms | 1.1× | 1.1% | PASS |
| mem_reads | `covering_index_scan` | 4.34ms | 4.13ms | 1.0× | 1.2% | PASS |
| mem_reads | `groupby_scan` | 38.67ms | 41.34ms | 1.1× | 1.1% | PASS |
| mem_reads | `index_join` | 8.09ms | 9.86ms | 1.2× | 1.4% | PASS |
| mem_reads | `index_join_scan` | 4.31ms | 5.47ms | 1.3× | 1.2% | PASS |
| mem_reads | `types_table_scan` | 1.13s | 1.24s | 1.1× | 0.7% | PASS |
| mem_reads | `table_scan` | 1.30s | 1.36s | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_read_only` | 149.55ms | 161.86ms | 1.1× | 1.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 244.93ms | 332.98ms | 1.4× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 20.04ms | 35.36ms | 1.8× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 70.18ms | 120.28ms | 1.7× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 51.69ms | 75.22ms | 1.5× | 0.9% | PASS |
| mem_writes | `oltp_delete_insert` | 49.98ms | 94.21ms | 1.9× | 0.9% | PASS |
| mem_writes | `oltp_write_only` | 27.24ms | 57.15ms | 2.1× | 0.7% | PASS |
| mem_writes | `types_delete_insert` | 32.50ms | 50.66ms | 1.6× | 0.8% | PASS |
| mem_writes | `oltp_read_write` | 97.92ms | 150.50ms | 1.5× | 0.9% | PASS |
| file_reads | `oltp_point_select` | 116.86ms | 57.18ms | 0.5× | 1.3% | PASS |
| file_reads | `oltp_range_select` | 27.76ms | 22.75ms | 0.8× | 1.7% | PASS |
| file_reads | `oltp_sum_range` | 26.78ms | 22.24ms | 0.8× | 1.6% | PASS |
| file_reads | `oltp_order_range` | 4.54ms | 4.12ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_distinct_range` | 5.62ms | 5.30ms | 0.9× | 1.6% | PASS |
| file_reads | `oltp_index_scan` | 13.21ms | 8.06ms | 0.6× | 1.5% | PASS |
| file_reads | `select_random_points` | 36.97ms | 32.62ms | 0.9× | 1.6% | PASS |
| file_reads | `select_random_ranges` | 16.04ms | 10.44ms | 0.7× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 13.14ms | 6.65ms | 0.5× | 1.9% | PASS |
| file_reads | `groupby_scan` | 39.88ms | 41.71ms | 1.0× | 1.1% | PASS |
| file_reads | `index_join` | 12.80ms | 11.98ms | 0.9× | 1.1% | PASS |
| file_reads | `index_join_scan` | 5.22ms | 5.86ms | 1.1× | 1.1% | PASS |
| file_reads | `types_table_scan` | 1.11s | 1.24s | 1.1× | 0.4% | PASS |
| file_reads | `table_scan` | 1.29s | 1.36s | 1.0× | 0.5% | PASS |
| file_reads | `oltp_read_only` | 270.04ms | 192.83ms | 0.7× | 0.5% | PASS |
| file_writes | `oltp_bulk_insert` | 260.74ms | 340.87ms | 1.3× | 0.8% | PASS |
| file_writes | `oltp_insert` | 25.87ms | 40.04ms | 1.5× | 1.7% | PASS |
| file_writes | `oltp_update_index` | 96.36ms | 129.63ms | 1.3× | 0.9% | PASS |
| file_writes | `oltp_update_non_index` | 76.20ms | 88.98ms | 1.2× | 1.4% | PASS |
| file_writes | `oltp_delete_insert` | 76.17ms | 105.27ms | 1.4× | 1.5% | PASS |
| file_writes | `oltp_write_only` | 49.12ms | 66.60ms | 1.4× | 1.8% | PASS |
| file_writes | `types_delete_insert` | 48.64ms | 56.59ms | 1.2× | 1.4% | PASS |
| file_writes | `oltp_read_write` | 121.90ms | 160.46ms | 1.3× | 1.6% | PASS |
| ac_reads | `oltp_point_select` | 61.40ms | 57.47ms | 0.9× | 0.8% | PASS |
| ac_reads | `oltp_range_select` | 23.17ms | 22.75ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_sum_range` | 21.47ms | 22.23ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_order_range` | 4.11ms | 4.13ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_distinct_range` | 5.16ms | 5.32ms | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_index_scan` | 7.78ms | 8.17ms | 1.0× | 1.3% | PASS |
| ac_reads | `select_random_points` | 32.03ms | 32.83ms | 1.0× | 1.5% | PASS |
| ac_reads | `select_random_ranges` | 10.72ms | 10.50ms | 1.0× | 1.2% | PASS |
| ac_reads | `covering_index_scan` | 7.65ms | 6.78ms | 0.9× | 1.0% | PASS |
| ac_reads | `groupby_scan` | 39.35ms | 41.91ms | 1.1× | 1.1% | PASS |
| ac_reads | `index_join` | 10.16ms | 11.98ms | 1.2× | 1.4% | PASS |
| ac_reads | `index_join_scan` | 4.87ms | 5.92ms | 1.2× | 1.3% | PASS |
| ac_reads | `types_table_scan` | 1.14s | 1.26s | 1.1× | 1.2% | PASS |
| ac_reads | `table_scan` | 1.33s | 1.37s | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_read_only` | 191.40ms | 193.10ms | 1.0× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 15.55ms | 55.28ms | 3.6× | 3.3% | PASS |
| ac_writes | `oltp_insert_ac` | 17.95ms | 71.53ms | 4.0× | 3.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 19.17ms | 80.87ms | 4.2× | 2.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 16.22ms | 65.37ms | 4.0× | 4.7% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 17.47ms | 75.78ms | 4.3× | 3.9% | PASS |
| ac_writes | `oltp_write_only_ac` | 17.62ms | 74.64ms | 4.2× | 4.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 15.47ms | 65.55ms | 4.2× | 5.0% | PASS |
| ac_writes | `oltp_read_write_ac` | 25.13ms | 82.35ms | 3.3× | 3.6% | PASS |

</details>

</details>

## Version-control latency

Wall time: 1m 47s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 53.46ms | 130.00ms | 41.1% | 1.4% | PASS |
| `status_dirty_many_tables` | 55.80ms | 130.00ms | 42.9% | 1.8% | PASS |
| `diff_regular_working_one_table` | 51.01ms | 120.00ms | 42.5% | 1.4% | PASS |
| `diff_regular_working_many_tables` | 59.01ms | 140.00ms | 42.2% | 1.0% | PASS |
| `diff_stat_working_many_tables` | 59.35ms | 140.00ms | 42.4% | 1.3% | PASS |
| `diff_schema_working_many_tables` | 59.83ms | 140.00ms | 42.7% | 1.5% | PASS |
| `branch_list_many_branches` | 16.78ms | 35.00ms | 47.9% | 1.9% | PASS |
| `branch_create_delete` | 29.53ms | 40.00ms | 73.8% | 9.3% | PASS |
| `checkout_branch_clean` | 90.59ms | 150.00ms | 60.4% | 7.1% | PASS |
| `merge_data_no_conflicts` | 32.77ms | 50.00ms | 65.5% | 4.0% | PASS |
| `merge_schema_no_conflicts` | 17.98ms | 35.00ms | 51.4% | 3.7% | PASS |
| `merge_data_conflicts` | 65.51ms | 180.00ms | 36.4% | 1.3% | PASS |
| `merge_data_conflicts_with_resolve` | 65.87ms | 180.00ms | 36.6% | 1.4% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
