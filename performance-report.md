# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-09-01 11:12 UTC
>
> Commit: [`deff2aa9be7a389451bfe03c2701ebeb09a3df1c`](https://github.com/dolthub/doltlite/commit/deff2aa9be7a389451bfe03c2701ebeb09a3df1c)
>
> Runner: ubuntu24 20260823.283.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/33493078887)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.46s | 9.94s | 1.1× | 1.9% | **PASS** |
| Writes | 1.93s | 3.02s | 1.6× | 1.9% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.48s | 10.26s | 1.0× | 2.0% | **PASS** |
| Writes | 4.23s | 4.20s | 1.0× | 9.2% | **PASS** |
| Autocommit writes | 2.23s | 7.10s | 3.2× | 13.8% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.64s | 2.72s | 1.0× | 4.3% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.78s | 2.81s | 1.0× | 2.1% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 1.64s | 1.73s | 1.1× | 2.4% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.39s | 2.69s | 1.1× | 1.0% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 448.94ms | 656.55ms | 1.5× | 3.3% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 615.98ms | 997.04ms | 1.6× | 2.0% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 356.11ms | 574.96ms | 1.6× | 1.8% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 512.16ms | 787.25ms | 1.5× | 0.8% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 3.01s | 2.87s | 1.0× | 3.1% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 3.14s | 2.90s | 0.9× | 2.1% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 1.83s | 1.77s | 1.0× | 2.0% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.50s | 2.72s | 1.1× | 1.1% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 1.07s | 1.04s | 1.0× | 15.7% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 991.60ms | 1.12s | 1.1× | 3.5% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 1.52s | 1.17s | 0.8× | 31.9% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 646.71ms | 863.35ms | 1.3× | 1.4% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.89s | 2.88s | 1.0× | 2.9% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.97s | 2.90s | 1.0× | 2.1% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 1.74s | 1.79s | 1.0× | 2.9% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.44s | 2.72s | 1.1× | 1.3% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 396.81ms | 1.27s | 3.2× | 37.6% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 224.58ms | 771.04ms | 3.4× | 6.3% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 1.47s | 4.50s | 3.1× | 57.6% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 138.77ms | 559.35ms | 4.0× | 8.0% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 23.91ms | 26.66ms | 1.1× | 4.1% | PASS |
| mem_reads | `oltp_range_select` | 10.70ms | 10.55ms | 1.0× | 4.3% | PASS |
| mem_reads | `oltp_sum_range` | 10.05ms | 10.46ms | 1.0× | 5.0% | PASS |
| mem_reads | `oltp_order_range` | 2.75ms | 2.75ms | 1.0× | 2.2% | PASS |
| mem_reads | `oltp_distinct_range` | 3.81ms | 3.90ms | 1.0× | 5.3% | PASS |
| mem_reads | `oltp_index_scan` | 3.85ms | 4.82ms | 1.3× | 4.4% | PASS |
| mem_reads | `select_random_points` | 10.48ms | 10.86ms | 1.0× | 5.5% | PASS |
| mem_reads | `select_random_ranges` | 3.03ms | 3.79ms | 1.3× | 3.3% | PASS |
| mem_reads | `covering_index_scan` | 3.99ms | 4.08ms | 1.0× | 5.4% | PASS |
| mem_reads | `groupby_scan` | 32.52ms | 34.18ms | 1.1× | 3.4% | PASS |
| mem_reads | `index_join` | 5.64ms | 7.62ms | 1.4× | 4.9% | PASS |
| mem_reads | `index_join_scan` | 3.36ms | 4.85ms | 1.4× | 6.0% | PASS |
| mem_reads | `types_table_scan` | 1.12s | 1.17s | 1.0× | 1.1% | PASS |
| mem_reads | `table_scan` | 1.30s | 1.31s | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_read_only` | 107.58ms | 113.44ms | 1.1× | 3.9% | PASS |
| mem_writes | `oltp_bulk_insert` | 180.51ms | 240.20ms | 1.3× | 2.8% | PASS |
| mem_writes | `oltp_insert` | 15.87ms | 26.27ms | 1.7× | 3.2% | PASS |
| mem_writes | `oltp_update_index` | 52.35ms | 88.19ms | 1.7× | 3.4% | PASS |
| mem_writes | `oltp_update_non_index` | 37.37ms | 50.43ms | 1.3× | 3.4% | PASS |
| mem_writes | `oltp_delete_insert` | 47.17ms | 69.58ms | 1.5× | 3.4% | PASS |
| mem_writes | `oltp_write_only` | 23.11ms | 44.52ms | 1.9× | 3.2% | PASS |
| mem_writes | `types_delete_insert` | 25.08ms | 33.24ms | 1.3× | 3.5% | PASS |
| mem_writes | `oltp_read_write` | 67.48ms | 104.12ms | 1.5× | 3.1% | PASS |
| file_reads | `oltp_point_select` | 104.78ms | 48.17ms | 0.5× | 2.4% | PASS |
| file_reads | `oltp_range_select` | 19.78ms | 13.18ms | 0.7× | 3.8% | PASS |
| file_reads | `oltp_sum_range` | 18.90ms | 13.10ms | 0.7× | 4.1% | PASS |
| file_reads | `oltp_order_range` | 3.70ms | 3.12ms | 0.8× | 4.0% | PASS |
| file_reads | `oltp_distinct_range` | 4.76ms | 4.16ms | 0.9× | 3.1% | PASS |
| file_reads | `oltp_index_scan` | 12.60ms | 7.45ms | 0.6× | 2.5% | PASS |
| file_reads | `select_random_points` | 20.36ms | 13.96ms | 0.7× | 2.3% | PASS |
| file_reads | `select_random_ranges` | 11.55ms | 6.19ms | 0.5× | 2.6% | PASS |
| file_reads | `covering_index_scan` | 12.91ms | 6.74ms | 0.5× | 3.9% | PASS |
| file_reads | `groupby_scan` | 33.47ms | 34.15ms | 1.0× | 5.1% | PASS |
| file_reads | `index_join` | 10.19ms | 9.29ms | 0.9× | 4.3% | PASS |
| file_reads | `index_join_scan` | 4.24ms | 5.04ms | 1.2× | 5.1% | PASS |
| file_reads | `types_table_scan` | 1.14s | 1.19s | 1.0× | 1.1% | PASS |
| file_reads | `table_scan` | 1.37s | 1.36s | 1.0× | 2.4% | PASS |
| file_reads | `oltp_read_only` | 236.42ms | 151.39ms | 0.6× | 1.7% | PASS |
| file_writes | `oltp_bulk_insert` | 255.15ms | 308.56ms | 1.2× | 19.3% | PASS |
| file_writes | `oltp_insert` | 29.74ms | 38.95ms | 1.3× | 9.9% | PASS |
| file_writes | `oltp_update_index` | 181.28ms | 157.59ms | 0.9× | 19.0% | PASS |
| file_writes | `oltp_update_non_index` | 134.05ms | 105.22ms | 0.8× | 11.3% | PASS |
| file_writes | `oltp_delete_insert` | 138.26ms | 136.00ms | 1.0× | 15.8% | PASS |
| file_writes | `oltp_write_only` | 106.34ms | 82.05ms | 0.8× | 29.4% | PASS |
| file_writes | `types_delete_insert` | 75.07ms | 69.33ms | 0.9× | 15.5% | PASS |
| file_writes | `oltp_read_write` | 147.13ms | 145.60ms | 1.0× | 5.8% | PASS |
| ac_reads | `oltp_point_select` | 53.93ms | 51.22ms | 0.9× | 1.3% | PASS |
| ac_reads | `oltp_range_select` | 15.61ms | 14.41ms | 0.9× | 4.8% | PASS |
| ac_reads | `oltp_sum_range` | 14.21ms | 14.03ms | 1.0× | 3.3% | PASS |
| ac_reads | `oltp_order_range` | 3.38ms | 3.24ms | 1.0× | 2.9% | PASS |
| ac_reads | `oltp_distinct_range` | 4.38ms | 4.38ms | 1.0× | 2.1% | PASS |
| ac_reads | `oltp_index_scan` | 7.44ms | 7.75ms | 1.0× | 1.8% | PASS |
| ac_reads | `select_random_points` | 15.03ms | 14.42ms | 1.0× | 3.7% | PASS |
| ac_reads | `select_random_ranges` | 6.27ms | 6.33ms | 1.0× | 1.8% | PASS |
| ac_reads | `covering_index_scan` | 7.51ms | 6.74ms | 0.9× | 2.9% | PASS |
| ac_reads | `groupby_scan` | 33.88ms | 35.00ms | 1.0× | 3.9% | PASS |
| ac_reads | `index_join` | 7.88ms | 9.93ms | 1.3× | 2.5% | PASS |
| ac_reads | `index_join_scan` | 4.00ms | 5.47ms | 1.4× | 4.2% | PASS |
| ac_reads | `types_table_scan` | 1.15s | 1.20s | 1.0× | 1.8% | PASS |
| ac_reads | `table_scan` | 1.40s | 1.35s | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_read_only` | 157.93ms | 154.25ms | 1.0× | 3.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 36.31ms | 97.37ms | 2.7× | 17.9% | PASS |
| ac_writes | `oltp_insert_ac` | 38.66ms | 136.89ms | 3.5× | 20.7% | PASS |
| ac_writes | `oltp_update_index_ac` | 47.12ms | 164.21ms | 3.5× | 35.0% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 50.43ms | 192.82ms | 3.8× | 44.2% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 83.30ms | 174.10ms | 2.1× | 52.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 53.45ms | 214.33ms | 4.0× | 48.7% | PASS |
| ac_writes | `types_delete_insert_ac` | 41.71ms | 153.78ms | 3.7× | 40.2% | PASS |
| ac_writes | `oltp_read_write_ac` | 45.84ms | 137.12ms | 3.0× | 30.3% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 37.06ms | 38.23ms | 1.0× | 3.1% | PASS |
| mem_reads | `oltp_range_select` | 16.79ms | 13.64ms | 0.8× | 2.3% | PASS |
| mem_reads | `oltp_sum_range` | 16.03ms | 13.78ms | 0.9× | 3.2% | PASS |
| mem_reads | `oltp_order_range` | 3.38ms | 3.27ms | 1.0× | 1.5% | PASS |
| mem_reads | `oltp_distinct_range` | 4.55ms | 4.41ms | 1.0× | 1.5% | PASS |
| mem_reads | `oltp_index_scan` | 4.06ms | 6.24ms | 1.5× | 1.7% | PASS |
| mem_reads | `select_random_points` | 23.26ms | 21.65ms | 0.9× | 3.2% | PASS |
| mem_reads | `select_random_ranges` | 3.71ms | 5.33ms | 1.4× | 1.5% | PASS |
| mem_reads | `covering_index_scan` | 4.19ms | 4.75ms | 1.1× | 2.5% | PASS |
| mem_reads | `groupby_scan` | 36.10ms | 35.89ms | 1.0× | 1.3% | PASS |
| mem_reads | `index_join` | 11.05ms | 9.70ms | 0.9× | 2.4% | PASS |
| mem_reads | `index_join_scan` | 3.88ms | 5.46ms | 1.4× | 2.1% | PASS |
| mem_reads | `types_table_scan` | 1.12s | 1.18s | 1.1× | 1.3% | PASS |
| mem_reads | `table_scan` | 1.36s | 1.32s | 1.0× | 2.2% | PASS |
| mem_reads | `oltp_read_only` | 144.17ms | 138.93ms | 1.0× | 1.9% | PASS |
| mem_writes | `oltp_bulk_insert` | 246.41ms | 367.83ms | 1.5× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 18.45ms | 39.41ms | 2.1× | 1.5% | PASS |
| mem_writes | `oltp_update_index` | 66.72ms | 138.32ms | 2.1× | 2.1% | PASS |
| mem_writes | `oltp_update_non_index` | 49.27ms | 79.16ms | 1.6× | 2.2% | PASS |
| mem_writes | `oltp_delete_insert` | 55.69ms | 105.52ms | 1.9× | 1.9% | PASS |
| mem_writes | `oltp_write_only` | 29.03ms | 61.75ms | 2.1× | 1.9% | PASS |
| mem_writes | `types_delete_insert` | 40.98ms | 54.85ms | 1.3× | 2.3% | PASS |
| mem_writes | `oltp_read_write` | 109.41ms | 150.19ms | 1.4× | 3.1% | PASS |
| file_reads | `oltp_point_select` | 109.86ms | 58.83ms | 0.5× | 1.2% | PASS |
| file_reads | `oltp_range_select` | 26.34ms | 16.49ms | 0.6× | 2.1% | PASS |
| file_reads | `oltp_sum_range` | 24.32ms | 16.24ms | 0.7× | 1.9% | PASS |
| file_reads | `oltp_order_range` | 4.42ms | 3.68ms | 0.8× | 2.1% | PASS |
| file_reads | `oltp_distinct_range` | 5.47ms | 4.74ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_index_scan` | 11.44ms | 8.44ms | 0.7× | 1.5% | PASS |
| file_reads | `select_random_points` | 33.61ms | 24.48ms | 0.7× | 4.3% | PASS |
| file_reads | `select_random_ranges` | 10.86ms | 7.32ms | 0.7× | 1.4% | PASS |
| file_reads | `covering_index_scan` | 11.80ms | 6.92ms | 0.6× | 1.9% | PASS |
| file_reads | `groupby_scan` | 37.01ms | 36.40ms | 1.0× | 2.5% | PASS |
| file_reads | `index_join` | 15.35ms | 11.07ms | 0.7× | 4.0% | PASS |
| file_reads | `index_join_scan` | 4.95ms | 5.90ms | 1.2× | 2.7% | PASS |
| file_reads | `types_table_scan` | 1.20s | 1.20s | 1.0× | 3.0% | PASS |
| file_reads | `table_scan` | 1.38s | 1.32s | 1.0× | 2.8% | PASS |
| file_reads | `oltp_read_only` | 258.68ms | 172.29ms | 0.7× | 1.6% | PASS |
| file_writes | `oltp_bulk_insert` | 272.02ms | 383.40ms | 1.4× | 1.1% | PASS |
| file_writes | `oltp_insert` | 26.84ms | 47.02ms | 1.8× | 2.2% | PASS |
| file_writes | `oltp_update_index` | 132.31ms | 166.03ms | 1.3× | 6.2% | PASS |
| file_writes | `oltp_update_non_index` | 119.43ms | 98.31ms | 0.8× | 10.3% | PASS |
| file_writes | `oltp_delete_insert` | 102.96ms | 124.60ms | 1.2× | 3.1% | PASS |
| file_writes | `oltp_write_only` | 84.74ms | 74.36ms | 0.9× | 8.4% | PASS |
| file_writes | `types_delete_insert` | 74.86ms | 64.89ms | 0.9× | 2.0% | PASS |
| file_writes | `oltp_read_write` | 178.44ms | 165.56ms | 0.9× | 3.9% | PASS |
| ac_reads | `oltp_point_select` | 62.87ms | 58.97ms | 0.9× | 1.6% | PASS |
| ac_reads | `oltp_range_select` | 21.08ms | 16.24ms | 0.8× | 2.2% | PASS |
| ac_reads | `oltp_sum_range` | 19.42ms | 16.42ms | 0.8× | 2.0% | PASS |
| ac_reads | `oltp_order_range` | 4.04ms | 3.72ms | 0.9× | 2.1% | PASS |
| ac_reads | `oltp_distinct_range` | 5.11ms | 4.79ms | 0.9× | 2.2% | PASS |
| ac_reads | `oltp_index_scan` | 6.81ms | 8.48ms | 1.2× | 1.3% | PASS |
| ac_reads | `select_random_points` | 27.57ms | 24.24ms | 0.9× | 2.4% | PASS |
| ac_reads | `select_random_ranges` | 6.23ms | 7.29ms | 1.2× | 1.2% | PASS |
| ac_reads | `covering_index_scan` | 7.17ms | 6.87ms | 1.0× | 2.0% | PASS |
| ac_reads | `groupby_scan` | 36.42ms | 36.11ms | 1.0× | 1.5% | PASS |
| ac_reads | `index_join` | 13.47ms | 11.13ms | 0.8× | 2.9% | PASS |
| ac_reads | `index_join_scan` | 4.50ms | 5.94ms | 1.3× | 4.6% | PASS |
| ac_reads | `types_table_scan` | 1.18s | 1.19s | 1.0× | 2.4% | PASS |
| ac_reads | `table_scan` | 1.39s | 1.33s | 1.0× | 2.6% | PASS |
| ac_reads | `oltp_read_only` | 188.90ms | 173.83ms | 0.9× | 1.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 26.27ms | 82.28ms | 3.1× | 6.4% | PASS |
| ac_writes | `oltp_insert_ac` | 27.85ms | 91.01ms | 3.3× | 5.6% | PASS |
| ac_writes | `oltp_update_index_ac` | 29.30ms | 107.29ms | 3.7× | 6.5% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.15ms | 89.88ms | 3.7× | 6.2% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 27.82ms | 99.20ms | 3.6× | 4.8% | PASS |
| ac_writes | `oltp_write_only_ac` | 28.11ms | 96.62ms | 3.4× | 5.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 25.84ms | 94.57ms | 3.7× | 9.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 35.23ms | 110.20ms | 3.1× | 6.6% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 22.14ms | 22.40ms | 1.0× | 2.9% | PASS |
| mem_reads | `oltp_range_select` | 9.74ms | 8.40ms | 0.9× | 3.2% | PASS |
| mem_reads | `oltp_sum_range` | 9.20ms | 8.46ms | 0.9× | 2.5% | PASS |
| mem_reads | `oltp_order_range` | 1.99ms | 1.95ms | 1.0× | 1.9% | PASS |
| mem_reads | `oltp_distinct_range` | 2.53ms | 2.53ms | 1.0× | 2.8% | PASS |
| mem_reads | `oltp_index_scan` | 2.49ms | 3.77ms | 1.5× | 1.9% | PASS |
| mem_reads | `select_random_points` | 15.36ms | 14.16ms | 0.9× | 2.4% | PASS |
| mem_reads | `select_random_ranges` | 2.40ms | 3.54ms | 1.5× | 1.9% | PASS |
| mem_reads | `covering_index_scan` | 2.44ms | 2.98ms | 1.2× | 1.9% | PASS |
| mem_reads | `groupby_scan` | 20.94ms | 20.54ms | 1.0× | 0.9% | PASS |
| mem_reads | `index_join` | 7.92ms | 6.09ms | 0.8× | 2.4% | PASS |
| mem_reads | `index_join_scan` | 2.67ms | 4.52ms | 1.7× | 4.6% | PASS |
| mem_reads | `types_table_scan` | 687.00ms | 737.39ms | 1.1× | 1.5% | PASS |
| mem_reads | `table_scan` | 778.06ms | 820.91ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_read_only` | 77.23ms | 73.23ms | 0.9× | 2.7% | PASS |
| mem_writes | `oltp_bulk_insert` | 138.04ms | 201.92ms | 1.5× | 1.4% | PASS |
| mem_writes | `oltp_insert` | 10.72ms | 22.16ms | 2.1× | 1.6% | PASS |
| mem_writes | `oltp_update_index` | 41.42ms | 84.56ms | 2.0× | 2.9% | PASS |
| mem_writes | `oltp_update_non_index` | 32.64ms | 49.31ms | 1.5× | 1.5% | PASS |
| mem_writes | `oltp_delete_insert` | 33.48ms | 64.28ms | 1.9× | 2.3% | PASS |
| mem_writes | `oltp_write_only` | 18.47ms | 38.95ms | 2.1× | 2.2% | PASS |
| mem_writes | `types_delete_insert` | 24.84ms | 32.39ms | 1.3× | 1.9% | PASS |
| mem_writes | `oltp_read_write` | 56.48ms | 81.38ms | 1.4× | 1.6% | PASS |
| file_reads | `oltp_point_select` | 76.11ms | 37.01ms | 0.5× | 1.6% | PASS |
| file_reads | `oltp_range_select` | 15.45ms | 9.84ms | 0.6× | 1.8% | PASS |
| file_reads | `oltp_sum_range` | 14.94ms | 9.95ms | 0.7× | 2.6% | PASS |
| file_reads | `oltp_order_range` | 2.74ms | 2.17ms | 0.8× | 2.3% | PASS |
| file_reads | `oltp_distinct_range` | 3.23ms | 2.71ms | 0.8× | 2.1% | PASS |
| file_reads | `oltp_index_scan` | 8.31ms | 5.58ms | 0.7× | 2.4% | PASS |
| file_reads | `select_random_points` | 20.15ms | 14.74ms | 0.7× | 1.9% | PASS |
| file_reads | `select_random_ranges` | 7.92ms | 5.11ms | 0.6× | 1.6% | PASS |
| file_reads | `covering_index_scan` | 8.39ms | 4.74ms | 0.6× | 2.0% | PASS |
| file_reads | `groupby_scan` | 21.34ms | 20.47ms | 1.0× | 1.3% | PASS |
| file_reads | `index_join` | 10.94ms | 7.27ms | 0.7× | 2.7% | PASS |
| file_reads | `index_join_scan` | 3.40ms | 4.47ms | 1.3× | 2.6% | PASS |
| file_reads | `types_table_scan` | 698.17ms | 738.36ms | 1.1× | 1.3% | PASS |
| file_reads | `table_scan` | 778.38ms | 815.74ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_read_only` | 155.91ms | 94.77ms | 0.6× | 2.0% | PASS |
| file_writes | `oltp_bulk_insert` | 283.32ms | 302.84ms | 1.1× | 23.9% | PASS |
| file_writes | `oltp_insert` | 48.08ms | 66.83ms | 1.4× | 69.3% | PASS |
| file_writes | `oltp_update_index` | 196.34ms | 182.02ms | 0.9× | 27.0% | PASS |
| file_writes | `oltp_update_non_index` | 180.74ms | 109.50ms | 0.6× | 34.5% | PASS |
| file_writes | `oltp_delete_insert` | 220.22ms | 151.18ms | 0.7× | 38.9% | PASS |
| file_writes | `oltp_write_only` | 185.59ms | 102.81ms | 0.6× | 29.3% | PASS |
| file_writes | `types_delete_insert` | 159.25ms | 97.68ms | 0.6× | 44.4% | PASS |
| file_writes | `oltp_read_write` | 249.58ms | 153.77ms | 0.6× | 27.5% | PASS |
| ac_reads | `oltp_point_select` | 40.52ms | 37.19ms | 0.9× | 3.3% | PASS |
| ac_reads | `oltp_range_select` | 11.67ms | 9.76ms | 0.8× | 3.9% | PASS |
| ac_reads | `oltp_sum_range` | 11.62ms | 9.99ms | 0.9× | 2.7% | PASS |
| ac_reads | `oltp_order_range` | 2.40ms | 2.18ms | 0.9× | 2.9% | PASS |
| ac_reads | `oltp_distinct_range` | 2.98ms | 2.73ms | 0.9× | 1.4% | PASS |
| ac_reads | `oltp_index_scan` | 4.62ms | 5.61ms | 1.2× | 3.8% | PASS |
| ac_reads | `select_random_points` | 16.23ms | 14.59ms | 0.9× | 2.9% | PASS |
| ac_reads | `select_random_ranges` | 4.06ms | 4.92ms | 1.2× | 3.0% | PASS |
| ac_reads | `covering_index_scan` | 4.45ms | 4.56ms | 1.0× | 4.3% | PASS |
| ac_reads | `groupby_scan` | 20.51ms | 20.27ms | 1.0× | 2.5% | PASS |
| ac_reads | `index_join` | 8.71ms | 7.26ms | 0.8× | 3.1% | PASS |
| ac_reads | `index_join_scan` | 2.83ms | 4.26ms | 1.5× | 2.7% | PASS |
| ac_reads | `types_table_scan` | 706.30ms | 740.46ms | 1.0× | 2.6% | PASS |
| ac_reads | `table_scan` | 798.91ms | 826.86ms | 1.0× | 2.3% | PASS |
| ac_reads | `oltp_read_only` | 101.34ms | 94.84ms | 0.9× | 3.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 144.22ms | 359.62ms | 2.5× | 57.8% | PASS |
| ac_writes | `oltp_insert_ac` | 145.16ms | 430.80ms | 3.0× | 50.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 217.13ms | 552.40ms | 2.5× | 43.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 145.73ms | 691.19ms | 4.7× | 66.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 230.05ms | 629.24ms | 2.7× | 48.9% | PASS |
| ac_writes | `oltp_write_only_ac` | 186.86ms | 676.17ms | 3.6× | 57.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 197.75ms | 600.40ms | 3.0× | 60.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 203.52ms | 561.02ms | 2.8× | 70.6% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 29.08ms | 34.50ms | 1.2× | 1.2% | PASS |
| mem_reads | `oltp_range_select` | 15.97ms | 19.30ms | 1.2× | 1.0% | PASS |
| mem_reads | `oltp_sum_range` | 15.10ms | 19.13ms | 1.3× | 0.8% | PASS |
| mem_reads | `oltp_order_range` | 3.13ms | 3.53ms | 1.1× | 0.6% | PASS |
| mem_reads | `oltp_distinct_range` | 3.95ms | 4.64ms | 1.2× | 0.3% | PASS |
| mem_reads | `oltp_index_scan` | 3.90ms | 5.08ms | 1.3× | 1.4% | PASS |
| mem_reads | `select_random_points` | 24.84ms | 29.10ms | 1.2× | 1.1% | PASS |
| mem_reads | `select_random_ranges` | 6.43ms | 8.02ms | 1.2× | 0.6% | PASS |
| mem_reads | `covering_index_scan` | 3.63ms | 3.70ms | 1.0× | 1.6% | PASS |
| mem_reads | `groupby_scan` | 31.73ms | 37.82ms | 1.2× | 0.5% | PASS |
| mem_reads | `index_join` | 6.74ms | 9.21ms | 1.4× | 1.1% | PASS |
| mem_reads | `index_join_scan` | 3.25ms | 4.97ms | 1.5× | 1.0% | PASS |
| mem_reads | `types_table_scan` | 964.56ms | 1.12s | 1.2× | 0.8% | PASS |
| mem_reads | `table_scan` | 1.15s | 1.23s | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_read_only` | 127.85ms | 152.37ms | 1.2× | 0.9% | PASS |
| mem_writes | `oltp_bulk_insert` | 216.81ms | 285.98ms | 1.3× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 16.60ms | 29.14ms | 1.8× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 58.59ms | 103.74ms | 1.8× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 43.46ms | 64.32ms | 1.5× | 0.8% | PASS |
| mem_writes | `oltp_delete_insert` | 42.56ms | 80.10ms | 1.9× | 0.8% | PASS |
| mem_writes | `oltp_write_only` | 22.53ms | 46.75ms | 2.1× | 0.8% | PASS |
| mem_writes | `types_delete_insert` | 27.33ms | 43.11ms | 1.6× | 1.0% | PASS |
| mem_writes | `oltp_read_write` | 84.28ms | 134.11ms | 1.6× | 0.9% | PASS |
| file_reads | `oltp_point_select` | 59.27ms | 43.69ms | 0.7× | 1.5% | PASS |
| file_reads | `oltp_range_select` | 19.23ms | 20.51ms | 1.1× | 0.9% | PASS |
| file_reads | `oltp_sum_range` | 18.50ms | 20.48ms | 1.1× | 1.0% | PASS |
| file_reads | `oltp_order_range` | 3.50ms | 3.69ms | 1.1× | 1.7% | PASS |
| file_reads | `oltp_distinct_range` | 4.39ms | 4.84ms | 1.1× | 1.9% | PASS |
| file_reads | `oltp_index_scan` | 7.02ms | 6.28ms | 0.9× | 1.6% | PASS |
| file_reads | `select_random_points` | 28.63ms | 30.64ms | 1.1× | 0.9% | PASS |
| file_reads | `select_random_ranges` | 9.82ms | 9.27ms | 0.9× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 6.70ms | 4.82ms | 0.7× | 2.2% | PASS |
| file_reads | `groupby_scan` | 32.22ms | 38.01ms | 1.2× | 0.9% | PASS |
| file_reads | `index_join` | 8.59ms | 10.32ms | 1.2× | 1.3% | PASS |
| file_reads | `index_join_scan` | 3.68ms | 5.17ms | 1.4× | 1.1% | PASS |
| file_reads | `types_table_scan` | 965.68ms | 1.12s | 1.2× | 0.8% | PASS |
| file_reads | `table_scan` | 1.16s | 1.23s | 1.1× | 1.3% | PASS |
| file_reads | `oltp_read_only` | 174.97ms | 168.19ms | 1.0× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 230.13ms | 297.62ms | 1.3× | 0.9% | PASS |
| file_writes | `oltp_insert` | 24.91ms | 34.59ms | 1.4× | 0.8% | PASS |
| file_writes | `oltp_update_index` | 76.17ms | 115.05ms | 1.5× | 1.6% | PASS |
| file_writes | `oltp_update_non_index` | 63.00ms | 75.58ms | 1.2× | 1.6% | PASS |
| file_writes | `oltp_delete_insert` | 62.53ms | 90.93ms | 1.5× | 0.8% | PASS |
| file_writes | `oltp_write_only` | 44.20ms | 55.96ms | 1.3× | 1.5% | PASS |
| file_writes | `types_delete_insert` | 39.00ms | 49.70ms | 1.3× | 1.5% | PASS |
| file_writes | `oltp_read_write` | 106.75ms | 143.92ms | 1.3× | 1.2% | PASS |
| ac_reads | `oltp_point_select` | 38.71ms | 43.71ms | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_range_select` | 17.21ms | 20.54ms | 1.2× | 1.0% | PASS |
| ac_reads | `oltp_sum_range` | 16.35ms | 20.52ms | 1.3× | 1.1% | PASS |
| ac_reads | `oltp_order_range` | 3.30ms | 3.73ms | 1.1× | 2.2% | PASS |
| ac_reads | `oltp_distinct_range` | 4.14ms | 4.89ms | 1.2× | 1.7% | PASS |
| ac_reads | `oltp_index_scan` | 5.14ms | 6.27ms | 1.2× | 2.2% | PASS |
| ac_reads | `select_random_points` | 26.29ms | 30.59ms | 1.2× | 1.3% | PASS |
| ac_reads | `select_random_ranges` | 7.66ms | 9.28ms | 1.2× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 4.79ms | 4.79ms | 1.0× | 1.9% | PASS |
| ac_reads | `groupby_scan` | 32.08ms | 38.00ms | 1.2× | 0.6% | PASS |
| ac_reads | `index_join` | 7.49ms | 10.26ms | 1.4× | 1.9% | PASS |
| ac_reads | `index_join_scan` | 3.48ms | 5.17ms | 1.5× | 1.4% | PASS |
| ac_reads | `types_table_scan` | 966.71ms | 1.12s | 1.2× | 0.7% | PASS |
| ac_reads | `table_scan` | 1.16s | 1.23s | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_read_only` | 143.21ms | 167.65ms | 1.2× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 16.36ms | 55.66ms | 3.4× | 7.8% | PASS |
| ac_writes | `oltp_insert_ac` | 17.31ms | 69.72ms | 4.0× | 6.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 18.08ms | 80.28ms | 4.4× | 8.3% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 15.81ms | 64.43ms | 4.1× | 6.1% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 16.97ms | 74.56ms | 4.4× | 8.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 17.11ms | 72.72ms | 4.2× | 9.7% | PASS |
| ac_writes | `types_delete_insert_ac` | 15.23ms | 63.60ms | 4.2× | 9.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 21.90ms | 78.37ms | 3.6× | 7.8% | PASS |

</details>

</details>

## Version-control latency

Wall time: 1m 43s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 63.60ms | 130.00ms | 48.9% | 0.4% | PASS |
| `status_dirty_many_tables` | 65.80ms | 130.00ms | 50.6% | 0.4% | PASS |
| `diff_regular_working_one_table` | 59.05ms | 120.00ms | 49.2% | 0.5% | PASS |
| `diff_regular_working_many_tables` | 70.24ms | 140.00ms | 50.2% | 0.6% | PASS |
| `diff_stat_working_many_tables` | 70.06ms | 140.00ms | 50.0% | 1.1% | PASS |
| `diff_schema_working_many_tables` | 69.80ms | 140.00ms | 49.9% | 0.8% | PASS |
| `branch_list_many_branches` | 17.92ms | 35.00ms | 51.2% | 1.1% | PASS |
| `branch_create_delete` | 27.56ms | 40.00ms | 68.9% | 4.1% | PASS |
| `checkout_branch_clean` | 84.30ms | 150.00ms | 56.2% | 1.7% | PASS |
| `merge_data_no_conflicts` | 30.84ms | 50.00ms | 61.7% | 3.3% | PASS |
| `merge_schema_no_conflicts` | 20.34ms | 35.00ms | 58.1% | 1.7% | PASS |
| `merge_data_conflicts` | 26.25ms | 180.00ms | 14.6% | 3.0% | PASS |
| `merge_data_conflicts_with_resolve` | 25.80ms | 180.00ms | 14.3% | 1.0% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
