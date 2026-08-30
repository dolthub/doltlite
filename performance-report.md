# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-08-30 10:58 UTC
>
> Commit: [`e437eb2aefa3f476616863e4be6174febe1a7ec9`](https://github.com/dolthub/doltlite/commit/e437eb2aefa3f476616863e4be6174febe1a7ec9)
>
> Runner: ubuntu24 20260823.283.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/33304292018)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.62s | 10.68s | 1.1× | 1.2% | **PASS** |
| Writes | 1.96s | 3.09s | 1.6× | 1.1% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.25s | 10.84s | 1.1× | 1.2% | **PASS** |
| Writes | 3.33s | 3.84s | 1.2× | 1.6% | **PASS** |
| Autocommit writes | 993.61ms | 3.41s | 3.4× | 6.7% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.52s | 2.86s | 1.1× | 1.6% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.10s | 2.28s | 1.1× | 1.6% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.39s | 2.55s | 1.1× | 1.0% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.61s | 3.00s | 1.1× | 1.0% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 451.37ms | 692.41ms | 1.5× | 1.3% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 409.66ms | 654.88ms | 1.6× | 1.2% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 501.30ms | 822.77ms | 1.6× | 0.9% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 593.39ms | 921.24ms | 1.6× | 1.0% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.63s | 2.87s | 1.1× | 1.4% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.17s | 2.30s | 1.1× | 1.2% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.50s | 2.59s | 1.0× | 1.3% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.95s | 3.08s | 1.0× | 1.1% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 589.06ms | 738.26ms | 1.3× | 1.4% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.31s | 1.18s | 0.9× | 17.3% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 638.14ms | 902.74ms | 1.4× | 1.2% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 792.38ms | 1.02s | 1.3× | 1.5% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.47s | 2.87s | 1.2× | 1.1% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.13s | 2.31s | 1.1× | 1.1% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.38s | 2.56s | 1.1× | 1.2% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.65s | 3.05s | 1.2× | 1.2% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 197.35ms | 725.94ms | 3.7× | 6.6% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 424.02ms | 1.33s | 3.1× | 45.0% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 145.32ms | 549.99ms | 3.8× | 6.1% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 226.91ms | 796.91ms | 3.5× | 6.5% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.26ms | 28.64ms | 1.2× | 1.1% | PASS |
| mem_reads | `oltp_range_select` | 10.11ms | 12.03ms | 1.2× | 1.6% | PASS |
| mem_reads | `oltp_sum_range` | 9.70ms | 10.87ms | 1.1× | 1.8% | PASS |
| mem_reads | `oltp_order_range` | 2.66ms | 2.87ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_distinct_range` | 3.76ms | 3.91ms | 1.0× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 3.99ms | 5.26ms | 1.3× | 1.8% | PASS |
| mem_reads | `select_random_points` | 10.51ms | 10.77ms | 1.0× | 2.4% | PASS |
| mem_reads | `select_random_ranges` | 3.05ms | 3.87ms | 1.3× | 1.1% | PASS |
| mem_reads | `covering_index_scan` | 4.28ms | 4.24ms | 1.0× | 2.6% | PASS |
| mem_reads | `groupby_scan` | 30.93ms | 30.93ms | 1.0× | 0.9% | PASS |
| mem_reads | `index_join` | 6.04ms | 8.03ms | 1.3× | 1.6% | PASS |
| mem_reads | `index_join_scan` | 3.48ms | 4.29ms | 1.2× | 1.9% | PASS |
| mem_reads | `types_table_scan` | 1.07s | 1.26s | 1.2× | 1.6% | PASS |
| mem_reads | `table_scan` | 1.23s | 1.35s | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_read_only` | 107.27ms | 118.15ms | 1.1× | 1.2% | PASS |
| mem_writes | `oltp_bulk_insert` | 180.78ms | 244.24ms | 1.4× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 15.52ms | 27.17ms | 1.8× | 1.3% | PASS |
| mem_writes | `oltp_update_index` | 52.59ms | 93.78ms | 1.8× | 1.3% | PASS |
| mem_writes | `oltp_update_non_index` | 35.56ms | 55.31ms | 1.6× | 1.2% | PASS |
| mem_writes | `oltp_delete_insert` | 47.10ms | 74.05ms | 1.6× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 22.86ms | 47.04ms | 2.1× | 1.5% | PASS |
| mem_writes | `types_delete_insert` | 25.13ms | 37.11ms | 1.5× | 1.5% | PASS |
| mem_writes | `oltp_read_write` | 71.82ms | 113.70ms | 1.6× | 1.5% | PASS |
| file_reads | `oltp_point_select` | 95.43ms | 47.77ms | 0.5× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 18.22ms | 14.08ms | 0.8× | 1.4% | PASS |
| file_reads | `oltp_sum_range` | 17.55ms | 12.98ms | 0.7× | 1.9% | PASS |
| file_reads | `oltp_order_range` | 3.44ms | 3.09ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_distinct_range` | 4.56ms | 4.17ms | 0.9× | 0.9% | PASS |
| file_reads | `oltp_index_scan` | 11.46ms | 7.27ms | 0.6× | 1.6% | PASS |
| file_reads | `select_random_points` | 17.93ms | 12.66ms | 0.7× | 2.0% | PASS |
| file_reads | `select_random_ranges` | 10.10ms | 5.77ms | 0.6× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 11.71ms | 6.32ms | 0.5× | 1.6% | PASS |
| file_reads | `groupby_scan` | 31.84ms | 31.29ms | 1.0× | 1.1% | PASS |
| file_reads | `index_join` | 10.42ms | 9.49ms | 0.9× | 2.1% | PASS |
| file_reads | `index_join_scan` | 4.48ms | 4.66ms | 1.0× | 2.6% | PASS |
| file_reads | `types_table_scan` | 1.03s | 1.24s | 1.2× | 0.4% | PASS |
| file_reads | `table_scan` | 1.16s | 1.33s | 1.1× | 0.4% | PASS |
| file_reads | `oltp_read_only` | 206.24ms | 144.57ms | 0.7× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 193.26ms | 253.54ms | 1.3× | 1.1% | PASS |
| file_writes | `oltp_insert` | 22.11ms | 30.65ms | 1.4× | 2.3% | PASS |
| file_writes | `oltp_update_index` | 75.66ms | 97.94ms | 1.3× | 1.4% | PASS |
| file_writes | `oltp_update_non_index` | 56.39ms | 64.91ms | 1.2× | 1.5% | PASS |
| file_writes | `oltp_delete_insert` | 66.70ms | 79.41ms | 1.2× | 1.4% | PASS |
| file_writes | `oltp_write_only` | 44.29ms | 52.26ms | 1.2× | 1.7% | PASS |
| file_writes | `types_delete_insert` | 39.62ms | 42.19ms | 1.1× | 1.4% | PASS |
| file_writes | `oltp_read_write` | 91.02ms | 117.35ms | 1.3× | 1.6% | PASS |
| ac_reads | `oltp_point_select` | 46.64ms | 46.88ms | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_range_select` | 12.56ms | 13.91ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_sum_range` | 12.01ms | 12.80ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_order_range` | 2.89ms | 3.08ms | 1.1× | 1.6% | PASS |
| ac_reads | `oltp_distinct_range` | 3.99ms | 4.14ms | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_index_scan` | 6.54ms | 7.21ms | 1.1× | 1.5% | PASS |
| ac_reads | `select_random_points` | 12.39ms | 12.51ms | 1.0× | 1.8% | PASS |
| ac_reads | `select_random_ranges` | 5.35ms | 5.75ms | 1.1× | 1.1% | PASS |
| ac_reads | `covering_index_scan` | 6.73ms | 6.20ms | 0.9× | 1.1% | PASS |
| ac_reads | `groupby_scan` | 30.93ms | 31.20ms | 1.0× | 0.7% | PASS |
| ac_reads | `index_join` | 7.44ms | 9.01ms | 1.2× | 1.7% | PASS |
| ac_reads | `index_join_scan` | 3.77ms | 4.56ms | 1.2× | 1.4% | PASS |
| ac_reads | `types_table_scan` | 1.03s | 1.24s | 1.2× | 0.5% | PASS |
| ac_reads | `table_scan` | 1.15s | 1.33s | 1.2× | 0.5% | PASS |
| ac_reads | `oltp_read_only` | 136.41ms | 144.25ms | 1.1× | 0.7% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.88ms | 79.23ms | 3.6× | 7.5% | PASS |
| ac_writes | `oltp_insert_ac` | 23.73ms | 89.84ms | 3.8× | 6.2% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.28ms | 100.33ms | 3.7× | 6.0% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.06ms | 85.75ms | 3.7× | 7.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 23.75ms | 93.52ms | 3.9× | 6.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.72ms | 92.77ms | 3.6× | 6.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.52ms | 84.59ms | 3.8× | 7.3% | PASS |
| ac_writes | `oltp_read_write_ac` | 29.40ms | 99.90ms | 3.4× | 7.0% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 26.01ms | 24.65ms | 0.9× | 2.2% | PASS |
| mem_reads | `oltp_range_select` | 12.72ms | 10.52ms | 0.8× | 1.6% | PASS |
| mem_reads | `oltp_sum_range` | 12.34ms | 9.88ms | 0.8× | 1.4% | PASS |
| mem_reads | `oltp_order_range` | 2.65ms | 2.56ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 3.44ms | 3.33ms | 1.0× | 0.8% | PASS |
| mem_reads | `oltp_index_scan` | 2.92ms | 4.00ms | 1.4× | 2.0% | PASS |
| mem_reads | `select_random_points` | 16.41ms | 15.02ms | 0.9× | 2.2% | PASS |
| mem_reads | `select_random_ranges` | 2.49ms | 3.67ms | 1.5× | 2.5% | PASS |
| mem_reads | `covering_index_scan` | 2.94ms | 3.10ms | 1.1× | 2.7% | PASS |
| mem_reads | `groupby_scan` | 27.59ms | 26.35ms | 1.0× | 1.2% | PASS |
| mem_reads | `index_join` | 8.88ms | 6.67ms | 0.8× | 1.9% | PASS |
| mem_reads | `index_join_scan` | 2.77ms | 4.33ms | 1.6× | 2.6% | PASS |
| mem_reads | `types_table_scan` | 870.59ms | 987.02ms | 1.1× | 0.3% | PASS |
| mem_reads | `table_scan` | 1.00s | 1.08s | 1.1× | 0.2% | PASS |
| mem_reads | `oltp_read_only` | 103.47ms | 98.61ms | 1.0× | 0.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 161.49ms | 237.46ms | 1.5× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 12.88ms | 25.96ms | 2.0× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 48.73ms | 95.06ms | 2.0× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 33.20ms | 53.57ms | 1.6× | 1.6% | PASS |
| mem_writes | `oltp_delete_insert` | 37.85ms | 70.15ms | 1.9× | 2.2% | PASS |
| mem_writes | `oltp_write_only` | 19.27ms | 40.00ms | 2.1× | 3.1% | PASS |
| mem_writes | `types_delete_insert` | 28.72ms | 35.99ms | 1.3× | 0.7% | PASS |
| mem_writes | `oltp_read_write` | 67.51ms | 96.68ms | 1.4× | 1.3% | PASS |
| file_reads | `oltp_point_select` | 51.42ms | 30.53ms | 0.6× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 15.28ms | 11.35ms | 0.7× | 1.0% | PASS |
| file_reads | `oltp_sum_range` | 15.36ms | 10.93ms | 0.7× | 1.0% | PASS |
| file_reads | `oltp_order_range` | 2.99ms | 2.67ms | 0.9× | 1.9% | PASS |
| file_reads | `oltp_distinct_range` | 3.74ms | 3.42ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 5.80ms | 5.11ms | 0.9× | 3.3% | PASS |
| file_reads | `select_random_points` | 21.39ms | 17.10ms | 0.8× | 1.4% | PASS |
| file_reads | `select_random_ranges` | 5.32ms | 4.63ms | 0.9× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 5.62ms | 3.95ms | 0.7× | 2.8% | PASS |
| file_reads | `groupby_scan` | 28.63ms | 26.55ms | 0.9× | 1.2% | PASS |
| file_reads | `index_join` | 10.83ms | 7.44ms | 0.7× | 2.9% | PASS |
| file_reads | `index_join_scan` | 3.12ms | 4.56ms | 1.5× | 2.5% | PASS |
| file_reads | `types_table_scan` | 867.35ms | 986.30ms | 1.1× | 0.3% | PASS |
| file_reads | `table_scan` | 998.92ms | 1.08s | 1.1× | 0.3% | PASS |
| file_reads | `oltp_read_only` | 137.72ms | 106.26ms | 0.8× | 0.6% | PASS |
| file_writes | `oltp_bulk_insert` | 271.93ms | 315.59ms | 1.2× | 9.1% | PASS |
| file_writes | `oltp_insert` | 30.78ms | 61.45ms | 2.0× | 41.7% | PASS |
| file_writes | `oltp_update_index` | 214.61ms | 188.23ms | 0.9× | 20.8% | PASS |
| file_writes | `oltp_update_non_index` | 163.53ms | 120.24ms | 0.7× | 19.8% | PASS |
| file_writes | `oltp_delete_insert` | 174.29ms | 139.95ms | 0.8× | 4.9% | PASS |
| file_writes | `oltp_write_only` | 136.75ms | 100.87ms | 0.7× | 14.2% | PASS |
| file_writes | `types_delete_insert` | 143.86ms | 92.55ms | 0.6× | 20.4% | PASS |
| file_writes | `oltp_read_write` | 177.80ms | 160.61ms | 0.9× | 14.9% | PASS |
| ac_reads | `oltp_point_select` | 35.61ms | 30.99ms | 0.9× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 13.89ms | 11.34ms | 0.8× | 0.9% | PASS |
| ac_reads | `oltp_sum_range` | 13.63ms | 10.81ms | 0.8× | 1.5% | PASS |
| ac_reads | `oltp_order_range` | 2.83ms | 2.66ms | 0.9× | 1.2% | PASS |
| ac_reads | `oltp_distinct_range` | 3.66ms | 3.44ms | 0.9× | 0.9% | PASS |
| ac_reads | `oltp_index_scan` | 4.26ms | 5.05ms | 1.2× | 2.7% | PASS |
| ac_reads | `select_random_points` | 20.12ms | 17.41ms | 0.9× | 1.1% | PASS |
| ac_reads | `select_random_ranges` | 3.73ms | 4.63ms | 1.2× | 2.0% | PASS |
| ac_reads | `covering_index_scan` | 4.08ms | 4.25ms | 1.0× | 2.2% | PASS |
| ac_reads | `groupby_scan` | 29.12ms | 26.89ms | 0.9× | 0.5% | PASS |
| ac_reads | `index_join` | 10.54ms | 8.04ms | 0.8× | 3.0% | PASS |
| ac_reads | `index_join_scan` | 3.22ms | 4.82ms | 1.5× | 1.3% | PASS |
| ac_reads | `types_table_scan` | 868.86ms | 987.33ms | 1.1× | 0.3% | PASS |
| ac_reads | `table_scan` | 1.00s | 1.08s | 1.1× | 0.3% | PASS |
| ac_reads | `oltp_read_only` | 116.60ms | 107.93ms | 0.9× | 0.6% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 43.04ms | 130.12ms | 3.0× | 43.2% | PASS |
| ac_writes | `oltp_insert_ac` | 57.12ms | 185.05ms | 3.2× | 53.6% | PASS |
| ac_writes | `oltp_update_index_ac` | 50.23ms | 141.41ms | 2.8× | 22.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 60.93ms | 236.29ms | 3.9× | 56.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 44.09ms | 149.72ms | 3.4× | 33.2% | PASS |
| ac_writes | `oltp_write_only_ac` | 55.92ms | 181.72ms | 3.2× | 50.7% | PASS |
| ac_writes | `types_delete_insert_ac` | 56.37ms | 182.66ms | 3.2× | 46.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 56.32ms | 127.74ms | 2.3× | 32.7% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 30.69ms | 31.45ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_range_select` | 13.46ms | 11.86ms | 0.9× | 0.8% | PASS |
| mem_reads | `oltp_sum_range` | 13.15ms | 11.87ms | 0.9× | 1.0% | PASS |
| mem_reads | `oltp_order_range` | 2.82ms | 2.75ms | 1.0× | 1.0% | PASS |
| mem_reads | `oltp_distinct_range` | 3.58ms | 3.59ms | 1.0× | 0.7% | PASS |
| mem_reads | `oltp_index_scan` | 3.52ms | 5.11ms | 1.5× | 0.9% | PASS |
| mem_reads | `select_random_points` | 19.50ms | 19.19ms | 1.0× | 1.2% | PASS |
| mem_reads | `select_random_ranges` | 2.99ms | 4.59ms | 1.5× | 1.1% | PASS |
| mem_reads | `covering_index_scan` | 3.65ms | 4.06ms | 1.1× | 1.3% | PASS |
| mem_reads | `groupby_scan` | 30.81ms | 29.41ms | 1.0× | 0.4% | PASS |
| mem_reads | `index_join` | 9.58ms | 8.22ms | 0.9× | 2.0% | PASS |
| mem_reads | `index_join_scan` | 3.13ms | 4.88ms | 1.6× | 0.9% | PASS |
| mem_reads | `types_table_scan` | 980.13ms | 1.09s | 1.1× | 0.8% | PASS |
| mem_reads | `table_scan` | 1.16s | 1.20s | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_read_only` | 119.25ms | 117.96ms | 1.0× | 1.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 203.13ms | 304.45ms | 1.5× | 1.4% | PASS |
| mem_writes | `oltp_insert` | 15.96ms | 33.06ms | 2.1× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 57.27ms | 116.58ms | 2.0× | 0.9% | PASS |
| mem_writes | `oltp_update_non_index` | 40.95ms | 64.32ms | 1.6× | 0.9% | PASS |
| mem_writes | `oltp_delete_insert` | 45.48ms | 88.00ms | 1.9× | 0.8% | PASS |
| mem_writes | `oltp_write_only` | 23.45ms | 50.30ms | 2.1× | 0.7% | PASS |
| mem_writes | `types_delete_insert` | 32.65ms | 44.86ms | 1.4× | 1.2% | PASS |
| mem_writes | `oltp_read_write` | 82.41ms | 121.20ms | 1.5× | 1.0% | PASS |
| file_reads | `oltp_point_select` | 61.43ms | 40.70ms | 0.7× | 2.0% | PASS |
| file_reads | `oltp_range_select` | 16.60ms | 12.91ms | 0.8× | 1.3% | PASS |
| file_reads | `oltp_sum_range` | 16.46ms | 13.00ms | 0.8× | 1.3% | PASS |
| file_reads | `oltp_order_range` | 3.16ms | 2.91ms | 0.9× | 1.0% | PASS |
| file_reads | `oltp_distinct_range` | 3.96ms | 3.75ms | 0.9× | 1.0% | PASS |
| file_reads | `oltp_index_scan` | 6.63ms | 6.20ms | 0.9× | 1.4% | PASS |
| file_reads | `select_random_points` | 23.52ms | 20.69ms | 0.9× | 1.3% | PASS |
| file_reads | `select_random_ranges` | 6.08ms | 5.59ms | 0.9× | 1.3% | PASS |
| file_reads | `covering_index_scan` | 6.84ms | 5.16ms | 0.8× | 1.4% | PASS |
| file_reads | `groupby_scan` | 31.38ms | 29.69ms | 0.9× | 0.6% | PASS |
| file_reads | `index_join` | 11.71ms | 9.25ms | 0.8× | 1.5% | PASS |
| file_reads | `index_join_scan` | 3.60ms | 5.11ms | 1.4× | 1.0% | PASS |
| file_reads | `types_table_scan` | 986.90ms | 1.10s | 1.1× | 1.1% | PASS |
| file_reads | `table_scan` | 1.15s | 1.20s | 1.0× | 1.3% | PASS |
| file_reads | `oltp_read_only` | 165.73ms | 132.15ms | 0.8× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 218.96ms | 315.21ms | 1.4× | 1.2% | PASS |
| file_writes | `oltp_insert` | 20.46ms | 39.41ms | 1.9× | 1.9% | PASS |
| file_writes | `oltp_update_index` | 74.51ms | 129.85ms | 1.7× | 1.1% | PASS |
| file_writes | `oltp_update_non_index` | 74.89ms | 77.21ms | 1.0× | 11.8% | PASS |
| file_writes | `oltp_delete_insert` | 65.30ms | 100.50ms | 1.5× | 0.9% | PASS |
| file_writes | `oltp_write_only` | 39.22ms | 59.10ms | 1.5× | 2.2% | PASS |
| file_writes | `types_delete_insert` | 46.55ms | 52.45ms | 1.1× | 1.2% | PASS |
| file_writes | `oltp_read_write` | 98.25ms | 129.02ms | 1.3× | 1.1% | PASS |
| ac_reads | `oltp_point_select` | 40.06ms | 40.72ms | 1.0× | 2.3% | PASS |
| ac_reads | `oltp_range_select` | 14.57ms | 12.95ms | 0.9× | 1.2% | PASS |
| ac_reads | `oltp_sum_range` | 14.25ms | 13.04ms | 0.9× | 1.2% | PASS |
| ac_reads | `oltp_order_range` | 2.98ms | 2.91ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_distinct_range` | 3.75ms | 3.77ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_index_scan` | 4.66ms | 6.22ms | 1.3× | 1.7% | PASS |
| ac_reads | `select_random_points` | 21.15ms | 20.64ms | 1.0× | 1.1% | PASS |
| ac_reads | `select_random_ranges` | 4.07ms | 5.58ms | 1.4× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 4.75ms | 5.12ms | 1.1× | 1.3% | PASS |
| ac_reads | `groupby_scan` | 30.97ms | 29.63ms | 1.0× | 0.5% | PASS |
| ac_reads | `index_join` | 10.59ms | 9.12ms | 0.9× | 1.6% | PASS |
| ac_reads | `index_join_scan` | 3.43ms | 5.11ms | 1.5× | 1.5% | PASS |
| ac_reads | `types_table_scan` | 969.18ms | 1.08s | 1.1× | 1.0% | PASS |
| ac_reads | `table_scan` | 1.12s | 1.19s | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_read_only` | 133.04ms | 131.14ms | 1.0× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 17.07ms | 55.11ms | 3.2× | 5.8% | PASS |
| ac_writes | `oltp_insert_ac` | 18.28ms | 70.04ms | 3.8× | 7.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 18.75ms | 79.66ms | 4.2× | 6.5% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 16.39ms | 62.98ms | 3.8× | 6.4% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 19.02ms | 73.83ms | 3.9× | 4.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 17.62ms | 70.16ms | 4.0× | 6.7% | PASS |
| ac_writes | `types_delete_insert_ac` | 16.52ms | 62.80ms | 3.8× | 5.0% | PASS |
| ac_writes | `oltp_read_write_ac` | 21.67ms | 75.42ms | 3.5× | 4.4% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 35.30ms | 39.85ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_range_select` | 20.32ms | 21.73ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_sum_range` | 18.38ms | 20.27ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_order_range` | 3.58ms | 3.86ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_distinct_range` | 4.71ms | 4.90ms | 1.0× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 4.41ms | 5.78ms | 1.3× | 1.0% | PASS |
| mem_reads | `select_random_points` | 27.81ms | 31.95ms | 1.1× | 1.5% | PASS |
| mem_reads | `select_random_ranges` | 7.88ms | 8.75ms | 1.1× | 0.7% | PASS |
| mem_reads | `covering_index_scan` | 4.21ms | 4.10ms | 1.0× | 1.4% | PASS |
| mem_reads | `groupby_scan` | 37.19ms | 37.80ms | 1.0× | 0.7% | PASS |
| mem_reads | `index_join` | 8.09ms | 10.11ms | 1.2× | 0.9% | PASS |
| mem_reads | `index_join_scan` | 4.00ms | 5.35ms | 1.3× | 1.8% | PASS |
| mem_reads | `types_table_scan` | 1.04s | 1.27s | 1.2× | 0.8% | PASS |
| mem_reads | `table_scan` | 1.24s | 1.37s | 1.1× | 2.6% | PASS |
| mem_reads | `oltp_read_only` | 150.24ms | 166.94ms | 1.1× | 1.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 244.17ms | 339.70ms | 1.4× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 18.95ms | 34.34ms | 1.8× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 66.83ms | 115.85ms | 1.7× | 1.0% | PASS |
| mem_writes | `oltp_update_non_index` | 50.48ms | 74.39ms | 1.5× | 1.3% | PASS |
| mem_writes | `oltp_delete_insert` | 49.35ms | 93.13ms | 1.9× | 0.9% | PASS |
| mem_writes | `oltp_write_only` | 26.59ms | 55.21ms | 2.1× | 1.2% | PASS |
| mem_writes | `types_delete_insert` | 33.10ms | 51.86ms | 1.6× | 1.0% | PASS |
| mem_writes | `oltp_read_write` | 103.93ms | 156.77ms | 1.5× | 1.7% | PASS |
| file_reads | `oltp_point_select` | 103.36ms | 57.39ms | 0.6× | 0.7% | PASS |
| file_reads | `oltp_range_select` | 27.78ms | 23.71ms | 0.9× | 1.0% | PASS |
| file_reads | `oltp_sum_range` | 25.93ms | 22.57ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_order_range` | 4.47ms | 4.14ms | 0.9× | 1.0% | PASS |
| file_reads | `oltp_distinct_range` | 5.66ms | 5.19ms | 0.9× | 1.0% | PASS |
| file_reads | `oltp_index_scan` | 11.99ms | 8.00ms | 0.7× | 1.1% | PASS |
| file_reads | `select_random_points` | 37.69ms | 35.39ms | 0.9× | 1.6% | PASS |
| file_reads | `select_random_ranges` | 15.28ms | 10.91ms | 0.7× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 11.67ms | 6.41ms | 0.5× | 1.5% | PASS |
| file_reads | `groupby_scan` | 38.53ms | 38.01ms | 1.0× | 0.7% | PASS |
| file_reads | `index_join` | 12.54ms | 12.02ms | 1.0× | 1.2% | PASS |
| file_reads | `index_join_scan` | 5.31ms | 5.83ms | 1.1× | 2.1% | PASS |
| file_reads | `types_table_scan` | 1.05s | 1.27s | 1.2× | 0.9% | PASS |
| file_reads | `table_scan` | 1.33s | 1.38s | 1.0× | 2.2% | PASS |
| file_reads | `oltp_read_only` | 259.45ms | 198.34ms | 0.8× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 258.05ms | 348.08ms | 1.3× | 1.1% | PASS |
| file_writes | `oltp_insert` | 28.53ms | 40.20ms | 1.4× | 1.8% | PASS |
| file_writes | `oltp_update_index` | 97.32ms | 132.55ms | 1.4× | 1.3% | PASS |
| file_writes | `oltp_update_non_index` | 80.42ms | 89.77ms | 1.1× | 1.6% | PASS |
| file_writes | `oltp_delete_insert` | 80.84ms | 107.54ms | 1.3× | 1.4% | PASS |
| file_writes | `oltp_write_only` | 56.32ms | 66.84ms | 1.2× | 1.9% | PASS |
| file_writes | `types_delete_insert` | 51.24ms | 59.01ms | 1.2× | 1.3% | PASS |
| file_writes | `oltp_read_write` | 139.65ms | 171.91ms | 1.2× | 1.7% | PASS |
| ac_reads | `oltp_point_select` | 57.36ms | 58.16ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 22.92ms | 23.89ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_sum_range` | 21.24ms | 22.86ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_order_range` | 3.96ms | 4.15ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_distinct_range` | 5.11ms | 5.20ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_index_scan` | 7.36ms | 8.21ms | 1.1× | 1.5% | PASS |
| ac_reads | `select_random_points` | 32.09ms | 35.96ms | 1.1× | 1.2% | PASS |
| ac_reads | `select_random_ranges` | 10.48ms | 10.96ms | 1.0× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 7.07ms | 6.52ms | 0.9× | 1.2% | PASS |
| ac_reads | `groupby_scan` | 37.91ms | 38.19ms | 1.0× | 0.6% | PASS |
| ac_reads | `index_join` | 9.97ms | 12.24ms | 1.2× | 1.9% | PASS |
| ac_reads | `index_join_scan` | 4.64ms | 5.77ms | 1.2× | 2.5% | PASS |
| ac_reads | `types_table_scan` | 1.04s | 1.26s | 1.2× | 0.9% | PASS |
| ac_reads | `table_scan` | 1.21s | 1.36s | 1.1× | 1.9% | PASS |
| ac_reads | `oltp_read_only` | 186.75ms | 197.30ms | 1.1× | 1.4% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 25.51ms | 82.80ms | 3.2× | 6.6% | PASS |
| ac_writes | `oltp_insert_ac` | 28.36ms | 101.47ms | 3.6× | 5.7% | PASS |
| ac_writes | `oltp_update_index_ac` | 30.60ms | 109.71ms | 3.6× | 7.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 25.98ms | 91.84ms | 3.5× | 4.4% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 27.98ms | 104.27ms | 3.7× | 6.5% | PASS |
| ac_writes | `oltp_write_only_ac` | 28.09ms | 100.58ms | 3.6× | 5.5% | PASS |
| ac_writes | `types_delete_insert_ac` | 24.77ms | 92.67ms | 3.7× | 6.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 35.63ms | 113.56ms | 3.2× | 7.5% | PASS |

</details>

</details>

## Version-control latency

Wall time: 2m 12s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 92.81ms | 130.00ms | 71.4% | 0.4% | PASS |
| `status_dirty_many_tables` | 96.08ms | 130.00ms | 73.9% | 0.4% | PASS |
| `diff_regular_working_one_table` | 87.32ms | 120.00ms | 72.8% | 0.3% | PASS |
| `diff_regular_working_many_tables` | 101.02ms | 140.00ms | 72.2% | 0.4% | PASS |
| `diff_stat_working_many_tables` | 101.09ms | 140.00ms | 72.2% | 0.3% | PASS |
| `diff_schema_working_many_tables` | 101.25ms | 140.00ms | 72.3% | 0.2% | PASS |
| `branch_list_many_branches` | 25.43ms | 35.00ms | 72.7% | 0.6% | PASS |
| `branch_create_delete` | 27.96ms | 40.00ms | 69.9% | 0.5% | PASS |
| `checkout_branch_clean` | 61.27ms | 150.00ms | 40.8% | 0.2% | PASS |
| `merge_data_no_conflicts` | 32.55ms | 50.00ms | 65.1% | 0.4% | PASS |
| `merge_schema_no_conflicts` | 24.44ms | 35.00ms | 69.8% | 1.1% | PASS |
| `merge_data_conflicts` | 36.68ms | 180.00ms | 20.4% | 0.4% | PASS |
| `merge_data_conflicts_with_resolve` | 36.69ms | 180.00ms | 20.4% | 0.3% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
