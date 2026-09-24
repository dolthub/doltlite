# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-09-24 11:13 UTC
>
> Commit: [`1066294abad4c33a71ee6ba6615a6460fa32fec9`](https://github.com/dolthub/doltlite/commit/1066294abad4c33a71ee6ba6615a6460fa32fec9)
>
> Runner: ubuntu24 20260920.314.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/35982274769)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.87s | 11.29s | 1.1× | 1.3% | **PASS** |
| Writes | 2.11s | 3.38s | 1.6× | 1.2% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.98s | 11.59s | 1.1× | 1.2% | **PASS** |
| Writes | 3.78s | 4.32s | 1.1× | 1.9% | **PASS** |
| Autocommit writes | 1.13s | 3.62s | 3.2× | 7.0% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.45s | 2.85s | 1.2× | 1.4% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.22s | 2.42s | 1.1× | 1.5% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.59s | 2.98s | 1.1× | 1.5% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.61s | 3.05s | 1.2× | 1.2% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 443.46ms | 720.81ms | 1.6× | 1.1% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 481.37ms | 723.61ms | 1.5× | 2.4% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 607.24ms | 995.01ms | 1.6× | 2.0% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 578.34ms | 942.25ms | 1.6× | 1.0% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.70s | 2.91s | 1.1× | 1.2% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.61s | 2.53s | 1.0× | 1.3% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.82s | 3.04s | 1.1× | 1.7% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.85s | 3.11s | 1.1× | 1.0% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 589.75ms | 784.36ms | 1.3× | 1.5% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.61s | 1.40s | 0.9× | 27.1% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 835.29ms | 1.10s | 1.3× | 2.1% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 750.48ms | 1.04s | 1.4× | 1.5% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.49s | 2.90s | 1.2× | 1.1% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.39s | 2.52s | 1.1× | 1.0% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.70s | 3.06s | 1.1× | 1.9% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.69s | 3.11s | 1.2× | 1.1% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 199.36ms | 732.49ms | 3.7× | 6.3% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 495.72ms | 1.56s | 3.1× | 57.3% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 215.84ms | 645.89ms | 3.0× | 6.3% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 216.18ms | 682.36ms | 3.2× | 7.1% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 23.91ms | 30.45ms | 1.3× | 2.1% | PASS |
| mem_reads | `oltp_range_select` | 9.86ms | 11.38ms | 1.2× | 3.1% | PASS |
| mem_reads | `oltp_sum_range` | 9.13ms | 11.25ms | 1.2× | 1.6% | PASS |
| mem_reads | `oltp_order_range` | 2.55ms | 2.82ms | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_distinct_range` | 3.63ms | 3.91ms | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_index_scan` | 3.82ms | 5.16ms | 1.4× | 1.5% | PASS |
| mem_reads | `select_random_points` | 9.69ms | 11.02ms | 1.1× | 1.7% | PASS |
| mem_reads | `select_random_ranges` | 4.64ms | 5.13ms | 1.1× | 1.0% | PASS |
| mem_reads | `covering_index_scan` | 7.67ms | 9.89ms | 1.3× | 0.8% | PASS |
| mem_reads | `groupby_scan` | 29.05ms | 32.39ms | 1.1× | 0.7% | PASS |
| mem_reads | `index_join` | 5.74ms | 7.78ms | 1.4× | 1.2% | PASS |
| mem_reads | `index_join_scan` | 3.09ms | 4.56ms | 1.5× | 1.6% | PASS |
| mem_reads | `types_table_scan` | 1.05s | 1.26s | 1.2× | 1.2% | PASS |
| mem_reads | `table_scan` | 1.18s | 1.33s | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_read_only` | 100.89ms | 118.87ms | 1.2× | 1.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 179.56ms | 274.94ms | 1.5× | 1.1% | PASS |
| mem_writes | `oltp_insert` | 15.26ms | 29.01ms | 1.9× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 51.27ms | 95.55ms | 1.9× | 1.3% | PASS |
| mem_writes | `oltp_update_non_index` | 35.37ms | 57.50ms | 1.6× | 1.3% | PASS |
| mem_writes | `oltp_delete_insert` | 45.55ms | 75.18ms | 1.7× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 22.26ms | 44.33ms | 2.0× | 0.9% | PASS |
| mem_writes | `types_delete_insert` | 24.68ms | 38.42ms | 1.6× | 1.1% | PASS |
| mem_writes | `oltp_read_write` | 69.51ms | 105.89ms | 1.5× | 1.8% | PASS |
| file_reads | `oltp_point_select` | 93.23ms | 48.91ms | 0.5× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 17.53ms | 13.52ms | 0.8× | 1.2% | PASS |
| file_reads | `oltp_sum_range` | 16.62ms | 13.34ms | 0.8× | 1.0% | PASS |
| file_reads | `oltp_order_range` | 3.42ms | 3.12ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_distinct_range` | 4.52ms | 4.22ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_index_scan` | 11.20ms | 7.50ms | 0.7× | 1.8% | PASS |
| file_reads | `select_random_points` | 17.36ms | 13.18ms | 0.8× | 1.2% | PASS |
| file_reads | `select_random_ranges` | 11.80ms | 7.09ms | 0.6× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 15.17ms | 12.26ms | 0.8× | 1.4% | PASS |
| file_reads | `groupby_scan` | 30.01ms | 32.79ms | 1.1× | 0.7% | PASS |
| file_reads | `index_join` | 9.92ms | 9.56ms | 1.0× | 1.9% | PASS |
| file_reads | `index_join_scan` | 4.12ms | 5.09ms | 1.2× | 1.3% | PASS |
| file_reads | `types_table_scan` | 1.04s | 1.26s | 1.2× | 0.7% | PASS |
| file_reads | `table_scan` | 1.21s | 1.34s | 1.1× | 1.1% | PASS |
| file_reads | `oltp_read_only` | 206.60ms | 148.44ms | 0.7× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 194.78ms | 282.74ms | 1.5× | 1.0% | PASS |
| file_writes | `oltp_insert` | 21.99ms | 32.58ms | 1.5× | 1.5% | PASS |
| file_writes | `oltp_update_index` | 76.40ms | 104.81ms | 1.4× | 1.4% | PASS |
| file_writes | `oltp_update_non_index` | 57.25ms | 68.57ms | 1.2× | 2.0% | PASS |
| file_writes | `oltp_delete_insert` | 66.03ms | 83.97ms | 1.3× | 1.5% | PASS |
| file_writes | `oltp_write_only` | 43.72ms | 54.08ms | 1.2× | 1.9% | PASS |
| file_writes | `types_delete_insert` | 38.96ms | 43.36ms | 1.1× | 1.8% | PASS |
| file_writes | `oltp_read_write` | 90.64ms | 114.25ms | 1.3× | 1.3% | PASS |
| ac_reads | `oltp_point_select` | 46.12ms | 48.66ms | 1.1× | 0.8% | PASS |
| ac_reads | `oltp_range_select` | 12.44ms | 13.44ms | 1.1× | 0.8% | PASS |
| ac_reads | `oltp_sum_range` | 11.46ms | 13.34ms | 1.2× | 1.3% | PASS |
| ac_reads | `oltp_order_range` | 2.92ms | 3.09ms | 1.1× | 1.6% | PASS |
| ac_reads | `oltp_distinct_range` | 3.98ms | 4.21ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 6.58ms | 7.68ms | 1.2× | 1.4% | PASS |
| ac_reads | `select_random_points` | 12.95ms | 13.33ms | 1.0× | 1.1% | PASS |
| ac_reads | `select_random_ranges` | 7.18ms | 7.21ms | 1.0× | 1.1% | PASS |
| ac_reads | `covering_index_scan` | 10.40ms | 12.33ms | 1.2× | 1.0% | PASS |
| ac_reads | `groupby_scan` | 29.30ms | 32.76ms | 1.1× | 1.1% | PASS |
| ac_reads | `index_join` | 7.22ms | 9.47ms | 1.3× | 1.7% | PASS |
| ac_reads | `index_join_scan` | 3.53ms | 5.01ms | 1.4× | 1.2% | PASS |
| ac_reads | `types_table_scan` | 1.04s | 1.25s | 1.2× | 0.4% | PASS |
| ac_reads | `table_scan` | 1.17s | 1.33s | 1.1× | 0.4% | PASS |
| ac_reads | `oltp_read_only` | 132.57ms | 146.57ms | 1.1× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.58ms | 76.02ms | 3.4× | 5.3% | PASS |
| ac_writes | `oltp_insert_ac` | 25.00ms | 90.26ms | 3.6× | 4.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.51ms | 107.27ms | 4.0× | 7.0% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.82ms | 85.11ms | 3.7× | 8.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.88ms | 94.04ms | 3.8× | 6.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.26ms | 93.09ms | 3.7× | 6.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.23ms | 86.76ms | 3.9× | 6.1% | PASS |
| ac_writes | `oltp_read_write_ac` | 30.09ms | 99.94ms | 3.3× | 6.5% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 28.37ms | 28.08ms | 1.0× | 2.3% | PASS |
| mem_reads | `oltp_range_select` | 14.12ms | 11.78ms | 0.8× | 3.4% | PASS |
| mem_reads | `oltp_sum_range` | 12.38ms | 11.46ms | 0.9× | 2.7% | PASS |
| mem_reads | `oltp_order_range` | 2.90ms | 2.68ms | 0.9× | 1.0% | PASS |
| mem_reads | `oltp_distinct_range` | 3.69ms | 3.54ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 3.19ms | 4.70ms | 1.5× | 1.2% | PASS |
| mem_reads | `select_random_points` | 18.08ms | 16.70ms | 0.9× | 2.5% | PASS |
| mem_reads | `select_random_ranges` | 5.42ms | 4.78ms | 0.9× | 1.2% | PASS |
| mem_reads | `covering_index_scan` | 5.90ms | 7.53ms | 1.3× | 1.2% | PASS |
| mem_reads | `groupby_scan` | 28.52ms | 29.60ms | 1.0× | 1.0% | PASS |
| mem_reads | `index_join` | 8.71ms | 7.06ms | 0.8× | 1.7% | PASS |
| mem_reads | `index_join_scan` | 3.08ms | 4.95ms | 1.6× | 0.8% | PASS |
| mem_reads | `types_table_scan` | 921.17ms | 1.03s | 1.1× | 1.5% | PASS |
| mem_reads | `table_scan` | 1.05s | 1.15s | 1.1× | 2.1% | PASS |
| mem_reads | `oltp_read_only` | 118.42ms | 109.98ms | 0.9× | 2.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 185.97ms | 262.51ms | 1.4× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 14.33ms | 29.57ms | 2.1× | 1.2% | PASS |
| mem_writes | `oltp_update_index` | 56.08ms | 108.75ms | 1.9× | 2.5% | PASS |
| mem_writes | `oltp_update_non_index` | 42.48ms | 62.05ms | 1.5× | 4.1% | PASS |
| mem_writes | `oltp_delete_insert` | 45.32ms | 76.86ms | 1.7× | 2.3% | PASS |
| mem_writes | `oltp_write_only` | 23.44ms | 43.05ms | 1.8× | 2.3% | PASS |
| mem_writes | `types_delete_insert` | 33.32ms | 38.56ms | 1.2× | 4.0% | PASS |
| mem_writes | `oltp_read_write` | 80.44ms | 102.26ms | 1.3× | 3.0% | PASS |
| file_reads | `oltp_point_select` | 95.49ms | 45.82ms | 0.5× | 1.8% | PASS |
| file_reads | `oltp_range_select` | 21.70ms | 14.01ms | 0.6× | 1.6% | PASS |
| file_reads | `oltp_sum_range` | 20.14ms | 13.85ms | 0.7× | 1.3% | PASS |
| file_reads | `oltp_order_range` | 3.73ms | 3.00ms | 0.8× | 0.8% | PASS |
| file_reads | `oltp_distinct_range` | 4.48ms | 3.86ms | 0.9× | 0.9% | PASS |
| file_reads | `oltp_index_scan` | 10.09ms | 6.95ms | 0.7× | 0.7% | PASS |
| file_reads | `select_random_points` | 25.88ms | 18.94ms | 0.7× | 1.5% | PASS |
| file_reads | `select_random_ranges` | 12.46ms | 6.76ms | 0.5× | 0.9% | PASS |
| file_reads | `covering_index_scan` | 13.04ms | 9.82ms | 0.8× | 1.0% | PASS |
| file_reads | `groupby_scan` | 30.02ms | 30.45ms | 1.0× | 0.8% | PASS |
| file_reads | `index_join` | 13.16ms | 9.14ms | 0.7× | 1.2% | PASS |
| file_reads | `index_join_scan` | 3.98ms | 5.63ms | 1.4× | 1.5% | PASS |
| file_reads | `types_table_scan` | 972.40ms | 1.05s | 1.1× | 2.3% | PASS |
| file_reads | `table_scan` | 1.17s | 1.17s | 1.0× | 3.4% | PASS |
| file_reads | `oltp_read_only` | 220.38ms | 137.54ms | 0.6× | 2.0% | PASS |
| file_writes | `oltp_bulk_insert` | 301.87ms | 379.84ms | 1.3× | 24.2% | PASS |
| file_writes | `oltp_insert` | 56.65ms | 61.24ms | 1.1× | 51.1% | PASS |
| file_writes | `oltp_update_index` | 241.64ms | 220.51ms | 0.9× | 22.7% | PASS |
| file_writes | `oltp_update_non_index` | 208.50ms | 139.19ms | 0.7× | 31.9% | PASS |
| file_writes | `oltp_delete_insert` | 218.70ms | 185.18ms | 0.8× | 17.0% | PASS |
| file_writes | `oltp_write_only` | 171.41ms | 129.09ms | 0.8× | 40.5% | PASS |
| file_writes | `types_delete_insert` | 187.41ms | 99.68ms | 0.5× | 30.0% | PASS |
| file_writes | `oltp_read_write` | 222.35ms | 183.30ms | 0.8× | 23.9% | PASS |
| ac_reads | `oltp_point_select` | 52.12ms | 46.08ms | 0.9× | 1.5% | PASS |
| ac_reads | `oltp_range_select` | 17.58ms | 14.03ms | 0.8× | 1.2% | PASS |
| ac_reads | `oltp_sum_range` | 15.98ms | 13.98ms | 0.9× | 1.4% | PASS |
| ac_reads | `oltp_order_range` | 3.33ms | 3.01ms | 0.9× | 0.7% | PASS |
| ac_reads | `oltp_distinct_range` | 4.07ms | 3.86ms | 0.9× | 0.6% | PASS |
| ac_reads | `oltp_index_scan` | 5.85ms | 6.97ms | 1.2× | 1.0% | PASS |
| ac_reads | `select_random_points` | 22.55ms | 19.57ms | 0.9× | 1.6% | PASS |
| ac_reads | `select_random_ranges` | 8.22ms | 6.79ms | 0.8× | 0.8% | PASS |
| ac_reads | `covering_index_scan` | 8.56ms | 9.69ms | 1.1× | 0.7% | PASS |
| ac_reads | `groupby_scan` | 28.97ms | 29.96ms | 1.0× | 0.6% | PASS |
| ac_reads | `index_join` | 10.82ms | 8.95ms | 0.8× | 0.7% | PASS |
| ac_reads | `index_join_scan` | 3.60ms | 5.49ms | 1.5× | 1.0% | PASS |
| ac_reads | `types_table_scan` | 950.05ms | 1.05s | 1.1× | 3.0% | PASS |
| ac_reads | `table_scan` | 1.11s | 1.17s | 1.1× | 3.8% | PASS |
| ac_reads | `oltp_read_only` | 150.39ms | 135.51ms | 0.9× | 1.7% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 57.12ms | 132.40ms | 2.3× | 42.2% | PASS |
| ac_writes | `oltp_insert_ac` | 61.62ms | 184.84ms | 3.0× | 59.3% | PASS |
| ac_writes | `oltp_update_index_ac` | 53.86ms | 254.21ms | 4.7× | 55.4% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 74.64ms | 187.28ms | 2.5× | 59.2% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 73.63ms | 177.33ms | 2.4× | 42.5% | PASS |
| ac_writes | `oltp_write_only_ac` | 40.01ms | 160.37ms | 4.0× | 60.9% | PASS |
| ac_writes | `types_delete_insert_ac` | 44.71ms | 172.55ms | 3.9× | 49.0% | PASS |
| ac_writes | `oltp_read_write_ac` | 90.14ms | 289.76ms | 3.2× | 66.1% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.82ms | 38.86ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_range_select` | 15.55ms | 16.18ms | 1.0× | 1.9% | PASS |
| mem_reads | `oltp_sum_range` | 14.23ms | 14.60ms | 1.0× | 1.5% | PASS |
| mem_reads | `oltp_order_range` | 3.20ms | 3.27ms | 1.0× | 1.4% | PASS |
| mem_reads | `oltp_distinct_range` | 4.26ms | 4.38ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 3.94ms | 6.44ms | 1.6× | 1.5% | PASS |
| mem_reads | `select_random_points` | 20.68ms | 21.76ms | 1.1× | 2.0% | PASS |
| mem_reads | `select_random_ranges` | 6.49ms | 6.73ms | 1.0× | 2.3% | PASS |
| mem_reads | `covering_index_scan` | 7.76ms | 10.00ms | 1.3× | 1.2% | PASS |
| mem_reads | `groupby_scan` | 33.33ms | 34.42ms | 1.0× | 0.8% | PASS |
| mem_reads | `index_join` | 10.04ms | 9.55ms | 1.0× | 2.4% | PASS |
| mem_reads | `index_join_scan` | 3.63ms | 5.85ms | 1.6× | 1.8% | PASS |
| mem_reads | `types_table_scan` | 1.07s | 1.28s | 1.2× | 1.3% | PASS |
| mem_reads | `table_scan` | 1.22s | 1.38s | 1.1× | 2.0% | PASS |
| mem_reads | `oltp_read_only` | 135.88ms | 144.61ms | 1.1× | 1.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 243.61ms | 368.53ms | 1.5× | 1.2% | PASS |
| mem_writes | `oltp_insert` | 18.50ms | 39.94ms | 2.2× | 1.1% | PASS |
| mem_writes | `oltp_update_index` | 64.67ms | 136.75ms | 2.1× | 1.9% | PASS |
| mem_writes | `oltp_update_non_index` | 49.35ms | 82.72ms | 1.7× | 2.0% | PASS |
| mem_writes | `oltp_delete_insert` | 53.88ms | 103.93ms | 1.9× | 2.3% | PASS |
| mem_writes | `oltp_write_only` | 29.81ms | 61.22ms | 2.1× | 2.0% | PASS |
| mem_writes | `types_delete_insert` | 40.44ms | 55.64ms | 1.4× | 2.5% | PASS |
| mem_writes | `oltp_read_write` | 106.97ms | 146.28ms | 1.4× | 3.2% | PASS |
| file_reads | `oltp_point_select` | 111.65ms | 61.15ms | 0.5× | 1.4% | PASS |
| file_reads | `oltp_range_select` | 25.17ms | 19.22ms | 0.8× | 1.4% | PASS |
| file_reads | `oltp_sum_range` | 23.46ms | 17.46ms | 0.7× | 2.3% | PASS |
| file_reads | `oltp_order_range` | 4.39ms | 3.62ms | 0.8× | 1.8% | PASS |
| file_reads | `oltp_distinct_range` | 5.44ms | 4.75ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 11.39ms | 8.82ms | 0.8× | 1.8% | PASS |
| file_reads | `select_random_points` | 31.67ms | 25.53ms | 0.8× | 4.3% | PASS |
| file_reads | `select_random_ranges` | 14.23ms | 9.09ms | 0.6× | 1.7% | PASS |
| file_reads | `covering_index_scan` | 15.28ms | 12.72ms | 0.8× | 1.9% | PASS |
| file_reads | `groupby_scan` | 34.66ms | 35.55ms | 1.0× | 1.2% | PASS |
| file_reads | `index_join` | 14.15ms | 11.38ms | 0.8× | 1.9% | PASS |
| file_reads | `index_join_scan` | 4.60ms | 6.32ms | 1.4× | 2.8% | PASS |
| file_reads | `types_table_scan` | 1.05s | 1.27s | 1.2× | 0.9% | PASS |
| file_reads | `table_scan` | 1.22s | 1.38s | 1.1× | 1.4% | PASS |
| file_reads | `oltp_read_only` | 248.46ms | 176.37ms | 0.7× | 1.2% | PASS |
| file_writes | `oltp_bulk_insert` | 266.19ms | 381.11ms | 1.4× | 0.9% | PASS |
| file_writes | `oltp_insert` | 25.93ms | 46.44ms | 1.8× | 2.4% | PASS |
| file_writes | `oltp_update_index` | 96.68ms | 154.84ms | 1.6× | 1.6% | PASS |
| file_writes | `oltp_update_non_index` | 94.71ms | 97.58ms | 1.0× | 9.7% | PASS |
| file_writes | `oltp_delete_insert` | 87.30ms | 117.32ms | 1.3× | 2.2% | PASS |
| file_writes | `oltp_write_only` | 59.29ms | 72.84ms | 1.2× | 2.0% | PASS |
| file_writes | `types_delete_insert` | 65.24ms | 67.01ms | 1.0× | 1.6% | PASS |
| file_writes | `oltp_read_write` | 139.96ms | 160.78ms | 1.1× | 2.4% | PASS |
| ac_reads | `oltp_point_select` | 59.70ms | 58.75ms | 1.0× | 2.2% | PASS |
| ac_reads | `oltp_range_select` | 19.12ms | 18.63ms | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_sum_range` | 17.82ms | 17.32ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_order_range` | 4.03ms | 3.71ms | 0.9× | 2.5% | PASS |
| ac_reads | `oltp_distinct_range` | 4.93ms | 4.77ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 6.55ms | 8.85ms | 1.3× | 2.1% | PASS |
| ac_reads | `select_random_points` | 26.63ms | 25.88ms | 1.0× | 2.3% | PASS |
| ac_reads | `select_random_ranges` | 9.28ms | 9.07ms | 1.0× | 2.0% | PASS |
| ac_reads | `covering_index_scan` | 10.71ms | 12.75ms | 1.2× | 1.3% | PASS |
| ac_reads | `groupby_scan` | 34.77ms | 35.76ms | 1.0× | 1.1% | PASS |
| ac_reads | `index_join` | 12.42ms | 11.55ms | 0.9× | 2.9% | PASS |
| ac_reads | `index_join_scan` | 4.31ms | 6.45ms | 1.5× | 2.7% | PASS |
| ac_reads | `types_table_scan` | 1.07s | 1.28s | 1.2× | 1.2% | PASS |
| ac_reads | `table_scan` | 1.24s | 1.39s | 1.1× | 1.8% | PASS |
| ac_reads | `oltp_read_only` | 182.71ms | 179.51ms | 1.0× | 1.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 23.45ms | 63.12ms | 2.7× | 6.3% | PASS |
| ac_writes | `oltp_insert_ac` | 26.33ms | 81.81ms | 3.1× | 6.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 29.10ms | 95.61ms | 3.3× | 4.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 25.06ms | 76.35ms | 3.0× | 7.9% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.93ms | 83.00ms | 3.1× | 5.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.12ms | 81.32ms | 3.1× | 7.9% | PASS |
| ac_writes | `types_delete_insert_ac` | 24.91ms | 76.39ms | 3.1× | 6.3% | PASS |
| ac_writes | `oltp_read_write_ac` | 33.95ms | 88.29ms | 2.6× | 6.2% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 32.64ms | 41.84ms | 1.3× | 1.1% | PASS |
| mem_reads | `oltp_range_select` | 18.77ms | 23.98ms | 1.3× | 1.2% | PASS |
| mem_reads | `oltp_sum_range` | 17.22ms | 21.59ms | 1.3× | 0.8% | PASS |
| mem_reads | `oltp_order_range` | 3.46ms | 3.98ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_distinct_range` | 4.59ms | 5.15ms | 1.1× | 0.7% | PASS |
| mem_reads | `oltp_index_scan` | 4.44ms | 6.11ms | 1.4× | 1.3% | PASS |
| mem_reads | `select_random_points` | 27.34ms | 33.24ms | 1.2× | 1.2% | PASS |
| mem_reads | `select_random_ranges` | 7.57ms | 9.06ms | 1.2× | 1.5% | PASS |
| mem_reads | `covering_index_scan` | 7.67ms | 9.98ms | 1.3× | 1.2% | PASS |
| mem_reads | `groupby_scan` | 35.19ms | 39.76ms | 1.1× | 0.8% | PASS |
| mem_reads | `index_join` | 7.82ms | 10.46ms | 1.3× | 1.4% | PASS |
| mem_reads | `index_join_scan` | 3.79ms | 5.83ms | 1.5× | 2.1% | PASS |
| mem_reads | `types_table_scan` | 1.06s | 1.28s | 1.2× | 0.9% | PASS |
| mem_reads | `table_scan` | 1.23s | 1.38s | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_read_only` | 147.50ms | 180.32ms | 1.2× | 0.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 240.44ms | 344.49ms | 1.4× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 18.50ms | 35.50ms | 1.9× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 65.35ms | 124.98ms | 1.9× | 1.5% | PASS |
| mem_writes | `oltp_update_non_index` | 49.71ms | 81.20ms | 1.6× | 1.1% | PASS |
| mem_writes | `oltp_delete_insert` | 48.19ms | 94.37ms | 2.0× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 26.41ms | 54.15ms | 2.1× | 1.0% | PASS |
| mem_writes | `types_delete_insert` | 31.68ms | 52.34ms | 1.7× | 0.9% | PASS |
| mem_writes | `oltp_read_write` | 98.04ms | 155.22ms | 1.6× | 1.2% | PASS |
| file_reads | `oltp_point_select` | 102.70ms | 60.73ms | 0.6× | 0.7% | PASS |
| file_reads | `oltp_range_select` | 26.24ms | 26.24ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_sum_range` | 24.83ms | 23.99ms | 1.0× | 0.9% | PASS |
| file_reads | `oltp_order_range` | 4.39ms | 4.30ms | 1.0× | 1.0% | PASS |
| file_reads | `oltp_distinct_range` | 5.50ms | 5.49ms | 1.0× | 0.8% | PASS |
| file_reads | `oltp_index_scan` | 11.78ms | 8.38ms | 0.7× | 1.0% | PASS |
| file_reads | `select_random_points` | 36.80ms | 36.65ms | 1.0× | 1.0% | PASS |
| file_reads | `select_random_ranges` | 15.05ms | 11.32ms | 0.8× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 15.09ms | 12.25ms | 0.8× | 0.7% | PASS |
| file_reads | `groupby_scan` | 35.95ms | 40.26ms | 1.1× | 0.6% | PASS |
| file_reads | `index_join` | 12.05ms | 12.13ms | 1.0× | 1.5% | PASS |
| file_reads | `index_join_scan` | 4.73ms | 6.20ms | 1.3× | 1.5% | PASS |
| file_reads | `types_table_scan` | 1.06s | 1.28s | 1.2× | 0.6% | PASS |
| file_reads | `table_scan` | 1.25s | 1.37s | 1.1× | 1.3% | PASS |
| file_reads | `oltp_read_only` | 252.28ms | 210.07ms | 0.8× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 254.75ms | 356.59ms | 1.4× | 0.8% | PASS |
| file_writes | `oltp_insert` | 25.83ms | 41.52ms | 1.6× | 2.0% | PASS |
| file_writes | `oltp_update_index` | 94.74ms | 138.95ms | 1.5× | 1.4% | PASS |
| file_writes | `oltp_update_non_index` | 75.31ms | 96.58ms | 1.3× | 1.3% | PASS |
| file_writes | `oltp_delete_insert` | 75.38ms | 109.14ms | 1.4× | 2.2% | PASS |
| file_writes | `oltp_write_only` | 50.18ms | 67.24ms | 1.3× | 1.8% | PASS |
| file_writes | `types_delete_insert` | 49.00ms | 61.58ms | 1.3× | 1.7% | PASS |
| file_writes | `oltp_read_write` | 125.29ms | 168.72ms | 1.3× | 1.1% | PASS |
| ac_reads | `oltp_point_select` | 55.93ms | 60.71ms | 1.1× | 0.9% | PASS |
| ac_reads | `oltp_range_select` | 21.33ms | 26.23ms | 1.2× | 1.3% | PASS |
| ac_reads | `oltp_sum_range` | 19.91ms | 23.98ms | 1.2× | 0.9% | PASS |
| ac_reads | `oltp_order_range` | 3.85ms | 4.32ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_distinct_range` | 4.90ms | 5.46ms | 1.1× | 0.9% | PASS |
| ac_reads | `oltp_index_scan` | 7.05ms | 8.38ms | 1.2× | 0.9% | PASS |
| ac_reads | `select_random_points` | 30.74ms | 36.72ms | 1.2× | 1.1% | PASS |
| ac_reads | `select_random_ranges` | 10.15ms | 11.36ms | 1.1× | 1.4% | PASS |
| ac_reads | `covering_index_scan` | 10.29ms | 12.23ms | 1.2× | 1.2% | PASS |
| ac_reads | `groupby_scan` | 35.13ms | 40.39ms | 1.1× | 1.1% | PASS |
| ac_reads | `index_join` | 9.45ms | 12.23ms | 1.3× | 1.5% | PASS |
| ac_reads | `index_join_scan` | 4.25ms | 6.29ms | 1.5× | 1.8% | PASS |
| ac_reads | `types_table_scan` | 1.06s | 1.28s | 1.2× | 0.5% | PASS |
| ac_reads | `table_scan` | 1.23s | 1.37s | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_read_only` | 185.61ms | 211.86ms | 1.1× | 0.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 23.67ms | 67.08ms | 2.8× | 7.9% | PASS |
| ac_writes | `oltp_insert_ac` | 26.40ms | 87.28ms | 3.3× | 5.6% | PASS |
| ac_writes | `oltp_update_index_ac` | 29.82ms | 97.86ms | 3.3× | 6.9% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 25.46ms | 78.38ms | 3.1× | 7.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.82ms | 87.35ms | 3.4× | 6.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.20ms | 88.71ms | 3.4× | 7.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 24.48ms | 79.16ms | 3.2× | 10.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 34.33ms | 96.54ms | 2.8× | 6.1% | PASS |

</details>

</details>

## Version-control latency

Wall time: 3m 24s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 34.31ms | 130.00ms | 26.4% | 0.7% | PASS |
| `status_dirty_many_tables` | 37.85ms | 130.00ms | 29.1% | 0.6% | PASS |
| `diff_regular_working_one_table` | 29.65ms | 120.00ms | 24.7% | 0.8% | PASS |
| `diff_regular_working_many_tables` | 42.46ms | 140.00ms | 30.3% | 0.5% | PASS |
| `diff_stat_working_many_tables` | 42.33ms | 140.00ms | 30.2% | 0.5% | PASS |
| `diff_schema_working_many_tables` | 42.80ms | 140.00ms | 30.6% | 0.6% | PASS |
| `branch_list_many_branches` | 21.90ms | 35.00ms | 62.6% | 0.9% | PASS |
| `branch_create_delete` | 24.00ms | 40.00ms | 60.0% | 1.2% | PASS |
| `at_literal_deep_history` | 24.51ms | 100.00ms | 24.5% | 0.7% | PASS |
| `diff_literal_deep_history` | 24.70ms | 120.00ms | 20.6% | 0.8% | PASS |
| `history_literal_deep_history` | 25.65ms | 150.00ms | 17.1% | 0.6% | PASS |
| `checkout_branch_clean` | 37.15ms | 150.00ms | 24.8% | 1.7% | PASS |
| `merge_data_no_conflicts` | 27.89ms | 50.00ms | 55.8% | 0.6% | PASS |
| `merge_data_secondary_index` | 869.07ms | 2.50s | 34.8% | 1.0% | PASS |
| `merge_schema_no_conflicts` | 21.59ms | 35.00ms | 61.7% | 1.2% | PASS |
| `merge_data_conflicts` | 29.75ms | 180.00ms | 16.5% | 0.5% | PASS |
| `merge_data_conflicts_with_resolve` | 30.54ms | 180.00ms | 17.0% | 0.6% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
