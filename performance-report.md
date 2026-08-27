# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-08-27 14:35 UTC
>
> Commit: [`4b53bcc2f56a20977db53d3105b10c266f966e75`](https://github.com/dolthub/doltlite/commit/4b53bcc2f56a20977db53d3105b10c266f966e75)
>
> Runner: ubuntu24 20260823.283.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/33074507648)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.52s | 9.83s | 1.0× | 1.9% | **PASS** |
| Writes | 1.91s | 3.01s | 1.6× | 1.7% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.15s | 10.01s | 1.0× | 1.9% | **PASS** |
| Writes | 4.02s | 4.12s | 1.0× | 8.7% | **PASS** |
| Autocommit writes | 2.66s | 8.26s | 3.1× | 14.4% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 1.71s | 1.75s | 1.0× | 1.7% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.81s | 2.86s | 1.0× | 2.0% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.12s | 2.28s | 1.1× | 2.8% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.89s | 2.94s | 1.0× | 1.2% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 260.70ms | 387.87ms | 1.5× | 1.7% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 609.23ms | 1.00s | 1.6× | 1.9% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 421.22ms | 665.52ms | 1.6× | 2.0% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 617.56ms | 960.09ms | 1.6× | 1.2% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 1.78s | 1.77s | 1.0× | 2.3% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.94s | 2.89s | 1.0× | 1.4% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.21s | 2.31s | 1.0× | 3.2% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 3.21s | 3.04s | 0.9× | 1.1% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 1.21s | 866.41ms | 0.7× | 36.0% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 990.73ms | 1.14s | 1.1× | 3.8% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 1.06s | 1.11s | 1.0× | 13.9% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 766.34ms | 1.02s | 1.3× | 1.5% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 1.73s | 1.78s | 1.0× | 1.8% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 3.11s | 2.98s | 1.0× | 1.9% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.15s | 2.30s | 1.1× | 2.8% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.93s | 3.02s | 1.0× | 1.3% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 1.87s | 5.55s | 3.0× | 52.8% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 215.24ms | 756.75ms | 3.5× | 6.3% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 414.29ms | 1.34s | 3.2× | 38.4% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 159.25ms | 612.08ms | 3.8× | 6.1% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 15.06ms | 16.29ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_range_select` | 7.39ms | 7.38ms | 1.0× | 2.0% | PASS |
| mem_reads | `oltp_sum_range` | 6.65ms | 6.99ms | 1.1× | 3.1% | PASS |
| mem_reads | `oltp_order_range` | 1.81ms | 1.85ms | 1.0× | 1.7% | PASS |
| mem_reads | `oltp_distinct_range` | 2.35ms | 2.47ms | 1.0× | 1.4% | PASS |
| mem_reads | `oltp_index_scan` | 2.37ms | 2.90ms | 1.2× | 2.4% | PASS |
| mem_reads | `select_random_points` | 7.31ms | 7.54ms | 1.0× | 6.7% | PASS |
| mem_reads | `select_random_ranges` | 1.86ms | 2.28ms | 1.2× | 4.5% | PASS |
| mem_reads | `covering_index_scan` | 2.44ms | 2.44ms | 1.0× | 1.0% | PASS |
| mem_reads | `groupby_scan` | 19.64ms | 20.95ms | 1.1× | 0.7% | PASS |
| mem_reads | `index_join` | 4.02ms | 5.42ms | 1.3× | 2.0% | PASS |
| mem_reads | `index_join_scan` | 2.14ms | 3.11ms | 1.5× | 5.3% | PASS |
| mem_reads | `types_table_scan` | 732.95ms | 755.81ms | 1.0× | 0.7% | PASS |
| mem_reads | `table_scan` | 836.15ms | 842.75ms | 1.0× | 0.9% | PASS |
| mem_reads | `oltp_read_only` | 64.29ms | 70.44ms | 1.1× | 1.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 100.20ms | 131.91ms | 1.3× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 9.05ms | 15.10ms | 1.7× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 30.68ms | 53.66ms | 1.7× | 1.7% | PASS |
| mem_writes | `oltp_update_non_index` | 20.53ms | 31.06ms | 1.5× | 2.3% | PASS |
| mem_writes | `oltp_delete_insert` | 29.66ms | 42.97ms | 1.4× | 2.1% | PASS |
| mem_writes | `oltp_write_only` | 14.71ms | 27.38ms | 1.9× | 1.8% | PASS |
| mem_writes | `types_delete_insert` | 15.40ms | 21.06ms | 1.4× | 1.3% | PASS |
| mem_writes | `oltp_read_write` | 40.46ms | 64.73ms | 1.6× | 1.6% | PASS |
| file_reads | `oltp_point_select` | 35.59ms | 21.14ms | 0.6× | 1.9% | PASS |
| file_reads | `oltp_range_select` | 9.99ms | 8.18ms | 0.8× | 2.4% | PASS |
| file_reads | `oltp_sum_range` | 9.46ms | 8.14ms | 0.9× | 2.3% | PASS |
| file_reads | `oltp_order_range` | 2.09ms | 2.00ms | 1.0× | 2.4% | PASS |
| file_reads | `oltp_distinct_range` | 2.75ms | 2.66ms | 1.0× | 1.9% | PASS |
| file_reads | `oltp_index_scan` | 4.95ms | 4.05ms | 0.8× | 3.9% | PASS |
| file_reads | `select_random_points` | 11.03ms | 9.58ms | 0.9× | 3.0% | PASS |
| file_reads | `select_random_ranges` | 4.23ms | 3.05ms | 0.7× | 2.3% | PASS |
| file_reads | `covering_index_scan` | 4.79ms | 3.37ms | 0.7× | 4.2% | PASS |
| file_reads | `groupby_scan` | 20.06ms | 21.27ms | 1.1× | 0.7% | PASS |
| file_reads | `index_join` | 5.31ms | 5.95ms | 1.1× | 2.0% | PASS |
| file_reads | `index_join_scan` | 2.58ms | 3.55ms | 1.4× | 4.3% | PASS |
| file_reads | `types_table_scan` | 732.03ms | 755.71ms | 1.0× | 0.5% | PASS |
| file_reads | `table_scan` | 839.88ms | 843.58ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_read_only` | 93.06ms | 78.16ms | 0.8× | 0.7% | PASS |
| file_writes | `oltp_bulk_insert` | 249.36ms | 210.59ms | 0.8× | 36.2% | PASS |
| file_writes | `oltp_insert` | 24.59ms | 52.77ms | 2.1× | 60.6% | PASS |
| file_writes | `oltp_update_index` | 182.82ms | 119.64ms | 0.7× | 31.5% | PASS |
| file_writes | `oltp_update_non_index` | 198.84ms | 127.96ms | 0.6× | 35.8% | PASS |
| file_writes | `oltp_delete_insert` | 151.03ms | 100.65ms | 0.7× | 23.4% | PASS |
| file_writes | `oltp_write_only` | 141.97ms | 64.63ms | 0.5× | 40.8% | PASS |
| file_writes | `types_delete_insert` | 90.53ms | 70.49ms | 0.8× | 59.1% | PASS |
| file_writes | `oltp_read_write` | 168.70ms | 119.68ms | 0.7× | 33.9% | PASS |
| ac_reads | `oltp_point_select` | 22.12ms | 21.72ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_range_select` | 8.97ms | 8.34ms | 0.9× | 1.5% | PASS |
| ac_reads | `oltp_sum_range` | 8.28ms | 8.24ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_order_range` | 1.98ms | 2.07ms | 1.0× | 2.0% | PASS |
| ac_reads | `oltp_distinct_range` | 2.66ms | 2.70ms | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_index_scan` | 3.74ms | 4.16ms | 1.1× | 2.6% | PASS |
| ac_reads | `select_random_points` | 9.99ms | 9.85ms | 1.0× | 2.6% | PASS |
| ac_reads | `select_random_ranges` | 3.02ms | 3.09ms | 1.0× | 2.3% | PASS |
| ac_reads | `covering_index_scan` | 3.47ms | 3.39ms | 1.0× | 2.1% | PASS |
| ac_reads | `groupby_scan` | 19.95ms | 21.30ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 4.86ms | 6.49ms | 1.3× | 2.9% | PASS |
| ac_reads | `index_join_scan` | 2.54ms | 3.71ms | 1.5× | 4.7% | PASS |
| ac_reads | `types_table_scan` | 732.22ms | 757.90ms | 1.0× | 0.5% | PASS |
| ac_reads | `table_scan` | 835.63ms | 843.91ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_read_only` | 74.83ms | 78.37ms | 1.0× | 1.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 270.14ms | 855.33ms | 3.2× | 56.6% | PASS |
| ac_writes | `oltp_insert_ac` | 357.86ms | 1.30s | 3.6× | 51.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 253.37ms | 975.80ms | 3.9× | 50.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 210.99ms | 544.46ms | 2.6× | 54.2% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 175.72ms | 396.69ms | 2.3× | 66.7% | PASS |
| ac_writes | `oltp_write_only_ac` | 95.21ms | 324.31ms | 3.4× | 49.0% | PASS |
| ac_writes | `types_delete_insert_ac` | 418.54ms | 903.17ms | 2.2× | 70.2% | PASS |
| ac_writes | `oltp_read_write_ac` | 90.46ms | 243.65ms | 2.7× | 25.5% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 39.08ms | 38.06ms | 1.0× | 3.7% | PASS |
| mem_reads | `oltp_range_select` | 18.28ms | 13.84ms | 0.8× | 2.1% | PASS |
| mem_reads | `oltp_sum_range` | 17.11ms | 14.44ms | 0.8× | 1.7% | PASS |
| mem_reads | `oltp_order_range` | 3.43ms | 3.12ms | 0.9× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 4.52ms | 4.21ms | 0.9× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 4.24ms | 6.32ms | 1.5× | 2.0% | PASS |
| mem_reads | `select_random_points` | 24.21ms | 21.50ms | 0.9× | 2.6% | PASS |
| mem_reads | `select_random_ranges` | 3.67ms | 5.27ms | 1.4× | 1.5% | PASS |
| mem_reads | `covering_index_scan` | 4.32ms | 4.71ms | 1.1× | 1.1% | PASS |
| mem_reads | `groupby_scan` | 35.96ms | 34.13ms | 0.9× | 0.8% | PASS |
| mem_reads | `index_join` | 11.41ms | 9.72ms | 0.9× | 2.4% | PASS |
| mem_reads | `index_join_scan` | 4.27ms | 5.50ms | 1.3× | 2.5% | PASS |
| mem_reads | `types_table_scan` | 1.16s | 1.22s | 1.1× | 2.3% | PASS |
| mem_reads | `table_scan` | 1.33s | 1.34s | 1.0× | 3.1% | PASS |
| mem_reads | `oltp_read_only` | 148.37ms | 137.65ms | 0.9× | 1.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 239.50ms | 368.61ms | 1.5× | 1.2% | PASS |
| mem_writes | `oltp_insert` | 18.41ms | 39.22ms | 2.1× | 1.7% | PASS |
| mem_writes | `oltp_update_index` | 71.31ms | 143.72ms | 2.0× | 2.0% | PASS |
| mem_writes | `oltp_update_non_index` | 50.38ms | 80.41ms | 1.6× | 2.0% | PASS |
| mem_writes | `oltp_delete_insert` | 55.72ms | 105.35ms | 1.9× | 1.9% | PASS |
| mem_writes | `oltp_write_only` | 28.87ms | 60.86ms | 2.1× | 1.8% | PASS |
| mem_writes | `types_delete_insert` | 41.14ms | 54.91ms | 1.3× | 2.1% | PASS |
| mem_writes | `oltp_read_write` | 103.90ms | 147.52ms | 1.4× | 1.7% | PASS |
| file_reads | `oltp_point_select` | 107.38ms | 56.69ms | 0.5× | 1.1% | PASS |
| file_reads | `oltp_range_select` | 24.88ms | 15.74ms | 0.6× | 2.4% | PASS |
| file_reads | `oltp_sum_range` | 23.82ms | 16.33ms | 0.7× | 1.3% | PASS |
| file_reads | `oltp_order_range` | 4.21ms | 3.42ms | 0.8× | 1.4% | PASS |
| file_reads | `oltp_distinct_range` | 5.46ms | 4.53ms | 0.8× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 11.45ms | 8.35ms | 0.7× | 1.1% | PASS |
| file_reads | `select_random_points` | 32.09ms | 24.25ms | 0.8× | 2.6% | PASS |
| file_reads | `select_random_ranges` | 10.90ms | 7.25ms | 0.7× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 11.87ms | 6.78ms | 0.6× | 2.3% | PASS |
| file_reads | `groupby_scan` | 36.60ms | 34.44ms | 0.9× | 0.8% | PASS |
| file_reads | `index_join` | 15.46ms | 11.08ms | 0.7× | 2.2% | PASS |
| file_reads | `index_join_scan` | 5.11ms | 5.96ms | 1.2× | 2.9% | PASS |
| file_reads | `types_table_scan` | 1.08s | 1.20s | 1.1× | 1.3% | PASS |
| file_reads | `table_scan` | 1.33s | 1.34s | 1.0× | 3.2% | PASS |
| file_reads | `oltp_read_only` | 251.11ms | 165.58ms | 0.7× | 1.5% | PASS |
| file_writes | `oltp_bulk_insert` | 265.70ms | 385.40ms | 1.5× | 0.9% | PASS |
| file_writes | `oltp_insert` | 27.41ms | 46.90ms | 1.7× | 2.5% | PASS |
| file_writes | `oltp_update_index` | 141.35ms | 165.38ms | 1.2× | 6.7% | PASS |
| file_writes | `oltp_update_non_index` | 106.71ms | 98.95ms | 0.9× | 13.7% | PASS |
| file_writes | `oltp_delete_insert` | 101.49ms | 125.63ms | 1.2× | 1.9% | PASS |
| file_writes | `oltp_write_only` | 90.71ms | 75.77ms | 0.8× | 8.5% | PASS |
| file_writes | `types_delete_insert` | 76.50ms | 69.37ms | 0.9× | 2.5% | PASS |
| file_writes | `oltp_read_write` | 180.84ms | 167.92ms | 0.9× | 5.0% | PASS |
| ac_reads | `oltp_point_select` | 64.70ms | 58.49ms | 0.9× | 1.5% | PASS |
| ac_reads | `oltp_range_select` | 21.59ms | 16.34ms | 0.8× | 2.0% | PASS |
| ac_reads | `oltp_sum_range` | 20.19ms | 16.83ms | 0.8× | 2.3% | PASS |
| ac_reads | `oltp_order_range` | 3.95ms | 3.49ms | 0.9× | 2.2% | PASS |
| ac_reads | `oltp_distinct_range` | 5.01ms | 4.56ms | 0.9× | 1.9% | PASS |
| ac_reads | `oltp_index_scan` | 7.20ms | 8.58ms | 1.2× | 1.8% | PASS |
| ac_reads | `select_random_points` | 29.22ms | 25.51ms | 0.9× | 2.8% | PASS |
| ac_reads | `select_random_ranges` | 6.40ms | 7.35ms | 1.1× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 7.46ms | 6.92ms | 0.9× | 2.2% | PASS |
| ac_reads | `groupby_scan` | 36.70ms | 34.91ms | 1.0× | 1.0% | PASS |
| ac_reads | `index_join` | 14.48ms | 11.66ms | 0.8× | 3.5% | PASS |
| ac_reads | `index_join_scan` | 4.92ms | 6.05ms | 1.2× | 3.1% | PASS |
| ac_reads | `types_table_scan` | 1.25s | 1.24s | 1.0× | 0.8% | PASS |
| ac_reads | `table_scan` | 1.44s | 1.37s | 0.9× | 0.7% | PASS |
| ac_reads | `oltp_read_only` | 190.43ms | 172.33ms | 0.9× | 1.4% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 24.61ms | 79.04ms | 3.2× | 6.4% | PASS |
| ac_writes | `oltp_insert_ac` | 27.61ms | 90.10ms | 3.3× | 7.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 28.32ms | 105.35ms | 3.7× | 5.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.25ms | 85.74ms | 3.7× | 6.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.34ms | 97.07ms | 3.7× | 5.8% | PASS |
| ac_writes | `oltp_write_only_ac` | 27.14ms | 99.59ms | 3.7× | 7.4% | PASS |
| ac_writes | `types_delete_insert_ac` | 24.04ms | 90.74ms | 3.8× | 6.1% | PASS |
| ac_writes | `oltp_read_write_ac` | 33.92ms | 109.11ms | 3.2× | 4.3% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 26.65ms | 25.52ms | 1.0× | 3.7% | PASS |
| mem_reads | `oltp_range_select` | 11.28ms | 10.20ms | 0.9× | 4.5% | PASS |
| mem_reads | `oltp_sum_range` | 11.28ms | 9.74ms | 0.9× | 3.9% | PASS |
| mem_reads | `oltp_order_range` | 2.53ms | 2.48ms | 1.0× | 3.8% | PASS |
| mem_reads | `oltp_distinct_range` | 3.29ms | 3.33ms | 1.0× | 3.1% | PASS |
| mem_reads | `oltp_index_scan` | 3.11ms | 4.12ms | 1.3× | 2.8% | PASS |
| mem_reads | `select_random_points` | 17.52ms | 16.00ms | 0.9× | 2.3% | PASS |
| mem_reads | `select_random_ranges` | 2.54ms | 4.00ms | 1.6× | 2.5% | PASS |
| mem_reads | `covering_index_scan` | 3.03ms | 3.30ms | 1.1× | 5.3% | PASS |
| mem_reads | `groupby_scan` | 28.17ms | 27.54ms | 1.0× | 1.8% | PASS |
| mem_reads | `index_join` | 9.30ms | 7.00ms | 0.8× | 2.8% | PASS |
| mem_reads | `index_join_scan` | 2.90ms | 4.51ms | 1.6× | 3.2% | PASS |
| mem_reads | `types_table_scan` | 886.87ms | 984.66ms | 1.1× | 0.7% | PASS |
| mem_reads | `table_scan` | 1.01s | 1.07s | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_read_only` | 103.80ms | 100.58ms | 1.0× | 1.7% | PASS |
| mem_writes | `oltp_bulk_insert` | 167.63ms | 242.25ms | 1.4× | 2.1% | PASS |
| mem_writes | `oltp_insert` | 13.43ms | 26.45ms | 2.0× | 1.9% | PASS |
| mem_writes | `oltp_update_index` | 49.03ms | 95.65ms | 2.0× | 1.9% | PASS |
| mem_writes | `oltp_update_non_index` | 35.21ms | 54.30ms | 1.5× | 1.9% | PASS |
| mem_writes | `oltp_delete_insert` | 39.22ms | 71.45ms | 1.8× | 2.7% | PASS |
| mem_writes | `oltp_write_only` | 20.16ms | 40.93ms | 2.0× | 2.0% | PASS |
| mem_writes | `types_delete_insert` | 27.90ms | 36.03ms | 1.3× | 2.5% | PASS |
| mem_writes | `oltp_read_write` | 68.64ms | 98.45ms | 1.4× | 1.8% | PASS |
| file_reads | `oltp_point_select` | 52.66ms | 32.04ms | 0.6× | 2.6% | PASS |
| file_reads | `oltp_range_select` | 14.82ms | 11.29ms | 0.8× | 3.2% | PASS |
| file_reads | `oltp_sum_range` | 14.45ms | 10.94ms | 0.8× | 2.5% | PASS |
| file_reads | `oltp_order_range` | 3.00ms | 2.67ms | 0.9× | 3.9% | PASS |
| file_reads | `oltp_distinct_range` | 3.81ms | 3.55ms | 0.9× | 3.0% | PASS |
| file_reads | `oltp_index_scan` | 6.01ms | 5.36ms | 0.9× | 4.3% | PASS |
| file_reads | `select_random_points` | 21.65ms | 17.78ms | 0.8× | 2.9% | PASS |
| file_reads | `select_random_ranges` | 5.31ms | 4.82ms | 0.9× | 4.7% | PASS |
| file_reads | `covering_index_scan` | 5.91ms | 4.26ms | 0.7× | 5.8% | PASS |
| file_reads | `groupby_scan` | 28.89ms | 28.60ms | 1.0× | 3.7% | PASS |
| file_reads | `index_join` | 11.18ms | 8.15ms | 0.7× | 3.4% | PASS |
| file_reads | `index_join_scan` | 3.31ms | 4.92ms | 1.5× | 4.2% | PASS |
| file_reads | `types_table_scan` | 888.54ms | 984.34ms | 1.1× | 0.9% | PASS |
| file_reads | `table_scan` | 1.01s | 1.08s | 1.1× | 0.8% | PASS |
| file_reads | `oltp_read_only` | 141.06ms | 110.43ms | 0.8× | 1.8% | PASS |
| file_writes | `oltp_bulk_insert` | 233.48ms | 303.19ms | 1.3× | 7.2% | PASS |
| file_writes | `oltp_insert` | 28.71ms | 49.30ms | 1.7× | 48.5% | PASS |
| file_writes | `oltp_update_index` | 151.09ms | 189.03ms | 1.3× | 10.4% | PASS |
| file_writes | `oltp_update_non_index` | 121.47ms | 112.34ms | 0.9× | 11.9% | PASS |
| file_writes | `oltp_delete_insert` | 149.60ms | 130.49ms | 0.9× | 9.0% | PASS |
| file_writes | `oltp_write_only` | 100.24ms | 90.90ms | 0.9× | 24.5% | PASS |
| file_writes | `types_delete_insert` | 110.86ms | 80.66ms | 0.7× | 25.8% | PASS |
| file_writes | `oltp_read_write` | 159.81ms | 149.98ms | 0.9× | 15.9% | PASS |
| ac_reads | `oltp_point_select` | 35.61ms | 32.05ms | 0.9× | 2.8% | PASS |
| ac_reads | `oltp_range_select` | 13.16ms | 11.27ms | 0.9× | 3.4% | PASS |
| ac_reads | `oltp_sum_range` | 12.70ms | 10.90ms | 0.9× | 3.0% | PASS |
| ac_reads | `oltp_order_range` | 2.84ms | 2.71ms | 1.0× | 3.4% | PASS |
| ac_reads | `oltp_distinct_range` | 3.63ms | 3.56ms | 1.0× | 2.4% | PASS |
| ac_reads | `oltp_index_scan` | 4.32ms | 5.28ms | 1.2× | 3.2% | PASS |
| ac_reads | `select_random_points` | 19.79ms | 17.33ms | 0.9× | 3.9% | PASS |
| ac_reads | `select_random_ranges` | 3.56ms | 4.69ms | 1.3× | 2.5% | PASS |
| ac_reads | `covering_index_scan` | 4.09ms | 4.30ms | 1.1× | 6.8% | PASS |
| ac_reads | `groupby_scan` | 28.62ms | 27.91ms | 1.0× | 2.2% | PASS |
| ac_reads | `index_join` | 10.24ms | 7.81ms | 0.8× | 2.7% | PASS |
| ac_reads | `index_join_scan` | 3.19ms | 4.88ms | 1.5× | 3.2% | PASS |
| ac_reads | `types_table_scan` | 885.51ms | 985.07ms | 1.1× | 0.9% | PASS |
| ac_reads | `table_scan` | 1.01s | 1.07s | 1.1× | 0.9% | PASS |
| ac_reads | `oltp_read_only` | 115.40ms | 109.64ms | 1.0× | 1.4% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 58.63ms | 188.61ms | 3.2× | 43.8% | PASS |
| ac_writes | `oltp_insert_ac` | 67.85ms | 193.43ms | 2.9× | 55.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 65.38ms | 246.14ms | 3.8× | 65.3% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 40.54ms | 125.75ms | 3.1× | 29.2% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 46.65ms | 157.54ms | 3.4× | 33.0% | PASS |
| ac_writes | `oltp_write_only_ac` | 40.11ms | 121.36ms | 3.0× | 20.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 41.42ms | 115.42ms | 2.8× | 22.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 53.70ms | 194.28ms | 3.6× | 52.4% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 34.09ms | 37.01ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_range_select` | 20.05ms | 21.02ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_sum_range` | 18.11ms | 20.45ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_order_range` | 3.76ms | 3.92ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 4.81ms | 5.10ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 4.76ms | 5.72ms | 1.2× | 1.5% | PASS |
| mem_reads | `select_random_points` | 28.30ms | 30.93ms | 1.1× | 1.5% | PASS |
| mem_reads | `select_random_ranges` | 7.69ms | 8.20ms | 1.1× | 1.1% | PASS |
| mem_reads | `covering_index_scan` | 4.35ms | 4.19ms | 1.0× | 2.3% | PASS |
| mem_reads | `groupby_scan` | 39.16ms | 42.09ms | 1.1× | 0.8% | PASS |
| mem_reads | `index_join` | 8.17ms | 10.46ms | 1.3× | 2.8% | PASS |
| mem_reads | `index_join_scan` | 4.29ms | 5.61ms | 1.3× | 2.0% | PASS |
| mem_reads | `types_table_scan` | 1.13s | 1.21s | 1.1× | 0.9% | PASS |
| mem_reads | `table_scan` | 1.42s | 1.36s | 1.0× | 4.8% | PASS |
| mem_reads | `oltp_read_only` | 161.51ms | 170.44ms | 1.1× | 0.7% | PASS |
| mem_writes | `oltp_bulk_insert` | 245.81ms | 340.51ms | 1.4× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 19.70ms | 35.61ms | 1.8× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 72.58ms | 128.73ms | 1.8× | 1.4% | PASS |
| mem_writes | `oltp_update_non_index` | 55.04ms | 80.09ms | 1.5× | 1.1% | PASS |
| mem_writes | `oltp_delete_insert` | 53.69ms | 101.39ms | 1.9× | 0.8% | PASS |
| mem_writes | `oltp_write_only` | 29.16ms | 61.13ms | 2.1× | 1.3% | PASS |
| mem_writes | `types_delete_insert` | 34.15ms | 53.34ms | 1.6× | 1.2% | PASS |
| mem_writes | `oltp_read_write` | 107.43ms | 159.28ms | 1.5× | 1.5% | PASS |
| file_reads | `oltp_point_select` | 119.19ms | 59.22ms | 0.5× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 29.83ms | 23.68ms | 0.8× | 1.2% | PASS |
| file_reads | `oltp_sum_range` | 27.27ms | 23.17ms | 0.8× | 1.0% | PASS |
| file_reads | `oltp_order_range` | 4.66ms | 4.20ms | 0.9× | 0.9% | PASS |
| file_reads | `oltp_distinct_range` | 5.74ms | 5.38ms | 0.9× | 0.7% | PASS |
| file_reads | `oltp_index_scan` | 13.65ms | 8.39ms | 0.6× | 1.5% | PASS |
| file_reads | `select_random_points` | 38.20ms | 34.49ms | 0.9× | 1.2% | PASS |
| file_reads | `select_random_ranges` | 16.32ms | 10.70ms | 0.7× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 12.99ms | 6.81ms | 0.5× | 1.0% | PASS |
| file_reads | `groupby_scan` | 40.18ms | 42.69ms | 1.1× | 1.0% | PASS |
| file_reads | `index_join` | 12.68ms | 11.93ms | 0.9× | 1.2% | PASS |
| file_reads | `index_join_scan` | 5.23ms | 5.96ms | 1.1× | 1.9% | PASS |
| file_reads | `types_table_scan` | 1.20s | 1.24s | 1.0× | 2.5% | PASS |
| file_reads | `table_scan` | 1.42s | 1.37s | 1.0× | 3.7% | PASS |
| file_reads | `oltp_read_only` | 274.62ms | 196.72ms | 0.7× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 261.33ms | 347.53ms | 1.3× | 1.2% | PASS |
| file_writes | `oltp_insert` | 25.92ms | 40.77ms | 1.6× | 1.5% | PASS |
| file_writes | `oltp_update_index` | 100.27ms | 134.80ms | 1.3× | 1.6% | PASS |
| file_writes | `oltp_update_non_index` | 79.16ms | 92.54ms | 1.2× | 1.1% | PASS |
| file_writes | `oltp_delete_insert` | 77.56ms | 108.73ms | 1.4× | 1.7% | PASS |
| file_writes | `oltp_write_only` | 50.10ms | 68.75ms | 1.4× | 1.7% | PASS |
| file_writes | `types_delete_insert` | 49.75ms | 57.92ms | 1.2× | 1.9% | PASS |
| file_writes | `oltp_read_write` | 122.25ms | 163.98ms | 1.3× | 1.3% | PASS |
| ac_reads | `oltp_point_select` | 61.34ms | 58.14ms | 0.9× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 23.35ms | 23.36ms | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_sum_range` | 21.32ms | 22.91ms | 1.1× | 1.0% | PASS |
| ac_reads | `oltp_order_range` | 4.16ms | 4.19ms | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_distinct_range` | 5.18ms | 5.36ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_index_scan` | 7.92ms | 8.19ms | 1.0× | 1.4% | PASS |
| ac_reads | `select_random_points` | 31.02ms | 33.12ms | 1.1× | 1.3% | PASS |
| ac_reads | `select_random_ranges` | 10.63ms | 10.61ms | 1.0× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 7.57ms | 6.73ms | 0.9× | 1.5% | PASS |
| ac_reads | `groupby_scan` | 39.40ms | 42.68ms | 1.1× | 0.9% | PASS |
| ac_reads | `index_join` | 10.32ms | 12.38ms | 1.2× | 1.9% | PASS |
| ac_reads | `index_join_scan` | 4.79ms | 6.09ms | 1.3× | 1.9% | PASS |
| ac_reads | `types_table_scan` | 1.20s | 1.25s | 1.0× | 1.3% | PASS |
| ac_reads | `table_scan` | 1.32s | 1.34s | 1.0× | 2.4% | PASS |
| ac_reads | `oltp_read_only` | 191.02ms | 195.83ms | 1.0× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 17.21ms | 58.20ms | 3.4× | 4.4% | PASS |
| ac_writes | `oltp_insert_ac` | 19.05ms | 76.37ms | 4.0× | 4.2% | PASS |
| ac_writes | `oltp_update_index_ac` | 21.41ms | 87.40ms | 4.1× | 5.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 17.88ms | 69.71ms | 3.9× | 7.7% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 18.85ms | 80.64ms | 4.3× | 6.5% | PASS |
| ac_writes | `oltp_write_only_ac` | 20.12ms | 81.09ms | 4.0× | 8.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 17.25ms | 70.17ms | 4.1× | 7.3% | PASS |
| ac_writes | `oltp_read_write_ac` | 27.47ms | 88.49ms | 3.2× | 5.4% | PASS |

</details>

</details>

## Version-control latency

Wall time: 2m 26s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 90.92ms | 130.00ms | 69.9% | 0.2% | PASS |
| `status_dirty_many_tables` | 94.44ms | 130.00ms | 72.7% | 0.2% | PASS |
| `diff_regular_working_one_table` | 85.99ms | 120.00ms | 71.7% | 0.2% | PASS |
| `diff_regular_working_many_tables` | 99.38ms | 140.00ms | 71.0% | 0.2% | PASS |
| `diff_stat_working_many_tables` | 99.30ms | 140.00ms | 70.9% | 0.2% | PASS |
| `diff_schema_working_many_tables` | 99.99ms | 140.00ms | 71.4% | 0.2% | PASS |
| `branch_list_many_branches` | 23.43ms | 35.00ms | 67.0% | 0.5% | PASS |
| `branch_create_delete` | 25.77ms | 40.00ms | 64.4% | 0.5% | PASS |
| `checkout_branch_clean` | 58.11ms | 150.00ms | 38.7% | 0.4% | PASS |
| `merge_data_no_conflicts` | 30.14ms | 50.00ms | 60.3% | 0.8% | PASS |
| `merge_schema_no_conflicts` | 22.22ms | 35.00ms | 63.5% | 1.0% | PASS |
| `merge_data_conflicts` | 128.16ms | 180.00ms | 71.2% | 0.2% | PASS |
| `merge_data_conflicts_with_resolve` | 128.39ms | 180.00ms | 71.3% | 0.2% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
