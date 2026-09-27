# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-09-27 11:12 UTC
>
> Commit: [`b4f19b30af06c5217d41ae6e993b3f85af517caf`](https://github.com/dolthub/doltlite/commit/b4f19b30af06c5217d41ae6e993b3f85af517caf)
>
> Runner: ubuntu24 20260920.314.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/36309880113)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.81s | 11.25s | 1.1× | 1.6% | **PASS** |
| Writes | 2.05s | 3.31s | 1.6× | 1.4% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.64s | 11.53s | 1.1× | 1.7% | **PASS** |
| Writes | 3.24s | 3.91s | 1.2× | 2.2% | **PASS** |
| Autocommit writes | 841.48ms | 2.48s | 2.9× | 6.5% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.42s | 2.75s | 1.1× | 1.2% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.85s | 3.29s | 1.2× | 1.5% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.55s | 3.21s | 1.3× | 1.4% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 1.99s | 2.01s | 1.0× | 2.9% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 433.55ms | 709.61ms | 1.6× | 1.3% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 626.63ms | 1.02s | 1.6× | 2.0% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 590.84ms | 974.47ms | 1.6× | 1.3% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 396.83ms | 603.81ms | 1.5× | 1.8% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.65s | 2.82s | 1.1× | 1.4% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 3.24s | 3.40s | 1.0× | 1.3% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.77s | 3.27s | 1.2× | 1.5% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 1.98s | 2.04s | 1.0× | 2.6% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 585.39ms | 788.91ms | 1.3× | 1.6% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 950.82ms | 1.13s | 1.2× | 4.3% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 805.45ms | 1.08s | 1.3× | 2.0% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 899.32ms | 910.15ms | 1.0× | 4.6% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.50s | 2.82s | 1.1× | 1.1% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.92s | 3.34s | 1.1× | 2.0% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.62s | 3.27s | 1.2× | 1.5% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 1.82s | 2.00s | 1.1× | 2.7% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 198.39ms | 624.54ms | 3.1× | 6.3% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 223.16ms | 666.83ms | 3.0× | 7.8% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 213.52ms | 637.35ms | 3.0× | 6.2% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 206.41ms | 549.45ms | 2.7× | 5.3% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 23.42ms | 30.65ms | 1.3× | 1.3% | PASS |
| mem_reads | `oltp_range_select` | 10.04ms | 11.51ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_sum_range` | 9.06ms | 11.40ms | 1.3× | 1.2% | PASS |
| mem_reads | `oltp_order_range` | 2.54ms | 2.86ms | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_distinct_range` | 3.62ms | 4.03ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 3.79ms | 5.20ms | 1.4× | 1.4% | PASS |
| mem_reads | `select_random_points` | 9.65ms | 11.19ms | 1.2× | 3.1% | PASS |
| mem_reads | `select_random_ranges` | 4.47ms | 5.17ms | 1.2× | 1.5% | PASS |
| mem_reads | `covering_index_scan` | 7.67ms | 10.96ms | 1.4× | 0.7% | PASS |
| mem_reads | `groupby_scan` | 29.60ms | 33.32ms | 1.1× | 0.6% | PASS |
| mem_reads | `index_join` | 5.71ms | 7.87ms | 1.4× | 1.0% | PASS |
| mem_reads | `index_join_scan` | 3.17ms | 4.79ms | 1.5× | 2.3% | PASS |
| mem_reads | `types_table_scan` | 1.04s | 1.17s | 1.1× | 0.5% | PASS |
| mem_reads | `table_scan` | 1.17s | 1.32s | 1.1× | 0.4% | PASS |
| mem_reads | `oltp_read_only` | 100.72ms | 122.24ms | 1.2× | 0.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 180.03ms | 278.50ms | 1.5× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 15.16ms | 28.57ms | 1.9× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 49.18ms | 90.72ms | 1.8× | 1.4% | PASS |
| mem_writes | `oltp_update_non_index` | 33.94ms | 54.48ms | 1.6× | 1.3% | PASS |
| mem_writes | `oltp_delete_insert` | 44.27ms | 72.76ms | 1.6× | 1.3% | PASS |
| mem_writes | `oltp_write_only` | 21.59ms | 42.96ms | 2.0× | 1.4% | PASS |
| mem_writes | `types_delete_insert` | 24.35ms | 36.78ms | 1.5× | 1.6% | PASS |
| mem_writes | `oltp_read_write` | 65.04ms | 104.83ms | 1.6× | 1.0% | PASS |
| file_reads | `oltp_point_select` | 91.99ms | 49.28ms | 0.5× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 17.52ms | 13.60ms | 0.8× | 1.8% | PASS |
| file_reads | `oltp_sum_range` | 16.39ms | 13.70ms | 0.8× | 1.7% | PASS |
| file_reads | `oltp_order_range` | 3.38ms | 3.17ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_distinct_range` | 4.48ms | 4.34ms | 1.0× | 1.1% | PASS |
| file_reads | `oltp_index_scan` | 10.99ms | 7.62ms | 0.7× | 1.8% | PASS |
| file_reads | `select_random_points` | 17.30ms | 13.44ms | 0.8× | 2.1% | PASS |
| file_reads | `select_random_ranges` | 11.71ms | 7.33ms | 0.6× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 14.96ms | 13.37ms | 0.9× | 1.1% | PASS |
| file_reads | `groupby_scan` | 30.48ms | 33.83ms | 1.1× | 1.4% | PASS |
| file_reads | `index_join` | 9.68ms | 9.61ms | 1.0× | 1.8% | PASS |
| file_reads | `index_join_scan` | 4.13ms | 5.25ms | 1.3× | 2.2% | PASS |
| file_reads | `types_table_scan` | 1.04s | 1.18s | 1.1× | 0.5% | PASS |
| file_reads | `table_scan` | 1.18s | 1.32s | 1.1× | 0.8% | PASS |
| file_reads | `oltp_read_only` | 200.81ms | 149.62ms | 0.7× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 193.93ms | 289.29ms | 1.5× | 1.2% | PASS |
| file_writes | `oltp_insert` | 21.66ms | 32.47ms | 1.5× | 1.4% | PASS |
| file_writes | `oltp_update_index` | 75.10ms | 102.42ms | 1.4× | 1.4% | PASS |
| file_writes | `oltp_update_non_index` | 56.85ms | 66.99ms | 1.2× | 1.7% | PASS |
| file_writes | `oltp_delete_insert` | 66.79ms | 84.82ms | 1.3× | 1.8% | PASS |
| file_writes | `oltp_write_only` | 43.81ms | 54.01ms | 1.2× | 2.1% | PASS |
| file_writes | `types_delete_insert` | 39.00ms | 42.84ms | 1.1× | 1.4% | PASS |
| file_writes | `oltp_read_write` | 88.25ms | 116.08ms | 1.3× | 1.7% | PASS |
| ac_reads | `oltp_point_select` | 45.89ms | 49.55ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_range_select` | 12.77ms | 13.59ms | 1.1× | 1.6% | PASS |
| ac_reads | `oltp_sum_range` | 11.54ms | 13.66ms | 1.2× | 1.0% | PASS |
| ac_reads | `oltp_order_range` | 2.87ms | 3.15ms | 1.1× | 1.8% | PASS |
| ac_reads | `oltp_distinct_range` | 3.95ms | 4.32ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_index_scan` | 6.33ms | 7.57ms | 1.2× | 1.8% | PASS |
| ac_reads | `select_random_points` | 12.57ms | 13.47ms | 1.1× | 1.7% | PASS |
| ac_reads | `select_random_ranges` | 6.97ms | 7.28ms | 1.0× | 0.9% | PASS |
| ac_reads | `covering_index_scan` | 10.31ms | 13.35ms | 1.3× | 1.1% | PASS |
| ac_reads | `groupby_scan` | 29.82ms | 33.79ms | 1.1× | 0.7% | PASS |
| ac_reads | `index_join` | 7.15ms | 9.63ms | 1.3× | 1.4% | PASS |
| ac_reads | `index_join_scan` | 3.59ms | 5.21ms | 1.5× | 1.5% | PASS |
| ac_reads | `types_table_scan` | 1.04s | 1.18s | 1.1× | 0.4% | PASS |
| ac_reads | `table_scan` | 1.17s | 1.32s | 1.1× | 0.5% | PASS |
| ac_reads | `oltp_read_only` | 133.96ms | 150.70ms | 1.1× | 0.7% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.99ms | 62.84ms | 2.9× | 5.9% | PASS |
| ac_writes | `oltp_insert_ac` | 24.25ms | 77.55ms | 3.2× | 5.1% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.34ms | 91.38ms | 3.5× | 6.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.08ms | 72.77ms | 3.0× | 5.9% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.98ms | 83.76ms | 3.4× | 8.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.29ms | 80.53ms | 3.2× | 9.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 21.80ms | 70.57ms | 3.2× | 7.1% | PASS |
| ac_writes | `oltp_read_write_ac` | 29.66ms | 85.14ms | 2.9× | 5.1% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 36.15ms | 41.28ms | 1.1× | 2.7% | PASS |
| mem_reads | `oltp_range_select` | 17.34ms | 15.67ms | 0.9× | 1.9% | PASS |
| mem_reads | `oltp_sum_range` | 15.45ms | 16.00ms | 1.0× | 1.4% | PASS |
| mem_reads | `oltp_order_range` | 3.31ms | 3.38ms | 1.0× | 1.4% | PASS |
| mem_reads | `oltp_distinct_range` | 4.40ms | 4.60ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 3.97ms | 6.57ms | 1.7× | 1.5% | PASS |
| mem_reads | `select_random_points` | 23.55ms | 22.35ms | 0.9× | 3.0% | PASS |
| mem_reads | `select_random_ranges` | 6.74ms | 6.90ms | 1.0× | 1.4% | PASS |
| mem_reads | `covering_index_scan` | 7.79ms | 11.35ms | 1.5× | 0.9% | PASS |
| mem_reads | `groupby_scan` | 34.18ms | 36.53ms | 1.1× | 0.8% | PASS |
| mem_reads | `index_join` | 10.87ms | 10.03ms | 0.9× | 2.1% | PASS |
| mem_reads | `index_join_scan` | 3.80ms | 6.00ms | 1.6× | 2.6% | PASS |
| mem_reads | `types_table_scan` | 1.15s | 1.40s | 1.2× | 2.6% | PASS |
| mem_reads | `table_scan` | 1.38s | 1.55s | 1.1× | 1.7% | PASS |
| mem_reads | `oltp_read_only` | 147.97ms | 152.57ms | 1.0× | 1.5% | PASS |
| mem_writes | `oltp_bulk_insert` | 239.99ms | 372.76ms | 1.6× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 18.29ms | 41.31ms | 2.3× | 1.3% | PASS |
| mem_writes | `oltp_update_index` | 72.70ms | 148.03ms | 2.0× | 1.6% | PASS |
| mem_writes | `oltp_update_non_index` | 53.92ms | 85.98ms | 1.6× | 2.0% | PASS |
| mem_writes | `oltp_delete_insert` | 58.73ms | 108.82ms | 1.9× | 2.2% | PASS |
| mem_writes | `oltp_write_only` | 30.87ms | 61.57ms | 2.0× | 2.2% | PASS |
| mem_writes | `types_delete_insert` | 42.42ms | 57.26ms | 1.3× | 1.9% | PASS |
| mem_writes | `oltp_read_write` | 109.71ms | 149.13ms | 1.4× | 2.4% | PASS |
| file_reads | `oltp_point_select` | 111.12ms | 61.70ms | 0.6× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 25.92ms | 18.06ms | 0.7× | 1.7% | PASS |
| file_reads | `oltp_sum_range` | 23.54ms | 18.41ms | 0.8× | 1.5% | PASS |
| file_reads | `oltp_order_range` | 4.26ms | 3.70ms | 0.9× | 1.5% | PASS |
| file_reads | `oltp_distinct_range` | 5.41ms | 4.94ms | 0.9× | 1.0% | PASS |
| file_reads | `oltp_index_scan` | 11.76ms | 8.87ms | 0.8× | 1.2% | PASS |
| file_reads | `select_random_points` | 36.55ms | 26.76ms | 0.7× | 2.7% | PASS |
| file_reads | `select_random_ranges` | 14.78ms | 9.15ms | 0.6× | 1.3% | PASS |
| file_reads | `covering_index_scan` | 15.87ms | 13.99ms | 0.9× | 1.3% | PASS |
| file_reads | `groupby_scan` | 35.87ms | 37.47ms | 1.0× | 0.9% | PASS |
| file_reads | `index_join` | 16.17ms | 11.49ms | 0.7× | 2.8% | PASS |
| file_reads | `index_join_scan` | 4.95ms | 6.65ms | 1.3× | 3.3% | PASS |
| file_reads | `types_table_scan` | 1.22s | 1.42s | 1.2× | 2.1% | PASS |
| file_reads | `table_scan` | 1.45s | 1.58s | 1.1× | 0.7% | PASS |
| file_reads | `oltp_read_only` | 261.89ms | 186.94ms | 0.7× | 1.2% | PASS |
| file_writes | `oltp_bulk_insert` | 265.99ms | 390.27ms | 1.5× | 1.0% | PASS |
| file_writes | `oltp_insert` | 26.06ms | 47.13ms | 1.8× | 2.1% | PASS |
| file_writes | `oltp_update_index` | 140.88ms | 162.89ms | 1.2× | 6.1% | PASS |
| file_writes | `oltp_update_non_index` | 107.02ms | 103.14ms | 1.0× | 13.4% | PASS |
| file_writes | `oltp_delete_insert` | 98.39ms | 124.26ms | 1.3× | 2.6% | PASS |
| file_writes | `oltp_write_only` | 88.33ms | 74.67ms | 0.8× | 12.1% | PASS |
| file_writes | `types_delete_insert` | 74.36ms | 67.58ms | 0.9× | 2.5% | PASS |
| file_writes | `oltp_read_write` | 149.79ms | 161.41ms | 1.1× | 6.5% | PASS |
| ac_reads | `oltp_point_select` | 60.73ms | 60.65ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_range_select` | 20.77ms | 17.97ms | 0.9× | 2.2% | PASS |
| ac_reads | `oltp_sum_range` | 18.72ms | 18.36ms | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_order_range` | 3.83ms | 3.69ms | 1.0× | 2.1% | PASS |
| ac_reads | `oltp_distinct_range` | 4.83ms | 4.92ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_index_scan` | 6.77ms | 8.73ms | 1.3× | 1.0% | PASS |
| ac_reads | `select_random_points` | 27.60ms | 25.64ms | 0.9× | 3.7% | PASS |
| ac_reads | `select_random_ranges` | 9.60ms | 9.07ms | 0.9× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 10.64ms | 13.74ms | 1.3× | 1.7% | PASS |
| ac_reads | `groupby_scan` | 34.54ms | 36.94ms | 1.1× | 0.7% | PASS |
| ac_reads | `index_join` | 13.20ms | 11.49ms | 0.9× | 2.8% | PASS |
| ac_reads | `index_join_scan` | 4.34ms | 6.53ms | 1.5× | 2.0% | PASS |
| ac_reads | `types_table_scan` | 1.15s | 1.40s | 1.2× | 3.2% | PASS |
| ac_reads | `table_scan` | 1.38s | 1.54s | 1.1× | 3.1% | PASS |
| ac_reads | `oltp_read_only` | 181.71ms | 181.11ms | 1.0× | 2.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 25.53ms | 65.72ms | 2.6× | 10.9% | PASS |
| ac_writes | `oltp_insert_ac` | 28.39ms | 79.17ms | 2.8× | 8.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 29.74ms | 95.98ms | 3.2× | 8.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 25.72ms | 78.26ms | 3.0× | 7.4% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 27.48ms | 88.70ms | 3.2× | 8.2% | PASS |
| ac_writes | `oltp_write_only_ac` | 27.06ms | 84.92ms | 3.1× | 7.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 25.38ms | 82.90ms | 3.3× | 6.0% | PASS |
| ac_writes | `oltp_read_write_ac` | 33.86ms | 91.17ms | 2.7× | 6.7% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 34.75ms | 40.47ms | 1.2× | 1.8% | PASS |
| mem_reads | `oltp_range_select` | 16.19ms | 15.44ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_sum_range` | 14.64ms | 15.74ms | 1.1× | 1.8% | PASS |
| mem_reads | `oltp_order_range` | 3.20ms | 3.36ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 4.32ms | 4.58ms | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_index_scan` | 3.98ms | 6.62ms | 1.7× | 1.7% | PASS |
| mem_reads | `select_random_points` | 21.89ms | 21.91ms | 1.0× | 1.8% | PASS |
| mem_reads | `select_random_ranges` | 6.73ms | 7.01ms | 1.0× | 1.3% | PASS |
| mem_reads | `covering_index_scan` | 7.74ms | 11.57ms | 1.5× | 0.8% | PASS |
| mem_reads | `groupby_scan` | 33.73ms | 36.11ms | 1.1× | 0.8% | PASS |
| mem_reads | `index_join` | 10.39ms | 9.75ms | 0.9× | 1.7% | PASS |
| mem_reads | `index_join_scan` | 3.76ms | 5.98ms | 1.6× | 2.1% | PASS |
| mem_reads | `types_table_scan` | 1.06s | 1.37s | 1.3× | 0.6% | PASS |
| mem_reads | `table_scan` | 1.20s | 1.51s | 1.3× | 0.7% | PASS |
| mem_reads | `oltp_read_only` | 137.64ms | 149.04ms | 1.1× | 1.4% | PASS |
| mem_writes | `oltp_bulk_insert` | 242.71ms | 370.75ms | 1.5× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 18.41ms | 39.34ms | 2.1× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 64.87ms | 133.65ms | 2.1× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 48.45ms | 80.38ms | 1.7× | 1.3% | PASS |
| mem_writes | `oltp_delete_insert` | 52.74ms | 99.75ms | 1.9× | 1.3% | PASS |
| mem_writes | `oltp_write_only` | 28.03ms | 57.82ms | 2.1× | 1.4% | PASS |
| mem_writes | `types_delete_insert` | 38.27ms | 52.23ms | 1.4× | 1.3% | PASS |
| mem_writes | `oltp_read_write` | 97.37ms | 140.54ms | 1.4× | 1.5% | PASS |
| file_reads | `oltp_point_select` | 105.41ms | 59.33ms | 0.6× | 1.4% | PASS |
| file_reads | `oltp_range_select` | 24.02ms | 17.75ms | 0.7× | 1.5% | PASS |
| file_reads | `oltp_sum_range` | 22.57ms | 18.14ms | 0.8× | 2.3% | PASS |
| file_reads | `oltp_order_range` | 4.27ms | 3.71ms | 0.9× | 1.9% | PASS |
| file_reads | `oltp_distinct_range` | 5.33ms | 4.93ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_index_scan` | 11.16ms | 8.95ms | 0.8× | 2.4% | PASS |
| file_reads | `select_random_points` | 30.93ms | 24.99ms | 0.8× | 2.2% | PASS |
| file_reads | `select_random_ranges` | 14.17ms | 9.15ms | 0.6× | 1.6% | PASS |
| file_reads | `covering_index_scan` | 15.29ms | 13.86ms | 0.9× | 1.4% | PASS |
| file_reads | `groupby_scan` | 35.08ms | 36.73ms | 1.0× | 1.5% | PASS |
| file_reads | `index_join` | 14.82ms | 11.62ms | 0.8× | 2.5% | PASS |
| file_reads | `index_join_scan` | 4.83ms | 6.54ms | 1.4× | 2.7% | PASS |
| file_reads | `types_table_scan` | 1.05s | 1.36s | 1.3× | 0.4% | PASS |
| file_reads | `table_scan` | 1.19s | 1.51s | 1.3× | 0.9% | PASS |
| file_reads | `oltp_read_only` | 240.35ms | 178.23ms | 0.7× | 1.2% | PASS |
| file_writes | `oltp_bulk_insert` | 263.31ms | 384.56ms | 1.5× | 1.0% | PASS |
| file_writes | `oltp_insert` | 25.32ms | 46.33ms | 1.8× | 2.0% | PASS |
| file_writes | `oltp_update_index` | 95.40ms | 151.89ms | 1.6× | 2.1% | PASS |
| file_writes | `oltp_update_non_index` | 90.02ms | 95.22ms | 1.1× | 9.9% | PASS |
| file_writes | `oltp_delete_insert` | 85.25ms | 115.41ms | 1.4× | 1.2% | PASS |
| file_writes | `oltp_write_only` | 56.23ms | 71.22ms | 1.3× | 1.8% | PASS |
| file_writes | `types_delete_insert` | 62.34ms | 64.73ms | 1.0× | 2.2% | PASS |
| file_writes | `oltp_read_write` | 127.58ms | 153.19ms | 1.2× | 2.5% | PASS |
| ac_reads | `oltp_point_select` | 57.61ms | 59.38ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_range_select` | 18.72ms | 17.86ms | 1.0× | 2.4% | PASS |
| ac_reads | `oltp_sum_range` | 17.24ms | 18.04ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_order_range` | 3.74ms | 3.71ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_distinct_range` | 4.79ms | 4.93ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_index_scan` | 6.52ms | 8.95ms | 1.4× | 1.7% | PASS |
| ac_reads | `select_random_points` | 24.63ms | 25.10ms | 1.0× | 2.4% | PASS |
| ac_reads | `select_random_ranges` | 9.12ms | 9.12ms | 1.0× | 1.5% | PASS |
| ac_reads | `covering_index_scan` | 10.34ms | 13.88ms | 1.3× | 1.3% | PASS |
| ac_reads | `groupby_scan` | 33.85ms | 36.52ms | 1.1× | 1.5% | PASS |
| ac_reads | `index_join` | 12.04ms | 11.56ms | 1.0× | 2.6% | PASS |
| ac_reads | `index_join_scan` | 4.19ms | 6.43ms | 1.5× | 2.0% | PASS |
| ac_reads | `types_table_scan` | 1.05s | 1.37s | 1.3× | 0.5% | PASS |
| ac_reads | `table_scan` | 1.19s | 1.51s | 1.3× | 1.0% | PASS |
| ac_reads | `oltp_read_only` | 173.00ms | 178.00ms | 1.0× | 1.3% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 24.80ms | 62.89ms | 2.5× | 6.3% | PASS |
| ac_writes | `oltp_insert_ac` | 26.51ms | 79.29ms | 3.0× | 5.3% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.66ms | 93.06ms | 3.4× | 9.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.12ms | 71.32ms | 3.1× | 6.1% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.31ms | 80.75ms | 3.1× | 5.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.88ms | 82.02ms | 3.1× | 6.8% | PASS |
| ac_writes | `types_delete_insert_ac` | 25.32ms | 78.46ms | 3.1× | 8.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.92ms | 89.56ms | 2.7× | 4.3% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 22.31ms | 24.87ms | 1.1× | 1.8% | PASS |
| mem_reads | `oltp_range_select` | 11.30ms | 12.34ms | 1.1× | 3.9% | PASS |
| mem_reads | `oltp_sum_range` | 10.65ms | 12.26ms | 1.2× | 3.4% | PASS |
| mem_reads | `oltp_order_range` | 2.35ms | 2.44ms | 1.0× | 2.5% | PASS |
| mem_reads | `oltp_distinct_range` | 2.84ms | 2.91ms | 1.0× | 2.3% | PASS |
| mem_reads | `oltp_index_scan` | 2.95ms | 3.69ms | 1.3× | 3.3% | PASS |
| mem_reads | `select_random_points` | 17.89ms | 19.41ms | 1.1× | 2.5% | PASS |
| mem_reads | `select_random_ranges` | 4.63ms | 5.17ms | 1.1× | 1.7% | PASS |
| mem_reads | `covering_index_scan` | 4.05ms | 5.52ms | 1.4× | 2.9% | PASS |
| mem_reads | `groupby_scan` | 21.09ms | 23.11ms | 1.1× | 1.6% | PASS |
| mem_reads | `index_join` | 4.82ms | 6.03ms | 1.2× | 1.6% | PASS |
| mem_reads | `index_join_scan` | 2.88ms | 4.55ms | 1.6× | 3.9% | PASS |
| mem_reads | `types_table_scan` | 741.10ms | 833.58ms | 1.1× | 3.1% | PASS |
| mem_reads | `table_scan` | 1.05s | 952.73ms | 0.9× | 3.5% | PASS |
| mem_reads | `oltp_read_only` | 94.00ms | 99.25ms | 1.1× | 3.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 146.65ms | 203.72ms | 1.4× | 1.1% | PASS |
| mem_writes | `oltp_insert` | 11.41ms | 21.35ms | 1.9× | 1.9% | PASS |
| mem_writes | `oltp_update_index` | 46.04ms | 84.02ms | 1.8× | 3.0% | PASS |
| mem_writes | `oltp_update_non_index` | 36.92ms | 53.96ms | 1.5× | 1.6% | PASS |
| mem_writes | `oltp_delete_insert` | 36.55ms | 65.78ms | 1.8× | 2.0% | PASS |
| mem_writes | `oltp_write_only` | 20.74ms | 38.61ms | 1.9× | 3.3% | PASS |
| mem_writes | `types_delete_insert` | 23.43ms | 34.38ms | 1.5× | 1.8% | PASS |
| mem_writes | `oltp_read_write` | 75.10ms | 102.00ms | 1.4× | 1.5% | PASS |
| file_reads | `oltp_point_select` | 81.43ms | 43.56ms | 0.5× | 1.6% | PASS |
| file_reads | `oltp_range_select` | 18.20ms | 14.88ms | 0.8× | 2.6% | PASS |
| file_reads | `oltp_sum_range` | 16.34ms | 13.86ms | 0.8× | 2.9% | PASS |
| file_reads | `oltp_order_range` | 2.94ms | 2.59ms | 0.9× | 2.0% | PASS |
| file_reads | `oltp_distinct_range` | 3.52ms | 3.18ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 9.09ms | 5.89ms | 0.6× | 3.0% | PASS |
| file_reads | `select_random_points` | 24.10ms | 20.93ms | 0.9× | 3.4% | PASS |
| file_reads | `select_random_ranges` | 10.40ms | 7.02ms | 0.7× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 10.29ms | 7.72ms | 0.8× | 2.5% | PASS |
| file_reads | `groupby_scan` | 21.85ms | 23.23ms | 1.1× | 1.8% | PASS |
| file_reads | `index_join` | 8.51ms | 8.19ms | 1.0× | 3.2% | PASS |
| file_reads | `index_join_scan` | 3.67ms | 4.84ms | 1.3× | 5.4% | PASS |
| file_reads | `types_table_scan` | 741.81ms | 849.72ms | 1.1× | 6.9% | PASS |
| file_reads | `table_scan` | 848.18ms | 912.82ms | 1.1× | 3.0% | PASS |
| file_reads | `oltp_read_only` | 175.68ms | 123.73ms | 0.7× | 1.9% | PASS |
| file_writes | `oltp_bulk_insert` | 199.19ms | 250.30ms | 1.3× | 3.7% | PASS |
| file_writes | `oltp_insert` | 19.58ms | 34.35ms | 1.8× | 11.5% | PASS |
| file_writes | `oltp_update_index` | 140.66ms | 131.53ms | 0.9× | 2.2% | PASS |
| file_writes | `oltp_update_non_index` | 120.35ms | 99.36ms | 0.8× | 0.8% | PASS |
| file_writes | `oltp_delete_insert` | 120.63ms | 116.93ms | 1.0× | 4.8% | PASS |
| file_writes | `oltp_write_only` | 90.29ms | 79.21ms | 0.9× | 8.0% | PASS |
| file_writes | `types_delete_insert` | 78.15ms | 66.67ms | 0.9× | 4.6% | PASS |
| file_writes | `oltp_read_write` | 130.47ms | 131.80ms | 1.0× | 4.6% | PASS |
| ac_reads | `oltp_point_select` | 40.24ms | 40.14ms | 1.0× | 3.1% | PASS |
| ac_reads | `oltp_range_select` | 14.13ms | 14.19ms | 1.0× | 2.2% | PASS |
| ac_reads | `oltp_sum_range` | 12.98ms | 13.76ms | 1.1× | 3.1% | PASS |
| ac_reads | `oltp_order_range` | 2.63ms | 2.57ms | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_distinct_range` | 3.13ms | 3.11ms | 1.0× | 2.4% | PASS |
| ac_reads | `oltp_index_scan` | 5.41ms | 5.85ms | 1.1× | 2.3% | PASS |
| ac_reads | `select_random_points` | 21.53ms | 21.99ms | 1.0× | 3.6% | PASS |
| ac_reads | `select_random_ranges` | 6.88ms | 7.03ms | 1.0× | 1.8% | PASS |
| ac_reads | `covering_index_scan` | 6.76ms | 7.88ms | 1.2× | 3.6% | PASS |
| ac_reads | `groupby_scan` | 21.61ms | 23.45ms | 1.1× | 1.9% | PASS |
| ac_reads | `index_join` | 6.65ms | 8.18ms | 1.2× | 3.0% | PASS |
| ac_reads | `index_join_scan` | 3.16ms | 4.55ms | 1.4× | 3.7% | PASS |
| ac_reads | `types_table_scan` | 710.72ms | 817.97ms | 1.2× | 2.7% | PASS |
| ac_reads | `table_scan` | 846.71ms | 912.54ms | 1.1× | 4.6% | PASS |
| ac_reads | `oltp_read_only` | 116.13ms | 118.92ms | 1.0× | 2.5% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.56ms | 59.03ms | 2.6× | 4.9% | PASS |
| ac_writes | `oltp_insert_ac` | 26.71ms | 69.51ms | 2.6× | 4.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 28.21ms | 78.25ms | 2.8× | 5.8% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.27ms | 63.08ms | 2.8× | 4.7% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.02ms | 72.28ms | 2.8× | 5.8% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.00ms | 72.59ms | 2.8× | 8.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 26.46ms | 59.51ms | 2.2× | 8.3% | PASS |
| ac_writes | `oltp_read_write_ac` | 28.18ms | 75.20ms | 2.7× | 4.9% | PASS |

</details>

</details>

## Version-control latency

Wall time: 3m 26s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 35.09ms | 130.00ms | 27.0% | 1.2% | PASS |
| `status_dirty_many_tables` | 38.61ms | 130.00ms | 29.7% | 1.3% | PASS |
| `diff_regular_working_one_table` | 29.86ms | 120.00ms | 24.9% | 0.8% | PASS |
| `diff_regular_working_many_tables` | 44.84ms | 140.00ms | 32.0% | 1.2% | PASS |
| `diff_stat_working_many_tables` | 43.76ms | 140.00ms | 31.3% | 0.9% | PASS |
| `diff_schema_working_many_tables` | 44.07ms | 140.00ms | 31.5% | 0.7% | PASS |
| `branch_list_many_branches` | 22.11ms | 35.00ms | 63.2% | 0.8% | PASS |
| `branch_create_delete` | 24.34ms | 40.00ms | 60.9% | 1.3% | PASS |
| `at_literal_deep_history` | 24.94ms | 100.00ms | 24.9% | 0.8% | PASS |
| `diff_literal_deep_history` | 24.89ms | 120.00ms | 20.7% | 0.8% | PASS |
| `history_literal_deep_history` | 25.93ms | 150.00ms | 17.3% | 0.7% | PASS |
| `checkout_branch_clean` | 37.50ms | 150.00ms | 25.0% | 1.4% | PASS |
| `merge_data_no_conflicts` | 28.49ms | 50.00ms | 57.0% | 1.2% | PASS |
| `merge_data_secondary_index` | 867.26ms | 2.50s | 34.7% | 0.6% | PASS |
| `merge_schema_no_conflicts` | 21.02ms | 35.00ms | 60.1% | 1.0% | PASS |
| `merge_data_conflicts` | 29.65ms | 180.00ms | 16.5% | 0.6% | PASS |
| `merge_data_conflicts_with_resolve` | 30.50ms | 180.00ms | 16.9% | 0.6% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
