# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-10-10 11:09 UTC
>
> Commit: [`4ea7c17e2257204a5eaf19748fbb7a1a33ce1ac5`](https://github.com/dolthub/doltlite/commit/4ea7c17e2257204a5eaf19748fbb7a1a33ce1ac5)
>
> Runner: ubuntu24 20261004.327.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/38042061253)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 8.37s | 9.44s | 1.1× | 1.9% | **PASS** |
| Writes | 1.71s | 2.71s | 1.6× | 1.5% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.63s | 9.78s | 1.0× | 1.4% | **PASS** |
| Writes | 3.34s | 3.60s | 1.1× | 2.5% | **PASS** |
| Autocommit writes | 805.32ms | 2.19s | 2.7× | 5.0% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.49s | 2.76s | 1.1× | 1.6% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.61s | 3.06s | 1.2× | 1.5% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 1.59s | 1.78s | 1.1× | 2.4% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 1.69s | 1.84s | 1.1× | 3.2% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 438.53ms | 718.78ms | 1.6× | 1.3% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 581.57ms | 941.84ms | 1.6× | 1.3% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 335.33ms | 523.33ms | 1.6× | 1.5% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 357.57ms | 526.87ms | 1.5× | 2.1% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.82s | 2.85s | 1.0× | 1.4% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 3.00s | 3.18s | 1.1× | 1.5% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 1.75s | 1.83s | 1.0× | 1.2% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.06s | 1.92s | 0.9× | 1.8% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 601.60ms | 798.43ms | 1.3× | 1.7% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 888.68ms | 1.05s | 1.2× | 3.1% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 978.05ms | 918.83ms | 0.9× | 3.6% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 875.51ms | 838.68ms | 1.0× | 2.8% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.68s | 2.86s | 1.1× | 1.4% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.67s | 3.12s | 1.2× | 1.3% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 1.64s | 1.82s | 1.1× | 1.4% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 1.70s | 1.86s | 1.1× | 1.9% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 190.29ms | 607.00ms | 3.2× | 6.0% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 203.62ms | 618.29ms | 3.0× | 5.8% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 213.16ms | 476.76ms | 2.2× | 4.0% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 198.25ms | 488.35ms | 2.5× | 4.6% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.09ms | 31.48ms | 1.3× | 1.7% | PASS |
| mem_reads | `oltp_range_select` | 10.40ms | 12.32ms | 1.2× | 1.8% | PASS |
| mem_reads | `oltp_sum_range` | 9.45ms | 12.01ms | 1.3× | 1.7% | PASS |
| mem_reads | `oltp_order_range` | 2.61ms | 3.02ms | 1.2× | 1.3% | PASS |
| mem_reads | `oltp_distinct_range` | 3.67ms | 4.14ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 3.91ms | 5.72ms | 1.5× | 2.1% | PASS |
| mem_reads | `select_random_points` | 10.06ms | 11.98ms | 1.2× | 2.5% | PASS |
| mem_reads | `select_random_ranges` | 4.64ms | 5.46ms | 1.2× | 1.6% | PASS |
| mem_reads | `covering_index_scan` | 7.62ms | 10.85ms | 1.4× | 1.3% | PASS |
| mem_reads | `groupby_scan` | 29.68ms | 33.03ms | 1.1× | 0.8% | PASS |
| mem_reads | `index_join` | 5.72ms | 9.04ms | 1.6× | 2.4% | PASS |
| mem_reads | `index_join_scan` | 3.19ms | 5.83ms | 1.8× | 2.1% | PASS |
| mem_reads | `types_table_scan` | 1.06s | 1.19s | 1.1× | 1.3% | PASS |
| mem_reads | `table_scan` | 1.21s | 1.31s | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_read_only` | 104.46ms | 126.27ms | 1.2× | 1.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 178.31ms | 277.54ms | 1.6× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 15.12ms | 27.36ms | 1.8× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 49.80ms | 91.61ms | 1.8× | 1.4% | PASS |
| mem_writes | `oltp_update_non_index` | 34.24ms | 57.18ms | 1.7× | 1.2% | PASS |
| mem_writes | `oltp_delete_insert` | 44.85ms | 73.56ms | 1.6× | 1.2% | PASS |
| mem_writes | `oltp_write_only` | 21.86ms | 44.29ms | 2.0× | 1.6% | PASS |
| mem_writes | `types_delete_insert` | 25.03ms | 37.67ms | 1.5× | 2.6% | PASS |
| mem_writes | `oltp_read_write` | 69.33ms | 109.58ms | 1.6× | 1.7% | PASS |
| file_reads | `oltp_point_select` | 93.82ms | 50.45ms | 0.5× | 0.6% | PASS |
| file_reads | `oltp_range_select` | 17.74ms | 14.34ms | 0.8× | 1.7% | PASS |
| file_reads | `oltp_sum_range` | 16.97ms | 14.17ms | 0.8× | 1.4% | PASS |
| file_reads | `oltp_order_range` | 3.42ms | 3.28ms | 1.0× | 1.4% | PASS |
| file_reads | `oltp_distinct_range` | 4.54ms | 4.44ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 11.29ms | 7.91ms | 0.7× | 2.1% | PASS |
| file_reads | `select_random_points` | 17.67ms | 14.29ms | 0.8× | 2.6% | PASS |
| file_reads | `select_random_ranges` | 12.00ms | 7.51ms | 0.6× | 1.2% | PASS |
| file_reads | `covering_index_scan` | 15.21ms | 12.83ms | 0.8× | 1.5% | PASS |
| file_reads | `groupby_scan` | 30.54ms | 33.58ms | 1.1× | 0.9% | PASS |
| file_reads | `index_join` | 10.01ms | 10.57ms | 1.1× | 2.8% | PASS |
| file_reads | `index_join_scan` | 4.18ms | 6.27ms | 1.5× | 2.8% | PASS |
| file_reads | `types_table_scan` | 1.10s | 1.20s | 1.1× | 0.9% | PASS |
| file_reads | `table_scan` | 1.27s | 1.32s | 1.0× | 0.7% | PASS |
| file_reads | `oltp_read_only` | 208.81ms | 155.29ms | 0.7× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 193.06ms | 288.57ms | 1.5× | 1.2% | PASS |
| file_writes | `oltp_insert` | 21.95ms | 31.00ms | 1.4× | 1.3% | PASS |
| file_writes | `oltp_update_index` | 79.70ms | 108.79ms | 1.4× | 2.0% | PASS |
| file_writes | `oltp_update_non_index` | 59.28ms | 68.08ms | 1.1× | 1.5% | PASS |
| file_writes | `oltp_delete_insert` | 69.04ms | 86.71ms | 1.3× | 2.3% | PASS |
| file_writes | `oltp_write_only` | 45.28ms | 53.73ms | 1.2× | 1.7% | PASS |
| file_writes | `types_delete_insert` | 40.36ms | 43.93ms | 1.1× | 1.7% | PASS |
| file_writes | `oltp_read_write` | 92.92ms | 117.62ms | 1.3× | 2.3% | PASS |
| ac_reads | `oltp_point_select` | 47.23ms | 50.46ms | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_range_select` | 13.29ms | 14.38ms | 1.1× | 1.9% | PASS |
| ac_reads | `oltp_sum_range` | 12.48ms | 14.15ms | 1.1× | 2.0% | PASS |
| ac_reads | `oltp_order_range` | 2.95ms | 3.27ms | 1.1× | 1.6% | PASS |
| ac_reads | `oltp_distinct_range` | 4.03ms | 4.44ms | 1.1× | 1.0% | PASS |
| ac_reads | `oltp_index_scan` | 6.62ms | 7.88ms | 1.2× | 1.4% | PASS |
| ac_reads | `select_random_points` | 13.43ms | 14.39ms | 1.1× | 2.6% | PASS |
| ac_reads | `select_random_ranges` | 7.16ms | 7.50ms | 1.0× | 1.2% | PASS |
| ac_reads | `covering_index_scan` | 10.61ms | 12.88ms | 1.2× | 1.4% | PASS |
| ac_reads | `groupby_scan` | 30.05ms | 33.67ms | 1.1× | 0.7% | PASS |
| ac_reads | `index_join` | 7.64ms | 10.69ms | 1.4× | 2.4% | PASS |
| ac_reads | `index_join_scan` | 3.75ms | 6.31ms | 1.7× | 2.7% | PASS |
| ac_reads | `types_table_scan` | 1.10s | 1.20s | 1.1× | 1.2% | PASS |
| ac_reads | `table_scan` | 1.28s | 1.32s | 1.0× | 0.6% | PASS |
| ac_reads | `oltp_read_only` | 142.87ms | 156.91ms | 1.1× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.07ms | 62.77ms | 2.8× | 7.3% | PASS |
| ac_writes | `oltp_insert_ac` | 23.44ms | 75.07ms | 3.2× | 7.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.32ms | 87.36ms | 3.3× | 6.8% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.89ms | 70.25ms | 3.1× | 6.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 23.18ms | 79.75ms | 3.4× | 4.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 23.39ms | 76.72ms | 3.3× | 4.7% | PASS |
| ac_writes | `types_delete_insert_ac` | 20.64ms | 70.02ms | 3.4× | 4.8% | PASS |
| ac_writes | `oltp_read_write_ac` | 28.36ms | 85.07ms | 3.0× | 5.2% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 35.03ms | 40.60ms | 1.2× | 1.9% | PASS |
| mem_reads | `oltp_range_select` | 16.28ms | 16.04ms | 1.0× | 1.7% | PASS |
| mem_reads | `oltp_sum_range` | 15.49ms | 15.40ms | 1.0× | 1.5% | PASS |
| mem_reads | `oltp_order_range` | 3.32ms | 3.46ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 4.41ms | 4.60ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 3.94ms | 6.73ms | 1.7× | 1.3% | PASS |
| mem_reads | `select_random_points` | 21.44ms | 23.37ms | 1.1× | 2.5% | PASS |
| mem_reads | `select_random_ranges` | 6.63ms | 6.94ms | 1.0× | 1.8% | PASS |
| mem_reads | `covering_index_scan` | 7.63ms | 10.84ms | 1.4× | 0.8% | PASS |
| mem_reads | `groupby_scan` | 34.30ms | 35.83ms | 1.0× | 0.8% | PASS |
| mem_reads | `index_join` | 10.27ms | 10.63ms | 1.0× | 2.0% | PASS |
| mem_reads | `index_join_scan` | 3.70ms | 6.49ms | 1.8× | 2.2% | PASS |
| mem_reads | `types_table_scan` | 1.08s | 1.31s | 1.2× | 0.5% | PASS |
| mem_reads | `table_scan` | 1.23s | 1.42s | 1.2× | 0.9% | PASS |
| mem_reads | `oltp_read_only` | 138.94ms | 148.78ms | 1.1× | 1.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 235.75ms | 337.78ms | 1.4× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 17.72ms | 37.67ms | 2.1× | 0.5% | PASS |
| mem_writes | `oltp_update_index` | 65.05ms | 133.86ms | 2.1× | 1.4% | PASS |
| mem_writes | `oltp_update_non_index` | 47.63ms | 81.22ms | 1.7× | 1.5% | PASS |
| mem_writes | `oltp_delete_insert` | 53.05ms | 99.27ms | 1.9× | 1.1% | PASS |
| mem_writes | `oltp_write_only` | 27.54ms | 56.47ms | 2.1× | 1.2% | PASS |
| mem_writes | `types_delete_insert` | 38.48ms | 55.30ms | 1.4× | 1.5% | PASS |
| mem_writes | `oltp_read_write` | 96.35ms | 140.29ms | 1.5× | 1.4% | PASS |
| file_reads | `oltp_point_select` | 104.86ms | 59.80ms | 0.6× | 0.9% | PASS |
| file_reads | `oltp_range_select` | 23.89ms | 18.13ms | 0.8× | 2.5% | PASS |
| file_reads | `oltp_sum_range` | 23.17ms | 17.55ms | 0.8× | 1.2% | PASS |
| file_reads | `oltp_order_range` | 4.15ms | 3.72ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_distinct_range` | 5.22ms | 4.87ms | 0.9× | 1.5% | PASS |
| file_reads | `oltp_index_scan` | 11.04ms | 8.78ms | 0.8× | 1.1% | PASS |
| file_reads | `select_random_points` | 30.20ms | 26.20ms | 0.9× | 2.2% | PASS |
| file_reads | `select_random_ranges` | 13.97ms | 9.05ms | 0.6× | 1.5% | PASS |
| file_reads | `covering_index_scan` | 14.95ms | 13.22ms | 0.9× | 0.9% | PASS |
| file_reads | `groupby_scan` | 35.05ms | 36.15ms | 1.0× | 0.9% | PASS |
| file_reads | `index_join` | 14.51ms | 11.97ms | 0.8× | 2.5% | PASS |
| file_reads | `index_join_scan` | 4.63ms | 7.07ms | 1.5× | 2.0% | PASS |
| file_reads | `types_table_scan` | 1.13s | 1.33s | 1.2× | 3.3% | PASS |
| file_reads | `table_scan` | 1.34s | 1.45s | 1.1× | 4.2% | PASS |
| file_reads | `oltp_read_only` | 245.14ms | 178.61ms | 0.7× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 261.10ms | 352.75ms | 1.4× | 1.1% | PASS |
| file_writes | `oltp_insert` | 25.12ms | 42.91ms | 1.7× | 1.2% | PASS |
| file_writes | `oltp_update_index` | 112.86ms | 151.47ms | 1.3× | 7.2% | PASS |
| file_writes | `oltp_update_non_index` | 92.70ms | 94.84ms | 1.0× | 9.0% | PASS |
| file_writes | `oltp_delete_insert` | 94.99ms | 115.31ms | 1.2× | 1.3% | PASS |
| file_writes | `oltp_write_only` | 79.99ms | 69.60ms | 0.9× | 19.2% | PASS |
| file_writes | `types_delete_insert` | 70.52ms | 67.26ms | 1.0× | 1.9% | PASS |
| file_writes | `oltp_read_write` | 151.39ms | 152.77ms | 1.0× | 4.2% | PASS |
| ac_reads | `oltp_point_select` | 58.30ms | 59.80ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 19.25ms | 18.30ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_sum_range` | 18.36ms | 17.66ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_order_range` | 3.86ms | 3.74ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_distinct_range` | 4.80ms | 4.88ms | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_index_scan` | 6.62ms | 8.85ms | 1.3× | 1.3% | PASS |
| ac_reads | `select_random_points` | 24.88ms | 26.21ms | 1.1× | 1.6% | PASS |
| ac_reads | `select_random_ranges` | 9.36ms | 9.08ms | 1.0× | 1.4% | PASS |
| ac_reads | `covering_index_scan` | 10.41ms | 13.25ms | 1.3× | 1.1% | PASS |
| ac_reads | `groupby_scan` | 34.46ms | 36.14ms | 1.0× | 0.7% | PASS |
| ac_reads | `index_join` | 12.21ms | 11.92ms | 1.0× | 1.4% | PASS |
| ac_reads | `index_join_scan` | 4.17ms | 6.98ms | 1.7× | 1.3% | PASS |
| ac_reads | `types_table_scan` | 1.07s | 1.31s | 1.2× | 0.7% | PASS |
| ac_reads | `table_scan` | 1.22s | 1.42s | 1.2× | 0.7% | PASS |
| ac_reads | `oltp_read_only` | 173.79ms | 177.87ms | 1.0× | 0.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 24.14ms | 62.93ms | 2.6× | 6.7% | PASS |
| ac_writes | `oltp_insert_ac` | 25.09ms | 74.47ms | 3.0× | 4.8% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.48ms | 89.65ms | 3.4× | 5.8% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.15ms | 70.40ms | 3.2× | 6.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.40ms | 81.72ms | 3.2× | 4.9% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.93ms | 79.30ms | 3.1× | 4.8% | PASS |
| ac_writes | `types_delete_insert_ac` | 23.43ms | 74.38ms | 3.2× | 10.0% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.01ms | 85.46ms | 2.8× | 5.7% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 21.08ms | 22.34ms | 1.1× | 2.3% | PASS |
| mem_reads | `oltp_range_select` | 9.29ms | 8.99ms | 1.0× | 2.4% | PASS |
| mem_reads | `oltp_sum_range` | 9.14ms | 8.91ms | 1.0× | 2.4% | PASS |
| mem_reads | `oltp_order_range` | 2.00ms | 2.04ms | 1.0× | 2.7% | PASS |
| mem_reads | `oltp_distinct_range` | 2.51ms | 2.51ms | 1.0× | 1.8% | PASS |
| mem_reads | `oltp_index_scan` | 2.40ms | 4.29ms | 1.8× | 2.5% | PASS |
| mem_reads | `select_random_points` | 13.75ms | 13.80ms | 1.0× | 3.1% | PASS |
| mem_reads | `select_random_ranges` | 3.97ms | 3.95ms | 1.0× | 2.6% | PASS |
| mem_reads | `covering_index_scan` | 3.86ms | 5.92ms | 1.5× | 1.1% | PASS |
| mem_reads | `groupby_scan` | 19.13ms | 19.53ms | 1.0× | 1.1% | PASS |
| mem_reads | `index_join` | 7.17ms | 7.34ms | 1.0× | 2.7% | PASS |
| mem_reads | `index_join_scan` | 2.59ms | 5.13ms | 2.0× | 3.0% | PASS |
| mem_reads | `types_table_scan` | 660.77ms | 760.50ms | 1.2× | 0.6% | PASS |
| mem_reads | `table_scan` | 755.10ms | 842.08ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_read_only` | 73.47ms | 73.62ms | 1.0× | 1.2% | PASS |
| mem_writes | `oltp_bulk_insert` | 134.74ms | 183.20ms | 1.4× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 10.36ms | 21.30ms | 2.1× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 38.32ms | 77.49ms | 2.0× | 1.6% | PASS |
| mem_writes | `oltp_update_non_index` | 29.74ms | 46.07ms | 1.5× | 1.6% | PASS |
| mem_writes | `oltp_delete_insert` | 31.16ms | 57.17ms | 1.8× | 1.3% | PASS |
| mem_writes | `oltp_write_only` | 16.95ms | 33.57ms | 2.0× | 1.4% | PASS |
| mem_writes | `types_delete_insert` | 22.60ms | 31.23ms | 1.4× | 1.6% | PASS |
| mem_writes | `oltp_read_write` | 51.47ms | 73.30ms | 1.4× | 1.8% | PASS |
| file_reads | `oltp_point_select` | 72.21ms | 35.97ms | 0.5× | 1.2% | PASS |
| file_reads | `oltp_range_select` | 14.64ms | 10.18ms | 0.7× | 1.4% | PASS |
| file_reads | `oltp_sum_range` | 14.28ms | 10.03ms | 0.7× | 1.4% | PASS |
| file_reads | `oltp_order_range` | 2.63ms | 2.21ms | 0.8× | 1.0% | PASS |
| file_reads | `oltp_distinct_range` | 3.15ms | 2.67ms | 0.8× | 1.4% | PASS |
| file_reads | `oltp_index_scan` | 7.87ms | 5.57ms | 0.7× | 0.7% | PASS |
| file_reads | `select_random_points` | 19.15ms | 14.44ms | 0.8× | 1.9% | PASS |
| file_reads | `select_random_ranges` | 9.45ms | 5.38ms | 0.6× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 9.48ms | 7.25ms | 0.8× | 1.0% | PASS |
| file_reads | `groupby_scan` | 19.65ms | 19.46ms | 1.0× | 1.2% | PASS |
| file_reads | `index_join` | 10.33ms | 7.47ms | 0.7× | 1.9% | PASS |
| file_reads | `index_join_scan` | 3.17ms | 4.67ms | 1.5× | 1.7% | PASS |
| file_reads | `types_table_scan` | 662.22ms | 760.14ms | 1.1× | 0.6% | PASS |
| file_reads | `table_scan` | 757.03ms | 847.23ms | 1.1× | 0.9% | PASS |
| file_reads | `oltp_read_only` | 146.41ms | 93.22ms | 0.6× | 1.4% | PASS |
| file_writes | `oltp_bulk_insert` | 196.96ms | 240.30ms | 1.2× | 2.6% | PASS |
| file_writes | `oltp_insert` | 22.20ms | 40.91ms | 1.8× | 8.7% | PASS |
| file_writes | `oltp_update_index` | 141.06ms | 153.59ms | 1.1× | 4.2% | PASS |
| file_writes | `oltp_update_non_index` | 120.63ms | 99.71ms | 0.8× | 3.1% | PASS |
| file_writes | `oltp_delete_insert` | 143.70ms | 121.27ms | 0.8× | 2.8% | PASS |
| file_writes | `oltp_write_only` | 108.33ms | 79.98ms | 0.7× | 6.1% | PASS |
| file_writes | `types_delete_insert` | 104.40ms | 63.17ms | 0.6× | 5.9% | PASS |
| file_writes | `oltp_read_write` | 140.77ms | 119.90ms | 0.9× | 2.3% | PASS |
| ac_reads | `oltp_point_select` | 38.63ms | 35.76ms | 0.9× | 2.1% | PASS |
| ac_reads | `oltp_range_select` | 11.42ms | 10.18ms | 0.9× | 1.9% | PASS |
| ac_reads | `oltp_sum_range` | 10.96ms | 9.95ms | 0.9× | 1.4% | PASS |
| ac_reads | `oltp_order_range` | 2.36ms | 2.21ms | 0.9× | 1.4% | PASS |
| ac_reads | `oltp_distinct_range` | 2.85ms | 2.68ms | 0.9× | 2.0% | PASS |
| ac_reads | `oltp_index_scan` | 4.54ms | 5.58ms | 1.2× | 2.0% | PASS |
| ac_reads | `select_random_points` | 15.76ms | 14.44ms | 0.9× | 1.9% | PASS |
| ac_reads | `select_random_ranges` | 6.12ms | 5.33ms | 0.9× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 6.05ms | 7.23ms | 1.2× | 2.0% | PASS |
| ac_reads | `groupby_scan` | 19.54ms | 19.53ms | 1.0× | 1.1% | PASS |
| ac_reads | `index_join` | 8.74ms | 7.48ms | 0.9× | 1.3% | PASS |
| ac_reads | `index_join_scan` | 2.88ms | 4.67ms | 1.6× | 1.4% | PASS |
| ac_reads | `types_table_scan` | 661.62ms | 763.41ms | 1.2× | 0.7% | PASS |
| ac_reads | `table_scan` | 753.38ms | 842.01ms | 1.1× | 0.9% | PASS |
| ac_reads | `oltp_read_only` | 98.14ms | 93.39ms | 1.0× | 1.4% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 26.26ms | 48.96ms | 1.9× | 2.7% | PASS |
| ac_writes | `oltp_insert_ac` | 27.83ms | 58.83ms | 2.1× | 7.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 28.36ms | 67.83ms | 2.4× | 5.9% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.47ms | 57.05ms | 2.3× | 6.9% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 27.96ms | 60.01ms | 2.1× | 2.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 27.35ms | 60.25ms | 2.2× | 2.8% | PASS |
| ac_writes | `types_delete_insert_ac` | 20.39ms | 57.83ms | 2.8× | 4.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 30.54ms | 66.00ms | 2.2× | 3.5% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 18.33ms | 22.61ms | 1.2× | 3.1% | PASS |
| mem_reads | `oltp_range_select` | 10.52ms | 11.48ms | 1.1× | 2.3% | PASS |
| mem_reads | `oltp_sum_range` | 9.96ms | 11.20ms | 1.1× | 3.4% | PASS |
| mem_reads | `oltp_order_range` | 2.19ms | 2.18ms | 1.0× | 3.0% | PASS |
| mem_reads | `oltp_distinct_range` | 2.63ms | 2.67ms | 1.0× | 3.2% | PASS |
| mem_reads | `oltp_index_scan` | 2.77ms | 3.83ms | 1.4× | 3.6% | PASS |
| mem_reads | `select_random_points` | 15.40ms | 18.66ms | 1.2× | 4.2% | PASS |
| mem_reads | `select_random_ranges` | 3.99ms | 4.88ms | 1.2× | 5.5% | PASS |
| mem_reads | `covering_index_scan` | 3.84ms | 5.37ms | 1.4× | 2.1% | PASS |
| mem_reads | `groupby_scan` | 19.50ms | 21.19ms | 1.1× | 1.4% | PASS |
| mem_reads | `index_join` | 4.65ms | 6.58ms | 1.4× | 2.4% | PASS |
| mem_reads | `index_join_scan` | 2.09ms | 4.45ms | 2.1× | 3.8% | PASS |
| mem_reads | `types_table_scan` | 674.03ms | 758.81ms | 1.1× | 1.2% | PASS |
| mem_reads | `table_scan` | 831.76ms | 872.21ms | 1.0× | 3.5% | PASS |
| mem_reads | `oltp_read_only` | 86.05ms | 90.14ms | 1.0× | 3.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 141.16ms | 179.35ms | 1.3× | 1.5% | PASS |
| mem_writes | `oltp_insert` | 11.04ms | 19.74ms | 1.8× | 1.9% | PASS |
| mem_writes | `oltp_update_index` | 42.81ms | 74.75ms | 1.7× | 2.3% | PASS |
| mem_writes | `oltp_update_non_index` | 33.02ms | 48.73ms | 1.5× | 1.5% | PASS |
| mem_writes | `oltp_delete_insert` | 31.74ms | 55.89ms | 1.8× | 2.4% | PASS |
| mem_writes | `oltp_write_only` | 17.82ms | 34.25ms | 1.9× | 2.3% | PASS |
| mem_writes | `types_delete_insert` | 21.21ms | 31.74ms | 1.5× | 2.0% | PASS |
| mem_writes | `oltp_read_write` | 58.78ms | 82.41ms | 1.4× | 2.5% | PASS |
| file_reads | `oltp_point_select` | 73.38ms | 37.22ms | 0.5× | 2.1% | PASS |
| file_reads | `oltp_range_select` | 16.79ms | 13.20ms | 0.8× | 1.1% | PASS |
| file_reads | `oltp_sum_range` | 15.81ms | 12.72ms | 0.8× | 1.8% | PASS |
| file_reads | `oltp_order_range` | 2.89ms | 2.43ms | 0.8× | 1.4% | PASS |
| file_reads | `oltp_distinct_range` | 3.42ms | 2.93ms | 0.9× | 2.0% | PASS |
| file_reads | `oltp_index_scan` | 8.68ms | 5.70ms | 0.7× | 1.7% | PASS |
| file_reads | `select_random_points` | 23.86ms | 20.98ms | 0.9× | 3.1% | PASS |
| file_reads | `select_random_ranges` | 9.91ms | 6.62ms | 0.7× | 1.8% | PASS |
| file_reads | `covering_index_scan` | 9.80ms | 7.37ms | 0.8× | 1.3% | PASS |
| file_reads | `groupby_scan` | 20.76ms | 21.55ms | 1.0× | 1.6% | PASS |
| file_reads | `index_join` | 8.05ms | 8.27ms | 1.0× | 4.5% | PASS |
| file_reads | `index_join_scan` | 3.40ms | 4.77ms | 1.4× | 6.7% | PASS |
| file_reads | `types_table_scan` | 702.49ms | 766.93ms | 1.1× | 2.2% | PASS |
| file_reads | `table_scan` | 992.55ms | 895.51ms | 0.9× | 1.6% | PASS |
| file_reads | `oltp_read_only` | 169.87ms | 113.34ms | 0.7× | 1.4% | PASS |
| file_writes | `oltp_bulk_insert` | 189.78ms | 221.50ms | 1.2× | 1.2% | PASS |
| file_writes | `oltp_insert` | 23.44ms | 30.10ms | 1.3× | 15.4% | PASS |
| file_writes | `oltp_update_index` | 144.91ms | 131.70ms | 0.9× | 3.7% | PASS |
| file_writes | `oltp_update_non_index` | 121.27ms | 91.50ms | 0.8× | 2.2% | PASS |
| file_writes | `oltp_delete_insert` | 122.90ms | 109.79ms | 0.9× | 2.9% | PASS |
| file_writes | `oltp_write_only` | 80.70ms | 71.45ms | 0.9× | 2.7% | PASS |
| file_writes | `types_delete_insert` | 70.29ms | 61.40ms | 0.9× | 2.9% | PASS |
| file_writes | `oltp_read_write` | 122.21ms | 121.23ms | 1.0× | 1.8% | PASS |
| ac_reads | `oltp_point_select` | 37.62ms | 37.03ms | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_range_select` | 13.39ms | 12.98ms | 1.0× | 2.3% | PASS |
| ac_reads | `oltp_sum_range` | 12.11ms | 12.73ms | 1.1× | 2.1% | PASS |
| ac_reads | `oltp_order_range` | 2.57ms | 2.41ms | 0.9× | 1.0% | PASS |
| ac_reads | `oltp_distinct_range` | 3.07ms | 2.90ms | 0.9× | 1.2% | PASS |
| ac_reads | `oltp_index_scan` | 5.10ms | 5.65ms | 1.1× | 2.1% | PASS |
| ac_reads | `select_random_points` | 19.33ms | 21.00ms | 1.1× | 2.2% | PASS |
| ac_reads | `select_random_ranges` | 6.40ms | 6.54ms | 1.0× | 0.9% | PASS |
| ac_reads | `covering_index_scan` | 6.30ms | 7.37ms | 1.2× | 1.2% | PASS |
| ac_reads | `groupby_scan` | 20.48ms | 21.59ms | 1.1× | 1.2% | PASS |
| ac_reads | `index_join` | 6.10ms | 8.09ms | 1.3× | 3.0% | PASS |
| ac_reads | `index_join_scan` | 2.79ms | 4.61ms | 1.7× | 2.8% | PASS |
| ac_reads | `types_table_scan` | 675.67ms | 762.09ms | 1.1× | 1.4% | PASS |
| ac_reads | `table_scan` | 779.45ms | 848.74ms | 1.1× | 1.8% | PASS |
| ac_reads | `oltp_read_only` | 109.30ms | 108.55ms | 1.0× | 1.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.53ms | 51.67ms | 2.4× | 4.1% | PASS |
| ac_writes | `oltp_insert_ac` | 25.42ms | 60.91ms | 2.4× | 4.6% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.22ms | 68.56ms | 2.5× | 6.5% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.44ms | 56.70ms | 2.6× | 3.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.96ms | 62.56ms | 2.5× | 4.5% | PASS |
| ac_writes | `oltp_write_only_ac` | 24.86ms | 62.17ms | 2.5× | 4.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 25.43ms | 59.54ms | 2.3× | 7.8% | PASS |
| ac_writes | `oltp_read_write_ac` | 27.38ms | 66.22ms | 2.4× | 5.7% | PASS |

</details>

</details>

## Version-control latency

Wall time: 4m 21s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 34.18ms | 38.00ms | 90.0% | 0.7% | PASS |
| `status_dirty_many_tables` | 39.07ms | 42.00ms | 93.0% | 2.0% | PASS |
| `diff_regular_working_one_table` | 30.61ms | 33.00ms | 92.7% | 1.3% | PASS |
| `diff_regular_working_many_tables` | 43.87ms | 50.00ms | 87.7% | 1.6% | PASS |
| `diff_stat_working_many_tables` | 43.66ms | 48.00ms | 91.0% | 1.0% | PASS |
| `diff_schema_working_many_tables` | 43.97ms | 48.00ms | 91.6% | 1.7% | PASS |
| `branch_list_many_branches` | 22.36ms | 25.00ms | 89.4% | 1.1% | PASS |
| `branch_create_delete` | 23.69ms | 27.00ms | 87.8% | 1.3% | PASS |
| `at_literal_deep_history` | 23.85ms | 28.00ms | 85.2% | 0.6% | PASS |
| `diff_literal_deep_history` | 23.84ms | 28.00ms | 85.1% | 0.4% | PASS |
| `history_literal_deep_history` | 25.01ms | 30.00ms | 83.4% | 0.5% | PASS |
| `checkout_branch_clean` | 32.73ms | 43.00ms | 76.1% | 0.6% | PASS |
| `merge_data_no_conflicts` | 27.42ms | 33.00ms | 83.1% | 0.8% | PASS |
| `merge_data_secondary_index` | 825.52ms | 944.00ms | 87.4% | 0.8% | PASS |
| `merge_schema_no_conflicts` | 22.16ms | 24.00ms | 92.3% | 2.0% | PASS |
| `merge_data_conflicts` | 31.21ms | 33.00ms | 94.6% | 1.4% | PASS |
| `merge_data_conflicts_with_resolve` | 32.04ms | 34.00ms | 94.2% | 1.2% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
