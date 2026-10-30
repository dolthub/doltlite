# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-10-30 11:35 UTC
>
> Commit: [`18fcc0fff23258f588f65765e081b5eb39f4d09a`](https://github.com/dolthub/doltlite/commit/18fcc0fff23258f588f65765e081b5eb39f4d09a)
>
> Runner: ubuntu24 20261026.354.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/41034130707)

The SQL workloads compare optimized DoltLite against stock SQLite on the same GitHub-hosted runner, alternating execution order on each repetition. Version-control sections measure DoltLite latency. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.04s | 12.47s | 1.2× | 1.3% | **PASS** |
| Writes | 2.01s | 3.43s | 1.7× | 1.1% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.68s | 12.65s | 1.2× | 1.3% | **PASS** |
| Writes | 3.72s | 4.39s | 1.2× | 1.6% | **PASS** |
| Autocommit writes | 844.68ms | 2.56s | 3.0× | 4.5% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.31s | 2.73s | 1.2× | 0.8% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.71s | 3.42s | 1.3× | 1.8% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.49s | 3.31s | 1.3× | 1.1% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.52s | 3.00s | 1.2× | 1.4% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 357.03ms | 625.84ms | 1.8× | 1.0% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 599.74ms | 1.02s | 1.7× | 1.7% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 575.05ms | 994.05ms | 1.7× | 1.0% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 482.49ms | 794.67ms | 1.6× | 1.6% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.42s | 2.76s | 1.1× | 0.8% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.84s | 3.42s | 1.2× | 1.8% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.72s | 3.35s | 1.2× | 1.4% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.70s | 3.12s | 1.2× | 1.5% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 927.62ms | 980.15ms | 1.1× | 1.3% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 922.85ms | 1.12s | 1.2× | 3.9% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 804.11ms | 1.10s | 1.4× | 1.9% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 1.06s | 1.18s | 1.1× | 2.0% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.34s | 2.76s | 1.2× | 0.8% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.77s | 3.43s | 1.2× | 1.5% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.59s | 3.35s | 1.3× | 1.1% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.60s | 3.07s | 1.2× | 1.8% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 230.51ms | 675.01ms | 2.9× | 3.0% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 191.51ms | 611.25ms | 3.2× | 5.4% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 199.19ms | 644.94ms | 3.2× | 5.3% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 223.47ms | 631.00ms | 2.8× | 3.0% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 20.77ms | 25.50ms | 1.2× | 0.7% | PASS |
| mem_reads | `oltp_range_select` | 9.48ms | 11.72ms | 1.2× | 0.8% | PASS |
| mem_reads | `oltp_sum_range` | 8.62ms | 10.66ms | 1.2× | 0.5% | PASS |
| mem_reads | `oltp_order_range` | 2.47ms | 2.93ms | 1.2× | 0.8% | PASS |
| mem_reads | `oltp_distinct_range` | 3.32ms | 3.89ms | 1.2× | 0.6% | PASS |
| mem_reads | `oltp_index_scan` | 3.45ms | 4.74ms | 1.4× | 0.8% | PASS |
| mem_reads | `select_random_points` | 9.70ms | 11.41ms | 1.2× | 0.8% | PASS |
| mem_reads | `select_random_ranges` | 3.91ms | 4.36ms | 1.1× | 1.0% | PASS |
| mem_reads | `covering_index_scan` | 6.29ms | 9.19ms | 1.5× | 0.7% | PASS |
| mem_reads | `groupby_scan` | 28.80ms | 31.53ms | 1.1× | 1.1% | PASS |
| mem_reads | `index_join` | 5.05ms | 7.89ms | 1.6× | 1.0% | PASS |
| mem_reads | `index_join_scan` | 2.77ms | 5.25ms | 1.9× | 1.4% | PASS |
| mem_reads | `types_table_scan` | 982.16ms | 1.18s | 1.2× | 0.2% | PASS |
| mem_reads | `table_scan` | 1.14s | 1.31s | 1.2× | 0.6% | PASS |
| mem_reads | `oltp_read_only` | 91.60ms | 111.77ms | 1.2× | 0.6% | PASS |
| mem_writes | `oltp_bulk_insert` | 138.09ms | 235.03ms | 1.7× | 0.4% | PASS |
| mem_writes | `oltp_insert` | 12.60ms | 21.71ms | 1.7× | 1.1% | PASS |
| mem_writes | `oltp_update_index` | 43.66ms | 86.85ms | 2.0× | 0.9% | PASS |
| mem_writes | `oltp_update_non_index` | 28.61ms | 52.44ms | 1.8× | 1.0% | PASS |
| mem_writes | `oltp_delete_insert` | 38.56ms | 65.06ms | 1.7× | 0.9% | PASS |
| mem_writes | `oltp_write_only` | 18.41ms | 40.50ms | 2.2× | 1.3% | PASS |
| mem_writes | `types_delete_insert` | 20.57ms | 30.30ms | 1.5× | 1.0% | PASS |
| mem_writes | `oltp_read_write` | 56.55ms | 93.95ms | 1.7× | 1.1% | PASS |
| file_reads | `oltp_point_select` | 47.23ms | 31.28ms | 0.7× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 12.69ms | 12.68ms | 1.0× | 0.7% | PASS |
| file_reads | `oltp_sum_range` | 12.07ms | 11.60ms | 1.0× | 1.0% | PASS |
| file_reads | `oltp_order_range` | 2.85ms | 3.06ms | 1.1× | 0.7% | PASS |
| file_reads | `oltp_distinct_range` | 3.71ms | 4.03ms | 1.1× | 0.9% | PASS |
| file_reads | `oltp_index_scan` | 6.42ms | 5.50ms | 0.9× | 1.0% | PASS |
| file_reads | `select_random_points` | 12.95ms | 12.26ms | 0.9× | 0.8% | PASS |
| file_reads | `select_random_ranges` | 6.79ms | 5.01ms | 0.7× | 0.9% | PASS |
| file_reads | `covering_index_scan` | 9.36ms | 9.93ms | 1.1× | 0.7% | PASS |
| file_reads | `groupby_scan` | 29.02ms | 31.59ms | 1.1× | 0.4% | PASS |
| file_reads | `index_join` | 6.87ms | 8.50ms | 1.2× | 1.0% | PASS |
| file_reads | `index_join_scan` | 3.16ms | 5.47ms | 1.7× | 1.5% | PASS |
| file_reads | `types_table_scan` | 982.02ms | 1.18s | 1.2× | 0.2% | PASS |
| file_reads | `table_scan` | 1.15s | 1.32s | 1.1× | 1.6% | PASS |
| file_reads | `oltp_read_only` | 131.10ms | 121.07ms | 0.9× | 0.5% | PASS |
| file_writes | `oltp_bulk_insert` | 191.87ms | 288.73ms | 1.5× | 1.3% | PASS |
| file_writes | `oltp_insert` | 27.29ms | 36.69ms | 1.3× | 2.6% | PASS |
| file_writes | `oltp_update_index` | 148.37ms | 147.42ms | 1.0× | 1.4% | PASS |
| file_writes | `oltp_update_non_index` | 120.84ms | 100.47ms | 0.8× | 1.1% | PASS |
| file_writes | `oltp_delete_insert` | 128.74ms | 121.79ms | 0.9× | 0.8% | PASS |
| file_writes | `oltp_write_only` | 95.65ms | 84.37ms | 0.9× | 1.3% | PASS |
| file_writes | `types_delete_insert` | 82.06ms | 64.29ms | 0.8× | 1.5% | PASS |
| file_writes | `oltp_read_write` | 132.79ms | 136.38ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_point_select` | 29.14ms | 30.89ms | 1.1× | 0.9% | PASS |
| ac_reads | `oltp_range_select` | 10.68ms | 12.56ms | 1.2× | 0.5% | PASS |
| ac_reads | `oltp_sum_range` | 10.17ms | 11.48ms | 1.1× | 0.9% | PASS |
| ac_reads | `oltp_order_range` | 2.64ms | 3.06ms | 1.2× | 0.9% | PASS |
| ac_reads | `oltp_distinct_range` | 3.54ms | 4.04ms | 1.1× | 0.9% | PASS |
| ac_reads | `oltp_index_scan` | 4.69ms | 5.47ms | 1.2× | 0.8% | PASS |
| ac_reads | `select_random_points` | 11.38ms | 12.39ms | 1.1× | 1.1% | PASS |
| ac_reads | `select_random_ranges` | 5.04ms | 5.04ms | 1.0× | 0.8% | PASS |
| ac_reads | `covering_index_scan` | 7.50ms | 9.93ms | 1.3× | 0.8% | PASS |
| ac_reads | `groupby_scan` | 28.88ms | 31.67ms | 1.1× | 0.4% | PASS |
| ac_reads | `index_join` | 5.99ms | 8.50ms | 1.4× | 1.3% | PASS |
| ac_reads | `index_join_scan` | 3.02ms | 5.43ms | 1.8× | 1.3% | PASS |
| ac_reads | `types_table_scan` | 982.55ms | 1.18s | 1.2× | 0.3% | PASS |
| ac_reads | `table_scan` | 1.14s | 1.31s | 1.2× | 0.7% | PASS |
| ac_reads | `oltp_read_only` | 104.17ms | 120.35ms | 1.2× | 0.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 27.04ms | 72.56ms | 2.7× | 2.9% | PASS |
| ac_writes | `oltp_insert_ac` | 29.24ms | 84.47ms | 2.9× | 2.3% | PASS |
| ac_writes | `oltp_update_index_ac` | 30.14ms | 93.45ms | 3.1× | 3.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 27.43ms | 77.68ms | 2.8× | 2.9% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 28.72ms | 87.48ms | 3.0× | 3.4% | PASS |
| ac_writes | `oltp_write_only_ac` | 28.71ms | 86.63ms | 3.0× | 3.0% | PASS |
| ac_writes | `types_delete_insert_ac` | 27.08ms | 80.33ms | 3.0× | 5.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.16ms | 92.41ms | 2.9× | 3.5% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 35.70ms | 41.82ms | 1.2× | 2.2% | PASS |
| mem_reads | `oltp_range_select` | 16.57ms | 16.82ms | 1.0× | 2.3% | PASS |
| mem_reads | `oltp_sum_range` | 15.50ms | 16.39ms | 1.1× | 1.8% | PASS |
| mem_reads | `oltp_order_range` | 3.26ms | 3.55ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 4.35ms | 4.70ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_index_scan` | 3.92ms | 6.93ms | 1.8× | 1.3% | PASS |
| mem_reads | `select_random_points` | 21.59ms | 23.70ms | 1.1× | 3.6% | PASS |
| mem_reads | `select_random_ranges` | 6.63ms | 7.17ms | 1.1× | 1.5% | PASS |
| mem_reads | `covering_index_scan` | 7.62ms | 11.37ms | 1.5× | 1.3% | PASS |
| mem_reads | `groupby_scan` | 34.62ms | 37.38ms | 1.1× | 0.7% | PASS |
| mem_reads | `index_join` | 10.53ms | 10.88ms | 1.0× | 2.6% | PASS |
| mem_reads | `index_join_scan` | 3.78ms | 6.82ms | 1.8× | 2.9% | PASS |
| mem_reads | `types_table_scan` | 1.12s | 1.50s | 1.3× | 2.3% | PASS |
| mem_reads | `table_scan` | 1.28s | 1.58s | 1.2× | 2.1% | PASS |
| mem_reads | `oltp_read_only` | 145.77ms | 156.70ms | 1.1× | 1.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 238.20ms | 372.73ms | 1.6× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 17.77ms | 38.54ms | 2.2× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 67.15ms | 147.19ms | 2.2× | 1.9% | PASS |
| mem_writes | `oltp_update_non_index` | 49.62ms | 85.03ms | 1.7× | 2.4% | PASS |
| mem_writes | `oltp_delete_insert` | 56.40ms | 110.45ms | 2.0× | 2.2% | PASS |
| mem_writes | `oltp_write_only` | 28.37ms | 57.76ms | 2.0× | 1.8% | PASS |
| mem_writes | `types_delete_insert` | 40.16ms | 62.95ms | 1.6× | 1.6% | PASS |
| mem_writes | `oltp_read_write` | 102.07ms | 145.20ms | 1.4× | 1.7% | PASS |
| file_reads | `oltp_point_select` | 102.16ms | 58.88ms | 0.6× | 1.4% | PASS |
| file_reads | `oltp_range_select` | 23.67ms | 18.75ms | 0.8× | 2.4% | PASS |
| file_reads | `oltp_sum_range` | 23.17ms | 18.37ms | 0.8× | 2.0% | PASS |
| file_reads | `oltp_order_range` | 4.10ms | 3.78ms | 0.9× | 1.8% | PASS |
| file_reads | `oltp_distinct_range` | 5.22ms | 4.95ms | 0.9× | 1.1% | PASS |
| file_reads | `oltp_index_scan` | 10.68ms | 8.80ms | 0.8× | 1.8% | PASS |
| file_reads | `select_random_points` | 30.11ms | 26.48ms | 0.9× | 2.5% | PASS |
| file_reads | `select_random_ranges` | 14.01ms | 9.17ms | 0.7× | 1.8% | PASS |
| file_reads | `covering_index_scan` | 14.68ms | 13.11ms | 0.9× | 1.6% | PASS |
| file_reads | `groupby_scan` | 35.56ms | 37.64ms | 1.1× | 0.9% | PASS |
| file_reads | `index_join` | 14.83ms | 12.21ms | 0.8× | 2.4% | PASS |
| file_reads | `index_join_scan` | 4.73ms | 7.23ms | 1.5× | 2.3% | PASS |
| file_reads | `types_table_scan` | 1.10s | 1.46s | 1.3× | 1.7% | PASS |
| file_reads | `table_scan` | 1.22s | 1.56s | 1.3× | 1.2% | PASS |
| file_reads | `oltp_read_only` | 239.88ms | 181.47ms | 0.8× | 1.5% | PASS |
| file_writes | `oltp_bulk_insert` | 263.64ms | 390.16ms | 1.5× | 0.8% | PASS |
| file_writes | `oltp_insert` | 25.51ms | 44.20ms | 1.7× | 1.5% | PASS |
| file_writes | `oltp_update_index` | 130.52ms | 166.81ms | 1.3× | 9.6% | PASS |
| file_writes | `oltp_update_non_index` | 99.70ms | 99.25ms | 1.0× | 12.1% | PASS |
| file_writes | `oltp_delete_insert` | 95.41ms | 124.29ms | 1.3× | 1.7% | PASS |
| file_writes | `oltp_write_only` | 86.66ms | 70.09ms | 0.8× | 12.0% | PASS |
| file_writes | `types_delete_insert` | 72.51ms | 71.36ms | 1.0× | 1.6% | PASS |
| file_writes | `oltp_read_write` | 148.90ms | 158.16ms | 1.1× | 6.1% | PASS |
| ac_reads | `oltp_point_select` | 59.17ms | 59.53ms | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_range_select` | 20.25ms | 19.03ms | 0.9× | 1.4% | PASS |
| ac_reads | `oltp_sum_range` | 18.84ms | 18.47ms | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_order_range` | 3.70ms | 3.77ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_distinct_range` | 4.79ms | 4.94ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_index_scan` | 6.60ms | 8.82ms | 1.3× | 1.0% | PASS |
| ac_reads | `select_random_points` | 26.48ms | 26.70ms | 1.0× | 2.1% | PASS |
| ac_reads | `select_random_ranges` | 9.21ms | 9.09ms | 1.0× | 1.4% | PASS |
| ac_reads | `covering_index_scan` | 10.32ms | 13.07ms | 1.3× | 1.5% | PASS |
| ac_reads | `groupby_scan` | 34.80ms | 37.52ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 12.24ms | 12.01ms | 1.0× | 1.9% | PASS |
| ac_reads | `index_join_scan` | 4.21ms | 7.14ms | 1.7× | 1.9% | PASS |
| ac_reads | `types_table_scan` | 1.07s | 1.45s | 1.3× | 0.9% | PASS |
| ac_reads | `table_scan` | 1.31s | 1.57s | 1.2× | 1.6% | PASS |
| ac_reads | `oltp_read_only` | 176.70ms | 181.43ms | 1.0× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.39ms | 62.44ms | 2.9× | 4.5% | PASS |
| ac_writes | `oltp_insert_ac` | 23.82ms | 73.86ms | 3.1× | 5.1% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.06ms | 88.94ms | 3.4× | 4.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 20.77ms | 69.53ms | 3.3× | 6.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.20ms | 81.81ms | 3.4× | 6.0% | PASS |
| ac_writes | `oltp_write_only_ac` | 23.25ms | 78.62ms | 3.4× | 5.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 21.97ms | 70.19ms | 3.2× | 7.0% | PASS |
| ac_writes | `oltp_read_write_ac` | 30.05ms | 85.87ms | 2.9× | 5.6% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 34.14ms | 41.50ms | 1.2× | 2.1% | PASS |
| mem_reads | `oltp_range_select` | 15.99ms | 16.57ms | 1.0× | 1.5% | PASS |
| mem_reads | `oltp_sum_range` | 14.37ms | 16.01ms | 1.1× | 2.0% | PASS |
| mem_reads | `oltp_order_range` | 3.11ms | 3.50ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_distinct_range` | 4.21ms | 4.67ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 3.87ms | 6.84ms | 1.8× | 1.8% | PASS |
| mem_reads | `select_random_points` | 20.04ms | 22.79ms | 1.1× | 1.1% | PASS |
| mem_reads | `select_random_ranges` | 6.31ms | 7.16ms | 1.1× | 1.1% | PASS |
| mem_reads | `covering_index_scan` | 7.61ms | 11.28ms | 1.5× | 1.0% | PASS |
| mem_reads | `groupby_scan` | 33.33ms | 36.66ms | 1.1× | 0.8% | PASS |
| mem_reads | `index_join` | 9.59ms | 10.94ms | 1.1× | 1.3% | PASS |
| mem_reads | `index_join_scan` | 3.52ms | 6.44ms | 1.8× | 1.5% | PASS |
| mem_reads | `types_table_scan` | 1.04s | 1.44s | 1.4× | 0.5% | PASS |
| mem_reads | `table_scan` | 1.17s | 1.53s | 1.3× | 0.5% | PASS |
| mem_reads | `oltp_read_only` | 131.07ms | 151.80ms | 1.2× | 0.9% | PASS |
| mem_writes | `oltp_bulk_insert` | 239.96ms | 375.00ms | 1.6× | 1.2% | PASS |
| mem_writes | `oltp_insert` | 18.25ms | 38.45ms | 2.1× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 62.49ms | 137.80ms | 2.2× | 1.3% | PASS |
| mem_writes | `oltp_update_non_index` | 46.20ms | 80.41ms | 1.7× | 0.9% | PASS |
| mem_writes | `oltp_delete_insert` | 51.40ms | 105.00ms | 2.0× | 0.9% | PASS |
| mem_writes | `oltp_write_only` | 27.13ms | 56.06ms | 2.1× | 1.0% | PASS |
| mem_writes | `types_delete_insert` | 36.72ms | 59.81ms | 1.6× | 1.0% | PASS |
| mem_writes | `oltp_read_write` | 92.91ms | 141.53ms | 1.5× | 1.1% | PASS |
| file_reads | `oltp_point_select` | 98.72ms | 58.85ms | 0.6× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 22.00ms | 18.37ms | 0.8× | 1.9% | PASS |
| file_reads | `oltp_sum_range` | 21.27ms | 18.03ms | 0.8× | 1.4% | PASS |
| file_reads | `oltp_order_range` | 4.00ms | 3.76ms | 0.9× | 1.9% | PASS |
| file_reads | `oltp_distinct_range` | 5.15ms | 4.91ms | 1.0× | 1.5% | PASS |
| file_reads | `oltp_index_scan` | 10.54ms | 8.70ms | 0.8× | 1.3% | PASS |
| file_reads | `select_random_points` | 28.79ms | 25.56ms | 0.9× | 2.1% | PASS |
| file_reads | `select_random_ranges` | 13.10ms | 9.10ms | 0.7× | 1.6% | PASS |
| file_reads | `covering_index_scan` | 14.56ms | 13.15ms | 0.9× | 0.9% | PASS |
| file_reads | `groupby_scan` | 34.48ms | 36.97ms | 1.1× | 0.7% | PASS |
| file_reads | `index_join` | 13.74ms | 12.14ms | 0.9× | 1.8% | PASS |
| file_reads | `index_join_scan` | 4.54ms | 6.88ms | 1.5× | 1.7% | PASS |
| file_reads | `types_table_scan` | 1.04s | 1.41s | 1.4× | 0.5% | PASS |
| file_reads | `table_scan` | 1.18s | 1.54s | 1.3× | 0.7% | PASS |
| file_reads | `oltp_read_only` | 232.51ms | 179.87ms | 0.8× | 1.1% | PASS |
| file_writes | `oltp_bulk_insert` | 261.28ms | 391.43ms | 1.5× | 0.8% | PASS |
| file_writes | `oltp_insert` | 25.45ms | 43.61ms | 1.7× | 1.3% | PASS |
| file_writes | `oltp_update_index` | 97.43ms | 156.98ms | 1.6× | 2.0% | PASS |
| file_writes | `oltp_update_non_index` | 86.38ms | 93.94ms | 1.1× | 5.9% | PASS |
| file_writes | `oltp_delete_insert` | 86.37ms | 121.51ms | 1.4× | 1.6% | PASS |
| file_writes | `oltp_write_only` | 57.41ms | 69.04ms | 1.2× | 2.0% | PASS |
| file_writes | `types_delete_insert` | 63.05ms | 67.91ms | 1.1× | 2.4% | PASS |
| file_writes | `oltp_read_write` | 126.74ms | 154.52ms | 1.2× | 1.7% | PASS |
| ac_reads | `oltp_point_select` | 56.94ms | 58.56ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_range_select` | 18.61ms | 18.43ms | 1.0× | 2.1% | PASS |
| ac_reads | `oltp_sum_range` | 17.49ms | 18.15ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_order_range` | 3.73ms | 3.74ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_distinct_range` | 4.74ms | 4.92ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_index_scan` | 6.42ms | 8.71ms | 1.4× | 1.6% | PASS |
| ac_reads | `select_random_points` | 24.24ms | 25.37ms | 1.0× | 1.6% | PASS |
| ac_reads | `select_random_ranges` | 9.03ms | 9.14ms | 1.0× | 1.1% | PASS |
| ac_reads | `covering_index_scan` | 10.24ms | 13.18ms | 1.3× | 1.1% | PASS |
| ac_reads | `groupby_scan` | 34.36ms | 36.99ms | 1.1× | 0.7% | PASS |
| ac_reads | `index_join` | 11.92ms | 12.14ms | 1.0× | 2.1% | PASS |
| ac_reads | `index_join_scan` | 4.13ms | 6.86ms | 1.7× | 1.1% | PASS |
| ac_reads | `types_table_scan` | 1.04s | 1.42s | 1.4× | 0.6% | PASS |
| ac_reads | `table_scan` | 1.18s | 1.53s | 1.3× | 0.7% | PASS |
| ac_reads | `oltp_read_only` | 169.00ms | 179.37ms | 1.1× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.97ms | 64.50ms | 2.9× | 4.8% | PASS |
| ac_writes | `oltp_insert_ac` | 24.48ms | 80.62ms | 3.3× | 5.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.88ms | 93.86ms | 3.5× | 6.4% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.45ms | 75.06ms | 3.5× | 6.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.49ms | 83.62ms | 3.3× | 5.7% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.26ms | 81.94ms | 3.2× | 4.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.41ms | 75.94ms | 3.4× | 9.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.24ms | 89.41ms | 2.9× | 4.8% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 27.98ms | 33.45ms | 1.2× | 1.0% | PASS |
| mem_reads | `oltp_range_select` | 17.33ms | 21.10ms | 1.2× | 1.6% | PASS |
| mem_reads | `oltp_sum_range` | 15.87ms | 19.00ms | 1.2× | 1.2% | PASS |
| mem_reads | `oltp_order_range` | 3.25ms | 3.84ms | 1.2× | 1.4% | PASS |
| mem_reads | `oltp_distinct_range` | 4.29ms | 5.00ms | 1.2× | 1.7% | PASS |
| mem_reads | `oltp_index_scan` | 4.09ms | 5.74ms | 1.4× | 2.6% | PASS |
| mem_reads | `select_random_points` | 26.74ms | 30.95ms | 1.2× | 1.4% | PASS |
| mem_reads | `select_random_ranges` | 6.55ms | 8.03ms | 1.2× | 0.7% | PASS |
| mem_reads | `covering_index_scan` | 6.50ms | 9.67ms | 1.5× | 1.4% | PASS |
| mem_reads | `groupby_scan` | 33.62ms | 37.65ms | 1.1× | 1.1% | PASS |
| mem_reads | `index_join` | 6.89ms | 10.68ms | 1.6× | 2.3% | PASS |
| mem_reads | `index_join_scan` | 3.39ms | 5.95ms | 1.8× | 2.9% | PASS |
| mem_reads | `types_table_scan` | 1.01s | 1.26s | 1.2× | 0.5% | PASS |
| mem_reads | `table_scan` | 1.22s | 1.39s | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_read_only` | 135.22ms | 163.39ms | 1.2× | 1.2% | PASS |
| mem_writes | `oltp_bulk_insert` | 200.19ms | 286.04ms | 1.4× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 16.38ms | 29.28ms | 1.8× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 58.44ms | 116.06ms | 2.0× | 1.4% | PASS |
| mem_writes | `oltp_update_non_index` | 40.67ms | 70.20ms | 1.7× | 1.5% | PASS |
| mem_writes | `oltp_delete_insert` | 40.10ms | 76.00ms | 1.9× | 1.7% | PASS |
| mem_writes | `oltp_write_only` | 21.87ms | 45.97ms | 2.1× | 1.8% | PASS |
| mem_writes | `types_delete_insert` | 26.33ms | 45.22ms | 1.7× | 1.9% | PASS |
| mem_writes | `oltp_read_write` | 78.50ms | 125.91ms | 1.6× | 2.0% | PASS |
| file_reads | `oltp_point_select` | 52.97ms | 38.18ms | 0.7× | 1.7% | PASS |
| file_reads | `oltp_range_select` | 20.35ms | 22.14ms | 1.1× | 1.5% | PASS |
| file_reads | `oltp_sum_range` | 18.95ms | 20.07ms | 1.1× | 1.8% | PASS |
| file_reads | `oltp_order_range` | 3.69ms | 4.04ms | 1.1× | 2.5% | PASS |
| file_reads | `oltp_distinct_range` | 4.71ms | 5.16ms | 1.1× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 7.09ms | 6.50ms | 0.9× | 1.3% | PASS |
| file_reads | `select_random_points` | 29.18ms | 31.09ms | 1.1× | 1.3% | PASS |
| file_reads | `select_random_ranges` | 9.36ms | 8.66ms | 0.9× | 1.9% | PASS |
| file_reads | `covering_index_scan` | 9.43ms | 10.36ms | 1.1× | 2.4% | PASS |
| file_reads | `groupby_scan` | 34.63ms | 38.20ms | 1.1× | 0.9% | PASS |
| file_reads | `index_join` | 8.89ms | 11.48ms | 1.3× | 1.8% | PASS |
| file_reads | `index_join_scan` | 3.88ms | 6.25ms | 1.6× | 2.0% | PASS |
| file_reads | `types_table_scan` | 1.04s | 1.30s | 1.3× | 0.8% | PASS |
| file_reads | `table_scan` | 1.28s | 1.45s | 1.1× | 1.1% | PASS |
| file_reads | `oltp_read_only` | 177.49ms | 174.32ms | 1.0× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 255.97ms | 343.34ms | 1.3× | 1.6% | PASS |
| file_writes | `oltp_insert` | 30.60ms | 46.14ms | 1.5× | 2.3% | PASS |
| file_writes | `oltp_update_index` | 163.84ms | 182.85ms | 1.1× | 2.7% | PASS |
| file_writes | `oltp_update_non_index` | 136.40ms | 121.92ms | 0.9× | 0.6% | PASS |
| file_writes | `oltp_delete_insert` | 134.07ms | 142.05ms | 1.1× | 1.2% | PASS |
| file_writes | `oltp_write_only` | 96.64ms | 94.73ms | 1.0× | 2.8% | PASS |
| file_writes | `types_delete_insert` | 88.00ms | 79.21ms | 0.9× | 2.6% | PASS |
| file_writes | `oltp_read_write` | 156.37ms | 173.88ms | 1.1× | 1.8% | PASS |
| ac_reads | `oltp_point_select` | 36.32ms | 39.05ms | 1.1× | 1.6% | PASS |
| ac_reads | `oltp_range_select` | 18.29ms | 21.87ms | 1.2× | 1.8% | PASS |
| ac_reads | `oltp_sum_range` | 17.19ms | 20.11ms | 1.2× | 1.5% | PASS |
| ac_reads | `oltp_order_range` | 3.48ms | 4.00ms | 1.1× | 1.7% | PASS |
| ac_reads | `oltp_distinct_range` | 4.35ms | 5.02ms | 1.2× | 2.0% | PASS |
| ac_reads | `oltp_index_scan` | 5.25ms | 6.41ms | 1.2× | 1.8% | PASS |
| ac_reads | `select_random_points` | 27.02ms | 30.73ms | 1.1× | 1.9% | PASS |
| ac_reads | `select_random_ranges` | 7.48ms | 8.46ms | 1.1× | 3.1% | PASS |
| ac_reads | `covering_index_scan` | 7.67ms | 10.49ms | 1.4× | 2.2% | PASS |
| ac_reads | `groupby_scan` | 34.04ms | 37.59ms | 1.1× | 1.1% | PASS |
| ac_reads | `index_join` | 7.94ms | 11.49ms | 1.4× | 1.9% | PASS |
| ac_reads | `index_join_scan` | 3.74ms | 6.29ms | 1.7× | 1.8% | PASS |
| ac_reads | `types_table_scan` | 1.05s | 1.29s | 1.2× | 1.2% | PASS |
| ac_reads | `table_scan` | 1.23s | 1.40s | 1.1× | 1.9% | PASS |
| ac_reads | `oltp_read_only` | 144.26ms | 166.88ms | 1.2× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 26.20ms | 67.29ms | 2.6× | 3.0% | PASS |
| ac_writes | `oltp_insert_ac` | 27.92ms | 78.59ms | 2.8× | 2.1% | PASS |
| ac_writes | `oltp_update_index_ac` | 29.24ms | 86.98ms | 3.0× | 3.0% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 26.30ms | 74.22ms | 2.8× | 3.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 27.54ms | 80.04ms | 2.9× | 2.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 27.60ms | 80.95ms | 2.9× | 2.9% | PASS |
| ac_writes | `types_delete_insert_ac` | 26.08ms | 75.62ms | 2.9× | 4.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.61ms | 87.29ms | 2.7× | 3.2% | PASS |

</details>

</details>

## Version-control latency

Wall time: 4m 33s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 35.40ms | 38.00ms | 93.2% | 0.7% | PASS |
| `status_dirty_many_tables` | 39.08ms | 42.00ms | 93.1% | 0.7% | PASS |
| `diff_regular_working_one_table` | 30.94ms | 33.00ms | 93.8% | 0.5% | PASS |
| `diff_regular_working_many_tables` | 44.75ms | 50.00ms | 89.5% | 0.5% | PASS |
| `diff_stat_working_many_tables` | 44.50ms | 48.00ms | 92.7% | 0.4% | PASS |
| `diff_schema_working_many_tables` | 44.98ms | 48.00ms | 93.7% | 0.5% | PASS |
| `branch_list_many_branches` | 23.25ms | 25.00ms | 93.0% | 1.0% | PASS |
| `branch_create_delete` | 24.88ms | 27.00ms | 92.2% | 0.9% | PASS |
| `at_literal_deep_history` | 25.00ms | 28.00ms | 89.3% | 0.6% | PASS |
| `diff_literal_deep_history` | 25.04ms | 28.00ms | 89.4% | 0.5% | PASS |
| `history_literal_deep_history` | 26.21ms | 30.00ms | 87.4% | 0.8% | PASS |
| `checkout_branch_clean` | 34.31ms | 43.00ms | 79.8% | 0.6% | PASS |
| `merge_data_no_conflicts` | 28.29ms | 33.00ms | 85.7% | 0.7% | PASS |
| `merge_data_secondary_index` | 912.88ms | 944.00ms | 96.7% | 0.3% | PASS |
| `merge_schema_no_conflicts` | 21.67ms | 24.00ms | 90.3% | 0.9% | PASS |
| `merge_data_conflicts` | 31.11ms | 33.00ms | 94.3% | 0.5% | PASS |
| `merge_data_conflicts_with_resolve` | 32.04ms | 34.00ms | 94.2% | 1.0% | PASS |

Version-control ceiling result: **PASS**.

## File remote performance

Status: passed. Transport: local file-backed remote.

100,000,000 logical payload bytes; 100,000 rows; 1,000 changed rows; 5 repetitions per operation and binary.

SQL timings exclude fixture generation, copies, database open and verification. Each SQL sample uses a fresh process with the OS cache primed by setup verification. SQL client cache: 65,536 KiB. copy_fsync times shutil.copyfile plus fsync.

Throughput uses logical payload bytes; destination growth is file-size change, not transferred bytes.

Nightly median ceilings require three-attempt confirmation; copy_fsync is a control.

| Operation | Ceiling ms | Result |
|---|---:|---|
| push_initial | 2000 | PASS |
| clone_initial | 3000 | PASS |
| push_noop | 10 | PASS |
| fetch_noop | 10 | PASS |
| push_incremental | 750 | PASS |
| fetch_incremental | 300 | PASS |
| pull_fast_forward | 300 | PASS |

Timing gate: PASS after 1 attempt(s).

candidate: 18fcc0f.

Fixture file bytes: base 107,968,266; delta 115,878,997.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 58.961 | 57.427–62.924 | 38.031 | 1696.04 | 107968266 |
| push_initial | candidate | 587.332 | 581.685–593.554 | 572.250 | 170.26 | 107956772 |
| clone_initial | candidate | 768.405 | 765.811–794.658 | 741.454 | 130.14 | 108475035 |
| push_noop | candidate | 0.652 | 0.605–1.603 | 0.375 | — | 0 |
| fetch_noop | candidate | 0.216 | 0.191–0.231 | 0.218 | — | 0 |
| push_incremental | candidate | 237.720 | 237.146–240.049 | 234.568 | 4.21 | 7909775 |
| fetch_incremental | candidate | 98.746 | 96.945–100.354 | 95.618 | 10.13 | 7910084 |
| pull_fast_forward | candidate | 83.119 | 82.600–83.979 | 80.107 | 12.03 | 7910844 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 90.36 | 175.487 | 108.763 | 18329 | 2066 |
| clone_initial | candidate | 90.86 | 295.074 | 109.784 | 47238 | 2337 |
| push_noop | candidate | 42.24 | 0.075 | 0.000 | 27 | 2 |
| fetch_noop | candidate | 42.24 | 0.135 | 0.000 | 40 | 0 |
| push_incremental | candidate | 42.24 | 96.891 | 7.910 | 22417 | 125 |
| fetch_incremental | candidate | 42.24 | 34.472 | 7.910 | 10808 | 124 |
| pull_fast_forward | candidate | 42.24 | 26.497 | 7.911 | 7031 | 126 |

</details>


<details>
<summary>1 GB file remotes (observational)</summary>

Both database sizes use a fixed 1 MB scattered update and a warm OS cache. 1 GB timings have no ceiling while a baseline is collected.

### 1 GB file remote performance

Status: passed. Transport: local file-backed remote.

1,000,000,000 logical payload bytes; 1,000,000 rows; 1,000 changed rows; 3 repetitions per operation and binary.

SQL timings exclude fixture generation, copies, database open and verification. Each SQL sample uses a fresh process with the OS cache primed by setup verification. SQL client cache: 65,536 KiB. copy_fsync times shutil.copyfile plus fsync.

Throughput uses logical payload bytes; destination growth is file-size change, not transferred bytes.

No speed thresholds are enforced.

candidate: 18fcc0f.

Fixture file bytes: base 1,087,718,031; delta 1,103,525,159.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 2687.362 | 2651.978–2692.441 | 397.225 | 372.11 | 1087718031 |
| push_initial | candidate | 8146.801 | 8121.742–8154.893 | 5924.909 | 122.75 | 1079566275 |
| clone_initial | candidate | 8976.589 | 8864.956–9104.185 | 8739.194 | 111.40 | 1102407326 |
| push_noop | candidate | 0.784 | 0.735–0.839 | 0.370 | — | 0 |
| fetch_noop | candidate | 0.207 | 0.207–0.209 | 0.210 | — | 0 |
| push_incremental | candidate | 1341.111 | 1335.925–1353.774 | 1335.970 | 0.75 | 15806003 |
| fetch_incremental | candidate | 289.292 | 286.332–291.858 | 283.890 | 3.46 | 15806513 |
| pull_fast_forward | candidate | 292.076 | 286.389–309.887 | 284.548 | 3.42 | 15807273 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 134.65 | 2091.695 | 1087.622 | 263453 | 20541 |
| clone_initial | candidate | 123.95 | 3517.395 | 1132.819 | 612238 | 32243 |
| push_noop | candidate | 44.96 | 0.078 | 0.000 | 27 | 2 |
| fetch_noop | candidate | 44.96 | 0.137 | 0.000 | 40 | 0 |
| push_incremental | candidate | 48.10 | 616.926 | 15.928 | 149852 | 313 |
| fetch_incremental | candidate | 50.32 | 62.270 | 15.928 | 13904 | 313 |
| pull_fast_forward | candidate | 50.30 | 62.203 | 15.929 | 13900 | 315 |

</details>


</details>

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh`, `test/vc_perf_ceiling.sh`, and `test/remote_perf.py`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
