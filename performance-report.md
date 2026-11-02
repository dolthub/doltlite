# DoltLite Performance Report

> Nightly result: **FAIL**
>
> Generated: 2026-11-02 11:36 UTC
>
> Commit: [`ebeaa210255b46495419a96dd8a303f3cd41542b`](https://github.com/dolthub/doltlite/commit/ebeaa210255b46495419a96dd8a303f3cd41542b)
>
> Runner: ubuntu24 20261026.354.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/41421428254)

The SQL workloads compare optimized DoltLite against stock SQLite on the same GitHub-hosted runner, alternating execution order on each repetition. Version-control sections measure DoltLite latency. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.60s | 11.22s | 1.2× | 2.0% | **PASS** |
| Writes | 2.09s | 3.67s | 1.8× | 1.5% | **FAIL** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10.52s | 11.54s | 1.1× | 1.6% | **PASS** |
| Writes | 3.35s | 4.22s | 1.3× | 2.1% | **PASS** |
| Autocommit writes | 782.82ms | 2.45s | 3.1× | 6.7% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 5.5× and 5.0× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 1.49s | 1.68s | 1.1× | 1.7% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.75s | 3.17s | 1.2× | 2.3% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 2.71s | 3.17s | 1.2× | 2.2% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 2.64s | 3.20s | 1.2× | 1.3% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 256.03ms | 459.22ms | 1.8× | 1.3% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 609.12ms | 1.07s | 1.8× | 1.9% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 619.77ms | 1.06s | 1.7× | 1.5% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 603.59ms | 1.07s | 1.8× | 1.3% | **FAIL** |
| File-backed | Reads | int | 15 | 55 | 1.76s | 1.84s | 1.0× | 1.3% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 3.09s | 3.26s | 1.1× | 1.7% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 2.74s | 3.18s | 1.2× | 1.6% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 2.93s | 3.26s | 1.1× | 1.7% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 738.56ms | 746.29ms | 1.0× | 3.3% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 956.96ms | 1.17s | 1.2× | 4.0% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 882.98ms | 1.19s | 1.3× | 2.4% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 772.82ms | 1.12s | 1.4× | 1.6% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 1.55s | 1.72s | 1.1× | 1.9% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.77s | 3.23s | 1.2× | 1.9% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 2.72s | 3.22s | 1.2× | 2.4% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 2.72s | 3.25s | 1.2× | 1.7% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 161.60ms | 463.62ms | 2.9× | 3.5% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 202.12ms | 641.42ms | 3.2× | 6.8% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 218.84ms | 699.97ms | 3.2× | 7.7% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 200.26ms | 648.93ms | 3.2× | 6.9% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 14.19ms | 18.49ms | 1.3× | 2.0% | PASS |
| mem_reads | `oltp_range_select` | 5.54ms | 6.94ms | 1.3× | 3.6% | PASS |
| mem_reads | `oltp_sum_range` | 5.27ms | 6.90ms | 1.3× | 3.0% | PASS |
| mem_reads | `oltp_order_range` | 1.52ms | 1.74ms | 1.1× | 2.3% | PASS |
| mem_reads | `oltp_distinct_range` | 2.01ms | 2.20ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_index_scan` | 2.29ms | 3.23ms | 1.4× | 2.8% | PASS |
| mem_reads | `select_random_points` | 8.02ms | 7.87ms | 1.0× | 3.8% | PASS |
| mem_reads | `select_random_ranges` | 2.61ms | 3.07ms | 1.2× | 1.5% | PASS |
| mem_reads | `covering_index_scan` | 3.85ms | 5.32ms | 1.4× | 1.3% | PASS |
| mem_reads | `groupby_scan` | 16.46ms | 17.79ms | 1.1× | 0.9% | PASS |
| mem_reads | `index_join` | 3.36ms | 4.94ms | 1.5× | 1.7% | PASS |
| mem_reads | `index_join_scan` | 2.21ms | 3.97ms | 1.8× | 4.5% | PASS |
| mem_reads | `types_table_scan` | 644.89ms | 728.54ms | 1.1× | 0.8% | PASS |
| mem_reads | `table_scan` | 727.55ms | 804.92ms | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_read_only` | 53.61ms | 64.34ms | 1.2× | 1.5% | PASS |
| mem_writes | `oltp_bulk_insert` | 103.78ms | 182.31ms | 1.8× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 8.90ms | 15.72ms | 1.8× | 1.2% | PASS |
| mem_writes | `oltp_update_index` | 30.41ms | 60.16ms | 2.0× | 1.2% | PASS |
| mem_writes | `oltp_update_non_index` | 21.68ms | 39.66ms | 1.8× | 1.2% | PASS |
| mem_writes | `oltp_delete_insert` | 26.82ms | 46.95ms | 1.8× | 1.7% | PASS |
| mem_writes | `oltp_write_only` | 13.30ms | 29.70ms | 2.2× | 2.2% | PASS |
| mem_writes | `types_delete_insert` | 15.08ms | 22.21ms | 1.5× | 2.3% | PASS |
| mem_writes | `oltp_read_write` | 36.06ms | 62.52ms | 1.7× | 1.4% | PASS |
| file_reads | `oltp_point_select` | 62.13ms | 30.96ms | 0.5× | 1.2% | PASS |
| file_reads | `oltp_range_select` | 10.89ms | 8.26ms | 0.8× | 1.3% | PASS |
| file_reads | `oltp_sum_range` | 10.48ms | 8.23ms | 0.8× | 1.5% | PASS |
| file_reads | `oltp_order_range` | 2.12ms | 1.92ms | 0.9× | 1.1% | PASS |
| file_reads | `oltp_distinct_range` | 2.61ms | 2.38ms | 0.9× | 1.2% | PASS |
| file_reads | `oltp_index_scan` | 7.48ms | 4.98ms | 0.7× | 1.8% | PASS |
| file_reads | `select_random_points` | 13.75ms | 9.40ms | 0.7× | 2.1% | PASS |
| file_reads | `select_random_ranges` | 7.57ms | 4.42ms | 0.6× | 1.6% | PASS |
| file_reads | `covering_index_scan` | 9.15ms | 7.10ms | 0.8× | 1.9% | PASS |
| file_reads | `groupby_scan` | 18.20ms | 18.89ms | 1.0× | 0.8% | PASS |
| file_reads | `index_join` | 6.73ms | 6.80ms | 1.0× | 3.1% | PASS |
| file_reads | `index_join_scan` | 2.95ms | 4.51ms | 1.5× | 2.4% | PASS |
| file_reads | `types_table_scan` | 691.68ms | 780.09ms | 1.1× | 0.7% | PASS |
| file_reads | `table_scan` | 780.49ms | 862.21ms | 1.1× | 0.8% | PASS |
| file_reads | `oltp_read_only` | 129.81ms | 87.58ms | 0.7× | 0.9% | PASS |
| file_writes | `oltp_bulk_insert` | 150.71ms | 232.73ms | 1.5× | 1.7% | PASS |
| file_writes | `oltp_insert` | 19.79ms | 27.55ms | 1.4× | 0.7% | PASS |
| file_writes | `oltp_update_index` | 121.51ms | 108.80ms | 0.9× | 3.7% | PASS |
| file_writes | `oltp_update_non_index` | 101.11ms | 79.16ms | 0.8× | 2.5% | PASS |
| file_writes | `oltp_delete_insert` | 102.38ms | 94.90ms | 0.9× | 3.5% | PASS |
| file_writes | `oltp_write_only` | 81.23ms | 63.27ms | 0.8× | 5.0% | PASS |
| file_writes | `types_delete_insert` | 60.87ms | 47.24ms | 0.8× | 13.2% | PASS |
| file_writes | `oltp_read_write` | 100.95ms | 92.63ms | 0.9× | 3.1% | PASS |
| ac_reads | `oltp_point_select` | 31.50ms | 32.08ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_range_select` | 7.68ms | 8.32ms | 1.1× | 1.7% | PASS |
| ac_reads | `oltp_sum_range` | 7.38ms | 8.23ms | 1.1× | 1.8% | PASS |
| ac_reads | `oltp_order_range` | 1.79ms | 1.91ms | 1.1× | 2.6% | PASS |
| ac_reads | `oltp_distinct_range` | 2.27ms | 2.38ms | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_index_scan` | 4.29ms | 4.92ms | 1.1× | 3.2% | PASS |
| ac_reads | `select_random_points` | 10.90ms | 9.79ms | 0.9× | 2.7% | PASS |
| ac_reads | `select_random_ranges` | 4.42ms | 4.41ms | 1.0× | 2.0% | PASS |
| ac_reads | `covering_index_scan` | 5.94ms | 7.17ms | 1.2× | 2.4% | PASS |
| ac_reads | `groupby_scan` | 17.69ms | 18.80ms | 1.1× | 1.2% | PASS |
| ac_reads | `index_join` | 4.79ms | 6.47ms | 1.3× | 4.3% | PASS |
| ac_reads | `index_join_scan` | 2.48ms | 4.21ms | 1.7× | 3.8% | PASS |
| ac_reads | `types_table_scan` | 645.85ms | 727.09ms | 1.1× | 0.7% | PASS |
| ac_reads | `table_scan` | 725.92ms | 800.32ms | 1.1× | 0.7% | PASS |
| ac_reads | `oltp_read_only` | 78.10ms | 82.19ms | 1.1× | 1.1% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 19.03ms | 49.85ms | 2.6× | 5.0% | PASS |
| ac_writes | `oltp_insert_ac` | 20.50ms | 57.11ms | 2.8× | 2.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 21.11ms | 64.31ms | 3.0× | 2.7% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 18.75ms | 53.30ms | 2.8× | 3.7% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 20.28ms | 59.48ms | 2.9× | 3.4% | PASS |
| ac_writes | `oltp_write_only_ac` | 20.50ms | 60.71ms | 3.0× | 3.9% | PASS |
| ac_writes | `types_delete_insert_ac` | 18.71ms | 56.02ms | 3.0× | 5.3% | PASS |
| ac_writes | `oltp_read_write_ac` | 22.71ms | 62.84ms | 2.8× | 3.1% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 36.73ms | 44.95ms | 1.2× | 2.3% | PASS |
| mem_reads | `oltp_range_select` | 17.20ms | 18.08ms | 1.1× | 1.9% | PASS |
| mem_reads | `oltp_sum_range` | 15.64ms | 16.41ms | 1.0× | 2.6% | PASS |
| mem_reads | `oltp_order_range` | 3.31ms | 3.61ms | 1.1× | 2.3% | PASS |
| mem_reads | `oltp_distinct_range` | 4.44ms | 4.73ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_index_scan` | 4.09ms | 7.40ms | 1.8× | 2.2% | PASS |
| mem_reads | `select_random_points` | 23.71ms | 24.39ms | 1.0× | 3.5% | PASS |
| mem_reads | `select_random_ranges` | 6.85ms | 7.63ms | 1.1× | 2.0% | PASS |
| mem_reads | `covering_index_scan` | 7.80ms | 11.80ms | 1.5× | 1.2% | PASS |
| mem_reads | `groupby_scan` | 34.62ms | 36.86ms | 1.1× | 0.8% | PASS |
| mem_reads | `index_join` | 10.81ms | 11.34ms | 1.0× | 2.5% | PASS |
| mem_reads | `index_join_scan` | 3.93ms | 7.01ms | 1.8× | 2.5% | PASS |
| mem_reads | `types_table_scan` | 1.13s | 1.36s | 1.2× | 3.3% | PASS |
| mem_reads | `table_scan` | 1.31s | 1.46s | 1.1× | 4.2% | PASS |
| mem_reads | `oltp_read_only` | 148.29ms | 161.54ms | 1.1× | 2.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 244.28ms | 404.26ms | 1.7× | 0.7% | PASS |
| mem_writes | `oltp_insert` | 18.09ms | 40.12ms | 2.2× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 67.51ms | 149.51ms | 2.2× | 1.8% | PASS |
| mem_writes | `oltp_update_non_index` | 50.53ms | 87.84ms | 1.7× | 1.7% | PASS |
| mem_writes | `oltp_delete_insert` | 56.14ms | 113.62ms | 2.0× | 2.3% | PASS |
| mem_writes | `oltp_write_only` | 28.70ms | 59.13ms | 2.1× | 2.0% | PASS |
| mem_writes | `types_delete_insert` | 40.74ms | 65.44ms | 1.6× | 2.0% | PASS |
| mem_writes | `oltp_read_write` | 103.12ms | 150.83ms | 1.5× | 2.8% | PASS |
| file_reads | `oltp_point_select` | 104.65ms | 61.81ms | 0.6× | 1.3% | PASS |
| file_reads | `oltp_range_select` | 25.08ms | 19.89ms | 0.8× | 1.7% | PASS |
| file_reads | `oltp_sum_range` | 23.07ms | 18.27ms | 0.8× | 2.4% | PASS |
| file_reads | `oltp_order_range` | 4.22ms | 3.80ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_distinct_range` | 5.35ms | 4.96ms | 0.9× | 1.0% | PASS |
| file_reads | `oltp_index_scan` | 10.93ms | 9.10ms | 0.8× | 1.9% | PASS |
| file_reads | `select_random_points` | 31.86ms | 27.50ms | 0.9× | 3.4% | PASS |
| file_reads | `select_random_ranges` | 13.96ms | 9.37ms | 0.7× | 1.5% | PASS |
| file_reads | `covering_index_scan` | 14.95ms | 13.75ms | 0.9× | 1.6% | PASS |
| file_reads | `groupby_scan` | 35.97ms | 37.39ms | 1.0× | 1.0% | PASS |
| file_reads | `index_join` | 15.50ms | 12.51ms | 0.8× | 4.3% | PASS |
| file_reads | `index_join_scan` | 4.86ms | 7.37ms | 1.5× | 2.5% | PASS |
| file_reads | `types_table_scan` | 1.19s | 1.38s | 1.2× | 3.7% | PASS |
| file_reads | `table_scan` | 1.37s | 1.47s | 1.1× | 2.9% | PASS |
| file_reads | `oltp_read_only` | 247.42ms | 186.88ms | 0.8× | 1.7% | PASS |
| file_writes | `oltp_bulk_insert` | 269.31ms | 411.67ms | 1.5× | 1.0% | PASS |
| file_writes | `oltp_insert` | 26.19ms | 45.24ms | 1.7× | 1.5% | PASS |
| file_writes | `oltp_update_index` | 136.63ms | 169.53ms | 1.2× | 6.0% | PASS |
| file_writes | `oltp_update_non_index` | 120.61ms | 102.15ms | 0.8× | 12.7% | PASS |
| file_writes | `oltp_delete_insert` | 97.87ms | 127.49ms | 1.3× | 1.4% | PASS |
| file_writes | `oltp_write_only` | 82.88ms | 71.56ms | 0.9× | 10.4% | PASS |
| file_writes | `types_delete_insert` | 74.26ms | 74.01ms | 1.0× | 2.1% | PASS |
| file_writes | `oltp_read_write` | 149.21ms | 163.56ms | 1.1× | 7.9% | PASS |
| ac_reads | `oltp_point_select` | 59.30ms | 61.28ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_range_select` | 20.03ms | 19.67ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_sum_range` | 18.34ms | 18.06ms | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_order_range` | 3.68ms | 3.80ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_distinct_range` | 4.82ms | 4.97ms | 1.0× | 1.5% | PASS |
| ac_reads | `oltp_index_scan` | 6.60ms | 9.15ms | 1.4× | 1.9% | PASS |
| ac_reads | `select_random_points` | 26.68ms | 27.45ms | 1.0× | 4.0% | PASS |
| ac_reads | `select_random_ranges` | 9.35ms | 9.41ms | 1.0× | 1.6% | PASS |
| ac_reads | `covering_index_scan` | 10.39ms | 13.67ms | 1.3× | 1.6% | PASS |
| ac_reads | `groupby_scan` | 34.94ms | 37.33ms | 1.1× | 1.2% | PASS |
| ac_reads | `index_join` | 12.49ms | 12.42ms | 1.0× | 2.3% | PASS |
| ac_reads | `index_join_scan` | 4.33ms | 7.33ms | 1.7× | 2.4% | PASS |
| ac_reads | `types_table_scan` | 1.13s | 1.36s | 1.2× | 4.8% | PASS |
| ac_reads | `table_scan` | 1.25s | 1.45s | 1.2× | 2.9% | PASS |
| ac_reads | `oltp_read_only` | 178.76ms | 186.93ms | 1.0× | 2.1% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 23.39ms | 64.69ms | 2.8× | 7.1% | PASS |
| ac_writes | `oltp_insert_ac` | 26.61ms | 78.44ms | 2.9× | 7.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.58ms | 93.54ms | 3.5× | 6.4% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.43ms | 74.12ms | 3.3× | 5.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.95ms | 85.21ms | 3.4× | 5.8% | PASS |
| ac_writes | `oltp_write_only_ac` | 24.21ms | 81.48ms | 3.4× | 7.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 23.14ms | 74.33ms | 3.2× | 9.8% | PASS |
| ac_writes | `oltp_read_write_ac` | 30.82ms | 89.61ms | 2.9× | 6.4% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 34.97ms | 44.36ms | 1.3× | 1.5% | PASS |
| mem_reads | `oltp_range_select` | 17.10ms | 17.78ms | 1.0× | 2.9% | PASS |
| mem_reads | `oltp_sum_range` | 15.45ms | 16.26ms | 1.1× | 2.6% | PASS |
| mem_reads | `oltp_order_range` | 3.21ms | 3.57ms | 1.1× | 1.9% | PASS |
| mem_reads | `oltp_distinct_range` | 4.34ms | 4.72ms | 1.1× | 2.0% | PASS |
| mem_reads | `oltp_index_scan` | 4.00ms | 7.36ms | 1.8× | 1.9% | PASS |
| mem_reads | `select_random_points` | 21.62ms | 23.74ms | 1.1× | 4.5% | PASS |
| mem_reads | `select_random_ranges` | 6.84ms | 7.60ms | 1.1× | 2.7% | PASS |
| mem_reads | `covering_index_scan` | 7.92ms | 11.98ms | 1.5× | 1.6% | PASS |
| mem_reads | `groupby_scan` | 35.16ms | 37.19ms | 1.1× | 1.0% | PASS |
| mem_reads | `index_join` | 10.89ms | 11.44ms | 1.1× | 3.0% | PASS |
| mem_reads | `index_join_scan` | 4.01ms | 7.04ms | 1.8× | 3.3% | PASS |
| mem_reads | `types_table_scan` | 1.13s | 1.37s | 1.2× | 2.2% | PASS |
| mem_reads | `table_scan` | 1.28s | 1.45s | 1.1× | 2.2% | PASS |
| mem_reads | `oltp_read_only` | 140.85ms | 157.50ms | 1.1× | 2.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 247.62ms | 405.51ms | 1.6× | 1.1% | PASS |
| mem_writes | `oltp_insert` | 18.68ms | 40.14ms | 2.1× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 66.08ms | 145.15ms | 2.2× | 1.6% | PASS |
| mem_writes | `oltp_update_non_index` | 50.66ms | 85.69ms | 1.7× | 1.6% | PASS |
| mem_writes | `oltp_delete_insert` | 54.11ms | 109.84ms | 2.0× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 29.53ms | 59.09ms | 2.0× | 1.4% | PASS |
| mem_writes | `types_delete_insert` | 39.93ms | 64.07ms | 1.6× | 1.8% | PASS |
| mem_writes | `oltp_read_write` | 113.15ms | 152.35ms | 1.3× | 3.4% | PASS |
| file_reads | `oltp_point_select` | 107.40ms | 63.30ms | 0.6× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 27.53ms | 20.59ms | 0.7× | 2.0% | PASS |
| file_reads | `oltp_sum_range` | 24.97ms | 18.98ms | 0.8× | 1.1% | PASS |
| file_reads | `oltp_order_range` | 4.90ms | 4.16ms | 0.8× | 2.7% | PASS |
| file_reads | `oltp_distinct_range` | 5.96ms | 5.29ms | 0.9× | 2.6% | PASS |
| file_reads | `oltp_index_scan` | 11.48ms | 9.37ms | 0.8× | 1.5% | PASS |
| file_reads | `select_random_points` | 35.04ms | 28.10ms | 0.8× | 2.3% | PASS |
| file_reads | `select_random_ranges` | 14.54ms | 9.65ms | 0.7× | 1.6% | PASS |
| file_reads | `covering_index_scan` | 16.01ms | 14.28ms | 0.9× | 1.5% | PASS |
| file_reads | `groupby_scan` | 37.16ms | 38.16ms | 1.0× | 1.1% | PASS |
| file_reads | `index_join` | 15.67ms | 12.91ms | 0.8× | 2.7% | PASS |
| file_reads | `index_join_scan` | 4.91ms | 7.38ms | 1.5× | 2.3% | PASS |
| file_reads | `types_table_scan` | 1.03s | 1.34s | 1.3× | 1.1% | PASS |
| file_reads | `table_scan` | 1.15s | 1.43s | 1.2× | 0.6% | PASS |
| file_reads | `oltp_read_only` | 251.21ms | 186.94ms | 0.7× | 2.1% | PASS |
| file_writes | `oltp_bulk_insert` | 271.07ms | 413.33ms | 1.5× | 1.0% | PASS |
| file_writes | `oltp_insert` | 27.40ms | 46.15ms | 1.7× | 2.5% | PASS |
| file_writes | `oltp_update_index` | 107.68ms | 175.24ms | 1.6× | 1.9% | PASS |
| file_writes | `oltp_update_non_index` | 91.47ms | 103.88ms | 1.1× | 6.5% | PASS |
| file_writes | `oltp_delete_insert` | 99.12ms | 135.24ms | 1.4× | 2.4% | PASS |
| file_writes | `oltp_write_only` | 64.54ms | 76.25ms | 1.2× | 2.6% | PASS |
| file_writes | `types_delete_insert` | 71.21ms | 74.24ms | 1.0× | 1.9% | PASS |
| file_writes | `oltp_read_write` | 150.49ms | 167.57ms | 1.1× | 2.5% | PASS |
| ac_reads | `oltp_point_select` | 62.21ms | 63.10ms | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_range_select` | 20.54ms | 20.07ms | 1.0× | 3.1% | PASS |
| ac_reads | `oltp_sum_range` | 18.77ms | 18.52ms | 1.0× | 2.4% | PASS |
| ac_reads | `oltp_order_range` | 4.50ms | 4.18ms | 0.9× | 3.9% | PASS |
| ac_reads | `oltp_distinct_range` | 5.30ms | 5.12ms | 1.0× | 2.7% | PASS |
| ac_reads | `oltp_index_scan` | 6.61ms | 9.20ms | 1.4× | 2.4% | PASS |
| ac_reads | `select_random_points` | 25.73ms | 26.51ms | 1.0× | 3.5% | PASS |
| ac_reads | `select_random_ranges` | 9.44ms | 9.46ms | 1.0× | 2.3% | PASS |
| ac_reads | `covering_index_scan` | 11.08ms | 14.72ms | 1.3× | 1.8% | PASS |
| ac_reads | `groupby_scan` | 35.52ms | 37.54ms | 1.1× | 1.6% | PASS |
| ac_reads | `index_join` | 13.09ms | 12.77ms | 1.0× | 3.2% | PASS |
| ac_reads | `index_join_scan` | 4.51ms | 7.29ms | 1.6× | 3.2% | PASS |
| ac_reads | `types_table_scan` | 1.04s | 1.34s | 1.3× | 1.1% | PASS |
| ac_reads | `table_scan` | 1.28s | 1.46s | 1.1× | 1.4% | PASS |
| ac_reads | `oltp_read_only` | 190.80ms | 189.60ms | 1.0× | 1.2% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 24.51ms | 70.63ms | 2.9× | 6.5% | PASS |
| ac_writes | `oltp_insert_ac` | 26.87ms | 91.17ms | 3.4× | 8.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 29.40ms | 102.36ms | 3.5× | 6.8% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.68ms | 78.28ms | 3.3× | 7.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 27.97ms | 89.26ms | 3.2× | 8.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.87ms | 89.51ms | 3.3× | 7.9% | PASS |
| ac_writes | `types_delete_insert_ac` | 24.93ms | 81.35ms | 3.3× | 9.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 34.62ms | 97.41ms | 2.8× | 7.5% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 35.89ms | 48.45ms | 1.3× | 1.3% | PASS |
| mem_reads | `oltp_range_select` | 21.02ms | 25.70ms | 1.2× | 1.2% | PASS |
| mem_reads | `oltp_sum_range` | 18.90ms | 23.77ms | 1.3× | 1.1% | PASS |
| mem_reads | `oltp_order_range` | 3.64ms | 4.42ms | 1.2× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 4.79ms | 5.53ms | 1.2× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 4.79ms | 7.54ms | 1.6× | 2.0% | PASS |
| mem_reads | `select_random_points` | 30.43ms | 36.83ms | 1.2× | 1.8% | PASS |
| mem_reads | `select_random_ranges` | 8.03ms | 10.16ms | 1.3× | 1.7% | PASS |
| mem_reads | `covering_index_scan` | 7.92ms | 12.14ms | 1.5× | 1.3% | PASS |
| mem_reads | `groupby_scan` | 37.24ms | 42.96ms | 1.2× | 1.0% | PASS |
| mem_reads | `index_join` | 8.12ms | 13.08ms | 1.6× | 1.6% | PASS |
| mem_reads | `index_join_scan` | 3.99ms | 7.15ms | 1.8× | 2.2% | PASS |
| mem_reads | `types_table_scan` | 1.05s | 1.33s | 1.3× | 1.1% | PASS |
| mem_reads | `table_scan` | 1.26s | 1.44s | 1.1× | 4.0% | PASS |
| mem_reads | `oltp_read_only` | 150.95ms | 192.16ms | 1.3× | 1.5% | PASS |
| mem_writes | `oltp_bulk_insert` | 249.53ms | 398.16ms | 1.6× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 19.07ms | 37.13ms | 1.9× | 1.1% | PASS |
| mem_writes | `oltp_update_index` | 67.33ms | 137.72ms | 2.0× | 1.4% | PASS |
| mem_writes | `oltp_update_non_index` | 52.56ms | 96.59ms | 1.8× | 1.6% | PASS |
| mem_writes | `oltp_delete_insert` | 51.27ms | 106.05ms | 2.1× | 1.6% | PASS |
| mem_writes | `oltp_write_only` | 28.23ms | 64.64ms | 2.3× | 1.3% | PASS |
| mem_writes | `types_delete_insert` | 33.84ms | 61.35ms | 1.8× | 1.3% | PASS |
| mem_writes | `oltp_read_write` | 101.77ms | 173.32ms | 1.7× | 1.7% | PASS |
| file_reads | `oltp_point_select` | 99.72ms | 62.46ms | 0.6× | 1.2% | PASS |
| file_reads | `oltp_range_select` | 26.27ms | 26.73ms | 1.0× | 2.0% | PASS |
| file_reads | `oltp_sum_range` | 24.99ms | 24.87ms | 1.0× | 1.7% | PASS |
| file_reads | `oltp_order_range` | 4.50ms | 4.64ms | 1.0× | 2.2% | PASS |
| file_reads | `oltp_distinct_range` | 5.65ms | 5.73ms | 1.0× | 1.7% | PASS |
| file_reads | `oltp_index_scan` | 11.10ms | 8.89ms | 0.8× | 1.4% | PASS |
| file_reads | `select_random_points` | 36.89ms | 37.98ms | 1.0× | 2.1% | PASS |
| file_reads | `select_random_ranges` | 14.59ms | 11.86ms | 0.8× | 1.5% | PASS |
| file_reads | `covering_index_scan` | 14.56ms | 13.55ms | 0.9× | 1.0% | PASS |
| file_reads | `groupby_scan` | 37.26ms | 42.41ms | 1.1× | 0.8% | PASS |
| file_reads | `index_join` | 11.93ms | 13.87ms | 1.2× | 2.0% | PASS |
| file_reads | `index_join_scan` | 4.99ms | 7.30ms | 1.5× | 2.5% | PASS |
| file_reads | `types_table_scan` | 1.05s | 1.33s | 1.3× | 1.3% | PASS |
| file_reads | `table_scan` | 1.34s | 1.45s | 1.1× | 2.4% | PASS |
| file_reads | `oltp_read_only` | 256.59ms | 220.50ms | 0.9× | 1.2% | PASS |
| file_writes | `oltp_bulk_insert` | 263.99ms | 397.04ms | 1.5× | 0.6% | PASS |
| file_writes | `oltp_insert` | 26.24ms | 40.89ms | 1.6× | 1.5% | PASS |
| file_writes | `oltp_update_index` | 95.66ms | 148.36ms | 1.6× | 1.4% | PASS |
| file_writes | `oltp_update_non_index` | 76.88ms | 101.13ms | 1.3× | 1.7% | PASS |
| file_writes | `oltp_delete_insert` | 77.62ms | 115.78ms | 1.5× | 2.1% | PASS |
| file_writes | `oltp_write_only` | 51.18ms | 68.82ms | 1.3× | 2.0% | PASS |
| file_writes | `types_delete_insert` | 50.87ms | 66.02ms | 1.3× | 1.9% | PASS |
| file_writes | `oltp_read_write` | 130.37ms | 177.68ms | 1.4× | 1.1% | PASS |
| ac_reads | `oltp_point_select` | 56.47ms | 62.52ms | 1.1× | 1.8% | PASS |
| ac_reads | `oltp_range_select` | 22.47ms | 26.80ms | 1.2× | 1.7% | PASS |
| ac_reads | `oltp_sum_range` | 20.89ms | 25.12ms | 1.2× | 1.7% | PASS |
| ac_reads | `oltp_order_range` | 4.03ms | 4.67ms | 1.2× | 2.8% | PASS |
| ac_reads | `oltp_distinct_range` | 5.18ms | 5.76ms | 1.1× | 2.1% | PASS |
| ac_reads | `oltp_index_scan` | 7.12ms | 8.97ms | 1.3× | 1.5% | PASS |
| ac_reads | `select_random_points` | 31.55ms | 37.89ms | 1.2× | 1.7% | PASS |
| ac_reads | `select_random_ranges` | 10.34ms | 11.88ms | 1.1× | 1.5% | PASS |
| ac_reads | `covering_index_scan` | 10.51ms | 13.61ms | 1.3× | 1.3% | PASS |
| ac_reads | `groupby_scan` | 37.34ms | 42.72ms | 1.1× | 1.1% | PASS |
| ac_reads | `index_join` | 9.74ms | 13.99ms | 1.4× | 1.7% | PASS |
| ac_reads | `index_join_scan` | 4.55ms | 7.32ms | 1.6× | 3.0% | PASS |
| ac_reads | `types_table_scan` | 1.06s | 1.33s | 1.3× | 1.2% | PASS |
| ac_reads | `table_scan` | 1.25s | 1.44s | 1.2× | 3.5% | PASS |
| ac_reads | `oltp_read_only` | 185.85ms | 218.20ms | 1.2× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 23.14ms | 67.63ms | 2.9× | 6.8% | PASS |
| ac_writes | `oltp_insert_ac` | 25.10ms | 80.70ms | 3.2× | 11.7% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.78ms | 92.58ms | 3.5× | 8.0% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.66ms | 73.23ms | 3.2× | 5.4% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.17ms | 83.33ms | 3.4× | 6.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 24.12ms | 82.58ms | 3.4× | 7.0% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.43ms | 78.95ms | 3.5× | 10.1% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.87ms | 89.93ms | 2.8× | 5.4% | PASS |

</details>

</details>

## Version-control latency

Wall time: 4m 28s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 34.85ms | 38.00ms | 91.7% | 0.6% | PASS |
| `status_dirty_many_tables` | 39.03ms | 42.00ms | 92.9% | 1.1% | PASS |
| `diff_regular_working_one_table` | 30.84ms | 33.00ms | 93.5% | 0.6% | PASS |
| `diff_regular_working_many_tables` | 45.50ms | 50.00ms | 91.0% | 0.5% | PASS |
| `diff_stat_working_many_tables` | 45.55ms | 48.00ms | 94.9% | 0.5% | PASS |
| `diff_schema_working_many_tables` | 45.91ms | 48.00ms | 95.7% | 0.5% | PASS |
| `branch_list_many_branches` | 23.37ms | 25.00ms | 93.5% | 0.8% | PASS |
| `branch_create_delete` | 24.68ms | 27.00ms | 91.4% | 0.6% | PASS |
| `at_literal_deep_history` | 24.88ms | 28.00ms | 88.8% | 0.6% | PASS |
| `diff_literal_deep_history` | 24.90ms | 28.00ms | 88.9% | 0.5% | PASS |
| `history_literal_deep_history` | 26.20ms | 30.00ms | 87.3% | 0.6% | PASS |
| `checkout_branch_clean` | 34.06ms | 43.00ms | 79.2% | 0.6% | PASS |
| `merge_data_no_conflicts` | 28.08ms | 33.00ms | 85.1% | 0.7% | PASS |
| `merge_data_secondary_index` | 888.65ms | 944.00ms | 94.1% | 0.5% | PASS |
| `merge_schema_no_conflicts` | 20.96ms | 24.00ms | 87.3% | 1.2% | PASS |
| `merge_data_conflicts` | 30.32ms | 33.00ms | 91.9% | 1.0% | PASS |
| `merge_data_conflicts_with_resolve` | 30.99ms | 34.00ms | 91.2% | 0.6% | PASS |

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

candidate: ebeaa21.

Fixture file bytes: base 107,968,266; delta 115,878,997.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 55.975 | 55.110–63.026 | 37.741 | 1786.51 | 107968266 |
| push_initial | candidate | 579.322 | 577.685–581.917 | 562.523 | 172.62 | 107956772 |
| clone_initial | candidate | 762.544 | 757.518–763.534 | 736.686 | 131.14 | 108475035 |
| push_noop | candidate | 0.698 | 0.667–1.078 | 0.387 | — | 0 |
| fetch_noop | candidate | 0.241 | 0.229–0.256 | 0.244 | — | 0 |
| push_incremental | candidate | 232.625 | 231.971–233.245 | 229.755 | 4.30 | 7909775 |
| fetch_incremental | candidate | 96.247 | 95.617–98.483 | 93.483 | 10.39 | 7910084 |
| pull_fast_forward | candidate | 85.070 | 84.396–86.330 | 82.352 | 11.76 | 7910844 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 90.41 | 175.487 | 108.763 | 18329 | 2066 |
| clone_initial | candidate | 90.71 | 294.856 | 109.784 | 47186 | 2337 |
| push_noop | candidate | 42.59 | 0.073 | 0.000 | 26 | 2 |
| fetch_noop | candidate | 42.59 | 0.135 | 0.000 | 40 | 0 |
| push_incremental | candidate | 42.59 | 96.693 | 7.910 | 22369 | 125 |
| fetch_incremental | candidate | 42.59 | 34.472 | 7.910 | 10808 | 124 |
| pull_fast_forward | candidate | 42.59 | 26.497 | 7.911 | 7031 | 126 |

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

candidate: ebeaa21.

Fixture file bytes: base 1,087,718,031; delta 1,103,525,159.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 2668.322 | 2640.010–2672.404 | 380.145 | 374.77 | 1087718031 |
| push_initial | candidate | 8003.451 | 7977.778–8073.873 | 5778.479 | 124.95 | 1079566275 |
| clone_initial | candidate | 8878.767 | 8847.665–8907.525 | 8522.885 | 112.63 | 1102407326 |
| push_noop | candidate | 0.895 | 0.835–0.897 | 0.356 | — | 0 |
| fetch_noop | candidate | 0.229 | 0.204–0.250 | 0.232 | — | 0 |
| push_incremental | candidate | 1324.895 | 1318.516–1326.903 | 1317.044 | 0.75 | 15806003 |
| fetch_incremental | candidate | 275.883 | 270.158–283.566 | 268.064 | 3.62 | 15806513 |
| pull_fast_forward | candidate | 279.126 | 277.030–279.691 | 270.673 | 3.58 | 15807273 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 134.75 | 2091.994 | 1087.622 | 263526 | 20541 |
| clone_initial | candidate | 124.02 | 3529.604 | 1132.819 | 615202 | 32243 |
| push_noop | candidate | 45.38 | 0.078 | 0.000 | 27 | 2 |
| fetch_noop | candidate | 45.38 | 0.137 | 0.000 | 40 | 0 |
| push_incremental | candidate | 48.27 | 621.628 | 15.928 | 150992 | 313 |
| fetch_incremental | candidate | 50.37 | 61.935 | 15.928 | 13822 | 313 |
| pull_fast_forward | candidate | 50.38 | 61.867 | 15.929 | 13818 | 315 |

</details>


</details>

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh`, `test/vc_perf_ceiling.sh`, and `test/remote_perf.py`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
