# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-11-17 11:26 UTC
>
> Commit: [`465e271637a82872d5de5f78ae720d93df229b52`](https://github.com/dolthub/doltlite/commit/465e271637a82872d5de5f78ae720d93df229b52)
>
> Runner: ubuntu24 20261111.127.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/44056420144)

The SQL workloads compare optimized DoltLite against stock SQLite on the same GitHub-hosted runner, alternating execution order on each repetition. Version-control sections measure DoltLite latency. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 8.55s | 9.79s | 1.15× | 1.2% | **PASS** |
| Writes | 1.70s | 2.62s | 1.54× | 1.0% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 8.97s | 9.87s | 1.10× | 1.2% | **PASS** |
| Writes | 2.70s | 3.14s | 1.16× | 1.8% | **PASS** |
| Autocommit writes | 601.26ms | 1.13s | 1.87× | 2.4% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.8× for a section average. Durable autocommit writes use 3.6× and 2.6× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 2.34s | 2.53s | 1.08× | 1.0% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.51s | 2.99s | 1.19× | 1.4% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 1.84s | 2.13s | 1.16× | 1.2% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 1.86s | 2.14s | 1.15× | 1.2% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 361.66ms | 583.68ms | 1.61× | 0.8% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 587.62ms | 914.55ms | 1.56× | 1.1% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 386.99ms | 582.93ms | 1.51× | 1.0% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 360.41ms | 539.14ms | 1.50× | 1.6% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.40s | 2.55s | 1.06× | 0.8% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.74s | 3.05s | 1.11× | 1.6% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 1.90s | 2.13s | 1.12× | 1.1% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 1.93s | 2.14s | 1.11× | 1.3% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 865.30ms | 901.06ms | 1.04× | 1.7% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 915.90ms | 1.03s | 1.12× | 3.3% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 470.57ms | 616.59ms | 1.31× | 1.8% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 449.04ms | 591.02ms | 1.32× | 1.8% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.31s | 2.54s | 1.10× | 0.9% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.73s | 3.08s | 1.13× | 1.6% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 1.86s | 2.13s | 1.15× | 2.6% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 1.90s | 2.15s | 1.13× | 1.1% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 244.81ms | 408.17ms | 1.67× | 3.0% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 199.00ms | 393.75ms | 1.98× | 4.7% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 81.10ms | 169.40ms | 2.09× | 1.9% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 76.35ms | 155.78ms | 2.04× | 1.3% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 21.19ms | 23.13ms | 1.1× | 1.3% | PASS |
| mem_reads | `oltp_range_select` | 9.55ms | 11.13ms | 1.2× | 0.7% | PASS |
| mem_reads | `oltp_sum_range` | 8.62ms | 9.58ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_order_range` | 2.48ms | 2.72ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_distinct_range` | 3.34ms | 3.69ms | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_index_scan` | 3.45ms | 4.23ms | 1.2× | 1.4% | PASS |
| mem_reads | `select_random_points` | 12.15ms | 10.67ms | 0.9× | 1.1% | PASS |
| mem_reads | `select_random_ranges` | 3.94ms | 4.04ms | 1.0× | 1.1% | PASS |
| mem_reads | `covering_index_scan` | 6.31ms | 8.79ms | 1.4× | 0.8% | PASS |
| mem_reads | `groupby_scan` | 29.34ms | 28.96ms | 1.0× | 0.6% | PASS |
| mem_reads | `index_join` | 5.08ms | 7.57ms | 1.5× | 1.3% | PASS |
| mem_reads | `index_join_scan` | 2.79ms | 5.01ms | 1.8× | 0.9% | PASS |
| mem_reads | `types_table_scan` | 984.96ms | 1.09s | 1.1× | 1.7% | PASS |
| mem_reads | `table_scan` | 1.16s | 1.22s | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_read_only` | 94.22ms | 104.19ms | 1.1× | 0.5% | PASS |
| mem_writes | `oltp_bulk_insert` | 141.17ms | 217.08ms | 1.5× | 0.5% | PASS |
| mem_writes | `oltp_insert` | 12.64ms | 20.29ms | 1.6× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 43.82ms | 82.05ms | 1.9× | 0.8% | PASS |
| mem_writes | `oltp_update_non_index` | 29.29ms | 49.26ms | 1.7× | 0.9% | PASS |
| mem_writes | `oltp_delete_insert` | 38.98ms | 61.91ms | 1.6× | 0.8% | PASS |
| mem_writes | `oltp_write_only` | 18.66ms | 38.57ms | 2.1× | 0.7% | PASS |
| mem_writes | `types_delete_insert` | 20.82ms | 27.92ms | 1.3× | 0.8% | PASS |
| mem_writes | `oltp_read_write` | 56.30ms | 86.61ms | 1.5× | 1.0% | PASS |
| file_reads | `oltp_point_select` | 46.76ms | 28.97ms | 0.6× | 0.7% | PASS |
| file_reads | `oltp_range_select` | 12.54ms | 12.08ms | 1.0× | 0.8% | PASS |
| file_reads | `oltp_sum_range` | 11.68ms | 10.58ms | 0.9× | 1.8% | PASS |
| file_reads | `oltp_order_range` | 2.85ms | 2.88ms | 1.0× | 0.8% | PASS |
| file_reads | `oltp_distinct_range` | 3.76ms | 3.89ms | 1.0× | 0.7% | PASS |
| file_reads | `oltp_index_scan` | 6.46ms | 5.26ms | 0.8× | 1.2% | PASS |
| file_reads | `select_random_points` | 15.83ms | 11.88ms | 0.7× | 1.2% | PASS |
| file_reads | `select_random_ranges` | 6.78ms | 4.77ms | 0.7× | 0.9% | PASS |
| file_reads | `covering_index_scan` | 9.33ms | 9.80ms | 1.1× | 0.6% | PASS |
| file_reads | `groupby_scan` | 29.88ms | 29.39ms | 1.0× | 0.4% | PASS |
| file_reads | `index_join` | 6.88ms | 8.13ms | 1.2× | 0.7% | PASS |
| file_reads | `index_join_scan` | 3.18ms | 5.18ms | 1.6× | 1.6% | PASS |
| file_reads | `types_table_scan` | 969.24ms | 1.08s | 1.1× | 0.6% | PASS |
| file_reads | `table_scan` | 1.14s | 1.22s | 1.1× | 1.7% | PASS |
| file_reads | `oltp_read_only` | 136.27ms | 115.32ms | 0.8× | 0.6% | PASS |
| file_writes | `oltp_bulk_insert` | 189.74ms | 260.12ms | 1.4× | 1.6% | PASS |
| file_writes | `oltp_insert` | 20.06ms | 27.38ms | 1.4× | 1.1% | PASS |
| file_writes | `oltp_update_index` | 140.99ms | 139.84ms | 1.0× | 1.5% | PASS |
| file_writes | `oltp_update_non_index` | 110.38ms | 89.55ms | 0.8× | 1.8% | PASS |
| file_writes | `oltp_delete_insert` | 120.78ms | 117.28ms | 1.0× | 0.5% | PASS |
| file_writes | `oltp_write_only` | 90.09ms | 79.27ms | 0.9× | 5.9% | PASS |
| file_writes | `types_delete_insert` | 70.00ms | 58.22ms | 0.8× | 4.5% | PASS |
| file_writes | `oltp_read_write` | 123.28ms | 129.41ms | 1.0× | 3.9% | PASS |
| ac_reads | `oltp_point_select` | 29.33ms | 28.95ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_range_select` | 10.70ms | 12.06ms | 1.1× | 0.5% | PASS |
| ac_reads | `oltp_sum_range` | 10.14ms | 10.54ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_order_range` | 2.67ms | 2.90ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_distinct_range` | 3.60ms | 3.83ms | 1.1× | 0.9% | PASS |
| ac_reads | `oltp_index_scan` | 4.72ms | 5.23ms | 1.1× | 1.0% | PASS |
| ac_reads | `select_random_points` | 13.89ms | 11.70ms | 0.8× | 0.8% | PASS |
| ac_reads | `select_random_ranges` | 5.04ms | 4.78ms | 0.9× | 0.8% | PASS |
| ac_reads | `covering_index_scan` | 7.48ms | 9.79ms | 1.3× | 0.9% | PASS |
| ac_reads | `groupby_scan` | 29.58ms | 29.30ms | 1.0× | 0.4% | PASS |
| ac_reads | `index_join` | 6.02ms | 8.09ms | 1.3× | 1.1% | PASS |
| ac_reads | `index_join_scan` | 3.05ms | 5.12ms | 1.7× | 1.0% | PASS |
| ac_reads | `types_table_scan` | 969.87ms | 1.08s | 1.1× | 1.0% | PASS |
| ac_reads | `table_scan` | 1.11s | 1.21s | 1.1× | 2.0% | PASS |
| ac_reads | `oltp_read_only` | 108.38ms | 114.35ms | 1.1× | 0.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 28.31ms | 40.10ms | 1.4× | 2.4% | PASS |
| ac_writes | `oltp_insert_ac` | 30.48ms | 49.77ms | 1.6× | 3.1% | PASS |
| ac_writes | `oltp_update_index_ac` | 33.32ms | 62.61ms | 1.9× | 4.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 28.42ms | 44.89ms | 1.6× | 3.2% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 30.05ms | 53.12ms | 1.8× | 2.9% | PASS |
| ac_writes | `oltp_write_only_ac` | 29.66ms | 52.93ms | 1.8× | 1.9% | PASS |
| ac_writes | `types_delete_insert_ac` | 30.87ms | 45.10ms | 1.5× | 4.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 33.70ms | 59.65ms | 1.8× | 2.4% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.88ms | 37.48ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_range_select` | 16.14ms | 15.89ms | 1.0× | 2.9% | PASS |
| mem_reads | `oltp_sum_range` | 14.62ms | 14.96ms | 1.0× | 2.5% | PASS |
| mem_reads | `oltp_order_range` | 3.23ms | 3.41ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 4.45ms | 4.49ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_index_scan` | 4.04ms | 6.49ms | 1.6× | 1.3% | PASS |
| mem_reads | `select_random_points` | 22.63ms | 22.04ms | 1.0× | 2.3% | PASS |
| mem_reads | `select_random_ranges` | 6.82ms | 6.64ms | 1.0× | 1.9% | PASS |
| mem_reads | `covering_index_scan` | 7.76ms | 11.08ms | 1.4× | 1.2% | PASS |
| mem_reads | `groupby_scan` | 33.85ms | 34.49ms | 1.0× | 0.8% | PASS |
| mem_reads | `index_join` | 10.02ms | 9.98ms | 1.0× | 1.4% | PASS |
| mem_reads | `index_join_scan` | 3.59ms | 6.21ms | 1.7× | 1.5% | PASS |
| mem_reads | `types_table_scan` | 1.05s | 1.28s | 1.2× | 1.5% | PASS |
| mem_reads | `table_scan` | 1.17s | 1.40s | 1.2× | 0.7% | PASS |
| mem_reads | `oltp_read_only` | 133.85ms | 140.30ms | 1.0× | 1.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 243.50ms | 335.11ms | 1.4× | 0.8% | PASS |
| mem_writes | `oltp_insert` | 17.87ms | 34.39ms | 1.9× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 64.14ms | 132.15ms | 2.1× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 48.54ms | 75.04ms | 1.5× | 1.4% | PASS |
| mem_writes | `oltp_delete_insert` | 54.16ms | 98.23ms | 1.8× | 0.9% | PASS |
| mem_writes | `oltp_write_only` | 27.62ms | 51.50ms | 1.9× | 1.3% | PASS |
| mem_writes | `types_delete_insert` | 37.80ms | 57.77ms | 1.5× | 1.2% | PASS |
| mem_writes | `oltp_read_write` | 94.00ms | 130.36ms | 1.4× | 1.1% | PASS |
| file_reads | `oltp_point_select` | 101.11ms | 54.63ms | 0.5× | 1.5% | PASS |
| file_reads | `oltp_range_select` | 23.49ms | 17.71ms | 0.8× | 2.0% | PASS |
| file_reads | `oltp_sum_range` | 21.77ms | 16.86ms | 0.8× | 1.5% | PASS |
| file_reads | `oltp_order_range` | 4.13ms | 3.65ms | 0.9× | 2.1% | PASS |
| file_reads | `oltp_distinct_range` | 5.30ms | 4.74ms | 0.9× | 1.9% | PASS |
| file_reads | `oltp_index_scan` | 10.86ms | 8.27ms | 0.8× | 1.6% | PASS |
| file_reads | `select_random_points` | 29.86ms | 24.42ms | 0.8× | 2.5% | PASS |
| file_reads | `select_random_ranges` | 13.68ms | 8.49ms | 0.6× | 1.4% | PASS |
| file_reads | `covering_index_scan` | 14.92ms | 13.00ms | 0.9× | 1.2% | PASS |
| file_reads | `groupby_scan` | 35.38ms | 34.87ms | 1.0× | 0.8% | PASS |
| file_reads | `index_join` | 14.29ms | 11.31ms | 0.8× | 2.3% | PASS |
| file_reads | `index_join_scan` | 4.55ms | 6.51ms | 1.4× | 1.8% | PASS |
| file_reads | `types_table_scan` | 1.04s | 1.27s | 1.2× | 0.8% | PASS |
| file_reads | `table_scan` | 1.18s | 1.40s | 1.2× | 1.6% | PASS |
| file_reads | `oltp_read_only` | 239.16ms | 168.17ms | 0.7× | 1.5% | PASS |
| file_writes | `oltp_bulk_insert` | 268.00ms | 352.12ms | 1.3× | 0.8% | PASS |
| file_writes | `oltp_insert` | 25.58ms | 39.70ms | 1.6× | 1.3% | PASS |
| file_writes | `oltp_update_index` | 131.11ms | 153.90ms | 1.2× | 8.0% | PASS |
| file_writes | `oltp_update_non_index` | 101.32ms | 89.53ms | 0.9× | 13.0% | PASS |
| file_writes | `oltp_delete_insert` | 96.61ms | 115.52ms | 1.2× | 1.5% | PASS |
| file_writes | `oltp_write_only` | 81.41ms | 65.42ms | 0.8× | 11.6% | PASS |
| file_writes | `types_delete_insert` | 72.04ms | 68.28ms | 0.9× | 1.9% | PASS |
| file_writes | `oltp_read_write` | 139.82ms | 143.25ms | 1.0× | 4.8% | PASS |
| ac_reads | `oltp_point_select` | 57.96ms | 54.76ms | 0.9× | 1.6% | PASS |
| ac_reads | `oltp_range_select` | 19.59ms | 17.76ms | 0.9× | 2.3% | PASS |
| ac_reads | `oltp_sum_range` | 17.96ms | 17.08ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_order_range` | 3.83ms | 3.71ms | 1.0× | 2.3% | PASS |
| ac_reads | `oltp_distinct_range` | 4.97ms | 4.77ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_index_scan` | 6.65ms | 8.32ms | 1.3× | 1.4% | PASS |
| ac_reads | `select_random_points` | 25.76ms | 24.78ms | 1.0× | 2.0% | PASS |
| ac_reads | `select_random_ranges` | 9.51ms | 8.60ms | 0.9× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 10.60ms | 13.12ms | 1.2× | 1.3% | PASS |
| ac_reads | `groupby_scan` | 35.16ms | 34.91ms | 1.0× | 1.1% | PASS |
| ac_reads | `index_join` | 12.40ms | 11.39ms | 0.9× | 1.7% | PASS |
| ac_reads | `index_join_scan` | 4.26ms | 6.63ms | 1.6× | 2.0% | PASS |
| ac_reads | `types_table_scan` | 1.04s | 1.28s | 1.2× | 1.0% | PASS |
| ac_reads | `table_scan` | 1.30s | 1.43s | 1.1× | 3.1% | PASS |
| ac_reads | `oltp_read_only` | 178.12ms | 169.85ms | 1.0× | 1.9% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 23.19ms | 33.40ms | 1.4× | 7.2% | PASS |
| ac_writes | `oltp_insert_ac` | 25.65ms | 44.84ms | 1.7× | 5.0% | PASS |
| ac_writes | `oltp_update_index_ac` | 25.81ms | 65.22ms | 2.5× | 4.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.55ms | 41.22ms | 1.8× | 7.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.81ms | 55.19ms | 2.2× | 3.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 24.21ms | 52.94ms | 2.2× | 5.4% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.17ms | 40.55ms | 1.8× | 3.5% | PASS |
| ac_writes | `oltp_read_write_ac` | 30.61ms | 60.41ms | 2.0× | 4.4% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.72ms | 23.68ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_range_select` | 11.48ms | 11.18ms | 1.0× | 1.2% | PASS |
| mem_reads | `oltp_sum_range` | 11.41ms | 10.65ms | 0.9× | 1.2% | PASS |
| mem_reads | `oltp_order_range` | 2.39ms | 2.50ms | 1.0× | 1.4% | PASS |
| mem_reads | `oltp_distinct_range` | 3.07ms | 3.18ms | 1.0× | 0.8% | PASS |
| mem_reads | `oltp_index_scan` | 2.93ms | 4.63ms | 1.6× | 1.4% | PASS |
| mem_reads | `select_random_points` | 17.98ms | 16.86ms | 0.9× | 1.2% | PASS |
| mem_reads | `select_random_ranges` | 4.70ms | 4.42ms | 0.9× | 1.2% | PASS |
| mem_reads | `covering_index_scan` | 4.81ms | 7.03ms | 1.5× | 0.5% | PASS |
| mem_reads | `groupby_scan` | 23.22ms | 23.66ms | 1.0× | 0.4% | PASS |
| mem_reads | `index_join` | 9.55ms | 8.28ms | 0.9× | 1.6% | PASS |
| mem_reads | `index_join_scan` | 2.73ms | 5.41ms | 2.0× | 2.2% | PASS |
| mem_reads | `types_table_scan` | 764.27ms | 908.86ms | 1.2× | 0.6% | PASS |
| mem_reads | `table_scan` | 862.88ms | 1.00s | 1.2× | 1.9% | PASS |
| mem_reads | `oltp_read_only` | 94.46ms | 95.68ms | 1.0× | 0.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 145.78ms | 194.95ms | 1.3× | 0.4% | PASS |
| mem_writes | `oltp_insert` | 11.83ms | 22.53ms | 1.9× | 0.6% | PASS |
| mem_writes | `oltp_update_index` | 47.03ms | 93.32ms | 2.0× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 33.46ms | 48.93ms | 1.5× | 1.6% | PASS |
| mem_writes | `oltp_delete_insert` | 37.66ms | 65.04ms | 1.7× | 1.4% | PASS |
| mem_writes | `oltp_write_only` | 20.19ms | 34.56ms | 1.7× | 1.2% | PASS |
| mem_writes | `types_delete_insert` | 26.81ms | 38.45ms | 1.4× | 1.0% | PASS |
| mem_writes | `oltp_read_write` | 64.23ms | 85.14ms | 1.3× | 0.9% | PASS |
| file_reads | `oltp_point_select` | 44.66ms | 28.00ms | 0.6× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 13.11ms | 11.53ms | 0.9× | 0.9% | PASS |
| file_reads | `oltp_sum_range` | 13.04ms | 11.10ms | 0.9× | 1.5% | PASS |
| file_reads | `oltp_order_range` | 2.61ms | 2.52ms | 1.0× | 1.1% | PASS |
| file_reads | `oltp_distinct_range` | 3.15ms | 3.10ms | 1.0× | 1.8% | PASS |
| file_reads | `oltp_index_scan` | 4.84ms | 4.81ms | 1.0× | 2.1% | PASS |
| file_reads | `select_random_points` | 18.72ms | 16.25ms | 0.9× | 2.3% | PASS |
| file_reads | `select_random_ranges` | 6.58ms | 4.63ms | 0.7× | 1.3% | PASS |
| file_reads | `covering_index_scan` | 6.88ms | 7.25ms | 1.1× | 0.7% | PASS |
| file_reads | `groupby_scan` | 23.20ms | 23.47ms | 1.0× | 0.5% | PASS |
| file_reads | `index_join` | 10.19ms | 8.00ms | 0.8× | 2.1% | PASS |
| file_reads | `index_join_scan` | 2.88ms | 5.27ms | 1.8× | 3.0% | PASS |
| file_reads | `types_table_scan` | 763.17ms | 907.07ms | 1.2× | 0.3% | PASS |
| file_reads | `table_scan` | 865.41ms | 999.26ms | 1.2× | 0.7% | PASS |
| file_reads | `oltp_read_only` | 118.80ms | 99.09ms | 0.8× | 0.8% | PASS |
| file_writes | `oltp_bulk_insert` | 155.57ms | 199.31ms | 1.3× | 0.9% | PASS |
| file_writes | `oltp_insert` | 14.94ms | 24.89ms | 1.7× | 1.0% | PASS |
| file_writes | `oltp_update_index` | 59.45ms | 96.68ms | 1.6× | 1.9% | PASS |
| file_writes | `oltp_update_non_index` | 44.63ms | 54.59ms | 1.2× | 1.9% | PASS |
| file_writes | `oltp_delete_insert` | 51.56ms | 72.55ms | 1.4× | 1.7% | PASS |
| file_writes | `oltp_write_only` | 31.48ms | 40.30ms | 1.3× | 2.8% | PASS |
| file_writes | `types_delete_insert` | 37.61ms | 39.20ms | 1.0× | 1.9% | PASS |
| file_writes | `oltp_read_write` | 75.33ms | 89.08ms | 1.2× | 1.8% | PASS |
| ac_reads | `oltp_point_select` | 30.19ms | 27.55ms | 0.9× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 11.32ms | 11.34ms | 1.0× | 2.1% | PASS |
| ac_reads | `oltp_sum_range` | 9.80ms | 10.23ms | 1.0× | 5.6% | PASS |
| ac_reads | `oltp_order_range` | 2.28ms | 2.33ms | 1.0× | 3.5% | PASS |
| ac_reads | `oltp_distinct_range` | 2.92ms | 3.01ms | 1.0× | 2.6% | PASS |
| ac_reads | `oltp_index_scan` | 3.31ms | 4.37ms | 1.3× | 3.2% | PASS |
| ac_reads | `select_random_points` | 17.64ms | 15.84ms | 0.9× | 2.8% | PASS |
| ac_reads | `select_random_ranges` | 5.06ms | 4.50ms | 0.9× | 3.3% | PASS |
| ac_reads | `covering_index_scan` | 5.46ms | 7.13ms | 1.3× | 0.6% | PASS |
| ac_reads | `groupby_scan` | 22.96ms | 23.35ms | 1.0× | 0.7% | PASS |
| ac_reads | `index_join` | 9.03ms | 7.62ms | 0.8× | 3.5% | PASS |
| ac_reads | `index_join_scan` | 2.77ms | 5.15ms | 1.9× | 3.0% | PASS |
| ac_reads | `types_table_scan` | 762.78ms | 907.20ms | 1.2× | 0.4% | PASS |
| ac_reads | `table_scan` | 868.95ms | 999.20ms | 1.1× | 0.8% | PASS |
| ac_reads | `oltp_read_only` | 100.62ms | 98.98ms | 1.0× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 9.13ms | 12.50ms | 1.4× | 1.3% | PASS |
| ac_writes | `oltp_insert_ac` | 10.03ms | 21.31ms | 2.1× | 2.4% | PASS |
| ac_writes | `oltp_update_index_ac` | 10.37ms | 29.47ms | 2.8× | 1.5% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 8.77ms | 15.95ms | 1.8× | 1.0% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 11.34ms | 24.90ms | 2.2× | 9.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 9.88ms | 22.60ms | 2.3× | 2.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 9.13ms | 16.50ms | 1.8× | 1.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 12.46ms | 26.18ms | 2.1× | 2.0% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 21.32ms | 23.62ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_range_select` | 13.79ms | 14.90ms | 1.1× | 1.2% | PASS |
| mem_reads | `oltp_sum_range` | 12.96ms | 13.63ms | 1.1× | 2.2% | PASS |
| mem_reads | `oltp_order_range` | 2.59ms | 2.75ms | 1.1× | 2.4% | PASS |
| mem_reads | `oltp_distinct_range` | 3.23ms | 3.44ms | 1.1× | 2.0% | PASS |
| mem_reads | `oltp_index_scan` | 3.13ms | 3.99ms | 1.3× | 1.5% | PASS |
| mem_reads | `select_random_points` | 20.81ms | 21.19ms | 1.0× | 0.8% | PASS |
| mem_reads | `select_random_ranges` | 5.08ms | 5.72ms | 1.1× | 0.9% | PASS |
| mem_reads | `covering_index_scan` | 4.79ms | 6.66ms | 1.4× | 0.6% | PASS |
| mem_reads | `groupby_scan` | 24.99ms | 26.57ms | 1.1× | 0.4% | PASS |
| mem_reads | `index_join` | 5.78ms | 7.96ms | 1.4× | 1.7% | PASS |
| mem_reads | `index_join_scan` | 2.88ms | 4.81ms | 1.7× | 2.6% | PASS |
| mem_reads | `types_table_scan` | 770.16ms | 894.83ms | 1.2× | 0.3% | PASS |
| mem_reads | `table_scan` | 867.99ms | 998.76ms | 1.2× | 1.3% | PASS |
| mem_reads | `oltp_read_only` | 95.76ms | 108.86ms | 1.1× | 1.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 150.23ms | 188.32ms | 1.3× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 12.34ms | 19.80ms | 1.6× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 42.62ms | 74.57ms | 1.7× | 2.2% | PASS |
| mem_writes | `oltp_update_non_index` | 29.77ms | 48.65ms | 1.6× | 1.9% | PASS |
| mem_writes | `oltp_delete_insert` | 30.50ms | 54.04ms | 1.8× | 2.5% | PASS |
| mem_writes | `oltp_write_only` | 15.79ms | 32.54ms | 2.1× | 2.5% | PASS |
| mem_writes | `types_delete_insert` | 18.47ms | 32.17ms | 1.7× | 1.0% | PASS |
| mem_writes | `oltp_read_write` | 60.69ms | 89.06ms | 1.5× | 1.4% | PASS |
| file_reads | `oltp_point_select` | 39.87ms | 27.04ms | 0.7× | 1.3% | PASS |
| file_reads | `oltp_range_select` | 14.68ms | 15.04ms | 1.0× | 4.2% | PASS |
| file_reads | `oltp_sum_range` | 13.88ms | 13.91ms | 1.0× | 4.2% | PASS |
| file_reads | `oltp_order_range` | 2.67ms | 2.75ms | 1.0× | 2.8% | PASS |
| file_reads | `oltp_distinct_range` | 3.28ms | 3.44ms | 1.0× | 1.9% | PASS |
| file_reads | `oltp_index_scan` | 4.99ms | 4.28ms | 0.9× | 1.8% | PASS |
| file_reads | `select_random_points` | 21.52ms | 20.86ms | 1.0× | 1.6% | PASS |
| file_reads | `select_random_ranges` | 6.67ms | 5.80ms | 0.9× | 2.1% | PASS |
| file_reads | `covering_index_scan` | 6.83ms | 7.07ms | 1.0× | 0.3% | PASS |
| file_reads | `groupby_scan` | 25.11ms | 26.56ms | 1.1× | 0.4% | PASS |
| file_reads | `index_join` | 6.79ms | 7.86ms | 1.2× | 0.9% | PASS |
| file_reads | `index_join_scan` | 3.15ms | 4.96ms | 1.6× | 0.8% | PASS |
| file_reads | `types_table_scan` | 771.15ms | 892.18ms | 1.2× | 0.4% | PASS |
| file_reads | `table_scan` | 882.88ms | 995.53ms | 1.1× | 0.8% | PASS |
| file_reads | `oltp_read_only` | 123.82ms | 115.45ms | 0.9× | 0.5% | PASS |
| file_writes | `oltp_bulk_insert` | 159.12ms | 194.41ms | 1.2× | 0.7% | PASS |
| file_writes | `oltp_insert` | 15.36ms | 22.22ms | 1.4× | 1.5% | PASS |
| file_writes | `oltp_update_index` | 57.51ms | 86.98ms | 1.5× | 1.1% | PASS |
| file_writes | `oltp_update_non_index` | 42.72ms | 53.97ms | 1.3× | 2.4% | PASS |
| file_writes | `oltp_delete_insert` | 43.81ms | 63.99ms | 1.5× | 2.1% | PASS |
| file_writes | `oltp_write_only` | 27.81ms | 37.50ms | 1.3× | 3.1% | PASS |
| file_writes | `types_delete_insert` | 28.43ms | 35.74ms | 1.3× | 2.5% | PASS |
| file_writes | `oltp_read_write` | 74.28ms | 96.21ms | 1.3× | 1.1% | PASS |
| ac_reads | `oltp_point_select` | 27.57ms | 28.22ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 14.21ms | 15.36ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_sum_range` | 13.67ms | 14.22ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_order_range` | 2.70ms | 2.82ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_distinct_range` | 3.41ms | 3.52ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_index_scan` | 3.86ms | 4.47ms | 1.2× | 1.5% | PASS |
| ac_reads | `select_random_points` | 20.91ms | 21.46ms | 1.0× | 1.1% | PASS |
| ac_reads | `select_random_ranges` | 5.67ms | 6.07ms | 1.1× | 1.4% | PASS |
| ac_reads | `covering_index_scan` | 5.46ms | 7.08ms | 1.3× | 0.4% | PASS |
| ac_reads | `groupby_scan` | 24.96ms | 26.55ms | 1.1× | 0.5% | PASS |
| ac_reads | `index_join` | 6.07ms | 7.71ms | 1.3× | 1.4% | PASS |
| ac_reads | `index_join_scan` | 2.93ms | 4.72ms | 1.6× | 2.7% | PASS |
| ac_reads | `types_table_scan` | 772.26ms | 892.28ms | 1.2× | 0.3% | PASS |
| ac_reads | `table_scan` | 884.70ms | 997.73ms | 1.1× | 0.6% | PASS |
| ac_reads | `oltp_read_only` | 106.74ms | 116.16ms | 1.1× | 0.6% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 8.65ms | 12.37ms | 1.4× | 0.7% | PASS |
| ac_writes | `oltp_insert_ac` | 9.56ms | 19.45ms | 2.0× | 0.8% | PASS |
| ac_writes | `oltp_update_index_ac` | 10.05ms | 25.99ms | 2.6× | 1.3% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 8.67ms | 15.54ms | 1.8× | 1.8% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 9.30ms | 20.72ms | 2.2× | 1.2% | PASS |
| ac_writes | `oltp_write_only_ac` | 9.52ms | 20.33ms | 2.1× | 1.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 8.32ms | 15.90ms | 1.9× | 1.4% | PASS |
| ac_writes | `oltp_read_write_ac` | 12.28ms | 25.48ms | 2.1× | 1.9% | PASS |

</details>

</details>

## Version-control latency

Wall time: 4m 51s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 24.39ms | 38.00ms | 64.2% | 1.0% | PASS |
| `status_dirty_many_tables` | 26.87ms | 42.00ms | 64.0% | 0.8% | PASS |
| `diff_regular_working_one_table` | 20.95ms | 33.00ms | 63.5% | 1.0% | PASS |
| `diff_regular_working_many_tables` | 30.11ms | 50.00ms | 60.2% | 0.7% | PASS |
| `diff_stat_working_many_tables` | 30.00ms | 48.00ms | 62.5% | 0.6% | PASS |
| `diff_schema_working_many_tables` | 30.42ms | 48.00ms | 63.4% | 0.8% | PASS |
| `branch_list_many_branches` | 15.09ms | 25.00ms | 60.3% | 0.9% | PASS |
| `branch_create_delete` | 16.55ms | 27.00ms | 61.3% | 1.1% | PASS |
| `at_literal_deep_history` | 16.25ms | 28.00ms | 58.0% | 0.6% | PASS |
| `diff_literal_deep_history` | 16.25ms | 28.00ms | 58.1% | 0.5% | PASS |
| `history_literal_deep_history` | 17.24ms | 30.00ms | 57.5% | 0.9% | PASS |
| `checkout_branch_clean` | 23.41ms | 43.00ms | 54.4% | 1.0% | PASS |
| `merge_data_no_conflicts` | 18.29ms | 33.00ms | 55.4% | 0.8% | PASS |
| `merge_data_secondary_index` | 636.55ms | 944.00ms | 67.4% | 0.7% | PASS |
| `merge_schema_no_conflicts` | 14.86ms | 24.00ms | 61.9% | 1.7% | PASS |
| `merge_data_conflicts` | 20.88ms | 33.00ms | 63.3% | 0.8% | PASS |
| `merge_data_conflicts_with_resolve` | 21.39ms | 34.00ms | 62.9% | 1.0% | PASS |

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

candidate: 465e271.

Fixture file bytes: base 107,968,301; delta 115,879,032.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 49.545 | 48.236–64.430 | 46.869 | 2018.37 | 107968301 |
| push_initial | candidate | 348.466 | 345.667–355.016 | 346.819 | 286.97 | 107956807 |
| clone_initial | candidate | 467.796 | 464.495–471.152 | 457.794 | 213.77 | 108475105 |
| push_noop | candidate | 0.384 | 0.312–0.411 | 0.204 | — | 0 |
| fetch_noop | candidate | 0.097 | 0.093–0.137 | 0.098 | — | 0 |
| push_incremental | candidate | 137.025 | 135.546–138.201 | 135.546 | 7.30 | 7909775 |
| fetch_incremental | candidate | 60.535 | 59.188–61.314 | 59.446 | 16.52 | 7910084 |
| pull_fast_forward | candidate | 53.427 | 51.998–54.454 | 52.233 | 18.72 | 7910844 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 90.39 | 175.487 | 108.763 | 18329 | 2066 |
| clone_initial | candidate | 90.86 | 295.195 | 110.833 | 47270 | 2353 |
| push_noop | candidate | 41.42 | 0.073 | 0.000 | 27 | 2 |
| fetch_noop | candidate | 41.42 | 0.135 | 0.000 | 42 | 0 |
| push_incremental | candidate | 41.42 | 96.815 | 7.910 | 22399 | 125 |
| fetch_incremental | candidate | 41.42 | 34.472 | 7.910 | 10810 | 124 |
| pull_fast_forward | candidate | 41.42 | 26.497 | 8.959 | 7032 | 142 |

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

candidate: 465e271.

Fixture file bytes: base 1,087,718,066; delta 1,103,525,194.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 5624.643 | 5584.343–5680.042 | 308.130 | 177.79 | 1087718066 |
| push_initial | candidate | 9376.723 | 9324.209–9411.501 | 4254.883 | 106.65 | 1079566310 |
| clone_initial | candidate | 11571.967 | 11562.938–11642.455 | 6157.960 | 86.42 | 1102407396 |
| push_noop | candidate | 0.607 | 0.596–2.580 | 0.282 | — | 0 |
| fetch_noop | candidate | 0.186 | 0.163–0.202 | 0.188 | — | 0 |
| push_incremental | candidate | 1026.313 | 1015.956–1033.425 | 947.321 | 0.97 | 15806003 |
| fetch_incremental | candidate | 305.340 | 303.132–311.360 | 229.829 | 3.28 | 15806513 |
| pull_fast_forward | candidate | 314.818 | 303.717–358.303 | 223.439 | 3.18 | 15807273 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 134.78 | 2092.304 | 1087.622 | 263602 | 20541 |
| clone_initial | candidate | 124.05 | 3527.792 | 1133.868 | 614714 | 32259 |
| push_noop | candidate | 44.12 | 0.078 | 0.000 | 28 | 2 |
| fetch_noop | candidate | 44.12 | 0.137 | 0.000 | 42 | 0 |
| push_incremental | candidate | 44.84 | 620.240 | 15.928 | 150633 | 313 |
| fetch_incremental | candidate | 50.39 | 62.246 | 15.928 | 13900 | 313 |
| pull_fast_forward | candidate | 50.43 | 62.179 | 16.978 | 13895 | 331 |

</details>


</details>

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh`, `test/vc_perf_ceiling.sh`, and `test/remote_perf.py`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
