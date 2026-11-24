# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-11-24 11:22 UTC
>
> Commit: [`c35ff128f4fbf07bb973255a8c3611ee42bdd08c`](https://github.com/dolthub/doltlite/commit/c35ff128f4fbf07bb973255a8c3611ee42bdd08c)
>
> Runner: ubuntu24 20261116.133.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/45366138885)

The SQL workloads compare optimized DoltLite against stock SQLite on the same GitHub-hosted runner, alternating execution order on each repetition. Version-control sections measure DoltLite latency. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class. Mean multiple is the average over workloads of each workload's paired DoltLite/SQLite ratio, so every workload counts equally whatever its run time.

### In-memory

| Operation | Mean multiple | Paired-ratio noise | Result |
|---|---:|---:|---|
| Reads | 1.15× | 1.2% | **PASS** |
| Writes | 1.70× | 1.1% | **PASS** |

### File-backed

| Operation | Mean multiple | Paired-ratio noise | Result |
|---|---:|---:|---|
| Reads | 0.89× | 1.4% | **PASS** |
| Writes | 1.27× | 1.8% | **PASS** |
| Autocommit writes | 2.02× | 4.1% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.8× for a section average. Durable autocommit writes use 3.6× and 2.6× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | Mean multiple | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 1.17× | 1.4% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 1.12× | 1.4% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 1.15× | 1.6% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 1.15× | 0.8% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 1.65× | 1.4% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 1.77× | 1.2% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 1.76× | 1.1% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 1.63× | 1.0% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 0.85× | 1.4% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 0.84× | 1.4% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 0.86× | 1.7% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 0.99× | 1.0% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 1.24× | 1.7% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.16× | 2.8% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 1.32× | 1.5% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 1.37× | 1.8% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 1.10× | 1.3% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 1.03× | 1.3% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 1.07× | 1.7% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 1.11× | 1.6% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 2.02× | 4.3% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 2.01× | 4.0% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 1.98× | 5.9% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 2.06× | 1.5% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 23.49ms | 28.50ms | 1.2× | 1.4% | PASS |
| mem_reads | `oltp_range_select` | 10.14ms | 11.84ms | 1.2× | 2.3% | PASS |
| mem_reads | `oltp_sum_range` | 9.32ms | 11.26ms | 1.2× | 1.2% | PASS |
| mem_reads | `oltp_order_range` | 2.54ms | 2.85ms | 1.1× | 1.8% | PASS |
| mem_reads | `oltp_distinct_range` | 3.65ms | 3.99ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 3.80ms | 5.06ms | 1.3× | 2.0% | PASS |
| mem_reads | `select_random_points` | 12.66ms | 11.35ms | 0.9× | 2.8% | PASS |
| mem_reads | `select_random_ranges` | 4.47ms | 5.03ms | 1.1× | 1.5% | PASS |
| mem_reads | `covering_index_scan` | 7.65ms | 11.23ms | 1.5× | 1.2% | PASS |
| mem_reads | `groupby_scan` | 30.09ms | 32.15ms | 1.1× | 0.6% | PASS |
| mem_reads | `index_join` | 5.75ms | 8.19ms | 1.4× | 1.7% | PASS |
| mem_reads | `index_join_scan` | 3.23ms | 5.47ms | 1.7× | 2.0% | PASS |
| mem_reads | `types_table_scan` | 1.02s | 771.33ms | 0.8× | 0.6% | PASS |
| mem_reads | `table_scan` | 1.14s | 892.73ms | 0.8× | 0.7% | PASS |
| mem_reads | `oltp_read_only` | 102.05ms | 116.91ms | 1.1× | 1.4% | PASS |
| mem_writes | `oltp_bulk_insert` | 176.91ms | 266.66ms | 1.5× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 15.09ms | 26.03ms | 1.7× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 49.51ms | 90.13ms | 1.8× | 1.4% | PASS |
| mem_writes | `oltp_update_non_index` | 34.61ms | 55.20ms | 1.6× | 1.5% | PASS |
| mem_writes | `oltp_delete_insert` | 44.47ms | 73.95ms | 1.7× | 1.6% | PASS |
| mem_writes | `oltp_write_only` | 21.73ms | 43.81ms | 2.0× | 1.4% | PASS |
| mem_writes | `types_delete_insert` | 24.26ms | 32.03ms | 1.3× | 1.8% | PASS |
| mem_writes | `oltp_read_write` | 66.60ms | 103.34ms | 1.6× | 1.4% | PASS |
| file_reads | `oltp_point_select` | 87.18ms | 45.69ms | 0.5× | 1.3% | PASS |
| file_reads | `oltp_range_select` | 17.27ms | 13.86ms | 0.8× | 2.4% | PASS |
| file_reads | `oltp_sum_range` | 16.19ms | 13.24ms | 0.8× | 1.8% | PASS |
| file_reads | `oltp_order_range` | 3.30ms | 3.12ms | 0.9× | 1.6% | PASS |
| file_reads | `oltp_distinct_range` | 4.45ms | 4.26ms | 1.0× | 1.4% | PASS |
| file_reads | `oltp_index_scan` | 10.41ms | 7.27ms | 0.7× | 1.0% | PASS |
| file_reads | `select_random_points` | 20.01ms | 13.42ms | 0.7× | 2.4% | PASS |
| file_reads | `select_random_ranges` | 11.13ms | 6.95ms | 0.6× | 1.7% | PASS |
| file_reads | `covering_index_scan` | 14.39ms | 13.38ms | 0.9× | 1.3% | PASS |
| file_reads | `groupby_scan` | 30.93ms | 32.57ms | 1.1× | 0.9% | PASS |
| file_reads | `index_join` | 9.37ms | 9.78ms | 1.0× | 1.5% | PASS |
| file_reads | `index_join_scan` | 4.13ms | 5.75ms | 1.4× | 2.0% | PASS |
| file_reads | `types_table_scan` | 1.02s | 772.83ms | 0.8× | 0.7% | PASS |
| file_reads | `table_scan` | 1.14s | 887.95ms | 0.8× | 0.7% | PASS |
| file_reads | `oltp_read_only` | 197.17ms | 143.62ms | 0.7× | 1.4% | PASS |
| file_writes | `oltp_bulk_insert` | 193.28ms | 279.98ms | 1.4× | 1.1% | PASS |
| file_writes | `oltp_insert` | 21.95ms | 29.77ms | 1.4× | 1.4% | PASS |
| file_writes | `oltp_update_index` | 76.38ms | 103.72ms | 1.4× | 1.9% | PASS |
| file_writes | `oltp_update_non_index` | 57.59ms | 62.04ms | 1.1× | 2.0% | PASS |
| file_writes | `oltp_delete_insert` | 67.22ms | 82.91ms | 1.2× | 1.8% | PASS |
| file_writes | `oltp_write_only` | 44.75ms | 54.90ms | 1.2× | 1.9% | PASS |
| file_writes | `types_delete_insert` | 40.15ms | 38.99ms | 1.0× | 1.2% | PASS |
| file_writes | `oltp_read_write` | 90.40ms | 113.55ms | 1.3× | 1.5% | PASS |
| ac_reads | `oltp_point_select` | 44.55ms | 45.89ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 12.72ms | 13.80ms | 1.1× | 2.0% | PASS |
| ac_reads | `oltp_sum_range` | 11.77ms | 13.25ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_order_range` | 2.86ms | 3.14ms | 1.1× | 1.1% | PASS |
| ac_reads | `oltp_distinct_range` | 3.97ms | 4.26ms | 1.1× | 1.2% | PASS |
| ac_reads | `oltp_index_scan` | 6.13ms | 7.26ms | 1.2× | 2.1% | PASS |
| ac_reads | `select_random_points` | 15.42ms | 13.39ms | 0.9× | 2.6% | PASS |
| ac_reads | `select_random_ranges` | 6.78ms | 6.97ms | 1.0× | 1.4% | PASS |
| ac_reads | `covering_index_scan` | 9.98ms | 13.37ms | 1.3× | 1.2% | PASS |
| ac_reads | `groupby_scan` | 30.42ms | 32.44ms | 1.1× | 0.8% | PASS |
| ac_reads | `index_join` | 7.10ms | 9.78ms | 1.4× | 2.0% | PASS |
| ac_reads | `index_join_scan` | 3.66ms | 5.78ms | 1.6× | 1.9% | PASS |
| ac_reads | `types_table_scan` | 1.02s | 772.86ms | 0.8× | 0.6% | PASS |
| ac_reads | `table_scan` | 1.15s | 890.19ms | 0.8× | 0.6% | PASS |
| ac_reads | `oltp_read_only` | 135.17ms | 143.43ms | 1.1× | 1.4% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 20.78ms | 33.19ms | 1.6× | 4.7% | PASS |
| ac_writes | `oltp_insert_ac` | 22.94ms | 45.02ms | 2.0× | 3.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 24.75ms | 60.26ms | 2.4× | 4.3% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.39ms | 38.98ms | 1.8× | 4.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 22.33ms | 49.05ms | 2.2× | 4.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 22.70ms | 48.30ms | 2.1× | 3.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 20.14ms | 39.79ms | 2.0× | 4.8% | PASS |
| ac_writes | `oltp_read_write_ac` | 27.25ms | 55.21ms | 2.0× | 2.9% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 35.06ms | 38.48ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_range_select` | 16.04ms | 16.53ms | 1.0× | 1.4% | PASS |
| mem_reads | `oltp_sum_range` | 14.99ms | 15.69ms | 1.0× | 1.0% | PASS |
| mem_reads | `oltp_order_range` | 3.35ms | 3.53ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_distinct_range` | 4.52ms | 4.63ms | 1.0× | 0.7% | PASS |
| mem_reads | `oltp_index_scan` | 3.96ms | 6.25ms | 1.6× | 1.1% | PASS |
| mem_reads | `select_random_points` | 20.68ms | 21.38ms | 1.0× | 2.1% | PASS |
| mem_reads | `select_random_ranges` | 6.37ms | 6.64ms | 1.0× | 1.6% | PASS |
| mem_reads | `covering_index_scan` | 7.71ms | 11.41ms | 1.5× | 1.0% | PASS |
| mem_reads | `groupby_scan` | 34.26ms | 36.32ms | 1.1× | 0.7% | PASS |
| mem_reads | `index_join` | 9.84ms | 9.49ms | 1.0× | 1.7% | PASS |
| mem_reads | `index_join_scan` | 3.61ms | 6.12ms | 1.7× | 1.9% | PASS |
| mem_reads | `types_table_scan` | 1.08s | 898.97ms | 0.8× | 1.9% | PASS |
| mem_reads | `table_scan` | 1.20s | 998.65ms | 0.8× | 1.4% | PASS |
| mem_reads | `oltp_read_only` | 136.78ms | 146.50ms | 1.1× | 1.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 243.47ms | 338.38ms | 1.4× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 18.00ms | 38.48ms | 2.1× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 65.13ms | 136.58ms | 2.1× | 1.0% | PASS |
| mem_writes | `oltp_update_non_index` | 49.56ms | 86.15ms | 1.7× | 1.9% | PASS |
| mem_writes | `oltp_delete_insert` | 53.72ms | 101.58ms | 1.9× | 1.3% | PASS |
| mem_writes | `oltp_write_only` | 28.34ms | 61.06ms | 2.2× | 0.9% | PASS |
| mem_writes | `types_delete_insert` | 39.20ms | 52.69ms | 1.3× | 1.3% | PASS |
| mem_writes | `oltp_read_write` | 102.25ms | 145.99ms | 1.4× | 1.3% | PASS |
| file_reads | `oltp_point_select` | 102.78ms | 56.16ms | 0.5× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 24.27ms | 18.61ms | 0.8× | 1.8% | PASS |
| file_reads | `oltp_sum_range` | 22.28ms | 17.61ms | 0.8× | 1.4% | PASS |
| file_reads | `oltp_order_range` | 4.18ms | 3.78ms | 0.9× | 1.5% | PASS |
| file_reads | `oltp_distinct_range` | 5.35ms | 4.89ms | 0.9× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 10.88ms | 8.36ms | 0.8× | 1.1% | PASS |
| file_reads | `select_random_points` | 30.60ms | 24.54ms | 0.8× | 2.0% | PASS |
| file_reads | `select_random_ranges` | 13.72ms | 8.64ms | 0.6× | 1.4% | PASS |
| file_reads | `covering_index_scan` | 14.83ms | 13.77ms | 0.9× | 1.2% | PASS |
| file_reads | `groupby_scan` | 35.91ms | 37.11ms | 1.0× | 0.7% | PASS |
| file_reads | `index_join` | 14.51ms | 11.62ms | 0.8× | 1.9% | PASS |
| file_reads | `index_join_scan` | 4.67ms | 6.71ms | 1.4× | 2.4% | PASS |
| file_reads | `types_table_scan` | 1.14s | 916.90ms | 0.8× | 1.4% | PASS |
| file_reads | `table_scan` | 1.32s | 1.02s | 0.8× | 1.1% | PASS |
| file_reads | `oltp_read_only` | 248.19ms | 176.52ms | 0.7× | 1.1% | PASS |
| file_writes | `oltp_bulk_insert` | 268.47ms | 358.17ms | 1.3× | 1.0% | PASS |
| file_writes | `oltp_insert` | 26.00ms | 44.63ms | 1.7× | 1.8% | PASS |
| file_writes | `oltp_update_index` | 138.91ms | 160.19ms | 1.2× | 3.7% | PASS |
| file_writes | `oltp_update_non_index` | 109.16ms | 101.85ms | 0.9× | 10.9% | PASS |
| file_writes | `oltp_delete_insert` | 97.98ms | 120.79ms | 1.2× | 2.0% | PASS |
| file_writes | `oltp_write_only` | 80.72ms | 74.93ms | 0.9× | 9.7% | PASS |
| file_writes | `types_delete_insert` | 72.13ms | 64.42ms | 0.9× | 1.2% | PASS |
| file_writes | `oltp_read_write` | 148.23ms | 158.59ms | 1.1× | 5.4% | PASS |
| ac_reads | `oltp_point_select` | 58.90ms | 56.35ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_range_select` | 20.17ms | 18.90ms | 0.9× | 2.5% | PASS |
| ac_reads | `oltp_sum_range` | 18.53ms | 17.97ms | 1.0× | 2.0% | PASS |
| ac_reads | `oltp_order_range` | 3.82ms | 3.83ms | 1.0× | 1.1% | PASS |
| ac_reads | `oltp_distinct_range` | 4.97ms | 4.93ms | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_index_scan` | 6.71ms | 8.53ms | 1.3× | 1.6% | PASS |
| ac_reads | `select_random_points` | 28.04ms | 25.69ms | 0.9× | 3.9% | PASS |
| ac_reads | `select_random_ranges` | 9.45ms | 8.75ms | 0.9× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 10.50ms | 13.86ms | 1.3× | 1.3% | PASS |
| ac_reads | `groupby_scan` | 35.43ms | 37.08ms | 1.0× | 0.7% | PASS |
| ac_reads | `index_join` | 12.56ms | 11.80ms | 0.9× | 1.7% | PASS |
| ac_reads | `index_join_scan` | 4.27ms | 7.08ms | 1.7× | 2.7% | PASS |
| ac_reads | `types_table_scan` | 1.24s | 966.51ms | 0.8× | 1.2% | PASS |
| ac_reads | `table_scan` | 1.39s | 1.05s | 0.8× | 0.9% | PASS |
| ac_reads | `oltp_read_only` | 185.98ms | 178.91ms | 1.0× | 1.1% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.67ms | 32.33ms | 1.5× | 4.2% | PASS |
| ac_writes | `oltp_insert_ac` | 24.25ms | 43.67ms | 1.8× | 4.8% | PASS |
| ac_writes | `oltp_update_index_ac` | 25.13ms | 65.65ms | 2.6× | 4.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 20.76ms | 40.00ms | 1.9× | 3.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.19ms | 56.02ms | 2.3× | 3.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 24.69ms | 52.93ms | 2.1× | 4.3% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.18ms | 40.74ms | 1.8× | 3.8% | PASS |
| ac_writes | `oltp_read_write_ac` | 29.86ms | 60.12ms | 2.0× | 2.4% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 34.18ms | 37.51ms | 1.1× | 2.7% | PASS |
| mem_reads | `oltp_range_select` | 15.37ms | 16.21ms | 1.1× | 1.7% | PASS |
| mem_reads | `oltp_sum_range` | 13.92ms | 15.12ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_order_range` | 3.06ms | 3.47ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_distinct_range` | 4.36ms | 4.62ms | 1.1× | 1.6% | PASS |
| mem_reads | `oltp_index_scan` | 3.89ms | 6.14ms | 1.6× | 2.0% | PASS |
| mem_reads | `select_random_points` | 20.30ms | 21.06ms | 1.0× | 2.4% | PASS |
| mem_reads | `select_random_ranges` | 6.06ms | 6.56ms | 1.1× | 2.0% | PASS |
| mem_reads | `covering_index_scan` | 7.74ms | 11.38ms | 1.5× | 0.8% | PASS |
| mem_reads | `groupby_scan` | 33.66ms | 36.21ms | 1.1× | 0.9% | PASS |
| mem_reads | `index_join` | 10.27ms | 10.06ms | 1.0× | 3.3% | PASS |
| mem_reads | `index_join_scan` | 3.60ms | 6.22ms | 1.7× | 2.4% | PASS |
| mem_reads | `types_table_scan` | 1.03s | 891.70ms | 0.9× | 1.1% | PASS |
| mem_reads | `table_scan` | 1.14s | 990.61ms | 0.9× | 0.5% | PASS |
| mem_reads | `oltp_read_only` | 133.22ms | 144.32ms | 1.1× | 0.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 246.42ms | 339.88ms | 1.4× | 0.9% | PASS |
| mem_writes | `oltp_insert` | 18.54ms | 38.15ms | 2.1× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 62.81ms | 131.56ms | 2.1× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 47.06ms | 81.59ms | 1.7× | 1.5% | PASS |
| mem_writes | `oltp_delete_insert` | 51.52ms | 98.12ms | 1.9× | 1.0% | PASS |
| mem_writes | `oltp_write_only` | 27.25ms | 58.20ms | 2.1× | 1.2% | PASS |
| mem_writes | `types_delete_insert` | 38.65ms | 50.39ms | 1.3× | 1.1% | PASS |
| mem_writes | `oltp_read_write` | 97.64ms | 139.98ms | 1.4× | 2.0% | PASS |
| file_reads | `oltp_point_select` | 100.75ms | 54.66ms | 0.5× | 1.4% | PASS |
| file_reads | `oltp_range_select` | 22.63ms | 18.24ms | 0.8× | 1.7% | PASS |
| file_reads | `oltp_sum_range` | 22.33ms | 17.30ms | 0.8× | 1.7% | PASS |
| file_reads | `oltp_order_range` | 4.12ms | 3.81ms | 0.9× | 2.1% | PASS |
| file_reads | `oltp_distinct_range` | 5.31ms | 4.89ms | 0.9× | 2.2% | PASS |
| file_reads | `oltp_index_scan` | 10.60ms | 8.37ms | 0.8× | 2.2% | PASS |
| file_reads | `select_random_points` | 28.80ms | 23.73ms | 0.8× | 2.9% | PASS |
| file_reads | `select_random_ranges` | 13.40ms | 8.58ms | 0.6× | 2.2% | PASS |
| file_reads | `covering_index_scan` | 14.65ms | 13.74ms | 0.9× | 1.0% | PASS |
| file_reads | `groupby_scan` | 35.43ms | 36.74ms | 1.0× | 1.3% | PASS |
| file_reads | `index_join` | 13.78ms | 11.73ms | 0.9× | 2.5% | PASS |
| file_reads | `index_join_scan` | 4.59ms | 6.67ms | 1.5× | 2.3% | PASS |
| file_reads | `types_table_scan` | 1.03s | 899.40ms | 0.9× | 0.7% | PASS |
| file_reads | `table_scan` | 1.15s | 1.00s | 0.9× | 1.1% | PASS |
| file_reads | `oltp_read_only` | 235.80ms | 171.06ms | 0.7× | 1.1% | PASS |
| file_writes | `oltp_bulk_insert` | 267.42ms | 355.14ms | 1.3× | 0.8% | PASS |
| file_writes | `oltp_insert` | 25.87ms | 43.71ms | 1.7× | 1.8% | PASS |
| file_writes | `oltp_update_index` | 99.25ms | 154.25ms | 1.6× | 1.4% | PASS |
| file_writes | `oltp_update_non_index` | 84.32ms | 97.34ms | 1.2× | 7.3% | PASS |
| file_writes | `oltp_delete_insert` | 84.86ms | 115.53ms | 1.4× | 1.0% | PASS |
| file_writes | `oltp_write_only` | 56.67ms | 72.76ms | 1.3× | 1.5% | PASS |
| file_writes | `types_delete_insert` | 63.15ms | 60.98ms | 1.0× | 1.8% | PASS |
| file_writes | `oltp_read_write` | 128.45ms | 154.05ms | 1.2× | 1.5% | PASS |
| ac_reads | `oltp_point_select` | 57.71ms | 54.96ms | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_range_select` | 18.68ms | 18.20ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_sum_range` | 16.89ms | 17.17ms | 1.0× | 1.9% | PASS |
| ac_reads | `oltp_order_range` | 3.74ms | 3.79ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_distinct_range` | 4.85ms | 4.92ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_index_scan` | 6.34ms | 8.38ms | 1.3× | 2.5% | PASS |
| ac_reads | `select_random_points` | 24.07ms | 23.65ms | 1.0× | 1.6% | PASS |
| ac_reads | `select_random_ranges` | 8.74ms | 8.52ms | 1.0× | 1.9% | PASS |
| ac_reads | `covering_index_scan` | 10.29ms | 13.71ms | 1.3× | 1.9% | PASS |
| ac_reads | `groupby_scan` | 34.90ms | 36.84ms | 1.1× | 0.9% | PASS |
| ac_reads | `index_join` | 11.74ms | 11.76ms | 1.0× | 2.5% | PASS |
| ac_reads | `index_join_scan` | 4.13ms | 6.66ms | 1.6× | 1.2% | PASS |
| ac_reads | `types_table_scan` | 1.03s | 898.76ms | 0.9× | 0.7% | PASS |
| ac_reads | `table_scan` | 1.15s | 999.99ms | 0.9× | 0.9% | PASS |
| ac_reads | `oltp_read_only` | 171.71ms | 171.05ms | 1.0× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 24.05ms | 34.61ms | 1.4× | 4.8% | PASS |
| ac_writes | `oltp_insert_ac` | 26.73ms | 52.69ms | 2.0× | 4.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.29ms | 69.27ms | 2.5× | 6.5% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.68ms | 43.59ms | 1.8× | 6.5% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.90ms | 55.31ms | 2.1× | 5.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 24.88ms | 53.33ms | 2.1× | 4.0% | PASS |
| ac_writes | `types_delete_insert_ac` | 24.31ms | 42.85ms | 1.8× | 9.1% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.90ms | 61.56ms | 1.9× | 6.5% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 21.98ms | 24.46ms | 1.1× | 0.7% | PASS |
| mem_reads | `oltp_range_select` | 14.50ms | 15.69ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_sum_range` | 13.82ms | 14.56ms | 1.1× | 0.7% | PASS |
| mem_reads | `oltp_order_range` | 2.71ms | 2.86ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_distinct_range` | 3.38ms | 3.56ms | 1.1× | 0.7% | PASS |
| mem_reads | `oltp_index_scan` | 3.29ms | 4.40ms | 1.3× | 1.8% | PASS |
| mem_reads | `select_random_points` | 21.22ms | 21.84ms | 1.0× | 1.1% | PASS |
| mem_reads | `select_random_ranges` | 5.16ms | 5.93ms | 1.2× | 1.0% | PASS |
| mem_reads | `covering_index_scan` | 4.79ms | 7.05ms | 1.5× | 0.5% | PASS |
| mem_reads | `groupby_scan` | 25.14ms | 27.98ms | 1.1× | 0.4% | PASS |
| mem_reads | `index_join` | 5.77ms | 8.24ms | 1.4× | 0.8% | PASS |
| mem_reads | `index_join_scan` | 2.90ms | 4.91ms | 1.7× | 1.8% | PASS |
| mem_reads | `types_table_scan` | 773.74ms | 586.60ms | 0.8× | 0.6% | PASS |
| mem_reads | `table_scan` | 886.44ms | 679.61ms | 0.8× | 1.0% | PASS |
| mem_reads | `oltp_read_only` | 98.36ms | 111.86ms | 1.1× | 0.5% | PASS |
| mem_writes | `oltp_bulk_insert` | 151.06ms | 194.11ms | 1.3× | 0.4% | PASS |
| mem_writes | `oltp_insert` | 12.43ms | 21.49ms | 1.7× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 47.01ms | 86.02ms | 1.8× | 1.0% | PASS |
| mem_writes | `oltp_update_non_index` | 33.63ms | 52.58ms | 1.6× | 1.1% | PASS |
| mem_writes | `oltp_delete_insert` | 34.61ms | 62.46ms | 1.8× | 1.1% | PASS |
| mem_writes | `oltp_write_only` | 18.69ms | 35.50ms | 1.9× | 1.2% | PASS |
| mem_writes | `types_delete_insert` | 21.34ms | 31.17ms | 1.5× | 0.9% | PASS |
| mem_writes | `oltp_read_write` | 65.97ms | 95.43ms | 1.4× | 0.7% | PASS |
| file_reads | `oltp_point_select` | 42.19ms | 29.09ms | 0.7× | 0.7% | PASS |
| file_reads | `oltp_range_select` | 16.36ms | 16.16ms | 1.0× | 0.9% | PASS |
| file_reads | `oltp_sum_range` | 15.94ms | 15.08ms | 0.9× | 0.6% | PASS |
| file_reads | `oltp_order_range` | 2.89ms | 2.94ms | 1.0× | 1.2% | PASS |
| file_reads | `oltp_distinct_range` | 3.60ms | 3.67ms | 1.0× | 2.0% | PASS |
| file_reads | `oltp_index_scan` | 5.35ms | 5.00ms | 0.9× | 1.2% | PASS |
| file_reads | `select_random_points` | 23.09ms | 22.44ms | 1.0× | 0.9% | PASS |
| file_reads | `select_random_ranges` | 7.28ms | 6.46ms | 0.9× | 1.0% | PASS |
| file_reads | `covering_index_scan` | 6.89ms | 7.54ms | 1.1× | 0.8% | PASS |
| file_reads | `groupby_scan` | 25.37ms | 28.09ms | 1.1× | 0.4% | PASS |
| file_reads | `index_join` | 6.99ms | 8.53ms | 1.2× | 1.5% | PASS |
| file_reads | `index_join_scan` | 3.20ms | 5.17ms | 1.6× | 2.2% | PASS |
| file_reads | `types_table_scan` | 777.40ms | 587.24ms | 0.8× | 0.8% | PASS |
| file_reads | `table_scan` | 888.83ms | 681.80ms | 0.8× | 2.3% | PASS |
| file_reads | `oltp_read_only` | 132.82ms | 119.69ms | 0.9× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 161.19ms | 201.96ms | 1.3× | 0.6% | PASS |
| file_writes | `oltp_insert` | 15.75ms | 24.62ms | 1.6× | 1.7% | PASS |
| file_writes | `oltp_update_index` | 59.59ms | 89.77ms | 1.5× | 2.0% | PASS |
| file_writes | `oltp_update_non_index` | 44.81ms | 55.71ms | 1.2× | 1.5% | PASS |
| file_writes | `oltp_delete_insert` | 46.23ms | 65.59ms | 1.4× | 2.1% | PASS |
| file_writes | `oltp_write_only` | 28.70ms | 40.88ms | 1.4× | 1.9% | PASS |
| file_writes | `types_delete_insert` | 28.73ms | 35.99ms | 1.3× | 2.1% | PASS |
| file_writes | `oltp_read_write` | 75.62ms | 99.53ms | 1.3× | 1.3% | PASS |
| ac_reads | `oltp_point_select` | 28.12ms | 28.74ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_range_select` | 14.63ms | 15.88ms | 1.1× | 1.7% | PASS |
| ac_reads | `oltp_sum_range` | 13.55ms | 14.62ms | 1.1× | 2.2% | PASS |
| ac_reads | `oltp_order_range` | 2.56ms | 2.80ms | 1.1× | 2.5% | PASS |
| ac_reads | `oltp_distinct_range` | 3.33ms | 3.55ms | 1.1× | 1.6% | PASS |
| ac_reads | `oltp_index_scan` | 3.73ms | 4.26ms | 1.1× | 1.7% | PASS |
| ac_reads | `select_random_points` | 20.73ms | 21.21ms | 1.0× | 1.6% | PASS |
| ac_reads | `select_random_ranges` | 5.74ms | 6.19ms | 1.1× | 1.3% | PASS |
| ac_reads | `covering_index_scan` | 5.51ms | 7.36ms | 1.3× | 0.4% | PASS |
| ac_reads | `groupby_scan` | 25.10ms | 27.98ms | 1.1× | 0.4% | PASS |
| ac_reads | `index_join` | 6.29ms | 8.33ms | 1.3× | 1.5% | PASS |
| ac_reads | `index_join_scan` | 3.06ms | 4.94ms | 1.6× | 1.9% | PASS |
| ac_reads | `types_table_scan` | 771.65ms | 584.75ms | 0.8× | 0.4% | PASS |
| ac_reads | `table_scan` | 884.53ms | 679.61ms | 0.8× | 1.6% | PASS |
| ac_reads | `oltp_read_only` | 110.37ms | 118.80ms | 1.1× | 1.6% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 8.69ms | 12.56ms | 1.4× | 0.7% | PASS |
| ac_writes | `oltp_insert_ac` | 9.80ms | 20.76ms | 2.1× | 1.9% | PASS |
| ac_writes | `oltp_update_index_ac` | 10.49ms | 28.11ms | 2.7× | 1.2% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 8.94ms | 15.99ms | 1.8× | 1.4% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 9.52ms | 22.12ms | 2.3× | 1.7% | PASS |
| ac_writes | `oltp_write_only_ac` | 9.70ms | 21.50ms | 2.2× | 2.5% | PASS |
| ac_writes | `types_delete_insert_ac` | 8.56ms | 16.57ms | 1.9× | 1.6% | PASS |
| ac_writes | `oltp_read_write_ac` | 12.83ms | 26.39ms | 2.1× | 1.1% | PASS |

</details>

</details>

## Version-control latency

Wall time: 4m 46s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 22.45ms | 38.00ms | 59.1% | 1.5% | PASS |
| `status_dirty_many_tables` | 24.74ms | 42.00ms | 58.9% | 0.6% | PASS |
| `diff_regular_working_one_table` | 21.30ms | 33.00ms | 64.5% | 0.8% | PASS |
| `diff_regular_working_many_tables` | 28.33ms | 50.00ms | 56.7% | 0.7% | PASS |
| `diff_stat_working_many_tables` | 28.59ms | 48.00ms | 59.6% | 0.8% | PASS |
| `diff_schema_working_many_tables` | 28.77ms | 48.00ms | 59.9% | 0.8% | PASS |
| `branch_list_many_branches` | 15.42ms | 25.00ms | 61.7% | 0.8% | PASS |
| `branch_create_delete` | 16.64ms | 27.00ms | 61.6% | 0.8% | PASS |
| `at_literal_deep_history` | 16.40ms | 28.00ms | 58.6% | 0.7% | PASS |
| `diff_literal_deep_history` | 16.31ms | 28.00ms | 58.2% | 0.8% | PASS |
| `history_literal_deep_history` | 16.70ms | 30.00ms | 55.7% | 0.9% | PASS |
| `checkout_branch_clean` | 21.82ms | 43.00ms | 50.8% | 0.8% | PASS |
| `merge_data_no_conflicts` | 17.56ms | 33.00ms | 53.2% | 0.8% | PASS |
| `merge_data_secondary_index` | 606.14ms | 944.00ms | 64.2% | 1.1% | PASS |
| `merge_schema_no_conflicts` | 14.36ms | 24.00ms | 59.8% | 0.9% | PASS |
| `merge_data_conflicts` | 20.21ms | 33.00ms | 61.2% | 0.6% | PASS |
| `merge_data_conflicts_with_resolve` | 20.85ms | 34.00ms | 61.3% | 0.6% | PASS |

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

candidate: c35ff12.

Fixture file bytes: base 107,968,301; delta 115,879,032.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 56.341 | 51.337–62.853 | 52.224 | 1774.91 | 107968301 |
| push_initial | candidate | 299.059 | 296.903–300.334 | 296.643 | 334.38 | 107956807 |
| clone_initial | candidate | 330.796 | 329.661–338.965 | 322.578 | 302.30 | 108475105 |
| push_noop | candidate | 0.361 | 0.349–0.396 | 0.195 | — | 0 |
| fetch_noop | candidate | 0.085 | 0.084–0.090 | 0.086 | — | 0 |
| push_incremental | candidate | 42.158 | 41.897–46.744 | 40.904 | 23.72 | 7909775 |
| fetch_incremental | candidate | 48.783 | 48.223–50.420 | 47.562 | 20.50 | 7910084 |
| pull_fast_forward | candidate | 49.189 | 48.028–50.212 | 47.695 | 20.33 | 7910844 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 90.45 | 175.487 | 108.763 | 18329 | 2066 |
| clone_initial | candidate | 91.45 | 215.906 | 110.833 | 27898 | 2353 |
| push_noop | candidate | 41.46 | 0.073 | 0.000 | 27 | 2 |
| fetch_noop | candidate | 41.46 | 0.068 | 0.000 | 22 | 0 |
| push_incremental | candidate | 41.46 | 10.743 | 7.910 | 1375 | 125 |
| fetch_incremental | candidate | 41.46 | 26.494 | 7.910 | 7016 | 124 |
| pull_fast_forward | candidate | 41.46 | 26.497 | 8.959 | 7032 | 142 |

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

candidate: c35ff12.

Fixture file bytes: base 1,087,718,066; delta 1,103,525,194.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 3232.826 | 3221.385–4157.002 | 572.788 | 309.33 | 1087718066 |
| push_initial | candidate | 5853.342 | 5790.045–5867.532 | 3194.936 | 170.84 | 1079566310 |
| clone_initial | candidate | 4357.721 | 4329.964–4876.995 | 3341.068 | 229.48 | 1102407396 |
| push_noop | candidate | 0.448 | 0.441–0.449 | 0.212 | — | 0 |
| fetch_noop | candidate | 0.080 | 0.076–0.089 | 0.082 | — | 0 |
| push_incremental | candidate | 186.767 | 185.821–191.310 | 184.214 | 5.35 | 15806003 |
| fetch_incremental | candidate | 189.056 | 188.587–192.289 | 185.298 | 5.29 | 15806513 |
| pull_fast_forward | candidate | 188.660 | 186.128–193.426 | 183.847 | 5.30 | 15807273 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 134.88 | 2091.203 | 1087.622 | 263333 | 20541 |
| clone_initial | candidate | 131.74 | 2178.002 | 1133.868 | 283999 | 32259 |
| push_noop | candidate | 44.22 | 0.078 | 0.000 | 28 | 2 |
| fetch_noop | candidate | 44.22 | 0.069 | 0.000 | 22 | 0 |
| push_incremental | candidate | 56.41 | 46.456 | 15.928 | 10046 | 313 |
| fetch_incremental | candidate | 50.47 | 62.036 | 15.928 | 13845 | 313 |
| pull_fast_forward | candidate | 50.63 | 62.039 | 16.978 | 13861 | 331 |

</details>


</details>

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh`, `test/vc_perf_ceiling.sh`, and `test/remote_perf.py`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
