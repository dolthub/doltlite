# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-11-22 11:22 UTC
>
> Commit: [`76f30e77f1ee4dd56dbe83b0810ca64b4c7a8a01`](https://github.com/dolthub/doltlite/commit/76f30e77f1ee4dd56dbe83b0810ca64b4c7a8a01)
>
> Runner: ubuntu24 20261116.133.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/45008036745)

The SQL workloads compare optimized DoltLite against stock SQLite on the same GitHub-hosted runner, alternating execution order on each repetition. Version-control sections measure DoltLite latency. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class. Mean multiple is the average over workloads of each workload's paired DoltLite/SQLite ratio, so every workload counts equally whatever its run time.

### In-memory

| Operation | Mean multiple | Paired-ratio noise | Result |
|---|---:|---:|---|
| Reads | 1.12× | 1.1% | **PASS** |
| Writes | 1.62× | 1.0% | **PASS** |

### File-backed

| Operation | Mean multiple | Paired-ratio noise | Result |
|---|---:|---:|---|
| Reads | 0.92× | 1.0% | **PASS** |
| Writes | 1.10× | 2.0% | **PASS** |
| Autocommit writes | 1.92× | 3.6% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.8× for a section average. Durable autocommit writes use 3.6× and 2.6× ceilings respectively. A breach counts only when the median of the paired ratios exceeds the same ceiling.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | Mean multiple | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 1.13× | 1.0% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 1.13× | 1.6% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 1.08× | 1.7% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 1.15× | 0.8% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 1.61× | 1.0% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 1.65× | 1.2% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 1.58× | 1.6% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 1.64× | 0.9% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 0.93× | 0.8% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 0.84× | 1.6% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 0.93× | 1.5% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 0.98× | 1.0% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 0.98× | 1.7% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.09× | 3.2% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 1.32× | 2.5% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 1.02× | 1.6% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 1.08× | 0.8% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 1.03× | 1.4% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 1.04× | 1.0% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 1.10× | 0.9% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 1.96× | 3.8% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 1.97× | 5.3% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 2.08× | 1.3% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 1.66× | 2.9% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 21.46ms | 23.75ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_range_select` | 9.75ms | 11.20ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_sum_range` | 8.66ms | 10.10ms | 1.2× | 1.2% | PASS |
| mem_reads | `oltp_order_range` | 2.51ms | 2.72ms | 1.1× | 1.7% | PASS |
| mem_reads | `oltp_distinct_range` | 3.36ms | 3.70ms | 1.1× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 3.47ms | 4.26ms | 1.2× | 1.3% | PASS |
| mem_reads | `select_random_points` | 12.08ms | 10.62ms | 0.9× | 1.0% | PASS |
| mem_reads | `select_random_ranges` | 3.91ms | 4.04ms | 1.0× | 0.9% | PASS |
| mem_reads | `covering_index_scan` | 6.31ms | 8.89ms | 1.4× | 1.1% | PASS |
| mem_reads | `groupby_scan` | 29.38ms | 31.27ms | 1.1× | 0.4% | PASS |
| mem_reads | `index_join` | 5.05ms | 7.23ms | 1.4× | 1.0% | PASS |
| mem_reads | `index_join_scan` | 2.77ms | 4.76ms | 1.7× | 1.0% | PASS |
| mem_reads | `types_table_scan` | 957.21ms | 672.75ms | 0.7× | 0.3% | PASS |
| mem_reads | `table_scan` | 1.08s | 790.95ms | 0.7× | 0.6% | PASS |
| mem_reads | `oltp_read_only` | 93.47ms | 104.42ms | 1.1× | 0.8% | PASS |
| mem_writes | `oltp_bulk_insert` | 141.30ms | 215.25ms | 1.5× | 0.4% | PASS |
| mem_writes | `oltp_insert` | 12.68ms | 19.54ms | 1.5× | 1.1% | PASS |
| mem_writes | `oltp_update_index` | 44.02ms | 80.79ms | 1.8× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 29.64ms | 48.50ms | 1.6× | 0.9% | PASS |
| mem_writes | `oltp_delete_insert` | 39.09ms | 60.69ms | 1.6× | 0.7% | PASS |
| mem_writes | `oltp_write_only` | 18.67ms | 36.90ms | 2.0× | 1.3% | PASS |
| mem_writes | `types_delete_insert` | 20.89ms | 27.04ms | 1.3× | 1.2% | PASS |
| mem_writes | `oltp_read_write` | 56.88ms | 85.80ms | 1.5× | 0.7% | PASS |
| file_reads | `oltp_point_select` | 47.10ms | 29.40ms | 0.6× | 0.8% | PASS |
| file_reads | `oltp_range_select` | 12.56ms | 11.97ms | 1.0× | 0.6% | PASS |
| file_reads | `oltp_sum_range` | 11.80ms | 10.86ms | 0.9× | 0.5% | PASS |
| file_reads | `oltp_order_range` | 2.86ms | 2.85ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_distinct_range` | 3.76ms | 3.85ms | 1.0× | 1.0% | PASS |
| file_reads | `oltp_index_scan` | 6.42ms | 5.28ms | 0.8× | 0.8% | PASS |
| file_reads | `select_random_points` | 15.56ms | 11.71ms | 0.8× | 0.8% | PASS |
| file_reads | `select_random_ranges` | 6.84ms | 4.83ms | 0.7× | 1.1% | PASS |
| file_reads | `covering_index_scan` | 9.39ms | 9.90ms | 1.1× | 0.7% | PASS |
| file_reads | `groupby_scan` | 30.21ms | 31.87ms | 1.1× | 0.5% | PASS |
| file_reads | `index_join` | 6.91ms | 8.04ms | 1.2× | 1.0% | PASS |
| file_reads | `index_join_scan` | 3.18ms | 5.11ms | 1.6× | 1.1% | PASS |
| file_reads | `types_table_scan` | 971.96ms | 680.53ms | 0.7× | 0.5% | PASS |
| file_reads | `table_scan` | 1.12s | 802.33ms | 0.7× | 1.6% | PASS |
| file_reads | `oltp_read_only` | 132.62ms | 113.94ms | 0.9× | 0.6% | PASS |
| file_writes | `oltp_bulk_insert` | 196.36ms | 274.44ms | 1.4× | 1.8% | PASS |
| file_writes | `oltp_insert` | 28.64ms | 35.54ms | 1.2× | 2.9% | PASS |
| file_writes | `oltp_update_index` | 150.15ms | 140.82ms | 0.9× | 2.1% | PASS |
| file_writes | `oltp_update_non_index` | 121.83ms | 96.33ms | 0.8× | 1.0% | PASS |
| file_writes | `oltp_delete_insert` | 129.40ms | 117.56ms | 0.9× | 1.5% | PASS |
| file_writes | `oltp_write_only` | 95.59ms | 80.69ms | 0.8× | 1.4% | PASS |
| file_writes | `types_delete_insert` | 82.64ms | 62.11ms | 0.8× | 3.2% | PASS |
| file_writes | `oltp_read_write` | 135.48ms | 130.52ms | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_point_select` | 29.42ms | 29.32ms | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_range_select` | 10.77ms | 11.98ms | 1.1× | 0.9% | PASS |
| ac_reads | `oltp_sum_range` | 10.10ms | 10.92ms | 1.1× | 0.8% | PASS |
| ac_reads | `oltp_order_range` | 2.68ms | 2.86ms | 1.1× | 0.7% | PASS |
| ac_reads | `oltp_distinct_range` | 3.56ms | 3.82ms | 1.1× | 1.3% | PASS |
| ac_reads | `oltp_index_scan` | 4.68ms | 5.26ms | 1.1× | 1.2% | PASS |
| ac_reads | `select_random_points` | 13.58ms | 11.57ms | 0.9× | 1.0% | PASS |
| ac_reads | `select_random_ranges` | 4.99ms | 4.79ms | 1.0× | 0.6% | PASS |
| ac_reads | `covering_index_scan` | 7.46ms | 9.77ms | 1.3× | 0.8% | PASS |
| ac_reads | `groupby_scan` | 29.46ms | 31.52ms | 1.1× | 0.6% | PASS |
| ac_reads | `index_join` | 5.96ms | 8.00ms | 1.3× | 1.0% | PASS |
| ac_reads | `index_join_scan` | 3.05ms | 5.07ms | 1.7× | 1.2% | PASS |
| ac_reads | `types_table_scan` | 961.09ms | 674.79ms | 0.7× | 0.3% | PASS |
| ac_reads | `table_scan` | 1.08s | 789.90ms | 0.7× | 0.7% | PASS |
| ac_reads | `oltp_read_only` | 105.03ms | 112.42ms | 1.1× | 0.5% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 27.90ms | 37.93ms | 1.4× | 3.6% | PASS |
| ac_writes | `oltp_insert_ac` | 29.47ms | 54.92ms | 1.9× | 5.1% | PASS |
| ac_writes | `oltp_update_index_ac` | 30.49ms | 76.51ms | 2.5× | 4.1% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 27.40ms | 49.68ms | 1.8× | 3.4% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 29.23ms | 61.04ms | 2.1× | 4.1% | PASS |
| ac_writes | `oltp_write_only_ac` | 28.82ms | 59.98ms | 2.1× | 3.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 27.21ms | 50.03ms | 1.8× | 3.6% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.40ms | 65.58ms | 2.0× | 4.5% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 34.23ms | 38.05ms | 1.1× | 1.8% | PASS |
| mem_reads | `oltp_range_select` | 16.11ms | 15.73ms | 1.0× | 2.4% | PASS |
| mem_reads | `oltp_sum_range` | 14.64ms | 15.37ms | 1.0× | 1.6% | PASS |
| mem_reads | `oltp_order_range` | 3.17ms | 3.42ms | 1.1× | 1.0% | PASS |
| mem_reads | `oltp_distinct_range` | 4.35ms | 4.55ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_index_scan` | 3.93ms | 6.43ms | 1.6× | 1.6% | PASS |
| mem_reads | `select_random_points` | 21.14ms | 22.20ms | 1.1× | 2.5% | PASS |
| mem_reads | `select_random_ranges` | 6.53ms | 6.68ms | 1.0× | 1.6% | PASS |
| mem_reads | `covering_index_scan` | 7.75ms | 11.17ms | 1.4× | 1.2% | PASS |
| mem_reads | `groupby_scan` | 34.33ms | 35.70ms | 1.0× | 0.8% | PASS |
| mem_reads | `index_join` | 10.26ms | 10.05ms | 1.0× | 1.6% | PASS |
| mem_reads | `index_join_scan` | 3.69ms | 6.28ms | 1.7× | 1.7% | PASS |
| mem_reads | `types_table_scan` | 1.04s | 891.58ms | 0.9× | 0.5% | PASS |
| mem_reads | `table_scan` | 1.17s | 990.08ms | 0.8× | 0.6% | PASS |
| mem_reads | `oltp_read_only` | 135.35ms | 142.65ms | 1.1× | 1.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 245.74ms | 338.99ms | 1.4× | 1.0% | PASS |
| mem_writes | `oltp_insert` | 18.01ms | 33.55ms | 1.9× | 0.8% | PASS |
| mem_writes | `oltp_update_index` | 64.37ms | 130.61ms | 2.0× | 1.0% | PASS |
| mem_writes | `oltp_update_non_index` | 47.76ms | 73.45ms | 1.5× | 1.5% | PASS |
| mem_writes | `oltp_delete_insert` | 53.62ms | 94.38ms | 1.8× | 1.2% | PASS |
| mem_writes | `oltp_write_only` | 27.61ms | 49.66ms | 1.8× | 1.1% | PASS |
| mem_writes | `types_delete_insert` | 39.27ms | 57.52ms | 1.5× | 2.4% | PASS |
| mem_writes | `oltp_read_write` | 97.74ms | 133.69ms | 1.4× | 2.1% | PASS |
| file_reads | `oltp_point_select` | 101.42ms | 54.94ms | 0.5× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 23.91ms | 17.59ms | 0.7× | 2.3% | PASS |
| file_reads | `oltp_sum_range` | 22.12ms | 17.17ms | 0.8× | 1.8% | PASS |
| file_reads | `oltp_order_range` | 4.03ms | 3.70ms | 0.9× | 1.6% | PASS |
| file_reads | `oltp_distinct_range` | 5.18ms | 4.85ms | 0.9× | 1.7% | PASS |
| file_reads | `oltp_index_scan` | 10.77ms | 8.34ms | 0.8× | 1.3% | PASS |
| file_reads | `select_random_points` | 29.61ms | 24.44ms | 0.8× | 2.3% | PASS |
| file_reads | `select_random_ranges` | 13.67ms | 8.61ms | 0.6× | 1.7% | PASS |
| file_reads | `covering_index_scan` | 14.77ms | 13.04ms | 0.9× | 1.3% | PASS |
| file_reads | `groupby_scan` | 35.44ms | 36.02ms | 1.0× | 0.8% | PASS |
| file_reads | `index_join` | 14.40ms | 11.34ms | 0.8× | 1.6% | PASS |
| file_reads | `index_join_scan` | 4.71ms | 6.60ms | 1.4× | 1.8% | PASS |
| file_reads | `types_table_scan` | 1.03s | 890.29ms | 0.9× | 0.4% | PASS |
| file_reads | `table_scan` | 1.17s | 990.49ms | 0.8× | 0.6% | PASS |
| file_reads | `oltp_read_only` | 235.57ms | 169.08ms | 0.7× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 270.03ms | 354.28ms | 1.3× | 1.1% | PASS |
| file_writes | `oltp_insert` | 25.79ms | 39.12ms | 1.5× | 1.3% | PASS |
| file_writes | `oltp_update_index` | 135.35ms | 150.45ms | 1.1× | 5.5% | PASS |
| file_writes | `oltp_update_non_index` | 107.19ms | 87.95ms | 0.8× | 13.8% | PASS |
| file_writes | `oltp_delete_insert` | 94.42ms | 111.53ms | 1.2× | 1.3% | PASS |
| file_writes | `oltp_write_only` | 78.63ms | 62.98ms | 0.8× | 7.8% | PASS |
| file_writes | `types_delete_insert` | 71.03ms | 66.56ms | 0.9× | 1.5% | PASS |
| file_writes | `oltp_read_write` | 141.91ms | 144.38ms | 1.0× | 5.0% | PASS |
| ac_reads | `oltp_point_select` | 57.29ms | 54.73ms | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_range_select` | 19.43ms | 17.51ms | 0.9× | 1.5% | PASS |
| ac_reads | `oltp_sum_range` | 17.66ms | 17.24ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_order_range` | 3.65ms | 3.69ms | 1.0× | 1.3% | PASS |
| ac_reads | `oltp_distinct_range` | 4.80ms | 4.84ms | 1.0× | 1.4% | PASS |
| ac_reads | `oltp_index_scan` | 6.58ms | 8.44ms | 1.3× | 2.3% | PASS |
| ac_reads | `select_random_points` | 25.24ms | 24.59ms | 1.0× | 2.6% | PASS |
| ac_reads | `select_random_ranges` | 9.37ms | 8.65ms | 0.9× | 1.4% | PASS |
| ac_reads | `covering_index_scan` | 10.41ms | 13.14ms | 1.3× | 1.4% | PASS |
| ac_reads | `groupby_scan` | 35.22ms | 36.03ms | 1.0× | 0.6% | PASS |
| ac_reads | `index_join` | 12.61ms | 11.44ms | 0.9× | 1.7% | PASS |
| ac_reads | `index_join_scan` | 4.26ms | 6.63ms | 1.6× | 1.2% | PASS |
| ac_reads | `types_table_scan` | 1.04s | 897.03ms | 0.9× | 0.8% | PASS |
| ac_reads | `table_scan` | 1.21s | 998.87ms | 0.8× | 1.0% | PASS |
| ac_reads | `oltp_read_only` | 172.50ms | 169.60ms | 1.0× | 1.4% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.64ms | 32.77ms | 1.5× | 5.4% | PASS |
| ac_writes | `oltp_insert_ac` | 25.62ms | 44.04ms | 1.7× | 5.2% | PASS |
| ac_writes | `oltp_update_index_ac` | 25.37ms | 65.47ms | 2.6× | 5.5% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.48ms | 41.72ms | 1.9× | 5.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.21ms | 54.86ms | 2.2× | 4.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.13ms | 51.51ms | 2.0× | 6.0% | PASS |
| ac_writes | `types_delete_insert_ac` | 23.19ms | 42.25ms | 1.8× | 4.8% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.71ms | 61.51ms | 1.9× | 6.4% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.23ms | 23.50ms | 1.0× | 1.1% | PASS |
| mem_reads | `oltp_range_select` | 11.05ms | 10.81ms | 1.0× | 1.7% | PASS |
| mem_reads | `oltp_sum_range` | 10.36ms | 10.19ms | 1.0× | 6.7% | PASS |
| mem_reads | `oltp_order_range` | 2.32ms | 2.38ms | 1.0× | 1.8% | PASS |
| mem_reads | `oltp_distinct_range` | 2.95ms | 3.06ms | 1.0× | 1.7% | PASS |
| mem_reads | `oltp_index_scan` | 2.75ms | 4.11ms | 1.5× | 3.2% | PASS |
| mem_reads | `select_random_points` | 16.97ms | 15.96ms | 0.9× | 1.4% | PASS |
| mem_reads | `select_random_ranges` | 4.59ms | 4.30ms | 0.9× | 1.9% | PASS |
| mem_reads | `covering_index_scan` | 4.81ms | 6.81ms | 1.4× | 0.7% | PASS |
| mem_reads | `groupby_scan` | 23.09ms | 24.62ms | 1.1× | 0.4% | PASS |
| mem_reads | `index_join` | 8.67ms | 7.49ms | 0.9× | 2.4% | PASS |
| mem_reads | `index_join_scan` | 2.40ms | 4.65ms | 1.9× | 4.4% | PASS |
| mem_reads | `types_table_scan` | 764.56ms | 587.87ms | 0.8× | 0.3% | PASS |
| mem_reads | `table_scan` | 852.53ms | 646.00ms | 0.8× | 1.4% | PASS |
| mem_reads | `oltp_read_only` | 88.53ms | 91.34ms | 1.0× | 1.1% | PASS |
| mem_writes | `oltp_bulk_insert` | 143.26ms | 189.13ms | 1.3× | 0.4% | PASS |
| mem_writes | `oltp_insert` | 11.52ms | 20.57ms | 1.8× | 0.5% | PASS |
| mem_writes | `oltp_update_index` | 42.16ms | 80.44ms | 1.9× | 1.2% | PASS |
| mem_writes | `oltp_update_non_index` | 29.57ms | 44.69ms | 1.5× | 1.7% | PASS |
| mem_writes | `oltp_delete_insert` | 33.94ms | 57.70ms | 1.7× | 1.9% | PASS |
| mem_writes | `oltp_write_only` | 17.30ms | 30.49ms | 1.8× | 1.6% | PASS |
| mem_writes | `types_delete_insert` | 24.80ms | 33.73ms | 1.4× | 1.8% | PASS |
| mem_writes | `oltp_read_write` | 59.38ms | 78.61ms | 1.3× | 1.5% | PASS |
| file_reads | `oltp_point_select` | 43.79ms | 27.55ms | 0.6× | 1.1% | PASS |
| file_reads | `oltp_range_select` | 12.49ms | 11.19ms | 0.9× | 3.3% | PASS |
| file_reads | `oltp_sum_range` | 12.33ms | 10.72ms | 0.9× | 3.5% | PASS |
| file_reads | `oltp_order_range` | 2.52ms | 2.41ms | 1.0× | 2.3% | PASS |
| file_reads | `oltp_distinct_range` | 3.15ms | 3.11ms | 1.0× | 1.5% | PASS |
| file_reads | `oltp_index_scan` | 4.83ms | 4.69ms | 1.0× | 1.4% | PASS |
| file_reads | `select_random_points` | 18.52ms | 16.15ms | 0.9× | 2.5% | PASS |
| file_reads | `select_random_ranges` | 6.59ms | 4.63ms | 0.7× | 1.7% | PASS |
| file_reads | `covering_index_scan` | 6.89ms | 7.19ms | 1.0× | 0.4% | PASS |
| file_reads | `groupby_scan` | 23.25ms | 24.62ms | 1.1× | 0.5% | PASS |
| file_reads | `index_join` | 9.70ms | 7.66ms | 0.8× | 1.9% | PASS |
| file_reads | `index_join_scan` | 2.66ms | 4.89ms | 1.8× | 3.0% | PASS |
| file_reads | `types_table_scan` | 764.89ms | 588.18ms | 0.8× | 0.3% | PASS |
| file_reads | `table_scan` | 869.50ms | 648.00ms | 0.7× | 0.5% | PASS |
| file_reads | `oltp_read_only` | 117.49ms | 97.78ms | 0.8× | 1.0% | PASS |
| file_writes | `oltp_bulk_insert` | 155.23ms | 199.52ms | 1.3× | 0.9% | PASS |
| file_writes | `oltp_insert` | 14.84ms | 23.94ms | 1.6× | 1.7% | PASS |
| file_writes | `oltp_update_index` | 57.56ms | 91.95ms | 1.6× | 3.1% | PASS |
| file_writes | `oltp_update_non_index` | 42.76ms | 52.46ms | 1.2× | 2.9% | PASS |
| file_writes | `oltp_delete_insert` | 48.76ms | 67.25ms | 1.4× | 2.8% | PASS |
| file_writes | `oltp_write_only` | 30.18ms | 37.64ms | 1.2× | 2.6% | PASS |
| file_writes | `types_delete_insert` | 36.89ms | 37.50ms | 1.0× | 2.5% | PASS |
| file_writes | `oltp_read_write` | 75.98ms | 88.17ms | 1.2× | 1.3% | PASS |
| ac_reads | `oltp_point_select` | 30.90ms | 28.03ms | 0.9× | 1.1% | PASS |
| ac_reads | `oltp_range_select` | 11.87ms | 11.45ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_sum_range` | 11.76ms | 11.10ms | 0.9× | 1.5% | PASS |
| ac_reads | `oltp_order_range` | 2.50ms | 2.49ms | 1.0× | 0.8% | PASS |
| ac_reads | `oltp_distinct_range` | 3.22ms | 3.22ms | 1.0× | 0.9% | PASS |
| ac_reads | `oltp_index_scan` | 3.75ms | 5.19ms | 1.4× | 1.7% | PASS |
| ac_reads | `select_random_points` | 19.09ms | 17.07ms | 0.9× | 1.0% | PASS |
| ac_reads | `select_random_ranges` | 5.64ms | 4.92ms | 0.9× | 1.7% | PASS |
| ac_reads | `covering_index_scan` | 5.56ms | 7.53ms | 1.4× | 0.6% | PASS |
| ac_reads | `groupby_scan` | 23.24ms | 24.81ms | 1.1× | 0.4% | PASS |
| ac_reads | `index_join` | 9.35ms | 7.79ms | 0.8× | 2.7% | PASS |
| ac_reads | `index_join_scan` | 2.74ms | 5.12ms | 1.9× | 3.7% | PASS |
| ac_reads | `types_table_scan` | 764.63ms | 589.70ms | 0.8× | 0.4% | PASS |
| ac_reads | `table_scan` | 869.81ms | 647.46ms | 0.7× | 0.5% | PASS |
| ac_reads | `oltp_read_only` | 100.05ms | 98.89ms | 1.0× | 0.7% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 8.84ms | 12.27ms | 1.4× | 0.7% | PASS |
| ac_writes | `oltp_insert_ac` | 9.86ms | 21.27ms | 2.2× | 1.7% | PASS |
| ac_writes | `oltp_update_index_ac` | 10.23ms | 29.34ms | 2.9× | 1.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 8.62ms | 15.78ms | 1.8× | 1.3% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 9.78ms | 22.59ms | 2.3× | 1.3% | PASS |
| ac_writes | `oltp_write_only_ac` | 9.62ms | 21.87ms | 2.3× | 1.1% | PASS |
| ac_writes | `types_delete_insert_ac` | 9.00ms | 16.32ms | 1.8× | 1.2% | PASS |
| ac_writes | `oltp_read_write_ac` | 13.00ms | 26.43ms | 2.0× | 1.9% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 27.83ms | 30.88ms | 1.1× | 0.7% | PASS |
| mem_reads | `oltp_range_select` | 17.25ms | 19.83ms | 1.1× | 0.9% | PASS |
| mem_reads | `oltp_sum_range` | 15.90ms | 18.19ms | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_order_range` | 3.32ms | 3.62ms | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_distinct_range` | 4.24ms | 4.61ms | 1.1× | 0.7% | PASS |
| mem_reads | `oltp_index_scan` | 3.99ms | 5.10ms | 1.3× | 1.4% | PASS |
| mem_reads | `select_random_points` | 25.33ms | 26.13ms | 1.0× | 0.7% | PASS |
| mem_reads | `select_random_ranges` | 6.23ms | 7.12ms | 1.1× | 0.8% | PASS |
| mem_reads | `covering_index_scan` | 6.34ms | 9.04ms | 1.4× | 1.1% | PASS |
| mem_reads | `groupby_scan` | 34.62ms | 37.47ms | 1.1× | 0.4% | PASS |
| mem_reads | `index_join` | 6.95ms | 9.55ms | 1.4× | 1.1% | PASS |
| mem_reads | `index_join_scan` | 3.38ms | 5.47ms | 1.6× | 0.9% | PASS |
| mem_reads | `types_table_scan` | 992.68ms | 740.42ms | 0.7× | 1.0% | PASS |
| mem_reads | `table_scan` | 1.12s | 851.54ms | 0.8× | 0.8% | PASS |
| mem_reads | `oltp_read_only` | 130.96ms | 147.97ms | 1.1× | 1.0% | PASS |
| mem_writes | `oltp_bulk_insert` | 201.44ms | 260.48ms | 1.3× | 0.4% | PASS |
| mem_writes | `oltp_insert` | 16.13ms | 26.03ms | 1.6× | 1.0% | PASS |
| mem_writes | `oltp_update_index` | 55.91ms | 100.52ms | 1.8× | 0.9% | PASS |
| mem_writes | `oltp_update_non_index` | 41.13ms | 65.41ms | 1.6× | 0.9% | PASS |
| mem_writes | `oltp_delete_insert` | 41.57ms | 73.17ms | 1.8× | 0.7% | PASS |
| mem_writes | `oltp_write_only` | 22.31ms | 44.24ms | 2.0× | 1.2% | PASS |
| mem_writes | `types_delete_insert` | 26.33ms | 42.57ms | 1.6× | 0.9% | PASS |
| mem_writes | `oltp_read_write` | 81.38ms | 121.02ms | 1.5× | 0.8% | PASS |
| file_reads | `oltp_point_select` | 53.90ms | 37.19ms | 0.7× | 1.0% | PASS |
| file_reads | `oltp_range_select` | 20.00ms | 20.62ms | 1.0× | 1.2% | PASS |
| file_reads | `oltp_sum_range` | 19.34ms | 19.18ms | 1.0× | 0.8% | PASS |
| file_reads | `oltp_order_range` | 3.75ms | 3.83ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_distinct_range` | 4.65ms | 4.79ms | 1.0× | 1.0% | PASS |
| file_reads | `oltp_index_scan` | 6.88ms | 6.16ms | 0.9× | 1.2% | PASS |
| file_reads | `select_random_points` | 28.86ms | 27.26ms | 0.9× | 0.8% | PASS |
| file_reads | `select_random_ranges` | 9.30ms | 7.98ms | 0.9× | 0.8% | PASS |
| file_reads | `covering_index_scan` | 9.31ms | 10.06ms | 1.1× | 0.7% | PASS |
| file_reads | `groupby_scan` | 35.13ms | 37.87ms | 1.1× | 0.5% | PASS |
| file_reads | `index_join` | 8.83ms | 10.53ms | 1.2× | 1.0% | PASS |
| file_reads | `index_join_scan` | 3.83ms | 5.77ms | 1.5× | 1.3% | PASS |
| file_reads | `types_table_scan` | 974.47ms | 739.20ms | 0.8× | 0.6% | PASS |
| file_reads | `table_scan` | 1.26s | 869.00ms | 0.7× | 2.8% | PASS |
| file_reads | `oltp_read_only` | 170.01ms | 156.87ms | 0.9× | 0.7% | PASS |
| file_writes | `oltp_bulk_insert` | 249.07ms | 318.29ms | 1.3× | 0.6% | PASS |
| file_writes | `oltp_insert` | 30.13ms | 37.31ms | 1.2× | 0.7% | PASS |
| file_writes | `oltp_update_index` | 167.17ms | 171.63ms | 1.0× | 2.1% | PASS |
| file_writes | `oltp_update_non_index` | 135.66ms | 116.89ms | 0.9× | 1.1% | PASS |
| file_writes | `oltp_delete_insert` | 135.91ms | 133.05ms | 1.0× | 3.1% | PASS |
| file_writes | `oltp_write_only` | 100.09ms | 88.76ms | 0.9× | 0.4% | PASS |
| file_writes | `types_delete_insert` | 90.06ms | 76.61ms | 0.9× | 4.4% | PASS |
| file_writes | `oltp_read_write` | 160.01ms | 164.23ms | 1.0× | 2.2% | PASS |
| ac_reads | `oltp_point_select` | 36.47ms | 37.40ms | 1.0× | 0.7% | PASS |
| ac_reads | `oltp_range_select` | 18.34ms | 20.68ms | 1.1× | 0.7% | PASS |
| ac_reads | `oltp_sum_range` | 17.30ms | 19.14ms | 1.1× | 0.6% | PASS |
| ac_reads | `oltp_order_range` | 3.56ms | 3.81ms | 1.1× | 1.0% | PASS |
| ac_reads | `oltp_distinct_range` | 4.49ms | 4.82ms | 1.1× | 0.6% | PASS |
| ac_reads | `oltp_index_scan` | 5.25ms | 6.20ms | 1.2× | 0.9% | PASS |
| ac_reads | `select_random_points` | 27.21ms | 27.37ms | 1.0× | 0.8% | PASS |
| ac_reads | `select_random_ranges` | 7.53ms | 8.03ms | 1.1× | 1.0% | PASS |
| ac_reads | `covering_index_scan` | 7.52ms | 10.08ms | 1.3× | 0.9% | PASS |
| ac_reads | `groupby_scan` | 35.23ms | 38.05ms | 1.1× | 0.5% | PASS |
| ac_reads | `index_join` | 7.97ms | 10.59ms | 1.3× | 1.1% | PASS |
| ac_reads | `index_join_scan` | 3.71ms | 5.85ms | 1.6× | 0.9% | PASS |
| ac_reads | `types_table_scan` | 1.01s | 749.11ms | 0.7× | 1.1% | PASS |
| ac_reads | `table_scan` | 1.22s | 867.30ms | 0.7× | 3.1% | PASS |
| ac_reads | `oltp_read_only` | 145.27ms | 157.76ms | 1.1× | 0.8% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 28.83ms | 38.82ms | 1.3× | 4.7% | PASS |
| ac_writes | `oltp_insert_ac` | 30.45ms | 50.58ms | 1.7× | 2.8% | PASS |
| ac_writes | `oltp_update_index_ac` | 31.35ms | 63.28ms | 2.0× | 2.9% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 28.25ms | 44.02ms | 1.6× | 3.9% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 29.71ms | 52.48ms | 1.8× | 2.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 29.66ms | 51.62ms | 1.7× | 3.0% | PASS |
| ac_writes | `types_delete_insert_ac` | 27.64ms | 40.73ms | 1.5× | 2.9% | PASS |
| ac_writes | `oltp_read_write_ac` | 34.86ms | 59.69ms | 1.7× | 3.1% | PASS |

</details>

</details>

## Version-control latency

Wall time: 4m 51s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 22.72ms | 38.00ms | 59.8% | 1.0% | PASS |
| `status_dirty_many_tables` | 25.29ms | 42.00ms | 60.2% | 1.0% | PASS |
| `diff_regular_working_one_table` | 21.63ms | 33.00ms | 65.5% | 0.7% | PASS |
| `diff_regular_working_many_tables` | 28.63ms | 50.00ms | 57.3% | 0.8% | PASS |
| `diff_stat_working_many_tables` | 28.51ms | 48.00ms | 59.4% | 1.1% | PASS |
| `diff_schema_working_many_tables` | 28.48ms | 48.00ms | 59.3% | 0.7% | PASS |
| `branch_list_many_branches` | 15.61ms | 25.00ms | 62.4% | 0.5% | PASS |
| `branch_create_delete` | 16.88ms | 27.00ms | 62.5% | 0.9% | PASS |
| `at_literal_deep_history` | 16.48ms | 28.00ms | 58.9% | 0.7% | PASS |
| `diff_literal_deep_history` | 16.70ms | 28.00ms | 59.6% | 0.7% | PASS |
| `history_literal_deep_history` | 17.33ms | 30.00ms | 57.8% | 0.7% | PASS |
| `checkout_branch_clean` | 22.49ms | 43.00ms | 52.3% | 1.3% | PASS |
| `merge_data_no_conflicts` | 18.39ms | 33.00ms | 55.7% | 0.7% | PASS |
| `merge_data_secondary_index` | 625.21ms | 944.00ms | 66.2% | 0.6% | PASS |
| `merge_schema_no_conflicts` | 14.87ms | 24.00ms | 61.9% | 0.7% | PASS |
| `merge_data_conflicts` | 20.85ms | 33.00ms | 63.2% | 0.5% | PASS |
| `merge_data_conflicts_with_resolve` | 21.46ms | 34.00ms | 63.1% | 0.7% | PASS |

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

candidate: 76f30e7.

Fixture file bytes: base 107,968,301; delta 115,879,032.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 46.211 | 45.910–63.232 | 44.301 | 2163.99 | 107968301 |
| push_initial | candidate | 348.814 | 345.153–353.800 | 346.558 | 286.69 | 107956807 |
| clone_initial | candidate | 468.177 | 467.298–471.216 | 459.695 | 213.59 | 108475105 |
| push_noop | candidate | 0.416 | 0.380–0.542 | 0.205 | — | 0 |
| fetch_noop | candidate | 0.120 | 0.100–0.144 | 0.122 | — | 0 |
| push_incremental | candidate | 136.849 | 136.750–139.117 | 135.842 | 7.31 | 7909775 |
| fetch_incremental | candidate | 61.128 | 60.237–61.373 | 59.532 | 16.36 | 7910084 |
| pull_fast_forward | candidate | 54.595 | 53.296–55.132 | 52.582 | 18.32 | 7910844 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 90.52 | 175.487 | 108.763 | 18329 | 2066 |
| clone_initial | candidate | 90.72 | 295.297 | 110.833 | 47295 | 2353 |
| push_noop | candidate | 41.55 | 0.073 | 0.000 | 27 | 2 |
| fetch_noop | candidate | 41.55 | 0.135 | 0.000 | 42 | 0 |
| push_incremental | candidate | 41.55 | 96.798 | 7.910 | 22397 | 125 |
| fetch_incremental | candidate | 41.55 | 34.472 | 7.910 | 10810 | 124 |
| pull_fast_forward | candidate | 41.55 | 26.497 | 8.959 | 7032 | 142 |

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

candidate: 76f30e7.

Fixture file bytes: base 1,087,718,066; delta 1,103,525,194.

| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |
|---|---|---:|---:|---:|---:|---:|
| copy_fsync | candidate | 4149.045 | 4022.232–4153.435 | 541.985 | 241.02 | 1087718066 |
| push_initial | candidate | 6134.612 | 6100.446–6389.951 | 3456.174 | 163.01 | 1079566310 |
| clone_initial | candidate | 5105.466 | 5096.841–5152.624 | 5078.655 | 195.87 | 1102407396 |
| push_noop | candidate | 0.427 | 0.413–0.491 | 0.227 | — | 0 |
| fetch_noop | candidate | 0.097 | 0.096–0.102 | 0.098 | — | 0 |
| push_incremental | candidate | 774.996 | 772.850–780.241 | 772.384 | 1.29 | 15806003 |
| fetch_incremental | candidate | 197.833 | 196.279–199.497 | 194.706 | 5.05 | 15806513 |
| pull_fast_forward | candidate | 198.972 | 198.875–202.067 | 196.733 | 5.03 | 15807273 |

<details>
<summary>Memory and file I/O</summary>

Successful VFS bytes and VFS requests cover local and remote files during the operation; they include OS-cache hits and are not physical disk traffic. Peak RSS covers the fresh SQL process, including database open. The Python copy control is not instrumented.

| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |
|---|---|---:|---:|---:|---:|---:|
| push_initial | candidate | 134.79 | 2091.978 | 1087.622 | 263522 | 20541 |
| clone_initial | candidate | 124.02 | 3524.242 | 1133.868 | 613904 | 32259 |
| push_noop | candidate | 44.04 | 0.078 | 0.000 | 28 | 2 |
| fetch_noop | candidate | 44.04 | 0.137 | 0.000 | 42 | 0 |
| push_incremental | candidate | 44.80 | 619.347 | 15.928 | 150447 | 313 |
| fetch_incremental | candidate | 50.38 | 62.109 | 15.928 | 13867 | 313 |
| pull_fast_forward | candidate | 50.46 | 62.042 | 16.978 | 13862 | 331 |

</details>


</details>

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh`, `test/vc_perf_ceiling.sh`, and `test/remote_perf.py`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
