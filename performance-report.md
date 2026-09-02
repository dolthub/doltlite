# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-09-02 11:10 UTC
>
> Commit: [`7e9ffb5db5f4e5ef5a8849ab9beea95068bb60c7`](https://github.com/dolthub/doltlite/commit/7e9ffb5db5f4e5ef5a8849ab9beea95068bb60c7)
>
> Runner: ubuntu24 20260823.283.1
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/33614952404)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians. Paired-ratio noise is the median absolute deviation of the paired DoltLite/SQLite ratios, expressed as a percentage.

## SQL workload summary

The primary view aggregates all key shapes and compares DoltLite with SQLite by storage mode and operation class.

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.05s | 9.36s | 1.0× | 1.6% | **PASS** |
| Writes | 1.78s | 2.74s | 1.5× | 1.5% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 9.46s | 9.38s | 1.0× | 1.1% | **PASS** |
| Writes | 4.71s | 4.27s | 0.9× | 6.7% | **PASS** |
| Autocommit writes | 3.95s | 12.21s | 3.1× | 24.9% | **PASS** |

The absolute ceiling is 2.3× per ordinary workload and 1.9× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>Key-shape and individual-workload breakdown</summary>

The integer, text, blob, and composite primary-key runs verify that performance holds across key shapes.

| Storage | Operation | Key shape | Workloads | Samples/workload | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| In-memory | Reads | int | 15 | 55 | 1.99s | 2.11s | 1.1× | 1.4% | **PASS** |
| In-memory | Reads | textpk | 15 | 55 | 2.22s | 2.26s | 1.0× | 1.6% | **PASS** |
| In-memory | Reads | blobpk | 15 | 55 | 3.02s | 3.03s | 1.0× | 1.6% | **PASS** |
| In-memory | Reads | compositepk | 15 | 55 | 1.82s | 1.96s | 1.1× | 2.6% | **PASS** |
| In-memory | Writes | int | 8 | 55 | 307.29ms | 459.82ms | 1.5× | 1.2% | **PASS** |
| In-memory | Writes | textpk | 8 | 55 | 478.71ms | 729.55ms | 1.5× | 1.6% | **PASS** |
| In-memory | Writes | blobpk | 8 | 55 | 640.82ms | 1.00s | 1.6× | 1.5% | **PASS** |
| In-memory | Writes | compositepk | 8 | 55 | 353.33ms | 543.34ms | 1.5× | 2.2% | **PASS** |
| File-backed | Reads | int | 15 | 55 | 2.07s | 2.13s | 1.0× | 1.2% | **PASS** |
| File-backed | Reads | textpk | 15 | 55 | 2.38s | 2.29s | 1.0× | 0.7% | **PASS** |
| File-backed | Reads | blobpk | 15 | 55 | 3.11s | 2.98s | 1.0× | 1.0% | **PASS** |
| File-backed | Reads | compositepk | 15 | 55 | 1.90s | 1.98s | 1.0× | 2.3% | **PASS** |
| File-backed | Writes | int | 8 | 55 | 802.47ms | 792.12ms | 1.0× | 2.4% | **PASS** |
| File-backed | Writes | textpk | 8 | 55 | 1.32s | 1.21s | 0.9× | 12.1% | **PASS** |
| File-backed | Writes | blobpk | 8 | 55 | 814.88ms | 1.04s | 1.3× | 1.9% | **PASS** |
| File-backed | Writes | compositepk | 8 | 55 | 1.77s | 1.23s | 0.7× | 34.9% | **PASS** |
| File-backed | Autocommit reads | int | 15 | 55 | 2.02s | 2.13s | 1.1× | 1.7% | **PASS** |
| File-backed | Autocommit reads | textpk | 15 | 55 | 2.26s | 2.30s | 1.0× | 0.7% | **PASS** |
| File-backed | Autocommit reads | blobpk | 15 | 55 | 3.11s | 3.07s | 1.0× | 1.4% | **PASS** |
| File-backed | Autocommit reads | compositepk | 15 | 55 | 1.86s | 1.98s | 1.1× | 1.6% | **PASS** |
| File-backed | Autocommit writes | int | 8 | 55 | 260.26ms | 759.56ms | 2.9× | 9.9% | **PASS** |
| File-backed | Autocommit writes | textpk | 8 | 55 | 447.21ms | 1.21s | 2.7× | 40.4% | **PASS** |
| File-backed | Autocommit writes | blobpk | 8 | 55 | 160.24ms | 608.47ms | 3.8× | 5.2% | **PASS** |
| File-backed | Autocommit writes | compositepk | 8 | 55 | 3.08s | 9.63s | 3.1× | 59.4% | **PASS** |

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 18.05ms | 19.76ms | 1.1× | 1.4% | PASS |
| mem_reads | `oltp_range_select` | 7.94ms | 8.12ms | 1.0× | 4.5% | PASS |
| mem_reads | `oltp_sum_range` | 7.38ms | 7.94ms | 1.1× | 2.9% | PASS |
| mem_reads | `oltp_order_range` | 2.13ms | 2.18ms | 1.0× | 1.7% | PASS |
| mem_reads | `oltp_distinct_range` | 2.92ms | 3.10ms | 1.1× | 0.8% | PASS |
| mem_reads | `oltp_index_scan` | 3.01ms | 3.49ms | 1.2× | 1.6% | PASS |
| mem_reads | `select_random_points` | 8.05ms | 8.69ms | 1.1× | 4.2% | PASS |
| mem_reads | `select_random_ranges` | 2.30ms | 2.74ms | 1.2× | 1.4% | PASS |
| mem_reads | `covering_index_scan` | 2.93ms | 2.86ms | 1.0× | 0.6% | PASS |
| mem_reads | `groupby_scan` | 24.68ms | 26.64ms | 1.1× | 1.3% | PASS |
| mem_reads | `index_join` | 4.44ms | 6.01ms | 1.4× | 1.1% | PASS |
| mem_reads | `index_join_scan` | 2.31ms | 3.39ms | 1.5× | 3.5% | PASS |
| mem_reads | `types_table_scan` | 853.61ms | 915.69ms | 1.1× | 0.2% | PASS |
| mem_reads | `table_scan` | 971.13ms | 1.01s | 1.0× | 0.3% | PASS |
| mem_reads | `oltp_read_only` | 79.51ms | 86.51ms | 1.1× | 0.5% | PASS |
| mem_writes | `oltp_bulk_insert` | 122.06ms | 163.27ms | 1.3× | 0.6% | PASS |
| mem_writes | `oltp_insert` | 10.95ms | 18.12ms | 1.7× | 0.7% | PASS |
| mem_writes | `oltp_update_index` | 36.51ms | 63.40ms | 1.7× | 1.1% | PASS |
| mem_writes | `oltp_update_non_index` | 24.47ms | 36.94ms | 1.5× | 2.2% | PASS |
| mem_writes | `oltp_delete_insert` | 33.05ms | 48.82ms | 1.5× | 1.5% | PASS |
| mem_writes | `oltp_write_only` | 15.61ms | 30.06ms | 1.9× | 1.6% | PASS |
| mem_writes | `types_delete_insert` | 17.56ms | 24.50ms | 1.4× | 0.9% | PASS |
| mem_writes | `oltp_read_write` | 47.08ms | 74.70ms | 1.6× | 1.2% | PASS |
| file_reads | `oltp_point_select` | 42.16ms | 25.78ms | 0.6× | 0.6% | PASS |
| file_reads | `oltp_range_select` | 11.10ms | 9.15ms | 0.8× | 1.2% | PASS |
| file_reads | `oltp_sum_range` | 10.43ms | 8.98ms | 0.9× | 1.6% | PASS |
| file_reads | `oltp_order_range` | 2.50ms | 2.36ms | 0.9× | 1.4% | PASS |
| file_reads | `oltp_distinct_range` | 3.32ms | 3.22ms | 1.0× | 1.3% | PASS |
| file_reads | `oltp_index_scan` | 5.62ms | 4.36ms | 0.8× | 1.4% | PASS |
| file_reads | `select_random_points` | 11.41ms | 10.10ms | 0.9× | 1.4% | PASS |
| file_reads | `select_random_ranges` | 4.76ms | 3.39ms | 0.7× | 1.5% | PASS |
| file_reads | `covering_index_scan` | 5.54ms | 3.56ms | 0.6× | 1.2% | PASS |
| file_reads | `groupby_scan` | 24.93ms | 26.23ms | 1.1× | 0.8% | PASS |
| file_reads | `index_join` | 5.93ms | 6.56ms | 1.1× | 0.9% | PASS |
| file_reads | `index_join_scan` | 2.72ms | 3.63ms | 1.3× | 3.2% | PASS |
| file_reads | `types_table_scan` | 854.22ms | 913.09ms | 1.1× | 0.2% | PASS |
| file_reads | `table_scan` | 970.87ms | 1.01s | 1.0× | 0.2% | PASS |
| file_reads | `oltp_read_only` | 113.53ms | 94.37ms | 0.8× | 0.7% | PASS |
| file_writes | `oltp_bulk_insert` | 169.24ms | 218.14ms | 1.3× | 4.1% | PASS |
| file_writes | `oltp_insert` | 22.92ms | 32.29ms | 1.4× | 2.5% | PASS |
| file_writes | `oltp_update_index` | 128.77ms | 118.64ms | 0.9× | 2.6% | PASS |
| file_writes | `oltp_update_non_index` | 103.36ms | 84.13ms | 0.8× | 1.8% | PASS |
| file_writes | `oltp_delete_insert` | 110.65ms | 97.48ms | 0.9× | 1.6% | PASS |
| file_writes | `oltp_write_only` | 82.42ms | 70.57ms | 0.9× | 1.6% | PASS |
| file_writes | `types_delete_insert` | 71.44ms | 54.49ms | 0.8× | 3.1% | PASS |
| file_writes | `oltp_read_write` | 113.67ms | 116.39ms | 1.0× | 2.4% | PASS |
| ac_reads | `oltp_point_select` | 25.89ms | 25.69ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_range_select` | 9.42ms | 9.13ms | 1.0× | 2.1% | PASS |
| ac_reads | `oltp_sum_range` | 8.75ms | 8.84ms | 1.0× | 2.5% | PASS |
| ac_reads | `oltp_order_range` | 2.33ms | 2.35ms | 1.0× | 1.7% | PASS |
| ac_reads | `oltp_distinct_range` | 3.14ms | 3.23ms | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_index_scan` | 3.96ms | 4.25ms | 1.1× | 1.9% | PASS |
| ac_reads | `select_random_points` | 9.66ms | 9.79ms | 1.0× | 2.0% | PASS |
| ac_reads | `select_random_ranges` | 3.21ms | 3.46ms | 1.1× | 2.3% | PASS |
| ac_reads | `covering_index_scan` | 3.92ms | 3.64ms | 0.9× | 2.1% | PASS |
| ac_reads | `groupby_scan` | 25.14ms | 26.40ms | 1.1× | 0.9% | PASS |
| ac_reads | `index_join` | 5.13ms | 6.60ms | 1.3× | 1.0% | PASS |
| ac_reads | `index_join_scan` | 2.69ms | 3.72ms | 1.4× | 1.9% | PASS |
| ac_reads | `types_table_scan` | 854.72ms | 913.16ms | 1.1× | 0.2% | PASS |
| ac_reads | `table_scan` | 971.71ms | 1.02s | 1.0× | 0.3% | PASS |
| ac_reads | `oltp_read_only` | 90.50ms | 94.51ms | 1.0× | 0.6% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 31.76ms | 84.58ms | 2.7× | 7.9% | PASS |
| ac_writes | `oltp_insert_ac` | 31.61ms | 93.83ms | 3.0× | 7.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 32.70ms | 102.01ms | 3.1× | 8.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 32.12ms | 89.50ms | 2.8× | 11.6% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 33.43ms | 102.86ms | 3.1× | 13.7% | PASS |
| ac_writes | `oltp_write_only_ac` | 33.44ms | 100.49ms | 3.0× | 14.9% | PASS |
| ac_writes | `types_delete_insert_ac` | 30.66ms | 88.81ms | 2.9× | 9.0% | PASS |
| ac_writes | `oltp_read_write_ac` | 34.55ms | 97.46ms | 2.8× | 10.7% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 28.60ms | 27.72ms | 1.0× | 1.6% | PASS |
| mem_reads | `oltp_range_select` | 14.63ms | 10.87ms | 0.7× | 3.3% | PASS |
| mem_reads | `oltp_sum_range` | 12.93ms | 10.64ms | 0.8× | 2.3% | PASS |
| mem_reads | `oltp_order_range` | 2.84ms | 2.53ms | 0.9× | 1.2% | PASS |
| mem_reads | `oltp_distinct_range` | 3.67ms | 3.40ms | 0.9× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 3.21ms | 4.54ms | 1.4× | 1.6% | PASS |
| mem_reads | `select_random_points` | 18.04ms | 16.03ms | 0.9× | 2.6% | PASS |
| mem_reads | `select_random_ranges` | 2.92ms | 4.19ms | 1.4× | 1.3% | PASS |
| mem_reads | `covering_index_scan` | 3.33ms | 3.57ms | 1.1× | 3.1% | PASS |
| mem_reads | `groupby_scan` | 29.34ms | 28.50ms | 1.0× | 0.8% | PASS |
| mem_reads | `index_join` | 9.01ms | 6.93ms | 0.8× | 2.8% | PASS |
| mem_reads | `index_join_scan` | 3.18ms | 4.61ms | 1.5× | 2.3% | PASS |
| mem_reads | `types_table_scan` | 923.08ms | 966.66ms | 1.0× | 1.2% | PASS |
| mem_reads | `table_scan` | 1.05s | 1.07s | 1.0× | 1.3% | PASS |
| mem_reads | `oltp_read_only` | 113.32ms | 102.44ms | 0.9× | 1.9% | PASS |
| mem_writes | `oltp_bulk_insert` | 192.19ms | 267.98ms | 1.4× | 1.1% | PASS |
| mem_writes | `oltp_insert` | 14.38ms | 28.12ms | 2.0× | 1.2% | PASS |
| mem_writes | `oltp_update_index` | 53.84ms | 104.38ms | 1.9× | 1.8% | PASS |
| mem_writes | `oltp_update_non_index` | 40.65ms | 58.63ms | 1.4× | 2.4% | PASS |
| mem_writes | `oltp_delete_insert` | 44.87ms | 79.34ms | 1.8× | 1.6% | PASS |
| mem_writes | `oltp_write_only` | 24.28ms | 46.67ms | 1.9× | 2.1% | PASS |
| mem_writes | `types_delete_insert` | 32.25ms | 38.76ms | 1.2× | 1.6% | PASS |
| mem_writes | `oltp_read_write` | 76.25ms | 105.66ms | 1.4× | 1.4% | PASS |
| file_reads | `oltp_point_select` | 93.92ms | 44.59ms | 0.5× | 0.7% | PASS |
| file_reads | `oltp_range_select` | 21.21ms | 12.52ms | 0.6× | 0.8% | PASS |
| file_reads | `oltp_sum_range` | 19.69ms | 12.46ms | 0.6× | 0.8% | PASS |
| file_reads | `oltp_order_range` | 3.65ms | 2.79ms | 0.8× | 0.9% | PASS |
| file_reads | `oltp_distinct_range` | 4.46ms | 3.66ms | 0.8× | 1.1% | PASS |
| file_reads | `oltp_index_scan` | 10.06ms | 6.66ms | 0.7× | 0.7% | PASS |
| file_reads | `select_random_points` | 25.06ms | 17.80ms | 0.7× | 0.6% | PASS |
| file_reads | `select_random_ranges` | 9.59ms | 5.97ms | 0.6× | 0.7% | PASS |
| file_reads | `covering_index_scan` | 10.25ms | 5.63ms | 0.5× | 0.9% | PASS |
| file_reads | `groupby_scan` | 30.18ms | 28.77ms | 1.0× | 0.6% | PASS |
| file_reads | `index_join` | 13.01ms | 8.70ms | 0.7× | 0.9% | PASS |
| file_reads | `index_join_scan` | 3.97ms | 4.96ms | 1.2× | 0.9% | PASS |
| file_reads | `types_table_scan` | 903.29ms | 954.18ms | 1.1× | 0.3% | PASS |
| file_reads | `table_scan` | 1.02s | 1.06s | 1.0× | 0.4% | PASS |
| file_reads | `oltp_read_only` | 204.05ms | 125.87ms | 0.6× | 0.6% | PASS |
| file_writes | `oltp_bulk_insert` | 279.61ms | 343.70ms | 1.2× | 6.0% | PASS |
| file_writes | `oltp_insert` | 27.95ms | 58.07ms | 2.1× | 14.5% | PASS |
| file_writes | `oltp_update_index` | 174.13ms | 182.05ms | 1.0× | 11.1% | PASS |
| file_writes | `oltp_update_non_index` | 190.40ms | 127.65ms | 0.7× | 20.5% | PASS |
| file_writes | `oltp_delete_insert` | 199.38ms | 147.99ms | 0.7× | 9.1% | PASS |
| file_writes | `oltp_write_only` | 125.71ms | 99.69ms | 0.8× | 7.4% | PASS |
| file_writes | `types_delete_insert` | 141.43ms | 86.08ms | 0.6× | 13.2% | PASS |
| file_writes | `oltp_read_write` | 182.30ms | 167.16ms | 0.9× | 14.2% | PASS |
| ac_reads | `oltp_point_select` | 50.21ms | 44.46ms | 0.9× | 0.6% | PASS |
| ac_reads | `oltp_range_select` | 16.93ms | 12.45ms | 0.7× | 0.8% | PASS |
| ac_reads | `oltp_sum_range` | 15.43ms | 12.42ms | 0.8× | 1.0% | PASS |
| ac_reads | `oltp_order_range` | 3.25ms | 2.78ms | 0.9× | 0.7% | PASS |
| ac_reads | `oltp_distinct_range` | 4.07ms | 3.66ms | 0.9× | 1.0% | PASS |
| ac_reads | `oltp_index_scan` | 5.80ms | 6.69ms | 1.2× | 0.7% | PASS |
| ac_reads | `select_random_points` | 20.75ms | 17.86ms | 0.9× | 0.6% | PASS |
| ac_reads | `select_random_ranges` | 5.31ms | 5.95ms | 1.1× | 0.8% | PASS |
| ac_reads | `covering_index_scan` | 5.93ms | 5.63ms | 0.9× | 0.7% | PASS |
| ac_reads | `groupby_scan` | 29.78ms | 28.76ms | 1.0× | 0.5% | PASS |
| ac_reads | `index_join` | 11.00ms | 8.68ms | 0.8× | 1.0% | PASS |
| ac_reads | `index_join_scan` | 3.65ms | 4.97ms | 1.4× | 0.7% | PASS |
| ac_reads | `types_table_scan` | 902.40ms | 954.91ms | 1.1× | 0.3% | PASS |
| ac_reads | `table_scan` | 1.05s | 1.07s | 1.0× | 1.0% | PASS |
| ac_reads | `oltp_read_only` | 141.95ms | 126.33ms | 0.9× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 41.72ms | 111.89ms | 2.7× | 49.3% | PASS |
| ac_writes | `oltp_insert_ac` | 56.66ms | 126.73ms | 2.2× | 38.5% | PASS |
| ac_writes | `oltp_update_index_ac` | 44.58ms | 172.19ms | 3.9× | 38.6% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 36.93ms | 115.56ms | 3.1× | 34.9% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 95.41ms | 200.88ms | 2.1× | 56.2% | PASS |
| ac_writes | `oltp_write_only_ac` | 53.97ms | 166.54ms | 3.1× | 47.4% | PASS |
| ac_writes | `types_delete_insert_ac` | 41.55ms | 117.58ms | 2.8× | 35.7% | PASS |
| ac_writes | `oltp_read_write_ac` | 76.39ms | 196.15ms | 2.6× | 42.2% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 35.24ms | 35.30ms | 1.0× | 1.3% | PASS |
| mem_reads | `oltp_range_select` | 18.05ms | 13.67ms | 0.8× | 2.8% | PASS |
| mem_reads | `oltp_sum_range` | 16.28ms | 13.26ms | 0.8× | 2.1% | PASS |
| mem_reads | `oltp_order_range` | 3.59ms | 3.21ms | 0.9× | 1.1% | PASS |
| mem_reads | `oltp_distinct_range` | 4.60ms | 4.27ms | 0.9× | 0.9% | PASS |
| mem_reads | `oltp_index_scan` | 4.09ms | 5.98ms | 1.5× | 2.2% | PASS |
| mem_reads | `select_random_points` | 21.61ms | 19.95ms | 0.9× | 3.4% | PASS |
| mem_reads | `select_random_ranges` | 3.50ms | 5.25ms | 1.5× | 1.2% | PASS |
| mem_reads | `covering_index_scan` | 4.27ms | 4.42ms | 1.0× | 1.6% | PASS |
| mem_reads | `groupby_scan` | 37.54ms | 36.08ms | 1.0× | 0.7% | PASS |
| mem_reads | `index_join` | 10.87ms | 8.87ms | 0.8× | 2.6% | PASS |
| mem_reads | `index_join_scan` | 3.86ms | 5.53ms | 1.4× | 1.6% | PASS |
| mem_reads | `types_table_scan` | 1.23s | 1.29s | 1.0× | 3.4% | PASS |
| mem_reads | `table_scan` | 1.47s | 1.45s | 1.0× | 0.5% | PASS |
| mem_reads | `oltp_read_only` | 158.21ms | 138.46ms | 0.9× | 0.9% | PASS |
| mem_writes | `oltp_bulk_insert` | 246.97ms | 350.56ms | 1.4× | 1.1% | PASS |
| mem_writes | `oltp_insert` | 19.51ms | 39.76ms | 2.0× | 1.2% | PASS |
| mem_writes | `oltp_update_index` | 74.31ms | 146.97ms | 2.0× | 1.9% | PASS |
| mem_writes | `oltp_update_non_index` | 55.53ms | 83.57ms | 1.5× | 1.6% | PASS |
| mem_writes | `oltp_delete_insert` | 59.07ms | 111.82ms | 1.9× | 1.5% | PASS |
| mem_writes | `oltp_write_only` | 31.53ms | 66.34ms | 2.1× | 1.5% | PASS |
| mem_writes | `types_delete_insert` | 42.38ms | 55.35ms | 1.3× | 1.5% | PASS |
| mem_writes | `oltp_read_write` | 111.51ms | 148.71ms | 1.3× | 1.3% | PASS |
| file_reads | `oltp_point_select` | 124.80ms | 58.80ms | 0.5× | 0.6% | PASS |
| file_reads | `oltp_range_select` | 28.12ms | 16.55ms | 0.6× | 1.1% | PASS |
| file_reads | `oltp_sum_range` | 26.05ms | 16.10ms | 0.6× | 1.2% | PASS |
| file_reads | `oltp_order_range` | 4.53ms | 3.50ms | 0.8× | 0.9% | PASS |
| file_reads | `oltp_distinct_range` | 5.58ms | 4.60ms | 0.8× | 0.9% | PASS |
| file_reads | `oltp_index_scan` | 12.99ms | 8.46ms | 0.7× | 1.0% | PASS |
| file_reads | `select_random_points` | 34.76ms | 24.03ms | 0.7× | 1.2% | PASS |
| file_reads | `select_random_ranges` | 12.00ms | 7.49ms | 0.6× | 0.9% | PASS |
| file_reads | `covering_index_scan` | 12.93ms | 6.95ms | 0.5× | 1.0% | PASS |
| file_reads | `groupby_scan` | 38.70ms | 36.51ms | 0.9× | 0.9% | PASS |
| file_reads | `index_join` | 16.17ms | 10.93ms | 0.7× | 3.1% | PASS |
| file_reads | `index_join_scan` | 5.08ms | 6.10ms | 1.2× | 2.7% | PASS |
| file_reads | `types_table_scan` | 1.15s | 1.23s | 1.1× | 0.6% | PASS |
| file_reads | `table_scan` | 1.37s | 1.39s | 1.0× | 2.5% | PASS |
| file_reads | `oltp_read_only` | 270.79ms | 165.04ms | 0.6× | 1.1% | PASS |
| file_writes | `oltp_bulk_insert` | 270.13ms | 359.18ms | 1.3× | 1.3% | PASS |
| file_writes | `oltp_insert` | 25.83ms | 45.65ms | 1.8× | 1.9% | PASS |
| file_writes | `oltp_update_index` | 97.05ms | 148.08ms | 1.5× | 2.1% | PASS |
| file_writes | `oltp_update_non_index` | 87.28ms | 90.51ms | 1.0× | 10.0% | PASS |
| file_writes | `oltp_delete_insert` | 87.52ms | 117.54ms | 1.3× | 1.9% | PASS |
| file_writes | `oltp_write_only` | 58.66ms | 73.94ms | 1.3× | 2.8% | PASS |
| file_writes | `types_delete_insert` | 63.24ms | 60.87ms | 1.0× | 1.8% | PASS |
| file_writes | `oltp_read_write` | 125.18ms | 148.18ms | 1.2× | 1.0% | PASS |
| ac_reads | `oltp_point_select` | 62.79ms | 56.52ms | 0.9× | 1.0% | PASS |
| ac_reads | `oltp_range_select` | 21.83ms | 16.30ms | 0.7× | 2.6% | PASS |
| ac_reads | `oltp_sum_range` | 19.47ms | 15.60ms | 0.8× | 2.6% | PASS |
| ac_reads | `oltp_order_range` | 4.16ms | 3.58ms | 0.9× | 3.4% | PASS |
| ac_reads | `oltp_distinct_range` | 5.18ms | 4.68ms | 0.9× | 2.5% | PASS |
| ac_reads | `oltp_index_scan` | 7.18ms | 8.18ms | 1.1× | 1.3% | PASS |
| ac_reads | `select_random_points` | 25.15ms | 22.27ms | 0.9× | 1.2% | PASS |
| ac_reads | `select_random_ranges` | 6.53ms | 7.49ms | 1.1× | 1.1% | PASS |
| ac_reads | `covering_index_scan` | 7.58ms | 6.98ms | 0.9× | 1.2% | PASS |
| ac_reads | `groupby_scan` | 38.13ms | 36.46ms | 1.0× | 0.7% | PASS |
| ac_reads | `index_join` | 13.25ms | 10.85ms | 0.8× | 2.3% | PASS |
| ac_reads | `index_join_scan` | 4.49ms | 6.06ms | 1.4× | 2.0% | PASS |
| ac_reads | `types_table_scan` | 1.26s | 1.28s | 1.0× | 1.4% | PASS |
| ac_reads | `table_scan` | 1.45s | 1.43s | 1.0× | 1.2% | PASS |
| ac_reads | `oltp_read_only` | 185.69ms | 162.70ms | 0.9× | 2.1% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 17.70ms | 59.83ms | 3.4× | 5.2% | PASS |
| ac_writes | `oltp_insert_ac` | 19.90ms | 76.12ms | 3.8× | 5.2% | PASS |
| ac_writes | `oltp_update_index_ac` | 20.55ms | 87.85ms | 4.3× | 5.9% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 17.21ms | 71.94ms | 4.2× | 6.7% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 20.22ms | 79.35ms | 3.9× | 5.2% | PASS |
| ac_writes | `oltp_write_only_ac` | 19.41ms | 76.93ms | 4.0× | 4.2% | PASS |
| ac_writes | `types_delete_insert_ac` | 18.61ms | 71.14ms | 3.8× | 5.0% | PASS |
| ac_writes | `oltp_read_write_ac` | 26.64ms | 85.32ms | 3.2× | 3.9% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio noise | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 19.68ms | 23.09ms | 1.2× | 2.2% | PASS |
| mem_reads | `oltp_range_select` | 12.00ms | 13.58ms | 1.1× | 4.6% | PASS |
| mem_reads | `oltp_sum_range` | 11.82ms | 13.29ms | 1.1× | 3.3% | PASS |
| mem_reads | `oltp_order_range` | 2.33ms | 2.46ms | 1.1× | 2.6% | PASS |
| mem_reads | `oltp_distinct_range` | 2.95ms | 3.28ms | 1.1× | 2.6% | PASS |
| mem_reads | `oltp_index_scan` | 2.70ms | 3.49ms | 1.3× | 3.7% | PASS |
| mem_reads | `select_random_points` | 18.48ms | 20.61ms | 1.1× | 2.4% | PASS |
| mem_reads | `select_random_ranges` | 4.52ms | 5.35ms | 1.2× | 3.1% | PASS |
| mem_reads | `covering_index_scan` | 2.46ms | 2.45ms | 1.0× | 0.9% | PASS |
| mem_reads | `groupby_scan` | 22.87ms | 27.78ms | 1.2× | 1.0% | PASS |
| mem_reads | `index_join` | 5.19ms | 6.90ms | 1.3× | 2.8% | PASS |
| mem_reads | `index_join_scan` | 2.56ms | 4.33ms | 1.7× | 4.9% | PASS |
| mem_reads | `types_table_scan` | 751.88ms | 818.97ms | 1.1× | 0.8% | PASS |
| mem_reads | `table_scan` | 867.41ms | 911.34ms | 1.1× | 1.5% | PASS |
| mem_reads | `oltp_read_only` | 90.87ms | 104.52ms | 1.2× | 1.3% | PASS |
| mem_writes | `oltp_bulk_insert` | 142.34ms | 190.70ms | 1.3× | 0.4% | PASS |
| mem_writes | `oltp_insert` | 11.79ms | 20.10ms | 1.7× | 0.9% | PASS |
| mem_writes | `oltp_update_index` | 42.83ms | 76.21ms | 1.8× | 2.3% | PASS |
| mem_writes | `oltp_update_non_index` | 30.51ms | 46.54ms | 1.5× | 2.6% | PASS |
| mem_writes | `oltp_delete_insert` | 30.97ms | 56.32ms | 1.8× | 2.1% | PASS |
| mem_writes | `oltp_write_only` | 16.39ms | 32.84ms | 2.0× | 2.9% | PASS |
| mem_writes | `types_delete_insert` | 19.57ms | 29.65ms | 1.5× | 1.4% | PASS |
| mem_writes | `oltp_read_write` | 58.94ms | 90.97ms | 1.5× | 2.6% | PASS |
| file_reads | `oltp_point_select` | 41.88ms | 29.23ms | 0.7× | 1.3% | PASS |
| file_reads | `oltp_range_select` | 15.57ms | 14.75ms | 0.9× | 3.2% | PASS |
| file_reads | `oltp_sum_range` | 15.12ms | 14.04ms | 0.9× | 3.2% | PASS |
| file_reads | `oltp_order_range` | 2.86ms | 2.71ms | 0.9× | 2.2% | PASS |
| file_reads | `oltp_distinct_range` | 3.43ms | 3.41ms | 1.0× | 3.3% | PASS |
| file_reads | `oltp_index_scan` | 5.32ms | 4.49ms | 0.8× | 3.7% | PASS |
| file_reads | `select_random_points` | 22.23ms | 23.44ms | 1.1× | 1.6% | PASS |
| file_reads | `select_random_ranges` | 7.49ms | 6.62ms | 0.9× | 1.4% | PASS |
| file_reads | `covering_index_scan` | 4.94ms | 3.65ms | 0.7× | 2.7% | PASS |
| file_reads | `groupby_scan` | 23.78ms | 27.60ms | 1.2× | 1.2% | PASS |
| file_reads | `index_join` | 6.86ms | 7.87ms | 1.1× | 2.9% | PASS |
| file_reads | `index_join_scan` | 3.18ms | 4.68ms | 1.5× | 2.9% | PASS |
| file_reads | `types_table_scan` | 755.06ms | 814.53ms | 1.1× | 1.0% | PASS |
| file_reads | `table_scan` | 870.98ms | 906.54ms | 1.0× | 2.3% | PASS |
| file_reads | `oltp_read_only` | 122.59ms | 113.76ms | 0.9× | 1.8% | PASS |
| file_writes | `oltp_bulk_insert` | 278.15ms | 283.17ms | 1.0× | 27.0% | PASS |
| file_writes | `oltp_insert` | 127.90ms | 75.64ms | 0.6× | 69.3% | PASS |
| file_writes | `oltp_update_index` | 255.50ms | 183.15ms | 0.7× | 31.0% | PASS |
| file_writes | `oltp_update_non_index` | 222.86ms | 133.44ms | 0.6× | 36.3% | PASS |
| file_writes | `oltp_delete_insert` | 202.77ms | 133.49ms | 0.7× | 31.4% | PASS |
| file_writes | `oltp_write_only` | 252.31ms | 93.95ms | 0.4× | 40.7% | PASS |
| file_writes | `types_delete_insert` | 129.69ms | 119.26ms | 0.9× | 46.0% | PASS |
| file_writes | `oltp_read_write` | 301.03ms | 203.56ms | 0.7× | 33.6% | PASS |
| ac_reads | `oltp_point_select` | 27.45ms | 29.45ms | 1.1× | 0.9% | PASS |
| ac_reads | `oltp_range_select` | 14.86ms | 14.79ms | 1.0× | 2.1% | PASS |
| ac_reads | `oltp_sum_range` | 14.54ms | 14.23ms | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_order_range` | 2.67ms | 2.66ms | 1.0× | 2.4% | PASS |
| ac_reads | `oltp_distinct_range` | 3.43ms | 3.44ms | 1.0× | 3.0% | PASS |
| ac_reads | `oltp_index_scan` | 4.29ms | 5.00ms | 1.2× | 2.8% | PASS |
| ac_reads | `select_random_points` | 21.18ms | 23.63ms | 1.1× | 1.6% | PASS |
| ac_reads | `select_random_ranges` | 5.92ms | 6.49ms | 1.1× | 1.1% | PASS |
| ac_reads | `covering_index_scan` | 3.49ms | 3.60ms | 1.0× | 2.5% | PASS |
| ac_reads | `groupby_scan` | 23.82ms | 27.75ms | 1.2× | 1.4% | PASS |
| ac_reads | `index_join` | 6.34ms | 8.46ms | 1.3× | 1.8% | PASS |
| ac_reads | `index_join_scan` | 3.15ms | 4.78ms | 1.5× | 4.1% | PASS |
| ac_reads | `types_table_scan` | 757.37ms | 817.04ms | 1.1× | 1.5% | PASS |
| ac_reads | `table_scan` | 869.92ms | 907.32ms | 1.0× | 1.6% | PASS |
| ac_reads | `oltp_read_only` | 103.64ms | 113.70ms | 1.1× | 1.0% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 418.42ms | 1.28s | 3.1× | 59.0% | PASS |
| ac_writes | `oltp_insert_ac` | 224.49ms | 922.94ms | 4.1× | 55.7% | PASS |
| ac_writes | `oltp_update_index_ac` | 342.93ms | 1.12s | 3.3× | 60.4% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 635.86ms | 1.57s | 2.5× | 47.4% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 317.66ms | 1.04s | 3.3× | 55.6% | PASS |
| ac_writes | `oltp_write_only_ac` | 326.66ms | 1.11s | 3.4× | 60.6% | PASS |
| ac_writes | `types_delete_insert_ac` | 374.30ms | 1.29s | 3.4× | 59.8% | PASS |
| ac_writes | `oltp_read_write_ac` | 444.18ms | 1.30s | 2.9× | 60.2% | PASS |

</details>

</details>

## Version-control latency

Wall time: 2m 11s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 91.95ms | 130.00ms | 70.7% | 0.2% | PASS |
| `status_dirty_many_tables` | 95.62ms | 130.00ms | 73.6% | 0.3% | PASS |
| `diff_regular_working_one_table` | 86.78ms | 120.00ms | 72.3% | 0.3% | PASS |
| `diff_regular_working_many_tables` | 100.49ms | 140.00ms | 71.8% | 0.3% | PASS |
| `diff_stat_working_many_tables` | 100.37ms | 140.00ms | 71.7% | 0.2% | PASS |
| `diff_schema_working_many_tables` | 100.66ms | 140.00ms | 71.9% | 0.3% | PASS |
| `branch_list_many_branches` | 24.74ms | 35.00ms | 70.7% | 1.5% | PASS |
| `branch_create_delete` | 27.30ms | 40.00ms | 68.3% | 1.4% | PASS |
| `checkout_branch_clean` | 59.99ms | 150.00ms | 40.0% | 1.0% | PASS |
| `merge_data_no_conflicts` | 31.82ms | 50.00ms | 63.6% | 1.3% | PASS |
| `merge_schema_no_conflicts` | 23.92ms | 35.00ms | 68.3% | 2.0% | PASS |
| `merge_data_conflicts` | 35.90ms | 180.00ms | 19.9% | 1.4% | PASS |
| `merge_data_conflicts_with_resolve` | 35.86ms | 180.00ms | 19.9% | 0.8% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
