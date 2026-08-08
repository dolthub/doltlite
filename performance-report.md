# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-08-07 22:15 UTC
>
> Commit: [`9e17622b32ab29d7a4f394eba93773fc468f7bc6`](https://github.com/dolthub/doltlite/commit/9e17622b32ab29d7a4f394eba93773fc468f7bc6)
>
> Runner: ubuntu24 20260720.247.2
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/31216770366)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians; MAD is the median absolute deviation and describes run-to-run noise.

## SQL workload summary

| Key shape | Workloads | Samples/workload | Wall time | SQLite median total | DoltLite median total | Ratio | Median paired-ratio MAD | Result |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| int | 69 | 55 | 1h 11m 53s | 9.29s | 11.03s | 1.187× | 1.06% | **PASS** |
| textpk | 69 | 55 | 1h 30m 1s | 9.58s | 11.28s | 1.178× | 1.24% | **PASS** |
| blobpk | 69 | 55 | 1h 33m 5s | 10.24s | 12.36s | 1.206× | 1.88% | **PASS** |
| compositepk | 69 | 55 | 1h 15m 45s | 8.18s | 10.15s | 1.241× | 1.83% | **PASS** |

The absolute ceiling is 2.4× per ordinary workload and 1.95× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.35ms | 27.56ms | 1.132× | 1.51% | PASS |
| mem_reads | `oltp_range_select` | 10.44ms | 11.80ms | 1.130× | 1.03% | PASS |
| mem_reads | `oltp_sum_range` | 9.62ms | 11.32ms | 1.176× | 0.97% | PASS |
| mem_reads | `oltp_order_range` | 2.65ms | 2.88ms | 1.088× | 0.87% | PASS |
| mem_reads | `oltp_distinct_range` | 3.73ms | 3.89ms | 1.043× | 0.83% | PASS |
| mem_reads | `oltp_index_scan` | 3.92ms | 4.85ms | 1.236× | 1.49% | PASS |
| mem_reads | `select_random_points` | 10.22ms | 10.97ms | 1.073× | 1.11% | PASS |
| mem_reads | `select_random_ranges` | 3.08ms | 3.98ms | 1.295× | 0.94% | PASS |
| mem_reads | `covering_index_scan` | 4.32ms | 4.05ms | 0.937× | 1.34% | PASS |
| mem_reads | `groupby_scan` | 31.83ms | 34.15ms | 1.073× | 0.68% | PASS |
| mem_reads | `index_join` | 5.91ms | 7.57ms | 1.282× | 1.05% | PASS |
| mem_reads | `index_join_scan` | 3.44ms | 4.62ms | 1.342× | 1.06% | PASS |
| mem_reads | `types_table_scan` | 1.11s | 1.26s | 1.140× | 0.57% | PASS |
| mem_reads | `table_scan` | 1.25s | 1.37s | 1.096× | 0.55% | PASS |
| mem_reads | `oltp_read_only` | 102.38ms | 114.72ms | 1.120× | 0.82% | PASS |
| mem_writes | `oltp_bulk_insert` | 181.85ms | 241.26ms | 1.327× | 0.82% | PASS |
| mem_writes | `oltp_insert` | 15.81ms | 28.14ms | 1.780× | 0.59% | PASS |
| mem_writes | `oltp_update_index` | 50.14ms | 103.72ms | 2.069× | 0.79% | PASS |
| mem_writes | `oltp_update_non_index` | 34.19ms | 57.65ms | 1.686× | 0.79% | PASS |
| mem_writes | `oltp_delete_insert` | 43.79ms | 77.37ms | 1.767× | 0.73% | PASS |
| mem_writes | `oltp_write_only` | 21.34ms | 44.55ms | 2.088× | 0.93% | PASS |
| mem_writes | `types_delete_insert` | 24.33ms | 39.49ms | 1.623× | 1.00% | PASS |
| mem_writes | `oltp_read_write` | 63.63ms | 103.86ms | 1.632× | 0.58% | PASS |
| file_reads | `oltp_point_select` | 105.94ms | 55.80ms | 0.527× | 1.05% | PASS |
| file_reads | `oltp_range_select` | 18.03ms | 14.67ms | 0.814× | 0.78% | PASS |
| file_reads | `oltp_sum_range` | 17.40ms | 14.26ms | 0.819× | 0.57% | PASS |
| file_reads | `oltp_order_range` | 3.42ms | 3.22ms | 0.943× | 1.21% | PASS |
| file_reads | `oltp_distinct_range` | 4.45ms | 4.22ms | 0.949× | 0.65% | PASS |
| file_reads | `oltp_index_scan` | 12.12ms | 8.07ms | 0.666× | 1.01% | PASS |
| file_reads | `select_random_points` | 18.80ms | 14.29ms | 0.760× | 3.49% | PASS |
| file_reads | `select_random_ranges` | 11.27ms | 6.90ms | 0.612× | 1.02% | PASS |
| file_reads | `covering_index_scan` | 12.66ms | 7.13ms | 0.563× | 1.79% | PASS |
| file_reads | `groupby_scan` | 32.94ms | 34.66ms | 1.052× | 0.82% | PASS |
| file_reads | `index_join` | 10.39ms | 9.77ms | 0.940× | 1.40% | PASS |
| file_reads | `index_join_scan` | 4.38ms | 5.00ms | 1.142× | 1.57% | PASS |
| file_reads | `types_table_scan` | 1.10s | 1.26s | 1.147× | 0.33% | PASS |
| file_reads | `table_scan` | 1.29s | 1.39s | 1.077× | 2.74% | PASS |
| file_reads | `oltp_read_only` | 232.37ms | 160.83ms | 0.692× | 0.74% | PASS |
| file_writes | `oltp_bulk_insert` | 195.67ms | 261.88ms | 1.338× | 0.96% | PASS |
| file_writes | `oltp_insert` | 22.49ms | 36.52ms | 1.624× | 2.25% | PASS |
| file_writes | `oltp_update_index` | 81.26ms | 133.99ms | 1.649× | 2.43% | PASS |
| file_writes | `oltp_update_non_index` | 60.01ms | 82.68ms | 1.378× | 1.83% | PASS |
| file_writes | `oltp_delete_insert` | 68.13ms | 100.33ms | 1.473× | 1.56% | PASS |
| file_writes | `oltp_write_only` | 45.05ms | 66.00ms | 1.465× | 2.69% | PASS |
| file_writes | `types_delete_insert` | 40.04ms | 53.62ms | 1.339× | 1.60% | PASS |
| file_writes | `oltp_read_write` | 90.00ms | 126.23ms | 1.403× | 3.05% | PASS |
| ac_reads | `oltp_point_select` | 51.85ms | 55.86ms | 1.077× | 1.13% | PASS |
| ac_reads | `oltp_range_select` | 14.22ms | 14.78ms | 1.039× | 2.32% | PASS |
| ac_reads | `oltp_sum_range` | 12.72ms | 14.30ms | 1.124× | 1.17% | PASS |
| ac_reads | `oltp_order_range` | 3.11ms | 3.24ms | 1.042× | 1.28% | PASS |
| ac_reads | `oltp_distinct_range` | 4.08ms | 4.23ms | 1.036× | 1.17% | PASS |
| ac_reads | `oltp_index_scan` | 7.11ms | 8.18ms | 1.151× | 1.49% | PASS |
| ac_reads | `select_random_points` | 14.33ms | 14.49ms | 1.011× | 1.66% | PASS |
| ac_reads | `select_random_ranges` | 6.03ms | 6.92ms | 1.147× | 0.82% | PASS |
| ac_reads | `covering_index_scan` | 7.51ms | 7.36ms | 0.981× | 0.96% | PASS |
| ac_reads | `groupby_scan` | 32.55ms | 34.67ms | 1.065× | 0.83% | PASS |
| ac_reads | `index_join` | 7.61ms | 9.68ms | 1.271× | 1.25% | PASS |
| ac_reads | `index_join_scan` | 3.97ms | 5.03ms | 1.267× | 1.60% | PASS |
| ac_reads | `types_table_scan` | 1.11s | 1.26s | 1.144× | 0.58% | PASS |
| ac_reads | `table_scan` | 1.25s | 1.37s | 1.097× | 0.52% | PASS |
| ac_reads | `oltp_read_only` | 142.45ms | 155.48ms | 1.091× | 0.65% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 15.62ms | 61.62ms | 3.945× | 4.72% | PASS |
| ac_writes | `oltp_insert_ac` | 17.24ms | 77.90ms | 4.519× | 4.30% | PASS |
| ac_writes | `oltp_update_index_ac` | 19.36ms | 92.45ms | 4.776× | 4.30% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 15.24ms | 70.21ms | 4.607× | 4.54% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 17.09ms | 85.07ms | 4.977× | 5.24% | PASS |
| ac_writes | `oltp_write_only_ac` | 17.10ms | 82.81ms | 4.844× | 3.30% | PASS |
| ac_writes | `types_delete_insert_ac` | 14.65ms | 70.23ms | 4.792× | 4.23% | PASS |
| ac_writes | `oltp_read_write_ac` | 21.76ms | 88.77ms | 4.079× | 3.21% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 25.29ms | 29.61ms | 1.171× | 1.06% | PASS |
| mem_reads | `oltp_range_select` | 11.78ms | 12.44ms | 1.056× | 1.44% | PASS |
| mem_reads | `oltp_sum_range` | 11.19ms | 11.59ms | 1.036× | 1.59% | PASS |
| mem_reads | `oltp_order_range` | 2.71ms | 2.88ms | 1.062× | 0.86% | PASS |
| mem_reads | `oltp_distinct_range` | 3.59ms | 3.80ms | 1.060× | 1.28% | PASS |
| mem_reads | `oltp_index_scan` | 3.93ms | 4.90ms | 1.246× | 1.37% | PASS |
| mem_reads | `select_random_points` | 15.98ms | 18.45ms | 1.154× | 1.24% | PASS |
| mem_reads | `select_random_ranges` | 3.36ms | 4.36ms | 1.298× | 1.13% | PASS |
| mem_reads | `covering_index_scan` | 3.79ms | 3.62ms | 0.957× | 1.31% | PASS |
| mem_reads | `groupby_scan` | 30.81ms | 31.48ms | 1.021× | 0.70% | PASS |
| mem_reads | `index_join` | 6.40ms | 8.12ms | 1.269× | 1.27% | PASS |
| mem_reads | `index_join_scan` | 4.17ms | 5.11ms | 1.226× | 1.25% | PASS |
| mem_reads | `types_table_scan` | 1.03s | 1.18s | 1.148× | 0.33% | PASS |
| mem_reads | `table_scan` | 1.22s | 1.29s | 1.050× | 0.66% | PASS |
| mem_reads | `oltp_read_only` | 103.62ms | 116.76ms | 1.127× | 0.73% | PASS |
| mem_writes | `oltp_bulk_insert` | 185.78ms | 271.31ms | 1.460× | 0.75% | PASS |
| mem_writes | `oltp_insert` | 18.41ms | 31.54ms | 1.713× | 1.08% | PASS |
| mem_writes | `oltp_update_index` | 60.64ms | 112.34ms | 1.853× | 0.93% | PASS |
| mem_writes | `oltp_update_non_index` | 40.48ms | 70.01ms | 1.730× | 1.03% | PASS |
| mem_writes | `oltp_delete_insert` | 42.99ms | 85.35ms | 1.985× | 0.65% | PASS |
| mem_writes | `oltp_write_only` | 23.96ms | 50.17ms | 2.093× | 0.88% | PASS |
| mem_writes | `types_delete_insert` | 26.88ms | 43.88ms | 1.632× | 0.99% | PASS |
| mem_writes | `oltp_read_write` | 68.70ms | 112.40ms | 1.636× | 1.17% | PASS |
| file_reads | `oltp_point_select` | 55.17ms | 40.54ms | 0.735× | 0.72% | PASS |
| file_reads | `oltp_range_select` | 15.13ms | 14.00ms | 0.925× | 1.49% | PASS |
| file_reads | `oltp_sum_range` | 14.36ms | 13.20ms | 0.919× | 1.23% | PASS |
| file_reads | `oltp_order_range` | 3.14ms | 3.08ms | 0.982× | 1.41% | PASS |
| file_reads | `oltp_distinct_range` | 4.03ms | 4.00ms | 0.991× | 1.45% | PASS |
| file_reads | `oltp_index_scan` | 7.13ms | 6.42ms | 0.901× | 2.06% | PASS |
| file_reads | `select_random_points` | 19.70ms | 20.19ms | 1.025× | 1.44% | PASS |
| file_reads | `select_random_ranges` | 6.50ms | 5.61ms | 0.863× | 1.48% | PASS |
| file_reads | `covering_index_scan` | 7.28ms | 5.09ms | 0.699× | 2.37% | PASS |
| file_reads | `groupby_scan` | 31.36ms | 31.80ms | 1.014× | 0.93% | PASS |
| file_reads | `index_join` | 8.32ms | 9.47ms | 1.138× | 2.09% | PASS |
| file_reads | `index_join_scan` | 4.63ms | 5.52ms | 1.191× | 1.41% | PASS |
| file_reads | `types_table_scan` | 1.03s | 1.18s | 1.145× | 0.34% | PASS |
| file_reads | `table_scan` | 1.20s | 1.28s | 1.063× | 0.28% | PASS |
| file_reads | `oltp_read_only` | 146.40ms | 131.47ms | 0.898× | 0.54% | PASS |
| file_writes | `oltp_bulk_insert` | 257.36ms | 359.83ms | 1.398× | 4.09% | PASS |
| file_writes | `oltp_insert` | 64.15ms | 65.07ms | 1.014× | 1.58% | PASS |
| file_writes | `oltp_update_index` | 226.90ms | 218.97ms | 0.965× | 1.18% | PASS |
| file_writes | `oltp_update_non_index` | 164.19ms | 148.59ms | 0.905× | 1.22% | PASS |
| file_writes | `oltp_delete_insert` | 186.76ms | 171.74ms | 0.920× | 0.80% | PASS |
| file_writes | `oltp_write_only` | 138.99ms | 119.97ms | 0.863× | 1.16% | PASS |
| file_writes | `types_delete_insert` | 112.75ms | 108.98ms | 0.967× | 2.58% | PASS |
| file_writes | `oltp_read_write` | 185.25ms | 187.73ms | 1.013× | 2.16% | PASS |
| ac_reads | `oltp_point_select` | 34.89ms | 40.53ms | 1.162× | 0.87% | PASS |
| ac_reads | `oltp_range_select` | 13.39ms | 14.00ms | 1.046× | 1.42% | PASS |
| ac_reads | `oltp_sum_range` | 12.76ms | 13.27ms | 1.040× | 1.13% | PASS |
| ac_reads | `oltp_order_range` | 2.97ms | 3.10ms | 1.046× | 0.98% | PASS |
| ac_reads | `oltp_distinct_range` | 3.86ms | 4.01ms | 1.039× | 1.29% | PASS |
| ac_reads | `oltp_index_scan` | 5.41ms | 6.49ms | 1.201× | 1.81% | PASS |
| ac_reads | `select_random_points` | 17.92ms | 20.27ms | 1.131× | 0.92% | PASS |
| ac_reads | `select_random_ranges` | 4.69ms | 5.66ms | 1.207× | 2.12% | PASS |
| ac_reads | `covering_index_scan` | 5.72ms | 5.17ms | 0.904× | 1.84% | PASS |
| ac_reads | `groupby_scan` | 31.51ms | 31.84ms | 1.011× | 0.67% | PASS |
| ac_reads | `index_join` | 8.02ms | 9.86ms | 1.230× | 1.49% | PASS |
| ac_reads | `index_join_scan` | 4.65ms | 5.61ms | 1.206× | 1.12% | PASS |
| ac_reads | `types_table_scan` | 1.03s | 1.18s | 1.148× | 0.38% | PASS |
| ac_reads | `table_scan` | 1.20s | 1.28s | 1.063× | 0.39% | PASS |
| ac_reads | `oltp_read_only` | 117.86ms | 132.33ms | 1.123× | 0.64% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 27.28ms | 96.19ms | 3.527× | 3.64% | PASS |
| ac_writes | `oltp_insert_ac` | 30.06ms | 107.63ms | 3.580× | 3.12% | PASS |
| ac_writes | `oltp_update_index_ac` | 31.15ms | 125.17ms | 4.018× | 3.84% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 27.63ms | 106.39ms | 3.851× | 3.38% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 29.34ms | 115.18ms | 3.925× | 3.29% | PASS |
| ac_writes | `oltp_write_only_ac` | 29.55ms | 115.71ms | 3.916× | 4.21% | PASS |
| ac_writes | `types_delete_insert_ac` | 27.29ms | 106.41ms | 3.899× | 3.65% | PASS |
| ac_writes | `oltp_read_write_ac` | 33.67ms | 121.54ms | 3.610× | 3.58% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 32.03ms | 37.83ms | 1.181× | 2.05% | PASS |
| mem_reads | `oltp_range_select` | 13.79ms | 15.33ms | 1.111× | 3.20% | PASS |
| mem_reads | `oltp_sum_range` | 12.27ms | 14.05ms | 1.145× | 1.76% | PASS |
| mem_reads | `oltp_order_range` | 3.00ms | 3.28ms | 1.095× | 1.18% | PASS |
| mem_reads | `oltp_distinct_range` | 4.01ms | 4.31ms | 1.072× | 1.30% | PASS |
| mem_reads | `oltp_index_scan` | 4.67ms | 6.38ms | 1.365× | 0.96% | PASS |
| mem_reads | `select_random_points` | 18.70ms | 20.92ms | 1.119× | 3.32% | PASS |
| mem_reads | `select_random_ranges` | 4.16ms | 5.21ms | 1.250× | 1.90% | PASS |
| mem_reads | `covering_index_scan` | 4.55ms | 4.75ms | 1.043× | 2.41% | PASS |
| mem_reads | `groupby_scan` | 32.01ms | 34.43ms | 1.076× | 1.13% | PASS |
| mem_reads | `index_join` | 6.85ms | 9.61ms | 1.402× | 2.79% | PASS |
| mem_reads | `index_join_scan` | 4.35ms | 5.42ms | 1.247× | 4.82% | PASS |
| mem_reads | `types_table_scan` | 1.21s | 1.34s | 1.111× | 1.26% | PASS |
| mem_reads | `table_scan` | 1.47s | 1.47s | 1.001× | 0.91% | PASS |
| mem_reads | `oltp_read_only` | 127.75ms | 142.69ms | 1.117× | 0.66% | PASS |
| mem_writes | `oltp_bulk_insert` | 241.56ms | 360.82ms | 1.494× | 0.56% | PASS |
| mem_writes | `oltp_insert` | 20.95ms | 40.42ms | 1.929× | 2.18% | PASS |
| mem_writes | `oltp_update_index` | 76.77ms | 138.95ms | 1.810× | 2.76% | PASS |
| mem_writes | `oltp_update_non_index` | 51.59ms | 87.92ms | 1.704× | 1.84% | PASS |
| mem_writes | `oltp_delete_insert` | 49.44ms | 103.06ms | 2.085× | 1.21% | PASS |
| mem_writes | `oltp_write_only` | 30.38ms | 64.01ms | 2.107× | 2.38% | PASS |
| mem_writes | `types_delete_insert` | 33.90ms | 56.32ms | 1.661× | 1.14% | PASS |
| mem_writes | `oltp_read_write` | 90.97ms | 144.31ms | 1.586× | 2.51% | PASS |
| file_reads | `oltp_point_select` | 103.04ms | 63.51ms | 0.616× | 0.76% | PASS |
| file_reads | `oltp_range_select` | 21.93ms | 18.24ms | 0.832× | 1.72% | PASS |
| file_reads | `oltp_sum_range` | 20.76ms | 17.07ms | 0.822× | 1.57% | PASS |
| file_reads | `oltp_order_range` | 3.87ms | 3.60ms | 0.932× | 1.73% | PASS |
| file_reads | `oltp_distinct_range` | 4.96ms | 4.68ms | 0.943× | 1.40% | PASS |
| file_reads | `oltp_index_scan` | 12.27ms | 9.08ms | 0.740× | 1.52% | PASS |
| file_reads | `select_random_points` | 27.64ms | 24.99ms | 0.904× | 1.99% | PASS |
| file_reads | `select_random_ranges` | 11.50ms | 7.79ms | 0.677× | 1.07% | PASS |
| file_reads | `covering_index_scan` | 12.53ms | 7.26ms | 0.579× | 1.62% | PASS |
| file_reads | `groupby_scan` | 33.27ms | 35.05ms | 1.054× | 0.66% | PASS |
| file_reads | `index_join` | 11.87ms | 11.77ms | 0.991× | 3.10% | PASS |
| file_reads | `index_join_scan` | 5.37ms | 5.76ms | 1.074× | 2.81% | PASS |
| file_reads | `types_table_scan` | 1.07s | 1.30s | 1.217× | 0.73% | PASS |
| file_reads | `table_scan` | 1.23s | 1.42s | 1.160× | 1.69% | PASS |
| file_reads | `oltp_read_only` | 219.29ms | 174.13ms | 0.794× | 1.00% | PASS |
| file_writes | `oltp_bulk_insert` | 260.19ms | 380.51ms | 1.462× | 0.67% | PASS |
| file_writes | `oltp_insert` | 32.16ms | 51.60ms | 1.605× | 1.32% | PASS |
| file_writes | `oltp_update_index` | 103.55ms | 160.98ms | 1.555× | 2.26% | PASS |
| file_writes | `oltp_update_non_index` | 79.09ms | 107.34ms | 1.357× | 1.45% | PASS |
| file_writes | `oltp_delete_insert` | 81.17ms | 128.98ms | 1.589× | 1.15% | PASS |
| file_writes | `oltp_write_only` | 56.27ms | 83.95ms | 1.492× | 2.27% | PASS |
| file_writes | `types_delete_insert` | 51.87ms | 71.62ms | 1.381× | 1.00% | PASS |
| file_writes | `oltp_read_write` | 115.59ms | 161.17ms | 1.394× | 2.27% | PASS |
| ac_reads | `oltp_point_select` | 53.82ms | 61.10ms | 1.135× | 0.87% | PASS |
| ac_reads | `oltp_range_select` | 16.37ms | 17.92ms | 1.095× | 3.08% | PASS |
| ac_reads | `oltp_sum_range` | 15.62ms | 16.86ms | 1.079× | 2.28% | PASS |
| ac_reads | `oltp_order_range` | 3.43ms | 3.61ms | 1.055× | 2.10% | PASS |
| ac_reads | `oltp_distinct_range` | 4.45ms | 4.67ms | 1.048× | 1.81% | PASS |
| ac_reads | `oltp_index_scan` | 7.69ms | 9.14ms | 1.188× | 2.21% | PASS |
| ac_reads | `select_random_points` | 22.61ms | 24.71ms | 1.093× | 2.43% | PASS |
| ac_reads | `select_random_ranges` | 6.90ms | 7.79ms | 1.128× | 1.52% | PASS |
| ac_reads | `covering_index_scan` | 7.89ms | 7.29ms | 0.923× | 2.50% | PASS |
| ac_reads | `groupby_scan` | 32.76ms | 35.18ms | 1.074× | 1.09% | PASS |
| ac_reads | `index_join` | 9.50ms | 11.61ms | 1.222× | 4.35% | PASS |
| ac_reads | `index_join_scan` | 5.09ms | 6.04ms | 1.187× | 3.52% | PASS |
| ac_reads | `types_table_scan` | 1.22s | 1.34s | 1.103× | 1.65% | PASS |
| ac_reads | `table_scan` | 1.36s | 1.44s | 1.060× | 4.50% | PASS |
| ac_reads | `oltp_read_only` | 158.97ms | 176.61ms | 1.111× | 1.88% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 24.09ms | 82.59ms | 3.429× | 6.15% | PASS |
| ac_writes | `oltp_insert_ac` | 26.11ms | 103.84ms | 3.978× | 5.38% | PASS |
| ac_writes | `oltp_update_index_ac` | 28.57ms | 115.96ms | 4.059× | 6.15% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.50ms | 96.32ms | 3.931× | 5.68% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.22ms | 105.12ms | 4.009× | 5.68% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.80ms | 105.46ms | 3.935× | 5.04% | PASS |
| ac_writes | `types_delete_insert_ac` | 24.01ms | 95.54ms | 3.979× | 6.90% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.64ms | 115.60ms | 3.542× | 5.55% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 23.21ms | 26.96ms | 1.161× | 1.75% | PASS |
| mem_reads | `oltp_range_select` | 13.77ms | 15.76ms | 1.144× | 1.62% | PASS |
| mem_reads | `oltp_sum_range` | 12.88ms | 14.73ms | 1.144× | 1.65% | PASS |
| mem_reads | `oltp_order_range` | 2.72ms | 3.01ms | 1.106× | 2.07% | PASS |
| mem_reads | `oltp_distinct_range` | 3.51ms | 3.81ms | 1.084× | 1.46% | PASS |
| mem_reads | `oltp_index_scan` | 3.23ms | 4.15ms | 1.284× | 2.64% | PASS |
| mem_reads | `select_random_points` | 21.23ms | 23.73ms | 1.117× | 1.44% | PASS |
| mem_reads | `select_random_ranges` | 5.35ms | 6.11ms | 1.143× | 2.42% | PASS |
| mem_reads | `covering_index_scan` | 2.96ms | 3.01ms | 1.017× | 1.40% | PASS |
| mem_reads | `groupby_scan` | 28.75ms | 30.42ms | 1.058× | 1.02% | PASS |
| mem_reads | `index_join` | 6.06ms | 7.83ms | 1.292× | 2.01% | PASS |
| mem_reads | `index_join_scan` | 2.84ms | 3.88ms | 1.366× | 2.43% | PASS |
| mem_reads | `types_table_scan` | 877.24ms | 1.03s | 1.174× | 0.45% | PASS |
| mem_reads | `table_scan` | 1.00s | 1.11s | 1.107× | 0.56% | PASS |
| mem_reads | `oltp_read_only` | 109.94ms | 122.34ms | 1.113× | 0.79% | PASS |
| mem_writes | `oltp_bulk_insert` | 171.21ms | 232.58ms | 1.358× | 0.70% | PASS |
| mem_writes | `oltp_insert` | 13.79ms | 24.59ms | 1.783× | 0.74% | PASS |
| mem_writes | `oltp_update_index` | 46.59ms | 81.69ms | 1.753× | 1.72% | PASS |
| mem_writes | `oltp_update_non_index` | 33.15ms | 55.72ms | 1.681× | 1.32% | PASS |
| mem_writes | `oltp_delete_insert` | 34.14ms | 65.48ms | 1.918× | 1.98% | PASS |
| mem_writes | `oltp_write_only` | 17.71ms | 38.57ms | 2.178× | 3.02% | PASS |
| mem_writes | `types_delete_insert` | 21.98ms | 36.03ms | 1.639× | 1.44% | PASS |
| mem_writes | `oltp_read_write` | 68.36ms | 103.91ms | 1.520× | 1.81% | PASS |
| file_reads | `oltp_point_select` | 47.69ms | 34.89ms | 0.731× | 1.37% | PASS |
| file_reads | `oltp_range_select` | 16.89ms | 16.88ms | 0.999× | 1.90% | PASS |
| file_reads | `oltp_sum_range` | 15.93ms | 15.70ms | 0.985× | 2.08% | PASS |
| file_reads | `oltp_order_range` | 3.09ms | 3.13ms | 1.014× | 2.14% | PASS |
| file_reads | `oltp_distinct_range` | 3.89ms | 3.93ms | 1.010× | 1.83% | PASS |
| file_reads | `oltp_index_scan` | 5.92ms | 5.10ms | 0.862× | 1.69% | PASS |
| file_reads | `select_random_points` | 24.15ms | 25.11ms | 1.040× | 1.57% | PASS |
| file_reads | `select_random_ranges` | 8.03ms | 7.10ms | 0.885× | 1.98% | PASS |
| file_reads | `covering_index_scan` | 5.61ms | 3.88ms | 0.692× | 0.96% | PASS |
| file_reads | `groupby_scan` | 28.97ms | 30.52ms | 1.053× | 1.21% | PASS |
| file_reads | `index_join` | 7.61ms | 8.53ms | 1.121× | 1.57% | PASS |
| file_reads | `index_join_scan` | 3.20ms | 4.25ms | 1.328× | 3.98% | PASS |
| file_reads | `types_table_scan` | 874.29ms | 1.03s | 1.182× | 0.51% | PASS |
| file_reads | `table_scan` | 1.00s | 1.11s | 1.106× | 0.45% | PASS |
| file_reads | `oltp_read_only` | 145.49ms | 134.45ms | 0.924× | 0.89% | PASS |
| file_writes | `oltp_bulk_insert` | 224.55ms | 295.01ms | 1.314× | 4.02% | PASS |
| file_writes | `oltp_insert` | 27.92ms | 45.23ms | 1.620× | 10.58% | PASS |
| file_writes | `oltp_update_index` | 150.63ms | 151.27ms | 1.004× | 3.32% | PASS |
| file_writes | `oltp_update_non_index` | 125.27ms | 111.93ms | 0.894× | 3.89% | PASS |
| file_writes | `oltp_delete_insert` | 133.90ms | 128.05ms | 0.956× | 9.96% | PASS |
| file_writes | `oltp_write_only` | 90.40ms | 89.94ms | 0.995× | 8.42% | PASS |
| file_writes | `types_delete_insert` | 82.15ms | 76.83ms | 0.935× | 10.81% | PASS |
| file_writes | `oltp_read_write` | 140.44ms | 160.11ms | 1.140× | 5.26% | PASS |
| ac_reads | `oltp_point_select` | 31.12ms | 34.82ms | 1.119× | 1.26% | PASS |
| ac_reads | `oltp_range_select` | 15.07ms | 16.84ms | 1.117× | 2.50% | PASS |
| ac_reads | `oltp_sum_range` | 13.89ms | 15.69ms | 1.130× | 1.92% | PASS |
| ac_reads | `oltp_order_range` | 2.92ms | 3.12ms | 1.070× | 1.85% | PASS |
| ac_reads | `oltp_distinct_range` | 3.71ms | 3.93ms | 1.059× | 1.35% | PASS |
| ac_reads | `oltp_index_scan` | 4.24ms | 5.12ms | 1.209× | 2.23% | PASS |
| ac_reads | `select_random_points` | 22.48ms | 24.80ms | 1.103× | 1.95% | PASS |
| ac_reads | `select_random_ranges` | 6.18ms | 7.02ms | 1.136× | 1.42% | PASS |
| ac_reads | `covering_index_scan` | 3.91ms | 3.89ms | 0.996× | 1.60% | PASS |
| ac_reads | `groupby_scan` | 28.83ms | 30.51ms | 1.058× | 1.26% | PASS |
| ac_reads | `index_join` | 6.74ms | 8.49ms | 1.259× | 1.29% | PASS |
| ac_reads | `index_join_scan` | 3.08ms | 4.19ms | 1.358× | 3.89% | PASS |
| ac_reads | `types_table_scan` | 873.49ms | 1.03s | 1.182× | 0.64% | PASS |
| ac_reads | `table_scan` | 1.00s | 1.11s | 1.105× | 0.52% | PASS |
| ac_reads | `oltp_read_only` | 122.26ms | 134.51ms | 1.100× | 0.75% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 35.79ms | 102.12ms | 2.853× | 26.14% | PASS |
| ac_writes | `oltp_insert_ac` | 41.78ms | 128.59ms | 3.078× | 49.04% | PASS |
| ac_writes | `oltp_update_index_ac` | 44.52ms | 195.03ms | 4.381× | 41.89% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 42.81ms | 138.16ms | 3.227× | 42.75% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 44.16ms | 146.03ms | 3.307× | 40.57% | PASS |
| ac_writes | `oltp_write_only_ac` | 41.03ms | 146.16ms | 3.562× | 41.07% | PASS |
| ac_writes | `types_delete_insert_ac` | 47.45ms | 182.06ms | 3.837× | 56.33% | PASS |
| ac_writes | `oltp_read_write_ac` | 45.80ms | 138.85ms | 3.032× | 27.28% | PASS |

</details>

## Version-control latency

Wall time: 2m 23s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 83.65ms | 130.00ms | 64.3% | 0.49% | PASS |
| `status_dirty_many_tables` | 86.79ms | 130.00ms | 66.8% | 0.59% | PASS |
| `diff_regular_working_one_table` | 79.48ms | 120.00ms | 66.2% | 0.53% | PASS |
| `diff_regular_working_many_tables` | 92.97ms | 140.00ms | 66.4% | 0.61% | PASS |
| `diff_stat_working_many_tables` | 93.19ms | 140.00ms | 66.6% | 0.59% | PASS |
| `diff_schema_working_many_tables` | 93.30ms | 140.00ms | 66.6% | 0.47% | PASS |
| `branch_list_many_branches` | 23.49ms | 35.00ms | 67.1% | 1.66% | PASS |
| `branch_create_delete` | 26.11ms | 40.00ms | 65.3% | 1.50% | PASS |
| `checkout_branch_clean` | 56.67ms | 150.00ms | 37.8% | 0.99% | PASS |
| `merge_data_no_conflicts` | 30.67ms | 50.00ms | 61.3% | 1.73% | PASS |
| `merge_schema_no_conflicts` | 23.30ms | 35.00ms | 66.6% | 2.35% | PASS |
| `merge_data_conflicts` | 127.93ms | 180.00ms | 71.1% | 0.24% | PASS |
| `merge_data_conflicts_with_resolve` | 128.08ms | 180.00ms | 71.2% | 0.24% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
