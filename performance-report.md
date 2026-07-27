# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-07-26 23:01 UTC
>
> Commit: [`a9b6d9f3e0febfdad89d67247745b10067d3680c`](https://github.com/dolthub/doltlite/commit/a9b6d9f3e0febfdad89d67247745b10067d3680c)
>
> Runner: ubuntu24 20260720.247.2
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/30197777424)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians; MAD is the median absolute deviation and describes run-to-run noise.

## SQL workload summary

| Key shape | Workloads | Samples/workload | Wall time | SQLite median total | DoltLite median total | Ratio | Median paired-ratio MAD | Result |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| int | 69 | 55 | 1h 13m 13s | 9.53s | 11.39s | 1.195× | 1.31% | **PASS** |
| textpk | 69 | 55 | 1h 36m 25s | 11.70s | 12.50s | 1.068× | 1.29% | **PASS** |
| blobpk | 69 | 55 | 1h 16m 2s | 7.60s | 9.19s | 1.210× | 2.58% | **PASS** |
| compositepk | 69 | 55 | 1h 27m 14s | 10.26s | 12.25s | 1.194× | 1.09% | **PASS** |

The absolute ceiling is 2.5× per ordinary workload and 2.0× for a section average. Durable autocommit writes use 10.0× and 5.0× ceilings respectively.

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 25.30ms | 28.37ms | 1.121× | 2.00% | PASS |
| mem_reads | `oltp_range_select` | 11.31ms | 12.87ms | 1.138× | 2.36% | PASS |
| mem_reads | `oltp_sum_range` | 9.91ms | 12.52ms | 1.263× | 2.10% | PASS |
| mem_reads | `oltp_order_range` | 2.73ms | 3.08ms | 1.127× | 1.25% | PASS |
| mem_reads | `oltp_distinct_range` | 3.73ms | 4.23ms | 1.135× | 0.75% | PASS |
| mem_reads | `oltp_index_scan` | 3.99ms | 5.09ms | 1.276× | 1.60% | PASS |
| mem_reads | `select_random_points` | 11.39ms | 11.79ms | 1.035× | 3.23% | PASS |
| mem_reads | `select_random_ranges` | 3.14ms | 4.10ms | 1.304× | 1.09% | PASS |
| mem_reads | `covering_index_scan` | 4.26ms | 4.18ms | 0.981× | 1.18% | PASS |
| mem_reads | `groupby_scan` | 31.84ms | 36.34ms | 1.141× | 0.57% | PASS |
| mem_reads | `index_join` | 5.93ms | 8.05ms | 1.359× | 1.55% | PASS |
| mem_reads | `index_join_scan` | 3.57ms | 4.94ms | 1.383× | 1.73% | PASS |
| mem_reads | `types_table_scan` | 1.12s | 1.31s | 1.169× | 0.65% | PASS |
| mem_reads | `table_scan` | 1.28s | 1.42s | 1.112× | 0.78% | PASS |
| mem_reads | `oltp_read_only` | 104.19ms | 122.43ms | 1.175× | 1.08% | PASS |
| mem_writes | `oltp_bulk_insert` | 182.99ms | 246.24ms | 1.346× | 0.93% | PASS |
| mem_writes | `oltp_insert` | 15.99ms | 29.07ms | 1.817× | 1.01% | PASS |
| mem_writes | `oltp_update_index` | 55.52ms | 94.14ms | 1.695× | 1.35% | PASS |
| mem_writes | `oltp_update_non_index` | 38.98ms | 62.28ms | 1.598× | 1.74% | PASS |
| mem_writes | `oltp_delete_insert` | 49.09ms | 74.64ms | 1.521× | 2.45% | PASS |
| mem_writes | `oltp_write_only` | 23.88ms | 47.74ms | 1.999× | 2.34% | PASS |
| mem_writes | `types_delete_insert` | 26.69ms | 41.35ms | 1.549× | 2.10% | PASS |
| mem_writes | `oltp_read_write` | 73.43ms | 111.98ms | 1.525× | 2.05% | PASS |
| file_reads | `oltp_point_select` | 108.43ms | 57.04ms | 0.526× | 0.87% | PASS |
| file_reads | `oltp_range_select` | 19.89ms | 15.95ms | 0.802× | 1.08% | PASS |
| file_reads | `oltp_sum_range` | 18.75ms | 15.56ms | 0.830× | 1.24% | PASS |
| file_reads | `oltp_order_range` | 3.67ms | 3.41ms | 0.929× | 0.87% | PASS |
| file_reads | `oltp_distinct_range` | 4.66ms | 4.59ms | 0.986× | 1.16% | PASS |
| file_reads | `oltp_index_scan` | 12.66ms | 8.40ms | 0.664× | 1.31% | PASS |
| file_reads | `select_random_points` | 19.98ms | 15.07ms | 0.755× | 2.17% | PASS |
| file_reads | `select_random_ranges` | 11.62ms | 7.03ms | 0.605× | 0.71% | PASS |
| file_reads | `covering_index_scan` | 13.20ms | 7.58ms | 0.574× | 0.96% | PASS |
| file_reads | `groupby_scan` | 33.11ms | 37.00ms | 1.118× | 0.58% | PASS |
| file_reads | `index_join` | 10.82ms | 10.50ms | 0.970× | 1.46% | PASS |
| file_reads | `index_join_scan` | 4.68ms | 5.46ms | 1.167× | 2.64% | PASS |
| file_reads | `types_table_scan` | 1.12s | 1.31s | 1.178× | 0.78% | PASS |
| file_reads | `table_scan` | 1.39s | 1.45s | 1.043× | 0.91% | PASS |
| file_reads | `oltp_read_only` | 229.76ms | 167.76ms | 0.730× | 1.09% | PASS |
| file_writes | `oltp_bulk_insert` | 196.88ms | 268.99ms | 1.366× | 0.96% | PASS |
| file_writes | `oltp_insert` | 22.38ms | 36.51ms | 1.631× | 1.59% | PASS |
| file_writes | `oltp_update_index` | 80.37ms | 120.37ms | 1.498× | 1.44% | PASS |
| file_writes | `oltp_update_non_index` | 60.52ms | 84.48ms | 1.396× | 1.42% | PASS |
| file_writes | `oltp_delete_insert` | 68.48ms | 95.52ms | 1.395× | 1.61% | PASS |
| file_writes | `oltp_write_only` | 43.87ms | 65.32ms | 1.489× | 1.60% | PASS |
| file_writes | `types_delete_insert` | 40.83ms | 53.89ms | 1.320× | 0.96% | PASS |
| file_writes | `oltp_read_write` | 87.25ms | 126.70ms | 1.452× | 1.01% | PASS |
| ac_reads | `oltp_point_select` | 51.69ms | 56.50ms | 1.093× | 0.99% | PASS |
| ac_reads | `oltp_range_select` | 14.37ms | 15.87ms | 1.105× | 1.34% | PASS |
| ac_reads | `oltp_sum_range` | 12.71ms | 15.44ms | 1.214× | 1.03% | PASS |
| ac_reads | `oltp_order_range` | 3.10ms | 3.40ms | 1.098× | 0.99% | PASS |
| ac_reads | `oltp_distinct_range` | 4.09ms | 4.61ms | 1.125× | 0.80% | PASS |
| ac_reads | `oltp_index_scan` | 7.00ms | 8.34ms | 1.193× | 1.55% | PASS |
| ac_reads | `select_random_points` | 14.04ms | 14.96ms | 1.066× | 1.27% | PASS |
| ac_reads | `select_random_ranges` | 5.97ms | 7.03ms | 1.176× | 1.09% | PASS |
| ac_reads | `covering_index_scan` | 7.28ms | 7.44ms | 1.023× | 1.32% | PASS |
| ac_reads | `groupby_scan` | 32.31ms | 36.87ms | 1.141× | 0.92% | PASS |
| ac_reads | `index_join` | 7.68ms | 10.28ms | 1.338× | 1.46% | PASS |
| ac_reads | `index_join_scan` | 3.97ms | 5.38ms | 1.355× | 1.91% | PASS |
| ac_reads | `types_table_scan` | 1.11s | 1.31s | 1.179× | 0.68% | PASS |
| ac_reads | `table_scan` | 1.28s | 1.42s | 1.108× | 1.14% | PASS |
| ac_reads | `oltp_read_only` | 142.08ms | 163.59ms | 1.151× | 1.24% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 16.13ms | 62.01ms | 3.845× | 5.77% | PASS |
| ac_writes | `oltp_insert_ac` | 18.09ms | 79.43ms | 4.390× | 4.56% | PASS |
| ac_writes | `oltp_update_index_ac` | 19.87ms | 94.23ms | 4.742× | 4.83% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 16.54ms | 73.17ms | 4.424× | 5.70% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 17.65ms | 84.81ms | 4.806× | 6.06% | PASS |
| ac_writes | `oltp_write_only_ac` | 18.13ms | 83.85ms | 4.625× | 5.02% | PASS |
| ac_writes | `types_delete_insert_ac` | 15.63ms | 72.42ms | 4.634× | 5.30% | PASS |
| ac_writes | `oltp_read_write_ac` | 23.39ms | 92.61ms | 3.960× | 4.66% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 36.83ms | 42.83ms | 1.163× | 1.39% | PASS |
| mem_reads | `oltp_range_select` | 18.01ms | 17.33ms | 0.963× | 1.62% | PASS |
| mem_reads | `oltp_sum_range` | 15.35ms | 16.89ms | 1.100× | 1.30% | PASS |
| mem_reads | `oltp_order_range` | 3.80ms | 3.88ms | 1.020× | 1.02% | PASS |
| mem_reads | `oltp_distinct_range` | 4.88ms | 5.20ms | 1.067× | 0.89% | PASS |
| mem_reads | `oltp_index_scan` | 4.81ms | 6.51ms | 1.355× | 1.76% | PASS |
| mem_reads | `select_random_points` | 19.93ms | 22.26ms | 1.117× | 1.02% | PASS |
| mem_reads | `select_random_ranges` | 4.46ms | 5.58ms | 1.251× | 1.25% | PASS |
| mem_reads | `covering_index_scan` | 5.68ms | 5.02ms | 0.884× | 4.96% | PASS |
| mem_reads | `groupby_scan` | 35.54ms | 37.61ms | 1.058× | 0.66% | PASS |
| mem_reads | `index_join` | 7.53ms | 10.30ms | 1.368× | 1.27% | PASS |
| mem_reads | `index_join_scan` | 4.91ms | 6.06ms | 1.236× | 1.02% | PASS |
| mem_reads | `types_table_scan` | 1.40s | 1.39s | 0.991× | 0.74% | PASS |
| mem_reads | `table_scan` | 1.74s | 1.50s | 0.865× | 0.66% | PASS |
| mem_reads | `oltp_read_only` | 133.14ms | 142.42ms | 1.070× | 1.29% | PASS |
| mem_writes | `oltp_bulk_insert` | 230.40ms | 348.67ms | 1.513× | 0.90% | PASS |
| mem_writes | `oltp_insert` | 23.35ms | 41.29ms | 1.768× | 1.31% | PASS |
| mem_writes | `oltp_update_index` | 79.34ms | 148.70ms | 1.874× | 1.48% | PASS |
| mem_writes | `oltp_update_non_index` | 53.35ms | 93.25ms | 1.748× | 0.81% | PASS |
| mem_writes | `oltp_delete_insert` | 56.86ms | 114.55ms | 2.015× | 1.37% | PASS |
| mem_writes | `oltp_write_only` | 32.84ms | 68.50ms | 2.086× | 1.22% | PASS |
| mem_writes | `types_delete_insert` | 35.23ms | 58.03ms | 1.647× | 0.81% | PASS |
| mem_writes | `oltp_read_write` | 94.53ms | 145.94ms | 1.544× | 1.41% | PASS |
| file_reads | `oltp_point_select` | 114.89ms | 65.90ms | 0.574× | 1.20% | PASS |
| file_reads | `oltp_range_select` | 24.50ms | 17.83ms | 0.728× | 1.03% | PASS |
| file_reads | `oltp_sum_range` | 22.24ms | 17.64ms | 0.793× | 0.87% | PASS |
| file_reads | `oltp_order_range` | 4.26ms | 3.70ms | 0.870× | 0.96% | PASS |
| file_reads | `oltp_distinct_range` | 5.25ms | 4.86ms | 0.926× | 1.00% | PASS |
| file_reads | `oltp_index_scan` | 13.66ms | 9.50ms | 0.696× | 0.84% | PASS |
| file_reads | `select_random_points` | 28.16ms | 24.86ms | 0.883× | 1.39% | PASS |
| file_reads | `select_random_ranges` | 12.78ms | 8.43ms | 0.659× | 0.63% | PASS |
| file_reads | `covering_index_scan` | 14.97ms | 8.00ms | 0.534× | 0.81% | PASS |
| file_reads | `groupby_scan` | 36.31ms | 37.76ms | 1.040× | 0.58% | PASS |
| file_reads | `index_join` | 12.53ms | 11.84ms | 0.945× | 1.13% | PASS |
| file_reads | `index_join_scan` | 5.89ms | 6.51ms | 1.106× | 1.18% | PASS |
| file_reads | `types_table_scan` | 1.40s | 1.38s | 0.984× | 1.02% | PASS |
| file_reads | `table_scan` | 1.39s | 1.43s | 1.027× | 2.67% | PASS |
| file_reads | `oltp_read_only` | 238.29ms | 176.91ms | 0.742× | 0.70% | PASS |
| file_writes | `oltp_bulk_insert` | 250.28ms | 372.61ms | 1.489× | 0.95% | PASS |
| file_writes | `oltp_insert` | 48.17ms | 52.77ms | 1.096× | 19.14% | PASS |
| file_writes | `oltp_update_index` | 116.99ms | 178.39ms | 1.525× | 1.49% | PASS |
| file_writes | `oltp_update_non_index` | 97.52ms | 117.89ms | 1.209× | 10.31% | PASS |
| file_writes | `oltp_delete_insert` | 94.19ms | 142.22ms | 1.510× | 1.83% | PASS |
| file_writes | `oltp_write_only` | 91.77ms | 90.77ms | 0.989× | 9.42% | PASS |
| file_writes | `types_delete_insert` | 56.94ms | 77.16ms | 1.355× | 1.75% | PASS |
| file_writes | `oltp_read_write` | 138.21ms | 166.18ms | 1.202× | 5.16% | PASS |
| ac_reads | `oltp_point_select` | 58.89ms | 65.82ms | 1.118× | 1.17% | PASS |
| ac_reads | `oltp_range_select` | 18.43ms | 17.79ms | 0.965× | 1.58% | PASS |
| ac_reads | `oltp_sum_range` | 16.48ms | 17.60ms | 1.068× | 1.94% | PASS |
| ac_reads | `oltp_order_range` | 3.78ms | 3.70ms | 0.979× | 1.16% | PASS |
| ac_reads | `oltp_distinct_range` | 4.66ms | 4.87ms | 1.044× | 1.16% | PASS |
| ac_reads | `oltp_index_scan` | 8.05ms | 9.39ms | 1.167× | 0.80% | PASS |
| ac_reads | `select_random_points` | 22.58ms | 24.56ms | 1.088× | 1.86% | PASS |
| ac_reads | `select_random_ranges` | 7.28ms | 8.38ms | 1.151× | 0.89% | PASS |
| ac_reads | `covering_index_scan` | 9.07ms | 7.88ms | 0.869× | 1.01% | PASS |
| ac_reads | `groupby_scan` | 35.33ms | 37.58ms | 1.064× | 0.63% | PASS |
| ac_reads | `index_join` | 9.68ms | 11.74ms | 1.213× | 1.52% | PASS |
| ac_reads | `index_join_scan` | 5.50ms | 6.60ms | 1.200× | 1.52% | PASS |
| ac_reads | `types_table_scan` | 1.24s | 1.33s | 1.074× | 2.86% | PASS |
| ac_reads | `table_scan` | 1.69s | 1.50s | 0.886× | 2.25% | PASS |
| ac_reads | `oltp_read_only` | 170.36ms | 181.51ms | 1.065× | 2.06% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 15.72ms | 61.75ms | 3.927× | 2.79% | PASS |
| ac_writes | `oltp_insert_ac` | 18.72ms | 77.60ms | 4.146× | 3.49% | PASS |
| ac_writes | `oltp_update_index_ac` | 20.42ms | 96.14ms | 4.707× | 2.34% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 15.76ms | 73.16ms | 4.642× | 3.47% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 17.84ms | 86.10ms | 4.826× | 2.14% | PASS |
| ac_writes | `oltp_write_only_ac` | 19.18ms | 85.11ms | 4.438× | 4.76% | PASS |
| ac_writes | `types_delete_insert_ac` | 15.42ms | 74.32ms | 4.819× | 3.93% | PASS |
| ac_writes | `oltp_read_write_ac` | 24.99ms | 94.13ms | 3.767× | 2.34% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 18.59ms | 21.87ms | 1.177× | 1.95% | PASS |
| mem_reads | `oltp_range_select` | 9.23ms | 9.29ms | 1.007× | 5.43% | PASS |
| mem_reads | `oltp_sum_range` | 9.59ms | 9.07ms | 0.946× | 3.10% | PASS |
| mem_reads | `oltp_order_range` | 2.20ms | 2.19ms | 0.997× | 1.78% | PASS |
| mem_reads | `oltp_distinct_range` | 2.78ms | 2.85ms | 1.026× | 1.43% | PASS |
| mem_reads | `oltp_index_scan` | 3.14ms | 4.06ms | 1.292× | 4.13% | PASS |
| mem_reads | `select_random_points` | 13.46ms | 15.42ms | 1.145× | 1.96% | PASS |
| mem_reads | `select_random_ranges` | 2.76ms | 3.33ms | 1.207× | 2.35% | PASS |
| mem_reads | `covering_index_scan` | 2.65ms | 2.64ms | 0.999× | 1.96% | PASS |
| mem_reads | `groupby_scan` | 20.40ms | 22.57ms | 1.107× | 1.01% | PASS |
| mem_reads | `index_join` | 5.02ms | 6.40ms | 1.276× | 1.31% | PASS |
| mem_reads | `index_join_scan` | 2.55ms | 4.13ms | 1.619× | 4.64% | PASS |
| mem_reads | `types_table_scan` | 759.08ms | 805.79ms | 1.062× | 0.73% | PASS |
| mem_reads | `table_scan` | 899.36ms | 903.65ms | 1.005× | 1.37% | PASS |
| mem_reads | `oltp_read_only` | 73.00ms | 82.59ms | 1.131× | 1.05% | PASS |
| mem_writes | `oltp_bulk_insert` | 131.72ms | 187.92ms | 1.427× | 0.56% | PASS |
| mem_writes | `oltp_insert` | 12.19ms | 22.78ms | 1.869× | 1.36% | PASS |
| mem_writes | `oltp_update_index` | 43.77ms | 81.90ms | 1.871× | 1.98% | PASS |
| mem_writes | `oltp_update_non_index` | 29.94ms | 51.56ms | 1.722× | 1.56% | PASS |
| mem_writes | `oltp_delete_insert` | 31.48ms | 62.89ms | 1.998× | 1.87% | PASS |
| mem_writes | `oltp_write_only` | 17.64ms | 37.43ms | 2.122× | 3.18% | PASS |
| mem_writes | `types_delete_insert` | 20.01ms | 31.64ms | 1.581× | 1.76% | PASS |
| mem_writes | `oltp_read_write` | 50.68ms | 83.26ms | 1.643× | 1.99% | PASS |
| file_reads | `oltp_point_select` | 39.13ms | 29.16ms | 0.745× | 1.32% | PASS |
| file_reads | `oltp_range_select` | 11.78ms | 10.49ms | 0.890× | 2.58% | PASS |
| file_reads | `oltp_sum_range` | 11.89ms | 10.22ms | 0.860× | 2.74% | PASS |
| file_reads | `oltp_order_range` | 2.30ms | 2.28ms | 0.990× | 2.85% | PASS |
| file_reads | `oltp_distinct_range` | 3.06ms | 2.99ms | 0.975× | 1.82% | PASS |
| file_reads | `oltp_index_scan` | 5.38ms | 5.16ms | 0.961× | 3.60% | PASS |
| file_reads | `select_random_points` | 15.68ms | 16.59ms | 1.058× | 2.40% | PASS |
| file_reads | `select_random_ranges` | 4.96ms | 4.27ms | 0.860× | 1.55% | PASS |
| file_reads | `covering_index_scan` | 5.09ms | 3.95ms | 0.775× | 3.27% | PASS |
| file_reads | `groupby_scan` | 20.50ms | 22.84ms | 1.114× | 0.89% | PASS |
| file_reads | `index_join` | 6.58ms | 7.91ms | 1.203× | 3.45% | PASS |
| file_reads | `index_join_scan` | 3.53ms | 4.98ms | 1.411× | 4.09% | PASS |
| file_reads | `types_table_scan` | 761.73ms | 810.74ms | 1.064× | 1.16% | PASS |
| file_reads | `table_scan` | 899.43ms | 911.58ms | 1.014× | 2.79% | PASS |
| file_reads | `oltp_read_only` | 106.08ms | 95.89ms | 0.904× | 1.26% | PASS |
| file_writes | `oltp_bulk_insert` | 250.09ms | 269.92ms | 1.079× | 11.76% | PASS |
| file_writes | `oltp_insert` | 39.01ms | 55.34ms | 1.419× | 35.14% | PASS |
| file_writes | `oltp_update_index` | 195.20ms | 177.81ms | 0.911× | 14.75% | PASS |
| file_writes | `oltp_update_non_index` | 163.47ms | 118.85ms | 0.727× | 35.10% | PASS |
| file_writes | `oltp_delete_insert` | 149.59ms | 136.84ms | 0.915× | 23.35% | PASS |
| file_writes | `oltp_write_only` | 128.73ms | 95.94ms | 0.745× | 41.19% | PASS |
| file_writes | `types_delete_insert` | 81.02ms | 80.23ms | 0.990× | 18.95% | PASS |
| file_writes | `oltp_read_write` | 131.41ms | 138.30ms | 1.052× | 22.16% | PASS |
| ac_reads | `oltp_point_select` | 25.90ms | 30.20ms | 1.166× | 0.72% | PASS |
| ac_reads | `oltp_range_select` | 11.38ms | 10.75ms | 0.945× | 1.63% | PASS |
| ac_reads | `oltp_sum_range` | 11.27ms | 10.32ms | 0.916× | 1.84% | PASS |
| ac_reads | `oltp_order_range` | 2.35ms | 2.36ms | 1.002× | 2.38% | PASS |
| ac_reads | `oltp_distinct_range` | 2.98ms | 3.01ms | 1.009× | 2.53% | PASS |
| ac_reads | `oltp_index_scan` | 3.86ms | 4.70ms | 1.220× | 3.46% | PASS |
| ac_reads | `select_random_points` | 14.39ms | 16.02ms | 1.113× | 1.66% | PASS |
| ac_reads | `select_random_ranges` | 3.34ms | 3.93ms | 1.177× | 3.57% | PASS |
| ac_reads | `covering_index_scan` | 3.48ms | 3.54ms | 1.020× | 3.51% | PASS |
| ac_reads | `groupby_scan` | 20.20ms | 22.65ms | 1.121× | 1.04% | PASS |
| ac_reads | `index_join` | 5.71ms | 7.25ms | 1.272× | 3.55% | PASS |
| ac_reads | `index_join_scan` | 3.47ms | 4.88ms | 1.405× | 5.29% | PASS |
| ac_reads | `types_table_scan` | 760.96ms | 809.03ms | 1.063× | 1.18% | PASS |
| ac_reads | `table_scan` | 900.89ms | 909.75ms | 1.010× | 4.65% | PASS |
| ac_reads | `oltp_read_only` | 83.61ms | 93.68ms | 1.120× | 0.81% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 59.00ms | 166.36ms | 2.820× | 47.71% | PASS |
| ac_writes | `oltp_insert_ac` | 46.86ms | 140.66ms | 3.002× | 41.79% | PASS |
| ac_writes | `oltp_update_index_ac` | 67.58ms | 209.08ms | 3.094× | 52.20% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 60.49ms | 178.48ms | 2.951× | 43.86% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 56.89ms | 221.45ms | 3.893× | 64.67% | PASS |
| ac_writes | `oltp_write_only_ac` | 60.04ms | 227.99ms | 3.797× | 37.07% | PASS |
| ac_writes | `types_delete_insert_ac` | 129.61ms | 399.38ms | 3.081× | 74.88% | PASS |
| ac_writes | `oltp_read_write_ac` | 65.36ms | 251.84ms | 3.853× | 51.19% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.02ms | 38.06ms | 1.153× | 1.16% | PASS |
| mem_reads | `oltp_range_select` | 19.34ms | 22.36ms | 1.156× | 1.66% | PASS |
| mem_reads | `oltp_sum_range` | 16.93ms | 21.81ms | 1.288× | 1.44% | PASS |
| mem_reads | `oltp_order_range` | 3.63ms | 4.08ms | 1.123× | 1.09% | PASS |
| mem_reads | `oltp_distinct_range` | 4.63ms | 5.29ms | 1.142× | 0.83% | PASS |
| mem_reads | `oltp_index_scan` | 4.62ms | 5.92ms | 1.280× | 1.22% | PASS |
| mem_reads | `select_random_points` | 27.30ms | 30.64ms | 1.122× | 1.32% | PASS |
| mem_reads | `select_random_ranges` | 7.31ms | 8.53ms | 1.167× | 0.85% | PASS |
| mem_reads | `covering_index_scan` | 4.30ms | 4.21ms | 0.980× | 1.42% | PASS |
| mem_reads | `groupby_scan` | 37.95ms | 43.13ms | 1.137× | 0.48% | PASS |
| mem_reads | `index_join` | 7.88ms | 10.23ms | 1.298× | 0.88% | PASS |
| mem_reads | `index_join_scan` | 4.09ms | 5.72ms | 1.397× | 0.98% | PASS |
| mem_reads | `types_table_scan` | 1.15s | 1.31s | 1.144× | 0.76% | PASS |
| mem_reads | `table_scan` | 1.33s | 1.43s | 1.071× | 0.71% | PASS |
| mem_reads | `oltp_read_only` | 145.48ms | 172.07ms | 1.183× | 0.69% | PASS |
| mem_writes | `oltp_bulk_insert` | 244.31ms | 345.80ms | 1.415× | 0.82% | PASS |
| mem_writes | `oltp_insert` | 19.32ms | 36.77ms | 1.903× | 0.87% | PASS |
| mem_writes | `oltp_update_index` | 68.42ms | 120.07ms | 1.755× | 1.02% | PASS |
| mem_writes | `oltp_update_non_index` | 51.66ms | 84.02ms | 1.626× | 0.97% | PASS |
| mem_writes | `oltp_delete_insert` | 50.30ms | 96.74ms | 1.923× | 0.94% | PASS |
| mem_writes | `oltp_write_only` | 27.98ms | 60.59ms | 2.165× | 1.00% | PASS |
| mem_writes | `types_delete_insert` | 32.98ms | 54.37ms | 1.648× | 1.07% | PASS |
| mem_writes | `oltp_read_write` | 98.89ms | 155.65ms | 1.574× | 1.74% | PASS |
| file_reads | `oltp_point_select` | 116.01ms | 66.61ms | 0.574× | 0.99% | PASS |
| file_reads | `oltp_range_select` | 28.03ms | 25.71ms | 0.917× | 2.48% | PASS |
| file_reads | `oltp_sum_range` | 25.79ms | 24.99ms | 0.969× | 1.47% | PASS |
| file_reads | `oltp_order_range` | 4.56ms | 4.43ms | 0.972× | 1.47% | PASS |
| file_reads | `oltp_distinct_range` | 5.57ms | 5.66ms | 1.016× | 1.01% | PASS |
| file_reads | `oltp_index_scan` | 13.09ms | 9.27ms | 0.708× | 1.52% | PASS |
| file_reads | `select_random_points` | 36.68ms | 34.19ms | 0.932× | 0.74% | PASS |
| file_reads | `select_random_ranges` | 15.83ms | 11.61ms | 0.733× | 0.74% | PASS |
| file_reads | `covering_index_scan` | 12.86ms | 7.64ms | 0.594× | 0.85% | PASS |
| file_reads | `groupby_scan` | 39.12ms | 43.88ms | 1.122× | 0.56% | PASS |
| file_reads | `index_join` | 12.57ms | 12.80ms | 1.018× | 1.22% | PASS |
| file_reads | `index_join_scan` | 5.09ms | 6.23ms | 1.224× | 1.97% | PASS |
| file_reads | `types_table_scan` | 1.19s | 1.34s | 1.124× | 1.38% | PASS |
| file_reads | `table_scan` | 1.34s | 1.43s | 1.067× | 1.83% | PASS |
| file_reads | `oltp_read_only` | 265.63ms | 215.28ms | 0.810× | 0.91% | PASS |
| file_writes | `oltp_bulk_insert` | 258.73ms | 367.15ms | 1.419× | 1.09% | PASS |
| file_writes | `oltp_insert` | 25.60ms | 46.67ms | 1.823× | 0.98% | PASS |
| file_writes | `oltp_update_index` | 96.63ms | 146.52ms | 1.516× | 1.23% | PASS |
| file_writes | `oltp_update_non_index` | 75.72ms | 105.55ms | 1.394× | 1.28% | PASS |
| file_writes | `oltp_delete_insert` | 74.72ms | 120.29ms | 1.610× | 1.54% | PASS |
| file_writes | `oltp_write_only` | 48.54ms | 80.02ms | 1.649× | 1.03% | PASS |
| file_writes | `types_delete_insert` | 48.51ms | 67.32ms | 1.388× | 1.10% | PASS |
| file_writes | `oltp_read_write` | 117.60ms | 172.86ms | 1.470× | 1.21% | PASS |
| ac_reads | `oltp_point_select` | 60.08ms | 66.83ms | 1.112× | 0.96% | PASS |
| ac_reads | `oltp_range_select` | 22.48ms | 25.68ms | 1.143× | 1.38% | PASS |
| ac_reads | `oltp_sum_range` | 20.12ms | 25.11ms | 1.248× | 1.07% | PASS |
| ac_reads | `oltp_order_range` | 4.08ms | 4.43ms | 1.084× | 1.04% | PASS |
| ac_reads | `oltp_distinct_range` | 5.10ms | 5.66ms | 1.110× | 1.10% | PASS |
| ac_reads | `oltp_index_scan` | 7.83ms | 9.27ms | 1.184× | 0.80% | PASS |
| ac_reads | `select_random_points` | 31.23ms | 34.37ms | 1.101× | 1.13% | PASS |
| ac_reads | `select_random_ranges` | 10.34ms | 11.61ms | 1.122× | 0.89% | PASS |
| ac_reads | `covering_index_scan` | 7.37ms | 7.55ms | 1.024× | 1.36% | PASS |
| ac_reads | `groupby_scan` | 38.25ms | 43.87ms | 1.147× | 0.37% | PASS |
| ac_reads | `index_join` | 9.83ms | 12.74ms | 1.296× | 1.03% | PASS |
| ac_reads | `index_join_scan` | 4.63ms | 6.31ms | 1.363× | 1.50% | PASS |
| ac_reads | `types_table_scan` | 1.13s | 1.30s | 1.153× | 0.58% | PASS |
| ac_reads | `table_scan` | 1.33s | 1.43s | 1.070× | 1.00% | PASS |
| ac_reads | `oltp_read_only` | 187.10ms | 215.46ms | 1.152× | 0.60% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 15.68ms | 60.64ms | 3.868× | 3.35% | PASS |
| ac_writes | `oltp_insert_ac` | 17.60ms | 82.70ms | 4.700× | 3.38% | PASS |
| ac_writes | `oltp_update_index_ac` | 19.63ms | 94.28ms | 4.802× | 4.09% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 16.50ms | 73.27ms | 4.440× | 4.46% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 17.41ms | 85.35ms | 4.903× | 4.04% | PASS |
| ac_writes | `oltp_write_only_ac` | 17.81ms | 86.00ms | 4.830× | 3.64% | PASS |
| ac_writes | `types_delete_insert_ac` | 15.39ms | 72.95ms | 4.741× | 6.03% | PASS |
| ac_writes | `oltp_read_write_ac` | 24.57ms | 93.42ms | 3.803× | 3.71% | PASS |

</details>

## Version-control latency

Wall time: 2m 16s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 80.76ms | 200.00ms | 40.4% | 0.41% | PASS |
| `status_dirty_many_tables` | 83.74ms | 200.00ms | 41.9% | 0.29% | PASS |
| `diff_regular_working_one_table` | 76.35ms | 150.00ms | 50.9% | 0.28% | PASS |
| `diff_regular_working_many_tables` | 88.43ms | 200.00ms | 44.2% | 0.33% | PASS |
| `diff_stat_working_many_tables` | 88.47ms | 200.00ms | 44.2% | 0.33% | PASS |
| `diff_schema_working_many_tables` | 88.91ms | 200.00ms | 44.5% | 0.29% | PASS |
| `branch_list_many_branches` | 21.64ms | 100.00ms | 21.6% | 0.38% | PASS |
| `branch_create_delete` | 23.98ms | 100.00ms | 24.0% | 0.70% | PASS |
| `checkout_branch_clean` | 53.81ms | 200.00ms | 26.9% | 0.53% | PASS |
| `merge_data_no_conflicts` | 27.92ms | 150.00ms | 18.6% | 0.64% | PASS |
| `merge_schema_no_conflicts` | 21.20ms | 100.00ms | 21.2% | 0.69% | PASS |
| `merge_data_conflicts` | 125.56ms | 250.00ms | 50.2% | 0.19% | PASS |
| `merge_data_conflicts_with_resolve` | 125.49ms | 250.00ms | 50.2% | 0.22% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
