# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-08-01 11:44 UTC
>
> Commit: [`3965061b6cacdde3365838d5df630b30e34622b7`](https://github.com/dolthub/doltlite/commit/3965061b6cacdde3365838d5df630b30e34622b7)
>
> Runner: ubuntu24 20260720.247.2
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/30695138415)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians; MAD is the median absolute deviation and describes run-to-run noise.

## SQL workload summary

| Key shape | Workloads | Samples/workload | Wall time | SQLite median total | DoltLite median total | Ratio | Median paired-ratio MAD | Result |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| int | 69 | 55 | 1h 12m 56s | 9.41s | 11.39s | 1.210× | 1.69% | **PASS** |
| textpk | 69 | 55 | 1h 17m 15s | 8.52s | 10.27s | 1.205× | 1.17% | **PASS** |
| blobpk | 69 | 55 | 1h 32m 41s | 10.43s | 11.82s | 1.132× | 2.28% | **PASS** |
| compositepk | 69 | 55 | 1h 27m 7s | 10.11s | 11.90s | 1.177× | 1.68% | **PASS** |

The absolute ceiling is 2.5× per ordinary workload and 2.0× for a section average. Durable autocommit writes use 10.0× and 5.0× ceilings respectively.

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.91ms | 28.27ms | 1.135× | 2.45% | PASS |
| mem_reads | `oltp_range_select` | 10.73ms | 12.94ms | 1.206× | 1.63% | PASS |
| mem_reads | `oltp_sum_range` | 9.38ms | 12.48ms | 1.331× | 1.83% | PASS |
| mem_reads | `oltp_order_range` | 2.68ms | 3.07ms | 1.146× | 1.07% | PASS |
| mem_reads | `oltp_distinct_range` | 3.66ms | 4.22ms | 1.154× | 0.97% | PASS |
| mem_reads | `oltp_index_scan` | 3.94ms | 5.11ms | 1.296× | 1.62% | PASS |
| mem_reads | `select_random_points` | 11.00ms | 11.70ms | 1.064× | 4.20% | PASS |
| mem_reads | `select_random_ranges` | 3.13ms | 4.13ms | 1.317× | 1.98% | PASS |
| mem_reads | `covering_index_scan` | 4.29ms | 4.21ms | 0.983× | 1.65% | PASS |
| mem_reads | `groupby_scan` | 31.80ms | 36.42ms | 1.145× | 0.48% | PASS |
| mem_reads | `index_join` | 5.89ms | 8.04ms | 1.364× | 1.33% | PASS |
| mem_reads | `index_join_scan` | 3.55ms | 5.01ms | 1.412× | 3.63% | PASS |
| mem_reads | `types_table_scan` | 1.11s | 1.31s | 1.179× | 0.66% | PASS |
| mem_reads | `table_scan` | 1.29s | 1.44s | 1.116× | 1.14% | PASS |
| mem_reads | `oltp_read_only` | 104.66ms | 123.32ms | 1.178× | 1.90% | PASS |
| mem_writes | `oltp_bulk_insert` | 183.01ms | 243.77ms | 1.332× | 0.59% | PASS |
| mem_writes | `oltp_insert` | 15.85ms | 28.51ms | 1.798× | 0.98% | PASS |
| mem_writes | `oltp_update_index` | 51.87ms | 91.50ms | 1.764× | 1.67% | PASS |
| mem_writes | `oltp_update_non_index` | 36.14ms | 59.81ms | 1.655× | 2.20% | PASS |
| mem_writes | `oltp_delete_insert` | 46.44ms | 71.50ms | 1.540× | 1.99% | PASS |
| mem_writes | `oltp_write_only` | 22.98ms | 45.82ms | 1.994× | 1.74% | PASS |
| mem_writes | `types_delete_insert` | 25.72ms | 40.16ms | 1.561× | 1.96% | PASS |
| mem_writes | `oltp_read_write` | 66.29ms | 106.55ms | 1.607× | 1.70% | PASS |
| file_reads | `oltp_point_select` | 107.50ms | 56.30ms | 0.524× | 0.75% | PASS |
| file_reads | `oltp_range_select` | 19.67ms | 15.75ms | 0.801× | 1.45% | PASS |
| file_reads | `oltp_sum_range` | 18.33ms | 15.42ms | 0.841× | 1.47% | PASS |
| file_reads | `oltp_order_range` | 3.61ms | 3.38ms | 0.937× | 1.16% | PASS |
| file_reads | `oltp_distinct_range` | 4.62ms | 4.56ms | 0.986× | 0.78% | PASS |
| file_reads | `oltp_index_scan` | 12.63ms | 8.18ms | 0.648× | 1.69% | PASS |
| file_reads | `select_random_points` | 19.76ms | 14.60ms | 0.739× | 2.12% | PASS |
| file_reads | `select_random_ranges` | 11.46ms | 6.96ms | 0.608× | 1.13% | PASS |
| file_reads | `covering_index_scan` | 13.02ms | 7.30ms | 0.561× | 1.15% | PASS |
| file_reads | `groupby_scan` | 32.76ms | 36.80ms | 1.124× | 0.62% | PASS |
| file_reads | `index_join` | 10.79ms | 10.24ms | 0.948× | 1.47% | PASS |
| file_reads | `index_join_scan` | 4.59ms | 5.38ms | 1.173× | 2.71% | PASS |
| file_reads | `types_table_scan` | 1.10s | 1.31s | 1.186× | 0.62% | PASS |
| file_reads | `table_scan` | 1.28s | 1.43s | 1.118× | 1.02% | PASS |
| file_reads | `oltp_read_only` | 225.81ms | 164.91ms | 0.730× | 1.31% | PASS |
| file_writes | `oltp_bulk_insert` | 198.12ms | 267.83ms | 1.352× | 1.05% | PASS |
| file_writes | `oltp_insert` | 22.39ms | 36.17ms | 1.616× | 1.75% | PASS |
| file_writes | `oltp_update_index` | 79.71ms | 117.99ms | 1.480× | 1.77% | PASS |
| file_writes | `oltp_update_non_index` | 60.64ms | 83.13ms | 1.371× | 1.86% | PASS |
| file_writes | `oltp_delete_insert` | 69.46ms | 95.47ms | 1.374× | 2.01% | PASS |
| file_writes | `oltp_write_only` | 44.53ms | 65.88ms | 1.480× | 1.96% | PASS |
| file_writes | `types_delete_insert` | 41.10ms | 53.59ms | 1.304× | 1.46% | PASS |
| file_writes | `oltp_read_write` | 89.21ms | 126.28ms | 1.416× | 1.94% | PASS |
| ac_reads | `oltp_point_select` | 52.46ms | 56.59ms | 1.079× | 1.17% | PASS |
| ac_reads | `oltp_range_select` | 14.79ms | 15.91ms | 1.076× | 2.00% | PASS |
| ac_reads | `oltp_sum_range` | 13.14ms | 15.57ms | 1.186× | 1.98% | PASS |
| ac_reads | `oltp_order_range` | 3.12ms | 3.38ms | 1.085× | 1.12% | PASS |
| ac_reads | `oltp_distinct_range` | 4.06ms | 4.59ms | 1.132× | 0.86% | PASS |
| ac_reads | `oltp_index_scan` | 7.13ms | 8.34ms | 1.168× | 2.10% | PASS |
| ac_reads | `select_random_points` | 14.63ms | 14.86ms | 1.016× | 1.99% | PASS |
| ac_reads | `select_random_ranges` | 6.04ms | 6.98ms | 1.155× | 1.26% | PASS |
| ac_reads | `covering_index_scan` | 7.49ms | 7.41ms | 0.989× | 2.26% | PASS |
| ac_reads | `groupby_scan` | 32.37ms | 36.97ms | 1.142× | 0.68% | PASS |
| ac_reads | `index_join` | 7.91ms | 10.32ms | 1.305× | 2.21% | PASS |
| ac_reads | `index_join_scan` | 3.94ms | 5.37ms | 1.362× | 2.96% | PASS |
| ac_reads | `types_table_scan` | 1.11s | 1.32s | 1.181× | 0.78% | PASS |
| ac_reads | `table_scan` | 1.30s | 1.44s | 1.106× | 1.12% | PASS |
| ac_reads | `oltp_read_only` | 145.13ms | 164.38ms | 1.133× | 1.36% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 15.54ms | 63.96ms | 4.115× | 4.37% | PASS |
| ac_writes | `oltp_insert_ac` | 17.75ms | 81.74ms | 4.605× | 4.13% | PASS |
| ac_writes | `oltp_update_index_ac` | 19.23ms | 96.39ms | 5.012× | 2.68% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 15.45ms | 73.84ms | 4.778× | 4.19% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 17.65ms | 86.42ms | 4.897× | 5.16% | PASS |
| ac_writes | `oltp_write_only_ac` | 17.64ms | 84.05ms | 4.764× | 5.23% | PASS |
| ac_writes | `types_delete_insert_ac` | 14.88ms | 73.85ms | 4.964× | 4.79% | PASS |
| ac_writes | `oltp_read_write_ac` | 23.00ms | 92.97ms | 4.042× | 3.22% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 25.33ms | 30.90ms | 1.220× | 1.01% | PASS |
| mem_reads | `oltp_range_select` | 11.18ms | 12.68ms | 1.134× | 0.98% | PASS |
| mem_reads | `oltp_sum_range` | 10.89ms | 12.15ms | 1.116× | 1.01% | PASS |
| mem_reads | `oltp_order_range` | 2.59ms | 2.82ms | 1.089× | 0.86% | PASS |
| mem_reads | `oltp_distinct_range` | 3.32ms | 3.70ms | 1.114× | 0.74% | PASS |
| mem_reads | `oltp_index_scan` | 3.81ms | 5.22ms | 1.368× | 1.45% | PASS |
| mem_reads | `select_random_points` | 15.61ms | 19.65ms | 1.259× | 0.97% | PASS |
| mem_reads | `select_random_ranges` | 3.32ms | 4.43ms | 1.337× | 1.15% | PASS |
| mem_reads | `covering_index_scan` | 3.95ms | 3.76ms | 0.952× | 1.49% | PASS |
| mem_reads | `groupby_scan` | 28.18ms | 30.56ms | 1.084× | 0.35% | PASS |
| mem_reads | `index_join` | 5.97ms | 8.07ms | 1.352× | 1.21% | PASS |
| mem_reads | `index_join_scan` | 3.75ms | 4.95ms | 1.323× | 0.92% | PASS |
| mem_reads | `types_table_scan` | 967.45ms | 1.12s | 1.154× | 0.45% | PASS |
| mem_reads | `table_scan` | 1.19s | 1.25s | 1.051× | 3.12% | PASS |
| mem_reads | `oltp_read_only` | 102.19ms | 118.36ms | 1.158× | 1.10% | PASS |
| mem_writes | `oltp_bulk_insert` | 198.45ms | 291.32ms | 1.468× | 0.73% | PASS |
| mem_writes | `oltp_insert` | 18.54ms | 32.85ms | 1.772× | 0.65% | PASS |
| mem_writes | `oltp_update_index` | 60.42ms | 114.18ms | 1.890× | 1.05% | PASS |
| mem_writes | `oltp_update_non_index` | 41.19ms | 71.63ms | 1.739× | 1.05% | PASS |
| mem_writes | `oltp_delete_insert` | 43.09ms | 86.13ms | 1.999× | 0.94% | PASS |
| mem_writes | `oltp_write_only` | 23.65ms | 50.49ms | 2.135× | 0.90% | PASS |
| mem_writes | `types_delete_insert` | 27.21ms | 45.46ms | 1.671× | 1.18% | PASS |
| mem_writes | `oltp_read_write` | 71.13ms | 116.83ms | 1.643× | 1.23% | PASS |
| file_reads | `oltp_point_select` | 54.33ms | 42.02ms | 0.773× | 1.20% | PASS |
| file_reads | `oltp_range_select` | 14.14ms | 13.99ms | 0.990× | 0.88% | PASS |
| file_reads | `oltp_sum_range` | 13.83ms | 13.60ms | 0.983× | 1.03% | PASS |
| file_reads | `oltp_order_range` | 2.91ms | 2.99ms | 1.028× | 1.50% | PASS |
| file_reads | `oltp_distinct_range` | 3.69ms | 3.89ms | 1.054× | 0.98% | PASS |
| file_reads | `oltp_index_scan` | 6.84ms | 6.63ms | 0.969× | 0.77% | PASS |
| file_reads | `select_random_points` | 18.97ms | 21.35ms | 1.125× | 1.46% | PASS |
| file_reads | `select_random_ranges` | 6.24ms | 5.64ms | 0.904× | 0.91% | PASS |
| file_reads | `covering_index_scan` | 7.01ms | 5.13ms | 0.731× | 1.36% | PASS |
| file_reads | `groupby_scan` | 28.52ms | 30.87ms | 1.083× | 0.68% | PASS |
| file_reads | `index_join` | 7.83ms | 9.34ms | 1.193× | 1.39% | PASS |
| file_reads | `index_join_scan` | 4.25ms | 5.22ms | 1.229× | 1.39% | PASS |
| file_reads | `types_table_scan` | 977.20ms | 1.12s | 1.149× | 0.70% | PASS |
| file_reads | `table_scan` | 1.16s | 1.25s | 1.073× | 1.40% | PASS |
| file_reads | `oltp_read_only` | 145.28ms | 135.19ms | 0.931× | 0.66% | PASS |
| file_writes | `oltp_bulk_insert` | 214.04ms | 310.76ms | 1.452× | 0.82% | PASS |
| file_writes | `oltp_insert` | 49.03ms | 42.01ms | 0.857× | 11.88% | PASS |
| file_writes | `oltp_update_index` | 82.78ms | 138.40ms | 1.672× | 1.63% | PASS |
| file_writes | `oltp_update_non_index` | 91.41ms | 88.83ms | 0.972× | 12.83% | PASS |
| file_writes | `oltp_delete_insert` | 63.67ms | 105.65ms | 1.659× | 1.69% | PASS |
| file_writes | `oltp_write_only` | 55.40ms | 65.98ms | 1.191× | 9.74% | PASS |
| file_writes | `types_delete_insert` | 39.73ms | 58.49ms | 1.472× | 1.34% | PASS |
| file_writes | `oltp_read_write` | 108.55ms | 133.69ms | 1.232× | 9.37% | PASS |
| ac_reads | `oltp_point_select` | 34.40ms | 42.14ms | 1.225× | 1.17% | PASS |
| ac_reads | `oltp_range_select` | 12.32ms | 14.01ms | 1.137× | 0.96% | PASS |
| ac_reads | `oltp_sum_range` | 12.02ms | 13.62ms | 1.133× | 0.88% | PASS |
| ac_reads | `oltp_order_range` | 2.75ms | 3.02ms | 1.101× | 1.64% | PASS |
| ac_reads | `oltp_distinct_range` | 3.51ms | 3.94ms | 1.122× | 1.09% | PASS |
| ac_reads | `oltp_index_scan` | 5.03ms | 6.65ms | 1.323× | 1.39% | PASS |
| ac_reads | `select_random_points` | 16.94ms | 21.33ms | 1.259× | 1.08% | PASS |
| ac_reads | `select_random_ranges` | 4.34ms | 5.63ms | 1.295× | 1.52% | PASS |
| ac_reads | `covering_index_scan` | 5.24ms | 5.15ms | 0.983× | 1.86% | PASS |
| ac_reads | `groupby_scan` | 28.55ms | 30.88ms | 1.082× | 0.52% | PASS |
| ac_reads | `index_join` | 7.05ms | 9.32ms | 1.323× | 1.46% | PASS |
| ac_reads | `index_join_scan` | 4.18ms | 5.25ms | 1.256× | 1.28% | PASS |
| ac_reads | `types_table_scan` | 976.96ms | 1.12s | 1.147× | 0.64% | PASS |
| ac_reads | `table_scan` | 1.14s | 1.24s | 1.093× | 1.14% | PASS |
| ac_reads | `oltp_read_only` | 115.26ms | 134.79ms | 1.170× | 0.85% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 15.25ms | 56.91ms | 3.731× | 6.00% | PASS |
| ac_writes | `oltp_insert_ac` | 17.83ms | 67.57ms | 3.789× | 3.54% | PASS |
| ac_writes | `oltp_update_index_ac` | 18.66ms | 84.04ms | 4.504× | 5.12% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 15.36ms | 64.74ms | 4.215× | 4.45% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 16.63ms | 74.27ms | 4.467× | 5.86% | PASS |
| ac_writes | `oltp_write_only_ac` | 17.22ms | 74.54ms | 4.328× | 5.93% | PASS |
| ac_writes | `types_delete_insert_ac` | 15.04ms | 66.77ms | 4.438× | 6.41% | PASS |
| ac_writes | `oltp_read_write_ac` | 21.22ms | 79.56ms | 3.750× | 3.33% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 32.49ms | 38.55ms | 1.187× | 1.94% | PASS |
| mem_reads | `oltp_range_select` | 13.57ms | 15.55ms | 1.146× | 2.64% | PASS |
| mem_reads | `oltp_sum_range` | 12.44ms | 14.58ms | 1.172× | 2.67% | PASS |
| mem_reads | `oltp_order_range` | 3.02ms | 3.23ms | 1.072× | 1.05% | PASS |
| mem_reads | `oltp_distinct_range` | 4.05ms | 4.40ms | 1.088× | 1.31% | PASS |
| mem_reads | `oltp_index_scan` | 4.77ms | 6.55ms | 1.373× | 2.25% | PASS |
| mem_reads | `select_random_points` | 18.84ms | 20.80ms | 1.104× | 2.38% | PASS |
| mem_reads | `select_random_ranges` | 4.15ms | 5.28ms | 1.273× | 2.04% | PASS |
| mem_reads | `covering_index_scan` | 4.57ms | 4.82ms | 1.054× | 2.78% | PASS |
| mem_reads | `groupby_scan` | 31.70ms | 35.21ms | 1.111× | 1.22% | PASS |
| mem_reads | `index_join` | 6.84ms | 9.59ms | 1.401× | 3.72% | PASS |
| mem_reads | `index_join_scan` | 4.30ms | 5.42ms | 1.261× | 4.21% | PASS |
| mem_reads | `types_table_scan` | 1.20s | 1.24s | 1.038× | 2.64% | PASS |
| mem_reads | `table_scan` | 1.41s | 1.34s | 0.948× | 2.95% | PASS |
| mem_reads | `oltp_read_only` | 126.26ms | 144.59ms | 1.145× | 2.31% | PASS |
| mem_writes | `oltp_bulk_insert` | 243.40ms | 358.57ms | 1.473× | 0.93% | PASS |
| mem_writes | `oltp_insert` | 20.33ms | 39.68ms | 1.952× | 0.97% | PASS |
| mem_writes | `oltp_update_index` | 74.65ms | 138.16ms | 1.851× | 2.00% | PASS |
| mem_writes | `oltp_update_non_index` | 52.80ms | 87.27ms | 1.653× | 1.50% | PASS |
| mem_writes | `oltp_delete_insert` | 52.61ms | 106.86ms | 2.031× | 1.80% | PASS |
| mem_writes | `oltp_write_only` | 31.00ms | 64.52ms | 2.081× | 1.71% | PASS |
| mem_writes | `types_delete_insert` | 35.14ms | 56.80ms | 1.616× | 1.32% | PASS |
| mem_writes | `oltp_read_write` | 97.30ms | 149.12ms | 1.533× | 1.66% | PASS |
| file_reads | `oltp_point_select` | 101.33ms | 62.43ms | 0.616× | 1.39% | PASS |
| file_reads | `oltp_range_select` | 22.27ms | 18.50ms | 0.831× | 2.65% | PASS |
| file_reads | `oltp_sum_range` | 20.54ms | 17.52ms | 0.853× | 2.40% | PASS |
| file_reads | `oltp_order_range` | 4.17ms | 3.98ms | 0.955× | 2.87% | PASS |
| file_reads | `oltp_distinct_range` | 5.23ms | 5.05ms | 0.965× | 2.96% | PASS |
| file_reads | `oltp_index_scan` | 11.71ms | 8.95ms | 0.764× | 2.13% | PASS |
| file_reads | `select_random_points` | 26.24ms | 24.07ms | 0.918× | 2.59% | PASS |
| file_reads | `select_random_ranges` | 10.88ms | 7.76ms | 0.713× | 1.29% | PASS |
| file_reads | `covering_index_scan` | 11.76ms | 7.26ms | 0.617× | 1.64% | PASS |
| file_reads | `groupby_scan` | 33.49ms | 36.21ms | 1.081× | 1.23% | PASS |
| file_reads | `index_join` | 10.90ms | 11.27ms | 1.034× | 2.19% | PASS |
| file_reads | `index_join_scan` | 5.23ms | 5.92ms | 1.132× | 2.62% | PASS |
| file_reads | `types_table_scan` | 1.17s | 1.24s | 1.057× | 2.31% | PASS |
| file_reads | `table_scan` | 1.38s | 1.33s | 0.965× | 4.12% | PASS |
| file_reads | `oltp_read_only` | 230.06ms | 179.52ms | 0.780× | 1.30% | PASS |
| file_writes | `oltp_bulk_insert` | 259.19ms | 385.50ms | 1.487× | 1.14% | PASS |
| file_writes | `oltp_insert` | 33.44ms | 52.55ms | 1.571× | 2.62% | PASS |
| file_writes | `oltp_update_index` | 104.29ms | 163.83ms | 1.571× | 1.85% | PASS |
| file_writes | `oltp_update_non_index` | 81.12ms | 110.78ms | 1.365× | 1.72% | PASS |
| file_writes | `oltp_delete_insert` | 85.74ms | 134.37ms | 1.567× | 1.34% | PASS |
| file_writes | `oltp_write_only` | 59.01ms | 87.20ms | 1.478× | 2.68% | PASS |
| file_writes | `types_delete_insert` | 54.65ms | 74.44ms | 1.362× | 1.98% | PASS |
| file_writes | `oltp_read_write` | 124.58ms | 169.10ms | 1.357× | 1.73% | PASS |
| ac_reads | `oltp_point_select` | 56.32ms | 63.16ms | 1.121× | 2.06% | PASS |
| ac_reads | `oltp_range_select` | 16.94ms | 18.45ms | 1.089× | 3.55% | PASS |
| ac_reads | `oltp_sum_range` | 15.82ms | 17.54ms | 1.109× | 2.07% | PASS |
| ac_reads | `oltp_order_range` | 3.91ms | 4.00ms | 1.023× | 3.04% | PASS |
| ac_reads | `oltp_distinct_range` | 4.79ms | 5.05ms | 1.055× | 2.28% | PASS |
| ac_reads | `oltp_index_scan` | 7.68ms | 9.13ms | 1.189× | 2.39% | PASS |
| ac_reads | `select_random_points` | 23.78ms | 25.65ms | 1.079× | 2.31% | PASS |
| ac_reads | `select_random_ranges` | 6.84ms | 7.82ms | 1.143× | 1.43% | PASS |
| ac_reads | `covering_index_scan` | 7.96ms | 7.35ms | 0.923× | 3.38% | PASS |
| ac_reads | `groupby_scan` | 32.73ms | 36.12ms | 1.104× | 1.02% | PASS |
| ac_reads | `index_join` | 9.05ms | 11.46ms | 1.266× | 3.63% | PASS |
| ac_reads | `index_join_scan` | 5.06ms | 6.06ms | 1.198× | 4.37% | PASS |
| ac_reads | `types_table_scan` | 1.07s | 1.21s | 1.124× | 1.01% | PASS |
| ac_reads | `table_scan` | 1.47s | 1.36s | 0.923× | 0.53% | PASS |
| ac_reads | `oltp_read_only` | 169.73ms | 184.37ms | 1.086× | 1.14% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.15ms | 81.05ms | 3.659× | 6.48% | PASS |
| ac_writes | `oltp_insert_ac` | 24.46ms | 104.33ms | 4.265× | 3.44% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.59ms | 118.77ms | 4.305× | 5.29% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.14ms | 94.46ms | 3.912× | 7.46% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.16ms | 106.59ms | 4.413× | 6.30% | PASS |
| ac_writes | `oltp_write_only_ac` | 24.64ms | 108.00ms | 4.383× | 6.20% | PASS |
| ac_writes | `types_delete_insert_ac` | 21.58ms | 97.86ms | 4.534× | 6.84% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.60ms | 117.68ms | 3.724× | 6.43% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 34.79ms | 41.91ms | 1.204× | 1.67% | PASS |
| mem_reads | `oltp_range_select` | 19.04ms | 23.07ms | 1.212× | 1.72% | PASS |
| mem_reads | `oltp_sum_range` | 18.29ms | 22.00ms | 1.203× | 1.41% | PASS |
| mem_reads | `oltp_order_range` | 3.49ms | 3.97ms | 1.138× | 1.75% | PASS |
| mem_reads | `oltp_distinct_range` | 4.58ms | 5.20ms | 1.135× | 1.16% | PASS |
| mem_reads | `oltp_index_scan` | 4.65ms | 6.22ms | 1.337× | 1.55% | PASS |
| mem_reads | `select_random_points` | 28.27ms | 32.50ms | 1.149× | 1.68% | PASS |
| mem_reads | `select_random_ranges` | 7.64ms | 9.31ms | 1.219× | 1.97% | PASS |
| mem_reads | `covering_index_scan` | 4.19ms | 4.36ms | 1.040× | 2.77% | PASS |
| mem_reads | `groupby_scan` | 35.72ms | 40.61ms | 1.137× | 0.74% | PASS |
| mem_reads | `index_join` | 8.14ms | 10.74ms | 1.321× | 2.03% | PASS |
| mem_reads | `index_join_scan` | 4.05ms | 5.44ms | 1.344× | 1.50% | PASS |
| mem_reads | `types_table_scan` | 1.06s | 1.21s | 1.138× | 1.61% | PASS |
| mem_reads | `table_scan` | 1.32s | 1.34s | 1.014× | 3.70% | PASS |
| mem_reads | `oltp_read_only` | 150.31ms | 179.99ms | 1.197× | 2.87% | PASS |
| mem_writes | `oltp_bulk_insert` | 247.01ms | 357.23ms | 1.446× | 0.95% | PASS |
| mem_writes | `oltp_insert` | 19.06ms | 36.37ms | 1.908× | 0.87% | PASS |
| mem_writes | `oltp_update_index` | 67.38ms | 115.67ms | 1.717× | 0.98% | PASS |
| mem_writes | `oltp_update_non_index` | 51.39ms | 83.09ms | 1.617× | 1.46% | PASS |
| mem_writes | `oltp_delete_insert` | 50.65ms | 97.59ms | 1.927× | 1.26% | PASS |
| mem_writes | `oltp_write_only` | 27.20ms | 57.17ms | 2.102× | 0.90% | PASS |
| mem_writes | `types_delete_insert` | 33.18ms | 55.24ms | 1.665× | 1.41% | PASS |
| mem_writes | `oltp_read_write` | 104.23ms | 159.42ms | 1.529× | 1.69% | PASS |
| file_reads | `oltp_point_select` | 103.16ms | 66.31ms | 0.643× | 1.48% | PASS |
| file_reads | `oltp_range_select` | 27.19ms | 26.46ms | 0.973× | 1.65% | PASS |
| file_reads | `oltp_sum_range` | 25.85ms | 25.19ms | 0.974× | 2.05% | PASS |
| file_reads | `oltp_order_range` | 4.46ms | 4.50ms | 1.009× | 3.01% | PASS |
| file_reads | `oltp_distinct_range` | 5.50ms | 5.69ms | 1.033× | 1.74% | PASS |
| file_reads | `oltp_index_scan` | 11.94ms | 9.09ms | 0.761× | 1.85% | PASS |
| file_reads | `select_random_points` | 37.41ms | 36.76ms | 0.983× | 1.34% | PASS |
| file_reads | `select_random_ranges` | 15.21ms | 12.36ms | 0.812× | 1.41% | PASS |
| file_reads | `covering_index_scan` | 11.56ms | 7.20ms | 0.623× | 1.73% | PASS |
| file_reads | `groupby_scan` | 36.58ms | 41.56ms | 1.136× | 0.86% | PASS |
| file_reads | `index_join` | 12.55ms | 13.10ms | 1.043× | 1.49% | PASS |
| file_reads | `index_join_scan` | 5.28ms | 6.07ms | 1.151× | 1.92% | PASS |
| file_reads | `types_table_scan` | 1.14s | 1.24s | 1.083× | 1.88% | PASS |
| file_reads | `table_scan` | 1.42s | 1.36s | 0.956× | 0.92% | PASS |
| file_reads | `oltp_read_only` | 258.32ms | 220.15ms | 0.852× | 1.63% | PASS |
| file_writes | `oltp_bulk_insert` | 260.88ms | 383.78ms | 1.471× | 1.39% | PASS |
| file_writes | `oltp_insert` | 26.46ms | 46.56ms | 1.760× | 1.93% | PASS |
| file_writes | `oltp_update_index` | 98.00ms | 146.21ms | 1.492× | 1.91% | PASS |
| file_writes | `oltp_update_non_index` | 77.63ms | 105.19ms | 1.355× | 2.18% | PASS |
| file_writes | `oltp_delete_insert` | 76.83ms | 119.03ms | 1.549× | 1.53% | PASS |
| file_writes | `oltp_write_only` | 50.20ms | 76.73ms | 1.529× | 1.59% | PASS |
| file_writes | `types_delete_insert` | 49.46ms | 68.51ms | 1.385× | 2.25% | PASS |
| file_writes | `oltp_read_write` | 127.93ms | 177.94ms | 1.391× | 1.54% | PASS |
| ac_reads | `oltp_point_select` | 56.90ms | 65.82ms | 1.157× | 0.92% | PASS |
| ac_reads | `oltp_range_select` | 20.80ms | 25.95ms | 1.248× | 1.13% | PASS |
| ac_reads | `oltp_sum_range` | 20.41ms | 24.99ms | 1.225× | 2.00% | PASS |
| ac_reads | `oltp_order_range` | 3.89ms | 4.42ms | 1.138× | 2.08% | PASS |
| ac_reads | `oltp_distinct_range` | 4.99ms | 5.65ms | 1.132× | 1.64% | PASS |
| ac_reads | `oltp_index_scan` | 7.03ms | 9.06ms | 1.289× | 1.74% | PASS |
| ac_reads | `select_random_points` | 31.07ms | 36.35ms | 1.170× | 1.30% | PASS |
| ac_reads | `select_random_ranges` | 10.07ms | 12.23ms | 1.214× | 1.87% | PASS |
| ac_reads | `covering_index_scan` | 6.72ms | 7.11ms | 1.057× | 2.03% | PASS |
| ac_reads | `groupby_scan` | 35.45ms | 41.43ms | 1.169× | 1.17% | PASS |
| ac_reads | `index_join` | 9.63ms | 12.94ms | 1.343× | 1.56% | PASS |
| ac_reads | `index_join_scan` | 4.69ms | 5.93ms | 1.265× | 2.70% | PASS |
| ac_reads | `types_table_scan` | 1.05s | 1.21s | 1.153× | 1.23% | PASS |
| ac_reads | `table_scan` | 1.27s | 1.33s | 1.053× | 5.06% | PASS |
| ac_reads | `oltp_read_only` | 190.96ms | 220.84ms | 1.156× | 1.26% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.82ms | 78.41ms | 3.593× | 3.58% | PASS |
| ac_writes | `oltp_insert_ac` | 24.62ms | 102.78ms | 4.175× | 4.91% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.20ms | 112.40ms | 4.132× | 5.47% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.33ms | 91.70ms | 3.931× | 5.25% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.46ms | 104.25ms | 4.262× | 4.05% | PASS |
| ac_writes | `oltp_write_only_ac` | 24.18ms | 99.80ms | 4.128× | 2.73% | PASS |
| ac_writes | `types_delete_insert_ac` | 21.52ms | 92.02ms | 4.275× | 4.44% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.18ms | 110.88ms | 3.556× | 3.53% | PASS |

</details>

## Version-control latency

Wall time: 1m 49s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 58.04ms | 200.00ms | 29.0% | 0.93% | PASS |
| `status_dirty_many_tables` | 60.28ms | 200.00ms | 30.1% | 0.80% | PASS |
| `diff_regular_working_one_table` | 53.99ms | 150.00ms | 36.0% | 0.79% | PASS |
| `diff_regular_working_many_tables` | 64.54ms | 200.00ms | 32.3% | 1.38% | PASS |
| `diff_stat_working_many_tables` | 64.77ms | 200.00ms | 32.4% | 1.30% | PASS |
| `diff_schema_working_many_tables` | 66.43ms | 200.00ms | 33.2% | 1.73% | PASS |
| `branch_list_many_branches` | 18.20ms | 100.00ms | 18.2% | 2.17% | PASS |
| `branch_create_delete` | 20.29ms | 100.00ms | 20.3% | 2.67% | PASS |
| `checkout_branch_clean` | 81.12ms | 200.00ms | 40.6% | 4.45% | PASS |
| `merge_data_no_conflicts` | 31.67ms | 150.00ms | 21.1% | 7.64% | PASS |
| `merge_schema_no_conflicts` | 18.21ms | 100.00ms | 18.2% | 2.72% | PASS |
| `merge_data_conflicts` | 67.59ms | 250.00ms | 27.0% | 0.81% | PASS |
| `merge_data_conflicts_with_resolve` | 68.13ms | 250.00ms | 27.3% | 1.22% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
