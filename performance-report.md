# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-08-04 11:58 UTC
>
> Commit: [`b26084ba74f4197c05a63f515308ca426f329def`](https://github.com/dolthub/doltlite/commit/b26084ba74f4197c05a63f515308ca426f329def)
>
> Runner: ubuntu24 20260720.247.2
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/30900218166)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians; MAD is the median absolute deviation and describes run-to-run noise.

## SQL workload summary

| Key shape | Workloads | Samples/workload | Wall time | SQLite median total | DoltLite median total | Ratio | Median paired-ratio MAD | Result |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| int | 69 | 55 | 1h 12m 49s | 9.02s | 11.78s | 1.306× | 1.45% | **PASS** |
| textpk | 69 | 55 | 1h 33m 5s | 10.00s | 12.27s | 1.227× | 1.59% | **PASS** |
| blobpk | 69 | 55 | 1h 31m 20s | 10.08s | 11.66s | 1.156× | 1.35% | **PASS** |
| compositepk | 69 | 55 | 1h 34m 16s | 10.84s | 16.61s | 1.532× | 2.16% | **PASS** |

The absolute ceiling is 2.5× per ordinary workload and 2.0× for a section average. Durable autocommit writes use 10.0× and 5.0× ceilings respectively.

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 23.86ms | 29.41ms | 1.232× | 1.45% | PASS |
| mem_reads | `oltp_range_select` | 10.11ms | 12.12ms | 1.200× | 2.29% | PASS |
| mem_reads | `oltp_sum_range` | 9.47ms | 12.01ms | 1.268× | 1.93% | PASS |
| mem_reads | `oltp_order_range` | 2.52ms | 2.88ms | 1.143× | 1.06% | PASS |
| mem_reads | `oltp_distinct_range` | 3.58ms | 3.95ms | 1.103× | 0.99% | PASS |
| mem_reads | `oltp_index_scan` | 3.97ms | 5.46ms | 1.376× | 1.63% | PASS |
| mem_reads | `select_random_points` | 10.12ms | 11.20ms | 1.106× | 3.37% | PASS |
| mem_reads | `select_random_ranges` | 2.99ms | 4.04ms | 1.352× | 1.72% | PASS |
| mem_reads | `covering_index_scan` | 4.24ms | 4.22ms | 0.995× | 1.20% | PASS |
| mem_reads | `groupby_scan` | 29.71ms | 32.89ms | 1.107× | 0.73% | PASS |
| mem_reads | `index_join` | 6.01ms | 8.22ms | 1.366× | 1.15% | PASS |
| mem_reads | `index_join_scan` | 3.51ms | 4.63ms | 1.320× | 2.28% | PASS |
| mem_reads | `types_table_scan` | 1.04s | 1.28s | 1.229× | 1.09% | PASS |
| mem_reads | `table_scan` | 1.16s | 1.36s | 1.167× | 0.45% | PASS |
| mem_reads | `oltp_read_only` | 102.93ms | 120.32ms | 1.169× | 1.49% | PASS |
| mem_writes | `oltp_bulk_insert` | 181.77ms | 250.75ms | 1.379× | 0.81% | PASS |
| mem_writes | `oltp_insert` | 15.54ms | 28.29ms | 1.820× | 0.65% | PASS |
| mem_writes | `oltp_update_index` | 49.41ms | 89.49ms | 1.811× | 0.96% | PASS |
| mem_writes | `oltp_update_non_index` | 33.52ms | 59.30ms | 1.769× | 1.26% | PASS |
| mem_writes | `oltp_delete_insert` | 44.75ms | 71.66ms | 1.601× | 1.01% | PASS |
| mem_writes | `oltp_write_only` | 22.00ms | 44.91ms | 2.041× | 1.17% | PASS |
| mem_writes | `types_delete_insert` | 24.81ms | 40.81ms | 1.645× | 1.17% | PASS |
| mem_writes | `oltp_read_write` | 66.95ms | 109.78ms | 1.640× | 1.57% | PASS |
| file_reads | `oltp_point_select` | 92.72ms | 53.63ms | 0.578× | 0.87% | PASS |
| file_reads | `oltp_range_select` | 17.52ms | 14.64ms | 0.836× | 1.95% | PASS |
| file_reads | `oltp_sum_range` | 17.26ms | 14.66ms | 0.849× | 1.55% | PASS |
| file_reads | `oltp_order_range` | 3.37ms | 3.19ms | 0.945× | 1.29% | PASS |
| file_reads | `oltp_distinct_range` | 4.40ms | 4.24ms | 0.964× | 1.79% | PASS |
| file_reads | `oltp_index_scan` | 11.25ms | 7.89ms | 0.701× | 1.75% | PASS |
| file_reads | `select_random_points` | 17.46ms | 13.70ms | 0.785× | 2.99% | PASS |
| file_reads | `select_random_ranges` | 9.94ms | 6.47ms | 0.651× | 0.63% | PASS |
| file_reads | `covering_index_scan` | 11.50ms | 6.69ms | 0.582× | 1.35% | PASS |
| file_reads | `groupby_scan` | 30.43ms | 33.20ms | 1.091× | 1.12% | PASS |
| file_reads | `index_join` | 10.15ms | 9.80ms | 0.966× | 1.38% | PASS |
| file_reads | `index_join_scan` | 4.41ms | 4.94ms | 1.121× | 2.05% | PASS |
| file_reads | `types_table_scan` | 1.03s | 1.27s | 1.227× | 0.30% | PASS |
| file_reads | `table_scan` | 1.16s | 1.36s | 1.170× | 0.87% | PASS |
| file_reads | `oltp_read_only` | 201.45ms | 155.61ms | 0.772× | 0.82% | PASS |
| file_writes | `oltp_bulk_insert` | 196.52ms | 268.17ms | 1.365× | 1.29% | PASS |
| file_writes | `oltp_insert` | 25.14ms | 35.48ms | 1.411× | 2.58% | PASS |
| file_writes | `oltp_update_index` | 78.03ms | 116.03ms | 1.487× | 1.77% | PASS |
| file_writes | `oltp_update_non_index` | 60.70ms | 80.92ms | 1.333× | 1.35% | PASS |
| file_writes | `oltp_delete_insert` | 70.50ms | 93.36ms | 1.324× | 2.07% | PASS |
| file_writes | `oltp_write_only` | 48.84ms | 62.74ms | 1.285× | 1.80% | PASS |
| file_writes | `types_delete_insert` | 41.60ms | 53.15ms | 1.278× | 1.19% | PASS |
| file_writes | `oltp_read_write` | 99.36ms | 131.84ms | 1.327× | 1.66% | PASS |
| ac_reads | `oltp_point_select` | 47.12ms | 53.77ms | 1.141× | 1.38% | PASS |
| ac_reads | `oltp_range_select` | 13.03ms | 14.66ms | 1.125× | 2.37% | PASS |
| ac_reads | `oltp_sum_range` | 12.49ms | 14.76ms | 1.181× | 2.11% | PASS |
| ac_reads | `oltp_order_range` | 2.94ms | 3.18ms | 1.084× | 1.34% | PASS |
| ac_reads | `oltp_distinct_range` | 3.94ms | 4.27ms | 1.084× | 1.10% | PASS |
| ac_reads | `oltp_index_scan` | 6.76ms | 8.24ms | 1.219× | 1.82% | PASS |
| ac_reads | `select_random_points` | 13.17ms | 13.70ms | 1.040× | 3.07% | PASS |
| ac_reads | `select_random_ranges` | 5.28ms | 6.49ms | 1.229× | 1.59% | PASS |
| ac_reads | `covering_index_scan` | 6.59ms | 6.75ms | 1.024× | 1.42% | PASS |
| ac_reads | `groupby_scan` | 29.86ms | 33.18ms | 1.111× | 0.91% | PASS |
| ac_reads | `index_join` | 7.59ms | 10.22ms | 1.346× | 1.80% | PASS |
| ac_reads | `index_join_scan` | 3.90ms | 4.94ms | 1.267× | 2.77% | PASS |
| ac_reads | `types_table_scan` | 1.04s | 1.27s | 1.224× | 0.55% | PASS |
| ac_reads | `table_scan` | 1.17s | 1.37s | 1.169× | 0.91% | PASS |
| ac_reads | `oltp_read_only` | 138.48ms | 156.87ms | 1.133× | 1.12% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 51.44ms | 164.73ms | 3.203× | 11.36% | PASS |
| ac_writes | `oltp_insert_ac` | 55.20ms | 183.65ms | 3.327× | 13.24% | PASS |
| ac_writes | `oltp_update_index_ac` | 53.40ms | 185.42ms | 3.472× | 11.70% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 46.12ms | 166.00ms | 3.600× | 14.62% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 46.59ms | 166.56ms | 3.575× | 11.13% | PASS |
| ac_writes | `oltp_write_only_ac` | 50.36ms | 176.91ms | 3.513× | 12.84% | PASS |
| ac_writes | `types_delete_insert_ac` | 47.59ms | 174.52ms | 3.668× | 8.64% | PASS |
| ac_writes | `oltp_read_write_ac` | 60.99ms | 205.87ms | 3.376× | 10.27% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 30.08ms | 37.82ms | 1.257× | 1.34% | PASS |
| mem_reads | `oltp_range_select` | 13.15ms | 14.05ms | 1.069× | 1.52% | PASS |
| mem_reads | `oltp_sum_range` | 11.96ms | 14.12ms | 1.181× | 1.59% | PASS |
| mem_reads | `oltp_order_range` | 2.92ms | 3.16ms | 1.083× | 1.18% | PASS |
| mem_reads | `oltp_distinct_range` | 3.95ms | 4.21ms | 1.065× | 1.03% | PASS |
| mem_reads | `oltp_index_scan` | 4.43ms | 6.25ms | 1.408× | 1.86% | PASS |
| mem_reads | `select_random_points` | 17.46ms | 20.94ms | 1.199× | 1.69% | PASS |
| mem_reads | `select_random_ranges` | 3.98ms | 5.23ms | 1.314× | 1.77% | PASS |
| mem_reads | `covering_index_scan` | 4.53ms | 4.58ms | 1.011× | 1.42% | PASS |
| mem_reads | `groupby_scan` | 31.70ms | 34.09ms | 1.075× | 0.93% | PASS |
| mem_reads | `index_join` | 6.89ms | 9.25ms | 1.343× | 2.34% | PASS |
| mem_reads | `index_join_scan` | 4.51ms | 5.38ms | 1.191× | 1.76% | PASS |
| mem_reads | `types_table_scan` | 1.05s | 1.32s | 1.261× | 0.66% | PASS |
| mem_reads | `table_scan` | 1.41s | 1.43s | 1.016× | 4.34% | PASS |
| mem_reads | `oltp_read_only` | 120.19ms | 137.53ms | 1.144× | 2.21% | PASS |
| mem_writes | `oltp_bulk_insert` | 236.86ms | 358.88ms | 1.515× | 0.67% | PASS |
| mem_writes | `oltp_insert` | 22.46ms | 40.01ms | 1.782× | 1.53% | PASS |
| mem_writes | `oltp_update_index` | 78.89ms | 141.32ms | 1.791× | 2.14% | PASS |
| mem_writes | `oltp_update_non_index` | 51.87ms | 91.30ms | 1.760× | 0.96% | PASS |
| mem_writes | `oltp_delete_insert` | 54.76ms | 107.51ms | 1.963× | 1.41% | PASS |
| mem_writes | `oltp_write_only` | 30.12ms | 62.50ms | 2.075× | 1.26% | PASS |
| mem_writes | `types_delete_insert` | 33.06ms | 55.49ms | 1.678× | 1.21% | PASS |
| mem_writes | `oltp_read_write` | 90.14ms | 142.15ms | 1.577× | 2.20% | PASS |
| file_reads | `oltp_point_select` | 99.52ms | 62.72ms | 0.630× | 1.43% | PASS |
| file_reads | `oltp_range_select` | 21.01ms | 16.69ms | 0.794× | 1.19% | PASS |
| file_reads | `oltp_sum_range` | 19.96ms | 16.78ms | 0.840× | 1.69% | PASS |
| file_reads | `oltp_order_range` | 3.95ms | 3.50ms | 0.886× | 1.54% | PASS |
| file_reads | `oltp_distinct_range` | 4.98ms | 4.57ms | 0.917× | 1.35% | PASS |
| file_reads | `oltp_index_scan` | 12.24ms | 9.12ms | 0.745× | 0.97% | PASS |
| file_reads | `select_random_points` | 26.40ms | 24.20ms | 0.917× | 1.56% | PASS |
| file_reads | `select_random_ranges` | 11.28ms | 7.78ms | 0.690× | 0.75% | PASS |
| file_reads | `covering_index_scan` | 12.81ms | 7.27ms | 0.567× | 1.00% | PASS |
| file_reads | `groupby_scan` | 33.09ms | 34.54ms | 1.044× | 1.12% | PASS |
| file_reads | `index_join` | 11.76ms | 11.18ms | 0.951× | 1.64% | PASS |
| file_reads | `index_join_scan` | 5.68ms | 5.99ms | 1.054× | 2.25% | PASS |
| file_reads | `types_table_scan` | 1.10s | 1.33s | 1.208× | 2.57% | PASS |
| file_reads | `table_scan` | 1.41s | 1.42s | 1.009× | 4.06% | PASS |
| file_reads | `oltp_read_only` | 231.63ms | 177.03ms | 0.764× | 1.19% | PASS |
| file_writes | `oltp_bulk_insert` | 257.70ms | 388.55ms | 1.508× | 1.25% | PASS |
| file_writes | `oltp_insert` | 50.09ms | 52.95ms | 1.057× | 21.37% | PASS |
| file_writes | `oltp_update_index` | 117.05ms | 171.08ms | 1.462× | 1.56% | PASS |
| file_writes | `oltp_update_non_index` | 94.33ms | 115.44ms | 1.224× | 10.49% | PASS |
| file_writes | `oltp_delete_insert` | 95.05ms | 136.19ms | 1.433× | 1.74% | PASS |
| file_writes | `oltp_write_only` | 86.56ms | 86.61ms | 1.001× | 9.24% | PASS |
| file_writes | `types_delete_insert` | 57.40ms | 76.07ms | 1.325× | 1.72% | PASS |
| file_writes | `oltp_read_write` | 140.30ms | 167.14ms | 1.191× | 7.64% | PASS |
| ac_reads | `oltp_point_select` | 54.65ms | 63.50ms | 1.162× | 1.15% | PASS |
| ac_reads | `oltp_range_select` | 17.28ms | 16.97ms | 0.982× | 2.01% | PASS |
| ac_reads | `oltp_sum_range` | 15.52ms | 16.98ms | 1.094× | 1.50% | PASS |
| ac_reads | `oltp_order_range` | 3.46ms | 3.49ms | 1.008× | 1.36% | PASS |
| ac_reads | `oltp_distinct_range` | 4.45ms | 4.55ms | 1.024× | 1.45% | PASS |
| ac_reads | `oltp_index_scan` | 7.34ms | 9.07ms | 1.236× | 1.59% | PASS |
| ac_reads | `select_random_points` | 21.47ms | 24.68ms | 1.149× | 2.07% | PASS |
| ac_reads | `select_random_ranges` | 6.77ms | 7.82ms | 1.155× | 1.53% | PASS |
| ac_reads | `covering_index_scan` | 8.26ms | 7.29ms | 0.882× | 2.58% | PASS |
| ac_reads | `groupby_scan` | 32.55ms | 34.79ms | 1.069× | 1.11% | PASS |
| ac_reads | `index_join` | 9.29ms | 11.28ms | 1.214× | 2.07% | PASS |
| ac_reads | `index_join_scan` | 5.08ms | 5.90ms | 1.162× | 1.71% | PASS |
| ac_reads | `types_table_scan` | 1.05s | 1.31s | 1.250× | 0.62% | PASS |
| ac_reads | `table_scan` | 1.21s | 1.40s | 1.157× | 0.47% | PASS |
| ac_reads | `oltp_read_only` | 150.10ms | 171.83ms | 1.145× | 0.76% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.19ms | 80.84ms | 3.815× | 5.29% | PASS |
| ac_writes | `oltp_insert_ac` | 26.21ms | 97.22ms | 3.710× | 6.23% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.72ms | 115.29ms | 4.315× | 7.13% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.48ms | 95.04ms | 4.227× | 7.40% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.01ms | 107.92ms | 4.495× | 5.18% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.58ms | 103.66ms | 3.900× | 8.64% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.81ms | 97.39ms | 4.270× | 6.66% | PASS |
| ac_writes | `oltp_read_write_ac` | 30.14ms | 114.00ms | 3.782× | 6.45% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 30.72ms | 35.05ms | 1.141× | 1.60% | PASS |
| mem_reads | `oltp_range_select` | 13.16ms | 13.63ms | 1.036× | 1.51% | PASS |
| mem_reads | `oltp_sum_range` | 12.17ms | 13.04ms | 1.072× | 1.40% | PASS |
| mem_reads | `oltp_order_range` | 3.07ms | 3.12ms | 1.015× | 1.01% | PASS |
| mem_reads | `oltp_distinct_range` | 4.14ms | 4.15ms | 1.002× | 0.70% | PASS |
| mem_reads | `oltp_index_scan` | 4.66ms | 6.00ms | 1.290× | 1.96% | PASS |
| mem_reads | `select_random_points` | 17.75ms | 19.98ms | 1.125× | 1.31% | PASS |
| mem_reads | `select_random_ranges` | 4.23ms | 5.22ms | 1.235× | 1.59% | PASS |
| mem_reads | `covering_index_scan` | 4.61ms | 4.57ms | 0.990× | 2.84% | PASS |
| mem_reads | `groupby_scan` | 33.98ms | 35.22ms | 1.037× | 0.66% | PASS |
| mem_reads | `index_join` | 6.92ms | 9.44ms | 1.364× | 2.17% | PASS |
| mem_reads | `index_join_scan` | 4.36ms | 5.62ms | 1.288× | 1.55% | PASS |
| mem_reads | `types_table_scan` | 1.17s | 1.27s | 1.090× | 1.77% | PASS |
| mem_reads | `table_scan` | 1.30s | 1.36s | 1.046× | 1.24% | PASS |
| mem_reads | `oltp_read_only` | 125.53ms | 131.97ms | 1.051× | 3.05% | PASS |
| mem_writes | `oltp_bulk_insert` | 237.35ms | 330.48ms | 1.392× | 0.61% | PASS |
| mem_writes | `oltp_insert` | 20.45ms | 38.86ms | 1.901× | 0.57% | PASS |
| mem_writes | `oltp_update_index` | 68.80ms | 130.78ms | 1.901× | 1.08% | PASS |
| mem_writes | `oltp_update_non_index` | 49.92ms | 85.94ms | 1.722× | 1.57% | PASS |
| mem_writes | `oltp_delete_insert` | 49.73ms | 102.92ms | 2.069× | 1.00% | PASS |
| mem_writes | `oltp_write_only` | 28.46ms | 63.17ms | 2.220× | 1.06% | PASS |
| mem_writes | `types_delete_insert` | 32.24ms | 52.18ms | 1.619× | 1.17% | PASS |
| mem_writes | `oltp_read_write` | 79.99ms | 133.44ms | 1.668× | 0.75% | PASS |
| file_reads | `oltp_point_select` | 113.32ms | 63.95ms | 0.564× | 1.27% | PASS |
| file_reads | `oltp_range_select` | 24.02ms | 18.95ms | 0.789× | 1.05% | PASS |
| file_reads | `oltp_sum_range` | 22.94ms | 18.33ms | 0.799× | 1.06% | PASS |
| file_reads | `oltp_order_range` | 4.42ms | 3.99ms | 0.902× | 1.60% | PASS |
| file_reads | `oltp_distinct_range` | 5.76ms | 5.16ms | 0.896× | 1.23% | PASS |
| file_reads | `oltp_index_scan` | 15.44ms | 10.33ms | 0.669× | 1.04% | PASS |
| file_reads | `select_random_points` | 31.52ms | 26.77ms | 0.849× | 1.15% | PASS |
| file_reads | `select_random_ranges` | 14.55ms | 9.39ms | 0.645× | 1.69% | PASS |
| file_reads | `covering_index_scan` | 15.22ms | 8.69ms | 0.571× | 2.09% | PASS |
| file_reads | `groupby_scan` | 34.65ms | 35.53ms | 1.025× | 1.20% | PASS |
| file_reads | `index_join` | 11.67ms | 11.16ms | 0.956× | 1.67% | PASS |
| file_reads | `index_join_scan` | 5.34ms | 5.97ms | 1.118× | 2.56% | PASS |
| file_reads | `types_table_scan` | 1.13s | 1.24s | 1.097× | 1.36% | PASS |
| file_reads | `table_scan` | 1.31s | 1.36s | 1.038× | 1.41% | PASS |
| file_reads | `oltp_read_only` | 239.43ms | 170.29ms | 0.711× | 1.35% | PASS |
| file_writes | `oltp_bulk_insert` | 259.24ms | 358.26ms | 1.382× | 0.97% | PASS |
| file_writes | `oltp_insert` | 30.88ms | 51.62ms | 1.672× | 1.89% | PASS |
| file_writes | `oltp_update_index` | 103.53ms | 165.29ms | 1.596× | 1.59% | PASS |
| file_writes | `oltp_update_non_index` | 77.88ms | 108.98ms | 1.399× | 1.84% | PASS |
| file_writes | `oltp_delete_insert` | 79.67ms | 131.44ms | 1.650× | 0.88% | PASS |
| file_writes | `oltp_write_only` | 55.84ms | 87.38ms | 1.565× | 2.42% | PASS |
| file_writes | `types_delete_insert` | 51.44ms | 71.99ms | 1.400× | 0.84% | PASS |
| file_writes | `oltp_read_write` | 111.10ms | 157.88ms | 1.421× | 1.90% | PASS |
| ac_reads | `oltp_point_select` | 58.31ms | 63.77ms | 1.094× | 1.04% | PASS |
| ac_reads | `oltp_range_select` | 16.79ms | 16.59ms | 0.988× | 2.18% | PASS |
| ac_reads | `oltp_sum_range` | 15.50ms | 16.01ms | 1.033× | 0.85% | PASS |
| ac_reads | `oltp_order_range` | 3.51ms | 3.49ms | 0.995× | 1.12% | PASS |
| ac_reads | `oltp_distinct_range` | 4.55ms | 4.48ms | 0.986× | 0.65% | PASS |
| ac_reads | `oltp_index_scan` | 7.91ms | 9.13ms | 1.154× | 1.22% | PASS |
| ac_reads | `select_random_points` | 21.36ms | 23.13ms | 1.083× | 1.76% | PASS |
| ac_reads | `select_random_ranges` | 7.23ms | 8.22ms | 1.138× | 1.04% | PASS |
| ac_reads | `covering_index_scan` | 8.29ms | 7.65ms | 0.923× | 1.26% | PASS |
| ac_reads | `groupby_scan` | 34.07ms | 35.45ms | 1.040× | 0.76% | PASS |
| ac_reads | `index_join` | 9.18ms | 11.12ms | 1.211× | 1.14% | PASS |
| ac_reads | `index_join_scan` | 4.93ms | 5.94ms | 1.204× | 1.14% | PASS |
| ac_reads | `types_table_scan` | 1.13s | 1.23s | 1.095× | 0.39% | PASS |
| ac_reads | `table_scan` | 1.40s | 1.39s | 0.989× | 3.24% | PASS |
| ac_reads | `oltp_read_only` | 165.20ms | 172.67ms | 1.045× | 1.35% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 17.02ms | 65.50ms | 3.848× | 5.00% | PASS |
| ac_writes | `oltp_insert_ac` | 18.27ms | 84.95ms | 4.650× | 2.93% | PASS |
| ac_writes | `oltp_update_index_ac` | 20.38ms | 97.52ms | 4.786× | 5.73% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 16.67ms | 79.00ms | 4.739× | 5.60% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 19.07ms | 90.61ms | 4.752× | 5.10% | PASS |
| ac_writes | `oltp_write_only_ac` | 18.11ms | 87.16ms | 4.813× | 2.79% | PASS |
| ac_writes | `types_delete_insert_ac` | 17.41ms | 82.98ms | 4.766× | 5.84% | PASS |
| ac_writes | `oltp_read_write_ac` | 25.24ms | 95.80ms | 3.796× | 5.10% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 19.79ms | 23.01ms | 1.163× | 2.02% | PASS |
| mem_reads | `oltp_range_select` | 13.10ms | 13.14ms | 1.003× | 3.22% | PASS |
| mem_reads | `oltp_sum_range` | 12.97ms | 12.51ms | 0.965× | 4.19% | PASS |
| mem_reads | `oltp_order_range` | 2.55ms | 2.50ms | 0.982× | 2.10% | PASS |
| mem_reads | `oltp_distinct_range` | 3.12ms | 3.12ms | 0.998× | 2.11% | PASS |
| mem_reads | `oltp_index_scan` | 3.05ms | 3.86ms | 1.265× | 2.16% | PASS |
| mem_reads | `select_random_points` | 19.16ms | 21.27ms | 1.111× | 1.57% | PASS |
| mem_reads | `select_random_ranges` | 4.83ms | 5.44ms | 1.127× | 1.89% | PASS |
| mem_reads | `covering_index_scan` | 2.46ms | 2.48ms | 1.009× | 0.80% | PASS |
| mem_reads | `groupby_scan` | 22.89ms | 24.34ms | 1.063× | 0.70% | PASS |
| mem_reads | `index_join` | 5.52ms | 7.01ms | 1.270× | 1.32% | PASS |
| mem_reads | `index_join_scan` | 2.79ms | 4.35ms | 1.559× | 3.38% | PASS |
| mem_reads | `types_table_scan` | 751.22ms | 815.30ms | 1.085× | 0.75% | PASS |
| mem_reads | `table_scan` | 890.63ms | 909.95ms | 1.022× | 3.12% | PASS |
| mem_reads | `oltp_read_only` | 101.72ms | 104.80ms | 1.030× | 1.44% | PASS |
| mem_writes | `oltp_bulk_insert` | 139.34ms | 199.44ms | 1.431× | 0.83% | PASS |
| mem_writes | `oltp_insert` | 11.70ms | 21.86ms | 1.868× | 1.00% | PASS |
| mem_writes | `oltp_update_index` | 45.77ms | 80.20ms | 1.752× | 0.96% | PASS |
| mem_writes | `oltp_update_non_index` | 32.76ms | 54.20ms | 1.655× | 1.18% | PASS |
| mem_writes | `oltp_delete_insert` | 33.00ms | 61.62ms | 1.867× | 1.15% | PASS |
| mem_writes | `oltp_write_only` | 17.55ms | 35.08ms | 1.998× | 2.21% | PASS |
| mem_writes | `types_delete_insert` | 20.10ms | 32.31ms | 1.607× | 1.56% | PASS |
| mem_writes | `oltp_read_write` | 62.75ms | 91.69ms | 1.461× | 0.83% | PASS |
| file_reads | `oltp_point_select` | 41.57ms | 31.34ms | 0.754× | 0.81% | PASS |
| file_reads | `oltp_range_select` | 16.67ms | 14.35ms | 0.861× | 1.14% | PASS |
| file_reads | `oltp_sum_range` | 16.23ms | 13.48ms | 0.831× | 1.76% | PASS |
| file_reads | `oltp_order_range` | 2.86ms | 2.69ms | 0.940× | 2.00% | PASS |
| file_reads | `oltp_distinct_range` | 3.50ms | 3.27ms | 0.935× | 2.44% | PASS |
| file_reads | `oltp_index_scan` | 5.50ms | 5.23ms | 0.951× | 3.37% | PASS |
| file_reads | `select_random_points` | 22.24ms | 23.11ms | 1.039× | 1.43% | PASS |
| file_reads | `select_random_ranges` | 7.32ms | 6.57ms | 0.899× | 1.74% | PASS |
| file_reads | `covering_index_scan` | 4.65ms | 3.54ms | 0.762× | 4.22% | PASS |
| file_reads | `groupby_scan` | 23.27ms | 24.63ms | 1.059× | 0.62% | PASS |
| file_reads | `index_join` | 7.05ms | 8.42ms | 1.195× | 3.26% | PASS |
| file_reads | `index_join_scan` | 3.24ms | 4.67ms | 1.441× | 3.89% | PASS |
| file_reads | `types_table_scan` | 750.20ms | 813.08ms | 1.084× | 1.04% | PASS |
| file_reads | `table_scan` | 858.78ms | 889.81ms | 1.036× | 1.76% | PASS |
| file_reads | `oltp_read_only` | 122.96ms | 111.37ms | 0.906× | 0.89% | PASS |
| file_writes | `oltp_bulk_insert` | 282.58ms | 327.31ms | 1.158× | 36.26% | PASS |
| file_writes | `oltp_insert` | 136.73ms | 105.49ms | 0.772× | 74.58% | PASS |
| file_writes | `oltp_update_index` | 238.77ms | 179.67ms | 0.752× | 32.75% | PASS |
| file_writes | `oltp_update_non_index` | 264.04ms | 156.18ms | 0.592× | 35.68% | PASS |
| file_writes | `oltp_delete_insert` | 286.18ms | 156.21ms | 0.546× | 44.41% | PASS |
| file_writes | `oltp_write_only` | 224.69ms | 136.57ms | 0.608× | 58.29% | PASS |
| file_writes | `types_delete_insert` | 151.86ms | 80.47ms | 0.530× | 53.44% | PASS |
| file_writes | `oltp_read_write` | 279.35ms | 203.58ms | 0.729× | 35.40% | PASS |
| ac_reads | `oltp_point_select` | 27.40ms | 31.59ms | 1.153× | 1.04% | PASS |
| ac_reads | `oltp_range_select` | 14.74ms | 14.16ms | 0.961× | 2.55% | PASS |
| ac_reads | `oltp_sum_range` | 14.03ms | 13.23ms | 0.943× | 1.95% | PASS |
| ac_reads | `oltp_order_range` | 2.66ms | 2.63ms | 0.991× | 2.47% | PASS |
| ac_reads | `oltp_distinct_range` | 3.44ms | 3.30ms | 0.960× | 2.38% | PASS |
| ac_reads | `oltp_index_scan` | 4.24ms | 5.22ms | 1.232× | 4.10% | PASS |
| ac_reads | `select_random_points` | 20.04ms | 22.46ms | 1.121× | 1.48% | PASS |
| ac_reads | `select_random_ranges` | 5.76ms | 6.41ms | 1.112× | 2.39% | PASS |
| ac_reads | `covering_index_scan` | 3.40ms | 3.44ms | 1.012× | 3.77% | PASS |
| ac_reads | `groupby_scan` | 23.19ms | 24.61ms | 1.061× | 0.72% | PASS |
| ac_reads | `index_join` | 6.23ms | 7.66ms | 1.230× | 3.21% | PASS |
| ac_reads | `index_join_scan` | 3.18ms | 4.59ms | 1.445× | 3.83% | PASS |
| ac_reads | `types_table_scan` | 749.79ms | 815.96ms | 1.088× | 1.52% | PASS |
| ac_reads | `table_scan` | 862.89ms | 899.20ms | 1.042× | 1.32% | PASS |
| ac_reads | `oltp_read_only` | 105.70ms | 112.38ms | 1.063× | 1.96% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 418.27ms | 1.19s | 2.853× | 47.87% | PASS |
| ac_writes | `oltp_insert_ac` | 429.50ms | 1.28s | 2.989× | 62.34% | PASS |
| ac_writes | `oltp_update_index_ac` | 440.40ms | 1.05s | 2.381× | 42.65% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 150.56ms | 531.43ms | 3.530× | 45.12% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 407.98ms | 1.53s | 3.745× | 46.90% | PASS |
| ac_writes | `oltp_write_only_ac` | 511.43ms | 1.70s | 3.318× | 45.75% | PASS |
| ac_writes | `types_delete_insert_ac` | 420.50ms | 964.50ms | 2.294× | 62.29% | PASS |
| ac_writes | `oltp_read_write_ac` | 249.70ms | 569.91ms | 2.282× | 54.77% | PASS |

</details>

## Version-control latency

Wall time: 2m 18s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 82.74ms | 200.00ms | 41.4% | 0.77% | PASS |
| `status_dirty_many_tables` | 85.40ms | 200.00ms | 42.7% | 0.85% | PASS |
| `diff_regular_working_one_table` | 77.55ms | 150.00ms | 51.7% | 0.68% | PASS |
| `diff_regular_working_many_tables` | 89.67ms | 200.00ms | 44.8% | 0.38% | PASS |
| `diff_stat_working_many_tables` | 89.64ms | 200.00ms | 44.8% | 0.40% | PASS |
| `diff_schema_working_many_tables` | 90.80ms | 200.00ms | 45.4% | 0.46% | PASS |
| `branch_list_many_branches` | 22.10ms | 100.00ms | 22.1% | 1.49% | PASS |
| `branch_create_delete` | 24.44ms | 100.00ms | 24.4% | 0.86% | PASS |
| `checkout_branch_clean` | 54.83ms | 200.00ms | 27.4% | 0.73% | PASS |
| `merge_data_no_conflicts` | 28.91ms | 150.00ms | 19.3% | 2.09% | PASS |
| `merge_schema_no_conflicts` | 21.78ms | 100.00ms | 21.8% | 1.80% | PASS |
| `merge_data_conflicts` | 126.46ms | 250.00ms | 50.6% | 0.40% | PASS |
| `merge_data_conflicts_with_resolve` | 126.31ms | 250.00ms | 50.5% | 0.36% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
