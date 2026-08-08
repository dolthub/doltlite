# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-08-08 11:18 UTC
>
> Commit: [`c21cd0075a29caa877beaa2933a5ddc57531d7f0`](https://github.com/dolthub/doltlite/commit/c21cd0075a29caa877beaa2933a5ddc57531d7f0)
>
> Runner: ubuntu24 20260720.247.2
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/31251208941)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians; MAD is the median absolute deviation and describes run-to-run noise.

## SQL workload summary

| Key shape | Workloads | Samples/workload | Wall time | SQLite median total | DoltLite median total | Ratio | Median paired-ratio MAD | Result |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| int | 69 | 55 | 1h 12m 1s | 8.89s | 11.26s | 1.266× | 1.51% | **PASS** |
| textpk | 69 | 55 | 1h 30m 15s | 9.52s | 11.67s | 1.226× | 1.33% | **PASS** |
| blobpk | 69 | 55 | 1h 31m 37s | 9.91s | 11.99s | 1.210× | 1.26% | **PASS** |
| compositepk | 69 | 55 | 1h 26m 39s | 9.82s | 13.95s | 1.420× | 1.56% | **PASS** |

The absolute ceiling is 2.4× per ordinary workload and 1.95× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 23.55ms | 29.53ms | 1.254× | 1.81% | PASS |
| mem_reads | `oltp_range_select` | 10.05ms | 12.41ms | 1.236× | 1.88% | PASS |
| mem_reads | `oltp_sum_range` | 9.40ms | 12.32ms | 1.311× | 1.42% | PASS |
| mem_reads | `oltp_order_range` | 2.50ms | 2.92ms | 1.169× | 1.26% | PASS |
| mem_reads | `oltp_distinct_range` | 3.59ms | 3.96ms | 1.103× | 1.25% | PASS |
| mem_reads | `oltp_index_scan` | 3.84ms | 5.28ms | 1.377× | 1.83% | PASS |
| mem_reads | `select_random_points` | 9.63ms | 11.27ms | 1.170× | 2.52% | PASS |
| mem_reads | `select_random_ranges` | 2.89ms | 4.04ms | 1.398× | 2.19% | PASS |
| mem_reads | `covering_index_scan` | 4.17ms | 4.16ms | 0.998× | 1.42% | PASS |
| mem_reads | `groupby_scan` | 29.56ms | 32.79ms | 1.109× | 0.86% | PASS |
| mem_reads | `index_join` | 6.01ms | 8.03ms | 1.335× | 1.02% | PASS |
| mem_reads | `index_join_scan` | 3.45ms | 4.69ms | 1.358× | 2.03% | PASS |
| mem_reads | `types_table_scan` | 1.04s | 1.29s | 1.236× | 0.61% | PASS |
| mem_reads | `table_scan` | 1.17s | 1.37s | 1.175× | 0.62% | PASS |
| mem_reads | `oltp_read_only` | 99.86ms | 120.99ms | 1.212× | 0.87% | PASS |
| mem_writes | `oltp_bulk_insert` | 177.05ms | 252.54ms | 1.426× | 1.33% | PASS |
| mem_writes | `oltp_insert` | 15.29ms | 28.27ms | 1.849× | 0.95% | PASS |
| mem_writes | `oltp_update_index` | 50.18ms | 105.60ms | 2.105× | 1.89% | PASS |
| mem_writes | `oltp_update_non_index` | 33.35ms | 59.16ms | 1.774× | 1.47% | PASS |
| mem_writes | `oltp_delete_insert` | 44.56ms | 78.87ms | 1.770× | 1.71% | PASS |
| mem_writes | `oltp_write_only` | 21.28ms | 44.57ms | 2.094× | 1.45% | PASS |
| mem_writes | `types_delete_insert` | 23.87ms | 40.21ms | 1.684× | 2.14% | PASS |
| mem_writes | `oltp_read_write` | 65.99ms | 110.45ms | 1.674× | 1.92% | PASS |
| file_reads | `oltp_point_select` | 91.94ms | 53.34ms | 0.580× | 0.90% | PASS |
| file_reads | `oltp_range_select` | 17.62ms | 14.98ms | 0.850× | 1.72% | PASS |
| file_reads | `oltp_sum_range` | 16.95ms | 14.92ms | 0.880× | 0.92% | PASS |
| file_reads | `oltp_order_range` | 3.45ms | 3.32ms | 0.963× | 1.97% | PASS |
| file_reads | `oltp_distinct_range` | 4.49ms | 4.34ms | 0.967× | 1.55% | PASS |
| file_reads | `oltp_index_scan` | 11.22ms | 8.22ms | 0.733× | 1.16% | PASS |
| file_reads | `select_random_points` | 17.99ms | 14.06ms | 0.781× | 1.72% | PASS |
| file_reads | `select_random_ranges` | 9.92ms | 6.52ms | 0.657× | 0.84% | PASS |
| file_reads | `covering_index_scan` | 11.56ms | 7.03ms | 0.608× | 0.83% | PASS |
| file_reads | `groupby_scan` | 30.80ms | 33.58ms | 1.090× | 1.03% | PASS |
| file_reads | `index_join` | 10.25ms | 10.32ms | 1.007× | 1.51% | PASS |
| file_reads | `index_join_scan` | 4.55ms | 5.19ms | 1.139× | 1.95% | PASS |
| file_reads | `types_table_scan` | 1.06s | 1.29s | 1.207× | 1.24% | PASS |
| file_reads | `table_scan` | 1.24s | 1.39s | 1.120× | 1.17% | PASS |
| file_reads | `oltp_read_only` | 206.40ms | 159.36ms | 0.772× | 0.52% | PASS |
| file_writes | `oltp_bulk_insert` | 190.76ms | 272.35ms | 1.428× | 1.15% | PASS |
| file_writes | `oltp_insert` | 24.79ms | 35.59ms | 1.436× | 1.61% | PASS |
| file_writes | `oltp_update_index` | 79.93ms | 131.32ms | 1.643× | 1.76% | PASS |
| file_writes | `oltp_update_non_index` | 61.57ms | 82.53ms | 1.341× | 1.67% | PASS |
| file_writes | `oltp_delete_insert` | 72.70ms | 101.93ms | 1.402× | 1.28% | PASS |
| file_writes | `oltp_write_only` | 50.71ms | 65.30ms | 1.288× | 1.27% | PASS |
| file_writes | `types_delete_insert` | 41.33ms | 54.17ms | 1.311× | 1.57% | PASS |
| file_writes | `oltp_read_write` | 96.26ms | 129.19ms | 1.342× | 1.72% | PASS |
| ac_reads | `oltp_point_select` | 46.38ms | 53.47ms | 1.153× | 1.37% | PASS |
| ac_reads | `oltp_range_select` | 12.83ms | 14.99ms | 1.168× | 1.78% | PASS |
| ac_reads | `oltp_sum_range` | 12.45ms | 15.00ms | 1.205× | 2.58% | PASS |
| ac_reads | `oltp_order_range` | 3.02ms | 3.33ms | 1.104× | 2.47% | PASS |
| ac_reads | `oltp_distinct_range` | 3.98ms | 4.36ms | 1.096× | 1.59% | PASS |
| ac_reads | `oltp_index_scan` | 6.58ms | 8.18ms | 1.242× | 1.42% | PASS |
| ac_reads | `select_random_points` | 13.30ms | 14.02ms | 1.054× | 3.35% | PASS |
| ac_reads | `select_random_ranges` | 5.39ms | 6.48ms | 1.203× | 1.24% | PASS |
| ac_reads | `covering_index_scan` | 6.55ms | 6.67ms | 1.018× | 1.42% | PASS |
| ac_reads | `groupby_scan` | 29.75ms | 33.40ms | 1.123× | 0.60% | PASS |
| ac_reads | `index_join` | 7.34ms | 9.78ms | 1.333× | 1.81% | PASS |
| ac_reads | `index_join_scan` | 3.84ms | 5.10ms | 1.328× | 1.44% | PASS |
| ac_reads | `types_table_scan` | 1.04s | 1.28s | 1.227× | 0.73% | PASS |
| ac_reads | `table_scan` | 1.17s | 1.37s | 1.173× | 0.68% | PASS |
| ac_reads | `oltp_read_only` | 132.73ms | 156.10ms | 1.176× | 0.70% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.63ms | 79.07ms | 3.656× | 4.73% | PASS |
| ac_writes | `oltp_insert_ac` | 23.80ms | 95.53ms | 4.014× | 2.87% | PASS |
| ac_writes | `oltp_update_index_ac` | 25.70ms | 110.59ms | 4.303× | 6.40% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.74ms | 87.97ms | 4.047× | 4.64% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 23.81ms | 101.12ms | 4.248× | 5.53% | PASS |
| ac_writes | `oltp_write_only_ac` | 24.42ms | 98.84ms | 4.047× | 6.39% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.79ms | 92.43ms | 4.056× | 6.93% | PASS |
| ac_writes | `oltp_read_write_ac` | 29.69ms | 109.60ms | 3.691× | 5.06% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 28.31ms | 37.56ms | 1.327× | 1.02% | PASS |
| mem_reads | `oltp_range_select` | 11.86ms | 14.59ms | 1.230× | 1.51% | PASS |
| mem_reads | `oltp_sum_range` | 11.01ms | 13.95ms | 1.267× | 1.10% | PASS |
| mem_reads | `oltp_order_range` | 2.73ms | 3.11ms | 1.139× | 1.12% | PASS |
| mem_reads | `oltp_distinct_range` | 3.80ms | 4.17ms | 1.096× | 0.88% | PASS |
| mem_reads | `oltp_index_scan` | 4.18ms | 6.05ms | 1.448× | 1.35% | PASS |
| mem_reads | `select_random_points` | 16.48ms | 20.58ms | 1.249× | 1.23% | PASS |
| mem_reads | `select_random_ranges` | 3.55ms | 5.21ms | 1.469× | 1.18% | PASS |
| mem_reads | `covering_index_scan` | 4.44ms | 4.35ms | 0.981× | 1.13% | PASS |
| mem_reads | `groupby_scan` | 30.90ms | 34.01ms | 1.101× | 1.23% | PASS |
| mem_reads | `index_join` | 6.71ms | 8.70ms | 1.298× | 1.31% | PASS |
| mem_reads | `index_join_scan` | 4.07ms | 5.34ms | 1.311× | 2.11% | PASS |
| mem_reads | `types_table_scan` | 1.07s | 1.24s | 1.156× | 0.64% | PASS |
| mem_reads | `table_scan` | 1.21s | 1.35s | 1.114× | 0.46% | PASS |
| mem_reads | `oltp_read_only` | 113.19ms | 135.67ms | 1.199× | 0.66% | PASS |
| mem_writes | `oltp_bulk_insert` | 229.38ms | 353.39ms | 1.541× | 0.64% | PASS |
| mem_writes | `oltp_insert` | 21.10ms | 39.25ms | 1.860× | 0.65% | PASS |
| mem_writes | `oltp_update_index` | 68.05ms | 130.06ms | 1.911× | 0.59% | PASS |
| mem_writes | `oltp_update_non_index` | 46.32ms | 85.13ms | 1.838× | 0.63% | PASS |
| mem_writes | `oltp_delete_insert` | 48.80ms | 101.39ms | 2.078× | 0.75% | PASS |
| mem_writes | `oltp_write_only` | 27.38ms | 60.20ms | 2.199× | 0.86% | PASS |
| mem_writes | `types_delete_insert` | 31.35ms | 54.10ms | 1.726× | 0.73% | PASS |
| mem_writes | `oltp_read_write` | 80.91ms | 137.62ms | 1.701× | 0.91% | PASS |
| file_reads | `oltp_point_select` | 96.47ms | 61.83ms | 0.641× | 0.96% | PASS |
| file_reads | `oltp_range_select` | 20.32ms | 17.36ms | 0.854× | 3.24% | PASS |
| file_reads | `oltp_sum_range` | 19.20ms | 16.83ms | 0.877× | 2.42% | PASS |
| file_reads | `oltp_order_range` | 3.74ms | 3.46ms | 0.925× | 3.66% | PASS |
| file_reads | `oltp_distinct_range` | 4.82ms | 4.51ms | 0.936× | 1.90% | PASS |
| file_reads | `oltp_index_scan` | 11.42ms | 8.96ms | 0.784× | 2.65% | PASS |
| file_reads | `select_random_points` | 24.93ms | 24.32ms | 0.975× | 2.07% | PASS |
| file_reads | `select_random_ranges` | 10.95ms | 7.75ms | 0.708× | 1.32% | PASS |
| file_reads | `covering_index_scan` | 12.39ms | 7.15ms | 0.577× | 2.77% | PASS |
| file_reads | `groupby_scan` | 32.56ms | 34.53ms | 1.060× | 1.33% | PASS |
| file_reads | `index_join` | 10.68ms | 11.19ms | 1.048× | 2.88% | PASS |
| file_reads | `index_join_scan` | 5.43ms | 5.93ms | 1.091× | 2.19% | PASS |
| file_reads | `types_table_scan` | 1.07s | 1.23s | 1.152× | 0.79% | PASS |
| file_reads | `table_scan` | 1.22s | 1.35s | 1.111× | 0.56% | PASS |
| file_reads | `oltp_read_only` | 213.51ms | 172.34ms | 0.807× | 1.29% | PASS |
| file_writes | `oltp_bulk_insert` | 247.59ms | 380.12ms | 1.535× | 0.86% | PASS |
| file_writes | `oltp_insert` | 49.50ms | 51.44ms | 1.039× | 10.16% | PASS |
| file_writes | `oltp_update_index` | 110.58ms | 164.53ms | 1.488× | 0.96% | PASS |
| file_writes | `oltp_update_non_index` | 97.19ms | 110.49ms | 1.137× | 6.56% | PASS |
| file_writes | `oltp_delete_insert` | 91.74ms | 131.18ms | 1.430× | 1.51% | PASS |
| file_writes | `oltp_write_only` | 91.63ms | 83.02ms | 0.906× | 14.56% | PASS |
| file_writes | `types_delete_insert` | 56.02ms | 72.92ms | 1.302× | 1.33% | PASS |
| file_writes | `oltp_read_write` | 140.00ms | 159.89ms | 1.142× | 5.91% | PASS |
| ac_reads | `oltp_point_select` | 52.01ms | 61.82ms | 1.189× | 0.73% | PASS |
| ac_reads | `oltp_range_select` | 15.55ms | 17.45ms | 1.122× | 1.39% | PASS |
| ac_reads | `oltp_sum_range` | 14.42ms | 16.75ms | 1.161× | 1.51% | PASS |
| ac_reads | `oltp_order_range` | 3.24ms | 3.45ms | 1.066× | 1.51% | PASS |
| ac_reads | `oltp_distinct_range` | 4.32ms | 4.52ms | 1.047× | 1.23% | PASS |
| ac_reads | `oltp_index_scan` | 6.96ms | 8.92ms | 1.282× | 1.79% | PASS |
| ac_reads | `select_random_points` | 20.55ms | 24.29ms | 1.182× | 1.62% | PASS |
| ac_reads | `select_random_ranges` | 6.31ms | 7.70ms | 1.221× | 1.20% | PASS |
| ac_reads | `covering_index_scan` | 7.26ms | 7.14ms | 0.983× | 4.21% | PASS |
| ac_reads | `groupby_scan` | 31.76ms | 34.46ms | 1.085× | 1.12% | PASS |
| ac_reads | `index_join` | 8.49ms | 11.17ms | 1.316× | 2.46% | PASS |
| ac_reads | `index_join_scan` | 5.17ms | 5.89ms | 1.140× | 2.65% | PASS |
| ac_reads | `types_table_scan` | 1.06s | 1.23s | 1.156× | 0.65% | PASS |
| ac_reads | `table_scan` | 1.22s | 1.35s | 1.107× | 1.08% | PASS |
| ac_reads | `oltp_read_only` | 149.72ms | 172.03ms | 1.149× | 1.23% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.02ms | 76.57ms | 3.643× | 3.64% | PASS |
| ac_writes | `oltp_insert_ac` | 24.06ms | 90.86ms | 3.777× | 5.17% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.30ms | 107.78ms | 3.948× | 5.01% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.51ms | 87.85ms | 3.902× | 5.00% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 23.94ms | 99.74ms | 4.166× | 4.87% | PASS |
| ac_writes | `oltp_write_only_ac` | 23.75ms | 97.52ms | 4.105× | 3.53% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.00ms | 91.73ms | 4.170× | 7.18% | PASS |
| ac_writes | `oltp_read_write_ac` | 30.71ms | 106.02ms | 3.452× | 5.21% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.74ms | 28.34ms | 1.146× | 1.78% | PASS |
| mem_reads | `oltp_range_select` | 11.02ms | 10.98ms | 0.997× | 2.35% | PASS |
| mem_reads | `oltp_sum_range` | 9.90ms | 10.60ms | 1.070× | 1.77% | PASS |
| mem_reads | `oltp_order_range` | 2.45ms | 2.53ms | 1.033× | 1.44% | PASS |
| mem_reads | `oltp_distinct_range` | 3.31ms | 3.32ms | 1.002× | 1.15% | PASS |
| mem_reads | `oltp_index_scan` | 3.72ms | 4.96ms | 1.335× | 2.31% | PASS |
| mem_reads | `select_random_points` | 14.03ms | 16.03ms | 1.143× | 1.62% | PASS |
| mem_reads | `select_random_ranges` | 3.37ms | 4.22ms | 1.250× | 1.17% | PASS |
| mem_reads | `covering_index_scan` | 3.54ms | 3.67ms | 1.038× | 3.52% | PASS |
| mem_reads | `groupby_scan` | 26.71ms | 27.86ms | 1.043× | 0.94% | PASS |
| mem_reads | `index_join` | 5.49ms | 7.26ms | 1.322× | 2.07% | PASS |
| mem_reads | `index_join_scan` | 3.66ms | 4.79ms | 1.309× | 1.93% | PASS |
| mem_reads | `types_table_scan` | 885.02ms | 962.49ms | 1.088× | 0.47% | PASS |
| mem_reads | `table_scan` | 1.03s | 1.05s | 1.014× | 0.86% | PASS |
| mem_reads | `oltp_read_only` | 94.33ms | 101.54ms | 1.076× | 2.43% | PASS |
| mem_writes | `oltp_bulk_insert` | 183.79ms | 261.62ms | 1.423× | 0.72% | PASS |
| mem_writes | `oltp_insert` | 15.95ms | 29.86ms | 1.872× | 1.26% | PASS |
| mem_writes | `oltp_update_index` | 57.02ms | 105.11ms | 1.843× | 1.72% | PASS |
| mem_writes | `oltp_update_non_index` | 40.66ms | 67.24ms | 1.654× | 1.17% | PASS |
| mem_writes | `oltp_delete_insert` | 39.94ms | 80.24ms | 2.009× | 1.13% | PASS |
| mem_writes | `oltp_write_only` | 22.86ms | 48.34ms | 2.114× | 1.49% | PASS |
| mem_writes | `types_delete_insert` | 25.34ms | 39.84ms | 1.572× | 1.12% | PASS |
| mem_writes | `oltp_read_write` | 62.67ms | 102.63ms | 1.638× | 0.77% | PASS |
| file_reads | `oltp_point_select` | 88.55ms | 49.19ms | 0.555× | 0.63% | PASS |
| file_reads | `oltp_range_select` | 17.66ms | 13.01ms | 0.737× | 0.58% | PASS |
| file_reads | `oltp_sum_range` | 16.75ms | 12.75ms | 0.761× | 0.69% | PASS |
| file_reads | `oltp_order_range` | 3.22ms | 2.80ms | 0.869× | 1.02% | PASS |
| file_reads | `oltp_distinct_range` | 4.04ms | 3.56ms | 0.880× | 0.76% | PASS |
| file_reads | `oltp_index_scan` | 10.64ms | 7.24ms | 0.681× | 0.76% | PASS |
| file_reads | `select_random_points` | 21.04ms | 18.06ms | 0.858× | 0.90% | PASS |
| file_reads | `select_random_ranges` | 10.03ms | 6.43ms | 0.641× | 0.92% | PASS |
| file_reads | `covering_index_scan` | 10.91ms | 6.10ms | 0.559× | 0.50% | PASS |
| file_reads | `groupby_scan` | 27.39ms | 28.04ms | 1.024× | 0.64% | PASS |
| file_reads | `index_join` | 9.58ms | 8.96ms | 0.935× | 0.92% | PASS |
| file_reads | `index_join_scan` | 4.41ms | 4.96ms | 1.124× | 0.89% | PASS |
| file_reads | `types_table_scan` | 996.00ms | 987.29ms | 0.991× | 4.11% | PASS |
| file_reads | `table_scan` | 1.03s | 1.05s | 1.016× | 1.46% | PASS |
| file_reads | `oltp_read_only` | 185.48ms | 131.78ms | 0.710× | 0.68% | PASS |
| file_writes | `oltp_bulk_insert` | 269.90ms | 350.79ms | 1.300× | 13.16% | PASS |
| file_writes | `oltp_insert` | 56.00ms | 60.41ms | 1.079× | 23.84% | PASS |
| file_writes | `oltp_update_index` | 190.74ms | 202.55ms | 1.062× | 7.45% | PASS |
| file_writes | `oltp_update_non_index` | 172.46ms | 139.87ms | 0.811× | 23.97% | PASS |
| file_writes | `oltp_delete_insert` | 167.80ms | 157.78ms | 0.940× | 16.86% | PASS |
| file_writes | `oltp_write_only` | 130.92ms | 116.05ms | 0.886× | 20.30% | PASS |
| file_writes | `types_delete_insert` | 109.65ms | 101.87ms | 0.929× | 24.06% | PASS |
| file_writes | `oltp_read_write` | 196.47ms | 181.42ms | 0.923× | 18.87% | PASS |
| ac_reads | `oltp_point_select` | 45.51ms | 49.52ms | 1.088× | 1.06% | PASS |
| ac_reads | `oltp_range_select` | 13.58ms | 13.11ms | 0.965× | 1.37% | PASS |
| ac_reads | `oltp_sum_range` | 12.62ms | 12.81ms | 1.016× | 0.72% | PASS |
| ac_reads | `oltp_order_range` | 2.87ms | 2.80ms | 0.976× | 1.20% | PASS |
| ac_reads | `oltp_distinct_range` | 3.67ms | 3.57ms | 0.973× | 1.22% | PASS |
| ac_reads | `oltp_index_scan` | 6.47ms | 7.35ms | 1.135× | 0.78% | PASS |
| ac_reads | `select_random_points` | 17.75ms | 18.85ms | 1.062× | 1.30% | PASS |
| ac_reads | `select_random_ranges` | 5.94ms | 6.49ms | 1.094× | 0.85% | PASS |
| ac_reads | `covering_index_scan` | 6.81ms | 6.18ms | 0.907× | 0.98% | PASS |
| ac_reads | `groupby_scan` | 27.54ms | 28.37ms | 1.030× | 0.71% | PASS |
| ac_reads | `index_join` | 7.68ms | 9.28ms | 1.207× | 1.61% | PASS |
| ac_reads | `index_join_scan` | 4.20ms | 5.15ms | 1.226× | 1.36% | PASS |
| ac_reads | `types_table_scan` | 1.09s | 1.02s | 0.938× | 1.20% | PASS |
| ac_reads | `table_scan` | 1.34s | 1.12s | 0.836× | 1.04% | PASS |
| ac_reads | `oltp_read_only` | 133.23ms | 136.99ms | 1.028× | 0.94% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 100.91ms | 199.93ms | 1.981× | 51.83% | PASS |
| ac_writes | `oltp_insert_ac` | 94.35ms | 354.20ms | 3.754× | 60.40% | PASS |
| ac_writes | `oltp_update_index_ac` | 81.78ms | 229.49ms | 2.806× | 49.59% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 69.66ms | 223.54ms | 3.209× | 45.96% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 113.44ms | 393.25ms | 3.467× | 60.40% | PASS |
| ac_writes | `oltp_write_only_ac` | 141.66ms | 575.46ms | 4.062× | 54.09% | PASS |
| ac_writes | `types_delete_insert_ac` | 137.96ms | 392.06ms | 2.842× | 47.89% | PASS |
| ac_writes | `oltp_read_write_ac` | 146.95ms | 564.83ms | 3.844× | 63.83% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 20.61ms | 24.48ms | 1.188× | 1.39% | PASS |
| mem_reads | `oltp_range_select` | 14.04ms | 13.57ms | 0.966× | 0.88% | PASS |
| mem_reads | `oltp_sum_range` | 13.68ms | 12.98ms | 0.948× | 1.57% | PASS |
| mem_reads | `oltp_order_range` | 2.63ms | 2.60ms | 0.989× | 1.54% | PASS |
| mem_reads | `oltp_distinct_range` | 3.24ms | 3.21ms | 0.990× | 0.96% | PASS |
| mem_reads | `oltp_index_scan` | 3.16ms | 4.02ms | 1.273× | 1.39% | PASS |
| mem_reads | `select_random_points` | 19.93ms | 21.94ms | 1.101× | 0.73% | PASS |
| mem_reads | `select_random_ranges` | 5.05ms | 5.83ms | 1.155× | 1.27% | PASS |
| mem_reads | `covering_index_scan` | 2.48ms | 2.64ms | 1.062× | 2.19% | PASS |
| mem_reads | `groupby_scan` | 23.31ms | 25.02ms | 1.073× | 0.84% | PASS |
| mem_reads | `index_join` | 5.66ms | 7.25ms | 1.281× | 0.94% | PASS |
| mem_reads | `index_join_scan` | 2.97ms | 4.47ms | 1.507× | 1.86% | PASS |
| mem_reads | `types_table_scan` | 750.37ms | 820.68ms | 1.094× | 0.94% | PASS |
| mem_reads | `table_scan` | 857.71ms | 890.18ms | 1.038× | 1.38% | PASS |
| mem_reads | `oltp_read_only` | 100.15ms | 106.69ms | 1.065× | 2.12% | PASS |
| mem_writes | `oltp_bulk_insert` | 140.59ms | 201.69ms | 1.435× | 0.89% | PASS |
| mem_writes | `oltp_insert` | 11.79ms | 21.86ms | 1.855× | 1.15% | PASS |
| mem_writes | `oltp_update_index` | 46.78ms | 83.02ms | 1.775× | 1.84% | PASS |
| mem_writes | `oltp_update_non_index` | 33.54ms | 55.34ms | 1.650× | 1.35% | PASS |
| mem_writes | `oltp_delete_insert` | 33.57ms | 62.64ms | 1.866× | 1.04% | PASS |
| mem_writes | `oltp_write_only` | 18.35ms | 37.08ms | 2.021× | 1.15% | PASS |
| mem_writes | `types_delete_insert` | 20.50ms | 34.31ms | 1.673× | 1.35% | PASS |
| mem_writes | `oltp_read_write` | 64.24ms | 94.89ms | 1.477× | 1.14% | PASS |
| file_reads | `oltp_point_select` | 42.19ms | 31.92ms | 0.757× | 0.73% | PASS |
| file_reads | `oltp_range_select` | 16.57ms | 14.44ms | 0.871× | 0.85% | PASS |
| file_reads | `oltp_sum_range` | 15.87ms | 13.77ms | 0.868× | 0.82% | PASS |
| file_reads | `oltp_order_range` | 2.84ms | 2.73ms | 0.961× | 1.71% | PASS |
| file_reads | `oltp_distinct_range` | 3.53ms | 3.31ms | 0.937× | 1.14% | PASS |
| file_reads | `oltp_index_scan` | 5.68ms | 5.29ms | 0.931× | 1.78% | PASS |
| file_reads | `select_random_points` | 22.21ms | 23.08ms | 1.039× | 1.17% | PASS |
| file_reads | `select_random_ranges` | 7.46ms | 6.61ms | 0.886× | 1.91% | PASS |
| file_reads | `covering_index_scan` | 4.88ms | 3.82ms | 0.783× | 1.85% | PASS |
| file_reads | `groupby_scan` | 24.08ms | 25.78ms | 1.071× | 0.76% | PASS |
| file_reads | `index_join` | 7.21ms | 8.49ms | 1.179× | 1.56% | PASS |
| file_reads | `index_join_scan` | 3.35ms | 4.74ms | 1.418× | 3.85% | PASS |
| file_reads | `types_table_scan` | 764.26ms | 842.36ms | 1.102× | 1.27% | PASS |
| file_reads | `table_scan` | 859.80ms | 896.59ms | 1.043× | 1.33% | PASS |
| file_reads | `oltp_read_only` | 123.81ms | 112.87ms | 0.912× | 1.54% | PASS |
| file_writes | `oltp_bulk_insert` | 300.78ms | 338.04ms | 1.124× | 33.40% | PASS |
| file_writes | `oltp_insert` | 42.44ms | 84.58ms | 1.993× | 68.39% | PASS |
| file_writes | `oltp_update_index` | 171.03ms | 156.46ms | 0.915× | 21.47% | PASS |
| file_writes | `oltp_update_non_index` | 168.34ms | 120.17ms | 0.714× | 31.96% | PASS |
| file_writes | `oltp_delete_insert` | 205.90ms | 162.72ms | 0.790× | 30.43% | PASS |
| file_writes | `oltp_write_only` | 204.00ms | 145.72ms | 0.714× | 43.81% | PASS |
| file_writes | `types_delete_insert` | 167.93ms | 120.08ms | 0.715× | 51.56% | PASS |
| file_writes | `oltp_read_write` | 277.28ms | 178.41ms | 0.643× | 32.57% | PASS |
| ac_reads | `oltp_point_select` | 28.13ms | 32.55ms | 1.157× | 1.08% | PASS |
| ac_reads | `oltp_range_select` | 15.35ms | 14.68ms | 0.956× | 1.57% | PASS |
| ac_reads | `oltp_sum_range` | 14.81ms | 13.90ms | 0.939× | 0.86% | PASS |
| ac_reads | `oltp_order_range` | 2.71ms | 2.76ms | 1.017× | 1.95% | PASS |
| ac_reads | `oltp_distinct_range` | 3.43ms | 3.37ms | 0.982× | 2.19% | PASS |
| ac_reads | `oltp_index_scan` | 4.33ms | 5.38ms | 1.242× | 2.07% | PASS |
| ac_reads | `select_random_points` | 21.40ms | 23.49ms | 1.098× | 1.61% | PASS |
| ac_reads | `select_random_ranges` | 5.92ms | 6.57ms | 1.110× | 1.34% | PASS |
| ac_reads | `covering_index_scan` | 3.54ms | 3.84ms | 1.084× | 2.42% | PASS |
| ac_reads | `groupby_scan` | 23.69ms | 25.21ms | 1.064× | 1.11% | PASS |
| ac_reads | `index_join` | 6.45ms | 8.26ms | 1.281× | 1.58% | PASS |
| ac_reads | `index_join_scan` | 3.21ms | 4.66ms | 1.450× | 2.65% | PASS |
| ac_reads | `types_table_scan` | 745.86ms | 822.59ms | 1.103× | 0.61% | PASS |
| ac_reads | `table_scan` | 859.58ms | 896.42ms | 1.043× | 1.24% | PASS |
| ac_reads | `oltp_read_only` | 104.28ms | 112.07ms | 1.075× | 0.90% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 172.09ms | 421.55ms | 2.450× | 55.93% | PASS |
| ac_writes | `oltp_insert_ac` | 381.29ms | 973.50ms | 2.553× | 61.51% | PASS |
| ac_writes | `oltp_update_index_ac` | 301.69ms | 1.11s | 3.666× | 56.91% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 233.72ms | 628.40ms | 2.689× | 60.49% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 239.80ms | 715.88ms | 2.985× | 70.47% | PASS |
| ac_writes | `oltp_write_only_ac` | 260.88ms | 738.42ms | 2.830× | 56.51% | PASS |
| ac_writes | `types_delete_insert_ac` | 468.60ms | 837.81ms | 1.788× | 62.39% | PASS |
| ac_writes | `oltp_read_write_ac` | 284.64ms | 710.86ms | 2.497× | 57.62% | PASS |

</details>

## Version-control latency

Wall time: 2m 21s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 83.06ms | 130.00ms | 63.9% | 0.76% | PASS |
| `status_dirty_many_tables` | 86.03ms | 130.00ms | 66.2% | 0.57% | PASS |
| `diff_regular_working_one_table` | 78.28ms | 120.00ms | 65.2% | 0.40% | PASS |
| `diff_regular_working_many_tables` | 90.44ms | 140.00ms | 64.6% | 0.33% | PASS |
| `diff_stat_working_many_tables` | 90.57ms | 140.00ms | 64.7% | 0.37% | PASS |
| `diff_schema_working_many_tables` | 91.03ms | 140.00ms | 65.0% | 0.34% | PASS |
| `branch_list_many_branches` | 23.27ms | 35.00ms | 66.5% | 1.10% | PASS |
| `branch_create_delete` | 25.43ms | 40.00ms | 63.6% | 1.06% | PASS |
| `checkout_branch_clean` | 55.60ms | 150.00ms | 37.1% | 0.71% | PASS |
| `merge_data_no_conflicts` | 29.83ms | 50.00ms | 59.7% | 1.58% | PASS |
| `merge_schema_no_conflicts` | 22.58ms | 35.00ms | 64.5% | 0.96% | PASS |
| `merge_data_conflicts` | 128.13ms | 180.00ms | 71.2% | 0.22% | PASS |
| `merge_data_conflicts_with_resolve` | 128.00ms | 180.00ms | 71.1% | 0.22% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
