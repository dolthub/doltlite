# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-08-07 11:27 UTC
>
> Commit: [`c741ef549a0c986248e405c3eee0625ff3b55613`](https://github.com/dolthub/doltlite/commit/c741ef549a0c986248e405c3eee0625ff3b55613)
>
> Runner: ubuntu24 20260720.247.2
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/31167385838)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians; MAD is the median absolute deviation and describes run-to-run noise.

## SQL workload summary

| Key shape | Workloads | Samples/workload | Wall time | SQLite median total | DoltLite median total | Ratio | Median paired-ratio MAD | Result |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| int | 69 | 55 | 1h 12m 40s | 9.53s | 11.13s | 1.168× | 1.26% | **PASS** |
| textpk | 69 | 55 | 1h 35m 41s | 10.92s | 12.21s | 1.118× | 3.26% | **PASS** |
| blobpk | 69 | 55 | 1h 29m 19s | 10.10s | 11.56s | 1.144× | 1.39% | **PASS** |
| compositepk | 69 | 55 | 1h 18m 22s | 8.41s | 9.90s | 1.176× | 1.03% | **PASS** |

The absolute ceiling is 2.5× per ordinary workload and 2.0× for a section average. Durable autocommit writes use 10.0× and 5.0× ceilings respectively.

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 26.00ms | 28.64ms | 1.102× | 1.82% | PASS |
| mem_reads | `oltp_range_select` | 11.17ms | 12.14ms | 1.087× | 2.38% | PASS |
| mem_reads | `oltp_sum_range` | 9.80ms | 11.44ms | 1.167× | 1.88% | PASS |
| mem_reads | `oltp_order_range` | 2.68ms | 2.88ms | 1.075× | 1.14% | PASS |
| mem_reads | `oltp_distinct_range` | 3.80ms | 3.88ms | 1.022× | 1.07% | PASS |
| mem_reads | `oltp_index_scan` | 4.01ms | 4.93ms | 1.228× | 1.83% | PASS |
| mem_reads | `select_random_points` | 10.74ms | 11.24ms | 1.046× | 1.97% | PASS |
| mem_reads | `select_random_ranges` | 3.15ms | 4.02ms | 1.276× | 2.01% | PASS |
| mem_reads | `covering_index_scan` | 4.37ms | 4.13ms | 0.945× | 1.54% | PASS |
| mem_reads | `groupby_scan` | 32.55ms | 34.37ms | 1.056× | 0.66% | PASS |
| mem_reads | `index_join` | 5.96ms | 7.90ms | 1.325× | 1.71% | PASS |
| mem_reads | `index_join_scan` | 3.55ms | 4.76ms | 1.340× | 2.16% | PASS |
| mem_reads | `types_table_scan` | 1.13s | 1.28s | 1.132× | 0.99% | PASS |
| mem_reads | `table_scan` | 1.30s | 1.39s | 1.071× | 1.29% | PASS |
| mem_reads | `oltp_read_only` | 106.09ms | 115.49ms | 1.089× | 1.20% | PASS |
| mem_writes | `oltp_bulk_insert` | 182.13ms | 242.16ms | 1.330× | 0.78% | PASS |
| mem_writes | `oltp_insert` | 15.97ms | 28.44ms | 1.781× | 1.10% | PASS |
| mem_writes | `oltp_update_index` | 52.19ms | 107.45ms | 2.059× | 1.23% | PASS |
| mem_writes | `oltp_update_non_index` | 36.09ms | 59.76ms | 1.656× | 1.70% | PASS |
| mem_writes | `oltp_delete_insert` | 45.69ms | 80.05ms | 1.752× | 0.98% | PASS |
| mem_writes | `oltp_write_only` | 22.29ms | 45.85ms | 2.057× | 1.11% | PASS |
| mem_writes | `types_delete_insert` | 25.22ms | 40.45ms | 1.604× | 1.10% | PASS |
| mem_writes | `oltp_read_write` | 67.49ms | 106.28ms | 1.575× | 1.11% | PASS |
| file_reads | `oltp_point_select` | 107.93ms | 55.72ms | 0.516× | 0.96% | PASS |
| file_reads | `oltp_range_select` | 19.48ms | 14.90ms | 0.765× | 1.66% | PASS |
| file_reads | `oltp_sum_range` | 18.50ms | 14.55ms | 0.786× | 1.12% | PASS |
| file_reads | `oltp_order_range` | 3.60ms | 3.23ms | 0.898× | 1.32% | PASS |
| file_reads | `oltp_distinct_range` | 4.65ms | 4.20ms | 0.904× | 1.33% | PASS |
| file_reads | `oltp_index_scan` | 12.36ms | 8.11ms | 0.656× | 1.87% | PASS |
| file_reads | `select_random_points` | 19.25ms | 14.27ms | 0.741× | 2.43% | PASS |
| file_reads | `select_random_ranges` | 11.37ms | 6.85ms | 0.603× | 1.17% | PASS |
| file_reads | `covering_index_scan` | 12.90ms | 7.24ms | 0.561× | 1.21% | PASS |
| file_reads | `groupby_scan` | 33.45ms | 34.73ms | 1.038× | 0.76% | PASS |
| file_reads | `index_join` | 10.42ms | 9.99ms | 0.959× | 1.50% | PASS |
| file_reads | `index_join_scan` | 4.46ms | 5.12ms | 1.148× | 2.43% | PASS |
| file_reads | `types_table_scan` | 1.12s | 1.28s | 1.137× | 1.10% | PASS |
| file_reads | `table_scan` | 1.31s | 1.40s | 1.068× | 1.55% | PASS |
| file_reads | `oltp_read_only` | 229.28ms | 158.92ms | 0.693× | 0.58% | PASS |
| file_writes | `oltp_bulk_insert` | 196.25ms | 260.81ms | 1.329× | 0.77% | PASS |
| file_writes | `oltp_insert` | 22.02ms | 35.78ms | 1.625× | 1.26% | PASS |
| file_writes | `oltp_update_index` | 77.45ms | 128.95ms | 1.665× | 1.19% | PASS |
| file_writes | `oltp_update_non_index` | 57.85ms | 81.06ms | 1.401× | 1.54% | PASS |
| file_writes | `oltp_delete_insert` | 66.70ms | 98.44ms | 1.476× | 1.15% | PASS |
| file_writes | `oltp_write_only` | 42.16ms | 64.95ms | 1.540× | 1.04% | PASS |
| file_writes | `types_delete_insert` | 39.66ms | 53.18ms | 1.341× | 1.31% | PASS |
| file_writes | `oltp_read_write` | 90.38ms | 126.61ms | 1.401× | 1.26% | PASS |
| ac_reads | `oltp_point_select` | 52.37ms | 55.92ms | 1.068× | 0.91% | PASS |
| ac_reads | `oltp_range_select` | 13.88ms | 14.91ms | 1.075× | 2.04% | PASS |
| ac_reads | `oltp_sum_range` | 12.87ms | 14.48ms | 1.125× | 1.62% | PASS |
| ac_reads | `oltp_order_range` | 3.08ms | 3.22ms | 1.045× | 1.73% | PASS |
| ac_reads | `oltp_distinct_range` | 4.12ms | 4.20ms | 1.019× | 0.99% | PASS |
| ac_reads | `oltp_index_scan` | 7.13ms | 8.23ms | 1.154× | 1.16% | PASS |
| ac_reads | `select_random_points` | 14.36ms | 14.52ms | 1.011× | 2.12% | PASS |
| ac_reads | `select_random_ranges` | 6.02ms | 6.88ms | 1.143× | 1.21% | PASS |
| ac_reads | `covering_index_scan` | 7.63ms | 7.40ms | 0.970× | 1.08% | PASS |
| ac_reads | `groupby_scan` | 33.26ms | 34.93ms | 1.050× | 0.64% | PASS |
| ac_reads | `index_join` | 8.00ms | 10.25ms | 1.281× | 1.23% | PASS |
| ac_reads | `index_join_scan` | 4.00ms | 5.15ms | 1.288× | 1.95% | PASS |
| ac_reads | `types_table_scan` | 1.11s | 1.27s | 1.144× | 1.01% | PASS |
| ac_reads | `table_scan` | 1.36s | 1.41s | 1.038× | 0.97% | PASS |
| ac_reads | `oltp_read_only` | 147.12ms | 157.05ms | 1.067× | 0.93% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 15.43ms | 60.62ms | 3.928× | 3.34% | PASS |
| ac_writes | `oltp_insert_ac` | 17.33ms | 77.45ms | 4.468× | 3.56% | PASS |
| ac_writes | `oltp_update_index_ac` | 18.72ms | 92.95ms | 4.965× | 3.21% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 15.32ms | 70.11ms | 4.575× | 4.24% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 17.18ms | 83.84ms | 4.879× | 4.01% | PASS |
| ac_writes | `oltp_write_only_ac` | 17.07ms | 81.09ms | 4.751× | 2.63% | PASS |
| ac_writes | `types_delete_insert_ac` | 14.99ms | 71.35ms | 4.761× | 4.32% | PASS |
| ac_writes | `oltp_read_write_ac` | 22.08ms | 89.13ms | 4.037× | 2.28% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 31.98ms | 38.24ms | 1.196× | 3.10% | PASS |
| mem_reads | `oltp_range_select` | 15.49ms | 14.48ms | 0.935× | 3.12% | PASS |
| mem_reads | `oltp_sum_range` | 13.67ms | 14.60ms | 1.068× | 2.86% | PASS |
| mem_reads | `oltp_order_range` | 3.18ms | 3.23ms | 1.014× | 1.77% | PASS |
| mem_reads | `oltp_distinct_range` | 4.16ms | 4.28ms | 1.030× | 1.58% | PASS |
| mem_reads | `oltp_index_scan` | 4.87ms | 6.48ms | 1.330× | 3.12% | PASS |
| mem_reads | `select_random_points` | 19.99ms | 21.54ms | 1.078× | 3.48% | PASS |
| mem_reads | `select_random_ranges` | 4.41ms | 5.27ms | 1.196× | 2.84% | PASS |
| mem_reads | `covering_index_scan` | 5.65ms | 4.84ms | 0.857× | 6.66% | PASS |
| mem_reads | `groupby_scan` | 32.89ms | 34.48ms | 1.048× | 1.10% | PASS |
| mem_reads | `index_join` | 7.56ms | 9.79ms | 1.295× | 6.01% | PASS |
| mem_reads | `index_join_scan` | 5.48ms | 5.51ms | 1.007× | 4.10% | PASS |
| mem_reads | `types_table_scan` | 1.20s | 1.28s | 1.065× | 4.34% | PASS |
| mem_reads | `table_scan` | 1.50s | 1.43s | 0.950× | 3.26% | PASS |
| mem_reads | `oltp_read_only` | 134.04ms | 141.64ms | 1.057× | 3.69% | PASS |
| mem_writes | `oltp_bulk_insert` | 236.09ms | 360.63ms | 1.528× | 1.14% | PASS |
| mem_writes | `oltp_insert` | 23.23ms | 40.22ms | 1.732× | 4.81% | PASS |
| mem_writes | `oltp_update_index` | 78.67ms | 139.44ms | 1.773× | 4.18% | PASS |
| mem_writes | `oltp_update_non_index` | 52.49ms | 90.76ms | 1.729× | 1.79% | PASS |
| mem_writes | `oltp_delete_insert` | 54.21ms | 106.42ms | 1.963× | 3.30% | PASS |
| mem_writes | `oltp_write_only` | 32.31ms | 64.13ms | 1.985× | 4.59% | PASS |
| mem_writes | `types_delete_insert` | 35.09ms | 56.86ms | 1.621× | 2.46% | PASS |
| mem_writes | `oltp_read_write` | 96.96ms | 145.26ms | 1.498× | 2.91% | PASS |
| file_reads | `oltp_point_select` | 102.29ms | 63.32ms | 0.619× | 1.39% | PASS |
| file_reads | `oltp_range_select` | 22.65ms | 17.27ms | 0.762× | 2.45% | PASS |
| file_reads | `oltp_sum_range` | 21.36ms | 17.32ms | 0.811× | 2.71% | PASS |
| file_reads | `oltp_order_range` | 4.04ms | 3.54ms | 0.876× | 2.71% | PASS |
| file_reads | `oltp_distinct_range` | 5.08ms | 4.62ms | 0.909× | 1.56% | PASS |
| file_reads | `oltp_index_scan` | 12.22ms | 9.22ms | 0.754× | 2.68% | PASS |
| file_reads | `select_random_points` | 28.29ms | 24.95ms | 0.882× | 4.15% | PASS |
| file_reads | `select_random_ranges` | 11.46ms | 7.78ms | 0.679× | 1.93% | PASS |
| file_reads | `covering_index_scan` | 13.48ms | 7.36ms | 0.546× | 3.68% | PASS |
| file_reads | `groupby_scan` | 33.58ms | 34.86ms | 1.038× | 0.89% | PASS |
| file_reads | `index_join` | 12.32ms | 11.51ms | 0.935× | 4.67% | PASS |
| file_reads | `index_join_scan` | 6.35ms | 5.97ms | 0.941× | 6.44% | PASS |
| file_reads | `types_table_scan` | 1.16s | 1.27s | 1.088× | 5.35% | PASS |
| file_reads | `table_scan` | 1.46s | 1.41s | 0.966× | 5.44% | PASS |
| file_reads | `oltp_read_only` | 236.24ms | 178.74ms | 0.757× | 1.98% | PASS |
| file_writes | `oltp_bulk_insert` | 257.33ms | 390.58ms | 1.518× | 1.26% | PASS |
| file_writes | `oltp_insert` | 66.87ms | 53.04ms | 0.793× | 15.23% | PASS |
| file_writes | `oltp_update_index` | 125.22ms | 176.17ms | 1.407× | 3.07% | PASS |
| file_writes | `oltp_update_non_index` | 108.96ms | 115.56ms | 1.061× | 11.89% | PASS |
| file_writes | `oltp_delete_insert` | 94.40ms | 137.81ms | 1.460× | 3.01% | PASS |
| file_writes | `oltp_write_only` | 84.00ms | 87.05ms | 1.036× | 13.74% | PASS |
| file_writes | `types_delete_insert` | 58.49ms | 75.99ms | 1.299× | 1.66% | PASS |
| file_writes | `oltp_read_write` | 148.01ms | 169.52ms | 1.145× | 7.61% | PASS |
| ac_reads | `oltp_point_select` | 56.13ms | 63.59ms | 1.133× | 1.79% | PASS |
| ac_reads | `oltp_range_select` | 18.01ms | 17.26ms | 0.958× | 2.35% | PASS |
| ac_reads | `oltp_sum_range` | 16.07ms | 17.22ms | 1.071× | 2.71% | PASS |
| ac_reads | `oltp_order_range` | 3.52ms | 3.55ms | 1.008× | 1.70% | PASS |
| ac_reads | `oltp_distinct_range` | 4.48ms | 4.60ms | 1.028× | 1.56% | PASS |
| ac_reads | `oltp_index_scan` | 7.68ms | 9.20ms | 1.198× | 3.16% | PASS |
| ac_reads | `select_random_points` | 23.36ms | 24.83ms | 1.063× | 3.76% | PASS |
| ac_reads | `select_random_ranges` | 6.86ms | 7.79ms | 1.135× | 2.47% | PASS |
| ac_reads | `covering_index_scan` | 8.84ms | 7.31ms | 0.827× | 3.35% | PASS |
| ac_reads | `groupby_scan` | 33.08ms | 34.91ms | 1.055× | 1.28% | PASS |
| ac_reads | `index_join` | 10.43ms | 11.47ms | 1.100× | 5.47% | PASS |
| ac_reads | `index_join_scan` | 5.77ms | 6.05ms | 1.048× | 4.92% | PASS |
| ac_reads | `types_table_scan` | 1.20s | 1.27s | 1.060× | 4.27% | PASS |
| ac_reads | `table_scan` | 1.48s | 1.43s | 0.970× | 3.96% | PASS |
| ac_reads | `oltp_read_only` | 166.25ms | 179.25ms | 1.078× | 2.78% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 23.98ms | 83.31ms | 3.474× | 6.07% | PASS |
| ac_writes | `oltp_insert_ac` | 26.39ms | 102.61ms | 3.888× | 6.44% | PASS |
| ac_writes | `oltp_update_index_ac` | 29.39ms | 120.16ms | 4.089× | 5.66% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.36ms | 96.56ms | 4.133× | 6.07% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.18ms | 107.50ms | 4.269× | 4.08% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.04ms | 106.32ms | 4.084× | 5.66% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.13ms | 100.89ms | 4.559× | 9.02% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.96ms | 115.89ms | 3.516× | 5.74% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 31.13ms | 35.07ms | 1.126× | 1.95% | PASS |
| mem_reads | `oltp_range_select` | 14.57ms | 13.73ms | 0.943× | 2.63% | PASS |
| mem_reads | `oltp_sum_range` | 12.97ms | 13.38ms | 1.031× | 2.63% | PASS |
| mem_reads | `oltp_order_range` | 3.13ms | 3.14ms | 1.005× | 1.12% | PASS |
| mem_reads | `oltp_distinct_range` | 4.15ms | 4.15ms | 1.001× | 0.99% | PASS |
| mem_reads | `oltp_index_scan` | 4.71ms | 6.03ms | 1.281× | 2.02% | PASS |
| mem_reads | `select_random_points` | 18.77ms | 20.11ms | 1.071× | 3.48% | PASS |
| mem_reads | `select_random_ranges` | 4.39ms | 5.28ms | 1.203× | 1.44% | PASS |
| mem_reads | `covering_index_scan` | 4.76ms | 4.75ms | 0.998× | 2.05% | PASS |
| mem_reads | `groupby_scan` | 34.47ms | 35.30ms | 1.024× | 0.83% | PASS |
| mem_reads | `index_join` | 7.17ms | 9.70ms | 1.352× | 2.62% | PASS |
| mem_reads | `index_join_scan` | 4.35ms | 5.48ms | 1.260× | 1.39% | PASS |
| mem_reads | `types_table_scan` | 1.16s | 1.26s | 1.084× | 2.84% | PASS |
| mem_reads | `table_scan` | 1.53s | 1.41s | 0.926× | 2.24% | PASS |
| mem_reads | `oltp_read_only` | 122.88ms | 129.43ms | 1.053× | 1.38% | PASS |
| mem_writes | `oltp_bulk_insert` | 237.12ms | 331.48ms | 1.398× | 0.90% | PASS |
| mem_writes | `oltp_insert` | 20.54ms | 39.11ms | 1.904× | 0.98% | PASS |
| mem_writes | `oltp_update_index` | 68.78ms | 129.60ms | 1.884× | 1.01% | PASS |
| mem_writes | `oltp_update_non_index` | 48.89ms | 84.18ms | 1.722× | 1.35% | PASS |
| mem_writes | `oltp_delete_insert` | 49.04ms | 102.23ms | 2.085× | 0.93% | PASS |
| mem_writes | `oltp_write_only` | 27.80ms | 62.38ms | 2.243× | 0.54% | PASS |
| mem_writes | `types_delete_insert` | 31.65ms | 51.95ms | 1.642× | 0.65% | PASS |
| mem_writes | `oltp_read_write` | 80.08ms | 132.68ms | 1.657× | 0.64% | PASS |
| file_reads | `oltp_point_select` | 114.15ms | 63.04ms | 0.552× | 1.22% | PASS |
| file_reads | `oltp_range_select` | 21.47ms | 16.51ms | 0.769× | 3.26% | PASS |
| file_reads | `oltp_sum_range` | 20.64ms | 16.12ms | 0.781× | 2.44% | PASS |
| file_reads | `oltp_order_range` | 3.98ms | 3.45ms | 0.868× | 2.87% | PASS |
| file_reads | `oltp_distinct_range` | 4.99ms | 4.47ms | 0.895× | 1.55% | PASS |
| file_reads | `oltp_index_scan` | 13.05ms | 8.96ms | 0.687× | 2.44% | PASS |
| file_reads | `select_random_points` | 26.20ms | 22.54ms | 0.860× | 2.18% | PASS |
| file_reads | `select_random_ranges` | 12.71ms | 8.12ms | 0.639× | 0.98% | PASS |
| file_reads | `covering_index_scan` | 13.75ms | 7.57ms | 0.550× | 2.18% | PASS |
| file_reads | `groupby_scan` | 34.08ms | 35.31ms | 1.036× | 0.71% | PASS |
| file_reads | `index_join` | 11.52ms | 11.01ms | 0.956× | 1.83% | PASS |
| file_reads | `index_join_scan` | 5.33ms | 6.06ms | 1.135× | 1.83% | PASS |
| file_reads | `types_table_scan` | 1.11s | 1.23s | 1.106× | 0.36% | PASS |
| file_reads | `table_scan` | 1.29s | 1.35s | 1.046× | 0.34% | PASS |
| file_reads | `oltp_read_only` | 239.85ms | 168.97ms | 0.705× | 0.49% | PASS |
| file_writes | `oltp_bulk_insert` | 258.65ms | 355.24ms | 1.373× | 0.81% | PASS |
| file_writes | `oltp_insert` | 30.78ms | 51.02ms | 1.658× | 1.25% | PASS |
| file_writes | `oltp_update_index` | 102.86ms | 163.15ms | 1.586× | 0.85% | PASS |
| file_writes | `oltp_update_non_index` | 77.38ms | 107.42ms | 1.388× | 0.94% | PASS |
| file_writes | `oltp_delete_insert` | 80.22ms | 130.01ms | 1.621× | 1.99% | PASS |
| file_writes | `oltp_write_only` | 53.38ms | 84.89ms | 1.590× | 2.08% | PASS |
| file_writes | `types_delete_insert` | 50.51ms | 70.04ms | 1.387× | 1.90% | PASS |
| file_writes | `oltp_read_write` | 108.18ms | 154.79ms | 1.431× | 1.18% | PASS |
| ac_reads | `oltp_point_select` | 58.00ms | 63.14ms | 1.089× | 0.86% | PASS |
| ac_reads | `oltp_range_select` | 16.50ms | 16.55ms | 1.003× | 1.46% | PASS |
| ac_reads | `oltp_sum_range` | 15.45ms | 16.12ms | 1.044× | 2.08% | PASS |
| ac_reads | `oltp_order_range` | 3.41ms | 3.47ms | 1.018× | 1.41% | PASS |
| ac_reads | `oltp_distinct_range` | 4.40ms | 4.46ms | 1.013× | 0.81% | PASS |
| ac_reads | `oltp_index_scan` | 7.64ms | 8.99ms | 1.178× | 1.08% | PASS |
| ac_reads | `select_random_points` | 20.85ms | 22.63ms | 1.085× | 0.98% | PASS |
| ac_reads | `select_random_ranges` | 7.04ms | 8.16ms | 1.159× | 1.04% | PASS |
| ac_reads | `covering_index_scan` | 7.96ms | 7.55ms | 0.948× | 0.72% | PASS |
| ac_reads | `groupby_scan` | 33.86ms | 35.32ms | 1.043× | 0.46% | PASS |
| ac_reads | `index_join` | 8.90ms | 11.07ms | 1.243× | 1.02% | PASS |
| ac_reads | `index_join_scan` | 4.85ms | 5.96ms | 1.228× | 1.57% | PASS |
| ac_reads | `types_table_scan` | 1.11s | 1.24s | 1.108× | 0.60% | PASS |
| ac_reads | `table_scan` | 1.29s | 1.35s | 1.042× | 0.54% | PASS |
| ac_reads | `oltp_read_only` | 159.73ms | 169.04ms | 1.058× | 0.75% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 15.77ms | 61.05ms | 3.870× | 3.44% | PASS |
| ac_writes | `oltp_insert_ac` | 17.64ms | 82.98ms | 4.704× | 3.67% | PASS |
| ac_writes | `oltp_update_index_ac` | 19.43ms | 93.67ms | 4.822× | 3.48% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 15.99ms | 72.42ms | 4.528× | 4.43% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 17.57ms | 83.94ms | 4.777× | 3.23% | PASS |
| ac_writes | `oltp_write_only_ac` | 17.16ms | 83.05ms | 4.838× | 3.83% | PASS |
| ac_writes | `types_delete_insert_ac` | 15.55ms | 73.32ms | 4.716× | 3.76% | PASS |
| ac_writes | `oltp_read_write_ac` | 22.86ms | 90.61ms | 3.964× | 1.96% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 26.11ms | 29.47ms | 1.129× | 1.76% | PASS |
| mem_reads | `oltp_range_select` | 15.56ms | 16.25ms | 1.044× | 1.06% | PASS |
| mem_reads | `oltp_sum_range` | 14.27ms | 14.92ms | 1.045× | 0.69% | PASS |
| mem_reads | `oltp_order_range` | 2.93ms | 3.06ms | 1.043× | 0.88% | PASS |
| mem_reads | `oltp_distinct_range` | 3.77ms | 3.85ms | 1.023× | 1.03% | PASS |
| mem_reads | `oltp_index_scan` | 3.69ms | 4.64ms | 1.258× | 1.58% | PASS |
| mem_reads | `select_random_points` | 21.45ms | 23.86ms | 1.112× | 0.63% | PASS |
| mem_reads | `select_random_ranges` | 5.95ms | 6.45ms | 1.085× | 1.55% | PASS |
| mem_reads | `covering_index_scan` | 3.39ms | 3.30ms | 0.974× | 1.48% | PASS |
| mem_reads | `groupby_scan` | 30.06ms | 31.14ms | 1.036× | 0.58% | PASS |
| mem_reads | `index_join` | 6.43ms | 7.79ms | 1.211× | 0.87% | PASS |
| mem_reads | `index_join_scan` | 3.39ms | 4.54ms | 1.338× | 0.88% | PASS |
| mem_reads | `types_table_scan` | 870.94ms | 970.52ms | 1.114× | 0.54% | PASS |
| mem_reads | `table_scan` | 998.30ms | 1.05s | 1.056× | 0.44% | PASS |
| mem_reads | `oltp_read_only` | 116.92ms | 125.35ms | 1.072× | 0.78% | PASS |
| mem_writes | `oltp_bulk_insert` | 191.28ms | 258.32ms | 1.350× | 0.83% | PASS |
| mem_writes | `oltp_insert` | 15.15ms | 27.08ms | 1.787× | 0.40% | PASS |
| mem_writes | `oltp_update_index` | 53.35ms | 89.11ms | 1.670× | 0.93% | PASS |
| mem_writes | `oltp_update_non_index` | 40.58ms | 63.19ms | 1.557× | 1.02% | PASS |
| mem_writes | `oltp_delete_insert` | 39.16ms | 72.14ms | 1.842× | 0.79% | PASS |
| mem_writes | `oltp_write_only` | 21.50ms | 43.98ms | 2.045× | 1.05% | PASS |
| mem_writes | `types_delete_insert` | 25.46ms | 39.93ms | 1.569× | 1.21% | PASS |
| mem_writes | `oltp_read_write` | 77.30ms | 112.14ms | 1.451× | 1.03% | PASS |
| file_reads | `oltp_point_select` | 91.11ms | 51.73ms | 0.568× | 1.02% | PASS |
| file_reads | `oltp_range_select` | 22.65ms | 18.26ms | 0.806× | 1.55% | PASS |
| file_reads | `oltp_sum_range` | 20.94ms | 17.24ms | 0.824× | 1.14% | PASS |
| file_reads | `oltp_order_range` | 3.71ms | 3.33ms | 0.899× | 1.59% | PASS |
| file_reads | `oltp_distinct_range` | 4.48ms | 4.16ms | 0.927× | 1.65% | PASS |
| file_reads | `oltp_index_scan` | 10.51ms | 7.23ms | 0.688× | 1.86% | PASS |
| file_reads | `select_random_points` | 28.72ms | 26.26ms | 0.914× | 1.02% | PASS |
| file_reads | `select_random_ranges` | 12.57ms | 8.90ms | 0.708× | 1.12% | PASS |
| file_reads | `covering_index_scan` | 10.10ms | 5.89ms | 0.583× | 1.48% | PASS |
| file_reads | `groupby_scan` | 30.99ms | 31.51ms | 1.017× | 0.95% | PASS |
| file_reads | `index_join` | 10.13ms | 9.86ms | 0.973× | 1.24% | PASS |
| file_reads | `index_join_scan` | 4.17ms | 4.93ms | 1.182× | 1.07% | PASS |
| file_reads | `types_table_scan` | 862.39ms | 964.36ms | 1.118× | 0.49% | PASS |
| file_reads | `table_scan` | 994.45ms | 1.05s | 1.055× | 0.61% | PASS |
| file_reads | `oltp_read_only` | 211.29ms | 157.49ms | 0.745× | 0.95% | PASS |
| file_writes | `oltp_bulk_insert` | 249.29ms | 318.20ms | 1.276× | 5.11% | PASS |
| file_writes | `oltp_insert` | 29.49ms | 48.07ms | 1.630× | 37.37% | PASS |
| file_writes | `oltp_update_index` | 151.99ms | 159.04ms | 1.046× | 3.19% | PASS |
| file_writes | `oltp_update_non_index` | 130.16ms | 118.77ms | 0.913× | 2.65% | PASS |
| file_writes | `oltp_delete_insert` | 130.33ms | 129.56ms | 0.994× | 2.03% | PASS |
| file_writes | `oltp_write_only` | 96.26ms | 99.06ms | 1.029× | 7.15% | PASS |
| file_writes | `types_delete_insert` | 80.00ms | 88.62ms | 1.108× | 14.86% | PASS |
| file_writes | `oltp_read_write` | 142.00ms | 166.62ms | 1.173× | 4.19% | PASS |
| ac_reads | `oltp_point_select` | 47.86ms | 52.02ms | 1.087× | 1.18% | PASS |
| ac_reads | `oltp_range_select` | 18.45ms | 18.28ms | 0.991× | 0.86% | PASS |
| ac_reads | `oltp_sum_range` | 16.86ms | 17.29ms | 1.026× | 0.74% | PASS |
| ac_reads | `oltp_order_range` | 3.37ms | 3.33ms | 0.988× | 0.77% | PASS |
| ac_reads | `oltp_distinct_range` | 4.17ms | 4.15ms | 0.996× | 0.75% | PASS |
| ac_reads | `oltp_index_scan` | 6.31ms | 7.24ms | 1.146× | 0.83% | PASS |
| ac_reads | `select_random_points` | 24.38ms | 26.24ms | 1.076× | 0.65% | PASS |
| ac_reads | `select_random_ranges` | 8.49ms | 8.86ms | 1.043× | 0.70% | PASS |
| ac_reads | `covering_index_scan` | 6.00ms | 5.91ms | 0.986× | 0.69% | PASS |
| ac_reads | `groupby_scan` | 30.44ms | 31.54ms | 1.036× | 0.59% | PASS |
| ac_reads | `index_join` | 8.10ms | 9.90ms | 1.223× | 1.14% | PASS |
| ac_reads | `index_join_scan` | 3.82ms | 4.92ms | 1.288× | 0.81% | PASS |
| ac_reads | `types_table_scan` | 862.52ms | 963.05ms | 1.117× | 0.50% | PASS |
| ac_reads | `table_scan` | 996.41ms | 1.05s | 1.053× | 0.41% | PASS |
| ac_reads | `oltp_read_only` | 149.00ms | 157.22ms | 1.055× | 0.54% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 34.04ms | 101.54ms | 2.983× | 19.55% | PASS |
| ac_writes | `oltp_insert_ac` | 36.02ms | 104.25ms | 2.894× | 29.01% | PASS |
| ac_writes | `oltp_update_index_ac` | 53.23ms | 204.74ms | 3.846× | 43.60% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 31.41ms | 106.74ms | 3.398× | 18.83% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 33.51ms | 129.51ms | 3.865× | 26.99% | PASS |
| ac_writes | `oltp_write_only_ac` | 37.09ms | 139.04ms | 3.749× | 28.92% | PASS |
| ac_writes | `types_delete_insert_ac` | 48.08ms | 130.29ms | 2.710× | 42.30% | PASS |
| ac_writes | `oltp_read_write_ac` | 38.97ms | 127.04ms | 3.260× | 27.04% | PASS |

</details>

## Version-control latency

Wall time: 2m 26s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 90.12ms | 200.00ms | 45.1% | 0.17% | PASS |
| `status_dirty_many_tables` | 93.30ms | 200.00ms | 46.6% | 0.19% | PASS |
| `diff_regular_working_one_table` | 85.66ms | 150.00ms | 57.1% | 0.25% | PASS |
| `diff_regular_working_many_tables` | 98.87ms | 200.00ms | 49.4% | 0.23% | PASS |
| `diff_stat_working_many_tables` | 98.82ms | 200.00ms | 49.4% | 0.25% | PASS |
| `diff_schema_working_many_tables` | 99.16ms | 200.00ms | 49.6% | 0.17% | PASS |
| `branch_list_many_branches` | 24.25ms | 100.00ms | 24.2% | 1.77% | PASS |
| `branch_create_delete` | 26.02ms | 100.00ms | 26.0% | 1.56% | PASS |
| `checkout_branch_clean` | 58.16ms | 200.00ms | 29.1% | 0.55% | PASS |
| `merge_data_no_conflicts` | 30.40ms | 150.00ms | 20.3% | 0.72% | PASS |
| `merge_schema_no_conflicts` | 22.78ms | 100.00ms | 22.8% | 0.93% | PASS |
| `merge_data_conflicts` | 128.96ms | 250.00ms | 51.6% | 0.15% | PASS |
| `merge_data_conflicts_with_resolve` | 128.62ms | 250.00ms | 51.4% | 0.23% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
