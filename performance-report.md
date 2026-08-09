# DoltLite Performance Report

> Nightly result: **FAIL**
>
> Generated: 2026-08-09 11:19 UTC
>
> Commit: [`ca4d77c6e0a9974d8a25827dac0e3433cc72e90e`](https://github.com/dolthub/doltlite/commit/ca4d77c6e0a9974d8a25827dac0e3433cc72e90e)
>
> Runner: ubuntu24 20260720.247.2
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/31306619792)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians; MAD is the median absolute deviation and describes run-to-run noise.

## SQL workload summary

| Key shape | Workloads | Samples/workload | Wall time | SQLite median total | DoltLite median total | Ratio | Median paired-ratio MAD | Result |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| int | 69 | 55 | 1h 11m 12s | 8.80s | 11.07s | 1.258× | 1.50% | **PASS** |
| textpk | 69 | 55 | 1h 32m 6s | 9.78s | 11.69s | 1.195× | 1.21% | **PASS** |
| blobpk | 69 | 55 | 1h 32m 17s | 10.29s | 12.03s | 1.169× | 1.59% | **PASS** |
| compositepk | 69 | 55 | 1h 26m 16s | 9.62s | 11.86s | 1.233× | 1.13% | **PASS** |

The absolute ceiling is 2.4× per ordinary workload and 1.95× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 23.15ms | 28.79ms | 1.244× | 1.44% | PASS |
| mem_reads | `oltp_range_select` | 9.68ms | 12.48ms | 1.289× | 1.78% | PASS |
| mem_reads | `oltp_sum_range` | 9.22ms | 12.20ms | 1.324× | 1.70% | PASS |
| mem_reads | `oltp_order_range` | 2.48ms | 2.90ms | 1.172× | 1.41% | PASS |
| mem_reads | `oltp_distinct_range` | 3.49ms | 3.98ms | 1.140× | 1.06% | PASS |
| mem_reads | `oltp_index_scan` | 3.77ms | 5.14ms | 1.363× | 1.63% | PASS |
| mem_reads | `select_random_points` | 9.38ms | 10.71ms | 1.141× | 2.23% | PASS |
| mem_reads | `select_random_ranges` | 2.86ms | 3.92ms | 1.373× | 1.95% | PASS |
| mem_reads | `covering_index_scan` | 4.18ms | 4.12ms | 0.984× | 1.63% | PASS |
| mem_reads | `groupby_scan` | 29.48ms | 33.54ms | 1.138× | 0.71% | PASS |
| mem_reads | `index_join` | 5.97ms | 7.87ms | 1.318× | 1.41% | PASS |
| mem_reads | `index_join_scan` | 3.40ms | 4.57ms | 1.343× | 2.01% | PASS |
| mem_reads | `types_table_scan` | 1.04s | 1.25s | 1.197× | 0.45% | PASS |
| mem_reads | `table_scan` | 1.17s | 1.32s | 1.136× | 0.48% | PASS |
| mem_reads | `oltp_read_only` | 99.20ms | 120.67ms | 1.216× | 0.85% | PASS |
| mem_writes | `oltp_bulk_insert` | 176.08ms | 249.54ms | 1.417× | 0.84% | PASS |
| mem_writes | `oltp_insert` | 15.21ms | 27.98ms | 1.840× | 0.82% | PASS |
| mem_writes | `oltp_update_index` | 48.65ms | 101.85ms | 2.094× | 1.16% | PASS |
| mem_writes | `oltp_update_non_index` | 32.68ms | 57.56ms | 1.761× | 1.51% | PASS |
| mem_writes | `oltp_delete_insert` | 43.95ms | 76.69ms | 1.745× | 1.03% | PASS |
| mem_writes | `oltp_write_only` | 21.00ms | 43.74ms | 2.083× | 1.60% | PASS |
| mem_writes | `types_delete_insert` | 23.63ms | 39.59ms | 1.675× | 1.73% | PASS |
| mem_writes | `oltp_read_write` | 64.35ms | 107.46ms | 1.670× | 1.08% | PASS |
| file_reads | `oltp_point_select` | 89.98ms | 52.58ms | 0.584× | 0.81% | PASS |
| file_reads | `oltp_range_select` | 16.96ms | 14.94ms | 0.881× | 2.73% | PASS |
| file_reads | `oltp_sum_range` | 16.44ms | 14.81ms | 0.901× | 2.17% | PASS |
| file_reads | `oltp_order_range` | 3.28ms | 3.22ms | 0.981× | 2.40% | PASS |
| file_reads | `oltp_distinct_range` | 4.30ms | 4.31ms | 1.000× | 1.78% | PASS |
| file_reads | `oltp_index_scan` | 10.73ms | 7.85ms | 0.731× | 1.85% | PASS |
| file_reads | `select_random_points` | 16.45ms | 13.24ms | 0.805× | 2.44% | PASS |
| file_reads | `select_random_ranges` | 9.67ms | 6.36ms | 0.657× | 1.43% | PASS |
| file_reads | `covering_index_scan` | 11.02ms | 6.80ms | 0.617× | 2.07% | PASS |
| file_reads | `groupby_scan` | 30.33ms | 34.02ms | 1.121× | 0.81% | PASS |
| file_reads | `index_join` | 9.81ms | 9.74ms | 0.994× | 1.70% | PASS |
| file_reads | `index_join_scan` | 4.34ms | 4.92ms | 1.134× | 1.51% | PASS |
| file_reads | `types_table_scan` | 1.04s | 1.25s | 1.198× | 0.43% | PASS |
| file_reads | `table_scan` | 1.17s | 1.33s | 1.134× | 0.52% | PASS |
| file_reads | `oltp_read_only` | 197.23ms | 155.92ms | 0.791× | 0.58% | PASS |
| file_writes | `oltp_bulk_insert` | 189.72ms | 268.90ms | 1.417× | 1.00% | PASS |
| file_writes | `oltp_insert` | 24.21ms | 35.01ms | 1.446× | 1.44% | PASS |
| file_writes | `oltp_update_index` | 75.46ms | 124.01ms | 1.643× | 1.50% | PASS |
| file_writes | `oltp_update_non_index` | 58.58ms | 79.05ms | 1.350× | 1.88% | PASS |
| file_writes | `oltp_delete_insert` | 69.37ms | 96.84ms | 1.396× | 1.77% | PASS |
| file_writes | `oltp_write_only` | 48.10ms | 62.62ms | 1.302× | 2.18% | PASS |
| file_writes | `types_delete_insert` | 39.97ms | 52.13ms | 1.304× | 1.47% | PASS |
| file_writes | `oltp_read_write` | 93.62ms | 126.19ms | 1.348× | 1.47% | PASS |
| ac_reads | `oltp_point_select` | 45.25ms | 52.47ms | 1.160× | 0.97% | PASS |
| ac_reads | `oltp_range_select` | 12.38ms | 14.98ms | 1.210× | 1.50% | PASS |
| ac_reads | `oltp_sum_range` | 11.85ms | 14.87ms | 1.254× | 1.08% | PASS |
| ac_reads | `oltp_order_range` | 2.83ms | 3.21ms | 1.136× | 1.23% | PASS |
| ac_reads | `oltp_distinct_range` | 3.82ms | 4.30ms | 1.126× | 1.11% | PASS |
| ac_reads | `oltp_index_scan` | 6.20ms | 7.86ms | 1.269× | 2.02% | PASS |
| ac_reads | `select_random_points` | 12.25ms | 13.35ms | 1.089× | 1.75% | PASS |
| ac_reads | `select_random_ranges` | 5.20ms | 6.38ms | 1.226× | 1.42% | PASS |
| ac_reads | `covering_index_scan` | 6.52ms | 6.76ms | 1.037× | 1.52% | PASS |
| ac_reads | `groupby_scan` | 29.71ms | 34.03ms | 1.145× | 0.85% | PASS |
| ac_reads | `index_join` | 7.38ms | 9.75ms | 1.321× | 1.47% | PASS |
| ac_reads | `index_join_scan` | 3.85ms | 4.95ms | 1.286× | 1.55% | PASS |
| ac_reads | `types_table_scan` | 1.04s | 1.24s | 1.196× | 0.36% | PASS |
| ac_reads | `table_scan` | 1.17s | 1.32s | 1.134× | 0.47% | PASS |
| ac_reads | `oltp_read_only` | 131.97ms | 155.70ms | 1.180× | 0.76% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 28.42ms | 96.72ms | 3.404× | 7.47% | PASS |
| ac_writes | `oltp_insert_ac` | 31.61ms | 112.58ms | 3.561× | 9.70% | PASS |
| ac_writes | `oltp_update_index_ac` | 33.00ms | 124.27ms | 3.766× | 5.98% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 28.71ms | 106.60ms | 3.713× | 9.29% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 30.17ms | 115.44ms | 3.827× | 6.09% | PASS |
| ac_writes | `oltp_write_only_ac` | 31.43ms | 115.05ms | 3.660× | 7.84% | PASS |
| ac_writes | `types_delete_insert_ac` | 27.81ms | 106.33ms | 3.823× | 5.87% | PASS |
| ac_writes | `oltp_read_write_ac` | 35.75ms | 122.94ms | 3.439× | 7.47% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 29.71ms | 37.87ms | 1.275× | 0.95% | PASS |
| mem_reads | `oltp_range_select` | 13.03ms | 14.40ms | 1.105× | 1.64% | PASS |
| mem_reads | `oltp_sum_range` | 11.89ms | 14.22ms | 1.196× | 1.06% | PASS |
| mem_reads | `oltp_order_range` | 2.96ms | 3.15ms | 1.065× | 1.30% | PASS |
| mem_reads | `oltp_distinct_range` | 3.94ms | 4.24ms | 1.074× | 0.98% | PASS |
| mem_reads | `oltp_index_scan` | 4.45ms | 6.21ms | 1.395× | 1.50% | PASS |
| mem_reads | `select_random_points` | 17.71ms | 20.99ms | 1.185× | 1.77% | PASS |
| mem_reads | `select_random_ranges` | 3.96ms | 5.33ms | 1.348× | 1.48% | PASS |
| mem_reads | `covering_index_scan` | 4.51ms | 4.63ms | 1.026× | 2.08% | PASS |
| mem_reads | `groupby_scan` | 31.62ms | 34.46ms | 1.090× | 0.93% | PASS |
| mem_reads | `index_join` | 6.82ms | 9.23ms | 1.355× | 1.57% | PASS |
| mem_reads | `index_join_scan` | 4.54ms | 5.53ms | 1.220× | 1.68% | PASS |
| mem_reads | `types_table_scan` | 1.08s | 1.24s | 1.149× | 0.49% | PASS |
| mem_reads | `table_scan` | 1.27s | 1.32s | 1.046× | 0.81% | PASS |
| mem_reads | `oltp_read_only` | 118.08ms | 138.12ms | 1.170× | 1.19% | PASS |
| mem_writes | `oltp_bulk_insert` | 234.76ms | 365.15ms | 1.555× | 0.69% | PASS |
| mem_writes | `oltp_insert` | 21.45ms | 39.61ms | 1.847× | 0.82% | PASS |
| mem_writes | `oltp_update_index` | 70.30ms | 132.02ms | 1.878× | 0.85% | PASS |
| mem_writes | `oltp_update_non_index` | 48.32ms | 86.31ms | 1.786× | 0.78% | PASS |
| mem_writes | `oltp_delete_insert` | 50.48ms | 103.33ms | 2.047× | 1.10% | PASS |
| mem_writes | `oltp_write_only` | 28.60ms | 61.28ms | 2.143× | 0.67% | PASS |
| mem_writes | `types_delete_insert` | 32.59ms | 55.21ms | 1.694× | 0.97% | PASS |
| mem_writes | `oltp_read_write` | 85.13ms | 141.46ms | 1.662× | 1.12% | PASS |
| file_reads | `oltp_point_select` | 99.15ms | 63.08ms | 0.636× | 0.99% | PASS |
| file_reads | `oltp_range_select` | 20.66ms | 17.13ms | 0.829× | 0.93% | PASS |
| file_reads | `oltp_sum_range` | 19.62ms | 16.98ms | 0.865× | 1.42% | PASS |
| file_reads | `oltp_order_range` | 3.83ms | 3.49ms | 0.911× | 1.66% | PASS |
| file_reads | `oltp_distinct_range` | 4.86ms | 4.62ms | 0.949× | 1.42% | PASS |
| file_reads | `oltp_index_scan` | 11.93ms | 9.19ms | 0.770× | 1.48% | PASS |
| file_reads | `select_random_points` | 26.14ms | 24.51ms | 0.938× | 1.81% | PASS |
| file_reads | `select_random_ranges` | 11.11ms | 7.89ms | 0.710× | 1.34% | PASS |
| file_reads | `covering_index_scan` | 12.61ms | 7.37ms | 0.584× | 1.35% | PASS |
| file_reads | `groupby_scan` | 32.76ms | 35.06ms | 1.070× | 0.73% | PASS |
| file_reads | `index_join` | 11.52ms | 11.43ms | 0.992× | 2.28% | PASS |
| file_reads | `index_join_scan` | 5.55ms | 6.10ms | 1.099× | 1.18% | PASS |
| file_reads | `types_table_scan` | 1.08s | 1.24s | 1.146× | 0.66% | PASS |
| file_reads | `table_scan` | 1.27s | 1.33s | 1.046× | 1.01% | PASS |
| file_reads | `oltp_read_only` | 221.89ms | 176.14ms | 0.794× | 0.82% | PASS |
| file_writes | `oltp_bulk_insert` | 253.74ms | 393.63ms | 1.551× | 0.89% | PASS |
| file_writes | `oltp_insert` | 49.04ms | 52.12ms | 1.063× | 22.65% | PASS |
| file_writes | `oltp_update_index` | 112.75ms | 166.74ms | 1.479× | 1.50% | PASS |
| file_writes | `oltp_update_non_index` | 99.38ms | 112.24ms | 1.129× | 7.21% | PASS |
| file_writes | `oltp_delete_insert` | 88.89ms | 132.96ms | 1.496× | 1.19% | PASS |
| file_writes | `oltp_write_only` | 86.52ms | 84.56ms | 0.977× | 13.74% | PASS |
| file_writes | `types_delete_insert` | 55.17ms | 74.48ms | 1.350× | 1.74% | PASS |
| file_writes | `oltp_read_write` | 143.40ms | 164.53ms | 1.147× | 5.32% | PASS |
| ac_reads | `oltp_point_select` | 52.79ms | 62.99ms | 1.193× | 0.88% | PASS |
| ac_reads | `oltp_range_select` | 16.06ms | 17.14ms | 1.067× | 1.48% | PASS |
| ac_reads | `oltp_sum_range` | 15.00ms | 17.04ms | 1.136× | 1.09% | PASS |
| ac_reads | `oltp_order_range` | 3.35ms | 3.48ms | 1.041× | 0.97% | PASS |
| ac_reads | `oltp_distinct_range` | 4.37ms | 4.62ms | 1.058× | 0.95% | PASS |
| ac_reads | `oltp_index_scan` | 7.31ms | 9.19ms | 1.258× | 1.21% | PASS |
| ac_reads | `select_random_points` | 21.01ms | 24.51ms | 1.166× | 0.91% | PASS |
| ac_reads | `select_random_ranges` | 6.58ms | 7.88ms | 1.198× | 1.04% | PASS |
| ac_reads | `covering_index_scan` | 7.91ms | 7.38ms | 0.933× | 1.65% | PASS |
| ac_reads | `groupby_scan` | 32.25ms | 34.90ms | 1.082× | 0.99% | PASS |
| ac_reads | `index_join` | 9.13ms | 11.51ms | 1.260× | 1.67% | PASS |
| ac_reads | `index_join_scan` | 5.09ms | 6.10ms | 1.198× | 1.52% | PASS |
| ac_reads | `types_table_scan` | 1.08s | 1.24s | 1.149× | 0.61% | PASS |
| ac_reads | `table_scan` | 1.27s | 1.33s | 1.045× | 0.90% | PASS |
| ac_reads | `oltp_read_only` | 153.24ms | 175.84ms | 1.148× | 1.16% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.87ms | 74.32ms | 3.249× | 6.10% | PASS |
| ac_writes | `oltp_insert_ac` | 24.98ms | 91.28ms | 3.655× | 4.39% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.32ms | 109.66ms | 4.014× | 4.80% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.28ms | 88.25ms | 3.962× | 5.80% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.41ms | 99.64ms | 4.082× | 6.33% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.08ms | 98.35ms | 3.771× | 5.68% | PASS |
| ac_writes | `types_delete_insert_ac` | 21.87ms | 89.68ms | 4.101× | 5.92% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.75ms | 106.38ms | 3.350× | 6.02% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 31.36ms | 38.10ms | 1.215× | 0.98% | PASS |
| mem_reads | `oltp_range_select` | 13.49ms | 14.58ms | 1.081× | 1.98% | PASS |
| mem_reads | `oltp_sum_range` | 12.44ms | 14.34ms | 1.153× | 1.95% | PASS |
| mem_reads | `oltp_order_range` | 2.98ms | 3.18ms | 1.067× | 1.02% | PASS |
| mem_reads | `oltp_distinct_range` | 4.02ms | 4.28ms | 1.065× | 0.97% | PASS |
| mem_reads | `oltp_index_scan` | 4.63ms | 6.40ms | 1.380× | 1.63% | PASS |
| mem_reads | `select_random_points` | 18.38ms | 21.30ms | 1.158× | 1.91% | PASS |
| mem_reads | `select_random_ranges` | 4.10ms | 5.28ms | 1.288× | 1.59% | PASS |
| mem_reads | `covering_index_scan` | 4.67ms | 4.78ms | 1.025× | 2.59% | PASS |
| mem_reads | `groupby_scan` | 32.01ms | 34.65ms | 1.083× | 0.76% | PASS |
| mem_reads | `index_join` | 7.08ms | 9.92ms | 1.401× | 1.85% | PASS |
| mem_reads | `index_join_scan` | 4.26ms | 5.57ms | 1.308× | 1.73% | PASS |
| mem_reads | `types_table_scan` | 1.17s | 1.27s | 1.087× | 1.97% | PASS |
| mem_reads | `table_scan` | 1.45s | 1.37s | 0.944× | 1.40% | PASS |
| mem_reads | `oltp_read_only` | 123.50ms | 140.01ms | 1.134× | 1.76% | PASS |
| mem_writes | `oltp_bulk_insert` | 236.26ms | 359.88ms | 1.523× | 0.92% | PASS |
| mem_writes | `oltp_insert` | 20.16ms | 39.96ms | 1.982× | 1.63% | PASS |
| mem_writes | `oltp_update_index` | 72.46ms | 136.39ms | 1.882× | 1.98% | PASS |
| mem_writes | `oltp_update_non_index` | 49.91ms | 86.60ms | 1.735× | 0.89% | PASS |
| mem_writes | `oltp_delete_insert` | 50.82ms | 105.48ms | 2.076× | 1.85% | PASS |
| mem_writes | `oltp_write_only` | 28.46ms | 62.21ms | 2.186× | 1.71% | PASS |
| mem_writes | `types_delete_insert` | 32.97ms | 55.27ms | 1.676× | 1.32% | PASS |
| mem_writes | `oltp_read_write` | 91.55ms | 146.95ms | 1.605× | 1.43% | PASS |
| file_reads | `oltp_point_select` | 100.89ms | 62.17ms | 0.616× | 0.81% | PASS |
| file_reads | `oltp_range_select` | 20.63ms | 17.00ms | 0.824× | 1.32% | PASS |
| file_reads | `oltp_sum_range` | 19.97ms | 16.75ms | 0.839× | 1.20% | PASS |
| file_reads | `oltp_order_range` | 3.82ms | 3.56ms | 0.933× | 1.75% | PASS |
| file_reads | `oltp_distinct_range` | 4.93ms | 4.66ms | 0.945× | 1.40% | PASS |
| file_reads | `oltp_index_scan` | 12.04ms | 8.98ms | 0.746× | 0.79% | PASS |
| file_reads | `select_random_points` | 26.53ms | 24.87ms | 0.938× | 1.80% | PASS |
| file_reads | `select_random_ranges` | 11.18ms | 7.76ms | 0.694× | 0.92% | PASS |
| file_reads | `covering_index_scan` | 12.18ms | 7.21ms | 0.592× | 1.04% | PASS |
| file_reads | `groupby_scan` | 33.04ms | 35.05ms | 1.061× | 0.69% | PASS |
| file_reads | `index_join` | 11.65ms | 11.46ms | 0.984× | 1.42% | PASS |
| file_reads | `index_join_scan` | 5.32ms | 6.03ms | 1.134× | 2.46% | PASS |
| file_reads | `types_table_scan` | 1.21s | 1.28s | 1.056× | 1.36% | PASS |
| file_reads | `table_scan` | 1.44s | 1.36s | 0.947× | 1.76% | PASS |
| file_reads | `oltp_read_only` | 228.66ms | 178.73ms | 0.782× | 1.01% | PASS |
| file_writes | `oltp_bulk_insert` | 255.21ms | 385.33ms | 1.510× | 1.07% | PASS |
| file_writes | `oltp_insert` | 36.98ms | 52.04ms | 1.407× | 2.00% | PASS |
| file_writes | `oltp_update_index` | 106.92ms | 163.92ms | 1.533× | 2.06% | PASS |
| file_writes | `oltp_update_non_index` | 84.26ms | 107.99ms | 1.282× | 1.31% | PASS |
| file_writes | `oltp_delete_insert` | 87.90ms | 131.89ms | 1.500× | 1.59% | PASS |
| file_writes | `oltp_write_only` | 64.89ms | 87.03ms | 1.341× | 1.35% | PASS |
| file_writes | `types_delete_insert` | 54.30ms | 72.39ms | 1.333× | 1.93% | PASS |
| file_writes | `oltp_read_write` | 122.39ms | 162.94ms | 1.331× | 1.29% | PASS |
| ac_reads | `oltp_point_select` | 53.67ms | 61.59ms | 1.148× | 0.92% | PASS |
| ac_reads | `oltp_range_select` | 16.63ms | 17.29ms | 1.040× | 1.85% | PASS |
| ac_reads | `oltp_sum_range` | 15.36ms | 16.98ms | 1.105× | 1.40% | PASS |
| ac_reads | `oltp_order_range` | 3.43ms | 3.60ms | 1.049× | 1.50% | PASS |
| ac_reads | `oltp_distinct_range` | 4.45ms | 4.69ms | 1.053× | 1.65% | PASS |
| ac_reads | `oltp_index_scan` | 7.58ms | 9.21ms | 1.216× | 1.69% | PASS |
| ac_reads | `select_random_points` | 21.73ms | 24.84ms | 1.143× | 1.61% | PASS |
| ac_reads | `select_random_ranges` | 6.68ms | 7.75ms | 1.160× | 1.10% | PASS |
| ac_reads | `covering_index_scan` | 7.54ms | 7.17ms | 0.951× | 1.46% | PASS |
| ac_reads | `groupby_scan` | 32.19ms | 35.01ms | 1.088× | 0.81% | PASS |
| ac_reads | `index_join` | 9.04ms | 11.34ms | 1.254× | 2.06% | PASS |
| ac_reads | `index_join_scan` | 4.68ms | 5.87ms | 1.255× | 2.14% | PASS |
| ac_reads | `types_table_scan` | 1.07s | 1.24s | 1.157× | 0.94% | PASS |
| ac_reads | `table_scan` | 1.21s | 1.32s | 1.093× | 1.10% | PASS |
| ac_reads | `oltp_read_only` | 149.87ms | 172.87ms | 1.153× | 0.95% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 28.67ms | 97.22ms | 3.391× | 8.51% | PASS |
| ac_writes | `oltp_insert_ac` | 33.30ms | 123.62ms | 3.712× | 6.22% | PASS |
| ac_writes | `oltp_update_index_ac` | 36.38ms | 137.00ms | 3.766× | 7.98% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 30.52ms | 114.62ms | 3.756× | 7.40% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 33.00ms | 126.90ms | 3.845× | 6.43% | PASS |
| ac_writes | `oltp_write_only_ac` | 33.27ms | 127.22ms | 3.824× | 6.44% | PASS |
| ac_writes | `types_delete_insert_ac` | 29.89ms | 115.08ms | 3.850× | 6.74% | PASS |
| ac_writes | `oltp_read_write_ac` | 37.61ms | 132.36ms | 3.519× | 5.09% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 31.32ms | 39.43ms | 1.259× | 0.99% | PASS |
| mem_reads | `oltp_range_select` | 17.85ms | 21.81ms | 1.222× | 0.98% | PASS |
| mem_reads | `oltp_sum_range` | 17.14ms | 21.16ms | 1.235× | 0.88% | PASS |
| mem_reads | `oltp_order_range` | 3.33ms | 3.86ms | 1.157× | 0.74% | PASS |
| mem_reads | `oltp_distinct_range` | 4.41ms | 4.99ms | 1.131× | 0.82% | PASS |
| mem_reads | `oltp_index_scan` | 4.25ms | 5.91ms | 1.391× | 0.90% | PASS |
| mem_reads | `select_random_points` | 26.17ms | 31.53ms | 1.205× | 1.00% | PASS |
| mem_reads | `select_random_ranges` | 7.20ms | 8.84ms | 1.227× | 1.11% | PASS |
| mem_reads | `covering_index_scan` | 4.18ms | 4.14ms | 0.990× | 1.19% | PASS |
| mem_reads | `groupby_scan` | 35.57ms | 40.54ms | 1.140× | 0.63% | PASS |
| mem_reads | `index_join` | 8.05ms | 10.15ms | 1.262× | 0.87% | PASS |
| mem_reads | `index_join_scan` | 3.91ms | 5.39ms | 1.381× | 3.44% | PASS |
| mem_reads | `types_table_scan` | 1.06s | 1.25s | 1.177× | 0.51% | PASS |
| mem_reads | `table_scan` | 1.21s | 1.34s | 1.107× | 1.44% | PASS |
| mem_reads | `oltp_read_only` | 152.93ms | 174.24ms | 1.139× | 1.42% | PASS |
| mem_writes | `oltp_bulk_insert` | 243.90ms | 359.44ms | 1.474× | 0.79% | PASS |
| mem_writes | `oltp_insert` | 19.12ms | 36.84ms | 1.927× | 0.59% | PASS |
| mem_writes | `oltp_update_index` | 68.25ms | 116.99ms | 1.714× | 1.03% | PASS |
| mem_writes | `oltp_update_non_index` | 50.89ms | 83.65ms | 1.644× | 0.75% | PASS |
| mem_writes | `oltp_delete_insert` | 49.99ms | 96.70ms | 1.934× | 0.99% | PASS |
| mem_writes | `oltp_write_only` | 26.72ms | 57.25ms | 2.143× | 1.24% | PASS |
| mem_writes | `types_delete_insert` | 32.16ms | 54.28ms | 1.688× | 1.31% | PASS |
| mem_writes | `oltp_read_write` | 103.74ms | 157.15ms | 1.515× | 0.91% | PASS |
| file_reads | `oltp_point_select` | 101.58ms | 64.11ms | 0.631× | 0.79% | PASS |
| file_reads | `oltp_range_select` | 26.56ms | 24.71ms | 0.930× | 1.13% | PASS |
| file_reads | `oltp_sum_range` | 25.43ms | 24.23ms | 0.952× | 0.86% | PASS |
| file_reads | `oltp_order_range` | 4.41ms | 4.29ms | 0.975× | 1.33% | PASS |
| file_reads | `oltp_distinct_range` | 5.51ms | 5.40ms | 0.981× | 1.18% | PASS |
| file_reads | `oltp_index_scan` | 11.84ms | 8.82ms | 0.745× | 1.22% | PASS |
| file_reads | `select_random_points` | 36.47ms | 35.97ms | 0.986× | 1.48% | PASS |
| file_reads | `select_random_ranges` | 15.15ms | 11.64ms | 0.768× | 1.04% | PASS |
| file_reads | `covering_index_scan` | 11.45ms | 7.04ms | 0.615× | 1.09% | PASS |
| file_reads | `groupby_scan` | 37.20ms | 41.34ms | 1.111× | 0.90% | PASS |
| file_reads | `index_join` | 12.41ms | 12.58ms | 1.014× | 1.53% | PASS |
| file_reads | `index_join_scan` | 5.11ms | 5.90ms | 1.155× | 1.90% | PASS |
| file_reads | `types_table_scan` | 1.06s | 1.25s | 1.177× | 0.71% | PASS |
| file_reads | `table_scan` | 1.22s | 1.33s | 1.090× | 0.98% | PASS |
| file_reads | `oltp_read_only` | 251.02ms | 209.51ms | 0.835× | 1.07% | PASS |
| file_writes | `oltp_bulk_insert` | 258.42ms | 379.73ms | 1.469× | 0.95% | PASS |
| file_writes | `oltp_insert` | 25.97ms | 46.08ms | 1.775× | 1.42% | PASS |
| file_writes | `oltp_update_index` | 95.63ms | 140.55ms | 1.470× | 1.49% | PASS |
| file_writes | `oltp_update_non_index` | 75.77ms | 103.28ms | 1.363× | 1.65% | PASS |
| file_writes | `oltp_delete_insert` | 75.60ms | 118.20ms | 1.563× | 1.66% | PASS |
| file_writes | `oltp_write_only` | 50.58ms | 76.42ms | 1.511× | 1.78% | PASS |
| file_writes | `types_delete_insert` | 48.33ms | 67.58ms | 1.398× | 1.54% | PASS |
| file_writes | `oltp_read_write` | 126.36ms | 175.81ms | 1.391× | 1.23% | PASS |
| ac_reads | `oltp_point_select` | 55.52ms | 64.46ms | 1.161× | 1.16% | PASS |
| ac_reads | `oltp_range_select` | 21.71ms | 24.80ms | 1.143× | 1.09% | PASS |
| ac_reads | `oltp_sum_range` | 20.56ms | 24.22ms | 1.178× | 1.25% | PASS |
| ac_reads | `oltp_order_range` | 3.89ms | 4.32ms | 1.110× | 1.57% | PASS |
| ac_reads | `oltp_distinct_range` | 4.99ms | 5.44ms | 1.090× | 1.09% | PASS |
| ac_reads | `oltp_index_scan` | 7.07ms | 8.85ms | 1.251× | 1.42% | PASS |
| ac_reads | `select_random_points` | 30.93ms | 35.98ms | 1.163× | 1.01% | PASS |
| ac_reads | `select_random_ranges` | 10.23ms | 11.70ms | 1.143× | 1.05% | PASS |
| ac_reads | `covering_index_scan` | 6.73ms | 7.07ms | 1.050× | 1.63% | PASS |
| ac_reads | `groupby_scan` | 36.60ms | 41.39ms | 1.131× | 1.01% | PASS |
| ac_reads | `index_join` | 9.78ms | 12.60ms | 1.288× | 1.27% | PASS |
| ac_reads | `index_join_scan` | 4.61ms | 5.96ms | 1.291× | 1.82% | PASS |
| ac_reads | `types_table_scan` | 1.06s | 1.25s | 1.176× | 0.47% | PASS |
| ac_reads | `table_scan` | 1.19s | 1.33s | 1.116× | 0.53% | PASS |
| ac_reads | `oltp_read_only` | 181.96ms | 209.52ms | 1.151× | 1.03% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.60ms | 74.71ms | 3.306× | 6.22% | PASS |
| ac_writes | `oltp_insert_ac` | 24.87ms | 96.00ms | 3.860× | 4.67% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.50ms | 107.27ms | 4.048× | 5.37% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.29ms | 88.05ms | 3.781× | 5.98% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 23.60ms | 98.03ms | 4.153× | 4.84% | PASS |
| ac_writes | `oltp_write_only_ac` | 24.63ms | 97.71ms | 3.967× | 4.56% | PASS |
| ac_writes | `types_delete_insert_ac` | 21.59ms | 87.07ms | 4.034× | 6.17% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.52ms | 106.97ms | 3.393× | 4.59% | PASS |

</details>

## Version-control latency

Wall time: 2m 0s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 48.39ms | 130.00ms | 37.2% | 1.06% | PASS |
| `status_dirty_many_tables` | 50.34ms | 130.00ms | 38.7% | 1.35% | PASS |
| `diff_regular_working_one_table` | 46.63ms | 120.00ms | 38.9% | 0.66% | PASS |
| `diff_regular_working_many_tables` | 53.20ms | 140.00ms | 38.0% | 1.05% | PASS |
| `diff_stat_working_many_tables` | 53.29ms | 140.00ms | 38.1% | 1.16% | PASS |
| `diff_schema_working_many_tables` | 54.63ms | 140.00ms | 39.0% | 1.32% | PASS |
| `branch_list_many_branches` | 15.53ms | 35.00ms | 44.4% | 1.24% | PASS |
| `branch_create_delete` | 37.31ms | 40.00ms | 93.3% | 52.99% | PASS |
| `checkout_branch_clean` | 156.68ms | 150.00ms | 104.5% | 43.77% | FAIL |
| `merge_data_no_conflicts` | 35.66ms | 50.00ms | 71.3% | 30.17% | PASS |
| `merge_schema_no_conflicts` | 41.78ms | 35.00ms | 119.4% | 61.13% | FAIL |
| `merge_data_conflicts` | 59.17ms | 180.00ms | 32.9% | 0.33% | PASS |
| `merge_data_conflicts_with_resolve` | 57.69ms | 180.00ms | 32.0% | 1.58% | PASS |

Version-control ceiling result: **FAIL**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
