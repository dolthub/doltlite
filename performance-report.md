# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-08-11 01:18 UTC
>
> Commit: [`6570094c7aabde219a2b8a45c7a5420cd261479a`](https://github.com/dolthub/doltlite/commit/6570094c7aabde219a2b8a45c7a5420cd261479a)
>
> Runner: ubuntu24 20260720.247.2
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/31443479357)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians; MAD is the median absolute deviation and describes run-to-run noise.

## SQL workload summary

| Key shape | Workloads | Samples/workload | Wall time | SQLite median total | DoltLite median total | Ratio | Median paired-ratio MAD | Result |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| int | 69 | 55 | 1h 11m 36s | 8.90s | 10.97s | 1.233× | 1.46% | **PASS** |
| textpk | 69 | 55 | 1h 30m 51s | 10.10s | 11.72s | 1.161× | 1.87% | **PASS** |
| blobpk | 69 | 55 | 1h 29m 7s | 8.55s | 10.34s | 1.210× | 0.86% | **PASS** |
| compositepk | 69 | 55 | 1h 28m 7s | 10.41s | 11.99s | 1.152× | 1.52% | **PASS** |

The absolute ceiling is 2.4× per ordinary workload and 1.95× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.14ms | 28.39ms | 1.176× | 1.36% | PASS |
| mem_reads | `oltp_range_select` | 10.23ms | 10.97ms | 1.072× | 1.47% | PASS |
| mem_reads | `oltp_sum_range` | 9.58ms | 11.00ms | 1.149× | 1.83% | PASS |
| mem_reads | `oltp_order_range` | 2.54ms | 2.78ms | 1.092× | 1.31% | PASS |
| mem_reads | `oltp_distinct_range` | 3.59ms | 3.87ms | 1.077× | 0.99% | PASS |
| mem_reads | `oltp_index_scan` | 3.93ms | 5.02ms | 1.277× | 1.09% | PASS |
| mem_reads | `select_random_points` | 9.87ms | 10.59ms | 1.073× | 2.14% | PASS |
| mem_reads | `select_random_ranges` | 2.98ms | 3.89ms | 1.309× | 1.75% | PASS |
| mem_reads | `covering_index_scan` | 4.22ms | 4.07ms | 0.965× | 1.27% | PASS |
| mem_reads | `groupby_scan` | 29.59ms | 32.27ms | 1.091× | 0.58% | PASS |
| mem_reads | `index_join` | 6.01ms | 7.68ms | 1.278× | 0.93% | PASS |
| mem_reads | `index_join_scan` | 3.46ms | 4.32ms | 1.246× | 1.49% | PASS |
| mem_reads | `types_table_scan` | 1.04s | 1.26s | 1.202× | 0.51% | PASS |
| mem_reads | `table_scan` | 1.18s | 1.33s | 1.129× | 0.52% | PASS |
| mem_reads | `oltp_read_only` | 102.73ms | 115.75ms | 1.127× | 0.78% | PASS |
| mem_writes | `oltp_bulk_insert` | 180.61ms | 249.88ms | 1.383× | 0.74% | PASS |
| mem_writes | `oltp_insert` | 15.48ms | 28.02ms | 1.810× | 0.80% | PASS |
| mem_writes | `oltp_update_index` | 49.95ms | 101.13ms | 2.024× | 1.42% | PASS |
| mem_writes | `oltp_update_non_index` | 34.06ms | 58.60ms | 1.720× | 1.45% | PASS |
| mem_writes | `oltp_delete_insert` | 44.95ms | 77.16ms | 1.717× | 1.57% | PASS |
| mem_writes | `oltp_write_only` | 21.66ms | 43.96ms | 2.029× | 1.52% | PASS |
| mem_writes | `types_delete_insert` | 24.54ms | 40.00ms | 1.630× | 1.26% | PASS |
| mem_writes | `oltp_read_write` | 70.36ms | 114.00ms | 1.620× | 2.58% | PASS |
| file_reads | `oltp_point_select` | 93.12ms | 53.25ms | 0.572× | 0.91% | PASS |
| file_reads | `oltp_range_select` | 17.60ms | 13.60ms | 0.773× | 1.90% | PASS |
| file_reads | `oltp_sum_range` | 16.80ms | 13.74ms | 0.818× | 1.69% | PASS |
| file_reads | `oltp_order_range` | 3.32ms | 3.10ms | 0.935× | 1.18% | PASS |
| file_reads | `oltp_distinct_range` | 4.34ms | 4.19ms | 0.964× | 1.29% | PASS |
| file_reads | `oltp_index_scan` | 10.99ms | 7.90ms | 0.718× | 1.34% | PASS |
| file_reads | `select_random_points` | 17.64ms | 13.38ms | 0.759× | 2.26% | PASS |
| file_reads | `select_random_ranges` | 9.75ms | 6.38ms | 0.654× | 1.01% | PASS |
| file_reads | `covering_index_scan` | 11.21ms | 6.88ms | 0.614× | 1.13% | PASS |
| file_reads | `groupby_scan` | 30.53ms | 32.76ms | 1.073× | 0.80% | PASS |
| file_reads | `index_join` | 9.88ms | 9.66ms | 0.977× | 1.47% | PASS |
| file_reads | `index_join_scan` | 4.35ms | 4.69ms | 1.077× | 1.86% | PASS |
| file_reads | `types_table_scan` | 1.05s | 1.26s | 1.197× | 0.70% | PASS |
| file_reads | `table_scan` | 1.20s | 1.33s | 1.115× | 1.17% | PASS |
| file_reads | `oltp_read_only` | 206.15ms | 153.55ms | 0.745× | 1.04% | PASS |
| file_writes | `oltp_bulk_insert` | 194.23ms | 269.93ms | 1.390× | 1.30% | PASS |
| file_writes | `oltp_insert` | 22.02ms | 35.34ms | 1.605× | 1.46% | PASS |
| file_writes | `oltp_update_index` | 78.03ms | 125.90ms | 1.613× | 1.97% | PASS |
| file_writes | `oltp_update_non_index` | 59.19ms | 81.43ms | 1.376× | 1.66% | PASS |
| file_writes | `oltp_delete_insert` | 67.65ms | 96.97ms | 1.433× | 2.09% | PASS |
| file_writes | `oltp_write_only` | 44.47ms | 62.81ms | 1.412× | 1.62% | PASS |
| file_writes | `types_delete_insert` | 39.99ms | 52.71ms | 1.318× | 1.61% | PASS |
| file_writes | `oltp_read_write` | 92.57ms | 131.27ms | 1.418× | 2.05% | PASS |
| ac_reads | `oltp_point_select` | 47.59ms | 53.22ms | 1.119× | 1.49% | PASS |
| ac_reads | `oltp_range_select` | 13.09ms | 13.52ms | 1.033× | 1.77% | PASS |
| ac_reads | `oltp_sum_range` | 12.32ms | 13.67ms | 1.109× | 1.55% | PASS |
| ac_reads | `oltp_order_range` | 2.94ms | 3.10ms | 1.055× | 0.89% | PASS |
| ac_reads | `oltp_distinct_range` | 3.96ms | 4.20ms | 1.060× | 1.00% | PASS |
| ac_reads | `oltp_index_scan` | 6.56ms | 7.93ms | 1.208× | 1.08% | PASS |
| ac_reads | `select_random_points` | 12.93ms | 13.30ms | 1.028× | 1.84% | PASS |
| ac_reads | `select_random_ranges` | 5.43ms | 6.39ms | 1.176× | 1.28% | PASS |
| ac_reads | `covering_index_scan` | 7.00ms | 6.93ms | 0.990× | 0.97% | PASS |
| ac_reads | `groupby_scan` | 29.95ms | 32.73ms | 1.093× | 0.82% | PASS |
| ac_reads | `index_join` | 7.67ms | 9.69ms | 1.264× | 2.31% | PASS |
| ac_reads | `index_join_scan` | 3.89ms | 4.71ms | 1.211× | 1.91% | PASS |
| ac_reads | `types_table_scan` | 1.05s | 1.26s | 1.196× | 0.72% | PASS |
| ac_reads | `table_scan` | 1.20s | 1.34s | 1.118× | 1.08% | PASS |
| ac_reads | `oltp_read_only` | 138.65ms | 152.81ms | 1.102× | 1.60% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 23.39ms | 76.68ms | 3.278× | 4.66% | PASS |
| ac_writes | `oltp_insert_ac` | 25.47ms | 91.26ms | 3.583× | 6.41% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.32ms | 105.38ms | 3.857× | 7.47% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.91ms | 87.13ms | 3.802× | 6.67% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.15ms | 95.39ms | 3.950× | 3.38% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.02ms | 96.84ms | 3.870× | 5.17% | PASS |
| ac_writes | `types_delete_insert_ac` | 21.73ms | 87.01ms | 4.004× | 4.45% | PASS |
| ac_writes | `oltp_read_write_ac` | 29.78ms | 101.89ms | 3.421× | 5.07% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 29.95ms | 36.42ms | 1.216× | 1.72% | PASS |
| mem_reads | `oltp_range_select` | 13.97ms | 13.57ms | 0.971× | 2.04% | PASS |
| mem_reads | `oltp_sum_range` | 12.36ms | 13.78ms | 1.114× | 1.45% | PASS |
| mem_reads | `oltp_order_range` | 2.99ms | 3.12ms | 1.043× | 1.52% | PASS |
| mem_reads | `oltp_distinct_range` | 3.99ms | 4.22ms | 1.057× | 1.02% | PASS |
| mem_reads | `oltp_index_scan` | 4.46ms | 5.87ms | 1.317× | 2.03% | PASS |
| mem_reads | `select_random_points` | 18.18ms | 20.54ms | 1.130× | 1.60% | PASS |
| mem_reads | `select_random_ranges` | 4.01ms | 5.14ms | 1.283× | 1.32% | PASS |
| mem_reads | `covering_index_scan` | 4.51ms | 4.36ms | 0.967× | 1.95% | PASS |
| mem_reads | `groupby_scan` | 31.77ms | 34.04ms | 1.071× | 0.65% | PASS |
| mem_reads | `index_join` | 6.90ms | 8.99ms | 1.303× | 2.20% | PASS |
| mem_reads | `index_join_scan` | 4.83ms | 5.29ms | 1.095× | 3.27% | PASS |
| mem_reads | `types_table_scan` | 1.13s | 1.22s | 1.080× | 2.12% | PASS |
| mem_reads | `table_scan` | 1.47s | 1.31s | 0.893× | 3.02% | PASS |
| mem_reads | `oltp_read_only` | 121.19ms | 134.49ms | 1.110× | 2.20% | PASS |
| mem_writes | `oltp_bulk_insert` | 229.90ms | 350.84ms | 1.526× | 0.83% | PASS |
| mem_writes | `oltp_insert` | 21.51ms | 38.80ms | 1.804× | 0.96% | PASS |
| mem_writes | `oltp_update_index` | 71.08ms | 152.12ms | 2.140× | 1.77% | PASS |
| mem_writes | `oltp_update_non_index` | 48.88ms | 107.03ms | 2.189× | 0.88% | PASS |
| mem_writes | `oltp_delete_insert` | 51.02ms | 108.75ms | 2.132× | 1.19% | PASS |
| mem_writes | `oltp_write_only` | 28.93ms | 63.21ms | 2.185× | 1.08% | PASS |
| mem_writes | `types_delete_insert` | 33.72ms | 61.81ms | 1.833× | 1.17% | PASS |
| mem_writes | `oltp_read_write` | 90.86ms | 147.32ms | 1.621× | 2.49% | PASS |
| file_reads | `oltp_point_select` | 98.00ms | 60.69ms | 0.619× | 0.74% | PASS |
| file_reads | `oltp_range_select` | 22.05ms | 16.10ms | 0.730× | 2.68% | PASS |
| file_reads | `oltp_sum_range` | 20.47ms | 16.56ms | 0.809× | 1.35% | PASS |
| file_reads | `oltp_order_range` | 3.88ms | 3.46ms | 0.892× | 1.52% | PASS |
| file_reads | `oltp_distinct_range` | 4.88ms | 4.58ms | 0.938× | 1.41% | PASS |
| file_reads | `oltp_index_scan` | 11.61ms | 8.70ms | 0.749× | 1.42% | PASS |
| file_reads | `select_random_points` | 26.16ms | 23.74ms | 0.908× | 2.05% | PASS |
| file_reads | `select_random_ranges` | 10.93ms | 7.69ms | 0.703× | 1.26% | PASS |
| file_reads | `covering_index_scan` | 11.70ms | 7.09ms | 0.606× | 3.19% | PASS |
| file_reads | `groupby_scan` | 33.07ms | 34.59ms | 1.046× | 0.98% | PASS |
| file_reads | `index_join` | 10.98ms | 11.09ms | 1.010× | 2.15% | PASS |
| file_reads | `index_join_scan` | 5.99ms | 5.82ms | 0.971× | 2.15% | PASS |
| file_reads | `types_table_scan` | 1.08s | 1.21s | 1.114× | 0.69% | PASS |
| file_reads | `table_scan` | 1.27s | 1.29s | 1.017× | 1.63% | PASS |
| file_reads | `oltp_read_only` | 223.32ms | 171.54ms | 0.768× | 1.21% | PASS |
| file_writes | `oltp_bulk_insert` | 248.27ms | 377.30ms | 1.520× | 0.91% | PASS |
| file_writes | `oltp_insert` | 48.27ms | 50.90ms | 1.054× | 6.32% | PASS |
| file_writes | `oltp_update_index` | 116.05ms | 187.40ms | 1.615× | 1.69% | PASS |
| file_writes | `oltp_update_non_index` | 101.78ms | 132.14ms | 1.298× | 10.14% | PASS |
| file_writes | `oltp_delete_insert` | 94.49ms | 137.16ms | 1.452× | 1.93% | PASS |
| file_writes | `oltp_write_only` | 90.25ms | 85.49ms | 0.947× | 10.42% | PASS |
| file_writes | `types_delete_insert` | 58.21ms | 80.41ms | 1.381× | 1.73% | PASS |
| file_writes | `oltp_read_write` | 141.33ms | 167.51ms | 1.185× | 4.54% | PASS |
| ac_reads | `oltp_point_select` | 52.64ms | 60.74ms | 1.154× | 1.30% | PASS |
| ac_reads | `oltp_range_select` | 17.23ms | 16.11ms | 0.935× | 2.44% | PASS |
| ac_reads | `oltp_sum_range` | 15.50ms | 16.56ms | 1.068× | 2.06% | PASS |
| ac_reads | `oltp_order_range` | 3.38ms | 3.46ms | 1.023× | 2.38% | PASS |
| ac_reads | `oltp_distinct_range` | 4.41ms | 4.58ms | 1.038× | 1.12% | PASS |
| ac_reads | `oltp_index_scan` | 7.17ms | 8.77ms | 1.223× | 2.34% | PASS |
| ac_reads | `select_random_points` | 21.55ms | 23.97ms | 1.112× | 1.87% | PASS |
| ac_reads | `select_random_ranges` | 6.54ms | 7.68ms | 1.175× | 1.36% | PASS |
| ac_reads | `covering_index_scan` | 8.09ms | 7.11ms | 0.879× | 4.28% | PASS |
| ac_reads | `groupby_scan` | 32.51ms | 34.52ms | 1.062× | 0.84% | PASS |
| ac_reads | `index_join` | 8.75ms | 11.02ms | 1.259× | 2.86% | PASS |
| ac_reads | `index_join_scan` | 5.49ms | 5.78ms | 1.053× | 2.13% | PASS |
| ac_reads | `types_table_scan` | 1.09s | 1.21s | 1.116× | 1.10% | PASS |
| ac_reads | `table_scan` | 1.25s | 1.29s | 1.033× | 0.78% | PASS |
| ac_reads | `oltp_read_only` | 153.87ms | 170.07ms | 1.105× | 1.21% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 36.09ms | 114.58ms | 3.175× | 8.48% | PASS |
| ac_writes | `oltp_insert_ac` | 35.24ms | 116.68ms | 3.311× | 10.03% | PASS |
| ac_writes | `oltp_update_index_ac` | 33.33ms | 126.18ms | 3.786× | 10.97% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 27.18ms | 103.23ms | 3.798× | 6.97% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 29.41ms | 113.22ms | 3.849× | 11.10% | PASS |
| ac_writes | `oltp_write_only_ac` | 29.24ms | 112.72ms | 3.855× | 7.75% | PASS |
| ac_writes | `types_delete_insert_ac` | 26.41ms | 103.93ms | 3.935× | 9.03% | PASS |
| ac_writes | `oltp_read_write_ac` | 35.76ms | 118.26ms | 3.307× | 8.10% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 23.24ms | 26.45ms | 1.138× | 0.91% | PASS |
| mem_reads | `oltp_range_select` | 10.36ms | 10.39ms | 1.003× | 0.94% | PASS |
| mem_reads | `oltp_sum_range` | 9.26ms | 10.13ms | 1.094× | 0.93% | PASS |
| mem_reads | `oltp_order_range` | 2.40ms | 2.48ms | 1.035× | 0.84% | PASS |
| mem_reads | `oltp_distinct_range` | 3.24ms | 3.31ms | 1.023× | 0.69% | PASS |
| mem_reads | `oltp_index_scan` | 3.58ms | 4.38ms | 1.227× | 1.22% | PASS |
| mem_reads | `select_random_points` | 13.37ms | 15.58ms | 1.166× | 0.92% | PASS |
| mem_reads | `select_random_ranges` | 3.25ms | 4.10ms | 1.260× | 0.74% | PASS |
| mem_reads | `covering_index_scan` | 3.52ms | 3.33ms | 0.947× | 1.19% | PASS |
| mem_reads | `groupby_scan` | 26.21ms | 28.08ms | 1.072× | 0.42% | PASS |
| mem_reads | `index_join` | 5.47ms | 6.78ms | 1.240× | 0.88% | PASS |
| mem_reads | `index_join_scan` | 3.54ms | 4.50ms | 1.271× | 0.74% | PASS |
| mem_reads | `types_table_scan` | 863.49ms | 981.73ms | 1.137× | 0.76% | PASS |
| mem_reads | `table_scan` | 996.60ms | 1.06s | 1.059× | 0.49% | PASS |
| mem_reads | `oltp_read_only` | 91.11ms | 99.12ms | 1.088× | 0.63% | PASS |
| mem_writes | `oltp_bulk_insert` | 183.41ms | 259.22ms | 1.413× | 0.75% | PASS |
| mem_writes | `oltp_insert` | 15.81ms | 29.19ms | 1.846× | 0.67% | PASS |
| mem_writes | `oltp_update_index` | 54.05ms | 112.07ms | 2.073× | 0.58% | PASS |
| mem_writes | `oltp_update_non_index` | 38.43ms | 78.02ms | 2.030× | 0.91% | PASS |
| mem_writes | `oltp_delete_insert` | 38.58ms | 81.99ms | 2.125× | 0.66% | PASS |
| mem_writes | `oltp_write_only` | 22.23ms | 48.63ms | 2.187× | 0.61% | PASS |
| mem_writes | `types_delete_insert` | 24.87ms | 43.71ms | 1.757× | 0.61% | PASS |
| mem_writes | `oltp_read_write` | 62.50ms | 106.73ms | 1.708× | 0.62% | PASS |
| file_reads | `oltp_point_select` | 88.35ms | 48.73ms | 0.552× | 0.69% | PASS |
| file_reads | `oltp_range_select` | 17.98ms | 12.79ms | 0.711× | 0.86% | PASS |
| file_reads | `oltp_sum_range` | 16.80ms | 12.54ms | 0.746× | 0.94% | PASS |
| file_reads | `oltp_order_range` | 3.24ms | 2.82ms | 0.869× | 0.86% | PASS |
| file_reads | `oltp_distinct_range` | 4.07ms | 3.62ms | 0.888× | 0.90% | PASS |
| file_reads | `oltp_index_scan` | 10.60ms | 7.04ms | 0.664× | 0.75% | PASS |
| file_reads | `select_random_points` | 20.95ms | 18.10ms | 0.864× | 0.65% | PASS |
| file_reads | `select_random_ranges` | 10.08ms | 6.44ms | 0.639× | 0.81% | PASS |
| file_reads | `covering_index_scan` | 10.98ms | 6.00ms | 0.546× | 0.88% | PASS |
| file_reads | `groupby_scan` | 27.47ms | 28.55ms | 1.039× | 0.79% | PASS |
| file_reads | `index_join` | 9.71ms | 8.92ms | 0.918× | 0.65% | PASS |
| file_reads | `index_join_scan` | 4.47ms | 4.94ms | 1.105× | 1.21% | PASS |
| file_reads | `types_table_scan` | 885.55ms | 988.74ms | 1.117× | 1.57% | PASS |
| file_reads | `table_scan` | 1.00s | 1.06s | 1.054× | 0.81% | PASS |
| file_reads | `oltp_read_only` | 185.42ms | 130.79ms | 0.705× | 0.73% | PASS |
| file_writes | `oltp_bulk_insert` | 283.20ms | 355.23ms | 1.254× | 13.94% | PASS |
| file_writes | `oltp_insert` | 51.61ms | 60.85ms | 1.179× | 13.70% | PASS |
| file_writes | `oltp_update_index` | 187.32ms | 208.69ms | 1.114× | 12.45% | PASS |
| file_writes | `oltp_update_non_index` | 190.61ms | 149.29ms | 0.783× | 22.88% | PASS |
| file_writes | `oltp_delete_insert` | 162.87ms | 162.45ms | 0.997× | 11.61% | PASS |
| file_writes | `oltp_write_only` | 127.54ms | 113.10ms | 0.887× | 18.22% | PASS |
| file_writes | `types_delete_insert` | 113.98ms | 104.21ms | 0.914× | 31.89% | PASS |
| file_writes | `oltp_read_write` | 166.67ms | 171.23ms | 1.027× | 15.55% | PASS |
| ac_reads | `oltp_point_select` | 45.11ms | 48.86ms | 1.083× | 0.76% | PASS |
| ac_reads | `oltp_range_select` | 13.68ms | 12.78ms | 0.934× | 0.99% | PASS |
| ac_reads | `oltp_sum_range` | 12.80ms | 12.61ms | 0.985× | 1.18% | PASS |
| ac_reads | `oltp_order_range` | 2.87ms | 2.83ms | 0.984× | 0.81% | PASS |
| ac_reads | `oltp_distinct_range` | 3.68ms | 3.61ms | 0.980× | 0.82% | PASS |
| ac_reads | `oltp_index_scan` | 6.34ms | 7.02ms | 1.106× | 1.00% | PASS |
| ac_reads | `select_random_points` | 16.75ms | 18.12ms | 1.082× | 0.68% | PASS |
| ac_reads | `select_random_ranges` | 5.87ms | 6.43ms | 1.097× | 0.83% | PASS |
| ac_reads | `covering_index_scan` | 6.71ms | 6.01ms | 0.896× | 0.75% | PASS |
| ac_reads | `groupby_scan` | 26.99ms | 28.53ms | 1.057× | 0.51% | PASS |
| ac_reads | `index_join` | 7.46ms | 8.85ms | 1.186× | 0.88% | PASS |
| ac_reads | `index_join_scan` | 4.10ms | 4.90ms | 1.195× | 0.70% | PASS |
| ac_reads | `types_table_scan` | 868.26ms | 978.08ms | 1.126× | 0.52% | PASS |
| ac_reads | `table_scan` | 997.02ms | 1.06s | 1.060× | 0.60% | PASS |
| ac_reads | `oltp_read_only` | 123.46ms | 130.95ms | 1.061× | 0.69% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 52.37ms | 232.10ms | 4.432× | 59.00% | PASS |
| ac_writes | `oltp_insert_ac` | 35.46ms | 113.99ms | 3.215× | 27.85% | PASS |
| ac_writes | `oltp_update_index_ac` | 39.48ms | 151.44ms | 3.836× | 63.80% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 33.85ms | 113.44ms | 3.351× | 46.25% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 34.25ms | 178.23ms | 5.204× | 64.65% | PASS |
| ac_writes | `oltp_write_only_ac` | 37.01ms | 126.85ms | 3.428× | 54.40% | PASS |
| ac_writes | `types_delete_insert_ac` | 48.84ms | 263.59ms | 5.397× | 52.89% | PASS |
| ac_writes | `oltp_read_write_ac` | 43.66ms | 157.29ms | 3.602× | 73.98% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 35.59ms | 41.06ms | 1.154× | 3.69% | PASS |
| mem_reads | `oltp_range_select` | 21.00ms | 22.66ms | 1.079× | 3.06% | PASS |
| mem_reads | `oltp_sum_range` | 18.50ms | 21.48ms | 1.161× | 1.79% | PASS |
| mem_reads | `oltp_order_range` | 3.59ms | 4.04ms | 1.126× | 1.46% | PASS |
| mem_reads | `oltp_distinct_range` | 4.72ms | 5.18ms | 1.099× | 1.41% | PASS |
| mem_reads | `oltp_index_scan` | 4.76ms | 6.23ms | 1.308× | 2.31% | PASS |
| mem_reads | `select_random_points` | 29.04ms | 32.49ms | 1.119× | 2.30% | PASS |
| mem_reads | `select_random_ranges` | 7.94ms | 9.08ms | 1.143× | 1.12% | PASS |
| mem_reads | `covering_index_scan` | 4.29ms | 4.43ms | 1.033× | 2.27% | PASS |
| mem_reads | `groupby_scan` | 36.68ms | 40.78ms | 1.112× | 0.73% | PASS |
| mem_reads | `index_join` | 8.27ms | 10.46ms | 1.265× | 1.62% | PASS |
| mem_reads | `index_join_scan` | 4.23ms | 5.42ms | 1.281× | 1.84% | PASS |
| mem_reads | `types_table_scan` | 1.08s | 1.22s | 1.133× | 0.64% | PASS |
| mem_reads | `table_scan` | 1.24s | 1.31s | 1.052× | 1.48% | PASS |
| mem_reads | `oltp_read_only` | 154.01ms | 174.60ms | 1.134× | 1.28% | PASS |
| mem_writes | `oltp_bulk_insert` | 249.04ms | 350.69ms | 1.408× | 1.32% | PASS |
| mem_writes | `oltp_insert` | 19.30ms | 36.05ms | 1.867× | 0.75% | PASS |
| mem_writes | `oltp_update_index` | 68.87ms | 139.31ms | 2.023× | 1.16% | PASS |
| mem_writes | `oltp_update_non_index` | 52.05ms | 105.45ms | 2.026× | 1.64% | PASS |
| mem_writes | `oltp_delete_insert` | 50.21ms | 102.48ms | 2.041× | 0.92% | PASS |
| mem_writes | `oltp_write_only` | 27.56ms | 60.70ms | 2.203× | 1.19% | PASS |
| mem_writes | `types_delete_insert` | 33.56ms | 62.67ms | 1.867× | 1.40% | PASS |
| mem_writes | `oltp_read_write` | 111.46ms | 171.82ms | 1.542× | 1.19% | PASS |
| file_reads | `oltp_point_select` | 105.09ms | 66.01ms | 0.628× | 1.07% | PASS |
| file_reads | `oltp_range_select` | 28.27ms | 24.81ms | 0.878× | 1.70% | PASS |
| file_reads | `oltp_sum_range` | 26.65ms | 24.88ms | 0.933× | 1.24% | PASS |
| file_reads | `oltp_order_range` | 4.82ms | 4.61ms | 0.957× | 2.69% | PASS |
| file_reads | `oltp_distinct_range` | 5.91ms | 5.78ms | 0.978× | 1.77% | PASS |
| file_reads | `oltp_index_scan` | 12.28ms | 9.20ms | 0.749× | 1.59% | PASS |
| file_reads | `select_random_points` | 39.28ms | 37.38ms | 0.951× | 1.46% | PASS |
| file_reads | `select_random_ranges` | 15.62ms | 12.10ms | 0.775× | 1.67% | PASS |
| file_reads | `covering_index_scan` | 12.02ms | 7.22ms | 0.601× | 1.08% | PASS |
| file_reads | `groupby_scan` | 38.34ms | 42.30ms | 1.103× | 0.72% | PASS |
| file_reads | `index_join` | 12.88ms | 13.10ms | 1.017× | 1.75% | PASS |
| file_reads | `index_join_scan` | 5.36ms | 6.16ms | 1.149× | 1.67% | PASS |
| file_reads | `types_table_scan` | 1.16s | 1.24s | 1.071× | 0.67% | PASS |
| file_reads | `table_scan` | 1.41s | 1.33s | 0.948× | 0.87% | PASS |
| file_reads | `oltp_read_only` | 265.05ms | 218.00ms | 0.823× | 0.74% | PASS |
| file_writes | `oltp_bulk_insert` | 265.46ms | 376.79ms | 1.419× | 0.97% | PASS |
| file_writes | `oltp_insert` | 27.48ms | 46.74ms | 1.701× | 1.76% | PASS |
| file_writes | `oltp_update_index` | 102.86ms | 172.81ms | 1.680× | 1.73% | PASS |
| file_writes | `oltp_update_non_index` | 81.19ms | 130.89ms | 1.612× | 1.30% | PASS |
| file_writes | `oltp_delete_insert` | 81.57ms | 129.81ms | 1.592× | 1.51% | PASS |
| file_writes | `oltp_write_only` | 54.07ms | 82.84ms | 1.532× | 1.54% | PASS |
| file_writes | `types_delete_insert` | 52.78ms | 78.06ms | 1.479× | 1.52% | PASS |
| file_writes | `oltp_read_write` | 137.92ms | 191.37ms | 1.388× | 1.14% | PASS |
| ac_reads | `oltp_point_select` | 58.47ms | 66.42ms | 1.136× | 1.44% | PASS |
| ac_reads | `oltp_range_select` | 23.16ms | 24.82ms | 1.071× | 1.86% | PASS |
| ac_reads | `oltp_sum_range` | 21.46ms | 24.87ms | 1.159× | 1.02% | PASS |
| ac_reads | `oltp_order_range` | 4.14ms | 4.48ms | 1.081× | 1.77% | PASS |
| ac_reads | `oltp_distinct_range` | 5.20ms | 5.65ms | 1.085× | 1.57% | PASS |
| ac_reads | `oltp_index_scan` | 7.55ms | 9.14ms | 1.211× | 1.49% | PASS |
| ac_reads | `select_random_points` | 33.12ms | 37.40ms | 1.129× | 1.20% | PASS |
| ac_reads | `select_random_ranges` | 10.69ms | 12.15ms | 1.137× | 1.60% | PASS |
| ac_reads | `covering_index_scan` | 7.17ms | 7.22ms | 1.006× | 1.81% | PASS |
| ac_reads | `groupby_scan` | 37.36ms | 42.12ms | 1.127× | 0.75% | PASS |
| ac_reads | `index_join` | 10.26ms | 13.18ms | 1.285× | 2.02% | PASS |
| ac_reads | `index_join_scan` | 4.80ms | 6.17ms | 1.285× | 2.40% | PASS |
| ac_reads | `types_table_scan` | 1.16s | 1.24s | 1.066× | 0.73% | PASS |
| ac_reads | `table_scan` | 1.42s | 1.34s | 0.943× | 0.72% | PASS |
| ac_reads | `oltp_read_only` | 196.56ms | 218.42ms | 1.111× | 0.95% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 23.93ms | 78.36ms | 3.274× | 4.93% | PASS |
| ac_writes | `oltp_insert_ac` | 25.40ms | 94.99ms | 3.739× | 7.42% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.76ms | 106.34ms | 3.974× | 5.85% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.72ms | 89.64ms | 3.626× | 5.78% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.55ms | 96.75ms | 3.942× | 5.28% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.62ms | 97.45ms | 3.803× | 6.15% | PASS |
| ac_writes | `types_delete_insert_ac` | 23.82ms | 86.37ms | 3.626× | 9.58% | PASS |
| ac_writes | `oltp_read_write_ac` | 33.28ms | 104.46ms | 3.139× | 6.68% | PASS |

</details>

## Version-control latency

Wall time: 2m 23s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 87.76ms | 130.00ms | 67.5% | 0.21% | PASS |
| `status_dirty_many_tables` | 91.18ms | 130.00ms | 70.1% | 0.18% | PASS |
| `diff_regular_working_one_table` | 85.02ms | 120.00ms | 70.8% | 0.18% | PASS |
| `diff_regular_working_many_tables` | 96.46ms | 140.00ms | 68.9% | 0.13% | PASS |
| `diff_stat_working_many_tables` | 96.48ms | 140.00ms | 68.9% | 0.13% | PASS |
| `diff_schema_working_many_tables` | 97.05ms | 140.00ms | 69.3% | 0.15% | PASS |
| `branch_list_many_branches` | 23.26ms | 35.00ms | 66.5% | 0.38% | PASS |
| `branch_create_delete` | 25.15ms | 40.00ms | 62.9% | 0.62% | PASS |
| `checkout_branch_clean` | 56.42ms | 150.00ms | 37.6% | 0.22% | PASS |
| `merge_data_no_conflicts` | 29.51ms | 50.00ms | 59.0% | 0.38% | PASS |
| `merge_schema_no_conflicts` | 22.10ms | 35.00ms | 63.1% | 0.44% | PASS |
| `merge_data_conflicts` | 128.58ms | 180.00ms | 71.4% | 0.16% | PASS |
| `merge_data_conflicts_with_resolve` | 128.24ms | 180.00ms | 71.2% | 0.13% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
