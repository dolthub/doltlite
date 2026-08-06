# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-08-06 11:56 UTC
>
> Commit: [`ffebc9b92070b27c45e907064c528d6f8f2ff5c3`](https://github.com/dolthub/doltlite/commit/ffebc9b92070b27c45e907064c528d6f8f2ff5c3)
>
> Runner: ubuntu24 20260720.247.2
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/31092764431)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians; MAD is the median absolute deviation and describes run-to-run noise.

## SQL workload summary

| Key shape | Workloads | Samples/workload | Wall time | SQLite median total | DoltLite median total | Ratio | Median paired-ratio MAD | Result |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| int | 69 | 55 | 1h 11m 54s | 9.48s | 11.05s | 1.165× | 1.12% | **PASS** |
| textpk | 69 | 55 | 1h 34m 6s | 11.38s | 11.95s | 1.050× | 1.51% | **PASS** |
| blobpk | 69 | 55 | 1h 31m 9s | 10.19s | 11.64s | 1.143× | 1.31% | **PASS** |
| compositepk | 69 | 55 | 1h 26m 17s | 9.74s | 11.99s | 1.231× | 1.49% | **PASS** |

The absolute ceiling is 2.5× per ordinary workload and 2.0× for a section average. Durable autocommit writes use 10.0× and 5.0× ceilings respectively.

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 25.46ms | 28.34ms | 1.113× | 2.16% | PASS |
| mem_reads | `oltp_range_select` | 10.98ms | 12.11ms | 1.104× | 2.12% | PASS |
| mem_reads | `oltp_sum_range` | 10.19ms | 11.79ms | 1.157× | 2.66% | PASS |
| mem_reads | `oltp_order_range` | 2.76ms | 2.95ms | 1.068× | 1.37% | PASS |
| mem_reads | `oltp_distinct_range` | 3.83ms | 3.93ms | 1.027× | 0.89% | PASS |
| mem_reads | `oltp_index_scan` | 4.09ms | 5.35ms | 1.307× | 1.94% | PASS |
| mem_reads | `select_random_points` | 10.37ms | 11.07ms | 1.068× | 1.67% | PASS |
| mem_reads | `select_random_ranges` | 3.04ms | 4.00ms | 1.319× | 1.32% | PASS |
| mem_reads | `covering_index_scan` | 4.35ms | 4.16ms | 0.957× | 1.83% | PASS |
| mem_reads | `groupby_scan` | 32.05ms | 34.40ms | 1.073× | 0.80% | PASS |
| mem_reads | `index_join` | 5.91ms | 7.85ms | 1.330× | 1.13% | PASS |
| mem_reads | `index_join_scan` | 3.53ms | 4.83ms | 1.369× | 2.23% | PASS |
| mem_reads | `types_table_scan` | 1.21s | 1.29s | 1.071× | 1.17% | PASS |
| mem_reads | `table_scan` | 1.39s | 1.40s | 1.012× | 2.87% | PASS |
| mem_reads | `oltp_read_only` | 103.49ms | 115.45ms | 1.116× | 0.65% | PASS |
| mem_writes | `oltp_bulk_insert` | 180.67ms | 241.25ms | 1.335× | 0.76% | PASS |
| mem_writes | `oltp_insert` | 15.85ms | 28.11ms | 1.774× | 0.92% | PASS |
| mem_writes | `oltp_update_index` | 51.04ms | 105.42ms | 2.065× | 1.19% | PASS |
| mem_writes | `oltp_update_non_index` | 34.46ms | 58.27ms | 1.691× | 0.93% | PASS |
| mem_writes | `oltp_delete_insert` | 44.25ms | 77.72ms | 1.757× | 0.79% | PASS |
| mem_writes | `oltp_write_only` | 21.77ms | 44.87ms | 2.061× | 1.02% | PASS |
| mem_writes | `types_delete_insert` | 24.54ms | 39.78ms | 1.621× | 0.66% | PASS |
| mem_writes | `oltp_read_write` | 64.68ms | 104.18ms | 1.611× | 0.97% | PASS |
| file_reads | `oltp_point_select` | 106.49ms | 55.94ms | 0.525× | 2.04% | PASS |
| file_reads | `oltp_range_select` | 18.14ms | 14.77ms | 0.814× | 0.80% | PASS |
| file_reads | `oltp_sum_range` | 17.40ms | 14.36ms | 0.826× | 1.08% | PASS |
| file_reads | `oltp_order_range` | 3.46ms | 3.24ms | 0.934× | 1.38% | PASS |
| file_reads | `oltp_distinct_range` | 4.54ms | 4.22ms | 0.930× | 0.78% | PASS |
| file_reads | `oltp_index_scan` | 12.25ms | 8.16ms | 0.666× | 0.89% | PASS |
| file_reads | `select_random_points` | 18.60ms | 14.31ms | 0.769× | 1.30% | PASS |
| file_reads | `select_random_ranges` | 11.23ms | 6.88ms | 0.613× | 0.68% | PASS |
| file_reads | `covering_index_scan` | 12.76ms | 7.33ms | 0.574× | 1.04% | PASS |
| file_reads | `groupby_scan` | 32.56ms | 34.73ms | 1.067× | 0.95% | PASS |
| file_reads | `index_join` | 10.41ms | 9.95ms | 0.956× | 0.93% | PASS |
| file_reads | `index_join_scan` | 4.40ms | 5.12ms | 1.163× | 1.28% | PASS |
| file_reads | `types_table_scan` | 1.10s | 1.26s | 1.137× | 0.54% | PASS |
| file_reads | `table_scan` | 1.27s | 1.37s | 1.084× | 0.39% | PASS |
| file_reads | `oltp_read_only` | 223.02ms | 155.82ms | 0.699× | 0.51% | PASS |
| file_writes | `oltp_bulk_insert` | 194.68ms | 259.37ms | 1.332× | 0.96% | PASS |
| file_writes | `oltp_insert` | 21.91ms | 36.04ms | 1.645× | 1.54% | PASS |
| file_writes | `oltp_update_index` | 77.65ms | 130.49ms | 1.681× | 0.92% | PASS |
| file_writes | `oltp_update_non_index` | 57.35ms | 80.98ms | 1.412× | 1.60% | PASS |
| file_writes | `oltp_delete_insert` | 65.43ms | 97.61ms | 1.492× | 1.34% | PASS |
| file_writes | `oltp_write_only` | 42.04ms | 65.52ms | 1.558× | 2.07% | PASS |
| file_writes | `types_delete_insert` | 39.12ms | 52.88ms | 1.352× | 1.52% | PASS |
| file_writes | `oltp_read_write` | 85.77ms | 124.12ms | 1.447× | 1.27% | PASS |
| ac_reads | `oltp_point_select` | 51.69ms | 55.64ms | 1.076× | 1.05% | PASS |
| ac_reads | `oltp_range_select` | 13.53ms | 14.72ms | 1.088× | 1.19% | PASS |
| ac_reads | `oltp_sum_range` | 12.51ms | 14.37ms | 1.149× | 1.57% | PASS |
| ac_reads | `oltp_order_range` | 3.05ms | 3.22ms | 1.055× | 1.20% | PASS |
| ac_reads | `oltp_distinct_range` | 4.08ms | 4.21ms | 1.032× | 0.78% | PASS |
| ac_reads | `oltp_index_scan` | 6.87ms | 8.03ms | 1.169× | 1.12% | PASS |
| ac_reads | `select_random_points` | 13.53ms | 14.15ms | 1.046× | 0.79% | PASS |
| ac_reads | `select_random_ranges` | 5.87ms | 6.84ms | 1.166× | 0.93% | PASS |
| ac_reads | `covering_index_scan` | 7.33ms | 7.18ms | 0.980× | 1.02% | PASS |
| ac_reads | `groupby_scan` | 32.18ms | 34.65ms | 1.077× | 0.66% | PASS |
| ac_reads | `index_join` | 7.58ms | 9.70ms | 1.279× | 0.91% | PASS |
| ac_reads | `index_join_scan` | 3.92ms | 5.07ms | 1.292× | 0.99% | PASS |
| ac_reads | `types_table_scan` | 1.10s | 1.26s | 1.139× | 0.63% | PASS |
| ac_reads | `table_scan` | 1.25s | 1.37s | 1.092× | 0.46% | PASS |
| ac_reads | `oltp_read_only` | 142.97ms | 155.34ms | 1.087× | 0.45% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 16.50ms | 64.50ms | 3.909× | 3.85% | PASS |
| ac_writes | `oltp_insert_ac` | 17.57ms | 79.35ms | 4.516× | 5.03% | PASS |
| ac_writes | `oltp_update_index_ac` | 19.03ms | 94.40ms | 4.960× | 3.46% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 15.53ms | 71.35ms | 4.594× | 3.30% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 17.35ms | 85.14ms | 4.907× | 4.79% | PASS |
| ac_writes | `oltp_write_only_ac` | 17.69ms | 83.80ms | 4.738× | 5.50% | PASS |
| ac_writes | `types_delete_insert_ac` | 15.62ms | 73.04ms | 4.676× | 5.87% | PASS |
| ac_writes | `oltp_read_write_ac` | 22.32ms | 90.34ms | 4.048× | 2.25% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 30.23ms | 35.22ms | 1.165× | 0.95% | PASS |
| mem_reads | `oltp_range_select` | 14.34ms | 13.66ms | 0.952× | 1.67% | PASS |
| mem_reads | `oltp_sum_range` | 12.88ms | 13.38ms | 1.038× | 1.51% | PASS |
| mem_reads | `oltp_order_range` | 3.23ms | 3.18ms | 0.985× | 1.54% | PASS |
| mem_reads | `oltp_distinct_range` | 4.29ms | 4.20ms | 0.978× | 0.85% | PASS |
| mem_reads | `oltp_index_scan` | 4.67ms | 6.05ms | 1.294× | 1.93% | PASS |
| mem_reads | `select_random_points` | 18.17ms | 20.39ms | 1.122× | 1.45% | PASS |
| mem_reads | `select_random_ranges` | 4.24ms | 5.31ms | 1.252× | 1.40% | PASS |
| mem_reads | `covering_index_scan` | 4.88ms | 4.53ms | 0.928× | 2.24% | PASS |
| mem_reads | `groupby_scan` | 34.16ms | 35.56ms | 1.041× | 0.62% | PASS |
| mem_reads | `index_join` | 7.13ms | 8.85ms | 1.241× | 1.89% | PASS |
| mem_reads | `index_join_scan` | 4.93ms | 5.66ms | 1.148× | 1.91% | PASS |
| mem_reads | `types_table_scan` | 1.30s | 1.29s | 0.997× | 6.42% | PASS |
| mem_reads | `table_scan` | 1.46s | 1.38s | 0.944× | 2.62% | PASS |
| mem_reads | `oltp_read_only` | 121.89ms | 129.71ms | 1.064× | 0.76% | PASS |
| mem_writes | `oltp_bulk_insert` | 232.39ms | 340.90ms | 1.467× | 0.73% | PASS |
| mem_writes | `oltp_insert` | 22.58ms | 39.81ms | 1.763× | 1.28% | PASS |
| mem_writes | `oltp_update_index` | 75.81ms | 140.59ms | 1.854× | 1.03% | PASS |
| mem_writes | `oltp_update_non_index` | 53.01ms | 92.60ms | 1.747× | 1.13% | PASS |
| mem_writes | `oltp_delete_insert` | 55.75ms | 110.94ms | 1.990× | 1.33% | PASS |
| mem_writes | `oltp_write_only` | 31.84ms | 66.67ms | 2.094× | 1.07% | PASS |
| mem_writes | `types_delete_insert` | 34.38ms | 56.07ms | 1.631× | 0.88% | PASS |
| mem_writes | `oltp_read_write` | 87.17ms | 137.25ms | 1.575× | 0.80% | PASS |
| file_reads | `oltp_point_select` | 114.68ms | 64.21ms | 0.560× | 1.00% | PASS |
| file_reads | `oltp_range_select` | 22.90ms | 16.73ms | 0.730× | 1.96% | PASS |
| file_reads | `oltp_sum_range` | 21.02ms | 16.46ms | 0.783× | 1.65% | PASS |
| file_reads | `oltp_order_range` | 4.00ms | 3.52ms | 0.879× | 1.81% | PASS |
| file_reads | `oltp_distinct_range` | 5.08ms | 4.53ms | 0.890× | 1.09% | PASS |
| file_reads | `oltp_index_scan` | 13.10ms | 9.14ms | 0.698× | 1.13% | PASS |
| file_reads | `select_random_points` | 26.51ms | 23.24ms | 0.877× | 0.88% | PASS |
| file_reads | `select_random_ranges` | 12.46ms | 8.26ms | 0.663× | 1.27% | PASS |
| file_reads | `covering_index_scan` | 13.89ms | 7.75ms | 0.558× | 0.86% | PASS |
| file_reads | `groupby_scan` | 34.89ms | 35.92ms | 1.029× | 0.84% | PASS |
| file_reads | `index_join` | 12.05ms | 11.38ms | 0.945× | 1.94% | PASS |
| file_reads | `index_join_scan` | 5.99ms | 6.31ms | 1.055× | 1.88% | PASS |
| file_reads | `types_table_scan` | 1.36s | 1.29s | 0.948× | 4.17% | PASS |
| file_reads | `table_scan` | 1.53s | 1.39s | 0.904× | 5.06% | PASS |
| file_reads | `oltp_read_only` | 246.82ms | 173.02ms | 0.701× | 0.62% | PASS |
| file_writes | `oltp_bulk_insert` | 254.54ms | 373.68ms | 1.468× | 0.81% | PASS |
| file_writes | `oltp_insert` | 48.05ms | 52.59ms | 1.094× | 19.85% | PASS |
| file_writes | `oltp_update_index` | 116.33ms | 174.72ms | 1.502× | 1.14% | PASS |
| file_writes | `oltp_update_non_index` | 106.83ms | 117.35ms | 1.098× | 13.64% | PASS |
| file_writes | `oltp_delete_insert` | 92.60ms | 139.33ms | 1.505× | 1.59% | PASS |
| file_writes | `oltp_write_only` | 81.99ms | 90.59ms | 1.105× | 13.81% | PASS |
| file_writes | `types_delete_insert` | 56.70ms | 76.69ms | 1.352× | 2.07% | PASS |
| file_writes | `oltp_read_write` | 138.56ms | 163.41ms | 1.179× | 5.71% | PASS |
| ac_reads | `oltp_point_select` | 58.66ms | 64.47ms | 1.099× | 1.37% | PASS |
| ac_reads | `oltp_range_select` | 18.11ms | 16.77ms | 0.926× | 1.16% | PASS |
| ac_reads | `oltp_sum_range` | 16.59ms | 16.65ms | 1.004× | 1.26% | PASS |
| ac_reads | `oltp_order_range` | 3.83ms | 3.57ms | 0.933× | 1.00% | PASS |
| ac_reads | `oltp_distinct_range` | 4.84ms | 4.58ms | 0.946× | 0.86% | PASS |
| ac_reads | `oltp_index_scan` | 8.26ms | 9.27ms | 1.122× | 0.88% | PASS |
| ac_reads | `select_random_points` | 22.33ms | 23.74ms | 1.063× | 1.02% | PASS |
| ac_reads | `select_random_ranges` | 7.53ms | 8.32ms | 1.106× | 0.91% | PASS |
| ac_reads | `covering_index_scan` | 9.44ms | 7.87ms | 0.834× | 1.20% | PASS |
| ac_reads | `groupby_scan` | 35.75ms | 36.54ms | 1.022× | 0.94% | PASS |
| ac_reads | `index_join` | 10.36ms | 11.98ms | 1.157× | 1.89% | PASS |
| ac_reads | `index_join_scan` | 5.76ms | 6.69ms | 1.161× | 1.94% | PASS |
| ac_reads | `types_table_scan` | 1.35s | 1.29s | 0.959× | 4.20% | PASS |
| ac_reads | `table_scan` | 1.57s | 1.39s | 0.890× | 4.34% | PASS |
| ac_reads | `oltp_read_only` | 173.56ms | 176.59ms | 1.017× | 1.80% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 16.95ms | 66.86ms | 3.943× | 4.21% | PASS |
| ac_writes | `oltp_insert_ac` | 20.80ms | 85.72ms | 4.121× | 5.85% | PASS |
| ac_writes | `oltp_update_index_ac` | 23.40ms | 104.24ms | 4.454× | 7.27% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 17.91ms | 80.26ms | 4.482× | 5.56% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 19.93ms | 91.99ms | 4.616× | 6.79% | PASS |
| ac_writes | `oltp_write_only_ac` | 20.36ms | 91.51ms | 4.496× | 6.65% | PASS |
| ac_writes | `types_delete_insert_ac` | 17.82ms | 81.55ms | 4.575× | 6.01% | PASS |
| ac_writes | `oltp_read_write_ac` | 27.15ms | 100.55ms | 3.704× | 5.28% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 31.48ms | 35.57ms | 1.130× | 1.64% | PASS |
| mem_reads | `oltp_range_select` | 14.37ms | 13.90ms | 0.967× | 2.13% | PASS |
| mem_reads | `oltp_sum_range` | 12.31ms | 13.23ms | 1.075× | 1.46% | PASS |
| mem_reads | `oltp_order_range` | 3.15ms | 3.20ms | 1.013× | 1.28% | PASS |
| mem_reads | `oltp_distinct_range` | 4.21ms | 4.19ms | 0.996× | 0.64% | PASS |
| mem_reads | `oltp_index_scan` | 4.67ms | 5.89ms | 1.262× | 1.63% | PASS |
| mem_reads | `select_random_points` | 18.72ms | 20.66ms | 1.104× | 1.77% | PASS |
| mem_reads | `select_random_ranges` | 4.59ms | 5.45ms | 1.187× | 1.19% | PASS |
| mem_reads | `covering_index_scan` | 5.12ms | 4.97ms | 0.971× | 3.79% | PASS |
| mem_reads | `groupby_scan` | 35.18ms | 36.04ms | 1.025× | 0.76% | PASS |
| mem_reads | `index_join` | 7.54ms | 10.44ms | 1.384× | 2.27% | PASS |
| mem_reads | `index_join_scan` | 4.75ms | 6.42ms | 1.349× | 2.89% | PASS |
| mem_reads | `types_table_scan` | 1.12s | 1.23s | 1.098× | 0.45% | PASS |
| mem_reads | `table_scan` | 1.30s | 1.35s | 1.035× | 0.29% | PASS |
| mem_reads | `oltp_read_only` | 120.29ms | 129.30ms | 1.075× | 1.25% | PASS |
| mem_writes | `oltp_bulk_insert` | 237.74ms | 334.83ms | 1.408× | 0.63% | PASS |
| mem_writes | `oltp_insert` | 20.58ms | 39.31ms | 1.910× | 0.78% | PASS |
| mem_writes | `oltp_update_index` | 70.23ms | 132.67ms | 1.889× | 0.72% | PASS |
| mem_writes | `oltp_update_non_index` | 49.85ms | 85.97ms | 1.724× | 0.83% | PASS |
| mem_writes | `oltp_delete_insert` | 50.35ms | 105.13ms | 2.088× | 0.76% | PASS |
| mem_writes | `oltp_write_only` | 28.96ms | 64.06ms | 2.212× | 0.73% | PASS |
| mem_writes | `types_delete_insert` | 33.20ms | 54.62ms | 1.645× | 0.94% | PASS |
| mem_writes | `oltp_read_write` | 87.61ms | 138.44ms | 1.580× | 1.20% | PASS |
| file_reads | `oltp_point_select` | 115.42ms | 64.06ms | 0.555× | 1.27% | PASS |
| file_reads | `oltp_range_select` | 22.86ms | 16.86ms | 0.737× | 1.72% | PASS |
| file_reads | `oltp_sum_range` | 21.60ms | 16.44ms | 0.761× | 1.70% | PASS |
| file_reads | `oltp_order_range` | 4.00ms | 3.57ms | 0.891× | 3.25% | PASS |
| file_reads | `oltp_distinct_range` | 5.09ms | 4.56ms | 0.896× | 1.43% | PASS |
| file_reads | `oltp_index_scan` | 13.23ms | 9.19ms | 0.694× | 1.31% | PASS |
| file_reads | `select_random_points` | 26.50ms | 23.39ms | 0.882× | 2.48% | PASS |
| file_reads | `select_random_ranges` | 12.51ms | 8.28ms | 0.662× | 1.45% | PASS |
| file_reads | `covering_index_scan` | 13.18ms | 7.74ms | 0.587× | 1.66% | PASS |
| file_reads | `groupby_scan` | 34.54ms | 35.84ms | 1.038× | 0.94% | PASS |
| file_reads | `index_join` | 11.66ms | 11.47ms | 0.984× | 1.36% | PASS |
| file_reads | `index_join_scan` | 5.48ms | 6.40ms | 1.168× | 1.93% | PASS |
| file_reads | `types_table_scan` | 1.13s | 1.23s | 1.093× | 0.60% | PASS |
| file_reads | `table_scan` | 1.30s | 1.35s | 1.034× | 0.43% | PASS |
| file_reads | `oltp_read_only` | 240.80ms | 169.57ms | 0.704× | 0.57% | PASS |
| file_writes | `oltp_bulk_insert` | 260.43ms | 360.33ms | 1.384× | 0.99% | PASS |
| file_writes | `oltp_insert` | 30.91ms | 52.02ms | 1.683× | 1.55% | PASS |
| file_writes | `oltp_update_index` | 104.62ms | 167.09ms | 1.597× | 0.82% | PASS |
| file_writes | `oltp_update_non_index` | 81.52ms | 112.75ms | 1.383× | 1.58% | PASS |
| file_writes | `oltp_delete_insert` | 86.18ms | 138.53ms | 1.607× | 1.59% | PASS |
| file_writes | `oltp_write_only` | 57.54ms | 90.29ms | 1.569× | 1.82% | PASS |
| file_writes | `types_delete_insert` | 53.81ms | 74.94ms | 1.393× | 1.22% | PASS |
| file_writes | `oltp_read_write` | 121.48ms | 165.53ms | 1.363× | 1.05% | PASS |
| ac_reads | `oltp_point_select` | 60.69ms | 65.49ms | 1.079× | 1.06% | PASS |
| ac_reads | `oltp_range_select` | 18.59ms | 17.02ms | 0.916× | 1.00% | PASS |
| ac_reads | `oltp_sum_range` | 16.84ms | 16.68ms | 0.990× | 1.15% | PASS |
| ac_reads | `oltp_order_range` | 3.73ms | 3.58ms | 0.960× | 0.90% | PASS |
| ac_reads | `oltp_distinct_range` | 4.72ms | 4.56ms | 0.965× | 0.62% | PASS |
| ac_reads | `oltp_index_scan` | 8.17ms | 9.27ms | 1.134× | 1.03% | PASS |
| ac_reads | `select_random_points` | 22.16ms | 23.57ms | 1.064× | 1.77% | PASS |
| ac_reads | `select_random_ranges` | 7.54ms | 8.30ms | 1.101× | 0.92% | PASS |
| ac_reads | `covering_index_scan` | 8.66ms | 7.79ms | 0.899× | 0.93% | PASS |
| ac_reads | `groupby_scan` | 34.96ms | 36.09ms | 1.032× | 0.80% | PASS |
| ac_reads | `index_join` | 9.65ms | 11.71ms | 1.213× | 1.68% | PASS |
| ac_reads | `index_join_scan` | 5.18ms | 6.54ms | 1.262× | 2.31% | PASS |
| ac_reads | `types_table_scan` | 1.19s | 1.26s | 1.059× | 2.05% | PASS |
| ac_reads | `table_scan` | 1.46s | 1.39s | 0.956× | 2.62% | PASS |
| ac_reads | `oltp_read_only` | 168.70ms | 173.19ms | 1.027× | 1.22% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 16.47ms | 63.51ms | 3.856× | 3.80% | PASS |
| ac_writes | `oltp_insert_ac` | 18.44ms | 85.48ms | 4.636× | 4.50% | PASS |
| ac_writes | `oltp_update_index_ac` | 20.80ms | 96.89ms | 4.657× | 4.58% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 16.30ms | 75.14ms | 4.610× | 4.95% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 17.88ms | 87.09ms | 4.871× | 4.67% | PASS |
| ac_writes | `oltp_write_only_ac` | 18.46ms | 85.92ms | 4.654× | 4.46% | PASS |
| ac_writes | `types_delete_insert_ac` | 15.48ms | 77.72ms | 5.019× | 4.15% | PASS |
| ac_writes | `oltp_read_write_ac` | 25.40ms | 94.44ms | 3.718× | 4.33% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.37ms | 40.38ms | 1.210× | 1.43% | PASS |
| mem_reads | `oltp_range_select` | 19.51ms | 21.09ms | 1.081× | 1.64% | PASS |
| mem_reads | `oltp_sum_range` | 18.17ms | 20.75ms | 1.142× | 1.30% | PASS |
| mem_reads | `oltp_order_range` | 3.56ms | 3.87ms | 1.086× | 0.73% | PASS |
| mem_reads | `oltp_distinct_range` | 4.61ms | 4.90ms | 1.063× | 1.44% | PASS |
| mem_reads | `oltp_index_scan` | 4.50ms | 6.01ms | 1.338× | 2.12% | PASS |
| mem_reads | `select_random_points` | 27.12ms | 31.47ms | 1.161× | 1.31% | PASS |
| mem_reads | `select_random_ranges` | 7.55ms | 8.86ms | 1.173× | 2.05% | PASS |
| mem_reads | `covering_index_scan` | 4.20ms | 4.21ms | 1.004× | 2.51% | PASS |
| mem_reads | `groupby_scan` | 36.91ms | 39.58ms | 1.072× | 0.93% | PASS |
| mem_reads | `index_join` | 8.07ms | 10.21ms | 1.264× | 1.81% | PASS |
| mem_reads | `index_join_scan` | 4.05ms | 5.31ms | 1.311× | 1.53% | PASS |
| mem_reads | `types_table_scan` | 1.06s | 1.23s | 1.158× | 1.05% | PASS |
| mem_reads | `table_scan` | 1.29s | 1.40s | 1.084× | 1.25% | PASS |
| mem_reads | `oltp_read_only` | 155.15ms | 170.93ms | 1.102× | 1.38% | PASS |
| mem_writes | `oltp_bulk_insert` | 244.06ms | 356.38ms | 1.460× | 0.91% | PASS |
| mem_writes | `oltp_insert` | 19.12ms | 36.92ms | 1.930× | 0.70% | PASS |
| mem_writes | `oltp_update_index` | 69.85ms | 121.68ms | 1.742× | 1.63% | PASS |
| mem_writes | `oltp_update_non_index` | 51.19ms | 83.71ms | 1.635× | 1.11% | PASS |
| mem_writes | `oltp_delete_insert` | 50.18ms | 97.46ms | 1.942× | 1.92% | PASS |
| mem_writes | `oltp_write_only` | 27.57ms | 60.00ms | 2.177× | 1.55% | PASS |
| mem_writes | `types_delete_insert` | 34.08ms | 58.30ms | 1.711× | 1.61% | PASS |
| mem_writes | `oltp_read_write` | 114.40ms | 165.68ms | 1.448× | 1.49% | PASS |
| file_reads | `oltp_point_select` | 101.43ms | 64.77ms | 0.639× | 1.13% | PASS |
| file_reads | `oltp_range_select` | 25.32ms | 23.85ms | 0.942× | 1.86% | PASS |
| file_reads | `oltp_sum_range` | 24.61ms | 23.57ms | 0.958× | 1.49% | PASS |
| file_reads | `oltp_order_range` | 4.26ms | 4.25ms | 0.998× | 1.76% | PASS |
| file_reads | `oltp_distinct_range` | 5.45ms | 5.33ms | 0.977× | 1.76% | PASS |
| file_reads | `oltp_index_scan` | 11.27ms | 8.89ms | 0.789× | 1.22% | PASS |
| file_reads | `select_random_points` | 36.57ms | 35.41ms | 0.968× | 1.79% | PASS |
| file_reads | `select_random_ranges` | 14.52ms | 11.67ms | 0.804× | 1.58% | PASS |
| file_reads | `covering_index_scan` | 11.02ms | 6.87ms | 0.623× | 1.21% | PASS |
| file_reads | `groupby_scan` | 36.76ms | 39.81ms | 1.083× | 0.88% | PASS |
| file_reads | `index_join` | 11.89ms | 12.28ms | 1.033× | 1.85% | PASS |
| file_reads | `index_join_scan` | 4.97ms | 5.79ms | 1.165× | 1.61% | PASS |
| file_reads | `types_table_scan` | 1.05s | 1.22s | 1.162× | 1.28% | PASS |
| file_reads | `table_scan` | 1.26s | 1.39s | 1.100× | 4.21% | PASS |
| file_reads | `oltp_read_only` | 250.73ms | 206.34ms | 0.823× | 1.31% | PASS |
| file_writes | `oltp_bulk_insert` | 257.83ms | 376.12ms | 1.459× | 0.97% | PASS |
| file_writes | `oltp_insert` | 25.85ms | 45.80ms | 1.772× | 1.45% | PASS |
| file_writes | `oltp_update_index` | 93.62ms | 141.01ms | 1.506× | 1.80% | PASS |
| file_writes | `oltp_update_non_index` | 74.79ms | 103.29ms | 1.381× | 1.48% | PASS |
| file_writes | `oltp_delete_insert` | 76.22ms | 119.78ms | 1.572× | 1.84% | PASS |
| file_writes | `oltp_write_only` | 50.13ms | 77.12ms | 1.538× | 1.52% | PASS |
| file_writes | `types_delete_insert` | 48.91ms | 69.06ms | 1.412× | 2.10% | PASS |
| file_writes | `oltp_read_write` | 128.75ms | 176.31ms | 1.369× | 1.25% | PASS |
| ac_reads | `oltp_point_select` | 55.40ms | 64.66ms | 1.167× | 1.15% | PASS |
| ac_reads | `oltp_range_select` | 21.69ms | 23.95ms | 1.104× | 1.46% | PASS |
| ac_reads | `oltp_sum_range` | 20.62ms | 23.65ms | 1.147× | 1.57% | PASS |
| ac_reads | `oltp_order_range` | 3.96ms | 4.29ms | 1.081× | 2.01% | PASS |
| ac_reads | `oltp_distinct_range` | 5.07ms | 5.38ms | 1.062× | 1.54% | PASS |
| ac_reads | `oltp_index_scan` | 7.22ms | 8.96ms | 1.242× | 1.15% | PASS |
| ac_reads | `select_random_points` | 31.51ms | 35.65ms | 1.132× | 1.81% | PASS |
| ac_reads | `select_random_ranges` | 10.26ms | 11.67ms | 1.138× | 1.01% | PASS |
| ac_reads | `covering_index_scan` | 6.85ms | 6.99ms | 1.020× | 1.28% | PASS |
| ac_reads | `groupby_scan` | 36.77ms | 39.99ms | 1.088× | 1.04% | PASS |
| ac_reads | `index_join` | 9.75ms | 12.51ms | 1.283× | 1.47% | PASS |
| ac_reads | `index_join_scan` | 4.61ms | 5.84ms | 1.267× | 1.40% | PASS |
| ac_reads | `types_table_scan` | 1.04s | 1.22s | 1.167× | 0.64% | PASS |
| ac_reads | `table_scan` | 1.19s | 1.38s | 1.151× | 0.54% | PASS |
| ac_reads | `oltp_read_only` | 184.30ms | 206.53ms | 1.121× | 1.03% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 24.27ms | 81.42ms | 3.355× | 5.65% | PASS |
| ac_writes | `oltp_insert_ac` | 27.02ms | 102.69ms | 3.801× | 5.93% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.29ms | 113.38ms | 4.154× | 5.36% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.59ms | 92.33ms | 3.754× | 5.06% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.95ms | 105.57ms | 4.068× | 5.09% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.55ms | 104.88ms | 4.105× | 6.47% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.80ms | 94.53ms | 4.146× | 8.20% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.94ms | 112.39ms | 3.518× | 3.95% | PASS |

</details>

## Version-control latency

Wall time: 2m 25s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 89.59ms | 200.00ms | 44.8% | 0.16% | PASS |
| `status_dirty_many_tables` | 92.95ms | 200.00ms | 46.5% | 0.19% | PASS |
| `diff_regular_working_one_table` | 85.42ms | 150.00ms | 56.9% | 0.16% | PASS |
| `diff_regular_working_many_tables` | 97.96ms | 200.00ms | 49.0% | 0.17% | PASS |
| `diff_stat_working_many_tables` | 98.11ms | 200.00ms | 49.1% | 0.12% | PASS |
| `diff_schema_working_many_tables` | 98.53ms | 200.00ms | 49.3% | 0.13% | PASS |
| `branch_list_many_branches` | 23.44ms | 100.00ms | 23.4% | 0.51% | PASS |
| `branch_create_delete` | 25.34ms | 100.00ms | 25.3% | 0.51% | PASS |
| `checkout_branch_clean` | 57.33ms | 200.00ms | 28.7% | 0.27% | PASS |
| `merge_data_no_conflicts` | 29.91ms | 150.00ms | 19.9% | 0.60% | PASS |
| `merge_schema_no_conflicts` | 22.17ms | 100.00ms | 22.2% | 0.80% | PASS |
| `merge_data_conflicts` | 128.67ms | 250.00ms | 51.5% | 0.14% | PASS |
| `merge_data_conflicts_with_resolve` | 128.56ms | 250.00ms | 51.4% | 0.11% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
