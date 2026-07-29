# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-07-28 11:52 UTC
>
> Commit: [`f56b2d05bdb8d771e3a15e14ebc8c33869e5a3c2`](https://github.com/dolthub/doltlite/commit/f56b2d05bdb8d771e3a15e14ebc8c33869e5a3c2)
>
> Runner: ubuntu24 20260720.247.2
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/30350197849)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians; MAD is the median absolute deviation and describes run-to-run noise.

## SQL workload summary

| Key shape | Workloads | Samples/workload | Wall time | SQLite median total | DoltLite median total | Ratio | Median paired-ratio MAD | Result |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| int | 69 | 55 | 1h 10m 56s | 8.69s | 10.81s | 1.244× | 1.24% | **PASS** |
| textpk | 69 | 55 | 1h 31m 25s | 9.90s | 11.86s | 1.199× | 1.66% | **PASS** |
| blobpk | 69 | 55 | 1h 31m 11s | 9.72s | 11.80s | 1.214× | 1.77% | **PASS** |
| compositepk | 69 | 55 | 1h 28m 2s | 10.17s | 12.09s | 1.189× | 1.63% | **PASS** |

The absolute ceiling is 2.5× per ordinary workload and 2.0× for a section average. Durable autocommit writes use 10.0× and 5.0× ceilings respectively.

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 23.21ms | 29.26ms | 1.261× | 1.33% | PASS |
| mem_reads | `oltp_range_select` | 9.41ms | 13.11ms | 1.393× | 1.86% | PASS |
| mem_reads | `oltp_sum_range` | 8.87ms | 12.40ms | 1.398× | 1.58% | PASS |
| mem_reads | `oltp_order_range` | 2.43ms | 2.94ms | 1.214× | 1.35% | PASS |
| mem_reads | `oltp_distinct_range` | 3.43ms | 4.10ms | 1.195× | 0.94% | PASS |
| mem_reads | `oltp_index_scan` | 3.75ms | 5.22ms | 1.393× | 1.59% | PASS |
| mem_reads | `select_random_points` | 9.24ms | 11.06ms | 1.197× | 2.17% | PASS |
| mem_reads | `select_random_ranges` | 2.81ms | 4.01ms | 1.425× | 1.59% | PASS |
| mem_reads | `covering_index_scan` | 4.15ms | 4.16ms | 1.001× | 1.02% | PASS |
| mem_reads | `groupby_scan` | 29.04ms | 34.55ms | 1.190× | 0.51% | PASS |
| mem_reads | `index_join` | 5.87ms | 8.12ms | 1.382× | 1.24% | PASS |
| mem_reads | `index_join_scan` | 3.14ms | 4.50ms | 1.433× | 2.69% | PASS |
| mem_reads | `types_table_scan` | 1.04s | 1.22s | 1.175× | 0.31% | PASS |
| mem_reads | `table_scan` | 1.16s | 1.32s | 1.135× | 0.34% | PASS |
| mem_reads | `oltp_read_only` | 99.12ms | 123.97ms | 1.251× | 0.79% | PASS |
| mem_writes | `oltp_bulk_insert` | 179.99ms | 250.00ms | 1.389× | 0.94% | PASS |
| mem_writes | `oltp_insert` | 15.38ms | 27.99ms | 1.820× | 0.74% | PASS |
| mem_writes | `oltp_update_index` | 48.31ms | 87.44ms | 1.810× | 0.91% | PASS |
| mem_writes | `oltp_update_non_index` | 33.01ms | 57.91ms | 1.754× | 1.22% | PASS |
| mem_writes | `oltp_delete_insert` | 43.29ms | 69.80ms | 1.612× | 1.15% | PASS |
| mem_writes | `oltp_write_only` | 20.97ms | 43.49ms | 2.074× | 0.98% | PASS |
| mem_writes | `types_delete_insert` | 23.55ms | 39.52ms | 1.678× | 1.39% | PASS |
| mem_writes | `oltp_read_write` | 64.10ms | 109.07ms | 1.702× | 1.51% | PASS |
| file_reads | `oltp_point_select` | 90.87ms | 53.47ms | 0.588× | 0.72% | PASS |
| file_reads | `oltp_range_select` | 16.17ms | 15.74ms | 0.974× | 1.47% | PASS |
| file_reads | `oltp_sum_range` | 16.01ms | 15.12ms | 0.945× | 1.51% | PASS |
| file_reads | `oltp_order_range` | 3.18ms | 3.27ms | 1.029× | 1.40% | PASS |
| file_reads | `oltp_distinct_range` | 4.21ms | 4.47ms | 1.062× | 1.54% | PASS |
| file_reads | `oltp_index_scan` | 10.72ms | 8.06ms | 0.752× | 1.16% | PASS |
| file_reads | `select_random_points` | 16.32ms | 14.04ms | 0.860× | 1.64% | PASS |
| file_reads | `select_random_ranges` | 9.60ms | 6.49ms | 0.676× | 0.66% | PASS |
| file_reads | `covering_index_scan` | 11.06ms | 6.90ms | 0.623× | 0.90% | PASS |
| file_reads | `groupby_scan` | 29.81ms | 35.04ms | 1.175× | 0.87% | PASS |
| file_reads | `index_join` | 9.68ms | 10.00ms | 1.033× | 1.10% | PASS |
| file_reads | `index_join_scan` | 4.17ms | 4.96ms | 1.189× | 1.81% | PASS |
| file_reads | `types_table_scan` | 1.04s | 1.22s | 1.175× | 0.64% | PASS |
| file_reads | `table_scan` | 1.16s | 1.32s | 1.138× | 0.43% | PASS |
| file_reads | `oltp_read_only` | 197.64ms | 160.11ms | 0.810× | 0.79% | PASS |
| file_writes | `oltp_bulk_insert` | 193.17ms | 270.04ms | 1.398× | 0.85% | PASS |
| file_writes | `oltp_insert` | 21.75ms | 35.15ms | 1.616× | 1.12% | PASS |
| file_writes | `oltp_update_index` | 73.90ms | 113.26ms | 1.533× | 1.23% | PASS |
| file_writes | `oltp_update_non_index` | 55.51ms | 79.39ms | 1.430× | 1.21% | PASS |
| file_writes | `oltp_delete_insert` | 65.31ms | 92.30ms | 1.413× | 1.37% | PASS |
| file_writes | `oltp_write_only` | 42.43ms | 62.74ms | 1.479× | 1.20% | PASS |
| file_writes | `types_delete_insert` | 38.50ms | 52.52ms | 1.364× | 1.29% | PASS |
| file_writes | `oltp_read_write` | 86.81ms | 128.19ms | 1.477× | 1.04% | PASS |
| ac_reads | `oltp_point_select` | 46.14ms | 53.48ms | 1.159× | 0.85% | PASS |
| ac_reads | `oltp_range_select` | 12.03ms | 15.71ms | 1.306× | 1.64% | PASS |
| ac_reads | `oltp_sum_range` | 11.53ms | 15.13ms | 1.313× | 1.63% | PASS |
| ac_reads | `oltp_order_range` | 2.76ms | 3.26ms | 1.182× | 1.47% | PASS |
| ac_reads | `oltp_distinct_range` | 3.75ms | 4.46ms | 1.187× | 0.97% | PASS |
| ac_reads | `oltp_index_scan` | 6.23ms | 8.08ms | 1.297× | 1.45% | PASS |
| ac_reads | `select_random_points` | 12.14ms | 14.02ms | 1.155× | 2.25% | PASS |
| ac_reads | `select_random_ranges` | 5.19ms | 6.52ms | 1.255× | 1.38% | PASS |
| ac_reads | `covering_index_scan` | 6.51ms | 6.92ms | 1.063× | 1.30% | PASS |
| ac_reads | `groupby_scan` | 29.31ms | 35.03ms | 1.195× | 0.79% | PASS |
| ac_reads | `index_join` | 7.33ms | 10.11ms | 1.380× | 1.16% | PASS |
| ac_reads | `index_join_scan` | 3.72ms | 4.96ms | 1.334× | 1.28% | PASS |
| ac_reads | `types_table_scan` | 1.04s | 1.22s | 1.176× | 0.49% | PASS |
| ac_reads | `table_scan` | 1.16s | 1.32s | 1.137× | 0.33% | PASS |
| ac_reads | `oltp_read_only` | 132.81ms | 160.04ms | 1.205× | 0.67% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 20.55ms | 74.72ms | 3.636× | 3.29% | PASS |
| ac_writes | `oltp_insert_ac` | 22.64ms | 91.36ms | 4.036× | 2.82% | PASS |
| ac_writes | `oltp_update_index_ac` | 25.06ms | 105.55ms | 4.212× | 3.60% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.01ms | 83.94ms | 3.995× | 3.93% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 22.51ms | 96.21ms | 4.274× | 3.61% | PASS |
| ac_writes | `oltp_write_only_ac` | 22.75ms | 95.05ms | 4.178× | 4.66% | PASS |
| ac_writes | `types_delete_insert_ac` | 20.33ms | 86.75ms | 4.267× | 6.38% | PASS |
| ac_writes | `oltp_read_write_ac` | 27.28ms | 101.94ms | 3.737× | 3.96% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 29.45ms | 37.19ms | 1.263× | 1.50% | PASS |
| mem_reads | `oltp_range_select` | 13.12ms | 15.12ms | 1.152× | 2.04% | PASS |
| mem_reads | `oltp_sum_range` | 11.80ms | 14.27ms | 1.209× | 1.36% | PASS |
| mem_reads | `oltp_order_range` | 2.93ms | 3.22ms | 1.100× | 1.26% | PASS |
| mem_reads | `oltp_distinct_range` | 3.98ms | 4.40ms | 1.104× | 0.99% | PASS |
| mem_reads | `oltp_index_scan` | 4.43ms | 6.22ms | 1.402× | 1.72% | PASS |
| mem_reads | `select_random_points` | 17.52ms | 20.63ms | 1.178× | 1.80% | PASS |
| mem_reads | `select_random_ranges` | 3.94ms | 5.20ms | 1.319× | 1.66% | PASS |
| mem_reads | `covering_index_scan` | 4.43ms | 4.55ms | 1.026× | 2.05% | PASS |
| mem_reads | `groupby_scan` | 31.48ms | 35.44ms | 1.126× | 0.91% | PASS |
| mem_reads | `index_join` | 6.75ms | 9.20ms | 1.363× | 2.00% | PASS |
| mem_reads | `index_join_scan` | 4.53ms | 5.41ms | 1.194× | 3.44% | PASS |
| mem_reads | `types_table_scan` | 1.08s | 1.23s | 1.139× | 1.28% | PASS |
| mem_reads | `table_scan` | 1.35s | 1.35s | 0.997× | 5.35% | PASS |
| mem_reads | `oltp_read_only` | 126.69ms | 144.21ms | 1.138× | 1.71% | PASS |
| mem_writes | `oltp_bulk_insert` | 228.91ms | 356.19ms | 1.556× | 0.94% | PASS |
| mem_writes | `oltp_insert` | 21.64ms | 39.67ms | 1.833× | 1.36% | PASS |
| mem_writes | `oltp_update_index` | 70.09ms | 132.16ms | 1.886× | 1.22% | PASS |
| mem_writes | `oltp_update_non_index` | 47.97ms | 85.71ms | 1.787× | 1.24% | PASS |
| mem_writes | `oltp_delete_insert` | 50.44ms | 102.99ms | 2.042× | 1.33% | PASS |
| mem_writes | `oltp_write_only` | 28.41ms | 60.92ms | 2.144× | 0.74% | PASS |
| mem_writes | `types_delete_insert` | 32.41ms | 54.98ms | 1.696× | 1.37% | PASS |
| mem_writes | `oltp_read_write` | 83.82ms | 139.59ms | 1.665× | 1.44% | PASS |
| file_reads | `oltp_point_select` | 98.61ms | 61.19ms | 0.621× | 1.09% | PASS |
| file_reads | `oltp_range_select` | 20.82ms | 17.64ms | 0.847× | 2.17% | PASS |
| file_reads | `oltp_sum_range` | 19.73ms | 16.90ms | 0.857× | 1.45% | PASS |
| file_reads | `oltp_order_range` | 3.89ms | 3.63ms | 0.934× | 1.76% | PASS |
| file_reads | `oltp_distinct_range` | 4.91ms | 4.77ms | 0.971× | 1.83% | PASS |
| file_reads | `oltp_index_scan` | 11.83ms | 8.97ms | 0.759× | 2.00% | PASS |
| file_reads | `select_random_points` | 25.95ms | 23.79ms | 0.917× | 2.31% | PASS |
| file_reads | `select_random_ranges` | 11.01ms | 7.71ms | 0.700× | 1.14% | PASS |
| file_reads | `covering_index_scan` | 12.49ms | 7.28ms | 0.583× | 2.44% | PASS |
| file_reads | `groupby_scan` | 32.89ms | 35.92ms | 1.092× | 1.23% | PASS |
| file_reads | `index_join` | 11.62ms | 11.27ms | 0.969× | 2.01% | PASS |
| file_reads | `index_join_scan` | 5.53ms | 6.06ms | 1.096× | 2.03% | PASS |
| file_reads | `types_table_scan` | 1.06s | 1.22s | 1.150× | 0.64% | PASS |
| file_reads | `table_scan` | 1.28s | 1.34s | 1.047× | 1.16% | PASS |
| file_reads | `oltp_read_only` | 220.30ms | 176.73ms | 0.802× | 0.91% | PASS |
| file_writes | `oltp_bulk_insert` | 247.53ms | 381.65ms | 1.542× | 1.02% | PASS |
| file_writes | `oltp_insert` | 49.77ms | 51.63ms | 1.037× | 9.97% | PASS |
| file_writes | `oltp_update_index` | 115.03ms | 167.33ms | 1.455× | 1.63% | PASS |
| file_writes | `oltp_update_non_index` | 93.71ms | 110.47ms | 1.179× | 4.13% | PASS |
| file_writes | `oltp_delete_insert` | 95.68ms | 131.54ms | 1.375× | 1.44% | PASS |
| file_writes | `oltp_write_only` | 90.82ms | 83.76ms | 0.922× | 10.77% | PASS |
| file_writes | `types_delete_insert` | 58.43ms | 73.69ms | 1.261× | 2.03% | PASS |
| file_writes | `oltp_read_write` | 144.31ms | 162.44ms | 1.126× | 6.63% | PASS |
| ac_reads | `oltp_point_select` | 52.82ms | 61.33ms | 1.161× | 1.24% | PASS |
| ac_reads | `oltp_range_select` | 16.02ms | 17.70ms | 1.105× | 1.65% | PASS |
| ac_reads | `oltp_sum_range` | 15.13ms | 16.92ms | 1.118× | 2.16% | PASS |
| ac_reads | `oltp_order_range` | 3.38ms | 3.57ms | 1.054× | 1.23% | PASS |
| ac_reads | `oltp_distinct_range` | 4.42ms | 4.74ms | 1.072× | 1.35% | PASS |
| ac_reads | `oltp_index_scan` | 7.27ms | 8.97ms | 1.234× | 1.56% | PASS |
| ac_reads | `select_random_points` | 21.23ms | 24.01ms | 1.131× | 2.10% | PASS |
| ac_reads | `select_random_ranges` | 6.53ms | 7.74ms | 1.185× | 0.76% | PASS |
| ac_reads | `covering_index_scan` | 7.93ms | 7.30ms | 0.920× | 2.35% | PASS |
| ac_reads | `groupby_scan` | 32.25ms | 35.84ms | 1.111× | 0.71% | PASS |
| ac_reads | `index_join` | 8.90ms | 11.21ms | 1.260× | 2.56% | PASS |
| ac_reads | `index_join_scan` | 5.19ms | 5.92ms | 1.140× | 2.98% | PASS |
| ac_reads | `types_table_scan` | 1.07s | 1.22s | 1.146× | 0.73% | PASS |
| ac_reads | `table_scan` | 1.28s | 1.34s | 1.046× | 1.25% | PASS |
| ac_reads | `oltp_read_only` | 153.40ms | 176.85ms | 1.153× | 1.49% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 28.63ms | 98.12ms | 3.427× | 7.93% | PASS |
| ac_writes | `oltp_insert_ac` | 32.54ms | 119.93ms | 3.685× | 5.99% | PASS |
| ac_writes | `oltp_update_index_ac` | 33.39ms | 139.62ms | 4.181× | 7.23% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 29.04ms | 112.83ms | 3.886× | 9.16% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 30.51ms | 128.85ms | 4.224× | 6.66% | PASS |
| ac_writes | `oltp_write_only_ac` | 30.79ms | 123.08ms | 3.997× | 8.45% | PASS |
| ac_writes | `types_delete_insert_ac` | 28.29ms | 111.63ms | 3.946× | 8.59% | PASS |
| ac_writes | `oltp_read_write_ac` | 38.32ms | 133.28ms | 3.478× | 6.66% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 31.28ms | 37.84ms | 1.210× | 1.61% | PASS |
| mem_reads | `oltp_range_select` | 12.84ms | 15.32ms | 1.193× | 1.97% | PASS |
| mem_reads | `oltp_sum_range` | 12.09ms | 14.27ms | 1.180× | 1.20% | PASS |
| mem_reads | `oltp_order_range` | 2.92ms | 3.26ms | 1.118× | 1.32% | PASS |
| mem_reads | `oltp_distinct_range` | 3.98ms | 4.41ms | 1.109× | 1.24% | PASS |
| mem_reads | `oltp_index_scan` | 4.58ms | 6.43ms | 1.402× | 1.80% | PASS |
| mem_reads | `select_random_points` | 18.13ms | 21.19ms | 1.169× | 2.06% | PASS |
| mem_reads | `select_random_ranges` | 4.09ms | 5.34ms | 1.306× | 2.11% | PASS |
| mem_reads | `covering_index_scan` | 4.36ms | 4.79ms | 1.097× | 2.06% | PASS |
| mem_reads | `groupby_scan` | 31.52ms | 35.30ms | 1.120× | 1.04% | PASS |
| mem_reads | `index_join` | 6.78ms | 9.59ms | 1.415× | 1.68% | PASS |
| mem_reads | `index_join_scan` | 4.14ms | 5.50ms | 1.328× | 1.77% | PASS |
| mem_reads | `types_table_scan` | 1.07s | 1.23s | 1.147× | 1.77% | PASS |
| mem_reads | `table_scan` | 1.27s | 1.34s | 1.052× | 2.98% | PASS |
| mem_reads | `oltp_read_only` | 121.63ms | 141.90ms | 1.167× | 1.21% | PASS |
| mem_writes | `oltp_bulk_insert` | 242.23ms | 357.08ms | 1.474× | 0.72% | PASS |
| mem_writes | `oltp_insert` | 19.89ms | 39.15ms | 1.969× | 0.75% | PASS |
| mem_writes | `oltp_update_index` | 69.46ms | 131.02ms | 1.886× | 1.94% | PASS |
| mem_writes | `oltp_update_non_index` | 49.17ms | 84.14ms | 1.711× | 1.41% | PASS |
| mem_writes | `oltp_delete_insert` | 49.46ms | 103.64ms | 2.095× | 1.44% | PASS |
| mem_writes | `oltp_write_only` | 28.57ms | 62.16ms | 2.176× | 1.63% | PASS |
| mem_writes | `types_delete_insert` | 32.69ms | 54.25ms | 1.659× | 1.38% | PASS |
| mem_writes | `oltp_read_write` | 86.03ms | 141.01ms | 1.639× | 1.66% | PASS |
| file_reads | `oltp_point_select` | 99.87ms | 62.22ms | 0.623× | 1.04% | PASS |
| file_reads | `oltp_range_select` | 20.34ms | 18.09ms | 0.889× | 1.68% | PASS |
| file_reads | `oltp_sum_range` | 19.34ms | 17.13ms | 0.886× | 1.79% | PASS |
| file_reads | `oltp_order_range` | 3.77ms | 3.66ms | 0.970× | 3.03% | PASS |
| file_reads | `oltp_distinct_range` | 4.88ms | 4.86ms | 0.998× | 1.82% | PASS |
| file_reads | `oltp_index_scan` | 11.62ms | 9.12ms | 0.784× | 1.53% | PASS |
| file_reads | `select_random_points` | 26.19ms | 24.80ms | 0.947× | 1.65% | PASS |
| file_reads | `select_random_ranges` | 11.11ms | 7.85ms | 0.706× | 1.42% | PASS |
| file_reads | `covering_index_scan` | 11.52ms | 7.34ms | 0.637× | 1.45% | PASS |
| file_reads | `groupby_scan` | 32.85ms | 35.99ms | 1.095× | 0.97% | PASS |
| file_reads | `index_join` | 10.74ms | 11.45ms | 1.066× | 1.79% | PASS |
| file_reads | `index_join_scan` | 5.13ms | 6.04ms | 1.177× | 2.38% | PASS |
| file_reads | `types_table_scan` | 1.07s | 1.23s | 1.148× | 1.12% | PASS |
| file_reads | `table_scan` | 1.28s | 1.34s | 1.052× | 2.90% | PASS |
| file_reads | `oltp_read_only` | 224.28ms | 179.65ms | 0.801× | 1.11% | PASS |
| file_writes | `oltp_bulk_insert` | 257.49ms | 382.73ms | 1.486× | 0.93% | PASS |
| file_writes | `oltp_insert` | 31.86ms | 51.93ms | 1.630× | 2.11% | PASS |
| file_writes | `oltp_update_index` | 103.66ms | 165.34ms | 1.595× | 2.16% | PASS |
| file_writes | `oltp_update_non_index` | 79.76ms | 109.14ms | 1.368× | 1.76% | PASS |
| file_writes | `oltp_delete_insert` | 81.79ms | 131.50ms | 1.608× | 1.78% | PASS |
| file_writes | `oltp_write_only` | 55.72ms | 83.42ms | 1.497× | 2.97% | PASS |
| file_writes | `types_delete_insert` | 52.07ms | 72.45ms | 1.391× | 2.22% | PASS |
| file_writes | `oltp_read_write` | 116.71ms | 163.74ms | 1.403× | 1.96% | PASS |
| ac_reads | `oltp_point_select` | 55.19ms | 62.85ms | 1.139× | 1.18% | PASS |
| ac_reads | `oltp_range_select` | 16.03ms | 18.03ms | 1.125× | 2.16% | PASS |
| ac_reads | `oltp_sum_range` | 15.08ms | 17.08ms | 1.132× | 1.70% | PASS |
| ac_reads | `oltp_order_range` | 3.41ms | 3.65ms | 1.069× | 1.96% | PASS |
| ac_reads | `oltp_distinct_range` | 4.43ms | 4.85ms | 1.094× | 1.13% | PASS |
| ac_reads | `oltp_index_scan` | 7.55ms | 9.25ms | 1.225× | 1.52% | PASS |
| ac_reads | `select_random_points` | 21.47ms | 24.85ms | 1.158× | 1.91% | PASS |
| ac_reads | `select_random_ranges` | 6.78ms | 7.85ms | 1.158× | 1.11% | PASS |
| ac_reads | `covering_index_scan` | 7.64ms | 7.34ms | 0.962× | 2.04% | PASS |
| ac_reads | `groupby_scan` | 32.53ms | 36.01ms | 1.107× | 1.32% | PASS |
| ac_reads | `index_join` | 8.97ms | 11.52ms | 1.284× | 2.22% | PASS |
| ac_reads | `index_join_scan` | 4.77ms | 6.00ms | 1.258× | 2.51% | PASS |
| ac_reads | `types_table_scan` | 1.07s | 1.23s | 1.147× | 1.24% | PASS |
| ac_reads | `table_scan` | 1.29s | 1.35s | 1.044× | 2.64% | PASS |
| ac_reads | `oltp_read_only` | 157.26ms | 179.03ms | 1.138× | 1.29% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 24.07ms | 85.77ms | 3.564× | 7.60% | PASS |
| ac_writes | `oltp_insert_ac` | 26.58ms | 107.14ms | 4.031× | 6.15% | PASS |
| ac_writes | `oltp_update_index_ac` | 28.32ms | 120.75ms | 4.263× | 7.81% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 26.08ms | 101.51ms | 3.892× | 4.77% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.84ms | 110.47ms | 4.116× | 8.51% | PASS |
| ac_writes | `oltp_write_only_ac` | 28.18ms | 110.22ms | 3.912× | 6.19% | PASS |
| ac_writes | `types_delete_insert_ac` | 24.11ms | 100.92ms | 4.186× | 8.09% | PASS |
| ac_writes | `oltp_read_write_ac` | 33.25ms | 119.56ms | 3.596× | 8.00% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.19ms | 40.81ms | 1.230× | 1.79% | PASS |
| mem_reads | `oltp_range_select` | 18.20ms | 22.56ms | 1.240× | 1.60% | PASS |
| mem_reads | `oltp_sum_range` | 17.84ms | 21.34ms | 1.196× | 1.25% | PASS |
| mem_reads | `oltp_order_range` | 3.50ms | 3.99ms | 1.140× | 0.99% | PASS |
| mem_reads | `oltp_distinct_range` | 4.67ms | 5.20ms | 1.112× | 1.28% | PASS |
| mem_reads | `oltp_index_scan` | 4.70ms | 6.47ms | 1.376× | 1.09% | PASS |
| mem_reads | `select_random_points` | 29.19ms | 32.97ms | 1.129× | 1.77% | PASS |
| mem_reads | `select_random_ranges` | 7.87ms | 9.24ms | 1.175× | 0.95% | PASS |
| mem_reads | `covering_index_scan` | 4.21ms | 4.51ms | 1.071× | 1.48% | PASS |
| mem_reads | `groupby_scan` | 36.08ms | 41.82ms | 1.159× | 0.60% | PASS |
| mem_reads | `index_join` | 8.21ms | 11.04ms | 1.345× | 1.78% | PASS |
| mem_reads | `index_join_scan` | 4.12ms | 5.60ms | 1.361× | 1.62% | PASS |
| mem_reads | `types_table_scan` | 1.12s | 1.25s | 1.112× | 2.35% | PASS |
| mem_reads | `table_scan` | 1.20s | 1.33s | 1.109× | 0.76% | PASS |
| mem_reads | `oltp_read_only` | 147.65ms | 175.62ms | 1.189× | 0.99% | PASS |
| mem_writes | `oltp_bulk_insert` | 246.71ms | 357.64ms | 1.450× | 0.78% | PASS |
| mem_writes | `oltp_insert` | 19.04ms | 36.53ms | 1.919× | 0.76% | PASS |
| mem_writes | `oltp_update_index` | 67.85ms | 117.10ms | 1.726× | 1.11% | PASS |
| mem_writes | `oltp_update_non_index` | 50.88ms | 83.46ms | 1.640× | 1.19% | PASS |
| mem_writes | `oltp_delete_insert` | 49.75ms | 96.08ms | 1.931× | 1.15% | PASS |
| mem_writes | `oltp_write_only` | 26.93ms | 57.42ms | 2.132× | 1.04% | PASS |
| mem_writes | `types_delete_insert` | 32.67ms | 54.75ms | 1.676× | 0.91% | PASS |
| mem_writes | `oltp_read_write` | 103.39ms | 157.50ms | 1.523× | 1.92% | PASS |
| file_reads | `oltp_point_select` | 102.11ms | 65.69ms | 0.643× | 0.98% | PASS |
| file_reads | `oltp_range_select` | 26.29ms | 25.69ms | 0.977× | 1.69% | PASS |
| file_reads | `oltp_sum_range` | 25.39ms | 24.36ms | 0.960× | 1.68% | PASS |
| file_reads | `oltp_order_range` | 4.34ms | 4.34ms | 1.000× | 1.31% | PASS |
| file_reads | `oltp_distinct_range` | 5.47ms | 5.58ms | 1.020× | 1.63% | PASS |
| file_reads | `oltp_index_scan` | 11.90ms | 9.07ms | 0.762× | 1.63% | PASS |
| file_reads | `select_random_points` | 37.15ms | 36.23ms | 0.975× | 1.77% | PASS |
| file_reads | `select_random_ranges` | 15.22ms | 12.10ms | 0.795× | 1.13% | PASS |
| file_reads | `covering_index_scan` | 11.82ms | 7.34ms | 0.621× | 1.53% | PASS |
| file_reads | `groupby_scan` | 36.99ms | 42.87ms | 1.159× | 0.96% | PASS |
| file_reads | `index_join` | 12.62ms | 13.23ms | 1.048× | 1.65% | PASS |
| file_reads | `index_join_scan` | 5.15ms | 6.12ms | 1.189× | 2.25% | PASS |
| file_reads | `types_table_scan` | 1.14s | 1.26s | 1.104× | 1.18% | PASS |
| file_reads | `table_scan` | 1.37s | 1.37s | 1.001× | 2.55% | PASS |
| file_reads | `oltp_read_only` | 264.97ms | 221.88ms | 0.837× | 1.18% | PASS |
| file_writes | `oltp_bulk_insert` | 262.71ms | 386.73ms | 1.472× | 0.95% | PASS |
| file_writes | `oltp_insert` | 26.42ms | 46.84ms | 1.773× | 2.14% | PASS |
| file_writes | `oltp_update_index` | 101.65ms | 151.20ms | 1.487× | 1.70% | PASS |
| file_writes | `oltp_update_non_index` | 79.43ms | 107.15ms | 1.349× | 1.64% | PASS |
| file_writes | `oltp_delete_insert` | 79.56ms | 122.94ms | 1.545× | 1.66% | PASS |
| file_writes | `oltp_write_only` | 51.72ms | 78.31ms | 1.514× | 2.26% | PASS |
| file_writes | `types_delete_insert` | 49.35ms | 68.65ms | 1.391× | 1.49% | PASS |
| file_writes | `oltp_read_write` | 131.49ms | 178.58ms | 1.358× | 2.09% | PASS |
| ac_reads | `oltp_point_select` | 58.46ms | 66.84ms | 1.143× | 1.68% | PASS |
| ac_reads | `oltp_range_select` | 22.77ms | 26.12ms | 1.147× | 2.16% | PASS |
| ac_reads | `oltp_sum_range` | 21.57ms | 24.91ms | 1.154× | 1.57% | PASS |
| ac_reads | `oltp_order_range` | 4.00ms | 4.46ms | 1.116× | 1.55% | PASS |
| ac_reads | `oltp_distinct_range` | 5.04ms | 5.68ms | 1.126× | 1.63% | PASS |
| ac_reads | `oltp_index_scan` | 7.33ms | 9.15ms | 1.248× | 1.62% | PASS |
| ac_reads | `select_random_points` | 32.61ms | 36.83ms | 1.129× | 2.05% | PASS |
| ac_reads | `select_random_ranges` | 10.56ms | 12.08ms | 1.144× | 1.40% | PASS |
| ac_reads | `covering_index_scan` | 7.01ms | 7.25ms | 1.033× | 2.08% | PASS |
| ac_reads | `groupby_scan` | 36.18ms | 42.66ms | 1.179× | 0.93% | PASS |
| ac_reads | `index_join` | 10.07ms | 13.16ms | 1.306× | 1.72% | PASS |
| ac_reads | `index_join_scan` | 4.65ms | 6.12ms | 1.314× | 2.77% | PASS |
| ac_reads | `types_table_scan` | 1.10s | 1.24s | 1.129× | 1.99% | PASS |
| ac_reads | `table_scan` | 1.34s | 1.36s | 1.015× | 2.56% | PASS |
| ac_reads | `oltp_read_only` | 190.74ms | 217.84ms | 1.142× | 1.52% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 25.72ms | 84.62ms | 3.290× | 7.02% | PASS |
| ac_writes | `oltp_insert_ac` | 27.86ms | 109.08ms | 3.916× | 4.62% | PASS |
| ac_writes | `oltp_update_index_ac` | 28.85ms | 119.42ms | 4.140× | 4.90% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.92ms | 98.76ms | 3.963× | 6.96% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 26.93ms | 108.33ms | 4.022× | 6.02% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.68ms | 105.68ms | 3.962× | 6.18% | PASS |
| ac_writes | `types_delete_insert_ac` | 23.32ms | 95.58ms | 4.099× | 6.28% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.62ms | 115.45ms | 3.539× | 6.24% | PASS |

</details>

## Version-control latency

Wall time: 2m 27s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 61.90ms | 200.00ms | 30.9% | 0.26% | PASS |
| `status_dirty_many_tables` | 64.04ms | 200.00ms | 32.0% | 0.18% | PASS |
| `diff_regular_working_one_table` | 58.09ms | 150.00ms | 38.7% | 0.19% | PASS |
| `diff_regular_working_many_tables` | 67.94ms | 200.00ms | 34.0% | 0.33% | PASS |
| `diff_stat_working_many_tables` | 67.98ms | 200.00ms | 34.0% | 0.30% | PASS |
| `diff_schema_working_many_tables` | 68.22ms | 200.00ms | 34.1% | 0.30% | PASS |
| `branch_list_many_branches` | 17.70ms | 100.00ms | 17.7% | 0.41% | PASS |
| `branch_create_delete` | 27.54ms | 100.00ms | 27.5% | 3.95% | PASS |
| `checkout_branch_clean` | 105.27ms | 200.00ms | 52.6% | 16.39% | PASS |
| `merge_data_no_conflicts` | 33.88ms | 150.00ms | 22.6% | 2.67% | PASS |
| `merge_schema_no_conflicts` | 19.21ms | 100.00ms | 19.2% | 7.81% | PASS |
| `merge_data_conflicts` | 98.78ms | 250.00ms | 39.5% | 0.14% | PASS |
| `merge_data_conflicts_with_resolve` | 98.83ms | 250.00ms | 39.5% | 0.13% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
