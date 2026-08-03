# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-08-03 12:18 UTC
>
> Commit: [`eae93d208c11d4669cc827b543c7c95f51e21bdb`](https://github.com/dolthub/doltlite/commit/eae93d208c11d4669cc827b543c7c95f51e21bdb)
>
> Runner: ubuntu24 20260720.247.2
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/30806774913)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians; MAD is the median absolute deviation and describes run-to-run noise.

## SQL workload summary

| Key shape | Workloads | Samples/workload | Wall time | SQLite median total | DoltLite median total | Ratio | Median paired-ratio MAD | Result |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| int | 69 | 55 | 1h 11m 20s | 8.85s | 10.97s | 1.240× | 1.38% | **PASS** |
| textpk | 69 | 55 | 1h 18m 13s | 8.86s | 10.18s | 1.149× | 1.32% | **PASS** |
| blobpk | 69 | 55 | 1h 29m 52s | 9.26s | 11.70s | 1.264× | 1.40% | **PASS** |
| compositepk | 69 | 55 | 1h 27m 32s | 10.43s | 12.09s | 1.159× | 1.34% | **PASS** |

The absolute ceiling is 2.5× per ordinary workload and 2.0× for a section average. Durable autocommit writes use 10.0× and 5.0× ceilings respectively.

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 21.47ms | 23.92ms | 1.114× | 1.08% | PASS |
| mem_reads | `oltp_range_select` | 9.67ms | 11.59ms | 1.199× | 1.27% | PASS |
| mem_reads | `oltp_sum_range` | 8.91ms | 10.89ms | 1.222× | 1.17% | PASS |
| mem_reads | `oltp_order_range` | 2.49ms | 2.78ms | 1.119× | 1.68% | PASS |
| mem_reads | `oltp_distinct_range` | 3.42ms | 3.71ms | 1.085× | 1.38% | PASS |
| mem_reads | `oltp_index_scan` | 3.58ms | 4.44ms | 1.239× | 2.00% | PASS |
| mem_reads | `select_random_points` | 10.11ms | 10.83ms | 1.071× | 1.21% | PASS |
| mem_reads | `select_random_ranges` | 2.67ms | 3.48ms | 1.303× | 2.10% | PASS |
| mem_reads | `covering_index_scan` | 3.44ms | 3.63ms | 1.055× | 1.55% | PASS |
| mem_reads | `groupby_scan` | 29.73ms | 31.21ms | 1.050× | 1.17% | PASS |
| mem_reads | `index_join` | 5.35ms | 7.54ms | 1.408× | 1.71% | PASS |
| mem_reads | `index_join_scan` | 2.92ms | 4.30ms | 1.471× | 1.55% | PASS |
| mem_reads | `types_table_scan` | 1.02s | 1.22s | 1.198× | 0.65% | PASS |
| mem_reads | `table_scan` | 1.21s | 1.33s | 1.104× | 1.37% | PASS |
| mem_reads | `oltp_read_only` | 94.51ms | 107.94ms | 1.142× | 0.76% | PASS |
| mem_writes | `oltp_bulk_insert` | 143.28ms | 194.17ms | 1.355× | 0.72% | PASS |
| mem_writes | `oltp_insert` | 12.88ms | 22.97ms | 1.782× | 1.11% | PASS |
| mem_writes | `oltp_update_index` | 44.53ms | 79.17ms | 1.778× | 1.28% | PASS |
| mem_writes | `oltp_update_non_index` | 29.27ms | 50.58ms | 1.728× | 1.22% | PASS |
| mem_writes | `oltp_delete_insert` | 39.95ms | 60.94ms | 1.526× | 0.83% | PASS |
| mem_writes | `oltp_write_only` | 18.77ms | 37.30ms | 1.987× | 1.43% | PASS |
| mem_writes | `types_delete_insert` | 21.00ms | 33.17ms | 1.580× | 1.33% | PASS |
| mem_writes | `oltp_read_write` | 57.20ms | 92.99ms | 1.626× | 1.05% | PASS |
| file_reads | `oltp_point_select` | 51.40ms | 34.10ms | 0.663× | 1.35% | PASS |
| file_reads | `oltp_range_select` | 12.88ms | 12.81ms | 0.995× | 1.13% | PASS |
| file_reads | `oltp_sum_range` | 12.57ms | 12.19ms | 0.970× | 1.04% | PASS |
| file_reads | `oltp_order_range` | 2.88ms | 2.96ms | 1.028× | 1.60% | PASS |
| file_reads | `oltp_distinct_range` | 3.80ms | 3.84ms | 1.011× | 1.11% | PASS |
| file_reads | `oltp_index_scan` | 6.97ms | 5.87ms | 0.843× | 1.68% | PASS |
| file_reads | `select_random_points` | 14.11ms | 12.35ms | 0.875× | 1.10% | PASS |
| file_reads | `select_random_ranges` | 5.83ms | 4.60ms | 0.790× | 1.80% | PASS |
| file_reads | `covering_index_scan` | 6.75ms | 4.95ms | 0.733× | 1.75% | PASS |
| file_reads | `groupby_scan` | 29.89ms | 31.22ms | 1.044× | 0.90% | PASS |
| file_reads | `index_join` | 7.20ms | 8.46ms | 1.174× | 1.36% | PASS |
| file_reads | `index_join_scan` | 3.36ms | 4.51ms | 1.341× | 2.17% | PASS |
| file_reads | `types_table_scan` | 1.01s | 1.21s | 1.205× | 0.51% | PASS |
| file_reads | `table_scan` | 1.17s | 1.32s | 1.134× | 0.96% | PASS |
| file_reads | `oltp_read_only` | 137.98ms | 123.84ms | 0.897× | 0.65% | PASS |
| file_writes | `oltp_bulk_insert` | 194.99ms | 258.50ms | 1.326× | 3.44% | PASS |
| file_writes | `oltp_insert` | 28.51ms | 40.76ms | 1.429× | 6.40% | PASS |
| file_writes | `oltp_update_index` | 150.43ms | 151.28ms | 1.006× | 1.51% | PASS |
| file_writes | `oltp_update_non_index` | 121.19ms | 107.39ms | 0.886× | 2.09% | PASS |
| file_writes | `oltp_delete_insert` | 129.56ms | 121.50ms | 0.938× | 1.57% | PASS |
| file_writes | `oltp_write_only` | 95.61ms | 88.37ms | 0.924× | 1.71% | PASS |
| file_writes | `types_delete_insert` | 82.13ms | 71.06ms | 0.865× | 3.12% | PASS |
| file_writes | `oltp_read_write` | 136.24ms | 145.36ms | 1.067× | 2.63% | PASS |
| ac_reads | `oltp_point_select` | 30.79ms | 34.22ms | 1.111× | 1.27% | PASS |
| ac_reads | `oltp_range_select` | 10.90ms | 12.94ms | 1.187× | 1.05% | PASS |
| ac_reads | `oltp_sum_range` | 10.57ms | 12.28ms | 1.161× | 1.39% | PASS |
| ac_reads | `oltp_order_range` | 2.67ms | 2.97ms | 1.113× | 1.34% | PASS |
| ac_reads | `oltp_distinct_range` | 3.58ms | 3.88ms | 1.081× | 1.83% | PASS |
| ac_reads | `oltp_index_scan` | 4.86ms | 5.85ms | 1.205× | 1.82% | PASS |
| ac_reads | `select_random_points` | 11.80ms | 12.28ms | 1.041× | 1.23% | PASS |
| ac_reads | `select_random_ranges` | 3.74ms | 4.57ms | 1.222× | 1.52% | PASS |
| ac_reads | `covering_index_scan` | 4.61ms | 4.95ms | 1.074× | 1.49% | PASS |
| ac_reads | `groupby_scan` | 29.54ms | 31.08ms | 1.052× | 0.73% | PASS |
| ac_reads | `index_join` | 6.23ms | 8.45ms | 1.356× | 1.40% | PASS |
| ac_reads | `index_join_scan` | 3.17ms | 4.51ms | 1.422× | 1.38% | PASS |
| ac_reads | `types_table_scan` | 1.01s | 1.22s | 1.207× | 0.59% | PASS |
| ac_reads | `table_scan` | 1.16s | 1.32s | 1.141× | 0.63% | PASS |
| ac_reads | `oltp_read_only` | 106.91ms | 123.33ms | 1.154× | 0.87% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 29.58ms | 112.11ms | 3.790× | 15.13% | PASS |
| ac_writes | `oltp_insert_ac` | 28.76ms | 126.17ms | 4.387× | 14.16% | PASS |
| ac_writes | `oltp_update_index_ac` | 30.62ms | 135.06ms | 4.410× | 10.95% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 26.69ms | 117.08ms | 4.387× | 15.28% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 32.00ms | 135.57ms | 4.237× | 13.35% | PASS |
| ac_writes | `oltp_write_only_ac` | 35.26ms | 131.68ms | 3.734× | 25.99% | PASS |
| ac_writes | `types_delete_insert_ac` | 30.75ms | 140.34ms | 4.564× | 22.41% | PASS |
| ac_writes | `oltp_read_write_ac` | 37.42ms | 138.65ms | 3.705× | 16.95% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 26.23ms | 31.04ms | 1.183× | 1.40% | PASS |
| mem_reads | `oltp_range_select` | 11.53ms | 12.17ms | 1.056× | 0.83% | PASS |
| mem_reads | `oltp_sum_range` | 10.97ms | 12.00ms | 1.094× | 1.03% | PASS |
| mem_reads | `oltp_order_range` | 2.62ms | 2.82ms | 1.077× | 0.64% | PASS |
| mem_reads | `oltp_distinct_range` | 3.37ms | 3.67ms | 1.089× | 0.55% | PASS |
| mem_reads | `oltp_index_scan` | 3.94ms | 5.23ms | 1.329× | 1.16% | PASS |
| mem_reads | `select_random_points` | 15.82ms | 19.56ms | 1.237× | 1.24% | PASS |
| mem_reads | `select_random_ranges` | 3.41ms | 4.56ms | 1.337× | 0.95% | PASS |
| mem_reads | `covering_index_scan` | 4.04ms | 4.00ms | 0.991× | 1.81% | PASS |
| mem_reads | `groupby_scan` | 29.23ms | 29.74ms | 1.018× | 0.45% | PASS |
| mem_reads | `index_join` | 6.08ms | 8.18ms | 1.346× | 1.35% | PASS |
| mem_reads | `index_join_scan` | 3.86ms | 5.00ms | 1.295× | 0.78% | PASS |
| mem_reads | `types_table_scan` | 992.74ms | 1.11s | 1.119× | 1.18% | PASS |
| mem_reads | `table_scan` | 1.25s | 1.21s | 0.971× | 1.97% | PASS |
| mem_reads | `oltp_read_only` | 105.38ms | 118.58ms | 1.125× | 0.68% | PASS |
| mem_writes | `oltp_bulk_insert` | 198.87ms | 292.45ms | 1.471× | 0.74% | PASS |
| mem_writes | `oltp_insert` | 18.83ms | 33.20ms | 1.763× | 0.90% | PASS |
| mem_writes | `oltp_update_index` | 63.09ms | 117.76ms | 1.866× | 1.64% | PASS |
| mem_writes | `oltp_update_non_index` | 42.09ms | 72.21ms | 1.716× | 0.97% | PASS |
| mem_writes | `oltp_delete_insert` | 43.60ms | 88.78ms | 2.036× | 1.02% | PASS |
| mem_writes | `oltp_write_only` | 24.22ms | 51.52ms | 2.127× | 0.88% | PASS |
| mem_writes | `types_delete_insert` | 27.49ms | 46.29ms | 1.684× | 0.67% | PASS |
| mem_writes | `oltp_read_write` | 73.67ms | 117.63ms | 1.597× | 1.17% | PASS |
| file_reads | `oltp_point_select` | 55.86ms | 41.98ms | 0.752× | 1.50% | PASS |
| file_reads | `oltp_range_select` | 14.49ms | 13.50ms | 0.932× | 0.87% | PASS |
| file_reads | `oltp_sum_range` | 14.12ms | 13.46ms | 0.953× | 1.31% | PASS |
| file_reads | `oltp_order_range` | 2.98ms | 3.00ms | 1.009× | 1.77% | PASS |
| file_reads | `oltp_distinct_range` | 3.75ms | 3.87ms | 1.031× | 1.26% | PASS |
| file_reads | `oltp_index_scan` | 7.15ms | 6.61ms | 0.924× | 1.40% | PASS |
| file_reads | `select_random_points` | 19.45ms | 21.32ms | 1.096× | 1.56% | PASS |
| file_reads | `select_random_ranges` | 6.48ms | 5.74ms | 0.887× | 1.56% | PASS |
| file_reads | `covering_index_scan` | 7.39ms | 5.34ms | 0.723× | 1.64% | PASS |
| file_reads | `groupby_scan` | 29.70ms | 30.04ms | 1.012× | 0.71% | PASS |
| file_reads | `index_join` | 8.09ms | 9.42ms | 1.165× | 1.28% | PASS |
| file_reads | `index_join_scan` | 4.35ms | 5.26ms | 1.210× | 1.17% | PASS |
| file_reads | `types_table_scan` | 1.01s | 1.12s | 1.111× | 1.59% | PASS |
| file_reads | `table_scan` | 1.24s | 1.22s | 0.982× | 1.43% | PASS |
| file_reads | `oltp_read_only` | 150.80ms | 136.48ms | 0.905× | 0.85% | PASS |
| file_writes | `oltp_bulk_insert` | 214.25ms | 311.97ms | 1.456× | 0.84% | PASS |
| file_writes | `oltp_insert` | 54.34ms | 42.39ms | 0.780× | 18.29% | PASS |
| file_writes | `oltp_update_index` | 85.18ms | 139.24ms | 1.635× | 1.22% | PASS |
| file_writes | `oltp_update_non_index` | 86.47ms | 88.74ms | 1.026× | 16.43% | PASS |
| file_writes | `oltp_delete_insert` | 64.97ms | 108.18ms | 1.665× | 1.32% | PASS |
| file_writes | `oltp_write_only` | 60.95ms | 67.00ms | 1.099× | 12.06% | PASS |
| file_writes | `types_delete_insert` | 40.50ms | 59.62ms | 1.472× | 1.43% | PASS |
| file_writes | `oltp_read_write` | 114.38ms | 133.12ms | 1.164× | 11.99% | PASS |
| ac_reads | `oltp_point_select` | 35.43ms | 41.86ms | 1.181× | 1.42% | PASS |
| ac_reads | `oltp_range_select` | 12.59ms | 13.44ms | 1.067× | 1.22% | PASS |
| ac_reads | `oltp_sum_range` | 12.28ms | 13.48ms | 1.098× | 0.90% | PASS |
| ac_reads | `oltp_order_range` | 2.80ms | 3.03ms | 1.083× | 1.97% | PASS |
| ac_reads | `oltp_distinct_range` | 3.54ms | 3.91ms | 1.104× | 1.19% | PASS |
| ac_reads | `oltp_index_scan` | 5.26ms | 6.61ms | 1.256× | 1.49% | PASS |
| ac_reads | `select_random_points` | 17.39ms | 21.27ms | 1.224× | 1.37% | PASS |
| ac_reads | `select_random_ranges` | 4.53ms | 5.76ms | 1.271× | 1.12% | PASS |
| ac_reads | `covering_index_scan` | 5.76ms | 5.35ms | 0.928× | 1.73% | PASS |
| ac_reads | `groupby_scan` | 29.63ms | 30.05ms | 1.014× | 0.34% | PASS |
| ac_reads | `index_join` | 7.40ms | 9.41ms | 1.271× | 1.54% | PASS |
| ac_reads | `index_join_scan` | 4.23ms | 5.26ms | 1.242× | 1.38% | PASS |
| ac_reads | `types_table_scan` | 989.82ms | 1.11s | 1.123× | 1.00% | PASS |
| ac_reads | `table_scan` | 1.22s | 1.21s | 0.993× | 1.39% | PASS |
| ac_reads | `oltp_read_only` | 120.36ms | 135.85ms | 1.129× | 1.13% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 14.97ms | 57.72ms | 3.855× | 6.30% | PASS |
| ac_writes | `oltp_insert_ac` | 18.24ms | 70.29ms | 3.854× | 6.22% | PASS |
| ac_writes | `oltp_update_index_ac` | 20.98ms | 85.80ms | 4.089× | 5.72% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 15.56ms | 66.62ms | 4.281× | 5.37% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 17.16ms | 76.42ms | 4.454× | 5.39% | PASS |
| ac_writes | `oltp_write_only_ac` | 17.22ms | 76.44ms | 4.440× | 6.27% | PASS |
| ac_writes | `types_delete_insert_ac` | 15.11ms | 67.45ms | 4.463× | 5.18% | PASS |
| ac_writes | `oltp_read_write_ac` | 21.59ms | 82.61ms | 3.826× | 4.55% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 29.62ms | 36.88ms | 1.245× | 1.11% | PASS |
| mem_reads | `oltp_range_select` | 12.19ms | 13.95ms | 1.145× | 1.69% | PASS |
| mem_reads | `oltp_sum_range` | 11.44ms | 13.70ms | 1.197× | 1.19% | PASS |
| mem_reads | `oltp_order_range` | 2.85ms | 3.19ms | 1.117× | 1.48% | PASS |
| mem_reads | `oltp_distinct_range` | 3.92ms | 4.22ms | 1.075× | 1.23% | PASS |
| mem_reads | `oltp_index_scan` | 4.48ms | 6.17ms | 1.378× | 1.22% | PASS |
| mem_reads | `select_random_points` | 17.20ms | 20.43ms | 1.188× | 1.60% | PASS |
| mem_reads | `select_random_ranges` | 3.92ms | 5.26ms | 1.342× | 1.92% | PASS |
| mem_reads | `covering_index_scan` | 4.37ms | 4.49ms | 1.027× | 1.53% | PASS |
| mem_reads | `groupby_scan` | 31.30ms | 34.06ms | 1.088× | 0.83% | PASS |
| mem_reads | `index_join` | 6.80ms | 8.97ms | 1.319× | 1.40% | PASS |
| mem_reads | `index_join_scan` | 4.18ms | 5.39ms | 1.291× | 2.45% | PASS |
| mem_reads | `types_table_scan` | 1.04s | 1.24s | 1.198× | 0.66% | PASS |
| mem_reads | `table_scan` | 1.18s | 1.34s | 1.127× | 0.59% | PASS |
| mem_reads | `oltp_read_only` | 114.69ms | 134.94ms | 1.177× | 0.81% | PASS |
| mem_writes | `oltp_bulk_insert` | 242.23ms | 352.41ms | 1.455× | 0.63% | PASS |
| mem_writes | `oltp_insert` | 19.81ms | 38.75ms | 1.956× | 0.78% | PASS |
| mem_writes | `oltp_update_index` | 66.64ms | 127.71ms | 1.916× | 0.94% | PASS |
| mem_writes | `oltp_update_non_index` | 47.70ms | 83.67ms | 1.754× | 1.13% | PASS |
| mem_writes | `oltp_delete_insert` | 48.09ms | 101.36ms | 2.108× | 1.06% | PASS |
| mem_writes | `oltp_write_only` | 27.26ms | 60.51ms | 2.220× | 1.12% | PASS |
| mem_writes | `types_delete_insert` | 31.83ms | 53.14ms | 1.670× | 1.13% | PASS |
| mem_writes | `oltp_read_write` | 81.57ms | 136.82ms | 1.677× | 1.36% | PASS |
| file_reads | `oltp_point_select` | 98.55ms | 61.11ms | 0.620× | 0.84% | PASS |
| file_reads | `oltp_range_select` | 19.58ms | 16.64ms | 0.850× | 1.68% | PASS |
| file_reads | `oltp_sum_range` | 19.10ms | 16.46ms | 0.862× | 1.95% | PASS |
| file_reads | `oltp_order_range` | 3.71ms | 3.58ms | 0.965× | 1.94% | PASS |
| file_reads | `oltp_distinct_range` | 4.79ms | 4.63ms | 0.967× | 2.14% | PASS |
| file_reads | `oltp_index_scan` | 11.63ms | 8.97ms | 0.771× | 1.33% | PASS |
| file_reads | `select_random_points` | 25.79ms | 23.88ms | 0.926× | 1.74% | PASS |
| file_reads | `select_random_ranges` | 10.82ms | 7.79ms | 0.720× | 1.47% | PASS |
| file_reads | `covering_index_scan` | 11.48ms | 7.26ms | 0.633× | 1.09% | PASS |
| file_reads | `groupby_scan` | 32.25ms | 34.69ms | 1.076× | 1.19% | PASS |
| file_reads | `index_join` | 10.85ms | 11.28ms | 1.039× | 1.34% | PASS |
| file_reads | `index_join_scan` | 5.17ms | 5.87ms | 1.135× | 2.43% | PASS |
| file_reads | `types_table_scan` | 1.04s | 1.24s | 1.197× | 0.57% | PASS |
| file_reads | `table_scan` | 1.19s | 1.34s | 1.126× | 0.70% | PASS |
| file_reads | `oltp_read_only` | 217.54ms | 171.56ms | 0.789× | 0.82% | PASS |
| file_writes | `oltp_bulk_insert` | 261.64ms | 378.41ms | 1.446× | 0.86% | PASS |
| file_writes | `oltp_insert` | 31.86ms | 51.77ms | 1.625× | 2.33% | PASS |
| file_writes | `oltp_update_index` | 100.94ms | 161.33ms | 1.598× | 1.73% | PASS |
| file_writes | `oltp_update_non_index` | 77.67ms | 107.70ms | 1.387× | 1.18% | PASS |
| file_writes | `oltp_delete_insert` | 79.41ms | 128.81ms | 1.622× | 2.33% | PASS |
| file_writes | `oltp_write_only` | 54.23ms | 82.94ms | 1.529× | 1.95% | PASS |
| file_writes | `types_delete_insert` | 50.98ms | 71.19ms | 1.396× | 1.52% | PASS |
| file_writes | `oltp_read_write` | 112.38ms | 159.36ms | 1.418× | 1.61% | PASS |
| ac_reads | `oltp_point_select` | 54.08ms | 61.15ms | 1.131× | 0.84% | PASS |
| ac_reads | `oltp_range_select` | 15.66ms | 16.61ms | 1.061× | 1.05% | PASS |
| ac_reads | `oltp_sum_range` | 14.89ms | 16.47ms | 1.106× | 1.13% | PASS |
| ac_reads | `oltp_order_range` | 3.37ms | 3.59ms | 1.064× | 2.10% | PASS |
| ac_reads | `oltp_distinct_range` | 4.38ms | 4.63ms | 1.057× | 2.09% | PASS |
| ac_reads | `oltp_index_scan` | 7.39ms | 8.97ms | 1.214× | 1.90% | PASS |
| ac_reads | `select_random_points` | 21.28ms | 23.97ms | 1.126× | 1.18% | PASS |
| ac_reads | `select_random_ranges` | 6.62ms | 7.81ms | 1.179× | 1.41% | PASS |
| ac_reads | `covering_index_scan` | 7.50ms | 7.25ms | 0.967× | 2.34% | PASS |
| ac_reads | `groupby_scan` | 32.01ms | 34.62ms | 1.082× | 0.64% | PASS |
| ac_reads | `index_join` | 8.86ms | 11.29ms | 1.274× | 1.64% | PASS |
| ac_reads | `index_join_scan` | 4.76ms | 5.85ms | 1.230× | 1.42% | PASS |
| ac_reads | `types_table_scan` | 1.04s | 1.24s | 1.197× | 0.62% | PASS |
| ac_reads | `table_scan` | 1.19s | 1.33s | 1.125× | 0.56% | PASS |
| ac_reads | `oltp_read_only` | 152.06ms | 171.33ms | 1.127× | 0.75% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 23.83ms | 84.35ms | 3.539× | 5.50% | PASS |
| ac_writes | `oltp_insert_ac` | 26.71ms | 108.70ms | 4.070× | 7.71% | PASS |
| ac_writes | `oltp_update_index_ac` | 28.86ms | 116.63ms | 4.041× | 6.35% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.91ms | 96.52ms | 4.037× | 4.68% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.78ms | 109.46ms | 4.246× | 5.46% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.53ms | 108.28ms | 4.240× | 6.27% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.98ms | 98.47ms | 4.285× | 6.22% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.95ms | 115.49ms | 3.615× | 5.71% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 34.23ms | 38.12ms | 1.114× | 1.70% | PASS |
| mem_reads | `oltp_range_select` | 20.55ms | 20.33ms | 0.989× | 2.04% | PASS |
| mem_reads | `oltp_sum_range` | 18.68ms | 19.42ms | 1.040× | 1.33% | PASS |
| mem_reads | `oltp_order_range` | 3.83ms | 3.86ms | 1.006× | 0.97% | PASS |
| mem_reads | `oltp_distinct_range` | 4.93ms | 4.87ms | 0.988× | 0.89% | PASS |
| mem_reads | `oltp_index_scan` | 4.83ms | 6.27ms | 1.297× | 2.06% | PASS |
| mem_reads | `select_random_points` | 28.50ms | 31.42ms | 1.102× | 1.45% | PASS |
| mem_reads | `select_random_ranges` | 7.75ms | 8.47ms | 1.094× | 1.26% | PASS |
| mem_reads | `covering_index_scan` | 4.42ms | 4.57ms | 1.035× | 2.64% | PASS |
| mem_reads | `groupby_scan` | 39.83ms | 40.35ms | 1.013× | 1.05% | PASS |
| mem_reads | `index_join` | 8.24ms | 10.32ms | 1.254× | 1.79% | PASS |
| mem_reads | `index_join_scan` | 4.41ms | 5.74ms | 1.304× | 1.45% | PASS |
| mem_reads | `types_table_scan` | 1.16s | 1.29s | 1.112× | 2.03% | PASS |
| mem_reads | `table_scan` | 1.40s | 1.42s | 1.012× | 3.40% | PASS |
| mem_reads | `oltp_read_only` | 157.49ms | 163.31ms | 1.037× | 1.82% | PASS |
| mem_writes | `oltp_bulk_insert` | 247.32ms | 347.10ms | 1.403× | 0.91% | PASS |
| mem_writes | `oltp_insert` | 19.76ms | 36.93ms | 1.869× | 0.64% | PASS |
| mem_writes | `oltp_update_index` | 69.15ms | 120.17ms | 1.738× | 1.22% | PASS |
| mem_writes | `oltp_update_non_index` | 53.14ms | 85.31ms | 1.605× | 0.91% | PASS |
| mem_writes | `oltp_delete_insert` | 51.69ms | 99.45ms | 1.924× | 1.30% | PASS |
| mem_writes | `oltp_write_only` | 28.32ms | 60.73ms | 2.145× | 1.57% | PASS |
| mem_writes | `types_delete_insert` | 33.64ms | 55.04ms | 1.636× | 1.90% | PASS |
| mem_writes | `oltp_read_write` | 100.77ms | 149.69ms | 1.485× | 1.37% | PASS |
| file_reads | `oltp_point_select` | 117.63ms | 66.94ms | 0.569× | 0.87% | PASS |
| file_reads | `oltp_range_select` | 28.58ms | 23.25ms | 0.813× | 2.22% | PASS |
| file_reads | `oltp_sum_range` | 27.16ms | 22.56ms | 0.830× | 1.42% | PASS |
| file_reads | `oltp_order_range` | 4.50ms | 4.18ms | 0.930× | 1.54% | PASS |
| file_reads | `oltp_distinct_range` | 5.78ms | 5.24ms | 0.906× | 1.74% | PASS |
| file_reads | `oltp_index_scan` | 13.33ms | 9.30ms | 0.698× | 1.32% | PASS |
| file_reads | `select_random_points` | 36.85ms | 34.42ms | 0.934× | 1.81% | PASS |
| file_reads | `select_random_ranges` | 16.07ms | 11.51ms | 0.716× | 1.04% | PASS |
| file_reads | `covering_index_scan` | 12.86ms | 7.64ms | 0.594× | 0.74% | PASS |
| file_reads | `groupby_scan` | 40.59ms | 40.73ms | 1.004× | 0.86% | PASS |
| file_reads | `index_join` | 12.75ms | 12.62ms | 0.990× | 1.02% | PASS |
| file_reads | `index_join_scan` | 5.40ms | 6.32ms | 1.171× | 2.25% | PASS |
| file_reads | `types_table_scan` | 1.15s | 1.29s | 1.129× | 1.36% | PASS |
| file_reads | `table_scan` | 1.40s | 1.41s | 1.012× | 3.24% | PASS |
| file_reads | `oltp_read_only` | 273.64ms | 203.40ms | 0.743× | 0.89% | PASS |
| file_writes | `oltp_bulk_insert` | 263.93ms | 369.80ms | 1.401× | 1.19% | PASS |
| file_writes | `oltp_insert` | 26.42ms | 47.07ms | 1.782× | 1.70% | PASS |
| file_writes | `oltp_update_index` | 97.98ms | 146.94ms | 1.500× | 1.08% | PASS |
| file_writes | `oltp_update_non_index` | 77.55ms | 107.05ms | 1.380× | 1.72% | PASS |
| file_writes | `oltp_delete_insert` | 77.21ms | 121.55ms | 1.574× | 1.62% | PASS |
| file_writes | `oltp_write_only` | 50.11ms | 80.09ms | 1.598× | 2.31% | PASS |
| file_writes | `types_delete_insert` | 49.19ms | 67.67ms | 1.376× | 1.17% | PASS |
| file_writes | `oltp_read_write` | 121.47ms | 169.30ms | 1.394× | 0.92% | PASS |
| ac_reads | `oltp_point_select` | 61.28ms | 66.68ms | 1.088× | 1.15% | PASS |
| ac_reads | `oltp_range_select` | 22.78ms | 23.12ms | 1.015× | 1.04% | PASS |
| ac_reads | `oltp_sum_range` | 21.34ms | 22.41ms | 1.050× | 1.28% | PASS |
| ac_reads | `oltp_order_range` | 4.09ms | 4.18ms | 1.021× | 1.17% | PASS |
| ac_reads | `oltp_distinct_range` | 5.17ms | 5.24ms | 1.013× | 1.15% | PASS |
| ac_reads | `oltp_index_scan` | 7.86ms | 9.25ms | 1.177× | 1.34% | PASS |
| ac_reads | `select_random_points` | 30.88ms | 34.12ms | 1.105× | 1.08% | PASS |
| ac_reads | `select_random_ranges` | 10.44ms | 11.45ms | 1.097× | 1.29% | PASS |
| ac_reads | `covering_index_scan` | 7.36ms | 7.49ms | 1.017× | 0.82% | PASS |
| ac_reads | `groupby_scan` | 39.49ms | 40.58ms | 1.028× | 0.69% | PASS |
| ac_reads | `index_join` | 10.12ms | 12.63ms | 1.247× | 1.05% | PASS |
| ac_reads | `index_join_scan` | 4.79ms | 6.21ms | 1.298× | 1.76% | PASS |
| ac_reads | `types_table_scan` | 1.12s | 1.27s | 1.137× | 0.64% | PASS |
| ac_reads | `table_scan` | 1.33s | 1.40s | 1.049× | 1.24% | PASS |
| ac_reads | `oltp_read_only` | 192.82ms | 202.60ms | 1.051× | 0.82% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 17.06ms | 64.24ms | 3.766× | 5.54% | PASS |
| ac_writes | `oltp_insert_ac` | 18.94ms | 86.73ms | 4.580× | 5.48% | PASS |
| ac_writes | `oltp_update_index_ac` | 20.79ms | 99.71ms | 4.797× | 6.26% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 16.69ms | 76.65ms | 4.594× | 5.46% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 18.87ms | 90.75ms | 4.810× | 5.62% | PASS |
| ac_writes | `oltp_write_only_ac` | 19.42ms | 88.31ms | 4.547× | 4.47% | PASS |
| ac_writes | `types_delete_insert_ac` | 16.04ms | 78.63ms | 4.901× | 5.69% | PASS |
| ac_writes | `oltp_read_write_ac` | 25.90ms | 96.75ms | 3.735× | 4.93% | PASS |

</details>

## Version-control latency

Wall time: 2m 18s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 81.43ms | 200.00ms | 40.7% | 0.82% | PASS |
| `status_dirty_many_tables` | 85.30ms | 200.00ms | 42.6% | 0.56% | PASS |
| `diff_regular_working_one_table` | 78.22ms | 150.00ms | 52.1% | 0.49% | PASS |
| `diff_regular_working_many_tables` | 90.39ms | 200.00ms | 45.2% | 0.90% | PASS |
| `diff_stat_working_many_tables` | 89.91ms | 200.00ms | 45.0% | 0.93% | PASS |
| `diff_schema_working_many_tables` | 90.66ms | 200.00ms | 45.3% | 0.60% | PASS |
| `branch_list_many_branches` | 23.59ms | 100.00ms | 23.6% | 1.40% | PASS |
| `branch_create_delete` | 25.86ms | 100.00ms | 25.9% | 1.75% | PASS |
| `checkout_branch_clean` | 54.99ms | 200.00ms | 27.5% | 1.08% | PASS |
| `merge_data_no_conflicts` | 28.24ms | 150.00ms | 18.8% | 1.40% | PASS |
| `merge_schema_no_conflicts` | 21.64ms | 100.00ms | 21.6% | 1.59% | PASS |
| `merge_data_conflicts` | 126.25ms | 250.00ms | 50.5% | 0.48% | PASS |
| `merge_data_conflicts_with_resolve` | 126.31ms | 250.00ms | 50.5% | 0.45% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
