# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-08-05 11:55 UTC
>
> Commit: [`8b4a146462117c9228952ae3beba96f93b675c87`](https://github.com/dolthub/doltlite/commit/8b4a146462117c9228952ae3beba96f93b675c87)
>
> Runner: ubuntu24 20260720.247.2
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/30996854836)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians; MAD is the median absolute deviation and describes run-to-run noise.

## SQL workload summary

| Key shape | Workloads | Samples/workload | Wall time | SQLite median total | DoltLite median total | Ratio | Median paired-ratio MAD | Result |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| int | 69 | 55 | 1h 12m 47s | 8.80s | 11.80s | 1.341× | 1.43% | **PASS** |
| textpk | 69 | 55 | 1h 33m 57s | 10.35s | 12.23s | 1.182× | 1.81% | **PASS** |
| blobpk | 69 | 55 | 1h 19m 30s | 8.26s | 10.12s | 1.225× | 2.46% | **PASS** |
| compositepk | 69 | 55 | 1h 27m 12s | 10.61s | 11.98s | 1.129× | 1.43% | **PASS** |

The absolute ceiling is 2.5× per ordinary workload and 2.0× for a section average. Durable autocommit writes use 10.0× and 5.0× ceilings respectively.

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.05ms | 29.59ms | 1.230× | 2.10% | PASS |
| mem_reads | `oltp_range_select` | 9.94ms | 12.95ms | 1.303× | 2.21% | PASS |
| mem_reads | `oltp_sum_range` | 9.44ms | 12.28ms | 1.301× | 1.63% | PASS |
| mem_reads | `oltp_order_range` | 2.50ms | 2.98ms | 1.191× | 1.13% | PASS |
| mem_reads | `oltp_distinct_range` | 3.54ms | 4.03ms | 1.141× | 1.14% | PASS |
| mem_reads | `oltp_index_scan` | 3.91ms | 5.25ms | 1.345× | 1.73% | PASS |
| mem_reads | `select_random_points` | 10.12ms | 11.17ms | 1.104× | 2.50% | PASS |
| mem_reads | `select_random_ranges` | 2.99ms | 4.04ms | 1.353× | 1.98% | PASS |
| mem_reads | `covering_index_scan` | 4.22ms | 4.21ms | 0.996× | 1.19% | PASS |
| mem_reads | `groupby_scan` | 29.68ms | 33.29ms | 1.122× | 0.64% | PASS |
| mem_reads | `index_join` | 5.99ms | 8.01ms | 1.337× | 1.19% | PASS |
| mem_reads | `index_join_scan` | 3.46ms | 4.49ms | 1.298× | 1.64% | PASS |
| mem_reads | `types_table_scan` | 1.04s | 1.39s | 1.333× | 0.67% | PASS |
| mem_reads | `table_scan` | 1.16s | 1.44s | 1.239× | 0.55% | PASS |
| mem_reads | `oltp_read_only` | 106.76ms | 125.78ms | 1.178× | 2.09% | PASS |
| mem_writes | `oltp_bulk_insert` | 181.82ms | 255.09ms | 1.403× | 1.16% | PASS |
| mem_writes | `oltp_insert` | 15.42ms | 28.23ms | 1.831× | 1.13% | PASS |
| mem_writes | `oltp_update_index` | 49.90ms | 104.04ms | 2.085× | 1.68% | PASS |
| mem_writes | `oltp_update_non_index` | 34.13ms | 59.40ms | 1.740× | 1.41% | PASS |
| mem_writes | `oltp_delete_insert` | 45.25ms | 79.71ms | 1.762× | 1.35% | PASS |
| mem_writes | `oltp_write_only` | 21.99ms | 45.38ms | 2.063× | 1.55% | PASS |
| mem_writes | `types_delete_insert` | 24.70ms | 40.37ms | 1.634× | 1.36% | PASS |
| mem_writes | `oltp_read_write` | 66.83ms | 108.75ms | 1.627× | 1.54% | PASS |
| file_reads | `oltp_point_select` | 93.10ms | 54.05ms | 0.581× | 0.95% | PASS |
| file_reads | `oltp_range_select` | 17.84ms | 15.69ms | 0.879× | 1.71% | PASS |
| file_reads | `oltp_sum_range` | 17.34ms | 15.30ms | 0.882× | 1.02% | PASS |
| file_reads | `oltp_order_range` | 3.41ms | 3.33ms | 0.975× | 1.84% | PASS |
| file_reads | `oltp_distinct_range` | 4.43ms | 4.39ms | 0.990× | 1.03% | PASS |
| file_reads | `oltp_index_scan` | 11.17ms | 8.06ms | 0.722× | 1.81% | PASS |
| file_reads | `select_random_points` | 17.27ms | 13.86ms | 0.803× | 1.88% | PASS |
| file_reads | `select_random_ranges` | 9.92ms | 6.45ms | 0.650× | 0.72% | PASS |
| file_reads | `covering_index_scan` | 11.62ms | 6.98ms | 0.601× | 1.77% | PASS |
| file_reads | `groupby_scan` | 30.81ms | 33.72ms | 1.095× | 0.96% | PASS |
| file_reads | `index_join` | 10.08ms | 10.04ms | 0.996× | 2.20% | PASS |
| file_reads | `index_join_scan` | 4.41ms | 4.87ms | 1.105× | 1.59% | PASS |
| file_reads | `types_table_scan` | 1.04s | 1.38s | 1.331× | 0.37% | PASS |
| file_reads | `table_scan` | 1.16s | 1.44s | 1.240× | 0.38% | PASS |
| file_reads | `oltp_read_only` | 204.86ms | 161.14ms | 0.787× | 1.11% | PASS |
| file_writes | `oltp_bulk_insert` | 195.72ms | 273.38ms | 1.397× | 1.65% | PASS |
| file_writes | `oltp_insert` | 22.50ms | 35.80ms | 1.591× | 1.55% | PASS |
| file_writes | `oltp_update_index` | 78.75ms | 130.49ms | 1.657× | 2.05% | PASS |
| file_writes | `oltp_update_non_index` | 58.72ms | 82.29ms | 1.401× | 1.41% | PASS |
| file_writes | `oltp_delete_insert` | 68.68ms | 99.94ms | 1.455× | 1.20% | PASS |
| file_writes | `oltp_write_only` | 43.40ms | 63.13ms | 1.455× | 1.41% | PASS |
| file_writes | `types_delete_insert` | 39.63ms | 53.22ms | 1.343× | 2.06% | PASS |
| file_writes | `oltp_read_write` | 90.21ms | 127.74ms | 1.416× | 1.17% | PASS |
| ac_reads | `oltp_point_select` | 46.29ms | 53.83ms | 1.163× | 0.72% | PASS |
| ac_reads | `oltp_range_select` | 12.59ms | 15.52ms | 1.232× | 1.64% | PASS |
| ac_reads | `oltp_sum_range` | 12.14ms | 15.04ms | 1.239× | 1.27% | PASS |
| ac_reads | `oltp_order_range` | 2.90ms | 3.31ms | 1.143× | 1.61% | PASS |
| ac_reads | `oltp_distinct_range` | 3.88ms | 4.37ms | 1.127× | 0.81% | PASS |
| ac_reads | `oltp_index_scan` | 6.42ms | 8.05ms | 1.254× | 1.88% | PASS |
| ac_reads | `select_random_points` | 12.99ms | 13.81ms | 1.063× | 2.33% | PASS |
| ac_reads | `select_random_ranges` | 5.42ms | 6.47ms | 1.194× | 0.96% | PASS |
| ac_reads | `covering_index_scan` | 7.00ms | 6.97ms | 0.996× | 1.43% | PASS |
| ac_reads | `groupby_scan` | 29.95ms | 33.69ms | 1.125× | 0.92% | PASS |
| ac_reads | `index_join` | 7.41ms | 9.97ms | 1.344× | 1.22% | PASS |
| ac_reads | `index_join_scan` | 3.83ms | 4.87ms | 1.273× | 1.39% | PASS |
| ac_reads | `types_table_scan` | 1.04s | 1.38s | 1.328× | 0.70% | PASS |
| ac_reads | `table_scan` | 1.17s | 1.44s | 1.230× | 0.67% | PASS |
| ac_reads | `oltp_read_only` | 136.65ms | 160.75ms | 1.176× | 1.03% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.91ms | 83.36ms | 3.639× | 7.27% | PASS |
| ac_writes | `oltp_insert_ac` | 25.20ms | 101.12ms | 4.012× | 5.31% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.40ms | 114.24ms | 4.170× | 2.97% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 24.39ms | 92.38ms | 3.787× | 4.10% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.77ms | 105.08ms | 4.243× | 4.99% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.43ms | 103.47ms | 4.069× | 4.95% | PASS |
| ac_writes | `types_delete_insert_ac` | 21.94ms | 93.20ms | 4.248× | 4.90% | PASS |
| ac_writes | `oltp_read_write_ac` | 30.25ms | 110.42ms | 3.650× | 4.21% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 31.54ms | 39.33ms | 1.247× | 1.40% | PASS |
| mem_reads | `oltp_range_select` | 13.83ms | 14.33ms | 1.036× | 1.88% | PASS |
| mem_reads | `oltp_sum_range` | 11.81ms | 14.17ms | 1.199× | 1.00% | PASS |
| mem_reads | `oltp_order_range` | 2.96ms | 3.16ms | 1.065× | 1.43% | PASS |
| mem_reads | `oltp_distinct_range` | 3.94ms | 4.21ms | 1.070× | 1.29% | PASS |
| mem_reads | `oltp_index_scan` | 4.46ms | 6.34ms | 1.420× | 1.71% | PASS |
| mem_reads | `select_random_points` | 17.81ms | 21.51ms | 1.208× | 1.33% | PASS |
| mem_reads | `select_random_ranges` | 3.99ms | 5.21ms | 1.306× | 1.27% | PASS |
| mem_reads | `covering_index_scan` | 4.54ms | 4.52ms | 0.995× | 1.69% | PASS |
| mem_reads | `groupby_scan` | 32.34ms | 34.52ms | 1.067× | 0.94% | PASS |
| mem_reads | `index_join` | 6.95ms | 9.40ms | 1.352× | 2.52% | PASS |
| mem_reads | `index_join_scan` | 4.56ms | 5.47ms | 1.200× | 1.65% | PASS |
| mem_reads | `types_table_scan` | 1.06s | 1.28s | 1.215× | 1.04% | PASS |
| mem_reads | `table_scan` | 1.26s | 1.42s | 1.125× | 3.05% | PASS |
| mem_reads | `oltp_read_only` | 117.40ms | 137.76ms | 1.173× | 1.81% | PASS |
| mem_writes | `oltp_bulk_insert` | 235.47ms | 362.92ms | 1.541× | 0.85% | PASS |
| mem_writes | `oltp_insert` | 21.86ms | 39.73ms | 1.818× | 1.33% | PASS |
| mem_writes | `oltp_update_index` | 75.53ms | 137.50ms | 1.820× | 1.89% | PASS |
| mem_writes | `oltp_update_non_index` | 49.83ms | 88.39ms | 1.774× | 0.96% | PASS |
| mem_writes | `oltp_delete_insert` | 53.72ms | 107.65ms | 2.004× | 2.10% | PASS |
| mem_writes | `oltp_write_only` | 30.02ms | 62.63ms | 2.086× | 1.65% | PASS |
| mem_writes | `types_delete_insert` | 32.82ms | 55.23ms | 1.683× | 1.72% | PASS |
| mem_writes | `oltp_read_write` | 91.83ms | 145.37ms | 1.583× | 1.73% | PASS |
| file_reads | `oltp_point_select` | 100.54ms | 63.83ms | 0.635× | 1.19% | PASS |
| file_reads | `oltp_range_select` | 21.39ms | 17.07ms | 0.798× | 3.02% | PASS |
| file_reads | `oltp_sum_range` | 20.22ms | 17.18ms | 0.850× | 1.34% | PASS |
| file_reads | `oltp_order_range` | 3.86ms | 3.51ms | 0.908× | 2.18% | PASS |
| file_reads | `oltp_distinct_range` | 4.95ms | 4.59ms | 0.929× | 1.32% | PASS |
| file_reads | `oltp_index_scan` | 11.78ms | 9.18ms | 0.779× | 1.94% | PASS |
| file_reads | `select_random_points` | 26.50ms | 25.40ms | 0.959× | 2.26% | PASS |
| file_reads | `select_random_ranges` | 11.27ms | 7.79ms | 0.692× | 1.04% | PASS |
| file_reads | `covering_index_scan` | 13.07ms | 7.33ms | 0.561× | 1.47% | PASS |
| file_reads | `groupby_scan` | 33.67ms | 35.20ms | 1.046× | 1.08% | PASS |
| file_reads | `index_join` | 11.05ms | 11.40ms | 1.031× | 3.03% | PASS |
| file_reads | `index_join_scan` | 5.68ms | 5.95ms | 1.047× | 3.48% | PASS |
| file_reads | `types_table_scan` | 1.11s | 1.29s | 1.171× | 3.14% | PASS |
| file_reads | `table_scan` | 1.50s | 1.45s | 0.964× | 2.80% | PASS |
| file_reads | `oltp_read_only` | 229.61ms | 178.93ms | 0.779× | 1.18% | PASS |
| file_writes | `oltp_bulk_insert` | 255.91ms | 393.74ms | 1.539× | 1.44% | PASS |
| file_writes | `oltp_insert` | 46.68ms | 52.36ms | 1.122× | 16.83% | PASS |
| file_writes | `oltp_update_index` | 111.53ms | 168.71ms | 1.513× | 1.56% | PASS |
| file_writes | `oltp_update_non_index` | 95.38ms | 113.02ms | 1.185× | 8.10% | PASS |
| file_writes | `oltp_delete_insert` | 92.49ms | 135.42ms | 1.464× | 1.85% | PASS |
| file_writes | `oltp_write_only` | 85.72ms | 84.61ms | 0.987× | 11.38% | PASS |
| file_writes | `types_delete_insert` | 55.27ms | 75.17ms | 1.360× | 1.35% | PASS |
| file_writes | `oltp_read_write` | 139.02ms | 166.86ms | 1.200× | 5.94% | PASS |
| ac_reads | `oltp_point_select` | 54.53ms | 63.89ms | 1.171× | 1.46% | PASS |
| ac_reads | `oltp_range_select` | 17.37ms | 17.18ms | 0.989× | 1.87% | PASS |
| ac_reads | `oltp_sum_range` | 15.88ms | 17.31ms | 1.090× | 2.36% | PASS |
| ac_reads | `oltp_order_range` | 3.43ms | 3.50ms | 1.021× | 1.07% | PASS |
| ac_reads | `oltp_distinct_range` | 4.42ms | 4.58ms | 1.036× | 1.39% | PASS |
| ac_reads | `oltp_index_scan` | 7.47ms | 9.27ms | 1.241× | 2.07% | PASS |
| ac_reads | `select_random_points` | 21.75ms | 25.48ms | 1.172× | 2.34% | PASS |
| ac_reads | `select_random_ranges` | 6.69ms | 7.79ms | 1.165× | 1.59% | PASS |
| ac_reads | `covering_index_scan` | 8.57ms | 7.34ms | 0.857× | 2.12% | PASS |
| ac_reads | `groupby_scan` | 32.95ms | 35.09ms | 1.065× | 1.11% | PASS |
| ac_reads | `index_join` | 9.78ms | 11.89ms | 1.216× | 3.69% | PASS |
| ac_reads | `index_join_scan` | 5.33ms | 6.02ms | 1.130× | 3.84% | PASS |
| ac_reads | `types_table_scan` | 1.22s | 1.32s | 1.077× | 1.51% | PASS |
| ac_reads | `table_scan` | 1.44s | 1.44s | 1.002× | 4.00% | PASS |
| ac_reads | `oltp_read_only` | 160.36ms | 178.27ms | 1.112× | 1.11% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.62ms | 77.37ms | 3.578× | 3.79% | PASS |
| ac_writes | `oltp_insert_ac` | 25.05ms | 94.32ms | 3.765× | 4.17% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.88ms | 109.78ms | 3.937× | 5.58% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.36ms | 89.14ms | 4.173× | 4.62% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 23.84ms | 100.60ms | 4.219× | 4.82% | PASS |
| ac_writes | `oltp_write_only_ac` | 24.59ms | 100.99ms | 4.107× | 5.20% | PASS |
| ac_writes | `types_delete_insert_ac` | 21.32ms | 91.27ms | 4.282× | 6.09% | PASS |
| ac_writes | `oltp_read_write_ac` | 31.13ms | 108.64ms | 3.489× | 3.66% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 19.09ms | 21.53ms | 1.128× | 1.71% | PASS |
| mem_reads | `oltp_range_select` | 10.09ms | 9.43ms | 0.935× | 2.60% | PASS |
| mem_reads | `oltp_sum_range` | 10.01ms | 8.97ms | 0.896× | 2.46% | PASS |
| mem_reads | `oltp_order_range` | 2.21ms | 2.17ms | 0.983× | 1.78% | PASS |
| mem_reads | `oltp_distinct_range` | 2.83ms | 2.77ms | 0.979× | 0.96% | PASS |
| mem_reads | `oltp_index_scan` | 3.21ms | 4.05ms | 1.264× | 2.25% | PASS |
| mem_reads | `select_random_points` | 13.75ms | 15.46ms | 1.124× | 1.38% | PASS |
| mem_reads | `select_random_ranges` | 2.87ms | 3.45ms | 1.201× | 1.68% | PASS |
| mem_reads | `covering_index_scan` | 2.69ms | 2.81ms | 1.048× | 2.27% | PASS |
| mem_reads | `groupby_scan` | 20.60ms | 22.05ms | 1.070× | 0.54% | PASS |
| mem_reads | `index_join` | 5.15ms | 6.69ms | 1.298× | 1.66% | PASS |
| mem_reads | `index_join_scan` | 3.02ms | 4.66ms | 1.544× | 6.12% | PASS |
| mem_reads | `types_table_scan` | 751.09ms | 829.29ms | 1.104× | 1.05% | PASS |
| mem_reads | `table_scan` | 883.95ms | 908.70ms | 1.028× | 3.52% | PASS |
| mem_reads | `oltp_read_only` | 75.55ms | 82.38ms | 1.090× | 1.72% | PASS |
| mem_writes | `oltp_bulk_insert` | 132.41ms | 191.13ms | 1.443× | 0.76% | PASS |
| mem_writes | `oltp_insert` | 12.39ms | 23.19ms | 1.871× | 0.86% | PASS |
| mem_writes | `oltp_update_index` | 45.96ms | 85.03ms | 1.850× | 1.79% | PASS |
| mem_writes | `oltp_update_non_index` | 29.21ms | 51.09ms | 1.749× | 2.57% | PASS |
| mem_writes | `oltp_delete_insert` | 30.86ms | 63.16ms | 2.047× | 2.00% | PASS |
| mem_writes | `oltp_write_only` | 17.93ms | 37.82ms | 2.109× | 1.90% | PASS |
| mem_writes | `types_delete_insert` | 20.34ms | 34.13ms | 1.678× | 1.90% | PASS |
| mem_writes | `oltp_read_write` | 53.16ms | 86.41ms | 1.625× | 1.64% | PASS |
| file_reads | `oltp_point_select` | 39.51ms | 29.03ms | 0.735× | 1.28% | PASS |
| file_reads | `oltp_range_select` | 11.10ms | 9.84ms | 0.886× | 2.36% | PASS |
| file_reads | `oltp_sum_range` | 10.88ms | 9.61ms | 0.884× | 5.72% | PASS |
| file_reads | `oltp_order_range` | 2.37ms | 2.25ms | 0.949× | 3.30% | PASS |
| file_reads | `oltp_distinct_range` | 3.04ms | 2.92ms | 0.960× | 2.04% | PASS |
| file_reads | `oltp_index_scan` | 5.65ms | 5.27ms | 0.934× | 2.21% | PASS |
| file_reads | `select_random_points` | 15.71ms | 16.13ms | 1.027× | 2.15% | PASS |
| file_reads | `select_random_ranges` | 4.77ms | 4.04ms | 0.848× | 2.30% | PASS |
| file_reads | `covering_index_scan` | 4.81ms | 3.58ms | 0.744× | 4.46% | PASS |
| file_reads | `groupby_scan` | 20.19ms | 21.88ms | 1.084× | 0.77% | PASS |
| file_reads | `index_join` | 6.44ms | 7.94ms | 1.233× | 3.95% | PASS |
| file_reads | `index_join_scan` | 3.67ms | 5.10ms | 1.392× | 2.96% | PASS |
| file_reads | `types_table_scan` | 755.11ms | 832.40ms | 1.102× | 1.55% | PASS |
| file_reads | `table_scan` | 886.62ms | 908.01ms | 1.024× | 4.53% | PASS |
| file_reads | `oltp_read_only` | 107.53ms | 95.37ms | 0.887× | 1.03% | PASS |
| file_writes | `oltp_bulk_insert` | 219.31ms | 298.55ms | 1.361× | 18.14% | PASS |
| file_writes | `oltp_insert` | 79.40ms | 61.45ms | 0.774× | 69.95% | PASS |
| file_writes | `oltp_update_index` | 194.54ms | 182.29ms | 0.937× | 22.10% | PASS |
| file_writes | `oltp_update_non_index` | 178.80ms | 128.86ms | 0.721× | 25.86% | PASS |
| file_writes | `oltp_delete_insert` | 218.91ms | 143.93ms | 0.658× | 35.04% | PASS |
| file_writes | `oltp_write_only` | 189.98ms | 109.12ms | 0.574× | 41.32% | PASS |
| file_writes | `types_delete_insert` | 89.84ms | 84.89ms | 0.945× | 32.07% | PASS |
| file_writes | `oltp_read_write` | 221.18ms | 159.64ms | 0.722× | 34.13% | PASS |
| ac_reads | `oltp_point_select` | 27.10ms | 30.45ms | 1.124× | 0.82% | PASS |
| ac_reads | `oltp_range_select` | 11.67ms | 10.55ms | 0.904× | 1.55% | PASS |
| ac_reads | `oltp_sum_range` | 11.38ms | 10.16ms | 0.893× | 1.29% | PASS |
| ac_reads | `oltp_order_range` | 2.38ms | 2.31ms | 0.972× | 2.67% | PASS |
| ac_reads | `oltp_distinct_range` | 3.04ms | 2.95ms | 0.967× | 1.84% | PASS |
| ac_reads | `oltp_index_scan` | 4.44ms | 5.26ms | 1.183× | 2.63% | PASS |
| ac_reads | `select_random_points` | 15.80ms | 17.23ms | 1.091× | 2.16% | PASS |
| ac_reads | `select_random_ranges` | 3.80ms | 4.28ms | 1.126× | 2.56% | PASS |
| ac_reads | `covering_index_scan` | 4.04ms | 3.93ms | 0.972× | 3.10% | PASS |
| ac_reads | `groupby_scan` | 21.30ms | 23.11ms | 1.085× | 2.02% | PASS |
| ac_reads | `index_join` | 6.29ms | 8.11ms | 1.289× | 3.02% | PASS |
| ac_reads | `index_join_scan` | 3.75ms | 5.25ms | 1.401× | 3.00% | PASS |
| ac_reads | `types_table_scan` | 766.18ms | 858.99ms | 1.121× | 1.92% | PASS |
| ac_reads | `table_scan` | 977.25ms | 978.88ms | 1.002× | 8.84% | PASS |
| ac_reads | `oltp_read_only` | 95.18ms | 100.94ms | 1.061× | 4.15% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 259.32ms | 595.40ms | 2.296× | 73.21% | PASS |
| ac_writes | `oltp_insert_ac` | 74.21ms | 280.38ms | 3.778× | 55.90% | PASS |
| ac_writes | `oltp_update_index_ac` | 62.90ms | 166.22ms | 2.643× | 41.15% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 62.50ms | 189.69ms | 3.035× | 48.83% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 91.84ms | 310.79ms | 3.384× | 67.29% | PASS |
| ac_writes | `oltp_write_only_ac` | 155.23ms | 269.44ms | 1.736× | 57.01% | PASS |
| ac_writes | `types_delete_insert_ac` | 127.98ms | 451.46ms | 3.528× | 62.87% | PASS |
| ac_writes | `oltp_read_write_ac` | 56.30ms | 180.16ms | 3.200× | 25.56% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 34.96ms | 38.96ms | 1.114× | 1.60% | PASS |
| mem_reads | `oltp_range_select` | 21.10ms | 20.35ms | 0.964× | 1.40% | PASS |
| mem_reads | `oltp_sum_range` | 19.23ms | 19.40ms | 1.009× | 1.12% | PASS |
| mem_reads | `oltp_order_range` | 3.83ms | 3.83ms | 0.999× | 1.21% | PASS |
| mem_reads | `oltp_distinct_range` | 4.93ms | 4.86ms | 0.985× | 0.81% | PASS |
| mem_reads | `oltp_index_scan` | 4.75ms | 5.94ms | 1.250× | 2.04% | PASS |
| mem_reads | `select_random_points` | 27.84ms | 30.48ms | 1.094× | 1.70% | PASS |
| mem_reads | `select_random_ranges` | 7.65ms | 8.26ms | 1.080× | 1.69% | PASS |
| mem_reads | `covering_index_scan` | 4.43ms | 4.21ms | 0.950× | 2.18% | PASS |
| mem_reads | `groupby_scan` | 38.80ms | 40.03ms | 1.032× | 0.76% | PASS |
| mem_reads | `index_join` | 8.24ms | 10.39ms | 1.261× | 1.67% | PASS |
| mem_reads | `index_join_scan` | 4.40ms | 5.75ms | 1.306× | 1.88% | PASS |
| mem_reads | `types_table_scan` | 1.21s | 1.30s | 1.068× | 2.52% | PASS |
| mem_reads | `table_scan` | 1.52s | 1.42s | 0.937× | 1.29% | PASS |
| mem_reads | `oltp_read_only` | 159.16ms | 161.94ms | 1.017× | 1.23% | PASS |
| mem_writes | `oltp_bulk_insert` | 246.52ms | 341.16ms | 1.384× | 0.70% | PASS |
| mem_writes | `oltp_insert` | 19.71ms | 36.55ms | 1.854× | 0.81% | PASS |
| mem_writes | `oltp_update_index` | 72.17ms | 125.89ms | 1.744× | 1.12% | PASS |
| mem_writes | `oltp_update_non_index` | 54.20ms | 86.96ms | 1.605× | 0.99% | PASS |
| mem_writes | `oltp_delete_insert` | 51.30ms | 99.25ms | 1.935× | 0.80% | PASS |
| mem_writes | `oltp_write_only` | 28.46ms | 61.56ms | 2.163× | 1.77% | PASS |
| mem_writes | `types_delete_insert` | 33.99ms | 55.68ms | 1.638× | 1.35% | PASS |
| mem_writes | `oltp_read_write` | 104.60ms | 152.50ms | 1.458× | 1.96% | PASS |
| file_reads | `oltp_point_select` | 117.78ms | 67.12ms | 0.570× | 1.64% | PASS |
| file_reads | `oltp_range_select` | 28.96ms | 23.09ms | 0.797× | 1.12% | PASS |
| file_reads | `oltp_sum_range` | 27.97ms | 22.65ms | 0.810× | 1.18% | PASS |
| file_reads | `oltp_order_range` | 4.73ms | 4.14ms | 0.875× | 1.49% | PASS |
| file_reads | `oltp_distinct_range` | 5.84ms | 5.21ms | 0.892× | 1.18% | PASS |
| file_reads | `oltp_index_scan` | 13.27ms | 9.21ms | 0.694× | 2.20% | PASS |
| file_reads | `select_random_points` | 37.85ms | 34.28ms | 0.906× | 1.37% | PASS |
| file_reads | `select_random_ranges` | 16.19ms | 11.33ms | 0.700× | 1.02% | PASS |
| file_reads | `covering_index_scan` | 13.14ms | 7.65ms | 0.582× | 1.76% | PASS |
| file_reads | `groupby_scan` | 40.20ms | 40.80ms | 1.015× | 0.66% | PASS |
| file_reads | `index_join` | 12.79ms | 12.55ms | 0.981× | 1.49% | PASS |
| file_reads | `index_join_scan` | 5.19ms | 6.09ms | 1.174× | 1.49% | PASS |
| file_reads | `types_table_scan` | 1.12s | 1.25s | 1.115× | 0.76% | PASS |
| file_reads | `table_scan` | 1.32s | 1.36s | 1.036× | 1.66% | PASS |
| file_reads | `oltp_read_only` | 277.88ms | 204.62ms | 0.736× | 1.62% | PASS |
| file_writes | `oltp_bulk_insert` | 262.58ms | 367.04ms | 1.398× | 0.80% | PASS |
| file_writes | `oltp_insert` | 26.49ms | 46.72ms | 1.764× | 1.22% | PASS |
| file_writes | `oltp_update_index` | 97.14ms | 144.69ms | 1.490× | 1.46% | PASS |
| file_writes | `oltp_update_non_index` | 78.47ms | 106.31ms | 1.355× | 1.80% | PASS |
| file_writes | `oltp_delete_insert` | 77.39ms | 122.14ms | 1.578× | 1.43% | PASS |
| file_writes | `oltp_write_only` | 49.98ms | 79.66ms | 1.594× | 2.83% | PASS |
| file_writes | `types_delete_insert` | 50.39ms | 68.29ms | 1.355× | 1.26% | PASS |
| file_writes | `oltp_read_write` | 122.80ms | 167.39ms | 1.363× | 1.08% | PASS |
| ac_reads | `oltp_point_select` | 61.61ms | 66.90ms | 1.086× | 1.33% | PASS |
| ac_reads | `oltp_range_select` | 23.16ms | 23.07ms | 0.996× | 1.46% | PASS |
| ac_reads | `oltp_sum_range` | 21.54ms | 22.22ms | 1.032× | 1.12% | PASS |
| ac_reads | `oltp_order_range` | 4.18ms | 4.12ms | 0.985× | 1.10% | PASS |
| ac_reads | `oltp_distinct_range` | 5.26ms | 5.19ms | 0.987× | 0.96% | PASS |
| ac_reads | `oltp_index_scan` | 7.79ms | 9.06ms | 1.164× | 1.23% | PASS |
| ac_reads | `select_random_points` | 31.39ms | 33.89ms | 1.080× | 1.57% | PASS |
| ac_reads | `select_random_ranges` | 10.63ms | 11.31ms | 1.065× | 0.99% | PASS |
| ac_reads | `covering_index_scan` | 7.64ms | 7.54ms | 0.987× | 1.45% | PASS |
| ac_reads | `groupby_scan` | 39.13ms | 40.73ms | 1.041× | 0.66% | PASS |
| ac_reads | `index_join` | 10.36ms | 12.85ms | 1.240× | 1.39% | PASS |
| ac_reads | `index_join_scan` | 4.90ms | 6.45ms | 1.315× | 1.75% | PASS |
| ac_reads | `types_table_scan` | 1.24s | 1.29s | 1.046× | 0.71% | PASS |
| ac_reads | `table_scan` | 1.32s | 1.37s | 1.044× | 1.88% | PASS |
| ac_reads | `oltp_read_only` | 192.66ms | 200.72ms | 1.042× | 0.75% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 16.03ms | 63.71ms | 3.973× | 5.38% | PASS |
| ac_writes | `oltp_insert_ac` | 18.89ms | 86.60ms | 4.585× | 3.60% | PASS |
| ac_writes | `oltp_update_index_ac` | 19.89ms | 97.75ms | 4.914× | 4.49% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 16.80ms | 74.48ms | 4.433× | 5.79% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 18.30ms | 88.87ms | 4.856× | 4.90% | PASS |
| ac_writes | `oltp_write_only_ac` | 19.02ms | 87.92ms | 4.623× | 5.10% | PASS |
| ac_writes | `types_delete_insert_ac` | 16.51ms | 76.76ms | 4.649× | 6.54% | PASS |
| ac_writes | `oltp_read_write_ac` | 25.37ms | 97.79ms | 3.855× | 5.73% | PASS |

</details>

## Version-control latency

Wall time: 2m 19s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 82.26ms | 200.00ms | 41.1% | 0.33% | PASS |
| `status_dirty_many_tables` | 85.19ms | 200.00ms | 42.6% | 0.28% | PASS |
| `diff_regular_working_one_table` | 77.91ms | 150.00ms | 51.9% | 0.37% | PASS |
| `diff_regular_working_many_tables` | 90.49ms | 200.00ms | 45.2% | 0.30% | PASS |
| `diff_stat_working_many_tables` | 90.58ms | 200.00ms | 45.3% | 0.41% | PASS |
| `diff_schema_working_many_tables` | 90.91ms | 200.00ms | 45.5% | 0.28% | PASS |
| `branch_list_many_branches` | 22.36ms | 100.00ms | 22.4% | 1.21% | PASS |
| `branch_create_delete` | 24.69ms | 100.00ms | 24.7% | 1.43% | PASS |
| `checkout_branch_clean` | 54.79ms | 200.00ms | 27.4% | 0.88% | PASS |
| `merge_data_no_conflicts` | 28.41ms | 150.00ms | 18.9% | 0.93% | PASS |
| `merge_schema_no_conflicts` | 21.83ms | 100.00ms | 21.8% | 1.03% | PASS |
| `merge_data_conflicts` | 126.92ms | 250.00ms | 50.8% | 0.29% | PASS |
| `merge_data_conflicts_with_resolve` | 126.87ms | 250.00ms | 50.7% | 0.23% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
