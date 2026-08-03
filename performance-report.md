# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-08-02 11:56 UTC
>
> Commit: [`553ecc7b52f31bb22a80b42ec4f197606bd5526f`](https://github.com/dolthub/doltlite/commit/553ecc7b52f31bb22a80b42ec4f197606bd5526f)
>
> Runner: ubuntu24 20260720.247.2
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/30743121963)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians; MAD is the median absolute deviation and describes run-to-run noise.

## SQL workload summary

| Key shape | Workloads | Samples/workload | Wall time | SQLite median total | DoltLite median total | Ratio | Median paired-ratio MAD | Result |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| int | 69 | 55 | 1h 12m 5s | 9.27s | 11.03s | 1.190× | 1.21% | **PASS** |
| textpk | 69 | 55 | 1h 36m 6s | 11.25s | 12.30s | 1.093× | 2.48% | **PASS** |
| blobpk | 69 | 55 | 1h 45m 54s | 12.08s | 15.22s | 1.260× | 1.50% | **PASS** |
| compositepk | 69 | 55 | 1h 20m 39s | 8.58s | 9.91s | 1.155× | 1.19% | **PASS** |

The absolute ceiling is 2.5× per ordinary workload and 2.0× for a section average. Durable autocommit writes use 10.0× and 5.0× ceilings respectively.

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.60ms | 27.36ms | 1.112× | 1.18% | PASS |
| mem_reads | `oltp_range_select` | 10.58ms | 11.92ms | 1.127× | 1.23% | PASS |
| mem_reads | `oltp_sum_range` | 9.70ms | 11.43ms | 1.179× | 1.13% | PASS |
| mem_reads | `oltp_order_range` | 2.67ms | 2.96ms | 1.107× | 1.00% | PASS |
| mem_reads | `oltp_distinct_range` | 3.83ms | 3.94ms | 1.027× | 1.08% | PASS |
| mem_reads | `oltp_index_scan` | 3.98ms | 5.01ms | 1.258× | 1.32% | PASS |
| mem_reads | `select_random_points` | 10.54ms | 11.18ms | 1.060× | 1.27% | PASS |
| mem_reads | `select_random_ranges` | 3.11ms | 4.02ms | 1.292× | 1.27% | PASS |
| mem_reads | `covering_index_scan` | 4.37ms | 4.13ms | 0.946× | 1.10% | PASS |
| mem_reads | `groupby_scan` | 31.92ms | 34.49ms | 1.080× | 0.60% | PASS |
| mem_reads | `index_join` | 5.97ms | 7.91ms | 1.325× | 1.09% | PASS |
| mem_reads | `index_join_scan` | 3.53ms | 4.70ms | 1.332× | 1.31% | PASS |
| mem_reads | `types_table_scan` | 1.11s | 1.28s | 1.152× | 0.44% | PASS |
| mem_reads | `table_scan` | 1.26s | 1.38s | 1.094× | 0.49% | PASS |
| mem_reads | `oltp_read_only` | 104.39ms | 115.94ms | 1.111× | 0.74% | PASS |
| mem_writes | `oltp_bulk_insert` | 181.34ms | 241.82ms | 1.334× | 0.66% | PASS |
| mem_writes | `oltp_insert` | 15.86ms | 28.43ms | 1.793× | 0.66% | PASS |
| mem_writes | `oltp_update_index` | 51.36ms | 88.63ms | 1.726× | 0.90% | PASS |
| mem_writes | `oltp_update_non_index` | 35.08ms | 58.85ms | 1.678× | 0.87% | PASS |
| mem_writes | `oltp_delete_insert` | 44.97ms | 70.03ms | 1.557× | 0.65% | PASS |
| mem_writes | `oltp_write_only` | 21.99ms | 44.83ms | 2.039× | 0.99% | PASS |
| mem_writes | `types_delete_insert` | 24.86ms | 39.67ms | 1.596× | 0.70% | PASS |
| mem_writes | `oltp_read_write` | 65.28ms | 103.19ms | 1.581× | 0.74% | PASS |
| file_reads | `oltp_point_select` | 107.25ms | 55.85ms | 0.521× | 0.86% | PASS |
| file_reads | `oltp_range_select` | 18.68ms | 14.78ms | 0.791× | 1.79% | PASS |
| file_reads | `oltp_sum_range` | 17.66ms | 14.32ms | 0.811× | 0.90% | PASS |
| file_reads | `oltp_order_range` | 3.45ms | 3.29ms | 0.956× | 1.90% | PASS |
| file_reads | `oltp_distinct_range` | 4.59ms | 4.30ms | 0.935× | 1.58% | PASS |
| file_reads | `oltp_index_scan` | 12.15ms | 8.14ms | 0.670× | 1.68% | PASS |
| file_reads | `select_random_points` | 18.40ms | 14.36ms | 0.780× | 2.25% | PASS |
| file_reads | `select_random_ranges` | 11.24ms | 6.93ms | 0.616× | 1.37% | PASS |
| file_reads | `covering_index_scan` | 12.74ms | 7.25ms | 0.569× | 1.40% | PASS |
| file_reads | `groupby_scan` | 32.70ms | 34.95ms | 1.069× | 0.52% | PASS |
| file_reads | `index_join` | 10.42ms | 9.97ms | 0.957× | 1.03% | PASS |
| file_reads | `index_join_scan` | 4.40ms | 5.09ms | 1.157× | 1.52% | PASS |
| file_reads | `types_table_scan` | 1.10s | 1.28s | 1.156× | 0.43% | PASS |
| file_reads | `table_scan` | 1.26s | 1.38s | 1.097× | 0.60% | PASS |
| file_reads | `oltp_read_only` | 223.08ms | 156.59ms | 0.702× | 0.62% | PASS |
| file_writes | `oltp_bulk_insert` | 195.10ms | 260.86ms | 1.337× | 0.65% | PASS |
| file_writes | `oltp_insert` | 21.89ms | 36.17ms | 1.652× | 1.53% | PASS |
| file_writes | `oltp_update_index` | 77.25ms | 116.02ms | 1.502× | 1.27% | PASS |
| file_writes | `oltp_update_non_index` | 57.73ms | 81.11ms | 1.405× | 1.54% | PASS |
| file_writes | `oltp_delete_insert` | 66.76ms | 93.75ms | 1.404× | 1.75% | PASS |
| file_writes | `oltp_write_only` | 42.65ms | 65.05ms | 1.525× | 1.55% | PASS |
| file_writes | `types_delete_insert` | 40.42ms | 52.96ms | 1.310× | 2.05% | PASS |
| file_writes | `oltp_read_write` | 86.94ms | 123.38ms | 1.419× | 1.24% | PASS |
| ac_reads | `oltp_point_select` | 51.75ms | 55.90ms | 1.080× | 1.02% | PASS |
| ac_reads | `oltp_range_select` | 13.75ms | 14.74ms | 1.072× | 1.11% | PASS |
| ac_reads | `oltp_sum_range` | 12.80ms | 14.36ms | 1.122× | 1.43% | PASS |
| ac_reads | `oltp_order_range` | 3.13ms | 3.31ms | 1.060× | 1.21% | PASS |
| ac_reads | `oltp_distinct_range` | 4.20ms | 4.30ms | 1.023× | 0.88% | PASS |
| ac_reads | `oltp_index_scan` | 7.08ms | 8.25ms | 1.166× | 1.26% | PASS |
| ac_reads | `select_random_points` | 14.04ms | 14.55ms | 1.036× | 1.24% | PASS |
| ac_reads | `select_random_ranges` | 6.05ms | 6.98ms | 1.154× | 1.26% | PASS |
| ac_reads | `covering_index_scan` | 7.37ms | 7.29ms | 0.990× | 1.07% | PASS |
| ac_reads | `groupby_scan` | 32.45ms | 34.95ms | 1.077× | 0.48% | PASS |
| ac_reads | `index_join` | 7.86ms | 10.14ms | 1.290× | 1.27% | PASS |
| ac_reads | `index_join_scan` | 4.06ms | 5.19ms | 1.280× | 1.69% | PASS |
| ac_reads | `types_table_scan` | 1.11s | 1.28s | 1.156× | 0.54% | PASS |
| ac_reads | `table_scan` | 1.26s | 1.38s | 1.099× | 0.59% | PASS |
| ac_reads | `oltp_read_only` | 144.11ms | 156.58ms | 1.087× | 0.73% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 14.95ms | 61.37ms | 4.105× | 4.06% | PASS |
| ac_writes | `oltp_insert_ac` | 17.03ms | 77.88ms | 4.573× | 3.22% | PASS |
| ac_writes | `oltp_update_index_ac` | 18.86ms | 94.55ms | 5.012× | 3.14% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 15.32ms | 70.56ms | 4.606× | 4.05% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 16.82ms | 84.63ms | 5.030× | 3.07% | PASS |
| ac_writes | `oltp_write_only_ac` | 17.12ms | 82.55ms | 4.822× | 2.43% | PASS |
| ac_writes | `types_delete_insert_ac` | 14.90ms | 72.07ms | 4.838× | 4.87% | PASS |
| ac_writes | `oltp_read_write_ac` | 22.22ms | 89.59ms | 4.032× | 2.14% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 32.28ms | 39.59ms | 1.227× | 1.39% | PASS |
| mem_reads | `oltp_range_select` | 15.24ms | 14.78ms | 0.969× | 4.49% | PASS |
| mem_reads | `oltp_sum_range` | 13.41ms | 15.03ms | 1.121× | 3.02% | PASS |
| mem_reads | `oltp_order_range` | 3.13ms | 3.28ms | 1.049× | 1.76% | PASS |
| mem_reads | `oltp_distinct_range` | 4.10ms | 4.40ms | 1.074× | 1.21% | PASS |
| mem_reads | `oltp_index_scan` | 4.84ms | 6.67ms | 1.378× | 1.92% | PASS |
| mem_reads | `select_random_points` | 19.70ms | 22.89ms | 1.162× | 3.24% | PASS |
| mem_reads | `select_random_ranges` | 4.35ms | 5.33ms | 1.224× | 2.54% | PASS |
| mem_reads | `covering_index_scan` | 5.42ms | 4.89ms | 0.901× | 5.95% | PASS |
| mem_reads | `groupby_scan` | 32.61ms | 34.81ms | 1.067× | 0.75% | PASS |
| mem_reads | `index_join` | 7.78ms | 10.25ms | 1.318× | 2.49% | PASS |
| mem_reads | `index_join_scan` | 5.31ms | 5.69ms | 1.073× | 6.59% | PASS |
| mem_reads | `types_table_scan` | 1.22s | 1.31s | 1.075× | 1.40% | PASS |
| mem_reads | `table_scan` | 1.49s | 1.40s | 0.936× | 0.83% | PASS |
| mem_reads | `oltp_read_only` | 128.57ms | 144.02ms | 1.120× | 1.56% | PASS |
| mem_writes | `oltp_bulk_insert` | 235.71ms | 361.90ms | 1.535× | 0.65% | PASS |
| mem_writes | `oltp_insert` | 23.57ms | 40.40ms | 1.714× | 2.44% | PASS |
| mem_writes | `oltp_update_index` | 80.27ms | 141.71ms | 1.766× | 2.46% | PASS |
| mem_writes | `oltp_update_non_index` | 52.93ms | 90.70ms | 1.714× | 1.52% | PASS |
| mem_writes | `oltp_delete_insert` | 56.22ms | 109.39ms | 1.946× | 1.71% | PASS |
| mem_writes | `oltp_write_only` | 32.23ms | 65.04ms | 2.018× | 1.93% | PASS |
| mem_writes | `types_delete_insert` | 35.49ms | 58.68ms | 1.653× | 1.11% | PASS |
| mem_writes | `oltp_read_write` | 97.66ms | 148.78ms | 1.523× | 1.69% | PASS |
| file_reads | `oltp_point_select` | 101.64ms | 63.78ms | 0.628× | 0.89% | PASS |
| file_reads | `oltp_range_select` | 22.88ms | 17.62ms | 0.770× | 4.03% | PASS |
| file_reads | `oltp_sum_range` | 21.20ms | 17.91ms | 0.844× | 3.21% | PASS |
| file_reads | `oltp_order_range` | 4.04ms | 3.66ms | 0.906× | 4.33% | PASS |
| file_reads | `oltp_distinct_range` | 5.11ms | 4.80ms | 0.940× | 4.96% | PASS |
| file_reads | `oltp_index_scan` | 12.37ms | 9.36ms | 0.757× | 1.26% | PASS |
| file_reads | `select_random_points` | 28.49ms | 26.74ms | 0.938× | 3.01% | PASS |
| file_reads | `select_random_ranges` | 11.51ms | 7.92ms | 0.688× | 1.30% | PASS |
| file_reads | `covering_index_scan` | 13.62ms | 7.45ms | 0.546× | 2.40% | PASS |
| file_reads | `groupby_scan` | 34.19ms | 35.71ms | 1.044× | 1.62% | PASS |
| file_reads | `index_join` | 12.66ms | 12.15ms | 0.959× | 2.89% | PASS |
| file_reads | `index_join_scan` | 6.11ms | 6.29ms | 1.030× | 5.63% | PASS |
| file_reads | `types_table_scan` | 1.27s | 1.32s | 1.038× | 0.81% | PASS |
| file_reads | `table_scan` | 1.56s | 1.42s | 0.910× | 0.79% | PASS |
| file_reads | `oltp_read_only` | 240.04ms | 184.40ms | 0.768× | 0.72% | PASS |
| file_writes | `oltp_bulk_insert` | 255.41ms | 390.46ms | 1.529× | 1.03% | PASS |
| file_writes | `oltp_insert` | 66.61ms | 53.56ms | 0.804× | 15.76% | PASS |
| file_writes | `oltp_update_index` | 122.34ms | 178.14ms | 1.456× | 1.53% | PASS |
| file_writes | `oltp_update_non_index` | 112.31ms | 117.52ms | 1.046× | 10.64% | PASS |
| file_writes | `oltp_delete_insert` | 96.28ms | 140.23ms | 1.456× | 1.81% | PASS |
| file_writes | `oltp_write_only` | 76.71ms | 87.78ms | 1.144× | 10.32% | PASS |
| file_writes | `types_delete_insert` | 57.81ms | 77.79ms | 1.346× | 1.47% | PASS |
| file_writes | `oltp_read_write` | 157.65ms | 175.52ms | 1.113× | 5.38% | PASS |
| ac_reads | `oltp_point_select` | 55.99ms | 64.82ms | 1.158× | 1.25% | PASS |
| ac_reads | `oltp_range_select` | 17.89ms | 17.72ms | 0.990× | 2.78% | PASS |
| ac_reads | `oltp_sum_range` | 16.13ms | 18.13ms | 1.124× | 2.76% | PASS |
| ac_reads | `oltp_order_range` | 3.54ms | 3.67ms | 1.037× | 5.78% | PASS |
| ac_reads | `oltp_distinct_range` | 4.60ms | 4.82ms | 1.048× | 4.59% | PASS |
| ac_reads | `oltp_index_scan` | 7.62ms | 9.47ms | 1.244× | 2.00% | PASS |
| ac_reads | `select_random_points` | 22.70ms | 26.56ms | 1.170× | 2.61% | PASS |
| ac_reads | `select_random_ranges` | 6.85ms | 7.92ms | 1.157× | 2.48% | PASS |
| ac_reads | `covering_index_scan` | 8.67ms | 7.41ms | 0.855× | 3.42% | PASS |
| ac_reads | `groupby_scan` | 33.44ms | 35.58ms | 1.064× | 0.95% | PASS |
| ac_reads | `index_join` | 10.08ms | 12.06ms | 1.197× | 4.00% | PASS |
| ac_reads | `index_join_scan` | 5.76ms | 6.30ms | 1.094× | 6.10% | PASS |
| ac_reads | `types_table_scan` | 1.26s | 1.32s | 1.046× | 1.05% | PASS |
| ac_reads | `table_scan` | 1.55s | 1.41s | 0.913× | 0.99% | PASS |
| ac_reads | `oltp_read_only` | 169.35ms | 184.57ms | 1.090× | 1.04% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.90ms | 80.41ms | 3.672× | 5.32% | PASS |
| ac_writes | `oltp_insert_ac` | 26.17ms | 94.40ms | 3.607× | 5.31% | PASS |
| ac_writes | `oltp_update_index_ac` | 27.33ms | 113.54ms | 4.155× | 5.03% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.16ms | 92.23ms | 4.161× | 5.68% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.79ms | 104.36ms | 4.210× | 3.50% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.50ms | 103.88ms | 4.074× | 7.65% | PASS |
| ac_writes | `types_delete_insert_ac` | 21.76ms | 93.13ms | 4.281× | 4.59% | PASS |
| ac_writes | `oltp_read_write_ac` | 30.66ms | 110.01ms | 3.589× | 5.40% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 28.12ms | 31.59ms | 1.123× | 1.67% | PASS |
| mem_reads | `oltp_range_select` | 12.84ms | 13.18ms | 1.027× | 1.63% | PASS |
| mem_reads | `oltp_sum_range` | 12.25ms | 12.25ms | 1.000× | 1.50% | PASS |
| mem_reads | `oltp_order_range` | 2.91ms | 3.09ms | 1.062× | 0.97% | PASS |
| mem_reads | `oltp_distinct_range` | 3.90ms | 4.04ms | 1.035× | 0.81% | PASS |
| mem_reads | `oltp_index_scan` | 4.21ms | 5.49ms | 1.303× | 1.07% | PASS |
| mem_reads | `select_random_points` | 17.38ms | 19.87ms | 1.143× | 1.50% | PASS |
| mem_reads | `select_random_ranges` | 3.55ms | 4.73ms | 1.331× | 1.94% | PASS |
| mem_reads | `covering_index_scan` | 3.72ms | 4.16ms | 1.117× | 1.59% | PASS |
| mem_reads | `groupby_scan` | 32.79ms | 32.40ms | 0.988× | 0.55% | PASS |
| mem_reads | `index_join` | 6.43ms | 9.11ms | 1.417× | 1.95% | PASS |
| mem_reads | `index_join_scan` | 3.83ms | 5.37ms | 1.403× | 1.39% | PASS |
| mem_reads | `types_table_scan` | 1.13s | 1.29s | 1.133× | 1.38% | PASS |
| mem_reads | `table_scan` | 1.46s | 1.41s | 0.968× | 0.78% | PASS |
| mem_reads | `oltp_read_only` | 117.31ms | 126.40ms | 1.078× | 1.68% | PASS |
| mem_writes | `oltp_bulk_insert` | 195.63ms | 281.45ms | 1.439× | 1.06% | PASS |
| mem_writes | `oltp_insert` | 17.46ms | 33.45ms | 1.916× | 0.85% | PASS |
| mem_writes | `oltp_update_index` | 62.30ms | 122.22ms | 1.962× | 1.61% | PASS |
| mem_writes | `oltp_update_non_index` | 43.27ms | 73.80ms | 1.705× | 1.49% | PASS |
| mem_writes | `oltp_delete_insert` | 44.02ms | 93.03ms | 2.113× | 1.38% | PASS |
| mem_writes | `oltp_write_only` | 24.27ms | 54.34ms | 2.239× | 1.35% | PASS |
| mem_writes | `types_delete_insert` | 27.83ms | 47.20ms | 1.696× | 1.33% | PASS |
| mem_writes | `oltp_read_write` | 76.27ms | 122.49ms | 1.606× | 2.50% | PASS |
| file_reads | `oltp_point_select` | 57.75ms | 41.41ms | 0.717× | 0.88% | PASS |
| file_reads | `oltp_range_select` | 16.89ms | 14.38ms | 0.851× | 3.68% | PASS |
| file_reads | `oltp_sum_range` | 16.16ms | 13.59ms | 0.841× | 2.92% | PASS |
| file_reads | `oltp_order_range` | 3.33ms | 3.23ms | 0.971× | 0.76% | PASS |
| file_reads | `oltp_distinct_range` | 4.32ms | 4.20ms | 0.973× | 0.73% | PASS |
| file_reads | `oltp_index_scan` | 7.69ms | 6.63ms | 0.863× | 1.05% | PASS |
| file_reads | `select_random_points` | 21.30ms | 20.89ms | 0.981× | 2.25% | PASS |
| file_reads | `select_random_ranges` | 6.82ms | 5.80ms | 0.850× | 0.99% | PASS |
| file_reads | `covering_index_scan` | 7.45ms | 5.35ms | 0.718× | 0.92% | PASS |
| file_reads | `groupby_scan` | 33.38ms | 32.47ms | 0.973× | 0.54% | PASS |
| file_reads | `index_join` | 8.83ms | 10.11ms | 1.145× | 1.76% | PASS |
| file_reads | `index_join_scan` | 4.34ms | 5.63ms | 1.298× | 2.06% | PASS |
| file_reads | `types_table_scan` | 1.16s | 1.30s | 1.119× | 1.26% | PASS |
| file_reads | `table_scan` | 1.43s | 1.41s | 0.983× | 1.10% | PASS |
| file_reads | `oltp_read_only` | 162.53ms | 141.58ms | 0.871× | 0.98% | PASS |
| file_writes | `oltp_bulk_insert` | 283.32ms | 365.38ms | 1.290× | 12.02% | PASS |
| file_writes | `oltp_insert` | 157.26ms | 75.55ms | 0.480× | 73.78% | PASS |
| file_writes | `oltp_update_index` | 189.79ms | 222.31ms | 1.171× | 10.48% | PASS |
| file_writes | `oltp_update_non_index` | 224.55ms | 148.28ms | 0.660× | 29.66% | PASS |
| file_writes | `oltp_delete_insert` | 198.90ms | 179.56ms | 0.903× | 26.42% | PASS |
| file_writes | `oltp_write_only` | 174.14ms | 128.08ms | 0.735× | 29.24% | PASS |
| file_writes | `types_delete_insert` | 104.09ms | 102.00ms | 0.980× | 27.27% | PASS |
| file_writes | `oltp_read_write` | 222.28ms | 194.87ms | 0.877× | 28.31% | PASS |
| ac_reads | `oltp_point_select` | 37.95ms | 41.74ms | 1.100× | 1.43% | PASS |
| ac_reads | `oltp_range_select` | 14.47ms | 14.56ms | 1.006× | 1.85% | PASS |
| ac_reads | `oltp_sum_range` | 13.84ms | 13.65ms | 0.987× | 1.50% | PASS |
| ac_reads | `oltp_order_range` | 3.14ms | 3.24ms | 1.032× | 1.12% | PASS |
| ac_reads | `oltp_distinct_range` | 4.11ms | 4.20ms | 1.022× | 0.93% | PASS |
| ac_reads | `oltp_index_scan` | 5.83ms | 6.70ms | 1.149× | 1.54% | PASS |
| ac_reads | `select_random_points` | 19.80ms | 21.47ms | 1.084× | 2.42% | PASS |
| ac_reads | `select_random_ranges` | 4.99ms | 5.87ms | 1.177× | 1.34% | PASS |
| ac_reads | `covering_index_scan` | 5.49ms | 5.37ms | 0.978× | 1.75% | PASS |
| ac_reads | `groupby_scan` | 33.15ms | 32.53ms | 0.981× | 0.67% | PASS |
| ac_reads | `index_join` | 7.81ms | 10.04ms | 1.287× | 1.42% | PASS |
| ac_reads | `index_join_scan` | 4.20ms | 5.64ms | 1.342× | 1.22% | PASS |
| ac_reads | `types_table_scan` | 1.17s | 1.30s | 1.110× | 0.69% | PASS |
| ac_reads | `table_scan` | 1.47s | 1.42s | 0.968× | 0.60% | PASS |
| ac_reads | `oltp_read_only` | 134.44ms | 142.13ms | 1.057× | 1.03% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 136.56ms | 451.97ms | 3.310× | 50.09% | PASS |
| ac_writes | `oltp_insert_ac` | 220.43ms | 544.03ms | 2.468× | 58.32% | PASS |
| ac_writes | `oltp_update_index_ac` | 199.32ms | 587.08ms | 2.945× | 72.33% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 50.92ms | 171.67ms | 3.372× | 25.55% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 180.13ms | 713.27ms | 3.960× | 51.27% | PASS |
| ac_writes | `oltp_write_only_ac` | 182.14ms | 556.21ms | 3.054× | 63.76% | PASS |
| ac_writes | `types_delete_insert_ac` | 135.97ms | 484.77ms | 3.565× | 59.59% | PASS |
| ac_writes | `oltp_read_write_ac` | 221.12ms | 469.19ms | 2.122× | 54.13% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 25.91ms | 29.18ms | 1.126× | 0.98% | PASS |
| mem_reads | `oltp_range_select` | 15.53ms | 15.62ms | 1.006× | 1.53% | PASS |
| mem_reads | `oltp_sum_range` | 14.54ms | 14.93ms | 1.027× | 1.68% | PASS |
| mem_reads | `oltp_order_range` | 2.98ms | 3.03ms | 1.019× | 0.92% | PASS |
| mem_reads | `oltp_distinct_range` | 3.82ms | 3.83ms | 1.004× | 0.73% | PASS |
| mem_reads | `oltp_index_scan` | 3.72ms | 4.70ms | 1.262× | 2.13% | PASS |
| mem_reads | `select_random_points` | 21.76ms | 24.07ms | 1.106× | 1.58% | PASS |
| mem_reads | `select_random_ranges` | 5.97ms | 6.44ms | 1.079× | 1.40% | PASS |
| mem_reads | `covering_index_scan` | 3.39ms | 3.34ms | 0.986× | 1.86% | PASS |
| mem_reads | `groupby_scan` | 30.25ms | 31.14ms | 1.030× | 0.71% | PASS |
| mem_reads | `index_join` | 6.43ms | 7.91ms | 1.231× | 1.43% | PASS |
| mem_reads | `index_join_scan` | 3.50ms | 4.60ms | 1.313× | 2.23% | PASS |
| mem_reads | `types_table_scan` | 876.81ms | 995.08ms | 1.135× | 0.63% | PASS |
| mem_reads | `table_scan` | 1.00s | 1.08s | 1.072× | 1.07% | PASS |
| mem_reads | `oltp_read_only` | 118.81ms | 124.36ms | 1.047× | 1.28% | PASS |
| mem_writes | `oltp_bulk_insert` | 190.97ms | 261.06ms | 1.367× | 0.76% | PASS |
| mem_writes | `oltp_insert` | 15.19ms | 27.37ms | 1.802× | 0.56% | PASS |
| mem_writes | `oltp_update_index` | 54.38ms | 90.64ms | 1.667× | 1.62% | PASS |
| mem_writes | `oltp_update_non_index` | 41.43ms | 63.73ms | 1.538× | 1.12% | PASS |
| mem_writes | `oltp_delete_insert` | 39.88ms | 73.45ms | 1.842× | 1.40% | PASS |
| mem_writes | `oltp_write_only` | 22.13ms | 44.97ms | 2.032× | 1.38% | PASS |
| mem_writes | `types_delete_insert` | 26.28ms | 40.67ms | 1.548× | 1.35% | PASS |
| mem_writes | `oltp_read_write` | 79.86ms | 113.45ms | 1.421× | 2.05% | PASS |
| file_reads | `oltp_point_select` | 92.81ms | 52.10ms | 0.561× | 0.93% | PASS |
| file_reads | `oltp_range_select` | 23.55ms | 18.08ms | 0.768× | 1.06% | PASS |
| file_reads | `oltp_sum_range` | 21.52ms | 17.34ms | 0.806× | 1.10% | PASS |
| file_reads | `oltp_order_range` | 3.77ms | 3.28ms | 0.870× | 1.10% | PASS |
| file_reads | `oltp_distinct_range` | 4.60ms | 4.13ms | 0.898× | 1.15% | PASS |
| file_reads | `oltp_index_scan` | 10.69ms | 7.35ms | 0.687× | 0.96% | PASS |
| file_reads | `select_random_points` | 30.27ms | 27.34ms | 0.903× | 1.23% | PASS |
| file_reads | `select_random_ranges` | 12.93ms | 8.94ms | 0.691× | 1.14% | PASS |
| file_reads | `covering_index_scan` | 10.36ms | 6.03ms | 0.582× | 0.94% | PASS |
| file_reads | `groupby_scan` | 31.39ms | 31.63ms | 1.008× | 0.88% | PASS |
| file_reads | `index_join` | 10.31ms | 10.04ms | 0.974× | 1.21% | PASS |
| file_reads | `index_join_scan` | 4.24ms | 4.93ms | 1.164× | 0.93% | PASS |
| file_reads | `types_table_scan` | 870.15ms | 992.72ms | 1.141× | 0.98% | PASS |
| file_reads | `table_scan` | 1.01s | 1.08s | 1.064× | 1.65% | PASS |
| file_reads | `oltp_read_only` | 213.03ms | 156.81ms | 0.736× | 0.81% | PASS |
| file_writes | `oltp_bulk_insert` | 260.42ms | 336.96ms | 1.294× | 6.14% | PASS |
| file_writes | `oltp_insert` | 33.70ms | 55.52ms | 1.648× | 35.11% | PASS |
| file_writes | `oltp_update_index` | 167.21ms | 169.68ms | 1.015× | 4.22% | PASS |
| file_writes | `oltp_update_non_index` | 139.66ms | 123.24ms | 0.882× | 1.62% | PASS |
| file_writes | `oltp_delete_insert` | 137.85ms | 141.07ms | 1.023× | 4.01% | PASS |
| file_writes | `oltp_write_only` | 100.66ms | 101.14ms | 1.005× | 16.21% | PASS |
| file_writes | `types_delete_insert` | 91.19ms | 93.07ms | 1.021× | 9.45% | PASS |
| file_writes | `oltp_read_write` | 155.50ms | 168.71ms | 1.085× | 3.14% | PASS |
| ac_reads | `oltp_point_select` | 48.25ms | 51.58ms | 1.069× | 0.90% | PASS |
| ac_reads | `oltp_range_select` | 18.70ms | 17.97ms | 0.961× | 1.19% | PASS |
| ac_reads | `oltp_sum_range` | 17.02ms | 17.34ms | 1.019× | 1.11% | PASS |
| ac_reads | `oltp_order_range` | 3.40ms | 3.29ms | 0.970× | 1.01% | PASS |
| ac_reads | `oltp_distinct_range` | 4.21ms | 4.12ms | 0.978× | 0.91% | PASS |
| ac_reads | `oltp_index_scan` | 6.40ms | 7.33ms | 1.144× | 1.11% | PASS |
| ac_reads | `select_random_points` | 24.51ms | 26.33ms | 1.074× | 1.02% | PASS |
| ac_reads | `select_random_ranges` | 8.53ms | 8.92ms | 1.046× | 1.00% | PASS |
| ac_reads | `covering_index_scan` | 6.04ms | 6.00ms | 0.992× | 0.78% | PASS |
| ac_reads | `groupby_scan` | 30.69ms | 31.52ms | 1.027× | 0.80% | PASS |
| ac_reads | `index_join` | 8.16ms | 10.02ms | 1.229× | 0.91% | PASS |
| ac_reads | `index_join_scan` | 3.86ms | 4.96ms | 1.285× | 1.00% | PASS |
| ac_reads | `types_table_scan` | 906.32ms | 1.00s | 1.104× | 2.01% | PASS |
| ac_reads | `table_scan` | 1.03s | 1.08s | 1.049× | 1.06% | PASS |
| ac_reads | `oltp_read_only` | 151.46ms | 157.47ms | 1.040× | 1.07% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 29.77ms | 80.46ms | 2.703× | 5.99% | PASS |
| ac_writes | `oltp_insert_ac` | 33.36ms | 124.39ms | 3.729× | 40.13% | PASS |
| ac_writes | `oltp_update_index_ac` | 33.70ms | 106.99ms | 3.174× | 5.01% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 30.80ms | 87.62ms | 2.845× | 6.61% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 32.75ms | 108.39ms | 3.310× | 26.02% | PASS |
| ac_writes | `oltp_write_only_ac` | 32.12ms | 99.03ms | 3.083× | 4.78% | PASS |
| ac_writes | `types_delete_insert_ac` | 30.15ms | 94.89ms | 3.147× | 7.11% | PASS |
| ac_writes | `oltp_read_write_ac` | 39.09ms | 110.87ms | 2.837× | 6.52% | PASS |

</details>

## Version-control latency

Wall time: 2m 18s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 81.79ms | 200.00ms | 40.9% | 0.55% | PASS |
| `status_dirty_many_tables` | 84.28ms | 200.00ms | 42.1% | 0.41% | PASS |
| `diff_regular_working_one_table` | 76.80ms | 150.00ms | 51.2% | 0.38% | PASS |
| `diff_regular_working_many_tables` | 89.67ms | 200.00ms | 44.8% | 0.43% | PASS |
| `diff_stat_working_many_tables` | 89.62ms | 200.00ms | 44.8% | 0.43% | PASS |
| `diff_schema_working_many_tables` | 90.10ms | 200.00ms | 45.1% | 0.42% | PASS |
| `branch_list_many_branches` | 21.80ms | 100.00ms | 21.8% | 0.72% | PASS |
| `branch_create_delete` | 24.09ms | 100.00ms | 24.1% | 0.85% | PASS |
| `checkout_branch_clean` | 53.84ms | 200.00ms | 26.9% | 0.71% | PASS |
| `merge_data_no_conflicts` | 28.12ms | 150.00ms | 18.7% | 0.64% | PASS |
| `merge_schema_no_conflicts` | 21.34ms | 100.00ms | 21.3% | 1.20% | PASS |
| `merge_data_conflicts` | 126.22ms | 250.00ms | 50.5% | 0.27% | PASS |
| `merge_data_conflicts_with_resolve` | 126.08ms | 250.00ms | 50.4% | 0.29% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
