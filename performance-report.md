# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-08-11 11:17 UTC
>
> Commit: [`1ae7e33c58de5f0b349b34f733c32e63e485578a`](https://github.com/dolthub/doltlite/commit/1ae7e33c58de5f0b349b34f733c32e63e485578a)
>
> Runner: ubuntu24 20260720.247.2
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/31479299346)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians; MAD is the median absolute deviation and describes run-to-run noise.

## SQL workload summary

| Key shape | Workloads | Samples/workload | Wall time | SQLite median total | DoltLite median total | Ratio | Median paired-ratio MAD | Result |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| int | 69 | 55 | 1h 11m 8s | 9.10s | 10.35s | 1.138× | 1.63% | **PASS** |
| textpk | 69 | 55 | 1h 25m 59s | 9.09s | 11.28s | 1.241× | 2.46% | **PASS** |
| blobpk | 69 | 55 | 1h 19m 21s | 8.26s | 9.40s | 1.139× | 0.80% | **PASS** |
| compositepk | 69 | 55 | 1h 26m 2s | 10.47s | 11.77s | 1.124× | 1.32% | **PASS** |

The absolute ceiling is 2.4× per ordinary workload and 1.95× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 24.39ms | 28.53ms | 1.170× | 2.48% | PASS |
| mem_reads | `oltp_range_select` | 10.81ms | 11.05ms | 1.022× | 2.59% | PASS |
| mem_reads | `oltp_sum_range` | 9.87ms | 11.21ms | 1.136× | 1.73% | PASS |
| mem_reads | `oltp_order_range` | 2.61ms | 2.78ms | 1.066× | 1.88% | PASS |
| mem_reads | `oltp_distinct_range` | 3.66ms | 3.88ms | 1.060× | 2.14% | PASS |
| mem_reads | `oltp_index_scan` | 4.05ms | 5.16ms | 1.275× | 1.75% | PASS |
| mem_reads | `select_random_points` | 10.85ms | 11.19ms | 1.032× | 3.98% | PASS |
| mem_reads | `select_random_ranges` | 3.18ms | 3.93ms | 1.236× | 1.53% | PASS |
| mem_reads | `covering_index_scan` | 4.28ms | 4.36ms | 1.018× | 2.01% | PASS |
| mem_reads | `groupby_scan` | 29.85ms | 32.32ms | 1.083× | 0.62% | PASS |
| mem_reads | `index_join` | 6.14ms | 8.11ms | 1.321× | 3.37% | PASS |
| mem_reads | `index_join_scan` | 3.56ms | 4.62ms | 1.297× | 2.40% | PASS |
| mem_reads | `types_table_scan` | 1.06s | 1.16s | 1.093× | 1.47% | PASS |
| mem_reads | `table_scan` | 1.23s | 1.24s | 1.007× | 2.81% | PASS |
| mem_reads | `oltp_read_only` | 106.73ms | 115.86ms | 1.086× | 1.79% | PASS |
| mem_writes | `oltp_bulk_insert` | 181.45ms | 248.62ms | 1.370× | 1.80% | PASS |
| mem_writes | `oltp_insert` | 15.61ms | 27.95ms | 1.790× | 0.83% | PASS |
| mem_writes | `oltp_update_index` | 52.60ms | 104.52ms | 1.987× | 1.63% | PASS |
| mem_writes | `oltp_update_non_index` | 35.66ms | 59.83ms | 1.678× | 2.30% | PASS |
| mem_writes | `oltp_delete_insert` | 46.59ms | 77.94ms | 1.673× | 1.48% | PASS |
| mem_writes | `oltp_write_only` | 22.81ms | 45.48ms | 1.994× | 1.84% | PASS |
| mem_writes | `types_delete_insert` | 25.44ms | 40.59ms | 1.595× | 1.79% | PASS |
| mem_writes | `oltp_read_write` | 72.11ms | 113.69ms | 1.577× | 1.71% | PASS |
| file_reads | `oltp_point_select` | 93.68ms | 47.35ms | 0.505× | 0.94% | PASS |
| file_reads | `oltp_range_select` | 17.85ms | 12.96ms | 0.726× | 2.11% | PASS |
| file_reads | `oltp_sum_range` | 17.35ms | 13.41ms | 0.773× | 2.10% | PASS |
| file_reads | `oltp_order_range` | 3.38ms | 3.05ms | 0.901× | 2.41% | PASS |
| file_reads | `oltp_distinct_range` | 4.43ms | 4.16ms | 0.939× | 1.70% | PASS |
| file_reads | `oltp_index_scan` | 11.12ms | 7.29ms | 0.656× | 1.76% | PASS |
| file_reads | `select_random_points` | 17.83ms | 13.06ms | 0.732× | 3.24% | PASS |
| file_reads | `select_random_ranges` | 9.97ms | 5.76ms | 0.578× | 1.09% | PASS |
| file_reads | `covering_index_scan` | 11.21ms | 6.23ms | 0.556× | 1.56% | PASS |
| file_reads | `groupby_scan` | 30.85ms | 32.73ms | 1.061× | 1.01% | PASS |
| file_reads | `index_join` | 10.13ms | 9.47ms | 0.935× | 1.57% | PASS |
| file_reads | `index_join_scan` | 4.83ms | 5.02ms | 1.040× | 3.54% | PASS |
| file_reads | `types_table_scan` | 1.12s | 1.19s | 1.059× | 0.85% | PASS |
| file_reads | `table_scan` | 1.23s | 1.24s | 1.007× | 1.13% | PASS |
| file_reads | `oltp_read_only` | 204.10ms | 142.14ms | 0.696× | 0.87% | PASS |
| file_writes | `oltp_bulk_insert` | 194.74ms | 268.74ms | 1.380× | 1.99% | PASS |
| file_writes | `oltp_insert` | 22.03ms | 34.65ms | 1.573× | 1.36% | PASS |
| file_writes | `oltp_update_index` | 76.59ms | 122.92ms | 1.605× | 1.16% | PASS |
| file_writes | `oltp_update_non_index` | 57.08ms | 78.23ms | 1.371× | 1.40% | PASS |
| file_writes | `oltp_delete_insert` | 67.03ms | 94.83ms | 1.415× | 1.05% | PASS |
| file_writes | `oltp_write_only` | 43.02ms | 61.95ms | 1.440× | 1.54% | PASS |
| file_writes | `types_delete_insert` | 38.95ms | 51.75ms | 1.329× | 1.30% | PASS |
| file_writes | `oltp_read_write` | 90.61ms | 127.39ms | 1.406× | 0.88% | PASS |
| ac_reads | `oltp_point_select` | 46.78ms | 46.80ms | 1.000× | 0.91% | PASS |
| ac_reads | `oltp_range_select` | 12.92ms | 12.80ms | 0.991× | 1.15% | PASS |
| ac_reads | `oltp_sum_range` | 12.14ms | 13.05ms | 1.075× | 0.93% | PASS |
| ac_reads | `oltp_order_range` | 2.88ms | 2.98ms | 1.033× | 1.48% | PASS |
| ac_reads | `oltp_distinct_range` | 3.88ms | 4.07ms | 1.050× | 1.12% | PASS |
| ac_reads | `oltp_index_scan` | 6.35ms | 7.18ms | 1.131× | 1.04% | PASS |
| ac_reads | `select_random_points` | 12.62ms | 12.82ms | 1.016× | 1.21% | PASS |
| ac_reads | `select_random_ranges` | 5.26ms | 5.75ms | 1.093× | 0.88% | PASS |
| ac_reads | `covering_index_scan` | 6.61ms | 6.17ms | 0.933× | 1.26% | PASS |
| ac_reads | `groupby_scan` | 30.41ms | 32.51ms | 1.069× | 0.52% | PASS |
| ac_reads | `index_join` | 7.50ms | 9.28ms | 1.238× | 1.19% | PASS |
| ac_reads | `index_join_scan` | 3.94ms | 4.77ms | 1.211× | 1.62% | PASS |
| ac_reads | `types_table_scan` | 1.04s | 1.15s | 1.109× | 0.36% | PASS |
| ac_reads | `table_scan` | 1.22s | 1.24s | 1.016× | 2.76% | PASS |
| ac_reads | `oltp_read_only` | 140.48ms | 142.96ms | 1.018× | 1.00% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.27ms | 72.92ms | 3.429× | 5.92% | PASS |
| ac_writes | `oltp_insert_ac` | 23.88ms | 89.13ms | 3.732× | 4.01% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.15ms | 102.74ms | 3.929× | 5.84% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 21.47ms | 83.33ms | 3.881× | 4.58% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 23.31ms | 93.95ms | 4.030× | 4.51% | PASS |
| ac_writes | `oltp_write_only_ac` | 23.75ms | 92.15ms | 3.880× | 4.27% | PASS |
| ac_writes | `types_delete_insert_ac` | 21.11ms | 82.39ms | 3.902× | 6.67% | PASS |
| ac_writes | `oltp_read_write_ac` | 29.18ms | 97.13ms | 3.329× | 5.32% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 20.26ms | 23.80ms | 1.175× | 2.45% | PASS |
| mem_reads | `oltp_range_select` | 11.12ms | 9.66ms | 0.869× | 2.14% | PASS |
| mem_reads | `oltp_sum_range` | 10.68ms | 9.39ms | 0.879× | 2.05% | PASS |
| mem_reads | `oltp_order_range` | 2.29ms | 2.25ms | 0.980× | 2.11% | PASS |
| mem_reads | `oltp_distinct_range` | 2.89ms | 2.88ms | 0.998× | 2.12% | PASS |
| mem_reads | `oltp_index_scan` | 3.25ms | 4.01ms | 1.233× | 2.61% | PASS |
| mem_reads | `select_random_points` | 14.49ms | 16.89ms | 1.165× | 1.67% | PASS |
| mem_reads | `select_random_ranges` | 3.04ms | 3.60ms | 1.184× | 4.10% | PASS |
| mem_reads | `covering_index_scan` | 2.98ms | 2.87ms | 0.963× | 3.61% | PASS |
| mem_reads | `groupby_scan` | 21.75ms | 23.00ms | 1.057× | 1.41% | PASS |
| mem_reads | `index_join` | 5.49ms | 7.10ms | 1.294× | 2.73% | PASS |
| mem_reads | `index_join_scan` | 4.00ms | 4.92ms | 1.229× | 3.04% | PASS |
| mem_reads | `types_table_scan` | 766.62ms | 810.46ms | 1.057× | 2.46% | PASS |
| mem_reads | `table_scan` | 920.60ms | 892.24ms | 0.969× | 4.53% | PASS |
| mem_reads | `oltp_read_only` | 81.49ms | 87.55ms | 1.074× | 2.11% | PASS |
| mem_writes | `oltp_bulk_insert` | 132.16ms | 193.83ms | 1.467× | 0.73% | PASS |
| mem_writes | `oltp_insert` | 14.34ms | 23.54ms | 1.642× | 1.17% | PASS |
| mem_writes | `oltp_update_index` | 49.90ms | 102.20ms | 2.048× | 1.76% | PASS |
| mem_writes | `oltp_update_non_index` | 32.39ms | 68.03ms | 2.101× | 2.01% | PASS |
| mem_writes | `oltp_delete_insert` | 35.75ms | 70.95ms | 1.985× | 1.57% | PASS |
| mem_writes | `oltp_write_only` | 20.58ms | 41.80ms | 2.031× | 2.49% | PASS |
| mem_writes | `types_delete_insert` | 21.78ms | 38.62ms | 1.773× | 1.70% | PASS |
| mem_writes | `oltp_read_write` | 56.69ms | 92.52ms | 1.632× | 1.79% | PASS |
| file_reads | `oltp_point_select` | 41.11ms | 28.41ms | 0.691× | 1.13% | PASS |
| file_reads | `oltp_range_select` | 13.77ms | 10.40ms | 0.756× | 2.21% | PASS |
| file_reads | `oltp_sum_range` | 13.13ms | 10.12ms | 0.771× | 1.78% | PASS |
| file_reads | `oltp_order_range` | 2.63ms | 2.39ms | 0.910× | 2.66% | PASS |
| file_reads | `oltp_distinct_range` | 3.28ms | 3.04ms | 0.929× | 2.20% | PASS |
| file_reads | `oltp_index_scan` | 6.08ms | 5.24ms | 0.861× | 2.01% | PASS |
| file_reads | `select_random_points` | 18.04ms | 17.89ms | 0.992× | 2.59% | PASS |
| file_reads | `select_random_ranges` | 5.26ms | 4.13ms | 0.785× | 1.37% | PASS |
| file_reads | `covering_index_scan` | 6.14ms | 3.79ms | 0.617× | 3.57% | PASS |
| file_reads | `groupby_scan` | 21.53ms | 22.68ms | 1.053× | 0.70% | PASS |
| file_reads | `index_join` | 7.97ms | 8.19ms | 1.027× | 2.95% | PASS |
| file_reads | `index_join_scan` | 4.61ms | 5.26ms | 1.140× | 2.48% | PASS |
| file_reads | `types_table_scan` | 758.82ms | 812.08ms | 1.070× | 2.82% | PASS |
| file_reads | `table_scan` | 919.49ms | 890.38ms | 0.968× | 6.57% | PASS |
| file_reads | `oltp_read_only` | 112.86ms | 94.23ms | 0.835× | 1.66% | PASS |
| file_writes | `oltp_bulk_insert` | 213.28ms | 293.60ms | 1.377× | 10.96% | PASS |
| file_writes | `oltp_insert` | 55.69ms | 55.34ms | 0.994× | 32.03% | PASS |
| file_writes | `oltp_update_index` | 285.33ms | 203.90ms | 0.715× | 31.70% | PASS |
| file_writes | `oltp_update_non_index` | 235.97ms | 162.69ms | 0.689× | 33.75% | PASS |
| file_writes | `oltp_delete_insert` | 183.08ms | 160.35ms | 0.876× | 17.91% | PASS |
| file_writes | `oltp_write_only` | 215.62ms | 111.19ms | 0.516× | 33.48% | PASS |
| file_writes | `types_delete_insert` | 140.39ms | 94.02ms | 0.670× | 41.49% | PASS |
| file_writes | `oltp_read_write` | 219.61ms | 171.42ms | 0.781× | 17.00% | PASS |
| ac_reads | `oltp_point_select` | 26.94ms | 28.62ms | 1.062× | 1.08% | PASS |
| ac_reads | `oltp_range_select` | 12.45ms | 10.36ms | 0.832× | 1.35% | PASS |
| ac_reads | `oltp_sum_range` | 12.15ms | 10.24ms | 0.843× | 1.85% | PASS |
| ac_reads | `oltp_order_range` | 2.52ms | 2.37ms | 0.941× | 2.33% | PASS |
| ac_reads | `oltp_distinct_range` | 3.10ms | 2.98ms | 0.964× | 1.45% | PASS |
| ac_reads | `oltp_index_scan` | 4.68ms | 5.10ms | 1.088× | 1.63% | PASS |
| ac_reads | `select_random_points` | 16.44ms | 17.76ms | 1.080× | 1.41% | PASS |
| ac_reads | `select_random_ranges` | 4.09ms | 4.11ms | 1.006× | 2.33% | PASS |
| ac_reads | `covering_index_scan` | 5.07ms | 3.84ms | 0.757× | 2.12% | PASS |
| ac_reads | `groupby_scan` | 21.45ms | 22.61ms | 1.054× | 0.55% | PASS |
| ac_reads | `index_join` | 7.29ms | 8.15ms | 1.119× | 2.95% | PASS |
| ac_reads | `index_join_scan` | 4.51ms | 5.18ms | 1.149× | 2.91% | PASS |
| ac_reads | `types_table_scan` | 777.64ms | 814.67ms | 1.048× | 3.29% | PASS |
| ac_reads | `table_scan` | 920.15ms | 895.71ms | 0.973× | 5.41% | PASS |
| ac_reads | `oltp_read_only` | 97.58ms | 97.75ms | 1.002× | 1.86% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 188.27ms | 398.81ms | 2.118× | 59.81% | PASS |
| ac_writes | `oltp_insert_ac` | 135.91ms | 381.63ms | 2.808× | 68.17% | PASS |
| ac_writes | `oltp_update_index_ac` | 181.29ms | 350.30ms | 1.932× | 59.04% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 166.46ms | 448.11ms | 2.692× | 56.89% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 290.03ms | 638.55ms | 2.202× | 74.20% | PASS |
| ac_writes | `oltp_write_only_ac` | 233.85ms | 419.74ms | 1.795× | 60.28% | PASS |
| ac_writes | `types_delete_insert_ac` | 137.09ms | 343.09ms | 2.503× | 62.89% | PASS |
| ac_writes | `oltp_read_write_ac` | 126.41ms | 671.51ms | 5.312× | 74.08% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 23.43ms | 26.45ms | 1.129× | 0.86% | PASS |
| mem_reads | `oltp_range_select` | 10.41ms | 10.31ms | 0.990× | 0.60% | PASS |
| mem_reads | `oltp_sum_range` | 9.37ms | 10.12ms | 1.079× | 0.55% | PASS |
| mem_reads | `oltp_order_range` | 2.39ms | 2.44ms | 1.021× | 0.78% | PASS |
| mem_reads | `oltp_distinct_range` | 3.24ms | 3.28ms | 1.010× | 0.63% | PASS |
| mem_reads | `oltp_index_scan` | 3.62ms | 4.42ms | 1.220× | 1.10% | PASS |
| mem_reads | `select_random_points` | 13.58ms | 15.48ms | 1.141× | 0.71% | PASS |
| mem_reads | `select_random_ranges` | 3.28ms | 3.98ms | 1.214× | 0.98% | PASS |
| mem_reads | `covering_index_scan` | 3.52ms | 3.28ms | 0.933× | 1.11% | PASS |
| mem_reads | `groupby_scan` | 26.29ms | 27.79ms | 1.057× | 0.64% | PASS |
| mem_reads | `index_join` | 5.49ms | 6.80ms | 1.238× | 0.88% | PASS |
| mem_reads | `index_join_scan` | 3.56ms | 4.53ms | 1.270× | 0.93% | PASS |
| mem_reads | `types_table_scan` | 868.32ms | 951.30ms | 1.096× | 0.48% | PASS |
| mem_reads | `table_scan` | 1.00s | 1.04s | 1.038× | 0.37% | PASS |
| mem_reads | `oltp_read_only` | 91.27ms | 99.17ms | 1.087× | 0.69% | PASS |
| mem_writes | `oltp_bulk_insert` | 183.87ms | 259.38ms | 1.411× | 1.25% | PASS |
| mem_writes | `oltp_insert` | 15.83ms | 28.98ms | 1.831× | 0.48% | PASS |
| mem_writes | `oltp_update_index` | 53.72ms | 112.70ms | 2.098× | 0.64% | PASS |
| mem_writes | `oltp_update_non_index` | 38.40ms | 78.04ms | 2.032× | 0.55% | PASS |
| mem_writes | `oltp_delete_insert` | 38.84ms | 82.18ms | 2.116× | 0.65% | PASS |
| mem_writes | `oltp_write_only` | 22.16ms | 48.76ms | 2.201× | 0.66% | PASS |
| mem_writes | `types_delete_insert` | 25.03ms | 44.11ms | 1.762× | 0.80% | PASS |
| mem_writes | `oltp_read_write` | 62.51ms | 107.36ms | 1.718× | 0.62% | PASS |
| file_reads | `oltp_point_select` | 88.47ms | 43.51ms | 0.492× | 0.70% | PASS |
| file_reads | `oltp_range_select` | 17.19ms | 12.13ms | 0.706× | 1.22% | PASS |
| file_reads | `oltp_sum_range` | 16.41ms | 11.99ms | 0.731× | 1.56% | PASS |
| file_reads | `oltp_order_range` | 3.17ms | 2.74ms | 0.863× | 1.43% | PASS |
| file_reads | `oltp_distinct_range` | 3.98ms | 3.55ms | 0.893× | 1.52% | PASS |
| file_reads | `oltp_index_scan` | 10.57ms | 6.56ms | 0.621× | 1.21% | PASS |
| file_reads | `select_random_points` | 20.69ms | 17.43ms | 0.843× | 1.09% | PASS |
| file_reads | `select_random_ranges` | 9.95ms | 5.76ms | 0.579× | 0.80% | PASS |
| file_reads | `covering_index_scan` | 10.89ms | 5.41ms | 0.497× | 1.44% | PASS |
| file_reads | `groupby_scan` | 27.23ms | 28.15ms | 1.034× | 0.50% | PASS |
| file_reads | `index_join` | 9.48ms | 8.62ms | 0.910× | 1.37% | PASS |
| file_reads | `index_join_scan` | 4.36ms | 4.88ms | 1.121× | 1.34% | PASS |
| file_reads | `types_table_scan` | 871.24ms | 943.45ms | 1.083× | 0.45% | PASS |
| file_reads | `table_scan` | 1.00s | 1.04s | 1.034× | 0.37% | PASS |
| file_reads | `oltp_read_only` | 185.19ms | 123.28ms | 0.666× | 0.56% | PASS |
| file_writes | `oltp_bulk_insert` | 237.72ms | 328.08ms | 1.380× | 3.54% | PASS |
| file_writes | `oltp_insert` | 48.26ms | 58.52ms | 1.213× | 1.18% | PASS |
| file_writes | `oltp_update_index` | 170.41ms | 207.30ms | 1.216× | 4.26% | PASS |
| file_writes | `oltp_update_non_index` | 138.78ms | 138.54ms | 0.998× | 0.65% | PASS |
| file_writes | `oltp_delete_insert` | 140.32ms | 149.04ms | 1.062× | 0.30% | PASS |
| file_writes | `oltp_write_only` | 100.02ms | 98.83ms | 0.988× | 0.57% | PASS |
| file_writes | `types_delete_insert` | 89.53ms | 87.35ms | 0.976× | 6.29% | PASS |
| file_writes | `oltp_read_write` | 140.06ms | 160.49ms | 1.146× | 1.70% | PASS |
| ac_reads | `oltp_point_select` | 45.29ms | 43.53ms | 0.961× | 0.91% | PASS |
| ac_reads | `oltp_range_select` | 13.39ms | 12.11ms | 0.904× | 0.70% | PASS |
| ac_reads | `oltp_sum_range` | 12.31ms | 11.99ms | 0.974× | 0.57% | PASS |
| ac_reads | `oltp_order_range` | 2.83ms | 2.75ms | 0.971× | 1.00% | PASS |
| ac_reads | `oltp_distinct_range` | 3.62ms | 3.55ms | 0.981× | 0.53% | PASS |
| ac_reads | `oltp_index_scan` | 6.36ms | 6.59ms | 1.036× | 1.00% | PASS |
| ac_reads | `select_random_points` | 16.64ms | 17.48ms | 1.051× | 0.74% | PASS |
| ac_reads | `select_random_ranges` | 5.81ms | 5.76ms | 0.992× | 0.57% | PASS |
| ac_reads | `covering_index_scan` | 6.71ms | 5.41ms | 0.806× | 0.61% | PASS |
| ac_reads | `groupby_scan` | 26.87ms | 28.11ms | 1.046× | 0.48% | PASS |
| ac_reads | `index_join` | 7.43ms | 8.62ms | 1.161× | 0.84% | PASS |
| ac_reads | `index_join_scan` | 4.08ms | 4.90ms | 1.203× | 0.67% | PASS |
| ac_reads | `types_table_scan` | 870.87ms | 944.91ms | 1.085× | 0.48% | PASS |
| ac_reads | `table_scan` | 1.00s | 1.04s | 1.034× | 0.33% | PASS |
| ac_reads | `oltp_read_only` | 123.37ms | 123.23ms | 0.999× | 0.53% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 28.47ms | 70.38ms | 2.472× | 4.50% | PASS |
| ac_writes | `oltp_insert_ac` | 32.65ms | 88.18ms | 2.701× | 4.64% | PASS |
| ac_writes | `oltp_update_index_ac` | 34.30ms | 99.62ms | 2.904× | 5.21% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 28.57ms | 82.45ms | 2.886× | 4.89% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 30.94ms | 89.15ms | 2.882× | 6.30% | PASS |
| ac_writes | `oltp_write_only_ac` | 31.28ms | 88.53ms | 2.830× | 4.04% | PASS |
| ac_writes | `types_delete_insert_ac` | 27.76ms | 81.59ms | 2.939× | 4.55% | PASS |
| ac_writes | `oltp_read_write_ac` | 35.61ms | 92.56ms | 2.599× | 5.00% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 33.76ms | 36.67ms | 1.086× | 1.40% | PASS |
| mem_reads | `oltp_range_select` | 20.35ms | 21.24ms | 1.044× | 1.65% | PASS |
| mem_reads | `oltp_sum_range` | 18.38ms | 20.26ms | 1.102× | 1.43% | PASS |
| mem_reads | `oltp_order_range` | 3.74ms | 3.90ms | 1.044× | 1.02% | PASS |
| mem_reads | `oltp_distinct_range` | 4.86ms | 5.05ms | 1.039× | 0.95% | PASS |
| mem_reads | `oltp_index_scan` | 4.73ms | 5.83ms | 1.234× | 2.54% | PASS |
| mem_reads | `select_random_points` | 28.77ms | 31.27ms | 1.087× | 1.15% | PASS |
| mem_reads | `select_random_ranges` | 7.70ms | 8.21ms | 1.066× | 1.00% | PASS |
| mem_reads | `covering_index_scan` | 4.34ms | 4.16ms | 0.957× | 4.50% | PASS |
| mem_reads | `groupby_scan` | 38.63ms | 41.39ms | 1.071× | 0.76% | PASS |
| mem_reads | `index_join` | 8.19ms | 10.10ms | 1.232× | 2.73% | PASS |
| mem_reads | `index_join_scan` | 4.41ms | 5.75ms | 1.305× | 2.03% | PASS |
| mem_reads | `types_table_scan` | 1.27s | 1.27s | 0.996× | 1.66% | PASS |
| mem_reads | `table_scan` | 1.49s | 1.38s | 0.923× | 2.92% | PASS |
| mem_reads | `oltp_read_only` | 158.19ms | 168.31ms | 1.064× | 1.75% | PASS |
| mem_writes | `oltp_bulk_insert` | 247.47ms | 338.64ms | 1.368× | 1.25% | PASS |
| mem_writes | `oltp_insert` | 19.64ms | 36.26ms | 1.846× | 0.98% | PASS |
| mem_writes | `oltp_update_index` | 70.63ms | 142.79ms | 2.022× | 1.68% | PASS |
| mem_writes | `oltp_update_non_index` | 53.13ms | 103.97ms | 1.957× | 1.31% | PASS |
| mem_writes | `oltp_delete_insert` | 51.13ms | 103.64ms | 2.027× | 1.61% | PASS |
| mem_writes | `oltp_write_only` | 28.27ms | 62.82ms | 2.222× | 1.29% | PASS |
| mem_writes | `types_delete_insert` | 33.51ms | 61.24ms | 1.827× | 1.53% | PASS |
| mem_writes | `oltp_read_write` | 102.52ms | 160.00ms | 1.561× | 1.79% | PASS |
| file_reads | `oltp_point_select` | 117.05ms | 58.44ms | 0.499× | 1.19% | PASS |
| file_reads | `oltp_range_select` | 28.55ms | 23.21ms | 0.813× | 1.58% | PASS |
| file_reads | `oltp_sum_range` | 27.11ms | 22.46ms | 0.829× | 1.27% | PASS |
| file_reads | `oltp_order_range` | 4.67ms | 4.19ms | 0.897× | 1.32% | PASS |
| file_reads | `oltp_distinct_range` | 5.76ms | 5.33ms | 0.926× | 1.40% | PASS |
| file_reads | `oltp_index_scan` | 13.37ms | 8.26ms | 0.618× | 1.87% | PASS |
| file_reads | `select_random_points` | 36.70ms | 32.88ms | 0.896× | 1.16% | PASS |
| file_reads | `select_random_ranges` | 16.21ms | 10.49ms | 0.647× | 0.86% | PASS |
| file_reads | `covering_index_scan` | 13.09ms | 6.63ms | 0.506× | 1.31% | PASS |
| file_reads | `groupby_scan` | 39.35ms | 41.97ms | 1.067× | 0.93% | PASS |
| file_reads | `index_join` | 12.76ms | 11.68ms | 0.915× | 1.39% | PASS |
| file_reads | `index_join_scan` | 5.16ms | 5.89ms | 1.141× | 1.90% | PASS |
| file_reads | `types_table_scan` | 1.12s | 1.22s | 1.088× | 0.86% | PASS |
| file_reads | `table_scan` | 1.30s | 1.35s | 1.040× | 0.83% | PASS |
| file_reads | `oltp_read_only` | 278.68ms | 199.95ms | 0.718× | 1.02% | PASS |
| file_writes | `oltp_bulk_insert` | 264.10ms | 365.25ms | 1.383× | 1.25% | PASS |
| file_writes | `oltp_insert` | 26.70ms | 46.87ms | 1.755× | 1.51% | PASS |
| file_writes | `oltp_update_index` | 104.25ms | 173.70ms | 1.666× | 1.74% | PASS |
| file_writes | `oltp_update_non_index` | 78.50ms | 125.18ms | 1.595× | 1.98% | PASS |
| file_writes | `oltp_delete_insert` | 75.24ms | 124.33ms | 1.652× | 1.13% | PASS |
| file_writes | `oltp_write_only` | 49.71ms | 81.70ms | 1.643× | 2.48% | PASS |
| file_writes | `types_delete_insert` | 49.13ms | 73.08ms | 1.487× | 1.74% | PASS |
| file_writes | `oltp_read_write` | 122.39ms | 176.14ms | 1.439× | 1.85% | PASS |
| ac_reads | `oltp_point_select` | 60.95ms | 57.94ms | 0.951× | 1.28% | PASS |
| ac_reads | `oltp_range_select` | 23.69ms | 23.53ms | 0.993× | 1.16% | PASS |
| ac_reads | `oltp_sum_range` | 21.90ms | 22.80ms | 1.041× | 1.18% | PASS |
| ac_reads | `oltp_order_range` | 4.21ms | 4.20ms | 0.996× | 1.04% | PASS |
| ac_reads | `oltp_distinct_range` | 5.22ms | 5.32ms | 1.019× | 0.80% | PASS |
| ac_reads | `oltp_index_scan` | 7.96ms | 8.33ms | 1.047× | 1.54% | PASS |
| ac_reads | `select_random_points` | 31.17ms | 32.97ms | 1.058× | 1.24% | PASS |
| ac_reads | `select_random_ranges` | 10.52ms | 10.51ms | 0.999× | 1.07% | PASS |
| ac_reads | `covering_index_scan` | 7.31ms | 6.42ms | 0.878× | 1.18% | PASS |
| ac_reads | `groupby_scan` | 38.61ms | 41.96ms | 1.087× | 0.93% | PASS |
| ac_reads | `index_join` | 10.04ms | 11.87ms | 1.182× | 1.20% | PASS |
| ac_reads | `index_join_scan` | 4.67ms | 5.92ms | 1.266× | 1.20% | PASS |
| ac_reads | `types_table_scan` | 1.12s | 1.22s | 1.086× | 0.67% | PASS |
| ac_reads | `table_scan` | 1.29s | 1.35s | 1.041× | 0.64% | PASS |
| ac_reads | `oltp_read_only` | 189.80ms | 195.12ms | 1.028× | 0.82% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 15.66ms | 56.30ms | 3.596× | 5.72% | PASS |
| ac_writes | `oltp_insert_ac` | 17.89ms | 73.64ms | 4.115× | 6.50% | PASS |
| ac_writes | `oltp_update_index_ac` | 19.35ms | 88.42ms | 4.569× | 4.83% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 15.72ms | 67.31ms | 4.283× | 5.08% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 17.50ms | 77.90ms | 4.450× | 3.89% | PASS |
| ac_writes | `oltp_write_only_ac` | 18.02ms | 78.40ms | 4.350× | 4.14% | PASS |
| ac_writes | `types_delete_insert_ac` | 15.86ms | 66.37ms | 4.185× | 6.02% | PASS |
| ac_writes | `oltp_read_write_ac` | 24.59ms | 85.92ms | 3.494× | 4.06% | PASS |

</details>

## Version-control latency

Wall time: 2m 18s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 80.91ms | 130.00ms | 62.2% | 0.56% | PASS |
| `status_dirty_many_tables` | 84.19ms | 130.00ms | 64.8% | 0.68% | PASS |
| `diff_regular_working_one_table` | 78.14ms | 120.00ms | 65.1% | 0.53% | PASS |
| `diff_regular_working_many_tables` | 89.22ms | 140.00ms | 63.7% | 0.66% | PASS |
| `diff_stat_working_many_tables` | 89.08ms | 140.00ms | 63.6% | 0.71% | PASS |
| `diff_schema_working_many_tables` | 89.50ms | 140.00ms | 63.9% | 0.62% | PASS |
| `branch_list_many_branches` | 22.68ms | 35.00ms | 64.8% | 1.96% | PASS |
| `branch_create_delete` | 24.72ms | 40.00ms | 61.8% | 1.38% | PASS |
| `checkout_branch_clean` | 54.50ms | 150.00ms | 36.3% | 1.10% | PASS |
| `merge_data_no_conflicts` | 29.23ms | 50.00ms | 58.5% | 2.23% | PASS |
| `merge_schema_no_conflicts` | 21.91ms | 35.00ms | 62.6% | 2.16% | PASS |
| `merge_data_conflicts` | 126.91ms | 180.00ms | 70.5% | 0.21% | PASS |
| `merge_data_conflicts_with_resolve` | 127.39ms | 180.00ms | 70.8% | 0.33% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
