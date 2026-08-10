# DoltLite Performance Report

> Nightly result: **PASS**
>
> Generated: 2026-08-10 11:35 UTC
>
> Commit: [`ba57097c33f46cc0ccd6376e68999362871c2cd0`](https://github.com/dolthub/doltlite/commit/ba57097c33f46cc0ccd6376e68999362871c2cd0)
>
> Runner: ubuntu24 20260720.247.2
>
> [GitHub Actions run](https://github.com/dolthub/doltlite/actions/runs/31377096378)

This report compares optimized DoltLite against stock SQLite on the same GitHub-hosted runner. Baseline and candidate execution order alternates on each repetition. Reported timings are medians; MAD is the median absolute deviation and describes run-to-run noise.

## SQL workload summary

| Key shape | Workloads | Samples/workload | Wall time | SQLite median total | DoltLite median total | Ratio | Median paired-ratio MAD | Result |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| int | 69 | 55 | 1h 10m 51s | 8.72s | 10.70s | 1.227× | 1.28% | **PASS** |
| textpk | 69 | 55 | 1h 33m 29s | 10.69s | 11.88s | 1.112× | 2.00% | **PASS** |
| blobpk | 69 | 55 | 1h 30m 44s | 9.96s | 11.63s | 1.167× | 1.69% | **PASS** |
| compositepk | 69 | 55 | 1h 26m 22s | 9.66s | 11.84s | 1.226× | 1.35% | **PASS** |

The absolute ceiling is 2.4× per ordinary workload and 1.95× for a section average. Durable autocommit writes use 6.0× and 5.0× ceilings respectively.

<details>
<summary>int workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 23.17ms | 28.79ms | 1.242× | 1.37% | PASS |
| mem_reads | `oltp_range_select` | 9.35ms | 11.43ms | 1.222× | 1.35% | PASS |
| mem_reads | `oltp_sum_range` | 8.94ms | 11.48ms | 1.284× | 1.40% | PASS |
| mem_reads | `oltp_order_range` | 2.41ms | 2.84ms | 1.177× | 1.36% | PASS |
| mem_reads | `oltp_distinct_range` | 3.44ms | 3.95ms | 1.148× | 1.14% | PASS |
| mem_reads | `oltp_index_scan` | 3.77ms | 5.03ms | 1.336× | 1.49% | PASS |
| mem_reads | `select_random_points` | 8.94ms | 10.83ms | 1.212× | 1.87% | PASS |
| mem_reads | `select_random_ranges` | 2.73ms | 3.95ms | 1.447× | 1.47% | PASS |
| mem_reads | `covering_index_scan` | 4.25ms | 4.19ms | 0.987× | 1.11% | PASS |
| mem_reads | `groupby_scan` | 29.48ms | 32.80ms | 1.113× | 0.53% | PASS |
| mem_reads | `index_join` | 6.00ms | 7.80ms | 1.301× | 1.08% | PASS |
| mem_reads | `index_join_scan` | 3.17ms | 4.57ms | 1.443× | 2.03% | PASS |
| mem_reads | `types_table_scan` | 1.04s | 1.21s | 1.168× | 0.55% | PASS |
| mem_reads | `table_scan` | 1.16s | 1.30s | 1.118× | 0.40% | PASS |
| mem_reads | `oltp_read_only` | 99.62ms | 118.43ms | 1.189× | 0.76% | PASS |
| mem_writes | `oltp_bulk_insert` | 180.74ms | 250.07ms | 1.384× | 0.60% | PASS |
| mem_writes | `oltp_insert` | 15.52ms | 27.85ms | 1.795× | 0.61% | PASS |
| mem_writes | `oltp_update_index` | 48.92ms | 99.59ms | 2.036× | 1.04% | PASS |
| mem_writes | `oltp_update_non_index` | 32.61ms | 57.43ms | 1.761× | 0.91% | PASS |
| mem_writes | `oltp_delete_insert` | 44.05ms | 76.11ms | 1.728× | 1.11% | PASS |
| mem_writes | `oltp_write_only` | 21.14ms | 43.46ms | 2.056× | 0.90% | PASS |
| mem_writes | `types_delete_insert` | 24.54ms | 39.58ms | 1.613× | 0.91% | PASS |
| mem_writes | `oltp_read_write` | 64.82ms | 106.99ms | 1.650× | 0.93% | PASS |
| file_reads | `oltp_point_select` | 90.98ms | 53.04ms | 0.583× | 0.99% | PASS |
| file_reads | `oltp_range_select` | 16.50ms | 14.04ms | 0.850× | 1.41% | PASS |
| file_reads | `oltp_sum_range` | 16.28ms | 14.18ms | 0.871× | 1.42% | PASS |
| file_reads | `oltp_order_range` | 3.23ms | 3.15ms | 0.973× | 1.61% | PASS |
| file_reads | `oltp_distinct_range` | 4.25ms | 4.29ms | 1.008× | 1.53% | PASS |
| file_reads | `oltp_index_scan` | 10.74ms | 7.63ms | 0.710× | 1.42% | PASS |
| file_reads | `select_random_points` | 16.38ms | 13.78ms | 0.841× | 1.83% | PASS |
| file_reads | `select_random_ranges` | 9.66ms | 6.42ms | 0.665× | 0.82% | PASS |
| file_reads | `covering_index_scan` | 11.13ms | 6.72ms | 0.604× | 1.49% | PASS |
| file_reads | `groupby_scan` | 30.37ms | 33.29ms | 1.096× | 0.83% | PASS |
| file_reads | `index_join` | 9.80ms | 9.62ms | 0.982× | 1.59% | PASS |
| file_reads | `index_join_scan` | 4.25ms | 4.99ms | 1.175× | 1.36% | PASS |
| file_reads | `types_table_scan` | 1.04s | 1.21s | 1.168× | 0.45% | PASS |
| file_reads | `table_scan` | 1.16s | 1.30s | 1.115× | 0.53% | PASS |
| file_reads | `oltp_read_only` | 199.37ms | 154.92ms | 0.777× | 0.74% | PASS |
| file_writes | `oltp_bulk_insert` | 193.87ms | 269.09ms | 1.388× | 0.98% | PASS |
| file_writes | `oltp_insert` | 21.96ms | 34.92ms | 1.590× | 1.20% | PASS |
| file_writes | `oltp_update_index` | 74.81ms | 122.61ms | 1.639× | 1.08% | PASS |
| file_writes | `oltp_update_non_index` | 55.70ms | 79.36ms | 1.425× | 1.48% | PASS |
| file_writes | `oltp_delete_insert` | 66.11ms | 95.71ms | 1.448× | 1.07% | PASS |
| file_writes | `oltp_write_only` | 43.06ms | 62.36ms | 1.448× | 0.93% | PASS |
| file_writes | `types_delete_insert` | 39.09ms | 52.11ms | 1.333× | 1.37% | PASS |
| file_writes | `oltp_read_write` | 88.77ms | 126.50ms | 1.425× | 1.23% | PASS |
| ac_reads | `oltp_point_select` | 45.85ms | 53.26ms | 1.162× | 0.84% | PASS |
| ac_reads | `oltp_range_select` | 11.96ms | 14.08ms | 1.177× | 1.70% | PASS |
| ac_reads | `oltp_sum_range` | 11.57ms | 14.28ms | 1.235× | 1.18% | PASS |
| ac_reads | `oltp_order_range` | 2.75ms | 3.16ms | 1.150× | 1.51% | PASS |
| ac_reads | `oltp_distinct_range` | 3.75ms | 4.29ms | 1.145× | 1.29% | PASS |
| ac_reads | `oltp_index_scan` | 6.14ms | 7.65ms | 1.246× | 1.42% | PASS |
| ac_reads | `select_random_points` | 11.89ms | 13.91ms | 1.169× | 2.43% | PASS |
| ac_reads | `select_random_ranges` | 5.09ms | 6.44ms | 1.265× | 1.28% | PASS |
| ac_reads | `covering_index_scan` | 6.56ms | 6.73ms | 1.025× | 1.17% | PASS |
| ac_reads | `groupby_scan` | 29.70ms | 33.38ms | 1.124× | 0.64% | PASS |
| ac_reads | `index_join` | 7.46ms | 9.71ms | 1.302× | 2.06% | PASS |
| ac_reads | `index_join_scan` | 3.76ms | 4.98ms | 1.325× | 1.41% | PASS |
| ac_reads | `types_table_scan` | 1.04s | 1.21s | 1.170× | 0.34% | PASS |
| ac_reads | `table_scan` | 1.16s | 1.30s | 1.115× | 0.47% | PASS |
| ac_reads | `oltp_read_only` | 133.04ms | 155.02ms | 1.165× | 0.62% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 21.45ms | 74.31ms | 3.465× | 3.55% | PASS |
| ac_writes | `oltp_insert_ac` | 23.91ms | 88.11ms | 3.685× | 4.05% | PASS |
| ac_writes | `oltp_update_index_ac` | 25.36ms | 102.44ms | 4.039× | 3.61% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 22.13ms | 84.30ms | 3.809× | 2.82% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 23.52ms | 93.12ms | 3.959× | 2.97% | PASS |
| ac_writes | `oltp_write_only_ac` | 23.66ms | 92.52ms | 3.910× | 4.01% | PASS |
| ac_writes | `types_delete_insert_ac` | 21.24ms | 84.84ms | 3.995× | 5.80% | PASS |
| ac_writes | `oltp_read_write_ac` | 28.17ms | 98.57ms | 3.499× | 2.54% | PASS |

</details>

<details>
<summary>textpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 30.14ms | 37.77ms | 1.253× | 1.40% | PASS |
| mem_reads | `oltp_range_select` | 13.16ms | 14.31ms | 1.087× | 2.00% | PASS |
| mem_reads | `oltp_sum_range` | 11.87ms | 14.06ms | 1.185× | 1.24% | PASS |
| mem_reads | `oltp_order_range` | 3.00ms | 3.26ms | 1.086× | 1.90% | PASS |
| mem_reads | `oltp_distinct_range` | 4.04ms | 4.33ms | 1.072× | 1.74% | PASS |
| mem_reads | `oltp_index_scan` | 4.53ms | 6.20ms | 1.367× | 1.79% | PASS |
| mem_reads | `select_random_points` | 17.85ms | 21.10ms | 1.183× | 1.80% | PASS |
| mem_reads | `select_random_ranges` | 4.10ms | 5.36ms | 1.308× | 1.64% | PASS |
| mem_reads | `covering_index_scan` | 4.69ms | 4.72ms | 1.006× | 2.71% | PASS |
| mem_reads | `groupby_scan` | 32.12ms | 35.16ms | 1.095× | 1.19% | PASS |
| mem_reads | `index_join` | 7.18ms | 9.76ms | 1.360× | 2.81% | PASS |
| mem_reads | `index_join_scan` | 4.66ms | 5.65ms | 1.213× | 1.92% | PASS |
| mem_reads | `types_table_scan` | 1.09s | 1.24s | 1.138× | 1.24% | PASS |
| mem_reads | `table_scan` | 1.34s | 1.33s | 0.992× | 4.68% | PASS |
| mem_reads | `oltp_read_only` | 125.63ms | 141.62ms | 1.127× | 1.69% | PASS |
| mem_writes | `oltp_bulk_insert` | 235.39ms | 362.00ms | 1.538× | 0.91% | PASS |
| mem_writes | `oltp_insert` | 21.91ms | 39.78ms | 1.816× | 1.40% | PASS |
| mem_writes | `oltp_update_index` | 75.80ms | 136.92ms | 1.806× | 2.56% | PASS |
| mem_writes | `oltp_update_non_index` | 50.53ms | 88.88ms | 1.759× | 1.75% | PASS |
| mem_writes | `oltp_delete_insert` | 54.88ms | 107.64ms | 1.961× | 2.21% | PASS |
| mem_writes | `oltp_write_only` | 31.04ms | 63.25ms | 2.038× | 2.66% | PASS |
| mem_writes | `types_delete_insert` | 33.53ms | 55.80ms | 1.664× | 1.27% | PASS |
| mem_writes | `oltp_read_write` | 88.60ms | 140.66ms | 1.588× | 2.09% | PASS |
| file_reads | `oltp_point_select` | 100.21ms | 62.70ms | 0.626× | 1.37% | PASS |
| file_reads | `oltp_range_select` | 22.01ms | 17.19ms | 0.781× | 2.40% | PASS |
| file_reads | `oltp_sum_range` | 20.50ms | 17.31ms | 0.844× | 1.84% | PASS |
| file_reads | `oltp_order_range` | 4.08ms | 3.67ms | 0.900× | 2.90% | PASS |
| file_reads | `oltp_distinct_range` | 5.08ms | 4.77ms | 0.938× | 2.10% | PASS |
| file_reads | `oltp_index_scan` | 12.15ms | 8.98ms | 0.739× | 1.21% | PASS |
| file_reads | `select_random_points` | 26.80ms | 24.57ms | 0.917× | 1.64% | PASS |
| file_reads | `select_random_ranges` | 11.23ms | 7.90ms | 0.703× | 0.84% | PASS |
| file_reads | `covering_index_scan` | 12.81ms | 7.47ms | 0.583× | 2.32% | PASS |
| file_reads | `groupby_scan` | 33.30ms | 35.69ms | 1.072× | 1.05% | PASS |
| file_reads | `index_join` | 11.81ms | 11.38ms | 0.963× | 1.81% | PASS |
| file_reads | `index_join_scan` | 5.79ms | 6.10ms | 1.053× | 2.71% | PASS |
| file_reads | `types_table_scan` | 1.26s | 1.28s | 1.018× | 4.04% | PASS |
| file_reads | `table_scan` | 1.59s | 1.36s | 0.856× | 2.69% | PASS |
| file_reads | `oltp_read_only` | 224.35ms | 176.71ms | 0.788× | 1.40% | PASS |
| file_writes | `oltp_bulk_insert` | 254.04ms | 389.35ms | 1.533× | 0.74% | PASS |
| file_writes | `oltp_insert` | 44.06ms | 52.43ms | 1.190× | 11.42% | PASS |
| file_writes | `oltp_update_index` | 117.86ms | 172.44ms | 1.463× | 1.29% | PASS |
| file_writes | `oltp_update_non_index` | 99.74ms | 113.95ms | 1.142× | 15.34% | PASS |
| file_writes | `oltp_delete_insert` | 91.71ms | 134.16ms | 1.463× | 1.77% | PASS |
| file_writes | `oltp_write_only` | 87.58ms | 85.36ms | 0.975× | 12.24% | PASS |
| file_writes | `types_delete_insert` | 57.67ms | 75.97ms | 1.317× | 2.40% | PASS |
| file_writes | `oltp_read_write` | 141.42ms | 166.34ms | 1.176× | 4.88% | PASS |
| ac_reads | `oltp_point_select` | 54.17ms | 63.24ms | 1.167× | 1.00% | PASS |
| ac_reads | `oltp_range_select` | 17.02ms | 17.19ms | 1.010× | 1.94% | PASS |
| ac_reads | `oltp_sum_range` | 15.19ms | 17.05ms | 1.122× | 1.87% | PASS |
| ac_reads | `oltp_order_range` | 3.53ms | 3.68ms | 1.040× | 2.65% | PASS |
| ac_reads | `oltp_distinct_range` | 4.55ms | 4.75ms | 1.045× | 2.45% | PASS |
| ac_reads | `oltp_index_scan` | 7.49ms | 8.97ms | 1.198× | 1.22% | PASS |
| ac_reads | `select_random_points` | 21.36ms | 24.37ms | 1.141× | 1.48% | PASS |
| ac_reads | `select_random_ranges` | 6.65ms | 7.89ms | 1.186× | 1.22% | PASS |
| ac_reads | `covering_index_scan` | 8.42ms | 7.43ms | 0.882× | 2.39% | PASS |
| ac_reads | `groupby_scan` | 32.84ms | 35.85ms | 1.092× | 1.05% | PASS |
| ac_reads | `index_join` | 9.42ms | 11.44ms | 1.215× | 2.87% | PASS |
| ac_reads | `index_join_scan` | 5.19ms | 6.10ms | 1.175× | 2.17% | PASS |
| ac_reads | `types_table_scan` | 1.13s | 1.25s | 1.106× | 2.97% | PASS |
| ac_reads | `table_scan` | 1.47s | 1.34s | 0.912× | 5.05% | PASS |
| ac_reads | `oltp_read_only` | 153.89ms | 176.49ms | 1.147× | 1.49% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 24.95ms | 82.30ms | 3.299× | 7.26% | PASS |
| ac_writes | `oltp_insert_ac` | 28.68ms | 99.74ms | 3.477× | 7.95% | PASS |
| ac_writes | `oltp_update_index_ac` | 30.13ms | 119.68ms | 3.972× | 7.15% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.57ms | 93.38ms | 3.961× | 5.77% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.65ms | 103.93ms | 4.052× | 5.18% | PASS |
| ac_writes | `oltp_write_only_ac` | 26.28ms | 102.96ms | 3.918× | 4.41% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.91ms | 95.76ms | 4.179× | 9.73% | PASS |
| ac_writes | `oltp_read_write_ac` | 33.68ms | 109.64ms | 3.255× | 5.43% | PASS |

</details>

<details>
<summary>blobpk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 30.42ms | 36.55ms | 1.202× | 2.06% | PASS |
| mem_reads | `oltp_range_select` | 12.83ms | 13.98ms | 1.089× | 2.42% | PASS |
| mem_reads | `oltp_sum_range` | 11.88ms | 13.64ms | 1.149× | 1.61% | PASS |
| mem_reads | `oltp_order_range` | 2.89ms | 3.20ms | 1.107× | 1.14% | PASS |
| mem_reads | `oltp_distinct_range` | 3.91ms | 4.25ms | 1.087× | 1.06% | PASS |
| mem_reads | `oltp_index_scan` | 4.52ms | 5.96ms | 1.319× | 1.26% | PASS |
| mem_reads | `select_random_points` | 18.19ms | 20.45ms | 1.124× | 2.32% | PASS |
| mem_reads | `select_random_ranges` | 4.08ms | 5.21ms | 1.279× | 1.55% | PASS |
| mem_reads | `covering_index_scan` | 4.47ms | 4.64ms | 1.037× | 1.96% | PASS |
| mem_reads | `groupby_scan` | 31.71ms | 34.92ms | 1.101× | 0.97% | PASS |
| mem_reads | `index_join` | 6.85ms | 9.31ms | 1.358× | 2.29% | PASS |
| mem_reads | `index_join_scan` | 4.35ms | 5.31ms | 1.221× | 2.79% | PASS |
| mem_reads | `types_table_scan` | 1.09s | 1.24s | 1.139× | 1.86% | PASS |
| mem_reads | `table_scan` | 1.40s | 1.35s | 0.966× | 1.07% | PASS |
| mem_reads | `oltp_read_only` | 125.22ms | 139.65ms | 1.115× | 1.78% | PASS |
| mem_writes | `oltp_bulk_insert` | 241.28ms | 356.49ms | 1.477× | 1.13% | PASS |
| mem_writes | `oltp_insert` | 20.01ms | 38.95ms | 1.946× | 0.94% | PASS |
| mem_writes | `oltp_update_index` | 69.22ms | 127.78ms | 1.846× | 1.46% | PASS |
| mem_writes | `oltp_update_non_index` | 49.35ms | 83.76ms | 1.697× | 1.84% | PASS |
| mem_writes | `oltp_delete_insert` | 48.99ms | 100.97ms | 2.061× | 1.23% | PASS |
| mem_writes | `oltp_write_only` | 27.75ms | 60.20ms | 2.169× | 1.01% | PASS |
| mem_writes | `types_delete_insert` | 32.69ms | 53.31ms | 1.631× | 1.79% | PASS |
| mem_writes | `oltp_read_write` | 86.74ms | 139.17ms | 1.604× | 1.61% | PASS |
| file_reads | `oltp_point_select` | 99.81ms | 60.74ms | 0.609× | 0.73% | PASS |
| file_reads | `oltp_range_select` | 20.16ms | 16.57ms | 0.822× | 2.35% | PASS |
| file_reads | `oltp_sum_range` | 19.53ms | 16.43ms | 0.841× | 1.71% | PASS |
| file_reads | `oltp_order_range` | 3.80ms | 3.58ms | 0.942× | 1.53% | PASS |
| file_reads | `oltp_distinct_range` | 4.83ms | 4.64ms | 0.961× | 1.34% | PASS |
| file_reads | `oltp_index_scan` | 11.89ms | 8.75ms | 0.736× | 1.69% | PASS |
| file_reads | `select_random_points` | 26.73ms | 23.87ms | 0.893× | 2.19% | PASS |
| file_reads | `select_random_ranges` | 11.17ms | 7.68ms | 0.688× | 0.76% | PASS |
| file_reads | `covering_index_scan` | 12.00ms | 7.19ms | 0.599× | 3.03% | PASS |
| file_reads | `groupby_scan` | 32.70ms | 35.44ms | 1.084× | 0.87% | PASS |
| file_reads | `index_join` | 11.17ms | 11.06ms | 0.990× | 2.76% | PASS |
| file_reads | `index_join_scan` | 5.30ms | 5.75ms | 1.086× | 2.01% | PASS |
| file_reads | `types_table_scan` | 1.08s | 1.24s | 1.148× | 1.29% | PASS |
| file_reads | `table_scan` | 1.27s | 1.33s | 1.042× | 1.62% | PASS |
| file_reads | `oltp_read_only` | 226.73ms | 175.78ms | 0.775× | 1.34% | PASS |
| file_writes | `oltp_bulk_insert` | 261.16ms | 379.98ms | 1.455× | 0.84% | PASS |
| file_writes | `oltp_insert` | 31.87ms | 51.44ms | 1.614× | 1.60% | PASS |
| file_writes | `oltp_update_index` | 104.87ms | 160.29ms | 1.528× | 1.77% | PASS |
| file_writes | `oltp_update_non_index` | 78.33ms | 106.68ms | 1.362× | 1.35% | PASS |
| file_writes | `oltp_delete_insert` | 81.30ms | 127.62ms | 1.570× | 1.50% | PASS |
| file_writes | `oltp_write_only` | 54.36ms | 82.42ms | 1.516× | 1.76% | PASS |
| file_writes | `types_delete_insert` | 51.93ms | 71.31ms | 1.373× | 1.86% | PASS |
| file_writes | `oltp_read_write` | 115.92ms | 160.20ms | 1.382× | 1.83% | PASS |
| ac_reads | `oltp_point_select` | 54.37ms | 60.98ms | 1.122× | 1.32% | PASS |
| ac_reads | `oltp_range_select` | 16.01ms | 16.63ms | 1.039× | 2.06% | PASS |
| ac_reads | `oltp_sum_range` | 14.93ms | 16.38ms | 1.097× | 1.51% | PASS |
| ac_reads | `oltp_order_range` | 3.29ms | 3.56ms | 1.084× | 0.99% | PASS |
| ac_reads | `oltp_distinct_range` | 4.32ms | 4.61ms | 1.066× | 1.07% | PASS |
| ac_reads | `oltp_index_scan` | 7.20ms | 8.71ms | 1.210× | 1.39% | PASS |
| ac_reads | `select_random_points` | 21.38ms | 23.66ms | 1.107× | 2.21% | PASS |
| ac_reads | `select_random_ranges` | 6.70ms | 7.71ms | 1.150× | 1.14% | PASS |
| ac_reads | `covering_index_scan` | 7.61ms | 7.17ms | 0.943× | 2.82% | PASS |
| ac_reads | `groupby_scan` | 32.10ms | 35.41ms | 1.103× | 0.75% | PASS |
| ac_reads | `index_join` | 8.88ms | 11.11ms | 1.251× | 2.36% | PASS |
| ac_reads | `index_join_scan` | 4.86ms | 5.78ms | 1.190× | 2.98% | PASS |
| ac_reads | `types_table_scan` | 1.08s | 1.24s | 1.150× | 1.49% | PASS |
| ac_reads | `table_scan` | 1.39s | 1.34s | 0.966× | 1.88% | PASS |
| ac_reads | `oltp_read_only` | 161.71ms | 176.50ms | 1.091× | 0.92% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.10ms | 72.20ms | 3.267× | 4.92% | PASS |
| ac_writes | `oltp_insert_ac` | 24.07ms | 91.84ms | 3.816× | 5.77% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.91ms | 102.98ms | 3.827× | 4.19% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.43ms | 85.09ms | 3.632× | 6.96% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 24.11ms | 93.78ms | 3.890× | 5.03% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.19ms | 92.21ms | 3.661× | 5.25% | PASS |
| ac_writes | `types_delete_insert_ac` | 22.77ms | 86.47ms | 3.798× | 6.21% | PASS |
| ac_writes | `oltp_read_write_ac` | 30.02ms | 101.21ms | 3.372× | 5.60% | PASS |

</details>

<details>
<summary>compositepk workload details</summary>

| Section | Workload | SQLite median | DoltLite median | Ratio | Paired-ratio MAD | Result |
|---|---|---:|---:|---:|---:|---|
| mem_reads | `oltp_point_select` | 32.79ms | 39.48ms | 1.204× | 1.71% | PASS |
| mem_reads | `oltp_range_select` | 18.79ms | 21.99ms | 1.170× | 1.32% | PASS |
| mem_reads | `oltp_sum_range` | 17.79ms | 21.37ms | 1.201× | 1.18% | PASS |
| mem_reads | `oltp_order_range` | 3.46ms | 3.97ms | 1.146× | 1.17% | PASS |
| mem_reads | `oltp_distinct_range` | 4.55ms | 5.09ms | 1.118× | 0.84% | PASS |
| mem_reads | `oltp_index_scan` | 4.54ms | 6.02ms | 1.326× | 2.08% | PASS |
| mem_reads | `select_random_points` | 27.69ms | 32.03ms | 1.157× | 1.90% | PASS |
| mem_reads | `select_random_ranges` | 7.52ms | 8.90ms | 1.183× | 1.01% | PASS |
| mem_reads | `covering_index_scan` | 4.25ms | 4.25ms | 1.000× | 1.34% | PASS |
| mem_reads | `groupby_scan` | 36.12ms | 40.98ms | 1.134× | 0.81% | PASS |
| mem_reads | `index_join` | 8.21ms | 10.31ms | 1.256× | 1.09% | PASS |
| mem_reads | `index_join_scan` | 4.15ms | 5.42ms | 1.307× | 1.63% | PASS |
| mem_reads | `types_table_scan` | 1.07s | 1.25s | 1.169× | 0.58% | PASS |
| mem_reads | `table_scan` | 1.19s | 1.33s | 1.112× | 0.74% | PASS |
| mem_reads | `oltp_read_only` | 147.76ms | 173.25ms | 1.173× | 0.73% | PASS |
| mem_writes | `oltp_bulk_insert` | 248.95ms | 351.28ms | 1.411× | 1.09% | PASS |
| mem_writes | `oltp_insert` | 19.34ms | 35.84ms | 1.853× | 0.85% | PASS |
| mem_writes | `oltp_update_index` | 67.33ms | 114.21ms | 1.696× | 0.83% | PASS |
| mem_writes | `oltp_update_non_index` | 50.43ms | 81.62ms | 1.619× | 0.89% | PASS |
| mem_writes | `oltp_delete_insert` | 49.67ms | 93.38ms | 1.880× | 0.97% | PASS |
| mem_writes | `oltp_write_only` | 26.73ms | 56.37ms | 2.109× | 0.89% | PASS |
| mem_writes | `types_delete_insert` | 32.19ms | 53.45ms | 1.660× | 1.41% | PASS |
| mem_writes | `oltp_read_write` | 101.27ms | 154.06ms | 1.521× | 1.19% | PASS |
| file_reads | `oltp_point_select` | 101.96ms | 64.11ms | 0.629× | 0.99% | PASS |
| file_reads | `oltp_range_select` | 26.11ms | 24.55ms | 0.940× | 1.60% | PASS |
| file_reads | `oltp_sum_range` | 25.27ms | 24.43ms | 0.967× | 1.22% | PASS |
| file_reads | `oltp_order_range` | 4.33ms | 4.35ms | 1.004× | 1.46% | PASS |
| file_reads | `oltp_distinct_range` | 5.44ms | 5.48ms | 1.007× | 1.64% | PASS |
| file_reads | `oltp_index_scan` | 11.60ms | 8.87ms | 0.765× | 1.99% | PASS |
| file_reads | `select_random_points` | 36.85ms | 36.22ms | 0.983× | 1.35% | PASS |
| file_reads | `select_random_ranges` | 14.87ms | 11.76ms | 0.791× | 1.27% | PASS |
| file_reads | `covering_index_scan` | 11.23ms | 7.07ms | 0.629× | 1.66% | PASS |
| file_reads | `groupby_scan` | 37.22ms | 42.20ms | 1.134× | 1.09% | PASS |
| file_reads | `index_join` | 12.38ms | 12.76ms | 1.030× | 1.34% | PASS |
| file_reads | `index_join_scan` | 5.32ms | 6.09ms | 1.144× | 2.59% | PASS |
| file_reads | `types_table_scan` | 1.08s | 1.25s | 1.160× | 1.12% | PASS |
| file_reads | `table_scan` | 1.19s | 1.33s | 1.111× | 0.77% | PASS |
| file_reads | `oltp_read_only` | 251.14ms | 211.18ms | 0.841× | 0.85% | PASS |
| file_writes | `oltp_bulk_insert` | 264.41ms | 375.25ms | 1.419× | 1.07% | PASS |
| file_writes | `oltp_insert` | 26.11ms | 45.79ms | 1.753× | 1.98% | PASS |
| file_writes | `oltp_update_index` | 98.61ms | 142.19ms | 1.442× | 1.96% | PASS |
| file_writes | `oltp_update_non_index` | 75.95ms | 103.76ms | 1.366× | 1.13% | PASS |
| file_writes | `oltp_delete_insert` | 78.59ms | 119.08ms | 1.515× | 1.72% | PASS |
| file_writes | `oltp_write_only` | 51.47ms | 78.01ms | 1.516× | 2.52% | PASS |
| file_writes | `types_delete_insert` | 49.95ms | 67.84ms | 1.358× | 2.33% | PASS |
| file_writes | `oltp_read_write` | 127.93ms | 175.35ms | 1.371× | 1.65% | PASS |
| ac_reads | `oltp_point_select` | 56.07ms | 64.42ms | 1.149× | 1.32% | PASS |
| ac_reads | `oltp_range_select` | 21.45ms | 24.60ms | 1.147× | 1.25% | PASS |
| ac_reads | `oltp_sum_range` | 20.48ms | 24.40ms | 1.191× | 1.48% | PASS |
| ac_reads | `oltp_order_range` | 3.83ms | 4.36ms | 1.137× | 1.88% | PASS |
| ac_reads | `oltp_distinct_range` | 4.96ms | 5.48ms | 1.105× | 1.16% | PASS |
| ac_reads | `oltp_index_scan` | 7.12ms | 8.83ms | 1.240× | 2.11% | PASS |
| ac_reads | `select_random_points` | 31.62ms | 36.09ms | 1.141× | 1.58% | PASS |
| ac_reads | `select_random_ranges` | 10.33ms | 11.70ms | 1.132× | 1.24% | PASS |
| ac_reads | `covering_index_scan` | 6.89ms | 7.15ms | 1.039× | 2.05% | PASS |
| ac_reads | `groupby_scan` | 36.43ms | 42.28ms | 1.161× | 1.01% | PASS |
| ac_reads | `index_join` | 9.71ms | 12.60ms | 1.297× | 1.68% | PASS |
| ac_reads | `index_join_scan` | 4.64ms | 5.97ms | 1.285× | 2.07% | PASS |
| ac_reads | `types_table_scan` | 1.07s | 1.25s | 1.166× | 1.39% | PASS |
| ac_reads | `table_scan` | 1.22s | 1.33s | 1.093× | 1.55% | PASS |
| ac_reads | `oltp_read_only` | 188.15ms | 212.46ms | 1.129× | 1.24% | PASS |
| ac_writes | `oltp_bulk_insert_ac` | 22.48ms | 77.19ms | 3.434× | 5.48% | PASS |
| ac_writes | `oltp_insert_ac` | 25.03ms | 95.00ms | 3.795× | 8.71% | PASS |
| ac_writes | `oltp_update_index_ac` | 26.96ms | 109.84ms | 4.073× | 5.73% | PASS |
| ac_writes | `oltp_update_non_index_ac` | 23.22ms | 87.83ms | 3.783× | 5.32% | PASS |
| ac_writes | `oltp_delete_insert_ac` | 25.22ms | 99.49ms | 3.945× | 8.62% | PASS |
| ac_writes | `oltp_write_only_ac` | 25.16ms | 100.33ms | 3.987× | 6.55% | PASS |
| ac_writes | `types_delete_insert_ac` | 23.50ms | 89.14ms | 3.793× | 6.25% | PASS |
| ac_writes | `oltp_read_write_ac` | 32.40ms | 103.65ms | 3.199× | 5.28% | PASS |

</details>

## Version-control latency

Wall time: 2m 7s. Samples per benchmark: 101.

| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |
|---|---:|---:|---:|---:|---|
| `status_clean_many_tables` | 67.41ms | 130.00ms | 51.9% | 0.65% | PASS |
| `status_dirty_many_tables` | 69.69ms | 130.00ms | 53.6% | 0.38% | PASS |
| `diff_regular_working_one_table` | 63.77ms | 120.00ms | 53.1% | 0.31% | PASS |
| `diff_regular_working_many_tables` | 73.59ms | 140.00ms | 52.6% | 0.27% | PASS |
| `diff_stat_working_many_tables` | 73.25ms | 140.00ms | 52.3% | 0.22% | PASS |
| `diff_schema_working_many_tables` | 73.73ms | 140.00ms | 52.7% | 0.26% | PASS |
| `branch_list_many_branches` | 21.25ms | 35.00ms | 60.7% | 0.92% | PASS |
| `branch_create_delete` | 32.11ms | 40.00ms | 80.3% | 1.68% | PASS |
| `checkout_branch_clean` | 102.28ms | 150.00ms | 68.2% | 3.71% | PASS |
| `merge_data_no_conflicts` | 38.55ms | 50.00ms | 77.1% | 1.06% | PASS |
| `merge_schema_no_conflicts` | 22.09ms | 35.00ms | 63.1% | 1.62% | PASS |
| `merge_data_conflicts` | 80.10ms | 180.00ms | 44.5% | 0.33% | PASS |
| `merge_data_conflicts_with_resolve` | 80.04ms | 180.00ms | 44.5% | 0.34% | PASS |

Version-control ceiling result: **PASS**.

## Reproducing

The workload definitions live in `test/sysbench_compare*.sh` and `test/vc_perf_ceiling.sh`. The nightly workflow retains the complete raw samples and generated reports as Actions artifacts for 30 days.
