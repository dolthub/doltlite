# PRAGMA support

Every SQLite pragma parses and runs on a DoltLite-format database. None
errors. The question is which ones mean what they mean in SQLite. This page
covers the main database; attached stock SQLite files and the `temp` schema
run on SQLite's own engine and behave exactly as stock.

Verified by running each pragma against `build/doltlite` and a stock
`sqlite3` of the same version on the same schema and diffing the output.

## Same as SQLite

Behaviour and output match stock:

`analysis_limit`, `application_id`, `automatic_index`, `busy_timeout`,
`case_sensitive_like`, `collation_list`, `compile_options`, `count_changes`,
`data_version`, `database_list`, `defer_foreign_keys`,
`empty_result_callbacks`, `foreign_key_check`, `foreign_key_list`,
`foreign_keys`, `full_column_names`, `function_list`, `hard_heap_limit`,
`ignore_check_constraints`, `index_info`, `index_list`, `legacy_alter_table`,
`module_list`, `optimize`, `pragma_list`, `query_only`, `read_uncommitted`,
`recursive_triggers`, `reverse_unordered_selects`, `short_column_names`,
`shrink_memory`, `soft_heap_limit`, `temp_store`,
`temp_store_directory`, `threads`, `trusted_schema`, `user_version`.

Two of these deserve a note. `query_only` also blocks every `dolt_*` function
that would write. `optimize` may run `ANALYZE`, and `sqlite_stat1` is a
versioned table like any other.

## Same intent, DoltLite mechanics

| Pragma | Behaviour |
|---|---|
| `cache_size` | Sizes the chunk cache. Negative values are a KiB budget as in SQLite. The floor is the engine default of 16384 chunks; smaller requests are raised to it. Reads back `-2000` (SQLite's default) until set, which understates the real capacity. |
| `synchronous` | `OFF` skips fsync on commit; every other level syncs. |
| `fullfsync` | Honoured on macOS. |
| `integrity_check`, `quick_check` | Run SQLite's row and index checks, then walk the chunk graph of the named tables and of every branch, tag, and working set. Missing or corrupt chunks count as errors. |
| `table_info`, `table_xinfo` | Non-integer primary key columns report `notnull=1`, because clustered keys are `NOT NULL`. |
| `table_list` | A table with a non-integer primary key reports `wr=1`; it is stored like a `WITHOUT ROWID` table. |
| `index_xinfo` | The automatic index of a non-integer primary key returns no rows: the key is the table, not a separate index. |
| `writable_schema` | Accepted, but `sqlite_master` still refuses writes. The catalog is a projection, not a table. |
| `schema_version` | Increments on every schema change. Assigning a value is ignored. |
| `data_version` | Changes when another connection commits, as in SQLite. |

## Accepted and inert

These read back a fixed value and ignore assignment. There are no pages,
journal, WAL, or freelist for them to act on.

| Pragma | Reads back |
|---|---|
| `journal_mode` | `wal`, whatever you set |
| `wal_checkpoint` | `0\|0\|0`. Every mode runs DoltLite garbage collection instead. |
| `wal_autocheckpoint` | the value you set; nothing to checkpoint |
| `auto_vacuum` | `0`. `incremental_vacuum` does nothing. `VACUUM` runs garbage collection. |
| `encoding` | `UTF-8`, including on an empty database |
| `page_size` | **the value you set**, even though no pages exist. Do not read it as confirmation. |
| `page_count`, `max_page_count` | chunk counts, not pages |
| `freelist_count` | `0` |
| `cache_spill` | `0` |
| `mmap_size` | `0` |
| `journal_size_limit` | `-1` |
| `secure_delete` | `0`. Content-addressed chunks are reclaimed by garbage collection, not overwritten. |
| `locking_mode` | the value you set; the writer lock is the graph lock and does not change |
| `cell_size_check`, `checkpoint_fullfsync` | the value you set |

Debug-build pragmas (`vdbe_trace`, `lock_status`, `stats`, and friends) are
unchanged from SQLite and absent from release builds.

## Related

`VACUUM INTO` writes a compacted DoltLite-format copy; `:memory:` as the
destination is refused. `dbstat` is a virtual table, not a pragma, and is
unsupported on DoltLite-format databases. The compatibility contract that
pins the tested rows is [sqlite-compatibility.md](sqlite-compatibility.md).
