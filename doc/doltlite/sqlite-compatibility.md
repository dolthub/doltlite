# SQLite compatibility

DoltLite targets SQLite SQL semantics and uses the bundled SQLite version's
public C declarations and `sqlite3_*` symbol names. That is API-surface
compatibility, not a claim that storage-coupled APIs keep SQLite pager or file
format semantics.

For a DoltLite-format main database, the compatibility contract is:

- DoltLite uses its own on-disk format. Standard SQLite files are detected and
  routed to SQLite's original B-tree engine, but Dolt version-control features
  are available only on DoltLite-format databases.
- No SQLite rollback-journal, WAL, or shared-memory sidecar is created.
  `PRAGMA journal_mode` reports `wal` as a compatibility value and ignores
  requests to change it. All `PRAGMA wal_checkpoint` modes bridge to DoltLite
  garbage collection and report zero WAL frames.
- A transaction that writes more than one file-backed database is rejected
  with `atomic commit across multiple file-backed databases is not supported`
  and rolled back in full. This includes TEMP triggers that write `main` while
  changing an attached file. Single-file writes and transactions involving a
  `:memory:` attachment are supported.
- `PRAGMA auto_vacuum` reports `0`; attempts to enable it and
  `PRAGMA incremental_vacuum` are no-ops. `VACUUM` runs DoltLite garbage
  collection instead of rebuilding SQLite pages. File-backed `VACUUM INTO`
  writes a compacted DoltLite-format copy; `:memory:` as the destination is
  refused. `SQLITE_DBCONFIG_RESET_DATABASE` plus `VACUUM` empties the
  current branch working catalog (`sqlite_master` has no user objects)
  and zeros `user_version` and `application_id`; other branches and
  commit history remain, so `dolt_reset('--hard')` restores this branch
  from HEAD.
- Text is stored as UTF-8. Requests for a UTF-16 database encoding leave
  `PRAGMA encoding` at `UTF-8`.
- Implicit rowids are allocated from a counter shared by every branch of a
  database, so an `INSERT` that omits the `INTEGER PRIMARY KEY` (or the
  rowid of a table without a primary key) never gets an id another branch
  already used, and such inserts merge cleanly. This gives every rowid table
  `AUTOINCREMENT` allocation: after the largest row is deleted the next id
  continues rather than being reused. `DROP TABLE` resets the counter and
  `ALTER TABLE ... RENAME` carries it. Only tables declared `AUTOINCREMENT`
  also record the counter in `sqlite_sequence`, which remains the reset
  surface for them: `UPDATE sqlite_sequence SET seq=N`,
  `DELETE FROM sqlite_sequence`, and inserting a seed row set or drop the
  shared counter for that table, after which the next id is
  `max(seq, max(rowid))+1` exactly as in SQLite.
- For tables without a primary key, `dolt_patch` selects a rowid alias using
  SQLite's case-insensitive identifier rules. Declared columns shadow `rowid`,
  `_rowid_`, and `oid` regardless of case; patch generation fails if all three
  aliases are shadowed.
- Named in-memory databases are shared between connections the way SQLite
  shares them: `file:name?mode=memory&cache=shared`, `file::memory:?cache=shared`,
  and `file:/name?vfs=memdb` open one store per name inside the process, with
  writers serialized like a file. `:memory:`, `mode=memory` without shared
  cache, and slash-less `vfs=memdb` names stay private to their connection.
  `dolt_gc` and `VACUUM INTO` treat a shared in-memory database as in-memory.
- `PRAGMA query_only` covers version control: while it is set, `dolt_add`,
  `dolt_commit`, `dolt_merge`, `dolt_tag`, `dolt_branch`, `dolt_gc`, and every
  other function that would change the file fail with `attempt to write a
  readonly database`, the same as DML. The `immutable=1` URI parameter opens
  the database read-only, as it does in SQLite.
- Application-defined collations registered with `sqlite3_create_collation*`
  are supported for expressions and unindexed columns. Persisted index keys,
  `UNIQUE` constraints, and non-integer primary keys using them are rejected
  because prolly sort keys cannot depend on application callbacks. An index
  may override such a column with `BINARY`, `NOCASE`, or `RTRIM`. Replacing one
  of those built-ins is rejected while a persisted index uses its name.
- A table with a non-`INTEGER PRIMARY KEY` is keyed by that primary key.
  `rowid` and `last_insert_rowid()` still work as a read-only SQL alias:
  a single integer PK is that value, otherwise a stable hash of the PK.
  `INSERT` and `UPDATE` of `rowid` fail with `no such column`, matching
  explicit `WITHOUT ROWID` — there is no stored `rowid` column. TEMP tables
  are not clustered, so those writes still work. An `INTEGER PRIMARY KEY`
  remains a writable rowid alias. Explicit `WITHOUT ROWID` tables have no
  `rowid` at all, matching SQLite. `.dump --preserve-rowids` omits the
  read-only alias from clustered-primary-key inserts so its output restores.
- Those clustered primary keys are `NOT NULL`, matching SQLite
  `WITHOUT ROWID` tables. `PRAGMA table_info` reports `notnull=1` on the PK
  columns, and inserting NULL fails with `NOT NULL constraint failed`. SQLite
  rowid tables still allow NULL in a TEXT, `INT`, `INTEGER PRIMARY KEY DESC`,
  or composite PK. TEMP tables are not clustered and keep SQLite's nullable
  PK. An `INTEGER PRIMARY KEY` remains a rowid alias.
- `sqlite_master` / `sqlite_schema` is a projection of the prolly catalog,
  not a stored table of verbatim DDL. After a schema commit, `sql` is the
  canonical `CREATE` text (whitespace and quoting normalized) and row order
  follows the catalog, not insertion order. `CHECK` constraint error messages
  follow that canonical form. Query results and constraint enforcement are
  unchanged.
- The `doltlite` CLI is the SQLite shell with a few deliberate differences:
  `.schema` and `.dump` list objects in catalog order and print the
  canonical `CREATE` text, the prompt is `doltlite> `, and `-version` prints
  the DoltLite version. Everything else, including `-deserialize`,
  `.open --deserialize|--zip|--hexdb`, `db@branch` open syntax and the
  substitute in-memory database on an unopenable path, follows the upstream
  shell; the upstream `shell*.test` files run against the CLI in CI.
- `sqlite3_backup_step()` copies a file-backed DoltLite database, including an
  attached database, as one operation; its page-count argument is not
  incremental. File-backed and in-memory DoltLite databases can be copied in
  either direction.
- `sqlite3_serialize()` and `sqlite3_deserialize()` use a contiguous native
  DoltLite database image, including the commit graph, refs, and working sets.
  The image is an existing DoltLite storage-format file represented as bytes,
  not a SQLite page image or a SQL dump, and is not readable by stock SQLite.
  `sqlite3_deserialize()` of a stock SQLite page image reopens that schema on
  SQLite's original B-tree engine, as stock does, without version control.
- `dbstat` is not supported on a DoltLite-format database: the chunk store has
  no SQLite page layout. A scan fails with an error rather than reporting an
  empty database. `dbstat` on an attached stock SQLite file still walks pages.

The machine-readable contract and its test mapping live in
[`test/sqlite_compatibility_contract.tsv`](../../test/sqlite_compatibility_contract.tsv).
The inherited-suite backlog lives with the assertions it gates, in
[`test/known_testfixture_divergences.txt`](../../test/known_testfixture_divergences.txt):
each line names one assertion and carries its disposition as
`class=intentional|unsupported|harness|engine-gap`, plus `issue=<number>` where
one is required. Gates classified as `engine-gap` are bugs to fix, not
compatibility promises.
