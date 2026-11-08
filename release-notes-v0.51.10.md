<!-- Draft: DoltLite 0.51.10. Changes from v0.51.9 through master 509df3f160. -->

Improved SQLite compatibility, database recovery, version-control correctness, and language bindings.

- Improved ordinary SQLite file support for `ALTER TABLE`, `VACUUM`, incremental vacuum, backups, `sqlite_dbpage`, and query planning. Aligned invalid-image handling in `sqlite3_deserialize` with SQLite.
- Prevented damaged DoltLite files from being reinitialized, restored SQLite WAL recovery after torn headers, and propagated transient file-probe errors without selecting the wrong engine.
- Preserved numeric types in expression indexes and added-column defaults, and fixed false integrity errors on `NOCASE` indexes. Existing expression indexes affected by the numeric-type bug need `REINDEX`.
- Fixed stale branch views after peer commits, including DoltLite attachments under a SQLite main database, and corrected nonlocal `ALTER` and `CREATE INDEX` routing.
- Fixed criss-cross merges with conflicting merge-base rows under unchanged schemas, and identical inserts across dropped-column changes.
- Preserved ignored and unstaged changes when committing analyzed merges, and rejected partial staging or reset operations that would remove another table's index.
- Improved rebase-plan action validation and SQL dump portability, and aligned forced branch self-copies with Dolt.
- Fixed `dolt_dump` exports for Unicode and control characters, wide tables, shadowed rowids, historical `commit_ref` columns, and `fts3tokenize` tables. Exports now reject views and triggers that depend on DoltLite system tables.
- Reduced constraint-validation work for small merges, batched conflict-row deletions, and accelerated equality probes of deleted keys.
- Fixed WASM database closing after commits, OPFS file opens, and Node.js imports from the WASM package root.
- Fixed Go context cancellation during statement compilation.
- Added Rust `Connection::open_with_flags` and `OpenFlags`. Invalid UTF-8 TEXT now returns as `Value::Blob` with its original bytes.
