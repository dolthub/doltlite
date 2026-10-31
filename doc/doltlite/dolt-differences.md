# How DoltLite differs from Dolt

Dolt is the reference: for any version-control operation, what Dolt does is
what DoltLite should do at the row level. The differences below are
deliberate and will not change. Anything else that disagrees with Dolt is a
bug; please file it.

## Same operations, SQLite shapes

| Dolt | DoltLite |
|---|---|
| `CALL dolt_commit('-m', ...)` returns a result row | `SELECT dolt_commit('-m', ...)` returns a scalar: the hash, or a status string |
| `SELECT * FROM dolt_commit_diff_t WHERE from_commit=... AND to_commit=...` | `SELECT * FROM dolt_diff_t('from', 'to')` |
| `USE mydb/feature` switches a session to a branch | Open the path `mydb.db@feature`, or `dolt_checkout('feature')` |
| `@@dolt_*` session variables | None. SQLite has no session variables; `dolt_config('user.name', ...)` sets the committer per connection. |
| Stored procedures, `dolt_procedures` | None. SQLite has no stored procedures. |
| One database per server, many databases | One file per database. There is no `dolt_undrop`, `dolt_purge_dropped_databases`, or `dolt_backup`; the file is the backup. |
| Column names on `dolt_log`, `dolt_diff`, and other system tables | May differ. Compare rows, not headers. |

Only command functions are direct-only; `dolt_hashof` and `dolt_version` are not.

## Not implemented, on purpose

| Surface | Why |
|---|---|
| `dolt_stash`, `dolt_stashes` | Every branch has its own working set and `dolt_checkout` never carries or refuses uncommitted work, so there is nothing to shelve. |
| `dolt_rm`, `dolt_mv` | `DROP TABLE` and `ALTER TABLE ... RENAME TO` already are the SQL. |
| `dolt_statistics`, `dolt_stats_*` | Statistics are `ANALYZE` and `sqlite_stat1`, an ordinary versioned table: per branch, committed, diffed, and merged with everything else. |
| `dolt_query_catalog` | DoltHub storage, not engine behaviour. |
| `dolt_commits`, `dolt_help`, `dolt_column_diff`, `dolt_reflog`, `has_ancestor` | Not yet. `dolt_log` and `dolt_commit_ancestors` cover the commit graph. |
| `dolt_commit_all`, `@@dolt_multi_branch_commit` | Not yet. A transaction can write several branches atomically ([concurrency.md](concurrency.md#several-branches-in-one-transaction)); commit each branch's working set from that branch. |
| Replication, read replicas, `dolt_backup` remotes | Sync is `dolt_push` / `dolt_pull` against a filesystem or HTTP remote. |

## Behaviour that differs

- **Conflicts are never committed.** Dolt can persist a conflicted working
  set behind `@@dolt_allow_commit_conflicts`. DoltLite refuses `COMMIT`
  while conflicts exist and rolls back an autocommit merge that conflicts.
  Constraint violations still persist, as in Dolt. See
  [transactions.md](transactions.md).
- **A Dolt commit is always durable when it returns.** Inside `BEGIN`,
  `dolt_commit` commits the SQL transaction and begins a new one, as Dolt
  does; `@@dolt_transaction_commit` has no counterpart.
- **Refs are not transactional.** `dolt_branch` and `dolt_tag` take effect
  immediately and survive `ROLLBACK`.
- **`dolt_docs` rows are stored, including the default.** A fresh database
  serves an `AGENT.md` row whose text is DoltLite's own guide. Deleting it
  sticks, it shows in diffs, and the table appears in `.tables` once it exists.
  Dolt resurrects its defaults on read.
- **Rowids are versioned identity.** Implicit rowids come from a counter
  shared by every branch so inserts merge cleanly; a non-integer primary key
  has a read-only `rowid` alias. Dolt has no rowid.
- **Vtable column collisions are suffixed.** If a user column in
  `dolt_diff_<table>`, `dolt_history_<table>`, or `dolt_conflicts_<table>`
  collides case-insensitively with a metadata column, the user column gets
  `_1`, `_2`, ... because SQLite requires unique column names.
- **Merges that cannot be represented are refused, not approximated.** A
  merge involving a primary-key change, or a derived index the engine cannot
  rebuild correctly, fails rather than producing a table with the wrong shape.
- **`dolt_push('--all')` requires the remote name.** Dolt pushes to the
  configured destination when that name is omitted. DoltLite has no separate
  push destination, so the call is `dolt_push('--all', remote)`.
- **Nonlocal tables are partial.** [dolt_nonlocal_tables](dolt_nonlocal_tables.md)
  redirects names as Dolt does, but nonlocal tables are not listed in
  `.tables`.
- **Deleting an absent remote branch succeeds.** `dolt_push(remote, ':branch')`
  returns 0 when that branch is already gone, including when this database
  has no tracking ref for it. Dolt reports an error and leaves the remote
  unchanged, which is the same end state.

## Same as Dolt, in case you wondered

Three-way row-level merge semantics, conflict and constraint-violation
tables, `dolt_diff_<table>` and `dolt_history_<table>` row shapes, revision
ranges, `dolt_ignore` pattern rules, `dolt_tests` assertions, and the refusal
to check out a commit or tag directly (a detached read-only snapshot is
opened by path instead). The oracle suites under `test/vc_oracle_*` run the
same SQL against both engines and diff the rows.
