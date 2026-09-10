# Transactions and version control

Version-control functions run inside SQLite transactions like any other
statement, with two rules that are easy to miss.

## What `ROLLBACK` undoes

| Inside `BEGIN` | On `ROLLBACK` |
|---|---|
| Row writes, `dolt_add`, `dolt_reset('--soft')` | Undone, staging included |
| `dolt_branch`, `dolt_tag`, `dolt_branch('-d')` | Kept. Refs are durable the moment the call returns. |
| `dolt_reset('--hard')` | Kept. The working set is already back at `HEAD`. |
| `dolt_checkout` | Kept. The connection stays on the new branch. |
| `dolt_commit`, and any merge, cherry-pick, revert, or pull that completes | Kept, and the SQL transaction is already over |

A Dolt commit is durable when it returns, so a successful `dolt_commit`
inside `BEGIN` also commits the SQL transaction. A following `ROLLBACK`
fails with `cannot rollback - no transaction is active`, and a `SAVEPOINT`
opened before it is gone. The same holds for an operation that creates a
commit or fast-forwards a branch. A `dolt_commit` that finds nothing to
commit has also already ended the transaction, as in Dolt; only option and
author errors are raised before that point. Put `dolt_commit` last.

Rows written before a `dolt_checkout` in the same transaction stay with the
branch they were written on.

## Conflicts live only inside a transaction

A merge, cherry-pick, revert, or pull that produces conflicts behaves
differently in the two modes:

| Mode | Outcome |
|---|---|
| Autocommit | The whole operation is rolled back. Error: `cannot merge: conflicts detected, autocommit transaction rolled back. Run the merge inside BEGIN/COMMIT ...` |
| Inside `BEGIN` | The transaction stays open with `dolt_conflicts`, `dolt_conflicts_<table>`, and `dolt_schema_conflicts` populated. Error: `Merge has N conflict(s). Resolve and then commit with dolt_commit.` |

While conflicts remain, `dolt_commit` fails with `cannot commit: unresolved
merge conflicts` and `COMMIT` fails with `constraint failed`. Resolve with
`dolt_conflicts_resolve('--ours' | '--theirs', table)` or by editing
`dolt_conflicts_<table>`, then `dolt_commit` records the merge commit.
Nothing conflicted is ever written to disk, so no other connection can see
or inherit a half-finished merge. Dolt can commit a conflicted working set;
DoltLite deliberately cannot.

Constraint violations are the exception. A merge that leaves violations in
autocommit mode is rolled back with `Committing this transaction resulted in a
working set with constraint violations, transaction rolled back.` Inside
`BEGIN` they persist in `dolt_constraint_violations_<table>` and block
`dolt_commit` until cleared or forced. See
[sqlite-compatibility.md](sqlite-compatibility.md) and
[concurrency.md](concurrency.md) for the contracts behind this.

## Busy outcomes

One durable writer at a time per database file. What a second connection sees:

| Situation | Result |
|---|---|
| A peer holds an open write transaction | `database is locked` (`SQLITE_BUSY`). `busy_timeout` applies. |
| A read transaction tries to write after a peer committed | `database is locked` (`SQLITE_BUSY_SNAPSHOT`). The read snapshot is stale; roll back and retry. |
| `dolt_commit` while a peer advanced the branch first | `commit conflict: another connection committed to this branch. Please retry your transaction.` |
| Merge, cherry-pick, revert, pull racing a peer commit | `SQLITE_BUSY`; nothing is lost, retry the operation |

Readers never block on writers and never see uncommitted rows or staging.

## Refusals

| Statement | Error |
|---|---|
| Writing two file-backed databases in one transaction | `atomic commit across multiple file-backed databases is not supported`; the transaction is rolled back |
| Any `dolt_*` write under `PRAGMA query_only=1` or `immutable=1` | `attempt to write a readonly database` |
| A `dolt_*` command inside a view, trigger, generated column, or index | `unsafe use of <function>()` at parse time; command functions are direct-only |
| `VACUUM` inside a transaction | `cannot VACUUM from within a transaction`, as in SQLite |
