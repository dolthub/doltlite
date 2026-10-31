# dolt_commit, dolt_commit_all, dolt_add, dolt_status, dolt_config

The commit loop: stage tables, commit them, see what is pending. Dolt
equivalents: [dolt_add](https://docs.dolthub.com/sql-reference/version-control/dolt-sql-procedures#dolt_add),
[dolt_commit](https://docs.dolthub.com/sql-reference/version-control/dolt-sql-procedures#dolt_commit),
`dolt_commit_all`,
[dolt_status](https://docs.dolthub.com/sql-reference/version-control/dolt-system-tables#dolt_status).

## Synopsis

```sql
SELECT dolt_config('user.name', 'Ann');
SELECT dolt_add('users', 'orders');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'message');
UPDATE users SET active = 0 WHERE id = 2;
SELECT dolt_commit('-am', 'message');           -- -a and -m combine
SELECT dolt_commit('--allow-empty', '-m', 'message', '--author', 'Ann <ann@example.com>');
SELECT * FROM dolt_status;
```

## dolt_add

| Argument | Meaning |
|---|---|
| `table`, ... | Stage the named tables. Unknown name: `table not found: <name>` |
| `.` | Stage every changed table, but only when it is the only table argument. Together with a name, each `.` is a missing table: `error: the table(s) . do not exist` |
| `-A`, `--all` | Stage every changed table not matched by `dolt_ignore`. Table arguments, including `.`, are ignored |
| `-f`, `--force` | Stage a table even though `dolt_ignore` matches it |

Returns `0`. No arguments: `dolt_add requires table name or '-A'`.

## dolt_commit

| Option | Meaning |
|---|---|
| `-m`, `--message` | Commit message. Required unless `--amend`. |
| `-a`, `--all` | Stage every modified or deleted tracked table first. Does not add new tables. |
| `-A`, `--ALL` | Stage everything, new tables included |
| `--author 'Name <email>'` | Override the committer for this commit |
| `--date` | Commit timestamp, ISO 8601 (`2020-01-02T03:04:05Z`) |
| `--amend` | Replace the HEAD commit with staged changes and, if given, a new message. The initial commit can be amended and stays parentless |
| `--allow-empty` | Create a commit even when nothing is staged |
| `--skip-empty` | Return `0` instead of erroring when nothing is staged |
| `-f`, `--force` | Commit despite rows in `dolt_constraint_violations` |
| `--skip-verification` | Accepted for Dolt compatibility; stored tests still run explicitly |

DoltLite runs stored tests through [`dolt_test_run`](dolt_tests.md#dolt_test_run),
not automatically on commits or merges. `--skip-verification` leaves explicit
test runs, conflict checks, and constraint checks unchanged.

Returns the new commit hash. Inside `BEGIN`, a successful commit also
commits the SQL transaction and begins a new one; see
[transactions.md](transactions.md).

| Error | Cause |
|---|---|
| `dolt_commit requires a message: SELECT dolt_commit('-m', 'msg')` | no `-m` unless `--amend`, or empty message |
| `nothing to commit, working tree clean (use dolt_add to stage changes)` | nothing staged and no `-a`/`-A` |
| `Author not formatted correctly. Use 'Name <author@example.com>' format` | bad `--author` |
| `cannot commit: unresolved merge conflicts. Use dolt_conflicts_resolve() first.` | see [dolt_merge.md](dolt_merge.md) |
| `cannot commit: unresolved entries in dolt_constraint_violations ...` | see [dolt_constraint_violations.md](dolt_constraint_violations.md) |
| `you are in the middle of a merge -- cannot amend` | `--amend` while a merge is in progress |
| `you are in the middle of a cherry-pick -- cannot amend` | `--amend` while a cherry-pick or revert is in progress |
| `you are in the middle of a rebase -- cannot amend` | `--amend` while a rebase is in progress, other than an `edit` pause |
| `unknown option`, `no value for option` | option parsing; the message names the option |
| `error: multiple values provided for` an option | the same option was given more than once, including `-m` / `--message` |
| `Cannot commit changes on more than one branch / database` | another branch of this database has writes in the transaction; use `dolt_commit_all` |

## dolt_commit_all

Commits every branch of this database the current transaction wrote, as
one publish: each branch gets its own commit with the same message and
options, and either every head moves or none does. Branches are written
together through [nonlocal tables](dolt_nonlocal_tables.md) or by attaching
another branch of the same file; see
[concurrency.md](concurrency.md#several-branches-in-one-transaction).

```sql
SELECT dolt_checkout('feature');
INSERT INTO dolt_nonlocal_tables VALUES ('orders', 'main', NULL, 'immediate');
BEGIN;
INSERT INTO orders VALUES (10, 1, 5.0);        -- main's working set
UPDATE users SET active = 0 WHERE id = 3;      -- feature's
SELECT key AS branch, value AS hash
  FROM json_each(dolt_commit_all('-Am', 'both branches'));
-- feature|<hash>
-- main|<hash>
SELECT dolt_checkout('main');
```

Takes `dolt_commit`'s options, except `--branch`. Each option applies to
every branch: `-a` and `-A` stage on each branch, and `--amend` amends each
branch's HEAD.

- A branch the transaction wrote counts even if its rows ended up unchanged.
  With no such branch, the session's branch is committed, so outside a
  transaction it acts as `dolt_commit`.
- Returns a JSON object of branch name to new commit hash, in branch order.
  A branch skipped by `--skip-empty` is left out, and `{}` means nothing was
  committed.
- Like `dolt_commit`, it commits the SQL transaction and begins a new one.
  Every branch's working set is published with it, including a branch that
  `--skip-empty` skipped.
- A branch whose head moved since this transaction read it fails the call
  with `cannot commit: database is busy or branch HEAD changed. Please retry
  your transaction.`

| Error | Cause |
|---|---|
| `nothing to commit on <branch>` | that branch has nothing staged; no head moves and the transaction stays open |
| `--branch is not supported by dolt_commit_all` | `--branch` given |

Other errors are `dolt_commit`'s.

## dolt_status

| Column | Values |
|---|---|
| `table_name` | table, or `dolt_ignore` / `dolt_docs` / `dolt_tests` once they exist |
| `staged` | `1` staged, `0` working |
| `status` | `new table`, `modified`, `deleted` |

A table that is both staged and further modified appears twice. Tables
matched by `dolt_ignore` are hidden. An index change shows as a modification
of its table.

## dolt_config

`dolt_config(key)` reads, `dolt_config(key, value)` sets. Keys: `user.name`,
`user.email`. Per connection, not persisted. Without them, commits record
committer `doltlite` with an empty email. Anything else:
`unknown config key (valid: user.name, user.email)`.

## Differences from Dolt

Functions instead of procedures; `-A` stages new tables where Dolt's `-A`
does the same. `@@dolt_transaction_commit` has no counterpart: a commit is
always durable when it returns. `dolt_commit_all` returns a JSON object
where Dolt returns one `(branch, hash)` row per branch, and
`@@dolt_multi_branch_commit`, which makes `dolt_commit` behave as
`dolt_commit_all`, is not implemented.

## See also

[dolt_reset.md](dolt_reset.md), [dolt_workspace.md](dolt_workspace.md) for
row-level staging, `test/vc_oracle_commit_test.sh`.
