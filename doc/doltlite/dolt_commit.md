# dolt_commit, dolt_add, dolt_status, dolt_config

The commit loop: stage tables, commit them, see what is pending. Dolt
equivalents: [dolt_add](https://docs.dolthub.com/sql-reference/version-control/dolt-sql-procedures#dolt_add),
[dolt_commit](https://docs.dolthub.com/sql-reference/version-control/dolt-sql-procedures#dolt_commit),
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
| `-A`, `--all` | Stage every changed table not matched by `dolt_ignore` |
| `-f`, `--force` | Stage a table even though `dolt_ignore` matches it |

Returns `0`. No arguments: `dolt_add requires table name or '-A'`.

## dolt_commit

| Option | Meaning |
|---|---|
| `-m`, `--message` | Commit message. Required unless `--amend`. |
| `-a`, `--all` | Stage every modified or deleted tracked table first. Does not add new tables. |
| `-A` | Stage everything, new tables included |
| `--author 'Name <email>'` | Override the committer for this commit |
| `--date` | Commit timestamp, ISO 8601 (`2020-01-02T03:04:05Z`) |
| `--amend` | Replace the HEAD commit with staged changes and, if given, a new message |
| `--allow-empty` | Create a commit even when nothing is staged |
| `--skip-empty` | Return `0` instead of erroring when nothing is staged |
| `-f`, `--force` | Commit despite rows in `dolt_constraint_violations` |

Returns the new commit hash. Inside `BEGIN`, a successful commit also ends
the SQL transaction; see [transactions.md](transactions.md).

| Error | Cause |
|---|---|
| `dolt_commit requires a message: SELECT dolt_commit('-m', 'msg')` | no `-m` unless `--amend`, or empty message |
| `nothing to commit, working tree clean (use dolt_add to stage changes)` | nothing staged and no `-a`/`-A` |
| `Author not formatted correctly. Use 'Name <author@example.com>' format` | bad `--author` |
| `cannot commit: unresolved merge conflicts. Use dolt_conflicts_resolve() first.` | see [dolt_merge.md](dolt_merge.md) |
| `cannot commit: unresolved entries in dolt_constraint_violations ...` | see [dolt_constraint_violations.md](dolt_constraint_violations.md) |
| `cannot --amend: HEAD has no parent (initial commit)` | amend on the first commit |
| `you are in the middle of a merge -- cannot amend` | `--amend` while a merge is in progress |
| `you are in the middle of a cherry-pick -- cannot amend` | `--amend` while a cherry-pick or revert is in progress |
| `you are in the middle of a rebase -- cannot amend` | `--amend` while a rebase is in progress |
| `unknown option`, `no value for option` | option parsing; the message names the option |

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
always durable when it returns.

## See also

[dolt_reset.md](dolt_reset.md), [dolt_workspace.md](dolt_workspace.md) for
row-level staging, `test/vc_oracle_commit_test.sh`.
