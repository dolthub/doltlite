# dolt_branch, dolt_checkout, dolt_branches

Branches and the connection's position on them. Dolt:
[dolt_branch](https://docs.dolthub.com/sql-reference/version-control/dolt-sql-procedures#dolt_branch),
[dolt_checkout](https://docs.dolthub.com/sql-reference/version-control/dolt-sql-procedures#dolt_checkout),
[dolt_branches](https://docs.dolthub.com/sql-reference/version-control/dolt-system-tables#dolt_branches).

## Synopsis

```sql
SELECT dolt_branch('feature');                 -- from HEAD
SELECT dolt_branch('feature', 'v1.0');         -- from any revision
SELECT dolt_branch('-m', 'old', 'new');        -- rename
SELECT dolt_branch('-c', 'src', 'copy');       -- copy
SELECT dolt_branch('-d', 'feature');           -- delete if merged
SELECT dolt_branch('-D', 'feature');           -- delete regardless
SELECT dolt_checkout('feature');
SELECT dolt_checkout('-b', 'feature2', 'main');
SELECT dolt_checkout('users');                 -- restore one table from HEAD
SELECT active_branch();
SELECT * FROM dolt_branches;
```

## dolt_branch

| Option | Meaning |
|---|---|
| `name [, rev]` | Create `name` at `rev` (default `HEAD`) |
| `-m`, `--move old new` | Rename |
| `-c`, `--copy old new` | Copy |
| `-d`, `--delete name` | Delete. Refused for the current branch, the default branch, or an unmerged branch. |
| `-D name` | Delete even if unmerged |
| `-f`, `--force` | With `name rev`: move an existing branch to `rev` |

Returns `0`. Branch and tag changes are durable immediately, even inside
`BEGIN`. Name rules are in [refs.md](refs.md).

| Error | Cause |
|---|---|
| `branch already exists` | create or rename onto an existing name |
| `cannot delete the current branch` | `-d` on the checked-out branch |
| `cannot delete the default branch; call dolt_default_branch(<other>) first` | `-d` on the default branch |
| `branch is not fully merged` | `-d` where `-D` is needed |
| `invalid branch name` | see the name rules |

## dolt_checkout

| Form | Effect |
|---|---|
| `branch` | Switch this connection to `branch` and load its working set. A name that exists only as `remotes/origin/<name>` creates the local branch. |
| `-b name [, rev]` | Create and switch. `rev` may come first or last. |
| `table` | Discard working changes to one table, back to `HEAD` |

Every branch has its own working set, so switching never carries or refuses
uncommitted work. Checking out a tag or commit is refused:
`dolt does not support a detached head state ...`; open the database by
path instead (`db/v1.0`). Other errors: `branch name required`,
`no such branch or table: <x>`, `start point not found`.

## Other functions

| Function | Meaning |
|---|---|
| `active_branch()` | Current branch, or `NULL` on a detached open |
| `dolt_default_branch()` | The branch a bare open lands on. `dolt_default_branch(name)` changes it; `branch '<x>' not found` otherwise. |
| `dolt_connect_branch(name)` | The primitive behind `db@branch`: switch to an existing local branch, with none of `dolt_checkout`'s create-from-remote or `-b` behaviour |

## dolt_branches

| Column | Meaning |
|---|---|
| `name`, `hash` | branch and its tip |
| `latest_committer`, `latest_committer_email`, `latest_commit_date`, `latest_commit_message` | tip commit metadata |
| `remote`, `branch` | upstream tracking, when set |
| `dirty` | `1` when the branch's working set has uncommitted changes |

`dolt_remote_branches` lists `remotes/<remote>/<branch>` rows with the same
tip columns.

## See also

[refs.md](refs.md), [concurrency.md](concurrency.md),
`test/vc_oracle_branch_test.sh`, `test/vc_oracle_checkout_test.sh`.
