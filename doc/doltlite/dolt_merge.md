# dolt_merge and conflicts

Three-way, row-level merge into the current branch. Dolt:
[dolt_merge](https://docs.dolthub.com/sql-reference/version-control/dolt-sql-procedures#dolt_merge),
[dolt_conflicts](https://docs.dolthub.com/sql-reference/version-control/dolt-system-tables#dolt_conflicts),
[dolt_conflicts_resolve](https://docs.dolthub.com/sql-reference/version-control/dolt-sql-procedures#dolt_conflicts_resolve).

## Synopsis

```sql
SELECT dolt_merge('topic');                       -- fast-forward when possible
SELECT dolt_merge('--no-ff', '-m', 'Merge topic', 'topic');
SELECT dolt_merge('--squash', 'topic');
SELECT dolt_merge('--no-commit', 'topic');
BEGIN;
SELECT dolt_merge('feature');                     -- error: conflicts on users
SELECT * FROM dolt_merge_status;
SELECT * FROM dolt_conflicts;
SELECT * FROM dolt_conflicts_users;
SELECT dolt_merge('--abort');
SELECT dolt_merge('feature');                     -- error: same conflicts again
SELECT dolt_conflicts_resolve('--theirs', 'users');
SELECT dolt_commit('-m', 'Merge feature');
SELECT dolt_merge_base('main', 'feature');
```

## dolt_merge

| Option | Meaning |
|---|---|
| `branch` | One commit [revision](refs.md) to merge in |
| `--no-ff` | Always create a merge commit, even when fast-forward is possible |
| `--squash` | Fast-forward: stage without moving `HEAD`. Three-way: make a one-parent commit; add `--no-commit` to only stage. |
| `--no-commit` | Three-way: stage and leave `is_merging = 1`, or `0` with `--squash`. A possible fast-forward still advances `HEAD`. |
| `-m`, `--message` | Message for the merge commit (default `Merge branch 'x' into y`) |
| `--abort` | Drop an in-progress merge and restore the pre-merge working set |

Committed merges return the new tip hash. A fast-forward returns the merged
branch's tip; an uncommitted three-way merge returns `0`.
`--squash` and `--no-ff` together: `flags '--squash' and '--no-ff' cannot
be used together`. Other errors: `usage: dolt_merge('branch')`,
`merge source not found`, `no merge in progress`.

Non-conflicting edits to different rows, or the same row on one side only,
merge. Both sides editing the same row differently is a conflict.

## Conflicts

A conflicting merge does not persist. In autocommit it is rolled back with
`cannot merge: conflicts detected, autocommit transaction rolled back ...`.
Inside `BEGIN` the error is `Merge has N conflict(s). Resolve and then
commit with dolt_commit.` and the transaction stays open with:

| Table | Columns |
|---|---|
| `dolt_conflicts` | `table`, `num_conflicts` |
| `dolt_conflicts_<table>` | `from_root_ish`, `base_<col>...`, `our_<col>...`, `our_diff_type`, `their_<col>...`, `their_diff_type`, `dolt_conflict_id` |
| `dolt_schema_conflicts` | `table_name`, `base_schema`, `our_schema`, `their_schema`, `description` |
| `dolt_merge_status` | `is_merging`, `source`, `source_commit`, `target`, `unmerged_tables` (one row, `0` and NULLs when idle) |

Resolve with `dolt_conflicts_resolve('--ours' | '--theirs', table, ...)`, or
`DELETE FROM dolt_conflicts_<table> WHERE dolt_conflict_id = ...` to keep the
working value (only `DELETE` is supported on conflict tables). One call can
resolve several named tables; a missing table name rejects the call before
any table is resolved. Existing tables without conflicts are ignored. Then
`dolt_commit` records the merge commit. `COMMIT` with conflicts left fails
with `constraint failed`; `dolt_commit` with `cannot commit: unresolved merge
conflicts. Use dolt_conflicts_resolve() first.` Schema conflicts cannot be
auto-resolved: abort, align the schemas on one side, merge again.

## Refusals

<!-- contract: merge.validation_error_restores -->
If applying a merged schema change fails, the refusal includes SQLite's
underlying error. If a foreign-key check cannot run, for example because a
referenced table was dropped, the refusal names the missing table and restores
the pre-merge working set, including when the database is reopened.

A merge that cannot produce a correct result fails instead of guessing: a
table whose primary key changed on either side, or a derived index (a
virtual-table shadow) the engine cannot rebuild from merged content.
Inside a plain `BEGIN`, constraint violations are recorded and preserved for
inspection; autocommit rolls the merge back. See
[dolt_constraint_violations.md](dolt_constraint_violations.md).

## dolt_merge_base

`dolt_merge_base(a, b)` returns the common ancestor's hash. Exactly two
commit revisions; `WORKING`/`STAGED` are rejected with `could not resolve
second argument to a commit`.

## Differences from Dolt

Conflicts are never committable (Dolt allows it behind
`@@dolt_allow_commit_conflicts`). A completed merge ends the enclosing SQL
transaction like `dolt_commit`.

## See also

[transactions.md](transactions.md), [dolt_cherry_pick.md](dolt_cherry_pick.md),
`test/vc_oracle_merge_test.sh`, `test/vc_oracle_conflicts_test.sh`.
