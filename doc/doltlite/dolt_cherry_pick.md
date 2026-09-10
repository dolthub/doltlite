# dolt_cherry_pick and dolt_revert

Apply one commit's changes, or their inverse, as a new commit on the current
branch. Dolt:
[dolt_cherry_pick](https://docs.dolthub.com/sql-reference/version-control/dolt-sql-procedures#dolt_cherry-pick),
[dolt_revert](https://docs.dolthub.com/sql-reference/version-control/dolt-sql-procedures#dolt_revert).

## Synopsis

```sql
SELECT dolt_cherry_pick('0123abcd...');
SELECT dolt_revert('HEAD');
BEGIN;
SELECT dolt_cherry_pick(dolt_hashof('feature'));   -- error: conflicts, inspect dolt_conflicts
SELECT dolt_cherry_pick('--abort');
ROLLBACK;
```

## dolt_cherry_pick

| Argument | Meaning |
|---|---|
| `commit` | One commit [revision](refs.md). Its parent-to-commit diff is applied as a three-way merge. |
| `--abort` | Drop an in-progress conflicted cherry-pick |

Returns the new commit hash, keeping the original message. Conflicts behave
as in [dolt_merge.md](dolt_merge.md): rolled back in autocommit, inspectable
inside `BEGIN`. A completed cherry-pick ends the enclosing SQL transaction.

| Error | Cause |
|---|---|
| `usage: dolt_cherry_pick('commit_hash')` | no argument |
| `invalid commit hash` | does not resolve |
| `cherry-picking a merge commit is not supported` | two-parent commit |
| `cherry-picking multiple commits is not supported yet.` | more than one commit |
| `cannot cherry-pick with uncommitted changes` | dirty working set |
| `no cherry-pick in progress` | `--abort` with nothing to abort |

## dolt_revert

`dolt_revert(commit)` commits the inverse of `commit` onto `HEAD` with the
message `Revert "<original message>"` and returns the new hash. One commit at
a time; the initial commit cannot be reverted. Conflicts behave like a merge.

| Error | Cause |
|---|---|
| `Your local changes would be overwritten by revert.` (with a hint to commit first) | dirty working set |
| `invalid commit hash` | does not resolve |

## Differences from Dolt

Dolt accepts several commits per call and revert ranges; DoltLite takes one.

## See also

`test/vc_oracle_revert_cherrypick_test.sh`,
`test/vc_oracle_cherry_pick_abort_test.sh`.
