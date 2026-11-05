# dolt_cherry_pick and dolt_revert

Apply one commit's changes, or their inverse, as a new commit on the current
branch. Dolt:
[dolt_cherry_pick](https://docs.dolthub.com/sql-reference/version-control/dolt-sql-procedures#dolt_cherry-pick),
[dolt_revert](https://docs.dolthub.com/sql-reference/version-control/dolt-sql-procedures#dolt_revert).

## Synopsis

```sql
SELECT dolt_cherry_pick('-x', '0123abcd...');     -- -x records the source hash
SELECT dolt_revert('HEAD');
SELECT dolt_revert('HEAD', '--author', 'Ann <ann@example.com>');
BEGIN;
SELECT dolt_cherry_pick(dolt_hashof('feature'));   -- error: conflicts, inspect dolt_conflicts
SELECT dolt_cherry_pick('--abort');
ROLLBACK;
```

## dolt_cherry_pick

| Argument | Meaning |
|---|---|
| `commit` | One commit [revision](refs.md). Its parent-to-commit diff is applied as a three-way merge. |
| `-x` | Append `(cherry picked from commit <hash>)` to the message, naming the source commit |
| `--abort` | Drop an in-progress conflicted cherry-pick |

Returns the new commit hash, keeping the original message (plus the `-x`
trailer when given). Conflicts behave
as in [dolt_merge.md](dolt_merge.md): rolled back in autocommit, inspectable
inside `BEGIN`. A completed cherry-pick commits the enclosing SQL
transaction and begins a new one, like `dolt_commit`.

| Error | Cause |
|---|---|
| `usage: dolt_cherry_pick('commit_hash')` | no argument |
| `invalid commit hash` | does not resolve |
| `cherry-picking a merge commit is not supported` | two-parent commit |
| `cherry-picking multiple commits is not supported yet.` | more than one commit |
| `cannot cherry-pick with uncommitted changes` | dirty working set |
| `no cherry-pick in progress` | `--abort` with nothing to abort |

## dolt_revert

`dolt_revert(commit)` commits the inverse of `commit` onto `HEAD` and returns
the new hash. The message follows git's default and names the reverted commit:

```
Revert "<original message>"

This reverts commit <hash>.
```

Several commits, or a range such as `'HEAD~2..HEAD'`, revert as one commit
each. The initial commit cannot be reverted. Conflicts behave
like a merge.

| Argument | Meaning |
|---|---|
| `commit` | One or more commit [revisions](refs.md), or a range, to invert onto `HEAD` |
| `--author 'Name <email>'` | Override the committer for the revert commit |

| Error | Cause |
|---|---|
| `Your local changes would be overwritten by revert.` (with a hint to commit first) | dirty working set |
| `invalid commit hash` | does not resolve |
| `Author not formatted correctly. Use 'Name <author@example.com>' format` | bad `--author` |

## Differences from Dolt

Dolt's `dolt_cherry_pick` likewise takes one commit. Dolt's revert message is
`Revert "<original message>"` with no trailer, and Dolt has no `-x`; the
source-commit trailers are DoltLite additions, following git.

## See also

`test/vc_oracle_revert_cherrypick_test.sh`,
`test/vc_oracle_cherry_pick_abort_test.sh`.
