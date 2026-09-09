# dolt_rebase

Replay the current branch's commits onto another branch. Dolt:
[dolt_rebase](https://docs.dolthub.com/sql-reference/version-control/dolt-sql-procedures#dolt_rebase).

## Synopsis

```sql
SELECT dolt_rebase('main');
SELECT dolt_rebase('-i', 'main');
UPDATE dolt_rebase SET action = 'squash' WHERE commit_message = 'fixup';
SELECT dolt_rebase('--continue');
SELECT dolt_rebase('--abort');
```

## Options

| Option | Meaning |
|---|---|
| `upstream` | Non-interactive: replay every commit not on `upstream` and move the branch |
| `-i`, `--interactive upstream` | Start a rebase and populate the `dolt_rebase` plan table; nothing is replayed until `--continue` |
| `--continue` | Apply the plan |
| `--abort` | Discard the plan and return to the original branch |

Returns `Successfully rebased and updated refs/heads/<branch>`, or `0` when
there is nothing to replay. A completed rebase ends the enclosing SQL
transaction like `dolt_commit`.

## The plan table

`dolt_rebase(rebase_order REAL PRIMARY KEY, action TEXT, commit_hash TEXT,
commit_message TEXT)` exists only during an interactive rebase. Edit it with
ordinary SQL: `action` is `pick`, `drop`, `reword`, `squash`, or `fixup`;
change `commit_message` to reword, `rebase_order` to reorder. The first
non-drop action must be `pick` or `reword`. While the plan is open the
connection sits on a working branch named `dolt_rebase_<branch>`.

## Behaviour

The rebase is atomic: a conflict or error restores the original branch and
reports `conflict rebasing "<message>"; rebase aborted, branch restored to
pre-rebase state`. The working set must be clean to start.

| Error | Cause |
|---|---|
| `usage: dolt_rebase('upstream_branch')` | no arguments |
| `cannot start a rebase with uncommitted changes` | dirty working set |
| `rebase already in progress; use --continue or --abort` | second `-i` |
| `no rebase in progress` | `--continue`/`--abort` with no plan |
| `first non-drop action must be pick or reword` | invalid plan |
| `rebase aborted due to changes in the source branch` | upstream moved during the rebase |

## Differences from Dolt

None intended. The 63-byte branch-name limit for interactive rebase
(`current branch name exceeds the 63-byte persisted-state limit`) is a
DoltLite storage limit.

## See also

[dolt_merge.md](dolt_merge.md), [dolt_cherry_pick.md](dolt_cherry_pick.md),
`test/vc_oracle_rebase_test.sh`.
