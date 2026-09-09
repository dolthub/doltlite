# dolt_reset and dolt_clean

Undo staging or working changes on the current branch. Dolt:
[dolt_reset](https://docs.dolthub.com/sql-reference/version-control/dolt-sql-procedures#dolt_reset),
[dolt_clean](https://docs.dolthub.com/sql-reference/version-control/dolt-sql-procedures#dolt_clean).

## Synopsis

```sql
SELECT dolt_reset();                    -- unstage everything
SELECT dolt_reset('users');             -- unstage one table
SELECT dolt_reset('--soft');            -- same as no arguments
SELECT dolt_reset('--hard');            -- discard staged and working changes
SELECT dolt_reset('--hard', 'HEAD~1');  -- also move the branch to a commit
SELECT dolt_clean('--dry-run');
SELECT dolt_clean('tmp_table');
SELECT dolt_clean();
```

## dolt_reset

| Form | Effect |
|---|---|
| no arguments, `--soft` | Unstage all tables; working changes stay |
| `table`, ... | Unstage those tables |
| `--hard` | Working set and staging back to `HEAD` |
| `--hard`, `rev` | Move the branch to `rev` (any [revision](refs.md)), then reset hard |

Returns `0`. `--hard` is not undone by `ROLLBACK`; see
[transactions.md](transactions.md).

| Error | Cause |
|---|---|
| `--hard and --soft are mutually exclusive options.` | both flags |
| `commit not found` | `rev` does not resolve |
| `table paths cannot be combined with --hard / --soft or a target ref` | mixing forms |
| `reset conflict: another connection moved this branch. Please retry your transaction.` | peer advanced the branch during the reset |

## dolt_clean

Drops untracked tables: those with status `new table` that have never been
committed.

| Argument | Meaning |
|---|---|
| none | Drop every untracked table |
| `table`, ... | Drop only those; an unknown name errors with `failed to clean; table not found: '<name>'` |
| `--dry-run` | Do nothing and return `0`; use `dolt_status` to see what would go |

## Differences from Dolt

`dolt_reset('--hard')` inside `BEGIN` takes effect immediately rather than
with the transaction.

## See also

[dolt_commit.md](dolt_commit.md), [dolt_workspace.md](dolt_workspace.md),
`test/vc_oracle_reset_test.sh`, `test/vc_oracle_clean_test.sh`.
