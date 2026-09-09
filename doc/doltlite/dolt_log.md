# History: dolt_log, dolt_history, dolt_at, dolt_blame

Commit history, and every table's past. Dolt:
[dolt_log](https://docs.dolthub.com/sql-reference/version-control/dolt-system-tables#dolt_log),
[dolt_history_<table>](https://docs.dolthub.com/sql-reference/version-control/dolt-system-tables#dolt_history_tablename),
[dolt_blame_<table>](https://docs.dolthub.com/sql-reference/version-control/dolt-system-tables#dolt_blame_tablename).

## Synopsis

```sql
SELECT * FROM dolt_log;                         -- current branch
SELECT * FROM dolt_log('feature');
SELECT * FROM dolt_log('main..feature');        -- on feature, not on main
SELECT * FROM dolt_history_users WHERE id = 42;
SELECT * FROM dolt_history_users('v1.0');
SELECT * FROM dolt_at_users('HEAD~3');          -- the table as it was
SELECT * FROM dolt_blame_users;
SELECT * FROM dolt_commit_ancestors WHERE commit_hash = dolt_hashof('HEAD');
```

## Surfaces

| Surface | Argument | Columns |
|---|---|---|
| `dolt_log` | none, one commit revision, or a range | `commit_hash`, `committer`, `email`, `date`, `message` |
| `dolt_history_<table>` | none or one commit revision | user columns, `commit_hash`, `committer`, `commit_date` |
| `dolt_at_<table>` | one revision, `WORKING`/`STAGED` allowed | user columns |
| `dolt_blame_<table>` | none | primary-key columns, `commit`, `commit_date`, `committer`, `email`, `message` |
| `dolt_commit_ancestors` | none | `commit_hash`, `parent_hash`, `parent_index` |

## Behaviour

- `dolt_log` walks from the given revision to the root, newest first. Ranges
  follow git: `a..b` is reachable from `b` not `a`; `a...b` is either side but
  not both. One argument only.
- `dolt_history_<table>` yields one row per version of each row in the
  ancestry of its starting point. Filter by `commit_hash` for one snapshot.
- `dolt_at_<table>` is a plain read of one snapshot; join it against the live
  table to compare.
- `dolt_blame_<table>` walks first parents from `HEAD` and reports the commit
  that last set each live row's values. Schema-only changes do not move blame.
  The table needs a primary key: `dolt_blame_<t>: table has no primary key`.
- `dolt_commit_ancestors` has one row per parent; a merge commit has
  `parent_index` 0 and 1, and a root has one row with a NULL parent.

| Error | Cause |
|---|---|
| `invalid dolt_log revision: <x>` | unknown revision, `WORKING`, or a malformed range |
| `ref not found: <x>` | `dolt_history_<t>` / `dolt_at_<t>` given an unknown revision |
| `no such table: <name>` | `dolt_at_<x>` where `x` is not a user table |

## Differences from Dolt

`dolt_log` walks selected ancestry; `dolt_commit_ancestors` exposes the graph
across refs. There is no separate flat `dolt_commits` table. Column names are
the DoltLite set above.

## See also

[refs.md](refs.md), [dolt_diff.md](dolt_diff.md),
`test/vc_oracle_log_test.sh`, `test/vc_oracle_history_test.sh`,
`test/vc_oracle_blame_test.sh`, `test/vc_oracle_at_test.sh`.
