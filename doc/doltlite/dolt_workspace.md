# dolt_workspace_<table>

Row-level view of a table's uncommitted changes, with a `staged` switch per
row. Dolt: [dolt_workspace_<table>](https://docs.dolthub.com/sql-reference/version-control/dolt-system-tables#dolt_workspace_tablename).

## Synopsis

```sql
SELECT id, staged, diff_type, to_id, to_rating, from_rating
  FROM dolt_workspace_ratings;

UPDATE dolt_workspace_ratings SET staged = 1 WHERE to_confidence > from_confidence;
UPDATE dolt_workspace_ratings SET staged = 0 WHERE id = 3;
DELETE FROM dolt_workspace_ratings WHERE staged = 0;   -- discard unstaged edits
SELECT dolt_commit('-m', 'accept the good ones');
```

## Columns

| Column | Meaning |
|---|---|
| `id` | Row ordinal within the workspace, starting at 1. Not the table's key. |
| `staged` | `1` if this row change is staged |
| `diff_type` | `added`, `modified`, `removed` |
| `to_<col>...` | Working or staged values; NULL for `removed` |
| `from_<col>...` | `HEAD` values; NULL for `added` |

## Behaviour

- `UPDATE ... SET staged = 1 | 0` stages or unstages individual row changes.
  `dolt_add` stages the whole table; this is the finer tool.
- `DELETE` discards unstaged row changes, returning those rows to `HEAD`.
  Staged rows must be unstaged first: `cannot delete staged rows from
  workspace`.
- Writes to any other column are ignored. Edit the table itself.
- Works for tables with any primary key shape, including none.

## See also

[dolt_commit.md](dolt_commit.md), [dolt_diff.md](dolt_diff.md),
`test/vc_oracle_workspace_test.sh`.
