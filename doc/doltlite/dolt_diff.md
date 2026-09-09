# Diffs

What changed between two points in history, at table, row, schema, or
SQL-statement granularity. Dolt:
[dolt_diff](https://docs.dolthub.com/sql-reference/version-control/dolt-system-tables#dolt_diff),
[dolt_diff_stat](https://docs.dolthub.com/sql-reference/version-control/dolt-sql-functions#dolt_diff_stat),
[dolt_patch](https://docs.dolthub.com/sql-reference/version-control/dolt-sql-functions#dolt_patch).

## Synopsis

```sql
SELECT * FROM dolt_diff WHERE table_name = 'users';        -- commits that touched a table
SELECT * FROM dolt_diff_users;                              -- every row change in history
SELECT * FROM dolt_diff_users WHERE to_commit = 'WORKING';  -- uncommitted
SELECT * FROM dolt_diff_users('v1.0', 'HEAD');              -- between two revisions
SELECT * FROM dolt_diff_users('main...feature');
SELECT * FROM dolt_diff_stat('v1.0', 'HEAD');
SELECT * FROM dolt_diff_summary('v1.0', 'HEAD', 'users');
SELECT * FROM dolt_schema_diff('v1.0..HEAD');
SELECT statement FROM dolt_patch('HEAD', 'WORKING') ORDER BY statement_order;
```

Revision spellings and which surfaces take ranges: [refs.md](refs.md).

## Surfaces

| Surface | Arguments | Columns |
|---|---|---|
| `dolt_diff` | none | `commit_hash`, `committer`, `email`, `date`, `message`, `data_change`, `schema_change`, `table_name` |
| `dolt_diff_<table>` | none; or `(from, to)`; or `(range)` | `to_<col>...`, `to_commit`, `to_commit_date`, `from_<col>...`, `from_commit`, `from_commit_date`, `diff_type` |
| `dolt_diff_stat` | `(from, to [, table])` | `table_name`, `rows_unmodified`, `rows_added`, `rows_deleted`, `rows_modified`, `cells_added`, `cells_deleted`, `cells_modified`, `old_row_count`, `new_row_count`, `old_cell_count`, `new_cell_count` |
| `dolt_diff_summary` | `(from, to [, table])` | `from_table_name`, `to_table_name`, `diff_type`, `data_change`, `schema_change` |
| `dolt_schema_diff` | `(from, to [, table])` or `(range [, table])` | `from_table_name`, `to_table_name`, `from_create_statement`, `to_create_statement` |
| `dolt_patch` | `(from, to [, table])` or `(range [, table])` | `statement_order`, `from_commit_hash`, `to_commit_hash`, `table_name`, `diff_type`, `statement` |

`diff_type` is `added`, `modified`, or `removed` for rows; `added`,
`dropped`, `modified`, or `renamed` for tables in `dolt_diff_summary`;
`schema` or `data` in `dolt_patch`.

## Behaviour

- `dolt_diff_<table>` with no arguments walks the current branch's history.
  `to_commit = 'WORKING'` rows cover staged and working edits together.
- The two-argument form is a snapshot comparison and works even if the table
  exists at only one endpoint. A single revision is an error:
  `dolt_diff_<table> requires a '..' or '...' revision range`.
- `dolt_diff_stat` and `dolt_diff_summary` take no range form:
  `dolt_diff_stat requires from_ref and to_ref`.
- `dolt_patch` emits ordered, executable SQLite: `schema` statements first,
  then `data`. When `ALTER TABLE` cannot express a change it emits a rebuild.
- `dolt_schema_diff` covers tables, views, indexes, and triggers.
- A user column that collides case-insensitively with a metadata column is
  renamed with a numeric suffix (`_1`, `_2`, ...).

Unknown table: `table not found: <x>` (stat, summary) or `dolt_patch: table
'<x>' does not exist`.

## Differences from Dolt

`dolt_diff_<table>(from, to)` replaces Dolt's `dolt_commit_diff_<table>`.
Column names may differ; row semantics match.

## See also

[dolt_log.md](dolt_log.md) for `dolt_history_<table>`,
`test/vc_oracle_diff_test.sh`, `test/vc_oracle_diff_tvf_test.sh`,
`test/vc_oracle_patch_test.sh`.
