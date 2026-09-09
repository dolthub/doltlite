# dolt_schemas and the versioned catalog

Views and triggers are part of the branch, and `sqlite_schema` is a
projection of the versioned catalog. Dolt:
[dolt_schemas](https://docs.dolthub.com/sql-reference/version-control/dolt-system-tables#dolt_schemas).

## Synopsis

```sql
CREATE VIEW active_users AS SELECT * FROM users WHERE active = 1;
CREATE TRIGGER audit AFTER UPDATE ON users BEGIN INSERT INTO log VALUES (new.id); END;
SELECT dolt_commit('-Am', 'view and trigger');
SELECT type, name, fragment FROM dolt_schemas;
SELECT * FROM dolt_schema_diff('v1.0', 'HEAD');
```

## dolt_schemas

| Column | Meaning |
|---|---|
| `type` | `view` or `trigger` |
| `name` | object name |
| `fragment` | the `CREATE` statement |
| `extra`, `sql_mode` | present for Dolt shape compatibility; empty |

Tables and indexes are not listed here; `sqlite_schema` has everything.

## Behaviour

- Schema objects switch with `dolt_checkout`, diff with
  `dolt_schema_diff` and `dolt_patch`, and merge. Two branches adding
  different views merge cleanly; the same name with different text is a
  schema conflict.
- After a schema commit, `sqlite_schema.sql` holds the canonical `CREATE`
  text (normalized whitespace and quoting) in catalog order, not what you
  typed. `.schema` and `.dump` print that text. `CHECK` constraint error
  messages use the canonical form.
- `sqlite_schema` cannot be written, even under `PRAGMA writable_schema`.
- `ANALYZE` output in `sqlite_stat1` is an ordinary versioned table.

## Differences from Dolt

`dolt_schemas` covers views and triggers only; Dolt also stores events and
procedures, which SQLite lacks.

## See also

[dolt_diff.md](dolt_diff.md), [sqlite-compatibility.md](sqlite-compatibility.md),
`test/vc_oracle_schemas_test.sh`, `test/vc_oracle_triggers_test.sh`.
