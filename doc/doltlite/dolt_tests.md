# dolt_tests and dolt_test_run

SQL assertions stored in the database and versioned with it. Dolt:
[dolt_tests](https://docs.dolthub.com/sql-reference/version-control/dolt-system-tables#dolt_tests),
[dolt_test_run](https://docs.dolthub.com/sql-reference/version-control/dolt-sql-procedures#dolt_test_run).

## Synopsis

```sql
INSERT INTO dolt_tests VALUES
  ('user count', 'users', 'SELECT * FROM users', 'expected_rows', '==', '3');
INSERT INTO dolt_tests VALUES
  ('has email', 'users', 'SELECT count(*) FROM users WHERE email IS NULL', 'expected_single_value', '==', '0');
SELECT * FROM dolt_test_run();              -- every test
SELECT * FROM dolt_test_run('users');       -- one group
SELECT * FROM dolt_test_run('user count');  -- one test
SELECT dolt_commit('-A', '-m', 'add tests');
```

## Schema

`dolt_tests(test_name, test_group, test_query, assertion_type,
assertion_comparator, assertion_value)`

| Column | Values |
|---|---|
| `assertion_type` | `expected_rows`, `expected_columns`, `expected_single_value`; anything else fails a `CHECK` constraint |
| `assertion_comparator` | `==`, `!=`, `<`, `>`, `<=`, `>=` |
| `assertion_value` | compared as integer, float, text, or NULL as the query result dictates |

Served empty before it exists; the first write creates the real table.

## dolt_test_run

Returns `test_name`, `test_group_name`, `query`, `status` (`PASS` or `FAIL`),
`message`. With no argument or `'*'` it runs everything; otherwise the
argument names a group or a single test.

Queries run read-only and one statement at a time: a write fails with
`Cannot execute write queries`, a `PRAGMA` with `Cannot execute PRAGMA
queries`, and `expected_single_value` requires exactly one cell.

| Error | Cause |
|---|---|
| `could not find tests for argument: <x>` | no group or test by that name |
| `dolt_test_run requires literal arguments` | argument is not a literal |

## Differences from Dolt

None intended.

## See also

[dolt_ignore.md](dolt_ignore.md), [dolt_docs.md](dolt_docs.md),
`test/vc_oracle_tests_test.sh`.
