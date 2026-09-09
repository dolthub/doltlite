# Versioned system tables: dolt_ignore, dolt_docs, dolt_tests

Three tables that live in the repository and version with it. Each is served
empty before it exists; the first write creates the real table, which then
commits, diffs, branches, and merges like any other. Dolt:
[dolt_ignore](https://docs.dolthub.com/sql-reference/version-control/dolt-system-tables#dolt_ignore),
[dolt_docs](https://docs.dolthub.com/sql-reference/version-control/dolt-system-tables#dolt_docs),
[dolt_tests](https://docs.dolthub.com/sql-reference/version-control/dolt-system-tables#dolt_tests).

## dolt_ignore

`dolt_ignore(pattern TEXT NOT NULL, ignored TINYINT NOT NULL, PRIMARY KEY(pattern))`

```sql
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
INSERT INTO dolt_ignore VALUES ('tmp_keep', 0);     -- exception
```

Matching tables are skipped by `dolt_add('-A')` and `dolt_commit('-A')`,
hidden from `dolt_status`, and left in the working set. `*` or `%` match any
run, `?` one character. The most specific matching pattern wins; two patterns
of equal specificity that disagree error with `the table <t> matches
conflicting patterns in dolt_ignore`. `dolt_add('-f', table)` stages an
ignored table anyway.

## dolt_docs

`dolt_docs(doc_name TEXT NOT NULL, doc_text TEXT NOT NULL, PRIMARY KEY(doc_name))`

```sql
SELECT * FROM dolt_docs;                              -- AGENT.md on a fresh database
REPLACE INTO dolt_docs VALUES ('README.md', '# my project');
```

A fresh database serves one default row, `AGENT.md`, DoltLite's guide for
AI agents (see [agents.md](agents.md)). It is stored on first write like any
other row, so deleting it sticks. `doc_name` is unique: a second `INSERT` of
the same name fails; use `REPLACE`.

## dolt_tests and dolt_test_run

`dolt_tests(test_name, test_group, test_query, assertion_type,
assertion_comparator, assertion_value)`

```sql
INSERT INTO dolt_tests VALUES
  ('user count', 'users', 'SELECT * FROM users', 'expected_rows', '==', '10');
SELECT * FROM dolt_test_run();          -- every test
SELECT * FROM dolt_test_run('users');   -- one group, or one test name
```

| Column | Values |
|---|---|
| `assertion_type` | `expected_rows`, `expected_columns`, `expected_single_value` (enforced by a CHECK constraint) |
| `assertion_comparator` | `==`, `!=`, `<`, `>`, `<=`, `>=` |
| `assertion_value` | compared as integer, float, text, or NULL as appropriate |

`dolt_test_run` returns `test_name`, `test_group_name`, `query`, `status`
(`PASS`/`FAIL`), `message`. Queries run read-only: a write fails with
`Cannot execute write queries`, a `PRAGMA` with `Cannot execute PRAGMA
queries`, and only one statement is allowed. An unknown group or name:
`could not find tests for argument: <x>`.

## Differences from Dolt

`dolt_docs` rows are stored, including the default; Dolt resurrects its
defaults on read. The tables appear in `.tables` once they exist. The default
`AGENT.md` text is DoltLite's own.

## See also

`test/vc_oracle_ignore_test.sh`, `test/vc_oracle_docs_test.sh`,
`test/vc_oracle_tests_test.sh`.
