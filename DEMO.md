# Doltlite Getting Started

This walkthrough mirrors the [Dolt getting-started tutorial](https://github.com/dolthub/dolt#dolt),
adapted for Doltlite's SQLite interface. All version control is done through SQL.

## Create Schema

```
$ ./doltlite mydb.db
```

```sql
CREATE TABLE employees(id INTEGER PRIMARY KEY, last_name TEXT, first_name TEXT);
CREATE TABLE teams(id INTEGER PRIMARY KEY, team_name TEXT);
CREATE TABLE employees_teams(
    team_id INTEGER, employee_id INTEGER,
    PRIMARY KEY(team_id, employee_id)
);
```

## First Commit

Stage and commit the schema:

```sql
SELECT dolt_commit('-A', '-m', 'Created initial schema');
-- ba06540ce0b7aa54a4bdc538f659801d42e005bb
```

Check the log:

```sql
SELECT * FROM dolt_log;
-- commit_hash                              | committer | email | date       | message
-- ba06540ce0b7aa54a4bdc538f659801d42e005bb | doltlite  |       | 1773784588 | Created initial schema
```

## Populate Data

```sql
INSERT INTO employees VALUES(0, 'Sehn', 'Tim');
INSERT INTO employees VALUES(1, 'Hendriks', 'Brian');
INSERT INTO employees VALUES(2, 'Son', 'Aaron');
INSERT INTO employees VALUES(3, 'Fitzgerald', 'Brian');

INSERT INTO teams VALUES(0, 'Engineering');
INSERT INTO teams VALUES(1, 'Sales');

INSERT INTO employees_teams VALUES(0,0), (0,1), (0,2), (1,0), (1,3);
```

Check status — three tables have uncommitted changes:

```sql
SELECT * FROM dolt_status;
-- table_name      | staged | status
-- employees       | 0      | modified
-- teams           | 0      | modified
-- employees_teams | 0      | modified
```

Query the data:

```sql
SELECT first_name, last_name, team_name
FROM employees e
JOIN employees_teams et ON e.id = et.employee_id
JOIN teams t ON t.id = et.team_id
WHERE team_name = 'Engineering';
-- Tim      | Sehn     | Engineering
-- Brian    | Hendriks | Engineering
-- Aaron    | Son      | Engineering
```

Commit and tag:

```sql
SELECT dolt_commit('-A', '-m', 'Populated tables with data');
SELECT dolt_tag('v1');
```

## Branch and Modify

Create a branch and make changes:

```sql
SELECT dolt_branch('modifications');
SELECT dolt_checkout('modifications');

UPDATE employees SET first_name = 'Timothy' WHERE first_name = 'Tim';
INSERT INTO employees VALUES(4, 'Wilkins', 'Daylon');
INSERT INTO employees_teams VALUES(0, 4);
DELETE FROM employees_teams WHERE employee_id = 0 AND team_id = 1;

SELECT dolt_commit('-A', '-m', 'Modifications on a branch');
```

## Switch Back to Main

Main is unchanged:

```sql
SELECT dolt_checkout('main');

SELECT id, first_name, last_name FROM employees ORDER BY id;
-- 0 | Tim   | Sehn
-- 1 | Brian | Hendriks
-- 2 | Aaron | Son
-- 3 | Brian | Fitzgerald
```

## Point-in-Time Query (AS OF)

See another branch's data without switching:

```sql
SELECT count(*) FROM dolt_at('employees', 'modifications');
-- 5

SELECT count(*) FROM dolt_at('employees', 'v1');
-- 4
```

## Merge

```sql
SELECT dolt_merge('modifications');

SELECT id, first_name, last_name FROM employees ORDER BY id;
-- 0 | Timothy | Sehn
-- 1 | Brian   | Hendriks
-- 2 | Aaron   | Son
-- 3 | Brian   | Fitzgerald
-- 4 | Daylon  | Wilkins
```

The merge brought in Timothy (renamed from Tim), Daylon (new hire),
added Daylon to Engineering, and removed Tim from Sales.

```sql
SELECT e.first_name, e.last_name, t.team_name
FROM employees e
JOIN employees_teams et ON e.id = et.employee_id
JOIN teams t ON t.id = et.team_id
WHERE team_name = 'Sales';
-- Brian | Fitzgerald | Sales
```

## Reset (Undo)

Accidentally add a row, then undo:

```sql
INSERT INTO employees VALUES(99, 'Temp', 'Temp');
SELECT count(*) FROM employees;
-- 6

SELECT dolt_reset('--hard');
SELECT count(*) FROM employees;
-- 5
```

## Branches and Tags

```sql
SELECT * FROM dolt_branches;
-- name          | hash                                     | is_current
-- main          | 4e130d15c8858d3ab474bfdb48f3f23e358fa822 | 1
-- modifications | 15542b3e3de19d575287bd5fde8398a52cf65534 | 0

SELECT * FROM dolt_tags;
-- name | hash
-- v1   | 4e130d15c8858d3ab474bfdb48f3f23e358fa822

SELECT active_branch();
-- main
```

## Audit Log

Every change to `employees` across all commits:

```sql
SELECT diff_type, rowid_val, to_commit FROM dolt_diff_employees;
-- added    | 0 | 4e130d...
-- added    | 1 | 4e130d...
-- added    | 2 | 4e130d...
-- added    | 3 | 4e130d...
-- modified | 0 | 15542b...
-- added    | 4 | 15542b...
```

## History (Time Travel)

Every version of employee 0 across all commits:

```sql
SELECT rowid_val, commit_hash, committer FROM dolt_history_employees
WHERE rowid_val = 0;
-- 0 | d5b412... | doltlite   (after merge: Timothy)
-- 0 | 7b18c3... | doltlite   (after schema_changes merge)
-- 0 | 4e130d... | doltlite   (original: Tim)
```

## Schema Diff

Compare schemas between two points:

```sql
SELECT table_name, diff_type, to_create_stmt
FROM dolt_schema_diff(
    (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 2),
    (SELECT commit_hash FROM dolt_log LIMIT 1)
);
-- Shows tables/indexes added, dropped, or modified between commits
```

## Garbage Collection

After deleting branches, reclaim space:

```sql
SELECT dolt_branch('-d', 'modifications');
SELECT dolt_gc();
-- "5 chunks removed, 23 chunks kept"
```

## Summary

Doltlite gives you the full Dolt version control workflow in a single
SQLite-compatible database file:

| Feature | SQL |
|---------|-----|
| Commit | `SELECT dolt_commit('-A', '-m', 'msg')` |
| Branch | `SELECT dolt_branch('name')` |
| Checkout | `SELECT dolt_checkout('name')` |
| Merge | `SELECT dolt_merge('branch')` |
| Log | `SELECT * FROM dolt_log` |
| Diff | `SELECT * FROM dolt_diff('table')` |
| Status | `SELECT * FROM dolt_status` |
| Reset | `SELECT dolt_reset('--hard')` |
| Tag | `SELECT dolt_tag('v1.0')` |
| AS OF | `SELECT * FROM dolt_at('table', 'ref')` |
| Audit | `SELECT * FROM dolt_diff_employees` |
| History | `SELECT * FROM dolt_history_employees` |
| Schema Diff | `SELECT * FROM dolt_schema_diff('v1', 'v2')` |
| Cherry-Pick | `SELECT dolt_cherry_pick('hash')` |
| Revert | `SELECT dolt_revert('hash')` |
| GC | `SELECT dolt_gc()` |
| Merge Base | `SELECT dolt_merge_base('h1', 'h2')` |
| Conflicts | `DELETE FROM dolt_conflicts_table WHERE base_rowid=N` |
