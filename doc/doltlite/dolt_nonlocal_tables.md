# dolt_nonlocal_tables

Table names on this branch that resolve to a table on another branch or ref.
Use it for data every branch should share, such as users or settings, while
the rest of the schema stays versioned per branch. Dolt:
[nonlocal tables](https://www.dolthub.com/blog/2025-10-06-nonlocal-tables/).

## Synopsis

```sql
SELECT dolt_checkout('-b', 'experiment');
INSERT INTO dolt_nonlocal_tables VALUES ('users', 'main', NULL, 'immediate');
INSERT INTO users(name) VALUES ('shared');   -- written to main's working set
SELECT dolt_checkout('main');
SELECT count(*) FROM users WHERE name = 'shared';   -- 1
```

## Schema

`dolt_nonlocal_tables(table_name TEXT NOT NULL, target_ref TEXT, ref_table TEXT, options TEXT, PRIMARY KEY(table_name))`

The table is served empty before it exists; the first write creates it, and
from then on it commits, diffs, branches, and merges like any other table. A
rule applies on the branch whose working set holds it.

| Column | Meaning |
|---|---|
| `table_name` | Name or pattern this branch redirects. `*` or `%` match any run of characters, `?` exactly one. |
| `target_ref` | Branch, tag, or commit the name resolves to. Empty or `NULL` means the current branch. |
| `ref_table` | Table on the target. Empty or `NULL` means the same name. |
| `options` | Must be `immediate`. |

## Behaviour

- Names are matched case-insensitively against the rules in `table_name`
  order; the first match wins, and it wins over a local table of the same
  name. `dolt_*` and `sqlite_*` names are never redirected.
- A branch target is that branch's working set: reads see its uncommitted
  rows and writes land there, uncommitted, for that branch to commit. A tag
  or commit target is read-only.
- A transaction that writes a nonlocal table and a local one commits both as
  one root, and `ROLLBACK` undoes both (see
  [concurrency.md](concurrency.md#several-branches-in-one-transaction)).
  Version-control commands are refused until that transaction commits or
  rolls back.
- `dolt_clean` never drops a table a rule claims.
- The connection attaches each target branch as a hidden database named
  `dolt_nonlocal:<ref>` while a rule names it. It appears in
  `PRAGMA database_list`; an unqualified name never resolves into it except
  through a rule.

## Errors

| Statement | Error |
|---|---|
| `CREATE TABLE` of a claimed name, other than the rule's own backing table on its target branch | `Cannot create table name <name> because it matches a name present in dolt_nonlocal_tables.` |
| Reading a table whose rule has another `options` value | `Invalid nonlocal table options <options>: only valid value is 'immediate'.` |
| Writing a table whose target is a tag or commit | `attempt to write a readonly database` |

`CREATE TABLE IF NOT EXISTS` of a claimed name that already resolves is a
no-op, as in Dolt.

## Differences from Dolt

- Nonlocal tables are not listed in `sqlite_master` or `.tables`; Dolt shows
  them in `SHOW TABLES`.
- A foreign key that references a claimed name is checked against this
  branch's own table of that name, if any, not the nonlocal table. Dolt checks
  the nonlocal table. Views and triggers resolve claimed names like any other
  statement.
- `dolt_commit_all` and `@@dolt_multi_branch_commit` are not implemented, so
  commit each branch's working set from that branch.
- A branch that a rule targets cannot be deleted or renamed while this
  connection has it attached.

## See also

[concurrency.md](concurrency.md), [dolt_ignore.md](dolt_ignore.md),
`test/vc_oracle_nonlocal_test.sh`, `test/doltlite_nonlocal_tables.test`.
