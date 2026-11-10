# DoltLite for AI agents

Every DoltLite database carries its own operations guide. Before generating
SQL against an unfamiliar database, read it:

```sql
SELECT doc_text FROM dolt_docs WHERE doc_name = 'AGENT.md';
```

A fresh database serves DoltLite's default text: the commit loop, branches,
remotes, diffs, conflict handling inside `BEGIN`, and the SQLite differences
that matter. It is an ordinary versioned row. A project replaces it with its
own conventions and the replacement commits, branches, and merges with the
data:

```sql
REPLACE INTO dolt_docs VALUES ('AGENT.md', '# Project rules ...');
SELECT dolt_commit('-A', '-m', 'agent guide');
```

Deleting the row sticks; DoltLite does not resurrect the default.

## Rules the default guide leans on

- Version control is SQL functions and tables: `SELECT dolt_commit(...)`,
  never `CALL`. See [dolt-differences.md](dolt-differences.md).
- `dolt_commit` commits the enclosing SQL transaction and begins a new one,
  so `COMMIT` what you write after it. A merge
  that conflicts in autocommit mode is rolled back; run merges inside
  `BEGIN` to inspect and resolve. See [transactions.md](transactions.md).
- Every revision spelling, including `HEAD~1`, `WORKING`, and `a..b`, is in
  [refs.md](refs.md).
- Each connection has its own branch, and a branch's uncommitted work is
  shared by every connection on it. Open `db@branch` to start on a branch.
- Statements that would write fail under `PRAGMA query_only`, which is the
  simplest way to give an agent read-only access.

## Discovering the surface

```sql
SELECT name FROM pragma_function_list WHERE name LIKE 'dolt_%' ORDER BY 1;
SELECT name FROM pragma_module_list  WHERE name LIKE 'dolt_%' ORDER BY 1;
SELECT dolt_version(), doltlite_engine();
```

`doltlite_engine()` returns `prolly` for DoltLite and `orig` for stock. `dolt_*`
calls stay listed but fail.

The repository's own guidance for agents working on the DoltLite source is
[AGENTS.md](../../AGENTS.md) at the repo root; this page is about databases.

## The default text

What a fresh database serves, byte for byte. `test/oracle_doc_agent_guide_test.sh`
fails when the engine and this block drift apart.

````markdown
# DoltLite database guide

Use SQLite SQL through `doltlite database.db` or the embedded library.

```sql
SELECT * FROM dolt_status;
SELECT * FROM dolt_log LIMIT 10;
SELECT dolt_add('my_table');
SELECT dolt_commit('-m', 'Describe the change');
SELECT dolt_checkout('-b', 'feature');
```

Version control uses SQL functions and system tables. Each branch keeps its
own uncommitted work; open `database.db/feature` to use it directly.
Query `dolt_diff_my_table('HEAD', 'WORKING')` for row changes.

Run `SELECT dolt_merge('feature')` inside `BEGIN` to inspect conflicts.
Read `dolt_conflicts`, `dolt_schema_conflicts`, and
`dolt_constraint_violations`. Resolve conflicts before committing, or use
`ROLLBACK` to abandon a conflicted merge. Conflicts never persist.
Inside `BEGIN`, `dolt_commit` commits and starts a new SQL transaction;
finish later writes with `COMMIT`.

For a configured remote: `SELECT dolt_push('origin', 'feature')`.

DoltLite storage format version 12 cannot be opened by stock SQLite.
With ATTACH, one transaction may write only one file-backed database;
branches of the same file can commit together.

Reference: https://github.com/dolthub/doltlite/tree/master/doc/doltlite
````
