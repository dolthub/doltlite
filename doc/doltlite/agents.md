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
- `dolt_commit` ends the enclosing SQL transaction, so call it last. A merge
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
# AGENT.md - DoltLite Database Operations Guide

This is a DoltLite database: SQLite-compatible SQL with Dolt-style
version control (commits, branches, diffs, merges) built in.
Version control operations are SQL function calls, not stored
procedures: use `SELECT dolt_commit(...)`, never `CALL dolt_commit(...)`.

## Core Workflow

```sql
SELECT dolt_add('-A');                    -- stage all changes
SELECT dolt_commit('-m', 'message');      -- commit staged changes
SELECT dolt_commit('-A', '-m', 'message');-- stage and commit at once
SELECT * FROM dolt_status;                -- what is staged / modified
SELECT * FROM dolt_log;                   -- commit history
```

## Branches

```sql
SELECT dolt_branch('feature');            -- create
SELECT dolt_checkout('feature');          -- switch
SELECT dolt_checkout('-b', 'feature2');   -- create and switch
SELECT active_branch();
SELECT * FROM dolt_branches;
SELECT dolt_merge('feature');
```

Each branch has its own working state; uncommitted changes are
per-branch.

## Remotes

```sql
SELECT dolt_remote('add', 'origin', 'file:///path/to/remote.db');
SELECT dolt_push('origin', 'main');
SELECT dolt_push('origin', 'v1');
SELECT dolt_push('origin', '--tags');
SELECT dolt_pull('origin', 'main');
SELECT dolt_clone('file:///path/to/source.db');
```

## Diffs and History

```sql
SELECT * FROM dolt_diff;                     -- tables changed per commit
SELECT * FROM dolt_diff_stat('v1', 'HEAD');  -- row/cell counts
SELECT * FROM dolt_patch('v1', 'v2');        -- executable SQL statements
-- Per user table <t>: dolt_diff_<t>, dolt_history_<t>, dolt_workspace_<t>
```

## Merge Conflicts

A merge that hits conflicts in autocommit mode rolls back. Run it inside
an explicit transaction to inspect and resolve:

```sql
BEGIN;
SELECT dolt_merge('feature');
SELECT * FROM dolt_conflicts;              -- summary per table
SELECT * FROM dolt_conflicts_<t>;          -- base/ours/theirs rows
SELECT dolt_conflicts_resolve('--ours', '<t>');
COMMIT;
SELECT dolt_commit('-m', 'merged');
```

Constraint violations may persist after merges. Inspect
`dolt_constraint_violations` and `dolt_constraint_violations_<t>`, then
run `SELECT dolt_verify_constraints('--all')` after repairs.

## Undoing Changes

```sql
SELECT dolt_reset('--hard');               -- discard working changes
SELECT dolt_reset('--hard', 'HEAD~1');     -- move HEAD back one commit
SELECT dolt_revert('HEAD');                -- new commit undoing HEAD
```

## Notes

- `dolt_docs` (this table) stores versioned documents keyed by name;
  `dolt_ignore` holds patterns for tables `dolt_add` should skip. Both
  commit, diff, branch and merge like ordinary tables.
- Beta storage format version 12 is not SQLite's page format; stock SQLite
  cannot open it, and no SQLite journal, `-wal`, or `-shm` sidecars exist.
- ATTACH works, but one transaction may write only one file-backed database.
- Most SQLite SQL features remain available, including triggers, views,
  FTS5, and R-Tree. For storage-coupled API and PRAGMA differences, see
  https://github.com/dolthub/doltlite/blob/master/doc/doltlite/sqlite-compatibility.md
  and pragmas.md alongside it.
- Full reference, one page per feature:
  https://github.com/dolthub/doltlite/tree/master/doc/doltlite
  Start with refs.md (revision syntax), transactions.md (what ROLLBACK
  undoes; dolt_commit ends the SQL transaction), and dolt-differences.md
  if you already know Dolt.
````
