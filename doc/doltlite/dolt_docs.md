# dolt_docs

Versioned documents keyed by name: a README, a license, or the `AGENT.md`
guide every fresh database carries. Dolt:
[dolt_docs](https://docs.dolthub.com/sql-reference/version-control/dolt-system-tables#dolt_docs).

## Synopsis

```sql
SELECT doc_name FROM dolt_docs;                       -- AGENT.md on a fresh database
INSERT INTO dolt_docs VALUES ('README.md', '# my project');
REPLACE INTO dolt_docs VALUES ('README.md', '# updated');
SELECT doc_text FROM dolt_docs WHERE doc_name = 'AGENT.md';
SELECT dolt_commit('-A', '-m', 'update docs');
```

## Schema

`dolt_docs(doc_name TEXT NOT NULL, doc_text TEXT NOT NULL, PRIMARY KEY(doc_name))`

Served before it exists; the first write creates the real table, which then
commits, diffs, branches, and merges like any other.

## Behaviour

- A fresh database serves one row, `AGENT.md`, DoltLite's operations guide
  for AI agents. See [agents.md](agents.md) for the text and how to use it.
- The default row is stored on first write like any other row, so deleting
  or replacing it sticks and shows in diffs.
- `doc_name` is the primary key: a second `INSERT` of the same name fails
  with `UNIQUE constraint failed: dolt_docs.doc_name`; use `REPLACE`.

## Differences from Dolt

Dolt synthesizes its default docs on every read and resurrects them when
deleted; DoltLite stores them. The default `AGENT.md` text is DoltLite's own,
and the table appears in `.tables` once it exists.

## See also

[agents.md](agents.md), [dolt_ignore.md](dolt_ignore.md),
[dolt_tests.md](dolt_tests.md), `test/vc_oracle_docs_test.sh`.
