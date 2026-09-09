# dolt_ignore

Patterns for tables that `dolt_add('-A')` and `dolt_commit('-A')` should
skip. Dolt: [dolt_ignore](https://docs.dolthub.com/sql-reference/version-control/dolt-system-tables#dolt_ignore).

## Synopsis

```sql
SELECT * FROM dolt_ignore;                          -- empty on a fresh database
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
INSERT INTO dolt_ignore VALUES ('tmp_keep', 0);     -- exception
SELECT dolt_add('-f', 'tmp_table');                 -- stage an ignored table anyway
```

## Schema

`dolt_ignore(pattern TEXT NOT NULL, ignored TINYINT NOT NULL, PRIMARY KEY(pattern))`

The table is served empty before it exists; the first write creates it, and
from then on it commits, diffs, branches, and merges like any other table.

## Behaviour

- `*` or `%` match any run of characters, `?` exactly one.
- A matching table is skipped by `dolt_add('-A')`, hidden from `dolt_status`,
  and left in the working set untouched.
- The most specific matching pattern wins, so `tmp_keep` with `ignored = 0`
  overrides `tmp_*`. Two patterns of equal specificity that disagree error
  with `the table <t> matches conflicting patterns in dolt_ignore`.
- Naming an ignored table in `dolt_add` does not stage it; only
  `dolt_add('-f', table)` does.

## Differences from Dolt

None intended. The table shows in `.tables` once it exists.

## See also

[dolt_commit.md](dolt_commit.md), [dolt_docs.md](dolt_docs.md),
[dolt_tests.md](dolt_tests.md), `test/vc_oracle_ignore_test.sh`.
