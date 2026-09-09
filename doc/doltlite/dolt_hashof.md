# Content hashes: dolt_hashof and friends

Forty-character hashes of commits, tables, indexes, and the whole catalog.
Dolt: [dolt_hashof](https://docs.dolthub.com/sql-reference/version-control/dolt-sql-functions#dolt_hashof),
[dolt_hashof_table](https://docs.dolthub.com/sql-reference/version-control/dolt-sql-functions#dolt_hashof_table),
[dolt_hashof_db](https://docs.dolthub.com/sql-reference/version-control/dolt-sql-functions#dolt_hashof_db).

## Synopsis

```sql
SELECT dolt_hashof('HEAD');                     -- commit hash
SELECT dolt_hashof('main~2');
SELECT dolt_hashof_table('users');              -- includes working edits
SELECT dolt_hashof_table('users', 'v1.0');
SELECT dolt_hashof_index('users_by_email');
SELECT dolt_hashof_db();                        -- whole catalog, WORKING
SELECT dolt_hashof_db('HEAD');
SELECT dolt_hashof_catalog();
```

| Function | Argument | Hashes |
|---|---|---|
| `dolt_hashof(rev)` | one commit [revision](refs.md); required | the commit |
| `dolt_hashof_table(table [, rev])` | default `WORKING` | the table's rows plus every index on it |
| `dolt_hashof_index(index [, rev])` | default `WORKING` | one index |
| `dolt_hashof_db([rev])` | default `WORKING` | every table root and the catalog membership |
| `dolt_hashof_catalog([rev])` | default `WORKING` | the stored catalog chunk itself |

All accept `WORKING` and `STAGED` except `dolt_hashof`, which needs a commit.

## Behaviour

`_table`, `_index`, and `_db` are history-independent: the same set of rows
hashes the same on any branch, in any insert order. `REINDEX` changes no hash
on a healthy database; a hash that moves across `REINDEX` means the stored
index did not match its rows. `_catalog` is the raw catalog chunk and is
layout-sensitive; use `_db` for content identity.

An index belongs to its table: a changed index moves `dolt_hashof_table` and
shows in `dolt_status` as a modification of the table.

| Error | Cause |
|---|---|
| `dolt_hashof: invalid ref spec` | unknown name, short hash, empty string, or `WORKING` |
| `dolt_hashof: invalid ancestor spec` | bad `~`/`^` suffix |
| `dolt_hashof() takes exactly one argument` | arity |

## See also

[refs.md](refs.md), `test/vc_oracle_hashof_test.sh`,
`test/vc_oracle_hashof_errors_test.sh`.
