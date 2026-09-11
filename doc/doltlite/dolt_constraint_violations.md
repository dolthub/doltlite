# Constraint violations and dolt_verify_constraints

Rows that break a foreign key, unique index, check, `NOT NULL`, or strict-type
constraint after a merge, and the re-scan that finds them. Dolt:
[dolt_constraint_violations](https://docs.dolthub.com/sql-reference/version-control/dolt-system-tables#dolt_constraint_violations),
[dolt_verify_constraints](https://docs.dolthub.com/sql-reference/version-control/dolt-sql-procedures#dolt_verify_constraints).

## Synopsis

```sql
SELECT * FROM dolt_constraint_violations;
SELECT violation_type, pk, violation_info FROM dolt_constraint_violations_child;
DELETE FROM dolt_constraint_violations_child WHERE pk = 2;   -- mark resolved
SELECT dolt_verify_constraints();                -- tables changed since HEAD
SELECT dolt_verify_constraints('--all');         -- every table
SELECT dolt_verify_constraints('--output-only', 'child');
```

## Where they come from

Merges apply cell by cell and do not run referential actions inline, so a
merged working set can hold rows no single side could have inserted. Those
land in `dolt_constraint_violations_<table>`. A merge that produces
violations in autocommit mode is rolled back: `Committing this transaction
resulted in a working set with constraint violations, transaction rolled
back.` Inside `BEGIN` they persist and block `dolt_commit` until resolved.

`NOT NULL` checks include stored and virtual generated columns. The checks
use their computed values: valid non-NULL results still merge, and nullable
generated columns may produce NULL.

## Tables

| Table | Columns |
|---|---|
| `dolt_constraint_violations` | `table`, `num_violations` |
| `dolt_constraint_violations_<table>` | `violation_type`, primary-key columns, remaining columns, `violation_info` (JSON) |

The per-table vtable exists only while that table has recorded violations.
`violation_type` is `foreign key`, `unique index`, `check constraint`, `not
null`, or `strict type`. Foreign-key, check, not-null, and strict-type
violators stay in the base table. A unique-index collision records every
member, and the higher-rowid row is moved out of the table into the violations
vtable. Resolve by fixing the data and `DELETE`-ing the violation row.

## dolt_verify_constraints

| Option | Meaning |
|---|---|
| none | Scan tables changed since `HEAD` and record violations |
| `-a`, `--all` | Scan every table |
| `--output-only` | Record nothing; only report |
| `table`, ... | Restrict the scan |

Returns `1` if violations were found, `0` otherwise. Each call rewrites the
findings for the scanned tables. Unknown table: `table not found: <x>`.

## Committing

`dolt_commit` refuses with `cannot commit: unresolved entries in
dolt_constraint_violations. Resolve them (DELETE from the per-table vtable)
then retry, or pass --force to commit anyway.` `--force` commits with the
violations still recorded. Unlike conflicts, violations do persist to disk
and follow the branch.

## See also

[dolt_merge.md](dolt_merge.md), [transactions.md](transactions.md),
`test/vc_oracle_constraint_violations_test.sh`,
`test/vc_oracle_verify_constraints_test.sh`.
