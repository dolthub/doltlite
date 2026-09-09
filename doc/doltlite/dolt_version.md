# dolt_version and doltlite_engine

```sql
SELECT dolt_version();      -- v0.50.7
SELECT doltlite_engine();   -- prolly
SELECT sqlite_version();    -- 3.54.0
```

| Function | Returns |
|---|---|
| `dolt_version()` | The DoltLite release, from `git describe` at build time. No arguments. |
| `doltlite_engine()` | `prolly` on a DoltLite-format main database, `orig` when the main database is a stock SQLite file |
| `sqlite_version()` | The bundled SQLite version, unchanged |

`doltlite_engine()` is the test to run before calling any `dolt_*` function:
on a stock file they fail with `dolt version-control features are not
available on stock SQLite databases`. The shell's `-version` prints all of
this on one line.

## See also

[cli.md](cli.md), [sqlite-files.md](sqlite-files.md).
