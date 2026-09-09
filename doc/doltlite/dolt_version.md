# dolt_version and doltlite_engine

```sql
SELECT dolt_version();      -- build version from git describe
SELECT doltlite_engine();   -- prolly
SELECT sqlite_version();    -- 3.54.0
```

| Function | Returns |
|---|---|
| `dolt_version()` | The DoltLite release, from `git describe` at build time. No arguments. |
| `doltlite_engine()` | `prolly` on a DoltLite-format main database, `orig` when the main database is a stock SQLite file |
| `sqlite_version()` | The bundled SQLite version, unchanged |

Use `doltlite_engine()` before storage-backed `dolt_*` operations. On a stock
file they fail with `dolt version-control features are not available on stock
SQLite databases`; `dolt_version()` remains available. Shell `-version` prints
the DoltLite version, SQLite version, and process bitness, but not the engine.

## See also

[cli.md](cli.md), [sqlite-files.md](sqlite-files.md).
