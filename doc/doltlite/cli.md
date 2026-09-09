# The `doltlite` shell and open options

`doltlite` is the SQLite shell. Every dot-command, flag, and output mode is
upstream's. This page lists what differs and the open-time options that
select an engine, branch, or access mode.

## Opening a database

```
doltlite my.db                 # DoltLite format if new; stock SQLite files open as stock
doltlite my.db@feature         # branch (also my.db/feature); see refs.md
doltlite my.db/v1.0            # tag or ancestor: read-only snapshot
doltlite :memory:
doltlite 'file:my.db?doltlite_engine=sqlite'
doltlite 'file:my.db?immutable=1'
doltlite 'file:clone.db?lazy_origin=1'
```

| URI parameter | Effect |
|---|---|
| `doltlite_engine=sqlite` | Create the file as a stock SQLite database. Ignored once the file has content, so it never reinterprets an existing database. |
| `immutable=1` | Read-only, as in SQLite. `dolt_*` writes fail with `attempt to write a readonly database`. |
| `lazy_origin=1` | On a lazy clone, fetch missing chunks from `origin` on demand. Without it an uncached chunk is an error. |

The same strings work in `sqlite3_open_v2()` and every binding. Engine
selection is by file header: a stock file always opens on SQLite's B-tree
engine, and `dolt_*` functions there fail with `dolt version-control features
are not available on stock SQLite databases`. `SELECT doltlite_engine()`
returns `prolly` or `orig`.

## What differs from the SQLite shell

| Surface | DoltLite |
|---|---|
| Prompt | `doltlite> ` |
| `-version` | DoltLite/SQLite versions and architecture |
| `.schema`, `.dump` | Objects in catalog order (sorted by name), with the canonical `CREATE` text rather than the text you typed |
| `.dump --preserve-rowids` | Omits the read-only rowid alias on tables with a non-integer primary key, so the output restores |
| `.backup`, `.restore` | Work within one format. A DoltLite source writes a DoltLite copy, a stock source a stock copy. Mixing them is refused: `cannot backup between a legacy SQLite database and a doltlite database`. |
| `.open path@branch` | Selects a branch, like the command line |
| Committer identity | `dolt_config('user.name', ...)` is per connection. With nothing set, commits record `doltlite` and an empty email. |

Views and triggers over `dolt_*` virtual tables work with the shell's default
`trusted_schema=0`.

## Environment variables

| Variable | Used by |
|---|---|
| `DOLTLITE_CA_FILE` | HTTPS remotes: a private CA bundle |
| `DOLTLITE_HTTP_TIMEOUT_MS` | HTTP remote requests (default 30000) |
| `DOLTLITE_CREDS_DIR` | Credential store location (default `~/.doltlite/creds`) |
| `DOLTLITE_CREDS_KID` | Which stored credential to present |
| `SQLITE_HISTORY`, `NO_COLOR`, `VISUAL` | Upstream shell behaviour, unchanged |

Remote credentials are covered in [auth.md](auth.md).
