# DoltLite documentation

Flat list. The [README](../../README.md) covers install, bindings, and a tour
of the version-control features; these pages hold the detail.

## Start here

- [demo.md](demo.md) — **The DoltHub Break Room Incident**: a whodunit that is
  secretly the tutorial. Every SQL block runs as written against
  `./doltlite case.db`, and a test keeps the story honest.

## Build and embed

- [building.md](building.md) — macOS, Linux, Windows, WebAssembly builds and flags
- [cli.md](cli.md) — the `doltlite` shell: what differs from `sqlite3`, URI parameters, environment variables
- [embedding.md](embedding.md) — C API surface, exported symbols, C / Python / Go quickstarts
- [bindings.md](bindings.md) — every language package: what it bundles, platforms, and gotchas
- [using-existing-sqlite-bindings.md](using-existing-sqlite-bindings.md) — pointing rusqlite, go-sqlite3, Dart, .NET and FFI bindings at libdoltlite

## Behaviour contracts

- [sqlite-compatibility.md](sqlite-compatibility.md) — where a DoltLite-format database differs from SQLite
- [concurrency.md](concurrency.md) — connections, processes, locks, and per-session branches
- [storage-format.md](storage-format.md) — the chunk-store file and frozen format version 12
- [pragmas.md](pragmas.md) — every PRAGMA: same as SQLite, adapted, or accepted and inert
- [sqlite-files.md](sqlite-files.md) — opening and attaching stock SQLite databases

## Version control

Every `dolt_*` function and table, one page per feature. Revision syntax and
the forms each surface accepts are in [refs.md](refs.md).

- [dolt_commit.md](dolt_commit.md) — `dolt_add`, `dolt_commit`, `dolt_status`, `dolt_config`
- [dolt_reset.md](dolt_reset.md) — `dolt_reset`, `dolt_clean`
- [dolt_branch.md](dolt_branch.md) — `dolt_branch`, `dolt_checkout`, `active_branch`, `dolt_branches`, `dolt_remote_branches`, `dolt_default_branch`, `dolt_connect_branch`
- [dolt_merge.md](dolt_merge.md) — `dolt_merge`, `dolt_merge_status`, `dolt_merge_base`, `dolt_conflicts`, `dolt_conflicts_<table>`, `dolt_schema_conflicts`, `dolt_conflicts_resolve`
- [dolt_diff.md](dolt_diff.md) — `dolt_diff`, `dolt_diff_<table>`, `dolt_diff_stat`, `dolt_diff_summary`, `dolt_schema_diff`, `dolt_patch`
- [dolt_log.md](dolt_log.md) — `dolt_log`, `dolt_history_<table>`, `dolt_at_<table>`, `dolt_blame_<table>`, `dolt_commit_ancestors`
- [dolt_remote.md](dolt_remote.md) — `dolt_remote`, `dolt_push`, `dolt_fetch`, `dolt_pull`, `dolt_clone`, `dolt_remotes`
- [dolt_rebase.md](dolt_rebase.md) — `dolt_rebase` and the plan table
- [dolt_cherry_pick.md](dolt_cherry_pick.md) — `dolt_cherry_pick`, `dolt_revert`
- [dolt_tag.md](dolt_tag.md) — `dolt_tag`, `dolt_tags`
- [dolt_hashof.md](dolt_hashof.md) — `dolt_hashof`, `dolt_hashof_table`, `dolt_hashof_index`, `dolt_hashof_db`, `dolt_hashof_catalog`
- [dolt_gc.md](dolt_gc.md) — `dolt_gc`, `VACUUM`
- [dolt_constraint_violations.md](dolt_constraint_violations.md) — `dolt_constraint_violations`, `dolt_constraint_violations_<table>`, `dolt_verify_constraints`
- [dolt_workspace.md](dolt_workspace.md) — `dolt_workspace_<table>`
- [dolt_ignore.md](dolt_ignore.md) — `dolt_ignore`, `dolt_docs`, `dolt_tests`, `dolt_test_run`
- [dolt_schemas.md](dolt_schemas.md) — `dolt_schemas` and the versioned catalog
- [dolt_creds.md](dolt_creds.md) — `dolt_creds_new`, `dolt_creds`
- [dolt_version.md](dolt_version.md) — `dolt_version`, `doltlite_engine`

## Cross-cutting

- [transactions.md](transactions.md) — what `ROLLBACK` undoes, conflicts inside vs outside `BEGIN`, busy outcomes, refusals
- [refs.md](refs.md) — revision syntax: branches, tags, hashes, `HEAD~N`, `WORKING`, `STAGED`, ranges, `db@branch` opens, name rules
- [dolt-differences.md](dolt-differences.md) — what is deliberately different from Dolt, and what is missing on purpose
- [agents.md](agents.md) — the in-database `AGENT.md` guide and the rules an AI agent should know

## Remotes

- [remotes.md](remotes.md) — push, fetch, pull, clone, lazy clones, filesystem and HTTP URLs
- [remotesrv.md](remotesrv.md) — running `doltlite-remotesrv`
- [auth.md](auth.md) — credentials, JWT, and TLS for remotes

## Extensions

- [wasm.md](wasm.md) — the WebAssembly build: persistence, remotes over XHR / worker threads, build flags
- [vec1.md](vec1.md) — versioned vector search

## Project

- [performance.md](performance.md) — benchmarks and asserted complexity properties
- [testing.md](testing.md) — test layers, oracles, and allowlists
