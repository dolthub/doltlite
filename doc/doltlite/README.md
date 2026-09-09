# DoltLite documentation

Flat list. The [README](../../README.md) covers install, bindings, and a tour
of the version-control features; these pages hold the detail.

## Build and embed

- [building.md](building.md) — macOS, Linux, Windows, WebAssembly builds and flags
- [cli.md](cli.md) — the `doltlite` shell: what differs from `sqlite3`, URI parameters, environment variables
- [embedding.md](embedding.md) — C API surface, exported symbols, C / Python / Go quickstarts
- [using-existing-sqlite-bindings.md](using-existing-sqlite-bindings.md) — pointing rusqlite, go-sqlite3, Dart, .NET and FFI bindings at libdoltlite

## Behaviour contracts

- [sqlite-compatibility.md](sqlite-compatibility.md) — where a DoltLite-format database differs from SQLite
- [concurrency.md](concurrency.md) — connections, processes, locks, and per-session branches
- [storage-format.md](storage-format.md) — the chunk-store file and frozen format version 12
- [pragmas.md](pragmas.md) — every PRAGMA: same as SQLite, adapted, or accepted and inert
- [sqlite-files.md](sqlite-files.md) — opening and attaching stock SQLite databases

## Version control

- [transactions.md](transactions.md) — what `ROLLBACK` undoes, conflicts inside vs outside `BEGIN`, busy outcomes, refusals
- [refs.md](refs.md) — revision syntax: branches, tags, hashes, `HEAD~N`, `WORKING`, `STAGED`, ranges, `db@branch` opens, name rules

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
- [demo.md](demo.md) — a narrative walkthrough of the version-control features
