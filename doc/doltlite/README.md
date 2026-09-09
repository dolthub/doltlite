# DoltLite documentation

Flat list. The [README](../../README.md) covers install, bindings, and a tour
of the version-control features; these pages hold the detail.

## Build and embed

- [building.md](building.md) — macOS, Linux, Windows, WebAssembly builds and flags
- [embedding.md](embedding.md) — C API surface, exported symbols, C / Python / Go quickstarts
- [using-existing-sqlite-bindings.md](using-existing-sqlite-bindings.md) — pointing rusqlite, go-sqlite3, Dart, .NET and FFI bindings at libdoltlite

## Behaviour contracts

- [sqlite-compatibility.md](sqlite-compatibility.md) — where a DoltLite-format database differs from SQLite
- [concurrency.md](concurrency.md) — connections, processes, locks, and per-session branches
- [storage-format.md](storage-format.md) — the chunk-store file and frozen format version 12
- [sqlite-files.md](sqlite-files.md) — opening and attaching stock SQLite databases

## Remotes

- [remotes.md](remotes.md) — push, fetch, pull, clone, lazy clones, filesystem and HTTP URLs
- [remotesrv.md](remotesrv.md) — running `doltlite-remotesrv`
- [auth.md](auth.md) — credentials, JWT, and TLS for remotes

## Extensions

- [vec1.md](vec1.md) — versioned vector search

## Project

- [performance.md](performance.md) — benchmarks and asserted complexity properties
- [testing.md](testing.md) — test layers, oracles, and allowlists
- [demo.md](demo.md) — a narrative walkthrough of the version-control features
