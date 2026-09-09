<p align="center">
  <img src="art/doltlite-logo.png" alt="DoltLite" width="600">
</p>

# DoltLite

A SQLite fork that replaces the B-tree storage engine with a content-addressed
[prolly tree](https://docs.dolthub.com/architecture/storage-engine/prolly-tree),
giving Git-like version control on a SQL database. The parser, planner, and
VDBE stay upstream-derived above SQLite's `btree.h` seam; below it, a
single-file chunk store backs prolly trees instead of SQLite pages.

[Why DoltLite?](https://www.dolthub.com/blog/2026-04-27-why-doltlite/) DoltLite
can be embedded in any language enabling local-first use cases for [Dolt](https://github.com/dolthub/dolt/).

You can read more about DoltLite, including its 
[origin story](https://www.dolthub.com/blog/2026-03-24-a-week-in-gas-town/), 
on the [DoltHub blog](https://www.dolthub.com/blog/?tags=doltlite). DoltLite is 
the proud product of 
[agentic engineering](https://www.dolthub.com/blog/2026-08-17-top-5-agent-engineered-open-source-projects/).

[DoltLite is Beta](https://www.dolthub.com/blog/2026-08-31-doltlite-beta/).
Documentation beyond this README lives in [doc/doltlite](doc/doltlite/README.md).

## Install

Prebuilt binaries: [github.com/dolthub/doltlite/releases](https://github.com/dolthub/doltlite/releases).

Each install method places the same set of files (paths shown for `/usr/local`):

- `bin/doltlite`, `bin/doltlite-remotesrv` — the CLI shell and remote sync server
- `include/doltlite.h` — embedding header (`sqlite3_*` plus DoltLite C APIs;
  `#include <doltlite.h>`)
- `include/doltlite_remotesrv.h` — in-process remote server API
- `lib/libdoltlite.a` — static library
- `lib/libdoltlite.{so,dylib}` — shared library

### macOS (Apple Silicon) / Linux (x86_64 or arm64)

```
sudo bash -c 'curl -fsSL https://github.com/dolthub/doltlite/releases/latest/download/install.sh | bash'
```

### Debian / Ubuntu

`.deb` packages ship for both `amd64` and `arm64`. Substitute `$ARCH` below:

```
VER=$(curl -fsSL https://api.github.com/repos/dolthub/doltlite/releases/latest | jq -r .tag_name | sed 's/^v//')
ARCH=amd64   # or arm64
BASE=https://github.com/dolthub/doltlite/releases/download/v${VER}
wget ${BASE}/libdoltlite0_${VER}_${ARCH}.deb ${BASE}/doltlite_${VER}_${ARCH}.deb
sudo dpkg -i libdoltlite0_*.deb doltlite_*.deb
```

Add `libdoltlite-dev_${VER}_${ARCH}.deb` for the header and static library.

### Windows

Download `doltlite-tools-win-x64-<ver>.zip` from
[releases](https://github.com/dolthub/doltlite/releases), extract `doltlite.exe`, add to `PATH`.

## Bindings

Language-specific wrappers around `libdoltlite`. Each one exposes the bundled
SQLite version's public `sqlite3_*` API surface plus the Dolt version-control
functions, subject to the [storage-engine exceptions](#sqlite-compatibility).

| Language | Distribution | Source |
|---|---|---|
| Python | `pip install doltlite` | [dolthub/doltlite-python](https://github.com/dolthub/doltlite-python) |
| Ruby | `gem install doltlite` | [dolthub/doltlite-ruby](https://github.com/dolthub/doltlite-ruby) |
| Node.js / Bun | `npm install @dolthub/doltlite` | [dolthub/doltlite-node](https://github.com/dolthub/doltlite-node) |
| PHP | `composer require dolthub/doltlite-php` | this repo ([`packaging/composer`](packaging/composer)), distributed via [dolthub/doltlite-php](https://github.com/dolthub/doltlite-php) |
| .NET | `dotnet add package DoltHub.Doltlite` | this repo ([`packaging/nuget`](packaging/nuget)); works under Microsoft.Data.Sqlite.Core, EF Core, Dapper |
| Rust | `cargo add doltlite` | this repo ([`packaging/rust`](packaging/rust)); engine vendored, or run the full default build and point `rusqlite` at it with `SQLITE3_LIB_DIR` |
| Go | `go get github.com/dolthub/doltlite-driver` | this repo ([`packaging/go`](packaging/go)), distributed via [dolthub/doltlite-driver](https://github.com/dolthub/doltlite-driver); `database/sql` driver, engine vendored |
| Browser / WASM | `npm install @dolthub/doltlite-wasm` | this repo ([`packaging/npm`](packaging/npm), built from [`ext/wasm`](ext/wasm)) |
| Swift (iOS / macOS) | SwiftPM: `https://github.com/dolthub/doltlite-swift` | [dolthub/doltlite-swift](https://github.com/dolthub/doltlite-swift) (XCFramework built by [`packaging/swift`](packaging/swift)) |
| Android | Gradle: `com.dolthub:doltlite-android` | [dolthub/doltlite-android](https://github.com/dolthub/doltlite-android) (AAR + JNA) |

## Building

```
cd build
../configure
make
./doltlite :memory:
```

Windows, WebAssembly, stock-SQLite comparison builds, and build flags:
[building.md](doc/doltlite/building.md).

## Using as a C Library

`#include <doltlite.h>` and link `libdoltlite.a -lpthread -lz`. The public API
is SQLite's `sqlite3_*` declarations plus the DoltLite additions in
`doltlite.h`. Details, exported symbols, and C / Python / Go quickstarts:
[embedding.md](doc/doltlite/embedding.md).

## Dolt Features

Version control is SQL functions and virtual tables. One example each; every
option, column, and error is on the linked page, and revision spellings
(`HEAD~1`, `WORKING`, `main..feature`, ...) are in
[refs.md](doc/doltlite/refs.md).

**Commit loop** — [dolt_commit.md](doc/doltlite/dolt_commit.md)

```sql
SELECT dolt_config('user.name', 'Ann');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'Add users');           -- or dolt_commit('-Am', 'msg')
SELECT * FROM dolt_status;
```

**Row-level staging** — [dolt_workspace.md](doc/doltlite/dolt_workspace.md)

```sql
UPDATE dolt_workspace_ratings SET staged = 1 WHERE to_confidence > from_confidence;
```

**Ignore, docs, tests** — [dolt_ignore.md](doc/doltlite/dolt_ignore.md)

```sql
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
INSERT INTO dolt_docs VALUES ('README.md', '# my project');
INSERT INTO dolt_tests VALUES ('count', 'users', 'SELECT * FROM users', 'expected_rows', '==', '10');
SELECT * FROM dolt_test_run();
```

**Diff** — [dolt_diff.md](doc/doltlite/dolt_diff.md)

```sql
SELECT * FROM dolt_diff_users('v1.0', 'HEAD');
SELECT * FROM dolt_diff_users WHERE to_commit = 'WORKING';
SELECT * FROM dolt_diff_stat('v1.0', 'HEAD');
SELECT statement FROM dolt_patch('v1.0', 'HEAD') ORDER BY statement_order;
```

**Log, history, blame** — [dolt_log.md](doc/doltlite/dolt_log.md)

```sql
SELECT * FROM dolt_log('main..feature');
SELECT * FROM dolt_history_users WHERE id = 42;
SELECT * FROM dolt_at_users('v1.0');
SELECT * FROM dolt_blame_users;
```

**Schema objects** — [dolt_schemas.md](doc/doltlite/dolt_schemas.md)

```sql
SELECT type, name, fragment FROM dolt_schemas;     -- views and triggers
```

**Undo** — [dolt_reset.md](doc/doltlite/dolt_reset.md), [dolt_cherry_pick.md](doc/doltlite/dolt_cherry_pick.md)

```sql
SELECT dolt_reset('--hard');
SELECT dolt_revert('HEAD');
SELECT dolt_cherry_pick('0123abcd...');
```

**Branches** — [dolt_branch.md](doc/doltlite/dolt_branch.md)

```sql
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
SELECT active_branch();
```

Each connection has its own branch; uncommitted work belongs to the branch.
Open one at connect time with `my.db@feature`, or a read-only snapshot with
`my.db/v1.0`.

**Tags** — [dolt_tag.md](doc/doltlite/dolt_tag.md)

```sql
SELECT dolt_tag('v1.0');
```

**Merge and conflicts** — [dolt_merge.md](doc/doltlite/dolt_merge.md)

```sql
BEGIN;
SELECT dolt_merge('feature');                 -- error names the conflicts, if any
SELECT * FROM dolt_conflicts_users;
SELECT dolt_conflicts_resolve('--theirs', 'users');
SELECT dolt_commit('-m', 'Merge feature');
```

Conflicts live only inside the transaction; nothing conflicted reaches disk.
Constraint violations from a merge land in
`dolt_constraint_violations_<table>`
([dolt_constraint_violations.md](doc/doltlite/dolt_constraint_violations.md)).

**Rebase** — [dolt_rebase.md](doc/doltlite/dolt_rebase.md)

```sql
SELECT dolt_rebase('main');
SELECT dolt_rebase('-i', 'main');             -- then edit the dolt_rebase plan table
```

**Hashes and GC** — [dolt_hashof.md](doc/doltlite/dolt_hashof.md), [dolt_gc.md](doc/doltlite/dolt_gc.md)

```sql
SELECT dolt_hashof('HEAD'), dolt_hashof_table('users'), dolt_hashof_db();
SELECT dolt_gc();
```

**Remotes** — [dolt_remote.md](doc/doltlite/dolt_remote.md)

```sql
SELECT dolt_remote('add', 'origin', 'file:///path/to/remote.doltlite');
SELECT dolt_push('origin', 'main');
SELECT dolt_pull('origin', 'main');
SELECT dolt_clone('http://myserver:8080/mydb.db');
```

Remote semantics: [remotes.md](doc/doltlite/remotes.md). Serving over HTTP
with `doltlite-remotesrv`, which binds to localhost until TLS and
authentication are configured: [remotesrv.md](doc/doltlite/remotesrv.md).

**Version** — `SELECT dolt_version();` ([dolt_version.md](doc/doltlite/dolt_version.md))

## Using Existing SQLite Databases

Stock SQLite files are detected by their header and opened on SQLite's
original B-tree engine, directly or via `ATTACH`. Version control applies only
to DoltLite-format databases. Engine selection, `ATTACH` hybrids, and backup
rules: [sqlite-files.md](doc/doltlite/sqlite-files.md).

## SQLite Compatibility

DoltLite keeps SQLite's SQL semantics and `sqlite3_*` API. Storage-coupled
behaviour differs:

- Own on-disk format; no rollback journal, WAL, or shared-memory sidecars.
  `PRAGMA journal_mode` reports `wal` and ignores changes.
- `VACUUM` and `PRAGMA wal_checkpoint` run DoltLite garbage collection.
- A write transaction may touch only one file-backed database.
- Non-integer primary keys are clustered and `NOT NULL`; `rowid` is a
  read-only alias for them.
- Rowids come from a counter shared by every branch, so implicit-rowid
  inserts merge cleanly.
- `sqlite_schema` is a projection of the catalog with canonical `CREATE` text.

The full contract and its test mapping: [sqlite-compatibility.md](doc/doltlite/sqlite-compatibility.md).

## Concurrency

Multiple connections and processes may share one file. Coordination is explicit:

- Each connection selects its own branch; the uncommitted working set belongs
  to the branch, so another connection on that branch sees it.
- One durable writer at a time. A concurrent writer gets `SQLITE_BUSY`.
- Readers stay live while a peer writes or runs GC.
- Commits, merges, and pushes re-confirm HEAD under the lock, so a stale tip
  never clobbers a peer.
- Conflicts are never durable; they live only in the transaction that made them.

The full contract and its test mapping: [concurrency.md](doc/doltlite/concurrency.md).

## Storage Format

A DoltLite database is one content-addressed chunk-store file, not SQLite
pages. Format version 12 is frozen for the beta: every version-12 file stays
readable and writable by later version-12 builds. Layers, constants, and the
bump procedure: [storage-format.md](doc/doltlite/storage-format.md).

## Vector Search

The SQLite team's [vec1](https://sqlite.org/vec1) vector ANN extension is
built in — no extension loading — and vector tables are versioned like
everything else: branch, diff, historical search, clone, and push.

```sql
CREATE VIRTUAL TABLE embeddings USING vec1(vector, category);
INSERT INTO embeddings(rowid, vector, category) VALUES (1, :f32blob, 3);

-- Train and build the index (PQ compression; needs >= 512 vectors)
SELECT vec1_train(vector, '{nbucket: 64, codesize: 8, distance: "cos"}')
  FROM embeddings_base;             -- returns a model blob
INSERT INTO embeddings(cmd, arg) VALUES ('rebuild', :model);

-- KNN with metadata filtering and exact reranking
SELECT rowid FROM embeddings(:query, '{k: 100}')
 WHERE category = 3
 ORDER BY vec1_cos_distance(:query, vector) LIMIT 10;
```

Train with `codesize > 0` and concurrent branch writes to a built index
merge automatically: the raw vectors merge row-by-row and the index
rebuilds itself from the merged data, deterministically. Uncompressed
indexes, mixed conflicts, and missing models surface explicit conflicts
instead of losing data. Merge and storage semantics:
[doc/doltlite/vec1.md](doc/doltlite/vec1.md).

## Performance

Nightly DoltLite-versus-SQLite numbers: [performance-report.md](performance-report.md).
Benchmark CI and the complexity properties asserted in tests:
[performance.md](doc/doltlite/performance.md).

## Running Tests

```bash
cd build
../configure && make
bash ../test/run_doltlite_tests.sh
bash ../test/run_c_tests.sh
```

Every test layer, oracle, and allowlist: [testing.md](doc/doltlite/testing.md).

## Architecture

Same prolly-tree design as [Dolt](https://github.com/dolthub/dolt) —
content-addressed immutable nodes with rolling-hash boundaries — in C under
SQLite's `btree.h` seam. Engine code is `src/prolly_*.c` and `src/chunk_*.c`;
`dolt_*` SQL surfaces are `src/doltlite_*.c`; `src/prolly_btree.c` dispatches
the btree API.

Deeper comparison:
[Dolt vs DoltLite Storage](https://www.dolthub.com/blog/2026-07-08-dolt-doltlite-storage-comp/).
