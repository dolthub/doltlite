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
| Browser / WASM | `npm install @dolthub/doltlite-wasm` | this repo ([`packaging/npm`](packaging/npm), built from [`ext/wasm`](ext/wasm)) |
| Swift (iOS / macOS) | SwiftPM: `https://github.com/dolthub/doltlite-swift` | [dolthub/doltlite-swift](https://github.com/dolthub/doltlite-swift) (XCFramework built by [`packaging/swift`](packaging/swift)) |
| Android | Gradle: `com.dolthub:doltlite-android` | [dolthub/doltlite-android](https://github.com/dolthub/doltlite-android) (AAR + JNA) |

## Building

### macOS / Linux

```
cd build
../configure
make
./doltlite :memory:
```

### Windows (MSYS2 / MINGW64)

```
pacman -S mingw-w64-x86_64-gcc mingw-w64-x86_64-zlib make tcl
mkdir -p build && cd build
../configure
make doltlite.exe
./doltlite.exe :memory:
```

To verify the engine:

```sql
SELECT doltlite_engine();
-- prolly
```

To build stock SQLite instead (for comparison):

```
make DOLTLITE_PROLLY=0 sqlite3
```

Vec1 is built into native DoltLite by default. Use `make DOLTLITE_VEC1=0` to
omit it. Compile the DoltLite amalgamation with `-DDOLTLITE_VEC1=1` to include
vec1; otherwise it can be built and loaded as an extension.

### WebAssembly (`ext/wasm`)

Vendored SQLite `ext/wasm`, defaulting to the DoltLite engine. Build generated
SQLite sources first, then wasm:

```bash
./configure
make sqlite3.c sqlite3.h sqlite3ext.h
make -C ext/wasm
# → ext/wasm/jswasm/{sqlite3.js,sqlite3.mjs,sqlite3.wasm}
make -C ext/wasm DOLTLITE_WASM=0   # upstream SQLite wasm instead
make -C ext/wasm DOLTLITE_ENABLE_REMOTES=0 # DoltLite without remote clients
make -C ext/wasm dist             # zip package
```

`DOLTLITE_ENABLE_REMOTES=0` omits clone, fetch, pull, push, HTTP, TLS, and
credential code. Calls to the remote SQL functions then return `DoltLite
remotes are disabled in this build`. Browser builds with remotes enabled need
an Emscripten-compatible socket transport or proxy; see
[`examples/wa-sqlite-clone.mjs`](examples/wa-sqlite-clone.mjs) for a public
clone request that exercises the client.

`DOLTLITE_ENABLE_CHUNK_SOURCE=0` omits host-provided and origin-backed lazy
chunk fetching. The feature is enabled by default.

## Using as a C Library

Public C API is the bundled SQLite declarations under `sqlite3_*` names plus
the DoltLite-specific declarations in `doltlite.h`. Port supported programs by
switching the include/link to `libdoltlite`; APIs tied to SQLite's pager, page
format, or journaling differ (see [SQLite Compatibility](#sqlite-compatibility)).
Dolt features are SQL functions (`dolt_commit`, `dolt_branch`, …) and virtual
tables (`dolt_log`, `dolt_diff_<table>`, …).

`doltlite_set_chunk_source()` registers synchronous `xGet` and `xGetMany`
callbacks for one attached database. `doltlite_init_lazy()` installs a refs
blob into a fresh or existing main database, allowing missing graph chunks to
be fetched and cached on demand. Source objects remain owned by the host and
must outlive their registrations.

Loadable extensions use `doltliteext.h` (rebranded `sqlite3ext.h`, shipped in
the amalgamation zip). The shared library exports only `sqlite3_*`,
`doltliteServe*` (`doltlite_remotesrv.h`), `doltlite_set_chunk_source`, and
`doltlite_init_lazy`; prolly/chunk-store internals, other `doltlite*` symbols,
and vendored crypto are hidden. The static archive is unfiltered for tests and
tooling.

```bash
cd build
../configure
make doltlite-lib   # libdoltlite.a and libdoltlite.dylib/.so

# Static (recommended) or dynamic
gcc -o myapp myapp.c -I/path/to/build libdoltlite.a -lpthread -lz
gcc -o myapp myapp.c -I/path/to/build -L/path/to/build -ldoltlite -lpthread -lz

sudo make install   # honours --prefix / DESTDIR; then:
gcc -o myapp myapp.c -ldoltlite -lpthread -lz
```

`make install` also installs SQLite-named artifacts (`sqlite3.h`,
`libsqlite3.*`, …) from this tree — release packages omit those so they do not
collide with system SQLite. Use a private `--prefix` if that matters.

### Quickstart Examples

Same flow (commits, branches, merges, diffs, tags) in each language.

**C** ([`examples/quickstart.c`](examples/quickstart.c)) — based on the
[SQLite quickstart](https://sqlite.org/quickstart.html):

```bash
cd build
gcc -o quickstart ../examples/quickstart.c -I. libdoltlite.a -lpthread -lz
./quickstart
```

**Python** ([`examples/quickstart.py`](examples/quickstart.py)) — stdlib
`sqlite3` with the [`doltlite`](https://github.com/dolthub/doltlite-python)
package (bundles libdoltlite):

```bash
pip install doltlite
python3 examples/quickstart.py
```

Needs a Python whose `_sqlite3` links a shared `libsqlite3` (distro,
Homebrew, pyenv, or conda). Avoid python-build-standalone (`uv python install`
defaults), the python.org macOS installer, and Apple system Python — they
static-link SQLite and cannot preload libdoltlite. Local-build preload
recipes (including macOS) are in the
[doltlite-python](https://github.com/dolthub/doltlite-python) README.

**Go** ([`examples/go/main.go`](examples/go/main.go)) — uses
[mattn/go-sqlite3](https://github.com/mattn/go-sqlite3) with the `libsqlite3`
build tag:

```bash
cd examples/go
CGO_CFLAGS="-I../../build" CGO_LDFLAGS="../../build/libdoltlite.a -lz -lpthread" \
    go build -tags libsqlite3 -o quickstart .
./quickstart
```

## Dolt Features

Version control operations are exposed as SQL functions and virtual tables.

### The basic commit loop

#### Configuration

Per-connection, not persisted. Used by `dolt_commit`, `dolt_merge`,
`dolt_cherry_pick`, and `dolt_revert`. `dolt_commit --author` overrides once.

```sql
SELECT dolt_config('user.name', 'Tim Sehn');
SELECT dolt_config('user.email', 'tim@dolthub.com');
SELECT dolt_config('user.name');
-- Tim Sehn
```

#### Staging and Committing

```sql
SELECT dolt_add('users');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'Add users table');
SELECT dolt_commit('-A', '-m', 'Initial commit');
SELECT dolt_commit('-am', 'Initial commit');   -- like git commit -am
SELECT dolt_commit('-m', 'Fix data', '--author', 'Alice <alice@example.com>');
```

#### Status

```sql
SELECT * FROM dolt_status;
-- table_name | staged | status
-- users      | 1      | modified
-- orders     | 0      | new table
```

#### Workspace Tables

Row-level working/staged edits; set `staged` to stage/unstage. `DELETE`
discards unstaged rows (staged rows must be unstaged first).

```sql
SELECT id, staged, diff_type, to_id, to_rating, to_confidence,
       from_rating, from_confidence
  FROM dolt_workspace_ratings;

UPDATE dolt_workspace_ratings
   SET staged = TRUE
 WHERE to_confidence > from_confidence;

SELECT dolt_commit('-m', 'accept higher-confidence edits');
```

#### Ignoring Tables (`dolt_ignore`)

Patterns skipped by `dolt_add` and hidden from `dolt_status` (tables stay in
the working set). `SELECT` works before any pattern exists; the first write
creates the backing table, which then commits, diffs, branches and merges
like any other table. Patterns use `*`/`%` = any and `?` = one char.
Most-specific match wins; equal-specificity conflicts error.

```sql
SELECT * FROM dolt_ignore;                       -- empty on a fresh repo
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
INSERT INTO dolt_ignore VALUES ('tmp_keep', 0);  -- un-ignore
```

#### Repository Docs (`dolt_docs`)

Versioned documents keyed by name (`README.md`, `LICENSE.md`, or any name),
as in Dolt. `SELECT` works before any doc exists; the first write statement
creates the backing table, which then commits, diffs, branches and merges
like any other table. A fresh repo serves a default `AGENT.md` (a usage
guide for AI agents); overwrite or delete it like any other doc.

```sql
SELECT * FROM dolt_docs;                       -- default AGENT.md on a fresh repo
INSERT INTO dolt_docs VALUES ('README.md', '# my project');
REPLACE INTO dolt_docs VALUES ('README.md', '# updated');
SELECT dolt_commit('-A', '-m', 'update readme');
```

#### Repository Tests (`dolt_tests`)

Versioned SQL tests live in `dolt_tests`. The first write creates the backing
table; test definitions then commit, diff, branch and merge like ordinary
data. Each read-only query can assert its row count, column count, or single
result value with `==`, `!=`, `<`, `>`, `<=`, or `>=`. Run every test with no
argument or `'*'`, or select tests by test name or group.

```sql
INSERT INTO dolt_tests VALUES (
  'user count', 'users', 'SELECT * FROM users', 'expected_rows', '==', '10'
);
SELECT * FROM dolt_test_run();
SELECT * FROM dolt_test_run('users');
```

### Inspecting what's there

#### Diff

```sql
-- Tables changed across commit history
SELECT * FROM dolt_diff WHERE table_name = 'users';

-- Row/cell counts between refs
SELECT * FROM dolt_diff_stat('v1.0', 'HEAD');
SELECT * FROM dolt_diff_stat('v1.0', 'HEAD', 'users');

-- Per-table added / dropped / renamed / modified
SELECT * FROM dolt_diff_summary('v1.0', 'HEAD');

-- Schema-level (tables, views, indexes)
SELECT * FROM dolt_schema_diff('v1.0', 'v2.0');

-- Ordered, executable SQLite statements (schema rebuilds when ALTER cannot express)
SELECT * FROM dolt_patch('v1.0', 'v2.0');
SELECT * FROM dolt_patch('v1.0', 'v2.0', 'users');
SELECT * FROM dolt_patch('v1.0..v2.0');
SELECT * FROM dolt_patch('main...feature', 'users');
SELECT statement FROM dolt_patch('HEAD', 'WORKING')
 WHERE diff_type = 'data'
 ORDER BY statement_order;

-- Per-table row history (to_/from_ columns + commit metadata + diff_type).
-- One vtable per user table. to_commit = 'WORKING' is staged + working.
SELECT * FROM dolt_diff_users;
SELECT * FROM dolt_diff_users WHERE to_id = 42;
SELECT * FROM dolt_diff_users WHERE to_commit = 'WORKING';

-- TVF form: snapshots at two refs (table name is in the module, like Dolt).
-- Two dots = endpoints; three dots = merge base to right endpoint.
SELECT * FROM dolt_diff_users('HEAD~1', 'HEAD');
SELECT * FROM dolt_diff_users('v1.0', 'WORKING');
SELECT * FROM dolt_diff_users('main..feature');
SELECT * FROM dolt_diff_users('main...feature');
-- The table may exist only at one endpoint.
SELECT * FROM dolt_diff_feature_only('main', 'feature');

SELECT d.*
  FROM dolt_diff_users AS d
  JOIN dolt_log('v1.0..HEAD') AS l ON l.commit_hash = d.to_commit;
```

#### Log and History

```sql
-- Commit history
SELECT * FROM dolt_log;
SELECT * FROM dolt_log('feature');
SELECT * FROM dolt_log('main..feature');
-- commit_hash | committer | email | date | message
```

Two per-table virtual tables for time travel:

```sql
-- Every version of every row in the current HEAD ancestry
SELECT * FROM dolt_history_users WHERE id = 42;

-- Start from another branch, tag, or commit
SELECT * FROM dolt_history_users('feature') WHERE id = 42;

-- Select one exact committed snapshot
SELECT * FROM dolt_history_users
 WHERE commit_hash = dolt_hashof('feature');

-- The table as it existed at a specific commit / branch / tag
SELECT * FROM dolt_at_users('abc123...');
SELECT * FROM dolt_at_users('feature');
SELECT * FROM dolt_at_users('v1.0');
```

#### Blame (`dolt_blame_<table>`)

Most recent commit that set each live row's current value:

```sql
SELECT * FROM dolt_blame_users;
-- id | commit | commit_date | committer | email | message
```

First-parent walk from HEAD: blame updates when a row differs from the
first parent (or from the merge base at merges). Schema-only changes
(`ALTER TABLE ADD COLUMN`) do not update blame.

#### Schema History (`dolt_schemas`)

Views and triggers from the branch-scoped `sqlite_schema` (not ordinary
tables/indexes). Switches with `dolt_checkout`:

```sql
CREATE VIEW active_users AS SELECT * FROM users WHERE active = 1;
CREATE TRIGGER audit_users AFTER UPDATE ON users
  BEGIN INSERT INTO audit VALUES(new.id, 'updated'); END;
SELECT dolt_commit('-Am', 'Add view and trigger');

SELECT * FROM dolt_schemas;
-- type    | name         | fragment                                  | extra | sql_mode
-- view    | active_users | CREATE VIEW active_users AS SELECT ...    |       |
-- trigger | audit_users  | CREATE TRIGGER audit_users AFTER UPDATE...|       |
```

Use `sqlite_schema` or `dolt_schema_diff` for the full schema surface.

### Undoing on one branch

#### Reset

```sql
SELECT dolt_reset('--soft');   -- unstage all, keep working changes
SELECT dolt_reset('--hard');   -- discard all uncommitted changes
```

#### Revert

New commit that applies the inverse of a target commit onto HEAD
(message `Revert '<original message>'`). Cannot revert the initial commit.

```sql
SELECT dolt_revert('abc123...');
-- Returns new commit hash, or "Revert completed with N conflict(s)"
```

### Parallel development

#### Branching (Per-Session)

Each connection tracks its own active branch (and session view of HEAD /
staging). Uncommitted work belongs to the **branch**, not the connection —
see [Concurrency](#concurrency).

```sql
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
SELECT active_branch();
SELECT * FROM dolt_branches;
SELECT dolt_branch('-d', 'feature');
```

Open a branch at connect time via the database path (CLI, C API, or bindings):

```bash
./doltlite my.db@feature
./doltlite my.db/feature
```

```c
sqlite3_open("my.db@feature", &db);
```

##### Detached revisions

A tag, commit hash, or ancestor spec in the same qualified database path opens
an immutable historical snapshot:

```bash
./doltlite my.db/v1
./doltlite my.db/0123456789abcdef0123456789abcdef01234567
./doltlite 'my.db/main~1'
```

Detached state belongs only to that connection. `active_branch()` returns
`NULL`, `HEAD` names the selected commit, and the database is read-only. A peer
may advance branches or delete the selected tag without changing the open
snapshot. `dolt_checkout()` does not enter detached state; checking out an
existing branch from a detached connection reattaches that session and makes it
writable again. Closing and reopening the unqualified database uses its default
branch normally.

#### Tags

```sql
SELECT dolt_tag('v1.0');                  -- tag HEAD
SELECT dolt_tag('v1.0', 'abc123...');     -- tag a commit
SELECT dolt_tag('-d', 'v1.0');
SELECT * FROM dolt_tags;
```

#### Merge

Three-way, **row-level** merge into the current branch. Non-conflicting row
edits auto-merge; same-row edits become conflicts (see below).

```sql
SELECT dolt_merge('feature');
-- Returns commit hash (clean merge), or "Merge completed with N conflict(s)"
```

#### Merge Status

Always one row (`is_merging = 0` and other columns NULL when idle):

```sql
SELECT * FROM dolt_merge_status;
-- is_merging | source  | source_commit | target          | unmerged_tables
-- 1          | feature | 0f470f8440... | refs/heads/main | orders, users
```

`unmerged_tables` is the name-ordered union of tables with data conflicts,
constraint violations, or schema conflicts. Merge state is in the working set,
so other connections see it too; `source` is recovered from the branch at the
merge commit when possible, otherwise the commit hash.

#### Conflicts

```sql
SELECT * FROM dolt_conflicts;
-- table | num_conflicts
-- users | 2

-- Per-table rows: base_/our_/their_ columns, diff_types, dolt_conflict_id
SELECT * FROM dolt_conflicts_users;

DELETE FROM dolt_conflicts_users WHERE dolt_conflict_id = 5;  -- keep working value
SELECT dolt_conflicts_resolve('--ours', 'users');
SELECT dolt_conflicts_resolve('--theirs', 'users');

SELECT dolt_commit('-A', '-m', 'msg');
-- Error: "cannot commit: unresolved merge conflicts"
```

Conflicts are never durable: they exist only in the transaction that produced
them. Resolve there; `COMMIT` is refused while any remain, and an autocommit
merge that conflicts is rolled back whole. Nothing conflicted is left on disk
for a later connection. Dolt can commit a conflicted working set — this is a
deliberate divergence.

#### Constraint Violations on Merge

Merges apply cell-by-cell and do not run referential actions inline.
Post-merge, violating rows land in `dolt_constraint_violations_<table>`
(summary: `dolt_constraint_violations`).

```sql
SELECT * FROM dolt_constraint_violations;
-- table | num_violations
-- child | 1

SELECT violation_type, pk, violation_info
  FROM dolt_constraint_violations_child;
-- foreign key | 2 | {"Columns":["v1"],"ReferencedTable":"parent",...}

DELETE FROM dolt_constraint_violations_child WHERE pk = 2;
```

Types match Dolt: `foreign key`, `unique index`, `check constraint`. FK/CHECK
violators stay in the base table; unique-index losers (highest rowid) are
evicted into the violations vtable. `dolt_commit` refuses while any remain
(`--force` bypasses). Re-scan:
`SELECT dolt_verify_constraints([--all] [--output-only] [table...]);`.

#### Cherry-Pick

Apply one commit's changes onto the current branch (parent→commit diff as a
three-way merge; conflicts like `dolt_merge`). Ranges / multi-commit are not
supported.

```sql
SELECT dolt_cherry_pick('abc123...');
-- Returns new commit hash, or "Cherry-pick completed with N conflict(s)"
```

#### Rebase

Replay this branch onto an upstream. Atomic: conflict/error restores the
pre-rebase branch. Interactive (`-i`) edits a plan table before apply:

```sql
SELECT dolt_rebase('main');
-- "Successfully rebased and updated refs/heads/feat"

SELECT dolt_rebase('-i', 'main');
-- Working branch dolt_rebase_<orig> + dolt_rebase plan rows (default pick).
-- action: pick | drop | reword | squash | fixup; edit commit_message /
-- rebase_order with normal SQL.

UPDATE dolt_rebase SET action='drop'   WHERE commit_message='debug';
UPDATE dolt_rebase SET action='squash' WHERE commit_message='fixup';
SELECT dolt_rebase('--continue');
SELECT dolt_rebase('--abort');
```

#### Merge Base

```sql
SELECT dolt_merge_base('abc123...', 'def456...');
```

### Introspection and ops

#### Content-Addressed Hashes

```sql
-- Commit hash (branch, tag, raw hash, HEAD, HEAD~N / HEAD^N)
SELECT dolt_hashof('main');
SELECT dolt_hashof('HEAD~2');

-- Table root (one-arg form includes uncommitted working edits)
SELECT dolt_hashof_table('users');
SELECT dolt_hashof_table('users', 'main');

-- Whole catalog (moves when any table root or membership changes)
SELECT dolt_hashof_db();
SELECT dolt_hashof_db('HEAD');
```

Results are 40-char lowercase hex. `_table` / `_db` are history-independent:
identical `(key, value)` sets hash the same regardless of insert order or
branch. Property tests: `test/vc_oracle_hashof_test.sh`.

#### Garbage Collection

Stop-the-world mark-and-sweep over branches, tags, history, catalogs, and
prolly nodes; rewrites the file with only live chunks. Safe and idempotent.

```sql
SELECT dolt_gc();
-- "12 chunks removed, 45 chunks kept"
```

#### Remotes

Git-like push / fetch / pull / clone between databases.

##### Filesystem Remotes

```sql
SELECT dolt_remote('add', 'origin', 'file:///path/to/remote.doltlite');
SELECT dolt_push('origin', 'main');
SELECT dolt_clone('file:///path/to/source.doltlite');
SELECT dolt_clone('--lazy', 'file:///path/to/source.doltlite');
SELECT dolt_fetch('origin', 'main');
SELECT dolt_pull('origin', 'main');   -- fetch, then fast-forward or merge
SELECT * FROM dolt_remotes;
```

A lazy clone installs refs and records `origin` without copying the reachable
chunk graph. It enables origin-backed reads on its current connection. To
reopen the clone in another process, opt in before the B-tree opens:

```sh
doltlite 'file:/path/to/lazy.db?lazy_origin=1'
```

Missing chunks are fetched and cached as queries need them. Fetch and
fast-forward pull remain refs-only while the connection is origin-enabled. A
divergent lazy pull is refused until the store is fully materialized.
Opening without `lazy_origin=1` leaves cached chunks available, but an
uncached miss fails with a hash-named error instead of contacting `origin`.

`dolt_pull` fetches, then fast-forwards if the local branch is an ancestor
of the remote tip. If the current branch has diverged, it three-way merges
like `dolt_merge`. A non-current branch that is not a fast-forward is
refused.

##### HTTP Remotes

Same ops as filesystem remotes; the URL includes the database name:

```sql
SELECT dolt_remote('add', 'origin', 'http://myserver:8080/mydb.db');
SELECT dolt_push('origin', 'main');
SELECT dolt_clone('http://myserver:8080/mydb.db');
SELECT dolt_clone('--lazy', 'http://myserver:8080/mydb.db');
```

##### Remote Server (`doltlite-remotesrv`)

> [!WARNING]
> The server binds to `127.0.0.1` by default. Bound anywhere else, it warns at
> startup about each protection left unconfigured: `--cert`/`--key` for TLS, and
> `--auth-keys` plus `--audience` for authentication. These are independent — TLS encrypts but does
> not authenticate, and without `--auth-keys` every client that can reach the port
> may read the served databases *and push to them*. Configure both, or place the
> server behind a reverse proxy that provides equivalent TLS and authentication.

Standalone HTTP server for a directory of databases (`make doltlite-remotesrv`
in `build/`):

```
./doltlite-remotesrv -p 8080 /path/to/databases/
./doltlite-remotesrv -p 8080 --bind 0.0.0.0 /path/to/databases/   # all interfaces
./doltlite-remotesrv -p 8443 --bind 0.0.0.0 \
  --cert server.crt --key server.key \
  --auth-keys /path/to/authorized-keys --audience db.example.com \
  /path/to/databases/
```

Each `.db` is at `http://host:8080/filename.db` (or the HTTPS URL). Clients use
the system trust store (`DOLTLITE_CA_FILE` for a private CA); credentials live
in `~/.doltlite/creds` (`SELECT dolt_creds_new();`). Authorize one without
copying its private seed by exporting its public JWK directly into the server's
key directory:

```sql
SELECT dolt_creds('export', '<credential-id>', '/path/to/authorized-keys');
```

With no directory argument, `dolt_creds('export', '<credential-id>')` returns
the public JWK. The server rejects private credential files in `--auth-keys`.
Default HTTP timeout is 30s (`DOLTLITE_HTTP_TIMEOUT_MS`). Embeddable as
`doltliteServeAsync` in `doltlite_remotesrv.h`. Transfers are content-addressed.
JWT, TLS, and credential-store details:
[doc/doltlite/auth.md](doc/doltlite/auth.md).

#### Version String

```sql
SELECT dolt_version();
-- e.g. "v0.11.38" (from git describe at compile time)
```

## Using Existing SQLite Databases

Header-based auto-detect: stock SQLite files use the original B-tree engine;
everything else is prolly. Typical hybrid: versioned tables on the DoltLite
main DB, high-write operational tables on an attached stock SQLite file. Version
control applies only to the DoltLite-format main database.

```sql
ATTACH DATABASE '/path/to/events.sqlite' AS ops;
SELECT * FROM ops.events WHERE type='click';
SELECT * FROM threads;   -- main DB, no prefix
SELECT t.title, e.type
  FROM threads t
  JOIN ops.events e ON t.id = e.thread_id;

-- Migrate either direction
INSERT INTO threads SELECT * FROM ops.threads;
INSERT INTO ops.archive SELECT * FROM threads WHERE archived=1;
CREATE TABLE local_events AS SELECT * FROM ops.events;

DETACH DATABASE ops;
```

Auto-detect reads an existing file's header, so it cannot classify a file that
does not exist yet: a database created by DoltLite is DoltLite-format. To create
a stock SQLite file instead, open it with `doltlite_engine=sqlite`:

```
doltlite 'file:/path/to/new.sqlite?doltlite_engine=sqlite'
```

The parameter selects the engine for a database being created and is ignored
once the file has content, so it can never reinterpret an existing database.
`.backup` and `VACUUM INTO` apply it for you when the source is a stock file, so
their output is a stock file too.

`VACUUM` on a stock database rewrites pages as SQLite does; on a DoltLite
database it garbage-collects unreachable chunks. `.backup`/`.restore` and
`sqlite3_backup_*` work within either format, but not between them — there is no
defined conversion, so a mixed pair is refused rather than half-copied.

## Per-Session Branching Architecture

Each connection selects a branch independently and recovers that branch's
working set when it checks it out. There is no `dolt_stash`: checkout does not
shelve uncommitted work between branches. Writer serialization, snapshot pins,
and multiproc rules are spelled out under [Concurrency](#concurrency).

## SQLite Compatibility

DoltLite targets SQLite SQL semantics and uses the bundled SQLite version's
public C declarations and `sqlite3_*` symbol names. That is API-surface
compatibility, not a claim that storage-coupled APIs keep SQLite pager or file
format semantics.

For a DoltLite-format main database, the compatibility contract is:

- DoltLite uses its own on-disk format. Standard SQLite files are detected and
  routed to SQLite's original B-tree engine, but Dolt version-control features
  are available only on DoltLite-format databases.
- No SQLite rollback-journal, WAL, or shared-memory sidecar is created.
  `PRAGMA journal_mode` reports `wal` as a compatibility value and ignores
  requests to change it. All `PRAGMA wal_checkpoint` modes bridge to DoltLite
  garbage collection and report zero WAL frames.
- A transaction that writes more than one file-backed database is rejected
  with `atomic commit across multiple file-backed databases is not supported`
  and rolled back in full. This includes TEMP triggers that write `main` while
  changing an attached file. Single-file writes and transactions involving a
  `:memory:` attachment are supported.
- `PRAGMA auto_vacuum` reports `0`; attempts to enable it and
  `PRAGMA incremental_vacuum` are no-ops. `VACUUM` runs DoltLite garbage
  collection instead of rebuilding SQLite pages. File-backed `VACUUM INTO`
  writes a compacted DoltLite-format copy; `:memory:` as the destination is
  refused. `SQLITE_DBCONFIG_RESET_DATABASE` plus `VACUUM` empties the
  current branch working catalog (`sqlite_master` has no user objects);
  other branches and commit history remain, so `dolt_reset('--hard')`
  restores this branch from HEAD.
- Text is stored as UTF-8. Requests for a UTF-16 database encoding leave
  `PRAGMA encoding` at `UTF-8`.
- Application-defined collations registered with `sqlite3_create_collation*`
  are supported for expressions and unindexed columns. Persisted index keys,
  `UNIQUE` constraints, and non-integer primary keys using them are rejected
  because prolly sort keys cannot depend on application callbacks. An index
  may override such a column with `BINARY`, `NOCASE`, or `RTRIM`. Replacing one
  of those built-ins is rejected while a persisted index uses its name.
- A table with a non-`INTEGER PRIMARY KEY` is keyed by that primary key.
  `rowid` and `last_insert_rowid()` still work as a read-only SQL alias:
  a single integer PK is that value, otherwise a stable hash of the PK.
  `INSERT` and `UPDATE` of `rowid` fail with `no such column`, matching
  explicit `WITHOUT ROWID` — there is no stored `rowid` column. TEMP tables
  are not clustered, so those writes still work. An `INTEGER PRIMARY KEY`
  remains a writable rowid alias. Explicit `WITHOUT ROWID` tables have no
  `rowid` at all, matching SQLite.
- Those clustered primary keys are `NOT NULL`, matching SQLite
  `WITHOUT ROWID` tables. `PRAGMA table_info` reports `notnull=1` on the PK
  columns, and inserting NULL fails with `NOT NULL constraint failed`. SQLite
  rowid tables still allow NULL in a TEXT, `INT`, `INTEGER PRIMARY KEY DESC`,
  or composite PK. TEMP tables are not clustered and keep SQLite's nullable
  PK. An `INTEGER PRIMARY KEY` remains a rowid alias.
- `sqlite_master` / `sqlite_schema` is a projection of the prolly catalog,
  not a stored table of verbatim DDL. After a schema commit, `sql` is the
  canonical `CREATE` text (whitespace and quoting normalized) and row order
  follows the catalog, not insertion order. `CHECK` constraint error messages
  follow that canonical form. Query results and constraint enforcement are
  unchanged.
- `sqlite3_backup_step()` copies a file-backed DoltLite database, including an
  attached database, as one operation; its page-count argument is not
  incremental. File-backed and in-memory DoltLite databases can be copied in
  either direction.
- `sqlite3_serialize()` and `sqlite3_deserialize()` use a contiguous native
  DoltLite database image, including the commit graph, refs, and working sets.
  The image is an existing DoltLite storage-format file represented as bytes,
  not a SQLite page image or a SQL dump, and is not readable by stock SQLite.
- `dbstat` is not supported on a DoltLite-format database: the chunk store has
  no SQLite page layout. A scan fails with an error rather than reporting an
  empty database. `dbstat` on an attached stock SQLite file still walks pages.

The machine-readable contract and its test mapping live in
[`test/sqlite_compatibility_contract.tsv`](test/sqlite_compatibility_contract.tsv).
The inherited-suite backlog lives with the assertions it gates, in
[`test/known_testfixture_divergences.txt`](test/known_testfixture_divergences.txt):
each line names one assertion and carries its disposition as
`class=intentional|unsupported|harness|engine-gap`, plus `issue=<number>` where
one is required. Gates classified as `engine-gap` are bugs to fix, not
compatibility promises.

## Concurrency

DoltLite supports multiple connections and processes on one database file, but
it is not a free-for-all multi-writer server. Coordination is explicit: a
**graph lock** sidecar serializes durable writers, write transactions pin a
chunk-store snapshot, and multi-step version-control ops re-check HEAD under
the lock before advancing a branch tip.

For a DoltLite-format main database, the concurrency contract is:

- **Per-connection branch selection.** Each connection holds its own active
  branch and session view of HEAD and staging (see
  [Per-Session Branching](#per-session-branching-architecture)). The
  uncommitted working set belongs to the branch, so another connection that
  selects that branch recovers it. Two connections may sit on different
  branches of the same file at once.
- **One durable writer at a time.** A connection that holds an explicit write
  transaction owns the graph lock. A peer that tries to begin a concurrent
  write gets `SQLITE_BUSY` (or a retryable busy class) until the owner
  commits or rolls back. After the lock is free, the peer can retry
  successfully. In serialized threading mode, sequential calls from different
  threads may continue and finish the same transaction.
- **Snapshot-safe write upgrades.** A transaction that has established a read
  snapshot cannot upgrade to a writer after a peer advances the store; the
  upgrade returns `SQLITE_BUSY_SNAPSHOT` instead of mixing catalogs. Once a
  write transaction begins, it holds the graph lock and pins its snapshot
  until commit or rollback.
- **Readers stay live.** A reader can see already-committed data while another
  process holds an uncommitted write. An open iterator completes safely while
  another process runs GC. Readers do not create SQLite `-wal`/`-shm`
  sidecars.
- **Multi-process commits are CAS-safe.** A process that races `dolt_commit`
  against a peer either wins a clean tip advance or loses with a busy /
  conflict outcome. The loser's stale tip must not clobber the winner's
  commit. Sequential multiproc commits both land; forked SQL transaction
  writers leave consistent table and index state.
- **VC ops re-confirm HEAD under the lock.** Merge, cherry-pick, and revert use
  locked compare-and-advance; pull and rebase use operation-specific locked
  branch expectations. A peer commit between planning and ref update yields
  `SQLITE_BUSY` instead of a lost update.
- **Remote ref installs are serialized.** HTTP pushes refresh the remote refs
  under the graph lock before validating and installing either conditional or
  plain ref updates. A stale push is rejected instead of replacing a peer's
  ref update.
- **GC cooperates with writers.** `dolt_gc` / `VACUUM` may be deferred or
  report busy while a writer holds the graph lock; after the writer finishes,
  GC completes without dropping reachable data. Multiproc GC-vs-commit and
  GC-vs-GC races leave committed rows intact.
- **Conflicts are never durable.** A conflicted merge exists only inside the
  transaction that produced it. Commit is refused while conflicts remain;
  nothing conflicted is left on disk for a later connection to inherit.
  Constraint violations still persist (see Constraint Violations on Merge).

The machine-readable contract and its test mapping live in
[`test/concurrency_contract.tsv`](test/concurrency_contract.tsv). Multiproc and
multi-connection C harnesses (`multi_process_*`, `concurrent_*`) are the
behavioral oracles; the contract test asserts that every claim still points at
a real check or source needle. Nightly stress soaks those harnesses for hours;
PR CI runs them at shorter budgets via `test/run_c_tests.sh` and
`build-test`.

## Storage Format

DoltLite does **not** use the SQLite page format. Primary databases are a
single content-addressed chunk-store file (magic `DLTC` / `0x444C5443`) with
prolly-tree chunks, a WAL of chunk/root records, and refs for branches and
tags. Stock SQLite files are still detected and opened for ordinary SQL (see
[Using Existing SQLite Databases](#using-existing-sqlite-databases)); version
control requires a DoltLite-format file.

### Frozen format version 12 (beta)

Chunk-store version **12** is the on-disk format frozen for the DoltLite beta.
Version 12 includes every nested format written into the store, including:

| Layer | Constant | Value |
|---|---|---|
| Chunk-store header | `CHUNK_STORE_VERSION` | **12** |
| Working-set blob | `WS_FORMAT_VERSION` | **v5** |
| Catalog entries | `CATALOG_FORMAT_V5` | **0x46** |
| Refs blob | refs serializer | **v7** |
| Commit blob | `DOLTLITE_COMMIT_V2` | **v2** |

- **Writers** stamp version 12 and emit the nested formats above.
- **Readers** require an exact `CHUNK_STORE_VERSION` match. A different version
  returns `SQLITE_NOTADB`; there is no silent reinterpretation or automatic
  rewrite on open.
- Every file produced by a beta or later version-12 release remains readable
  and writable by later version-12 builds. An incompatible change to any nested
  format requires a `CHUNK_STORE_VERSION` bump even when that format has its own
  marker.
- Bumping `CHUNK_STORE_VERSION` requires updating this section, adding a corpus
  entry under [`test/format-corpus/`](test/format-corpus/), updating
  [`test/storage_format_contract.tsv`](test/storage_format_contract.tsv), and
  documenting whether version 12 is open-only, migrated, or refused.

The frozen version-12 file and its generation recipe live in
[`test/format-corpus/v12/`](test/format-corpus/v12/). The machine-readable
contract is
[`test/storage_format_contract.tsv`](test/storage_format_contract.tsv); CI runs
[`test/storage_format_contract_test.sh`](test/storage_format_contract_test.sh)
to verify the fixture, read and extend it, run GC, reject other header versions,
and keep evidence needles live.

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

Nightly DoltLite-versus-SQLite numbers:
[performance-report.md](performance-report.md). Per-release comparisons ship on
[GitHub releases](https://github.com/dolthub/doltlite/releases).

PR CI runs paired sysbench-style workloads (int / text / blob / composite PK)
and a short version-control latency suite against the PR base, with automatic
remeasurement on borderline regressions. Details live in
[`.github/workflows/benchmark.yml`](.github/workflows/benchmark.yml).

Complexity properties asserted in CI (`test/doltlite_perf.sh`,
`test/doltlite_structural.sh`):

- **O(log n)** point SELECT / UPDATE / DELETE by primary key
- **O(n log n)** bulk INSERT inside an explicit transaction
- **O(changes)** `dolt_diff` between commits (not proportional to table size)
- **Structural sharing** between versions (small edits add little file growth)
- **GC** reclaims unreachable chunks without dropping reachable data

## Running Tests

```bash
cd build
../configure && make

# DoltLite shell suites (branch / commit / merge / remotes / …)
bash ../test/run_doltlite_tests.sh

# C unit / multiproc / stress harnesses
bash ../test/run_c_tests.sh

# Upstream SQLite TCL suite (prolly engine) — one CI bucket
bash ../test/run_testfixture.sh "SQLite regression core-sql" 300 \
  $(tr '\n' ' ' < ../test/regression-buckets/core-sql.txt)

# Differential oracles (need stock sqlite3 and/or dolt on PATH)
bash ../test/sql_oracle_test.sh ./doltlite ./sqlite3
bash ../test/vc_oracle_workspace_test.sh ./doltlite dolt

# sqllogictest corpus (needs Fossil + corpus checkout)
bash ../test/run_sqllogictest.sh ./doltlite ./sqlite3 /path/to/sqllogictest
```

CI wiring, coverage floors, and full bucket lists are in
[`.github/workflows/test.yml`](.github/workflows/test.yml) and
[AGENTS.md](AGENTS.md). Contract suites
(`sqlite_compatibility_contract_test.sh`, `concurrency_contract_test.sh`,
`storage_format_contract_test.sh`) gate the README contracts above.

Inherited TCL allowlists:
[`test/known_testfixture_divergences.txt`](test/known_testfixture_divergences.txt),
[`test/known_testfixture_crashes.txt`](test/known_testfixture_crashes.txt).
Both carry a `class=` disposition per gate; the totals are pinned by
[`test/known_testfixture_exception_ratchet.txt`](test/known_testfixture_exception_ratchet.txt).

## Architecture

Same prolly-tree design as [Dolt](https://github.com/dolthub/dolt) —
content-addressed immutable nodes with rolling-hash boundaries — in C under
SQLite's `btree.h` seam. Engine code is `src/prolly_*.c` and `src/chunk_*.c`;
`dolt_*` SQL surfaces are `src/doltlite_*.c`; `src/prolly_btree.c` dispatches
the btree API.

Deeper comparison:
[Dolt vs DoltLite Storage](https://www.dolthub.com/blog/2026-07-08-dolt-doltlite-storage-comp/).
