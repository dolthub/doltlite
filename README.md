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

SQLite requires virtual-table column names to be unique. If a generated user
column in `dolt_diff_<table>`, `dolt_history_<table>`, or
`dolt_conflicts_<table>` collides case-insensitively with a metadata column,
the metadata keeps its Dolt name and the user column receives the first
available numeric suffix (`_1`, `_2`, …).

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

The file is the longest existing database-file prefix, so branch names may
contain `/` (for example, `my.db/feature/parser`).

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

-- Table root and its indexes (one-arg form includes uncommitted working edits)
SELECT dolt_hashof_table('users');
SELECT dolt_hashof_table('users', 'main');

-- One index on its own
SELECT dolt_hashof_index('users_by_email');
SELECT dolt_hashof_index('users_by_email', 'main');

-- Whole catalog (moves when any table root or membership changes)
SELECT dolt_hashof_db();
SELECT dolt_hashof_db('HEAD');
```

Results are 40-char lowercase hex. `_table` / `_db` are history-independent:
identical `(key, value)` sets hash the same regardless of insert order or
branch. Property tests: `test/vc_oracle_hashof_test.sh`.

An index is part of the table it indexes. `dolt_hashof_index` hashes one
index, `dolt_hashof_table` folds in every index of that table, and a change to
an index shows in `dolt_status` as a modification of the table it belongs to,
never as a row of its own. `REINDEX` rewrites what the rows imply, so on a
database whose indexes match its rows it changes no hash and leaves
`dolt_status` clean; a hash that moves across `REINDEX` means the stored index
did not match its rows.

#### Garbage Collection

Stop-the-world mark-and-sweep over branches, tags, history, catalogs, and
prolly nodes; rewrites the file with only live chunks. Safe and idempotent.

```sql
SELECT dolt_gc();
-- "12 chunks removed, 45 chunks kept"
```

#### Remotes

Git-like push / fetch / pull / clone between databases, over the filesystem
or HTTP.

```sql
SELECT dolt_remote('add', 'origin', 'file:///path/to/remote.doltlite');
SELECT dolt_push('origin', 'main');
SELECT dolt_pull('origin', 'main');
SELECT dolt_clone('http://myserver:8080/mydb.db');
```

Remote semantics and lazy clones: [remotes.md](doc/doltlite/remotes.md).
Serving databases with `doltlite-remotesrv`, which binds to localhost until
TLS and authentication are configured: [remotesrv.md](doc/doltlite/remotesrv.md).

#### Version String

```sql
SELECT dolt_version();
-- e.g. "v0.11.38" (from git describe at compile time)
```

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
