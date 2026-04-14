<p align="center">
  <img src="doltlite_logo.webp" alt="Doltlite" width="600">
</p>

# Doltlite

A SQLite fork that replaces the B-tree storage engine with a content-addressed
prolly tree, enabling Git-like version control on a SQL database. Everything
above SQLite's `btree.h` interface (VDBE, query planner, parser) is untouched.
Everything below it -- the pager and on-disk format -- is replaced with a
prolly tree engine backed by a single-file content-addressed chunk store.

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

## Using as a C Library

Doltlite is designed as a drop-in replacement for SQLite. It uses the same
`sqlite3.h` header and `sqlite3_*` API, so existing C programs work without
code changes — just link against `libdoltlite` instead of `libsqlite3` to get
version control. The build produces `libdoltlite.a` (static) and
`libdoltlite.dylib`/`.so` (shared) with the full prolly tree engine and all
Dolt functions included.

```bash
cd build
../configure
make doltlite-lib   # builds libdoltlite.a and libdoltlite.dylib/.so
```

Compile and link your program:

```bash
# Static link (recommended — single binary, no runtime deps)
gcc -o myapp myapp.c -I/path/to/build libdoltlite.a -lpthread -lz

# Dynamic link
gcc -o myapp myapp.c -I/path/to/build -L/path/to/build -ldoltlite -lpthread -lz
```

The API is the standard [SQLite C API](https://sqlite.org/cintro.html) —
`sqlite3_open`, `sqlite3_exec`, `sqlite3_prepare_v2`, etc. Dolt features are
called as SQL functions (`dolt_commit`, `dolt_branch`, `dolt_merge`, ...) and
virtual tables (`dolt_log`, `dolt_diff_<table>`, `dolt_history_<table>`, ...).

### Quickstart Examples

Complete working examples that demonstrate commits, branches, merges,
point-in-time queries, diffs, and tags. Each example does the same thing
in a different language.

**C** ([`examples/quickstart.c`](examples/quickstart.c)) — based on the
[SQLite quickstart](https://sqlite.org/quickstart.html):

```bash
cd build
gcc -o quickstart ../examples/quickstart.c -I. libdoltlite.a -lpthread -lz
./quickstart
```

**Python** ([`examples/quickstart.py`](examples/quickstart.py)) — uses the
standard `sqlite3` module, zero code changes:

```bash
cd build
LD_PRELOAD=./libdoltlite.so python3 ../examples/quickstart.py
```

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

### Staging and Committing

```sql
-- Stage specific tables or all changes
SELECT dolt_add('users');
SELECT dolt_add('-A');

-- Commit staged changes
SELECT dolt_commit('-m', 'Add users table');

-- Stage and commit in one step
SELECT dolt_commit('-A', '-m', 'Initial commit');

-- Shorthand (compound flags, like git commit -am)
SELECT dolt_commit('-am', 'Initial commit');

-- Commit with author
SELECT dolt_commit('-m', 'Fix data', '--author', 'Alice <alice@example.com>');
```

### Configuration

```sql
-- Set committer name and email (per-session)
SELECT dolt_config('user.name', 'Tim Sehn');
SELECT dolt_config('user.email', 'tim@dolthub.com');

-- Read current config
SELECT dolt_config('user.name');
-- Tim Sehn
```

All commit-creating operations (`dolt_commit`, `dolt_merge`, `dolt_cherry_pick`,
`dolt_revert`) use these values. The `--author` flag on `dolt_commit` overrides
the session config for a single commit. Config is per-connection and not
persisted — set it at the start of each session.

### Status and History

```sql
-- Working/staged changes
SELECT * FROM dolt_status;
-- table_name | staged | status
-- users      | 1      | modified
-- orders     | 0      | new table

-- Commit history
SELECT * FROM dolt_log;
-- commit_hash | committer | email | date | message
```

### History and Point-in-Time Queries

Two per-table virtual tables for time travel:

```sql
-- Every version of every row across all commits
SELECT * FROM dolt_history_users WHERE rowid_val = 42;

-- The table as it existed at a specific commit / branch / tag
SELECT * FROM dolt_at_users('abc123...');
SELECT * FROM dolt_at_users('feature');
SELECT * FROM dolt_at_users('v1.0');
```

### Diff

Several ways to ask what changed:

```sql
-- Which tables changed across the commit history?
SELECT * FROM dolt_diff WHERE table_name = 'users';

-- Row- and cell-level change counts between two refs (commits, branches, tags)
SELECT * FROM dolt_diff_stat('v1.0', 'HEAD');
SELECT * FROM dolt_diff_stat('v1.0', 'HEAD', 'users');  -- narrow to one table

-- High-level per-table classification: added / dropped / renamed / modified
SELECT * FROM dolt_diff_summary('v1.0', 'HEAD');

-- Schema-level diff (tables, views, indexes)
SELECT * FROM dolt_schema_diff('v1.0', 'v2.0');

-- Row-level history for a single table: every INSERT / UPDATE / DELETE
-- that was ever committed, with real per-column to_/from_ pairs plus
-- commit metadata and a diff_type. One virtual table per user table,
-- auto-registered on each commit. Filter by to_commit (including the
-- special 'WORKING' value for staged + working changes) or from_commit
-- to narrow to a specific slice.
SELECT * FROM dolt_diff_users;
-- to_id | to_name | to_email | to_commit | to_commit_date |
--   from_id | from_name | from_email | from_commit | from_commit_date |
--   diff_type

SELECT diff_type, to_name, to_email, to_commit
  FROM dolt_diff_users
  WHERE to_id = 42;

SELECT * FROM dolt_diff_users WHERE to_commit = 'WORKING';  -- staged+working
```

### Schemas (dolt_schemas)

Projection of views and triggers from `sqlite_schema`. This is the Dolt-style
surface for browsing non-table schema objects. Because `sqlite_schema` lives
in the branch-scoped catalog, `dolt_schemas` is version-controlled per branch
just like user tables — switching branches with `dolt_checkout` will show the
views and triggers defined on that branch:

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

Rows are filtered to `type IN ('view','trigger')` — ordinary tables and
indexes are not reported here. Use `sqlite_schema` directly (or
`dolt_schema_diff`) if you need the full schema surface.

### Blame (dolt_blame_&lt;table&gt;)

For each live row, the most recent commit that introduced its current
value:

```sql
SELECT * FROM dolt_blame_users;
-- id | commit | commit_date | committer | email | message
```

Walks history first-parent from HEAD; at linear commits a row is
blamed if it differs from first-parent, at merge commits if it
differs from the merge base. Schema-only changes (`ALTER TABLE ADD
COLUMN`) don't update blame.

### Reset

```sql
SELECT dolt_reset('--soft');   -- unstage all, keep working changes
SELECT dolt_reset('--hard');   -- discard all uncommitted changes
```

### Branching (Per-Session)

Each connection tracks its own active branch. Branch state (active branch
name, HEAD commit, staged catalog hash) lives in the `Btree` struct
(per-connection). Each connection gets its own `BtShared` and chunk store.

```sql
-- Create a branch at current HEAD
SELECT dolt_branch('feature');

-- Switch to it (fails if uncommitted changes exist)
SELECT dolt_checkout('feature');

-- See current branch
SELECT active_branch();

-- List all branches
SELECT * FROM dolt_branches;
-- name | hash | latest_committer | latest_committer_email
-- | latest_commit_date | latest_commit_message | remote | branch | dirty

-- Delete a branch
SELECT dolt_branch('-d', 'feature');
```

### Tags

Immutable named pointers to commits:

```sql
SELECT dolt_tag('v1.0');                  -- tag HEAD
SELECT dolt_tag('v1.0', 'abc123...');     -- tag specific commit
SELECT dolt_tag('-d', 'v1.0');            -- delete tag
SELECT * FROM dolt_tags;                  -- list tags
```

### Merge

Three-way merge of another branch into the current branch. Merges at the
**row level** — non-conflicting changes to different rows of the same table
are auto-merged. Conflicts (same row modified on both branches) are detected
and stored for resolution.

```sql
SELECT dolt_merge('feature');
-- Returns commit hash (clean merge), or "Merge completed with N conflict(s)"
```

### Conflicts

View and resolve merge conflicts:

```sql
-- View which tables have conflicts (summary)
SELECT * FROM dolt_conflicts;
-- table_name | num_conflicts
-- users      | 2

-- View individual conflict rows for a table
SELECT * FROM dolt_conflicts_users;
-- base_rowid | base_value | our_rowid | our_value | their_rowid | their_value

-- Resolve individual conflicts by deleting them (keeps current working value)
DELETE FROM dolt_conflicts_users WHERE base_rowid = 5;

-- Or resolve all conflicts for a table at once
SELECT dolt_conflicts_resolve('--ours', 'users');   -- keep our values
SELECT dolt_conflicts_resolve('--theirs', 'users'); -- take their values

-- Commit is blocked while conflicts exist
SELECT dolt_commit('-A', '-m', 'msg');
-- Error: "cannot commit: unresolved merge conflicts"
```

### Cherry-Pick

Apply the changes from a specific commit onto the current branch:

```sql
SELECT dolt_cherry_pick('abc123...');
-- Returns new commit hash, or "Cherry-pick completed with N conflict(s)"
```

Cherry-pick works by computing the diff between the target commit and its
parent, then applying that diff to the current HEAD as a three-way merge.
Conflicts are handled the same way as `dolt_merge`.

### Revert

Create a new commit that undoes the changes from a specific commit:

```sql
SELECT dolt_revert('abc123...');
-- Returns new commit hash, or "Revert completed with N conflict(s)"
```

Revert computes the inverse of the target commit's changes and applies
them to the current HEAD. The new commit message is
`Revert '<original message>'`. Cannot revert the initial commit.

### Rebase

Replay the current branch's commits on top of an upstream:

```sql
SELECT dolt_rebase('main');
-- "Successfully rebased and updated refs/heads/feat"
```

Atomic: any conflict or error during the replay restores the branch
to its pre-rebase state. Interactive mode lets you edit the plan
before applying it:

```sql
SELECT dolt_rebase('-i', 'main');
-- Creates a working branch dolt_rebase_<orig> and a dolt_rebase
-- table with one row per commit (default action: pick). Edit with
-- normal SQL: action in ('pick','drop','reword','squash','fixup'),
-- change commit_message, or reorder with fractional rebase_order.

UPDATE dolt_rebase SET action='drop'   WHERE commit_message='debug';
UPDATE dolt_rebase SET action='squash' WHERE commit_message='fixup';
SELECT dolt_rebase('--continue');  -- apply the edited plan
SELECT dolt_rebase('--abort');     -- throw the working branch away
```

### Garbage Collection

Remove unreachable chunks from the store to reclaim space:

```sql
SELECT dolt_gc();
-- "12 chunks removed, 45 chunks kept"
```

Stop-the-world mark-and-sweep: walks all branches, tags, commit
history, catalogs, and prolly tree nodes to find reachable chunks,
then rewrites the file with only live data. Safe and idempotent.

### Merge Base

Find the common ancestor of two commits:

```sql
SELECT dolt_merge_base('abc123...', 'def456...');
```

### Remotes

Doltlite supports Git-like remotes for pushing, fetching, pulling, and cloning
between databases.

#### Filesystem Remotes

```sql
-- Add a remote
SELECT dolt_remote('add', 'origin', 'file:///path/to/remote.doltlite');

-- Push a branch
SELECT dolt_push('origin', 'main');

-- Clone a remote database
SELECT dolt_clone('file:///path/to/source.doltlite');

-- Fetch updates
SELECT dolt_fetch('origin', 'main');

-- Pull (fetch + fast-forward)
SELECT dolt_pull('origin', 'main');

-- List remotes
SELECT * FROM dolt_remotes;
```

#### HTTP Remotes

```sql
-- Add an HTTP remote (URL includes database name)
SELECT dolt_remote('add', 'origin', 'http://myserver:8080/mydb.db');

-- All operations work identically to file:// remotes
SELECT dolt_push('origin', 'main');
SELECT dolt_clone('http://myserver:8080/mydb.db');
SELECT dolt_fetch('origin', 'main');
SELECT dolt_pull('origin', 'main');
```

#### Remote Server (`doltlite-remotesrv`)

Doltlite includes a standalone HTTP server for serving databases over the
network. Build it alongside doltlite:

```
cd build
make doltlite-remotesrv
```

Start serving a directory of databases:

```
./doltlite-remotesrv -p 8080 /path/to/databases/
```

Every `.db` file in that directory becomes accessible at
`http://host:8080/filename.db`. The server supports push, fetch, pull, and
clone — multiple clients can collaborate on the same databases.

The server is also embeddable as a library (`doltliteServeAsync` in
`doltlite_remotesrv.h`) for applications that want to host remotes in-process.
Transfers are content-addressed — only chunks the remote doesn't already
have are sent.

## Using Existing SQLite Databases

Doltlite can ATTACH standard SQLite databases alongside its own prolly-tree
storage. This lets you keep versioned tables in doltlite and high-write
operational tables in standard SQLite, queried through a single connection.

Doltlite detects the file format automatically from the header — no
configuration needed. Standard SQLite files route to SQLite's original B-tree
engine; everything else uses the prolly tree.

### Basic ATTACH

```sql
-- Attach a standard SQLite database
ATTACH DATABASE '/path/to/events.sqlite' AS ops;

-- Query it (prefix table names with the alias)
SELECT * FROM ops.events WHERE type='click';

-- Main db tables need no prefix
SELECT * FROM threads;

-- Detach when done
DETACH DATABASE ops;
```

### Cross-Database JOINs

```sql
-- Join doltlite (versioned) tables with SQLite (attached) tables
SELECT t.title, e.type
FROM threads t
JOIN ops.events e ON t.id = e.thread_id;
```

### Migrating Data Between Formats

```sql
-- Copy from SQLite into doltlite (now versioned)
INSERT INTO threads SELECT * FROM ops.threads;

-- Copy from doltlite into SQLite (for export)
INSERT INTO ops.archive SELECT * FROM threads WHERE archived=1;

-- One-step copy with CREATE TABLE...AS
CREATE TABLE local_events AS SELECT * FROM ops.events;
```

### Hybrid Storage Pattern

Use doltlite for tables that benefit from version control, and standard SQLite
for high-throughput tables that don't need history:

```sql
-- Main DB: doltlite (versioned)
CREATE TABLE config(key TEXT PRIMARY KEY, val TEXT);
SELECT dolt_commit('-am', 'Add config table');

-- Attached: standard SQLite (high-write, no versioning overhead)
ATTACH DATABASE 'telemetry.sqlite' AS tel;
CREATE TABLE tel.events(seq INTEGER PRIMARY KEY, kind TEXT, payload TEXT);

-- Hot write path goes to standard SQLite
INSERT INTO tel.events VALUES(1, 'pageview', '{"url":"/home"}');

-- Analytics spans both databases
SELECT c.val, count(e.seq)
FROM config c
JOIN tel.events e ON e.kind = c.key
GROUP BY c.key;

-- Version control only applies to main db
SELECT * FROM dolt_diff WHERE table_name='config';
```

## Per-Session Branching Architecture

Each connection gets its own `Btree` / `BtShared` pair and independently
tracks branch name, HEAD commit, and staged catalog hash, so different
connections can sit on different branches at the same time. Each branch's
working catalog lives in its own chunk, so one branch's autocommit can
never corrupt another branch's reads. Writes and commit-graph mutations
are serialized through an exclusive file-level lock (matching SQLite's
standard behavior); reads are concurrent.

## Performance

### Sysbench OLTP Benchmarks: Doltlite vs SQLite

Doltlite is a drop-in replacement for SQLite, so the natural question is: what
does version control cost?

Every PR runs a [sysbench-style benchmark](test/sysbench_compare.sh) comparing
doltlite against stock SQLite on 23 OLTP workloads, with a 3× ceiling enforced
by CI. The per-release numbers (reads + writes table) are published with each
release on the [GitHub releases page](https://github.com/timsehn/doltlite/releases).
Run `test/sysbench_compare.sh` to reproduce locally.

### Algorithmic Complexity

All numbers below have automated assertions in CI (`test/doltlite_perf.sh` and `test/doltlite_structural.sh`).

- **O(log n) Point Operations** -- SELECT, UPDATE, and DELETE by primary key are O(log n), essentially constant time from 1K to 1M rows. Tested and asserted at 1K, 100K, and 1M rows.
- **O(n log n) Bulk Insert** -- Bulk INSERT inside BEGIN/COMMIT scales as O(n log n). 1M rows inserts in ~2 seconds. CTE-based inserts also scale linearly (5M rows in 11s).
- **O(changes) Diff** -- `dolt_diff` between two commits is proportional to the number of changed rows, not the table size. A single-row diff on a 1M-row table takes the same time as on a 1K-row table (~30ms).
- **Structural Sharing** -- The prolly tree provides structural sharing between versions. Changing 1 row in a 10K-row table adds only 1.9% to the file size (5.2KB on 273KB). Branch creation with 1 new row adds ~10% overhead.
- **Garbage Collection** -- `dolt_gc()` reclaims orphaned chunks. Deleting a branch with 1000 unique rows and running GC reclaims 53% of file size. GC is idempotent and preserves all reachable data.

## Running Tests

### SQLite Tcl Test Suite

87,000+ upstream SQLite test cases pass with 0 correctness failures.
Build `testfixture` and run `bash test/run_testfixture.sh` (CI runs the
full sweep on every PR; see `.github/workflows/test.yml` for the
invocation).

### Doltlite Shell Tests

39 test suites covering all features:

```bash
# Run all suites
cd build
bash ../test/run_doltlite_tests.sh

# Run individual suites
bash ../test/doltlite_parity.sh          # SQLite compatibility (110 tests)
bash ../test/doltlite_commit.sh          # Commits and log
bash ../test/doltlite_staging.sh         # Add, status, staging
bash ../test/doltlite_branch.sh          # Branching and checkout
bash ../test/doltlite_merge.sh           # Three-way merge
bash ../test/doltlite_attach_sqlite.sh   # ATTACH standard SQLite databases
```

### Differential Oracle Tests

Doltlite ships a suite of differential oracle tests that run the same SQL
through doltlite and stock sqlite3 and compare results byte-for-byte. Each
script is focused on a SQL feature surface — savepoints, foreign keys,
UPSERT, generated columns, WITHOUT ROWID, large BLOBs at chunk boundaries,
ATTACH cross-engine queries, TEMP tables, triggers, dot-commands, FTS5 —
and scenarios are written to hit storage-layer edge cases. The oracles
drove most of the correctness fixes in recent releases.

```bash
cd build
bash ../test/sql_oracle_test.sh ./doltlite ./sqlite3
bash ../test/oracle_savepoints_test.sh ./doltlite ./sqlite3
bash ../test/oracle_foreign_keys_test.sh ./doltlite ./sqlite3
bash ../test/oracle_upsert_test.sh ./doltlite ./sqlite3
bash ../test/oracle_generated_columns_test.sh ./doltlite ./sqlite3
bash ../test/oracle_without_rowid_test.sh ./doltlite ./sqlite3
bash ../test/oracle_large_blobs_test.sh ./doltlite ./sqlite3
bash ../test/oracle_attach_test.sh ./doltlite ./sqlite3
bash ../test/oracle_temp_tables_test.sh ./doltlite ./sqlite3
bash ../test/oracle_triggers_test.sh ./doltlite ./sqlite3
bash ../test/oracle_fts5_test.sh ./doltlite ./sqlite3
```

A separate CI job builds the same suite with
`-fsanitize=address,undefined` and runs every oracle under ASan/UBSan to
catch memory and undefined-behavior bugs before they reach master.

### SQL Logic Test Suite

100% pass on the [sqllogictest](https://www.sqlite.org/sqllogictest/) suite
— the same 5.7M-statement corpus SQLite itself uses — verified against
stock SQLite as the reference. CI runs the full suite on every PR; run
`bash test/run_sqllogictest.sh` locally (requires Fossil for the upstream
corpus).

### Concurrent Branch Tests

C tests that verify cross-branch isolation — two connections on different
branches both write and read without corrupting each other:

```bash
cd build
gcc -o cross_branch_test ../test/cross_branch_test.c \
    -I. -I../src libdoltlite.a -lz -lpthread
./cross_branch_test
```

## Architecture

Doltlite implements the same prolly tree design as
[Dolt](https://github.com/dolthub/dolt) — content-addressed immutable
nodes with rolling-hash-determined boundaries — adapted for SQLite's
constraints and C implementation. The prolly tree engine lives in
`src/prolly_*.c`, the feature-level implementations of `dolt_*` SQL
functions and vtables live in `src/doltlite_*.c`, and `src/prolly_btree.c`
is the integration point where prolly dispatches against SQLite's
`btree.h` API.

See [`docs/architecture.md`](docs/architecture.md) for a side-by-side
of doltlite and Dolt covering node format, key encoding, tree mutation,
chunk store, commit graph, and GC.
