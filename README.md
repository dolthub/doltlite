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

### History (dolt_history_&lt;table&gt;)

Time-travel query showing every version of every row across all commits:

```sql
SELECT * FROM dolt_history_users;
-- rowid_val | value | commit_hash | committer | commit_date

-- How many times was row 42 changed?
SELECT count(*) FROM dolt_history_users WHERE rowid_val = 42;

-- What did the table look like at a specific commit?
SELECT * FROM dolt_history_users WHERE commit_hash = 'abc123...';
```

### Point-in-Time Queries (AS OF)

Read a table as it existed at any commit, branch, or tag.
Returns the real table columns (not generic blobs):

```sql
SELECT * FROM dolt_at_users('abc123...');
-- id | name | email (same columns as the actual table)

SELECT * FROM dolt_at_users('feature');
SELECT * FROM dolt_at_users('v1.0');

-- Compare current vs historical
SELECT count(*) FROM users;                     -- 100
SELECT count(*) FROM dolt_at_users('v1.0');    -- 42
```

### Diff

Row-level diff between any two commits, or working state vs HEAD:

```sql
SELECT * FROM dolt_diff('users');
SELECT * FROM dolt_diff('users', 'abc123...', 'def456...');
-- diff_type | rowid_val | from_value | to_value
```

### Schema Diff

Compare schemas between any two commits, branches, or tags:

```sql
SELECT * FROM dolt_schema_diff('v1.0', 'v2.0');
-- table_name | from_create_stmt | to_create_stmt | diff_type

-- Shows tables added, dropped, or modified (schema changed)
-- Also detects new indexes and views
```

### Audit Log (dolt_diff_&lt;table&gt;)

Full history of every change to every row, across all commits:

```sql
SELECT * FROM dolt_diff_users;
-- diff_type | rowid_val | from_value | to_value |
--   from_commit | to_commit | from_commit_date | to_commit_date

-- Every INSERT, UPDATE, DELETE that was ever committed is here
SELECT diff_type, rowid_val, to_commit FROM dolt_diff_users
  WHERE rowid_val = 42;
```

One `dolt_diff_<table>` virtual table is automatically created for each
user table. The table walks the full commit history and diffs each
consecutive pair of commits.

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
-- name | hash | is_current

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

#### How It Works

Content-addressed chunk transfer — only sends chunks the remote doesn't already
have. BFS traversal of the DAG with batch `HasMany` pruning.

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
SELECT * FROM dolt_diff('config');
```

## Per-Session Branching Architecture

Each connection gets its own `Btree` and `BtShared` (not shared across
connections). Doltlite stores the session's branch name, HEAD commit hash,
and staged catalog hash in the `Btree` struct.

- Each connection can be on a different branch.
- `dolt_checkout` reloads the table registry from the target branch's catalog.
- All commit graph mutations (`dolt_commit`, `dolt_merge`, `dolt_reset`,
  `dolt_branch`, `dolt_tag`, push, pull) are serialized via an exclusive
  file-level lock. Under that lock, the connection refreshes from disk
  before writing, preventing silent data loss from concurrent commits.
- DML (INSERT/UPDATE/DELETE) concurrency between multiple connections
  is not fully characterized. The PagerShim does not implement SQLite's
  file-level write locking, so concurrent write transactions are not
  serialized the way standard SQLite serializes them. Single-connection
  use is fully supported. Multi-connection concurrent DML behavior is
  an area of active investigation.

## Performance

### Sysbench OLTP Benchmarks: Doltlite vs SQLite

Doltlite is a drop-in replacement for SQLite, so the natural question is: what
does version control cost?

Every PR runs a [sysbench-style benchmark](test/sysbench_compare.sh) comparing
doltlite against stock SQLite on 23 OLTP workloads. Results are posted as a PR
comment.

#### Reads

| Test | SQLite (ms) | Doltlite (ms) | Multiplier |
|------|-------------|---------------|------------|
| oltp_point_select | 41 | 45 | 1.10 |
| oltp_range_select | 28 | 64 | 2.29 |
| oltp_sum_range | 12 | 14 | 1.17 |
| oltp_order_range | 5 | 6 | 1.20 |
| oltp_distinct_range | 6 | 7 | 1.17 |
| oltp_index_scan | 6 | 7 | 1.17 |
| select_random_points | 19 | 35 | 1.84 |
| select_random_ranges | 6 | 8 | 1.33 |
| covering_index_scan | 10 | 24 | 2.40 |
| groupby_scan | 47 | 54 | 1.15 |
| index_join | 6 | 8 | 1.33 |
| index_join_scan | 3 | 6 | 2.00 |
| types_table_scan | 10 | 12 | 1.20 |
| table_scan | 1 | 1 | 1.00 |
| oltp_read_only | 183 | 234 | 1.28 |

#### Writes

| Test | SQLite (ms) | Doltlite (ms) | Multiplier |
|------|-------------|---------------|------------|
| oltp_bulk_insert | 25 | 35 | 1.40 |
| oltp_insert | 17 | 36 | 2.12 |
| oltp_update_index | 40 | 129 | 3.23 |
| oltp_update_non_index | 30 | 46 | 1.53 |
| oltp_delete_insert | 39 | 69 | 1.77 |
| oltp_write_only | 16 | 30 | 1.88 |
| types_delete_insert | 22 | 26 | 1.18 |
| oltp_read_write | 116 | 158 | 1.36 |

_10K rows, file-backed, Linux x64 (GitHub Actions). Run `test/sysbench_compare.sh` to reproduce._

**Reads are within 1-2.4x across all workloads.** The VDBE, query planner,
parser, and all upper layers are untouched SQLite — only the storage engine is
replaced. Point selects, range queries, aggregates, GROUP BY, and full table
scans are all close to parity. The composite oltp_read_only benchmark is 1.28x.

**Index scans are 1-2.4x.** Secondary index scans (oltp_index_scan,
covering_index_scan) use sort key materialization — pre-computed memcmp-sortable
keys stored alongside the original SQLite record. This enables O(1) key
comparison in the prolly tree. index_join_scan is now 2x (down from 46x in
earlier versions) thanks to optimized cursor descent and structural sharing
improvements.

**Writes are within 1-3.2x.** Edits accumulate in a skip list and flush once at
commit time using a Dolt-style cursor-path-stack algorithm. Only the root-to-leaf
path is rewritten per edit; unchanged subtrees are structurally shared.

**oltp_update_index is 3.2x** (down from 380x in the initial implementation and
12x in previous releases). This benchmark does 10K updates to an indexed column
in one transaction. Improvements came from sort key materialization (memcmp
replaces field-by-field comparison), IndexMoveto scan limits, savepoint
structural sharing, and deferred MutMap flush for ephemeral tables.

### Algorithmic Complexity

All numbers below have automated assertions in CI (`test/doltlite_perf.sh` and `test/doltlite_structural.sh`).

- **O(log n) Point Operations** -- SELECT, UPDATE, and DELETE by primary key are O(log n), essentially constant time from 1K to 1M rows. Tested and asserted at 1K, 100K, and 1M rows.
- **O(n log n) Bulk Insert** -- Bulk INSERT inside BEGIN/COMMIT scales as O(n log n). 1M rows inserts in ~2 seconds. CTE-based inserts also scale linearly (5M rows in 11s).
- **O(changes) Diff** -- `dolt_diff` between two commits is proportional to the number of changed rows, not the table size. A single-row diff on a 1M-row table takes the same time as on a 1K-row table (~30ms).
- **Structural Sharing** -- The prolly tree provides structural sharing between versions. Changing 1 row in a 10K-row table adds only 1.9% to the file size (5.2KB on 273KB). Branch creation with 1 new row adds ~10% overhead.
- **Garbage Collection** -- `dolt_gc()` reclaims orphaned chunks. Deleting a branch with 1000 unique rows and running GC reclaims 53% of file size. GC is idempotent and preserves all reachable data.

## Running Tests

### SQLite Tcl Test Suite

87,000+ SQLite test cases pass with 0 correctness failures.

```bash
# Install Tcl (macOS)
brew install tcl-tk

# Configure with Tcl support
cd build
../configure --with-tcl=$(brew --prefix tcl-tk)/lib

# Build testfixture
make testfixture OPTS="-L$(brew --prefix)/lib"

# Run a single test file
./testfixture ../test/select1.test

# Run with timeout
perl -e 'alarm(60); exec @ARGV' ./testfixture ../test/select1.test

# Count passes
./testfixture ../test/func.test 2>&1 | grep -c "Ok$"
```

Stock SQLite testfixture for comparison:

```
make testfixture DOLTLITE_PROLLY=0 USE_AMALGAMATION=1
```

### Doltlite Shell Tests

31 test suites covering all features:

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

### SQL Logic Test Suite

Doltlite passes 100% of the
[sqllogictest](https://www.sqlite.org/sqllogictest/) suite — the same
5.7 million-statement correctness corpus that SQLite itself uses. Every PR
runs the full suite in CI, comparing Doltlite's results against stock SQLite
as a reference. Zero failures, zero errors.

The test works by building the official
[sqllogictest C runner](https://www.sqlite.org/sqllogictest/) twice — once
linked against stock SQLite, once against Doltlite's amalgamation — and
running every `.test` file through both in `--verify` mode. Any result
divergence from stock SQLite is a failure.

```bash
# Build both runners and run the full suite (requires Fossil)
fossil clone https://www.sqlite.org/sqllogictest/ /tmp/sqllogictest.fossil
mkdir -p /tmp/sqllogictest && cd /tmp/sqllogictest && fossil open /tmp/sqllogictest.fossil

# Build stock runner (reference)
cd src
gcc -O2 -DSQLITE_NO_SYNC=1 -DSQLITE_THREADSAFE=0 \
    -DSQLITE_OMIT_LOAD_EXTENSION -c md5.c sqlite3.c
gcc -O2 -o sqllogictest-stock sqllogictest.c md5.o sqlite3.o -lpthread -lm

# Build doltlite runner (replace amalgamation)
cp /path/to/doltlite/build/sqlite3.c sqlite3.c
cp /path/to/doltlite/build/sqlite3.h sqlite3.h
gcc -O2 -DSQLITE_NO_SYNC=1 -DSQLITE_THREADSAFE=0 \
    -DSQLITE_OMIT_LOAD_EXTENSION -c sqlite3.c
gcc -O2 -o sqllogictest-doltlite sqllogictest.c md5.o sqlite3.o -lpthread -lm -lz

# Run the suite
bash test/run_sqllogictest.sh \
    sqllogictest-doltlite sqllogictest-stock /tmp/sqllogictest/test
```

### Concurrent Branch Test

A C test that opens two connections on different branches and verifies they see
different data:

```bash
cd build
# Compile (adjust flags as needed)
gcc -o concurrent_branch_test ../test/concurrent_branch_test.c \
    -I../src -L. -lsqlite3 -lpthread
./concurrent_branch_test
```

## Architecture

### Prolly Tree Engine

| File | Purpose |
|------|---------|
| `prolly_hash.c/h` | xxHash32 content addressing |
| `prolly_node.c/h` | Binary node format (serialization, field access) |
| `prolly_cache.c/h` | LRU node cache |
| `prolly_cursor.c/h` | Tree cursor (seek, next, prev) |
| `prolly_mutmap.c/h` | Skip list write buffer for pending edits |
| `prolly_chunker.c/h` | Rolling hash tree builder |
| `prolly_mutate.c/h` | Merge-flush edits into tree |
| `prolly_diff.c/h` | Tree-level diff (drives `dolt_diff`) |
| `prolly_arena.c/h` | Arena allocator for tree operations |
| `prolly_btree.c` | `btree.h` API implementation (main integration point) |
| `sortkey.c/h` | Sort key encoding for memcmp-sortable index keys |
| `chunk_store.c` | Single-file content-addressed chunk storage |
| `pager_shim.c` | Pager facade (satisfies pager API without page-based I/O) |
| `btree_orig_*.c` | Original SQLite btree compiled with renamed symbols (for ATTACH) |
| `btree_orig_api.c/h` | Bridge API between prolly dispatch and original btree |

### Doltlite Feature Files

| File | Purpose |
|------|---------|
| `doltlite.c` | `dolt_add`, `dolt_commit`, `dolt_reset`, `dolt_merge`, registration |
| `doltlite_status.c` | `dolt_status` virtual table |
| `doltlite_log.c` | `dolt_log` virtual table |
| `doltlite_diff.c` | `dolt_diff` table-valued function |
| `doltlite_branch.c` | `dolt_branch`, `dolt_checkout`, `active_branch`, `dolt_branches` |
| `doltlite_tag.c` | `dolt_tag`, `dolt_tags` |
| `doltlite_merge.c` | Three-way catalog and row-level merge |
| `doltlite_conflicts.c` | `dolt_conflicts`, `dolt_conflicts_resolve` |
| `doltlite_ancestor.c` | Common ancestor search, `dolt_merge_base` |
| `doltlite_commit.h` | Commit object serialization/deserialization |
| `doltlite_ancestor.h` | Ancestor-finding API |
| `doltlite_history.c` | `dolt_history_<table>` virtual table |
| `doltlite_at.c` | `dolt_at_<table>` point-in-time query |
| `doltlite_schema_diff.c` | `dolt_schema_diff` virtual table |
| `doltlite_gc.c` | `dolt_gc` garbage collection |
| `doltlite_remote.c` | Remote management (`dolt_remote`, `dolt_push`, `dolt_fetch`, `dolt_clone`) |
| `doltlite_http_remote.c` | HTTP remote client (BSD sockets) |
| `doltlite_remotesrv.c` | Standalone HTTP server for remotes |

## Dolt vs Doltlite: Storage Engine Comparison

Doltlite implements the same prolly tree architecture as
[Dolt](https://github.com/dolthub/dolt), but adapted for SQLite's constraints
and C implementation. The core idea is identical — content-addressed immutable
nodes with rolling-hash-determined boundaries — but the details differ
significantly.

### Prolly Tree

Both use prolly trees (probabilistic B-trees) where node boundaries are
determined by a rolling hash over key bytes rather than fixed fan-out. This gives
content-defined chunking: identical subtrees produce identical hashes regardless
of where they appear, enabling structural sharing between versions.

| | Dolt | Doltlite |
|--|------|----------|
| **Language** | Go | C (inside SQLite) |
| **Node format** | FlatBuffers | Custom binary (header + offset arrays + data regions) |
| **Hash function** | xxhash, 20 bytes | xxHash32 with 5 seeds packed into 20 bytes |
| **Chunk target** | ~4KB | 4KB (512B min, 16KB max) |
| **Boundary detection** | Rolling hash, `(hash & pattern) == pattern` | Same algorithm |

### Key Encoding

**Dolt** uses a purpose-built tuple encoding: fields are serialized as contiguous
bytes with a trailing offset array and field count. Keys sort lexicographically,
so comparison is a single `memcmp`.

**Doltlite** uses sort key materialization for index (BLOBKEY) entries. Each
SQLite record is converted to a memcmp-sortable byte string at insert time:
integers and floats are encoded as IEEE 754 doubles with sign normalization,
text and blobs use NUL-byte escaping with double-NUL terminators. The sort key
is stored as the prolly tree key; the original SQLite record is stored as the
value (for reads). This enables `memcmp` comparison in the tree at the cost of
~2x index entry size. For INTKEY tables (rowid tables), keys are 8-byte
little-endian integers — comparison is trivial.

### Tree Mutation

**Dolt** uses a chunker with `advanceTo` boundary synchronization. Two cursors
track the old tree and new tree simultaneously. When the chunker fires a boundary
that aligns with an old tree node boundary, it skips the entire unchanged
subtree. This handles splits, merges, and boundary drift naturally within a
single bottom-up pass.

**Doltlite** uses a cursor-path-stack approach. For each edit, it seeks from root
to leaf, clones the leaf into a node builder, applies edits, serializes the new
leaf (with rolling-hash re-chunking for overflow/underflow), and rewrites
ancestors by walking up the path stack. Unchanged subtrees are never loaded. A
hybrid strategy falls back to a full O(N+M) merge-walk when the edit count is
large relative to tree size.

Both achieve O(M log N) for sparse edits. Dolt's approach is more elegant for
boundary maintenance; doltlite's is simpler to implement in C and integrates
naturally with SQLite's cursor-based API.

### Chunk Store

**Dolt** uses the Noms Block Store (NBS) format with multiple table files
organized into generations (oldgen/newgen). Writers append new table files;
readers see consistent snapshots. This enables MVCC-like concurrency with
optimistic locking at the manifest level.

**Doltlite** uses a single file with three regions: a 168-byte manifest header
at offset 0, a compacted chunk data region with sorted index (written by GC),
and a WAL region at the end of the file (append-only journal of new chunks).
Normal commits append to the WAL region at EOF. GC rewrites the entire file
with all chunks compacted (empty WAL region). Concurrency uses file-level
locking for serialization.

### Commits and Metadata

**Dolt** stores commits as FlatBuffer-serialized objects forming a DAG (directed
acyclic graph) with multiple parents for merge commits. Commits include a parent
closure for O(1) ancestor queries and a height field for efficient traversal.

**Doltlite** stores commits as custom binary objects forming a DAG with
multi-parent support (merge commits record both parents). Each branch has an
associated WorkingSet chunk that stores staged catalog and merge state
independently. The catalog hash is purely data-derived (no runtime metadata),
enabling O(1) dirty checks via hash comparison. Branches and tags are stored in
a serialized refs chunk referenced by the manifest.

### Garbage Collection

Both use mark-and-sweep: walk all reachable chunks from branches, tags, and
commit history, then remove everything else. Dolt rewrites live data into new
table files and deletes old ones. Doltlite compacts in-place by rewriting the
single database file with only live chunks.
