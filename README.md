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

```
cd build
../configure
make
./doltlite :memory:
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

-- Commit with author
SELECT dolt_commit('-m', 'Fix data', '--author', 'Alice <alice@example.com>');
```

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

Each connection tracks its own active branch. Two connections can be on
different branches, seeing different data, at the same time. Branch state
(active branch name, HEAD commit, staged catalog hash) lives in the `Btree`
struct (per-connection). The chunk store in `BtShared` is shared across
connections.

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

## Per-Session Branching Architecture

SQLite's `Btree` struct is per-connection. Doltlite stores each session's
branch name, HEAD commit hash, and staged catalog hash there. The underlying
chunk store (`BtShared`) is shared. This means:

- Two connections to the same database file can be on different branches.
- Each sees its own tables, schema, and data based on its branch's HEAD.
- `dolt_checkout` reloads the table registry from the target branch's catalog.
- Writers are serialized by SQLite's existing locking, so branch switches are
  safe.

The `concurrent_branch_test.c` test demonstrates two connections on different
branches querying different data simultaneously.

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
| oltp_point_select | 95 | 61 | 0.64 |
| oltp_range_select | 35 | 38 | 1.09 |
| oltp_sum_range | 12 | 13 | 1.08 |
| oltp_order_range | 7 | 6 | 0.86 |
| oltp_distinct_range | 5 | 7 | 1.40 |
| oltp_index_scan | 8 | 32 | 4.00 |
| select_random_points | 19 | 33 | 1.74 |
| select_random_ranges | 9 | 7 | 0.78 |
| covering_index_scan | 12 | 35 | 2.92 |
| groupby_scan | 37 | 42 | 1.14 |
| index_join | 6 | 12 | 2.00 |
| index_join_scan | 3 | 138 | 46.00 |
| types_table_scan | 12 | 12 | 1.00 |
| table_scan | 3 | 1 | 0.33 |
| oltp_read_only | 216 | 209 | 0.97 |

#### Writes

| Test | SQLite (ms) | Doltlite (ms) | Multiplier |
|------|-------------|---------------|------------|
| oltp_bulk_insert | 18 | 20 | 1.11 |
| oltp_insert | 14 | 18 | 1.29 |
| oltp_update_index | 29 | 335 | 11.55 |
| oltp_update_non_index | 21 | 25 | 1.19 |
| oltp_delete_insert | 29 | 161 | 5.55 |
| oltp_write_only | 14 | 129 | 9.21 |
| types_delete_insert | 16 | 17 | 1.06 |
| oltp_read_write | 75 | 192 | 2.56 |

_10K rows, file-backed, macOS ARM. Run `test/sysbench_compare.sh` to reproduce._

**Reads are at parity or faster for most workloads.** The VDBE, query planner,
parser, and all upper layers are untouched SQLite — only the storage engine is
replaced. Point selects, range queries, aggregates, and the composite
oltp_read_only benchmark are all within 1-2x of stock SQLite. Several benchmarks
are actually faster than SQLite on file-backed databases because the prolly tree's
content-addressed cache avoids redundant disk reads.

**Index scans are 3-4x.** Secondary index scans (oltp_index_scan,
covering_index_scan) use sort key materialization — pre-computed memcmp-sortable
keys stored alongside the original SQLite record. This enables O(1) key
comparison in the prolly tree but doubles index entry size. index_join_scan
(46x) remains the main outlier due to this storage overhead on join-heavy
workloads.

**Most writes are within 1-3x of SQLite.** Edits accumulate in a skip list and
flush once at commit time using a Dolt-style cursor-path-stack algorithm. Only
the root-to-leaf path is rewritten per edit; unchanged subtrees are structurally
shared.

**oltp_update_index is 12x.** This benchmark does 10K updates to an indexed
column in one transaction. Sort key materialization replaced the expensive
field-by-field record comparison with memcmp, and a scan limit in IndexMoveto
prevents O(N) linear scans through deleted entries. Combined, these brought the
multiplier from 380x down to ~12x.

### Algorithmic Complexity

All numbers below have automated assertions in CI (`test/doltlite_perf.sh` and `test/doltlite_structural.sh`).

- **O(log n) Point Operations** -- SELECT, UPDATE, and DELETE by primary key are O(log n), essentially constant time from 1K to 1M rows. Tested and asserted at 1K, 100K, and 1M rows.
- **O(n log n) Bulk Insert** -- Bulk INSERT inside BEGIN/COMMIT scales as O(n log n). 1M rows inserts in ~2 seconds.
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

Feature-specific shell tests in `test/`:

```bash
bash test/doltlite_commit.sh
bash test/doltlite_staging.sh
bash test/doltlite_diff.sh
bash test/doltlite_reset.sh
bash test/doltlite_branch.sh
bash test/doltlite_merge.sh
bash test/doltlite_tag.sh
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
