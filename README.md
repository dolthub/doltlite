# Doltlite

A SQLite fork that replaces the B-tree storage engine with a content-addressed
prolly tree, enabling Git-like version control on a SQL database. Everything
above SQLite's `btree.h` interface (VDBE, query planner, parser) is untouched.
Everything below it -- the pager, WAL, and on-disk format -- is replaced with a
prolly tree engine backed by an append-only chunk store.

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
| `chunk_store.c` | File-backed, append-only chunk storage |
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
