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

### Diff

Row-level diff between any two commits, or working state vs HEAD:

```sql
SELECT * FROM dolt_diff('users');
SELECT * FROM dolt_diff('users', 'abc123...', 'def456...');
-- diff_type | rowid_val | from_value | to_value
```

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

Three-way merge of another branch into the current branch. Finds the common
ancestor via `dolt_merge_base`, merges catalogs, and creates a merge commit.
Fast-forwards when possible. Fails with an error if both branches modified the
same table (conflict).

```sql
SELECT dolt_merge('feature');
-- Returns the merge commit hash, or "Already up to date"
```

### Merge Base

Find the common ancestor of two commits:

```sql
SELECT dolt_merge_base('abc123...', 'def456...');
-- Returns the ancestor commit hash
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
| `doltlite_merge.c` | Three-way catalog merge logic |
| `doltlite_ancestor.c` | Common ancestor search, `dolt_merge_base` |
| `doltlite_commit.h` | Commit object serialization/deserialization |
| `doltlite_ancestor.h` | Ancestor-finding API |
