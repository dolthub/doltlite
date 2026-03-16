# Doltlite

A SQLite fork that replaces the B-tree storage engine with a content-addressed
prolly tree. This enables structural sharing and Git-like version control
semantics -- the foundation for building a version-controlled SQL database.

SQLite's architecture has a clean internal boundary at `btree.h`, roughly 60
functions that separate the VDBE (virtual machine, query planner, parser) from
the storage engine. Doltlite leaves everything above that boundary untouched.
Everything below it -- the pager, WAL, and on-disk file format -- is replaced
with a prolly tree engine.

## How It Works

The replacement storage layer has three key components:

- **Prolly tree nodes** -- Content-addressed, structurally shared tree nodes.
  Two trees that contain mostly the same data share most of their nodes. This
  is what makes diff, merge, and branch operations possible.

- **Chunk store** -- A single-file, append-only store. Chunks are addressed by
  hash. A manifest at a known location tracks the root.

- **Rolling hash boundaries** -- Tree node splits are determined by a rolling
  hash over key content, not by fixed page sizes. This produces balanced trees
  whose structure is determined by content rather than insertion order.

## Building

```
cd build
../configure
make
./doltlite :memory:
```

The default `make` builds `doltlite` with the prolly engine. No build flags
needed.

To build stock SQLite instead:

```
make DOLTLITE_PROLLY=0 sqlite3
```

## Verifying the Engine

```
SELECT doltlite_engine();
-- returns: prolly
```

## Status

- 8,500+ lines of new C code across 23 files
- All ~60 btree.h functions implemented
- **87,000+ SQLite test cases passing** across 90+ test files
- File persistence working (data survives close+reopen)
- Bulk inserts up to 100K rows (10K rows in 0.16s)

Largest passing test files: select9 (36,716), e_expr (16,618), func (15,030),
randexpr1 (2,600), in2 (1,999), date (1,683), printf (1,410), fkey2 (1,216),
enc4 (1,114), expr (660), types2 (398), auth (376), trans (328), where (317).

## Running Tests

The test suite uses SQLite's `testfixture` (Tcl-based). You need Tcl dev libraries
installed.

```
# Install Tcl (macOS)
brew install tcl-tk

# Configure with Tcl support
cd build
../configure --with-tcl=$(brew --prefix tcl-tk)/lib

# Build testfixture
make testfixture OPTS="-L$(brew --prefix)/lib"

# Run a single test file
./testfixture ../test/select1.test

# Run a test file with timeout (some tests are slow)
perl -e 'alarm(60); exec @ARGV' ./testfixture ../test/select1.test

# Count passing tests in a file
./testfixture ../test/func.test 2>&1 | grep -c "Ok$"

# Run multiple test files and count passes
for test in insert select1 delete update trans where expr func; do
  echo -n "$test: "
  perl -e 'alarm(60); exec @ARGV' ./testfixture ../test/${test}.test 2>&1 | grep -c "Ok$"
done
```

To build stock SQLite's testfixture for comparison:

```
make testfixture DOLTLITE_PROLLY=0 USE_AMALGAMATION=1
```

## Architecture

Key source files in the prolly engine:

| File | Purpose |
|------|---------|
| `prolly_hash.c` | xxHash32 content addressing |
| `prolly_node.c` | Binary node format (serialization, field access) |
| `prolly_cache.c` | LRU node cache |
| `chunk_store.c` | File-backed chunk storage |
| `prolly_cursor.c` | Tree cursor (seek, next, prev) |
| `prolly_mutmap.c` | Skip list write buffer for pending edits |
| `prolly_chunker.c` | Rolling hash tree builder |
| `prolly_mutate.c` | Merge-flush edits into tree |
| `prolly_btree.c` | btree.h API implementation (the main integration point) |
| `pager_shim.c` | Pager facade (satisfies pager API without page-based I/O) |

The VDBE, query planner, parser, and all SQL-level functionality are unchanged
from upstream SQLite. The entire modification is below the btree.h interface.
