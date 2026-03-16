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
- **20,314 SQLite test cases passing** across 27 test files
- 22 of 27 test files at **100% pass rate** (within 60s timeout per file)
- File persistence working (data survives close+reopen)
- Bulk inserts up to 100K rows (10K rows in 0.16s)

SQLite test results (27 files):

| Test File | Result |
|-----------|--------|
| func | 15,030/15,030 |
| date | 1,683/1,683 |
| expr | 660/660 |
| auth | 376/376 |
| trans | 328/328 |
| where | 317/317 |
| select1 | 191/191 |
| join | 191/191 |
| conflict | 147/147 |
| update | 140/140 |
| savepoint | 139/139 |
| cse | 124/124 |
| select4 | 123/123 |
| index | 120/120 |
| alter | 119/119 |
| view | 118/118 |
| attach | 112/112 |
| select3 | 90/90 |
| insert | 83/83 |
| subquery | 81/81 |
| delete | 67/67 |
| vacuum | 46/46 |
| select2 | 20/20 |
| coalesce | 9/9 |
| build, collate, fkey, trigger | timeout on first test |

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
