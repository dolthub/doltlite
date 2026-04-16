# Doltlite Architecture

This document covers doltlite's internal design and how it differs from
[Dolt](https://github.com/dolthub/dolt), which doltlite borrows its prolly
tree idea from.

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
little-endian integers — comparison is trivial. TEXT columns declared with a
`COLLATE NOCASE` or `COLLATE RTRIM` collation have their bytes preprocessed
(ASCII case folding / trailing-space stripping) before encoding, so two values
that are equal under the collation produce identical sort keys.

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
independently, plus a per-branch working catalog tracked in a separate working
state chunk (referenced by the manifest). This allows connections on different
branches to each find their own catalog on refresh without reading a stale
catalog from another branch. The catalog hash is purely data-derived (no
runtime metadata), enabling O(1) dirty checks via hash comparison. Branches
and tags are stored in a serialized refs chunk referenced by the manifest.

### Garbage Collection

Both use mark-and-sweep: walk all reachable chunks from branches, tags, and
commit history, then remove everything else. Dolt rewrites live data into new
table files and deletes old ones. Doltlite compacts in-place by rewriting the
single database file with only live chunks.
