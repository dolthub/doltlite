# DoltLite Invariants

These are the properties that must hold after every operation. Violations mean
data loss, corruption, or undefined behavior. Every test should check these.
Every code change should preserve them.

## Storage Invariants

**S1. Atomicity.** After a commit, either ALL chunks for that commit are on disk
or NONE are. There is no state where a root record references chunks that
don't exist in the file. The WAL root record at EOF is the single source of
truth — it is written last, before fsync.

**S2. Durability.** After `chunkStoreCommit` returns SQLITE_OK, the data
survives a process crash or power loss. This means: all WAL records are
written before fsync, and fsync completes before the function returns. No
write to an earlier file offset may occur before fsync (this would create a
window where a crash leaves the file referencing data that isn't on disk).

**S3. Chunk integrity.** Every chunk retrievable by `chunkStoreGet` must
hash to the address used to retrieve it. A hash mismatch means the file is
corrupt and must be reported, never silently ignored.

**S4. Refs are the root of all data.** The refs chunk maps branch names to
commit hashes. If refs cannot be loaded or deserialized, the database is
corrupt — ALL branches, tags, and data are unreachable. Failure to load refs
must be treated as SQLITE_CORRUPT, never silently ignored.

**S5. WAL replay is complete.** On open, the entire WAL region is replayed.
There is no "fast path" that skips replay. The manifest at offset 0 may be
stale — the WAL root record is authoritative.

## Commit Invariants

**C1. No silent commit loss.** If `dolt_commit` returns a hash, that commit
exists and is the tip of its branch. If another connection committed to the
same branch since this connection last saw it, `dolt_commit` must return an
error, not silently overwrite the other connection's commit.

**C2. Commit chain integrity.** Every commit's parent hash points to a commit
that exists in the store (except the initial commit, which has an empty parent).
The `dolt_log` walk from any branch tip must terminate at the initial commit
without encountering a missing chunk.

**C3. Catalog consistency.** Every commit's catalog hash points to a catalog
chunk that exists and deserializes correctly. Every table root in the catalog
points to a valid prolly tree root chunk.

## Concurrency Invariants

**X1. Session isolation at commit time.** Each connection has its own session
HEAD. A connection can only commit if its session HEAD matches the current
branch tip. This prevents the "lost update" problem where one connection's
commit silently overwrites another's.

**X2. Snapshot pinning.** While a read transaction is active
(`snapshotPinned == 1`), the connection's view of the chunk store does not
change, even if another connection commits. This prevents torn reads.

**X3. Known limitation: shared in-memory state.** Multiple in-process
connections share a single BtShared and its prolly tree state. Concurrent
DML from multiple connections to the same table can corrupt the in-memory
tree. This is a known architectural limitation (see issue #250). For now,
DoltLite is validated for single-connection use only.

## Error Handling Invariants

**E1. No swallowed errors at layer boundaries.** When `chunkStoreGet`,
`csDeserializeRefs`, or `chunkStoreOpen` return an error, the caller must
propagate it. Silent success after a failure is a data loss bug.

**E2. Corrupt data is reported, not worked around.** If a chunk is missing,
a hash doesn't match, or a record can't be parsed, return SQLITE_CORRUPT.
Do not return an empty result, a default value, or SQLITE_OK.

## Layering Invariants

**L1.** `chunk_store.*` depends on nothing DoltLite-specific.
**L2.** `prolly_*.c` depends on `chunk_store` but not on `doltlite_*.c`.
**L3.** `prolly_*.c` never uses `sqlite3_prepare_v2`, `sqlite3_step`, or
other high-level SQL APIs.
**L4.** `doltlite_*.c` accesses BtShared internals only through accessor
functions, never via pointer arithmetic.

Enforced by `test/lint_layers.sh`, which runs during `make`.
