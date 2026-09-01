# Host-provided chunk sources

This note records the design constraints for the read-only host chunk-source
seam. A host may satisfy a local chunk miss, but it does not participate in
writes or ref publication.

## Existing receive path

Clone and fetch use `doltliteSyncChunks`. It walks the reachable graph in
batches of 256 hashes, filters chunks already present at the destination,
fetches with `xGetChunks` when available, verifies every fetched address, and
stages missing chunks through `chunkStorePut`. `doltliteLocalAsRemote` takes the
graph lock on the first put, force-refreshes the store, and retains the lock
until one `chunkStoreCommit` publishes the whole transfer. Pull uses this fetch
path before advancing or merging refs.

That staging and commit machinery is the write-through path to reuse. Its lock
envelope is not suitable for demand reads: after the first put, a multi-batch
clone may perform more network reads while holding the graph lock.

## Write-through policy

A registered source gets a persistent, source-free cache-writer `ChunkStore`
handle for the same file. The live query store is never used to stage fetched
chunks. This separation prevents a cache commit from publishing chunks or refs
already staged by a SQL write transaction, and prevents a force-refresh from
changing a pinned read snapshot to a peer's refs.

On a miss, the engine calls the host before taking the cache writer's graph
lock and verifies the returned bytes against the requested hash through the
normal chunk-read verification path. It then locks and force-refreshes the
cache writer, rechecks which hashes are now local, stages the remaining bytes
with `chunkStorePut`, and issues one `chunkStoreCommit`. A scalar `xGet` is one
single-chunk commit batch; one `xGetMany` result is one multi-chunk commit
batch. The recheck makes duplicate cross-process fetches harmless.

Fetched bytes also enter a bounded per-connection memory cache. The live store
can therefore finish its pinned snapshot without adopting the cache writer's
new manifest, and prefetched chunks are immediately visible to the query. A
later normal transaction-boundary refresh discovers the durable append.
Read-only stores skip the cache writer and use only this memory cache. The same
applies when there is no stable writable file, including memory and buffer
stores.

The cache writer is opened without `SQLITE_OPEN_CREATE`, has no chunk source of
its own, and lives until the registration is cleared or its connection closes.
Moved or replaced files make write-through read-only rather than allowing the
writer to create or adopt another file. The writer refreshes before every
batch, so its manifest retains current peer refs while the live store remains
pinned to its own view.

There is no cross-process fetch coordination. The graph lock protects only the
local append and manifest publication, never the host callback. Arbitrary
accumulation across callbacks is intentionally avoided: there is no independent
safe flush boundary, it would retain unbounded bytes, and staging on the live
store would mix cache data with transaction state.

## Calls made while locked

`chunkStoreGet` does not acquire the graph lock. Ordinary read transactions do
not hold it, but write transactions and graph-changing operations do, and their
tree walks may still encounter a chunk miss. Calling a host or trying to take
the cache writer's lock from such a path would either block peer processes or
deadlock behind the live store.

An already cached chunk remains readable while the live store holds the graph
lock. A new source miss while that lock is held is refused with a busy result;
the engine never invokes `xGet` or `xGetMany` there. Hosts that intend to write
against a lazy store must first warm the chunks those writes will inspect. This
keeps the seam read-only and avoids adding cross-process network coordination.

## Full and sparse chunks

The source contract always returns the complete logical byte string, including
any zero tail. A normal `chunkStoreGet` miss consumes it directly. A
`chunkStoreGetSparse` miss falls through the same path and reports
`nDataPhys == nData`; sparse-aware node parsing therefore treats the returned
buffer as an ordinary full chunk. Write-through may store that full form.
Locally produced sparse zero-tail chunks retain the existing sparse read and
write paths. Both representations hash to the same logical bytes.

## Refs-only bootstrap

`chunkStoreInstallRefsBlob` already supplies the storage-layer bootstrap and
can replace the refs blob when the remote advances. Old cached chunks remain
valid because their addresses are content hashes.

The SQL B-tree open path currently loads the branch working set or head commit
and then the catalog before a caller can register a source through an API that
takes `sqlite3 *`. A refs-only file cannot therefore be reopened lazily without
one more read-path change: store open must retain the refs and defer graph
hydration until a source can be registered or the graph is first accessed.
With no source registered, that deferred access must preserve the existing
`SQLITE_NOTFOUND` behavior. Installing refs into an already open fresh store is
the compatible same-connection bootstrap; reopening that store depends on the
deferred hydration change.

## Interface and build limits

Registration is per attached database. The source object and its context are
owned by the host and must outlive registration; returned chunk buffers use
SQLite's allocator and become engine-owned. Source `NOTFOUND`, corruption, and
I/O failures affect only the read statement, and partially returned batches
are freed without being persisted.

The feature is guarded by `DOLTLITE_ENABLE_CHUNK_SOURCE`, defaulting to 1. A
compiled-out build and a connection with no registered source retain the old
read path. Public declarations must remain C89-compatible, and any new source
file must be included by `tool/mksqlite3c.tcl` so the amalgamation and
compile-out tests cover the same implementation.

## Deferred follow-ups

- A built-in source backed by the configured `origin` remote.
- Richer prefetch scheduling beyond the first honest interior-node batch site.
- WASM host functions, including async and JSPI plumbing.
