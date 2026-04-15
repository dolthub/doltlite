# MutMap Design Notes

This document defines the behavioral contract of `ProllyMutMap` as it
exists today and the staged refactor plan for splitting table and index
edit tracking behind the same API.

## Purpose

`ProllyMutMap` is the in-memory edit buffer that sits between SQLite's
btree layer and immutable prolly roots. It has to serve several roles at
once:

- exact lookup of pending edits
- ordered iteration for merged cursor scans and flush
- savepoint rollback / release
- clone / snapshot handoff during mid-savepoint flush
- stable cursor-visible identity for exact-hit positioning

That combination is what has made the subsystem both performance-
sensitive and regression-prone.

## Current Contract

The public contract in `src/prolly_mutmap.h` is what `prolly_btree.c`
depends on. Any replacement implementation must preserve these
behaviors.

### Data Model

A mutmap stores pending edits keyed by:

- `intKey` when `isIntKey` is true
- blob key bytes otherwise

Each key maps to exactly one current entry:

- `PROLLY_EDIT_INSERT` with a value payload
- `PROLLY_EDIT_DELETE` as a tombstone

The mutmap is not a visible row set. It is an edit set against an
immutable base tree, so tombstones are first-class entries.

### Lookup

- `prollyMutMapFindRc()` returns the current entry for an exact key or
  `NULL` if absent.
- lookup must surface allocation failure and not collapse it into “not
  found”.

### Ordered Semantics

- `prollyMutMapEntryAt(i)` returns the `i`th key in sorted key order.
- `prollyMutMapOrderIndexFromEntry(e)` returns the sorted position of an
  exact entry.
- iteration (`First`, `Seek`, `Next`, `Last`) is in the same total key
  order as `prollyCompareKeys()`.
- exact lookup and ordered iteration must agree on membership.

### Savepoints

- `PushSavepoint(level)` advances the logical savepoint level for new
  writes.
- `RollbackToSavepoint(level)` must restore the exact edit set that was
  visible when that savepoint began.
- `ReleaseSavepoint(level)` must preserve visible contents while
  collapsing ownership of edits to the parent level.
- statement-savepoint release must stay cheap. This is why the current
  implementation has `levelBase`.

### Flush / Snapshot Interaction

Mid-savepoint flushes move a pending mutmap into a savepoint snapshot and
replace it with a fresh empty mutmap. Any replacement implementation
must preserve:

- clone fidelity
- later rollback through saved pending snapshots
- continued attribution of subsequent writes to the same active
  savepoint level

## Current Implementations

Today there is still one concrete implementation, but the mutmap now
records a logical kind:

- `PROLLY_MUTMAP_KIND_TABLE`
- `PROLLY_MUTMAP_KIND_INDEX`

Both kinds currently route through the same legacy array-backed
implementation. This is intentional. The kind split exists first so we
can:

- document the intended workload split
- differential-test semantics before changing behavior
- let `prolly_btree` stop assuming there is only one implementation

## Planned Split

### Table MutMap

The table path will stay close to the current implementation at first.
It already behaves correctly, and it is tightly coupled to savepoint
snapshot behavior.

Near-term goal:

- cleanup and invariant hardening only

### Index MutMap

The index path is the real performance target.

Desired properties:

- cheap exact lookup
- cheap ordered iteration without global sort materialization
- stable identity for savepoint undo
- no rank-conversion hot path just to step a cursor

The intended replacement is an ordered mutable structure, most likely a
skip-list-like edit set paired with exact-key lookup.

## Refactor Plan

1. Lock down behavior with differential tests.
2. Keep the current implementation behind kind-aware dispatch.
3. Add a new index implementation behind the same API.
4. Run the new index implementation in shadow / differential mode.
5. Cut non-table roots over only after:
   - exact lookup matches
   - ordered iteration matches
   - savepoint rollback/release matches
   - clone / snapshot semantics match

## Invariants Worth Asserting

- savepoint release does not change visible contents
- rollback restores exact contents and exact iteration order
- exact lookup and ordered iteration agree for every key
- clone preserves contents, order, and savepoint metadata
- statement-savepoint release is sublinear in the number of entries

## Test Strategy

The mutmap-specific differential harness should compare:

- sorted-mode mutmap
- lazy-order mutmap
- a simple model of pending edits and savepoint snapshots

for randomized sequences of:

- insert
- delete
- find
- ordered iteration
- clone
- push savepoint
- release savepoint
- rollback savepoint

That harness is the safety rail for the later index implementation
rewrite.
