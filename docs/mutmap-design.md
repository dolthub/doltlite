# MutMap Design Notes

This document defines the behavioral contract of `ProllyMutMap` as it
exists today, and the constraints any future rewrite needs to respect.

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

## Current Implementation

Today there is one concrete mutmap implementation. That is deliberate.
Past performance experiments in this area have shown that carrying two
implementations in the code at once creates too much complexity and too
many opportunities for correctness regressions.

This note exists to make future work safer without changing that fact.

## Future Rewrite Constraints

The index path is still the most likely place for a future performance
rewrite, but any such rewrite should remain off the production path
until it clearly:

- matches the current implementation's correctness
- matches or beats the current implementation's write-path performance
- preserves savepoint behavior, clone behavior, and ordered iteration

So the immediate goal is not “split the implementation now.” The
immediate goal is:

- document the contract precisely
- strengthen model-based randomized coverage
- only attempt a rewrite once the safety rails are in place

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

That harness is the safety rail for any later mutmap rewrite.
