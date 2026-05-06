#ifndef SQLITE_PROLLY_MUTMAP_H
#define SQLITE_PROLLY_MUTMAP_H

#include "sqliteInt.h"

#define PROLLY_EDIT_INSERT 1
#define PROLLY_EDIT_DELETE 2

typedef struct ProllyMutMap ProllyMutMap;
typedef struct ProllyMutMapEntry ProllyMutMapEntry;
typedef struct ProllyMutMapIter ProllyMutMapIter;

struct ProllyMutMapEntry {
  u8 op;
  u8 *pKey;
  int nKey;
  u8 *pVal;
  int nVal;
  int bornAt;
};

/* Decodes a sortable 8-byte BE entry key back to i64. Only valid for
** entries in an INT-mode map; the bytes layout matches the on-disk
** PROLLY_NODE_INTKEY encoding (sign-flipped big-endian). */
i64 prollyMutMapEntryIntKey(const ProllyMutMapEntry *e);

/* Lazy — allocated only when an in-place mutation under an active
** savepoint is about to overwrite the previous (op, value). */
typedef struct ProllyMutMapUndoRec ProllyMutMapUndoRec;
struct ProllyMutMapUndoRec {
  int level;
  int entryIdx;
  int prevBornAt;
  u8 prevOp;
  u8 *prevVal;
  int nPrevVal;
};

struct ProllyMutMap {
  u8 isIntKey;
  u8 keepSorted;
  u8 orderDirty;
  int nEntries;
  int nAlloc;
  int levelBase;
  ProllyMutMapEntry *aEntries;
  /* aOrder / aPos form a two-way mapping between sorted position and
  ** physical entry index. aHash is an open-addressing lookup table
  ** storing (phys + 1) so 0 marks an empty slot. */
  int *aOrder;
  int *aPos;
  int *aHash;
  int nHashAlloc;
  /* 0 disables the undo-log path entirely — the autocommit fast path. */
  int currentSavepointLevel;
  ProllyMutMapUndoRec *aUndo;
  int nUndo;
  int nUndoAlloc;
  /* Bumped on every mutation that can shift cursor positions: insert
  ** of a new key, delete of an existing key, savepoint rollback (drops
  ** entries). NOT bumped by in-place value updates or savepoint
  ** release (which only relabels bornAt). Cursors snapshot this value
  ** when they record an mmIdx, then re-resolve their position by key
  ** if the snapshot is stale. This matters now that per-table mutmaps
  ** are shared across cursors. */
  u32 generation;
};

int prollyMutMapInit(ProllyMutMap *mm, u8 isIntKey);
int prollyMutMapInitMode(ProllyMutMap *mm, u8 isIntKey, u8 keepSorted);

int prollyMutMapInsert(ProllyMutMap *mm,
                       const u8 *pKey, int nKey, i64 intKey,
                       const u8 *pVal, int nVal);

int prollyMutMapDelete(ProllyMutMap *mm,
                       const u8 *pKey, int nKey, i64 intKey);

int prollyMutMapFindRc(
  ProllyMutMap *mm,
  const u8 *pKey, int nKey, i64 intKey,
  ProllyMutMapEntry **ppEntry
);

/* Resolve a key to its current sorted-order index in the mutmap, or
** the position where it would be inserted if absent. Output (*pIdx)
** is in [0, nEntries]; (*pFound) is non-zero iff the key is present.
** Used by cursors that cached an mmIdx at an earlier generation —
** when generation has advanced, the cached idx may point at the
** wrong entry, so the cursor re-resolves by its cached key. */
int prollyMutMapResolveSortedPos(
  ProllyMutMap *mm,
  const u8 *pKey, int nKey, i64 intKey,
  int *pIdx, int *pFound
);

ProllyMutMapEntry *prollyMutMapEntryAt(ProllyMutMap *mm, int idx);

int prollyMutMapOrderIndexFromEntry(ProllyMutMap *mm, ProllyMutMapEntry *pEntry);

int prollyMutMapCount(ProllyMutMap *mm);

int prollyMutMapIsEmpty(ProllyMutMap *mm);

struct ProllyMutMapIter {
  ProllyMutMap *pMap;
  int idx;
};

void prollyMutMapIterFirst(ProllyMutMapIter *it, ProllyMutMap *mm);

void prollyMutMapIterNext(ProllyMutMapIter *it);

int prollyMutMapIterValid(ProllyMutMapIter *it);

ProllyMutMapEntry *prollyMutMapIterEntry(ProllyMutMapIter *it);

void prollyMutMapIterSeek(ProllyMutMapIter *it, ProllyMutMap *mm,
                          const u8 *pKey, int nKey, i64 intKey);

void prollyMutMapIterLast(ProllyMutMapIter *it, ProllyMutMap *mm);

int prollyMutMapMerge(ProllyMutMap *pDst, ProllyMutMap *pSrc);

int prollyMutMapClone(ProllyMutMap **out, const ProllyMutMap *src);

/* Rollback order matters: walk the undo log backward FIRST (restoring
** in-place mutations), THEN drop entries with bornAt >= level. Doing it
** the other way would drop in-place-mutated entries before their undo
** record gets applied. */
void prollyMutMapPushSavepoint(ProllyMutMap *mm, int level);
int  prollyMutMapRollbackToSavepoint(ProllyMutMap *mm, int level);
void prollyMutMapReleaseSavepoint(ProllyMutMap *mm, int level);

void prollyMutMapClear(ProllyMutMap *mm);

void prollyMutMapFree(ProllyMutMap *mm);

#endif
