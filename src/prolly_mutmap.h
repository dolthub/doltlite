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
  u64 keyPrefix;
  u32 keyHash;
  u8 aKeyInline[40];
  u8 *pVal;
  int nVal;
  int nValAlloc;
  /* The logical value is pVal[0..nVal) followed by nZeroTail zero bytes
  ** that are never materialized in pVal. Only intkey-table inserts create
  ** tails; blob-key (index) entries always have nZeroTail==0. */
  i64 nZeroTail;
  int bornAt;
};

i64 prollyMutMapEntryIntKey(const ProllyMutMapEntry *e);

typedef struct ProllyMutMapUndoRec ProllyMutMapUndoRec;
struct ProllyMutMapUndoRec {
  int level;
  int entryIdx;
  int prevBornAt;
  u8 prevOp;
  u8 *prevVal;
  int nPrevVal;
  i64 nPrevZeroTail;
};

struct ProllyMutMap {
  u8 isIntKey;
  u8 keepSorted;
  u8 orderDirty;
  /* Set after ordered access so mixed read/write maps keep their order. */
  u8 preferSorted;
  u8 posDirty;
  u8 appendSorted;
  int nEntries;
  int nAlloc;
  int levelBase;
  ProllyMutMapEntry *aEntries;
  /* aEntries is append ordered. aOrder is key sorted, aPos maps physical
  ** entry indexes back into aOrder, and aHash accelerates point lookup. */
  int *aOrder;
  int *aPos;
  int *aHash;
  int nHashAlloc;
  void *aSortScratch;
  int nSortScratchBytes;
  int currentSavepointLevel;
  ProllyMutMapUndoRec *aUndo;
  int nUndo;
  int nUndoAlloc;
  /* Cursors cache this value so they can detect pending-map replacement or
  ** rollback without pointer comparisons against recycled allocations. */
  u32 generation;
};

int prollyMutMapInit(ProllyMutMap *mm, u8 isIntKey);
int prollyMutMapInitMode(ProllyMutMap *mm, u8 isIntKey, u8 keepSorted);

int prollyMutMapInsert(ProllyMutMap *mm,
                       const u8 *pKey, int nKey, i64 intKey,
                       const u8 *pVal, int nVal);

int prollyMutMapInsertZeroTail(ProllyMutMap *mm, i64 intKey,
                               const u8 *pVal, int nValPrefix,
                               i64 nZeroTail);

int prollyMutMapReplaceEntry(
  ProllyMutMap *mm,
  ProllyMutMapEntry *e,
  const u8 *pVal,
  int nVal
);

int prollyMutMapDelete(ProllyMutMap *mm,
                       const u8 *pKey, int nKey, i64 intKey);

int prollyMutMapDeleteEntry(
  ProllyMutMap *mm,
  ProllyMutMapEntry *e
);

int prollyMutMapFindRc(
  ProllyMutMap *mm,
  const u8 *pKey, int nKey, i64 intKey,
  ProllyMutMapEntry **ppEntry
);

int prollyMutMapResolveSortedPos(
  ProllyMutMap *mm,
  const u8 *pKey, int nKey, i64 intKey,
  int *pIdx, int *pFound
);

int prollyMutMapEntryAt(ProllyMutMap *mm, int idx, ProllyMutMapEntry **ppEntry);

int prollyMutMapOrderIndexFromEntry(ProllyMutMap *mm, ProllyMutMapEntry *pEntry);

int prollyMutMapCount(ProllyMutMap *mm);

int prollyMutMapIsEmpty(ProllyMutMap *mm);

struct ProllyMutMapIter {
  ProllyMutMap *pMap;
  int idx;
};

/* Compute the sorted iteration order up front, returning SQLITE_NOMEM on
** allocation failure. Useful before a loop of ordered reads that cannot
** conveniently propagate an error mid-iteration. */
int prollyMutMapEnsureOrder(ProllyMutMap *mm);

/* Iterator initializers materialize the sorted order and return SQLITE_NOMEM
** if that allocation fails; the iterator is left invalid (past the end) so an
** unchecked caller iterates nothing rather than a stale order. */
int prollyMutMapIterFirst(ProllyMutMapIter *it, ProllyMutMap *mm);

void prollyMutMapIterNext(ProllyMutMapIter *it);

int prollyMutMapIterValid(ProllyMutMapIter *it);

ProllyMutMapEntry *prollyMutMapIterEntry(ProllyMutMapIter *it);

int prollyMutMapIterSeek(ProllyMutMapIter *it, ProllyMutMap *mm,
                         const u8 *pKey, int nKey, i64 intKey);

int prollyMutMapIterLast(ProllyMutMapIter *it, ProllyMutMap *mm);

int prollyMutMapClone(ProllyMutMap **out, const ProllyMutMap *src);

void prollyMutMapPushSavepoint(ProllyMutMap *mm, int level);
int  prollyMutMapRollbackToSavepoint(ProllyMutMap *mm, int level);
void prollyMutMapReleaseSavepoint(ProllyMutMap *mm, int level);

void prollyMutMapClear(ProllyMutMap *mm);

void prollyMutMapFree(ProllyMutMap *mm);

#endif
