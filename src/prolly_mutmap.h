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
  u8 aKeyInline[8];
  u8 *pVal;
  int nVal;
  int nValAlloc;
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
};

struct ProllyMutMap {
  u8 isIntKey;
  u8 keepSorted;
  u8 orderDirty;
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

int prollyMutMapDelete(ProllyMutMap *mm,
                       const u8 *pKey, int nKey, i64 intKey);

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

void prollyMutMapPushSavepoint(ProllyMutMap *mm, int level);
int  prollyMutMapRollbackToSavepoint(ProllyMutMap *mm, int level);
void prollyMutMapReleaseSavepoint(ProllyMutMap *mm, int level);

void prollyMutMapClear(ProllyMutMap *mm);

void prollyMutMapFree(ProllyMutMap *mm);

#endif
