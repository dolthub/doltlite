#ifndef SQLITE_PROLLY_MUTMAP_H
#define SQLITE_PROLLY_MUTMAP_H

#include "sqliteInt.h"

#define PROLLY_EDIT_INSERT 1
#define PROLLY_EDIT_DELETE 2

typedef struct ProllyMutMap ProllyMutMap;
typedef struct ProllyMutMapEntry ProllyMutMapEntry;
typedef struct ProllyMutMapIter ProllyMutMapIter;
typedef enum ProllyMutMapKind ProllyMutMapKind;

enum ProllyMutMapKind {
  PROLLY_MUTMAP_KIND_TABLE = 1,
  PROLLY_MUTMAP_KIND_INDEX = 2
};

struct ProllyMutMapEntry {
  u8 op;
  u8 isIntKey;
  i64 intKey;
  u8 *pKey;
  int nKey;
  u8 *pVal;
  int nVal;
  int bornAt;
};

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
  u8 eKind;
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
};

int prollyMutMapInit(ProllyMutMap *mm, u8 isIntKey);
int prollyMutMapInitMode(ProllyMutMap *mm, u8 isIntKey, u8 keepSorted);
int prollyMutMapInitKind(
  ProllyMutMap *mm, u8 isIntKey, u8 keepSorted, ProllyMutMapKind eKind
);

int prollyMutMapInsert(ProllyMutMap *mm,
                       const u8 *pKey, int nKey, i64 intKey,
                       const u8 *pVal, int nVal);

int prollyMutMapDelete(ProllyMutMap *mm,
                       const u8 *pKey, int nKey, i64 intKey);

ProllyMutMapEntry *prollyMutMapFind(ProllyMutMap *mm,
                                     const u8 *pKey, int nKey, i64 intKey);

int prollyMutMapFindRc(
  ProllyMutMap *mm,
  const u8 *pKey, int nKey, i64 intKey,
  ProllyMutMapEntry **ppEntry
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
