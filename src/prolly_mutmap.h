/* Sorted map of pending INSERT/DELETE edits, consumed by prolly_mutate. */
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
  u8 isIntKey;
  i64 intKey;
  u8 *pKey;
  int nKey;
  u8 *pVal;
  int nVal;
  /* Savepoint level at which this entry was created or last
  ** modified in place. 0 means "no savepoint active when written".
  ** Used by the savepoint rollback path to drop post-savepoint
  ** entries — see prollyMutMapPushSavepoint. */
  int bornAt;
};

/* Undo record: pre-mutation snapshot of an entry whose op or value
** was overwritten in place under an active savepoint. The record
** captures enough to put the entry back the way it was. Created
** lazily — only when an in-place mutation actually crosses a
** savepoint boundary, never on fresh-key inserts. */
typedef struct ProllyMutMapUndoRec ProllyMutMapUndoRec;
struct ProllyMutMapUndoRec {
  int level;        /* savepoint level this record belongs to */
  int entryIdx;     /* aEntries[] index at the time of capture */
  int prevBornAt;   /* original bornAt to restore */
  u8 prevOp;
  u8 *prevVal;
  int nPrevVal;
};

struct ProllyMutMap {
  u8 isIntKey;
  int nEntries;
  int nAlloc;
  ProllyMutMapEntry *aEntries;
  int *aOrder;              /* sorted-position -> physical entry index */
  int *aPos;                /* physical entry index -> sorted-position */
  /* Active savepoint level. 0 = no savepoint, mutations skip
  ** the undo-log path entirely (fast path for autocommit). */
  int currentSavepointLevel;
  /* Undo log, append-only between savepoint state transitions.
  ** Rollback walks backward; release drops the suffix at or
  ** above the released level. */
  ProllyMutMapUndoRec *aUndo;
  int nUndo;
  int nUndoAlloc;
};

int prollyMutMapInit(ProllyMutMap *mm, u8 isIntKey);

int prollyMutMapInsert(ProllyMutMap *mm,
                       const u8 *pKey, int nKey, i64 intKey,
                       const u8 *pVal, int nVal);

int prollyMutMapDelete(ProllyMutMap *mm,
                       const u8 *pKey, int nKey, i64 intKey);

ProllyMutMapEntry *prollyMutMapFind(ProllyMutMap *mm,
                                     const u8 *pKey, int nKey, i64 intKey);

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

/* Deep-copy src into a fresh mutmap returned via *out. Preserves entries
** (key/val bytes), the undo log, isIntKey, and currentSavepointLevel.
** Used by the btree savepoint layer to snapshot a pPending mutmap before
** the btree flushes it into the underlying tree — without this the
** savepoint's root snapshot alone loses pre-savepoint buffered writes. */
int prollyMutMapClone(ProllyMutMap **out, const ProllyMutMap *src);

/* Savepoint controls. The btree calls these to bracket a savepoint:
**
**   prollyMutMapPushSavepoint(mm, level)
**     O(1). Sets mm->currentSavepointLevel to level. New entries
**     written henceforth get bornAt = level; in-place mutations on
**     existing entries with bornAt < level log an undo record (lazy
**     — only the first mutation per entry per level allocates).
**
**   prollyMutMapRollbackToSavepoint(mm, level)
**     Reverts everything done at savepoint levels >= level.
**     Order matters: walk the undo log backward FIRST, restoring
**     in-place mutations to their pre-savepoint state (including
**     resetting bornAt), THEN drop remaining entries with
**     bornAt >= level (fresh inserts under the rolled-back savepoints
**     that have no undo records). Sets currentSavepointLevel to
**     level - 1.
**
**   prollyMutMapReleaseSavepoint(mm, level)
**     Commits everything done at levels >= level into level-1.
**     Relabels undo records at level >= the released level down to
**     level-1 (so a future ROLLBACK TO the parent still finds the
**     pre-release state) and clamps entry bornAt >= level to level-1
**     (so fresh post-savepoint inserts aren't mistakenly dropped by
**     a later parent rollback). Special case: when releasing to 0
**     (fast path), the entire undo log is freed. Sets
**     currentSavepointLevel to level - 1.
**
** Hot path: when currentSavepointLevel == 0 (autocommit, no user
** savepoints) the in-place mutation paths fast-path past the undo
** log code entirely. */
void prollyMutMapPushSavepoint(ProllyMutMap *mm, int level);
int  prollyMutMapRollbackToSavepoint(ProllyMutMap *mm, int level);
void prollyMutMapReleaseSavepoint(ProllyMutMap *mm, int level);

void prollyMutMapClear(ProllyMutMap *mm);

void prollyMutMapFree(ProllyMutMap *mm);

#endif 
