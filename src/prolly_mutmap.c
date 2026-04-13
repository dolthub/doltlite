
#ifdef DOLTLITE_PROLLY

#include "prolly_mutmap.h"
#include "prolly_node.h"
#include <string.h>

#define MUTMAP_INIT_CAP 16

static int compareEntries(
  u8 isIntKey,
  const u8 *pKeyA, int nKeyA, i64 intKeyA,
  const u8 *pKeyB, int nKeyB, i64 intKeyB
){
  u8 flags = isIntKey ? PROLLY_NODE_INTKEY : PROLLY_NODE_BLOBKEY;
  return prollyCompareKeys(flags, pKeyA, nKeyA, intKeyA,
                           pKeyB, nKeyB, intKeyB);
}

static ProllyMutMapEntry *entryAtOrder(ProllyMutMap *mm, int idx){
  return &mm->aEntries[mm->aOrder[idx]];
}

static void freeEntryData(ProllyMutMapEntry *e){
  sqlite3_free(e->pKey);
  sqlite3_free(e->pVal);
  e->pKey = 0;
  e->pVal = 0;
  e->nKey = 0;
  e->nVal = 0;
}

static int copyEntryData(ProllyMutMapEntry *e,
                         const u8 *pKey, int nKey,
                         const u8 *pVal, int nVal){
  e->pKey = 0;
  e->nKey = 0;
  e->pVal = 0;
  e->nVal = 0;
  if( !e->isIntKey && pKey && nKey>0 ){
    e->pKey = (u8*)sqlite3_malloc(nKey);
    if( !e->pKey ) return SQLITE_NOMEM;
    memcpy(e->pKey, pKey, nKey);
    e->nKey = nKey;
  }
  if( pVal && nVal>0 ){
    e->pVal = (u8*)sqlite3_malloc(nVal);
    if( !e->pVal ){
      sqlite3_free(e->pKey);
      e->pKey = 0;
      return SQLITE_NOMEM;
    }
    memcpy(e->pVal, pVal, nVal);
    e->nVal = nVal;
  }
  return SQLITE_OK;
}

static int bsearch_key(ProllyMutMap *mm,
                       const u8 *pKey, int nKey, i64 intKey,
                       int *pFound){
  int lo = 0, hi = mm->nEntries;
  *pFound = 0;
  while( lo < hi ){
    int mid = lo + (hi - lo) / 2;
    ProllyMutMapEntry *e = entryAtOrder(mm, mid);
    int c = compareEntries(mm->isIntKey,
                           e->pKey, e->nKey, e->intKey,
                           pKey, nKey, intKey);
    if( c < 0 ){
      lo = mid + 1;
    }else if( c > 0 ){
      hi = mid;
    }else{
      *pFound = 1;
      return mid;
    }
  }
  return lo;
}

static int ensureCapacity(ProllyMutMap *mm){
  if( mm->nEntries >= mm->nAlloc ){
    int nNew = mm->nAlloc ? mm->nAlloc * 2 : MUTMAP_INIT_CAP;
    ProllyMutMapEntry *aNew = sqlite3_realloc(mm->aEntries,
                                nNew * sizeof(ProllyMutMapEntry));
    int *aOrderNew;
    int *aPosNew;
    if( !aNew ) return SQLITE_NOMEM;
    aOrderNew = sqlite3_realloc(mm->aOrder, nNew * sizeof(int));
    if( !aOrderNew ) return SQLITE_NOMEM;
    aPosNew = sqlite3_realloc(mm->aPos, nNew * sizeof(int));
    if( !aPosNew ) return SQLITE_NOMEM;
    mm->aEntries = aNew;
    mm->aOrder = aOrderNew;
    mm->aPos = aPosNew;
    mm->nAlloc = nNew;
  }
  return SQLITE_OK;
}

int prollyMutMapInit(ProllyMutMap *mm, u8 isIntKey){
  memset(mm, 0, sizeof(*mm));
  mm->isIntKey = isIntKey;
  return SQLITE_OK;
}

/* Append an undo record snapshotting the current state of
** aEntries[idx]. Only called when an in-place mutation under an
** active savepoint would lose information that ROLLBACK TO needs.
*/
static int appendUndoRec(ProllyMutMap *mm, int idx){
  ProllyMutMapEntry *e;
  ProllyMutMapUndoRec *rec;
  if( mm->nUndo >= mm->nUndoAlloc ){
    int nNew = mm->nUndoAlloc ? mm->nUndoAlloc*2 : 8;
    ProllyMutMapUndoRec *aNew = sqlite3_realloc(mm->aUndo,
        nNew * sizeof(ProllyMutMapUndoRec));
    if( !aNew ) return SQLITE_NOMEM;
    mm->aUndo = aNew;
    mm->nUndoAlloc = nNew;
  }
  e = &mm->aEntries[idx];
  rec = &mm->aUndo[mm->nUndo];
  rec->level = mm->currentSavepointLevel;
  rec->entryIdx = idx;
  rec->prevBornAt = e->bornAt;
  rec->prevOp = e->op;
  rec->nPrevVal = e->nVal;
  if( e->nVal > 0 && e->pVal ){
    rec->prevVal = (u8*)sqlite3_malloc(e->nVal);
    if( !rec->prevVal ) return SQLITE_NOMEM;
    memcpy(rec->prevVal, e->pVal, e->nVal);
  }else{
    rec->prevVal = 0;
  }
  mm->nUndo++;
  return SQLITE_OK;
}

/* When an insert at idx shifts later entries to the right, any
** undo records pointing at the shifted entries need their indices
** bumped to track. Symmetric for an undo of the insert. */
static void shiftUndoIndices(ProllyMutMap *mm, int idx, int delta){
  int i;
  for(i=0; i<mm->nUndo; i++){
    if( mm->aUndo[i].entryIdx >= idx ){
      mm->aUndo[i].entryIdx += delta;
    }
  }
}

int prollyMutMapInsert(
  ProllyMutMap *mm,
  const u8 *pKey, int nKey, i64 intKey,
  const u8 *pVal, int nVal
){
  int found, idx, rc;

  idx = bsearch_key(mm, pKey, nKey, intKey, &found);

  if( found ){
    ProllyMutMapEntry *e = entryAtOrder(mm, idx);
    /* In-place mutation. If a savepoint is active AND the entry
    ** predates it, snapshot before overwriting. */
    if( mm->currentSavepointLevel > 0
     && e->bornAt < mm->currentSavepointLevel ){
      rc = appendUndoRec(mm, idx);
      if( rc!=SQLITE_OK ) return rc;
    }
    e->op = PROLLY_EDIT_INSERT;
    sqlite3_free(e->pVal);
    e->pVal = 0;
    e->nVal = 0;
    if( pVal && nVal>0 ){
      e->pVal = (u8*)sqlite3_malloc(nVal);
      if( !e->pVal ) return SQLITE_NOMEM;
      memcpy(e->pVal, pVal, nVal);
      e->nVal = nVal;
    }
    e->bornAt = mm->currentSavepointLevel;
    return SQLITE_OK;
  }

  rc = ensureCapacity(mm);
  if( rc!=SQLITE_OK ) return rc;

  {
    int phys = mm->nEntries;
    int i;
    ProllyMutMapEntry *e = &mm->aEntries[phys];
    memset(e, 0, sizeof(*e));
    e->op = PROLLY_EDIT_INSERT;
    e->isIntKey = mm->isIntKey;
    e->intKey = intKey;
    e->bornAt = mm->currentSavepointLevel;
    rc = copyEntryData(e, pKey, nKey, pVal, nVal);
    if( rc!=SQLITE_OK ){
      return rc;
    }
    if( idx < mm->nEntries ){
      memmove(&mm->aOrder[idx+1], &mm->aOrder[idx],
              (mm->nEntries - idx) * sizeof(int));
    }
    mm->aOrder[idx] = phys;
    mm->aPos[phys] = idx;
    for(i = idx + 1; i <= mm->nEntries; i++){
      mm->aPos[mm->aOrder[i]] = i;
    }
  }

  mm->nEntries++;
  return SQLITE_OK;
}

int prollyMutMapDelete(
  ProllyMutMap *mm,
  const u8 *pKey, int nKey, i64 intKey
){
  int found, idx, rc;

  idx = bsearch_key(mm, pKey, nKey, intKey, &found);

  if( found ){
    ProllyMutMapEntry *e = entryAtOrder(mm, idx);
    if( e->op == PROLLY_EDIT_INSERT ){
      if( mm->currentSavepointLevel > 0
       && e->bornAt < mm->currentSavepointLevel ){
        rc = appendUndoRec(mm, idx);
        if( rc!=SQLITE_OK ) return rc;
      }
      e->op = PROLLY_EDIT_DELETE;
      sqlite3_free(e->pVal);
      e->pVal = 0;
      e->nVal = 0;
      e->bornAt = mm->currentSavepointLevel;
      return SQLITE_OK;
    }
    /* Already a DELETE — no state change. */
    return SQLITE_OK;
  }

  rc = ensureCapacity(mm);
  if( rc!=SQLITE_OK ) return rc;

  {
    int phys = mm->nEntries;
    int i;
    ProllyMutMapEntry *e = &mm->aEntries[phys];
    memset(e, 0, sizeof(*e));
    e->op = PROLLY_EDIT_DELETE;
    e->isIntKey = mm->isIntKey;
    e->intKey = intKey;
    e->bornAt = mm->currentSavepointLevel;
    rc = copyEntryData(e, pKey, nKey, 0, 0);
    if( rc!=SQLITE_OK ){
      return rc;
    }
    if( idx < mm->nEntries ){
      memmove(&mm->aOrder[idx+1], &mm->aOrder[idx],
              (mm->nEntries - idx) * sizeof(int));
    }
    mm->aOrder[idx] = phys;
    mm->aPos[phys] = idx;
    for(i = idx + 1; i <= mm->nEntries; i++){
      mm->aPos[mm->aOrder[i]] = i;
    }
  }

  mm->nEntries++;
  return SQLITE_OK;
}

void prollyMutMapPushSavepoint(ProllyMutMap *mm, int level){
  if( !mm ) return;
  mm->currentSavepointLevel = level;
}

/* Rollback order matters: first walk the undo log backward restoring
** in-place mutations to their pre-savepoint state (including resetting
** bornAt). THEN drop any remaining entries with bornAt >= level —
** those are fresh-key inserts under the rolled-back savepoints, which
** never had undo records in the first place. Doing it the other way
** would drop in-place-mutated entries before their undo record was
** applied, losing the restored state. */
int prollyMutMapRollbackToSavepoint(ProllyMutMap *mm, int level){
  int i;
  if( !mm ) return SQLITE_OK;

  /* Restore in-place mutations. Walk undo backward. */
  while( mm->nUndo > 0
      && mm->aUndo[mm->nUndo - 1].level >= level ){
    ProllyMutMapUndoRec *rec = &mm->aUndo[mm->nUndo - 1];
    int idx = rec->entryIdx;
    if( idx >= 0 && idx < mm->nEntries ){
      ProllyMutMapEntry *e = &mm->aEntries[idx];
      e->op = rec->prevOp;
      e->bornAt = rec->prevBornAt;
      sqlite3_free(e->pVal);
      e->pVal = 0;
      e->nVal = 0;
      if( rec->prevVal && rec->nPrevVal > 0 ){
        e->pVal = (u8*)sqlite3_malloc(rec->nPrevVal);
        if( !e->pVal ) return SQLITE_NOMEM;
        memcpy(e->pVal, rec->prevVal, rec->nPrevVal);
        e->nVal = rec->nPrevVal;
      }
    }
    sqlite3_free(rec->prevVal);
    rec->prevVal = 0;
    mm->nUndo--;
  }

  /* Drop new entries (bornAt >= level) — these are fresh-key inserts
  ** under the rolled-back savepoints. Walk descending so removals
  ** don't disturb later indices. Physical entries are compacted once at
  ** the end; sorted order is rebuilt by filtering the old order vector. */
  {
    int oldN = mm->nEntries;
    int *aMap = 0;
    int newN = 0;
    if( oldN > 0 ){
      aMap = sqlite3_malloc(oldN * sizeof(int));
      if( !aMap ) return SQLITE_NOMEM;
    }
    for(i=0; i<oldN; i++){
      if( mm->aEntries[i].bornAt >= level ){
        freeEntryData(&mm->aEntries[i]);
        aMap[i] = -1;
      }else{
        if( newN != i ){
          mm->aEntries[newN] = mm->aEntries[i];
        }
        aMap[i] = newN++;
      }
    }
    {
      int j;
      int out = 0;
      for(j=0; j<oldN; j++){
        int mapped = aMap[mm->aOrder[j]];
        if( mapped >= 0 ){
          mm->aOrder[out++] = mapped;
        }
      }
      mm->nEntries = out;
      for(j=0; j<mm->nEntries; j++){
        mm->aPos[mm->aOrder[j]] = j;
      }
      for(j=0; j<mm->nUndo; j++){
        mm->aUndo[j].entryIdx = aMap[mm->aUndo[j].entryIdx];
      }
    }
    sqlite3_free(aMap);
  }

  mm->currentSavepointLevel = level - 1;
  return SQLITE_OK;
}

/* RELEASE commits everything at level >= `level` into level `level-1`.
** Two things need to happen: (1) in-place undo records at the released
** levels get "inherited" by the parent level so a future ROLLBACK TO
** the parent can still find the pre-release state; (2) entries with
** bornAt in the released range get clamped down to level-1 so a later
** ROLLBACK TO the parent doesn't mistakenly drop them as fresh post-
** savepoint inserts. Special case: releasing to level 0 (fast path)
** drops the undo log entirely — there's no outer savepoint to preserve
** those records for, and COMMIT/full-ROLLBACK don't use them. */
void prollyMutMapReleaseSavepoint(ProllyMutMap *mm, int level){
  int i;
  int target;
  if( !mm ) return;
  target = level - 1;

  if( target == 0 ){
    for(i=0; i<mm->nUndo; i++){
      sqlite3_free(mm->aUndo[i].prevVal);
      mm->aUndo[i].prevVal = 0;
    }
    mm->nUndo = 0;
  }else{
    for(i=0; i<mm->nUndo; i++){
      if( mm->aUndo[i].level >= level ){
        mm->aUndo[i].level = target;
      }
    }
  }

  for(i=0; i<mm->nEntries; i++){
    if( mm->aEntries[i].bornAt >= level ){
      mm->aEntries[i].bornAt = target;
    }
  }

  mm->currentSavepointLevel = target;
}

ProllyMutMapEntry *prollyMutMapFind(ProllyMutMap *mm,
                                     const u8 *pKey, int nKey, i64 intKey){
  int found, idx;
  if( mm->nEntries==0 ) return 0;
  idx = bsearch_key(mm, pKey, nKey, intKey, &found);
  return found ? entryAtOrder(mm, idx) : 0;
}

ProllyMutMapEntry *prollyMutMapEntryAt(ProllyMutMap *mm, int idx){
  return entryAtOrder(mm, idx);
}

int prollyMutMapOrderIndexFromEntry(ProllyMutMap *mm, ProllyMutMapEntry *pEntry){
  int phys = (int)(pEntry - mm->aEntries);
  return mm->aPos[phys];
}

int prollyMutMapCount(ProllyMutMap *mm){
  return mm->nEntries;
}

int prollyMutMapIsEmpty(ProllyMutMap *mm){
  return mm->nEntries == 0;
}

void prollyMutMapIterFirst(ProllyMutMapIter *it, ProllyMutMap *mm){
  it->pMap = mm;
  it->idx = 0;
}

void prollyMutMapIterNext(ProllyMutMapIter *it){
  if( it->idx < it->pMap->nEntries ) it->idx++;
}

int prollyMutMapIterValid(ProllyMutMapIter *it){
  return it->idx >= 0 && it->idx < it->pMap->nEntries;
}

ProllyMutMapEntry *prollyMutMapIterEntry(ProllyMutMapIter *it){
  return entryAtOrder(it->pMap, it->idx);
}

void prollyMutMapIterSeek(ProllyMutMapIter *it, ProllyMutMap *mm,
                          const u8 *pKey, int nKey, i64 intKey){
  int found = 0;
  it->pMap = mm;
  it->idx = bsearch_key(mm, pKey, nKey, intKey, &found);
}

void prollyMutMapIterLast(ProllyMutMapIter *it, ProllyMutMap *mm){
  it->pMap = mm;
  it->idx = mm->nEntries>0 ? mm->nEntries - 1 : mm->nEntries;
}

/* Clear wipes data only (entries + undo records). currentSavepointLevel
** is savepoint *context* not data; a flushMutMap mid-savepoint calls
** clear but the mutmap continues to live under the same savepoint,
** so any subsequent writes must still be attributed to that level. */
void prollyMutMapClear(ProllyMutMap *mm){
  int i;
  for(i=0; i<mm->nEntries; i++){
    freeEntryData(&mm->aEntries[i]);
  }
  mm->nEntries = 0;
  for(i=0; i<mm->nUndo; i++){
    sqlite3_free(mm->aUndo[i].prevVal);
  }
  mm->nUndo = 0;
}

void prollyMutMapFree(ProllyMutMap *mm){
  prollyMutMapClear(mm);
  sqlite3_free(mm->aEntries);
  mm->aEntries = 0;
  sqlite3_free(mm->aOrder);
  mm->aOrder = 0;
  sqlite3_free(mm->aPos);
  mm->aPos = 0;
  mm->nAlloc = 0;
  sqlite3_free(mm->aUndo);
  mm->aUndo = 0;
  mm->nUndoAlloc = 0;
  mm->currentSavepointLevel = 0;
}

int prollyMutMapClone(ProllyMutMap **out, const ProllyMutMap *src){
  ProllyMutMap *dst;
  int i;
  *out = 0;
  dst = (ProllyMutMap*)sqlite3_malloc(sizeof(ProllyMutMap));
  if( !dst ) return SQLITE_NOMEM;
  memset(dst, 0, sizeof(*dst));
  dst->isIntKey = src->isIntKey;
  dst->currentSavepointLevel = src->currentSavepointLevel;

  if( src->nEntries > 0 ){
    dst->aEntries = (ProllyMutMapEntry*)sqlite3_malloc(
        src->nEntries * (int)sizeof(ProllyMutMapEntry));
    dst->aOrder = (int*)sqlite3_malloc(src->nEntries * (int)sizeof(int));
    dst->aPos = (int*)sqlite3_malloc(src->nEntries * (int)sizeof(int));
    if( !dst->aEntries || !dst->aOrder || !dst->aPos ){
      prollyMutMapFree(dst);
      sqlite3_free(dst);
      return SQLITE_NOMEM;
    }
    memset(dst->aEntries, 0,
           src->nEntries * sizeof(ProllyMutMapEntry));
    dst->nAlloc = src->nEntries;
    for(i=0; i<src->nEntries; i++){
      ProllyMutMapEntry *se = &src->aEntries[i];
      ProllyMutMapEntry *de = &dst->aEntries[i];
      de->op = se->op;
      de->isIntKey = se->isIntKey;
      de->intKey = se->intKey;
      de->bornAt = se->bornAt;
      de->nKey = 0;
      de->nVal = 0;
      de->pKey = 0;
      de->pVal = 0;
      if( se->pKey && se->nKey > 0 ){
        de->pKey = (u8*)sqlite3_malloc(se->nKey);
        if( !de->pKey ){
          dst->nEntries = i;
          prollyMutMapFree(dst);
          sqlite3_free(dst);
          return SQLITE_NOMEM;
        }
        memcpy(de->pKey, se->pKey, se->nKey);
        de->nKey = se->nKey;
      }
      if( se->pVal && se->nVal > 0 ){
        de->pVal = (u8*)sqlite3_malloc(se->nVal);
        if( !de->pVal ){
          sqlite3_free(de->pKey);
          de->pKey = 0;
          dst->nEntries = i;
          prollyMutMapFree(dst);
          sqlite3_free(dst);
          return SQLITE_NOMEM;
        }
        memcpy(de->pVal, se->pVal, se->nVal);
        de->nVal = se->nVal;
      }
      dst->nEntries++;
    }
    memcpy(dst->aOrder, src->aOrder, src->nEntries * sizeof(int));
    memcpy(dst->aPos, src->aPos, src->nEntries * sizeof(int));
  }

  if( src->nUndo > 0 ){
    dst->aUndo = (ProllyMutMapUndoRec*)sqlite3_malloc(
        src->nUndo * (int)sizeof(ProllyMutMapUndoRec));
    if( !dst->aUndo ){
      prollyMutMapFree(dst);
      sqlite3_free(dst);
      return SQLITE_NOMEM;
    }
    memset(dst->aUndo, 0,
           src->nUndo * sizeof(ProllyMutMapUndoRec));
    dst->nUndoAlloc = src->nUndo;
    for(i=0; i<src->nUndo; i++){
      ProllyMutMapUndoRec *sr = &src->aUndo[i];
      ProllyMutMapUndoRec *dr = &dst->aUndo[i];
      dr->level = sr->level;
      dr->entryIdx = sr->entryIdx;
      dr->prevBornAt = sr->prevBornAt;
      dr->prevOp = sr->prevOp;
      dr->nPrevVal = 0;
      dr->prevVal = 0;
      if( sr->prevVal && sr->nPrevVal > 0 ){
        dr->prevVal = (u8*)sqlite3_malloc(sr->nPrevVal);
        if( !dr->prevVal ){
          dst->nUndo = i;
          prollyMutMapFree(dst);
          sqlite3_free(dst);
          return SQLITE_NOMEM;
        }
        memcpy(dr->prevVal, sr->prevVal, sr->nPrevVal);
        dr->nPrevVal = sr->nPrevVal;
      }
      dst->nUndo++;
    }
  }

  *out = dst;
  return SQLITE_OK;
}

int prollyMutMapMerge(ProllyMutMap *pDst, ProllyMutMap *pSrc){
  int i, rc;
  for(i=0; i<pSrc->nEntries; i++){
    ProllyMutMapEntry *e = &pSrc->aEntries[i];
    if( e->op==PROLLY_EDIT_INSERT ){
      rc = prollyMutMapInsert(pDst, e->pKey, e->nKey, e->intKey,
                               e->pVal, e->nVal);
    }else{
      rc = prollyMutMapDelete(pDst, e->pKey, e->nKey, e->intKey);
    }
    if( rc!=SQLITE_OK ) return rc;
  }
  prollyMutMapClear(pSrc);
  return SQLITE_OK;
}

#endif 
