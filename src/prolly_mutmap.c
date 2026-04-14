
#ifdef DOLTLITE_PROLLY

#include "prolly_mutmap.h"
#include "prolly_node.h"
#include <string.h>
#include <stdlib.h>

#define MUTMAP_INIT_CAP 16
#define MUTMAP_MIN_HASH 32
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

static u32 hashKey(
  u8 isIntKey,
  const u8 *pKey, int nKey, i64 intKey
){
  u32 h = 2166136261u;
  if( isIntKey ){
    u64 x = (u64)intKey;
    int i;
    for(i=0; i<8; i++){
      h ^= (u8)(x & 0xff);
      h *= 16777619u;
      x >>= 8;
    }
  }else{
    int i;
    for(i=0; i<nKey; i++){
      h ^= pKey[i];
      h *= 16777619u;
    }
    h ^= (u32)nKey;
    h *= 16777619u;
  }
  return h;
}

static ProllyMutMap *gSortCtx = 0;

static int encodeLevel(ProllyMutMap *mm, int level){
  if( level<=0 ) return 0;
  return level + mm->levelBase;
}

static int decodeLevel(ProllyMutMap *mm, int stored){
  if( stored<=0 ) return 0;
  if( stored<=mm->levelBase ) return 0;
  return stored - mm->levelBase;
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

static int hashEntryMatches(
  ProllyMutMap *mm, int phys,
  const u8 *pKey, int nKey, i64 intKey
){
  ProllyMutMapEntry *e = &mm->aEntries[phys];
  return compareEntries(mm->isIntKey,
                        e->pKey, e->nKey, e->intKey,
                        pKey, nKey, intKey)==0;
}

static int rebuildHash(ProllyMutMap *mm){
  int i;
  int nHash = MUTMAP_MIN_HASH;
  while( nHash < (mm->nEntries > 0 ? mm->nEntries * 2 : 1) ) nHash *= 2;
  if( nHash != mm->nHashAlloc ){
    int *aNew = sqlite3_realloc(mm->aHash, nHash * sizeof(int));
    if( !aNew ) return SQLITE_NOMEM;
    mm->aHash = aNew;
    mm->nHashAlloc = nHash;
  }
  memset(mm->aHash, 0, mm->nHashAlloc * sizeof(int));
  for(i=0; i<mm->nEntries; i++){
    u32 h = hashKey(mm->isIntKey,
                    mm->aEntries[i].pKey, mm->aEntries[i].nKey, mm->aEntries[i].intKey);
    int mask = mm->nHashAlloc - 1;
    int slot = (int)(h & (u32)mask);
    while( mm->aHash[slot] != 0 ){
      slot = (slot + 1) & mask;
    }
    mm->aHash[slot] = i + 1;
  }
  return SQLITE_OK;
}

static int ensureHashForInsert(ProllyMutMap *mm){
  if( mm->keepSorted ) return SQLITE_OK;
  if( mm->nHashAlloc==0 || (mm->nEntries + 1) * 2 > mm->nHashAlloc ){
    return rebuildHash(mm);
  }
  return SQLITE_OK;
}

static void hashInsertPhys(ProllyMutMap *mm, int phys){
  u32 h;
  int mask;
  int slot;
  if( mm->keepSorted || mm->nHashAlloc==0 ) return;
  h = hashKey(mm->isIntKey,
              mm->aEntries[phys].pKey, mm->aEntries[phys].nKey,
              mm->aEntries[phys].intKey);
  mask = mm->nHashAlloc - 1;
  slot = (int)(h & (u32)mask);
  while( mm->aHash[slot] != 0 ){
    slot = (slot + 1) & mask;
  }
  mm->aHash[slot] = phys + 1;
}

static int findPhysLazy(ProllyMutMap *mm,
                        const u8 *pKey, int nKey, i64 intKey,
                        int *pPhys){
  *pPhys = -1;
  if( mm->nEntries==0 ) return SQLITE_OK;
  if( mm->nHashAlloc==0 ){
    int rc = rebuildHash(mm);
    if( rc!=SQLITE_OK ) return rc;
  }
  {
    u32 h = hashKey(mm->isIntKey, pKey, nKey, intKey);
    int mask = mm->nHashAlloc - 1;
    int slot = (int)(h & (u32)mask);
    while( mm->aHash[slot] != 0 ){
      int phys = mm->aHash[slot] - 1;
      if( hashEntryMatches(mm, phys, pKey, nKey, intKey) ){
        *pPhys = phys;
        return SQLITE_OK;
      }
      slot = (slot + 1) & mask;
    }
  }
  return SQLITE_OK;
}

static int compareOrderIndexes(const void *a, const void *b){
  ProllyMutMap *mm = gSortCtx;
  int ia = *(const int*)a;
  int ib = *(const int*)b;
  ProllyMutMapEntry *ea = &mm->aEntries[ia];
  ProllyMutMapEntry *eb = &mm->aEntries[ib];
  return compareEntries(mm->isIntKey,
                        ea->pKey, ea->nKey, ea->intKey,
                        eb->pKey, eb->nKey, eb->intKey);
}

static int ensureOrder(ProllyMutMap *mm){
  int i;
  if( mm->keepSorted || !mm->orderDirty ) return SQLITE_OK;
  for(i=0; i<mm->nEntries; i++){
    mm->aOrder[i] = i;
  }
  gSortCtx = mm;
  qsort(mm->aOrder, mm->nEntries, sizeof(int), compareOrderIndexes);
  gSortCtx = 0;
  for(i=0; i<mm->nEntries; i++){
    mm->aPos[mm->aOrder[i]] = i;
  }
  mm->orderDirty = 0;
  return SQLITE_OK;
}

static int rankEntryWithoutOrder(ProllyMutMap *mm, int phys){
  ProllyMutMapEntry *target = &mm->aEntries[phys];
  int rank = 0;
  int i;
  for(i=0; i<mm->nEntries; i++){
    if( i==phys ) continue;
    if( compareEntries(mm->isIntKey,
                       mm->aEntries[i].pKey, mm->aEntries[i].nKey,
                       mm->aEntries[i].intKey,
                       target->pKey, target->nKey, target->intKey) < 0 ){
      rank++;
    }
  }
  return rank;
}

static int ensureCapacity(ProllyMutMap *mm){
  if( mm->nEntries >= mm->nAlloc ){
    int nNew = mm->nAlloc ? mm->nAlloc * 2 : MUTMAP_INIT_CAP;
    ProllyMutMapEntry *aNew;
    int *aOrderNew;
    int *aPosNew;
    aNew = sqlite3_malloc(nNew * sizeof(ProllyMutMapEntry));
    aOrderNew = sqlite3_malloc(nNew * sizeof(int));
    aPosNew = sqlite3_malloc(nNew * sizeof(int));
    if( !aNew || !aOrderNew || !aPosNew ){
      sqlite3_free(aNew);
      sqlite3_free(aOrderNew);
      sqlite3_free(aPosNew);
      return SQLITE_NOMEM;
    }
    if( mm->nEntries > 0 ){
      memcpy(aNew, mm->aEntries, mm->nEntries * sizeof(ProllyMutMapEntry));
      memcpy(aOrderNew, mm->aOrder, mm->nEntries * sizeof(int));
      memcpy(aPosNew, mm->aPos, mm->nEntries * sizeof(int));
    }
    sqlite3_free(mm->aEntries);
    sqlite3_free(mm->aOrder);
    sqlite3_free(mm->aPos);
    mm->aEntries = aNew;
    mm->aOrder = aOrderNew;
    mm->aPos = aPosNew;
    mm->nAlloc = nNew;
  }
  return SQLITE_OK;
}

int prollyMutMapInit(ProllyMutMap *mm, u8 isIntKey){
  return prollyMutMapInitMode(mm, isIntKey, 1);
}

int prollyMutMapInitMode(ProllyMutMap *mm, u8 isIntKey, u8 keepSorted){
  memset(mm, 0, sizeof(*mm));
  mm->isIntKey = isIntKey;
  mm->keepSorted = keepSorted;
  return SQLITE_OK;
}

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
  rec->prevBornAt = decodeLevel(mm, e->bornAt);
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
  int found = 0, idx = 0, rc, phys = -1;

  if( mm->keepSorted ){
    idx = bsearch_key(mm, pKey, nKey, intKey, &found);
    if( found ){
      phys = mm->aOrder[idx];
    }
  }else{
    rc = findPhysLazy(mm, pKey, nKey, intKey, &phys);
    if( rc!=SQLITE_OK ) return rc;
    found = (phys >= 0);
  }

  if( found ){
    ProllyMutMapEntry *e = &mm->aEntries[phys];

    if( mm->currentSavepointLevel > 0
     && decodeLevel(mm, e->bornAt) < mm->currentSavepointLevel ){
      rc = appendUndoRec(mm, phys);
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
    e->bornAt = encodeLevel(mm, mm->currentSavepointLevel);
    return SQLITE_OK;
  }

  rc = ensureCapacity(mm);
  if( rc!=SQLITE_OK ) return rc;
  rc = ensureHashForInsert(mm);
  if( rc!=SQLITE_OK ) return rc;

  {
    int i;
    ProllyMutMapEntry *e;
    phys = mm->nEntries;
    e = &mm->aEntries[phys];
    memset(e, 0, sizeof(*e));
    e->op = PROLLY_EDIT_INSERT;
    e->isIntKey = mm->isIntKey;
    e->intKey = intKey;
    e->bornAt = encodeLevel(mm, mm->currentSavepointLevel);
    rc = copyEntryData(e, pKey, nKey, pVal, nVal);
    if( rc!=SQLITE_OK ){
      return rc;
    }
    if( mm->keepSorted ){
      if( idx < mm->nEntries ){
        memmove(&mm->aOrder[idx+1], &mm->aOrder[idx],
                (mm->nEntries - idx) * sizeof(int));
      }
      mm->aOrder[idx] = phys;
      mm->aPos[phys] = idx;
      for(i = idx + 1; i <= mm->nEntries; i++){
        mm->aPos[mm->aOrder[i]] = i;
      }
    }else{
      mm->aOrder[phys] = phys;
      mm->aPos[phys] = phys;
      mm->orderDirty = 1;
    }
  }

  mm->nEntries++;
  if( !mm->keepSorted ){
    hashInsertPhys(mm, phys);
  }
  return SQLITE_OK;
}

int prollyMutMapDelete(
  ProllyMutMap *mm,
  const u8 *pKey, int nKey, i64 intKey
){
  int found = 0, idx = 0, rc, phys = -1;

  if( mm->keepSorted ){
    idx = bsearch_key(mm, pKey, nKey, intKey, &found);
    if( found ){
      phys = mm->aOrder[idx];
    }
  }else{
    rc = findPhysLazy(mm, pKey, nKey, intKey, &phys);
    if( rc!=SQLITE_OK ) return rc;
    found = (phys >= 0);
  }

  if( found ){
    ProllyMutMapEntry *e = &mm->aEntries[phys];
    if( e->op == PROLLY_EDIT_INSERT ){
      if( mm->currentSavepointLevel > 0
       && decodeLevel(mm, e->bornAt) < mm->currentSavepointLevel ){
        rc = appendUndoRec(mm, phys);
        if( rc!=SQLITE_OK ) return rc;
      }
      e->op = PROLLY_EDIT_DELETE;
      sqlite3_free(e->pVal);
      e->pVal = 0;
      e->nVal = 0;
      e->bornAt = encodeLevel(mm, mm->currentSavepointLevel);
      return SQLITE_OK;
    }

    return SQLITE_OK;
  }

  rc = ensureCapacity(mm);
  if( rc!=SQLITE_OK ) return rc;
  rc = ensureHashForInsert(mm);
  if( rc!=SQLITE_OK ) return rc;

  {
    int i;
    ProllyMutMapEntry *e;
    phys = mm->nEntries;
    e = &mm->aEntries[phys];
    memset(e, 0, sizeof(*e));
    e->op = PROLLY_EDIT_DELETE;
    e->isIntKey = mm->isIntKey;
    e->intKey = intKey;
    e->bornAt = encodeLevel(mm, mm->currentSavepointLevel);
    rc = copyEntryData(e, pKey, nKey, 0, 0);
    if( rc!=SQLITE_OK ){
      return rc;
    }
    if( mm->keepSorted ){
      if( idx < mm->nEntries ){
        memmove(&mm->aOrder[idx+1], &mm->aOrder[idx],
                (mm->nEntries - idx) * sizeof(int));
      }
      mm->aOrder[idx] = phys;
      mm->aPos[phys] = idx;
      for(i = idx + 1; i <= mm->nEntries; i++){
        mm->aPos[mm->aOrder[i]] = i;
      }
    }else{
      mm->aOrder[phys] = phys;
      mm->aPos[phys] = phys;
      mm->orderDirty = 1;
    }
  }

  mm->nEntries++;
  if( !mm->keepSorted ){
    hashInsertPhys(mm, phys);
  }
  return SQLITE_OK;
}

void prollyMutMapPushSavepoint(ProllyMutMap *mm, int level){
  if( !mm ) return;
  mm->currentSavepointLevel = level;
}

/* Order matters: restore in-place mutations from the undo log
** FIRST, then drop fresh-key inserts with bornAt >= level. Doing it
** the other way would drop in-place-mutated entries before their
** undo record gets applied, losing the restored state. */
int prollyMutMapRollbackToSavepoint(ProllyMutMap *mm, int level){
  int i;
  if( !mm ) return SQLITE_OK;

  while( mm->nUndo > 0
      && mm->aUndo[mm->nUndo - 1].level >= level ){
    ProllyMutMapUndoRec *rec = &mm->aUndo[mm->nUndo - 1];
    int idx = rec->entryIdx;
    if( idx >= 0 && idx < mm->nEntries ){
      ProllyMutMapEntry *e = &mm->aEntries[idx];
      e->op = rec->prevOp;
      e->bornAt = encodeLevel(mm, rec->prevBornAt);
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


  {
    int oldN = mm->nEntries;
    int *aMap = 0;
    int newN = 0;
    if( oldN > 0 ){
      aMap = sqlite3_malloc(oldN * sizeof(int));
      if( !aMap ) return SQLITE_NOMEM;
    }
    for(i=0; i<oldN; i++){
      if( decodeLevel(mm, mm->aEntries[i].bornAt) >= level ){
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
      if( mm->keepSorted ){
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
      }else{
        mm->nEntries = newN;
        mm->orderDirty = 1;
      }
      for(j=0; j<mm->nUndo; j++){
        mm->aUndo[j].entryIdx = aMap[mm->aUndo[j].entryIdx];
      }
    }
    sqlite3_free(aMap);
  }

  if( !mm->keepSorted ){
    int rc = rebuildHash(mm);
    if( rc!=SQLITE_OK ) return rc;
  }

  mm->currentSavepointLevel = level - 1;
  return SQLITE_OK;
}

void prollyMutMapReleaseSavepoint(ProllyMutMap *mm, int level){
  int i;
  int target;
  if( !mm ) return;
  target = level - 1;

  if( level==1 && target==0 ){
    for(i=0; i<mm->nUndo; i++){
      sqlite3_free(mm->aUndo[i].prevVal);
      mm->aUndo[i].prevVal = 0;
    }
    mm->nUndo = 0;
    mm->levelBase++;
    mm->currentSavepointLevel = 0;
    return;
  }

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
    if( decodeLevel(mm, mm->aEntries[i].bornAt) >= level ){
      mm->aEntries[i].bornAt = encodeLevel(mm, target);
    }
  }

  mm->currentSavepointLevel = target;
}

int prollyMutMapFindRc(
  ProllyMutMap *mm,
  const u8 *pKey, int nKey, i64 intKey,
  ProllyMutMapEntry **ppEntry
){
  int found, idx, phys, rc;
  *ppEntry = 0;
  if( mm->nEntries==0 ) return SQLITE_OK;
  if( mm->keepSorted ){
    idx = bsearch_key(mm, pKey, nKey, intKey, &found);
    *ppEntry = found ? entryAtOrder(mm, idx) : 0;
    return SQLITE_OK;
  }
  rc = findPhysLazy(mm, pKey, nKey, intKey, &phys);
  if( rc!=SQLITE_OK ) return rc;
  *ppEntry = phys >= 0 ? &mm->aEntries[phys] : 0;
  return SQLITE_OK;
}

ProllyMutMapEntry *prollyMutMapFind(ProllyMutMap *mm,
                                     const u8 *pKey, int nKey, i64 intKey){
  ProllyMutMapEntry *pEntry = 0;
  if( prollyMutMapFindRc(mm, pKey, nKey, intKey, &pEntry)!=SQLITE_OK ){
    return 0;
  }
  return pEntry;
}

ProllyMutMapEntry *prollyMutMapEntryAt(ProllyMutMap *mm, int idx){
  ensureOrder(mm);
  return entryAtOrder(mm, idx);
}

int prollyMutMapOrderIndexFromEntry(ProllyMutMap *mm, ProllyMutMapEntry *pEntry){
  int phys = (int)(pEntry - mm->aEntries);
  if( !mm->keepSorted && mm->orderDirty ){
    return rankEntryWithoutOrder(mm, phys);
  }
  ensureOrder(mm);
  return mm->aPos[phys];
}

int prollyMutMapCount(ProllyMutMap *mm){
  return mm->nEntries;
}

int prollyMutMapIsEmpty(ProllyMutMap *mm){
  return mm->nEntries == 0;
}

void prollyMutMapIterFirst(ProllyMutMapIter *it, ProllyMutMap *mm){
  ensureOrder(mm);
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
  ensureOrder(it->pMap);
  return entryAtOrder(it->pMap, it->idx);
}

void prollyMutMapIterSeek(ProllyMutMapIter *it, ProllyMutMap *mm,
                          const u8 *pKey, int nKey, i64 intKey){
  int found = 0;
  ensureOrder(mm);
  it->pMap = mm;
  it->idx = bsearch_key(mm, pKey, nKey, intKey, &found);
}

void prollyMutMapIterLast(ProllyMutMapIter *it, ProllyMutMap *mm){
  ensureOrder(mm);
  it->pMap = mm;
  it->idx = mm->nEntries>0 ? mm->nEntries - 1 : mm->nEntries;
}

/* Wipes data only — currentSavepointLevel is context not data. A
** flushMutMap mid-savepoint calls clear and the mutmap continues to
** live under the same savepoint, so subsequent writes must still
** attribute to that level. */
void prollyMutMapClear(ProllyMutMap *mm){
  int i;
  for(i=0; i<mm->nEntries; i++){
    freeEntryData(&mm->aEntries[i]);
  }
  mm->nEntries = 0;
  mm->orderDirty = 0;
  if( mm->aHash && mm->nHashAlloc>0 ){
    memset(mm->aHash, 0, mm->nHashAlloc * sizeof(int));
  }
  for(i=0; i<mm->nUndo; i++){
    sqlite3_free(mm->aUndo[i].prevVal);
  }
  mm->nUndo = 0;
  mm->levelBase = 0;
}

void prollyMutMapFree(ProllyMutMap *mm){
  prollyMutMapClear(mm);
  sqlite3_free(mm->aEntries);
  mm->aEntries = 0;
  sqlite3_free(mm->aOrder);
  mm->aOrder = 0;
  sqlite3_free(mm->aPos);
  mm->aPos = 0;
  sqlite3_free(mm->aHash);
  mm->aHash = 0;
  mm->nHashAlloc = 0;
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
  dst->keepSorted = src->keepSorted;
  dst->orderDirty = src->orderDirty;
  dst->levelBase = src->levelBase;
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
    if( src->keepSorted || !src->orderDirty ){
      memcpy(dst->aOrder, src->aOrder, src->nEntries * sizeof(int));
      memcpy(dst->aPos, src->aPos, src->nEntries * sizeof(int));
    }else{
      memset(dst->aOrder, 0, src->nEntries * sizeof(int));
      memset(dst->aPos, 0, src->nEntries * sizeof(int));
    }
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

  if( !dst->keepSorted && dst->nEntries > 0 ){
    int rc = rebuildHash(dst);
    if( rc!=SQLITE_OK ){
      prollyMutMapFree(dst);
      sqlite3_free(dst);
      return rc;
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
