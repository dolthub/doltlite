
#ifdef DOLTLITE_PROLLY

#include "prolly_mutmap.h"
#include "prolly_node.h"
#include <assert.h>
#include <string.h>
#include <stdlib.h>

#define MUTMAP_INIT_CAP 16
#define MUTMAP_MIN_HASH 32

struct ProllyMutMapOps {
  int (*xInsert)(ProllyMutMap*, const u8*, int, i64, const u8*, int);
  int (*xDelete)(ProllyMutMap*, const u8*, int, i64);
  int (*xFindRc)(ProllyMutMap*, const u8*, int, i64, ProllyMutMapEntry**);
  ProllyMutMapEntry *(*xFind)(ProllyMutMap*, const u8*, int, i64);
  ProllyMutMapEntry *(*xEntryAt)(ProllyMutMap*, int);
  int (*xOrderIndexFromEntry)(ProllyMutMap*, ProllyMutMapEntry*);
  void (*xIterFirst)(ProllyMutMapIter*, ProllyMutMap*);
  ProllyMutMapEntry *(*xIterEntry)(ProllyMutMapIter*);
  void (*xIterSeek)(ProllyMutMapIter*, ProllyMutMap*, const u8*, int, i64);
  void (*xIterLast)(ProllyMutMapIter*, ProllyMutMap*);
  int (*xRollbackToSavepoint)(ProllyMutMap*, int);
  void (*xReleaseSavepoint)(ProllyMutMap*, int);
  int (*xClone)(ProllyMutMap**, const ProllyMutMap*);
};

static const ProllyMutMapOps gLegacyMutMapOps;
static const ProllyMutMapOps gIndexMutMapOps;

static const ProllyMutMapOps *indexPrimaryOps(void){
  return &gLegacyMutMapOps;
}

static const ProllyMutMapOps *indexShadowOps(void){
  return &gIndexMutMapOps;
}

#ifdef DOLTLITE_MUTMAP_SHADOW_VALIDATE
static int shadowEnsureInit(ProllyMutMap *mm);
static int shadowValidateMatches(ProllyMutMap *mm);
#endif

typedef struct IndexMutMapState IndexMutMapState;
struct IndexMutMapState {
  int root;
  int *aLeft;
  int *aRight;
  int *aParent;
  int *aSize;
  u32 *aPriority;
  int nAlloc;
  int nBuilt;
  u8 bDirty;
};
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

static IndexMutMapState *indexState(ProllyMutMap *mm){
  return (IndexMutMapState*)mm->pImpl;
}

static int indexEnsureStateCapacity(ProllyMutMap *mm, int nMin){
  IndexMutMapState *st = indexState(mm);
  int nNew;
  int *aLeftNew;
  int *aRightNew;
  int *aParentNew;
  int *aSizeNew;
  u32 *aPriorityNew;
  if( !st ) return SQLITE_MISUSE;
  if( st->nAlloc >= nMin ) return SQLITE_OK;
  nNew = st->nAlloc ? st->nAlloc * 2 : MUTMAP_INIT_CAP;
  while( nNew < nMin ) nNew *= 2;
  aLeftNew = sqlite3_malloc(nNew * sizeof(int));
  aRightNew = sqlite3_malloc(nNew * sizeof(int));
  aParentNew = sqlite3_malloc(nNew * sizeof(int));
  aSizeNew = sqlite3_malloc(nNew * sizeof(int));
  aPriorityNew = sqlite3_malloc(nNew * sizeof(u32));
  if( !aLeftNew || !aRightNew || !aParentNew || !aSizeNew || !aPriorityNew ){
    sqlite3_free(aLeftNew);
    sqlite3_free(aRightNew);
    sqlite3_free(aParentNew);
    sqlite3_free(aSizeNew);
    sqlite3_free(aPriorityNew);
    return SQLITE_NOMEM;
  }
  if( st->nAlloc > 0 ){
    memcpy(aLeftNew, st->aLeft, st->nAlloc * sizeof(int));
    memcpy(aRightNew, st->aRight, st->nAlloc * sizeof(int));
    memcpy(aParentNew, st->aParent, st->nAlloc * sizeof(int));
    memcpy(aSizeNew, st->aSize, st->nAlloc * sizeof(int));
    memcpy(aPriorityNew, st->aPriority, st->nAlloc * sizeof(u32));
  }
  sqlite3_free(st->aLeft);
  sqlite3_free(st->aRight);
  sqlite3_free(st->aParent);
  sqlite3_free(st->aSize);
  sqlite3_free(st->aPriority);
  st->aLeft = aLeftNew;
  st->aRight = aRightNew;
  st->aParent = aParentNew;
  st->aSize = aSizeNew;
  st->aPriority = aPriorityNew;
  st->nAlloc = nNew;
  return SQLITE_OK;
}

static void indexRefreshSize(IndexMutMapState *st, int idx){
  int leftSize = st->aLeft[idx] >= 0 ? st->aSize[st->aLeft[idx]] : 0;
  int rightSize = st->aRight[idx] >= 0 ? st->aSize[st->aRight[idx]] : 0;
  st->aSize[idx] = leftSize + rightSize + 1;
}

static void indexRefreshSizeUp(IndexMutMapState *st, int idx){
  while( idx >= 0 ){
    indexRefreshSize(st, idx);
    idx = st->aParent[idx];
  }
}

static void indexRotateLeft(IndexMutMapState *st, int x){
  int y = st->aRight[x];
  int p = st->aParent[x];
  st->aRight[x] = st->aLeft[y];
  if( st->aLeft[y] >= 0 ) st->aParent[st->aLeft[y]] = x;
  st->aLeft[y] = x;
  st->aParent[x] = y;
  st->aParent[y] = p;
  if( p >= 0 ){
    if( st->aLeft[p] == x ) st->aLeft[p] = y;
    else st->aRight[p] = y;
  }else{
    st->root = y;
  }
  indexRefreshSize(st, x);
  indexRefreshSize(st, y);
}

static void indexRotateRight(IndexMutMapState *st, int x){
  int y = st->aLeft[x];
  int p = st->aParent[x];
  st->aLeft[x] = st->aRight[y];
  if( st->aRight[y] >= 0 ) st->aParent[st->aRight[y]] = x;
  st->aRight[y] = x;
  st->aParent[x] = y;
  st->aParent[y] = p;
  if( p >= 0 ){
    if( st->aLeft[p] == x ) st->aLeft[p] = y;
    else st->aRight[p] = y;
  }else{
    st->root = y;
  }
  indexRefreshSize(st, x);
  indexRefreshSize(st, y);
}

static int indexRankForPhys(ProllyMutMap *mm, int phys){
  IndexMutMapState *st = indexState(mm);
  int rank = st->aLeft[phys] >= 0 ? st->aSize[st->aLeft[phys]] : 0;
  while( st->aParent[phys] >= 0 ){
    int parent = st->aParent[phys];
    if( st->aRight[parent] == phys ){
      rank += 1;
      if( st->aLeft[parent] >= 0 ) rank += st->aSize[st->aLeft[parent]];
    }
    phys = parent;
  }
  return rank;
}

static int indexSelectPhys(IndexMutMapState *st, int rank){
  int cur = st->root;
  while( cur >= 0 ){
    int leftSize = st->aLeft[cur] >= 0 ? st->aSize[st->aLeft[cur]] : 0;
    if( rank < leftSize ){
      cur = st->aLeft[cur];
    }else if( rank > leftSize ){
      rank -= leftSize + 1;
      cur = st->aRight[cur];
    }else{
      return cur;
    }
  }
  return -1;
}

static int indexLowerBoundRank(
  ProllyMutMap *mm, const u8 *pKey, int nKey, i64 intKey, int *pFound, int *pPhys
){
  IndexMutMapState *st = indexState(mm);
  int cur = st->root;
  int candidate = -1;
  *pFound = 0;
  *pPhys = -1;
  while( cur >= 0 ){
    ProllyMutMapEntry *e = &mm->aEntries[cur];
    int c = compareEntries(mm->isIntKey, e->pKey, e->nKey, e->intKey, pKey, nKey, intKey);
    if( c < 0 ){
      cur = st->aRight[cur];
    }else if( c > 0 ){
      candidate = cur;
      cur = st->aLeft[cur];
    }else{
      *pFound = 1;
      *pPhys = cur;
      return indexRankForPhys(mm, cur);
    }
  }
  if( candidate >= 0 ){
    *pPhys = candidate;
    return indexRankForPhys(mm, candidate);
  }
  return mm->nEntries;
}

static int indexInsertPhys(ProllyMutMap *mm, int phys){
  IndexMutMapState *st = indexState(mm);
  int cur;
  int parent = -1;
  int rc;
  rc = indexEnsureStateCapacity(mm, phys + 1);
  if( rc!=SQLITE_OK ) return rc;
  st->aLeft[phys] = -1;
  st->aRight[phys] = -1;
  st->aParent[phys] = -1;
  st->aSize[phys] = 1;
  st->aPriority[phys] = hashKey(
      mm->isIntKey, mm->aEntries[phys].pKey, mm->aEntries[phys].nKey, mm->aEntries[phys].intKey);
  if( st->root < 0 ){
    st->root = phys;
    return SQLITE_OK;
  }
  cur = st->root;
  while( cur >= 0 ){
    ProllyMutMapEntry *e = &mm->aEntries[cur];
    int c = compareEntries(mm->isIntKey,
                           mm->aEntries[phys].pKey, mm->aEntries[phys].nKey, mm->aEntries[phys].intKey,
                           e->pKey, e->nKey, e->intKey);
    parent = cur;
    if( c < 0 ){
      cur = st->aLeft[cur];
    }else{
      cur = st->aRight[cur];
    }
  }
  st->aParent[phys] = parent;
  if( compareEntries(mm->isIntKey,
                     mm->aEntries[phys].pKey, mm->aEntries[phys].nKey, mm->aEntries[phys].intKey,
                     mm->aEntries[parent].pKey, mm->aEntries[parent].nKey, mm->aEntries[parent].intKey) < 0 ){
    st->aLeft[parent] = phys;
  }else{
    st->aRight[parent] = phys;
  }
  indexRefreshSizeUp(st, parent);
  while( st->aParent[phys] >= 0 &&
         st->aPriority[phys] < st->aPriority[st->aParent[phys]] ){
    int p = st->aParent[phys];
    if( st->aLeft[p] == phys ) indexRotateRight(st, p);
    else indexRotateLeft(st, p);
  }
  return SQLITE_OK;
}

static int indexRebuildTree(ProllyMutMap *mm){
  IndexMutMapState *st = indexState(mm);
  int i;
  int rc = indexEnsureStateCapacity(mm, mm->nEntries);
  if( rc!=SQLITE_OK ) return rc;
  st->root = -1;
  for(i=0; i<mm->nEntries; i++){
    st->aLeft[i] = -1;
    st->aRight[i] = -1;
    st->aParent[i] = -1;
    st->aSize[i] = 1;
    st->aPriority[i] = hashKey(mm->isIntKey, mm->aEntries[i].pKey, mm->aEntries[i].nKey, mm->aEntries[i].intKey);
  }
  for(i=0; i<mm->nEntries; i++){
    rc = indexInsertPhys(mm, i);
    if( rc!=SQLITE_OK ) return rc;
  }
  return SQLITE_OK;
}

static int indexEnsureInit(ProllyMutMap *mm){
  if( mm->pImpl ) return SQLITE_OK;
  mm->pImpl = sqlite3_malloc(sizeof(IndexMutMapState));
  if( !mm->pImpl ) return SQLITE_NOMEM;
  memset(mm->pImpl, 0, sizeof(IndexMutMapState));
  indexState(mm)->root = -1;
  return SQLITE_OK;
}

static void indexStateClear(IndexMutMapState *st){
  if( !st ) return;
  st->root = -1;
  st->nBuilt = 0;
  st->bDirty = 0;
}

static void indexStateFree(IndexMutMapState *st){
  if( !st ) return;
  sqlite3_free(st->aLeft);
  sqlite3_free(st->aRight);
  sqlite3_free(st->aParent);
  sqlite3_free(st->aSize);
  sqlite3_free(st->aPriority);
  sqlite3_free(st);
}

int prollyMutMapInit(ProllyMutMap *mm, u8 isIntKey){
  return prollyMutMapInitKind(mm, isIntKey, 1, PROLLY_MUTMAP_KIND_TABLE);
}

int prollyMutMapInitMode(ProllyMutMap *mm, u8 isIntKey, u8 keepSorted){
  ProllyMutMapKind eKind = keepSorted
    ? PROLLY_MUTMAP_KIND_TABLE
    : PROLLY_MUTMAP_KIND_INDEX;
  return prollyMutMapInitKind(mm, isIntKey, keepSorted, eKind);
}

int prollyMutMapInitKind(
  ProllyMutMap *mm, u8 isIntKey, u8 keepSorted, ProllyMutMapKind eKind
){
  int rc = SQLITE_OK;
  memset(mm, 0, sizeof(*mm));
  mm->isIntKey = isIntKey;
  mm->keepSorted = keepSorted;
  mm->eKind = (u8)eKind;
  mm->pOps = (eKind==PROLLY_MUTMAP_KIND_INDEX)
    ? indexPrimaryOps()
    : &gLegacyMutMapOps;
  if( eKind==PROLLY_MUTMAP_KIND_INDEX && mm->pOps==&gIndexMutMapOps ){
    rc = indexEnsureInit(mm);
    if( rc!=SQLITE_OK ) return rc;
  }
#ifdef DOLTLITE_MUTMAP_SHADOW_VALIDATE
  if( eKind==PROLLY_MUTMAP_KIND_INDEX ){
    rc = shadowEnsureInit(mm);
    if( rc!=SQLITE_OK ){
      return rc;
    }
  }
#endif
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

static int prollyMutMapInsertLegacy(
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

static int prollyMutMapDeleteLegacy(
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
#ifdef DOLTLITE_MUTMAP_SHADOW_VALIDATE
  if( mm->pShadow ){
    mm->pShadow->currentSavepointLevel = level;
  }
#endif
}

/* Order matters: restore in-place mutations from the undo log
** FIRST, then drop fresh-key inserts with bornAt >= level. Doing it
** the other way would drop in-place-mutated entries before their
** undo record gets applied, losing the restored state. */
static int prollyMutMapRollbackToSavepointLegacy(ProllyMutMap *mm, int level){
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

static void prollyMutMapReleaseSavepointLegacy(ProllyMutMap *mm, int level){
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

static int prollyMutMapFindRcLegacy(
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

static ProllyMutMapEntry *prollyMutMapFindLegacy(
  ProllyMutMap *mm,
  const u8 *pKey, int nKey, i64 intKey
){
  ProllyMutMapEntry *pEntry = 0;
  if( prollyMutMapFindRcLegacy(mm, pKey, nKey, intKey, &pEntry)!=SQLITE_OK ){
    return 0;
  }
  return pEntry;
}

static ProllyMutMapEntry *prollyMutMapEntryAtLegacy(ProllyMutMap *mm, int idx){
  ensureOrder(mm);
  return entryAtOrder(mm, idx);
}

static int prollyMutMapOrderIndexFromEntryLegacy(
  ProllyMutMap *mm, ProllyMutMapEntry *pEntry
){
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

static void prollyMutMapIterFirstLegacy(ProllyMutMapIter *it, ProllyMutMap *mm){
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

static ProllyMutMapEntry *prollyMutMapIterEntryLegacy(ProllyMutMapIter *it){
  ensureOrder(it->pMap);
  return entryAtOrder(it->pMap, it->idx);
}

static void prollyMutMapIterSeekLegacy(
  ProllyMutMapIter *it, ProllyMutMap *mm,
  const u8 *pKey, int nKey, i64 intKey
){
  int found = 0;
  ensureOrder(mm);
  it->pMap = mm;
  it->idx = bsearch_key(mm, pKey, nKey, intKey, &found);
}

static void prollyMutMapIterLastLegacy(ProllyMutMapIter *it, ProllyMutMap *mm){
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
#ifdef DOLTLITE_MUTMAP_SHADOW_VALIDATE
  if( mm->pShadow ){
    prollyMutMapClear(mm->pShadow);
  }
#endif
  if( mm->eKind==PROLLY_MUTMAP_KIND_INDEX ){
    indexStateClear(indexState(mm));
  }
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
#ifdef DOLTLITE_MUTMAP_SHADOW_VALIDATE
  if( mm->pShadow ){
    prollyMutMapFree(mm->pShadow);
    sqlite3_free(mm->pShadow);
    mm->pShadow = 0;
  }
#endif
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
  indexStateFree((IndexMutMapState*)mm->pImpl);
  mm->pImpl = 0;
}

static int prollyMutMapCloneLegacy(ProllyMutMap **out, const ProllyMutMap *src){
  ProllyMutMap *dst;
  int i;
  *out = 0;
  dst = (ProllyMutMap*)sqlite3_malloc(sizeof(ProllyMutMap));
  if( !dst ) return SQLITE_NOMEM;
  memset(dst, 0, sizeof(*dst));
  dst->isIntKey = src->isIntKey;
  dst->keepSorted = src->keepSorted;
  dst->orderDirty = src->orderDirty;
  dst->eKind = src->eKind;
  dst->pOps = src->pOps;
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

#ifdef DOLTLITE_MUTMAP_SHADOW_VALIDATE
static int shadowEnsureInit(ProllyMutMap *mm){
  if( mm->pShadow || mm->eKind!=PROLLY_MUTMAP_KIND_INDEX ) return SQLITE_OK;
  mm->pShadow = sqlite3_malloc(sizeof(ProllyMutMap));
  if( !mm->pShadow ) return SQLITE_NOMEM;
  memset(mm->pShadow, 0, sizeof(ProllyMutMap));
  mm->pShadow->isIntKey = mm->isIntKey;
  mm->pShadow->keepSorted = 0;
  mm->pShadow->eKind = PROLLY_MUTMAP_KIND_INDEX;
  mm->pShadow->pOps = indexShadowOps();
  mm->pShadow->currentSavepointLevel = mm->currentSavepointLevel;
  if( mm->pShadow->pOps == &gIndexMutMapOps ){
    int rc = indexEnsureInit(mm->pShadow);
    if( rc!=SQLITE_OK ){
      sqlite3_free(mm->pShadow);
      mm->pShadow = 0;
      return rc;
    }
  }
  return SQLITE_OK;
}

static int shadowValidateMatches(ProllyMutMap *mm){
  ProllyMutMapIter aIt;
  ProllyMutMapIter bIt;
  int i;
  if( !mm->pShadow ) return SQLITE_OK;
  if( mm->nEntries != mm->pShadow->nEntries ) return SQLITE_CORRUPT;
  mm->pOps->xIterFirst(&aIt, mm);
  mm->pShadow->pOps->xIterFirst(&bIt, mm->pShadow);
  for(i=0; i<mm->nEntries; i++){
    ProllyMutMapEntry *aEntry;
    ProllyMutMapEntry *bEntry;
    if( !prollyMutMapIterValid(&aIt) || !prollyMutMapIterValid(&bIt) ){
      return SQLITE_CORRUPT;
    }
    aEntry = mm->pOps->xIterEntry(&aIt);
    bEntry = mm->pShadow->pOps->xIterEntry(&bIt);
    if( !aEntry || !bEntry ) return SQLITE_CORRUPT;
    if( aEntry->op != bEntry->op ) return SQLITE_CORRUPT;
    if( aEntry->intKey != bEntry->intKey ) return SQLITE_CORRUPT;
    if( aEntry->nKey != bEntry->nKey ) return SQLITE_CORRUPT;
    if( aEntry->nVal != bEntry->nVal ) return SQLITE_CORRUPT;
    if( aEntry->nKey > 0 && memcmp(aEntry->pKey, bEntry->pKey, aEntry->nKey)!=0 ){
      return SQLITE_CORRUPT;
    }
    if( aEntry->nVal > 0 && memcmp(aEntry->pVal, bEntry->pVal, aEntry->nVal)!=0 ){
      return SQLITE_CORRUPT;
    }
    prollyMutMapIterNext(&aIt);
    prollyMutMapIterNext(&bIt);
  }
  if( prollyMutMapIterValid(&aIt) || prollyMutMapIterValid(&bIt) ){
    return SQLITE_CORRUPT;
  }
  return SQLITE_OK;
}
#endif

static const ProllyMutMapOps gLegacyMutMapOps = {
  prollyMutMapInsertLegacy,
  prollyMutMapDeleteLegacy,
  prollyMutMapFindRcLegacy,
  prollyMutMapFindLegacy,
  prollyMutMapEntryAtLegacy,
  prollyMutMapOrderIndexFromEntryLegacy,
  prollyMutMapIterFirstLegacy,
  prollyMutMapIterEntryLegacy,
  prollyMutMapIterSeekLegacy,
  prollyMutMapIterLastLegacy,
  prollyMutMapRollbackToSavepointLegacy,
  prollyMutMapReleaseSavepointLegacy,
  prollyMutMapCloneLegacy
};

static int prollyMutMapInsertIndex(
  ProllyMutMap *mm,
  const u8 *pKey, int nKey, i64 intKey,
  const u8 *pVal, int nVal
){
  int rc;
  int phys = -1;
  if( !mm->pImpl ){
    rc = indexEnsureInit(mm);
    if( rc!=SQLITE_OK ) return rc;
  }
  rc = findPhysLazy(mm, pKey, nKey, intKey, &phys);
  if( rc!=SQLITE_OK ) return rc;
  if( phys >= 0 ){
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
  phys = mm->nEntries;
  memset(&mm->aEntries[phys], 0, sizeof(ProllyMutMapEntry));
  mm->aEntries[phys].op = PROLLY_EDIT_INSERT;
  mm->aEntries[phys].isIntKey = mm->isIntKey;
  mm->aEntries[phys].intKey = intKey;
  mm->aEntries[phys].bornAt = encodeLevel(mm, mm->currentSavepointLevel);
  rc = copyEntryData(&mm->aEntries[phys], pKey, nKey, pVal, nVal);
  if( rc!=SQLITE_OK ) return rc;
  mm->nEntries++;
  hashInsertPhys(mm, phys);
  indexState(mm)->bDirty = 1;
  return SQLITE_OK;
}

static int prollyMutMapDeleteIndex(
  ProllyMutMap *mm,
  const u8 *pKey, int nKey, i64 intKey
){
  int rc;
  int phys = -1;
  if( !mm->pImpl ){
    rc = indexEnsureInit(mm);
    if( rc!=SQLITE_OK ) return rc;
  }
  rc = findPhysLazy(mm, pKey, nKey, intKey, &phys);
  if( rc!=SQLITE_OK ) return rc;
  if( phys >= 0 ){
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
    }
    return SQLITE_OK;
  }
  rc = ensureCapacity(mm);
  if( rc!=SQLITE_OK ) return rc;
  rc = ensureHashForInsert(mm);
  if( rc!=SQLITE_OK ) return rc;
  phys = mm->nEntries;
  memset(&mm->aEntries[phys], 0, sizeof(ProllyMutMapEntry));
  mm->aEntries[phys].op = PROLLY_EDIT_DELETE;
  mm->aEntries[phys].isIntKey = mm->isIntKey;
  mm->aEntries[phys].intKey = intKey;
  mm->aEntries[phys].bornAt = encodeLevel(mm, mm->currentSavepointLevel);
  rc = copyEntryData(&mm->aEntries[phys], pKey, nKey, 0, 0);
  if( rc!=SQLITE_OK ) return rc;
  mm->nEntries++;
  hashInsertPhys(mm, phys);
  indexState(mm)->bDirty = 1;
  return SQLITE_OK;
}

static int prollyMutMapFindRcIndex(
  ProllyMutMap *mm,
  const u8 *pKey, int nKey, i64 intKey,
  ProllyMutMapEntry **ppEntry
){
  int phys = -1;
  int rc;
  *ppEntry = 0;
  if( mm->nEntries==0 ) return SQLITE_OK;
  rc = findPhysLazy(mm, pKey, nKey, intKey, &phys);
  if( rc!=SQLITE_OK ) return rc;
  if( phys >= 0 ) *ppEntry = &mm->aEntries[phys];
  return SQLITE_OK;
}

static ProllyMutMapEntry *prollyMutMapFindIndex(
  ProllyMutMap *mm, const u8 *pKey, int nKey, i64 intKey
){
  ProllyMutMapEntry *pEntry = 0;
  if( prollyMutMapFindRcIndex(mm, pKey, nKey, intKey, &pEntry)!=SQLITE_OK ){
    return 0;
  }
  return pEntry;
}

static int indexEnsureOrdered(ProllyMutMap *mm){
  IndexMutMapState *st = indexState(mm);
  if( !st ) return SQLITE_MISUSE;
  if( st->bDirty || st->nBuilt != mm->nEntries ){
    int rc = indexRebuildTree(mm);
    if( rc!=SQLITE_OK ) return rc;
    st->nBuilt = mm->nEntries;
    st->bDirty = 0;
  }
  return SQLITE_OK;
}

static ProllyMutMapEntry *prollyMutMapEntryAtIndex(ProllyMutMap *mm, int idx){
  int phys;
  if( idx < 0 || idx >= mm->nEntries ) return 0;
  if( indexEnsureOrdered(mm)!=SQLITE_OK ) return 0;
  phys = indexSelectPhys(indexState(mm), idx);
  return phys >= 0 ? &mm->aEntries[phys] : 0;
}

static int prollyMutMapOrderIndexFromEntryIndex(
  ProllyMutMap *mm, ProllyMutMapEntry *pEntry
){
  if( indexEnsureOrdered(mm)!=SQLITE_OK ) return -1;
  return indexRankForPhys(mm, (int)(pEntry - mm->aEntries));
}

static void prollyMutMapIterFirstIndex(ProllyMutMapIter *it, ProllyMutMap *mm){
  indexEnsureOrdered(mm);
  it->pMap = mm;
  it->idx = 0;
}

static ProllyMutMapEntry *prollyMutMapIterEntryIndex(ProllyMutMapIter *it){
  return prollyMutMapEntryAtIndex(it->pMap, it->idx);
}

static void prollyMutMapIterSeekIndex(
  ProllyMutMapIter *it, ProllyMutMap *mm, const u8 *pKey, int nKey, i64 intKey
){
  int found;
  int phys;
  indexEnsureOrdered(mm);
  it->pMap = mm;
  it->idx = indexLowerBoundRank(mm, pKey, nKey, intKey, &found, &phys);
}

static void prollyMutMapIterLastIndex(ProllyMutMapIter *it, ProllyMutMap *mm){
  indexEnsureOrdered(mm);
  it->pMap = mm;
  it->idx = mm->nEntries>0 ? mm->nEntries - 1 : 0;
}

static int prollyMutMapRollbackToSavepointIndex(ProllyMutMap *mm, int level){
  int rc = prollyMutMapRollbackToSavepointLegacy(mm, level);
  if( rc!=SQLITE_OK ) return rc;
  indexState(mm)->bDirty = 1;
  indexState(mm)->nBuilt = 0;
  return SQLITE_OK;
}

static void prollyMutMapReleaseSavepointIndex(ProllyMutMap *mm, int level){
  prollyMutMapReleaseSavepointLegacy(mm, level);
  indexState(mm)->bDirty = 1;
  indexState(mm)->nBuilt = 0;
}

static int prollyMutMapCloneIndex(ProllyMutMap **out, const ProllyMutMap *src){
  int rc = prollyMutMapCloneLegacy(out, src);
  if( rc!=SQLITE_OK ) return rc;
  (*out)->pOps = &gIndexMutMapOps;
  rc = indexEnsureInit(*out);
  if( rc!=SQLITE_OK ){
    prollyMutMapFree(*out);
    sqlite3_free(*out);
    *out = 0;
    return rc;
  }
  indexState(*out)->bDirty = 1;
  indexState(*out)->nBuilt = 0;
  return SQLITE_OK;
}

static const ProllyMutMapOps gIndexMutMapOps = {
  prollyMutMapInsertIndex,
  prollyMutMapDeleteIndex,
  prollyMutMapFindRcIndex,
  prollyMutMapFindIndex,
  prollyMutMapEntryAtIndex,
  prollyMutMapOrderIndexFromEntryIndex,
  prollyMutMapIterFirstIndex,
  prollyMutMapIterEntryIndex,
  prollyMutMapIterSeekIndex,
  prollyMutMapIterLastIndex,
  prollyMutMapRollbackToSavepointIndex,
  prollyMutMapReleaseSavepointIndex,
  prollyMutMapCloneIndex
};

int prollyMutMapInsert(
  ProllyMutMap *mm,
  const u8 *pKey, int nKey, i64 intKey,
  const u8 *pVal, int nVal
){
  int rc;
  if( !mm->pOps ) return SQLITE_MISUSE;
  rc = mm->pOps->xInsert(mm, pKey, nKey, intKey, pVal, nVal);
#ifdef DOLTLITE_MUTMAP_SHADOW_VALIDATE
  if( rc==SQLITE_OK && mm->pShadow ){
    rc = mm->pShadow->pOps->xInsert(mm->pShadow, pKey, nKey, intKey, pVal, nVal);
    if( rc==SQLITE_OK ) rc = shadowValidateMatches(mm);
  }
#endif
  return rc;
}

int prollyMutMapDelete(
  ProllyMutMap *mm,
  const u8 *pKey, int nKey, i64 intKey
){
  int rc;
  if( !mm->pOps ) return SQLITE_MISUSE;
  rc = mm->pOps->xDelete(mm, pKey, nKey, intKey);
#ifdef DOLTLITE_MUTMAP_SHADOW_VALIDATE
  if( rc==SQLITE_OK && mm->pShadow ){
    rc = mm->pShadow->pOps->xDelete(mm->pShadow, pKey, nKey, intKey);
    if( rc==SQLITE_OK ) rc = shadowValidateMatches(mm);
  }
#endif
  return rc;
}

int prollyMutMapRollbackToSavepoint(ProllyMutMap *mm, int level){
  int rc;
  if( !mm->pOps ) return SQLITE_MISUSE;
  rc = mm->pOps->xRollbackToSavepoint(mm, level);
#ifdef DOLTLITE_MUTMAP_SHADOW_VALIDATE
  if( rc==SQLITE_OK && mm->pShadow ){
    rc = mm->pShadow->pOps->xRollbackToSavepoint(mm->pShadow, level);
    if( rc==SQLITE_OK ) rc = shadowValidateMatches(mm);
  }
#endif
  return rc;
}

void prollyMutMapReleaseSavepoint(ProllyMutMap *mm, int level){
  if( !mm->pOps ) return;
  mm->pOps->xReleaseSavepoint(mm, level);
#ifdef DOLTLITE_MUTMAP_SHADOW_VALIDATE
  if( mm->pShadow ){
    mm->pShadow->pOps->xReleaseSavepoint(mm->pShadow, level);
    assert(shadowValidateMatches(mm)==SQLITE_OK);
  }
#endif
}

int prollyMutMapFindRc(
  ProllyMutMap *mm,
  const u8 *pKey, int nKey, i64 intKey,
  ProllyMutMapEntry **ppEntry
){
  if( !mm->pOps ){
    *ppEntry = 0;
    return SQLITE_MISUSE;
  }
  return mm->pOps->xFindRc(mm, pKey, nKey, intKey, ppEntry);
}

ProllyMutMapEntry *prollyMutMapFind(
  ProllyMutMap *mm, const u8 *pKey, int nKey, i64 intKey
){
  return mm->pOps ? mm->pOps->xFind(mm, pKey, nKey, intKey) : 0;
}

ProllyMutMapEntry *prollyMutMapEntryAt(ProllyMutMap *mm, int idx){
  return mm->pOps ? mm->pOps->xEntryAt(mm, idx) : 0;
}

int prollyMutMapOrderIndexFromEntry(ProllyMutMap *mm, ProllyMutMapEntry *pEntry){
  return mm->pOps ? mm->pOps->xOrderIndexFromEntry(mm, pEntry) : -1;
}

void prollyMutMapIterFirst(ProllyMutMapIter *it, ProllyMutMap *mm){
  if( mm->pOps ){
    mm->pOps->xIterFirst(it, mm);
  }else{
    it->pMap = mm;
    it->idx = 0;
  }
}

ProllyMutMapEntry *prollyMutMapIterEntry(ProllyMutMapIter *it){
  return it->pMap->pOps ? it->pMap->pOps->xIterEntry(it) : 0;
}

void prollyMutMapIterSeek(
  ProllyMutMapIter *it, ProllyMutMap *mm,
  const u8 *pKey, int nKey, i64 intKey
){
  if( mm->pOps ){
    mm->pOps->xIterSeek(it, mm, pKey, nKey, intKey);
  }else{
    it->pMap = mm;
    it->idx = 0;
  }
}

void prollyMutMapIterLast(ProllyMutMapIter *it, ProllyMutMap *mm){
  if( mm->pOps ){
    mm->pOps->xIterLast(it, mm);
  }else{
    it->pMap = mm;
    it->idx = 0;
  }
}

int prollyMutMapClone(ProllyMutMap **out, const ProllyMutMap *src){
  int rc;
  if( !src->pOps ){
    *out = 0;
    return SQLITE_MISUSE;
  }
  rc = src->pOps->xClone(out, src);
#ifdef DOLTLITE_MUTMAP_SHADOW_VALIDATE
  if( rc==SQLITE_OK && *out && src->pShadow ){
    rc = prollyMutMapClone(&(*out)->pShadow, src->pShadow);
    if( rc!=SQLITE_OK ){
      prollyMutMapFree(*out);
      sqlite3_free(*out);
      *out = 0;
      return rc;
    }
  }
#endif
  return rc;
}

#endif
