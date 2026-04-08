
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
    ProllyMutMapEntry *e = &mm->aEntries[mid];
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
    if( !aNew ) return SQLITE_NOMEM;
    mm->aEntries = aNew;
    mm->nAlloc = nNew;
  }
  return SQLITE_OK;
}

int prollyMutMapInit(ProllyMutMap *mm, u8 isIntKey){
  memset(mm, 0, sizeof(*mm));
  mm->isIntKey = isIntKey;
  return SQLITE_OK;
}

int prollyMutMapInsert(
  ProllyMutMap *mm,
  const u8 *pKey, int nKey, i64 intKey,
  const u8 *pVal, int nVal
){
  int found, idx, rc;

  idx = bsearch_key(mm, pKey, nKey, intKey, &found);

  if( found ){
    
    ProllyMutMapEntry *e = &mm->aEntries[idx];
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
    return SQLITE_OK;
  }

  
  rc = ensureCapacity(mm);
  if( rc!=SQLITE_OK ) return rc;

  
  if( idx < mm->nEntries ){
    memmove(&mm->aEntries[idx+1], &mm->aEntries[idx],
            (mm->nEntries - idx) * sizeof(ProllyMutMapEntry));
  }

  
  {
    ProllyMutMapEntry *e = &mm->aEntries[idx];
    memset(e, 0, sizeof(*e));
    e->op = PROLLY_EDIT_INSERT;
    e->isIntKey = mm->isIntKey;
    e->intKey = intKey;
    rc = copyEntryData(e, pKey, nKey, pVal, nVal);
    if( rc!=SQLITE_OK ){
      
      if( idx < mm->nEntries ){
        memmove(&mm->aEntries[idx], &mm->aEntries[idx+1],
                (mm->nEntries - idx) * sizeof(ProllyMutMapEntry));
      }
      return rc;
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
    ProllyMutMapEntry *e = &mm->aEntries[idx];
    if( e->op == PROLLY_EDIT_INSERT ){
      
      e->op = PROLLY_EDIT_DELETE;
      sqlite3_free(e->pVal);
      e->pVal = 0;
      e->nVal = 0;
      return SQLITE_OK;
    }
    
    return SQLITE_OK;
  }

  
  rc = ensureCapacity(mm);
  if( rc!=SQLITE_OK ) return rc;

  if( idx < mm->nEntries ){
    memmove(&mm->aEntries[idx+1], &mm->aEntries[idx],
            (mm->nEntries - idx) * sizeof(ProllyMutMapEntry));
  }

  {
    ProllyMutMapEntry *e = &mm->aEntries[idx];
    memset(e, 0, sizeof(*e));
    e->op = PROLLY_EDIT_DELETE;
    e->isIntKey = mm->isIntKey;
    e->intKey = intKey;
    rc = copyEntryData(e, pKey, nKey, 0, 0);
    if( rc!=SQLITE_OK ){
      if( idx < mm->nEntries ){
        memmove(&mm->aEntries[idx], &mm->aEntries[idx+1],
                (mm->nEntries - idx) * sizeof(ProllyMutMapEntry));
      }
      return rc;
    }
  }

  mm->nEntries++;
  return SQLITE_OK;
}

ProllyMutMapEntry *prollyMutMapFind(ProllyMutMap *mm,
                                     const u8 *pKey, int nKey, i64 intKey){
  int found, idx;
  if( mm->nEntries==0 ) return 0;
  idx = bsearch_key(mm, pKey, nKey, intKey, &found);
  return found ? &mm->aEntries[idx] : 0;
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
  return it->idx < it->pMap->nEntries;
}

ProllyMutMapEntry *prollyMutMapIterEntry(ProllyMutMapIter *it){
  return &it->pMap->aEntries[it->idx];
}

void prollyMutMapIterSeek(ProllyMutMapIter *it, ProllyMutMap *mm,
                          const u8 *pKey, int nKey, i64 intKey){
  int found = 0;
  it->pMap = mm;
  it->idx = bsearch_key(mm, pKey, nKey, intKey, &found);
}

void prollyMutMapIterLast(ProllyMutMapIter *it, ProllyMutMap *mm){
  it->pMap = mm;
  it->idx = mm->nEntries - 1;
}

void prollyMutMapClear(ProllyMutMap *mm){
  int i;
  for(i=0; i<mm->nEntries; i++){
    freeEntryData(&mm->aEntries[i]);
  }
  mm->nEntries = 0;
}

void prollyMutMapFree(ProllyMutMap *mm){
  prollyMutMapClear(mm);
  sqlite3_free(mm->aEntries);
  mm->aEntries = 0;
  mm->nAlloc = 0;
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
