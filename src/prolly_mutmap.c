/*
** Mutable map implementation: sorted array for buffering pending edits
** before tree flush. Supports insert, delete, find, and ordered iteration.
**
** Sorted array gives O(log M) find, O(M) insert (shift), O(M) clone
** (memcpy + key/val copy). For typical M <= 256, this is faster than
** the previous skip list which had O(M log M) clone cost.
*/
#ifdef DOLTLITE_PROLLY

#include "prolly_mutmap.h"
#include "prolly_node.h"
#include <string.h>

#define MUTMAP_INIT_CAP 16

/*
** Compare two keys using the shared comparator from prolly_node.c.
*/
static int compareEntries(
  u8 isIntKey,
  const u8 *pKeyA, int nKeyA, i64 intKeyA,
  const u8 *pKeyB, int nKeyB, i64 intKeyB
){
  u8 flags = isIntKey ? PROLLY_NODE_INTKEY : PROLLY_NODE_BLOBKEY;
  return prollyCompareKeys(flags, pKeyA, nKeyA, intKeyA,
                           pKeyB, nKeyB, intKeyB);
}

/*
** Free the key and value buffers of an entry (but not the entry itself,
** since entries are stored inline in the array).
*/
static void freeEntryData(ProllyMutMapEntry *e){
  sqlite3_free(e->pKey);
  sqlite3_free(e->pVal);
  e->pKey = 0;
  e->pVal = 0;
  e->nKey = 0;
  e->nVal = 0;
}

/*
** Copy key and value data into an entry. The entry's op, isIntKey,
** and intKey must already be set.
*/
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

/*
** Binary search for a key. Returns the index where the key is found
** or should be inserted. Sets *pFound to 1 if an exact match exists.
*/
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

/*
** Ensure the array has capacity for at least one more entry.
*/
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

/*
** Initialize a mutable map.
*/
int prollyMutMapInit(ProllyMutMap *mm, u8 isIntKey){
  memset(mm, 0, sizeof(*mm));
  mm->isIntKey = isIntKey;
  return SQLITE_OK;
}

/*
** Insert or update a key-value pair.
*/
int prollyMutMapInsert(
  ProllyMutMap *mm,
  const u8 *pKey, int nKey, i64 intKey,
  const u8 *pVal, int nVal
){
  int found, idx, rc;

  idx = bsearch_key(mm, pKey, nKey, intKey, &found);

  if( found ){
    /* Update existing entry */
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

  /* New entry: grow array if needed, shift entries right, insert */
  rc = ensureCapacity(mm);
  if( rc!=SQLITE_OK ) return rc;

  /* Shift entries at idx..nEntries-1 right by 1 */
  if( idx < mm->nEntries ){
    memmove(&mm->aEntries[idx+1], &mm->aEntries[idx],
            (mm->nEntries - idx) * sizeof(ProllyMutMapEntry));
  }

  /* Initialize the new entry */
  {
    ProllyMutMapEntry *e = &mm->aEntries[idx];
    memset(e, 0, sizeof(*e));
    e->op = PROLLY_EDIT_INSERT;
    e->isIntKey = mm->isIntKey;
    e->intKey = intKey;
    rc = copyEntryData(e, pKey, nKey, pVal, nVal);
    if( rc!=SQLITE_OK ){
      /* Shift back on failure */
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

/*
** Mark a key for deletion.
*/
int prollyMutMapDelete(
  ProllyMutMap *mm,
  const u8 *pKey, int nKey, i64 intKey
){
  int found, idx, rc;

  idx = bsearch_key(mm, pKey, nKey, intKey, &found);

  if( found ){
    ProllyMutMapEntry *e = &mm->aEntries[idx];
    if( e->op == PROLLY_EDIT_INSERT ){
      /* Convert INSERT to DELETE */
      e->op = PROLLY_EDIT_DELETE;
      sqlite3_free(e->pVal);
      e->pVal = 0;
      e->nVal = 0;
      return SQLITE_OK;
    }
    /* Already a DELETE */
    return SQLITE_OK;
  }

  /* New DELETE entry */
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

/*
** Look up a key. Returns entry pointer or NULL.
*/
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

/*
** Clear all entries.
*/
void prollyMutMapClear(ProllyMutMap *mm){
  int i;
  for(i=0; i<mm->nEntries; i++){
    freeEntryData(&mm->aEntries[i]);
  }
  mm->nEntries = 0;
}

/*
** Deep-clone. Clone is O(M) — memcpy the array then copy key/val data.
*/
ProllyMutMap *prollyMutMapClone(ProllyMutMap *mm){
  ProllyMutMap *pNew;
  int i;

  if( !mm || mm->nEntries==0 ) return 0;

  pNew = sqlite3_malloc(sizeof(ProllyMutMap));
  if( !pNew ) return 0;

  pNew->isIntKey = mm->isIntKey;
  pNew->nEntries = mm->nEntries;
  pNew->nAlloc = mm->nEntries;
  pNew->aEntries = sqlite3_malloc(mm->nEntries * sizeof(ProllyMutMapEntry));
  if( !pNew->aEntries ){
    sqlite3_free(pNew);
    return 0;
  }

  /* Copy the entry structs (shallow copy — pKey/pVal pointers are stale) */
  memcpy(pNew->aEntries, mm->aEntries,
         mm->nEntries * sizeof(ProllyMutMapEntry));

  /* Deep-copy each entry's key and value data */
  for(i=0; i<pNew->nEntries; i++){
    ProllyMutMapEntry *src = &mm->aEntries[i];
    ProllyMutMapEntry *dst = &pNew->aEntries[i];
    dst->pKey = 0;
    dst->pVal = 0;
    if( src->pKey && src->nKey>0 ){
      dst->pKey = sqlite3_malloc(src->nKey);
      if( !dst->pKey ) goto clone_fail;
      memcpy(dst->pKey, src->pKey, src->nKey);
    }
    if( src->pVal && src->nVal>0 ){
      dst->pVal = sqlite3_malloc(src->nVal);
      if( !dst->pVal ) goto clone_fail;
      memcpy(dst->pVal, src->pVal, src->nVal);
    }
  }
  return pNew;

clone_fail:
  /* Free partially cloned entries */
  for(i=0; i<pNew->nEntries; i++){
    sqlite3_free(pNew->aEntries[i].pKey);
    sqlite3_free(pNew->aEntries[i].pVal);
  }
  sqlite3_free(pNew->aEntries);
  sqlite3_free(pNew);
  return 0;
}

/*
** Free all resources.
*/
void prollyMutMapFree(ProllyMutMap *mm){
  prollyMutMapClear(mm);
  sqlite3_free(mm->aEntries);
  mm->aEntries = 0;
  mm->nAlloc = 0;
}

/*
** Merge all entries from pSrc into pDst. pSrc is emptied.
*/
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

#endif /* DOLTLITE_PROLLY */
