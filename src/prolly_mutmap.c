/*
** Mutable map implementation: skip list for buffering pending edits
** before tree flush. Supports insert, delete, and ordered iteration.
*/
#ifdef DOLTLITE_PROLLY

#include "prolly_mutmap.h"
#include "prolly_node.h"
#include <string.h>

/*
** Generate a random level for a new skip list entry using geometric
** distribution. Uses a simple LCG PRNG and counts trailing zeros.
** Returns a value between 1 and PROLLY_SKIPLIST_MAXLEVEL.
*/
static int randomLevel(ProllyMutMap *mm){
  int level = 1;
  mm->prng = mm->prng * 6364136223846793005ULL + 1;
  sqlite3_uint64 r = (sqlite3_uint64)mm->prng;
  /* Count trailing zeros to get geometric distribution */
  while( (r & 1)==0 && level < PROLLY_SKIPLIST_MAXLEVEL ){
    level++;
    r >>= 1;
  }
  return level;
}

/*
** compareEntries: thin wrapper around prollyCompareKeys (prolly_node.c).
** Maps the isIntKey flag to PROLLY_NODE_INTKEY for the shared comparator.
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
** Allocate a new skip list entry with the given level. The entry struct
** is allocated with trailing space for 'level' forward pointers.
** Key and value data are copied into separately allocated buffers.
**
** Returns NULL on allocation failure.
*/
static ProllyMutMapEntry *allocEntry(
  u8 isIntKey,
  const u8 *pKey, int nKey, i64 intKey,
  const u8 *pVal, int nVal,
  u8 op,
  int level
){
  ProllyMutMapEntry *pEntry;
  int sz = sizeof(ProllyMutMapEntry) + level * sizeof(ProllyMutMapEntry*);
  int i;

  pEntry = (ProllyMutMapEntry*)sqlite3_malloc(sz);
  if( pEntry==0 ) return 0;
  memset(pEntry, 0, sz);

  pEntry->op = op;
  pEntry->isIntKey = isIntKey;
  pEntry->intKey = intKey;
  pEntry->nLevel = level;
  pEntry->pKey = 0;
  pEntry->nKey = 0;
  pEntry->pVal = 0;
  pEntry->nVal = 0;

  /* Copy blob key if not integer key */
  if( !isIntKey && pKey && nKey>0 ){
    pEntry->pKey = (u8*)sqlite3_malloc(nKey);
    if( pEntry->pKey==0 ){
      sqlite3_free(pEntry);
      return 0;
    }
    memcpy(pEntry->pKey, pKey, nKey);
    pEntry->nKey = nKey;
  }

  /* Copy value data */
  if( pVal && nVal>0 ){
    pEntry->pVal = (u8*)sqlite3_malloc(nVal);
    if( pEntry->pVal==0 ){
      sqlite3_free(pEntry->pKey);
      sqlite3_free(pEntry);
      return 0;
    }
    memcpy(pEntry->pVal, pVal, nVal);
    pEntry->nVal = nVal;
  }

  /* Initialize forward pointers to NULL */
  for(i=0; i<level; i++){
    pEntry->aForward[i] = 0;
  }

  return pEntry;
}

/*
** Free a skip list entry and its associated key/value buffers.
*/
static void freeEntry(ProllyMutMapEntry *pEntry){
  if( pEntry ){
    sqlite3_free(pEntry->pKey);
    sqlite3_free(pEntry->pVal);
    sqlite3_free(pEntry);
  }
}

/*
** Initialize a mutable map. Allocates the header sentinel node with
** PROLLY_SKIPLIST_MAXLEVEL forward pointers, all set to NULL.
**
** Returns SQLITE_OK on success, SQLITE_NOMEM on allocation failure.
*/
int prollyMutMapInit(ProllyMutMap *mm, u8 isIntKey){
  int sz = sizeof(ProllyMutMapEntry)
         + PROLLY_SKIPLIST_MAXLEVEL * sizeof(ProllyMutMapEntry*);
  int i;

  memset(mm, 0, sizeof(*mm));
  mm->isIntKey = isIntKey;
  mm->nEntries = 0;
  mm->maxLevel = 0;
  mm->prng = 42;

  mm->pHeader = (ProllyMutMapEntry*)sqlite3_malloc(sz);
  if( mm->pHeader==0 ) return SQLITE_NOMEM;
  memset(mm->pHeader, 0, sz);
  mm->pHeader->nLevel = PROLLY_SKIPLIST_MAXLEVEL;

  for(i=0; i<PROLLY_SKIPLIST_MAXLEVEL; i++){
    mm->pHeader->aForward[i] = 0;
  }

  return SQLITE_OK;
}

/*
** Insert or update a key-value pair in the mutable map.
** Copies key and value data. Sets op to PROLLY_EDIT_INSERT.
**
** If the key already exists, the existing entry's op and value are updated.
** If the key is new, a new entry is allocated and spliced into the skip list.
**
** Returns SQLITE_OK on success, SQLITE_NOMEM on allocation failure.
*/
int prollyMutMapInsert(
  ProllyMutMap *mm,
  const u8 *pKey, int nKey, i64 intKey,
  const u8 *pVal, int nVal
){
  ProllyMutMapEntry *update[PROLLY_SKIPLIST_MAXLEVEL];
  ProllyMutMapEntry *p;
  int i, c, level;

  /* Walk the skip list from the highest level down, building the update array */
  p = mm->pHeader;
  for(i = mm->maxLevel - 1; i >= 0; i--){
    while( p->aForward[i] != 0 ){
      c = compareEntries(mm->isIntKey,
            p->aForward[i]->pKey, p->aForward[i]->nKey, p->aForward[i]->intKey,
            pKey, nKey, intKey);
      if( c >= 0 ) break;
      p = p->aForward[i];
    }
    update[i] = p;
  }

  /* Check if key already exists */
  p = (mm->maxLevel > 0) ? update[0]->aForward[0] : mm->pHeader->aForward[0];
  if( p != 0 ){
    c = compareEntries(mm->isIntKey,
          p->pKey, p->nKey, p->intKey,
          pKey, nKey, intKey);
    if( c == 0 ){
      /* Key exists: update op and value */
      p->op = PROLLY_EDIT_INSERT;
      sqlite3_free(p->pVal);
      p->pVal = 0;
      p->nVal = 0;
      if( pVal && nVal > 0 ){
        p->pVal = (u8*)sqlite3_malloc(nVal);
        if( p->pVal == 0 ) return SQLITE_NOMEM;
        memcpy(p->pVal, pVal, nVal);
        p->nVal = nVal;
      }
      return SQLITE_OK;
    }
  }

  /* New key: generate random level and allocate entry */
  level = randomLevel(mm);

  ProllyMutMapEntry *pNew = allocEntry(
    mm->isIntKey, pKey, nKey, intKey, pVal, nVal,
    PROLLY_EDIT_INSERT, level
  );
  if( pNew == 0 ) return SQLITE_NOMEM;

  /* If new level exceeds current max, extend update array */
  if( level > mm->maxLevel ){
    for(i = mm->maxLevel; i < level; i++){
      update[i] = mm->pHeader;
    }
    mm->maxLevel = level;
  }

  /* Splice the new entry into the skip list */
  for(i = 0; i < level; i++){
    pNew->aForward[i] = update[i]->aForward[i];
    update[i]->aForward[i] = pNew;
  }

  mm->nEntries++;
  return SQLITE_OK;
}

/*
** Mark a key for deletion in the mutable map.
**
** If the key exists and its op is INSERT, the entry is removed entirely
** (insert then delete = no-op). If the key doesn't exist or has a
** different op, an entry is inserted/updated with op=DELETE and NULL value.
**
** Returns SQLITE_OK on success, SQLITE_NOMEM on allocation failure.
*/
int prollyMutMapDelete(
  ProllyMutMap *mm,
  const u8 *pKey, int nKey, i64 intKey
){
  ProllyMutMapEntry *update[PROLLY_SKIPLIST_MAXLEVEL];
  ProllyMutMapEntry *p;
  int i, c;

  /* Walk the skip list from the highest level down, building the update array */
  p = mm->pHeader;
  for(i = mm->maxLevel - 1; i >= 0; i--){
    while( p->aForward[i] != 0 ){
      c = compareEntries(mm->isIntKey,
            p->aForward[i]->pKey, p->aForward[i]->nKey, p->aForward[i]->intKey,
            pKey, nKey, intKey);
      if( c >= 0 ) break;
      p = p->aForward[i];
    }
    update[i] = p;
  }

  /* Check if key already exists */
  p = (mm->maxLevel > 0) ? update[0]->aForward[0] : mm->pHeader->aForward[0];
  if( p != 0 ){
    c = compareEntries(mm->isIntKey,
          p->pKey, p->nKey, p->intKey,
          pKey, nKey, intKey);
    if( c == 0 ){
      if( p->op == PROLLY_EDIT_INSERT ){
        /* Convert INSERT to DELETE. We cannot simply remove the entry
        ** because the original data may still exist in the unflushed tree.
        ** The DELETE marker ensures reads see this key as deleted rather
        ** than falling through to the stale tree data. */
        p->op = PROLLY_EDIT_DELETE;
        sqlite3_free(p->pVal);
        p->pVal = 0;
        p->nVal = 0;
        return SQLITE_OK;
      }else{
        /* Already a DELETE entry; nothing to do */
        return SQLITE_OK;
      }
    }
  }

  /* Key not found: insert a new DELETE entry */
  {
    int level = randomLevel(mm);
    ProllyMutMapEntry *pNew = allocEntry(
      mm->isIntKey, pKey, nKey, intKey, 0, 0,
      PROLLY_EDIT_DELETE, level
    );
    if( pNew == 0 ) return SQLITE_NOMEM;

    if( level > mm->maxLevel ){
      for(i = mm->maxLevel; i < level; i++){
        update[i] = mm->pHeader;
      }
      mm->maxLevel = level;
    }

    for(i = 0; i < level; i++){
      pNew->aForward[i] = update[i]->aForward[i];
      update[i]->aForward[i] = pNew;
    }

    mm->nEntries++;
  }
  return SQLITE_OK;
}

/*
** Look up a key in the skip list. Returns the entry if found, NULL otherwise.
*/
ProllyMutMapEntry *prollyMutMapFind(ProllyMutMap *mm,
                                     const u8 *pKey, int nKey, i64 intKey){
  ProllyMutMapEntry *p = mm->pHeader;
  int i;
  for(i = mm->maxLevel - 1; i >= 0; i--){
    while( p->aForward[i] ){
      int c = compareEntries(mm->isIntKey,
                             p->aForward[i]->pKey, p->aForward[i]->nKey,
                             p->aForward[i]->intKey,
                             pKey, nKey, intKey);
      if( c < 0 ){
        p = p->aForward[i];
      } else if( c == 0 ){
        return p->aForward[i];
      } else {
        break;
      }
    }
  }
  return 0;
}

/*
** Return the number of entries in the mutable map.
*/
int prollyMutMapCount(ProllyMutMap *mm){
  return mm->nEntries;
}

/*
** Return true if the mutable map has no entries.
*/
int prollyMutMapIsEmpty(ProllyMutMap *mm){
  return mm->nEntries == 0;
}

/*
** Initialize the iterator to point at the first entry in the map.
*/
void prollyMutMapIterFirst(ProllyMutMapIter *it, ProllyMutMap *mm){
  it->pMap = mm;
  it->pCurrent = mm->pHeader->aForward[0];
}

/*
** Advance the iterator to the next entry.
*/
void prollyMutMapIterNext(ProllyMutMapIter *it){
  if( it->pCurrent ){
    it->pCurrent = it->pCurrent->aForward[0];
  }
}

/*
** Return true if the iterator points to a valid entry.
*/
int prollyMutMapIterValid(ProllyMutMapIter *it){
  return it->pCurrent != 0;
}

/*
** Return the entry the iterator currently points to.
*/
ProllyMutMapEntry *prollyMutMapIterEntry(ProllyMutMapIter *it){
  return it->pCurrent;
}

/*
** Clear all entries from the mutable map. Walks level 0 and frees
** each entry. Resets header forward pointers to NULL.
*/
void prollyMutMapClear(ProllyMutMap *mm){
  ProllyMutMapEntry *p;
  ProllyMutMapEntry *pNext;
  int i;

  if( mm->pHeader==0 ) return;

  p = mm->pHeader->aForward[0];
  while( p ){
    pNext = p->aForward[0];
    freeEntry(p);
    p = pNext;
  }

  for(i = 0; i < PROLLY_SKIPLIST_MAXLEVEL; i++){
    mm->pHeader->aForward[i] = 0;
  }
  mm->nEntries = 0;
  mm->maxLevel = 0;
}

/*
** Free all resources associated with the mutable map.
** Clears all entries and frees the header sentinel.
*/
void prollyMutMapFree(ProllyMutMap *mm){
  if( mm->pHeader==0 ) return;
  prollyMutMapClear(mm);
  sqlite3_free(mm->pHeader);
  mm->pHeader = 0;
}

/*
** Merge all entries from pSrc into pDst. After this call, pSrc is empty
** and all its entries have been copied into pDst. This avoids the need
** to flush (rebuild the prolly tree) when two cursors on the same table
** both have pending edits.
*/
int prollyMutMapMerge(ProllyMutMap *pDst, ProllyMutMap *pSrc){
  ProllyMutMapIter iter;
  int rc = SQLITE_OK;

  prollyMutMapIterFirst(&iter, pSrc);
  while( prollyMutMapIterValid(&iter) ){
    ProllyMutMapEntry *e = prollyMutMapIterEntry(&iter);
    if( e->op == PROLLY_EDIT_INSERT ){
      rc = prollyMutMapInsert(pDst, e->pKey, e->nKey, e->intKey,
                               e->pVal, e->nVal);
    } else {
      rc = prollyMutMapDelete(pDst, e->pKey, e->nKey, e->intKey);
    }
    if( rc!=SQLITE_OK ) return rc;
    prollyMutMapIterNext(&iter);
  }
  prollyMutMapClear(pSrc);
  return SQLITE_OK;
}

#endif /* DOLTLITE_PROLLY */
