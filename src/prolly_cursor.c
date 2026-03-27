/*
** Implementation of hierarchical cursor for prolly tree traversal.
** See prolly_cursor.h for interface documentation.
*/
#ifdef DOLTLITE_PROLLY

#include "prolly_cursor.h"
#include <string.h>
#include <assert.h>

/*
** Helper: load a node by hash from cache or chunk store.
** On success, *ppEntry is set to a cache entry with an incremented refcount.
*/
static int loadNode(ProllyCursor *cur, const ProllyHash *hash,
                    ProllyCacheEntry **ppEntry){
  ProllyCacheEntry *pEntry;
  int rc;

  *ppEntry = 0;

  /* Try cache first */
  pEntry = prollyCacheGet(cur->pCache, hash);
  if( pEntry ){
    *ppEntry = pEntry;
    return SQLITE_OK;
  }

  /* Cache miss: fetch from chunk store */
  u8 *pData = 0;
  int nData = 0;
  rc = chunkStoreGet(cur->pStore, hash, &pData, &nData);
  if( rc!=SQLITE_OK ){
    return rc;
  }

  /* Insert into cache. prollyCachePut makes its own copy of pData. */
  pEntry = prollyCachePut(cur->pCache, hash, pData, nData);
  sqlite3_free(pData);
  if( pEntry==0 ){
    return SQLITE_NOMEM;
  }

  *ppEntry = pEntry;
  return SQLITE_OK;
}

/*
** Helper: from the current level, descend to the leftmost leaf.
** Assumes aLevel[iLevel] is already loaded and idx is set.
*/
static int descendToLeftmostLeaf(ProllyCursor *cur){
  int rc;
  while( cur->aLevel[cur->iLevel].pEntry->node.level>0 ){
    ProllyCacheEntry *pParent = cur->aLevel[cur->iLevel].pEntry;
    int idx = cur->aLevel[cur->iLevel].idx;
    ProllyHash childHash;
    ProllyCacheEntry *pChild = 0;

    prollyNodeChildHash(&pParent->node, idx, &childHash);

    cur->iLevel++;
    if( cur->iLevel>=PROLLY_CURSOR_MAX_DEPTH ){
      return SQLITE_CORRUPT;
    }

    rc = loadNode(cur, &childHash, &pChild);
    if( rc!=SQLITE_OK ) return rc;

    cur->aLevel[cur->iLevel].pEntry = pChild;
    cur->aLevel[cur->iLevel].idx = 0;
  }
  cur->nLevel = cur->iLevel + 1;
  return SQLITE_OK;
}

/*
** Helper: from the current level, descend to the rightmost leaf.
** Assumes aLevel[iLevel] is already loaded and idx is set.
*/
static int descendToRightmostLeaf(ProllyCursor *cur){
  int rc;
  while( cur->aLevel[cur->iLevel].pEntry->node.level>0 ){
    ProllyCacheEntry *pParent = cur->aLevel[cur->iLevel].pEntry;
    int idx = cur->aLevel[cur->iLevel].idx;
    ProllyHash childHash;
    ProllyCacheEntry *pChild = 0;

    prollyNodeChildHash(&pParent->node, idx, &childHash);

    cur->iLevel++;
    if( cur->iLevel>=PROLLY_CURSOR_MAX_DEPTH ){
      return SQLITE_CORRUPT;
    }

    rc = loadNode(cur, &childHash, &pChild);
    if( rc!=SQLITE_OK ) return rc;

    cur->aLevel[cur->iLevel].pEntry = pChild;
    cur->aLevel[cur->iLevel].idx = pChild->node.nItems - 1;
  }
  cur->nLevel = cur->iLevel + 1;
  return SQLITE_OK;
}

/*
** Initialize a cursor. Does not load the root node.
*/
void prollyCursorInit(ProllyCursor *cur, ChunkStore *pStore,
                      ProllyCache *pCache, const ProllyHash *pRoot, u8 flags){
  memset(cur, 0, sizeof(*cur));
  cur->pStore = pStore;
  cur->pCache = pCache;
  memcpy(&cur->root, pRoot, sizeof(ProllyHash));
  cur->flags = flags;
  cur->eState = PROLLY_CURSOR_INVALID;
}

/*
** Move cursor to the first (leftmost) entry in the tree.
** Sets *pRes=1 if the tree is empty, 0 otherwise.
*/
int prollyCursorFirst(ProllyCursor *cur, int *pRes){
  int rc;

  prollyCursorReleaseAll(cur);

  if( prollyHashIsEmpty(&cur->root) ){
    cur->eState = PROLLY_CURSOR_EOF;
    *pRes = 1;
    return SQLITE_OK;
  }

  ProllyCacheEntry *pRoot = 0;
  rc = loadNode(cur, &cur->root, &pRoot);
  if( rc!=SQLITE_OK ) return rc;

  cur->iLevel = 0;
  cur->aLevel[0].pEntry = pRoot;
  cur->aLevel[0].idx = 0;

  rc = descendToLeftmostLeaf(cur);
  if( rc!=SQLITE_OK ) return rc;

  cur->eState = PROLLY_CURSOR_VALID;
  *pRes = 0;
  return SQLITE_OK;
}

/*
** Move cursor to the last (rightmost) entry in the tree.
** Sets *pRes=1 if the tree is empty, 0 otherwise.
*/
int prollyCursorLast(ProllyCursor *cur, int *pRes){
  int rc;

  prollyCursorReleaseAll(cur);

  if( prollyHashIsEmpty(&cur->root) ){
    cur->eState = PROLLY_CURSOR_EOF;
    *pRes = 1;
    return SQLITE_OK;
  }

  ProllyCacheEntry *pRoot = 0;
  rc = loadNode(cur, &cur->root, &pRoot);
  if( rc!=SQLITE_OK ) return rc;

  cur->iLevel = 0;
  cur->aLevel[0].pEntry = pRoot;
  cur->aLevel[0].idx = pRoot->node.nItems - 1;

  rc = descendToRightmostLeaf(cur);
  if( rc!=SQLITE_OK ) return rc;

  cur->eState = PROLLY_CURSOR_VALID;
  *pRes = 0;
  return SQLITE_OK;
}

/*
** Advance cursor to the next entry.
** If already at the last entry, sets eState to EOF.
*/
int prollyCursorNext(ProllyCursor *cur){
  int rc;

  assert( cur->eState==PROLLY_CURSOR_VALID );

  /* Try to advance within the current leaf */
  ProllyCacheEntry *pLeaf = cur->aLevel[cur->iLevel].pEntry;
  if( cur->aLevel[cur->iLevel].idx < pLeaf->node.nItems - 1 ){
    cur->aLevel[cur->iLevel].idx++;
    return SQLITE_OK;
  }

  /* Pop up the stack looking for a level we can advance */
  int level = cur->iLevel;
  while( level>0 ){
    /* Release the current level's node */
    prollyCacheRelease(cur->pCache, cur->aLevel[level].pEntry);
    cur->aLevel[level].pEntry = 0;
    level--;

    ProllyCacheEntry *pNode = cur->aLevel[level].pEntry;
    if( cur->aLevel[level].idx < pNode->node.nItems - 1 ){
      cur->aLevel[level].idx++;
      cur->iLevel = level;
      rc = descendToLeftmostLeaf(cur);
      if( rc!=SQLITE_OK ) return rc;
      cur->eState = PROLLY_CURSOR_VALID;
      return SQLITE_OK;
    }
  }

  /* We've exhausted the tree */
  cur->eState = PROLLY_CURSOR_EOF;
  return SQLITE_OK;
}

/*
** Move cursor to the previous entry.
** If already at the first entry, sets eState to EOF.
*/
int prollyCursorPrev(ProllyCursor *cur){
  int rc;

  assert( cur->eState==PROLLY_CURSOR_VALID );

  /* Try to go back within the current leaf */
  if( cur->aLevel[cur->iLevel].idx > 0 ){
    cur->aLevel[cur->iLevel].idx--;
    return SQLITE_OK;
  }

  /* Pop up the stack looking for a level we can go back */
  int level = cur->iLevel;
  while( level>0 ){
    prollyCacheRelease(cur->pCache, cur->aLevel[level].pEntry);
    cur->aLevel[level].pEntry = 0;
    level--;

    if( cur->aLevel[level].idx > 0 ){
      cur->aLevel[level].idx--;
      cur->iLevel = level;
      rc = descendToRightmostLeaf(cur);
      if( rc!=SQLITE_OK ) return rc;
      cur->eState = PROLLY_CURSOR_VALID;
      return SQLITE_OK;
    }
  }

  /* We've exhausted the tree */
  cur->eState = PROLLY_CURSOR_EOF;
  return SQLITE_OK;
}

/*
** Seek to an integer key (for INTKEY / table btrees).
** *pRes is set to: 0 if exact match, -1 if cursor is at a smaller key,
** +1 if cursor is at a larger key.
*/
int prollyCursorSeekInt(ProllyCursor *cur, i64 intKey, int *pRes){
  int rc;

  prollyCursorReleaseAll(cur);

  if( prollyHashIsEmpty(&cur->root) ){
    cur->eState = PROLLY_CURSOR_INVALID;
    *pRes = -1;
    return SQLITE_OK;
  }

  ProllyCacheEntry *pEntry = 0;
  rc = loadNode(cur, &cur->root, &pEntry);
  if( rc!=SQLITE_OK ) return rc;

  cur->iLevel = 0;
  cur->aLevel[0].pEntry = pEntry;

  /* Descend from root to leaf, binary searching at each level */
  while( pEntry->node.level>0 ){
    int searchRes;
    int idx = prollyNodeSearchInt(&pEntry->node, intKey, &searchRes);

    /*
    ** Internal node keys are the LAST (max) key in each child subtree.
    ** prollyNodeSearchInt returns the index where intKey would be inserted.
    **
    ** If searchRes==0: exact match at idx, descend into child idx.
    ** If searchRes>0: intKey > key[idx], need child idx+1 (next subtree).
    ** If searchRes<0: intKey < key[idx], descend into child idx
    **   (this child's subtree contains keys up to key[idx]).
    **
    ** But idx is clamped to [0, nItems-1] by the binary search, so if
    ** intKey > all keys, idx is at nItems-1 and searchRes>0. In that
    ** case we can't go to idx+1 (no more children) — but for a valid
    ** tree, this child should contain the key since it's the rightmost.
    */
    if( searchRes>0 && idx<pEntry->node.nItems-1 ){
      idx++;
    }

    cur->aLevel[cur->iLevel].idx = idx;

    ProllyHash childHash;
    prollyNodeChildHash(&pEntry->node, idx, &childHash);

    cur->iLevel++;
    if( cur->iLevel>=PROLLY_CURSOR_MAX_DEPTH ){
      return SQLITE_CORRUPT;
    }

    ProllyCacheEntry *pChild = 0;
    rc = loadNode(cur, &childHash, &pChild);
    if( rc!=SQLITE_OK ) return rc;

    cur->aLevel[cur->iLevel].pEntry = pChild;
    pEntry = pChild;
  }

  cur->nLevel = cur->iLevel + 1;

  /* At the leaf: binary search for the key */
  int leafRes;
  int leafIdx;
  if( pEntry->node.nItems==0 ){
    cur->eState = PROLLY_CURSOR_EOF;
    *pRes = -1;
    return SQLITE_OK;
  }
  leafIdx = prollyNodeSearchInt(&pEntry->node, intKey, &leafRes);
  cur->aLevel[cur->iLevel].idx = leafIdx;

  if( leafRes==0 ){
    /* Exact match */
    cur->eState = PROLLY_CURSOR_VALID;
    *pRes = 0;
  } else if( leafIdx>=pEntry->node.nItems ){
    /* Key is past the end of this leaf; try to advance to next entry */
    cur->aLevel[cur->iLevel].idx = pEntry->node.nItems - 1;
    cur->eState = PROLLY_CURSOR_VALID;
    rc = prollyCursorNext(cur);
    if( rc!=SQLITE_OK ) return rc;
    if( cur->eState==PROLLY_CURSOR_EOF ){
      /* Past last entry in tree, back up to last valid */
      rc = prollyCursorLast(cur, &(int){0});
      if( rc!=SQLITE_OK ) return rc;
      *pRes = -1;
    } else {
      *pRes = 1;
    }
  } else {
    cur->eState = PROLLY_CURSOR_VALID;
    if( leafRes<0 ){
      *pRes = 1;  /* cursor key > seek key */
    } else {
      *pRes = -1; /* cursor key < seek key */
    }
  }

  return SQLITE_OK;
}

/*
** Seek to a blob key (for BLOBKEY / index btrees).
** *pRes is set to: 0 if exact match, -1 if cursor is at a smaller key,
** +1 if cursor is at a larger key.
*/
int prollyCursorSeekBlob(ProllyCursor *cur,
                         const u8 *pKey, int nKey, int *pRes){
  int rc;

  prollyCursorReleaseAll(cur);

  if( prollyHashIsEmpty(&cur->root) ){
    cur->eState = PROLLY_CURSOR_INVALID;
    *pRes = -1;
    return SQLITE_OK;
  }

  ProllyCacheEntry *pEntry = 0;
  rc = loadNode(cur, &cur->root, &pEntry);
  if( rc!=SQLITE_OK ) return rc;

  cur->iLevel = 0;
  cur->aLevel[0].pEntry = pEntry;

  /* Descend from root to leaf */
  while( pEntry->node.level>0 ){
    int searchRes;
    int idx = prollyNodeSearchBlob(&pEntry->node, pKey, nKey, &searchRes);

    if( searchRes>0 && idx<pEntry->node.nItems-1 ){
      /* Key larger than key at idx, stay at idx */
    } else if( searchRes<0 && idx>0 ){
      idx--;
    }

    cur->aLevel[cur->iLevel].idx = idx;

    ProllyHash childHash;
    prollyNodeChildHash(&pEntry->node, idx, &childHash);

    cur->iLevel++;
    if( cur->iLevel>=PROLLY_CURSOR_MAX_DEPTH ){
      return SQLITE_CORRUPT;
    }

    ProllyCacheEntry *pChild = 0;
    rc = loadNode(cur, &childHash, &pChild);
    if( rc!=SQLITE_OK ) return rc;

    cur->aLevel[cur->iLevel].pEntry = pChild;
    pEntry = pChild;
  }

  cur->nLevel = cur->iLevel + 1;

  /* At the leaf: binary search */
  int leafRes;
  int leafIdx;
  if( pEntry->node.nItems==0 ){
    cur->eState = PROLLY_CURSOR_EOF;
    *pRes = -1;
    return SQLITE_OK;
  }
  leafIdx = prollyNodeSearchBlob(&pEntry->node, pKey, nKey, &leafRes);
  cur->aLevel[cur->iLevel].idx = leafIdx;

  if( leafRes==0 ){
    cur->eState = PROLLY_CURSOR_VALID;
    *pRes = 0;
  } else if( leafIdx>=pEntry->node.nItems ){
    cur->aLevel[cur->iLevel].idx = pEntry->node.nItems - 1;
    cur->eState = PROLLY_CURSOR_VALID;
    rc = prollyCursorNext(cur);
    if( rc!=SQLITE_OK ) return rc;
    if( cur->eState==PROLLY_CURSOR_EOF ){
      rc = prollyCursorLast(cur, &(int){0});
      if( rc!=SQLITE_OK ) return rc;
      *pRes = -1;
    } else {
      *pRes = 1;
    }
  } else {
    cur->eState = PROLLY_CURSOR_VALID;
    if( leafRes<0 ){
      *pRes = 1;
    } else {
      *pRes = -1;
    }
  }

  return SQLITE_OK;
}

/*
** Binary search within a node using sqlite3VdbeRecordCompare.
** Returns index of match or insertion point. *pRes like VdbeRecordCompare:
** 0=exact match (or eqSeen), cmp>0 if entry[idx] > search, cmp<0 if < search.
** *pEqSeen set if any eqSeen was triggered.
*/
static int nodeSearchRecord(
  const ProllyNode *pNode,
  UnpackedRecord *pIdxKey,
  int *pRes,
  int *pEqSeen
){
  int lo = 0;
  int hi = pNode->nItems - 1;
  int mid;
  const u8 *pMidKey;
  int nMidKey;

  *pEqSeen = 0;

  if( pNode->nItems==0 ){
    *pRes = -1;
    return 0;
  }

  /* Find the leftmost entry where cmp >= 0 (entry >= search key).
  ** When eqSeen is set, we treat it as cmp==0 and continue searching
  ** left to find the first such entry. */
  {
    int bestIdx = -1;
    int bestCmp = 0;
    int bestEqSeen = 0;

    while( lo<=hi ){
      mid = lo + (hi - lo) / 2;
      prollyNodeKey(pNode, mid, &pMidKey, &nMidKey);
      pIdxKey->eqSeen = 0;
      int c = sqlite3VdbeRecordCompare(nMidKey, pMidKey, pIdxKey);
      if( c==0 || pIdxKey->eqSeen ){
        /* Match found — record it but keep searching left for first match */
        bestIdx = mid;
        bestCmp = c;
        bestEqSeen = pIdxKey->eqSeen;
        hi = mid - 1;
      }else if( c>0 ){
        /* entry[mid] > search key — record as candidate, search left */
        if( bestIdx<0 ){
          bestIdx = mid;
          bestCmp = c;
          bestEqSeen = 0;
        }
        hi = mid - 1;
      }else{
        /* entry[mid] < search key → search in upper half */
        lo = mid + 1;
      }
    }

    if( bestIdx>=0 ){
      *pRes = bestCmp;
      *pEqSeen = bestEqSeen;
      return bestIdx;
    }
  }

  /* All entries < search key */
  *pRes = -1;
  return pNode->nItems - 1;
}

/*
** Seek cursor using VdbeRecordCompare (for IndexMoveto).
** Descends from root to leaf doing binary search at each level.
** *pRes: 0=exact match, >0=cursor at larger entry, <0=cursor past end.
** *pEqSeen: set if eqSeen was triggered (partial match).
*/
int prollyCursorSeekRecord(ProllyCursor *cur, UnpackedRecord *pIdxKey,
                           int *pRes, int *pEqSeen){
  int rc;

  prollyCursorReleaseAll(cur);
  *pEqSeen = 0;

  if( prollyHashIsEmpty(&cur->root) ){
    cur->eState = PROLLY_CURSOR_INVALID;
    *pRes = -1;
    return SQLITE_OK;
  }

  ProllyCacheEntry *pEntry = 0;
  rc = loadNode(cur, &cur->root, &pEntry);
  if( rc!=SQLITE_OK ) return rc;

  cur->iLevel = 0;
  cur->aLevel[0].pEntry = pEntry;

  /* Descend from root to leaf */
  while( pEntry->node.level>0 ){
    int searchRes;
    int eqSeen = 0;
    int idx = nodeSearchRecord(&pEntry->node, pIdxKey, &searchRes, &eqSeen);

    /* For internal nodes, position at the child that could contain the key.
    ** If searchRes > 0, entry[idx] > search → child at idx-1 or idx contains it.
    ** If searchRes < 0, search > all entries → child at last entry.
    ** If exact match, descend into child at idx. */
    if( searchRes>0 && idx>0 ){
      idx--;
    }

    cur->aLevel[cur->iLevel].idx = idx;

    ProllyHash childHash;
    prollyNodeChildHash(&pEntry->node, idx, &childHash);

    cur->iLevel++;
    if( cur->iLevel>=PROLLY_CURSOR_MAX_DEPTH ){
      return SQLITE_CORRUPT;
    }

    ProllyCacheEntry *pChild = 0;
    rc = loadNode(cur, &childHash, &pChild);
    if( rc!=SQLITE_OK ) return rc;

    cur->aLevel[cur->iLevel].pEntry = pChild;
    pEntry = pChild;
  }

  cur->nLevel = cur->iLevel + 1;

  /* At the leaf: binary search */
  int leafRes;
  int leafEqSeen = 0;
  int leafIdx = nodeSearchRecord(&pEntry->node, pIdxKey, &leafRes, &leafEqSeen);
  cur->aLevel[cur->iLevel].idx = leafIdx;

  if( leafRes==0 || leafEqSeen ){
    /* Exact match (or eqSeen partial match) */
    cur->eState = PROLLY_CURSOR_VALID;
    *pRes = leafRes;
    *pEqSeen = leafEqSeen;
  } else if( leafRes>0 ){
    /* entry[leafIdx] > search key → cursor at larger entry */
    cur->eState = PROLLY_CURSOR_VALID;
    *pRes = leafRes;
  } else {
    /* All entries < search key → position past end */
    /* Try to advance to next leaf via Next */
    cur->aLevel[cur->iLevel].idx = pEntry->node.nItems - 1;
    cur->eState = PROLLY_CURSOR_VALID;
    rc = prollyCursorNext(cur);
    if( rc!=SQLITE_OK ) return rc;
    if( cur->eState==PROLLY_CURSOR_EOF ){
      /* Truly past end of tree */
      rc = prollyCursorLast(cur, &(int){0});
      if( rc!=SQLITE_OK ) return rc;
      *pRes = -1;
    } else {
      *pRes = 1;  /* Cursor now at next larger entry */
    }
  }

  return SQLITE_OK;
}

/*
** Return 1 if cursor is valid (pointing to an entry), 0 otherwise.
*/
int prollyCursorIsValid(ProllyCursor *cur){
  return cur->eState==PROLLY_CURSOR_VALID;
}

/*
** Get the current blob key from the leaf node the cursor points to.
*/
void prollyCursorKey(ProllyCursor *cur, const u8 **ppKey, int *pnKey){
  assert( cur->eState==PROLLY_CURSOR_VALID );
  ProllyCacheEntry *pLeaf = cur->aLevel[cur->iLevel].pEntry;
  int idx = cur->aLevel[cur->iLevel].idx;
  prollyNodeKey(&pLeaf->node, idx, ppKey, pnKey);
}

/*
** Get the current integer key from the leaf node.
*/
i64 prollyCursorIntKey(ProllyCursor *cur){
  assert( cur->eState==PROLLY_CURSOR_VALID );
  ProllyCacheEntry *pLeaf = cur->aLevel[cur->iLevel].pEntry;
  int idx = cur->aLevel[cur->iLevel].idx;
  return prollyNodeIntKey(&pLeaf->node, idx);
}

/*
** Get the current value from the leaf node.
*/
void prollyCursorValue(ProllyCursor *cur, const u8 **ppVal, int *pnVal){
  assert( cur->eState==PROLLY_CURSOR_VALID );
  ProllyCacheEntry *pLeaf = cur->aLevel[cur->iLevel].pEntry;
  int idx = cur->aLevel[cur->iLevel].idx;
  prollyNodeValue(&pLeaf->node, idx, ppVal, pnVal);
}

/*
** Save the current cursor position so it can be restored later.
** If cursor is not valid, clears hasSavedPosition.
*/
int prollyCursorSave(ProllyCursor *cur){
  if( cur->eState!=PROLLY_CURSOR_VALID ){
    cur->hasSavedPosition = 0;
    return SQLITE_OK;
  }

  /* Verify we actually have a valid leaf node to read from */
  if( cur->iLevel < 0 || cur->iLevel >= PROLLY_CURSOR_MAX_DEPTH
   || !cur->aLevel[cur->iLevel].pEntry ){
    cur->hasSavedPosition = 0;
    cur->eState = PROLLY_CURSOR_INVALID;
    return SQLITE_OK;
  }

  /* Save the key */
  if( cur->flags & PROLLY_NODE_INTKEY ){
    cur->iSavedIntKey = prollyCursorIntKey(cur);
  } else {
    const u8 *pKey;
    int nKey;
    prollyCursorKey(cur, &pKey, &nKey);
    if( cur->pSavedKey ){
      sqlite3_free(cur->pSavedKey);
      cur->pSavedKey = 0;
    }
    cur->pSavedKey = (u8*)sqlite3_malloc(nKey);
    if( cur->pSavedKey==0 ){
      return SQLITE_NOMEM;
    }
    memcpy(cur->pSavedKey, pKey, nKey);
    cur->nSavedKey = nKey;
  }

  /* Release all node references */
  prollyCursorReleaseAll(cur);

  cur->hasSavedPosition = 1;
  cur->eState = PROLLY_CURSOR_INVALID;
  return SQLITE_OK;
}

/*
** Restore cursor to its previously saved position.
** *pDifferentRow is set to 0 if the exact row was found, 1 otherwise.
*/
int prollyCursorRestore(ProllyCursor *cur, int *pDifferentRow){
  int rc;

  if( !cur->hasSavedPosition ){
    *pDifferentRow = 1;
    cur->eState = PROLLY_CURSOR_INVALID;
    return SQLITE_OK;
  }

  int res;
  if( cur->flags & PROLLY_NODE_INTKEY ){
    rc = prollyCursorSeekInt(cur, cur->iSavedIntKey, &res);
  } else {
    rc = prollyCursorSeekBlob(cur, cur->pSavedKey, cur->nSavedKey, &res);
  }
  if( rc!=SQLITE_OK ) return rc;

  if( res==0 ){
    *pDifferentRow = 0;
  } else {
    *pDifferentRow = 1;
  }

  /* Free saved key */
  if( cur->pSavedKey ){
    sqlite3_free(cur->pSavedKey);
    cur->pSavedKey = 0;
    cur->nSavedKey = 0;
  }
  cur->hasSavedPosition = 0;

  return SQLITE_OK;
}

/*
** Release all node references held by the cursor's level stack.
*/
void prollyCursorReleaseAll(ProllyCursor *cur){
  int i;
  for(i=0; i<PROLLY_CURSOR_MAX_DEPTH; i++){
    if( cur->aLevel[i].pEntry ){
      prollyCacheRelease(cur->pCache, cur->aLevel[i].pEntry);
      cur->aLevel[i].pEntry = 0;
      cur->aLevel[i].idx = 0;
    }
  }
  cur->nLevel = 0;
  cur->iLevel = 0;
  /* Mark cursor invalid since all node references are released.
  ** Without this, prollyCursorIsValid returns true but the cursor
  ** has no valid node data, causing NULL dereferences. */
  cur->eState = PROLLY_CURSOR_INVALID;
}

/*
** Close the cursor and free all resources.
*/
void prollyCursorClose(ProllyCursor *cur){
  prollyCursorReleaseAll(cur);
  if( cur->pSavedKey ){
    sqlite3_free(cur->pSavedKey);
    cur->pSavedKey = 0;
    cur->nSavedKey = 0;
  }
  cur->hasSavedPosition = 0;
  cur->eState = PROLLY_CURSOR_INVALID;
}

/*
** Public wrapper for loadNode. Used by prolly_btree.c for custom seek.
*/
int prollyCursorLoadNode(ProllyCursor *cur, const ProllyHash *hash,
                         ProllyCacheEntry **ppEntry){
  return loadNode(cur, hash, ppEntry);
}

#endif /* DOLTLITE_PROLLY */
