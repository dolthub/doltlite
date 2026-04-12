
#ifdef DOLTLITE_PROLLY

#include "prolly_cursor.h"
#include <string.h>
#include <assert.h>

static int loadNode(ProllyCursor *cur, const ProllyHash *hash,
                    ProllyCacheEntry **ppEntry){
  ProllyCacheEntry *pEntry;
  int rc;
  u8 *pData = 0;
  int nData = 0;

  *ppEntry = 0;


  pEntry = prollyCacheGet(cur->pCache, hash);
  if( pEntry ){
    *ppEntry = pEntry;
    return SQLITE_OK;
  }

  rc = chunkStoreGet(cur->pStore, hash, &pData, &nData);
  if( rc!=SQLITE_OK ){
    return rc;
  }

  
  pEntry = prollyCachePut(cur->pCache, hash, pData, nData, &rc);
  sqlite3_free(pData);
  if( pEntry==0 ){
    return rc;
  }

  *ppEntry = pEntry;
  return SQLITE_OK;
}

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

void prollyCursorInit(ProllyCursor *cur, ChunkStore *pStore,
                      ProllyCache *pCache, const ProllyHash *pRoot, u8 flags){
  memset(cur, 0, sizeof(*cur));
  cur->pStore = pStore;
  cur->pCache = pCache;
  memcpy(&cur->root, pRoot, sizeof(ProllyHash));
  cur->flags = flags;
  cur->eState = PROLLY_CURSOR_INVALID;
}

int prollyCursorFirst(ProllyCursor *cur, int *pRes){
  int rc;
  ProllyCacheEntry *pRoot = 0;
  ProllyCacheEntry *pLeaf;

  prollyCursorReleaseAll(cur);

  if( prollyHashIsEmpty(&cur->root) ){
    cur->eState = PROLLY_CURSOR_EOF;
    *pRes = 1;
    return SQLITE_OK;
  }

  rc = loadNode(cur, &cur->root, &pRoot);
  if( rc!=SQLITE_OK ) return rc;

  cur->iLevel = 0;
  cur->aLevel[0].pEntry = pRoot;
  cur->aLevel[0].idx = 0;

  rc = descendToLeftmostLeaf(cur);
  if( rc!=SQLITE_OK ) return rc;

  pLeaf = cur->aLevel[cur->iLevel].pEntry;
  if( pLeaf->node.nItems==0 ){
    cur->eState = PROLLY_CURSOR_EOF;
    *pRes = 1;
    return SQLITE_OK;
  }

  cur->eState = PROLLY_CURSOR_VALID;
  *pRes = 0;
  return SQLITE_OK;
}

int prollyCursorLast(ProllyCursor *cur, int *pRes){
  int rc;
  ProllyCacheEntry *pRoot = 0;
  ProllyCacheEntry *pLeaf;

  prollyCursorReleaseAll(cur);

  if( prollyHashIsEmpty(&cur->root) ){
    cur->eState = PROLLY_CURSOR_EOF;
    *pRes = 1;
    return SQLITE_OK;
  }

  rc = loadNode(cur, &cur->root, &pRoot);
  if( rc!=SQLITE_OK ) return rc;
  if( pRoot->node.nItems==0 ){
    cur->iLevel = 0;
    cur->aLevel[0].pEntry = pRoot;
    cur->aLevel[0].idx = 0;
    cur->nLevel = 1;
    cur->eState = PROLLY_CURSOR_EOF;
    *pRes = 1;
    return SQLITE_OK;
  }

  cur->iLevel = 0;
  cur->aLevel[0].pEntry = pRoot;
  cur->aLevel[0].idx = pRoot->node.nItems - 1;

  rc = descendToRightmostLeaf(cur);
  if( rc!=SQLITE_OK ) return rc;

  pLeaf = cur->aLevel[cur->iLevel].pEntry;
  if( pLeaf->node.nItems==0 ){
    cur->eState = PROLLY_CURSOR_EOF;
    *pRes = 1;
    return SQLITE_OK;
  }

  cur->eState = PROLLY_CURSOR_VALID;
  *pRes = 0;
  return SQLITE_OK;
}

int prollyCursorNext(ProllyCursor *cur){
  int rc;
  ProllyCacheEntry *pLeaf;
  int level;
  ProllyCacheEntry *pNode;

  assert( cur->eState==PROLLY_CURSOR_VALID );


  pLeaf = cur->aLevel[cur->iLevel].pEntry;
  if( cur->aLevel[cur->iLevel].idx < pLeaf->node.nItems - 1 ){
    cur->aLevel[cur->iLevel].idx++;
    return SQLITE_OK;
  }


  level = cur->iLevel;
  while( level>0 ){

    prollyCacheRelease(cur->pCache, cur->aLevel[level].pEntry);
    cur->aLevel[level].pEntry = 0;
    level--;

    pNode = cur->aLevel[level].pEntry;
    if( cur->aLevel[level].idx < pNode->node.nItems - 1 ){
      cur->aLevel[level].idx++;
      cur->iLevel = level;
      rc = descendToLeftmostLeaf(cur);
      if( rc!=SQLITE_OK ) return rc;
      cur->eState = PROLLY_CURSOR_VALID;
      return SQLITE_OK;
    }
  }

  
  cur->eState = PROLLY_CURSOR_EOF;
  return SQLITE_OK;
}

int prollyCursorPrev(ProllyCursor *cur){
  int rc;
  int level;

  assert( cur->eState==PROLLY_CURSOR_VALID );


  if( cur->aLevel[cur->iLevel].idx > 0 ){
    cur->aLevel[cur->iLevel].idx--;
    return SQLITE_OK;
  }


  level = cur->iLevel;
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

  
  cur->eState = PROLLY_CURSOR_EOF;
  return SQLITE_OK;
}

int prollyCursorSeekInt(ProllyCursor *cur, i64 intKey, int *pRes){
  int rc;
  ProllyCacheEntry *pEntry = 0;
  ProllyHash childHash;
  ProllyCacheEntry *pChild = 0;
  int leafRes;
  int leafIdx;

  prollyCursorReleaseAll(cur);

  if( prollyHashIsEmpty(&cur->root) ){
    cur->eState = PROLLY_CURSOR_INVALID;
    *pRes = -1;
    return SQLITE_OK;
  }

  rc = loadNode(cur, &cur->root, &pEntry);
  if( rc!=SQLITE_OK ) return rc;

  cur->iLevel = 0;
  cur->aLevel[0].pEntry = pEntry;

  
  while( pEntry->node.level>0 ){
    int searchRes;
    int idx = prollyNodeSearchInt(&pEntry->node, intKey, &searchRes);

    
    if( searchRes>0 && idx<pEntry->node.nItems-1 ){
      idx++;
    }

    cur->aLevel[cur->iLevel].idx = idx;

    prollyNodeChildHash(&pEntry->node, idx, &childHash);

    cur->iLevel++;
    if( cur->iLevel>=PROLLY_CURSOR_MAX_DEPTH ){
      return SQLITE_CORRUPT;
    }

    pChild = 0;
    rc = loadNode(cur, &childHash, &pChild);
    if( rc!=SQLITE_OK ) return rc;

    cur->aLevel[cur->iLevel].pEntry = pChild;
    pEntry = pChild;
  }

  cur->nLevel = cur->iLevel + 1;

  if( pEntry->node.nItems==0 ){
    cur->eState = PROLLY_CURSOR_EOF;
    *pRes = -1;
    return SQLITE_OK;
  }
  leafIdx = prollyNodeSearchInt(&pEntry->node, intKey, &leafRes);
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

int prollyCursorSeekBlob(ProllyCursor *cur,
                         const u8 *pKey, int nKey, int *pRes){
  int rc;
  ProllyCacheEntry *pEntry = 0;
  ProllyHash childHash;
  ProllyCacheEntry *pChild = 0;
  int leafRes;
  int leafIdx;

  prollyCursorReleaseAll(cur);

  if( prollyHashIsEmpty(&cur->root) ){
    cur->eState = PROLLY_CURSOR_INVALID;
    *pRes = -1;
    return SQLITE_OK;
  }

  rc = loadNode(cur, &cur->root, &pEntry);
  if( rc!=SQLITE_OK ) return rc;

  cur->iLevel = 0;
  cur->aLevel[0].pEntry = pEntry;

  
  while( pEntry->node.level>0 ){
    int searchRes;
    int idx = prollyNodeSearchBlob(&pEntry->node, pKey, nKey, &searchRes);

    if( searchRes>0 && idx<pEntry->node.nItems-1 ){
      idx++;
    }

    cur->aLevel[cur->iLevel].idx = idx;

    prollyNodeChildHash(&pEntry->node, idx, &childHash);

    cur->iLevel++;
    if( cur->iLevel>=PROLLY_CURSOR_MAX_DEPTH ){
      return SQLITE_CORRUPT;
    }

    pChild = 0;
    rc = loadNode(cur, &childHash, &pChild);
    if( rc!=SQLITE_OK ) return rc;

    cur->aLevel[cur->iLevel].pEntry = pChild;
    pEntry = pChild;
  }

  cur->nLevel = cur->iLevel + 1;

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

int prollyCursorIsValid(ProllyCursor *cur){
  return cur->eState==PROLLY_CURSOR_VALID;
}

void prollyCursorKey(ProllyCursor *cur, const u8 **ppKey, int *pnKey){
  ProllyCacheEntry *pLeaf;
  int idx;
  assert( cur->eState==PROLLY_CURSOR_VALID );
  pLeaf = cur->aLevel[cur->iLevel].pEntry;
  idx = cur->aLevel[cur->iLevel].idx;
  prollyNodeKey(&pLeaf->node, idx, ppKey, pnKey);
}

i64 prollyCursorIntKey(ProllyCursor *cur){
  ProllyCacheEntry *pLeaf;
  int idx;
  assert( cur->eState==PROLLY_CURSOR_VALID );
  pLeaf = cur->aLevel[cur->iLevel].pEntry;
  idx = cur->aLevel[cur->iLevel].idx;
  return prollyNodeIntKey(&pLeaf->node, idx);
}

void prollyCursorValue(ProllyCursor *cur, const u8 **ppVal, int *pnVal){
  ProllyCacheEntry *pLeaf;
  int idx;
  assert( cur->eState==PROLLY_CURSOR_VALID );
  pLeaf = cur->aLevel[cur->iLevel].pEntry;
  idx = cur->aLevel[cur->iLevel].idx;
  prollyNodeValue(&pLeaf->node, idx, ppVal, pnVal);
}

int prollyCursorSave(ProllyCursor *cur){
  if( cur->eState!=PROLLY_CURSOR_VALID ){
    cur->hasSavedPosition = 0;
    return SQLITE_OK;
  }

  
  if( cur->iLevel < 0 || cur->iLevel >= PROLLY_CURSOR_MAX_DEPTH
   || !cur->aLevel[cur->iLevel].pEntry ){
    cur->hasSavedPosition = 0;
    cur->eState = PROLLY_CURSOR_INVALID;
    return SQLITE_OK;
  }

  
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

  
  prollyCursorReleaseAll(cur);

  cur->hasSavedPosition = 1;
  cur->eState = PROLLY_CURSOR_INVALID;
  return SQLITE_OK;
}

int prollyCursorRestore(ProllyCursor *cur, int *pDifferentRow){
  int rc;
  int res;

  if( !cur->hasSavedPosition ){
    *pDifferentRow = 1;
    cur->eState = PROLLY_CURSOR_INVALID;
    return SQLITE_OK;
  }

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

  
  if( cur->pSavedKey ){
    sqlite3_free(cur->pSavedKey);
    cur->pSavedKey = 0;
    cur->nSavedKey = 0;
  }
  cur->hasSavedPosition = 0;

  return SQLITE_OK;
}

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
  
  cur->eState = PROLLY_CURSOR_INVALID;
}

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

#endif 
