#ifdef DOLTLITE_PROLLY

#include "prolly_btree_int.h"

int countTreeEntries(Btree *pBtree, Pgno iTable, i64 *pCount){
  int rc;
  struct TableEntry *pTE;
  BtShared *pBt = pBtree->pBt;

  pTE = findTable(pBtree, iTable);
  if( !pTE || prollyHashIsEmpty(&pTE->root) ){
    *pCount = 0;
    return SQLITE_OK;
  }

  {
    u64 cnt = 0;
    rc = prollySubtreeCount(&pBt->store, &pBt->cache, &pTE->root, &cnt);
    *pCount = (i64)cnt;
  }
  return rc;
}

static int countCursorLeftSiblings(
  ChunkStore *pStore,
  ProllyCache *pCache,
  ProllyCursor *pCur,
  int iLevel,
  i64 *pCount
){
  ProllyCacheEntry *pEntry = pCur->aLevel[iLevel].pEntry;
  int iChild = pCur->aLevel[iLevel].idx;
  int i;
  i64 n = 0;
  int rc = SQLITE_OK;

  if( !pEntry || pEntry->node.level==0 ){
    *pCount = 0;
    return SQLITE_OK;
  }

  for(i=0; i<iChild; i++){
    if( prollyNodeHasSubtreeCounts(&pEntry->node) ){
      n += (i64)prollyNodeChildSubtreeCount(&pEntry->node, i);
    }else{
      ProllyHash childHash;
      u64 nChild = 0;
      prollyNodeChildHash(&pEntry->node, i, &childHash);
      rc = prollySubtreeCount(pStore, pCache, &childHash, &nChild);
      if( rc!=SQLITE_OK ) return rc;
      n += (i64)nChild;
    }
  }

  *pCount = n;
  return SQLITE_OK;
}

static int prollyCursorCurrentRank(
  ChunkStore *pStore,
  ProllyCache *pCache,
  ProllyCursor *pCur,
  i64 *pRank
){
  ProllyCacheEntry *pLeaf;
  i64 n = 0;
  int i;
  int rc;

  if( pCur->eState!=PROLLY_CURSOR_VALID ){
    *pRank = 0;
    return SQLITE_OK;
  }

  for(i=0; i<pCur->iLevel; i++){
    i64 nLeft = 0;
    rc = countCursorLeftSiblings(pStore, pCache, pCur, i, &nLeft);
    if( rc!=SQLITE_OK ) return rc;
    n += nLeft;
  }

  pLeaf = pCur->aLevel[pCur->iLevel].pEntry;
  if( !pLeaf || pLeaf->node.level!=0 ) return SQLITE_CORRUPT;
  n += pCur->aLevel[pCur->iLevel].idx;
  *pRank = n;
  return SQLITE_OK;
}

static int countIntKeysUpTo(
  Btree *pBtree,
  const ProllyHash *pRoot,
  u8 flags,
  i64 intKey,
  i64 *pCount
){
  BtShared *pBt = pBtree->pBt;
  ProllyCursor cur;
  i64 nRank = 0;
  int res = 0;
  int rc;

  if( prollyHashIsEmpty(pRoot) ){
    *pCount = 0;
    return SQLITE_OK;
  }

  prollyCursorInit(&cur, &pBt->store, &pBt->cache, pRoot, flags);
  rc = prollyCursorSeekInt(&cur, intKey, &res);
  if( rc!=SQLITE_OK ){ prollyCursorClose(&cur); return rc; }
  if( cur.eState!=PROLLY_CURSOR_VALID ){
    prollyCursorClose(&cur);
    *pCount = 0;
    return SQLITE_OK;
  }
  rc = prollyCursorCurrentRank(&pBt->store, &pBt->cache, &cur, &nRank);
  if( rc==SQLITE_OK ){
    *pCount = nRank + (res<=0 ? 1 : 0);
  }
  prollyCursorClose(&cur);
  return rc;
}

static int countTreeIntRange(
  Btree *pBtree,
  Pgno iTable,
  i64 iLower,
  i64 iUpper,
  i64 *pCount
){
  struct TableEntry *pTE;
  i64 nBeforeLower = 0;
  i64 nThroughUpper = 0;
  int rc;

  *pCount = 0;
  if( iLower>iUpper ) return SQLITE_OK;
  pTE = findTable(pBtree, iTable);
  if( !pTE || prollyHashIsEmpty(&pTE->root) ) return SQLITE_OK;

  if( iLower==SMALLEST_INT64 ){
    nBeforeLower = 0;
  }else{
    rc = countIntKeysUpTo(pBtree, &pTE->root, pTE->flags,
                          iLower-1, &nBeforeLower);
    if( rc!=SQLITE_OK ) return rc;
  }
  rc = countIntKeysUpTo(pBtree, &pTE->root, pTE->flags,
                        iUpper, &nThroughUpper);
  if( rc!=SQLITE_OK ) return rc;
  *pCount = nThroughUpper - nBeforeLower;
  if( *pCount<0 ) *pCount = 0;
  return SQLITE_OK;
}

int sortKeyFromUnpackedForCount(
  BtCursor *pCur,
  UnpackedRecord *pRec,
  u8 **ppBuf,
  int *pnAlloc,
  int *pnOut
){
  int rc;
  int nKeyField = 0;
  if( pCur->pKeyInfo && pRec->nField < pCur->pKeyInfo->nAllField ){
    nKeyField = (int)pRec->nField;
  }
  if( unpackedRecordCanUseIntSortKey(
        pCur, pRec, nKeyField>0 ? nKeyField : (int)pRec->nField) ){
    return sortKeyFromUnpackedIntRecordBuffer(
        pRec, nKeyField>0 ? nKeyField : (int)pRec->nField,
        ppBuf, pnAlloc, pnOut);
  }
  rc = sortKeyFromMemPrefixCollBuffer(
      pRec->aMem, (int)pRec->nField, nKeyField, pCur->pKeyInfo,
      ppBuf, pnAlloc, pnOut);
  if( rc==SQLITE_NOTFOUND ){
    u8 *pSerKey = 0;
    int nSerAlloc = 0;
    int nSerKey = 0;
    rc = serializeUnpackedRecordBuffer(pRec, &pSerKey, &nSerAlloc, &nSerKey);
    if( rc==SQLITE_OK ){
      rc = sortKeyFromRecordPrefixCollBuffer(
          pSerKey, nSerKey, nKeyField, pCur->pKeyInfo, ppBuf, pnAlloc, pnOut);
    }
    sqlite3_free(pSerKey);
  }
  return rc;
}

/* Smallest key sorting above every key that has pKey as a prefix. Trailing
** 0xff bytes must be dropped, not carried: keeping them yields a bound above
** the true successor, which counts keys past the end of the range. */
static int blobPrefixSuccessor(const u8 *pKey, int nKey, u8 **ppOut, int *pnOut){
  int i;
  u8 *pOut;
  for(i=nKey-1; i>=0 && pKey[i]==0xff; i--){}
  if( i<0 ){
    *ppOut = 0;
    *pnOut = 0;
    return SQLITE_OK;
  }
  pOut = sqlite3_malloc(i+1);
  if( !pOut ) return SQLITE_NOMEM;
  memcpy(pOut, pKey, i+1);
  pOut[i]++;
  *ppOut = pOut;
  *pnOut = i+1;
  return SQLITE_OK;
}

static int countBlobKeysBefore(
  Btree *pBtree,
  const ProllyHash *pRoot,
  u8 flags,
  const u8 *pKey,
  int nKey,
  i64 *pCount
){
  BtShared *pBt = pBtree->pBt;
  ProllyCursor cur;
  i64 nRank = 0;
  int res = 0;
  int rc;

  if( prollyHashIsEmpty(pRoot) ){
    *pCount = 0;
    return SQLITE_OK;
  }

  prollyCursorInit(&cur, &pBt->store, &pBt->cache, pRoot, flags);
  rc = prollyCursorSeekBlob(&cur, pKey, nKey, &res);
  if( rc!=SQLITE_OK ){ prollyCursorClose(&cur); return rc; }
  if( cur.eState!=PROLLY_CURSOR_VALID ){
    prollyCursorClose(&cur);
    *pCount = 0;
    return SQLITE_OK;
  }
  rc = prollyCursorCurrentRank(&pBt->store, &pBt->cache, &cur, &nRank);
  if( rc==SQLITE_OK ){
    *pCount = nRank + (res<0 ? 1 : 0);
  }
  prollyCursorClose(&cur);
  return rc;
}

static int countTreeBlobPrefixRange(
  BtCursor *pCur,
  UnpackedRecord *pLower,
  UnpackedRecord *pUpper,
  i64 *pCount
){
  struct TableEntry *pTE;
  u8 *pLowerKey = 0;
  u8 *pUpperKey = 0;
  u8 *pUpperNext = 0;
  int nLowerKey = 0;
  int nUpperKey = 0;
  int nUpperNext = 0;
  int nLowerAlloc = 0;
  int nUpperAlloc = 0;
  i64 nLower = 0;
  i64 nUpper = 0;
  int rc;

  *pCount = 0;
  pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
  if( !pTE || prollyHashIsEmpty(&pTE->root) ) return SQLITE_OK;

  rc = sortKeyFromUnpackedForCount(
      pCur, pLower, &pLowerKey, &nLowerAlloc, &nLowerKey);
  if( rc!=SQLITE_OK ) goto done;
  rc = sortKeyFromUnpackedForCount(
      pCur, pUpper, &pUpperKey, &nUpperAlloc, &nUpperKey);
  if( rc!=SQLITE_OK ) goto done;
  rc = blobPrefixSuccessor(pUpperKey, nUpperKey, &pUpperNext, &nUpperNext);
  if( rc!=SQLITE_OK ) goto done;

  rc = countBlobKeysBefore(pCur->pBtree, &pTE->root, pTE->flags,
                           pLowerKey, nLowerKey, &nLower);
  if( rc!=SQLITE_OK ) goto done;
  if( pUpperNext ){
    rc = countBlobKeysBefore(pCur->pBtree, &pTE->root, pTE->flags,
                             pUpperNext, nUpperNext, &nUpper);
  }else{
    rc = countTreeEntries(pCur->pBtree, pCur->pgnoRoot, &nUpper);
  }
  if( rc==SQLITE_OK ){
    *pCount = nUpper - nLower;
    if( *pCount<0 ) *pCount = 0;
  }

done:
  sqlite3_free(pLowerKey);
  sqlite3_free(pUpperKey);
  sqlite3_free(pUpperNext);
  return rc;
}
/* Drain any pending mutations for this cursor's table into the tree root so a
** count walks committed data. Mirrors the snapshot/apply/clear sequence used
** on the write path. */
static int flushPendingForCount(BtCursor *pCur){
  struct TableEntry *pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
  if( pTE && pTE->pPending ){
    ProllyMutMap *pMap = (ProllyMutMap*)pTE->pPending;
    if( !prollyMutMapIsEmpty(pMap) ){
      ProllyMutMap *pFlushMap = pMap;
      int captured = 0;
      int rc = snapshotPendingForFlush(pCur->pBtree, pCur->pgnoRoot,
                                       (ProllyMutMap**)&pTE->pPending,
                                       &pFlushMap, &captured);
      if( rc!=SQLITE_OK ) return rc;
      if( captured ){
        refreshCursorMutMapAliases(pCur->pBtree, pCur->pBt, pCur->pgnoRoot,
                                   (ProllyMutMap*)pTE->pPending);
      }
      rc = applyMutMapToTableRoot(pCur->pBt, pTE, pFlushMap);
      if( rc!=SQLITE_OK ) return rc;
      if( pTE->pPending==pMap ){
        prollyMutMapClear(pMap);
      }
    }
  }
  flushIfNeeded(pCur);
  return SQLITE_OK;
}

int prollyBtCursorCount(sqlite3 *db, BtCursor *pCur, i64 *pnEntry){
  int rc;
  (void)db;
  rc = flushPendingForCount(pCur);
  if( rc!=SQLITE_OK ) return rc;
  return countTreeEntries(pCur->pBtree, pCur->pgnoRoot, pnEntry);
}

int prollyBtCursorCountRange(
  sqlite3 *db,
  BtCursor *pCur,
  i64 iLower,
  i64 iUpper,
  i64 *pnEntry
){
  int rc;
  (void)db;
  rc = flushPendingForCount(pCur);
  if( rc!=SQLITE_OK ) return rc;
  return countTreeIntRange(pCur->pBtree, pCur->pgnoRoot,
                           iLower, iUpper, pnEntry);
}

int prollyBtCursorCountIndexRange(
  sqlite3 *db,
  BtCursor *pCur,
  UnpackedRecord *pLower,
  UnpackedRecord *pUpper,
  i64 *pnEntry
){
  int rc;
  (void)db;
  if( pCur->curIntKey ) return SQLITE_NOTFOUND;
  rc = flushPendingForCount(pCur);
  if( rc!=SQLITE_OK ) return rc;
  return countTreeBlobPrefixRange(pCur, pLower, pUpper, pnEntry);
}

int sqlite3BtreeCount(sqlite3 *db, BtCursor *pCur, i64 *pnEntry){
  if( !pCur ) return SQLITE_OK;
  return pCur->pCurOps->xCount(db, pCur, pnEntry);
}

int sqlite3BtreeCountRange(
  sqlite3 *db,
  BtCursor *pCur,
  i64 iLower,
  i64 iUpper,
  i64 *pnEntry
){
  if( !pCur ) return SQLITE_OK;
  return pCur->pCurOps->xCountRange(db, pCur, iLower, iUpper, pnEntry);
}

int sqlite3BtreeCountIndexRange(
  sqlite3 *db,
  BtCursor *pCur,
  UnpackedRecord *pLower,
  UnpackedRecord *pUpper,
  i64 *pnEntry
){
  if( !pCur ) return SQLITE_OK;
  return pCur->pCurOps->xCountIndexRange(db, pCur, pLower, pUpper, pnEntry);
}

i64 prollyBtCursorRowCountEst(BtCursor *pCur){
  struct TableEntry *pTE;
  ProllyCursor *pProllyCur = &pCur->pCur;
  i64 nEst = 0;
  i64 nPending = 0;
  int i;

  pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
  if( !pTE || prollyHashIsEmpty(&pTE->root) ){
    if( pTE && pTE->pPending ){
      nPending = (i64)prollyMutMapCount((ProllyMutMap*)pTE->pPending);
    }
    return nPending;
  }

  if( pProllyCur->eState==PROLLY_CURSOR_VALID
   && pProllyCur->aLevel[pProllyCur->iLevel].pEntry
  ){
    nEst = pProllyCur->aLevel[pProllyCur->iLevel].pEntry->node.nItems;
    for(i=0; i<pProllyCur->iLevel; i++){
      ProllyCacheEntry *pLevel = pProllyCur->aLevel[i].pEntry;
      int n;
      if( !pLevel ) break;
      n = pLevel->node.nItems;
      if( n<=0 ) n = 1;
      if( nEst > 0x7fffffffffffLL / n ){
        nEst = 0x7fffffffffffLL;
        break;
      }
      nEst *= n;
    }
  } else {
    ProllyCacheEntry *pRoot = prollyCacheGet(&pCur->pBt->cache, &pTE->root);
    if( pRoot ){
      int level = pRoot->node.level;
      int rootItems = pRoot->node.nItems;
      prollyCacheRelease(&pCur->pBt->cache, pRoot);
      if( level<=0 ){
        nEst = rootItems;
      } else {
        nEst = rootItems;
        for(i=0; i<level; i++){
          if( nEst > 0x7fffffffffffLL / 100 ){
            nEst = 0x7fffffffffffLL;
            break;
          }
          nEst *= 100;
        }
      }
    } else {
      nEst = 10000;
    }
  }

  if( pTE->pPending ){
    nPending = (i64)prollyMutMapCount((ProllyMutMap*)pTE->pPending);
    nEst += nPending;
  }

  if( nEst<1 ) nEst = 1;
  return nEst;
}

i64 sqlite3BtreeRowCountEst(BtCursor *pCur){
  return pCur->pCurOps->xRowCountEst(pCur);
}

#endif
