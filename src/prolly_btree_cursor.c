#ifdef DOLTLITE_PROLLY

#include "prolly_btree_int.h"

void cacheCurrentTreePayloadIfIntKey(BtCursor *pCur){
  if( pCur->curIntKey ){
    const u8 *pVal; int nVal; int nAvail;
    CLEAR_CACHED_PAYLOAD(pCur);
    prollyBtreeCursorCurrentTreeValueSpan(pCur, &pVal, &nVal, &nAvail);
    if( nVal > 0 && nAvail==nVal ){
      pCur->pCachedPayload = (u8*)pVal;
      pCur->nCachedPayload = nVal;
      pCur->cachedPayloadOwned = 0;
    }
  }
}

void cacheCurrentTreeStoredPayloadNonIntKey(BtCursor *pCur){
  const u8 *pVal; int nVal;
  CLEAR_CACHED_PAYLOAD(pCur);
  cursorCurrentTreeValue(pCur, &pVal, &nVal);
  if( nVal > 0 ){
    pCur->pCachedPayload = (u8*)pVal;
    pCur->nCachedPayload = nVal;
    pCur->cachedPayloadOwned = 0;
  }
}


static int isDegenerateSchemaPayload(const u8 *pVal, int nVal){
  return nVal==6
      && pVal
      && pVal[0]==6
      && pVal[1]==0
      && pVal[2]==0
      && pVal[3]==0
      && pVal[4]==0
      && pVal[5]==0;
}

static int cursorOnDegenerateSchemaRow(BtCursor *pCur, int *pIsDegenerate){
  const u8 *pVal;
  int nVal;
  int rc;
  *pIsDegenerate = 0;
  if( pCur->pgnoRoot!=1 || !pCur->curIntKey ) return SQLITE_OK;
  if( pCur->eState!=CURSOR_VALID ) return SQLITE_OK;
  rc = getCursorPayload(pCur, &pVal, &nVal);
  if( rc!=SQLITE_OK ) return rc;
  if( !pVal || nVal<=0 ){
    *pIsDegenerate = 1;
    return SQLITE_OK;
  }
  if( pCur->pBtree->bFilterSchemaPlaceholders
   && isDegenerateSchemaPayload(pVal, nVal) ){
    *pIsDegenerate = 1;
  }
  return SQLITE_OK;
}

static int skipDegenerateSchemaRows(BtCursor *pCur, int dir, int *pRes){
  int rc = SQLITE_OK;
  int isDegenerate = 0;
  if( pRes ) *pRes = 0;
  while( pCur->eState==CURSOR_VALID ){
    rc = cursorOnDegenerateSchemaRow(pCur, &isDegenerate);
    if( rc!=SQLITE_OK || !isDegenerate ) break;
    CLEAR_CACHED_PAYLOAD(pCur);
    rc = dir>=0 ? prollyBtCursorNext(pCur, 0) : prollyBtCursorPrevious(pCur, 0);
    if( rc!=SQLITE_OK && rc!=SQLITE_DONE ) break;
    if( pCur->eState==CURSOR_INVALID ){
      if( pRes ) *pRes = 1;
      rc = SQLITE_OK;
      break;
    }
  }
  return rc;
}

static SQLITE_INLINE int prollyCursorNextFastLeaf(ProllyCursor *pCur){
  ProllyCursorLevel *pLevel = &pCur->aLevel[pCur->iLevel];
  ProllyCacheEntry *pLeaf = pLevel->pEntry;
  assert( pCur->eState==PROLLY_CURSOR_VALID );
  if( pLevel->idx < pLeaf->node.nItems - 1 ){
    pLevel->idx++;
    return SQLITE_OK;
  }
  return prollyCursorNext(pCur);
}

static SQLITE_INLINE int prollyBtCursorNextFastIntLeaf(BtCursor *pCur){
  ProllyCursor *pProllyCur = &pCur->pCur;
  ProllyCursorLevel *pLevel;
  ProllyNode *pNode;
  const u8 *pVal;
  int nVal;
  int nAvail;
  int rc;
  if( pCur->eState!=CURSOR_VALID
   || pProllyCur->eState!=PROLLY_CURSOR_VALID
   || !pCur->curIntKey
   || pCur->mmActive
   || pCur->pMutMap!=0
   || pCur->cachedPayloadOwned
   || pCur->pCachedFrom!=0 ){
    return SQLITE_NOTFOUND;
  }
  pLevel = &pProllyCur->aLevel[pProllyCur->iLevel];
  pNode = &pLevel->pEntry->node;
  if( pLevel->idx>=pNode->nItems-1 ) return SQLITE_NOTFOUND;
  rc = prollyCursorCheckInterrupt(pCur);
  if( rc!=SQLITE_OK ) return rc;
  pLevel->idx++;
  prollyNodeValueSpanInline(pNode, pLevel->idx, &pVal, &nVal, &nAvail);
  if( nVal>0 && nAvail==nVal ){
    pCur->pCachedPayload = (u8*)pVal;
    pCur->nCachedPayload = nVal;
  }else{
    pCur->pCachedPayload = 0;
    pCur->nCachedPayload = 0;
  }
  pCur->curFlags &= ~(BTCF_AtLast|BTCF_ValidNKey|BTCF_DeleteKey);
  return SQLITE_OK;
}


static int mergeCompare(BtCursor *pCur, ProllyMutMapEntry *e){
  assert( pCur!=0 && e!=0 );
  assert( pCur->pCur.eState==PROLLY_CURSOR_VALID );
  if( pCur->curIntKey ){
    u64 tk = cursorCurrentTreeKeyPrefixInt(pCur);
    u64 ek = e->keyPrefix;
    if( tk < ek ) return -1;
    if( tk > ek ) return 1;
    return 0;
  }else{
    const u8 *pK; int nK;
    int n; int c;
    prollyCursorKey(&pCur->pCur, &pK, &nK);
    n = nK < e->nKey ? nK : e->nKey;
    c = memcmp(pK, e->pKey, n);
    if( c ) return c;
    return (nK < e->nKey) ? -1 : (nK > e->nKey) ? 1 : 0;
  }
}

static int mergeScan(BtCursor *pCur, int dir, int *pRes){
  assert( pCur!=0 );
  assert( pCur->pMutMap!=0 );
  assert( dir==1 || dir==-1 );
  if( pCur->mmPhysActive ){
    assert( pCur->mmPhysIdx>=0 && pCur->mmPhysIdx<pCur->pMutMap->nEntries );
    pCur->mmIdx = prollyMutMapOrderIndexFromEntry(
        pCur->pMutMap, &pCur->pMutMap->aEntries[pCur->mmPhysIdx]);
    pCur->mmPhysIdx = -1;
    pCur->mmPhysActive = 0;
  }
  for(;;){
    int treeOk = (pCur->pCur.eState==PROLLY_CURSOR_VALID);
    int mutOk  = (pCur->mmIdx >= 0 && pCur->mmIdx < pCur->pMutMap->nEntries);
    ProllyMutMapEntry *e;
    int cmp;
    int rc = prollyCursorCheckInterrupt(pCur);
    if( rc!=SQLITE_OK ) return rc;

    if( !treeOk && !mutOk ){
      if( pRes ){ *pRes = 1; return SQLITE_OK; }
      return SQLITE_DONE;
    }
    if( !mutOk ){
      pCur->mergeSrc = MERGE_SRC_TREE;
      if( pRes ) *pRes = 0;
      return SQLITE_OK;
    }
    rc = orderedMutMapEntryAt(pCur->pMutMap, pCur->mmIdx, &e);
    if( rc!=SQLITE_OK ) return rc;
    if( !treeOk ){
      if( e->op==PROLLY_EDIT_DELETE ){ pCur->mmIdx += dir; continue; }
      pCur->mergeSrc = MERGE_SRC_MUT;
      if( pRes ) *pRes = 0;
      return SQLITE_OK;
    }
    cmp = mergeCompare(pCur, e);
    if( cmp*dir < 0 ){
      pCur->mergeSrc = MERGE_SRC_TREE;
      if( pRes ) *pRes = 0;
      return SQLITE_OK;
    }else if( cmp*dir > 0 ){
      if( e->op==PROLLY_EDIT_DELETE ){ pCur->mmIdx += dir; continue; }
      pCur->mergeSrc = MERGE_SRC_MUT;
      if( pRes ) *pRes = 0;
      return SQLITE_OK;
    }else{
      if( e->op==PROLLY_EDIT_DELETE ){
        pCur->mmIdx += dir;
        cmp = advanceTreeCursor(pCur, dir);
        if( cmp!=SQLITE_OK ) return cmp;
        continue;
      }
      pCur->mergeSrc = MERGE_SRC_BOTH;
      if( pRes ) *pRes = 0;
      return SQLITE_OK;
    }
  }
}

static int ensureCursorMutMapOrder(BtCursor *pCur){
  if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
    return prollyMutMapEnsureOrder(pCur->pMutMap);
  }
  return SQLITE_OK;
}

static int cursorNormalizeMmPhys(BtCursor *pCur){
  if( pCur->mmPhysActive ){
    int rc;
    assert( pCur->pMutMap!=0 );
    assert( pCur->mmPhysIdx>=0 && pCur->mmPhysIdx<pCur->pMutMap->nEntries );
    rc = ensureCursorMutMapOrder(pCur);
    if( rc!=SQLITE_OK ) return rc;
    pCur->mmIdx = prollyMutMapOrderIndexFromEntry(
        pCur->pMutMap, &pCur->pMutMap->aEntries[pCur->mmPhysIdx]);
    pCur->mmPhysIdx = -1;
    pCur->mmPhysActive = 0;
  }
  return SQLITE_OK;
}

static int materializeDeferredTreeSeek(BtCursor *pCur, int dir){
  int rc;
  int res = 0;
  if( !pCur->deferredTreeSeek ) return SQLITE_OK;
  assert( pCur->mmActive );
  assert( pCur->pMutMap!=0 );
#ifdef SQLITE_DEBUG
  if( pCur->curIntKey && pCur->mmIdx>=0 && pCur->mmIdx<pCur->pMutMap->nEntries ){
    ProllyMutMapEntry *eChk = 0;
    assert( orderedMutMapEntryAt(pCur->pMutMap, pCur->mmIdx, &eChk)==SQLITE_OK );
    assert( eChk && prollyMutMapEntryIntKey(eChk)==pCur->cachedIntKey );
  }
#endif
  pCur->deferredTreeSeek = 0;
  refreshCursorRoot(pCur);
  rc = prollyCursorCheckInterrupt(pCur);
  if( rc!=SQLITE_OK ) return rc;
  if( pCur->curIntKey ){
    rc = prollyCursorSeekInt(&pCur->pCur, pCur->cachedIntKey, &res);
  }else{
    ProllyMutMapEntry *e;
    rc = orderedMutMapEntryAt(pCur->pMutMap, pCur->mmIdx, &e);
    if( rc!=SQLITE_OK ) return rc;
    rc = prollyCursorSeekBlob(&pCur->pCur, e->pKey, e->nKey, &res);
  }
  if( rc!=SQLITE_OK ) return rc;
  if( res==0 ){
    pCur->mergeSrc = MERGE_SRC_BOTH;
  }else{
    pCur->mergeSrc = MERGE_SRC_MUT;
    if( dir>0 && res<0 && prollyCursorIsValid(&pCur->pCur) ){
      rc = prollyCursorNext(&pCur->pCur);
    }else if( dir<0 && res>0 && prollyCursorIsValid(&pCur->pCur) ){
      rc = prollyCursorPrev(&pCur->pCur);
    }
  }
  return rc;
}

/* First live row >= cachedIntKey; if none, prime past EOF for a backward step. */
static int deferredMergedSeekPosition(BtCursor *pCur){
  ProllyMutMapIter it;
  int res = 0;
  int rc;
  i64 intKey;
  pCur->deferredMergedSeek = 0;
  intKey = pCur->cachedIntKey;
  refreshCursorRoot(pCur);
  rc = prollyCursorCheckInterrupt(pCur);
  if( rc!=SQLITE_OK ) return rc;
  rc = prollyCursorSeekInt(&pCur->pCur, intKey, &res);
  if( rc!=SQLITE_OK ) return rc;
  if( res<0 && prollyCursorIsValid(&pCur->pCur) ){
    rc = prollyCursorNext(&pCur->pCur);
    if( rc!=SQLITE_OK ) return rc;
  }
  rc = prollyMutMapIterSeek(&it, pCur->pMutMap, 0, 0, intKey);
  if( rc!=SQLITE_OK ) return rc;
  pCur->mmIdx = it.idx;
  pCur->mmActive = 1;
  return SQLITE_OK;
}

static int materializeDeferredMergedSeek(BtCursor *pCur){
  int res = 0;
  int rc;
  if( !pCur->deferredMergedSeek ) return SQLITE_OK;
  rc = deferredMergedSeekPosition(pCur);
  if( rc!=SQLITE_OK ) return rc;
  rc = mergeScan(pCur, 1, &res);
  if( rc!=SQLITE_OK ) return rc;
  if( res==0 ){
    pCur->eState = CURSOR_VALID;
    /* BOTH shadows the tree; getCursorPayload prefers the cache. */
    if( pCur->mergeSrc==MERGE_SRC_TREE ){
      cacheCurrentTreePayloadIfIntKey(pCur);
    }
  }else{
    pCur->mergeSrc = MERGE_SRC_BOTH;
    pCur->eState = CURSOR_INVALID;
  }
  return SQLITE_OK;
}

/* Last live row below cachedIntKey (res<0). For Eof/payload, not a step. */
int materializeDeferredMergedSeekBackward(BtCursor *pCur){
  int res = 0;
  int rc;
  if( !pCur->deferredMergedSeek ) return SQLITE_OK;
  rc = deferredMergedSeekPosition(pCur);
  if( rc!=SQLITE_OK ) return rc;
  pCur->mergeSrc = MERGE_SRC_BOTH;
  if( pCur->pCur.eState==PROLLY_CURSOR_VALID ){
    rc = advanceTreeCursor(pCur, -1);
    if( rc!=SQLITE_OK ) return rc;
  }else if( pCur->pCur.eState==PROLLY_CURSOR_EOF ){
    rc = prollyCursorPrev(&pCur->pCur);
    if( rc!=SQLITE_OK ) return rc;
  }
  pCur->mmIdx--;
  rc = mergeScan(pCur, -1, &res);
  if( rc!=SQLITE_OK ) return rc;
  if( res==0 ){
    pCur->eState = CURSOR_VALID;
    if( pCur->mergeSrc==MERGE_SRC_TREE ){
      cacheCurrentTreePayloadIfIntKey(pCur);
    }
  }else{
    pCur->eState = CURSOR_INVALID;
  }
  return SQLITE_OK;
}

static int mergeStepForward(BtCursor *pCur){
  int rc = SQLITE_OK;
  rc = cursorNormalizeMmPhys(pCur);
  if( rc!=SQLITE_OK ) return rc;
  if( pCur->deferredMergedSeek ){
    rc = materializeDeferredMergedSeek(pCur);
    if( rc!=SQLITE_OK ) return rc;
    return pCur->eState==CURSOR_VALID ? SQLITE_OK : SQLITE_DONE;
  }
  rc = materializeDeferredTreeSeek(pCur, 1);
  if( rc!=SQLITE_OK ) return rc;
  /* After a backward MUT landing, re-seek the tree above this key. */
  if( pCur->mergeStepDir < 0
   && pCur->mergeSrc==MERGE_SRC_MUT
   && pCur->mmIdx>=0 && pCur->mmIdx<pCur->pMutMap->nEntries ){
    ProllyMutMapEntry *e;
    rc = orderedMutMapEntryAt(pCur->pMutMap, pCur->mmIdx, &e);
    if( rc!=SQLITE_OK ) return rc;
    {
      int res = 0;
      refreshCursorRoot(pCur);
      if( pCur->curIntKey ){
        rc = prollyCursorSeekInt(&pCur->pCur, prollyMutMapEntryIntKey(e), &res);
      }else{
        rc = prollyCursorSeekBlob(&pCur->pCur, e->pKey, e->nKey, &res);
      }
      if( rc!=SQLITE_OK ) return rc;
      if( res<0 && prollyCursorIsValid(&pCur->pCur) ){
        rc = prollyCursorNext(&pCur->pCur);
        if( rc!=SQLITE_OK ) return rc;
      }else if( res==0 ){
        pCur->mergeSrc = MERGE_SRC_BOTH;
      }
    }
  }
  /* After a reversal, skip mut-map entries now behind the cursor. */
  if( pCur->mergeSrc==MERGE_SRC_TREE && prollyCursorIsValid(&pCur->pCur) ){
    if( pCur->mmIdx < 0 ) pCur->mmIdx = 0;
    while( pCur->mmIdx < pCur->pMutMap->nEntries ){
      ProllyMutMapEntry *e;
      rc = orderedMutMapEntryAt(pCur->pMutMap, pCur->mmIdx, &e);
      if( rc!=SQLITE_OK ) return rc;
      if( mergeCompare(pCur, e) <= 0 ) break;
      pCur->mmIdx++;
    }
  }
  if( pCur->mergeSrc==MERGE_SRC_TREE || pCur->mergeSrc==MERGE_SRC_BOTH ){
    rc = advanceTreeCursor(pCur, 1);
    if( rc!=SQLITE_OK ) return rc;
  }
  if( pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH )
    pCur->mmIdx++;
  rc = mergeScan(pCur, 1, 0);
  pCur->mergeStepDir = 1;
  CLEAR_CACHED_PAYLOAD(pCur);
  return rc;
}

static int mergeStepBackward(BtCursor *pCur){
  int rc = SQLITE_OK;

  rc = cursorNormalizeMmPhys(pCur);
  if( rc!=SQLITE_OK ) return rc;
  if( pCur->deferredMergedSeek ){
    rc = deferredMergedSeekPosition(pCur);
    if( rc!=SQLITE_OK ) return rc;
    pCur->mergeSrc = MERGE_SRC_BOTH;
  }
  rc = materializeDeferredTreeSeek(pCur, -1);
  if( rc!=SQLITE_OK ) return rc;
  /* After a forward MUT landing, re-seek the tree below this key. */
  if( pCur->mergeStepDir > 0
   && pCur->mergeSrc==MERGE_SRC_MUT
   && pCur->mmIdx>=0 && pCur->mmIdx<pCur->pMutMap->nEntries ){
    ProllyMutMapEntry *e;
    rc = orderedMutMapEntryAt(pCur->pMutMap, pCur->mmIdx, &e);
    if( rc!=SQLITE_OK ) return rc;
    {
      int res = 0;
      refreshCursorRoot(pCur);
      if( pCur->curIntKey ){
        rc = prollyCursorSeekInt(&pCur->pCur, prollyMutMapEntryIntKey(e), &res);
      }else{
        rc = prollyCursorSeekBlob(&pCur->pCur, e->pKey, e->nKey, &res);
      }
      if( rc!=SQLITE_OK ) return rc;
      if( res>0 && prollyCursorIsValid(&pCur->pCur) ){
        rc = prollyCursorPrev(&pCur->pCur);
        if( rc!=SQLITE_OK ) return rc;
      }else if( res==0 ){
        pCur->mergeSrc = MERGE_SRC_BOTH;
      }
    }
  }
  /* After a reversal, walk mmIdx back onto the pending row below. */
  if( pCur->mergeSrc==MERGE_SRC_TREE && prollyCursorIsValid(&pCur->pCur) ){
    if( pCur->mmIdx >= pCur->pMutMap->nEntries ){
      pCur->mmIdx = pCur->pMutMap->nEntries - 1;
    }
    while( pCur->mmIdx >= 0 ){
      ProllyMutMapEntry *e;
      rc = orderedMutMapEntryAt(pCur->pMutMap, pCur->mmIdx, &e);
      if( rc!=SQLITE_OK ) return rc;
      if( mergeCompare(pCur, e) >= 0 ) break;
      pCur->mmIdx--;
    }
  }
  if( pCur->mergeSrc==MERGE_SRC_TREE || pCur->mergeSrc==MERGE_SRC_BOTH ){
    rc = advanceTreeCursor(pCur, -1);
    if( rc!=SQLITE_OK ) return rc;
  }
  if( pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH )
    pCur->mmIdx--;
  rc = mergeScan(pCur, -1, 0);
  pCur->mergeStepDir = -1;
  CLEAR_CACHED_PAYLOAD(pCur);
  return rc;
}

static int seedMutMapIterFromCursor(
  BtCursor *pCur,
  ProllyMutMapIter *pIt
){
  if( pCur->curIntKey ){
    if( pCur->curFlags & (BTCF_ValidNKey|BTCF_DeleteKey) ){
      return prollyMutMapIterSeek(pIt, pCur->pMutMap, 0, 0,
                                  pCur->cachedIntKey);
    }
  }else{
    if( pCur->pCachedPayload && pCur->nCachedPayload > 0 ){
      u8 *pSortKey = 0;
      int nSortKey = 0;
      int nMutKeyField = 0;
      int rc;
      if( pCur->pKeyInfo
       && pCur->pKeyInfo->nKeyField < pCur->pKeyInfo->nAllField ){
        nMutKeyField = (int)pCur->pKeyInfo->nKeyField;
      }
      rc = sortKeyFromRecordPrefixColl(pCur->pCachedPayload, pCur->nCachedPayload,
                                        nMutKeyField, pCur->pKeyInfo,
                                        &pSortKey, &nSortKey);
      if( rc!=SQLITE_OK ) return rc;
      rc = prollyMutMapIterSeek(pIt, pCur->pMutMap, pSortKey, nSortKey, 0);
      sqlite3_free(pSortKey);
      return rc;
    }
  }
  return SQLITE_NOTFOUND;
}

/* Resume after a write deactivated merge: re-seed both sides past cachedIntKey. */
static int resumeDeactivatedMergedScan(BtCursor *pCur, int dir){
  ProllyMutMapIter it;
  i64 intKey = pCur->cachedIntKey;
  int res = 0;
  int rc;

  refreshCursorRoot(pCur);
  rc = prollyCursorCheckInterrupt(pCur);
  if( rc!=SQLITE_OK ) return rc;
  rc = prollyCursorSeekInt(&pCur->pCur, intKey, &res);
  if( rc!=SQLITE_OK ) return rc;
  if( prollyCursorIsValid(&pCur->pCur) && res*dir<=0 ){
    rc = dir>0 ? prollyCursorNext(&pCur->pCur) : prollyCursorPrev(&pCur->pCur);
    if( rc!=SQLITE_OK ) return rc;
  }
  rc = prollyMutMapIterSeek(&it, pCur->pMutMap, 0, 0, intKey);
  if( rc!=SQLITE_OK ) return rc;
  if( dir>0 ){
    if( it.idx < pCur->pMutMap->nEntries ){
      ProllyMutMapEntry *e;
      rc = orderedMutMapEntryAt(pCur->pMutMap, it.idx, &e);
      if( rc!=SQLITE_OK ) return rc;
      if( prollyMutMapEntryIntKey(e)==intKey ) it.idx++;
    }
  }else{
    it.idx--;
  }
  pCur->mmIdx = it.idx;
  pCur->mmPhysIdx = -1;
  pCur->mmPhysActive = 0;
  pCur->mmActive = 1;
  pCur->deferredTreeSeek = 0;
  pCur->mergeStepDir = (i8)(dir>0 ? 1 : -1);
  rc = mergeScan(pCur, dir, &res);
  if( rc!=SQLITE_OK ) return rc;
  if( res ){
    pCur->eState = CURSOR_INVALID;
    return SQLITE_DONE;
  }
  pCur->eState = CURSOR_VALID;
  if( pCur->mergeSrc==MERGE_SRC_TREE ){
    cacheCurrentTreePayloadIfIntKey(pCur);
  }
  return SQLITE_OK;
}

static int mergeFirst(BtCursor *pCur, int *pRes){
  int rc = ensureCursorMutMapOrder(pCur);
  if( rc!=SQLITE_OK ) return rc;
  pCur->mergeSrc = MERGE_SRC_TREE;
  pCur->mmIdx = 0;
  return mergeScan(pCur, 1, pRes);
}

int mergeLast(BtCursor *pCur, int *pRes){
  int rc = ensureCursorMutMapOrder(pCur);
  if( rc!=SQLITE_OK ) return rc;
  pCur->mmIdx = pCur->pMutMap->nEntries - 1;
  pCur->mergeSrc = MERGE_SRC_TREE;
  return mergeScan(pCur, -1, pRes);
}

int prollyBtCursorFirst(BtCursor *pCur, int *pRes){
  int rc;
  CLEAR_CACHED_PAYLOAD(pCur);
  CLEAR_CACHED_SEEK_KEY(pCur);
  CLEAR_CACHED_COMPARE_KEY(pCur);
  refreshCursorRoot(pCur);
  rc = prollyCursorCheckInterrupt(pCur);
  if( rc!=SQLITE_OK ) return rc;
  rc = prollyCursorFirst(&pCur->pCur, pRes);
  if( rc!=SQLITE_OK ) return rc;
  clearMergeCursorState(pCur);

  if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
    pCur->mmActive = 1;
    rc = mergeFirst(pCur, pRes);
    if( rc!=SQLITE_OK ) return rc;
  }else{
    pCur->mmActive = 0;
  }
  pCur->eState = (*pRes==0) ? CURSOR_VALID : CURSOR_INVALID;
  pCur->curFlags &= ~BTCF_AtLast;
  if( rc==SQLITE_OK && pCur->eState==CURSOR_VALID ){
    rc = skipDegenerateSchemaRows(pCur, 1, pRes);
  }
  return rc;
}
int sqlite3BtreeFirst(BtCursor *pCur, int *pRes){
  if( !pCur ) return SQLITE_OK;
  return pCur->pCurOps->xFirst(pCur, pRes);
}

int prollyBtCursorLast(BtCursor *pCur, int *pRes){
  int rc;
  CLEAR_CACHED_PAYLOAD(pCur);
  CLEAR_CACHED_SEEK_KEY(pCur);
  CLEAR_CACHED_COMPARE_KEY(pCur);
  refreshCursorRoot(pCur);
  rc = prollyCursorCheckInterrupt(pCur);
  if( rc!=SQLITE_OK ) return rc;
  rc = prollyCursorLast(&pCur->pCur, pRes);
  if( rc!=SQLITE_OK ) return rc;
  clearMergeCursorState(pCur);

  if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
    pCur->mmActive = 1;
    rc = mergeLast(pCur, pRes);
    if( rc!=SQLITE_OK ) return rc;
  }else{
    pCur->mmActive = 0;
  }
  if( *pRes==0 ){
    pCur->eState = CURSOR_VALID;
    pCur->curFlags |= BTCF_AtLast;
  } else {
    pCur->eState = CURSOR_INVALID;
  }
  if( rc==SQLITE_OK && pCur->eState==CURSOR_VALID ){
    rc = skipDegenerateSchemaRows(pCur, -1, pRes);
  }
  return rc;
}
int sqlite3BtreeLast(BtCursor *pCur, int *pRes){
  if( !pCur ) return SQLITE_OK;
  return pCur->pCurOps->xLast(pCur, pRes);
}

static SQLITE_INLINE int prollyBtCursorStepPrologue(
  BtCursor *pCur,
  int dir,
  int *pImmediate
){
  int rc;
  *pImmediate = 1;
  CLEAR_CACHED_PAYLOAD(pCur);

  rc = prollyCursorCheckInterrupt(pCur);
  if( rc!=SQLITE_OK ) return rc;

  if( pCur->eState==CURSOR_INVALID ){
    return SQLITE_DONE;
  }

  /* Faulted: nodes already released; do not step into freed memory. */
  if( pCur->eState==CURSOR_FAULT ){
    return pCur->skipNext;
  }

  if( pCur->eState==CURSOR_REQUIRESEEK ){
    rc = restoreCursorPosition(pCur, 0);
    if( rc!=SQLITE_OK ) return rc;
    if( pCur->eState==CURSOR_INVALID ){
      return SQLITE_DONE;
    }
  }

  if( pCur->eState==CURSOR_SKIPNEXT ){
    pCur->eState = CURSOR_VALID;
    if( (dir>0) ? (pCur->skipNext>0) : (pCur->skipNext<0) ){
      pCur->skipNext = 0;
      return SQLITE_OK;
    }
    pCur->skipNext = 0;
  }

  *pImmediate = 0;
  return SQLITE_OK;
}

static int prollyCursorClassifyMergeSrc(BtCursor *pCur, int idx){
  int both = 0;
  if( idx >= 0 && idx < pCur->pMutMap->nEntries
   && prollyCursorIsValid(&pCur->pCur) ){
    ProllyMutMapEntry *e;
    int rc = prollyMutMapEntryAt(pCur->pMutMap, idx, &e);
    if( rc!=SQLITE_OK ) return rc;
    both = mergeCompare(pCur, e)==0;
  }
  if( both ){
    pCur->mergeSrc = MERGE_SRC_BOTH;
  }else if( !prollyCursorIsValid(&pCur->pCur) ){
    pCur->mergeSrc = MERGE_SRC_MUT;
  }else{
    pCur->mergeSrc = MERGE_SRC_TREE;
  }
  return SQLITE_OK;
}

static int prollyCursorApplyMergeStep(BtCursor *pCur, int dir){
  int rc = dir>0 ? mergeStepForward(pCur) : mergeStepBackward(pCur);
  if( rc==SQLITE_DONE ){
    pCur->eState = CURSOR_INVALID;
  }else if( rc==SQLITE_OK ){
    pCur->eState = CURSOR_VALID;
  }
  return rc;
}

/* Tree steps never return SQLITE_DONE; map EOF eState to SQLITE_DONE. */
static SQLITE_INLINE int prollyCursorFinishTreeStep(BtCursor *pCur, int rc){
  if( rc!=SQLITE_OK ) return rc;
  if( pCur->pCur.eState==PROLLY_CURSOR_VALID ){
    pCur->eState = CURSOR_VALID;
    if( pCur->curIntKey ){
      cacheCurrentTreePayloadIfIntKey(pCur);
    }else{
      cacheCurrentTreeStoredPayloadNonIntKey(pCur);
    }
    return SQLITE_OK;
  }
  pCur->eState = CURSOR_INVALID;
  return SQLITE_DONE;
}

int prollyBtCursorNext(BtCursor *pCur, int flags){
  int rc, immediate;
  (void)flags;

  rc = prollyBtCursorNextFastIntLeaf(pCur);
  if( rc!=SQLITE_NOTFOUND ) return rc;

  rc = prollyBtCursorStepPrologue(pCur, 1, &immediate);
  if( immediate ) return rc;

  /* BTCF_AtLast: a merged Last cursor must not step into leftover merge state. */
  if( pCur->mmActive && (pCur->curFlags & BTCF_AtLast)!=0 ){
    pCur->eState = CURSOR_INVALID;
    return SQLITE_DONE;
  }

  if( !pCur->mmActive && pCur->pMutMap==0 ){
    rc = prollyCursorFinishTreeStep(pCur, prollyCursorNextFastLeaf(&pCur->pCur));
    if( rc==SQLITE_DONE ) return rc;
    pCur->curFlags &= ~(BTCF_AtLast|BTCF_ValidNKey|BTCF_DeleteKey);
    return rc;
  }

  if( pCur->mmActive ){
    rc = prollyCursorApplyMergeStep(pCur, 1);
  }else if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap)
         && pCur->curIntKey
         && (pCur->curFlags & (BTCF_ValidNKey|BTCF_DeleteKey))
         && !(prollyCursorIsValid(&pCur->pCur)
              && prollyCursorIntKey(&pCur->pCur)==pCur->cachedIntKey) ){
    /* Tree not on the logical key: resume from cachedIntKey, not the tree. */
    rc = ensureCursorMutMapOrder(pCur);
    if( rc!=SQLITE_OK ) return rc;
    rc = resumeDeactivatedMergedScan(pCur, 1);
  }else if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
    ProllyMutMapIter it;
    rc = ensureCursorMutMapOrder(pCur);
    if( rc!=SQLITE_OK ) return rc;
    if( pCur->curIntKey && prollyCursorIsValid(&pCur->pCur) ){
      rc = prollyMutMapIterSeek(&it, pCur->pMutMap, 0, 0,
                                prollyCursorIntKey(&pCur->pCur));
    }else if( !pCur->curIntKey && prollyCursorIsValid(&pCur->pCur) ){
      const u8 *pK; int nK;
      prollyCursorKey(&pCur->pCur, &pK, &nK);
      rc = prollyMutMapIterSeek(&it, pCur->pMutMap, pK, nK, 0);
    }else if( pCur->eState==CURSOR_VALID ){
      rc = seedMutMapIterFromCursor(pCur, &it);
      if( rc==SQLITE_NOTFOUND ){
        rc = prollyMutMapIterFirst(&it, pCur->pMutMap);
      }
    }else{
      rc = prollyMutMapIterFirst(&it, pCur->pMutMap);
    }
    if( rc!=SQLITE_OK ) return rc;
    pCur->mmIdx = it.idx;
    pCur->mmActive = 1;
    rc = prollyCursorClassifyMergeSrc(pCur, it.idx);
    if( rc!=SQLITE_OK ) return rc;
    rc = prollyCursorApplyMergeStep(pCur, 1);
  }else{
    rc = prollyCursorFinishTreeStep(pCur, prollyCursorNextFastLeaf(&pCur->pCur));
    if( rc==SQLITE_DONE ) return rc;
  }
  pCur->curFlags &= ~(BTCF_AtLast|BTCF_ValidNKey|BTCF_DeleteKey);
  if( rc==SQLITE_OK && pCur->eState==CURSOR_VALID ){
    rc = skipDegenerateSchemaRows(pCur, 1, 0);
  }
  return rc;
}
int sqlite3BtreeNext(BtCursor *pCur, int flags){
  if( !pCur ) return SQLITE_DONE;
  if( pCur->pCurOps==&prollyCursorOps ){
    return prollyBtCursorNext(pCur, flags);
  }
  return pCur->pCurOps->xNext(pCur, flags);
}

int prollyBtCursorPrevious(BtCursor *pCur, int flags){
  int rc, immediate;
  (void)flags;

  rc = prollyBtCursorStepPrologue(pCur, -1, &immediate);
  if( immediate ) return rc;

  if( pCur->mmActive ){
    rc = prollyCursorApplyMergeStep(pCur, -1);
  }else if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap)
         && pCur->curIntKey
         && (pCur->curFlags & (BTCF_ValidNKey|BTCF_DeleteKey))
         && !(prollyCursorIsValid(&pCur->pCur)
              && prollyCursorIntKey(&pCur->pCur)==pCur->cachedIntKey) ){
    rc = ensureCursorMutMapOrder(pCur);
    if( rc!=SQLITE_OK ) return rc;
    rc = resumeDeactivatedMergedScan(pCur, -1);
  }else if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
    ProllyMutMapIter it;
    rc = ensureCursorMutMapOrder(pCur);
    if( rc!=SQLITE_OK ) return rc;
    /* Seek is a lower bound; a backward step always uses the prior entry. */
    if( pCur->curIntKey && prollyCursorIsValid(&pCur->pCur) ){
      rc = prollyMutMapIterSeek(&it, pCur->pMutMap, 0, 0,
                                prollyCursorIntKey(&pCur->pCur));
      if( rc!=SQLITE_OK ) return rc;
      it.idx--;
    }else if( !pCur->curIntKey && prollyCursorIsValid(&pCur->pCur) ){
      const u8 *pK; int nK;
      prollyCursorKey(&pCur->pCur, &pK, &nK);
      rc = prollyMutMapIterSeek(&it, pCur->pMutMap, pK, nK, 0);
      if( rc!=SQLITE_OK ) return rc;
      it.idx--;
    }else if( pCur->eState==CURSOR_VALID ){
      rc = seedMutMapIterFromCursor(pCur, &it);
      if( rc==SQLITE_NOTFOUND ){
        rc = prollyMutMapIterLast(&it, pCur->pMutMap);
      }else if( rc==SQLITE_OK ){
        it.idx--;
      }
    }else{
      rc = prollyMutMapIterLast(&it, pCur->pMutMap);
    }
    if( rc!=SQLITE_OK ) return rc;
    pCur->mmIdx = it.idx;
    pCur->mmActive = 1;
    rc = prollyCursorClassifyMergeSrc(pCur, it.idx);
    if( rc!=SQLITE_OK ) return rc;
    rc = prollyCursorApplyMergeStep(pCur, -1);
  }else{
    rc = prollyCursorFinishTreeStep(pCur, prollyCursorPrev(&pCur->pCur));
    if( rc==SQLITE_DONE ) return rc;
  }
  pCur->curFlags &= ~(BTCF_AtLast|BTCF_ValidNKey|BTCF_DeleteKey);
  if( rc==SQLITE_OK && pCur->eState==CURSOR_VALID ){
    rc = skipDegenerateSchemaRows(pCur, -1, 0);
  }
  return rc;
}
int sqlite3BtreePrevious(BtCursor *pCur, int flags){
  if( !pCur ) return SQLITE_DONE;
  return pCur->pCurOps->xPrevious(pCur, flags);
}

int prollyBtCursorEof(BtCursor *pCur){
  if( pCur->deferredMergedSeek ){
    int rc = materializeDeferredMergedSeekBackward(pCur);
    if( rc!=SQLITE_OK ){
      pCur->eState = CURSOR_FAULT;
      pCur->skipNext = rc;
      return 1;
    }
  }
  return (pCur->eState!=CURSOR_VALID);
}

/* SQLITE_OK for orig; otherwise skipNext of a CURSOR_FAULT. */
int doltliteBtreeCursorFaultCode(BtCursor *pCur){
  if( !pCur || pCur->pCurOps!=&prollyCursorOps ) return SQLITE_OK;
  if( pCur->eState!=CURSOR_FAULT ) return SQLITE_OK;
  return pCur->skipNext ? pCur->skipNext : SQLITE_ERROR;
}

int sqlite3BtreeEof(BtCursor *pCur){
  if( !pCur ) return 1;
  return pCur->pCurOps->xEof(pCur);
}

int prollyBtCursorIsEmpty(BtCursor *pCur, int *pRes){
  struct TableEntry *pTE;
  pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
  if( !pTE ){
    *pRes = 1;
    return SQLITE_OK;
  }
  /* Pending rows (even tombstones) make the table non-empty. O(1) for OP_IfEmpty. */
  *pRes = (prollyHashIsEmpty(&pTE->root)
        && (pTE->pPending==0 || prollyMutMapIsEmpty(pTE->pPending))) ? 1 : 0;
  return SQLITE_OK;
}
int sqlite3BtreeIsEmpty(BtCursor *pCur, int *pRes){
  if( !pCur ) { *pRes = 1; return SQLITE_OK; }
  return pCur->pCurOps->xIsEmpty(pCur, pRes);
}


int sqlite3BtreeProllyIndexRowid(BtCursor *pCur, i64 *pRowid){
  const u8 *pKey = 0;
  int nKey = 0;
  u8 *pRec = 0;
  int nRecAlloc = 0;
  int nRec = 0;
  int rc;
  u32 szHdr;
  u32 typeRowid;
  u32 lenRowid;
  Mem v;

  if( !pCur || pCur->pCurOps!=&prollyCursorOps || pCur->curIntKey ){
    return SQLITE_NOTFOUND;
  }
  if( pCur->eState!=CURSOR_VALID ){
    return SQLITE_NOTFOUND;
  }
  if( pCur->isTableRoot ){
    *pRowid = sqlite3BtreeSqlRowid(pCur);
    if( pCur->eState==CURSOR_FAULT ) return pCur->skipNext;
    return SQLITE_OK;
  }

  if( pCur->mmActive
   && (pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH) ){
    ProllyMutMapEntry *e;
    int rc = currentMutMapEntry(pCur, &e);
    if( rc!=SQLITE_OK ) return rc;
    if( !e ) return SQLITE_NOTFOUND;
    pKey = e->pKey;
    nKey = e->nKey;
  }else if( prollyCursorIsValid(&pCur->pCur) ){
    prollyCursorKey(&pCur->pCur, &pKey, &nKey);
  }else{
    return SQLITE_NOTFOUND;
  }

  rc = recordFromSortKeyBufferColl(
      pKey, nKey, pCur->pKeyInfo, &pRec, &nRecAlloc, &nRec);
  if( rc!=SQLITE_OK ){
    sqlite3_free(pRec);
    return rc;
  }

  getVarint32NR(pRec, szHdr);
  testcase( szHdr==3 );
  testcase( szHdr==(u32)nRec );
  testcase( szHdr>0x7fffffff );
  if( unlikely(szHdr<3 || szHdr>(u32)nRec) ){
    goto prolly_idx_rowid_corruption;
  }

  getVarint32NR(&pRec[szHdr-1], typeRowid);
  testcase( typeRowid==1 );
  testcase( typeRowid==2 );
  testcase( typeRowid==3 );
  testcase( typeRowid==4 );
  testcase( typeRowid==5 );
  testcase( typeRowid==6 );
  testcase( typeRowid==8 );
  testcase( typeRowid==9 );
  if( unlikely(typeRowid<1 || typeRowid>9 || typeRowid==7) ){
    goto prolly_idx_rowid_corruption;
  }
  lenRowid = (u32)dlSerialTypeLen(typeRowid);
  testcase( (u32)nRec==szHdr+lenRowid );
  if( unlikely((u32)nRec<szHdr+lenRowid) ){
    goto prolly_idx_rowid_corruption;
  }

  sqlite3VdbeSerialGet(&pRec[nRec-lenRowid], typeRowid, &v);
  *pRowid = v.u.i;
  sqlite3_free(pRec);
  return SQLITE_OK;

prolly_idx_rowid_corruption:
  sqlite3_free(pRec);
  return SQLITE_CORRUPT_BKPT;
}

void prollyBtCursorCursorPin(BtCursor *pCur){
  pCur->isPinned = 1;
  pCur->curFlags |= BTCF_Pinned;
}
void sqlite3BtreeCursorPin(BtCursor *pCur){
  if( !pCur ) return;
  pCur->pCurOps->xCursorPin(pCur);
}

void prollyBtCursorCursorUnpin(BtCursor *pCur){
  pCur->isPinned = 0;
  pCur->curFlags &= ~BTCF_Pinned;
}
void sqlite3BtreeCursorUnpin(BtCursor *pCur){
  if( !pCur ) return;
  pCur->pCurOps->xCursorUnpin(pCur);
}

void prollyBtCursorCursorHintFlags(BtCursor *pCur, unsigned x){
  pCur->hints = (u8)(x & 0xFF);
}
void sqlite3BtreeCursorHintFlags(BtCursor *pCur, unsigned x){
  pCur->pCurOps->xCursorHintFlags(pCur, x);
}

#ifdef SQLITE_ENABLE_CURSOR_HINTS
void sqlite3BtreeCursorHint(BtCursor *pCur, int eHintType, ...){
  (void)pCur; (void)eHintType;
}
#endif

int prollyBtCursorCursorHasHint(BtCursor *pCur, unsigned int mask){
  return (pCur->hints & mask) != 0;
}
int sqlite3BtreeCursorHasHint(BtCursor *pCur, unsigned int mask){
  return pCur->pCurOps->xCursorHasHint(pCur, mask);
}

BtCursor *sqlite3BtreeFakeValidCursor(void){
  static BtCursor fakeCursor;
  static int initialized = 0;
  if( !initialized ){
    memset(&fakeCursor, 0, sizeof(fakeCursor));
    fakeCursor.eState = CURSOR_VALID;
    initialized = 1;
  }
  return &fakeCursor;
}


#ifndef NDEBUG
int prollyBtCursorCursorIsValid(BtCursor *pCur){
  return pCur && pCur->eState==CURSOR_VALID;
}
int sqlite3BtreeCursorIsValid(BtCursor *pCur){
  return pCur->pCurOps->xCursorIsValid(pCur);
}
#endif

int prollyBtCursorCursorIsValidNN(BtCursor *pCur){
  assert( pCur!=0 );
  return pCur->eState==CURSOR_VALID;
}
int sqlite3BtreeCursorIsValidNN(BtCursor *pCur){
  return pCur->pCurOps->xCursorIsValidNN(pCur);
}

#ifdef SQLITE_DEBUG
sqlite3_uint64 sqlite3BtreeSeekCount(Btree *p){
  return p ? p->nSeek : 0;
}
#endif

#ifdef SQLITE_TEST
int sqlite3BtreeCursorInfo(BtCursor *pCur, int *aResult, int upCnt){
  (void)pCur;
  if( aResult ){
    aResult[0] = 0;
    aResult[1] = 0;
    aResult[2] = 0;
    aResult[3] = 0;
    aResult[4] = 0;
    if( upCnt >= 6 ){
      aResult[5] = 0;
    }
    if( upCnt >= 10 ){
      aResult[6] = 0;
      aResult[7] = 0;
      aResult[8] = 0;
      aResult[9] = 0;
    }
  }
  return SQLITE_OK;
}

/* Amalgamation testfixture already defines this via test_btree.c. */
#ifndef SQLITE_AMALGAMATION
void sqlite3BtreeCursorList(Btree *p){
#ifndef SQLITE_OMIT_TRACE
  BtCursor *pCur;
  BtShared *pBt;

  if( !p || !p->pBt ) return;
  pBt = p->pBt;

  for(pCur=pBt->pCursor; pCur; pCur=pCur->pNext){
    const char *zState;
    switch( pCur->eState ){
      case CURSOR_VALID:       zState = "VALID";       break;
      case CURSOR_INVALID:     zState = "INVALID";     break;
      case CURSOR_SKIPNEXT:    zState = "SKIPNEXT";    break;
      case CURSOR_REQUIRESEEK: zState = "REQUIRESEEK"; break;
      case CURSOR_FAULT:       zState = "FAULT";       break;
      default:                 zState = "UNKNOWN";     break;
    }
    sqlite3DebugPrintf(
      "CURSOR %p: table=%d wrFlag=%d state=%s intKey=%d\n",
      (void*)pCur,
      (int)pCur->pgnoRoot,
      (pCur->curFlags & BTCF_WriteFlag) ? 1 : 0,
      zState,
      (int)pCur->curIntKey
    );
  }
#else
  (void)p;
#endif
}
#endif /* !SQLITE_AMALGAMATION */
#endif


#endif /* DOLTLITE_PROLLY */
