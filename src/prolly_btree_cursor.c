#ifdef DOLTLITE_PROLLY

#include "prolly_btree_int.h"

/* Cursor navigation, seeking, comparison, and payload access. */

static void cacheCurrentTreePayloadIfIntKey(BtCursor *pCur){
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

static void cacheCurrentTreeStoredPayloadNonIntKey(BtCursor *pCur){
  const u8 *pVal; int nVal;
  CLEAR_CACHED_PAYLOAD(pCur);
  cursorCurrentTreeValue(pCur, &pVal, &nVal);
  if( nVal > 0 ){
    pCur->pCachedPayload = (u8*)pVal;
    pCur->nCachedPayload = nVal;
    pCur->cachedPayloadOwned = 0;
  }
}


static int keyInfoHasUnsupportedCollation(
  const KeyInfo *pKeyInfo,
  int nField
){
  int i;
  if( !pKeyInfo ) return 0;
  if( nField<=0 || nField>pKeyInfo->nAllField ){
    nField = pKeyInfo->nAllField;
  }
  for(i=0; i<nField; i++){
    const CollSeq *pColl = pKeyInfo->aColl[i];
    if( !pColl || !pColl->zName ) continue;
    if( sqlite3StrICmp(pColl->zName, "BINARY")!=0
     && sqlite3StrICmp(pColl->zName, "NOCASE")!=0
     && sqlite3StrICmp(pColl->zName, "RTRIM")!=0 ){
      return 1;
    }
  }
  return 0;
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
    e = orderedMutMapEntryAt(pCur->pMutMap, pCur->mmIdx);
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
  pCur->deferredTreeSeek = 0;
  refreshCursorRoot(pCur);
  rc = prollyCursorCheckInterrupt(pCur);
  if( rc!=SQLITE_OK ) return rc;
  if( pCur->curIntKey ){
    rc = prollyCursorSeekInt(&pCur->pCur, pCur->cachedIntKey, &res);
  }else{
    ProllyMutMapEntry *e = orderedMutMapEntryAt(pCur->pMutMap, pCur->mmIdx);
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

/* Complete a table moveto whose merged repositioning was deferred: place
** the merged cursor on the first live row >= cachedIntKey. When nothing at
** or above the key survives, the cursor is left CURSOR_INVALID with the
** merge state primed one past the end, so a backward step from here lands
** on the last live row. */
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

/* Complete a deferred merged seek forward: land on the first live row above
** cachedIntKey (the key itself is known dead). */
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
    if( pCur->mergeSrc!=MERGE_SRC_MUT ){
      cacheCurrentTreePayloadIfIntKey(pCur);
    }
  }else{
    /* Nothing live at or above the key: leave the merge state primed one
    ** past the end so a backward step lands on the last live row. */
    pCur->mergeSrc = MERGE_SRC_BOTH;
    pCur->eState = CURSOR_INVALID;
  }
  return SQLITE_OK;
}

/* Complete a deferred merged seek backward: land on the last live row below
** cachedIntKey, matching the res<0 contract the deferred moveto reported.
** Used by the passive consumers (Eof and the payload/rowid readers), which
** read the current position rather than stepping. */
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
    if( pCur->mergeSrc!=MERGE_SRC_MUT ){
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
    /* The cursor conceptually sits on the hole left at cachedIntKey, so a
    ** forward step means landing on the first live row above it. */
    rc = materializeDeferredMergedSeek(pCur);
    if( rc!=SQLITE_OK ) return rc;
    return pCur->eState==CURSOR_VALID ? SQLITE_OK : SQLITE_DONE;
  }
  rc = materializeDeferredTreeSeek(pCur, 1);
  if( rc!=SQLITE_OK ) return rc;
  if( pCur->mergeSrc==MERGE_SRC_TREE || pCur->mergeSrc==MERGE_SRC_BOTH ){
    rc = advanceTreeCursor(pCur, 1);
    if( rc!=SQLITE_OK ) return rc;
  }
  if( pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH )
    pCur->mmIdx++;
  rc = mergeScan(pCur, 1, 0);
  CLEAR_CACHED_PAYLOAD(pCur);
  return rc;
}

static int mergeStepBackward(BtCursor *pCur){
  int rc = SQLITE_OK;
  rc = cursorNormalizeMmPhys(pCur);
  if( rc!=SQLITE_OK ) return rc;
  if( pCur->deferredMergedSeek ){
    /* The cursor conceptually sits on the hole left at cachedIntKey. Seed
    ** both sides at the first entries >= the key so the normal backward
    ** step below retreats onto the last live row underneath it. */
    rc = deferredMergedSeekPosition(pCur);
    if( rc!=SQLITE_OK ) return rc;
    pCur->mergeSrc = MERGE_SRC_BOTH;
  }
  rc = materializeDeferredTreeSeek(pCur, -1);
  if( rc!=SQLITE_OK ) return rc;
  if( pCur->mergeSrc==MERGE_SRC_TREE || pCur->mergeSrc==MERGE_SRC_BOTH ){
    rc = advanceTreeCursor(pCur, -1);
    if( rc!=SQLITE_OK ) return rc;
  }
  if( pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH )
    pCur->mmIdx--;
  rc = mergeScan(pCur, -1, 0);
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

static int mergeFirst(BtCursor *pCur, int *pRes){
  int rc = ensureCursorMutMapOrder(pCur);
  if( rc!=SQLITE_OK ) return rc;
  pCur->mergeSrc = MERGE_SRC_TREE;
  pCur->mmIdx = 0;
  return mergeScan(pCur, 1, pRes);
}

static int mergeLast(BtCursor *pCur, int *pRes){
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

static int prollyBtCursorStepPrologue(BtCursor *pCur, int dir, int *pImmediate){
  int rc;
  *pImmediate = 1;
  CLEAR_CACHED_PAYLOAD(pCur);

  rc = prollyCursorCheckInterrupt(pCur);
  if( rc!=SQLITE_OK ) return rc;

  if( pCur->eState==CURSOR_INVALID ){
    return SQLITE_DONE;
  }

  /* Faulted mid-scan: cached nodes are already released, so return the stored
  ** error instead of advancing into freed memory. */
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

static void prollyCursorClassifyMergeSrc(BtCursor *pCur, int idx){
  if( idx >= 0 && idx < pCur->pMutMap->nEntries
   && prollyCursorIsValid(&pCur->pCur)
   && mergeCompare(pCur, prollyMutMapEntryAt(pCur->pMutMap, idx))==0 ){
    pCur->mergeSrc = MERGE_SRC_BOTH;
  }else if( !prollyCursorIsValid(&pCur->pCur) ){
    pCur->mergeSrc = MERGE_SRC_MUT;
  }else{
    pCur->mergeSrc = MERGE_SRC_TREE;
  }
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

/* Fold a raw tree-step rc into cursor state. The tree step signals end-of-data
** through eState, never by returning SQLITE_DONE, so returning SQLITE_DONE here
** unambiguously means "went invalid" and lets callers skip the trailing
** curFlags clear exactly as the open-coded versions did. */
static int prollyCursorFinishTreeStep(BtCursor *pCur, int rc){
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

  rc = prollyBtCursorStepPrologue(pCur, 1, &immediate);
  if( immediate ) return rc;

  if( !pCur->mmActive && pCur->pMutMap==0 ){
    rc = prollyCursorFinishTreeStep(pCur, prollyCursorNextFastLeaf(&pCur->pCur));
    if( rc==SQLITE_DONE ) return rc;
    pCur->curFlags &= ~(BTCF_AtLast|BTCF_ValidNKey|BTCF_DeleteKey);
    return rc;
  }

  if( pCur->mmActive ){
    rc = prollyCursorApplyMergeStep(pCur, 1);
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
    prollyCursorClassifyMergeSrc(pCur, it.idx);
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
  }else if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
    ProllyMutMapIter it;
    rc = ensureCursorMutMapOrder(pCur);
    if( rc!=SQLITE_OK ) return rc;
    if( pCur->curIntKey && prollyCursorIsValid(&pCur->pCur) ){
      rc = prollyMutMapIterSeek(&it, pCur->pMutMap, 0, 0,
                                prollyCursorIntKey(&pCur->pCur));
      if( rc!=SQLITE_OK ) return rc;
      if( it.idx >= pCur->pMutMap->nEntries
       || mergeCompare(pCur, orderedMutMapEntryAt(pCur->pMutMap, it.idx))>=0
       || orderedMutMapEntryAt(pCur->pMutMap, it.idx)->op==PROLLY_EDIT_DELETE
      ){
        it.idx--;
      }
    }else if( !pCur->curIntKey && prollyCursorIsValid(&pCur->pCur) ){
      const u8 *pK; int nK;
      prollyCursorKey(&pCur->pCur, &pK, &nK);
      rc = prollyMutMapIterSeek(&it, pCur->pMutMap, pK, nK, 0);
      if( rc!=SQLITE_OK ) return rc;
      if( it.idx >= pCur->pMutMap->nEntries
       || mergeCompare(pCur, orderedMutMapEntryAt(pCur->pMutMap, it.idx))>=0
       || orderedMutMapEntryAt(pCur->pMutMap, it.idx)->op==PROLLY_EDIT_DELETE
      ){
        it.idx--;
      }
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
    prollyCursorClassifyMergeSrc(pCur, it.idx);
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
int sqlite3BtreeEof(BtCursor *pCur){
  if( !pCur ) return 1;
  return pCur->pCurOps->xEof(pCur);
}

int prollyBtCursorIsEmpty(BtCursor *pCur, int *pRes){
  struct TableEntry *pTE;
  pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
  if( !pTE ){
    *pRes = 1;
  } else {
    *pRes = prollyHashIsEmpty(&pTE->root) ? 1 : 0;
  }
  return SQLITE_OK;
}
int sqlite3BtreeIsEmpty(BtCursor *pCur, int *pRes){
  if( !pCur ) { *pRes = 1; return SQLITE_OK; }
  return pCur->pCurOps->xIsEmpty(pCur, pRes);
}

int prollyBtCursorTableMoveto(
  BtCursor *pCur,
  i64 intKey,
  int bias,
  int *pRes
){
  int rc;
  struct TableEntry *pTE;
  int rootIsEmpty;
  int canUseAppendSeekFloor;
  (void)bias;

  assert( pCur->curIntKey );

  pCur->nSeek++;
  if( pCur->pBtree ) pCur->pBtree->nSeek++;
  clearMergeCursorState(pCur);
  CLEAR_CACHED_PAYLOAD(pCur);
  CLEAR_CACHED_SEEK_KEY(pCur);
  CLEAR_CACHED_COMPARE_KEY(pCur);

  pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
  if( pTE && pTE->pPending && pCur->pMutMap!=(ProllyMutMap*)pTE->pPending ){
    pCur->pMutMap = (ProllyMutMap*)pTE->pPending;
  }
  canUseAppendSeekFloor = pCur->pBtree
    && pCur->pBtree->db
    && !pCur->pBtree->db->autoCommit
    && pCur->pBtree->db->pSavepoint==0;
  rootIsEmpty = prollyHashIsEmpty(&pCur->pCur.root);
  if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
    ProllyMutMapEntry *pEntry = 0;
    if( rootIsEmpty
     && pCur->pMutMap->isIntKey
     && pCur->pMutMap->appendSorted
     && intKey > prollyMutMapEntryIntKey(
                  &pCur->pMutMap->aEntries[pCur->pMutMap->nEntries-1])
    ){
      goto table_moveto_deferred_miss;
    }
    if( canUseAppendSeekFloor
     && pTE
     && pTE->appendSeekFloorValid
     && prollyHashCompare(&pTE->root, &pTE->appendSeekRoot)==0
     && pCur->pMutMap->isIntKey
     && pCur->pMutMap->appendSorted
     && intKey >= pTE->appendSeekFloor
     && intKey > prollyMutMapEntryIntKey(
                  &pCur->pMutMap->aEntries[pCur->pMutMap->nEntries-1])
    ){
      goto table_moveto_deferred_miss;
    }
    rc = prollyMutMapFindRc(pCur->pMutMap, 0, 0, intKey, &pEntry);
    if( rc!=SQLITE_OK ) return rc;
    if( pEntry && pEntry->op==PROLLY_EDIT_INSERT ){
      if( pCur->isPinned ){
        return SQLITE_CONSTRAINT_PINNED;
      }
      *pRes = 0;
      setCursorToMutMapEntryPhys(pCur, (int)(pEntry - pCur->pMutMap->aEntries));
      pCur->deferredTreeSeek = 1;
      return SQLITE_OK;
    }

    /* intKey is delete-masked or absent from the mut map. Probe the tree
    ** for a live exact hit first so equality lookups stay on the fast
    ** path. */
    if( !pEntry ){
      refreshCursorRoot(pCur);
      if( !prollyHashIsEmpty(&pCur->pCur.root) ){
        int res = 0;
        rc = prollyCursorSeekInt(&pCur->pCur, intKey, &res);
        if( rc!=SQLITE_OK ) return rc;
        if( res==0 ){
          *pRes = 0;
          pCur->eState = CURSOR_VALID;
          pCur->curFlags |= BTCF_ValidNKey;
          pCur->cachedIntKey = intKey;
          CLEAR_CACHED_PAYLOAD(pCur);
          cacheCurrentTreePayloadIfIntKey(pCur);
          return SQLITE_OK;
        }
        if( res<0 && canUseAppendSeekFloor && pTE ){
          if( !pTE->appendSeekFloorValid
           || prollyHashCompare(&pTE->root, &pTE->appendSeekRoot)!=0
           || intKey < pTE->appendSeekFloor
          ){
            pTE->appendSeekFloor = intKey;
          }
          pTE->appendSeekRoot = pTE->root;
          pTE->appendSeekFloorValid = 1;
        }
      }
    }

    /* No live exact match. Report "not found" now but defer the merged
    ** repositioning (which materializes the mut-map order) until something
    ** actually consumes the cursor position: equality probes on write paths
    ** never do, while range seeks consume it immediately via Eof/Next/Prev.
    ** The raw tree position must not leak out of this path: it can sit on a
    ** delete-masked row, skip mut-map-only rows, or see an empty root when
    ** every live row is in the mut map.
    **
    ** The result must be -1, not +1: the VDBE treats res>0 as "the cursor
    ** sits on a row above the key" and reads it with no Eof check, which a
    ** deferred position cannot honor when nothing at or above the key
    ** survives. With res<0 every consumer either steps forward through the
    ** merge machinery or checks Eof before reading. */
table_moveto_deferred_miss:
    pCur->deferredMergedSeek = 1;
    pCur->mmActive = 1;
    pCur->cachedIntKey = intKey;
    pCur->curFlags &= ~BTCF_ValidNKey;
    pCur->eState = CURSOR_VALID;
    *pRes = -1;
    return SQLITE_OK;
  }

  if( rootIsEmpty ){
    refreshCursorRoot(pCur);
    rootIsEmpty = prollyHashIsEmpty(&pCur->pCur.root);
  }
  if( rootIsEmpty ){
    /* Empty tables return res<0 with an invalid cursor, matching stock. */
    *pRes = -1;
    pCur->eState = CURSOR_INVALID;
    return SQLITE_OK;
  }

  refreshCursorRoot(pCur);

  rc = prollyCursorSeekInt(&pCur->pCur, intKey, pRes);
  if( rc==SQLITE_OK ){
    if( *pRes==0 ){
      pCur->eState = CURSOR_VALID;
      pCur->curFlags |= BTCF_ValidNKey;
      pCur->cachedIntKey = intKey;
      CLEAR_CACHED_PAYLOAD(pCur);
      cacheCurrentTreePayloadIfIntKey(pCur);
    } else if( pCur->pCur.eState==PROLLY_CURSOR_VALID ){
      if( *pRes<0 && canUseAppendSeekFloor ){
        if( pTE ){
          if( !pTE->appendSeekFloorValid
           || prollyHashCompare(&pTE->root, &pTE->appendSeekRoot)!=0
           || intKey < pTE->appendSeekFloor
          ){
            pTE->appendSeekFloor = intKey;
          }
          pTE->appendSeekRoot = pTE->root;
          pTE->appendSeekFloorValid = 1;
        }
      }
      pCur->eState = CURSOR_VALID;
      pCur->curFlags &= ~BTCF_ValidNKey;
      cacheCurrentTreePayloadIfIntKey(pCur);
    } else {
      pCur->eState = CURSOR_INVALID;
    }
  }
  return rc;
}
int sqlite3BtreeTableMoveto(
  BtCursor *pCur,
  i64 intKey,
  int bias,
  int *pRes
){
  if( !pCur ) return SQLITE_OK;
  return pCur->pCurOps->xTableMoveto(pCur, intKey, bias, pRes);
}

u32 btreeSerialType(Mem *pMem, u32 *pLen){
  int flags = pMem->flags;
  if( flags & MEM_Null ){ *pLen = 0; return SERIAL_TYPE_NULL; }
  if( flags & MEM_Int ){
    i64 v = pMem->u.i;
    if( v==0 ){ *pLen = 0; return SERIAL_TYPE_ZERO; }
    if( v==1 ){ *pLen = 0; return SERIAL_TYPE_ONE; }
    if( v>=-128 && v<=127 ){ *pLen = 1; return SERIAL_TYPE_INT8; }
    if( v>=-32768 && v<=32767 ){ *pLen = 2; return SERIAL_TYPE_INT16; }
    if( v>=-8388608 && v<=8388607 ){ *pLen = 3; return SERIAL_TYPE_INT24; }
    if( v>=-2147483648LL && v<=2147483647LL ){ *pLen = 4; return SERIAL_TYPE_INT32; }
    if( v>=-140737488355328LL && v<=140737488355327LL ){ *pLen = 6; return SERIAL_TYPE_INT48; }
    *pLen = 8; return SERIAL_TYPE_INT64;
  }
  if( flags & MEM_Real ){ *pLen = 8; return SERIAL_TYPE_FLOAT64; }
  if( flags & MEM_Str ){
    u32 n = (u32)pMem->n;
    *pLen = n;
    return n*2 + SERIAL_TYPE_TEXT_BASE;
  }
  if( flags & MEM_Blob ){
    u32 n = (u32)pMem->n;
    *pLen = n;
    return n*2 + SERIAL_TYPE_BLOB_BASE;
  }
  *pLen = 0; return SERIAL_TYPE_NULL;
}

static int findMatchingMutMapEntry(
  ProllyMutMap *pMap,
  KeyInfo *pKeyInfo,
  UnpackedRecord *pIdxKey,
  const u8 *pSortKey,
  int nSortKey,
  int bExactKey,
  ProllyMutMapEntry **ppMatch,
  int *pCmp
){
  int rc = SQLITE_OK;
  int cmp = 0;
  ProllyMutMapEntry *pMatch = 0;
  u8 *pRecBuf = 0;
  int nRecBufAlloc = 0;
  int lo = 0, found = 0;

  *ppMatch = 0;
  *pCmp = 0;
  if( !pMap || prollyMutMapIsEmpty(pMap) ){
    return SQLITE_OK;
  }

  if( bExactKey
   || (pKeyInfo && pIdxKey->nField >= pKeyInfo->nAllField) ){
    ProllyMutMapEntry *pEntry = 0;
    rc = prollyMutMapFindRc(pMap, pSortKey, nSortKey, 0, &pEntry);
    if( rc!=SQLITE_OK ) return rc;
    if( pEntry && pEntry->op==PROLLY_EDIT_INSERT ){
      *ppMatch = pEntry;
    }
    return SQLITE_OK;
  }

  rc = prollyMutMapResolveSortedPos(pMap, pSortKey, nSortKey, 0,
                                    &lo, &found);

  while( rc==SQLITE_OK && lo < pMap->nEntries ){
    ProllyMutMapEntry *pEntry = prollyMutMapEntryAt(pMap, lo);
    const u8 *pRec = pEntry->pVal;
    int nRec = pEntry->nVal;
    int cmpLen;
    int prefixCmp;

    if( pMap->isIntKey ){
      lo++;
      continue;
    }
    cmpLen = pEntry->nKey < nSortKey ? pEntry->nKey : nSortKey;
    prefixCmp = memcmp(pEntry->pKey, pSortKey, cmpLen);
    if( prefixCmp>0 && pIdxKey->default_rc<0 ){
      if( pEntry->op==PROLLY_EDIT_INSERT ){
        pMatch = pEntry;
        cmp = 1;
      }else{
        lo++;
        continue;
      }
      break;
    }
    if( prefixCmp>0 ){
      break;
    }
    if( prefixCmp==0 && pEntry->nKey < nSortKey ){
      break;
    }
    if( nRec==0 ){
      rc = recordFromSortKeyBufferColl(pEntry->pKey, pEntry->nKey,
                                    pKeyInfo,
                                    &pRecBuf, &nRecBufAlloc, &nRec);
      if( rc!=SQLITE_OK ) break;
      pRec = pRecBuf;
    }
    pIdxKey->eqSeen = 0;
    cmp = sqlite3VdbeRecordCompare(nRec, pRec, pIdxKey);
    if( cmp!=0 && !pIdxKey->eqSeen ){
      break;
    }
    if( pEntry->op==PROLLY_EDIT_INSERT ){
      pMatch = pEntry;
      break;
    }
    lo++;
  }

  sqlite3_free(pRecBuf);
  if( rc==SQLITE_OK && pMatch ){
    *ppMatch = pMatch;
    *pCmp = cmp;
  }
  return rc;
}

static int scanMutMapForCustomCollation(
  BtCursor *pCur,
  ProllyMutMap *pMap,
  UnpackedRecord *pIdxKey,
  ProllyMutMapEntry **ppMatch,
  int *pCmp
){
  int i;
  int rc = SQLITE_OK;
  u8 *pRecBuf = 0;
  int nRecBufAlloc = 0;

  *ppMatch = 0;
  *pCmp = 0;
  if( !pMap || prollyMutMapIsEmpty(pMap) || pMap->isIntKey ){
    return SQLITE_OK;
  }

  for(i=0; i<pMap->nEntries; i++){
    ProllyMutMapEntry *pEntry = &pMap->aEntries[i];
    const u8 *pRec = pEntry->pVal;
    int nRec = pEntry->nVal;
    int cmp;

    if( pEntry->op!=PROLLY_EDIT_INSERT ){
      continue;
    }
    if( nRec==0 ){
      rc = recordFromSortKeyBufferColl(pEntry->pKey, pEntry->nKey,
                                    pCur->pKeyInfo,
                                    &pRecBuf, &nRecBufAlloc, &nRec);
      if( rc!=SQLITE_OK ) break;
      pRec = pRecBuf;
    }
    pIdxKey->eqSeen = 0;
    cmp = sqlite3VdbeRecordCompare(nRec, pRec, pIdxKey);
    if( cmp==0 || pIdxKey->eqSeen ){
      *ppMatch = pEntry;
      *pCmp = cmp;
      break;
    }
  }

  sqlite3_free(pRecBuf);
  return rc;
}

static int scanTreeForCustomCollation(
  BtCursor *pCur,
  UnpackedRecord *pIdxKey,
  int *pFound,
  int *pCmp
){
  int rc;
  int res = 0;
  u8 *pRecBuf = pCur->pMovetoRec;
  int nRecBufAlloc = pCur->nMovetoRecAlloc;

  *pFound = 0;
  *pCmp = 0;
  rc = prollyCursorFirst(&pCur->pCur, &res);
  while( rc==SQLITE_OK && res==0 && prollyCursorIsValid(&pCur->pCur) ){
    const u8 *pKey = 0;
    int nKey = 0;
    const u8 *pRec = 0;
    int nRec = 0;
    int cmp;
    int isDeleted = 0;

    rc = prollyCursorCheckInterrupt(pCur);
    if( rc!=SQLITE_OK ) break;
    prollyCursorKey(&pCur->pCur, &pKey, &nKey);
    if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
      ProllyMutMapEntry *pEntry = 0;
      rc = prollyMutMapFindRc(pCur->pMutMap, pKey, nKey, 0, &pEntry);
      if( rc!=SQLITE_OK ) break;
      if( pEntry && pEntry->op==PROLLY_EDIT_DELETE ) isDeleted = 1;
    }
    if( !isDeleted ){
      prollyCursorValue(&pCur->pCur, &pRec, &nRec);
      if( nRec==0 ){
        rc = recordFromSortKeyBufferColl(pKey, nKey, pCur->pKeyInfo,
                                         &pRecBuf, &nRecBufAlloc, &nRec);
        if( rc!=SQLITE_OK ) break;
        pRec = pRecBuf;
      }
      pIdxKey->eqSeen = 0;
      cmp = sqlite3VdbeRecordCompare(nRec, pRec, pIdxKey);
      if( cmp==0 || pIdxKey->eqSeen ){
        *pFound = 1;
        *pCmp = cmp;
        break;
      }
    }
    rc = prollyCursorNext(&pCur->pCur);
  }

  pCur->pMovetoRec = pRecBuf;
  pCur->nMovetoRecAlloc = nRecBufAlloc;
  return rc;
}

int prollyBtCursorIndexMoveto(
  BtCursor *pCur,
  UnpackedRecord *pIdxKey,
  int *pRes
){
  int rc;

  assert( !pCur->curIntKey );

  if( pCur->pBtree ) pCur->pBtree->nSeek++;

  clearMergeCursorState(pCur);
  CLEAR_CACHED_PAYLOAD(pCur);
  CLEAR_CACHED_SEEK_KEY(pCur);
  CLEAR_CACHED_COMPARE_KEY(pCur);

  refreshCursorRoot(pCur);

  {
    int treeFound = 0, mutFound = 0;
    int treeCmp = 0, mutCmp = 0;
    const u8 *mutKey = 0;
    int mutNKey = 0;
    ProllyMutMapEntry *mutE = 0;
    int mutFromCursorMap = 0;
    int exactMutMapKey = 0;

    u8 *pSerKey = 0;
    int nSerKey = 0;
    u8 *pSortKey = 0;
    int nSortKey = 0;
    int nSeekKeyField = 0;
    if( pCur->pKeyInfo && pIdxKey->nField < pCur->pKeyInfo->nAllField ){
      nSeekKeyField = (int)pIdxKey->nField;
    }
    /* Table-root range seeks must ignore probe fields beyond the PK. */
    if( pCur->isTableRoot && pCur->pKeyInfo
     && pIdxKey->default_rc != 0
     && pIdxKey->nField > pCur->pKeyInfo->nKeyField
     && (nSeekKeyField==0 || nSeekKeyField > pCur->pKeyInfo->nKeyField) ){
      nSeekKeyField = pCur->pKeyInfo->nKeyField;
    }
    if( unpackedRecordCanUseIntSortKey(
            pCur, pIdxKey,
            nSeekKeyField>0 ? nSeekKeyField : (int)pIdxKey->nField) ){
      rc = sortKeyFromUnpackedIntRecordBuffer(
          pIdxKey, nSeekKeyField>0 ? nSeekKeyField : (int)pIdxKey->nField,
          &pCur->pSeekSortKey, &pCur->nSeekSortKeyAlloc, &nSortKey);
    }else{
      rc = sortKeyFromMemPrefixCollBuffer(
          pIdxKey->aMem, (int)pIdxKey->nField, nSeekKeyField,
          pCur->pKeyInfo,
          &pCur->pSeekSortKey, &pCur->nSeekSortKeyAlloc, &nSortKey);
      if( rc==SQLITE_NOTFOUND ){
        rc = serializeUnpackedRecordBuffer(
            pIdxKey, &pCur->pSeekRecord, &pCur->nSeekRecordAlloc, &nSerKey);
        if( rc!=SQLITE_OK ) return rc;
        pSerKey = pCur->pSeekRecord;
        rc = sortKeyFromRecordPrefixCollBuffer(
            pSerKey, nSerKey, nSeekKeyField, pCur->pKeyInfo,
            &pCur->pSeekSortKey, &pCur->nSeekSortKeyAlloc, &nSortKey);
      }
    }
    if( rc!=SQLITE_OK ) return rc;
    pSortKey = pCur->pSeekSortKey;
    pCur->nSeekSortKey = nSortKey;
    pCur->nSeekKeyField = nSeekKeyField;

    if( pCur->pKeyInfo
     && nSeekKeyField == pCur->pKeyInfo->nKeyField ){
      if( pCur->isTableRoot ){
        exactMutMapKey = 1;
      }
    }

    if( pCur->pKeyInfo
     && (exactMutMapKey || pIdxKey->nField >= pCur->pKeyInfo->nAllField)
     && keyInfoHasUnsupportedCollation(
          pCur->pKeyInfo,
          nSeekKeyField>0 ? nSeekKeyField : (int)pIdxKey->nField) ){
      struct TableEntry *pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
      ProllyMutMap *pPending = pTE ? (ProllyMutMap*)pTE->pPending : 0;
      ProllyMutMapEntry *pEntry = 0;
      int cmp = 0;
      int treeFound = 0;

      rc = scanMutMapForCustomCollation(
          pCur, (ProllyMutMap*)pCur->pMutMap, pIdxKey, &pEntry, &cmp);
      if( rc!=SQLITE_OK ) return rc;
      if( pEntry ){
        setCursorToMutMapEntryPhys(
            pCur, (int)(pEntry - pCur->pMutMap->aEntries));
        *pRes = cmp;
        pIdxKey->eqSeen = 1;
        return SQLITE_OK;
      }

      if( pPending && pPending!=pCur->pMutMap ){
        rc = scanMutMapForCustomCollation(
            pCur, pPending, pIdxKey, &pEntry, &cmp);
        if( rc!=SQLITE_OK ) return rc;
        if( pEntry ){
          const u8 *pVal = pEntry->pVal;
          int nVal = pEntry->nVal;
          if( nVal==0 ){
            rc = cacheCursorPayloadReconstructed(
                pCur, pEntry->pKey, pEntry->nKey);
          }else{
            rc = cacheCursorPayloadCopy(pCur, pVal, nVal);
          }
          if( rc!=SQLITE_OK ) return rc;
          pCur->eState = CURSOR_VALID;
          *pRes = cmp;
          pIdxKey->eqSeen = 1;
          return SQLITE_OK;
        }
      }

      rc = scanTreeForCustomCollation(pCur, pIdxKey, &treeFound, &cmp);
      if( rc!=SQLITE_OK ) return rc;
      if( treeFound ){
        pCur->eState = CURSOR_VALID;
        cacheCurrentTreeStoredPayloadNonIntKey(pCur);
        *pRes = cmp;
        pIdxKey->eqSeen = 1;
        return SQLITE_OK;
      }
    }

    if( pCur->pKeyInfo
     && (exactMutMapKey || pIdxKey->nField >= pCur->pKeyInfo->nAllField) ){
      struct TableEntry *pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
      ProllyMutMap *pPending = pTE ? (ProllyMutMap*)pTE->pPending : 0;
      ProllyMutMapEntry *pEntry = 0;
      if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
        rc = prollyMutMapFindRc(pCur->pMutMap, pSortKey, nSortKey, 0, &pEntry);
        if( rc!=SQLITE_OK ) return rc;
        if( pEntry && pEntry->op==PROLLY_EDIT_INSERT ){
          if( pCur->isPinned ){
            return SQLITE_CONSTRAINT_PINNED;
          }
          setCursorToMutMapEntryPhys(
              pCur, (int)(pEntry - pCur->pMutMap->aEntries));
          /* The tree side is still wherever the last operation left it. A
          ** later step must re-seek it to this key before merging, or it
          ** feeds stale entries into the merge scan. */
          pCur->deferredTreeSeek = 1;
          *pRes = 0;
          pIdxKey->eqSeen = 1;
          return SQLITE_OK;
        }
      }
      if( pPending && pPending!=pCur->pMutMap
       && !prollyMutMapIsEmpty(pPending) ){
        rc = prollyMutMapFindRc(pPending, pSortKey, nSortKey, 0, &pEntry);
        if( rc!=SQLITE_OK ) return rc;
        if( pEntry && pEntry->op==PROLLY_EDIT_INSERT ){
          if( pCur->isPinned ){
            return SQLITE_CONSTRAINT_PINNED;
          }
          pCur->pMutMap = pPending;
          setCursorToMutMapEntryPhys(
              pCur, (int)(pEntry - pPending->aEntries));
          pCur->deferredTreeSeek = 1;
          *pRes = 0;
          pIdxKey->eqSeen = 1;
          return SQLITE_OK;
        }
      }
    }

    {
      int seekRes = 0;
      rc = prollyCursorSeekBlob(&pCur->pCur, pSortKey, nSortKey, &seekRes);
      if( rc==SQLITE_OK
       && seekRes==0
       && pCur->pCur.eState==PROLLY_CURSOR_VALID
       && pCur->pKeyInfo
       && (exactMutMapKey || pIdxKey->nField >= pCur->pKeyInfo->nAllField) ){
        int isDeleted = 0;
        if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
          ProllyMutMapEntry *mmE = 0;
          rc = prollyMutMapFindRc(pCur->pMutMap, pSortKey, nSortKey, 0, &mmE);
          if( rc!=SQLITE_OK ) return rc;
          if( mmE && mmE->op==PROLLY_EDIT_DELETE ) isDeleted = 1;
        }
        if( !isDeleted ){
          *pRes = 0;
          pIdxKey->eqSeen = 1;
          pCur->eState = CURSOR_VALID;
          cacheCurrentTreeStoredPayloadNonIntKey(pCur);
          return SQLITE_OK;
        }
      }
    }
    /* A failed tree seek must not fall through to the mut-map merge below:
    ** that path resets rc and reports "row not found" for what was really
    ** an I/O or allocation failure. */
    if( rc!=SQLITE_OK ) return rc;
    if( pCur->pCur.eState==PROLLY_CURSOR_VALID ){
      int iLevel = pCur->pCur.iLevel;
      ProllyCacheEntry *pLeaf = pCur->pCur.aLevel[iLevel].pEntry;
      int lo = pCur->pCur.aLevel[iLevel].idx;
      int nItems = pLeaf->node.nItems;

      int bestIdx = -1;
      int bestCmp = 0;
      {
        u8 *pRecBuf = pCur->pMovetoRec;
        int nRecBufAlloc = pCur->nMovetoRecAlloc;
        int i;

       for(;;){
        for( i = lo; i < nItems; i++ ){
          const u8 *pSK; int nSK;
          const u8 *pVal; int nVal;
          int recCmp;
          prollyNodeKey(&pLeaf->node, i, &pSK, &nSK);

          {
            int cmpLen = nSK < nSortKey ? nSK : nSortKey;
            int prefixCmp = memcmp(pSK, pSortKey, cmpLen);
            if( prefixCmp > 0 ){
              if( bestIdx < 0 ){
                if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
                  ProllyMutMapEntry *mmE = 0;
                  rc = prollyMutMapFindRc(pCur->pMutMap, pSK, nSK, 0, &mmE);
                  if( rc!=SQLITE_OK ) break;
                  if( mmE && mmE->op==PROLLY_EDIT_DELETE ){
                    /* Delete-masked row past the seek key: the next live row
                    ** is still the positioning answer, so keep scanning. */
                    continue;
                  }
                }
                bestIdx = i;
                if( nSeekKeyField>0 ){
                  pIdxKey->eqSeen = 0;
                  bestCmp = 1;
                }else{
                  const u8 *pVal2; int nVal2;
                  prollyNodeValue(&pLeaf->node, i, &pVal2, &nVal2);
                  if( nVal2==0 ){
                    rc = recordFromSortKeyBufferColl(
                        pSK, nSK, pCur->pKeyInfo,
                        &pRecBuf, &nRecBufAlloc, &nVal2);
                    if( rc!=SQLITE_OK ) break;
                    pVal2 = pRecBuf;
                  }
                  pIdxKey->eqSeen = 0;
                  bestCmp = sqlite3VdbeRecordCompare(nVal2, pVal2, pIdxKey);
                }
              }
              break;
            }
            if( nSeekKeyField>0 && prefixCmp==0 && nSK>=nSortKey ){
              if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
                ProllyMutMapEntry *mmE = 0;
                rc = prollyMutMapFindRc(pCur->pMutMap, pSK, nSK, 0, &mmE);
                if( rc!=SQLITE_OK ) break;
                if( mmE && mmE->op==PROLLY_EDIT_DELETE ){
                  continue;
                }
              }
              pIdxKey->eqSeen = 1;
              bestIdx = i;
              bestCmp = pIdxKey->default_rc;
              treeFound = 1;
              treeCmp = bestCmp;
              if( pIdxKey->default_rc < 0 ){
                continue;
              }
              break;
            }
          }
          prollyNodeValue(&pLeaf->node, i, &pVal, &nVal);
          if( nVal==0 ){
            rc = recordFromSortKeyBufferColl(
                pSK, nSK, pCur->pKeyInfo, &pRecBuf, &nRecBufAlloc, &nVal);
            if( rc!=SQLITE_OK ) break;
            pVal = pRecBuf;
          }
          pIdxKey->eqSeen = 0;
          recCmp = sqlite3VdbeRecordCompare(nVal, pVal, pIdxKey);

          if( recCmp==0 || pIdxKey->eqSeen ){
            if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
              ProllyMutMapEntry *mmE = 0;
              rc = prollyMutMapFindRc(pCur->pMutMap, pSK, nSK, 0, &mmE);
              if( rc!=SQLITE_OK ) break;
              if( mmE && mmE->op==PROLLY_EDIT_DELETE ){
                continue;
              }
            }
            bestIdx = i;
            bestCmp = recCmp;
            treeFound = 1;
            treeCmp = recCmp;
            break;
          } else if( recCmp > 0 ){
            if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
              ProllyMutMapEntry *mmE = 0;
              rc = prollyMutMapFindRc(pCur->pMutMap, pSK, nSK, 0, &mmE);
              if( rc!=SQLITE_OK ) break;
              if( mmE && mmE->op==PROLLY_EDIT_DELETE ){
                continue;
              }
            }
            if( bestIdx < 0 ){
              bestIdx = i;
              bestCmp = recCmp;
            }
          }
        }
        if( rc!=SQLITE_OK || treeFound || bestIdx>=0 ) break;
        if( !(pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap)) ) break;
        /* Everything from the seek point through the end of this leaf was
        ** delete-masked; the first live row may sit in a later leaf. */
        if( nItems>0 ) pCur->pCur.aLevel[pCur->pCur.iLevel].idx = nItems-1;
        rc = prollyCursorNext(&pCur->pCur);
        if( rc!=SQLITE_OK ) break;
        if( pCur->pCur.eState!=PROLLY_CURSOR_VALID ) break;
        iLevel = pCur->pCur.iLevel;
        pLeaf = pCur->pCur.aLevel[iLevel].pEntry;
        nItems = pLeaf->node.nItems;
        lo = 0;
       }
        pCur->pMovetoRec = pRecBuf;
        pCur->nMovetoRecAlloc = nRecBufAlloc;
        if( rc!=SQLITE_OK ) return rc;
      }

      if( treeFound ){

        pCur->pCur.aLevel[iLevel].idx = bestIdx;
      } else if( bestIdx >= 0 ){

        pCur->pCur.aLevel[iLevel].idx = bestIdx;
        treeCmp = bestCmp;
        treeFound = 1;
      }

    }

    {
      struct TableEntry *pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
      ProllyMutMap *pPending = pTE ? (ProllyMutMap*)pTE->pPending : 0;
      if( ((pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap))
         || (pPending && pPending!=pCur->pMutMap
             && !prollyMutMapIsEmpty(pPending)))
       && !(treeFound && treeCmp==0) ){
      int savedEqSeen = pIdxKey->eqSeen;
      rc = findMatchingMutMapEntry((ProllyMutMap*)pCur->pMutMap,
                                   pCur->pKeyInfo,
                                   pIdxKey, pSortKey, nSortKey,
                                   exactMutMapKey,
                                   &mutE, &mutCmp);
      if( rc!=SQLITE_OK ){
        return rc;
      }
      if( mutE ) mutFromCursorMap = 1;
      if( !mutE && pPending && pPending!=pCur->pMutMap ){
        rc = findMatchingMutMapEntry(pPending,
                                     pCur->pKeyInfo,
                                     pIdxKey, pSortKey, nSortKey,
                                     exactMutMapKey,
                                     &mutE, &mutCmp);
        if( rc!=SQLITE_OK ){
          return rc;
        }
      }
      if( mutE ){

        const u8 *pMutVal = mutE->pVal;
        int nMutVal = mutE->nVal;
        if( mutFromCursorMap && nMutVal==0 ){
          mutFound = 1;
        }else if( nMutVal==0 ){
          rc = recordFromSortKeyBufferColl(
              mutE->pKey, mutE->nKey, pCur->pKeyInfo,
              &pCur->pMovetoRec, &pCur->nMovetoRecAlloc, &nMutVal);
          if( rc!=SQLITE_OK ) return rc;
          pMutVal = pCur->pMovetoRec;
        }
        if( pMutVal ){
          mutKey = pMutVal;
          mutNKey = nMutVal;
          mutFound = 1;
        }
      }
      pIdxKey->eqSeen = savedEqSeen;
    }
    }

    if( mutFound ){
      int useMut = !treeFound;
      if( treeFound ){
        const u8 *pTreeKey = 0;
        int nTreeKey = 0;
        int nCmp;
        int cmp;
        prollyCursorKey(&pCur->pCur, &pTreeKey, &nTreeKey);
        nCmp = nTreeKey < mutE->nKey ? nTreeKey : mutE->nKey;
        cmp = memcmp(pTreeKey, mutE->pKey, nCmp);
        if( cmp>0 || (cmp==0 && nTreeKey>mutE->nKey) ){
          useMut = 1;
        }
      }
      if( useMut ){
        if( mutFromCursorMap ){
          setCursorToMutMapEntryPhys(
              pCur, (int)(mutE - pCur->pMutMap->aEntries));
          pCur->deferredTreeSeek = 1;
        }else{
          rc = cacheCursorPayloadCopy(pCur, mutKey, mutNKey);
          if( rc!=SQLITE_OK ){
            return rc;
          }
          pCur->eState = CURSOR_VALID;
        }
        *pRes = mutCmp;
        pIdxKey->eqSeen = 1;
        return SQLITE_OK;
      }
    }
    if( treeFound ){
      *pRes = treeCmp;
      /* A prefix seek can land on a tree row whose value was overwritten in
      ** this transaction (full-key seeks catch this in the exact-match fast
      ** path above). Serve the row from the mut map, or the caller reads the
      ** stale tree payload. */
      if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
        const u8 *pTreeKey = 0;
        int nTreeKey = 0;
        ProllyMutMapEntry *mmE = 0;
        prollyCursorKey(&pCur->pCur, &pTreeKey, &nTreeKey);
        rc = prollyMutMapFindRc(pCur->pMutMap, pTreeKey, nTreeKey, 0, &mmE);
        if( rc!=SQLITE_OK ) return rc;
        if( mmE && mmE->op==PROLLY_EDIT_INSERT ){
          setCursorToMutMapEntryPhys(
              pCur, (int)(mmE - pCur->pMutMap->aEntries));
          pCur->deferredTreeSeek = 1;
          return SQLITE_OK;
        }
      }
      pCur->eState = CURSOR_VALID;
      cacheCurrentTreeStoredPayloadNonIntKey(pCur);
      return SQLITE_OK;
    }
  }

  {
    int lastRes = 0;
    rc = prollyCursorLast(&pCur->pCur, &lastRes);
    if( rc!=SQLITE_OK ) return rc;
    if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
      /* No live row at or above the seek key on either side. Land on the
      ** merged last row so delete-masked tree rows are never exposed and
      ** mut-map-only rows are still reachable. */
      pCur->mmActive = 1;
      rc = mergeLast(pCur, &lastRes);
      if( rc!=SQLITE_OK ) return rc;
    }
    if( lastRes==0 ){
      pCur->eState = CURSOR_VALID;
      *pRes = -1;
      if( !pCur->mmActive || pCur->mergeSrc!=MERGE_SRC_MUT ){
        cacheCurrentTreeStoredPayloadNonIntKey(pCur);
      }
    } else {
      pCur->eState = CURSOR_INVALID;
      *pRes = -1;
    }
  }
  return SQLITE_OK;
}
int sqlite3BtreeIndexMoveto(
  BtCursor *pCur,
  UnpackedRecord *pIdxKey,
  int *pRes
){
  if( !pCur ) return SQLITE_OK;
  return pCur->pCurOps->xIndexMoveto(pCur, pIdxKey, pRes);
}

int cachedSeekKeyMatchesCurrent(BtCursor *pCur){
  const u8 *pKey = 0;
  int nKey = 0;

  if( !pCur || pCur->curIntKey
   || pCur->nSeekSortKey<=0 || pCur->nSeekKeyField!=0 ){
    return 0;
  }
  if( pCur->mmActive
   && (pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH) ){
    ProllyMutMapEntry *e = currentMutMapEntry(pCur);
    if( !e ) return 0;
    pKey = e->pKey;
    nKey = e->nKey;
  }else if( prollyCursorIsValid(&pCur->pCur) ){
    prollyCursorKey(&pCur->pCur, &pKey, &nKey);
  }
  return pKey && nKey==pCur->nSeekSortKey
      && memcmp(pKey, pCur->pSeekSortKey, nKey)==0;
}

int sqlite3BtreeProllyCachedIndexKeyCompare(
  BtCursor *pCur,
  UnpackedRecord *pIdxKey,
  int *pRes
){
  const u8 *pKey = 0;
  int nKey = 0;
  int nCmp;
  int cmp;

  if( !pCur || pCur->pCurOps!=&prollyCursorOps || pCur->curIntKey ){
    return SQLITE_NOTFOUND;
  }
  if( pCur->eState!=CURSOR_VALID || !pIdxKey || !pCur->pKeyInfo ){
    return SQLITE_NOTFOUND;
  }

  if( pCur->mmActive
   && (pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH) ){
    ProllyMutMapEntry *e = currentMutMapEntry(pCur);
    if( !e ) return SQLITE_NOTFOUND;
    pKey = e->pKey;
    nKey = e->nKey;
  }else if( prollyCursorIsValid(&pCur->pCur) ){
    prollyCursorKey(&pCur->pCur, &pKey, &nKey);
  }else{
    return SQLITE_NOTFOUND;
  }

  if( prollyBtCursorCursorHasHint(pCur, BTREE_SEEK_EQ)
   && pCur->nSeekSortKey>0
   && pCur->nSeekKeyField==(int)pIdxKey->nField ){
    nCmp = nKey < pCur->nSeekSortKey ? nKey : pCur->nSeekSortKey;
    cmp = memcmp(pKey, pCur->pSeekSortKey, nCmp);
    if( cmp<0 ){
      *pRes = -1;
    }else if( cmp>0 ){
      *pRes = 1;
    }else if( nKey < pCur->nSeekSortKey ){
      *pRes = -1;
    }else{
      pIdxKey->eqSeen = 1;
      *pRes = pIdxKey->default_rc;
    }
    return SQLITE_OK;
  }

  {
    int rc;
    if( pCur->nCompareSortKey<=0
     || pCur->nCompareKeyField!=(int)pIdxKey->nField ){
      rc = sortKeyFromUnpackedForCount(
          pCur, pIdxKey, &pCur->pCompareSortKey,
          &pCur->nCompareSortKeyAlloc, &pCur->nCompareSortKey);
      if( rc!=SQLITE_OK ){
        CLEAR_CACHED_COMPARE_KEY(pCur);
        return rc;
      }
      pCur->nCompareKeyField = (int)pIdxKey->nField;
    }
    nCmp = nKey < pCur->nCompareSortKey ? nKey : pCur->nCompareSortKey;
    cmp = memcmp(pKey, pCur->pCompareSortKey, nCmp);
    if( cmp<0 ){
      *pRes = -1;
    }else if( cmp>0 ){
      *pRes = 1;
    }else if( nKey < pCur->nCompareSortKey ){
      *pRes = -1;
    }else{
      pIdxKey->eqSeen = 1;
      *pRes = pIdxKey->default_rc;
    }
    return SQLITE_OK;
  }
}

/* Clear cached compare key before OP_SeekScan changes seek targets. */
void sqlite3BtreeProllyClearCompareKey(BtCursor *pCur){
  if( pCur ){
    CLEAR_CACHED_COMPARE_KEY(pCur);
  }
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

  if( pCur->mmActive
   && (pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH) ){
    ProllyMutMapEntry *e = currentMutMapEntry(pCur);
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
  lenRowid = prollySerialTypeLen(typeRowid);
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

/* In the amalgamation, testfixture also links test_btree.c which defines this
** same SQLITE_TEST-only debug helper; skip ours there to avoid a duplicate
** symbol (the non-amalgamation build never compiles this — libdoltlite.a is
** built without SQLITE_TEST). */
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
