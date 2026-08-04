#ifdef DOLTLITE_PROLLY

#include "prolly_btree_int.h"

void prollyBtreeCursorCurrentTreeValueSpan(
  BtCursor *pCur,
  const u8 **ppData,
  int *pnData,
  int *pnAvail
){
  ProllyCursor *pProllyCur = &pCur->pCur;
  ProllyCacheEntry *pLeaf;
  ProllyNode *pNode;
  int i;
  assert( pCur!=0 );
  assert( pProllyCur->eState==PROLLY_CURSOR_VALID );
  assert( pProllyCur->iLevel>=0 && pProllyCur->iLevel<PROLLY_CURSOR_MAX_DEPTH );
  pLeaf = pProllyCur->aLevel[pProllyCur->iLevel].pEntry;
  assert( pLeaf!=0 );
  pNode = &pLeaf->node;
  i = pProllyCur->aLevel[pProllyCur->iLevel].idx;
  assert( i>=0 && i<(int)pNode->nItems );
  prollyNodeValueSpan(pNode, i, ppData, pnData, pnAvail);
}

int prollyBtreeCursorCurrentTreeValueCopy(
  BtCursor *pCur,
  u32 offset,
  u32 amt,
  void *pBuf
){
  const u8 *pData;
  int nData;
  int nAvail;
  prollyBtreeCursorCurrentTreeValueSpan(pCur, &pData, &nData, &nAvail);
  if( (i64)offset + (i64)amt > (i64)nData ){
    return SQLITE_CORRUPT_BKPT;
  }
  if( amt>0 ){
    u32 nPrefix = offset < (u32)nAvail ? (u32)nAvail - offset : 0;
    if( nPrefix>amt ) nPrefix = amt;
    if( nPrefix>0 ) memcpy(pBuf, pData + offset, nPrefix);
    if( nPrefix<amt ) memset((u8*)pBuf + nPrefix, 0, amt - nPrefix);
  }
  return SQLITE_OK;
}

/* These accessors have no error channel, so a failed deferred-seek
** materialization has to be recorded on the cursor instead: fault it and stash
** the code in skipNext, the way prollyBtCursorEof() does, so the next cursor
** operation returns the real error. Returning a zero key or an empty payload
** without faulting reports a row that was never read -- an OOM or interrupt
** during positioning became wrong data under SQLITE_OK. */
i64 prollyBtCursorIntegerKey(BtCursor *pCur){
  if( pCur->deferredMergedSeek ){
    int rc = materializeDeferredMergedSeekBackward(pCur);
    if( rc!=SQLITE_OK ){
      pCur->eState = CURSOR_FAULT;
      pCur->skipNext = rc;
      return 0;
    }
    if( pCur->eState!=CURSOR_VALID ) return 0;
  }
  assert( pCur->eState==CURSOR_VALID );
  assert( pCur->curIntKey );

  if( pCur->mmActive
   && (pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH) ){
    ProllyMutMapEntry *e;
    int rc = currentMutMapEntry(pCur, &e);
    if( rc!=SQLITE_OK ){
      pCur->eState = CURSOR_FAULT;
      pCur->skipNext = rc;
      return 0;
    }
    return prollyMutMapEntryIntKey(e);
  }
  if( pCur->pCur.eState!=PROLLY_CURSOR_VALID
   && (pCur->curFlags & BTCF_ValidNKey) ){
    return pCur->cachedIntKey;
  }
  assert( pCur->pCur.eState==PROLLY_CURSOR_VALID );
  return cursorCurrentTreeIntKey(pCur);
}
i64 sqlite3BtreeIntegerKey(BtCursor *pCur){
  if( pCur->pCurOps==&prollyCursorOps ){
    return prollyBtCursorIntegerKey(pCur);
  }
  return pCur->pCurOps->xIntegerKey(pCur);
}

static int cursorPayloadFault(
  BtCursor *pCur,
  int rc,
  const u8 **ppData,
  int *pnData
){
  sqlite3 *db = pCur && pCur->pBtree ? pCur->pBtree->db : 0;
  /* PayloadSize and PayloadFetch may both reconstruct payloads. Surface OOM
  ** through db->mallocFailed so a size/fetch mismatch cannot masquerade as
  ** a malformed record. */
  if( rc==SQLITE_NOMEM && db ){
    sqlite3OomFault(db);
  }
  *ppData = 0;
  *pnData = 0;
  return rc;
}

int getCursorPayload(BtCursor *pCur, const u8 **ppData, int *pnData){
  *ppData = 0;
  *pnData = 0;
  assert( pCur!=0 );
  assert( pCur->eState!=CURSOR_FAULT );

  if( pCur->pCachedPayload && pCur->nCachedPayload > 0 ){
    *ppData = pCur->pCachedPayload;
    *pnData = pCur->nCachedPayload;
    return SQLITE_OK;
  }

  if( pCur->mmActive
   && (pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH) ){
    ProllyMutMapEntry *e;
    int rcEntry = currentMutMapEntry(pCur, &e);
    if( rcEntry!=SQLITE_OK ){
      return cursorPayloadFault(pCur, rcEntry, ppData, pnData);
    }
    if( pCur->curIntKey ){
      if( e->nZeroTail > 0 ){
        int rc = cacheCursorPayloadZeroTail(pCur, e->pVal, e->nVal,
                                            e->nZeroTail);
        if( rc!=SQLITE_OK ){
          return cursorPayloadFault(pCur, rc, ppData, pnData);
        }
        *ppData = pCur->pCachedPayload;
        *pnData = pCur->nCachedPayload;
        return SQLITE_OK;
      }
      pCur->pCachedPayload = e->pVal;
      pCur->nCachedPayload = e->nVal;
      pCur->cachedPayloadOwned = 0;
      *ppData = e->pVal;
      *pnData = e->nVal;
    }else{
      if( e->nVal > 0 && e->pVal ){
        pCur->pCachedPayload = e->pVal;
        pCur->nCachedPayload = e->nVal;
        pCur->cachedPayloadOwned = 0;
        *ppData = e->pVal;
        *pnData = e->nVal;
      }else{
        int rc = cacheCursorPayloadReconstructed(pCur, e->pKey, e->nKey);
        if( rc==SQLITE_OK ){
          *ppData = pCur->pCachedPayload;
          *pnData = pCur->nCachedPayload;
        }else{
          return cursorPayloadFault(pCur, rc, ppData, pnData);
        }
      }
    }
    return SQLITE_OK;
  }

  if( pCur->curIntKey ){
    int nAvail;
    prollyBtreeCursorCurrentTreeValueSpan(pCur, ppData, pnData, &nAvail);
    if( nAvail<*pnData ){
      int rc = cacheCursorPayloadZeroTail(pCur, *ppData, nAvail,
                                          (i64)*pnData - nAvail);
      if( rc!=SQLITE_OK ){
        return cursorPayloadFault(pCur, rc, ppData, pnData);
      }
      *ppData = pCur->pCachedPayload;
      *pnData = pCur->nCachedPayload;
    }else if( *pnData > 0 ){
      pCur->pCachedPayload = (u8*)*ppData;
      pCur->nCachedPayload = *pnData;
      pCur->cachedPayloadOwned = 0;
    }
  }else{

    const u8 *pVal; int nVal;
    cursorCurrentTreeValue(pCur, &pVal, &nVal);
    if( nVal > 0 ){
      pCur->pCachedPayload = (u8*)pVal;
      pCur->nCachedPayload = nVal;
      pCur->cachedPayloadOwned = 0;
      *ppData = pVal;
      *pnData = nVal;
    }else{
      const u8 *pKey; int nKey; int rc;
      prollyCursorKey(&pCur->pCur, &pKey, &nKey);
      rc = cacheCursorPayloadReconstructed(pCur, pKey, nKey);
      if( rc==SQLITE_OK ){
        *ppData = pCur->pCachedPayload;
        *pnData = pCur->nCachedPayload;
      }else{
        return cursorPayloadFault(pCur, rc, ppData, pnData);
      }
    }
  }
  return SQLITE_OK;
}

u32 prollyBtCursorPayloadSize(BtCursor *pCur){
  const u8 *pData;
  int nData;
  if( pCur->deferredMergedSeek ){
    int rc = materializeDeferredMergedSeekBackward(pCur);
    if( rc!=SQLITE_OK ){
      pCur->eState = CURSOR_FAULT;
      pCur->skipNext = rc;
      return 0;
    }
    if( pCur->eState!=CURSOR_VALID ) return 0;
  }
  assert( pCur->eState==CURSOR_VALID );
  if( pCur->pCachedPayload && pCur->nCachedPayload > 0 ){
    return (u32)pCur->nCachedPayload;
  }
  if( pCur->curIntKey && pCur->mmActive
   && (pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH) ){
    ProllyMutMapEntry *e;
    int rc = currentMutMapEntry(pCur, &e);
    if( rc!=SQLITE_OK ){
      pCur->eState = CURSOR_FAULT;
      pCur->skipNext = rc;
      return 0;
    }
    /* Answer from the pending row before consulting the tree. The tree cursor
    ** may still be valid for MERGE_SRC_BOTH, but its value is stale. */
    return (u32)((i64)e->nVal + e->nZeroTail);
  }
  if( pCur->curIntKey && pCur->pCur.eState==PROLLY_CURSOR_VALID ){
    const u8 *pVal;
    int nVal;
    int nAvail;
    prollyBtreeCursorCurrentTreeValueSpan(pCur, &pVal, &nVal, &nAvail);
    if( nAvail<nVal ){
      return (u32)nVal;
    }
  }
  if( getCursorPayload(pCur, &pData, &nData)!=SQLITE_OK ){
    return 0;
  }
  return (u32)nData;
}
u32 sqlite3BtreePayloadSize(BtCursor *pCur){
  if( pCur->pCurOps==&prollyCursorOps ){
    return prollyBtCursorPayloadSize(pCur);
  }
  return pCur->pCurOps->xPayloadSize(pCur);
}

int copyZeroTailPayload(
  ProllyMutMapEntry *e,
  u32 offset,
  u32 amt,
  void *pBuf
){
  i64 nTotal = (i64)e->nVal + e->nZeroTail;
  if( (i64)offset + (i64)amt > nTotal ){
    return SQLITE_CORRUPT_BKPT;
  }
  if( amt>0 ){
    u32 nPrefix = offset < (u32)e->nVal ? (u32)e->nVal - offset : 0;
    if( nPrefix>amt ) nPrefix = amt;
    if( nPrefix>0 ) memcpy(pBuf, e->pVal + offset, nPrefix);
    if( nPrefix<amt ) memset((u8*)pBuf + nPrefix, 0, amt - nPrefix);
  }
  return SQLITE_OK;
}

int prollyBtCursorPayload(BtCursor *pCur, u32 offset, u32 amt, void *pBuf){
  const u8 *pData;
  int nData;
  int rc;

  if( pCur->deferredMergedSeek ){
    rc = materializeDeferredMergedSeekBackward(pCur);
    if( rc!=SQLITE_OK ) return rc;
    if( pCur->eState!=CURSOR_VALID ) return SQLITE_ABORT;
  }
  assert( pCur->eState==CURSOR_VALID );
  if( pCur->curIntKey
   && pCur->mmActive
   && (pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH) ){
    ProllyMutMapEntry *e;
    rc = currentMutMapEntry(pCur, &e);
    if( rc!=SQLITE_OK ) return rc;
    if( e && e->nZeroTail>0 ){
      return copyZeroTailPayload(e, offset, amt, pBuf);
    }
    if( e ){
      if( (i64)offset + (i64)amt > (i64)e->nVal ){
        return SQLITE_CORRUPT_BKPT;
      }
      memcpy(pBuf, e->pVal + offset, amt);
      return SQLITE_OK;
    }
  }
  if( pCur->curIntKey && pCur->pCur.eState==PROLLY_CURSOR_VALID ){
    const u8 *pVal;
    int nVal;
    int nAvail;
    prollyBtreeCursorCurrentTreeValueSpan(pCur, &pVal, &nVal, &nAvail);
    if( nAvail<nVal ){
      return prollyBtreeCursorCurrentTreeValueCopy(pCur, offset, amt, pBuf);
    }
  }
  rc = getCursorPayload(pCur, &pData, &nData);
  if( rc!=SQLITE_OK ){
    return rc;
  }

  if( (i64)offset + (i64)amt > (i64)nData ){
    return SQLITE_CORRUPT_BKPT;
  }

  memcpy(pBuf, pData + offset, amt);
  return SQLITE_OK;
}
int sqlite3BtreePayload(BtCursor *pCur, u32 offset, u32 amt, void *pBuf){
  if( !pCur ) return SQLITE_OK;
  return pCur->pCurOps->xPayload(pCur, offset, amt, pBuf);
}

const void *prollyBtCursorPayloadFetch(BtCursor *pCur, u32 *pAmt){
  const u8 *pData;
  int nData;

  if( pCur->deferredMergedSeek ){
    int rc = materializeDeferredMergedSeekBackward(pCur);
    if( rc!=SQLITE_OK ){
      pCur->eState = CURSOR_FAULT;
      pCur->skipNext = rc;
      if( pAmt ) *pAmt = 0;
      return 0;
    }
    if( pCur->eState!=CURSOR_VALID ){
      if( pAmt ) *pAmt = 0;
      return 0;
    }
  }
  assert( pCur->eState==CURSOR_VALID );
  if( pCur->pCachedPayload && pCur->nCachedPayload > 0 ){
    if( pAmt ) *pAmt = (u32)pCur->nCachedPayload;
    return (const void*)pCur->pCachedPayload;
  }
  if( pCur->curIntKey
   && pCur->mmActive
   && (pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH) ){
    ProllyMutMapEntry *e;
    int rc = currentMutMapEntry(pCur, &e);
    if( rc!=SQLITE_OK ){
      pCur->eState = CURSOR_FAULT;
      pCur->skipNext = rc;
      return 0;
    }
    if( e && e->nZeroTail>0 && e->nVal>0 ){
      if( pAmt ) *pAmt = (u32)e->nVal;
      return (const void*)e->pVal;
    }
    if( e && e->nVal>0 ){
      if( pAmt ) *pAmt = (u32)e->nVal;
      return (const void*)e->pVal;
    }
  }
  if( pCur->curIntKey && pCur->pCur.eState==PROLLY_CURSOR_VALID ){
    int nLogical;
    int nAvail;
    prollyBtreeCursorCurrentTreeValueSpan(pCur, &pData, &nLogical, &nAvail);
    if( nAvail<nLogical ){
      if( pAmt ) *pAmt = (u32)nAvail;
      return (const void*)pData;
    }
  }
  if( getCursorPayload(pCur, &pData, &nData)!=SQLITE_OK ){
    if( pAmt ) *pAmt = 0;
    return 0;
  }

  if( pAmt ) *pAmt = (u32)nData;
  return (const void*)pData;
}
const void *sqlite3BtreePayloadFetch(BtCursor *pCur, u32 *pAmt){
  if( !pCur ) return 0;
  if( pCur->pCurOps==&prollyCursorOps ){
    return prollyBtCursorPayloadFetch(pCur, pAmt);
  }
  return pCur->pCurOps->xPayloadFetch(pCur, pAmt);
}

sqlite3_int64 prollyBtCursorMaxRecordSize(BtCursor *pCur){
  (void)pCur;
  return PROLLY_MAX_RECORD_SIZE;
}
sqlite3_int64 sqlite3BtreeMaxRecordSize(BtCursor *pCur){
  return pCur->pCurOps->xMaxRecordSize(pCur);
}

i64 prollyBtCursorOffset(BtCursor *pCur){
  (void)pCur;
  return 0;
}
i64 sqlite3BtreeOffset(BtCursor *pCur){
  return pCur->pCurOps->xOffset(pCur);
}

#endif
