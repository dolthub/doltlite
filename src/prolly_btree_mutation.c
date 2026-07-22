#ifdef DOLTLITE_PROLLY

#include "prolly_btree_int.h"

/* Mutation maps, inserts, deletes, transfers, and incremental blobs. */

static int keyInfoHasLossyCollation(const KeyInfo *pKeyInfo){
  int i;
  if( !pKeyInfo ) return 0;
  for(i=0; i<pKeyInfo->nAllField; i++){
    const CollSeq *pColl = pKeyInfo->aColl[i];
    if( !pColl || !pColl->zName ) continue;
    if( sqlite3StrICmp(pColl->zName, "NOCASE")==0
     || sqlite3StrICmp(pColl->zName, "RTRIM")==0 ){
      return 1;
    }
  }
  return 0;
}

int applyMutMapToTableRoot(
  BtShared *pBt,
  struct TableEntry *pTE,
  ProllyMutMap *pMap
){
  ProllyMutator mut;
  int rc;

  assert( pBt!=0 && pTE!=0 && pMap!=0 );
  memset(&mut, 0, sizeof(mut));
  mut.pStore = &pBt->store;
  mut.pCache = &pBt->cache;
  mut.oldRoot = pTE->root;
  mut.pEdits = pMap;
  mut.flags = pTE->flags;

  rc = prollyMutateFlush(&mut);
  if( rc!=SQLITE_OK ) return rc;

  pTE->root = mut.newRoot;
  return SQLITE_OK;
}

int cacheCursorPayloadCopy(BtCursor *pCur, const u8 *pData, int nData){
  u8 *pCopy = 0;
  if( nData > 0 ){
    pCopy = sqlite3_malloc(nData);
    if( !pCopy ) return SQLITE_NOMEM;
    memcpy(pCopy, pData, nData);
  }
  CLEAR_CACHED_PAYLOAD(pCur);
  pCur->pCachedPayload = pCopy;
  pCur->nCachedPayload = nData;
  pCur->cachedPayloadOwned = 1;
  return SQLITE_OK;
}

/* Cache a payload stored as a prefix plus a symbolic zero tail,
** materializing the zeros. */
int cacheCursorPayloadZeroTail(BtCursor *pCur, const u8 *pData,
                               int nData, i64 nZeroTail){
  i64 nTotal64 = (i64)nData + nZeroTail;
  int nTotal;
  u8 *pCopy;
  if( nTotal64 > 0x7fffffff ) return SQLITE_TOOBIG;
  nTotal = (int)nTotal64;
  if( nTotal<=0 ) return cacheCursorPayloadCopy(pCur, pData, nData);
  pCopy = sqlite3_malloc(nTotal);
  if( !pCopy ) return SQLITE_NOMEM;
  if( nData > 0 ){
    memcpy(pCopy, pData, nData);
  }
  memset(pCopy + nData, 0, (size_t)(nTotal - nData));
  CLEAR_CACHED_PAYLOAD(pCur);
  pCur->pCachedPayload = pCopy;
  pCur->nCachedPayload = nTotal;
  pCur->cachedPayloadOwned = 1;
  return SQLITE_OK;
}

int cacheCursorPayloadReconstructed(
  BtCursor *pCur, const u8 *pSortKey, int nSortKey
){
  int nRec = 0;
  int rc = recordFromSortKeyBufferColl(
      pSortKey, nSortKey, pCur->pKeyInfo,
      &pCur->pReconPayload, &pCur->nReconPayloadAlloc, &nRec);
  if( rc!=SQLITE_OK ) return rc;
  CLEAR_CACHED_PAYLOAD(pCur);
  pCur->pCachedPayload = pCur->pReconPayload;
  pCur->nCachedPayload = nRec;
  pCur->cachedPayloadOwned = 0;
  return SQLITE_OK;
}

int serializeUnpackedRecordBuffer(
  UnpackedRecord *pRec, u8 **ppBuf, int *pnAlloc, int *pnOut
){
  int nField = pRec->nField;
  Mem *aMem = pRec->aMem;
  i64 nData = 0;
  u32 aType[MAX_RECORD_FIELDS];
  u32 aLen[MAX_RECORD_FIELDS];
  int i;
  u8 *pOut;
  int nHdr, nTotal;
  i64 nTotal64;

  if( nField > MAX_RECORD_FIELDS ) nField = MAX_RECORD_FIELDS;

  for(i=0; i<nField; i++){
    aType[i] = btreeSerialType(&aMem[i], &aLen[i]);
    nData += aLen[i];
  }

  nHdr = 1;
  for(i=0; i<nField; i++) nHdr += sqlite3VarintLen(aType[i]);
  if( nHdr > MAX_ONEBYTE_HEADER ) nHdr++;

  nTotal64 = (i64)nHdr + nData;
  if( nTotal64 > 0x7fffffff ) return SQLITE_TOOBIG;
  nTotal = (int)nTotal64;
  if( *pnAlloc < nTotal ){
    u8 *pNew = (u8*)sqlite3_realloc(*ppBuf, nTotal);
    if( !pNew ) return SQLITE_NOMEM;
    *ppBuf = pNew;
    *pnAlloc = nTotal;
  }
  pOut = *ppBuf;

  {
    int off = putVarint32(pOut, (u32)nHdr);
    for(i=0; i<nField; i++){
      off += putVarint32(pOut + off, aType[i]);
    }
  }

  {
    u32 off = (u32)nHdr;
    for(i=0; i<nField; i++){
      Mem *p = &aMem[i];
      u32 st = aType[i];
      if( st==SERIAL_TYPE_NULL || st==SERIAL_TYPE_ZERO || st==SERIAL_TYPE_ONE ){
      }else if( st<=SERIAL_TYPE_INT64 ){
        i64 v = p->u.i;
        int nByte = (int)aLen[i];
        int j;
        for(j=nByte-1; j>=0; j--){
          pOut[off+j] = (u8)(v & 0xFF);
          v >>= 8;
        }
        off += nByte;
      }else if( st==SERIAL_TYPE_FLOAT64 ){
        u64 floatBits;
        int j;
        memcpy(&floatBits, &p->u.r, 8);
        for(j=7; j>=0; j--){
          pOut[off+j] = (u8)(floatBits & 0xFF);
          floatBits >>= 8;
        }
        off += 8;
      }else{
        int nByte = (int)aLen[i];
        if( nByte > 0 && p->z ) memcpy(pOut + off, p->z, nByte);
        off += nByte;
      }
    }
  }

  *pnOut = nTotal;
  return SQLITE_OK;
}

int unpackedRecordCanUseIntSortKey(
  BtCursor *pCur,
  UnpackedRecord *pRec,
  int nField
){
  KeyInfo *pKeyInfo = pCur->pKeyInfo;
  int i;
  if( !pKeyInfo || !pRec || nField<=0 || pRec->nField<nField ) return 0;
  if( nField > pKeyInfo->nAllField ) return 0;
  for(i=0; i<nField; i++){
    CollSeq *pColl;
    if( !(pRec->aMem[i].flags & MEM_Int) ) return 0;
    if( pKeyInfo->aSortFlags && (pKeyInfo->aSortFlags[i] & KEYINFO_ORDER_DESC) ){
      return 0;
    }
    pColl = pKeyInfo->aColl[i];
    if( pColl && pColl->zName && sqlite3StrICmp(pColl->zName, "BINARY")!=0 ){
      return 0;
    }
  }
  return 1;
}

static int prollyGetVarint32(const u8 *p, u32 *pVal){
  u32 a = *p;
  if( !(a & 0x80) ){
    *pVal = a;
    return 1;
  }
  {
    u32 v = a & 0x7f;
    int i = 1;
    do {
      a = p[i];
      v = (v << 7) | (a & 0x7f);
      i++;
    } while( (a & 0x80) && i < 9 );
    *pVal = v;
    return i;
  }
}

u32 prollySerialTypeLen(u32 serialType){
  static const u8 aLen[] = {0, 1, 2, 3, 4, 6, 8};
  if( serialType <= 6 ) return aLen[serialType];
  if( serialType == 7 ) return 8;
  if( serialType >= 12 ) return (serialType - 12) / 2;
  return 0;
}

static int sortKeyFromIntRecordLocal(
  BtCursor *pCur,
  const u8 *pRec,
  int nRec,
  int nKeyField,
  u8 *pOut,
  int nOutCap,
  int *pnOut
){
  u32 hdrSize;
  u32 hdrOff;
  u32 dataOff;
  int nField = 0;
  int outPos = 0;

  *pnOut = 0;
  if( !pRec || nRec<=0 ) return SQLITE_NOTFOUND;
  hdrOff = prollyGetVarint32(pRec, &hdrSize);
  if( hdrSize > (u32)nRec ) return SQLITE_CORRUPT;
  dataOff = hdrSize;

  while( hdrOff < hdrSize ){
    u32 serialType;
    u32 fieldLen;
    i64 v;
    int n = 0;
    int rc;
    if( nKeyField>0 && nField>=nKeyField ) break;
    if( pCur->pKeyInfo && nField>=pCur->pKeyInfo->nAllField ){
      return SQLITE_NOTFOUND;
    }
    if( pCur->pKeyInfo
     && pCur->pKeyInfo->aSortFlags
     && (pCur->pKeyInfo->aSortFlags[nField] & KEYINFO_ORDER_DESC) ){
      return SQLITE_NOTFOUND;
    }
    hdrOff += prollyGetVarint32(pRec + hdrOff, &serialType);
    fieldLen = prollySerialTypeLen(serialType);
    if( fieldLen > (u32)nRec - dataOff ) return SQLITE_CORRUPT;
    if( serialType==8 ){
      v = 0;
    }else if( serialType==9 ){
      v = 1;
    }else if( serialType>=1 && serialType<=6 ){
      u64 uv = (pRec[dataOff] & 0x80) ? (u64)-1 : 0;
      u32 i;
      for(i=0; i<fieldLen; i++){
        uv = (uv << 8) | pRec[dataOff + i];
      }
      v = (i64)uv;
    }else{
      return SQLITE_NOTFOUND;
    }
    if( outPos + 18 > nOutCap ) return SQLITE_NOTFOUND;
    rc = sortKeyFromInt64(v, pOut + outPos, &n);
    if( rc!=SQLITE_OK ) return rc;
    outPos += n;
    dataOff += fieldLen;
    nField++;
  }
  if( nField<=0 || (nKeyField>0 && nField<nKeyField) ){
    return SQLITE_NOTFOUND;
  }
  *pnOut = outPos;
  return SQLITE_OK;
}

int sortKeyFromUnpackedIntRecordBuffer(
  UnpackedRecord *pRec,
  int nField,
  u8 **ppBuf,
  int *pnAlloc,
  int *pnOut
){
  int i;
  int nOut = 0;
  int nAlloc = nField * 18;
  if( *pnAlloc < nAlloc ){
    u8 *pNew = (u8*)sqlite3_realloc64(*ppBuf, (sqlite3_uint64)nAlloc);
    if( !pNew ) return SQLITE_NOMEM;
    *ppBuf = pNew;
    *pnAlloc = nAlloc;
  }
  for(i=0; i<nField; i++){
    i64 v = pRec->aMem[i].u.i;
    int n = 0;
    if( sortKeyInt64FitsExact(v) ){
      sortKeyWriteExactInt64(v, *ppBuf + nOut);
      nOut += 9;
    }else{
      int rc = sortKeyFromInt64(v, *ppBuf + nOut, &n);
      if( rc!=SQLITE_OK ) return rc;
      nOut += n;
    }
  }
  *pnOut = nOut;
  return SQLITE_OK;
}
void clearMergeCursorState(BtCursor *pCur){
  pCur->mmIdx = -1;
  pCur->mmPhysIdx = -1;
  pCur->mmActive = 0;
  pCur->mmPhysActive = 0;
  pCur->deferredTreeSeek = 0;
  pCur->deferredMergedSeek = 0;
  pCur->mergeSrc = MERGE_SRC_TREE;
}

ProllyMutMapEntry *currentMutMapEntry(BtCursor *pCur){
  assert( pCur!=0 );
  assert( pCur->mmActive );
  assert( pCur->pMutMap!=0 );
  if( pCur->mmPhysActive ){
    assert( pCur->mmPhysIdx>=0 && pCur->mmPhysIdx<pCur->pMutMap->nEntries );
    return &pCur->pMutMap->aEntries[pCur->mmPhysIdx];
  }
  assert( pCur->mmIdx>=0 );
  return prollyMutMapEntryAt(pCur->pMutMap, pCur->mmIdx);
}

SQLITE_INLINE ProllyMutMapEntry *orderedMutMapEntryAt(
  ProllyMutMap *pMap,
  int idx
){
  if( pMap->keepSorted || !pMap->orderDirty ){
    return &pMap->aEntries[pMap->aOrder[idx]];
  }
  return prollyMutMapEntryAt(pMap, idx);
}

void setCursorToMutMapEntryPhys(BtCursor *pCur, int physIdx){
  ProllyMutMapEntry *pEntry;
  assert( pCur!=0 && pCur->pMutMap!=0 );
  assert( physIdx>=0 && physIdx<pCur->pMutMap->nEntries );
  pEntry = &pCur->pMutMap->aEntries[physIdx];
  CLEAR_CACHED_PAYLOAD(pCur);
  pCur->mmIdx = -1;
  pCur->mmPhysIdx = physIdx;
  pCur->mmActive = 1;
  pCur->mmPhysActive = 1;
  pCur->deferredTreeSeek = 0;
  pCur->mergeSrc = MERGE_SRC_MUT;
  pCur->eState = CURSOR_VALID;
  pCur->curFlags &= ~BTCF_AtLast;
  if( pCur->curIntKey ){
    pCur->cachedIntKey = prollyMutMapEntryIntKey(pEntry);
    pCur->curFlags |= BTCF_ValidNKey;
  }else{
    pCur->curFlags &= ~BTCF_ValidNKey;
  }
}

static void setCursorToMutMapMissingEntryPhys(BtCursor *pCur, int physIdx){
  ProllyMutMapEntry *pEntry;
  assert( pCur!=0 && pCur->pMutMap!=0 );
  assert( physIdx>=0 && physIdx<pCur->pMutMap->nEntries );
  pEntry = &pCur->pMutMap->aEntries[physIdx];
  CLEAR_CACHED_PAYLOAD(pCur);
  pCur->mmIdx = -1;
  pCur->mmPhysIdx = physIdx;
  pCur->mmActive = 1;
  pCur->mmPhysActive = 1;
  pCur->deferredTreeSeek = 0;
  pCur->mergeSrc = MERGE_SRC_MUT;
  pCur->eState = CURSOR_INVALID;
  pCur->curFlags &= ~BTCF_AtLast;
  if( pCur->curIntKey ){
    pCur->cachedIntKey = prollyMutMapEntryIntKey(pEntry);
    pCur->curFlags |= BTCF_ValidNKey;
  }else{
    pCur->curFlags &= ~BTCF_ValidNKey;
  }
}

int prollyCursorCheckInterrupt(BtCursor *pCur){
  sqlite3 *db = pCur && pCur->pBtree ? pCur->pBtree->db : 0;
  if( db && AtomicLoad(&db->u1.isInterrupted) ){
    return SQLITE_INTERRUPT;
  }
  return SQLITE_OK;
}

int advanceTreeCursor(BtCursor *pCur, int dir){
  int rc = prollyCursorCheckInterrupt(pCur);
  if( rc!=SQLITE_OK ) return rc;
  if( dir>0 ){
    return prollyCursorNext(&pCur->pCur);
  }else{
    return prollyCursorPrev(&pCur->pCur);
  }
}

int flushMutMap(BtCursor *pCur){
  struct TableEntry *pTE;
  ProllyMutMap *pFlushMap;
  int captured;
  int rc;

  assert( pCur!=0 && pCur->pBtree!=0 && pCur->pBt!=0 );
  assert( pCur->pBtree->inTrans==TRANS_WRITE );
  pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
  if( !pTE ){
    return SQLITE_INTERNAL;
  }
  if( !pTE->pPending || prollyMutMapIsEmpty((ProllyMutMap*)pTE->pPending) ){
    return SQLITE_OK;
  }

  pFlushMap = (ProllyMutMap*)pTE->pPending;
  captured = 0;
  rc = snapshotPendingForFlush(pCur->pBtree, pCur->pgnoRoot,
                               (ProllyMutMap**)&pTE->pPending,
                               &pFlushMap, &captured);
  if( rc!=SQLITE_OK ) return rc;
  if( captured ){
    refreshCursorMutMapAliases(pCur->pBtree, pCur->pBt, pCur->pgnoRoot,
                               (ProllyMutMap*)pTE->pPending);
  }
  rc = applyMutMapToTableRoot(pCur->pBt, pTE, pFlushMap);
  if( rc!=SQLITE_OK ) return rc;
  pCur->pCur.root = pTE->root;
  if( captured ){
    pCur->flushSeekEdits = 0;
  }else{
    prollyMutMapClear((ProllyMutMap*)pTE->pPending);
  }

  return SQLITE_OK;
}

int flushPendingForTable(
  Btree *pBtree,
  BtShared *pBt,
  struct TableEntry *pTE,
  int clearInPlace
){
  ProllyMutMap *pMap;
  ProllyMutMap *pFlushMap;
  int captured = 0;
  int rc;

  if( !pTE || !pTE->pPending ) return SQLITE_OK;
  pMap = (ProllyMutMap*)pTE->pPending;
  if( prollyMutMapIsEmpty(pMap) ) return SQLITE_OK;

  pFlushMap = pMap;
  rc = snapshotPendingForFlush(pBtree, pTE->iTable,
                               (ProllyMutMap**)&pTE->pPending,
                               &pFlushMap, &captured);
  if( rc!=SQLITE_OK ) return rc;
  if( captured ){
    refreshCursorMutMapAliases(pBtree, pBt, pTE->iTable,
                               (ProllyMutMap*)pTE->pPending);
  }

  rc = applyMutMapToTableRoot(pBt, pTE, pFlushMap);
  if( rc!=SQLITE_OK ) return rc;

  if( pTE->pPending==pMap ){
    if( clearInPlace ){
      prollyMutMapClear(pMap);
    }else{
      prollyMutMapFree(pMap);
      sqlite3_free(pMap);
      pTE->pPending = 0;
      refreshCursorMutMapAliases(pBtree, pBt, pTE->iTable, 0);
    }
  }
  pTE->pendingFlushSeekEdits = 0;
  return SQLITE_OK;
}
int syncBtreeSavepoints(Btree *pBtree){
  sqlite3 *db = pBtree ? pBtree->db : 0;
  /* vtab xSavepoint writes belong to the parent statement scope. */
  if( db && db->nVtabSavepoint==0 ){
    int target = db->nSavepoint + db->nStatement;
    while( pBtree->nSavepoint < target ){
      int rc = pushSavepoint(pBtree, pBtree->nSavepoint >= db->nSavepoint);
      if( rc!=SQLITE_OK ) return rc;
    }
  }
  return SQLITE_OK;
}
static int syncSavepoints(BtCursor *pCur){
  return syncBtreeSavepoints(pCur->pBtree);
}

/* Pending edit maps are shared by every cursor on the same table. When a flush
** swaps the table map, every live cursor must drop iterator state into it. */
void refreshCursorMutMapAliases(Btree *pBtree, BtShared *pBt,
                                        Pgno iTable, ProllyMutMap *pNewMap){
  BtCursor *p;
  assert( pBtree!=0 && pBt!=0 );
  for(p = pBt->pCursor; p; p = p->pNext){
    if( p->pBtree==pBtree && p->pgnoRoot==iTable ){
      p->pMutMap = pNewMap;
      p->mmActive = 0;
      p->mmPhysActive = 0;
      p->deferredTreeSeek = 0;
      p->mmIdx = -1;
      p->mmPhysIdx = -1;
    }
  }
}

int ensureMutMap(BtCursor *pCur){
  int rc;
  struct TableEntry *pTE;
  ProllyMutMap *pMap;

  assert( pCur!=0 && pCur->pBtree!=0 );
  assert( pCur->curFlags & BTCF_WriteFlag );
  pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
  if( !pTE ) return SQLITE_INTERNAL;

  if( pTE->pPending ){
    ProllyMutMap *pExisting = (ProllyMutMap*)pTE->pPending;
    /* Empty maps still track savepoint depth for later edits. */
    if( pCur->pBtree
     && prollyMutMapIsEmpty(pExisting)
     && pExisting->currentSavepointLevel != pCur->pBtree->nSavepoint ){
      pExisting->currentSavepointLevel = pCur->pBtree->nSavepoint;
    }
    pCur->pMutMap = pExisting;
    assert( pCur->pMutMap==pTE->pPending );
    return SQLITE_OK;
  }

  pMap = sqlite3_malloc(sizeof(ProllyMutMap));
  if( !pMap ) return SQLITE_NOMEM;
  rc = prollyMutMapInitMode(pMap, pCur->curIntKey, 0);
  if( rc!=SQLITE_OK ){
    sqlite3_free(pMap);
    return rc;
  }
  if( pCur->pBtree ){
    pMap->currentSavepointLevel = pCur->pBtree->nSavepoint;
  }
  pTE->pPending = pMap;
  refreshCursorMutMapAliases(pCur->pBtree, pCur->pBt, pCur->pgnoRoot, pMap);
  assert( pCur->pMutMap==pMap );
  return SQLITE_OK;
}

int saveCursorPosition(BtCursor *pCur){
  assert( pCur!=0 );
  if( pCur->eState!=CURSOR_VALID && pCur->eState!=CURSOR_SKIPNEXT ){
    return SQLITE_OK;
  }
  if( pCur->isPinned ){
    return SQLITE_CONSTRAINT_PINNED;
  }

  CLEAR_CACHED_PAYLOAD(pCur);

  /* A map-sourced row's key comes from the mutmap below; the tree cursor may
  ** legitimately be unpositioned (all rows still pending). */
  if( !prollyCursorIsValid(&pCur->pCur)
   && !(pCur->mmActive
        && (pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH)) ){
    if( pCur->curIntKey && (pCur->curFlags & BTCF_ValidNKey) ){

      pCur->nKey = pCur->cachedIntKey;
      pCur->pKey = 0;
      pCur->eState = CURSOR_REQUIRESEEK;
      return SQLITE_OK;
    }
    pCur->eState = CURSOR_INVALID;
    return SQLITE_OK;
  }

  if( pCur->curIntKey ){
    if( pCur->mmActive
     && (pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH) ){
      pCur->nKey = prollyMutMapEntryIntKey(currentMutMapEntry(pCur));
    }else{
      pCur->nKey = prollyCursorIntKey(&pCur->pCur);
    }
    pCur->pKey = 0;
  } else {
    const u8 *pKey = 0;
    int nKey = 0;
    if( pCur->mmActive
     && (pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH) ){
      ProllyMutMapEntry *pEntry = currentMutMapEntry(pCur);
      pKey = pEntry->pKey;
      nKey = pEntry->nKey;
    }else{
      prollyCursorKey(&pCur->pCur, &pKey, &nKey);
    }
    sqlite3_free(pCur->pKey);
    pCur->pKey = 0;
    if( nKey>0 ){
      pCur->pKey = sqlite3_malloc(nKey);
      if( !pCur->pKey ){
        return SQLITE_NOMEM;
      }
      memcpy(pCur->pKey, pKey, nKey);
      pCur->nKey = nKey;
    } else {
      pCur->nKey = 0;
    }
  }

  prollyCursorReleaseAll(&pCur->pCur);
  pCur->deferredTreeSeek = 0;

  pCur->eState = CURSOR_REQUIRESEEK;
  assert( pCur->pCur.eState!=PROLLY_CURSOR_VALID );
  return SQLITE_OK;
}

/* Borrowed pointer into the catalog or parsed schema; NULL if the number
** names nothing. Never allocates, so absence is unambiguous. */
const char *findTableNumberName(sqlite3 *db, Pgno iTable){
  Btree *pBtree;
  Schema *pSchema;
  HashElem *k;
  int i;
  if( !db || db->nDb<=0 ) return 0;
  pBtree = db->aDb[0].pBt;
  if( pBtree && pBtree->cat.a ){
    for(i=0; i<pBtree->cat.n; i++){
      if( pBtree->cat.a[i].iTable==iTable && pBtree->cat.a[i].zName ){
        return pBtree->cat.a[i].zName;
      }
    }
  }
  pSchema = db->aDb[0].pSchema;
  if( pSchema ){
    for(k=sqliteHashFirst(&pSchema->tblHash); k; k=sqliteHashNext(k)){
      Table *pTab = (Table*)sqliteHashData(k);
      if( pTab && pTab->tnum==(Pgno)iTable ){
        return pTab->zName;
      }
    }
  }
  return 0;
}

int tableEntryIsTableRoot(Btree *pBtree, struct TableEntry *pTE,
                                 int *pRc){
  if( !pTE || pTE->iTable<=1 ) return 0;
  if( pTE->tableRootKnown ) return pTE->isTableRoot ? 1 : 0;
  if( !pTE->zName && pBtree && pBtree->db ){
    const char *z = findTableNumberName(pBtree->db, pTE->iTable);
    if( z ){
      pTE->zName = sqlite3_mprintf("%s", z);
      if( !pTE->zName ){
        /* Leave the verdict uncached so a post-OOM retry can classify. */
        if( pRc ) *pRc = SQLITE_NOMEM;
        return 0;
      }
    }
  }
  pTE->isTableRoot = pTE->zName!=0;
  pTE->tableRootKnown = 1;
  return pTE->isTableRoot ? 1 : 0;
}

static int cursorIsShadowTableRoot(BtCursor *pCur, int *pRc){
  struct TableEntry *pTE;
  if( !pCur || !pCur->pBtree || !pCur->pBtree->db ) return 0;
  pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
  if( !tableEntryIsTableRoot(pCur->pBtree, pTE, pRc) || !pTE->zName ) return 0;
  return sqlite3ShadowTableName(pCur->pBtree->db, pTE->zName);
}

int restoreCursorPosition(BtCursor *pCur, int *pDifferentRow){
  int rc = SQLITE_OK;
  int res = 0;

  assert( pCur!=0 );
  if( pCur->eState!=CURSOR_REQUIRESEEK ){
    if( pDifferentRow ) *pDifferentRow = 0;
    return SQLITE_OK;
  }

  refreshCursorRoot(pCur);

  if( pCur->curIntKey ){
    rc = prollyBtCursorTableMoveto(pCur, pCur->nKey, 0, &res);
  } else {
    struct TableEntry *pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
    if( pTE && pTE->pPending && pCur->pMutMap!=(ProllyMutMap*)pTE->pPending ){
      pCur->pMutMap = (ProllyMutMap*)pTE->pPending;
    }
    if( pCur->pKey && pCur->nKey>0 ){
      rc = prollyCursorSeekBlob(&pCur->pCur,
                                 (const u8*)pCur->pKey, (int)pCur->nKey,
                                 &res);
    } else {
      pCur->eState = CURSOR_INVALID;
      if( pDifferentRow ) *pDifferentRow = 1;
      return SQLITE_OK;
    }
  }

  if( pCur->pKey ){
    sqlite3_free(pCur->pKey);
    pCur->pKey = 0;
  }

  if( rc==SQLITE_OK ){
    if( res==0 ){
      pCur->eState = CURSOR_VALID;
      if( pDifferentRow ){
        *pDifferentRow = pCur->mmActive
                       && (pCur->mergeSrc==MERGE_SRC_MUT
                           || pCur->mergeSrc==MERGE_SRC_BOTH);
      }
    } else if( pCur->pCur.eState==PROLLY_CURSOR_VALID ){
      pCur->skipNext = res;
      pCur->eState = CURSOR_SKIPNEXT;
      if( pDifferentRow ) *pDifferentRow = 1;
    } else {
      pCur->eState = CURSOR_INVALID;
      if( pDifferentRow ) *pDifferentRow = 1;
    }
  } else {
    pCur->eState = CURSOR_FAULT;
    pCur->skipNext = rc;
    if( pDifferentRow ) *pDifferentRow = 1;
  }

  return rc;
}


int prollyBtCursorInsert(
  BtCursor *pCur,
  const BtreePayload *pPayload,
  int flags,
  int seekResult
){
  int rc;
  const u8 *pInsertedPayload = 0;
  int nInsertedPayload = 0;
  u8 aLocalSortKey[128];
  const u8 *pSortKey = 0;
  int nSortKey = 0;
  (void)seekResult;
  pCur->deferredMergedSeek = 0;

  if( flags & BTREE_PREFORMAT ){
    return SQLITE_OK;
  }

  assert( pCur->pBtree->inTrans==TRANS_WRITE );
  assert( pCur->curFlags & BTCF_WriteFlag );

  rc = syncSavepoints(pCur);
  if( rc!=SQLITE_OK ) return rc;

  if( pCur->pgnoRoot==1 ){
    rc = ensureStatementSavepointsCaptured(pCur->pBtree);
    if( rc!=SQLITE_OK ) return rc;
    pCur->pBtree->bMasterRootChangedTxn = 1;
  }

  rc = saveAllCursors(pCur->pBtree, pCur->pBt, pCur->pgnoRoot, pCur);
  if( rc!=SQLITE_OK ) return rc;

  if( pCur->curIntKey && !(pCur->curFlags & BTCF_Incrblob) ){
    prollyInvalidateIncrblobCursors(pCur->pBt, pCur->pgnoRoot, pPayload->nKey, 0);
  }

  rc = ensureMutMap(pCur);
  if( rc!=SQLITE_OK ) return rc;

  if( pCur->curIntKey ){
    const u8 *pData = (const u8*)pPayload->pData;
    int nData = pPayload->nData;
    i64 nTotal64 = (i64)nData + (i64)pPayload->nZero;

    if( nData<0 || pPayload->nZero<0 || nTotal64 > 0x7fffffff ){
      return SQLITE_TOOBIG;
    }
    pInsertedPayload = pData;
    nInsertedPayload = nData;

    if( pPayload->nZero > 0 ){
      /* The zero tail stays symbolic all the way to the chunk store; the
      ** zeros are never materialized in memory. */
      rc = prollyMutMapInsertZeroTail(pCur->pMutMap, pPayload->nKey,
                                      pData, nData, (i64)pPayload->nZero);
    }else if( pCur->mmActive
     && pCur->mmPhysActive
     && pCur->pMutMap
     && (pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH)
     && (pCur->curFlags & BTCF_ValidNKey)
     && pCur->cachedIntKey==pPayload->nKey ){
      ProllyMutMapEntry *pEntry = currentMutMapEntry(pCur);
      if( pEntry->op==PROLLY_EDIT_INSERT ){
        rc = prollyMutMapReplaceEntry(pCur->pMutMap, pEntry, pData, nData);
      }else{
        rc = prollyMutMapInsert(pCur->pMutMap,
                                 NULL, 0, pPayload->nKey,
                                 pData, nData);
      }
    }else{
      rc = prollyMutMapInsert(pCur->pMutMap,
                               NULL, 0, pPayload->nKey,
                               pData, nData);
    }
  } else {

    int nKeyField = 0;
    int splitKey = 0;
    int storePayload = 0;
    int isIndex = 0;
    if( pCur->pKeyInfo ){
      struct TableEntry *pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
      int rcRoot = SQLITE_OK;
      isIndex = (pTE && !tableEntryIsTableRoot(pCur->pBtree, pTE, &rcRoot));
      if( rcRoot!=SQLITE_OK ) return rcRoot;
      storePayload = keyInfoHasLossyCollation(pCur->pKeyInfo);
    }
    if( pCur->pKeyInfo
     && pCur->pKeyInfo->nKeyField < pCur->pKeyInfo->nAllField ){
      nKeyField = (int)pCur->pKeyInfo->nKeyField;
      splitKey = 1;
      storePayload = 1;
    }
    /* Numeric sort keys normalize INTEGER and integral REAL values for
    ** comparison. Keep the original record when reconstruction must preserve
    ** a REAL serial type for result rows or covering-index reads. */
    if( !storePayload
     && sortKeyRecordNeedsPayload(
          (const u8*)pPayload->pKey, (int)pPayload->nKey,
          isIndex ? 0 : (splitKey ? nKeyField : 0)) ){
      storePayload = 1;
    }
    rc = sortKeyFromIntRecordLocal(
        pCur, (const u8*)pPayload->pKey, (int)pPayload->nKey,
        isIndex ? 0 : (splitKey ? nKeyField : 0),
        aLocalSortKey, (int)sizeof(aLocalSortKey), &nSortKey);
    if( rc==SQLITE_OK ){
      pSortKey = aLocalSortKey;
    }else if( rc==SQLITE_NOTFOUND ){
      rc = sortKeyFromRecordPrefixCollBuffer(
          (const u8*)pPayload->pKey, (int)pPayload->nKey,
          isIndex ? 0 : (splitKey ? nKeyField : 0),
          pCur->pKeyInfo,
          &pCur->pSeekSortKey, &pCur->nSeekSortKeyAlloc, &nSortKey);
      pSortKey = pCur->pSeekSortKey;
    }
    if( rc==SQLITE_OK ){
      if( storePayload ){
        rc = prollyMutMapInsert(pCur->pMutMap,
                                 pSortKey, nSortKey, 0,
                                 (const u8*)pPayload->pKey, (int)pPayload->nKey);
      }else{
        rc = prollyMutMapInsert(pCur->pMutMap,
                                 pSortKey, nSortKey, 0,
                                 NULL, 0);
      }
    }
  }

  if( rc!=SQLITE_OK ){
    return rc;
  }
  rc = prollyBtreeCheckMaxPageCount(pCur->pBtree);
  if( rc!=SQLITE_OK ){
    return rc;
  }

  {
    int canDefer = (pCur->pgnoRoot > 1);
    if( canDefer && mutMapShouldDrain(pCur) ){
      canDefer = 0;
    }
    if( canDefer ){
      if( (flags & BTREE_SAVEPOSITION) && pCur->curIntKey ){
        pCur->eState = CURSOR_VALID;
        pCur->curFlags |= BTCF_ValidNKey;
        pCur->cachedIntKey = pPayload->nKey;
        if( pPayload->nZero > 0 ){
          rc = cacheCursorPayloadZeroTail(pCur, pInsertedPayload,
                                          nInsertedPayload,
                                          (i64)pPayload->nZero);
        }else{
          rc = cacheCursorPayloadCopy(pCur, pInsertedPayload,
                                      nInsertedPayload);
        }
        if( rc!=SQLITE_OK ) return rc;

        pCur->mmActive = 0;
        pCur->flushSeekEdits = 0;
      } else if( (flags & BTREE_SAVEPOSITION) && !pCur->curIntKey ){
        ProllyMutMapEntry *pEntry = 0;
        CLEAR_CACHED_PAYLOAD(pCur);
        if( prollyCursorIsValid(&pCur->pCur) ){
          int trc = prollyCursorNext(&pCur->pCur);
          if( trc!=SQLITE_OK ) return trc;
        }
        rc = prollyMutMapFindRc(pCur->pMutMap, pSortKey, nSortKey, 0, &pEntry);
        if( rc!=SQLITE_OK ) return rc;
        if( pEntry ){
          pCur->mmIdx = prollyMutMapOrderIndexFromEntry(pCur->pMutMap, pEntry);
          pCur->mmPhysIdx = -1;
          pCur->mmActive = 1;
          pCur->mmPhysActive = 0;
          pCur->mergeSrc = MERGE_SRC_MUT;
          pCur->eState = CURSOR_VALID;
        }else{
          pCur->mmActive = 0;
          pCur->eState = pCur->pCur.eState==PROLLY_CURSOR_VALID
                       ? CURSOR_SKIPNEXT : CURSOR_INVALID;
          pCur->skipNext = pCur->eState==CURSOR_SKIPNEXT ? 1 : 0;
        }
        pCur->flushSeekEdits = 0;
      } else {
        pCur->eState = CURSOR_INVALID;
        pCur->flushSeekEdits = 0;
      }
      return SQLITE_OK;
    }
  }

  rc = flushMutMap(pCur);
  if( rc!=SQLITE_OK ) return rc;
  {
    struct TableEntry *pTE2 = findTable(pCur->pBtree, pCur->pgnoRoot);
    if( pTE2 ){
      prollyCursorClose(&pCur->pCur);
      prollyCursorInit(&pCur->pCur, &pCur->pBt->store, &pCur->pBt->cache,
                       &pTE2->root, pTE2->flags);
      prollyCursorAllowSparse(&pCur->pCur, 1);
    }
  }
  if( pCur->curIntKey ){
    int res = 0;
    rc = prollyCursorSeekInt(&pCur->pCur, pPayload->nKey, &res);
    if( rc==SQLITE_OK ){
      pCur->eState = CURSOR_VALID;
      if( res==0 ){
        pCur->curFlags |= BTCF_ValidNKey;
        pCur->cachedIntKey = pPayload->nKey;
      }
    }
  } else {
    int res = 0;
    rc = prollyCursorSeekBlob(&pCur->pCur,
                               (const u8*)pPayload->pKey,
                               (int)pPayload->nKey, &res);
    if( rc==SQLITE_OK ) pCur->eState = CURSOR_VALID;
  }

  return rc;
}

int flushIfNeeded(BtCursor *pCur){
  int rc;
  struct TableEntry *pTE;

  if( !pCur->pMutMap || prollyMutMapIsEmpty(pCur->pMutMap) ){
    return SQLITE_OK;
  }

  {
    BtCursor *p;
    for(p = pCur->pBt->pCursor; p; p = p->pNext){
      if( p->pBtree==pCur->pBtree
       && p!=pCur
       && p->pgnoRoot==pCur->pgnoRoot ){
        if( !p->isPinned
         && (p->eState==CURSOR_VALID || p->eState==CURSOR_SKIPNEXT) ){
          p->isPinned = 1;
          rc = saveCursorPosition(p);
          p->isPinned = 0;
          if( rc!=SQLITE_OK ) return rc;
        } else if( p->eState!=CURSOR_REQUIRESEEK
                && p->eState!=CURSOR_INVALID ){
          prollyCursorReleaseAll(&p->pCur);
        }
      }
    }
  }

  /* Save the initiating cursor too; callback-driven COMMIT can keep it live. */
  if( !pCur->isPinned
   && (pCur->eState==CURSOR_VALID || pCur->eState==CURSOR_SKIPNEXT) ){
    rc = saveCursorPosition(pCur);
    if( rc!=SQLITE_OK ) return rc;
  }

  rc = flushMutMap(pCur);
  if( rc!=SQLITE_OK ) return rc;

  pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
  if( pTE ){
    prollyCursorClose(&pCur->pCur);
    prollyCursorInit(&pCur->pCur, &pCur->pBt->store, &pCur->pBt->cache,
                     &pTE->root, pTE->flags);
    prollyCursorAllowSparse(&pCur->pCur, 1);
  }
  /* The flush emptied the map; a saved cursor restoring with a stale
  ** mmActive/mmIdx would consult it at a dead index. */
  refreshCursorMutMapAliases(pCur->pBtree, pCur->pBt, pCur->pgnoRoot,
                             pTE ? (ProllyMutMap*)pTE->pPending : 0);
  /* Preserve REQUIRESEEK/FAULT states across flush. */
  if( pCur->eState!=CURSOR_REQUIRESEEK && pCur->eState!=CURSOR_FAULT ){
    pCur->eState = CURSOR_INVALID;
  }
  return SQLITE_OK;
}

int flushAllPending(Btree *pBtree, BtShared *pBt, Pgno iTable){
  BtCursor *p;
  int rc;

  assert( pBtree!=0 && pBt!=0 );
  assert( pBtree->inTrans==TRANS_WRITE );
  for(p = pBt->pCursor; p; p = p->pNext){
    if( p->pBtree==pBtree && (iTable==0 || p->pgnoRoot==iTable) ){
      rc = flushIfNeeded(p);
      if( rc!=SQLITE_OK ) return rc;
    }
  }

  rc = flushDeferredEdits(pBtree, pBt);
  if( rc!=SQLITE_OK ) return rc;

  return SQLITE_OK;
}

int flushDeferredEdits(Btree *pBtree, BtShared *pBt){
  int rc = SQLITE_OK;
  if( pBtree ){
    int i;
    for(i=0; i<pBtree->cat.n; i++){
      struct TableEntry *pTE = &pBtree->cat.a[i];
      rc = flushPendingForTable(pBtree, pBt, pTE, 0);
      if( rc!=SQLITE_OK ) return rc;
    }
  }
  return rc;
}

static int btreeDeleteImmediate(BtCursor *pCur){
  int rc;

  rc = flushMutMap(pCur);
  if( rc!=SQLITE_OK ){
    return rc;
  }

  {
    struct TableEntry *pTE2 = findTable(pCur->pBtree, pCur->pgnoRoot);
    if( pTE2 ){
      prollyCursorClose(&pCur->pCur);
      prollyCursorInit(&pCur->pCur, &pCur->pBt->store, &pCur->pBt->cache,
                       &pTE2->root, pTE2->flags);
      prollyCursorAllowSparse(&pCur->pCur, 1);
    }
  }

  pCur->curFlags &= ~(BTCF_ValidNKey|BTCF_AtLast);
  return rc;
}
int sqlite3BtreeInsert(
  BtCursor *pCur,
  const BtreePayload *pPayload,
  int flags,
  int seekResult
){
  if( !pCur ) return SQLITE_OK;
  return pCur->pCurOps->xInsert(pCur, pPayload, flags, seekResult);
}

int prollyBtCursorDelete(BtCursor *pCur, u8 flags){
  int rc;
  const u8 *pKey = 0;
  int nKey = 0;
  i64 iKey = 0;

  u8 *pSavedDelKey = 0;
  int nSavedDelKey = 0;
  int savedDelKeyOwned = 0;
  i64 savedIntKey = 0;
  int hasSavedKey = 0;

  pCur->deferredMergedSeek = 0;

  assert( pCur->pBtree->inTrans==TRANS_WRITE );
  assert( pCur->curFlags & BTCF_WriteFlag );

  if( pCur->eState==CURSOR_REQUIRESEEK ){
    rc = restoreCursorPosition(pCur, 0);
    if( rc!=SQLITE_OK || pCur->eState!=CURSOR_VALID ) return rc;
  }else if( pCur->eState==CURSOR_SKIPNEXT ){
    pCur->eState = CURSOR_VALID;
    pCur->skipNext = 0;
  }else if( pCur->eState==CURSOR_INVALID ){

  }else if( pCur->eState!=CURSOR_VALID ){
    return SQLITE_CORRUPT_BKPT;
  }

  if( pCur->eState==CURSOR_VALID || pCur->eState==CURSOR_INVALID ){
    if( pCur->curIntKey ){
      if( pCur->mmActive
       && (pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH) ){
        savedIntKey = prollyMutMapEntryIntKey(currentMutMapEntry(pCur));
        hasSavedKey = 1;
      }else if( !prollyCursorIsValid(&pCur->pCur)
       && (pCur->curFlags & BTCF_ValidNKey) ){
        savedIntKey = pCur->cachedIntKey;
        hasSavedKey = 1;
      }else if( prollyCursorIsValid(&pCur->pCur) ){
        savedIntKey = prollyCursorIntKey(&pCur->pCur);
        hasSavedKey = 1;
      }
    } else {
      if( pCur->nSeekSortKey>0
       && pCur->nSeekKeyField==0
       && ((flags & BTREE_AUXDELETE) || cachedSeekKeyMatchesCurrent(pCur)) ){
        pSavedDelKey = pCur->pSeekSortKey;
        nSavedDelKey = pCur->nSeekSortKey;
        hasSavedKey = 1;
      }else if( pCur->mmActive
       && (pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH) ){
        ProllyMutMapEntry *e = currentMutMapEntry(pCur);
        pSavedDelKey = sqlite3_malloc(e->nKey);
        if( !pSavedDelKey ) return SQLITE_NOMEM;
        memcpy(pSavedDelKey, e->pKey, e->nKey);
        nSavedDelKey = e->nKey;
        savedDelKeyOwned = 1;
        hasSavedKey = 1;
      }else if( pCur->pCachedPayload && pCur->nCachedPayload > 0 ){
        int nDelKeyField = 0;
        if( pCur->pKeyInfo
         && pCur->pKeyInfo->nKeyField < pCur->pKeyInfo->nAllField ){
          struct TableEntry *pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
          int rcRoot = SQLITE_OK;
          if( pTE && !tableEntryIsTableRoot(pCur->pBtree, pTE, &rcRoot) ){
            if( rcRoot!=SQLITE_OK ) return rcRoot;
            nDelKeyField = 0;
          }else{
            nDelKeyField = (int)pCur->pKeyInfo->nKeyField;
          }
        }
        rc = sortKeyFromRecordPrefixCollBuffer(
            pCur->pCachedPayload, pCur->nCachedPayload,
            nDelKeyField, pCur->pKeyInfo,
            &pCur->pSeekSortKey, &pCur->nSeekSortKeyAlloc,
            &nSavedDelKey);
        if( rc!=SQLITE_OK ) return rc;
        pSavedDelKey = pCur->pSeekSortKey;
        hasSavedKey = 1;
      }else if( prollyCursorIsValid(&pCur->pCur) ){
        const u8 *pTmp; int nTmp;
        prollyCursorKey(&pCur->pCur, &pTmp, &nTmp);
        pSavedDelKey = sqlite3_malloc(nTmp);
        if( !pSavedDelKey ) return SQLITE_NOMEM;
        memcpy(pSavedDelKey, pTmp, nTmp);
        nSavedDelKey = nTmp;
        savedDelKeyOwned = 1;
        hasSavedKey = 1;
      }
    }
  }

  rc = syncSavepoints(pCur);
  if( rc!=SQLITE_OK ) goto delete_cleanup;

  if( pCur->pgnoRoot==1 ){
    rc = ensureStatementSavepointsCaptured(pCur->pBtree);
    if( rc!=SQLITE_OK ) goto delete_cleanup;
    pCur->pBtree->bMasterRootChangedTxn = 1;
  }

  rc = saveAllCursors(pCur->pBtree, pCur->pBt, pCur->pgnoRoot, pCur);
  if( rc!=SQLITE_OK ) goto delete_cleanup;

  if( pCur->curIntKey && hasSavedKey ){
    prollyInvalidateIncrblobCursors(pCur->pBt, pCur->pgnoRoot, savedIntKey, 0);
  }

  rc = ensureMutMap(pCur);
  if( rc!=SQLITE_OK ) goto delete_cleanup;

  /* No saved key means no-op; falling through would delete rowid 0 or ''. */
  if( !hasSavedKey ){
    rc = SQLITE_OK;
    goto delete_cleanup;
  }
  if( pCur->curIntKey ){
    iKey = savedIntKey;
    if( pCur->mmActive
     && pCur->mmPhysActive
     && pCur->pMutMap
     && (pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH) ){
      ProllyMutMapEntry *pEntry = currentMutMapEntry(pCur);
      if( pEntry->op==PROLLY_EDIT_INSERT
       && prollyMutMapEntryIntKey(pEntry)==iKey ){
        rc = prollyMutMapDeleteEntry(pCur->pMutMap, pEntry);
      }else{
        rc = prollyMutMapDelete(pCur->pMutMap, NULL, 0, iKey);
      }
    }else{
      rc = prollyMutMapDelete(pCur->pMutMap, NULL, 0, iKey);
    }
  } else {
    pKey = pSavedDelKey;
    nKey = nSavedDelKey;
    if( pCur->mmActive
     && pCur->mmPhysActive
     && pCur->pMutMap
     && (pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH) ){
      ProllyMutMapEntry *pEntry = currentMutMapEntry(pCur);
      if( pEntry->op==PROLLY_EDIT_INSERT
       && pEntry->nKey==nKey
       && nKey>0
       && memcmp(pEntry->pKey, pKey, nKey)==0 ){
        rc = prollyMutMapDeleteEntry(pCur->pMutMap, pEntry);
      }else{
        rc = prollyMutMapDelete(pCur->pMutMap, pKey, nKey, 0);
      }
    }else{
      rc = prollyMutMapDelete(pCur->pMutMap, pKey, nKey, 0);
    }
    /* pKey may alias pSavedDelKey until the reseek below finishes. */
  }

  if( rc!=SQLITE_OK ) goto delete_cleanup;

  {
    int canDefer = (pCur->pgnoRoot > 1);
    if( canDefer && mutMapShouldDrain(pCur) ){
      canDefer = 0;
    }
    if( canDefer ){
      CLEAR_CACHED_PAYLOAD(pCur);
      pCur->curFlags &= ~(BTCF_ValidNKey|BTCF_AtLast);
      pCur->mmActive = 0;
      if( flags & (BTREE_SAVEPOSITION | BTREE_AUXDELETE) ){
        int rcRoot = SQLITE_OK;
        pCur->flushSeekEdits = 1;
        if( pCur->curIntKey && hasSavedKey
         && ((flags & BTREE_AUXDELETE)
             || cursorIsShadowTableRoot(pCur, &rcRoot)) ){
          pCur->cachedIntKey = iKey;
          pCur->curFlags |= BTCF_DeleteKey;
        }else if( rcRoot!=SQLITE_OK ){
          rc = rcRoot;
          goto delete_cleanup;
        }else if( !pCur->curIntKey && hasSavedKey && nKey>0 ){
          /* Park the cursor on the mut-map tombstone of the deleted key so
          ** the next step resumes the scan from there. Re-seeding from a
          ** stale or unset position can restart the scan at an unrelated
          ** row, making a delete loop re-visit (and delete) live rows that
          ** never matched its constraint. */
          ProllyMutMapEntry *pEntry = 0;
          rc = prollyMutMapFindRc(pCur->pMutMap, pKey, nKey, 0, &pEntry);
          if( rc!=SQLITE_OK ) goto delete_cleanup;
          if( pEntry ){
            setCursorToMutMapMissingEntryPhys(
                pCur, (int)(pEntry - pCur->pMutMap->aEntries));
            pCur->deferredTreeSeek = 1;
          }
        }
        pCur->eState = CURSOR_SKIPNEXT;
        pCur->skipNext = 0;
      } else {
        pCur->eState = CURSOR_INVALID;
      }
      rc = SQLITE_OK;
      goto delete_cleanup;
    }
  }

  rc = btreeDeleteImmediate(pCur);
  if( rc!=SQLITE_OK ) goto delete_cleanup;

  if( flags & BTREE_SAVEPOSITION ){
    int res = 0;
    if( pCur->curIntKey ){
      rc = prollyCursorSeekInt(&pCur->pCur, iKey, &res);
    } else if( pKey && nKey > 0 ){

      u8 *pReseek = sqlite3_malloc(nKey);
      if( pReseek ){
        memcpy(pReseek, pKey, nKey);
        rc = prollyCursorSeekBlob(&pCur->pCur, pReseek, nKey, &res);
        sqlite3_free(pReseek);
      } else {
        rc = SQLITE_NOMEM;
      }
    } else {
      rc = SQLITE_OK;
      res = -1;
    }
    if( rc==SQLITE_OK && prollyCursorIsValid(&pCur->pCur) ){
      pCur->eState = CURSOR_SKIPNEXT;
      pCur->skipNext = (res>=0) ? 1 : -1;
    } else {
      pCur->eState = CURSOR_INVALID;
    }
  } else {
    pCur->eState = CURSOR_INVALID;
  }

  rc = SQLITE_OK;

delete_cleanup:
  if( savedDelKeyOwned ) sqlite3_free(pSavedDelKey);
  return rc;
}
int sqlite3BtreeDelete(BtCursor *pCur, u8 flags){
  if( !pCur ) return SQLITE_OK;
  return pCur->pCurOps->xDelete(pCur, flags);
}

int prollyBtCursorTransferRow(BtCursor *pDest, BtCursor *pSrc, i64 iKey){
  int rc;
  BtreePayload payload;

  assert( pSrc->eState==CURSOR_VALID );

  memset(&payload, 0, sizeof(payload));

  if( pDest->curIntKey ){
    /* Read the source value through getCursorPayload (mutmap-aware): a row that
    ** lives in the source's pending mutmap has no tree node, so a bare
    ** prollyCursorValue() on pSrc->pCur NULL-derefs in prollyNodeValue. */
    const u8 *pVal;
    int nVal;
    rc = getCursorPayload(pSrc, &pVal, &nVal);
    if( rc!=SQLITE_OK ){
      return rc;
    }
    payload.nKey = iKey;
    payload.pData = pVal;
    payload.nData = nVal;
  } else {
    const u8 *pKey;
    int nKey;
    if( pSrc->mmActive
     && (pSrc->mergeSrc==MERGE_SRC_MUT || pSrc->mergeSrc==MERGE_SRC_BOTH) ){
      ProllyMutMapEntry *pEntry = currentMutMapEntry(pSrc);
      pKey = pEntry->pKey;
      nKey = pEntry->nKey;
    }else{
      prollyCursorKey(&pSrc->pCur, &pKey, &nKey);
    }
    payload.pKey = pKey;
    payload.nKey = nKey;
  }

  rc = sqlite3BtreeInsert(pDest, &payload, 0, 0);
  return rc;
}


#ifndef SQLITE_OMIT_INCRBLOB

int prollyBtCursorPayloadChecked(BtCursor *pCur, u32 offset, u32 amt, void *pBuf){
  const u8 *pVal;
  int nVal;
  int rc;

  if( pCur->eState!=CURSOR_VALID ){
    return SQLITE_ABORT;
  }

  if( pCur->curIntKey
   && pCur->mmActive
   && (pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH) ){
    ProllyMutMapEntry *e = currentMutMapEntry(pCur);
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

  rc = getCursorPayload(pCur, &pVal, &nVal);
  if( rc!=SQLITE_OK ){
    return rc;
  }

  if( (i64)offset + (i64)amt > (i64)nVal ){
    return SQLITE_CORRUPT_BKPT;
  }

  memcpy(pBuf, pVal + offset, amt);
  return SQLITE_OK;
}
int sqlite3BtreePayloadChecked(BtCursor *pCur, u32 offset, u32 amt, void *pBuf){
  return pCur->pCurOps->xPayloadChecked(pCur, offset, amt, pBuf);
}

int prollyBtCursorPutData(BtCursor *pCur, u32 offset, u32 amt, void *pBuf){
  int rc;
  const u8 *pVal;
  int nVal;
  u8 *pNew;
  i64 savedIntKey = 0;
  int hasSavedIntKey = 0;
  BtreePayload payload;

  if( pCur->eState!=CURSOR_VALID ){
    return SQLITE_ABORT;
  }
  if( !(pCur->curFlags & BTCF_WriteFlag) ){
    return SQLITE_READONLY;
  }
  assert( pCur->curFlags & BTCF_Incrblob );

  /* A mutmap-sourced row's current value lives in the pending map, not the
  ** tree leaf; the raw tree value here is stale or unpositioned. */
  rc = getCursorPayload(pCur, &pVal, &nVal);
  if( rc!=SQLITE_OK ) return rc;

  if( (i64)offset + (i64)amt > (i64)nVal ){
    return SQLITE_CORRUPT_BKPT;
  }

  pNew = sqlite3_malloc(nVal);
  if( !pNew ) return SQLITE_NOMEM;
  memcpy(pNew, pVal, nVal);

  memcpy(pNew + offset, pBuf, amt);

  memset(&payload, 0, sizeof(payload));

  if( pCur->curIntKey ){
    /* A row pending in the mutmap may have no positioned tree cursor. */
    if( pCur->mmActive
     && (pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH) ){
      payload.nKey = prollyMutMapEntryIntKey(currentMutMapEntry(pCur));
    }else{
      payload.nKey = prollyCursorIntKey(&pCur->pCur);
    }
    savedIntKey = payload.nKey;
    hasSavedIntKey = 1;
    payload.pData = pNew;
    payload.nData = nVal;
  } else {
    const u8 *pKey;
    int nKey;
    prollyCursorKey(&pCur->pCur, &pKey, &nKey);
    payload.pKey = pKey;
    payload.nKey = nKey;
    payload.pData = pNew;
    payload.nData = nVal;
  }

  rc = sqlite3BtreeInsert(pCur, &payload, 0, 0);
  sqlite3_free(pNew);
  if( rc==SQLITE_OK && hasSavedIntKey ){
    ProllyMutMapEntry *pEntry = 0;
    if( pCur->pMutMap ){
      rc = prollyMutMapFindRc(pCur->pMutMap, 0, 0, savedIntKey, &pEntry);
    }
    if( rc==SQLITE_OK && pEntry && pEntry->op==PROLLY_EDIT_INSERT ){
      setCursorToMutMapEntryPhys(
          pCur, (int)(pEntry - pCur->pMutMap->aEntries));
      pCur->deferredTreeSeek = 1;
    }else if( rc==SQLITE_OK ){
      pCur->nKey = savedIntKey;
      pCur->pKey = 0;
      pCur->cachedIntKey = savedIntKey;
      pCur->curFlags |= BTCF_ValidNKey;
      pCur->eState = CURSOR_REQUIRESEEK;
    }
  }
  return rc;
}
int sqlite3BtreePutData(BtCursor *pCur, u32 offset, u32 amt, void *pBuf){
  return pCur->pCurOps->xPutData(pCur, offset, amt, pBuf);
}

void prollyBtCursorIncrblobCursor(BtCursor *pCur){
  pCur->curFlags |= BTCF_Incrblob;
}
void sqlite3BtreeIncrblobCursor(BtCursor *pCur){
  pCur->pCurOps->xIncrblobCursor(pCur);
}

#endif


void doltliteSetTableSchemaHash(sqlite3 *db, Pgno iTable, const ProllyHash *pH){
  Btree *pBtree;
  int i;
  if( !db || db->nDb<=0 || !db->aDb[0].pBt ) return;
  pBtree = db->aDb[0].pBt;
  for(i=0; i<pBtree->cat.n; i++){
    if( pBtree->cat.a[i].iTable==iTable ){
      memcpy(&pBtree->cat.a[i].schemaHash, pH, sizeof(ProllyHash));
      return;
    }
  }
}

/* Declared in doltlite_internal.h. Forward-declared here because that
** header redefines TableEntry/SchemaEntry in a shape incompatible with
** this file's local definitions, so we can't just include it. */
extern int doltliteBuildIndexSortKey(
  const u8 *pRec, int nRec,
  const i16 *aiColumn, int nIdxCol,
  KeyInfo *pKeyInfo,
  int iPKey, i64 intKey,
  const u8 *pTreeKey, int nTreeKey,
  u8 **ppKey, int *pnKey
);

/* Read the current value bytes at (pKey/intKey) in the table's prolly
** tree. *ppOld is set to a freshly-allocated copy (caller frees) or
** to NULL if the key is not present. */
static int readRowValue(
  BtShared *pBt,
  struct TableEntry *pTE,
  const u8 *pKey, int nKey, i64 intKey,
  u8 **ppOld, int *pnOld
){
  ProllyCursor cur;
  int rc;
  int res = 0;
  *ppOld = 0;
  *pnOld = 0;
  prollyCursorInit(&cur, &pBt->store, &pBt->cache, &pTE->root, pTE->flags);
  if( pTE->flags & PROLLY_NODE_INTKEY ){
    rc = prollyCursorSeekInt(&cur, intKey, &res);
  }else{
    rc = prollyCursorSeekBlob(&cur, pKey, nKey, &res);
  }
  if( rc==SQLITE_OK && res==0 && prollyCursorIsValid(&cur) ){
    const u8 *pVal = 0;
    int nVal = 0;
    prollyCursorValue(&cur, &pVal, &nVal);
    if( nVal>0 ){
      *ppOld = sqlite3_malloc(nVal);
      if( !*ppOld ){
        prollyCursorClose(&cur);
        return SQLITE_NOMEM;
      }
      memcpy(*ppOld, pVal, nVal);
      *pnOld = nVal;
    }
  }
  prollyCursorClose(&cur);
  return rc;
}

/* Mutate one secondary index: delete the old key (if pOldVal is set)
** and insert the new key (if pNewVal is set). Used by the raw-row
** mutation path so dolt_conflicts_resolve --theirs (and any future
** caller) keeps secondary indexes consistent with the table. */
static int mutateSecondaryIndex(
  BtShared *pBt,
  struct TableEntry *pIdxTE,
  Index *pIdx,
  int iPKey,
  i64 intKey,
  const u8 *pTreeKey, int nTreeKey,
  const u8 *pOldVal, int nOldVal,
  const u8 *pNewVal, int nNewVal
){
  ProllyMutMap mm;
  ProllyMutator mut;
  u8 *pOldKey = 0, *pNewKey = 0;
  int nOldKey = 0, nNewKey = 0;
  int rc;

  rc = prollyMutMapInit(&mm, 0);
  if( rc!=SQLITE_OK ) return rc;

  if( pOldVal && nOldVal>0 ){
    rc = doltliteBuildIndexSortKey(pOldVal, nOldVal,
                                   pIdx->aiColumn, pIdx->nKeyCol, 0,
                                   iPKey, intKey,
                                   pTreeKey, nTreeKey,
                                   &pOldKey, &nOldKey);
    if( rc==SQLITE_OK ){
      rc = prollyMutMapDelete(&mm, pOldKey, nOldKey, 0);
    }
    sqlite3_free(pOldKey);
    if( rc!=SQLITE_OK ){
      prollyMutMapFree(&mm);
      return rc;
    }
  }
  if( pNewVal && nNewVal>0 ){
    rc = doltliteBuildIndexSortKey(pNewVal, nNewVal,
                                   pIdx->aiColumn, pIdx->nKeyCol, 0,
                                   iPKey, intKey,
                                   pTreeKey, nTreeKey,
                                   &pNewKey, &nNewKey);
    if( rc==SQLITE_OK ){
      rc = prollyMutMapInsert(&mm, pNewKey, nNewKey, 0, 0, 0);
    }
    sqlite3_free(pNewKey);
    if( rc!=SQLITE_OK ){
      prollyMutMapFree(&mm);
      return rc;
    }
  }

  memset(&mut, 0, sizeof(mut));
  mut.pStore = &pBt->store;
  mut.pCache = &pBt->cache;
  memcpy(&mut.oldRoot, &pIdxTE->root, sizeof(ProllyHash));
  mut.pEdits = &mm;
  mut.flags = pIdxTE->flags;

  rc = prollyMutateFlush(&mut);
  if( rc==SQLITE_OK ){
    memcpy(&pIdxTE->root, &mut.newRoot, sizeof(ProllyHash));
  }
  prollyMutMapFree(&mm);
  return rc;
}

int doltliteApplyRawRowMutation(
  sqlite3 *db,
  const char *zTable,
  const u8 *pKey, int nKey, i64 intKey,
  const u8 *pVal, int nVal
){
  Btree *pBtree;
  BtShared *pBt;
  struct TableEntry *pTE;
  ProllyMutMap mm;
  ProllyMutator mut;
  u8 *pOldVal = 0;
  int nOldVal = 0;
  Table *pTab = 0;
  int rc;
  u8 isIntKey;

  if( !db || !zTable ) return SQLITE_MISUSE;
  if( db->nDb<=0 || !db->aDb[0].pBt ) return SQLITE_ERROR;
  pBtree = db->aDb[0].pBt;
  pBt = pBtree->pBt;
  if( !pBt ) return SQLITE_ERROR;
  rc = doltliteEnsureWriteTxnAndSavepoints(db);
  if( rc!=SQLITE_OK ) return rc;

  {
    int i;
    pTE = 0;
    for(i=0; i<pBtree->cat.n; i++){
      if( pBtree->cat.a[i].zName
       && strcmp(pBtree->cat.a[i].zName, zTable)==0 ){
        pTE = &pBtree->cat.a[i];
        break;
      }
    }
  }
  if( !pTE ) return SQLITE_NOTFOUND;

  rc = flushPendingForTable(pBtree, pBt, pTE, 0);
  if( rc!=SQLITE_OK ) return rc;

  /* Read the existing row value so we can compute index keys to delete.
  ** Only matters when the table has secondary indexes. */
  pTab = sqlite3FindTable(db, zTable, "main");
  if( pTab && pTab->pIndex ){
    rc = readRowValue(pBt, pTE, pKey, nKey, intKey, &pOldVal, &nOldVal);
    if( rc!=SQLITE_OK ){
      sqlite3_free(pOldVal);
      return rc;
    }
  }

  isIntKey = (pTE->flags & PROLLY_NODE_INTKEY) ? 1 : 0;
  rc = prollyMutMapInit(&mm, isIntKey);
  if( rc!=SQLITE_OK ){
    sqlite3_free(pOldVal);
    return rc;
  }

  if( pVal ){
    rc = prollyMutMapInsert(&mm, pKey, nKey, intKey, pVal, nVal);
  }else{
    rc = prollyMutMapDelete(&mm, pKey, nKey, intKey);
  }
  if( rc!=SQLITE_OK ){
    prollyMutMapFree(&mm);
    sqlite3_free(pOldVal);
    return rc;
  }

  memset(&mut, 0, sizeof(mut));
  mut.pStore = &pBt->store;
  mut.pCache = &pBt->cache;
  memcpy(&mut.oldRoot, &pTE->root, sizeof(ProllyHash));
  mut.pEdits = &mm;
  mut.flags = pTE->flags;

  rc = prollyMutateFlush(&mut);
  if( rc==SQLITE_OK ){
    memcpy(&pTE->root, &mut.newRoot, sizeof(ProllyHash));
  }
  prollyMutMapFree(&mm);

  /* Update secondary indexes (delete old key, insert new). pOldVal can
  ** be NULL when this is an insert of a fresh PK; pVal is NULL when
  ** this is a delete. */
  if( rc==SQLITE_OK && pTab && pTab->pIndex
   && (pOldVal || (pVal && nVal>0)) ){
    Index *pIdx;
    int iPKey = pTab->iPKey;
    for(pIdx=pTab->pIndex; pIdx && rc==SQLITE_OK; pIdx=pIdx->pNext){
      struct TableEntry *pIdxTE = 0;
      int j;
      /* WITHOUT ROWID tables expose the PK as a pseudo-INDEX whose
      ** tnum equals the table's own root. Mutating it like a
      ** secondary index would overwrite the table tree. Skip. */
      if( pIdx->idxType==SQLITE_IDXTYPE_PRIMARYKEY ) continue;
      for(j=0; j<pBtree->cat.n; j++){
        if( pBtree->cat.a[j].iTable==(Pgno)pIdx->tnum ){
          pIdxTE = &pBtree->cat.a[j];
          break;
        }
      }
      if( !pIdxTE ) continue;
      rc = mutateSecondaryIndex(pBt, pIdxTE, pIdx, iPKey, intKey,
                                pKey, nKey,
                                pOldVal, nOldVal, pVal, nVal);
    }
  }

  sqlite3_free(pOldVal);
  return rc;
}

int doltliteEnsureWriteTxnAndSavepoints(sqlite3 *db){
  Btree *pBtree;
  int rc = SQLITE_OK;
  int target;

  if( !db || db->nDb<=0 || !db->aDb[0].pBt ) return SQLITE_ERROR;
  pBtree = db->aDb[0].pBt;
  if( pBtree->inTrans!=TRANS_WRITE ){
    rc = sqlite3BtreeBeginTrans(pBtree, 2, 0);
    if( rc!=SQLITE_OK ) return rc;
  }

  /* VC functions can write through this btree layer without first touching a
  ** SQL table, so mirror SQLite's active savepoint stack before mutating it. */
  target = db->nSavepoint;
  while( pBtree->nSavepoint < target ){
    rc = pushSavepoint(pBtree, 0);
    if( rc!=SQLITE_OK ) return rc;
  }
  return SQLITE_OK;
}


#endif /* DOLTLITE_PROLLY */
