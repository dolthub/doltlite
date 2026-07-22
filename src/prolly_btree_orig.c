#ifdef DOLTLITE_PROLLY

#include "prolly_btree_int.h"

/* Dispatch adapters for attached databases backed by SQLite's B-tree. */

int origBtreeCloseVt(Btree *p){
  int rc = origBtreeClose(p->pOrigBtree);
  p->pOrigBtree = 0;

  if( p->pSchema && p->xFreeSchema ) p->xFreeSchema(p->pSchema);
  sqlite3_free(p);
  return rc;
}
int origBtreeNewDbVt(Btree *p){
  return origBtreeNewDb(p->pOrigBtree);
}
int origBtreeSetCacheSizeVt(Btree *p, int mxPage){
  return origBtreeSetCacheSize(p->pOrigBtree, mxPage);
}
int origBtreeSetSpillSizeVt(Btree *p, int mxPage){
  return origBtreeSetSpillSize(p->pOrigBtree, mxPage);
}
int origBtreeSetMmapLimitVt(Btree *p, sqlite3_int64 szMmap){
  return origBtreeSetMmapLimit(p->pOrigBtree, szMmap);
}
int origBtreeSetPagerFlagsVt(Btree *p, unsigned pgFlags){
  return origBtreeSetPagerFlags(p->pOrigBtree, pgFlags);
}
int origBtreeSetPageSizeVt(Btree *p, int nPagesize, int nReserve, int eFix){
  return origBtreeSetPageSize(p->pOrigBtree, nPagesize, nReserve, eFix);
}
int origBtreeGetPageSizeVt(Btree *p){
  return origBtreeGetPageSize(p->pOrigBtree);
}
Pgno origBtreeMaxPageCountVt(Btree *p, Pgno mxPage){
  return origBtreeMaxPageCount(p->pOrigBtree, mxPage);
}
Pgno origBtreeLastPageVt(Btree *p){
  return origBtreeLastPage(p->pOrigBtree);
}
int origBtreeSecureDeleteVt(Btree *p, int newFlag){
  return origBtreeSecureDelete(p->pOrigBtree, newFlag);
}
int origBtreeGetRequestedReserveVt(Btree *p){
  return origBtreeGetRequestedReserve(p->pOrigBtree);
}
int origBtreeGetReserveNoMutexVt(Btree *p){
  return origBtreeGetReserveNoMutex(p->pOrigBtree);
}
int origBtreeSetAutoVacuumVt(Btree *p, int autoVacuum){
  return origBtreeSetAutoVacuum(p->pOrigBtree, autoVacuum);
}
int origBtreeGetAutoVacuumVt(Btree *p){
  return origBtreeGetAutoVacuum(p->pOrigBtree);
}
int origBtreeIncrVacuumVt(Btree *p){
  return origBtreeIncrVacuum(p->pOrigBtree);
}
const char *origBtreeGetFilenameVt(Btree *p){
  return origBtreeGetFilename(p->pOrigBtree);
}
const char *origBtreeGetJournalnameVt(Btree *p){
  return origBtreeGetJournalname(p->pOrigBtree);
}
int origBtreeIsReadonlyVt(Btree *p){
  return origBtreeIsReadonly(p->pOrigBtree);
}
int origBtreeBeginTransVt(Btree *p, int wrFlag, int *pSchemaVersion){
  return origBtreeBeginTrans(p->pOrigBtree, wrFlag, pSchemaVersion);
}
int origBtreeCommitPhaseOneVt(Btree *p, const char *zSuperJrnl){
  return origBtreeCommitPhaseOne(p->pOrigBtree, zSuperJrnl);
}
int origBtreeCommitPhaseTwoVt(Btree *p, int bCleanup){
  return origBtreeCommitPhaseTwo(p->pOrigBtree, bCleanup);
}
int origBtreeCommitVt(Btree *p){
  return origBtreeCommit(p->pOrigBtree);
}
int origBtreeRollbackVt(Btree *p, int tripCode, int writeOnly){
  return origBtreeRollback(p->pOrigBtree, tripCode, writeOnly);
}
int origBtreeBeginStmtVt(Btree *p, int iStatement){
  return origBtreeBeginStmt(p->pOrigBtree, iStatement);
}
int origBtreeSavepointVt(Btree *p, int op, int iSavepoint){
  return origBtreeSavepoint(p->pOrigBtree, op, iSavepoint);
}
int origBtreeTxnStateVt(Btree *p){
  return origBtreeTxnState(p->pOrigBtree);
}
int origBtreeCreateTableVt(Btree *p, Pgno *piTable, int flags){
  return origBtreeCreateTable(p->pOrigBtree, piTable, flags);
}
int origBtreeDropTableVt(Btree *p, int iTable, int *piMoved){
  return origBtreeDropTable(p->pOrigBtree, iTable, piMoved);
}
int origBtreeClearTableVt(Btree *p, int iTable, i64 *pnChange){
  return origBtreeClearTable(p->pOrigBtree, iTable, pnChange);
}
void origBtreeGetMetaVt(Btree *p, int idx, u32 *pValue){
  origBtreeGetMeta(p->pOrigBtree, idx, pValue);
}
int origBtreeUpdateMetaVt(Btree *p, int idx, u32 value){
  return origBtreeUpdateMeta(p->pOrigBtree, idx, value);
}
void *origBtreeSchemaVt(Btree *p, int nBytes, void (*xFree)(void*)){
  return (void*)origBtreeSchema(p->pOrigBtree, nBytes, xFree);
}
int origBtreeSchemaLockedVt(Btree *p){
  return origBtreeSchemaLocked(p->pOrigBtree);
}
int origBtreeLockTableVt(Btree *p, int iTab, u8 isWriteLock){
  return origBtreeLockTable(p->pOrigBtree, iTab, isWriteLock);
}
int origBtreeCursorVt(Btree *p, Pgno iTable, int wrFlag,
                             struct KeyInfo *pKeyInfo, BtCursor *pCur){
  void *pOC = sqlite3_malloc(origBtreeCursorSize());
  int rc;
  if( !pOC ) return SQLITE_NOMEM;
  memset(pOC, 0, origBtreeCursorSize());
  pCur->pOrigCursor = pOC;
  pCur->pCurOps = &origCursorVtOps;
  pCur->pBtree = p;
  rc = origBtreeCursor(p->pOrigBtree, iTable, wrFlag, pKeyInfo, pOC);
  if( rc!=SQLITE_OK ){
    sqlite3_free(pOC);
    pCur->pOrigCursor = 0;
  }
  return rc;
}
void origBtreeEnterVt(Btree *p){
  origBtreeEnter(p->pOrigBtree);
}
void origBtreeLeaveVt(Btree *p){
  origBtreeLeave(p->pOrigBtree);
}
struct Pager *origBtreePagerVt(Btree *p){
  return (struct Pager*)origBtreePager(p->pOrigBtree);
}
#ifdef SQLITE_DEBUG
int origBtreeClosesWithCursorVt(Btree *p, BtCursor *pCur){
  (void)p; (void)pCur;
  return 1;
}
#endif

int origCursorClearTableOfCursorVt(BtCursor *pCur){
  return origBtreeClearTableOfCursor(pCur->pOrigCursor);
}
int origCursorCloseCursorVt(BtCursor *pCur){
  Btree *pWrapper = pCur->pBtree;
  int willAutoCloseInner;
  int rc;
  if( pCur->pOrigCursor==0 ) return SQLITE_OK;
  willAutoCloseInner = origBtreeCursorIsLastOnSingle(pCur->pOrigCursor);
  rc = origBtreeCloseCursor(pCur->pOrigCursor);
  sqlite3_free(pCur->pOrigCursor);
  pCur->pOrigCursor = 0;
  if( willAutoCloseInner && pWrapper ){
    pWrapper->pOrigBtree = 0;
    sqlite3_free(pWrapper);
  }
  return rc;
}
int origCursorCursorHasMovedVt(BtCursor *pCur){
  return origBtreeCursorHasMoved(pCur->pOrigCursor);
}
int origCursorCursorRestoreVt(BtCursor *pCur, int *pDifferentRow){
  return origBtreeCursorRestore(pCur->pOrigCursor, pDifferentRow);
}
int origCursorFirstVt(BtCursor *pCur, int *pRes){
  return origBtreeFirst(pCur->pOrigCursor, pRes);
}
int origCursorLastVt(BtCursor *pCur, int *pRes){
  return origBtreeLast(pCur->pOrigCursor, pRes);
}
int origCursorNextVt(BtCursor *pCur, int flags){
  return origBtreeNext(pCur->pOrigCursor, flags);
}
int origCursorPreviousVt(BtCursor *pCur, int flags){
  return origBtreePrevious(pCur->pOrigCursor, flags);
}
int origCursorEofVt(BtCursor *pCur){
  return origBtreeEof(pCur->pOrigCursor);
}
int origCursorIsEmptyVt(BtCursor *pCur, int *pRes){
  return origBtreeIsEmpty(pCur->pOrigCursor, pRes);
}
int origCursorTableMovetoVt(BtCursor *pCur, i64 intKey, int bias, int *pRes){
  return origBtreeTableMoveto(pCur->pOrigCursor, intKey, bias, pRes);
}
int origCursorIndexMovetoVt(BtCursor *pCur, UnpackedRecord *pIdxKey, int *pRes){
  return origBtreeIndexMoveto(pCur->pOrigCursor, pIdxKey, pRes);
}
i64 origCursorIntegerKeyVt(BtCursor *pCur){
  return origBtreeIntegerKey(pCur->pOrigCursor);
}
u32 origCursorPayloadSizeVt(BtCursor *pCur){
  return origBtreePayloadSize(pCur->pOrigCursor);
}
int origCursorPayloadVt(BtCursor *pCur, u32 offset, u32 amt, void *pBuf){
  return origBtreePayload(pCur->pOrigCursor, offset, amt, pBuf);
}
const void *origCursorPayloadFetchVt(BtCursor *pCur, u32 *pAmt){
  return origBtreePayloadFetch(pCur->pOrigCursor, pAmt);
}
sqlite3_int64 origCursorMaxRecordSizeVt(BtCursor *pCur){
  return origBtreeMaxRecordSize(pCur->pOrigCursor);
}
i64 origCursorOffsetVt(BtCursor *pCur){
  (void)pCur;
  return -1;
}
int origCursorInsertVt(BtCursor *pCur, const BtreePayload *pPayload, int flags, int seekResult){
  return origBtreeInsert(pCur->pOrigCursor, pPayload, flags, seekResult);
}
int origCursorDeleteVt(BtCursor *pCur, u8 flags){
  return origBtreeDelete(pCur->pOrigCursor, flags);
}
int origCursorTransferRowVt(BtCursor *pDest, BtCursor *pSrc, i64 iKey){
  return origBtreeTransferRow(pDest->pOrigCursor, pSrc->pOrigCursor, iKey);
}
void origCursorClearCursorVt(BtCursor *pCur){
  /* OP_NullRow must invalidate underlying orig cursors too. */
  origBtreeClearCursor(pCur->pOrigCursor);
}
int origCursorCountVt(sqlite3 *db, BtCursor *pCur, i64 *pnEntry){
  return origBtreeCount(db, pCur->pOrigCursor, pnEntry);
}
int origCursorCountRangeVt(
  sqlite3 *db,
  BtCursor *pCur,
  i64 iLower,
  i64 iUpper,
  i64 *pnEntry
){
  int rc;
  int res = 0;
  i64 n = 0;
  (void)db;

  *pnEntry = 0;
  if( iLower>iUpper ) return SQLITE_OK;
  rc = origBtreeTableMoveto(pCur->pOrigCursor, iLower, 0, &res);
  if( rc!=SQLITE_OK ) return rc;
  if( res!=0 && origBtreeEof(pCur->pOrigCursor) ){
    return SQLITE_OK;
  }
  while( !origBtreeEof(pCur->pOrigCursor) ){
    if( origBtreeIntegerKey(pCur->pOrigCursor)>iUpper ) break;
    n++;
    rc = origBtreeNext(pCur->pOrigCursor, 0);
    if( rc!=SQLITE_OK ) return rc;
  }
  *pnEntry = n;
  return SQLITE_OK;
}
int origCursorCountIndexRangeVt(
  sqlite3 *db,
  BtCursor *pCur,
  UnpackedRecord *pLower,
  UnpackedRecord *pUpper,
  i64 *pnEntry
){
  return origBtreeCountIndexRange(db, pCur->pOrigCursor,
                                  pLower, pUpper, pnEntry);
}
i64 origCursorRowCountEstVt(BtCursor *pCur){
  (void)pCur;
  return -1;
}
void origCursorCursorPinVt(BtCursor *pCur){
  origBtreeCursorPin(pCur->pOrigCursor);
}
void origCursorCursorUnpinVt(BtCursor *pCur){
  origBtreeCursorUnpin(pCur->pOrigCursor);
}
void origCursorCursorHintFlagsVt(BtCursor *pCur, unsigned x){
  (void)pCur; (void)x;

}
int origCursorCursorHasHintVt(BtCursor *pCur, unsigned int mask){
  (void)pCur; (void)mask;
  return 0;
}
#ifndef SQLITE_OMIT_INCRBLOB
int origCursorPayloadCheckedVt(BtCursor *pCur, u32 offset, u32 amt, void *pBuf){
  return origBtreePayloadChecked(pCur->pOrigCursor, offset, amt, pBuf);
}
int origCursorPutDataVt(BtCursor *pCur, u32 offset, u32 amt, void *pBuf){
  (void)pCur; (void)offset; (void)amt; (void)pBuf;
  return SQLITE_OK;
}
void origCursorIncrblobCursorVt(BtCursor *pCur){
  (void)pCur;

}
#endif
#ifndef NDEBUG
int origCursorCursorIsValidVt(BtCursor *pCur){
  (void)pCur;
  return 1;
}
#endif
int origCursorCursorIsValidNNVt(BtCursor *pCur){
  return origBtreeCursorIsValidNN(pCur->pOrigCursor);
}

extern int doltliteRegister(sqlite3 *db);


#endif /* DOLTLITE_PROLLY */
