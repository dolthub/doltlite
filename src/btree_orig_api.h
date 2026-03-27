/*
** Public API for the original SQLite btree, compiled with renamed symbols.
** All functions take/return void* for Btree and BtCursor to avoid struct
** conflicts with prolly_btree.c's definitions.
**
** These are implemented in btree_orig_api.c.
*/
#ifndef BTREE_ORIG_API_H
#define BTREE_ORIG_API_H

#include "sqliteInt.h"

int origBtreeOpen(sqlite3_vfs *pVfs, const char *zFilename,
                  sqlite3 *db, void **ppBtree, int flags, int vfsFlags);
int origBtreeClose(void *pBtree);
int origBtreeNewDb(void *pBtree);
int origBtreeSetCacheSize(void *p, int mxPage);
int origBtreeSetSpillSize(void *p, int mxPage);
int origBtreeSetMmapLimit(void *p, sqlite3_int64 szMmap);
int origBtreeSetPagerFlags(void *p, unsigned pgFlags);
int origBtreeSetPageSize(void *p, int nPagesize, int nReserve, int eFix);
int origBtreeGetPageSize(void *p);
Pgno origBtreeMaxPageCount(void *p, Pgno mxPage);
Pgno origBtreeLastPage(void *p);
int origBtreeSecureDelete(void *p, int newFlag);
int origBtreeGetRequestedReserve(void *p);
int origBtreeGetReserveNoMutex(void *p);
int origBtreeSetAutoVacuum(void *p, int autoVacuum);
int origBtreeGetAutoVacuum(void *p);
int origBtreeIncrVacuum(void *p);
const char *origBtreeGetFilename(void *p);
const char *origBtreeGetJournalname(void *p);
int origBtreeIsReadonly(void *p);
int origBtreeBeginTrans(void *p, int wrFlag, int *pSchemaVersion);
int origBtreeCommitPhaseOne(void *p, const char *zSuperJrnl);
int origBtreeCommitPhaseTwo(void *p, int bCleanup);
int origBtreeCommit(void *p);
int origBtreeRollback(void *p, int tripCode, int writeOnly);
int origBtreeBeginStmt(void *p, int iStatement);
int origBtreeSavepoint(void *p, int op, int iSavepoint);
int origBtreeTxnState(void *p);
int origBtreeCreateTable(void *p, Pgno *piTable, int flags);
int origBtreeDropTable(void *p, int iTable, int *piMoved);
int origBtreeClearTable(void *p, int iTable, i64 *pnChange);
void origBtreeGetMeta(void *p, int idx, u32 *pValue);
int origBtreeUpdateMeta(void *p, int idx, u32 value);
int origBtreeSchemaLocked(void *p);
int origBtreeLockTable(void *p, int iTab, u8 isWriteLock);
Schema *origBtreeSchema(void *p, int nBytes, void(*xFree)(void*));
int origBtreeCursor(void *p, Pgno iTable, int wrFlag,
                    struct KeyInfo *pKeyInfo, void *pCur);
int origBtreeCloseCursor(void *pCur);
int origBtreeCursorHasMoved(void *pCur);
int origBtreeCursorRestore(void *pCur, int *pDifferentRow);
int origBtreeFirst(void *pCur, int *pRes);
int origBtreeLast(void *pCur, int *pRes);
int origBtreeNext(void *pCur, int flags);
int origBtreePrevious(void *pCur, int flags);
int origBtreeEof(void *pCur);
int origBtreeTableMoveto(void *pCur, i64 intKey, int biasRight, int *pRes);
int origBtreeIndexMoveto(void *pCur, UnpackedRecord *pIdxKey, int *pRes);
int origBtreeInsert(void *pCur, const BtreePayload *pPayload,
                    int flags, int seekResult);
int origBtreeDelete(void *pCur, u8 flags);
int origBtreePayloadSize(void *pCur);
int origBtreePayload(void *pCur, u32 offset, u32 amt, void *pBuf);
const void *origBtreePayloadFetch(void *pCur, u32 *pAmt);
i64 origBtreeIntegerKey(void *pCur);
u32 origBtreePayloadChecked(void *pCur, u32 offset, u32 amt, void *pBuf);
int origBtreeCount(sqlite3 *db, void *pCur, i64 *pnEntry);
int origBtreeClosesWithCursor(void *p, void *pCur);
void origBtreeTripAllCursors(void *p, int errCode, int writeOnly);
void origBtreeCursorPin(void *pCur);
void origBtreeCursorUnpin(void *pCur);
int origBtreeTransferRow(void *pDest, void *pSrc, i64 iKey);
void origBtreeEnterAll(sqlite3 *db);
void origBtreeLeaveAll(sqlite3 *db);
int origBtreeIsEmpty(void *pCur, int *pRes);
int origBtreeClearTableOfCursor(void *pCur);
int origBtreeMaxRecordSize(void *pCur);
void origBtreeCursorHint(void *pCur, unsigned int mask, ...);

int origBtreeCursorSize(void);
void origBtreeEnter(void *p);
void origBtreeLeave(void *p);
void *origBtreePager(void *p);

/* Detect if a file is standard SQLite format (returns 1) or doltlite (0) */
int origBtreeIsSqliteFile(const char *zFilename);

#endif /* BTREE_ORIG_API_H */
