/*
** Bridge between the opaque void* API (btree_orig_api.h) and the
** renamed original SQLite btree functions (orig_sqlite3Btree*).
**
** This file is compiled WITH the btree_orig_prefix.h renames active,
** so it can reference the original Btree/BtCursor structs directly.
*/
#ifdef DOLTLITE_PROLLY

#include "btree_orig_prefix.h"
#include "sqliteInt.h"
#include "btreeInt.h"

#include <stdio.h>
#include <string.h>

/* Cast helpers */
#define B(p) ((Btree*)(p))
#define C(p) ((BtCursor*)(p))

int origBtreeOpen(sqlite3_vfs *pVfs, const char *zFilename,
                  sqlite3 *db, void **ppBtree, int flags, int vfsFlags){
  return orig_sqlite3BtreeOpen(pVfs, zFilename, db, (Btree**)ppBtree,
                               flags, vfsFlags);
}
int origBtreeClose(void *p){ return orig_sqlite3BtreeClose(B(p)); }
int origBtreeNewDb(void *p){ return orig_sqlite3BtreeNewDb(B(p)); }
int origBtreeSetCacheSize(void *p, int n){ return orig_sqlite3BtreeSetCacheSize(B(p),n); }
int origBtreeSetSpillSize(void *p, int n){ return orig_sqlite3BtreeSetSpillSize(B(p),n); }
int origBtreeSetMmapLimit(void *p, sqlite3_int64 sz){ return orig_sqlite3BtreeSetMmapLimit(B(p),sz); }
int origBtreeSetPagerFlags(void *p, unsigned f){ return orig_sqlite3BtreeSetPagerFlags(B(p),f); }
int origBtreeSetPageSize(void *p, int n, int r, int e){ return orig_sqlite3BtreeSetPageSize(B(p),n,r,e); }
int origBtreeGetPageSize(void *p){ return orig_sqlite3BtreeGetPageSize(B(p)); }
Pgno origBtreeMaxPageCount(void *p, Pgno mx){ return orig_sqlite3BtreeMaxPageCount(B(p),mx); }
Pgno origBtreeLastPage(void *p){ return orig_sqlite3BtreeLastPage(B(p)); }
int origBtreeSecureDelete(void *p, int f){ return orig_sqlite3BtreeSecureDelete(B(p),f); }
int origBtreeGetRequestedReserve(void *p){ return orig_sqlite3BtreeGetRequestedReserve(B(p)); }
int origBtreeGetReserveNoMutex(void *p){ return orig_sqlite3BtreeGetReserveNoMutex(B(p)); }
int origBtreeSetAutoVacuum(void *p, int a){ return orig_sqlite3BtreeSetAutoVacuum(B(p),a); }
int origBtreeGetAutoVacuum(void *p){ return orig_sqlite3BtreeGetAutoVacuum(B(p)); }
int origBtreeIncrVacuum(void *p){ return orig_sqlite3BtreeIncrVacuum(B(p)); }
const char *origBtreeGetFilename(void *p){ return orig_sqlite3BtreeGetFilename(B(p)); }
const char *origBtreeGetJournalname(void *p){ return orig_sqlite3BtreeGetJournalname(B(p)); }
int origBtreeIsReadonly(void *p){ return orig_sqlite3BtreeIsReadonly(B(p)); }
int origBtreeBeginTrans(void *p, int wr, int *pSV){ return orig_sqlite3BtreeBeginTrans(B(p),wr,pSV); }
int origBtreeCommitPhaseOne(void *p, const char *z){ return orig_sqlite3BtreeCommitPhaseOne(B(p),z); }
int origBtreeCommitPhaseTwo(void *p, int c){ return orig_sqlite3BtreeCommitPhaseTwo(B(p),c); }
int origBtreeCommit(void *p){ return orig_sqlite3BtreeCommit(B(p)); }
int origBtreeRollback(void *p, int t, int w){ return orig_sqlite3BtreeRollback(B(p),t,w); }
int origBtreeBeginStmt(void *p, int i){ return orig_sqlite3BtreeBeginStmt(B(p),i); }
int origBtreeSavepoint(void *p, int op, int i){ return orig_sqlite3BtreeSavepoint(B(p),op,i); }
int origBtreeTxnState(void *p){ return orig_sqlite3BtreeTxnState(B(p)); }
int origBtreeCreateTable(void *p, Pgno *pi, int f){ return orig_sqlite3BtreeCreateTable(B(p),pi,f); }
int origBtreeDropTable(void *p, int i, int *pm){ return orig_sqlite3BtreeDropTable(B(p),i,pm); }
int origBtreeClearTable(void *p, int i, i64 *pn){ return orig_sqlite3BtreeClearTable(B(p),i,pn); }
void origBtreeGetMeta(void *p, int i, u32 *pv){ orig_sqlite3BtreeGetMeta(B(p),i,pv); }
int origBtreeUpdateMeta(void *p, int i, u32 v){ return orig_sqlite3BtreeUpdateMeta(B(p),i,v); }
int origBtreeSchemaLocked(void *p){ return orig_sqlite3BtreeSchemaLocked(B(p)); }
int origBtreeLockTable(void *p, int t, u8 w){ return orig_sqlite3BtreeLockTable(B(p),t,w); }

Schema *origBtreeSchema(void *p, int nBytes, void(*xFree)(void*)){
  return orig_sqlite3BtreeSchema(B(p), nBytes, xFree);
}

int origBtreeCursor(void *p, Pgno iTable, int wrFlag,
                    struct KeyInfo *pKeyInfo, void *pCur){
  return orig_sqlite3BtreeCursor(B(p), iTable, wrFlag, pKeyInfo, C(pCur));
}
int origBtreeCloseCursor(void *pCur){ return orig_sqlite3BtreeCloseCursor(C(pCur)); }
int origBtreeCursorHasMoved(void *pCur){ return orig_sqlite3BtreeCursorHasMoved(C(pCur)); }
int origBtreeCursorRestore(void *pCur, int *pd){ return orig_sqlite3BtreeCursorRestore(C(pCur),pd); }
int origBtreeFirst(void *pCur, int *pRes){ return orig_sqlite3BtreeFirst(C(pCur),pRes); }
int origBtreeLast(void *pCur, int *pRes){ return orig_sqlite3BtreeLast(C(pCur),pRes); }
int origBtreeNext(void *pCur, int f){ return orig_sqlite3BtreeNext(C(pCur),f); }
int origBtreePrevious(void *pCur, int f){ return orig_sqlite3BtreePrevious(C(pCur),f); }
int origBtreeEof(void *pCur){ return orig_sqlite3BtreeEof(C(pCur)); }

int origBtreeTableMoveto(void *pCur, i64 intKey, int biasRight, int *pRes){
  return orig_sqlite3BtreeTableMoveto(C(pCur), (u64)intKey, biasRight, pRes);
}
int origBtreeIndexMoveto(void *pCur, UnpackedRecord *pIdxKey, int *pRes){
  return orig_sqlite3BtreeIndexMoveto(C(pCur), pIdxKey, pRes);
}
int origBtreeInsert(void *pCur, const BtreePayload *pPayload,
                    int flags, int seekResult){
  return orig_sqlite3BtreeInsert(C(pCur), pPayload, flags, seekResult);
}
int origBtreeDelete(void *pCur, u8 flags){
  return orig_sqlite3BtreeDelete(C(pCur), flags);
}
int origBtreePayloadSize(void *pCur){ return orig_sqlite3BtreePayloadSize(C(pCur)); }
int origBtreePayload(void *pCur, u32 off, u32 amt, void *pBuf){
  return orig_sqlite3BtreePayload(C(pCur), off, amt, pBuf);
}
const void *origBtreePayloadFetch(void *pCur, u32 *pAmt){
  return orig_sqlite3BtreePayloadFetch(C(pCur), pAmt);
}
i64 origBtreeIntegerKey(void *pCur){ return orig_sqlite3BtreeIntegerKey(C(pCur)); }
u32 origBtreePayloadChecked(void *pCur, u32 off, u32 amt, void *pBuf){
  return orig_sqlite3BtreePayloadChecked(C(pCur), off, amt, pBuf);
}
int origBtreeCount(sqlite3 *db, void *pCur, i64 *pn){
  return orig_sqlite3BtreeCount(db, C(pCur), pn);
}
int origBtreeClosesWithCursor(void *p, void *pCur){
  /* Only used in assert() — compiled out in NDEBUG builds */
  (void)p; (void)pCur;
  return 1;
}
void origBtreeTripAllCursors(void *p, int e, int w){
  orig_sqlite3BtreeTripAllCursors(B(p), e, w);
}
void origBtreeCursorPin(void *pCur){ orig_sqlite3BtreeCursorPin(C(pCur)); }
void origBtreeCursorUnpin(void *pCur){ orig_sqlite3BtreeCursorUnpin(C(pCur)); }
int origBtreeTransferRow(void *pDest, void *pSrc, i64 iKey){
  return orig_sqlite3BtreeTransferRow(C(pDest), C(pSrc), iKey);
}
void origBtreeEnterAll(sqlite3 *db){ orig_sqlite3BtreeEnterAll(db); }
void origBtreeLeaveAll(sqlite3 *db){ orig_sqlite3BtreeLeaveAll(db); }
int origBtreeIsEmpty(void *pCur, int *pRes){
  return orig_sqlite3BtreeIsEmpty(C(pCur), pRes);
}
int origBtreeClearTableOfCursor(void *pCur){
  return orig_sqlite3BtreeClearTableOfCursor(C(pCur));
}
int origBtreeMaxRecordSize(void *pCur){
  return orig_sqlite3BtreeMaxRecordSize(C(pCur));
}
void origBtreeCursorHint(void *pCur, unsigned int mask, ...){
  /* Hints are advisory — safe to ignore for dispatch simplicity */
  (void)pCur; (void)mask;
}

int origBtreeCursorSize(void){ return orig_sqlite3BtreeCursorSize(); }
void origBtreeEnter(void *p){ orig_sqlite3BtreeEnter(B(p)); }
void origBtreeLeave(void *p){ orig_sqlite3BtreeLeave(B(p)); }
void *origBtreePager(void *p){ return orig_sqlite3BtreePager(B(p)); }

/*
** Detect if a file is a standard SQLite database by reading the header.
** Returns 1 if the file starts with "SQLite format 3\0", 0 otherwise.
*/
int origBtreeIsSqliteFile(const char *zFilename){
  FILE *f;
  char buf[16];
  if( !zFilename || zFilename[0]=='\0' || strcmp(zFilename,":memory:")==0 ){
    return 0;
  }
  f = fopen(zFilename, "rb");
  if( !f ) return 0;  /* File doesn't exist yet — will be doltlite */
  if( fread(buf, 1, 16, f) < 16 ){
    fclose(f);
    return 0;  /* Too small — empty file, will be doltlite */
  }
  fclose(f);
  return memcmp(buf, "SQLite format 3\000", 16)==0;
}

#endif /* DOLTLITE_PROLLY */
