#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "sqliteInt.h"
#include "vdbeInt.h"
#include "prolly_btree_int.h"

static int nPass = 0;
static int nFail = 0;
static sqlite3_mem_methods gRealMem;
static int gFailNextAlloc = 0;

static void *oomMalloc(int n){
  if( gFailNextAlloc>0 ){
    gFailNextAlloc--;
    if( gFailNextAlloc==0 ) return 0;
  }
  return gRealMem.xMalloc(n);
}

static void *oomRealloc(void *p, int n){
  if( gFailNextAlloc>0 ){
    gFailNextAlloc--;
    if( gFailNextAlloc==0 ) return 0;
  }
  return gRealMem.xRealloc(p, n);
}

static void check(const char *zName, int cond){
  if( cond ){
    nPass++;
  }else{
    nFail++;
    fprintf(stderr, "FAIL: %s\n", zName);
  }
}

static void checkStr(const char *zName, const char *zGot, const char *zWant){
  if( zGot && zWant && strcmp(zGot, zWant)==0 ){
    nPass++;
  }else{
    nFail++;
    fprintf(stderr, "FAIL: %s: got '%s', want '%s'\n",
            zName, zGot ? zGot : "(null)", zWant ? zWant : "(null)");
  }
}

static int execSql(sqlite3 *db, const char *zSql){
  char *zErr = 0;
  int rc = sqlite3_exec(db, zSql, 0, 0, &zErr);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "SQL error: %s [%s]\n", zErr ? zErr : "?", zSql);
    sqlite3_free(zErr);
  }
  return rc;
}

static void removeDb(const char *zPath){
  char zLock[512];
  remove(zPath);
  sqlite3_snprintf(sizeof(zLock), zLock, ".%s-lock", zPath);
  remove(zLock);
}

static int makeTextIntRecord(u8 *aBuf, const char *zKey, int val){
  int nKey = (int)strlen(zKey);
  aBuf[0] = 0x03;
  aBuf[1] = (u8)(13 + 2 * nKey);
  aBuf[2] = 0x01;
  memcpy(aBuf + 3, zKey, (size_t)nKey);
  aBuf[3 + nKey] = (u8)(val & 0xff);
  return 4 + nKey;
}

static int readCursorKey(BtCursor *pCur, char *zOut, int nOut){
  u8 aBuf[128];
  u32 nPayload;
  int rc;
  int hdrSize;
  int st0;
  int nText;

  nPayload = sqlite3BtreePayloadSize(pCur);
  if( nPayload<3 || nPayload>(u32)sizeof(aBuf) ) return SQLITE_CORRUPT;
  rc = sqlite3BtreePayload(pCur, 0, nPayload, aBuf);
  if( rc!=SQLITE_OK ) return rc;
  hdrSize = aBuf[0];
  st0 = aBuf[1];
  if( st0<13 || (st0 & 1)==0 ) return SQLITE_CORRUPT;
  nText = (st0 - 13) / 2;
  if( hdrSize + nText > (int)nPayload || nText >= nOut ) return SQLITE_CORRUPT;
  memcpy(zOut, aBuf + hdrSize, (size_t)nText);
  zOut[nText] = '\0';
  return SQLITE_OK;
}

static int insertSavePos(BtCursor *pCur, const char *zKey, int val){
  u8 aRec[128];
  BtreePayload payload;
  int nRec = makeTextIntRecord(aRec, zKey, val);
  memset(&payload, 0, sizeof(payload));
  payload.pKey = aRec;
  payload.nKey = nRec;
  return sqlite3BtreeInsert(pCur, &payload, BTREE_SAVEPOSITION, 0);
}

static void checkRemainingForward(
  const char *zPrefix,
  BtCursor *pCur,
  const char **azWant,
  int nWant
){
  int i;
  int rc;
  char zGot[64];
  char zLabel[128];

  for(i=0; i<nWant; i++){
    rc = sqlite3BtreeNext(pCur, 0);
    sqlite3_snprintf(sizeof(zLabel), zLabel, "%s: Next[%d] rc", zPrefix, i);
    check(zLabel, rc==SQLITE_OK);
    if( rc!=SQLITE_OK ) return;
    rc = readCursorKey(pCur, zGot, (int)sizeof(zGot));
    sqlite3_snprintf(sizeof(zLabel), zLabel, "%s: Next[%d] key", zPrefix, i);
    check(zLabel, rc==SQLITE_OK);
    if( rc==SQLITE_OK ) checkStr(zLabel, zGot, azWant[i]);
  }
  rc = sqlite3BtreeNext(pCur, 0);
  sqlite3_snprintf(sizeof(zLabel), zLabel, "%s: Next EOF", zPrefix);
  check(zLabel, rc==SQLITE_DONE);
}

static void checkRemainingBackward(
  const char *zPrefix,
  BtCursor *pCur,
  const char **azWant,
  int nWant
){
  int i;
  int rc;
  char zGot[64];
  char zLabel[128];

  for(i=0; i<nWant; i++){
    rc = sqlite3BtreePrevious(pCur, 0);
    sqlite3_snprintf(sizeof(zLabel), zLabel, "%s: Prev[%d] rc", zPrefix, i);
    check(zLabel, rc==SQLITE_OK);
    if( rc!=SQLITE_OK ) return;
    rc = readCursorKey(pCur, zGot, (int)sizeof(zGot));
    sqlite3_snprintf(sizeof(zLabel), zLabel, "%s: Prev[%d] key", zPrefix, i);
    check(zLabel, rc==SQLITE_OK);
    if( rc==SQLITE_OK ) checkStr(zLabel, zGot, azWant[i]);
  }
  rc = sqlite3BtreePrevious(pCur, 0);
  sqlite3_snprintf(sizeof(zLabel), zLabel, "%s: Prev BOF", zPrefix);
  check(zLabel, rc==SQLITE_DONE);
}

static void runNonIntKeySuite(
  const char *zName,
  const char *zSchema,
  const char *zSeedSql,
  const char *zTable,
  const char *zIndex
){
  sqlite3 *db = 0;
  Btree *pBt;
  BtCursor *pCur = 0;
  Table *pTab;
  Index *pIdx;
  KeyInfo *pKeyInfo = 0;
  Parse sParse;
  Pgno root = 0;
  int rc;
  int res = 0;
  char zDb[128];
  char zGot[64];
  char zLabel[128];

  sqlite3_snprintf(sizeof(zDb), zDb, "savepos_%s.db", zName);
  removeDb(zDb);
  rc = sqlite3_open(zDb, &db);
  check("open", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ){ sqlite3_close(db); return; }

  if( execSql(db, zSchema)!=SQLITE_OK || execSql(db, zSeedSql)!=SQLITE_OK ){
    nFail++;
    sqlite3_close(db);
    removeDb(zDb);
    return;
  }

  sqlite3BtreeEnterAll(db);
  pBt = db->aDb[0].pBt;
  pTab = sqlite3FindTable(db, zTable, "main");
  pIdx = zIndex ? sqlite3FindIndex(db, zIndex, "main")
                : (pTab ? sqlite3PrimaryKeyIndex(pTab) : 0);
  if( pIdx ){
    root = pIdx->tnum;
    memset(&sParse, 0, sizeof(sParse));
    sParse.db = db;
    pKeyInfo = sqlite3KeyInfoOfIndex(&sParse, pIdx);
  }
  check("resolved non-intkey root and KeyInfo", pBt!=0 && root>1 && pKeyInfo!=0);
  if( !pBt || root<=1 || !pKeyInfo ){
    sqlite3KeyInfoUnref(pKeyInfo);
    sqlite3BtreeLeaveAll(db);
    sqlite3_close(db);
    removeDb(zDb);
    return;
  }

  pCur = sqlite3_malloc(sqlite3BtreeCursorSize());
  check("alloc cursor", pCur!=0);
  if( !pCur ){
    sqlite3KeyInfoUnref(pKeyInfo);
    sqlite3BtreeLeaveAll(db);
    sqlite3_close(db);
    removeDb(zDb);
    return;
  }
  memset(pCur, 0, sqlite3BtreeCursorSize());

  check("begin write txn", sqlite3BtreeBeginTrans(pBt, 1, 0)==SQLITE_OK);
  check("open write cursor",
        sqlite3BtreeCursor(pBt, root, BTREE_WRCSR, pKeyInfo, pCur)==SQLITE_OK);

  /* Case 1: tree cursor sits behind inserted key ('b' < 'e'), then Next. */
  check("case1: First", sqlite3BtreeFirst(pCur, &res)==SQLITE_OK && res==0);
  check("case1: insert 'e'", insertSavePos(pCur, "e", 5)==SQLITE_OK);
  check("case1: read cur", readCursorKey(pCur, zGot, sizeof(zGot))==SQLITE_OK);
  sqlite3_snprintf(sizeof(zLabel), zLabel, "%s case1 cur", zName);
  checkStr(zLabel, zGot, "e");
  {
    const char *aWant[] = { "f", "h" };
    sqlite3_snprintf(sizeof(zLabel), zLabel, "%s case1", zName);
    checkRemainingForward(zLabel, pCur, aWant, 2);
  }

  /* Case 2: tree cursor is at EOF, insert 'c', then Next. */
  check("case2: Last", sqlite3BtreeLast(pCur, &res)==SQLITE_OK && res==0);
  check("case2: step to EOF", sqlite3BtreeNext(pCur, 0)==SQLITE_DONE);
  check("case2: insert 'c'", insertSavePos(pCur, "c", 3)==SQLITE_OK);
  check("case2: read cur", readCursorKey(pCur, zGot, sizeof(zGot))==SQLITE_OK);
  sqlite3_snprintf(sizeof(zLabel), zLabel, "%s case2 cur", zName);
  checkStr(zLabel, zGot, "c");
  {
    const char *aWant[] = { "d", "e", "f", "h" };
    sqlite3_snprintf(sizeof(zLabel), zLabel, "%s case2", zName);
    checkRemainingForward(zLabel, pCur, aWant, 4);
  }

  /* Case 3: tree cursor sits ahead of inserted key ('h' > 'c2'), then Previous. */
  check("case3: Last", sqlite3BtreeLast(pCur, &res)==SQLITE_OK && res==0);
  check("case3: insert 'c2'", insertSavePos(pCur, "c2", 32)==SQLITE_OK);
  check("case3: read cur", readCursorKey(pCur, zGot, sizeof(zGot))==SQLITE_OK);
  sqlite3_snprintf(sizeof(zLabel), zLabel, "%s case3 cur", zName);
  checkStr(zLabel, zGot, "c2");
  {
    const char *aWant[] = { "c", "b" };
    sqlite3_snprintf(sizeof(zLabel), zLabel, "%s case3", zName);
    checkRemainingBackward(zLabel, pCur, aWant, 2);
  }

  /* Case 4: cursor at Last (BTCF_AtLast), insert earlier key 'a', then Next. */
  check("case4: Last", sqlite3BtreeLast(pCur, &res)==SQLITE_OK && res==0);
  check("case4: insert 'a'", insertSavePos(pCur, "a", 1)==SQLITE_OK);
  check("case4: read cur", readCursorKey(pCur, zGot, sizeof(zGot))==SQLITE_OK);
  sqlite3_snprintf(sizeof(zLabel), zLabel, "%s case4 cur", zName);
  checkStr(zLabel, zGot, "a");
  {
    const char *aWant[] = { "b", "c", "c2", "d", "e", "f", "h" };
    sqlite3_snprintf(sizeof(zLabel), zLabel, "%s case4", zName);
    checkRemainingForward(zLabel, pCur, aWant, 7);
  }

  /* Case 5: overwrite committed key 'd' in place (MERGE_SRC_BOTH), then Next/Prev. */
  check("case5: First", sqlite3BtreeFirst(pCur, &res)==SQLITE_OK && res==0);
  while( readCursorKey(pCur, zGot, sizeof(zGot))==SQLITE_OK
      && strcmp(zGot, "d")!=0 ){
    rc = sqlite3BtreeNext(pCur, 0);
    if( rc!=SQLITE_OK ) break;
  }
  checkStr("case5: reached 'd'", zGot, "d");
  check("case5: overwrite 'd'",
        insertSavePos(pCur, "d", zIndex ? 4 : 44)==SQLITE_OK);
  check("case5: Next -> 'e'",
        sqlite3BtreeNext(pCur, 0)==SQLITE_OK
        && readCursorKey(pCur, zGot, sizeof(zGot))==SQLITE_OK);
  checkStr("case5: Next key", zGot, "e");
  check("case5: Prev -> 'd'",
        sqlite3BtreePrevious(pCur, 0)==SQLITE_OK
        && readCursorKey(pCur, zGot, sizeof(zGot))==SQLITE_OK);
  checkStr("case5: Prev1 key", zGot, "d");
  check("case5: Prev -> 'c2'",
        sqlite3BtreePrevious(pCur, 0)==SQLITE_OK
        && readCursorKey(pCur, zGot, sizeof(zGot))==SQLITE_OK);
  checkStr("case5: Prev2 key", zGot, "c2");

  sqlite3BtreeCloseCursor(pCur);
  sqlite3_free(pCur);
  check("commit", sqlite3BtreeCommit(pBt)==SQLITE_OK);
  sqlite3KeyInfoUnref(pKeyInfo);
  sqlite3BtreeLeaveAll(db);
  sqlite3_close(db);
  removeDb(zDb);
}

static void initIntSeekKey(
  UnpackedRecord *pRec,
  Mem *aMem,
  KeyInfo *pKeyInfo,
  i64 v0,
  int nField,
  i64 v1
){
  memset(pRec, 0, sizeof(*pRec));
  memset(aMem, 0, sizeof(Mem) * 2);
  aMem[0].flags = MEM_Int;
  aMem[0].u.i = v0;
  aMem[1].flags = MEM_Int;
  aMem[1].u.i = v1;
  pRec->pKeyInfo = pKeyInfo;
  pRec->aMem = aMem;
  pRec->nField = (u16)nField;
  pRec->default_rc = 0;
}

static void runCachedSeekKeyTests(void){
  sqlite3 *db = 0;
  Btree *pBt;
  BtCursor *pCur = 0;
  Index *pIdx;
  KeyInfo *pKeyInfo = 0;
  Parse sParse;
  Pgno root = 0;
  UnpackedRecord seekKey;
  Mem aMem[2];
  BtreePayload payload;
  u8 aRec[128];
  int nRec;
  int rc;
  int res = 0;
  int cmpRes = 0;
  const char *zDb = "savepos_cached_seek.db";

  removeDb(zDb);
  rc = sqlite3_open(zDb, &db);
  check("seekkey: open", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ){ sqlite3_close(db); return; }

  if( execSql(db,
        "CREATE TABLE t_sk(id INTEGER PRIMARY KEY, k);"
        "CREATE INDEX t_sk_k ON t_sk(k);"
        "INSERT INTO t_sk VALUES(1,10),(2,20),(3,30);")!=SQLITE_OK ){
    nFail++;
    sqlite3_close(db);
    removeDb(zDb);
    return;
  }

  sqlite3BtreeEnterAll(db);
  pBt = db->aDb[0].pBt;
  pIdx = sqlite3FindIndex(db, "t_sk_k", "main");
  if( pIdx ){
    root = pIdx->tnum;
    memset(&sParse, 0, sizeof(sParse));
    sParse.db = db;
    pKeyInfo = sqlite3KeyInfoOfIndex(&sParse, pIdx);
  }
  check("seekkey: resolved index", pBt!=0 && root>1 && pKeyInfo!=0);
  if( !pBt || root<=1 || !pKeyInfo ){
    sqlite3KeyInfoUnref(pKeyInfo);
    sqlite3BtreeLeaveAll(db);
    sqlite3_close(db);
    removeDb(zDb);
    return;
  }

  pCur = sqlite3_malloc(sqlite3BtreeCursorSize());
  check("seekkey: alloc cursor", pCur!=0);
  if( !pCur ){
    sqlite3KeyInfoUnref(pKeyInfo);
    sqlite3BtreeLeaveAll(db);
    sqlite3_close(db);
    removeDb(zDb);
    return;
  }
  memset(pCur, 0, sqlite3BtreeCursorSize());

  check("seekkey: begin write txn", sqlite3BtreeBeginTrans(pBt, 1, 0)==SQLITE_OK);
  check("seekkey: open cursor",
        sqlite3BtreeCursor(pBt, root, BTREE_WRCSR, pKeyInfo, pCur)==SQLITE_OK);

  /* ClearCursor must clear cached seek key metadata. */
  initIntSeekKey(&seekKey, aMem, pKeyInfo, 10, 2, 1);
  check("seekkey: moveto (10,1)",
        sqlite3BtreeIndexMoveto(pCur, &seekKey, &res)==SQLITE_OK && res==0);
  check("seekkey: cached after moveto",
        pCur->nSeekSortKey>0 && pCur->pSeekSortKey==pCur->aSeekSortKey);
  sqlite3BtreeClearCursor(pCur);
  check("seekkey: cleared by ClearCursor",
        pCur->nSeekSortKey==0 && pCur->nSeekKeyField==0);

  /* Insert overwrites pSeekSortKey scratch; cached seek metadata must clear
  ** so BTREE_SEEK_EQ compare and BTREE_AUXDELETE use the current row. */
  sqlite3BtreeCursorHintFlags(pCur, BTREE_SEEK_EQ);
  initIntSeekKey(&seekKey, aMem, pKeyInfo, 10, 1, 0);
  check("seekkey: moveto (10)",
        sqlite3BtreeIndexMoveto(pCur, &seekKey, &res)==SQLITE_OK && res==0);
  check("seekkey: prefix cached",
        pCur->nSeekSortKey>0 && pCur->nSeekKeyField==1
        && pCur->pSeekSortKey==pCur->aSeekSortKey);

  nRec = makeTextIntRecord(aRec, "zz", 9);
  memset(&payload, 0, sizeof(payload));
  payload.pKey = aRec;
  payload.nKey = nRec;
  check("seekkey: insert 'zz'",
        sqlite3BtreeInsert(pCur, &payload, BTREE_SAVEPOSITION, 0)==SQLITE_OK);
  check("seekkey: cleared after insert",
        pCur->nSeekSortKey==0 && pCur->nSeekKeyField==0);

  cmpRes = 0;
  check("seekkey: cached compare after insert",
        sqlite3BtreeProllyCachedIndexKeyCompare(pCur, &seekKey, 0, &cmpRes)==SQLITE_OK
        && cmpRes>0);
  check("seekkey: auxdelete after insert",
        sqlite3BtreeDelete(pCur, BTREE_AUXDELETE)==SQLITE_OK);
  check("seekkey: cleared after auxdelete",
        pCur->nSeekSortKey==0 && pCur->nSeekKeyField==0);

  /* Confirm (10,1) survived and ('zz',9) was deleted. */
  initIntSeekKey(&seekKey, aMem, pKeyInfo, 10, 2, 1);
  check("seekkey: (10,1) still present",
        sqlite3BtreeIndexMoveto(pCur, &seekKey, &res)==SQLITE_OK && res==0);

  /* OOM on insert after int seek (pSeekSortKey == aSeekSortKey):
  ** must not leave pSeekSortKey == NULL with nSeekSortKey > 0. */
  sqlite3BtreeCloseCursor(pCur);
  memset(pCur, 0, sqlite3BtreeCursorSize());
  check("seekkey: reopen cursor for insert OOM",
        sqlite3BtreeCursor(pBt, root, BTREE_WRCSR, pKeyInfo, pCur)==SQLITE_OK);
  sqlite3BtreeCursorHintFlags(pCur, BTREE_SEEK_EQ);
  initIntSeekKey(&seekKey, aMem, pKeyInfo, 10, 1, 0);
  check("seekkey: moveto (10) before insert OOM",
        sqlite3BtreeIndexMoveto(pCur, &seekKey, &res)==SQLITE_OK && res==0);
  check("seekkey: aSeekSortKey active before insert OOM",
        pCur->pSeekSortKey==pCur->aSeekSortKey && pCur->nSeekSortKey>0);

  nRec = makeTextIntRecord(aRec, "oom_insert", 11);
  memset(&payload, 0, sizeof(payload));
  payload.pKey = aRec;
  payload.nKey = nRec;
  gFailNextAlloc = 1;
  rc = sqlite3BtreeInsert(pCur, &payload, BTREE_SAVEPOSITION, 0);
  gFailNextAlloc = 0;
  check("seekkey: insert OOM returned NOMEM", rc==SQLITE_NOMEM);
  check("seekkey: insert OOM cleared seek key",
        pCur->pSeekSortKey==0 && pCur->nSeekSortKey==0 && pCur->nSeekKeyField==0);

  cmpRes = -1;
  check("seekkey: compare safe after insert OOM",
        sqlite3BtreeProllyCachedIndexKeyCompare(pCur, &seekKey, 0, &cmpRes)==SQLITE_OK
        && cmpRes==0);
  check("seekkey: auxdelete safe after insert OOM",
        sqlite3BtreeDelete(pCur, BTREE_AUXDELETE)==SQLITE_OK);
  check("seekkey: (10,1) deleted by auxdelete",
        sqlite3BtreeIndexMoveto(pCur, &seekKey, &res)==SQLITE_OK && res!=0);

  /* OOM on delete cached-payload path when pSeekSortKey == aSeekSortKey:
  ** must not leave pSeekSortKey == NULL with nSeekSortKey > 0. */
  sqlite3BtreeCloseCursor(pCur);
  memset(pCur, 0, sqlite3BtreeCursorSize());
  check("seekkey: reopen cursor for delete OOM",
        sqlite3BtreeCursor(pBt, root, BTREE_WRCSR, pKeyInfo, pCur)==SQLITE_OK);
  initIntSeekKey(&seekKey, aMem, pKeyInfo, 20, 1, 0);
  check("seekkey: moveto (20) before delete OOM",
        sqlite3BtreeIndexMoveto(pCur, &seekKey, &res)==SQLITE_OK && res==0);
  check("seekkey: aSeekSortKey active before delete OOM",
        pCur->pSeekSortKey==pCur->aSeekSortKey && pCur->nSeekSortKey>0
        && pCur->nSeekKeyField==1);

  nRec = makeTextIntRecord(aRec, "cached_del", 12);
  pCur->pCachedPayload = sqlite3_malloc(nRec);
  check("seekkey: alloc cached payload", pCur->pCachedPayload!=0);
  if( pCur->pCachedPayload ){
    memcpy(pCur->pCachedPayload, aRec, (size_t)nRec);
    pCur->nCachedPayload = nRec;
    pCur->cachedPayloadOwned = 1;
    prollyCursorClose(&pCur->pCur);
    pCur->mmActive = 0;

    gFailNextAlloc = 1;
    rc = sqlite3BtreeDelete(pCur, 0);
    gFailNextAlloc = 0;
    check("seekkey: delete OOM returned NOMEM", rc==SQLITE_NOMEM);
    check("seekkey: delete OOM cleared seek key",
          pCur->pSeekSortKey==0 && pCur->nSeekSortKey==0 && pCur->nSeekKeyField==0);
  }

  sqlite3BtreeCloseCursor(pCur);
  sqlite3_free(pCur);
  check("seekkey: commit", sqlite3BtreeCommit(pBt)==SQLITE_OK);
  sqlite3KeyInfoUnref(pKeyInfo);
  sqlite3BtreeLeaveAll(db);
  sqlite3_close(db);
  removeDb(zDb);
}

static void runAtLastSeekClearTests(void){
  sqlite3 *db = 0;
  Btree *pBt;
  BtCursor *pCur = 0;
  Table *pTab;
  Index *pIdx;
  KeyInfo *pKeyInfo = 0;
  Parse sParse;
  Pgno root = 0;
  UnpackedRecord seekKey;
  Mem aMem[2];
  BtreePayload payload;
  u8 aRec[128];
  u8 aFirstKey[128];
  int nFirstKey = 0;
  int rc;
  int res = 0;
  int exact = 0;
  char zGot[64];
  const char *zDb = "savepos_atlast_seek.db";

  removeDb(zDb);
  rc = sqlite3_open(zDb, &db);
  check("atlast: open", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ){ sqlite3_close(db); return; }

  if( execSql(db,
        "CREATE TABLE t_int(id INTEGER PRIMARY KEY, v TEXT);"
        "INSERT INTO t_int VALUES(10,'a'),(20,'b'),(30,'c');"
        "CREATE TABLE t_idx(id INTEGER PRIMARY KEY, k TEXT);"
        "CREATE INDEX t_idx_k ON t_idx(k);"
        "INSERT INTO t_idx VALUES(2,'b'),(4,'d'),(6,'f'),(8,'h');")!=SQLITE_OK ){
    nFail++;
    sqlite3_close(db);
    removeDb(zDb);
    return;
  }

  sqlite3BtreeEnterAll(db);
  pBt = db->aDb[0].pBt;
  pTab = sqlite3FindTable(db, "t_int", "main");
  pIdx = sqlite3FindIndex(db, "t_idx_k", "main");
  check("atlast: resolved table and index", pBt!=0 && pTab!=0 && pIdx!=0);
  if( !pBt || !pTab || !pIdx ){
    sqlite3BtreeLeaveAll(db);
    sqlite3_close(db);
    removeDb(zDb);
    return;
  }

  pCur = sqlite3_malloc(sqlite3BtreeCursorSize());
  check("atlast: alloc cursor", pCur!=0);
  if( !pCur ){
    sqlite3BtreeLeaveAll(db);
    sqlite3_close(db);
    removeDb(zDb);
    return;
  }
  memset(pCur, 0, sqlite3BtreeCursorSize());

  check("atlast: begin write txn", sqlite3BtreeBeginTrans(pBt, 1, 0)==SQLITE_OK);

  /* 1. Intkey table with pending mutmap edit:
  ** Last() sets BTCF_AtLast; TableMoveto() to an absent earlier key takes
  ** the deferred-miss path (mmActive=1), and Next() must visit all rows. */
  root = pTab->tnum;
  check("atlast int: open cursor",
        sqlite3BtreeCursor(pBt, root, BTREE_WRCSR, 0, pCur)==SQLITE_OK);
  {
    int nRec = makeTextIntRecord(aRec, "p", 25);
    memset(&payload, 0, sizeof(payload));
    payload.nKey = 25;
    payload.pData = aRec;
    payload.nData = nRec;
    check("atlast int: pending insert 25",
          sqlite3BtreeInsert(pCur, &payload, 0, 0)==SQLITE_OK);
  }
  check("atlast int: Last",
        sqlite3BtreeLast(pCur, &res)==SQLITE_OK && res==0);
  check("atlast int: Last key is 30", sqlite3BtreeIntegerKey(pCur)==30);
  check("atlast int: BTCF_AtLast set after Last",
        (pCur->curFlags & BTCF_AtLast)!=0);

  check("atlast int: TableMoveto(5) deferred miss",
        sqlite3BtreeTableMoveto(pCur, 5, 0, &res)==SQLITE_OK && res==-1);
  check("atlast int: BTCF_AtLast cleared after TableMoveto",
        (pCur->curFlags & BTCF_AtLast)==0);
  {
    i64 aWant[] = { 10, 20, 25, 30 };
    int i;
    for(i=0; i<4; i++){
      rc = sqlite3BtreeNext(pCur, 0);
      check("atlast int: Next rc", rc==SQLITE_OK);
      if( rc!=SQLITE_OK ) break;
      check("atlast int: Next key", sqlite3BtreeIntegerKey(pCur)==aWant[i]);
    }
    check("atlast int: Next EOF", sqlite3BtreeNext(pCur, 0)==SQLITE_DONE);
  }

  /* Also verify TableMoveto() to an existing committed key after Last(). */
  check("atlast int: Last before exact hit",
        sqlite3BtreeLast(pCur, &res)==SQLITE_OK && res==0);
  check("atlast int: TableMoveto(10) exact hit",
        sqlite3BtreeTableMoveto(pCur, 10, 0, &res)==SQLITE_OK && res==0);
  check("atlast int: BTCF_AtLast cleared on exact hit",
        (pCur->curFlags & BTCF_AtLast)==0);
  {
    i64 aWant[] = { 20, 25, 30 };
    int i;
    for(i=0; i<3; i++){
      rc = sqlite3BtreeNext(pCur, 0);
      check("atlast int exact: Next rc", rc==SQLITE_OK);
      if( rc!=SQLITE_OK ) break;
      check("atlast int exact: Next key", sqlite3BtreeIntegerKey(pCur)==aWant[i]);
    }
    check("atlast int exact: Next EOF", sqlite3BtreeNext(pCur, 0)==SQLITE_DONE);
  }
  sqlite3BtreeCloseCursor(pCur);

  /* 2. Non-intkey index with pending mutmap edit:
  ** Last() sets BTCF_AtLast; SeekMergedAtOrAfter() and IndexMoveto() must
  ** clear BTCF_AtLast so subsequent Next() visits all remaining rows. */
  memset(&sParse, 0, sizeof(sParse));
  sParse.db = db;
  pKeyInfo = sqlite3KeyInfoOfIndex(&sParse, pIdx);
  check("atlast idx: KeyInfo", pKeyInfo!=0);
  if( pKeyInfo ){
    const u8 *pKey = 0;
    int nKey = 0;
    memset(pCur, 0, sqlite3BtreeCursorSize());
    check("atlast idx: open cursor",
          sqlite3BtreeCursor(pBt, pIdx->tnum, BTREE_WRCSR, pKeyInfo, pCur)==SQLITE_OK);
    /* Capture sort key of first entry ('b') before adding pending edits. */
    check("atlast idx: First",
          sqlite3BtreeFirst(pCur, &res)==SQLITE_OK && res==0);
    check("atlast idx: capture first sort key",
          prollyBtCursorCurrentSortKey(pCur, &pKey, &nKey)==SQLITE_OK
          && nKey>0 && nKey<=(int)sizeof(aFirstKey));
    if( nKey>0 && nKey<=(int)sizeof(aFirstKey) ){
      memcpy(aFirstKey, pKey, (size_t)nKey);
      nFirstKey = nKey;
    }

    check("atlast idx: pending insert 'e'",
          insertSavePos(pCur, "e", 5)==SQLITE_OK);

    check("atlast idx: Last before SeekMergedAtOrAfter",
          sqlite3BtreeLast(pCur, &res)==SQLITE_OK && res==0);
    check("atlast idx: BTCF_AtLast set", (pCur->curFlags & BTCF_AtLast)!=0);
    check("atlast idx: SeekMergedAtOrAfter('b')",
          prollyBtCursorSeekMergedAtOrAfter(pCur, aFirstKey, nFirstKey, &exact)==SQLITE_OK
          && exact==1);
    check("atlast idx: BTCF_AtLast cleared by SeekMergedAtOrAfter",
          (pCur->curFlags & BTCF_AtLast)==0);
    check("atlast idx: current key is 'b'",
          readCursorKey(pCur, zGot, (int)sizeof(zGot))==SQLITE_OK);
    checkStr("atlast idx: current key str", zGot, "b");
    {
      const char *aWant[] = { "d", "e", "f", "h" };
      checkRemainingForward("atlast idx merged", pCur, aWant, 4);
    }

    /* Last() followed by IndexMoveto() to an earlier key, then Next(). */
    check("atlast idx: Last before IndexMoveto",
          sqlite3BtreeLast(pCur, &res)==SQLITE_OK && res==0);
    check("atlast idx: BTCF_AtLast set before IndexMoveto",
          (pCur->curFlags & BTCF_AtLast)!=0);
    memset(&seekKey, 0, sizeof(seekKey));
    memset(aMem, 0, sizeof(aMem));
    aMem[0].flags = MEM_Str | MEM_Term;
    aMem[0].z = "d";
    aMem[0].n = 1;
    aMem[0].enc = SQLITE_UTF8;
    seekKey.pKeyInfo = pKeyInfo;
    seekKey.aMem = aMem;
    seekKey.nField = 1;
    seekKey.default_rc = 0;
    check("atlast idx: IndexMoveto('d')",
          sqlite3BtreeIndexMoveto(pCur, &seekKey, &res)==SQLITE_OK && res==0);
    check("atlast idx: BTCF_AtLast cleared by IndexMoveto",
          (pCur->curFlags & BTCF_AtLast)==0);
    {
      const char *aWant[] = { "e", "f", "h" };
      checkRemainingForward("atlast idx moveto", pCur, aWant, 3);
    }

    sqlite3BtreeCloseCursor(pCur);
    sqlite3KeyInfoUnref(pKeyInfo);
  }

  sqlite3_free(pCur);
  check("atlast: commit", sqlite3BtreeCommit(pBt)==SQLITE_OK);
  sqlite3BtreeLeaveAll(db);
  sqlite3_close(db);
  removeDb(zDb);
}

int main(void){
  sqlite3_mem_methods wrapMem;
  if( sqlite3_config(SQLITE_CONFIG_GETMALLOC, &gRealMem)==SQLITE_OK ){
    wrapMem = gRealMem;
    wrapMem.xMalloc = oomMalloc;
    wrapMem.xRealloc = oomRealloc;
    sqlite3_config(SQLITE_CONFIG_MALLOC, &wrapMem);
  }

  runNonIntKeySuite(
      "without_rowid",
      "CREATE TABLE t(k TEXT PRIMARY KEY, v INT) WITHOUT ROWID;",
      "INSERT INTO t VALUES('b',2),('d',4),('f',6),('h',8);",
      "t",
      0);

  runNonIntKeySuite(
      "secondary_index",
      "CREATE TABLE t2(id INTEGER PRIMARY KEY, k TEXT);"
      "CREATE INDEX t2_k ON t2(k);",
      "INSERT INTO t2 VALUES(2,'b'),(4,'d'),(6,'f'),(8,'h');",
      "t2",
      "t2_k");

  runCachedSeekKeyTests();
  runAtLastSeekClearTests();

  printf("prolly_btree_saveposition_test: %d passed, %d failed\n", nPass, nFail);
  return nFail ? 1 : 0;
}
