#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "sqliteInt.h"
#include "vdbeInt.h"

static int nPass = 0;
static int nFail = 0;

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

int main(void){
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

  printf("prolly_btree_saveposition_test: %d passed, %d failed\n", nPass, nFail);
  return nFail ? 1 : 0;
}
