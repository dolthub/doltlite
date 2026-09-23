/* PRAGMA integrity_check compares per-tree entry counts to spot an index
** holding entries no table row accounts for. Those counts come from
** sqlite3BtreeIntegrityCheck's aCnt array; when it reports zero for every
** tree the comparison is 0==0 and the check can never fire. */
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "sqliteInt.h"
#include "vdbeInt.h"

static int nPass = 0;
static int nFail = 0;

static void check(const char *name, int condition){
  if( condition ){
    nPass++;
  }else{
    nFail++;
    fprintf(stderr, "FAIL: %s\n", name);
  }
}

static void checkEq(const char *name, i64 got, i64 want){
  if( got==want ){
    nPass++;
  }else{
    nFail++;
    fprintf(stderr, "FAIL: %s: got %lld, want %lld\n", name,
            (long long)got, (long long)want);
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

/* Collect the root pages of every table and index in the schema. */
static int collectRoots(sqlite3 *db, Pgno *aRoot, char **azName, int mxRoot){
  sqlite3_stmt *pStmt = 0;
  int n = 0;
  int rc = sqlite3_prepare_v2(db,
      "SELECT name, rootpage FROM sqlite_master"
      " WHERE rootpage>0 ORDER BY rootpage", -1, &pStmt, 0);
  if( rc!=SQLITE_OK ) return -1;
  while( sqlite3_step(pStmt)==SQLITE_ROW && n<mxRoot ){
    azName[n] = sqlite3_mprintf("%s", sqlite3_column_text(pStmt, 0));
    aRoot[n] = (Pgno)sqlite3_column_int64(pStmt, 1);
    n++;
  }
  sqlite3_finalize(pStmt);
  return n;
}

/* Build one database shape, then assert every tree reports its true entry
** count and that integrity_check still passes the database. */
static void oneShape(const char *zName, const char *zSchema, int nRow){
  sqlite3 *db = 0;
  Btree *pBt;
  Pgno aRoot[16];
  char *azName[16];
  Mem aCnt[16];
  char *zSql;
  int nRoot, i, rc, nErr = 0;
  char *zOut = 0;

  removeDb("icc_test.db");
  rc = sqlite3_open("icc_test.db", &db);
  check("open", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ){ sqlite3_close(db); return; }

  if( execSql(db, zSchema)!=SQLITE_OK ){ sqlite3_close(db); nFail++; return; }
  zSql = sqlite3_mprintf(
      "WITH RECURSIVE s(x) AS ("
      "  SELECT 1 UNION ALL SELECT x+1 FROM s WHERE x<%d)"
      "INSERT INTO t(a,b) SELECT x, 'v'||x FROM s;", nRow);
  rc = execSql(db, zSql);
  sqlite3_free(zSql);
  if( rc!=SQLITE_OK ){ sqlite3_close(db); nFail++; return; }

  nRoot = collectRoots(db, aRoot, azName, 16);
  check("collected roots", nRoot>0);
  if( nRoot<=0 ){ sqlite3_close(db); return; }

  pBt = db->aDb[0].pBt;
  check("main btree present", pBt!=0);
  if( !pBt ){ sqlite3_close(db); return; }

  rc = sqlite3BtreeBeginTrans(pBt, 0, 0);
  check("read txn", rc==SQLITE_OK);

  memset(aCnt, 0, sizeof(aCnt));
  for(i=0; i<nRoot; i++) aCnt[i].db = db;

  rc = sqlite3BtreeIntegrityCheck(db, pBt, aRoot, aCnt, nRoot,
                                  100, &nErr, &zOut);
  check("integrity check ran", rc==SQLITE_OK);
  checkEq("no structural errors", nErr, 0);
  sqlite3_free(zOut);

  /* Every tree of this shape -- the table and each of its indexes -- holds
  ** exactly one entry per row, so a correct tally is nRow everywhere. */
  for(i=0; i<nRoot; i++){
    char zLabel[128];
    sqlite3_snprintf(sizeof(zLabel), zLabel, "%s: count for '%s'",
                     zName, azName[i]);
    checkEq(zLabel, sqlite3_value_int64(&aCnt[i]), nRow);
  }

  sqlite3BtreeCommit(pBt);

  {
    sqlite3_stmt *pStmt = 0;
    const char *zRes = "";
    if( sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &pStmt, 0)
        ==SQLITE_OK && sqlite3_step(pStmt)==SQLITE_ROW ){
      zRes = (const char*)sqlite3_column_text(pStmt, 0);
      check("integrity_check ok", zRes && strcmp(zRes, "ok")==0);
      if( !zRes || strcmp(zRes, "ok")!=0 ){
        fprintf(stderr, "  %s: integrity_check said '%s'\n", zName,
                zRes ? zRes : "(null)");
      }
    }else{
      nFail++;
      fprintf(stderr, "FAIL: %s: integrity_check did not run\n", zName);
    }
    sqlite3_finalize(pStmt);
  }

  for(i=0; i<nRoot; i++) sqlite3_free(azName[i]);
  sqlite3_close(db);
  removeDb("icc_test.db");
}

/* A record wider than its table is the shape a merge left behind before the
** relayout fix, and the shape a later ADD COLUMN misreads. integrity_check
** has to name it instead of answering "ok", so plant one directly: a
** three-field record in a two-column table. */
static void wideRecord(void){
  sqlite3 *db = 0;
  Btree *pBt;
  BtCursor *pCur;
  BtreePayload x;
  sqlite3_stmt *pStmt = 0;
  /* three NULL fields: header length 4, then three serial types of 0 */
  static const unsigned char aRec[4] = { 0x04, 0x00, 0x00, 0x00 };
  Pgno root = 0;
  int rc;

  removeDb("icc_wide.db");
  rc = sqlite3_open("icc_wide.db", &db);
  check("wide: open", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ){ sqlite3_close(db); return; }
  if( execSql(db, "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT);"
                  "INSERT INTO t VALUES(1,'one');")!=SQLITE_OK ){
    nFail++;
    sqlite3_close(db);
    return;
  }
  if( sqlite3_prepare_v2(db,
          "SELECT rootpage FROM sqlite_master WHERE name='t'", -1, &pStmt, 0)
       ==SQLITE_OK
   && sqlite3_step(pStmt)==SQLITE_ROW ){
    root = (Pgno)sqlite3_column_int64(pStmt, 0);
  }
  sqlite3_finalize(pStmt);
  check("wide: root page", root>0);

  pBt = db->aDb[0].pBt;
  pCur = sqlite3_malloc(sqlite3BtreeCursorSize());
  check("wide: cursor alloc", pCur!=0 && pBt!=0);
  if( !pCur || !pBt || root==0 ){
    sqlite3_free(pCur);
    sqlite3_close(db);
    return;
  }
  memset(pCur, 0, sqlite3BtreeCursorSize());
  check("wide: write txn", sqlite3BtreeBeginTrans(pBt, 1, 0)==SQLITE_OK);
  check("wide: cursor",
        sqlite3BtreeCursor(pBt, root, BTREE_WRCSR, 0, pCur)==SQLITE_OK);
  memset(&x, 0, sizeof(x));
  x.nKey = 2;
  x.pData = (void*)aRec;
  x.nData = (int)sizeof(aRec);
  check("wide: planted the record", sqlite3BtreeInsert(pCur, &x, 0, 0)==SQLITE_OK);
  sqlite3BtreeCloseCursor(pCur);
  sqlite3_free(pCur);
  check("wide: commit", sqlite3BtreeCommit(pBt)==SQLITE_OK);

  if( sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &pStmt, 0)
      ==SQLITE_OK ){
    int nNamed = 0;
    int nRow = 0;
    while( sqlite3_step(pStmt)==SQLITE_ROW ){
      const char *zRow = (const char*)sqlite3_column_text(pStmt, 0);
      nRow++;
      if( zRow && strstr(zRow, "stores 3 fields for 2 columns")!=0 ) nNamed++;
      if( zRow && strcmp(zRow, "ok")==0 ){
        fprintf(stderr, "  wide: integrity_check answered 'ok'\n");
      }
    }
    check("wide: integrity_check names the row", nNamed==1);
    check("wide: integrity_check reported something", nRow>0);
  }else{
    nFail++;
    fprintf(stderr, "FAIL: wide: integrity_check did not run\n");
  }
  sqlite3_finalize(pStmt);

  /* The planted row is the one a later ADD COLUMN misreads: its default
  ** lands on the stale field, while the untouched row reads the default. */
  check("wide: add column", execSql(db,
      "ALTER TABLE t ADD COLUMN p INTEGER DEFAULT 7;")==SQLITE_OK);
  if( sqlite3_prepare_v2(db, "SELECT count(*) FROM t WHERE p IS NULL", -1,
                         &pStmt, 0)==SQLITE_OK
   && sqlite3_step(pStmt)==SQLITE_ROW ){
    checkEq("wide: stale field shadows the default",
            sqlite3_column_int64(pStmt, 0), 1);
  }else{
    nFail++;
  }
  sqlite3_finalize(pStmt);

  sqlite3_close(db);
  removeDb("icc_wide.db");
}

/* DROP COLUMN narrows the live schema before its row rewrite is flushed.
** integrity_check and quick_check must not compare the old records to the
** new width while that rewrite is still pending. */
static int pragmaIsOk(sqlite3 *db, const char *zSql){
  sqlite3_stmt *pStmt = 0;
  int ok = 0;
  if( sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0)==SQLITE_OK
   && sqlite3_step(pStmt)==SQLITE_ROW ){
    const char *z = (const char*)sqlite3_column_text(pStmt, 0);
    ok = z && strcmp(z, "ok")==0 && sqlite3_step(pStmt)==SQLITE_DONE;
    if( !ok && z ) fprintf(stderr, "  %s -> %s\n", zSql, z);
  }
  sqlite3_finalize(pStmt);
  return ok;
}

static void dropColumnInTxn(void){
  sqlite3 *db = 0;
  sqlite3_stmt *pStmt = 0;

  removeDb("icc_drop.db");
  if( sqlite3_open("icc_drop.db", &db)!=SQLITE_OK ){
    nFail++;
    sqlite3_close(db);
    return;
  }
  check("drop: setup", execSql(db,
      "CREATE TABLE t(id INTEGER PRIMARY KEY, a, b, c);"
      "INSERT INTO t VALUES(1,'a1','b1','c1'),(2,'a2','b2','c2');"
      "BEGIN;"
      "ALTER TABLE t DROP COLUMN c;")==SQLITE_OK);
  check("drop: in-txn integrity_check",
        pragmaIsOk(db, "PRAGMA integrity_check"));
  check("drop: in-txn quick_check",
        pragmaIsOk(db, "PRAGMA quick_check"));
  if( sqlite3_prepare_v2(db,
          "SELECT id || a || b FROM t ORDER BY id", -1, &pStmt, 0)==SQLITE_OK
   && sqlite3_step(pStmt)==SQLITE_ROW ){
    const char *z = (const char*)sqlite3_column_text(pStmt, 0);
    check("drop: first row", z && strcmp(z, "1a1b1")==0);
    if( sqlite3_step(pStmt)==SQLITE_ROW ){
      z = (const char*)sqlite3_column_text(pStmt, 0);
      check("drop: second row", z && strcmp(z, "2a2b2")==0);
    }else{
      nFail++;
    }
  }else{
    nFail += 2;
  }
  sqlite3_finalize(pStmt);
  check("drop: commit", execSql(db, "COMMIT;")==SQLITE_OK);
  check("drop: after-commit integrity_check",
        pragmaIsOk(db, "PRAGMA integrity_check"));
  sqlite3_close(db);
  removeDb("icc_drop.db");
}

int main(void){
  oneShape("rowid pk, one index",
      "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT);"
      "CREATE INDEX ib ON t(b);", 500);

  oneShape("rowid pk, two indexes",
      "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT);"
      "CREATE INDEX ib ON t(b);"
      "CREATE UNIQUE INDEX ib2 ON t(b,a);", 500);

  oneShape("text pk, one index",
      "CREATE TABLE t(a TEXT PRIMARY KEY, b TEXT);"
      "CREATE INDEX ib ON t(b);", 300);

  oneShape("composite pk without rowid",
      "CREATE TABLE t(a INT, b TEXT, PRIMARY KEY(a,b)) WITHOUT ROWID;"
      "CREATE INDEX ib ON t(b);", 300);

  wideRecord();
  dropColumnInTxn();

  printf("integrity_check_counts_test: %d passed, %d failed\n", nPass, nFail);
  return nFail==0 ? 0 : 1;
}
