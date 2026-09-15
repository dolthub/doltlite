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

  printf("integrity_check_counts_test: %d passed, %d failed\n", nPass, nFail);
  return nFail==0 ? 0 : 1;
}
