#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "sqlite3.h"

static int nPass;
static int nFail;

static void check(const char *zName, int condition){
  if( condition ){
    nPass++;
  }else{
    nFail++;
    fprintf(stderr, "FAIL: %s\n", zName);
  }
}

static int execSql(sqlite3 *db, const char *zSql){
  char *zErr = 0;
  int rc = sqlite3_exec(db, zSql, 0, 0, &zErr);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "SQL error: %s (rc=%d)\nSQL: %s\n",
            zErr ? zErr : "?", rc, zSql);
  }
  sqlite3_free(zErr);
  return rc;
}

static int scalarInt(sqlite3 *db, const char *zSql, int *pValue){
  sqlite3_stmt *pStmt = 0;
  int rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  if( rc==SQLITE_OK ) rc = sqlite3_step(pStmt);
  if( rc==SQLITE_ROW ){
    *pValue = sqlite3_column_int(pStmt, 0);
    rc = SQLITE_OK;
  }
  sqlite3_finalize(pStmt);
  return rc;
}

static int scalarText(sqlite3 *db, const char *zSql, char *zOut, int nOut){
  sqlite3_stmt *pStmt = 0;
  int rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  if( rc==SQLITE_OK ) rc = sqlite3_step(pStmt);
  if( rc==SQLITE_ROW ){
    const unsigned char *z = sqlite3_column_text(pStmt, 0);
    if( z ) snprintf(zOut, nOut, "%s", z);
    rc = SQLITE_OK;
  }
  sqlite3_finalize(pStmt);
  return rc;
}

static int scalarIsNull(sqlite3 *db, const char *zSql){
  sqlite3_stmt *pStmt = 0;
  int isNull = 0;
  int rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  if( rc==SQLITE_OK && sqlite3_step(pStmt)==SQLITE_ROW ){
    isNull = sqlite3_column_type(pStmt, 0)==SQLITE_NULL;
  }
  sqlite3_finalize(pStmt);
  return isNull;
}

static void checkDetached(sqlite3 *db, const char *zName){
  int n = 0;
  char zCheck[128];
  snprintf(zCheck, sizeof(zCheck), "%s: readonly", zName);
  check(zCheck, sqlite3_db_readonly(db, "main")==1);
  snprintf(zCheck, sizeof(zCheck), "%s: active branch null", zName);
  check(zCheck, scalarIsNull(db, "SELECT active_branch()"));
  snprintf(zCheck, sizeof(zCheck), "%s: historical rows", zName);
  check(zCheck, scalarInt(db, "SELECT count(*) FROM t", &n)==SQLITE_OK && n==1);
  snprintf(zCheck, sizeof(zCheck), "%s: rejects writes", zName);
  check(zCheck, sqlite3_exec(db, "INSERT INTO t VALUES(3)", 0, 0, 0)==SQLITE_READONLY);
}

int main(void){
  char zDir[] = "/tmp/doltlite-detached-head-XXXXXX";
  char zPath[1024];
  char zOpen[1024];
  char zLock[1024];
  char zHash[64] = {0};
  sqlite3 *db = 0;
  int rc;
  int n = 0;

  check("create temporary directory", mkdtemp(zDir)!=0);
  snprintf(zPath, sizeof(zPath), "%s/test.db", zDir);

  rc = sqlite3_open(zPath, &db);
  check("open database", rc==SQLITE_OK);
  check("create table", execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY)")==SQLITE_OK);
  check("insert first row", execSql(db, "INSERT INTO t VALUES(1)")==SQLITE_OK);
  check("commit first row", execSql(db, "SELECT dolt_commit('-A','-m','c1')")==SQLITE_OK);
  check("create tag", execSql(db, "SELECT dolt_tag('v1')")==SQLITE_OK);
  check("resolve tag hash", scalarText(db, "SELECT dolt_hashof('v1')", zHash,
                                      sizeof(zHash))==SQLITE_OK && strlen(zHash)==40);
  check("insert second row", execSql(db, "INSERT INTO t VALUES(2)")==SQLITE_OK);
  check("commit second row", execSql(db, "SELECT dolt_commit('-A','-m','c2')")==SQLITE_OK);
  check("attached checkout tag rejected",
        sqlite3_exec(db, "SELECT dolt_checkout('v1')", 0, 0, 0)!=SQLITE_OK);
  check("rejected checkout stays attached",
        scalarText(db, "SELECT active_branch()", zOpen, sizeof(zOpen))==SQLITE_OK
        && strcmp(zOpen, "main")==0 && sqlite3_db_readonly(db, "main")==0);
  snprintf(zOpen, sizeof(zOpen), "SELECT dolt_checkout('%s')", zHash);
  check("attached checkout hash rejected", sqlite3_exec(db, zOpen, 0, 0, 0)!=SQLITE_OK);
  sqlite3_close(db);
  db = 0;

  snprintf(zOpen, sizeof(zOpen), "%s/v1", zPath);
  rc = sqlite3_open(zOpen, &db);
  check("open slash tag", rc==SQLITE_OK);
  if( rc==SQLITE_OK ){
    checkDetached(db, "slash tag");
    check("reattach branch", execSql(db, "SELECT dolt_checkout('main')")==SQLITE_OK);
    check("reattached writable", sqlite3_db_readonly(db, "main")==0);
    check("reattached active branch",
          scalarText(db, "SELECT active_branch()", zOpen, sizeof(zOpen))==SQLITE_OK
          && strcmp(zOpen, "main")==0);
    check("reattached write", execSql(db, "INSERT INTO t VALUES(3)")==SQLITE_OK);
  }
  sqlite3_close(db);
  db = 0;

  snprintf(zOpen, sizeof(zOpen), "%s@v1", zPath);
  rc = sqlite3_open(zOpen, &db);
  check("open at tag", rc==SQLITE_OK);
  if( rc==SQLITE_OK ) checkDetached(db, "at tag");
  sqlite3_close(db);
  db = 0;

  snprintf(zOpen, sizeof(zOpen), "%s/%s", zPath, zHash);
  rc = sqlite3_open(zOpen, &db);
  check("open commit hash", rc==SQLITE_OK);
  if( rc==SQLITE_OK ) checkDetached(db, "commit hash");
  sqlite3_close(db);
  db = 0;

  rc = sqlite3_open(":memory:", &db);
  check("open attach host", rc==SQLITE_OK);
  if( rc==SQLITE_OK ){
    snprintf(zOpen, sizeof(zOpen), "ATTACH DATABASE '%s/v1' AS hist", zPath);
    check("attach detached tag", execSql(db, zOpen)==SQLITE_OK);
    check("attached tag readonly", sqlite3_db_readonly(db, "hist")==1);
    check("attached historical rows",
          scalarInt(db, "SELECT count(*) FROM hist.t", &n)==SQLITE_OK && n==1);
    check("attached tag rejects writes",
          sqlite3_exec(db, "INSERT INTO hist.t VALUES(4)", 0, 0, 0)==SQLITE_READONLY);
  }
  sqlite3_close(db);

  unlink(zPath);
  snprintf(zLock, sizeof(zLock), "%s-lock", zPath);
  unlink(zLock);
  rmdir(zDir);

  printf("%d passed, %d failed\n", nPass, nFail);
  return nFail ? 1 : 0;
}
