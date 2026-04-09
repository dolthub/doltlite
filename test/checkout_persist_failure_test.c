/*
** Regression test for issue #304.
**
** Verifies that dolt_checkout() surfaces an error when the final refs
** commit fails, instead of incorrectly returning success.
**
** Build from build/:
**   cc -g -I. -o checkout_persist_failure_test \
**     ../test/checkout_persist_failure_test.c libdoltlite.a -lz -lpthread -lm
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "sqlite3.h"

static int nPass = 0;
static int nFail = 0;
static char g_buf[8192];

static void check(const char *name, int condition){
  if( condition ){
    nPass++;
  }else{
    nFail++;
    fprintf(stderr, "FAIL: %s\n", name);
  }
}

static const char *exec1(sqlite3 *db, const char *sql){
  sqlite3_stmt *stmt = 0;
  int rc;
  g_buf[0] = 0;
  rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);
  if( rc!=SQLITE_OK ){
    snprintf(g_buf, sizeof(g_buf), "ERROR: %s", sqlite3_errmsg(db));
    return g_buf;
  }
  rc = sqlite3_step(stmt);
  if( rc==SQLITE_ROW ){
    const char *val = (const char*)sqlite3_column_text(stmt, 0);
    if( val ) snprintf(g_buf, sizeof(g_buf), "%s", val);
  }else if( rc!=SQLITE_DONE ){
    snprintf(g_buf, sizeof(g_buf), "ERROR: %s", sqlite3_errmsg(db));
  }
  sqlite3_finalize(stmt);
  return g_buf;
}

static int execsql(sqlite3 *db, const char *sql){
  char *err = 0;
  int rc = sqlite3_exec(db, sql, 0, 0, &err);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "  SQL error: %s (rc=%d)\n  SQL: %s\n",
            err ? err : "?", rc, sql);
    sqlite3_free(err);
  }
  return rc;
}

static void remove_db(const char *path){
  char tmp[512];
  remove(path);
  snprintf(tmp, sizeof(tmp), "%s-wal", path);
  remove(tmp);
  snprintf(tmp, sizeof(tmp), "%s-shm", path);
  remove(tmp);
  snprintf(tmp, sizeof(tmp), "%s-journal", path);
  remove(tmp);
}

typedef struct FailFile FailFile;
struct FailFile {
  sqlite3_file base;
  sqlite3_file *pReal;
};

static sqlite3_vfs gFailVfs;
static sqlite3_vfs *gBaseVfs = 0;
static int gFailSyncOnce = 0;
static int gFailHits = 0;

static int failClose(sqlite3_file *pFile){
  FailFile *p = (FailFile*)pFile;
  return p->pReal->pMethods->xClose(p->pReal);
}

static int failRead(sqlite3_file *pFile, void *zBuf, int iAmt, sqlite3_int64 iOfst){
  FailFile *p = (FailFile*)pFile;
  return p->pReal->pMethods->xRead(p->pReal, zBuf, iAmt, iOfst);
}

static int failWrite(sqlite3_file *pFile, const void *zBuf, int iAmt, sqlite3_int64 iOfst){
  FailFile *p = (FailFile*)pFile;
  return p->pReal->pMethods->xWrite(p->pReal, zBuf, iAmt, iOfst);
}

static int failTruncate(sqlite3_file *pFile, sqlite3_int64 size){
  FailFile *p = (FailFile*)pFile;
  return p->pReal->pMethods->xTruncate(p->pReal, size);
}

static int failSync(sqlite3_file *pFile, int flags){
  FailFile *p = (FailFile*)pFile;
  if( gFailSyncOnce>0 ){
    gFailSyncOnce--;
    gFailHits++;
    return SQLITE_IOERR;
  }
  return p->pReal->pMethods->xSync(p->pReal, flags);
}

static int failFileSize(sqlite3_file *pFile, sqlite3_int64 *pSize){
  FailFile *p = (FailFile*)pFile;
  return p->pReal->pMethods->xFileSize(p->pReal, pSize);
}

static int failLock(sqlite3_file *pFile, int eLock){
  FailFile *p = (FailFile*)pFile;
  return p->pReal->pMethods->xLock(p->pReal, eLock);
}

static int failUnlock(sqlite3_file *pFile, int eLock){
  FailFile *p = (FailFile*)pFile;
  return p->pReal->pMethods->xUnlock(p->pReal, eLock);
}

static int failCheckReservedLock(sqlite3_file *pFile, int *pResOut){
  FailFile *p = (FailFile*)pFile;
  return p->pReal->pMethods->xCheckReservedLock(p->pReal, pResOut);
}

static int failFileControl(sqlite3_file *pFile, int op, void *pArg){
  FailFile *p = (FailFile*)pFile;
  return p->pReal->pMethods->xFileControl(p->pReal, op, pArg);
}

static int failSectorSize(sqlite3_file *pFile){
  FailFile *p = (FailFile*)pFile;
  return p->pReal->pMethods->xSectorSize(p->pReal);
}

static int failDeviceCharacteristics(sqlite3_file *pFile){
  FailFile *p = (FailFile*)pFile;
  return p->pReal->pMethods->xDeviceCharacteristics(p->pReal);
}

static int failShmMap(sqlite3_file *pFile, int iPg, int pgsz, int bExtend, void volatile **pp){
  FailFile *p = (FailFile*)pFile;
  return p->pReal->pMethods->xShmMap(p->pReal, iPg, pgsz, bExtend, pp);
}

static int failShmLock(sqlite3_file *pFile, int offset, int n, int flags){
  FailFile *p = (FailFile*)pFile;
  return p->pReal->pMethods->xShmLock(p->pReal, offset, n, flags);
}

static void failShmBarrier(sqlite3_file *pFile){
  FailFile *p = (FailFile*)pFile;
  p->pReal->pMethods->xShmBarrier(p->pReal);
}

static int failShmUnmap(sqlite3_file *pFile, int deleteFlag){
  FailFile *p = (FailFile*)pFile;
  return p->pReal->pMethods->xShmUnmap(p->pReal, deleteFlag);
}

static int failFetch(sqlite3_file *pFile, sqlite3_int64 iOfst, int iAmt, void **pp){
  FailFile *p = (FailFile*)pFile;
  if( p->pReal->pMethods->iVersion<3 || p->pReal->pMethods->xFetch==0 ){
    *pp = 0;
    return SQLITE_OK;
  }
  return p->pReal->pMethods->xFetch(p->pReal, iOfst, iAmt, pp);
}

static int failUnfetch(sqlite3_file *pFile, sqlite3_int64 iOfst, void *pPage){
  FailFile *p = (FailFile*)pFile;
  if( p->pReal->pMethods->iVersion<3 || p->pReal->pMethods->xUnfetch==0 ){
    return SQLITE_OK;
  }
  return p->pReal->pMethods->xUnfetch(p->pReal, iOfst, pPage);
}

static const sqlite3_io_methods gFailIoMethods = {
  3,
  failClose,
  failRead,
  failWrite,
  failTruncate,
  failSync,
  failFileSize,
  failLock,
  failUnlock,
  failCheckReservedLock,
  failFileControl,
  failSectorSize,
  failDeviceCharacteristics,
  failShmMap,
  failShmLock,
  failShmBarrier,
  failShmUnmap,
  failFetch,
  failUnfetch
};

static int failOpen(sqlite3_vfs *pVfs, const char *zName, sqlite3_file *pFile,
                    int flags, int *pOutFlags){
  FailFile *p = (FailFile*)pFile;
  sqlite3_file *pReal = (sqlite3_file*)&p[1];
  int rc;

  memset(p, 0, sizeof(*p));
  rc = gBaseVfs->xOpen(gBaseVfs, zName, pReal, flags, pOutFlags);
  if( rc!=SQLITE_OK ) return rc;

  p->pReal = pReal;
  p->base.pMethods = &gFailIoMethods;
  return SQLITE_OK;
}

static int registerFailVfs(void){
  if( gBaseVfs ) return SQLITE_OK;
  gBaseVfs = sqlite3_vfs_find(0);
  if( !gBaseVfs ) return SQLITE_ERROR;
  memset(&gFailVfs, 0, sizeof(gFailVfs));
  gFailVfs = *gBaseVfs;
  gFailVfs.zName = "doltlite-failvfs";
  gFailVfs.szOsFile = sizeof(FailFile) + gBaseVfs->szOsFile;
  gFailVfs.xOpen = failOpen;
  return sqlite3_vfs_register(&gFailVfs, 0);
}

static int open_db(const char *path, sqlite3 **ppDb){
  int rc = sqlite3_open_v2(path, ppDb,
      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, gFailVfs.zName);
  if( rc==SQLITE_OK ){
    sqlite3_busy_timeout(*ppDb, 5000);
  }
  return rc;
}

static void test_checkout_surfaces_persist_failure(void){
  sqlite3 *db1 = 0;
  const char *dbpath = "/tmp/test_checkout_persist_failure.db";
  const char *res;

  printf("--- Test 1: dolt_checkout surfaces final persist failure ---\n");
  remove_db(dbpath);
  gFailSyncOnce = 0;
  gFailHits = 0;

  check("register_fail_vfs", registerFailVfs()==SQLITE_OK);
  check("open_db1", open_db(dbpath, &db1)==SQLITE_OK);

  check("setup_schema", execsql(db1,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');")==SQLITE_OK);
  check("init_commit",
    strlen(exec1(db1, "SELECT dolt_commit('-A', '-m', 'init')"))==40);
  check("create_feature_branch",
    strcmp(exec1(db1, "SELECT dolt_branch('feature')"), "0")==0);

  gFailSyncOnce = 1;
  res = exec1(db1, "SELECT dolt_checkout('feature')");
  check("persist_failure_was_injected", gFailHits>0);
  check("checkout_returns_error_on_persist_failure", strstr(res, "ERROR:")!=0);
  check("session_branch_already_switched_before_error",
    strcmp(exec1(db1, "SELECT active_branch()"), "feature")==0);

  sqlite3_close(db1);
  remove_db(dbpath);
}

int main(void){
  printf("=== Checkout Persist Failure Test ===\n\n");
  test_checkout_surfaces_persist_failure();
  printf("\nResults: %d passed, %d failed out of %d tests\n",
         nPass, nFail, nPass+nFail);
  return nFail ? 1 : 0;
}
