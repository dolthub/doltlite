/*
** Shared regression runner for focused DoltLite C repros.
**
** Build from build/:
**   cc -g -I. -I../src -o doltlite_regression_test_c \
**     ../test/doltlite_regression_test_c.c libdoltlite.a -lz -lpthread -lm
**
** Run from build/:
**   ./doltlite_regression_test_c all
**   ./doltlite_regression_test_c concurrent_refs
**   ./doltlite_regression_test_c checkout_persist_failure
**   ./doltlite_regression_test_c savepoint_catalog_restore
**   ./doltlite_regression_test_c refs_blob_corruption
**   ./doltlite_regression_test_c conflicts_blob_corruption
**   ./doltlite_regression_test_c status_error_propagation
**   ./doltlite_regression_test_c remote_refs_corruption
**   ./doltlite_regression_test_c chunk_walk_corruption
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "sqlite3.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_chunk_walk.h"
#include "doltlite_internal.h"

typedef unsigned char u8;
typedef unsigned int Pgno;

extern int doltliteFlushAndSerializeCatalog(sqlite3 *db, u8 **ppOut, int *pnOut);
extern void doltliteSetTableSchemaHash(sqlite3 *db, Pgno iTable, const ProllyHash *pH);

static int nPass = 0;
static int nFail = 0;
static char gBuf[8192];

typedef struct RegressionCase RegressionCase;
struct RegressionCase {
  const char *zName;
  const char *zTitle;
  void (*xRun)(void);
};

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
  gBuf[0] = 0;
  rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);
  if( rc!=SQLITE_OK ){
    snprintf(gBuf, sizeof(gBuf), "ERROR: %s", sqlite3_errmsg(db));
    return gBuf;
  }
  rc = sqlite3_step(stmt);
  if( rc==SQLITE_ROW ){
    const char *val = (const char*)sqlite3_column_text(stmt, 0);
    if( val ) snprintf(gBuf, sizeof(gBuf), "%s", val);
  }else if( rc!=SQLITE_DONE ){
    snprintf(gBuf, sizeof(gBuf), "ERROR: %s", sqlite3_errmsg(db));
  }
  sqlite3_finalize(stmt);
  return gBuf;
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

static int execsql_silent(sqlite3 *db, const char *sql){
  char *err = 0;
  int rc = sqlite3_exec(db, sql, 0, 0, &err);
  sqlite3_free(err);
  return rc;
}

static int open_db(const char *path, sqlite3 **ppDb){
  int rc = sqlite3_open(path, ppDb);
  if( rc==SQLITE_OK ){
    sqlite3_busy_timeout(*ppDb, 5000);
  }
  return rc;
}

static void make_dbpath(char *zBuf, size_t nBuf, const char *zBase){
  snprintf(zBuf, nBuf, "/tmp/%s_%ld.db", zBase, (long)getpid());
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

static Pgno table_rootpage(sqlite3 *db, const char *zName){
  sqlite3_stmt *stmt = 0;
  Pgno pgno = 0;
  if( sqlite3_prepare_v2(
          db,
          "SELECT rootpage FROM sqlite_master "
          "WHERE type='table' AND name=?1",
          -1, &stmt, 0)==SQLITE_OK ){
    sqlite3_bind_text(stmt, 1, zName, -1, SQLITE_STATIC);
    if( sqlite3_step(stmt)==SQLITE_ROW ){
      pgno = (Pgno)sqlite3_column_int(stmt, 0);
    }
  }
  sqlite3_finalize(stmt);
  return pgno;
}

typedef struct FailFile FailFile;
struct FailFile {
  sqlite3_file base;
  sqlite3_file *pReal;
};

static sqlite3_vfs gFailVfs;
static sqlite3_vfs *gBaseVfs = 0;
static int gFailSyncOnce = 0;
static int gFailAccessOnce = 0;
static int gFailHasMovedOnce = 0;
static int gFailFileSizeOnce = 0;
static int gFailHits = 0;

static int failAccess(sqlite3_vfs *pVfs, const char *zName, int flags, int *pResOut);

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
  if( gFailFileSizeOnce>0 ){
    gFailFileSizeOnce--;
    gFailHits++;
    return SQLITE_IOERR;
  }
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
  if( op==SQLITE_FCNTL_HAS_MOVED && gFailHasMovedOnce>0 ){
    gFailHasMovedOnce--;
    gFailHits++;
    return SQLITE_IOERR;
  }
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

static int failShmMap(sqlite3_file *pFile, int iPg, int pgsz, int bExtend,
                      void volatile **pp){
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
  gFailVfs.xAccess = failAccess;
  return sqlite3_vfs_register(&gFailVfs, 0);
}

static int failAccess(sqlite3_vfs *pVfs, const char *zName, int flags, int *pResOut){
  (void)pVfs;
  if( gFailAccessOnce>0 ){
    gFailAccessOnce--;
    gFailHits++;
    return SQLITE_IOERR;
  }
  return gBaseVfs->xAccess(gBaseVfs, zName, flags, pResOut);
}

static int open_fail_db(const char *path, sqlite3 **ppDb){
  int rc = sqlite3_open_v2(path, ppDb,
      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, gFailVfs.zName);
  if( rc==SQLITE_OK ){
    sqlite3_busy_timeout(*ppDb, 5000);
  }
  return rc;
}

static void test_concurrent_refs_stale_reset_is_rejected(void){
  sqlite3 *db1 = 0, *db2 = 0, *db3 = 0;
  char dbpath[256];
  char firstCommit[128];
  char secondCommit[128];
  char sql[512];

  printf("--- Test 1: stale dolt_reset is rejected ---\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_concurrent_refs_reset");
  remove_db(dbpath);

  check("open_db1", open_db(dbpath, &db1)==SQLITE_OK);
  check("setup_schema", execsql(db1,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');")==SQLITE_OK);

  snprintf(firstCommit, sizeof(firstCommit), "%s",
           exec1(db1, "SELECT dolt_commit('-A', '-m', 'init')"));
  check("first_commit_hash", strlen(firstCommit)==40);
  check("open_db2", open_db(dbpath, &db2)==SQLITE_OK);

  check("insert_second_row",
    execsql(db1, "INSERT INTO t VALUES(2,'b')")==SQLITE_OK);
  snprintf(secondCommit, sizeof(secondCommit), "%s",
           exec1(db1, "SELECT dolt_commit('-A', '-m', 'second')"));
  check("second_commit_hash", strlen(secondCommit)==40);

  snprintf(sql, sizeof(sql), "SELECT dolt_reset('%s')", firstCommit);
  exec1(db2, sql);
  check("stale_reset_is_rejected",
    strstr(gBuf, "ERROR")!=0 || strstr(gBuf, "conflict")!=0);

  sqlite3_close(db1);
  sqlite3_close(db2);

  check("open_db3", open_db(dbpath, &db3)==SQLITE_OK);
  check("newer_commit_still_head",
    strcmp(exec1(db3, "SELECT message FROM dolt_log LIMIT 1"), "second")==0);
  snprintf(sql, sizeof(sql),
           "SELECT count(*) FROM dolt_log WHERE commit_hash='%s'", secondCommit);
  check("newer_commit_still_visible_in_log",
    strcmp(exec1(db3, sql), "1")==0);
  check("branch_history_has_both_commits",
    strcmp(exec1(db3, "SELECT count(*) FROM dolt_log"), "3")==0);

  sqlite3_close(db3);
  remove_db(dbpath);
}

static void test_concurrent_refs_checkout_refreshes_branch(void){
  sqlite3 *db1 = 0, *db2 = 0;
  char dbpath[256];

  printf("--- Test 2: stale dolt_checkout refreshes target branch ---\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_concurrent_refs_checkout");
  remove_db(dbpath);

  check("checkout_open_db1", open_db(dbpath, &db1)==SQLITE_OK);
  check("checkout_setup_schema", execsql(db1,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');")==SQLITE_OK);
  check("checkout_init_commit",
    strlen(exec1(db1, "SELECT dolt_commit('-A', '-m', 'init')"))==40);
  check("checkout_create_branch",
    strcmp(exec1(db1, "SELECT dolt_branch('feature')"), "0")==0);
  check("checkout_open_db2", open_db(dbpath, &db2)==SQLITE_OK);
  check("checkout_switch_db1_feature",
    strcmp(exec1(db1, "SELECT dolt_checkout('feature')"), "0")==0);
  check("checkout_feature_insert",
    execsql(db1, "INSERT INTO t VALUES(2,'feature')")==SQLITE_OK);
  check("checkout_feature_commit",
    strlen(exec1(db1, "SELECT dolt_commit('-A', '-m', 'feature update')"))==40);

  check("checkout_stale_connection_switches",
    strcmp(exec1(db2, "SELECT dolt_checkout('feature')"), "0")==0);
  check("checkout_latest_branch_tip_visible",
    strcmp(exec1(db2, "SELECT message FROM dolt_log LIMIT 1"), "feature update")==0);
  check("checkout_latest_branch_data_visible",
    strcmp(exec1(db2, "SELECT count(*) FROM t"), "2")==0);

  sqlite3_close(db1);
  sqlite3_close(db2);
  remove_db(dbpath);
}

static void run_concurrent_refs(void){
  printf("=== Concurrent Refs Test ===\n\n");
  test_concurrent_refs_stale_reset_is_rejected();
  test_concurrent_refs_checkout_refreshes_branch();
}

static void run_checkout_persist_failure(void){
  sqlite3 *db1 = 0;
  char dbpath[256];
  const char *res;

  printf("=== Checkout Persist Failure Test ===\n\n");
  printf("--- Test 1: dolt_checkout surfaces final persist failure ---\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_checkout_persist_failure");
  remove_db(dbpath);
  gFailSyncOnce = 0;
  gFailHits = 0;

  check("register_fail_vfs", registerFailVfs()==SQLITE_OK);
  check("open_db1", open_fail_db(dbpath, &db1)==SQLITE_OK);

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

static void run_savepoint_catalog_restore(void){
  sqlite3 *db = 0;
  char dbpath[256];
  u8 *aBefore = 0;
  u8 *aAfter = 0;
  int nBefore = 0;
  int nAfter = 0;
  Pgno iTable;
  ProllyHash fakeHash;

  printf("=== Savepoint Catalog Restore Test ===\n\n");
  printf("--- Test 1: savepoint rollback restores schema metadata ---\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_savepoint_catalog_restore");
  remove_db(dbpath);

  check("open_db", sqlite3_open(dbpath, &db)==SQLITE_OK);
  if( !db ) return;

  check("create_table", execsql(db,
      "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);")==SQLITE_OK);
  check("serialize_before",
      doltliteFlushAndSerializeCatalog(db, &aBefore, &nBefore)==SQLITE_OK);

  iTable = table_rootpage(db, "t");
  check("lookup_rootpage", iTable>0);

  memset(&fakeHash, 0x5a, sizeof(fakeHash));
  check("savepoint_begin", execsql(db, "SAVEPOINT sp;")==SQLITE_OK);
  doltliteSetTableSchemaHash(db, iTable, &fakeHash);
  check("rollback_to", execsql(db, "ROLLBACK TO sp;")==SQLITE_OK);
  check("release_sp", execsql(db, "RELEASE sp;")==SQLITE_OK);

  check("serialize_after",
      doltliteFlushAndSerializeCatalog(db, &aAfter, &nAfter)==SQLITE_OK);
  check("catalog_equal_after_rollback",
      nBefore==nAfter && memcmp(aBefore, aAfter, nBefore)==0);

  sqlite3_free(aBefore);
  sqlite3_free(aAfter);
  sqlite3_close(db);
}

static void run_refs_blob_corruption(void){
  ChunkStore cs;
  ChunkStore cs2;
  u8 *pBlob = 0;
  int nBlob = 0;
  int rc;

  printf("=== Refs Blob Corruption Test ===\n\n");
  printf("--- Test 1: truncated refs blob is rejected ---\n");

  check("open_mem_store_1",
        chunkStoreOpen(&cs, sqlite3_vfs_find(0), ":memory:", 0)==SQLITE_OK);
  check("open_mem_store_2",
        chunkStoreOpen(&cs2, sqlite3_vfs_find(0), ":memory:", 0)==SQLITE_OK);

  check("set_default_branch",
        chunkStoreSetDefaultBranch(&cs, "main")==SQLITE_OK);
  cs.aBranches = sqlite3_malloc(sizeof(*cs.aBranches));
  check("alloc_branch_ref", cs.aBranches!=0);
  if( cs.aBranches ){
    memset(cs.aBranches, 0, sizeof(*cs.aBranches));
    cs.nBranches = 1;
    cs.aBranches[0].zName = sqlite3_mprintf("main");
    check("alloc_branch_name", cs.aBranches[0].zName!=0);
  }

  cs.aTags = sqlite3_malloc(sizeof(*cs.aTags));
  check("alloc_tag_ref", cs.aTags!=0);
  if( cs.aTags ){
    memset(cs.aTags, 0, sizeof(*cs.aTags));
    cs.nTags = 1;
    cs.aTags[0].zName = sqlite3_mprintf("v1");
    check("alloc_tag_name", cs.aTags[0].zName!=0);
  }

  check("serialize_refs_blob",
        chunkStoreSerializeRefsToBlob(&cs, &pBlob, &nBlob)==SQLITE_OK);
  check("refs_blob_has_tag_payload", nBlob>0);

  rc = chunkStoreLoadRefsFromBlob(&cs2, pBlob, nBlob-20);
  check("truncated_blob_returns_corrupt", rc==SQLITE_CORRUPT);

  sqlite3_free(pBlob);
  chunkStoreClose(&cs2);
  chunkStoreClose(&cs);
}

static void run_refresh_error_propagation(void){
  sqlite3 *db = 0;
  ChunkStore cs;
  int changed = -1;
  int rc;
  char dbpath[256];

  printf("=== Refresh Error Propagation Test ===\n\n");
  check("register_fail_vfs_for_refresh", registerFailVfs()==SQLITE_OK);

  printf("--- Test 1: xAccess failure is surfaced ---\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_refresh_access_failure");
  remove_db(dbpath);
  check("open_empty_chunk_store",
        chunkStoreOpen(&cs, &gFailVfs, dbpath,
          SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  gFailHits = 0;
  gFailAccessOnce = 1;
  changed = -1;
  rc = chunkStoreRefreshIfChanged(&cs, &changed);
  check("refresh_returns_access_error", rc!=SQLITE_OK);
  check("access_failure_injected", gFailHits>0);
  check("changed_left_false_on_access_error", changed==0);
  chunkStoreClose(&cs);
  remove_db(dbpath);

  printf("--- Test 2: xFileControl failure is surfaced ---\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_refresh_filecontrol_failure");
  remove_db(dbpath);
  check("open_sql_db", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_doltlite_repo", execsql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);
  sqlite3_close(db);
  db = 0;
  check("open_chunk_store_with_file",
        chunkStoreOpen(&cs, &gFailVfs, dbpath,
          SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  gFailHits = 0;
  gFailHasMovedOnce = 1;
  changed = -1;
  rc = chunkStoreRefreshIfChanged(&cs, &changed);
  check("refresh_returns_filecontrol_error", rc!=SQLITE_OK);
  check("filecontrol_failure_injected", gFailHits>0);
  check("changed_left_false_on_filecontrol_error", changed==0);
  chunkStoreClose(&cs);
  remove_db(dbpath);
}

static void run_conflicts_blob_corruption(void){
  sqlite3 *db = 0;
  ChunkStore *cs = 0;
  ProllyHash hash;
  char dbpath[256];
  u8 badBlob[] = {
    1, 0,
    1, 0, 't',
    1, 0, 0
  };

  printf("=== Conflicts Blob Corruption Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_conflicts_blob_corruption");
  remove_db(dbpath);

  check("open_db", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo", execsql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);

  cs = doltliteGetChunkStore(db);
  check("have_chunk_store", cs!=0);
  if( cs ){
    check("put_bad_conflicts_blob",
      chunkStorePut(cs, badBlob, (int)sizeof(badBlob), &hash)==SQLITE_OK);
    doltliteSetSessionConflictsCatalog(db, &hash);
    check("corrupt_conflicts_table_errors",
      execsql_silent(db, "SELECT count(*) FROM dolt_conflicts;")!=SQLITE_OK);
  }

  sqlite3_close(db);
  remove_db(dbpath);
}

static void run_status_error_propagation(void){
  sqlite3 *db = 0;
  char dbpath[256];
  ProllyHash badHash;
  int rc;

  printf("=== Status Error Propagation Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_status_error_propagation");
  remove_db(dbpath);

  check("open_db", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_status_repo", execsql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "INSERT INTO t VALUES(2,'b');")==SQLITE_OK);

  memset(&badHash, 0x7b, sizeof(badHash));
  doltliteSetSessionStaged(db, &badHash);
  rc = execsql_silent(db, "SELECT count(*) FROM dolt_status;");
  check("status_surfaces_persist_error", rc!=SQLITE_OK);

  sqlite3_close(db);
  remove_db(dbpath);
}

static void run_remote_refs_corruption(void){
  sqlite3 *srcDb = 0;
  sqlite3 *localDb = 0;
  ChunkStore cs;
  ProllyHash badRefsHash;
  u8 badBlob[] = { 5, 0, 0, 0, 0, 1, 0, 0 };
  char remotePath[256];
  char localPath[256];
  int rc;

  printf("=== Remote Refs Corruption Test ===\n\n");
  make_dbpath(remotePath, sizeof(remotePath), "test_remote_refs_corruption_remote");
  make_dbpath(localPath, sizeof(localPath), "test_remote_refs_corruption_local");
  remove_db(remotePath);
  remove_db(localPath);

  check("open_remote_db", open_db(remotePath, &srcDb)==SQLITE_OK);
  check("setup_remote_repo", execsql(srcDb,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);
  sqlite3_close(srcDb);
  srcDb = 0;

  check("open_local_db", open_db(localPath, &localDb)==SQLITE_OK);
  {
    char sql[1024];
    snprintf(sql, sizeof(sql),
      "CREATE TABLE seed(x INTEGER);"
      "SELECT dolt_commit('-A', '-m', 'seed');"
      "SELECT dolt_remote('add','origin','file://%s');",
      remotePath);
    check("setup_local_remote", execsql(localDb, sql)==SQLITE_OK);
  }

  check("open_chunk_store", chunkStoreOpen(&cs, sqlite3_vfs_find(0), remotePath,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  check("lock_remote_store", chunkStoreLockAndRefresh(&cs)==SQLITE_OK);
  check("put_bad_remote_refs",
        chunkStorePut(&cs, badBlob, (int)sizeof(badBlob), &badRefsHash)==SQLITE_OK);
  memcpy(&cs.refsHash, &badRefsHash, sizeof(ProllyHash));
  check("commit_bad_remote_refs", chunkStoreCommit(&cs)==SQLITE_OK);
  chunkStoreUnlock(&cs);
  chunkStoreClose(&cs);

  rc = execsql_silent(localDb, "SELECT dolt_fetch('origin');");
  check("fetch_surfaces_corrupt_refs", rc!=SQLITE_OK);

  sqlite3_close(localDb);
  remove_db(remotePath);
  remove_db(localPath);
}

static void run_chunk_walk_corruption(void){
  static const u8 badCatalog[] = {
    'D','L','C','T',
    3, 0,
    1, 0, 0, 0,
    2, 0, 0, 0,
    0,
    1, 0
  };
  int rc;

  printf("=== Chunk Walk Corruption Test ===\n\n");
  rc = doltliteEnumerateChunkChildren(badCatalog, (int)sizeof(badCatalog), 0, 0);
  check("truncated_catalog_is_corrupt", rc==SQLITE_CORRUPT);
}

static const RegressionCase aCases[] = {
  { "concurrent_refs", "Concurrent Refs Test", run_concurrent_refs },
  { "checkout_persist_failure", "Checkout Persist Failure Test", run_checkout_persist_failure },
  { "savepoint_catalog_restore", "Savepoint Catalog Restore Test", run_savepoint_catalog_restore },
  { "refs_blob_corruption", "Refs Blob Corruption Test", run_refs_blob_corruption },
  { "refresh_error_propagation", "Refresh Error Propagation Test", run_refresh_error_propagation },
  { "conflicts_blob_corruption", "Conflicts Blob Corruption Test", run_conflicts_blob_corruption },
  { "status_error_propagation", "Status Error Propagation Test", run_status_error_propagation },
  { "remote_refs_corruption", "Remote Refs Corruption Test", run_remote_refs_corruption },
  { "chunk_walk_corruption", "Chunk Walk Corruption Test", run_chunk_walk_corruption }
};

static int run_case_by_name(const char *zName){
  int i;
  for(i=0; i<(int)(sizeof(aCases)/sizeof(aCases[0])); i++){
    if( strcmp(aCases[i].zName, zName)==0 ){
      aCases[i].xRun();
      return 1;
    }
  }
  return 0;
}

static void print_usage(const char *argv0){
  int i;
  fprintf(stderr, "Usage: %s all|", argv0);
  for(i=0; i<(int)(sizeof(aCases)/sizeof(aCases[0])); i++){
    fprintf(stderr, "%s%s", i ? "|" : "", aCases[i].zName);
  }
  fprintf(stderr, "\n");
}

int main(int argc, char **argv){
  int i;
  if( argc!=2 ){
    print_usage(argv[0]);
    return 2;
  }

  if( strcmp(argv[1], "all")==0 ){
    printf("=== DoltLite Regression Tests ===\n\n");
    for(i=0; i<(int)(sizeof(aCases)/sizeof(aCases[0])); i++){
      aCases[i].xRun();
      if( i+1 < (int)(sizeof(aCases)/sizeof(aCases[0])) ){
        printf("\n");
      }
    }
  }else if( !run_case_by_name(argv[1]) ){
    print_usage(argv[0]);
    return 2;
  }

  printf("\nResults: %d passed, %d failed out of %d tests\n",
         nPass, nFail, nPass+nFail);
  return nFail ? 1 : 0;
}
