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
**   ./doltlite_regression_test_c ancestor_missing_start
**   ./doltlite_regression_test_c pull_persist_failure
**   ./doltlite_regression_test_c clone_persist_failure
**   ./doltlite_regression_test_c resolve_ref_non_commit
**   ./doltlite_regression_test_c commit_parent_limit
**   ./doltlite_regression_test_c merge_persist_failure
**   ./doltlite_regression_test_c cherry_pick_stale_branch
**   ./doltlite_regression_test_c branches_metadata_corruption
**   ./doltlite_regression_test_c gc_rewrite_failure
**   ./doltlite_regression_test_c record_decode_corruption
**   ./doltlite_regression_test_c reload_refs_transactional
**   ./doltlite_regression_test_c refresh_refs_corruption_preserves_state
**   ./doltlite_regression_test_c prolly_node_corruption
**   ./doltlite_regression_test_c truncated_wal_rejected
**   ./doltlite_regression_test_c refresh_open_path_transactional
**   ./doltlite_regression_test_c wal_offset_corruption
**   ./doltlite_regression_test_c integrity_check_walks_nodes
**   ./doltlite_regression_test_c memory_chunk_lookup_corruption
**   ./doltlite_regression_test_c prolly_diff_record_corruption
**   ./doltlite_regression_test_c integrity_check_repo_state
**   ./doltlite_regression_test_c integrity_check_session_merge_state
**   ./doltlite_regression_test_c btree_commit_failure_transactional
**   ./doltlite_regression_test_c mutmap_empty_reverse_iter
**   ./doltlite_regression_test_c working_set_refreshes_staged_across_connections
**   ./doltlite_regression_test_c reopen_preserves_staged_working_set
**   ./doltlite_regression_test_c begin_write_refreshes_working_set_metadata
**   ./doltlite_regression_test_c begin_write_from_stale_read_snapshot
**   ./doltlite_regression_test_c open_rejects_corrupt_working_set
**   ./doltlite_regression_test_c prolly_blob_cursor_seek_past_max
**   ./doltlite_regression_test_c prolly_cursor_empty_leaf_root
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "sqlite3.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_commit.h"
#include "doltlite_chunk_walk.h"
#include "doltlite_ancestor.h"
#include "doltlite_internal.h"
#include "doltlite_record.h"
#include "prolly_cache.h"
#include "prolly_cursor.h"
#include "prolly_diff.h"
#include "prolly_node.h"
#include "prolly_mutmap.h"

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

static int stmt_column_text_equals(sqlite3_stmt *stmt, int iCol, const char *zExpect){
  const unsigned char *z = sqlite3_column_text(stmt, iCol);
  if( !zExpect ) return z==0;
  return z && strcmp((const char*)z, zExpect)==0;
}

typedef struct DiffCountCtx DiffCountCtx;
struct DiffCountCtx {
  int nChange;
};

static int count_diff_change(void *pCtx, const ProllyDiffChange *pChange){
  DiffCountCtx *p = (DiffCountCtx*)pCtx;
  (void)pChange;
  p->nChange++;
  return SQLITE_OK;
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
static int gFailWriteOnce = 0;
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
  if( gFailWriteOnce>0 ){
    gFailWriteOnce--;
    gFailHits++;
    return SQLITE_IOERR_WRITE;
  }
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

static int persist_working_set(sqlite3 *db){
  return doltlitePersistWorkingSet(db);
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
  check("session_branch_restored_after_error",
    strcmp(exec1(db1, "SELECT active_branch()"), "main")==0);

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
  sqlite3 *cloneDb = 0;
  ChunkStore cs;
  ProllyHash badRefsHash;
  u8 badBlob[] = { 5, 0, 0, 0, 0, 1, 0, 0 };
  char remotePath[256];
  char localPath[256];
  char clonePath[256];
  int rc;

  printf("=== Remote Refs Corruption Test ===\n\n");
  make_dbpath(remotePath, sizeof(remotePath), "test_remote_refs_corruption_remote");
  make_dbpath(localPath, sizeof(localPath), "test_remote_refs_corruption_local");
  make_dbpath(clonePath, sizeof(clonePath), "test_remote_refs_corruption_clone");
  remove_db(remotePath);
  remove_db(localPath);
  remove_db(clonePath);

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

  rc = execsql_silent(localDb, "SELECT dolt_push('origin','main');");
  check("push_surfaces_corrupt_refs", rc!=SQLITE_OK);

  check("open_clone_db", open_db(clonePath, &cloneDb)==SQLITE_OK);
  if( cloneDb ){
    char sql[1024];
    snprintf(sql, sizeof(sql), "SELECT dolt_clone('file://%s')", remotePath);
    rc = execsql_silent(cloneDb, sql);
    check("clone_surfaces_corrupt_refs", rc!=SQLITE_OK);
  }

  sqlite3_close(cloneDb);
  sqlite3_close(localDb);
  remove_db(remotePath);
  remove_db(localPath);
  remove_db(clonePath);
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

static void run_ancestor_missing_start(void){
  sqlite3 *db = 0;
  char dbpath[256];
  ProllyHash badHash;
  ProllyHash headHash;
  ProllyHash ancestor;
  int rc;

  printf("=== Ancestor Missing Start Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_ancestor_missing_start");
  remove_db(dbpath);

  check("open_db", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo", execsql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);

  doltliteGetSessionHead(db, &headHash);
  memset(&badHash, 0x5a, sizeof(badHash));
  rc = doltliteFindAncestor(db, &badHash, &headHash, &ancestor);
  check("missing_start_commit_returns_notfound", rc==SQLITE_NOTFOUND);

  sqlite3_close(db);
  remove_db(dbpath);
}

static void run_pull_persist_failure(void){
  sqlite3 *localDb = 0;
  sqlite3 *remoteDb = 0;
  char localPath[256];
  char remotePath[256];
  char sql[1024];
  const char *res;

  printf("=== Pull Persist Failure Test ===\n\n");
  make_dbpath(localPath, sizeof(localPath), "test_pull_persist_failure_local");
  make_dbpath(remotePath, sizeof(remotePath), "test_pull_persist_failure_remote");
  remove_db(localPath);
  remove_db(remotePath);

  gFailSyncOnce = 0;
  gFailHits = 0;
  check("register_fail_vfs_for_pull", registerFailVfs()==SQLITE_OK);
  check("open_local_fail_db", open_fail_db(localPath, &localDb)==SQLITE_OK);
  check("open_remote_db", open_db(remotePath, &remoteDb)==SQLITE_OK);

  snprintf(sql, sizeof(sql),
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "SELECT dolt_remote('add','origin','file://%s');"
    "SELECT dolt_push('origin','main');",
    remotePath);
  check("setup_local_and_push", execsql(localDb, sql)==SQLITE_OK);

  sqlite3_close(remoteDb);
  remoteDb = 0;
  check("reopen_remote_db", open_db(remotePath, &remoteDb)==SQLITE_OK);
  check("advance_remote", execsql(remoteDb,
    "INSERT INTO t VALUES(2,'b');"
    "SELECT dolt_commit('-A', '-m', 'remote update');")==SQLITE_OK);

  gFailHits = 0;
  gFailSyncOnce = 2;
  res = exec1(localDb, "SELECT dolt_pull('origin','main')");
  check("pull_failure_was_injected", gFailHits>0);
  check("pull_returns_error_on_persist_failure", strstr(res, "ERROR:")!=0);
  check("pull_branch_stays_main",
    strcmp(exec1(localDb, "SELECT active_branch()"), "main")==0);
  check("pull_head_data_restored",
    strcmp(exec1(localDb, "SELECT count(*) FROM t"), "1")==0);

  sqlite3_close(remoteDb);
  sqlite3_close(localDb);
  remove_db(localPath);
  remove_db(remotePath);
}

static void run_clone_persist_failure(void){
  sqlite3 *localDb = 0;
  sqlite3 *remoteDb = 0;
  char localPath[256];
  char remotePath[256];
  char sql[1024];
  const char *res;

  printf("=== Clone Persist Failure Test ===\n\n");
  make_dbpath(localPath, sizeof(localPath), "test_clone_persist_failure_local");
  make_dbpath(remotePath, sizeof(remotePath), "test_clone_persist_failure_remote");
  remove_db(localPath);
  remove_db(remotePath);

  gFailSyncOnce = 0;
  gFailHits = 0;
  check("register_fail_vfs_for_clone", registerFailVfs()==SQLITE_OK);
  check("open_remote_db", open_db(remotePath, &remoteDb)==SQLITE_OK);
  check("setup_remote_repo", execsql(remoteDb,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);
  sqlite3_close(remoteDb);
  remoteDb = 0;

  check("open_local_fail_db", open_fail_db(localPath, &localDb)==SQLITE_OK);
  snprintf(sql, sizeof(sql), "SELECT dolt_clone('file://%s')", remotePath);
  gFailHits = 0;
  gFailSyncOnce = 2;
  res = exec1(localDb, sql);
  check("clone_failure_was_injected", gFailHits>0);
  check("clone_returns_error_on_persist_failure", strstr(res, "ERROR:")!=0);
  check("clone_does_not_leave_origin_remote",
    strcmp(exec1(localDb, "SELECT count(*) FROM dolt_remotes"), "0")==0);
  check("clone_restores_empty_catalog",
    strcmp(exec1(localDb,
      "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='t'"), "0")==0);

  sqlite3_close(localDb);
  remove_db(localPath);
  remove_db(remotePath);
}

static void run_resolve_ref_non_commit(void){
  sqlite3 *db = 0;
  ChunkStore *cs = 0;
  char dbpath[256];
  ProllyHash headHash;
  ProllyHash resolved;
  DoltliteCommit commit;
  char hex[PROLLY_HASH_SIZE*2 + 1];
  int rc;

  printf("=== Resolve Ref Non-Commit Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_resolve_ref_non_commit");
  remove_db(dbpath);

  check("open_db", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo", execsql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);

  doltliteGetSessionHead(db, &headHash);
  memset(&commit, 0, sizeof(commit));
  check("load_head_commit", doltliteLoadCommit(db, &headHash, &commit)==SQLITE_OK);
  doltliteHashToHex(&commit.catalogHash, hex);

  rc = doltliteResolveRef(db, hex, &resolved);
  check("hex_catalog_hash_is_not_resolved_as_commit", rc!=SQLITE_OK);

  cs = doltliteGetChunkStore(db);
  check("have_chunk_store", cs!=0);
  if( cs ){
    check("corrupt_branch_ref_to_catalog_hash",
      chunkStoreUpdateBranch(cs, "main", &commit.catalogHash)==SQLITE_OK);
    rc = doltliteResolveRef(db, "main", &resolved);
    check("branch_catalog_hash_is_not_resolved_as_commit", rc!=SQLITE_OK);
  }

  doltliteCommitClear(&commit);
  sqlite3_close(db);
  remove_db(dbpath);
}

static void run_commit_parent_limit(void){
  DoltliteCommit commit;
  u8 *pBlob = 0;
  int nBlob = 0;
  int rc;

  printf("=== Commit Parent Limit Test ===\n\n");
  memset(&commit, 0, sizeof(commit));
  commit.nParents = DOLTLITE_MAX_PARENTS + 1;

  rc = doltliteCommitSerialize(&commit, &pBlob, &nBlob);
  check("serialize_rejects_too_many_parents", rc==SQLITE_TOOBIG);
  sqlite3_free(pBlob);
}

static void run_merge_persist_failure(void){
  sqlite3 *db = 0;
  char dbpath[256];
  const char *res;

  printf("=== Merge Persist Failure Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_merge_persist_failure");
  remove_db(dbpath);

  gFailSyncOnce = 0;
  gFailHits = 0;
  check("register_fail_vfs_for_merge", registerFailVfs()==SQLITE_OK);
  check("open_fail_db", open_fail_db(dbpath, &db)==SQLITE_OK);
  check("setup_ff_merge_repo", execsql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "SELECT dolt_branch('feature');"
    "SELECT dolt_checkout('feature');"
    "INSERT INTO t VALUES(2,'b');"
    "SELECT dolt_commit('-A', '-m', 'feature work');"
    "SELECT dolt_checkout('main');")==SQLITE_OK);

  gFailHits = 0;
  gFailSyncOnce = 1;
  res = exec1(db, "SELECT dolt_merge('feature')");
  check("merge_failure_was_injected", gFailHits>0);
  check("merge_returns_error_on_persist_failure", strstr(res, "ERROR:")!=0);
  check("merge_restores_branch_name",
    strcmp(exec1(db, "SELECT active_branch()"), "main")==0);

  sqlite3_close(db);
  remove_db(dbpath);
}

static void run_cherry_pick_stale_branch(void){
  sqlite3 *db1 = 0;
  sqlite3 *db2 = 0;
  sqlite3 *db3 = 0;
  char dbpath[256];
  char sql[256];
  char featHash[128];
  const char *res;

  printf("=== Cherry-pick Stale Branch Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_cherry_pick_stale_branch");
  remove_db(dbpath);

  check("open_db1", open_db(dbpath, &db1)==SQLITE_OK);
  check("setup_init", execsql(db1,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "SELECT dolt_branch('feature');"
    "SELECT dolt_checkout('feature');"
    "INSERT INTO t VALUES(2,'feat');"
    "SELECT dolt_commit('-A', '-m', 'feature work');"
    "SELECT dolt_checkout('main');")==SQLITE_OK);

  snprintf(featHash, sizeof(featHash), "%s",
           exec1(db1, "SELECT hash FROM dolt_branches WHERE name='feature'"));

  check("open_db2", open_db(dbpath, &db2)==SQLITE_OK);
  check("db2_sees_old_main",
    strcmp(exec1(db2, "SELECT count(*) FROM t"), "1")==0);

  check("advance_main_in_db1", execsql(db1,
    "INSERT INTO t VALUES(3,'main');"
    "SELECT dolt_commit('-A', '-m', 'main work');")==SQLITE_OK);

  snprintf(sql, sizeof(sql), "SELECT dolt_cherry_pick('%s')", featHash);
  res = exec1(db2, sql);
  check("stale_cherry_pick_returns_error", strstr(res, "ERROR:")!=0);

  check("open_db3", open_db(dbpath, &db3)==SQLITE_OK);
  check("stale_cherry_pick_does_not_persist_feature_row",
    strcmp(exec1(db3, "SELECT count(*) FROM t"), "2")==0);

  sqlite3_close(db3);
  sqlite3_close(db2);
  sqlite3_close(db1);
  remove_db(dbpath);
}

static void run_branches_metadata_corruption(void){
  sqlite3 *db = 0;
  ChunkStore cs;
  char dbpath[256];
  ProllyHash badHash;
  int iFeature = -1;
  int rc;

  printf("=== Branches Metadata Corruption Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_branches_metadata_corruption");
  remove_db(dbpath);

  check("open_db", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo", execsql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "SELECT dolt_branch('feature');"
    "SELECT dolt_checkout('feature');"
    "INSERT INTO t VALUES(2,'feat');"
    "SELECT dolt_add('-A');"
    "SELECT dolt_checkout('main');")==SQLITE_OK);
  sqlite3_close(db);
  db = 0;

  memset(&badHash, 0x7c, sizeof(badHash));
  memset(&cs, 0, sizeof(cs));
  check("open_store", chunkStoreOpen(&cs, sqlite3_vfs_find(0), dbpath,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  check("lock_store", chunkStoreLockAndRefresh(&cs)==SQLITE_OK);
  {
    int i;
    for(i=0; i<cs.nBranches; i++){
      if( cs.aBranches[i].zName && strcmp(cs.aBranches[i].zName, "feature")==0 ){
        iFeature = i;
        break;
      }
    }
  }
  check("have_feature_branch", iFeature >= 0);
  if( iFeature >= 0 ){
    memcpy(&cs.aBranches[iFeature].commitHash, &badHash, sizeof(ProllyHash));
    memcpy(&cs.aBranches[iFeature].workingSetHash, &badHash, sizeof(ProllyHash));
  }
  check("serialize_corrupt_branch_refs", chunkStoreSerializeRefs(&cs)==SQLITE_OK);
  check("commit_corrupt_branch_refs", chunkStoreCommit(&cs)==SQLITE_OK);
  chunkStoreUnlock(&cs);
  chunkStoreClose(&cs);

  check("reopen_db", open_db(dbpath, &db)==SQLITE_OK);
  rc = execsql_silent(db,
    "SELECT latest_commit_message FROM dolt_branches WHERE name='feature';");
  check("branches_latest_commit_surfaces_corruption", rc!=SQLITE_OK);
  rc = execsql_silent(db,
    "SELECT dirty FROM dolt_branches WHERE name='feature';");
  check("branches_dirty_surfaces_corruption", rc!=SQLITE_OK);

  sqlite3_close(db);
  remove_db(dbpath);
}

static void run_gc_rewrite_failure(void){
  sqlite3 *db = 0;
  sqlite3 *db2 = 0;
  char dbpath[256];
  const char *res;

  printf("=== GC Rewrite Failure Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_gc_rewrite_failure");
  remove_db(dbpath);

  gFailSyncOnce = 0;
  gFailHits = 0;
  check("register_fail_vfs_for_gc", registerFailVfs()==SQLITE_OK);
  check("open_fail_db", open_fail_db(dbpath, &db)==SQLITE_OK);
  check("setup_gc_repo", execsql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "INSERT INTO t VALUES(2,'b');"
    "SELECT dolt_commit('-A', '-m', 'second');")==SQLITE_OK);

  gFailHits = 0;
  gFailSyncOnce = 1;
  res = exec1(db, "SELECT dolt_gc()");
  check("gc_failure_was_injected", gFailHits>0);
  check("gc_returns_error_on_rewrite_failure", strstr(res, "ERROR:")!=0);
  check("gc_connection_still_reads_data",
    strcmp(exec1(db, "SELECT count(*) FROM t"), "2")==0);
  check("gc_connection_still_reads_log",
    strcmp(exec1(db, "SELECT count(*) FROM dolt_log"), "3")==0);

  sqlite3_close(db);
  check("reopen_db_after_gc_failure", open_db(dbpath, &db2)==SQLITE_OK);
  check("gc_reopen_reads_data",
    strcmp(exec1(db2, "SELECT count(*) FROM t"), "2")==0);
  check("gc_reopen_reads_log",
    strcmp(exec1(db2, "SELECT count(*) FROM dolt_log"), "3")==0);

  sqlite3_close(db2);
  remove_db(dbpath);
}

static void run_record_decode_corruption(void){
  static const u8 badRecord[] = {
    0x05, 0x01
  };
  char *z;

  printf("=== Record Decode Corruption Test ===\n\n");
  z = doltliteDecodeRecord(badRecord, (int)sizeof(badRecord));
  check("corrupt_record_decodes_to_null", z==0);
  sqlite3_free(z);
}

static void run_reload_refs_transactional(void){
  ChunkStore cs;
  ProllyHash emptyHash;
  ProllyHash badHash;
  static const u8 badBlob[] = { 6, 0, 0, 0 };
  int nBranchesBefore;
  char *zDefaultBefore = 0;
  char *zBranchBefore = 0;
  int rc;

  printf("=== Reload Refs Transactional Test ===\n\n");
  memset(&emptyHash, 0, sizeof(emptyHash));
  check("open_mem_store",
        chunkStoreOpen(&cs, sqlite3_vfs_find(0), ":memory:", 0)==SQLITE_OK);
  check("set_default_branch",
        chunkStoreSetDefaultBranch(&cs, "main")==SQLITE_OK);
  check("add_branch",
        chunkStoreAddBranch(&cs, "main", &emptyHash)==SQLITE_OK);
  check("serialize_good_refs",
        chunkStoreSerializeRefs(&cs)==SQLITE_OK);
  nBranchesBefore = cs.nBranches;
  zDefaultBefore = sqlite3_mprintf("%s", cs.zDefaultBranch ? cs.zDefaultBranch : "");
  zBranchBefore = (cs.aBranches && cs.nBranches>0 && cs.aBranches[0].zName)
                ? sqlite3_mprintf("%s", cs.aBranches[0].zName)
                : sqlite3_mprintf("");

  check("load_bad_refs_blob_as_chunk",
        chunkStorePut(&cs, badBlob, (int)sizeof(badBlob), &badHash)==SQLITE_OK);

  rc = chunkStoreLoadRefsFromBlob(&cs, badBlob, (int)sizeof(badBlob));
  check("load_refs_blob_returns_corrupt", rc==SQLITE_CORRUPT);
  check("load_refs_blob_preserves_default_branch",
        cs.zDefaultBranch && strcmp(cs.zDefaultBranch, zDefaultBefore)==0);
  check("load_refs_blob_preserves_branch_count", cs.nBranches==nBranchesBefore);
  check("load_refs_blob_preserves_branch_name",
        nBranchesBefore==0 ||
        (cs.aBranches && cs.aBranches[0].zName
         && strcmp(cs.aBranches[0].zName, zBranchBefore)==0));

  memcpy(&cs.refsHash, &badHash, sizeof(badHash));
  rc = chunkStoreReloadRefs(&cs);
  check("reload_refs_returns_corrupt", rc==SQLITE_CORRUPT);
  check("reload_refs_preserves_default_branch",
        cs.zDefaultBranch && strcmp(cs.zDefaultBranch, zDefaultBefore)==0);
  check("reload_refs_preserves_branch_count", cs.nBranches==nBranchesBefore);
  check("reload_refs_preserves_branch_name",
        nBranchesBefore==0 ||
        (cs.aBranches && cs.aBranches[0].zName
         && strcmp(cs.aBranches[0].zName, zBranchBefore)==0));

  sqlite3_free(zDefaultBefore);
  sqlite3_free(zBranchBefore);
  chunkStoreClose(&cs);
}

static void run_refresh_refs_corruption_preserves_state(void){
  sqlite3 *db = 0;
  ChunkStore cs1;
  ChunkStore cs2;
  ProllyHash badHash;
  ProllyHash refsHashBefore;
  static const u8 badBlob[] = { 6, 0, 0, 0 };
  char dbpath[256];
  int nBranchesBefore;
  char *zDefaultBefore = 0;
  char *zBranchBefore = 0;
  int changed = -1;
  int rc;

  printf("=== Refresh Corrupt Refs State Preservation Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_refresh_refs_preserves_state");
  remove_db(dbpath);

  check("open_db", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo", execsql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);
  sqlite3_close(db);
  db = 0;

  check("open_store_1", chunkStoreOpen(&cs1, sqlite3_vfs_find(0), dbpath,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  refsHashBefore = cs1.refsHash;
  nBranchesBefore = cs1.nBranches;
  zDefaultBefore = sqlite3_mprintf("%s", cs1.zDefaultBranch ? cs1.zDefaultBranch : "");
  zBranchBefore = (cs1.aBranches && cs1.nBranches>0 && cs1.aBranches[0].zName)
                ? sqlite3_mprintf("%s", cs1.aBranches[0].zName)
                : sqlite3_mprintf("");

  check("open_store_2", chunkStoreOpen(&cs2, sqlite3_vfs_find(0), dbpath,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  check("lock_store_2", chunkStoreLockAndRefresh(&cs2)==SQLITE_OK);
  check("put_bad_refs_chunk",
        chunkStorePut(&cs2, badBlob, (int)sizeof(badBlob), &badHash)==SQLITE_OK);
  memcpy(&cs2.refsHash, &badHash, sizeof(badHash));
  check("commit_bad_refs_hash", chunkStoreCommit(&cs2)==SQLITE_OK);
  chunkStoreUnlock(&cs2);
  chunkStoreClose(&cs2);

  rc = chunkStoreRefreshIfChanged(&cs1, &changed);
  check("refresh_returns_error_for_corrupt_refs", rc==SQLITE_CORRUPT);
  check("refresh_does_not_mark_changed", changed==0);
  check("refresh_preserves_default_branch",
        cs1.zDefaultBranch && strcmp(cs1.zDefaultBranch, zDefaultBefore)==0);
  check("refresh_preserves_branch_count", cs1.nBranches==nBranchesBefore);
  check("refresh_preserves_branch_name",
        nBranchesBefore==0 ||
        (cs1.aBranches && cs1.aBranches[0].zName
         && strcmp(cs1.aBranches[0].zName, zBranchBefore)==0));
  check("refresh_preserves_refs_hash",
        memcmp(&cs1.refsHash, &refsHashBefore, sizeof(ProllyHash))==0);
  sqlite3_free(zDefaultBefore);
  sqlite3_free(zBranchBefore);
  chunkStoreClose(&cs1);
  remove_db(dbpath);
}

static void run_prolly_node_corruption(void){
  static const u8 badIntKeyNode[] = {
    'D','O','N','P',
    0,
    1, 0,
    PROLLY_NODE_INTKEY,
    0,0,0,0,
    7,0,0,0,
    0,0,0,0,
    1,0,0,0,
    1,2,3,4,5,6,7,
    0x2a
  };
  ProllyNode node;

  printf("=== Prolly Node Corruption Test ===\n\n");
  check("intkey_width_corruption_is_rejected",
        prollyNodeParse(&node, badIntKeyNode, (int)sizeof(badIntKeyNode))==SQLITE_CORRUPT);
  {
    static const u8 badEmptyInternalNode[] = {
      'D','O','N','P',
      1,
      0, 0,
      PROLLY_NODE_BLOBKEY
    };
    check("empty_internal_node_is_rejected",
          prollyNodeParse(&node, badEmptyInternalNode,
                          (int)sizeof(badEmptyInternalNode))==SQLITE_CORRUPT);
  }
}

static void run_memory_chunk_lookup_corruption(void){
  ChunkStore cs;
  ProllyHash h;
  static const u8 payload[] = { 1, 2, 3, 4 };
  u8 *pOut = 0;
  int nOut = 0;
  int rc;

  printf("=== Memory Chunk Lookup Corruption Test ===\n\n");
  check("open_mem_store_for_lookup",
        chunkStoreOpen(&cs, sqlite3_vfs_find(0), ":memory:", 0)==SQLITE_OK);
  check("put_chunk_for_lookup",
        chunkStorePut(&cs, payload, (int)sizeof(payload), &h)==SQLITE_OK);
  check("commit_mem_store_lookup",
        chunkStoreCommit(&cs)==SQLITE_OK);
  check("have_committed_index_entry", cs.nIndex > 0);
  if( cs.nIndex > 0 ){
    cs.aIndex[0].offset = cs.nWriteBuf + 100;
  }
  rc = chunkStoreGet(&cs, &h, &pOut, &nOut);
  check("memory_lookup_corruption_returns_corrupt", rc==SQLITE_CORRUPT);
  sqlite3_free(pOut);
  chunkStoreClose(&cs);
}

static void run_prolly_diff_record_corruption(void){
  static const u8 badRecord[] = { 0x05, 0x01 };
  int equal = 0;
  int rc;

  printf("=== Prolly Diff Record Corruption Test ===\n\n");
  rc = prollyValuesEqual(badRecord, (int)sizeof(badRecord),
                         badRecord, (int)sizeof(badRecord), &equal);
  check("prolly_diff_surfaces_record_corruption", rc==SQLITE_CORRUPT);
  check("corrupt_records_not_reported_equal", equal==0);
}

static void run_integrity_check_repo_state(void){
  sqlite3 *db = 0;
  ChunkStore cs;
  char dbpath[256];
  ProllyHash badHash;
  int nErr = 0;
  int rc;

  printf("=== Integrity Check Repository State Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_integrity_check_repo_state");
  remove_db(dbpath);

  check("open_db_repo_state", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_state", execsql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);
  sqlite3_close(db);
  db = 0;

  memset(&badHash, 0x5d, sizeof(badHash));
  check("open_store_repo_state", chunkStoreOpen(&cs, sqlite3_vfs_find(0), dbpath,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  check("lock_store_repo_state", chunkStoreLockAndRefresh(&cs)==SQLITE_OK);
  check("set_bad_branch_working_set",
        chunkStoreSetBranchWorkingSet(&cs, "main", &badHash)==SQLITE_OK);
  check("serialize_corrupt_branch_refs", chunkStoreSerializeRefs(&cs)==SQLITE_OK);
  check("commit_bad_branch_working_set", chunkStoreCommit(&cs)==SQLITE_OK);
  chunkStoreUnlock(&cs);
  chunkStoreClose(&cs);

  check("reopen_repo_state_db", open_db(dbpath, &db)==SQLITE_OK);
  rc = doltliteCheckRepoGraphIntegrity(db->aDb[0].pBt, 100, &nErr);
  check("repo_graph_integrity_call_succeeds", rc==SQLITE_OK);
  check("integrity_check_reports_repo_state_corruption", nErr>0);
  sqlite3_close(db);
  remove_db(dbpath);
}

static void run_integrity_check_session_merge_state(void){
  sqlite3 *db = 0;
  char dbpath[256];
  ProllyHash badHash;
  int nErr = 0;
  int rc;

  printf("=== Integrity Check Session Merge State Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_integrity_check_session_merge_state");
  remove_db(dbpath);

  check("open_db_session_merge_state", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_session_merge_state", execsql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);

  memset(&badHash, 0x4c, sizeof(badHash));
  doltliteSetSessionMergeState(db, 1, &badHash, &badHash);

  rc = doltliteCheckRepoGraphIntegrity(db->aDb[0].pBt, 100, &nErr);
  check("session_merge_state_integrity_call_succeeds", rc==SQLITE_OK);
  check("integrity_check_reports_session_merge_state_corruption", nErr>0);

  sqlite3_close(db);
  remove_db(dbpath);
}

static void run_prepared_stmt_reuse_after_commit(void){
  sqlite3 *db = 0;
  sqlite3_stmt *stmt = 0;
  char dbpath[256];
  int rc;

  printf("=== Prepared Statement Reuse After Commit Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_prepared_stmt_reuse_after_commit");
  remove_db(dbpath);

  check("open_db_stmt_commit", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_stmt_commit", execsql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);

  rc = sqlite3_prepare_v2(db, "SELECT id, v FROM t ORDER BY id", -1, &stmt, 0);
  check("prepare_stmt_commit", rc==SQLITE_OK);
  if( rc==SQLITE_OK ){
    check("stmt_commit_first_row", sqlite3_step(stmt)==SQLITE_ROW);
    check("stmt_commit_first_id", sqlite3_column_int(stmt, 0)==1);
    check("stmt_commit_first_val", stmt_column_text_equals(stmt, 1, "a"));
    check("stmt_commit_done", sqlite3_step(stmt)==SQLITE_DONE);
    check("stmt_commit_reset_initial", sqlite3_reset(stmt)==SQLITE_OK);

    check("commit_new_row_same_conn", execsql(db,
      "INSERT INTO t VALUES(2,'b');"
      "SELECT dolt_commit('-A', '-m', 'second');")==SQLITE_OK);

    check("stmt_commit_reuse_row1", sqlite3_step(stmt)==SQLITE_ROW);
    check("stmt_commit_reuse_row1_id", sqlite3_column_int(stmt, 0)==1);
    check("stmt_commit_reuse_row1_val", stmt_column_text_equals(stmt, 1, "a"));
    check("stmt_commit_reuse_row2", sqlite3_step(stmt)==SQLITE_ROW);
    check("stmt_commit_reuse_row2_id", sqlite3_column_int(stmt, 0)==2);
    check("stmt_commit_reuse_row2_val", stmt_column_text_equals(stmt, 1, "b"));
    check("stmt_commit_reuse_done", sqlite3_step(stmt)==SQLITE_DONE);
    check("stmt_commit_reset_final", sqlite3_reset(stmt)==SQLITE_OK);
  }

  sqlite3_finalize(stmt);
  sqlite3_close(db);
  remove_db(dbpath);
}

static void run_prepared_stmt_reuse_after_schema_checkout(void){
  sqlite3 *db = 0;
  sqlite3_stmt *stmt = 0;
  char dbpath[256];
  int rc;

  printf("=== Prepared Statement Reuse After Schema Checkout Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_prepared_stmt_reuse_after_schema_checkout");
  remove_db(dbpath);

  check("open_db_stmt_checkout", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_stmt_checkout", execsql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "SELECT dolt_checkout('-b', 'schema_branch');"
    "ALTER TABLE t ADD COLUMN x INT;"
    "UPDATE t SET x=7;"
    "SELECT dolt_commit('-A', '-m', 'schema');"
    "SELECT dolt_checkout('main');")==SQLITE_OK);

  rc = sqlite3_prepare_v2(db, "SELECT * FROM t ORDER BY id", -1, &stmt, 0);
  check("prepare_stmt_checkout", rc==SQLITE_OK);
  if( rc==SQLITE_OK ){
    check("stmt_checkout_main_row", sqlite3_step(stmt)==SQLITE_ROW);
    check("stmt_checkout_main_colcount", sqlite3_column_count(stmt)==2);
    check("stmt_checkout_main_id", sqlite3_column_int(stmt, 0)==1);
    check("stmt_checkout_main_val", stmt_column_text_equals(stmt, 1, "a"));
    check("stmt_checkout_main_done", sqlite3_step(stmt)==SQLITE_DONE);
    check("stmt_checkout_reset_initial", sqlite3_reset(stmt)==SQLITE_OK);

    check("checkout_schema_branch", execsql(db,
      "SELECT dolt_checkout('schema_branch');")==SQLITE_OK);

    check("stmt_checkout_branch_row", sqlite3_step(stmt)==SQLITE_ROW);
    check("stmt_checkout_branch_colcount", sqlite3_column_count(stmt)==3);
    check("stmt_checkout_branch_id", sqlite3_column_int(stmt, 0)==1);
    check("stmt_checkout_branch_val", stmt_column_text_equals(stmt, 1, "a"));
    check("stmt_checkout_branch_extra", sqlite3_column_int(stmt, 2)==7);
    check("stmt_checkout_branch_done", sqlite3_step(stmt)==SQLITE_DONE);
    check("stmt_checkout_reset_final", sqlite3_reset(stmt)==SQLITE_OK);
  }

  sqlite3_finalize(stmt);
  sqlite3_close(db);
  remove_db(dbpath);
}

static void run_working_set_refreshes_staged_across_connections(void){
  sqlite3 *db1 = 0;
  sqlite3 *db2 = 0;
  char dbpath[256];
  ProllyHash stagedBefore;
  ProllyHash stagedExpected;
  ProllyHash stagedAfter;

  printf("=== Working Set Refreshes Staged Across Connections Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_working_set_refreshes_staged_across_connections");
  remove_db(dbpath);

  check("open_db1_for_cross_conn_staged", open_db(dbpath, &db1)==SQLITE_OK);
  check("create_table_for_cross_conn_staged",
        execsql(db1, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);")==SQLITE_OK);
  check("insert_row_for_cross_conn_staged",
        execsql(db1, "INSERT INTO t VALUES(1,'a');")==SQLITE_OK);
  check("initial_commit_for_cross_conn_staged",
        execsql(db1, "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);
  check("open_db2_for_cross_conn_staged", open_db(dbpath, &db2)==SQLITE_OK);

  doltliteGetSessionStaged(db2, &stagedBefore);
  check("db1_stage_new_row", execsql(db1,
    "INSERT INTO t VALUES(2,'b');"
    "SELECT dolt_add('-A');")==SQLITE_OK);
  doltliteGetSessionStaged(db1, &stagedExpected);
  check("staged_hash_changed_after_dolt_add",
        memcmp(&stagedExpected, &stagedBefore, sizeof(ProllyHash))!=0);

  check("db2_refreshes_working_catalog_on_read",
        strcmp(exec1(db2, "SELECT count(*) FROM t"), "2")==0);
  doltliteGetSessionStaged(db2, &stagedAfter);
  check("db2_refreshes_staged_hash",
        memcmp(&stagedAfter, &stagedExpected, sizeof(ProllyHash))==0);

  sqlite3_close(db2);
  sqlite3_close(db1);
  remove_db(dbpath);
}

static void run_reopen_preserves_staged_working_set(void){
  sqlite3 *db = 0;
  char dbpath[256];
  ProllyHash stagedBeforeClose;
  ProllyHash stagedAfterReopen;

  printf("=== Reopen Preserves Staged Working Set Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_reopen_preserves_staged_working_set");
  remove_db(dbpath);

  check("open_db_for_reopen_staged", open_db(dbpath, &db)==SQLITE_OK);
  check("create_table_for_reopen_staged",
        execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);")==SQLITE_OK);
  check("insert_base_row_for_reopen_staged",
        execsql(db, "INSERT INTO t VALUES(1,'a');")==SQLITE_OK);
  check("commit_base_row_for_reopen_staged",
        execsql(db, "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);
  check("stage_new_row_for_reopen_staged",
        execsql(db,
          "INSERT INTO t VALUES(2,'b');"
          "SELECT dolt_add('-A');")==SQLITE_OK);
  check("staged_status_before_close",
        strcmp(exec1(db, "SELECT count(*) FROM dolt_status WHERE staged=1"), "1")==0);
  doltliteGetSessionStaged(db, &stagedBeforeClose);
  check("staged_hash_before_close_nonempty",
        !prollyHashIsEmpty(&stagedBeforeClose));
  sqlite3_close(db);
  db = 0;

  check("reopen_db_for_reopen_staged", open_db(dbpath, &db)==SQLITE_OK);
  check("staged_status_after_reopen",
        strcmp(exec1(db, "SELECT count(*) FROM dolt_status WHERE staged=1"), "1")==0);
  doltliteGetSessionStaged(db, &stagedAfterReopen);
  check("staged_hash_after_reopen_matches",
        memcmp(&stagedAfterReopen, &stagedBeforeClose, sizeof(ProllyHash))==0);

  sqlite3_close(db);
  remove_db(dbpath);
}

static void run_begin_write_refreshes_working_set_metadata(void){
  sqlite3 *db1 = 0;
  sqlite3 *db2 = 0;
  char dbpath[256];
  ProllyHash stagedBefore;
  ProllyHash stagedExpected;
  ProllyHash mergeExpected;
  ProllyHash conflictsExpected;
  ProllyHash stagedAfter;
  ProllyHash mergeAfter;
  ProllyHash conflictsAfter;
  u8 isMergingAfter = 0;

  printf("=== Begin Write Refreshes Working Set Metadata Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_begin_write_refreshes_working_set_metadata");
  remove_db(dbpath);

  check("open_db1_for_begin_write_refresh", open_db(dbpath, &db1)==SQLITE_OK);
  check("create_table_for_begin_write_refresh",
        execsql(db1, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);")==SQLITE_OK);
  check("insert_row_for_begin_write_refresh",
        execsql(db1, "INSERT INTO t VALUES(1,'a');")==SQLITE_OK);
  check("initial_commit_for_begin_write_refresh",
        execsql(db1, "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);
  check("open_db2_for_begin_write_refresh", open_db(dbpath, &db2)==SQLITE_OK);

  doltliteGetSessionStaged(db2, &stagedBefore);
  check("db1_prepare_new_working_state", execsql(db1,
    "INSERT INTO t VALUES(2,'b');"
    "SELECT dolt_add('-A');")==SQLITE_OK);
  doltliteGetSessionStaged(db1, &stagedExpected);
  memset(&mergeExpected, 0x55, sizeof(mergeExpected));
  memset(&conflictsExpected, 0x66, sizeof(conflictsExpected));
  doltliteSetSessionMergeState(db1, 1, &mergeExpected, &conflictsExpected);
  check("persist_merge_state_for_begin_write_refresh",
        persist_working_set(db1)==SQLITE_OK);

  check("db2_staged_state_is_initially_stale",
        memcmp(&stagedBefore, &stagedExpected, sizeof(ProllyHash))!=0);
  check("db2_begin_immediate_refreshes_branch_state",
        execsql(db2, "BEGIN IMMEDIATE;")==SQLITE_OK);
  check("db2_sees_latest_working_rows_in_write_txn",
        strcmp(exec1(db2, "SELECT count(*) FROM t"), "2")==0);
  doltliteGetSessionStaged(db2, &stagedAfter);
  doltliteGetSessionMergeState(db2, &isMergingAfter, &mergeAfter, &conflictsAfter);
  check("begin_write_refreshes_staged_hash",
        memcmp(&stagedAfter, &stagedExpected, sizeof(ProllyHash))==0);
  check("begin_write_refreshes_merge_flag", isMergingAfter==1);
  check("begin_write_refreshes_merge_commit",
        memcmp(&mergeAfter, &mergeExpected, sizeof(ProllyHash))==0);
  check("begin_write_refreshes_conflicts_catalog",
        memcmp(&conflictsAfter, &conflictsExpected, sizeof(ProllyHash))==0);
  check("rollback_begin_write_refresh_txn", execsql(db2, "ROLLBACK;")==SQLITE_OK);

  sqlite3_close(db2);
  sqlite3_close(db1);
  remove_db(dbpath);
}

static void run_begin_write_from_stale_read_snapshot(void){
  sqlite3 *db1 = 0;
  sqlite3 *db2 = 0;
  sqlite3_stmt *pRead = 0;
  char dbpath[256];
  int rc;

  printf("=== Begin Write From Stale Read Snapshot Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_begin_write_from_stale_read_snapshot");
  remove_db(dbpath);

  check("open_db1_for_stale_snapshot", open_db(dbpath, &db1)==SQLITE_OK);
  check("create_table_for_stale_snapshot",
        execsql(db1, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);")==SQLITE_OK);
  check("insert_row_for_stale_snapshot",
        execsql(db1, "INSERT INTO t VALUES(1,'a');")==SQLITE_OK);
  check("open_db2_for_stale_snapshot", open_db(dbpath, &db2)==SQLITE_OK);

  check("begin_read_txn_for_stale_snapshot", execsql(db2, "BEGIN;")==SQLITE_OK);
  check("prepare_read_in_stale_snapshot",
        sqlite3_prepare_v2(db2, "SELECT count(*) FROM t", -1, &pRead, 0)==SQLITE_OK);
  check("step_read_in_stale_snapshot", sqlite3_step(pRead)==SQLITE_ROW);
  check("read_in_stale_snapshot", sqlite3_column_int(pRead, 0)==1);
  check("db1_autocommit_change_after_read_snapshot",
        execsql(db1, "INSERT INTO t VALUES(2,'b');")==SQLITE_OK);

  rc = execsql_silent(db2, "INSERT INTO t VALUES(3,'c');");
  check("write_upgrade_fails", rc!=SQLITE_OK);
  check("write_upgrade_returns_busy_snapshot",
        sqlite3_extended_errcode(db2)==SQLITE_BUSY_SNAPSHOT);
  sqlite3_finalize(pRead);
  check("rollback_stale_snapshot_txn", execsql(db2, "ROLLBACK;")==SQLITE_OK);
  check("stale_snapshot_did_not_overwrite_rows",
        strcmp(exec1(db1, "SELECT count(*) FROM t"), "2")==0);

  sqlite3_close(db2);
  sqlite3_close(db1);
  remove_db(dbpath);
}

static void run_open_rejects_corrupt_working_set(void){
  sqlite3 *db = 0;
  sqlite3 *db2 = 0;
  ChunkStore cs;
  char dbpath[256];
  int stmtRc;
  ProllyHash badHash;
  int rc;
  static const unsigned char badBlob[] = { 0x01, 0x02, 0x03, 0x04 };

  printf("=== Open Rejects Corrupt Working Set Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_open_rejects_corrupt_working_set");
  remove_db(dbpath);

  check("open_db_for_corrupt_working_set", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_corrupt_working_set", execsql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);
  sqlite3_close(db);
  db = 0;

  check("open_store_for_corrupt_working_set",
        chunkStoreOpen(&cs, sqlite3_vfs_find(0), dbpath,
          SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  check("store_bad_working_set_blob",
        chunkStorePut(&cs, badBlob, (int)sizeof(badBlob), &badHash)==SQLITE_OK);
  check("point_main_branch_at_bad_working_set",
        chunkStoreSetBranchWorkingSet(&cs, "main", &badHash)==SQLITE_OK);
  check("serialize_refs_for_bad_working_set",
        chunkStoreSerializeRefs(&cs)==SQLITE_OK);
  check("commit_bad_working_set_refs", chunkStoreCommit(&cs)==SQLITE_OK);
  chunkStoreClose(&cs);

  rc = sqlite3_open(dbpath, &db2);
  stmtRc = db2 ? execsql_silent(db2, "SELECT count(*) FROM sqlite_master;") : rc;
  check("open_or_first_statement_returns_corrupt_for_bad_working_set",
        rc==SQLITE_CORRUPT || stmtRc==SQLITE_CORRUPT);
  if( db2 ) sqlite3_close(db2);
  remove_db(dbpath);
}

static void run_truncated_wal_is_rejected(void){
  sqlite3 *db = 0;
  ChunkStore cs;
  char dbpath[256];
  int rc;

  printf("=== Truncated WAL Rejected Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_truncated_wal_rejected");
  remove_db(dbpath);

  check("open_db_for_truncated_wal", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_truncated_wal", execsql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "INSERT INTO t VALUES(2,'b');"
    "SELECT dolt_commit('-A', '-m', 'second');")==SQLITE_OK);
  sqlite3_close(db);

  check("open_store_for_wal_tag_corruption", chunkStoreOpen(&cs, sqlite3_vfs_find(0), dbpath,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  check("have_wal_region", cs.nWalData > 0);
  if( cs.nWalData > 0 ){
    unsigned char badTag = 0xff;
    check("corrupt_first_wal_tag",
          sqlite3OsWrite(cs.pFile, &badTag, 1, cs.iWalOffset)==SQLITE_OK);
  }
  chunkStoreClose(&cs);

  rc = chunkStoreOpen(&cs, sqlite3_vfs_find(0), dbpath,
          SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB);
  check("chunk_store_open_rejects_corrupt_wal", rc==SQLITE_CORRUPT);

  remove_db(dbpath);
}

static void run_refresh_open_path_transactional(void){
  ChunkStore cs;
  char dbpath[256];
  FILE *f = 0;
  int changed = -1;
  int rc;

  printf("=== Refresh Open Path Transactional Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_refresh_open_path_transactional");
  remove_db(dbpath);

  check("open_empty_store_with_no_file",
        chunkStoreOpen(&cs, sqlite3_vfs_find(0), dbpath,
          SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  check("store_starts_without_file", cs.pFile==0);

  f = fopen(dbpath, "wb");
  check("create_corrupt_file", f!=0);
  if( f ){
    static const unsigned char badFile[] = { 'b', 'a', 'd' };
    check("write_corrupt_file",
          fwrite(badFile, 1, sizeof(badFile), f)==sizeof(badFile));
    fclose(f);
  }

  rc = chunkStoreRefreshIfChanged(&cs, &changed);
  check("refresh_open_path_returns_error", rc!=SQLITE_OK);
  check("refresh_open_path_does_not_mark_changed", changed==0);
  check("refresh_open_path_preserves_empty_refs", prollyHashIsEmpty(&cs.refsHash));
  check("refresh_open_path_preserves_branch_count", cs.nBranches==0);

  chunkStoreClose(&cs);
  remove_db(dbpath);
}

static void run_wal_offset_corruption_is_rejected(void){
  sqlite3 *db = 0;
  ChunkStore cs;
  char dbpath[256];
  int iWal = -1;
  u8 *pData = 0;
  int nData = 0;
  int rc;

  printf("=== WAL Offset Corruption Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_wal_offset_corruption");
  remove_db(dbpath);

  check("open_db_for_wal_offset", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_wal_offset", execsql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "INSERT INTO t VALUES(2,'b');"
    "SELECT dolt_commit('-A', '-m', 'second');")==SQLITE_OK);
  sqlite3_close(db);

  check("open_store_for_wal_offset", chunkStoreOpen(&cs, sqlite3_vfs_find(0), dbpath,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  {
    int i;
    for(i=0; i<cs.nIndex; i++){
      if( cs.aIndex[i].offset < 0 ){
        iWal = i;
        break;
      }
    }
  }
  check("have_wal_backed_index_entry", iWal >= 0);
  if( iWal >= 0 ){
    cs.aIndex[iWal].offset = -(cs.nWalData + cs.aIndex[iWal].size + 2);
    rc = chunkStoreGet(&cs, &cs.aIndex[iWal].hash, &pData, &nData);
    check("corrupt_wal_offset_returns_error", rc!=SQLITE_OK);
  }
  sqlite3_free(pData);
  chunkStoreClose(&cs);
  remove_db(dbpath);
}

static void run_integrity_check_walks_prolly_nodes(void){
  sqlite3 *db = 0;
  sqlite3 *db2 = 0;
  ChunkStore cs;
  char dbpath[256];
  ProllyHash catHash;
  struct TableEntry *aTables = 0;
  int nTables = 0;
  Pgno iNextTable = 0;
  struct TableEntry *pTable = 0;
  i64 dataOff = -1;

  printf("=== Integrity Check Walks Prolly Nodes Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_integrity_check_walks_nodes");
  remove_db(dbpath);

  check("open_db_for_integrity_walk", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_integrity_walk", execsql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "INSERT INTO t VALUES(2,'b');"
    "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);
  check("get_head_catalog_hash", doltliteGetHeadCatalogHash(db, &catHash)==SQLITE_OK);
  check("load_head_catalog", doltliteLoadCatalog(db, &catHash, &aTables, &nTables, &iNextTable)==SQLITE_OK);
  pTable = doltliteFindTableByName(aTables, nTables, "t");
  check("find_table_root_in_catalog", pTable!=0);
  sqlite3_close(db);

  check("open_store_for_integrity_walk", chunkStoreOpen(&cs, sqlite3_vfs_find(0), dbpath,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  if( pTable ){
    int i;
    for(i=0; i<cs.nIndex; i++){
      if( prollyHashCompare(&cs.aIndex[i].hash, &pTable->root)==0 ){
        if( cs.aIndex[i].offset < 0 ){
          dataOff = cs.iWalOffset + (-(cs.aIndex[i].offset + 1));
        }else{
          dataOff = cs.aIndex[i].offset + 4;
        }
        break;
      }
    }
  }
  check("find_root_chunk_offset", dataOff >= 0);
  if( dataOff >= 0 ){
    unsigned char badByte = 0;
    check("corrupt_root_chunk_magic",
          sqlite3OsWrite(cs.pFile, &badByte, 1, dataOff)==SQLITE_OK);
  }
  chunkStoreClose(&cs);

  check("reopen_db_for_integrity_walk", open_db(dbpath, &db2)==SQLITE_OK);
  check("integrity_check_surfaces_root_corruption",
        strcmp(exec1(db2, "PRAGMA integrity_check"), "ok")!=0);

  sqlite3_close(db2);
  remove_db(dbpath);
}

static void run_btree_commit_failure_transactional(void){
  sqlite3 *db = 0;
  char dbpath[256];
  int rc;
  ProllyHash headCatHash;
  ProllyHash dummyStaged;
  ProllyHash dummyMerge;
  ProllyHash dummyConflicts;
  ProllyHash stagedAfter;
  ProllyHash mergeAfter;
  ProllyHash conflictsAfter;
  u8 isMergingAfter = 0;

  printf("=== Btree Commit Failure Transaction Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_btree_commit_failure_transactional");
  remove_db(dbpath);
  gFailWriteOnce = 0;
  gFailSyncOnce = 0;
  gFailHits = 0;

  check("register_fail_vfs_for_btree_commit", registerFailVfs()==SQLITE_OK);
  check("open_fail_db_for_btree_commit", open_fail_db(dbpath, &db)==SQLITE_OK);
  check("setup_table", execsql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');")==SQLITE_OK);
  check("get_head_catalog_for_commit_failure",
        doltliteGetHeadCatalogHash(db, &headCatHash)==SQLITE_OK);
  doltliteSetSessionStaged(db, &headCatHash);
  doltliteClearSessionMergeState(db);
  check("begin_write_txn", execsql(db, "BEGIN; INSERT INTO t VALUES(2,'b');")==SQLITE_OK);
  memset(&dummyStaged, 0x71, sizeof(dummyStaged));
  memset(&dummyMerge, 0x72, sizeof(dummyMerge));
  memset(&dummyConflicts, 0x73, sizeof(dummyConflicts));
  doltliteSetSessionStaged(db, &dummyStaged);
  doltliteSetSessionMergeState(db, 1, &dummyMerge, &dummyConflicts);

  gFailWriteOnce = 1;
  rc = execsql_silent(db, "COMMIT;");
  check("commit_failure_injected", gFailHits>0);
  check("commit_returns_error", rc!=SQLITE_OK);
  check("autocommit_restored_after_failed_commit", sqlite3_get_autocommit(db)==1);
  check("failed_commit_rolled_back_visible_state",
        strcmp(exec1(db, "SELECT count(*) FROM t"), "1")==0);
  doltliteGetSessionStaged(db, &stagedAfter);
  doltliteGetSessionMergeState(db, &isMergingAfter, &mergeAfter, &conflictsAfter);
  check("failed_commit_restores_session_staged",
        memcmp(&stagedAfter, &headCatHash, sizeof(headCatHash))==0);
  check("failed_commit_clears_merge_flag", isMergingAfter==0);
  check("failed_commit_restores_merge_commit",
        memcmp(&mergeAfter, &(ProllyHash){{0}}, sizeof(ProllyHash))==0);
  check("failed_commit_restores_conflicts_catalog",
        memcmp(&conflictsAfter, &(ProllyHash){{0}}, sizeof(ProllyHash))==0);

  sqlite3_close(db);
  check("reopen_after_failed_commit", open_db(dbpath, &db)==SQLITE_OK);
  check("failed_commit_did_not_persist_row",
        strcmp(exec1(db, "SELECT count(*) FROM t"), "1")==0);
  sqlite3_close(db);
  remove_db(dbpath);
}

static void run_savepoint_restores_session_metadata(void){
  sqlite3 *db = 0;
  char dbpath[256];
  ProllyHash baseStaged;
  ProllyHash dummyStaged;
  ProllyHash dummyMerge;
  ProllyHash dummyConflicts;
  ProllyHash stagedAfter;
  ProllyHash mergeAfter;
  ProllyHash conflictsAfter;
  u8 isMergingAfter = 0;

  printf("=== Savepoint Restores Session Metadata Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_savepoint_restores_session_metadata");
  remove_db(dbpath);

  check("open_db_for_savepoint_metadata", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_savepoint_metadata", execsql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);

  check("get_head_catalog_for_savepoint_metadata",
        doltliteGetHeadCatalogHash(db, &baseStaged)==SQLITE_OK);
  doltliteSetSessionStaged(db, &baseStaged);
  doltliteClearSessionMergeState(db);

  check("begin_immediate_for_savepoint_metadata",
        execsql(db, "BEGIN IMMEDIATE;")==SQLITE_OK);
  check("create_savepoint_for_metadata",
        execsql(db, "SAVEPOINT s1;")==SQLITE_OK);

  memset(&dummyStaged, 0x41, sizeof(dummyStaged));
  memset(&dummyMerge, 0x42, sizeof(dummyMerge));
  memset(&dummyConflicts, 0x43, sizeof(dummyConflicts));
  doltliteSetSessionStaged(db, &dummyStaged);
  doltliteSetSessionMergeState(db, 1, &dummyMerge, &dummyConflicts);
  doltliteGetSessionStaged(db, &stagedAfter);
  check("savepoint_metadata_mutation_applied",
        memcmp(&stagedAfter, &dummyStaged, sizeof(dummyStaged))==0);

  check("rollback_to_savepoint_metadata",
        execsql(db, "ROLLBACK TO s1;")==SQLITE_OK);
  doltliteGetSessionStaged(db, &stagedAfter);
  doltliteGetSessionMergeState(db, &isMergingAfter, &mergeAfter, &conflictsAfter);
  check("rollback_to_savepoint_restores_staged",
        memcmp(&stagedAfter, &baseStaged, sizeof(baseStaged))==0);
  check("rollback_to_savepoint_clears_merge_flag", isMergingAfter==0);
  check("rollback_to_savepoint_clears_merge_commit",
        memcmp(&mergeAfter, &(ProllyHash){{0}}, sizeof(ProllyHash))==0);
  check("rollback_to_savepoint_clears_conflicts_catalog",
        memcmp(&conflictsAfter, &(ProllyHash){{0}}, sizeof(ProllyHash))==0);

  check("release_savepoint_metadata", execsql(db, "RELEASE s1;")==SQLITE_OK);
  check("rollback_outer_txn_metadata", execsql(db, "ROLLBACK;")==SQLITE_OK);
  sqlite3_close(db);
  remove_db(dbpath);
}

static void run_hard_reset_failure_restores_memory_state(void){
  sqlite3 *db = 0;
  char dbpath[256];
  ProllyHash headCatHash;
  int rc;

  printf("=== Hard Reset Failure Restores Memory State Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_hard_reset_failure_restores_memory_state");
  remove_db(dbpath);
  gFailWriteOnce = 0;
  gFailSyncOnce = 0;
  gFailHits = 0;

  check("register_fail_vfs_for_hard_reset", registerFailVfs()==SQLITE_OK);
  check("open_fail_db_for_hard_reset", open_fail_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_hard_reset_failure", execsql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "INSERT INTO t VALUES(2,'b');")==SQLITE_OK);
  check("working_row_visible_before_hard_reset",
        strcmp(exec1(db, "SELECT count(*) FROM t"), "2")==0);
  check("get_head_catalog_for_hard_reset",
        doltliteGetHeadCatalogHash(db, &headCatHash)==SQLITE_OK);

  gFailHits = 0;
  gFailWriteOnce = 1;
  rc = doltliteHardReset(db, &headCatHash);
  check("hard_reset_failure_injected", gFailHits>0);
  check("hard_reset_returns_error_on_commit_failure", rc!=SQLITE_OK);
  check("failed_hard_reset_preserves_memory_state",
        strcmp(exec1(db, "SELECT count(*) FROM t"), "2")==0);

  sqlite3_close(db);
  check("reopen_after_failed_hard_reset", open_db(dbpath, &db)==SQLITE_OK);
  check("failed_hard_reset_preserves_durable_state",
        strcmp(exec1(db, "SELECT count(*) FROM t"), "2")==0);
  sqlite3_close(db);
  remove_db(dbpath);
}

static void run_mutmap_empty_reverse_iter(void){
  ProllyMutMap mm;
  ProllyMutMapIter it;

  printf("=== MutMap Empty Reverse Iterator Test ===\n\n");
  check("mutmap_init_for_reverse_iter", prollyMutMapInit(&mm, 1)==SQLITE_OK);
  prollyMutMapIterLast(&it, &mm);
  check("empty_reverse_iter_is_invalid", !prollyMutMapIterValid(&it));
  prollyMutMapFree(&mm);
}

static void run_prolly_blob_cursor_seek_across_internal_boundary(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyCursor cur;
  ProllyNodeBuilder b;
  ProllyHash leftHash, rightHash, rootHash;
  u8 *pNode = 0;
  int nNode = 0;
  const u8 *pKey = 0;
  int nKey = 0;
  int rc;
  int res = 99;

  static const u8 v1[] = { '1' };
  static const u8 v2[] = { '2' };
  static const u8 kA[] = { 'a' };
  static const u8 kM[] = { 'm' };
  static const u8 kT[] = { 't' };
  static const u8 kZ[] = { 'z' };

  printf("=== Prolly Blob Cursor Internal Boundary Test ===\n\n");

  check("open_memory_store_for_blob_cursor",
        chunkStoreOpen(&cs, sqlite3_vfs_find(0), ":memory:", 0)==SQLITE_OK);
  check("init_cache_for_blob_cursor", prollyCacheInit(&cache, 8)==SQLITE_OK);

  prollyNodeBuilderInit(&b, 0, PROLLY_NODE_BLOBKEY);
  check("build_left_leaf_a",
        prollyNodeBuilderAdd(&b, kA, sizeof(kA), v1, sizeof(v1))==SQLITE_OK);
  check("build_left_leaf_m",
        prollyNodeBuilderAdd(&b, kM, sizeof(kM), v1, sizeof(v1))==SQLITE_OK);
  check("finish_left_leaf", prollyNodeBuilderFinish(&b, &pNode, &nNode)==SQLITE_OK);
  check("store_left_leaf", chunkStorePut(&cs, pNode, nNode, &leftHash)==SQLITE_OK);
  sqlite3_free(pNode);
  pNode = 0;
  prollyNodeBuilderFree(&b);

  prollyNodeBuilderInit(&b, 0, PROLLY_NODE_BLOBKEY);
  check("build_right_leaf_t",
        prollyNodeBuilderAdd(&b, kT, sizeof(kT), v2, sizeof(v2))==SQLITE_OK);
  check("build_right_leaf_z",
        prollyNodeBuilderAdd(&b, kZ, sizeof(kZ), v2, sizeof(v2))==SQLITE_OK);
  check("finish_right_leaf", prollyNodeBuilderFinish(&b, &pNode, &nNode)==SQLITE_OK);
  check("store_right_leaf", chunkStorePut(&cs, pNode, nNode, &rightHash)==SQLITE_OK);
  sqlite3_free(pNode);
  pNode = 0;
  prollyNodeBuilderFree(&b);

  prollyNodeBuilderInit(&b, 1, PROLLY_NODE_BLOBKEY);
  check("build_root_left_sep",
        prollyNodeBuilderAdd(&b, kM, sizeof(kM),
                             leftHash.data, PROLLY_HASH_SIZE)==SQLITE_OK);
  check("build_root_right_sep",
        prollyNodeBuilderAdd(&b, kZ, sizeof(kZ),
                             rightHash.data, PROLLY_HASH_SIZE)==SQLITE_OK);
  check("finish_root", prollyNodeBuilderFinish(&b, &pNode, &nNode)==SQLITE_OK);
  check("store_root", chunkStorePut(&cs, pNode, nNode, &rootHash)==SQLITE_OK);
  sqlite3_free(pNode);
  prollyNodeBuilderFree(&b);

  prollyCursorInit(&cur, &cs, &cache, &rootHash, PROLLY_NODE_BLOBKEY);
  rc = prollyCursorSeekBlob(&cur, kT, sizeof(kT), &res);
  check("seek_blob_key_across_internal_boundary", rc==SQLITE_OK);
  check("seek_blob_key_finds_exact_match", res==0);
  check("blob_cursor_valid_after_seek", prollyCursorIsValid(&cur));
  if( prollyCursorIsValid(&cur) ){
    prollyCursorKey(&cur, &pKey, &nKey);
    check("blob_cursor_lands_on_right_child_key",
          nKey==(int)sizeof(kT) && memcmp(pKey, kT, sizeof(kT))==0);
  }

  prollyCursorClose(&cur);
  prollyCacheFree(&cache);
  chunkStoreClose(&cs);
}

static void run_prolly_blob_cursor_seek_past_max(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyCursor cur;
  ProllyNodeBuilder b;
  ProllyHash leftHash, rightHash, rootHash;
  u8 *pNode = 0;
  int nNode = 0;
  const u8 *pKey = 0;
  int nKey = 0;
  int rc;
  int res = 99;

  static const u8 v1[] = { '1' };
  static const u8 v2[] = { '2' };
  static const u8 kA[] = { 'a' };
  static const u8 kM[] = { 'm' };
  static const u8 kT[] = { 't' };
  static const u8 kZ[] = { 'z' };
  static const u8 kZZ[] = { 'z', 'z' };

  printf("=== Prolly Blob Cursor Seek Past Max Test ===\n\n");

  check("open_memory_store_for_blob_cursor_past_max",
        chunkStoreOpen(&cs, sqlite3_vfs_find(0), ":memory:", 0)==SQLITE_OK);
  check("init_cache_for_blob_cursor_past_max", prollyCacheInit(&cache, 8)==SQLITE_OK);

  prollyNodeBuilderInit(&b, 0, PROLLY_NODE_BLOBKEY);
  check("build_left_leaf_a_past_max",
        prollyNodeBuilderAdd(&b, kA, sizeof(kA), v1, sizeof(v1))==SQLITE_OK);
  check("build_left_leaf_m_past_max",
        prollyNodeBuilderAdd(&b, kM, sizeof(kM), v1, sizeof(v1))==SQLITE_OK);
  check("finish_left_leaf_past_max", prollyNodeBuilderFinish(&b, &pNode, &nNode)==SQLITE_OK);
  check("store_left_leaf_past_max", chunkStorePut(&cs, pNode, nNode, &leftHash)==SQLITE_OK);
  sqlite3_free(pNode);
  pNode = 0;
  prollyNodeBuilderFree(&b);

  prollyNodeBuilderInit(&b, 0, PROLLY_NODE_BLOBKEY);
  check("build_right_leaf_t_past_max",
        prollyNodeBuilderAdd(&b, kT, sizeof(kT), v2, sizeof(v2))==SQLITE_OK);
  check("build_right_leaf_z_past_max",
        prollyNodeBuilderAdd(&b, kZ, sizeof(kZ), v2, sizeof(v2))==SQLITE_OK);
  check("finish_right_leaf_past_max", prollyNodeBuilderFinish(&b, &pNode, &nNode)==SQLITE_OK);
  check("store_right_leaf_past_max", chunkStorePut(&cs, pNode, nNode, &rightHash)==SQLITE_OK);
  sqlite3_free(pNode);
  pNode = 0;
  prollyNodeBuilderFree(&b);

  prollyNodeBuilderInit(&b, 1, PROLLY_NODE_BLOBKEY);
  check("build_root_left_sep_past_max",
        prollyNodeBuilderAdd(&b, kM, sizeof(kM),
                             leftHash.data, PROLLY_HASH_SIZE)==SQLITE_OK);
  check("build_root_right_sep_past_max",
        prollyNodeBuilderAdd(&b, kZ, sizeof(kZ),
                             rightHash.data, PROLLY_HASH_SIZE)==SQLITE_OK);
  check("finish_root_past_max", prollyNodeBuilderFinish(&b, &pNode, &nNode)==SQLITE_OK);
  check("store_root_past_max", chunkStorePut(&cs, pNode, nNode, &rootHash)==SQLITE_OK);
  sqlite3_free(pNode);
  prollyNodeBuilderFree(&b);

  prollyCursorInit(&cur, &cs, &cache, &rootHash, PROLLY_NODE_BLOBKEY);
  rc = prollyCursorSeekBlob(&cur, kZZ, sizeof(kZZ), &res);
  check("seek_blob_key_past_max", rc==SQLITE_OK);
  check("seek_blob_key_past_max_result", res==-1);
  check("blob_cursor_valid_after_past_max_seek", prollyCursorIsValid(&cur));
  if( prollyCursorIsValid(&cur) ){
    prollyCursorKey(&cur, &pKey, &nKey);
    check("blob_cursor_lands_on_max_key_after_past_max_seek",
          nKey==(int)sizeof(kZ) && memcmp(pKey, kZ, sizeof(kZ))==0);
  }

  prollyCursorClose(&cur);
  prollyCacheFree(&cache);
  chunkStoreClose(&cs);
}

static void run_prolly_cursor_empty_leaf_root(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyCursor cur;
  ProllyHash rootHash;
  int rc;
  int res = 99;
  static const u8 emptyLeafRoot[] = {
    'D', 'O', 'N', 'P', 0, 0, 0, PROLLY_NODE_BLOBKEY
  };

  printf("=== Prolly Cursor Empty Leaf Root Test ===\n\n");

  check("open_memory_store_for_empty_leaf_root",
        chunkStoreOpen(&cs, sqlite3_vfs_find(0), ":memory:", 0)==SQLITE_OK);
  check("init_cache_for_empty_leaf_root", prollyCacheInit(&cache, 4)==SQLITE_OK);
  check("store_empty_leaf_root",
        chunkStorePut(&cs, emptyLeafRoot, (int)sizeof(emptyLeafRoot), &rootHash)==SQLITE_OK);

  prollyCursorInit(&cur, &cs, &cache, &rootHash, PROLLY_NODE_BLOBKEY);
  rc = prollyCursorFirst(&cur, &res);
  check("cursor_first_empty_leaf_root_ok", rc==SQLITE_OK);
  check("cursor_first_empty_leaf_root_eof", res==1);
  check("cursor_first_empty_leaf_root_not_valid", !prollyCursorIsValid(&cur));
  prollyCursorClose(&cur);

  prollyCursorInit(&cur, &cs, &cache, &rootHash, PROLLY_NODE_BLOBKEY);
  rc = prollyCursorLast(&cur, &res);
  check("cursor_last_empty_leaf_root_ok", rc==SQLITE_OK);
  check("cursor_last_empty_leaf_root_eof", res==1);
  check("cursor_last_empty_leaf_root_not_valid", !prollyCursorIsValid(&cur));
  prollyCursorClose(&cur);

  prollyCacheFree(&cache);
  chunkStoreClose(&cs);
}

static void run_prolly_cursor_surfaces_corrupt_node(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyCursor cur;
  ProllyHash rootHash;
  int rc;
  int res = 99;
  static const u8 badNode[] = { 'b', 'a', 'd', '!' };

  printf("=== Prolly Cursor Corrupt Node Test ===\n\n");

  check("open_memory_store_for_corrupt_cursor",
        chunkStoreOpen(&cs, sqlite3_vfs_find(0), ":memory:", 0)==SQLITE_OK);
  check("init_cache_for_corrupt_cursor", prollyCacheInit(&cache, 4)==SQLITE_OK);
  check("store_bad_root_node",
        chunkStorePut(&cs, badNode, (int)sizeof(badNode), &rootHash)==SQLITE_OK);

  prollyCursorInit(&cur, &cs, &cache, &rootHash, PROLLY_NODE_BLOBKEY);
  rc = prollyCursorFirst(&cur, &res);
  check("cursor_first_reports_corrupt_node", rc==SQLITE_CORRUPT);

  prollyCursorClose(&cur);
  prollyCacheFree(&cache);
  chunkStoreClose(&cs);
}

static void run_prolly_diff_iter_copies_blob_keys(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyDiffIter iter;
  ProllyDiffChange *pCh = 0;
  ProllyNodeBuilder b;
  ProllyHash oldLeftHash, oldRightHash, oldRootHash, newRootHash;
  u8 *pNode = 0;
  int nNode = 0;
  int rc;
  int i;
  int keyBackedByCache = 0;
  static const u8 oldKey[] = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
  static const u8 newKey[] = "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz";
  static const u8 v1[] = { '1' };
  static const u8 v2[] = { '2' };

  printf("=== Prolly Diff Iterator Copies Blob Keys Test ===\n\n");

  check("open_memory_store_for_diff_iter_key_copy",
        chunkStoreOpen(&cs, sqlite3_vfs_find(0), ":memory:", 0)==SQLITE_OK);
  check("init_cache_for_diff_iter_key_copy", prollyCacheInit(&cache, 8)==SQLITE_OK);

  prollyNodeBuilderInit(&b, 0, PROLLY_NODE_BLOBKEY);
  check("build_old_left_leaf_for_diff_iter_key_copy",
        prollyNodeBuilderAdd(&b, oldKey, sizeof(oldKey), v1, sizeof(v1))==SQLITE_OK);
  check("finish_old_left_leaf_for_diff_iter_key_copy",
        prollyNodeBuilderFinish(&b, &pNode, &nNode)==SQLITE_OK);
  check("store_old_left_leaf_for_diff_iter_key_copy",
        chunkStorePut(&cs, pNode, nNode, &oldLeftHash)==SQLITE_OK);
  sqlite3_free(pNode);
  pNode = 0;
  prollyNodeBuilderFree(&b);

  prollyNodeBuilderInit(&b, 0, PROLLY_NODE_BLOBKEY);
  check("build_old_right_leaf_for_diff_iter_key_copy",
        prollyNodeBuilderAdd(&b, newKey, sizeof(newKey), v2, sizeof(v2))==SQLITE_OK);
  check("finish_old_right_leaf_for_diff_iter_key_copy",
        prollyNodeBuilderFinish(&b, &pNode, &nNode)==SQLITE_OK);
  check("store_old_right_leaf_for_diff_iter_key_copy",
        chunkStorePut(&cs, pNode, nNode, &oldRightHash)==SQLITE_OK);
  sqlite3_free(pNode);
  pNode = 0;
  prollyNodeBuilderFree(&b);

  prollyNodeBuilderInit(&b, 1, PROLLY_NODE_BLOBKEY);
  check("build_old_root_left_sep_for_diff_iter_key_copy",
        prollyNodeBuilderAdd(&b, oldKey, sizeof(oldKey),
                             oldLeftHash.data, PROLLY_HASH_SIZE)==SQLITE_OK);
  check("build_old_root_right_sep_for_diff_iter_key_copy",
        prollyNodeBuilderAdd(&b, newKey, sizeof(newKey),
                             oldRightHash.data, PROLLY_HASH_SIZE)==SQLITE_OK);
  check("finish_old_root_for_diff_iter_key_copy",
        prollyNodeBuilderFinish(&b, &pNode, &nNode)==SQLITE_OK);
  check("store_old_root_for_diff_iter_key_copy",
        chunkStorePut(&cs, pNode, nNode, &oldRootHash)==SQLITE_OK);
  sqlite3_free(pNode);
  pNode = 0;
  prollyNodeBuilderFree(&b);

  prollyNodeBuilderInit(&b, 0, PROLLY_NODE_BLOBKEY);
  check("build_new_root_for_diff_iter_key_copy",
        prollyNodeBuilderAdd(&b, newKey, sizeof(newKey), v2, sizeof(v2))==SQLITE_OK);
  check("finish_new_root_for_diff_iter_key_copy",
        prollyNodeBuilderFinish(&b, &pNode, &nNode)==SQLITE_OK);
  check("store_new_root_for_diff_iter_key_copy",
        chunkStorePut(&cs, pNode, nNode, &newRootHash)==SQLITE_OK);
  sqlite3_free(pNode);
  pNode = 0;
  prollyNodeBuilderFree(&b);

  rc = prollyDiffIterOpen(&iter, &cs, &cache, &oldRootHash, &newRootHash,
                          PROLLY_NODE_BLOBKEY);
  check("open_diff_iter_key_copy", rc==SQLITE_OK);
  if( rc==SQLITE_OK ){
    rc = prollyDiffIterStep(&iter, &pCh);
    check("step_diff_iter_key_copy", rc==SQLITE_ROW);
    if( rc==SQLITE_ROW ){
      check("diff_iter_key_copy_delete_type", pCh->type==PROLLY_DIFF_DELETE);
      check("diff_iter_key_copy_matches_expected_before_purge",
            pCh->nKey==(int)sizeof(oldKey) &&
            memcmp(pCh->pKey, oldKey, sizeof(oldKey))==0);
      for(i=0; i<cache.nBucket; i++){
        ProllyCacheEntry *pEntry = cache.aBucket[i];
        while( pEntry ){
          if( pCh->pKey >= pEntry->pData &&
              pCh->pKey < pEntry->pData + pEntry->nData ){
            keyBackedByCache = 1;
          }
          pEntry = pEntry->pHashNext;
        }
      }
      check("diff_iter_key_copy_not_backed_by_cache_node", !keyBackedByCache);
    }
    prollyDiffIterClose(&iter);
  }

  prollyCacheFree(&cache);
  chunkStoreClose(&cs);
}

static void run_prolly_diff_leaf_surfaces_record_corruption(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyNodeBuilder b;
  ProllyHash oldRootHash, newRootHash;
  u8 *pNode = 0;
  int nNode = 0;
  int rc;
  DiffCountCtx ctx;
  static const u8 badRecord[] = { 0x05, 0x01 };
  static const u8 key[] = { 'a' };
  static const u8 key2[] = { 'b' };
  static const u8 goodRecord[] = { 0x02, 0x09 };

  printf("=== Prolly Diff Leaf Corruption Test ===\n\n");

  memset(&ctx, 0, sizeof(ctx));
  check("open_memory_store_for_diff_leaf_corruption",
        chunkStoreOpen(&cs, sqlite3_vfs_find(0), ":memory:", 0)==SQLITE_OK);
  check("init_cache_for_diff_leaf_corruption", prollyCacheInit(&cache, 4)==SQLITE_OK);

  prollyNodeBuilderInit(&b, 0, PROLLY_NODE_BLOBKEY);
  check("build_old_leaf_bad_record",
        prollyNodeBuilderAdd(&b, key, sizeof(key),
                             badRecord, sizeof(badRecord))==SQLITE_OK);
  check("finish_old_leaf_bad_record",
        prollyNodeBuilderFinish(&b, &pNode, &nNode)==SQLITE_OK);
  check("store_old_leaf_bad_record",
        chunkStorePut(&cs, pNode, nNode, &oldRootHash)==SQLITE_OK);
  sqlite3_free(pNode);
  pNode = 0;
  prollyNodeBuilderFree(&b);

  prollyNodeBuilderInit(&b, 0, PROLLY_NODE_BLOBKEY);
  check("build_new_leaf_bad_record",
        prollyNodeBuilderAdd(&b, key, sizeof(key),
                             badRecord, sizeof(badRecord))==SQLITE_OK);
  check("build_new_leaf_extra_row",
        prollyNodeBuilderAdd(&b, key2, sizeof(key2),
                             goodRecord, sizeof(goodRecord))==SQLITE_OK);
  check("finish_new_leaf_bad_record",
        prollyNodeBuilderFinish(&b, &pNode, &nNode)==SQLITE_OK);
  check("store_new_leaf_bad_record",
        chunkStorePut(&cs, pNode, nNode, &newRootHash)==SQLITE_OK);
  sqlite3_free(pNode);
  pNode = 0;
  prollyNodeBuilderFree(&b);

  rc = prollyDiff(&cs, &cache, &oldRootHash, &newRootHash,
                  PROLLY_NODE_BLOBKEY, count_diff_change, &ctx);
  check("diff_leaf_bad_record_returns_corrupt", rc==SQLITE_CORRUPT);
  check("diff_leaf_bad_record_emits_no_changes", ctx.nChange==0);

  prollyCacheFree(&cache);
  chunkStoreClose(&cs);
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
  { "chunk_walk_corruption", "Chunk Walk Corruption Test", run_chunk_walk_corruption },
  { "ancestor_missing_start", "Ancestor Missing Start Test", run_ancestor_missing_start },
  { "pull_persist_failure", "Pull Persist Failure Test", run_pull_persist_failure },
  { "clone_persist_failure", "Clone Persist Failure Test", run_clone_persist_failure },
  { "resolve_ref_non_commit", "Resolve Ref Non-Commit Test", run_resolve_ref_non_commit },
  { "commit_parent_limit", "Commit Parent Limit Test", run_commit_parent_limit },
  { "merge_persist_failure", "Merge Persist Failure Test", run_merge_persist_failure },
  { "cherry_pick_stale_branch", "Cherry-pick Stale Branch Test", run_cherry_pick_stale_branch },
  { "branches_metadata_corruption", "Branches Metadata Corruption Test", run_branches_metadata_corruption },
  { "gc_rewrite_failure", "GC Rewrite Failure Test", run_gc_rewrite_failure },
  { "record_decode_corruption", "Record Decode Corruption Test", run_record_decode_corruption },
  { "reload_refs_transactional", "Reload Refs Transactional Test", run_reload_refs_transactional },
  { "refresh_refs_corruption_preserves_state", "Refresh Corrupt Refs State Preservation Test", run_refresh_refs_corruption_preserves_state },
  { "prolly_node_corruption", "Prolly Node Corruption Test", run_prolly_node_corruption },
  { "truncated_wal_rejected", "Truncated WAL Rejected Test", run_truncated_wal_is_rejected },
  { "refresh_open_path_transactional", "Refresh Open Path Transactional Test", run_refresh_open_path_transactional },
  { "wal_offset_corruption", "WAL Offset Corruption Test", run_wal_offset_corruption_is_rejected },
  { "integrity_check_walks_nodes", "Integrity Check Walks Prolly Nodes Test", run_integrity_check_walks_prolly_nodes },
  { "memory_chunk_lookup_corruption", "Memory Chunk Lookup Corruption Test", run_memory_chunk_lookup_corruption },
  { "prolly_diff_record_corruption", "Prolly Diff Record Corruption Test", run_prolly_diff_record_corruption },
  { "integrity_check_repo_state", "Integrity Check Repository State Test", run_integrity_check_repo_state },
  { "integrity_check_session_merge_state", "Integrity Check Session Merge State Test", run_integrity_check_session_merge_state },
  { "prepared_stmt_reuse_after_commit", "Prepared Statement Reuse After Commit Test", run_prepared_stmt_reuse_after_commit },
  { "prepared_stmt_reuse_after_schema_checkout", "Prepared Statement Reuse After Schema Checkout Test", run_prepared_stmt_reuse_after_schema_checkout },
  { "working_set_refreshes_staged_across_connections", "Working Set Refreshes Staged Across Connections Test", run_working_set_refreshes_staged_across_connections },
  { "reopen_preserves_staged_working_set", "Reopen Preserves Staged Working Set Test", run_reopen_preserves_staged_working_set },
  { "begin_write_refreshes_working_set_metadata", "Begin Write Refreshes Working Set Metadata Test", run_begin_write_refreshes_working_set_metadata },
  { "begin_write_from_stale_read_snapshot", "Begin Write From Stale Read Snapshot Test", run_begin_write_from_stale_read_snapshot },
  { "open_rejects_corrupt_working_set", "Open Rejects Corrupt Working Set Test", run_open_rejects_corrupt_working_set },
  { "btree_commit_failure_transactional", "Btree Commit Failure Transaction Test", run_btree_commit_failure_transactional },
  { "savepoint_restores_session_metadata", "Savepoint Restores Session Metadata Test", run_savepoint_restores_session_metadata },
  { "hard_reset_failure_restores_memory_state", "Hard Reset Failure Restores Memory State Test", run_hard_reset_failure_restores_memory_state },
  { "mutmap_empty_reverse_iter", "MutMap Empty Reverse Iterator Test", run_mutmap_empty_reverse_iter },
  { "prolly_blob_cursor_boundary", "Prolly Blob Cursor Internal Boundary Test", run_prolly_blob_cursor_seek_across_internal_boundary },
  { "prolly_blob_cursor_seek_past_max", "Prolly Blob Cursor Seek Past Max Test", run_prolly_blob_cursor_seek_past_max },
  { "prolly_cursor_empty_leaf_root", "Prolly Cursor Empty Leaf Root Test", run_prolly_cursor_empty_leaf_root },
  { "prolly_cursor_corrupt_node", "Prolly Cursor Corrupt Node Test", run_prolly_cursor_surfaces_corrupt_node },
  { "prolly_diff_iter_copies_blob_keys", "Prolly Diff Iterator Blob Key Copy Test", run_prolly_diff_iter_copies_blob_keys },
  { "prolly_diff_leaf_record_corruption", "Prolly Diff Leaf Corruption Test", run_prolly_diff_leaf_surfaces_record_corruption }
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
