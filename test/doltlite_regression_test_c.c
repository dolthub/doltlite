#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/stat.h>
#include "sqlite3.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "chunk_refs.h"
#include "doltlite_commit.h"
#include "doltlite_chunk_walk.h"
#include "doltlite_ancestor.h"
#include "doltlite_internal.h"
#include "doltlite_record.h"
#include "prolly_cache.h"
#include "prolly_cursor.h"
#include "prolly_diff.h"
#include "prolly_chunker.h"
#include "prolly_mutate.h"
#include "prolly_node.h"
#include "prolly_mutmap.h"
#include "vdbeInt.h"
#include "sortkey.h"
#include "doltlite_remote.h"

typedef unsigned char u8;
typedef unsigned int Pgno;

extern int doltliteFlushAndSerializeCatalog(sqlite3 *db, u8 **ppOut, int *pnOut);
extern int doltliteRemoteSrvCommitPendingForTest(ChunkStore *pStore);
extern int doltliteRemoteSrvApplyRefsForTest(
  ChunkStore *pStore, const u8 *pBody, int nBody
);
extern ChunkStore *doltliteFsRemoteStoreForTest(DoltliteRemote *pRemote);
extern int doltliteRemoteSrvApplyScopedRefsForTest(
  ChunkStore *pStore, const char *zBranch, int bForce,
  const u8 *pBody, int nBody
);
extern int doltliteRemoteSrvResolveRootForTest(
  ChunkStore *pStore, ProllyHash *pRoot
);
extern int btreeReadWorkingCatalog(
  ChunkStore*, const char*, ProllyHash*, ProllyHash*
);

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

static const char *queryScalarText(sqlite3 *db, const char *sql){
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

static int execSql(sqlite3 *db, const char *sql){
  char *err = 0;
  int rc = sqlite3_exec(db, sql, 0, 0, &err);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "  SQL error: %s (rc=%d)\n  SQL: %s\n",
            err ? err : "?", rc, sql);
    sqlite3_free(err);
  }
  return rc;
}

static int execSqlSilent(sqlite3 *db, const char *sql){
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

static sqlite3_int64 file_size_or_negative(const char *path){
  struct stat st;
  if( stat(path, &st)!=0 ) return -1;
  return (sqlite3_int64)st.st_size;
}

static void make_dbpath(char *zBuf, size_t nBuf, const char *zBase);
static void removeDbFiles(const char *path);

static int backup_db(sqlite3 *src, sqlite3 *dest){
  sqlite3_backup *pBackup;
  int rc;
  int rcFinish;

  pBackup = sqlite3_backup_init(dest, "main", src, "main");
  if( pBackup==0 ) return sqlite3_errcode(dest);

  rc = sqlite3_backup_step(pBackup, 1);
  rcFinish = sqlite3_backup_finish(pBackup);
  if( rc==SQLITE_DONE ) rc = rcFinish;
  return rc;
}

static int custom_collation_cmp(
  void *pCtx,
  int nA,
  const void *pA,
  int nB,
  const void *pB
){
  int n;
  int cmp;
  (void)pCtx;
  n = nA < nB ? nA : nB;
  cmp = n>0 ? memcmp(pA, pB, (size_t)n) : 0;
  if( cmp==0 ){
    if( nA<nB ) cmp = -1;
    if( nA>nB ) cmp = 1;
  }
  return cmp<0 ? 1 : cmp>0 ? -1 : 0;
}

static void custom_collation_destroy(void *pCtx){
  int *pnDestroy = (int*)pCtx;
  (*pnDestroy)++;
}

static void run_custom_collation_unindexed(void){
  char dbpath[256];
  sqlite3 *db = 0;
  int rc;
  int nDestroy = 0;

  make_dbpath(dbpath, sizeof(dbpath), "test_custom_collation_unindexed");
  removeDbFiles(dbpath);
  check("create_collation_open", open_db(dbpath, &db)==SQLITE_OK);
  if( db==0 ) return;

  rc = sqlite3_create_collation(
      db, "reverse", SQLITE_UTF8, 0, custom_collation_cmp);
  check("create_collation_registered", rc==SQLITE_OK);
  check("custom_collation_column", execSql(db,
      "CREATE TABLE custom_values(v TEXT COLLATE reverse);"
      "INSERT INTO custom_values VALUES('alpha'),('gamma'),('beta');"
      )==SQLITE_OK);
  check("custom_collation_order_by",
      strcmp(queryScalarText(db,
        "SELECT group_concat(v, ',') FROM ("
        "SELECT v FROM custom_values ORDER BY v)"),
        "gamma,beta,alpha")==0);
  check("custom_collation_comparison",
      strcmp(queryScalarText(db,
        "SELECT 'alpha' < 'beta' COLLATE reverse"), "0")==0);

  rc = execSqlSilent(db, "CREATE INDEX custom_values_v ON custom_values(v)");
  check("custom_collation_index_rejected", rc==SQLITE_ERROR);
  check("custom_collation_index_errmsg",
      strstr(sqlite3_errmsg(db),
        "does not support indexes with custom collation 'reverse'")!=0);
  check("custom_collation_index_not_created",
      strcmp(queryScalarText(db,
        "SELECT count(*) FROM sqlite_master "
        "WHERE type='index' AND name='custom_values_v'"), "0")==0);
  check("custom_collation_builtin_override", execSql(db,
      "CREATE INDEX custom_values_binary "
      "ON custom_values(v COLLATE BINARY)")==SQLITE_OK);
  check("custom_collation_unique_rejected",
      execSqlSilent(db,
        "CREATE TABLE custom_unique(v TEXT COLLATE reverse UNIQUE)")
        ==SQLITE_ERROR);
  check("custom_collation_primary_key_rejected",
      execSqlSilent(db,
        "CREATE TABLE custom_pk(v TEXT PRIMARY KEY COLLATE reverse)")
        ==SQLITE_ERROR);
  rc = sqlite3_create_collation(
      db, "BINARY", SQLITE_UTF8, 0, custom_collation_cmp);
  check("custom_collation_builtin_replacement_rejected", rc==SQLITE_ERROR);
  check("custom_collation_builtin_replacement_errmsg",
      strstr(sqlite3_errmsg(db),
        "cannot replace collation 'BINARY' used by index "
        "'custom_values_binary'")!=0);

  rc = sqlite3_create_collation_v2(
      db, "reverse_v2", SQLITE_UTF8, &nDestroy,
      custom_collation_cmp, custom_collation_destroy);
  check("create_collation_v2_registered", rc==SQLITE_OK);
  check("create_collation_v2_works",
      strcmp(queryScalarText(db,
        "SELECT 'alpha' < 'beta' COLLATE reverse_v2"), "0")==0);

#ifndef SQLITE_OMIT_UTF16
  {
    const unsigned short zName16[] = {
      'r', 'e', 'v', 'e', 'r', 's', 'e', '1', '6', 0
    };
    rc = sqlite3_create_collation16(
        db, zName16, SQLITE_UTF8, 0, custom_collation_cmp);
    check("create_collation16_registered", rc==SQLITE_OK);
    check("create_collation16_works",
        strcmp(queryScalarText(db,
          "SELECT 'alpha' < 'beta' COLLATE reverse16"), "0")==0);
  }
#endif

  check("builtin_collation_still_works",
        strcmp(queryScalarText(db, "SELECT 'a'='A' COLLATE NOCASE"), "1")==0);
  check("custom_collation_commit",
      strlen(queryScalarText(db,
        "SELECT dolt_commit('-A', '-m', 'custom collation')"))==40);
  sqlite3_close(db);
  check("create_collation_v2_destroy_called", nDestroy==1);

  db = 0;
  check("custom_collation_reopen", open_db(dbpath, &db)==SQLITE_OK);
  if( db ){
    check("custom_collation_requires_reregistration",
        strcmp(queryScalarText(db,
          "SELECT v FROM custom_values ORDER BY v LIMIT 1"),
          "ERROR: no such collation sequence: reverse")==0);
    check("custom_collation_reregister",
        sqlite3_create_collation(
          db, "reverse", SQLITE_UTF8, 0, custom_collation_cmp)==SQLITE_OK);
    check("custom_collation_after_reregister",
        strcmp(queryScalarText(db,
          "SELECT v FROM custom_values ORDER BY v LIMIT 1"), "gamma")==0);
    sqlite3_close(db);
  }
  removeDbFiles(dbpath);
}

typedef struct SerializeMutexProbe SerializeMutexProbe;
struct SerializeMutexProbe {
  sqlite3 *db;
  int rc;
};

static void *trySerializeConnectionMutex(void *pArg){
  SerializeMutexProbe *p = (SerializeMutexProbe*)pArg;
  sqlite3_mutex *pMutex = sqlite3_db_mutex(p->db);
  p->rc = sqlite3_mutex_try(pMutex);
  if( p->rc==SQLITE_OK ) sqlite3_mutex_leave(pMutex);
  return 0;
}

static void run_serialize_deserialize(void){
  char path[256];
  char auxPath[256];
  char imagePath[256];
  sqlite3 *db = 0;
  sqlite3 *dest = 0;
  sqlite3 *readOnly = 0;
  sqlite3 *fixed = 0;
  sqlite3 *busy = 0;
  sqlite3 *malformed = 0;
  sqlite3 *named = 0;
  sqlite3 *emptySrc = 0;
  sqlite3 *emptyDest = 0;
  sqlite3 *zeroDest = 0;
  sqlite3 *rawFile = 0;
  sqlite3_int64 nData = 0;
  sqlite3_int64 nDirect = 0;
  sqlite3_int64 nCopy = 0;
  sqlite3_int64 nReadOnly = 0;
  sqlite3_int64 nFixed = 0;
  sqlite3_int64 nBusy = 0;
  sqlite3_int64 nAux = 0;
  sqlite3_int64 nEmpty = 0;
  unsigned char *pData = 0;
  unsigned char *pDirect = 0;
  unsigned char *pCopy = 0;
  unsigned char *pReadOnly = 0;
  unsigned char *pFixed = 0;
  unsigned char *pBusy = 0;
  unsigned char *pBad = 0;
  unsigned char *pAux = 0;
  unsigned char *pEmpty = 0;
  char *zSql = 0;
  FILE *f = 0;
  SerializeMutexProbe probe;
  pthread_t thread;
  int rc;

  make_dbpath(path, sizeof(path), "doltlite_serialize_mutex");
  make_dbpath(auxPath, sizeof(auxPath), "doltlite_serialize_aux");
  make_dbpath(imagePath, sizeof(imagePath), "doltlite_serialize_image");
  removeDbFiles(path);
  removeDbFiles(auxPath);
  removeDbFiles(imagePath);
  rc = sqlite3_open_v2(path, &db,
      SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE|SQLITE_OPEN_FULLMUTEX, 0);
  check("serialize_mutex_open", rc==SQLITE_OK);
  if( db==0 ) return;

  check("serialize_mutex_setup",
        execSql(db,
          "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT UNIQUE);"
          "CREATE INDEX t_v ON t(v);"
          "INSERT INTO t VALUES(1,'one'),(2,'two');"
          "SELECT dolt_commit('-A','-m','base');"
          "SELECT dolt_branch('feature');"
          "INSERT INTO t VALUES(3,'working');")==SQLITE_OK);
  pData = sqlite3_serialize(db, "main", &nData, 0);
  check("serialize_native_image", pData!=0 && nData>168
        && memcmp(pData, "CTLD", 4)==0);
  f = fopen(imagePath, "wb");
  check("serialize_image_file_open", f!=0);
  if( f && pData ){
    check("serialize_image_file_write",
          fwrite(pData, 1, (size_t)nData, f)==(size_t)nData);
    fclose(f);
    f = 0;
  }else if( f ){
    fclose(f);
    f = 0;
  }
  check("serialize_image_file_database", open_db(imagePath, &rawFile)==SQLITE_OK);
  if( rawFile ){
    check("serialize_image_file_rows",
          strcmp(queryScalarText(rawFile, "SELECT count(*) FROM t"), "3")==0);
    check("serialize_image_file_refs",
          strcmp(queryScalarText(rawFile,
            "SELECT count(*) FROM dolt_branches WHERE name='feature'"),
            "1")==0);
  }
  pDirect = sqlite3_serialize(
      db, "main", &nDirect, SQLITE_SERIALIZE_NOCOPY);
  check("serialize_disk_nocopy", pDirect==0 && nDirect==-1);

  probe.db = db;
  probe.rc = SQLITE_ERROR;
  rc = pthread_create(&thread, 0, trySerializeConnectionMutex, &probe);
  check("serialize_mutex_thread_create", rc==0);
  if( rc==0 ){
    rc = pthread_join(thread, 0);
    check("serialize_mutex_thread_join", rc==0);
    check("serialize_mutex_released", probe.rc==SQLITE_OK);
  }

  check("deserialize_open", open_db(":memory:", &dest)==SQLITE_OK);
  if( dest && pData ){
    rc = sqlite3_deserialize(dest, "main", pData, nData, nData,
        SQLITE_DESERIALIZE_FREEONCLOSE | SQLITE_DESERIALIZE_RESIZEABLE);
    pData = 0;
    check("deserialize_roundtrip", rc==SQLITE_OK);
    check("deserialize_rows",
          strcmp(queryScalarText(dest,
            "SELECT group_concat(id || ':' || v, ',') FROM t"),
            "1:one,2:two,3:working")==0);
    check("deserialize_index",
          strcmp(queryScalarText(dest,
            "SELECT count(*) FROM sqlite_master "
            "WHERE type='index' AND name='t_v'"), "1")==0);
    check("deserialize_refs",
          strcmp(queryScalarText(dest,
            "SELECT count(*) FROM dolt_branches WHERE name='feature'"),
            "1")==0);
    check("deserialize_history",
          strcmp(queryScalarText(dest, "SELECT count(*)>=1 FROM dolt_log"),
            "1")==0);
    check("deserialize_working_state",
          strcmp(queryScalarText(dest,
            "SELECT count(*) FROM dolt_status WHERE table_name='t'"),
            "1")==0);
    pDirect = sqlite3_serialize(
        dest, "main", &nDirect, SQLITE_SERIALIZE_NOCOPY);
    check("deserialize_nocopy", pDirect!=0 && nDirect==nData);
    check("deserialize_resizable_write",
          execSql(dest, "INSERT INTO t VALUES(4, zeroblob(20000));")
            ==SQLITE_OK);
    pCopy = sqlite3_serialize(dest, "main", &nCopy, 0);
    check("deserialize_reserialize", pCopy!=0 && nCopy>nData);
    sqlite3_free(pCopy);
    pCopy = 0;
  }

  pReadOnly = sqlite3_serialize(db, "main", &nReadOnly, 0);
  check("deserialize_readonly_image", pReadOnly!=0 && nReadOnly>0);
  check("deserialize_readonly_open", open_db(":memory:", &readOnly)==SQLITE_OK);
  if( readOnly && pReadOnly ){
    rc = sqlite3_deserialize(readOnly, 0, pReadOnly, nReadOnly, nReadOnly,
        SQLITE_DESERIALIZE_FREEONCLOSE | SQLITE_DESERIALIZE_READONLY);
    pReadOnly = 0;
    check("deserialize_readonly", rc==SQLITE_OK);
    check("deserialize_readonly_query",
          strcmp(queryScalarText(readOnly, "SELECT count(*) FROM t"), "3")==0);
    check("deserialize_readonly_rejects_write",
          execSqlSilent(readOnly, "INSERT INTO t VALUES(4,'four')")
            ==SQLITE_READONLY);
  }

  pFixed = sqlite3_serialize(db, "main", &nFixed, 0);
  check("deserialize_fixed_image", pFixed!=0 && nFixed>0);
  check("deserialize_fixed_open", open_db(":memory:", &fixed)==SQLITE_OK);
  if( fixed && pFixed ){
    rc = sqlite3_deserialize(fixed, 0, pFixed, nFixed, nFixed,
        SQLITE_DESERIALIZE_FREEONCLOSE);
    pFixed = 0;
    check("deserialize_fixed", rc==SQLITE_OK);
    check("deserialize_fixed_full",
          execSqlSilent(fixed, "INSERT INTO t VALUES(4, zeroblob(20000))")
            ==SQLITE_FULL);
  }

  pBusy = sqlite3_serialize(db, "main", &nBusy, 0);
  check("deserialize_busy_image", pBusy!=0 && nBusy>0);
  check("deserialize_busy_open", open_db(":memory:", &busy)==SQLITE_OK);
  if( busy && pBusy ){
    check("deserialize_busy_setup",
          execSql(busy,
            "CREATE TABLE busy_t(x); INSERT INTO busy_t VALUES(1);"
            "BEGIN; SELECT * FROM busy_t;")==SQLITE_OK);
    rc = sqlite3_deserialize(busy, 0, pBusy, nBusy, nBusy,
        SQLITE_DESERIALIZE_FREEONCLOSE);
    pBusy = 0;
    check("deserialize_busy", rc==SQLITE_BUSY);
    execSqlSilent(busy, "ROLLBACK");
  }

  check("deserialize_malformed_open", open_db(":memory:", &malformed)==SQLITE_OK);
  pBad = sqlite3_malloc64(256);
  check("deserialize_malformed_alloc", pBad!=0);
  if( malformed && pBad ){
    memset(pBad, 0xa5, 256);
    rc = sqlite3_deserialize(malformed, 0, pBad, 256, 256,
        SQLITE_DESERIALIZE_FREEONCLOSE);
    pBad = 0;
    check("deserialize_malformed_accept_or_reject",
          rc==SQLITE_OK || rc==SQLITE_NOTADB || rc==SQLITE_CORRUPT);
    if( rc==SQLITE_OK ){
      check("deserialize_malformed_query_rejected",
            strstr(queryScalarText(malformed, "SELECT count(*) FROM sqlite_master"),
                   "not a database")!=0);
    }
  }

  check("serialize_named_open", open_db(":memory:", &named)==SQLITE_OK);
  zSql = sqlite3_mprintf(
      "ATTACH %Q AS aux; CREATE TABLE aux.a(id INTEGER PRIMARY KEY, v TEXT);"
      "INSERT INTO aux.a VALUES(1,'attached');", auxPath);
  check("serialize_named_alloc", zSql!=0);
  check("serialize_named_setup", named && zSql
        && execSql(named, zSql)==SQLITE_OK);
  sqlite3_free(zSql);
  zSql = 0;
  if( named ){
    pAux = sqlite3_serialize(named, "aux", &nAux, 0);
    check("serialize_named_image", pAux!=0 && nAux>0);
    if( pAux ){
      rc = sqlite3_deserialize(named, "aux", pAux, nAux, nAux,
          SQLITE_DESERIALIZE_FREEONCLOSE | SQLITE_DESERIALIZE_RESIZEABLE);
      pAux = 0;
      check("deserialize_named", rc==SQLITE_OK);
      check("deserialize_named_rows",
            strcmp(queryScalarText(named, "SELECT v FROM aux.a"),
                   "attached")==0);
    }
  }

  check("serialize_empty_source_open", open_db(":memory:", &emptySrc)==SQLITE_OK);
  check("serialize_empty_dest_open", open_db(":memory:", &emptyDest)==SQLITE_OK);
  if( emptySrc && emptyDest ){
    pEmpty = sqlite3_serialize(emptySrc, 0, &nEmpty, 0);
    check("serialize_empty_image", pEmpty!=0 && nEmpty>168);
    if( pEmpty ){
      rc = sqlite3_deserialize(emptyDest, 0, pEmpty, nEmpty, nEmpty,
          SQLITE_DESERIALIZE_FREEONCLOSE | SQLITE_DESERIALIZE_RESIZEABLE);
      pEmpty = 0;
      check("deserialize_empty", rc==SQLITE_OK);
      check("deserialize_empty_schema",
            strcmp(queryScalarText(emptyDest,
              "SELECT count(*) FROM sqlite_master"), "0")==0);
    }
  }
  check("deserialize_zero_open", open_db(":memory:", &zeroDest)==SQLITE_OK);
  pEmpty = sqlite3_malloc64(1);
  check("deserialize_zero_alloc", pEmpty!=0);
  if( zeroDest && pEmpty ){
    rc = sqlite3_deserialize(zeroDest, 0, pEmpty, 0, 1,
        SQLITE_DESERIALIZE_FREEONCLOSE | SQLITE_DESERIALIZE_RESIZEABLE);
    pEmpty = 0;
    check("deserialize_zero", rc==SQLITE_OK);
    check("deserialize_zero_write",
          execSql(zeroDest,
            "CREATE TABLE zero_t(v TEXT); INSERT INTO zero_t VALUES('ok');")
            ==SQLITE_OK);
  }

  sqlite3_free(pData);
  sqlite3_free(pReadOnly);
  sqlite3_free(pFixed);
  sqlite3_free(pBusy);
  sqlite3_free(pBad);
  sqlite3_free(pAux);
  sqlite3_free(pEmpty);
  sqlite3_close(rawFile);
  sqlite3_close(zeroDest);
  sqlite3_close(emptyDest);
  sqlite3_close(emptySrc);
  sqlite3_close(named);
  sqlite3_close(malformed);
  sqlite3_close(busy);
  sqlite3_close(fixed);
  sqlite3_close(readOnly);
  sqlite3_close(dest);
  sqlite3_close(db);
  removeDbFiles(path);
  removeDbFiles(auxPath);
  removeDbFiles(imagePath);
}

static void run_memory_readonly_open(void){
  sqlite3 *db = 0;
  int rc;

  check("memory_readonly_open",
        sqlite3_open_v2(":memory:", &db, SQLITE_OPEN_READONLY, 0)==SQLITE_OK);
  if( db==0 ) return;
  rc = sqlite3_exec(db, "CREATE TABLE t1(x)", 0, 0, 0);
  check("memory_readonly_write_rejected", rc==SQLITE_READONLY);
  check("memory_readonly_errmsg",
        strcmp(sqlite3_errmsg(db), "attempt to write a readonly database")==0);
  check("memory_readonly_read_ok",
        strcmp(queryScalarText(db, "SELECT count(*) FROM sqlite_master"), "0")==0);
  sqlite3_close(db);
  db = 0;

  check("memory_readwrite_open",
        sqlite3_open_v2(":memory:", &db,
            SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE, 0)==SQLITE_OK);
  if( db==0 ) return;
  check("memory_readwrite_write_ok",
        sqlite3_exec(db, "CREATE TABLE t1(x)", 0, 0, 0)==SQLITE_OK);
  sqlite3_close(db);
}

static void run_rowid_in_integer_literals_uses_rowset(void){
  sqlite3 *db = 0;
  sqlite3_stmt *stmt = 0;
  int rc;
  int sawRowSetRead = 0;
  int sawOpenEphemeral = 0;

  check("rowid_in_rowset_open", open_db(":memory:", &db)==SQLITE_OK);
  if( db==0 ) return;

  rc = execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');"
  );
  check("rowid_in_rowset_setup", rc==SQLITE_OK);

  rc = sqlite3_prepare_v2(db,
    "EXPLAIN SELECT id, v FROM t WHERE id IN (3,1,2,2)",
    -1, &stmt, 0
  );
  check("rowid_in_rowset_explain_prepare", rc==SQLITE_OK);
  while( rc==SQLITE_OK && sqlite3_step(stmt)==SQLITE_ROW ){
    const char *zOpcode = (const char*)sqlite3_column_text(stmt, 1);
    if( zOpcode && strcmp(zOpcode, "RowSetRead")==0 ) sawRowSetRead = 1;
    if( zOpcode && strcmp(zOpcode, "OpenEphemeral")==0 ) sawOpenEphemeral = 1;
  }
  sqlite3_finalize(stmt);

  check("rowid_in_rowset_opcode", sawRowSetRead==1);
  check("rowid_in_no_ephemeral_opcode", sawOpenEphemeral==0);
  check("rowid_in_sorted_unique_results",
        strcmp(queryScalarText(db,
          "SELECT group_concat(id || ':' || v) "
          "FROM (SELECT id, v FROM t WHERE id IN (3,1,2,2));"),
          "1:a,2:b,3:c")==0);

  sqlite3_close(db);
}

static void make_dbpath(char *zBuf, size_t nBuf, const char *zBase);
static void removeDbFiles(const char *path);

static void run_backup_safety(void){
  sqlite3 *srcBig = 0;
  sqlite3 *srcSmall = 0;
  sqlite3 *dest = 0;
  sqlite3 *sameA = 0;
  sqlite3 *sameB = 0;
  sqlite3 *srcAttached = 0;
  sqlite3 *destAttached = 0;
  sqlite3 *memSrc = 0;
  sqlite3 *memDest = 0;
  sqlite3 *memClone = 0;
  sqlite3 *fileFromMem = 0;
  sqlite3_backup *pBackup;
  char zBig[512];
  char zSmall[512];
  char zDest[512];
  char zSame[512];
  char zSrcHost[512];
  char zDestHost[512];
  char zSrcAux[512];
  char zDestAux[512];
  char zDestOther[512];
  char zMemFile[512];
  char *sql = 0;
  sqlite3_int64 nSmall;
  sqlite3_int64 nDest;
  int rc;

  make_dbpath(zBig, sizeof(zBig), "backup_safety_big");
  make_dbpath(zSmall, sizeof(zSmall), "backup_safety_small");
  make_dbpath(zDest, sizeof(zDest), "backup_safety_dest");
  make_dbpath(zSame, sizeof(zSame), "backup_safety_same");
  make_dbpath(zSrcHost, sizeof(zSrcHost), "backup_safety_src_host");
  make_dbpath(zDestHost, sizeof(zDestHost), "backup_safety_dest_host");
  make_dbpath(zSrcAux, sizeof(zSrcAux), "backup_safety_src_aux");
  make_dbpath(zDestAux, sizeof(zDestAux), "backup_safety_dest_aux");
  make_dbpath(zDestOther, sizeof(zDestOther), "backup_safety_dest_other");
  make_dbpath(zMemFile, sizeof(zMemFile), "backup_safety_mem_file");
  removeDbFiles(zBig);
  removeDbFiles(zSmall);
  removeDbFiles(zDest);
  removeDbFiles(zSame);
  removeDbFiles(zSrcHost);
  removeDbFiles(zDestHost);
  removeDbFiles(zSrcAux);
  removeDbFiles(zDestAux);
  removeDbFiles(zDestOther);
  removeDbFiles(zMemFile);

  check("backup_safety_open_big", open_db(zBig, &srcBig)==SQLITE_OK);
  check("backup_safety_open_dest", open_db(zDest, &dest)==SQLITE_OK);
  if( srcBig==0 || dest==0 ) goto backup_safety_done;

  rc = execSql(srcBig,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<1200) "
    "INSERT INTO t SELECT x, printf('big-%04d', x) FROM c;");
  check("backup_safety_seed_big", rc==SQLITE_OK);
  check("backup_safety_copy_big_one_step", backup_db(srcBig, dest)==SQLITE_OK);
  check("backup_safety_dest_big_visible",
        strcmp(queryScalarText(dest, "SELECT count(*) FROM t"), "1200")==0);

  sqlite3_close(srcBig);
  sqlite3_close(dest);
  srcBig = 0;
  dest = 0;

  check("backup_safety_open_small", open_db(zSmall, &srcSmall)==SQLITE_OK);
  check("backup_safety_reopen_dest", open_db(zDest, &dest)==SQLITE_OK);
  if( srcSmall==0 || dest==0 ) goto backup_safety_done;

  rc = execSql(srcSmall,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1, 'small');");
  check("backup_safety_seed_small", rc==SQLITE_OK);
  nSmall = file_size_or_negative(zSmall);
  check("backup_safety_small_has_size", nSmall > 0);

  check("backup_safety_copy_small", backup_db(srcSmall, dest)==SQLITE_OK);
  nDest = file_size_or_negative(zDest);
  check("backup_safety_dest_truncated", nDest==nSmall);
  check("backup_safety_dest_handle_refreshed",
        strcmp(queryScalarText(dest, "SELECT count(*) FROM t"), "1")==0);
  check("backup_safety_dest_value_refreshed",
        strcmp(queryScalarText(dest, "SELECT v FROM t WHERE id=1"), "small")==0);

  check("backup_safety_open_memory_dest",
        open_db(":memory:", &memDest)==SQLITE_OK);
  if( memDest ){
    check("backup_safety_file_to_memory",
          backup_db(srcSmall, memDest)==SQLITE_OK);
    check("backup_safety_file_to_memory_rows",
          strcmp(queryScalarText(memDest,
            "SELECT group_concat(id || ':' || v) FROM t"), "1:small")==0);
  }

  check("backup_safety_open_memory_source",
        open_db(":memory:", &memSrc)==SQLITE_OK);
  if( memSrc ){
    check("backup_safety_seed_memory_source",
          execSql(memSrc,
            "CREATE TABLE m(id INTEGER PRIMARY KEY, v TEXT);"
            "CREATE INDEX m_v ON m(v);"
            "INSERT INTO m VALUES(1,'memory'),(2,'source');"
            "SELECT dolt_branch('feature');")==SQLITE_OK);
    check("backup_safety_open_file_from_memory",
          open_db(zMemFile, &fileFromMem)==SQLITE_OK);
    if( fileFromMem ){
      check("backup_safety_memory_to_file",
            backup_db(memSrc, fileFromMem)==SQLITE_OK);
      check("backup_safety_memory_to_file_rows",
            strcmp(queryScalarText(fileFromMem,
              "SELECT group_concat(id || ':' || v, ',') FROM m"),
              "1:memory,2:source")==0);
      check("backup_safety_memory_to_file_index",
            strcmp(queryScalarText(fileFromMem,
              "SELECT count(*) FROM sqlite_master "
              "WHERE type='index' AND name='m_v'"), "1")==0);
      check("backup_safety_memory_to_file_refs",
            strcmp(queryScalarText(fileFromMem,
              "SELECT count(*) FROM dolt_branches WHERE name='feature'"),
              "1")==0);
    }

    check("backup_safety_open_memory_clone",
          open_db(":memory:", &memClone)==SQLITE_OK);
    if( memClone ){
      check("backup_safety_seed_memory_clone",
            execSql(memClone,
              "CREATE TABLE old(v TEXT); INSERT INTO old VALUES('old');")
              ==SQLITE_OK);
      check("backup_safety_memory_to_memory",
            backup_db(memSrc, memClone)==SQLITE_OK);
      check("backup_safety_memory_to_memory_rows",
            strcmp(queryScalarText(memClone,
              "SELECT group_concat(id || ':' || v, ',') FROM m"),
              "1:memory,2:source")==0);
      check("backup_safety_memory_to_memory_replaced",
            strcmp(queryScalarText(memClone,
              "SELECT count(*) FROM sqlite_master WHERE name='old'"), "0")==0);
      check("backup_safety_memory_to_memory_refs",
            strcmp(queryScalarText(memClone,
              "SELECT count(*) FROM dolt_branches WHERE name='feature'"),
              "1")==0);
    }
  }

  check("backup_safety_open_same_a", open_db(zSame, &sameA)==SQLITE_OK);
  check("backup_safety_open_same_b", open_db(zSame, &sameB)==SQLITE_OK);
  if( sameA && sameB ){
    check("backup_safety_same_seed",
          execSql(sameA, "CREATE TABLE s(x); INSERT INTO s VALUES(1);")==SQLITE_OK);
    pBackup = sqlite3_backup_init(sameB, "main", sameA, "main");
    check("backup_safety_same_file_rejected", pBackup==0);
    check("backup_safety_same_file_message",
          strstr(sqlite3_errmsg(sameB), "same database")!=0);
    if( pBackup ) sqlite3_backup_finish(pBackup);
  }

  check("backup_safety_open_attached_source",
        open_db(zSrcHost, &srcAttached)==SQLITE_OK);
  check("backup_safety_open_attached_dest",
        open_db(zDestHost, &destAttached)==SQLITE_OK);
  if( srcAttached && destAttached ){
    sql = sqlite3_mprintf(
        "ATTACH %Q AS aux;"
        "CREATE TABLE aux.t(id INTEGER PRIMARY KEY, v TEXT);"
        "INSERT INTO aux.t VALUES(1, 'attached'), (2, 'source');",
        zSrcAux);
    check("backup_safety_attach_source_alloc", sql!=0);
    check("backup_safety_attach_source",
          sql && execSql(srcAttached, sql)==SQLITE_OK);
    sqlite3_free(sql);
    sql = 0;
    sql = sqlite3_mprintf(
        "CREATE TABLE sentinel(v TEXT);"
        "INSERT INTO sentinel VALUES('main');"
        "ATTACH %Q AS aux;"
        "CREATE TABLE aux.old(v TEXT);"
        "INSERT INTO aux.old VALUES('old');",
        zDestAux);
    check("backup_safety_attach_dest_alloc", sql!=0);
    check("backup_safety_attach_dest",
          sql && execSql(destAttached, sql)==SQLITE_OK);
    sqlite3_free(sql);
    sql = 0;

    pBackup = sqlite3_backup_init(destAttached, "aux", srcAttached, "aux");
    check("backup_safety_attached_init_for_detach", pBackup!=0);
    if( pBackup ){
      check("backup_safety_attached_dest_detach",
            execSqlSilent(destAttached, "DETACH aux")==SQLITE_OK);
      sql = sqlite3_mprintf(
          "ATTACH %Q AS aux;"
          "CREATE TABLE aux.other(v TEXT);"
          "INSERT INTO aux.other VALUES('other');",
          zDestOther);
      check("backup_safety_attach_replacement_alloc", sql!=0);
      check("backup_safety_attach_replacement",
            sql && execSql(destAttached, sql)==SQLITE_OK);
      sqlite3_free(sql);
      sql = 0;
      check("backup_safety_attached_replaced_dest",
            sqlite3_backup_step(pBackup, 1)==SQLITE_ERROR);
      check("backup_safety_attached_replaced_dest_message",
            strstr(sqlite3_errmsg(destAttached), "unknown database aux")!=0);
      check("backup_safety_attached_replaced_dest_finish",
            sqlite3_backup_finish(pBackup)==SQLITE_ERROR);
      check("backup_safety_replacement_unchanged",
            strcmp(queryScalarText(destAttached,
              "SELECT v FROM aux.other"), "other")==0);
      check("backup_safety_detach_replacement",
            execSql(destAttached, "DETACH aux")==SQLITE_OK);
    }

    sql = sqlite3_mprintf("ATTACH %Q AS aux", zDestAux);
    check("backup_safety_reattach_dest_alloc", sql!=0);
    check("backup_safety_reattach_dest",
          sql && execSql(destAttached, sql)==SQLITE_OK);
    sqlite3_free(sql);
    sql = 0;
    check("backup_safety_original_unchanged",
          strcmp(queryScalarText(destAttached,
            "SELECT v FROM aux.old"), "old")==0);

    pBackup = sqlite3_backup_init(destAttached, "aux", srcAttached, "aux");
    check("backup_safety_attached_init", pBackup!=0);
    if( pBackup ){
      check("backup_safety_attached_source_detach_locked",
            execSqlSilent(srcAttached, "DETACH aux")!=SQLITE_OK
            && strstr(sqlite3_errmsg(srcAttached), "database aux is locked")!=0);
      check("backup_safety_attached_source_close_busy",
            sqlite3_close(srcAttached)==SQLITE_BUSY);
      check("backup_safety_attached_step",
            sqlite3_backup_step(pBackup, 1)==SQLITE_DONE);
      check("backup_safety_attached_finish",
            sqlite3_backup_finish(pBackup)==SQLITE_OK);
      check("backup_safety_attached_source_detach_after_finish",
            execSql(srcAttached, "DETACH aux")==SQLITE_OK);
    }
    check("backup_safety_attached_dest_rows",
          strcmp(queryScalarText(destAttached,
            "SELECT group_concat(id || ':' || v, ',') "
            "FROM (SELECT id, v FROM aux.t ORDER BY id)"),
            "1:attached,2:source")==0);
    check("backup_safety_attached_dest_old_replaced",
          strcmp(queryScalarText(destAttached,
            "SELECT count(*) FROM aux.sqlite_master WHERE name='old'"), "0")==0);
    check("backup_safety_attached_dest_main_untouched",
          strcmp(queryScalarText(destAttached,
            "SELECT v FROM main.sentinel"), "main")==0);
  }

backup_safety_done:
  sqlite3_free(sql);
  if( sameA ) sqlite3_close(sameA);
  if( sameB ) sqlite3_close(sameB);
  if( srcAttached ) sqlite3_close(srcAttached);
  if( destAttached ) sqlite3_close(destAttached);
  if( memSrc ) sqlite3_close(memSrc);
  if( memDest ) sqlite3_close(memDest);
  if( memClone ) sqlite3_close(memClone);
  if( fileFromMem ) sqlite3_close(fileFromMem);
  if( srcBig ) sqlite3_close(srcBig);
  if( srcSmall ) sqlite3_close(srcSmall);
  if( dest ) sqlite3_close(dest);
  removeDbFiles(zBig);
  removeDbFiles(zSmall);
  removeDbFiles(zDest);
  removeDbFiles(zSame);
  removeDbFiles(zSrcHost);
  removeDbFiles(zDestHost);
  removeDbFiles(zSrcAux);
  removeDbFiles(zDestAux);
  removeDbFiles(zDestOther);
  removeDbFiles(zMemFile);
}

static void run_integer_pk_autocommit_append_correctness(void){
  sqlite3 *db = 0;
  sqlite3_stmt *stmt = 0;
  char dbpath[256];
  int rc;
  int i;
  const int nRow = 1500;

  make_dbpath(dbpath, sizeof(dbpath), "test_integer_pk_autocommit_append");
  removeDbFiles(dbpath);

  check("integer_pk_append_open", open_db(dbpath, &db)==SQLITE_OK);
  if( db==0 ) goto integer_pk_append_done;

  check("integer_pk_append_create",
        execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER, v TEXT);")
        ==SQLITE_OK);
  rc = sqlite3_prepare_v2(db,
      "INSERT INTO t VALUES(?1, ?2, ?3)", -1, &stmt, 0);
  check("integer_pk_append_prepare", rc==SQLITE_OK);
  for(i=1; rc==SQLITE_OK && i<=nRow; i++){
    char zVal[32];
    sqlite3_snprintf(sizeof(zVal), zVal, "v-%04d", i);
    sqlite3_bind_int(stmt, 1, i);
    sqlite3_bind_int(stmt, 2, i*2);
    sqlite3_bind_text(stmt, 3, zVal, -1, SQLITE_TRANSIENT);
    rc = sqlite3_step(stmt);
    if( rc!=SQLITE_DONE ){
      fprintf(stderr, "  insert failed at row %d: %s (rc=%d)\n",
              i, sqlite3_errmsg(db), rc);
      break;
    }
    rc = sqlite3_reset(stmt);
    sqlite3_clear_bindings(stmt);
  }
  sqlite3_finalize(stmt);
  stmt = 0;
  check("integer_pk_append_inserts", rc==SQLITE_OK);

  check("integer_pk_append_count_min_max_sum",
        strcmp(queryScalarText(db,
          "SELECT count(*) || ',' || min(id) || ',' || max(id) || ',' || sum(k) "
          "FROM t"), "1500,1,1500,2251500")==0);
  check("integer_pk_append_point_read",
        strcmp(queryScalarText(db, "SELECT v FROM t WHERE id=1499"), "v-1499")==0);
  check("integer_pk_append_integrity",
        strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);

  sqlite3_close(db);
  db = 0;

  check("integer_pk_append_reopen", open_db(dbpath, &db)==SQLITE_OK);
  if( db ){
    check("integer_pk_append_reopen_count",
          strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "1500")==0);
    check("integer_pk_append_reopen_point_read",
          strcmp(queryScalarText(db, "SELECT v FROM t WHERE id=1500"), "v-1500")==0);
    check("integer_pk_append_reopen_integrity",
          strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);
  }

integer_pk_append_done:
  if( stmt ) sqlite3_finalize(stmt);
  if( db ) sqlite3_close(db);
  removeDbFiles(dbpath);
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

typedef struct RepoStateSnapshot RepoStateSnapshot;
struct RepoStateSnapshot {
  char zBranch[128];
  char zHead[128];
  char zStatusCount[32];
  char zConflictsCount[32];
  char zRemotesCount[32];
  char zRebasePlanCount[32];
  u8 isMerging;
  u8 isRebasing;
  ProllyHash mergeHash;
  ProllyHash conflictsHash;
  ProllyHash rebaseOntoHash;
  char zOrigBranch[128];
};

static int count_diff_change(void *pCtx, const ProllyDiffChange *pChange){
  DiffCountCtx *p = (DiffCountCtx*)pCtx;
  (void)pChange;
  p->nChange++;
  return SQLITE_OK;
}

static void capture_repo_state_snapshot(sqlite3 *db, RepoStateSnapshot *p){
  const char *zOrigBranch = 0;
  memset(p, 0, sizeof(*p));
  sqlite3_snprintf(sizeof(p->zBranch), p->zBranch, "%s",
                   queryScalarText(db, "SELECT active_branch()"));
  sqlite3_snprintf(sizeof(p->zHead), p->zHead, "%s",
                   queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"));
  sqlite3_snprintf(sizeof(p->zStatusCount), p->zStatusCount, "%s",
                   queryScalarText(db, "SELECT count(*) FROM dolt_status"));
  sqlite3_snprintf(sizeof(p->zConflictsCount), p->zConflictsCount, "%s",
                   queryScalarText(db, "SELECT count(*) FROM dolt_conflicts"));
  sqlite3_snprintf(sizeof(p->zRemotesCount), p->zRemotesCount, "%s",
                   queryScalarText(db, "SELECT count(*) FROM dolt_remotes"));
  sqlite3_snprintf(sizeof(p->zRebasePlanCount), p->zRebasePlanCount, "%s",
                   queryScalarText(db, "SELECT count(*) FROM sqlite_master "
                             "WHERE type='table' AND name='dolt_rebase'"));
  doltliteGetSessionMergeState(db, &p->isMerging, &p->mergeHash, &p->conflictsHash);
  doltliteGetSessionRebaseState(db, &p->isRebasing, 0, &p->rebaseOntoHash, &zOrigBranch, 0);
  if( zOrigBranch ){
    sqlite3_snprintf(sizeof(p->zOrigBranch), p->zOrigBranch, "%s", zOrigBranch);
  }
}

static int repo_state_snapshot_eq(const RepoStateSnapshot *a, const RepoStateSnapshot *b){
  return strcmp(a->zBranch, b->zBranch)==0
      && strcmp(a->zHead, b->zHead)==0
      && strcmp(a->zStatusCount, b->zStatusCount)==0
      && strcmp(a->zConflictsCount, b->zConflictsCount)==0
      && strcmp(a->zRemotesCount, b->zRemotesCount)==0
      && strcmp(a->zRebasePlanCount, b->zRebasePlanCount)==0
      && a->isMerging==b->isMerging
      && a->isRebasing==b->isRebasing
      && prollyHashCompare(&a->mergeHash, &b->mergeHash)==0
      && prollyHashCompare(&a->conflictsHash, &b->conflictsHash)==0
      && prollyHashCompare(&a->rebaseOntoHash, &b->rebaseOntoHash)==0
      && strcmp(a->zOrigBranch, b->zOrigBranch)==0;
}

static void make_dbpath(char *zBuf, size_t nBuf, const char *zBase){
  snprintf(zBuf, nBuf, "/tmp/%s_%ld.db", zBase, (long)getpid());
}

static void removeDbFiles(const char *path){
  char tmp[512];
  remove(path);
  snprintf(tmp, sizeof(tmp), "%s-wal", path);
  remove(tmp);
  snprintf(tmp, sizeof(tmp), "%s-shm", path);
  remove(tmp);
  snprintf(tmp, sizeof(tmp), "%s-journal", path);
  remove(tmp);
}

static void checkBranchState(
  sqlite3 *db,
  const char *zPrefix,
  const char *zActiveBranch,
  const char *zBranchCount,
  const char *zNamedBranch,
  const char *zNamedBranchCount
){
  char zCheck[128];
  char zSql[160];

  sqlite3_snprintf(sizeof(zCheck), zCheck, "%s_active_branch", zPrefix);
  check(zCheck,
        strcmp(queryScalarText(db, "SELECT active_branch()"), zActiveBranch)==0);

  sqlite3_snprintf(sizeof(zCheck), zCheck, "%s_branch_count", zPrefix);
  check(zCheck,
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_branches"),
               zBranchCount)==0);

  if( zNamedBranch ){
    sqlite3_snprintf(sizeof(zSql), zSql,
                     "SELECT count(*) FROM dolt_branches WHERE name='%q'",
                     zNamedBranch);
    sqlite3_snprintf(sizeof(zCheck), zCheck, "%s_named_branch", zPrefix);
    check(zCheck, strcmp(queryScalarText(db, zSql), zNamedBranchCount)==0);
  }
}

static void make_prolly_blob_key(int iKey, u8 *aBuf, int nBuf){
  snprintf((char*)aBuf, nBuf, "key-%028d", iKey);
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
static int gFailOpenMainOnce = 0;
static int gFailHits = 0;
static int gFailFullPathnameHits = 0;
static const char *gFullPathnameSuffix = 0;
static char gRewrittenFullPath[512];
static int gRegressionFaultCode = 0;
static int gRegressionFaultHits = 0;

static int regressionFaultCallback(int iCode){
  if( iCode==gRegressionFaultCode ){
    gRegressionFaultHits++;
    return 1;
  }
  return 0;
}

static int failAccess(sqlite3_vfs *pVfs, const char *zName, int flags, int *pResOut);
static int failFullPathname(sqlite3_vfs *pVfs, const char *zName, int nOut, char *zOut);

static void normalizeNonVerbatimWinPath(char *zPath){
#if SQLITE_OS_WIN
  if( zPath[0]=='\\' && zPath[1]=='\\'
   && (zPath[2]=='?' || zPath[2]=='.') && zPath[3]=='\\'
  ){
    return;
  }
  while( *zPath ){
    if( *zPath=='\\' ) *zPath = '/';
    zPath++;
  }
#else
  (void)zPath;
#endif
}

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
  int nName = zName ? (int)strlen(zName) : 0;
  int isGcTmp = nName>=7 && strcmp(zName+nName-7, "-gc-tmp")==0;
  int isDoltLiteLock = nName>=5 && strcmp(zName+nName-5, "-lock")==0;

  memset(p, 0, sizeof(*p));
  if( gFailOpenMainOnce>0 && !isGcTmp && !isDoltLiteLock
   && (flags & SQLITE_OPEN_MAIN_DB)!=0
  ){
    gFailOpenMainOnce--;
    gFailHits++;
    return SQLITE_CANTOPEN;
  }
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
  gFailVfs.xFullPathname = failFullPathname;
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

static int failFullPathname(sqlite3_vfs *pVfs, const char *zName, int nOut, char *zOut){
  int rc;
  int n;
  int nSuffix;
  (void)pVfs;
  gFailFullPathnameHits++;
  rc = gBaseVfs->xFullPathname(gBaseVfs, zName, nOut, zOut);
  if( (rc==SQLITE_OK || rc==SQLITE_OK_SYMLINK) && gFullPathnameSuffix ){
    n = (int)strlen(zOut);
    nSuffix = (int)strlen(gFullPathnameSuffix);
    if( n<nSuffix || strcmp(zOut+n-nSuffix, gFullPathnameSuffix)!=0 ){
      if( n+nSuffix+1>nOut ) return SQLITE_CANTOPEN;
      memcpy(zOut+n, gFullPathnameSuffix, nSuffix+1);
    }
    sqlite3_snprintf(sizeof(gRewrittenFullPath), gRewrittenFullPath, "%s", zOut);
  }
  return rc;
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
  removeDbFiles(dbpath);

  check("open_db1", open_db(dbpath, &db1)==SQLITE_OK);
  check("setup_schema", execSql(db1,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');")==SQLITE_OK);

  snprintf(firstCommit, sizeof(firstCommit), "%s",
           queryScalarText(db1, "SELECT dolt_commit('-A', '-m', 'init')"));
  check("first_commit_hash", strlen(firstCommit)==40);
  check("open_db2", open_db(dbpath, &db2)==SQLITE_OK);
  /* Pin db2's snapshot before the peer commits so it genuinely cannot
  ** observe that commit. Merely having opened earlier no longer makes a
  ** session stale: a connection adopts the branch head with the catalog at
  ** each statement, so an unpinned db2 would reset from a current view --
  ** an ordinary backward reset, and what Dolt does (see
  ** test_concurrent_refs_informed_reset_moves_branch). */
  check("pin_db2_snapshot", execSql(db2, "BEGIN")==SQLITE_OK);
  queryScalarText(db2, "SELECT count(*) FROM t");

  check("insert_second_row",
    execSql(db1, "INSERT INTO t VALUES(2,'b')")==SQLITE_OK);
  snprintf(secondCommit, sizeof(secondCommit), "%s",
           queryScalarText(db1, "SELECT dolt_commit('-A', '-m', 'second')"));
  check("second_commit_hash", strlen(secondCommit)==40);

  snprintf(sql, sizeof(sql), "SELECT dolt_reset('%s')", firstCommit);
  queryScalarText(db2, sql);
  check("stale_reset_is_rejected",
    strstr(gBuf, "ERROR")!=0 || strstr(gBuf, "conflict")!=0);
  execSqlSilent(db2, "ROLLBACK");

  sqlite3_close(db1);
  sqlite3_close(db2);

  check("open_db3", open_db(dbpath, &db3)==SQLITE_OK);
  check("newer_commit_still_head",
    strcmp(queryScalarText(db3, "SELECT message FROM dolt_log LIMIT 1"), "second")==0);
  snprintf(sql, sizeof(sql),
           "SELECT count(*) FROM dolt_log WHERE commit_hash='%s'", secondCommit);
  check("newer_commit_still_visible_in_log",
    strcmp(queryScalarText(db3, sql), "1")==0);
  check("branch_history_has_both_commits",
    strcmp(queryScalarText(db3, "SELECT count(*) FROM dolt_log"), "3")==0);

  sqlite3_close(db3);
  removeDbFiles(dbpath);
}

/* The counterpart to the stale case: a session with a current view that
** asks to reset backward gets the reset. Dolt 2.2.2 does the same -- the
** branch moves to the named commit and the newer commit leaves the log. */
static void test_concurrent_refs_informed_reset_moves_branch(void){
  sqlite3 *db1 = 0, *db2 = 0, *db3 = 0;
  char dbpath[256];
  char firstCommit[128];
  char sql[512];

  printf("--- Test 1b: informed dolt_reset moves the branch ---\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_concurrent_refs_informed_reset");
  removeDbFiles(dbpath);

  check("ir_open_db1", open_db(dbpath, &db1)==SQLITE_OK);
  check("ir_setup", execSql(db1,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');")==SQLITE_OK);
  snprintf(firstCommit, sizeof(firstCommit), "%s",
           queryScalarText(db1, "SELECT dolt_commit('-A', '-m', 'init')"));
  check("ir_first_commit", strlen(firstCommit)==40);

  check("ir_open_db2", open_db(dbpath, &db2)==SQLITE_OK);
  check("ir_peer_commit", execSql(db1,
    "INSERT INTO t VALUES(2,'b');"
    "SELECT dolt_commit('-A', '-m', 'second');")==SQLITE_OK);

  /* db2 has no pinned snapshot, so this statement sees the peer commit
  ** first and the reset is an informed one. */
  snprintf(sql, sizeof(sql), "SELECT dolt_reset('%s')", firstCommit);
  queryScalarText(db2, sql);
  check("ir_reset_accepted", strstr(gBuf, "ERROR")==0 && strstr(gBuf, "conflict")==0);

  sqlite3_close(db1);
  sqlite3_close(db2);

  check("ir_open_db3", open_db(dbpath, &db3)==SQLITE_OK);
  check("ir_branch_moved_back",
    strcmp(queryScalarText(db3, "SELECT message FROM dolt_log LIMIT 1"), "init")==0);
  check("ir_log_shrank",
    strcmp(queryScalarText(db3, "SELECT count(*) FROM dolt_log"), "2")==0);
  sqlite3_close(db3);
  removeDbFiles(dbpath);
}

static void test_concurrent_refs_checkout_refreshes_branch(void){
  sqlite3 *db1 = 0, *db2 = 0;
  char dbpath[256];

  printf("--- Test 2: stale dolt_checkout refreshes target branch ---\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_concurrent_refs_checkout");
  removeDbFiles(dbpath);

  check("checkout_open_db1", open_db(dbpath, &db1)==SQLITE_OK);
  check("checkout_setup_schema", execSql(db1,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');")==SQLITE_OK);
  check("checkout_init_commit",
    strlen(queryScalarText(db1, "SELECT dolt_commit('-A', '-m', 'init')"))==40);
  check("checkout_create_branch",
    strcmp(queryScalarText(db1, "SELECT dolt_branch('feature')"), "0")==0);
  check("checkout_open_db2", open_db(dbpath, &db2)==SQLITE_OK);
  check("checkout_switch_db1_feature",
    strcmp(queryScalarText(db1, "SELECT dolt_checkout('feature')"), "0")==0);
  check("checkout_feature_insert",
    execSql(db1, "INSERT INTO t VALUES(2,'feature')")==SQLITE_OK);
  check("checkout_feature_commit",
    strlen(queryScalarText(db1, "SELECT dolt_commit('-A', '-m', 'feature update')"))==40);

  check("checkout_stale_connection_switches",
    strcmp(queryScalarText(db2, "SELECT dolt_checkout('feature')"), "0")==0);
  check("checkout_latest_branch_tip_visible",
    strcmp(queryScalarText(db2, "SELECT message FROM dolt_log LIMIT 1"), "feature update")==0);
  check("checkout_latest_branch_data_visible",
    strcmp(queryScalarText(db2, "SELECT count(*) FROM t"), "2")==0);

  sqlite3_close(db1);
  sqlite3_close(db2);
  removeDbFiles(dbpath);
}

static void run_concurrent_refs(void){
  printf("=== Concurrent Refs Test ===\n\n");
  test_concurrent_refs_stale_reset_is_rejected();
  test_concurrent_refs_informed_reset_moves_branch();
  test_concurrent_refs_checkout_refreshes_branch();
}

static void run_checkout_persist_failure(void){
  sqlite3 *db1 = 0;
  sqlite3 *db2 = 0;
  char dbpath[256];
  const char *res;

  printf("=== Checkout Persist Failure Test ===\n\n");
  printf("--- Test 1: dolt_checkout surfaces final persist failure ---\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_checkout_persist_failure");
  removeDbFiles(dbpath);
  gFailSyncOnce = 0;
  gFailOpenMainOnce = 0;
  gFailHits = 0;

  check("register_fail_vfs", registerFailVfs()==SQLITE_OK);
  check("open_db1", open_fail_db(dbpath, &db1)==SQLITE_OK);

  check("setup_schema", execSql(db1,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');")==SQLITE_OK);
  check("init_commit",
    strlen(queryScalarText(db1, "SELECT dolt_commit('-A', '-m', 'init')"))==40);
  check("create_feature_branch",
    strcmp(queryScalarText(db1, "SELECT dolt_branch('feature')"), "0")==0);

  gFailSyncOnce = 1;
  res = queryScalarText(db1, "SELECT dolt_checkout('feature')");
  check("persist_failure_was_injected", gFailHits>0);
  check("checkout_returns_error_on_persist_failure", strstr(res, "ERROR:")!=0);
  check("session_branch_restored_after_error",
    strcmp(queryScalarText(db1, "SELECT active_branch()"), "main")==0);
  check("working_rows_preserved_after_checkout_error",
    strcmp(queryScalarText(db1, "SELECT count(*) FROM t"), "1")==0);

  sqlite3_close(db1);
  db1 = 0;

  check("reopen_db_after_checkout_persist_failure", open_db(dbpath, &db2)==SQLITE_OK);
  check("active_branch_persists_after_checkout_error",
    strcmp(queryScalarText(db2, "SELECT active_branch()"), "main")==0);
  check("main_rows_persist_after_checkout_error",
    strcmp(queryScalarText(db2, "SELECT count(*) FROM t"), "1")==0);
  check("feature_tip_still_visible_after_checkout_error",
    strcmp(queryScalarText(db2,
      "SELECT latest_commit_message FROM dolt_branches WHERE name='feature'"),
      "init")==0);
  check("feature_checkout_still_works_after_checkout_error",
    strcmp(queryScalarText(db2, "SELECT dolt_checkout('feature')"), "0")==0);
  check("feature_rows_visible_after_reopen_checkout",
    strcmp(queryScalarText(db2, "SELECT count(*) FROM t"), "1")==0);

  sqlite3_close(db2);
  removeDbFiles(dbpath);
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
  removeDbFiles(dbpath);

  check("open_db", sqlite3_open(dbpath, &db)==SQLITE_OK);
  if( !db ) return;

  check("create_table", execSql(db,
      "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);")==SQLITE_OK);
  check("serialize_before",
      doltliteFlushAndSerializeCatalog(db, &aBefore, &nBefore)==SQLITE_OK);

  iTable = table_rootpage(db, "t");
  check("lookup_rootpage", iTable>0);

  memset(&fakeHash, 0x5a, sizeof(fakeHash));
  check("savepoint_begin", execSql(db, "SAVEPOINT sp;")==SQLITE_OK);
  check("set_fake_schema_hash",
      doltliteSetTableSchemaHash(db, iTable, &fakeHash)==SQLITE_OK);
  check("rollback_to", execSql(db, "ROLLBACK TO sp;")==SQLITE_OK);
  check("release_sp", execSql(db, "RELEASE sp;")==SQLITE_OK);

  check("serialize_after",
      doltliteFlushAndSerializeCatalog(db, &aAfter, &nAfter)==SQLITE_OK);
  check("catalog_equal_after_rollback",
      nBefore==nAfter && memcmp(aBefore, aAfter, nBefore)==0);

  sqlite3_free(aBefore);
  sqlite3_free(aAfter);
  sqlite3_close(db);
}

static void checkLabeled(const char *zLabel, const char *zWhat, int cond){
  char zName[192];
  snprintf(zName, sizeof(zName), "%s_%s", zLabel, zWhat);
  check(zName, cond);
}

/* A rollback that fails while persisting the restored working set still has to
** end the write transaction. It releases the graph lock on the way out, and
** sqlite3RollbackAll throws the return code away, so a connection left at
** TRANS_WRITE would let the next write short-circuit prollyBtreeBeginTrans and
** mutate the store unlocked. */
static void rollbackPersistFaultCase(int iFault, const char *zLabel){
  sqlite3 *db = 0;
  char dbpath[256];
  char zName[128];
  int rc;

  snprintf(zName, sizeof(zName), "test_rollback_persist_%d", iFault);
  make_dbpath(dbpath, sizeof(dbpath), zName);
  removeDbFiles(dbpath);
  check("rollback_persist_open", sqlite3_open(dbpath, &db)==SQLITE_OK);
  if( !db ) return;

  check("rollback_persist_seed", execSql(db,
      "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
      "INSERT INTO t VALUES(1, 'base');")==SQLITE_OK);
  check("rollback_persist_commit",
      strlen(queryScalarText(db, "SELECT dolt_commit('-A','-m','base')"))==40);

  check("rollback_persist_begin", execSql(db, "BEGIN")==SQLITE_OK);
  check("rollback_persist_write",
      execSql(db, "INSERT INTO t VALUES(2, 'pending')")==SQLITE_OK);

  gRegressionFaultCode = iFault;
  gRegressionFaultHits = 0;
  sqlite3_test_control(SQLITE_TESTCTRL_FAULT_INSTALL, regressionFaultCallback);
  rc = execSqlSilent(db, "ROLLBACK");
  sqlite3_test_control(SQLITE_TESTCTRL_FAULT_INSTALL, 0);
  gRegressionFaultCode = 0;
  (void)rc;

  checkLabeled(zLabel, "fault_injected", gRegressionFaultHits>0);

  /* The rolled-back row must not reappear, and the connection must still be
  ** usable rather than wedged in a phantom transaction. */
  checkLabeled(zLabel, "pending_row_discarded",
      strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE id=2"), "0")==0);
  checkLabeled(zLabel, "writes_work_after_failed_rollback",
      execSql(db, "INSERT INTO t VALUES(3, 'after')")==SQLITE_OK);
  checkLabeled(zLabel, "post_rollback_commit_ok",
      strlen(queryScalarText(db, "SELECT dolt_commit('-A','-m','after')"))==40);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_rollback_persist_failure_ends_txn(void){
  printf("=== Rollback Persist Failure Ends Write Txn Test ===\n\n");
  /* Only the serialize failure is reachable from a test: the persist chain
  ** below it is skipped whenever the restored working set already matches the
  ** one on disk, which it does after an ordinary rollback. Both failures leave
  ** through the same teardown. */
  rollbackPersistFaultCase(957, "rollback_serialize_fault");
}

static void run_session_string_setter_oom(void){
  sqlite3 *db = 0;
  char dbpath[256];
  int rc;

  printf("=== Session String Setter OOM Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_session_string_setter_oom");
  removeDbFiles(dbpath);

  check("session_string_open_db", sqlite3_open(dbpath, &db)==SQLITE_OK);
  if( !db ) return;

  check("session_string_create_base", execSql(db,
      "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
      "INSERT INTO t VALUES(1, 'main');")==SQLITE_OK);
  check("session_string_commit_base",
      strlen(queryScalarText(
          db, "SELECT dolt_commit('-A', '-m', 'base')"))==40);
  check("session_string_create_feature",
      execSql(db, "SELECT dolt_branch('feature')")==SQLITE_OK);
  check("session_string_checkout_feature",
      execSql(db, "SELECT dolt_checkout('feature')")==SQLITE_OK);
  check("session_string_update_feature",
      execSql(db, "UPDATE t SET v='feature' WHERE id=1")==SQLITE_OK);
  check("session_string_commit_feature",
      strlen(queryScalarText(
          db, "SELECT dolt_commit('-A', '-m', 'feature')"))==40);
  check("session_string_checkout_main",
      execSql(db, "SELECT dolt_checkout('main')")==SQLITE_OK);
  check("session_string_main_value_before_fault",
      strcmp(queryScalarText(db, "SELECT v FROM t WHERE id=1"), "main")==0);

  gRegressionFaultCode = 955;
  gRegressionFaultHits = 0;
  sqlite3_test_control(SQLITE_TESTCTRL_FAULT_INSTALL, regressionFaultCallback);
  rc = execSqlSilent(db, "SELECT dolt_checkout('feature')");
  sqlite3_test_control(SQLITE_TESTCTRL_FAULT_INSTALL, 0);
  gRegressionFaultCode = 0;
  check("session_branch_oom_injected", gRegressionFaultHits==1);
  check("session_branch_oom_reported", rc==SQLITE_NOMEM);
  check("session_branch_preserved_after_oom",
      strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("session_catalog_preserved_after_branch_oom",
      strcmp(queryScalarText(db, "SELECT v FROM t WHERE id=1"), "main")==0);
  check("session_branch_retry_succeeds",
      execSql(db, "SELECT dolt_checkout('feature')")==SQLITE_OK);
  check("session_branch_retry_loads_feature",
      strcmp(queryScalarText(db, "SELECT v FROM t WHERE id=1"), "feature")==0);

  gRegressionFaultCode = 955;
  gRegressionFaultHits = 0;
  sqlite3_test_control(SQLITE_TESTCTRL_FAULT_INSTALL, regressionFaultCallback);
  rc = execSqlSilent(db,
      "SELECT dolt_branch('-m', 'feature', 'renamed')");
  sqlite3_test_control(SQLITE_TESTCTRL_FAULT_INSTALL, 0);
  gRegressionFaultCode = 0;
  check("session_branch_rename_oom_injected", gRegressionFaultHits==1);
  check("session_branch_rename_oom_reported", rc==SQLITE_NOMEM);
  check("session_branch_rename_preserves_active_branch",
      strcmp(queryScalarText(db, "SELECT active_branch()"), "feature")==0);
  check("session_branch_rename_preserves_refs",
      strcmp(queryScalarText(db,
          "SELECT "
          "(SELECT count(*) FROM dolt_branches WHERE name='feature') || ',' || "
          "(SELECT count(*) FROM dolt_branches WHERE name='renamed')"),
          "1,0")==0);

  check("session_author_name_seed", execSql(db,
      "SELECT dolt_config('user.name', 'Original Name')")==SQLITE_OK);
  gRegressionFaultCode = 955;
  gRegressionFaultHits = 0;
  sqlite3_test_control(SQLITE_TESTCTRL_FAULT_INSTALL, regressionFaultCallback);
  rc = execSqlSilent(db,
      "SELECT dolt_config('user.name', 'Replacement Name')");
  sqlite3_test_control(SQLITE_TESTCTRL_FAULT_INSTALL, 0);
  gRegressionFaultCode = 0;
  check("session_author_name_oom_injected", gRegressionFaultHits==1);
  check("session_author_name_oom_reported", rc==SQLITE_NOMEM);
  check("session_author_name_preserved",
      strcmp(queryScalarText(
          db, "SELECT dolt_config('user.name')"), "Original Name")==0);

  check("session_author_email_seed", execSql(db,
      "SELECT dolt_config('user.email', 'original@example.com')")==SQLITE_OK);
  gRegressionFaultCode = 955;
  gRegressionFaultHits = 0;
  sqlite3_test_control(SQLITE_TESTCTRL_FAULT_INSTALL, regressionFaultCallback);
  rc = execSqlSilent(db,
      "SELECT dolt_config('user.email', 'replacement@example.com')");
  sqlite3_test_control(SQLITE_TESTCTRL_FAULT_INSTALL, 0);
  gRegressionFaultCode = 0;
  check("session_author_email_oom_injected", gRegressionFaultHits==1);
  check("session_author_email_oom_reported", rc==SQLITE_NOMEM);
  check("session_author_email_preserved",
      strcmp(queryScalarText(
          db, "SELECT dolt_config('user.email')"),
          "original@example.com")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static int denySchemaMasterRead(
  void *pCtx,
  int action,
  const char *zArg1,
  const char *zArg2,
  const char *zDb,
  const char *zTrigger
){
  (void)pCtx;
  (void)zArg2;
  (void)zDb;
  (void)zTrigger;
  if( action==SQLITE_READ && zArg1
   && strcmp(zArg1, "sqlite_master")==0 ){
    return SQLITE_DENY;
  }
  return SQLITE_OK;
}

static void run_schema_hash_error_propagation(void){
  sqlite3 *db = 0;
  char dbpath[256];
  ProllyHash fakeHash;
  const char *zResult;
  int nLogBefore;

  printf("=== Schema Hash Error Propagation Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_schema_hash_error_propagation");
  removeDbFiles(dbpath);

  check("schema_hash_open_db", sqlite3_open(dbpath, &db)==SQLITE_OK);
  if( !db ) return;

  check("schema_hash_create_base", execSql(db,
      "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);")==SQLITE_OK);
  check("schema_hash_commit_base",
      strlen(queryScalarText(
          db, "SELECT dolt_commit('-A', '-m', 'base')"))==40);

  check("schema_hash_set_authorizer",
      sqlite3_set_authorizer(db, denySchemaMasterRead, 0)==SQLITE_OK);
  check("schema_hash_prepare_error_propagated",
      doltliteUpdateSchemaHashes(db)==SQLITE_AUTH);
  check("schema_hash_clear_authorizer",
      sqlite3_set_authorizer(db, 0, 0)==SQLITE_OK);

  memset(&fakeHash, 0x5a, sizeof(fakeHash));
  check("schema_hash_missing_table_reported",
      doltliteSetTableSchemaHash(
          db, (Pgno)0x7ffffffe, &fakeHash)==SQLITE_NOTFOUND);

  check("schema_hash_create_virtual_table", execSql(db,
      "CREATE VIRTUAL TABLE docs USING fts5(body);")==SQLITE_OK);
  check("schema_hash_virtual_root_ignored",
      doltliteUpdateSchemaHashes(db)==SQLITE_OK);
  check("schema_hash_commit_virtual_table",
      strlen(queryScalarText(
          db, "SELECT dolt_commit('-A', '-m', 'virtual table')"))==40);

  check("schema_hash_alter_table", execSql(db,
      "ALTER TABLE t ADD COLUMN extra TEXT;")==SQLITE_OK);
  nLogBefore = atoi(queryScalarText(db, "SELECT count(*) FROM dolt_log"));

  gRegressionFaultCode = 954;
  gRegressionFaultHits = 0;
  sqlite3_test_control(SQLITE_TESTCTRL_FAULT_INSTALL, regressionFaultCallback);
  zResult = queryScalarText(
      db, "SELECT dolt_commit('-A', '-m', 'schema change')");
  sqlite3_test_control(SQLITE_TESTCTRL_FAULT_INSTALL, 0);
  gRegressionFaultCode = 0;

  check("schema_hash_canonicalization_failure_injected",
      gRegressionFaultHits==1);
  check("schema_hash_commit_reports_refresh_failure",
      strstr(zResult, "ERROR:")!=0);
  check("schema_hash_failed_commit_does_not_advance_log",
      atoi(queryScalarText(db, "SELECT count(*) FROM dolt_log"))==nLogBefore);
  check("schema_hash_failed_commit_keeps_ddl",
      strcmp(queryScalarText(db,
          "SELECT count(*) FROM pragma_table_info('t') "
          "WHERE name='extra'"), "1")==0);

  check("schema_hash_retry_commit_succeeds",
      strlen(queryScalarText(
          db, "SELECT dolt_commit('-A', '-m', 'schema change')"))==40);
  check("schema_hash_retry_records_schema_diff",
      strcmp(queryScalarText(db,
          "SELECT count(*) FROM dolt_schema_diff('HEAD~1','HEAD','t')"),
          "1")==0);

  sqlite3_close(db);
  db = 0;
  check("schema_hash_reopen_db", sqlite3_open(dbpath, &db)==SQLITE_OK);
  check("schema_hash_reopen_has_column",
      strcmp(queryScalarText(db,
          "SELECT count(*) FROM pragma_table_info('t') "
          "WHERE name='extra'"), "1")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static int countChunkChild(void *pCtx, const ProllyHash *pHash){
  int *pCount = (int*)pCtx;
  (void)pHash;
  (*pCount)++;
  return SQLITE_OK;
}

static void run_refs_blob_corruption(void){
  ChunkStore cs;
  ChunkStore cs2;
  ChunkStore branchless;
  ProllyHash commitHash;
  ProllyHash rootHash;
  ProllyHash workingSetHash;
  u8 *pBlob = 0;
  u8 *pMissingDefault = 0;
  u8 *pBranchless = 0;
  u8 *pTrailing = 0;
  u8 shortBlob[13];
  int nBlob = 0;
  int nMissingDefault = 0;
  int nBranchless = 0;
  int allTruncationsCorrupt = 1;
  int nChildren = 0;
  int i;
  int rc;

  printf("=== Refs Blob Corruption Test ===\n\n");
  printf("--- Test 1: truncated refs blob is rejected ---\n");

  check("open_mem_store_1",
        chunkStoreOpen(&cs, sqlite3_vfs_find(0), ":memory:", 0)==SQLITE_OK);
  check("open_mem_store_2",
        chunkStoreOpen(&cs2, sqlite3_vfs_find(0), ":memory:", 0)==SQLITE_OK);
  check("open_mem_store_branchless",
        chunkStoreOpen(&branchless, sqlite3_vfs_find(0), ":memory:", 0)==SQLITE_OK);

  check("set_default_branch",
        chunkStoreSetDefaultBranch(&cs, "main")==SQLITE_OK);
  memset(&commitHash, 0x11, sizeof(commitHash));
  memset(&workingSetHash, 0x22, sizeof(workingSetHash));
  check("add_branch_ref",
        chunkStoreAddBranch(&cs, "main", &commitHash)==SQLITE_OK);
  check("set_branch_working_set",
        chunkStoreSetBranchWorkingSet(
          &cs, "main", &workingSetHash)==SQLITE_OK);
  check("add_tag_ref",
        chunkStoreAddTagFull(&cs, "v1", &commitHash, "A", "a@b", 42,
                             "tag message")==SQLITE_OK);
  check("add_remote_ref",
        chunkStoreAddRemote(&cs, "origin", "file:///tmp/origin")==SQLITE_OK);
  check("add_tracking_ref",
        chunkStoreUpdateTracking(
          &cs, "origin", "main", &commitHash)==SQLITE_OK);
  check("add_sequence_ref",
        chunkStoreBumpSequence(&cs, "items", 99)==SQLITE_OK);

  check("serialize_refs_blob",
        chunkStoreSerializeRefsToBlob(&cs, &pBlob, &nBlob)==SQLITE_OK);
  check("refs_blob_has_tag_payload", nBlob>0);

  rc = chunkStoreLoadRefsFromBlob(&cs2, pBlob, nBlob);
  check("complete_refs_blob_loads", rc==SQLITE_OK);
  check("complete_refs_blob_preserves_sections",
        cs2.refs.nBranches==1 && cs2.refs.nTags==1
        && cs2.refs.nRemotes==1 && cs2.refs.nTracking==1
        && cs2.refs.nSequences==1);
  check("complete_refs_blob_preserves_sequence",
        refsTableGetSequence(&cs2.refs, "items")==99);
  rc = doltliteEnumerateChunkChildren(
      pBlob, nBlob, countChunkChild, &nChildren);
  check("refs_walker_uses_complete_decoder",
        rc==SQLITE_OK && nChildren==4);

  check("set_missing_default_branch",
        chunkStoreSetDefaultBranch(&cs, "missing")==SQLITE_OK);
  rc = doltliteRemoteSrvResolveRootForTest(&cs, &rootHash);
  check("missing_default_branch_root_returns_corrupt", rc==SQLITE_CORRUPT);
  check("serialize_missing_default_branch",
        chunkStoreSerializeRefsToBlob(
          &cs, &pMissingDefault, &nMissingDefault)==SQLITE_OK);
  check("restore_default_branch",
        chunkStoreSetDefaultBranch(&cs, "main")==SQLITE_OK);
  rc = chunkStoreLoadRefsFromBlob(
      &cs2, pMissingDefault, nMissingDefault);
  check("missing_default_branch_returns_corrupt", rc==SQLITE_CORRUPT);
  rc = doltliteEnumerateChunkChildren(
      pMissingDefault, nMissingDefault, 0, 0);
  check("refs_walker_rejects_missing_default_branch", rc==SQLITE_CORRUPT);

  check("serialize_branchless_refs",
        chunkStoreSerializeRefsToBlob(
          &branchless, &pBranchless, &nBranchless)==SQLITE_OK);
  rc = doltliteRemoteSrvResolveRootForTest(&branchless, &rootHash);
  check("branchless_root_is_empty",
        rc==SQLITE_OK && prollyHashIsEmpty(&rootHash));
  rc = chunkStoreLoadRefsFromBlob(&cs2, pBranchless, nBranchless);
  check("branchless_refs_blob_loads",
        rc==SQLITE_OK && refsTableBranchCount(&cs2.refs)==0);

  for(i=0; i<nBlob; i++){
    rc = chunkStoreLoadRefsFromBlob(&cs2, pBlob, i);
    if( rc!=SQLITE_CORRUPT ) allTruncationsCorrupt = 0;
  }
  check("every_truncated_blob_returns_corrupt", allTruncationsCorrupt);

  shortBlob[0] = 7;
  CS_WRITE_U32(shortBlob+1, 4);
  memcpy(shortBlob+5, "main", 4);
  CS_WRITE_U32(shortBlob+9, 0);
  rc = chunkStoreLoadRefsFromBlob(&cs2, shortBlob, sizeof(shortBlob));
  check("missing_v7_sections_return_corrupt", rc==SQLITE_CORRUPT);

  pTrailing = sqlite3_malloc(nBlob+1);
  check("grow_refs_blob_for_trailing_byte", pTrailing!=0);
  if( pTrailing ){
    memcpy(pTrailing, pBlob, nBlob);
    pTrailing[nBlob] = 0;
    rc = chunkStoreLoadRefsFromBlob(&cs2, pTrailing, nBlob+1);
    check("trailing_refs_byte_returns_corrupt", rc==SQLITE_CORRUPT);
  }

  sqlite3_free(pTrailing);
  sqlite3_free(pBranchless);
  sqlite3_free(pMissingDefault);
  sqlite3_free(pBlob);
  chunkStoreClose(&branchless);
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
  removeDbFiles(dbpath);
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
  removeDbFiles(dbpath);

  printf("--- Test 2: xFileControl failure is surfaced ---\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_refresh_filecontrol_failure");
  removeDbFiles(dbpath);
  check("open_sql_db", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_doltlite_repo", execSql(db,
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
  removeDbFiles(dbpath);
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
  removeDbFiles(dbpath);

  check("open_db", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);

  cs = doltliteGetChunkStore(db);
  check("have_chunk_store", cs!=0);
  if( cs ){
    check("put_bad_conflicts_blob",
      chunkStorePut(cs, badBlob, (int)sizeof(badBlob), &hash)==SQLITE_OK);
    doltliteSetSessionConflictsCatalog(db, &hash);
  }

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_status_error_propagation(void){
  sqlite3 *db = 0;
  char dbpath[256];
  ProllyHash badHash;
  int rc;

  printf("=== Status Error Propagation Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_status_error_propagation");
  removeDbFiles(dbpath);

  check("open_db", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_status_repo", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "INSERT INTO t VALUES(2,'b');")==SQLITE_OK);

  memset(&badHash, 0x7b, sizeof(badHash));
  doltliteSetSessionStaged(db, &badHash);
  rc = execSqlSilent(db, "SELECT count(*) FROM dolt_status;");
  check("status_surfaces_persist_error", rc!=SQLITE_OK);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_status_many_table_renames(void){
  sqlite3 *db = 0;
  char dbpath[256];
  char sql[256];
  int i;

  printf("=== Status Many Table Renames Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_status_many_table_renames");
  removeDbFiles(dbpath);

  check("open_db_for_status_many_table_renames", open_db(dbpath, &db)==SQLITE_OK);
  check("begin_status_many_table_renames_setup", execSql(db, "BEGIN;")==SQLITE_OK);
  for(i=1; i<=80; i++){
    sqlite3_snprintf(sizeof(sql), sql,
      "CREATE TABLE t%03d(id INTEGER PRIMARY KEY);"
      "INSERT INTO t%03d VALUES(1);", i, i);
    check("create_table_for_status_many_table_renames",
          execSql(db, sql)==SQLITE_OK);
  }
  check("commit_status_many_table_renames_seed", execSql(db,
    "COMMIT;"
    "SELECT dolt_commit('-A', '-m', 'seed');")==SQLITE_OK);

  for(i=1; i<=80; i++){
    sqlite3_snprintf(sizeof(sql), sql,
      "ALTER TABLE t%03d RENAME TO r%03d;", i, i);
    check("rename_table_for_status_many_table_renames",
          execSql(db, sql)==SQLITE_OK);
  }

  check("status_many_table_rename_count",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM dolt_status WHERE status='renamed';"),
          "80")==0);
  check("status_many_table_rename_sample",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM dolt_status "
          "WHERE table_name='t080 -> r080' AND status='renamed';"),
          "1")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}


static void run_refs_commit_merges_concurrent_peer_refs(void){
  ChunkStore csA, csB, csC;
  ProllyHash h1, h2, h3, hB, chunkHash;
  ProllyHash found;
  static const u8 payload[] = "refs-merge-pending-chunk";
  char dbpath[256];

  printf("=== Refs Commit Merges Concurrent Peer Refs Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_refs_commit_merges_peer");
  removeDbFiles(dbpath);
  memset(&csA, 0, sizeof(csA));
  memset(&csB, 0, sizeof(csB));
  memset(&csC, 0, sizeof(csC));

  prollyHashCompute("one", 3, &h1);
  prollyHashCompute("two", 3, &h2);
  prollyHashCompute("three", 5, &h3);
  prollyHashCompute("peer", 4, &hB);

  /* Seed the store with a branch. */
  check("rm_open_A",
        chunkStoreOpen(&csA, sqlite3_vfs_find(0), dbpath,
                      SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE|SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  check("rm_seed_branch",
        chunkStoreAddBranch(&csA, "seed", &h1)==SQLITE_OK);
  check("rm_seed_default",
        chunkStoreSetDefaultBranch(&csA, "seed")==SQLITE_OK);
  check("rm_seed_serialize", chunkStoreSerializeRefs(&csA)==SQLITE_OK);
  check("rm_seed_commit", chunkStoreCommit(&csA)==SQLITE_OK);

  /* A peer advances the refs on disk; A's view stays at the old base. */
  check("rm_open_B",
        chunkStoreOpen(&csB, sqlite3_vfs_find(0), dbpath,
                      SQLITE_OPEN_READWRITE|SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  check("rm_peer_branch",
        chunkStoreAddBranch(&csB, "peer_keep", &hB)==SQLITE_OK);
  check("rm_peer_serialize", chunkStoreSerializeRefs(&csB)==SQLITE_OK);
  check("rm_peer_commit", chunkStoreCommit(&csB)==SQLITE_OK);

  /* A stages a chunk and its own ref change over the stale view, then
  ** commits: the resolve path adopts the peer's newer refs, and the
  ** local delta must merge onto them instead of replacing them. */
  check("rm_local_branch",
        chunkStoreAddBranch(&csA, "local_new", &h3)==SQLITE_OK);
  check("rm_local_chunk",
        chunkStorePut(&csA, payload, (int)sizeof(payload),
                      &chunkHash)==SQLITE_OK);
  check("rm_local_serialize", chunkStoreSerializeRefs(&csA)==SQLITE_OK);
  check("rm_local_commit", chunkStoreCommit(&csA)==SQLITE_OK);

  /* A fresh reader sees the union: the peer's branch, the local branch,
  ** and the seed. */
  check("rm_open_C",
        chunkStoreOpen(&csC, sqlite3_vfs_find(0), dbpath,
                      SQLITE_OPEN_READWRITE|SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  check("rm_peer_survives",
        chunkStoreFindBranch(&csC, "peer_keep", &found)==SQLITE_OK
        && prollyHashCompare(&found, &hB)==0);
  check("rm_local_survives",
        chunkStoreFindBranch(&csC, "local_new", &found)==SQLITE_OK
        && prollyHashCompare(&found, &h3)==0);
  check("rm_seed_survives",
        chunkStoreFindBranch(&csC, "seed", &found)==SQLITE_OK
        && prollyHashCompare(&found, &h1)==0);

  chunkStoreClose(&csA);
  chunkStoreClose(&csB);
  chunkStoreClose(&csC);
  removeDbFiles(dbpath);
}

/* Each ref category merges in isolation, so a pairing legal in both views
** separately can still be illegal together: a peer repoints the default
** branch while this session deletes that branch. The decoder requires the
** default to name a live branch, so publishing that pair leaves a store no
** open can read -- the merge must refuse it instead. */
static void run_refs_merge_rejects_dangling_default(void){
  ChunkStore csA, csB, csC;
  ProllyHash h1, h2, chunkHash;
  static const u8 payload[] = "refs-dangling-default-chunk";
  char dbpath[256];

  printf("=== Refs Merge Rejects Dangling Default Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_refs_dangling_default");
  removeDbFiles(dbpath);
  memset(&csA, 0, sizeof(csA));
  memset(&csB, 0, sizeof(csB));
  memset(&csC, 0, sizeof(csC));

  prollyHashCompute("main-tip", 8, &h1);
  prollyHashCompute("feat-tip", 8, &h2);

  check("dd_open_A",
        chunkStoreOpen(&csA, sqlite3_vfs_find(0), dbpath,
            SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE|SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  check("dd_seed_main", chunkStoreAddBranch(&csA, "main", &h1)==SQLITE_OK);
  check("dd_seed_feat", chunkStoreAddBranch(&csA, "feat", &h2)==SQLITE_OK);
  check("dd_seed_default", chunkStoreSetDefaultBranch(&csA, "main")==SQLITE_OK);
  check("dd_seed_serialize", chunkStoreSerializeRefs(&csA)==SQLITE_OK);
  check("dd_seed_commit", chunkStoreCommit(&csA)==SQLITE_OK);

  /* Peer repoints the default onto feat. */
  check("dd_open_B",
        chunkStoreOpen(&csB, sqlite3_vfs_find(0), dbpath,
            SQLITE_OPEN_READWRITE|SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  check("dd_peer_default",
        chunkStoreSetDefaultBranch(&csB, "feat")==SQLITE_OK);
  check("dd_peer_serialize", chunkStoreSerializeRefs(&csB)==SQLITE_OK);
  check("dd_peer_commit", chunkStoreCommit(&csB)==SQLITE_OK);

  /* This session, still on the base view where the default is main,
  ** legally deletes feat and commits alongside a staged chunk. */
  check("dd_local_delete", chunkStoreDeleteBranch(&csA, "feat")==SQLITE_OK);
  check("dd_local_serialize", chunkStoreSerializeRefs(&csA)==SQLITE_OK);
  check("dd_local_put",
        chunkStorePut(&csA, payload, (int)sizeof(payload), &chunkHash)==SQLITE_OK);
  check("dd_local_commit_refused",
        chunkStoreCommit(&csA)==SQLITE_BUSY_SNAPSHOT);

  chunkStoreClose(&csA);
  chunkStoreClose(&csB);

  /* The store must still open: a published dangling default is unreadable. */
  check("dd_reopen",
        chunkStoreOpen(&csC, sqlite3_vfs_find(0), dbpath,
            SQLITE_OPEN_READWRITE|SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  check("dd_reopen_default_live",
        chunkStoreFindBranch(&csC, "feat", 0)==SQLITE_OK);
  chunkStoreClose(&csC);
  removeDbFiles(dbpath);
}

/* A commit that lost the refs merge reinstates its pre-merge view for the
** caller to unwind over. Retrying that commit without rolling back first
** must not republish the stale table wholesale -- that is exactly the peer
** clobber the merge exists to prevent. */
static void run_refs_commit_retry_after_conflict_still_fails(void){
  ChunkStore csA, csB, csC;
  ProllyHash h1, hA, hB, hKeep, found, chunkHash;
  static const u8 payload[] = "refs-retry-chunk";
  char dbpath[256];

  printf("=== Refs Commit Retry After Conflict Still Fails Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_refs_retry_conflict");
  removeDbFiles(dbpath);
  memset(&csA, 0, sizeof(csA));
  memset(&csB, 0, sizeof(csB));
  memset(&csC, 0, sizeof(csC));

  prollyHashCompute("base", 4, &h1);
  prollyHashCompute("ours", 4, &hA);
  prollyHashCompute("theirs", 6, &hB);
  prollyHashCompute("keepme", 6, &hKeep);

  check("rr_open_A",
        chunkStoreOpen(&csA, sqlite3_vfs_find(0), dbpath,
            SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE|SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  check("rr_seed_branch", chunkStoreAddBranch(&csA, "shared", &h1)==SQLITE_OK);
  check("rr_seed_default",
        chunkStoreSetDefaultBranch(&csA, "shared")==SQLITE_OK);
  check("rr_seed_serialize", chunkStoreSerializeRefs(&csA)==SQLITE_OK);
  check("rr_seed_commit", chunkStoreCommit(&csA)==SQLITE_OK);

  /* Peer moves the shared branch and adds an unrelated one. */
  check("rr_open_B",
        chunkStoreOpen(&csB, sqlite3_vfs_find(0), dbpath,
            SQLITE_OPEN_READWRITE|SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  check("rr_peer_update",
        chunkStoreUpdateBranch(&csB, "shared", &hB)==SQLITE_OK);
  check("rr_peer_add", chunkStoreAddBranch(&csB, "peer_keep", &hKeep)==SQLITE_OK);
  check("rr_peer_serialize", chunkStoreSerializeRefs(&csB)==SQLITE_OK);
  check("rr_peer_commit", chunkStoreCommit(&csB)==SQLITE_OK);

  /* This session moves the same branch from its stale base: a real
  ** conflict, correctly refused. */
  check("rr_local_update",
        chunkStoreUpdateBranch(&csA, "shared", &hA)==SQLITE_OK);
  check("rr_local_serialize", chunkStoreSerializeRefs(&csA)==SQLITE_OK);
  check("rr_local_put",
        chunkStorePut(&csA, payload, (int)sizeof(payload), &chunkHash)==SQLITE_OK);
  check("rr_first_commit_conflicts",
        chunkStoreCommit(&csA)==SQLITE_BUSY_SNAPSHOT);
  check("rr_retry_still_conflicts",
        chunkStoreCommit(&csA)==SQLITE_BUSY_SNAPSHOT);

  chunkStoreClose(&csA);
  chunkStoreClose(&csB);

  check("rr_reopen",
        chunkStoreOpen(&csC, sqlite3_vfs_find(0), dbpath,
            SQLITE_OPEN_READWRITE|SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  check("rr_peer_tip_survives",
        chunkStoreFindBranch(&csC, "shared", &found)==SQLITE_OK
        && prollyHashCompare(&found, &hB)==0);
  check("rr_peer_branch_survives",
        chunkStoreFindBranch(&csC, "peer_keep", 0)==SQLITE_OK);
  chunkStoreClose(&csC);
  removeDbFiles(dbpath);
}

/* AUTOINCREMENT counters merge as high-water marks, but only for counters
** this session actually changed. Bumping an untouched one back onto disk
** resurrects a counter a peer dropped with its table, so a recreated table
** would resume from the old mark instead of restarting. */
static void run_refs_merge_keeps_peer_sequence_drop(void){
  ChunkStore csA, csB, csC;
  ProllyHash h1, chunkHash;
  static const u8 payload[] = "refs-seq-drop-chunk";
  i64 seqVal = 0;
  char dbpath[256];

  printf("=== Refs Merge Keeps Peer Sequence Drop Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_refs_seq_drop");
  removeDbFiles(dbpath);
  memset(&csA, 0, sizeof(csA));
  memset(&csB, 0, sizeof(csB));
  memset(&csC, 0, sizeof(csC));

  prollyHashCompute("main-tip", 8, &h1);

  check("sd_open_A",
        chunkStoreOpen(&csA, sqlite3_vfs_find(0), dbpath,
            SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE|SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  check("sd_seed_branch", chunkStoreAddBranch(&csA, "main", &h1)==SQLITE_OK);
  check("sd_seed_default", chunkStoreSetDefaultBranch(&csA, "main")==SQLITE_OK);
  check("sd_seed_sequence", chunkStoreBumpSequence(&csA, "t", 5)==SQLITE_OK);
  check("sd_seed_serialize", chunkStoreSerializeRefs(&csA)==SQLITE_OK);
  check("sd_seed_commit", chunkStoreCommit(&csA)==SQLITE_OK);

  /* Peer drops the table, taking its counter with it. */
  check("sd_open_B",
        chunkStoreOpen(&csB, sqlite3_vfs_find(0), dbpath,
            SQLITE_OPEN_READWRITE|SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  chunkStoreDropSequence(&csB, "t");
  check("sd_peer_serialize", chunkStoreSerializeRefs(&csB)==SQLITE_OK);
  check("sd_peer_commit", chunkStoreCommit(&csB)==SQLITE_OK);

  /* This session commits something unrelated over its stale base. */
  check("sd_local_branch",
        chunkStoreAddBranch(&csA, "unrelated", &h1)==SQLITE_OK);
  check("sd_local_serialize", chunkStoreSerializeRefs(&csA)==SQLITE_OK);
  check("sd_local_put",
        chunkStorePut(&csA, payload, (int)sizeof(payload), &chunkHash)==SQLITE_OK);
  check("sd_local_commit", chunkStoreCommit(&csA)==SQLITE_OK);

  chunkStoreClose(&csA);
  chunkStoreClose(&csB);

  check("sd_reopen",
        chunkStoreOpen(&csC, sqlite3_vfs_find(0), dbpath,
            SQLITE_OPEN_READWRITE|SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  seqVal = chunkStoreGetSequenceValue(&csC, "t");
  check("sd_dropped_sequence_stays_dropped", seqVal==0);
  check("sd_unrelated_branch_survives",
        chunkStoreFindBranch(&csC, "unrelated", 0)==SQLITE_OK);
  chunkStoreClose(&csC);
  removeDbFiles(dbpath);
}

static void run_refs_commit_conflicting_peer_ref_fails(void){
  ChunkStore csA, csB, csC;
  ProllyHash h1, hA, hB, chunkHash;
  ProllyHash found;
  static const u8 payload[] = "refs-conflict-pending-chunk";
  char dbpath[256];

  printf("=== Refs Commit Conflicting Peer Ref Fails Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_refs_commit_conflict_peer");
  removeDbFiles(dbpath);
  memset(&csA, 0, sizeof(csA));
  memset(&csB, 0, sizeof(csB));
  memset(&csC, 0, sizeof(csC));

  prollyHashCompute("base", 4, &h1);
  prollyHashCompute("ours", 4, &hA);
  prollyHashCompute("theirs", 6, &hB);

  check("rc_open_A",
        chunkStoreOpen(&csA, sqlite3_vfs_find(0), dbpath,
                      SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE|SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  check("rc_seed_branch",
        chunkStoreAddBranch(&csA, "shared", &h1)==SQLITE_OK);
  check("rc_seed_default",
        chunkStoreSetDefaultBranch(&csA, "shared")==SQLITE_OK);
  check("rc_seed_serialize", chunkStoreSerializeRefs(&csA)==SQLITE_OK);
  check("rc_seed_commit", chunkStoreCommit(&csA)==SQLITE_OK);

  /* Both sides move the SAME branch to different tips. */
  check("rc_open_B",
        chunkStoreOpen(&csB, sqlite3_vfs_find(0), dbpath,
                      SQLITE_OPEN_READWRITE|SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  check("rc_peer_move",
        chunkStoreUpdateBranch(&csB, "shared", &hB)==SQLITE_OK);
  check("rc_peer_serialize", chunkStoreSerializeRefs(&csB)==SQLITE_OK);
  check("rc_peer_commit", chunkStoreCommit(&csB)==SQLITE_OK);

  check("rc_local_move",
        chunkStoreUpdateBranch(&csA, "shared", &hA)==SQLITE_OK);
  check("rc_local_chunk",
        chunkStorePut(&csA, payload, (int)sizeof(payload),
                      &chunkHash)==SQLITE_OK);
  check("rc_local_serialize", chunkStoreSerializeRefs(&csA)==SQLITE_OK);
  /* A real race on one ref must fail the commit, not pick a winner. */
  check("rc_commit_fails",
        chunkStoreCommit(&csA)==SQLITE_BUSY_SNAPSHOT);

  /* The peer's move is what disk keeps. */
  check("rc_open_C",
        chunkStoreOpen(&csC, sqlite3_vfs_find(0), dbpath,
                      SQLITE_OPEN_READWRITE|SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  check("rc_peer_tip_kept",
        chunkStoreFindBranch(&csC, "shared", &found)==SQLITE_OK
        && prollyHashCompare(&found, &hB)==0);

  chunkStoreClose(&csA);
  chunkStoreClose(&csB);
  chunkStoreClose(&csC);
  removeDbFiles(dbpath);
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
  removeDbFiles(remotePath);
  removeDbFiles(localPath);
  removeDbFiles(clonePath);

  check("open_remote_db", open_db(remotePath, &srcDb)==SQLITE_OK);
  check("setup_remote_repo", execSql(srcDb,
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
    check("setup_local_remote", execSql(localDb, sql)==SQLITE_OK);
  }

  check("open_chunk_store", chunkStoreOpen(&cs, sqlite3_vfs_find(0), remotePath,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  check("lock_remote_store", chunkStoreLockAndRefresh(&cs)==SQLITE_OK);
  check("put_bad_remote_refs",
        chunkStorePut(&cs, badBlob, (int)sizeof(badBlob), &badRefsHash)==SQLITE_OK);
  memcpy(&cs.refs.refsHash, &badRefsHash, sizeof(ProllyHash));
  check("commit_bad_remote_refs", chunkStoreCommit(&cs)==SQLITE_OK);
  chunkStoreUnlock(&cs);
  chunkStoreClose(&cs);

  rc = execSqlSilent(localDb, "SELECT dolt_fetch('origin');");
  check("fetch_surfaces_corrupt_refs", rc!=SQLITE_OK);

  rc = execSqlSilent(localDb, "SELECT dolt_push('origin','main');");
  check("push_surfaces_corrupt_refs", rc!=SQLITE_OK);

  check("open_clone_db", open_db(clonePath, &cloneDb)==SQLITE_OK);
  if( cloneDb ){
    char sql[1024];
    snprintf(sql, sizeof(sql), "SELECT dolt_clone('file://%s')", remotePath);
    rc = execSqlSilent(cloneDb, sql);
    check("clone_surfaces_corrupt_refs", rc!=SQLITE_OK);
  }

  sqlite3_close(cloneDb);
  sqlite3_close(localDb);
  removeDbFiles(remotePath);
  removeDbFiles(localPath);
  removeDbFiles(clonePath);
}

/* A fetch commits its chunks before any ref roots them, so a gc landing in
** that window collects the whole fetched history. Installing the tracking
** ref anyway leaves it pointing at absent chunks, which breaks gc forever
** and aborts the historical-table registration every later connection runs
** -- taking dolt_remote/dolt_hashof and friends down with it, with no
** in-band way back. The install must verify the graph and fail instead. */
static const char *gGcWindowPath = 0;
static int gGcWindowCollected = 0;

static void gcInFetchWindow(void *pArg){
  sqlite3 *gcDb = 0;
  (void)pArg;
  gGcWindowCollected = 0;
  if( !gGcWindowPath ) return;
  if( open_db(gGcWindowPath, &gcDb)==SQLITE_OK ){
    /* Platforms that cannot rewrite a file another handle holds open (and
    ** so cannot gc here) still exercise the health invariants below. */
    gGcWindowCollected = execSqlSilent(gcDb, "SELECT dolt_gc()")==SQLITE_OK;
  }
  sqlite3_close(gcDb);
}

static void run_fetch_ref_install_survives_window_gc(void){
  sqlite3 *remoteDb = 0;
  sqlite3 *localDb = 0;
  sqlite3 *afterDb = 0;
  char remotePath[256];
  char localPath[256];
  char sql[512];
  const char *res;

  printf("=== Fetch Ref Install Survives Window GC Test ===\n\n");
  make_dbpath(remotePath, sizeof(remotePath), "test_fetch_window_gc_remote");
  make_dbpath(localPath, sizeof(localPath), "test_fetch_window_gc_local");
  removeDbFiles(remotePath);
  removeDbFiles(localPath);

  check("window_gc_open_remote", open_db(remotePath, &remoteDb)==SQLITE_OK);
  check("window_gc_seed_remote", execSql(remoteDb,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'base');"
    "SELECT dolt_commit('-A','-m','base');"
    "INSERT INTO t VALUES(2,'more');"
    "SELECT dolt_commit('-A','-m','more');")==SQLITE_OK);

  check("window_gc_open_local", open_db(localPath, &localDb)==SQLITE_OK);
  check("window_gc_seed_local", execSql(localDb,
    "CREATE TABLE u(id INTEGER PRIMARY KEY);"
    "INSERT INTO u VALUES(1);"
    "SELECT dolt_commit('-A','-m','local');")==SQLITE_OK);
  snprintf(sql, sizeof(sql),
           "SELECT dolt_remote('add','origin','file://%s')", remotePath);
  check("window_gc_add_remote", execSql(localDb, sql)==SQLITE_OK);

  gGcWindowPath = localPath;
  doltliteTestSetBeforeRefInstallHook(gcInFetchWindow, 0);
  res = queryScalarText(localDb, "SELECT dolt_fetch('origin','main')");
  if( gGcWindowCollected ){
    check("window_gc_fetch_reports_failure", res && strstr(res, "0")!=res);
  }else{
    printf("  SKIP: window_gc_fetch_reports_failure (in-window gc unavailable)\n");
  }
  doltliteTestSetBeforeRefInstallHook(0, 0);
  gGcWindowPath = 0;
  check("window_gc_close_local_clean", sqlite3_close(localDb)==SQLITE_OK);
  localDb = 0;
  check("window_gc_close_remote_clean", sqlite3_close(remoteDb)==SQLITE_OK);
  remoteDb = 0;

  /* A fresh connection must still get the full function surface: the
  ** registration chain aborts on the first failing member, so a dangling
  ** tracking ref silently removes everything after it. */
  check("window_gc_reopen", open_db(localPath, &afterDb)==SQLITE_OK);
  check("window_gc_remote_fn_alive",
        strstr(queryScalarText(afterDb,
            "SELECT count(*) FROM dolt_remotes"), "1")!=0);
  check("window_gc_hashof_fn_alive",
        execSqlSilent(afterDb, "SELECT dolt_hashof('HEAD')")==SQLITE_OK);
  check("window_gc_gc_still_works",
        execSqlSilent(afterDb, "SELECT dolt_gc()")==SQLITE_OK);
  check("window_gc_local_data_intact",
        strcmp(queryScalarText(afterDb, "SELECT count(*) FROM u"), "1")==0);

  /* And the fetch must heal on retry: the chunks the gc took are re-synced,
  ** the tracking ref lands, and gc -- which walks tracking commits -- proves
  ** the graph behind it is complete. */
  check("window_gc_retry_fetch",
        execSqlSilent(afterDb, "SELECT dolt_fetch('origin','main')")==SQLITE_OK);
  check("window_gc_retry_tracking_landed",
        strcmp(queryScalarText(afterDb,
            "SELECT count(*) FROM dolt_remote_branches"), "1")==0);
  check("window_gc_retry_graph_complete",
        execSqlSilent(afterDb, "SELECT dolt_gc()")==SQLITE_OK);

  sqlite3_close(afterDb);
  removeDbFiles(remotePath);
  removeDbFiles(localPath);
}

/* Clone has the same window as fetch: it commits the synced chunks, and
** until the refs blob lands nothing roots them. A gc on the database being
** cloned into collects the lot, and installing branch refs over that leaves
** them naming absent chunks. */
static void run_clone_ref_install_survives_window_gc(void){
  sqlite3 *remoteDb = 0;
  sqlite3 *cloneDb = 0;
  sqlite3 *afterDb = 0;
  char remotePath[256];
  char clonePath[256];
  char sql[512];
  int rc;

  printf("=== Clone Ref Install Survives Window GC Test ===\n\n");
  make_dbpath(remotePath, sizeof(remotePath), "test_clone_window_gc_remote");
  make_dbpath(clonePath, sizeof(clonePath), "test_clone_window_gc_clone");
  removeDbFiles(remotePath);
  removeDbFiles(clonePath);

  check("clone_window_open_remote", open_db(remotePath, &remoteDb)==SQLITE_OK);
  check("clone_window_seed_remote", execSql(remoteDb,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'base');"
    "SELECT dolt_commit('-A','-m','base');"
    "INSERT INTO t VALUES(2,'more');"
    "SELECT dolt_commit('-A','-m','more');")==SQLITE_OK);

  check("clone_window_open_target", open_db(clonePath, &cloneDb)==SQLITE_OK);

  gGcWindowPath = clonePath;
  doltliteTestSetBeforeRefInstallHook(gcInFetchWindow, 0);
  snprintf(sql, sizeof(sql), "SELECT dolt_clone('file://%s')", remotePath);
  rc = execSqlSilent(cloneDb, sql);
  if( gGcWindowCollected ){
    check("clone_window_reports_failure", rc!=SQLITE_OK);
    check("clone_window_busy_snapshot",
          sqlite3_extended_errcode(cloneDb)==SQLITE_BUSY_SNAPSHOT);
  }else{
    printf("  SKIP: clone_window_reports_failure (in-window gc unavailable)\n");
    printf("  SKIP: clone_window_busy_snapshot (in-window gc unavailable)\n");
  }
  doltliteTestSetBeforeRefInstallHook(0, 0);
  gGcWindowPath = 0;
  check("clone_window_close_target_clean", sqlite3_close(cloneDb)==SQLITE_OK);
  cloneDb = 0;
  check("clone_window_close_remote_clean", sqlite3_close(remoteDb)==SQLITE_OK);
  remoteDb = 0;

  /* The refused clone must leave a database that still works: the function
  ** registration chain aborts on its first failing member, so a branch ref
  ** naming absent chunks takes every later function down with it. */
  check("clone_window_reopen", open_db(clonePath, &afterDb)==SQLITE_OK);
  check("clone_window_hashof_fn_alive",
        execSqlSilent(afterDb, "SELECT dolt_hashof('HEAD')")==SQLITE_OK);
  check("clone_window_gc_still_works",
        execSqlSilent(afterDb, "SELECT dolt_gc()")==SQLITE_OK);

  /* And it must heal on retry, re-syncing whatever the gc took. Where the gc
  ** could not run there was nothing to refuse, so the first clone landed and
  ** a second one has nowhere to go -- the rows are already the proof. */
  if( gGcWindowCollected ){
    snprintf(sql, sizeof(sql), "SELECT dolt_clone('file://%s')", remotePath);
    check("clone_window_retry_succeeds",
          execSqlSilent(afterDb, sql)==SQLITE_OK);
  }else{
    printf("  SKIP: clone_window_retry_succeeds (in-window gc unavailable)\n");
  }
  check("clone_window_rows",
        strcmp(queryScalarText(afterDb, "SELECT count(*) FROM t"), "2")==0);
  check("clone_window_graph_complete",
        execSqlSilent(afterDb, "SELECT dolt_gc()")==SQLITE_OK);

  sqlite3_close(afterDb);
  removeDbFiles(remotePath);
  removeDbFiles(clonePath);
}

static void run_fetch_preserves_concurrent_local_refs(void){
  sqlite3 *remoteDb = 0;
  sqlite3 *localDb1 = 0;
  sqlite3 *localDb2 = 0;
  sqlite3 *localDb3 = 0;
  char remotePath[256];
  char localPath[256];
  char sql[1024];
  char remoteHead[128];

  printf("=== Fetch Preserves Concurrent Local Refs Test ===\n\n");
  make_dbpath(remotePath, sizeof(remotePath),
              "test_fetch_preserves_concurrent_local_refs_remote");
  make_dbpath(localPath, sizeof(localPath),
              "test_fetch_preserves_concurrent_local_refs_local");
  removeDbFiles(remotePath);
  removeDbFiles(localPath);

  check("open_remote_for_concurrent_fetch",
        open_db(remotePath, &remoteDb)==SQLITE_OK);
  check("setup_remote_for_concurrent_fetch", execSql(remoteDb,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'base');"
    "SELECT dolt_commit('-A','-m','base');")==SQLITE_OK);

  check("open_local1_for_concurrent_fetch",
        open_db(localPath, &localDb1)==SQLITE_OK);
  snprintf(sql, sizeof(sql), "SELECT dolt_clone('file://%s')", remotePath);
  check("clone_for_concurrent_fetch", execSql(localDb1, sql)==SQLITE_OK);
  check("pin_local1_before_concurrent_fetch", execSql(localDb1,
    "BEGIN;"
    "SELECT count(*) FROM dolt_branches;")==SQLITE_OK);

  check("open_local2_for_concurrent_fetch",
        open_db(localPath, &localDb2)==SQLITE_OK);
  check("create_peer_branch_during_fetch",
        strcmp(queryScalarText(localDb2,
          "SELECT dolt_branch('peer_keep')"), "0")==0);

  check("advance_remote_for_concurrent_fetch", execSql(remoteDb,
    "INSERT INTO t VALUES(2,'remote');"
    "SELECT dolt_commit('-A','-m','remote');")==SQLITE_OK);
  sqlite3_snprintf(sizeof(remoteHead), remoteHead, "%s",
                   queryScalarText(remoteDb, "SELECT dolt_hashof('HEAD')"));
  check("fetch_with_stale_local_refs",
        strcmp(queryScalarText(localDb1,
          "SELECT dolt_fetch('origin','main')"), "0")==0);

  sqlite3_close(localDb2);
  sqlite3_close(localDb1);
  sqlite3_close(remoteDb);

  check("reopen_after_concurrent_fetch",
        open_db(localPath, &localDb3)==SQLITE_OK);
  check("fetch_preserves_peer_branch",
        strcmp(queryScalarText(localDb3,
          "SELECT count(*) FROM dolt_branches WHERE name='peer_keep'"), "1")==0);
  check("fetch_updates_tracking_branch",
        strcmp(queryScalarText(localDb3,
          "SELECT dolt_hashof('origin/main')"), remoteHead)==0);

  sqlite3_close(localDb3);
  removeDbFiles(remotePath);
  removeDbFiles(localPath);
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

  {
    static const u8 badConflictFrame[] = {
      'D','L','C', 2,
      0, 0
    };
    static const u8 badConstraintFrame[] = {
      'D','C','V', 2,
      0, 0
    };
    rc = doltliteEnumerateChunkChildren(badConflictFrame,
                                        (int)sizeof(badConflictFrame), 0, 0);
    check("bad_conflict_frame_version_is_corrupt", rc==SQLITE_CORRUPT);
    rc = doltliteEnumerateChunkChildren(badConstraintFrame,
                                        (int)sizeof(badConstraintFrame), 0, 0);
    check("bad_constraint_frame_version_is_corrupt", rc==SQLITE_CORRUPT);
  }

  {
    static const u8 legacyCatalogV2[] = {
      0x43,
      1, 0, 0, 0,
      0, 0, 0, 0
    };
    rc = doltliteEnumerateChunkChildren(legacyCatalogV2,
                                        (int)sizeof(legacyCatalogV2), 0, 0);
    check("legacy_catalog_v2_is_corrupt_to_chunk_walk", rc==SQLITE_CORRUPT);
  }

  {
    static const u8 legacyRefsV5[] = {
      5,
      0, 0, 0, 0,
      0, 0, 0, 0,
      0, 0, 0, 0,
      0, 0, 0, 0,
      0, 0, 0, 0
    };
    static const u8 legacyRefsV6[] = {
      6,
      0, 0, 0, 0,
      0, 0, 0, 0,
      0, 0, 0, 0,
      0, 0, 0, 0,
      0, 0, 0, 0
    };
    static const u8 currentRefsV7[] = {
      7,
      0, 0, 0, 0,
      0, 0, 0, 0,
      0, 0, 0, 0,
      0, 0, 0, 0,
      0, 0, 0, 0,
      0, 0, 0, 0
    };
    rc = doltliteEnumerateChunkChildren(legacyRefsV5,
                                        (int)sizeof(legacyRefsV5), 0, 0);
    check("legacy_refs_v5_is_corrupt_to_chunk_walk", rc==SQLITE_CORRUPT);
    rc = doltliteEnumerateChunkChildren(legacyRefsV6,
                                        (int)sizeof(legacyRefsV6), 0, 0);
    check("legacy_refs_v6_is_corrupt_to_chunk_walk", rc==SQLITE_CORRUPT);
    rc = doltliteEnumerateChunkChildren(currentRefsV7,
                                        (int)sizeof(currentRefsV7), 0, 0);
    check("current_refs_v7_is_accepted_by_chunk_walk", rc==SQLITE_OK);
  }
}

static void init_v3_catalog_blob(u8 *aCat, int nCat, u16 nName){
  memset(aCat, 0, nCat);
  aCat[0] = CATALOG_FORMAT_V3;
  aCat[1] = 1;
  aCat[5] = 2;
  aCat[9] = BTREE_INTKEY;
  if( nCat>51 ){
    aCat[50] = (u8)nName;
    aCat[51] = (u8)(nName >> 8);
  }
  if( nName>0 && nCat>52 ){
    aCat[52] = 't';
  }
}

/* Two named V3 entries, the second numbered iSecond. 48 bytes per entry:
** iTable(4) flags(1) root(20) schemaHash(20) nameLen(2) name(1). */
#define V3_TWO_ENTRY_CAT_SIZE 101
static void init_v3_two_entry_catalog(u8 *aCat, u8 iSecond){
  memset(aCat, 0, V3_TWO_ENTRY_CAT_SIZE);
  aCat[0] = CATALOG_FORMAT_V3;
  aCat[1] = 2;
  aCat[5] = 2;
  aCat[9] = BTREE_INTKEY;
  aCat[50] = 1;
  aCat[52] = 't';
  aCat[53] = iSecond;
  aCat[57] = BTREE_INTKEY;
  aCat[98] = 1;
  aCat[100] = 'u';
}

static void run_catalog_deserialize_corruption(void){
  sqlite3 *db = 0;
  ChunkStore *cs;
  ProllyHash h;
  struct TableEntry *aTables = 0;
  int nTables = 0;
  int rc;
  u8 truncatedNameCat[52];
  u8 trailingCat[54];
  u8 missingNameLenCat[50];
  u8 dupTableCat[V3_TWO_ENTRY_CAT_SIZE];
  u8 distinctTableCat[V3_TWO_ENTRY_CAT_SIZE];

  printf("=== Catalog Deserialize Corruption Test ===\n\n");

  check("open_memory_db_for_catalog_corruption", open_db(":memory:", &db)==SQLITE_OK);
  cs = doltliteGetChunkStore(db);
  check("get_chunk_store_for_catalog_corruption", cs!=0);

  init_v3_catalog_blob(truncatedNameCat, (int)sizeof(truncatedNameCat), 1);
  check("put_truncated_name_catalog",
        chunkStorePut(cs, truncatedNameCat, (int)sizeof(truncatedNameCat), &h)==SQLITE_OK);
  rc = doltliteLoadCatalog(db, &h, &aTables, &nTables, 0);
  check("truncated_name_catalog_rejected", rc==SQLITE_CORRUPT);
  doltliteFreeCatalog(aTables, nTables);
  aTables = 0; nTables = 0;

  init_v3_catalog_blob(trailingCat, (int)sizeof(trailingCat), 1);
  trailingCat[53] = 0xAA;
  check("put_trailing_catalog",
        chunkStorePut(cs, trailingCat, (int)sizeof(trailingCat), &h)==SQLITE_OK);
  rc = doltliteLoadCatalog(db, &h, &aTables, &nTables, 0);
  check("trailing_catalog_rejected", rc==SQLITE_CORRUPT);
  doltliteFreeCatalog(aTables, nTables);
  aTables = 0; nTables = 0;

  init_v3_catalog_blob(missingNameLenCat, (int)sizeof(missingNameLenCat), 0);
  check("put_missing_name_len_catalog",
        chunkStorePut(cs, missingNameLenCat, (int)sizeof(missingNameLenCat), &h)==SQLITE_OK);
  rc = doltliteLoadCatalog(db, &h, &aTables, &nTables, 0);
  check("missing_name_len_catalog_rejected", rc==SQLITE_CORRUPT);
  doltliteFreeCatalog(aTables, nTables);
  aTables = 0; nTables = 0;

  /* Two entries claiming the same table number, which is legitimate: views,
  ** triggers and virtual tables all serialize as number zero. catAdd hands the
  ** second entry the one the first already named, so the second name used to
  ** overwrite a live pointer and strand it -- unbounded, since the blob picks
  ** how many times to repeat. Reloading must not grow sqlite3's live total. */
  init_v3_two_entry_catalog(dupTableCat, 2);
  check("put_duplicate_itable_catalog",
        chunkStorePut(cs, dupTableCat, (int)sizeof(dupTableCat), &h)==SQLITE_OK);
  rc = doltliteLoadCatalog(db, &h, &aTables, &nTables, 0);
  check("duplicate_itable_catalog_loads", rc==SQLITE_OK);
  check("duplicate_itable_collapses_to_one_entry", nTables==1);
  doltliteFreeCatalog(aTables, nTables);
  aTables = 0; nTables = 0;
  {
    sqlite3_int64 beforeBytes;
    int iRepeat;
    beforeBytes = sqlite3_memory_used();
    for(iRepeat=0; iRepeat<64; iRepeat++){
      rc = doltliteLoadCatalog(db, &h, &aTables, &nTables, 0);
      if( rc!=SQLITE_OK ) break;
      doltliteFreeCatalog(aTables, nTables);
      aTables = 0; nTables = 0;
    }
    check("duplicate_itable_reload_ok", rc==SQLITE_OK);
    check("duplicate_itable_catalog_does_not_leak_names",
          sqlite3_memory_used()==beforeBytes);
  }

  init_v3_two_entry_catalog(distinctTableCat, 3);
  check("put_distinct_itable_catalog",
        chunkStorePut(cs, distinctTableCat, (int)sizeof(distinctTableCat), &h)==SQLITE_OK);
  rc = doltliteLoadCatalog(db, &h, &aTables, &nTables, 0);
  check("distinct_itable_catalog_accepted", rc==SQLITE_OK);
  check("distinct_itable_catalog_has_both", nTables==2);
  doltliteFreeCatalog(aTables, nTables);

  sqlite3_close(db);
}

static void fill_v5_one_entry_catalog(u8 *buf, int nBuf, Pgno iTable){
  u8 *q = buf;
  memset(buf, 0, nBuf);
  *q++ = CATALOG_FORMAT_V5;
  q[0] = 1; q += 4;
  q += 8;
  q[0] = (u8)iTable; q[1] = (u8)(iTable>>8);
  q[2] = (u8)(iTable>>16); q[3] = (u8)(iTable>>24);
}

static void run_schema_loader_missing_master(void){
  sqlite3 *db = 0;
  ChunkStore *cs;
  ProllyCache *pCache;
  ProllyHash emptyHash;
  ProllyHash catHash;
  SchemaEntry *aSchema = 0;
  int nSchema = 0;
  SchemaEntry one;
  int found = 0;
  int rc;
  u8 emptyBlob[CAT_HEADER_SIZE_V5];
  u8 noMaster[CAT_HEADER_SIZE_V5 + CAT_ENTRY_FIXED_SIZE_V4];
  u8 emptyMaster[CAT_HEADER_SIZE_V5 + CAT_ENTRY_FIXED_SIZE_V4];

  printf("=== Schema Loader Missing Master Test ===\n\n");
  check("open_memory_db_for_schema_loader", open_db(":memory:", &db)==SQLITE_OK);
  cs = doltliteGetChunkStore(db);
  pCache = doltliteGetCache(db);
  check("schema_loader_chunk_store", cs!=0 && pCache!=0);

  memset(&emptyHash, 0, sizeof(emptyHash));
  rc = loadSchemaFromCatalog(db, cs, pCache, &emptyHash, &aSchema, &nSchema);
  check("empty_catalog_hash_is_ok", rc==SQLITE_OK && aSchema==0 && nSchema==0);
  rc = loadSchemaEntryFromCatalog(db, cs, pCache, &emptyHash, "t", &one, &found);
  check("empty_catalog_hash_entry_is_ok", rc==SQLITE_OK && found==0);

  check("setup_real_catalog",
        execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY);"
                    "SELECT dolt_commit('-A','-m','init');")==SQLITE_OK);
  check("real_catalog_hash",
        doltliteGetHeadCatalogHash(db, &catHash)==SQLITE_OK
        && !prollyHashIsEmpty(&catHash));
  rc = loadSchemaFromCatalog(db, cs, pCache, &catHash, &aSchema, &nSchema);
  check("real_catalog_loads_schema", rc==SQLITE_OK && nSchema>0);
  freeSchemaEntries(aSchema, nSchema);
  aSchema = 0; nSchema = 0;
  rc = loadSchemaEntryFromCatalog(db, cs, pCache, &catHash, "t", &one, &found);
  check("real_catalog_finds_table", rc==SQLITE_OK && found==1);
  clearSchemaEntry(&one);

  memset(emptyBlob, 0, sizeof(emptyBlob));
  emptyBlob[0] = CATALOG_FORMAT_V5;
  check("put_zero_entry_catalog",
        chunkStorePut(cs, emptyBlob, (int)sizeof(emptyBlob), &catHash)==SQLITE_OK);
  rc = loadSchemaFromCatalog(db, cs, pCache, &catHash, &aSchema, &nSchema);
  check("zero_entry_catalog_is_empty_schema",
        rc==SQLITE_OK && aSchema==0 && nSchema==0);
  rc = loadSchemaEntryFromCatalog(db, cs, pCache, &catHash, "t", &one, &found);
  check("zero_entry_catalog_lookup_is_ok", rc==SQLITE_OK && found==0);

  fill_v5_one_entry_catalog(noMaster, (int)sizeof(noMaster), 2);
  check("put_catalog_without_master",
        chunkStorePut(cs, noMaster, (int)sizeof(noMaster), &catHash)==SQLITE_OK);
  rc = loadSchemaFromCatalog(db, cs, pCache, &catHash, &aSchema, &nSchema);
  check("missing_master_entry_is_corrupt", rc==SQLITE_CORRUPT);
  check("missing_master_leaves_no_entries", aSchema==0 && nSchema==0);
  rc = loadSchemaEntryFromCatalog(db, cs, pCache, &catHash, "t", &one, &found);
  check("missing_master_entry_lookup_is_corrupt", rc==SQLITE_CORRUPT);

  fill_v5_one_entry_catalog(emptyMaster, (int)sizeof(emptyMaster), 1);
  check("put_catalog_with_empty_master_root",
        chunkStorePut(cs, emptyMaster, (int)sizeof(emptyMaster), &catHash)==SQLITE_OK);
  rc = loadSchemaFromCatalog(db, cs, pCache, &catHash, &aSchema, &nSchema);
  check("empty_master_root_is_empty_schema",
        rc==SQLITE_OK && aSchema==0 && nSchema==0);
  rc = loadSchemaEntryFromCatalog(db, cs, pCache, &catHash, "t", &one, &found);
  check("empty_master_root_lookup_is_ok", rc==SQLITE_OK && found==0);

  sqlite3_close(db);
}

/* OP_RowCell / TransferRow for blob-key indexes and WITHOUT ROWID tables
** must feed Insert a SQLite record. Passing the tree sort key makes
** sortKeyFromIntRecordLocal treat 0x15/0x35 tags as a record header. */
static int xferBlobkeyPayloadEquals(
  BtCursor *pCur, const u8 *pWant, int nWant
){
  u8 buf[64];
  u32 n;
  int rc;
  int res;

  rc = sqlite3BtreeFirst(pCur, &res);
  if( rc!=SQLITE_OK || res!=0 ) return 0;
  n = sqlite3BtreePayloadSize(pCur);
  if( n!=(u32)nWant || n>sizeof(buf) ) return 0;
  rc = sqlite3BtreePayload(pCur, 0, n, buf);
  return rc==SQLITE_OK && memcmp(buf, pWant, nWant)==0;
}

static void run_transfer_row_blobkey_uses_record(void){
  sqlite3 *db = 0;
  Btree *pBt;
  BtCursor *pSrc = 0;
  BtCursor *pDest = 0;
  Pgno iSrc = 0;
  Pgno iDest = 0;
  BtreePayload payload;
  char dbpath[256];
  int nCur;
  int rc;
  int res;
  /* One TEXT field "ab": header 2, serial 17, payload. */
  static const u8 rec[] = { 0x02, 0x11, 'a', 'b' };

  printf("=== TransferRow Blob-Key Uses Record Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_transfer_row_blobkey");
  removeDbFiles(dbpath);
  check("xfer_open", open_db(dbpath, &db)==SQLITE_OK);
  check("xfer_write_txn",
        execSql(db, "BEGIN; CREATE TABLE _lock(id INTEGER PRIMARY KEY);")
        ==SQLITE_OK);

  pBt = db->aDb[0].pBt;
  check("xfer_btree", pBt!=0 && sqlite3BtreeIsDoltliteFormat(pBt));
  sqlite3BtreeEnter(pBt);
  nCur = sqlite3BtreeCursorSize();
  pSrc = sqlite3_malloc(nCur);
  pDest = sqlite3_malloc(nCur);
  check("xfer_cursors_alloc", pSrc!=0 && pDest!=0);
  if( !pSrc || !pDest ){
    sqlite3_free(pSrc);
    sqlite3_free(pDest);
    sqlite3BtreeLeave(pBt);
    sqlite3_close(db);
    removeDbFiles(dbpath);
    return;
  }
  sqlite3BtreeCursorZero(pSrc);
  sqlite3BtreeCursorZero(pDest);

  rc = sqlite3BtreeCreateTable(pBt, &iSrc, BTREE_BLOBKEY);
  check("xfer_create_src", rc==SQLITE_OK && iSrc>0);
  rc = sqlite3BtreeCreateTable(pBt, &iDest, BTREE_BLOBKEY);
  check("xfer_create_dest", rc==SQLITE_OK && iDest>0 && iDest!=iSrc);

  rc = sqlite3BtreeCursor(pBt, iSrc, BTREE_WRCSR, 0, pSrc);
  check("xfer_open_src", rc==SQLITE_OK);
  rc = sqlite3BtreeCursor(pBt, iDest, BTREE_WRCSR, 0, pDest);
  check("xfer_open_dest", rc==SQLITE_OK);

  memset(&payload, 0, sizeof(payload));
  payload.pKey = rec;
  payload.nKey = (int)sizeof(rec);
  rc = sqlite3BtreeInsert(pSrc, &payload, 0, 0);
  check("xfer_insert_src", rc==SQLITE_OK);
  rc = sqlite3BtreeFirst(pSrc, &res);
  check("xfer_src_landed", rc==SQLITE_OK && res==0);

  rc = sqlite3BtreeTransferRow(pDest, pSrc, 0);
  check("xfer_mutmap_ok", rc==SQLITE_OK);
  check("xfer_mutmap_record",
        xferBlobkeyPayloadEquals(pDest, rec, (int)sizeof(rec)));

  sqlite3BtreeCloseCursor(pSrc);
  sqlite3BtreeCloseCursor(pDest);
  sqlite3BtreeLeave(pBt);
  check("xfer_commit_src", execSql(db, "COMMIT;")==SQLITE_OK);
  check("xfer_new_write_txn",
        execSql(db, "BEGIN; INSERT INTO _lock VALUES(1);")==SQLITE_OK);

  sqlite3BtreeEnter(pBt);
  sqlite3BtreeCursorZero(pSrc);
  sqlite3BtreeCursorZero(pDest);
  rc = sqlite3BtreeCreateTable(pBt, &iDest, BTREE_BLOBKEY);
  check("xfer_create_tree_dest", rc==SQLITE_OK && iDest>0);
  rc = sqlite3BtreeCursor(pBt, iSrc, BTREE_WRCSR, 0, pSrc);
  check("xfer_reopen_src", rc==SQLITE_OK);
  rc = sqlite3BtreeCursor(pBt, iDest, BTREE_WRCSR, 0, pDest);
  check("xfer_open_tree_dest", rc==SQLITE_OK);
  rc = sqlite3BtreeFirst(pSrc, &res);
  check("xfer_tree_src_landed", rc==SQLITE_OK && res==0);

  rc = sqlite3BtreeTransferRow(pDest, pSrc, 0);
  check("xfer_tree_ok", rc==SQLITE_OK);
  check("xfer_tree_record",
        xferBlobkeyPayloadEquals(pDest, rec, (int)sizeof(rec)));

  sqlite3BtreeCloseCursor(pSrc);
  sqlite3BtreeCloseCursor(pDest);
  sqlite3BtreeLeave(pBt);
  sqlite3_free(pSrc);
  sqlite3_free(pDest);
  execSqlSilent(db, "ROLLBACK;");
  sqlite3_close(db);
  removeDbFiles(dbpath);

  /* User-facing copy path: secondary index + WITHOUT ROWID survive
  ** INSERT…SELECT and VACUUM (prolly VACUUM is GC; integrity still
  ** proves the indexes match the tables). */
  check("xfer_reopen_sql", open_db(dbpath, &db)==SQLITE_OK);
  check("xfer_sql_setup", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, n REAL);"
    "CREATE UNIQUE INDEX idx ON t(v);"
    "INSERT INTO t VALUES(1,'alpha',1.5),(2,'beta',2.25),(3,'gamma',3.0);"
    "CREATE TABLE wr(k TEXT PRIMARY KEY, v INT, n REAL) WITHOUT ROWID;"
    "INSERT INTO wr VALUES('a',10,1.0),('b',20,2.0),('c',30,3.0);"
    "CREATE TABLE t2(id INTEGER PRIMARY KEY, v TEXT, n REAL);"
    "CREATE UNIQUE INDEX idx2 ON t2(v);"
    "INSERT INTO t2 SELECT * FROM t;"
    "CREATE TABLE wr2(k TEXT PRIMARY KEY, v INT, n REAL) WITHOUT ROWID;"
    "INSERT INTO wr2 SELECT * FROM wr;")==SQLITE_OK);
  check("xfer_sql_integrity",
        strcmp(queryScalarText(db, "PRAGMA integrity_check;"), "ok")==0);
  check("xfer_sql_index_lookup",
        strcmp(queryScalarText(db, "SELECT id FROM t2 WHERE v='beta'"), "2")==0);
  check("xfer_sql_unique",
        (execSqlSilent(db, "INSERT INTO t2 VALUES(4,'alpha',9);")&0xff)
        ==SQLITE_CONSTRAINT);
  check("xfer_sql_wr_lookup",
        strcmp(queryScalarText(db, "SELECT v FROM wr2 WHERE k='b'"), "20")==0);
  check("xfer_sql_vacuum", execSql(db, "VACUUM;")==SQLITE_OK);
  check("xfer_sql_integrity_after_vacuum",
        strcmp(queryScalarText(db, "PRAGMA integrity_check;"), "ok")==0);
  check("xfer_sql_index_after_vacuum",
        strcmp(queryScalarText(db, "SELECT id FROM t2 WHERE v='gamma'"), "3")==0);
  check("xfer_sql_wr_after_vacuum",
        strcmp(queryScalarText(db, "SELECT v FROM wr2 WHERE k='c'"), "30")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
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
  removeDbFiles(dbpath);

  check("open_db", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);

  doltliteGetSessionHead(db, &headHash);
  memset(&badHash, 0x5a, sizeof(badHash));
  rc = doltliteFindAncestor(db, &badHash, &headHash, &ancestor);
  check("missing_start_commit_returns_notfound", rc==SQLITE_NOTFOUND);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_ancestor_criss_cross_single_walk(void){
  enum { CRISS_CROSS_DEPTH = 32 };
  sqlite3 *db = 0;
  char dbpath[256];
  char zMessage[64];
  DoltliteCommit rootCommit;
  ProllyHash rootHash;
  ProllyHash left;
  ProllyHash right;
  ProllyHash previousLeft;
  ProllyHash previousRight;
  ProllyHash ancestor;
  int rc = SQLITE_OK;
  int i;

  printf("=== Ancestor Criss-Cross Single Walk Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_ancestor_criss_cross_single_walk");
  removeDbFiles(dbpath);

  check("open_criss_cross_db", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_criss_cross_repo", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'root');"
    "SELECT dolt_commit('-A', '-m', 'root');")==SQLITE_OK);
  doltliteGetSessionHead(db, &rootHash);
  memset(&rootCommit, 0, sizeof(rootCommit));
  check("load_criss_cross_root",
        doltliteLoadCommit(db, &rootHash, &rootCommit)==SQLITE_OK);

  rc = doltliteCreateAndStoreCommitWithTime(db, &rootHash,
      &rootCommit.catalogHash, "left-0", "test", "test@example.com", 0, 0,
      1000, &left);
  if( rc==SQLITE_OK ){
    rc = doltliteCreateAndStoreCommitWithTime(db, &rootHash,
        &rootCommit.catalogHash, "right-0", "test", "test@example.com", 0, 0,
        1001, &right);
  }
  for(i=1; rc==SQLITE_OK && i<=CRISS_CROSS_DEPTH; i++){
    ProllyHash nextLeft;
    ProllyHash nextRight;
    previousLeft = left;
    previousRight = right;
    snprintf(zMessage, sizeof(zMessage), "left-%d", i);
    rc = doltliteCreateAndStoreCommitWithTime(db, &previousLeft,
        &rootCommit.catalogHash, zMessage, "test", "test@example.com",
        &previousRight, 1, 1000 + i*2, &nextLeft);
    if( rc==SQLITE_OK ){
      snprintf(zMessage, sizeof(zMessage), "right-%d", i);
      rc = doltliteCreateAndStoreCommitWithTime(db, &previousRight,
          &rootCommit.catalogHash, zMessage, "test", "test@example.com",
          &previousLeft, 1, 1001 + i*2, &nextRight);
    }
    left = nextLeft;
    right = nextRight;
  }
  check("build_criss_cross_history", rc==SQLITE_OK);

  gRegressionFaultCode = 959;
  gRegressionFaultHits = 0;
  sqlite3_test_control(SQLITE_TESTCTRL_FAULT_INSTALL, regressionFaultCallback);
  rc = doltliteFindAncestor(db, &left, &right, &ancestor);
  sqlite3_test_control(SQLITE_TESTCTRL_FAULT_INSTALL, 0);
  gRegressionFaultCode = 0;
  check("find_criss_cross_ancestor", rc==SQLITE_OK);
  check("criss_cross_ancestor_is_maximal",
        prollyHashCompare(&ancestor, &previousLeft)==0 ||
        prollyHashCompare(&ancestor, &previousRight)==0);
  check("criss_cross_ancestor_loads_each_commit_once",
        gRegressionFaultHits<=2 + 2*(CRISS_CROSS_DEPTH + 1));

  doltliteCommitClear(&rootCommit);
  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_pull_persist_failure(void){
  sqlite3 *localDb = 0;
  sqlite3 *remoteDb = 0;
  sqlite3 *remoteClientDb = 0;
  char localPath[256];
  char remotePath[256];
  char remoteClientPath[256];
  char sql[1024];
  const char *res;
  char zHeadBefore[128];

  printf("=== Pull Persist Failure Test ===\n\n");
  make_dbpath(localPath, sizeof(localPath), "test_pull_persist_failure_local");
  make_dbpath(remotePath, sizeof(remotePath), "test_pull_persist_failure_remote");
  make_dbpath(remoteClientPath, sizeof(remoteClientPath), "test_pull_persist_failure_remote_client");
  removeDbFiles(localPath);
  removeDbFiles(remotePath);
  removeDbFiles(remoteClientPath);

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
    "SELECT dolt_push('origin','main','--force');",
    remotePath);
  check("setup_local_and_push", execSql(localDb, sql)==SQLITE_OK);

  check("open_remote_client_db", open_db(remoteClientPath, &remoteClientDb)==SQLITE_OK);
  snprintf(sql, sizeof(sql), "SELECT dolt_clone('file://%s')", remotePath);
  check("clone_remote_into_remote_client", execSql(remoteClientDb, sql)==SQLITE_OK);
  check("advance_remote", execSql(remoteClientDb,
    "INSERT INTO t VALUES(2,'b');"
    "SELECT dolt_add('-A');"
    "SELECT dolt_commit('-m', 'remote update');"
    "SELECT dolt_push('origin','main');")==SQLITE_OK);
  sqlite3_snprintf(sizeof(zHeadBefore), zHeadBefore, "%s",
                   queryScalarText(localDb, "SELECT commit_hash FROM dolt_log LIMIT 1"));

  gFailHits = 0;
  gFailSyncOnce = 2;
  res = queryScalarText(localDb, "SELECT dolt_pull('origin','main')");
  check("pull_failure_was_injected", gFailHits>0);
  check("pull_returns_error_on_persist_failure", strstr(res, "ERROR:")!=0);
  check("pull_branch_stays_main",
    strcmp(queryScalarText(localDb, "SELECT active_branch()"), "main")==0);
  check("pull_head_data_restored",
    strcmp(queryScalarText(localDb, "SELECT count(*) FROM t"), "1")==0);
  check("pull_head_hash_restored",
    strcmp(queryScalarText(localDb, "SELECT commit_hash FROM dolt_log LIMIT 1"), zHeadBefore)==0);
  check("pull_remote_kept",
    strcmp(queryScalarText(localDb, "SELECT count(*) FROM dolt_remotes"), "1")==0);

  sqlite3_close(remoteClientDb);
  remoteClientDb = 0;
  sqlite3_close(remoteDb);
  remoteDb = 0;
  sqlite3_close(localDb);
  localDb = 0;

  check("reopen_local_after_pull_failure", open_db(localPath, &localDb)==SQLITE_OK);
  check("pull_failure_persists_branch_after_reopen",
    strcmp(queryScalarText(localDb, "SELECT active_branch()"), "main")==0);
  check("pull_failure_persists_rows_after_reopen",
    strcmp(queryScalarText(localDb, "SELECT count(*) FROM t"), "1")==0);
  check("pull_failure_persists_head_after_reopen",
    strcmp(queryScalarText(localDb, "SELECT commit_hash FROM dolt_log LIMIT 1"), zHeadBefore)==0);
  check("pull_failure_persists_remote_after_reopen",
    strcmp(queryScalarText(localDb, "SELECT count(*) FROM dolt_remotes"), "1")==0);

  sqlite3_close(localDb);
  removeDbFiles(localPath);
  removeDbFiles(remotePath);
  removeDbFiles(remoteClientPath);
}

/* Pulling a branch that does not exist locally used to add it to the
** in-memory refs outside the graph lock and never serialize it: the
** branch answered queries for the rest of the session and silently
** vanished on reopen (a later unrelated ref write could also clobber or
** accidentally persist it). The create must go through the atomic
** ref-mutation helper like every other ref write. */
static void run_pull_new_branch_persists(void){
  sqlite3 *localDb = 0;
  sqlite3 *remoteDb = 0;
  sqlite3 *remoteClientDb = 0;
  char localPath[256];
  char remotePath[256];
  char remoteClientPath[256];
  char sql[1024];
  const char *res;

  printf("=== Pull New Branch Persists Test ===\n\n");
  make_dbpath(localPath, sizeof(localPath), "test_pull_new_branch_local");
  make_dbpath(remotePath, sizeof(remotePath), "test_pull_new_branch_remote");
  make_dbpath(remoteClientPath, sizeof(remoteClientPath),
              "test_pull_new_branch_client");
  removeDbFiles(localPath);
  removeDbFiles(remotePath);
  removeDbFiles(remoteClientPath);

  check("open_local_db", open_db(localPath, &localDb)==SQLITE_OK);
  check("open_remote_db", open_db(remotePath, &remoteDb)==SQLITE_OK);

  snprintf(sql, sizeof(sql),
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "SELECT dolt_remote('add','origin','file://%s');"
    "SELECT dolt_push('origin','main','--force');",
    remotePath);
  check("setup_local_and_push", execSql(localDb, sql)==SQLITE_OK);

  check("open_remote_client_db",
        open_db(remoteClientPath, &remoteClientDb)==SQLITE_OK);
  snprintf(sql, sizeof(sql), "SELECT dolt_clone('file://%s')", remotePath);
  check("clone_remote_into_client", execSql(remoteClientDb, sql)==SQLITE_OK);
  check("client_creates_feature", execSql(remoteClientDb,
    "SELECT dolt_branch('feature');"
    "SELECT dolt_checkout('feature');"
    "INSERT INTO t VALUES(2,'b');"
    "SELECT dolt_add('-A');"
    "SELECT dolt_commit('-m','feature work');"
    "SELECT dolt_push('origin','feature');")==SQLITE_OK);

  res = queryScalarText(localDb, "SELECT dolt_pull('origin','feature')");
  check("pull_new_branch_succeeds", strstr(res, "ERROR:")==0);
  check("pulled_branch_visible_in_session",
    strcmp(queryScalarText(localDb,
      "SELECT count(*) FROM dolt_branches WHERE name='feature'"), "1")==0);

  sqlite3_close(localDb);
  localDb = 0;
  check("reopen_local_after_pull", open_db(localPath, &localDb)==SQLITE_OK);
  check("pulled_branch_persisted_across_reopen",
    strcmp(queryScalarText(localDb,
      "SELECT count(*) FROM dolt_branches WHERE name='feature'"), "1")==0);
  check("pulled_branch_checkout_works", execSql(localDb,
    "SELECT dolt_checkout('feature')")==SQLITE_OK);
  check("pulled_branch_rows_present",
    strcmp(queryScalarText(localDb, "SELECT count(*) FROM t"), "2")==0);

  sqlite3_close(remoteClientDb);
  sqlite3_close(remoteDb);
  sqlite3_close(localDb);
  removeDbFiles(localPath);
  removeDbFiles(remotePath);
  removeDbFiles(remoteClientPath);
}

static void run_pull_dirty_working_set_fails(void){
  sqlite3 *localDb = 0;
  sqlite3 *remoteDb = 0;
  sqlite3 *remoteClientDb = 0;
  char localPath[256];
  char remotePath[256];
  char remoteClientPath[256];
  char sql[1024];
  const char *res;
  char zHeadBefore[128];

  printf("=== Pull Dirty Working Set Fails Test ===\n\n");
  make_dbpath(localPath, sizeof(localPath), "test_pull_dirty_working_set_local");
  make_dbpath(remotePath, sizeof(remotePath), "test_pull_dirty_working_set_remote");
  make_dbpath(remoteClientPath, sizeof(remoteClientPath), "test_pull_dirty_working_set_remote_client");
  removeDbFiles(localPath);
  removeDbFiles(remotePath);
  removeDbFiles(remoteClientPath);

  check("open_local_db_for_pull_dirty", open_db(localPath, &localDb)==SQLITE_OK);
  check("open_remote_db_for_pull_dirty", open_db(remotePath, &remoteDb)==SQLITE_OK);

  snprintf(sql, sizeof(sql),
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "SELECT dolt_remote('add','origin','file://%s');"
    "SELECT dolt_push('origin','main','--force');",
    remotePath);
  check("setup_local_and_push_for_pull_dirty", execSql(localDb, sql)==SQLITE_OK);

  check("open_remote_client_db_for_pull_dirty", open_db(remoteClientPath, &remoteClientDb)==SQLITE_OK);
  snprintf(sql, sizeof(sql), "SELECT dolt_clone('file://%s')", remotePath);
  check("clone_remote_into_remote_client_for_pull_dirty", execSql(remoteClientDb, sql)==SQLITE_OK);
  check("advance_remote_for_pull_dirty", execSql(remoteClientDb,
    "INSERT INTO t VALUES(2,'b');"
    "SELECT dolt_add('-A');"
    "SELECT dolt_commit('-m', 'remote update');"
    "SELECT dolt_push('origin','main');")==SQLITE_OK);

  check("make_local_working_dirty", execSql(localDb,
    "UPDATE t SET v='local dirty' WHERE id=1;")==SQLITE_OK);
  sqlite3_snprintf(sizeof(zHeadBefore), zHeadBefore, "%s",
                   queryScalarText(localDb, "SELECT commit_hash FROM dolt_log LIMIT 1"));

  res = queryScalarText(localDb, "SELECT dolt_pull('origin','main')");
  check("pull_dirty_working_returns_error", strstr(res, "ERROR:")!=0);
  check("pull_dirty_working_reports_uncommitted", strstr(res, "uncommitted changes")!=0);
  check("pull_dirty_working_branch_stays_main",
    strcmp(queryScalarText(localDb, "SELECT active_branch()"), "main")==0);
  check("pull_dirty_working_keeps_local_rows",
    strcmp(queryScalarText(localDb, "SELECT v FROM t WHERE id=1"), "local dirty")==0);
  check("pull_dirty_working_keeps_head",
    strcmp(queryScalarText(localDb, "SELECT commit_hash FROM dolt_log LIMIT 1"), zHeadBefore)==0);

  sqlite3_close(remoteClientDb);
  remoteClientDb = 0;
  sqlite3_close(remoteDb);
  remoteDb = 0;
  sqlite3_close(localDb);
  localDb = 0;

  check("reopen_local_after_pull_dirty_working", open_db(localPath, &localDb)==SQLITE_OK);
  check("pull_dirty_working_persists_local_rows_after_reopen",
    strcmp(queryScalarText(localDb, "SELECT v FROM t WHERE id=1"), "local dirty")==0);
  check("pull_dirty_working_persists_head_after_reopen",
    strcmp(queryScalarText(localDb, "SELECT commit_hash FROM dolt_log LIMIT 1"), zHeadBefore)==0);

  sqlite3_close(localDb);
  removeDbFiles(localPath);
  removeDbFiles(remotePath);
  removeDbFiles(remoteClientPath);
}

static void run_pull_staged_changes_fails(void){
  sqlite3 *localDb = 0;
  sqlite3 *remoteDb = 0;
  sqlite3 *remoteClientDb = 0;
  char localPath[256];
  char remotePath[256];
  char remoteClientPath[256];
  char sql[1024];
  const char *res;
  char zHeadBefore[128];

  printf("=== Pull Staged Changes Fails Test ===\n\n");
  make_dbpath(localPath, sizeof(localPath), "test_pull_staged_changes_local");
  make_dbpath(remotePath, sizeof(remotePath), "test_pull_staged_changes_remote");
  make_dbpath(remoteClientPath, sizeof(remoteClientPath), "test_pull_staged_changes_remote_client");
  removeDbFiles(localPath);
  removeDbFiles(remotePath);
  removeDbFiles(remoteClientPath);

  check("open_local_db_for_pull_staged", open_db(localPath, &localDb)==SQLITE_OK);
  check("open_remote_db_for_pull_staged", open_db(remotePath, &remoteDb)==SQLITE_OK);

  snprintf(sql, sizeof(sql),
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "SELECT dolt_remote('add','origin','file://%s');"
    "SELECT dolt_push('origin','main','--force');",
    remotePath);
  check("setup_local_and_push_for_pull_staged", execSql(localDb, sql)==SQLITE_OK);

  check("open_remote_client_db_for_pull_staged", open_db(remoteClientPath, &remoteClientDb)==SQLITE_OK);
  snprintf(sql, sizeof(sql), "SELECT dolt_clone('file://%s')", remotePath);
  check("clone_remote_into_remote_client_for_pull_staged", execSql(remoteClientDb, sql)==SQLITE_OK);
  check("advance_remote_for_pull_staged", execSql(remoteClientDb,
    "INSERT INTO t VALUES(2,'b');"
    "SELECT dolt_add('-A');"
    "SELECT dolt_commit('-m', 'remote update');"
    "SELECT dolt_push('origin','main');")==SQLITE_OK);

  check("make_local_staged_dirty", execSql(localDb,
    "UPDATE t SET v='local staged' WHERE id=1;"
    "SELECT dolt_add('-A');")==SQLITE_OK);
  sqlite3_snprintf(sizeof(zHeadBefore), zHeadBefore, "%s",
                   queryScalarText(localDb, "SELECT commit_hash FROM dolt_log LIMIT 1"));

  res = queryScalarText(localDb, "SELECT dolt_pull('origin','main')");
  check("pull_staged_returns_error", strstr(res, "ERROR:")!=0);
  check("pull_staged_reports_uncommitted", strstr(res, "uncommitted changes")!=0);
  check("pull_staged_branch_stays_main",
    strcmp(queryScalarText(localDb, "SELECT active_branch()"), "main")==0);
  check("pull_staged_keeps_local_rows",
    strcmp(queryScalarText(localDb, "SELECT v FROM t WHERE id=1"), "local staged")==0);
  check("pull_staged_keeps_head",
    strcmp(queryScalarText(localDb, "SELECT commit_hash FROM dolt_log LIMIT 1"), zHeadBefore)==0);

  sqlite3_close(remoteClientDb);
  remoteClientDb = 0;
  sqlite3_close(remoteDb);
  remoteDb = 0;
  sqlite3_close(localDb);
  localDb = 0;

  check("reopen_local_after_pull_staged", open_db(localPath, &localDb)==SQLITE_OK);
  check("pull_staged_persists_local_rows_after_reopen",
    strcmp(queryScalarText(localDb, "SELECT v FROM t WHERE id=1"), "local staged")==0);
  check("pull_staged_persists_head_after_reopen",
    strcmp(queryScalarText(localDb, "SELECT commit_hash FROM dolt_log LIMIT 1"), zHeadBefore)==0);

  sqlite3_close(localDb);
  removeDbFiles(localPath);
  removeDbFiles(remotePath);
  removeDbFiles(remoteClientPath);
}

static void run_push_persist_failure(void){
  sqlite3 *localDb = 0;
  sqlite3 *remoteDb = 0;
  char localPath[256];
  char remotePath[256];
  char sql[1024];
  const char *res;
  char zRemoteHeadBefore[128];

  printf("=== Push Persist Failure Test ===\n\n");
  make_dbpath(localPath, sizeof(localPath), "test_push_persist_failure_local");
  make_dbpath(remotePath, sizeof(remotePath), "test_push_persist_failure_remote");
  removeDbFiles(localPath);
  removeDbFiles(remotePath);

  gFailWriteOnce = 0;
  gFailSyncOnce = 0;
  gFailHits = 0;
  check("register_fail_vfs_for_push", registerFailVfs()==SQLITE_OK);
  check("open_remote_db_for_push", open_db(remotePath, &remoteDb)==SQLITE_OK);
  check("setup_remote_repo_for_push", execSql(remoteDb,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'remote');"
    "SELECT dolt_commit('-A', '-m', 'remote init');")==SQLITE_OK);
  sqlite3_snprintf(sizeof(zRemoteHeadBefore), zRemoteHeadBefore, "%s",
                   queryScalarText(remoteDb, "SELECT commit_hash FROM dolt_log LIMIT 1"));
  sqlite3_close(remoteDb);
  remoteDb = 0;

  check("open_local_fail_db_for_push", open_fail_db(localPath, &localDb)==SQLITE_OK);
  snprintf(sql, sizeof(sql),
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'local');"
    "SELECT dolt_commit('-A', '-m', 'local init');"
    "SELECT dolt_remote('add','origin','file://%s');"
    "UPDATE t SET v='local pushed' WHERE id=1;"
    "SELECT dolt_commit('-A', '-m', 'local update');",
    remotePath);
  check("setup_local_repo_for_push", execSql(localDb, sql)==SQLITE_OK);

  gFailHits = 0;
  gFailSyncOnce = 1;
  res = queryScalarText(localDb, "SELECT dolt_push('origin','main','--force')");
  check("push_failure_was_injected", gFailHits>0);
  check("push_returns_error_on_persist_failure", strstr(res, "ERROR:")!=0);
  check("push_keeps_local_rows",
    strcmp(queryScalarText(localDb, "SELECT v FROM t WHERE id=1"), "local pushed")==0);
  check("push_keeps_local_remote",
    strcmp(queryScalarText(localDb, "SELECT count(*) FROM dolt_remotes"), "1")==0);

  sqlite3_close(localDb);
  localDb = 0;

  check("reopen_remote_after_push_failure", open_db(remotePath, &remoteDb)==SQLITE_OK);
  check("push_failure_preserves_remote_rows_after_reopen",
    strcmp(queryScalarText(remoteDb, "SELECT v FROM t WHERE id=1"), "remote")==0);
  check("push_failure_preserves_remote_head_after_reopen",
    strcmp(queryScalarText(remoteDb, "SELECT commit_hash FROM dolt_log LIMIT 1"), zRemoteHeadBefore)==0);
  check("push_failure_preserves_remote_log_count_after_reopen",
    strcmp(queryScalarText(remoteDb, "SELECT count(*) FROM dolt_log"), "2")==0);

  sqlite3_close(remoteDb);
  removeDbFiles(localPath);
  removeDbFiles(remotePath);
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
  removeDbFiles(localPath);
  removeDbFiles(remotePath);

  gFailSyncOnce = 0;
  gFailHits = 0;
  check("register_fail_vfs_for_clone", registerFailVfs()==SQLITE_OK);
  check("open_remote_db", open_db(remotePath, &remoteDb)==SQLITE_OK);
  check("setup_remote_repo", execSql(remoteDb,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);
  sqlite3_close(remoteDb);
  remoteDb = 0;

  check("open_local_fail_db", open_fail_db(localPath, &localDb)==SQLITE_OK);
  snprintf(sql, sizeof(sql), "SELECT dolt_clone('file://%s')", remotePath);
  gFailHits = 0;
  gFailSyncOnce = 2;
  res = queryScalarText(localDb, sql);
  check("clone_failure_was_injected", gFailHits>0);
  check("clone_returns_error_on_persist_failure", strstr(res, "ERROR:")!=0);
  check("clone_does_not_leave_origin_remote",
    strcmp(queryScalarText(localDb, "SELECT count(*) FROM dolt_remotes"), "0")==0);
  check("clone_restores_empty_catalog",
    strcmp(queryScalarText(localDb,
      "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='t'"), "0")==0);

  sqlite3_close(localDb);
  localDb = 0;

  check("reopen_local_after_clone_failure", open_db(localPath, &localDb)==SQLITE_OK);
  check("clone_failure_persists_no_origin_after_reopen",
    strcmp(queryScalarText(localDb, "SELECT count(*) FROM dolt_remotes"), "0")==0);
  check("clone_failure_persists_empty_catalog_after_reopen",
    strcmp(queryScalarText(localDb,
      "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='t'"), "0")==0);

  sqlite3_close(localDb);
  removeDbFiles(localPath);
  removeDbFiles(remotePath);
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
  removeDbFiles(dbpath);

  check("open_db", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo", execSql(db,
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
  removeDbFiles(dbpath);
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

static void run_commit_am_many_tables(void){
  sqlite3 *db = 0;
  char dbpath[256];
  char sql[256];
  int i;
  const char *res;

  printf("=== Commit -am Many Tables Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_commit_am_many_tables");
  removeDbFiles(dbpath);

  check("open_db_for_commit_am_many_tables", open_db(dbpath, &db)==SQLITE_OK);
  check("begin_commit_am_many_tables_setup", execSql(db, "BEGIN;")==SQLITE_OK);
  for(i=1; i<=100; i++){
    sqlite3_snprintf(sizeof(sql), sql,
      "CREATE TABLE t%03d(id INTEGER PRIMARY KEY, v INT);"
      "INSERT INTO t%03d VALUES(1,0);", i, i);
    check("create_table_for_commit_am_many_tables", execSql(db, sql)==SQLITE_OK);
  }
  check("seed_commit_am_many_tables", execSql(db,
    "COMMIT;"
    "SELECT dolt_commit('-A', '-m', 'seed');")==SQLITE_OK);

  for(i=1; i<=80; i++){
    sqlite3_snprintf(sizeof(sql), sql,
      "UPDATE t%03d SET v=%d WHERE id=1;", i, i);
    check("update_table_for_commit_am_many_tables", execSql(db, sql)==SQLITE_OK);
  }
  for(i=81; i<=90; i++){
    sqlite3_snprintf(sizeof(sql), sql, "DROP TABLE t%03d;", i);
    check("drop_table_for_commit_am_many_tables", execSql(db, sql)==SQLITE_OK);
  }
  for(i=1; i<=10; i++){
    sqlite3_snprintf(sizeof(sql), sql,
      "CREATE TABLE n%03d(id INTEGER PRIMARY KEY);"
      "INSERT INTO n%03d VALUES(1);", i, i);
    check("create_untracked_table_for_commit_am_many_tables",
          execSql(db, sql)==SQLITE_OK);
  }

  res = queryScalarText(db, "SELECT dolt_commit('-am', 'tracked changes');");
  check("commit_am_many_tables_returns_hash", strlen(res)==40);
  check("commit_am_many_tables_log_message",
        strcmp(queryScalarText(db, "SELECT message FROM dolt_log LIMIT 1;"),
               "tracked changes")==0);
  check("commit_am_many_tables_updated_tracked_value",
        strcmp(queryScalarText(db, "SELECT v FROM t080;"), "80")==0);
  check("commit_am_many_tables_dropped_tracked_absent",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM sqlite_master WHERE type='table' "
          "AND name='t090';"), "0")==0);
  res = queryScalarText(db,
          "SELECT count(*) FROM dolt_status "
          "WHERE staged=0 AND status='new table';");
  check("commit_am_many_tables_new_tables_left_unstaged",
        strcmp(res, "10")==0);
  if( strcmp(res, "10")!=0 ){
    sqlite3_stmt *pDump = 0;
    fprintf(stderr, "  unstaged-new count=%s; full dolt_status:\n", res);
    if( sqlite3_prepare_v2(db,
          "SELECT table_name, staged, status FROM dolt_status;",
          -1, &pDump, 0)==SQLITE_OK ){
      while( sqlite3_step(pDump)==SQLITE_ROW ){
        fprintf(stderr, "    %s staged=%d %s\n",
                sqlite3_column_text(pDump, 0), sqlite3_column_int(pDump, 1),
                sqlite3_column_text(pDump, 2));
      }
    }
    sqlite3_finalize(pDump);
  }

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_blame_all_parents_merge_base(void){
  sqlite3 *db = 0;
  ChunkStore *cs = 0;
  char dbpath[256];
  ProllyHash b1Hash, b2Hash, b3Hash, curHeadHash, mergeHash;
  ProllyHash base12Hash, base123Hash;
  DoltliteCommit b2Commit, mergeCommit;
  u8 *pCommitData = 0;
  int nCommitData = 0;
  const char *res;

  printf("=== Blame All-Parents Merge Base Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_blame_all_parents_merge_base");
  removeDbFiles(dbpath);

  memset(&b1Hash, 0, sizeof(b1Hash));
  memset(&b2Hash, 0, sizeof(b2Hash));
  memset(&b3Hash, 0, sizeof(b3Hash));
  memset(&curHeadHash, 0, sizeof(curHeadHash));
  memset(&mergeHash, 0, sizeof(mergeHash));
  memset(&base12Hash, 0, sizeof(base12Hash));
  memset(&base123Hash, 0, sizeof(base123Hash));
  memset(&b2Commit, 0, sizeof(b2Commit));
  memset(&mergeCommit, 0, sizeof(mergeCommit));

  check("open_db_for_blame_all_parents", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_base_for_blame_all_parents", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'base');"
    "SELECT dolt_commit('-A', '-m', 'BASE');"
    "SELECT dolt_branch('b1');"
    "SELECT dolt_checkout('b1');"
    "UPDATE t SET v='one' WHERE id=1;"
    "SELECT dolt_commit('-A', '-m', 'B1');")==SQLITE_OK);
  doltliteGetSessionHead(db, &b1Hash);
  check("have_b1_head_for_blame_all_parents", !prollyHashIsEmpty(&b1Hash));

  check("setup_b2_for_blame_all_parents", execSql(db,
    "SELECT dolt_branch('b2');"
    "SELECT dolt_checkout('b2');"
    "INSERT INTO t VALUES(2,'side');"
    "SELECT dolt_commit('-A', '-m', 'B2');")==SQLITE_OK);
  doltliteGetSessionHead(db, &b2Hash);
  check("have_b2_head_for_blame_all_parents", !prollyHashIsEmpty(&b2Hash));

  check("setup_b3_for_blame_all_parents", execSql(db,
    "SELECT dolt_checkout('main');"
    "SELECT dolt_branch('b3');"
    "SELECT dolt_checkout('b3');"
    "INSERT INTO t VALUES(3,'other');"
    "SELECT dolt_commit('-A', '-m', 'B3');")==SQLITE_OK);
  doltliteGetSessionHead(db, &b3Hash);
  check("have_b3_head_for_blame_all_parents", !prollyHashIsEmpty(&b3Hash));

  check("ancestor_b1_b2_for_blame_all_parents",
        doltliteFindAncestor(db, &b1Hash, &b2Hash, &base12Hash)==SQLITE_OK
        && !prollyHashIsEmpty(&base12Hash)
        && prollyHashCompare(&base12Hash, &b1Hash)==0);
  check("ancestor_b12_b3_for_blame_all_parents",
        doltliteFindAncestor(db, &base12Hash, &b3Hash, &base123Hash)==SQLITE_OK
        && !prollyHashIsEmpty(&base123Hash)
        && prollyHashCompare(&base123Hash, &b1Hash)!=0);

  check("checkout_b2_before_synthetic_merge", execSql(db,
    "SELECT dolt_checkout('b2');")==SQLITE_OK);
  doltliteGetSessionHead(db, &curHeadHash);
  check("head_is_b2_before_synthetic_merge",
        prollyHashCompare(&curHeadHash, &b2Hash)==0);
  check("load_b2_commit_for_blame_all_parents",
        doltliteLoadCommit(db, &b2Hash, &b2Commit)==SQLITE_OK);

  cs = doltliteGetChunkStore(db);
  check("have_chunk_store_for_blame_all_parents", cs!=0);
  if( cs ){
    mergeCommit.parentHash = b1Hash;
    mergeCommit.catalogHash = b2Commit.catalogHash;
    mergeCommit.timestamp = b2Commit.timestamp + 1;
    mergeCommit.zName = sqlite3_mprintf("oracle");
    mergeCommit.zEmail = sqlite3_mprintf("oracle@test");
    mergeCommit.zMessage = sqlite3_mprintf("OCTO");
    mergeCommit.nParents = 3;
    mergeCommit.aParents[0] = b1Hash;
    mergeCommit.aParents[1] = b2Hash;
    mergeCommit.aParents[2] = b3Hash;
    check("serialize_octopus_commit_for_blame_all_parents",
          doltliteCommitSerialize(&mergeCommit, &pCommitData, &nCommitData)==SQLITE_OK);
    check("store_octopus_commit_for_blame_all_parents",
          chunkStorePut(cs, pCommitData, nCommitData, &mergeHash)==SQLITE_OK);
    check("commit_octopus_commit_for_blame_all_parents",
          chunkStoreCommit(cs)==SQLITE_OK);
    /* These are internal entry points whose production callers run inside
    ** sqlite3_step, so they expect the connection mutex to be held.
    ** doltliteSwitchCatalog resets connection schemas and unlocks the vtab
    ** list, which asserts on it. */
    sqlite3_mutex_enter(sqlite3_db_mutex(db));
    doltliteSetSessionHead(db, &mergeHash);
    doltliteSetSessionStaged(db, &mergeCommit.catalogHash);
    check("switch_catalog_to_octopus_commit_for_blame_all_parents",
          doltliteSwitchCatalog(db, &mergeCommit.catalogHash)==SQLITE_OK);
    sqlite3_mutex_leave(sqlite3_db_mutex(db));
  }

  res = queryScalarText(db, "SELECT message FROM dolt_blame_t WHERE id = 1;");
  check("blame_query_returns_row_for_all_parents", res[0]!=0);
  check("blame_uses_all_parents_merge_base", strcmp(res, "OCTO")==0);

  sqlite3_free(pCommitData);
  doltliteCommitClear(&b2Commit);
  doltliteCommitClear(&mergeCommit);
  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_blame_deep_history_scan(void){
  sqlite3 *db = 0;
  char dbpath[256];
  char sql[256];
  int i;

  printf("=== Blame Deep History Scan Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_blame_deep_history_scan");
  removeDbFiles(dbpath);

  check("open_db_for_blame_deep_history_scan", open_db(dbpath, &db)==SQLITE_OK);
  check("create_table_for_blame_deep_history_scan", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);"
    "BEGIN;")==SQLITE_OK);
  for(i=1; i<=100; i++){
    sqlite3_snprintf(sizeof(sql), sql,
      "INSERT INTO t VALUES(%d,0);", i);
    check("insert_row_for_blame_deep_history_scan",
          execSql(db, sql)==SQLITE_OK);
  }
  check("commit_initial_rows_for_blame_deep_history_scan", execSql(db,
    "COMMIT;"
    "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);

  for(i=1; i<=80; i++){
    sqlite3_snprintf(sizeof(sql), sql,
      "UPDATE t SET v=%d WHERE id=%d;"
      "SELECT dolt_commit('-A', '-m', 'c%d');", i, i, i);
    check("commit_update_for_blame_deep_history_scan",
          execSql(db, sql)==SQLITE_OK);
  }

  check("blame_deep_history_row_count",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_blame_t;"), "100")==0);
  check("blame_deep_history_updated_row",
        strcmp(queryScalarText(db, "SELECT message FROM dolt_blame_t WHERE id=80;"),
               "c80")==0);
  check("blame_deep_history_unchanged_row",
        strcmp(queryScalarText(db, "SELECT message FROM dolt_blame_t WHERE id=100;"),
               "init")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static char *blameWidePkSql(
  sqlite3 *db,
  const char *zTable,
  int nCols,
  int insertRow
){
  sqlite3_str *pSql = sqlite3_str_new(db);
  int i;
  if( !pSql ) return 0;
  sqlite3_str_appendf(pSql, "CREATE TABLE \"%w\"(", zTable);
  for(i=1; i<=nCols; i++){
    sqlite3_str_appendf(pSql, "%sc%d INTEGER", i==1 ? "" : ",", i);
  }
  sqlite3_str_appendall(pSql, ",PRIMARY KEY(");
  for(i=1; i<=nCols; i++){
    sqlite3_str_appendf(pSql, "%sc%d", i==1 ? "" : ",", i);
  }
  sqlite3_str_appendall(pSql, "));");
  if( insertRow ){
    sqlite3_str_appendf(pSql, "INSERT INTO \"%w\" VALUES(", zTable);
    for(i=1; i<=nCols; i++){
      sqlite3_str_appendf(pSql, "%s%d", i==1 ? "" : ",", i);
    }
    sqlite3_str_appendall(pSql, ");");
  }
  return sqlite3_str_finish(pSql);
}

static void run_blame_wide_primary_key(void){
  sqlite3 *db = 0;
  sqlite3_stmt *pStmt = 0;
  char dbpath[256];
  char *zSql;
  int nLimit;
  int rc;

  printf("=== Blame Wide Primary Key Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_blame_wide_primary_key");
  removeDbFiles(dbpath);

  check("open_db_for_blame_wide_primary_key",
        open_db(dbpath, &db)==SQLITE_OK);
  if( !db ) return;

  zSql = blameWidePkSql(db, "wide65", 65, 1);
  check("build_65_column_primary_key_sql", zSql!=0);
  if( zSql ){
    check("create_65_column_primary_key", execSql(db, zSql)==SQLITE_OK);
    sqlite3_free(zSql);
  }
  check("commit_65_column_primary_key", execSql(db,
        "SELECT dolt_commit('-Am', 'wide primary key');")==SQLITE_OK);
  check("blame_65_column_schema_complete",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM pragma_table_info('dolt_blame_wide65');"),
          "70")==0);
  check("blame_65_column_row_complete",
        strcmp(queryScalarText(db,
          "SELECT c65 || '|' || message FROM dolt_blame_wide65;"),
          "65|wide primary key")==0);

  nLimit = sqlite3_limit(db, SQLITE_LIMIT_COLUMN, -1);
  zSql = blameWidePkSql(db, "widemax", nLimit, 0);
  check("build_max_column_primary_key_sql", zSql!=0);
  if( zSql ){
    check("create_max_column_primary_key", execSql(db, zSql)==SQLITE_OK);
    sqlite3_free(zSql);
  }
  check("commit_max_column_primary_key", execSql(db,
        "SELECT dolt_commit('-Am', 'maximum primary key');")==SQLITE_OK);
  check("max_column_primary_key_is_complete",
        atoi(queryScalarText(db,
          "SELECT count(*) FROM pragma_table_info('widemax');"))==nLimit);
  rc = sqlite3_prepare_v2(db,
      "SELECT count(*) FROM dolt_blame_widemax;", -1, &pStmt, 0);
  check("blame_max_column_primary_key_fails_explicitly", rc==SQLITE_TOOBIG);
  check("blame_max_column_primary_key_reports_limit",
        strstr(sqlite3_errmsg(db), "column limit")!=0);
  sqlite3_finalize(pStmt);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_merge_persist_failure(void){
  sqlite3 *db = 0;
  char dbpath[256];
  const char *res;

  printf("=== Merge Persist Failure Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_merge_persist_failure");
  removeDbFiles(dbpath);

  gFailSyncOnce = 0;
  gFailOpenMainOnce = 0;
  gFailHits = 0;
  check("register_fail_vfs_for_merge", registerFailVfs()==SQLITE_OK);
  check("open_fail_db", open_fail_db(dbpath, &db)==SQLITE_OK);
  check("setup_ff_merge_repo", execSql(db,
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
  res = queryScalarText(db, "SELECT dolt_merge('feature')");
  check("merge_failure_was_injected", gFailHits>0);
  check("merge_returns_error_on_persist_failure", strstr(res, "ERROR:")!=0);
  check("merge_restores_branch_name",
    strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_merge_conflict_persist_failure(void){
  sqlite3 *db = 0;
  char dbpath[256];
  const char *res;
  ProllyHash conflictHash;

  printf("=== Merge Conflict Persist Failure Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_merge_conflict_persist_failure");
  removeDbFiles(dbpath);

  check("open_db_for_merge_conflict_persist_failure",
        open_db(dbpath, &db)==SQLITE_OK);
  check("setup_merge_conflict_persist_failure", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'base');"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "SELECT dolt_branch('feature');"
    "UPDATE t SET v='main' WHERE id=1;"
    "SELECT dolt_commit('-A', '-m', 'main edit');"
    "SELECT dolt_checkout('feature');"
    "UPDATE t SET v='feature' WHERE id=1;"
    "SELECT dolt_commit('-A', '-m', 'feature edit');"
    "SELECT dolt_checkout('main');")==SQLITE_OK);

  gRegressionFaultCode = 950;
  gRegressionFaultHits = 0;
  sqlite3_test_control(SQLITE_TESTCTRL_FAULT_INSTALL, regressionFaultCallback);
  res = queryScalarText(db, "SELECT dolt_merge('feature')");
  sqlite3_test_control(SQLITE_TESTCTRL_FAULT_INSTALL, 0);
  gRegressionFaultCode = 0;

  check("merge_conflict_persist_failure_was_injected",
        gRegressionFaultHits==1);
  check("merge_conflict_persist_failure_is_propagated",
        strcmp(res, "ERROR: merge failed")==0);
  doltliteGetSessionConflictsCatalog(db, &conflictHash);
  check("merge_conflict_persist_failure_does_not_publish_hash",
        prollyHashIsEmpty(&conflictHash));
  check("merge_conflict_persist_failure_preserves_working_row",
        strcmp(queryScalarText(db, "SELECT v FROM t WHERE id=1"), "main")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_conflict_serializer_bounds(void){
  ChunkStore cs;
  DoltliteConflictTable table;
  DoltliteConflictRow row;
  ProllyHash hash;
  DlByteWriter w;
  u8 aBuf[5] = {0, 0, 0, 0, 0x5a};
  int rc;

  printf("=== Conflict Serializer Bounds Test ===\n\n");
  memset(&cs, 0, sizeof(cs));
  memset(&table, 0, sizeof(table));
  memset(&row, 0, sizeof(row));
  check("open_memory_store_for_conflict_serializer_bounds",
        chunkStoreOpen(&cs, sqlite3_vfs_find(0), ":memory:", 0)==SQLITE_OK);

  table.zName = "t";
  table.nConflicts = 1;
  table.aRows = &row;
  row.pKey = aBuf;
  row.nKey = INT_MAX;
  rc = doltliteSerializeConflicts(&cs, &table, 1, &hash);
  check("conflict_serializer_rejects_size_overflow", rc==SQLITE_TOOBIG);

  dlWriterInit(&w, aBuf, 4);
  dlWriteU32(&w, 1);
  dlWriteU8(&w, 2);
  check("byte_writer_reports_overflow", w.err!=0);
  check("byte_writer_preserves_canary", aBuf[4]==0x5a);

  chunkStoreClose(&cs);
}

static void run_merge_conflict_surfaces_error_and_rollback_clears_durable_state(void){
  sqlite3 *db = 0;
  char dbpath[256];
  const char *res;

  printf("=== Merge Conflict Surfaces Error Rolls Back Durable State Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_merge_conflict_surfaces_error");
  removeDbFiles(dbpath);

  check("open_db_for_merge_conflict_error", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_merge_conflict_repo", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'base');"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "SELECT dolt_branch('feature');"
    "UPDATE t SET v='main' WHERE id=1;"
    "SELECT dolt_commit('-A', '-m', 'main edit');"
    "SELECT dolt_checkout('feature');"
    "UPDATE t SET v='feature' WHERE id=1;"
    "SELECT dolt_commit('-A', '-m', 'feature edit');"
    "SELECT dolt_checkout('main');")==SQLITE_OK);

  res = queryScalarText(db, "SELECT dolt_merge('feature')");
  check("merge_conflict_returns_error",
        strstr(res, "ERROR:")!=0);
  check("merge_conflict_keeps_active_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("merge_conflict_keeps_working_value_before_close",
        strcmp(queryScalarText(db, "SELECT v FROM t WHERE id=1"), "main")==0);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_after_merge_conflict_error", open_db(dbpath, &db)==SQLITE_OK);
  check("merge_conflict_reopen_has_no_summary_table_rows",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_conflicts"), "0")==0);
  check("merge_conflict_reopen_has_no_per_table_rows",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_conflicts_t"), "0")==0);
  check("merge_conflict_reopen_keeps_working_value",
        strcmp(queryScalarText(db, "SELECT v FROM t WHERE id=1"), "main")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_failed_merge_reopen_clears_ephemeral_conflict_state(void){
  sqlite3 *db = 0;
  char dbpath[256];
  const char *res;
  ProllyHash stagedBeforeClose;
  ProllyHash mergeBeforeClose;
  ProllyHash conflictsBeforeClose;
  ProllyHash stagedAfterReopen;
  ProllyHash mergeAfterReopen;
  ProllyHash conflictsAfterReopen;
  u8 isMergingBeforeClose = 0;
  u8 isMergingAfterReopen = 0;

  printf("=== Failed Merge Reopen Clears Ephemeral Conflict State Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_failed_merge_reopen_preserves_working_set_state");
  removeDbFiles(dbpath);

  check("open_db_for_failed_merge_reopen", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_failed_merge_reopen_repo", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'base');"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "SELECT dolt_branch('feature');"
    "UPDATE t SET v='main' WHERE id=1;"
    "SELECT dolt_commit('-A', '-m', 'main edit');"
    "SELECT dolt_checkout('feature');"
    "UPDATE t SET v='feature' WHERE id=1;"
    "SELECT dolt_commit('-A', '-m', 'feature edit');"
    "SELECT dolt_checkout('main');")==SQLITE_OK);

  res = queryScalarText(db, "SELECT dolt_merge('feature')");
  check("failed_merge_reopen_returns_error",
        strstr(res, "ERROR:")!=0);
  check("failed_merge_reopen_working_value_before_close",
        strcmp(queryScalarText(db, "SELECT v FROM t WHERE id=1"), "main")==0);
  doltliteGetSessionStaged(db, &stagedBeforeClose);
  doltliteGetSessionMergeState(db, &isMergingBeforeClose,
                               &mergeBeforeClose, &conflictsBeforeClose);
  check("failed_merge_reopen_staged_hash_before_close_nonempty",
        !prollyHashIsEmpty(&stagedBeforeClose));
  check("failed_merge_reopen_merging_flag_cleared_before_close",
        isMergingBeforeClose==0);
  check("failed_merge_reopen_conflicts_hash_cleared_before_close",
        prollyHashIsEmpty(&conflictsBeforeClose));

  sqlite3_close(db);
  db = 0;

  check("reopen_db_for_failed_merge_reopen", open_db(dbpath, &db)==SQLITE_OK);
  check("failed_merge_reopen_conflicts_summary_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_conflicts"), "0")==0);
  check("failed_merge_reopen_conflicts_table_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_conflicts_t"), "0")==0);
  check("failed_merge_reopen_working_value_after_reopen",
        strcmp(queryScalarText(db, "SELECT v FROM t WHERE id=1"), "main")==0);
  check("failed_merge_reopen_branch_after_reopen",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);

  doltliteGetSessionStaged(db, &stagedAfterReopen);
  doltliteGetSessionMergeState(db, &isMergingAfterReopen,
                               &mergeAfterReopen, &conflictsAfterReopen);
  check("failed_merge_reopen_merging_flag_cleared_after_reopen",
        isMergingAfterReopen==0);
  check("failed_merge_reopen_conflicts_hash_cleared_after_reopen",
        prollyHashIsEmpty(&conflictsAfterReopen));

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_merge_abort_after_reopen_restores_durable_state(void){
  sqlite3 *db = 0;
  char dbpath[256];
  const char *res;
  u8 isMerging = 0;
  ProllyHash mergeHash;
  ProllyHash conflictsHash;
  char zHeadBefore[128];
  RepoStateSnapshot beforeReopenAbort;
  RepoStateSnapshot afterReopenAbort;

  printf("=== Merge Abort After Reopen Restores Durable State Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_merge_abort_after_reopen_restores_durable_state");
  removeDbFiles(dbpath);

  check("open_db_for_merge_abort_after_reopen", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_merge_abort_after_reopen", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'base');"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "SELECT dolt_branch('feature');"
    "UPDATE t SET v='main' WHERE id=1;"
    "SELECT dolt_commit('-A', '-m', 'main edit');"
    "SELECT dolt_checkout('feature');"
    "UPDATE t SET v='feature' WHERE id=1;"
    "SELECT dolt_commit('-A', '-m', 'feature edit');"
    "SELECT dolt_checkout('main');")==SQLITE_OK);
  sqlite3_snprintf(sizeof(zHeadBefore), zHeadBefore, "%s",
                   queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"));

  res = queryScalarText(db, "SELECT dolt_merge('feature')");
  check("merge_abort_after_reopen_setup_conflict",
        strstr(res, "ERROR:")!=0);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_for_merge_abort_after_reopen", open_db(dbpath, &db)==SQLITE_OK);
  check("merge_abort_after_reopen_conflicts_persist_before_abort",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_conflicts"), "0")==0);
  check("merge_abort_after_reopen_branch_before_abort",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  doltliteGetSessionMergeState(db, &isMerging, &mergeHash, &conflictsHash);
  check("merge_abort_after_reopen_merging_flag_before_abort", isMerging==0);
  check("merge_abort_after_reopen_conflicts_hash_before_abort",
        prollyHashIsEmpty(&conflictsHash));

  check("merge_abort_after_reopen_returns_error",
        strstr(queryScalarText(db, "SELECT dolt_merge('--abort')"), "ERROR: no merge in progress")!=0);
  check("merge_abort_after_reopen_clears_conflicts",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_conflicts"), "0")==0);
  check("merge_abort_after_reopen_restores_rows",
        strcmp(queryScalarText(db, "SELECT v FROM t WHERE id=1"), "main")==0);
  check("merge_abort_after_reopen_restores_status_clean",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_status"), "0")==0);
  check("merge_abort_after_reopen_restores_head",
        strcmp(queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"), zHeadBefore)==0);
  doltliteGetSessionMergeState(db, &isMerging, &mergeHash, &conflictsHash);
  check("merge_abort_after_reopen_clears_merge_flag", isMerging==0);
  check("merge_abort_after_reopen_clears_conflicts_hash",
        prollyHashIsEmpty(&conflictsHash));
  capture_repo_state_snapshot(db, &beforeReopenAbort);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_after_merge_abort", open_db(dbpath, &db)==SQLITE_OK);
  check("merge_abort_persists_no_conflicts",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_conflicts"), "0")==0);
  check("merge_abort_persists_rows",
        strcmp(queryScalarText(db, "SELECT v FROM t WHERE id=1"), "main")==0);
  check("merge_abort_persists_status_clean",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_status"), "0")==0);
  check("merge_abort_persists_head",
        strcmp(queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"), zHeadBefore)==0);
  doltliteGetSessionMergeState(db, &isMerging, &mergeHash, &conflictsHash);
  check("merge_abort_persists_merge_flag_cleared", isMerging==0);
  check("merge_abort_persists_conflicts_hash_cleared",
        prollyHashIsEmpty(&conflictsHash));
  capture_repo_state_snapshot(db, &afterReopenAbort);
  check("merge_abort_reopen_snapshot_matches",
        repo_state_snapshot_eq(&beforeReopenAbort, &afterReopenAbort));

  sqlite3_close(db);
  removeDbFiles(dbpath);
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
  removeDbFiles(dbpath);

  check("open_db1", open_db(dbpath, &db1)==SQLITE_OK);
  check("setup_init", execSql(db1,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "SELECT dolt_branch('feature');"
    "SELECT dolt_checkout('feature');"
    "INSERT INTO t VALUES(2,'feat');"
    "SELECT dolt_commit('-A', '-m', 'feature work');"
    "SELECT dolt_checkout('main');")==SQLITE_OK);

  snprintf(featHash, sizeof(featHash), "%s",
           queryScalarText(db1, "SELECT hash FROM dolt_branches WHERE name='feature'"));

  check("open_db2", open_db(dbpath, &db2)==SQLITE_OK);
  check("db2_sees_old_main",
    strcmp(queryScalarText(db2, "SELECT count(*) FROM t"), "1")==0);

  check("advance_main_in_db1", execSql(db1,
    "INSERT INTO t VALUES(3,'main');"
    "SELECT dolt_commit('-A', '-m', 'main work');")==SQLITE_OK);

  snprintf(sql, sizeof(sql), "SELECT dolt_cherry_pick('%s')", featHash);
  res = queryScalarText(db2, sql);
  check("stale_cherry_pick_returns_error", strstr(res, "ERROR:")!=0);

  check("open_db3", open_db(dbpath, &db3)==SQLITE_OK);
  check("stale_cherry_pick_does_not_persist_feature_row",
    strcmp(queryScalarText(db3, "SELECT count(*) FROM t"), "2")==0);

  sqlite3_close(db3);
  sqlite3_close(db2);
  sqlite3_close(db1);
  removeDbFiles(dbpath);
}

static void run_cherry_pick_stale_conflict_clears_session(void){
  sqlite3 *db = 0;
  char dbpath[256];
  const char *res;
  ProllyHash conflictsHash;
  u8 isMerging = 0;

  printf("=== Cherry-pick Stale Conflict Clears Session ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_cherry_pick_stale_conflict_clears_session");
  removeDbFiles(dbpath);

  check("open_db_stale_conflict", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_stale_conflict_repo", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'base');"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "SELECT dolt_branch('feature');"
    "SELECT dolt_checkout('feature');"
    "UPDATE t SET v='feat' WHERE id=1;"
    "SELECT dolt_commit('-A', '-m', 'feat');"
    "SELECT dolt_checkout('main');"
    "UPDATE t SET v='main' WHERE id=1;"
    "SELECT dolt_commit('-A', '-m', 'main');")==SQLITE_OK);

  doltliteTestFailNextHeadConfirm();
  res = queryScalarText(db, "SELECT dolt_cherry_pick('feature')");
  check("stale_conflict_cherry_pick_busy",
        strstr(res, "another connection")!=0);

  doltliteGetSessionMergeState(db, &isMerging, 0, &conflictsHash);
  check("stale_conflict_cherry_pick_not_merging", isMerging==0);
  check("stale_conflict_cherry_pick_no_conflicts_catalog",
        prollyHashIsEmpty(&conflictsHash));
  check("stale_conflict_working_still_main",
        strcmp(queryScalarText(db, "SELECT v FROM t WHERE id=1"), "main")==0);

  check("stale_conflict_insert_after",
        execSql(db, "INSERT INTO t VALUES(3,'after');")==SQLITE_OK);
  res = queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'after');");
  check("stale_conflict_cherry_pick_not_unresolved_merge",
        strstr(res, "unresolved merge conflicts")==0);
  check("stale_conflict_cherry_pick_commit_after", strlen(res)==40);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_cherry_pick_conflict_is_not_a_merge(void){
  sqlite3 *db = 0;
  char dbpath[256];
  const char *res;
  u8 isMerging = 0;

  printf("=== Cherry-pick Conflict Is Not A Merge Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath),
              "test_cherry_pick_conflict_is_not_a_merge");
  removeDbFiles(dbpath);

  check("open_db_for_cherry_pick_conflict_not_merge",
        open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_cherry_pick_conflict_not_merge", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A','-m','init');"
    "SELECT dolt_checkout('-b','feat');"
    "UPDATE t SET v='F';"
    "SELECT dolt_commit('-A','-m','f');"
    "SELECT dolt_checkout('main');"
    "UPDATE t SET v='M';"
    "SELECT dolt_commit('-A','-m','m');"
    "SELECT dolt_branch('other');")==SQLITE_OK);

  check("begin_for_cherry_pick_conflict_not_merge",
        execSql(db, "BEGIN;")==SQLITE_OK);
  res = queryScalarText(db, "SELECT dolt_cherry_pick('feat')");
  check("cherry_pick_conflict_reports_conflicts",
        strstr(res, "conflict")!=0);
  doltliteGetSessionMergeState(db, &isMerging, 0, 0);
  check("cherry_pick_conflict_is_not_merging", isMerging==0);
  check("cherry_pick_conflict_rows_are_visible",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_conflicts"),
               "1")==0);
  res = queryScalarText(db, "SELECT dolt_checkout('other')");
  check("cherry_pick_conflict_blocks_checkout",
        strstr(res, "unresolved conflicts")!=0);
  check("cherry_pick_conflict_checkout_is_not_a_merge",
        strstr(res, "unresolved merge conflicts")==0);
  res = queryScalarText(db, "SELECT dolt_merge('--abort')");
  check("cherry_pick_conflict_has_no_merge_to_abort",
        strstr(res, "no merge in progress")!=0);
  check("rollback_cherry_pick_conflict_not_merge",
        execSql(db, "ROLLBACK;")==SQLITE_OK);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_revert_conflict_is_not_a_merge(void){
  sqlite3 *db = 0;
  char dbpath[256];
  const char *res;
  u8 isMerging = 0;

  printf("=== Revert Conflict Is Not A Merge Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_revert_conflict_is_not_a_merge");
  removeDbFiles(dbpath);

  check("open_db_for_revert_conflict_not_merge",
        open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_revert_conflict_not_merge", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A','-m','init');"
    "UPDATE t SET v='b';"
    "SELECT dolt_commit('-A','-m','b');"
    "UPDATE t SET v='c';"
    "SELECT dolt_commit('-A','-m','c');")==SQLITE_OK);

  check("begin_for_revert_conflict_not_merge",
        execSql(db, "BEGIN;")==SQLITE_OK);
  res = queryScalarText(db, "SELECT dolt_revert('HEAD~1')");
  check("revert_conflict_reports_conflicts",
        strstr(res, "conflict")!=0);
  doltliteGetSessionMergeState(db, &isMerging, 0, 0);
  check("revert_conflict_is_not_merging", isMerging==0);
  check("revert_conflict_rows_are_visible",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_conflicts"),
               "1")==0);
  res = queryScalarText(db, "SELECT dolt_merge('--abort')");
  check("revert_conflict_has_no_merge_to_abort",
        strstr(res, "no merge in progress")!=0);
  check("rollback_revert_conflict_not_merge",
        execSql(db, "ROLLBACK;")==SQLITE_OK);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_cherry_pick_seal_fail_keeps_advanced_tip(void){
  sqlite3 *db = 0;
  ChunkStore *cs;
  char dbpath[256];
  const char *res;
  ProllyHash sessionHead;
  ProllyHash diskTip;
  int found = 0;

  printf("=== Cherry-pick Seal Failure Keeps Advanced Tip ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_cherry_pick_seal_fail_keeps_advanced_tip");
  removeDbFiles(dbpath);

  check("open_db_seal_fail", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_seal_fail_repo", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'base');"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "SELECT dolt_branch('feature');"
    "SELECT dolt_checkout('feature');"
    "INSERT INTO t VALUES(2,'feat');"
    "SELECT dolt_commit('-A', '-m', 'feat');"
    "SELECT dolt_checkout('main');")==SQLITE_OK);

  doltliteTestFailNextVcSeal();
  res = queryScalarText(db, "SELECT dolt_cherry_pick('feature')");
  check("cherry_pick_seal_fail_reports_error", strstr(res, "ERROR:")!=0);

  cs = doltliteGetChunkStore(db);
  check("cherry_pick_seal_fail_has_store", cs!=0);
  doltliteGetSessionHead(db, &sessionHead);
  check("cherry_pick_seal_fail_read_disk_tip",
        cs && chunkStoreFindBranch(cs, "main", &diskTip)==SQLITE_OK);
  found = cs && !prollyHashIsEmpty(&diskTip);
  check("cherry_pick_seal_fail_disk_tip_advanced", found);
  check("cherry_pick_seal_fail_session_matches_disk",
        found && prollyHashCompare(&sessionHead, &diskTip)==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_failed_cherry_pick_reopen_preserves_conflict_state(void){
  sqlite3 *db = 0;
  char dbpath[256];
  const char *res;
  ProllyHash stagedBeforeClose;
  ProllyHash mergeBeforeClose;
  ProllyHash conflictsBeforeClose;
  ProllyHash stagedAfterReopen;
  ProllyHash mergeAfterReopen;
  ProllyHash conflictsAfterReopen;
  u8 isMergingBeforeClose = 0;
  u8 isMergingAfterReopen = 0;

  printf("=== Failed Cherry-pick Reopen Preserves Conflict State Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_failed_cherry_pick_reopen_preserves_conflict_state");
  removeDbFiles(dbpath);

  check("open_db_for_failed_cherry_pick_reopen", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_failed_cherry_pick_reopen_repo", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'base');"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "SELECT dolt_branch('feature');"
    "SELECT dolt_checkout('feature');"
    "UPDATE t SET v='feature' WHERE id=1;"
    "SELECT dolt_commit('-A', '-m', 'feature edit');"
    "SELECT dolt_tag('feat-conflict');"
    "SELECT dolt_checkout('main');"
    "UPDATE t SET v='main' WHERE id=1;"
    "SELECT dolt_commit('-A', '-m', 'main edit');")==SQLITE_OK);

  res = queryScalarText(db, "SELECT dolt_cherry_pick('feat-conflict')");
  check("failed_cherry_pick_reopen_returns_error",
        strstr(res, "ERROR:")!=0);
  check("failed_cherry_pick_reopen_working_value_before_close",
        strcmp(queryScalarText(db, "SELECT v FROM t WHERE id=1"), "main")==0);
  check("failed_cherry_pick_reopen_branch_before_close",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);

  doltliteGetSessionStaged(db, &stagedBeforeClose);
  doltliteGetSessionMergeState(db, &isMergingBeforeClose,
                               &mergeBeforeClose, &conflictsBeforeClose);
  check("failed_cherry_pick_reopen_staged_hash_before_close_nonempty",
        !prollyHashIsEmpty(&stagedBeforeClose));
  check("failed_cherry_pick_reopen_conflicts_hash_cleared_before_close",
        prollyHashIsEmpty(&conflictsBeforeClose));

  sqlite3_close(db);
  db = 0;

  check("reopen_db_for_failed_cherry_pick_reopen", open_db(dbpath, &db)==SQLITE_OK);
  check("failed_cherry_pick_reopen_conflicts_summary_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_conflicts"), "0")==0);
  check("failed_cherry_pick_reopen_conflicts_table_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_conflicts_t"), "0")==0);
  check("failed_cherry_pick_reopen_working_value_after_reopen",
        strcmp(queryScalarText(db, "SELECT v FROM t WHERE id=1"), "main")==0);
  check("failed_cherry_pick_reopen_branch_after_reopen",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);

  doltliteGetSessionStaged(db, &stagedAfterReopen);
  doltliteGetSessionMergeState(db, &isMergingAfterReopen,
                               &mergeAfterReopen, &conflictsAfterReopen);
  check("failed_cherry_pick_reopen_staged_hash_matches_after_reopen",
        memcmp(&stagedAfterReopen, &stagedBeforeClose, sizeof(ProllyHash))==0);
  check("failed_cherry_pick_reopen_merging_flag_matches_after_reopen",
        isMergingAfterReopen==0);
  check("failed_cherry_pick_reopen_merge_hash_matches_after_reopen",
        memcmp(&mergeAfterReopen, &mergeBeforeClose, sizeof(ProllyHash))==0);
  check("failed_cherry_pick_reopen_conflicts_hash_matches_after_reopen",
        prollyHashIsEmpty(&conflictsAfterReopen));

  sqlite3_close(db);
  removeDbFiles(dbpath);
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
  removeDbFiles(dbpath);

  check("open_db", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo", execSql(db,
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
    for(i=0; i<cs.refs.nBranches; i++){
      if( cs.refs.aBranches[i].zName && strcmp(cs.refs.aBranches[i].zName, "feature")==0 ){
        iFeature = i;
        break;
      }
    }
  }
  check("have_feature_branch", iFeature >= 0);
  if( iFeature >= 0 ){
    memcpy(&cs.refs.aBranches[iFeature].commitHash, &badHash, sizeof(ProllyHash));
    memcpy(&cs.refs.aBranches[iFeature].workingSetHash, &badHash, sizeof(ProllyHash));
  }
  check("serialize_corrupt_branch_refs", chunkStoreSerializeRefs(&cs)==SQLITE_OK);
  check("commit_corrupt_branch_refs", chunkStoreCommit(&cs)==SQLITE_OK);
  chunkStoreUnlock(&cs);
  chunkStoreClose(&cs);

  check("reopen_db", open_db(dbpath, &db)==SQLITE_OK);
  rc = execSqlSilent(db,
    "SELECT latest_commit_message FROM dolt_branches WHERE name='feature';");
  check("branches_latest_commit_surfaces_corruption", rc!=SQLITE_OK);
  rc = execSqlSilent(db,
    "SELECT dirty FROM dolt_branches WHERE name='feature';");
  check("branches_dirty_surfaces_corruption", rc!=SQLITE_OK);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_gc_rewrite_failure(void){
  sqlite3 *db = 0;
  sqlite3 *db2 = 0;
  char dbpath[256];
  const char *res;

  printf("=== GC Rewrite Failure Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_gc_rewrite_failure");
  removeDbFiles(dbpath);

  gFailSyncOnce = 0;
  gFailHits = 0;
  check("register_fail_vfs_for_gc", registerFailVfs()==SQLITE_OK);
  check("open_fail_db", open_fail_db(dbpath, &db)==SQLITE_OK);
  check("setup_gc_repo", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "INSERT INTO t VALUES(2,'b');"
    "SELECT dolt_commit('-A', '-m', 'second');")==SQLITE_OK);

  gFailHits = 0;
  gFailSyncOnce = 1;
  res = queryScalarText(db, "SELECT dolt_gc()");
  check("gc_failure_was_injected", gFailHits>0);
  check("gc_returns_error_on_rewrite_failure", strstr(res, "ERROR:")!=0);
  check("gc_connection_still_reads_data",
    strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "2")==0);
  check("gc_connection_still_reads_log",
    strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_log"), "3")==0);

  gFailHits = 0;
  gFailOpenMainOnce = 3;
  res = queryScalarText(db, "SELECT dolt_gc()");
  check("gc_post_replace_open_failure_was_injected", gFailHits>0);
  check("gc_post_replace_open_failure_returns_error", strstr(res, "ERROR:")!=0);
  gFailOpenMainOnce = 0;
  check("gc_post_replace_connection_can_continue",
    execSql(db,
      "INSERT INTO t VALUES(3,'c');"
      "SELECT dolt_commit('-A', '-m', 'third');")==SQLITE_OK);
  check("gc_post_replace_connection_reads_data",
    strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "3")==0);
  check("gc_post_replace_connection_reads_log",
    strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_log"), "4")==0);

  sqlite3_close(db);
  check("reopen_db_after_gc_failure", open_db(dbpath, &db2)==SQLITE_OK);
  check("gc_reopen_reads_data",
    strcmp(queryScalarText(db2, "SELECT count(*) FROM t"), "3")==0);
  check("gc_reopen_reads_log",
    strcmp(queryScalarText(db2, "SELECT count(*) FROM dolt_log"), "4")==0);

  sqlite3_close(db2);
  removeDbFiles(dbpath);
}

/* The header-size varint is part of the header it measures, so adding it can
** push the size across a varint width. With 127 single-byte type codes the
** header reaches 128, which no longer fits the one byte that was reserved for
** it; the record was then built one byte short and would not parse back. */
static void run_record_header_varint_boundary(void){
  static const int aField[] = { 125, 126, 127, 128, 129, 253, 254, 255 };
  int k;

  for(k=0; k<(int)(sizeof(aField)/sizeof(aField[0])); k++){
    int nField = aField[k];
    DoltliteSerialValue *aMem;
    DoltliteRecordInfo info;
    u8 *pRec;
    int nRec = 0;
    int i;
    int ok;
    char zName[128];

    aMem = sqlite3_malloc((int)sizeof(*aMem) * nField);
    if( !aMem ){
      check("record_header_boundary_alloc", 0);
      return;
    }
    for(i=0; i<nField; i++){
      memset(&aMem[i], 0, sizeof(aMem[i]));
      aMem[i].eType = SQLITE_INTEGER;
      /* 2..127 all encode as serial type 1: one header byte, one body byte.
      ** 128 would need type 2, which would change the header arithmetic. */
      aMem[i].i = (i % 126) + 2;
    }

    pRec = doltliteBuildRecord(aMem, nField, &nRec);
    memset(&info, 0, sizeof(info));
    ok = pRec!=0
      && doltliteParseRecordStrict(pRec, nRec, &info)==SQLITE_OK
      && info.nField==nField;
    if( ok ){
      for(i=0; i<nField; i++){
        if( info.aType[i]!=1 ){ ok = 0; break; }
      }
    }
    sqlite3_snprintf((int)sizeof(zName), zName,
                     "record_header_varint_boundary_%d_fields", nField);
    check(zName, ok);
    sqlite3_free(pRec);
    sqlite3_free(aMem);
  }
}

/* csRefArrayGrow computed (n+1)*stride in int. At stride 72 and n 59652416 that
** wraps to 6728 where the true size is 4294974024, so realloc succeeds with a
** 6KB buffer and the zeroing memset writes 72 bytes at offset 4294973952. The
** guard has to reject the request instead of allocating the wrapped size. */
static void run_ref_array_grow_rejects_overflow(void){
  void *aBase = 0;
  int rc;

  rc = csRefArrayGrow(&aBase, 59652416, 72);
  check("ref_array_grow_rejects_wrapping_size", rc!=SQLITE_OK);
  check("ref_array_grow_left_the_array_alone", aBase==0);

  /* An ordinary growth still works. */
  rc = csRefArrayGrow(&aBase, 0, 72);
  check("ref_array_grow_still_grows", rc==SQLITE_OK && aBase!=0);
  if( aBase ){
    int i;
    int allZero = 1;
    for(i=0; i<72; i++){
      if( ((unsigned char*)aBase)[i]!=0 ) allZero = 0;
    }
    check("ref_array_grow_zeroes_the_new_slot", allZero);
  }
  sqlite3_free(aBase);
}

static void run_record_decode_corruption(void){
  static const u8 badRecord[] = {
    0x05, 0x01
  };
  static const u8 serialTypeOverflow[] = {
    0x06, 0x90, 0x80, 0x80, 0x80, 0x0e, 0x00
  };
  static const u8 headerSizeOverflow[] = {
    0x90, 0x80, 0x80, 0x80, 0x06, 0x08
  };
  static const u8 maxVarintSerialType[] = {
    0x0a, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff
  };
  static const u8 validBlob[] = { 0x02, 0x0e, 0x2a };
  static const u8 validText[] = { 0x02, 0x0f, 0x78 };
  DoltliteRecordInfo info;
  char *z;
  int rc;

  printf("=== Record Decode Corruption Test ===\n\n");
  z = doltliteDecodeRecord(badRecord, (int)sizeof(badRecord));
  check("corrupt_record_decodes_to_null", z==0);
  sqlite3_free(z);

  check("decode_null_record_rejected", doltliteDecodeRecord(0, 1)==0);
  check("decode_zero_length_record_rejected",
        doltliteDecodeRecord(badRecord, 0)==0);
  check("decode_negative_length_record_rejected",
        doltliteDecodeRecord(badRecord, -1)==0);

  memset(&info, 0xff, sizeof(info));
  rc = doltliteParseRecordStrict(0, 1, &info);
  check("parse_null_record_rejected", rc==SQLITE_CORRUPT && info.nField==0);
  memset(&info, 0xff, sizeof(info));
  rc = doltliteParseRecordStrict(badRecord, 0, &info);
  check("parse_zero_length_record_rejected",
        rc==SQLITE_CORRUPT && info.nField==0);
  memset(&info, 0xff, sizeof(info));
  rc = doltliteParseRecordStrict(badRecord, -1, &info);
  check("parse_negative_length_record_rejected",
        rc==SQLITE_CORRUPT && info.nField==0);

  check("oversized_serial_type_is_corrupt",
        doltliteParseRecordStrict(serialTypeOverflow,
          (int)sizeof(serialTypeOverflow), &info)==SQLITE_CORRUPT);
  z = doltliteDecodeRecord(serialTypeOverflow,
                           (int)sizeof(serialTypeOverflow));
  check("oversized_serial_type_does_not_decode", z==0);
  sqlite3_free(z);

  check("oversized_header_size_is_corrupt",
        doltliteParseRecordStrict(headerSizeOverflow,
          (int)sizeof(headerSizeOverflow), &info)==SQLITE_CORRUPT);
  z = doltliteDecodeRecord(headerSizeOverflow,
                           (int)sizeof(headerSizeOverflow));
  check("oversized_header_size_does_not_decode", z==0);
  sqlite3_free(z);

  check("maximum_varint_serial_type_is_corrupt",
        doltliteParseRecordStrict(maxVarintSerialType,
          (int)sizeof(maxVarintSerialType), &info)==SQLITE_CORRUPT);
  check("largest_serial_payload_length_fits",
        dlSerialTypeLen((u64)INT_MAX * 2 + 12)==INT_MAX);
  check("oversized_serial_payload_length_is_rejected",
        dlSerialTypeLen((u64)INT_MAX * 2 + 14)<0);

  check("neighboring_blob_record_is_valid",
        doltliteParseRecordStrict(validBlob,
          (int)sizeof(validBlob), &info)==SQLITE_OK
        && info.nField==1 && info.aType[0]==14 && info.aOffset[0]==2);
  check("neighboring_text_record_is_valid",
        doltliteParseRecordStrict(validText,
          (int)sizeof(validText), &info)==SQLITE_OK
        && info.nField==1 && info.aType[0]==15 && info.aOffset[0]==2);
}

static void run_sortkey_two_numeric_roundtrip(void){
  static const u8 record[] = {
    0x03,
    0x01,
    0x03,
    0x7b,
    0x06, 0xf8, 0x55
  };
  u8 *pSortKey = 0;
  u8 *pRoundTrip = 0;
  int nSortKey = 0;
  int nRoundTrip = 0;
  int rc;

  printf("=== Sortkey Two Numeric Roundtrip Test ===\n\n");

  rc = sortKeyFromRecord(record, (int)sizeof(record), &pSortKey, &nSortKey);
  check("two_numeric_sortkey_encode_ok", rc==SQLITE_OK);
  check("two_numeric_sortkey_has_expected_width", nSortKey==18);

  if( rc==SQLITE_OK ){
    rc = recordFromSortKey(pSortKey, nSortKey, &pRoundTrip, &nRoundTrip);
    check("two_numeric_sortkey_decode_ok", rc==SQLITE_OK);
    check("two_numeric_sortkey_roundtrips",
      nRoundTrip==(int)sizeof(record)
      && memcmp(pRoundTrip, record, sizeof(record))==0);
  }

  sqlite3_free(pSortKey);
  sqlite3_free(pRoundTrip);
}

static void run_sortkey_numeric_text_roundtrip(void){
  static const u8 fastRecord[] = {
    0x03,
    0x01,
    0x13,
    0x7b,
    'a', 'b', 'c'
  };
  static const u8 escapedRecord[] = {
    0x03,
    0x01,
    0x13,
    0x7b,
    'x', 0x00, 'y'
  };
  const u8 *aRecord[] = { fastRecord, escapedRecord };
  int aRecordSize[] = { (int)sizeof(fastRecord), (int)sizeof(escapedRecord) };
  const char *aName[] = { "numeric_text_fast", "numeric_text_escaped" };
  int i;

  printf("=== Sortkey Numeric Text Roundtrip Test ===\n\n");

  for(i=0; i<2; i++){
    u8 *pSortKey = 0;
    u8 *pRoundTrip = 0;
    char zCheck[80];
    int nSortKey = 0;
    int nRoundTrip = 0;
    int rc;

    rc = sortKeyFromRecord(aRecord[i], aRecordSize[i], &pSortKey, &nSortKey);
    sqlite3_snprintf(sizeof(zCheck), zCheck, "%s_sortkey_encode_ok", aName[i]);
    check(zCheck, rc==SQLITE_OK);

    if( rc==SQLITE_OK ){
      rc = recordFromSortKey(pSortKey, nSortKey, &pRoundTrip, &nRoundTrip);
      sqlite3_snprintf(sizeof(zCheck), zCheck, "%s_sortkey_decode_ok", aName[i]);
      check(zCheck, rc==SQLITE_OK);
      sqlite3_snprintf(sizeof(zCheck), zCheck, "%s_sortkey_roundtrips", aName[i]);
      check(zCheck,
        nRoundTrip==aRecordSize[i]
        && memcmp(pRoundTrip, aRecord[i], (size_t)aRecordSize[i])==0);
    }

    sqlite3_free(pSortKey);
    sqlite3_free(pRoundTrip);
  }
}

static void run_sortkey_numeric_blob_roundtrip(void){
  static const u8 fastRecord[] = {
    0x03,
    0x01,
    0x12,
    0x7b,
    0x01, 0x02, 0x03
  };
  static const u8 escapedRecord[] = {
    0x03,
    0x01,
    0x12,
    0x7b,
    0x01, 0x00, 0x03
  };
  const u8 *aRecord[] = { fastRecord, escapedRecord };
  int aRecordSize[] = { (int)sizeof(fastRecord), (int)sizeof(escapedRecord) };
  const char *aName[] = { "numeric_blob_fast", "numeric_blob_escaped" };
  int i;

  printf("=== Sortkey Numeric Blob Roundtrip Test ===\n\n");

  for(i=0; i<2; i++){
    u8 *pSortKey = 0;
    u8 *pRoundTrip = 0;
    char zCheck[80];
    int nSortKey = 0;
    int nRoundTrip = 0;
    int rc;

    rc = sortKeyFromRecord(aRecord[i], aRecordSize[i], &pSortKey, &nSortKey);
    sqlite3_snprintf(sizeof(zCheck), zCheck, "%s_sortkey_encode_ok", aName[i]);
    check(zCheck, rc==SQLITE_OK);

    if( rc==SQLITE_OK ){
      rc = recordFromSortKey(pSortKey, nSortKey, &pRoundTrip, &nRoundTrip);
      sqlite3_snprintf(sizeof(zCheck), zCheck, "%s_sortkey_decode_ok", aName[i]);
      check(zCheck, rc==SQLITE_OK);
      sqlite3_snprintf(sizeof(zCheck), zCheck, "%s_sortkey_roundtrips", aName[i]);
      check(zCheck,
        nRoundTrip==aRecordSize[i]
        && memcmp(pRoundTrip, aRecord[i], (size_t)aRecordSize[i])==0);
    }

    sqlite3_free(pSortKey);
    sqlite3_free(pRoundTrip);
  }
}

static void run_sortkey_buffer_exact_size(void){
  u8 record[73];
  u8 *pSortKey = 0;
  u8 *pRoundTrip = 0;
  int nAlloc = 1;
  int nSortKey = 0;
  int nRoundTrip = 0;
  int rc;
  int i;

  printf("=== Sortkey Buffer Exact Size Test ===\n\n");

  record[0] = 0x03;
  record[1] = 0x81;
  record[2] = 0x18;
  for(i=0; i<70; i++){
    record[3+i] = (u8)('a' + (i % 26));
  }
  record[3+17] = 0x00;
  record[3+53] = 0x00;

  pSortKey = sqlite3_malloc64((sqlite3_uint64)nAlloc);
  check("sortkey_buffer_initial_alloc_ok", pSortKey!=0);
  if( !pSortKey ) return;

  rc = sortKeyFromRecordPrefixCollBuffer(
      record, (int)sizeof(record), 0, 0, &pSortKey, &nAlloc, &nSortKey);
  check("sortkey_buffer_exact_size_encode_ok", rc==SQLITE_OK);
  check("sortkey_buffer_exact_size_output", nSortKey==75);
  check("sortkey_buffer_exact_size_allocation", nAlloc==75);

  if( rc==SQLITE_OK ){
    rc = recordFromSortKey(pSortKey, nSortKey, &pRoundTrip, &nRoundTrip);
    check("sortkey_buffer_exact_size_decode_ok", rc==SQLITE_OK);
    check("sortkey_buffer_exact_size_roundtrips",
      nRoundTrip==(int)sizeof(record)
      && memcmp(pRoundTrip, record, sizeof(record))==0);
  }

  sqlite3_free(pSortKey);
  sqlite3_free(pRoundTrip);
}

static void run_sortkey_mem_matches_record(void){
  static const u8 record[] = {
    0x04,
    0x01,
    0x13,
    0x12,
    0x7b,
    'a', 0x00, 'b',
    0x01, 0x00, 0x03
  };
  Mem aMem[3];
  u8 *pRecordKey = 0;
  u8 *pMemKey = 0;
  u8 *pRecordPrefix = 0;
  u8 *pMemPrefix = 0;
  int nRecordKey = 0;
  int nMemKey = 0;
  int nRecordPrefix = 0;
  int nMemPrefix = 0;
  int nMemAlloc = 0;
  int nMemPrefixAlloc = 0;
  int rc;
  Mem fastMem;
  u8 *pFastKey = 0;
  int nFastKey = 0;
  int nFastAlloc = 0;
  static const u8 aFastExpected[] = {
    SORTKEY_TEXT, 'a', 'b', 'c', 0x00, 0x00
  };

  printf("=== Sortkey Mem Matches Record Test ===\n\n");

  memset(aMem, 0, sizeof(aMem));
  aMem[0].flags = MEM_Int;
  aMem[0].u.i = 123;
  aMem[1].flags = MEM_Str;
  aMem[1].z = "a\000b";
  aMem[1].n = 3;
  aMem[2].flags = MEM_Blob;
  aMem[2].z = "\001\000\003";
  aMem[2].n = 3;

  rc = sortKeyFromRecordPrefixColl(record, (int)sizeof(record), 0, 0,
                                   &pRecordKey, &nRecordKey);
  check("sortkey_mem_record_encode_ok", rc==SQLITE_OK);
  rc = sortKeyFromMemPrefixCollBuffer(aMem, 3, 0, 0,
                                      &pMemKey, &nMemAlloc, &nMemKey);
  check("sortkey_mem_encode_ok", rc==SQLITE_OK);
  check("sortkey_mem_matches_record",
        nMemKey==nRecordKey && memcmp(pMemKey, pRecordKey, nRecordKey)==0);

  rc = sortKeyFromRecordPrefixColl(record, (int)sizeof(record), 1, 0,
                                   &pRecordPrefix, &nRecordPrefix);
  check("sortkey_mem_record_prefix_encode_ok", rc==SQLITE_OK);
  rc = sortKeyFromMemPrefixCollBuffer(aMem, 3, 1, 0,
                                      &pMemPrefix, &nMemPrefixAlloc,
                                      &nMemPrefix);
  check("sortkey_mem_prefix_encode_ok", rc==SQLITE_OK);
  check("sortkey_mem_prefix_matches_record",
        nMemPrefix==nRecordPrefix
        && memcmp(pMemPrefix, pRecordPrefix, nRecordPrefix)==0);

  memset(&fastMem, 0, sizeof(fastMem));
  fastMem.flags = MEM_Str;
  fastMem.z = "abc";
  fastMem.n = 3;
  rc = sortKeyFromMemPrefixCollBuffer(&fastMem, 1, 0, 0,
                                      &pFastKey, &nFastAlloc, &nFastKey);
  check("sortkey_mem_single_binary_text_fast_ok", rc==SQLITE_OK);
  check("sortkey_mem_single_binary_text_fast_output",
        nFastKey==(int)sizeof(aFastExpected)
        && memcmp(pFastKey, aFastExpected, sizeof(aFastExpected))==0);
  check("sortkey_mem_single_binary_text_fast_alloc", nFastAlloc==64);

  sqlite3_free(pRecordKey);
  sqlite3_free(pMemKey);
  sqlite3_free(pRecordPrefix);
  sqlite3_free(pMemPrefix);
  sqlite3_free(pFastKey);
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
  nBranchesBefore = cs.refs.nBranches;
  zDefaultBefore = sqlite3_mprintf("%s", cs.refs.zDefaultBranch ? cs.refs.zDefaultBranch : "");
  zBranchBefore = (cs.refs.aBranches && cs.refs.nBranches>0 && cs.refs.aBranches[0].zName)
                ? sqlite3_mprintf("%s", cs.refs.aBranches[0].zName)
                : sqlite3_mprintf("");

  check("load_bad_refs_blob_as_chunk",
        chunkStorePut(&cs, badBlob, (int)sizeof(badBlob), &badHash)==SQLITE_OK);

  rc = chunkStoreLoadRefsFromBlob(&cs, badBlob, (int)sizeof(badBlob));
  check("load_refs_blob_returns_corrupt", rc==SQLITE_CORRUPT);
  check("load_refs_blob_preserves_default_branch",
        cs.refs.zDefaultBranch && strcmp(cs.refs.zDefaultBranch, zDefaultBefore)==0);
  check("load_refs_blob_preserves_branch_count", cs.refs.nBranches==nBranchesBefore);
  check("load_refs_blob_preserves_branch_name",
        nBranchesBefore==0 ||
        (cs.refs.aBranches && cs.refs.aBranches[0].zName
         && strcmp(cs.refs.aBranches[0].zName, zBranchBefore)==0));

  memcpy(&cs.refs.refsHash, &badHash, sizeof(badHash));
  rc = chunkStoreReloadRefs(&cs);
  check("reload_refs_returns_corrupt", rc==SQLITE_CORRUPT);
  check("reload_refs_preserves_default_branch",
        cs.refs.zDefaultBranch && strcmp(cs.refs.zDefaultBranch, zDefaultBefore)==0);
  check("reload_refs_preserves_branch_count", cs.refs.nBranches==nBranchesBefore);
  check("reload_refs_preserves_branch_name",
        nBranchesBefore==0 ||
        (cs.refs.aBranches && cs.refs.aBranches[0].zName
         && strcmp(cs.refs.aBranches[0].zName, zBranchBefore)==0));

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
  removeDbFiles(dbpath);

  check("open_db", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);
  sqlite3_close(db);
  db = 0;

  check("open_store_1", chunkStoreOpen(&cs1, sqlite3_vfs_find(0), dbpath,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  refsHashBefore = cs1.refs.refsHash;
  nBranchesBefore = cs1.refs.nBranches;
  zDefaultBefore = sqlite3_mprintf("%s", cs1.refs.zDefaultBranch ? cs1.refs.zDefaultBranch : "");
  zBranchBefore = (cs1.refs.aBranches && cs1.refs.nBranches>0 && cs1.refs.aBranches[0].zName)
                ? sqlite3_mprintf("%s", cs1.refs.aBranches[0].zName)
                : sqlite3_mprintf("");

  check("open_store_2", chunkStoreOpen(&cs2, sqlite3_vfs_find(0), dbpath,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  check("lock_store_2", chunkStoreLockAndRefresh(&cs2)==SQLITE_OK);
  check("put_bad_refs_chunk",
        chunkStorePut(&cs2, badBlob, (int)sizeof(badBlob), &badHash)==SQLITE_OK);
  memcpy(&cs2.refs.refsHash, &badHash, sizeof(badHash));
  check("commit_bad_refs_hash", chunkStoreCommit(&cs2)==SQLITE_OK);
  chunkStoreUnlock(&cs2);
  chunkStoreClose(&cs2);

  rc = chunkStoreRefreshIfChanged(&cs1, &changed);
  check("refresh_returns_error_for_corrupt_refs", rc==SQLITE_CORRUPT);
  check("refresh_does_not_mark_changed", changed==0);
  check("refresh_preserves_default_branch",
        cs1.refs.zDefaultBranch && strcmp(cs1.refs.zDefaultBranch, zDefaultBefore)==0);
  check("refresh_preserves_branch_count", cs1.refs.nBranches==nBranchesBefore);
  check("refresh_preserves_branch_name",
        nBranchesBefore==0 ||
        (cs1.refs.aBranches && cs1.refs.aBranches[0].zName
         && strcmp(cs1.refs.aBranches[0].zName, zBranchBefore)==0));
  check("refresh_preserves_refs_hash",
        memcmp(&cs1.refs.refsHash, &refsHashBefore, sizeof(ProllyHash))==0);
  sqlite3_free(zDefaultBefore);
  sqlite3_free(zBranchBefore);
  chunkStoreClose(&cs1);
  removeDbFiles(dbpath);
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
  check("have_committed_index_entry", cs.index.nIndex > 0);
  if( cs.index.nIndex > 0 ){
    cs.index.aIndex[0].offset = cs.staging.nWriteBuf + 100;
  }
  rc = chunkStoreGet(&cs, &h, &pOut, &nOut);
  check("memory_lookup_corruption_returns_corrupt", rc==SQLITE_CORRUPT);
  sqlite3_free(pOut);
  chunkStoreClose(&cs);
}

/* A remote that answers every fetch with bytes that are not the chunk asked
** for. doltliteSyncChunks must reject the payload rather than store it under
** its own address -- which would leave the requested address absent -- and must
** not walk it for children. */
typedef struct LyingRemote LyingRemote;
struct LyingRemote {
  DoltliteRemote base;
  int nServed;
};

/* Serves a well-formed leaf node, just not the one requested. Unparseable bytes
** would not test anything: syncEnqueueChildren rejects those on its own. A
** valid node at the wrong address is the case that used to pass silently. */
static int lyingGetChunk(DoltliteRemote *pRemote, const ProllyHash *pHash,
                         u8 **ppData, int *pnData){
  LyingRemote *p = (LyingRemote*)pRemote;
  ProllyNodeBuilder b;
  u8 *pData = 0;
  int nData = 0;
  int rc;
  (void)pHash;

  prollyNodeBuilderInit(&b, 0, 0);
  rc = prollyNodeBuilderAdd(&b, (const u8*)"k", 1, (const u8*)"v", 1);
  if( rc==SQLITE_OK ) rc = prollyNodeBuilderFinish(&b, &pData, &nData);
  prollyNodeBuilderFree(&b);
  if( rc!=SQLITE_OK ){
    sqlite3_free(pData);
    return rc;
  }
  *ppData = pData;
  *pnData = nData;
  p->nServed++;
  return SQLITE_OK;
}

static int lyingHasChunks(DoltliteRemote *pRemote, const ProllyHash *aHash,
                          int nHash, u8 *aResult){
  int i;
  (void)pRemote; (void)aHash;
  for(i=0; i<nHash; i++) aResult[i] = 0;
  return SQLITE_OK;
}

typedef struct CountingSink CountingSink;
struct CountingSink {
  DoltliteRemote base;
  int nPut;
};

static int sinkPutChunk(DoltliteRemote *pRemote, const ProllyHash *pHash,
                        const u8 *pData, int nData){
  (void)pHash; (void)pData; (void)nData;
  ((CountingSink*)pRemote)->nPut++;
  return SQLITE_OK;
}

static int sinkHasChunks(DoltliteRemote *pRemote, const ProllyHash *aHash,
                         int nHash, u8 *aResult){
  int i;
  (void)pRemote; (void)aHash;
  for(i=0; i<nHash; i++) aResult[i] = 0;
  return SQLITE_OK;
}

static void run_sync_rejects_wrong_chunk(void){
  LyingRemote src;
  CountingSink dst;
  ProllyHash root;
  int rc;

  memset(&src, 0, sizeof(src));
  src.base.xGetChunk = lyingGetChunk;
  src.base.xHasChunks = lyingHasChunks;
  memset(&dst, 0, sizeof(dst));
  dst.base.xPutChunk = sinkPutChunk;
  dst.base.xHasChunks = sinkHasChunks;

  /* Any non-empty address the served payload will not hash to. */
  memset(&root, 0, sizeof(root));
  root.data[0] = 0xab;
  root.data[1] = 0xcd;

  rc = doltliteSyncChunks(&src.base, &dst.base, &root, 1);
  /* The payload must not reach the store: writing it puts the content at its
  ** own address and leaves the requested one absent, which only shows up much
  ** later as a NOTFOUND on a ref that looks fine. */
  check("sync_does_not_store_a_chunk_from_the_wrong_address", dst.nPut==0);
  check("sync_reports_the_wrong_address_as_corrupt", rc!=SQLITE_OK);
  check("sync_stopped_after_the_first_bad_chunk", src.nServed==1);
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
  removeDbFiles(dbpath);

  check("open_db_repo_state", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_state", execSql(db,
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
  removeDbFiles(dbpath);
}

static void run_integrity_check_session_merge_state(void){
  sqlite3 *db = 0;
  char dbpath[256];
  ProllyHash badHash;
  int nErr = 0;
  int rc;

  printf("=== Integrity Check Session Merge State Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_integrity_check_session_merge_state");
  removeDbFiles(dbpath);

  check("open_db_session_merge_state", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_session_merge_state", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);

  memset(&badHash, 0x4c, sizeof(badHash));
  doltliteSetSessionMergeState(db, 1, &badHash, &badHash);

  rc = doltliteCheckRepoGraphIntegrity(db->aDb[0].pBt, 100, &nErr);
  check("session_merge_state_integrity_call_succeeds", rc==SQLITE_OK);
  check("integrity_check_reports_session_merge_state_corruption", nErr>0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_prepared_stmt_reuse_after_commit(void){
  sqlite3 *db = 0;
  sqlite3_stmt *stmt = 0;
  char dbpath[256];
  int rc;

  printf("=== Prepared Statement Reuse After Commit Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_prepared_stmt_reuse_after_commit");
  removeDbFiles(dbpath);

  check("open_db_stmt_commit", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_stmt_commit", execSql(db,
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

    check("commit_new_row_same_conn", execSql(db,
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
  removeDbFiles(dbpath);
}

static void run_prepared_stmt_reuse_after_schema_checkout(void){
  sqlite3 *db = 0;
  sqlite3_stmt *stmt = 0;
  char dbpath[256];
  int rc;

  printf("=== Prepared Statement Reuse After Schema Checkout Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_prepared_stmt_reuse_after_schema_checkout");
  removeDbFiles(dbpath);

  check("open_db_stmt_checkout", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_stmt_checkout", execSql(db,
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

    check("checkout_schema_branch", execSql(db,
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
  removeDbFiles(dbpath);
}

/* A session that refreshes after a peer commit adopts the peer's catalog;
** it must adopt that snapshot's branch head with it. Holding the old head
** makes every later working-set persist record a commit the branch no
** longer points at, and the load gate discards such a blob -- so rows this
** session durably wrote vanish for every future connection. */
/* Persisting a working set is the one ref install with no compare-and-swap:
** when the branch ref is missing it created one. A session whose branch a
** peer deleted would therefore resurrect it on its next write, restoring a
** ref the peer removed and carrying that session's later commits on it. */
static void run_persist_does_not_resurrect_deleted_branch(void){
  sqlite3 *dbA = 0;
  sqlite3 *dbB = 0;
  sqlite3 *dbC = 0;
  char dbpath[256];

  printf("=== Persist Does Not Resurrect Deleted Branch Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_persist_no_resurrect");
  removeDbFiles(dbpath);

  check("nr_open_A", open_db(dbpath, &dbA)==SQLITE_OK);
  check("nr_seed", execSql(dbA,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'one');"
    "SELECT dolt_commit('-A','-m','base');"
    "SELECT dolt_branch('feat');")==SQLITE_OK);
  check("nr_checkout_feat",
        execSql(dbA, "SELECT dolt_checkout('feat');")==SQLITE_OK);
  check("nr_commit_on_feat", execSql(dbA,
    "INSERT INTO t VALUES(2,'two');"
    "SELECT dolt_commit('-A','-m','feat work');")==SQLITE_OK);

  /* A peer deletes the branch this session is on. */
  check("nr_open_B", open_db(dbpath, &dbB)==SQLITE_OK);
  check("nr_peer_delete",
        execSql(dbB, "SELECT dolt_branch('-D','feat');")==SQLITE_OK);
  check("nr_deleted_for_peer",
        strcmp(queryScalarText(dbB,
            "SELECT count(*) FROM dolt_branches WHERE name='feat'"), "0")==0);

  /* The orphaned session's next write must not put the branch back. */
  execSqlSilent(dbA, "INSERT INTO t VALUES(3,'three');");
  execSqlSilent(dbA, "SELECT dolt_commit('-A','-m','after delete');");
  sqlite3_close(dbA);
  dbA = 0;
  check("nr_still_deleted_for_peer",
        strcmp(queryScalarText(dbB,
            "SELECT count(*) FROM dolt_branches WHERE name='feat'"), "0")==0);
  check("nr_close_B", sqlite3_close(dbB)==SQLITE_OK);
  dbB = 0;

  check("nr_open_C", open_db(dbpath, &dbC)==SQLITE_OK);
  check("nr_still_deleted_for_fresh",
        strcmp(queryScalarText(dbC,
            "SELECT count(*) FROM dolt_branches WHERE name='feat'"), "0")==0);
  check("nr_main_intact",
        strcmp(queryScalarText(dbC,
            "SELECT count(*) FROM dolt_branches WHERE name='main'"), "1")==0);
  check("nr_close_C", sqlite3_close(dbC)==SQLITE_OK);
  removeDbFiles(dbpath);
}

static void run_peer_commit_keeps_local_row_durable(void){
  sqlite3 *dbA = 0;
  sqlite3 *dbB = 0;
  sqlite3 *dbC = 0;
  char dbpath[256];

  printf("=== Peer Commit Keeps Local Row Durable Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_peer_commit_local_row");
  removeDbFiles(dbpath);

  check("pk_open_A", open_db(dbpath, &dbA)==SQLITE_OK);
  check("pk_seed", execSql(dbA,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'one');"
    "SELECT dolt_commit('-A','-m','base');")==SQLITE_OK);

  /* A writes an uncommitted row, then a peer commits on the same branch. */
  check("pk_local_row", execSql(dbA, "INSERT INTO t VALUES(2,'two');")==SQLITE_OK);
  check("pk_open_B", open_db(dbpath, &dbB)==SQLITE_OK);
  check("pk_peer_commit", execSql(dbB,
    "INSERT INTO t VALUES(3,'three');"
    "SELECT dolt_commit('-A','-m','peer');")==SQLITE_OK);
  check("pk_close_B", sqlite3_close(dbB)==SQLITE_OK);
  dbB = 0;

  /* A writes again after the peer landed. Autocommit reported success, so
  ** the row must be there for anyone who opens the database next. */
  check("pk_local_row_after_peer",
        execSql(dbA, "INSERT INTO t VALUES(4,'four');")==SQLITE_OK);
  check("pk_close_A", sqlite3_close(dbA)==SQLITE_OK);
  dbA = 0;

  check("pk_open_C", open_db(dbpath, &dbC)==SQLITE_OK);
  check("pk_row_survived",
        strcmp(queryScalarText(dbC, "SELECT count(*) FROM t WHERE id=4"),
               "1")==0);
  check("pk_close_C", sqlite3_close(dbC)==SQLITE_OK);
  removeDbFiles(dbpath);
}

/* The same stale head is what dolt_commit compares against, so a session
** that outlives a peer commit could never commit again -- every retry hit
** the conflict error, and only reconnecting escaped it. */
static void run_commit_recovers_after_peer_commit(void){
  sqlite3 *dbA = 0;
  sqlite3 *dbB = 0;
  const char *res;
  char dbpath[256];

  printf("=== Commit Recovers After Peer Commit Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_commit_recovers_after_peer");
  removeDbFiles(dbpath);

  check("cr_open_A", open_db(dbpath, &dbA)==SQLITE_OK);
  check("cr_seed", execSql(dbA,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'one');"
    "SELECT dolt_commit('-A','-m','base');")==SQLITE_OK);

  check("cr_open_B", open_db(dbpath, &dbB)==SQLITE_OK);
  check("cr_peer_commit", execSql(dbB,
    "INSERT INTO t VALUES(2,'two');"
    "SELECT dolt_commit('-A','-m','peer');")==SQLITE_OK);
  check("cr_close_B", sqlite3_close(dbB)==SQLITE_OK);
  dbB = 0;

  check("cr_local_row", execSql(dbA, "INSERT INTO t VALUES(3,'three');")==SQLITE_OK);
  res = queryScalarText(dbA, "SELECT dolt_commit('-A','-m','after peer')");
  check("cr_commit_succeeds", res && strlen(res)==40 && strstr(res, "ERROR")==0);
  check("cr_close_A", sqlite3_close(dbA)==SQLITE_OK);
  removeDbFiles(dbpath);
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
  removeDbFiles(dbpath);

  check("open_db1_for_cross_conn_staged", open_db(dbpath, &db1)==SQLITE_OK);
  check("create_table_for_cross_conn_staged",
        execSql(db1, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);")==SQLITE_OK);
  check("insert_row_for_cross_conn_staged",
        execSql(db1, "INSERT INTO t VALUES(1,'a');")==SQLITE_OK);
  check("initial_commit_for_cross_conn_staged",
        execSql(db1, "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);
  check("open_db2_for_cross_conn_staged", open_db(dbpath, &db2)==SQLITE_OK);

  doltliteGetSessionStaged(db2, &stagedBefore);
  check("db1_stage_new_row", execSql(db1,
    "INSERT INTO t VALUES(2,'b');"
    "SELECT dolt_add('-A');")==SQLITE_OK);
  doltliteGetSessionStaged(db1, &stagedExpected);
  check("staged_hash_changed_after_dolt_add",
        memcmp(&stagedExpected, &stagedBefore, sizeof(ProllyHash))!=0);

  check("db2_refreshes_working_catalog_on_read",
        strcmp(queryScalarText(db2, "SELECT count(*) FROM t"), "2")==0);
  doltliteGetSessionStaged(db2, &stagedAfter);
  check("db2_refreshes_staged_hash",
        memcmp(&stagedAfter, &stagedExpected, sizeof(ProllyHash))==0);

  sqlite3_close(db2);
  sqlite3_close(db1);
  removeDbFiles(dbpath);
}

static void run_reopen_preserves_staged_working_set(void){
  sqlite3 *db = 0;
  char dbpath[256];
  ProllyHash stagedBeforeClose;
  ProllyHash stagedAfterReopen;

  printf("=== Reopen Preserves Staged Working Set Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_reopen_preserves_staged_working_set");
  removeDbFiles(dbpath);

  check("open_db_for_reopen_staged", open_db(dbpath, &db)==SQLITE_OK);
  check("create_table_for_reopen_staged",
        execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);")==SQLITE_OK);
  check("insert_base_row_for_reopen_staged",
        execSql(db, "INSERT INTO t VALUES(1,'a');")==SQLITE_OK);
  check("commit_base_row_for_reopen_staged",
        execSql(db, "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);
  check("stage_new_row_for_reopen_staged",
        execSql(db,
          "INSERT INTO t VALUES(2,'b');"
          "SELECT dolt_add('-A');")==SQLITE_OK);
  check("staged_status_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_status WHERE staged=1"), "1")==0);
  doltliteGetSessionStaged(db, &stagedBeforeClose);
  check("staged_hash_before_close_nonempty",
        !prollyHashIsEmpty(&stagedBeforeClose));
  sqlite3_close(db);
  db = 0;

  check("reopen_db_for_reopen_staged", open_db(dbpath, &db)==SQLITE_OK);
  check("staged_status_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_status WHERE staged=1"), "1")==0);
  doltliteGetSessionStaged(db, &stagedAfterReopen);
  check("staged_hash_after_reopen_matches",
        memcmp(&stagedAfterReopen, &stagedBeforeClose, sizeof(ProllyHash))==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
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
  removeDbFiles(dbpath);

  check("open_db1_for_begin_write_refresh", open_db(dbpath, &db1)==SQLITE_OK);
  check("create_table_for_begin_write_refresh",
        execSql(db1, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);")==SQLITE_OK);
  check("insert_row_for_begin_write_refresh",
        execSql(db1, "INSERT INTO t VALUES(1,'a');")==SQLITE_OK);
  check("initial_commit_for_begin_write_refresh",
        execSql(db1, "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);
  check("open_db2_for_begin_write_refresh", open_db(dbpath, &db2)==SQLITE_OK);

  doltliteGetSessionStaged(db2, &stagedBefore);
  check("db1_prepare_new_working_state", execSql(db1,
    "INSERT INTO t VALUES(2,'b');"
    "SELECT dolt_add('-A');")==SQLITE_OK);
  doltliteGetSessionStaged(db1, &stagedExpected);
  memset(&mergeExpected, 0x55, sizeof(mergeExpected));
  /* The conflicts catalog stays empty: a non-empty one is never written to disk,
  ** so no connection can refresh one from disk and a distinctive value here would
  ** simply never arrive. The merge flag and merge commit still prove the refresh. */
  memset(&conflictsExpected, 0, sizeof(conflictsExpected));
  doltliteSetSessionMergeState(db1, 1, &mergeExpected, &conflictsExpected);
  check("persist_merge_state_for_begin_write_refresh",
        persist_working_set(db1)==SQLITE_OK);

  check("db2_staged_state_is_initially_stale",
        memcmp(&stagedBefore, &stagedExpected, sizeof(ProllyHash))!=0);
  check("db2_begin_immediate_refreshes_branch_state",
        execSql(db2, "BEGIN IMMEDIATE;")==SQLITE_OK);
  check("db2_sees_latest_working_rows_in_write_txn",
        strcmp(queryScalarText(db2, "SELECT count(*) FROM t"), "2")==0);
  doltliteGetSessionStaged(db2, &stagedAfter);
  doltliteGetSessionMergeState(db2, &isMergingAfter, &mergeAfter, &conflictsAfter);
  check("begin_write_refreshes_staged_hash",
        memcmp(&stagedAfter, &stagedExpected, sizeof(ProllyHash))==0);
  check("begin_write_refreshes_merge_flag", isMergingAfter==1);
  check("begin_write_refreshes_merge_commit",
        memcmp(&mergeAfter, &mergeExpected, sizeof(ProllyHash))==0);
  check("begin_write_refreshes_conflicts_catalog_empty",
        memcmp(&conflictsAfter, &conflictsExpected, sizeof(ProllyHash))==0);
  check("rollback_begin_write_refresh_txn", execSql(db2, "ROLLBACK;")==SQLITE_OK);

  sqlite3_close(db2);
  sqlite3_close(db1);
  removeDbFiles(dbpath);
}

static void run_begin_write_from_stale_read_snapshot(void){
  sqlite3 *db1 = 0;
  sqlite3 *db2 = 0;
  sqlite3_stmt *pRead = 0;
  char dbpath[256];
  int rc;

  printf("=== Begin Write From Stale Read Snapshot Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_begin_write_from_stale_read_snapshot");
  removeDbFiles(dbpath);

  check("open_db1_for_stale_snapshot", open_db(dbpath, &db1)==SQLITE_OK);
  check("create_table_for_stale_snapshot",
        execSql(db1, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);")==SQLITE_OK);
  check("insert_row_for_stale_snapshot",
        execSql(db1, "INSERT INTO t VALUES(1,'a');")==SQLITE_OK);
  check("open_db2_for_stale_snapshot", open_db(dbpath, &db2)==SQLITE_OK);

  check("begin_read_txn_for_stale_snapshot", execSql(db2, "BEGIN;")==SQLITE_OK);
  check("prepare_read_in_stale_snapshot",
        sqlite3_prepare_v2(db2, "SELECT count(*) FROM t", -1, &pRead, 0)==SQLITE_OK);
  check("step_read_in_stale_snapshot", sqlite3_step(pRead)==SQLITE_ROW);
  check("read_in_stale_snapshot", sqlite3_column_int(pRead, 0)==1);
  check("db1_autocommit_change_after_read_snapshot",
        execSql(db1, "INSERT INTO t VALUES(2,'b');")==SQLITE_OK);

  rc = execSqlSilent(db2, "INSERT INTO t VALUES(3,'c');");
  check("write_upgrade_fails", rc!=SQLITE_OK);
  check("write_upgrade_returns_busy_snapshot",
        sqlite3_extended_errcode(db2)==SQLITE_BUSY_SNAPSHOT);
  sqlite3_finalize(pRead);
  check("rollback_stale_snapshot_txn", execSql(db2, "ROLLBACK;")==SQLITE_OK);
  check("stale_snapshot_did_not_overwrite_rows",
        strcmp(queryScalarText(db1, "SELECT count(*) FROM t"), "2")==0);

  sqlite3_close(db2);
  sqlite3_close(db1);
  removeDbFiles(dbpath);
}

static void run_open_rejects_corrupt_working_set(void){
  sqlite3 *db = 0;
  sqlite3 *db2 = 0;
  ChunkStore cs;
  char dbpath[256];
  int stmtRc;
  ProllyHash badHash;
  int rc;
  unsigned char badBlob[WS_TOTAL_SIZE-1];

  printf("=== Open Rejects Corrupt Working Set Test ===\n\n");
  memset(badBlob, 0, sizeof(badBlob));
  badBlob[0] = WS_FORMAT_VERSION_V5;
  make_dbpath(dbpath, sizeof(dbpath), "test_open_rejects_corrupt_working_set");
  removeDbFiles(dbpath);

  check("open_db_for_corrupt_working_set", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_corrupt_working_set", execSql(db,
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
  stmtRc = db2 ? execSqlSilent(db2, "SELECT count(*) FROM sqlite_master;") : rc;
  check("open_or_first_statement_returns_corrupt_for_bad_working_set",
        rc==SQLITE_CORRUPT || stmtRc==SQLITE_CORRUPT);
  if( db2 ) sqlite3_close(db2);
  removeDbFiles(dbpath);
}

static int loadWorkingSetBytes(const u8 *data, int nData){
  ChunkStore cs;
  ProllyHash emptyHash;
  ProllyHash wsHash;
  int rc;

  memset(&cs, 0, sizeof(cs));
  memset(&emptyHash, 0, sizeof(emptyHash));
  rc = chunkStoreOpen(&cs, sqlite3_vfs_find(0), ":memory:", 0);
  if( rc==SQLITE_OK ) rc = chunkStorePut(&cs, data, nData, &wsHash);
  if( rc==SQLITE_OK ) rc = chunkStoreAddBranch(&cs, "main", &emptyHash);
  if( rc==SQLITE_OK ){
    rc = chunkStoreSetBranchWorkingSet(&cs, "main", &wsHash);
  }
  if( rc==SQLITE_OK ){
    rc = btreeReadWorkingCatalog(&cs, "main", 0, 0);
  }
  chunkStoreClose(&cs);
  return rc;
}

static void run_working_set_blob_size_validation(void){
  static const struct {
    int version;
    int size;
  } aCase[] = {
    { WS_FORMAT_VERSION_V2, WS_TOTAL_SIZE_V2 },
    { WS_FORMAT_VERSION_V3, WS_TOTAL_SIZE_V3 },
    { WS_FORMAT_VERSION_V4, WS_TOTAL_SIZE_V4 },
    { WS_FORMAT_VERSION_V5, WS_TOTAL_SIZE }
  };
  u8 data[WS_TOTAL_SIZE+1];
  int i;

  printf("=== Working Set Blob Size Validation Test ===\n\n");
  for(i=0; i<(int)(sizeof(aCase)/sizeof(aCase[0])); i++){
    char zName[80];
    memset(data, 0, sizeof(data));
    data[0] = (u8)aCase[i].version;
    sqlite3_snprintf(sizeof(zName), zName,
                     "working_set_v%d_exact_size", aCase[i].version);
    check(zName, loadWorkingSetBytes(data, aCase[i].size)==SQLITE_OK);
    sqlite3_snprintf(sizeof(zName), zName,
                     "working_set_v%d_short_rejected", aCase[i].version);
    check(zName,
          loadWorkingSetBytes(data, aCase[i].size-1)==SQLITE_CORRUPT);
    sqlite3_snprintf(sizeof(zName), zName,
                     "working_set_v%d_trailing_rejected", aCase[i].version);
    check(zName,
          loadWorkingSetBytes(data, aCase[i].size+1)==SQLITE_CORRUPT);
  }
}

static void run_open_ignores_stale_working_set(void){
  sqlite3 *db = 0;
  sqlite3 *db2 = 0;
  sqlite3 *db3 = 0;
  ChunkStore cs;
  ProllyHash staleWorkingSet;
  char dbpath[256];

  printf("=== Open Ignores Stale Working Set Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_open_ignores_stale_working_set");
  removeDbFiles(dbpath);

  check("open_db_for_stale_working_set", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_stale_working_set", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'one');"
    "SELECT dolt_commit('-A', '-m', 'first');")==SQLITE_OK);
  check("capture_stale_working_set",
        chunkStoreGetBranchWorkingSet(doltliteGetChunkStore(db), "main",
                                      &staleWorkingSet)==SQLITE_OK);
  check("open_peer_before_head_advance", open_db(dbpath, &db2)==SQLITE_OK);
  check("advance_head_past_stale_working_set", execSql(db,
    "INSERT INTO t VALUES(2,'two');"
    "SELECT dolt_commit('-A', '-m', 'second');")==SQLITE_OK);
  sqlite3_close(db);
  db = 0;

  check("open_store_to_restore_stale_working_set",
        chunkStoreOpen(&cs, sqlite3_vfs_find(0), dbpath,
          SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  check("restore_stale_working_set",
        chunkStoreSetBranchWorkingSet(&cs, "main", &staleWorkingSet)==SQLITE_OK);
  check("serialize_stale_working_set_refs",
        chunkStoreSerializeRefs(&cs)==SQLITE_OK);
  check("commit_stale_working_set_refs", chunkStoreCommit(&cs)==SQLITE_OK);
  chunkStoreClose(&cs);

  check("peer_refresh_follows_head_past_stale_working_set",
        strcmp(queryScalarText(db2, "SELECT count(*) FROM t"), "2")==0);
  sqlite3_close(db2);

  check("reopen_db_with_stale_working_set", open_db(dbpath, &db3)==SQLITE_OK);
  check("head_retains_second_commit",
        strcmp(queryScalarText(db3, "SELECT message FROM dolt_log LIMIT 1"),
               "second")==0);
  check("live_catalog_follows_head_past_stale_working_set",
        strcmp(queryScalarText(db3, "SELECT count(*) FROM t"), "2")==0);

  sqlite3_close(db3);
  removeDbFiles(dbpath);
}

static void run_diff_stat_requires_refs(void){
  sqlite3 *db = 0;
  char dbpath[256];
  const char *res;

  printf("=== Diff Stat Requires Refs Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_diff_stat_requires_refs");
  removeDbFiles(dbpath);

  check("open_db_for_diff_stat_requires_refs", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_diff_stat_requires_refs", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);

  res = queryScalarText(db, "SELECT count(*) FROM dolt_diff_stat('HEAD');");
  check("diff_stat_missing_to_ref_returns_error", strstr(res, "ERROR:")!=0);
  res = queryScalarText(db, "SELECT count(*) FROM dolt_diff_summary('HEAD');");
  check("diff_summary_missing_to_ref_returns_error", strstr(res, "ERROR:")!=0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_diff_stat_wide_modified_rows(void){
  sqlite3 *db = 0;
  char dbpath[256];
  char sql[4096];
  int i;

  printf("=== Diff Stat Wide Modified Rows Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_diff_stat_wide_modified_rows");
  removeDbFiles(dbpath);

  check("open_db_for_diff_stat_wide_modified_rows",
        open_db(dbpath, &db)==SQLITE_OK);

  sqlite3_snprintf(sizeof(sql), sql,
      "CREATE TABLE t(id INTEGER PRIMARY KEY");
  for(i=1; i<=32; i++){
    sqlite3_snprintf(sizeof(sql) - strlen(sql), sql + strlen(sql),
        ", c%03d INT", i);
  }
  sqlite3_snprintf(sizeof(sql) - strlen(sql), sql + strlen(sql), ");");
  check("create_wide_table_for_diff_stat", execSql(db, sql)==SQLITE_OK);

  check("begin_wide_rows_for_diff_stat", execSql(db, "BEGIN;")==SQLITE_OK);
  for(i=1; i<=80; i++){
    sqlite3_snprintf(sizeof(sql), sql, "INSERT INTO t(id) VALUES(%d);", i);
    check("insert_wide_row_for_diff_stat", execSql(db, sql)==SQLITE_OK);
  }
  check("commit_wide_base_for_diff_stat", execSql(db,
    "COMMIT;"
    "SELECT dolt_commit('-A', '-m', 'base');")==SQLITE_OK);

  check("update_wide_rows_for_diff_stat", execSql(db,
    "UPDATE t SET c016=id, c032=id*2;"
    "SELECT dolt_commit('-A', '-m', 'wide update');")==SQLITE_OK);

  check("diff_stat_wide_rows_modified",
        strcmp(queryScalarText(db,
          "SELECT rows_modified FROM dolt_diff_stat('HEAD~1','HEAD','t');"),
          "80")==0);
  check("diff_stat_wide_cells_modified",
        strcmp(queryScalarText(db,
          "SELECT cells_modified FROM dolt_diff_stat('HEAD~1','HEAD','t');"),
          "160")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_diff_table_deep_history_map(void){
  sqlite3 *db = 0;
  char dbpath[256];
  char sql[256];
  int i;

  printf("=== Diff Table Deep History Map Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_diff_table_deep_history_map");
  removeDbFiles(dbpath);

  check("open_db_for_diff_table_deep_history_map",
        open_db(dbpath, &db)==SQLITE_OK);
  check("setup_diff_table_deep_history_map", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);"
    "INSERT INTO t VALUES(1,0);"
    "SELECT dolt_commit('-A', '-m', 'c0');")==SQLITE_OK);

  for(i=1; i<=80; i++){
    sqlite3_snprintf(sizeof(sql), sql,
      "UPDATE t SET v=%d WHERE id=1;"
      "SELECT dolt_commit('-A', '-m', 'c%d');", i, i);
    check("diff_table_deep_history_commit", execSql(db, sql)==SQLITE_OK);
  }

  check("diff_table_deep_history_count",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM dolt_diff_t WHERE diff_type='modified';"),
          "80")==0);
  check("diff_table_deep_history_latest_value",
        strcmp(queryScalarText(db,
          "SELECT to_v FROM dolt_diff_t "
          "WHERE to_commit=(SELECT commit_hash FROM dolt_log "
          "                 WHERE message='c80');"),
          "80")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_diff_stat_surfaces_corrupt_root(void){
  sqlite3 *db = 0;
  ChunkStore *cs = 0;
  char dbpath[256];
  ProllyHash headHash, fromHash, badRootHash, badCatHash, badCommitHash;
  DoltliteCommit headCommit, fromCommit, badCommit;
  struct TableEntry *aTables = 0;
  int nTables = 0;
  u8 *pCatData = 0;
  int nCatData = 0;
  u8 *pCommitData = 0;
  int nCommitData = 0;
  int rc;
  int i;
  char zFrom[PROLLY_HASH_SIZE*2 + 1];
  char zTo[PROLLY_HASH_SIZE*2 + 1];
  char sql[512];
  const char *res;
  static const u8 badNode[] = { 'b', 'a', 'd', '!' };

  printf("=== Diff Stat Surfaces Corrupt Root Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_diff_stat_surfaces_corrupt_root");
  removeDbFiles(dbpath);

  memset(&headCommit, 0, sizeof(headCommit));
  memset(&fromCommit, 0, sizeof(fromCommit));
  memset(&badCommit, 0, sizeof(badCommit));

  check("open_db_for_diff_stat_corrupt_root", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_diff_stat_corrupt_root", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'c1');"
    "DROP TABLE t;"
    "SELECT dolt_commit('-A', '-m', 'c2');")==SQLITE_OK);

  doltliteGetSessionHead(db, &headHash);
  check("load_head_commit_for_diff_stat_corrupt_root",
        doltliteLoadCommit(db, &headHash, &headCommit)==SQLITE_OK);
  if( headCommit.nParents>0 ){
    fromHash = headCommit.aParents[0];
  }else{
    fromHash = headCommit.parentHash;
  }
  check("have_from_commit_hash_for_diff_stat_corrupt_root",
        !prollyHashIsEmpty(&fromHash));
  check("load_from_commit_for_diff_stat_corrupt_root",
        doltliteLoadCommit(db, &fromHash, &fromCommit)==SQLITE_OK);
  check("load_from_catalog_for_diff_stat_corrupt_root",
        doltliteLoadCatalog(db, &fromCommit.catalogHash, &aTables, &nTables, 0)==SQLITE_OK);

  for(i=0; i<nTables; i++){
    if( aTables[i].zName && strcmp(aTables[i].zName, "t")==0 ){
      break;
    }
  }
  check("find_table_in_from_catalog_for_diff_stat_corrupt_root", i<nTables);

  cs = doltliteGetChunkStore(db);
  check("have_chunk_store_for_diff_stat_corrupt_root", cs!=0);
  if( cs && i<nTables ){
    check("store_bad_root_for_diff_stat_corrupt_root",
          chunkStorePut(cs, badNode, (int)sizeof(badNode), &badRootHash)==SQLITE_OK);
    aTables[i].root = badRootHash;
    check("serialize_corrupt_catalog_for_diff_stat_corrupt_root",
          doltliteSerializeCatalogEntries(db, aTables, nTables, &pCatData, &nCatData)==SQLITE_OK);
    check("store_corrupt_catalog_for_diff_stat_corrupt_root",
          chunkStorePut(cs, pCatData, nCatData, &badCatHash)==SQLITE_OK);

    badCommit.parentHash = fromCommit.parentHash;
    badCommit.catalogHash = badCatHash;
    badCommit.timestamp = fromCommit.timestamp;
    badCommit.zName = sqlite3_mprintf("%s", fromCommit.zName ? fromCommit.zName : "");
    badCommit.zEmail = sqlite3_mprintf("%s", fromCommit.zEmail ? fromCommit.zEmail : "");
    badCommit.zMessage = sqlite3_mprintf("%s", fromCommit.zMessage ? fromCommit.zMessage : "");
    badCommit.nParents = fromCommit.nParents;
    memcpy(badCommit.aParents, fromCommit.aParents, sizeof(fromCommit.aParents));
    check("serialize_bad_commit_for_diff_stat_corrupt_root",
          doltliteCommitSerialize(&badCommit, &pCommitData, &nCommitData)==SQLITE_OK);
    check("store_bad_commit_for_diff_stat_corrupt_root",
          chunkStorePut(cs, pCommitData, nCommitData, &badCommitHash)==SQLITE_OK);
    rc = chunkStoreCommit(cs);
    check("commit_bad_commit_for_diff_stat_corrupt_root", rc==SQLITE_OK);
  }

  doltliteHashToHex(&badCommitHash, zFrom);
  doltliteHashToHex(&headHash, zTo);

  snprintf(sql, sizeof(sql),
           "SELECT count(*) FROM dolt_diff_stat('%s','%s','t');",
           zFrom, zTo);
  res = queryScalarText(db, sql);
  check("diff_stat_corrupt_root_returns_error", strstr(res, "ERROR:")!=0);

  snprintf(sql, sizeof(sql),
           "SELECT count(*) FROM dolt_diff_summary('%s','%s','t');",
           zFrom, zTo);
  res = queryScalarText(db, sql);
  check("diff_summary_corrupt_root_returns_error", strstr(res, "ERROR:")!=0);

  sqlite3_free(pCommitData);
  sqlite3_free(pCatData);
  doltliteFreeCatalog(aTables, nTables);
  doltliteCommitClear(&badCommit);
  doltliteCommitClear(&fromCommit);
  doltliteCommitClear(&headCommit);
  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_diff_surfaces_read_errors(void){
  sqlite3 *db = 0;
  char dbpath[256];
  ProllyHash realHead;
  ProllyHash missingHead;
  const char *res;

  printf("=== Diff Surfaces Read Errors Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_diff_surfaces_read_errors");
  removeDbFiles(dbpath);

  check("open_db_for_diff_read_errors", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_diff_read_errors", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY);"
    "INSERT INTO t VALUES(1);"
    "SELECT dolt_commit('-A', '-m', 'base');"
    "CREATE VIEW v AS SELECT id FROM t;")==SQLITE_OK);

  doltliteGetSessionHead(db, &realHead);
  memset(&missingHead, 0x5a, sizeof(missingHead));
  doltliteSetSessionHead(db, &missingHead);
  res = queryScalarText(db,
    "SELECT count(*) FROM dolt_diff WHERE commit_hash='WORKING'");
  check("diff_missing_head_returns_error", strstr(res, "ERROR:")!=0);
  doltliteSetSessionHead(db, &realHead);

  gRegressionFaultCode = 957;
  gRegressionFaultHits = 0;
  sqlite3_test_control(SQLITE_TESTCTRL_FAULT_INSTALL, regressionFaultCallback);
  res = queryScalarText(db,
    "SELECT count(*) FROM dolt_diff "
    "WHERE commit_hash='WORKING' AND table_name='dolt_schemas'");
  sqlite3_test_control(SQLITE_TESTCTRL_FAULT_INSTALL, 0);
  gRegressionFaultCode = 0;
  check("diff_schema_diff_read_failure_injected", gRegressionFaultHits==1);
  check("diff_schema_diff_read_failure_returned", strstr(res, "ERROR:")!=0);

  gRegressionFaultCode = 958;
  gRegressionFaultHits = 0;
  sqlite3_test_control(SQLITE_TESTCTRL_FAULT_INSTALL, regressionFaultCallback);
  res = queryScalarText(db,
    "SELECT count(*) FROM dolt_diff "
    "WHERE commit_hash='WORKING' AND table_name='dolt_schemas'");
  sqlite3_test_control(SQLITE_TESTCTRL_FAULT_INSTALL, 0);
  gRegressionFaultCode = 0;
  check("diff_schema_scan_failure_injected", gRegressionFaultHits==1);
  check("diff_schema_scan_failure_returned", strstr(res, "ERROR:")!=0);

  check("diff_schema_scan_retry_succeeds",
    strcmp(queryScalarText(db,
      "SELECT count(*) FROM dolt_diff "
      "WHERE commit_hash='WORKING' AND table_name='dolt_schemas'"),
      "1")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_truncated_wal_is_rejected(void){
  sqlite3 *db = 0;
  ChunkStore cs;
  char dbpath[256];
  int rc;

  printf("=== Truncated WAL Rejected Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_truncated_wal_rejected");
  removeDbFiles(dbpath);

  check("open_db_for_truncated_wal", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_truncated_wal", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "INSERT INTO t VALUES(2,'b');"
    "SELECT dolt_commit('-A', '-m', 'second');")==SQLITE_OK);
  sqlite3_close(db);

  check("open_store_for_wal_tag_corruption", chunkStoreOpen(&cs, sqlite3_vfs_find(0), dbpath,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  check("have_wal_region", walStateGetDataSize(&cs.wal) > 0);
  if( walStateGetDataSize(&cs.wal) > 0 ){
    unsigned char badTag = 0xff;
    check("corrupt_first_wal_tag",
          sqlite3OsWrite(cs.file.pFile, &badTag, 1, walStateGetOffset(&cs.wal))==SQLITE_OK);
  }
  chunkStoreClose(&cs);

  rc = chunkStoreOpen(&cs, sqlite3_vfs_find(0), dbpath,
          SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB);
  check("chunk_store_open_succeeds_on_corrupt_wal", rc==SQLITE_OK);
  if( rc==SQLITE_OK ){
    u8 *pData = 0;
    int nData = 0;
    check("corrupt_wal_poisons_store", cs.corruptMidStream);
    if( cs.index.nIndex > 0 ){
      rc = chunkStoreGet(&cs, &cs.index.aIndex[0].hash, &pData, &nData);
      check("chunk_read_rejects_corrupt_wal", rc==SQLITE_CORRUPT);
      sqlite3_free(pData);
    }
    chunkStoreClose(&cs);
  }

  removeDbFiles(dbpath);
}

static void run_refresh_open_path_transactional(void){
  ChunkStore cs;
  char dbpath[256];
  FILE *f = 0;
  int changed = -1;
  int rc;

  printf("=== Refresh Open Path Transactional Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_refresh_open_path_transactional");
  removeDbFiles(dbpath);

  check("open_empty_store_with_no_file",
        chunkStoreOpen(&cs, sqlite3_vfs_find(0), dbpath,
          SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  check("store_starts_without_file", cs.file.pFile==0);

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
  check("refresh_open_path_preserves_empty_refs", prollyHashIsEmpty(&cs.refs.refsHash));
  check("refresh_open_path_preserves_branch_count", cs.refs.nBranches==0);

  chunkStoreClose(&cs);
  removeDbFiles(dbpath);
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
  removeDbFiles(dbpath);

  check("open_db_for_wal_offset", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_wal_offset", execSql(db,
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
    for(i=0; i<cs.index.nIndex; i++){
      if( cs.index.aIndex[i].offset >= walStateGetOffset(&cs.wal) ){
        iWal = i;
        break;
      }
    }
  }
  check("have_wal_backed_index_entry", iWal >= 0);
  if( iWal >= 0 ){
    cs.index.aIndex[iWal].offset = cs.file.iFileSize + 1024;
    rc = chunkStoreGet(&cs, &cs.index.aIndex[iWal].hash, &pData, &nData);
    check("corrupt_wal_offset_returns_error", rc!=SQLITE_OK);
  }
  sqlite3_free(pData);
  chunkStoreClose(&cs);
  removeDbFiles(dbpath);
}

static void run_wal_mid_corruption_rejected(void){
  sqlite3 *db = 0;
  ChunkStore cs;
  char dbpath[256];
  i64 walOff = -1;
  i64 walSize = -1;
  i64 magicPos = -1;
  u8 *walBuf = 0;
  int rc;

  printf("=== WAL Mid-Corruption Rejected Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_wal_mid_corruption_rejected");
  removeDbFiles(dbpath);

  check("open_db_for_wal_mid", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_wal_mid", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "INSERT INTO t VALUES(2,'b');"
    "SELECT dolt_commit('-A', '-m', 'second');"
    "INSERT INTO t VALUES(3,'c');"
    "SELECT dolt_commit('-A', '-m', 'third');")==SQLITE_OK);
  sqlite3_close(db);

  check("open_store_for_wal_mid", chunkStoreOpen(&cs, sqlite3_vfs_find(0), dbpath,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  walOff = walStateGetOffset(&cs.wal);
  walSize = walStateGetDataSize(&cs.wal);
  check("have_wal_for_mid_corruption", walSize > 256);

  if( walSize > 0 ){
    walBuf = (u8 *)sqlite3_malloc((int)walSize);
    check("alloc_wal_buf", walBuf != 0);
    if( walBuf ){
      check("read_wal_buf",
            sqlite3OsRead(cs.file.pFile, walBuf, (int)walSize, walOff)==SQLITE_OK);
      {
        i64 p;
        for(p=0; p+4 <= walSize; p++){
          u32 v = (u32)walBuf[p]
                | ((u32)walBuf[p+1]<<8)
                | ((u32)walBuf[p+2]<<16)
                | ((u32)walBuf[p+3]<<24);
          if( v == CHUNK_STORE_MAGIC ){
            magicPos = p;
            break;
          }
        }
      }
      sqlite3_free(walBuf);
    }
  }
  check("found_root_magic_in_wal", magicPos >= 0 && magicPos < walSize - 8);

  if( magicPos >= 0 ){
    u8 badMagic[4] = {0xde, 0xad, 0xbe, 0xef};
    check("corrupt_root_magic",
          sqlite3OsWrite(cs.file.pFile, badMagic, 4, walOff + magicPos)==SQLITE_OK);
  }
  chunkStoreClose(&cs);

  rc = chunkStoreOpen(&cs, sqlite3_vfs_find(0), dbpath,
          SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB);
  check("chunk_store_open_succeeds_on_mid_wal_corruption", rc==SQLITE_OK);
  if( rc==SQLITE_OK ){
    u8 *pData = 0;
    int nData = 0;
    check("mid_wal_corruption_poisons_store", cs.corruptMidStream);
    if( cs.index.nIndex > 0 ){
      rc = chunkStoreGet(&cs, &cs.index.aIndex[0].hash, &pData, &nData);
      check("chunk_read_rejects_mid_wal_corruption", rc==SQLITE_CORRUPT);
      sqlite3_free(pData);
    }
    chunkStoreClose(&cs);
  }

  removeDbFiles(dbpath);
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
  removeDbFiles(dbpath);

  check("open_db_for_integrity_walk", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_integrity_walk", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "INSERT INTO t VALUES(2,'b');"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "SELECT dolt_gc();")==SQLITE_OK);
  check("get_head_catalog_hash", doltliteGetHeadCatalogHash(db, &catHash)==SQLITE_OK);
  check("load_head_catalog", doltliteLoadCatalog(db, &catHash, &aTables, &nTables, &iNextTable)==SQLITE_OK);
  pTable = doltliteFindTableByName(aTables, nTables, "t");
  check("find_table_root_in_catalog", pTable!=0);
  sqlite3_close(db);

  check("open_store_for_integrity_walk", chunkStoreOpen(&cs, sqlite3_vfs_find(0), dbpath,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  if( pTable ){
    int i;
    for(i=0; i<cs.index.nIndex; i++){
      if( prollyHashCompare(&cs.index.aIndex[i].hash, &pTable->root)==0 ){
        check("root_chunk_is_compacted", cs.index.aIndex[i].offset >= 0);
        if( cs.index.aIndex[i].offset < 0 ){
          dataOff = walStateGetOffset(&cs.wal) + (-(cs.index.aIndex[i].offset + 1));
        }else{
          dataOff = cs.index.aIndex[i].offset + 4;
        }
        break;
      }
    }
  }
  check("find_root_chunk_offset", dataOff >= 0);
  if( dataOff >= 0 ){
    unsigned char badByte = 0;
    check("corrupt_root_chunk_magic",
          sqlite3OsWrite(cs.file.pFile, &badByte, 1, dataOff)==SQLITE_OK);
  }
  chunkStoreClose(&cs);

  check("reopen_db_for_integrity_walk", open_db(dbpath, &db2)==SQLITE_OK);
  check("integrity_check_surfaces_root_corruption",
        strcmp(queryScalarText(db2, "PRAGMA integrity_check"), "ok")!=0);

  sqlite3_close(db2);
  doltliteFreeCatalog(aTables, nTables);
  removeDbFiles(dbpath);
}

static void run_table_moveto_mutmap_delete_preserves_neighbors(void){
  sqlite3 *db = 0;
  char dbpath[256];

  printf("=== Table Moveto MutMap Delete Preserves Neighbors Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_table_moveto_mutmap_delete_preserves_neighbors");
  removeDbFiles(dbpath);

  check("open_db_for_table_moveto_delete", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_table_for_table_moveto_delete", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "INSERT INTO t VALUES(3,'c');")==SQLITE_OK);
  check("begin_txn_for_table_moveto_delete", execSql(db, "BEGIN IMMEDIATE;")==SQLITE_OK);
  check("insert_mutmap_only_row_for_table_moveto_delete",
        execSql(db, "INSERT INTO t VALUES(2,'b');")==SQLITE_OK);
  check("delete_mutmap_only_row_by_rowid",
        execSql(db, "DELETE FROM t WHERE rowid=2;")==SQLITE_OK);
  check("neighbors_preserved_after_delete",
        strcmp(queryScalarText(db,
          "SELECT group_concat(id, ',') FROM (SELECT id FROM t ORDER BY id)"),
          "1,3")==0);
  check("rollback_table_moveto_delete_txn", execSql(db, "ROLLBACK;")==SQLITE_OK);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_table_moveto_mutmap_exact_keeps_iteration_aligned(void){
  sqlite3 *db = 0;
  char dbpath[256];

  printf("=== Table Moveto MutMap Exact Keeps Iteration Aligned Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_table_moveto_mutmap_exact_keeps_iteration_aligned");
  removeDbFiles(dbpath);

  check("open_db_for_table_moveto_exact", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_table_for_table_moveto_exact", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "INSERT INTO t VALUES(2,'b');"
    "INSERT INTO t VALUES(3,'c');")==SQLITE_OK);
  check("begin_txn_for_table_moveto_exact", execSql(db, "BEGIN IMMEDIATE;")==SQLITE_OK);
  check("update_existing_row_for_table_moveto_exact",
        execSql(db, "UPDATE t SET v='bb' WHERE id=2;")==SQLITE_OK);
  check("table_moveto_exact_forward_iteration",
        strcmp(queryScalarText(db,
          "SELECT group_concat(id || ':' || v, ',') "
          "FROM (SELECT id, v FROM t WHERE id>=2 ORDER BY id)"),
          "2:bb,3:c")==0);
  check("table_moveto_exact_reverse_iteration",
        strcmp(queryScalarText(db,
          "SELECT group_concat(id || ':' || v, ',') "
          "FROM (SELECT id, v FROM t WHERE id<=2 ORDER BY id DESC)"),
          "2:bb,1:a")==0);
  check("rollback_table_moveto_exact_txn", execSql(db, "ROLLBACK;")==SQLITE_OK);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_index_moveto_mutmap_exact_keeps_iteration_aligned(void){
  sqlite3 *db = 0;
  sqlite3_stmt *stmt = 0;
  char dbpath[256];
  int rc;

  printf("=== Index Moveto MutMap Exact Keeps Iteration Aligned Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_index_moveto_mutmap_exact_keeps_iteration_aligned");
  removeDbFiles(dbpath);

  check("open_db_for_index_moveto_exact", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_table_for_index_moveto_exact", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, b TEXT, c TEXT);"
    "CREATE INDEX idx_b ON t(b);"
    "INSERT INTO t VALUES(1,'a','aa');"
    "INSERT INTO t VALUES(3,'c','cc');")==SQLITE_OK);
  check("begin_txn_for_index_moveto_exact", execSql(db, "BEGIN IMMEDIATE;")==SQLITE_OK);
  check("insert_mutmap_only_index_row",
        execSql(db, "INSERT INTO t VALUES(2,'b','bb');")==SQLITE_OK);

  rc = sqlite3_prepare_v2(db,
    "SELECT b FROM t INDEXED BY idx_b WHERE b >= 'b' ORDER BY b",
    -1, &stmt, 0);
  check("prepare_index_moveto_exact_stmt", rc==SQLITE_OK);
  if( rc==SQLITE_OK ){
    check("index_moveto_exact_first_row", sqlite3_step(stmt)==SQLITE_ROW);
    check("index_moveto_exact_first_val", stmt_column_text_equals(stmt, 0, "b"));
    check("index_moveto_exact_second_row", sqlite3_step(stmt)==SQLITE_ROW);
    check("index_moveto_exact_second_val", stmt_column_text_equals(stmt, 0, "c"));
    check("index_moveto_exact_done", sqlite3_step(stmt)==SQLITE_DONE);
  }
  sqlite3_finalize(stmt);
  check("rollback_index_moveto_exact_txn", execSql(db, "ROLLBACK;")==SQLITE_OK);

  sqlite3_close(db);
  removeDbFiles(dbpath);
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
  removeDbFiles(dbpath);
  gFailWriteOnce = 0;
  gFailSyncOnce = 0;
  gFailHits = 0;

  check("register_fail_vfs_for_btree_commit", registerFailVfs()==SQLITE_OK);
  check("open_fail_db_for_btree_commit", open_fail_db(dbpath, &db)==SQLITE_OK);
  check("setup_table", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');")==SQLITE_OK);
  check("get_head_catalog_for_commit_failure",
        doltliteGetHeadCatalogHash(db, &headCatHash)==SQLITE_OK);
  doltliteSetSessionStaged(db, &headCatHash);
  doltliteClearSessionMergeState(db);
  check("begin_write_txn", execSql(db, "BEGIN; INSERT INTO t VALUES(2,'b');")==SQLITE_OK);
  memset(&dummyStaged, 0x71, sizeof(dummyStaged));
  memset(&dummyMerge, 0x72, sizeof(dummyMerge));
  /* Empty, so the commit gets far enough to hit the injected write failure:
  ** unresolved conflicts are refused before any write is attempted. The staged
  ** and merge-commit values below still carry the restoration check. */
  memset(&dummyConflicts, 0, sizeof(dummyConflicts));
  doltliteSetSessionStaged(db, &dummyStaged);
  doltliteSetSessionMergeState(db, 1, &dummyMerge, &dummyConflicts);

  gFailWriteOnce = 1;
  rc = execSqlSilent(db, "COMMIT;");
  check("commit_failure_injected", gFailHits>0);
  check("commit_returns_error", rc!=SQLITE_OK);
  check("autocommit_restored_after_failed_commit", sqlite3_get_autocommit(db)==1);
  check("failed_commit_rolled_back_visible_state",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "1")==0);
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
        strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "1")==0);
  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_commit_rejects_renamed_database(void){
#ifndef _WIN32
  sqlite3 *db = 0;
  char dbpath[256];
  char renamedPath[320];
  int rc;

  printf("=== Commit Rejects Renamed Database Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_commit_rejects_renamed_database");
  sqlite3_snprintf(sizeof(renamedPath), renamedPath, "%s-renamed", dbpath);
  removeDbFiles(dbpath);
  removeDbFiles(renamedPath);

  check("renamed_commit_open", open_db(dbpath, &db)==SQLITE_OK);
  check("renamed_commit_setup", execSql(db,
    "CREATE TABLE t1(a,b,c);"
    "INSERT INTO t1 VALUES(673,'stone','philips');"
    "SELECT dolt_commit('-A','-m','base');")==SQLITE_OK);
  check("renamed_commit_stage", execSql(db,
    "BEGIN;"
    "UPDATE t1 SET b='staged';")==SQLITE_OK);
  check("renamed_commit_rename", rename(dbpath, renamedPath)==0);

  rc = execSqlSilent(db, "COMMIT;");
  check("renamed_commit_readonly", rc==SQLITE_READONLY);
  check("renamed_commit_autocommit_restored", sqlite3_get_autocommit(db)==1);
  check("renamed_commit_connection_rolled_back",
        strcmp(queryScalarText(db, "SELECT b FROM t1 WHERE a=673"), "stone")==0);
  sqlite3_close(db);
  db = 0;

  check("renamed_commit_reopen", open_db(renamedPath, &db)==SQLITE_OK);
  check("renamed_commit_row_not_durable",
        strcmp(queryScalarText(db, "SELECT b FROM t1 WHERE a=673"), "stone")==0);
  check("renamed_commit_status_not_durable",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_status"), "0")==0);
  sqlite3_close(db);
  removeDbFiles(dbpath);
  removeDbFiles(renamedPath);
#endif
}

static void run_attached_database_seed_and_repair(void){
  sqlite3 *db = 0;
  ChunkStore cs;
  ProllyHash tip;
  ProllyHash empty;
  char mainPath[256];
  char auxPath[256];
  char *sql;
  int rc;

  printf("=== Attached Database Seed And Repair Test ===\n\n");
  make_dbpath(mainPath, sizeof(mainPath), "test_attached_seed_main");
  make_dbpath(auxPath, sizeof(auxPath), "test_attached_seed_aux");
  removeDbFiles(mainPath);
  removeDbFiles(auxPath);

  check("attached_seed_open_main", open_db(mainPath, &db)==SQLITE_OK);
  sql = sqlite3_mprintf(
      "ATTACH %Q AS aux;"
      "CREATE TABLE aux.t2(y INTEGER);"
      "INSERT INTO aux.t2 VALUES(7);", auxPath);
  check("attached_seed_sql_alloc", sql!=0);
  check("attached_seed_create_data", sql && execSql(db, sql)==SQLITE_OK);
  sqlite3_free(sql);
  sqlite3_close(db);
  db = 0;

  rc = chunkStoreOpen(&cs, sqlite3_vfs_find(0), auxPath,
                      SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB);
  check("attached_seed_open_raw", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ){
    removeDbFiles(mainPath);
    removeDbFiles(auxPath);
    return;
  }
  memset(&tip, 0, sizeof(tip));
  check("attached_seed_branch_exists",
        rc==SQLITE_OK && chunkStoreFindBranch(&cs, "main", &tip)==SQLITE_OK);
  check("attached_seed_branch_nonzero", !prollyHashIsEmpty(&tip));

  memset(&empty, 0, sizeof(empty));
  check("attached_seed_lock_raw", chunkStoreLockAndRefresh(&cs)==SQLITE_OK);
  check("attached_seed_zero_tip",
        chunkStoreUpdateBranch(&cs, "main", &empty)==SQLITE_OK);
  check("attached_seed_serialize_zero_tip",
        chunkStoreSerializeRefs(&cs)==SQLITE_OK);
  check("attached_seed_commit_zero_tip", chunkStoreCommit(&cs)==SQLITE_OK);
  chunkStoreUnlock(&cs);
  chunkStoreClose(&cs);

  check("attached_seed_reopen_for_repair", open_db(auxPath, &db)==SQLITE_OK);
  check("attached_seed_repair_keeps_rows",
        strcmp(queryScalarText(db, "SELECT group_concat(y) FROM t2"), "7")==0);
  check("attached_seed_repair_creates_log",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_log"), "1")==0);
  check("attached_seed_repair_keeps_dirty_table",
        strcmp(queryScalarText(db,
          "SELECT status||'|'||staged FROM dolt_status WHERE table_name='t2'"),
          "new table|0")==0);
  check("attached_seed_commit_after_repair",
        execSql(db, "SELECT dolt_commit('-A','-m','after repair');")==SQLITE_OK);
  check("attached_seed_history_after_repair",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_log"), "2")==0);
  check("attached_seed_rows_after_commit",
        strcmp(queryScalarText(db, "SELECT group_concat(y) FROM t2"), "7")==0);
  sqlite3_close(db);
  removeDbFiles(mainPath);
  removeDbFiles(auxPath);
}

static void run_write_rejects_foreign_database_at_path(void){
#ifndef _WIN32
  sqlite3 *db = 0;
  sqlite3 *foreign = 0;
  char dbpath[256];
  char renamedPath[320];
  int rc;

  printf("=== Write Rejects Foreign Database At Path Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_write_rejects_foreign_database");
  sqlite3_snprintf(sizeof(renamedPath), renamedPath, "%s-renamed", dbpath);
  removeDbFiles(dbpath);
  removeDbFiles(renamedPath);

  check("foreign_path_open", open_db(dbpath, &db)==SQLITE_OK);
  check("foreign_path_setup", execSql(db,
    "CREATE TABLE t1(a,b,c);"
    "INSERT INTO t1 VALUES(673,'stone','philips');"
    "SELECT dolt_commit('-A','-m','base');"
    "UPDATE t1 SET b='dirty';")==SQLITE_OK);
  check("foreign_path_rename", rename(dbpath, renamedPath)==0);

  check("foreign_path_open_stranger", open_db(dbpath, &foreign)==SQLITE_OK);
  check("foreign_path_seed_stranger",
        execSql(foreign, "CREATE TABLE t2(x,y,z);")==SQLITE_OK);
  sqlite3_close(foreign);
  foreign = 0;

  check("foreign_path_read_keeps_own_store",
        strcmp(queryScalarText(db, "SELECT b FROM t1 WHERE a=673"), "dirty")==0);
  rc = execSqlSilent(db, "UPDATE t1 SET b='after';");
  check("foreign_path_write_readonly", rc==SQLITE_READONLY);
  /* The VC write path force-refreshes before advancing the ref; it must refuse
  ** the same way instead of reloading by path and adopting the stranger. */
  rc = execSqlSilent(db, "SELECT dolt_commit('-A','-m','vc');");
  check("foreign_path_vc_commit_readonly", rc==SQLITE_READONLY);
  check("foreign_path_read_survives_refusals",
        strcmp(queryScalarText(db, "SELECT b FROM t1 WHERE a=673"), "dirty")==0);
  check("foreign_path_own_schema_retained",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM sqlite_master WHERE name='t2'"), "0")==0);
  sqlite3_close(db);
  db = 0;

  check("foreign_path_reopen_own_store", open_db(renamedPath, &db)==SQLITE_OK);
  check("foreign_path_own_store_unmodified",
        strcmp(queryScalarText(db, "SELECT b FROM t1 WHERE a=673"), "dirty")==0);
  sqlite3_close(db);
  db = 0;

  check("foreign_path_reopen_stranger", open_db(dbpath, &foreign)==SQLITE_OK);
  check("foreign_path_stranger_unmodified",
        strcmp(queryScalarText(foreign,
          "SELECT count(*) FROM sqlite_master WHERE name='t1'"), "0")==0);
  sqlite3_close(foreign);
  removeDbFiles(dbpath);
  removeDbFiles(renamedPath);
#endif
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
  removeDbFiles(dbpath);

  check("open_db_for_savepoint_metadata", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_savepoint_metadata", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);

  check("get_head_catalog_for_savepoint_metadata",
        doltliteGetHeadCatalogHash(db, &baseStaged)==SQLITE_OK);
  doltliteSetSessionStaged(db, &baseStaged);
  doltliteClearSessionMergeState(db);

  check("begin_immediate_for_savepoint_metadata",
        execSql(db, "BEGIN IMMEDIATE;")==SQLITE_OK);
  check("create_savepoint_for_metadata",
        execSql(db, "SAVEPOINT s1;")==SQLITE_OK);

  memset(&dummyStaged, 0x41, sizeof(dummyStaged));
  memset(&dummyMerge, 0x42, sizeof(dummyMerge));
  memset(&dummyConflicts, 0x43, sizeof(dummyConflicts));
  doltliteSetSessionStaged(db, &dummyStaged);
  doltliteSetSessionMergeState(db, 1, &dummyMerge, &dummyConflicts);
  doltliteGetSessionStaged(db, &stagedAfter);
  check("savepoint_metadata_mutation_applied",
        memcmp(&stagedAfter, &dummyStaged, sizeof(dummyStaged))==0);

  check("rollback_to_savepoint_metadata",
        execSql(db, "ROLLBACK TO s1;")==SQLITE_OK);
  doltliteGetSessionStaged(db, &stagedAfter);
  doltliteGetSessionMergeState(db, &isMergingAfter, &mergeAfter, &conflictsAfter);
  check("rollback_to_savepoint_restores_staged",
        memcmp(&stagedAfter, &baseStaged, sizeof(baseStaged))==0);
  check("rollback_to_savepoint_clears_merge_flag", isMergingAfter==0);
  check("rollback_to_savepoint_clears_merge_commit",
        memcmp(&mergeAfter, &(ProllyHash){{0}}, sizeof(ProllyHash))==0);
  check("rollback_to_savepoint_clears_conflicts_catalog",
        memcmp(&conflictsAfter, &(ProllyHash){{0}}, sizeof(ProllyHash))==0);

  check("release_savepoint_metadata", execSql(db, "RELEASE s1;")==SQLITE_OK);
  check("rollback_outer_txn_metadata", execSql(db, "ROLLBACK;")==SQLITE_OK);
  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_savepoint_flush_snapshot_rollback_reopen(void){
  sqlite3 *db = 0;
  char dbpath[256];

  printf("=== Savepoint Flush Snapshot Rollback Reopen Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_savepoint_flush_snapshot_rollback_reopen");
  removeDbFiles(dbpath);

  check("open_db_for_flush_snapshot_rollback", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_flush_snapshot_rollback", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER, v TEXT);"
    "CREATE INDEX k_idx ON t(k);"
    "INSERT INTO t VALUES(1, 1, 'a');"
    "INSERT INTO t VALUES(2, 2, 'b');"
    "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);

  check("begin_txn_for_flush_snapshot_rollback",
        execSql(db, "BEGIN IMMEDIATE;")==SQLITE_OK);
  check("savepoint_outer_for_flush_snapshot_rollback",
        execSql(db, "SAVEPOINT outer_sp;")==SQLITE_OK);
  check("outer_index_update_for_flush_snapshot_rollback",
        execSql(db, "UPDATE t SET k=11, v='outer' WHERE id=1;")==SQLITE_OK);
  check("savepoint_inner_for_flush_snapshot_rollback",
        execSql(db, "SAVEPOINT inner_sp;")==SQLITE_OK);
  check("inner_index_edits_for_flush_snapshot_rollback",
        execSql(db,
          "UPDATE t SET k=22, v='inner' WHERE id=2;"
          "INSERT INTO t VALUES(3, 33, 'inner3');")==SQLITE_OK);
  check("rollback_inner_after_flush_snapshot",
        execSql(db, "ROLLBACK TO inner_sp;")==SQLITE_OK);
  check("release_inner_after_flush_snapshot",
        execSql(db, "RELEASE inner_sp;")==SQLITE_OK);
  check("commit_outer_after_flush_snapshot",
        execSql(db, "COMMIT;")==SQLITE_OK);

  check("rollback_path_outer_row_visible_before_close",
        strcmp(queryScalarText(db, "SELECT k FROM t WHERE id=1"), "11")==0);
  check("rollback_path_inner_row_reverted_before_close",
        strcmp(queryScalarText(db, "SELECT k FROM t WHERE id=2"), "2")==0);
  check("rollback_path_insert_reverted_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE id=3"), "0")==0);
  check("rollback_path_index_lookup_outer_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE k=11"), "1")==0);
  check("rollback_path_index_lookup_inner_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE k=22"), "0")==0);
  check("rollback_path_integrity_before_close",
        strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_for_flush_snapshot_rollback", open_db(dbpath, &db)==SQLITE_OK);
  check("rollback_path_outer_row_visible_after_reopen",
        strcmp(queryScalarText(db, "SELECT k FROM t WHERE id=1"), "11")==0);
  check("rollback_path_inner_row_reverted_after_reopen",
        strcmp(queryScalarText(db, "SELECT k FROM t WHERE id=2"), "2")==0);
  check("rollback_path_insert_reverted_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE id=3"), "0")==0);
  check("rollback_path_index_lookup_outer_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE k=11"), "1")==0);
  check("rollback_path_index_lookup_inner_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE k=22"), "0")==0);
  check("rollback_path_integrity_after_reopen",
        strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_savepoint_flush_snapshot_release_reopen(void){
  sqlite3 *db = 0;
  char dbpath[256];

  printf("=== Savepoint Flush Snapshot Release Reopen Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_savepoint_flush_snapshot_release_reopen");
  removeDbFiles(dbpath);

  check("open_db_for_flush_snapshot_release", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_flush_snapshot_release", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER, v TEXT);"
    "CREATE INDEX k_idx ON t(k);"
    "INSERT INTO t VALUES(1, 1, 'a');"
    "INSERT INTO t VALUES(2, 2, 'b');"
    "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);

  check("begin_txn_for_flush_snapshot_release",
        execSql(db, "BEGIN IMMEDIATE;")==SQLITE_OK);
  check("savepoint_outer_for_flush_snapshot_release",
        execSql(db, "SAVEPOINT outer_sp;")==SQLITE_OK);
  check("outer_index_update_for_flush_snapshot_release",
        execSql(db, "UPDATE t SET k=11, v='outer' WHERE id=1;")==SQLITE_OK);
  check("savepoint_inner_for_flush_snapshot_release",
        execSql(db, "SAVEPOINT inner_sp;")==SQLITE_OK);
  check("inner_index_edits_for_flush_snapshot_release",
        execSql(db,
          "UPDATE t SET k=22, v='inner' WHERE id=2;"
          "INSERT INTO t VALUES(3, 33, 'inner3');")==SQLITE_OK);
  check("release_inner_after_flush_snapshot",
        execSql(db, "RELEASE inner_sp;")==SQLITE_OK);
  check("release_outer_after_flush_snapshot",
        execSql(db, "RELEASE outer_sp;")==SQLITE_OK);
  check("commit_after_flush_snapshot_release",
        execSql(db, "COMMIT;")==SQLITE_OK);
  check("dolt_commit_after_flush_snapshot_release",
        execSql(db, "SELECT dolt_commit('-A', '-m', 'after savepoints');")==SQLITE_OK);

  check("release_path_outer_row_visible_before_close",
        strcmp(queryScalarText(db, "SELECT k FROM t WHERE id=1"), "11")==0);
  check("release_path_inner_row_visible_before_close",
        strcmp(queryScalarText(db, "SELECT k FROM t WHERE id=2"), "22")==0);
  check("release_path_insert_visible_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE id=3"), "1")==0);
  check("release_path_index_lookup_outer_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE k=11"), "1")==0);
  check("release_path_index_lookup_inner_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE k=22"), "1")==0);
  check("release_path_index_lookup_insert_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE k=33"), "1")==0);
  check("release_path_integrity_before_close",
        strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);
  check("release_path_clean_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_status"), "0")==0);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_for_flush_snapshot_release", open_db(dbpath, &db)==SQLITE_OK);
  check("release_path_outer_row_visible_after_reopen",
        strcmp(queryScalarText(db, "SELECT k FROM t WHERE id=1"), "11")==0);
  check("release_path_inner_row_visible_after_reopen",
        strcmp(queryScalarText(db, "SELECT k FROM t WHERE id=2"), "22")==0);
  check("release_path_insert_visible_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE id=3"), "1")==0);
  check("release_path_index_lookup_outer_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE k=11"), "1")==0);
  check("release_path_index_lookup_inner_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE k=22"), "1")==0);
  check("release_path_index_lookup_insert_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE k=33"), "1")==0);
  check("release_path_integrity_after_reopen",
        strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);
  check("release_path_clean_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_status"), "0")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_savepoint_failed_commit_rollback_reopen(void){
  sqlite3 *db = 0;
  char dbpath[256];
  char zHeadBefore[128];

  printf("=== Savepoint Failed Commit Rollback Reopen Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_savepoint_failed_commit_rollback_reopen");
  removeDbFiles(dbpath);

  check("open_db_for_savepoint_failed_commit_rollback",
        open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_savepoint_failed_commit_rollback", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER, v TEXT);"
    "CREATE INDEX k_idx ON t(k);"
    "INSERT INTO t VALUES(1, 1, 'a');"
    "INSERT INTO t VALUES(2, 2, 'b');"
    "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);
  sqlite3_snprintf(sizeof(zHeadBefore), zHeadBefore, "%s",
                   queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"));

  check("begin_txn_for_savepoint_failed_commit_rollback",
        execSql(db, "BEGIN IMMEDIATE;")==SQLITE_OK);
  check("savepoint_outer_for_savepoint_failed_commit_rollback",
        execSql(db, "SAVEPOINT outer_sp;")==SQLITE_OK);
  check("outer_edits_for_savepoint_failed_commit_rollback",
        execSql(db,
          "UPDATE t SET k=11, v='outer' WHERE id=1;"
          "INSERT INTO t VALUES(4, 44, 'outer4');")==SQLITE_OK);
  check("savepoint_inner_for_savepoint_failed_commit_rollback",
        execSql(db, "SAVEPOINT inner_sp;")==SQLITE_OK);
  check("inner_edits_for_savepoint_failed_commit_rollback",
        execSql(db,
          "UPDATE t SET k=22, v='inner' WHERE id=2;"
          "INSERT INTO t VALUES(3, 33, 'inner3');")==SQLITE_OK);

  check("rollback_inner_after_failed_commit",
        execSql(db, "ROLLBACK TO inner_sp;")==SQLITE_OK);
  check("release_inner_after_failed_commit_rollback",
        execSql(db, "RELEASE inner_sp;")==SQLITE_OK);
  check("release_outer_after_failed_commit_rollback",
        execSql(db, "RELEASE outer_sp;")==SQLITE_OK);
  check("commit_after_failed_commit_rollback",
        execSql(db, "COMMIT;")==SQLITE_OK);

  check("failed_commit_rollback_outer_update_visible_before_close",
        strcmp(queryScalarText(db, "SELECT k FROM t WHERE id=1"), "11")==0);
  check("failed_commit_rollback_inner_update_reverted_before_close",
        strcmp(queryScalarText(db, "SELECT k FROM t WHERE id=2"), "2")==0);
  check("failed_commit_rollback_inner_insert_reverted_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE id=3"), "0")==0);
  check("failed_commit_rollback_outer_insert_visible_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE id=4"), "1")==0);
  check("failed_commit_rollback_outer_index_visible_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE k=11"), "1")==0);
  check("failed_commit_rollback_inner_index_reverted_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE k=22"), "0")==0);
  check("failed_commit_rollback_head_unchanged_before_close",
        strcmp(queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"), zHeadBefore)==0);
  check("failed_commit_rollback_status_dirty_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_status"), "1")==0);
  check("failed_commit_rollback_integrity_before_close",
        strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_for_savepoint_failed_commit_rollback", open_db(dbpath, &db)==SQLITE_OK);
  check("failed_commit_rollback_outer_update_visible_after_reopen",
        strcmp(queryScalarText(db, "SELECT k FROM t WHERE id=1"), "11")==0);
  check("failed_commit_rollback_inner_update_reverted_after_reopen",
        strcmp(queryScalarText(db, "SELECT k FROM t WHERE id=2"), "2")==0);
  check("failed_commit_rollback_inner_insert_reverted_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE id=3"), "0")==0);
  check("failed_commit_rollback_outer_insert_visible_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE id=4"), "1")==0);
  check("failed_commit_rollback_outer_index_visible_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE k=11"), "1")==0);
  check("failed_commit_rollback_inner_index_reverted_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE k=22"), "0")==0);
  check("failed_commit_rollback_head_unchanged_after_reopen",
        strcmp(queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"), zHeadBefore)==0);
  check("failed_commit_rollback_status_dirty_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_status"), "1")==0);
  check("failed_commit_rollback_integrity_after_reopen",
        strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_savepoint_failed_commit_release_reopen(void){
  sqlite3 *db = 0;
  char dbpath[256];
  char zHeadBefore[128];

  printf("=== Savepoint Failed Commit Release Reopen Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_savepoint_failed_commit_release_reopen");
  removeDbFiles(dbpath);

  check("open_db_for_savepoint_failed_commit_release",
        open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_savepoint_failed_commit_release", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER, v TEXT);"
    "CREATE INDEX k_idx ON t(k);"
    "INSERT INTO t VALUES(1, 1, 'a');"
    "INSERT INTO t VALUES(2, 2, 'b');"
    "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);
  sqlite3_snprintf(sizeof(zHeadBefore), zHeadBefore, "%s",
                   queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"));

  check("begin_txn_for_savepoint_failed_commit_release",
        execSql(db, "BEGIN IMMEDIATE;")==SQLITE_OK);
  check("savepoint_outer_for_savepoint_failed_commit_release",
        execSql(db, "SAVEPOINT outer_sp;")==SQLITE_OK);
  check("outer_edits_for_savepoint_failed_commit_release",
        execSql(db,
          "UPDATE t SET k=11, v='outer' WHERE id=1;"
          "INSERT INTO t VALUES(4, 44, 'outer4');")==SQLITE_OK);
  check("savepoint_inner_for_savepoint_failed_commit_release",
        execSql(db, "SAVEPOINT inner_sp;")==SQLITE_OK);
  check("inner_edits_for_savepoint_failed_commit_release",
        execSql(db,
          "UPDATE t SET k=22, v='inner' WHERE id=2;"
          "INSERT INTO t VALUES(3, 33, 'inner3');")==SQLITE_OK);

  check("release_inner_after_failed_commit_release",
        execSql(db, "RELEASE inner_sp;")==SQLITE_OK);
  check("release_outer_after_failed_commit_release",
        execSql(db, "RELEASE outer_sp;")==SQLITE_OK);
  check("commit_after_failed_commit_release",
        execSql(db, "COMMIT;")==SQLITE_OK);

  check("failed_commit_release_outer_update_visible_before_close",
        strcmp(queryScalarText(db, "SELECT k FROM t WHERE id=1"), "11")==0);
  check("failed_commit_release_inner_update_visible_before_close",
        strcmp(queryScalarText(db, "SELECT k FROM t WHERE id=2"), "22")==0);
  check("failed_commit_release_inner_insert_visible_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE id=3"), "1")==0);
  check("failed_commit_release_outer_insert_visible_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE id=4"), "1")==0);
  check("failed_commit_release_outer_index_visible_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE k=11"), "1")==0);
  check("failed_commit_release_inner_index_visible_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE k=22"), "1")==0);
  check("failed_commit_release_insert_index_visible_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE k=33"), "1")==0);
  check("failed_commit_release_head_unchanged_before_close",
        strcmp(queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"), zHeadBefore)==0);
  check("failed_commit_release_status_dirty_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_status"), "1")==0);
  check("failed_commit_release_integrity_before_close",
        strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_for_savepoint_failed_commit_release", open_db(dbpath, &db)==SQLITE_OK);
  check("failed_commit_release_outer_update_visible_after_reopen",
        strcmp(queryScalarText(db, "SELECT k FROM t WHERE id=1"), "11")==0);
  check("failed_commit_release_inner_update_visible_after_reopen",
        strcmp(queryScalarText(db, "SELECT k FROM t WHERE id=2"), "22")==0);
  check("failed_commit_release_inner_insert_visible_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE id=3"), "1")==0);
  check("failed_commit_release_outer_insert_visible_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE id=4"), "1")==0);
  check("failed_commit_release_outer_index_visible_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE k=11"), "1")==0);
  check("failed_commit_release_inner_index_visible_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE k=22"), "1")==0);
  check("failed_commit_release_insert_index_visible_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE k=33"), "1")==0);
  check("failed_commit_release_head_unchanged_after_reopen",
        strcmp(queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"), zHeadBefore)==0);
  check("failed_commit_release_status_dirty_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_status"), "1")==0);
  check("failed_commit_release_integrity_after_reopen",
        strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_savepoint_failed_commit_outer_rollback_reopen(void){
  sqlite3 *db = 0;
  char dbpath[256];
  char zHeadBefore[128];

  printf("=== Savepoint Failed Commit Outer Rollback Reopen Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath),
              "test_savepoint_failed_commit_outer_rollback_reopen");
  removeDbFiles(dbpath);

  check("open_db_for_savepoint_failed_commit_outer_rollback",
        open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_savepoint_failed_commit_outer_rollback", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER, v TEXT);"
    "CREATE INDEX k_idx ON t(k);"
    "INSERT INTO t VALUES(1, 1, 'a');"
    "INSERT INTO t VALUES(2, 2, 'b');"
    "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);
  sqlite3_snprintf(sizeof(zHeadBefore), zHeadBefore, "%s",
                   queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"));

  check("begin_txn_for_savepoint_failed_commit_outer_rollback",
        execSql(db, "BEGIN IMMEDIATE;")==SQLITE_OK);
  check("savepoint_outer_for_savepoint_failed_commit_outer_rollback",
        execSql(db, "SAVEPOINT outer_sp;")==SQLITE_OK);
  check("outer_edits_for_savepoint_failed_commit_outer_rollback",
        execSql(db,
          "UPDATE t SET k=11, v='outer' WHERE id=1;"
          "INSERT INTO t VALUES(4, 44, 'outer4');")==SQLITE_OK);
  check("savepoint_inner_for_savepoint_failed_commit_outer_rollback",
        execSql(db, "SAVEPOINT inner_sp;")==SQLITE_OK);
  check("inner_edits_for_savepoint_failed_commit_outer_rollback",
        execSql(db,
          "UPDATE t SET k=22, v='inner' WHERE id=2;"
          "INSERT INTO t VALUES(3, 33, 'inner3');")==SQLITE_OK);

  check("rollback_outer_after_failed_commit",
        execSql(db, "ROLLBACK TO outer_sp;")==SQLITE_OK);
  check("release_outer_after_failed_commit_outer_rollback",
        execSql(db, "RELEASE outer_sp;")==SQLITE_OK);
  check("commit_after_failed_commit_outer_rollback",
        execSql(db, "COMMIT;")==SQLITE_OK);

  check("failed_commit_outer_rollback_row1_before_close",
        strcmp(queryScalarText(db, "SELECT k FROM t WHERE id=1"), "1")==0);
  check("failed_commit_outer_rollback_row2_before_close",
        strcmp(queryScalarText(db, "SELECT k FROM t WHERE id=2"), "2")==0);
  check("failed_commit_outer_rollback_row3_absent_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE id=3"), "0")==0);
  check("failed_commit_outer_rollback_row4_absent_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE id=4"), "0")==0);
  check("failed_commit_outer_rollback_k11_absent_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE k=11"), "0")==0);
  check("failed_commit_outer_rollback_k22_absent_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE k=22"), "0")==0);
  check("failed_commit_outer_rollback_head_unchanged_before_close",
        strcmp(queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"), zHeadBefore)==0);
  check("failed_commit_outer_rollback_status_clean_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_status"), "0")==0);
  check("failed_commit_outer_rollback_integrity_before_close",
        strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_for_savepoint_failed_commit_outer_rollback",
        open_db(dbpath, &db)==SQLITE_OK);
  check("failed_commit_outer_rollback_row1_after_reopen",
        strcmp(queryScalarText(db, "SELECT k FROM t WHERE id=1"), "1")==0);
  check("failed_commit_outer_rollback_row2_after_reopen",
        strcmp(queryScalarText(db, "SELECT k FROM t WHERE id=2"), "2")==0);
  check("failed_commit_outer_rollback_row3_absent_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE id=3"), "0")==0);
  check("failed_commit_outer_rollback_row4_absent_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE id=4"), "0")==0);
  check("failed_commit_outer_rollback_k11_absent_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE k=11"), "0")==0);
  check("failed_commit_outer_rollback_k22_absent_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE k=22"), "0")==0);
  check("failed_commit_outer_rollback_head_unchanged_after_reopen",
        strcmp(queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"), zHeadBefore)==0);
  check("failed_commit_outer_rollback_status_clean_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_status"), "0")==0);
  check("failed_commit_outer_rollback_integrity_after_reopen",
        strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_savepoint_flush_snapshot_multi_table_rollback_reopen(void){
  sqlite3 *db = 0;
  char dbpath[256];

  printf("=== Savepoint Flush Snapshot Multi Table Rollback Reopen Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath),
              "test_savepoint_flush_snapshot_multi_table_rollback_reopen");
  removeDbFiles(dbpath);

  check("open_db_for_flush_snapshot_multi_table_rollback",
        open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_flush_snapshot_multi_table_rollback", execSql(db,
    "CREATE TABLE a(id INTEGER PRIMARY KEY, k INTEGER, v TEXT);"
    "CREATE INDEX a_k_idx ON a(k);"
    "CREATE TABLE b(id INTEGER PRIMARY KEY, k INTEGER, v TEXT);"
    "CREATE INDEX b_k_idx ON b(k);"
    "INSERT INTO a VALUES(1, 1, 'a1');"
    "INSERT INTO a VALUES(2, 2, 'a2');"
    "INSERT INTO b VALUES(1, 10, 'b1');"
    "INSERT INTO b VALUES(2, 20, 'b2');"
    "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);

  check("begin_txn_for_flush_snapshot_multi_table_rollback",
        execSql(db, "BEGIN IMMEDIATE;")==SQLITE_OK);
  check("savepoint_outer_for_flush_snapshot_multi_table_rollback",
        execSql(db, "SAVEPOINT outer_sp;")==SQLITE_OK);
  check("outer_edits_for_flush_snapshot_multi_table_rollback",
        execSql(db,
          "UPDATE a SET k=11, v='outer-a' WHERE id=1;"
          "INSERT INTO b VALUES(3, 30, 'outer-b3');")==SQLITE_OK);
  check("savepoint_inner_for_flush_snapshot_multi_table_rollback",
        execSql(db, "SAVEPOINT inner_sp;")==SQLITE_OK);
  check("inner_edits_for_flush_snapshot_multi_table_rollback",
        execSql(db,
          "UPDATE a SET k=22, v='inner-a' WHERE id=2;"
          "INSERT INTO a VALUES(3, 33, 'inner-a3');"
          "UPDATE b SET k=33, v='inner-b' WHERE id=2;")==SQLITE_OK);
  check("rollback_inner_after_flush_snapshot_multi_table",
        execSql(db, "ROLLBACK TO inner_sp;")==SQLITE_OK);
  check("release_inner_after_flush_snapshot_multi_table",
        execSql(db, "RELEASE inner_sp;")==SQLITE_OK);
  check("release_outer_after_flush_snapshot_multi_table",
        execSql(db, "RELEASE outer_sp;")==SQLITE_OK);
  check("commit_after_flush_snapshot_multi_table_rollback",
        execSql(db, "COMMIT;")==SQLITE_OK);

  check("flush_snapshot_multi_table_a1_before_close",
        strcmp(queryScalarText(db, "SELECT k FROM a WHERE id=1"), "11")==0);
  check("flush_snapshot_multi_table_a2_before_close",
        strcmp(queryScalarText(db, "SELECT k FROM a WHERE id=2"), "2")==0);
  check("flush_snapshot_multi_table_a3_absent_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM a WHERE id=3"), "0")==0);
  check("flush_snapshot_multi_table_b2_before_close",
        strcmp(queryScalarText(db, "SELECT k FROM b WHERE id=2"), "20")==0);
  check("flush_snapshot_multi_table_b3_present_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM b WHERE id=3"), "1")==0);
  check("flush_snapshot_multi_table_a11_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM a WHERE k=11"), "1")==0);
  check("flush_snapshot_multi_table_a22_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM a WHERE k=22"), "0")==0);
  check("flush_snapshot_multi_table_b30_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM b WHERE k=30"), "1")==0);
  check("flush_snapshot_multi_table_b33_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM b WHERE k=33"), "0")==0);
  check("flush_snapshot_multi_table_integrity_before_close",
        strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_for_flush_snapshot_multi_table_rollback",
        open_db(dbpath, &db)==SQLITE_OK);
  check("flush_snapshot_multi_table_a1_after_reopen",
        strcmp(queryScalarText(db, "SELECT k FROM a WHERE id=1"), "11")==0);
  check("flush_snapshot_multi_table_a2_after_reopen",
        strcmp(queryScalarText(db, "SELECT k FROM a WHERE id=2"), "2")==0);
  check("flush_snapshot_multi_table_a3_absent_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM a WHERE id=3"), "0")==0);
  check("flush_snapshot_multi_table_b2_after_reopen",
        strcmp(queryScalarText(db, "SELECT k FROM b WHERE id=2"), "20")==0);
  check("flush_snapshot_multi_table_b3_present_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM b WHERE id=3"), "1")==0);
  check("flush_snapshot_multi_table_a11_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM a WHERE k=11"), "1")==0);
  check("flush_snapshot_multi_table_a22_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM a WHERE k=22"), "0")==0);
  check("flush_snapshot_multi_table_b30_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM b WHERE k=30"), "1")==0);
  check("flush_snapshot_multi_table_b33_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM b WHERE k=33"), "0")==0);
  check("flush_snapshot_multi_table_integrity_after_reopen",
        strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_savepoint_same_name_shadowing_index_reopen(void){
  sqlite3 *db = 0;
  char dbpath[256];

  printf("=== Savepoint Same Name Shadowing Index Reopen Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath),
              "test_savepoint_same_name_shadowing_index_reopen");
  removeDbFiles(dbpath);

  check("open_db_for_savepoint_same_name_shadowing",
        open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_savepoint_same_name_shadowing", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER, v TEXT);"
    "CREATE INDEX k_idx ON t(k);"
    "INSERT INTO t VALUES(1, 1, 'a');"
    "INSERT INTO t VALUES(2, 2, 'b');"
    "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);

  check("begin_txn_for_savepoint_same_name_shadowing",
        execSql(db, "BEGIN IMMEDIATE;")==SQLITE_OK);
  check("savepoint_shadow_outer",
        execSql(db, "SAVEPOINT same_sp;")==SQLITE_OK);
  check("shadow_outer_edits",
        execSql(db,
          "UPDATE t SET k=11, v='outer' WHERE id=1;"
          "INSERT INTO t VALUES(3, 33, 'outer3');")==SQLITE_OK);
  check("savepoint_shadow_inner",
        execSql(db, "SAVEPOINT same_sp;")==SQLITE_OK);
  check("shadow_inner_edits",
        execSql(db,
          "UPDATE t SET k=22, v='inner' WHERE id=2;"
          "INSERT INTO t VALUES(4, 44, 'inner4');")==SQLITE_OK);
  check("rollback_shadow_inner",
        execSql(db, "ROLLBACK TO same_sp;")==SQLITE_OK);
  check("release_shadow_inner",
        execSql(db, "RELEASE same_sp;")==SQLITE_OK);
  check("release_shadow_outer",
        execSql(db, "RELEASE same_sp;")==SQLITE_OK);
  check("commit_shadow_same_name",
        execSql(db, "COMMIT;")==SQLITE_OK);

  check("shadow_same_name_id1_before_close",
        strcmp(queryScalarText(db, "SELECT k FROM t WHERE id=1"), "11")==0);
  check("shadow_same_name_id2_before_close",
        strcmp(queryScalarText(db, "SELECT k FROM t WHERE id=2"), "2")==0);
  check("shadow_same_name_id3_present_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE id=3"), "1")==0);
  check("shadow_same_name_id4_absent_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE id=4"), "0")==0);
  check("shadow_same_name_k11_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE k=11"), "1")==0);
  check("shadow_same_name_k22_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE k=22"), "0")==0);
  check("shadow_same_name_integrity_before_close",
        strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_for_savepoint_same_name_shadowing",
        open_db(dbpath, &db)==SQLITE_OK);
  check("shadow_same_name_id1_after_reopen",
        strcmp(queryScalarText(db, "SELECT k FROM t WHERE id=1"), "11")==0);
  check("shadow_same_name_id2_after_reopen",
        strcmp(queryScalarText(db, "SELECT k FROM t WHERE id=2"), "2")==0);
  check("shadow_same_name_id3_present_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE id=3"), "1")==0);
  check("shadow_same_name_id4_absent_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE id=4"), "0")==0);
  check("shadow_same_name_k11_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE k=11"), "1")==0);
  check("shadow_same_name_k22_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE k=22"), "0")==0);
  check("shadow_same_name_integrity_after_reopen",
        strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_mutmap_rollback_stale_order(void){
  sqlite3 *db = 0;
  char dbpath[256];
  char *err = 0;
  int rc;

  printf("=== Mutmap Rollback Stale Order Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_mutmap_rollback_stale_order");
  removeDbFiles(dbpath);

  check("open_db_for_mutmap_stale_order", open_db(dbpath, &db)==SQLITE_OK);
  check("fk_on_for_mutmap_stale_order", execSql(db, "PRAGMA foreign_keys=ON;")==SQLITE_OK);
  check("setup_for_mutmap_stale_order", execSql(db,
    "CREATE TABLE node(nodeid PRIMARY KEY, "
    "  parent REFERENCES node DEFERRABLE INITIALLY DEFERRED);"
    "CREATE TABLE leaf(cellid PRIMARY KEY, "
    "  parent REFERENCES node DEFERRABLE INITIALLY DEFERRED);"
    "INSERT INTO node VALUES(1,NULL),(2,NULL);")==SQLITE_OK);
  check("prior_txn_delete_for_mutmap_stale_order", execSql(db,
    "BEGIN; DELETE FROM node WHERE nodeid=1; COMMIT;")==SQLITE_OK);
  check("begin_trigger_txn_for_mutmap_stale_order", execSql(db, "BEGIN;")==SQLITE_OK);
  check("delete_all_for_mutmap_stale_order", execSql(db,
    "DELETE FROM leaf; DELETE FROM node;")==SQLITE_OK);
  check("insert_leaf_for_mutmap_stale_order", execSql(db,
    "INSERT INTO leaf VALUES(91,1),(92,2),(93,1);")==SQLITE_OK);
  rc = sqlite3_exec(db, "INSERT INTO node SELECT parent,3 FROM leaf;", 0, 0, &err);
  sqlite3_free(err); err = 0;
  check("failed_insert_aborts_for_mutmap_stale_order", rc!=SQLITE_OK);
  check("node_readable_and_empty_after_failed_insert",
    strcmp(queryScalarText(db, "SELECT count(*) FROM node"), "0")==0);
  check("integrity_ok_after_failed_insert",
    strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);
  check("rollback_trigger_txn_for_mutmap_stale_order", execSql(db, "ROLLBACK;")==SQLITE_OK);

  sqlite3_close(db);
}

static void run_savepoint_schema_rollback_reopen(void){
  sqlite3 *db = 0;
  char dbpath[256];

  printf("=== Savepoint Schema Rollback Reopen Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_savepoint_schema_rollback_reopen");
  removeDbFiles(dbpath);

  check("open_db_for_savepoint_schema_rollback", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_savepoint_schema_rollback", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1, 'a');"
    "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);

  check("begin_txn_for_savepoint_schema_rollback",
        execSql(db, "BEGIN IMMEDIATE;")==SQLITE_OK);
  check("savepoint_outer_for_schema_rollback",
        execSql(db, "SAVEPOINT outer_sp;")==SQLITE_OK);
  check("outer_schema_rollback_edit",
        execSql(db, "UPDATE t SET v='outer' WHERE id=1;")==SQLITE_OK);
  check("savepoint_inner_for_schema_rollback",
        execSql(db, "SAVEPOINT inner_sp;")==SQLITE_OK);
  check("inner_schema_changes",
        execSql(db,
          "ALTER TABLE t ADD COLUMN extra TEXT;"
          "CREATE TABLE aux(id INTEGER PRIMARY KEY, note TEXT);"
          "INSERT INTO aux VALUES(1, 'tmp');")==SQLITE_OK);
  check("rollback_inner_schema_changes",
        execSql(db, "ROLLBACK TO inner_sp;")==SQLITE_OK);
  check("release_inner_schema_changes",
        execSql(db, "RELEASE inner_sp;")==SQLITE_OK);
  check("release_outer_schema_changes",
        execSql(db, "RELEASE outer_sp;")==SQLITE_OK);
  check("commit_schema_rollback",
        execSql(db, "COMMIT;")==SQLITE_OK);

  check("schema_rollback_row_before_close",
        strcmp(queryScalarText(db, "SELECT v FROM t WHERE id=1"), "outer")==0);
  check("schema_rollback_extra_absent_before_close",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM pragma_table_info('t') WHERE name='extra'"), "0")==0);
  check("schema_rollback_aux_absent_before_close",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='aux'"), "0")==0);
  check("schema_rollback_integrity_before_close",
        strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_for_savepoint_schema_rollback", open_db(dbpath, &db)==SQLITE_OK);
  check("schema_rollback_row_after_reopen",
        strcmp(queryScalarText(db, "SELECT v FROM t WHERE id=1"), "outer")==0);
  check("schema_rollback_extra_absent_after_reopen",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM pragma_table_info('t') WHERE name='extra'"), "0")==0);
  check("schema_rollback_aux_absent_after_reopen",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='aux'"), "0")==0);
  check("schema_rollback_integrity_after_reopen",
        strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_savepoint_trigger_rollback_reopen(void){
  sqlite3 *db = 0;
  char dbpath[256];

  printf("=== Savepoint Trigger Rollback Reopen Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_savepoint_trigger_rollback_reopen");
  removeDbFiles(dbpath);

  check("open_db_for_savepoint_trigger_rollback", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_savepoint_trigger_rollback", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "CREATE TABLE audit(msg TEXT);"
    "CREATE TRIGGER t_au AFTER UPDATE ON t BEGIN "
      "INSERT INTO audit VALUES('u:' || NEW.id || ':' || NEW.v);"
    "END;"
    "INSERT INTO t VALUES(1, 'a');"
    "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);

  check("begin_txn_for_savepoint_trigger_rollback",
        execSql(db, "BEGIN IMMEDIATE;")==SQLITE_OK);
  check("savepoint_outer_for_trigger_rollback",
        execSql(db, "SAVEPOINT outer_sp;")==SQLITE_OK);
  check("outer_trigger_edit",
        execSql(db, "UPDATE t SET v='outer' WHERE id=1;")==SQLITE_OK);
  check("savepoint_inner_for_trigger_rollback",
        execSql(db, "SAVEPOINT inner_sp;")==SQLITE_OK);
  check("inner_trigger_edit",
        execSql(db, "UPDATE t SET v='inner' WHERE id=1;")==SQLITE_OK);
  check("rollback_inner_trigger_edit",
        execSql(db, "ROLLBACK TO inner_sp;")==SQLITE_OK);
  check("release_inner_trigger_edit",
        execSql(db, "RELEASE inner_sp;")==SQLITE_OK);
  check("release_outer_trigger_edit",
        execSql(db, "RELEASE outer_sp;")==SQLITE_OK);
  check("commit_trigger_rollback",
        execSql(db, "COMMIT;")==SQLITE_OK);

  check("trigger_rollback_row_before_close",
        strcmp(queryScalarText(db, "SELECT v FROM t WHERE id=1"), "outer")==0);
  check("trigger_rollback_audit_count_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM audit"), "1")==0);
  check("trigger_rollback_audit_msg_before_close",
        strcmp(queryScalarText(db, "SELECT msg FROM audit LIMIT 1"), "u:1:outer")==0);
  check("trigger_rollback_integrity_before_close",
        strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_for_savepoint_trigger_rollback", open_db(dbpath, &db)==SQLITE_OK);
  check("trigger_rollback_row_after_reopen",
        strcmp(queryScalarText(db, "SELECT v FROM t WHERE id=1"), "outer")==0);
  check("trigger_rollback_audit_count_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM audit"), "1")==0);
  check("trigger_rollback_audit_msg_after_reopen",
        strcmp(queryScalarText(db, "SELECT msg FROM audit LIMIT 1"), "u:1:outer")==0);
  check("trigger_rollback_integrity_after_reopen",
        strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_begin_release_then_outer_rollback_reopen(void){
  sqlite3 *db = 0;
  char dbpath[256];

  printf("=== Begin Release Then Outer Rollback Reopen Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath),
              "test_begin_release_then_outer_rollback_reopen");
  removeDbFiles(dbpath);

  check("open_db_for_begin_release_then_outer_rollback", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_begin_release_then_outer_rollback", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER, v TEXT);"
    "CREATE INDEX k_idx ON t(k);"
    "INSERT INTO t VALUES(1, 1, 'a');"
    "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);

  check("begin_txn_for_begin_release_then_outer_rollback",
        execSql(db, "BEGIN IMMEDIATE;")==SQLITE_OK);
  check("savepoint_outer_for_begin_release_then_outer_rollback",
        execSql(db, "SAVEPOINT outer_sp;")==SQLITE_OK);
  check("outer_edit_for_begin_release_then_outer_rollback",
        execSql(db, "UPDATE t SET k=11, v='outer' WHERE id=1;")==SQLITE_OK);
  check("savepoint_inner_for_begin_release_then_outer_rollback",
        execSql(db, "SAVEPOINT inner_sp;")==SQLITE_OK);
  check("inner_edit_for_begin_release_then_outer_rollback",
        execSql(db, "INSERT INTO t VALUES(2, 22, 'inner2');")==SQLITE_OK);
  check("release_inner_for_begin_release_then_outer_rollback",
        execSql(db, "RELEASE inner_sp;")==SQLITE_OK);
  check("rollback_outer_for_begin_release_then_outer_rollback",
        execSql(db, "ROLLBACK;")==SQLITE_OK);

  check("begin_release_outer_rollback_row1_before_close",
        strcmp(queryScalarText(db, "SELECT k FROM t WHERE id=1"), "1")==0);
  check("begin_release_outer_rollback_row2_absent_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE id=2"), "0")==0);
  check("begin_release_outer_rollback_k11_absent_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE k=11"), "0")==0);
  check("begin_release_outer_rollback_status_clean_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_status"), "0")==0);
  check("begin_release_outer_rollback_integrity_before_close",
        strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_for_begin_release_then_outer_rollback", open_db(dbpath, &db)==SQLITE_OK);
  check("begin_release_outer_rollback_row1_after_reopen",
        strcmp(queryScalarText(db, "SELECT k FROM t WHERE id=1"), "1")==0);
  check("begin_release_outer_rollback_row2_absent_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE id=2"), "0")==0);
  check("begin_release_outer_rollback_k11_absent_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE k=11"), "0")==0);
  check("begin_release_outer_rollback_status_clean_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_status"), "0")==0);
  check("begin_release_outer_rollback_integrity_after_reopen",
        strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_savepoint_failed_commit_schema_rollback_reopen(void){
  sqlite3 *db = 0;
  char dbpath[256];
  char zHeadBefore[128];

  printf("=== Savepoint Failed Commit Schema Rollback Reopen Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath),
              "test_savepoint_failed_commit_schema_rollback_reopen");
  removeDbFiles(dbpath);

  check("open_db_for_savepoint_failed_commit_schema_rollback",
        open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_savepoint_failed_commit_schema_rollback", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1, 'a');"
    "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);
  sqlite3_snprintf(sizeof(zHeadBefore), zHeadBefore, "%s",
                   queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"));

  check("begin_txn_for_savepoint_failed_commit_schema_rollback",
        execSql(db, "BEGIN IMMEDIATE;")==SQLITE_OK);
  check("savepoint_outer_for_savepoint_failed_commit_schema_rollback",
        execSql(db, "SAVEPOINT outer_sp;")==SQLITE_OK);
  check("outer_edit_for_savepoint_failed_commit_schema_rollback",
        execSql(db, "UPDATE t SET v='outer' WHERE id=1;")==SQLITE_OK);
  check("savepoint_inner_for_savepoint_failed_commit_schema_rollback",
        execSql(db, "SAVEPOINT inner_sp;")==SQLITE_OK);
  check("inner_schema_for_savepoint_failed_commit_schema_rollback",
        execSql(db,
          "ALTER TABLE t ADD COLUMN extra TEXT;"
          "CREATE TABLE aux(id INTEGER PRIMARY KEY, note TEXT);"
          "INSERT INTO aux VALUES(1, 'tmp');")==SQLITE_OK);

  check("rollback_inner_schema_after_failed_commit",
        execSql(db, "ROLLBACK TO inner_sp;")==SQLITE_OK);
  check("release_inner_schema_after_failed_commit",
        execSql(db, "RELEASE inner_sp;")==SQLITE_OK);
  check("release_outer_schema_after_failed_commit",
        execSql(db, "RELEASE outer_sp;")==SQLITE_OK);
  check("commit_after_failed_commit_schema_rollback",
        execSql(db, "COMMIT;")==SQLITE_OK);

  check("failed_commit_schema_row_before_close",
        strcmp(queryScalarText(db, "SELECT v FROM t WHERE id=1"), "outer")==0);
  check("failed_commit_schema_extra_absent_before_close",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM pragma_table_info('t') WHERE name='extra'"), "0")==0);
  check("failed_commit_schema_aux_absent_before_close",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='aux'"), "0")==0);
  check("failed_commit_schema_head_unchanged_before_close",
        strcmp(queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"), zHeadBefore)==0);
  check("failed_commit_schema_status_dirty_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_status"), "1")==0);
  check("failed_commit_schema_integrity_before_close",
        strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_for_savepoint_failed_commit_schema_rollback",
        open_db(dbpath, &db)==SQLITE_OK);
  check("failed_commit_schema_row_after_reopen",
        strcmp(queryScalarText(db, "SELECT v FROM t WHERE id=1"), "outer")==0);
  check("failed_commit_schema_extra_absent_after_reopen",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM pragma_table_info('t') WHERE name='extra'"), "0")==0);
  check("failed_commit_schema_aux_absent_after_reopen",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='aux'"), "0")==0);
  check("failed_commit_schema_head_unchanged_after_reopen",
        strcmp(queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"), zHeadBefore)==0);
  check("failed_commit_schema_status_dirty_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_status"), "1")==0);
  check("failed_commit_schema_integrity_after_reopen",
        strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_savepoint_nested_trigger_inner_rollback_reopen(void){
  sqlite3 *db = 0;
  char dbpath[256];

  printf("=== Savepoint Nested Trigger Inner Rollback Reopen Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath),
              "test_savepoint_nested_trigger_inner_rollback_reopen");
  removeDbFiles(dbpath);

  check("open_db_for_savepoint_nested_trigger_inner_rollback",
        open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_savepoint_nested_trigger_inner_rollback", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "CREATE TABLE audit(msg TEXT);"
    "CREATE TRIGGER t_au AFTER UPDATE ON t BEGIN "
      "INSERT INTO audit VALUES('u:' || NEW.id || ':' || NEW.v);"
    "END;"
    "INSERT INTO t VALUES(1, 'a');"
    "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);

  check("begin_txn_for_savepoint_nested_trigger_inner_rollback",
        execSql(db, "BEGIN IMMEDIATE;")==SQLITE_OK);
  check("savepoint_outer_for_nested_trigger_inner_rollback",
        execSql(db, "SAVEPOINT outer_sp;")==SQLITE_OK);
  check("outer_trigger_edit_for_nested_trigger_inner_rollback",
        execSql(db, "UPDATE t SET v='outer' WHERE id=1;")==SQLITE_OK);
  check("savepoint_inner_for_nested_trigger_inner_rollback",
        execSql(db, "SAVEPOINT inner_sp;")==SQLITE_OK);
  check("inner_trigger_edit_for_nested_trigger_inner_rollback",
        execSql(db, "UPDATE t SET v='inner' WHERE id=1;")==SQLITE_OK);
  check("rollback_inner_for_nested_trigger_inner_rollback",
        execSql(db, "ROLLBACK TO inner_sp;")==SQLITE_OK);
  check("release_inner_for_nested_trigger_inner_rollback",
        execSql(db, "RELEASE inner_sp;")==SQLITE_OK);
  check("release_outer_for_nested_trigger_inner_rollback",
        execSql(db, "RELEASE outer_sp;")==SQLITE_OK);
  check("commit_nested_trigger_inner_rollback",
        execSql(db, "COMMIT;")==SQLITE_OK);

  check("nested_trigger_row_before_close",
        strcmp(queryScalarText(db, "SELECT v FROM t WHERE id=1"), "outer")==0);
  check("nested_trigger_audit_count_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM audit"), "1")==0);
  check("nested_trigger_audit_msg_before_close",
        strcmp(queryScalarText(db, "SELECT msg FROM audit LIMIT 1"), "u:1:outer")==0);
  check("nested_trigger_integrity_before_close",
        strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_for_savepoint_nested_trigger_inner_rollback",
        open_db(dbpath, &db)==SQLITE_OK);
  check("nested_trigger_row_after_reopen",
        strcmp(queryScalarText(db, "SELECT v FROM t WHERE id=1"), "outer")==0);
  check("nested_trigger_audit_count_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM audit"), "1")==0);
  check("nested_trigger_audit_msg_after_reopen",
        strcmp(queryScalarText(db, "SELECT msg FROM audit LIMIT 1"), "u:1:outer")==0);
  check("nested_trigger_integrity_after_reopen",
        strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_hard_reset_failure_restores_memory_state(void){
  sqlite3 *db = 0;
  char dbpath[256];
  ProllyHash headCatHash;
  int rc;

  printf("=== Hard Reset Failure Restores Memory State Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_hard_reset_failure_restores_memory_state");
  removeDbFiles(dbpath);
  gFailWriteOnce = 0;
  gFailSyncOnce = 0;
  gFailHits = 0;

  check("register_fail_vfs_for_hard_reset", registerFailVfs()==SQLITE_OK);
  check("open_fail_db_for_hard_reset", open_fail_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_hard_reset_failure", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "INSERT INTO t VALUES(2,'b');")==SQLITE_OK);
  check("working_row_visible_before_hard_reset",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "2")==0);
  check("get_head_catalog_for_hard_reset",
        doltliteGetHeadCatalogHash(db, &headCatHash)==SQLITE_OK);

  gFailHits = 0;
  gFailWriteOnce = 1;
  /* Production callers reach doltliteHardReset from inside sqlite3_step with
  ** the connection mutex held; it resets connection schemas, which asserts. */
  sqlite3_mutex_enter(sqlite3_db_mutex(db));
  rc = doltliteHardReset(db, &headCatHash);
  sqlite3_mutex_leave(sqlite3_db_mutex(db));
  check("hard_reset_failure_injected", gFailHits>0);
  check("hard_reset_returns_error_on_commit_failure", rc!=SQLITE_OK);
  check("failed_hard_reset_preserves_memory_state",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "2")==0);

  sqlite3_close(db);
  check("reopen_after_failed_hard_reset", open_db(dbpath, &db)==SQLITE_OK);
  check("failed_hard_reset_preserves_durable_state",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "2")==0);
  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_hard_reset_command_failure_preserves_durable_state(void){
  sqlite3 *db = 0;
  char dbpath[256];
  const char *res;
  char zHeadBefore[128];

  printf("=== Hard Reset Command Failure Preserves Durable State Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_hard_reset_command_failure_preserves_durable_state");
  removeDbFiles(dbpath);
  gFailWriteOnce = 0;
  gFailSyncOnce = 0;
  gFailHits = 0;

  check("register_fail_vfs_for_hard_reset_command", registerFailVfs()==SQLITE_OK);
  check("open_fail_db_for_hard_reset_command", open_fail_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_hard_reset_command_failure", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "INSERT INTO t VALUES(2,'b');")==SQLITE_OK);
  sqlite3_snprintf(sizeof(zHeadBefore), zHeadBefore, "%s",
                   queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"));
  check("hard_reset_command_working_rows_before_failure",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "2")==0);
  check("hard_reset_command_status_before_failure",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_status"), "1")==0);

  gFailHits = 0;
  gFailWriteOnce = 1;
  res = queryScalarText(db, "SELECT dolt_reset('--hard')");
  check("hard_reset_command_failure_injected", gFailHits>0);
  check("hard_reset_command_returns_error", strstr(res, "ERROR:")!=0);
  check("hard_reset_command_preserves_memory_rows",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "2")==0);
  check("hard_reset_command_preserves_memory_status",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_status"), "1")==0);
  check("hard_reset_command_preserves_memory_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("hard_reset_command_preserves_memory_head",
        strcmp(queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"), zHeadBefore)==0);

  sqlite3_close(db);
  db = 0;

  check("reopen_after_failed_hard_reset_command", open_db(dbpath, &db)==SQLITE_OK);
  check("failed_hard_reset_command_preserves_durable_rows",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "2")==0);
  check("failed_hard_reset_command_preserves_durable_status",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_status"), "1")==0);
  check("failed_hard_reset_command_preserves_durable_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("failed_hard_reset_command_preserves_durable_head",
        strcmp(queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"), zHeadBefore)==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_amend_persist_failure_preserves_durable_state(void){
  sqlite3 *db = 0;
  char dbpath[256];
  const char *res;
  char zHeadBefore[128];

  printf("=== Amend Persist Failure Preserves Durable State Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_amend_persist_failure_preserves_durable_state");
  removeDbFiles(dbpath);
  gFailWriteOnce = 0;
  gFailSyncOnce = 0;
  gFailHits = 0;

  check("register_fail_vfs_for_amend", registerFailVfs()==SQLITE_OK);
  check("open_fail_db_for_amend", open_fail_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_amend_failure", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "INSERT INTO t VALUES(2,'b');"
    "SELECT dolt_commit('-A', '-m', 'second');")==SQLITE_OK);
  sqlite3_snprintf(sizeof(zHeadBefore), zHeadBefore, "%s",
                   queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"));

  gFailHits = 0;
  gFailWriteOnce = 1;
  res = queryScalarText(db, "SELECT dolt_commit('--amend', '-m', 'amended')");
  check("amend_failure_was_injected", gFailHits>0);
  check("amend_returns_error_on_persist_failure", strstr(res, "ERROR:")!=0);
  check("amend_failure_keeps_active_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("amend_failure_keeps_rows",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "2")==0);
  check("amend_failure_keeps_head",
        strcmp(queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"), zHeadBefore)==0);
  check("amend_failure_keeps_message",
        strcmp(queryScalarText(db, "SELECT message FROM dolt_log LIMIT 1"), "second")==0);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_after_amend_failure", open_db(dbpath, &db)==SQLITE_OK);
  check("amend_failure_persists_active_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("amend_failure_persists_rows",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "2")==0);
  check("amend_failure_persists_head",
        strcmp(queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"), zHeadBefore)==0);
  check("amend_failure_persists_message",
        strcmp(queryScalarText(db, "SELECT message FROM dolt_log LIMIT 1"), "second")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_delete_current_branch_failure_preserves_durable_state(void){
  sqlite3 *db = 0;
  char dbpath[256];
  const char *res;

  printf("=== Delete Current Branch Failure Preserves Durable State Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_delete_current_branch_failure_preserves_durable_state");
  removeDbFiles(dbpath);

  check("open_db_for_delete_current_branch_failure", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_delete_current_branch_failure", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "SELECT dolt_branch('feature');")==SQLITE_OK);

  res = queryScalarText(db, "SELECT dolt_branch('-d', 'main')");
  check("delete_current_branch_returns_error",
        strstr(res, "ERROR: cannot delete the current branch")!=0);
  checkBranchState(db, "delete_current_branch_keeps", "main", "2",
                   "feature", "1");

  sqlite3_close(db);
  db = 0;

  check("reopen_db_after_delete_current_branch_failure", open_db(dbpath, &db)==SQLITE_OK);
  checkBranchState(db, "delete_current_branch_persists", "main", "2",
                   "feature", "1");

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_delete_missing_branch_preserves_durable_state(void){
  sqlite3 *db = 0;
  char dbpath[256];
  const char *res;

  printf("=== Delete Missing Branch Preserves Durable State Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_delete_missing_branch_preserves_durable_state");
  removeDbFiles(dbpath);

  check("open_db_for_delete_missing_branch", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_delete_missing_branch", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "SELECT dolt_branch('feature');")==SQLITE_OK);

  res = queryScalarText(db, "SELECT dolt_branch('-d', 'nope')");
  check("delete_missing_branch_returns_error",
        strstr(res, "ERROR: branch not found")!=0);
  checkBranchState(db, "delete_missing_branch_keeps", "main", "2",
                   "feature", "1");

  sqlite3_close(db);
  db = 0;

  check("reopen_db_after_delete_missing_branch", open_db(dbpath, &db)==SQLITE_OK);
  checkBranchState(db, "delete_missing_branch_persists", "main", "2",
                   "feature", "1");

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_force_delete_missing_branch_preserves_durable_state(void){
  sqlite3 *db = 0;
  char dbpath[256];
  const char *res;

  printf("=== Force Delete Missing Branch Preserves Durable State Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_force_delete_missing_branch_preserves_durable_state");
  removeDbFiles(dbpath);

  check("open_db_for_force_delete_missing_branch", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_force_delete_missing_branch", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "SELECT dolt_branch('feature');")==SQLITE_OK);

  res = queryScalarText(db, "SELECT dolt_branch('-D', 'nope')");
  check("force_delete_missing_branch_returns_error",
        strstr(res, "ERROR: branch not found")!=0);
  checkBranchState(db, "force_delete_missing_branch_keeps", "main", "2",
                   "feature", "1");

  sqlite3_close(db);
  db = 0;

  check("reopen_db_after_force_delete_missing_branch", open_db(dbpath, &db)==SQLITE_OK);
  checkBranchState(db, "force_delete_missing_branch_persists", "main", "2",
                   "feature", "1");

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_rebase_continue_invalid_plan_preserves_durable_state(void){
  sqlite3 *db = 0;
  char dbpath[256];
  const char *res;
  u8 isRebasing = 0;
  const char *zOrigBranch = 0;

  printf("=== Rebase Continue Invalid Plan Preserves Durable State Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath),
              "test_rebase_continue_invalid_plan_preserves_durable_state");
  removeDbFiles(dbpath);

  check("open_db_for_rebase_invalid_plan", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_rebase_invalid_plan", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);"
    "INSERT INTO t VALUES (1, 1);"
    "SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');"
    "SELECT dolt_checkout('-b', 'feat');"
    "INSERT INTO t VALUES (2, 2);"
    "SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f1');"
    "INSERT INTO t VALUES (3, 3);"
    "SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f2');"
    "INSERT INTO t VALUES (4, 4);"
    "SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f3');"
    "SELECT dolt_checkout('main');"
    "INSERT INTO t VALUES (10, 10);"
    "SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'm');"
    "SELECT dolt_checkout('feat');"
    "SELECT dolt_rebase('-i', 'main');")==SQLITE_OK);

  res = queryScalarText(db, "UPDATE dolt_rebase SET action = 'oops' WHERE commit_message = 'f1'");
  check("rebase_invalid_plan_returns_error",
        strcmp(res, "")==0);
  check("rebase_invalid_plan_keeps_working_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "dolt_rebase_feat")==0);
  check("rebase_invalid_plan_keeps_plan_table",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_rebase"), "3")==0);
  doltliteGetSessionRebaseState(db, &isRebasing, 0, 0, &zOrigBranch, 0);
  check("rebase_invalid_plan_keeps_rebase_flag", isRebasing==1);
  check("rebase_invalid_plan_keeps_orig_branch",
        zOrigBranch && strcmp(zOrigBranch, "feat")==0);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_after_rebase_invalid_plan", open_db(dbpath, &db)==SQLITE_OK);
  check("rebase_invalid_plan_persists_working_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("rebase_invalid_plan_persists_plan_table",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_rebase"), "3")==0);
  doltliteGetSessionRebaseState(db, &isRebasing, 0, 0, &zOrigBranch, 0);
  check("rebase_invalid_plan_persists_rebase_flag", isRebasing==1);
  check("rebase_invalid_plan_persists_orig_branch",
        zOrigBranch && strcmp(zOrigBranch, "feat")==0);
  check("rebase_invalid_plan_abort_after_reopen",
        strcmp(queryScalarText(db, "SELECT dolt_rebase('--abort')"), "Interactive rebase aborted")==0);
  check("rebase_invalid_plan_abort_restores_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "feat")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_rebase_abort_after_reopen_restores_durable_state(void){
  sqlite3 *db = 0;
  char dbpath[256];
  u8 isRebasing = 0;
  const char *zOrigBranch = 0;
  char zHeadBefore[128];
  RepoStateSnapshot beforeReopenAbort;
  RepoStateSnapshot afterReopenAbort;

  printf("=== Rebase Abort After Reopen Restores Durable State Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_rebase_abort_after_reopen_restores_durable_state");
  removeDbFiles(dbpath);

  check("open_db_for_rebase_abort_after_reopen", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_rebase_abort_after_reopen", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);"
    "INSERT INTO t VALUES (1, 1);"
    "SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');"
    "SELECT dolt_checkout('-b', 'feat');"
    "INSERT INTO t VALUES (2, 2);"
    "SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f1');"
    "INSERT INTO t VALUES (3, 3);"
    "SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f2');"
    "SELECT dolt_checkout('main');"
    "INSERT INTO t VALUES (10, 10);"
    "SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'm');"
    "SELECT dolt_checkout('feat');")==SQLITE_OK);
  sqlite3_snprintf(sizeof(zHeadBefore), zHeadBefore, "%s",
                   queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"));

  check("start_interactive_rebase_for_abort_after_reopen",
        strstr(queryScalarText(db, "SELECT dolt_rebase('-i', 'main')"),
               "interactive rebase started on branch dolt_rebase_feat")!=0);
  check("rebase_abort_after_reopen_branch_before_close",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "dolt_rebase_feat")==0);
  check("rebase_abort_after_reopen_plan_table_before_close",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_rebase"), "2")==0);
  doltliteGetSessionRebaseState(db, &isRebasing, 0, 0, &zOrigBranch, 0);
  check("rebase_abort_after_reopen_flag_before_close", isRebasing==1);
  check("rebase_abort_after_reopen_orig_branch_before_close",
        zOrigBranch && strcmp(zOrigBranch, "feat")==0);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_for_rebase_abort_after_reopen", open_db(dbpath, &db)==SQLITE_OK);
  check("rebase_abort_after_reopen_branch_before_abort",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("rebase_abort_after_reopen_plan_before_abort",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_rebase"), "2")==0);
  doltliteGetSessionRebaseState(db, &isRebasing, 0, 0, &zOrigBranch, 0);
  check("rebase_abort_after_reopen_flag_before_abort", isRebasing==1);

  check("rebase_abort_after_reopen_returns_success",
        strcmp(queryScalarText(db, "SELECT dolt_rebase('--abort')"), "Interactive rebase aborted")==0);
  check("rebase_abort_after_reopen_restores_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "feat")==0);
  check("rebase_abort_after_reopen_restores_head",
        strcmp(queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"), zHeadBefore)==0);
  check("rebase_abort_after_reopen_drops_plan",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='dolt_rebase'"), "0")==0);
  doltliteGetSessionRebaseState(db, &isRebasing, 0, 0, &zOrigBranch, 0);
  check("rebase_abort_after_reopen_clears_flag", isRebasing==0);
  capture_repo_state_snapshot(db, &beforeReopenAbort);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_after_rebase_abort", open_db(dbpath, &db)==SQLITE_OK);
  check("rebase_abort_persists_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("rebase_abort_persists_head",
        strcmp(queryScalarText(db, "SELECT message FROM dolt_log LIMIT 1"), "m")==0);
  check("rebase_abort_persists_no_plan",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='dolt_rebase'"), "0")==0);
  doltliteGetSessionRebaseState(db, &isRebasing, 0, 0, &zOrigBranch, 0);
  check("rebase_abort_persists_flag_cleared", isRebasing==0);
  capture_repo_state_snapshot(db, &afterReopenAbort);
  check("rebase_abort_reopen_table_rows_match",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "2")==0);
  check("rebase_abort_reopen_feat_head_preserved",
        strcmp(queryScalarText(db,
          "SELECT hash FROM dolt_branches WHERE name='feat'"), zHeadBefore)==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static int setup_dirty_default_rebase(sqlite3 *db){
  return execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'one');"
    "SELECT dolt_add('-A'); SELECT dolt_commit('-m','init');"
    "SELECT dolt_checkout('-b','feat');"
    "INSERT INTO t VALUES(10,'f1');"
    "SELECT dolt_add('-A'); SELECT dolt_commit('-m','f1');"
    "SELECT dolt_checkout('main');"
    "INSERT INTO t VALUES(2,'m1');"
    "SELECT dolt_add('-A'); SELECT dolt_commit('-m','m1');"
    "INSERT INTO t VALUES(99,'row99');"
    "SELECT dolt_checkout('feat');");
}

static void run_rebase_abort_after_reopen_dirty_default(void){
  sqlite3 *db = 0;
  char dbpath[256];
  u8 isRebasing = 0;
  const char *zOrigBranch = 0;

  printf("=== Rebase Abort After Reopen Dirty Default Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_rebase_abort_after_reopen_dirty_default");
  removeDbFiles(dbpath);

  check("open_db_for_rebase_abort_after_reopen_dirty",
        open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_rebase_abort_after_reopen_dirty",
        setup_dirty_default_rebase(db)==SQLITE_OK);
  check("start_interactive_rebase_dirty_default",
        strstr(queryScalarText(db, "SELECT dolt_rebase('-i', 'main')"),
               "interactive rebase started on branch dolt_rebase_feat")!=0);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_for_rebase_abort_dirty_default", open_db(dbpath, &db)==SQLITE_OK);
  check("rebase_abort_dirty_reopen_lands_on_main",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("rebase_abort_dirty_reopen_keeps_uncommitted_row",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE v='row99'"), "1")==0);
  check("rebase_abort_dirty_reopen_plan_stays_off_return",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='dolt_rebase'"),
          "0")==0);
  doltliteGetSessionRebaseState(db, &isRebasing, 0, 0, &zOrigBranch, 0);
  check("rebase_abort_dirty_reopen_flag", isRebasing==1);
  check("rebase_abort_dirty_reopen_orig_branch",
        zOrigBranch && strcmp(zOrigBranch, "feat")==0);
  check("rebase_abort_after_reopen_dirty_returns_success",
        strcmp(queryScalarText(db, "SELECT dolt_rebase('--abort')"),
               "Interactive rebase aborted")==0);
  check("rebase_abort_after_reopen_dirty_restores_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "feat")==0);
  check("rebase_abort_after_reopen_dirty_checkout_main",
        strcmp(queryScalarText(db, "SELECT dolt_checkout('main')"), "0")==0);
  check("rebase_abort_after_reopen_dirty_row_survives",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE v='row99'"), "1")==0);
  check("rebase_abort_after_reopen_dirty_drops_temp_branch",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM dolt_branches WHERE name='dolt_rebase_feat'"), "0")==0);
  doltliteGetSessionRebaseState(db, &isRebasing, 0, 0, 0, 0);
  check("rebase_abort_after_reopen_dirty_clears_flag", isRebasing==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_rebase_continue_after_reopen_dirty_default(void){
  sqlite3 *db = 0;
  char dbpath[256];
  u8 isRebasing = 0;

  printf("=== Rebase Continue After Reopen Dirty Default Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath),
              "test_rebase_continue_after_reopen_dirty_default");
  removeDbFiles(dbpath);

  check("open_db_for_rebase_continue_after_reopen_dirty",
        open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_rebase_continue_after_reopen_dirty",
        setup_dirty_default_rebase(db)==SQLITE_OK);
  check("start_interactive_rebase_dirty_default_for_continue",
        strstr(queryScalarText(db, "SELECT dolt_rebase('-i', 'main')"),
               "interactive rebase started on branch dolt_rebase_feat")!=0);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_for_rebase_continue_dirty_default", open_db(dbpath, &db)==SQLITE_OK);
  check("rebase_continue_dirty_reopen_lands_on_main",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("rebase_continue_dirty_reopen_keeps_uncommitted_row",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE v='row99'"), "1")==0);
  doltliteGetSessionRebaseState(db, &isRebasing, 0, 0, 0, 0);
  check("rebase_continue_dirty_reopen_flag", isRebasing==1);
  check("rebase_continue_after_reopen_dirty_succeeds",
        strcmp(queryScalarText(db, "SELECT dolt_rebase('--continue')"),
               "Successfully rebased and updated refs/heads/feat")==0);
  check("rebase_continue_after_reopen_dirty_checkout_main",
        strcmp(queryScalarText(db, "SELECT dolt_checkout('main')"), "0")==0);
  check("rebase_continue_after_reopen_dirty_row_survives",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE v='row99'"), "1")==0);
  check("rebase_continue_after_reopen_dirty_drops_temp_branch",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM dolt_branches WHERE name='dolt_rebase_feat'"), "0")==0);
  doltliteGetSessionRebaseState(db, &isRebasing, 0, 0, 0, 0);
  check("rebase_continue_after_reopen_dirty_clears_flag", isRebasing==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_rebase_continue_cas_keeps_dirty_default(void){
  sqlite3 *db1 = 0;
  sqlite3 *db2 = 0;
  sqlite3 *db3 = 0;
  char dbpath[256];
  const char *res;
  u8 isRebasing = 0;

  printf("=== Rebase Continue CAS Keeps Dirty Default Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath),
              "test_rebase_continue_cas_keeps_dirty_default");
  removeDbFiles(dbpath);

  check("open_db1_for_rebase_continue_cas_dirty",
        open_db(dbpath, &db1)==SQLITE_OK);
  check("setup_repo_for_rebase_continue_cas_dirty",
        setup_dirty_default_rebase(db1)==SQLITE_OK);
  check("start_rebase_for_continue_cas_dirty",
        strstr(queryScalarText(db1, "SELECT dolt_rebase('-i', 'main')"),
               "interactive rebase started on branch dolt_rebase_feat")!=0);
  check("drop_plan_for_continue_cas_dirty",
        execSql(db1, "UPDATE dolt_rebase SET action='drop';")==SQLITE_OK);

  check("open_db2_for_rebase_continue_cas_dirty",
        open_db(dbpath, &db2)==SQLITE_OK);
  check("checkout_feat_for_continue_cas_dirty",
        strcmp(queryScalarText(db2, "SELECT dolt_checkout('feat')"), "0")==0);
  check("peer_commit_for_continue_cas_dirty", execSql(db2,
    "INSERT INTO t VALUES(123,'peer');"
    "SELECT dolt_commit('-A','-m','move feat');")==SQLITE_OK);

  res = queryScalarText(db1, "SELECT dolt_rebase('--continue')");
  check("continue_cas_dirty_names_source_branch",
        strstr(res, "changes in branch feat")!=0);
  check("continue_cas_dirty_checkout_main",
        strcmp(queryScalarText(db1, "SELECT dolt_checkout('main')"), "0")==0);
  check("continue_cas_dirty_row_survives_same_session",
        strcmp(queryScalarText(db1, "SELECT count(*) FROM t WHERE v='row99'"),
               "1")==0);

  sqlite3_close(db2);
  sqlite3_close(db1);

  check("reopen_after_continue_cas_dirty", open_db(dbpath, &db3)==SQLITE_OK);
  check("continue_cas_dirty_row_survives_reopen",
        strcmp(queryScalarText(db3, "SELECT count(*) FROM t WHERE v='row99'"),
               "1")==0);
  check("continue_cas_dirty_keeps_temp_branch",
        strcmp(queryScalarText(db3,
          "SELECT count(*) FROM dolt_branches WHERE name='dolt_rebase_feat'"),
          "1")==0);
  doltliteGetSessionRebaseState(db3, &isRebasing, 0, 0, 0, 0);
  check("continue_cas_dirty_keeps_flag", isRebasing==1);
  check("continue_cas_dirty_abort_after",
        strcmp(queryScalarText(db3, "SELECT dolt_rebase('--abort')"),
               "Interactive rebase aborted")==0);
  check("continue_cas_dirty_abort_checkout_main",
        strcmp(queryScalarText(db3, "SELECT dolt_checkout('main')"), "0")==0);
  check("continue_cas_dirty_row_survives_abort",
        strcmp(queryScalarText(db3, "SELECT count(*) FROM t WHERE v='row99'"),
               "1")==0);
  check("continue_cas_dirty_drops_temp_branch",
        strcmp(queryScalarText(db3,
          "SELECT count(*) FROM dolt_branches WHERE name='dolt_rebase_feat'"),
          "0")==0);

  sqlite3_close(db3);
  removeDbFiles(dbpath);
}

static int setup_onto_other_rebase(sqlite3 *db){
  return execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'base');"
    "SELECT dolt_add('-A'); SELECT dolt_commit('-m','init');"
    "SELECT dolt_checkout('-b','other');"
    "INSERT INTO t VALUES(2,'other');"
    "SELECT dolt_add('-A'); SELECT dolt_commit('-m','other');"
    "SELECT dolt_checkout('main');"
    "SELECT dolt_checkout('-b','feat');"
    "INSERT INTO t VALUES(3,'feat');"
    "SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat');");
}

static void run_rebase_abort_after_reopen_onto_other(void){
  sqlite3 *db = 0;
  char dbpath[256];
  u8 isRebasing = 0;
  const char *zOrigBranch = 0;

  printf("=== Rebase Abort After Reopen Onto Other Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_rebase_abort_after_reopen_onto_other");
  removeDbFiles(dbpath);

  check("open_db_for_rebase_abort_after_reopen_onto_other",
        open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_rebase_abort_after_reopen_onto_other",
        setup_onto_other_rebase(db)==SQLITE_OK);
  check("start_interactive_rebase_onto_other",
        strstr(queryScalarText(db, "SELECT dolt_rebase('-i', 'other')"),
               "interactive rebase started on branch dolt_rebase_feat")!=0);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_for_rebase_abort_onto_other", open_db(dbpath, &db)==SQLITE_OK);
  check("rebase_abort_onto_other_reopen_lands_on_main",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("rebase_abort_onto_other_reopen_keeps_main_rows",
        strcmp(queryScalarText(db, "SELECT group_concat(v, ',') FROM t"),
               "base")==0);
  check("rebase_abort_onto_other_reopen_plan_stays_off_return",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='dolt_rebase'"),
          "0")==0);
  doltliteGetSessionRebaseState(db, &isRebasing, 0, 0, &zOrigBranch, 0);
  check("rebase_abort_onto_other_reopen_flag", isRebasing==1);
  check("rebase_abort_onto_other_reopen_orig_branch",
        zOrigBranch && strcmp(zOrigBranch, "feat")==0);
  check("rebase_abort_after_reopen_onto_other_returns_success",
        strcmp(queryScalarText(db, "SELECT dolt_rebase('--abort')"),
               "Interactive rebase aborted")==0);
  check("rebase_abort_after_reopen_onto_other_restores_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "feat")==0);
  check("rebase_abort_after_reopen_onto_other_feat_rows",
        strcmp(queryScalarText(db, "SELECT group_concat(v, ',') FROM t"),
               "base,feat")==0);
  check("rebase_abort_after_reopen_onto_other_checkout_main",
        strcmp(queryScalarText(db, "SELECT dolt_checkout('main')"), "0")==0);
  check("rebase_abort_after_reopen_onto_other_main_rows",
        strcmp(queryScalarText(db, "SELECT group_concat(v, ',') FROM t"),
               "base")==0);
  check("rebase_abort_after_reopen_onto_other_drops_temp_branch",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM dolt_branches WHERE name='dolt_rebase_feat'"), "0")==0);
  doltliteGetSessionRebaseState(db, &isRebasing, 0, 0, 0, 0);
  check("rebase_abort_after_reopen_onto_other_clears_flag", isRebasing==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_rebase_continue_after_reopen_onto_other(void){
  sqlite3 *db = 0;
  char dbpath[256];
  u8 isRebasing = 0;
  const char *zOrigBranch = 0;

  printf("=== Rebase Continue After Reopen Onto Other Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath),
              "test_rebase_continue_after_reopen_onto_other");
  removeDbFiles(dbpath);

  check("open_db_for_rebase_continue_after_reopen_onto_other",
        open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_rebase_continue_after_reopen_onto_other",
        setup_onto_other_rebase(db)==SQLITE_OK);
  check("start_interactive_rebase_onto_other_for_continue",
        strstr(queryScalarText(db, "SELECT dolt_rebase('-i', 'other')"),
               "interactive rebase started on branch dolt_rebase_feat")!=0);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_for_rebase_continue_onto_other", open_db(dbpath, &db)==SQLITE_OK);
  check("rebase_continue_onto_other_reopen_lands_on_main",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("rebase_continue_onto_other_reopen_keeps_main_rows",
        strcmp(queryScalarText(db, "SELECT group_concat(v, ',') FROM t"),
               "base")==0);
  doltliteGetSessionRebaseState(db, &isRebasing, 0, 0, &zOrigBranch, 0);
  check("rebase_continue_onto_other_reopen_flag", isRebasing==1);
  check("rebase_continue_onto_other_reopen_orig_branch",
        zOrigBranch && strcmp(zOrigBranch, "feat")==0);
  check("rebase_continue_after_reopen_onto_other_succeeds",
        strcmp(queryScalarText(db, "SELECT dolt_rebase('--continue')"),
               "Successfully rebased and updated refs/heads/feat")==0);
  check("rebase_continue_after_reopen_onto_other_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "feat")==0);
  check("rebase_continue_after_reopen_onto_other_feat_rows",
        strcmp(queryScalarText(db, "SELECT group_concat(v, ',') FROM t"),
               "base,other,feat")==0);
  check("rebase_continue_after_reopen_onto_other_checkout_main",
        strcmp(queryScalarText(db, "SELECT dolt_checkout('main')"), "0")==0);
  check("rebase_continue_after_reopen_onto_other_main_rows",
        strcmp(queryScalarText(db, "SELECT group_concat(v, ',') FROM t"),
               "base")==0);
  check("rebase_continue_after_reopen_onto_other_drops_temp_branch",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM dolt_branches WHERE name='dolt_rebase_feat'"), "0")==0);
  doltliteGetSessionRebaseState(db, &isRebasing, 0, 0, 0, 0);
  check("rebase_continue_after_reopen_onto_other_clears_flag", isRebasing==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_rebase_abort_retry_after_claim_commit_error(void){
  sqlite3 *db = 0;
  char dbpath[256];
  const char *res;
  u8 isRebasing = 0;

  printf("=== Rebase Abort Retry After Claim Commit Error Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath),
              "test_rebase_abort_retry_after_claim_commit_error");
  removeDbFiles(dbpath);

  check("open_db_for_rebase_abort_retry_after_claim_error",
        open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_rebase_abort_retry_after_claim_error",
        setup_dirty_default_rebase(db)==SQLITE_OK);
  check("start_interactive_rebase_for_claim_commit_error",
        strstr(queryScalarText(db, "SELECT dolt_rebase('-i', 'main')"),
               "interactive rebase started on branch dolt_rebase_feat")!=0);

  sqlite3_close(db);
  db = 0;
  check("reopen_db_before_claim_commit_error", open_db(dbpath, &db)==SQLITE_OK);

  gRegressionFaultCode = 961;
  gRegressionFaultHits = 0;
  sqlite3_test_control(SQLITE_TESTCTRL_FAULT_INSTALL, regressionFaultCallback);
  res = queryScalarText(db, "SELECT dolt_rebase('--abort')");
  sqlite3_test_control(SQLITE_TESTCTRL_FAULT_INSTALL, 0);
  gRegressionFaultCode = 0;

  check("rebase_abort_claim_commit_error_was_injected", gRegressionFaultHits==1);
  check("rebase_abort_claim_commit_error_is_recovery_failure",
        strstr(res, "ERROR: rebase recovery failed")!=0);
  check("rebase_abort_claim_commit_error_does_not_report_success",
        strstr(res, "Interactive rebase aborted")==0);

  sqlite3_close(db);
  db = 0;
  check("reopen_db_after_claim_commit_error", open_db(dbpath, &db)==SQLITE_OK);
  check("rebase_abort_retry_after_claim_error_lands_on_main",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("rebase_abort_retry_after_claim_error_keeps_uncommitted_row",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE v='row99'"),
               "1")==0);
  check("rebase_abort_retry_after_claim_error_keeps_temp_branch",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM dolt_branches WHERE name='dolt_rebase_feat'"),
          "1")==0);
  doltliteGetSessionRebaseState(db, &isRebasing, 0, 0, 0, 0);
  check("rebase_abort_retry_after_claim_error_keeps_flag", isRebasing==1);
  check("rebase_abort_retry_after_claim_error_succeeds",
        strcmp(queryScalarText(db, "SELECT dolt_rebase('--abort')"),
               "Interactive rebase aborted")==0);
  check("rebase_abort_retry_after_claim_error_checkout_main",
        strcmp(queryScalarText(db, "SELECT dolt_checkout('main')"), "0")==0);
  check("rebase_abort_retry_after_claim_error_row_survives",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE v='row99'"),
               "1")==0);
  check("rebase_abort_retry_after_claim_error_drops_temp_branch",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM dolt_branches WHERE name='dolt_rebase_feat'"),
          "0")==0);
  doltliteGetSessionRebaseState(db, &isRebasing, 0, 0, 0, 0);
  check("rebase_abort_retry_after_claim_error_clears_flag", isRebasing==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_rebase_main_table_schema_guard(void){
  sqlite3 *db = 0;
  char dbpath[256];
  const char *res;
  u8 isRebasing = 0;
  const char *zOrigBranch = 0;

  printf("=== Rebase Main Table Schema Guard Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_rebase_main_table_schema_guard");
  removeDbFiles(dbpath);

  check("open_db_for_rebase_schema_guard", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_rebase_schema_guard", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);"
    "INSERT INTO t VALUES (1, 1);"
    "SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');"
    "SELECT dolt_checkout('-b', 'feat');"
    "INSERT INTO t VALUES (2, 2);"
    "SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f1');"
    "SELECT dolt_checkout('main');"
    "INSERT INTO t VALUES (10, 10);"
    "SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'm');"
    "SELECT dolt_checkout('feat');"
    "SELECT dolt_rebase('-i', 'main');"
    "DROP TABLE main.dolt_rebase;"
    "CREATE TABLE main.dolt_rebase(id INTEGER PRIMARY KEY);")==SQLITE_OK);

  res = queryScalarText(db, "SELECT dolt_rebase('--continue')");
  check("rebase_schema_guard_returns_error",
        strstr(res, "ERROR: dolt_rebase has an unexpected schema")!=0);
  check("rebase_schema_guard_keeps_working_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "dolt_rebase_feat")==0);
  doltliteGetSessionRebaseState(db, &isRebasing, 0, 0, &zOrigBranch, 0);
  check("rebase_schema_guard_keeps_rebase_flag", isRebasing==1);
  check("rebase_schema_guard_keeps_orig_branch",
        zOrigBranch && strcmp(zOrigBranch, "feat")==0);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_for_rebase_schema_guard", open_db(dbpath, &db)==SQLITE_OK);
  check("rebase_schema_guard_persists_working_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("rebase_schema_guard_abort_works",
        strcmp(queryScalarText(db, "SELECT dolt_rebase('--abort')"), "Interactive rebase aborted")==0);
  check("rebase_schema_guard_abort_restores_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "feat")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_rebase_temp_shadow_ignored(void){
  sqlite3 *db = 0;
  char dbpath[256];
  const char *res;

  printf("=== Rebase Temp Shadow Ignored Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_rebase_temp_shadow_ignored");
  removeDbFiles(dbpath);

  check("open_db_for_rebase_temp_shadow", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_rebase_temp_shadow", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);"
    "INSERT INTO t VALUES (1, 1);"
    "SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');"
    "SELECT dolt_checkout('-b', 'feat');"
    "INSERT INTO t VALUES (2, 2);"
    "SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f1');"
    "INSERT INTO t VALUES (3, 3);"
    "SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f2');"
    "SELECT dolt_checkout('main');"
    "INSERT INTO t VALUES (10, 10);"
    "SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'm');"
    "SELECT dolt_checkout('feat');"
    "SELECT dolt_rebase('-i', 'main');"
    "CREATE TEMP TABLE dolt_rebase(rebase_order REAL PRIMARY KEY, action TEXT, commit_hash TEXT, commit_message TEXT);"
    "INSERT INTO temp.dolt_rebase VALUES(1, 'oops', 'badbadbadbadbadbadbadbadbadbadbadbadbadb', 'shadow');")==SQLITE_OK);

  res = queryScalarText(db, "SELECT dolt_rebase('--continue')");
  check("rebase_temp_shadow_ignored_continue_succeeds",
        strstr(res, "Successfully rebased and updated refs/heads/feat")!=0);
  check("rebase_temp_shadow_ignored_restores_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "feat")==0);
  check("rebase_temp_shadow_ignored_rows_rebased",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "4")==0);
  check("rebase_temp_shadow_ignored_main_plan_gone",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM main.sqlite_master WHERE type='table' AND name='dolt_rebase'"), "0")==0);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_for_rebase_temp_shadow", open_db(dbpath, &db)==SQLITE_OK);
  check("rebase_temp_shadow_ignored_branch_after_reopen",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("rebase_temp_shadow_ignored_rows_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "2")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_rebase_plan_read_error_is_not_partial(void){
  sqlite3 *db = 0;
  char dbpath[256];
  const char *res;

  printf("=== Rebase Plan Read Error Is Not Partial Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_rebase_plan_read_error");
  removeDbFiles(dbpath);

  check("open_db_for_rebase_plan_read_error", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_rebase_plan_read_error", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);"
    "INSERT INTO t VALUES (1, 1);"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "SELECT dolt_checkout('-b', 'feat');"
    "INSERT INTO t VALUES (2, 2);"
    "SELECT dolt_commit('-A', '-m', 'f1');"
    "INSERT INTO t VALUES (3, 3);"
    "SELECT dolt_commit('-A', '-m', 'f2');"
    "SELECT dolt_checkout('main');"
    "INSERT INTO t VALUES (10, 10);"
    "SELECT dolt_commit('-A', '-m', 'm1');"
    "SELECT dolt_checkout('feat');")==SQLITE_OK);
  check("start_interactive_rebase_for_plan_read_error",
        strstr(queryScalarText(db, "SELECT dolt_rebase('-i', 'main')"),
               "interactive rebase started")!=0);

  gRegressionFaultCode = 951;
  gRegressionFaultHits = 0;
  sqlite3_test_control(SQLITE_TESTCTRL_FAULT_INSTALL, regressionFaultCallback);
  res = queryScalarText(db, "SELECT dolt_rebase('--continue')");
  sqlite3_test_control(SQLITE_TESTCTRL_FAULT_INSTALL, 0);
  gRegressionFaultCode = 0;

  check("rebase_plan_read_error_was_injected", gRegressionFaultHits==1);
  check("rebase_plan_read_error_is_returned",
        strstr(res, "ERROR: rebase failed")!=0);
  check("rebase_plan_read_error_does_not_claim_restoration",
        strstr(res, "branch restored to pre-rebase state")==0);
  check("rebase_plan_read_error_keeps_working_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"),
               "dolt_rebase_feat")==0);
  check("rebase_plan_read_error_keeps_full_plan",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_rebase"), "2")==0);
  check("rebase_plan_read_error_can_abort",
        strcmp(queryScalarText(db, "SELECT dolt_rebase('--abort')"),
               "Interactive rebase aborted")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_rebase_upstream_history_failure_is_atomic(void){
  sqlite3 *db = 0;
  char dbpath[256];
  char zFeatHead[128];
  char zMainHead[128];
  const char *res;
  RepoStateSnapshot before;
  RepoStateSnapshot after;
  u8 isRebasing = 0;
  int rc;

  printf("=== Rebase Upstream History Failure Is Atomic Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_rebase_upstream_history_failure");
  removeDbFiles(dbpath);

  check("open_db_for_rebase_upstream_history_failure",
        open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_rebase_upstream_history_failure", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'base');"
    "SELECT dolt_commit('-A','-m','base');"
    "SELECT dolt_checkout('-b','feat');"
    "INSERT INTO t VALUES(2,'feat');"
    "SELECT dolt_commit('-A','-m','feat');"
    "SELECT dolt_checkout('main');"
    "INSERT INTO t VALUES(3,'main');"
    "SELECT dolt_commit('-A','-m','main');"
    "SELECT dolt_checkout('feat');")==SQLITE_OK);
  sqlite3_snprintf(sizeof(zFeatHead), zFeatHead, "%s",
      queryScalarText(db, "SELECT hash FROM dolt_branches WHERE name='feat'"));
  sqlite3_snprintf(sizeof(zMainHead), zMainHead, "%s",
      queryScalarText(db, "SELECT hash FROM dolt_branches WHERE name='main'"));
  capture_repo_state_snapshot(db, &before);

  gRegressionFaultCode = 956;
  gRegressionFaultHits = 0;
  sqlite3_test_control(SQLITE_TESTCTRL_FAULT_INSTALL, regressionFaultCallback);
  res = queryScalarText(db, "SELECT dolt_rebase('-i','main')");
  rc = sqlite3_errcode(db);
  sqlite3_test_control(SQLITE_TESTCTRL_FAULT_INSTALL, 0);
  gRegressionFaultCode = 0;

  check("rebase_upstream_history_failure_was_injected",
        gRegressionFaultHits==1);
  check("rebase_upstream_history_failure_is_returned",
        rc==SQLITE_IOERR && strstr(res, "ERROR:")!=0);
  capture_repo_state_snapshot(db, &after);
  check("rebase_upstream_history_failure_preserves_session_state",
        repo_state_snapshot_eq(&before, &after));
  check("rebase_upstream_history_failure_creates_no_working_branch",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM dolt_branches "
          "WHERE name='dolt_rebase_feat'"), "0")==0);
  check("rebase_upstream_history_failure_preserves_branch_count",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_branches"), "2")==0);
  check("rebase_upstream_history_failure_preserves_feat_head",
        strcmp(queryScalarText(db,
          "SELECT hash FROM dolt_branches WHERE name='feat'"), zFeatHead)==0);
  check("rebase_upstream_history_failure_preserves_main_head",
        strcmp(queryScalarText(db,
          "SELECT hash FROM dolt_branches WHERE name='main'"), zMainHead)==0);

  sqlite3_close(db);
  db = 0;
  check("reopen_db_after_rebase_upstream_history_failure",
        open_db(dbpath, &db)==SQLITE_OK);
  check("rebase_upstream_history_failure_persists_no_working_branch",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM dolt_branches "
          "WHERE name='dolt_rebase_feat'"), "0")==0);
  check("rebase_upstream_history_failure_persists_no_plan",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM sqlite_master "
          "WHERE type='table' AND name='dolt_rebase'"), "0")==0);
  doltliteGetSessionRebaseState(db, &isRebasing, 0, 0, 0, 0);
  check("rebase_upstream_history_failure_persists_no_rebase_state",
        isRebasing==0);
  check("rebase_upstream_history_failure_persists_feat_head",
        strcmp(queryScalarText(db,
          "SELECT hash FROM dolt_branches WHERE name='feat'"), zFeatHead)==0);
  check("rebase_upstream_history_failure_persists_main_head",
        strcmp(queryScalarText(db,
          "SELECT hash FROM dolt_branches WHERE name='main'"), zMainHead)==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_rebase_start_failure_cleans_working_branch(void){
  sqlite3 *db = 0;
  char dbpath[256];
  const char *res;
  u8 isRebasing = 0;

  printf("=== Rebase Start Failure Cleans Working Branch Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath),
              "test_rebase_start_failure_cleans_working_branch");
  removeDbFiles(dbpath);

  check("open_db_for_rebase_start_failure", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_rebase_start_failure", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'base');"
    "SELECT dolt_commit('-A','-m','base');"
    "SELECT dolt_checkout('-b','feat');"
    "INSERT INTO t VALUES(2,'feat');"
    "SELECT dolt_commit('-A','-m','feat');"
    "SELECT dolt_checkout('main');"
    "INSERT INTO t VALUES(3,'main');"
    "SELECT dolt_commit('-A','-m','main');"
    "SELECT dolt_checkout('feat');")==SQLITE_OK);

  gRegressionFaultCode = 960;
  gRegressionFaultHits = 0;
  sqlite3_test_control(SQLITE_TESTCTRL_FAULT_INSTALL, regressionFaultCallback);
  res = queryScalarText(db, "SELECT dolt_rebase('-i','main')");
  sqlite3_test_control(SQLITE_TESTCTRL_FAULT_INSTALL, 0);
  gRegressionFaultCode = 0;

  check("rebase_start_failure_was_injected", gRegressionFaultHits==1);
  check("rebase_start_failure_is_returned", strstr(res, "ERROR:")!=0);
  check("rebase_start_failure_restores_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "feat")==0);
  check("rebase_start_failure_removes_working_branch",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM dolt_branches "
          "WHERE name='dolt_rebase_feat'"), "0")==0);
  check("rebase_start_failure_leaves_no_plan",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM sqlite_master "
          "WHERE type='table' AND name='dolt_rebase'"), "0")==0);
  doltliteGetSessionRebaseState(db, &isRebasing, 0, 0, 0, 0);
  check("rebase_start_failure_clears_rebase_state", isRebasing==0);
  check("rebase_start_failure_allows_retry",
        strstr(queryScalarText(db, "SELECT dolt_rebase('-i','main')"),
               "interactive rebase started")!=0);
  check("rebase_start_failure_retry_can_abort",
        strcmp(queryScalarText(db, "SELECT dolt_rebase('--abort')"),
               "Interactive rebase aborted")==0);

  sqlite3_close(db);
  db = 0;
  check("reopen_db_after_rebase_start_failure", open_db(dbpath, &db)==SQLITE_OK);
  check("rebase_start_failure_persists_no_working_branch",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM dolt_branches "
          "WHERE name='dolt_rebase_feat'"), "0")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_rebase_recovery_failure_is_reported(void){
  sqlite3 *db = 0;
  char dbpath[256];
  const char *res;

  printf("=== Rebase Recovery Failure Is Reported Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_rebase_recovery_failure");
  removeDbFiles(dbpath);

  check("open_db_for_rebase_recovery_failure", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_rebase_recovery_failure", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);"
    "INSERT INTO t VALUES (1, 1);"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "SELECT dolt_checkout('-b', 'feat');"
    "UPDATE t SET v=2 WHERE id=1;"
    "SELECT dolt_commit('-A', '-m', 'f1');"
    "SELECT dolt_checkout('main');"
    "UPDATE t SET v=3 WHERE id=1;"
    "SELECT dolt_commit('-A', '-m', 'm1');"
    "SELECT dolt_checkout('feat');")==SQLITE_OK);
  check("start_interactive_rebase_for_recovery_failure",
        strstr(queryScalarText(db, "SELECT dolt_rebase('-i', 'main')"),
               "interactive rebase started")!=0);

  gRegressionFaultCode = 952;
  gRegressionFaultHits = 0;
  sqlite3_test_control(SQLITE_TESTCTRL_FAULT_INSTALL, regressionFaultCallback);
  res = queryScalarText(db, "SELECT dolt_rebase('--continue')");
  sqlite3_test_control(SQLITE_TESTCTRL_FAULT_INSTALL, 0);
  gRegressionFaultCode = 0;

  check("rebase_recovery_failure_was_injected", gRegressionFaultHits==1);
  check("rebase_recovery_failure_is_returned",
        strstr(res, "ERROR: rebase recovery failed")!=0);
  check("rebase_recovery_failure_does_not_claim_restoration",
        strstr(res, "branch restored to pre-rebase state")==0);

  sqlite3_close(db);
  db = 0;
  removeDbFiles(dbpath);

  check("open_db_for_rebase_abort_recovery_failure",
        open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_rebase_abort_recovery_failure", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);"
    "INSERT INTO t VALUES (1, 1);"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "SELECT dolt_checkout('-b', 'feat');"
    "INSERT INTO t VALUES (2, 2);"
    "SELECT dolt_commit('-A', '-m', 'f1');"
    "SELECT dolt_checkout('main');"
    "INSERT INTO t VALUES (3, 3);"
    "SELECT dolt_commit('-A', '-m', 'm1');"
    "SELECT dolt_checkout('feat');")==SQLITE_OK);
  check("start_interactive_rebase_for_abort_recovery_failure",
        strstr(queryScalarText(db, "SELECT dolt_rebase('-i', 'main')"),
               "interactive rebase started")!=0);

  gRegressionFaultCode = 953;
  gRegressionFaultHits = 0;
  sqlite3_test_control(SQLITE_TESTCTRL_FAULT_INSTALL, regressionFaultCallback);
  res = queryScalarText(db, "SELECT dolt_rebase('--abort')");
  sqlite3_test_control(SQLITE_TESTCTRL_FAULT_INSTALL, 0);
  gRegressionFaultCode = 0;

  check("rebase_abort_recovery_failure_was_injected", gRegressionFaultHits==1);
  check("rebase_abort_recovery_failure_is_returned",
        strstr(res, "ERROR: rebase recovery failed")!=0);
  check("rebase_abort_recovery_failure_does_not_report_success",
        strstr(res, "Interactive rebase aborted")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_rebase_continue_conflict_abort_restores_durable_state(void){
  sqlite3 *db = 0;
  char dbpath[256];
  const char *res;
  u8 isRebasing = 0;
  const char *zOrigBranch = 0;

  printf("=== Rebase Continue Conflict Abort Restores Durable State Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath),
              "test_rebase_continue_conflict_abort_restores_durable_state");
  removeDbFiles(dbpath);

  check("open_db_for_rebase_continue_conflict_abort", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_rebase_continue_conflict_abort", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);"
    "INSERT INTO t VALUES (1, 1);"
    "SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');"
    "SELECT dolt_checkout('-b', 'feat');"
    "UPDATE t SET v = 2 WHERE id = 1;"
    "SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f1');"
    "SELECT dolt_checkout('main');"
    "UPDATE t SET v = 3 WHERE id = 1;"
    "SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'm1');"
    "SELECT dolt_checkout('feat');")==SQLITE_OK);

  check("start_interactive_rebase_for_conflict_abort",
        strstr(queryScalarText(db, "SELECT dolt_rebase('-i', 'main')"),
               "interactive rebase started on branch dolt_rebase_feat")!=0);

  res = queryScalarText(db, "SELECT dolt_rebase('--continue')");
  check("rebase_continue_conflict_abort_returns_error",
        strstr(res, "data conflicts from rebase")!=0);
  check("rebase_continue_conflict_abort_restores_branch_same_session",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "feat")==0);
  check("rebase_continue_conflict_abort_drops_plan_same_session",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='dolt_rebase'"), "0")==0);
  check("rebase_continue_conflict_abort_clears_conflicts_same_session",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_conflicts"), "0")==0);
  check("rebase_continue_conflict_abort_restores_row_same_session",
        strcmp(queryScalarText(db, "SELECT v FROM t WHERE id=1"), "2")==0);
  doltliteGetSessionRebaseState(db, &isRebasing, 0, 0, &zOrigBranch, 0);
  check("rebase_continue_conflict_abort_clears_flag_same_session", isRebasing==0);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_for_rebase_continue_conflict_abort", open_db(dbpath, &db)==SQLITE_OK);
  check("rebase_continue_conflict_abort_restores_branch_after_reopen",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("rebase_continue_conflict_abort_drops_plan_after_reopen",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='dolt_rebase'"), "0")==0);
  check("rebase_continue_conflict_abort_clears_conflicts_after_reopen",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_conflicts"), "0")==0);
  check("rebase_continue_conflict_abort_restores_row_after_reopen",
        strcmp(queryScalarText(db, "SELECT v FROM t WHERE id=1"), "3")==0);
  doltliteGetSessionRebaseState(db, &isRebasing, 0, 0, &zOrigBranch, 0);
  check("rebase_continue_conflict_abort_clears_flag_after_reopen", isRebasing==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_rebase_concurrent_peer_commit_is_preserved(void){
  sqlite3 *db1 = 0;
  sqlite3 *db2 = 0;
  sqlite3 *db3 = 0;
  char dbpath[256];
  const char *res;

  printf("=== Rebase Concurrent Peer Commit Preservation Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath),
              "test_rebase_concurrent_peer_commit_is_preserved");
  removeDbFiles(dbpath);

  check("open_db1_for_rebase_concurrent_peer", open_db(dbpath, &db1)==SQLITE_OK);
  check("setup_repo_for_rebase_concurrent_peer", execSql(db1,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'base');"
    "SELECT dolt_commit('-A','-m','base');"
    "SELECT dolt_checkout('-b','feat');"
    "INSERT INTO t VALUES(2,'feat');"
    "SELECT dolt_commit('-A','-m','feat');"
    "SELECT dolt_checkout('main');"
    "INSERT INTO t VALUES(3,'main');"
    "SELECT dolt_commit('-A','-m','main');"
    "SELECT dolt_checkout('feat');")==SQLITE_OK);
  check("start_rebase_before_peer_commit",
        strstr(queryScalarText(db1, "SELECT dolt_rebase('-i','main')"),
               "interactive rebase started")!=0);

  check("open_db2_for_rebase_concurrent_peer", open_db(dbpath, &db2)==SQLITE_OK);
  check("checkout_feat_for_rebase_concurrent_peer",
        strcmp(queryScalarText(db2, "SELECT dolt_checkout('feat')"), "0")==0);
  check("commit_rebase_concurrent_peer", execSql(db2,
    "INSERT INTO t VALUES(4,'peer');"
    "SELECT dolt_commit('-A','-m','peer');")==SQLITE_OK);

  res = queryScalarText(db1, "SELECT dolt_rebase('--continue')");
  check("rebase_rejects_concurrent_peer_commit", strstr(res, "ERROR:")!=0);
  check("rebase_rejects_concurrent_peer_names_source_branch",
        strstr(res, "changes in branch feat")!=0);

  sqlite3_close(db2);
  sqlite3_close(db1);

  check("open_db3_after_rebase_concurrent_peer", open_db(dbpath, &db3)==SQLITE_OK);
  check("rebase_concurrent_working_branch_kept",
        strcmp(queryScalarText(db3,
          "SELECT count(*) FROM dolt_branches WHERE name='dolt_rebase_feat'"), "1")==0);
  check("checkout_working_after_rebase_concurrent_peer",
        strcmp(queryScalarText(db3, "SELECT dolt_checkout('dolt_rebase_feat')"), "0")==0);
  check("rebase_concurrent_plan_kept",
        strcmp(queryScalarText(db3,
          "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='dolt_rebase'"),
               "1")==0);
  check("rebase_concurrent_abort_after_cas_reject",
        strstr(queryScalarText(db3, "SELECT dolt_rebase('--abort')"),
               "Interactive rebase aborted")!=0);
  check("checkout_feat_after_rebase_concurrent_peer",
        strcmp(queryScalarText(db3, "SELECT dolt_checkout('feat')"), "0")==0);
  check("rebase_concurrent_peer_row_is_preserved",
        strcmp(queryScalarText(db3, "SELECT v FROM t WHERE id=4"), "peer")==0);
  check("rebase_concurrent_peer_remains_head",
        strcmp(queryScalarText(db3, "SELECT message FROM dolt_log LIMIT 1"), "peer")==0);
  check("rebase_concurrent_working_branch_removed_after_abort",
        strcmp(queryScalarText(db3,
          "SELECT count(*) FROM dolt_branches WHERE name='dolt_rebase_feat'"), "0")==0);

  sqlite3_close(db3);
  removeDbFiles(dbpath);
}

static sqlite3 *gRebaseStaleDb = 0;

static void rebaseAdvanceForgetPeerBranch(void){
  ChunkStore *cs;
  if( !gRebaseStaleDb ) return;
  cs = doltliteGetChunkStore(gRebaseStaleDb);
  if( !cs ) return;
  (void)chunkStoreDeleteBranch(cs, "other");
  memset(&cs->refs.refsHash, 0, sizeof(cs->refs.refsHash));
}

static void run_rebase_continue_keeps_peer_branch(void){
  sqlite3 *db1 = 0;
  sqlite3 *db2 = 0;
  sqlite3 *db3 = 0;
  char dbpath[256];
  const char *res;

  printf("=== Rebase Continue Keeps Peer Branch Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_rebase_continue_keeps_peer_branch");
  removeDbFiles(dbpath);

  check("open_db1_for_rebase_peer_branch", open_db(dbpath, &db1)==SQLITE_OK);
  check("setup_repo_for_rebase_peer_branch", execSql(db1,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'base');"
    "SELECT dolt_commit('-A','-m','base');"
    "SELECT dolt_checkout('-b','feat');"
    "INSERT INTO t VALUES(2,'feat');"
    "SELECT dolt_commit('-A','-m','feat');"
    "SELECT dolt_checkout('main');"
    "INSERT INTO t VALUES(3,'main');"
    "SELECT dolt_commit('-A','-m','main');"
    "SELECT dolt_checkout('feat');")==SQLITE_OK);
  check("start_rebase_for_peer_branch",
        strstr(queryScalarText(db1, "SELECT dolt_rebase('-i','main')"),
               "interactive rebase started")!=0);

  check("open_db2_for_rebase_peer_branch", open_db(dbpath, &db2)==SQLITE_OK);
  check("peer_creates_other_before_continue",
        execSql(db2, "SELECT dolt_branch('other');")==SQLITE_OK);

  gRebaseStaleDb = db1;
  doltliteTestSetRebaseBeforeAdvanceHook(rebaseAdvanceForgetPeerBranch);
  res = queryScalarText(db1, "SELECT dolt_rebase('--continue')");
  gRebaseStaleDb = 0;
  doltliteTestSetRebaseBeforeAdvanceHook(0);
  check("rebase_continue_with_peer_branch_succeeds",
        strstr(res, "Successfully rebased")!=0);

  sqlite3_close(db2);
  sqlite3_close(db1);

  check("open_db3_after_rebase_peer_branch", open_db(dbpath, &db3)==SQLITE_OK);
  check("rebase_continue_keeps_peer_branch",
        strcmp(queryScalarText(db3,
          "SELECT count(*) FROM dolt_branches WHERE name='other'"), "1")==0);
  check("checkout_feat_after_rebase_peer_branch",
        strcmp(queryScalarText(db3, "SELECT dolt_checkout('feat')"), "0")==0);
  check("rebase_continue_keeps_feat_row",
        strcmp(queryScalarText(db3, "SELECT v FROM t WHERE id=2"), "feat")==0);

  sqlite3_close(db3);
  removeDbFiles(dbpath);
}

static void run_branch_copy_existing_dest_preserves_durable_state(void){
  sqlite3 *db = 0;
  char dbpath[256];
  const char *res;

  printf("=== Branch Copy Existing Dest Preserves Durable State Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_branch_copy_existing_dest_preserves_durable_state");
  removeDbFiles(dbpath);

  check("open_db_for_branch_copy_existing_dest", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_branch_copy_existing_dest", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "SELECT dolt_branch('feature');"
    "SELECT dolt_branch('clone');")==SQLITE_OK);

  res = queryScalarText(db, "SELECT dolt_branch('-c', 'feature', 'clone')");
  check("branch_copy_existing_dest_returns_error",
        strstr(res, "ERROR: branch already exists")!=0);
  check("branch_copy_existing_dest_keeps_active_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("branch_copy_existing_dest_keeps_feature_branch",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM dolt_branches WHERE name='feature'"), "1")==0);
  check("branch_copy_existing_dest_keeps_clone_branch",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM dolt_branches WHERE name='clone'"), "1")==0);
  check("branch_copy_existing_dest_keeps_branch_count",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_branches"), "3")==0);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_after_branch_copy_existing_dest", open_db(dbpath, &db)==SQLITE_OK);
  check("branch_copy_existing_dest_persists_active_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("branch_copy_existing_dest_persists_feature_branch",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM dolt_branches WHERE name='feature'"), "1")==0);
  check("branch_copy_existing_dest_persists_clone_branch",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM dolt_branches WHERE name='clone'"), "1")==0);
  check("branch_copy_existing_dest_persists_branch_count",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_branches"), "3")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_branch_copy_missing_source_preserves_durable_state(void){
  sqlite3 *db = 0;
  char dbpath[256];
  const char *res;

  printf("=== Branch Copy Missing Source Preserves Durable State Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_branch_copy_missing_source_preserves_durable_state");
  removeDbFiles(dbpath);

  check("open_db_for_branch_copy_missing_source", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_branch_copy_missing_source", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "SELECT dolt_branch('clone');")==SQLITE_OK);

  res = queryScalarText(db, "SELECT dolt_branch('-c', 'missing', 'clone2')");
  check("branch_copy_missing_source_returns_error",
        strstr(res, "ERROR: source branch not found")!=0);
  check("branch_copy_missing_source_keeps_active_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("branch_copy_missing_source_keeps_clone_branch",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM dolt_branches WHERE name='clone'"), "1")==0);
  check("branch_copy_missing_source_keeps_missing_absent",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM dolt_branches WHERE name='clone2'"), "0")==0);
  check("branch_copy_missing_source_keeps_branch_count",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_branches"), "2")==0);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_after_branch_copy_missing_source", open_db(dbpath, &db)==SQLITE_OK);
  check("branch_copy_missing_source_persists_active_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("branch_copy_missing_source_persists_clone_branch",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM dolt_branches WHERE name='clone'"), "1")==0);
  check("branch_copy_missing_source_persists_missing_absent",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM dolt_branches WHERE name='clone2'"), "0")==0);
  check("branch_copy_missing_source_persists_branch_count",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_branches"), "2")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_branch_rename_missing_source_preserves_durable_state(void){
  sqlite3 *db = 0;
  char dbpath[256];
  const char *res;

  printf("=== Branch Rename Missing Source Preserves Durable State Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_branch_rename_missing_source_preserves_durable_state");
  removeDbFiles(dbpath);

  check("open_db_for_branch_rename_missing_source", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_branch_rename_missing_source", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "SELECT dolt_branch('other');")==SQLITE_OK);

  res = queryScalarText(db, "SELECT dolt_branch('-m', 'missing', 'renamed')");
  check("branch_rename_missing_source_returns_error",
        strstr(res, "ERROR: source branch not found")!=0);
  check("branch_rename_missing_source_keeps_active_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("branch_rename_missing_source_keeps_other_branch",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM dolt_branches WHERE name='other'"), "1")==0);
  check("branch_rename_missing_source_keeps_renamed_absent",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM dolt_branches WHERE name='renamed'"), "0")==0);
  check("branch_rename_missing_source_keeps_branch_count",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_branches"), "2")==0);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_after_branch_rename_missing_source", open_db(dbpath, &db)==SQLITE_OK);
  check("branch_rename_missing_source_persists_active_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("branch_rename_missing_source_persists_other_branch",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM dolt_branches WHERE name='other'"), "1")==0);
  check("branch_rename_missing_source_persists_renamed_absent",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM dolt_branches WHERE name='renamed'"), "0")==0);
  check("branch_rename_missing_source_persists_branch_count",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_branches"), "2")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}
static void run_branch_create_existing_name_preserves_durable_state(void){
  sqlite3 *db = 0;
  char dbpath[256];
  const char *res;

  printf("=== Branch Create Existing Name Preserves Durable State Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_branch_create_existing_name_preserves_durable_state");
  removeDbFiles(dbpath);

  check("open_db_for_branch_create_existing_name", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_branch_create_existing_name", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "SELECT dolt_branch('feature');")==SQLITE_OK);

  res = queryScalarText(db, "SELECT dolt_branch('feature')");
  check("branch_create_existing_name_returns_error",
        strstr(res, "ERROR: branch already exists")!=0);
  check("branch_create_existing_name_keeps_active_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("branch_create_existing_name_keeps_branch_count",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_branches"), "2")==0);
  check("branch_create_existing_name_keeps_feature_branch",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM dolt_branches WHERE name='feature'"), "1")==0);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_after_branch_create_existing_name", open_db(dbpath, &db)==SQLITE_OK);
  check("branch_create_existing_name_persists_active_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("branch_create_existing_name_persists_branch_count",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_branches"), "2")==0);
  check("branch_create_existing_name_persists_feature_branch",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM dolt_branches WHERE name='feature'"), "1")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_branch_create_bad_start_preserves_durable_state(void){
  sqlite3 *db = 0;
  char dbpath[256];
  const char *res;

  printf("=== Branch Create Bad Start Preserves Durable State Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_branch_create_bad_start_preserves_durable_state");
  removeDbFiles(dbpath);

  check("open_db_for_branch_create_bad_start", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_branch_create_bad_start", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);

  res = queryScalarText(db, "SELECT dolt_branch('bad_start', 'does-not-exist')");
  check("branch_create_bad_start_returns_error",
        strstr(res, "ERROR: start point not found")!=0);
  check("branch_create_bad_start_keeps_active_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("branch_create_bad_start_keeps_branch_count",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_branches"), "1")==0);
  check("branch_create_bad_start_keeps_new_branch_absent",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM dolt_branches WHERE name='bad_start'"), "0")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_revert_bad_ref_failure_preserves_durable_state(void){
  sqlite3 *db = 0;
  char dbpath[256];
  const char *res;
  char zHeadBefore[128];

  printf("=== Revert Bad Ref Failure Preserves Durable State Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_revert_bad_ref_failure_preserves_durable_state");
  removeDbFiles(dbpath);

  check("open_db_for_revert_bad_ref_failure", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_revert_bad_ref_failure", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_add('-A');"
    "SELECT dolt_commit('-m', 'init');"
    "INSERT INTO t VALUES(2,'b');"
    "SELECT dolt_add('-A');"
    "SELECT dolt_commit('-m', 'second');")==SQLITE_OK);
  sqlite3_snprintf(sizeof(zHeadBefore), zHeadBefore, "%s",
                   queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"));

  res = queryScalarText(db, "SELECT dolt_revert('does-not-exist')");
  check("revert_bad_ref_returns_error",
        strstr(res, "ERROR:")!=0);
  check("revert_bad_ref_keeps_active_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("revert_bad_ref_keeps_working_rows",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "2")==0);
  check("revert_bad_ref_keeps_status_clean",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_status"), "0")==0);
  check("revert_bad_ref_keeps_head",
        strcmp(queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"), zHeadBefore)==0);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_after_revert_bad_ref_failure", open_db(dbpath, &db)==SQLITE_OK);
  check("revert_bad_ref_persists_active_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("revert_bad_ref_persists_working_rows",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "2")==0);
  check("revert_bad_ref_persists_status_clean",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_status"), "0")==0);
  check("revert_bad_ref_persists_head",
        strcmp(queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"), zHeadBefore)==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_checkout_nonexistent_preserves_durable_state(void){
  sqlite3 *db = 0;
  char dbpath[256];
  const char *res;

  printf("=== Checkout Nonexistent Preserves Durable State Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_checkout_nonexistent_preserves_durable_state");
  removeDbFiles(dbpath);

  check("open_db_for_checkout_nonexistent", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_checkout_nonexistent", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);

  res = queryScalarText(db, "SELECT dolt_checkout('nope')");
  check("checkout_nonexistent_returns_error", strstr(res, "ERROR:")!=0);
  check("checkout_nonexistent_keeps_active_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("checkout_nonexistent_keeps_rows",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "1")==0);
  check("checkout_nonexistent_keeps_branch_count",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_branches"), "1")==0);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_after_checkout_nonexistent", open_db(dbpath, &db)==SQLITE_OK);
  check("checkout_nonexistent_persists_active_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("checkout_nonexistent_persists_rows",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "1")==0);
  check("checkout_nonexistent_persists_branch_count",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_branches"), "1")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_checkout_dash_b_existing_branch_preserves_durable_state(void){
  sqlite3 *db = 0;
  char dbpath[256];
  const char *res;

  printf("=== Checkout -b Existing Branch Preserves Durable State Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_checkout_dash_b_existing_branch_preserves_durable_state");
  removeDbFiles(dbpath);

  check("open_db_for_checkout_dash_b_existing_branch", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_checkout_dash_b_existing_branch", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "SELECT dolt_branch('feature');")==SQLITE_OK);

  res = queryScalarText(db, "SELECT dolt_checkout('-b', 'feature')");
  check("checkout_dash_b_existing_branch_returns_error", strstr(res, "ERROR:")!=0);
  check("checkout_dash_b_existing_branch_keeps_active_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("checkout_dash_b_existing_branch_keeps_feature_branch",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM dolt_branches WHERE name='feature'"), "1")==0);
  check("checkout_dash_b_existing_branch_keeps_branch_count",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_branches"), "2")==0);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_after_checkout_dash_b_existing_branch", open_db(dbpath, &db)==SQLITE_OK);
  check("checkout_dash_b_existing_branch_persists_active_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("checkout_dash_b_existing_branch_persists_feature_branch",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM dolt_branches WHERE name='feature'"), "1")==0);
  check("checkout_dash_b_existing_branch_persists_branch_count",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_branches"), "2")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_cherry_pick_bad_ref_failure_preserves_durable_state(void){
  sqlite3 *db = 0;
  char dbpath[256];
  const char *res;
  char zHeadBefore[128];

  printf("=== Cherry-pick Bad Ref Failure Preserves Durable State Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_cherry_pick_bad_ref_failure_preserves_durable_state");
  removeDbFiles(dbpath);

  check("open_db_for_cherry_pick_bad_ref_failure", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_cherry_pick_bad_ref_failure", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_add('-A');"
    "SELECT dolt_commit('-m', 'init');"
    "INSERT INTO t VALUES(2,'b');"
    "SELECT dolt_add('-A');"
    "SELECT dolt_commit('-m', 'second');")==SQLITE_OK);
  sqlite3_snprintf(sizeof(zHeadBefore), zHeadBefore, "%s",
                   queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"));

  res = queryScalarText(db, "SELECT dolt_cherry_pick('does-not-exist')");
  check("cherry_pick_bad_ref_returns_error",
        strstr(res, "ERROR:")!=0);
  check("cherry_pick_bad_ref_keeps_active_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("cherry_pick_bad_ref_keeps_working_rows",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "2")==0);
  check("cherry_pick_bad_ref_keeps_status_clean",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_status"), "0")==0);
  check("cherry_pick_bad_ref_keeps_head",
        strcmp(queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"), zHeadBefore)==0);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_after_cherry_pick_bad_ref_failure", open_db(dbpath, &db)==SQLITE_OK);
  check("cherry_pick_bad_ref_persists_active_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("cherry_pick_bad_ref_persists_working_rows",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "2")==0);
  check("cherry_pick_bad_ref_persists_status_clean",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_status"), "0")==0);
  check("cherry_pick_bad_ref_persists_head",
        strcmp(queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"), zHeadBefore)==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_merge_nonexistent_branch_preserves_durable_state(void){
  sqlite3 *db = 0;
  char dbpath[256];
  const char *res;
  char zHeadBefore[128];

  printf("=== Merge Nonexistent Branch Preserves Durable State Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_merge_nonexistent_branch_preserves_durable_state");
  removeDbFiles(dbpath);

  check("open_db_for_merge_nonexistent_branch", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_merge_nonexistent_branch", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');")==SQLITE_OK);
  sqlite3_snprintf(sizeof(zHeadBefore), zHeadBefore, "%s",
                   queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"));

  res = queryScalarText(db, "SELECT dolt_merge('nope')");
  check("merge_nonexistent_branch_returns_error",
        strstr(res, "ERROR:")!=0);
  check("merge_nonexistent_branch_keeps_active_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("merge_nonexistent_branch_keeps_rows",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "1")==0);
  check("merge_nonexistent_branch_keeps_head",
        strcmp(queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"), zHeadBefore)==0);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_after_merge_nonexistent_branch", open_db(dbpath, &db)==SQLITE_OK);
  check("merge_nonexistent_branch_persists_active_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("merge_nonexistent_branch_persists_rows",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "1")==0);
  check("merge_nonexistent_branch_persists_head",
        strcmp(queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"), zHeadBefore)==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_rebase_continue_without_active_preserves_durable_state(void){
  sqlite3 *db = 0;
  char dbpath[256];
  const char *res;
  char zHeadBefore[128];

  printf("=== Rebase Continue Without Active Preserves Durable State Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_rebase_continue_without_active_preserves_durable_state");
  removeDbFiles(dbpath);

  check("open_db_for_rebase_continue_without_active", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_rebase_continue_without_active", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY);"
    "SELECT dolt_add('-A');"
    "SELECT dolt_commit('-m', 'init');")==SQLITE_OK);
  sqlite3_snprintf(sizeof(zHeadBefore), zHeadBefore, "%s",
                   queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"));

  res = queryScalarText(db, "SELECT dolt_rebase('--continue')");
  check("rebase_continue_without_active_returns_error",
        strstr(res, "ERROR:")!=0);
  check("rebase_continue_without_active_keeps_active_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("rebase_continue_without_active_keeps_head",
        strcmp(queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"), zHeadBefore)==0);
  check("rebase_continue_without_active_keeps_no_plan",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='dolt_rebase'"), "0")==0);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_after_rebase_continue_without_active", open_db(dbpath, &db)==SQLITE_OK);
  check("rebase_continue_without_active_persists_active_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("rebase_continue_without_active_persists_head",
        strcmp(queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"), zHeadBefore)==0);
  check("rebase_continue_without_active_persists_no_plan",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='dolt_rebase'"), "0")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_remote_add_duplicate_preserves_durable_state(void){
  sqlite3 *db = 0;
  char dbpath[256];
  const char *res;

  printf("=== Remote Add Duplicate Preserves Durable State Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_remote_add_duplicate_preserves_durable_state");
  removeDbFiles(dbpath);

  check("open_db_for_remote_add_duplicate", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_remote_add_duplicate", execSql(db,
    "SELECT dolt_remote('add', 'origin', 'file:///tmp/oracle_origin');")==SQLITE_OK);

  res = queryScalarText(db, "SELECT dolt_remote('add', 'origin', 'file:///tmp/oracle_other')");
  check("remote_add_duplicate_returns_error", strstr(res, "ERROR:")!=0);
  check("remote_add_duplicate_keeps_remote_count",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_remotes"), "1")==0);
  check("remote_add_duplicate_keeps_origin_url",
        strcmp(queryScalarText(db,
          "SELECT url FROM dolt_remotes WHERE name='origin'"), "file:///tmp/oracle_origin")==0);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_after_remote_add_duplicate", open_db(dbpath, &db)==SQLITE_OK);
  check("remote_add_duplicate_persists_remote_count",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_remotes"), "1")==0);
  check("remote_add_duplicate_persists_origin_url",
        strcmp(queryScalarText(db,
          "SELECT url FROM dolt_remotes WHERE name='origin'"), "file:///tmp/oracle_origin")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_remote_remove_missing_preserves_durable_state(void){
  sqlite3 *db = 0;
  char dbpath[256];
  const char *res;

  printf("=== Remote Remove Missing Preserves Durable State Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_remote_remove_missing_preserves_durable_state");
  removeDbFiles(dbpath);

  check("open_db_for_remote_remove_missing", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_remote_remove_missing", execSql(db,
    "SELECT dolt_remote('add', 'origin', 'file:///tmp/oracle_origin');")==SQLITE_OK);

  res = queryScalarText(db, "SELECT dolt_remote('remove', 'nonexistent')");
  check("remote_remove_missing_returns_error", strstr(res, "ERROR:")!=0);
  check("remote_remove_missing_keeps_remote_count",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_remotes"), "1")==0);
  check("remote_remove_missing_keeps_origin",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM dolt_remotes WHERE name='origin'"), "1")==0);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_after_remote_remove_missing", open_db(dbpath, &db)==SQLITE_OK);
  check("remote_remove_missing_persists_remote_count",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_remotes"), "1")==0);
  check("remote_remove_missing_persists_origin",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM dolt_remotes WHERE name='origin'"), "1")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}
static void run_reset_bad_ref_failure_preserves_durable_state(void){
  sqlite3 *db = 0;
  char dbpath[256];
  const char *res;
  char zHeadBefore[128];

  printf("=== Reset Bad Ref Failure Preserves Durable State Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_reset_bad_ref_failure_preserves_durable_state");
  removeDbFiles(dbpath);

  check("open_db_for_reset_bad_ref_failure", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_repo_for_reset_bad_ref_failure", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'init');"
    "INSERT INTO t VALUES(2,'b');")==SQLITE_OK);
  sqlite3_snprintf(sizeof(zHeadBefore), zHeadBefore, "%s",
                   queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"));

  res = queryScalarText(db, "SELECT dolt_reset('--hard', 'not_a_real_ref')");
  check("reset_bad_ref_returns_error",
        strstr(res, "ERROR: commit not found")!=0);
  check("reset_bad_ref_keeps_active_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("reset_bad_ref_keeps_working_rows",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "2")==0);
  check("reset_bad_ref_keeps_status",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_status"), "1")==0);
  check("reset_bad_ref_keeps_head",
        strcmp(queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"), zHeadBefore)==0);

  sqlite3_close(db);
  db = 0;

  check("reopen_db_after_reset_bad_ref_failure", open_db(dbpath, &db)==SQLITE_OK);
  check("reset_bad_ref_persists_active_branch",
        strcmp(queryScalarText(db, "SELECT active_branch()"), "main")==0);
  check("reset_bad_ref_persists_working_rows",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "2")==0);
  check("reset_bad_ref_persists_status",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_status"), "1")==0);
  check("reset_bad_ref_persists_head",
        strcmp(queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1"), zHeadBefore)==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
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

static void run_mutmap_delete_reinsert_reuses_entry(void){
  int mode;
  static const u8 aFirst[] = { 1, 2, 3, 4 };
  static const u8 aSecond[] = { 5, 6, 7, 8 };

  printf("=== MutMap Delete Reinsert Reuses Entry Test ===\n\n");
  for( mode = 0; mode < 2; mode++ ){
    ProllyMutMap mm;
    ProllyMutMapEntry *e = 0;
    void *pFirstVal = 0;
    int nFirstValAlloc = 0;
    int rc;

    check("mutmap_delete_reinsert_init",
          prollyMutMapInitMode(&mm, 1, (u8)mode)==SQLITE_OK);
    check("mutmap_delete_reinsert_insert",
          prollyMutMapInsert(&mm, 0, 0, 42, aFirst, sizeof(aFirst))==SQLITE_OK);
    rc = prollyMutMapFindRc(&mm, 0, 0, 42, &e);
    check("mutmap_delete_reinsert_find_first", rc==SQLITE_OK && e!=0);
    if( e ){
      pFirstVal = e->pVal;
      nFirstValAlloc = e->nValAlloc;
    }
    check("mutmap_delete_reinsert_delete",
          prollyMutMapDelete(&mm, 0, 0, 42)==SQLITE_OK);
    rc = prollyMutMapFindRc(&mm, 0, 0, 42, &e);
    check("mutmap_delete_reinsert_find_deleted", rc==SQLITE_OK && e!=0);
    check("mutmap_delete_reinsert_keeps_value_buffer",
          e!=0
       && e->op==PROLLY_EDIT_DELETE
       && e->nVal==0
       && e->pVal==pFirstVal
       && e->nValAlloc==nFirstValAlloc);
    check("mutmap_delete_reinsert_insert_again",
          prollyMutMapInsert(&mm, 0, 0, 42, aSecond, sizeof(aSecond))==SQLITE_OK);

    rc = prollyMutMapFindRc(&mm, 0, 0, 42, &e);
    check("mutmap_delete_reinsert_find", rc==SQLITE_OK && e!=0);
    check("mutmap_delete_reinsert_value",
          e!=0
       && e->op==PROLLY_EDIT_INSERT
       && e->nVal==(int)sizeof(aSecond)
       && e->pVal==pFirstVal
       && memcmp(e->pVal, aSecond, sizeof(aSecond))==0);

    prollyMutMapFree(&mm);
  }
}

static void run_mutmap_append_sorted_order(void){
  ProllyMutMap mm;
  ProllyMutMapIter it;
  ProllyMutMapEntry *pEntry;
  static const u8 aVal[] = { 1 };
  static const u8 aKey1[] = { 'a', 'a' };
  static const u8 aKey2[] = { 'b', 'b' };
  static const u8 aKey3[] = { 'c', 'c' };
  static const u8 aKey0[] = { '0', '0' };
  char zKey[8];
  int i;

  printf("=== MutMap Append Sorted Order Test ===\n\n");
  check("mutmap_append_sorted_init",
        prollyMutMapInitMode(&mm, 0, 0)==SQLITE_OK);
  check("mutmap_append_sorted_insert_1",
        prollyMutMapInsert(&mm, aKey1, sizeof(aKey1), 0,
                           aVal, sizeof(aVal))==SQLITE_OK);
  check("mutmap_append_sorted_insert_2",
        prollyMutMapInsert(&mm, aKey2, sizeof(aKey2), 0,
                           aVal, sizeof(aVal))==SQLITE_OK);
  check("mutmap_append_sorted_insert_3",
        prollyMutMapInsert(&mm, aKey3, sizeof(aKey3), 0,
                           aVal, sizeof(aVal))==SQLITE_OK);
  check("mutmap_append_sorted_flag_stays_set", mm.appendSorted);
  prollyMutMapIterFirst(&it, &mm);
  check("mutmap_append_sorted_iter_first",
        prollyMutMapIterValid(&it)
     && memcmp(prollyMutMapIterEntry(&it)->pKey, aKey1, sizeof(aKey1))==0);
  check("mutmap_append_sorted_order_clean", !mm.orderDirty);
  prollyMutMapFree(&mm);

  check("mutmap_append_unsorted_init",
        prollyMutMapInitMode(&mm, 0, 0)==SQLITE_OK);
  check("mutmap_append_unsorted_insert_1",
        prollyMutMapInsert(&mm, aKey2, sizeof(aKey2), 0,
                           aVal, sizeof(aVal))==SQLITE_OK);
  check("mutmap_append_unsorted_insert_2",
        prollyMutMapInsert(&mm, aKey0, sizeof(aKey0), 0,
                           aVal, sizeof(aVal))==SQLITE_OK);
  check("mutmap_append_unsorted_flag_clears", !mm.appendSorted);
  prollyMutMapFree(&mm);

  check("mutmap_blob_deferred_pos_init",
        prollyMutMapInitMode(&mm, 0, 0)==SQLITE_OK);
  for(i=1; i<=100; i++){
    sqlite3_snprintf(sizeof(zKey), zKey, "%04d", i);
    check("mutmap_blob_deferred_pos_insert",
          prollyMutMapInsert(&mm, (u8*)zKey, 4, 0,
                             aVal, sizeof(aVal))==SQLITE_OK);
  }
  check("mutmap_blob_deferred_pos_order",
        prollyMutMapEnsureOrder(&mm)==SQLITE_OK);
  sqlite3_snprintf(sizeof(zKey), zKey, "%04d", 0);
  check("mutmap_blob_deferred_pos_prepend",
        prollyMutMapInsert(&mm, (u8*)zKey, 4, 0,
                           aVal, sizeof(aVal))==SQLITE_OK);
  sqlite3_snprintf(sizeof(zKey), zKey, "%04d", 50);
  check("mutmap_blob_deferred_pos_find",
        prollyMutMapFindRc(&mm, (u8*)zKey, 4, 0, &pEntry)==SQLITE_OK
     && pEntry!=0);
  check("mutmap_blob_deferred_pos_rank",
        pEntry!=0 && prollyMutMapOrderIndexFromEntry(&mm, pEntry)==50);
  check("mutmap_blob_deferred_pos_entry",
        prollyMutMapEntryAt(&mm, 50, &pEntry)==SQLITE_OK
     && memcmp(pEntry->pKey, zKey, 4)==0);
  prollyMutMapFree(&mm);
}

typedef struct MutMapModelEntry MutMapModelEntry;
struct MutMapModelEntry {
  i64 key;
  u8 op;
  int val;
};

typedef struct MutMapModel MutMapModel;
struct MutMapModel {
  MutMapModelEntry *a;
  int n;
  int nAlloc;
};

typedef struct MutMapModelStack MutMapModelStack;
struct MutMapModelStack {
  MutMapModel *a;
  int n;
  int nAlloc;
};

static void mutmapModelClear(MutMapModel *p){
  sqlite3_free(p->a);
  memset(p, 0, sizeof(*p));
}

static int mutmapModelEnsureCapacity(MutMapModel *p, int nMin){
  if( p->nAlloc < nMin ){
    int nNew = p->nAlloc ? p->nAlloc * 2 : 8;
    MutMapModelEntry *aNew;
    while( nNew < nMin ) nNew *= 2;
    aNew = sqlite3_realloc(p->a, nNew * (int)sizeof(MutMapModelEntry));
    if( !aNew ) return SQLITE_NOMEM;
    p->a = aNew;
    p->nAlloc = nNew;
  }
  return SQLITE_OK;
}

static int mutmapModelFindIndex(const MutMapModel *p, i64 key){
  int lo = 0;
  int hi = p->n;
  while( lo < hi ){
    int mid = lo + (hi - lo) / 2;
    if( p->a[mid].key < key ){
      lo = mid + 1;
    }else if( p->a[mid].key > key ){
      hi = mid;
    }else{
      return mid;
    }
  }
  return ~lo;
}

static int mutmapModelSet(MutMapModel *p, i64 key, u8 op, int val){
  int idx = mutmapModelFindIndex(p, key);
  if( idx >= 0 ){
    p->a[idx].op = op;
    p->a[idx].val = val;
    return SQLITE_OK;
  }
  idx = ~idx;
  if( mutmapModelEnsureCapacity(p, p->n + 1)!=SQLITE_OK ) return SQLITE_NOMEM;
  if( idx < p->n ){
    memmove(&p->a[idx + 1], &p->a[idx], (p->n - idx) * (int)sizeof(MutMapModelEntry));
  }
  p->a[idx].key = key;
  p->a[idx].op = op;
  p->a[idx].val = val;
  p->n++;
  return SQLITE_OK;
}

static int mutmapModelClone(MutMapModel *pDst, const MutMapModel *pSrc){
  memset(pDst, 0, sizeof(*pDst));
  if( pSrc->n==0 ) return SQLITE_OK;
  pDst->a = sqlite3_malloc(pSrc->n * (int)sizeof(MutMapModelEntry));
  if( !pDst->a ) return SQLITE_NOMEM;
  memcpy(pDst->a, pSrc->a, pSrc->n * sizeof(MutMapModelEntry));
  pDst->n = pSrc->n;
  pDst->nAlloc = pSrc->n;
  return SQLITE_OK;
}

static int mutmapModelPush(MutMapModelStack *pStack, const MutMapModel *pCur){
  MutMapModel snap;
  memset(&snap, 0, sizeof(snap));
  if( mutmapModelClone(&snap, pCur)!=SQLITE_OK ) return SQLITE_NOMEM;
  if( pStack->n >= pStack->nAlloc ){
    int nNew = pStack->nAlloc ? pStack->nAlloc * 2 : 4;
    MutMapModel *aNew = sqlite3_realloc(pStack->a, nNew * (int)sizeof(MutMapModel));
    if( !aNew ){
      mutmapModelClear(&snap);
      return SQLITE_NOMEM;
    }
    pStack->a = aNew;
    pStack->nAlloc = nNew;
  }
  pStack->a[pStack->n++] = snap;
  return SQLITE_OK;
}

static void mutmapModelRestore(MutMapModel *pDst, const MutMapModel *pSrc){
  mutmapModelClear(pDst);
  (void)mutmapModelClone(pDst, pSrc);
}

static void mutmapModelPopRelease(MutMapModelStack *pStack){
  if( pStack->n<=0 ) return;
  mutmapModelClear(&pStack->a[pStack->n - 1]);
  pStack->n--;
}

static void mutmapModelPopRollback(MutMapModelStack *pStack, MutMapModel *pCur){
  if( pStack->n<=0 ) return;
  mutmapModelRestore(pCur, &pStack->a[pStack->n - 1]);
  mutmapModelClear(&pStack->a[pStack->n - 1]);
  pStack->n--;
}

static void mutmapModelStackClear(MutMapModelStack *pStack){
  int i;
  for(i=0; i<pStack->n; i++){
    mutmapModelClear(&pStack->a[i]);
  }
  sqlite3_free(pStack->a);
  memset(pStack, 0, sizeof(*pStack));
}

static unsigned int mutmapRandNext(unsigned int *pState){
  *pState = (*pState * 1103515245u) + 12345u;
  return *pState;
}

static int mutmapAssertMatchesModel(
  const char *zLabel,
  ProllyMutMap *pMap,
  const MutMapModel *pModel
){
  int ok = 1;
  int i;
  ProllyMutMapIter it;
  (void)zLabel;
  if( prollyMutMapCount(pMap)!=pModel->n ){
    return 0;
  }
  prollyMutMapIterFirst(&it, pMap);
  for(i=0; i<pModel->n; i++){
    ProllyMutMapEntry *pEntry;
    ProllyMutMapEntry *pFind;
    int rc;
    if( !prollyMutMapIterValid(&it) ) return 0;
    pEntry = prollyMutMapIterEntry(&it);
    if( !pEntry ) return 0;
    ok = ok && prollyMutMapEntryIntKey(pEntry)==pModel->a[i].key;
    ok = ok && pEntry->op==pModel->a[i].op;
    ok = ok && ((pEntry->op==PROLLY_EDIT_DELETE)
             || (pEntry->nVal==(int)sizeof(int) && memcmp(pEntry->pVal, &pModel->a[i].val, sizeof(int))==0));
    {
      ProllyMutMapEntry *pAt = 0;
      ok = ok && prollyMutMapEntryAt(pMap, i, &pAt)==SQLITE_OK && pAt==pEntry;
    }
    ok = ok && prollyMutMapOrderIndexFromEntry(pMap, pEntry)==i;
    rc = prollyMutMapFindRc(pMap, 0, 0, pModel->a[i].key, &pFind);
    ok = ok && rc==SQLITE_OK;
    ok = ok && pFind==pEntry;
    prollyMutMapIterNext(&it);
  }
  ok = ok && !prollyMutMapIterValid(&it);
  for(i=0; i<8; i++){
    i64 miss = 1000 + i;
    ProllyMutMapEntry *pFind = 0;
    int rc = prollyMutMapFindRc(pMap, 0, 0, miss, &pFind);
    ok = ok && rc==SQLITE_OK;
    ok = ok && pFind==0;
  }
  return ok;
}

static void run_mutmap_resolve_sorted_pos(void){
  ProllyMutMap sorted, lazy;
  i64 keys[] = { 10, 30, 20, 50, 40 };
  int n = sizeof(keys)/sizeof(keys[0]);
  int val = 1;
  int i;
  u32 gen0;
  int idx, found;

  printf("=== MutMap ResolveSortedPos Test ===\n\n");

  check("rsp_init_sorted", prollyMutMapInitMode(&sorted, 1, 1)==SQLITE_OK);
  check("rsp_init_lazy",   prollyMutMapInitMode(&lazy,   1, 0)==SQLITE_OK);
  check("rsp_initial_gen_sorted_zero", sorted.generation == 0);
  check("rsp_initial_gen_lazy_zero",   lazy.generation   == 0);

  for(i=0; i<n; i++){
    gen0 = sorted.generation;
    check("rsp_insert_sorted_rc",
          prollyMutMapInsert(&sorted, 0, 0, keys[i], (const u8*)&val, sizeof(val))==SQLITE_OK);
    check("rsp_insert_bumps_gen_sorted", sorted.generation > gen0);

    gen0 = lazy.generation;
    check("rsp_insert_lazy_rc",
          prollyMutMapInsert(&lazy,   0, 0, keys[i], (const u8*)&val, sizeof(val))==SQLITE_OK);
    check("rsp_insert_bumps_gen_lazy",   lazy.generation   > gen0);
  }

  gen0 = sorted.generation;
  val = 999;
  check("rsp_inplace_update_sorted_rc",
        prollyMutMapInsert(&sorted, 0, 0, 30, (const u8*)&val, sizeof(val))==SQLITE_OK);
  check("rsp_inplace_does_not_bump_sorted", sorted.generation == gen0);

  gen0 = lazy.generation;
  check("rsp_inplace_update_lazy_rc",
        prollyMutMapInsert(&lazy,   0, 0, 30, (const u8*)&val, sizeof(val))==SQLITE_OK);
  check("rsp_inplace_does_not_bump_lazy",   lazy.generation == gen0);

  check("rsp_resolve_present_10_sorted",
        prollyMutMapResolveSortedPos(&sorted, 0, 0, 10, &idx, &found)==SQLITE_OK
          && idx==0 && found);
  check("rsp_resolve_present_30_sorted",
        prollyMutMapResolveSortedPos(&sorted, 0, 0, 30, &idx, &found)==SQLITE_OK
          && idx==2 && found);
  check("rsp_resolve_present_50_sorted",
        prollyMutMapResolveSortedPos(&sorted, 0, 0, 50, &idx, &found)==SQLITE_OK
          && idx==4 && found);
  check("rsp_resolve_absent_25_sorted",
        prollyMutMapResolveSortedPos(&sorted, 0, 0, 25, &idx, &found)==SQLITE_OK
          && idx==2 && !found);
  check("rsp_resolve_absent_5_sorted",
        prollyMutMapResolveSortedPos(&sorted, 0, 0,  5, &idx, &found)==SQLITE_OK
          && idx==0 && !found);
  check("rsp_resolve_absent_999_sorted",
        prollyMutMapResolveSortedPos(&sorted, 0, 0, 999, &idx, &found)==SQLITE_OK
          && idx==5 && !found);

  check("rsp_resolve_present_30_lazy",
        prollyMutMapResolveSortedPos(&lazy,   0, 0, 30, &idx, &found)==SQLITE_OK
          && idx==2 && found);
  check("rsp_resolve_absent_25_lazy",
        prollyMutMapResolveSortedPos(&lazy,   0, 0, 25, &idx, &found)==SQLITE_OK
          && idx==2 && !found);
  check("rsp_resolve_absent_999_lazy",
        prollyMutMapResolveSortedPos(&lazy,   0, 0, 999, &idx, &found)==SQLITE_OK
          && idx==5 && !found);

  {
    ProllyMutMap empty;
    check("rsp_init_empty", prollyMutMapInitMode(&empty, 1, 1)==SQLITE_OK);
    check("rsp_resolve_empty",
          prollyMutMapResolveSortedPos(&empty, 0, 0, 42, &idx, &found)==SQLITE_OK
            && idx==0 && !found);
    prollyMutMapFree(&empty);
  }

  gen0 = sorted.generation;
  check("rsp_delete_absent_sorted_rc",
        prollyMutMapDelete(&sorted, 0, 0, 999)==SQLITE_OK);
  check("rsp_delete_absent_bumps_gen_sorted", sorted.generation > gen0);

  gen0 = sorted.generation;
  check("rsp_delete_existing_sorted_rc",
        prollyMutMapDelete(&sorted, 0, 0, 30)==SQLITE_OK);
  check("rsp_delete_existing_does_not_bump_sorted", sorted.generation == gen0);

  prollyMutMapFree(&sorted);
  prollyMutMapFree(&lazy);
}

static void run_mutmap_differential_randomized(void){
  ProllyMutMap sorted;
  ProllyMutMap lazy;
  MutMapModel model;
  MutMapModelStack stack;
  unsigned int rng = 0x5eed1234u;
  int level = 0;
  int rc;
  int i;

  printf("=== MutMap Differential Randomized Test ===\n\n");
  memset(&model, 0, sizeof(model));
  memset(&stack, 0, sizeof(stack));

  check("mutmap_diff_init_sorted",
        prollyMutMapInitMode(&sorted, 1, 1)==SQLITE_OK);
  check("mutmap_diff_init_lazy",
        prollyMutMapInitMode(&lazy, 1, 0)==SQLITE_OK);

  for(i=0; i<1500; i++){
    unsigned int r = mutmapRandNext(&rng);
    int op = (int)(r % 10);
    i64 key = (i64)(mutmapRandNext(&rng) % 64);
    int val = (int)(mutmapRandNext(&rng) % 100000);
    int useAbsent = mutmapModelFindIndex(&model, key)<0 && (r & 0x100)!=0;

    if( op < 4 ){
      rc = mutmapModelSet(&model, key, PROLLY_EDIT_INSERT, val);
      check("mutmap_diff_model_insert_rc", rc==SQLITE_OK);
      check("mutmap_diff_sorted_insert_rc",
            (useAbsent
              ? prollyMutMapInsertAbsent(&sorted, 0, 0, key,
                                         (const u8*)&val, sizeof(val))
              : prollyMutMapInsert(&sorted, 0, 0, key,
                                   (const u8*)&val, sizeof(val)))==SQLITE_OK);
      check("mutmap_diff_lazy_insert_rc",
            (useAbsent
              ? prollyMutMapInsertAbsent(&lazy, 0, 0, key,
                                         (const u8*)&val, sizeof(val))
              : prollyMutMapInsert(&lazy, 0, 0, key,
                                   (const u8*)&val, sizeof(val)))==SQLITE_OK);
    }else if( op < 7 ){
      rc = mutmapModelSet(&model, key, PROLLY_EDIT_DELETE, 0);
      check("mutmap_diff_model_delete_rc", rc==SQLITE_OK);
      check("mutmap_diff_sorted_delete_rc",
            (useAbsent
              ? prollyMutMapDeleteAbsent(&sorted, 0, 0, key)
              : prollyMutMapDelete(&sorted, 0, 0, key))==SQLITE_OK);
      check("mutmap_diff_lazy_delete_rc",
            (useAbsent
              ? prollyMutMapDeleteAbsent(&lazy, 0, 0, key)
              : prollyMutMapDelete(&lazy, 0, 0, key))==SQLITE_OK);
    }else if( op == 7 ){
      level++;
      check("mutmap_diff_push_snapshot",
            mutmapModelPush(&stack, &model)==SQLITE_OK);
      prollyMutMapPushSavepoint(&sorted, level);
      prollyMutMapPushSavepoint(&lazy, level);
    }else if( op == 8 ){
      if( level > 0 ){
        mutmapModelPopRollback(&stack, &model);
        check("mutmap_diff_sorted_rollback_rc",
              prollyMutMapRollbackToSavepoint(&sorted, level)==SQLITE_OK);
        check("mutmap_diff_lazy_rollback_rc",
              prollyMutMapRollbackToSavepoint(&lazy, level)==SQLITE_OK);
        level--;
      }
    }else{
      if( level > 0 ){
        mutmapModelPopRelease(&stack);
        prollyMutMapReleaseSavepoint(&sorted, level);
        prollyMutMapReleaseSavepoint(&lazy, level);
        level--;
      }
    }

    check("mutmap_diff_sorted_matches_model",
          mutmapAssertMatchesModel("sorted", &sorted, &model));
    check("mutmap_diff_lazy_matches_model",
          mutmapAssertMatchesModel("lazy", &lazy, &model));

    if( (i % 97)==0 ){
      ProllyMutMap *pClone = 0;
      check("mutmap_diff_clone_sorted_rc", prollyMutMapClone(&pClone, &sorted)==SQLITE_OK);
      if( pClone ){
        check("mutmap_diff_clone_sorted_matches_model",
              mutmapAssertMatchesModel("sorted_clone", pClone, &model));
        prollyMutMapFree(pClone);
        sqlite3_free(pClone);
      }
      pClone = 0;
      check("mutmap_diff_clone_lazy_rc", prollyMutMapClone(&pClone, &lazy)==SQLITE_OK);
      if( pClone ){
        check("mutmap_diff_clone_lazy_matches_model",
              mutmapAssertMatchesModel("lazy_clone", pClone, &model));
        prollyMutMapFree(pClone);
        sqlite3_free(pClone);
      }
    }
  }

  while( level > 0 ){
    mutmapModelPopRollback(&stack, &model);
    check("mutmap_diff_sorted_final_rollback_rc",
          prollyMutMapRollbackToSavepoint(&sorted, level)==SQLITE_OK);
    check("mutmap_diff_lazy_final_rollback_rc",
          prollyMutMapRollbackToSavepoint(&lazy, level)==SQLITE_OK);
    level--;
  }
  check("mutmap_diff_sorted_final_matches_model",
        mutmapAssertMatchesModel("sorted_final", &sorted, &model));
  check("mutmap_diff_lazy_final_matches_model",
        mutmapAssertMatchesModel("lazy_final", &lazy, &model));

  mutmapModelStackClear(&stack);
  mutmapModelClear(&model);
  prollyMutMapFree(&sorted);
  prollyMutMapFree(&lazy);
}

static void run_prolly_mutate_preserves_order_across_skipped_subtrees(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyChunker chunker;
  ProllyCursor cur;
  ProllyHash rootHash, newRootHash;
  ProllyNode rootNode;
  u8 *pRootData = 0;
  int nRootData = 0;
  u8 aKey[33];
  const u8 *pKey = 0;
  int nKey = 0;
  int rc;
  int res = 99;
  int i;
  int expected = 0;
  int nSeen = 0;
  int iterRcOk = 1;
  const int nItem = 100000;
  const int iDelete = 1;
  u8 aVal[256];

  printf("=== Prolly Mutate Skipped Subtree Order Test ===\n\n");
  memset(aVal, 'v', sizeof(aVal));

  check("open_memory_store_for_prolly_mutate_skip",
        chunkStoreOpen(&cs, sqlite3_vfs_find(0), ":memory:", 0)==SQLITE_OK);
  check("init_cache_for_prolly_mutate_skip", prollyCacheInit(&cache, 64)==SQLITE_OK);
  check("init_chunker_for_prolly_mutate_skip",
        prollyChunkerInit(&chunker, &cs, PROLLY_NODE_BLOBKEY)==SQLITE_OK);

  for( i = 0; i < nItem; i++ ){
    make_prolly_blob_key(i, aKey, sizeof(aKey));
    check("add_key_to_chunker_for_prolly_mutate_skip",
          prollyChunkerAdd(&chunker, aKey, 32, aVal, sizeof(aVal))==SQLITE_OK);
  }
  check("finish_chunker_for_prolly_mutate_skip", prollyChunkerFinish(&chunker)==SQLITE_OK);
  prollyChunkerGetRoot(&chunker, &rootHash);
  prollyChunkerFree(&chunker);

  check("load_root_for_prolly_mutate_skip",
        chunkStoreGet(&cs, &rootHash, &pRootData, &nRootData)==SQLITE_OK);
  check("parse_root_for_prolly_mutate_skip",
        prollyNodeParse(&rootNode, pRootData, nRootData)==SQLITE_OK);
  check("root_is_multi_level_for_prolly_mutate_skip", rootNode.level >= 2);
  sqlite3_free(pRootData);
  pRootData = 0;

  make_prolly_blob_key(iDelete, aKey, sizeof(aKey));
  rc = prollyMutateDelete(&cs, &cache, &rootHash, PROLLY_NODE_BLOBKEY,
                          aKey, 32, 0, &newRootHash);
  check("delete_key_from_multi_level_tree", rc==SQLITE_OK);

  prollyCursorInit(&cur, &cs, &cache, &newRootHash, PROLLY_NODE_BLOBKEY);
  rc = prollyCursorFirst(&cur, &res);
  check("cursor_first_after_prolly_mutate_skip", rc==SQLITE_OK);
  check("cursor_first_after_prolly_mutate_skip_valid",
        res==0 && prollyCursorIsValid(&cur));

  while( prollyCursorIsValid(&cur) ){
    while( expected==iDelete ){
      expected++;
    }
    make_prolly_blob_key(expected, aKey, sizeof(aKey));
    prollyCursorKey(&cur, &pKey, &nKey);
    if( nKey!=32 || memcmp(pKey, aKey, 32)!=0 ){
      iterRcOk = 0;
      break;
    }
    nSeen++;
    expected++;
    rc = prollyCursorNext(&cur);
    if( rc!=SQLITE_OK ){
      iterRcOk = 0;
      break;
    }
  }
  check("iterate_after_prolly_mutate_skip_in_order", iterRcOk);
  check("iterate_after_prolly_mutate_skip_count", nSeen==nItem-1);
  check("iterate_after_prolly_mutate_skip_expected_end", expected==nItem);

  prollyCursorClose(&cur);
  prollyCacheFree(&cache);
  chunkStoreClose(&cs);
}

static void run_prolly_mutate_appends_blob_key_to_right_edge(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyChunker chunker;
  ProllyCursor cur;
  ProllyHash rootHash, newRootHash;
  ProllyNode rootNode;
  u8 *pRootData = 0;
  int nRootData = 0;
  u8 aKey[33];
  const u8 *pKey = 0;
  int nKey = 0;
  int rc;
  int res = 99;
  int i;
  const int nItem = 100000;
  u8 aVal[256];

  printf("=== Prolly Mutate Blob Right Edge Append Test ===\n\n");
  memset(aVal, 'v', sizeof(aVal));

  check("open_memory_store_for_prolly_blob_append",
        chunkStoreOpen(&cs, sqlite3_vfs_find(0), ":memory:", 0)==SQLITE_OK);
  check("init_cache_for_prolly_blob_append",
        prollyCacheInit(&cache, 64)==SQLITE_OK);
  check("init_chunker_for_prolly_blob_append",
        prollyChunkerInit(&chunker, &cs, PROLLY_NODE_BLOBKEY)==SQLITE_OK);

  for( i = 0; i < nItem; i++ ){
    make_prolly_blob_key(i, aKey, sizeof(aKey));
    check("add_key_to_chunker_for_prolly_blob_append",
          prollyChunkerAdd(&chunker, aKey, 32, aVal, sizeof(aVal))==SQLITE_OK);
  }
  check("finish_chunker_for_prolly_blob_append",
        prollyChunkerFinish(&chunker)==SQLITE_OK);
  prollyChunkerGetRoot(&chunker, &rootHash);
  prollyChunkerFree(&chunker);

  check("load_root_for_prolly_blob_append",
        chunkStoreGet(&cs, &rootHash, &pRootData, &nRootData)==SQLITE_OK);
  check("parse_root_for_prolly_blob_append",
        prollyNodeParse(&rootNode, pRootData, nRootData)==SQLITE_OK);
  check("root_is_multi_level_for_prolly_blob_append", rootNode.level >= 2);
  sqlite3_free(pRootData);

  make_prolly_blob_key(nItem, aKey, sizeof(aKey));
  rc = prollyMutateInsert(&cs, &cache, &rootHash, PROLLY_NODE_BLOBKEY,
                          aKey, 32, 0, aVal, sizeof(aVal), &newRootHash);
  check("append_blob_key_to_multi_level_tree", rc==SQLITE_OK);

  prollyCursorInit(&cur, &cs, &cache, &newRootHash, PROLLY_NODE_BLOBKEY);
  rc = prollyCursorLast(&cur, &res);
  check("cursor_last_after_blob_append", rc==SQLITE_OK);
  check("cursor_last_after_blob_append_valid",
        res==0 && prollyCursorIsValid(&cur));
  if( prollyCursorIsValid(&cur) ){
    prollyCursorKey(&cur, &pKey, &nKey);
    check("last_key_after_blob_append_is_new_key",
          nKey==32 && memcmp(pKey, aKey, 32)==0);
  }

  prollyCursorClose(&cur);
  prollyCacheFree(&cache);
  chunkStoreClose(&cs);
}

static void run_prolly_mutate_batches_existing_int_replacements(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyChunker chunker;
  ProllyMutMap mm;
  ProllyMutator mut;
  ProllyCursor cur;
  ProllyHash rootHash, newRootHash;
  ProllyNode rootNode;
  u8 *pRootData = 0;
  int nRootData = 0;
  u8 aKey[8];
  u8 aVal[32];
  const u8 *pVal = 0;
  int nVal = 0;
  int rc;
  int res = 99;
  int i;
  int nSeen = 0;
  const int nItem = 100000;

  printf("=== Prolly Mutate Batch Int Replacement Test ===\n\n");

  check("open_memory_store_for_prolly_batch_replace",
        chunkStoreOpen(&cs, sqlite3_vfs_find(0), ":memory:", 0)==SQLITE_OK);
  check("init_cache_for_prolly_batch_replace",
        prollyCacheInit(&cache, 64)==SQLITE_OK);
  check("init_chunker_for_prolly_batch_replace",
        prollyChunkerInit(&chunker, &cs, PROLLY_NODE_INTKEY)==SQLITE_OK);

  for( i = 1; i <= nItem; i++ ){
    prollyEncodeIntKey(i, aKey);
    sqlite3_snprintf(sizeof(aVal), (char*)aVal, "row-%d", i);
    check("add_key_to_chunker_for_prolly_batch_replace",
          prollyChunkerAdd(&chunker, aKey, sizeof(aKey),
                           aVal, (int)strlen((char*)aVal))==SQLITE_OK);
  }
  check("finish_chunker_for_prolly_batch_replace",
        prollyChunkerFinish(&chunker)==SQLITE_OK);
  prollyChunkerGetRoot(&chunker, &rootHash);
  prollyChunkerFree(&chunker);

  check("load_root_for_prolly_batch_replace",
        chunkStoreGet(&cs, &rootHash, &pRootData, &nRootData)==SQLITE_OK);
  check("parse_root_for_prolly_batch_replace",
        prollyNodeParse(&rootNode, pRootData, nRootData)==SQLITE_OK);
  check("root_is_multi_level_for_prolly_batch_replace", rootNode.level >= 2);
  sqlite3_free(pRootData);

  check("init_mutmap_for_prolly_batch_replace",
        prollyMutMapInitMode(&mm, 1, 0)==SQLITE_OK);
  for( i = 1000; i < 2000; i++ ){
    sqlite3_snprintf(sizeof(aVal), (char*)aVal, "new-%d", i);
    check("insert_replacement_for_prolly_batch_replace",
          prollyMutMapInsert(&mm, 0, 0, i,
                             aVal, (int)strlen((char*)aVal))==SQLITE_OK);
  }

  memset(&mut, 0, sizeof(mut));
  mut.pStore = &cs;
  mut.pCache = &cache;
  mut.oldRoot = rootHash;
  mut.pEdits = &mm;
  mut.flags = PROLLY_NODE_INTKEY;
  rc = prollyMutateFlush(&mut);
  check("flush_batch_int_replacements", rc==SQLITE_OK);
  newRootHash = mut.newRoot;
  prollyMutMapFree(&mm);

  prollyCursorInit(&cur, &cs, &cache, &newRootHash, PROLLY_NODE_INTKEY);
  rc = prollyCursorSeekInt(&cur, 1500, &res);
  check("seek_replaced_batch_int_key", rc==SQLITE_OK);
  check("seek_replaced_batch_int_key_found",
        res==0 && prollyCursorIsValid(&cur));
  if( prollyCursorIsValid(&cur) ){
    prollyCursorValue(&cur, &pVal, &nVal);
    check("replaced_batch_int_value",
          nVal==8 && memcmp(pVal, "new-1500", 8)==0);
  }
  prollyCursorClose(&cur);

  prollyCursorInit(&cur, &cs, &cache, &newRootHash, PROLLY_NODE_INTKEY);
  rc = prollyCursorFirst(&cur, &res);
  check("cursor_first_after_batch_int_replace", rc==SQLITE_OK);
  while( prollyCursorIsValid(&cur) ){
    nSeen++;
    rc = prollyCursorNext(&cur);
    if( rc!=SQLITE_OK ) break;
  }
  check("batch_int_replace_count_unchanged", nSeen==nItem);
  check("iterate_after_batch_int_replace_ok", rc==SQLITE_OK);

  prollyCursorClose(&cur);
  prollyCacheFree(&cache);
  chunkStoreClose(&cs);
}

static void run_chunk_store_rollback_restores_refs_hash(void){
  ChunkStore cs;
  ProllyHash emptyHash;
  ProllyHash refsHashBefore;
  ProllyHash foundHash;

  printf("=== Chunk Store Rollback Restores Refs Hash Test ===\n\n");

  memset(&emptyHash, 0, sizeof(emptyHash));
  check("open_memory_store_for_refs_rollback",
        chunkStoreOpen(&cs, sqlite3_vfs_find(0), ":memory:", 0)==SQLITE_OK);
  check("set_default_branch_for_refs_rollback",
        chunkStoreSetDefaultBranch(&cs, "main")==SQLITE_OK);
  check("add_main_branch_for_refs_rollback",
        chunkStoreAddBranch(&cs, "main", &emptyHash)==SQLITE_OK);
  check("serialize_initial_refs_for_refs_rollback",
        chunkStoreSerializeRefs(&cs)==SQLITE_OK);
  check("commit_initial_refs_for_refs_rollback",
        chunkStoreCommit(&cs)==SQLITE_OK);
  refsHashBefore = cs.refs.refsHash;

  check("add_tag_before_rollback",
        chunkStoreAddTag(&cs, "v1", &emptyHash)==SQLITE_OK);
  check("serialize_updated_refs_before_rollback",
        chunkStoreSerializeRefs(&cs)==SQLITE_OK);
  check("refs_hash_changed_before_rollback",
        memcmp(&cs.refs.refsHash, &refsHashBefore, sizeof(ProllyHash))!=0);

  chunkStoreRollback(&cs);
  check("refs_hash_restored_on_rollback",
        memcmp(&cs.refs.refsHash, &refsHashBefore, sizeof(ProllyHash))==0);
  check("reload_refs_after_rollback", chunkStoreReloadRefs(&cs)==SQLITE_OK);
  check("tag_absent_after_reload_rollback",
        chunkStoreFindTag(&cs, "v1", &foundHash)!=SQLITE_OK);

  chunkStoreClose(&cs);
}

static void run_chunk_store_commit_failure_restores_refs_hash(void){
  ChunkStore cs;
  ChunkStore reopened;
  ProllyHash emptyHash;
  ProllyHash refsHashBefore;
  ProllyHash foundHash;
  char dbpath[256];
  int rc;

  printf("=== Chunk Store Commit Failure Restores Refs Hash Test ===\n\n");

  memset(&emptyHash, 0, sizeof(emptyHash));
  make_dbpath(dbpath, sizeof(dbpath), "test_refs_commit_failure_restore");
  removeDbFiles(dbpath);

  gFailSyncOnce = 0;
  gFailHits = 0;
  check("register_fail_vfs_for_refs_commit_failure", registerFailVfs()==SQLITE_OK);
  check("open_fail_store_for_refs_commit_failure",
        chunkStoreOpen(&cs, &gFailVfs, dbpath,
          SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  check("set_default_branch_for_refs_commit_failure",
        chunkStoreSetDefaultBranch(&cs, "main")==SQLITE_OK);
  check("add_main_branch_for_refs_commit_failure",
        chunkStoreAddBranch(&cs, "main", &emptyHash)==SQLITE_OK);
  check("serialize_initial_refs_for_refs_commit_failure",
        chunkStoreSerializeRefs(&cs)==SQLITE_OK);
  check("commit_initial_refs_for_refs_commit_failure",
        chunkStoreCommit(&cs)==SQLITE_OK);
  refsHashBefore = cs.refs.refsHash;

  check("add_tag_for_refs_commit_failure",
        chunkStoreAddTag(&cs, "v1", &emptyHash)==SQLITE_OK);
  check("serialize_updated_refs_for_refs_commit_failure",
        chunkStoreSerializeRefs(&cs)==SQLITE_OK);
  check("refs_hash_changed_for_refs_commit_failure",
        memcmp(&cs.refs.refsHash, &refsHashBefore, sizeof(ProllyHash))!=0);

  gFailHits = 0;
  gFailSyncOnce = 1;
  rc = chunkStoreCommit(&cs);
  check("commit_failure_injected_for_refs_commit_failure", gFailHits>0);
  check("commit_failure_surfaces_for_refs_commit_failure", rc!=SQLITE_OK);
  check("refs_hash_restored_on_commit_failure",
        memcmp(&cs.refs.refsHash, &refsHashBefore, sizeof(ProllyHash))==0);
  check("in_memory_tag_absent_after_commit_failure",
        chunkStoreFindTag(&cs, "v1", &foundHash)!=SQLITE_OK);

  chunkStoreRollback(&cs);
  check("reload_refs_after_commit_failure_rollback",
        chunkStoreReloadRefs(&cs)==SQLITE_OK);
  check("tag_absent_after_commit_failure_rollback",
        chunkStoreFindTag(&cs, "v1", &foundHash)!=SQLITE_OK);

  chunkStoreClose(&cs);
  check("reopen_store_after_refs_commit_failure",
        chunkStoreOpen(&reopened, sqlite3_vfs_find(0), dbpath,
          SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  check("reopened_store_has_no_failed_tag",
        chunkStoreFindTag(&reopened, "v1", &foundHash)!=SQLITE_OK);
  check("reopened_store_keeps_default_branch",
        reopened.refs.zDefaultBranch!=0 && strcmp(reopened.refs.zDefaultBranch, "main")==0);
  chunkStoreClose(&reopened);
  removeDbFiles(dbpath);
}

static void run_chunk_store_uses_vfs_full_pathname(void){
  ChunkStore cs;
  char dbpath[256];
  char zExpected[512];
  const char *zStored;
  int rc;

  printf("=== Chunk Store Uses VFS Full Pathname Test ===\n\n");

  memset(&cs, 0, sizeof(cs));
  make_dbpath(dbpath, sizeof(dbpath), "test_chunk_store_full_pathname");
  removeDbFiles(dbpath);
  check("register_fail_vfs_for_full_pathname", registerFailVfs()==SQLITE_OK);

  gFullPathnameSuffix = ".canonical-test";
  gFailFullPathnameHits = 0;
  gRewrittenFullPath[0] = 0;
  rc = chunkStoreOpen(&cs, &gFailVfs, dbpath,
          SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB);
  gFullPathnameSuffix = 0;
  check("open_store_uses_full_pathname", rc==SQLITE_OK);

  if( rc==SQLITE_OK ){
    zStored = chunkStoreFilename(&cs);
    check("full_pathname_was_called", gFailFullPathnameHits>0);
    check("full_pathname_was_rewritten", gRewrittenFullPath[0]!=0);
    sqlite3_snprintf(sizeof(zExpected), zExpected, "%s", gRewrittenFullPath);
    normalizeNonVerbatimWinPath(zExpected);
    check("chunk_store_keeps_full_pathname",
          zStored!=0 && strcmp(zStored, zExpected)==0);
    chunkStoreClose(&cs);
  }

  removeDbFiles(dbpath);
  if( gRewrittenFullPath[0] ) removeDbFiles(gRewrittenFullPath);
  gRewrittenFullPath[0] = 0;
}

static void run_remotesrv_put_refs_failure_restores_state(void){
  ChunkStore cs;
  ChunkStore reopened;
  ProllyHash emptyHash;
  ProllyHash refsHashBefore;
  ProllyHash foundHash;
  char dbpath[256];
  int rc;
  static const u8 aBadRefs[] = { 'n','o','t','-','r','e','f','s' };

  printf("=== RemoteSrv Put Refs Failure Restores State Test ===\n\n");

  memset(&emptyHash, 0, sizeof(emptyHash));
  make_dbpath(dbpath, sizeof(dbpath), "test_remotesrv_put_refs_failure_restore");
  removeDbFiles(dbpath);

  check("register_fail_vfs_for_remotesrv_put_refs", registerFailVfs()==SQLITE_OK);
  check("open_fail_store_for_remotesrv_put_refs",
        chunkStoreOpen(&cs, &gFailVfs, dbpath,
          SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  check("set_default_branch_for_remotesrv_put_refs",
        chunkStoreSetDefaultBranch(&cs, "main")==SQLITE_OK);
  check("add_main_branch_for_remotesrv_put_refs",
        chunkStoreAddBranch(&cs, "main", &emptyHash)==SQLITE_OK);
  check("serialize_initial_refs_for_remotesrv_put_refs",
        chunkStoreSerializeRefs(&cs)==SQLITE_OK);
  check("commit_initial_refs_for_remotesrv_put_refs",
        chunkStoreCommit(&cs)==SQLITE_OK);
  refsHashBefore = cs.refs.refsHash;

  rc = doltliteRemoteSrvApplyRefsForTest(&cs, aBadRefs, (int)sizeof(aBadRefs));
  check("remotesrv_put_refs_reload_failure_surfaces", rc!=SQLITE_OK);
  check("remotesrv_put_refs_restores_refs_hash",
        memcmp(&cs.refs.refsHash, &refsHashBefore, sizeof(ProllyHash))==0);
  check("remotesrv_put_refs_keeps_default_branch",
        cs.refs.zDefaultBranch!=0 && strcmp(cs.refs.zDefaultBranch, "main")==0);
  check("remotesrv_put_refs_keeps_main_branch",
        chunkStoreFindBranch(&cs, "main", &foundHash)==SQLITE_OK);
  check("remotesrv_put_refs_does_not_leave_failed_tag",
        chunkStoreFindTag(&cs, "v1", &foundHash)!=SQLITE_OK);

  chunkStoreClose(&cs);
  check("reopen_store_after_remotesrv_put_refs_failure",
        chunkStoreOpen(&reopened, sqlite3_vfs_find(0), dbpath,
          SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  check("reopened_store_after_remotesrv_put_refs_keeps_main",
        chunkStoreFindBranch(&reopened, "main", &foundHash)==SQLITE_OK);
  check("reopened_store_after_remotesrv_put_refs_has_no_failed_tag",
        chunkStoreFindTag(&reopened, "v1", &foundHash)!=SQLITE_OK);
  chunkStoreClose(&reopened);
  removeDbFiles(dbpath);
}

static u8 *singleBranchRefsBlob(const char *zBranch, int *pnBlob){
  ChunkStore refs;
  ProllyHash emptyHash;
  u8 *pBlob = 0;

  memset(&refs, 0, sizeof(refs));
  memset(&emptyHash, 0, sizeof(emptyHash));
  *pnBlob = 0;
  if( chunkStoreSetDefaultBranch(&refs, zBranch)!=SQLITE_OK ) goto done;
  if( chunkStoreAddBranch(&refs, zBranch, &emptyHash)!=SQLITE_OK ) goto done;
  if( chunkStoreSerializeRefsToBlob(&refs, &pBlob, pnBlob)!=SQLITE_OK ){
    pBlob = 0;
  }
done:
  chunkStoreClose(&refs);
  return pBlob;
}

static void run_remotesrv_plain_refs_refreshes_under_lock(void){
  ChunkStore cs1;
  ChunkStore cs2;
  ChunkStore reopened;
  ProllyHash foundHash;
  u8 *pRefsA;
  u8 *pRefsB;
  char dbpath[256];
  int nRefsA;
  int nRefsB;
  int rc;

  printf("=== RemoteSrv Plain Refs Refreshes Under Lock Test ===\n\n");
  memset(&cs1, 0, sizeof(cs1));
  memset(&cs2, 0, sizeof(cs2));
  memset(&reopened, 0, sizeof(reopened));
  make_dbpath(dbpath, sizeof(dbpath), "test_remotesrv_plain_refs_lock");
  removeDbFiles(dbpath);
  pRefsA = singleBranchRefsBlob("branch_a", &nRefsA);
  pRefsB = singleBranchRefsBlob("branch_b", &nRefsB);
  check("plain_refs_serialize_branch_a", pRefsA!=0);
  check("plain_refs_serialize_branch_b", pRefsB!=0);

  check("plain_refs_open_store_1",
        chunkStoreOpen(&cs1, sqlite3_vfs_find(0), dbpath,
          SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
          | SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  check("plain_refs_open_stale_store_2",
        chunkStoreOpen(&cs2, sqlite3_vfs_find(0), dbpath,
          SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
          | SQLITE_OPEN_MAIN_DB)==SQLITE_OK);

  rc = doltliteRemoteSrvApplyScopedRefsForTest(
      &cs1, "branch_a", 0, pRefsA, nRefsA);
  check("plain_refs_first_install_succeeds", rc==SQLITE_OK);
  rc = doltliteRemoteSrvApplyScopedRefsForTest(
      &cs2, "branch_b", 0, pRefsB, nRefsB);
  check("plain_refs_stale_install_rejected", rc==SQLITE_CONSTRAINT);
  check("plain_refs_stale_store_refreshes_branch_a",
        chunkStoreFindBranch(&cs2, "branch_a", &foundHash)==SQLITE_OK);
  check("plain_refs_stale_store_does_not_install_branch_b",
        chunkStoreFindBranch(&cs2, "branch_b", &foundHash)!=SQLITE_OK);
  rc = chunkStoreLockAndRefresh(&cs1);
  check("plain_refs_failure_releases_graph_lock", rc==SQLITE_OK);
  if( rc==SQLITE_OK ) chunkStoreUnlock(&cs1);

  chunkStoreClose(&cs2);
  chunkStoreClose(&cs1);
  check("plain_refs_reopen_store",
        chunkStoreOpen(&reopened, sqlite3_vfs_find(0), dbpath,
          SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  check("plain_refs_reopen_keeps_branch_a",
        chunkStoreFindBranch(&reopened, "branch_a", &foundHash)==SQLITE_OK);
  check("plain_refs_reopen_has_no_branch_b",
        chunkStoreFindBranch(&reopened, "branch_b", &foundHash)!=SQLITE_OK);
  chunkStoreClose(&reopened);
  sqlite3_free(pRefsA);
  sqlite3_free(pRefsB);
  removeDbFiles(dbpath);
}

static void run_remotesrv_stale_size_heuristic_keeps_peer_branch(void){
  ChunkStore cs1;
  ChunkStore cs2;
  ChunkStore reopened;
  ProllyHash foundHash;
  u8 *pRefsA;
  u8 *pRefsB;
  char dbpath[256];
  int nRefsA;
  int nRefsB;
  int rc;

  printf("=== RemoteSrv Stale Size Heuristic Keeps Peer Branch ===\n\n");
  memset(&cs1, 0, sizeof(cs1));
  memset(&cs2, 0, sizeof(cs2));
  memset(&reopened, 0, sizeof(reopened));
  make_dbpath(dbpath, sizeof(dbpath), "test_remotesrv_stale_size_heuristic");
  removeDbFiles(dbpath);
  pRefsA = singleBranchRefsBlob("branch_a", &nRefsA);
  pRefsB = singleBranchRefsBlob("branch_b", &nRefsB);
  check("stale_size_serialize_branch_a", pRefsA!=0);
  check("stale_size_serialize_branch_b", pRefsB!=0);

  check("stale_size_open_store_1",
        chunkStoreOpen(&cs1, sqlite3_vfs_find(0), dbpath,
          SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
          | SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  check("stale_size_open_store_2",
        chunkStoreOpen(&cs2, sqlite3_vfs_find(0), dbpath,
          SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
          | SQLITE_OPEN_MAIN_DB)==SQLITE_OK);

  rc = doltliteRemoteSrvApplyScopedRefsForTest(
      &cs1, "branch_a", 0, pRefsA, nRefsA);
  check("stale_size_first_install_succeeds", rc==SQLITE_OK);
  chunkFileSetSize(&cs2.file, ((i64)1) << 40);
  cs2.snapshotPinned = 1;

  rc = doltliteRemoteSrvApplyScopedRefsForTest(
      &cs2, "branch_b", 0, pRefsB, nRefsB);
  check("stale_size_second_install_rejected", rc==SQLITE_CONSTRAINT);
  check("stale_size_store_keeps_branch_a",
        chunkStoreFindBranch(&cs2, "branch_a", &foundHash)==SQLITE_OK);
  check("stale_size_store_does_not_install_branch_b",
        chunkStoreFindBranch(&cs2, "branch_b", &foundHash)!=SQLITE_OK);

  chunkStoreClose(&cs2);
  chunkStoreClose(&cs1);
  check("stale_size_reopen_store",
        chunkStoreOpen(&reopened, sqlite3_vfs_find(0), dbpath,
          SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  check("stale_size_reopen_keeps_branch_a",
        chunkStoreFindBranch(&reopened, "branch_a", &foundHash)==SQLITE_OK);
  check("stale_size_reopen_has_no_branch_b",
        chunkStoreFindBranch(&reopened, "branch_b", &foundHash)!=SQLITE_OK);
  chunkStoreClose(&reopened);
  sqlite3_free(pRefsA);
  sqlite3_free(pRefsB);
  removeDbFiles(dbpath);
}

static void run_file_remote_stale_size_heuristic_keeps_peer_branch(void){
  DoltliteRemote *pA = 0;
  DoltliteRemote *pB = 0;
  ChunkStore *pStoreB;
  ChunkStore reopened;
  ProllyHash foundHash;
  ProllyHash expected;
  u8 *pRefsA;
  u8 *pRefsB;
  char dbpath[256];
  int nRefsA;
  int nRefsB;
  int rc;

  printf("=== File Remote Stale Size Heuristic Keeps Peer Branch ===\n\n");
  memset(&reopened, 0, sizeof(reopened));
  memset(&expected, 0, sizeof(expected));
  make_dbpath(dbpath, sizeof(dbpath), "test_file_remote_stale_size_heuristic");
  removeDbFiles(dbpath);
  pRefsA = singleBranchRefsBlob("branch_a", &nRefsA);
  pRefsB = singleBranchRefsBlob("branch_b", &nRefsB);
  check("file_remote_serialize_branch_a", pRefsA!=0);
  check("file_remote_serialize_branch_b", pRefsB!=0);

  pA = doltliteFsRemoteOpen(sqlite3_vfs_find(0), dbpath);
  pB = doltliteFsRemoteOpen(sqlite3_vfs_find(0), dbpath);
  check("file_remote_open_a", pA!=0);
  check("file_remote_open_b", pB!=0);
  pStoreB = doltliteFsRemoteStoreForTest(pB);
  check("file_remote_store_b", pStoreB!=0);

  rc = pA->xSetRefsIf(pA, &expected, "branch_a", 0, pRefsA, nRefsA);
  if( rc==SQLITE_OK ) rc = pA->xCommit(pA);
  check("file_remote_first_install_succeeds", rc==SQLITE_OK);
  chunkFileSetSize(&pStoreB->file, ((i64)1) << 40);
  pStoreB->snapshotPinned = 1;

  rc = pB->xSetRefsIf(pB, &expected, "branch_b", 0, pRefsB, nRefsB);
  check("file_remote_second_install_rejected", rc==SQLITE_CONSTRAINT || rc==SQLITE_BUSY);
  if( rc==SQLITE_OK ){
    (void)pB->xCommit(pB);
  }
  check("file_remote_store_keeps_branch_a",
        pStoreB && chunkStoreFindBranch(pStoreB, "branch_a",
                                        &foundHash)==SQLITE_OK);
  check("file_remote_store_does_not_install_branch_b",
        pStoreB && chunkStoreFindBranch(pStoreB, "branch_b",
                                        &foundHash)!=SQLITE_OK);

  if( pB ) pB->xClose(pB);
  if( pA ) pA->xClose(pA);
  check("file_remote_reopen_store",
        chunkStoreOpen(&reopened, sqlite3_vfs_find(0), dbpath,
          SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  check("file_remote_reopen_keeps_branch_a",
        chunkStoreFindBranch(&reopened, "branch_a", &foundHash)==SQLITE_OK);
  check("file_remote_reopen_has_no_branch_b",
        chunkStoreFindBranch(&reopened, "branch_b", &foundHash)!=SQLITE_OK);
  chunkStoreClose(&reopened);
  sqlite3_free(pRefsA);
  sqlite3_free(pRefsB);
  removeDbFiles(dbpath);
}

static void run_remotesrv_chunk_commit_failure_clears_pending(void){
  ChunkStore cs;
  ChunkStore reopened;
  ProllyHash emptyHash;
  ProllyHash foundHash;
  ProllyHash chunkHash;
  char dbpath[256];
  int rc;
  static const u8 aChunk[] = { 'o','r','p','h','a','n' };

  printf("=== RemoteSrv Chunk Commit Failure Clears Pending Test ===\n\n");

  memset(&emptyHash, 0, sizeof(emptyHash));
  memset(&chunkHash, 0, sizeof(chunkHash));
  make_dbpath(dbpath, sizeof(dbpath), "test_remotesrv_chunk_commit_failure");
  removeDbFiles(dbpath);

  gFailHits = 0;
  gFailSyncOnce = 0;
  check("register_fail_vfs_for_remotesrv_chunk_commit", registerFailVfs()==SQLITE_OK);
  check("open_fail_store_for_remotesrv_chunk_commit",
        chunkStoreOpen(&cs, &gFailVfs, dbpath,
          SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  check("set_default_branch_for_remotesrv_chunk_commit",
        chunkStoreSetDefaultBranch(&cs, "main")==SQLITE_OK);
  check("add_main_branch_for_remotesrv_chunk_commit",
        chunkStoreAddBranch(&cs, "main", &emptyHash)==SQLITE_OK);
  check("serialize_initial_refs_for_remotesrv_chunk_commit",
        chunkStoreSerializeRefs(&cs)==SQLITE_OK);
  check("commit_initial_refs_for_remotesrv_chunk_commit",
        chunkStoreCommit(&cs)==SQLITE_OK);
  check("queue_pending_chunk_for_remotesrv_chunk_commit",
        chunkStorePut(&cs, aChunk, (int)sizeof(aChunk), &chunkHash)==SQLITE_OK);
  {
    int hasChunk = 0;
    check("pending_chunk_visible_before_failed_commit_rc",
          chunkStoreHas(&cs, &chunkHash, &hasChunk)==SQLITE_OK);
    check("pending_chunk_visible_before_failed_commit", hasChunk);
  }

  gFailHits = 0;
  gFailSyncOnce = 1;
  rc = doltliteRemoteSrvCommitPendingForTest(&cs);
  check("remotesrv_chunk_commit_failure_injected", gFailHits>0);
  check("remotesrv_chunk_commit_failure_surfaces", rc!=SQLITE_OK);
  {
    int hasChunk = 1;
    check("remotesrv_chunk_commit_rolls_back_pending_visibility_rc",
          chunkStoreHas(&cs, &chunkHash, &hasChunk)==SQLITE_OK);
    check("remotesrv_chunk_commit_rolls_back_pending_visibility",
          !hasChunk);
  }
  check("remotesrv_chunk_commit_clears_pending_count", cs.staging.nPending==0);

  gFailSyncOnce = 0;
  check("serialize_followup_refs_for_remotesrv_chunk_commit",
        chunkStoreAddTag(&cs, "v1", &emptyHash)==SQLITE_OK);
  check("serialize_followup_refs_for_remotesrv_chunk_commit_2",
        chunkStoreSerializeRefs(&cs)==SQLITE_OK);
  check("commit_followup_refs_for_remotesrv_chunk_commit",
        chunkStoreCommit(&cs)==SQLITE_OK);
  {
    int hasChunk = 1;
    check("failed_chunk_not_visible_after_followup_commit_rc",
          chunkStoreHas(&cs, &chunkHash, &hasChunk)==SQLITE_OK);
    check("failed_chunk_not_visible_after_followup_commit",
          !hasChunk);
  }

  chunkStoreClose(&cs);
  check("reopen_store_after_remotesrv_chunk_commit_failure",
        chunkStoreOpen(&reopened, sqlite3_vfs_find(0), dbpath,
          SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB)==SQLITE_OK);
  check("reopened_store_after_remotesrv_chunk_commit_has_tag",
        chunkStoreFindTag(&reopened, "v1", &foundHash)==SQLITE_OK);
  {
    int hasChunk = 1;
    check("reopened_store_after_remotesrv_chunk_commit_has_no_failed_chunk_rc",
          chunkStoreHas(&reopened, &chunkHash, &hasChunk)==SQLITE_OK);
    check("reopened_store_after_remotesrv_chunk_commit_has_no_failed_chunk",
          !hasChunk);
  }
  chunkStoreClose(&reopened);
  removeDbFiles(dbpath);
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

static int addIntKeyItem(ProllyNodeBuilder *b, i64 iKey, const u8 *pVal, int nVal){
  u8 aKey[8];
  prollyEncodeIntKey(iKey, aKey);
  return prollyNodeBuilderAdd(b, aKey, sizeof(aKey), pVal, nVal);
}

static void run_prolly_int_cursor_seek_across_internal_boundary(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyCursor cur;
  ProllyNodeBuilder b;
  ProllyHash leftHash, rightHash, rootHash;
  u8 *pNode = 0;
  int nNode = 0;
  int rc;
  int res = 99;

  static const u8 v1[] = { '1' };
  static const u8 v2[] = { '2' };

  printf("=== Prolly Int Cursor Internal Boundary Test ===\n\n");

  check("open_memory_store_for_int_cursor",
        chunkStoreOpen(&cs, sqlite3_vfs_find(0), ":memory:", 0)==SQLITE_OK);
  check("init_cache_for_int_cursor", prollyCacheInit(&cache, 8)==SQLITE_OK);

  prollyNodeBuilderInit(&b, 0, PROLLY_NODE_INTKEY);
  check("build_left_leaf_10", addIntKeyItem(&b, 10, v1, sizeof(v1))==SQLITE_OK);
  check("build_left_leaf_20", addIntKeyItem(&b, 20, v1, sizeof(v1))==SQLITE_OK);
  check("finish_int_left_leaf", prollyNodeBuilderFinish(&b, &pNode, &nNode)==SQLITE_OK);
  check("store_int_left_leaf", chunkStorePut(&cs, pNode, nNode, &leftHash)==SQLITE_OK);
  sqlite3_free(pNode);
  pNode = 0;
  prollyNodeBuilderFree(&b);

  prollyNodeBuilderInit(&b, 0, PROLLY_NODE_INTKEY);
  check("build_right_leaf_30", addIntKeyItem(&b, 30, v2, sizeof(v2))==SQLITE_OK);
  check("build_right_leaf_40", addIntKeyItem(&b, 40, v2, sizeof(v2))==SQLITE_OK);
  check("finish_int_right_leaf", prollyNodeBuilderFinish(&b, &pNode, &nNode)==SQLITE_OK);
  check("store_int_right_leaf", chunkStorePut(&cs, pNode, nNode, &rightHash)==SQLITE_OK);
  sqlite3_free(pNode);
  pNode = 0;
  prollyNodeBuilderFree(&b);

  prollyNodeBuilderInit(&b, 1, PROLLY_NODE_INTKEY);
  check("build_int_root_left_sep",
        addIntKeyItem(&b, 20, leftHash.data, PROLLY_HASH_SIZE)==SQLITE_OK);
  check("build_int_root_right_sep",
        addIntKeyItem(&b, 40, rightHash.data, PROLLY_HASH_SIZE)==SQLITE_OK);
  check("finish_int_root", prollyNodeBuilderFinish(&b, &pNode, &nNode)==SQLITE_OK);
  check("store_int_root", chunkStorePut(&cs, pNode, nNode, &rootHash)==SQLITE_OK);
  sqlite3_free(pNode);
  prollyNodeBuilderFree(&b);

  prollyCursorInit(&cur, &cs, &cache, &rootHash, PROLLY_NODE_INTKEY);
  rc = prollyCursorSeekInt(&cur, 30, &res);
  check("seek_int_key_across_internal_boundary", rc==SQLITE_OK);
  check("seek_int_key_finds_exact_match", res==0);
  check("int_cursor_valid_after_seek", prollyCursorIsValid(&cur));
  if( prollyCursorIsValid(&cur) ){
    check("int_cursor_lands_on_right_child_key", prollyCursorIntKey(&cur)==30);
  }

  prollyCursorClose(&cur);
  prollyCacheFree(&cache);
  chunkStoreClose(&cs);
}

static void run_prolly_int_cursor_seek_past_max(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyCursor cur;
  ProllyNodeBuilder b;
  ProllyHash leftHash, rightHash, rootHash;
  u8 *pNode = 0;
  int nNode = 0;
  int rc;
  int res = 99;

  static const u8 v1[] = { '1' };
  static const u8 v2[] = { '2' };

  printf("=== Prolly Int Cursor Seek Past Max Test ===\n\n");

  check("open_memory_store_for_int_cursor_past_max",
        chunkStoreOpen(&cs, sqlite3_vfs_find(0), ":memory:", 0)==SQLITE_OK);
  check("init_cache_for_int_cursor_past_max", prollyCacheInit(&cache, 8)==SQLITE_OK);

  prollyNodeBuilderInit(&b, 0, PROLLY_NODE_INTKEY);
  check("build_past_max_left_leaf_10", addIntKeyItem(&b, 10, v1, sizeof(v1))==SQLITE_OK);
  check("build_past_max_left_leaf_20", addIntKeyItem(&b, 20, v1, sizeof(v1))==SQLITE_OK);
  check("finish_past_max_int_left_leaf", prollyNodeBuilderFinish(&b, &pNode, &nNode)==SQLITE_OK);
  check("store_past_max_int_left_leaf", chunkStorePut(&cs, pNode, nNode, &leftHash)==SQLITE_OK);
  sqlite3_free(pNode);
  pNode = 0;
  prollyNodeBuilderFree(&b);

  prollyNodeBuilderInit(&b, 0, PROLLY_NODE_INTKEY);
  check("build_past_max_right_leaf_30", addIntKeyItem(&b, 30, v2, sizeof(v2))==SQLITE_OK);
  check("build_past_max_right_leaf_40", addIntKeyItem(&b, 40, v2, sizeof(v2))==SQLITE_OK);
  check("finish_past_max_int_right_leaf", prollyNodeBuilderFinish(&b, &pNode, &nNode)==SQLITE_OK);
  check("store_past_max_int_right_leaf", chunkStorePut(&cs, pNode, nNode, &rightHash)==SQLITE_OK);
  sqlite3_free(pNode);
  pNode = 0;
  prollyNodeBuilderFree(&b);

  prollyNodeBuilderInit(&b, 1, PROLLY_NODE_INTKEY);
  check("build_past_max_int_root_left_sep",
        addIntKeyItem(&b, 20, leftHash.data, PROLLY_HASH_SIZE)==SQLITE_OK);
  check("build_past_max_int_root_right_sep",
        addIntKeyItem(&b, 40, rightHash.data, PROLLY_HASH_SIZE)==SQLITE_OK);
  check("finish_past_max_int_root", prollyNodeBuilderFinish(&b, &pNode, &nNode)==SQLITE_OK);
  check("store_past_max_int_root", chunkStorePut(&cs, pNode, nNode, &rootHash)==SQLITE_OK);
  sqlite3_free(pNode);
  prollyNodeBuilderFree(&b);

  prollyCursorInit(&cur, &cs, &cache, &rootHash, PROLLY_NODE_INTKEY);
  rc = prollyCursorSeekInt(&cur, 99, &res);
  check("seek_int_key_past_max", rc==SQLITE_OK);
  check("seek_int_key_past_max_result", res==-1);
  check("int_cursor_valid_after_past_max_seek", prollyCursorIsValid(&cur));
  if( prollyCursorIsValid(&cur) ){
    check("int_cursor_lands_on_max_key_after_past_max_seek",
          prollyCursorIntKey(&cur)==40);
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

static void putU32le(u8 *p, u32 v){
  p[0] = (u8)(v & 0xff);
  p[1] = (u8)((v >> 8) & 0xff);
  p[2] = (u8)((v >> 16) & 0xff);
  p[3] = (u8)((v >> 24) & 0xff);
}

/* An internal node may not point at an empty leaf. Unchecked,
** descendToExtremeLeaf sets idx to nItems-1 == -1 when walking right, and
** prollyCursorNext/Prev then mark the cursor valid over a node whose key array
** is a null pointer -- the next key read segfaults. prollyCursorFirst/Last
** already refuse it, so only the step paths were exposed. */
static void run_prolly_cursor_empty_leaf_under_internal(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyCursor cur;
  ProllyHash leafFull, leafEmpty, rootHash;
  u8 internalNode[8 + 24 + 2 + PROLLY_HASH_SIZE*2];
  int rc, res = 99;
  int n = 0;

  static const u8 fullLeaf[] = {
    'D','O','N','P',
    0,
    1, 0,
    PROLLY_NODE_BLOBKEY,
    0,0,0,0,
    1,0,0,0,
    0,0,0,0,
    1,0,0,0,
    'a',
    'A'
  };
  static const u8 emptyLeaf[] = {
    'D','O','N','P', 0, 0, 0, PROLLY_NODE_BLOBKEY
  };

  printf("=== Prolly Cursor Empty Leaf Under Internal Test ===\n\n");

  check("empty_leaf_internal_open_store",
        chunkStoreOpen(&cs, sqlite3_vfs_find(0), ":memory:", 0)==SQLITE_OK);
  check("empty_leaf_internal_init_cache", prollyCacheInit(&cache, 8)==SQLITE_OK);
  check("empty_leaf_internal_put_full",
        chunkStorePut(&cs, fullLeaf, (int)sizeof(fullLeaf), &leafFull)==SQLITE_OK);
  check("empty_leaf_internal_put_empty",
        chunkStorePut(&cs, emptyLeaf, (int)sizeof(emptyLeaf), &leafEmpty)==SQLITE_OK);

  memcpy(internalNode, "DONP", 4);            n = 4;
  internalNode[n++] = 1;                      /* level 1 */
  internalNode[n++] = 2;                      /* count lo */
  internalNode[n++] = 0;                      /* count hi */
  internalNode[n++] = PROLLY_NODE_BLOBKEY;
  putU32le(internalNode + n, 0);  n += 4;     /* aKeyOff */
  putU32le(internalNode + n, 1);  n += 4;
  putU32le(internalNode + n, 2);  n += 4;
  putU32le(internalNode + n, 0);  n += 4;     /* aValOff, hash-sized stride */
  putU32le(internalNode + n, PROLLY_HASH_SIZE);      n += 4;
  putU32le(internalNode + n, PROLLY_HASH_SIZE*2);    n += 4;
  internalNode[n++] = 'a';
  internalNode[n++] = 'b';
  memcpy(internalNode + n, leafFull.data, PROLLY_HASH_SIZE);  n += PROLLY_HASH_SIZE;
  memcpy(internalNode + n, leafEmpty.data, PROLLY_HASH_SIZE); n += PROLLY_HASH_SIZE;
  check("empty_leaf_internal_node_size", n==(int)sizeof(internalNode));
  check("empty_leaf_internal_put_root",
        chunkStorePut(&cs, internalNode, n, &rootHash)==SQLITE_OK);

  prollyCursorInit(&cur, &cs, &cache, &rootHash, PROLLY_NODE_BLOBKEY);
  rc = prollyCursorFirst(&cur, &res);
  check("empty_leaf_internal_first_ok", rc==SQLITE_OK);
  check("empty_leaf_internal_first_valid", res==0 && prollyCursorIsValid(&cur));
  rc = prollyCursorNext(&cur);
  check("empty_leaf_internal_next_reports_corrupt", rc==SQLITE_CORRUPT);
  /* The error alone is not enough: the accessors gate on eState, so a cursor
  ** left VALID here would happily read the empty child. */
  check("empty_leaf_internal_next_leaves_cursor_unusable",
        !prollyCursorIsValid(&cur));
  prollyCursorClose(&cur);

  prollyCursorInit(&cur, &cs, &cache, &rootHash, PROLLY_NODE_BLOBKEY);
  rc = prollyCursorLast(&cur, &res);
  check("empty_leaf_internal_last_reports_corrupt", rc==SQLITE_CORRUPT);
  check("empty_leaf_internal_last_leaves_cursor_unusable",
        !prollyCursorIsValid(&cur));
  prollyCursorClose(&cur);

  /* Walking forward from the populated leaf must not expose a row from the
  ** empty child, however the caller drives it. */
  prollyCursorInit(&cur, &cs, &cache, &rootHash, PROLLY_NODE_BLOBKEY);
  rc = prollyCursorFirst(&cur, &res);
  check("empty_leaf_internal_first_still_valid",
        rc==SQLITE_OK && res==0 && prollyCursorIsValid(&cur));
  if( prollyCursorIsValid(&cur) ){
    const u8 *pKey = 0;
    int nKey = 0;
    prollyCursorKey(&cur, &pKey, &nKey);
    check("empty_leaf_internal_first_key_is_the_real_row",
          nKey==1 && pKey && pKey[0]=='a');
  }
  check("empty_leaf_internal_step_corrupts", prollyCursorNext(&cur)==SQLITE_CORRUPT);
  check("empty_leaf_internal_no_row_after_step", !prollyCursorIsValid(&cur));
  prollyCursorClose(&cur);

  prollyCacheFree(&cache);
  chunkStoreClose(&cs);
}

/* Offsets must start at 0. Readers reach key data both through the offset
** array and by striding from pKeyData (prollyNodeIntKey uses i*8), so a
** nonzero first offset makes them read different bytes of the same node while
** every other structural check still passes. */
static void run_prolly_node_first_offset_validation(void){
  ProllyNode node;

  static const u8 keyOffStartsLate[] = {
    'D','O','N','P',
    0,
    1, 0,
    PROLLY_NODE_INTKEY,
    8,0,0,0,          /* aKeyOff[0] -- should be 0 */
    16,0,0,0,         /* aKeyOff[1], stride 8 so the width check passes */
    0,0,0,0,
    1,0,0,0,
    1,2,3,4,5,6,7,8,
    9,10,11,12,13,14,15,16,
    0x2a
  };
  static const u8 valOffStartsLate[] = {
    'D','O','N','P',
    0,
    1, 0,
    PROLLY_NODE_INTKEY,
    0,0,0,0,
    8,0,0,0,
    1,0,0,0,          /* aValOff[0] -- should be 0 */
    2,0,0,0,
    1,2,3,4,5,6,7,8,
    0x2a, 0x2b
  };
  static const u8 bothStartAtZero[] = {
    'D','O','N','P',
    0,
    1, 0,
    PROLLY_NODE_INTKEY,
    0,0,0,0,
    8,0,0,0,
    0,0,0,0,
    1,0,0,0,
    1,2,3,4,5,6,7,8,
    0x2a
  };

  printf("=== Prolly Node First Offset Validation Test ===\n\n");

  check("key_offsets_must_start_at_zero",
        prollyNodeParse(&node, keyOffStartsLate,
                        (int)sizeof(keyOffStartsLate))==SQLITE_CORRUPT);
  check("val_offsets_must_start_at_zero",
        prollyNodeParse(&node, valOffStartsLate,
                        (int)sizeof(valOffStartsLate))==SQLITE_CORRUPT);
  /* The same shape with well-formed offsets still parses, so the new check is
  ** not just rejecting everything. */
  check("zero_start_offsets_still_parse",
        prollyNodeParse(&node, bothStartAtZero,
                        (int)sizeof(bothStartAtZero))==SQLITE_OK);
  check("zero_start_offsets_expose_one_item", node.nItems==1);
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
                          PROLLY_NODE_BLOBKEY, PROLLY_NODE_BLOBKEY);
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

/* Incremental blob writes against a stock-format database. The wrapper
** dispatches xPutData per cursor kind, and the legacy arm was a stub that
** returned SQLITE_OK without touching the row, so sqlite3_blob_write reported
** success and threw the bytes away. */
static void run_incrblob_legacy_engine_write(void){
  char dbpath[512];
  char zUri[640];
  sqlite3 *db = 0;
  sqlite3_blob *pBlob = 0;
  char buf[16];
  int rc;

  printf("=== Incrblob Legacy Engine Write Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_incrblob_legacy");
  removeDbFiles(dbpath);

  snprintf(zUri, sizeof(zUri), "file:%s?doltlite_engine=sqlite", dbpath);
  rc = sqlite3_open_v2(zUri, &db,
                       SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE|SQLITE_OPEN_URI,
                       0);
  check("incrblob_legacy_open", rc==SQLITE_OK);
  if( !db ) return;
  check("incrblob_legacy_engine_is_orig",
        strcmp(queryScalarText(db, "SELECT doltlite_engine()"), "orig")==0);
  check("incrblob_legacy_setup", execSql(db,
      "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
      "INSERT INTO t VALUES(1, '0123456789');")==SQLITE_OK);

  check("incrblob_legacy_blob_open",
        sqlite3_blob_open(db, "main", "t", "v", 1, 1, &pBlob)==SQLITE_OK);
  if( pBlob ){
    check("incrblob_legacy_write_ok",
          sqlite3_blob_write(pBlob, "WRITTEN!!!", 10, 0)==SQLITE_OK);
    check("incrblob_legacy_blob_close", sqlite3_blob_close(pBlob)==SQLITE_OK);
  }

  check("incrblob_legacy_write_landed",
        strcmp(queryScalarText(db, "SELECT v FROM t WHERE id=1"),
               "WRITTEN!!!")==0);

  /* And it is durable, not just visible to the writing connection. */
  sqlite3_close(db);
  db = 0;
  rc = sqlite3_open_v2(zUri, &db, SQLITE_OPEN_READWRITE|SQLITE_OPEN_URI, 0);
  check("incrblob_legacy_reopen", rc==SQLITE_OK);
  if( db ){
    check("incrblob_legacy_write_persisted",
          strcmp(queryScalarText(db, "SELECT v FROM t WHERE id=1"),
                 "WRITTEN!!!")==0);
  }

  /* A partial overwrite must leave the rest of the value alone. */
  pBlob = 0;
  if( db && sqlite3_blob_open(db, "main", "t", "v", 1, 1, &pBlob)==SQLITE_OK ){
    check("incrblob_legacy_partial_write",
          sqlite3_blob_write(pBlob, "ZZ", 2, 4)==SQLITE_OK);
    sqlite3_blob_close(pBlob);
    check("incrblob_legacy_partial_result",
          strcmp(queryScalarText(db, "SELECT v FROM t WHERE id=1"),
                 "WRITZZN!!!")==0);
  }

  memset(buf, 0, sizeof(buf));
  pBlob = 0;
  if( db && sqlite3_blob_open(db, "main", "t", "v", 1, 0, &pBlob)==SQLITE_OK ){
    check("incrblob_legacy_readback",
          sqlite3_blob_read(pBlob, buf, 10, 0)==SQLITE_OK
          && memcmp(buf, "WRITZZN!!!", 10)==0);
    sqlite3_blob_close(pBlob);
  }

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_incrblob_chunked_and_multihandle(void){
  char dbpath[512];
  sqlite3 *db = 0;
  sqlite3_blob *h1 = 0, *h2 = 0;
  char buf[8];
  int bigBlobSize = 10 * 1024 * 1024;
  int memoryCeiling = 5 * 1024 * 1024;
  int rc;

  printf("=== Incrblob Chunked Record And Multi-Handle Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_incrblob_chunked");
  removeDbFiles(dbpath);

  check("incrblob_open_db", open_db(dbpath, &db)==SQLITE_OK);
  if( !db ) return;
  check("incrblob_setup", execSql(db,
      "CREATE TABLE t(k INTEGER PRIMARY KEY, v BLOB);"
      "INSERT INTO t VALUES(1, zeroblob(5000));"
      "INSERT INTO t VALUES(2, zeroblob(20));")==SQLITE_OK);

  check("incrblob_open_h1", sqlite3_blob_open(db, "main", "t", "v", 1, 1, &h1)==SQLITE_OK);
  check("incrblob_open_h2", sqlite3_blob_open(db, "main", "t", "v", 1, 1, &h2)==SQLITE_OK);
  check("incrblob_first_write", sqlite3_blob_write(h2, "SQLite", 6, 4094)==SQLITE_OK);
  check("incrblob_second_write", sqlite3_blob_write(h2, "again!", 6, 10)==SQLITE_OK);
  memset(buf, 0, sizeof(buf));
  check("incrblob_other_handle_read", sqlite3_blob_read(h1, buf, 6, 4094)==SQLITE_OK);
  check("incrblob_other_handle_sees_write", memcmp(buf, "SQLite", 6)==0);
  memset(buf, 0, sizeof(buf));
  check("incrblob_writer_read_back", sqlite3_blob_read(h2, buf, 6, 4094)==SQLITE_OK
      && memcmp(buf, "SQLite", 6)==0);
  sqlite3_blob_close(h1); h1 = 0;
  sqlite3_blob_close(h2); h2 = 0;
  check("incrblob_size_intact",
      strcmp(queryScalarText(db, "SELECT length(v) FROM t WHERE k=1"), "5000")==0);
  check("incrblob_data_landed",
      strcmp(queryScalarText(db,
        "SELECT CAST(substr(v,4095,6) AS TEXT) FROM t WHERE k=1"), "SQLite")==0);
  check("incrblob_first_write_survives_second",
      strcmp(queryScalarText(db,
        "SELECT CAST(substr(v,11,6) AS TEXT) || CAST(substr(v,4095,6) AS TEXT)"
        " FROM t WHERE k=1"), "again!SQLite")==0);

  check("incrblob_reopen_h1", sqlite3_blob_open(db, "main", "t", "v", 2, 0, &h1)==SQLITE_OK);
  check("incrblob_other_row_update", execSql(db,
      "UPDATE t SET v = zeroblob(5001) WHERE k=1")==SQLITE_OK);
  check("incrblob_survives_other_row", sqlite3_blob_read(h1, buf, 2, 0)==SQLITE_OK);
  check("incrblob_open_row_update", execSql(db,
      "UPDATE t SET v = zeroblob(21) WHERE k=2")==SQLITE_OK);
  rc = sqlite3_blob_read(h1, buf, 2, 0);
  check("incrblob_aborted_after_sql_write", rc==SQLITE_ABORT);
  sqlite3_blob_close(h1); h1 = 0;

  check("incrblob_reopen_h2", sqlite3_blob_open(db, "main", "t", "v", 2, 0, &h2)==SQLITE_OK);
  check("incrblob_delete_open_row", execSql(db, "DELETE FROM t WHERE k=2")==SQLITE_OK);
  rc = sqlite3_blob_read(h2, buf, 2, 0);
  check("incrblob_aborted_after_delete", rc==SQLITE_ABORT);
  sqlite3_blob_close(h2); h2 = 0;

  check("incrblob_pending_setup", execSql(db,
      "BEGIN; INSERT INTO t VALUES(4, zeroblob(20));")==SQLITE_OK);
  check("incrblob_pending_open", sqlite3_blob_open(db, "main", "t", "v", 4, 1, &h1)==SQLITE_OK);
  check("incrblob_pending_write", h1 && sqlite3_blob_write(h1, "HELLO!", 6, 7)==SQLITE_OK);
  memset(buf, 0, sizeof(buf));
  check("incrblob_pending_read", h1 && sqlite3_blob_read(h1, buf, 6, 7)==SQLITE_OK
      && memcmp(buf, "HELLO!", 6)==0);
  if( h1 ) sqlite3_blob_close(h1);
  check("incrblob_pending_commit", execSql(db, "COMMIT;")==SQLITE_OK);
  check("incrblob_pending_durable",
      strcmp(queryScalarText(db,
        "SELECT CAST(substr(v,8,6) AS TEXT) || '/' || length(v) FROM t WHERE k=4"),
        "HELLO!/20")==0);

  check("incrblob_large_committed_setup", execSql(db,
      "INSERT INTO t VALUES(5, zeroblob(10 * 1024 * 1024));"
      )==SQLITE_OK);
  sqlite3_memory_highwater(1);
  check("incrblob_large_committed_open",
      sqlite3_blob_open(db, "main", "t", "v", 5, 0, &h1)==SQLITE_OK);
  check("incrblob_large_committed_open_memory",
      sqlite3_memory_highwater(0) < memoryCeiling);
  memset(buf, 1, 4);
  check("incrblob_large_committed_tail_read",
      h1 && sqlite3_blob_read(h1, buf, 4, bigBlobSize - 4)==SQLITE_OK);
  check("incrblob_large_committed_tail_zero",
      buf[0]==0 && buf[1]==0 && buf[2]==0 && buf[3]==0);
  check("incrblob_large_committed_read_memory",
      sqlite3_memory_highwater(0) < memoryCeiling);
  if( h1 ) sqlite3_blob_close(h1);
  h1 = 0;

  check("incrblob_large_pending_setup", execSql(db,
      "BEGIN; INSERT INTO t VALUES(6, zeroblob(10 * 1024 * 1024));"
      )==SQLITE_OK);
  sqlite3_memory_highwater(1);
  check("incrblob_large_pending_open",
      sqlite3_blob_open(db, "main", "t", "v", 6, 0, &h1)==SQLITE_OK);
  check("incrblob_large_pending_open_memory",
      sqlite3_memory_highwater(0) < memoryCeiling);
  memset(buf, 1, 4);
  check("incrblob_large_pending_tail_read",
      h1 && sqlite3_blob_read(h1, buf, 4, bigBlobSize - 4)==SQLITE_OK);
  check("incrblob_large_pending_tail_zero",
      buf[0]==0 && buf[1]==0 && buf[2]==0 && buf[3]==0);
  check("incrblob_large_pending_read_memory",
      sqlite3_memory_highwater(0) < memoryCeiling);
  if( h1 ) sqlite3_blob_close(h1);
  h1 = 0;
  check("incrblob_large_pending_rollback", execSql(db, "ROLLBACK;")==SQLITE_OK);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

/* Historical sides whose schema the scratch db cannot parse (a CHECK naming
** an application-registered function) must never render records with the
** wrong layout. When the visited schema drifted from the live one, the read
** fails; when it is identical, the declared-layout fallback is provably
** safe and the values still come back correct. */
static void sideCheckFn(sqlite3_context *ctx, int n, sqlite3_value **v){
  (void)n;
  (void)v;
  sqlite3_result_int(ctx, 1);
}

static void run_diff_side_schema_custom_function(void){
  char dbpath[512];
  sqlite3 *db = 0;
  int rc;

  printf("=== Diff Side Schema Custom Function Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_diff_side_function");
  removeDbFiles(dbpath);

  rc = sqlite3_open(dbpath, &db);
  check("side_fn_open", rc==SQLITE_OK);
  if( !db ) return;
  check("side_fn_register",
        sqlite3_create_function(db, "side_ok", 1, SQLITE_UTF8, 0,
                                sideCheckFn, 0, 0)==SQLITE_OK);

  /* Unchanged schema: both sides fail the scratch parse, but the live
  ** schema hashes identically, so the fallback renders real values. */
  check("side_fn_setup_same", execSql(db,
      "CREATE TABLE s(a TEXT CHECK(side_ok(a)), pk TEXT PRIMARY KEY, c TEXT);"
      "INSERT INTO s VALUES('a1','k1','c1');"
      "SELECT dolt_commit('-Am','base_s');"
      "UPDATE s SET c='c2';"
      "SELECT dolt_commit('-Am','update_s');")==SQLITE_OK);
  check("side_fn_same_schema_renders",
        strcmp(queryScalarText(db,
            "SELECT from_c FROM dolt_diff_s WHERE diff_type='modified'"),
            "c1")==0);

  /* Drifted schema: the from side cannot be mapped and cannot fall back,
  ** so the read fails instead of mislabeling the dropped column's value. */
  check("side_fn_setup_drift", execSql(db,
      "CREATE TABLE t(a TEXT CHECK(side_ok(a)), pk TEXT PRIMARY KEY, c TEXT);"
      "INSERT INTO t VALUES('a1','k1','c1');"
      "SELECT dolt_commit('-Am','base_t');"
      "ALTER TABLE t DROP COLUMN a;"
      "UPDATE t SET c='c2';"
      "SELECT dolt_commit('-Am','drop_a');")==SQLITE_OK);
  check("side_fn_drift_read_fails",
        execSqlSilent(db,
            "SELECT from_c FROM dolt_diff_t WHERE diff_type='modified'")
        !=SQLITE_OK);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

/* A single-column PRIMARY KEY named "rowid" keeps stock rowid-table form
** instead of the usual conversion to WITHOUT ROWID storage. sqlite-vec
** declares its vector-chunk shadow tables exactly this way and then writes
** them through sqlite3_blob_open, which refuses WITHOUT ROWID tables. The
** PK becomes a real unique index on a rowid table — a shape prolly-side
** index maintenance never saw before this exemption — so this also pins
** uniqueness enforcement and index consistency across updates and commits. */
static void run_rowid_named_pk_keeps_rowid_table(void){
  char dbpath[512];
  sqlite3 *db = 0;
  sqlite3_blob *pBlob = 0;
  char buf[32];
  int rc;

  printf("=== Rowid-Named PK Keeps Rowid Table Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_rowid_named_pk");
  removeDbFiles(dbpath);

  rc = sqlite3_open(dbpath, &db);
  check("rowid_pk_open", rc==SQLITE_OK);
  if( !db ) return;
  check("rowid_pk_setup", execSql(db,
      "CREATE TABLE vc(rowid PRIMARY KEY, vectors BLOB NOT NULL);"
      "INSERT INTO vc VALUES(1, zeroblob(16384));")==SQLITE_OK);

  check("rowid_pk_blob_open",
        sqlite3_blob_open(db, "main", "vc", "vectors", 1, 1, &pBlob)==SQLITE_OK);
  if( pBlob ){
    check("rowid_pk_blob_write",
          sqlite3_blob_write(pBlob, "VECTORDATA", 10, 4096)==SQLITE_OK);
    check("rowid_pk_blob_close", sqlite3_blob_close(pBlob)==SQLITE_OK);
    pBlob = 0;
  }
  check("rowid_pk_write_landed",
        strcmp(queryScalarText(db,
            "SELECT substr(vectors, 4097, 10) FROM vc WHERE rowid=1"),
            "VECTORDATA")==0);

  /* The PK is enforced through a real unique index, and that index is
  ** maintained when the key column changes. */
  check("rowid_pk_unique_enforced",
        execSqlSilent(db, "INSERT INTO vc VALUES(1, x'00')")!=SQLITE_OK);
  check("rowid_pk_index_exists",
        strcmp(queryScalarText(db,
            "SELECT count(*) FROM sqlite_master"
            " WHERE name LIKE 'sqlite_autoindex_vc%'"), "1")==0);
  check("rowid_pk_update_key", execSql(db,
      "INSERT INTO vc VALUES(2, x'bb');"
      "UPDATE vc SET rowid=7 WHERE rowid=2;")==SQLITE_OK);
  check("rowid_pk_index_lookup",
        strcmp(queryScalarText(db, "SELECT hex(vectors) FROM vc WHERE rowid=7"),
               "BB")==0);
  check("rowid_pk_integrity",
        strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);

  /* The shape survives version control: commit, branch, and read the blob
  ** back on the old branch. */
  check("rowid_pk_commit",
        execSql(db, "SELECT dolt_commit('-A','-m','vc rows')")==SQLITE_OK);
  check("rowid_pk_branch",
        execSql(db, "SELECT dolt_checkout('-b','old')")==SQLITE_OK);
  check("rowid_pk_back_to_main",
        execSql(db, "SELECT dolt_checkout('main')")==SQLITE_OK);
  check("rowid_pk_mutate_main",
        execSql(db, "DELETE FROM vc WHERE rowid=7;"
                    "SELECT dolt_commit('-A','-m','drop 7')")==SQLITE_OK);
  check("rowid_pk_old_branch",
        execSql(db, "SELECT dolt_checkout('old')")==SQLITE_OK);
  memset(buf, 0, sizeof(buf));
  rc = sqlite3_blob_open(db, "main", "vc", "vectors", 1, 0, &pBlob);
  check("rowid_pk_old_branch_blob_open", rc==SQLITE_OK);
  if( pBlob ){
    check("rowid_pk_old_branch_blob_read",
          sqlite3_blob_read(pBlob, buf, 10, 4096)==SQLITE_OK
          && memcmp(buf, "VECTORDATA", 10)==0);
    sqlite3_blob_close(pBlob);
    pBlob = 0;
  }
  check("rowid_pk_old_branch_rows",
        strcmp(queryScalarText(db, "SELECT count(*) FROM vc"), "2")==0);
  check("rowid_pk_old_branch_integrity",
        strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

/* The refs blob is untrusted (it comes from a .db file or a remote fetch).
** Each section reads a u32 count and allocates count*sizeof(struct); the count
** must be bounded by the smallest possible on-disk entry and the allocation
** sized with a 64-bit product, or a crafted count wraps a 32-bit size into a
** tiny allocation that the fill loop then overruns. This verifies a valid blob
** round-trips and that oversized counts are rejected as corrupt rather than
** trusted. (The wrap itself needs a >256MB input, beyond this corpus's and the
** fuzzer's size budget, so it is prevented by construction here.) */
static void run_refs_deserialize_overflow_guard(void){
  printf("=== Refs Deserialize Overflow Guard Test ===\n\n");

  {
    u8 buf[128];
    u8 *p = buf;
    ChunkStore cs;
    int n, rc;
    *p++ = 7;                                 /* version */
    CS_WRITE_U32(p, 4); p += 4;               /* default branch name length */
    memcpy(p, "main", 4); p += 4;
    CS_WRITE_U32(p, 1); p += 4;               /* nBranches */
    CS_WRITE_U32(p, 4); p += 4;               /* branch name length */
    memcpy(p, "main", 4); p += 4;
    memset(p, 0xAB, PROLLY_HASH_SIZE); p += PROLLY_HASH_SIZE;  /* commit hash */
    memset(p, 0xCD, PROLLY_HASH_SIZE); p += PROLLY_HASH_SIZE;  /* working set */
    CS_WRITE_U32(p, 0); p += 4;               /* nTags */
    CS_WRITE_U32(p, 0); p += 4;               /* nRemotes */
    CS_WRITE_U32(p, 0); p += 4;               /* nTracking */
    CS_WRITE_U32(p, 0); p += 4;               /* nSequences */
    n = (int)(p - buf);

    memset(&cs, 0, sizeof(cs));
    rc = csDeserializeRefs(&cs, buf, n);
    check("refs_roundtrip_ok", rc==SQLITE_OK);
    check("refs_roundtrip_default_branch",
          cs.refs.zDefaultBranch && strcmp(cs.refs.zDefaultBranch, "main")==0);
    check("refs_roundtrip_branch_count", cs.refs.nBranches==1);
    check("refs_roundtrip_branch_name",
          cs.refs.nBranches==1 && cs.refs.aBranches[0].zName
          && strcmp(cs.refs.aBranches[0].zName, "main")==0);
    csFreeRefsState(&cs);
  }

  {
    u8 buf[16];
    u8 *p = buf;
    ChunkStore cs;
    int rc;
    *p++ = 7;
    CS_WRITE_U32(p, 0); p += 4;               /* empty default branch name */
    CS_WRITE_U32(p, 0x10000000); p += 4;      /* 268M branches, tiny blob */
    memset(&cs, 0, sizeof(cs));
    rc = csDeserializeRefs(&cs, buf, (int)(p - buf));
    check("refs_oversized_branches_rejected", rc==SQLITE_CORRUPT);
    csFreeRefsState(&cs);
  }

  {
    u8 buf[24];
    u8 *p = buf;
    ChunkStore cs;
    int rc;
    *p++ = 7;
    CS_WRITE_U32(p, 0); p += 4;               /* default branch name length */
    CS_WRITE_U32(p, 0); p += 4;               /* nBranches = 0 */
    CS_WRITE_U32(p, 0x10000000); p += 4;      /* oversized nTags */
    memset(&cs, 0, sizeof(cs));
    rc = csDeserializeRefs(&cs, buf, (int)(p - buf));
    check("refs_oversized_tags_rejected", rc==SQLITE_CORRUPT);
    csFreeRefsState(&cs);
  }
}

static void run_directonly_dolt_functions(void){
  static const char *zCommandNames =
    "'dolt_add','dolt_branch','dolt_checkout','dolt_cherry_pick',"
    "'dolt_clone','dolt_commit','dolt_config','dolt_conflicts_resolve',"
    "'dolt_connect_branch','dolt_creds','dolt_creds_new',"
    "'dolt_default_branch','dolt_fetch','dolt_gc','dolt_merge','dolt_pull',"
    "'dolt_push','dolt_rebase','dolt_remote','dolt_reset','dolt_revert',"
    "'dolt_tag','doltlite_internal_materialize_default_column'";
  sqlite3 *db = 0;
  char dbpath[256];
  char *zSql;
  const char *zResult;
  int rc;

  printf("=== Direct-Only Dolt Functions Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_directonly_dolt_functions");
  removeDbFiles(dbpath);
  check("directonly_open", open_db(dbpath, &db)==SQLITE_OK);
  if( !db ) return;

  zSql = sqlite3_mprintf(
    "SELECT count(*) || '|' || coalesce(sum((flags & %d)!=0),0) "
    "FROM pragma_function_list WHERE name IN (%s)",
    SQLITE_DIRECTONLY, zCommandNames);
  check("directonly_flags_query_allocated", zSql!=0);
  if( zSql ){
    zResult = queryScalarText(db, zSql);
    check("all_command_functions_are_directonly",
          strcmp(zResult, "23|23")==0);
    sqlite3_free(zSql);
  }

  zSql = sqlite3_mprintf(
    "SELECT count(*) FROM pragma_function_list "
    "WHERE name IN ('active_branch','dolt_version','dolt_merge_base',"
    "'dolt_hashof','dolt_hashof_table','dolt_hashof_db',"
    "'dolt_hashof_catalog') AND (flags & %d)!=0",
    SQLITE_DIRECTONLY);
  check("readonly_flags_query_allocated", zSql!=0);
  if( zSql ){
    zResult = queryScalarText(db, zSql);
    check("readonly_functions_are_not_directonly", strcmp(zResult, "0")==0);
    sqlite3_free(zSql);
  }

  rc = execSql(db,
    "CREATE TABLE direct_t(id INTEGER PRIMARY KEY, value TEXT);"
    "INSERT INTO direct_t VALUES(1,'one');"
    "SELECT dolt_config('user.name','Direct Only Test');"
    "SELECT dolt_config('user.email','direct@example.com');"
    "SELECT dolt_commit('-A','-m','direct call');"
    "SELECT dolt_branch('feature');"
    "SELECT dolt_tag('v1');"
    "SELECT dolt_remote('add','origin','file:///tmp/direct-only-unused');"
    "SELECT dolt_remote('remove','origin');");
  check("direct_command_calls_still_work", rc==SQLITE_OK);
  zResult = queryScalarText(db, "SELECT dolt_gc()");
  check("direct_gc_call_still_works", strstr(zResult, "ERROR:")==0);

  rc = execSql(db,
    "CREATE VIEW allowed_dolt_view AS "
    "SELECT dolt_version() AS version, active_branch() AS branch;");
  check("readonly_dolt_view_created", rc==SQLITE_OK);
  zResult = queryScalarText(db, "SELECT branch FROM allowed_dolt_view");
  check("readonly_dolt_view_works", strcmp(zResult, "main")==0);

  rc = execSql(db, "CREATE VIEW blocked_dolt_view AS SELECT dolt_gc()");
  check("directonly_view_created", rc==SQLITE_OK);
  zResult = queryScalarText(db, "SELECT * FROM blocked_dolt_view");
  check("directonly_view_rejected",
        strstr(zResult, "unsafe use of dolt_gc()")!=0);

  rc = execSql(db,
    "CREATE TABLE trigger_t(id INTEGER PRIMARY KEY);"
    "CREATE TRIGGER blocked_dolt_trigger AFTER INSERT ON trigger_t BEGIN "
    "SELECT dolt_branch('trigger-branch'); END;");
  check("directonly_trigger_created", rc==SQLITE_OK);
  rc = execSqlSilent(db, "INSERT INTO trigger_t VALUES(1)");
  check("directonly_trigger_rejected", rc==SQLITE_ERROR
        && strstr(sqlite3_errmsg(db), "unsafe use of dolt_branch()")!=0);
  check("directonly_trigger_statement_rolled_back",
        strcmp(queryScalarText(db, "SELECT count(*) FROM trigger_t"), "0")==0);
  check("directonly_trigger_had_no_branch_side_effect",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM dolt_branches WHERE name='trigger-branch'"),
          "0")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

/* A VC function evaluated in the same statement as a dolt_tags or
** dolt_branches scan mutates and reallocates the live refs arrays the
** cursors used to read rows from — the scan ended early against the
** shrinking live count and the stale index read freed memory. The cursors
** must serve rows from a snapshot taken at xFilter. */
static void run_refs_vtab_snapshot_stability(void){
  sqlite3 *db = 0;
  char dbpath[256];
  int rc;

  printf("=== Refs Vtab Snapshot Stability Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_refs_vtab_snapshot");
  removeDbFiles(dbpath);
  check("snapshot_open", open_db(dbpath, &db)==SQLITE_OK);
  if( !db ) return;

  rc = execSql(db,
    "SELECT dolt_config('user.name','snap'),"
    "       dolt_config('user.email','snap@example.com');"
    "CREATE TABLE t(a INTEGER PRIMARY KEY);"
    "INSERT INTO t VALUES(1);"
    "SELECT dolt_add('-A');"
    "SELECT dolt_commit('-m','base');"
    "SELECT dolt_tag('tag_a'), dolt_tag('tag_b'), dolt_tag('tag_c');");
  check("snapshot_setup", rc==SQLITE_OK);

  /* Deleting each tag while scanning must still visit every tag that was
  ** visible when the scan started, and delete all of them. A subquery
  ** would let the planner prune the side-effecting column, so step the
  ** scan directly. */
  {
    sqlite3_stmt *pStmt = 0;
    int nRows = 0;
    rc = sqlite3_prepare_v2(db,
        "SELECT tag_name, dolt_tag('-d', tag_name) FROM dolt_tags",
        -1, &pStmt, 0);
    check("tags_scan_prepared", rc==SQLITE_OK);
    while( sqlite3_step(pStmt)==SQLITE_ROW ) nRows++;
    check("tags_scan_finalized", sqlite3_finalize(pStmt)==SQLITE_OK);
    check("tags_scan_sees_snapshot", nRows==3);
  }
  check("tags_all_deleted",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_tags"), "0")==0);

  rc = execSql(db,
    "SELECT dolt_branch('br_a'), dolt_branch('br_b'), dolt_branch('br_c')");
  check("snapshot_branches_created", rc==SQLITE_OK);
  {
    sqlite3_stmt *pStmt = 0;
    int nRows = 0;
    rc = sqlite3_prepare_v2(db,
        "SELECT name, CASE WHEN name<>'main'"
        " THEN dolt_branch('-D', name) END FROM dolt_branches",
        -1, &pStmt, 0);
    check("branches_scan_prepared", rc==SQLITE_OK);
    while( sqlite3_step(pStmt)==SQLITE_ROW ) nRows++;
    check("branches_scan_finalized", sqlite3_finalize(pStmt)==SQLITE_OK);
    check("branches_scan_sees_snapshot", nRows==4);
  }
  check("branches_all_deleted",
        strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_branches"),
               "1")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static int storageFormatWriteU32(const char *zPath, long iOff, unsigned int v){
  unsigned char a[4];
  FILE *f;
  size_t n;
  a[0] = (unsigned char)v;
  a[1] = (unsigned char)(v >> 8);
  a[2] = (unsigned char)(v >> 16);
  a[3] = (unsigned char)(v >> 24);
  f = fopen(zPath, "r+b");
  if( !f ) return 0;
  if( fseek(f, iOff, SEEK_SET)!=0 ){
    fclose(f);
    return 0;
  }
  n = fwrite(a, 1, sizeof(a), f);
  if( fclose(f)!=0 ) return 0;
  return n==sizeof(a);
}

static int storageFormatReadHeader(
  const char *zPath,
  unsigned int *pMagic,
  unsigned int *pVersion
){
  unsigned char a[8];
  FILE *f;
  size_t n;
  f = fopen(zPath, "rb");
  if( !f ) return 0;
  n = fread(a, 1, sizeof(a), f);
  fclose(f);
  if( n!=sizeof(a) ) return 0;
  *pMagic = (unsigned int)a[0]
          | (unsigned int)a[1] << 8
          | (unsigned int)a[2] << 16
          | (unsigned int)a[3] << 24;
  *pVersion = (unsigned int)a[4]
            | (unsigned int)a[5] << 8
            | (unsigned int)a[6] << 16
            | (unsigned int)a[7] << 24;
  return 1;
}

static void storageFormatCheckNotadb(
  const char *zPath,
  const char *zOpenCheck,
  const char *zRcCheck
){
  sqlite3 *db = 0;
  int rc;
  rc = sqlite3_open(zPath, &db);
  check(zOpenCheck, rc==SQLITE_OK);
  if( rc==SQLITE_OK ){
    rc = execSqlSilent(db, "SELECT count(*) FROM t");
    check(zRcCheck, rc==SQLITE_NOTADB
                        && sqlite3_extended_errcode(db)==SQLITE_NOTADB);
  }
  sqlite3_close(db);
}

static void run_storage_format_v12(void){
  sqlite3 *db = 0;
  ChunkStore *cs;
  ProllyHash wsHash;
  ProllyHash headHash;
  DoltliteCommit commit;
  unsigned char *pData = 0;
  int nData = 0;
  int rc;
  unsigned int magic = 0;
  unsigned int version = 0;
  char dbpath[256];

  printf("=== Storage Format Version 12 Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_storage_format_v12");
  removeDbFiles(dbpath);
  memset(&commit, 0, sizeof(commit));

  check("storage_v12_open", open_db(dbpath, &db)==SQLITE_OK);
  check("storage_v12_setup", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "CREATE INDEX idx_v ON t(v);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A', '-m', 'v12');")==SQLITE_OK);
  cs = doltliteGetChunkStore(db);
  check("storage_v12_chunk_store", cs!=0);

  rc = chunkStoreGetBranchWorkingSet(cs, "main", &wsHash);
  if( rc==SQLITE_OK ) rc = chunkStoreGet(cs, &wsHash, &pData, &nData);
  check("storage_v12_working_set_is_v5",
        rc==SQLITE_OK && nData>0 && pData[0]==WS_FORMAT_VERSION_V5);
  sqlite3_free(pData);
  pData = 0;
  nData = 0;

  doltliteGetSessionHead(db, &headHash);
  rc = doltliteLoadCommit(db, &headHash, &commit);
  if( rc==SQLITE_OK ) rc = chunkStoreGet(cs, &commit.catalogHash, &pData, &nData);
  check("storage_v12_catalog_is_v5",
        rc==SQLITE_OK && nData>0 && pData[0]==CATALOG_FORMAT_V5);
  sqlite3_free(pData);
  pData = 0;
  nData = 0;

  rc = chunkStoreGet(cs, &headHash, &pData, &nData);
  check("storage_v12_commit_is_v2",
        rc==SQLITE_OK && nData>0 && pData[0]==DOLTLITE_COMMIT_V2);
  sqlite3_free(pData);
  pData = 0;
  nData = 0;

  rc = chunkStoreSerializeRefsToBlob(cs, &pData, &nData);
  check("storage_v12_refs_are_v7",
        rc==SQLITE_OK && nData>0 && pData[0]==7);
  sqlite3_free(pData);
  doltliteCommitClear(&commit);

  sqlite3_close(db);
  db = 0;
  check("storage_v12_header_read",
        storageFormatReadHeader(dbpath, &magic, &version));
  check("storage_v12_header_magic", magic==CHUNK_STORE_MAGIC);
  check("storage_v12_header_version", version==CHUNK_STORE_VERSION);

  check("storage_v12_patch_v13", storageFormatWriteU32(dbpath, 4, 13));
  storageFormatCheckNotadb(dbpath, "storage_v12_v13_open",
                          "storage_v12_v13_returns_exact_notadb");

  check("storage_v12_patch_v11", storageFormatWriteU32(dbpath, 4, 11));
  storageFormatCheckNotadb(dbpath, "storage_v12_v11_open",
                          "storage_v12_v11_returns_exact_notadb");

  check("storage_v12_restore_version", storageFormatWriteU32(dbpath, 4, 12));
  check("storage_v12_patch_magic",
        storageFormatWriteU32(dbpath, 0, 0x01234567));
  storageFormatCheckNotadb(dbpath, "storage_v12_bad_magic_open",
                          "storage_v12_bad_magic_returns_exact_notadb");

  removeDbFiles(dbpath);
}

static int collect_stepped_text(sqlite3_stmt *stmt, char *zOut, int nOut){
  int n = 0;
  int rc;
  zOut[0] = 0;
  while( (rc = sqlite3_step(stmt))==SQLITE_ROW ){
    const unsigned char *z = sqlite3_column_text(stmt, 0);
    int need = (int)strlen(zOut) + (n ? 1 : 0) + (z ? (int)strlen((const char*)z) : 4) + 1;
    if( need>nOut ) return SQLITE_TOOBIG;
    if( n ) strcat(zOut, ",");
    strcat(zOut, z ? (const char*)z : "NULL");
    n++;
  }
  return rc;
}

static void run_blob_restore_mutmap_keeps_scan(void){
  sqlite3 *db = 0;
  sqlite3_stmt *scan = 0;
  sqlite3_stmt *ins = 0;
  char dbpath[256];
  char seen[128];
  int rc;

  printf("=== Blob Restore MutMap Keeps Scan Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_blob_restore_mutmap_keeps_scan");
  removeDbFiles(dbpath);

  check("open_db_for_blob_restore", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_wr_table_for_blob_restore", execSql(db,
    "CREATE TABLE t(a TEXT PRIMARY KEY, b INT) WITHOUT ROWID;")==SQLITE_OK);
  check("begin_wr_blob_restore_txn", execSql(db, "BEGIN;")==SQLITE_OK);
  check("insert_pending_wr_rows",
        execSql(db, "INSERT INTO t VALUES('a',1),('b',2),('c',3);")==SQLITE_OK);

  rc = sqlite3_prepare_v2(db, "SELECT a FROM t ORDER BY a;", -1, &scan, 0);
  check("prepare_wr_scan", rc==SQLITE_OK);
  rc = sqlite3_prepare_v2(db, "INSERT INTO t VALUES('az',11);", -1, &ins, 0);
  check("prepare_wr_insert", rc==SQLITE_OK);
  if( scan && ins ){
    rc = sqlite3_step(scan);
    check("wr_scan_first", rc==SQLITE_ROW && stmt_column_text_equals(scan, 0, "a"));
    check("wr_insert_mid_scan", sqlite3_step(ins)==SQLITE_DONE);
    rc = collect_stepped_text(scan, seen, (int)sizeof(seen));
    check("wr_scan_rest_done", rc==SQLITE_DONE);
    check("wr_scan_sees_remaining", strcmp(seen, "az,b,c")==0);
  }
  sqlite3_finalize(scan);
  sqlite3_finalize(ins);
  scan = 0;
  ins = 0;
  check("wr_final_rows",
        strcmp(queryScalarText(db,
          "SELECT group_concat(a, ',') FROM (SELECT a FROM t ORDER BY a)"),
          "a,az,b,c")==0);
  check("commit_wr_blob_restore", execSql(db, "COMMIT;")==SQLITE_OK);

  check("setup_index_for_blob_restore", execSql(db,
    "CREATE TABLE w(id INTEGER PRIMARY KEY, v TEXT UNIQUE);"
    "CREATE INDEX wv ON w(v);")==SQLITE_OK);
  check("begin_idx_blob_restore_txn", execSql(db, "BEGIN;")==SQLITE_OK);
  check("insert_pending_idx_rows",
        execSql(db, "INSERT INTO w VALUES(1,'a'),(2,'c');")==SQLITE_OK);
  rc = sqlite3_prepare_v2(db,
    "SELECT v FROM w INDEXED BY wv WHERE v>='a';", -1, &scan, 0);
  check("prepare_idx_scan", rc==SQLITE_OK);
  rc = sqlite3_prepare_v2(db, "INSERT INTO w VALUES(3,'b');", -1, &ins, 0);
  check("prepare_idx_insert", rc==SQLITE_OK);
  if( scan && ins ){
    rc = sqlite3_step(scan);
    check("idx_scan_first", rc==SQLITE_ROW && stmt_column_text_equals(scan, 0, "a"));
    check("idx_insert_mid_scan", sqlite3_step(ins)==SQLITE_DONE);
    rc = collect_stepped_text(scan, seen, (int)sizeof(seen));
    check("idx_scan_rest_done", rc==SQLITE_DONE);
    check("idx_scan_sees_remaining", strcmp(seen, "b,c")==0);
  }
  sqlite3_finalize(scan);
  sqlite3_finalize(ins);
  check("commit_idx_blob_restore", execSql(db, "COMMIT;")==SQLITE_OK);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_intpk_scan_delete_keeps_scan(void){
  sqlite3 *db = 0;
  sqlite3_stmt *scan = 0;
  sqlite3_stmt *del = 0;
  char dbpath[256];
  char seen[128];
  int rc;

  printf("=== INT PK Scan Delete Keeps Scan Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_intpk_scan_delete_keeps_scan");
  removeDbFiles(dbpath);

  check("open_db_for_intpk_scan_delete", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_committed_intpk_rows", execSql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e');")==SQLITE_OK);
  check("begin_committed_intpk_scan_delete", execSql(db, "BEGIN;")==SQLITE_OK);

  rc = sqlite3_prepare_v2(db, "SELECT id FROM t ORDER BY id;", -1, &scan, 0);
  check("prepare_committed_intpk_scan", rc==SQLITE_OK);
  rc = sqlite3_prepare_v2(db, "DELETE FROM t WHERE id=?;", -1, &del, 0);
  check("prepare_committed_intpk_delete", rc==SQLITE_OK);
  seen[0] = 0;
  if( scan && del ){
    while( (rc = sqlite3_step(scan))==SQLITE_ROW ){
      int id = sqlite3_column_int(scan, 0);
      if( seen[0] ) strcat(seen, ",");
      sprintf(seen+strlen(seen), "%d", id);
      if( (id%2)==0 ){
        sqlite3_reset(del);
        sqlite3_bind_int(del, 1, id);
        check("committed_intpk_delete_even", sqlite3_step(del)==SQLITE_DONE);
      }
    }
    check("committed_intpk_scan_done", rc==SQLITE_DONE);
    check("committed_intpk_scan_sees_all", strcmp(seen, "1,2,3,4,5")==0);
  }
  sqlite3_finalize(scan);
  sqlite3_finalize(del);
  scan = 0;
  del = 0;
  check("committed_intpk_leftover_odds",
        strcmp(queryScalarText(db,
          "SELECT group_concat(id, ',') FROM (SELECT id FROM t ORDER BY id)"),
          "1,3,5")==0);
  check("rollback_committed_intpk_scan_delete", execSql(db, "ROLLBACK;")==SQLITE_OK);

  check("setup_uncommitted_intpk_rows", execSql(db,
    "CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT);"
    "BEGIN;"
    "INSERT INTO u VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e');")==SQLITE_OK);
  rc = sqlite3_prepare_v2(db, "SELECT id FROM u ORDER BY id;", -1, &scan, 0);
  check("prepare_uncommitted_intpk_scan", rc==SQLITE_OK);
  rc = sqlite3_prepare_v2(db, "DELETE FROM u WHERE id=?;", -1, &del, 0);
  check("prepare_uncommitted_intpk_delete", rc==SQLITE_OK);
  seen[0] = 0;
  if( scan && del ){
    while( (rc = sqlite3_step(scan))==SQLITE_ROW ){
      int id = sqlite3_column_int(scan, 0);
      if( seen[0] ) strcat(seen, ",");
      sprintf(seen+strlen(seen), "%d", id);
      if( (id%2)==0 ){
        sqlite3_reset(del);
        sqlite3_bind_int(del, 1, id);
        check("uncommitted_intpk_delete_even", sqlite3_step(del)==SQLITE_DONE);
      }
    }
    check("uncommitted_intpk_scan_done", rc==SQLITE_DONE);
    check("uncommitted_intpk_scan_sees_all", strcmp(seen, "1,2,3,4,5")==0);
  }
  sqlite3_finalize(scan);
  sqlite3_finalize(del);
  check("uncommitted_intpk_leftover_odds",
        strcmp(queryScalarText(db,
          "SELECT group_concat(id, ',') FROM (SELECT id FROM u ORDER BY id)"),
          "1,3,5")==0);
  check("rollback_uncommitted_intpk_scan_delete", execSql(db, "ROLLBACK;")==SQLITE_OK);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_count_flush_keeps_scan(void){
  sqlite3 *db = 0;
  sqlite3_stmt *scan = 0;
  char dbpath[256];
  char seen[128];
  int rc;

  printf("=== Count Flush Keeps Scan Test ===\n\n");
  make_dbpath(dbpath, sizeof(dbpath), "test_count_flush_keeps_scan");
  removeDbFiles(dbpath);

  check("open_db_for_count_flush", open_db(dbpath, &db)==SQLITE_OK);
  check("setup_wr_for_count_flush", execSql(db,
    "CREATE TABLE t(a TEXT PRIMARY KEY, b INT) WITHOUT ROWID;")==SQLITE_OK);
  check("begin_wr_count_flush", execSql(db, "BEGIN;")==SQLITE_OK);
  check("insert_wr_pending_for_count_flush",
        execSql(db, "INSERT INTO t VALUES('a',1),('b',2),('c',3);")==SQLITE_OK);
  rc = sqlite3_prepare_v2(db,
    "SELECT a || '|' || (SELECT count(*) FROM t) FROM t ORDER BY a;",
    -1, &scan, 0);
  check("prepare_wr_count_flush_scan", rc==SQLITE_OK);
  if( scan ){
    rc = collect_stepped_text(scan, seen, (int)sizeof(seen));
    check("wr_count_flush_scan_done", rc==SQLITE_DONE);
    check("wr_count_flush_sees_all", strcmp(seen, "a|3,b|3,c|3")==0);
  }
  sqlite3_finalize(scan);
  scan = 0;
  check("rollback_wr_count_flush", execSql(db, "ROLLBACK;")==SQLITE_OK);

  check("setup_intpk_for_count_flush", execSql(db,
    "CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT);")==SQLITE_OK);
  check("begin_intpk_count_flush", execSql(db, "BEGIN;")==SQLITE_OK);
  check("insert_intpk_pending_for_count_flush",
        execSql(db, "INSERT INTO u VALUES(1,'a'),(2,'b'),(3,'c');")==SQLITE_OK);
  rc = sqlite3_prepare_v2(db,
    "SELECT v || '|' || (SELECT count(*) FROM u) FROM u ORDER BY id;",
    -1, &scan, 0);
  check("prepare_intpk_count_flush_scan", rc==SQLITE_OK);
  if( scan ){
    rc = collect_stepped_text(scan, seen, (int)sizeof(seen));
    check("intpk_count_flush_scan_done", rc==SQLITE_DONE);
    check("intpk_count_flush_sees_all", strcmp(seen, "a|3,b|3,c|3")==0);
  }
  sqlite3_finalize(scan);
  check("rollback_intpk_count_flush", execSql(db, "ROLLBACK;")==SQLITE_OK);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static void run_negzero_sortkey_eq(void){
  static const u8 posZero[] = {
    0x02, 0x07,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
  };
  static const u8 negZero[] = {
    0x02, 0x07,
    0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
  };
  sqlite3 *db = 0;
  u8 *pPos = 0;
  u8 *pNeg = 0;
  char dbpath[256];
  int nPos = 0;
  int nNeg = 0;
  int rc;

  printf("=== Negzero Sortkey Eq Test ===\n\n");

  rc = sortKeyFromRecord(posZero, (int)sizeof(posZero), &pPos, &nPos);
  check("negzero_pos_encode_ok", rc==SQLITE_OK);
  rc = sortKeyFromRecord(negZero, (int)sizeof(negZero), &pNeg, &nNeg);
  check("negzero_neg_encode_ok", rc==SQLITE_OK);
  check("negzero_keys_same_width", nPos==nNeg && nPos>0);
  check("negzero_keys_equal",
        nPos==nNeg && nPos>0 && pPos && pNeg && memcmp(pPos, pNeg, nPos)==0);
  sqlite3_free(pPos);
  sqlite3_free(pNeg);

  make_dbpath(dbpath, sizeof(dbpath), "test_negzero_sortkey_eq");
  removeDbFiles(dbpath);
  check("open_db_for_negzero", open_db(dbpath, &db)==SQLITE_OK);

  check("setup_negzero_real_pk", execSql(db,
    "CREATE TABLE t(id REAL PRIMARY KEY, v INT) WITHOUT ROWID;")==SQLITE_OK);
  check("insert_poszero_pk", execSql(db, "INSERT INTO t VALUES (0.0, 1);")==SQLITE_OK);
  rc = execSqlSilent(db, "INSERT INTO t VALUES (-0.0, 2);");
  check("negzero_pk_unique", (rc&0xff)==SQLITE_CONSTRAINT);
  check("negzero_pk_row_count",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "1")==0);
  check("negzero_pk_seek_neg",
        strcmp(queryScalarText(db, "SELECT v FROM t WHERE id = -0.0"), "1")==0);
  check("negzero_pk_seek_pos",
        strcmp(queryScalarText(db, "SELECT v FROM t WHERE id = 0.0"), "1")==0);

  check("setup_negzero_first_pk", execSql(db,
    "CREATE TABLE w(id REAL PRIMARY KEY, v INT) WITHOUT ROWID;")==SQLITE_OK);
  check("insert_negzero_pk_first",
        execSql(db, "INSERT INTO w VALUES (-0.0, 7);")==SQLITE_OK);
  check("poszero_seek_after_neg_insert",
        strcmp(queryScalarText(db, "SELECT v FROM w WHERE id = 0.0"), "7")==0);
  rc = execSqlSilent(db, "INSERT INTO w VALUES (0.0, 8);");
  check("poszero_pk_unique_after_neg", (rc&0xff)==SQLITE_CONSTRAINT);

  check("setup_negzero_secondary", execSql(db,
    "CREATE TABLE u(id INTEGER PRIMARY KEY, v REAL);"
    "CREATE INDEX uv ON u(v);"
    "CREATE INDEX ud ON u(v DESC);"
    "INSERT INTO u VALUES (1, 0.0), (2, 1.5), (3, -0.0);")==SQLITE_OK);
  check("negzero_idx_eq_neg",
        strcmp(queryScalarText(db,
          "SELECT group_concat(id) FROM (SELECT id FROM u WHERE v = -0.0 ORDER BY id)"),
          "1,3")==0);
  check("negzero_idx_eq_pos",
        strcmp(queryScalarText(db,
          "SELECT group_concat(id) FROM (SELECT id FROM u WHERE v = 0.0 ORDER BY id)"),
          "1,3")==0);
  check("negzero_idx_count",
        strcmp(queryScalarText(db, "SELECT count(*) FROM u WHERE v = -0.0"),
               "2")==0);
  check("negzero_not_indexed_count",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM u NOT INDEXED WHERE v = -0.0"),
          "2")==0);
  check("negzero_between_count",
        strcmp(queryScalarText(db,
          "SELECT count(*) FROM u WHERE v BETWEEN -0.0 AND 0.0"),
          "2")==0);
  check("negzero_desc_idx_eq_neg",
        strcmp(queryScalarText(db,
          "SELECT group_concat(id) FROM ("
          "SELECT id FROM u INDEXED BY ud WHERE v = -0.0 ORDER BY id)"),
          "1,3")==0);

  sqlite3_close(db);
  removeDbFiles(dbpath);
}

static const RegressionCase aCases[] = {
  { "refs_vtab_snapshot_stability", "Refs Vtab Snapshot Stability Test", run_refs_vtab_snapshot_stability },
  { "storage_format_v12", "Storage Format Version 12 Test", run_storage_format_v12 },
  { "directonly_dolt_functions", "Direct-Only Dolt Functions Test", run_directonly_dolt_functions },
  { "refs_deserialize_overflow_guard", "Refs Deserialize Overflow Guard Test", run_refs_deserialize_overflow_guard },
  { "backup_safety", "Backup Safety Test", run_backup_safety },
  { "integer_pk_autocommit_append_correctness", "Integer PK Autocommit Append Correctness Test", run_integer_pk_autocommit_append_correctness },
  { "custom_collation_unindexed", "Custom Collation Unindexed Test", run_custom_collation_unindexed },
  { "serialize_deserialize", "Serialize Deserialize Test", run_serialize_deserialize },
  { "memory_readonly_open", "Memory Read-Only Open Test", run_memory_readonly_open },
  { "rowid_in_integer_literals_uses_rowset", "Rowid IN Integer Literals RowSet Test", run_rowid_in_integer_literals_uses_rowset },
  { "concurrent_refs", "Concurrent Refs Test", run_concurrent_refs },
  { "checkout_persist_failure", "Checkout Persist Failure Test", run_checkout_persist_failure },
  { "savepoint_catalog_restore", "Savepoint Catalog Restore Test", run_savepoint_catalog_restore },
  { "session_string_setter_oom", "Session String Setter OOM Test", run_session_string_setter_oom },
  { "schema_hash_error_propagation", "Schema Hash Error Propagation Test", run_schema_hash_error_propagation },
  { "refs_blob_corruption", "Refs Blob Corruption Test", run_refs_blob_corruption },
  { "refresh_error_propagation", "Refresh Error Propagation Test", run_refresh_error_propagation },
  { "conflicts_blob_corruption", "Conflicts Blob Corruption Test", run_conflicts_blob_corruption },
  { "status_error_propagation", "Status Error Propagation Test", run_status_error_propagation },
  { "status_many_table_renames", "Status Many Table Renames Test", run_status_many_table_renames },
  { "refs_commit_merges_concurrent_peer_refs", "Refs Commit Merges Concurrent Peer Refs Test", run_refs_commit_merges_concurrent_peer_refs },
  { "refs_merge_rejects_dangling_default", "Refs Merge Rejects Dangling Default Test", run_refs_merge_rejects_dangling_default },
  { "refs_commit_retry_after_conflict_still_fails", "Refs Commit Retry After Conflict Still Fails Test", run_refs_commit_retry_after_conflict_still_fails },
  { "refs_merge_keeps_peer_sequence_drop", "Refs Merge Keeps Peer Sequence Drop Test", run_refs_merge_keeps_peer_sequence_drop },
  { "refs_commit_conflicting_peer_ref_fails", "Refs Commit Conflicting Peer Ref Fails Test", run_refs_commit_conflicting_peer_ref_fails },
  { "remote_refs_corruption", "Remote Refs Corruption Test", run_remote_refs_corruption },
  { "fetch_ref_install_survives_window_gc", "Fetch Ref Install Survives Window GC Test", run_fetch_ref_install_survives_window_gc },
  { "clone_ref_install_survives_window_gc", "Clone Ref Install Survives Window GC Test", run_clone_ref_install_survives_window_gc },
  { "fetch_preserves_concurrent_local_refs", "Fetch Preserves Concurrent Local Refs Test", run_fetch_preserves_concurrent_local_refs },
  { "chunk_walk_corruption", "Chunk Walk Corruption Test", run_chunk_walk_corruption },
  { "catalog_deserialize_corruption", "Catalog Deserialize Corruption Test", run_catalog_deserialize_corruption },
  { "schema_loader_missing_master", "Schema Loader Missing Master Test", run_schema_loader_missing_master },
  { "transfer_row_blobkey_uses_record", "TransferRow Blob-Key Uses Record Test", run_transfer_row_blobkey_uses_record },
  { "ancestor_missing_start", "Ancestor Missing Start Test", run_ancestor_missing_start },
  { "ancestor_criss_cross_single_walk", "Ancestor Criss-Cross Single Walk Test", run_ancestor_criss_cross_single_walk },
  { "pull_persist_failure", "Pull Persist Failure Test", run_pull_persist_failure },
  { "push_persist_failure", "Push Persist Failure Test", run_push_persist_failure },
  { "pull_new_branch_persists", "Pull New Branch Persists Test", run_pull_new_branch_persists },
  { "pull_dirty_working_set_fails", "Pull Dirty Working Set Fails Test", run_pull_dirty_working_set_fails },
  { "pull_staged_changes_fails", "Pull Staged Changes Fails Test", run_pull_staged_changes_fails },
  { "push_persist_failure", "Push Persist Failure Test", run_push_persist_failure },
  { "clone_persist_failure", "Clone Persist Failure Test", run_clone_persist_failure },
  { "resolve_ref_non_commit", "Resolve Ref Non-Commit Test", run_resolve_ref_non_commit },
  { "commit_parent_limit", "Commit Parent Limit Test", run_commit_parent_limit },
  { "commit_am_many_tables", "Commit -am Many Tables Test", run_commit_am_many_tables },
  { "blame_all_parents_merge_base", "Blame All-Parents Merge Base Test", run_blame_all_parents_merge_base },
  { "blame_deep_history_scan", "Blame Deep History Scan Test", run_blame_deep_history_scan },
  { "blame_wide_primary_key", "Blame Wide Primary Key Test", run_blame_wide_primary_key },
  { "merge_persist_failure", "Merge Persist Failure Test", run_merge_persist_failure },
  { "merge_conflict_persist_failure", "Merge Conflict Persist Failure Test", run_merge_conflict_persist_failure },
  { "conflict_serializer_bounds", "Conflict Serializer Bounds Test", run_conflict_serializer_bounds },
  { "merge_conflict_surfaces_error", "Merge Conflict Surfaces Error Test", run_merge_conflict_surfaces_error_and_rollback_clears_durable_state },
  { "failed_merge_reopen_preserves_working_set_state", "Failed Merge Reopen Preserves Working Set State Test", run_failed_merge_reopen_clears_ephemeral_conflict_state },
  { "cherry_pick_stale_branch", "Cherry-pick Stale Branch Test", run_cherry_pick_stale_branch },
  { "cherry_pick_stale_conflict_clears_session", "Cherry-pick Stale Conflict Clears Session Test", run_cherry_pick_stale_conflict_clears_session },
  { "cherry_pick_conflict_is_not_a_merge", "Cherry-pick Conflict Is Not A Merge Test", run_cherry_pick_conflict_is_not_a_merge },
  { "revert_conflict_is_not_a_merge", "Revert Conflict Is Not A Merge Test", run_revert_conflict_is_not_a_merge },
  { "cherry_pick_seal_fail_keeps_advanced_tip", "Cherry-pick Seal Failure Keeps Advanced Tip Test", run_cherry_pick_seal_fail_keeps_advanced_tip },
  { "failed_cherry_pick_reopen_preserves_conflict_state", "Failed Cherry-pick Reopen Preserves Conflict State Test", run_failed_cherry_pick_reopen_preserves_conflict_state },
  { "branches_metadata_corruption", "Branches Metadata Corruption Test", run_branches_metadata_corruption },
  { "gc_rewrite_failure", "GC Rewrite Failure Test", run_gc_rewrite_failure },
  { "record_decode_corruption", "Record Decode Corruption Test", run_record_decode_corruption },
  { "sync_rejects_wrong_chunk", "Sync Chunk Address Verification Test", run_sync_rejects_wrong_chunk },
  { "ref_array_grow_overflow", "Ref Array Grow Overflow Test", run_ref_array_grow_rejects_overflow },
  { "record_header_varint_boundary", "Record Header Varint Boundary Test", run_record_header_varint_boundary },
  { "sortkey_two_numeric_roundtrip", "Sortkey Two Numeric Roundtrip Test", run_sortkey_two_numeric_roundtrip },
  { "sortkey_numeric_text_roundtrip", "Sortkey Numeric Text Roundtrip Test", run_sortkey_numeric_text_roundtrip },
  { "sortkey_numeric_blob_roundtrip", "Sortkey Numeric Blob Roundtrip Test", run_sortkey_numeric_blob_roundtrip },
  { "sortkey_buffer_exact_size", "Sortkey Buffer Exact Size Test", run_sortkey_buffer_exact_size },
  { "sortkey_mem_matches_record", "Sortkey Mem Matches Record Test", run_sortkey_mem_matches_record },
  { "reload_refs_transactional", "Reload Refs Transactional Test", run_reload_refs_transactional },
  { "refresh_refs_corruption_preserves_state", "Refresh Corrupt Refs State Preservation Test", run_refresh_refs_corruption_preserves_state },
  { "prolly_node_corruption", "Prolly Node Corruption Test", run_prolly_node_corruption },
  { "truncated_wal_rejected", "Truncated WAL Rejected Test", run_truncated_wal_is_rejected },
  { "refresh_open_path_transactional", "Refresh Open Path Transactional Test", run_refresh_open_path_transactional },
  { "wal_offset_corruption", "WAL Offset Corruption Test", run_wal_offset_corruption_is_rejected },
  { "wal_mid_corruption_rejected", "WAL Mid-Corruption Rejected Test", run_wal_mid_corruption_rejected },
  { "integrity_check_walks_nodes", "Integrity Check Walks Prolly Nodes Test", run_integrity_check_walks_prolly_nodes },
  { "memory_chunk_lookup_corruption", "Memory Chunk Lookup Corruption Test", run_memory_chunk_lookup_corruption },
  { "prolly_diff_record_corruption", "Prolly Diff Record Corruption Test", run_prolly_diff_record_corruption },
  { "integrity_check_repo_state", "Integrity Check Repository State Test", run_integrity_check_repo_state },
  { "integrity_check_session_merge_state", "Integrity Check Session Merge State Test", run_integrity_check_session_merge_state },
  { "prepared_stmt_reuse_after_commit", "Prepared Statement Reuse After Commit Test", run_prepared_stmt_reuse_after_commit },
  { "prepared_stmt_reuse_after_schema_checkout", "Prepared Statement Reuse After Schema Checkout Test", run_prepared_stmt_reuse_after_schema_checkout },
  { "persist_does_not_resurrect_deleted_branch", "Persist Does Not Resurrect Deleted Branch Test", run_persist_does_not_resurrect_deleted_branch },
  { "peer_commit_keeps_local_row_durable", "Peer Commit Keeps Local Row Durable Test", run_peer_commit_keeps_local_row_durable },
  { "commit_recovers_after_peer_commit", "Commit Recovers After Peer Commit Test", run_commit_recovers_after_peer_commit },
  { "working_set_refreshes_staged_across_connections", "Working Set Refreshes Staged Across Connections Test", run_working_set_refreshes_staged_across_connections },
  { "reopen_preserves_staged_working_set", "Reopen Preserves Staged Working Set Test", run_reopen_preserves_staged_working_set },
  { "begin_write_refreshes_working_set_metadata", "Begin Write Refreshes Working Set Metadata Test", run_begin_write_refreshes_working_set_metadata },
  { "begin_write_from_stale_read_snapshot", "Begin Write From Stale Read Snapshot Test", run_begin_write_from_stale_read_snapshot },
  { "open_rejects_corrupt_working_set", "Open Rejects Corrupt Working Set Test", run_open_rejects_corrupt_working_set },
  { "working_set_blob_size_validation", "Working Set Blob Size Validation Test", run_working_set_blob_size_validation },
  { "open_ignores_stale_working_set", "Open Ignores Stale Working Set Test", run_open_ignores_stale_working_set },
  { "diff_stat_requires_refs", "Diff Stat Requires Refs Test", run_diff_stat_requires_refs },
  { "diff_stat_wide_modified_rows", "Diff Stat Wide Modified Rows Test", run_diff_stat_wide_modified_rows },
  { "diff_table_deep_history_map", "Diff Table Deep History Map Test", run_diff_table_deep_history_map },
  { "diff_stat_surfaces_corrupt_root", "Diff Stat Surfaces Corrupt Root Test", run_diff_stat_surfaces_corrupt_root },
  { "diff_surfaces_read_errors", "Diff Surfaces Read Errors Test", run_diff_surfaces_read_errors },
  { "table_moveto_mutmap_delete_preserves_neighbors", "Table Moveto MutMap Delete Preserves Neighbors Test", run_table_moveto_mutmap_delete_preserves_neighbors },
  { "table_moveto_mutmap_exact_keeps_iteration_aligned", "Table Moveto MutMap Exact Keeps Iteration Aligned Test", run_table_moveto_mutmap_exact_keeps_iteration_aligned },
  { "index_moveto_mutmap_exact_keeps_iteration_aligned", "Index Moveto MutMap Exact Keeps Iteration Aligned Test", run_index_moveto_mutmap_exact_keeps_iteration_aligned },
  { "btree_commit_failure_transactional", "Btree Commit Failure Transaction Test", run_btree_commit_failure_transactional },
  { "commit_rejects_renamed_database", "Commit Rejects Renamed Database Test", run_commit_rejects_renamed_database },
  { "write_rejects_foreign_database_at_path", "Write Rejects Foreign Database At Path Test", run_write_rejects_foreign_database_at_path },
  { "attached_database_seed_and_repair", "Attached Database Seed And Repair Test", run_attached_database_seed_and_repair },
  { "savepoint_restores_session_metadata", "Savepoint Restores Session Metadata Test", run_savepoint_restores_session_metadata },
  { "savepoint_flush_snapshot_rollback_reopen", "Savepoint Flush Snapshot Rollback Reopen Test", run_savepoint_flush_snapshot_rollback_reopen },
  { "savepoint_flush_snapshot_release_reopen", "Savepoint Flush Snapshot Release Reopen Test", run_savepoint_flush_snapshot_release_reopen },
  { "savepoint_failed_commit_rollback_reopen", "Savepoint Failed Commit Rollback Reopen Test", run_savepoint_failed_commit_rollback_reopen },
  { "savepoint_failed_commit_release_reopen", "Savepoint Failed Commit Release Reopen Test", run_savepoint_failed_commit_release_reopen },
  { "savepoint_failed_commit_outer_rollback_reopen", "Savepoint Failed Commit Outer Rollback Reopen Test", run_savepoint_failed_commit_outer_rollback_reopen },
  { "savepoint_flush_snapshot_multi_table_rollback_reopen", "Savepoint Flush Snapshot Multi Table Rollback Reopen Test", run_savepoint_flush_snapshot_multi_table_rollback_reopen },
  { "savepoint_same_name_shadowing_index_reopen", "Savepoint Same Name Shadowing Index Reopen Test", run_savepoint_same_name_shadowing_index_reopen },
  { "savepoint_schema_rollback_reopen", "Savepoint Schema Rollback Reopen Test", run_savepoint_schema_rollback_reopen },
  { "mutmap_rollback_stale_order", "Mutmap Rollback Stale Order Test", run_mutmap_rollback_stale_order },
  { "savepoint_trigger_rollback_reopen", "Savepoint Trigger Rollback Reopen Test", run_savepoint_trigger_rollback_reopen },
  { "begin_release_then_outer_rollback_reopen", "Begin Release Then Outer Rollback Reopen Test", run_begin_release_then_outer_rollback_reopen },
  { "savepoint_failed_commit_schema_rollback_reopen", "Savepoint Failed Commit Schema Rollback Reopen Test", run_savepoint_failed_commit_schema_rollback_reopen },
  { "savepoint_nested_trigger_inner_rollback_reopen", "Savepoint Nested Trigger Inner Rollback Reopen Test", run_savepoint_nested_trigger_inner_rollback_reopen },
  { "hard_reset_failure_restores_memory_state", "Hard Reset Failure Restores Memory State Test", run_hard_reset_failure_restores_memory_state },
  { "hard_reset_command_failure_preserves_durable_state", "Hard Reset Command Failure Preserves Durable State Test", run_hard_reset_command_failure_preserves_durable_state },
  { "amend_persist_failure_preserves_durable_state", "Amend Persist Failure Preserves Durable State Test", run_amend_persist_failure_preserves_durable_state },
  { "delete_current_branch_failure_preserves_durable_state", "Delete Current Branch Failure Preserves Durable State Test", run_delete_current_branch_failure_preserves_durable_state },
  { "delete_missing_branch_preserves_durable_state", "Delete Missing Branch Preserves Durable State Test", run_delete_missing_branch_preserves_durable_state },
  { "force_delete_missing_branch_preserves_durable_state", "Force Delete Missing Branch Preserves Durable State Test", run_force_delete_missing_branch_preserves_durable_state },
  { "rebase_continue_invalid_plan_preserves_durable_state", "Rebase Continue Invalid Plan Preserves Durable State Test", run_rebase_continue_invalid_plan_preserves_durable_state },
  { "branch_copy_existing_dest_preserves_durable_state", "Branch Copy Existing Dest Preserves Durable State Test", run_branch_copy_existing_dest_preserves_durable_state },
  { "branch_copy_missing_source_preserves_durable_state", "Branch Copy Missing Source Preserves Durable State Test", run_branch_copy_missing_source_preserves_durable_state },
  { "branch_rename_missing_source_preserves_durable_state", "Branch Rename Missing Source Preserves Durable State Test", run_branch_rename_missing_source_preserves_durable_state },
  { "branch_create_existing_name_preserves_durable_state", "Branch Create Existing Name Preserves Durable State Test", run_branch_create_existing_name_preserves_durable_state },
  { "branch_create_bad_start_preserves_durable_state", "Branch Create Bad Start Preserves Durable State Test", run_branch_create_bad_start_preserves_durable_state },
  { "revert_bad_ref_failure_preserves_durable_state", "Revert Bad Ref Failure Preserves Durable State Test", run_revert_bad_ref_failure_preserves_durable_state },
  { "cherry_pick_bad_ref_failure_preserves_durable_state", "Cherry-pick Bad Ref Failure Preserves Durable State Test", run_cherry_pick_bad_ref_failure_preserves_durable_state },
  { "merge_nonexistent_branch_preserves_durable_state", "Merge Nonexistent Branch Preserves Durable State Test", run_merge_nonexistent_branch_preserves_durable_state },
  { "merge_abort_after_reopen_restores_durable_state", "Merge Abort After Reopen Restores Durable State Test", run_merge_abort_after_reopen_restores_durable_state },
  { "rebase_continue_without_active_preserves_durable_state", "Rebase Continue Without Active Preserves Durable State Test", run_rebase_continue_without_active_preserves_durable_state },
  { "rebase_abort_after_reopen_restores_durable_state", "Rebase Abort After Reopen Restores Durable State Test", run_rebase_abort_after_reopen_restores_durable_state },
  { "rebase_abort_after_reopen_dirty_default", "Rebase Abort After Reopen Dirty Default Test", run_rebase_abort_after_reopen_dirty_default },
  { "rebase_continue_after_reopen_dirty_default", "Rebase Continue After Reopen Dirty Default Test", run_rebase_continue_after_reopen_dirty_default },
  { "rebase_continue_cas_keeps_dirty_default", "Rebase Continue CAS Keeps Dirty Default Test", run_rebase_continue_cas_keeps_dirty_default },
  { "rebase_abort_after_reopen_onto_other", "Rebase Abort After Reopen Onto Other Test", run_rebase_abort_after_reopen_onto_other },
  { "rebase_continue_after_reopen_onto_other", "Rebase Continue After Reopen Onto Other Test", run_rebase_continue_after_reopen_onto_other },
  { "rebase_abort_retry_after_claim_commit_error", "Rebase Abort Retry After Claim Commit Error Test", run_rebase_abort_retry_after_claim_commit_error },
  { "rebase_plan_read_error_is_not_partial", "Rebase Plan Read Error Is Not Partial Test", run_rebase_plan_read_error_is_not_partial },
  { "rebase_upstream_history_failure_is_atomic", "Rebase Upstream History Failure Is Atomic Test", run_rebase_upstream_history_failure_is_atomic },
  { "rebase_start_failure_cleans_working_branch", "Rebase Start Failure Cleans Working Branch Test", run_rebase_start_failure_cleans_working_branch },
  { "rebase_recovery_failure_is_reported", "Rebase Recovery Failure Is Reported Test", run_rebase_recovery_failure_is_reported },
  { "rebase_continue_conflict_abort_restores_durable_state", "Rebase Continue Conflict Abort Restores Durable State Test", run_rebase_continue_conflict_abort_restores_durable_state },
  { "rebase_concurrent_peer_commit_is_preserved", "Rebase Concurrent Peer Commit Preservation Test", run_rebase_concurrent_peer_commit_is_preserved },
  { "rebase_continue_keeps_peer_branch", "Rebase Continue Keeps Peer Branch Test", run_rebase_continue_keeps_peer_branch },
  { "rebase_main_table_schema_guard", "Rebase Main Table Schema Guard Test", run_rebase_main_table_schema_guard },
  { "rebase_temp_shadow_ignored", "Rebase Temp Shadow Ignored Test", run_rebase_temp_shadow_ignored },
  { "remote_add_duplicate_preserves_durable_state", "Remote Add Duplicate Preserves Durable State Test", run_remote_add_duplicate_preserves_durable_state },
  { "remote_remove_missing_preserves_durable_state", "Remote Remove Missing Preserves Durable State Test", run_remote_remove_missing_preserves_durable_state },
  { "checkout_nonexistent_preserves_durable_state", "Checkout Nonexistent Preserves Durable State Test", run_checkout_nonexistent_preserves_durable_state },
  { "checkout_dash_b_existing_branch_preserves_durable_state", "Checkout -b Existing Branch Preserves Durable State Test", run_checkout_dash_b_existing_branch_preserves_durable_state },
  { "reset_bad_ref_failure_preserves_durable_state", "Reset Bad Ref Failure Preserves Durable State Test", run_reset_bad_ref_failure_preserves_durable_state },
  { "mutmap_empty_reverse_iter", "MutMap Empty Reverse Iterator Test", run_mutmap_empty_reverse_iter },
  { "mutmap_delete_reinsert_reuses_entry", "MutMap Delete Reinsert Reuses Entry Test", run_mutmap_delete_reinsert_reuses_entry },
  { "mutmap_append_sorted_order", "MutMap Append Sorted Order Test", run_mutmap_append_sorted_order },
  { "mutmap_resolve_sorted_pos", "MutMap ResolveSortedPos Test", run_mutmap_resolve_sorted_pos },
  { "mutmap_differential_randomized", "MutMap Differential Randomized Test", run_mutmap_differential_randomized },
  { "prolly_mutate_skip_subtree_order", "Prolly Mutate Skipped Subtree Order Test", run_prolly_mutate_preserves_order_across_skipped_subtrees },
  { "prolly_mutate_blob_right_edge_append", "Prolly Mutate Blob Right Edge Append Test", run_prolly_mutate_appends_blob_key_to_right_edge },
  { "prolly_mutate_batch_int_replace", "Prolly Mutate Batch Int Replacement Test", run_prolly_mutate_batches_existing_int_replacements },
  { "refs_hash_rollback_restore", "Chunk Store Rollback Restores Refs Hash Test", run_chunk_store_rollback_restores_refs_hash },
  { "refs_hash_commit_failure_restore", "Chunk Store Commit Failure Restores Refs Hash Test", run_chunk_store_commit_failure_restores_refs_hash },
  { "chunk_store_full_pathname", "Chunk Store Uses VFS Full Pathname Test", run_chunk_store_uses_vfs_full_pathname },
  { "remotesrv_put_refs_failure_restore", "RemoteSrv Put Refs Failure Restores State Test", run_remotesrv_put_refs_failure_restores_state },
  { "remotesrv_plain_refs_refreshes_under_lock", "RemoteSrv Plain Refs Refreshes Under Lock Test", run_remotesrv_plain_refs_refreshes_under_lock },
  { "remotesrv_stale_size_heuristic_keeps_peer_branch", "RemoteSrv Stale Size Heuristic Keeps Peer Branch Test", run_remotesrv_stale_size_heuristic_keeps_peer_branch },
  { "file_remote_stale_size_heuristic_keeps_peer_branch", "File Remote Stale Size Heuristic Keeps Peer Branch Test", run_file_remote_stale_size_heuristic_keeps_peer_branch },
  { "remotesrv_chunk_commit_failure_clears_pending", "RemoteSrv Chunk Commit Failure Clears Pending Test", run_remotesrv_chunk_commit_failure_clears_pending },
  { "prolly_int_cursor_boundary", "Prolly Int Cursor Internal Boundary Test", run_prolly_int_cursor_seek_across_internal_boundary },
  { "prolly_int_cursor_seek_past_max", "Prolly Int Cursor Seek Past Max Test", run_prolly_int_cursor_seek_past_max },
  { "prolly_blob_cursor_boundary", "Prolly Blob Cursor Internal Boundary Test", run_prolly_blob_cursor_seek_across_internal_boundary },
  { "prolly_blob_cursor_seek_past_max", "Prolly Blob Cursor Seek Past Max Test", run_prolly_blob_cursor_seek_past_max },
  { "prolly_cursor_empty_leaf_root", "Prolly Cursor Empty Leaf Root Test", run_prolly_cursor_empty_leaf_root },
  { "prolly_cursor_corrupt_node", "Prolly Cursor Corrupt Node Test", run_prolly_cursor_surfaces_corrupt_node },
  { "prolly_cursor_empty_leaf_under_internal", "Prolly Cursor Empty Leaf Under Internal Test", run_prolly_cursor_empty_leaf_under_internal },
  { "prolly_node_first_offset_validation", "Prolly Node First Offset Validation Test", run_prolly_node_first_offset_validation },
  { "prolly_diff_iter_copies_blob_keys", "Prolly Diff Iterator Blob Key Copy Test", run_prolly_diff_iter_copies_blob_keys },
  { "prolly_diff_leaf_record_corruption", "Prolly Diff Leaf Corruption Test", run_prolly_diff_leaf_surfaces_record_corruption },
  { "incrblob_legacy_engine_write", "Incrblob Legacy Engine Write Test", run_incrblob_legacy_engine_write },
  { "incrblob_chunked_and_multihandle", "Incrblob Chunked Record And Multi-Handle Test", run_incrblob_chunked_and_multihandle },
  { "rowid_named_pk_keeps_rowid_table", "Rowid-Named PK Keeps Rowid Table Test", run_rowid_named_pk_keeps_rowid_table },
  { "diff_side_schema_custom_function", "Diff Side Schema Custom Function Test", run_diff_side_schema_custom_function },
  { "rollback_persist_failure_ends_txn", "Rollback Persist Failure Ends Write Txn Test", run_rollback_persist_failure_ends_txn },
  { "blob_restore_mutmap_keeps_scan", "Blob Restore MutMap Keeps Scan Test", run_blob_restore_mutmap_keeps_scan },
  { "intpk_scan_delete_keeps_scan", "INT PK Scan Delete Keeps Scan Test", run_intpk_scan_delete_keeps_scan },
  { "count_flush_keeps_scan", "Count Flush Keeps Scan Test", run_count_flush_keeps_scan },
  { "negzero_sortkey_eq", "Negzero Sortkey Eq Test", run_negzero_sortkey_eq }
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

static int case_is_excluded(const char *zName, const char *zList){
  int nName = (int)strlen(zName);
  const char *z = zList;
  while( z && *z ){
    while( *z==',' ) z++;
    if( strncmp(z, zName, nName)==0 && (z[nName]==0 || z[nName]==',') ){
      return 1;
    }
    z = strchr(z, ',');
  }
  return 0;
}

static void print_usage(const char *argv0){
  int i;
  fprintf(stderr, "Usage: %s all|all_except=name[,name...]|", argv0);
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
  }else if( strncmp(argv[1], "all_except=", 11)==0 ){
    const char *zExclude = argv[1] + 11;
    printf("=== DoltLite Regression Tests ===\n\n");
    for(i=0; i<(int)(sizeof(aCases)/sizeof(aCases[0])); i++){
      if( case_is_excluded(aCases[i].zName, zExclude) ){
        printf("=== %s ===\n\nSKIP: excluded by command line\n",
               aCases[i].zTitle);
      }else{
        aCases[i].xRun();
      }
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
