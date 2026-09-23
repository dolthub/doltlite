#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include "sqlite3.h"
#include "chunk_store.h"
#include "doltlite_commit.h"

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

static const char *queryScalarText(sqlite3 *db, const char *sql){
  static char zBuf[256];
  sqlite3_stmt *pStmt = 0;
  zBuf[0] = 0;
  if( sqlite3_prepare_v2(db, sql, -1, &pStmt, 0)==SQLITE_OK ){
    if( sqlite3_step(pStmt)==SQLITE_ROW ){
      const unsigned char *z = sqlite3_column_text(pStmt, 0);
      if( z ) sqlite3_snprintf(sizeof(zBuf), zBuf, "%s", (const char*)z);
    }
  }
  sqlite3_finalize(pStmt);
  return zBuf;
}

static int execSql(sqlite3 *db, const char *sql){
  return sqlite3_exec(db, sql, 0, 0, 0);
}

static void removeDbFiles(const char *path){
  char zSide[512];
  unlink(path);
  sqlite3_snprintf(sizeof(zSide), zSide, ".%s-lock", path);
  unlink(zSide);
}

static int captureTip(sqlite3 *db, const char *zBranch, ProllyHash *pTip){
  char zSql[256];
  const char *zHex;
  sqlite3_snprintf(sizeof(zSql), zSql,
      "SELECT hash FROM dolt_branches WHERE name='%s'", zBranch);
  zHex = queryScalarText(db, zSql);
  if( strlen(zHex)!=40 ) return SQLITE_ERROR;
  return doltliteHexToHash(zHex, pTip);
}

static int captureWorkingSet(const char *path, const char *zBranch,
                             ProllyHash *pWs){
  ChunkStore cs;
  int rc = chunkStoreOpen(&cs, sqlite3_vfs_find(0), path,
                          SQLITE_OPEN_READONLY | SQLITE_OPEN_MAIN_DB);
  if( rc!=SQLITE_OK ) return rc;
  rc = chunkStoreGetBranchWorkingSet(&cs, zBranch, pWs);
  chunkStoreClose(&cs);
  return rc;
}

static int storeHasChunk(const char *path, const ProllyHash *pHash, int *pHas){
  ChunkStore cs;
  int rc = chunkStoreOpen(&cs, sqlite3_vfs_find(0), path,
                          SQLITE_OPEN_READONLY | SQLITE_OPEN_MAIN_DB);
  *pHas = 0;
  if( rc!=SQLITE_OK ) return rc;
  rc = chunkStoreHas(&cs, pHash, pHas);
  chunkStoreClose(&cs);
  return rc;
}

/* The moved-file ownership check refuses to adopt a file at this handle's path
** unless the file holds one of the handle's branch tips. That is only sound if
** GC preserves every branch tip -- including a tip the handle remembers from an
** arbitrarily stale view, which survives as an ancestor of whatever the branch
** has since become. These tests pin that contract at the chunk level so a
** change to GC's root set fails here instead of silently breaking moved-file
** correctness. */
static void test_tip_survives_gc(void){
  const char *path = "test_gc_tip_basic.db";
  sqlite3 *db = 0;
  ProllyHash seedTip, tip1;
  int has = 0;

  printf("--- Tip survival: seed and committed tips across dolt_gc ---\n");
  removeDbFiles(path);

  sqlite3_open(path, &db);
  check("basic_seed_tip_captured", captureTip(db, "main", &seedTip)==SQLITE_OK);
  check("basic_gc_on_seed_only",
        strstr(queryScalarText(db, "SELECT dolt_gc()"), "error")==0);
  sqlite3_close(db);
  check("basic_seed_tip_survives",
        storeHasChunk(path, &seedTip, &has)==SQLITE_OK && has);

  sqlite3_open(path, &db);
  check("basic_setup", execSql(db,
      "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
      "INSERT INTO t VALUES(1,'a');")==SQLITE_OK);
  check("basic_commit",
        strlen(queryScalarText(db, "SELECT dolt_commit('-A','-m','c1')"))==40);
  check("basic_tip_captured", captureTip(db, "main", &tip1)==SQLITE_OK);
  check("basic_gc",
        strstr(queryScalarText(db, "SELECT dolt_gc()"), "error")==0);
  sqlite3_close(db);
  check("basic_tip_survives",
        storeHasChunk(path, &tip1, &has)==SQLITE_OK && has);
  check("basic_seed_still_reachable_as_ancestor",
        storeHasChunk(path, &seedTip, &has)==SQLITE_OK && has);

  removeDbFiles(path);
}

static void test_stale_tip_survives_peer_advance_and_gc(void){
  const char *path = "test_gc_tip_stale.db";
  sqlite3 *db = 0;
  ProllyHash staleTip, staleWs;
  int has = 0;
  int i;

  printf("--- Tip survival: stale tip across peer advance + gc; working set does not ---\n");
  removeDbFiles(path);

  sqlite3_open(path, &db);
  check("stale_setup", execSql(db,
      "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
      "INSERT INTO t VALUES(1,'a');")==SQLITE_OK);
  check("stale_commit",
        strlen(queryScalarText(db, "SELECT dolt_commit('-A','-m','old')"))==40);
  check("stale_tip_captured", captureTip(db, "main", &staleTip)==SQLITE_OK);
  sqlite3_close(db);
  check("stale_ws_captured",
        captureWorkingSet(path, "main", &staleWs)==SQLITE_OK);

  for(i=0; i<3; i++){
    char zSql[128];
    sqlite3_open(path, &db);
    sqlite3_snprintf(sizeof(zSql), zSql,
        "INSERT INTO t VALUES(%d+10, 'p%d');", i, i);
    check("stale_peer_write", execSql(db, zSql)==SQLITE_OK);
    check("stale_peer_commit",
          strlen(queryScalarText(db, "SELECT dolt_commit('-A','-m','adv')"))==40);
    check("stale_peer_gc",
          strstr(queryScalarText(db, "SELECT dolt_gc()"), "error")==0);
    sqlite3_close(db);
  }

  check("stale_tip_survives_repeated_gc",
        storeHasChunk(path, &staleTip, &has)==SQLITE_OK && has);
  /* The negative space: the stale working-set root is legitimately swept once a
  ** peer advances, which is why the ownership check must not probe it. */
  check("stale_working_set_swept",
        storeHasChunk(path, &staleWs, &has)==SQLITE_OK && !has);

  removeDbFiles(path);
}

static void test_tip_survives_vacuum(void){
  const char *path = "test_gc_tip_vacuum.db";
  sqlite3 *db = 0;
  ProllyHash tip;
  int has = 0;

  printf("--- Tip survival: VACUUM is gc ---\n");
  removeDbFiles(path);

  sqlite3_open(path, &db);
  check("vacuum_setup", execSql(db,
      "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
      "INSERT INTO t VALUES(1,'a');")==SQLITE_OK);
  check("vacuum_commit",
        strlen(queryScalarText(db, "SELECT dolt_commit('-A','-m','c1')"))==40);
  check("vacuum_tip_captured", captureTip(db, "main", &tip)==SQLITE_OK);
  check("vacuum_more_writes", execSql(db,
      "INSERT INTO t VALUES(2,'b'); DELETE FROM t WHERE id=2;")==SQLITE_OK);
  check("vacuum_runs", execSql(db, "VACUUM;")==SQLITE_OK);
  sqlite3_close(db);
  check("vacuum_tip_survives",
        storeHasChunk(path, &tip, &has)==SQLITE_OK && has);

  removeDbFiles(path);
}

static void test_branch_tips_across_gc(void){
  const char *path = "test_gc_tip_branches.db";
  sqlite3 *db = 0;
  ProllyHash featTip;
  int has = 0;

  printf("--- Tip survival: side branch kept; deleted branch's unique tip collected ---\n");
  removeDbFiles(path);

  sqlite3_open(path, &db);
  check("branch_setup", execSql(db,
      "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
      "INSERT INTO t VALUES(1,'a');")==SQLITE_OK);
  check("branch_base_commit",
        strlen(queryScalarText(db, "SELECT dolt_commit('-A','-m','base')"))==40);
  check("branch_create",
        strstr(queryScalarText(db, "SELECT dolt_branch('feat')"), "error")==0);
  check("branch_checkout",
        strstr(queryScalarText(db, "SELECT dolt_checkout('feat')"), "error")==0);
  check("branch_feat_write",
        execSql(db, "INSERT INTO t VALUES(2,'feat');")==SQLITE_OK);
  check("branch_feat_commit",
        strlen(queryScalarText(db, "SELECT dolt_commit('-A','-m','feat')"))==40);
  check("branch_feat_tip_captured", captureTip(db, "feat", &featTip)==SQLITE_OK);
  check("branch_back_to_main",
        strstr(queryScalarText(db, "SELECT dolt_checkout('main')"), "error")==0);
  check("branch_main_advance", execSql(db,
      "INSERT INTO t VALUES(3,'main');")==SQLITE_OK);
  check("branch_main_commit",
        strlen(queryScalarText(db, "SELECT dolt_commit('-A','-m','main2')"))==40);
  check("branch_gc_keeps_side_branch",
        strstr(queryScalarText(db, "SELECT dolt_gc()"), "error")==0);
  sqlite3_close(db);
  check("branch_feat_tip_survives",
        storeHasChunk(path, &featTip, &has)==SQLITE_OK && has);

  /* Deleting feat makes its unique tip unreachable; gc collects it. A handle
  ** that only remembers deleted branches then fails the ownership proof and
  ** goes read-only, which is the intended behavior for a database that has
  ** genuinely moved on without it. */
  sqlite3_open(path, &db);
  check("branch_delete",
        strstr(queryScalarText(db, "SELECT dolt_branch('-D','feat')"), "error")==0);
  check("branch_gc_after_delete",
        strstr(queryScalarText(db, "SELECT dolt_gc()"), "error")==0);
  sqlite3_close(db);
  check("branch_deleted_tip_collected",
        storeHasChunk(path, &featTip, &has)==SQLITE_OK && !has);

  removeDbFiles(path);
}

static int seedAllocationFixture(
  const char *path, ProllyHash *pDead, int nLive
){
  sqlite3 *db = 0;
  ChunkStore cs;
  int i, wrote = 0;
  int rc;

  removeDbFiles(path);
  rc = sqlite3_open(path, &db);
  if( rc==SQLITE_OK ){
    rc = execSql(db,
        "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
        "INSERT INTO t VALUES(1,'kept');"
        "SELECT dolt_commit('-Am','kept');");
  }
  sqlite3_close(db);
  if( rc!=SQLITE_OK ) return rc;
  rc = chunkStoreOpen(&cs, sqlite3_vfs_find(0), path,
                      SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB);
  if( rc!=SQLITE_OK ) return rc;
  rc = chunkStoreLockAndRefresh(&cs);
  if( rc==SQLITE_OK && nLive>0 ){
    DoltliteCommit commit;
    ProllyHash tip;
    u8 *data = 0;
    int nData = 0;
    ProllyHash *aTips = sqlite3_malloc64((sqlite3_uint64)nLive*sizeof(ProllyHash));
    memset(&commit, 0, sizeof(commit));
    rc = aTips ? chunkStoreFindBranch(&cs, "main", &tip) : SQLITE_NOMEM;
    if( rc==SQLITE_OK ) rc = chunkStoreGet(&cs, &tip, &data, &nData);
    if( rc==SQLITE_OK ) rc = doltliteCommitDeserialize(data, nData, &commit);
    sqlite3_free(data);
    for(i=nLive-1; rc==SQLITE_OK && i>=0; i--){
      int j;
      data = 0;
      commit.nParents = 0;
      commit.timestamp = i;
      for(j=8*i+1; j<nLive && j<=8*i+8; j++){
        commit.aParents[commit.nParents++] = aTips[j];
      }
      if( commit.nParents==0 ) commit.aParents[commit.nParents++] = tip;
      rc = doltliteCommitSerialize(&commit, &data, &nData);
      if( rc==SQLITE_OK ) rc = chunkStorePut(&cs, data, nData, &aTips[i]);
      sqlite3_free(data);
    }
    doltliteCommitClear(&commit);
    if( rc==SQLITE_OK ) rc = chunkStoreUpdateBranch(&cs, "main", &aTips[0]);
    sqlite3_free(aTips);
    if( rc==SQLITE_OK ) rc = chunkStoreSerializeRefs(&cs);
  }
  for(i=0; rc==SQLITE_OK && i<40000; i++){
    u8 data[4];
    data[0] = (u8)i;
    data[1] = (u8)(i>>8);
    data[2] = (u8)(i>>16);
    data[3] = (u8)(i>>24);
    rc = chunkStorePut(&cs, data, sizeof(data), pDead);
  }
  if( rc==SQLITE_OK ) rc = chunkStoreCommit(&cs);
  if( rc==SQLITE_OK ) rc = csWriteWalCheckpoint(&cs, 1, &wrote);
  if( rc==SQLITE_OK && !wrote && cs.wal.checkpointMagic==0 ) rc = SQLITE_ERROR;
  chunkStoreUnlock(&cs);
  chunkStoreClose(&cs);
  return rc;
}

static void test_gc_allocation_budget(void){
  const char *path = "test_gc_allocation.db";
  const char *copy = "test_gc_allocation_copy.db";
  const char *aSql[] = {
    "VACUUM", "SELECT dolt_gc()", "VACUUM INTO 'test_gc_allocation_copy.db'"
  };
  ProllyHash dead;
  ChunkStore cs;
  sqlite3 *db = 0;
  sqlite3_int64 oldLimit;
  int rc, i, has;

  rc = seedAllocationFixture(path, &dead, 0);
  check("allocation_seed_lazy_index", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto allocation_done;
  rc = chunkStoreOpen(&cs, sqlite3_vfs_find(0), path,
                      SQLITE_OPEN_READONLY | SQLITE_OPEN_MAIN_DB);
  check("allocation_open_lazy_index", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto allocation_done;
  check("allocation_index_has_no_overlay",
        cs.index.lazy.active && cs.index.lazy.nEntries>40000 && cs.index.nIndex==0);
  oldLimit = sqlite3_hard_heap_limit64(sqlite3_memory_used()+2*1024*1024);
  rc = csMaterializeIndex(&cs);
  sqlite3_hard_heap_limit64(oldLimit);
  check("allocation_materialize_with_2mib_headroom", rc==SQLITE_OK);
  rc = chunkStoreHas(&cs, &dead, &has);
  check("allocation_materialize_keeps_index_entries", rc==SQLITE_OK && has);
  chunkStoreClose(&cs);

  for(i=0; i<3; i++){
    const char *resultPath = i==2 ? copy : path;
    removeDbFiles(copy);
    rc = seedAllocationFixture(path, &dead, 0);
    check("allocation_gc_seed", rc==SQLITE_OK);
    if( rc!=SQLITE_OK ) goto allocation_done;
    rc = sqlite3_open(path, &db);
    check("allocation_gc_open", rc==SQLITE_OK);
    if( rc!=SQLITE_OK ) goto allocation_done;
    oldLimit = sqlite3_hard_heap_limit64(sqlite3_memory_used()+2*1024*1024);
    rc = execSql(db, aSql[i]);
    sqlite3_hard_heap_limit64(oldLimit);
    if( rc!=SQLITE_OK ) fprintf(stderr, "%s: %s\n", aSql[i], sqlite3_errmsg(db));
    check("allocation_gc_with_2mib_headroom", rc==SQLITE_OK);
    sqlite3_close(db);
    db = 0;
    if( rc!=SQLITE_OK ) continue;
    rc = storeHasChunk(resultPath, &dead, &has);
    check("allocation_gc_collects_unreachable", rc==SQLITE_OK && !has);
    rc = sqlite3_open(resultPath, &db);
    check("allocation_gc_reopen", rc==SQLITE_OK);
    check("allocation_gc_preserves_rows",
          strcmp(queryScalarText(db, "SELECT v FROM t WHERE id=1"), "kept")==0);
    check("allocation_gc_preserves_history",
          strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_log"), "2")==0);
    check("allocation_gc_integrity",
          strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);
    sqlite3_close(db);
    db = 0;
  }

allocation_done:
  if( db ) sqlite3_close(db);
  removeDbFiles(path);
  removeDbFiles(copy);
}

static void test_gc_live_allocation_budget(void){
  const char *path = "test_gc_live_allocation.db";
  const char *copy = "test_gc_live_copy.db";
  const char *aSql[] = {
    "VACUUM", "SELECT dolt_gc()", "VACUUM INTO 'test_gc_live_copy.db'"
  };
  int i, rc, has;
  ProllyHash dead;
  sqlite3 *db = 0;
  sqlite3_int64 oldLimit;

  for(i=0; i<3; i++){
    const char *resultPath = i==2 ? copy : path;
    ChunkStore cs;
    removeDbFiles(copy);
    rc = seedAllocationFixture(path, &dead, 40000);
    check("live_allocation_seed", rc==SQLITE_OK);
    if( rc!=SQLITE_OK ) break;
    rc = sqlite3_open(path, &db);
    check("live_allocation_open", rc==SQLITE_OK);
    if( rc!=SQLITE_OK ) break;
    oldLimit = sqlite3_hard_heap_limit64(sqlite3_memory_used()+5*1024*1024);
    rc = execSql(db, aSql[i]);
    sqlite3_hard_heap_limit64(oldLimit);
    if( rc!=SQLITE_OK ) fprintf(stderr, "%s: %s\n", aSql[i], sqlite3_errmsg(db));
    check("live_allocation_gc_with_5mib_headroom", rc==SQLITE_OK);
    if( rc!=SQLITE_OK ){
      check("live_allocation_failed_gc_preserves_history",
            strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_log"), "40002")==0);
      rc = execSql(db, aSql[i]);
      check("live_allocation_retry", rc==SQLITE_OK);
    }
    sqlite3_close(db);
    db = 0;
    if( rc!=SQLITE_OK ) continue;
    rc = storeHasChunk(resultPath, &dead, &has);
    check("live_allocation_collects_unreachable", rc==SQLITE_OK && !has);
    rc = sqlite3_open(resultPath, &db);
    check("live_allocation_reopen", rc==SQLITE_OK);
    if( rc!=SQLITE_OK ) break;
    check("live_allocation_preserves_rows",
          strcmp(queryScalarText(db, "SELECT v FROM t WHERE id=1"), "kept")==0);
    check("live_allocation_preserves_history",
          strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_log"), "40002")==0);
    check("live_allocation_integrity",
          strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);
    sqlite3_close(db);
    db = 0;
    rc = chunkStoreOpen(&cs, sqlite3_vfs_find(0), resultPath,
                        SQLITE_OPEN_READONLY | SQLITE_OPEN_MAIN_DB);
    check("live_allocation_open_compacted_index", rc==SQLITE_OK);
    if( rc!=SQLITE_OK ) break;
    chunkIndexReplaceEntries(&cs.index, 0, 0);
    oldLimit = sqlite3_hard_heap_limit64(
        sqlite3_memory_used()+256*1024);
    rc = csReadIndex(&cs);
    sqlite3_hard_heap_limit64(oldLimit);
    check("live_allocation_read_index_without_full_copy", rc==SQLITE_OK);
    if( rc==SQLITE_OK ){
      rc = chunkStoreHas(&cs, &dead, &has);
      check("live_allocation_lazy_lookup", rc==SQLITE_OK && !has);
    }
    chunkStoreClose(&cs);
    rc = chunkStoreOpen(&cs, sqlite3_vfs_find(0), resultPath,
                        SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB);
    check("live_allocation_checkpoint_open", rc==SQLITE_OK);
    if( rc==SQLITE_OK ){
      int wrote = 0;
      rc = chunkStoreLockAndRefresh(&cs);
      if( rc==SQLITE_OK ){
        oldLimit = sqlite3_hard_heap_limit64(sqlite3_memory_used()+1024*1024);
        rc = csWriteWalCheckpoint(&cs, 1, &wrote);
        sqlite3_hard_heap_limit64(oldLimit);
      }
      check("live_allocation_checkpoint_with_1mib_headroom", rc==SQLITE_OK && wrote);
      chunkStoreUnlock(&cs);
      chunkStoreClose(&cs);
    }
    rc = sqlite3_open(resultPath, &db);
    check("live_allocation_checkpoint_reopen", rc==SQLITE_OK);
    check("live_allocation_checkpoint_history",
          strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_log"), "40002")==0);
    check("live_allocation_write_after_checkpoint",
          execSql(db, "INSERT INTO t VALUES(2,'after'); SELECT dolt_commit('-Am','after');")==SQLITE_OK);
    sqlite3_close(db);
    db = 0;
  }
  if( db ) sqlite3_close(db);
  removeDbFiles(path);
  removeDbFiles(copy);
}

static void indexTestHash(ProllyHash *h, int i){
  memset(h, 0, sizeof(*h));
  h->data[0] = (u8)(i>>24);
  h->data[1] = (u8)(i>>16);
  h->data[2] = (u8)(i>>8);
  h->data[3] = (u8)i;
}

static void test_index_spool(void){
  ChunkIndexSpool *p = 0;
  ChunkIndexEntry e;
  sqlite3_int64 oldLimit = sqlite3_hard_heap_limit64(sqlite3_memory_used()+512*1024);
  int i, rc = csIndexSpoolInit(sqlite3_vfs_find(0), &p);
  for(i=0; rc==SQLITE_OK && i<80000; i++){
    int key = (int)(((sqlite3_int64)i*7919)%40000);
    indexTestHash(&e.hash, key);
    e.offset = key+CHUNK_MANIFEST_SIZE;
    e.size = key;
    rc = csIndexSpoolAdd(p, &e);
  }
  if( rc==SQLITE_OK ) rc = csIndexSpoolFinish(p);
  check("spool_sort_and_deduplicate_with_512kib", rc==SQLITE_OK);
  if( rc==SQLITE_OK ){
    check("spool_unique_count", csIndexSpoolCount(p)==40000);
    for(i=0; rc==SQLITE_OK && i<40000; i++){
      ProllyHash h;
      indexTestHash(&h, i);
      rc = csIndexSpoolGet(p, i, &e);
      if( rc==SQLITE_OK && (memcmp(&e.hash, &h, sizeof(h))
       || e.size!=i || e.offset!=i+CHUNK_MANIFEST_SIZE) ) rc = SQLITE_ERROR;
    }
    check("spool_sorted_entries", rc==SQLITE_OK);
    for(i=39999; rc==SQLITE_OK && i>=0; i-=97){
      ProllyHash h;
      int found;
      indexTestHash(&h, i);
      rc = csIndexSpoolFind(p, &h, &found);
      if( rc==SQLITE_OK && found!=i ) rc = SQLITE_ERROR;
    }
    check("spool_random_lookup", rc==SQLITE_OK);
  }
  csIndexSpoolFree(p);
  sqlite3_hard_heap_limit64(oldLimit);
}

#define LARGE_INDEX_COUNT (67108864+129)
#define LARGE_INDEX_OFFSET (CHUNK_MANIFEST_SIZE+5)
static int largeIndexRead(sqlite3_file *f, void *buf, int n, sqlite3_int64 off){
  u8 *a = buf;
  int i;
  (void)f;
  if( off<LARGE_INDEX_OFFSET || (off-LARGE_INDEX_OFFSET)%32 || n%32 ){
    return SQLITE_IOERR;
  }
  for(i=0; i<n; i+=32){
    ProllyHash h;
    int key = (int)((off-LARGE_INDEX_OFFSET+i)/32);
    indexTestHash(&h, key);
    memcpy(a+i, h.data, PROLLY_HASH_SIZE);
    CS_WRITE_I64(a+i+20, CHUNK_MANIFEST_SIZE);
    CS_WRITE_U32(a+i+28, 1);
  }
  return SQLITE_OK;
}
static int largeIndexSize(sqlite3_file *f, sqlite3_int64 *size){
  (void)f;
  *size = LARGE_INDEX_OFFSET+(sqlite3_int64)LARGE_INDEX_COUNT*32;
  return SQLITE_OK;
}
static void test_index_over_2gib(void){
  sqlite3_io_methods methods;
  sqlite3_file file;
  ChunkStore cs;
  ChunkIndexEntry e;
  ProllyHash h;
  sqlite3_int64 oldLimit;
  int found = 0, rc;
  memset(&methods, 0, sizeof(methods));
  methods.iVersion = 1;
  methods.xRead = largeIndexRead;
  methods.xFileSize = largeIndexSize;
  file.pMethods = &methods;
  memset(&cs, 0, sizeof(cs));
  cs.file.pFile = &file;
  chunkIndexSetMetadata(&cs.index, LARGE_INDEX_COUNT, LARGE_INDEX_OFFSET,
                       (sqlite3_int64)LARGE_INDEX_COUNT*32);
  oldLimit = sqlite3_hard_heap_limit64(sqlite3_memory_used()+256*1024);
  rc = csReadIndex(&cs);
  check("flat_index_over_2gib_with_256kib", rc==SQLITE_OK);
  if( rc==SQLITE_OK ){
    indexTestHash(&h, LARGE_INDEX_COUNT-1);
    rc = csIndexLookup(&cs, &h, &e, &found);
    check("flat_index_lookup_beyond_2gib", rc==SQLITE_OK && found && e.size==1);
    indexTestHash(&h, LARGE_INDEX_COUNT);
    rc = csIndexLookup(&cs, &h, &e, &found);
    check("flat_index_absent_beyond_2gib", rc==SQLITE_OK && !found);
  }
  chunkIndexReplaceEntries(&cs.index, 0, 0);
  sqlite3_hard_heap_limit64(oldLimit);
}

int main(void){
  printf("=== GC Tip Survival Contract Tests ===\n\n");

  test_tip_survives_gc();
  test_stale_tip_survives_peer_advance_and_gc();
  test_tip_survives_vacuum();
  test_branch_tips_across_gc();
  test_gc_allocation_budget();
  test_gc_live_allocation_budget();
  test_index_spool();
  test_index_over_2gib();

  printf("\ngc_tip_survival_test: %d passed, %d failed\n", nPass, nFail);
  return nFail ? 1 : 0;
}
