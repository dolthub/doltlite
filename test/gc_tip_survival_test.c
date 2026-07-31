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

int main(void){
  printf("=== GC Tip Survival Contract Tests ===\n\n");

  test_tip_survives_gc();
  test_stale_tip_survives_peer_advance_and_gc();
  test_tip_survives_vacuum();
  test_branch_tips_across_gc();

  printf("\ngc_tip_survival_test: %d passed, %d failed\n", nPass, nFail);
  return nFail ? 1 : 0;
}
