#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "sqlite3.h"
#include "doltlite_internal.h"

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

static int exec(sqlite3 *db, const char *zSql){
  char *zErr = 0;
  int rc = sqlite3_exec(db, zSql, 0, 0, &zErr);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "SQL rc=%d %s [%s]\n", rc, zErr ? zErr : "", zSql);
  }
  sqlite3_free(zErr);
  return rc;
}

static int execQuiet(sqlite3 *db, const char *zSql, char **pzErr){
  return sqlite3_exec(db, zSql, 0, 0, pzErr);
}

static int queryInt(sqlite3 *db, const char *zSql, int *pOut){
  sqlite3_stmt *p = 0;
  int rc = sqlite3_prepare_v2(db, zSql, -1, &p, 0);
  if( rc!=SQLITE_OK ) return rc;
  rc = sqlite3_step(p);
  if( rc==SQLITE_ROW ){
    *pOut = sqlite3_column_int(p, 0);
    sqlite3_finalize(p);
    return SQLITE_OK;
  }
  sqlite3_finalize(p);
  return rc==SQLITE_DONE ? SQLITE_ERROR : sqlite3_errcode(db);
}

static char *queryText(sqlite3 *db, const char *zSql){
  sqlite3_stmt *p = 0;
  char *z = 0;
  if( sqlite3_prepare_v2(db, zSql, -1, &p, 0)!=SQLITE_OK ) return 0;
  if( sqlite3_step(p)==SQLITE_ROW && sqlite3_column_text(p, 0) ){
    z = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(p, 0));
  }
  sqlite3_finalize(p);
  return z;
}

static sqlite3 *openDb(const char *zPath){
  sqlite3 *db = 0;
  remove(zPath);
  if( sqlite3_open(zPath, &db)!=SQLITE_OK ) return 0;
  sqlite3_busy_timeout(db, 5000);
  return db;
}

static void closeRm(sqlite3 *db, const char *zPath){
  sqlite3_close(db);
  remove(zPath);
}

static int setupUniqueCv(sqlite3 *db){
  return exec(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, u INT UNIQUE, v TEXT);"
    "INSERT INTO t VALUES(1,1,'base1'),(2,2,'base2');"
    "SELECT dolt_commit('-Am','init');"
    "SELECT dolt_branch('feat');"
    "SELECT dolt_checkout('feat');"
    "UPDATE t SET u=9, v='feat2' WHERE id=2;"
    "SELECT dolt_commit('-Am','feat');"
    "SELECT dolt_checkout('main');"
    "UPDATE t SET u=9, v='main1' WHERE id=1;"
    "SELECT dolt_commit('-Am','main');");
}

static int setupTwoTableConflicts(sqlite3 *db){
  return exec(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'base');"
    "INSERT INTO u VALUES(1,'base');"
    "SELECT dolt_commit('-Am','init');"
    "SELECT dolt_branch('feat');"
    "SELECT dolt_checkout('feat');"
    "UPDATE t SET v='feat' WHERE id=1;"
    "UPDATE u SET v='feat' WHERE id=1;"
    "SELECT dolt_commit('-Am','feat');"
    "SELECT dolt_checkout('main');"
    "UPDATE t SET v='main' WHERE id=1;"
    "UPDATE u SET v='main' WHERE id=1;"
    "SELECT dolt_commit('-Am','main');");
}

static int setupSchemaMerge(sqlite3 *db){
  return exec(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-Am','base');"
    "ANALYZE;"
    "SELECT dolt_branch('feat');"
    "SELECT dolt_checkout('feat');"
    "ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'x';"
    "SELECT dolt_commit('-Am','feat col');"
    "SELECT dolt_checkout('main');"
    "INSERT INTO t VALUES(2,'b');"
    "SELECT dolt_commit('-Am','main row');");
}

/* --force commit must not advance HEAD if clearing CVs fails to persist. */
static void test_force_commit_checks_cv_clear(void){
  char zPath[256];
  sqlite3 *db;
  char *zHeadBefore = 0, *zHeadAfter = 0, *zErr = 0;
  int nCv = -1, rc;

  snprintf(zPath, sizeof(zPath), "/tmp/cas_swallow_cv_%d.db", (int)getpid());
  db = openDb(zPath);
  check("cv: open", db!=0);
  if( !db ) return;
  check("cv: setup", setupUniqueCv(db)==SQLITE_OK);
  check("cv: begin", exec(db, "BEGIN")==SQLITE_OK);
  rc = execQuiet(db, "SELECT dolt_merge('feat')", &zErr);
  sqlite3_free(zErr);
  zErr = 0;
  check("cv: merge in txn", rc!=SQLITE_OK);
  check("cv: violations present",
        queryInt(db, "SELECT count(*) FROM dolt_constraint_violations", &nCv)
          ==SQLITE_OK && nCv>0);
  zHeadBefore = queryText(db, "SELECT dolt_hashof('HEAD')");
  doltliteTestFailPersistAtCall(1);
  rc = execQuiet(db, "SELECT dolt_commit('--force','-Am','forced')", &zErr);
  check("cv: force commit fails", rc!=SQLITE_OK);
  zHeadAfter = queryText(db, "SELECT dolt_hashof('HEAD')");
  check("cv: HEAD unchanged",
        zHeadBefore && zHeadAfter && strcmp(zHeadBefore, zHeadAfter)==0);
  sqlite3_free(zHeadBefore);
  sqlite3_free(zHeadAfter);
  sqlite3_free(zErr);
  sqlite3_exec(db, "ROLLBACK", 0, 0, 0);
  closeRm(db, zPath);
}

/* Restore failure after a failed merge-commit create must surface that
** code, not a generic "failed to create merge commit". */
static void test_merge_restore_not_voided(void){
  char zPath[256];
  sqlite3 *db;
  char *zErr = 0;
  int rc;

  snprintf(zPath, sizeof(zPath), "/tmp/cas_swallow_restore_%d.db", (int)getpid());
  db = openDb(zPath);
  check("restore: open", db!=0);
  if( !db ) return;
  check("restore: setup",
        exec(db,
          "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
          "INSERT INTO t VALUES(1,'a');"
          "SELECT dolt_commit('-Am','base');"
          "SELECT dolt_branch('feat');"
          "SELECT dolt_checkout('feat');"
          "INSERT INTO t VALUES(2,'feat');"
          "SELECT dolt_commit('-Am','feat row');"
          "SELECT dolt_checkout('main');"
          "INSERT INTO t VALUES(3,'main');"
          "SELECT dolt_commit('-Am','main row');")==SQLITE_OK);
  doltliteTestClearFaults();
  doltliteTestFailNextCreateCommit();
  doltliteTestFailNextRestore();
  rc = execQuiet(db, "SELECT dolt_merge('--no-ff','feat')", &zErr);
  check("restore: merge fails", rc!=SQLITE_OK);
  check("restore: reports restore IOERR, not generic create failure",
        sqlite3_extended_errcode(db)==SQLITE_IOERR
        && (!zErr || strstr(zErr, "failed to create merge commit")==0));
  sqlite3_free(zErr);
  closeRm(db, zPath);
}

/* Multi-table resolve must not keep table t resolved if table u fails. */
static void test_conflicts_resolve_savepoint(void){
  char zPath[256];
  sqlite3 *db;
  char *zErr = 0;
  int nT = -1, nU = -1, nAll = -1, rc;

  snprintf(zPath, sizeof(zPath), "/tmp/cas_swallow_resolve_%d.db", (int)getpid());
  db = openDb(zPath);
  check("resolve: open", db!=0);
  if( !db ) return;
  check("resolve: setup", setupTwoTableConflicts(db)==SQLITE_OK);
  check("resolve: begin", exec(db, "BEGIN")==SQLITE_OK);
  rc = execQuiet(db, "SELECT dolt_merge('feat')", &zErr);
  sqlite3_free(zErr);
  zErr = 0;
  check("resolve: merge conflicts", rc!=SQLITE_OK);
  check("resolve: two conflict tables",
        queryInt(db, "SELECT count(*) FROM dolt_conflicts", &nAll)==SQLITE_OK
        && nAll>=2);
  doltliteTestFailPersistAtCall(2);
  rc = execQuiet(db, "SELECT dolt_conflicts_resolve('--ours','t','u')", &zErr);
  check("resolve: second table persist fails", rc!=SQLITE_OK);
  check("resolve: t still conflicted",
        queryInt(db, "SELECT count(*) FROM dolt_conflicts_t", &nT)==SQLITE_OK
        && nT>0);
  check("resolve: u still conflicted",
        queryInt(db, "SELECT count(*) FROM dolt_conflicts_u", &nU)==SQLITE_OK
        && nU>0);
  sqlite3_free(zErr);
  sqlite3_exec(db, "ROLLBACK", 0, 0, 0);
  closeRm(db, zPath);
}

/* Branch move must not delete the source if the working-set read fails. */
static void test_branch_move_ws_read_failure(void){
  char zPath[256];
  sqlite3 *db;
  char *zErr = 0;
  int nFeat = -1, nRenamed = -1, rc;

  snprintf(zPath, sizeof(zPath), "/tmp/cas_swallow_move_%d.db", (int)getpid());
  db = openDb(zPath);
  check("move: open", db!=0);
  if( !db ) return;
  check("move: setup",
        exec(db,
          "CREATE TABLE t(id INTEGER PRIMARY KEY);"
          "INSERT INTO t VALUES(1);"
          "SELECT dolt_commit('-Am','init');"
          "SELECT dolt_branch('feat');")==SQLITE_OK);
  doltliteTestFailNextGetBranchWorkingSet();
  rc = execQuiet(db, "SELECT dolt_branch('-m','feat','renamed')", &zErr);
  check("move: fails", rc!=SQLITE_OK);
  check("move: source remains",
        queryInt(db, "SELECT count(*) FROM dolt_branches WHERE name='feat'",
                 &nFeat)==SQLITE_OK && nFeat==1);
  check("move: dest not created",
        queryInt(db, "SELECT count(*) FROM dolt_branches WHERE name='renamed'",
                 &nRenamed)==SQLITE_OK && nRenamed==0);
  sqlite3_free(zErr);
  closeRm(db, zPath);
}

typedef struct PeerCommit PeerCommit;
struct PeerCommit {
  sqlite3 *db;
  int rc;
};

static void peerCommitDuringInstall(void *pArg){
  PeerCommit *p = (PeerCommit*)pArg;
  sqlite3_busy_timeout(p->db, 0);
  p->rc = sqlite3_exec(p->db, "INSERT INTO t(id,v) VALUES(99,'peer')", 0, 0, 0);
  if( p->rc==SQLITE_OK ){
    p->rc = sqlite3_exec(p->db, "SELECT dolt_commit('-Am','peer')", 0, 0, 0);
  }
}

/* Merge install must not hold the graph lock across ALTER/ANALYZE: a peer
** commit in that window succeeds, and the merge then returns BUSY. */
static void test_merge_install_unlock_window(void){
  char zPath[256];
  sqlite3 *db1, *db2 = 0;
  char *zErr = 0, *zHead = 0, *zPeer = 0;
  PeerCommit peer;
  int rc;

  snprintf(zPath, sizeof(zPath), "/tmp/cas_swallow_lock_%d.db", (int)getpid());
  db1 = openDb(zPath);
  check("lock: open", db1!=0);
  if( !db1 ) return;
  doltliteTestClearFaults();
  check("lock: setup", setupSchemaMerge(db1)==SQLITE_OK);
  check("lock: peer open", sqlite3_open(zPath, &db2)==SQLITE_OK);
  sqlite3_busy_timeout(db2, 0);
  peer.db = db2;
  peer.rc = SQLITE_ERROR;
  doltliteTestSetMergeInstallHook(peerCommitDuringInstall, &peer);
  rc = execQuiet(db1, "SELECT dolt_merge('--no-ff','feat')", &zErr);
  doltliteTestSetMergeInstallHook(0, 0);
  check("lock: peer commit not blocked", peer.rc==SQLITE_OK);
  check("lock: merge yields BUSY, not a clobber",
        rc!=SQLITE_OK && zErr && strstr(zErr, "another connection")!=0);
  zHead = queryText(db2, "SELECT dolt_hashof('HEAD')");
  zPeer = queryText(db2, "SELECT commit_hash FROM dolt_log WHERE message='peer'");
  check("lock: HEAD is the peer commit",
        zHead && zPeer && strcmp(zHead, zPeer)==0);
  sqlite3_free(zHead);
  sqlite3_free(zPeer);
  sqlite3_free(zErr);
  sqlite3_close(db2);
  closeRm(db1, zPath);
}

int main(void){
  sqlite3_initialize();
  test_force_commit_checks_cv_clear();
  test_merge_restore_not_voided();
  test_conflicts_resolve_savepoint();
  test_branch_move_ws_read_failure();
  test_merge_install_unlock_window();
  printf("vc_cas_swallow_test: %d passed, %d failed\n", nPass, nFail);
  return nFail ? 1 : 0;
}
