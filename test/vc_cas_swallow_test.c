#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "sqlite3.h"
#include "doltlite_internal.h"
#include "lib/test_tmpdir.h"

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
    "WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i<20) "
    "INSERT INTO t SELECT i,'base' FROM s;"
    "INSERT INTO u SELECT * FROM t;"
    "SELECT dolt_commit('-Am','init');"
    "SELECT dolt_branch('feat');"
    "SELECT dolt_checkout('feat');"
    "UPDATE t SET v='feat';"
    "UPDATE u SET v='feat';"
    "SELECT dolt_commit('-Am','feat');"
    "SELECT dolt_checkout('main');"
    "UPDATE t SET v='main';"
    "UPDATE u SET v='main';"
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

/* --force commit advances HEAD and keeps recorded CVs on the working set. */
static void test_force_commit_keeps_cvs(void){
  char zPath[256];
  sqlite3 *db;
  char *zHeadBefore = 0, *zHeadAfter = 0, *zErr = 0;
  int nCv = -1, rc;

  snprintf(zPath, sizeof(zPath), DOLTLITE_TEST_TMPDIR "/cas_swallow_cv_%d.db", (int)getpid());
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
  rc = execQuiet(db, "SELECT dolt_commit('--force','-Am','forced')", &zErr);
  check("cv: force commit succeeds", rc==SQLITE_OK);
  sqlite3_free(zErr);
  zHeadAfter = queryText(db, "SELECT dolt_hashof('HEAD')");
  check("cv: HEAD advanced",
        zHeadBefore && zHeadAfter && strcmp(zHeadBefore, zHeadAfter)!=0);
  check("cv: violations still recorded",
        queryInt(db, "SELECT count(*) FROM dolt_constraint_violations", &nCv)
          ==SQLITE_OK && nCv>0);
  sqlite3_free(zHeadBefore);
  sqlite3_free(zHeadAfter);
  closeRm(db, zPath);
}

/* Restore failure after a failed merge-commit create must surface that
** code, not a generic "failed to create merge commit". */
static void test_merge_restore_not_voided(void){
  char zPath[256];
  sqlite3 *db;
  char *zErr = 0;
  int rc;

  snprintf(zPath, sizeof(zPath), DOLTLITE_TEST_TMPDIR "/cas_swallow_restore_%d.db", (int)getpid());
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

  snprintf(zPath, sizeof(zPath), DOLTLITE_TEST_TMPDIR "/cas_swallow_resolve_%d.db", (int)getpid());
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

typedef struct ResolveInterrupt ResolveInterrupt;
struct ResolveInterrupt {
  sqlite3 *db;
  int tick;
  int count;
  int direct;
};

static int resolveProgress(void *pArg){
  ResolveInterrupt *p = (ResolveInterrupt*)pArg;
  if( ++p->count!=p->tick ) return 0;
  if( p->direct ){
    sqlite3_interrupt(p->db);
    return 0;
  }
  return 1;
}

static void test_conflicts_resolve_interrupt(int nTables, int direct){
  char zPath[256];
  int tick;
  int nInterrupted = 0;
  snprintf(zPath, sizeof(zPath), DOLTLITE_TEST_TMPDIR "/resolve_interrupt_%d.db", (int)getpid());
  for(tick=1; tick<=200; tick++){
    sqlite3 *db = openDb(zPath);
    ResolveInterrupt progress;
    char *zErr = 0;
    int rc, nRows = -1, nConflicts = -1;
    check("resolve interrupt: open", db!=0);
    if( !db ) break;
    check("resolve interrupt: setup", setupTwoTableConflicts(db)==SQLITE_OK);
    check("resolve interrupt: begin", exec(db, "BEGIN")==SQLITE_OK);
    rc = execQuiet(db, "SELECT dolt_merge('feat')", &zErr);
    check("resolve interrupt: merge conflicts", rc!=SQLITE_OK);
    sqlite3_free(zErr);
    zErr = 0;
    progress.db = db;
    progress.tick = tick;
    progress.count = 0;
    progress.direct = direct;
    sqlite3_progress_handler(db, 1, resolveProgress, &progress);
    rc = execQuiet(db, nTables==1
        ? "SELECT dolt_conflicts_resolve('--theirs','t')"
        : "SELECT dolt_conflicts_resolve('--theirs','t','u')", &zErr);
    sqlite3_progress_handler(db, 0, 0, 0);
    check("resolve interrupt: transaction retained", !sqlite3_get_autocommit(db));
    check("resolve_interrupt_no_internal_savepoint", db->pSavepoint==0 && db->nSavepoint==0);
    check("resolve interrupt: masks balanced", db->nVcInterruptMask==0);
    if( rc!=SQLITE_OK ){
      nInterrupted++;
      check("resolve interrupt: precise code", rc==SQLITE_INTERRUPT);
      check("resolve interrupt: message", zErr && strstr(zErr, "interrupt")!=0);
      check("resolve_interrupt_rows_unchanged",
        queryInt(db, "SELECT count(*) FROM t WHERE v='main'", &nRows)==SQLITE_OK && nRows==20);
      check("resolve interrupt: other rows unchanged",
        queryInt(db, "SELECT count(*) FROM u WHERE v='main'", &nRows)==SQLITE_OK && nRows==20);
      check("resolve_interrupt_conflicts_unchanged",
        queryInt(db, "SELECT count(*) FROM dolt_conflicts_t", &nConflicts)==SQLITE_OK && nConflicts==20);
      check("resolve interrupt: other conflicts unchanged",
        queryInt(db, "SELECT count(*) FROM dolt_conflicts_u", &nConflicts)==SQLITE_OK && nConflicts==20);
      check("resolve interrupt: retry succeeds", exec(db,
        "SELECT dolt_conflicts_resolve('--theirs','t','u');")==SQLITE_OK);
    }else{
      check("resolve interrupt: success applies all rows",
        queryInt(db, "SELECT count(*) FROM t WHERE v='feat'", &nRows)==SQLITE_OK && nRows==20);
      check("resolve interrupt: success clears selected conflicts",
        queryInt(db, "SELECT count(*) FROM dolt_conflicts", &nConflicts)==SQLITE_OK
        && nConflicts==2-nTables);
    }
    sqlite3_free(zErr);
    check("resolve interrupt: rollback", exec(db, "ROLLBACK")==SQLITE_OK);
    closeRm(db, zPath);
    if( progress.count<tick ) break;
  }
  check("resolve interrupt: cancellable", nInterrupted>0);
  check("resolve interrupt: sweep terminates", tick<=200);
}

typedef struct ResolveTrace ResolveTrace;
struct ResolveTrace {
  sqlite3 *db;
  const char *zSql;
  int fired;
};

static int resolveTrace(unsigned type, void *pArg, void *pStmt, void *unused){
  ResolveTrace *p = (ResolveTrace*)pArg;
  const char *zSql = sqlite3_sql((sqlite3_stmt*)pStmt);
  (void)unused;
  if( type==SQLITE_TRACE_PROFILE && !p->fired && zSql
   && strcmp(zSql, p->zSql)==0 ){
    p->fired = 1;
    sqlite3_interrupt(p->db);
  }
  return 0;
}

static void test_conflicts_resolve_boundary_interrupt(int releasing){
  char zPath[256];
  sqlite3 *db;
  Savepoint *pCaller;
  ResolveTrace trace;
  char *zErr = 0;
  int rc, nRows = -1, nConflicts = -1;
  snprintf(zPath, sizeof(zPath), DOLTLITE_TEST_TMPDIR "/resolve_boundary_%d.db", (int)getpid());
  db = openDb(zPath);
  check("resolve boundary: open", db!=0);
  if( !db ) return;
  check("resolve boundary: setup", setupTwoTableConflicts(db)==SQLITE_OK);
  check("resolve boundary: begin", exec(db, "BEGIN")==SQLITE_OK);
  rc = execQuiet(db, "SELECT dolt_merge('feat')", &zErr);
  check("resolve boundary: merge conflicts", rc!=SQLITE_OK);
  sqlite3_free(zErr);
  zErr = 0;
  check("resolve boundary: caller savepoint", exec(db,
    "SAVEPOINT doltlite_conflicts_resolve; INSERT INTO t VALUES(100,'caller');")==SQLITE_OK);
  pCaller = db->pSavepoint;
  trace.db = db;
  trace.fired = 0;
  trace.zSql = releasing ? "RELEASE doltlite_conflicts_resolve"
                        : "SAVEPOINT doltlite_conflicts_resolve";
  sqlite3_trace_v2(db, SQLITE_TRACE_PROFILE, resolveTrace, &trace);
  rc = execQuiet(db, "SELECT dolt_conflicts_resolve('--theirs','t')", &zErr);
  sqlite3_trace_v2(db, 0, 0, 0);
  check("resolve boundary: interrupt delivered", trace.fired);
  check("resolve boundary: return matches outcome", rc==(releasing ? SQLITE_OK : SQLITE_INTERRUPT));
  if( !releasing ){
    check("resolve boundary: error message", zErr && strcmp(zErr, "not an error")!=0);
  }
  check("resolve_boundary_caller_savepoint_retained",
        db->pSavepoint==pCaller && db->nSavepoint==1 && !sqlite3_get_autocommit(db));
  check("resolve boundary: masks balanced", db->nVcInterruptMask==0);
  check("resolve boundary: expected rows",
    queryInt(db, releasing ? "SELECT count(*) FROM t WHERE v='feat'"
                           : "SELECT count(*) FROM t WHERE v='main'", &nRows)==SQLITE_OK
    && nRows==20);
  check("resolve boundary: caller row retained",
    queryInt(db, "SELECT count(*) FROM t WHERE id=100 AND v='caller'", &nRows)==SQLITE_OK
    && nRows==1);
  check("resolve boundary: expected conflicts",
    queryInt(db, "SELECT count(*) FROM dolt_conflicts", &nConflicts)==SQLITE_OK
    && nConflicts==(releasing ? 1 : 2));
  sqlite3_free(zErr);
  check("resolve boundary: caller rollback", exec(db,
    "ROLLBACK TO doltlite_conflicts_resolve; RELEASE doltlite_conflicts_resolve;")==SQLITE_OK);
  check("resolve boundary: restored conflicts",
    queryInt(db, "SELECT count(*) FROM dolt_conflicts", &nConflicts)==SQLITE_OK && nConflicts==2);
  check("resolve boundary: rollback", exec(db, "ROLLBACK")==SQLITE_OK);
  closeRm(db, zPath);
}

/* Branch move must not delete the source if the working-set read fails. */
static void test_branch_move_ws_read_failure(void){
  char zPath[256];
  sqlite3 *db;
  char *zErr = 0;
  int nFeat = -1, nRenamed = -1, rc;

  snprintf(zPath, sizeof(zPath), DOLTLITE_TEST_TMPDIR "/cas_swallow_move_%d.db", (int)getpid());
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

/* The merge holds its write transaction across ALTER/ANALYZE: a peer
** commit in that window is refused, the merge lands, and the peer's commit
** lands on top of it once retried. */
static void test_merge_install_unlock_window(void){
  char zPath[256];
  sqlite3 *db1, *db2 = 0;
  char *zErr = 0, *zHead = 0, *zPeer = 0;
  PeerCommit peer;
  int rc;

  snprintf(zPath, sizeof(zPath), DOLTLITE_TEST_TMPDIR "/cas_swallow_lock_%d.db", (int)getpid());
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
  check("lock: peer commit refused", (peer.rc&0xff)==SQLITE_BUSY);
  check("lock: merge lands", rc==SQLITE_OK);
  sqlite3_busy_timeout(db2, 5000);
  check("lock: peer commit after", exec(db2,
    "INSERT INTO t(id,v) VALUES(99,'peer'); SELECT dolt_commit('-Am','peer')")
    ==SQLITE_OK);
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

static void peerWriteDuringInstall(void *pArg){
  PeerCommit *p = (PeerCommit*)pArg;
  p->rc = sqlite3_exec(p->db, "INSERT INTO t(id,v) VALUES(99,'peer')", 0, 0, 0);
}

/* A peer's write in the install window is refused while the merge holds
** its transaction; once the merge lands the peer's write does too, and the
** merging session's next write must not erase it. */
static void test_merge_install_peer_write_kept(void){
  char zPath[256];
  sqlite3 *db1, *db2 = 0;
  char *zErr = 0;
  PeerCommit peer;
  int nPeer = -1, rc;

  snprintf(zPath, sizeof(zPath), DOLTLITE_TEST_TMPDIR "/cas_swallow_peerws_%d.db", (int)getpid());
  db1 = openDb(zPath);
  check("peerws: open", db1!=0);
  if( !db1 ) return;
  doltliteTestClearFaults();
  check("peerws: setup", exec(db1,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-Am','base');"
    "SELECT dolt_branch('feat');"
    "SELECT dolt_checkout('feat');"
    "CREATE INDEX tv ON t(v);"
    "INSERT INTO t VALUES(3,'c');"
    "SELECT dolt_commit('-Am','feat index');"
    "SELECT dolt_checkout('main');"
    "INSERT INTO t VALUES(2,'b');"
    "SELECT dolt_commit('-Am','main row');")==SQLITE_OK);
  check("peerws: peer open", sqlite3_open(zPath, &db2)==SQLITE_OK);
  sqlite3_busy_timeout(db2, 0);
  peer.db = db2;
  peer.rc = SQLITE_ERROR;
  doltliteTestSetMergeInstallHook(peerWriteDuringInstall, &peer);
  rc = execQuiet(db1, "SELECT dolt_merge('--no-ff','feat')", &zErr);
  doltliteTestSetMergeInstallHook(0, 0);
  check("peerws: peer write refused", (peer.rc&0xff)==SQLITE_BUSY);
  check("peerws: merge lands", rc==SQLITE_OK);
  sqlite3_busy_timeout(db2, 5000);
  check("peerws: peer write acknowledged", exec(db2,
    "INSERT INTO t(id,v) VALUES(99,'peer')")==SQLITE_OK);
  check("peerws: follow-up write", exec(db1,
    "INSERT INTO t(id,v) VALUES(50,'after')")==SQLITE_OK);
  check("peerws: peer row kept",
        queryInt(db2, "SELECT count(*) FROM t WHERE id=99", &nPeer)==SQLITE_OK
        && nPeer==1);
  sqlite3_free(zErr);
  sqlite3_close(db2);
  closeRm(db1, zPath);
}

typedef struct RestorePeer RestorePeer;
struct RestorePeer {
  sqlite3 *db;
  sqlite3 *peer;
  int commitPeer;
  int peerRc;
  int readRc;
  int nPeer;
  int refsMoved;
  int committedRefsMoved;
};

static void refreshPeerDuringInstall(void *pArg){
  RestorePeer *p = (RestorePeer*)pArg;
  ChunkStore *cs = doltliteGetChunkStore(p->db);
  ProllyHash before = cs->refs.committedRefsHash;
  p->peerRc = exec(p->peer, "INSERT INTO t VALUES(99,'peer')");
  if( p->peerRc==SQLITE_OK && p->commitPeer ){
    p->peerRc = exec(p->peer, "SELECT dolt_commit('-am','peer')");
  }
  if( p->peerRc==SQLITE_OK ){
    p->peerRc = exec(p->peer, "BEGIN IMMEDIATE");
  }
  p->readRc = queryInt(p->db, "SELECT count(*) FROM t WHERE id=99",
                       &p->nPeer);
  p->refsMoved = prollyHashCompare(&before, &cs->refs.committedRefsHash)!=0;
}

static void test_failed_merge_restore_after_peer_refresh(void){
  int commitPeer, action;
  for(commitPeer=0; commitPeer<2; commitPeer++){
    for(action=0; action<3; action++){
      char zPath[256];
      sqlite3 *db, *peer = 0;
      RestorePeer probe;
      char *zErr = 0, *zRows = 0, *zIntegrity = 0;
      int rc, n = -1;

      snprintf(zPath, sizeof(zPath), DOLTLITE_TEST_TMPDIR
          "/cas_restore_refresh_%d_%d_%d.db", (int)getpid(), commitPeer, action);
      db = openDb(zPath);
      check("restore refresh: open", db!=0);
      if( !db ) continue;
      check("restore refresh: setup", exec(db,
        "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT);"
        "INSERT INTO t VALUES(1,'base');"
        "SELECT dolt_commit('-Am','base');"
        "SELECT dolt_checkout('-b','feat');"
        "INSERT INTO t VALUES(2,'feat');"
        "SELECT dolt_commit('-am','feat');"
        "SELECT dolt_checkout('main');"
        "INSERT INTO t VALUES(3,'main');"
        "SELECT dolt_commit('-am','main');")==SQLITE_OK);
      check("restore refresh: peer open", sqlite3_open(zPath, &peer)==SQLITE_OK);
      sqlite3_busy_timeout(db, 0);
      sqlite3_busy_timeout(peer, 0);
      memset(&probe, 0, sizeof(probe));
      probe.db = db;
      probe.peer = peer;
      probe.commitPeer = commitPeer;
      probe.peerRc = probe.readRc = SQLITE_ERROR;
      doltliteTestSetMergeInstallHook(refreshPeerDuringInstall, &probe);
      rc = execQuiet(db, "SELECT dolt_merge('--no-ff','feat')", &zErr);
      doltliteTestSetMergeInstallHook(0, 0);
      /* The merge holds its transaction through install: the peer is
      ** refused there, the nested read sees only the merge's own snapshot,
      ** and the peer's write lands after the merge. */
      check("restore refresh: peer refused", (probe.peerRc&0xff)==SQLITE_BUSY);
      check("restore refresh: nested read kept snapshot",
            probe.readRc==SQLITE_OK && probe.nPeer==0 && !probe.refsMoved);
      check("restore refresh: merge lands", rc==SQLITE_OK);
      sqlite3_busy_timeout(peer, 5000);
      check("restore refresh: peer acknowledged",
            exec(peer, "INSERT INTO t VALUES(99,'peer')")==SQLITE_OK
            && (!commitPeer
                || exec(peer, "SELECT dolt_commit('-am','peer')")==SQLITE_OK));
      if( action==0 ){
        check("restore refresh: next read sees peer",
              queryInt(db, "SELECT count(*) FROM t WHERE id=99", &n)==SQLITE_OK
              && n==1);
      }else if( action==1 ){
        check("restore refresh: next add", exec(db, "SELECT dolt_add('t')")==SQLITE_OK);
        check("restore refresh: staged peer",
              queryInt(db, "SELECT count(*) FROM dolt_at_t('STAGED') WHERE id=99",
                       &n)==SQLITE_OK && n==1);
      }
      check("restore refresh: next write", exec(db,
        "INSERT INTO t VALUES(4,'after');"
        "SELECT dolt_add('t'); SELECT dolt_commit('-m','after');")==SQLITE_OK);
      sqlite3_close(peer);
      sqlite3_close(db);
      db = 0;
      check("restore refresh: reopen", sqlite3_open(zPath, &db)==SQLITE_OK);
      zRows = queryText(db,
          "SELECT group_concat(id) FROM (SELECT id FROM t ORDER BY id)");
      check("restore refresh: acknowledged rows kept",
            zRows && strcmp(zRows, "1,2,3,4,99")==0);
      zIntegrity = queryText(db, "PRAGMA integrity_check");
      check("restore refresh: integrity", zIntegrity && strcmp(zIntegrity, "ok")==0);
      if( commitPeer ){
        check("restore refresh: peer commit kept", queryInt(db,
            "SELECT count(*) FROM dolt_log WHERE message='peer'", &n)==SQLITE_OK
            && n==1);
      }
      sqlite3_free(zRows);
      sqlite3_free(zIntegrity);
      sqlite3_free(zErr);
      closeRm(db, zPath);
    }
  }
}

static void moveRefsDuringExplicitMerge(void *pArg){
  RestorePeer *p = (RestorePeer*)pArg;
  ChunkStore *cs = doltliteGetChunkStore(p->db);
  ProllyHash before = cs->refs.refsHash;
  ProllyHash committed = cs->refs.committedRefsHash;
  p->peerRc = exec(p->db, "SELECT dolt_tag('during')");
  p->refsMoved = prollyHashCompare(&before, &cs->refs.refsHash)!=0;
  p->committedRefsMoved =
      prollyHashCompare(&committed, &cs->refs.committedRefsHash)!=0;
  doltliteTestFailNextHeadConfirm();
}

static void test_restore_moved_refs_keeps_explicit_work(void){
  int commit;
  for(commit=0; commit<2; commit++){
    char zPath[256];
    sqlite3 *db;
    RestorePeer probe;
    char *zErr = 0, *zRows = 0;
    int rc, n = -1;

    snprintf(zPath, sizeof(zPath), DOLTLITE_TEST_TMPDIR
        "/cas_restore_explicit_%d_%d.db", (int)getpid(), commit);
    db = openDb(zPath);
    check("restore explicit: open", db!=0);
    if( !db ) continue;
    check("restore explicit: setup", exec(db,
      "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT);"
      "CREATE TABLE u(id INTEGER PRIMARY KEY);"
      "INSERT INTO t VALUES(1,'base');"
      "SELECT dolt_commit('-Am','base');"
      "SELECT dolt_checkout('-b','feat');"
      "INSERT INTO t VALUES(2,'feat');"
      "SELECT dolt_commit('-am','feat');"
      "SELECT dolt_checkout('main');"
      "INSERT INTO t VALUES(3,'main');"
      "SELECT dolt_commit('-am','main');"
      "BEGIN; INSERT INTO u VALUES(99);")==SQLITE_OK);
    memset(&probe, 0, sizeof(probe));
    probe.db = db;
    probe.peerRc = SQLITE_ERROR;
    doltliteTestSetMergeInstallHook(moveRefsDuringExplicitMerge, &probe);
    rc = execQuiet(db, "SELECT dolt_merge('--no-ff','feat')", &zErr);
    doltliteTestSetMergeInstallHook(0, 0);
    doltliteTestClearFaults();
    check("restore explicit: refs moved", probe.peerRc==SQLITE_OK && probe.refsMoved);
    check("restore explicit: refs stayed pending", !probe.committedRefsMoved);
    check("restore explicit: busy keeps transaction", rc==SQLITE_BUSY
          && !sqlite3_get_autocommit(db));
    check("restore explicit: local work kept", queryInt(db,
        "SELECT count(*) FROM u WHERE id=99", &n)==SQLITE_OK && n==1);
    check("restore explicit: nested tag discarded", queryInt(db,
        "SELECT count(*) FROM dolt_tags WHERE tag_name='during'", &n)==SQLITE_OK && n==0);
    check("restore explicit: next write", exec(db, "INSERT INTO u VALUES(100)")==SQLITE_OK);
    check("restore explicit: finish", exec(db, commit ? "COMMIT" : "ROLLBACK")==SQLITE_OK);
    sqlite3_close(db);
    db = 0;
    check("restore explicit: reopen", sqlite3_open(zPath, &db)==SQLITE_OK);
    check("restore explicit: local rows durable", queryInt(db,
        "SELECT count(*) FROM u", &n)==SQLITE_OK && n==(commit ? 2 : 0));
    check("restore explicit: nested tag not durable", queryInt(db,
        "SELECT count(*) FROM dolt_tags WHERE tag_name='during'", &n)==SQLITE_OK && n==0);
    zRows = queryText(db, "SELECT group_concat(id) FROM (SELECT id FROM t ORDER BY id)");
    check("restore explicit: merge not applied", zRows && strcmp(zRows, "1,3")==0);
    sqlite3_free(zRows);
    sqlite3_free(zErr);
    closeRm(db, zPath);
  }
}

/* The merge replays feat's columns onto main's schema one ALTER at a time.
** A later ALTER that fails must leave none of them on disk. */
static void test_merge_schema_actions_all_or_nothing(void){
  char zPath[256];
  sqlite3 *db, *db2 = 0;
  char *zErr = 0, *zBefore = 0, *zAfter = 0, *zIntegrity = 0;
  int nIdx = -1, rc;

  snprintf(zPath, sizeof(zPath), DOLTLITE_TEST_TMPDIR "/cas_swallow_schema_%d.db", (int)getpid());
  db = openDb(zPath);
  check("schemaact: open", db!=0);
  if( !db ) return;
  doltliteTestClearFaults();
  check("schemaact: setup", exec(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "CREATE INDEX tv ON t(v);"
    "INSERT INTO t VALUES(1,'a'),(2,'b');"
    "SELECT dolt_commit('-Am','base');"
    "SELECT dolt_branch('feat');"
    "SELECT dolt_checkout('feat');"
    "ALTER TABLE t ADD COLUMN a TEXT;"
    "ALTER TABLE t ADD COLUMN b TEXT;"
    "INSERT INTO t VALUES(3,'c','x','y');"
    "SELECT dolt_commit('-Am','feat cols');"
    "SELECT dolt_checkout('main');"
    "ALTER TABLE t ADD COLUMN m TEXT;"
    "INSERT INTO t VALUES(4,'d','z');"
    "SELECT dolt_commit('-Am','main col');")==SQLITE_OK);
  zBefore = queryText(db, "SELECT sql FROM sqlite_master WHERE name='t'");
  doltliteTestFailMergeInstallStepAt(2);
  rc = execQuiet(db, "SELECT dolt_merge('--no-ff','feat')", &zErr);
  doltliteTestFailMergeInstallStepAt(0);
  check("schemaact: merge fails", rc!=SQLITE_OK);
  sqlite3_free(zErr);
  zErr = 0;
  check("schemaact: peer open", sqlite3_open(zPath, &db2)==SQLITE_OK);
  zAfter = queryText(db2, "SELECT sql FROM sqlite_master WHERE name='t'");
  check("schemaact: no column published",
        zBefore && zAfter && strcmp(zBefore, zAfter)==0);
  zIntegrity = queryText(db2, "PRAGMA integrity_check");
  check("schemaact: integrity ok", zIntegrity && strcmp(zIntegrity, "ok")==0);
  check("schemaact: index read after failed merge",
        queryInt(db2, "SELECT count(*) FROM t WHERE v='d'", &nIdx)==SQLITE_OK
        && nIdx==1);
  sqlite3_close(db2);
  check("schemaact: retry merges",
        execQuiet(db, "SELECT dolt_merge('--no-ff','feat')", &zErr)==SQLITE_OK);
  sqlite3_free(zErr);
  sqlite3_free(zBefore);
  sqlite3_free(zAfter);
  sqlite3_free(zIntegrity);
  closeRm(db, zPath);
}

/* feat's index covers only feat's rows, so the merge rebuilds it after
** replaying feat's column. A failed rebuild must not leave the merged table
** published under that stale index. */
static void test_merge_install_reindex_all_or_nothing(void){
  char zPath[256];
  sqlite3 *db, *db2 = 0;
  char *zErr = 0, *zBefore = 0, *zAfter = 0, *zIntegrity = 0;
  int rc;

  snprintf(zPath, sizeof(zPath), DOLTLITE_TEST_TMPDIR "/cas_swallow_reindex_%d.db", (int)getpid());
  db = openDb(zPath);
  check("reindex: open", db!=0);
  if( !db ) return;
  doltliteTestClearFaults();
  check("reindex: setup", exec(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a'),(2,'b');"
    "SELECT dolt_commit('-Am','base');"
    "SELECT dolt_branch('feat');"
    "SELECT dolt_checkout('feat');"
    "CREATE INDEX tv ON t(v);"
    "ALTER TABLE t ADD COLUMN a TEXT;"
    "INSERT INTO t VALUES(3,'c','x');"
    "SELECT dolt_commit('-Am','feat index');"
    "SELECT dolt_checkout('main');"
    "ALTER TABLE t ADD COLUMN m TEXT;"
    "INSERT INTO t VALUES(4,'d','z'),(5,'e','y');"
    "SELECT dolt_commit('-Am','main rows');")==SQLITE_OK);
  zBefore = queryText(db, "SELECT group_concat(name,',') FROM sqlite_master");
  doltliteTestFailMergeInstallStepAt(2);
  rc = execQuiet(db, "SELECT dolt_merge('--no-ff','feat')", &zErr);
  doltliteTestFailMergeInstallStepAt(0);
  check("reindex: merge fails", rc!=SQLITE_OK);
  sqlite3_free(zErr);
  zErr = 0;
  check("reindex: peer open", sqlite3_open(zPath, &db2)==SQLITE_OK);
  zAfter = queryText(db2, "SELECT group_concat(name,',') FROM sqlite_master");
  check("reindex: nothing published",
        zBefore && zAfter && strcmp(zBefore, zAfter)==0);
  zIntegrity = queryText(db2, "PRAGMA integrity_check");
  check("reindex: integrity ok", zIntegrity && strcmp(zIntegrity, "ok")==0);
  sqlite3_free(zIntegrity);
  sqlite3_close(db2);
  check("reindex: retry merges",
        execQuiet(db, "SELECT dolt_merge('--no-ff','feat')", &zErr)==SQLITE_OK);
  zIntegrity = queryText(db, "PRAGMA integrity_check");
  check("reindex: integrity ok after merge",
        zIntegrity && strcmp(zIntegrity, "ok")==0);
  sqlite3_free(zErr);
  sqlite3_free(zBefore);
  sqlite3_free(zAfter);
  sqlite3_free(zIntegrity);
  closeRm(db, zPath);
}

/* Cherry-pick rebuilds the adopted index, then replays the picked column.
** A failed replay must not leave the rebuilt index published over rows the
** schema cannot read. */
static void test_cherry_pick_install_all_or_nothing(void){
  char zPath[256];
  sqlite3 *db, *db2 = 0;
  char *zErr = 0, *zBefore = 0, *zAfter = 0, *zIntegrity = 0;
  int rc;

  snprintf(zPath, sizeof(zPath), DOLTLITE_TEST_TMPDIR "/cas_swallow_cpinstall_%d.db", (int)getpid());
  db = openDb(zPath);
  check("cpinstall: open", db!=0);
  if( !db ) return;
  doltliteTestClearFaults();
  check("cpinstall: setup", exec(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a'),(2,'b');"
    "SELECT dolt_commit('-Am','base');"
    "SELECT dolt_branch('feat');"
    "SELECT dolt_checkout('feat');"
    "CREATE INDEX tv ON t(v);"
    "ALTER TABLE t ADD COLUMN a TEXT;"
    "INSERT INTO t VALUES(3,'c','x');"
    "SELECT dolt_commit('-Am','feat index');"
    "SELECT dolt_checkout('main');"
    "ALTER TABLE t ADD COLUMN m TEXT;"
    "INSERT INTO t VALUES(4,'d','z'),(5,'e','y');"
    "SELECT dolt_commit('-Am','main rows');")==SQLITE_OK);
  zBefore = queryText(db, "SELECT group_concat(name,',') FROM sqlite_master");
  doltliteTestFailMergeInstallStepAt(2);
  rc = execQuiet(db, "SELECT dolt_cherry_pick('feat')", &zErr);
  doltliteTestFailMergeInstallStepAt(0);
  check("cpinstall: cherry-pick fails", rc!=SQLITE_OK);
  sqlite3_free(zErr);
  check("cpinstall: peer open", sqlite3_open(zPath, &db2)==SQLITE_OK);
  zAfter = queryText(db2, "SELECT group_concat(name,',') FROM sqlite_master");
  check("cpinstall: nothing published",
        zBefore && zAfter && strcmp(zBefore, zAfter)==0);
  zIntegrity = queryText(db2, "PRAGMA integrity_check");
  check("cpinstall: integrity ok", zIntegrity && strcmp(zIntegrity, "ok")==0);
  sqlite3_close(db2);
  sqlite3_free(zBefore);
  sqlite3_free(zAfter);
  sqlite3_free(zIntegrity);
  closeRm(db, zPath);
}

/* A fast-forward becomes irreversible once the branch ref moves. An interrupt
** that arrives after that point cannot be honoured, and reporting it would
** describe a merge that did happen as one that did not. */
static void interruptDuringAdvance(void *pArg){
  sqlite3_interrupt((sqlite3*)pArg);
}

static void test_ff_interrupt_after_advance(void){
  char zPath[256];
  sqlite3 *db;
  char *zErr = 0, *zHead = 0, *zFeat = 0;
  int rc;

  snprintf(zPath, sizeof(zPath), DOLTLITE_TEST_TMPDIR "/cas_swallow_ffint_%d.db", (int)getpid());
  db = openDb(zPath);
  check("ffint: open", db!=0);
  if( !db ) return;
  doltliteTestClearFaults();
  check("ffint: setup", exec(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-Am','base');"
    "SELECT dolt_branch('feat');"
    "SELECT dolt_checkout('feat');"
    "INSERT INTO t VALUES(2,'b');"
    "SELECT dolt_commit('-Am','feat row');"
    "SELECT dolt_checkout('main');")==SQLITE_OK);
  zFeat = queryText(db, "SELECT dolt_hashof('feat')");

  doltliteTestSetMergeInstallHook(interruptDuringAdvance, db);
  rc = execQuiet(db, "SELECT dolt_merge('feat')", &zErr);
  doltliteTestSetMergeInstallHook(0, 0);

  zHead = queryText(db, "SELECT dolt_hashof('HEAD')");
  check("ffint: fast-forward reported as done",
        rc==SQLITE_OK);
  check("ffint: branch advanced",
        zHead && zFeat && strcmp(zHead, zFeat)==0);
  check("ffint: merged row present",
        exec(db, "SELECT 1 FROM t WHERE id=2")==SQLITE_OK);
  sqlite3_free(zErr);
  sqlite3_free(zHead);
  sqlite3_free(zFeat);
  sqlite3_close(db);
  remove(zPath);
}

static int failApplyPublishTrace(
  unsigned type, void *pArg, void *pStmt, void *pSql
){
  int *pFired = (int*)pArg;
  const char *sql = sqlite3_sql((sqlite3_stmt*)pStmt);
  (void)pSql;
  if( type==SQLITE_TRACE_STMT && !*pFired && sql
   && strstr(sql, "SELECT 1 FROM main.sqlite_master WHERE")
   && strstr(sql, "REFERENCES") ){
    *pFired = 1;
    doltliteTestFailNextHeadConfirm();
  }
  return 0;
}

static void test_apply_publish_failure_is_atomic(void){
  const char *azCreate[] = {
    "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT)",
    "CREATE TABLE t(id INT PRIMARY KEY,v TEXT)",
    "CREATE TABLE t(id INT PRIMARY KEY,v TEXT) WITHOUT ROWID"
  };
  int layout, revert;
  for(layout=0; layout<3; layout++){
    for(revert=0; revert<2; revert++){
      char path[512], branchPath[544];
      sqlite3 *db;
      char *before, *after, *rows, *err = 0;
      ProllyHash beforeCat, afterCat;
      int fired = 0, rc, n = -1;
      snprintf(path, sizeof(path), DOLTLITE_TEST_TMPDIR
          "/cas_apply_publish_%d_%d_%d.db", (int)getpid(), layout, revert);
      snprintf(branchPath, sizeof(branchPath), "%s@%s", path,
               revert ? "feature" : "main");
      db = openDb(path);
      check("apply failure: open", db!=0);
      if( !db ) continue;
      check("apply failure: create", exec(db, azCreate[layout])==SQLITE_OK);
      check("apply failure: setup", exec(db,
          "INSERT INTO t VALUES(1,'base');"
          "CREATE INDEX ix_t_v ON t(v);"
          "SELECT dolt_commit('-Am','base');"
          "SELECT dolt_checkout('-b','feature');"
          "DROP INDEX ix_t_v; INSERT INTO t VALUES(2,'feature');"
          "SELECT dolt_commit('-Am','feature');")==SQLITE_OK);
      if( !revert ){
        check("apply failure: checkout", exec(db,
            "SELECT dolt_checkout('main')")==SQLITE_OK);
      }
      before = queryText(db, "SELECT dolt_hashof('HEAD')");
      check("apply failure: initial catalog",
            doltliteGetPersistedWorkingCatalogHash(db, &beforeCat)==SQLITE_OK);
      sqlite3_trace_v2(db, SQLITE_TRACE_STMT, failApplyPublishTrace, &fired);
      rc = execQuiet(db, revert ? "SELECT dolt_revert('HEAD')"
          : "SELECT dolt_cherry_pick('feature')", &err);
      sqlite3_trace_v2(db, 0, 0, 0);
      doltliteTestClearFaults();
      check("apply failure: final confirm reached", fired);
      check("apply failure: busy", rc==SQLITE_BUSY);
      check("apply failure: autocommit", sqlite3_get_autocommit(db));
      check("apply failure: transaction ended",
            sqlite3_txn_state(db, "main")==SQLITE_TXN_NONE);
      check("apply failure: catalog unchanged",
            doltliteGetPersistedWorkingCatalogHash(db, &afterCat)==SQLITE_OK
            && prollyHashCompare(&beforeCat, &afterCat)==0);
      after = queryText(db, "SELECT dolt_hashof('HEAD')");
      check("apply failure: head unchanged",
            before && after && strcmp(before, after)==0);
      sqlite3_free(before);
      sqlite3_free(after);
      sqlite3_free(err);
      sqlite3_close(db);
      db = 0;
      check("apply failure: reopen", sqlite3_open(branchPath, &db)==SQLITE_OK);
      rows = queryText(db,
          "SELECT group_concat(id) FROM (SELECT id FROM t ORDER BY id)");
      check("apply failure: durable rows unchanged",
            rows && strcmp(rows, revert ? "1,2" : "1")==0);
      sqlite3_free(rows);
      check("apply failure: durable index unchanged", queryInt(db,
          "SELECT count(*) FROM sqlite_schema WHERE name='ix_t_v'", &n)==SQLITE_OK
          && n==(revert ? 0 : 1));
      check("apply failure: retry", exec(db, revert
          ? "SELECT dolt_revert('HEAD')"
          : "SELECT dolt_cherry_pick('feature')")==SQLITE_OK);
      closeRm(db, path);
    }
  }
}

int main(void){
  sqlite3_initialize();
  test_force_commit_keeps_cvs();
  test_merge_restore_not_voided();
  test_conflicts_resolve_savepoint();
  test_conflicts_resolve_interrupt(1, 0);
  test_conflicts_resolve_interrupt(1, 1);
  test_conflicts_resolve_interrupt(2, 0);
  test_conflicts_resolve_interrupt(2, 1);
  test_conflicts_resolve_boundary_interrupt(0);
  test_conflicts_resolve_boundary_interrupt(1);
  test_branch_move_ws_read_failure();
  test_merge_install_unlock_window();
  test_merge_install_peer_write_kept();
  test_failed_merge_restore_after_peer_refresh();
  test_restore_moved_refs_keeps_explicit_work();
  test_merge_schema_actions_all_or_nothing();
  test_merge_install_reindex_all_or_nothing();
  test_cherry_pick_install_all_or_nothing();
  test_ff_interrupt_after_advance();
  test_apply_publish_failure_is_atomic();
  printf("vc_cas_swallow_test: %d passed, %d failed\n", nPass, nFail);
  return nFail ? 1 : 0;
}
