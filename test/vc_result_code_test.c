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

static void checkRc(const char *name, int got, int want){
  if( got==want ){
    nPass++;
  }else{
    nFail++;
    fprintf(stderr, "FAIL: %s: got rc=%d, wanted rc=%d\n", name, got, want);
  }
}

static int exec(sqlite3 *db, const char *zSql){
  return sqlite3_exec(db, zSql, 0, 0, 0);
}

static const char *seedSql =
  "CREATE TABLE t(k INTEGER PRIMARY KEY, v TEXT);"
  "INSERT INTO t VALUES(1,'a');"
  "SELECT dolt_commit('-Am','base');"
  "SELECT dolt_branch('feature');"
  "SELECT dolt_tag('v1','HEAD');";

/* A refused write reports SQLITE_READONLY whether it is plain DML or a
** version control command, so an embedder can branch on the code. */
static void test_readonly_code(const char *zPath){
  sqlite3 *db = 0;
  remove(zPath);
  check("ro: open", sqlite3_open(zPath, &db)==SQLITE_OK);
  if( !db ) return;
  check("ro: seed", exec(db, seedSql)==SQLITE_OK);
  check("ro: query_only", exec(db, "PRAGMA query_only=1")==SQLITE_OK);

  checkRc("ro: INSERT", exec(db, "INSERT INTO t VALUES(2,'b')"), SQLITE_READONLY);
  checkRc("ro: dolt_add", exec(db, "SELECT dolt_add('-A')"), SQLITE_READONLY);
  checkRc("ro: dolt_branch", exec(db, "SELECT dolt_branch('b1')"), SQLITE_READONLY);
  checkRc("ro: dolt_tag", exec(db, "SELECT dolt_tag('t1')"), SQLITE_READONLY);
  checkRc("ro: dolt_checkout -b",
          exec(db, "SELECT dolt_checkout('-b','b2')"), SQLITE_READONLY);
  /* These three relay the code through the shared apply-merged finish and
  ** the checkout error path, which used to report only a message. */
  checkRc("ro: dolt_checkout",
          exec(db, "SELECT dolt_checkout('feature')"), SQLITE_READONLY);
  checkRc("ro: dolt_revert",
          exec(db, "SELECT dolt_revert('HEAD')"), SQLITE_READONLY);
  checkRc("ro: dolt_cherry_pick",
          exec(db, "SELECT dolt_cherry_pick('HEAD')"), SQLITE_READONLY);
  sqlite3_close(db);

  check("ro: reopen readonly",
        sqlite3_open_v2(zPath, &db, SQLITE_OPEN_READONLY, 0)==SQLITE_OK);
  if( !db ) return;
  checkRc("ro_open: INSERT",
          exec(db, "INSERT INTO t VALUES(2,'b')"), SQLITE_READONLY);
  checkRc("ro_open: dolt_branch",
          exec(db, "SELECT dolt_branch('b1')"), SQLITE_READONLY);
  checkRc("ro_open: dolt_tag",
          exec(db, "SELECT dolt_tag('t1')"), SQLITE_READONLY);
  checkRc("ro_open: dolt_checkout",
          exec(db, "SELECT dolt_checkout('feature')"), SQLITE_READONLY);
  checkRc("ro_open: dolt_revert",
          exec(db, "SELECT dolt_revert('HEAD')"), SQLITE_READONLY);
  checkRc("ro_open: dolt_cherry_pick",
          exec(db, "SELECT dolt_cherry_pick('HEAD')"), SQLITE_READONLY);
  sqlite3_close(db);
  remove(zPath);
}

/* Lock contention reports SQLITE_BUSY with a message about the lock, so a
** retry loop keyed on the code catches every command. */
static void test_busy_code(const char *zBase, const char *zWork){
  static const char *azBusy[] = {
    "SELECT dolt_add('t')",
    "SELECT dolt_merge('feature')",
    "SELECT dolt_checkout('feature')",
    "SELECT dolt_checkout('-b','nb')",
    "SELECT dolt_branch('nb2')",
    "SELECT dolt_branch('-d','feature')",
    "SELECT dolt_branch('-m','feature','f2')",
    "SELECT dolt_tag('v2','HEAD')",
    "SELECT dolt_tag('-d','v1')",
    "SELECT dolt_reset('--hard')",
    "SELECT dolt_clean()",
    "SELECT dolt_gc()",
    0
  };
  sqlite3 *db = 0;
  int i;

  remove(zBase);
  check("busy: open base", sqlite3_open(zBase, &db)==SQLITE_OK);
  if( !db ) return;
  check("busy: seed", exec(db, seedSql)==SQLITE_OK);
  sqlite3_close(db);

  for(i=0; azBusy[i]; i++){
    sqlite3 *dbPeer = 0;
    char *zErr = 0;
    char zCmd[1024];
    int rc;

    snprintf(zCmd, sizeof(zCmd), "cp -f '%s' '%s'", zBase, zWork);
    if( system(zCmd)!=0 ){
      check("busy: copy base", 0);
      return;
    }
    if( sqlite3_open(zWork, &dbPeer)!=SQLITE_OK ) return;
    if( strcmp(azBusy[i], "SELECT dolt_clean()")==0 ){
      checkRc("busy: untracked clean table", exec(dbPeer,
        "CREATE TABLE untracked(id INTEGER PRIMARY KEY)"), SQLITE_OK);
    }
    if( exec(dbPeer, "BEGIN IMMEDIATE; INSERT INTO t VALUES(50,'peer')")
          !=SQLITE_OK ){
      check("busy: peer takes the write lock", 0);
      sqlite3_close(dbPeer);
      return;
    }
    if( sqlite3_open(zWork, &db)!=SQLITE_OK ) return;
    sqlite3_busy_timeout(db, 20);

    rc = sqlite3_exec(db, azBusy[i], 0, 0, &zErr);
    checkRc(azBusy[i], rc, SQLITE_BUSY);
    /* A code with no message left on the connection reads "not an error",
    ** which tells an operator nothing about a lock. */
    check("busy: message is not bogus",
          zErr!=0 && strcmp(zErr, "not an error")!=0);
    sqlite3_free(zErr);
    if( strcmp(azBusy[i], "SELECT dolt_clean()")==0 ){
      checkRc("busy: refused clean preserves untracked table",
              exec(db, "SELECT * FROM untracked"), SQLITE_OK);
    }
    sqlite3_close(db);
    sqlite3_close(dbPeer);
  }
  remove(zBase);
  remove(zWork);
}

/* SQLITE_NOTFOUND is an internal sentinel these surfaces pass among
** themselves. A caller must never receive it. */
static void test_no_internal_code_escapes(const char *zPath){
  static const char *azMissing[] = {
    "SELECT dolt_branch('-d','nosuch')",
    "SELECT dolt_branch('-m','nosuch','x')",
    "SELECT dolt_branch('-c','nosuch','x')",
    "SELECT dolt_checkout('nosuch')",
    "SELECT dolt_tag('-d','nosuch')",
    "SELECT dolt_merge('nosuch')",
    "SELECT dolt_reset('--hard','nosuch')",
    "SELECT dolt_remote('remove','nosuch')",
    "SELECT dolt_fetch('nosuch')",
    "SELECT dolt_add('nosuchtable')",
    "SELECT dolt_branch('feature')",
    "SELECT dolt_tag('v1','HEAD')",
    0
  };
  sqlite3 *db = 0;
  int i;

  remove(zPath);
  check("missing: open", sqlite3_open(zPath, &db)==SQLITE_OK);
  if( !db ) return;
  check("missing: seed", exec(db, seedSql)==SQLITE_OK);

  for(i=0; azMissing[i]; i++){
    char *zErr = 0;
    int rc = sqlite3_exec(db, azMissing[i], 0, 0, &zErr);
    checkRc(azMissing[i], rc, SQLITE_ERROR);
    check("missing: says why", zErr!=0 && zErr[0]!=0);
    sqlite3_free(zErr);
  }
  sqlite3_close(db);
  remove(zPath);
}

static int rowCount(sqlite3 *db);

static void test_detached_after_gc(const char *zPath){
  char zLookup[256];
  const char *azRead[] = {
    "SELECT count(*), sum(length(v)) FROM t",
    "SELECT count(*) FROM dolt_log",
    "PRAGMA integrity_check",
    zLookup,
    0
  };
  sqlite3 *db = 0, *peer = 0;
  sqlite3_stmt *stmt = 0;
  char zHash[41];
  char zDetached[320];
  int i;

  remove(zPath);
  zHash[0] = 0;
  check("detached_gc: open", sqlite3_open(zPath, &db)==SQLITE_OK);
  check("detached_gc: seed", exec(db,
      "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
      "INSERT INTO t VALUES(1,'a');"
      "SELECT dolt_commit('-Am','c1');"
      "SELECT dolt_branch('x');"
      "SELECT dolt_checkout('x');"
      "WITH RECURSIVE n(i) AS (SELECT 2 UNION ALL SELECT i+1 FROM n"
      "  WHERE i<2001) INSERT INTO t SELECT i, randomblob(200) FROM n;"
      "SELECT dolt_commit('-am','on x');"
      "SELECT dolt_checkout('main');")==SQLITE_OK);
  if( sqlite3_prepare_v2(db, "SELECT dolt_hashof('x')", -1, &stmt, 0)==SQLITE_OK
   && sqlite3_step(stmt)==SQLITE_ROW ){
    snprintf(zHash, sizeof(zHash), "%s", sqlite3_column_text(stmt, 0));
  }
  sqlite3_finalize(stmt);
  sqlite3_close(db);
  db = 0;

  snprintf(zLookup,sizeof(zLookup),
      "SELECT count(*) FROM dolt_log WHERE commit_hash='%s'",zHash);
  snprintf(zDetached, sizeof(zDetached), "%s/%s", zPath, zHash);
  check("detached_gc: open detached", sqlite3_open(zDetached, &db)==SQLITE_OK);
  sqlite3_extended_result_codes(db, 1);
  check("detached_gc: point read", exec(db,
      "SELECT count(*) FROM t WHERE id=1")==SQLITE_OK);
  check("detached_gc: peer open", sqlite3_open(zPath, &peer)==SQLITE_OK);
  check("detached_gc: peer deletes branch",
        exec(peer, "SELECT dolt_branch('-D','x')")==SQLITE_OK);
  check("detached_gc: peer gc", exec(peer, "SELECT dolt_gc()")==SQLITE_OK);
  sqlite3_close(peer);

  for(i=0; azRead[i]; i++){
    char *zErr = 0;
    int rc = sqlite3_exec(db, azRead[i], 0, 0, &zErr);
    checkRc(azRead[i], rc, SQLITE_BUSY_SNAPSHOT);
    checkRc("detached_gc: extended code", sqlite3_extended_errcode(db),
            SQLITE_BUSY_SNAPSHOT);
    check("detached_gc: names revision", zErr && strstr(zErr,zHash));
    check("detached_gc: says unavailable", zErr && strstr(zErr,"no longer available"));
    sqlite3_free(zErr);
  }
  checkRc("detached_gc: checkout recovers", exec(db,"SELECT dolt_checkout('main')"),
          SQLITE_OK);
  check("detached_gc: recovered writable", sqlite3_db_readonly(db,"main")==0);
  checkRc("detached_gc: recovered write", exec(db,"INSERT INTO t VALUES(2,'recovered')"),
          SQLITE_OK);
  check("detached_gc: recovered rows", rowCount(db)==2);
  sqlite3_close(db);
  checkRc("detached_gc: reopen", sqlite3_open(zPath,&db),SQLITE_OK);
  check("detached_gc: durable rows", rowCount(db)==2);
  sqlite3_close(db);
  remove(zPath);
}

typedef struct CommitBusyCtx CommitBusyCtx;
struct CommitBusyCtx {
  sqlite3 *peer;
  int calls;
  int releaseRc;
  const char *zFinish;
};

static int releaseCommitPeer(void *arg, int attempt){
  CommitBusyCtx *p = (CommitBusyCtx*)arg;
  (void)attempt;
  p->calls++;
  if( p->calls>3 ) return 0;
  p->releaseRc = exec(p->peer, p->zFinish ? p->zFinish : "ROLLBACK");
  return p->releaseRc==SQLITE_OK;
}

static int rowCount(sqlite3 *db){
  sqlite3_stmt *stmt = 0;
  int n = -1;
  if( sqlite3_prepare_v2(db, "SELECT count(*) FROM t", -1, &stmt, 0)
      ==SQLITE_OK && sqlite3_step(stmt)==SQLITE_ROW ){
    n = sqlite3_column_int(stmt, 0);
  }
  sqlite3_finalize(stmt);
  return n;
}

static void test_commit_busy(const char *zPath){
  sqlite3 *db = 0;
  sqlite3 *peer = 0;
  char *zErr = 0;
  CommitBusyCtx ctx;
  int rc;
  remove(zPath);
  checkRc("commit_busy: open", sqlite3_open(zPath, &db), SQLITE_OK);
  checkRc("commit_busy: seed", exec(db, seedSql), SQLITE_OK);
  checkRc("commit_busy: prior insert",
          exec(db, "INSERT INTO t VALUES(2,'prior')"), SQLITE_OK);
  checkRc("commit_busy: open peer", sqlite3_open(zPath, &peer), SQLITE_OK);
  checkRc("commit_busy: peer lock",
          exec(peer, "BEGIN IMMEDIATE; INSERT INTO t VALUES(50,'peer')"),
          SQLITE_OK);
  rc = sqlite3_exec(db, "SELECT dolt_commit('-Am','blocked')", 0, 0, &zErr);
  checkRc("commit_busy: retryable code", rc, SQLITE_BUSY);
  check("commit_busy: honest message", zErr && strstr(zErr,"busy")
        && !strstr(zErr,"another connection committed"));
  sqlite3_free(zErr);
  check("commit_busy: prior row survives", rowCount(db)==2);
  checkRc("commit_busy: refused insert",
          exec(db, "INSERT INTO t VALUES(3,'retry')"), SQLITE_BUSY);
  check("commit_busy: refused insert absent", rowCount(db)==2);
  memset(&ctx, 0, sizeof(ctx));
  ctx.peer = peer;
  sqlite3_busy_handler(db, releaseCommitPeer, &ctx);
  rc = exec(db, "SELECT dolt_commit('-Am','retry')");
  checkRc("commit_busy: handler permits retry", rc, SQLITE_OK);
  check("commit_busy: handler called", ctx.calls>0);
  checkRc("commit_busy: peer released", ctx.releaseRc, SQLITE_OK);
  sqlite3_busy_handler(db, 0, 0);
  if( !sqlite3_get_autocommit(peer) ) exec(peer, "ROLLBACK");
  sqlite3_close(peer);
  checkRc("commit_busy: following insert and commit",
          exec(db, "INSERT INTO t VALUES(3,'retry');"
                   "SELECT dolt_commit('-Am','after')"), SQLITE_OK);
  sqlite3_close(db);
  checkRc("commit_busy: reopen", sqlite3_open(zPath, &db), SQLITE_OK);
  check("commit_busy: reopened rows", rowCount(db)==3);
  sqlite3_close(db);
  remove(zPath);
}

typedef struct RefBusyCtx RefBusyCtx;
struct RefBusyCtx {
  sqlite3 *peer;
  int calls;
  int release;
  int releaseRc;
};

static int refBusyHandler(void *arg, int attempt){
  RefBusyCtx *p = (RefBusyCtx*)arg;
  p->calls++;
  if( !p->release || attempt>4 ) return 0;
  if( attempt<2 ) return 1;
  p->releaseRc = exec(p->peer, "ROLLBACK");
  return p->releaseRc==SQLITE_OK;
}

static int scalarInt(sqlite3 *db, const char *zSql){
  sqlite3_stmt *pStmt = 0;
  int result = -1;
  if( sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0)==SQLITE_OK
   && sqlite3_step(pStmt)==SQLITE_ROW ){
    result = sqlite3_column_int(pStmt, 0);
  }
  sqlite3_finalize(pStmt);
  return result;
}

static void test_waited_peer_commit(const char *zPath){
  static const struct {
    const char *setup;
    const char *sql;
    const char *verify;
    int nRows;
    int peerInHead;
  } cases[] = {
    {"", "SELECT dolt_checkout('-b','new')",
     "SELECT active_branch()='new'", 2, 0},
    {"", "SELECT dolt_checkout('-b','new','HEAD')",
     "SELECT active_branch()='new'", 2, 0},
    {"", "SELECT dolt_checkout('feature')",
     "SELECT active_branch()='feature'", 2, 0},
    {"", "SELECT dolt_commit('--allow-empty','-m','waited')",
     "SELECT count(*) FROM dolt_log WHERE message='waited'", 2, 0},
    {"", "SELECT dolt_commit('-Am','waited')",
     "SELECT count(*) FROM dolt_log WHERE message='waited'", 2, 1},
    {"", "SELECT dolt_commit('-am','waited')",
     "SELECT count(*) FROM dolt_log WHERE message='waited'", 2, 1},
    {"INSERT INTO t VALUES(2,'staged'); SELECT dolt_add('t')",
     "SELECT dolt_commit('-m','waited')",
     "SELECT count(*)=2 FROM dolt_at_t('HEAD')", 3, 0}
  };
  int i;
  for(i=0; i<(int)(sizeof(cases)/sizeof(cases[0])); i++){
    sqlite3 *db = 0, *peer = 0, *observer = 0;
    CommitBusyCtx ctx;
    int rc;
    remove(zPath);
    checkRc("waited_peer: open", sqlite3_open(zPath,&db), SQLITE_OK);
    checkRc("waited_peer: seed", exec(db,seedSql), SQLITE_OK);
    checkRc("waited_peer: setup", exec(db,cases[i].setup), SQLITE_OK);
    checkRc("waited_peer: peer open", sqlite3_open(zPath,&peer), SQLITE_OK);
    checkRc("waited_peer: peer write", exec(peer,
      "BEGIN IMMEDIATE; INSERT INTO t VALUES(50,'peer')"), SQLITE_OK);
    memset(&ctx,0,sizeof(ctx));
    ctx.peer = peer;
    ctx.zFinish = "COMMIT";
    sqlite3_busy_handler(db,releaseCommitPeer,&ctx);
    rc = exec(db,cases[i].sql);
    checkRc(cases[i].sql,rc,SQLITE_OK);
    check("waited_peer: callback called",ctx.calls>0);
    checkRc("waited_peer: peer committed",ctx.releaseRc,SQLITE_OK);
    sqlite3_busy_handler(db,0,0);
    if( !sqlite3_get_autocommit(peer) ) exec(peer,"ROLLBACK");
    check("waited_peer: operation took effect",scalarInt(db,cases[i].verify)==1);
    check("waited_peer: staged contents",scalarInt(db,
      "SELECT count(*) FROM dolt_at_t('HEAD') WHERE k=50")==cases[i].peerInHead);
    check("waited_peer: integrity",scalarInt(db,
      "SELECT integrity_check='ok' FROM pragma_integrity_check")==1);
    checkRc("waited_peer: observer open",sqlite3_open(zPath,&observer),SQLITE_OK);
    check("waited_peer: main rows preserved",rowCount(observer)==cases[i].nRows);
    check("waited_peer: peer row durable",scalarInt(observer,
      "SELECT count(*) FROM t WHERE k=50 AND v='peer'")==1);
    sqlite3_close(observer);
    sqlite3_close(peer);
    sqlite3_close(db);
  }
  remove(zPath);
}

static void test_ref_busy_handler(const char *zPath){
  static const struct {
    const char *setup;
    const char *sql;
    const char *verify;
    int noWait;
  } cases[] = {
    {"", "SELECT dolt_branch('new')",
     "SELECT count(*) FROM dolt_branches WHERE name='new'", 0},
    {"", "SELECT dolt_branch('-d','feature')",
     "SELECT count(*)=0 FROM dolt_branches WHERE name='feature'", 0},
    {"", "SELECT dolt_branch('-m','feature','renamed')",
     "SELECT count(*) FROM dolt_branches WHERE name='renamed'", 0},
    {"", "SELECT dolt_branch('-c','feature','copied')",
     "SELECT count(*) FROM dolt_branches WHERE name='copied'", 0},
    {"", "SELECT dolt_tag('new')",
     "SELECT count(*) FROM dolt_tags WHERE tag_name='new'", 0},
    {"", "SELECT dolt_tag('-d','v1')",
     "SELECT count(*)=0 FROM dolt_tags WHERE tag_name='v1'", 0},
    {"", "SELECT dolt_checkout('feature')", "SELECT active_branch()='feature'", 0},
    {"", "SELECT dolt_checkout('-b','new')", "SELECT active_branch()='new'", 0},
    {"UPDATE t SET v='dirty'", "SELECT dolt_reset('--hard')",
     "SELECT v='a' FROM t WHERE k=1", 0},
    {"", "SELECT dolt_clean()", "SELECT count(*) FROM t", 1},
    {"", "SELECT dolt_reset('t')", "SELECT count(*) FROM t", 1},
    {"CREATE TABLE untracked(id INTEGER PRIMARY KEY)", "SELECT dolt_clean()",
     "SELECT count(*)=0 FROM sqlite_master WHERE name='untracked'", 0},
    {"", "VACUUM", "SELECT count(*) FROM t", 0}
  };
  int i;
  for(i=0; i<(int)(sizeof(cases)/sizeof(cases[0])); i++){
    sqlite3 *db = 0;
    sqlite3 *peer = 0;
    RefBusyCtx ctx;
    int rc;
    remove(zPath);
    checkRc("ref_busy: open", sqlite3_open(zPath, &db), SQLITE_OK);
    checkRc("ref_busy: seed", exec(db, seedSql), SQLITE_OK);
    checkRc("ref_busy: setup", exec(db, cases[i].setup), SQLITE_OK);
    checkRc("ref_busy: peer open", sqlite3_open(zPath, &peer), SQLITE_OK);
    checkRc("ref_busy: peer lock",
            exec(peer, "BEGIN IMMEDIATE; INSERT INTO t VALUES(50,'peer')"),
            SQLITE_OK);
    memset(&ctx, 0, sizeof(ctx));
    ctx.peer = peer;
    sqlite3_busy_handler(db, refBusyHandler, &ctx);
    if( strcmp(cases[i].sql, "VACUUM")==0 ){
      sqlite3_mutex_enter(sqlite3_db_mutex(db));
      rc = doltliteGcCompactStore(db, doltliteGetChunkStore(db));
      sqlite3_mutex_leave(sqlite3_db_mutex(db));
      checkRc("ref_busy: automatic compaction defers", rc, SQLITE_BUSY);
      check("ref_busy: automatic compaction does not wait", ctx.calls==0);
    }
    rc = exec(db, cases[i].sql);
    if( cases[i].noWait ){
      checkRc(cases[i].sql, rc, SQLITE_OK);
      check("ref_busy: noop does not wait", ctx.calls==0);
      check("ref_busy: noop preserves peer transaction", !sqlite3_get_autocommit(peer));
      check("ref_busy: noop preserves peer pending row", rowCount(peer)==2);
      ctx.release = 1;
      checkRc("ref_busy: repeated noop", exec(db, cases[i].sql), SQLITE_OK);
      check("ref_busy: repeated noop does not wait", ctx.calls==0);
      check("ref_busy: repeated noop preserves peer transaction", !sqlite3_get_autocommit(peer));
      checkRc("ref_busy: noop peer rollback", exec(peer, "ROLLBACK"), SQLITE_OK);
    }else{
      checkRc(cases[i].sql, rc, SQLITE_BUSY);
      if( ctx.calls==0 ) fprintf(stderr, "No busy callback: %s\n", cases[i].sql);
      check("ref_busy: declining handler is called", ctx.calls>0);
      check("ref_busy: decline preserves peer transaction", !sqlite3_get_autocommit(peer));
      if( strcmp(cases[i].sql, "SELECT dolt_clean()")==0 ){
        checkRc("ref_busy: refused clean preserves untracked table",
                exec(db, "SELECT * FROM untracked"), SQLITE_OK);
      }
      ctx.calls = 0;
      ctx.release = 1;
      sqlite3_busy_handler(db, refBusyHandler, &ctx);
      rc = exec(db, cases[i].sql);
      checkRc(cases[i].sql, rc, SQLITE_OK);
      check("ref_busy: handler permits retry", ctx.calls>=3);
      checkRc("ref_busy: peer rollback", ctx.releaseRc, SQLITE_OK);
      check("ref_busy: peer released", sqlite3_get_autocommit(peer));
    }
    sqlite3_busy_handler(db, 0, 0);
    check("ref_busy: operation took effect", scalarInt(db, cases[i].verify)==1);
    check("ref_busy: peer uncommitted row absent", rowCount(db)==1);
    if( !sqlite3_get_autocommit(peer) ) exec(peer, "ROLLBACK");
    sqlite3_close(peer);
    sqlite3_close(db);
  }
  remove(zPath);
}

/* A lock dolt_gc cannot take is retryable, including gc inside the caller's
** own write transaction. */
static void test_gc_busy(const char *zPath){
  sqlite3 *db = 0;
  sqlite3 *peer = 0;
  char *zErr = 0;
  int rc;

  remove(zPath);
  checkRc("gc_busy: open", sqlite3_open(zPath, &db), SQLITE_OK);
  sqlite3_extended_result_codes(db, 1);
  checkRc("gc_busy: seed", exec(db, seedSql), SQLITE_OK);
  checkRc("gc_busy: begin", exec(db, "BEGIN"), SQLITE_OK);
  rc = sqlite3_exec(db, "SELECT dolt_gc()", 0, 0, &zErr);
  checkRc("gc_busy: own txn", rc, SQLITE_BUSY);
  check("gc_busy: own txn says exclusive",
        zErr && strstr(zErr, "gc requires exclusive access")!=0);
  sqlite3_free(zErr);
  zErr = 0;
  checkRc("gc_busy: rollback", exec(db, "ROLLBACK"), SQLITE_OK);

  checkRc("gc_busy: peer open", sqlite3_open(zPath, &peer), SQLITE_OK);
  checkRc("gc_busy: peer lock",
          exec(peer, "BEGIN IMMEDIATE; INSERT INTO t VALUES(50,'peer')"),
          SQLITE_OK);
  sqlite3_busy_timeout(db, 20);
  rc = sqlite3_exec(db, "SELECT dolt_gc()", 0, 0, &zErr);
  checkRc("gc_busy: peer holds lock", rc, SQLITE_BUSY);
  check("gc_busy: peer lock names the lock",
        zErr && strstr(zErr, "failed to acquire lock for gc")!=0);
  sqlite3_free(zErr);
  sqlite3_close(db);
  sqlite3_close(peer);
  remove(zPath);
}

/* A clean session that read before a peer commit must cherry-pick that
** peer's tip, not report the peer commit as local uncommitted changes. */
static void test_stale_cherry_pick(const char *zPath){
  static const char *zSetup =
    "CREATE TABLE t(id INTEGER PRIMARY KEY);"
    "CREATE TABLE u(x INTEGER PRIMARY KEY);"
    "INSERT INTO t VALUES(1);"
    "SELECT dolt_commit('-Am','init');"
    "SELECT dolt_branch('f');"
    "SELECT dolt_checkout('f');"
    "INSERT INTO u VALUES(9);"
    "SELECT dolt_commit('-am','f1');"
    "SELECT dolt_checkout('main');";
  sqlite3 *db = 0;
  sqlite3 *peer = 0;
  char *zErr = 0;
  int rc;

  remove(zPath);
  checkRc("stale_pick: open", sqlite3_open(zPath, &db), SQLITE_OK);
  sqlite3_extended_result_codes(db, 1);
  checkRc("stale_pick: seed", exec(db, zSetup), SQLITE_OK);
  checkRc("stale_pick: read", exec(db, "SELECT count(*) FROM t"), SQLITE_OK);
  checkRc("stale_pick: peer open", sqlite3_open(zPath, &peer), SQLITE_OK);
  checkRc("stale_pick: peer commit",
          exec(peer, "INSERT INTO t VALUES(900);"
                     "SELECT dolt_commit('-am','peer')"), SQLITE_OK);
  sqlite3_close(peer);
  rc = sqlite3_exec(db, "SELECT dolt_cherry_pick('f')", 0, 0, &zErr);
  checkRc("stale_pick: cherry-pick", rc, SQLITE_OK);
  check("stale_pick: not a false dirty tree",
        zErr==0 || strstr(zErr, "uncommitted changes")==0);
  sqlite3_free(zErr);
  zErr = 0;
  check("stale_pick: picked row", scalarInt(db, "SELECT x FROM u")==9);
  check("stale_pick: peer row kept", scalarInt(db, "SELECT count(*) FROM t")==2);

  checkRc("stale_pick: local edit",
          exec(db, "INSERT INTO t VALUES(3)"), SQLITE_OK);
  rc = sqlite3_exec(db, "SELECT dolt_cherry_pick('HEAD')", 0, 0, &zErr);
  checkRc("stale_pick: real dirt stays an error", rc, SQLITE_ERROR);
  check("stale_pick: real dirt names uncommitted changes",
        zErr && strstr(zErr, "uncommitted changes")!=0);
  sqlite3_free(zErr);
  sqlite3_close(db);
  remove(zPath);
}

/* The retry message is SQLITE_BUSY, including merge, cherry-pick and revert. */
static void test_peer_branch_busy_code(const char *zPath){
  static const char *zSetup =
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-Am','init');"
    "SELECT dolt_branch('f');"
    "SELECT dolt_checkout('f');"
    "INSERT INTO t VALUES(2,'f');"
    "SELECT dolt_commit('-am','f1');"
    "SELECT dolt_checkout('main');"
    "UPDATE t SET v='m' WHERE id=1;"
    "SELECT dolt_commit('-am','m1');";
  static const char *azSql[] = {
    "SELECT dolt_merge('f')",
    "SELECT dolt_cherry_pick('f')",
    "SELECT dolt_revert('HEAD')",
    0
  };
  int i;

  for(i=0; azSql[i]; i++){
    sqlite3 *db = 0;
    char *zErr = 0;
    int rc;
    remove(zPath);
    checkRc("peer_busy: open", sqlite3_open(zPath, &db), SQLITE_OK);
    sqlite3_extended_result_codes(db, 1);
    checkRc("peer_busy: seed", exec(db, zSetup), SQLITE_OK);
    doltliteTestFailNextHeadConfirm();
    rc = sqlite3_exec(db, azSql[i], 0, 0, &zErr);
    checkRc(azSql[i], rc, SQLITE_BUSY);
    check("peer_busy: asks for a retry",
          zErr && strstr(zErr, "another connection committed")!=0);
    sqlite3_free(zErr);
    doltliteTestClearFaults();
    sqlite3_close(db);
  }
  remove(zPath);
}

int main(void){
  char zPath[256];
  char zBase[256];
  char zWork[256];
  int pid = (int)getpid();

  snprintf(zPath, sizeof(zPath), DOLTLITE_TEST_TMPDIR "/dolt_rc_%d.db", pid);
  snprintf(zBase, sizeof(zBase), DOLTLITE_TEST_TMPDIR "/dolt_rc_base_%d.db", pid);
  snprintf(zWork, sizeof(zWork), DOLTLITE_TEST_TMPDIR "/dolt_rc_work_%d.db", pid);

  test_readonly_code(zPath);
  test_busy_code(zBase, zWork);
  test_gc_busy(zPath);
  test_stale_cherry_pick(zPath);
  test_peer_branch_busy_code(zPath);
  test_no_internal_code_escapes(zPath);
  test_commit_busy(zPath);
  test_waited_peer_commit(zPath);
  test_ref_busy_handler(zPath);
  test_detached_after_gc(zPath);

  printf("vc_result_code_test: %d passed, %d failed\n", nPass, nFail);
  return nFail ? 1 : 0;
}
