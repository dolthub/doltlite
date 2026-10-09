#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include "sqlite3.h"
#include "doltlite_internal.h"
#include "chunk_store.h"
#include "doltlite_remote.h"
#include <pthread.h>
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
  sqlite3_free(zErr);
  return rc;
}

static int execErr(sqlite3 *db, const char *zSql, char **pzErr){
  return sqlite3_exec(db, zSql, 0, 0, pzErr);
}

static void rm(const char *zPath){
  char zBuf[512];
  const char *zSlash = strrchr(zPath, '/');
  remove(zPath);
  sqlite3_snprintf(sizeof(zBuf), zBuf, "%s-lock", zPath);
  remove(zBuf);
  sqlite3_snprintf(sizeof(zBuf), zBuf, "%s-wal", zPath);
  remove(zBuf);
  if( zSlash ){
    sqlite3_snprintf(sizeof(zBuf), zBuf, "%.*s.%s-lock",
                     (int)(zSlash - zPath + 1), zPath, zSlash+1);
  }else{
    sqlite3_snprintf(sizeof(zBuf), zBuf, ".%s-lock", zPath);
  }
  remove(zBuf);
}

/* A peer holding the remote graph lock must not be reported as a ref race. */
static void test_lock_busy_not_refs_changed(void){
  char zRemote[256], zSrc[256], zUrl[512];
  sqlite3 *dbRemote = 0, *dbSrc = 0;
  ChunkStore *cs = 0;
  char *zErr = 0;
  int rc;

  snprintf(zRemote, sizeof(zRemote), DOLTLITE_TEST_TMPDIR "/push_lock_remote_%d.db", (int)getpid());
  snprintf(zSrc, sizeof(zSrc), DOLTLITE_TEST_TMPDIR "/push_lock_src_%d.db", (int)getpid());
  rm(zRemote);
  rm(zSrc);

  check("lock: open remote", sqlite3_open(zRemote, &dbRemote)==SQLITE_OK);
  check("lock: seed remote",
        exec(dbRemote,
          "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
          "INSERT INTO t VALUES(1,'base');"
          "SELECT dolt_commit('-Am','base');")==SQLITE_OK);

  check("lock: open src", sqlite3_open(zSrc, &dbSrc)==SQLITE_OK);
  sqlite3_snprintf(sizeof(zUrl), zUrl,
    "SELECT dolt_clone('file://%s');", zRemote);
  check("lock: clone", exec(dbSrc, zUrl)==SQLITE_OK);
  check("lock: local commit",
        exec(dbSrc,
          "INSERT INTO t VALUES(2,'src');"
          "SELECT dolt_commit('-Am','src');")==SQLITE_OK);

  cs = doltliteGetChunkStore(dbRemote);
  check("lock: chunk store", cs!=0);
  if( cs ){
    check("lock: acquire graph lock",
          chunkStoreLockAndRefresh(cs)==SQLITE_OK);
  }

  sqlite3_snprintf(sizeof(zUrl), zUrl,
    "SELECT dolt_push('origin','main');");
  rc = execErr(dbSrc, zUrl, &zErr);
  check("lock_busy_not_refs_changed", rc==SQLITE_BUSY);
  check("lock: message names the lock",
        zErr && strstr(zErr, "locked")!=0);
  check("lock: message is not a ref-race",
        zErr && strstr(zErr, "refs changed")==0);
  sqlite3_free(zErr);

  if( cs ) chunkStoreUnlock(cs);
  zErr = 0;
  rc = execErr(dbSrc, "SELECT dolt_push('origin','main');", &zErr);
  check("lock: push succeeds after unlock", rc==SQLITE_OK);
  sqlite3_free(zErr);

  sqlite3_close(dbSrc);
  sqlite3_close(dbRemote);
  rm(zRemote);
  rm(zSrc);
}

/* Diverged tips are a fast-forward refusal, not lock contention. */
static void test_diverged_is_not_lock(void){
  char zRemote[256], zSrc[256], zUrl[512];
  sqlite3 *dbRemote = 0, *dbSrc = 0;
  char *zErr = 0;
  int rc;

  snprintf(zRemote, sizeof(zRemote), DOLTLITE_TEST_TMPDIR "/push_nff_remote_%d.db", (int)getpid());
  snprintf(zSrc, sizeof(zSrc), DOLTLITE_TEST_TMPDIR "/push_nff_src_%d.db", (int)getpid());
  rm(zRemote);
  rm(zSrc);

  check("nff: open remote", sqlite3_open(zRemote, &dbRemote)==SQLITE_OK);
  check("nff: seed remote",
        exec(dbRemote,
          "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
          "INSERT INTO t VALUES(1,'base');"
          "SELECT dolt_commit('-Am','base');")==SQLITE_OK);
  check("nff: open src", sqlite3_open(zSrc, &dbSrc)==SQLITE_OK);
  sqlite3_snprintf(sizeof(zUrl), zUrl,
    "SELECT dolt_clone('file://%s');", zRemote);
  check("nff: clone", exec(dbSrc, zUrl)==SQLITE_OK);
  check("nff: remote moves",
        exec(dbRemote,
          "INSERT INTO t VALUES(3,'remote');"
          "SELECT dolt_commit('-Am','remote');")==SQLITE_OK);
  check("nff: src moves",
        exec(dbSrc,
          "INSERT INTO t VALUES(2,'src');"
          "SELECT dolt_commit('-Am','src');")==SQLITE_OK);
  rc = execErr(dbSrc, "SELECT dolt_push('origin','main');", &zErr);
  check("nff: push refused", rc!=SQLITE_OK);
  check("nff: not a lock message",
        !zErr || strstr(zErr, "locked")==0);
  check("nff: fast-forward or refs diagnosis",
        zErr && (strstr(zErr, "fast-forward")!=0
              || strstr(zErr, "refs changed")!=0));
  sqlite3_free(zErr);

  sqlite3_close(dbSrc);
  sqlite3_close(dbRemote);
  rm(zRemote);
  rm(zSrc);
}

static DoltliteRemote originalRemote;
static sqlite3 *racePeer;
static int raceStage, raceSameBranch, raceRemaining, raceCount;

static int movePeer(void){
  char *zSql;
  int rc;
  raceRemaining--;
  raceCount++;
  zSql = raceSameBranch
      ? sqlite3_mprintf("INSERT INTO t VALUES(%d,'peer');"
                         "SELECT dolt_commit('-Am','peer');", 100+raceCount)
      : sqlite3_mprintf("SELECT dolt_branch('peer%d');", raceCount);
  if( !zSql ) return SQLITE_NOMEM;
  rc = exec(racePeer, zSql);
  sqlite3_free(zSql);
  return rc;
}

static int racingGetRefs(DoltliteRemote *p, u8 **ppData, int *pnData){
  int rc = originalRemote.xGetRefs(p, ppData, pnData);
  if( rc==SQLITE_OK && raceStage==1 && raceRemaining ) rc = movePeer();
  return rc;
}

static int racingSetRefs(DoltliteRemote *p, const ProllyHash *h,
                         const char *zRef, int force, const u8 *data, int n){
  if( raceStage==2 && raceRemaining ){
    ProllyHash wrong = {{0}};
    int rc = originalRemote.xSetRefsIf(p, &wrong, zRef, force, data, n);
    if( rc!=SQLITE_BUSY_SNAPSHOT ) return SQLITE_ERROR;
    rc = movePeer();
    return rc==SQLITE_OK ? SQLITE_BUSY_SNAPSHOT : rc;
  }
  return originalRemote.xSetRefsIf(p, h, zRef, force, data, n);
}

static int racingCommit(DoltliteRemote *p){
  if( raceStage==3 && raceRemaining ){
    ProllyHash wrong = {{0}};
    int rc = originalRemote.xSetRefsIf(p, &wrong, "main", 0, 0, 0);
    if( rc!=SQLITE_BUSY_SNAPSHOT ) return SQLITE_ERROR;
    rc = movePeer();
    return rc==SQLITE_OK ? SQLITE_BUSY_SNAPSHOT : rc;
  }
  return originalRemote.xCommit(p);
}

static void test_push_ref_race(int stage, int sameBranch, int noOp,
                              int force, int nRaces, int deleting){
  char zRemote[256], zSrc[256], zSql[512];
  sqlite3 *dbSrc = 0;
  DoltliteRemote *remote;
  ChunkStore *local;
  ProllyHash localTip, remoteTip;
  int rc;
  snprintf(zRemote, sizeof(zRemote), DOLTLITE_TEST_TMPDIR "/push_race_remote_%d.db", (int)getpid());
  snprintf(zSrc, sizeof(zSrc), DOLTLITE_TEST_TMPDIR "/push_race_src_%d.db", (int)getpid());
  rm(zRemote);
  rm(zSrc);
  check("race: open peer", sqlite3_open(zRemote, &racePeer)==SQLITE_OK);
  check("race: seed peer", exec(racePeer,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'base');"
    "SELECT dolt_commit('-Am','base');") == SQLITE_OK);
  if( deleting ){
    check("race: retain default branch", exec(racePeer,
      "SELECT dolt_branch('keep'); SELECT dolt_default_branch('keep');")==SQLITE_OK);
  }
  check("race: open src", sqlite3_open(zSrc, &dbSrc)==SQLITE_OK);
  sqlite3_snprintf(sizeof(zSql), zSql, "SELECT dolt_clone('file://%s');", zRemote);
  check("race: clone", exec(dbSrc, zSql)==SQLITE_OK);
  check("race: checkout main", exec(dbSrc, "SELECT dolt_checkout('main');")==SQLITE_OK);
  if( !noOp ){
    check("race: commit local", exec(dbSrc,
      "INSERT INTO t VALUES(2,'local');"
      "SELECT dolt_commit('-Am','local');") == SQLITE_OK);
  }
  local = doltliteGetChunkStore(dbSrc);
  check("race: read local head", chunkStoreFindBranch(local, "main", &localTip)==SQLITE_OK);
  remote = doltliteFsRemoteOpen(sqlite3_vfs_find(0), zRemote);
  check("race: open transport", remote!=0);
  if( remote ){
    originalRemote = *remote;
    raceStage = stage;
    raceSameBranch = sameBranch;
    raceRemaining = nRaces;
    raceCount = 0;
    remote->xGetRefs = racingGetRefs;
    remote->xSetRefsIf = racingSetRefs;
    remote->xCommit = racingCommit;
    rc = doltlitePush(local, remote, deleting ? ":main" : "main", force);
    if( rc!=SQLITE_OK && !sameBranch && nRaces<64 ){
      fprintf(stderr, "race stage=%d noOp=%d delete=%d rc=%d\n",
              stage, noOp, deleting, rc);
    }
    if( sameBranch ){
      check("same_branch_race_rejected_even_with_force", rc==SQLITE_BUSY_SNAPSHOT);
    }else if( nRaces>=64 ){
      check("unrelated_ref_retry_is_bounded", rc==SQLITE_BUSY_SNAPSHOT && raceCount==64);
    }else{
      check("unrelated_ref_push_retries", rc==SQLITE_OK);
      check("race: all unrelated updates completed", raceCount==nRaces);
    }
    remote->xClose(remote);
    check("race: refresh peer", chunkStoreLockAndRefresh(doltliteGetChunkStore(racePeer))==SQLITE_OK);
    rc = chunkStoreFindBranch(doltliteGetChunkStore(racePeer), "main", &remoteTip);
    if( !sameBranch && nRaces<64 ){
      check("race: expected final target", deleting ? rc==SQLITE_NOTFOUND
          : rc==SQLITE_OK && prollyHashCompare(&localTip, &remoteTip)==0);
    }else{
      check("race: rejected push preserves peer", rc==SQLITE_OK
          && prollyHashCompare(&localTip, &remoteTip)!=0);
    }
    if( !sameBranch ){
      sqlite3_snprintf(sizeof(zSql), zSql, "peer%d", raceCount);
      check("race: unrelated ref survives",
        chunkStoreFindBranch(doltliteGetChunkStore(racePeer), zSql, 0)==SQLITE_OK);
    }
    chunkStoreUnlock(doltliteGetChunkStore(racePeer));
  }
  sqlite3_close(dbSrc);
  sqlite3_close(racePeer);
  racePeer = 0;
  rm(zRemote);
  rm(zSrc);
}

/* A lock held by a peer is a wait, not a failure: the push must honour this
** connection's busy_timeout rather than a schedule of its own. A peer thread
** releases the graph lock partway through the window. */
struct LockHolder {
  ChunkStore *cs;
  int holdMs;
};

static void *holdGraphLock(void *pArg){
  struct LockHolder *p = (struct LockHolder*)pArg;
  sqlite3_sleep(p->holdMs);
  chunkStoreUnlock(p->cs);
  return 0;
}

static void test_push_waits_for_busy_timeout(void){
  char zRemote[256], zSrc[256], zUrl[512];
  sqlite3 *dbRemote = 0, *dbSrc = 0;
  ChunkStore *cs;
  struct LockHolder holder;
  pthread_t tid;
  char *zErr = 0;
  int rc;

  snprintf(zRemote, sizeof(zRemote), DOLTLITE_TEST_TMPDIR "/push_wait_remote_%d.db", (int)getpid());
  snprintf(zSrc, sizeof(zSrc), DOLTLITE_TEST_TMPDIR "/push_wait_src_%d.db", (int)getpid());
  rm(zRemote);
  rm(zSrc);

  check("wait: open remote", sqlite3_open(zRemote, &dbRemote)==SQLITE_OK);
  check("wait: seed remote",
        exec(dbRemote,
          "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
          "INSERT INTO t VALUES(1,'base');"
          "SELECT dolt_commit('-Am','base');")==SQLITE_OK);

  check("wait: open src", sqlite3_open(zSrc, &dbSrc)==SQLITE_OK);
  sqlite3_snprintf(sizeof(zUrl), zUrl,
    "SELECT dolt_clone('file://%s');", zRemote);
  check("wait: clone", exec(dbSrc, zUrl)==SQLITE_OK);
  check("wait: local commit",
        exec(dbSrc,
          "INSERT INTO t VALUES(2,'src');"
          "SELECT dolt_commit('-Am','src');")==SQLITE_OK);

  cs = doltliteGetChunkStore(dbRemote);
  check("wait: chunk store", cs!=0);
  if( !cs ){
    sqlite3_close(dbSrc);
    sqlite3_close(dbRemote);
    return;
  }
  check("wait: acquire graph lock", chunkStoreLockAndRefresh(cs)==SQLITE_OK);

  holder.cs = cs;
  holder.holdMs = 1500;
  check("wait: start holder", pthread_create(&tid, 0, holdGraphLock, &holder)==0);

  sqlite3_busy_timeout(dbSrc, 20000);
  rc = execErr(dbSrc, "SELECT dolt_push('origin','main');", &zErr);
  check("wait: push waits out the peer instead of failing", rc==SQLITE_OK);
  if( rc!=SQLITE_OK && zErr ) printf("  got: %s\n", zErr);
  sqlite3_free(zErr);
  pthread_join(tid, 0);

  sqlite3_close(dbSrc);
  sqlite3_close(dbRemote);
  rm(zRemote);
  rm(zSrc);
}

/* The same holds for the local store: fetch and pull install refs under
** this database's lock, which a peer connection may be holding. */
static void test_fetch_pull_wait_for_local_busy_timeout(void){
  char zRemote[256], zSrc[256], zUrl[512];
  const char *aOp[2] = {"SELECT dolt_fetch('origin');",
                        "SELECT dolt_pull('origin');"};
  sqlite3 *dbRemote = 0, *dbSrc = 0, *dbPeer = 0;
  int i;

  snprintf(zRemote, sizeof(zRemote), DOLTLITE_TEST_TMPDIR "/fetch_wait_remote_%d.db", (int)getpid());
  snprintf(zSrc, sizeof(zSrc), DOLTLITE_TEST_TMPDIR "/fetch_wait_src_%d.db", (int)getpid());
  rm(zRemote);
  rm(zSrc);

  check("local wait: open remote", sqlite3_open(zRemote, &dbRemote)==SQLITE_OK);
  check("local wait: seed remote",
        exec(dbRemote,
          "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
          "INSERT INTO t VALUES(1,'base');"
          "SELECT dolt_commit('-Am','base');")==SQLITE_OK);
  check("local wait: open src", sqlite3_open(zSrc, &dbSrc)==SQLITE_OK);
  sqlite3_snprintf(sizeof(zUrl), zUrl,
    "SELECT dolt_clone('file://%s');", zRemote);
  check("local wait: clone", exec(dbSrc, zUrl)==SQLITE_OK);
  check("local wait: open peer", sqlite3_open(zSrc, &dbPeer)==SQLITE_OK);
  sqlite3_busy_timeout(dbSrc, 20000);

  for(i=0; i<2; i++){
    struct LockHolder holder;
    pthread_t tid;
    char zName[64];
    char *zErr = 0;
    int rc;
    sqlite3_snprintf(sizeof(zUrl), zUrl,
      "INSERT INTO t VALUES(%d,'remote'); SELECT dolt_commit('-am','r%d');",
      10+i, i);
    check("local wait: remote commit", exec(dbRemote, zUrl)==SQLITE_OK);
    holder.cs = doltliteGetChunkStore(dbPeer);
    holder.holdMs = 1500;
    check("local wait: acquire local graph lock",
          chunkStoreLockAndRefresh(holder.cs)==SQLITE_OK);
    check("local wait: start holder",
          pthread_create(&tid, 0, holdGraphLock, &holder)==0);
    rc = execErr(dbSrc, aOp[i], &zErr);
    sqlite3_snprintf(sizeof(zName), zName, "local wait: %s waits out the peer",
                     i ? "pull" : "fetch");
    check(zName, rc==SQLITE_OK);
    if( rc!=SQLITE_OK && zErr ) printf("  got: %s\n", zErr);
    sqlite3_free(zErr);
    pthread_join(tid, 0);
  }
  {
    sqlite3_stmt *pStmt = 0;
    int n = -1;
    if( sqlite3_prepare_v2(dbSrc, "SELECT count(*) FROM t WHERE id IN (10,11)",
                           -1, &pStmt, 0)==SQLITE_OK
     && sqlite3_step(pStmt)==SQLITE_ROW ){
      n = sqlite3_column_int(pStmt, 0);
    }
    sqlite3_finalize(pStmt);
    check("local wait: pull landed both remote commits", n==2);
  }

  sqlite3_close(dbPeer);
  sqlite3_close(dbSrc);
  sqlite3_close(dbRemote);
  rm(zRemote);
  rm(zSrc);
}

int main(void){
  sqlite3_initialize();
  check("empty_delete_target_is_misuse",
        doltlitePush(0, 0, ":", 0)==SQLITE_MISUSE);
  test_lock_busy_not_refs_changed();
  test_push_waits_for_busy_timeout();
  test_fetch_pull_wait_for_local_busy_timeout();
  test_diverged_is_not_lock();
  test_push_ref_race(1, 0, 0, 0, 1, 0);
  test_push_ref_race(2, 0, 0, 0, 2, 0);
  test_push_ref_race(3, 0, 0, 0, 2, 0);
  test_push_ref_race(2, 0, 0, 0, 20, 0);
  test_push_ref_race(3, 0, 0, 0, 20, 0);
  test_push_ref_race(1, 0, 1, 0, 1, 0);
  test_push_ref_race(1, 1, 0, 0, 1, 0);
  test_push_ref_race(2, 1, 0, 1, 1, 0);
  test_push_ref_race(3, 1, 0, 1, 1, 0);
  test_push_ref_race(2, 0, 0, 0, 100, 0);
  test_push_ref_race(2, 0, 0, 0, 1, 1);
  test_push_ref_race(2, 1, 0, 1, 1, 1);
  printf("remote_push_lock_busy_test: %d passed, %d failed\n", nPass, nFail);
  return nFail ? 1 : 0;
}
