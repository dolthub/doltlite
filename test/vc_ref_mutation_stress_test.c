#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include "sqlite3.h"
#include "doltlite_internal.h"

#define N_WORKERS 3
#define N_ROUNDS 4
#define DEFAULT_RENAME_ROUNDS 50

/* Retry budgets here are wall clock, not attempt counts. An attempt count is
** not a bound on anything: the same 600 attempts are seconds when contention
** resolves quickly and tens of minutes when each one blocks out a busy
** timeout, so it cannot express "keep trying for a while, then say so".
** Under ThreadSanitizer, which runs the same work roughly 10x slower on a
** two-core runner, that ambiguity is the difference between passing and a
** silent give-up. threadtest5 buys the same headroom with THREADTEST5_BUSY_MS. */
#define STRESS_BUSY_MS_DEFAULT 60000

static sqlite3_int64 nowMs(void){
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (sqlite3_int64)ts.tv_sec*1000 + ts.tv_nsec/1000000;
}

static int stressBusyMs(void){
  static int ms = 0;
  if( ms==0 ){
    const char *z = getenv("DOLTLITE_STRESS_BUSY_MS");
    ms = z ? atoi(z) : STRESS_BUSY_MS_DEFAULT;
    if( ms<1000 ) ms = STRESS_BUSY_MS_DEFAULT;
  }
  return ms;
}

typedef struct Budget Budget;
struct Budget {
  sqlite3_int64 deadline;
  int attempts;
};

/* Budgets nest -- commitBranchRow retries around execSqlWithRetry, which
** retries around a statement -- so a per-call budget alone bounds nothing: a
** worker gets one per operation per round, and the product can outrun the CI
** job's own timeout, turning a legible failure into a killed job. Each phase
** therefore caps the budgets inside it, so the whole test is bounded by the
** number of phases rather than by how deeply its retries happen to nest. */
static sqlite3_int64 gPhaseDeadline = 0;

static void startPhase(void){
  gPhaseDeadline = nowMs() + stressBusyMs();
}

static void budgetStart(Budget *p){
  sqlite3_int64 deadline = nowMs() + stressBusyMs();
  if( gPhaseDeadline && deadline>gPhaseDeadline ) deadline = gPhaseDeadline;
  p->deadline = deadline;
  p->attempts = 0;
}

static void budgetStartWithFloor(Budget *p, int floorMs){
  budgetStart(p);
  if( nowMs() >= p->deadline ) p->deadline = nowMs() + floorMs;
}

static int budgetLive(Budget *p){
  p->attempts++;
  return nowMs() < p->deadline;
}

/* Every give-up path reports itself. A bare SQLITE_BUSY out of a retry loop
** reaches the caller with nothing set on the handle, so the worker used to
** report "rc=5 msg=not an error" without naming what it had been waiting on.
** budgetLive() counts the check that ends the loop, so zero attempts here
** means the phase budget was already spent before this call ran at all. */
static int budgetSpent(Budget *p, const char *zOp, int rc, const char *zMsg){
  fprintf(stderr, "gave up on %s after %d attempts in %dms; last rc=%d msg=%s\n",
          zOp, p->attempts - 1, stressBusyMs(), rc,
          zMsg && zMsg[0] ? zMsg : "(none)");
  return SQLITE_BUSY;
}

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

static void cleanupDb(const char *path){
  char wal[256];
  char shm[256];
  remove(path);
  snprintf(wal, sizeof(wal), "%s-wal", path);
  remove(wal);
  snprintf(shm, sizeof(shm), "%s-shm", path);
  remove(shm);
}

static int isRetryableRc(int rc){
  return rc==SQLITE_BUSY || rc==SQLITE_LOCKED || rc==SQLITE_SCHEMA;
}

static int msgContains(const char *msg, const char *needle){
  return msg && strstr(msg, needle)!=0;
}

static int isRetryableMsg(const char *msg){
  return msgContains(msg, "busy")
      || msgContains(msg, "locked")
      || msgContains(msg, "schema has changed")
      || msgContains(msg, "failed to snapshot current branch state")
      || msgContains(msg, "unknown operation");
}

static int execSql(sqlite3 *db, const char *sql){
  char *err = 0;
  int rc = sqlite3_exec(db, sql, 0, 0, &err);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "SQL error rc=%d: %s\n  SQL: %s\n",
            rc, err ? err : sqlite3_errmsg(db), sql);
  }
  sqlite3_free(err);
  return rc;
}

static int execSqlWithRetry(sqlite3 *db, const char *sql){
  Budget budget;
  char last[256];
  int lastRc = SQLITE_OK;
  last[0] = 0;
  budgetStart(&budget);
  while( budgetLive(&budget) ){
    char *err = 0;
    int rc = sqlite3_exec(db, sql, 0, 0, &err);
    const char *msg = err ? err : sqlite3_errmsg(db);
    if( rc==SQLITE_OK ){
      sqlite3_free(err);
      return SQLITE_OK;
    }
    if( !isRetryableRc(rc) && !isRetryableMsg(msg) ){
      fprintf(stderr, "SQL error rc=%d: %s\n  SQL: %s\n", rc, msg, sql);
      sqlite3_free(err);
      return rc;
    }
    lastRc = rc;
    snprintf(last, sizeof(last), "%s", msg ? msg : "");
    sqlite3_free(err);
    sqlite3_sleep(5);
  }
  fprintf(stderr, "  SQL: %s\n", sql);
  return budgetSpent(&budget, "execSqlWithRetry", lastRc, last);
}

static int queryTextWithRetry(sqlite3 *db, const char *sql, char *out, int nOut){
  Budget budget;
  char last[256];
  int lastRc = SQLITE_OK;
  out[0] = 0;
  last[0] = 0;
  budgetStart(&budget);
  while( budgetLive(&budget) ){
    sqlite3_stmt *stmt = 0;
    int rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);
    if( rc!=SQLITE_OK ){
      if( isRetryableRc(rc) || isRetryableMsg(sqlite3_errmsg(db)) ){
        lastRc = rc;
        snprintf(last, sizeof(last), "%s", sqlite3_errmsg(db));
        sqlite3_sleep(5);
        continue;
      }
      snprintf(out, nOut, "ERROR: %s", sqlite3_errmsg(db));
      return rc;
    }
    rc = sqlite3_step(stmt);
    if( rc==SQLITE_ROW ){
      const char *v = (const char*)sqlite3_column_text(stmt, 0);
      if( v ) snprintf(out, nOut, "%s", v);
      sqlite3_finalize(stmt);
      return SQLITE_OK;
    }
    sqlite3_finalize(stmt);
    if( rc==SQLITE_DONE ) return SQLITE_OK;
    if( !isRetryableRc(rc) && !isRetryableMsg(sqlite3_errmsg(db)) ){
      snprintf(out, nOut, "%s", sqlite3_errmsg(db));
      return rc;
    }
    lastRc = rc;
    snprintf(last, sizeof(last), "%s", sqlite3_errmsg(db));
    sqlite3_sleep(5);
  }
  snprintf(out, nOut, "BUSY");
  fprintf(stderr, "  SQL: %s\n", sql);
  return budgetSpent(&budget, "queryTextWithRetry", lastRc, last);
}

static int queryIntWithRetry(sqlite3 *db, const char *sql, int *pOut){
  char buf[128];
  int rc = queryTextWithRetry(db, sql, buf, sizeof(buf));
  if( rc==SQLITE_OK ) *pOut = atoi(buf);
  return rc;
}

/* The worker reopens between rounds, and an open that loses the race for the
** store lock is as retryable as any other step here. Left unretried it was
** also the only way out of a worker loop that reported no error at all: it
** returns straight through the caller, and the fresh handle it leaves behind
** answers sqlite3_errmsg() with "not an error". */
static int reopenDb(const char *path, sqlite3 **pDb){
  Budget budget;
  int rc = SQLITE_OK;
  char last[256];
  last[0] = 0;
  if( *pDb ){
    sqlite3_close(*pDb);
    *pDb = 0;
  }
  budgetStartWithFloor(&budget, 2000);
  while( budgetLive(&budget) ){
    const char *msg;
    rc = sqlite3_open(path, pDb);
    if( rc==SQLITE_OK ){
      sqlite3_busy_timeout(*pDb, 5000);
      return SQLITE_OK;
    }
    msg = *pDb ? sqlite3_errmsg(*pDb) : 0;
    snprintf(last, sizeof(last), "%s", msg ? msg : "");
    if( !isRetryableRc(rc) && !isRetryableMsg(msg) ){
      fprintf(stderr, "open failed rc=%d: %s\n", rc, msg ? msg : "(none)");
      return rc;
    }
    sqlite3_close(*pDb);
    *pDb = 0;
    sqlite3_sleep(5);
  }
  return budgetSpent(&budget, "reopenDb", rc, last[0] ? last : "open never succeeded");
}

static int setupDb(const char *path){
  sqlite3 *db = 0;
  int rc;
  cleanupDb(path);
  rc = sqlite3_open(path, &db);
  if( rc!=SQLITE_OK ) return rc;
  sqlite3_busy_timeout(db, 5000);
  rc = execSql(db, "CREATE TABLE ref_rows(id INTEGER PRIMARY KEY, worker INT, round INT)");
  if( rc==SQLITE_OK ){
    rc = execSql(db, "INSERT INTO ref_rows VALUES(0, -1, -1)");
  }
  if( rc==SQLITE_OK ){
    rc = execSql(db, "SELECT dolt_commit('-A','-m','ref mutation init')");
  }
  sqlite3_close(db);
  return rc;
}

static int createBranch(sqlite3 *db, const char *zBranch){
  char sql[256];
  char out[256];
  int rc;
  snprintf(sql, sizeof(sql), "SELECT dolt_branch('%s')", zBranch);
  rc = queryTextWithRetry(db, sql, out, sizeof(out));
  if( rc==SQLITE_OK ) return SQLITE_OK;
  if( msgContains(out, "branch already exists") ) return SQLITE_OK;
  return rc;
}

static int commitBranchRow(sqlite3 **pDb, const char *path, int worker, int round){
  sqlite3 *db = *pDb;
  char branch[64];
  char sql[256];
  char out[256];
  int rowid = worker*1000 + round;
  Budget budget;
  out[0] = 0;
  snprintf(branch, sizeof(branch), "stress_w%d_r%d", worker, round);

  budgetStart(&budget);
  while( budgetLive(&budget) ){
    int rc;
    rc = createBranch(db, branch);
    if( rc!=SQLITE_OK ) goto retry;
    snprintf(sql, sizeof(sql), "SELECT dolt_checkout('%s')", branch);
    rc = execSqlWithRetry(db, sql);
    if( rc!=SQLITE_OK ) goto retry;
    snprintf(sql, sizeof(sql),
             "INSERT OR IGNORE INTO ref_rows VALUES(%d, %d, %d)",
             rowid, worker, round);
    rc = execSqlWithRetry(db, sql);
    if( rc!=SQLITE_OK ) goto retry;
    snprintf(sql, sizeof(sql),
             "SELECT dolt_commit('-A','-m','%s commit')", branch);
    rc = queryTextWithRetry(db, sql, out, sizeof(out));
    if( rc==SQLITE_OK && strlen(out)==40 ) return SQLITE_OK;
    if( msgContains(out, "nothing to commit, working tree clean") ){
      return SQLITE_OK;
    }
retry:
    sqlite3_sleep(5);
    rc = reopenDb(path, pDb);
    if( rc!=SQLITE_OK ) return rc;
    db = *pDb;
  }
  return budgetSpent(&budget, "commitBranchRow", SQLITE_BUSY, out);
}

static int mergeBranchToMain(sqlite3 **pDb, const char *path, int worker, int round){
  sqlite3 *db = *pDb;
  char branch[64];
  char sql[256];
  char out[256];
  int rowid = worker*1000 + round;
  int count = 0;
  Budget budget;
  out[0] = 0;
  snprintf(branch, sizeof(branch), "stress_w%d_r%d", worker, round);

  budgetStart(&budget);
  while( budgetLive(&budget) ){
    int rc = execSqlWithRetry(db, "SELECT dolt_checkout('main')");
    if( rc==SQLITE_OK ){
      snprintf(sql, sizeof(sql),
               "SELECT count(*) FROM ref_rows WHERE id=%d", rowid);
      rc = queryIntWithRetry(db, sql, &count);
      if( rc==SQLITE_OK && count==1 ) return SQLITE_OK;
      snprintf(sql, sizeof(sql), "SELECT dolt_merge('%s')", branch);
      rc = queryTextWithRetry(db, sql, out, sizeof(out));
      if( rc==SQLITE_OK
       && (strlen(out)==40 || msgContains(out, "Already up to date")) ){
        /* Never treat a merge result as success unless the worker row is
        ** actually on main — a lost-update CAS clobber used to return a
        ** 40-char hash while dropping a peer's already-merged rows. */
        count = 0;
        snprintf(sql, sizeof(sql),
                 "SELECT count(*) FROM ref_rows WHERE id=%d", rowid);
        rc = queryIntWithRetry(db, sql, &count);
        if( rc==SQLITE_OK && count==1 ) return SQLITE_OK;
      }
    }
    sqlite3_sleep(5);
    rc = reopenDb(path, pDb);
    if( rc!=SQLITE_OK ) return rc;
    db = *pDb;
  }
  return budgetSpent(&budget, "mergeBranchToMain", SQLITE_BUSY, out);
}

static int deleteBranch(sqlite3 *db, const char *zBranch){
  char sql[256];
  char last[256];
  int lastRc = SQLITE_OK;
  Budget budget;
  last[0] = 0;
  snprintf(sql, sizeof(sql), "SELECT dolt_branch('-d','%s')", zBranch);
  budgetStart(&budget);
  while( budgetLive(&budget) ){
    char *err = 0;
    int rc = sqlite3_exec(db, sql, 0, 0, &err);
    const char *msg = err ? err : sqlite3_errmsg(db);
    if( rc==SQLITE_OK || msgContains(msg, "branch not found") ){
      sqlite3_free(err);
      return SQLITE_OK;
    }
    if( !isRetryableRc(rc) && !isRetryableMsg(msg) ){
      fprintf(stderr, "delete branch failed rc=%d: %s\n", rc, msg);
      sqlite3_free(err);
      return rc;
    }
    lastRc = rc;
    snprintf(last, sizeof(last), "%s", msg ? msg : "");
    sqlite3_free(err);
    sqlite3_sleep(5);
  }
  return budgetSpent(&budget, "deleteBranch", lastRc, last);
}

static int runWorker(const char *path, int worker){
  sqlite3 *db = 0;
  int rc;
  int round;
  rc = reopenDb(path, &db);
  if( rc!=SQLITE_OK ) return 10;

  for( round=1; round<=N_ROUNDS; round++ ){
    char branch[64];
    startPhase();
    snprintf(branch, sizeof(branch), "stress_w%d_r%d", worker, round);
    rc = commitBranchRow(&db, path, worker, round);
    if( rc!=SQLITE_OK ) goto worker_error;
    rc = mergeBranchToMain(&db, path, worker, round);
    if( rc!=SQLITE_OK ) goto worker_error;
    rc = execSqlWithRetry(db, "SELECT dolt_checkout('main')");
    if( rc!=SQLITE_OK ) goto worker_error;
    rc = deleteBranch(db, branch);
    if( rc!=SQLITE_OK ) goto worker_error;
  }

  sqlite3_close(db);
  return 0;

worker_error:
  fprintf(stderr, "worker %d failed rc=%d msg=%s\n",
          worker, rc, db ? sqlite3_errmsg(db) : "no live connection");
  sqlite3_close(db);
  return 1;
}

static void verifyFinalState(const char *path){
  sqlite3 *db = 0;
  int rc;
  int count = 0;
  char msg[256];

  startPhase();
  rc = sqlite3_open(path, &db);
  check("verify_open", rc==SQLITE_OK);
  sqlite3_busy_timeout(db, 5000);

  rc = execSqlWithRetry(db, "SELECT dolt_connect_branch('main')");
  check("verify_checkout_main", rc==SQLITE_OK);
  rc = queryIntWithRetry(db, "SELECT count(*) FROM ref_rows", &count);
  check("verify_all_rows_merged",
        rc==SQLITE_OK && count==1 + N_WORKERS*N_ROUNDS);
  rc = queryIntWithRetry(db,
    "SELECT count(*) FROM ref_rows WHERE worker>=0", &count);
  check("verify_worker_rows_merged", rc==SQLITE_OK && count==N_WORKERS*N_ROUNDS);
  rc = queryIntWithRetry(db,
    "SELECT count(*) FROM dolt_branches WHERE name='main'", &count);
  check("verify_main_branch_exists", rc==SQLITE_OK && count==1);
  rc = queryIntWithRetry(db,
    "SELECT count(*) FROM dolt_branches WHERE name LIKE 'stress_w%'", &count);
  check("verify_temp_branches_deleted", rc==SQLITE_OK && count==0);
  rc = queryTextWithRetry(db, "SELECT message FROM dolt_log LIMIT 1",
                          msg, sizeof(msg));
  check("verify_log_readable", rc==SQLITE_OK && msg[0]!=0);

  sqlite3_close(db);
}

static int pipeSend(int fd, char value){
  return write(fd, &value, 1)==1 ? 0 : 1;
}

static int pipeReceive(int fd, char *pValue){
  return read(fd, pValue, 1)==1 ? 0 : 1;
}

static int setupDefaultRenameDb(const char *path){
  sqlite3 *db = 0;
  int rc;
  cleanupDb(path);
  rc = sqlite3_open(path, &db);
  if( rc==SQLITE_OK ){
    rc = execSql(db,
      "CREATE TABLE t(id INTEGER PRIMARY KEY);"
      "SELECT dolt_commit('-A','-m','init');"
      "SELECT dolt_branch('feat')");
  }
  sqlite3_close(db);
  return rc;
}

static int runDefaultSetterChild(const char *path, int readFd, int writeFd){
  sqlite3 *db = 0;
  char value;
  char out[128];
  int i;
  int rc = sqlite3_open(path, &db);
  if( rc!=SQLITE_OK ) return 1;
  sqlite3_busy_timeout(db, 5000);
  for(i=0; i<DEFAULT_RENAME_ROUNDS; i++){
    if( pipeReceive(readFd, &value) ){
      sqlite3_close(db);
      return 1;
    }
    rc = queryTextWithRetry(db, "SELECT dolt_default_branch('feat')",
                            out, sizeof(out));
    value = rc==SQLITE_OK && strcmp(out, "0")==0 ? '1' : '0';
    if( pipeSend(writeFd, value) ){
      sqlite3_close(db);
      return 1;
    }
    if( value!='1' ){
      sqlite3_close(db);
      return 1;
    }
  }
  sqlite3_close(db);
  return 0;
}

static void runDefaultRenameStress(void){
  const char *path = "/tmp/test_vc_default_rename_stress.db";
  sqlite3 *db = 0;
  int toChild[2];
  int fromChild[2];
  pid_t pid;
  int status = 0;
  int i;
  const char *zSrc = "main";
  const char *zDest = "trunk";
  char out[128];

  startPhase();
  check("default_rename_setup", setupDefaultRenameDb(path)==SQLITE_OK);
  check("default_rename_open", sqlite3_open(path, &db)==SQLITE_OK);
  sqlite3_busy_timeout(db, 5000);
  check("default_rename_initial_default",
        queryTextWithRetry(db, "SELECT dolt_default_branch()",
                           out, sizeof(out))==SQLITE_OK
        && strcmp(out, "main")==0);
  check("default_rename_pipe_to_child", pipe(toChild)==0);
  check("default_rename_pipe_from_child", pipe(fromChild)==0);
  pid = fork();
  check("default_rename_fork", pid>=0);
  if( pid==0 ){
    close(toChild[1]);
    close(fromChild[0]);
    _exit(runDefaultSetterChild(path, toChild[0], fromChild[1]));
  }
  close(toChild[0]);
  close(fromChild[1]);

  for(i=0; i<DEFAULT_RENAME_ROUNDS && pid>0; i++){
    char value = 0;
    char sql[256];
    int rc;
    check("default_rename_signal_setter", pipeSend(toChild[1], '1')==0);
    check("default_rename_setter_completed",
          pipeReceive(fromChild[0], &value)==0 && value=='1');
    snprintf(sql, sizeof(sql),
             "SELECT dolt_branch('-m','%s','%s')", zSrc, zDest);
    rc = queryTextWithRetry(db, sql, out, sizeof(out));
    check("default_rename_move_completed",
          rc==SQLITE_OK && strcmp(out, "0")==0);
    rc = queryTextWithRetry(db, "SELECT dolt_default_branch()",
                            out, sizeof(out));
    check("default_rename_preserves_peer_default",
          rc==SQLITE_OK && strcmp(out, "feat")==0);
    snprintf(sql, sizeof(sql),
             "SELECT dolt_default_branch('%s')", zDest);
    rc = queryTextWithRetry(db, sql, out, sizeof(out));
    check("default_rename_prepares_next_round",
          rc==SQLITE_OK && strcmp(out, "0")==0);
    {
      const char *zTmp = zSrc;
      zSrc = zDest;
      zDest = zTmp;
    }
  }

  close(toChild[1]);
  close(fromChild[0]);
  if( pid>0 ) waitpid(pid, &status, 0);
  check("default_rename_setter_exited_cleanly",
        pid>0 && WIFEXITED(status) && WEXITSTATUS(status)==0);
  sqlite3_close(db);
  cleanupDb(path);
}

typedef struct FailingMutationCtx FailingMutationCtx;
struct FailingMutationCtx {
  const char *zBranch;
  ProllyHash head;
};

static int failAfterBranchAdd(sqlite3 *db, ChunkStore *cs, void *pArg){
  FailingMutationCtx *p = (FailingMutationCtx*)pArg;
  int rc;
  (void)db;
  rc = chunkStoreAddBranch(cs, p->zBranch, &p->head);
  return rc==SQLITE_OK ? SQLITE_IOERR : rc;
}

static int addMutationBranch(sqlite3 *db, ChunkStore *cs, void *pArg){
  FailingMutationCtx *p = (FailingMutationCtx*)pArg;
  (void)db;
  return chunkStoreAddBranch(cs, p->zBranch, &p->head);
}

static void runAtomicMutationTests(void){
  const char *path = "/tmp/test_vc_atomic_mutation.db";
  sqlite3 *db1 = 0;
  sqlite3 *db2 = 0;
  FailingMutationCtx mutation;
  DoltliteBranchExpectation expected;
  int count = 0;
  int rc;

  startPhase();
  check("atomic_mutation_setup", setupDb(path)==SQLITE_OK);
  check("atomic_mutation_open_first", sqlite3_open(path, &db1)==SQLITE_OK);
  sqlite3_busy_timeout(db1, 5000);
  doltliteGetSessionHead(db1, &mutation.head);
  mutation.zBranch = "rollback_probe";
  rc = doltliteMutateRefs(db1, failAfterBranchAdd, &mutation);
  check("atomic_mutation_surfaces_callback_failure", rc==SQLITE_IOERR);
  check("atomic_mutation_restores_in_memory_refs",
        queryIntWithRetry(db1,
          "SELECT count(*) FROM dolt_branches WHERE name='rollback_probe'",
          &count)==SQLITE_OK && count==0);
  sqlite3_close(db1);
  db1 = 0;
  check("atomic_mutation_reopen_first", sqlite3_open(path, &db1)==SQLITE_OK);
  check("atomic_mutation_failure_not_persisted",
        queryIntWithRetry(db1,
          "SELECT count(*) FROM dolt_branches WHERE name='rollback_probe'",
          &count)==SQLITE_OK && count==0);

  check("atomic_mutation_open_peer", sqlite3_open(path, &db2)==SQLITE_OK);
  sqlite3_busy_timeout(db1, 5000);
  sqlite3_busy_timeout(db2, 5000);
  doltliteGetSessionHead(db1, &mutation.head);
  check("atomic_mutation_peer_insert",
        execSql(db2, "INSERT INTO ref_rows VALUES(99, 99, 99)")==SQLITE_OK);
  check("atomic_mutation_peer_commit",
        execSql(db2, "SELECT dolt_commit('-A','-m','peer advance')")==SQLITE_OK);
  mutation.zBranch = "cas_probe";
  expected.zBranch = "main";
  expected.pTip = &mutation.head;
  rc = doltliteMutateRefsExpected(
      db1, &expected, 1, addMutationBranch, &mutation);
  check("atomic_mutation_rejects_stale_expected_tip", rc==SQLITE_BUSY);
  check("atomic_mutation_rejected_ref_absent",
        queryIntWithRetry(db1,
          "SELECT count(*) FROM dolt_branches WHERE name='cas_probe'",
          &count)==SQLITE_OK && count==0);
  sqlite3_close(db2);
  sqlite3_close(db1);
  cleanupDb(path);
}

int main(void){
  const char *path = "/tmp/test_vc_ref_mutation_stress.db";
  pid_t pids[N_WORKERS];
  int status;
  int i;

  setvbuf(stdout, 0, _IOLBF, 0);
  printf("=== VC Ref Mutation Stress Test ===\n\n");
  check("setup_db", setupDb(path)==SQLITE_OK);

  for( i=0; i<N_WORKERS; i++ ){
    pids[i] = fork();
    if( pids[i]==0 ) _exit(runWorker(path, i));
    check("fork_worker", pids[i]>=0);
  }

  for( i=0; i<N_WORKERS; i++ ){
    if( pids[i]<0 ) continue;
    status = 0;
    waitpid(pids[i], &status, 0);
    check("worker_exited_cleanly",
          WIFEXITED(status) && WEXITSTATUS(status)==0);
  }

  verifyFinalState(path);
  cleanupDb(path);
  runDefaultRenameStress();
  runAtomicMutationTests();

  printf("\nResults: %d passed, %d failed out of %d tests\n",
         nPass, nFail, nPass+nFail);
  return nFail>0 ? 1 : 0;
}
