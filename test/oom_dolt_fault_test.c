#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include "sqlite3.h"
#include "doltlite_internal.h"

#ifndef MAX_FAIL_N
# define MAX_FAIL_N 500
#endif

static int nPass = 0;
static int nFail = 0;

typedef struct OomState {
  long long nCall;
  long long nFailAt;
  int       triggered;
} OomState;

static OomState gOom;
static sqlite3_mem_methods gReal;

static void *oomMalloc(int n){
  long long c;
  c = ++gOom.nCall;
  if( c==gOom.nFailAt ){
    gOom.triggered = 1;
    return 0;
  }
  return gReal.xMalloc(n);
}

static void oomFree(void *p){
  if( p==0 ) return;
  gReal.xFree(p);
}

static void *oomRealloc(void *p, int n){
  long long c;
  c = ++gOom.nCall;
  if( c==gOom.nFailAt ){
    gOom.triggered = 1;
    return 0;
  }
  return gReal.xRealloc(p, n);
}

static int oomSize(void *p){ return gReal.xSize(p); }
static int oomRoundup(int n){ return gReal.xRoundup(n); }
static int oomInit(void *p){ return gReal.xInit ? gReal.xInit(p) : SQLITE_OK; }
static void oomShutdown(void *p){ if(gReal.xShutdown) gReal.xShutdown(p); }

static sqlite3_mem_methods gWrap = {
  oomMalloc, oomFree, oomRealloc, oomSize, oomRoundup, oomInit, oomShutdown, 0
};

static int installOomAllocator(void){
  int rc = sqlite3_config(SQLITE_CONFIG_GETMALLOC, &gReal);
  if( rc!=SQLITE_OK ) return rc;
  rc = sqlite3_config(SQLITE_CONFIG_MALLOC, &gWrap);
  return rc;
}

static void resetOom(long long failAt){
  gOom.nCall = 0;
  gOom.nFailAt = failAt;
  gOom.triggered = 0;
}

static void disableOom(void){
  resetOom(-1);
}

static int isAcceptableErr(int rc){
  if( rc==SQLITE_OK ) return 1;
  if( rc==SQLITE_NOMEM ) return 1;
  if( rc==SQLITE_ERROR ) return 1;
  if( rc==SQLITE_BUSY ) return 1;
  if( rc==SQLITE_FULL ) return 1;
  if( rc==SQLITE_CONSTRAINT ) return 1;
  if( rc==SQLITE_MISUSE ) return 1;
  if( rc==SQLITE_IOERR_NOMEM ) return 1;
  if( rc==SQLITE_INTERRUPT ) return 1;
  if( rc==SQLITE_ABORT ) return 1;
  if( (rc&0xFF)==SQLITE_IOERR ) return 1;
  return 0;
}

static int execSilent(sqlite3 *db, const char *sql){
  char *err = 0;
  int rc = sqlite3_exec(db, sql, 0, 0, &err);
  if( err ) sqlite3_free(err);
  return rc;
}

static int runQuerySilent(sqlite3 *db, const char *sql){
  sqlite3_stmt *stmt = 0;
  int rc;
  int frc;
  rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);
  if( rc!=SQLITE_OK ){
    if( stmt ) sqlite3_finalize(stmt);
    return rc;
  }
  while( (rc = sqlite3_step(stmt))==SQLITE_ROW ){
    int n;
    int i;
    n = sqlite3_column_count(stmt);
    for( i=0; i<n; i++ ){
      (void)sqlite3_column_text(stmt, i);
    }
  }
  frc = sqlite3_finalize(stmt);
  if( rc==SQLITE_DONE ) return frc;
  return rc;
}

static void cleanupFiles(const char *base){
  char buf[512];
  remove(base);
  snprintf(buf, sizeof(buf), "%s-chunks", base);
  remove(buf);
  snprintf(buf, sizeof(buf), "%s-journal", base);
  remove(buf);
  snprintf(buf, sizeof(buf), "%s-wal", base);
  remove(buf);
  snprintf(buf, sizeof(buf), "%s-shm", base);
  remove(buf);
}

static int setupBase(sqlite3 *db){
  int rc;
  rc = execSilent(db,
      "SELECT dolt_config('user.name','oom-author'),"
      "       dolt_config('user.email','oom@example.com')");
  if( rc ) return rc;
  rc = execSilent(db, "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)");
  if( rc ) return rc;
  rc = execSilent(db, "INSERT INTO t VALUES(1,'one'),(2,'two'),(3,'three')");
  if( rc ) return rc;
  rc = execSilent(db, "SELECT dolt_add('-A')");
  if( rc ) return rc;
  rc = execSilent(db, "SELECT dolt_commit('-m','base')");
  return rc;
}

typedef int (*OpFn)(sqlite3 *db);
typedef int (*OpSetupFn)(sqlite3 *db);

static ProllyHash gSavepointPreRebase;
static ProllyHash gSavepointRebaseOnto;

static int verifyLatestCommitMetadata(sqlite3 *db){
  sqlite3_stmt *stmt = 0;
  const char *zName;
  const char *zEmail;
  const char *zMessage;
  int rc;
  int frc;
  rc = sqlite3_prepare_v2(db,
      "SELECT committer,email,message FROM dolt_log LIMIT 1",
      -1, &stmt, 0);
  if( rc!=SQLITE_OK ) return rc;
  rc = sqlite3_step(stmt);
  if( rc==SQLITE_ROW ){
    zName = (const char*)sqlite3_column_text(stmt, 0);
    zEmail = (const char*)sqlite3_column_text(stmt, 1);
    zMessage = (const char*)sqlite3_column_text(stmt, 2);
    rc = zName && strcmp(zName, "oom-author")==0
      && zEmail && strcmp(zEmail, "oom@example.com")==0
      && zMessage && strcmp(zMessage, "iter")==0
      ? SQLITE_OK : SQLITE_CORRUPT;
  }else if( rc==SQLITE_DONE ){
    rc = SQLITE_CORRUPT;
  }
  frc = sqlite3_finalize(stmt);
  return rc==SQLITE_OK ? frc : rc;
}

static int opCommit(sqlite3 *db){
  int rc;
  rc = execSilent(db, "INSERT INTO t VALUES(4,'four')");
  if( rc ) return rc;
  rc = execSilent(db, "SELECT dolt_add('-A')");
  if( rc ) return rc;
  rc = execSilent(db, "SELECT dolt_commit('-m','iter')");
  if( rc ) return rc;
  return verifyLatestCommitMetadata(db);
}

static int opBranchCreate(sqlite3 *db){
  return execSilent(db, "SELECT dolt_branch('br_iter')");
}

static int opBranchDelete(sqlite3 *db){
  int rc = execSilent(db, "SELECT dolt_branch('br_del')");
  if( rc ) return rc;
  return execSilent(db, "SELECT dolt_branch('-D','br_del')");
}

static int opCheckout(sqlite3 *db){
  int rc = execSilent(db, "SELECT dolt_branch('br_co')");
  if( rc ) return rc;
  rc = execSilent(db, "SELECT dolt_checkout('br_co')");
  if( rc ) return rc;
  return execSilent(db, "SELECT dolt_checkout('main')");
}

static int opLogScan(sqlite3 *db){
  return runQuerySilent(db,
    "SELECT commit_hash, message, committer FROM dolt_log LIMIT 32");
}

static int opMerge(sqlite3 *db){
  int rc;
  rc = execSilent(db, "SELECT dolt_branch('br_merge')");
  if( rc ) return rc;
  rc = execSilent(db, "SELECT dolt_checkout('br_merge')");
  if( rc ) return rc;
  rc = execSilent(db, "INSERT INTO t VALUES(99,'merge-branch')");
  if( rc ) return rc;
  rc = execSilent(db, "SELECT dolt_add('-A')");
  if( rc ) return rc;
  rc = execSilent(db, "SELECT dolt_commit('-m','for-merge')");
  if( rc ) return rc;
  rc = execSilent(db, "SELECT dolt_checkout('main')");
  if( rc ) return rc;
  return execSilent(db, "SELECT dolt_merge('br_merge')");
}

static int opDiffTable(sqlite3 *db){
  int rc = execSilent(db, "INSERT INTO t VALUES(50,'diffed')");
  if( rc ) return rc;
  return runQuerySilent(db,
    "SELECT to_a, to_b, diff_type FROM dolt_diff_t LIMIT 16");
}

static int setupSavepointState(sqlite3 *db){
  int rc;
  rc = doltliteGetHeadCatalogHash(db, &gSavepointPreRebase);
  if( rc!=SQLITE_OK ) return rc;
  doltliteGetSessionHead(db, &gSavepointRebaseOnto);
  return doltliteSetSessionRebaseState(db, 1, &gSavepointPreRebase,
                                       &gSavepointRebaseOnto, "main", "main");
}

static int opSavepointState(sqlite3 *db){
  ProllyHash stagedBefore;
  ProllyHash stagedAfter;
  ProllyHash preRebase;
  ProllyHash rebaseOnto;
  const char *zOrig = 0;
  const char *zReturn = 0;
  u8 isRebasing = 0;
  int stateRestored;
  int rc;

  doltliteGetSessionStaged(db, &stagedBefore);
  rc = execSilent(db, "INSERT INTO t VALUES(4,'four')");
  if( rc ) return rc;
  rc = execSilent(db, "BEGIN IMMEDIATE; SAVEPOINT state_sp");
  if( rc ) return rc;
  rc = execSilent(db, "SELECT dolt_add('-A')");
  if( rc ) return rc;
  rc = execSilent(db, "ROLLBACK TO state_sp");
  if( rc ) return rc;

  doltliteGetSessionStaged(db, &stagedAfter);
  doltliteGetSessionRebaseState(db, &isRebasing, &preRebase, &rebaseOnto,
                               &zOrig, &zReturn);
  stateRestored =
      memcmp(&stagedAfter, &stagedBefore, sizeof(stagedAfter))==0
   && isRebasing
   && memcmp(&preRebase, &gSavepointPreRebase, sizeof(preRebase))==0
   && memcmp(&rebaseOnto, &gSavepointRebaseOnto, sizeof(rebaseOnto))==0
   && zOrig && strcmp(zOrig, "main")==0
   && zReturn && strcmp(zReturn, "main")==0;
  (void)execSilent(db, "ROLLBACK");
  return stateRestored ? SQLITE_OK : SQLITE_CORRUPT;
}

static int opRebaseStateSet(sqlite3 *db){
  ProllyHash newPreRebase;
  ProllyHash newRebaseOnto;
  ProllyHash actualPreRebase;
  ProllyHash actualRebaseOnto;
  const char *zOrig = 0;
  const char *zReturn = 0;
  u8 isRebasing = 0;
  int rc;
  int stateValid;

  memset(&newPreRebase, 0x91, sizeof(newPreRebase));
  memset(&newRebaseOnto, 0x92, sizeof(newRebaseOnto));
  rc = doltliteSetSessionRebaseState(db, 1, &newPreRebase, &newRebaseOnto,
                                     "new-orig", "new-return");
  doltliteGetSessionRebaseState(db, &isRebasing, &actualPreRebase,
                               &actualRebaseOnto, &zOrig, &zReturn);
  if( rc==SQLITE_OK ){
    stateValid = isRebasing
      && memcmp(&actualPreRebase, &newPreRebase, sizeof(newPreRebase))==0
      && memcmp(&actualRebaseOnto, &newRebaseOnto, sizeof(newRebaseOnto))==0
      && zOrig && strcmp(zOrig, "new-orig")==0
      && zReturn && strcmp(zReturn, "new-return")==0;
  }else{
    stateValid = isRebasing
      && memcmp(&actualPreRebase, &gSavepointPreRebase,
                sizeof(gSavepointPreRebase))==0
      && memcmp(&actualRebaseOnto, &gSavepointRebaseOnto,
                sizeof(gSavepointRebaseOnto))==0
      && zOrig && strcmp(zOrig, "main")==0
      && zReturn && strcmp(zReturn, "main")==0;
  }
  return stateValid ? rc : SQLITE_CORRUPT;
}

typedef struct OpEntry {
  const char *name;
  OpFn fn;
  OpSetupFn setup;
} OpEntry;

static OpEntry kOps[] = {
  { "dolt_commit",         opCommit, 0 },
  { "dolt_branch_create",  opBranchCreate, 0 },
  { "dolt_branch_delete",  opBranchDelete, 0 },
  { "dolt_checkout",       opCheckout, 0 },
  { "dolt_log_scan",       opLogScan, 0 },
  { "dolt_merge",          opMerge, 0 },
  { "dolt_diff_table",     opDiffTable, 0 },
  { "savepoint_vc_state",  opSavepointState, setupSavepointState },
  { "rebase_state_set",    opRebaseStateSet, setupSavepointState },
};
#define N_OPS (int)(sizeof(kOps)/sizeof(kOps[0]))

static const char *kDbBase = "/tmp/test_oom_dolt.db";

#define CHILD_OK 0
#define CHILD_ERR_OK 1
#define CHILD_BUG 2

static int childRunOp(OpEntry *op, long long failAt){
  sqlite3 *db = 0;
  int rc;
  int orc;
  int crc;
  long long triggered;
  long long ncall;
  cleanupFiles(kDbBase);
  disableOom();
  rc = sqlite3_open(kDbBase, &db);
  if( rc!=SQLITE_OK ){
    if( db ) sqlite3_close(db);
    cleanupFiles(kDbBase);
    return CHILD_BUG;
  }
  rc = setupBase(db);
  if( rc!=SQLITE_OK ){
    sqlite3_close(db);
    cleanupFiles(kDbBase);
    return CHILD_BUG;
  }
  if( op->setup ){
    rc = op->setup(db);
    if( rc!=SQLITE_OK ){
      sqlite3_close(db);
      cleanupFiles(kDbBase);
      return CHILD_BUG;
    }
  }
  resetOom(failAt);
  orc = op->fn(db);
  triggered = gOom.triggered;
  ncall = gOom.nCall;
  disableOom();
  crc = sqlite3_close(db);
  cleanupFiles(kDbBase);
  if( !isAcceptableErr(orc) ){
    fprintf(stderr, "  BUG[%s,N=%lld]: op rc=%d (not acceptable). triggered=%lld nCall=%lld closeRc=%d\n",
            op->name, failAt, orc, triggered, ncall, crc);
    return CHILD_BUG;
  }
  if( !isAcceptableErr(crc) && crc!=SQLITE_OK ){
    fprintf(stderr, "  BUG[%s,N=%lld]: close rc=%d after op rc=%d\n",
            op->name, failAt, crc, orc);
    return CHILD_BUG;
  }
  if( orc!=SQLITE_OK && triggered ){
    return CHILD_ERR_OK;
  }
  return CHILD_OK;
}

static int sweepOp(OpEntry *op){
  int opBugs = 0;
  int nOk = 0;
  int nErrOk = 0;
  long long nMax = MAX_FAIL_N;
  long long n;
  pid_t pid;
  pid_t r;
  int status;
  fflush(stdout);
  fflush(stderr);
  for( n=1; n<=nMax; n++ ){
    pid = fork();
    if( pid<0 ){
      fprintf(stderr, "  fork failed at N=%lld\n", n);
      return 1;
    }
    if( pid==0 ){
      int rc = childRunOp(op, n);
      fflush(stdout);
      fflush(stderr);
      _exit(rc);
    }
    status = 0;
    r = waitpid(pid, &status, 0);
    if( r<0 ){
      fprintf(stderr, "  waitpid failed at N=%lld\n", n);
      return 1;
    }
    if( WIFSIGNALED(status) ){
      int sig = WTERMSIG(status);
      fprintf(stderr, "  BUG[%s,N=%lld]: child killed by signal %d\n",
              op->name, n, sig);
      opBugs++;
    }else if( WIFEXITED(status) ){
      int ec = WEXITSTATUS(status);
      if( ec==CHILD_BUG ){
        opBugs++;
      }else if( ec==CHILD_ERR_OK ){
        nErrOk++;
      }else{
        nOk++;
      }
    }
  }
  printf("  [%s] swept N=1..%lld, ok=%d, errOk=%d, bugs=%d\n",
         op->name, n-1, nOk, nErrOk, opBugs);
  fflush(stdout);
  return opBugs;
}

int main(void){
  int rc = installOomAllocator();
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "Failed to install OOM allocator: rc=%d\n", rc);
    return 2;
  }
  disableOom();
  rc = sqlite3_initialize();
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "sqlite3_initialize failed: rc=%d\n", rc);
    return 2;
  }

  {
    int totalBugs = 0;
    int i;
    printf("OOM fault-injection sweep across %d operations, MAX_FAIL_N=%d\n",
           N_OPS, MAX_FAIL_N);
    for( i=0; i<N_OPS; i++ ){
      int opBugs;
      printf("[%d/%d] sweeping op: %s\n", i+1, N_OPS, kOps[i].name);
      opBugs = sweepOp(&kOps[i]);
      if( opBugs>0 ){
        nFail++;
      }else{
        nPass++;
      }
      totalBugs += opBugs;
    }
    printf("\n%d ops passed, %d ops surfaced bugs\n", nPass, nFail);
    printf("Total fault-iterations that surfaced bugs: %d\n", totalBugs);
  }
  return nFail>0 ? 1 : 0;
}
