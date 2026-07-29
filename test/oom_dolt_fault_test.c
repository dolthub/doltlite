#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include "sqlite3.h"
#include "doltlite_commit.h"
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

static int querySingleInt(sqlite3 *db, const char *sql, int *pValue){
  sqlite3_stmt *stmt = 0;
  int rc;
  int frc;
  rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);
  if( rc!=SQLITE_OK ) return rc;
  rc = sqlite3_step(stmt);
  if( rc==SQLITE_ROW ){
    *pValue = sqlite3_column_int(stmt, 0);
    rc = sqlite3_step(stmt)==SQLITE_DONE ? SQLITE_OK : SQLITE_CORRUPT;
  }else if( rc==SQLITE_DONE ){
    rc = SQLITE_CORRUPT;
  }
  frc = sqlite3_finalize(stmt);
  return rc==SQLITE_OK ? frc : rc;
}

static int querySingleText(
  sqlite3 *db,
  const char *sql,
  char *zOut,
  int nOut
){
  sqlite3_stmt *stmt = 0;
  const unsigned char *zValue;
  int rc;
  int frc;
  rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);
  if( rc!=SQLITE_OK ) return rc;
  rc = sqlite3_step(stmt);
  if( rc==SQLITE_ROW ){
    zValue = sqlite3_column_text(stmt, 0);
    if( !zValue || sqlite3_column_bytes(stmt, 0)>=nOut ){
      rc = SQLITE_CORRUPT;
    }else{
      memcpy(zOut, zValue, (size_t)sqlite3_column_bytes(stmt, 0)+1);
      rc = sqlite3_step(stmt)==SQLITE_DONE ? SQLITE_OK : SQLITE_CORRUPT;
    }
  }else if( rc==SQLITE_DONE ){
    rc = SQLITE_CORRUPT;
  }
  frc = sqlite3_finalize(stmt);
  return rc==SQLITE_OK ? frc : rc;
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

/* Divergent three-way merge: both sides insert rows so catalog pass1/pass2
** and row merge run. Setup leaves main ready to merge feat. */
static int setupThreeWayMerge(sqlite3 *db){
  int rc;
  rc = execSilent(db, "SELECT dolt_branch('feat')");
  if( rc ) return rc;
  rc = execSilent(db, "SELECT dolt_checkout('feat')");
  if( rc ) return rc;
  rc = execSilent(db, "INSERT INTO t VALUES(20,'feat')");
  if( rc ) return rc;
  rc = execSilent(db, "SELECT dolt_add('-A')");
  if( rc ) return rc;
  rc = execSilent(db, "SELECT dolt_commit('-m','feat-side')");
  if( rc ) return rc;
  rc = execSilent(db, "SELECT dolt_checkout('main')");
  if( rc ) return rc;
  rc = execSilent(db, "INSERT INTO t VALUES(10,'main')");
  if( rc ) return rc;
  rc = execSilent(db, "SELECT dolt_add('-A')");
  if( rc ) return rc;
  return execSilent(db, "SELECT dolt_commit('-m','main-side')");
}

static int opThreeWayMerge(sqlite3 *db){
  return execSilent(db, "SELECT dolt_merge('feat')");
}

static int setupLinearRebase(sqlite3 *db){
  int rc;
  rc = execSilent(db, "SELECT dolt_checkout('-b','feat')");
  if( rc ) return rc;
  rc = execSilent(db, "INSERT INTO t VALUES(4,'feat-one')");
  if( rc ) return rc;
  rc = execSilent(db, "SELECT dolt_add('-A')");
  if( rc ) return rc;
  rc = execSilent(db, "SELECT dolt_commit('-m','feat-one')");
  if( rc ) return rc;
  rc = execSilent(db, "INSERT INTO t VALUES(6,'feat-two')");
  if( rc ) return rc;
  rc = execSilent(db, "SELECT dolt_add('-A')");
  if( rc ) return rc;
  rc = execSilent(db, "SELECT dolt_commit('-m','feat-two')");
  if( rc ) return rc;
  rc = execSilent(db, "SELECT dolt_checkout('main')");
  if( rc ) return rc;
  rc = execSilent(db, "INSERT INTO t VALUES(5,'main')");
  if( rc ) return rc;
  rc = execSilent(db, "SELECT dolt_add('-A')");
  if( rc ) return rc;
  rc = execSilent(db, "SELECT dolt_commit('-m','main')");
  if( rc ) return rc;
  return execSilent(db, "SELECT dolt_checkout('feat')");
}

static int opLinearRebase(sqlite3 *db){
  return execSilent(db, "SELECT dolt_rebase('main')");
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
  const char *zOptionalBranch;
  int iOptionalRow;
  const char *zOptionalValue;
  int iOptionalRow2;
  const char *zOptionalValue2;
  const char *zOptionalHeadMessage;
  const char *zOptionalHeadMessage2;
  int allowRebaseState;
  const char *zActiveBranch;
  const char *zStateInvariantSql;
  int iOptionalRow3;
  const char *zOptionalValue3;
} OpEntry;

static OpEntry kOps[] = {
  { "dolt_commit",         opCommit, 0,
    0, 4, "four", 0, 0, "iter", 0, 0 },
  { "dolt_branch_create",  opBranchCreate, 0,
    "br_iter", 0, 0, 0, 0, 0, 0, 0 },
  { "dolt_branch_delete",  opBranchDelete, 0,
    "br_del", 0, 0, 0, 0, 0, 0, 0 },
  { "dolt_checkout",       opCheckout, 0,
    "br_co", 0, 0, 0, 0, 0, 0, 0 },
  { "dolt_log_scan",       opLogScan, 0,
    0, 0, 0, 0, 0, 0, 0, 0 },
  { "dolt_merge",          opMerge, 0,
    "br_merge", 99, "merge-branch", 0, 0, "for-merge", 0, 0 },
  /* Setup leaves HEAD at main-side; success advances to the merge commit. */
  { "dolt_three_way_merge", opThreeWayMerge, setupThreeWayMerge,
    "feat", 10, "main", 20, "feat",
    "Merge branch 'feat' into main", "main-side", 0 },
  { "dolt_rebase_linear",   opLinearRebase, setupLinearRebase,
    "feat", 4, "feat-one", 5, "main", "feat-two", 0, 0, "feat",
    "SELECT "
      "EXISTS(SELECT 1 FROM t WHERE a=4 AND b='feat-one') AND "
      "EXISTS(SELECT 1 FROM t WHERE a=6 AND b='feat-two') AND "
      "((EXISTS(SELECT 1 FROM t WHERE a=5 AND b='main')) OR "
       "(NOT EXISTS(SELECT 1 FROM t WHERE a=5))) AND "
      "NOT EXISTS(SELECT 1 FROM dolt_status)", 6, "feat-two" },
  { "dolt_diff_table",     opDiffTable, 0,
    0, 50, "diffed", 0, 0, 0, 0, 0 },
  { "savepoint_vc_state",  opSavepointState, setupSavepointState,
    0, 4, "four", 0, 0, 0, 0, 1 },
  { "rebase_state_set",    opRebaseStateSet, setupSavepointState,
    0, 0, 0, 0, 0, 0, 0, 1 },
};
#define N_OPS (int)(sizeof(kOps)/sizeof(kOps[0]))

static const char *kDbBase = "/tmp/test_oom_dolt.db";

#define CHILD_OK 0
#define CHILD_ERR_OK 1
#define CHILD_BUG 2

static int verifyCatalogHash(sqlite3 *db, const ProllyHash *pHash){
  struct TableEntry *aTables = 0;
  int nTables = 0;
  int rc;
  if( prollyHashIsEmpty(pHash) ) return SQLITE_CORRUPT;
  rc = doltliteLoadCatalog(db, pHash, &aTables, &nTables, 0);
  if( rc==SQLITE_OK && nTables<1 ) rc = SQLITE_CORRUPT;
  doltliteFreeCatalog(aTables, nTables);
  return rc;
}

static int verifyCommitHash(sqlite3 *db, const ProllyHash *pHash){
  DoltliteCommit c;
  int rc;
  if( prollyHashIsEmpty(pHash) ) return SQLITE_CORRUPT;
  memset(&c, 0, sizeof(c));
  rc = doltliteLoadCommit(db, pHash, &c);
  if( rc==SQLITE_OK ){
    if( !c.zName || !c.zName[0]
     || !c.zEmail || !c.zEmail[0]
     || !c.zMessage || !c.zMessage[0] ){
      rc = SQLITE_CORRUPT;
    }else{
      rc = verifyCatalogHash(db, &c.catalogHash);
    }
  }
  doltliteCommitClear(&c);
  return rc;
}

static int verifyReopenedState(
  sqlite3 *db,
  const OpEntry *op,
  const char **pzInvariant
){
  ProllyHash head;
  ProllyHash staged;
  ProllyHash mergeCommit;
  ProllyHash conflictsCatalog;
  ProllyHash preRebaseCatalog;
  ProllyHash rebaseOnto;
  const char *zOrig = 0;
  const char *zReturn = 0;
  sqlite3_stmt *stmt = 0;
  char zText[128];
  char *zSql = 0;
  u8 isMerging = 0;
  u8 isRebasing = 0;
  int n;
  int rc;
  int frc;

#define VERIFY_INVARIANT(label, expression) \
  do{ \
    rc = (expression); \
    if( rc!=SQLITE_OK ){ \
      *pzInvariant = (label); \
      goto verify_done; \
    } \
  }while(0)

  if( !sqlite3_get_autocommit(db) ){
    *pzInvariant = "reopen autocommit";
    return SQLITE_CORRUPT;
  }

  VERIFY_INVARIANT("integrity_check",
      querySingleText(db, "PRAGMA integrity_check", zText, sizeof(zText)));
  if( strcmp(zText, "ok")!=0 ){
    *pzInvariant = "integrity_check result";
    return SQLITE_CORRUPT;
  }

  if( op->zActiveBranch ){
    zSql = sqlite3_mprintf("SELECT dolt_checkout(%Q)", op->zActiveBranch);
    if( !zSql ){
      *pzInvariant = "verify branch SQL allocation";
      return SQLITE_NOMEM;
    }
    rc = execSilent(db, zSql);
    sqlite3_free(zSql);
    zSql = 0;
    if( rc!=SQLITE_OK ){
      *pzInvariant = "verify branch checkout";
      return rc;
    }
  }

  VERIFY_INVARIANT("active branch",
      querySingleText(db, "SELECT active_branch()", zText, sizeof(zText)));
  if( strcmp(zText, op->zActiveBranch ? op->zActiveBranch : "main")!=0 ){
    *pzInvariant = "active branch";
    return SQLITE_CORRUPT;
  }

  zSql = sqlite3_mprintf(
      "SELECT count(*) FROM dolt_branches "
      "WHERE name NOT IN ('main',%Q) "
      "OR name IS NULL OR name='' OR length(hash)!=40 "
      "OR latest_committer IS NULL OR latest_committer='' "
      "OR latest_committer_email IS NULL OR latest_committer_email='' "
      "OR latest_commit_message IS NULL OR latest_commit_message=''",
      op->zOptionalBranch ? op->zOptionalBranch : "main");
  if( !zSql ){
    *pzInvariant = "branch invariant SQL allocation";
    return SQLITE_NOMEM;
  }
  rc = querySingleInt(db, zSql, &n);
  sqlite3_free(zSql);
  zSql = 0;
  if( rc!=SQLITE_OK || n!=0 ){
    *pzInvariant = "branch refs and metadata";
    return rc==SQLITE_OK ? SQLITE_CORRUPT : rc;
  }
  VERIFY_INVARIANT("main branch count",
      querySingleInt(db,
        "SELECT count(*) FROM dolt_branches WHERE name='main'", &n));
  if( n!=1 ){
    *pzInvariant = "exactly one main branch";
    return SQLITE_CORRUPT;
  }
  zSql = sqlite3_mprintf(
      "SELECT count(*) FROM dolt_branches "
      "WHERE name=%Q AND hash=dolt_hashof('HEAD')",
      op->zActiveBranch ? op->zActiveBranch : "main");
  if( !zSql ){
    *pzInvariant = "HEAD/branch SQL allocation";
    return SQLITE_NOMEM;
  }
  rc = querySingleInt(db, zSql, &n);
  sqlite3_free(zSql);
  zSql = 0;
  if( rc!=SQLITE_OK ){
    *pzInvariant = "HEAD/branch equality query";
    return rc;
  }
  if( n!=1 ){
    *pzInvariant = "HEAD equals active branch";
    return SQLITE_CORRUPT;
  }

  doltliteGetSessionHead(db, &head);
  VERIFY_INVARIANT("HEAD commit graph", verifyCommitHash(db, &head));
  doltliteGetSessionStaged(db, &staged);
  VERIFY_INVARIANT("staged catalog", verifyCatalogHash(db, &staged));

  VERIFY_INVARIANT("log metadata scan",
      querySingleInt(db,
        "SELECT count(*) FROM dolt_log "
        "WHERE length(commit_hash)!=40 "
        "OR committer IS NULL OR committer='' "
        "OR message IS NULL OR message='' "
        "OR (message!='Initialize data repository' "
        "    AND (email IS NULL OR email=''))", &n));
  if( n!=0 ){
    *pzInvariant = "complete log metadata";
    return SQLITE_CORRUPT;
  }
  VERIFY_INVARIANT("log head message",
      querySingleText(db,
        "SELECT message FROM dolt_log LIMIT 1", zText, sizeof(zText)));
  if( strcmp(zText, "base")!=0
   && (!op->zOptionalHeadMessage
       || strcmp(zText, op->zOptionalHeadMessage)!=0)
   && (!op->zOptionalHeadMessage2
       || strcmp(zText, op->zOptionalHeadMessage2)!=0) ){
    *pzInvariant = "allowed HEAD message";
    return SQLITE_CORRUPT;
  }

  VERIFY_INVARIANT("base working rows",
      querySingleInt(db,
        "SELECT count(*) FROM t WHERE "
        "(a=1 AND b='one') OR "
        "(a=2 AND b='two') OR "
        "(a=3 AND b='three')", &n));
  if( n!=3 ){
    *pzInvariant = "base working rows preserved";
    return SQLITE_CORRUPT;
  }
  if( op->iOptionalRow>0 && op->iOptionalRow2>0 && op->iOptionalRow3>0 ){
    zSql = sqlite3_mprintf(
        "SELECT count(*) FROM t WHERE NOT ("
        "(a=1 AND b='one') OR "
        "(a=2 AND b='two') OR "
        "(a=3 AND b='three') OR "
        "(a=%d AND b=%Q) OR "
        "(a=%d AND b=%Q) OR "
        "(a=%d AND b=%Q))",
        op->iOptionalRow, op->zOptionalValue,
        op->iOptionalRow2, op->zOptionalValue2,
        op->iOptionalRow3, op->zOptionalValue3);
  }else if( op->iOptionalRow>0 && op->iOptionalRow2>0 ){
    zSql = sqlite3_mprintf(
        "SELECT count(*) FROM t WHERE NOT ("
        "(a=1 AND b='one') OR "
        "(a=2 AND b='two') OR "
        "(a=3 AND b='three') OR "
        "(a=%d AND b=%Q) OR "
        "(a=%d AND b=%Q))",
        op->iOptionalRow, op->zOptionalValue,
        op->iOptionalRow2, op->zOptionalValue2);
  }else if( op->iOptionalRow>0 ){
    zSql = sqlite3_mprintf(
        "SELECT count(*) FROM t WHERE NOT ("
        "(a=1 AND b='one') OR "
        "(a=2 AND b='two') OR "
        "(a=3 AND b='three') OR "
        "(a=%d AND b=%Q))",
        op->iOptionalRow, op->zOptionalValue);
  }else{
    zSql = sqlite3_mprintf(
        "SELECT count(*) FROM t WHERE NOT ("
        "(a=1 AND b='one') OR "
        "(a=2 AND b='two') OR "
        "(a=3 AND b='three'))");
  }
  if( !zSql ){
    *pzInvariant = "row invariant SQL allocation";
    return SQLITE_NOMEM;
  }
  rc = querySingleInt(db, zSql, &n);
  sqlite3_free(zSql);
  zSql = 0;
  if( rc!=SQLITE_OK || n!=0 ){
    *pzInvariant = "only complete working rows";
    return rc==SQLITE_OK ? SQLITE_CORRUPT : rc;
  }

  if( op->zStateInvariantSql ){
    VERIFY_INVARIANT("complete operation state",
        querySingleInt(db, op->zStateInvariantSql, &n));
    if( n!=1 ){
      *pzInvariant = "complete operation state";
      return SQLITE_CORRUPT;
    }
  }

  rc = sqlite3_prepare_v2(db,
      "SELECT table_name,staged,status FROM dolt_status", -1, &stmt, 0);
  if( rc!=SQLITE_OK ){
    *pzInvariant = "status prepare";
    goto verify_done;
  }
  n = 0;
  while( (rc = sqlite3_step(stmt))==SQLITE_ROW ){
    const char *zTable = (const char*)sqlite3_column_text(stmt, 0);
    const char *zStatus = (const char*)sqlite3_column_text(stmt, 2);
    int stagedValue = sqlite3_column_int(stmt, 1);
    if( !zTable || strcmp(zTable, "t")!=0
     || (stagedValue!=0 && stagedValue!=1)
     || !zStatus || strcmp(zStatus, "modified")!=0 ){
      rc = SQLITE_CORRUPT;
      break;
    }
    n++;
    if( n>2 ){
      rc = SQLITE_CORRUPT;
      break;
    }
  }
  if( rc==SQLITE_DONE ) rc = SQLITE_OK;
  frc = sqlite3_finalize(stmt);
  stmt = 0;
  if( rc==SQLITE_OK ) rc = frc;
  if( rc!=SQLITE_OK ){
    *pzInvariant = "coherent staged/working status";
    goto verify_done;
  }

  VERIFY_INVARIANT("conflict rows",
      querySingleInt(db, "SELECT count(*) FROM dolt_conflicts", &n));
  if( n!=0 ){
    *pzInvariant = "no merge conflict rows";
    return SQLITE_CORRUPT;
  }
  VERIFY_INVARIANT("constraint violation rows",
      querySingleInt(db,
        "SELECT count(*) FROM dolt_constraint_violations", &n));
  if( n!=0 ){
    *pzInvariant = "no constraint violation rows";
    return SQLITE_CORRUPT;
  }

  doltliteGetSessionMergeState(db, &isMerging,
                               &mergeCommit, &conflictsCatalog);
  if( isMerging ){
    *pzInvariant = "no merge state after fast-forward/error";
    return SQLITE_CORRUPT;
  }
  doltliteGetSessionRebaseState(db, &isRebasing,
      &preRebaseCatalog, &rebaseOnto, &zOrig, &zReturn);
  if( isRebasing ){
    if( !op->allowRebaseState || !zOrig || !zOrig[0]
     || !zReturn || !zReturn[0] ){
      *pzInvariant = "coherent rebase strings";
      return SQLITE_CORRUPT;
    }
    VERIFY_INVARIANT("pre-rebase catalog",
        verifyCatalogHash(db, &preRebaseCatalog));
    VERIFY_INVARIANT("rebase-onto commit",
        verifyCommitHash(db, &rebaseOnto));
  }

  rc = SQLITE_OK;
verify_done:
  sqlite3_finalize(stmt);
  sqlite3_free(zSql);
  return rc;
#undef VERIFY_INVARIANT
}

static int childRunOp(OpEntry *op, long long failAt){
  sqlite3 *db = 0;
  sqlite3 *verifyDb = 0;
  const char *zInvariant = 0;
  int rc;
  int orc;
  int crc;
  int vrc;
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
  if( !isAcceptableErr(orc) ){
    fprintf(stderr, "  BUG[%s,N=%lld]: op rc=%d (not acceptable). triggered=%lld nCall=%lld closeRc=%d\n",
            op->name, failAt, orc, triggered, ncall, crc);
    cleanupFiles(kDbBase);
    return CHILD_BUG;
  }
  if( !triggered && orc!=SQLITE_OK ){
    fprintf(stderr, "  BUG[%s,N=%lld]: op rc=%d without injected OOM. nCall=%lld closeRc=%d\n",
            op->name, failAt, orc, ncall, crc);
    cleanupFiles(kDbBase);
    return CHILD_BUG;
  }
  if( crc!=SQLITE_OK ){
    fprintf(stderr, "  BUG[%s,N=%lld]: close rc=%d after op rc=%d\n",
            op->name, failAt, crc, orc);
    cleanupFiles(kDbBase);
    return CHILD_BUG;
  }
  rc = sqlite3_open(kDbBase, &verifyDb);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "  BUG[%s,N=%lld]: reopen rc=%d after op rc=%d\n",
            op->name, failAt, rc, orc);
    if( verifyDb ) sqlite3_close(verifyDb);
    cleanupFiles(kDbBase);
    return CHILD_BUG;
  }
  vrc = verifyReopenedState(verifyDb, op, &zInvariant);
  if( vrc!=SQLITE_OK ){
    char zHeadMessage[128] = "";
    int nRow4 = -1;
    int nRow5 = -1;
    int nRow6 = -1;
    int nStatus = -1;
    (void)querySingleText(verifyDb,
        "SELECT message FROM dolt_log LIMIT 1",
        zHeadMessage, sizeof(zHeadMessage));
    (void)querySingleInt(verifyDb,
        "SELECT count(*) FROM t WHERE a=4", &nRow4);
    (void)querySingleInt(verifyDb,
        "SELECT count(*) FROM t WHERE a=5", &nRow5);
    (void)querySingleInt(verifyDb,
        "SELECT count(*) FROM t WHERE a=6", &nRow6);
    (void)querySingleInt(verifyDb,
        "SELECT count(*) FROM dolt_status", &nStatus);
    fprintf(stderr,
        "  BUG[%s,N=%lld]: reopen invariant \"%s\" rc=%d "
        "after op rc=%d triggered=%lld nCall=%lld "
        "head=%s rows=%d/%d/%d status=%d\n",
        op->name, failAt, zInvariant ? zInvariant : "unknown",
        vrc, orc, triggered, ncall, zHeadMessage,
        nRow4, nRow5, nRow6, nStatus);
    sqlite3_close(verifyDb);
    cleanupFiles(kDbBase);
    return CHILD_BUG;
  }
  crc = sqlite3_close(verifyDb);
  cleanupFiles(kDbBase);
  if( crc!=SQLITE_OK ){
    fprintf(stderr,
        "  BUG[%s,N=%lld]: verification close rc=%d after op rc=%d\n",
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
  long long nStart = 1;
  long long n;
  const char *zFailAt = getenv("OOM_DOLT_FAIL_AT");
  pid_t pid;
  pid_t r;
  int status;
  fflush(stdout);
  fflush(stderr);
  if( zFailAt && zFailAt[0] ){
    nStart = strtoll(zFailAt, 0, 10);
    nMax = nStart;
  }
  for( n=nStart; n<=nMax; n++ ){
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
  printf("  [%s] swept N=%lld..%lld, ok=%d, errOk=%d, bugs=%d\n",
         op->name, nStart, n-1, nOk, nErrOk, opBugs);
  fflush(stdout);
  return opBugs;
}

int main(void){
  const char *zOnlyOp = getenv("OOM_DOLT_OP");
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
      if( zOnlyOp && strcmp(zOnlyOp, kOps[i].name)!=0 ) continue;
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
