/*
** Rebase is not atomic against a concurrent peer commit by design, so it is
** raced here only against non-committing peers (GC, checkout).
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include "sqlite3.h"

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

static int execSql(sqlite3 *db, const char *sql){
  char *err = 0;
  int rc = sqlite3_exec(db, sql, 0, 0, &err);
  if( err ) sqlite3_free(err);
  return rc;
}

static char result_buf[4096];
static const char *queryScalarText(sqlite3 *db, const char *sql){
  sqlite3_stmt *s = 0;
  int rc;
  result_buf[0] = 0;
  rc = sqlite3_prepare_v2(db, sql, -1, &s, 0);
  if( rc!=SQLITE_OK ){
    snprintf(result_buf, sizeof(result_buf), "PREP_ERR: %s", sqlite3_errmsg(db));
    return result_buf;
  }
  rc = sqlite3_step(s);
  if( rc==SQLITE_ROW ){
    const char *v = (const char*)sqlite3_column_text(s, 0);
    if( v ) snprintf(result_buf, sizeof(result_buf), "%s", v);
  }else if( rc!=SQLITE_DONE ){
    snprintf(result_buf, sizeof(result_buf), "STEP_ERR: %s", sqlite3_errmsg(db));
  }
  sqlite3_finalize(s);
  return result_buf;
}

static int looks_like_error(const char *s){
  if( !s ) return 1;
  if( strstr(s, "ERR")!=0 ) return 1;
  if( strstr(s, "err")!=0 ) return 1;
  return 0;
}

static int looks_like_lock_busy(const char *s){
  if( !s ) return 0;
  if( strlen(s)==0 ) return 1;
  if( strstr(s, "locked")!=0 ) return 1;
  if( strstr(s, "busy")!=0 ) return 1;
  if( strstr(s, "BUSY")!=0 ) return 1;
  if( strstr(s, "ERR")!=0 ) return 1;
  return 0;
}

static int looks_like_commit_conflict(const char *s){
  return s!=0 && strstr(s, "commit conflict: another connection committed")!=0;
}

static int is_commit_hash(const char *s){
  return s!=0 && strlen(s)==40;
}

static int countRows(sqlite3 *db, const char *sql){
  sqlite3_stmt *s = 0;
  int rc = sqlite3_prepare_v2(db, sql, -1, &s, 0);
  int n = -1;
  if( rc==SQLITE_OK ){
    if( sqlite3_step(s)==SQLITE_ROW ) n = sqlite3_column_int(s, 0);
    sqlite3_finalize(s);
  }
  return n;
}

/* Commit conflict means a peer advanced main first; reopen and re-merge. Uses
** a short busy_timeout with one attempt per round so contention fails fast and
** the round loop is the only retry path, bounding total time. */
static int mergeIntoMainWithRetry(const char *path, const char *branch,
                                  int max_rounds){
  int round;
  for(round=0; round<max_rounds; round++){
    sqlite3 *db = 0;
    const char *r;
    char sql[128];
    if( sqlite3_open(path, &db)!=SQLITE_OK ){ sqlite3_close(db); usleep(10000); continue; }
    sqlite3_busy_timeout(db, 1000);
    r = queryScalarText(db, "SELECT dolt_checkout('main')");
    if( looks_like_lock_busy(r) ){ sqlite3_close(db); usleep(10000); continue; }
    snprintf(sql, sizeof(sql), "SELECT dolt_merge('%s')", branch);
    r = queryScalarText(db, sql);
    if( is_commit_hash(r) || strstr(r, "Already up to date")!=0 ){
      sqlite3_close(db);
      return 0;
    }
    sqlite3_close(db);
    if( looks_like_commit_conflict(r) || looks_like_lock_busy(r) ){
      usleep(10000 + (round%16)*5000);
      continue;
    }
    fprintf(stderr, "[diag] merge('%s') unexpected result: %s\n", branch, r);
    return 1;
  }
  return 2;
}

static void setup_base(const char *path, int nBase){
  sqlite3 *db = 0;
  int i;
  remove(path);
  sqlite3_open(path, &db);
  execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");
  for(i=1; i<=nBase; i++){
    char sql[128];
    snprintf(sql, sizeof(sql), "INSERT INTO t VALUES(%d, 'base_%d')", i, i);
    execSql(db, sql);
  }
  queryScalarText(db, "SELECT dolt_commit('-A','-m','base')");
  sqlite3_close(db);
}

static void seed_feature_branch(const char *path, const char *branch,
                                int base, int nRows){
  sqlite3 *db = 0;
  char sql[160];
  int i;
  sqlite3_open(path, &db);
  sqlite3_busy_timeout(db, 30000);
  queryScalarText(db, "SELECT dolt_checkout('main')");
  snprintf(sql, sizeof(sql), "SELECT dolt_branch('%s')", branch);
  queryScalarText(db, sql);
  snprintf(sql, sizeof(sql), "SELECT dolt_checkout('%s')", branch);
  queryScalarText(db, sql);
  for(i=0; i<nRows; i++){
    snprintf(sql, sizeof(sql), "INSERT INTO t VALUES(%d, '%s_%d')",
             base+i, branch, i);
    execSql(db, sql);
  }
  snprintf(sql, sizeof(sql), "SELECT dolt_commit('-A','-m','%s work')", branch);
  queryScalarText(db, sql);
  queryScalarText(db, "SELECT dolt_checkout('main')");
  sqlite3_close(db);
}


static void test_concurrent_mergers_into_main(void){
  const char *path = "/tmp/test_concurrent_mergers.db";
  const int N_MERGERS = 8;
  const int N_PER = 10;
  pid_t pids[8];
  int i, status;

  printf("--- Test 1: %d processes merging distinct branches into main ---\n",
         N_MERGERS);
  setup_base(path, 20);
  for(i=0; i<N_MERGERS; i++){
    char br[32];
    snprintf(br, sizeof(br), "feat%d", i);
    seed_feature_branch(path, br, 100 + i*100, N_PER);
  }

  for(i=0; i<N_MERGERS; i++){
    pids[i] = fork();
    if( pids[i]==0 ){
      char br[32];
      snprintf(br, sizeof(br), "feat%d", i);
      _exit(mergeIntoMainWithRetry(path, br, 400));
    }
  }
  for(i=0; i<N_MERGERS; i++){
    char nm[48];
    waitpid(pids[i], &status, 0);
    snprintf(nm, sizeof(nm), "mergers_child_%d_ok", i);
    check(nm, WIFEXITED(status) && WEXITSTATUS(status)==0);
  }

  {
    sqlite3 *db = 0;
    sqlite3_open(path, &db);
    sqlite3_busy_timeout(db, 30000);
    queryScalarText(db, "SELECT dolt_checkout('main')");
    check("mergers_total_count",
          countRows(db, "SELECT count(*) FROM t")==20 + N_MERGERS*N_PER);
    for(i=0; i<N_MERGERS; i++){
      char sql[96], nm[48];
      snprintf(sql, sizeof(sql),
               "SELECT count(*) FROM t WHERE id>=%d AND id<%d",
               100 + i*100, 100 + i*100 + N_PER);
      snprintf(nm, sizeof(nm), "mergers_feat%d_rows_present", i);
      check(nm, countRows(db, sql)==N_PER);
    }
    sqlite3_close(db);
  }

  remove(path);
}


static void test_merge_racing_target_commits(void){
  const char *path = "/tmp/test_merge_racing_commits.db";
  const int N_MAIN_COMMITS = 8;
  pid_t writer, merger;
  int status1, status2;

  printf("--- Test 2: merge into main racing concurrent main commits ---\n");
  setup_base(path, 10);
  seed_feature_branch(path, "feat", 100, 15);

  writer = fork();
  if( writer==0 ){
    sqlite3 *db = 0;
    int i, fails = 0;
    sqlite3_open(path, &db);
    sqlite3_busy_timeout(db, 1000);
    for(i=0; i<N_MAIN_COMMITS; i++){
      char sql[128];
      const char *r;
      int att, committed = 0;
      if( looks_like_lock_busy(queryScalarText(db, "SELECT dolt_checkout('main')")) ){
        usleep(10000); continue;
      }
      snprintf(sql, sizeof(sql), "INSERT INTO t VALUES(%d, 'mc_%d')", 500+i, i);
      if( execSql(db, sql)!=SQLITE_OK ){ fails++; continue; }
      snprintf(sql, sizeof(sql), "SELECT dolt_commit('-A','-m','mc_%d')", i);
      for(att=0; att<150 && !committed; att++){
        r = queryScalarText(db, sql);
        if( is_commit_hash(r) ){ committed = 1; break; }
        if( looks_like_commit_conflict(r) || looks_like_lock_busy(r) ){
          usleep(8000); continue;
        }
        fails++; break;   /* only a hard, non-retryable result is a failure */
      }
      usleep(15000);
    }
    sqlite3_close(db);
    _exit(fails>0 ? 1 : 0);
  }

  merger = fork();
  if( merger==0 ){
    _exit(mergeIntoMainWithRetry(path, "feat", 250));
  }

  waitpid(writer, &status1, 0);
  waitpid(merger, &status2, 0);
  check("merge_race_writer_ok", WIFEXITED(status1) && WEXITSTATUS(status1)==0);
  check("merge_race_merger_ok", WIFEXITED(status2) && WEXITSTATUS(status2)==0);

  {
    sqlite3 *db = 0;
    sqlite3_open(path, &db);
    sqlite3_busy_timeout(db, 30000);
    queryScalarText(db, "SELECT dolt_checkout('main')");
    check("merge_race_feat_rows_present",
          countRows(db, "SELECT count(*) FROM t WHERE id>=100 AND id<115")==15);
    check("merge_race_some_main_commits_present",
          countRows(db, "SELECT count(*) FROM t WHERE id>=500 AND id<520")>=1);
    sqlite3_close(db);
  }

  remove(path);
}


/* Interactive rebase of feat onto main, no plan edits. A rebase spans several
** sub-statements and is not atomic against a concurrent peer, so the whole op
** aborts and retries on a transient step failure. Uses a short busy_timeout so
** a contended step fails fast and the round loop (not a blocking wait) is the
** only retry path, bounding total time. Returns 0 on success. */
/* Retry --abort until it is not lock-contended, so an interrupted rebase's
** plan table is reliably cleared before the next attempt. */
static void rebaseAbort(sqlite3 *db){
  int a;
  for(a=0; a<40; a++){
    if( !looks_like_lock_busy(queryScalarText(db, "SELECT dolt_rebase('--abort')")) ) return;
    usleep(10000);
  }
}

static int doRebaseOnce(const char *path){
  int round;
  char lastErr[256] = "";
  for(round=0; round<120; round++){
    sqlite3 *db = 0;
    const char *r;
    if( sqlite3_open(path, &db)!=SQLITE_OK ){ sqlite3_close(db); usleep(20000); continue; }
    sqlite3_busy_timeout(db, 800);
    rebaseAbort(db);
    if( looks_like_lock_busy(queryScalarText(db, "SELECT dolt_checkout('feat')")) ){
      sqlite3_close(db); usleep(20000); continue;
    }
    r = queryScalarText(db, "SELECT dolt_rebase('-i','main')");
    if( !looks_like_error(r) ){
      r = queryScalarText(db, "SELECT dolt_rebase('--continue')");
      if( !looks_like_error(r) ){ sqlite3_close(db); return 0; }
    }
    snprintf(lastErr, sizeof(lastErr), "%s", r);
    rebaseAbort(db);
    sqlite3_close(db);
    usleep(20000 + (round%8)*10000);
  }
  fprintf(stderr, "[diag] rebase exhausted; last result: %s\n", lastErr);
  return 1;
}

static void test_rebase_racing_gc(void){
  const char *path = "/tmp/test_rebase_vs_gc.db";
  pid_t gc_pid;
  int status, rebase_rc;
  int i;

  printf("--- Test 3: rebase racing a dolt_gc() loop ---\n");
  setup_base(path, 30);

  {
    sqlite3 *db = 0;
    sqlite3_open(path, &db);
    sqlite3_busy_timeout(db, 30000);
    queryScalarText(db, "SELECT dolt_checkout('-b','feat')");
    for(i=0; i<5; i++){
      char sql[128];
      snprintf(sql, sizeof(sql), "INSERT INTO t VALUES(%d, 'feat_%d')", 200+i, i);
      execSql(db, sql);
      snprintf(sql, sizeof(sql), "SELECT dolt_commit('-A','-m','feat_%d')", i);
      queryScalarText(db, sql);
    }
    queryScalarText(db, "SELECT dolt_checkout('main')");
    for(i=0; i<5; i++){
      char sql[128];
      snprintf(sql, sizeof(sql), "INSERT INTO t VALUES(%d, 'main_%d')", 300+i, i);
      execSql(db, sql);
      snprintf(sql, sizeof(sql), "SELECT dolt_commit('-A','-m','main_%d')", i);
      queryScalarText(db, sql);
    }
    sqlite3_close(db);
  }

  gc_pid = fork();
  if( gc_pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(path, &db);
    sqlite3_busy_timeout(db, 2000);
    for(i=0; i<5; i++){
      queryScalarText(db, "SELECT dolt_gc()");
      usleep(40000);
    }
    sqlite3_close(db);
    _exit(0);
  }

  rebase_rc = doRebaseOnce(path);
  check("rebase_vs_gc_rebase_ok", rebase_rc==0);

  waitpid(gc_pid, &status, 0);
  check("rebase_vs_gc_gc_child_ok", WIFEXITED(status) && WEXITSTATUS(status)==0);

  {
    sqlite3 *db = 0;
    sqlite3_open(path, &db);
    sqlite3_busy_timeout(db, 30000);
    queryScalarText(db, "SELECT dolt_checkout('feat')");
    check("rebase_vs_gc_feat_rows_present",
          countRows(db, "SELECT count(*) FROM t WHERE id>=200 AND id<205")==5);
    check("rebase_vs_gc_main_rows_replayed_onto",
          countRows(db, "SELECT count(*) FROM t WHERE id>=300 AND id<305")==5);
    check("rebase_vs_gc_base_intact",
          countRows(db, "SELECT count(*) FROM t WHERE id<=30")==30);
    sqlite3_close(db);
  }

  remove(path);
}


static void test_rebase_racing_checkout(void){
  const char *path = "/tmp/test_rebase_vs_checkout.db";
  pid_t co_pid;
  int status, rebase_rc;
  int i;

  printf("--- Test 4: rebase racing a checkout loop ---\n");
  setup_base(path, 20);

  {
    sqlite3 *db = 0;
    sqlite3_open(path, &db);
    sqlite3_busy_timeout(db, 30000);
    queryScalarText(db, "SELECT dolt_checkout('-b','feat')");
    for(i=0; i<4; i++){
      char sql[128];
      snprintf(sql, sizeof(sql), "INSERT INTO t VALUES(%d, 'feat_%d')", 200+i, i);
      execSql(db, sql);
      snprintf(sql, sizeof(sql), "SELECT dolt_commit('-A','-m','feat_%d')", i);
      queryScalarText(db, sql);
    }
    queryScalarText(db, "SELECT dolt_checkout('main')");
    execSql(db, "INSERT INTO t VALUES(300, 'main_x')");
    queryScalarText(db, "SELECT dolt_commit('-A','-m','main_x')");
    queryScalarText(db, "SELECT dolt_branch('side')");
    sqlite3_close(db);
  }

  co_pid = fork();
  if( co_pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(path, &db);
    sqlite3_busy_timeout(db, 2000);
    for(i=0; i<40; i++){
      queryScalarText(db, "SELECT dolt_checkout('main')");
      queryScalarText(db, "SELECT dolt_checkout('side')");
      usleep(3000);
    }
    sqlite3_close(db);
    _exit(0);
  }

  rebase_rc = doRebaseOnce(path);
  check("rebase_vs_checkout_rebase_ok", rebase_rc==0);

  waitpid(co_pid, &status, 0);
  check("rebase_vs_checkout_child_ok",
        WIFEXITED(status) && WEXITSTATUS(status)==0);

  {
    sqlite3 *db = 0;
    sqlite3_open(path, &db);
    sqlite3_busy_timeout(db, 30000);
    queryScalarText(db, "SELECT dolt_checkout('feat')");
    check("rebase_vs_checkout_feat_rows_present",
          countRows(db, "SELECT count(*) FROM t WHERE id>=200 AND id<204")==4);
    check("rebase_vs_checkout_main_row_replayed_onto",
          countRows(db, "SELECT count(*) FROM t WHERE id=300")==1);
    sqlite3_close(db);
  }

  remove(path);
}


int main(void){
  setvbuf(stdout, 0, _IOLBF, 0);
  printf("=== Multi-Process Merge & Rebase Concurrency Tests ===\n\n");

  test_concurrent_mergers_into_main();
  test_merge_racing_target_commits();
  test_rebase_racing_gc();
  test_rebase_racing_checkout();

  printf("\n=== Results: %d passed, %d failed out of %d tests ===\n",
    nPass, nFail, nPass+nFail);
  return nFail > 0 ? 1 : 0;
}
