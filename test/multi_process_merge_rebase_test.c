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

static int commitMainRowWithRetry(const char *path, int iRow){
  int att;
  for(att=0; att<150; att++){
    sqlite3 *db = 0;
    const char *r;
    char sql[128];
    int rc;

    rc = sqlite3_open(path, &db);
    if( rc!=SQLITE_OK ){
      sqlite3_close(db);
      usleep(8000);
      continue;
    }
    sqlite3_busy_timeout(db, 1000);
    r = queryScalarText(db, "SELECT dolt_checkout('main')");
    if( looks_like_lock_busy(r) ){
      sqlite3_close(db);
      usleep(8000);
      continue;
    }
    snprintf(sql, sizeof(sql),
             "INSERT OR IGNORE INTO t VALUES(%d, 'mc_%d')", 500+iRow, iRow);
    rc = execSql(db, sql);
    if( rc==SQLITE_BUSY || rc==SQLITE_LOCKED ){
      sqlite3_close(db);
      usleep(8000);
      continue;
    }
    if( rc!=SQLITE_OK ){
      sqlite3_close(db);
      return 1;
    }
    snprintf(sql, sizeof(sql), "SELECT dolt_commit('-A','-m','mc_%d')", iRow);
    r = queryScalarText(db, sql);
    if( is_commit_hash(r) ){
      sqlite3_close(db);
      return 0;
    }
    if( !looks_like_commit_conflict(r) && !looks_like_lock_busy(r) ){
      sqlite3_close(db);
      return 1;
    }
    sqlite3_close(db);
    usleep(8000);
  }
  return 1;
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
    int i, fails = 0;
    for(i=0; i<N_MAIN_COMMITS; i++){
      if( commitMainRowWithRetry(path, i)!=0 ) fails++;
      usleep(15000);
    }
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


/* Interactive rebase left open; parent --continue and child --abort race.
** Exactly one must win cleanly; the loser must report "no rebase in progress"
** (not "rebase recovery failed"). Final state must be usable. */
static void test_concurrent_continue_abort(void){
  const char *path = "/tmp/test_rebase_continue_abort_race.db";
  int trial, wins_continue = 0, wins_abort = 0, bad = 0;

  printf("--- Test 5: concurrent --continue vs --abort ---\n");

  for(trial=0; trial<12; trial++){
    pid_t child;
    int status;
    int pipefd[2];
    int readyfd[2];
    int gofd[2];
    char contOut[256], abortOut[256];
    char branchPath[320];
    sqlite3 *db = 0;
    int contWin, abortWin, contLost, abortLost;
    int exactlyOneWin, loserOk;

    remove(path);
    { char w[256]; snprintf(w, sizeof(w), "%s-wal", path); remove(w); }

    sqlite3_open(path, &db);
    sqlite3_busy_timeout(db, 30000);
    execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT)");
    execSql(db, "INSERT INTO t VALUES(1, 1)");
    queryScalarText(db, "SELECT dolt_commit('-A','-m','c1')");
    queryScalarText(db, "SELECT dolt_checkout('-b','feat')");
    execSql(db, "INSERT INTO t VALUES(2, 2)");
    queryScalarText(db, "SELECT dolt_commit('-A','-m','f1')");
    queryScalarText(db, "SELECT dolt_checkout('main')");
    execSql(db, "INSERT INTO t VALUES(10, 10)");
    queryScalarText(db, "SELECT dolt_commit('-A','-m','m1')");
    queryScalarText(db, "SELECT dolt_checkout('feat')");
    queryScalarText(db, "SELECT dolt_rebase('-i','main')");
    sqlite3_close(db);
    snprintf(branchPath, sizeof(branchPath), "%s/dolt_rebase_feat", path);

    contOut[0] = abortOut[0] = 0;
    if( pipe(pipefd)!=0 || pipe(readyfd)!=0 || pipe(gofd)!=0 ){
      bad++;
      continue;
    }
    child = fork();
    if( child==0 ){
      sqlite3 *cdb = 0;
      const char *r;
      char signal;
      close(pipefd[0]);
      close(readyfd[0]);
      close(gofd[1]);
      sqlite3_open(branchPath, &cdb);
      sqlite3_busy_timeout(cdb, 10000);
      (void)write(readyfd[1], "R", 1);
      (void)read(gofd[0], &signal, 1);
      if( trial%3==2 ) usleep(2000);
      r = queryScalarText(cdb, "SELECT dolt_rebase('--abort')");
      {
        size_t n = strlen(r)+1;
        const char *p = r;
        while( n>0 ){
          ssize_t w = write(pipefd[1], p, n);
          if( w<=0 ) break;
          p += (size_t)w;
          n -= (size_t)w;
        }
      }
      close(pipefd[1]);
      close(readyfd[1]);
      close(gofd[0]);
      sqlite3_close(cdb);
      _exit(0);
    }
    close(pipefd[1]);
    close(readyfd[1]);
    close(gofd[0]);

    {
      sqlite3 *pdb = 0;
      const char *r;
      char signal;
      sqlite3_open(branchPath, &pdb);
      sqlite3_busy_timeout(pdb, 10000);
      (void)read(readyfd[0], &signal, 1);
      (void)write(gofd[1], "G", 1);
      if( trial%3==1 ) usleep(2000);
      r = queryScalarText(pdb, "SELECT dolt_rebase('--continue')");
      snprintf(contOut, sizeof(contOut), "%s", r);
      sqlite3_close(pdb);
    }
    close(readyfd[0]);
    close(gofd[1]);

    {
      ssize_t n = read(pipefd[0], abortOut, sizeof(abortOut)-1);
      if( n<0 ) n = 0;
      abortOut[n] = 0;
      close(pipefd[0]);
    }
    waitpid(child, &status, 0);

    contWin = strstr(contOut, "Successfully")!=0;
    abortWin = strstr(abortOut, "Interactive rebase aborted")!=0;
    contLost = strstr(contOut, "no rebase in progress")!=0;
    abortLost = strstr(abortOut, "no rebase in progress")!=0;
    exactlyOneWin = contWin ^ abortWin;
    loserOk = (contWin && abortLost) || (abortWin && contLost);

    if( contWin ) wins_continue++;
    if( abortWin ) wins_abort++;
    if( !exactlyOneWin || !loserOk ){
      bad++;
      fprintf(stderr, "FAIL trial %d cont=[%s] abort=[%s]\n",
              trial, contOut, abortOut);
    }

    {
      sqlite3 *v = 0;
      const char *h;
      int tempBranches;
      int planTables;
      int featRows;
      int mainRows;
      sqlite3_open(path, &v);
      sqlite3_busy_timeout(v, 30000);
      queryScalarText(v, "SELECT dolt_checkout('feat')");
      tempBranches = countRows(v,
          "SELECT count(*) FROM dolt_branches WHERE name='dolt_rebase_feat'");
      planTables = countRows(v,
          "SELECT count(*) FROM sqlite_master WHERE name='dolt_rebase'");
      featRows = countRows(v, "SELECT count(*) FROM t WHERE id=2");
      mainRows = countRows(v, "SELECT count(*) FROM t WHERE id=10");
      if( tempBranches!=0 || planTables!=0 || featRows!=1
       || mainRows!=(contWin ? 1 : 0) ){
        bad++;
        fprintf(stderr,
                "FAIL trial %d final temp=%d plan=%d feat=%d main=%d cont=[%s] abort=[%s]\n",
                trial, tempBranches, planTables, featRows, mainRows,
                contOut, abortOut);
      }
      execSql(v, "INSERT INTO t VALUES(99, 99)");
      h = queryScalarText(v, "SELECT dolt_commit('-A','-m','post-race')");
      check("continue_abort_post_commit", is_commit_hash(h));
      sqlite3_close(v);
    }
  }

  check("continue_abort_no_bad_outcomes", bad==0);
  check("continue_abort_exactly_one_winner", wins_continue+wins_abort==12);
  check("continue_abort_had_activity", (wins_continue+wins_abort)>0);
  printf("  continue wins=%d abort wins=%d bad=%d\n",
         wins_continue, wins_abort, bad);
  remove(path);
}

int main(void){
  setvbuf(stdout, 0, _IOLBF, 0);
  printf("=== Multi-Process Merge & Rebase Concurrency Tests ===\n\n");

  test_concurrent_mergers_into_main();
  test_merge_racing_target_commits();
  test_rebase_racing_gc();
  test_rebase_racing_checkout();
  test_concurrent_continue_abort();

  printf("\n=== Results: %d passed, %d failed out of %d tests ===\n",
    nPass, nFail, nPass+nFail);
  return nFail > 0 ? 1 : 0;
}
