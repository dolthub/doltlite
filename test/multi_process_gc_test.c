#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <signal.h>
#include <errno.h>
#include "sqlite3.h"
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

/* glibc fortify marks these results warn_unused_result. A failed sync pipe
** means the test cannot run meaningfully, so fail the process immediately. */
static void mpPipe(int aFd[2]){
  if( pipe(aFd)!=0 ){ perror("pipe"); _exit(1); }
}

static void mpWrite(int fd, const char *zByte){
  if( write(fd,zByte,1)!=1 ) _exit(1);
}

static void mpRead(int fd, char *pValue){
  if( read(fd,pValue,1)!=1 ) _exit(1);
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
    snprintf(result_buf, sizeof(result_buf), "PREP_ERR(%d)", rc);
    return result_buf;
  }
  rc = sqlite3_step(s);
  if( rc==SQLITE_ROW ){
    const char *v = (const char*)sqlite3_column_text(s, 0);
    if( v ) snprintf(result_buf, sizeof(result_buf), "%s", v);
  }else if( rc!=SQLITE_DONE ){
    snprintf(result_buf, sizeof(result_buf), "STEP_ERR(%d)", rc);
  }
  sqlite3_finalize(s);
  return result_buf;
}

static int file_exists(const char *path){
  struct stat st;
  return stat(path, &st)==0;
}

static int countRowsDiag(sqlite3 *db, const char *sql){
  sqlite3_stmt *s = 0;
  int rc = sqlite3_prepare_v2(db, sql, -1, &s, 0);
  int n = -1;
  if( rc==SQLITE_OK ){
    if( sqlite3_step(s)==SQLITE_ROW ){
      n = sqlite3_column_int(s, 0);
    }
    sqlite3_finalize(s);
  }
  return n;
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

static const char *callWithRetry(sqlite3 *db, const char *sql, int max_attempts){
  int attempt;
  static char last[4096];
  for(attempt=0; attempt<max_attempts; attempt++){
    const char *r = queryScalarText(db, sql);
    if( !looks_like_lock_busy(r) ){
      snprintf(last, sizeof(last), "%s", r);
      return last;
    }
    snprintf(last, sizeof(last), "%s", r);
    usleep(3000 + (attempt%16)*3000);
  }
  return last;
}

static void setup_db_with_rows(const char *path, int n){
  sqlite3 *db = 0;
  int i;
  remove(path);
  sqlite3_open(path, &db);
  execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");
  for(i=1; i<=n; i++){
    char sql[128];
    snprintf(sql, sizeof(sql), "INSERT INTO t VALUES(%d, 'v%d')", i, i);
    execSql(db, sql);
  }
  queryScalarText(db, "SELECT dolt_commit('-A','-m','setup')");
  sqlite3_close(db);
}

static void make_garbage(const char *path, int n){
  sqlite3 *db = 0;
  int i;
  sqlite3_open(path, &db);
  for(i=0; i<n; i++){
    char sql[128];
    snprintf(sql, sizeof(sql), "INSERT INTO t VALUES(%d, 'garbage_%d')", 100000+i, i);
    execSql(db, sql);
  }
  queryScalarText(db, "SELECT dolt_commit('-A','-m','garbage')");
  execSql(db, "DELETE FROM t WHERE id >= 100000");
  queryScalarText(db, "SELECT dolt_commit('-A','-m','cleanup')");
  sqlite3_close(db);
}


static void test_gc_vs_gc_parallel(void){
  const char *path = DOLTLITE_TEST_TMPDIR "/test_gc_vs_gc.db";
  pid_t pid1, pid2;
  int status1, status2;

  printf("--- Test 1: Two concurrent dolt_gc() calls ---\n");
  setup_db_with_rows(path, 100);
  make_garbage(path, 30);

  pid1 = fork();
  if( pid1==0 ){
    sqlite3 *db = 0;
    const char *r;
    sqlite3_open(path, &db);
    r = callWithRetry(db, "SELECT dolt_gc()", 200);
    sqlite3_close(db);
    _exit(looks_like_lock_busy(r) ? 1 : 0);
  }
  pid2 = fork();
  if( pid2==0 ){
    sqlite3 *db = 0;
    const char *r;
    sqlite3_open(path, &db);
    r = callWithRetry(db, "SELECT dolt_gc()", 200);
    sqlite3_close(db);
    _exit(looks_like_lock_busy(r) ? 1 : 0);
  }

  waitpid(pid1, &status1, 0);
  waitpid(pid2, &status2, 0);
  check("gc_vs_gc_first_ok",
        WIFEXITED(status1) && WEXITSTATUS(status1)==0);
  check("gc_vs_gc_second_ok",
        WIFEXITED(status2) && WEXITSTATUS(status2)==0);

  {
    sqlite3 *db = 0;
    sqlite3_open(path, &db);
    check("gc_vs_gc_count_intact",
      strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "100")==0);
    check("gc_vs_gc_first_row_intact",
      strcmp(queryScalarText(db, "SELECT v FROM t WHERE id=1"), "v1")==0);
    check("gc_vs_gc_last_row_intact",
      strcmp(queryScalarText(db, "SELECT v FROM t WHERE id=100"), "v100")==0);
    sqlite3_close(db);
  }

  remove(path);
}


static void test_gc_blocked_then_retries(void){
  const char *path = DOLTLITE_TEST_TMPDIR "/test_gc_blocked_retry.db";
  pid_t pid;
  int status;
  int pipefd[2];

  printf("--- Test 2: GC blocked by writer, retries after commit ---\n");
  setup_db_with_rows(path, 50);
  mpPipe(pipefd);

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    close(pipefd[0]);
    sqlite3_open(path, &db);
    sqlite3_busy_timeout(db, 5000);
    execSql(db, "BEGIN");
    execSql(db, "INSERT INTO t VALUES(9999, 'pending')");
    mpWrite(pipefd[1], "B");
    close(pipefd[1]);
    sleep(1);
    execSql(db, "COMMIT");
    queryScalarText(db, "SELECT dolt_commit('-A','-m','from child')");
    sqlite3_close(db);
    _exit(0);
  }

  {
    sqlite3 *db = 0;
    char buf;
    const char *r;
    close(pipefd[1]);
    mpRead(pipefd[0], &buf);
    close(pipefd[0]);

    sqlite3_open(path, &db);
    sqlite3_busy_timeout(db, 100);
    r = queryScalarText(db, "SELECT dolt_gc()");
    check("gc_blocked_first_attempt", looks_like_lock_busy(r));
    sqlite3_close(db);
  }

  waitpid(pid, &status, 0);
  check("gc_blocked_child_committed",
        WIFEXITED(status) && WEXITSTATUS(status)==0);

  {
    sqlite3 *db = 0;
    const char *r;
    sqlite3_open(path, &db);
    r = queryScalarText(db, "SELECT dolt_gc()");
    check("gc_succeeds_after_writer", !looks_like_error(r));
    check("gc_blocked_data_intact",
      strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "51")==0);
    check("gc_blocked_pending_row_present",
      strcmp(queryScalarText(db, "SELECT v FROM t WHERE id=9999"), "pending")==0);
    sqlite3_close(db);
  }

  remove(path);
}


static void test_gc_waits_for_busy_writer(void){
  const char *path = DOLTLITE_TEST_TMPDIR "/test_gc_waits_busy_writer.db";
  pid_t pid;
  int status;
  int pipefd[2];

  printf("--- Test 2b: GC waits for busy writer ---\n");
  setup_db_with_rows(path, 50);
  mpPipe(pipefd);

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    close(pipefd[0]);
    sqlite3_open(path, &db);
    sqlite3_busy_timeout(db, 5000);
    execSql(db, "BEGIN");
    execSql(db, "INSERT INTO t VALUES(9998, 'pending_wait')");
    mpWrite(pipefd[1], "B");
    close(pipefd[1]);
    sleep(1);
    execSql(db, "COMMIT");
    queryScalarText(db, "SELECT dolt_commit('-A','-m','from child wait')");
    sqlite3_close(db);
    _exit(0);
  }

  {
    sqlite3 *db = 0;
    char buf;
    const char *r;
    close(pipefd[1]);
    mpRead(pipefd[0], &buf);
    close(pipefd[0]);

    sqlite3_open(path, &db);
    sqlite3_busy_timeout(db, 5000);
    r = queryScalarText(db, "SELECT dolt_gc()");
    check("gc_waits_for_busy_writer", !looks_like_error(r));
    check("gc_waits_not_lock_busy", !looks_like_lock_busy(r));
    sqlite3_close(db);
  }

  waitpid(pid, &status, 0);
  check("gc_wait_writer_child_committed",
        WIFEXITED(status) && WEXITSTATUS(status)==0);

  {
    sqlite3 *db = 0;
    sqlite3_open(path, &db);
    check("gc_wait_writer_data_intact",
      strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "51")==0);
    check("gc_wait_writer_pending_row_present",
      strcmp(queryScalarText(db, "SELECT v FROM t WHERE id=9998"), "pending_wait")==0);
    sqlite3_close(db);
  }

  remove(path);
}


static void test_gc_vs_dolt_commit(void){
  const char *path = DOLTLITE_TEST_TMPDIR "/test_gc_vs_commit.db";
  pid_t pid;
  int status;

  printf("--- Test 3: dolt_gc() racing dolt_commit() ---\n");
  setup_db_with_rows(path, 100);
  make_garbage(path, 30);

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    int i, commit_failures = 0;
    sqlite3_open(path, &db);
    sqlite3_busy_timeout(db, 30000);
    for(i=0; i<25; i++){
      char sql[128];
      const char *r;
      snprintf(sql, sizeof(sql), "INSERT INTO t VALUES(%d, 'racer_%d')", 2000+i, i);
      if( execSql(db, sql)!=SQLITE_OK ){ sqlite3_close(db); _exit(2); }
      snprintf(sql, sizeof(sql), "SELECT dolt_commit('-A','-m','race_%d')", i);
      r = callWithRetry(db, sql, 200);
      if( strlen(r)!=40 ) commit_failures++;
    }
    sqlite3_close(db);
    _exit(commit_failures==0 ? 0 : (10 + (commit_failures>240 ? 240 : commit_failures)));
  }

  {
    sqlite3 *db = 0;
    int i;
    sqlite3_open(path, &db);
    sqlite3_busy_timeout(db, 30000);
    for(i=0; i<6; i++){
      const char *r = callWithRetry(db, "SELECT dolt_gc()", 200);
      char nm[64];
      snprintf(nm, sizeof(nm), "gc_vs_commit_gc_iter_%d", i);
      check(nm, !looks_like_lock_busy(r));
      usleep(40000);
    }
    sqlite3_close(db);
  }

  waitpid(pid, &status, 0);
  check("gc_vs_commit_writer_ok",
        WIFEXITED(status) && WEXITSTATUS(status)==0);

  {
    sqlite3 *db = 0;
    sqlite3_open(path, &db);
    check("gc_vs_commit_total_count",
      strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "125")==0);
    check("gc_vs_commit_all_racer_rows_present",
      strcmp(queryScalarText(db,
        "SELECT count(*) FROM t WHERE id >= 2000 AND id < 2025"), "25")==0);
    sqlite3_close(db);
  }

  remove(path);
}


static void test_reader_iterator_during_gc(void){
  const char *path = DOLTLITE_TEST_TMPDIR "/test_reader_iter_gc.db";
  pid_t pid;
  int status;
  int pipefd_go[2];

  printf("--- Test 4: SELECT iterator open during dolt_gc() ---\n");
  setup_db_with_rows(path, 100);
  make_garbage(path, 40);
  mpPipe(pipefd_go);

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    char buf;
    const char *r;
    close(pipefd_go[1]);

    mpRead(pipefd_go[0], &buf);
    close(pipefd_go[0]);

    sqlite3_open(path, &db);
    sqlite3_busy_timeout(db, 30000);
    r = queryScalarText(db, "SELECT dolt_gc()");
    sqlite3_close(db);
    _exit(looks_like_error(r) ? 1 : 0);
  }

  {
    sqlite3 *db = 0;
    sqlite3_stmt *s = 0;
    int rc, rowCount = 0;
    int i;

    sqlite3_open(path, &db);
    sqlite3_busy_timeout(db, 30000);
    close(pipefd_go[0]);

    rc = sqlite3_prepare_v2(db, "SELECT id FROM t ORDER BY id", -1, &s, 0);
    check("reader_iter_prepare", rc==SQLITE_OK);

    for(i=0; i<5; i++){
      rc = sqlite3_step(s);
      if( rc==SQLITE_ROW ) rowCount++;
    }

    mpWrite(pipefd_go[1], "G");
    close(pipefd_go[1]);

    usleep(150000);

    while( (rc = sqlite3_step(s))==SQLITE_ROW ){
      rowCount++;
    }
    check("reader_iter_step_completed", rc==SQLITE_DONE);
    check("reader_iter_saw_all_rows", rowCount==100);
    sqlite3_finalize(s);
    sqlite3_close(db);
  }

  waitpid(pid, &status, 0);
  check("reader_iter_child_gc_ok",
        WIFEXITED(status) && WEXITSTATUS(status)==0);

  {
    sqlite3 *db = 0;
    sqlite3_open(path, &db);
    check("reader_iter_post_gc_count",
      strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "100")==0);
    sqlite3_close(db);
  }

  remove(path);
}


static void test_gc_vs_branch_create(void){
  const char *path = DOLTLITE_TEST_TMPDIR "/test_gc_vs_branch.db";
  pid_t pid;
  int status;

  printf("--- Test 5: dolt_gc() racing dolt_branch() ---\n");
  setup_db_with_rows(path, 50);
  make_garbage(path, 20);

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    int i, failures = 0;
    sqlite3_open(path, &db);
    sqlite3_busy_timeout(db, 30000);
    for(i=0; i<10; i++){
      char sql[128];
      const char *r;
      snprintf(sql, sizeof(sql), "SELECT dolt_branch('br_%d')", i);
      r = callWithRetry(db, sql, 200);
      if( looks_like_lock_busy(r) ) failures++;
    }
    sqlite3_close(db);
    _exit(failures==0 ? 0 : (20 + (failures>200 ? 200 : failures)));
  }

  {
    sqlite3 *db = 0;
    int i;
    sqlite3_open(path, &db);
    sqlite3_busy_timeout(db, 30000);
    for(i=0; i<3; i++){
      const char *r = callWithRetry(db, "SELECT dolt_gc()", 200);
      char nm[64];
      snprintf(nm, sizeof(nm), "gc_vs_branch_gc_iter_%d", i);
      check(nm, !looks_like_lock_busy(r));
      usleep(20000);
    }
    sqlite3_close(db);
  }

  waitpid(pid, &status, 0);
  check("gc_vs_branch_child_ok",
        WIFEXITED(status) && WEXITSTATUS(status)==0);

  {
    sqlite3 *db = 0;
    int i;
    sqlite3_open(path, &db);
    check("gc_vs_branch_branches_count",
      strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_branches"), "11")==0);
    for(i=0; i<10; i++){
      char sql[128], nm[64];
      snprintf(sql, sizeof(sql), "SELECT dolt_checkout('br_%d')", i);
      queryScalarText(db, sql);
      snprintf(nm, sizeof(nm), "gc_vs_branch_br_%d_data", i);
      check(nm, strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "50")==0);
    }
    sqlite3_close(db);
  }

  remove(path);
}


static void test_gc_vs_merge(void){
  const char *path = DOLTLITE_TEST_TMPDIR "/test_gc_vs_merge.db";
  pid_t pid;
  int status;
  int pipefd[2];

  printf("--- Test 6: dolt_gc() racing dolt_merge() ---\n");
  setup_db_with_rows(path, 50);

  {
    sqlite3 *db = 0;
    const char *r;
    int i;
    sqlite3_open(path, &db);
    r = queryScalarText(db, "SELECT dolt_branch('feat')");
    check("gc_vs_merge_setup_branch_feat", !looks_like_error(r));
    r = queryScalarText(db, "SELECT dolt_checkout('feat')");
    check("gc_vs_merge_setup_checkout_feat", !looks_like_error(r));
    for(i=51; i<=70; i++){
      char sql[128];
      snprintf(sql, sizeof(sql), "INSERT INTO t VALUES(%d, 'feat_%d')", i, i);
      if( execSql(db, sql)!=SQLITE_OK ){
        fprintf(stderr, "[diag] gc_vs_merge feat INSERT %d failed: %s\n",
                i, sqlite3_errmsg(db));
      }
    }
    r = queryScalarText(db, "SELECT dolt_commit('-A','-m','feat work')");
    check("gc_vs_merge_setup_commit_feat", strlen(r)==40);
    r = queryScalarText(db, "SELECT dolt_checkout('main')");
    check("gc_vs_merge_setup_checkout_main", !looks_like_error(r));
    for(i=71; i<=90; i++){
      char sql[128];
      snprintf(sql, sizeof(sql), "INSERT INTO t VALUES(%d, 'main_%d')", i, i);
      if( execSql(db, sql)!=SQLITE_OK ){
        fprintf(stderr, "[diag] gc_vs_merge main INSERT %d failed: %s\n",
                i, sqlite3_errmsg(db));
      }
    }
    r = queryScalarText(db, "SELECT dolt_commit('-A','-m','main work')");
    check("gc_vs_merge_setup_commit_main", strlen(r)==40);
    sqlite3_close(db);
  }

  mpPipe(pipefd);

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    const char *r;
    close(pipefd[0]);
    sqlite3_open(path, &db);
    sqlite3_busy_timeout(db, 30000);
    mpWrite(pipefd[1], "M");
    close(pipefd[1]);
    r = callWithRetry(db, "SELECT dolt_merge('feat')", 200);
    sqlite3_close(db);
    _exit(looks_like_lock_busy(r) ? 1 : 0);
  }

  {
    sqlite3 *db = 0;
    char buf;
    const char *r;
    close(pipefd[1]);
    mpRead(pipefd[0], &buf);
    close(pipefd[0]);
    sqlite3_open(path, &db);
    sqlite3_busy_timeout(db, 30000);
    r = callWithRetry(db, "SELECT dolt_gc()", 200);
    check("gc_vs_merge_gc_ok", !looks_like_lock_busy(r));
    sqlite3_close(db);
  }

  waitpid(pid, &status, 0);
  check("gc_vs_merge_child_ok",
        WIFEXITED(status) && WEXITSTATUS(status)==0);

  {
    sqlite3 *db = 0;
    sqlite3_open(path, &db);
    check("gc_vs_merge_total_count",
      countRowsDiag(db, "SELECT count(*) FROM t")==90);
    check("gc_vs_merge_feat_rows_present",
      countRowsDiag(db,
        "SELECT count(*) FROM t WHERE id BETWEEN 51 AND 70")==20);
    check("gc_vs_merge_main_rows_present",
      countRowsDiag(db,
        "SELECT count(*) FROM t WHERE id BETWEEN 71 AND 90")==20);
    sqlite3_close(db);
  }

  remove(path);
}


static void test_gc_vs_checkout(void){
  const char *path = DOLTLITE_TEST_TMPDIR "/test_gc_vs_checkout.db";
  pid_t pid;
  int status;

  printf("--- Test 7: dolt_gc() racing dolt_checkout() ---\n");
  setup_db_with_rows(path, 50);

  {
    sqlite3 *db = 0;
    int b;
    sqlite3_open(path, &db);
    for(b=0; b<5; b++){
      char sql[128];
      snprintf(sql, sizeof(sql), "SELECT dolt_branch('b%d')", b);
      queryScalarText(db, sql);
      snprintf(sql, sizeof(sql), "SELECT dolt_checkout('b%d')", b);
      queryScalarText(db, sql);
      snprintf(sql, sizeof(sql), "INSERT INTO t VALUES(%d, 'b%d_mark')", 300+b, b);
      execSql(db, sql);
      snprintf(sql, sizeof(sql), "SELECT dolt_commit('-A','-m','b%d work')", b);
      queryScalarText(db, sql);
      queryScalarText(db, "SELECT dolt_checkout('main')");
    }
    sqlite3_close(db);
  }

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    int i, failures = 0;
    sqlite3_open(path, &db);
    sqlite3_busy_timeout(db, 30000);
    for(i=0; i<30; i++){
      char sql[128];
      const char *r;
      snprintf(sql, sizeof(sql), "SELECT dolt_checkout('b%d')", i%5);
      r = callWithRetry(db, sql, 200);
      if( looks_like_lock_busy(r) ) failures++;
      r = callWithRetry(db, "SELECT dolt_checkout('main')", 200);
      if( looks_like_lock_busy(r) ) failures++;
    }
    sqlite3_close(db);
    _exit(failures==0 ? 0 : (30 + (failures>200 ? 200 : failures)));
  }

  {
    sqlite3 *db = 0;
    int i;
    sqlite3_open(path, &db);
    sqlite3_busy_timeout(db, 30000);
    for(i=0; i<4; i++){
      const char *r = callWithRetry(db, "SELECT dolt_gc()", 200);
      char nm[64];
      snprintf(nm, sizeof(nm), "gc_vs_checkout_gc_iter_%d", i);
      check(nm, !looks_like_lock_busy(r));
      usleep(20000);
    }
    sqlite3_close(db);
  }

  waitpid(pid, &status, 0);
  check("gc_vs_checkout_child_ok",
        WIFEXITED(status) && WEXITSTATUS(status)==0);

  {
    sqlite3 *db = 0;
    int b;
    sqlite3_open(path, &db);
    for(b=0; b<5; b++){
      char sql[128], nm[64];
      snprintf(sql, sizeof(sql), "SELECT dolt_checkout('b%d')", b);
      queryScalarText(db, sql);
      snprintf(sql, sizeof(sql), "SELECT v FROM t WHERE id=%d", 300+b);
      snprintf(nm, sizeof(nm), "gc_vs_checkout_b%d_mark_intact", b);
      {
        char want[32];
        snprintf(want, sizeof(want), "b%d_mark", b);
        check(nm, strcmp(queryScalarText(db, sql), want)==0);
      }
    }
    sqlite3_close(db);
  }

  remove(path);
}


static void test_continuous_writes_with_gc(void){
  const char *path = DOLTLITE_TEST_TMPDIR "/test_continuous_gc.db";
  pid_t writer_pid, gc_pid;
  int status1, status2;
  const int N_WRITES = 40;
  const int N_GCS = 8;

  printf("--- Test 8: Continuous writes hammered by GC loop ---\n");
  setup_db_with_rows(path, 10);

  writer_pid = fork();
  if( writer_pid==0 ){
    sqlite3 *db = 0;
    int i, commit_failures = 0;
    sqlite3_open(path, &db);
    sqlite3_busy_timeout(db, 60000);
    for(i=0; i<N_WRITES; i++){
      char sql[128];
      const char *r;
      snprintf(sql, sizeof(sql), "INSERT INTO t VALUES(%d, 'w_%d')", 5000+i, i);
      if( execSql(db, sql)!=SQLITE_OK ){ sqlite3_close(db); _exit(1); }
      snprintf(sql, sizeof(sql), "SELECT dolt_commit('-A','-m','w_%d')", i);
      r = callWithRetry(db, sql, 200);
      if( strlen(r)!=40 ) commit_failures++;
    }
    sqlite3_close(db);
    _exit(commit_failures==0 ? 0 : (40 + (commit_failures>200 ? 200 : commit_failures)));
  }

  gc_pid = fork();
  if( gc_pid==0 ){
    sqlite3 *db = 0;
    int i, gc_failures = 0;
    sqlite3_open(path, &db);
    sqlite3_busy_timeout(db, 60000);
    for(i=0; i<N_GCS; i++){
      const char *r = callWithRetry(db, "SELECT dolt_gc()", 200);
      if( looks_like_lock_busy(r) ) gc_failures++;
      usleep(60000);
    }
    sqlite3_close(db);
    _exit(gc_failures==0 ? 0 : (50 + (gc_failures>200 ? 200 : gc_failures)));
  }

  waitpid(writer_pid, &status1, 0);
  waitpid(gc_pid, &status2, 0);
  check("continuous_writer_exit_ok",
        WIFEXITED(status1) && WEXITSTATUS(status1)==0);
  check("continuous_gc_exit_ok",
        WIFEXITED(status2) && WEXITSTATUS(status2)==0);

  {
    sqlite3 *db = 0;
    char want[32];
    sqlite3_open(path, &db);
    snprintf(want, sizeof(want), "%d", 10 + N_WRITES);
    check("continuous_final_count",
      strcmp(queryScalarText(db, "SELECT count(*) FROM t"), want)==0);
    snprintf(want, sizeof(want), "%d", N_WRITES);
    check("continuous_all_writer_rows_present",
      strcmp(queryScalarText(db,
        "SELECT count(*) FROM t WHERE id >= 5000 AND id < 5000+40"), want)==0);
    sqlite3_close(db);
  }

  remove(path);
}


static void test_kill_gc_mid_sweep(void){
  const char *path = DOLTLITE_TEST_TMPDIR "/test_kill_gc.db";
  pid_t pid;
  int status;
  int iter;
  int killed_in_progress = 0;

  printf("--- Test 9: SIGKILL during dolt_gc(); recover on reopen ---\n");
  setup_db_with_rows(path, 150);
  make_garbage(path, 60);

  for(iter=0; iter<6; iter++){
    pid = fork();
    if( pid==0 ){
      sqlite3 *db = 0;
      sqlite3_open(path, &db);
      sqlite3_busy_timeout(db, 30000);
      queryScalarText(db, "SELECT dolt_gc()");
      sqlite3_close(db);
      _exit(0);
    }
    usleep(2000 + iter*8000);
    if( kill(pid, SIGKILL)==0 ) killed_in_progress++;
    waitpid(pid, &status, 0);

    {
      sqlite3 *db = 0;
      int rc = sqlite3_open(path, &db);
      char nm[64];
      snprintf(nm, sizeof(nm), "kill_gc_reopen_iter_%d", iter);
      check(nm, rc==SQLITE_OK);
      snprintf(nm, sizeof(nm), "kill_gc_count_iter_%d", iter);
      check(nm,
        strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "150")==0);
      sqlite3_close(db);
    }
  }

  check("kill_gc_at_least_one_killed", killed_in_progress > 0);

  {
    sqlite3 *db = 0;
    const char *r;
    sqlite3_open(path, &db);
    r = queryScalarText(db, "SELECT dolt_gc()");
    check("kill_gc_post_recovery_gc_ok", !looks_like_error(r));
    check("kill_gc_final_count",
      strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "150")==0);
    sqlite3_close(db);
  }

  remove(path);
}


static void test_gc_tmp_file_cleanup(void){
  const char *path = DOLTLITE_TEST_TMPDIR "/test_gc_tmp_cleanup.db";
  char tmpPath[256];
  pid_t pid;
  int status;
  int iter;

  printf("--- Test 10: Orphan -gc-tmp cleaned up after killed GC ---\n");
  setup_db_with_rows(path, 80);
  make_garbage(path, 40);

  for(iter=0; iter<6; iter++){
    pid = fork();
    if( pid==0 ){
      sqlite3 *db = 0;
      sqlite3_open(path, &db);
      sqlite3_busy_timeout(db, 30000);
      queryScalarText(db, "SELECT dolt_gc()");
      sqlite3_close(db);
      _exit(0);
    }
    usleep(2000 + iter*6000);
    kill(pid, SIGKILL);
    waitpid(pid, &status, 0);
  }

  {
    sqlite3 *db = 0;
    const char *r;
    sqlite3_open(path, &db);
    r = queryScalarText(db, "SELECT dolt_gc()");
    check("gc_tmp_followup_gc_ok", !looks_like_error(r));
    sqlite3_close(db);
  }

  snprintf(tmpPath, sizeof(tmpPath), "%s-gc-tmp", path);
  check("gc_tmp_file_removed_after_clean_gc", !file_exists(tmpPath));

  {
    sqlite3 *db = 0;
    sqlite3_open(path, &db);
    check("gc_tmp_data_intact_after_cleanup",
      strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "80")==0);
    sqlite3_close(db);
  }

  remove(path);
  remove(tmpPath);
}


static void test_concurrent_staging_then_gc(void){
  const char *path = DOLTLITE_TEST_TMPDIR "/test_concurrent_stage.db";
  pid_t pid;
  int status;
  int pipefd_stage[2], pipefd_gc[2];

  printf("--- Test 11: Cross-process staged data vs dolt_gc() ---\n");
  setup_db_with_rows(path, 50);
  mpPipe(pipefd_stage);
  mpPipe(pipefd_gc);

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    char buf;
    const char *r;
    close(pipefd_stage[0]);
    close(pipefd_gc[1]);

    sqlite3_open(path, &db);
    sqlite3_busy_timeout(db, 30000);
    execSql(db, "INSERT INTO t VALUES(500, 'staged_a')");
    execSql(db, "INSERT INTO t VALUES(501, 'staged_b')");
    r = callWithRetry(db, "SELECT dolt_add('-A')", 200);
    if( looks_like_lock_busy(r) ){ sqlite3_close(db); _exit(2); }

    mpWrite(pipefd_stage[1], "S");
    close(pipefd_stage[1]);

    mpRead(pipefd_gc[0], &buf);
    close(pipefd_gc[0]);

    r = callWithRetry(db, "SELECT dolt_commit('-m','staged commit')", 200);
    sqlite3_close(db);
    _exit(strlen(r)==40 ? 0 : 3);
  }

  {
    sqlite3 *db = 0;
    char buf;
    const char *r;
    close(pipefd_stage[1]);
    close(pipefd_gc[0]);

    mpRead(pipefd_stage[0], &buf);
    close(pipefd_stage[0]);

    sqlite3_open(path, &db);
    sqlite3_busy_timeout(db, 30000);
    r = callWithRetry(db, "SELECT dolt_gc()", 200);
    check("staging_gc_parent_gc_ok", !looks_like_lock_busy(r));
    sqlite3_close(db);

    mpWrite(pipefd_gc[1], "G");
    close(pipefd_gc[1]);
  }

  waitpid(pid, &status, 0);
  check("staging_gc_child_commit_ok",
        WIFEXITED(status) && WEXITSTATUS(status)==0);

  {
    sqlite3 *db = 0;
    sqlite3_open(path, &db);
    check("staging_gc_final_count",
      strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "52")==0);
    check("staging_gc_staged_a_present",
      strcmp(queryScalarText(db,
        "SELECT v FROM t WHERE id=500"), "staged_a")==0);
    check("staging_gc_staged_b_present",
      strcmp(queryScalarText(db,
        "SELECT v FROM t WHERE id=501"), "staged_b")==0);
    sqlite3_close(db);
  }

  remove(path);
}

static void test_stale_session_roots_after_gc(void){
  const char *path = DOLTLITE_TEST_TMPDIR "/test_stale_session_roots.db";
  sqlite3 *db1 = 0;
  sqlite3 *db2 = 0;
  sqlite3 *db3 = 0;
  const char *r;

  printf("--- Test 12: Stale session roots after reset and GC ---\n");
  remove(path);

  sqlite3_open(path, &db1);
  sqlite3_busy_timeout(db1, 30000);
  check("stale_session_create",
        execSql(db1, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")==SQLITE_OK);
  check("stale_session_insert_base",
        execSql(db1, "INSERT INTO t VALUES(1, 'base')")==SQLITE_OK);
  r = queryScalarText(db1, "SELECT dolt_commit('-A','-m','base')");
  check("stale_session_base_commit", strlen(r)==40);
  check("stale_session_insert_working",
        execSql(db1, "INSERT INTO t VALUES(2, 'working')")==SQLITE_OK);
  check("stale_session_db1_sees_working",
        strcmp(queryScalarText(db1, "SELECT count(*) FROM t"), "2")==0);

  sqlite3_open(path, &db2);
  sqlite3_busy_timeout(db2, 30000);
  r = queryScalarText(db2, "SELECT dolt_reset('--hard')");
  check("stale_session_reset_ok", !looks_like_error(r));
  r = queryScalarText(db2, "SELECT dolt_gc()");
  check("stale_session_db2_gc_ok", !looks_like_error(r));
  check("stale_session_db2_integrity",
        strcmp(queryScalarText(db2, "PRAGMA integrity_check"), "ok")==0);
  sqlite3_close(db2);

  r = queryScalarText(db1, "SELECT dolt_gc()");
  check("stale_session_db1_gc_skips_missing_session_roots",
        !looks_like_error(r));
  sqlite3_close(db1);

  sqlite3_open(path, &db3);
  check("stale_session_final_integrity",
        strcmp(queryScalarText(db3, "PRAGMA integrity_check"), "ok")==0);
  sqlite3_close(db3);

  remove(path);
}


/* A peer rewrites the only branch tip, then compacts. Every tip the idle
** connection remembers is gone from the new file, which must still be
** recognised as the same store: fresh reads, and writes still allowed. */
static void test_peer_rewrite_then_gc(void){
  static const char *azRewrite[] = {
    "SELECT dolt_commit('-a','--amend','-m','c2 amended')",
    "SELECT dolt_reset('--hard','HEAD~1')",
  };
  static const char *azExpect[] = { "1,2,3,4", "1,4" };
  static const char *azCompact[] = { "SELECT dolt_gc()", "VACUUM" };
  const char *path = DOLTLITE_TEST_TMPDIR "/test_peer_rewrite_gc.db";
  int i, j;

  printf("--- Test: idle connection after a peer rewrites the tip and compacts ---\n");
  for(i=0; i<2; i++){
    for(j=0; j<2; j++){
      sqlite3 *a = 0, *b = 0, *c = 0;
      char name[96];
      remove(path);
      sqlite3_open(path, &a);
      sqlite3_busy_timeout(a, 5000);
      execSql(a, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
                 "INSERT INTO t VALUES(1,'base');"
                 "SELECT dolt_commit('-Am','init');"
                 "INSERT INTO t VALUES(2,'two');"
                 "SELECT dolt_commit('-am','c2');");
      queryScalarText(a, "SELECT group_concat(id) FROM t");
      sqlite3_open(path, &b);
      sqlite3_busy_timeout(b, 5000);
      execSql(b, "INSERT INTO t VALUES(3,'three')");
      queryScalarText(b, azRewrite[i]);
      snprintf(name, sizeof(name), "peer_rewrite_%d_compact_%d_ok", i, j);
      check(name, !looks_like_error(queryScalarText(b, azCompact[j])));
      sqlite3_close(b);
      snprintf(name, sizeof(name), "peer_rewrite_%d_compact_%d_write", i, j);
      check(name, execSql(a, "INSERT INTO t VALUES(4,'mine')")==SQLITE_OK);
      sqlite3_close(a);
      sqlite3_open(path, &c);
      snprintf(name, sizeof(name), "peer_rewrite_%d_compact_%d_rows", i, j);
      check(name, strcmp(queryScalarText(c,
            "SELECT group_concat(id) FROM (SELECT id FROM t ORDER BY id)"),
            azExpect[i])==0);
      snprintf(name, sizeof(name), "peer_rewrite_%d_compact_%d_integrity", i, j);
      check(name, strcmp(queryScalarText(c, "PRAGMA integrity_check"), "ok")==0);
      sqlite3_close(c);
    }
  }
  remove(path);
}

static void test_compacted_peer_rewrite(void){
  const char *compact[] = {"SELECT dolt_gc()", "VACUUM"};
  const char *rewrite[] = {
    "INSERT INTO t VALUES(3,'three'); SELECT dolt_commit('-a','--amend','-m','amended')",
    "SELECT dolt_reset('--hard','HEAD~1')",
    "SELECT dolt_reset('--hard','HEAD~1')"
  };
  const char *before[] = {"1,2,3", "1", "1,2"};
  const char *after[] = {"1,2,3,4", "1,4", "1,2,4"};
  const char *path = DOLTLITE_TEST_TMPDIR "/compacted_peer_rewrite.db";
  int first, last, op, readFirst;
  for(first=0; first<2; first++) for(last=0; last<2; last++)
  for(op=0; op<3; op++) for(readFirst=0; readFirst<2; readFirst++){
    sqlite3 *a = 0, *b = 0, *c = 0;
    int version, failures = nFail;
    remove(path);
    check("compacted_peer_open",sqlite3_open(path,&a)==SQLITE_OK);
    check("compacted_peer_seed",execSql(a,
        "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); CREATE INDEX tv ON t(v);"
        "INSERT INTO t VALUES(1,'one'); SELECT dolt_commit('-Am','c1');"
        "INSERT INTO t VALUES(2,'two'); SELECT dolt_commit('-am','c2')")==SQLITE_OK);
    if( op==2 ) check("compacted_peer_delete",execSql(a,
        "DELETE FROM t WHERE id=2; SELECT dolt_commit('-am','deleted')")==SQLITE_OK);
    check("compacted_peer_writer_open",sqlite3_open(path,&b)==SQLITE_OK);
    check("compacted_peer_first_gc",execSql(b,compact[first])==SQLITE_OK);
    check("compacted_peer_adopts_gc",strcmp(queryScalarText(a,
        "SELECT group_concat(id) FROM t"),op==2 ? "1" : "1,2")==0);
    version = atoi(queryScalarText(a,"PRAGMA data_version"));
    check("compacted_peer_rewrite",execSql(b,rewrite[op])==SQLITE_OK);
    check("compacted_peer_second_gc",execSql(b,compact[last])==SQLITE_OK);
    if( readFirst ){
      check("compacted_peer_fresh_rows",strcmp(queryScalarText(a,
          "SELECT group_concat(id) FROM (SELECT id FROM t INDEXED BY tv ORDER BY id)"),before[op])==0);
      check("compacted_peer_data_version",atoi(queryScalarText(a,"PRAGMA data_version"))!=version);
    }
    check("compacted_peer_write",execSql(a,"INSERT INTO t VALUES(4,'mine')")==SQLITE_OK);
    check("compacted_peer_checkout",execSql(a,"SELECT dolt_checkout('main')")==SQLITE_OK);
    sqlite3_close(a); sqlite3_close(b);
    check("compacted_peer_observer",sqlite3_open(path,&c)==SQLITE_OK);
    check("compacted_peer_durable_rows",strcmp(queryScalarText(c,
        "SELECT group_concat(id) FROM (SELECT id FROM t ORDER BY id)"),after[op])==0);
    check("compacted_peer_integrity",strcmp(queryScalarText(c,"PRAGMA integrity_check"),"ok")==0);
    sqlite3_close(c);
    if( nFail!=failures ) fprintf(stderr,"compacted peer case: first=%d last=%d op=%d read=%d\n",first,last,op,readFirst);
  }
  remove(path);
}

static void test_compacted_process_rewrite(void){
  const char *compact[] = {"SELECT dolt_gc()", "VACUUM"};
  const char *path = DOLTLITE_TEST_TMPDIR "/compacted_process_rewrite.db";
  int first, last, reset;
  for(first=0; first<2; first++) for(last=0; last<2; last++) for(reset=0; reset<2; reset++){
    sqlite3 *reader = 0;
    int status = 0, version;
    pid_t pid;
    remove(path);
    check("compacted_process_open",sqlite3_open(path,&reader)==SQLITE_OK);
    check("compacted_process_seed",execSql(reader,
        "CREATE TABLE t(id INTEGER PRIMARY KEY); INSERT INTO t VALUES(1);"
        "SELECT dolt_commit('-Am','c1'); INSERT INTO t VALUES(2);"
        "SELECT dolt_commit('-am','c2')")==SQLITE_OK);
    check("compacted_process_first_gc",execSql(reader,compact[first])==SQLITE_OK);
    version = atoi(queryScalarText(reader,"PRAGMA data_version"));
    pid = fork();
    if( pid==0 ){
      sqlite3 *writer = 0;
      int rc = sqlite3_open(path,&writer);
      if( rc==SQLITE_OK ) rc = execSql(writer,reset
          ? "SELECT dolt_reset('--hard','HEAD~1')"
          : "INSERT INTO t VALUES(3); SELECT dolt_commit('-a','--amend','-m','amended')");
      if( rc==SQLITE_OK ) rc = execSql(writer,compact[last]);
      sqlite3_close(writer);
      _exit(rc==SQLITE_OK ? 0 : 1);
    }
    check("compacted_process_fork",pid>0);
    if( pid>0 ){
      waitpid(pid,&status,0);
      check("compacted_process_writer",WIFEXITED(status) && WEXITSTATUS(status)==0);
      check("compacted_process_fresh_rows",strcmp(queryScalarText(reader,
          "SELECT group_concat(id) FROM t"),reset ? "1" : "1,2,3")==0);
      check("compacted_process_data_version",atoi(queryScalarText(reader,"PRAGMA data_version"))!=version);
      check("compacted_process_write",execSql(reader,"INSERT INTO t VALUES(4)")==SQLITE_OK);
    }
    sqlite3_close(reader);
  }
  remove(path);
}

/* A put that finds its chunk already stored writes nothing. If that chunk is
** an unreferenced leftover, a peer's gc may delete it before the writer
** publishes, and the published ref would dangle. */
static void test_gc_vs_put_deduped_against_garbage(void){
  char path[256];
  char pause[256];
  char ready[300];
  char orphan[64];
  sqlite3 *db = 0;
  int status = 0;
  int i;
  pid_t pid;
  const char *zCommit =
      "SELECT dolt_commit('--allow-empty','-m','e','--date','2026-01-01T00:00:00Z')";

  printf("--- Test: gc vs a put deduplicated against garbage ---\n");
  snprintf(path, sizeof(path), DOLTLITE_TEST_TMPDIR "/mp_gc_dedup_%d.db", (int)getpid());
  snprintf(pause, sizeof(pause), DOLTLITE_TEST_TMPDIR "/mp_gc_dedup_%d.pause", (int)getpid());
  snprintf(ready, sizeof(ready), "%s.ready", pause);
  remove(pause);
  remove(ready);
  setup_db_with_rows(path, 5);

  check("gc_dedup_open", sqlite3_open(path, &db)==SQLITE_OK);
  snprintf(orphan, sizeof(orphan), "%s", queryScalarText(db, zCommit));
  check("gc_dedup_orphan_commit", strlen(orphan)==40);
  check("gc_dedup_orphan_unreferenced",
        execSql(db, "SELECT dolt_reset('--hard','HEAD~1')")==SQLITE_OK);
  sqlite3_close(db);
  db = 0;

  {
    FILE *hold = fopen(pause, "w");
    if( !hold ){ perror("pause"); _exit(1); }
    if( fputc('x', hold)==EOF ){ perror("pause"); _exit(1); }
    fclose(hold);
  }
  pid = fork();
  if( pid==0 ){
    int rc;
    setenv("DOLTLITE_PAUSE_BEFORE_BRANCH_ADVANCE", pause, 1);
    if( sqlite3_open(path, &db)!=SQLITE_OK ) _exit(1);
    sqlite3_busy_timeout(db, 10000);
    rc = execSql(db, zCommit);
    sqlite3_close(db);
    if( rc==SQLITE_BUSY ) _exit(10);
    if( rc==SQLITE_OK ) _exit(0);
    _exit(3);
  }
  if( pid<0 ){ perror("fork"); _exit(1); }
  for( i=0; i<500 && access(ready, F_OK)!=0; i++ ) sqlite3_sleep(20);
  check("gc_dedup_reached_pause", access(ready, F_OK)==0);
  check("gc_dedup_gc_open", sqlite3_open(path, &db)==SQLITE_OK);
  sqlite3_busy_timeout(db, 200);
  check("gc_dedup_gc_refused",
        (execSql(db, "SELECT dolt_gc()")&0xff)==SQLITE_BUSY);
  remove(pause);
  waitpid(pid, &status, 0);
  check("gc_dedup_commit_ok", WIFEXITED(status) && WEXITSTATUS(status)==0);
  sqlite3_busy_timeout(db, 10000);
  check("gc_dedup_gc", execSql(db, "SELECT dolt_gc()")==SQLITE_OK);
  check("gc_dedup_log_readable",
        countRowsDiag(db, "SELECT count(*) FROM dolt_log")>0);
  check("gc_dedup_integrity",
        strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);
  check("gc_dedup_head_is_orphan",
        strcmp(queryScalarText(db, "SELECT dolt_hashof('HEAD')"), orphan)==0);
  check("gc_dedup_retry_log",
        countRowsDiag(db, "SELECT count(*) FROM dolt_log")>0);
  check("gc_dedup_gc_after", execSql(db, "SELECT dolt_gc()")==SQLITE_OK);
  sqlite3_close(db);
  remove(path);
  remove(pause);
  remove(ready);
}

static void mpHoldPause(const char *pause, const char *ready){
  FILE *hold;
  remove(ready);
  hold = fopen(pause, "w");
  if( !hold ){ perror("pause"); _exit(1); }
  if( fputc('x', hold)==EOF ){ perror("pause"); _exit(1); }
  fclose(hold);
}

static int mpAwaitPause(const char *ready){
  int i;
  for( i=0; i<500 && access(ready, F_OK)!=0; i++ ) sqlite3_sleep(20);
  return access(ready, F_OK)==0;
}

/* A peer that writes and collects while a commit is paused mid-command is
** refused rather than sweeping the catalog the commit is about to publish.
** The commit lands, then the peer's write and gc do, and both survive. */
static void test_gc_sweeps_refused_commit_catalog(void){
  char path[256];
  char pause[256];
  char ready[300];
  sqlite3 *db = 0;
  int status = 0;
  pid_t pid;

  printf("--- Test: refused commit after a peer swept its catalog ---\n");
  snprintf(path, sizeof(path), DOLTLITE_TEST_TMPDIR "/mp_gc_refused_%d.db", (int)getpid());
  snprintf(pause, sizeof(pause), DOLTLITE_TEST_TMPDIR "/mp_gc_refused_%d.pause", (int)getpid());
  snprintf(ready, sizeof(ready), "%s.ready", pause);
  setup_db_with_rows(path, 5);
  mpHoldPause(pause, ready);
  pid = fork();
  if( pid==0 ){
    int rc;
    setenv("DOLTLITE_PAUSE_BEFORE_BRANCH_ADVANCE", pause, 1);
    if( sqlite3_open(path, &db)!=SQLITE_OK ) _exit(1);
    sqlite3_busy_timeout(db, 10000);
    if( execSql(db, "INSERT INTO t VALUES(100,'mine')")!=SQLITE_OK ) _exit(2);
    rc = execSql(db, "SELECT dolt_commit('-Am','mine')");
    unsetenv("DOLTLITE_PAUSE_BEFORE_BRANCH_ADVANCE");
    if( rc!=SQLITE_OK ) _exit(10);
    sqlite3_close(db);
    _exit(0);
  }
  if( pid<0 ){ perror("fork"); _exit(1); }
  check("gc_refused_reached_pause", mpAwaitPause(ready));
  check("gc_refused_peer_open", sqlite3_open(path, &db)==SQLITE_OK);
  sqlite3_busy_timeout(db, 200);
  check("gc_refused_peer_insert_refused",
        (execSql(db, "INSERT INTO t VALUES(200,'peer')")&0xff)==SQLITE_BUSY);
  check("gc_refused_peer_gc_refused",
        (execSql(db, "SELECT dolt_gc()")&0xff)==SQLITE_BUSY);
  remove(pause);
  waitpid(pid, &status, 0);
  if( !WIFEXITED(status) || WEXITSTATUS(status)!=0 ){
    fprintf(stderr, "  paused-commit child exit %d\n",
            WIFEXITED(status) ? WEXITSTATUS(status) : -1);
  }
  check("gc_refused_commit_lands",
        WIFEXITED(status) && WEXITSTATUS(status)==0);
  sqlite3_busy_timeout(db, 10000);
  check("gc_refused_peer_insert",
        execSql(db, "INSERT INTO t VALUES(200,'peer')")==SQLITE_OK);
  check("gc_refused_peer_gc", execSql(db, "SELECT dolt_gc()")==SQLITE_OK);
  check("gc_refused_rows_kept",
        countRowsDiag(db, "SELECT count(*) FROM t WHERE id IN (100,200)")==2);
  check("gc_refused_commit_kept",
        countRowsDiag(db, "SELECT count(*) FROM dolt_at_t('HEAD') WHERE id=100")==1);
  check("gc_refused_integrity",
        strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);
  sqlite3_close(db);
  remove(path);
  remove(pause);
  remove(ready);
}

static void test_gc_before_add_persist(void){
  char path[256];
  char pause[256];
  char ready[300];
  sqlite3 *db = 0;
  int status = 0;
  int staged[2], resume[2];
  int gotStaged;
  char ch = 'g';
  pid_t pid;

  printf("--- Test: peer gc between dolt_add's staging and its persist ---\n");
  snprintf(path, sizeof(path), DOLTLITE_TEST_TMPDIR "/mp_gc_addpersist_%d.db", (int)getpid());
  snprintf(pause, sizeof(pause), DOLTLITE_TEST_TMPDIR "/mp_gc_addpersist_%d.pause", (int)getpid());
  snprintf(ready, sizeof(ready), "%s.ready", pause);
  setup_db_with_rows(path, 5);
  mpHoldPause(pause, ready);
  if( pipe(staged) || pipe(resume) ){ perror("pipe"); _exit(1); }
  pid = fork();
  if( pid==0 ){
    int rc;
    close(staged[0]);
    close(resume[1]);
    if( sqlite3_open(path, &db)!=SQLITE_OK ) _exit(1);
    sqlite3_busy_timeout(db, 10000);
    if( execSql(db, "INSERT INTO t VALUES(100,'mine')")!=SQLITE_OK ) _exit(2);
    setenv("DOLTLITE_PAUSE_BEFORE_PERSIST", pause, 1);
    rc = execSql(db, "SELECT dolt_add('.')");
    unsetenv("DOLTLITE_PAUSE_BEFORE_PERSIST");
    if( rc==SQLITE_READONLY ) _exit(10);
    if( rc!=SQLITE_OK && (rc&0xff)!=SQLITE_BUSY ) _exit(11);
    ch = 'd';
    if( write(staged[1], &ch, 1)!=1 ) _exit(14);
    if( read(resume[0], &ch, 1)!=1 || ch!='g' ) _exit(15);
    if( execSql(db, "INSERT INTO t VALUES(300,'after')")!=SQLITE_OK ) _exit(12);
    if( execSql(db, "SELECT dolt_add('.')")!=SQLITE_OK ) _exit(13);
    sqlite3_close(db);
    _exit(0);
  }
  if( pid<0 ){ perror("fork"); _exit(1); }
  close(staged[1]);
  close(resume[0]);
  check("gc_addpersist_reached_pause", mpAwaitPause(ready));
  check("gc_addpersist_peer_open", sqlite3_open(path, &db)==SQLITE_OK);
  sqlite3_busy_timeout(db, 0);
  check("gc_addpersist_peer_busy", (execSql(db, "SELECT dolt_gc()")&0xff)==SQLITE_BUSY);
  remove(pause);
  gotStaged = read(staged[0], &ch, 1)==1 && ch=='d';
  check("gc_addpersist_finished_staging", gotStaged);
  sqlite3_busy_timeout(db, 10000);
  check("gc_addpersist_peer_gc", execSql(db, "SELECT dolt_gc()")==SQLITE_OK);
  ch = 'g';
  if( gotStaged ) check("gc_addpersist_resume", write(resume[1], &ch, 1)==1);
  waitpid(pid, &status, 0);
  close(staged[0]);
  close(resume[1]);
  if( !WIFEXITED(status) || WEXITSTATUS(status)!=0 ){
    fprintf(stderr, "  add-persist child exit %d\n",
            WIFEXITED(status) ? WEXITSTATUS(status) : -1);
  }
  check("gc_addpersist_not_readonly",
        WIFEXITED(status) && WEXITSTATUS(status)==0);
  check("gc_addpersist_rows",
        countRowsDiag(db, "SELECT count(*) FROM t WHERE id IN (100,300)")==2);
  check("gc_addpersist_integrity",
        strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);
  sqlite3_close(db);
  remove(path);
  remove(pause);
  remove(ready);
}

/* gc's own refresh consumes the signal that makes the next statement reload
** the session, so a peer's write must still be visible after this
** connection's gc, not the catalog the sweep may have collected. */
static void test_own_gc_sees_peer_write(void){
  char path[256];
  sqlite3 *db = 0;
  sqlite3 *peer = 0;

  printf("--- Test: reads after this connection's gc see a peer's write ---\n");
  snprintf(path, sizeof(path), DOLTLITE_TEST_TMPDIR "/mp_gc_ownreload_%d.db", (int)getpid());
  setup_db_with_rows(path, 1);
  check("gc_ownreload_open", sqlite3_open(path, &db)==SQLITE_OK);
  check("gc_ownreload_peer_open", sqlite3_open(path, &peer)==SQLITE_OK);
  check("gc_ownreload_local",
        execSql(db, "INSERT INTO t VALUES(2,'mine')")==SQLITE_OK);
  check("gc_ownreload_loaded", countRowsDiag(db, "SELECT count(*) FROM t")==2);
  check("gc_ownreload_peer_write",
        execSql(peer, "INSERT INTO t VALUES(3,'peer'); DELETE FROM t WHERE id=2")
          ==SQLITE_OK);
  check("gc_ownreload_gc", execSql(db, "SELECT dolt_gc()")==SQLITE_OK);
  check("gc_ownreload_sees_peer",
        strcmp(queryScalarText(db,
          "SELECT group_concat(id) FROM (SELECT id FROM t ORDER BY id)"),
          "1,3")==0);
  sqlite3_close(peer);
  sqlite3_close(db);
  remove(path);
}

/* A command reads its source commit inside its write transaction. A peer
** that tries to delete the source branch and run gc while the command runs
** is refused, so the commit cannot be swept from under it; both succeed
** once the command lands, and the store stays intact. */
static void gc_sweeps_vc_source_case(const char *zName, const char *zCmd,
                                     int bDiverge){
  char path[256];
  char pause[256];
  char ready[300];
  char label[128];
  sqlite3 *db = 0;
  int status = 0;
  pid_t pid;

  printf("--- Test: peer deletes and sweeps the source of %s ---\n", zName);
  snprintf(path, sizeof(path), DOLTLITE_TEST_TMPDIR "/mp_gc_src_%s_%d.db",
           zName, (int)getpid());
  snprintf(pause, sizeof(pause), DOLTLITE_TEST_TMPDIR "/mp_gc_src_%s_%d.pause",
           zName, (int)getpid());
  snprintf(ready, sizeof(ready), "%s.ready", pause);
  setup_db_with_rows(path, 5);
  sqlite3_open(path, &db);
  execSql(db, "SELECT dolt_checkout('-b','feat')");
  execSql(db, "UPDATE t SET v='feat' WHERE id=4");
  execSql(db, "SELECT dolt_commit('-Am','feat')");
  execSql(db, "SELECT dolt_checkout('main')");
  if( bDiverge ){
    execSql(db, "UPDATE t SET v='main' WHERE id=1");
    execSql(db, "SELECT dolt_commit('-Am','main')");
  }
  sqlite3_close(db);
  db = 0;

  mpHoldPause(pause, ready);
  pid = fork();
  if( pid==0 ){
    int rc;
    setenv("DOLTLITE_PAUSE_BEFORE_HEAD_CONFIRM", pause, 1);
    if( sqlite3_open(path, &db)!=SQLITE_OK ) _exit(1);
    sqlite3_busy_timeout(db, 10000);
    rc = execSql(db, zCmd);
    sqlite3_close(db);
    if( (rc&0xff)==SQLITE_BUSY ) _exit(10);
    _exit(rc==SQLITE_OK ? 0 : 3);
  }
  if( pid<0 ){ perror("fork"); _exit(1); }
  snprintf(label, sizeof(label), "gc_src_%s_reached_pause", zName);
  check(label, mpAwaitPause(ready));
  sqlite3_open(path, &db);
  sqlite3_busy_timeout(db, 200);
  snprintf(label, sizeof(label), "gc_src_%s_peer_delete_refused", zName);
  check(label, (execSql(db, "SELECT dolt_branch('-D','feat')")&0xff)==SQLITE_BUSY);
  snprintf(label, sizeof(label), "gc_src_%s_peer_gc_refused", zName);
  check(label, (execSql(db, "SELECT dolt_gc()")&0xff)==SQLITE_BUSY);
  remove(pause);
  waitpid(pid, &status, 0);
  snprintf(label, sizeof(label), "gc_src_%s_lands", zName);
  check(label, WIFEXITED(status) && WEXITSTATUS(status)==0);
  sqlite3_busy_timeout(db, 10000);
  snprintf(label, sizeof(label), "gc_src_%s_peer_delete", zName);
  check(label, execSql(db, "SELECT dolt_branch('-D','feat')")==SQLITE_OK);
  snprintf(label, sizeof(label), "gc_src_%s_peer_gc", zName);
  check(label, execSql(db, "SELECT dolt_gc()")==SQLITE_OK);
  sqlite3_close(db);
  sqlite3_open(path, &db);
  snprintf(label, sizeof(label), "gc_src_%s_integrity", zName);
  check(label, strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);
  snprintf(label, sizeof(label), "gc_src_%s_log", zName);
  check(label, countRowsDiag(db, "SELECT count(*) FROM dolt_log")>0);
  snprintf(label, sizeof(label), "gc_src_%s_gc_after", zName);
  check(label, execSql(db, "SELECT dolt_gc()")==SQLITE_OK);
  sqlite3_close(db);
  remove(path);
  remove(pause);
  remove(ready);
}

static void test_deleted_branch_checkout_after_gc(void){
  const char *azBegin[] = {
    0, "BEGIN", "SAVEPOINT caller", "BEGIN; SAVEPOINT caller"
  };
  const char *azWrite[] = {
    "SELECT dolt_commit('-a','--allow-empty','-m','rejected')",
    "SELECT dolt_commit('-A','--amend','-m','rejected')",
    "SELECT dolt_commit('--amend','-m','rejected')",
    "SELECT dolt_reset('--hard')",
    "SELECT dolt_reset('--soft','HEAD')"
  };
  char path[256], head[64], label[128];
  int gc, dirty, boundary;

  printf("--- Test: checkout escapes a peer-deleted branch after gc ---\n");
  snprintf(path, sizeof(path), DOLTLITE_TEST_TMPDIR "/mp_gc_deleted_%d.db",
           (int)getpid());
  for(gc=0; gc<2; gc++){
    for(dirty=0; dirty<2; dirty++){
      for(boundary=0; boundary<4; boundary++){
        sqlite3 *db = 0, *fresh = 0;
        char *zErr = 0;
        pid_t child;
        int rc, status = 0, i;

        setup_db_with_rows(path, 5);
        check("gc_deleted_open", sqlite3_open(path, &db)==SQLITE_OK);
        check("gc_deleted_feature", execSql(db,
            "SELECT dolt_checkout('-b','feat');"
            "CREATE TABLE feat_only(id INTEGER PRIMARY KEY);"
            "INSERT INTO t VALUES(100,'feat');"
            "SELECT dolt_commit('-am','feat commit')")==SQLITE_OK);
        if( dirty ){
          check("gc_deleted_dirty", execSql(db,
              "INSERT INTO t VALUES(200,'dirty')")==SQLITE_OK);
        }
        snprintf(head, sizeof(head), "%s",
                 queryScalarText(db, "SELECT dolt_hashof('HEAD')"));
        if( azBegin[boundary] ){
          check("gc_deleted_begin", execSql(db, azBegin[boundary])==SQLITE_OK);
          check("gc_deleted_pin", countRowsDiag(db,
              "SELECT count(*) FROM t")==6+dirty);
        }
        child = fork();
        if( child<0 ){ perror("fork"); exit(1); }
        if( child==0 ){
          sqlite3 *peer = 0;
          if( sqlite3_open(path, &peer)!=SQLITE_OK ) _exit(1);
          rc = execSql(peer, "SELECT dolt_branch('-D','feat')");
          if( rc==SQLITE_OK && gc ) rc = execSql(peer, "SELECT dolt_gc()");
          sqlite3_close(peer);
          _exit(rc==SQLITE_OK ? 0 : 2);
        }
        check("gc_deleted_wait", waitpid(child, &status, 0)==child);
        check("gc_deleted_peer", WIFEXITED(status) && WEXITSTATUS(status)==0);
        check("gc_deleted_release_cache", execSql(db, "PRAGMA shrink_memory")==SQLITE_OK);
        rc = execSql(db, "SELECT count(*) FROM dolt_status");
        check("gc_deleted_status_not_corrupt",
              rc==SQLITE_OK || (rc&0xff)==SQLITE_BUSY);
        if( gc && !boundary ) check("gc_deleted_status_busy", rc==SQLITE_BUSY);
        for(i=0; i<(int)(sizeof(azWrite)/sizeof(azWrite[0])); i++){
          rc = execSql(db, azWrite[i]);
          snprintf(label, sizeof(label), "gc_deleted_%d_%d_%d_write_%d_busy",
                   gc, dirty, boundary, i);
          check(label, (rc&0xff)==SQLITE_BUSY);
        }
        rc = sqlite3_exec(db, "INSERT INTO t VALUES(300,'rejected')", 0, 0, &zErr);
        check("gc_deleted_insert_busy", (rc&0xff)==SQLITE_BUSY);
        check("gc_deleted_insert_names_branch",
              zErr && (strstr(zErr, "branch 'feat' no longer exists")
                       || (boundary && strstr(zErr, "database is locked"))));
        sqlite3_free(zErr);
        rc = execSql(db, "SELECT dolt_checkout('nosuch')");
        check("gc_deleted_invalid_checkout_refused", rc!=SQLITE_OK);
        check("gc_deleted_invalid_checkout_keeps_head", strcmp(queryScalarText(db,
              "SELECT dolt_hashof('HEAD')"), head)==0);
        check("gc_deleted_release_checkout_cache", execSql(db,
              "PRAGMA shrink_memory")==SQLITE_OK);
        rc = execSql(db, "SELECT dolt_checkout('main')");
        if( (rc&0xff)==SQLITE_BUSY && !sqlite3_get_autocommit(db) ){
          check("gc_deleted_stale_rollback", execSql(db, "ROLLBACK")==SQLITE_OK);
          rc = execSql(db, "SELECT dolt_checkout('main')");
        }
        snprintf(label, sizeof(label), "gc_deleted_%d_%d_%d_checkout_recovers",
                 gc, dirty, boundary);
        check(label, rc==SQLITE_OK);
        if( !sqlite3_get_autocommit(db) ){
          check("gc_deleted_end_transaction", execSql(db, "COMMIT")==SQLITE_OK);
        }
        check("gc_deleted_repeat_checkout", execSql(db,
              "SELECT dolt_checkout('main')")==SQLITE_OK);
        check("gc_deleted_target_rows", countRowsDiag(db, "SELECT count(*) FROM t")==5);
        check("gc_deleted_not_resurrected", countRowsDiag(db,
              "SELECT count(*) FROM dolt_branches WHERE name='feat'")==0);
        check("gc_deleted_target_status", countRowsDiag(db,
              "SELECT count(*) FROM dolt_status")==0);
        check("gc_deleted_recovered_write", execSql(db,
              "INSERT INTO t VALUES(300,'recovered');"
              "SELECT dolt_commit('-am','recovered')")==SQLITE_OK);
        sqlite3_close(db);
        check("gc_deleted_reopen", sqlite3_open(path, &fresh)==SQLITE_OK);
        check("gc_deleted_durable_rows", countRowsDiag(fresh,
              "SELECT count(*) FROM dolt_at_t('main')")==6);
        check("gc_deleted_durable_no_branch", countRowsDiag(fresh,
              "SELECT count(*) FROM dolt_branches WHERE name='feat'")==0);
        check("gc_deleted_integrity", strcmp(queryScalarText(fresh,
              "PRAGMA integrity_check"), "ok")==0);
        sqlite3_close(fresh);
      }
    }
  }
  remove(path);
}

static void test_gc_sweeps_vc_source(void){
  gc_sweeps_vc_source_case("reset_soft", "SELECT dolt_reset('--soft','feat')", 0);
  gc_sweeps_vc_source_case("reset", "SELECT dolt_reset('feat')", 0);
  gc_sweeps_vc_source_case("merge_ff", "SELECT dolt_merge('feat')", 0);
  gc_sweeps_vc_source_case("merge", "SELECT dolt_merge('feat')", 1);
  gc_sweeps_vc_source_case("cherry_pick", "SELECT dolt_cherry_pick('feat')", 1);
}

int main(){
  printf("=== Multi-Process GC Concurrency Tests ===\n\n");

  test_gc_vs_gc_parallel();
  test_gc_blocked_then_retries();
  test_gc_waits_for_busy_writer();
  test_gc_vs_dolt_commit();
  test_reader_iterator_during_gc();
  test_gc_vs_branch_create();
  test_gc_vs_merge();
  test_gc_vs_checkout();
  test_continuous_writes_with_gc();
  test_kill_gc_mid_sweep();
  test_gc_tmp_file_cleanup();
  test_concurrent_staging_then_gc();
  test_stale_session_roots_after_gc();
  test_peer_rewrite_then_gc();
  test_compacted_peer_rewrite();
  test_compacted_process_rewrite();
  test_gc_vs_put_deduped_against_garbage();
  test_gc_sweeps_refused_commit_catalog();
  test_gc_before_add_persist();
  test_own_gc_sees_peer_write();
  test_gc_sweeps_vc_source();
  test_deleted_branch_checkout_after_gc();

  printf("\n=== Results: %d passed, %d failed out of %d tests ===\n",
    nPass, nFail, nPass+nFail);
  return nFail > 0 ? 1 : 0;
}
