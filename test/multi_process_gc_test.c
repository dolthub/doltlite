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
  const char *path = "/tmp/test_gc_vs_gc.db";
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
  const char *path = "/tmp/test_gc_blocked_retry.db";
  pid_t pid;
  int status;
  int pipefd[2];

  printf("--- Test 2: GC blocked by writer, retries after commit ---\n");
  setup_db_with_rows(path, 50);
  pipe(pipefd);

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    close(pipefd[0]);
    sqlite3_open(path, &db);
    sqlite3_busy_timeout(db, 5000);
    execSql(db, "BEGIN");
    execSql(db, "INSERT INTO t VALUES(9999, 'pending')");
    write(pipefd[1], "B", 1);
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
    read(pipefd[0], &buf, 1);
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


static void test_gc_vs_dolt_commit(void){
  const char *path = "/tmp/test_gc_vs_commit.db";
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
  const char *path = "/tmp/test_reader_iter_gc.db";
  pid_t pid;
  int status;
  int pipefd_go[2];

  printf("--- Test 4: SELECT iterator open during dolt_gc() ---\n");
  setup_db_with_rows(path, 100);
  make_garbage(path, 40);
  pipe(pipefd_go);

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    char buf;
    const char *r;
    close(pipefd_go[1]);

    read(pipefd_go[0], &buf, 1);
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

    write(pipefd_go[1], "G", 1);
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
  const char *path = "/tmp/test_gc_vs_branch.db";
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
  const char *path = "/tmp/test_gc_vs_merge.db";
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

  pipe(pipefd);

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    const char *r;
    close(pipefd[0]);
    sqlite3_open(path, &db);
    sqlite3_busy_timeout(db, 30000);
    write(pipefd[1], "M", 1);
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
    read(pipefd[0], &buf, 1);
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
  const char *path = "/tmp/test_gc_vs_checkout.db";
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
  const char *path = "/tmp/test_continuous_gc.db";
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
  const char *path = "/tmp/test_kill_gc.db";
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
  const char *path = "/tmp/test_gc_tmp_cleanup.db";
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
  const char *path = "/tmp/test_concurrent_stage.db";
  pid_t pid;
  int status;
  int pipefd_stage[2], pipefd_gc[2];

  printf("--- Test 11: Cross-process staged data vs dolt_gc() ---\n");
  setup_db_with_rows(path, 50);
  pipe(pipefd_stage);
  pipe(pipefd_gc);

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

    write(pipefd_stage[1], "S", 1);
    close(pipefd_stage[1]);

    read(pipefd_gc[0], &buf, 1);
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

    read(pipefd_stage[0], &buf, 1);
    close(pipefd_stage[0]);

    sqlite3_open(path, &db);
    sqlite3_busy_timeout(db, 30000);
    r = callWithRetry(db, "SELECT dolt_gc()", 200);
    check("staging_gc_parent_gc_ok", !looks_like_lock_busy(r));
    sqlite3_close(db);

    write(pipefd_gc[1], "G", 1);
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
  const char *path = "/tmp/test_stale_session_roots.db";
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


int main(){
  printf("=== Multi-Process GC Concurrency Tests ===\n\n");

  test_gc_vs_gc_parallel();
  test_gc_blocked_then_retries();
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

  printf("\n=== Results: %d passed, %d failed out of %d tests ===\n",
    nPass, nFail, nPass+nFail);
  return nFail > 0 ? 1 : 0;
}
