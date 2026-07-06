#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <signal.h>
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

static int isRetryable(int rc){
  return rc==SQLITE_BUSY || rc==SQLITE_LOCKED || rc==SQLITE_BUSY_SNAPSHOT;
}

static int execSqlRetry(sqlite3 *db, const char *sql){
  int i;
  for(i=0; i<500; i++){
    int rc = execSql(db, sql);
    if( rc==SQLITE_OK ) return SQLITE_OK;
    if( !isRetryable(rc) ) return rc;
    sqlite3_sleep(5);
  }
  return SQLITE_BUSY;
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

static int envInt(const char *zName, int defaultValue){
  const char *z = getenv(zName);
  char *zEnd = 0;
  long v;
  if( z==0 || z[0]==0 ) return defaultValue;
  v = strtol(z, &zEnd, 10);
  if( zEnd==z || zEnd[0]!=0 || v<0 || v>0x7fffffff ) return defaultValue;
  return (int)v;
}

static void setup_db(const char *path){
  sqlite3 *db = 0;
  remove(path);
  sqlite3_open(path, &db);
  execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");
  execSql(db, "INSERT INTO t VALUES(1, 'original')");
  queryScalarText(db, "SELECT dolt_commit('-A','-m','init')");
  sqlite3_close(db);
}

static void test_two_writers(void){
  const char *path = "/tmp/test_mp_writers.db";
  pid_t pid;
  int status;

  printf("--- Test 1: Two processes writing simultaneously ---\n");
  setup_db(path);

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(path, &db);
    execSql(db, "BEGIN");
    execSql(db, "INSERT INTO t VALUES(2, 'from_child')");
    sleep(2);
    execSql(db, "COMMIT");
    sqlite3_close(db);
    _exit(0);
  }

  usleep(200000); /* 200ms — child should have the lock by now */

  {
    sqlite3 *db = 0;
    int rc;
    sqlite3_open(path, &db);
    sqlite3_busy_timeout(db, 100); /* Short timeout — should fail fast */
    rc = execSql(db, "INSERT INTO t VALUES(3, 'from_parent')");
    check("mp_parent_busy", rc==SQLITE_BUSY);
    sqlite3_close(db);
  }

  waitpid(pid, &status, 0);
  check("mp_child_exited_ok", WIFEXITED(status) && WEXITSTATUS(status)==0);

  {
    sqlite3 *db = 0;
    int rc;
    sqlite3_open(path, &db);
    rc = execSql(db, "INSERT INTO t VALUES(3, 'from_parent')");
    check("mp_parent_after_child", rc==SQLITE_OK);
    sqlite3_close(db);
  }

  remove(path);
}

static void test_reader_during_write(void){
  const char *path = "/tmp/test_mp_readwrite.db";
  pid_t pid;
  int status;

  printf("--- Test 2: Reader during concurrent write ---\n");
  setup_db(path);

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(path, &db);
    execSql(db, "BEGIN");
    execSql(db, "INSERT INTO t VALUES(2, 'uncommitted')");
    sleep(2);
    execSql(db, "COMMIT");
    sqlite3_close(db);
    _exit(0);
  }

  usleep(200000); /* Wait for child to start writing */

  {
    sqlite3 *db = 0;
    sqlite3_open(path, &db);
    check("mp_reader_sees_committed",
      strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "1")==0);
    check("mp_reader_sees_original",
      strcmp(queryScalarText(db, "SELECT v FROM t WHERE id=1"), "original")==0);
    sqlite3_close(db);
  }

  waitpid(pid, &status, 0);

  {
    sqlite3 *db = 0;
    sqlite3_open(path, &db);
    check("mp_after_child_commit",
      strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "2")==0);
    sqlite3_close(db);
  }

  remove(path);
}

static void test_sequential_processes(void){
  const char *path = "/tmp/test_mp_seq.db";
  pid_t pid;
  int status;

  printf("--- Test 3: Sequential writes from different processes ---\n");
  setup_db(path);

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(path, &db);
    execSql(db, "INSERT INTO t VALUES(2, 'from_child')");
    queryScalarText(db, "SELECT dolt_commit('-A','-m','child commit')");
    sqlite3_close(db);
    _exit(0);
  }

  waitpid(pid, &status, 0);
  check("mp_seq_child_ok", WIFEXITED(status) && WEXITSTATUS(status)==0);

  {
    sqlite3 *db = 0;
    sqlite3_open(path, &db);
    execSql(db, "INSERT INTO t VALUES(3, 'from_parent')");
    queryScalarText(db, "SELECT dolt_commit('-A','-m','parent commit')");

    check("mp_seq_count",
      strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "3")==0);
    check("mp_seq_log",
      strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_log"), "4")==0);
    sqlite3_close(db);
  }

  remove(path);
}

static void test_gc_during_read(void){
  const char *path = "/tmp/test_mp_gc_read.db";
  pid_t pid;
  int status;
  int pipefd[2];

  printf("--- Test 4: GC while another process reads ---\n");
  setup_db(path);

  {
    sqlite3 *db = 0;
    int i;
    sqlite3_open(path, &db);
    for(i=2; i<=10; i++){
      char sql[128];
      snprintf(sql, sizeof(sql), "INSERT INTO t VALUES(%d, 'row_%d')", i, i);
      execSql(db, sql);
    }
    queryScalarText(db, "SELECT dolt_commit('-A','-m','add rows')");
    sqlite3_close(db);
  }

  pipe(pipefd);

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    char buf;
    close(pipefd[0]);
    sqlite3_open(path, &db);

    queryScalarText(db, "SELECT count(*) FROM t");

    write(pipefd[1], "R", 1);
    close(pipefd[1]);

    sleep(3);

    {
      const char *r = queryScalarText(db, "SELECT count(*) FROM t");
      int ok = (strcmp(r, "10")==0);
      sqlite3_close(db);
      _exit(ok ? 0 : 1);
    }
  }

  {
    char buf;
    sqlite3 *db = 0;
    close(pipefd[1]);
    read(pipefd[0], &buf, 1); /* Wait for child's signal */
    close(pipefd[0]);

    sqlite3_open(path, &db);
    queryScalarText(db, "SELECT dolt_gc()");
    sqlite3_close(db);
  }

  waitpid(pid, &status, 0);
  check("mp_gc_child_still_reads", WIFEXITED(status) && WEXITSTATUS(status)==0);

  {
    sqlite3 *db = 0;
    sqlite3_open(path, &db);
    check("mp_gc_data_intact",
      strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "10")==0);
    sqlite3_close(db);
  }

  remove(path);
}

static void test_gc_blocked_by_writer(void){
  const char *path = "/tmp/test_mp_gc_write.db";
  pid_t pid;
  int status;
  int pipefd[2];

  printf("--- Test 5: GC blocked by concurrent writer ---\n");
  setup_db(path);

  pipe(pipefd);

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    close(pipefd[0]);
    sqlite3_open(path, &db);
    execSql(db, "BEGIN");
    execSql(db, "INSERT INTO t VALUES(2, 'blocking')");

    write(pipefd[1], "W", 1);
    close(pipefd[1]);

    sleep(2);
    execSql(db, "COMMIT");
    sqlite3_close(db);
    _exit(0);
  }

  {
    char buf;
    sqlite3 *db = 0;
    const char *r;
    close(pipefd[1]);
    read(pipefd[0], &buf, 1);
    close(pipefd[0]);

    sqlite3_open(path, &db);
    sqlite3_busy_timeout(db, 100); /* Short timeout */
    r = queryScalarText(db, "SELECT dolt_gc()");
    check("mp_gc_blocked",
      strstr(r, "ERR")!=0 || strstr(r, "BUSY")!=0 ||
      strstr(r, "busy")!=0 || strstr(r, "locked")!=0 ||
      strlen(r)==0);
    sqlite3_close(db);
  }

  waitpid(pid, &status, 0);

  {
    sqlite3 *db = 0;
    sqlite3_open(path, &db);
    queryScalarText(db, "SELECT dolt_gc()");
    check("mp_gc_after_writer_ok",
      strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "2")==0);
    sqlite3_close(db);
  }

  remove(path);
}

static void test_cross_process_commit_conflict(void){
  const char *stalePath = "/tmp/test_mp_conflict_stale.db";
  const char *freshPath = "/tmp/test_mp_conflict_fresh.db";
  pid_t pid;
  int status;
  int pipefd[2];

  printf("--- Test 6: Cross-process commit conflict ---\n");

  setup_db(stalePath);

  pipe(pipefd);

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    char buf;
    close(pipefd[0]);
    sqlite3_open(stalePath, &db);
    execSql(db, "INSERT INTO t VALUES(2, 'child')");
    queryScalarText(db, "SELECT dolt_commit('-A','-m','child commit')");

    write(pipefd[1], "C", 1);
    close(pipefd[1]);
    sqlite3_close(db);
    _exit(0);
  }

  {
    sqlite3 *db = 0;
    const char *r;
    char buf;

    sqlite3_open(stalePath, &db);

    close(pipefd[1]);
    read(pipefd[0], &buf, 1);
    close(pipefd[0]);
    waitpid(pid, &status, 0);

    execSql(db, "INSERT INTO t VALUES(3, 'parent')");
    r = queryScalarText(db, "SELECT dolt_commit('-A','-m','parent commit')");
    check("mp_conflict_detected",
      strstr(r, "conflict")!=0 || strstr(r, "ERR")!=0);

    sqlite3_close(db);
  }

  {
    sqlite3 *db = 0;
    sqlite3_open(stalePath, &db);
    check("mp_child_commit_survived",
      strcmp(queryScalarText(db, "SELECT message FROM dolt_log LIMIT 1"),
             "child commit")==0);
    check("mp_stale_parent_not_committed",
      strcmp(queryScalarText(db,
        "SELECT count(*) FROM dolt_log WHERE message='parent commit'"),
        "0")==0);
    sqlite3_close(db);
  }

  remove(stalePath);

  setup_db(freshPath);
  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(freshPath, &db);
    execSql(db, "INSERT INTO t VALUES(2, 'child')");
    queryScalarText(db, "SELECT dolt_commit('-A','-m','child commit')");
    sqlite3_close(db);
    _exit(0);
  }
  waitpid(pid, &status, 0);
  check("mp_fresh_child_ok", WIFEXITED(status) && WEXITSTATUS(status)==0);

  {
    sqlite3 *db = 0;
    const char *r;
    sqlite3_open(freshPath, &db);
    execSql(db, "INSERT INTO t VALUES(3, 'parent')");
    r = queryScalarText(db, "SELECT dolt_commit('-A','-m','parent commit')");
    check("mp_fresh_parent_commits", strlen(r)==40);
    check("mp_fresh_parent_latest",
      strcmp(queryScalarText(db, "SELECT message FROM dolt_log LIMIT 1"),
             "parent commit")==0);
    check("mp_fresh_child_preserved",
      strcmp(queryScalarText(db,
        "SELECT count(*) FROM dolt_log WHERE message='child commit'"),
        "1")==0);
    check("mp_fresh_log_count",
      strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_log"), "4")==0);
    sqlite3_close(db);
  }

  remove(freshPath);
}

static int commit_worker(const char *path, int worker, int nOps, int fdReady, int fdGo){
  sqlite3 *db = 0;
  char ch = 0;
  int op;
  int rc = sqlite3_open(path, &db);
  if( rc!=SQLITE_OK ) return 1;
  sqlite3_busy_timeout(db, 5000);

  if( write(fdReady, "R", 1)!=1 ) return 2;
  if( read(fdGo, &ch, 1)!=1 ) return 3;
  close(fdReady);
  close(fdGo);

  for(op=0; op<nOps; op++){
    char sql[256];
    int attempt;
    int id = worker * 100000 + op;
    snprintf(sql, sizeof(sql),
             "INSERT OR IGNORE INTO t(id, v) VALUES(%d, 'worker-%d-op-%d')",
             id, worker, op);
    for(attempt=0; attempt<500; attempt++){
      rc = execSqlRetry(db, "BEGIN IMMEDIATE");
      if( rc==SQLITE_OK ){
        rc = execSql(db, sql);
        if( rc==SQLITE_OK ){
          rc = execSql(db, "COMMIT");
          if( rc==SQLITE_OK ) break;
        }
        if( rc!=SQLITE_OK ) execSql(db, "ROLLBACK");
      }
      if( !isRetryable(rc) ) break;
      sqlite3_sleep(5);
    }
    if( attempt>=500 || (rc!=SQLITE_OK && !isRetryable(rc)) ){
      sqlite3_close(db);
      return 10;
    }
    if( (op % 7)==0 ) sqlite3_sleep(1);
  }

  sqlite3_close(db);
  return 0;
}

static void test_many_process_commit_contention(void){
  const char *path = "/tmp/test_mp_commit_contention.db";
  enum { N_WORKERS = 6 };
  int nOps = envInt("DOLTLITE_MP_COMMIT_OPS", 200);
  pid_t pids[N_WORKERS];
  int ready[N_WORKERS][2];
  int go[N_WORKERS][2];
  int i;
  int status;

  printf("--- Test 7: Many processes committing to one store ---\n");
  setup_db(path);
  {
    sqlite3 *db = 0;
    sqlite3_open(path, &db);
    execSql(db, "CREATE INDEX IF NOT EXISTS t_v_idx ON t(v)");
    queryScalarText(db, "SELECT dolt_commit('-A','-m','add index')");
    sqlite3_close(db);
  }

  for(i=0; i<N_WORKERS; i++){
    pipe(ready[i]);
    pipe(go[i]);
    pids[i] = fork();
    if( pids[i]==0 ){
      int rc;
      close(ready[i][0]);
      close(go[i][1]);
      rc = commit_worker(path, i+1, nOps, ready[i][1], go[i][0]);
      _exit(rc);
    }
    close(ready[i][1]);
    close(go[i][0]);
    check("mp_commit_fork", pids[i]>0);
  }

  for(i=0; i<N_WORKERS; i++){
    char ch;
    check("mp_commit_worker_ready", read(ready[i][0], &ch, 1)==1);
    close(ready[i][0]);
  }
  for(i=0; i<N_WORKERS; i++){
    check("mp_commit_worker_start", write(go[i][1], "G", 1)==1);
    close(go[i][1]);
  }

  for(i=0; i<N_WORKERS; i++){
    waitpid(pids[i], &status, 0);
    check("mp_commit_worker_ok", WIFEXITED(status) && WEXITSTATUS(status)==0);
  }

  {
    sqlite3 *db = 0;
    char zExpected[32];
    snprintf(zExpected, sizeof(zExpected), "%d", N_WORKERS*nOps);
    sqlite3_open(path, &db);
    check("mp_commit_final_rows",
      strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE id>=100000"),
             zExpected)==0);
    check("mp_commit_index_rows",
      strcmp(queryScalarText(db,
        "SELECT count(*) FROM t INDEXED BY t_v_idx WHERE v >= 'worker-'"),
        zExpected)==0);
    check("mp_commit_log_readable",
      atoi(queryScalarText(db, "SELECT count(*) FROM dolt_log"))>=2);
    check("mp_commit_final_commit",
      strlen(queryScalarText(db,
        "SELECT dolt_commit('-A','-m','mp contention final')"))==40
      || strstr(result_buf, "nothing to commit")!=0);
    sqlite3_close(db);
  }

  remove(path);
}

int main(){
  printf("=== Multi-Process Concurrency Tests ===\n\n");

  test_two_writers();
  test_reader_during_write();
  test_sequential_processes();
  test_gc_during_read();
  test_gc_blocked_by_writer();
  test_cross_process_commit_conflict();
  test_many_process_commit_contention();

  printf("\n=== Results: %d passed, %d failed out of %d tests ===\n",
    nPass, nFail, nPass+nFail);
  return nFail > 0 ? 1 : 0;
}
