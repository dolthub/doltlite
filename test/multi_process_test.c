/*
** Multi-process concurrency tests.
**
** Verifies that two separate OS processes can safely interact with
** the same DoltLite database file. Uses fork() to create child
** processes that operate independently.
**
** Tests:
** 1. Two writers: second process gets SQLITE_BUSY
** 2. Reader during write: reader sees consistent pre-commit state
** 3. Sequential writes from different processes: both succeed
** 4. GC while another process has the file open
** 5. GC blocks if another process holds a write lock
**
** Build from build/ directory:
**   cc -g -I. -o multi_process_test \
**     ../test/multi_process_test.c libdoltlite.a -lz -lpthread
*/
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

static int execsql(sqlite3 *db, const char *sql){
  char *err = 0;
  int rc = sqlite3_exec(db, sql, 0, 0, &err);
  if( err ) sqlite3_free(err);
  return rc;
}

static char result_buf[4096];
static const char *exec1(sqlite3 *db, const char *sql){
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

static void setup_db(const char *path){
  sqlite3 *db = 0;
  remove(path);
  sqlite3_open(path, &db);
  execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");
  execsql(db, "INSERT INTO t VALUES(1, 'original')");
  exec1(db, "SELECT dolt_commit('-A','-m','init')");
  sqlite3_close(db);
}

/*
** Test 1: Two processes try to write simultaneously.
** Child holds a write transaction, parent tries to write — gets BUSY.
*/
static void test_two_writers(void){
  const char *path = "/tmp/test_mp_writers.db";
  pid_t pid;
  int status;

  printf("--- Test 1: Two processes writing simultaneously ---\n");
  setup_db(path);

  pid = fork();
  if( pid==0 ){
    /* Child: open, begin write, hold it for 2 seconds */
    sqlite3 *db = 0;
    sqlite3_open(path, &db);
    execsql(db, "BEGIN");
    execsql(db, "INSERT INTO t VALUES(2, 'from_child')");
    sleep(2);
    execsql(db, "COMMIT");
    sqlite3_close(db);
    _exit(0);
  }

  /* Parent: wait briefly for child to acquire lock, then try to write */
  usleep(200000); /* 200ms — child should have the lock by now */

  {
    sqlite3 *db = 0;
    int rc;
    sqlite3_open(path, &db);
    sqlite3_busy_timeout(db, 100); /* Short timeout — should fail fast */
    rc = execsql(db, "INSERT INTO t VALUES(3, 'from_parent')");
    check("mp_parent_busy", rc==SQLITE_BUSY);
    sqlite3_close(db);
  }

  waitpid(pid, &status, 0);
  check("mp_child_exited_ok", WIFEXITED(status) && WEXITSTATUS(status)==0);

  /* After child exits, parent can write */
  {
    sqlite3 *db = 0;
    int rc;
    sqlite3_open(path, &db);
    rc = execsql(db, "INSERT INTO t VALUES(3, 'from_parent')");
    check("mp_parent_after_child", rc==SQLITE_OK);
    sqlite3_close(db);
  }

  remove(path);
}

/*
** Test 2: Reader during concurrent write.
** Child writes (doesn't commit yet), parent reads — sees pre-commit state.
*/
static void test_reader_during_write(void){
  const char *path = "/tmp/test_mp_readwrite.db";
  pid_t pid;
  int status;

  printf("--- Test 2: Reader during concurrent write ---\n");
  setup_db(path);

  pid = fork();
  if( pid==0 ){
    /* Child: begin write, hold for 2 seconds, then commit */
    sqlite3 *db = 0;
    sqlite3_open(path, &db);
    execsql(db, "BEGIN");
    execsql(db, "INSERT INTO t VALUES(2, 'uncommitted')");
    sleep(2);
    execsql(db, "COMMIT");
    sqlite3_close(db);
    _exit(0);
  }

  usleep(200000); /* Wait for child to start writing */

  /* Parent reads — should see the committed state (1 row), not child's uncommitted INSERT */
  {
    sqlite3 *db = 0;
    sqlite3_open(path, &db);
    check("mp_reader_sees_committed",
      strcmp(exec1(db, "SELECT count(*) FROM t"), "1")==0);
    check("mp_reader_sees_original",
      strcmp(exec1(db, "SELECT v FROM t WHERE id=1"), "original")==0);
    sqlite3_close(db);
  }

  waitpid(pid, &status, 0);

  /* After child commits, a new connection sees both rows */
  {
    sqlite3 *db = 0;
    sqlite3_open(path, &db);
    check("mp_after_child_commit",
      strcmp(exec1(db, "SELECT count(*) FROM t"), "2")==0);
    sqlite3_close(db);
  }

  remove(path);
}

/*
** Test 3: Sequential writes from different processes.
** Child writes and exits. Parent writes after. Both rows survive.
*/
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
    execsql(db, "INSERT INTO t VALUES(2, 'from_child')");
    exec1(db, "SELECT dolt_commit('-A','-m','child commit')");
    sqlite3_close(db);
    _exit(0);
  }

  waitpid(pid, &status, 0);
  check("mp_seq_child_ok", WIFEXITED(status) && WEXITSTATUS(status)==0);

  /* Parent writes after child is done */
  {
    sqlite3 *db = 0;
    sqlite3_open(path, &db);
    execsql(db, "INSERT INTO t VALUES(3, 'from_parent')");
    exec1(db, "SELECT dolt_commit('-A','-m','parent commit')");

    check("mp_seq_count",
      strcmp(exec1(db, "SELECT count(*) FROM t"), "3")==0);
    check("mp_seq_log",
      strcmp(exec1(db, "SELECT count(*) FROM dolt_log"), "3")==0);
    sqlite3_close(db);
  }

  remove(path);
}

/*
** Test 4: GC while another process has the file open for reading.
** The reader should still be able to read (old fd is valid on POSIX).
** After GC, a new open should see the compacted file.
*/
static void test_gc_during_read(void){
  const char *path = "/tmp/test_mp_gc_read.db";
  pid_t pid;
  int status;
  int pipefd[2];

  printf("--- Test 4: GC while another process reads ---\n");
  setup_db(path);

  /* Add more data to make GC meaningful */
  {
    sqlite3 *db = 0;
    int i;
    sqlite3_open(path, &db);
    for(i=2; i<=10; i++){
      char sql[128];
      snprintf(sql, sizeof(sql), "INSERT INTO t VALUES(%d, 'row_%d')", i, i);
      execsql(db, sql);
    }
    exec1(db, "SELECT dolt_commit('-A','-m','add rows')");
    sqlite3_close(db);
  }

  pipe(pipefd);

  pid = fork();
  if( pid==0 ){
    /* Child: open file, signal parent, then hold it open */
    sqlite3 *db = 0;
    char buf;
    close(pipefd[0]);
    sqlite3_open(path, &db);

    /* Verify we can read */
    exec1(db, "SELECT count(*) FROM t");

    /* Signal parent: "I have the file open" */
    write(pipefd[1], "R", 1);
    close(pipefd[1]);

    /* Hold open for 3 seconds while parent does GC */
    sleep(3);

    /* After GC, can we still read? (POSIX: old fd still valid) */
    {
      const char *r = exec1(db, "SELECT count(*) FROM t");
      int ok = (strcmp(r, "10")==0);
      sqlite3_close(db);
      _exit(ok ? 0 : 1);
    }
  }

  /* Parent: wait for child to open file, then GC */
  {
    char buf;
    sqlite3 *db = 0;
    close(pipefd[1]);
    read(pipefd[0], &buf, 1); /* Wait for child's signal */
    close(pipefd[0]);

    sqlite3_open(path, &db);
    exec1(db, "SELECT dolt_gc()");
    sqlite3_close(db);
  }

  waitpid(pid, &status, 0);
  check("mp_gc_child_still_reads", WIFEXITED(status) && WEXITSTATUS(status)==0);

  /* After GC, new connection sees all data */
  {
    sqlite3 *db = 0;
    sqlite3_open(path, &db);
    check("mp_gc_data_intact",
      strcmp(exec1(db, "SELECT count(*) FROM t"), "10")==0);
    sqlite3_close(db);
  }

  remove(path);
}

/*
** Test 5: GC while another process holds a write lock.
** GC involves chunkStoreCommit which acquires the graph lock.
** If another process holds the write lock, GC should get BUSY.
*/
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
    /* Child: hold write lock for 2 seconds */
    sqlite3 *db = 0;
    close(pipefd[0]);
    sqlite3_open(path, &db);
    execsql(db, "BEGIN");
    execsql(db, "INSERT INTO t VALUES(2, 'blocking')");

    /* Signal parent: "I hold the write lock" */
    write(pipefd[1], "W", 1);
    close(pipefd[1]);

    sleep(2);
    execsql(db, "COMMIT");
    sqlite3_close(db);
    _exit(0);
  }

  /* Parent: wait for child to hold lock, then try GC */
  {
    char buf;
    sqlite3 *db = 0;
    const char *r;
    close(pipefd[1]);
    read(pipefd[0], &buf, 1);
    close(pipefd[0]);

    sqlite3_open(path, &db);
    sqlite3_busy_timeout(db, 100); /* Short timeout */
    r = exec1(db, "SELECT dolt_gc()");
    /* GC should fail because child holds the write lock.
    ** It either returns an error or BUSY. */
    check("mp_gc_blocked",
      strstr(r, "ERR")!=0 || strstr(r, "BUSY")!=0 ||
      strstr(r, "busy")!=0 || strstr(r, "locked")!=0 ||
      strlen(r)==0);
    sqlite3_close(db);
  }

  waitpid(pid, &status, 0);

  /* After child exits, GC should succeed */
  {
    sqlite3 *db = 0;
    sqlite3_open(path, &db);
    exec1(db, "SELECT dolt_gc()");
    check("mp_gc_after_writer_ok",
      strcmp(exec1(db, "SELECT count(*) FROM t"), "2")==0);
    sqlite3_close(db);
  }

  remove(path);
}

/*
** Test 6: dolt_commit from two processes to the same branch.
** Second should get conflict error (same as in-process test but
** verifying it works cross-process).
*/
static void test_cross_process_commit_conflict(void){
  const char *path = "/tmp/test_mp_conflict.db";
  pid_t pid;
  int status;
  int pipefd[2];

  printf("--- Test 6: Cross-process commit conflict ---\n");
  setup_db(path);

  pipe(pipefd);

  pid = fork();
  if( pid==0 ){
    /* Child: insert and commit */
    sqlite3 *db = 0;
    char buf;
    close(pipefd[0]);
    sqlite3_open(path, &db);
    execsql(db, "INSERT INTO t VALUES(2, 'child')");
    exec1(db, "SELECT dolt_commit('-A','-m','child commit')");

    /* Signal parent */
    write(pipefd[1], "C", 1);
    close(pipefd[1]);
    sqlite3_close(db);
    _exit(0);
  }

  /* Parent: open before child commits (stale state) */
  {
    sqlite3 *db = 0;
    const char *r;
    char buf;

    sqlite3_open(path, &db);

    /* Wait for child to commit */
    close(pipefd[1]);
    read(pipefd[0], &buf, 1);
    close(pipefd[0]);
    waitpid(pid, &status, 0);

    /* Parent tries to commit — should detect conflict */
    execsql(db, "INSERT INTO t VALUES(3, 'parent')");
    r = exec1(db, "SELECT dolt_commit('-A','-m','parent commit')");
    check("mp_conflict_detected",
      strstr(r, "conflict")!=0 || strstr(r, "ERR")!=0);

    sqlite3_close(db);
  }

  /* Verify child's commit survived */
  {
    sqlite3 *db = 0;
    sqlite3_open(path, &db);
    check("mp_child_commit_survived",
      strcmp(exec1(db, "SELECT count(*) FROM dolt_log"), "2")==0);
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

  printf("\n=== Results: %d passed, %d failed out of %d tests ===\n",
    nPass, nFail, nPass+nFail);
  return nFail > 0 ? 1 : 0;
}
