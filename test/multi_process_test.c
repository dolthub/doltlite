#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <signal.h>
#include <fcntl.h>
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
  const char *path = DOLTLITE_TEST_TMPDIR "/test_mp_writers.db";
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
  const char *path = DOLTLITE_TEST_TMPDIR "/test_mp_readwrite.db";
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

static void test_reader_after_peer_restore(void){
  const char *path = DOLTLITE_TEST_TMPDIR "/test_mp_restore_refresh.db";
  const char *source = DOLTLITE_TEST_TMPDIR "/test_mp_restore_source.db";
  const char *saved = DOLTLITE_TEST_TMPDIR "/test_mp_restore_refresh_saved.db";
  const char *foreignPath = DOLTLITE_TEST_TMPDIR "/test_mp_restore_foreign.db";
  sqlite3 *reader = 0;
  sqlite3 *idle = 0;
  sqlite3 *gc = 0;
  sqlite3 *foreign = 0;
  pid_t pid;
  int status;

  printf("--- Test 3: Reader refreshes after peer restore ---\n");
  remove(path);
  remove(source);
  remove(saved);
  remove(foreignPath);
  check("mp_restore_reader_open", sqlite3_open(path, &reader)==SQLITE_OK);
  check("mp_restore_reader_seed",
        execSql(reader,
          "CREATE TABLE old_table(x);"
          "SELECT dolt_commit('-A','-m','old state');")==SQLITE_OK);
  check("mp_restore_idle_open", sqlite3_open(path, &idle)==SQLITE_OK);
  check("mp_restore_idle_seed",
        execSql(idle, "SELECT * FROM old_table;")==SQLITE_OK);

  pid = fork();
  if( pid==0 ){
    sqlite3 *src = 0;
    sqlite3 *dest = 0;
    sqlite3_backup *backup = 0;
    int rc = sqlite3_open(source, &src);
    if( rc==SQLITE_OK ) rc = sqlite3_open(path, &dest);
    if( rc==SQLITE_OK ){
      backup = sqlite3_backup_init(dest, "main", src, "main");
      if( backup==0 ) rc = sqlite3_errcode(dest);
    }
    if( rc==SQLITE_OK ) rc = sqlite3_backup_step(backup, -1);
    if( backup ){
      int finishRc = sqlite3_backup_finish(backup);
      if( rc==SQLITE_DONE ) rc = finishRc;
    }
    if( rc==SQLITE_OK ){
      rc = execSql(dest,
        "CREATE TABLE t1(x);"
        "CREATE VIEW v2 AS SELECT x+1 AS y FROM t1;"
        "CREATE VIEW v1 AS SELECT y+1 FROM v2;");
    }
    if( src ) sqlite3_close(src);
    if( dest ) sqlite3_close(dest);
    _exit(rc==SQLITE_OK ? 0 : 1);
  }

  check("mp_restore_fork", pid>0);
  waitpid(pid, &status, 0);
  check("mp_restore_writer_ok",
        WIFEXITED(status) && WEXITSTATUS(status)==0);
  check("mp_restore_reader_sees_peer_schema",
        execSql(reader, "DROP VIEW v1; DROP VIEW v2;")
          ==SQLITE_OK);
  check("mp_restore_reader_commit",
        execSql(reader, "SELECT dolt_commit('-A','-m','after restore');")
          ==SQLITE_OK);
  check("mp_restore_gc_open", sqlite3_open(path, &gc)==SQLITE_OK);
  check("mp_restore_gc", execSql(gc, "SELECT dolt_gc();")==SQLITE_OK);
  sqlite3_close(gc);
  gc = 0;
  check("mp_restore_idle_after_gc",
        execSql(idle, "CREATE TABLE after_gc(x);")==SQLITE_OK);
  sqlite3_close(idle);
  idle = 0;

  check("mp_restore_foreign_open",
        sqlite3_open(foreignPath, &foreign)==SQLITE_OK);
  check("mp_restore_foreign_seed",
        execSql(foreign,
          "CREATE TABLE stranger(x);"
          "SELECT dolt_commit('-A','-m','stranger');")==SQLITE_OK);
  sqlite3_close(foreign);
  foreign = 0;
  check("mp_restore_current_rename", rename(path, saved)==0);
  check("mp_restore_foreign_rename", rename(foreignPath, path)==0);
  check("mp_restore_foreign_still_rejected",
        execSql(reader, "CREATE TABLE must_not_land(x);")==SQLITE_READONLY);

  sqlite3_close(reader);
  sqlite3_close(idle);
  sqlite3_close(gc);
  reader = 0;
  remove(path);
  check("mp_restore_reuse_open",
        sqlite3_open(foreignPath, &foreign)==SQLITE_OK);
  check("mp_restore_reuse_seed",
        execSql(foreign,
          "CREATE TABLE fresh_path(x);"
          "SELECT dolt_commit('-A','-m','fresh path');")==SQLITE_OK);
  sqlite3_close(foreign);
  foreign = 0;
  check("mp_restore_reuse_install", rename(foreignPath, path)==0);
  check("mp_restore_reuse_reader_open",
        sqlite3_open(path, &reader)==SQLITE_OK);
  check("mp_restore_reuse_reader_read",
        execSql(reader, "SELECT * FROM fresh_path;")==SQLITE_OK);
  check("mp_restore_reuse_current_rename", rename(path, foreignPath)==0);
  check("mp_restore_reuse_old_rename", rename(saved, path)==0);
  check("mp_restore_stale_proof_rejected",
        execSql(reader, "CREATE TABLE must_not_return(x);")==SQLITE_READONLY);

  sqlite3_close(reader);
  remove(path);
  remove(source);
  remove(saved);
  remove(foreignPath);
}

static void test_reader_close_during_write_upgrade(void){
  const char *path = DOLTLITE_TEST_TMPDIR "/test_mp_reader_close_upgrade.db";
  int start[2];
  int done[2];
  pid_t pid;
  int status;
  char buf;

  printf("--- Test 2b: Reader close during write upgrade ---\n");
  setup_db(path);
  mpPipe(start);
  mpPipe(done);

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    int childRc = 0;
    close(start[1]);
    close(done[0]);
    mpRead(start[0], &buf);
    if( sqlite3_open(path, &db)!=SQLITE_OK ){
      childRc = 2;
    }else if( strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "1")!=0 ){
      childRc = 3;
    }
    if( db ) sqlite3_close(db);
    mpWrite(done[1], "d");
    _exit(childRc);
  }

  close(start[0]);
  close(done[1]);
  {
    sqlite3 *db = 0;
    sqlite3_open(path, &db);
    sqlite3_extended_result_codes(db, 1);
    check("mp_reader_close_setup_write",
      execSql(db, "UPDATE t SET v='updated' WHERE id=1")==SQLITE_OK);
    check("mp_reader_close_begin", execSql(db, "BEGIN")==SQLITE_OK);
    check("mp_reader_close_snapshot",
      strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "1")==0);

    mpWrite(start[1], "s");
    mpRead(done[0], &buf);
    waitpid(pid, &status, 0);
    check("mp_reader_close_peer_ok",
      WIFEXITED(status) && WEXITSTATUS(status)==0);
    check("mp_reader_close_upgrade_write",
      execSql(db, "INSERT INTO t VALUES(2, 'after reader')")==SQLITE_OK);
    check("mp_reader_close_upgrade_rows",
      strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "2")==0);
    check("mp_reader_close_upgrade_commit", execSql(db, "COMMIT")==SQLITE_OK);
    sqlite3_close(db);
  }
  close(start[1]);
  close(done[0]);

  {
    sqlite3 *db = 0;
    sqlite3_open(path, &db);
    check("mp_reader_close_upgrade_durable",
      strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "2")==0);
    sqlite3_close(db);
  }

  remove(path);
}

static void test_add_during_transaction(void){
  const char *path = DOLTLITE_TEST_TMPDIR "/test_mp_add_txn.db";
  int ready[2];
  int release[2];
  pid_t pid;
  int status;
  char buf;

  printf("--- Test 2c: dolt_add during a transaction ---\n");
  setup_db(path);
  mpPipe(ready);
  mpPipe(release);

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    close(ready[0]);
    close(release[1]);
    if( sqlite3_open(path, &db)!=SQLITE_OK ) _exit(2);
    if( execSql(db, "BEGIN")!=SQLITE_OK ) _exit(3);
    if( execSql(db, "INSERT INTO t VALUES(2, 'uncommitted')")!=SQLITE_OK ){
      _exit(4);
    }
    if( execSql(db, "SELECT dolt_add('t')")!=SQLITE_OK ) _exit(5);
    mpWrite(ready[1], "r");
    mpRead(release[0], &buf);
    _exit(0);
  }

  close(ready[1]);
  close(release[0]);
  mpRead(ready[0], &buf);

  {
    sqlite3 *db = 0;
    sqlite3_open(path, &db);
    check("mp_add_peer_rows_committed_only",
      strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "1")==0);
    check("mp_add_peer_status_empty",
      strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_status"), "0")==0);
    sqlite3_close(db);
  }

  mpWrite(release[1], "x");
  close(ready[0]);
  close(release[1]);
  waitpid(pid, &status, 0);
  check("mp_add_writer_exited_ok",
    WIFEXITED(status) && WEXITSTATUS(status)==0);

  {
    sqlite3 *db = 0;
    sqlite3_open(path, &db);
    check("mp_add_crash_rows_committed_only",
      strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "1")==0);
    check("mp_add_crash_status_empty",
      strcmp(queryScalarText(db, "SELECT count(*) FROM dolt_status"), "0")==0);
    check("mp_add_working_update",
      execSql(db, "UPDATE t SET v='working' WHERE id=1")==SQLITE_OK);
    check("mp_add_first_op_begin", execSql(db, "BEGIN")==SQLITE_OK);
    check("mp_add_first_op_stage",
      execSql(db, "SELECT dolt_add('t')")==SQLITE_OK);
    check("mp_add_first_op_rollback", execSql(db, "ROLLBACK")==SQLITE_OK);
    check("mp_add_first_op_rollback_unstaged",
      strcmp(queryScalarText(db,
        "SELECT staged FROM dolt_status WHERE table_name='t'"), "0")==0);
    check("mp_add_first_op_rollback_keeps_working",
      strcmp(queryScalarText(db, "SELECT v FROM t WHERE id=1"), "working")==0);
    sqlite3_close(db);
  }

  remove(path);
}

static void test_sequential_processes(void){
  const char *path = DOLTLITE_TEST_TMPDIR "/test_mp_seq.db";
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
  const char *path = DOLTLITE_TEST_TMPDIR "/test_mp_gc_read.db";
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

  mpPipe(pipefd);

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    char buf;
    close(pipefd[0]);
    sqlite3_open(path, &db);

    queryScalarText(db, "SELECT count(*) FROM t");

    mpWrite(pipefd[1], "R");
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
    mpRead(pipefd[0], &buf); /* Wait for child's signal */
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
  const char *path = DOLTLITE_TEST_TMPDIR "/test_mp_gc_write.db";
  pid_t pid;
  int status;
  int pipefd[2];

  printf("--- Test 5: GC blocked by concurrent writer ---\n");
  setup_db(path);

  mpPipe(pipefd);

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    close(pipefd[0]);
    sqlite3_open(path, &db);
    execSql(db, "BEGIN");
    execSql(db, "INSERT INTO t VALUES(2, 'blocking')");

    mpWrite(pipefd[1], "W");
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
    mpRead(pipefd[0], &buf);
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

/* The unpinned counterpart: a process that opened before a peer committed
** adopts the branch head at its next statement, so its commit stacks on the
** peer's instead of being refused. Verified against Dolt 2.2.2 with two
** sql-server sessions: the second commit is accepted and both commits and
** both rows survive. */
static void test_cross_process_commit_after_peer(void){
  const char *path = DOLTLITE_TEST_TMPDIR "/test_mp_commit_after_peer.db";
  pid_t pid;
  int status;
  sqlite3 *db = 0;
  const char *r;

  printf("--- Test 6b: commit after a peer commit is accepted ---\n");

  setup_db(path);
  sqlite3_open(path, &db);

  pid = fork();
  if( pid==0 ){
    sqlite3 *cdb = 0;
    sqlite3_open(path, &cdb);
    execSql(cdb, "INSERT INTO t VALUES(2, 'child')");
    queryScalarText(cdb, "SELECT dolt_commit('-A','-m','child commit')");
    sqlite3_close(cdb);
    _exit(0);
  }
  waitpid(pid, &status, 0);
  check("mp_after_peer_child_ok", WIFEXITED(status) && WEXITSTATUS(status)==0);

  execSql(db, "INSERT INTO t VALUES(3, 'parent')");
  r = queryScalarText(db, "SELECT dolt_commit('-A','-m','parent commit')");
  check("mp_after_peer_commit_accepted", strlen(r)==40);
  sqlite3_close(db);

  sqlite3_open(path, &db);
  check("mp_after_peer_head_is_parent",
    strcmp(queryScalarText(db, "SELECT message FROM dolt_log LIMIT 1"),
           "parent commit")==0);
  check("mp_after_peer_child_survived",
    strcmp(queryScalarText(db,
      "SELECT count(*) FROM dolt_log WHERE message='child commit'"), "1")==0);
  check("mp_after_peer_rows",
    strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "3")==0);
  sqlite3_close(db);
  remove(path);
}

static void test_cross_process_commit_conflict(void){
  const char *stalePath = DOLTLITE_TEST_TMPDIR "/test_mp_conflict_stale.db";
  const char *freshPath = DOLTLITE_TEST_TMPDIR "/test_mp_conflict_fresh.db";
  pid_t pid;
  int status;
  int pipefd[2];
  int startfd[2];

  printf("--- Test 6: Cross-process commit conflict ---\n");

  setup_db(stalePath);

  mpPipe(pipefd);
  mpPipe(startfd);

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    char buf;
    close(pipefd[0]);
    close(startfd[1]);
    sqlite3_open(stalePath, &db);
    mpRead(startfd[0], &buf);
    close(startfd[0]);
    execSql(db, "INSERT INTO t VALUES(2, 'child')");
    queryScalarText(db, "SELECT dolt_commit('-A','-m','child commit')");

    mpWrite(pipefd[1], "C");
    close(pipefd[1]);
    sqlite3_close(db);
    _exit(0);
  }

  {
    sqlite3 *db = 0;
    const char *r;
    char buf;

    sqlite3_open(stalePath, &db);
    /* Pin this process's snapshot before the child commits, so it truly
    ** cannot observe that commit. Without the pin it would adopt the branch
    ** head at its next statement and stack on the child's commit, which is
    ** what Dolt does with two live sessions -- covered by
    ** test_cross_process_commit_after_peer below. */
    execSql(db, "BEGIN");
    queryScalarText(db, "SELECT count(*) FROM t");
    close(startfd[0]);
    mpWrite(startfd[1], "P");
    close(startfd[1]);

    close(pipefd[1]);
    mpRead(pipefd[0], &buf);
    close(pipefd[0]);
    waitpid(pid, &status, 0);
    check("mp_conflict_child_exit", WIFEXITED(status) && WEXITSTATUS(status)==0);

    execSql(db, "INSERT INTO t VALUES(3, 'parent')");
    r = queryScalarText(db, "SELECT dolt_commit('-A','-m','parent commit')");
    check("mp_conflict_detected",
      strstr(r, "conflict")!=0 || strstr(r, "ERR")!=0);
    execSql(db, "ROLLBACK");

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

static void test_stale_commit_variants(void){
  static const char *azOp[] = {
    "SELECT dolt_commit('--amend','-m','caller')",
    "SELECT dolt_commit('-a','--amend','-m','caller')",
    "SELECT dolt_commit('-A','--amend','-m','caller')",
    "SELECT dolt_commit('--allow-empty','-m','caller')",
    "SELECT dolt_commit('-a','--allow-empty','-m','caller')",
    "SELECT dolt_commit('-A','--allow-empty','-m','caller')",
    "SELECT dolt_commit('-a','-m','caller')",
    "SELECT dolt_commit('-A','-m','caller')",
    "SELECT dolt_commit('-a','--skip-empty','-m','caller')",
    "SELECT dolt_commit('-A','--skip-empty','-m','caller')"
  };
  static const char *azBegin[] = {
    "BEGIN", "SAVEPOINT caller", "BEGIN; SAVEPOINT caller"
  };
  char path[256];
  int boundary, op;

  printf("--- Stale commit variants preserve a peer process's commit ---\n");
  snprintf(path, sizeof(path), DOLTLITE_TEST_TMPDIR "/mp_stale_commit_%d.db",
           (int)getpid());
  for(boundary=0; boundary<3; boundary++){
    for(op=0; op<(int)(sizeof(azOp)/sizeof(azOp[0])); op++){
      sqlite3 *db = 0, *fresh = 0;
      char head[64], staged[64], peerHead[64], label[128];
      int rc, status;
      pid_t child;

      setup_db(path);
      check("mp_stale_commit_open", sqlite3_open(path, &db)==SQLITE_OK);
      check("mp_stale_commit_seed", execSql(db,
          "INSERT INTO t VALUES(2,'two'); SELECT dolt_commit('-am','c2')")
          ==SQLITE_OK);
      snprintf(head, sizeof(head), "%s",
               queryScalarText(db, "SELECT dolt_hashof('HEAD')"));
      check("mp_stale_commit_begin", execSql(db, azBegin[boundary])==SQLITE_OK);
      check("mp_stale_commit_pin", strcmp(queryScalarText(db,
            "SELECT message FROM dolt_log LIMIT 1"), "c2")==0);
      check("mp_stale_commit_read", strcmp(queryScalarText(db,
            "SELECT count(*) FROM t"), "2")==0);
      snprintf(staged, sizeof(staged), "%s",
               queryScalarText(db, "SELECT count(*) FROM dolt_status WHERE staged"));
      check("mp_stale_commit_staging_valid", strcmp(staged, "0")==0);
      child = fork();
      if( child<0 ){ perror("fork"); exit(1); }
      if( child==0 ){
        sqlite3 *peer = 0;
        if( sqlite3_open(path, &peer)!=SQLITE_OK ) _exit(1);
        sqlite3_busy_timeout(peer, 5000);
        rc = execSql(peer, "INSERT INTO t VALUES(900,'peer');"
                           "SELECT dolt_commit('-am','peer commit')");
        sqlite3_close(peer);
        _exit(rc==SQLITE_OK ? 0 : 2);
      }
      check("mp_stale_commit_peer_wait", waitpid(child, &status, 0)==child);
      check("mp_stale_commit_peer_exit", WIFEXITED(status) && WEXITSTATUS(status)==0);
      check("mp_stale_commit_observer", sqlite3_open(path, &fresh)==SQLITE_OK);
      snprintf(peerHead, sizeof(peerHead), "%s",
               queryScalarText(fresh, "SELECT dolt_hashof('HEAD')"));
      check("mp_stale_commit_peer_advanced", strcmp(peerHead, head)!=0);
      rc = execSql(db, azOp[op]);
      snprintf(label, sizeof(label), "mp_stale_commit_%d_%d_busy", boundary, op);
      check(label, (rc&0xff)==SQLITE_BUSY);
      check("mp_stale_commit_caller_transaction", !sqlite3_get_autocommit(db));
      check("mp_stale_commit_snapshot_head", strcmp(queryScalarText(db,
            "SELECT dolt_hashof('HEAD')"), head)==0);
      check("mp_stale_commit_staging_unchanged", strcmp(queryScalarText(db,
            "SELECT count(*) FROM dolt_status WHERE staged"), staged)==0);
      snprintf(label, sizeof(label), "mp_stale_commit_%d_%d_peer_hash", boundary, op);
      check(label, strcmp(queryScalarText(fresh,
            "SELECT dolt_hashof('HEAD')"), peerHead)==0);
      check("mp_stale_commit_peer_message", strcmp(queryScalarText(fresh,
            "SELECT message FROM dolt_log LIMIT 1"), "peer commit")==0);
      check("mp_stale_commit_no_caller_commit", strcmp(queryScalarText(fresh,
            "SELECT count(*) FROM dolt_log WHERE message='caller'"), "0")==0);
      check("mp_stale_commit_rollback", execSql(db, "ROLLBACK")==SQLITE_OK);
      sqlite3_close(db);
      sqlite3_close(fresh);
      check("mp_stale_commit_reopen", sqlite3_open(path, &fresh)==SQLITE_OK);
      check("mp_stale_commit_durable_hash", strcmp(queryScalarText(fresh,
            "SELECT dolt_hashof('HEAD')"), peerHead)==0);
      check("mp_stale_commit_durable_rows", strcmp(queryScalarText(fresh,
            "SELECT group_concat(id) FROM (SELECT id FROM t ORDER BY id)"),
            "1,2,900")==0);
      check("mp_stale_commit_committed_rows", strcmp(queryScalarText(fresh,
            "SELECT group_concat(id) FROM (SELECT id FROM dolt_at_t('main') ORDER BY id)"),
            "1,2,900")==0);
      check("mp_stale_commit_integrity", strcmp(queryScalarText(fresh,
            "PRAGMA integrity_check"), "ok")==0);
      sqlite3_close(fresh);
    }
  }
  remove(path);
}

typedef struct StaleCommitPeer StaleCommitPeer;
struct StaleCommitPeer {
  int toPeer;
  int fromPeer;
  int target;
  int tick;
  int fired;
  char result;
};

static int staleCommitProgress(void *pArg){
  StaleCommitPeer *p = (StaleCommitPeer*)pArg;
  if( ++p->tick==p->target && !p->fired ){
    p->fired = 1;
    mpWrite(p->toPeer, "w");
    mpRead(p->fromPeer, &p->result);
  }
  return 0;
}

static void test_stale_commit_stage_race(void){
  const char *azOp[] = {
    "SELECT dolt_commit('-a','--amend','-m','caller')",
    "SELECT dolt_commit('-A','--amend','-m','caller')"
  };
  char path[256];
  int op;
  printf("--- Amend keeps its pinned HEAD while automatic staging races ---\n");
  snprintf(path, sizeof(path), DOLTLITE_TEST_TMPDIR "/mp_stale_stage_%d.db",
           (int)getpid());
  for(op=0; op<2; op++){
    int target, nAcked = 0, nBad = 0;
    for(target=1; target<=1000; target++){
      sqlite3 *db = 0, *fresh = 0;
      StaleCommitPeer p;
      int toPeer[2], fromPeer[2], rc, status;
      pid_t child;
      char ch;
      setup_db(path);
      check("mp_stale_stage_setup_open", sqlite3_open(path, &db)==SQLITE_OK);
      check("mp_stale_stage_setup", execSql(db,
          "INSERT INTO t VALUES(2,'two'); SELECT dolt_commit('-am','c2')")
          ==SQLITE_OK);
      sqlite3_close(db);
      mpPipe(toPeer);
      mpPipe(fromPeer);
      child = fork();
      if( child<0 ){ perror("fork"); exit(1); }
      if( child==0 ){
        sqlite3 *peer = 0;
        close(toPeer[1]);
        close(fromPeer[0]);
        if( sqlite3_open(path, &peer)!=SQLITE_OK ) _exit(1);
        mpWrite(fromPeer[1], "r");
        mpRead(toPeer[0], &ch);
        if( ch=='w' ){
          rc = execSql(peer, "INSERT INTO t VALUES(900,'peer');"
                            "SELECT dolt_commit('-am','peer commit')");
          mpWrite(fromPeer[1], rc==SQLITE_OK ? "o" : isRetryable(rc) ? "b" : "e");
        }
        sqlite3_close(peer);
        _exit(0);
      }
      close(toPeer[0]);
      close(fromPeer[1]);
      mpRead(fromPeer[0], &ch);
      check("mp_stale_stage_open", sqlite3_open(path, &db)==SQLITE_OK);
      check("mp_stale_stage_begin", execSql(db, "BEGIN")==SQLITE_OK);
      check("mp_stale_stage_pin", strcmp(queryScalarText(db,
            "SELECT count(*) FROM t"), "2")==0);
      memset(&p, 0, sizeof(p));
      p.toPeer = toPeer[1];
      p.fromPeer = fromPeer[0];
      p.target = target;
      sqlite3_progress_handler(db, 1, staleCommitProgress, &p);
      rc = execSql(db, azOp[op]);
      sqlite3_progress_handler(db, 0, 0, 0);
      if( !p.fired ) mpWrite(toPeer[1], "q");
      close(toPeer[1]);
      close(fromPeer[0]);
      waitpid(child, &status, 0);
      if( !WIFEXITED(status) || WEXITSTATUS(status)!=0 || p.result=='e'
       || (rc!=SQLITE_OK && !isRetryable(rc)) ) nBad++;
      if( p.result=='o' ){
        nAcked++;
        if( (rc&0xff)!=SQLITE_BUSY ) nBad++;
        sqlite3_open(path, &fresh);
        if( strcmp(queryScalarText(fresh,
              "SELECT message FROM dolt_log LIMIT 1"), "peer commit") ) nBad++;
        if( strcmp(queryScalarText(fresh,
              "SELECT count(*) FROM dolt_log WHERE message='caller'"), "0") ) nBad++;
        if( strcmp(queryScalarText(fresh, "SELECT group_concat(id) FROM "
              "(SELECT id FROM dolt_at_t('main') ORDER BY id)"), "1,2,900") ) nBad++;
        sqlite3_close(fresh);
      }
      execSql(db, "ROLLBACK");
      sqlite3_close(db);
      if( !p.fired ) break;
    }
    printf("  amend %s: peer commits=%d violations=%d\n", op ? "-A" : "-a",
           nAcked, nBad);
    check("mp_stale_stage_peer_exercised", nAcked>0);
    check("mp_stale_stage_peer_commit_kept", nBad==0);
    check("mp_stale_stage_terminated", target<=1000);
  }
  remove(path);
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
  const char *path = DOLTLITE_TEST_TMPDIR "/test_mp_commit_contention.db";
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
    mpPipe(ready[i]);
    mpPipe(go[i]);
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

typedef struct MpRefBusyCtx MpRefBusyCtx;
struct MpRefBusyCtx {
  int releaseFd;
  int readyFd;
  int calls;
};

/* Releases the peer on the first wait, then keeps retrying while the peer
** finishes letting go of the file: its close appends a clean-close marker,
** and a statement that starts meanwhile waits as in stock SQLite. */
static int mpReleaseBusyPeer(void *arg, int attempt){
  MpRefBusyCtx *p = (MpRefBusyCtx*)arg;
  char ch;
  if( p->calls++==0 ){
    mpWrite(p->releaseFd, "G");
    mpRead(p->readyFd, &ch);
    return ch=='D';
  }
  if( attempt>2000 ) return 0;
  sqlite3_sleep(5);
  return 1;
}

static void test_ref_commands_busy_handler(void){
  static const char *azSql[] = {
    "SELECT dolt_branch('new')",
    "SELECT dolt_tag('new')",
    "SELECT dolt_checkout('feature')",
    "SELECT dolt_checkout('-b','new')",
    "SELECT dolt_reset('--hard')",
    "SELECT dolt_clean()",
    "VACUUM",
    "SELECT dolt_commit('--allow-empty','-m','waited')",
    "SELECT dolt_commit('-A','--allow-empty','-m','waited')"
  };
  char path[256];
  int i, useTimeout, commitPeer;
  snprintf(path, sizeof(path), DOLTLITE_TEST_TMPDIR "/mp_ref_busy_%d.db", (int)getpid());
  for(commitPeer=0; commitPeer<2; commitPeer++){
    for(useTimeout=0; useTimeout<2; useTimeout++){
      for(i=0; i<(int)(sizeof(azSql)/sizeof(azSql[0])); i++){
        sqlite3 *db = 0;
        int ready[2], release[2];
        int rc, status;
        pid_t pid;
        char ch;
        MpRefBusyCtx ctx;
        if( commitPeer && !strstr(azSql[i],"dolt_checkout")
         && !strstr(azSql[i],"dolt_commit") ) continue;
        remove(path);
        check("mp_ref_busy_open", sqlite3_open(path, &db)==SQLITE_OK);
        check("mp_ref_busy_seed", execSql(db,
          "CREATE TABLE t(id INTEGER PRIMARY KEY); INSERT INTO t VALUES(1);"
          "SELECT dolt_commit('-Am','base');"
          "SELECT dolt_branch('feature');")==SQLITE_OK);
        if( strstr(azSql[i], "dolt_clean") ){
          check("mp_ref_busy_clean_untracked", execSql(db,
            "CREATE TABLE scratch(id INTEGER PRIMARY KEY)")==SQLITE_OK);
        }
        sqlite3_close(db);
        mpPipe(ready);
        mpPipe(release);
        pid = fork();
        if( pid==0 ){
          close(ready[0]);
          close(release[1]);
          if( sqlite3_open(path, &db)!=SQLITE_OK ) _exit(1);
          if( execSql(db, "BEGIN IMMEDIATE; INSERT INTO t VALUES(2)")!=SQLITE_OK ) _exit(2);
          mpWrite(ready[1], "R");
          mpRead(release[0], &ch);
          if( useTimeout ) sqlite3_sleep(100);
          if( execSql(db, commitPeer ? "COMMIT" : "ROLLBACK")!=SQLITE_OK ) _exit(3);
          sqlite3_close(db);
          mpWrite(ready[1], "D");
          _exit(0);
        }
        if( pid<0 ){ perror("fork"); _exit(1); }
        close(ready[1]);
        close(release[0]);
        mpRead(ready[0], &ch);
        check("mp_ref_busy_peer_ready", ch=='R');
        check("mp_ref_busy_parent_open", sqlite3_open(path, &db)==SQLITE_OK);
        memset(&ctx, 0, sizeof(ctx));
        ctx.releaseFd = release[1];
        ctx.readyFd = ready[0];
        if( useTimeout ){
          sqlite3_busy_timeout(db, 5000);
          mpWrite(release[1], "G");
        }else{
          sqlite3_busy_handler(db, mpReleaseBusyPeer, &ctx);
        }
        rc = execSql(db, azSql[i]);
        if( rc!=SQLITE_OK ) fprintf(stderr, "%s: %s\n", azSql[i], sqlite3_errmsg(db));
        check(useTimeout ? "mp_ref_busy_timeout_waits" : "mp_ref_busy_handler_waits", rc==SQLITE_OK);
        if( strstr(azSql[i], "dolt_clean") ){
          check("mp_ref_busy_clean_removed", strcmp(queryScalarText(db,
            "SELECT count(*) FROM sqlite_master WHERE name='scratch'"), "0")==0);
        }
        if( !useTimeout ){
          check("mp_ref_busy_callback_invoked", ctx.calls>0);
          if( ctx.calls==0 ) mpWrite(release[1], "G");
        }
        waitpid(pid, &status, 0);
        check("mp_ref_busy_peer_released", WIFEXITED(status) && WEXITSTATUS(status)==0);
        close(ready[0]);
        close(release[1]);
        check("mp_ref_busy_rows_preserved", strcmp(queryScalarText(db,
          "SELECT count(*) FROM t"),
          commitPeer && strstr(azSql[i],"dolt_commit") ? "2" : "1")==0);
        if( commitPeer ){
          sqlite3 *observer = 0;
          check("mp_ref_busy_observer_open",sqlite3_open(path,&observer)==SQLITE_OK);
          check("mp_ref_busy_committed_peer_kept",strcmp(queryScalarText(observer,
            "SELECT count(*) FROM t WHERE id=2"),"1")==0);
          sqlite3_close(observer);
        }
        sqlite3_close(db);
      }
    }
  }
  remove(path);
}

static void test_write_after_lost_commit_race(void){
  char path[256];
  sqlite3 *db = 0;
  int ready[2], release[2], done[2];
  int rc, status;
  pid_t pid;
  char ch;
  MpRefBusyCtx ctx;

  printf("--- Test 6a: Autocommit write after a lost commit race ---\n");
  snprintf(path, sizeof(path), DOLTLITE_TEST_TMPDIR "/mp_lost_race_%d.db", (int)getpid());
  setup_db(path);
  check("mp_lost_race_open", sqlite3_open(path, &db)==SQLITE_OK);
  check("mp_lost_race_first_write", execSql(db, "INSERT INTO t VALUES(2, 'mine')")==SQLITE_OK);
  mpPipe(ready);
  mpPipe(release);
  mpPipe(done);
  pid = fork();
  if( pid==0 ){
    close(ready[0]);
    close(release[1]);
    close(done[1]);
    db = 0;
    if( sqlite3_open(path, &db)!=SQLITE_OK ) _exit(1);
    sqlite3_busy_timeout(db, 10000);
    if( execSql(db, "BEGIN IMMEDIATE; INSERT INTO t VALUES(10, 'peer')")!=SQLITE_OK ) _exit(2);
    mpWrite(ready[1], "R");
    mpRead(release[0], &ch);
    if( strlen(queryScalarText(db, "SELECT dolt_commit('-am','peer')"))!=40 ) _exit(3);
    mpWrite(ready[1], "D");
    mpRead(done[0], &ch);
    if( execSql(db, "INSERT INTO t VALUES(11, 'peer'); COMMIT")!=SQLITE_OK ) _exit(4);
    if( strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE id=3"), "1")!=0 ) _exit(5);
    sqlite3_close(db);
    _exit(0);
  }
  if( pid<0 ){ perror("fork"); _exit(1); }
  close(ready[1]);
  close(release[0]);
  close(done[0]);
  mpRead(ready[0], &ch);
  memset(&ctx, 0, sizeof(ctx));
  ctx.releaseFd = release[1];
  ctx.readyFd = ready[0];
  sqlite3_busy_handler(db, mpReleaseBusyPeer, &ctx);
  rc = execSql(db, "SELECT dolt_commit('-am','mine')");
  check("mp_lost_race_commit_already_committed", rc==SQLITE_ERROR && ctx.calls>0
    && strstr(sqlite3_errmsg(db),"nothing to commit")!=0);
  if( ctx.calls==0 ) mpWrite(release[1], "G");
  sqlite3_busy_timeout(db, 10000);
  check("mp_lost_race_acked_write", execSql(db, "INSERT INTO t VALUES(3, 'mine')")==SQLITE_OK);
  mpWrite(done[1], "I");
  waitpid(pid, &status, 0);
  check("mp_lost_race_peer_sees_acked_write", WIFEXITED(status) && WEXITSTATUS(status)==0);
  check("mp_lost_race_own_view",
    strcmp(queryScalarText(db, "SELECT group_concat(id) FROM (SELECT id FROM t ORDER BY id)"),
           "1,2,3,10,11")==0);
  check("mp_lost_race_retry_commits",
    strlen(queryScalarText(db, "SELECT dolt_commit('-am','retry')"))==40);
  check("mp_lost_race_retry_contents",
    strcmp(queryScalarText(db, "SELECT group_concat(id) FROM (SELECT id FROM dolt_at_t('HEAD') ORDER BY id)"),
           "1,2,3,10,11")==0);
  sqlite3_close(db);
  close(ready[0]);
  close(release[1]);
  close(done[1]);
  remove(path);
}

/* Holds dolt_commit after it has hashed its working catalog and before it
** publishes that catalog. The command is one write transaction, so a peer
** insert in the gap is refused rather than computed around, and the same
** insert lands once the commit is done. */
static void test_commit_keeps_peer_insert_during_publish(void){
  char path[256];
  char pause[256];
  char ready[300];
  sqlite3 *db = 0;
  int status = 0;
  int i;
  pid_t pid;

  printf("--- Test 6e: a peer insert during dolt_commit waits for it and survives ---\n");
  snprintf(path, sizeof(path), DOLTLITE_TEST_TMPDIR "/mp_commit_gap_%d.db", (int)getpid());
  snprintf(pause, sizeof(pause), DOLTLITE_TEST_TMPDIR "/mp_commit_gap_%d.pause", (int)getpid());
  snprintf(ready, sizeof(ready), "%s.ready", pause);
  remove(path);
  remove(pause);
  remove(ready);
  setup_db(path);
  {
    /* SQLite xAccess treats a zero-length file as absent. */
    FILE *hold = fopen(pause, "w");
    if( !hold ){ perror("pause"); _exit(1); }
    if( fputc('x', hold)==EOF ){ perror("pause"); _exit(1); }
    fclose(hold);
  }
  pid = fork();
  if( pid==0 ){
    int rc;
    setenv("DOLTLITE_PAUSE_BEFORE_BRANCH_ADVANCE", pause, 1);
    db = 0;
    if( sqlite3_open(path, &db)!=SQLITE_OK ) _exit(1);
    sqlite3_busy_timeout(db, 10000);
    if( execSql(db, "INSERT INTO t VALUES(10, 'snap')")!=SQLITE_OK ) _exit(2);
    rc = execSql(db, "SELECT dolt_commit('-Am','snap')");
    sqlite3_close(db);
    if( rc==SQLITE_OK ) _exit(0);
    _exit(3);
  }
  if( pid<0 ){ perror("fork"); _exit(1); }
  for( i=0; i<500 && access(ready, F_OK)!=0; i++ ) sqlite3_sleep(20);
  check("mp_commit_gap_reached_pause", access(ready, F_OK)==0);
  check("mp_commit_gap_open", sqlite3_open(path, &db)==SQLITE_OK);
  sqlite3_busy_timeout(db, 200);
  check("mp_commit_gap_peer_insert_busy",
        execSql(db, "INSERT INTO t VALUES(11, 'peer')")==SQLITE_BUSY);
  remove(pause);
  waitpid(pid, &status, 0);
  check("mp_commit_gap_commit_ok",
        WIFEXITED(status) && WEXITSTATUS(status)==0);
  sqlite3_busy_timeout(db, 10000);
  check("mp_commit_gap_peer_insert",
        execSql(db, "INSERT INTO t VALUES(11, 'peer')")==SQLITE_OK);
  check("mp_commit_gap_peer_kept",
        strcmp(queryScalarText(db,
          "SELECT group_concat(id) FROM (SELECT id FROM t ORDER BY id)"),
          "1,10,11")==0);
  check("mp_commit_gap_committed",
        strcmp(queryScalarText(db,
          "SELECT group_concat(id) FROM (SELECT id FROM dolt_at_t('HEAD') ORDER BY id)"),
          "1,10")==0);
  check("mp_commit_gap_retry",
        strlen(queryScalarText(db, "SELECT dolt_commit('-Am','retry')"))==40);
  check("mp_commit_gap_head",
        strcmp(queryScalarText(db,
          "SELECT group_concat(id) FROM (SELECT id FROM dolt_at_t('HEAD') ORDER BY id)"),
          "1,10,11")==0);
  sqlite3_close(db);
  remove(path);
  remove(pause);
  remove(ready);
}

/* A peer that tries to commit while dolt_commit holds the lock is refused,
** then commits once it is free. Neither side's acknowledged writes, nor the
** ones the first connection makes afterwards, may be lost. */
static void test_commit_lost_race_keeps_later_writes(void){
  char path[256];
  char pause[256];
  char ready[300];
  sqlite3 *db = 0;
  int status = 0;
  int rc;
  pid_t pid;

  printf("--- Test 6f: a peer commit during dolt_commit waits and keeps every write ---\n");
  snprintf(path, sizeof(path), DOLTLITE_TEST_TMPDIR "/mp_commit_lost_%d.db", (int)getpid());
  snprintf(pause, sizeof(pause), DOLTLITE_TEST_TMPDIR "/mp_commit_lost_%d.pause", (int)getpid());
  snprintf(ready, sizeof(ready), "%s.ready", pause);
  remove(path);
  remove(pause);
  remove(ready);
  setup_db(path);
  {
    FILE *hold = fopen(pause, "w");
    if( !hold ){ perror("pause"); _exit(1); }
    if( fputc('x', hold)==EOF ){ perror("pause"); _exit(1); }
    fclose(hold);
  }
  check("mp_commit_lost_open", sqlite3_open(path, &db)==SQLITE_OK);
  sqlite3_busy_timeout(db, 200);
  check("mp_commit_lost_dirty", execSql(db, "INSERT INTO t VALUES(10, 'mine')")==SQLITE_OK);
  pid = fork();
  if( pid==0 ){
    sqlite3 *peer = 0;
    int i, ok;
    for( i=0; i<500 && access(ready, F_OK)!=0; i++ ) sqlite3_sleep(20);
    if( access(ready, F_OK)!=0 ){ remove(pause); _exit(1); }
    if( sqlite3_open(path, &peer)!=SQLITE_OK ){ remove(pause); _exit(2); }
    sqlite3_busy_timeout(peer, 200);
    ok = execSql(peer, "UPDATE t SET v='peer' WHERE id=1")==SQLITE_BUSY;
    remove(pause);
    sqlite3_busy_timeout(peer, 10000);
    if( ok ){
      ok = execSql(peer, "UPDATE t SET v='peer' WHERE id=1; "
                         "SELECT dolt_commit('-am','peer')")==SQLITE_OK
        && execSql(peer, "INSERT INTO t VALUES(11, 'peer')")==SQLITE_OK;
    }
    sqlite3_close(peer);
    _exit(ok ? 0 : 3);
  }
  if( pid<0 ){ perror("fork"); _exit(1); }
  setenv("DOLTLITE_PAUSE_BEFORE_BRANCH_ADVANCE", pause, 1);
  rc = execSql(db, "SELECT dolt_commit('-am','mine')");
  unsetenv("DOLTLITE_PAUSE_BEFORE_BRANCH_ADVANCE");
  waitpid(pid, &status, 0);
  check("mp_commit_lost_peer_acked", WIFEXITED(status) && WEXITSTATUS(status)==0);
  check("mp_commit_lost_commit_ok", rc==SQLITE_OK);
  check("mp_commit_lost_sees_peer",
        strcmp(queryScalarText(db,
          "SELECT group_concat(id||v) FROM (SELECT id, v FROM t ORDER BY id)"),
          "1peer,10mine,11peer")==0);
  check("mp_commit_lost_follow_up",
        execSql(db, "INSERT INTO t VALUES(12, 'after')")==SQLITE_OK);
  sqlite3_close(db);
  check("mp_commit_lost_reopen", sqlite3_open(path, &db)==SQLITE_OK);
  check("mp_commit_lost_rows_kept",
        strcmp(queryScalarText(db,
          "SELECT group_concat(id) FROM (SELECT id FROM t ORDER BY id)"),
          "1,10,11,12")==0);
  check("mp_commit_lost_history",
        strcmp(queryScalarText(db,
          "SELECT group_concat(message, '|') FROM (SELECT message FROM dolt_log LIMIT 2)"),
          "peer|mine")==0);
  sqlite3_close(db);
  remove(path);
  remove(pause);
  remove(ready);
}

static void test_commit_does_not_erase_peer_write(void){
  char path[256];
  sqlite3 *db = 0;
  int ready[2], release[2];
  int rc, status;
  pid_t pid;
  char ch;
  MpRefBusyCtx ctx;

  printf("--- Test 6c: dolt_commit does not erase a peer's working-set write ---\n");
  snprintf(path, sizeof(path), DOLTLITE_TEST_TMPDIR "/mp_ws_clobber_%d.db", (int)getpid());
  setup_db(path);
  check("mp_ws_clobber_open", sqlite3_open(path, &db)==SQLITE_OK);
  check("mp_ws_clobber_first_write", execSql(db, "INSERT INTO t VALUES(2, 'mine')")==SQLITE_OK);
  mpPipe(ready);
  mpPipe(release);
  pid = fork();
  if( pid==0 ){
    close(ready[0]);
    close(release[1]);
    db = 0;
    if( sqlite3_open(path, &db)!=SQLITE_OK ) _exit(1);
    sqlite3_busy_timeout(db, 10000);
    if( execSql(db, "BEGIN IMMEDIATE")!=SQLITE_OK ) _exit(2);
    mpWrite(ready[1], "R");
    mpRead(release[0], &ch);
    if( execSql(db, "INSERT INTO t VALUES(20, 'peer'); COMMIT")!=SQLITE_OK ) _exit(3);
    mpWrite(ready[1], "D");
    sqlite3_close(db);
    _exit(0);
  }
  if( pid<0 ){ perror("fork"); _exit(1); }
  close(ready[1]);
  close(release[0]);
  mpRead(ready[0], &ch);
  memset(&ctx, 0, sizeof(ctx));
  ctx.releaseFd = release[1];
  ctx.readyFd = ready[0];
  sqlite3_busy_handler(db, mpReleaseBusyPeer, &ctx);
  rc = execSql(db, "SELECT dolt_commit('-am','mine')");
  if( isRetryable(rc) ){
    check("mp_ws_clobber_busy_peer_write_kept",
      strcmp(queryScalarText(db,
        "SELECT group_concat(id) FROM (SELECT id FROM t ORDER BY id)"),
        "1,2,20")==0);
    rc = execSql(db, "SELECT dolt_commit('-am','mine')");
  }
  check("mp_ws_clobber_commit_waited", rc==SQLITE_OK && ctx.calls>0);
  if( ctx.calls==0 ) mpWrite(release[1], "G");
  waitpid(pid, &status, 0);
  check("mp_ws_clobber_peer_ok", WIFEXITED(status) && WEXITSTATUS(status)==0);
  sqlite3_busy_timeout(db, 10000);
  check("mp_ws_clobber_peer_write_kept",
    strcmp(queryScalarText(db, "SELECT group_concat(id) FROM (SELECT id FROM t ORDER BY id)"),
           "1,2,20")==0);
  check("mp_ws_clobber_retry_commits",
    strlen(queryScalarText(db, "SELECT dolt_commit('-am','retry','--allow-empty')"))==40);
  check("mp_ws_clobber_retry_contents",
    strcmp(queryScalarText(db, "SELECT group_concat(id) FROM (SELECT id FROM dolt_at_t('HEAD') ORDER BY id)"),
           "1,2,20")==0);
  sqlite3_close(db);
  close(ready[0]);
  close(release[1]);
  remove(path);
}

static void test_ref_command_binds_post_wait_tip(void){
  static const char *azSql[] = {
    "SELECT dolt_branch('x')",
    "SELECT dolt_tag('x')",
    "SELECT dolt_checkout('-b','x')"
  };
  static const char *azRef[] = { "x", "x", "x" };
  char path[256];
  int i;

  printf("--- Test 6d: A ref command that waited binds the post-wait tip ---\n");
  snprintf(path, sizeof(path), DOLTLITE_TEST_TMPDIR "/mp_ref_wait_%d.db", (int)getpid());
  for(i=0; i<(int)(sizeof(azSql)/sizeof(azSql[0])); i++){
    sqlite3 *db = 0;
    int ready[2], release[2];
    int rc, status;
    pid_t pid;
    char ch;
    char zRef[64], zMain[64], zHead[64];
    MpRefBusyCtx ctx;
    char *zQ;

    setup_db(path);
    check("mp_ref_wait_open", sqlite3_open(path, &db)==SQLITE_OK);
    check("mp_ref_wait_prime", strlen(queryScalarText(db, "SELECT dolt_hashof('HEAD')"))==40);
    mpPipe(ready);
    mpPipe(release);
    pid = fork();
    if( pid==0 ){
      close(ready[0]);
      close(release[1]);
      db = 0;
      if( sqlite3_open(path, &db)!=SQLITE_OK ) _exit(1);
      sqlite3_busy_timeout(db, 10000);
      if( execSql(db, "BEGIN IMMEDIATE; INSERT INTO t VALUES(10, 'peer')")!=SQLITE_OK ) _exit(2);
      mpWrite(ready[1], "R");
      mpRead(release[0], &ch);
      if( strlen(queryScalarText(db, "SELECT dolt_commit('-am','peer')"))!=40 ) _exit(3);
      mpWrite(ready[1], "D");
      sqlite3_close(db);
      _exit(0);
    }
    if( pid<0 ){ perror("fork"); _exit(1); }
    close(ready[1]);
    close(release[0]);
    mpRead(ready[0], &ch);
    memset(&ctx, 0, sizeof(ctx));
    ctx.releaseFd = release[1];
    ctx.readyFd = ready[0];
    sqlite3_busy_handler(db, mpReleaseBusyPeer, &ctx);
    rc = execSql(db, azSql[i]);
    if( rc!=SQLITE_OK ) fprintf(stderr, "%s: %s\n", azSql[i], sqlite3_errmsg(db));
    check("mp_ref_wait_command_ok", rc==SQLITE_OK && ctx.calls>0);
    if( ctx.calls==0 ) mpWrite(release[1], "G");
    waitpid(pid, &status, 0);
    check("mp_ref_wait_peer_ok", WIFEXITED(status) && WEXITSTATUS(status)==0);
    sqlite3_busy_timeout(db, 10000);
    zQ = sqlite3_mprintf("SELECT dolt_hashof('%s')", azRef[i]);
    snprintf(zRef, sizeof(zRef), "%s", queryScalarText(db, zQ));
    sqlite3_free(zQ);
    snprintf(zMain, sizeof(zMain), "%s", queryScalarText(db, "SELECT dolt_hashof('main')"));
    snprintf(zHead, sizeof(zHead), "%s", queryScalarText(db, "SELECT dolt_hashof('HEAD')"));
    check("mp_ref_wait_ref_at_post_wait_tip", strlen(zMain)==40 && strcmp(zRef, zMain)==0);
    check("mp_ref_wait_head_follows_branch", strcmp(zHead, zMain)==0);
    check("mp_ref_wait_sees_peer_row",
      strcmp(queryScalarText(db, "SELECT count(*) FROM t WHERE id=10"), "1")==0);
    sqlite3_close(db);
    close(ready[0]);
    close(release[1]);
  }
  remove(path);
}

static void test_ref_command_keeps_peer_write(void){
  static const char *azSql[] = {
    "SELECT dolt_branch('x')",
    "SELECT dolt_tag('x')",
    "SELECT dolt_remote('add','o','file:///nonexistent')",
    "SELECT dolt_gc()"
  };
  char path[256];
  int i;

  printf("--- Test 6e: A ref command does not let the next write erase a peer's ---\n");
  snprintf(path, sizeof(path), DOLTLITE_TEST_TMPDIR "/mp_ref_peer_ws_%d.db", (int)getpid());
  for(i=0; i<(int)(sizeof(azSql)/sizeof(azSql[0])); i++){
    sqlite3 *db = 0;
    int status;
    pid_t pid;

    setup_db(path);
    check("mp_ref_peer_ws_open", sqlite3_open(path, &db)==SQLITE_OK);
    sqlite3_busy_timeout(db, 10000);
    check("mp_ref_peer_ws_first_write",
          execSql(db, "INSERT INTO t VALUES(2, 'mine')")==SQLITE_OK);
    pid = fork();
    if( pid==0 ){
      db = 0;
      if( sqlite3_open(path, &db)!=SQLITE_OK ) _exit(1);
      sqlite3_busy_timeout(db, 10000);
      if( execSql(db, "INSERT INTO t VALUES(20, 'peer')")!=SQLITE_OK ) _exit(2);
      sqlite3_close(db);
      _exit(0);
    }
    if( pid<0 ){ perror("fork"); _exit(1); }
    waitpid(pid, &status, 0);
    check("mp_ref_peer_ws_peer_ok", WIFEXITED(status) && WEXITSTATUS(status)==0);
    check("mp_ref_peer_ws_command_ok", execSql(db, azSql[i])==SQLITE_OK);
    check("mp_ref_peer_ws_next_write",
          execSql(db, "INSERT INTO t VALUES(3, 'mine')")==SQLITE_OK);
    sqlite3_close(db);
    db = 0;
    sqlite3_open(path, &db);
    check("mp_ref_peer_ws_peer_write_kept",
      strcmp(queryScalarText(db, "SELECT group_concat(id) FROM (SELECT id FROM t ORDER BY id)"),
             "1,2,3,20")==0);
    sqlite3_close(db);
  }
  remove(path);
}

typedef struct MpMidOpPeer MpMidOpPeer;
struct MpMidOpPeer {
  sqlite3 *peer;
  int fireAt;
  int nCalls;
  int fired;
  int peerRc;
};

static int mpFirePeerMidOp(void *arg){
  MpMidOpPeer *p = (MpMidOpPeer*)arg;
  if( ++p->nCalls==p->fireAt && !p->fired ){
    p->fired = 1;
    p->peerRc = execSql(p->peer, "INSERT INTO t VALUES(20, 'peer')");
  }
  return 0;
}

/* The peer's autocommit lands inside the command, which is then refused
** busy; the session's next write must still keep the peer's row. */
static void test_busy_command_keeps_peer_write(void){
  static const char *azSql[] = {
    "SELECT dolt_commit('-am','mine')",
    "SELECT dolt_add('.')"
  };
  char path[256];
  int i;

  printf("--- Test 6f: A command refused busy mid-peer-write keeps the peer's row ---\n");
  snprintf(path, sizeof(path), DOLTLITE_TEST_TMPDIR "/mp_busy_peer_ws_%d.db", (int)getpid());
  for(i=0; i<(int)(sizeof(azSql)/sizeof(azSql[0])); i++){
    int k;
    int nLost = 0;
    for(k=1; ; k++){
      sqlite3 *db = 0;
      sqlite3 *check = 0;
      MpMidOpPeer ctx;

      setup_db(path);
      sqlite3_open(path, &db);
      memset(&ctx, 0, sizeof(ctx));
      sqlite3_open(path, &ctx.peer);
      ctx.fireAt = k;
      ctx.peerRc = -1;
      execSql(db, "INSERT INTO t VALUES(2, 'mine')");
      sqlite3_progress_handler(db, 1, mpFirePeerMidOp, &ctx);
      execSql(db, azSql[i]);
      sqlite3_progress_handler(db, 0, 0, 0);
      if( !ctx.fired ){
        sqlite3_close(ctx.peer);
        sqlite3_close(db);
        break;
      }
      execSql(db, "INSERT INTO t VALUES(3, 'mine')");
      sqlite3_open(path, &check);
      if( ctx.peerRc==SQLITE_OK
       && strcmp(queryScalarText(check,
            "SELECT count(*) FROM t WHERE id IN (2,3,20)"), "3")!=0 ){
        if( nLost++==0 ) fprintf(stderr, "%s: peer row lost at step %d\n", azSql[i], k);
      }
      sqlite3_close(check);
      sqlite3_close(ctx.peer);
      sqlite3_close(db);
    }
    check("mp_busy_peer_ws_swept", k>1);
    check("mp_busy_peer_ws_peer_write_kept", nLost==0);
  }
  remove(path);
}

typedef struct MpPeerDuringOp MpPeerDuringOp;
struct MpPeerDuringOp {
  sqlite3 *peer;
  int fireAt;
  int nCalls;
  int fired;
  int peerRc;
};

static int mpPeerWritesDuringOp(void *arg){
  MpPeerDuringOp *p = (MpPeerDuringOp*)arg;
  if( ++p->nCalls==p->fireAt && !p->fired ){
    p->fired = 1;
    p->peerRc = execSql(p->peer, "INSERT INTO t VALUES(20, 'peer')");
  }
  return 0;
}

/* A peer's autocommit on main lands at every step of an operation that
** replaces or rewrites main's working set; its row must survive. */
static void test_op_keeps_peer_write_on_main(void){
  static const struct {
    const char *zPre;
    const char *zOp;
  } aCase[] = {
    { "SELECT 1", "SELECT dolt_merge('f')" },
    { "SELECT 1", "SELECT dolt_merge('--no-ff','f')" },
    { "SELECT 1", "SELECT dolt_merge('--squash','f')" },
    { "SELECT 1", "SELECT dolt_merge('--no-ff','--no-commit','f')" },
    { "SELECT 1", "SELECT dolt_cherry_pick('f')" },
    { "SELECT dolt_merge('f')", "SELECT dolt_revert('HEAD')" },
    { "INSERT INTO t VALUES(2, 'mine')", "SELECT dolt_checkout('f')" },
    { "INSERT INTO t VALUES(2, 'mine')", "SELECT dolt_checkout('u')" },
    { "INSERT INTO t VALUES(2, 'mine')", "VACUUM" },
    { "SELECT dolt_remote('add','origin','file://" DOLTLITE_TEST_TMPDIR "/mp_op_peer_ws_remote.db');"
      "SELECT dolt_push('origin','f','--force')",
      "SELECT dolt_pull('origin','f')" }
  };
  char path[256];
  int i;

  printf("--- Test 6g: An operation on main keeps a peer's mid-operation write ---\n");
  snprintf(path, sizeof(path), DOLTLITE_TEST_TMPDIR "/mp_op_peer_ws_%d.db", (int)getpid());
  for(i=0; i<(int)(sizeof(aCase)/sizeof(aCase[0])); i++){
    int k;
    int nLost = 0;
    for(k=1; ; k++){
      sqlite3 *db = 0;
      sqlite3 *check = 0;
      MpPeerDuringOp ctx;

      setup_db(path);
      sqlite3_open(path, &db);
      execSql(db,
        "CREATE TABLE u(x); SELECT dolt_commit('-Am','u');"
        "SELECT dolt_branch('f'); SELECT dolt_checkout('f');"
        "INSERT INTO u VALUES(9); SELECT dolt_commit('-am','f1');"
        "SELECT dolt_checkout('main');");
      execSql(db, aCase[i].zPre);
      memset(&ctx, 0, sizeof(ctx));
      sqlite3_open(path, &ctx.peer);
      ctx.fireAt = k;
      ctx.peerRc = -1;
      sqlite3_progress_handler(db, 1, mpPeerWritesDuringOp, &ctx);
      execSql(db, aCase[i].zOp);
      sqlite3_progress_handler(db, 0, 0, 0);
      sqlite3_close(ctx.peer);
      sqlite3_close(db);
      if( !ctx.fired ) break;
      sqlite3_open(path, &check);
      if( ctx.peerRc==SQLITE_OK
       && strcmp(queryScalarText(check,
            "SELECT count(*) FROM main.t WHERE id=20"), "1")!=0 ){
        if( nLost++==0 ){
          fprintf(stderr, "%s: peer row lost at step %d\n", aCase[i].zOp, k);
        }
      }
      sqlite3_close(check);
    }
    check("mp_op_peer_ws_swept", k>1);
    check("mp_op_peer_ws_peer_write_kept", nLost==0);
  }
  remove(path);
  remove(DOLTLITE_TEST_TMPDIR "/mp_op_peer_ws_remote.db");
}

static void test_publication_busy_commit(void){
  const char *azAction[] = {"commit", "prepared", "release", "rollback"};
  char path[256];
  int i;

  printf("--- Publication BUSY preserves the transaction for retry ---\n");
  snprintf(path, sizeof(path), DOLTLITE_TEST_TMPDIR "/mp_publication_busy_%d.db",
           (int)getpid());
  for(i=0; i<4; i++){
    sqlite3 *db = 0, *peer = 0;
    sqlite3_stmt *commit = 0;
    int ready[2], release[2];
    int rc, status;
    pid_t pid;
    char ch, zName[96];
    const char *zFinish = i==2 ? "RELEASE outer" : "COMMIT";

    setup_db(path);
    mpPipe(ready);
    mpPipe(release);
    pid = fork();
    if( pid<0 ){ perror("fork"); _exit(1); }
    if( pid==0 ){
      sqlite3_vfs *vfs = sqlite3_vfs_find(0);
      sqlite3_file *file = malloc(vfs->szOsFile);
      char zOpen[260];
      int flags = 0;
      close(ready[0]);
      close(release[1]);
      if( !file ) _exit(1);
      memset(file, 0, vfs->szOsFile);
      /* xOpen expects the main-db name double-nul terminated. */
      memset(zOpen, 0, sizeof(zOpen));
      snprintf(zOpen, sizeof(zOpen)-2, "%s", path);
      if( vfs->xOpen(vfs, zOpen, file,
          SQLITE_OPEN_MAIN_DB|SQLITE_OPEN_READWRITE, &flags)!=SQLITE_OK ){
        _exit(2);
      }
      if( file->pMethods->xLock(file, SQLITE_LOCK_SHARED)!=SQLITE_OK ) _exit(3);
      mpWrite(ready[1], "r");
      mpRead(release[0], &ch);
      if( file->pMethods->xUnlock(file, SQLITE_LOCK_NONE)!=SQLITE_OK ) _exit(4);
      if( file->pMethods->xClose(file)!=SQLITE_OK ) _exit(5);
      free(file);
      _exit(0);
    }
    close(ready[1]);
    close(release[0]);
    mpRead(ready[0], &ch);
    check("mp_publication_busy_open", sqlite3_open(path, &db)==SQLITE_OK);
    sqlite3_busy_timeout(db, 10);
    check("mp_publication_busy_begin", execSql(db,
          i==2 ? "SAVEPOINT outer" : "BEGIN")==SQLITE_OK);
    check("mp_publication_busy_insert",
          execSql(db, "INSERT INTO t VALUES(2,'pending')")==SQLITE_OK);
    if( i==1 ){
      check("mp_publication_busy_prepare",
            sqlite3_prepare_v2(db, zFinish, -1, &commit, 0)==SQLITE_OK);
    }
    rc = commit ? sqlite3_step(commit) : execSql(db, zFinish);
    snprintf(zName, sizeof(zName), "mp_publication_busy_%s_refused", azAction[i]);
    check(zName, rc==SQLITE_BUSY);
    check("mp_publication_busy_transaction_open", !sqlite3_get_autocommit(db));
    snprintf(zName, sizeof(zName), "mp_publication_busy_%s_rows_kept", azAction[i]);
    check(zName, strcmp(queryScalarText(db,
          "SELECT group_concat(id) FROM (SELECT id FROM t ORDER BY id)"), "1,2")==0);
    check("mp_publication_busy_peer_open", sqlite3_open(path, &peer)==SQLITE_OK);
    check("mp_publication_busy_graph_lock_kept",
          execSql(peer, "BEGIN IMMEDIATE")==SQLITE_BUSY);
    execSql(peer, "ROLLBACK");
    check("mp_publication_busy_unpublished", strcmp(queryScalarText(peer,
          "SELECT group_concat(id) FROM (SELECT id FROM t ORDER BY id)"), "1")==0);
    sqlite3_close(peer);
    rc = commit ? sqlite3_step(commit) : execSql(db, zFinish);
    check("mp_publication_busy_repeated_refusal", rc==SQLITE_BUSY);
    if( i==3 ){
      check("mp_publication_busy_rollback", execSql(db, "ROLLBACK")==SQLITE_OK);
    }
    mpWrite(release[1], "g");
    close(ready[0]);
    close(release[1]);
    check("mp_publication_busy_child_wait", waitpid(pid, &status, 0)==pid);
    check("mp_publication_busy_child_exit", WIFEXITED(status) && WEXITSTATUS(status)==0);
    if( i!=3 ){
      rc = commit ? sqlite3_step(commit) : execSql(db, zFinish);
      check("mp_publication_busy_retry", rc==(commit ? SQLITE_DONE : SQLITE_OK));
    }
    sqlite3_finalize(commit);
    check("mp_publication_busy_transaction_ended", sqlite3_get_autocommit(db));
    sqlite3_close(db);
    check("mp_publication_busy_reopen", sqlite3_open(path, &db)==SQLITE_OK);
    snprintf(zName, sizeof(zName), "mp_publication_busy_%s_durable", azAction[i]);
    check(zName, strcmp(queryScalarText(db,
          "SELECT group_concat(id) FROM (SELECT id FROM t ORDER BY id)"),
          i==3 ? "1" : "1,2")==0);
    check("mp_publication_busy_integrity",
          strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);
    sqlite3_close(db);
  }
  remove(path);
}

typedef struct MpCleanResetPeer MpCleanResetPeer;
struct MpCleanResetPeer {
  int toPeer;
  int fromPeer;
  int fireAt;
  int nCalls;
  int fired;
  char result;
};

static int mpCleanResetProgress(void *arg){
  MpCleanResetPeer *p = (MpCleanResetPeer*)arg;
  if( ++p->nCalls==p->fireAt && !p->fired ){
    p->fired = 1;
    mpWrite(p->toPeer, "w");
    mpRead(p->fromPeer, &p->result);
  }
  return 0;
}

static void test_clean_reset_peer_process(void){
  static const struct {
    const char *zName;
    const char *zPre;
    const char *zSql;
  } aCase[] = {
    { "clean_noop", "", "SELECT dolt_clean()" },
    { "clean_tracked", "", "SELECT dolt_clean('t')" },
    { "clean_dry", "CREATE TABLE scratch(id INTEGER PRIMARY KEY)",
      "SELECT dolt_clean('--dry-run')" },
    { "clean_drop", "CREATE TABLE scratch(id INTEGER PRIMARY KEY)",
      "SELECT dolt_clean()" },
    { "reset_noop", "", "SELECT dolt_reset('t')" },
    { "reset_staged", "SELECT dolt_add('t')", "SELECT dolt_reset('t')" },
    { "status", "", "SELECT count(*) FROM dolt_status" }
  };
  char path[256];
  int i, action;

  printf("--- Clean and table reset preserve a peer process's writes ---\n");
  snprintf(path, sizeof(path), DOLTLITE_TEST_TMPDIR "/mp_clean_reset_%d.db",
           (int)getpid());
  for(i=0; i<(int)(sizeof(aCase)/sizeof(aCase[0])); i++){
    int nAcked = 0, nOk = 0, nLost = 0, nStale = 0, nErrors = 0;
    char zName[96];
    for(action=0; action<2; action++){
      int k;
      for(k=1; k<5000; k++){
        sqlite3 *db = 0, *fresh = 0;
        MpCleanResetPeer p;
        int toPeer[2], fromPeer[2];
        int rc, status;
        pid_t pid;
        char ch;

        setup_db(path);
        sqlite3_open(path, &db);
        check("mp_clean_reset_setup", execSql(db,
          "CREATE TABLE u(id INTEGER PRIMARY KEY);"
          "SELECT dolt_commit('-Am','u');"
          "INSERT INTO t VALUES(3,'dirty')")==SQLITE_OK);
        check("mp_clean_reset_pre", execSql(db, aCase[i].zPre)==SQLITE_OK);
        sqlite3_close(db);
        mpPipe(toPeer);
        mpPipe(fromPeer);
        pid = fork();
        if( pid<0 ){ perror("fork"); _exit(1); }
        if( pid==0 ){
          sqlite3 *peer = 0;
          int peerRc;
          close(toPeer[1]);
          close(fromPeer[0]);
          if( sqlite3_open(path, &peer)!=SQLITE_OK ) _exit(1);
          if( strcmp(queryScalarText(peer, "SELECT count(*) FROM u"), "0") ){
            _exit(2);
          }
          mpWrite(fromPeer[1], "r");
          mpRead(toPeer[0], &ch);
          if( ch=='w' ){
            peerRc = execSql(peer, "INSERT INTO u VALUES(20)");
            if( peerRc==SQLITE_OK && action ){
              peerRc = execSql(peer, "SELECT dolt_commit('-am','peer')");
            }
            mpWrite(fromPeer[1], peerRc==SQLITE_OK ? "o"
                       : isRetryable(peerRc) ? "b" : "e");
          }
          sqlite3_close(peer);
          _exit(0);
        }
        close(toPeer[0]);
        close(fromPeer[1]);
        mpRead(fromPeer[0], &ch);
        memset(&p, 0, sizeof(p));
        p.toPeer = toPeer[1];
        p.fromPeer = fromPeer[0];
        p.fireAt = k;
        sqlite3_open(path, &db);
        queryScalarText(db, "SELECT count(*) FROM u");
        sqlite3_progress_handler(db, 1, mpCleanResetProgress, &p);
        rc = execSql(db, aCase[i].zSql);
        sqlite3_progress_handler(db, 0, 0, 0);
        if( !p.fired ) mpWrite(toPeer[1], "q");
        close(toPeer[1]);
        close(fromPeer[0]);
        waitpid(pid, &status, 0);
        if( !WIFEXITED(status) || WEXITSTATUS(status)!=0 ) nErrors++;
        if( rc!=SQLITE_OK && !isRetryable(rc) ) nErrors++;
        if( p.result=='e' ) nErrors++;
        if( p.result=='o' ){
          nAcked++;
          sqlite3_open(path, &fresh);
          if( strcmp(queryScalarText(fresh,
                "SELECT count(*) FROM u WHERE id=20"), "1") ) nLost++;
          if( strcmp(queryScalarText(db,
                "SELECT count(*) FROM u WHERE id=20"), "1") ) nStale++;
          if( isRetryable(rc) ) rc = execSql(db, aCase[i].zSql);
          if( rc==SQLITE_OK ) nOk++;
          if( execSql(db, "SELECT dolt_clean();"
                          "INSERT INTO t VALUES(4,'later')")!=SQLITE_OK ){
            nErrors++;
          }
          if( strcmp(queryScalarText(fresh,
                "SELECT count(*) FROM u WHERE id=20"), "1") ) nLost++;
          if( strcmp(queryScalarText(fresh, "PRAGMA integrity_check"), "ok") ){
            nErrors++;
          }
          sqlite3_close(fresh);
        }
        sqlite3_close(db);
        if( !p.fired ) break;
      }
      if( k==5000 ) nErrors++;
    }
    printf("  %s: acknowledged=%d succeeded=%d lost=%d stale=%d errors=%d\n",
           aCase[i].zName, nAcked, nOk, nLost, nStale, nErrors);
    snprintf(zName, sizeof(zName), "mp_clean_reset_%s_exercised", aCase[i].zName);
    check(zName, nAcked>0 && nOk>0);
    snprintf(zName, sizeof(zName), "mp_clean_reset_%s_peer_kept", aCase[i].zName);
    check(zName, nLost==0);
    snprintf(zName, sizeof(zName), "mp_clean_reset_%s_reads_fresh", aCase[i].zName);
    check(zName, nStale==0);
    snprintf(zName, sizeof(zName), "mp_clean_reset_%s_errors", aCase[i].zName);
    check(zName, nErrors==0);
  }
  remove(path);
}

/* A process holding the PENDING byte, as a publishing writer does, blocks
** the read lock a statement refreshes under. The reader must wait or report
** busy, not answer from the snapshot it had before a peer's acknowledged
** insert. */
static int nPendingBusy = 0;
static int pendingToHolder[2], pendingFromHolder[2];

static void pendingHolderCmd(char c){
  char r;
  if( write(pendingToHolder[1], &c, 1)!=1
   || read(pendingFromHolder[0], &r, 1)!=1 ){
    perror("pending holder");
    exit(1);
  }
}

static int releasePendingOnBusy(void *pArg, int n){
  (void)pArg;
  if( nPendingBusy++==0 ) pendingHolderCmd('u');
  return n<200;
}

static int countRows(sqlite3 *db, int *pRc){
  sqlite3_stmt *pStmt = 0;
  int n = -1;
  *pRc = sqlite3_prepare_v2(db, "SELECT count(*) FROM t", -1, &pStmt, 0);
  if( *pRc==SQLITE_OK ){
    *pRc = sqlite3_step(pStmt);
    if( *pRc==SQLITE_ROW ){
      n = sqlite3_column_int(pStmt, 0);
      *pRc = SQLITE_OK;
    }
  }
  sqlite3_finalize(pStmt);
  return n;
}

static void test_reader_waits_for_publishing_writer(void){
  char path[256];
  sqlite3 *db = 0, *reader = 0, *writer = 0;
  pid_t holder;
  int rc, n;

  printf("--- Test: reader does not answer stale while a writer publishes ---\n");
  snprintf(path, sizeof(path), DOLTLITE_TEST_TMPDIR "/mp_pending_read_%d.db",
           (int)getpid());
  remove(path);
  sqlite3_open(path, &db);
  execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY);"
               "SELECT dolt_commit('-Am','init')");
  sqlite3_close(db);

  if( pipe(pendingToHolder) || pipe(pendingFromHolder) ){
    perror("pipe");
    exit(1);
  }
  holder = fork();
  if( holder==0 ){
    struct flock fl;
    char ch;
    int fd = open(path, O_RDWR);
    close(pendingToHolder[1]);
    close(pendingFromHolder[0]);
    memset(&fl, 0, sizeof(fl));
    fl.l_whence = SEEK_SET;
    fl.l_start = 0x40000000;
    fl.l_len = 1;
    while( read(pendingToHolder[0], &ch, 1)==1 ){
      fl.l_type = ch=='l' ? F_WRLCK : F_UNLCK;
      if( fcntl(fd, F_SETLK, &fl)!=0 ) _exit(1);
      if( write(pendingFromHolder[1], &ch, 1)!=1 ) _exit(1);
    }
    _exit(0);
  }

  sqlite3_open(path, &reader);
  check("pending_read_initial", countRows(reader, &rc)==0 && rc==SQLITE_OK);
  sqlite3_open(path, &writer);
  check("pending_read_peer_insert",
        execSql(writer, "INSERT INTO t VALUES(42)")==SQLITE_OK);
  sqlite3_close(writer);

  pendingHolderCmd('l');
  n = countRows(reader, &rc);
  check("pending_read_no_stale_without_handler",
        (rc==SQLITE_OK && n==1) || rc==SQLITE_BUSY);
  nPendingBusy = 0;
  sqlite3_busy_handler(reader, releasePendingOnBusy, 0);
  n = countRows(reader, &rc);
  if( nPendingBusy==0 ) pendingHolderCmd('u');
  check("pending_read_waits_then_sees_insert", rc==SQLITE_OK && n==1);
  sqlite3_close(reader);

  {
    char featPath[320];
    sqlite3_stmt *pStmt = 0;
    snprintf(featPath, sizeof(featPath), "%s/feat", path);
    sqlite3_open(path, &db);
    execSql(db, "SELECT dolt_checkout('-b','feat');"
                "INSERT INTO t VALUES(2);"
                "SELECT dolt_commit('-Am','f1');"
                "SELECT dolt_checkout('main');"
                "INSERT INTO t VALUES(3);"
                "SELECT dolt_commit('-Am','m1')");
    sqlite3_close(db);
    sqlite3_open(featPath, &reader);
    check("pending_rebase_start",
          execSql(reader, "SELECT dolt_rebase('-i','main')")==SQLITE_OK);
    sqlite3_open(path, &writer);
    execSql(writer, "INSERT INTO t VALUES(43)");
    sqlite3_close(writer);
    pendingHolderCmd('l');
    rc = sqlite3_prepare_v2(reader, "SELECT dolt_rebase('--continue')", -1,
                            &pStmt, 0);
    if( rc==SQLITE_OK ){
      rc = sqlite3_step(pStmt);
      if( rc==SQLITE_ROW && sqlite3_column_type(pStmt, 0)==SQLITE_NULL ){
        rc = SQLITE_MISUSE;
      }
    }
    sqlite3_finalize(pStmt);
    pendingHolderCmd('u');
    check("pending_rebase_continue_reports_outcome", rc!=SQLITE_MISUSE);
    sqlite3_close(reader);
  }

  close(pendingToHolder[1]);
  waitpid(holder, 0, 0);
  close(pendingToHolder[0]);
  close(pendingFromHolder[0]);
  close(pendingFromHolder[1]);
  remove(path);
}

/* A process holding SHARED on the file, as a reader does while it opens,
** keeps a publish from taking EXCLUSIVE. Ref commands must wait for it in
** the busy handler like INSERT and dolt_commit do. */
static void test_ref_commands_wait_for_reader_lock(void){
  static const char *azSql[] = {
    "SELECT dolt_branch('b2')",
    "SELECT dolt_tag('v1')",
    "SELECT dolt_checkout('-b','b3')",
    "SELECT dolt_checkout('feat')",
    "SELECT dolt_remote('remove','origin')",
    "SELECT dolt_reset('--soft','HEAD')",
    "INSERT INTO t VALUES(3,'x')",
  };
  char path[256];
  int i;

  printf("--- Test: ref commands wait out a reader's file lock ---\n");
  snprintf(path, sizeof(path), DOLTLITE_TEST_TMPDIR "/mp_ref_reader_lock_%d.db",
           (int)getpid());
  for(i=0; i<(int)(sizeof(azSql)/sizeof(azSql[0])); i++){
    sqlite3 *db = 0;
    int ready[2];
    char ch, zName[96];
    pid_t pid;
    int rc;
    remove(path);
    sqlite3_open(path, &db);
    execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
                "INSERT INTO t VALUES(1,'a');"
                "SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat');"
                "SELECT dolt_remote('add','origin','file:///nonexistent-mp');"
                "INSERT INTO t VALUES(2,'dirty')");
    sqlite3_close(db);
    db = 0;
    mpPipe(ready);
    pid = fork();
    if( pid==0 ){
      sqlite3_vfs *pVfs = sqlite3_vfs_find(0);
      sqlite3_file *pFile = (sqlite3_file*)malloc(pVfs->szOsFile);
      char zOpen[260];
      int outFlags = 0;
      close(ready[0]);
      /* xOpen expects the main-db name double-nul terminated. */
      memset(zOpen, 0, sizeof(zOpen));
      snprintf(zOpen, sizeof(zOpen)-2, "%s", path);
      if( !pFile || pVfs->xOpen(pVfs, zOpen, pFile,
            SQLITE_OPEN_MAIN_DB|SQLITE_OPEN_READWRITE, &outFlags) ) _exit(1);
      if( pFile->pMethods->xLock(pFile, SQLITE_LOCK_SHARED) ) _exit(2);
      mpWrite(ready[1], "L");
      sqlite3_sleep(1000);
      pFile->pMethods->xUnlock(pFile, SQLITE_LOCK_NONE);
      pFile->pMethods->xClose(pFile);
      _exit(0);
    }
    if( pid<0 ){ perror("fork"); _exit(1); }
    close(ready[1]);
    mpRead(ready[0], &ch);
    sqlite3_open(path, &db);
    sqlite3_busy_timeout(db, 10000);
    rc = execSql(db, azSql[i]);
    if( rc!=SQLITE_OK ) fprintf(stderr, "  %s: %s\n", azSql[i], sqlite3_errmsg(db));
    snprintf(zName, sizeof(zName), "mp_ref_reader_lock_waits_%d", i);
    check(zName, rc==SQLITE_OK);
    waitpid(pid, 0, 0);
    close(ready[0]);
    sqlite3_close(db);
  }
  remove(path);
}

/* A shared holder forces publication to wait. The wait must not keep the
** pending lock, so another process can read the pre-commit snapshot. */
static void test_reader_during_publication_wait(void){
  char path[256];
  char pause[256];
  char readyPath[300];
  sqlite3 *db = 0;
  int holdReady[2], holdRelease[2];
  int status = 0;
  int i;
  pid_t holder, publisher;
  char ch;

  printf("--- Test: reader proceeds while publication waits ---\n");
  snprintf(path, sizeof(path),
           DOLTLITE_TEST_TMPDIR "/mp_pub_wait_%d.db", (int)getpid());
  snprintf(pause, sizeof(pause),
           DOLTLITE_TEST_TMPDIR "/mp_pub_wait_%d.pause", (int)getpid());
  snprintf(readyPath, sizeof(readyPath), "%s.ready", pause);
  remove(path);
  remove(pause);
  remove(readyPath);
  setup_db(path);
  {
    FILE *hold = fopen(pause, "w");
    if( !hold ){ perror("pause"); _exit(1); }
    if( fputc('x', hold)==EOF ){ perror("pause"); _exit(1); }
    fclose(hold);
  }

  mpPipe(holdReady);
  mpPipe(holdRelease);
  holder = fork();
  if( holder<0 ){ perror("fork"); _exit(1); }
  if( holder==0 ){
    sqlite3_vfs *vfs = sqlite3_vfs_find(0);
    sqlite3_file *file = malloc(vfs->szOsFile);
    char zOpen[260];
    int flags = 0;
    close(holdReady[0]);
    close(holdRelease[1]);
    if( !file ) _exit(1);
    memset(file, 0, vfs->szOsFile);
    /* xOpen expects the main-db name double-nul terminated. */
    memset(zOpen, 0, sizeof(zOpen));
    snprintf(zOpen, sizeof(zOpen)-2, "%s", path);
    if( vfs->xOpen(vfs, zOpen, file,
        SQLITE_OPEN_MAIN_DB|SQLITE_OPEN_READWRITE, &flags)!=SQLITE_OK ){
      _exit(2);
    }
    if( file->pMethods->xLock(file, SQLITE_LOCK_SHARED)!=SQLITE_OK ) _exit(3);
    mpWrite(holdReady[1], "r");
    mpRead(holdRelease[0], &ch);
    if( file->pMethods->xUnlock(file, SQLITE_LOCK_NONE)!=SQLITE_OK ) _exit(4);
    if( file->pMethods->xClose(file)!=SQLITE_OK ) _exit(5);
    free(file);
    _exit(0);
  }
  close(holdReady[1]);
  close(holdRelease[0]);
  mpRead(holdReady[0], &ch);

  publisher = fork();
  if( publisher<0 ){ perror("fork"); _exit(1); }
  if( publisher==0 ){
    int rc;
    close(holdReady[0]);
    close(holdRelease[1]);
    setenv("DOLTLITE_PAUSE_DURING_PUBLISH", pause, 1);
    if( sqlite3_open(path, &db)!=SQLITE_OK ) _exit(1);
    sqlite3_busy_timeout(db, 15000);
    rc = execSql(db, "INSERT INTO t VALUES(2, 'pending')");
    sqlite3_close(db);
    if( rc==SQLITE_OK ) _exit(0);
    if( rc==SQLITE_BUSY ) _exit(10);
    _exit(3);
  }

  for( i=0; i<500 && access(readyPath, F_OK)!=0; i++ ) sqlite3_sleep(20);
  check("mp_pub_wait_reached_pause", access(readyPath, F_OK)==0);
  check("mp_pub_wait_open", sqlite3_open(path, &db)==SQLITE_OK);
  sqlite3_busy_timeout(db, 1000);
  check("mp_pub_wait_sees_precommit",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "1")==0);
  sqlite3_close(db);

  mpWrite(holdRelease[1], "g");
  check("mp_pub_wait_holder_exit", waitpid(holder, &status, 0)==holder);
  check("mp_pub_wait_holder_ok",
        WIFEXITED(status) && WEXITSTATUS(status)==0);
  remove(pause);
  check("mp_pub_wait_publisher_exit", waitpid(publisher, &status, 0)==publisher);
  check("mp_pub_wait_publisher_ok",
        WIFEXITED(status) && WEXITSTATUS(status)==0);
  check("mp_pub_wait_reopen", sqlite3_open(path, &db)==SQLITE_OK);
  check("mp_pub_wait_sees_commit",
        strcmp(queryScalarText(db, "SELECT count(*) FROM t"), "2")==0);
  sqlite3_close(db);
  close(holdReady[0]);
  close(holdRelease[1]);
  remove(path);
  remove(pause);
  remove(readyPath);
}

static int schemaRaceSend = -1;
static int schemaRaceRecv = -1;
static int schemaRaceFired = 0;

static int schemaRaceFault(int iTest){
  if( iTest==964 && !schemaRaceFired ){
    char ch;
    schemaRaceFired = 1;
    mpWrite(schemaRaceSend, "a");
    mpRead(schemaRaceRecv, &ch);
  }
  return 0;
}

static void test_schema_load_snapshot(void){
  int withoutRowid, restore;
  for(withoutRowid=0; withoutRowid<2; withoutRowid++){
    for(restore=0; restore<3; restore++){
      const char *path = DOLTLITE_TEST_TMPDIR "/multi_process_schema_snapshot.db";
      const char *branchPath = DOLTLITE_TEST_TMPDIR "/multi_process_schema_snapshot.db@b1";
      const char *azRestore[] = {
        "SELECT dolt_revert('HEAD')",
        "SELECT dolt_reset('--hard','HEAD~1')",
        "SELECT dolt_checkout('HEAD~1','t')"
      };
      sqlite3 *db = 0;
      sqlite3_stmt *stmt = 0;
      int toPeer[2], fromPeer[2], status, rc;
      char ch;
      pid_t pid;

      remove(path);
      check("mp_schema_snapshot_setup_open", sqlite3_open(path, &db)==SQLITE_OK);
      check("mp_schema_snapshot_create", execSql(db, withoutRowid
          ? "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT) WITHOUT ROWID"
          : "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")==SQLITE_OK);
      check("mp_schema_snapshot_seed", execSql(db,
          "INSERT INTO t VALUES(1,'base');"
          "SELECT dolt_commit('-Am','base');"
          "SELECT dolt_branch('b1')")==SQLITE_OK);
      sqlite3_close(db);
      db = 0;
      mpPipe(toPeer);
      mpPipe(fromPeer);
      pid = fork();
      if( pid<0 ){ perror("fork"); _exit(1); }
      if( pid==0 ){
        alarm(30);
        close(toPeer[1]);
        close(fromPeer[0]);
        if( sqlite3_open(branchPath, &db)!=SQLITE_OK ) _exit(2);
        sqlite3_busy_timeout(db, 5000);
        mpRead(toPeer[0], &ch);
        if( execSql(db,
            "ALTER TABLE t ADD COLUMN old_column TEXT DEFAULT 'obsolete';"
            "SELECT dolt_commit('-Am','add column')")!=SQLITE_OK ) _exit(3);
        mpWrite(fromPeer[1], "a");
        mpRead(toPeer[0], &ch);
        if( execSql(db, azRestore[restore])!=SQLITE_OK ) _exit(4);
        mpWrite(fromPeer[1], "r");
        sqlite3_close(db);
        _exit(0);
      }
      close(toPeer[0]);
      close(fromPeer[1]);
      schemaRaceSend = toPeer[1];
      schemaRaceRecv = fromPeer[0];
      schemaRaceFired = 0;
      sqlite3_test_control(SQLITE_TESTCTRL_FAULT_INSTALL, schemaRaceFault);
      check("mp_schema_snapshot_reader_open",
            sqlite3_open(branchPath, &db)==SQLITE_OK);
      rc = sqlite3_prepare_v2(db, "SELECT * FROM t", -1, &stmt, 0);
      sqlite3_test_control(SQLITE_TESTCTRL_FAULT_INSTALL, 0);
      check("mp_schema_snapshot_prepare", rc==SQLITE_OK);
      check("mp_schema_snapshot_peer_ran", schemaRaceFired);
      check("mp_schema_snapshot_cookie_matches_columns",
            sqlite3_column_count(stmt)==2);
      sqlite3_finalize(stmt);
      if( !schemaRaceFired ) mpWrite(toPeer[1], "a");
      mpWrite(toPeer[1], "r");
      mpRead(fromPeer[0], &ch);
      check("mp_schema_snapshot_peer_exit", waitpid(pid, &status, 0)==pid);
      check("mp_schema_snapshot_peer_ok", WIFEXITED(status) && WEXITSTATUS(status)==0);
      close(toPeer[1]);
      close(fromPeer[0]);
      check("mp_schema_snapshot_insert", execSql(db,
          "INSERT INTO t(id,v) VALUES(2,'after restore')")==SQLITE_OK);
      check("mp_schema_snapshot_integrity",
            strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);
      check("mp_schema_snapshot_commit", execSql(db,
          "SELECT dolt_commit('-Am','after restore')")==SQLITE_OK);
      sqlite3_close(db);
      db = 0;
      check("mp_schema_snapshot_reopen", sqlite3_open(branchPath, &db)==SQLITE_OK);
      check("mp_schema_snapshot_durable_integrity",
            strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);
      check("mp_schema_snapshot_durable_rows", strcmp(queryScalarText(db,
          "SELECT group_concat(id||':'||v, ',') FROM (SELECT * FROM t ORDER BY id)"),
          "1:base,2:after restore")==0);
      check("mp_schema_snapshot_new_column", execSql(db,
          "ALTER TABLE t ADD COLUMN fresh TEXT DEFAULT 'fresh'")==SQLITE_OK);
      check("mp_schema_snapshot_new_default", strcmp(queryScalarText(db,
          "SELECT count(*) FROM t WHERE fresh='fresh'"), "2")==0);
      sqlite3_close(db);
      remove(path);
    }
  }
}

int main(){
  printf("=== Multi-Process Concurrency Tests ===\n\n");

  test_schema_load_snapshot();
  test_publication_busy_commit();
  test_clean_reset_peer_process();
  test_ref_commands_busy_handler();
  test_ref_commands_wait_for_reader_lock();
  test_two_writers();
  test_reader_during_write();
  test_reader_after_peer_restore();
  test_reader_close_during_write_upgrade();
  test_add_during_transaction();
  test_sequential_processes();
  test_gc_during_read();
  test_gc_blocked_by_writer();
  test_cross_process_commit_conflict();
  test_stale_commit_variants();
  test_stale_commit_stage_race();
  test_write_after_lost_commit_race();
  test_commit_does_not_erase_peer_write();
  test_commit_keeps_peer_insert_during_publish();
  test_commit_lost_race_keeps_later_writes();
  test_op_keeps_peer_write_on_main();
  test_busy_command_keeps_peer_write();
  test_ref_command_binds_post_wait_tip();
  test_ref_command_keeps_peer_write();
  test_cross_process_commit_after_peer();
  test_many_process_commit_contention();
  test_reader_waits_for_publishing_writer();
  test_reader_during_publication_wait();

  printf("\n=== Results: %d passed, %d failed out of %d tests ===\n",
    nPass, nFail, nPass+nFail);
  return nFail > 0 ? 1 : 0;
}
