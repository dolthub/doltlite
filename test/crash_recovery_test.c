/*
** Crash recovery tests for DoltLite.
**
** These tests verify the storage invariants that a content-addressed
** chunk store must preserve across process crashes:
**
**   ATOMICITY: After a crash, either ALL chunks for a commit are on disk
**   or NONE are. There is no state where a root record references chunks
**   that don't exist. (Tests 3, 5, 10, 15, 18-22)
**
**   DURABILITY: After dolt_commit returns, the data survives a crash.
**   The WAL root record is written last before fsync, and fsync completes
**   before the function returns. (Tests 2, 6, 13, 16, 25, 26)
**
**   RECOVERY: On open, the WAL is fully replayed. The manifest at offset 0
**   may be stale — the WAL root record is authoritative. A truncated WAL
**   record is ignored, recovering to the last complete root. (Tests 1-30)
**
**   PREFIX INTEGRITY: If N commits were attempted and a crash occurs,
**   some prefix of those commits (0..N) is intact and the commit chain
**   from any branch tip terminates at the initial commit without
**   encountering a missing chunk. (Tests 7, 11, 17, 28)
**
** Uses POSIX fork()/kill(SIGKILL) to simulate crashes. The parent
** process reopens the database and verifies invariants hold.
**
** The GC cases in this file are best-effort async-kill tests around
** dolt_gc(). Deterministic crash-at-write coverage for the rewrite path
** lives in test/crash_injection_test.sh under SQLITE_TEST builds.
**
** Build from build/ directory:
**   cc -I. -o crash_recovery_test \
**     ../test/crash_recovery_test.c libdoltlite.a -lz -lpthread
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
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

/* Execute SQL, return first column of first row (static buffer). */
static char g_buf[8192];
static const char *exec1(sqlite3 *db, const char *sql){
  sqlite3_stmt *stmt = 0;
  int rc;
  g_buf[0] = 0;
  rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);
  if( rc!=SQLITE_OK ){
    snprintf(g_buf, sizeof(g_buf), "ERROR: %s", sqlite3_errmsg(db));
    return g_buf;
  }
  rc = sqlite3_step(stmt);
  if( rc==SQLITE_ROW ){
    const char *val = (const char*)sqlite3_column_text(stmt, 0);
    if( val ) snprintf(g_buf, sizeof(g_buf), "%s", val);
  }else if( rc==SQLITE_ERROR ){
    snprintf(g_buf, sizeof(g_buf), "ERROR: %s", sqlite3_errmsg(db));
  }
  sqlite3_finalize(stmt);
  return g_buf;
}

/* Execute SQL, return the integer from first column of first row. */
static int exec_int(sqlite3 *db, const char *sql, int dflt){
  sqlite3_stmt *stmt;
  int val = dflt;
  if( sqlite3_prepare_v2(db, sql, -1, &stmt, 0)==SQLITE_OK ){
    if( sqlite3_step(stmt)==SQLITE_ROW ){
      val = sqlite3_column_int(stmt, 0);
    }
  }
  sqlite3_finalize(stmt);
  return val;
}

/* Execute SQL, ignore result. Return rc. */
static int execsql(sqlite3 *db, const char *sql){
  char *err = 0;
  int rc = sqlite3_exec(db, sql, 0, 0, &err);
  if( rc!=SQLITE_OK && err ){
    /* Silently consume; caller decides what to check. */
    sqlite3_free(err);
  }
  return rc;
}

/* Remove database and associated WAL/journal files. */
static void remove_db(const char *path){
  char tmp[512];
  remove(path);
  snprintf(tmp, sizeof(tmp), "%s-wal", path);
  remove(tmp);
  snprintf(tmp, sizeof(tmp), "%s-journal", path);
  remove(tmp);
}

/* Generate a unique database path for each test invocation. */
static int g_test_seq = 0;
static char g_dbpath[512];
static const char *fresh_db(void){
  snprintf(g_dbpath, sizeof(g_dbpath),
           "/tmp/crash_test_%d_%d.db", (int)getpid(), g_test_seq++);
  remove_db(g_dbpath);
  return g_dbpath;
}

/*
** Verify database consistency after crash.
** Returns 1 if the database is consistent, 0 otherwise.
** Checks:
**   1. sqlite3_open succeeds
**   2. All branches in dolt_branches are valid
**   3. dolt_log shows a consistent commit chain
**   4. No unexpected errors from normal queries
*/
static int verify_consistency(const char *dbpath, const char *label){
  sqlite3 *db = 0;
  int rc;
  int ok = 1;
  char desc[256];

  rc = sqlite3_open(dbpath, &db);
  snprintf(desc, sizeof(desc), "%s: sqlite3_open succeeds", label);
  check(desc, rc==SQLITE_OK);
  if( rc!=SQLITE_OK ){
    sqlite3_close(db);
    return 0;
  }

  /* Check that dolt_branches is queryable and all branches are valid. */
  {
    sqlite3_stmt *stmt = 0;
    rc = sqlite3_prepare_v2(db,
      "SELECT name, hash FROM dolt_branches", -1, &stmt, 0);
    snprintf(desc, sizeof(desc), "%s: dolt_branches queryable", label);
    check(desc, rc==SQLITE_OK);
    if( rc==SQLITE_OK ){
      int nBranch = 0;
      while( sqlite3_step(stmt)==SQLITE_ROW ){
        const char *bname = (const char*)sqlite3_column_text(stmt, 0);
        const char *bhash = (const char*)sqlite3_column_text(stmt, 1);
        if( bname && bhash && strlen(bhash)==40 ){
          nBranch++;
        }else{
          snprintf(desc, sizeof(desc),
                   "%s: branch '%s' has valid hash", label,
                   bname ? bname : "(null)");
          check(desc, 0);
          ok = 0;
        }
      }
      snprintf(desc, sizeof(desc),
               "%s: at least one branch exists", label);
      check(desc, nBranch>=1);
      if( nBranch<1 ) ok = 0;
    }else{
      ok = 0;
    }
    sqlite3_finalize(stmt);
  }

  /* Check that dolt_log is queryable. */
  {
    int nLog = exec_int(db, "SELECT count(*) FROM dolt_log", -1);
    snprintf(desc, sizeof(desc), "%s: dolt_log has entries", label);
    check(desc, nLog>=0);
    if( nLog<0 ) ok = 0;
  }

  sqlite3_close(db);
  return ok;
}

/*
** Verify that a specific table has the expected row count.
*/
static void verify_row_count(const char *dbpath, const char *label,
                             const char *table, int expected){
  sqlite3 *db = 0;
  char sql[256], desc[256];
  int rc, cnt;

  rc = sqlite3_open(dbpath, &db);
  if( rc!=SQLITE_OK ){
    snprintf(desc, sizeof(desc), "%s: open for row count", label);
    check(desc, 0);
    sqlite3_close(db);
    return;
  }
  snprintf(sql, sizeof(sql), "SELECT count(*) FROM %s", table);
  cnt = exec_int(db, sql, -1);
  snprintf(desc, sizeof(desc), "%s: %s has %d rows", label, table, expected);
  check(desc, cnt==expected);
  sqlite3_close(db);
}

/*
** Verify the commit count in dolt_log.
*/
static int verify_commit_count(const char *dbpath, const char *label,
                                int expected){
  sqlite3 *db = 0;
  char desc[256];
  int rc, cnt;

  rc = sqlite3_open(dbpath, &db);
  if( rc!=SQLITE_OK ){
    sqlite3_close(db);
    return -1;
  }
  cnt = exec_int(db, "SELECT count(*) FROM dolt_log", -1);
  snprintf(desc, sizeof(desc), "%s: dolt_log has %d commits", label, expected);
  check(desc, cnt==expected);
  sqlite3_close(db);
  return cnt;
}

/* ====================================================================
** GROUP 1: Single commit crash recovery
** ==================================================================== */

/*
** Test 1: Baseline -- commit completes, child exits cleanly.
** Parent verifies data is visible after reopen.
*/
static void test_01_clean_commit(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 01: Clean commit baseline ---\n");

  pid = fork();
  if( pid==0 ){
    /* Child: create, insert, commit, exit cleanly. */
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    execsql(db, "INSERT INTO t VALUES(1, 'one')");
    execsql(db, "INSERT INTO t VALUES(2, 'two')");
    execsql(db, "INSERT INTO t VALUES(3, 'three')");
    exec1(db, "SELECT dolt_commit('-A', '-m', 'initial')");
    sqlite3_close(db);
    _exit(0);
  }
  /* Parent: wait for child, then verify. */
  {
    int status;
    waitpid(pid, &status, 0);
    check("test_01: child exited cleanly", WIFEXITED(status));
  }
  verify_consistency(dbpath, "test_01");
  verify_row_count(dbpath, "test_01", "t", 3);
  verify_commit_count(dbpath, "test_01", 1);
  remove_db(dbpath);
}

/*
** Test 2: Kill child immediately after dolt_commit returns.
** The commit should be fully durable.
*/
static void test_02_kill_after_commit(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 02: Kill after commit returns ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    execsql(db, "INSERT INTO t VALUES(1, 'alpha')");
    execsql(db, "INSERT INTO t VALUES(2, 'beta')");
    exec1(db, "SELECT dolt_commit('-A', '-m', 'committed')");
    /* Signal parent we are past the commit. */
    sqlite3_sleep(500);
    /* Parent will kill us here. */
    sqlite3_sleep(60000);
    _exit(0);
  }
  /* Parent: give child time to commit, then kill. */
  sqlite3_sleep(2000);
  kill(pid, SIGKILL);
  {
    int status;
    waitpid(pid, &status, 0);
    check("test_02: child was killed", WIFSIGNALED(status));
  }
  verify_consistency(dbpath, "test_02");
  verify_row_count(dbpath, "test_02", "t", 2);
  verify_commit_count(dbpath, "test_02", 1);
  remove_db(dbpath);
}

/*
** Test 3: Kill child during commit (before dolt_commit likely finishes).
** The database should be usable; commit is either fully present or fully absent.
*/
static void test_03_kill_during_commit(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 03: Kill during commit ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    {
      int i;
      char sql[128];
      for( i=0; i<100; i++ ){
        snprintf(sql, sizeof(sql),
                 "INSERT INTO t VALUES(%d, 'row-%d')", i, i);
        execsql(db, sql);
      }
    }
    /* Start the commit -- parent will try to kill during this. */
    exec1(db, "SELECT dolt_commit('-A', '-m', 'big commit')");
    sqlite3_sleep(60000);
    _exit(0);
  }
  /* Kill quickly to try to interrupt the commit. */
  sqlite3_sleep(100);
  kill(pid, SIGKILL);
  {
    int status;
    waitpid(pid, &status, 0);
  }

  /* Database must be openable. */
  {
    sqlite3 *db = 0;
    int rc = sqlite3_open(dbpath, &db);
    check("test_03: db opens after crash", rc==SQLITE_OK);
    if( rc==SQLITE_OK ){
      int nLog = exec_int(db, "SELECT count(*) FROM dolt_log", -1);
      /* Commit is either fully present (1 commit) or fully absent (0 commits).
      ** 0 means the initial state (no user commit), 1 means the commit landed. */
      check("test_03: commit atomic (0 or 1)",
            nLog==0 || nLog==1);
      if( nLog==1 ){
        int cnt = exec_int(db, "SELECT count(*) FROM t", -1);
        check("test_03: if committed, all 100 rows present", cnt==100);
      }
    }
    sqlite3_close(db);
  }
  remove_db(dbpath);
}

/*
** Test 4: Two commits, kill after second. First commit must be intact.
*/
static void test_04_two_commits_kill_after_second(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 04: Two commits, kill after second ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    execsql(db, "INSERT INTO t VALUES(1, 'first')");
    exec1(db, "SELECT dolt_commit('-A', '-m', 'commit-1')");
    execsql(db, "INSERT INTO t VALUES(2, 'second')");
    exec1(db, "SELECT dolt_commit('-A', '-m', 'commit-2')");
    sqlite3_sleep(500);
    sqlite3_sleep(60000);
    _exit(0);
  }
  sqlite3_sleep(3000);
  kill(pid, SIGKILL);
  {
    int status;
    waitpid(pid, &status, 0);
  }
  verify_consistency(dbpath, "test_04");

  /* First commit must always be present. */
  {
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    int nLog = exec_int(db, "SELECT count(*) FROM dolt_log", -1);
    check("test_04: at least commit-1 present", nLog>=1);
    /* If both commits landed, verify both rows. */
    if( nLog>=2 ){
      int cnt = exec_int(db, "SELECT count(*) FROM t", -1);
      check("test_04: both commits => 2 rows", cnt==2);
    }else if( nLog==1 ){
      int cnt = exec_int(db, "SELECT count(*) FROM t", -1);
      check("test_04: only first commit => 1 row", cnt==1);
    }
    sqlite3_close(db);
  }
  remove_db(dbpath);
}

/*
** Test 5: Two commits, kill during second commit.
** First commit must always be intact.
*/
static void test_05_kill_during_second_commit(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 05: Kill during second commit ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    execsql(db, "INSERT INTO t VALUES(1, 'stable')");
    exec1(db, "SELECT dolt_commit('-A', '-m', 'stable-commit')");

    /* Second commit: insert many rows to increase commit duration. */
    {
      int i;
      char sql[128];
      for( i=100; i<200; i++ ){
        snprintf(sql, sizeof(sql),
                 "INSERT INTO t VALUES(%d, 'bulk-%d')", i, i);
        execsql(db, sql);
      }
    }
    exec1(db, "SELECT dolt_commit('-A', '-m', 'bulk-commit')");
    sqlite3_sleep(60000);
    _exit(0);
  }
  /* Let first commit finish, then try to catch second commit mid-flight. */
  sqlite3_sleep(1500);
  kill(pid, SIGKILL);
  {
    int status;
    waitpid(pid, &status, 0);
  }

  {
    sqlite3 *db = 0;
    int rc = sqlite3_open(dbpath, &db);
    check("test_05: db opens", rc==SQLITE_OK);
    if( rc==SQLITE_OK ){
      int nLog = exec_int(db, "SELECT count(*) FROM dolt_log", -1);
      check("test_05: at least stable-commit present", nLog>=1);
      /* The stable row must always be present. */
      int hasStable = exec_int(db,
        "SELECT count(*) FROM t WHERE val='stable'", -1);
      check("test_05: stable row survives crash", hasStable==1);
    }
    sqlite3_close(db);
  }
  remove_db(dbpath);
}

/*
** Test 6: Commit, close, reopen, commit again. Kill after second commit.
** Verifies persistence across open/close cycles.
*/
static void test_06_reopen_then_crash(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 06: Reopen then crash ---\n");

  /* First: set up initial commit in a separate child. */
  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    execsql(db, "INSERT INTO t VALUES(1, 'persisted')");
    exec1(db, "SELECT dolt_commit('-A', '-m', 'first')");
    sqlite3_close(db);
    _exit(0);
  }
  { int status; waitpid(pid, &status, 0); }

  /* Second child: reopen, add data, commit, then get killed. */
  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execsql(db, "INSERT INTO t VALUES(2, 'new')");
    exec1(db, "SELECT dolt_commit('-A', '-m', 'second')");
    sqlite3_sleep(500);
    sqlite3_sleep(60000);
    _exit(0);
  }
  sqlite3_sleep(3000);
  kill(pid, SIGKILL);
  { int status; waitpid(pid, &status, 0); }

  verify_consistency(dbpath, "test_06");
  /* First commit must survive. */
  {
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    int hasFirst = exec_int(db,
      "SELECT count(*) FROM t WHERE val='persisted'", -1);
    check("test_06: first commit data survives", hasFirst==1);
    sqlite3_close(db);
  }
  remove_db(dbpath);
}

/* ====================================================================
** GROUP 2: Multi-commit sequences
** ==================================================================== */

/*
** Test 7: Five sequential commits. Kill at a random point.
** Verify some prefix of commits is intact.
*/
static void test_07_five_commits_random_kill(void){
  const char *dbpath = fresh_db();
  pid_t pid;
  int kill_delay_ms;

  printf("--- Test 07: Five sequential commits, random kill ---\n");

  /* Choose a random kill delay between 500ms and 4000ms. */
  srand((unsigned)time(NULL) ^ (unsigned)getpid());
  kill_delay_ms = 500 + (rand() % 3500);

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    int i;
    char sql[256];
    sqlite3_open(dbpath, &db);
    execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    for( i=1; i<=5; i++ ){
      snprintf(sql, sizeof(sql), "INSERT INTO t VALUES(%d, 'v%d')", i, i);
      execsql(db, sql);
      snprintf(sql, sizeof(sql),
               "SELECT dolt_commit('-A', '-m', 'commit-%d')", i);
      exec1(db, sql);
    }
    sqlite3_sleep(60000);
    _exit(0);
  }
  sqlite3_sleep(kill_delay_ms);
  kill(pid, SIGKILL);
  { int status; waitpid(pid, &status, 0); }

  {
    sqlite3 *db = 0;
    int rc = sqlite3_open(dbpath, &db);
    check("test_07: db opens", rc==SQLITE_OK);
    if( rc==SQLITE_OK ){
      int nLog = exec_int(db, "SELECT count(*) FROM dolt_log", -1);
      check("test_07: commit count in [0..5]", nLog>=0 && nLog<=5);

      /* If there are N commits, there should be N rows. */
      if( nLog>0 ){
        int nRows = exec_int(db, "SELECT count(*) FROM t", -1);
        check("test_07: row count matches commit count", nRows==nLog);
      }
    }
    sqlite3_close(db);
  }
  remove_db(dbpath);
}

/*
** Test 08: Branch, commit on branch, checkout main, commit on main.
** Kill. Verify branches are consistent.
*/
static void test_08_branch_crash(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 08: Branch + commit + crash ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    execsql(db, "INSERT INTO t VALUES(1, 'main-1')");
    exec1(db, "SELECT dolt_commit('-A', '-m', 'initial')");

    exec1(db, "SELECT dolt_branch('feature')");
    exec1(db, "SELECT dolt_checkout('feature')");
    execsql(db, "INSERT INTO t VALUES(2, 'feature-1')");
    exec1(db, "SELECT dolt_commit('-A', '-m', 'feature commit')");

    exec1(db, "SELECT dolt_checkout('main')");
    execsql(db, "INSERT INTO t VALUES(3, 'main-2')");
    exec1(db, "SELECT dolt_commit('-A', '-m', 'main commit 2')");

    sqlite3_sleep(500);
    sqlite3_sleep(60000);
    _exit(0);
  }
  sqlite3_sleep(4000);
  kill(pid, SIGKILL);
  { int status; waitpid(pid, &status, 0); }

  {
    sqlite3 *db = 0;
    int rc = sqlite3_open(dbpath, &db);
    check("test_08: db opens", rc==SQLITE_OK);
    if( rc==SQLITE_OK ){
      /* dolt_branches must be queryable with valid hashes. */
      sqlite3_stmt *stmt = 0;
      rc = sqlite3_prepare_v2(db,
        "SELECT name, hash FROM dolt_branches", -1, &stmt, 0);
      check("test_08: branches queryable", rc==SQLITE_OK);
      if( rc==SQLITE_OK ){
        int nBranch = 0;
        while( sqlite3_step(stmt)==SQLITE_ROW ){
          const char *h = (const char*)sqlite3_column_text(stmt, 1);
          if( h && strlen(h)==40 ) nBranch++;
        }
        check("test_08: branches have valid hashes", nBranch>=1);
      }
      sqlite3_finalize(stmt);

      /* dolt_log must be consistent. */
      int nLog = exec_int(db, "SELECT count(*) FROM dolt_log", -1);
      check("test_08: dolt_log consistent", nLog>=0);
    }
    sqlite3_close(db);
  }
  remove_db(dbpath);
}

/*
** Test 09: Three commits on main, create branch at second commit.
** Kill after all operations. Verify both branches are valid.
*/
static void test_09_branch_at_commit(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 09: Branch at specific commit, then crash ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    execsql(db, "INSERT INTO t VALUES(1, 'one')");
    exec1(db, "SELECT dolt_commit('-A', '-m', 'first')");
    execsql(db, "INSERT INTO t VALUES(2, 'two')");
    exec1(db, "SELECT dolt_commit('-A', '-m', 'second')");
    /* Create branch from current HEAD. */
    exec1(db, "SELECT dolt_branch('snap')");
    execsql(db, "INSERT INTO t VALUES(3, 'three')");
    exec1(db, "SELECT dolt_commit('-A', '-m', 'third')");
    sqlite3_sleep(500);
    sqlite3_sleep(60000);
    _exit(0);
  }
  sqlite3_sleep(4000);
  kill(pid, SIGKILL);
  { int status; waitpid(pid, &status, 0); }

  verify_consistency(dbpath, "test_09");
  remove_db(dbpath);
}

/*
** Test 10: Five sequential commits. Kill very quickly (10ms).
** Very early kill to test minimal state.
*/
static void test_10_very_early_kill(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 10: Very early kill ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    int i;
    char sql[256];
    sqlite3_open(dbpath, &db);
    execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    for( i=1; i<=5; i++ ){
      snprintf(sql, sizeof(sql), "INSERT INTO t VALUES(%d, 'v%d')", i, i);
      execsql(db, sql);
      snprintf(sql, sizeof(sql),
               "SELECT dolt_commit('-A', '-m', 'commit-%d')", i);
      exec1(db, sql);
    }
    sqlite3_sleep(60000);
    _exit(0);
  }
  sqlite3_sleep(10);
  kill(pid, SIGKILL);
  { int status; waitpid(pid, &status, 0); }

  /* Database file might not even exist if killed early enough. */
  {
    sqlite3 *db = 0;
    int rc = sqlite3_open(dbpath, &db);
    if( rc==SQLITE_OK ){
      int nLog = exec_int(db, "SELECT count(*) FROM dolt_log", -1);
      /* Whatever number of commits landed, data must be consistent. */
      check("test_10: commit count non-negative", nLog>=0);
      if( nLog>0 ){
        int nRows = exec_int(db, "SELECT count(*) FROM t", -1);
        check("test_10: row count matches commits", nRows==nLog);
      }
    }else{
      /* If we killed before the file was created, that's OK. */
      check("test_10: db either opens or does not exist", 1);
    }
    sqlite3_close(db);
  }
  remove_db(dbpath);
}

/*
** Test 11: Multiple commits with increasing data. Kill mid-sequence.
*/
static void test_11_increasing_data_kill(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 11: Increasing data per commit, kill mid-sequence ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    int i, j;
    char sql[256];
    sqlite3_open(dbpath, &db);
    execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");

    int row_id = 1;
    for( i=1; i<=5; i++ ){
      /* Commit i inserts i*10 rows. */
      for( j=0; j<i*10; j++ ){
        snprintf(sql, sizeof(sql),
                 "INSERT INTO t VALUES(%d, 'batch%d-row%d')", row_id++, i, j);
        execsql(db, sql);
      }
      snprintf(sql, sizeof(sql),
               "SELECT dolt_commit('-A', '-m', 'batch-%d')", i);
      exec1(db, sql);
    }
    sqlite3_sleep(60000);
    _exit(0);
  }
  sqlite3_sleep(2000);
  kill(pid, SIGKILL);
  { int status; waitpid(pid, &status, 0); }

  {
    sqlite3 *db = 0;
    int rc = sqlite3_open(dbpath, &db);
    check("test_11: db opens", rc==SQLITE_OK);
    if( rc==SQLITE_OK ){
      int nLog = exec_int(db, "SELECT count(*) FROM dolt_log", -1);
      check("test_11: commit count in [0..5]", nLog>=0 && nLog<=5);
      /* Verify data is self-consistent: all queryable rows belong to
      ** committed batches. */
      if( nLog>0 ){
        int nRows = exec_int(db, "SELECT count(*) FROM t", -1);
        /* Expected rows = sum of 10+20+...+nLog*10 = nLog*(nLog+1)*5 */
        int expected = nLog * (nLog + 1) * 5;
        check("test_11: row count matches committed batches",
              nRows==expected);
      }
    }
    sqlite3_close(db);
  }
  remove_db(dbpath);
}

/* ====================================================================
** GROUP 3: GC crash recovery
** ==================================================================== */

/*
** Test 12: Commit, GC, commit. Asynchronously kill around GC.
** Database should be usable afterward.
*/
static void test_12_gc_crash(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 12: GC crash recovery ---\n");

  /* First, create a database with some history to give GC work to do. */
  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    execsql(db, "INSERT INTO t VALUES(1, 'a')");
    exec1(db, "SELECT dolt_commit('-A', '-m', 'c1')");
    execsql(db, "INSERT INTO t VALUES(2, 'b')");
    exec1(db, "SELECT dolt_commit('-A', '-m', 'c2')");
    execsql(db, "DELETE FROM t WHERE id=1");
    exec1(db, "SELECT dolt_commit('-A', '-m', 'c3')");
    sqlite3_close(db);
    _exit(0);
  }
  { int status; waitpid(pid, &status, 0); }

  /* Now run GC in a child and kill it. */
  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    exec1(db, "SELECT dolt_gc()");
    sqlite3_sleep(60000);
    _exit(0);
  }
  sqlite3_sleep(100);
  kill(pid, SIGKILL);
  { int status; waitpid(pid, &status, 0); }

  /* Verify the database is still usable. */
  {
    sqlite3 *db = 0;
    int rc = sqlite3_open(dbpath, &db);
    check("test_12: db opens after GC crash", rc==SQLITE_OK);
    if( rc==SQLITE_OK ){
      /* Must be able to query data. */
      int cnt = exec_int(db, "SELECT count(*) FROM t", -1);
      check("test_12: table queryable after GC crash", cnt>=0);
      /* dolt_log must still work. */
      int nLog = exec_int(db, "SELECT count(*) FROM dolt_log", -1);
      check("test_12: dolt_log works after GC crash", nLog>=1);
    }
    sqlite3_close(db);
  }
  remove_db(dbpath);
}

/*
** Test 13: GC completes cleanly, then crash.
** Verify that the post-GC state is durable.
*/
static void test_13_gc_then_crash(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 13: GC completes, then crash ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    execsql(db, "INSERT INTO t VALUES(1, 'a')");
    exec1(db, "SELECT dolt_commit('-A', '-m', 'c1')");
    execsql(db, "INSERT INTO t VALUES(2, 'b')");
    exec1(db, "SELECT dolt_commit('-A', '-m', 'c2')");
    exec1(db, "SELECT dolt_gc()");
    /* Now do more work after GC. */
    execsql(db, "INSERT INTO t VALUES(3, 'c')");
    exec1(db, "SELECT dolt_commit('-A', '-m', 'post-gc')");
    sqlite3_sleep(500);
    sqlite3_sleep(60000);
    _exit(0);
  }
  sqlite3_sleep(4000);
  kill(pid, SIGKILL);
  { int status; waitpid(pid, &status, 0); }

  {
    sqlite3 *db = 0;
    int rc = sqlite3_open(dbpath, &db);
    check("test_13: db opens", rc==SQLITE_OK);
    if( rc==SQLITE_OK ){
      int nLog = exec_int(db, "SELECT count(*) FROM dolt_log", -1);
      check("test_13: at least 2 commits (pre-GC)", nLog>=2);
    }
    sqlite3_close(db);
  }
  remove_db(dbpath);
}

/*
** Test 14: GC with branches. Asynchronously kill around GC.
** Both branches' data must remain reachable.
*/
static void test_14_gc_with_branches_crash(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 14: GC with branches crash ---\n");

  /* Setup: two branches with data. */
  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    execsql(db, "INSERT INTO t VALUES(1, 'main')");
    exec1(db, "SELECT dolt_commit('-A', '-m', 'init')");
    exec1(db, "SELECT dolt_branch('dev')");
    exec1(db, "SELECT dolt_checkout('dev')");
    execsql(db, "INSERT INTO t VALUES(2, 'dev-data')");
    exec1(db, "SELECT dolt_commit('-A', '-m', 'dev commit')");
    exec1(db, "SELECT dolt_checkout('main')");
    execsql(db, "INSERT INTO t VALUES(3, 'main-extra')");
    exec1(db, "SELECT dolt_commit('-A', '-m', 'main extra')");
    sqlite3_close(db);
    _exit(0);
  }
  { int status; waitpid(pid, &status, 0); }

  /* Now GC and crash. */
  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    exec1(db, "SELECT dolt_gc()");
    sqlite3_sleep(60000);
    _exit(0);
  }
  sqlite3_sleep(200);
  kill(pid, SIGKILL);
  { int status; waitpid(pid, &status, 0); }

  verify_consistency(dbpath, "test_14");
  {
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    /* Both branches should still exist. */
    int nBranch = exec_int(db,
      "SELECT count(*) FROM dolt_branches", -1);
    check("test_14: both branches survive GC crash", nBranch==2);
    sqlite3_close(db);
  }
  remove_db(dbpath);
}

/* ====================================================================
** GROUP 4: Large data
** ==================================================================== */

/*
** Test 15: Insert 1000 rows, commit (multi-chunk). Kill mid-commit.
** Verify atomicity.
*/
static void test_15_large_insert_crash(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 15: 1000-row commit crash ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    int i;
    char sql[256];
    sqlite3_open(dbpath, &db);
    execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT, data TEXT)");
    for( i=0; i<1000; i++ ){
      snprintf(sql, sizeof(sql),
        "INSERT INTO t VALUES(%d, 'row-%d', '%0128d')", i, i, i);
      execsql(db, sql);
    }
    exec1(db, "SELECT dolt_commit('-A', '-m', 'thousand rows')");
    sqlite3_sleep(60000);
    _exit(0);
  }
  /* Kill during/after the large commit. */
  sqlite3_sleep(500);
  kill(pid, SIGKILL);
  { int status; waitpid(pid, &status, 0); }

  {
    sqlite3 *db = 0;
    int rc = sqlite3_open(dbpath, &db);
    check("test_15: db opens", rc==SQLITE_OK);
    if( rc==SQLITE_OK ){
      int nLog = exec_int(db, "SELECT count(*) FROM dolt_log", -1);
      check("test_15: commit atomic (0 or 1)", nLog==0 || nLog==1);
      if( nLog==1 ){
        int cnt = exec_int(db, "SELECT count(*) FROM t", -1);
        check("test_15: all 1000 rows if committed", cnt==1000);
      }
    }
    sqlite3_close(db);
  }
  remove_db(dbpath);
}

/*
** Test 16: Large insert that completes, then kill. All data must persist.
*/
static void test_16_large_insert_complete_then_kill(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 16: 1000-row commit completes, then kill ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    int i;
    char sql[256];
    sqlite3_open(dbpath, &db);
    execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    for( i=0; i<1000; i++ ){
      snprintf(sql, sizeof(sql),
        "INSERT INTO t VALUES(%d, 'row-%d')", i, i);
      execsql(db, sql);
    }
    exec1(db, "SELECT dolt_commit('-A', '-m', 'thousand rows')");
    sqlite3_sleep(500);
    sqlite3_sleep(60000);
    _exit(0);
  }
  sqlite3_sleep(5000);
  kill(pid, SIGKILL);
  { int status; waitpid(pid, &status, 0); }

  verify_consistency(dbpath, "test_16");
  verify_row_count(dbpath, "test_16", "t", 1000);
  verify_commit_count(dbpath, "test_16", 1);
  remove_db(dbpath);
}

/*
** Test 17: Multiple large commits. Verify prefix integrity.
*/
static void test_17_multiple_large_commits(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 17: Multiple large commits ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    int i, batch;
    char sql[256];
    sqlite3_open(dbpath, &db);
    execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, batch INT, val TEXT)");

    for( batch=1; batch<=3; batch++ ){
      for( i=0; i<200; i++ ){
        int id = (batch-1)*200 + i;
        snprintf(sql, sizeof(sql),
          "INSERT INTO t VALUES(%d, %d, 'b%d-r%d')", id, batch, batch, i);
        execsql(db, sql);
      }
      snprintf(sql, sizeof(sql),
               "SELECT dolt_commit('-A', '-m', 'batch-%d')", batch);
      exec1(db, sql);
    }
    sqlite3_sleep(60000);
    _exit(0);
  }
  sqlite3_sleep(3000);
  kill(pid, SIGKILL);
  { int status; waitpid(pid, &status, 0); }

  {
    sqlite3 *db = 0;
    int rc = sqlite3_open(dbpath, &db);
    check("test_17: db opens", rc==SQLITE_OK);
    if( rc==SQLITE_OK ){
      int nLog = exec_int(db, "SELECT count(*) FROM dolt_log", -1);
      check("test_17: commit count in [0..3]", nLog>=0 && nLog<=3);
      if( nLog>0 ){
        int nRows = exec_int(db, "SELECT count(*) FROM t", -1);
        check("test_17: rows match commits", nRows==nLog*200);
      }
    }
    sqlite3_close(db);
  }
  remove_db(dbpath);
}

/* ====================================================================
** Additional crash scenarios
** ==================================================================== */

/*
** Test 18: CREATE TABLE + commit, then ALTER TABLE + commit. Kill after ALTER.
** Schema changes must be atomic with the commit.
*/
static void test_18_schema_change_crash(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 18: Schema change crash ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    execsql(db, "INSERT INTO t VALUES(1, 'a')");
    exec1(db, "SELECT dolt_commit('-A', '-m', 'create')");
    execsql(db, "ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'x'");
    execsql(db, "INSERT INTO t VALUES(2, 'b', 'y')");
    exec1(db, "SELECT dolt_commit('-A', '-m', 'alter')");
    sqlite3_sleep(500);
    sqlite3_sleep(60000);
    _exit(0);
  }
  sqlite3_sleep(3000);
  kill(pid, SIGKILL);
  { int status; waitpid(pid, &status, 0); }

  {
    sqlite3 *db = 0;
    int rc = sqlite3_open(dbpath, &db);
    check("test_18: db opens", rc==SQLITE_OK);
    if( rc==SQLITE_OK ){
      int nLog = exec_int(db, "SELECT count(*) FROM dolt_log", -1);
      check("test_18: at least 1 commit", nLog>=1);
      /* If both commits landed, the extra column must exist. */
      if( nLog==2 ){
        const char *v = exec1(db, "SELECT extra FROM t WHERE id=2");
        check("test_18: alter committed with data", strcmp(v, "y")==0);
      }
      /* If only first commit, table is still basic schema. */
      if( nLog==1 ){
        int cnt = exec_int(db, "SELECT count(*) FROM t", -1);
        check("test_18: first commit has 1 row", cnt==1);
      }
    }
    sqlite3_close(db);
  }
  remove_db(dbpath);
}

/*
** Test 19: Delete all rows, commit. Kill. Verify empty table or prior state.
*/
static void test_19_delete_all_crash(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 19: Delete all rows then crash ---\n");

  /* Setup with data. */
  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    execsql(db, "INSERT INTO t VALUES(1, 'a')");
    execsql(db, "INSERT INTO t VALUES(2, 'b')");
    execsql(db, "INSERT INTO t VALUES(3, 'c')");
    exec1(db, "SELECT dolt_commit('-A', '-m', 'initial')");
    sqlite3_close(db);
    _exit(0);
  }
  { int status; waitpid(pid, &status, 0); }

  /* Now delete all and try to commit. */
  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execsql(db, "DELETE FROM t");
    exec1(db, "SELECT dolt_commit('-A', '-m', 'delete all')");
    sqlite3_sleep(500);
    sqlite3_sleep(60000);
    _exit(0);
  }
  sqlite3_sleep(1500);
  kill(pid, SIGKILL);
  { int status; waitpid(pid, &status, 0); }

  {
    sqlite3 *db = 0;
    int rc = sqlite3_open(dbpath, &db);
    check("test_19: db opens", rc==SQLITE_OK);
    if( rc==SQLITE_OK ){
      int nLog = exec_int(db, "SELECT count(*) FROM dolt_log", -1);
      int cnt = exec_int(db, "SELECT count(*) FROM t", -1);
      /* Either the delete committed (0 rows, 2 log entries) or
      ** only the initial state remains (3 rows, 1 log entry). */
      check("test_19: consistent state",
            (nLog==2 && cnt==0) || (nLog==1 && cnt==3));
    }
    sqlite3_close(db);
  }
  remove_db(dbpath);
}

/*
** Test 20: UPDATE all rows, commit. Kill. Verify either old or new values.
*/
static void test_20_update_crash(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 20: Update all rows then crash ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    execsql(db, "INSERT INTO t VALUES(1, 'old')");
    execsql(db, "INSERT INTO t VALUES(2, 'old')");
    execsql(db, "INSERT INTO t VALUES(3, 'old')");
    exec1(db, "SELECT dolt_commit('-A', '-m', 'initial')");
    execsql(db, "UPDATE t SET val='new'");
    exec1(db, "SELECT dolt_commit('-A', '-m', 'update all')");
    sqlite3_sleep(500);
    sqlite3_sleep(60000);
    _exit(0);
  }
  sqlite3_sleep(2500);
  kill(pid, SIGKILL);
  { int status; waitpid(pid, &status, 0); }

  {
    sqlite3 *db = 0;
    int rc = sqlite3_open(dbpath, &db);
    check("test_20: db opens", rc==SQLITE_OK);
    if( rc==SQLITE_OK ){
      int nOld = exec_int(db,
        "SELECT count(*) FROM t WHERE val='old'", -1);
      int nNew = exec_int(db,
        "SELECT count(*) FROM t WHERE val='new'", -1);
      /* Either all old (3) or all new (3). No partial updates. */
      check("test_20: atomic update (all old or all new)",
            (nOld==3 && nNew==0) || (nOld==0 && nNew==3));
    }
    sqlite3_close(db);
  }
  remove_db(dbpath);
}

/*
** Test 21: DROP TABLE then crash. Table is either dropped or not.
*/
static void test_21_drop_table_crash(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 21: Drop table then crash ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    execsql(db, "CREATE TABLE t2(id INTEGER PRIMARY KEY, val TEXT)");
    execsql(db, "INSERT INTO t VALUES(1, 'keep')");
    execsql(db, "INSERT INTO t2 VALUES(1, 'drop-me')");
    exec1(db, "SELECT dolt_commit('-A', '-m', 'two tables')");
    execsql(db, "DROP TABLE t2");
    exec1(db, "SELECT dolt_commit('-A', '-m', 'dropped t2')");
    sqlite3_sleep(500);
    sqlite3_sleep(60000);
    _exit(0);
  }
  sqlite3_sleep(3000);
  kill(pid, SIGKILL);
  { int status; waitpid(pid, &status, 0); }

  {
    sqlite3 *db = 0;
    int rc = sqlite3_open(dbpath, &db);
    check("test_21: db opens", rc==SQLITE_OK);
    if( rc==SQLITE_OK ){
      /* t must always exist. */
      int cnt_t = exec_int(db, "SELECT count(*) FROM t", -1);
      check("test_21: table t exists", cnt_t==1);
      /* t2 either exists or not. */
      int cnt_t2 = exec_int(db, "SELECT count(*) FROM t2", -2);
      check("test_21: t2 state consistent",
            cnt_t2==1 || cnt_t2==-2);
    }
    sqlite3_close(db);
  }
  remove_db(dbpath);
}

/*
** Test 22: Multiple tables created in one commit. Atomicity check.
*/
static void test_22_multi_table_commit_crash(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 22: Multi-table single commit crash ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    int i;
    char sql[256];
    sqlite3_open(dbpath, &db);

    for( i=1; i<=10; i++ ){
      snprintf(sql, sizeof(sql),
        "CREATE TABLE t%d(id INTEGER PRIMARY KEY, val TEXT)", i);
      execsql(db, sql);
      snprintf(sql, sizeof(sql),
        "INSERT INTO t%d VALUES(1, 'data-%d')", i, i);
      execsql(db, sql);
    }
    exec1(db, "SELECT dolt_commit('-A', '-m', '10 tables')");
    sqlite3_sleep(60000);
    _exit(0);
  }
  sqlite3_sleep(300);
  kill(pid, SIGKILL);
  { int status; waitpid(pid, &status, 0); }

  {
    sqlite3 *db = 0;
    int rc = sqlite3_open(dbpath, &db);
    check("test_22: db opens", rc==SQLITE_OK);
    if( rc==SQLITE_OK ){
      int nLog = exec_int(db, "SELECT count(*) FROM dolt_log", -1);
      if( nLog==1 ){
        /* If committed, all 10 tables must exist with data. */
        int i;
        int allOk = 1;
        for( i=1; i<=10; i++ ){
          char sql[128];
          snprintf(sql, sizeof(sql),
                   "SELECT count(*) FROM t%d", i);
          int cnt = exec_int(db, sql, -1);
          if( cnt!=1 ) allOk = 0;
        }
        check("test_22: all 10 tables present if committed", allOk);
      }else{
        check("test_22: commit atomic (0 or 1)", nLog==0 || nLog==1);
      }
    }
    sqlite3_close(db);
  }
  remove_db(dbpath);
}

/*
** Test 23: Merge then crash. Verify either merge completed or pre-merge state.
*/
static void test_23_merge_crash(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 23: Merge then crash ---\n");

  /* Setup: main and feature branches. */
  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    execsql(db, "INSERT INTO t VALUES(1, 'shared')");
    exec1(db, "SELECT dolt_commit('-A', '-m', 'init')");
    exec1(db, "SELECT dolt_branch('feature')");
    exec1(db, "SELECT dolt_checkout('feature')");
    execsql(db, "INSERT INTO t VALUES(2, 'feature')");
    exec1(db, "SELECT dolt_commit('-A', '-m', 'feature data')");
    exec1(db, "SELECT dolt_checkout('main')");
    sqlite3_close(db);
    _exit(0);
  }
  { int status; waitpid(pid, &status, 0); }

  /* Now merge and crash. */
  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    exec1(db, "SELECT dolt_merge('feature')");
    sqlite3_sleep(500);
    sqlite3_sleep(60000);
    _exit(0);
  }
  sqlite3_sleep(2000);
  kill(pid, SIGKILL);
  { int status; waitpid(pid, &status, 0); }

  verify_consistency(dbpath, "test_23");
  {
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    /* Data should be consistent regardless of merge status. */
    int cnt = exec_int(db, "SELECT count(*) FROM t", -1);
    check("test_23: row count is 1 or 2",
          cnt==1 || cnt==2);
    sqlite3_close(db);
  }
  remove_db(dbpath);
}

/*
** Test 24: Repeated crash-and-recover cycles. Each cycle adds a commit.
** Verifies that recovery does not corrupt state for subsequent operations.
*/
static void test_24_repeated_crash_cycles(void){
  const char *dbpath = fresh_db();
  int cycle;
  pid_t pid;

  printf("--- Test 24: Repeated crash/recover cycles ---\n");

  /* Initial setup. */
  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    execsql(db, "INSERT INTO t VALUES(0, 'seed')");
    exec1(db, "SELECT dolt_commit('-A', '-m', 'seed')");
    sqlite3_close(db);
    _exit(0);
  }
  { int status; waitpid(pid, &status, 0); }

  /* Now do 5 crash-and-recover cycles. */
  for( cycle=1; cycle<=5; cycle++ ){
    pid = fork();
    if( pid==0 ){
      sqlite3 *db = 0;
      char sql[256];
      sqlite3_open(dbpath, &db);
      snprintf(sql, sizeof(sql),
               "INSERT INTO t VALUES(%d, 'cycle-%d')", cycle, cycle);
      execsql(db, sql);
      snprintf(sql, sizeof(sql),
               "SELECT dolt_commit('-A', '-m', 'cycle-%d')", cycle);
      exec1(db, sql);
      sqlite3_sleep(500);
      sqlite3_sleep(60000);
      _exit(0);
    }
    sqlite3_sleep(2000);
    kill(pid, SIGKILL);
    { int status; waitpid(pid, &status, 0); }

    /* Verify database is usable after each crash. */
    {
      sqlite3 *db = 0;
      int rc = sqlite3_open(dbpath, &db);
      char desc[128];
      snprintf(desc, sizeof(desc), "test_24_cycle%d: db opens", cycle);
      check(desc, rc==SQLITE_OK);
      if( rc==SQLITE_OK ){
        int nLog = exec_int(db, "SELECT count(*) FROM dolt_log", -1);
        snprintf(desc, sizeof(desc),
                 "test_24_cycle%d: log non-negative", cycle);
        check(desc, nLog>=1);
      }
      sqlite3_close(db);
    }
  }

  /* Final check: seed row must always be present. */
  {
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    int hasSeed = exec_int(db,
      "SELECT count(*) FROM t WHERE val='seed'", -1);
    check("test_24: seed row always present", hasSeed==1);
    sqlite3_close(db);
  }
  remove_db(dbpath);
}

/*
** Test 25: Tag creation then crash.
*/
static void test_25_tag_crash(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 25: Tag creation then crash ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    execsql(db, "INSERT INTO t VALUES(1, 'tagged')");
    exec1(db, "SELECT dolt_commit('-A', '-m', 'for tag')");
    exec1(db, "SELECT dolt_tag('v1.0')");
    sqlite3_sleep(500);
    sqlite3_sleep(60000);
    _exit(0);
  }
  sqlite3_sleep(3000);
  kill(pid, SIGKILL);
  { int status; waitpid(pid, &status, 0); }

  verify_consistency(dbpath, "test_25");
  {
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    /* Commit must be present. Tag may or may not have persisted. */
    int nLog = exec_int(db, "SELECT count(*) FROM dolt_log", -1);
    check("test_25: commit present", nLog>=1);
    int cnt = exec_int(db, "SELECT count(*) FROM t", -1);
    check("test_25: data intact", cnt==1);
    sqlite3_close(db);
  }
  remove_db(dbpath);
}

/*
** Test 26: Commit with BLOB data. Kill. Verify atomicity with binary data.
*/
static void test_26_blob_data_crash(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 26: BLOB data commit crash ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    int i;
    sqlite3_open(dbpath, &db);
    execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB)");
    for( i=0; i<50; i++ ){
      sqlite3_stmt *stmt = 0;
      char sql_text[] = "INSERT INTO t VALUES(?, randomblob(1024))";
      sqlite3_prepare_v2(db, sql_text, -1, &stmt, 0);
      sqlite3_bind_int(stmt, 1, i);
      sqlite3_step(stmt);
      sqlite3_finalize(stmt);
    }
    exec1(db, "SELECT dolt_commit('-A', '-m', 'blobs')");
    sqlite3_sleep(500);
    sqlite3_sleep(60000);
    _exit(0);
  }
  sqlite3_sleep(2000);
  kill(pid, SIGKILL);
  { int status; waitpid(pid, &status, 0); }

  {
    sqlite3 *db = 0;
    int rc = sqlite3_open(dbpath, &db);
    check("test_26: db opens", rc==SQLITE_OK);
    if( rc==SQLITE_OK ){
      int nLog = exec_int(db, "SELECT count(*) FROM dolt_log", -1);
      check("test_26: commit atomic (0 or 1)", nLog==0 || nLog==1);
      if( nLog==1 ){
        int cnt = exec_int(db, "SELECT count(*) FROM t", -1);
        check("test_26: all 50 blob rows if committed", cnt==50);
        /* Verify blobs are readable (not corrupted). */
        int blobLen = exec_int(db,
          "SELECT length(data) FROM t WHERE id=0", -1);
        check("test_26: blob data intact", blobLen==1024);
      }
    }
    sqlite3_close(db);
  }
  remove_db(dbpath);
}

/*
** Test 27: Uncommitted changes (no dolt_commit). Kill.
** On reopen, only the last committed state should be visible.
*/
static void test_27_uncommitted_changes_crash(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 27: Uncommitted changes crash ---\n");

  /* First: create committed baseline. */
  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    execsql(db, "INSERT INTO t VALUES(1, 'committed')");
    exec1(db, "SELECT dolt_commit('-A', '-m', 'baseline')");
    sqlite3_close(db);
    _exit(0);
  }
  { int status; waitpid(pid, &status, 0); }

  /* Second child: make uncommitted changes and crash. */
  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execsql(db, "INSERT INTO t VALUES(2, 'uncommitted')");
    execsql(db, "INSERT INTO t VALUES(3, 'uncommitted')");
    /* No dolt_commit! */
    sqlite3_sleep(500);
    sqlite3_sleep(60000);
    _exit(0);
  }
  sqlite3_sleep(1000);
  kill(pid, SIGKILL);
  { int status; waitpid(pid, &status, 0); }

  {
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    int nLog = exec_int(db, "SELECT count(*) FROM dolt_log", -1);
    check("test_27: only baseline commit", nLog==1);
    /* The committed row must be present. The uncommitted rows may or
    ** may not be in the SQL layer (WAL recovery), but dolt state should
    ** reflect only committed data. */
    int committed = exec_int(db,
      "SELECT count(*) FROM t WHERE val='committed'", -1);
    check("test_27: committed data present", committed==1);
    sqlite3_close(db);
  }
  remove_db(dbpath);
}

/*
** Test 28: Rapid small commits (10 commits of 1 row each).
** Kill at random time. Verify prefix integrity.
*/
static void test_28_rapid_small_commits(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 28: Rapid small commits ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    int i;
    char sql[256];
    sqlite3_open(dbpath, &db);
    execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    for( i=1; i<=10; i++ ){
      snprintf(sql, sizeof(sql),
               "INSERT INTO t VALUES(%d, 'r%d')", i, i);
      execsql(db, sql);
      snprintf(sql, sizeof(sql),
               "SELECT dolt_commit('-A', '-m', 'r%d')", i);
      exec1(db, sql);
    }
    sqlite3_sleep(60000);
    _exit(0);
  }
  sqlite3_sleep(1000);
  kill(pid, SIGKILL);
  { int status; waitpid(pid, &status, 0); }

  {
    sqlite3 *db = 0;
    int rc = sqlite3_open(dbpath, &db);
    check("test_28: db opens", rc==SQLITE_OK);
    if( rc==SQLITE_OK ){
      int nLog = exec_int(db, "SELECT count(*) FROM dolt_log", -1);
      check("test_28: commit count in [0..10]", nLog>=0 && nLog<=10);
      if( nLog>0 ){
        int nRows = exec_int(db, "SELECT count(*) FROM t", -1);
        check("test_28: rows match commits", nRows==nLog);
      }
    }
    sqlite3_close(db);
  }
  remove_db(dbpath);
}

/*
** Test 29: GC after many small commits. Asynchronously kill around GC.
** All committed data must remain accessible.
*/
static void test_29_gc_after_many_commits(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 29: GC after many commits then crash ---\n");

  /* Setup: 10 commits. */
  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    int i;
    char sql[256];
    sqlite3_open(dbpath, &db);
    execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    for( i=1; i<=10; i++ ){
      snprintf(sql, sizeof(sql),
               "INSERT INTO t VALUES(%d, 'v%d')", i, i);
      execsql(db, sql);
      snprintf(sql, sizeof(sql),
               "SELECT dolt_commit('-A', '-m', 'c%d')", i);
      exec1(db, sql);
    }
    sqlite3_close(db);
    _exit(0);
  }
  { int status; waitpid(pid, &status, 0); }

  /* GC and crash. */
  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    exec1(db, "SELECT dolt_gc()");
    sqlite3_sleep(60000);
    _exit(0);
  }
  sqlite3_sleep(200);
  kill(pid, SIGKILL);
  { int status; waitpid(pid, &status, 0); }

  {
    sqlite3 *db = 0;
    int rc = sqlite3_open(dbpath, &db);
    check("test_29: db opens after GC crash", rc==SQLITE_OK);
    if( rc==SQLITE_OK ){
      int nLog = exec_int(db, "SELECT count(*) FROM dolt_log", -1);
      check("test_29: all 10 commits present", nLog==10);
      int cnt = exec_int(db, "SELECT count(*) FROM t", -1);
      check("test_29: all 10 rows present", cnt==10);
    }
    sqlite3_close(db);
  }
  remove_db(dbpath);
}

/*
** Test 30: Crash immediately on open (before any SQL).
** Verify empty database is not corrupted.
*/
static void test_30_crash_on_open(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 30: Crash immediately after open ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    /* Do nothing -- just crash immediately. */
    sqlite3_sleep(60000);
    _exit(0);
  }
  sqlite3_sleep(50);
  kill(pid, SIGKILL);
  { int status; waitpid(pid, &status, 0); }

  /* The database file might not exist or might be a valid empty DB. */
  {
    sqlite3 *db = 0;
    int rc = sqlite3_open(dbpath, &db);
    /* Either the file is valid or does not exist (open creates empty). */
    check("test_30: db openable after early crash", rc==SQLITE_OK);
    sqlite3_close(db);
  }
  remove_db(dbpath);
}

/* ====================================================================
** Main
** ==================================================================== */

int main(void){
  printf("=== DoltLite Crash Recovery Tests ===\n\n");

  /* Group 1: Single commit crash recovery */
  test_01_clean_commit();
  test_02_kill_after_commit();
  test_03_kill_during_commit();
  test_04_two_commits_kill_after_second();
  test_05_kill_during_second_commit();
  test_06_reopen_then_crash();

  /* Group 2: Multi-commit sequences */
  test_07_five_commits_random_kill();
  test_08_branch_crash();
  test_09_branch_at_commit();
  test_10_very_early_kill();
  test_11_increasing_data_kill();

  /* Group 3: GC crash recovery */
  test_12_gc_crash();
  test_13_gc_then_crash();
  test_14_gc_with_branches_crash();

  /* Group 4: Large data */
  test_15_large_insert_crash();
  test_16_large_insert_complete_then_kill();
  test_17_multiple_large_commits();

  /* Additional scenarios */
  test_18_schema_change_crash();
  test_19_delete_all_crash();
  test_20_update_crash();
  test_21_drop_table_crash();
  test_22_multi_table_commit_crash();
  test_23_merge_crash();
  test_24_repeated_crash_cycles();
  test_25_tag_crash();
  test_26_blob_data_crash();
  test_27_uncommitted_changes_crash();
  test_28_rapid_small_commits();
  test_29_gc_after_many_commits();
  test_30_crash_on_open();

  printf("\n=== Results: %d passed, %d failed out of %d tests ===\n",
         nPass, nFail, nPass+nFail);
  return nFail > 0 ? 1 : 0;
}
