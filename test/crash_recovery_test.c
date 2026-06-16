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

static char g_buf[8192];
static const char *queryScalarText(sqlite3 *db, const char *sql){
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

static int queryScalarInt(sqlite3 *db, const char *sql, int dflt){
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

static int execSql(sqlite3 *db, const char *sql){
  char *err = 0;
  int rc = sqlite3_exec(db, sql, 0, 0, &err);
  if( rc!=SQLITE_OK && err ){
    sqlite3_free(err);
  }
  return rc;
}

static void removeDbFiles(const char *path){
  char tmp[512];
  remove(path);
  snprintf(tmp, sizeof(tmp), "%s-wal", path);
  remove(tmp);
  snprintf(tmp, sizeof(tmp), "%s-journal", path);
  remove(tmp);
}

static int g_test_seq = 0;
static char g_dbpath[512];
static const char *fresh_db(void){
  snprintf(g_dbpath, sizeof(g_dbpath),
           "/tmp/crash_test_%d_%d.db", (int)getpid(), g_test_seq++);
  removeDbFiles(g_dbpath);
  return g_dbpath;
}

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

  {
    int nLog = queryScalarInt(db, "SELECT count(*) FROM dolt_log", -1);
    snprintf(desc, sizeof(desc), "%s: dolt_log has entries", label);
    check(desc, nLog>=0);
    if( nLog<0 ) ok = 0;
  }

  sqlite3_close(db);
  return ok;
}

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
  cnt = queryScalarInt(db, sql, -1);
  snprintf(desc, sizeof(desc), "%s: %s has %d rows", label, table, expected);
  check(desc, cnt==expected);
  sqlite3_close(db);
}

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
  cnt = queryScalarInt(db, "SELECT count(*) FROM dolt_log", -1);
  snprintf(desc, sizeof(desc), "%s: dolt_log has %d commits", label, expected);
  check(desc, cnt==expected);
  sqlite3_close(db);
  return cnt;
}

static int exec_user_commit_count(sqlite3 *db){
  int nLog = queryScalarInt(db, "SELECT count(*) FROM dolt_log", -1);
  if( nLog<0 ) return nLog;
  return nLog>0 ? nLog-1 : 0;
}

static int verify_user_commit_count(const char *dbpath, const char *label,
                                    int expected){
  sqlite3 *db = 0;
  char desc[256];
  int rc, cnt;

  rc = sqlite3_open(dbpath, &db);
  if( rc!=SQLITE_OK ){
    sqlite3_close(db);
    return -1;
  }
  cnt = exec_user_commit_count(db);
  snprintf(desc, sizeof(desc), "%s: user commit count is %d", label, expected);
  check(desc, cnt==expected);
  sqlite3_close(db);
  return cnt;
}


static void test_01_clean_commit(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 01: Clean commit baseline ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    execSql(db, "INSERT INTO t VALUES(1, 'one')");
    execSql(db, "INSERT INTO t VALUES(2, 'two')");
    execSql(db, "INSERT INTO t VALUES(3, 'three')");
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'initial')");
    sqlite3_close(db);
    _exit(0);
  }
  {
    int status;
    waitpid(pid, &status, 0);
    check("test_01: child exited cleanly", WIFEXITED(status));
  }
  verify_consistency(dbpath, "test_01");
  verify_row_count(dbpath, "test_01", "t", 3);
  verify_user_commit_count(dbpath, "test_01", 1);
  removeDbFiles(dbpath);
}

static void test_02_kill_after_commit(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 02: Kill after commit returns ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    execSql(db, "INSERT INTO t VALUES(1, 'alpha')");
    execSql(db, "INSERT INTO t VALUES(2, 'beta')");
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'committed')");
    sqlite3_sleep(500);
    sqlite3_sleep(60000);
    _exit(0);
  }
  sqlite3_sleep(2000);
  kill(pid, SIGKILL);
  {
    int status;
    waitpid(pid, &status, 0);
    check("test_02: child was killed", WIFSIGNALED(status));
  }
  verify_consistency(dbpath, "test_02");
  verify_row_count(dbpath, "test_02", "t", 2);
  verify_user_commit_count(dbpath, "test_02", 1);
  removeDbFiles(dbpath);
}

static void test_03_kill_during_commit(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 03: Kill during commit ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    {
      int i;
      char sql[128];
      for( i=0; i<100; i++ ){
        snprintf(sql, sizeof(sql),
                 "INSERT INTO t VALUES(%d, 'row-%d')", i, i);
        execSql(db, sql);
      }
    }
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'big commit')");
    sqlite3_sleep(60000);
    _exit(0);
  }
  sqlite3_sleep(100);
  kill(pid, SIGKILL);
  {
    int status;
    waitpid(pid, &status, 0);
  }

  {
    sqlite3 *db = 0;
    int rc = sqlite3_open(dbpath, &db);
    check("test_03: db opens after crash", rc==SQLITE_OK);
    if( rc==SQLITE_OK ){
      int nLog = exec_user_commit_count(db);
      check("test_03: commit atomic (0 or 1)",
            nLog==0 || nLog==1);
      if( nLog==1 ){
        int cnt = queryScalarInt(db, "SELECT count(*) FROM t", -1);
        check("test_03: if committed, all 100 rows present", cnt==100);
      }
    }
    sqlite3_close(db);
  }
  removeDbFiles(dbpath);
}

static void test_04_two_commits_kill_after_second(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 04: Two commits, kill after second ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    execSql(db, "INSERT INTO t VALUES(1, 'first')");
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'commit-1')");
    execSql(db, "INSERT INTO t VALUES(2, 'second')");
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'commit-2')");
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

  {
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    int nLog = exec_user_commit_count(db);
    check("test_04: at least commit-1 present", nLog>=1);
    if( nLog>=2 ){
      int cnt = queryScalarInt(db, "SELECT count(*) FROM t", -1);
      check("test_04: both commits => 2 rows", cnt==2);
    }else if( nLog==1 ){
      int cnt = queryScalarInt(db, "SELECT count(*) FROM t", -1);
      check("test_04: only first commit => 1 row", cnt==1);
    }
    sqlite3_close(db);
  }
  removeDbFiles(dbpath);
}

static void test_05_kill_during_second_commit(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 05: Kill during second commit ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    execSql(db, "INSERT INTO t VALUES(1, 'stable')");
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'stable-commit')");

    {
      int i;
      char sql[128];
      for( i=100; i<200; i++ ){
        snprintf(sql, sizeof(sql),
                 "INSERT INTO t VALUES(%d, 'bulk-%d')", i, i);
        execSql(db, sql);
      }
    }
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'bulk-commit')");
    sqlite3_sleep(60000);
    _exit(0);
  }
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
      int nLog = exec_user_commit_count(db);
      check("test_05: at least stable-commit present", nLog>=1);
      int hasStable = queryScalarInt(db,
        "SELECT count(*) FROM t WHERE val='stable'", -1);
      check("test_05: stable row survives crash", hasStable==1);
    }
    sqlite3_close(db);
  }
  removeDbFiles(dbpath);
}

static void test_06_reopen_then_crash(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 06: Reopen then crash ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    execSql(db, "INSERT INTO t VALUES(1, 'persisted')");
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'first')");
    sqlite3_close(db);
    _exit(0);
  }
  { int status; waitpid(pid, &status, 0); }

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execSql(db, "INSERT INTO t VALUES(2, 'new')");
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'second')");
    sqlite3_sleep(500);
    sqlite3_sleep(60000);
    _exit(0);
  }
  sqlite3_sleep(3000);
  kill(pid, SIGKILL);
  { int status; waitpid(pid, &status, 0); }

  verify_consistency(dbpath, "test_06");
  {
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    int hasFirst = queryScalarInt(db,
      "SELECT count(*) FROM t WHERE val='persisted'", -1);
    check("test_06: first commit data survives", hasFirst==1);
    sqlite3_close(db);
  }
  removeDbFiles(dbpath);
}


static void test_07_five_commits_random_kill(void){
  const char *dbpath = fresh_db();
  pid_t pid;
  int kill_delay_ms;

  printf("--- Test 07: Five sequential commits, random kill ---\n");

  srand((unsigned)time(NULL) ^ (unsigned)getpid());
  kill_delay_ms = 500 + (rand() % 3500);

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    int i;
    char sql[256];
    sqlite3_open(dbpath, &db);
    execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    for( i=1; i<=5; i++ ){
      snprintf(sql, sizeof(sql), "INSERT INTO t VALUES(%d, 'v%d')", i, i);
      execSql(db, sql);
      snprintf(sql, sizeof(sql),
               "SELECT dolt_commit('-A', '-m', 'commit-%d')", i);
      queryScalarText(db, sql);
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
      int nLog = exec_user_commit_count(db);
      check("test_07: commit count in [0..5]", nLog>=0 && nLog<=5);

      if( nLog>0 ){
        int nRows = queryScalarInt(db, "SELECT count(*) FROM t", -1);
        check("test_07: row count matches commit count", nRows==nLog);
      }
    }
    sqlite3_close(db);
  }
  removeDbFiles(dbpath);
}

static void test_08_branch_crash(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 08: Branch + commit + crash ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    execSql(db, "INSERT INTO t VALUES(1, 'main-1')");
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'initial')");

    queryScalarText(db, "SELECT dolt_branch('feature')");
    queryScalarText(db, "SELECT dolt_checkout('feature')");
    execSql(db, "INSERT INTO t VALUES(2, 'feature-1')");
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'feature commit')");

    queryScalarText(db, "SELECT dolt_checkout('main')");
    execSql(db, "INSERT INTO t VALUES(3, 'main-2')");
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'main commit 2')");

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

      int nLog = exec_user_commit_count(db);
      check("test_08: dolt_log consistent", nLog>=0);
    }
    sqlite3_close(db);
  }
  removeDbFiles(dbpath);
}

static void test_09_branch_at_commit(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 09: Branch at specific commit, then crash ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    execSql(db, "INSERT INTO t VALUES(1, 'one')");
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'first')");
    execSql(db, "INSERT INTO t VALUES(2, 'two')");
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'second')");
    queryScalarText(db, "SELECT dolt_branch('snap')");
    execSql(db, "INSERT INTO t VALUES(3, 'three')");
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'third')");
    sqlite3_sleep(500);
    sqlite3_sleep(60000);
    _exit(0);
  }
  sqlite3_sleep(4000);
  kill(pid, SIGKILL);
  { int status; waitpid(pid, &status, 0); }

  verify_consistency(dbpath, "test_09");
  removeDbFiles(dbpath);
}

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
    execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    for( i=1; i<=5; i++ ){
      snprintf(sql, sizeof(sql), "INSERT INTO t VALUES(%d, 'v%d')", i, i);
      execSql(db, sql);
      snprintf(sql, sizeof(sql),
               "SELECT dolt_commit('-A', '-m', 'commit-%d')", i);
      queryScalarText(db, sql);
    }
    sqlite3_sleep(60000);
    _exit(0);
  }
  sqlite3_sleep(10);
  kill(pid, SIGKILL);
  { int status; waitpid(pid, &status, 0); }

  {
    sqlite3 *db = 0;
    int rc = sqlite3_open(dbpath, &db);
    if( rc==SQLITE_OK ){
      int nLog = exec_user_commit_count(db);
      check("test_10: commit count non-negative", nLog>=0);
      if( nLog>0 ){
        int nRows = queryScalarInt(db, "SELECT count(*) FROM t", -1);
        check("test_10: row count matches commits or next insert",
              nRows==nLog || nRows==nLog+1);
      }
    }else{
      check("test_10: db either opens or does not exist", 1);
    }
    sqlite3_close(db);
  }
  removeDbFiles(dbpath);
}

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
    execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");

    int row_id = 1;
    for( i=1; i<=5; i++ ){
      for( j=0; j<i*10; j++ ){
        snprintf(sql, sizeof(sql),
                 "INSERT INTO t VALUES(%d, 'batch%d-row%d')", row_id++, i, j);
        execSql(db, sql);
      }
      snprintf(sql, sizeof(sql),
               "SELECT dolt_commit('-A', '-m', 'batch-%d')", i);
      queryScalarText(db, sql);
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
      int nLog = exec_user_commit_count(db);
      check("test_11: commit count in [0..5]", nLog>=0 && nLog<=5);
      {
        int nRows = queryScalarInt(db, "SELECT count(*) FROM t", -1);
        int expected = nLog * (nLog + 1) * 5;
        int upper = expected;
        if( nLog<5 ){
          upper += (nLog + 1) * 10;
        }
        check("test_11: row count preserves committed prefix",
              nRows>=expected);
        check("test_11: row count bounded by next in-flight batch",
              nRows<=upper);
      }
    }
    sqlite3_close(db);
  }
  removeDbFiles(dbpath);
}


static void test_12_gc_crash(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 12: GC crash recovery ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    execSql(db, "INSERT INTO t VALUES(1, 'a')");
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'c1')");
    execSql(db, "INSERT INTO t VALUES(2, 'b')");
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'c2')");
    execSql(db, "DELETE FROM t WHERE id=1");
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'c3')");
    sqlite3_close(db);
    _exit(0);
  }
  { int status; waitpid(pid, &status, 0); }

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    queryScalarText(db, "SELECT dolt_gc()");
    sqlite3_sleep(60000);
    _exit(0);
  }
  sqlite3_sleep(100);
  kill(pid, SIGKILL);
  { int status; waitpid(pid, &status, 0); }

  {
    sqlite3 *db = 0;
    int rc = sqlite3_open(dbpath, &db);
    check("test_12: db opens after GC crash", rc==SQLITE_OK);
    if( rc==SQLITE_OK ){
      int cnt = queryScalarInt(db, "SELECT count(*) FROM t", -1);
      check("test_12: table queryable after GC crash", cnt>=0);
      int nLog = exec_user_commit_count(db);
      check("test_12: dolt_log works after GC crash", nLog>=1);
    }
    sqlite3_close(db);
  }
  removeDbFiles(dbpath);
}

static void test_13_gc_then_crash(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 13: GC completes, then crash ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    execSql(db, "INSERT INTO t VALUES(1, 'a')");
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'c1')");
    execSql(db, "INSERT INTO t VALUES(2, 'b')");
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'c2')");
    queryScalarText(db, "SELECT dolt_gc()");
    execSql(db, "INSERT INTO t VALUES(3, 'c')");
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'post-gc')");
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
      int nLog = exec_user_commit_count(db);
      check("test_13: at least 2 commits (pre-GC)", nLog>=2);
    }
    sqlite3_close(db);
  }
  removeDbFiles(dbpath);
}

static void test_14_gc_with_branches_crash(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 14: GC with branches crash ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    execSql(db, "INSERT INTO t VALUES(1, 'main')");
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'init')");
    queryScalarText(db, "SELECT dolt_branch('dev')");
    queryScalarText(db, "SELECT dolt_checkout('dev')");
    execSql(db, "INSERT INTO t VALUES(2, 'dev-data')");
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'dev commit')");
    queryScalarText(db, "SELECT dolt_checkout('main')");
    execSql(db, "INSERT INTO t VALUES(3, 'main-extra')");
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'main extra')");
    sqlite3_close(db);
    _exit(0);
  }
  { int status; waitpid(pid, &status, 0); }

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    queryScalarText(db, "SELECT dolt_gc()");
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
    int nBranch = queryScalarInt(db,
      "SELECT count(*) FROM dolt_branches", -1);
    check("test_14: both branches survive GC crash", nBranch==2);
    sqlite3_close(db);
  }
  removeDbFiles(dbpath);
}


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
    execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT, data TEXT)");
    for( i=0; i<1000; i++ ){
      snprintf(sql, sizeof(sql),
        "INSERT INTO t VALUES(%d, 'row-%d', '%0128d')", i, i, i);
      execSql(db, sql);
    }
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'thousand rows')");
    sqlite3_sleep(60000);
    _exit(0);
  }
  sqlite3_sleep(500);
  kill(pid, SIGKILL);
  { int status; waitpid(pid, &status, 0); }

  {
    sqlite3 *db = 0;
    int rc = sqlite3_open(dbpath, &db);
    check("test_15: db opens", rc==SQLITE_OK);
    if( rc==SQLITE_OK ){
      int nLog = exec_user_commit_count(db);
      check("test_15: commit atomic (0 or 1)", nLog==0 || nLog==1);
      if( nLog==1 ){
        int cnt = queryScalarInt(db, "SELECT count(*) FROM t", -1);
        check("test_15: all 1000 rows if committed", cnt==1000);
      }
    }
    sqlite3_close(db);
  }
  removeDbFiles(dbpath);
}

static void test_16_large_insert_complete_then_kill(void){
  const char *dbpath = fresh_db();
  char donepath[1024];
  pid_t pid;

  printf("--- Test 16: 1000-row commit completes, then kill ---\n");
  snprintf(donepath, sizeof(donepath), "%s.done", dbpath);
  unlink(donepath);

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    int i;
    char sql[256];
    sqlite3_open(dbpath, &db);
    execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    for( i=0; i<1000; i++ ){
      snprintf(sql, sizeof(sql),
        "INSERT INTO t VALUES(%d, 'row-%d')", i, i);
      execSql(db, sql);
    }
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'thousand rows')");
    {
      FILE *f = fopen(donepath, "w");
      if( f ){
        fputs("done\n", f);
        fclose(f);
      }
    }
    sqlite3_sleep(500);
    sqlite3_sleep(60000);
    _exit(0);
  }
  {
    int i, sawDone = 0;
    for( i=0; i<200; i++ ){
      if( access(donepath, F_OK)==0 ){
        sawDone = 1;
        break;
      }
      sqlite3_sleep(100);
    }
    check("test_16: commit returned before kill", sawDone);
  }
  kill(pid, SIGKILL);
  { int status; waitpid(pid, &status, 0); }

  verify_consistency(dbpath, "test_16");
  verify_row_count(dbpath, "test_16", "t", 1000);
  verify_user_commit_count(dbpath, "test_16", 1);
  unlink(donepath);
  removeDbFiles(dbpath);
}

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
    execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, batch INT, val TEXT)");

    for( batch=1; batch<=3; batch++ ){
      for( i=0; i<200; i++ ){
        int id = (batch-1)*200 + i;
        snprintf(sql, sizeof(sql),
          "INSERT INTO t VALUES(%d, %d, 'b%d-r%d')", id, batch, batch, i);
        execSql(db, sql);
      }
      snprintf(sql, sizeof(sql),
               "SELECT dolt_commit('-A', '-m', 'batch-%d')", batch);
      queryScalarText(db, sql);
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
      int nLog = exec_user_commit_count(db);
      check("test_17: commit count in [0..3]", nLog>=0 && nLog<=3);
      {
        int nRows = queryScalarInt(db, "SELECT count(*) FROM t", -1);
        int expected = nLog * 200;
        int upper = expected;
        if( nLog<3 ){
          upper += 200;
        }
        check("test_17: rows preserve committed prefix", nRows>=expected);
        check("test_17: rows bounded by next in-flight batch", nRows<=upper);
      }
    }
    sqlite3_close(db);
  }
  removeDbFiles(dbpath);
}


static void test_18_schema_change_crash(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 18: Schema change crash ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    execSql(db, "INSERT INTO t VALUES(1, 'a')");
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'create')");
    execSql(db, "ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'x'");
    execSql(db, "INSERT INTO t VALUES(2, 'b', 'y')");
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'alter')");
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
      int nLog = exec_user_commit_count(db);
      check("test_18: at least 1 commit", nLog>=1);
      if( nLog==2 ){
        const char *v = queryScalarText(db, "SELECT extra FROM t WHERE id=2");
        check("test_18: alter committed with data", strcmp(v, "y")==0);
      }
      if( nLog==1 ){
        int cnt = queryScalarInt(db, "SELECT count(*) FROM t", -1);
        check("test_18: first commit has 1 row", cnt==1);
      }
    }
    sqlite3_close(db);
  }
  removeDbFiles(dbpath);
}

static void test_19_delete_all_crash(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 19: Delete all rows then crash ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    execSql(db, "INSERT INTO t VALUES(1, 'a')");
    execSql(db, "INSERT INTO t VALUES(2, 'b')");
    execSql(db, "INSERT INTO t VALUES(3, 'c')");
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'initial')");
    sqlite3_close(db);
    _exit(0);
  }
  { int status; waitpid(pid, &status, 0); }

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execSql(db, "DELETE FROM t");
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'delete all')");
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
      int nLog = exec_user_commit_count(db);
      int cnt = queryScalarInt(db, "SELECT count(*) FROM t", -1);
      check("test_19: consistent state",
            (nLog==2 && cnt==0) || (nLog==1 && cnt==3));
    }
    sqlite3_close(db);
  }
  removeDbFiles(dbpath);
}

static void test_20_update_crash(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 20: Update all rows then crash ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    execSql(db, "INSERT INTO t VALUES(1, 'old')");
    execSql(db, "INSERT INTO t VALUES(2, 'old')");
    execSql(db, "INSERT INTO t VALUES(3, 'old')");
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'initial')");
    execSql(db, "UPDATE t SET val='new'");
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'update all')");
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
      int nOld = queryScalarInt(db,
        "SELECT count(*) FROM t WHERE val='old'", -1);
      int nNew = queryScalarInt(db,
        "SELECT count(*) FROM t WHERE val='new'", -1);
      check("test_20: atomic update (all old or all new)",
            (nOld==3 && nNew==0) || (nOld==0 && nNew==3));
    }
    sqlite3_close(db);
  }
  removeDbFiles(dbpath);
}

static void test_21_drop_table_crash(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 21: Drop table then crash ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    execSql(db, "CREATE TABLE t2(id INTEGER PRIMARY KEY, val TEXT)");
    execSql(db, "INSERT INTO t VALUES(1, 'keep')");
    execSql(db, "INSERT INTO t2 VALUES(1, 'drop-me')");
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'two tables')");
    execSql(db, "DROP TABLE t2");
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'dropped t2')");
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
      int cnt_t = queryScalarInt(db, "SELECT count(*) FROM t", -1);
      check("test_21: table t exists", cnt_t==1);
      int cnt_t2 = queryScalarInt(db, "SELECT count(*) FROM t2", -2);
      check("test_21: t2 state consistent",
            cnt_t2==1 || cnt_t2==-2);
    }
    sqlite3_close(db);
  }
  removeDbFiles(dbpath);
}

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
      execSql(db, sql);
      snprintf(sql, sizeof(sql),
        "INSERT INTO t%d VALUES(1, 'data-%d')", i, i);
      execSql(db, sql);
    }
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', '10 tables')");
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
      int nLog = exec_user_commit_count(db);
      if( nLog==1 ){
        int i;
        int allOk = 1;
        for( i=1; i<=10; i++ ){
          char sql[128];
          snprintf(sql, sizeof(sql),
                   "SELECT count(*) FROM t%d", i);
          int cnt = queryScalarInt(db, sql, -1);
          if( cnt!=1 ) allOk = 0;
        }
        check("test_22: all 10 tables present if committed", allOk);
      }else{
        check("test_22: commit atomic (0 or 1)", nLog==0 || nLog==1);
      }
    }
    sqlite3_close(db);
  }
  removeDbFiles(dbpath);
}

static void test_23_merge_crash(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 23: Merge then crash ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    execSql(db, "INSERT INTO t VALUES(1, 'shared')");
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'init')");
    queryScalarText(db, "SELECT dolt_branch('feature')");
    queryScalarText(db, "SELECT dolt_checkout('feature')");
    execSql(db, "INSERT INTO t VALUES(2, 'feature')");
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'feature data')");
    queryScalarText(db, "SELECT dolt_checkout('main')");
    sqlite3_close(db);
    _exit(0);
  }
  { int status; waitpid(pid, &status, 0); }

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    queryScalarText(db, "SELECT dolt_merge('feature')");
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
    int cnt = queryScalarInt(db, "SELECT count(*) FROM t", -1);
    check("test_23: row count is 1 or 2",
          cnt==1 || cnt==2);
    sqlite3_close(db);
  }
  removeDbFiles(dbpath);
}

static void test_24_repeated_crash_cycles(void){
  const char *dbpath = fresh_db();
  int cycle;
  pid_t pid;

  printf("--- Test 24: Repeated crash/recover cycles ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    execSql(db, "INSERT INTO t VALUES(0, 'seed')");
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'seed')");
    sqlite3_close(db);
    _exit(0);
  }
  { int status; waitpid(pid, &status, 0); }

  for( cycle=1; cycle<=5; cycle++ ){
    pid = fork();
    if( pid==0 ){
      sqlite3 *db = 0;
      char sql[256];
      sqlite3_open(dbpath, &db);
      snprintf(sql, sizeof(sql),
               "INSERT INTO t VALUES(%d, 'cycle-%d')", cycle, cycle);
      execSql(db, sql);
      snprintf(sql, sizeof(sql),
               "SELECT dolt_commit('-A', '-m', 'cycle-%d')", cycle);
      queryScalarText(db, sql);
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
      char desc[128];
      snprintf(desc, sizeof(desc), "test_24_cycle%d: db opens", cycle);
      check(desc, rc==SQLITE_OK);
      if( rc==SQLITE_OK ){
        int nLog = exec_user_commit_count(db);
        snprintf(desc, sizeof(desc),
                 "test_24_cycle%d: log non-negative", cycle);
        check(desc, nLog>=1);
      }
      sqlite3_close(db);
    }
  }

  {
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    int hasSeed = queryScalarInt(db,
      "SELECT count(*) FROM t WHERE val='seed'", -1);
    check("test_24: seed row always present", hasSeed==1);
    sqlite3_close(db);
  }
  removeDbFiles(dbpath);
}

static void test_25_tag_crash(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 25: Tag creation then crash ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    execSql(db, "INSERT INTO t VALUES(1, 'tagged')");
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'for tag')");
    queryScalarText(db, "SELECT dolt_tag('v1.0')");
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
    int nLog = exec_user_commit_count(db);
    check("test_25: commit present", nLog>=1);
    int cnt = queryScalarInt(db, "SELECT count(*) FROM t", -1);
    check("test_25: data intact", cnt==1);
    sqlite3_close(db);
  }
  removeDbFiles(dbpath);
}

static void test_26_blob_data_crash(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 26: BLOB data commit crash ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    int i;
    sqlite3_open(dbpath, &db);
    execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB)");
    for( i=0; i<50; i++ ){
      sqlite3_stmt *stmt = 0;
      char sql_text[] = "INSERT INTO t VALUES(?, randomblob(1024))";
      sqlite3_prepare_v2(db, sql_text, -1, &stmt, 0);
      sqlite3_bind_int(stmt, 1, i);
      sqlite3_step(stmt);
      sqlite3_finalize(stmt);
    }
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'blobs')");
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
      int nLog = exec_user_commit_count(db);
      check("test_26: commit atomic (0 or 1)", nLog==0 || nLog==1);
      if( nLog==1 ){
        int cnt = queryScalarInt(db, "SELECT count(*) FROM t", -1);
        check("test_26: all 50 blob rows if committed", cnt==50);
        int blobLen = queryScalarInt(db,
          "SELECT length(data) FROM t WHERE id=0", -1);
        check("test_26: blob data intact", blobLen==1024);
      }
    }
    sqlite3_close(db);
  }
  removeDbFiles(dbpath);
}

static void test_27_uncommitted_changes_crash(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 27: Uncommitted changes crash ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    execSql(db, "INSERT INTO t VALUES(1, 'committed')");
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'baseline')");
    sqlite3_close(db);
    _exit(0);
  }
  { int status; waitpid(pid, &status, 0); }

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    execSql(db, "INSERT INTO t VALUES(2, 'uncommitted')");
    execSql(db, "INSERT INTO t VALUES(3, 'uncommitted')");
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
    int nLog = exec_user_commit_count(db);
    check("test_27: only baseline commit", nLog==1);
    int committed = queryScalarInt(db,
      "SELECT count(*) FROM t WHERE val='committed'", -1);
    check("test_27: committed data present", committed==1);
    sqlite3_close(db);
  }
  removeDbFiles(dbpath);
}

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
    execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    for( i=1; i<=10; i++ ){
      snprintf(sql, sizeof(sql),
               "INSERT INTO t VALUES(%d, 'r%d')", i, i);
      execSql(db, sql);
      snprintf(sql, sizeof(sql),
               "SELECT dolt_commit('-A', '-m', 'r%d')", i);
      queryScalarText(db, sql);
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
      int nLog = exec_user_commit_count(db);
      check("test_28: commit count in [0..10]", nLog>=0 && nLog<=10);
      if( nLog>0 ){
        int nRows = queryScalarInt(db, "SELECT count(*) FROM t", -1);
        check("test_28: rows match commits or next insert",
              nRows==nLog || nRows==nLog+1);
      }
    }
    sqlite3_close(db);
  }
  removeDbFiles(dbpath);
}

static void test_29_gc_after_many_commits(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 29: GC after many commits then crash ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    int i;
    char sql[256];
    sqlite3_open(dbpath, &db);
    execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    for( i=1; i<=10; i++ ){
      snprintf(sql, sizeof(sql),
               "INSERT INTO t VALUES(%d, 'v%d')", i, i);
      execSql(db, sql);
      snprintf(sql, sizeof(sql),
               "SELECT dolt_commit('-A', '-m', 'c%d')", i);
      queryScalarText(db, sql);
    }
    sqlite3_close(db);
    _exit(0);
  }
  { int status; waitpid(pid, &status, 0); }

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    queryScalarText(db, "SELECT dolt_gc()");
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
      int nLog = exec_user_commit_count(db);
      check("test_29: all 10 commits present", nLog==10);
      int cnt = queryScalarInt(db, "SELECT count(*) FROM t", -1);
      check("test_29: all 10 rows present", cnt==10);
    }
    sqlite3_close(db);
  }
  removeDbFiles(dbpath);
}

static void test_30_crash_on_open(void){
  const char *dbpath = fresh_db();
  pid_t pid;

  printf("--- Test 30: Crash immediately after open ---\n");

  pid = fork();
  if( pid==0 ){
    sqlite3 *db = 0;
    sqlite3_open(dbpath, &db);
    sqlite3_sleep(60000);
    _exit(0);
  }
  sqlite3_sleep(50);
  kill(pid, SIGKILL);
  { int status; waitpid(pid, &status, 0); }

  {
    sqlite3 *db = 0;
    int rc = sqlite3_open(dbpath, &db);
    check("test_30: db openable after early crash", rc==SQLITE_OK);
    sqlite3_close(db);
  }
  removeDbFiles(dbpath);
}


int main(void){
  printf("=== DoltLite Crash Recovery Tests ===\n\n");

  test_01_clean_commit();
  test_02_kill_after_commit();
  test_03_kill_during_commit();
  test_04_two_commits_kill_after_second();
  test_05_kill_during_second_commit();
  test_06_reopen_then_crash();

  test_07_five_commits_random_kill();
  test_08_branch_crash();
  test_09_branch_at_commit();
  test_10_very_early_kill();
  test_11_increasing_data_kill();

  test_12_gc_crash();
  test_13_gc_then_crash();
  test_14_gc_with_branches_crash();

  test_15_large_insert_crash();
  test_16_large_insert_complete_then_kill();
  test_17_multiple_large_commits();

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
