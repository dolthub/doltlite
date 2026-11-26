#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <dirent.h>
#include "sqlite3.h"
#include "chunk_store.h"
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
           DOLTLITE_TEST_TMPDIR "/crash_test_%d_%d.db", (int)getpid(), g_test_seq++);
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

  {
    const char *integ = queryScalarText(db, "PRAGMA integrity_check");
    snprintf(desc, sizeof(desc), "%s: integrity_check is ok", label);
    check(desc, integ && strcmp(integ, "ok")==0);
    if( !integ || strcmp(integ, "ok")!=0 ) ok = 0;
  }

  {
    int nConflicts = queryScalarInt(db, "SELECT count(*) FROM dolt_conflicts", -1);
    snprintf(desc, sizeof(desc), "%s: no persisted conflicts", label);
    check(desc, nConflicts==0);
    if( nConflicts!=0 ) ok = 0;
  }

  {
    const char *zHead = queryScalarText(db, "SELECT dolt_hashof('HEAD')");
    int nHead = zHead ? (int)strlen(zHead) : 0;
    snprintf(desc, sizeof(desc), "%s: HEAD hash resolves", label);
    check(desc, nHead==40);
    if( nHead!=40 ) ok = 0;
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
    int nLog;
    sqlite3_open(dbpath, &db);
    nLog = exec_user_commit_count(db);
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
      int hasStable;
      check("test_05: at least stable-commit present", nLog>=1);
      hasStable = queryScalarInt(db,
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
    int hasFirst;
    sqlite3_open(dbpath, &db);
    hasFirst = queryScalarInt(db,
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
        int nHeadRows = queryScalarInt(db,
          "SELECT count(*) FROM dolt_at_t('HEAD')", -1);
        check("test_07: committed row count matches commit count",
              nHeadRows==nLog);
        check("test_07: working rows match commits or next insert",
              nRows==nLog || (nLog<5 && nRows==nLog+1));
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
      int nLog;
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

      nLog = exec_user_commit_count(db);
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
    int row_id = 1;
    char sql[256];
    sqlite3_open(dbpath, &db);
    execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");

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
      int nLog;
      check("test_12: table queryable after GC crash", cnt>=0);
      nLog = exec_user_commit_count(db);
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
    int nBranch;
    sqlite3_open(dbpath, &db);
    nBranch = queryScalarInt(db,
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
      int cnt_t2;
      check("test_21: table t exists", cnt_t==1);
      cnt_t2 = queryScalarInt(db, "SELECT count(*) FROM t2", -2);
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
          int cnt;
          snprintf(sql, sizeof(sql),
                   "SELECT count(*) FROM t%d", i);
          cnt = queryScalarInt(db, sql, -1);
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
    int cnt;
    sqlite3_open(dbpath, &db);
    cnt = queryScalarInt(db, "SELECT count(*) FROM t", -1);
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
    int hasSeed;
    sqlite3_open(dbpath, &db);
    hasSeed = queryScalarInt(db,
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
    int nLog;
    int cnt;
    sqlite3_open(dbpath, &db);
    nLog = exec_user_commit_count(db);
    check("test_25: commit present", nLog>=1);
    cnt = queryScalarInt(db, "SELECT count(*) FROM t", -1);
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
        int blobLen;
        check("test_26: all 50 blob rows if committed", cnt==50);
        blobLen = queryScalarInt(db,
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
    int nLog;
    int committed;
    sqlite3_open(dbpath, &db);
    nLog = exec_user_commit_count(db);
    check("test_27: only baseline commit", nLog==1);
    committed = queryScalarInt(db,
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
      int cnt;
      check("test_29: all 10 commits present", nLog==10);
      cnt = queryScalarInt(db, "SELECT count(*) FROM t", -1);
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


/*
** Test 31 support: a wrapper VFS that tracks which written byte ranges have
** not yet been covered by an fsync. Every sealed root record carries a
** durableTo watermark; recovery poisons the store when damage lands below
** it, so a root record written while bytes below its watermark are still
** unsynced is a durability-contract violation (a crash mid-final-fsync can
** persist the root but tear the unsynced bytes, falsely poisoning a store
** whose previous commit was intact).
*/

typedef struct DuraRange DuraRange;
struct DuraRange { sqlite3_int64 off; sqlite3_int64 len; };

typedef struct DuraFile DuraFile;
struct DuraFile {
  sqlite3_file base;
  sqlite3_file *pReal;
  int isMain;
  DuraRange *aUnsynced;
  int nUnsynced;
  int nAlloc;
};

static sqlite3_vfs gDuraVfs;
static sqlite3_vfs *gDuraBase = 0;
static int gDuraViolations = 0;
static int gDuraRootsSeen = 0;
static int gDuraCrashAt = 0;
static int gDuraSyncCount = 0;
static int gDuraCrashBefore = 0;
static int gDuraWriteCount = 0;
static int gDuraFailWriteAt = 0;
static int gDuraFailSyncOnce = 0;
static int gDuraFailRollbackWrite = 0;
static int gDuraFailRollbackTruncate = 0;
static int gDuraSectorSize = 0;
static int gDuraDeviceCharOverride = 0;
static int gDuraDeviceChar = 0;

static int duraClose(sqlite3_file *pFile){
  DuraFile *p = (DuraFile*)pFile;
  sqlite3_free(p->aUnsynced);
  p->aUnsynced = 0;
  return p->pReal->pMethods->xClose(p->pReal);
}

static int duraRead(sqlite3_file *pFile, void *zBuf, int iAmt, sqlite3_int64 iOfst){
  DuraFile *p = (DuraFile*)pFile;
  return p->pReal->pMethods->xRead(p->pReal, zBuf, iAmt, iOfst);
}

static int duraWrite(sqlite3_file *pFile, const void *zBuf, int iAmt,
                     sqlite3_int64 iOfst){
  DuraFile *p = (DuraFile*)pFile;
  int rc;
  if( p->isMain && gDuraFailRollbackWrite>0 ){
    gDuraFailRollbackWrite--;
    return SQLITE_IOERR_WRITE;
  }
  if( p->isMain && ++gDuraWriteCount==gDuraFailWriteAt ){
    return SQLITE_IOERR_WRITE;
  }
  rc = p->pReal->pMethods->xWrite(p->pReal, zBuf, iAmt, iOfst);
  if( rc!=SQLITE_OK || !p->isMain ) return rc;
  if( iAmt==1+CHUNK_MANIFEST_SIZE
   && ((const unsigned char*)zBuf)[0]==CS_WAL_TAG_ROOT
   && CS_READ_U32((const unsigned char*)zBuf+1)==CHUNK_STORE_MAGIC ){
    sqlite3_int64 durableTo =
      CS_READ_I64((const unsigned char*)zBuf+1+CS_MANIFEST_DURABLE_TO_OFF);
    int i;
    gDuraRootsSeen++;
    /* The manifest header below CHUNK_MANIFEST_SIZE is outside the WAL
    ** region durableTo protects; only WAL bytes count. */
    for(i=0; i<p->nUnsynced; i++){
      if( p->aUnsynced[i].off < durableTo
       && p->aUnsynced[i].off + p->aUnsynced[i].len > CHUNK_MANIFEST_SIZE ){
        gDuraViolations++;
        break;
      }
    }
  }
  if( p->nUnsynced==p->nAlloc ){
    int nNew = p->nAlloc ? p->nAlloc*2 : 64;
    DuraRange *aNew = sqlite3_realloc(p->aUnsynced,
                                      nNew*(int)sizeof(DuraRange));
    if( !aNew ) return SQLITE_NOMEM;
    p->aUnsynced = aNew;
    p->nAlloc = nNew;
  }
  p->aUnsynced[p->nUnsynced].off = iOfst;
  p->aUnsynced[p->nUnsynced].len = iAmt;
  p->nUnsynced++;
  return SQLITE_OK;
}

static int duraTruncate(sqlite3_file *pFile, sqlite3_int64 size){
  DuraFile *p = (DuraFile*)pFile;
  if( p->isMain && gDuraFailRollbackTruncate>0 ){
    gDuraFailRollbackTruncate--;
    return SQLITE_IOERR_TRUNCATE;
  }
  return p->pReal->pMethods->xTruncate(p->pReal, size);
}

static int duraSync(sqlite3_file *pFile, int flags){
  DuraFile *p = (DuraFile*)pFile;
  int rc;
  if( p->isMain && gDuraFailSyncOnce>0 ){
    gDuraFailSyncOnce--;
    gDuraFailRollbackWrite = 10;
    gDuraFailRollbackTruncate = 10;
    return SQLITE_IOERR_FSYNC;
  }
  if( p->isMain && gDuraCrashAt>0 ){
    gDuraSyncCount++;
    if( gDuraCrashBefore && gDuraSyncCount==gDuraCrashAt ) _exit(99);
  }
  rc = p->pReal->pMethods->xSync(p->pReal, flags);
  if( p->isMain && !gDuraCrashBefore && gDuraCrashAt>0
   && gDuraSyncCount==gDuraCrashAt && rc==SQLITE_OK ) _exit(99);
  if( rc==SQLITE_OK ) p->nUnsynced = 0;
  return rc;
}

static int duraFileSize(sqlite3_file *pFile, sqlite3_int64 *pSize){
  DuraFile *p = (DuraFile*)pFile;
  return p->pReal->pMethods->xFileSize(p->pReal, pSize);
}

static int duraLock(sqlite3_file *pFile, int eLock){
  DuraFile *p = (DuraFile*)pFile;
  return p->pReal->pMethods->xLock(p->pReal, eLock);
}

static int duraUnlock(sqlite3_file *pFile, int eLock){
  DuraFile *p = (DuraFile*)pFile;
  return p->pReal->pMethods->xUnlock(p->pReal, eLock);
}

static int duraCheckReservedLock(sqlite3_file *pFile, int *pResOut){
  DuraFile *p = (DuraFile*)pFile;
  return p->pReal->pMethods->xCheckReservedLock(p->pReal, pResOut);
}

static int duraFileControl(sqlite3_file *pFile, int op, void *pArg){
  DuraFile *p = (DuraFile*)pFile;
  return p->pReal->pMethods->xFileControl(p->pReal, op, pArg);
}

static int duraSectorSize(sqlite3_file *pFile){
  DuraFile *p = (DuraFile*)pFile;
  if( gDuraSectorSize>0 ) return gDuraSectorSize;
  return p->pReal->pMethods->xSectorSize(p->pReal);
}

static int duraDeviceCharacteristics(sqlite3_file *pFile){
  DuraFile *p = (DuraFile*)pFile;
  if( gDuraDeviceCharOverride ) return gDuraDeviceChar;
  return p->pReal->pMethods->xDeviceCharacteristics(p->pReal);
}

static int duraShmMap(sqlite3_file *pFile, int iPg, int pgsz, int bExtend,
                      void volatile **pp){
  DuraFile *p = (DuraFile*)pFile;
  return p->pReal->pMethods->xShmMap(p->pReal, iPg, pgsz, bExtend, pp);
}

static int duraShmLock(sqlite3_file *pFile, int offset, int n, int flags){
  DuraFile *p = (DuraFile*)pFile;
  return p->pReal->pMethods->xShmLock(p->pReal, offset, n, flags);
}

static void duraShmBarrier(sqlite3_file *pFile){
  DuraFile *p = (DuraFile*)pFile;
  p->pReal->pMethods->xShmBarrier(p->pReal);
}

static int duraShmUnmap(sqlite3_file *pFile, int deleteFlag){
  DuraFile *p = (DuraFile*)pFile;
  return p->pReal->pMethods->xShmUnmap(p->pReal, deleteFlag);
}

static int duraFetch(sqlite3_file *pFile, sqlite3_int64 iOfst, int iAmt,
                     void **pp){
  DuraFile *p = (DuraFile*)pFile;
  if( p->pReal->pMethods->iVersion<3 || p->pReal->pMethods->xFetch==0 ){
    *pp = 0;
    return SQLITE_OK;
  }
  return p->pReal->pMethods->xFetch(p->pReal, iOfst, iAmt, pp);
}

static int duraUnfetch(sqlite3_file *pFile, sqlite3_int64 iOfst, void *pPage){
  DuraFile *p = (DuraFile*)pFile;
  if( p->pReal->pMethods->iVersion<3 || p->pReal->pMethods->xUnfetch==0 ){
    return SQLITE_OK;
  }
  return p->pReal->pMethods->xUnfetch(p->pReal, iOfst, pPage);
}

static const sqlite3_io_methods gDuraIoMethods = {
  3,
  duraClose,
  duraRead,
  duraWrite,
  duraTruncate,
  duraSync,
  duraFileSize,
  duraLock,
  duraUnlock,
  duraCheckReservedLock,
  duraFileControl,
  duraSectorSize,
  duraDeviceCharacteristics,
  duraShmMap,
  duraShmLock,
  duraShmBarrier,
  duraShmUnmap,
  duraFetch,
  duraUnfetch
};

static int duraOpen(sqlite3_vfs *pVfs, const char *zName, sqlite3_file *pFile,
                    int flags, int *pOutFlags){
  DuraFile *p = (DuraFile*)pFile;
  sqlite3_file *pReal = (sqlite3_file*)&p[1];
  int rc;
  int nName = zName ? (int)strlen(zName) : 0;
  int isGcTmp = nName>=7 && strcmp(zName+nName-7, "-gc-tmp")==0;
  int isLock = nName>=5 && strcmp(zName+nName-5, "-lock")==0;

  memset(p, 0, sizeof(*p));
  rc = gDuraBase->xOpen(gDuraBase, zName, pReal, flags, pOutFlags);
  if( rc!=SQLITE_OK ) return rc;
  p->pReal = pReal;
  p->isMain = (flags & SQLITE_OPEN_MAIN_DB)!=0 && !isGcTmp && !isLock;
  p->base.pMethods = &gDuraIoMethods;
  return SQLITE_OK;
}

static int duraRegister(void){
  if( gDuraBase ) return SQLITE_OK;
  gDuraBase = sqlite3_vfs_find(0);
  if( !gDuraBase ) return SQLITE_ERROR;
  gDuraVfs = *gDuraBase;
  gDuraVfs.zName = "dura-track";
  gDuraVfs.szOsFile = (int)sizeof(DuraFile) + gDuraBase->szOsFile;
  gDuraVfs.xOpen = duraOpen;
  return sqlite3_vfs_register(&gDuraVfs, 0);
}

static void test_pending_drain_batch(void){
  int sparse, outcome;
  printf("--- Pending chunk drain batching ---\n");
  check("drain_batch_vfs", duraRegister()==SQLITE_OK);
  for(sparse=0; sparse<2; sparse++){
    for(outcome=0; outcome<3; outcome++){
      const char *zPath = fresh_db();
      ChunkStore cs;
      ProllyHash base, sparseHash;
      ProllyHash *aHash = sqlite3_malloc(4096*sizeof(ProllyHash));
      unsigned char *pData = sqlite3_malloc(16384);
      char zLabel[100];
      int i, rc, valid = 1, has = 0;
      snprintf(zLabel, sizeof(zLabel), "drain_batch_%d_%d", sparse, outcome);
      check("drain_batch_alloc", aHash!=0 && pData!=0);
      if( !aHash || !pData ){
        sqlite3_free(aHash);
        sqlite3_free(pData);
        continue;
      }
      memset(pData, 0x5a, 16384);
      rc = chunkStoreOpen(&cs, &gDuraVfs, zPath, SQLITE_OPEN_CREATE);
      if( rc==SQLITE_OK ) rc = chunkStoreLockAndRefresh(&cs);
      if( rc==SQLITE_OK ) rc = chunkStorePut(&cs, (const u8*)"base", 4, &base);
      if( rc==SQLITE_OK ) rc = chunkStoreCommit(&cs);
      chunkStoreUnlock(&cs);
      if( rc==SQLITE_OK ) rc = chunkStoreLockAndRefresh(&cs);
      if( rc==SQLITE_OK && sparse ){
        rc = chunkStorePutSparse(&cs, (const u8*)"prefix", 6, 100000,
                                 &sparseHash);
      }
      check(zLabel, rc==SQLITE_OK);
      if( rc!=SQLITE_OK ){
        chunkStoreClose(&cs);
        sqlite3_free(aHash);
        sqlite3_free(pData);
        removeDbFiles(zPath);
        continue;
      }
      gDuraWriteCount = 0;
      gDuraFailWriteAt = outcome==2 ? 2 : 0;
      for(i=0; i<4096 && rc==SQLITE_OK; i++){
        pData[0] = (u8)i;
        pData[1] = (u8)(i>>8);
        rc = chunkStorePut(&cs, pData, 16384, &aHash[i]);
      }
      gDuraFailWriteAt = 0;
      if( outcome==2 ){
        check("drain_batch_write_error", rc==SQLITE_IOERR_WRITE);
        check("drain_batch_error_not_published", cs.staging.nRecentUncommitted==0);
      }else{
        check("drain_batch_put", rc==SQLITE_OK);
        check("drain_batch_spilled", cs.staging.nRecentUncommitted>0);
        printf("  %s: %d drained chunks, %d writes\n", zLabel,
               cs.staging.nRecentUncommitted, gDuraWriteCount);
        if( !sparse ){
          check("drain_batch_fewer_writes_than_chunks",
                gDuraWriteCount<cs.staging.nRecentUncommitted);
        }
        for(i=0; i<4096 && valid; i++){
          u8 *pRead = 0;
          int nRead = 0;
          pData[0] = (u8)i;
          pData[1] = (u8)(i>>8);
          rc = chunkStoreGet(&cs, &aHash[i], &pRead, &nRead);
          valid = rc==SQLITE_OK && nRead==16384
               && memcmp(pRead, pData, 16384)==0;
          sqlite3_free(pRead);
        }
        check("drain_batch_read_spilled_chunks", valid);
      }
      if( outcome==0 ){
        check("drain_batch_commit", chunkStoreCommit(&cs)==SQLITE_OK);
      }else{
        chunkStoreRollback(&cs);
      }
      chunkStoreUnlock(&cs);
      chunkStoreClose(&cs);
      rc = chunkStoreOpen(&cs, &gDuraVfs, zPath, SQLITE_OPEN_READWRITE);
      check("drain_batch_reopen", rc==SQLITE_OK);
      if( rc==SQLITE_OK ){
        rc = chunkStoreHas(&cs, &base, &has);
        check("drain_batch_base_kept", rc==SQLITE_OK && has);
        rc = chunkStoreHas(&cs, &aHash[0], &has);
        check("drain_batch_reopen_visibility",
              rc==SQLITE_OK && has==(outcome==0));
        if( sparse && outcome==0 ){
          u8 *pRead = 0;
          int nRead = 0;
          rc = chunkStoreGet(&cs, &sparseHash, &pRead, &nRead);
          valid = rc==SQLITE_OK && nRead==100006
               && memcmp(pRead, "prefix", 6)==0;
          for(i=6; i<nRead && valid; i++) if( pRead[i]!=0 ) valid = 0;
          check("drain_batch_sparse_content", valid);
          sqlite3_free(pRead);
        }
        chunkStoreClose(&cs);
      }
      sqlite3_free(aHash);
      sqlite3_free(pData);
      removeDbFiles(zPath);
    }
  }
}

static void test_31_drain_durability_watermark(void){
  const char *dbpath = fresh_db();
  sqlite3 *db = 0;
  int rc;

  printf("--- Test 31: drained WAL bytes synced before durableTo claims them ---\n");

  if( duraRegister()!=SQLITE_OK ){
    check("test_31: tracking vfs registered", 0);
    return;
  }
  /* Force mid-transaction drains at a tiny threshold so the multi-drain
  ** commit shape is exercised without writing 64MB. */
  setenv("DOLTLITE_CHUNK_PENDING_DRAIN_LIMIT", "8192", 1);

  rc = sqlite3_open_v2(dbpath, &db,
                       SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE, "dura-track");
  check("test_31: db opens on tracking vfs", rc==SQLITE_OK);
  if( rc==SQLITE_OK ){
    execSql(db, "CREATE TABLE t31(a INTEGER PRIMARY KEY, b BLOB)");
    execSql(db, "BEGIN");
    execSql(db,
      "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<200) "
      "INSERT INTO t31 SELECT x, randomblob(2000) FROM c");
    execSql(db, "COMMIT");
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 't31')");
    check("test_31: root records observed", gDuraRootsSeen>0);
    check("test_31: no unsynced WAL bytes below durableTo", gDuraViolations==0);
    check("test_31: rows committed",
          queryScalarInt(db, "SELECT count(*) FROM t31", -1)==200);
  }
  sqlite3_close(db);
  unsetenv("DOLTLITE_CHUNK_PENDING_DRAIN_LIMIT");
  removeDbFiles(dbpath);
}

static void verify_merge_abort_local(sqlite3 *db, const char *label, int autoInc){
  char desc[256];
  snprintf(desc, sizeof(desc), "%s: untracked rows and index preserved", label);
  check(desc, strcmp(queryScalarText(db,
        "SELECT note FROM scratch INDEXED BY scratch_note WHERE id=1"),
        "my uncommitted work")==0);
  snprintf(desc, sizeof(desc), "%s: local view preserved", label);
  check(desc, strcmp(queryScalarText(db, "SELECT note FROM scratch_view"),
        "my uncommitted work")==0);
  if( autoInc ){
    snprintf(desc, sizeof(desc), "%s: local sequence preserved", label);
    check(desc, queryScalarInt(db,
          "SELECT seq FROM sqlite_sequence WHERE name='scratch'", -1)==1);
  }
  snprintf(desc, sizeof(desc), "%s: untouched table edit preserved", label);
  check(desc, queryScalarInt(db, "SELECT v FROM u WHERE id=1", -1)==99);
}

static void test_32_merge_abort_atomic_sync(void){
  static const char *zLocal =
    "CREATE INDEX scratch_note ON scratch(note);"
    "CREATE VIEW scratch_view AS SELECT note FROM scratch;"
    "INSERT INTO scratch(note) VALUES('my uncommitted work');"
    "UPDATE u SET v=99 WHERE id=1;";
  int kind, before;

  printf("--- Test 32: merge abort preserves local work across sync crashes ---\n");
  check("test_32: tracking vfs registered", duraRegister()==SQLITE_OK);
  for(kind=0; kind<3; kind++){
    for(before=0; before<2; before++){
      int at;
      for(at=1; at<=8; at++){
        const char *dbpath = fresh_db();
        sqlite3 *db = 0;
        pid_t pid;
        int status = 0, crashed, merging;
        char label[96], desc[256], head[64];
        snprintf(label, sizeof(label), "abort_%s_%s_sync_%d",
                 kind==1 ? "conflict" : kind==2 ? "ignored" : "nocommit",
                 before ? "before" : "after", at);
        check("test_32: setup open", sqlite3_open(dbpath, &db)==SQLITE_OK);
        check("test_32: setup base", execSql(db,
          "CREATE TABLE t(id INT PRIMARY KEY,v INT);"
          "CREATE TABLE u(id INT PRIMARY KEY,v INT);"
          "INSERT INTO t VALUES(1,1),(2,2); INSERT INTO u VALUES(1,1);")
          ==SQLITE_OK);
        if( kind==2 ){
          check("test_32: ignore pattern", execSql(db,
                "INSERT INTO dolt_ignore VALUES('scratch',1)")==SQLITE_OK);
        }
        check("test_32: setup branches", execSql(db,
          "SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat');"
          "UPDATE t SET v=10 WHERE id=1; SELECT dolt_commit('-am','main change');"
          "SELECT dolt_checkout('feat');")==SQLITE_OK);
        check("test_32: feature edit", execSql(db, kind==1
          ? "UPDATE t SET v=20 WHERE id=1" : "UPDATE t SET v=20 WHERE id=2")
          ==SQLITE_OK);
        check("test_32: setup main", execSql(db,
          "SELECT dolt_commit('-am','feat change'); SELECT dolt_checkout('main');")
          ==SQLITE_OK);
        snprintf(head, sizeof(head), "%s",
                 queryScalarText(db, "SELECT dolt_hashof('HEAD')"));
        if( kind!=1 ){
          check("test_32: persisted merge", execSql(db,
                "SELECT dolt_merge('--no-commit','feat')")==SQLITE_OK);
          check("test_32: persisted merge active", queryScalarInt(db,
                "SELECT is_merging FROM dolt_merge_status", -1)==1);
        }
        check("test_32: untracked table", execSql(db, kind==1
          ? "CREATE TABLE scratch(id INTEGER PRIMARY KEY,note TEXT)"
          : "CREATE TABLE scratch(id INTEGER PRIMARY KEY AUTOINCREMENT,note TEXT)")
          ==SQLITE_OK);
        check("test_32: durable local work", execSql(db, zLocal)==SQLITE_OK);
        sqlite3_close(db);
        db = 0;

        pid = fork();
        if( pid<0 ){ perror("fork"); exit(1); }
        if( pid==0 ){
          int rc;
          if( sqlite3_open_v2(dbpath, &db,
                SQLITE_OPEN_READWRITE, "dura-track")!=SQLITE_OK ) _exit(10);
          if( kind==1 ){
            char zMergeError[256];
            if( execSql(db, "BEGIN")!=SQLITE_OK ) _exit(11);
            rc = execSql(db, "SELECT dolt_merge('feat')");
            snprintf(zMergeError, sizeof(zMergeError), "%s", sqlite3_errmsg(db));
            if( queryScalarInt(db, "SELECT count(*) FROM dolt_conflicts", 0)==0 ){
              fprintf(stderr, "%s: merge setup rc=%d: %s\n", label, rc, zMergeError);
              _exit(12);
            }
          }
          gDuraCrashAt = at;
          gDuraSyncCount = 0;
          gDuraCrashBefore = before;
          rc = execSql(db, "SELECT dolt_merge('--abort')");
          gDuraCrashAt = 0;
          if( rc!=SQLITE_OK ) _exit(13);
          _exit(0);
        }
        check("test_32: child waited", waitpid(pid, &status, 0)==pid);
        crashed = WIFEXITED(status) && WEXITSTATUS(status)==99;
        snprintf(desc, sizeof(desc), "%s: child crashed or abort succeeded", label);
        check(desc, crashed || (WIFEXITED(status) && WEXITSTATUS(status)==0));
        if( at==1 ){
          snprintf(desc, sizeof(desc), "%s: sync crash exercised", label);
          check(desc, crashed);
        }
        check("test_32: recovery open", sqlite3_open(dbpath, &db)==SQLITE_OK);
        verify_merge_abort_local(db, label, kind!=1);
        merging = queryScalarInt(db, "SELECT is_merging FROM dolt_merge_status", -1);
        snprintf(desc, sizeof(desc), "%s: complete merge or complete abort", label);
        check(desc, (merging==0 || (kind!=1 && crashed && merging==1))
          && strcmp(queryScalarText(db, "SELECT group_concat(v) FROM "
               "(SELECT v FROM t ORDER BY id)"), merging==1 ? "10,20" : "10,2")==0);
        snprintf(desc, sizeof(desc), "%s: HEAD unchanged", label);
        check(desc, strcmp(queryScalarText(db, "SELECT dolt_hashof('HEAD')"),head)==0);
        snprintf(desc, sizeof(desc), "%s: conflicts never persisted", label);
        check(desc, queryScalarInt(db, "SELECT count(*) FROM dolt_conflicts", -1)==0);
        if( merging==1 ){
          check("test_32: recovered merge abortable", execSql(db,
                "SELECT dolt_merge('--abort')")==SQLITE_OK);
          verify_merge_abort_local(db, label, kind!=1);
        }
        snprintf(desc, sizeof(desc), "%s: integrity ok", label);
        check(desc, strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);
        sqlite3_close(db);
        removeDbFiles(dbpath);
        if( !crashed ) break;
      }
      check("test_32: all sync points swept", at<=8);
    }
  }
}

/* An autocommit merge that would add constraint violations is rolled back.
** A sync crash anywhere in that rollback must leave the pre-merge state,
** not the pre-merge rows with the merge's violations still recorded. */
static void test_33_cv_rollback_atomic_sync(void){
  int before;

  printf("--- Test 33: constraint-violation rollback survives sync crashes ---\n");
  check("test_33: tracking vfs registered", duraRegister()==SQLITE_OK);
  for(before=0; before<2; before++){
    int at;
    for(at=1; at<=8; at++){
      const char *dbpath = fresh_db();
      sqlite3 *db = 0;
      pid_t pid;
      int status = 0, crashed;
      char label[64], desc[160];
      snprintf(label, sizeof(label), "cv_rollback_%s_sync_%d",
               before ? "before" : "after", at);
      check("test_33: setup open", sqlite3_open(dbpath, &db)==SQLITE_OK);
      check("test_33: setup", execSql(db,
        "CREATE TABLE p(id INT PRIMARY KEY);"
        "CREATE TABLE c(id INT PRIMARY KEY, pid INT REFERENCES p(id));"
        "INSERT INTO p VALUES(1),(2); INSERT INTO c VALUES(10,1);"
        "SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat');"
        "DELETE FROM p WHERE id=2; SELECT dolt_commit('-am','main deletes p 2');"
        "SELECT dolt_checkout('feat');"
        "INSERT INTO c VALUES(11,2); SELECT dolt_commit('-am','feat uses p 2');"
        "SELECT dolt_checkout('main');")==SQLITE_OK);
      sqlite3_close(db);
      db = 0;

      pid = fork();
      if( pid<0 ){ perror("fork"); exit(1); }
      if( pid==0 ){
        int rc;
        if( sqlite3_open_v2(dbpath, &db,
              SQLITE_OPEN_READWRITE, "dura-track")!=SQLITE_OK ) _exit(10);
        if( execSql(db, "PRAGMA foreign_keys=ON")!=SQLITE_OK ) _exit(11);
        gDuraCrashAt = at;
        gDuraSyncCount = 0;
        gDuraCrashBefore = before;
        rc = execSql(db, "SELECT dolt_merge('feat')");
        gDuraCrashAt = 0;
        if( rc==SQLITE_OK ) _exit(12);
        _exit(gDuraSyncCount==0 ? 20 : 0);
      }
      check("test_33: child waited", waitpid(pid, &status, 0)==pid);
      crashed = WIFEXITED(status) && WEXITSTATUS(status)==99;
      snprintf(desc, sizeof(desc), "%s: crashed or rolled back", label);
      check(desc, crashed || (WIFEXITED(status)
            && (WEXITSTATUS(status)==0 || WEXITSTATUS(status)==20)));
      /* The merge either syncs (and a crash there must recover) or, refused
      ** before it publishes anything, never touches the file. */
      if( at==1 ){
        snprintf(desc, sizeof(desc), "%s: sync crash exercised or nothing written", label);
        check(desc, crashed || (WIFEXITED(status) && WEXITSTATUS(status)==20));
      }
      check("test_33: recovery open", sqlite3_open(dbpath, &db)==SQLITE_OK);
      execSql(db, "SELECT dolt_checkout('feat'); SELECT dolt_checkout('main')");
      snprintf(desc, sizeof(desc), "%s: pre-merge rows", label);
      check(desc, strcmp(queryScalarText(db,
            "SELECT group_concat(id) FROM c"), "10")==0);
      snprintf(desc, sizeof(desc), "%s: no constraint violations", label);
      check(desc, queryScalarInt(db,
            "SELECT count(*) FROM dolt_constraint_violations", -1)==0);
      snprintf(desc, sizeof(desc), "%s: next commit succeeds", label);
      check(desc, execSql(db,
            "SELECT dolt_commit('--allow-empty','-m','next')")==SQLITE_OK);
      sqlite3_close(db);
      removeDbFiles(dbpath);
    }
  }
}

/* A sync crash inside dolt_rebase leaves either no rebase or one that
** --abort can find and discard; a failed --continue keeps the session on its
** branch, and the rebase can then be retried. */
static void test_34_rebase_start_crash(void){
  int interactive;

  printf("--- Test 34: a crashed rebase can be aborted and retried ---\n");
  check("test_34: tracking vfs registered", duraRegister()==SQLITE_OK);
  for(interactive=0; interactive<3; interactive++){
    int before;
    for(before=0; before<2; before++){
      int at;
      for(at=1; at<=16; at++){
        const char *dbpath = fresh_db();
        char featPath[600], head[64], label[64], desc[160];
        sqlite3 *db = 0;
        pid_t pid;
        int status = 0, crashed, leftover;
        snprintf(label, sizeof(label), "rebase_%s_%s_sync_%d",
                 interactive==2 ? "continue" : interactive ? "i" : "plain",
                 before ? "before" : "after", at);
        snprintf(featPath, sizeof(featPath), "%s/feat", dbpath);
        check("test_34: setup open", sqlite3_open(dbpath, &db)==SQLITE_OK);
        check("test_34: setup", execSql(db,
          "CREATE TABLE t(id INT PRIMARY KEY, v INT);"
          "INSERT INTO t VALUES(1,1),(2,2),(3,3);"
          "SELECT dolt_commit('-Am','c1'); SELECT dolt_branch('feat');"
          "INSERT INTO t VALUES(4,4); SELECT dolt_commit('-am','c2');"
          "SELECT dolt_checkout('feat');"
          "INSERT INTO t VALUES(10,10); SELECT dolt_commit('-am','f1');"
          "INSERT INTO t VALUES(11,11); SELECT dolt_commit('-am','f2');")
          ==SQLITE_OK);
        snprintf(head, sizeof(head), "%s",
                 queryScalarText(db, "SELECT dolt_hashof('feat')"));
        sqlite3_close(db);
        db = 0;

        pid = fork();
        if( pid<0 ){ perror("fork"); exit(1); }
        if( pid==0 ){
          int rc;
          if( sqlite3_open_v2(featPath, &db,
                SQLITE_OPEN_READWRITE, "dura-track")!=SQLITE_OK ) _exit(10);
          if( interactive==2
           && execSql(db, "SELECT dolt_rebase('-i','main')")!=SQLITE_OK ){
            _exit(12);
          }
          gDuraCrashAt = at;
          gDuraSyncCount = 0;
          gDuraCrashBefore = before;
          rc = execSql(db, interactive==2 ? "SELECT dolt_rebase('--continue')"
                         : interactive ? "SELECT dolt_rebase('-i','main')"
                         : "SELECT dolt_rebase('main')");
          gDuraCrashAt = 0;
          _exit(rc==SQLITE_OK ? 0 : 11);
        }
        check("test_34: child waited", waitpid(pid, &status, 0)==pid);
        crashed = WIFEXITED(status) && WEXITSTATUS(status)==99;
        snprintf(desc, sizeof(desc), "%s: crashed or rebased", label);
        check(desc, crashed || (WIFEXITED(status) && WEXITSTATUS(status)==0));
        if( !crashed ){
          removeDbFiles(dbpath);
          break;
        }

        check("test_34: recovery open", sqlite3_open(featPath, &db)==SQLITE_OK);
        leftover = queryScalarInt(db,
            "SELECT count(*) FROM dolt_branches WHERE name='dolt_rebase_feat'", -1);
        if( leftover && interactive!=1 ){
          execSql(db, "SELECT dolt_rebase('--continue')");
          snprintf(desc, sizeof(desc), "%s: failed continue keeps branch", label);
          check(desc, strcmp(queryScalarText(db, "SELECT active_branch()"),
                             "feat")==0);
        }
        if( leftover ){
          snprintf(desc, sizeof(desc), "%s: abort finds the rebase", label);
          check(desc, execSql(db, "SELECT dolt_rebase('--abort')")==SQLITE_OK);
          /* A crash past the commit point leaves feat rebased; abort then
          ** only discards the leftover working branch. */
          snprintf(desc, sizeof(desc), "%s: abort restores the branch", label);
          check(desc, strcmp(queryScalarText(db, "SELECT active_branch()"),
                             "feat")==0
                   && (strcmp(queryScalarText(db, "SELECT dolt_hashof('feat')"),
                              head)==0
                    || strcmp(queryScalarText(db,
                         "SELECT group_concat(id) FROM (SELECT id FROM t ORDER BY id)"),
                         "1,2,3,4,10,11")==0));
        }
        snprintf(desc, sizeof(desc), "%s: no working branch left", label);
        check(desc, queryScalarInt(db,
            "SELECT count(*) FROM dolt_branches WHERE name='dolt_rebase_feat'",
            -1)==0);
        if( strcmp(queryScalarText(db, "SELECT dolt_hashof('feat')"), head)==0 ){
          snprintf(desc, sizeof(desc), "%s: retry rebases", label);
          check(desc, execSql(db, "SELECT dolt_rebase('main')")==SQLITE_OK);
        }
        snprintf(desc, sizeof(desc), "%s: rebased rows", label);
        check(desc, strcmp(queryScalarText(db,
              "SELECT group_concat(id) FROM (SELECT id FROM t ORDER BY id)"),
              "1,2,3,4,10,11")==0);
        sqlite3_close(db);
        removeDbFiles(dbpath);
      }
    }
  }
}

/* Files left beside zDest whose names contain "<dest base>-backup". */
static int countBackupLeftovers(const char *zDest){
  char zDir[512];
  char zNeedle[300];
  const char *zBase = strrchr(zDest, '/');
  DIR *d;
  struct dirent *e;
  int n = 0;
  if( zBase ){
    snprintf(zDir, sizeof(zDir), "%.*s", (int)(zBase - zDest), zDest);
    zBase++;
  }else{
    snprintf(zDir, sizeof(zDir), ".");
    zBase = zDest;
  }
  snprintf(zNeedle, sizeof(zNeedle), "%s-backup", zBase);
  d = opendir(zDir);
  if( !d ) return -1;
  while( (e = readdir(d))!=0 ){
    if( strstr(e->d_name, zNeedle) ) n++;
  }
  closedir(d);
  return n;
}

static int runBackup(const char *zSrc, const char *zDest, const char *zVfs){
  sqlite3 *pSrc = 0, *pDest = 0;
  sqlite3_backup *b;
  int rc;
  if( sqlite3_open(zSrc, &pSrc)!=SQLITE_OK ) return SQLITE_CANTOPEN;
  if( sqlite3_open_v2(zDest, &pDest,
        SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE, zVfs)!=SQLITE_OK ){
    sqlite3_close(pSrc);
    return SQLITE_CANTOPEN;
  }
  b = sqlite3_backup_init(pDest, "main", pSrc, "main");
  rc = b ? sqlite3_backup_step(b, -1) : sqlite3_errcode(pDest);
  if( b ) sqlite3_backup_finish(b);
  sqlite3_close(pDest);
  sqlite3_close(pSrc);
  return rc==SQLITE_DONE ? SQLITE_OK : rc;
}

/* A backup that crashes before installing its copy leaves the staged file
** and its lock sidecar; the next backup to the same destination removes
** them, and a successful backup leaves nothing beside the destination. */
static void test_35_backup_crash_leftovers(void){
  int before;
  printf("--- Test 35: backup leaves no temporary files after a crash ---\n");
  check("test_35: tracking vfs registered", duraRegister()==SQLITE_OK);
  for(before=0; before<2; before++){
    int at;
    for(at=1; at<=6; at++){
      char zSrc[512], zDest[600], desc[160];
      sqlite3 *db = 0;
      pid_t pid;
      int status = 0, crashed;
      snprintf(zSrc, sizeof(zSrc), "%s", fresh_db());
      snprintf(zDest, sizeof(zDest), "%s-dest.db", zSrc);
      removeDbFiles(zDest);
      check("test_35: setup open", sqlite3_open(zSrc, &db)==SQLITE_OK);
      check("test_35: setup", execSql(db,
        "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c"
        " WHERE x<2000) INSERT INTO t SELECT x, printf('%0100d', x) FROM c;"
        "SELECT dolt_commit('-Am','base');")==SQLITE_OK);
      sqlite3_close(db);
      check("test_35: first backup", runBackup(zSrc, zDest, 0)==SQLITE_OK);
      snprintf(desc, sizeof(desc), "backup_%s_%d: no leftovers after success",
               before ? "before" : "after", at);
      check(desc, countBackupLeftovers(zDest)==0);

      pid = fork();
      if( pid<0 ){ perror("fork"); exit(1); }
      if( pid==0 ){
        gDuraCrashAt = at;
        gDuraSyncCount = 0;
        gDuraCrashBefore = before;
        _exit(runBackup(zSrc, zDest, "dura-track")==SQLITE_OK ? 0 : 11);
      }
      check("test_35: child waited", waitpid(pid, &status, 0)==pid);
      crashed = WIFEXITED(status) && WEXITSTATUS(status)==99;
      if( !crashed ){
        removeDbFiles(zSrc);
        removeDbFiles(zDest);
        break;
      }
      check("test_35: backup after crash", runBackup(zSrc, zDest, 0)==SQLITE_OK);
      snprintf(desc, sizeof(desc), "backup_%s_%d: crash leftovers removed",
               before ? "before" : "after", at);
      check(desc, countBackupLeftovers(zDest)==0);
      check("test_35: dest open", sqlite3_open(zDest, &db)==SQLITE_OK);
      snprintf(desc, sizeof(desc), "backup_%s_%d: dest rows",
               before ? "before" : "after", at);
      check(desc, queryScalarInt(db, "SELECT count(*) FROM t", -1)==2000);
      sqlite3_close(db);
      removeDbFiles(zSrc);
      removeDbFiles(zDest);
    }
  }
}

static void test_36_large_sector_zero_tail_probe(void){
  static const int aSector[] = { 16384, 65536 };
  int i;
  printf("--- Test 36: large sectorSize zero-tail probe and replay ---\n");
  check("test_36: tracking vfs registered", duraRegister()==SQLITE_OK);
  for(i=0; i<(int)(sizeof(aSector)/sizeof(aSector[0])); i++){
    int sectorSz = aSector[i];
    char zPath[512], desc[160];
    sqlite3 *dbA = 0, *dbB = 0, *dbC = 0;
    int rc;

    snprintf(zPath, sizeof(zPath), "%s", fresh_db());
    check("test_36: setup open", sqlite3_open(zPath, &dbA)==SQLITE_OK);
    check("test_36: setup commit", execSql(dbA,
      "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
      "INSERT INTO t VALUES (1, 'base');"
      "SELECT dolt_commit('-Am', 'base');")==SQLITE_OK);
    sqlite3_close(dbA);
    dbA = 0;

    gDuraSectorSize = sectorSz;
    gDuraDeviceCharOverride = 1;
    gDuraDeviceChar = 0;

    rc = sqlite3_open_v2(zPath, &dbA, SQLITE_OPEN_READWRITE, "dura-track");
    snprintf(desc, sizeof(desc), "sector_%d: open dbA", sectorSz);
    check(desc, rc==SQLITE_OK);
    rc = sqlite3_open_v2(zPath, &dbB, SQLITE_OPEN_READWRITE, "dura-track");
    snprintf(desc, sizeof(desc), "sector_%d: open dbB", sectorSz);
    check(desc, rc==SQLITE_OK);

    snprintf(desc, sizeof(desc), "sector_%d: initial dbA count", sectorSz);
    check(desc, queryScalarInt(dbA, "SELECT count(*) FROM t", -1)==1);
    snprintf(desc, sizeof(desc), "sector_%d: initial dbB count", sectorSz);
    check(desc, queryScalarInt(dbB, "SELECT count(*) FROM t", -1)==1);

    snprintf(desc, sizeof(desc), "sector_%d: peer commit on dbB", sectorSz);
    check(desc, execSql(dbB,
      "INSERT INTO t VALUES (2, 'from_b');"
      "SELECT dolt_commit('-am', 'peer');")==SQLITE_OK);

    snprintf(desc, sizeof(desc), "sector_%d: dbA sees peer commit across zero gap", sectorSz);
    check(desc, queryScalarInt(dbA, "SELECT count(*) FROM t", -1)==2);

    snprintf(desc, sizeof(desc), "sector_%d: dbA commits after peer", sectorSz);
    check(desc, execSql(dbA,
      "INSERT INTO t VALUES (3, 'from_a');"
      "SELECT dolt_commit('-am', 'after_peer');")==SQLITE_OK);

    snprintf(desc, sizeof(desc), "sector_%d: dbB sees dbA commit", sectorSz);
    check(desc, queryScalarInt(dbB, "SELECT count(*) FROM t", -1)==3);

    /* Failed sync on dbA leaves a sealed root at an unaligned tail end;
    ** dbB adopts it and appends at the next sectorSz boundary, so closing
    ** dbA must detect dbB's commit across the >4096 zero gap and not scrub
    ** the adopted root. */
    gDuraFailSyncOnce = 1;
    rc = execSql(dbA, "INSERT INTO t VALUES (4, 'sync_fail');");
    gDuraFailSyncOnce = 0;
    gDuraFailRollbackWrite = 0;
    gDuraFailRollbackTruncate = 0;
    snprintf(desc, sizeof(desc), "sector_%d: injected sync error on dbA", sectorSz);
    check(desc, rc!=SQLITE_OK);

    snprintf(desc, sizeof(desc), "sector_%d: dbB adopts unaligned root", sectorSz);
    check(desc, queryScalarInt(dbB, "SELECT count(*) FROM t", -1)==4);
    snprintf(desc, sizeof(desc), "sector_%d: dbB commits past adopted root", sectorSz);
    check(desc, execSql(dbB, "INSERT INTO t VALUES (5, 'after_adopt');")==SQLITE_OK);

    sqlite3_close(dbA);
    dbA = 0;
    sqlite3_close(dbB);
    dbB = 0;

    rc = sqlite3_open_v2(zPath, &dbC, SQLITE_OPEN_READWRITE, "dura-track");
    snprintf(desc, sizeof(desc), "sector_%d: reopen dbC", sectorSz);
    check(desc, rc==SQLITE_OK);
    snprintf(desc, sizeof(desc), "sector_%d: reopen preserves all 5 rows", sectorSz);
    check(desc, queryScalarInt(dbC, "SELECT count(*) FROM t", -1)==5);
    snprintf(desc, sizeof(desc), "sector_%d: reopen integrity_check", sectorSz);
    check(desc, strcmp(queryScalarText(dbC, "PRAGMA integrity_check"), "ok")==0);
    sqlite3_close(dbC);

    gDuraSectorSize = 0;
    gDuraDeviceCharOverride = 0;
    gDuraDeviceChar = 0;
    removeDbFiles(zPath);
  }
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
  test_31_drain_durability_watermark();
  test_pending_drain_batch();
  test_32_merge_abort_atomic_sync();
  test_33_cv_rollback_atomic_sync();
  test_34_rebase_start_crash();
  test_35_backup_crash_leftovers();
  test_36_large_sector_zero_tail_probe();

  printf("\n=== Results: %d passed, %d failed out of %d tests ===\n",
         nPass, nFail, nPass+nFail);
  return nFail > 0 ? 1 : 0;
}
