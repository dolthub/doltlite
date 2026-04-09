/*
** Concurrent refs mutation repro for DoltLite.
**
** This reproduces a stale-writer bug in concurrent refs mutation:
**
**   1. db1 and db2 open on the same branch tip
**   2. db1 creates a new commit on main
**   3. db2, still stale, runs dolt_reset(<old hash>)
**
** Correct behavior:
**   - the stale reset is rejected
**   - the branch tip remains on the newer commit
**   - branch history still contains both commits
**
** Current buggy behavior before the fix:
**   - dolt_reset() returns success
**   - the branch tip moves back to the old commit
**   - the newer commit disappears from dolt_log on that branch
**
** Build from build/:
**   cc -g -I. -o concurrent_refs_test \
**     ../test/concurrent_refs_test.c libdoltlite.a -lz -lpthread
**
** Run from build/:
**   ./concurrent_refs_test
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "  SQL error: %s (rc=%d)\n  SQL: %s\n",
            err ? err : "?", rc, sql);
    sqlite3_free(err);
  }
  return rc;
}

static char result_buf[8192];
static const char *exec1(sqlite3 *db, const char *sql){
  sqlite3_stmt *stmt = 0;
  int rc;
  result_buf[0] = 0;
  rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);
  if( rc!=SQLITE_OK ){
    snprintf(result_buf, sizeof(result_buf), "ERROR: %s", sqlite3_errmsg(db));
    return result_buf;
  }
  rc = sqlite3_step(stmt);
  if( rc==SQLITE_ROW ){
    const char *val = (const char*)sqlite3_column_text(stmt, 0);
    if( val ) snprintf(result_buf, sizeof(result_buf), "%s", val);
  }else if( rc!=SQLITE_DONE ){
    snprintf(result_buf, sizeof(result_buf), "ERROR: %s", sqlite3_errmsg(db));
  }
  sqlite3_finalize(stmt);
  return result_buf;
}

static int open_db(const char *path, sqlite3 **ppDb){
  int rc = sqlite3_open(path, ppDb);
  if( rc==SQLITE_OK ){
    sqlite3_busy_timeout(*ppDb, 5000);
  }
  return rc;
}

static void remove_db(const char *path){
  char tmp[512];
  remove(path);
  snprintf(tmp, sizeof(tmp), "%s-wal", path);
  remove(tmp);
  snprintf(tmp, sizeof(tmp), "%s-shm", path);
  remove(tmp);
}

static void test_stale_reset_loses_branch_history(void){
  sqlite3 *db1 = 0, *db2 = 0, *db3 = 0;
  const char *dbpath = "/tmp/test_concurrent_refs_reset.db";
  char firstCommit[128];
  char secondCommit[128];
  char sql[512];

  printf("--- Test 1: stale dolt_reset is rejected ---\n");
  remove_db(dbpath);

  check("open_db1", open_db(dbpath, &db1)==SQLITE_OK);
  check("open_db2", open_db(dbpath, &db2)==SQLITE_OK);

  check("setup_schema", execsql(db1,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');")==SQLITE_OK);

  snprintf(firstCommit, sizeof(firstCommit), "%s",
           exec1(db1, "SELECT dolt_commit('-A', '-m', 'init')"));
  check("first_commit_hash", strlen(firstCommit)==40);

  check("insert_second_row",
    execsql(db1, "INSERT INTO t VALUES(2,'b')")==SQLITE_OK);
  snprintf(secondCommit, sizeof(secondCommit), "%s",
           exec1(db1, "SELECT dolt_commit('-A', '-m', 'second')"));
  check("second_commit_hash", strlen(secondCommit)==40);

  snprintf(sql, sizeof(sql), "SELECT dolt_reset('%s')", firstCommit);
  exec1(db2, sql);
  check("stale_reset_is_rejected",
    strstr(result_buf, "ERROR")!=0 || strstr(result_buf, "conflict")!=0);

  sqlite3_close(db1);
  sqlite3_close(db2);

  check("open_db3", open_db(dbpath, &db3)==SQLITE_OK);

  check("newer_commit_still_head",
    strcmp(exec1(db3, "SELECT message FROM dolt_log LIMIT 1"), "second")==0);
  snprintf(sql, sizeof(sql),
           "SELECT count(*) FROM dolt_log WHERE commit_hash='%s'", secondCommit);
  check("newer_commit_still_visible_in_log",
    strcmp(exec1(db3, sql), "1")==0);
  check("branch_history_has_both_commits",
    strcmp(exec1(db3, "SELECT count(*) FROM dolt_log"), "2")==0);

  sqlite3_close(db3);
  remove_db(dbpath);
}

int main(void){
  printf("=== Concurrent Refs Test ===\n\n");

  test_stale_reset_loses_branch_history();

  printf("\nResults: %d passed, %d failed out of %d tests\n",
         nPass, nFail, nPass+nFail);
  return nFail > 0 ? 1 : 0;
}
