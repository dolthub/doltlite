/*
** Concurrent commit invariant tests for DoltLite.
**
** These tests verify the commit isolation invariant:
**
**   NO SILENT COMMIT LOSS: If dolt_commit returns a hash, that commit
**   is the branch tip. If another connection committed to the same branch
**   since this connection last saw it, dolt_commit must return an error —
**   not silently overwrite the other connection's commit.
**
** This invariant was violated in the original code. Aaron's code review
** demonstrated the bug: Connection A commits "add one", Connection B
** commits "add two", and "add one" disappears from the log with no error.
**
** Test 1 reproduces Aaron's exact scenario.
** Test 2 verifies single-connection commits still work (no false positives).
** Test 3 verifies sequential commits from reopened connections work.
**
** Build:
**   cc -g -I. -o concurrent_commit_test \
**     ../test/concurrent_commit_test.c libdoltlite.a -lz -lpthread
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

static char result_buf[4096];
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
  }else if( rc==SQLITE_ERROR ){
    snprintf(result_buf, sizeof(result_buf), "ERROR: %s", sqlite3_errmsg(db));
  }
  sqlite3_finalize(stmt);
  return result_buf;
}

static int execsql(sqlite3 *db, const char *sql){
  char *err = 0;
  int rc = sqlite3_exec(db, sql, 0, 0, &err);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "  SQL error: %s (rc=%d)\n  SQL: %s\n", err ? err : "?", rc, sql);
    sqlite3_free(err);
  }
  return rc;
}

/*
** Test 1: Aaron's exact scenario.
**
** Connection 1 creates a table and commits.
** Connection 2 opens and sees the table.
** Connection 1 inserts a row and commits ("add one").
** Connection 2 inserts a different row and commits ("add two").
**
** Before fix: "add one" commit is silently lost.
** After fix: "add two" commit fails with a conflict error.
*/
static void test_aaron_scenario(void){
  sqlite3 *db1 = 0, *db2 = 0;
  const char *dbpath = "/tmp/test_concurrent_commit.db";
  int rc;
  const char *res;

  printf("--- Test 1: Aaron's concurrent commit scenario ---\n");

  remove(dbpath);

  rc = sqlite3_open(dbpath, &db1);
  check("open_db1", rc==SQLITE_OK);

  /* Connection 1: create table and commit */
  execsql(db1, "CREATE TABLE vals (id INT, val INT)");
  res = exec1(db1, "SELECT dolt_commit('-A', '-m', 'Initial commit')");
  check("initial_commit_ok", strlen(res)==40);

  /* Connection 2: open, sees the table */
  rc = sqlite3_open(dbpath, &db2);
  check("open_db2", rc==SQLITE_OK);
  res = exec1(db2, "SELECT count(*) FROM vals");
  check("db2_sees_table", strcmp(res, "0")==0);

  /* Connection 1: insert and commit */
  execsql(db1, "INSERT INTO vals VALUES (1, 1)");
  res = exec1(db1, "SELECT dolt_commit('-A', '-m', 'add one')");
  check("commit_add_one", strlen(res)==40);

  /* Connection 2: insert and try to commit — should fail */
  execsql(db2, "INSERT INTO vals VALUES (2, 2)");
  res = exec1(db2, "SELECT dolt_commit('-A', '-m', 'add two')");
  check("commit_add_two_rejected",
    strstr(res, "ERROR") != 0 || strstr(res, "conflict") != 0);

  /* Verify "add one" commit is NOT lost.
  ** Re-open db1 to get a fresh view — the shared in-memory state may have
  ** been polluted by db2's uncommitted DML (a known issue with shared
  ** BtShared when concurrent DML touches the same tables). */
  sqlite3_close(db1);
  rc = sqlite3_open(dbpath, &db1);
  check("reopen_db1", rc==SQLITE_OK);

  res = exec1(db1, "SELECT count(*) FROM dolt_log");
  check("log_has_2_entries", strcmp(res, "2")==0);

  res = exec1(db1, "SELECT message FROM dolt_log LIMIT 1");
  check("latest_commit_is_add_one", strcmp(res, "add one")==0);

  /* Verify the data from "add one" is intact */
  res = exec1(db1, "SELECT val FROM vals WHERE id=1");
  check("add_one_data_intact", strcmp(res, "1")==0);

  sqlite3_close(db1);
  sqlite3_close(db2);
  remove(dbpath);
}

/*
** Test 2: Single connection commits work normally (no false positives).
*/
static void test_single_connection(void){
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_single_commit.db";
  int rc;
  const char *res;

  printf("--- Test 2: Single connection commits work normally ---\n");

  remove(dbpath);
  rc = sqlite3_open(dbpath, &db);
  check("single_open", rc==SQLITE_OK);

  execsql(db, "CREATE TABLE t1 (a INT, b TEXT)");
  res = exec1(db, "SELECT dolt_commit('-A', '-m', 'first')");
  check("single_commit_1", strlen(res)==40);

  execsql(db, "INSERT INTO t1 VALUES (1, 'hello')");
  res = exec1(db, "SELECT dolt_commit('-A', '-m', 'second')");
  check("single_commit_2", strlen(res)==40);

  execsql(db, "INSERT INTO t1 VALUES (2, 'world')");
  res = exec1(db, "SELECT dolt_commit('-A', '-m', 'third')");
  check("single_commit_3", strlen(res)==40);

  res = exec1(db, "SELECT count(*) FROM dolt_log");
  check("single_log_count", strcmp(res, "3")==0);

  res = exec1(db, "SELECT count(*) FROM t1");
  check("single_data_count", strcmp(res, "2")==0);

  sqlite3_close(db);
  remove(dbpath);
}

/*
** Test 3: Sequential commits from different connections (no overlap) should
** work fine — each connection refreshes before committing.
*/
static void test_sequential_multi_connection(void){
  sqlite3 *db1 = 0, *db2 = 0;
  const char *dbpath = "/tmp/test_sequential_commit.db";
  int rc;
  const char *res;

  printf("--- Test 3: Sequential multi-connection commits (no overlap) ---\n");

  remove(dbpath);

  rc = sqlite3_open(dbpath, &db1);
  check("seq_open_db1", rc==SQLITE_OK);
  rc = sqlite3_open(dbpath, &db2);
  check("seq_open_db2", rc==SQLITE_OK);

  /* db1 creates and commits */
  execsql(db1, "CREATE TABLE seq (id INT, name TEXT)");
  res = exec1(db1, "SELECT dolt_commit('-A', '-m', 'create seq')");
  check("seq_first_commit", strlen(res)==40);

  /* db2 opens AFTER db1's commit, inserts, and commits.
  ** Since db2 opened after the commit, it should see the committed state
  ** and be able to commit without conflict. */
  sqlite3_close(db2);
  rc = sqlite3_open(dbpath, &db2);
  check("seq_reopen_db2", rc==SQLITE_OK);

  execsql(db2, "INSERT INTO seq VALUES (1, 'from_db2')");
  res = exec1(db2, "SELECT dolt_commit('-A', '-m', 'db2 insert')");
  check("seq_db2_commit", strlen(res)==40);

  /* Reopen db1 to get fresh state, then commit */
  sqlite3_close(db1);
  rc = sqlite3_open(dbpath, &db1);
  check("seq_reopen_db1", rc==SQLITE_OK);

  execsql(db1, "INSERT INTO seq VALUES (2, 'from_db1')");
  res = exec1(db1, "SELECT dolt_commit('-A', '-m', 'db1 insert')");
  check("seq_db1_commit", strlen(res)==40);

  /* Both rows should exist */
  res = exec1(db1, "SELECT count(*) FROM seq");
  check("seq_both_rows", strcmp(res, "2")==0);

  res = exec1(db1, "SELECT count(*) FROM dolt_log");
  check("seq_log_count", strcmp(res, "3")==0);

  sqlite3_close(db1);
  sqlite3_close(db2);
  remove(dbpath);
}

int main(){
  printf("=== Concurrent Commit Test ===\n\n");

  test_aaron_scenario();
  test_single_connection();
  test_sequential_multi_connection();

  printf("\nResults: %d passed, %d failed out of %d tests\n",
    nPass, nFail, nPass+nFail);
  return nFail > 0 ? 1 : 0;
}
