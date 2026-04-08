/*
** SQL transaction concurrency tests.
**
** These test standard SQL transaction semantics: two connections open
** explicit transactions (BEGIN), make conflicting changes, and the
** first to COMMIT wins. The second should get an error on COMMIT
** (SQLITE_BUSY or equivalent) and roll back.
**
** This is how standard SQLite behaves and how DoltLite should behave
** for DML on the same branch.
**
** Build from build/ directory:
**   cc -g -I. -o sql_transaction_test \
**     ../test/sql_transaction_test.c libdoltlite.a -lz -lpthread
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

static sqlite3 *open_db(const char *path){
  sqlite3 *db = 0;
  sqlite3_open(path, &db);
  sqlite3_busy_timeout(db, 5000);
  return db;
}

/*
** Test 1: INSERT same primary key from two transactions.
** First COMMIT wins, second should fail.
*/
static void test_insert_conflict(void){
  const char *path = "/tmp/test_txn_insert.db";
  sqlite3 *a, *b;
  int rc_a, rc_b;

  printf("--- Test 1: INSERT same PK from two transactions ---\n");
  remove(path);

  a = open_db(path);
  b = open_db(path);

  execsql(a, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");
  exec1(a, "SELECT dolt_commit('-A','-m','init')");

  /* Both begin explicit transactions */
  execsql(a, "BEGIN");
  execsql(b, "BEGIN");

  /* Both insert same primary key */
  execsql(a, "INSERT INTO t VALUES(1, 'from_a')");
  execsql(b, "INSERT INTO t VALUES(1, 'from_b')");

  /* First commit should succeed */
  rc_a = execsql(a, "COMMIT");
  check("insert_a_commit_ok", rc_a==SQLITE_OK);

  /* Second commit should fail (SQLITE_BUSY or SQLITE_LOCKED) */
  rc_b = execsql(b, "COMMIT");
  check("insert_b_commit_fails", rc_b!=SQLITE_OK);

  /* Verify a's value won */
  check("insert_a_wins",
    strcmp(exec1(a, "SELECT v FROM t WHERE id=1"), "from_a")==0);

  sqlite3_close(a);
  sqlite3_close(b);
  remove(path);
}

/*
** Test 2: UPDATE same row with different values from two transactions.
** First COMMIT wins, second should fail.
*/
static void test_update_conflict(void){
  const char *path = "/tmp/test_txn_update.db";
  sqlite3 *a, *b;
  int rc_a, rc_b;

  printf("--- Test 2: UPDATE same row from two transactions ---\n");
  remove(path);

  a = open_db(path);
  execsql(a, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");
  execsql(a, "INSERT INTO t VALUES(1, 'original')");
  exec1(a, "SELECT dolt_commit('-A','-m','init')");
  sqlite3_close(a);

  a = open_db(path);
  b = open_db(path);

  execsql(a, "BEGIN");
  execsql(b, "BEGIN");

  execsql(a, "UPDATE t SET v='updated_a' WHERE id=1");
  execsql(b, "UPDATE t SET v='updated_b' WHERE id=1");

  rc_a = execsql(a, "COMMIT");
  check("update_a_commit_ok", rc_a==SQLITE_OK);

  rc_b = execsql(b, "COMMIT");
  check("update_b_commit_fails", rc_b!=SQLITE_OK);

  /* a's value should be visible */
  {
    sqlite3 *c = open_db(path);
    check("update_a_wins",
      strcmp(exec1(c, "SELECT v FROM t WHERE id=1"), "updated_a")==0);
    sqlite3_close(c);
  }

  sqlite3_close(a);
  sqlite3_close(b);
  remove(path);
}

/*
** Test 3: DELETE and UPDATE same row from two transactions.
** First COMMIT wins, second should fail.
*/
static void test_delete_update_conflict(void){
  const char *path = "/tmp/test_txn_delupd.db";
  sqlite3 *a, *b;
  int rc_a, rc_b;

  printf("--- Test 3: DELETE vs UPDATE same row ---\n");
  remove(path);

  a = open_db(path);
  execsql(a, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");
  execsql(a, "INSERT INTO t VALUES(1, 'original')");
  exec1(a, "SELECT dolt_commit('-A','-m','init')");
  sqlite3_close(a);

  a = open_db(path);
  b = open_db(path);

  execsql(a, "BEGIN");
  execsql(b, "BEGIN");

  execsql(a, "DELETE FROM t WHERE id=1");
  execsql(b, "UPDATE t SET v='updated_b' WHERE id=1");

  rc_a = execsql(a, "COMMIT");
  check("delupd_a_commit_ok", rc_a==SQLITE_OK);

  rc_b = execsql(b, "COMMIT");
  check("delupd_b_commit_fails", rc_b!=SQLITE_OK);

  /* Row should be deleted (a won) */
  {
    sqlite3 *c = open_db(path);
    check("delupd_row_gone",
      strcmp(exec1(c, "SELECT count(*) FROM t WHERE id=1"), "0")==0);
    sqlite3_close(c);
  }

  sqlite3_close(a);
  sqlite3_close(b);
  remove(path);
}

/*
** Test 4: Non-conflicting INSERTs (different PKs) should both succeed.
*/
static void test_non_conflicting_inserts(void){
  const char *path = "/tmp/test_txn_noconflict.db";
  sqlite3 *a, *b;
  int rc_a, rc_b;

  printf("--- Test 4: Non-conflicting INSERTs (different PKs) ---\n");
  remove(path);

  a = open_db(path);
  b = open_db(path);

  execsql(a, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");
  exec1(a, "SELECT dolt_commit('-A','-m','init')");

  execsql(a, "BEGIN");
  execsql(b, "BEGIN");

  execsql(a, "INSERT INTO t VALUES(1, 'from_a')");
  execsql(b, "INSERT INTO t VALUES(2, 'from_b')");

  rc_a = execsql(a, "COMMIT");
  check("noconflict_a_commit_ok", rc_a==SQLITE_OK);

  rc_b = execsql(b, "COMMIT");
  check("noconflict_b_commit_ok", rc_b==SQLITE_OK);

  /* Both rows should exist */
  {
    sqlite3 *c = open_db(path);
    check("noconflict_both_exist",
      strcmp(exec1(c, "SELECT count(*) FROM t"), "2")==0);
    sqlite3_close(c);
  }

  sqlite3_close(a);
  sqlite3_close(b);
  remove(path);
}

/*
** Test 5: Sequential transactions (no overlap) should both succeed.
*/
static void test_sequential_transactions(void){
  const char *path = "/tmp/test_txn_seq.db";
  sqlite3 *a, *b;
  int rc;

  printf("--- Test 5: Sequential transactions (no overlap) ---\n");
  remove(path);

  a = open_db(path);
  b = open_db(path);

  execsql(a, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");
  exec1(a, "SELECT dolt_commit('-A','-m','init')");

  /* a: full transaction, committed before b starts */
  execsql(a, "BEGIN");
  execsql(a, "INSERT INTO t VALUES(1, 'from_a')");
  rc = execsql(a, "COMMIT");
  check("seq_a_ok", rc==SQLITE_OK);

  /* b: starts after a committed */
  execsql(b, "BEGIN");
  execsql(b, "INSERT INTO t VALUES(2, 'from_b')");
  rc = execsql(b, "COMMIT");
  check("seq_b_ok", rc==SQLITE_OK);

  check("seq_count",
    strcmp(exec1(a, "SELECT count(*) FROM t"), "2")==0);

  sqlite3_close(a);
  sqlite3_close(b);
  remove(path);
}

/*
** Test 6: Read during write transaction.
** Connection b should be able to read while a has an open write txn.
*/
static void test_read_during_write(void){
  const char *path = "/tmp/test_txn_readwrite.db";
  sqlite3 *a, *b;

  printf("--- Test 6: Read during write transaction ---\n");
  remove(path);

  a = open_db(path);
  b = open_db(path);

  execsql(a, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");
  execsql(a, "INSERT INTO t VALUES(1, 'original')");
  exec1(a, "SELECT dolt_commit('-A','-m','init')");

  /* a starts a write transaction */
  execsql(a, "BEGIN");
  execsql(a, "INSERT INTO t VALUES(2, 'new_row')");

  /* b should be able to read */
  check("read_during_write",
    strcmp(exec1(b, "SELECT count(*) FROM t"), "0")!=0);

  execsql(a, "COMMIT");

  sqlite3_close(a);
  sqlite3_close(b);
  remove(path);
}

/*
** Test 7: ROLLBACK releases the write lock.
** After a rolls back, b should be able to write.
*/
static void test_rollback_releases(void){
  const char *path = "/tmp/test_txn_rollback.db";
  sqlite3 *a, *b;
  int rc;

  printf("--- Test 7: ROLLBACK releases lock ---\n");
  remove(path);

  a = open_db(path);
  b = open_db(path);

  execsql(a, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");
  exec1(a, "SELECT dolt_commit('-A','-m','init')");

  /* a writes then rolls back */
  execsql(a, "BEGIN");
  execsql(a, "INSERT INTO t VALUES(1, 'will_rollback')");
  execsql(a, "ROLLBACK");

  /* b should be able to write and commit */
  execsql(b, "BEGIN");
  execsql(b, "INSERT INTO t VALUES(1, 'from_b')");
  rc = execsql(b, "COMMIT");
  check("rollback_b_ok", rc==SQLITE_OK);

  check("rollback_b_value",
    strcmp(exec1(b, "SELECT v FROM t WHERE id=1"), "from_b")==0);

  sqlite3_close(a);
  sqlite3_close(b);
  remove(path);
}

/*
** Test 8: Multiple rows in conflicting transactions.
** a inserts rows 1-5, b inserts rows 3-7 (overlap on 3-5).
*/
static void test_multi_row_conflict(void){
  const char *path = "/tmp/test_txn_multi.db";
  sqlite3 *a, *b;
  int rc_a, rc_b;
  int i;

  printf("--- Test 8: Multi-row overlapping transactions ---\n");
  remove(path);

  a = open_db(path);
  b = open_db(path);

  execsql(a, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");
  exec1(a, "SELECT dolt_commit('-A','-m','init')");

  execsql(a, "BEGIN");
  execsql(b, "BEGIN");

  for(i=1; i<=5; i++){
    char sql[128];
    snprintf(sql, sizeof(sql), "INSERT INTO t VALUES(%d, 'a_%d')", i, i);
    execsql(a, sql);
  }
  for(i=3; i<=7; i++){
    char sql[128];
    snprintf(sql, sizeof(sql), "INSERT INTO t VALUES(%d, 'b_%d')", i, i);
    execsql(b, sql);
  }

  rc_a = execsql(a, "COMMIT");
  check("multi_a_commit_ok", rc_a==SQLITE_OK);

  rc_b = execsql(b, "COMMIT");
  check("multi_b_commit_fails", rc_b!=SQLITE_OK);

  sqlite3_close(a);
  sqlite3_close(b);
  remove(path);
}

int main(){
  printf("=== SQL Transaction Concurrency Tests ===\n\n");

  test_insert_conflict();
  test_update_conflict();
  test_delete_update_conflict();
  test_non_conflicting_inserts();
  test_sequential_transactions();
  test_read_during_write();
  test_rollback_releases();
  test_multi_row_conflict();

  printf("\n=== Results: %d passed, %d failed out of %d tests ===\n",
    nPass, nFail, nPass+nFail);
  return nFail > 0 ? 1 : 0;
}
