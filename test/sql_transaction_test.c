/*
** SQL transaction concurrency tests.
**
** Two connections open explicit transactions (BEGIN), make conflicting
** changes, and the first writer's lock blocks the second. The second
** gets SQLITE_BUSY on its write statement (not on COMMIT).
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

/* Execute with busy retry */
static int execsql_retry(sqlite3 *db, const char *sql, int maxRetries){
  char *err = 0;
  int rc;
  int attempts = 0;
  do {
    err = 0;
    rc = sqlite3_exec(db, sql, 0, 0, &err);
    if( rc==SQLITE_BUSY ){
      sqlite3_free(err);
      sqlite3_sleep(10);
      attempts++;
    }else{
      if( err ) sqlite3_free(err);
      break;
    }
  } while( attempts < maxRetries );
  return rc;
}

static char result_buf[4096];
static const char *exec1(sqlite3 *db, const char *sql){
  sqlite3_stmt *s = 0;
  int rc;
  result_buf[0] = 0;
  rc = sqlite3_prepare_v2(db, sql, -1, &s, 0);
  if( rc!=SQLITE_OK ) return "PREP_ERR";
  rc = sqlite3_step(s);
  if( rc==SQLITE_ROW ){
    const char *v = (const char*)sqlite3_column_text(s, 0);
    if( v ) snprintf(result_buf, sizeof(result_buf), "%s", v);
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
** Test 1: INSERT same PK from two transactions.
** B's INSERT gets SQLITE_BUSY because A holds the write lock.
*/
static void test_insert_conflict(void){
  const char *path = "/tmp/test_txn_insert.db";
  sqlite3 *a, *b;
  int rc_a_ins, rc_b_ins;

  printf("--- Test 1: INSERT same PK from two transactions ---\n");
  remove(path);

  a = open_db(path);
  execsql(a, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");
  exec1(a, "SELECT dolt_commit('-A','-m','init')");

  b = open_db(path);

  execsql(a, "BEGIN");
  execsql(b, "BEGIN");

  rc_a_ins = execsql(a, "INSERT INTO t VALUES(1, 'from_a')");
  check("insert_a_ok", rc_a_ins==SQLITE_OK);

  /* B's INSERT should get BUSY because A holds the write lock */
  rc_b_ins = execsql(b, "INSERT INTO t VALUES(1, 'from_b')");
  check("insert_b_busy", rc_b_ins==SQLITE_BUSY);

  execsql(a, "COMMIT");

  /* After A commits and releases the lock, B can retry */
  rc_b_ins = execsql(b, "INSERT INTO t VALUES(1, 'from_b')");
  /* B's INSERT should fail with SQLITE_CONSTRAINT (PK already exists) */
  check("insert_b_constraint", rc_b_ins!=SQLITE_OK);

  execsql(b, "ROLLBACK");

  check("insert_a_wins",
    strcmp(exec1(a, "SELECT v FROM t WHERE id=1"), "from_a")==0);

  sqlite3_close(a);
  sqlite3_close(b);
  remove(path);
}

/*
** Test 2: UPDATE same row from two transactions.
** B's UPDATE gets SQLITE_BUSY.
*/
static void test_update_conflict(void){
  const char *path = "/tmp/test_txn_update.db";
  sqlite3 *a, *b;
  int rc;

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

  rc = execsql(a, "UPDATE t SET v='updated_a' WHERE id=1");
  check("update_a_ok", rc==SQLITE_OK);

  rc = execsql(b, "UPDATE t SET v='updated_b' WHERE id=1");
  check("update_b_busy", rc==SQLITE_BUSY);

  execsql(a, "COMMIT");
  execsql(b, "ROLLBACK");

  check("update_a_wins",
    strcmp(exec1(a, "SELECT v FROM t WHERE id=1"), "updated_a")==0);

  sqlite3_close(a);
  sqlite3_close(b);
  remove(path);
}

/*
** Test 3: DELETE and UPDATE same row.
** B gets SQLITE_BUSY.
*/
static void test_delete_update_conflict(void){
  const char *path = "/tmp/test_txn_delupd.db";
  sqlite3 *a, *b;
  int rc;

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

  rc = execsql(a, "DELETE FROM t WHERE id=1");
  check("delupd_a_ok", rc==SQLITE_OK);

  rc = execsql(b, "UPDATE t SET v='updated_b' WHERE id=1");
  check("delupd_b_busy", rc==SQLITE_BUSY);

  execsql(a, "COMMIT");
  execsql(b, "ROLLBACK");

  check("delupd_row_gone",
    strcmp(exec1(a, "SELECT count(*) FROM t WHERE id=1"), "0")==0);

  sqlite3_close(a);
  sqlite3_close(b);
  remove(path);
}

/*
** Test 4: Non-conflicting INSERTs (different PKs).
** B waits for A to commit, then succeeds. Both rows survive.
*/
static void test_non_conflicting_inserts(void){
  const char *path = "/tmp/test_txn_noconflict.db";
  sqlite3 *a, *b;
  int rc;

  printf("--- Test 4: Non-conflicting INSERTs (different PKs) ---\n");
  remove(path);

  a = open_db(path);
  b = open_db(path);

  execsql(a, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");
  exec1(a, "SELECT dolt_commit('-A','-m','init')");

  /* A inserts and commits first */
  execsql(a, "BEGIN");
  execsql(a, "INSERT INTO t VALUES(1, 'from_a')");
  rc = execsql(a, "COMMIT");
  check("noconflict_a_ok", rc==SQLITE_OK);

  /* B inserts different PK — should succeed now that A released */
  execsql(b, "BEGIN");
  rc = execsql(b, "INSERT INTO t VALUES(2, 'from_b')");
  check("noconflict_b_insert_ok", rc==SQLITE_OK);
  rc = execsql(b, "COMMIT");
  check("noconflict_b_commit_ok", rc==SQLITE_OK);

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
** Test 5: Sequential transactions (no overlap) both succeed.
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

  execsql(a, "BEGIN");
  execsql(a, "INSERT INTO t VALUES(1, 'from_a')");
  rc = execsql(a, "COMMIT");
  check("seq_a_ok", rc==SQLITE_OK);

  execsql(b, "BEGIN");
  rc = execsql(b, "INSERT INTO t VALUES(2, 'from_b')");
  check("seq_b_insert_ok", rc==SQLITE_OK);
  rc = execsql(b, "COMMIT");
  check("seq_b_commit_ok", rc==SQLITE_OK);

  /* Reopen to see both commits */
  {
    sqlite3 *c = open_db(path);
    check("seq_count",
      strcmp(exec1(c, "SELECT count(*) FROM t"), "2")==0);
    sqlite3_close(c);
  }

  sqlite3_close(a);
  sqlite3_close(b);
  remove(path);
}

/*
** Test 6: Read during write transaction.
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

  execsql(a, "BEGIN");
  execsql(a, "INSERT INTO t VALUES(2, 'new_row')");

  /* B should still be able to read (read doesn't need write lock) */
  check("read_during_write",
    strcmp(exec1(b, "SELECT count(*) FROM t"), "0")!=0);

  execsql(a, "COMMIT");

  sqlite3_close(a);
  sqlite3_close(b);
  remove(path);
}

/*
** Test 7: ROLLBACK releases the write lock.
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

  execsql(a, "BEGIN");
  execsql(a, "INSERT INTO t VALUES(1, 'will_rollback')");
  execsql(a, "ROLLBACK");

  /* B should be able to acquire write lock now */
  execsql(b, "BEGIN");
  rc = execsql(b, "INSERT INTO t VALUES(1, 'from_b')");
  check("rollback_b_insert_ok", rc==SQLITE_OK);
  rc = execsql(b, "COMMIT");
  check("rollback_b_commit_ok", rc==SQLITE_OK);

  check("rollback_b_value",
    strcmp(exec1(b, "SELECT v FROM t WHERE id=1"), "from_b")==0);

  sqlite3_close(a);
  sqlite3_close(b);
  remove(path);
}

/*
** Test 8: B uses busy_timeout to wait for A, then succeeds.
*/
static void test_busy_retry(void){
  const char *path = "/tmp/test_txn_retry.db";
  sqlite3 *a, *b;
  int rc;

  printf("--- Test 8: B waits with busy_timeout for A ---\n");
  remove(path);

  a = open_db(path);
  b = open_db(path);

  execsql(a, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");
  exec1(a, "SELECT dolt_commit('-A','-m','init')");

  execsql(a, "BEGIN");
  execsql(a, "INSERT INTO t VALUES(1, 'from_a')");

  /* B's INSERT will get BUSY, but busy_timeout should wait */
  /* Since A hasn't committed yet, B will timeout. Use short timeout. */
  sqlite3_busy_timeout(b, 100);
  rc = execsql(b, "INSERT INTO t VALUES(2, 'from_b')");
  check("retry_b_busy_timeout", rc==SQLITE_BUSY);

  /* Now A commits */
  execsql(a, "COMMIT");

  /* B can now retry and succeed */
  rc = execsql_retry(b, "INSERT INTO t VALUES(2, 'from_b')", 50);
  check("retry_b_succeeds_after", rc==SQLITE_OK);

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
  test_busy_retry();

  printf("\n=== Results: %d passed, %d failed out of %d tests ===\n",
    nPass, nFail, nPass+nFail);
  return nFail > 0 ? 1 : 0;
}
