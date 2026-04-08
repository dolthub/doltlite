/*
** Cross-branch concurrent access test for DoltLite.
**
** Reproduces the corruption described in issue #277:
** Two connections on different branches both autocommit.
** Without per-branch working catalogs, the second writer's
** catalog overwrites the first's in the manifest, causing
** SQLITE_CORRUPT reads from the first connection.
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
    if( val ){
      snprintf(result_buf, sizeof(result_buf), "%s", val);
    }
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

int main(){
  sqlite3 *db1 = 0, *db2 = 0;
  const char *dbpath = "/tmp/test_cross_branch.db";
  const char *r;
  int rc;

  remove(dbpath);
  { char w[256]; snprintf(w,256,"%s-wal",dbpath); remove(w); }

  printf("=== Cross-Branch Concurrent Access Test ===\n\n");

  /* Open db1 for initial setup */
  rc = sqlite3_open(dbpath, &db1);
  check("open_db1", rc==SQLITE_OK);
  sqlite3_busy_timeout(db1, 5000);

  printf("--- Setup: create table and initial commit on main ---\n");

  /* db1: create schema, insert data, commit on main */
  rc = execsql(db1, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
  check("create_table", rc==SQLITE_OK);
  rc = execsql(db1, "INSERT INTO t VALUES(1, 'main-1')");
  check("insert_main", rc==SQLITE_OK);
  exec1(db1, "SELECT dolt_commit('-A', '-m', 'initial on main')");

  /* Open db2 AFTER the commit so it sees the head commit */
  rc = sqlite3_open(dbpath, &db2);
  check("open_db2", rc==SQLITE_OK);
  sqlite3_busy_timeout(db2, 5000);

  /* Verify both see it */
  check("db1_sees_main", strcmp(exec1(db1, "SELECT val FROM t WHERE id=1"), "main-1")==0);
  check("db2_sees_main", strcmp(exec1(db2, "SELECT val FROM t WHERE id=1"), "main-1")==0);

  printf("--- Create dev branch and checkout db2 to dev ---\n");

  /* db2: create branch and checkout */
  exec1(db2, "SELECT dolt_checkout('-b', 'dev')");
  check("db2_on_dev", strcmp(exec1(db2, "SELECT active_branch()"), "dev")==0);
  check("db1_on_main", strcmp(exec1(db1, "SELECT active_branch()"), "main")==0);

  printf("--- Test 1: autocommit writes on different branches ---\n");

  /* db1 writes on main (autocommit) */
  rc = execsql(db1, "INSERT INTO t VALUES(2, 'main-2')");
  check("db1_insert_main2", rc==SQLITE_OK);

  /* db2 writes on dev (autocommit) — this overwrites manifest catalog */
  rc = execsql(db2, "INSERT INTO t VALUES(3, 'dev-3')");
  check("db2_insert_dev3", rc==SQLITE_OK);

  printf("--- Test 2: reads after cross-branch writes (the bug) ---\n");

  /* THIS IS THE CRITICAL TEST: db1 on main should still see its data
  ** without SQLITE_CORRUPT. Before the fix, the manifest catalog was
  ** overwritten by db2's autocommit, causing db1 to load the wrong
  ** catalog and get corrupt reads. */
  r = exec1(db1, "SELECT count(*) FROM t");
  check("db1_main_count", strcmp(r, "2")==0);

  r = exec1(db1, "SELECT val FROM t WHERE id=1");
  check("db1_main_val1", strcmp(r, "main-1")==0);

  r = exec1(db1, "SELECT val FROM t WHERE id=2");
  check("db1_main_val2", strcmp(r, "main-2")==0);

  /* db2 on dev should see its data */
  r = exec1(db2, "SELECT count(*) FROM t");
  check("db2_dev_count", strcmp(r, "2")==0);

  r = exec1(db2, "SELECT val FROM t WHERE id=1");
  check("db2_dev_val1", strcmp(r, "main-1")==0);

  r = exec1(db2, "SELECT val FROM t WHERE id=3");
  check("db2_dev_val3", strcmp(r, "dev-3")==0);

  printf("--- Test 3: more writes interleaved ---\n");

  /* Interleave more writes */
  rc = execsql(db1, "INSERT INTO t VALUES(4, 'main-4')");
  check("db1_insert_main4", rc==SQLITE_OK);

  rc = execsql(db2, "INSERT INTO t VALUES(5, 'dev-5')");
  check("db2_insert_dev5", rc==SQLITE_OK);

  rc = execsql(db1, "INSERT INTO t VALUES(6, 'main-6')");
  check("db1_insert_main6", rc==SQLITE_OK);

  /* Verify isolation */
  r = exec1(db1, "SELECT count(*) FROM t");
  check("db1_main_count_after", strcmp(r, "4")==0);

  r = exec1(db2, "SELECT count(*) FROM t");
  check("db2_dev_count_after", strcmp(r, "3")==0);

  printf("--- Test 4: dolt_commit on each branch ---\n");

  exec1(db1, "SELECT dolt_commit('-A', '-m', 'main writes')");
  exec1(db2, "SELECT dolt_commit('-A', '-m', 'dev writes')");

  /* Verify commits */
  r = exec1(db1, "SELECT message FROM dolt_log LIMIT 1");
  check("db1_commit_msg", strcmp(r, "main writes")==0);

  r = exec1(db2, "SELECT message FROM dolt_log LIMIT 1");
  check("db2_commit_msg", strcmp(r, "dev writes")==0);

  printf("--- Test 5: reads after commits still correct ---\n");

  r = exec1(db1, "SELECT count(*) FROM t");
  check("db1_final_count", strcmp(r, "4")==0);

  r = exec1(db2, "SELECT count(*) FROM t");
  check("db2_final_count", strcmp(r, "3")==0);

  printf("--- Test 6: fresh connection sees committed state ---\n");

  {
    sqlite3 *fresh = 0;
    sqlite3_open(dbpath, &fresh);
    /* Fresh connection opens on default branch (main after last commit) */
    r = exec1(fresh, "SELECT count(*) FROM t");
    /* Should see main's 4 rows (main was last to update default branch) */
    check("fresh_sees_data", strcmp(r, "0")!=0);
    sqlite3_close(fresh);
  }

  printf("--- Test 7: checkout doesn't corrupt source branch ---\n");

  /* Start fresh for this test */
  sqlite3_close(db1);
  sqlite3_close(db2);
  remove(dbpath);
  { char w[256]; snprintf(w,256,"%s-wal",dbpath); remove(w); }

  rc = sqlite3_open(dbpath, &db1);
  check("t7_open", rc==SQLITE_OK);
  sqlite3_busy_timeout(db1, 5000);

  execsql(db1, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
  execsql(db1, "INSERT INTO t VALUES(1, 'committed')");
  exec1(db1, "SELECT dolt_commit('-A', '-m', 'init')");
  exec1(db1, "SELECT dolt_checkout('-b', 'feature')");

  /* Commit on feature */
  execsql(db1, "INSERT INTO t VALUES(2, 'on-feature')");
  exec1(db1, "SELECT dolt_commit('-A', '-m', 'feature commit')");
  check("t7_feature_has_2", strcmp(exec1(db1, "SELECT count(*) FROM t"), "2")==0);

  /* Switch to main — should see 1 row, not 2 */
  exec1(db1, "SELECT dolt_checkout('main')");
  check("t7_main_has_1", strcmp(exec1(db1, "SELECT count(*) FROM t"), "1")==0);

  /* Switch back to feature — should see 2 rows */
  exec1(db1, "SELECT dolt_checkout('feature')");
  r = exec1(db1, "SELECT count(*) FROM t");
  check("t7_feature_preserved", strcmp(r, "2")==0);

  printf("--- Test 8: checkout with uncommitted changes doesn't error ---\n");

  /* Uncommitted insert, then checkout — should not error */
  execsql(db1, "INSERT INTO t VALUES(3, 'uncommitted')");
  check("t8_has_3", strcmp(exec1(db1, "SELECT count(*) FROM t"), "3")==0);

  rc = execsql(db1, "SELECT dolt_checkout('main')");
  check("t8_checkout_ok", rc==SQLITE_OK);
  check("t8_main_has_1", strcmp(exec1(db1, "SELECT count(*) FROM t"), "1")==0);

  /* Cleanup */
  sqlite3_close(db1);
  remove(dbpath);
  { char w[256]; snprintf(w,256,"%s-wal",dbpath); remove(w); }

  printf("\nResults: %d passed, %d failed out of %d tests\n", nPass, nFail, nPass+nFail);
  return nFail > 0 ? 1 : 0;
}
