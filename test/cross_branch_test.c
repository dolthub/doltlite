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
static const char *queryScalarText(sqlite3 *db, const char *sql){
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

static int execSql(sqlite3 *db, const char *sql){
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

  rc = sqlite3_open(dbpath, &db1);
  check("open_db1", rc==SQLITE_OK);
  sqlite3_busy_timeout(db1, 5000);

  printf("--- Setup: create table and initial commit on main ---\n");

  rc = execSql(db1, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
  check("create_table", rc==SQLITE_OK);
  rc = execSql(db1, "INSERT INTO t VALUES(1, 'main-1')");
  check("insert_main", rc==SQLITE_OK);
  queryScalarText(db1, "SELECT dolt_commit('-A', '-m', 'initial on main')");

  rc = sqlite3_open(dbpath, &db2);
  check("open_db2", rc==SQLITE_OK);
  sqlite3_busy_timeout(db2, 5000);

  check("db1_sees_main", strcmp(queryScalarText(db1, "SELECT val FROM t WHERE id=1"), "main-1")==0);
  check("db2_sees_main", strcmp(queryScalarText(db2, "SELECT val FROM t WHERE id=1"), "main-1")==0);

  printf("--- Create dev branch and checkout db2 to dev ---\n");

  queryScalarText(db2, "SELECT dolt_checkout('-b', 'dev')");
  check("db2_on_dev", strcmp(queryScalarText(db2, "SELECT active_branch()"), "dev")==0);
  check("db1_on_main", strcmp(queryScalarText(db1, "SELECT active_branch()"), "main")==0);

  printf("--- Test 1: autocommit writes on different branches ---\n");

  rc = execSql(db1, "INSERT INTO t VALUES(2, 'main-2')");
  check("db1_insert_main2", rc==SQLITE_OK);

  rc = execSql(db2, "INSERT INTO t VALUES(3, 'dev-3')");
  check("db2_insert_dev3", rc==SQLITE_OK);

  printf("--- Test 2: reads after cross-branch writes (the bug) ---\n");

  r = queryScalarText(db1, "SELECT count(*) FROM t");
  check("db1_main_count", strcmp(r, "2")==0);

  r = queryScalarText(db1, "SELECT val FROM t WHERE id=1");
  check("db1_main_val1", strcmp(r, "main-1")==0);

  r = queryScalarText(db1, "SELECT val FROM t WHERE id=2");
  check("db1_main_val2", strcmp(r, "main-2")==0);

  r = queryScalarText(db2, "SELECT count(*) FROM t");
  check("db2_dev_count", strcmp(r, "2")==0);

  r = queryScalarText(db2, "SELECT val FROM t WHERE id=1");
  check("db2_dev_val1", strcmp(r, "main-1")==0);

  r = queryScalarText(db2, "SELECT val FROM t WHERE id=3");
  check("db2_dev_val3", strcmp(r, "dev-3")==0);

  printf("--- Test 3: more writes interleaved ---\n");

  rc = execSql(db1, "INSERT INTO t VALUES(4, 'main-4')");
  check("db1_insert_main4", rc==SQLITE_OK);

  rc = execSql(db2, "INSERT INTO t VALUES(5, 'dev-5')");
  check("db2_insert_dev5", rc==SQLITE_OK);

  rc = execSql(db1, "INSERT INTO t VALUES(6, 'main-6')");
  check("db1_insert_main6", rc==SQLITE_OK);

  r = queryScalarText(db1, "SELECT count(*) FROM t");
  check("db1_main_count_after", strcmp(r, "4")==0);

  r = queryScalarText(db2, "SELECT count(*) FROM t");
  check("db2_dev_count_after", strcmp(r, "3")==0);

  printf("--- Test 4: dolt_commit on each branch ---\n");

  queryScalarText(db1, "SELECT dolt_commit('-A', '-m', 'main writes')");
  queryScalarText(db2, "SELECT dolt_commit('-A', '-m', 'dev writes')");

  r = queryScalarText(db1, "SELECT message FROM dolt_log LIMIT 1");
  check("db1_commit_msg", strcmp(r, "main writes")==0);

  r = queryScalarText(db2, "SELECT message FROM dolt_log LIMIT 1");
  check("db2_commit_msg", strcmp(r, "dev writes")==0);

  printf("--- Test 5: reads after commits still correct ---\n");

  r = queryScalarText(db1, "SELECT count(*) FROM t");
  check("db1_final_count", strcmp(r, "4")==0);

  r = queryScalarText(db2, "SELECT count(*) FROM t");
  check("db2_final_count", strcmp(r, "3")==0);

  printf("--- Test 6: fresh connection sees committed state ---\n");

  {
    sqlite3 *fresh = 0;
    sqlite3_open(dbpath, &fresh);
    r = queryScalarText(fresh, "SELECT count(*) FROM t");
    check("fresh_sees_data", strcmp(r, "0")!=0);
    sqlite3_close(fresh);
  }

  printf("--- Test 7: checkout doesn't corrupt source branch ---\n");

  sqlite3_close(db1);
  sqlite3_close(db2);
  remove(dbpath);
  { char w[256]; snprintf(w,256,"%s-wal",dbpath); remove(w); }

  rc = sqlite3_open(dbpath, &db1);
  check("t7_open", rc==SQLITE_OK);
  sqlite3_busy_timeout(db1, 5000);

  execSql(db1, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
  execSql(db1, "INSERT INTO t VALUES(1, 'committed')");
  queryScalarText(db1, "SELECT dolt_commit('-A', '-m', 'init')");
  queryScalarText(db1, "SELECT dolt_checkout('-b', 'feature')");

  execSql(db1, "INSERT INTO t VALUES(2, 'on-feature')");
  queryScalarText(db1, "SELECT dolt_commit('-A', '-m', 'feature commit')");
  check("t7_feature_has_2", strcmp(queryScalarText(db1, "SELECT count(*) FROM t"), "2")==0);

  queryScalarText(db1, "SELECT dolt_checkout('main')");
  check("t7_main_has_1", strcmp(queryScalarText(db1, "SELECT count(*) FROM t"), "1")==0);

  queryScalarText(db1, "SELECT dolt_checkout('feature')");
  r = queryScalarText(db1, "SELECT count(*) FROM t");
  check("t7_feature_preserved", strcmp(r, "2")==0);

  printf("--- Test 8: uncommitted changes survive checkout roundtrip ---\n");

  execSql(db1, "INSERT INTO t VALUES(3, 'uncommitted')");
  check("t8_has_3", strcmp(queryScalarText(db1, "SELECT count(*) FROM t"), "3")==0);

  rc = execSql(db1, "SELECT dolt_checkout('main')");
  check("t8_checkout_ok", rc==SQLITE_OK);
  check("t8_main_has_1", strcmp(queryScalarText(db1, "SELECT count(*) FROM t"), "1")==0);

  queryScalarText(db1, "SELECT dolt_checkout('feature')");
  r = queryScalarText(db1, "SELECT count(*) FROM t");
  check("t8_uncommitted_survives", strcmp(r, "3")==0);
  r = queryScalarText(db1, "SELECT val FROM t WHERE id=3");
  check("t8_uncommitted_val", strcmp(r, "uncommitted")==0);

  printf("--- Test 9: branch deletion ---\n");

  sqlite3_close(db1);
  remove(dbpath);
  { char w[256]; snprintf(w,256,"%s-wal",dbpath); remove(w); }

  rc = sqlite3_open(dbpath, &db1);
  check("t9_open", rc==SQLITE_OK);
  sqlite3_busy_timeout(db1, 5000);

  execSql(db1, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
  execSql(db1, "INSERT INTO t VALUES(1, 'main-data')");
  queryScalarText(db1, "SELECT dolt_commit('-A', '-m', 'init main')");

  queryScalarText(db1, "SELECT dolt_checkout('-b', 'doomed')");
  execSql(db1, "INSERT INTO t VALUES(2, 'doomed-data')");
  queryScalarText(db1, "SELECT dolt_commit('-A', '-m', 'doomed commit')");
  check("t9_doomed_has_2", strcmp(queryScalarText(db1, "SELECT count(*) FROM t"), "2")==0);

  queryScalarText(db1, "SELECT dolt_checkout('main')");
  check("t9_main_has_1", strcmp(queryScalarText(db1, "SELECT count(*) FROM t"), "1")==0);

  queryScalarText(db1, "SELECT dolt_branch('-D', 'doomed')");

  r = queryScalarText(db1, "SELECT count(*) FROM dolt_branches WHERE name='doomed'");
  check("t9_branch_gone", strcmp(r, "0")==0);

  r = queryScalarText(db1, "SELECT count(*) FROM dolt_branches");
  check("t9_only_main", strcmp(r, "1")==0);

  check("t9_main_intact", strcmp(queryScalarText(db1, "SELECT val FROM t WHERE id=1"), "main-data")==0);

  printf("--- Test 10: branch deletion + GC reclaims space ---\n");

  {
    long sizeBefore, sizeAfter;
    FILE *f;

    queryScalarText(db1, "SELECT dolt_checkout('-b', 'bigbranch')");
    {
      int i;
      for(i=100; i<200; i++){
        char sql[128];
        snprintf(sql, sizeof(sql), "INSERT INTO t VALUES(%d, 'bulk-%d')", i, i);
        execSql(db1, sql);
      }
    }
    queryScalarText(db1, "SELECT dolt_commit('-A', '-m', 'bulk data')");
    queryScalarText(db1, "SELECT dolt_checkout('main')");

    sqlite3_close(db1);
    f = fopen(dbpath, "rb");
    if(f){ fseek(f,0,SEEK_END); sizeBefore=ftell(f); fclose(f); }
    else sizeBefore=0;

    rc = sqlite3_open(dbpath, &db1);
    check("t10_reopen", rc==SQLITE_OK);
    sqlite3_busy_timeout(db1, 5000);

    queryScalarText(db1, "SELECT dolt_branch('-D', 'bigbranch')");
    queryScalarText(db1, "SELECT dolt_gc()");

    sqlite3_close(db1);
    f = fopen(dbpath, "rb");
    if(f){ fseek(f,0,SEEK_END); sizeAfter=ftell(f); fclose(f); }
    else sizeAfter=0;

    check("t10_gc_shrinks", sizeAfter < sizeBefore);

    rc = sqlite3_open(dbpath, &db1);
    check("t10_reopen2", rc==SQLITE_OK);
    sqlite3_busy_timeout(db1, 5000);
    check("t10_main_survives", strcmp(queryScalarText(db1, "SELECT val FROM t WHERE id=1"), "main-data")==0);
  }

  sqlite3_close(db1);
  remove(dbpath);
  { char w[256]; snprintf(w,256,"%s-wal",dbpath); remove(w); }

  printf("\nResults: %d passed, %d failed out of %d tests\n", nPass, nFail, nPass+nFail);
  return nFail > 0 ? 1 : 0;
}
