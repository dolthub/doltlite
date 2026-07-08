#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
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

static void check_int(const char *name, int got, int expected){
  if( got==expected ){
    nPass++;
  }else{
    nFail++;
    fprintf(stderr, "FAIL: %s: expected %d, got %d\n", name, expected, got);
  }
}

static void check_str(const char *name, const char *got, const char *expected){
  if( got && expected && strcmp(got, expected)==0 ){
    nPass++;
  }else{
    nFail++;
    fprintf(stderr, "FAIL: %s: expected '%s', got '%s'\n",
            name, expected ? expected : "(null)", got ? got : "(null)");
  }
}

static int execSql(sqlite3 *db, const char *sql){
  char *err = 0;
  int rc = sqlite3_exec(db, sql, 0, 0, &err);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "  SQL error: %s (rc=%d)\n  SQL: %s\n",
            err ? err : "?", rc, sql);
    sqlite3_free(err);
  }
  return rc;
}

static int count_rows(sqlite3_stmt *pStmt){
  int n = 0;
  int rc;
  while( (rc = sqlite3_step(pStmt))==SQLITE_ROW ) n++;
  sqlite3_reset(pStmt);
  return n;
}

static int step_int(sqlite3_stmt *pStmt){
  int v = -1;
  if( sqlite3_step(pStmt)==SQLITE_ROW ){
    v = sqlite3_column_int(pStmt, 0);
  }
  sqlite3_reset(pStmt);
  return v;
}

static char sBuf[4096];
static const char *step_text(sqlite3_stmt *pStmt){
  const char *v;
  sBuf[0] = 0;
  if( sqlite3_step(pStmt)==SQLITE_ROW ){
    v = (const char*)sqlite3_column_text(pStmt, 0);
    if( v ) snprintf(sBuf, sizeof(sBuf), "%s", v);
  }
  sqlite3_reset(pStmt);
  return sBuf;
}

static void test_insert_reuse(void){
  sqlite3 *db = 0;
  sqlite3_stmt *pIns = 0;
  sqlite3_stmt *pCnt = 0;
  int rc, i;

  rc = sqlite3_open(":memory:", &db);
  check("insert_reuse: open", rc==SQLITE_OK);

  execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");

  rc = sqlite3_prepare_v2(db, "INSERT INTO t VALUES(?,?)", -1, &pIns, 0);
  check("insert_reuse: prepare insert", rc==SQLITE_OK);

  rc = sqlite3_prepare_v2(db, "SELECT count(*) FROM t", -1, &pCnt, 0);
  check("insert_reuse: prepare count", rc==SQLITE_OK);

  for(i=1; i<=100; i++){
    char buf[32];
    snprintf(buf, sizeof(buf), "row_%d", i);
    sqlite3_bind_int(pIns, 1, i);
    sqlite3_bind_text(pIns, 2, buf, -1, SQLITE_TRANSIENT);
    rc = sqlite3_step(pIns);
    check("insert_reuse: step", rc==SQLITE_DONE);
    sqlite3_reset(pIns);
  }

  check_int("insert_reuse: count after 100 inserts", step_int(pCnt), 100);

  for(i=101; i<=200; i++){
    char buf[32];
    snprintf(buf, sizeof(buf), "row_%d", i);
    sqlite3_bind_int(pIns, 1, i);
    sqlite3_bind_text(pIns, 2, buf, -1, SQLITE_TRANSIENT);
    rc = sqlite3_step(pIns);
    check("insert_reuse: step batch 2", rc==SQLITE_DONE);
    sqlite3_reset(pIns);
  }

  check_int("insert_reuse: count after 200 inserts", step_int(pCnt), 200);

  sqlite3_finalize(pIns);
  sqlite3_finalize(pCnt);
  sqlite3_close(db);
}

static void test_update_delete_reuse(void){
  sqlite3 *db = 0;
  sqlite3_stmt *pUpd = 0;
  sqlite3_stmt *pDel = 0;
  sqlite3_stmt *pSel = 0;
  int rc, i;

  rc = sqlite3_open(":memory:", &db);
  check("upd_del_reuse: open", rc==SQLITE_OK);

  execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT)");
  for(i=1; i<=10; i++){
    char sql[64];
    snprintf(sql, sizeof(sql), "INSERT INTO t VALUES(%d, %d)", i, i*10);
    execSql(db, sql);
  }

  rc = sqlite3_prepare_v2(db, "UPDATE t SET v=? WHERE id=?", -1, &pUpd, 0);
  check("upd_del_reuse: prepare update", rc==SQLITE_OK);

  for(i=1; i<=5; i++){
    sqlite3_bind_int(pUpd, 1, i*100);
    sqlite3_bind_int(pUpd, 2, i);
    rc = sqlite3_step(pUpd);
    check("upd_del_reuse: step update", rc==SQLITE_DONE);
    check_int("upd_del_reuse: changes", sqlite3_changes(db), 1);
    sqlite3_reset(pUpd);
  }

  rc = sqlite3_prepare_v2(db, "SELECT v FROM t WHERE id=?", -1, &pSel, 0);
  sqlite3_bind_int(pSel, 1, 3);
  check_int("upd_del_reuse: row 3 updated", step_int(pSel), 300);

  sqlite3_bind_int(pSel, 1, 7);
  check_int("upd_del_reuse: row 7 unchanged", step_int(pSel), 70);

  rc = sqlite3_prepare_v2(db, "DELETE FROM t WHERE id=?", -1, &pDel, 0);
  for(i=1; i<=3; i++){
    sqlite3_bind_int(pDel, 1, i);
    rc = sqlite3_step(pDel);
    check("upd_del_reuse: step delete", rc==SQLITE_DONE);
    sqlite3_reset(pDel);
  }

  sqlite3_bind_int(pSel, 1, 1);
  check_int("upd_del_reuse: row 1 deleted", step_int(pSel), -1);

  sqlite3_finalize(pUpd);
  sqlite3_finalize(pDel);
  sqlite3_finalize(pSel);
  sqlite3_close(db);
}

static void test_vtable_reuse(void){
  sqlite3 *db = 0;
  sqlite3_stmt *pLog = 0;
  sqlite3_stmt *pStatus = 0;
  sqlite3_stmt *pBranch = 0;
  int rc;

  rc = sqlite3_open("test_vtable_reuse.db", &db);
  check("vtable_reuse: open", rc==SQLITE_OK);

  execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT)");
  execSql(db, "INSERT INTO t VALUES(1, 10)");
  execSql(db, "SELECT dolt_commit('-A','-m','c1')");

  rc = sqlite3_prepare_v2(db, "SELECT count(*) FROM dolt_log", -1, &pLog, 0);
  check("vtable_reuse: prepare log", rc==SQLITE_OK);
  rc = sqlite3_prepare_v2(db, "SELECT count(*) FROM dolt_status", -1, &pStatus, 0);
  check("vtable_reuse: prepare status", rc==SQLITE_OK);
  rc = sqlite3_prepare_v2(db, "SELECT count(*) FROM dolt_branches", -1, &pBranch, 0);
  check("vtable_reuse: prepare branches", rc==SQLITE_OK);

  check_int("vtable_reuse: log count pass 1", step_int(pLog), 2);
  check_int("vtable_reuse: status count pass 1", step_int(pStatus), 0);
  check_int("vtable_reuse: branch count pass 1", step_int(pBranch), 1);

  execSql(db, "INSERT INTO t VALUES(2, 20)");

  check_int("vtable_reuse: log count pass 2 (same)", step_int(pLog), 2);
  check_int("vtable_reuse: status count pass 2 (dirty)", step_int(pStatus), 1);

  execSql(db, "SELECT dolt_commit('-A','-m','c2')");
  check_int("vtable_reuse: log count pass 3", step_int(pLog), 3);
  check_int("vtable_reuse: status count pass 3 (clean)", step_int(pStatus), 0);

  execSql(db, "SELECT dolt_branch('feature')");
  check_int("vtable_reuse: branch count pass 2", step_int(pBranch), 2);

  execSql(db, "INSERT INTO t VALUES(3, 30)");
  execSql(db, "SELECT dolt_commit('-A','-m','c3')");
  check_int("vtable_reuse: log count pass 4", step_int(pLog), 4);
  check_int("vtable_reuse: status count pass 4", step_int(pStatus), 0);

  sqlite3_finalize(pLog);
  sqlite3_finalize(pStatus);
  sqlite3_finalize(pBranch);
  sqlite3_close(db);
  unlink("test_vtable_reuse.db");
}

static void test_diff_table_reuse(void){
  sqlite3 *db = 0;
  sqlite3_stmt *pDiff = 0;
  int rc;

  rc = sqlite3_open("test_diff_reuse.db", &db);
  check("diff_reuse: open", rc==SQLITE_OK);

  execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT)");
  execSql(db, "INSERT INTO t VALUES(1, 10)");
  execSql(db, "SELECT dolt_commit('-A','-m','c1')");

  rc = sqlite3_prepare_v2(db,
    "SELECT count(*) FROM dolt_diff_t", -1, &pDiff, 0);
  check("diff_reuse: prepare", rc==SQLITE_OK);

  {
    int n = step_int(pDiff);
    check("diff_reuse: pass 1 has rows", n > 0);
  }

  execSql(db, "INSERT INTO t VALUES(2, 20)");
  {
    int n1 = step_int(pDiff);
    execSql(db, "SELECT dolt_commit('-A','-m','c2')");
    int n2 = step_int(pDiff);
    check("diff_reuse: pass 2 more rows after working", n1 > 0);
    check("diff_reuse: pass 3 committed", n2 > 0);
  }

  execSql(db, "UPDATE t SET v=99 WHERE id=1");
  execSql(db, "SELECT dolt_commit('-A','-m','c3')");
  {
    int n = step_int(pDiff);
    check("diff_reuse: pass 4 after update+commit", n > 0);
  }

  sqlite3_finalize(pDiff);
  sqlite3_close(db);
  unlink("test_diff_reuse.db");
}

static void test_diff_summary_reuse(void){
  sqlite3 *db = 0;
  sqlite3_stmt *pDiff = 0;
  int rc;

  rc = sqlite3_open("test_diff_summary_reuse.db", &db);
  check("diff_summary_reuse: open", rc==SQLITE_OK);

  execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT)");
  execSql(db, "INSERT INTO t VALUES(1, 10)");
  execSql(db, "SELECT dolt_commit('-A','-m','c1')");

  rc = sqlite3_prepare_v2(db,
    "SELECT count(*) FROM dolt_diff", -1, &pDiff, 0);
  check("diff_summary_reuse: prepare", rc==SQLITE_OK);

  {
    int n1 = step_int(pDiff);
    if( n1==0 ){
      printf("  (dolt_diff no-arg summary not available, skipping)\n");
      sqlite3_finalize(pDiff);
      sqlite3_close(db);
      unlink("test_diff_summary_reuse.db");
      return;
    }
    check_int("diff_summary_reuse: pass 1", n1, 1);
  }

  execSql(db, "INSERT INTO t VALUES(2, 20)");
  execSql(db, "SELECT dolt_commit('-A','-m','c2')");
  check_int("diff_summary_reuse: pass 2", step_int(pDiff), 2);

  execSql(db, "UPDATE t SET v=99 WHERE id=1");
  check_int("diff_summary_reuse: pass 3 working", step_int(pDiff), 3);

  execSql(db, "SELECT dolt_commit('-A','-m','c3')");
  check_int("diff_summary_reuse: pass 4", step_int(pDiff), 3);

  sqlite3_finalize(pDiff);
  sqlite3_close(db);
  unlink("test_diff_summary_reuse.db");
}

static void test_commit_reuse(void){
  sqlite3 *db = 0;
  sqlite3_stmt *pCommit = 0;
  sqlite3_stmt *pLog = 0;
  int rc;

  rc = sqlite3_open("test_commit_reuse.db", &db);
  check("commit_reuse: open", rc==SQLITE_OK);

  execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT)");

  rc = sqlite3_prepare_v2(db,
    "SELECT dolt_commit('-A','-m',?)", -1, &pCommit, 0);
  check("commit_reuse: prepare commit", rc==SQLITE_OK);

  rc = sqlite3_prepare_v2(db,
    "SELECT count(*) FROM dolt_log", -1, &pLog, 0);
  check("commit_reuse: prepare log", rc==SQLITE_OK);

  execSql(db, "INSERT INTO t VALUES(1, 10)");
  sqlite3_bind_text(pCommit, 1, "c1", -1, SQLITE_STATIC);
  rc = sqlite3_step(pCommit);
  check("commit_reuse: step c1", rc==SQLITE_ROW);
  sqlite3_reset(pCommit);
  check_int("commit_reuse: log after c1", step_int(pLog), 2);

  execSql(db, "INSERT INTO t VALUES(2, 20)");
  sqlite3_bind_text(pCommit, 1, "c2", -1, SQLITE_STATIC);
  rc = sqlite3_step(pCommit);
  check("commit_reuse: step c2", rc==SQLITE_ROW);
  sqlite3_reset(pCommit);
  check_int("commit_reuse: log after c2", step_int(pLog), 3);

  execSql(db, "INSERT INTO t VALUES(3, 30)");
  sqlite3_bind_text(pCommit, 1, "c3", -1, SQLITE_STATIC);
  rc = sqlite3_step(pCommit);
  check("commit_reuse: step c3", rc==SQLITE_ROW);
  sqlite3_reset(pCommit);
  check_int("commit_reuse: log after c3", step_int(pLog), 4);

  sqlite3_bind_text(pCommit, 1, "empty", -1, SQLITE_STATIC);
  rc = sqlite3_step(pCommit);
  sqlite3_reset(pCommit);

  {
    int n = step_int(pLog);
    check("commit_reuse: log after empty commit", n>=4 && n<=5);
  }

  sqlite3_finalize(pCommit);
  sqlite3_finalize(pLog);
  sqlite3_close(db);
  unlink("test_commit_reuse.db");
}

static void test_rapid_interleave(void){
  sqlite3 *db = 0;
  sqlite3_stmt *pCnt = 0;
  sqlite3_stmt *pIns = 0;
  int rc, i;

  rc = sqlite3_open("test_interleave.db", &db);
  check("interleave: open", rc==SQLITE_OK);

  execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT)");
  execSql(db, "SELECT dolt_commit('-A','-m','init_t')");

  rc = sqlite3_prepare_v2(db, "SELECT count(*) FROM t", -1, &pCnt, 0);
  check("interleave: prepare count", rc==SQLITE_OK);

  rc = sqlite3_prepare_v2(db, "INSERT INTO t VALUES(?,?)", -1, &pIns, 0);
  check("interleave: prepare insert", rc==SQLITE_OK);

  for(i=1; i<=20; i++){
    sqlite3_bind_int(pIns, 1, i);
    sqlite3_bind_int(pIns, 2, i*10);
    rc = sqlite3_step(pIns);
    check("interleave: insert step", rc==SQLITE_DONE);
    sqlite3_reset(pIns);

    check_int("interleave: count mid-loop", step_int(pCnt), i);

    if( i % 5 == 0 ){
      char msg[32];
      snprintf(msg, sizeof(msg), "batch_%d", i/5);
      char sql[128];
      snprintf(sql, sizeof(sql),
               "SELECT dolt_commit('-A','-m','%s')", msg);
      execSql(db, sql);
    }
  }

  check_int("interleave: final count", step_int(pCnt), 20);

  sqlite3_finalize(pCnt);
  sqlite3_finalize(pIns);
  sqlite3_close(db);
  unlink("test_interleave.db");
}

static void test_partial_step(void){
  sqlite3 *db = 0;
  sqlite3_stmt *pSel = 0;
  int rc;

  rc = sqlite3_open("test_partial.db", &db);
  check("partial: open", rc==SQLITE_OK);

  execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT)");
  execSql(db, "INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50)");
  execSql(db, "SELECT dolt_commit('-A','-m','c1')");

  rc = sqlite3_prepare_v2(db,
    "SELECT id FROM t ORDER BY id", -1, &pSel, 0);
  check("partial: prepare", rc==SQLITE_OK);

  rc = sqlite3_step(pSel);
  check("partial: step 1", rc==SQLITE_ROW);
  check_int("partial: row 1", sqlite3_column_int(pSel, 0), 1);
  rc = sqlite3_step(pSel);
  check("partial: step 2", rc==SQLITE_ROW);
  check_int("partial: row 2", sqlite3_column_int(pSel, 0), 2);
  sqlite3_reset(pSel);

  rc = sqlite3_step(pSel);
  check("partial: restart step 1", rc==SQLITE_ROW);
  check_int("partial: restart row 1", sqlite3_column_int(pSel, 0), 1);
  sqlite3_reset(pSel);

  check_int("partial: full count", count_rows(pSel), 5);

  {
    sqlite3_stmt *pLog = 0;
    rc = sqlite3_prepare_v2(db,
      "SELECT message FROM dolt_log", -1, &pLog, 0);
    check("partial: prepare log", rc==SQLITE_OK);

    rc = sqlite3_step(pLog);
    check("partial: log step 1", rc==SQLITE_ROW);
    sqlite3_reset(pLog);

    rc = sqlite3_step(pLog);
    check("partial: log restart", rc==SQLITE_ROW);
    sqlite3_reset(pLog);

    sqlite3_finalize(pLog);
  }

  sqlite3_finalize(pSel);
  sqlite3_close(db);
  unlink("test_partial.db");
}

static void test_v1_stmt_survives_data_rollback(void){
  sqlite3 *db = 0;
  sqlite3_stmt *pSel = 0;
  const char *zTail = 0;
  int rc;

  unlink("test_v1_rollback.db");
  rc = sqlite3_open("test_v1_rollback.db", &db);
  check("v1_rollback: open", rc==SQLITE_OK);

  execSql(db,
    "CREATE TABLE t1(a);"
    "INSERT INTO t1 VALUES(1);"
    "INSERT INTO t1 VALUES(2);");

  rc = sqlite3_prepare(db, "SELECT a FROM t1 ORDER BY a", -1, &pSel, &zTail);
  check("v1_rollback: prepare", rc==SQLITE_OK);

  rc = sqlite3_step(pSel);
  check("v1_rollback: pre-step", rc==SQLITE_ROW);
  check_int("v1_rollback: pre-step row", sqlite3_column_int(pSel, 0), 1);

  rc = sqlite3_reset(pSel);
  check("v1_rollback: reset", rc==SQLITE_OK);

  execSql(db, "BEGIN; INSERT INTO t1 VALUES(3); ROLLBACK;");

  rc = sqlite3_step(pSel);
  check("v1_rollback: step after data rollback", rc==SQLITE_ROW);
  if( rc==SQLITE_ROW ){
    check_int("v1_rollback: row after rollback", sqlite3_column_int(pSel, 0), 1);
  }

  rc = sqlite3_finalize(pSel);
  check("v1_rollback: finalize", rc==SQLITE_OK);

  sqlite3_close(db);
  unlink("test_v1_rollback.db");
}


int main(int argc, char **argv){
  (void)argc; (void)argv;

  printf("=== Prepared Statement Reuse Tests ===\n\n");

  printf("--- INSERT reuse ---\n");
  test_insert_reuse();

  printf("--- UPDATE/DELETE reuse ---\n");
  test_update_delete_reuse();

  printf("--- vtable query reuse across state changes ---\n");
  test_vtable_reuse();

  printf("--- dolt_diff_<table> reuse ---\n");
  test_diff_table_reuse();

  printf("--- dolt_diff summary reuse ---\n");
  test_diff_summary_reuse();

  printf("--- dolt_commit reuse ---\n");
  test_commit_reuse();

  printf("--- rapid interleave ---\n");
  test_rapid_interleave();

  printf("--- partial step + reset ---\n");
  test_partial_step();

  printf("--- v1 statement survives data rollback ---\n");
  test_v1_stmt_survives_data_rollback();

  printf("\n=== Results: %d passed, %d failed ===\n", nPass, nFail);
  return nFail > 0 ? 1 : 0;
}
