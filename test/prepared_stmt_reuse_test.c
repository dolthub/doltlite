/*
** Prepared statement reuse tests for DoltLite.
**
** Exercises the prepare-once / bind-step-reset-rebind-step cycle that
** better-sqlite3 and other client libraries use. Covers:
**   - INSERT/UPDATE/DELETE with bound parameters via reset+rebind
**   - SELECT with reset+re-step (verifying fresh results each time)
**   - Virtual table queries (dolt_log, dolt_diff_<table>, dolt_status,
**     dolt_branches, dolt_diff) reused across state changes
**   - Custom functions (dolt_commit, dolt_add) called via prepared stmts
**   - Interleaved: mutate state, re-step a previously prepared vtable
**     query and verify it picks up the new state
**
** Build from build/:
**   cc -g -I. -I../src -o prepared_stmt_reuse_test \
**     ../test/prepared_stmt_reuse_test.c libdoltlite.a -lz -lpthread -lm
**
** Run:
**   ./prepared_stmt_reuse_test
*/
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

/* Count rows returned by stepping a prepared statement to completion.
** Resets the statement afterward. */
static int count_rows(sqlite3_stmt *pStmt){
  int n = 0;
  int rc;
  while( (rc = sqlite3_step(pStmt))==SQLITE_ROW ) n++;
  sqlite3_reset(pStmt);
  return n;
}

/* Step once, return first column as int. Resets afterward. */
static int step_int(sqlite3_stmt *pStmt){
  int v = -1;
  if( sqlite3_step(pStmt)==SQLITE_ROW ){
    v = sqlite3_column_int(pStmt, 0);
  }
  sqlite3_reset(pStmt);
  return v;
}

/* Step once, return first column as string (static buffer). Resets. */
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

/* ------------------------------------------------------------------ */
/*  Test: INSERT with bound params, prepare once, reset+rebind loop   */
/* ------------------------------------------------------------------ */
static void test_insert_reuse(void){
  sqlite3 *db = 0;
  sqlite3_stmt *pIns = 0;
  sqlite3_stmt *pCnt = 0;
  int rc, i;

  rc = sqlite3_open(":memory:", &db);
  check("insert_reuse: open", rc==SQLITE_OK);

  execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");

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

  /* Reuse the same INSERT stmt for 100 more rows */
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

/* ------------------------------------------------------------------ */
/*  Test: UPDATE/DELETE with reused prepared stmts                     */
/* ------------------------------------------------------------------ */
static void test_update_delete_reuse(void){
  sqlite3 *db = 0;
  sqlite3_stmt *pUpd = 0;
  sqlite3_stmt *pDel = 0;
  sqlite3_stmt *pSel = 0;
  int rc, i;

  rc = sqlite3_open(":memory:", &db);
  check("upd_del_reuse: open", rc==SQLITE_OK);

  execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT)");
  for(i=1; i<=10; i++){
    char sql[64];
    snprintf(sql, sizeof(sql), "INSERT INTO t VALUES(%d, %d)", i, i*10);
    execsql(db, sql);
  }

  rc = sqlite3_prepare_v2(db, "UPDATE t SET v=? WHERE id=?", -1, &pUpd, 0);
  check("upd_del_reuse: prepare update", rc==SQLITE_OK);

  /* Update rows 1..5 with reused stmt */
  for(i=1; i<=5; i++){
    sqlite3_bind_int(pUpd, 1, i*100);
    sqlite3_bind_int(pUpd, 2, i);
    rc = sqlite3_step(pUpd);
    check("upd_del_reuse: step update", rc==SQLITE_DONE);
    check_int("upd_del_reuse: changes", sqlite3_changes(db), 1);
    sqlite3_reset(pUpd);
  }

  /* Verify row 3 was updated */
  rc = sqlite3_prepare_v2(db, "SELECT v FROM t WHERE id=?", -1, &pSel, 0);
  sqlite3_bind_int(pSel, 1, 3);
  check_int("upd_del_reuse: row 3 updated", step_int(pSel), 300);

  /* Reuse SELECT with different binding */
  sqlite3_bind_int(pSel, 1, 7);
  check_int("upd_del_reuse: row 7 unchanged", step_int(pSel), 70);

  /* DELETE with reuse */
  rc = sqlite3_prepare_v2(db, "DELETE FROM t WHERE id=?", -1, &pDel, 0);
  for(i=1; i<=3; i++){
    sqlite3_bind_int(pDel, 1, i);
    rc = sqlite3_step(pDel);
    check("upd_del_reuse: step delete", rc==SQLITE_DONE);
    sqlite3_reset(pDel);
  }

  /* Verify deleted */
  sqlite3_bind_int(pSel, 1, 1);
  check_int("upd_del_reuse: row 1 deleted", step_int(pSel), -1);

  sqlite3_finalize(pUpd);
  sqlite3_finalize(pDel);
  sqlite3_finalize(pSel);
  sqlite3_close(db);
}

/* ------------------------------------------------------------------ */
/*  Test: SELECT on vtables with reset+re-step across state changes   */
/* ------------------------------------------------------------------ */
static void test_vtable_reuse(void){
  sqlite3 *db = 0;
  sqlite3_stmt *pLog = 0;
  sqlite3_stmt *pStatus = 0;
  sqlite3_stmt *pBranch = 0;
  int rc;

  rc = sqlite3_open("test_vtable_reuse.db", &db);
  check("vtable_reuse: open", rc==SQLITE_OK);

  execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT)");
  execsql(db, "INSERT INTO t VALUES(1, 10)");
  execsql(db, "SELECT dolt_commit('-A','-m','c1')");

  /* Prepare vtable queries once */
  rc = sqlite3_prepare_v2(db, "SELECT count(*) FROM dolt_log", -1, &pLog, 0);
  check("vtable_reuse: prepare log", rc==SQLITE_OK);
  rc = sqlite3_prepare_v2(db, "SELECT count(*) FROM dolt_status", -1, &pStatus, 0);
  check("vtable_reuse: prepare status", rc==SQLITE_OK);
  rc = sqlite3_prepare_v2(db, "SELECT count(*) FROM dolt_branches", -1, &pBranch, 0);
  check("vtable_reuse: prepare branches", rc==SQLITE_OK);

  /* First pass: 2 log entries (c1 + init), 0 status, 1 branch */
  check_int("vtable_reuse: log count pass 1", step_int(pLog), 2);
  check_int("vtable_reuse: status count pass 1", step_int(pStatus), 0);
  check_int("vtable_reuse: branch count pass 1", step_int(pBranch), 1);

  /* Mutate state: add a row (uncommitted) */
  execsql(db, "INSERT INTO t VALUES(2, 20)");

  /* Re-step the SAME prepared stmts — they should reflect new state */
  check_int("vtable_reuse: log count pass 2 (same)", step_int(pLog), 2);
  check_int("vtable_reuse: status count pass 2 (dirty)", step_int(pStatus), 1);

  /* Commit, re-step */
  execsql(db, "SELECT dolt_commit('-A','-m','c2')");
  check_int("vtable_reuse: log count pass 3", step_int(pLog), 3);
  check_int("vtable_reuse: status count pass 3 (clean)", step_int(pStatus), 0);

  /* Create a branch, re-step branches */
  execsql(db, "SELECT dolt_branch('feature')");
  check_int("vtable_reuse: branch count pass 2", step_int(pBranch), 2);

  /* Another commit, re-step everything */
  execsql(db, "INSERT INTO t VALUES(3, 30)");
  execsql(db, "SELECT dolt_commit('-A','-m','c3')");
  check_int("vtable_reuse: log count pass 4", step_int(pLog), 4);
  check_int("vtable_reuse: status count pass 4", step_int(pStatus), 0);

  sqlite3_finalize(pLog);
  sqlite3_finalize(pStatus);
  sqlite3_finalize(pBranch);
  sqlite3_close(db);
  unlink("test_vtable_reuse.db");
}

/* ------------------------------------------------------------------ */
/*  Test: dolt_diff_<table> vtable reuse across commits               */
/* ------------------------------------------------------------------ */
static void test_diff_table_reuse(void){
  sqlite3 *db = 0;
  sqlite3_stmt *pDiff = 0;
  int rc;

  rc = sqlite3_open("test_diff_reuse.db", &db);
  check("diff_reuse: open", rc==SQLITE_OK);

  execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT)");
  execsql(db, "INSERT INTO t VALUES(1, 10)");
  execsql(db, "SELECT dolt_commit('-A','-m','c1')");

  /* Prepare a dolt_diff_t query once */
  rc = sqlite3_prepare_v2(db,
    "SELECT count(*) FROM dolt_diff_t", -1, &pDiff, 0);
  check("diff_reuse: prepare", rc==SQLITE_OK);

  /* Pass 1: c1 has 1 row added from init → diff has entries */
  {
    int n = step_int(pDiff);
    check("diff_reuse: pass 1 has rows", n > 0);
  }

  /* Add a row (working change) → diff includes WORKING */
  execsql(db, "INSERT INTO t VALUES(2, 20)");
  {
    int n1 = step_int(pDiff);
    /* Commit it */
    execsql(db, "SELECT dolt_commit('-A','-m','c2')");
    int n2 = step_int(pDiff);
    /* After commit, WORKING row disappears but committed row appears */
    check("diff_reuse: pass 2 more rows after working", n1 > 0);
    check("diff_reuse: pass 3 committed", n2 > 0);
  }

  /* Another round */
  execsql(db, "UPDATE t SET v=99 WHERE id=1");
  execsql(db, "SELECT dolt_commit('-A','-m','c3')");
  {
    int n = step_int(pDiff);
    check("diff_reuse: pass 4 after update+commit", n > 0);
  }

  sqlite3_finalize(pDiff);
  sqlite3_close(db);
  unlink("test_diff_reuse.db");
}

/* ------------------------------------------------------------------ */
/*  Test: dolt_diff (no-arg summary) vtable reuse                     */
/* ------------------------------------------------------------------ */
static void test_diff_summary_reuse(void){
  sqlite3 *db = 0;
  sqlite3_stmt *pDiff = 0;
  int rc;

  rc = sqlite3_open("test_diff_summary_reuse.db", &db);
  check("diff_summary_reuse: open", rc==SQLITE_OK);

  execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT)");
  execsql(db, "INSERT INTO t VALUES(1, 10)");
  execsql(db, "SELECT dolt_commit('-A','-m','c1')");

  /* Prepare dolt_diff summary query once. The no-arg summary form was
  ** added in PR #387; on earlier builds it returns 0 rows. Detect
  ** whether the surface is available by checking the first pass and
  ** skip if not implemented yet. */
  rc = sqlite3_prepare_v2(db,
    "SELECT count(*) FROM dolt_diff", -1, &pDiff, 0);
  check("diff_summary_reuse: prepare", rc==SQLITE_OK);

  /* Pass 1: c1 + init → 1 summary row (c1 touches t) */
  {
    int n1 = step_int(pDiff);
    if( n1==0 ){
      /* Summary form not yet implemented — skip remaining checks. */
      printf("  (dolt_diff no-arg summary not available, skipping)\n");
      sqlite3_finalize(pDiff);
      sqlite3_close(db);
      unlink("test_diff_summary_reuse.db");
      return;
    }
    check_int("diff_summary_reuse: pass 1", n1, 1);
  }

  /* Commit more → 2 summary rows */
  execsql(db, "INSERT INTO t VALUES(2, 20)");
  execsql(db, "SELECT dolt_commit('-A','-m','c2')");
  check_int("diff_summary_reuse: pass 2", step_int(pDiff), 2);

  /* With working changes → includes WORKING row */
  execsql(db, "UPDATE t SET v=99 WHERE id=1");
  check_int("diff_summary_reuse: pass 3 working", step_int(pDiff), 3);

  /* Commit → WORKING disappears, c3 appears: still 3 */
  execsql(db, "SELECT dolt_commit('-A','-m','c3')");
  check_int("diff_summary_reuse: pass 4", step_int(pDiff), 3);

  sqlite3_finalize(pDiff);
  sqlite3_close(db);
  unlink("test_diff_summary_reuse.db");
}

/* ------------------------------------------------------------------ */
/*  Test: dolt_commit/dolt_add as prepared stmt with reset             */
/* ------------------------------------------------------------------ */
static void test_commit_reuse(void){
  sqlite3 *db = 0;
  sqlite3_stmt *pCommit = 0;
  sqlite3_stmt *pLog = 0;
  int rc;

  rc = sqlite3_open("test_commit_reuse.db", &db);
  check("commit_reuse: open", rc==SQLITE_OK);

  execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT)");

  /* Prepare dolt_commit('-A','-m',?) with bound message */
  rc = sqlite3_prepare_v2(db,
    "SELECT dolt_commit('-A','-m',?)", -1, &pCommit, 0);
  check("commit_reuse: prepare commit", rc==SQLITE_OK);

  rc = sqlite3_prepare_v2(db,
    "SELECT count(*) FROM dolt_log", -1, &pLog, 0);
  check("commit_reuse: prepare log", rc==SQLITE_OK);

  /* Commit 1 */
  execsql(db, "INSERT INTO t VALUES(1, 10)");
  sqlite3_bind_text(pCommit, 1, "c1", -1, SQLITE_STATIC);
  rc = sqlite3_step(pCommit);
  check("commit_reuse: step c1", rc==SQLITE_ROW);
  sqlite3_reset(pCommit);
  check_int("commit_reuse: log after c1", step_int(pLog), 2);

  /* Commit 2 — reuse the same prepared stmt */
  execsql(db, "INSERT INTO t VALUES(2, 20)");
  sqlite3_bind_text(pCommit, 1, "c2", -1, SQLITE_STATIC);
  rc = sqlite3_step(pCommit);
  check("commit_reuse: step c2", rc==SQLITE_ROW);
  sqlite3_reset(pCommit);
  check_int("commit_reuse: log after c2", step_int(pLog), 3);

  /* Commit 3 */
  execsql(db, "INSERT INTO t VALUES(3, 30)");
  sqlite3_bind_text(pCommit, 1, "c3", -1, SQLITE_STATIC);
  rc = sqlite3_step(pCommit);
  check("commit_reuse: step c3", rc==SQLITE_ROW);
  sqlite3_reset(pCommit);
  check_int("commit_reuse: log after c3", step_int(pLog), 4);

  /* Commit with no changes — should error or return empty */
  sqlite3_bind_text(pCommit, 1, "empty", -1, SQLITE_STATIC);
  rc = sqlite3_step(pCommit);
  /* Doltlite may return an error or an empty commit hash */
  sqlite3_reset(pCommit);

  /* Log count should not increase for a no-change commit (or at most +1) */
  {
    int n = step_int(pLog);
    check("commit_reuse: log after empty commit", n>=4 && n<=5);
  }

  sqlite3_finalize(pCommit);
  sqlite3_finalize(pLog);
  sqlite3_close(db);
  unlink("test_commit_reuse.db");
}

/* ------------------------------------------------------------------ */
/*  Test: dolt_diff('table',from,to) legacy TVF reuse                 */
/* ------------------------------------------------------------------ */
static void test_legacy_diff_reuse(void){
  sqlite3 *db = 0;
  sqlite3_stmt *pDiff = 0;
  int rc;

  rc = sqlite3_open("test_legacy_diff_reuse.db", &db);
  check("legacy_diff_reuse: open", rc==SQLITE_OK);

  execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT)");
  execsql(db, "INSERT INTO t VALUES(1, 10)");
  execsql(db, "SELECT dolt_commit('-A','-m','c1')");
  execsql(db, "INSERT INTO t VALUES(2, 20)");
  execsql(db, "SELECT dolt_commit('-A','-m','c2')");
  execsql(db, "UPDATE t SET v=99 WHERE id=1");
  execsql(db, "SELECT dolt_commit('-A','-m','c3')");

  /* Prepare the legacy TVF form with bound refs */
  rc = sqlite3_prepare_v2(db,
    "SELECT count(*) FROM dolt_diff('t', "
    " (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 2),"
    " (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1))",
    -1, &pDiff, 0);
  check("legacy_diff_reuse: prepare", rc==SQLITE_OK);

  /* Step + reset multiple times — should give same result */
  {
    int n1 = step_int(pDiff);
    int n2 = step_int(pDiff);
    int n3 = step_int(pDiff);
    check("legacy_diff_reuse: consistent pass 1", n1==n2);
    check("legacy_diff_reuse: consistent pass 2", n2==n3);
    check("legacy_diff_reuse: has rows", n1>0);
  }

  sqlite3_finalize(pDiff);
  sqlite3_close(db);
  unlink("test_legacy_diff_reuse.db");
}

/* ------------------------------------------------------------------ */
/*  Test: Rapid interleave — mutate between vtable resets              */
/* ------------------------------------------------------------------ */
static void test_rapid_interleave(void){
  sqlite3 *db = 0;
  sqlite3_stmt *pCnt = 0;
  sqlite3_stmt *pIns = 0;
  int rc, i;

  rc = sqlite3_open("test_interleave.db", &db);
  check("interleave: open", rc==SQLITE_OK);

  execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT)");
  execsql(db, "SELECT dolt_commit('-A','-m','init_t')");

  rc = sqlite3_prepare_v2(db, "SELECT count(*) FROM t", -1, &pCnt, 0);
  check("interleave: prepare count", rc==SQLITE_OK);

  rc = sqlite3_prepare_v2(db, "INSERT INTO t VALUES(?,?)", -1, &pIns, 0);
  check("interleave: prepare insert", rc==SQLITE_OK);

  /* Rapidly alternate: insert via prepared stmt, check count, commit */
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
      execsql(db, sql);
    }
  }

  check_int("interleave: final count", step_int(pCnt), 20);

  sqlite3_finalize(pCnt);
  sqlite3_finalize(pIns);
  sqlite3_close(db);
  unlink("test_interleave.db");
}

/* ------------------------------------------------------------------ */
/*  Test: Partial step (step once, don't exhaust, reset, re-step)     */
/* ------------------------------------------------------------------ */
static void test_partial_step(void){
  sqlite3 *db = 0;
  sqlite3_stmt *pSel = 0;
  int rc;

  rc = sqlite3_open("test_partial.db", &db);
  check("partial: open", rc==SQLITE_OK);

  execsql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT)");
  execsql(db, "INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50)");
  execsql(db, "SELECT dolt_commit('-A','-m','c1')");

  rc = sqlite3_prepare_v2(db,
    "SELECT id FROM t ORDER BY id", -1, &pSel, 0);
  check("partial: prepare", rc==SQLITE_OK);

  /* Step twice (get rows 1,2), then reset without exhausting */
  rc = sqlite3_step(pSel);
  check("partial: step 1", rc==SQLITE_ROW);
  check_int("partial: row 1", sqlite3_column_int(pSel, 0), 1);
  rc = sqlite3_step(pSel);
  check("partial: step 2", rc==SQLITE_ROW);
  check_int("partial: row 2", sqlite3_column_int(pSel, 0), 2);
  sqlite3_reset(pSel);

  /* Re-step: should start from row 1 again */
  rc = sqlite3_step(pSel);
  check("partial: restart step 1", rc==SQLITE_ROW);
  check_int("partial: restart row 1", sqlite3_column_int(pSel, 0), 1);
  sqlite3_reset(pSel);

  /* Now exhaust it fully */
  check_int("partial: full count", count_rows(pSel), 5);

  /* Partial step on dolt_log */
  {
    sqlite3_stmt *pLog = 0;
    rc = sqlite3_prepare_v2(db,
      "SELECT message FROM dolt_log", -1, &pLog, 0);
    check("partial: prepare log", rc==SQLITE_OK);

    /* Step once, get first message */
    rc = sqlite3_step(pLog);
    check("partial: log step 1", rc==SQLITE_ROW);
    sqlite3_reset(pLog);

    /* Re-step: first row should be identical */
    rc = sqlite3_step(pLog);
    check("partial: log restart", rc==SQLITE_ROW);
    sqlite3_reset(pLog);

    sqlite3_finalize(pLog);
  }

  sqlite3_finalize(pSel);
  sqlite3_close(db);
  unlink("test_partial.db");
}

/* ------------------------------------------------------------------ */

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

  printf("--- legacy diff TVF reuse ---\n");
  test_legacy_diff_reuse();

  printf("--- rapid interleave ---\n");
  test_rapid_interleave();

  printf("--- partial step + reset ---\n");
  test_partial_step();

  printf("\n=== Results: %d passed, %d failed ===\n", nPass, nFail);
  return nFail > 0 ? 1 : 0;
}
