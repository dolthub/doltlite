/*
** Test doltliteFindAncestor: find common ancestor between two branches.
**
** Creates a commit history like:
**   C1 -- C2 -- C3 (main)
**               \-- C4 -- C5 (feature)
** Then verifies that the common ancestor of C3 and C5 is C2.
*/
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
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
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_ancestor.db";
  int rc;
  const char *main_head, *feature_head, *ancestor;
  char main_head_buf[128], feature_head_buf[128];

  remove(dbpath);
  remove("/tmp/test_ancestor.db-chunks");

  rc = sqlite3_open(dbpath, &db);
  check("open db", rc==SQLITE_OK);

  /* Create table and initial commit (C1) */
  execsql(db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b TEXT)");
  execsql(db, "INSERT INTO t1 VALUES(1, 'hello')");
  execsql(db, "SELECT dolt_add('-A')");
  execsql(db, "SELECT dolt_commit('-m', 'C1: initial')");

  /* Second commit on main (C2) - this will be the common ancestor */
  execsql(db, "INSERT INTO t1 VALUES(2, 'world')");
  execsql(db, "SELECT dolt_add('-A')");
  execsql(db, "SELECT dolt_commit('-m', 'C2: add row 2')");

  /* Record C2's hash (this is HEAD on main right now) */
  const char *c2_hash = exec1(db, "SELECT commit_hash FROM dolt_log LIMIT 1");
  char c2_hash_buf[128];
  snprintf(c2_hash_buf, sizeof(c2_hash_buf), "%s", c2_hash);

  /* Create feature branch at C2 */
  execsql(db, "SELECT dolt_branch('feature')");

  /* Continue on main: C3 */
  execsql(db, "INSERT INTO t1 VALUES(3, 'main-only')");
  execsql(db, "SELECT dolt_add('-A')");
  execsql(db, "SELECT dolt_commit('-m', 'C3: main diverge')");

  /* Record main HEAD (C3) */
  main_head = exec1(db, "SELECT commit_hash FROM dolt_log LIMIT 1");
  snprintf(main_head_buf, sizeof(main_head_buf), "%s", main_head);

  /* Switch to feature branch and make commits C4, C5 */
  execsql(db, "SELECT dolt_checkout('feature')");
  execsql(db, "INSERT INTO t1 VALUES(4, 'feature-row')");
  execsql(db, "SELECT dolt_add('-A')");
  execsql(db, "SELECT dolt_commit('-m', 'C4: feature work')");

  execsql(db, "INSERT INTO t1 VALUES(5, 'more-feature')");
  execsql(db, "SELECT dolt_add('-A')");
  execsql(db, "SELECT dolt_commit('-m', 'C5: more feature work')");

  /* Record feature HEAD (C5) */
  feature_head = exec1(db, "SELECT commit_hash FROM dolt_log LIMIT 1");
  snprintf(feature_head_buf, sizeof(feature_head_buf), "%s", feature_head);

  printf("C2 (expected ancestor): %s\n", c2_hash_buf);
  printf("C3 (main HEAD):         %s\n", main_head_buf);
  printf("C5 (feature HEAD):      %s\n", feature_head_buf);

  /* Test 1: Find ancestor of main HEAD and feature HEAD */
  {
    char sql[512];
    snprintf(sql, sizeof(sql),
      "SELECT dolt_merge_base('%s', '%s')", main_head_buf, feature_head_buf);
    ancestor = exec1(db, sql);
    printf("Ancestor (main,feature): %s\n", ancestor);
    check("ancestor of diverged branches is C2",
          strcmp(ancestor, c2_hash_buf)==0);
  }

  /* Test 2: Ancestor of a commit with itself */
  {
    char sql[512];
    snprintf(sql, sizeof(sql),
      "SELECT dolt_merge_base('%s', '%s')", main_head_buf, main_head_buf);
    ancestor = exec1(db, sql);
    printf("Ancestor (self,self):    %s\n", ancestor);
    check("ancestor of commit with itself is the commit",
          strcmp(ancestor, main_head_buf)==0);
  }

  /* Test 3: Ancestor where one is ancestor of the other */
  {
    char sql[512];
    snprintf(sql, sizeof(sql),
      "SELECT dolt_merge_base('%s', '%s')", c2_hash_buf, feature_head_buf);
    ancestor = exec1(db, sql);
    printf("Ancestor (C2,feature):   %s\n", ancestor);
    check("ancestor where one is ancestor of other",
          strcmp(ancestor, c2_hash_buf)==0);
  }

  sqlite3_close(db);
  remove(dbpath);
  remove("/tmp/test_ancestor.db-chunks");

  printf("\n%d passed, %d failed\n", nPass, nFail);
  return nFail>0 ? 1 : 0;
}
