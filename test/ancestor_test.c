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
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_ancestor.db";
  int rc;
  const char *main_head, *feature_head, *ancestor, *c2_hash;
  char main_head_buf[128], feature_head_buf[128], merge_head_buf[128];
  char c2_hash_buf[128];

  remove(dbpath);
  remove("/tmp/test_ancestor.db-chunks");

  rc = sqlite3_open(dbpath, &db);
  check("open db", rc==SQLITE_OK);

  execSql(db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b TEXT)");
  execSql(db, "INSERT INTO t1 VALUES(1, 'hello')");
  execSql(db, "SELECT dolt_add('-A')");
  execSql(db, "SELECT dolt_commit('-m', 'C1: initial')");

  execSql(db, "INSERT INTO t1 VALUES(2, 'world')");
  execSql(db, "SELECT dolt_add('-A')");
  execSql(db, "SELECT dolt_commit('-m', 'C2: add row 2')");

  c2_hash = queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1");
  snprintf(c2_hash_buf, sizeof(c2_hash_buf), "%s", c2_hash);

  execSql(db, "SELECT dolt_branch('feature')");

  execSql(db, "INSERT INTO t1 VALUES(3, 'main-only')");
  execSql(db, "SELECT dolt_add('-A')");
  execSql(db, "SELECT dolt_commit('-m', 'C3: main diverge')");

  main_head = queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1");
  snprintf(main_head_buf, sizeof(main_head_buf), "%s", main_head);

  execSql(db, "SELECT dolt_checkout('feature')");
  execSql(db, "INSERT INTO t1 VALUES(4, 'feature-row')");
  execSql(db, "SELECT dolt_add('-A')");
  execSql(db, "SELECT dolt_commit('-m', 'C4: feature work')");

  execSql(db, "INSERT INTO t1 VALUES(5, 'more-feature')");
  execSql(db, "SELECT dolt_add('-A')");
  execSql(db, "SELECT dolt_commit('-m', 'C5: more feature work')");

  feature_head = queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1");
  snprintf(feature_head_buf, sizeof(feature_head_buf), "%s", feature_head);

  printf("C2 (expected ancestor): %s\n", c2_hash_buf);
  printf("C3 (main HEAD):         %s\n", main_head_buf);
  printf("C5 (feature HEAD):      %s\n", feature_head_buf);

  {
    char sql[512];
    snprintf(sql, sizeof(sql),
      "SELECT dolt_merge_base('%s', '%s')", main_head_buf, feature_head_buf);
    ancestor = queryScalarText(db, sql);
    printf("Ancestor (main,feature): %s\n", ancestor);
    check("ancestor of diverged branches is C2",
          strcmp(ancestor, c2_hash_buf)==0);
  }

  {
    char sql[512];
    snprintf(sql, sizeof(sql),
      "SELECT dolt_merge_base('%s', '%s')", main_head_buf, main_head_buf);
    ancestor = queryScalarText(db, sql);
    printf("Ancestor (self,self):    %s\n", ancestor);
    check("ancestor of commit with itself is the commit",
          strcmp(ancestor, main_head_buf)==0);
  }

  {
    char sql[512];
    snprintf(sql, sizeof(sql),
      "SELECT dolt_merge_base('%s', '%s')", c2_hash_buf, feature_head_buf);
    ancestor = queryScalarText(db, sql);
    printf("Ancestor (C2,feature):   %s\n", ancestor);
    check("ancestor where one is ancestor of other",
          strcmp(ancestor, c2_hash_buf)==0);
  }

  execSql(db, "SELECT dolt_checkout('main')");
  execSql(db, "SELECT dolt_merge('feature')");
  main_head = queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1");
  snprintf(merge_head_buf, sizeof(merge_head_buf), "%s", main_head);

  {
    char sql[512];
    snprintf(sql, sizeof(sql),
      "SELECT dolt_merge_base('%s^2', '%s^2')", merge_head_buf, merge_head_buf);
    ancestor = queryScalarText(db, sql);
    printf("Ancestor (HEAD^2,HEAD^2): %s\n", ancestor);
    check("merge second-parent ref resolves to feature head",
          strcmp(ancestor, feature_head_buf)==0);
  }

  {
    char sql[512];
    snprintf(sql, sizeof(sql),
      "SELECT dolt_merge_base('%s^2', '%s~1')", merge_head_buf, merge_head_buf);
    ancestor = queryScalarText(db, sql);
    printf("Ancestor (HEAD^2,HEAD~1): %s\n", ancestor);
    check("merge first/second parent base is C2",
          strcmp(ancestor, c2_hash_buf)==0);
  }

  sqlite3_close(db);
  remove(dbpath);
  remove("/tmp/test_ancestor.db-chunks");

  printf("\n%d passed, %d failed\n", nPass, nFail);
  return nFail>0 ? 1 : 0;
}
