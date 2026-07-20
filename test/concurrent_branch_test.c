#include <stdio.h>
#include <stdlib.h>
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
  }else if( rc!=SQLITE_DONE ){
    snprintf(result_buf, sizeof(result_buf), "ERROR: %s", sqlite3_errmsg(db));
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
  sqlite3 *db3 = 0;
  const char *dbpath = "/tmp/test_concurrent_branch.db";
  const char *defaultPath = "/tmp/test_concurrent_default_branch.db";
  int rc;

  remove(dbpath); { char _w[256]; snprintf(_w,256,"%s-wal",dbpath); remove(_w); }

  printf("=== Concurrent Branch Test ===\n\n");

  rc = sqlite3_open(dbpath, &db1);
  check("open_db1", rc==SQLITE_OK);

  execSql(db1, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
  execSql(db1, "INSERT INTO t VALUES(1, 'main-data')");
  queryScalarText(db1, "SELECT dolt_commit('-A', '-m', 'init on main')");

  queryScalarText(db1, "SELECT dolt_branch('feature')");

  queryScalarText(db1, "SELECT dolt_checkout('feature')");
  check("db1_on_feature", strcmp(queryScalarText(db1, "SELECT active_branch()"), "feature")==0);

  execSql(db1, "INSERT INTO t VALUES(2, 'feature-data')");
  queryScalarText(db1, "SELECT dolt_commit('-A', '-m', 'add on feature')");

  queryScalarText(db1, "SELECT dolt_checkout('main')");
  check("db1_back_on_main", strcmp(queryScalarText(db1, "SELECT active_branch()"), "main")==0);

  rc = sqlite3_open(dbpath, &db2);
  check("open_db2", rc==SQLITE_OK);

  check("db2_starts_on_main", strcmp(queryScalarText(db2, "SELECT active_branch()"), "main")==0);

  check("db1_main_count", strcmp(queryScalarText(db1, "SELECT count(*) FROM t"), "1")==0);
  check("db2_main_count", strcmp(queryScalarText(db2, "SELECT count(*) FROM t"), "1")==0);
  check("db1_main_val", strcmp(queryScalarText(db1, "SELECT val FROM t WHERE id=1"), "main-data")==0);
  check("db2_main_val", strcmp(queryScalarText(db2, "SELECT val FROM t WHERE id=1"), "main-data")==0);

  queryScalarText(db2, "SELECT dolt_checkout('feature')");
  check("db2_on_feature", strcmp(queryScalarText(db2, "SELECT active_branch()"), "feature")==0);

  check("db1_still_main", strcmp(queryScalarText(db1, "SELECT active_branch()"), "main")==0);

  check("db2_feature_count", strcmp(queryScalarText(db2, "SELECT count(*) FROM t"), "2")==0);
  check("db2_sees_feature_data", strcmp(queryScalarText(db2, "SELECT val FROM t WHERE id=2"), "feature-data")==0);

  check("db2_branch_count", atoi(queryScalarText(db2, "SELECT count(*) FROM dolt_branches"))>=1);

  check("db2_log_feature", strcmp(queryScalarText(db2, "SELECT message FROM dolt_log LIMIT 1"), "add on feature")==0);

  sqlite3_close(db1);
  sqlite3_close(db2);
  remove(dbpath); { char _w[256]; snprintf(_w,256,"%s-wal",dbpath); remove(_w); }

  remove(defaultPath);
  rc = sqlite3_open(defaultPath, &db1);
  check("open_stale_rename_connection", rc==SQLITE_OK);
  check("setup_default_rename_repo", execSql(db1,
    "CREATE TABLE t(id INTEGER PRIMARY KEY);"
    "SELECT dolt_commit('-A','-m','init');"
    "SELECT dolt_branch('feat');")==SQLITE_OK);
  rc = sqlite3_open(defaultPath, &db2);
  check("open_default_setter_connection", rc==SQLITE_OK);
  check("peer_sets_new_default",
        strcmp(queryScalarText(db2,
          "SELECT dolt_default_branch('feat')"), "0")==0);
  check("stale_connection_renames_old_default",
        strcmp(queryScalarText(db1,
          "SELECT dolt_branch('-m','main','trunk')"), "0")==0);
  rc = sqlite3_open(defaultPath, &db3);
  check("open_fresh_default_reader", rc==SQLITE_OK);
  check("stale_rename_preserves_peer_default",
        strcmp(queryScalarText(db3,
          "SELECT dolt_default_branch()"), "feat")==0);
  check("stale_rename_moves_source_branch",
        strcmp(queryScalarText(db3,
          "SELECT count(*) FROM dolt_branches WHERE name='trunk'"), "1")==0);
  check("stale_rename_removes_old_branch_name",
        strcmp(queryScalarText(db3,
          "SELECT count(*) FROM dolt_branches WHERE name='main'"), "0")==0);
  sqlite3_close(db3);

  check("peer_creates_branch_after_setter_opened",
        strcmp(queryScalarText(db1,
          "SELECT dolt_branch('side')"), "0")==0);
  check("stale_default_setter_refreshes_refs",
        strcmp(queryScalarText(db2,
          "SELECT dolt_default_branch('trunk')"), "0")==0);
  rc = sqlite3_open(defaultPath, &db3);
  check("reopen_after_stale_default_setter", rc==SQLITE_OK);
  check("stale_default_setter_preserves_peer_branch",
        strcmp(queryScalarText(db3,
          "SELECT count(*) FROM dolt_branches WHERE name='side'"), "1")==0);
  check("stale_default_setter_persists_selection",
        strcmp(queryScalarText(db3,
          "SELECT dolt_default_branch()"), "trunk")==0);
  sqlite3_close(db3);
  db3 = 0;

  check("peer_selects_new_default_before_delete",
        strcmp(queryScalarText(db2,
          "SELECT dolt_default_branch('side')"), "0")==0);
  check("stale_connection_cannot_delete_new_default",
        strstr(queryScalarText(db1,
          "SELECT dolt_branch('-D','side')"),
          "cannot delete the default branch")!=0);
  rc = sqlite3_open(defaultPath, &db3);
  check("reopen_after_stale_default_delete", rc==SQLITE_OK);
  check("stale_delete_preserves_default_branch",
        strcmp(queryScalarText(db3,
          "SELECT dolt_default_branch()"), "side")==0);
  check("stale_delete_preserves_default_ref",
        strcmp(queryScalarText(db3,
          "SELECT count(*) FROM dolt_branches WHERE name='side'"), "1")==0);

  sqlite3_close(db1);
  sqlite3_close(db2);
  sqlite3_close(db3);
  remove(defaultPath);
  { char _w[256]; snprintf(_w,256,"%s-wal",defaultPath); remove(_w); }

  printf("\nResults: %d passed, %d failed out of %d tests\n", nPass, nFail, nPass+nFail);
  return nFail > 0 ? 1 : 0;
}
