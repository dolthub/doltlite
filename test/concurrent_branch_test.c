#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdlib.h>
#include "sqlite3.h"
#include "lib/test_tmpdir.h"

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

static void test_checkout_commit_after_peer_rollback(void){
  const char *zPath = DOLTLITE_TEST_TMPDIR "/test_checkout_commit_peer_rollback.db";
  const char *azKey[] = { "INTEGER PRIMARY KEY", "INT PRIMARY KEY" };
  int i;
  for(i=0; i<2; i++){
    sqlite3 *peer = 0, *db = 0;
    char zSql[2048], zBranchPath[512], zHead[64], zSourceHead[64];
    remove(zPath);
    check("checkout_commit_peer_open", sqlite3_open(zPath, &peer)==SQLITE_OK);
    snprintf(zSql, sizeof(zSql),
      "CREATE TABLE w(id %s,v TEXT);"
      "INSERT INTO w VALUES(1,'base');"
      "SELECT dolt_commit('-Am','base');"
      "SELECT dolt_checkout('-b','b3');"
      "ALTER TABLE w ADD COLUMN source TEXT;"
      "CREATE INDEX source_iw ON w(v);"
      "INSERT INTO w(id,v) VALUES(3,'source');"
      "SELECT dolt_commit('-am','source');"
      "SELECT dolt_checkout('main');"
      "SELECT dolt_checkout('-b','b1');"
      "ALTER TABLE w ADD COLUMN target TEXT;"
      "CREATE INDEX target_iw ON w(v);"
      "INSERT INTO w(id,v) VALUES(11,'target'),(12,'target');"
      "SELECT dolt_commit('-am','target');"
      "SELECT dolt_checkout('b3');", azKey[i]);
    check("checkout_commit_peer_setup", execSql(peer, zSql)==SQLITE_OK);
    snprintf(zBranchPath, sizeof(zBranchPath), "%s@b3", zPath);
    check("checkout_commit_victim_open", sqlite3_open(zBranchPath, &db)==SQLITE_OK);
    check("checkout_commit_source_edits", execSql(db,
      "BEGIN; UPDATE w SET v='edited' WHERE id=3; COMMIT;"
      "DELETE FROM w WHERE id=3;")==SQLITE_OK);
    snprintf(zSourceHead, sizeof(zSourceHead), "%s",
        queryScalarText(db, "SELECT dolt_hashof('HEAD')"));
    check("checkout_commit_peer_source_refreshed", strcmp(queryScalarText(peer,
      "SELECT group_concat(id) FROM (SELECT id FROM w ORDER BY id)"), "1")==0);
    check("checkout_commit_peer_rollback", execSql(peer,
      "BEGIN; SAVEPOINT caller;"
      "SELECT dolt_checkout('b1');"
      "INSERT INTO w(id,v) VALUES(99,'rolled back');"
      "SAVEPOINT tail; ROLLBACK;")==SQLITE_OK);
    check("checkout_commit_peer_publish", execSql(peer,
      "INSERT INTO w(id,v) VALUES(88,'peer');")==SQLITE_OK);
    snprintf(zHead, sizeof(zHead), "%s",
        queryScalarText(peer, "SELECT dolt_hashof('HEAD')"));
    check("checkout_commit_victim_publish", execSql(db,
      "SELECT dolt_checkout('b1');"
      "SELECT dolt_commit('-am','after checkout','--allow-empty');")==SQLITE_OK);
    check("checkout_commit_target_rows_kept", strcmp(queryScalarText(db,
      "SELECT group_concat(id) FROM (SELECT id FROM dolt_at_w('HEAD') ORDER BY id)"),
      "1,11,12,88")==0);
    check("checkout_commit_stacks_on_target", strcmp(queryScalarText(db,
      "SELECT dolt_hashof('HEAD~1')"), zHead)==0);
    check("checkout_commit_target_schema_kept", strcmp(queryScalarText(db,
      "SELECT group_concat(name) FROM pragma_table_info('w')"), "id,v,target")==0);
    check("checkout_commit_target_index_kept", strcmp(queryScalarText(db,
      "SELECT count(*) FROM sqlite_schema WHERE name='target_iw'"), "1")==0);
    check("checkout_commit_source_index_absent", strcmp(queryScalarText(db,
      "SELECT count(*) FROM sqlite_schema WHERE name='source_iw'"), "0")==0);
    check("checkout_commit_peer_sees_target", strcmp(queryScalarText(peer,
      "SELECT group_concat(id) FROM (SELECT id FROM w ORDER BY id)"), "1,11,12,88")==0);
    check("checkout_commit_target_integrity", strcmp(queryScalarText(db,
      "PRAGMA integrity_check"), "ok")==0);
    check("checkout_commit_return_to_source", execSql(db,
      "SELECT dolt_checkout('b3')")==SQLITE_OK);
    check("checkout_commit_source_rows_kept", strcmp(queryScalarText(db,
      "SELECT group_concat(id) FROM (SELECT id FROM w ORDER BY id)"), "1")==0);
    check("checkout_commit_source_head_kept", strcmp(queryScalarText(db,
      "SELECT dolt_hashof('HEAD')"), zSourceHead)==0);
    sqlite3_close(peer);
    sqlite3_close(db);
    snprintf(zBranchPath, sizeof(zBranchPath), "%s@b1", zPath);
    check("checkout_commit_reopen", sqlite3_open(zBranchPath, &db)==SQLITE_OK);
    check("checkout_commit_durable_rows_kept", strcmp(queryScalarText(db,
      "SELECT group_concat(id) FROM (SELECT id FROM w ORDER BY id)"), "1,11,12,88")==0);
    check("checkout_commit_durable_integrity", strcmp(queryScalarText(db,
      "PRAGMA integrity_check"), "ok")==0);
    sqlite3_close(db);
    remove(zPath);
  }
}

int main(){
  sqlite3 *db1 = 0, *db2 = 0;
  sqlite3 *db3 = 0;
  const char *dbpath = DOLTLITE_TEST_TMPDIR "/test_concurrent_branch.db";
  const char *defaultPath = DOLTLITE_TEST_TMPDIR "/test_concurrent_default_branch.db";
  int rc;

  test_checkout_commit_after_peer_rollback();

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
  {
    char zFeat[512];
    snprintf(zFeat, sizeof(zFeat), "%s/feat", defaultPath);
    rc = sqlite3_open(zFeat, &db2);
  }
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
