#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
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

static char result_buf[8192];
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
    if( val ) snprintf(result_buf, sizeof(result_buf), "%s", val);
  }else if( rc==SQLITE_ERROR ){
    snprintf(result_buf, sizeof(result_buf), "ERROR: %s", sqlite3_errmsg(db));
  }
  sqlite3_finalize(stmt);
  return result_buf;
}

static int queryScalarInt(sqlite3 *db, const char *sql){
  const char *r = queryScalarText(db, sql);
  if( strncmp(r, "ERROR", 5)==0 ) return -1;
  return atoi(r);
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

static int isValidHash(const char *s){
  int i;
  if( !s || strlen(s)!=40 ) return 0;
  for( i=0; i<40; i++ ){
    if( !isxdigit((unsigned char)s[i]) ) return 0;
  }
  return 1;
}

static int checkInvariants(sqlite3 *db, const char *context){
  int violations = 0;
  char label[256];

  {
    sqlite3_stmt *stmt = 0;
    int rc = sqlite3_prepare_v2(db,
      "SELECT name, hash FROM dolt_branches", -1, &stmt, 0);
    if( rc!=SQLITE_OK ){
      snprintf(label, sizeof(label), "%s: prepare dolt_branches", context);
      check(label, 0);
      violations++;
    }else{
      int nBranches = 0;
      while( sqlite3_step(stmt)==SQLITE_ROW ){
        const char *name = (const char*)sqlite3_column_text(stmt, 0);
        const char *hash = (const char*)sqlite3_column_text(stmt, 1);
        nBranches++;
        snprintf(label, sizeof(label), "%s: branch '%s' has valid hash",
                 context, name ? name : "NULL");
        if( !isValidHash(hash) ){
          check(label, 0);
          violations++;
        }else{
          check(label, 1);
        }
      }
      sqlite3_finalize(stmt);
      snprintf(label, sizeof(label), "%s: at least one branch exists", context);
      if( nBranches < 1 ){
        check(label, 0);
        violations++;
      }else{
        check(label, 1);
      }
    }
  }

  {
    sqlite3_stmt *stmt = 0;
    int rc = sqlite3_prepare_v2(db,
      "SELECT commit_hash, committer, date, message FROM dolt_log",
      -1, &stmt, 0);
    if( rc!=SQLITE_OK ){
      snprintf(label, sizeof(label), "%s: prepare dolt_log", context);
      check(label, 0);
      violations++;
    }else{
      int nCommits = 0;
      while( sqlite3_step(stmt)==SQLITE_ROW ){
        const char *hash = (const char*)sqlite3_column_text(stmt, 0);
        const char *committer = (const char*)sqlite3_column_text(stmt, 1);
        const char *date = (const char*)sqlite3_column_text(stmt, 2);
        const char *message = (const char*)sqlite3_column_text(stmt, 3);
        nCommits++;

        snprintf(label, sizeof(label), "%s: log commit %d has valid hash",
                 context, nCommits);
        if( !isValidHash(hash) ){
          check(label, 0); violations++;
        }else{
          check(label, 1);
        }

        snprintf(label, sizeof(label), "%s: log commit %d has message",
                 context, nCommits);
        if( !message || strlen(message)==0 ){
          check(label, 0); violations++;
        }else{
          check(label, 1);
        }

        snprintf(label, sizeof(label), "%s: log commit %d has date",
                 context, nCommits);
        if( !date || strlen(date)==0 ){
          check(label, 0); violations++;
        }else{
          check(label, 1);
        }
      }
      sqlite3_finalize(stmt);

      snprintf(label, sizeof(label), "%s: at least one commit in log", context);
      if( nCommits < 1 ){
        check(label, 0); violations++;
      }else{
        check(label, 1);
      }
    }
  }

  {
    const char *branch = queryScalarText(db, "SELECT active_branch()");
    snprintf(label, sizeof(label), "%s: active_branch() non-empty", context);
    if( !branch || strlen(branch)==0 || strncmp(branch,"ERROR",5)==0 ){
      check(label, 0); violations++;
    }else{
      check(label, 1);

      char sql[512];
      snprintf(sql, sizeof(sql),
        "SELECT count(*) FROM dolt_branches WHERE name='%s'", branch);
      int cnt = queryScalarInt(db, sql);
      snprintf(label, sizeof(label),
        "%s: active_branch '%s' exists in dolt_branches", context, branch);
      if( cnt!=1 ){
        check(label, 0); violations++;
      }else{
        check(label, 1);
      }
    }
  }

  {
    sqlite3_stmt *stmt = 0;
    int rc = sqlite3_prepare_v2(db,
      "SELECT name FROM sqlite_master WHERE type='table'",
      -1, &stmt, 0);
    if( rc==SQLITE_OK ){
      while( sqlite3_step(stmt)==SQLITE_ROW ){
        const char *tname = (const char*)sqlite3_column_text(stmt, 0);
        if( tname ){
          char sql[512];
          snprintf(sql, sizeof(sql), "SELECT count(*) FROM \"%s\"", tname);
          int cnt = queryScalarInt(db, sql);
          snprintf(label, sizeof(label),
            "%s: table '%s' is queryable", context, tname);
          if( cnt < 0 ){
            check(label, 0); violations++;
          }else{
            check(label, 1);
          }
        }
      }
      sqlite3_finalize(stmt);
    }
  }

  {
    sqlite3_stmt *stmt = 0;
    int rc = sqlite3_prepare_v2(db,
      "SELECT table_name, staged, status FROM dolt_status",
      -1, &stmt, 0);
    snprintf(label, sizeof(label), "%s: dolt_status is queryable", context);
    if( rc!=SQLITE_OK ){
      check(label, 0); violations++;
    }else{
      check(label, 1);
      while( sqlite3_step(stmt)==SQLITE_ROW ){
        const char *tname = (const char*)sqlite3_column_text(stmt, 0);
        int staged = sqlite3_column_int(stmt, 1);
        const char *status = (const char*)sqlite3_column_text(stmt, 2);
        (void)tname; (void)staged; (void)status;
      }
      sqlite3_finalize(stmt);
    }
  }

  return violations;
}

static void checkCleanStatus(sqlite3 *db, const char *context){
  char label[256];
  int cnt = queryScalarInt(db, "SELECT count(*) FROM dolt_status");
  snprintf(label, sizeof(label),
    "%s: dolt_status shows no uncommitted changes", context);
  check(label, cnt==0);
}

static void removeDb(const char *path){
  char wal[512];
  remove(path);
  snprintf(wal, sizeof(wal), "%s-wal", path);
  remove(wal);
}


static void test_basic_commit(void){
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_inv_basic.db";

  printf("--- Test 1: Basic create/insert/commit ---\n");
  removeDb(dbpath);

  check("open_basic", sqlite3_open(dbpath, &db)==SQLITE_OK);
  execSql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT)");
  execSql(db, "INSERT INTO t1 VALUES(1, 'alice')");
  execSql(db, "INSERT INTO t1 VALUES(2, 'bob')");
  queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'Initial commit')");

  checkInvariants(db, "basic_commit");
  checkCleanStatus(db, "basic_commit");

  sqlite3_close(db);
  removeDb(dbpath);
}

static void test_multi_branch(void){
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_inv_multibranch.db";

  printf("--- Test 2: Multiple branches with commits ---\n");
  removeDb(dbpath);

  check("open_multibranch", sqlite3_open(dbpath, &db)==SQLITE_OK);

  execSql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)");
  execSql(db, "INSERT INTO t1 VALUES(1, 'main-init')");
  queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'init on main')");

  queryScalarText(db, "SELECT dolt_branch('dev')");
  queryScalarText(db, "SELECT dolt_branch('staging')");

  queryScalarText(db, "SELECT dolt_checkout('dev')");
  execSql(db, "INSERT INTO t1 VALUES(2, 'dev-row')");
  queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'dev commit')");
  checkInvariants(db, "multibranch_dev");
  checkCleanStatus(db, "multibranch_dev");

  queryScalarText(db, "SELECT dolt_checkout('staging')");
  execSql(db, "INSERT INTO t1 VALUES(3, 'staging-row')");
  queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'staging commit')");
  checkInvariants(db, "multibranch_staging");
  checkCleanStatus(db, "multibranch_staging");

  queryScalarText(db, "SELECT dolt_checkout('main')");
  checkInvariants(db, "multibranch_main");
  checkCleanStatus(db, "multibranch_main");

  check("three_branches", queryScalarInt(db, "SELECT count(*) FROM dolt_branches")==3);

  sqlite3_close(db);
  removeDb(dbpath);
}

static void test_merge(void){
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_inv_merge.db";

  printf("--- Test 3: Merge two branches ---\n");
  removeDb(dbpath);

  check("open_merge", sqlite3_open(dbpath, &db)==SQLITE_OK);

  execSql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)");
  execSql(db, "INSERT INTO t1 VALUES(1, 'init')");
  queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'init')");

  queryScalarText(db, "SELECT dolt_branch('feature')");
  queryScalarText(db, "SELECT dolt_checkout('feature')");
  execSql(db, "INSERT INTO t1 VALUES(2, 'feature-data')");
  queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'feature commit')");

  queryScalarText(db, "SELECT dolt_checkout('main')");
  execSql(db, "INSERT INTO t1 VALUES(3, 'main-data')");
  queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'main commit')");

  queryScalarText(db, "SELECT dolt_merge('feature')");
  checkInvariants(db, "after_merge");

  check("merge_row_count", queryScalarInt(db, "SELECT count(*) FROM t1")==3);

  sqlite3_close(db);
  removeDb(dbpath);
}

static void test_insert_delete(void){
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_inv_del.db";

  printf("--- Test 4: Insert then delete rows ---\n");
  removeDb(dbpath);

  check("open_del", sqlite3_open(dbpath, &db)==SQLITE_OK);

  execSql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)");
  execSql(db, "INSERT INTO t1 VALUES(1, 'a')");
  execSql(db, "INSERT INTO t1 VALUES(2, 'b')");
  execSql(db, "INSERT INTO t1 VALUES(3, 'c')");
  execSql(db, "INSERT INTO t1 VALUES(4, 'd')");
  queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'add 4 rows')");
  checkInvariants(db, "insert_phase");
  checkCleanStatus(db, "insert_phase");

  execSql(db, "DELETE FROM t1 WHERE id IN (2, 4)");
  queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'delete rows 2 and 4')");
  checkInvariants(db, "delete_phase");
  checkCleanStatus(db, "delete_phase");

  check("remaining_rows", queryScalarInt(db, "SELECT count(*) FROM t1")==2);

  sqlite3_close(db);
  removeDb(dbpath);
}

static void test_sequential_commits(void){
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_inv_seq.db";
  int i;

  printf("--- Test 5: 10 sequential commits ---\n");
  removeDb(dbpath);

  check("open_seq", sqlite3_open(dbpath, &db)==SQLITE_OK);

  execSql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)");
  queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'create table')");
  checkInvariants(db, "seq_0");

  for( i=1; i<=10; i++ ){
    char sql[256], ctx[64], msg[64];
    switch( i % 4 ){
      case 1:
        snprintf(sql, sizeof(sql),
          "INSERT INTO t1 VALUES(%d, 'row%d')", i*10, i);
        break;
      case 2:
        snprintf(sql, sizeof(sql),
          "UPDATE t1 SET val='updated%d' WHERE id=(SELECT min(id) FROM t1)", i);
        break;
      case 3:
        snprintf(sql, sizeof(sql),
          "INSERT INTO t1 VALUES(%d, 'extra%d')", i*10+1, i);
        break;
      case 0:
        snprintf(sql, sizeof(sql),
          "DELETE FROM t1 WHERE id=(SELECT max(id) FROM t1)");
        break;
    }
    execSql(db, sql);
    snprintf(msg, sizeof(msg), "SELECT dolt_commit('-A', '-m', 'commit %d')", i);
    queryScalarText(db, msg);
    snprintf(ctx, sizeof(ctx), "seq_%d", i);
    checkInvariants(db, ctx);
    checkCleanStatus(db, ctx);
  }

  check("seq_log_count", queryScalarInt(db, "SELECT count(*) FROM dolt_log")==12);

  sqlite3_close(db);
  removeDb(dbpath);
}

static void test_schema_change(void){
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_inv_schema.db";

  printf("--- Test 6: Schema change on branch ---\n");
  removeDb(dbpath);

  check("open_schema", sqlite3_open(dbpath, &db)==SQLITE_OK);

  execSql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT)");
  execSql(db, "INSERT INTO t1 VALUES(1, 'alice')");
  queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'init')");

  queryScalarText(db, "SELECT dolt_branch('schema-change')");
  queryScalarText(db, "SELECT dolt_checkout('schema-change')");
  execSql(db, "ALTER TABLE t1 ADD COLUMN age INTEGER DEFAULT 0");
  execSql(db, "UPDATE t1 SET age=30 WHERE id=1");
  queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'add age column')");
  checkInvariants(db, "schema_branch");
  checkCleanStatus(db, "schema_branch");

  queryScalarText(db, "SELECT dolt_checkout('main')");
  checkInvariants(db, "schema_main_after");
  checkCleanStatus(db, "schema_main_after");

  const char *r = queryScalarText(db, "SELECT age FROM t1");
  check("main_no_age_column", strncmp(r, "ERROR", 5)==0);

  sqlite3_close(db);
  removeDb(dbpath);
}

static void test_gc(void){
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_inv_gc.db";

  printf("--- Test 7: GC preserves invariants ---\n");
  removeDb(dbpath);

  check("open_gc", sqlite3_open(dbpath, &db)==SQLITE_OK);

  execSql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)");
  execSql(db, "INSERT INTO t1 VALUES(1, 'a')");
  queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'c1')");
  execSql(db, "INSERT INTO t1 VALUES(2, 'b')");
  queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'c2')");
  execSql(db, "INSERT INTO t1 VALUES(3, 'c')");
  queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'c3')");

  queryScalarText(db, "SELECT dolt_gc()");
  checkInvariants(db, "after_gc");
  checkCleanStatus(db, "after_gc");

  check("gc_data_intact", queryScalarInt(db, "SELECT count(*) FROM t1")==3);
  check("gc_log_intact", queryScalarInt(db, "SELECT count(*) FROM dolt_log")==4);

  sqlite3_close(db);
  removeDb(dbpath);
}

static void test_reset_hard(void){
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_inv_reset.db";

  printf("--- Test 8: Reset --hard preserves invariants ---\n");
  removeDb(dbpath);

  check("open_reset", sqlite3_open(dbpath, &db)==SQLITE_OK);

  execSql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)");
  execSql(db, "INSERT INTO t1 VALUES(1, 'original')");
  queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'init')");

  execSql(db, "INSERT INTO t1 VALUES(2, 'uncommitted')");
  execSql(db, "UPDATE t1 SET val='modified' WHERE id=1");

  check("dirty_before_reset",
    queryScalarInt(db, "SELECT count(*) FROM dolt_status") > 0);

  queryScalarText(db, "SELECT dolt_reset('--hard')");
  checkInvariants(db, "after_reset");
  checkCleanStatus(db, "after_reset");

  check("reset_data", strcmp(queryScalarText(db, "SELECT val FROM t1 WHERE id=1"),
                             "original")==0);
  check("reset_row_count", queryScalarInt(db, "SELECT count(*) FROM t1")==1);

  sqlite3_close(db);
  removeDb(dbpath);
}

static void test_random_operations(void){
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_inv_random.db";
  int i;

  printf("--- Test 9: 100 random operations ---\n");
  removeDb(dbpath);

  check("open_random", sqlite3_open(dbpath, &db)==SQLITE_OK);

  execSql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)");
  queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'init')");

  srand(42); /* Deterministic seed */
  for( i=0; i<100; i++ ){
    char sql[256];
    int op = rand() % 5;
    switch( op ){
      case 0: /* INSERT */
        snprintf(sql, sizeof(sql),
          "INSERT OR IGNORE INTO t1 VALUES(%d, 'v%d')", rand()%500, i);
        execSql(db, sql);
        break;
      case 1: /* UPDATE */
        snprintf(sql, sizeof(sql),
          "UPDATE t1 SET val='u%d' WHERE id=(SELECT id FROM t1 ORDER BY RANDOM() LIMIT 1)", i);
        execSql(db, sql);
        break;
      case 2: /* DELETE */
        execSql(db, "DELETE FROM t1 WHERE id=(SELECT id FROM t1 ORDER BY RANDOM() LIMIT 1)");
        break;
      case 3: /* COMMIT */
        snprintf(sql, sizeof(sql),
          "SELECT dolt_commit('-A', '-m', 'random op %d')", i);
        queryScalarText(db, sql);
        break;
      case 4: /* SELECT (read-only, verify no crash) */
        queryScalarText(db, "SELECT count(*) FROM t1");
        break;
    }
  }

  queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'final random commit')");
  checkInvariants(db, "after_random_100");
  checkCleanStatus(db, "after_random_100");

  sqlite3_close(db);
  removeDb(dbpath);
}

static void test_multi_table(void){
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_inv_multi.db";

  printf("--- Test 10: Multiple tables ---\n");
  removeDb(dbpath);

  check("open_multi", sqlite3_open(dbpath, &db)==SQLITE_OK);

  execSql(db, "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT)");
  execSql(db, "CREATE TABLE posts(id INTEGER PRIMARY KEY, uid INTEGER, title TEXT)");
  execSql(db, "CREATE TABLE tags(id INTEGER PRIMARY KEY, pid INTEGER, tag TEXT)");
  execSql(db, "INSERT INTO users VALUES(1,'alice')");
  execSql(db, "INSERT INTO users VALUES(2,'bob')");
  execSql(db, "INSERT INTO posts VALUES(1,1,'Hello')");
  execSql(db, "INSERT INTO posts VALUES(2,2,'World')");
  execSql(db, "INSERT INTO tags VALUES(1,1,'intro')");
  queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'multi table init')");
  checkInvariants(db, "multi_table");
  checkCleanStatus(db, "multi_table");

  execSql(db, "INSERT INTO users VALUES(3,'charlie')");
  execSql(db, "INSERT INTO posts VALUES(3,3,'Third post')");
  execSql(db, "INSERT INTO tags VALUES(2,3,'new')");
  execSql(db, "DELETE FROM tags WHERE id=1");
  queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'multi table update')");
  checkInvariants(db, "multi_table_updated");
  checkCleanStatus(db, "multi_table_updated");

  sqlite3_close(db);
  removeDb(dbpath);
}

static void test_diverge_merge(void){
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_inv_divmerge.db";

  printf("--- Test 11: Diverge and merge ---\n");
  removeDb(dbpath);

  check("open_divmerge", sqlite3_open(dbpath, &db)==SQLITE_OK);

  execSql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)");
  execSql(db, "INSERT INTO t1 VALUES(1, 'shared')");
  queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'init')");

  queryScalarText(db, "SELECT dolt_branch('branch_a')");
  queryScalarText(db, "SELECT dolt_branch('branch_b')");

  queryScalarText(db, "SELECT dolt_checkout('branch_a')");
  execSql(db, "INSERT INTO t1 VALUES(100, 'from_a')");
  queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'branch_a work')");
  checkInvariants(db, "branch_a");

  queryScalarText(db, "SELECT dolt_checkout('branch_b')");
  execSql(db, "INSERT INTO t1 VALUES(200, 'from_b')");
  queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'branch_b work')");
  checkInvariants(db, "branch_b");

  queryScalarText(db, "SELECT dolt_checkout('main')");
  queryScalarText(db, "SELECT dolt_merge('branch_a')");
  checkInvariants(db, "merge_a_into_main");

  queryScalarText(db, "SELECT dolt_merge('branch_b')");
  checkInvariants(db, "merge_b_into_main");

  check("merged_all_rows", queryScalarInt(db, "SELECT count(*) FROM t1")==3);

  sqlite3_close(db);
  removeDb(dbpath);
}

static void test_gc_after_branch_delete(void){
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_inv_gcbr.db";

  printf("--- Test 12: GC after branch deletion ---\n");
  removeDb(dbpath);

  check("open_gcbr", sqlite3_open(dbpath, &db)==SQLITE_OK);

  execSql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)");
  execSql(db, "INSERT INTO t1 VALUES(1, 'init')");
  queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'init')");

  queryScalarText(db, "SELECT dolt_branch('ephemeral')");
  queryScalarText(db, "SELECT dolt_checkout('ephemeral')");
  execSql(db, "INSERT INTO t1 VALUES(2, 'ephemeral')");
  queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'ephemeral commit')");

  queryScalarText(db, "SELECT dolt_checkout('main')");
  queryScalarText(db, "SELECT dolt_branch('-d', 'ephemeral')");
  queryScalarText(db, "SELECT dolt_gc()");

  checkInvariants(db, "gc_after_branch_delete");
  checkCleanStatus(db, "gc_after_branch_delete");
  check("gcbr_data", queryScalarInt(db, "SELECT count(*) FROM t1")==1);

  sqlite3_close(db);
  removeDb(dbpath);
}

static void test_persistence(void){
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_inv_persist.db";

  printf("--- Test 13: Persistence across reopen ---\n");
  removeDb(dbpath);

  check("open_persist1", sqlite3_open(dbpath, &db)==SQLITE_OK);
  execSql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)");
  execSql(db, "INSERT INTO t1 VALUES(1, 'persistent')");
  queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'persist commit')");
  queryScalarText(db, "SELECT dolt_branch('saved')");
  sqlite3_close(db);

  db = 0;
  check("open_persist2", sqlite3_open(dbpath, &db)==SQLITE_OK);
  checkInvariants(db, "after_reopen");
  checkCleanStatus(db, "after_reopen");

  check("persist_data", strcmp(queryScalarText(db, "SELECT val FROM t1 WHERE id=1"),
                               "persistent")==0);
  check("persist_branches",
    queryScalarInt(db, "SELECT count(*) FROM dolt_branches")==2);

  sqlite3_close(db);
  removeDb(dbpath);
}

static void test_large_table(void){
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_inv_large.db";
  int i;

  printf("--- Test 14: Large table (500 rows) ---\n");
  removeDb(dbpath);

  check("open_large", sqlite3_open(dbpath, &db)==SQLITE_OK);

  execSql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)");
  execSql(db, "BEGIN");
  for( i=0; i<500; i++ ){
    char sql[128];
    snprintf(sql, sizeof(sql), "INSERT INTO t1 VALUES(%d, 'row_%d')", i, i);
    execSql(db, sql);
  }
  execSql(db, "COMMIT");
  queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'bulk insert 500 rows')");

  checkInvariants(db, "large_table");
  checkCleanStatus(db, "large_table");
  check("large_count", queryScalarInt(db, "SELECT count(*) FROM t1")==500);

  sqlite3_close(db);
  removeDb(dbpath);
}

static void test_drop_table(void){
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_inv_drop.db";

  printf("--- Test 15: Drop table ---\n");
  removeDb(dbpath);

  check("open_drop", sqlite3_open(dbpath, &db)==SQLITE_OK);

  execSql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)");
  execSql(db, "CREATE TABLE t2(id INTEGER PRIMARY KEY, val TEXT)");
  execSql(db, "INSERT INTO t1 VALUES(1, 'a')");
  execSql(db, "INSERT INTO t2 VALUES(1, 'b')");
  queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'two tables')");
  checkInvariants(db, "before_drop");

  execSql(db, "DROP TABLE t2");
  queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'drop t2')");
  checkInvariants(db, "after_drop");
  checkCleanStatus(db, "after_drop");

  const char *r = queryScalarText(db, "SELECT count(*) FROM t2");
  check("t2_gone", strncmp(r, "ERROR", 5)==0);

  sqlite3_close(db);
  removeDb(dbpath);
}

static void test_staging(void){
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_inv_staging.db";

  printf("--- Test 16: Staging workflow ---\n");
  removeDb(dbpath);

  check("open_staging", sqlite3_open(dbpath, &db)==SQLITE_OK);

  execSql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)");
  execSql(db, "INSERT INTO t1 VALUES(1, 'a')");
  queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'init')");

  execSql(db, "INSERT INTO t1 VALUES(2, 'b')");
  queryScalarText(db, "SELECT dolt_add('t1')");

  check("staging_has_staged",
    queryScalarInt(db, "SELECT count(*) FROM dolt_status WHERE staged=1") > 0);

  checkInvariants(db, "with_staged_changes");

  queryScalarText(db, "SELECT dolt_commit('-m', 'staged commit')");
  checkInvariants(db, "after_staged_commit");
  checkCleanStatus(db, "after_staged_commit");

  sqlite3_close(db);
  removeDb(dbpath);
}

static void test_reset_to_hash(void){
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_inv_resethash.db";

  printf("--- Test 17: Reset to specific commit hash ---\n");
  removeDb(dbpath);

  check("open_resethash", sqlite3_open(dbpath, &db)==SQLITE_OK);

  execSql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)");
  execSql(db, "INSERT INTO t1 VALUES(1, 'v1')");
  queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'c1')");

  const char *h = queryScalarText(db, "SELECT commit_hash FROM dolt_log LIMIT 1");
  char hash1[64];
  snprintf(hash1, sizeof(hash1), "%s", h);

  execSql(db, "INSERT INTO t1 VALUES(2, 'v2')");
  queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'c2')");

  execSql(db, "INSERT INTO t1 VALUES(3, 'v3')");
  queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'c3')");

  check("before_reset_3_commits",
    queryScalarInt(db, "SELECT count(*) FROM dolt_log")==4);

  {
    char sql[256];
    snprintf(sql, sizeof(sql), "SELECT dolt_reset('--hard','%s')", hash1);
    queryScalarText(db, sql);
  }

  checkInvariants(db, "after_reset_to_hash");
  checkCleanStatus(db, "after_reset_to_hash");
  check("reset_to_c1", queryScalarInt(db, "SELECT count(*) FROM t1")==1);

  sqlite3_close(db);
  removeDb(dbpath);
}

static void test_single_row_mutation_fast_path(void){
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_inv_single_mutation.db";
  int i;

  printf("--- Test 18: Single-row mutation fast path shape ---\n");
  removeDb(dbpath);

  check("open_single_mutation", sqlite3_open(dbpath, &db)==SQLITE_OK);

  execSql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)");
  execSql(db, "BEGIN");
  for( i=1; i<=5000; i++ ){
    char sql[128];
    snprintf(sql, sizeof(sql), "INSERT INTO t1 VALUES(%d, 'row_%d')", i, i);
    execSql(db, sql);
  }
  execSql(db, "COMMIT");

  execSql(db, "DELETE FROM t1 WHERE id=2500");
  execSql(db, "INSERT INTO t1 VALUES(2500, 'restored')");
  execSql(db, "DELETE FROM t1 WHERE id=3333");
  execSql(db, "INSERT INTO t1 VALUES(3333, 'again')");

  check("single_mutation_count",
    queryScalarInt(db, "SELECT count(*) FROM t1")==5000);
  check("single_mutation_min",
    queryScalarInt(db, "SELECT min(id) FROM t1")==1);
  check("single_mutation_max",
    queryScalarInt(db, "SELECT max(id) FROM t1")==5000);
  check("single_mutation_value_2500",
    strcmp(queryScalarText(db, "SELECT val FROM t1 WHERE id=2500"),
           "restored")==0);
  check("single_mutation_value_3333",
    strcmp(queryScalarText(db, "SELECT val FROM t1 WHERE id=3333"),
           "again")==0);
  check("single_mutation_integrity",
    strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);

  execSql(db, "CREATE TABLE tblob(id BLOB PRIMARY KEY, val TEXT, pad TEXT)");
  execSql(db, "BEGIN");
  for( i=1; i<=5000; i++ ){
    char sql[256];
    snprintf(sql, sizeof(sql),
      "INSERT INTO tblob VALUES(x'%032x', 'row_%d', 'pad')", i, i);
    execSql(db, sql);
  }
  execSql(db, "COMMIT");

  execSql(db,
    "UPDATE tblob SET val='short' WHERE id=x'000000000000000000000000000009c4'");
  execSql(db,
    "UPDATE tblob SET val='this replacement is intentionally longer' "
    "WHERE id=x'00000000000000000000000000000d05'");

  check("single_mutation_blob_count",
    queryScalarInt(db, "SELECT count(*) FROM tblob")==5000);
  check("single_mutation_blob_short",
    strcmp(queryScalarText(db,
      "SELECT val FROM tblob WHERE id=x'000000000000000000000000000009c4'"),
      "short")==0);
  check("single_mutation_blob_long",
    strcmp(queryScalarText(db,
      "SELECT val FROM tblob WHERE id=x'00000000000000000000000000000d05'"),
      "this replacement is intentionally longer")==0);
  check("single_mutation_blob_integrity",
    strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);

  execSql(db, "CREATE TABLE ttext(id TEXT PRIMARY KEY, k INTEGER, val TEXT)");
  execSql(db, "CREATE INDEX ttext_k ON ttext(k)");
  execSql(db, "BEGIN");
  for( i=1; i<=5000; i++ ){
    char sql[256];
    snprintf(sql, sizeof(sql),
      "INSERT INTO ttext VALUES('%032x', %d, 'row_%d')", i, i, i);
    execSql(db, sql);
  }
  execSql(db, "COMMIT");

  execSql(db,
    "UPDATE ttext SET k=8001 "
    "WHERE id='000000000000000000000000000009c4'");
  execSql(db,
    "UPDATE ttext SET k=8002 "
    "WHERE id='00000000000000000000000000000d05'");

  check("single_mutation_text_index_count",
    queryScalarInt(db, "SELECT count(*) FROM ttext")==5000);
  check("single_mutation_text_index_old_gone",
    queryScalarInt(db, "SELECT count(*) FROM ttext WHERE k IN (2500,3333)")==0);
  check("single_mutation_text_index_new",
    queryScalarInt(db, "SELECT count(*) FROM ttext WHERE k IN (8001,8002)")==2);
  check("single_mutation_text_index_integrity",
    strcmp(queryScalarText(db, "PRAGMA integrity_check"), "ok")==0);

  sqlite3_close(db);
  removeDb(dbpath);
}


int main(void){
  printf("=== DoltLite Invariant Tests ===\n\n");

  test_basic_commit();
  test_multi_branch();
  test_merge();
  test_insert_delete();
  test_sequential_commits();
  test_schema_change();
  test_gc();
  test_reset_hard();
  test_random_operations();
  test_multi_table();
  test_diverge_merge();
  test_gc_after_branch_delete();
  test_persistence();
  test_large_table();
  test_drop_table();
  test_staging();
  test_reset_to_hash();
  test_single_row_mutation_fast_path();

  printf("\n=== Results: %d passed, %d failed out of %d tests ===\n",
    nPass, nFail, nPass+nFail);
  return nFail > 0 ? 1 : 0;
}
