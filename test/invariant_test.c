/*
** Invariant test for DoltLite: verifies internal consistency of a
** DoltLite database after various operations.
**
** Uses SQL functions and virtual tables to inspect the state of
** branches, commits, working set, and tables.
**
** Build (from the build/ directory):
**   cc -g -I. -o invariant_test ../test/invariant_test.c libdoltlite.a -lz -lpthread
*/
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

/* Execute SQL, return first column of first row as string (static buffer). */
static char result_buf[8192];
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
    if( val ) snprintf(result_buf, sizeof(result_buf), "%s", val);
  }else if( rc==SQLITE_ERROR ){
    snprintf(result_buf, sizeof(result_buf), "ERROR: %s", sqlite3_errmsg(db));
  }
  sqlite3_finalize(stmt);
  return result_buf;
}

/* Execute SQL, return integer result. Returns -1 on error. */
static int exec1_int(sqlite3 *db, const char *sql){
  const char *r = exec1(db, sql);
  if( strncmp(r, "ERROR", 5)==0 ) return -1;
  return atoi(r);
}

/* Execute SQL, ignore result. */
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

/* Return 1 if string is a valid 40-char hex hash. */
static int isValidHash(const char *s){
  int i;
  if( !s || strlen(s)!=40 ) return 0;
  for( i=0; i<40; i++ ){
    if( !isxdigit((unsigned char)s[i]) ) return 0;
  }
  return 1;
}

/*
** checkInvariants: verify internal consistency of a DoltLite database.
** Returns the number of invariant violations found.
*/
static int checkInvariants(sqlite3 *db, const char *context){
  int violations = 0;
  char label[256];

  /* --- Invariant 1: Every branch has a valid 40-hex-char commit hash --- */
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

  /* --- Invariant 2: Every commit in dolt_log has required fields --- */
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

  /* --- Invariant 3: active_branch() returns a non-empty string
  **     that exists in dolt_branches --- */
  {
    const char *branch = exec1(db, "SELECT active_branch()");
    snprintf(label, sizeof(label), "%s: active_branch() non-empty", context);
    if( !branch || strlen(branch)==0 || strncmp(branch,"ERROR",5)==0 ){
      check(label, 0); violations++;
    }else{
      check(label, 1);

      /* Verify the active branch exists in dolt_branches */
      char sql[512];
      snprintf(sql, sizeof(sql),
        "SELECT count(*) FROM dolt_branches WHERE name='%s'", branch);
      int cnt = exec1_int(db, sql);
      snprintf(label, sizeof(label),
        "%s: active_branch '%s' exists in dolt_branches", context, branch);
      if( cnt!=1 ){
        check(label, 0); violations++;
      }else{
        check(label, 1);
      }
    }
  }

  /* --- Invariant 4: Every table in sqlite_master is queryable --- */
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
          int cnt = exec1_int(db, sql);
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

  /* --- Invariant 5: dolt_status doesn't crash and returns valid rows --- */
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
        /* Just iterate; if it crashes, the test crashes. */
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

/*
** checkCleanStatus: verify that dolt_status shows no uncommitted changes.
*/
static void checkCleanStatus(sqlite3 *db, const char *context){
  char label[256];
  int cnt = exec1_int(db, "SELECT count(*) FROM dolt_status");
  snprintf(label, sizeof(label),
    "%s: dolt_status shows no uncommitted changes", context);
  check(label, cnt==0);
}

/*
** Helper to remove DB files.
*/
static void removeDb(const char *path){
  char wal[512];
  remove(path);
  snprintf(wal, sizeof(wal), "%s-wal", path);
  remove(wal);
}

/* ========================================================================
** Test scenarios
** ======================================================================== */

/*
** Test 1: Create table, insert, commit -> check invariants.
*/
static void test_basic_commit(void){
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_inv_basic.db";

  printf("--- Test 1: Basic create/insert/commit ---\n");
  removeDb(dbpath);

  check("open_basic", sqlite3_open(dbpath, &db)==SQLITE_OK);
  execsql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT)");
  execsql(db, "INSERT INTO t1 VALUES(1, 'alice')");
  execsql(db, "INSERT INTO t1 VALUES(2, 'bob')");
  exec1(db, "SELECT dolt_commit('-A', '-m', 'Initial commit')");

  checkInvariants(db, "basic_commit");
  checkCleanStatus(db, "basic_commit");

  sqlite3_close(db);
  removeDb(dbpath);
}

/*
** Test 2: Create 3 branches, commit on each -> check invariants on each.
*/
static void test_multi_branch(void){
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_inv_multibranch.db";

  printf("--- Test 2: Multiple branches with commits ---\n");
  removeDb(dbpath);

  check("open_multibranch", sqlite3_open(dbpath, &db)==SQLITE_OK);

  /* Initial commit on main */
  execsql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)");
  execsql(db, "INSERT INTO t1 VALUES(1, 'main-init')");
  exec1(db, "SELECT dolt_commit('-A', '-m', 'init on main')");

  /* Create branches */
  exec1(db, "SELECT dolt_branch('dev')");
  exec1(db, "SELECT dolt_branch('staging')");

  /* Commit on dev */
  exec1(db, "SELECT dolt_checkout('dev')");
  execsql(db, "INSERT INTO t1 VALUES(2, 'dev-row')");
  exec1(db, "SELECT dolt_commit('-A', '-m', 'dev commit')");
  checkInvariants(db, "multibranch_dev");
  checkCleanStatus(db, "multibranch_dev");

  /* Commit on staging */
  exec1(db, "SELECT dolt_checkout('staging')");
  execsql(db, "INSERT INTO t1 VALUES(3, 'staging-row')");
  exec1(db, "SELECT dolt_commit('-A', '-m', 'staging commit')");
  checkInvariants(db, "multibranch_staging");
  checkCleanStatus(db, "multibranch_staging");

  /* Back to main, verify */
  exec1(db, "SELECT dolt_checkout('main')");
  checkInvariants(db, "multibranch_main");
  checkCleanStatus(db, "multibranch_main");

  /* Verify branch count */
  check("three_branches", exec1_int(db, "SELECT count(*) FROM dolt_branches")==3);

  sqlite3_close(db);
  removeDb(dbpath);
}

/*
** Test 3: Merge two branches -> check invariants.
*/
static void test_merge(void){
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_inv_merge.db";

  printf("--- Test 3: Merge two branches ---\n");
  removeDb(dbpath);

  check("open_merge", sqlite3_open(dbpath, &db)==SQLITE_OK);

  execsql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)");
  execsql(db, "INSERT INTO t1 VALUES(1, 'init')");
  exec1(db, "SELECT dolt_commit('-A', '-m', 'init')");

  exec1(db, "SELECT dolt_branch('feature')");
  exec1(db, "SELECT dolt_checkout('feature')");
  execsql(db, "INSERT INTO t1 VALUES(2, 'feature-data')");
  exec1(db, "SELECT dolt_commit('-A', '-m', 'feature commit')");

  exec1(db, "SELECT dolt_checkout('main')");
  execsql(db, "INSERT INTO t1 VALUES(3, 'main-data')");
  exec1(db, "SELECT dolt_commit('-A', '-m', 'main commit')");

  exec1(db, "SELECT dolt_merge('feature')");
  checkInvariants(db, "after_merge");

  /* After merge commit, all 3 rows should exist */
  check("merge_row_count", exec1_int(db, "SELECT count(*) FROM t1")==3);

  sqlite3_close(db);
  removeDb(dbpath);
}

/*
** Test 4: Create table, add rows, commit, delete some, commit -> check.
*/
static void test_insert_delete(void){
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_inv_del.db";

  printf("--- Test 4: Insert then delete rows ---\n");
  removeDb(dbpath);

  check("open_del", sqlite3_open(dbpath, &db)==SQLITE_OK);

  execsql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)");
  execsql(db, "INSERT INTO t1 VALUES(1, 'a')");
  execsql(db, "INSERT INTO t1 VALUES(2, 'b')");
  execsql(db, "INSERT INTO t1 VALUES(3, 'c')");
  execsql(db, "INSERT INTO t1 VALUES(4, 'd')");
  exec1(db, "SELECT dolt_commit('-A', '-m', 'add 4 rows')");
  checkInvariants(db, "insert_phase");
  checkCleanStatus(db, "insert_phase");

  execsql(db, "DELETE FROM t1 WHERE id IN (2, 4)");
  exec1(db, "SELECT dolt_commit('-A', '-m', 'delete rows 2 and 4')");
  checkInvariants(db, "delete_phase");
  checkCleanStatus(db, "delete_phase");

  check("remaining_rows", exec1_int(db, "SELECT count(*) FROM t1")==2);

  sqlite3_close(db);
  removeDb(dbpath);
}

/*
** Test 5: 10 sequential commits with mixed operations -> check after each.
*/
static void test_sequential_commits(void){
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_inv_seq.db";
  int i;

  printf("--- Test 5: 10 sequential commits ---\n");
  removeDb(dbpath);

  check("open_seq", sqlite3_open(dbpath, &db)==SQLITE_OK);

  execsql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)");
  exec1(db, "SELECT dolt_commit('-A', '-m', 'create table')");
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
    execsql(db, sql);
    snprintf(msg, sizeof(msg), "SELECT dolt_commit('-A', '-m', 'commit %d')", i);
    exec1(db, msg);
    snprintf(ctx, sizeof(ctx), "seq_%d", i);
    checkInvariants(db, ctx);
    checkCleanStatus(db, ctx);
  }

  /* Verify log has 11 commits (create + 10 sequential) */
  check("seq_log_count", exec1_int(db, "SELECT count(*) FROM dolt_log")==11);

  sqlite3_close(db);
  removeDb(dbpath);
}

/*
** Test 6: Branch, modify schema (add column), commit, checkout original.
*/
static void test_schema_change(void){
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_inv_schema.db";

  printf("--- Test 6: Schema change on branch ---\n");
  removeDb(dbpath);

  check("open_schema", sqlite3_open(dbpath, &db)==SQLITE_OK);

  execsql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT)");
  execsql(db, "INSERT INTO t1 VALUES(1, 'alice')");
  exec1(db, "SELECT dolt_commit('-A', '-m', 'init')");

  exec1(db, "SELECT dolt_branch('schema-change')");
  exec1(db, "SELECT dolt_checkout('schema-change')");
  execsql(db, "ALTER TABLE t1 ADD COLUMN age INTEGER DEFAULT 0");
  execsql(db, "UPDATE t1 SET age=30 WHERE id=1");
  exec1(db, "SELECT dolt_commit('-A', '-m', 'add age column')");
  checkInvariants(db, "schema_branch");
  checkCleanStatus(db, "schema_branch");

  /* Switch back to original branch */
  exec1(db, "SELECT dolt_checkout('main')");
  checkInvariants(db, "schema_main_after");
  checkCleanStatus(db, "schema_main_after");

  /* Main should not have the age column */
  const char *r = exec1(db, "SELECT age FROM t1");
  check("main_no_age_column", strncmp(r, "ERROR", 5)==0);

  sqlite3_close(db);
  removeDb(dbpath);
}

/*
** Test 7: GC -> check invariants.
*/
static void test_gc(void){
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_inv_gc.db";

  printf("--- Test 7: GC preserves invariants ---\n");
  removeDb(dbpath);

  check("open_gc", sqlite3_open(dbpath, &db)==SQLITE_OK);

  execsql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)");
  execsql(db, "INSERT INTO t1 VALUES(1, 'a')");
  exec1(db, "SELECT dolt_commit('-A', '-m', 'c1')");
  execsql(db, "INSERT INTO t1 VALUES(2, 'b')");
  exec1(db, "SELECT dolt_commit('-A', '-m', 'c2')");
  execsql(db, "INSERT INTO t1 VALUES(3, 'c')");
  exec1(db, "SELECT dolt_commit('-A', '-m', 'c3')");

  exec1(db, "SELECT dolt_gc()");
  checkInvariants(db, "after_gc");
  checkCleanStatus(db, "after_gc");

  check("gc_data_intact", exec1_int(db, "SELECT count(*) FROM t1")==3);
  check("gc_log_intact", exec1_int(db, "SELECT count(*) FROM dolt_log")==3);

  sqlite3_close(db);
  removeDb(dbpath);
}

/*
** Test 8: Reset --hard -> check invariants.
*/
static void test_reset_hard(void){
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_inv_reset.db";

  printf("--- Test 8: Reset --hard preserves invariants ---\n");
  removeDb(dbpath);

  check("open_reset", sqlite3_open(dbpath, &db)==SQLITE_OK);

  execsql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)");
  execsql(db, "INSERT INTO t1 VALUES(1, 'original')");
  exec1(db, "SELECT dolt_commit('-A', '-m', 'init')");

  /* Make uncommitted changes */
  execsql(db, "INSERT INTO t1 VALUES(2, 'uncommitted')");
  execsql(db, "UPDATE t1 SET val='modified' WHERE id=1");

  /* Verify dirty status before reset */
  check("dirty_before_reset",
    exec1_int(db, "SELECT count(*) FROM dolt_status") > 0);

  exec1(db, "SELECT dolt_reset('--hard')");
  checkInvariants(db, "after_reset");
  checkCleanStatus(db, "after_reset");

  /* Data reverted */
  check("reset_data", strcmp(exec1(db, "SELECT val FROM t1 WHERE id=1"),
                             "original")==0);
  check("reset_row_count", exec1_int(db, "SELECT count(*) FROM t1")==1);

  sqlite3_close(db);
  removeDb(dbpath);
}

/*
** Test 9: Random sequence of 100 operations -> check invariants at end.
*/
static void test_random_operations(void){
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_inv_random.db";
  int i;

  printf("--- Test 9: 100 random operations ---\n");
  removeDb(dbpath);

  check("open_random", sqlite3_open(dbpath, &db)==SQLITE_OK);

  execsql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)");
  exec1(db, "SELECT dolt_commit('-A', '-m', 'init')");

  srand(42); /* Deterministic seed */
  for( i=0; i<100; i++ ){
    char sql[256];
    int op = rand() % 5;
    switch( op ){
      case 0: /* INSERT */
        snprintf(sql, sizeof(sql),
          "INSERT OR IGNORE INTO t1 VALUES(%d, 'v%d')", rand()%500, i);
        execsql(db, sql);
        break;
      case 1: /* UPDATE */
        snprintf(sql, sizeof(sql),
          "UPDATE t1 SET val='u%d' WHERE id=(SELECT id FROM t1 ORDER BY RANDOM() LIMIT 1)", i);
        execsql(db, sql);
        break;
      case 2: /* DELETE */
        execsql(db, "DELETE FROM t1 WHERE id=(SELECT id FROM t1 ORDER BY RANDOM() LIMIT 1)");
        break;
      case 3: /* COMMIT */
        snprintf(sql, sizeof(sql),
          "SELECT dolt_commit('-A', '-m', 'random op %d')", i);
        exec1(db, sql);
        break;
      case 4: /* SELECT (read-only, verify no crash) */
        exec1(db, "SELECT count(*) FROM t1");
        break;
    }
  }

  /* Final commit to ensure everything is committed */
  exec1(db, "SELECT dolt_commit('-A', '-m', 'final random commit')");
  checkInvariants(db, "after_random_100");
  checkCleanStatus(db, "after_random_100");

  sqlite3_close(db);
  removeDb(dbpath);
}

/*
** Test 10: Multiple tables with foreign-key-like relationships.
*/
static void test_multi_table(void){
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_inv_multi.db";

  printf("--- Test 10: Multiple tables ---\n");
  removeDb(dbpath);

  check("open_multi", sqlite3_open(dbpath, &db)==SQLITE_OK);

  execsql(db, "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT)");
  execsql(db, "CREATE TABLE posts(id INTEGER PRIMARY KEY, uid INTEGER, title TEXT)");
  execsql(db, "CREATE TABLE tags(id INTEGER PRIMARY KEY, pid INTEGER, tag TEXT)");
  execsql(db, "INSERT INTO users VALUES(1,'alice')");
  execsql(db, "INSERT INTO users VALUES(2,'bob')");
  execsql(db, "INSERT INTO posts VALUES(1,1,'Hello')");
  execsql(db, "INSERT INTO posts VALUES(2,2,'World')");
  execsql(db, "INSERT INTO tags VALUES(1,1,'intro')");
  exec1(db, "SELECT dolt_commit('-A', '-m', 'multi table init')");
  checkInvariants(db, "multi_table");
  checkCleanStatus(db, "multi_table");

  /* Modify multiple tables */
  execsql(db, "INSERT INTO users VALUES(3,'charlie')");
  execsql(db, "INSERT INTO posts VALUES(3,3,'Third post')");
  execsql(db, "INSERT INTO tags VALUES(2,3,'new')");
  execsql(db, "DELETE FROM tags WHERE id=1");
  exec1(db, "SELECT dolt_commit('-A', '-m', 'multi table update')");
  checkInvariants(db, "multi_table_updated");
  checkCleanStatus(db, "multi_table_updated");

  sqlite3_close(db);
  removeDb(dbpath);
}

/*
** Test 11: Branch, diverge, merge with non-conflicting changes.
*/
static void test_diverge_merge(void){
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_inv_divmerge.db";

  printf("--- Test 11: Diverge and merge ---\n");
  removeDb(dbpath);

  check("open_divmerge", sqlite3_open(dbpath, &db)==SQLITE_OK);

  execsql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)");
  execsql(db, "INSERT INTO t1 VALUES(1, 'shared')");
  exec1(db, "SELECT dolt_commit('-A', '-m', 'init')");

  exec1(db, "SELECT dolt_branch('branch_a')");
  exec1(db, "SELECT dolt_branch('branch_b')");

  /* Diverge on branch_a */
  exec1(db, "SELECT dolt_checkout('branch_a')");
  execsql(db, "INSERT INTO t1 VALUES(100, 'from_a')");
  exec1(db, "SELECT dolt_commit('-A', '-m', 'branch_a work')");
  checkInvariants(db, "branch_a");

  /* Diverge on branch_b */
  exec1(db, "SELECT dolt_checkout('branch_b')");
  execsql(db, "INSERT INTO t1 VALUES(200, 'from_b')");
  exec1(db, "SELECT dolt_commit('-A', '-m', 'branch_b work')");
  checkInvariants(db, "branch_b");

  /* Merge branch_a into main */
  exec1(db, "SELECT dolt_checkout('main')");
  exec1(db, "SELECT dolt_merge('branch_a')");
  checkInvariants(db, "merge_a_into_main");

  /* Merge branch_b into main */
  exec1(db, "SELECT dolt_merge('branch_b')");
  checkInvariants(db, "merge_b_into_main");

  check("merged_all_rows", exec1_int(db, "SELECT count(*) FROM t1")==3);

  sqlite3_close(db);
  removeDb(dbpath);
}

/*
** Test 12: GC after branch deletion -> check invariants.
*/
static void test_gc_after_branch_delete(void){
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_inv_gcbr.db";

  printf("--- Test 12: GC after branch deletion ---\n");
  removeDb(dbpath);

  check("open_gcbr", sqlite3_open(dbpath, &db)==SQLITE_OK);

  execsql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)");
  execsql(db, "INSERT INTO t1 VALUES(1, 'init')");
  exec1(db, "SELECT dolt_commit('-A', '-m', 'init')");

  exec1(db, "SELECT dolt_branch('ephemeral')");
  exec1(db, "SELECT dolt_checkout('ephemeral')");
  execsql(db, "INSERT INTO t1 VALUES(2, 'ephemeral')");
  exec1(db, "SELECT dolt_commit('-A', '-m', 'ephemeral commit')");

  exec1(db, "SELECT dolt_checkout('main')");
  exec1(db, "SELECT dolt_branch('-d', 'ephemeral')");
  exec1(db, "SELECT dolt_gc()");

  checkInvariants(db, "gc_after_branch_delete");
  checkCleanStatus(db, "gc_after_branch_delete");
  check("gcbr_data", exec1_int(db, "SELECT count(*) FROM t1")==1);

  sqlite3_close(db);
  removeDb(dbpath);
}

/*
** Test 13: Reopen database -> check invariants persist.
*/
static void test_persistence(void){
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_inv_persist.db";

  printf("--- Test 13: Persistence across reopen ---\n");
  removeDb(dbpath);

  check("open_persist1", sqlite3_open(dbpath, &db)==SQLITE_OK);
  execsql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)");
  execsql(db, "INSERT INTO t1 VALUES(1, 'persistent')");
  exec1(db, "SELECT dolt_commit('-A', '-m', 'persist commit')");
  exec1(db, "SELECT dolt_branch('saved')");
  sqlite3_close(db);

  /* Reopen */
  db = 0;
  check("open_persist2", sqlite3_open(dbpath, &db)==SQLITE_OK);
  checkInvariants(db, "after_reopen");
  checkCleanStatus(db, "after_reopen");

  check("persist_data", strcmp(exec1(db, "SELECT val FROM t1 WHERE id=1"),
                               "persistent")==0);
  check("persist_branches",
    exec1_int(db, "SELECT count(*) FROM dolt_branches")==2);

  sqlite3_close(db);
  removeDb(dbpath);
}

/*
** Test 14: Large number of rows -> check invariants.
*/
static void test_large_table(void){
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_inv_large.db";
  int i;

  printf("--- Test 14: Large table (500 rows) ---\n");
  removeDb(dbpath);

  check("open_large", sqlite3_open(dbpath, &db)==SQLITE_OK);

  execsql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)");
  execsql(db, "BEGIN");
  for( i=0; i<500; i++ ){
    char sql[128];
    snprintf(sql, sizeof(sql), "INSERT INTO t1 VALUES(%d, 'row_%d')", i, i);
    execsql(db, sql);
  }
  execsql(db, "COMMIT");
  exec1(db, "SELECT dolt_commit('-A', '-m', 'bulk insert 500 rows')");

  checkInvariants(db, "large_table");
  checkCleanStatus(db, "large_table");
  check("large_count", exec1_int(db, "SELECT count(*) FROM t1")==500);

  sqlite3_close(db);
  removeDb(dbpath);
}

/*
** Test 15: Drop table, commit, check invariants.
*/
static void test_drop_table(void){
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_inv_drop.db";

  printf("--- Test 15: Drop table ---\n");
  removeDb(dbpath);

  check("open_drop", sqlite3_open(dbpath, &db)==SQLITE_OK);

  execsql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)");
  execsql(db, "CREATE TABLE t2(id INTEGER PRIMARY KEY, val TEXT)");
  execsql(db, "INSERT INTO t1 VALUES(1, 'a')");
  execsql(db, "INSERT INTO t2 VALUES(1, 'b')");
  exec1(db, "SELECT dolt_commit('-A', '-m', 'two tables')");
  checkInvariants(db, "before_drop");

  execsql(db, "DROP TABLE t2");
  exec1(db, "SELECT dolt_commit('-A', '-m', 'drop t2')");
  checkInvariants(db, "after_drop");
  checkCleanStatus(db, "after_drop");

  /* t2 should no longer exist */
  const char *r = exec1(db, "SELECT count(*) FROM t2");
  check("t2_gone", strncmp(r, "ERROR", 5)==0);

  sqlite3_close(db);
  removeDb(dbpath);
}

/*
** Test 16: Staging workflow -> check invariants.
*/
static void test_staging(void){
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_inv_staging.db";

  printf("--- Test 16: Staging workflow ---\n");
  removeDb(dbpath);

  check("open_staging", sqlite3_open(dbpath, &db)==SQLITE_OK);

  execsql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)");
  execsql(db, "INSERT INTO t1 VALUES(1, 'a')");
  exec1(db, "SELECT dolt_commit('-A', '-m', 'init')");

  /* Make changes, stage manually */
  execsql(db, "INSERT INTO t1 VALUES(2, 'b')");
  exec1(db, "SELECT dolt_add('t1')");

  /* Status should show staged changes */
  check("staging_has_staged",
    exec1_int(db, "SELECT count(*) FROM dolt_status WHERE staged=1") > 0);

  checkInvariants(db, "with_staged_changes");

  /* Commit staged changes */
  exec1(db, "SELECT dolt_commit('-m', 'staged commit')");
  checkInvariants(db, "after_staged_commit");
  checkCleanStatus(db, "after_staged_commit");

  sqlite3_close(db);
  removeDb(dbpath);
}

/*
** Test 17: Reset to specific commit hash.
*/
static void test_reset_to_hash(void){
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_inv_resethash.db";

  printf("--- Test 17: Reset to specific commit hash ---\n");
  removeDb(dbpath);

  check("open_resethash", sqlite3_open(dbpath, &db)==SQLITE_OK);

  execsql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)");
  execsql(db, "INSERT INTO t1 VALUES(1, 'v1')");
  exec1(db, "SELECT dolt_commit('-A', '-m', 'c1')");

  /* Save first commit hash */
  const char *h = exec1(db, "SELECT commit_hash FROM dolt_log LIMIT 1");
  char hash1[64];
  snprintf(hash1, sizeof(hash1), "%s", h);

  execsql(db, "INSERT INTO t1 VALUES(2, 'v2')");
  exec1(db, "SELECT dolt_commit('-A', '-m', 'c2')");

  execsql(db, "INSERT INTO t1 VALUES(3, 'v3')");
  exec1(db, "SELECT dolt_commit('-A', '-m', 'c3')");

  check("before_reset_3_commits",
    exec1_int(db, "SELECT count(*) FROM dolt_log")==3);

  /* Reset to first commit */
  {
    char sql[256];
    snprintf(sql, sizeof(sql), "SELECT dolt_reset('--hard','%s')", hash1);
    exec1(db, sql);
  }

  checkInvariants(db, "after_reset_to_hash");
  checkCleanStatus(db, "after_reset_to_hash");
  check("reset_to_c1", exec1_int(db, "SELECT count(*) FROM t1")==1);

  sqlite3_close(db);
  removeDb(dbpath);
}

/* ========================================================================
** Main
** ======================================================================== */

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

  printf("\n=== Results: %d passed, %d failed out of %d tests ===\n",
    nPass, nFail, nPass+nFail);
  return nFail > 0 ? 1 : 0;
}
