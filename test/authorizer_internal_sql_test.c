#include <stdio.h>
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

/* Deny everything a sandbox would deny while still permitting the statement
** itself: the user issues no PRAGMA and reads no table, so a version control
** command must not fail on the statements it runs internally. */
static int authSandbox(
  void *pArg, int code,
  const char *z1, const char *z2, const char *z3, const char *z4
){
  (void)pArg; (void)z1; (void)z2; (void)z3; (void)z4;
  if( code==SQLITE_SELECT || code==SQLITE_FUNCTION ) return SQLITE_OK;
  return SQLITE_DENY;
}

/* Refusing one dolt_* function must still work: the shield covers the
** implementation, never the decision to call it. */
static int authDenyCommit(
  void *pArg, int code,
  const char *z1, const char *z2, const char *z3, const char *z4
){
  (void)pArg; (void)z1; (void)z3; (void)z4;
  if( code==SQLITE_FUNCTION && z2 && strcmp(z2, "dolt_commit")==0 ){
    return SQLITE_DENY;
  }
  return SQLITE_OK;
}

/* Deny reads of the placeholder table a declared vtab schema names. It does
** not exist, so only our own declaration can be asking about it. */
static int authDenyPlaceholder(
  void *pArg, int code,
  const char *z1, const char *z2, const char *z3, const char *z4
){
  (void)pArg; (void)z2; (void)z3; (void)z4;
  if( code==SQLITE_READ && z1 && strcmp(z1, "x")==0 ) return SQLITE_DENY;
  return SQLITE_OK;
}

static int authDenyAll(
  void *pArg, int code,
  const char *z1, const char *z2, const char *z3, const char *z4
){
  (void)pArg; (void)code; (void)z1; (void)z2; (void)z3; (void)z4;
  return SQLITE_DENY;
}

static int exec(sqlite3 *db, const char *zSql){
  return sqlite3_exec(db, zSql, 0, 0, 0);
}

static sqlite3 *openSeeded(const char *zPath){
  sqlite3 *db = 0;
  remove(zPath);
  if( sqlite3_open(zPath, &db)!=SQLITE_OK ) return 0;
  if( exec(db, "CREATE TABLE t(k INTEGER PRIMARY KEY, v TEXT);"
               "INSERT INTO t VALUES(1,'a');"
               "SELECT dolt_commit('-Am','base');")!=SQLITE_OK ){
    sqlite3_close(db);
    return 0;
  }
  return db;
}

static void test_commands_run_under_a_sandbox(const char *zPath){
  sqlite3 *db = openSeeded(zPath);
  check("sandbox: open", db!=0);
  if( !db ) return;
  check("sandbox: dirty the table", exec(db, "INSERT INTO t VALUES(2,'b')")==SQLITE_OK);
  sqlite3_set_authorizer(db, authSandbox, 0);

  check("sandbox: dolt_add", exec(db, "SELECT dolt_add('-A')")==SQLITE_OK);
  check("sandbox: dolt_commit", exec(db, "SELECT dolt_commit('-m','c2')")==SQLITE_OK);
  check("sandbox: dolt_branch", exec(db, "SELECT dolt_branch('b1')")==SQLITE_OK);
  check("sandbox: dolt_checkout", exec(db, "SELECT dolt_checkout('b1')")==SQLITE_OK);
  check("sandbox: dolt_merge", exec(db, "SELECT dolt_merge('main')")==SQLITE_OK);
  check("sandbox: dolt_tag", exec(db, "SELECT dolt_tag('v1')")==SQLITE_OK);
  check("sandbox: dolt_reset", exec(db, "SELECT dolt_reset('--hard')")==SQLITE_OK);
  check("sandbox: dolt_gc", exec(db, "SELECT dolt_gc()")==SQLITE_OK);

  sqlite3_set_authorizer(db, 0, 0);
  sqlite3_close(db);
  remove(zPath);
}

static void test_function_denial_still_applies(const char *zPath){
  sqlite3 *db = openSeeded(zPath);
  char *zErr = 0;
  int rc;
  check("deny: open", db!=0);
  if( !db ) return;
  check("deny: dirty the table", exec(db, "INSERT INTO t VALUES(2,'b')")==SQLITE_OK);
  sqlite3_set_authorizer(db, authDenyCommit, 0);

  rc = sqlite3_exec(db, "SELECT dolt_commit('-m','nope')", 0, 0, &zErr);
  check("deny: dolt_commit refused", rc!=SQLITE_OK);
  check("deny: names the function",
        zErr!=0 && strstr(zErr, "dolt_commit")!=0);
  sqlite3_free(zErr);
  check("deny: other commands unaffected",
        exec(db, "SELECT dolt_add('-A')")==SQLITE_OK);

  sqlite3_set_authorizer(db, 0, 0);
  sqlite3_close(db);
  remove(zPath);
}

/* Lazy system tables declare a placeholder schema at connect time. Reads of
** that placeholder must not reach the authorizer, or refusing them fails the
** constructor for a table the user is allowed to read. */
static void test_lazy_vtabs_ignore_placeholder_denial(const char *zPath){
  sqlite3 *db = openSeeded(zPath);
  check("placeholder: open", db!=0);
  if( !db ) return;
  sqlite3_set_authorizer(db, authDenyPlaceholder, 0);
  check("placeholder: dolt_docs",
        exec(db, "SELECT count(*) FROM dolt_docs")==SQLITE_OK);
  check("placeholder: dolt_ignore",
        exec(db, "SELECT count(*) FROM dolt_ignore")==SQLITE_OK);
  check("placeholder: dolt_tests",
        exec(db, "SELECT count(*) FROM dolt_tests")==SQLITE_OK);
  sqlite3_set_authorizer(db, 0, 0);
  sqlite3_close(db);
  remove(zPath);
}

static void test_deny_all_refuses_without_crashing(const char *zPath){
  sqlite3 *db = openSeeded(zPath);
  check("denyall: open", db!=0);
  if( !db ) return;
  sqlite3_set_authorizer(db, authDenyAll, 0);
  check("denyall: dolt_commit refused",
        exec(db, "SELECT dolt_commit('-m','nope')")!=SQLITE_OK);
  check("denyall: dolt_log refused",
        exec(db, "SELECT count(*) FROM dolt_log")!=SQLITE_OK);
  sqlite3_set_authorizer(db, 0, 0);
  sqlite3_close(db);
  remove(zPath);
}

/* dolt_test_run installs its own authorizer to sandbox user-authored test
** queries. The shield must not disarm it. */
static void test_dolt_tests_sandbox_survives(const char *zPath){
  sqlite3 *db = openSeeded(zPath);
  sqlite3_stmt *pStmt = 0;
  int sawWriteRefusal = 0;
  int rc;
  check("tests: open", db!=0);
  if( !db ) return;
  check("tests: seed", exec(db,
      "INSERT INTO dolt_tests(test_name,test_group,test_query,"
      "assertion_type,assertion_comparator,assertion_value) "
      "VALUES('w','g','INSERT INTO t VALUES(9,''x'')',"
      "'expected_rows','==','1');")==SQLITE_OK);

  rc = sqlite3_prepare_v2(db,
      "SELECT status, message FROM dolt_test_run('*')", -1, &pStmt, 0);
  check("tests: prepare run", rc==SQLITE_OK);
  while( rc==SQLITE_OK && sqlite3_step(pStmt)==SQLITE_ROW ){
    const char *zMsg = (const char*)sqlite3_column_text(pStmt, 1);
    if( zMsg && strstr(zMsg, "write")!=0 ) sawWriteRefusal = 1;
  }
  sqlite3_finalize(pStmt);
  check("tests: write query still refused", sawWriteRefusal);

  sqlite3_close(db);
  remove(zPath);
}

/* The user issues no PRAGMA, so a version control read surface must not fail
** because its constructor asked for one. */
static int authDenyPragma(
  void *pArg, int code,
  const char *z1, const char *z2, const char *z3, const char *z4
){
  (void)pArg; (void)z1; (void)z2; (void)z3; (void)z4;
  return code==SQLITE_PRAGMA ? SQLITE_DENY : SQLITE_OK;
}

/* Likewise for the catalog: the user reads dolt_status, not sqlite_master. */
static int authDenyMaster(
  void *pArg, int code,
  const char *z1, const char *z2, const char *z3, const char *z4
){
  (void)pArg; (void)z2; (void)z3; (void)z4;
  if( code==SQLITE_READ && z1 && strcmp(z1, "sqlite_master")==0 ){
    return SQLITE_DENY;
  }
  return SQLITE_OK;
}

static int authDenyReadOfT(
  void *pArg, int code,
  const char *z1, const char *z2, const char *z3, const char *z4
){
  (void)pArg; (void)z2; (void)z3; (void)z4;
  if( code==SQLITE_READ && z1 && strcmp(z1, "t")==0 ) return SQLITE_DENY;
  return SQLITE_OK;
}

static const char *azReadSurface[] = {
  "SELECT count(*) FROM dolt_status",
  "SELECT count(*) FROM dolt_log",
  "SELECT count(*) FROM dolt_branches",
  "SELECT count(*) FROM dolt_tags",
  "SELECT count(*) FROM dolt_diff",
  "SELECT count(*) FROM dolt_diff_t",
  "SELECT count(*) FROM dolt_history_t",
  "SELECT count(*) FROM dolt_blame_t",
  "SELECT count(*) FROM dolt_workspace_t",
  "SELECT count(*) FROM dolt_at_t('HEAD')",
  "SELECT count(*) FROM dolt_conflicts",
  "SELECT count(*) FROM dolt_constraint_violations",
  "SELECT count(*) FROM dolt_merge_status",
  "SELECT count(*) FROM dolt_schemas",
  "SELECT count(*) FROM dolt_remotes",
  "SELECT count(*) FROM dolt_commit_ancestors",
  "SELECT count(*) FROM dolt_diff_summary('HEAD','WORKING')",
  "SELECT count(*) FROM dolt_diff_stat('HEAD','WORKING')",
  "SELECT count(*) FROM dolt_schema_diff('HEAD','WORKING')",
  "SELECT count(*) FROM dolt_patch('HEAD','WORKING')",
  "SELECT length(dolt_hashof('HEAD'))",
  "SELECT length(dolt_hashof_table('t'))",
  "SELECT length(dolt_hashof_db())",
  "SELECT length(dolt_hashof_catalog())",
  "SELECT length(dolt_hashof_index('t_v'))",
};

static void runReadSurfaces(
  const char *zPath,
  const char *zLabel,
  int (*xAuth)(void*,int,const char*,const char*,const char*,const char*)
){
  sqlite3 *db = openSeeded(zPath);
  size_t i;
  char zName[160];
  snprintf(zName, sizeof(zName), "%s: open", zLabel);
  check(zName, db!=0);
  if( !db ) return;
  /* An index so dolt_hashof_index has a real name to resolve. */
  exec(db, "CREATE UNIQUE INDEX IF NOT EXISTS t_v ON t(v)");
  sqlite3_set_authorizer(db, xAuth, 0);
  for(i=0; i<sizeof(azReadSurface)/sizeof(azReadSurface[0]); i++){
    int rc = exec(db, azReadSurface[i]);
    snprintf(zName, sizeof(zName), "%s: %s", zLabel, azReadSurface[i]);
    check(zName, rc==SQLITE_OK);
  }
  sqlite3_set_authorizer(db, 0, 0);
  sqlite3_close(db);
  remove(zPath);
}

/* Writing a lazy system table materializes it; the user authorized the
** insert, not the CREATE TABLE it takes to store the row. */
static int authDenyCreateTable(
  void *pArg, int code,
  const char *z1, const char *z2, const char *z3, const char *z4
){
  (void)pArg; (void)z1; (void)z2; (void)z3; (void)z4;
  return code==SQLITE_CREATE_TABLE ? SQLITE_DENY : SQLITE_OK;
}

static void test_lazy_vtab_write_is_shielded(const char *zPath){
  sqlite3 *db = openSeeded(zPath);
  check("lazywrite: open", db!=0);
  if( !db ) return;
  sqlite3_set_authorizer(db, authDenyCreateTable, 0);
  check("lazywrite: dolt_docs insert",
        exec(db, "INSERT INTO dolt_docs(doc_name,doc_text) VALUES('a','b')")
          ==SQLITE_OK);
  check("lazywrite: dolt_ignore insert",
        exec(db, "INSERT INTO dolt_ignore(pattern,ignored) VALUES('x%',1)")
          ==SQLITE_OK);
  sqlite3_set_authorizer(db, 0, 0);
  sqlite3_close(db);
  remove(zPath);
}

/* The shield covers our internals, never the user's own statement. */
static void test_user_sql_is_still_authorized(const char *zPath){
  sqlite3 *db = openSeeded(zPath);
  check("usersql: open", db!=0);
  if( !db ) return;
  sqlite3_set_authorizer(db, authDenyReadOfT, 0);
  check("usersql: direct read refused",
        exec(db, "SELECT count(*) FROM t")!=SQLITE_OK);
  check("usersql: read through a view refused",
        exec(db, "SELECT count(*) FROM (SELECT v FROM t)")!=SQLITE_OK);
  sqlite3_set_authorizer(db, 0, 0);
  sqlite3_close(db);
  remove(zPath);
}

int main(void){
  char zPath[256];
  snprintf(zPath, sizeof(zPath), "/tmp/dolt_auth_internal_%d.db", (int)getpid());

  test_commands_run_under_a_sandbox(zPath);
  test_function_denial_still_applies(zPath);
  test_lazy_vtabs_ignore_placeholder_denial(zPath);
  test_deny_all_refuses_without_crashing(zPath);
  test_dolt_tests_sandbox_survives(zPath);
  runReadSurfaces(zPath, "denypragma", authDenyPragma);
  runReadSurfaces(zPath, "denymaster", authDenyMaster);
  test_lazy_vtab_write_is_shielded(zPath);
  test_user_sql_is_still_authorized(zPath);

  printf("authorizer_internal_sql_test: %d passed, %d failed\n", nPass, nFail);
  return nFail ? 1 : 0;
}
