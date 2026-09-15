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

static void checkInt(const char *name, int got, int want){
  if( got==want ){
    nPass++;
  }else{
    nFail++;
    fprintf(stderr, "FAIL: %s: got %d, want %d\n", name, got, want);
  }
}

static int exec(sqlite3 *db, const char *zSql){
  char *zErr = 0;
  int rc = sqlite3_exec(db, zSql, 0, 0, &zErr);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "SQL error: %s [%s]\n", zErr ? zErr : "?", zSql);
    sqlite3_free(zErr);
  }
  return rc;
}

static int queryInt(sqlite3 *db, const char *zSql, int *pOut){
  sqlite3_stmt *p = 0;
  int rc = sqlite3_prepare_v2(db, zSql, -1, &p, 0);
  if( rc!=SQLITE_OK ) return rc;
  rc = sqlite3_step(p);
  if( rc==SQLITE_ROW ){
    *pOut = sqlite3_column_int(p, 0);
    sqlite3_finalize(p);
    return SQLITE_OK;
  }
  sqlite3_finalize(p);
  return rc==SQLITE_DONE ? SQLITE_ERROR : sqlite3_errcode(db);
}

static int explainHasOpcode(sqlite3 *db, const char *zSql, const char *zOp){
  sqlite3_stmt *p = 0;
  char *zExplain;
  int found = 0;
  zExplain = sqlite3_mprintf("EXPLAIN %s", zSql);
  if( sqlite3_prepare_v2(db, zExplain, -1, &p, 0)!=SQLITE_OK ){
    sqlite3_free(zExplain);
    return -1;
  }
  sqlite3_free(zExplain);
  while( sqlite3_step(p)==SQLITE_ROW ){
    const unsigned char *z = sqlite3_column_text(p, 1);
    if( z && strcmp((const char*)z, zOp)==0 ) found = 1;
  }
  sqlite3_finalize(p);
  return found;
}

static void countStep(sqlite3_context *ctx, int n, sqlite3_value **v){
  (void)ctx; (void)n; (void)v;
}

static void countFinal(sqlite3_context *ctx){
  sqlite3_result_int(ctx, 42);
}

static sqlite3 *openSeeded(const char *zPath){
  sqlite3 *db = 0;
  remove(zPath);
  if( sqlite3_open(zPath, &db)!=SQLITE_OK ) return 0;
  if( exec(db,
        "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);"
        "INSERT INTO t VALUES(1,1),(2,2),(3,3);")!=SQLITE_OK ){
    sqlite3_close(db);
    return 0;
  }
  return db;
}

static void test_builtin_shortcut(const char *zPath){
  sqlite3 *db = openSeeded(zPath);
  int n = 0;
  check("builtin: open", db!=0);
  if( !db ) return;

  check("builtin: plain", queryInt(db, "SELECT count(*) FROM t", &n)==SQLITE_OK);
  checkInt("builtin: plain value", n, 3);

  check("builtin: rowid BETWEEN",
        queryInt(db, "SELECT count(*) FROM t WHERE id BETWEEN 1 AND 3", &n)
          ==SQLITE_OK);
  checkInt("builtin: rowid BETWEEN value", n, 3);
  check("builtin: rowid BETWEEN uses CountRange",
        explainHasOpcode(db, "SELECT count(*) FROM t WHERE id BETWEEN 1 AND 3",
                         "CountRange")==1);

  check("builtin: create index", exec(db, "CREATE INDEX idx ON t(v)")==SQLITE_OK);
  check("builtin: index BETWEEN",
        queryInt(db, "SELECT count(*) FROM t WHERE v BETWEEN 1 AND 3", &n)
          ==SQLITE_OK);
  checkInt("builtin: index BETWEEN value", n, 3);
  check("builtin: index BETWEEN uses CountIndexRange",
        explainHasOpcode(db, "SELECT count(*) FROM t WHERE v BETWEEN 1 AND 3",
                         "CountIndexRange")==1);

  sqlite3_close(db);
  remove(zPath);
}

static void test_overridden_count(const char *zPath){
  sqlite3 *db = openSeeded(zPath);
  int n = 0;
  check("override: open", db!=0);
  if( !db ) return;

  check("override: register count(*)",
        sqlite3_create_function(db, "count", 0, SQLITE_UTF8, 0, 0,
                                countStep, countFinal)==SQLITE_OK);

  check("override: plain", queryInt(db, "SELECT count(*) FROM t", &n)==SQLITE_OK);
  checkInt("override: plain value", n, 42);

  check("override: rowid BETWEEN",
        queryInt(db, "SELECT count(*) FROM t WHERE id BETWEEN 1 AND 3", &n)
          ==SQLITE_OK);
  checkInt("override: rowid BETWEEN value", n, 42);
  check("override: rowid BETWEEN skips CountRange",
        explainHasOpcode(db, "SELECT count(*) FROM t WHERE id BETWEEN 1 AND 3",
                         "CountRange")==0);

  check("override: create index", exec(db, "CREATE INDEX idx ON t(v)")==SQLITE_OK);
  check("override: index BETWEEN",
        queryInt(db, "SELECT count(*) FROM t WHERE v BETWEEN 1 AND 3", &n)
          ==SQLITE_OK);
  checkInt("override: index BETWEEN value", n, 42);
  check("override: index BETWEEN skips CountIndexRange",
        explainHasOpcode(db, "SELECT count(*) FROM t WHERE v BETWEEN 1 AND 3",
                         "CountIndexRange")==0);

  sqlite3_close(db);
  remove(zPath);
}

static void test_overridden_count_one_arg(const char *zPath){
  sqlite3 *db = openSeeded(zPath);
  int n = 0;
  check("count1: open", db!=0);
  if( !db ) return;

  check("count1: register count(X)",
        sqlite3_create_function(db, "count", 1, SQLITE_UTF8, 0, 0,
                                countStep, countFinal)==SQLITE_OK);
  check("count1: create index", exec(db, "CREATE INDEX idx ON t(v)")==SQLITE_OK);

  check("count1: ordinary",
        queryInt(db, "SELECT count(v) FROM t", &n)==SQLITE_OK);
  checkInt("count1: ordinary value", n, 42);

  check("count1: index BETWEEN",
        queryInt(db, "SELECT count(v) FROM t WHERE v BETWEEN 1 AND 3", &n)
          ==SQLITE_OK);
  checkInt("count1: index BETWEEN value", n, 42);
  check("count1: index BETWEEN skips CountIndexRange",
        explainHasOpcode(db,
                          "SELECT count(v) FROM t WHERE v BETWEEN 1 AND 3",
                          "CountIndexRange")==0);

  sqlite3_close(db);
  remove(zPath);
}

int main(void){
  char zPath[256];
  snprintf(zPath, sizeof(zPath), "/tmp/count_range_override_%d.db", (int)getpid());
  test_builtin_shortcut(zPath);
  test_overridden_count(zPath);
  test_overridden_count_one_arg(zPath);
  printf("count_range_override_test: %d passed, %d failed\n", nPass, nFail);
  return nFail ? 1 : 0;
}
