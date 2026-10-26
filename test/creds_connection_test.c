#include "sqlite3.h"
#include "doltlite_creds.h"
#include "lib/test_tmpdir.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int failures;
static void check(const char *name, int ok){
  if( !ok ){ fprintf(stderr, "FAIL: %s\n", name); failures++; }
}
static int exec(sqlite3 *db, const char *sql){
  char *err = 0;
  int rc = sqlite3_exec(db, sql, 0, 0, &err);
  if( rc ) fprintf(stderr, "%s\n", err ? err : sqlite3_errstr(rc));
  sqlite3_free(err);
  return rc;
}
static void checkActive(sqlite3 *db, const char *kid){
  sqlite3_stmt *stmt = 0;
  int rc = sqlite3_prepare_v2(db,
      "SELECT kid, typeof(active) FROM dolt_creds_ls() WHERE active=true", -1,
      &stmt, 0);
  check("prepare active selection", rc==SQLITE_OK);
  if( rc==SQLITE_OK ){
    check("one active key", sqlite3_step(stmt)==SQLITE_ROW);
    check("selected key matches connection", sqlite3_column_text(stmt, 0)
        && strcmp((const char*)sqlite3_column_text(stmt, 0), kid)==0);
    check("active is a boolean integer", sqlite3_column_text(stmt, 1)
        && strcmp((const char*)sqlite3_column_text(stmt, 1), "integer")==0);
    check("only one active key", sqlite3_step(stmt)==SQLITE_DONE);
  }
  sqlite3_finalize(stmt);
}
int main(void){
  char dir[] = DOLTLITE_TEST_TMPDIR "/creds-connection-XXXXXX";
  DoltliteCreds *a = 0, *b = 0;
  sqlite3 *one = 0, *two = 0, *three = 0;
  char *ka = 0, *kb = 0, *sql = 0, *path;
  check("temporary directory", mkdtemp(dir)!=0);
  setenv("DOLTLITE_CREDS_DIR", dir, 1);
  unsetenv("DOLTLITE_CREDS_KID");
  check("generate first key", doltliteCredsGenerate(&a)==0);
  check("generate second key", doltliteCredsGenerate(&b)==0);
  if( !a || !b ) return 1;
  ka = doltliteCredsKid(a);
  kb = doltliteCredsKid(b);
  check("save first key", doltliteCredsSave(a, dir)==0);
  check("save second key", doltliteCredsSave(b, dir)==0);
  check("persist default", doltliteCredsConfigSet(dir, "user.creds", ka, 0)==0);
  check("open first connection", sqlite3_open(":memory:", &one)==SQLITE_OK);
  check("open second connection", sqlite3_open(":memory:", &two)==SQLITE_OK);
  check("open default connection", sqlite3_open(":memory:", &three)==SQLITE_OK);
  sql = sqlite3_mprintf("SELECT dolt_creds_use('%q','session')", kb);
  check("set first connection override", exec(one, sql)==SQLITE_OK);
  sqlite3_free(sql);
  checkActive(one, kb);
  checkActive(two, ka);
  checkActive(three, ka);
  sql = sqlite3_mprintf("SELECT dolt_creds_use('%q','session')", ka);
  check("set second connection override", exec(two, sql)==SQLITE_OK);
  sqlite3_free(sql);
  sql = sqlite3_mprintf("SELECT dolt_creds_use('%q')", kb);
  check("change persisted default", exec(three, sql)==SQLITE_OK);
  sqlite3_free(sql);
  checkActive(one, kb);
  checkActive(two, ka);
  checkActive(three, kb);
  sqlite3_close(one);
  sqlite3_close(two);
  sqlite3_close(three);
  doltliteCredsRemove(dir, ka);
  doltliteCredsRemove(dir, kb);
  path = sqlite3_mprintf("%s/config", dir); remove(path); sqlite3_free(path);
  path = sqlite3_mprintf("%s/config.lock", dir); remove(path); sqlite3_free(path);
  rmdir(dir);
  sqlite3_free(ka);
  sqlite3_free(kb);
  doltliteCredsFree(a);
  doltliteCredsFree(b);
  printf("creds_connection_test: %d failures\n", failures);
  return failures!=0;
}
