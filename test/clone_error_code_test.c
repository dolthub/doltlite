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

/* dolt_clone must surface the underlying error code from doltliteClone rather
** than flattening every failure to SQLITE_ERROR, so an embedder can react to
** e.g. SQLITE_AUTH. A file:// remote under a nonexistent parent directory
** fails the open inside the clone with SQLITE_CANTOPEN. */
static void test_clone_error_code_propagates(void){
  sqlite3 *db = 0;
  char *err = 0;
  char target[256];
  char url[256];
  int rc;

  snprintf(target, sizeof(target), "/tmp/dolt_clone_ec_target_%d.db", (int)getpid());
  snprintf(url, sizeof(url),
           "file:///tmp/dolt_clone_ec_nodir_%d/remote.db", (int)getpid());
  remove(target);

  rc = sqlite3_open(target, &db);
  check("clone_ec: open target", rc==SQLITE_OK);

  {
    char sql[512];
    snprintf(sql, sizeof(sql), "SELECT dolt_clone('%s')", url);
    rc = sqlite3_exec(db, sql, 0, 0, &err);
  }
  check("clone_ec: clone fails", rc!=SQLITE_OK);
  check("clone_ec: code is CANTOPEN, not flattened to ERROR",
        sqlite3_extended_errcode(db)==SQLITE_CANTOPEN);

  sqlite3_free(err);
  sqlite3_close(db);
  remove(target);
}

int main(void){
  sqlite3_initialize();
  printf("dolt_clone error-code propagation test\n");
  printf("======================================\n\n");

  test_clone_error_code_propagates();

  printf("\n%d passed, %d failed\n", nPass, nFail);
  return nFail>0 ? 1 : 0;
}
