#include <stdio.h>
#include <stdlib.h>
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

static void checkRc(const char *name, int got, int want){
  if( got==want ){
    nPass++;
  }else{
    nFail++;
    fprintf(stderr, "FAIL: %s: got rc=%d, wanted rc=%d\n", name, got, want);
  }
}

static int exec(sqlite3 *db, const char *zSql){
  return sqlite3_exec(db, zSql, 0, 0, 0);
}

static const char *seedSql =
  "CREATE TABLE t(k INTEGER PRIMARY KEY, v TEXT);"
  "INSERT INTO t VALUES(1,'a');"
  "SELECT dolt_commit('-Am','base');"
  "SELECT dolt_branch('feature');"
  "SELECT dolt_tag('v1','HEAD');";

/* A refused write reports SQLITE_READONLY whether it is plain DML or a
** version control command, so an embedder can branch on the code. */
static void test_readonly_code(const char *zPath){
  sqlite3 *db = 0;
  remove(zPath);
  check("ro: open", sqlite3_open(zPath, &db)==SQLITE_OK);
  if( !db ) return;
  check("ro: seed", exec(db, seedSql)==SQLITE_OK);
  check("ro: query_only", exec(db, "PRAGMA query_only=1")==SQLITE_OK);

  checkRc("ro: INSERT", exec(db, "INSERT INTO t VALUES(2,'b')"), SQLITE_READONLY);
  checkRc("ro: dolt_add", exec(db, "SELECT dolt_add('-A')"), SQLITE_READONLY);
  checkRc("ro: dolt_branch", exec(db, "SELECT dolt_branch('b1')"), SQLITE_READONLY);
  checkRc("ro: dolt_tag", exec(db, "SELECT dolt_tag('t1')"), SQLITE_READONLY);
  checkRc("ro: dolt_checkout -b",
          exec(db, "SELECT dolt_checkout('-b','b2')"), SQLITE_READONLY);
  sqlite3_close(db);

  check("ro: reopen readonly",
        sqlite3_open_v2(zPath, &db, SQLITE_OPEN_READONLY, 0)==SQLITE_OK);
  if( !db ) return;
  checkRc("ro_open: INSERT",
          exec(db, "INSERT INTO t VALUES(2,'b')"), SQLITE_READONLY);
  checkRc("ro_open: dolt_branch",
          exec(db, "SELECT dolt_branch('b1')"), SQLITE_READONLY);
  checkRc("ro_open: dolt_tag",
          exec(db, "SELECT dolt_tag('t1')"), SQLITE_READONLY);
  sqlite3_close(db);
  remove(zPath);
}

/* Lock contention reports SQLITE_BUSY with a message about the lock, so a
** retry loop keyed on the code catches every command. */
static void test_busy_code(const char *zBase, const char *zWork){
  static const char *azBusy[] = {
    "SELECT dolt_add('t')",
    "SELECT dolt_merge('feature')",
    "SELECT dolt_checkout('feature')",
    "SELECT dolt_checkout('-b','nb')",
    "SELECT dolt_branch('nb2')",
    "SELECT dolt_branch('-d','feature')",
    "SELECT dolt_branch('-m','feature','f2')",
    "SELECT dolt_tag('v2','HEAD')",
    "SELECT dolt_tag('-d','v1')",
    "SELECT dolt_reset('--hard')",
    "SELECT dolt_clean()",
    0
  };
  sqlite3 *db = 0;
  int i;

  remove(zBase);
  check("busy: open base", sqlite3_open(zBase, &db)==SQLITE_OK);
  if( !db ) return;
  check("busy: seed", exec(db, seedSql)==SQLITE_OK);
  sqlite3_close(db);

  for(i=0; azBusy[i]; i++){
    sqlite3 *dbPeer = 0;
    char *zErr = 0;
    char zCmd[1024];
    int rc;

    snprintf(zCmd, sizeof(zCmd), "cp -f '%s' '%s'", zBase, zWork);
    if( system(zCmd)!=0 ){
      check("busy: copy base", 0);
      return;
    }
    if( sqlite3_open(zWork, &dbPeer)!=SQLITE_OK ) return;
    if( exec(dbPeer, "BEGIN IMMEDIATE; INSERT INTO t VALUES(50,'peer')")
          !=SQLITE_OK ){
      check("busy: peer takes the write lock", 0);
      sqlite3_close(dbPeer);
      return;
    }
    if( sqlite3_open(zWork, &db)!=SQLITE_OK ) return;
    sqlite3_busy_timeout(db, 20);

    rc = sqlite3_exec(db, azBusy[i], 0, 0, &zErr);
    checkRc(azBusy[i], rc, SQLITE_BUSY);
    /* A code with no message left on the connection reads "not an error",
    ** which tells an operator nothing about a lock. */
    check("busy: message is not bogus",
          zErr!=0 && strcmp(zErr, "not an error")!=0);
    sqlite3_free(zErr);
    sqlite3_close(db);
    sqlite3_close(dbPeer);
  }
  remove(zBase);
  remove(zWork);
}

/* SQLITE_NOTFOUND is an internal sentinel these surfaces pass among
** themselves. A caller must never receive it. */
static void test_no_internal_code_escapes(const char *zPath){
  static const char *azMissing[] = {
    "SELECT dolt_branch('-d','nosuch')",
    "SELECT dolt_branch('-m','nosuch','x')",
    "SELECT dolt_branch('-c','nosuch','x')",
    "SELECT dolt_checkout('nosuch')",
    "SELECT dolt_tag('-d','nosuch')",
    "SELECT dolt_merge('nosuch')",
    "SELECT dolt_reset('--hard','nosuch')",
    "SELECT dolt_remote('remove','nosuch')",
    "SELECT dolt_fetch('nosuch')",
    "SELECT dolt_add('nosuchtable')",
    "SELECT dolt_branch('feature')",
    "SELECT dolt_tag('v1','HEAD')",
    0
  };
  sqlite3 *db = 0;
  int i;

  remove(zPath);
  check("missing: open", sqlite3_open(zPath, &db)==SQLITE_OK);
  if( !db ) return;
  check("missing: seed", exec(db, seedSql)==SQLITE_OK);

  for(i=0; azMissing[i]; i++){
    char *zErr = 0;
    int rc = sqlite3_exec(db, azMissing[i], 0, 0, &zErr);
    checkRc(azMissing[i], rc, SQLITE_ERROR);
    check("missing: says why", zErr!=0 && zErr[0]!=0);
    sqlite3_free(zErr);
  }
  sqlite3_close(db);
  remove(zPath);
}

int main(void){
  char zPath[256];
  char zBase[256];
  char zWork[256];
  int pid = (int)getpid();

  snprintf(zPath, sizeof(zPath), "/tmp/dolt_rc_%d.db", pid);
  snprintf(zBase, sizeof(zBase), "/tmp/dolt_rc_base_%d.db", pid);
  snprintf(zWork, sizeof(zWork), "/tmp/dolt_rc_work_%d.db", pid);

  test_readonly_code(zPath);
  test_busy_code(zBase, zWork);
  test_no_internal_code_escapes(zPath);

  printf("vc_result_code_test: %d passed, %d failed\n", nPass, nFail);
  return nFail ? 1 : 0;
}
