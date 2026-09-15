#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include "sqlite3.h"
#include "doltlite_internal.h"
#include "chunk_store.h"

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

static int exec(sqlite3 *db, const char *zSql){
  char *zErr = 0;
  int rc = sqlite3_exec(db, zSql, 0, 0, &zErr);
  sqlite3_free(zErr);
  return rc;
}

static int execErr(sqlite3 *db, const char *zSql, char **pzErr){
  return sqlite3_exec(db, zSql, 0, 0, pzErr);
}

static void rm(const char *zPath){
  char zBuf[512];
  const char *zSlash = strrchr(zPath, '/');
  remove(zPath);
  sqlite3_snprintf(sizeof(zBuf), zBuf, "%s-lock", zPath);
  remove(zBuf);
  sqlite3_snprintf(sizeof(zBuf), zBuf, "%s-wal", zPath);
  remove(zBuf);
  if( zSlash ){
    sqlite3_snprintf(sizeof(zBuf), zBuf, "%.*s.%s-lock",
                     (int)(zSlash - zPath + 1), zPath, zSlash+1);
  }else{
    sqlite3_snprintf(sizeof(zBuf), zBuf, ".%s-lock", zPath);
  }
  remove(zBuf);
}

/* A peer holding the remote graph lock must not be reported as a ref race. */
static void test_lock_busy_not_refs_changed(void){
  char zRemote[256], zSrc[256], zUrl[512];
  sqlite3 *dbRemote = 0, *dbSrc = 0;
  ChunkStore *cs = 0;
  char *zErr = 0;
  int rc;

  snprintf(zRemote, sizeof(zRemote), "/tmp/push_lock_remote_%d.db", (int)getpid());
  snprintf(zSrc, sizeof(zSrc), "/tmp/push_lock_src_%d.db", (int)getpid());
  rm(zRemote);
  rm(zSrc);

  check("lock: open remote", sqlite3_open(zRemote, &dbRemote)==SQLITE_OK);
  check("lock: seed remote",
        exec(dbRemote,
          "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
          "INSERT INTO t VALUES(1,'base');"
          "SELECT dolt_commit('-Am','base');")==SQLITE_OK);

  check("lock: open src", sqlite3_open(zSrc, &dbSrc)==SQLITE_OK);
  sqlite3_snprintf(sizeof(zUrl), zUrl,
    "SELECT dolt_clone('file://%s');", zRemote);
  check("lock: clone", exec(dbSrc, zUrl)==SQLITE_OK);
  check("lock: local commit",
        exec(dbSrc,
          "INSERT INTO t VALUES(2,'src');"
          "SELECT dolt_commit('-Am','src');")==SQLITE_OK);

  cs = doltliteGetChunkStore(dbRemote);
  check("lock: chunk store", cs!=0);
  if( cs ){
    check("lock: acquire graph lock",
          chunkStoreLockAndRefresh(cs)==SQLITE_OK);
  }

  sqlite3_snprintf(sizeof(zUrl), zUrl,
    "SELECT dolt_push('origin','main');");
  rc = execErr(dbSrc, zUrl, &zErr);
  check("lock_busy_not_refs_changed", rc==SQLITE_BUSY);
  check("lock: message names the lock",
        zErr && strstr(zErr, "locked")!=0);
  check("lock: message is not a ref-race",
        zErr && strstr(zErr, "refs changed")==0);
  sqlite3_free(zErr);

  if( cs ) chunkStoreUnlock(cs);
  zErr = 0;
  rc = execErr(dbSrc, "SELECT dolt_push('origin','main');", &zErr);
  check("lock: push succeeds after unlock", rc==SQLITE_OK);
  sqlite3_free(zErr);

  sqlite3_close(dbSrc);
  sqlite3_close(dbRemote);
  rm(zRemote);
  rm(zSrc);
}

/* Diverged tips are a fast-forward refusal, not lock contention. */
static void test_diverged_is_not_lock(void){
  char zRemote[256], zSrc[256], zUrl[512];
  sqlite3 *dbRemote = 0, *dbSrc = 0;
  char *zErr = 0;
  int rc;

  snprintf(zRemote, sizeof(zRemote), "/tmp/push_nff_remote_%d.db", (int)getpid());
  snprintf(zSrc, sizeof(zSrc), "/tmp/push_nff_src_%d.db", (int)getpid());
  rm(zRemote);
  rm(zSrc);

  check("nff: open remote", sqlite3_open(zRemote, &dbRemote)==SQLITE_OK);
  check("nff: seed remote",
        exec(dbRemote,
          "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
          "INSERT INTO t VALUES(1,'base');"
          "SELECT dolt_commit('-Am','base');")==SQLITE_OK);
  check("nff: open src", sqlite3_open(zSrc, &dbSrc)==SQLITE_OK);
  sqlite3_snprintf(sizeof(zUrl), zUrl,
    "SELECT dolt_clone('file://%s');", zRemote);
  check("nff: clone", exec(dbSrc, zUrl)==SQLITE_OK);
  check("nff: remote moves",
        exec(dbRemote,
          "INSERT INTO t VALUES(3,'remote');"
          "SELECT dolt_commit('-Am','remote');")==SQLITE_OK);
  check("nff: src moves",
        exec(dbSrc,
          "INSERT INTO t VALUES(2,'src');"
          "SELECT dolt_commit('-Am','src');")==SQLITE_OK);
  rc = execErr(dbSrc, "SELECT dolt_push('origin','main');", &zErr);
  check("nff: push refused", rc!=SQLITE_OK);
  check("nff: not a lock message",
        !zErr || strstr(zErr, "locked")==0);
  check("nff: fast-forward or refs diagnosis",
        zErr && (strstr(zErr, "fast-forward")!=0
              || strstr(zErr, "refs changed")!=0));
  sqlite3_free(zErr);

  sqlite3_close(dbSrc);
  sqlite3_close(dbRemote);
  rm(zRemote);
  rm(zSrc);
}

int main(void){
  sqlite3_initialize();
  test_lock_busy_not_refs_changed();
  test_diverged_is_not_lock();
  printf("remote_push_lock_busy_test: %d passed, %d failed\n", nPass, nFail);
  return nFail ? 1 : 0;
}
