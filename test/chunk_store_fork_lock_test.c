#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "sqlite3.h"
#include "chunk_store.h"

#ifndef _WIN32
# include <unistd.h>
# include <sys/types.h>
# include <sys/wait.h>
#endif

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

static void rm_db(const char *zPath){
  char zBuf[512];
  remove(zPath);
  snprintf(zBuf, sizeof(zBuf), "%s-lock", zPath);
  remove(zBuf);
  snprintf(zBuf, sizeof(zBuf), "%s-wal", zPath);
  remove(zBuf);
}

static int exec_sql(sqlite3 *db, const char *zSql){
  char *zErr = 0;
  int rc = sqlite3_exec(db, zSql, 0, 0, &zErr);
  if( zErr ){
    fprintf(stderr, "exec error: %s\n", zErr);
    sqlite3_free(zErr);
  }
  return rc;
}

static int create_committed_db(const char *zPath){
  sqlite3 *db = 0;
  int rc;
  rc = sqlite3_open(zPath, &db);
  if( rc!=SQLITE_OK ) return rc;
  rc = exec_sql(db,
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'a');"
    "SELECT dolt_commit('-A','-m','init');"
  );
  sqlite3_close(db);
  return rc;
}

static void test_fork_child_does_not_keep_parent_lock(void){
#ifdef _WIN32
  check("fork_lock_test_skipped_on_windows", 1);
#else
  const char *zPath = "/tmp/test_chunk_store_fork_lock.db";
  sqlite3_vfs *pVfs;
  ChunkStore cs1, cs2;
  pid_t pid;
  int status = 0;
  int rc;

  printf("--- fork child does not keep released chunk-store lock ---\n");
  rm_db(zPath);
  rc = create_committed_db(zPath);
  check("fork_lock_setup_db", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) return;

  pVfs = sqlite3_vfs_find(0);
  check("fork_lock_has_vfs", pVfs!=0);
  if( !pVfs ) return;

  rc = chunkStoreOpen(&cs1, pVfs, zPath,
    SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB);
  check("fork_lock_open_cs1", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) return;

  rc = chunkStoreLockAndRefresh(&cs1);
  check("fork_lock_acquire_parent", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ){
    chunkStoreClose(&cs1);
    return;
  }

  pid = fork();
  check("fork_lock_fork_succeeded", pid>=0);
  if( pid==0 ){
    sleep(2);
    _exit(0);
  }
  if( pid<0 ){
    chunkStoreUnlock(&cs1);
    chunkStoreClose(&cs1);
    return;
  }

  chunkStoreUnlock(&cs1);
  chunkStoreClose(&cs1);

  rc = chunkStoreOpen(&cs2, pVfs, zPath,
    SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB);
  check("fork_lock_open_cs2", rc==SQLITE_OK);
  if( rc==SQLITE_OK ){
    rc = chunkStoreLockAndRefresh(&cs2);
    check("fork_lock_reacquire_while_child_alive", rc==SQLITE_OK);
    if( rc==SQLITE_OK ) chunkStoreUnlock(&cs2);
    chunkStoreClose(&cs2);
  }

  waitpid(pid, &status, 0);
  check("fork_lock_child_exit_ok", WIFEXITED(status) && WEXITSTATUS(status)==0);
  rm_db(zPath);
#endif
}

int main(void){
  test_fork_child_does_not_keep_parent_lock();

  printf("\n");
  printf("chunk_store_fork_lock_test: %d passed, %d failed\n", nPass, nFail);
  return nFail ? 1 : 0;
}
