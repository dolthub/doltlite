#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "sqlite3.h"
#include "chunk_store_int.h"
#include "doltlite_internal.h"
#include "lib/test_tmpdir.h"

#ifndef _WIN32
# include <unistd.h>
# include <sys/types.h>
# include <sys/wait.h>
# include <fcntl.h>
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
  const char *zPath = DOLTLITE_TEST_TMPDIR "/test_chunk_store_fork_lock.db";
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

static void test_graph_lock_promotion_has_one_winner(void){
#ifdef _WIN32
  check("graph_lock_promotion_skipped_on_windows", 1);
#else
  char zPath[] = DOLTLITE_TEST_TMPDIR "/test_chunk_store_lock_promotion\0";
  sqlite3_vfs *pVfs = sqlite3_vfs_find(0);
  sqlite3_file *pFile1 = 0;
  sqlite3_file *pFile2 = 0;
  int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
            | SQLITE_OPEN_MAIN_DB;
  int rc1;
  int rc2;

  printf("--- graph lock promotion has one winner ---\n");
  check("graph_lock_promotion_has_vfs", pVfs!=0);
  if( !pVfs ) return;
  sqlite3OsDelete(pVfs, zPath, 0);

  rc1 = sqlite3OsOpenMalloc(pVfs, zPath, &pFile1, flags, 0);
  rc2 = sqlite3OsOpenMalloc(pVfs, zPath, &pFile2, flags, 0);
  check("graph_lock_promotion_open_first", rc1==SQLITE_OK);
  check("graph_lock_promotion_open_second", rc2==SQLITE_OK);
  if( rc1!=SQLITE_OK || rc2!=SQLITE_OK ) goto done;

  rc1 = sqlite3OsLock(pFile1, SQLITE_LOCK_SHARED);
  rc2 = sqlite3OsLock(pFile2, SQLITE_LOCK_SHARED);
  check("graph_lock_promotion_share_first", rc1==SQLITE_OK);
  check("graph_lock_promotion_share_second", rc2==SQLITE_OK);
  if( rc1!=SQLITE_OK || rc2!=SQLITE_OK ) goto done;

  rc1 = csFileLockPromote(pFile1);
  rc2 = csFileLockPromote(pFile2);
  check("graph_lock_promotion_single_winner",
        (rc1==SQLITE_OK && rc2==SQLITE_BUSY)
     || (rc1==SQLITE_BUSY && rc2==SQLITE_OK));

done:
  if( pFile1 ){
    sqlite3OsUnlock(pFile1, SQLITE_LOCK_NONE);
    sqlite3OsCloseFree(pFile1);
  }
  if( pFile2 ){
    sqlite3OsUnlock(pFile2, SQLITE_LOCK_NONE);
    sqlite3OsCloseFree(pFile2);
  }
  sqlite3OsDelete(pVfs, zPath, 0);
#endif
}


#ifndef _WIN32
static void check_process_busy(const char *zPath, const char *zName){
  int status = 0;
  pid_t pid = fork();
  if( pid==0 ){
    sqlite3 *child = 0;
    int ok = sqlite3_open(zPath, &child)==SQLITE_OK;
    if( ok ){
      int rc = sqlite3_exec(child, "INSERT INTO t VALUES(99,'child')",0,0,0);
      ok = rc==SQLITE_BUSY;
      rc = sqlite3_exec(child, "SELECT dolt_branch('child')",0,0,0);
      ok = ok && rc==SQLITE_BUSY;
    }
    sqlite3_close(child);
    _exit(ok ? 0 : 1);
  }
  if( pid>0 ) waitpid(pid, &status, 0);
  check(zName, pid>0 && WIFEXITED(status) && WEXITSTATUS(status)==0);
}
#endif

static void test_unlinked_graph_lock(int cached){
#ifdef _WIN32
  (void)cached;
#else
  const char *zPath = DOLTLITE_TEST_TMPDIR "/test_unlinked_graph_lock.db";
  const char *zLock = DOLTLITE_TEST_TMPDIR "/.test_unlinked_graph_lock.db-lock";
  sqlite3 *db = 0, *peer = 0, *reader = 0;
  sqlite3_stmt *stmt = 0;
  int rc;

  rm_db(zPath);
  check("unlink_setup", create_committed_db(zPath)==SQLITE_OK);
  check("unlink_open", sqlite3_open(zPath, &db)==SQLITE_OK);
  if( cached ){
    check("unlink_cached_open", sqlite3_open(zPath, &peer)==SQLITE_OK);
    check("unlink_cached_lock", exec_sql(peer, "BEGIN IMMEDIATE; ROLLBACK")==SQLITE_OK);
  }
  check("unlink_writer_begin", exec_sql(db,
    "BEGIN IMMEDIATE; INSERT INTO t VALUES(2,'owner')")==SQLITE_OK);
  check("unlink_sidecar_removed", remove(zLock)==0);

  check_process_busy(zPath, "unlink_process_sql_and_vc_busy");
  if( !peer ) check("unlink_peer_open", sqlite3_open(zPath, &peer)==SQLITE_OK);
  check("unlink_peer_sql_busy", sqlite3_exec(peer,
    "INSERT INTO t VALUES(3,'peer')", 0, 0, 0)==SQLITE_BUSY);
  check("unlink_peer_vc_busy", sqlite3_exec(peer,
    "SELECT dolt_branch('peer')", 0, 0, 0)==SQLITE_BUSY);
  rc = sqlite3_prepare_v2(peer, "SELECT count(*) FROM t", -1, &stmt, 0);
  check("unlink_reader", rc==SQLITE_OK && sqlite3_step(stmt)==SQLITE_ROW
                          && sqlite3_column_int(stmt, 0)==1);
  sqlite3_finalize(stmt);
  check("unlink_reader_open", sqlite3_open(zPath, &reader)==SQLITE_OK);
  check("unlink_reader_close", sqlite3_close(reader)==SQLITE_OK);
  check_process_busy(zPath, "unlink_process_busy_after_reader_close");
  check("unlink_owner_commit", exec_sql(db, "COMMIT")==SQLITE_OK);
  check("unlink_peer_retry", exec_sql(peer,
    "INSERT INTO t VALUES(3,'peer'); SELECT dolt_commit('-Am','both')")==SQLITE_OK);
  check("unlink_old_handle_retry", exec_sql(db,
    "INSERT INTO t VALUES(4,'owner-again')")==SQLITE_OK);
  sqlite3_close(peer);
  sqlite3_close(db);
  check("unlink_reopen", sqlite3_open(zPath, &db)==SQLITE_OK);
  stmt = 0;
  rc = sqlite3_prepare_v2(db, "SELECT count(*) FROM t", -1, &stmt, 0);
  check("unlink_rows_durable", rc==SQLITE_OK && sqlite3_step(stmt)==SQLITE_ROW
                                && sqlite3_column_int(stmt, 0)==4);
  sqlite3_finalize(stmt);
  stmt = 0;
  rc = sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &stmt, 0);
  check("unlink_integrity", rc==SQLITE_OK && sqlite3_step(stmt)==SQLITE_ROW
                           && strcmp((const char*)sqlite3_column_text(stmt,0),"ok")==0);
  sqlite3_finalize(stmt);
  sqlite3_close(db);
  rm_db(zPath);
  remove(zLock);
#endif
}

static void test_unlinked_lock_replacement(int backup){
#ifndef _WIN32
  const char *zPath = DOLTLITE_TEST_TMPDIR "/test_unlinked_replacement.db";
  const char *zLock = DOLTLITE_TEST_TMPDIR "/.test_unlinked_replacement.db-lock";
  sqlite3 *db = 0, *src = 0, *peer = 0;
  ChunkStore *cs;
  int rc;

  rm_db(zPath);
  check("unlink_replace_setup", create_committed_db(zPath)==SQLITE_OK);
  check("unlink_replace_open", sqlite3_open(zPath, &db)==SQLITE_OK);
  cs = doltliteGetChunkStore(db);
  if( backup ){
    check("unlink_replace_source", sqlite3_open(":memory:", &src)==SQLITE_OK);
    check("unlink_replace_source_rows", exec_sql(src,
      "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'replacement')")==SQLITE_OK);
  }
  sqlite3_mutex_enter(sqlite3_db_mutex(db));
  rc = chunkStoreLockAndRefresh(cs);
  check("unlink_replace_lock", rc==SQLITE_OK);
  check("unlink_replace_remove", remove(zLock)==0);
  if( rc==SQLITE_OK ){
    if( backup ){
      sqlite3_backup *pBackup = sqlite3_backup_init(db, "main", src, "main");
      check("unlink_replace_backup_init", pBackup!=0);
      if( pBackup ){
        check("unlink_replace_backup_step", sqlite3_backup_step(pBackup,-1)==SQLITE_DONE);
        check("unlink_replace_backup_finish", sqlite3_backup_finish(pBackup)==SQLITE_OK);
      }
    }else{
      check("unlink_replace_gc", doltliteGcCompactStore(db, cs)==SQLITE_OK);
    }
    check("unlink_replace_peer_open", sqlite3_open(zPath, &peer)==SQLITE_OK);
    check("unlink_replace_sql_busy", sqlite3_exec(peer,
      "INSERT INTO t VALUES(2,'peer')",0,0,0)==SQLITE_BUSY);
    check("unlink_replace_vc_busy", sqlite3_exec(peer,
      "SELECT dolt_branch('peer')",0,0,0)==SQLITE_BUSY);
    check_process_busy(zPath, "unlink_replace_process_busy");
    chunkStoreUnlock(cs);
    check("unlink_replace_retry", exec_sql(peer,
      "INSERT INTO t VALUES(2,'peer'); SELECT dolt_commit('-Am','peer')")==SQLITE_OK);
    check("unlink_replace_owner_retry", exec_sql(db,
      "INSERT INTO t VALUES(3,'owner')")==SQLITE_OK);
  }
  sqlite3_mutex_leave(sqlite3_db_mutex(db));
  sqlite3_close(peer);
  sqlite3_close(src);
  sqlite3_close(db);
  rm_db(zPath);
  remove(zLock);
#else
  (void)backup;
#endif
}

static void test_zero_byte_open_keeps_peer_lock(int readOnlyFirst){
#ifndef _WIN32
  const char *zPath = DOLTLITE_TEST_TMPDIR "/test_zero_byte_peer_lock.db";
  const char *zUri =
    "file:" DOLTLITE_TEST_TMPDIR "/test_zero_byte_peer_lock.db?doltlite_engine=sqlite";
  sqlite3 *owner = 0, *peer = 0;
  sqlite3_stmt *stmt = 0;
  FILE *f;
  pid_t pid;
  int status = 0;
  int rc;

  printf("--- opening a 0-byte file keeps a peer connection's locks ---\n");
  rm_db(zPath);
  f = fopen(zPath, "wb");
  check("zero_lock_create_empty", f!=0);
  if( !f ) return;
  fclose(f);

  if( readOnlyFirst ){
    check("zero_lock_readonly_peer_open", sqlite3_open_v2(zPath, &peer,
        SQLITE_OPEN_READONLY, 0)==SQLITE_OK);
    check("zero_lock_readonly_peer_empty", exec_sql(peer,
        "SELECT count(*) FROM sqlite_master")==SQLITE_OK);
  }
  rc = sqlite3_open_v2(zUri, &owner,
    SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, 0);
  check("zero_lock_owner_open", rc==SQLITE_OK);
  check("zero_lock_owner_begin", exec_sql(owner,
    "BEGIN IMMEDIATE; CREATE TABLE s(x)")==SQLITE_OK);
  if( !readOnlyFirst ){
    check("zero_lock_peer_open", sqlite3_open(zPath, &peer)==SQLITE_OK);
  }
  sqlite3_exec(peer, "SELECT count(*) FROM sqlite_master", 0, 0, 0);

  pid = fork();
  if( pid==0 ){
    /* A forked child inherits the parent's in-memory lock state, so ask
    ** the kernel whether the parent still holds SQLite's RESERVED byte. */
    struct flock lk;
    int fd = open(zPath, O_RDWR);
    int held = 0;
    if( fd>=0 ){
      memset(&lk, 0, sizeof(lk));
      lk.l_type = F_WRLCK;
      lk.l_whence = SEEK_SET;
      lk.l_start = 0x40000001;
      lk.l_len = 1;
      held = fcntl(fd, F_GETLK, &lk)==0 && lk.l_type!=F_UNLCK;
      close(fd);
    }
    _exit(held ? 0 : 1);
  }
  if( pid>0 ) waitpid(pid, &status, 0);
  check("zero_lock_owner_keeps_reserved_lock",
        pid>0 && WIFEXITED(status) && WEXITSTATUS(status)==0);

  check("zero_lock_owner_commit", exec_sql(owner, "COMMIT")==SQLITE_OK);
  sqlite3_close(peer);
  sqlite3_close(owner);

  owner = 0;
  rc = sqlite3_open(zPath, &owner);
  if( rc==SQLITE_OK ){
    rc = sqlite3_prepare_v2(owner,
      "SELECT group_concat(name) FROM sqlite_master", -1, &stmt, 0);
  }
  check("zero_lock_owner_table_durable", rc==SQLITE_OK
        && sqlite3_step(stmt)==SQLITE_ROW
        && sqlite3_column_text(stmt, 0)!=0
        && strcmp((const char*)sqlite3_column_text(stmt, 0), "s")==0);
  sqlite3_finalize(stmt);
  sqlite3_close(owner);
  rm_db(zPath);
#else
  (void)readOnlyFirst;
#endif
}

static int count_rows(sqlite3 *db, const char *zSql){
  sqlite3_stmt *stmt = 0;
  int n = -1;
  if( sqlite3_prepare_v2(db, zSql, -1, &stmt, 0)==SQLITE_OK
   && sqlite3_step(stmt)==SQLITE_ROW ){
    n = sqlite3_column_int(stmt, 0);
  }
  sqlite3_finalize(stmt);
  return n;
}

static void test_zero_byte_peer_follows_sqlite_creator(void){
  const char *zPath = DOLTLITE_TEST_TMPDIR "/test_zero_byte_creator.db";
  const char *zUri =
    "file:" DOLTLITE_TEST_TMPDIR "/test_zero_byte_creator.db?doltlite_engine=sqlite";
  sqlite3 *creator = 0, *peer = 0;
  sqlite3_stmt *stmt = 0;
  FILE *f;
  int rc;

  printf("--- a peer of a SQLite creator on a 0-byte file sees its tables ---\n");
  rm_db(zPath);
  remove(DOLTLITE_TEST_TMPDIR "/.test_zero_byte_creator.db-lock");
  f = fopen(zPath, "wb");
  check("creator_create_empty", f!=0);
  if( !f ) return;
  fclose(f);

  rc = sqlite3_open_v2(zUri, &creator,
    SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, 0);
  check("creator_open", rc==SQLITE_OK);
  check("creator_begin", exec_sql(creator,
    "BEGIN IMMEDIATE; CREATE TABLE s(a); INSERT INTO s VALUES('stock1')")==SQLITE_OK);
  check("creator_peer_open", sqlite3_open(zPath, &peer)==SQLITE_OK);
  check("creator_peer_sees_empty",
        count_rows(peer, "SELECT count(*) FROM sqlite_master")==0);
  check("creator_commit", exec_sql(creator, "COMMIT")==SQLITE_OK);
  check("creator_peer_sees_table",
        count_rows(peer, "SELECT count(*) FROM s")==1);
  rc = sqlite3_prepare_v2(peer, "SELECT doltlite_engine()", -1, &stmt, 0);
  check("creator_peer_on_sqlite_engine", rc==SQLITE_OK
        && sqlite3_step(stmt)==SQLITE_ROW
        && strcmp((const char*)sqlite3_column_text(stmt, 0), "orig")==0);
  sqlite3_finalize(stmt);
  sqlite3_close(peer);
  sqlite3_close(creator);
  {
    FILE *pLock = fopen(DOLTLITE_TEST_TMPDIR "/.test_zero_byte_creator.db-lock", "rb");
    check("creator_peer_leaves_no_lock_sidecar", pLock==0);
    if( pLock ) fclose(pLock);
  }
  rm_db(zPath);
}

int main(void){
  test_fork_child_does_not_keep_parent_lock();
  test_zero_byte_open_keeps_peer_lock(0);
  test_zero_byte_open_keeps_peer_lock(1);
  test_zero_byte_peer_follows_sqlite_creator();
  test_graph_lock_promotion_has_one_winner();
  test_unlinked_graph_lock(0);
  test_unlinked_graph_lock(1);
  test_unlinked_lock_replacement(0);
  test_unlinked_lock_replacement(1);

  printf("\n");
  printf("chunk_store_fork_lock_test: %d passed, %d failed\n", nPass, nFail);
  return nFail ? 1 : 0;
}
