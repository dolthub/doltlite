#include "sqlite3.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static int fail(const char *zMsg){
  fprintf(stderr, "FAIL: %s\n", zMsg);
  return 1;
}

static int probeExec(sqlite3 *db, const char *zSql){
  char *zErr = 0;
  int rc = sqlite3_exec(db, zSql, 0, 0, &zErr);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "SQL error (%d) on [%s]: %s\n", rc, zSql, zErr ? zErr : "");
    sqlite3_free(zErr);
  }
  return rc;
}

static int queryText(sqlite3 *db, const char *zSql, char *zBuf, int nBuf){
  sqlite3_stmt *pStmt = 0;
  int rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  if( rc!=SQLITE_OK ) return rc;
  rc = sqlite3_step(pStmt);
  if( rc!=SQLITE_ROW ){
    sqlite3_finalize(pStmt);
    return rc==SQLITE_DONE ? SQLITE_NOTFOUND : rc;
  }
  snprintf(zBuf, (size_t)nBuf, "%s",
           sqlite3_column_text(pStmt, 0)
             ? (const char*)sqlite3_column_text(pStmt, 0) : "");
  sqlite3_finalize(pStmt);
  return SQLITE_OK;
}

static int queryInt(sqlite3 *db, const char *zSql, int *pOut){
  sqlite3_stmt *pStmt = 0;
  int rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  if( rc!=SQLITE_OK ) return rc;
  rc = sqlite3_step(pStmt);
  if( rc!=SQLITE_ROW ){
    sqlite3_finalize(pStmt);
    return rc==SQLITE_DONE ? SQLITE_NOTFOUND : rc;
  }
  *pOut = sqlite3_column_int(pStmt, 0);
  sqlite3_finalize(pStmt);
  return SQLITE_OK;
}

static sqlite3_int64 fileSizeOf(const char *zPath){
  struct stat st;
  if( stat(zPath, &st)!=0 ) return -1;
  return (sqlite3_int64)st.st_size;
}

static int backupInto(sqlite3 *pDest, const char *zSrc){
  sqlite3 *pSrc = 0;
  sqlite3_backup *pBak = 0;
  int rc = sqlite3_open_v2(zSrc, &pSrc, SQLITE_OPEN_READONLY, 0);
  if( rc!=SQLITE_OK ) return rc;
  pBak = sqlite3_backup_init(pDest, "main", pSrc, "main");
  if( !pBak ){
    rc = sqlite3_errcode(pDest);
    sqlite3_close(pSrc);
    return rc ? rc : SQLITE_ERROR;
  }
  rc = sqlite3_backup_step(pBak, -1);
  if( rc==SQLITE_DONE ) rc = SQLITE_OK;
  (void)sqlite3_backup_finish(pBak);
  sqlite3_close(pSrc);
  return rc;
}

/* Scenario 1: GC shrinks the file in place; an already-open peer must detect
** the shrink, reload cleanly, read the new state, and commit without appending
** past EOF. */
static int testGcShrinkPeer(const char *zDir){
  char zDb[512];
  char zVal[128];
  sqlite3 *db1 = 0, *db2 = 0, *db3 = 0;
  sqlite3_int64 szBefore, szAfter;
  int i, cnt = 0;

  snprintf(zDb, sizeof(zDb), "%s/gc_shrink.db", zDir);
  if( sqlite3_open(zDb, &db1)!=SQLITE_OK ) return fail("gc: open db1");
  if( probeExec(db1, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);")!=SQLITE_OK ){
    return fail("gc: create table");
  }
  if( probeExec(db1, "BEGIN;")!=SQLITE_OK ) return fail("gc: begin");
  for(i=1; i<=120; i++){
    char zSql[512];
    snprintf(zSql, sizeof(zSql),
      "INSERT INTO t VALUES(%d, 'row_%03d_%03d_%03d_%03d_%03d_%03d_%03d_%03d');",
      i, i, i*3, i*7, i*11, i*13, i*17, i*19, i*23);
    if( probeExec(db1, zSql)!=SQLITE_OK ) return fail("gc: insert");
  }
  if( probeExec(db1, "COMMIT;")!=SQLITE_OK ) return fail("gc: commit");
  if( probeExec(db1,
      "SELECT dolt_add('-A');"
      "SELECT dolt_commit('-m', 'bulk');"
      "DELETE FROM t WHERE id > 1;"
      "UPDATE t SET v = 'after_gc' WHERE id = 1;"
      "SELECT dolt_add('-A');"
      "SELECT dolt_commit('--amend', '-m', 'trimmed');")!=SQLITE_OK ){
    return fail("gc: amend");
  }

  /* Open peer before GC so it holds the pre-GC file size and WAL offsets. */
  if( sqlite3_open(zDb, &db2)!=SQLITE_OK ) return fail("gc: open db2");
  if( queryText(db2, "SELECT v FROM t WHERE id=1;", zVal, sizeof(zVal))!=SQLITE_OK
   || strcmp(zVal, "after_gc")!=0 ){
    return fail("gc: db2 pre-gc read");
  }

  szBefore = fileSizeOf(zDb);
  if( probeExec(db1, "SELECT dolt_gc();")!=SQLITE_OK ) return fail("gc: dolt_gc");
  szAfter = fileSizeOf(zDb);
  if( szAfter<=0 || szAfter>=szBefore ){
    fprintf(stderr, "gc size before=%lld after=%lld\n",
            (long long)szBefore, (long long)szAfter);
    return fail("gc: expected file to shrink");
  }

  /* Peer reads and writes after in-place shrink. */
  if( queryText(db2, "SELECT v FROM t WHERE id=1;", zVal, sizeof(zVal))!=SQLITE_OK
   || strcmp(zVal, "after_gc")!=0 ){
    return fail("gc: db2 post-gc read");
  }
  if( probeExec(db2, "INSERT INTO t VALUES(2, 'peer_after_gc');")!=SQLITE_OK ){
    return fail("gc: db2 post-gc insert");
  }

  if( sqlite3_open(zDb, &db3)!=SQLITE_OK ) return fail("gc: open db3");
  if( queryInt(db3, "SELECT count(*) FROM t;", &cnt)!=SQLITE_OK || cnt!=2 ){
    return fail("gc: db3 row count");
  }
  if( queryText(db3, "SELECT v FROM t WHERE id=2;", zVal, sizeof(zVal))!=SQLITE_OK
   || strcmp(zVal, "peer_after_gc")!=0 ){
    return fail("gc: db3 verify peer write");
  }

  sqlite3_close(db3);
  sqlite3_close(db2);
  sqlite3_close(db1);
  return 0;
}

/* Scenario 2: In-place backup replaces the file with an exact same-size file
** containing different content/manifest; an already-open peer must detect the
** change, reload cleanly, read the new state, and write cleanly. */
static int testSameSizeBackupPeer(const char *zDir){
  char zSrcA[512], zSrcB[512], zDest[512];
  char zVal[128];
  sqlite3 *dbA = 0, *dbB = 0, *db1 = 0, *db2 = 0, *db3 = 0;
  sqlite3_int64 szA, szB, szBefore, szAfter;
  int cnt = 0;

  snprintf(zSrcA, sizeof(zSrcA), "%s/same_a.db", zDir);
  snprintf(zSrcB, sizeof(zSrcB), "%s/same_b.db", zDir);
  snprintf(zDest, sizeof(zDest), "%s/same_dest.db", zDir);

  if( sqlite3_open(zSrcA, &dbA)!=SQLITE_OK ) return fail("same: open srcA");
  if( probeExec(dbA,
      "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
      "INSERT INTO t VALUES(1, 'payload_0123456789_A');"
      "SELECT dolt_add('-A');"
      "SELECT dolt_commit('-m', 'msg_A', '--author', 'Test <t@t>', '--date', '2026-01-01T00:00:00Z');"
      "SELECT dolt_gc();")!=SQLITE_OK ){
    return fail("same: init srcA");
  }
  sqlite3_close(dbA);

  if( sqlite3_open(zSrcB, &dbB)!=SQLITE_OK ) return fail("same: open srcB");
  if( probeExec(dbB,
      "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
      "INSERT INTO t VALUES(1, 'payload_0123456789_B');"
      "SELECT dolt_add('-A');"
      "SELECT dolt_commit('-m', 'msg_B', '--author', 'Test <t@t>', '--date', '2026-01-01T00:00:00Z');"
      "SELECT dolt_gc();")!=SQLITE_OK ){
    return fail("same: init srcB");
  }
  sqlite3_close(dbB);

  szA = fileSizeOf(zSrcA);
  szB = fileSizeOf(zSrcB);
  if( szA<=0 || szA!=szB ){
    fprintf(stderr, "same-size setup mismatch: szA=%lld szB=%lld\n",
            (long long)szA, (long long)szB);
    return fail("same: expected srcA and srcB to have identical size");
  }

  /* Populate dest from srcA, then open peer db2 on dest. */
  if( sqlite3_open(zDest, &db1)!=SQLITE_OK ) return fail("same: open db1");
  if( backupInto(db1, zSrcA)!=SQLITE_OK ) return fail("same: backup srcA");

  if( sqlite3_open(zDest, &db2)!=SQLITE_OK ) return fail("same: open db2");
  if( queryText(db2, "SELECT v FROM t WHERE id=1;", zVal, sizeof(zVal))!=SQLITE_OK
   || strcmp(zVal, "payload_0123456789_A")!=0 ){
    return fail("same: db2 pre-backup read");
  }

  /* Replace dest in place with srcB of identical file size. */
  szBefore = fileSizeOf(zDest);
  if( backupInto(db1, zSrcB)!=SQLITE_OK ) return fail("same: backup srcB");
  szAfter = fileSizeOf(zDest);
  if( szAfter!=szBefore ){
    fprintf(stderr, "same: size changed before=%lld after=%lld\n",
            (long long)szBefore, (long long)szAfter);
    return fail("same: expected identical file size after backup");
  }

  /* Peer must detect the same-size in-place replacement and read srcB's row. */
  if( queryText(db2, "SELECT v FROM t WHERE id=1;", zVal, sizeof(zVal))!=SQLITE_OK
   || strcmp(zVal, "payload_0123456789_B")!=0 ){
    fprintf(stderr, "same: got [%s] expected [payload_0123456789_B]\n", zVal);
    return fail("same: db2 missed same-size in-place replacement");
  }
  if( probeExec(db2, "INSERT INTO t VALUES(2, 'peer_after_same');")!=SQLITE_OK ){
    return fail("same: db2 post-backup insert");
  }

  if( sqlite3_open(zDest, &db3)!=SQLITE_OK ) return fail("same: open db3");
  if( queryInt(db3, "SELECT count(*) FROM t;", &cnt)!=SQLITE_OK || cnt!=2 ){
    return fail("same: db3 row count");
  }
  if( queryText(db3, "SELECT v FROM t WHERE id=1;", zVal, sizeof(zVal))!=SQLITE_OK
   || strcmp(zVal, "payload_0123456789_B")!=0 ){
    return fail("same: db3 row 1");
  }
  if( queryText(db3, "SELECT v FROM t WHERE id=2;", zVal, sizeof(zVal))!=SQLITE_OK
   || strcmp(zVal, "peer_after_same")!=0 ){
    return fail("same: db3 row 2");
  }

  sqlite3_close(db3);
  sqlite3_close(db2);
  sqlite3_close(db1);
  return 0;
}

/* Scenario 3: Peer does a direct write without a prior SELECT after an in-place
** shrink (exercises commit append-point detection). */
static int testDirectWriteAfterShrink(const char *zDir){
  char zSrcSmall[512], zDest[512];
  char zVal[128];
  sqlite3 *dbSmall = 0, *db1 = 0, *db2 = 0, *db3 = 0;
  sqlite3_int64 szBefore, szAfter;
  int i, cnt = 0;

  snprintf(zSrcSmall, sizeof(zSrcSmall), "%s/small.db", zDir);
  snprintf(zDest, sizeof(zDest), "%s/shrink_write.db", zDir);

  if( sqlite3_open(zSrcSmall, &dbSmall)!=SQLITE_OK ) return fail("sw: open small");
  if( probeExec(dbSmall,
      "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
      "INSERT INTO t VALUES(1, 'from_small');"
      "SELECT dolt_add('-A');"
      "SELECT dolt_commit('-m', 'small');"
      "SELECT dolt_gc();")!=SQLITE_OK ){
    return fail("sw: init small");
  }
  sqlite3_close(dbSmall);

  if( sqlite3_open(zDest, &db1)!=SQLITE_OK ) return fail("sw: open db1");
  if( probeExec(db1, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);")!=SQLITE_OK ){
    return fail("sw: create dest");
  }
  if( probeExec(db1, "BEGIN;")!=SQLITE_OK ) return fail("sw: begin");
  for(i=1; i<=80; i++){
    char zSql[256];
    snprintf(zSql, sizeof(zSql),
      "INSERT INTO t VALUES(%d, 'large_row_%03d_%03d_%03d');", i, i, i*5, i*9);
    if( probeExec(db1, zSql)!=SQLITE_OK ) return fail("sw: insert");
  }
  if( probeExec(db1, "COMMIT;")!=SQLITE_OK ) return fail("sw: commit");

  if( sqlite3_open(zDest, &db2)!=SQLITE_OK ) return fail("sw: open db2");
  if( queryInt(db2, "SELECT count(*) FROM t;", &cnt)!=SQLITE_OK || cnt!=80 ){
    return fail("sw: db2 pre-shrink count");
  }

  szBefore = fileSizeOf(zDest);
  if( backupInto(db1, zSrcSmall)!=SQLITE_OK ) return fail("sw: backup small");
  szAfter = fileSizeOf(zDest);
  if( szAfter<=0 || szAfter>=szBefore ){
    return fail("sw: expected dest to shrink");
  }

  /* Direct write on db2 without reading first. */
  if( probeExec(db2, "INSERT INTO t VALUES(2, 'direct_write');")!=SQLITE_OK ){
    return fail("sw: db2 direct write after shrink");
  }

  if( sqlite3_open(zDest, &db3)!=SQLITE_OK ) return fail("sw: open db3");
  if( queryInt(db3, "SELECT count(*) FROM t;", &cnt)!=SQLITE_OK || cnt!=2 ){
    return fail("sw: db3 count");
  }
  if( queryText(db3, "SELECT v FROM t WHERE id=1;", zVal, sizeof(zVal))!=SQLITE_OK
   || strcmp(zVal, "from_small")!=0 ){
    return fail("sw: db3 row 1");
  }
  if( queryText(db3, "SELECT v FROM t WHERE id=2;", zVal, sizeof(zVal))!=SQLITE_OK
   || strcmp(zVal, "direct_write")!=0 ){
    return fail("sw: db3 row 2");
  }

  sqlite3_close(db3);
  sqlite3_close(db2);
  sqlite3_close(db1);
  return 0;
}

int main(void){
  char zDir[] = "/tmp/doltlite-wasm-inplace-XXXXXX";
  if( !mkdtemp(zDir) ) return fail("mkdtemp");
  if( sqlite3_initialize()!=SQLITE_OK ) return fail("sqlite3_initialize");

  if( testGcShrinkPeer(zDir)!=0 ) return 1;
  if( testSameSizeBackupPeer(zDir)!=0 ) return 1;
  if( testDirectWriteAfterShrink(zDir)!=0 ) return 1;

  printf("amalgamation wasm inplace change detect: PASS\n");
  return 0;
}
