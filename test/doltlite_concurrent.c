/*
** Concurrent read/write tests for doltlite WAL mode and MVCC.
**
** Tests snapshot isolation, concurrent readers, reader/writer
** non-blocking, checkpoint, and WAL mode PRAGMA.
**
** Build from build/ directory:
**   cc -I. -I../src -o test_concurrent ../test/doltlite_concurrent.c *.o -lz -lpthread
** Run:
**   ./test_concurrent
*/
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "sqlite3.h"

static int passed = 0;
static int failed = 0;

#define CHECK(label, cond) do { \
  if( cond ){ printf("PASS: %s\n", label); passed++; } \
  else { printf("FAIL: %s\n", label); failed++; } \
} while(0)

static int exec_int(sqlite3 *db, const char *sql, int dflt){
  sqlite3_stmt *stmt;
  int val = dflt;
  if( sqlite3_prepare_v2(db, sql, -1, &stmt, 0)==SQLITE_OK ){
    if( sqlite3_step(stmt)==SQLITE_ROW ){
      val = sqlite3_column_int(stmt, 0);
    }
  }
  sqlite3_finalize(stmt);
  return val;
}

static const char *exec_text(sqlite3 *db, const char *sql, char *buf, int nBuf){
  sqlite3_stmt *stmt;
  buf[0] = 0;
  if( sqlite3_prepare_v2(db, sql, -1, &stmt, 0)==SQLITE_OK ){
    if( sqlite3_step(stmt)==SQLITE_ROW ){
      const char *z = (const char*)sqlite3_column_text(stmt, 0);
      if( z ){
        int n = (int)strlen(z);
        if( n >= nBuf ) n = nBuf - 1;
        memcpy(buf, z, n);
        buf[n] = 0;
      }
    }
  }
  sqlite3_finalize(stmt);
  return buf;
}

static int exec_ok(sqlite3 *db, const char *sql){
  char *err = 0;
  int rc = sqlite3_exec(db, sql, 0, 0, &err);
  if( err ) sqlite3_free(err);
  return rc;
}

static void setup_db(const char *path){
  sqlite3 *db;
  remove(path);
  sqlite3_open(path, &db);
  exec_ok(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
  exec_ok(db, "INSERT INTO t VALUES(1, 'alpha')");
  exec_ok(db, "INSERT INTO t VALUES(2, 'beta')");
  exec_ok(db, "INSERT INTO t VALUES(3, 'gamma')");
  exec_ok(db, "SELECT dolt_commit('-am', 'init')");
  sqlite3_close(db);
}

/* ----------------------------------------------------------------
** Test 1: WAL mode is default
** ---------------------------------------------------------------- */
static void test_wal_default(const char *path){
  sqlite3 *db;
  char buf[64];
  sqlite3_open(path, &db);
  exec_text(db, "PRAGMA journal_mode", buf, sizeof(buf));
  CHECK("wal_default: journal_mode is wal", strcmp(buf, "wal")==0);
  sqlite3_close(db);
}

/* ----------------------------------------------------------------
** Test 2: PRAGMA journal_mode=WAL succeeds
** ---------------------------------------------------------------- */
static void test_wal_pragma(const char *path){
  sqlite3 *db;
  char buf[64];
  sqlite3_open(path, &db);
  exec_text(db, "PRAGMA journal_mode=WAL", buf, sizeof(buf));
  CHECK("wal_pragma: setting WAL mode succeeds", strcmp(buf, "wal")==0);
  sqlite3_close(db);
}

/* ----------------------------------------------------------------
** Test 3: Snapshot isolation — reader doesn't see concurrent commit
** ---------------------------------------------------------------- */
static void test_snapshot_isolation(const char *path){
  sqlite3 *dbA, *dbB;
  char buf[64];

  sqlite3_open(path, &dbA);
  sqlite3_open(path, &dbB);

  /* A starts read transaction */
  exec_ok(dbA, "BEGIN");
  exec_text(dbA, "SELECT val FROM t WHERE id=1", buf, sizeof(buf));
  CHECK("snapshot: A reads 'alpha' before B commits", strcmp(buf, "alpha")==0);

  /* B updates and commits */
  exec_ok(dbB, "UPDATE t SET val='ALPHA' WHERE id=1");
  exec_ok(dbB, "SELECT dolt_commit('-am', 'update alpha')");

  /* A should still see old value */
  exec_text(dbA, "SELECT val FROM t WHERE id=1", buf, sizeof(buf));
  CHECK("snapshot: A still sees 'alpha' after B commits", strcmp(buf, "alpha")==0);

  /* A ends transaction, starts new one — should see update */
  exec_ok(dbA, "COMMIT");
  exec_ok(dbA, "BEGIN");
  exec_text(dbA, "SELECT val FROM t WHERE id=1", buf, sizeof(buf));
  CHECK("snapshot: A sees 'ALPHA' in new transaction", strcmp(buf, "ALPHA")==0);
  exec_ok(dbA, "COMMIT");

  sqlite3_close(dbA);
  sqlite3_close(dbB);
}

/* ----------------------------------------------------------------
** Test 4: Multiple concurrent readers
** ---------------------------------------------------------------- */
static void test_concurrent_readers(const char *path){
  sqlite3 *db1, *db2, *db3;
  int v1, v2, v3;

  sqlite3_open(path, &db1);
  sqlite3_open(path, &db2);
  sqlite3_open(path, &db3);

  /* All three start read transactions simultaneously */
  exec_ok(db1, "BEGIN");
  exec_ok(db2, "BEGIN");
  exec_ok(db3, "BEGIN");

  v1 = exec_int(db1, "SELECT count(*) FROM t", -1);
  v2 = exec_int(db2, "SELECT count(*) FROM t", -1);
  v3 = exec_int(db3, "SELECT count(*) FROM t", -1);

  CHECK("concurrent_readers: all three see same count", v1==v2 && v2==v3 && v1>=3);

  exec_ok(db1, "COMMIT");
  exec_ok(db2, "COMMIT");
  exec_ok(db3, "COMMIT");

  sqlite3_close(db1);
  sqlite3_close(db2);
  sqlite3_close(db3);
}

/* ----------------------------------------------------------------
** Test 5: Reader doesn't block writer
** ---------------------------------------------------------------- */
static void test_reader_no_block_writer(const char *path){
  sqlite3 *dbR, *dbW;
  int rc;

  sqlite3_open(path, &dbR);
  sqlite3_open(path, &dbW);

  /* Reader holds open transaction */
  exec_ok(dbR, "BEGIN");
  exec_int(dbR, "SELECT count(*) FROM t", -1);

  /* Writer should succeed despite open reader */
  exec_ok(dbW, "INSERT INTO t VALUES(100, 'new')");
  rc = exec_ok(dbW, "SELECT dolt_commit('-am', 'add 100')");
  CHECK("reader_no_block_writer: write succeeds while reader is active",
        rc==SQLITE_OK);

  exec_ok(dbR, "COMMIT");

  /* Clean up */
  exec_ok(dbW, "DELETE FROM t WHERE id=100");
  exec_ok(dbW, "SELECT dolt_commit('-am', 'remove 100')");

  sqlite3_close(dbR);
  sqlite3_close(dbW);
}

/* ----------------------------------------------------------------
** Test 6: Writer doesn't block reader
** ---------------------------------------------------------------- */
static void test_writer_no_block_reader(const char *path){
  sqlite3 *dbR, *dbW;

  sqlite3_open(path, &dbR);
  sqlite3_open(path, &dbW);

  /* Writer starts working (uncommitted changes) */
  exec_ok(dbW, "INSERT INTO t VALUES(200, 'wip')");

  /* Reader should succeed despite writer's uncommitted changes */
  exec_ok(dbR, "BEGIN");
  int cnt = exec_int(dbR, "SELECT count(*) FROM t", -1);
  CHECK("writer_no_block_reader: reader succeeds while writer has pending changes",
        cnt >= 3);
  exec_ok(dbR, "COMMIT");

  /* Rollback writer's changes */
  exec_ok(dbW, "DELETE FROM t WHERE id=200");

  sqlite3_close(dbR);
  sqlite3_close(dbW);
}

/* ----------------------------------------------------------------
** Test 7: Snapshot sees consistent state across multiple queries
** ---------------------------------------------------------------- */
static void test_snapshot_consistency(const char *path){
  sqlite3 *dbA, *dbB;
  char buf[64];

  sqlite3_open(path, &dbA);
  sqlite3_open(path, &dbB);

  exec_ok(dbA, "BEGIN");
  int cnt1 = exec_int(dbA, "SELECT count(*) FROM t", -1);

  /* B adds a row and commits */
  exec_ok(dbB, "INSERT INTO t VALUES(300, 'added')");
  exec_ok(dbB, "SELECT dolt_commit('-am', 'add 300')");

  /* A queries again — should see same count (snapshot) */
  int cnt2 = exec_int(dbA, "SELECT count(*) FROM t", -1);
  CHECK("snapshot_consistency: count stable across concurrent commit",
        cnt1==cnt2);

  /* A can also read specific rows consistently */
  exec_text(dbA, "SELECT val FROM t WHERE id=2", buf, sizeof(buf));
  CHECK("snapshot_consistency: A reads 'beta' consistently",
        strcmp(buf, "beta")==0);

  exec_ok(dbA, "COMMIT");

  /* Clean up */
  exec_ok(dbB, "DELETE FROM t WHERE id=300");
  exec_ok(dbB, "SELECT dolt_commit('-am', 'remove 300')");

  sqlite3_close(dbA);
  sqlite3_close(dbB);
}

/* ----------------------------------------------------------------
** Test 8: PRAGMA wal_checkpoint works
** ---------------------------------------------------------------- */
static void test_checkpoint(const char *path){
  sqlite3 *db;
  int rc;

  sqlite3_open(path, &db);

  /* Make some commits to build up WAL */
  exec_ok(db, "INSERT INTO t VALUES(400, 'ckpt1')");
  exec_ok(db, "SELECT dolt_commit('-am', 'ckpt1')");
  exec_ok(db, "INSERT INTO t VALUES(401, 'ckpt2')");
  exec_ok(db, "SELECT dolt_commit('-am', 'ckpt2')");

  /* Checkpoint */
  rc = exec_ok(db, "PRAGMA wal_checkpoint(TRUNCATE)");
  CHECK("checkpoint: PRAGMA wal_checkpoint succeeds", rc==SQLITE_OK);

  /* Data should still be there */
  int cnt = exec_int(db, "SELECT count(*) FROM t WHERE id>=400", -1);
  CHECK("checkpoint: data preserved after checkpoint", cnt==2);

  /* Clean up */
  exec_ok(db, "DELETE FROM t WHERE id>=400");
  exec_ok(db, "SELECT dolt_commit('-am', 'clean up ckpt')");

  sqlite3_close(db);
}

/* ----------------------------------------------------------------
** Test 9: New transaction after checkpoint sees correct data
** ---------------------------------------------------------------- */
static void test_post_checkpoint_read(const char *path){
  sqlite3 *db1, *db2;

  sqlite3_open(path, &db1);

  exec_ok(db1, "INSERT INTO t VALUES(500, 'postckpt')");
  exec_ok(db1, "SELECT dolt_commit('-am', 'add 500')");
  exec_ok(db1, "PRAGMA wal_checkpoint(TRUNCATE)");

  /* Open fresh connection after checkpoint */
  sqlite3_open(path, &db2);
  int cnt = exec_int(db2, "SELECT count(*) FROM t WHERE id=500", -1);
  CHECK("post_checkpoint_read: new connection sees data after checkpoint", cnt==1);

  /* Clean up */
  exec_ok(db1, "DELETE FROM t WHERE id=500");
  exec_ok(db1, "SELECT dolt_commit('-am', 'clean up 500')");

  sqlite3_close(db1);
  sqlite3_close(db2);
}

/* ----------------------------------------------------------------
** Test 10: Snapshot isolation with large data
** ---------------------------------------------------------------- */
static void test_snapshot_large(const char *path){
  sqlite3 *dbA, *dbB;

  sqlite3_open(path, &dbA);
  sqlite3_open(path, &dbB);

  int cnt_before = exec_int(dbA, "SELECT count(*) FROM t", -1);

  /* A starts read transaction */
  exec_ok(dbA, "BEGIN");
  int cnt_a1 = exec_int(dbA, "SELECT count(*) FROM t", -1);

  /* B inserts 100 rows and commits */
  exec_ok(dbB, "BEGIN");
  {
    char sql[128];
    int i;
    for(i = 1000; i < 1100; i++){
      sqlite3_snprintf(sizeof(sql), sql,
        "INSERT INTO t VALUES(%d, 'bulk%d')", i, i);
      exec_ok(dbB, sql);
    }
  }
  exec_ok(dbB, "COMMIT");
  exec_ok(dbB, "SELECT dolt_commit('-am', 'bulk insert')");

  /* A should see original count */
  int cnt_a2 = exec_int(dbA, "SELECT count(*) FROM t", -1);
  CHECK("snapshot_large: 100-row insert invisible to snapshot reader",
        cnt_a1==cnt_a2);

  exec_ok(dbA, "COMMIT");

  /* After ending transaction, A should see new rows */
  int cnt_a3 = exec_int(dbA, "SELECT count(*) FROM t", -1);
  CHECK("snapshot_large: new transaction sees bulk insert",
        cnt_a3 == cnt_before + 100);

  /* Clean up */
  exec_ok(dbB, "DELETE FROM t WHERE id>=1000");
  exec_ok(dbB, "SELECT dolt_commit('-am', 'clean up bulk')");

  sqlite3_close(dbA);
  sqlite3_close(dbB);
}

int main(void){
  const char *path = "/tmp/test_concurrent.db";

  printf("=== Doltlite Concurrent Read/Write Tests ===\n\n");

  setup_db(path);

  test_wal_default(path);
  test_wal_pragma(path);
  test_snapshot_isolation(path);
  test_concurrent_readers(path);
  test_reader_no_block_writer(path);
  test_writer_no_block_reader(path);
  test_snapshot_consistency(path);
  test_checkpoint(path);
  test_post_checkpoint_read(path);
  test_snapshot_large(path);

  printf("\nResults: %d passed, %d failed out of %d tests\n",
         passed, failed, passed+failed);

  remove(path);
  return failed ? 1 : 0;
}
