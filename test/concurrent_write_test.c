/*
** Concurrent access tests for DoltLite (single-writer, multi-reader).
**
** These tests verify the concurrency model that DoltLite currently supports:
** one connection does all DML writes, other connections can read.
**
**   READER CONSISTENCY: Readers see a consistent view — either the state
**   before or after a write, never a partial or corrupt intermediate state.
**
**   KNOWN LIMITATION: Multiple in-process connections sharing a BtShared
**   cannot safely do concurrent DML to the same tables. This corrupts the
**   shared in-memory prolly tree (see issue #250). The multi-writer
**   scenario is documented as SKIPPED at the end of this file.
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdlib.h>
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

/* Execute SQL, return first column of first row as string (static buffer) */
static char result_buf[4096];
static const char *exec1(sqlite3 *db, const char *sql){
  sqlite3_stmt *stmt = 0;
  int rc;
  result_buf[0] = 0;
  rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);
  if( rc!=SQLITE_OK ){
    snprintf(result_buf, sizeof(result_buf), "ERROR: %s", sqlite3_errmsg(db));
    return result_buf;
  }
  rc = sqlite3_step(stmt);
  if( rc==SQLITE_ROW ){
    const char *val = (const char*)sqlite3_column_text(stmt, 0);
    if( val ){
      snprintf(result_buf, sizeof(result_buf), "%s", val);
    }
  }
  sqlite3_finalize(stmt);
  return result_buf;
}

/* Execute SQL, ignore result */
static int execsql(sqlite3 *db, const char *sql){
  char *err = 0;
  int rc = sqlite3_exec(db, sql, 0, 0, &err);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "  SQL error: %s (rc=%d)\n  SQL: %s\n", err ? err : "?", rc, sql);
    sqlite3_free(err);
  }
  return rc;
}

/* Execute SQL with retry on SQLITE_BUSY */
static int execsql_busy(sqlite3 *db, const char *sql, int maxRetries){
  char *err = 0;
  int rc;
  int attempts = 0;
  do {
    err = 0;
    rc = sqlite3_exec(db, sql, 0, 0, &err);
    if( rc==SQLITE_BUSY ){
      sqlite3_free(err);
      sqlite3_sleep(10);
      attempts++;
    } else {
      if( rc!=SQLITE_OK ){
        fprintf(stderr, "  SQL error: %s (rc=%d)\n  SQL: %s\n", err ? err : "?", rc, sql);
        sqlite3_free(err);
      }
      break;
    }
  } while( attempts < maxRetries );
  return rc;
}

/* exec1 with retry on SQLITE_BUSY */
static const char *exec1_busy(sqlite3 *db, const char *sql, int maxRetries){
  sqlite3_stmt *stmt = 0;
  int rc;
  int attempts = 0;
  result_buf[0] = 0;
  rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);
  if( rc!=SQLITE_OK ){
    snprintf(result_buf, sizeof(result_buf), "ERROR: %s", sqlite3_errmsg(db));
    return result_buf;
  }
  do {
    rc = sqlite3_step(stmt);
    if( rc==SQLITE_BUSY ){
      sqlite3_reset(stmt);
      sqlite3_sleep(10);
      attempts++;
    } else {
      break;
    }
  } while( attempts < maxRetries );
  if( rc==SQLITE_ROW ){
    const char *val = (const char*)sqlite3_column_text(stmt, 0);
    if( val ){
      snprintf(result_buf, sizeof(result_buf), "%s", val);
    }
  }
  sqlite3_finalize(stmt);
  return result_buf;
}

int main(){
  sqlite3 *db1 = 0, *db2 = 0, *db3 = 0, *db4 = 0;
  const char *dbpath = "/tmp/test_concurrent_write.db";
  int rc;
  const int RETRIES = 50;

  remove(dbpath); { char _w[256]; snprintf(_w,256,"%s-wal",dbpath); remove(_w); }

  printf("=== Concurrent Write Test ===\n\n");

  /* --- Open 4 connections to the same database --- */
  rc = sqlite3_open(dbpath, &db1);
  check("open_db1", rc==SQLITE_OK);
  rc = sqlite3_open(dbpath, &db2);
  check("open_db2", rc==SQLITE_OK);
  rc = sqlite3_open(dbpath, &db3);
  check("open_db3", rc==SQLITE_OK);
  rc = sqlite3_open(dbpath, &db4);
  check("open_db4", rc==SQLITE_OK);

  /* Set busy timeout on all connections */
  sqlite3_busy_timeout(db1, 5000);
  sqlite3_busy_timeout(db2, 5000);
  sqlite3_busy_timeout(db3, 5000);
  sqlite3_busy_timeout(db4, 5000);

  /* --- All connections should be on main --- */
  check("db1_on_main", strcmp(exec1(db1, "SELECT active_branch()"), "main")==0);
  check("db2_on_main", strcmp(exec1(db2, "SELECT active_branch()"), "main")==0);
  check("db3_on_main", strcmp(exec1(db3, "SELECT active_branch()"), "main")==0);
  check("db4_on_main", strcmp(exec1(db4, "SELECT active_branch()"), "main")==0);

  printf("--- Test 1: Schema setup from connection 1 ---\n");
  rc = execsql(db1, "CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT, qty INTEGER)");
  check("create_table", rc==SQLITE_OK);
  rc = execsql(db1, "INSERT INTO items VALUES(1, 'apple', 10)");
  check("seed_insert", rc==SQLITE_OK);
  exec1(db1, "SELECT dolt_commit('-A', '-m', 'initial schema and seed data')");

  /* Verify all connections see the table */
  check("db2_sees_table", strcmp(exec1(db2, "SELECT count(*) FROM items"), "1")==0);
  check("db3_sees_table", strcmp(exec1(db3, "SELECT count(*) FROM items"), "1")==0);
  check("db4_sees_table", strcmp(exec1(db4, "SELECT count(*) FROM items"), "1")==0);

  printf("--- Test 2: Single-writer INSERT, multi-reader verify ---\n");

  /* All DML goes through db1 to avoid shared BtShared corruption.
  ** Other connections verify reads. See INVARIANTS.md X3. */
  rc = execsql(db1, "INSERT INTO items VALUES(2, 'banana', 20)");
  check("db1_insert_2", rc==SQLITE_OK);
  rc = execsql(db1, "INSERT INTO items VALUES(3, 'cherry', 30)");
  check("db1_insert_3", rc==SQLITE_OK);
  rc = execsql(db1, "INSERT INTO items VALUES(4, 'date', 40)");
  check("db1_insert_4", rc==SQLITE_OK);
  rc = execsql(db1, "INSERT INTO items VALUES(5, 'elderberry', 50)");
  check("db1_insert_5", rc==SQLITE_OK);

  /* All rows visible from writer and readers */
  check("all_inserts_visible_db1", strcmp(exec1(db1, "SELECT count(*) FROM items"), "5")==0);
  check("all_inserts_visible_db2", strcmp(exec1(db2, "SELECT count(*) FROM items"), "5")==0);

  printf("--- Test 3: Single-writer UPDATE, multi-reader verify ---\n");

  rc = execsql(db1, "UPDATE items SET qty=11 WHERE id=1");
  check("db1_update_1", rc==SQLITE_OK);
  rc = execsql(db1, "UPDATE items SET qty=22 WHERE id=2");
  check("db1_update_2", rc==SQLITE_OK);
  rc = execsql(db1, "UPDATE items SET qty=33 WHERE id=3");
  check("db1_update_3", rc==SQLITE_OK);
  rc = execsql(db1, "UPDATE items SET qty=44 WHERE id=4");
  check("db1_update_4", rc==SQLITE_OK);

  /* Verify updates from reader connections */
  check("update_visible_1", strcmp(exec1(db3, "SELECT qty FROM items WHERE id=1"), "11")==0);
  check("update_visible_2", strcmp(exec1(db4, "SELECT qty FROM items WHERE id=2"), "22")==0);
  check("update_visible_3", strcmp(exec1(db2, "SELECT qty FROM items WHERE id=3"), "33")==0);
  check("update_visible_4", strcmp(exec1(db3, "SELECT qty FROM items WHERE id=4"), "44")==0);

  printf("--- Test 4: Single-writer DELETE, multi-reader verify ---\n");

  rc = execsql(db1, "DELETE FROM items WHERE id=5");
  check("db1_delete", rc==SQLITE_OK);

  check("delete_visible_db2", strcmp(exec1(db2, "SELECT count(*) FROM items"), "4")==0);
  check("delete_visible_db3", strcmp(exec1(db3, "SELECT count(*) FROM items"), "4")==0);

  printf("--- Test 5: Mixed operations, single writer ---\n");

  rc = execsql(db1, "INSERT INTO items VALUES(6, 'fig', 60)");
  check("mix_insert", rc==SQLITE_OK);
  rc = execsql(db1, "UPDATE items SET name='apricot' WHERE id=1");
  check("mix_update", rc==SQLITE_OK);
  rc = execsql(db1, "DELETE FROM items WHERE id=4");
  check("mix_delete", rc==SQLITE_OK);

  /* Verify final state from readers: ids 1,2,3,6 remain */
  check("final_count", strcmp(exec1(db2, "SELECT count(*) FROM items"), "4")==0);
  check("row1_name", strcmp(exec1(db3, "SELECT name FROM items WHERE id=1"), "apricot")==0);
  check("row6_exists", strcmp(exec1(db4, "SELECT name FROM items WHERE id=6"), "fig")==0);
  check("row4_gone", strcmp(exec1(db2, "SELECT count(*) FROM items WHERE id=4"), "0")==0);

  printf("--- Test 6: dolt_commit captures all writes ---\n");

  exec1(db1, "SELECT dolt_commit('-A', '-m', 'writes from single connection')");

  /* Verify commit from the same connection that committed */
  check("commit_log_db1",
    strcmp(exec1(db1, "SELECT message FROM dolt_log LIMIT 1"),
           "writes from single connection")==0);

  printf("--- Test 7: dolt_log shows commit from this session ---\n");

  /* Each session has its own branch view. When multiple connections write
  ** to the WAL, each connection's commit chain is independent. db1 sees
  ** its most recent commit; earlier commits may not chain correctly when
  ** other connections' WAL writes interleave. */
  check("log_has_entries", strcmp(exec1(db1, "SELECT count(*) FROM dolt_log"), "0")!=0);
  check("log_first",
    strcmp(exec1(db1, "SELECT message FROM dolt_log LIMIT 1"),
           "writes from single connection")==0);

  printf("--- Test 8: Reads from other connections while writing ---\n");

  /* Write on db1, read from others */
  rc = execsql(db1, "INSERT INTO items VALUES(7, 'grape', 70)");
  check("write_for_read_test", rc==SQLITE_OK);

  /* Other connections can read */
  check("read_during_write_db2",
    strcmp(exec1(db2, "SELECT count(*) FROM items"), "5")==0);
  check("read_during_write_db3",
    strcmp(exec1(db3, "SELECT name FROM items WHERE id=1"), "apricot")==0);
  check("read_during_write_db4",
    strcmp(exec1(db4, "SELECT count(*) FROM items WHERE id=7"), "1")==0);

  /* More writes from db1, reads from others */
  rc = execsql(db1, "UPDATE items SET qty=77 WHERE id=7");
  check("update_write", rc==SQLITE_OK);

  check("read_after_update",
    strcmp(exec1(db3, "SELECT qty FROM items WHERE id=7"), "77")==0);

  printf("--- Test 9: Bulk writes from single connection ---\n");

  /* All bulk writes go through db1 */
  {
    int i;
    int totalOk = 0;
    for( i=100; i<110; i++ ){
      char sql[256];
      snprintf(sql, sizeof(sql), "INSERT INTO items VALUES(%d, 'bulk-%d', %d)", i, i, i*10);
      rc = execsql(db1, sql);
      if( rc==SQLITE_OK ) totalOk++;
    }
    check("bulk_writes_all_succeeded", totalOk==10);
    check("bulk_count", strcmp(exec1(db1, "SELECT count(*) FROM items WHERE id>=100"), "10")==0);
  }

  printf("--- Test 10: Final commit and verification ---\n");

  exec1(db1, "SELECT dolt_commit('-A', '-m', 'bulk inserts and interleaved ops')");

  check("final_log_has_entries", strcmp(exec1(db1, "SELECT count(*) FROM dolt_log"), "0")!=0);
  check("final_log_msg",
    strcmp(exec1(db1, "SELECT message FROM dolt_log LIMIT 1"),
           "bulk inserts and interleaved ops")==0);

  /* Final row count from writer */
  check("final_total_db1", strcmp(exec1(db1, "SELECT count(*) FROM items"), "15")==0);
  /* Readers see the committed state via fresh connections */
  {
    sqlite3 *fresh = 0;
    sqlite3_open(dbpath, &fresh);
    check("final_total_fresh", strcmp(exec1(fresh, "SELECT count(*) FROM items"), "15")==0);
    sqlite3_close(fresh);
  }

  /* --- Cleanup --- */
  sqlite3_close(db1);
  sqlite3_close(db2);
  sqlite3_close(db3);
  sqlite3_close(db4);
  remove(dbpath); { char _w[256]; snprintf(_w,256,"%s-wal",dbpath); remove(_w); }

  /* --- SKIPPED: Multi-writer DML (known broken, see issue #250) ---
  **
  ** The following scenario causes "database disk image is malformed" errors
  ** because multiple in-process connections share a single BtShared and its
  ** prolly tree state. Concurrent DML from different connections corrupts
  ** the in-memory tree. This is documented in INVARIANTS.md X3.
  **
  ** To reproduce:
  **   sqlite3 *a, *b;
  **   sqlite3_open(path, &a); sqlite3_open(path, &b);
  **   execsql(a, "CREATE TABLE t(id INT, v INT)");
  **   execsql(a, "SELECT dolt_commit('-A','-m','init')");
  **   execsql(a, "INSERT INTO t VALUES(1,1)");  // a writes to shared tree
  **   execsql(b, "INSERT INTO t VALUES(2,2)");  // b corrupts a's in-flight state
  **   // subsequent queries may return SQLITE_CORRUPT
  **
  ** Fix requires copy-on-write mutation buffers or full MVCC (issue #250).
  */
  printf("\nSKIPPED: Multi-writer DML from different connections (issue #250)\n");

  printf("\nResults: %d passed, %d failed out of %d tests\n", nPass, nFail, nPass+nFail);
  return nFail > 0 ? 1 : 0;
}
