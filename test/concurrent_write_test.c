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

static char result_buf[4096];
static const char *queryScalarText(sqlite3 *db, const char *sql){
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

static int execSql(sqlite3 *db, const char *sql){
  char *err = 0;
  int rc = sqlite3_exec(db, sql, 0, 0, &err);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "  SQL error: %s (rc=%d)\n  SQL: %s\n", err ? err : "?", rc, sql);
    sqlite3_free(err);
  }
  return rc;
}

static int execSqlWithBusyRetry(sqlite3 *db, const char *sql, int maxRetries){
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

static void test_multi_writer_dml(void){
  sqlite3 *a = 0, *b = 0, *fresh = 0;
  const char *dbpath = "/tmp/test_multi_writer_dml.db";
  const char *r;
  int rc;

  printf("--- Test 11: Multi-writer DML from different connections ---\n");

  remove(dbpath); { char _w[256]; snprintf(_w,256,"%s-wal",dbpath); remove(_w); }

  rc = sqlite3_open(dbpath, &a);
  check("mw_open_a", rc==SQLITE_OK);
  rc = sqlite3_open(dbpath, &b);
  check("mw_open_b", rc==SQLITE_OK);
  sqlite3_busy_timeout(a, 5000);
  sqlite3_busy_timeout(b, 5000);

  rc = execSql(a, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, qty INT)");
  check("mw_create_table", rc==SQLITE_OK);
  rc = execSql(a, "CREATE INDEX t_v_idx ON t(v)");
  check("mw_create_index", rc==SQLITE_OK);
  r = queryScalarText(a, "SELECT dolt_commit('-A','-m','mw init')");
  check("mw_initial_commit", strlen(r)==40);

  rc = execSql(a, "INSERT INTO t VALUES(1, 'from_a', 10)");
  check("mw_a_insert_1", rc==SQLITE_OK);
  rc = execSql(b, "INSERT INTO t VALUES(2, 'from_b', 20)");
  check("mw_b_insert_2", rc==SQLITE_OK);
  check("mw_count_after_two_writers",
    strcmp(queryScalarText(a, "SELECT count(*) FROM t"), "2")==0);
  check("mw_reader_sees_two",
    strcmp(queryScalarText(b, "SELECT count(*) FROM t"), "2")==0);

  rc = execSql(a, "UPDATE t SET qty=11 WHERE id=1");
  check("mw_a_update", rc==SQLITE_OK);
  rc = execSql(b, "UPDATE t SET qty=22 WHERE id=2");
  check("mw_b_update", rc==SQLITE_OK);
  check("mw_updates_visible",
    strcmp(queryScalarText(a, "SELECT sum(qty) FROM t"), "33")==0);

  rc = execSql(a, "DELETE FROM t WHERE id=1");
  check("mw_a_delete", rc==SQLITE_OK);
  rc = execSql(b, "INSERT INTO t VALUES(3, 'from_b_3', 30)");
  check("mw_b_insert_3", rc==SQLITE_OK);
  check("mw_delete_insert_count",
    strcmp(queryScalarText(a, "SELECT count(*) FROM t"), "2")==0);
  check("mw_index_lookup",
    strcmp(queryScalarText(b, "SELECT id FROM t WHERE v='from_b_3'"), "3")==0);

  rc = execSql(a, "INSERT INTO t VALUES(3, 'dup', 99)");
  check("mw_duplicate_pk_rejected", rc!=SQLITE_OK);
  check("mw_duplicate_did_not_corrupt",
    strcmp(queryScalarText(b, "SELECT count(*) FROM t"), "2")==0);

  execSql(a, "BEGIN");
  rc = execSql(a, "INSERT INTO t VALUES(4, 'held_by_a', 40)");
  check("mw_explicit_a_insert", rc==SQLITE_OK);
  sqlite3_busy_timeout(b, 100);
  rc = execSql(b, "INSERT INTO t VALUES(5, 'blocked_b', 50)");
  check("mw_explicit_b_busy", rc==SQLITE_BUSY);
  rc = execSql(a, "COMMIT");
  check("mw_explicit_a_commit", rc==SQLITE_OK);
  sqlite3_busy_timeout(b, 5000);
  rc = execSql(b, "INSERT INTO t VALUES(5, 'blocked_b', 50)");
  check("mw_explicit_b_retry_ok", rc==SQLITE_OK);

  r = queryScalarText(b, "SELECT dolt_commit('-A','-m','mw mixed writes')");
  check("mw_commit_mixed_writes", strlen(r)==40);

  rc = sqlite3_open(dbpath, &fresh);
  check("mw_open_fresh", rc==SQLITE_OK);
  check("mw_fresh_count",
    strcmp(queryScalarText(fresh, "SELECT count(*) FROM t"), "4")==0);
  check("mw_fresh_sum",
    strcmp(queryScalarText(fresh, "SELECT sum(qty) FROM t"), "142")==0);
  check("mw_latest_commit",
    strcmp(queryScalarText(fresh, "SELECT message FROM dolt_log LIMIT 1"),
           "mw mixed writes")==0);

  sqlite3_close(fresh);
  sqlite3_close(a);
  sqlite3_close(b);
  remove(dbpath); { char _w[256]; snprintf(_w,256,"%s-wal",dbpath); remove(_w); }
}

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

  rc = sqlite3_open(dbpath, &db1);
  check("open_db1", rc==SQLITE_OK);
  rc = sqlite3_open(dbpath, &db2);
  check("open_db2", rc==SQLITE_OK);
  rc = sqlite3_open(dbpath, &db3);
  check("open_db3", rc==SQLITE_OK);
  rc = sqlite3_open(dbpath, &db4);
  check("open_db4", rc==SQLITE_OK);

  sqlite3_busy_timeout(db1, 5000);
  sqlite3_busy_timeout(db2, 5000);
  sqlite3_busy_timeout(db3, 5000);
  sqlite3_busy_timeout(db4, 5000);

  check("db1_on_main", strcmp(queryScalarText(db1, "SELECT active_branch()"), "main")==0);
  check("db2_on_main", strcmp(queryScalarText(db2, "SELECT active_branch()"), "main")==0);
  check("db3_on_main", strcmp(queryScalarText(db3, "SELECT active_branch()"), "main")==0);
  check("db4_on_main", strcmp(queryScalarText(db4, "SELECT active_branch()"), "main")==0);

  printf("--- Test 1: Schema setup from connection 1 ---\n");
  rc = execSql(db1, "CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT, qty INTEGER)");
  check("create_table", rc==SQLITE_OK);
  rc = execSql(db1, "INSERT INTO items VALUES(1, 'apple', 10)");
  check("seed_insert", rc==SQLITE_OK);
  queryScalarText(db1, "SELECT dolt_commit('-A', '-m', 'initial schema and seed data')");

  check("db2_sees_table", strcmp(queryScalarText(db2, "SELECT count(*) FROM items"), "1")==0);
  check("db3_sees_table", strcmp(queryScalarText(db3, "SELECT count(*) FROM items"), "1")==0);
  check("db4_sees_table", strcmp(queryScalarText(db4, "SELECT count(*) FROM items"), "1")==0);

  printf("--- Test 2: Single-writer INSERT, multi-reader verify ---\n");

  rc = execSql(db1, "INSERT INTO items VALUES(2, 'banana', 20)");
  check("db1_insert_2", rc==SQLITE_OK);
  rc = execSql(db1, "INSERT INTO items VALUES(3, 'cherry', 30)");
  check("db1_insert_3", rc==SQLITE_OK);
  rc = execSql(db1, "INSERT INTO items VALUES(4, 'date', 40)");
  check("db1_insert_4", rc==SQLITE_OK);
  rc = execSql(db1, "INSERT INTO items VALUES(5, 'elderberry', 50)");
  check("db1_insert_5", rc==SQLITE_OK);

  check("all_inserts_visible_db1", strcmp(queryScalarText(db1, "SELECT count(*) FROM items"), "5")==0);
  check("all_inserts_visible_db2", strcmp(queryScalarText(db2, "SELECT count(*) FROM items"), "5")==0);

  printf("--- Test 3: Single-writer UPDATE, multi-reader verify ---\n");

  rc = execSql(db1, "UPDATE items SET qty=11 WHERE id=1");
  check("db1_update_1", rc==SQLITE_OK);
  rc = execSql(db1, "UPDATE items SET qty=22 WHERE id=2");
  check("db1_update_2", rc==SQLITE_OK);
  rc = execSql(db1, "UPDATE items SET qty=33 WHERE id=3");
  check("db1_update_3", rc==SQLITE_OK);
  rc = execSql(db1, "UPDATE items SET qty=44 WHERE id=4");
  check("db1_update_4", rc==SQLITE_OK);

  check("update_visible_1", strcmp(queryScalarText(db3, "SELECT qty FROM items WHERE id=1"), "11")==0);
  check("update_visible_2", strcmp(queryScalarText(db4, "SELECT qty FROM items WHERE id=2"), "22")==0);
  check("update_visible_3", strcmp(queryScalarText(db2, "SELECT qty FROM items WHERE id=3"), "33")==0);
  check("update_visible_4", strcmp(queryScalarText(db3, "SELECT qty FROM items WHERE id=4"), "44")==0);

  printf("--- Test 4: Single-writer DELETE, multi-reader verify ---\n");

  rc = execSql(db1, "DELETE FROM items WHERE id=5");
  check("db1_delete", rc==SQLITE_OK);

  check("delete_visible_db2", strcmp(queryScalarText(db2, "SELECT count(*) FROM items"), "4")==0);
  check("delete_visible_db3", strcmp(queryScalarText(db3, "SELECT count(*) FROM items"), "4")==0);

  printf("--- Test 5: Mixed operations, single writer ---\n");

  rc = execSql(db1, "INSERT INTO items VALUES(6, 'fig', 60)");
  check("mix_insert", rc==SQLITE_OK);
  rc = execSql(db1, "UPDATE items SET name='apricot' WHERE id=1");
  check("mix_update", rc==SQLITE_OK);
  rc = execSql(db1, "DELETE FROM items WHERE id=4");
  check("mix_delete", rc==SQLITE_OK);

  check("final_count", strcmp(queryScalarText(db2, "SELECT count(*) FROM items"), "4")==0);
  check("row1_name", strcmp(queryScalarText(db3, "SELECT name FROM items WHERE id=1"), "apricot")==0);
  check("row6_exists", strcmp(queryScalarText(db4, "SELECT name FROM items WHERE id=6"), "fig")==0);
  check("row4_gone", strcmp(queryScalarText(db2, "SELECT count(*) FROM items WHERE id=4"), "0")==0);

  printf("--- Test 6: dolt_commit captures all writes ---\n");

  queryScalarText(db1, "SELECT dolt_commit('-A', '-m', 'writes from single connection')");

  check("commit_log_db1",
    strcmp(queryScalarText(db1, "SELECT message FROM dolt_log LIMIT 1"),
           "writes from single connection")==0);

  printf("--- Test 7: dolt_log shows commit from this session ---\n");

  check("log_has_entries", strcmp(queryScalarText(db1, "SELECT count(*) FROM dolt_log"), "0")!=0);
  check("log_first",
    strcmp(queryScalarText(db1, "SELECT message FROM dolt_log LIMIT 1"),
           "writes from single connection")==0);

  printf("--- Test 8: Reads from other connections while writing ---\n");

  rc = execSql(db1, "INSERT INTO items VALUES(7, 'grape', 70)");
  check("write_for_read_test", rc==SQLITE_OK);

  check("read_during_write_db2",
    strcmp(queryScalarText(db2, "SELECT count(*) FROM items"), "5")==0);
  check("read_during_write_db3",
    strcmp(queryScalarText(db3, "SELECT name FROM items WHERE id=1"), "apricot")==0);
  check("read_during_write_db4",
    strcmp(queryScalarText(db4, "SELECT count(*) FROM items WHERE id=7"), "1")==0);

  rc = execSql(db1, "UPDATE items SET qty=77 WHERE id=7");
  check("update_write", rc==SQLITE_OK);

  check("read_after_update",
    strcmp(queryScalarText(db3, "SELECT qty FROM items WHERE id=7"), "77")==0);

  printf("--- Test 9: Bulk writes from single connection ---\n");

  {
    int i;
    int totalOk = 0;
    for( i=100; i<110; i++ ){
      char sql[256];
      snprintf(sql, sizeof(sql), "INSERT INTO items VALUES(%d, 'bulk-%d', %d)", i, i, i*10);
      rc = execSql(db1, sql);
      if( rc==SQLITE_OK ) totalOk++;
    }
    check("bulk_writes_all_succeeded", totalOk==10);
    check("bulk_count", strcmp(queryScalarText(db1, "SELECT count(*) FROM items WHERE id>=100"), "10")==0);
  }

  printf("--- Test 10: Final commit and verification ---\n");

  queryScalarText(db1, "SELECT dolt_commit('-A', '-m', 'bulk inserts and interleaved ops')");

  check("final_log_has_entries", strcmp(queryScalarText(db1, "SELECT count(*) FROM dolt_log"), "0")!=0);
  check("final_log_msg",
    strcmp(queryScalarText(db1, "SELECT message FROM dolt_log LIMIT 1"),
           "bulk inserts and interleaved ops")==0);

  check("final_total_db1", strcmp(queryScalarText(db1, "SELECT count(*) FROM items"), "15")==0);
  {
    sqlite3 *fresh = 0;
    sqlite3_open(dbpath, &fresh);
    check("final_total_fresh", strcmp(queryScalarText(fresh, "SELECT count(*) FROM items"), "15")==0);
    sqlite3_close(fresh);
  }

  sqlite3_close(db1);
  sqlite3_close(db2);
  sqlite3_close(db3);
  sqlite3_close(db4);
  remove(dbpath); { char _w[256]; snprintf(_w,256,"%s-wal",dbpath); remove(_w); }

  test_multi_writer_dml();

  printf("\nResults: %d passed, %d failed out of %d tests\n", nPass, nFail, nPass+nFail);
  return nFail > 0 ? 1 : 0;
}
