/*
** Regression test for issue #305.
**
** Verifies that ROLLBACK TO restores the full catalog entry snapshot for a
** table, not just its prolly root. The test mutates the in-memory schemaHash
** inside a savepoint, rolls back, and confirms the serialized catalog bytes
** match the pre-savepoint snapshot.
**
** Build from build/:
**   cc -g -I. -o savepoint_catalog_restore_test \
**     ../test/savepoint_catalog_restore_test.c libdoltlite.a -lz -lpthread -lm
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "sqlite3.h"
#include "prolly_hash.h"

typedef unsigned char u8;
typedef unsigned int Pgno;

extern int doltliteFlushAndSerializeCatalog(sqlite3 *db, u8 **ppOut, int *pnOut);
extern void doltliteSetTableSchemaHash(sqlite3 *db, Pgno iTable, const ProllyHash *pH);

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

static int execsql(sqlite3 *db, const char *sql){
  char *err = 0;
  int rc = sqlite3_exec(db, sql, 0, 0, &err);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "  SQL error: %s (rc=%d)\n  SQL: %s\n",
            err ? err : "?", rc, sql);
    sqlite3_free(err);
  }
  return rc;
}

static Pgno table_rootpage(sqlite3 *db, const char *zName){
  sqlite3_stmt *stmt = 0;
  Pgno pgno = 0;
  if( sqlite3_prepare_v2(
          db,
          "SELECT rootpage FROM sqlite_master "
          "WHERE type='table' AND name=?1",
          -1, &stmt, 0)==SQLITE_OK ){
    sqlite3_bind_text(stmt, 1, zName, -1, SQLITE_STATIC);
    if( sqlite3_step(stmt)==SQLITE_ROW ){
      pgno = (Pgno)sqlite3_column_int(stmt, 0);
    }
  }
  sqlite3_finalize(stmt);
  return pgno;
}

static void remove_db(const char *path){
  char tmp[512];
  remove(path);
  snprintf(tmp, sizeof(tmp), "%s-wal", path);
  remove(tmp);
  snprintf(tmp, sizeof(tmp), "%s-shm", path);
  remove(tmp);
  snprintf(tmp, sizeof(tmp), "%s-journal", path);
  remove(tmp);
}

static void test_schema_hash_restored_on_savepoint_rollback(void){
  sqlite3 *db = 0;
  const char *dbpath = "/tmp/test_savepoint_catalog_restore.db";
  u8 *aBefore = 0;
  u8 *aAfter = 0;
  int nBefore = 0;
  int nAfter = 0;
  Pgno iTable;
  ProllyHash fakeHash;

  printf("--- Test 1: savepoint rollback restores schema metadata ---\n");
  remove_db(dbpath);

  check("open_db", sqlite3_open(dbpath, &db)==SQLITE_OK);
  if( !db ) return;

  check("create_table", execsql(db,
      "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);")==SQLITE_OK);
  check("serialize_before",
      doltliteFlushAndSerializeCatalog(db, &aBefore, &nBefore)==SQLITE_OK);

  iTable = table_rootpage(db, "t");
  check("lookup_rootpage", iTable>0);

  memset(&fakeHash, 0x5a, sizeof(fakeHash));
  check("savepoint_begin", execsql(db, "SAVEPOINT sp;")==SQLITE_OK);
  doltliteSetTableSchemaHash(db, iTable, &fakeHash);
  check("rollback_to", execsql(db, "ROLLBACK TO sp;")==SQLITE_OK);
  check("release_sp", execsql(db, "RELEASE sp;")==SQLITE_OK);

  check("serialize_after",
      doltliteFlushAndSerializeCatalog(db, &aAfter, &nAfter)==SQLITE_OK);
  check("catalog_equal_after_rollback",
      nBefore==nAfter && memcmp(aBefore, aAfter, nBefore)==0);

  sqlite3_free(aBefore);
  sqlite3_free(aAfter);
  sqlite3_close(db);
}

int main(void){
  test_schema_hash_restored_on_savepoint_rollback();
  printf("\n%d passed, %d failed\n", nPass, nFail);
  return nFail ? 1 : 0;
}
