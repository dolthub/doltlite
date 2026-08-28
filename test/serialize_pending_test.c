/* sqlite3_serialize must capture uncommitted writes the way stock captures
** dirty pager pages, and SQLITE_SERIALIZE_NOCOPY must report the image size
** even though a file-backed database has no in-memory image to hand out.
** The serialized image points its working set at the live catalog; the
** source connection's open transaction must survive untouched. */

#ifdef DOLTLITE_PROLLY

#include "sqlite3.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

static int failures = 0;

static void check(int cond, const char *label){
  if( cond ){
    printf("ok %s\n", label);
  }else{
    fprintf(stderr, "FAIL %s\n", label);
    failures++;
  }
}

static char *oneText(sqlite3 *db, const char *zSql){
  sqlite3_stmt *pStmt = 0;
  char *zOut = 0;
  if( sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0)==SQLITE_OK
   && sqlite3_step(pStmt)==SQLITE_ROW ){
    const unsigned char *z = sqlite3_column_text(pStmt, 0);
    if( z ) zOut = strdup((const char*)z);
  }
  sqlite3_finalize(pStmt);
  return zOut;
}

static sqlite3 *deserializeInto(unsigned char *p, sqlite3_int64 n){
  sqlite3 *db2 = 0;
  if( sqlite3_open(":memory:", &db2)!=SQLITE_OK ) return 0;
  if( sqlite3_deserialize(db2, "main", p, n, n,
                          SQLITE_DESERIALIZE_FREEONCLOSE)!=SQLITE_OK ){
    sqlite3_close(db2);
    return 0;
  }
  return db2;
}

static void dirtyTxnCase(const char *zPath){
  sqlite3 *db = 0;
  sqlite3 *db2 = 0;
  unsigned char *p = 0;
  sqlite3_int64 n = 0;
  char *z;

  unlink(zPath);
  check(sqlite3_open(zPath, &db)==SQLITE_OK, "open source");
  check(sqlite3_exec(db,
      "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT);"
      "INSERT INTO t VALUES(1,'committed');"
      "BEGIN;"
      "INSERT INTO t VALUES(2,'dirty');"
      "CREATE TABLE u(x INT);", 0, 0, 0)==SQLITE_OK, "populate + dirty txn");

  p = sqlite3_serialize(db, "main", &n, 0);
  check(p!=0 && n>0, "serialize returns image");

  db2 = deserializeInto(p, n);
  check(db2!=0, "deserialize image");
  p = 0;  /* owned by db2 */
  if( db2 ){
    z = oneText(db2,
        "SELECT group_concat(name) FROM sqlite_master"
        " WHERE type='table' ORDER BY name");
    check(z && strstr(z,"t") && strstr(z,"u"), "image has both tables");
    free(z);
    z = oneText(db2, "SELECT group_concat(b) FROM t");
    check(z && strstr(z,"committed") && strstr(z,"dirty"),
          "image has committed and dirty rows");
    free(z);
    sqlite3_close(db2);
  }

  /* Serialize must not disturb the source's open transaction. */
  check(sqlite3_get_autocommit(db)==0, "source still in transaction");
  check(sqlite3_exec(db, "ROLLBACK", 0, 0, 0)==SQLITE_OK, "rollback works");
  z = oneText(db, "SELECT group_concat(b) FROM t");
  check(z && strcmp(z,"committed")==0 , "rollback discards dirty row");
  free(z);
  z = oneText(db,
      "SELECT count(*) FROM sqlite_master WHERE name='u'");
  check(z && strcmp(z,"0")==0, "rollback discards dirty table");
  free(z);
  sqlite3_close(db);
  unlink(zPath);
}

static void createOnlyCase(const char *zPath){
  sqlite3 *db = 0;
  sqlite3 *db2 = 0;
  unsigned char *p = 0;
  sqlite3_int64 n = 0;
  char *z;

  unlink(zPath);
  check(sqlite3_open(zPath, &db)==SQLITE_OK, "open fresh source");
  check(sqlite3_exec(db,
      "BEGIN; CREATE TABLE only_dirty(x INT);"
      "INSERT INTO only_dirty VALUES(7);", 0, 0, 0)==SQLITE_OK,
      "create-only dirty txn");
  p = sqlite3_serialize(db, "main", &n, 0);
  check(p!=0 && n>0, "fresh-db serialize returns image");
  db2 = deserializeInto(p, n);
  check(db2!=0, "fresh-db image deserializes");
  if( db2 ){
    z = oneText(db2, "SELECT x FROM only_dirty");
    check(z && strcmp(z,"7")==0, "image has create-only table and row");
    free(z);
    sqlite3_close(db2);
  }
  sqlite3_close(db);
  unlink(zPath);
}

static void unlinkDb(const char *zPath){
  char zLock[512];
  unlink(zPath);
  snprintf(zLock, sizeof(zLock), "%s-lock", zPath);
  unlink(zLock);
}

static void attachedDirtyTxnCase(const char *zMain, const char *zAux){
  sqlite3 *db = 0;
  sqlite3 *db2 = 0;
  unsigned char *p = 0;
  sqlite3_int64 n = 0;
  char *z;
  char zSql[700];

  unlinkDb(zMain);
  unlinkDb(zAux);
  check(sqlite3_open(zMain, &db)==SQLITE_OK, "open attached source");
  snprintf(zSql, sizeof(zSql), "ATTACH '%s' AS aux;", zAux);
  check(sqlite3_exec(db, zSql, 0, 0, 0)==SQLITE_OK, "attach aux");
  check(sqlite3_exec(db,
      "CREATE TABLE aux.t(a INTEGER PRIMARY KEY, b TEXT);"
      "INSERT INTO aux.t VALUES(1,'committed');"
      "BEGIN;"
      "INSERT INTO aux.t VALUES(2,'dirty');"
      "CREATE TABLE aux.u(x INT);"
      "INSERT INTO aux.u VALUES(9);", 0, 0, 0)==SQLITE_OK,
      "populate attached + dirty txn");

  p = sqlite3_serialize(db, "aux", &n, 0);
  check(p!=0 && n>0, "serialize attached returns image");

  db2 = deserializeInto(p, n);
  check(db2!=0, "deserialize attached image");
  p = 0;
  if( db2 ){
    z = oneText(db2,
        "SELECT group_concat(name) FROM sqlite_master"
        " WHERE type='table' ORDER BY name");
    check(z && strstr(z,"t") && strstr(z,"u"),
          "attached image has both tables");
    free(z);
    z = oneText(db2, "SELECT group_concat(b) FROM t");
    check(z && strstr(z,"committed") && strstr(z,"dirty"),
          "attached image has committed and dirty rows");
    free(z);
    z = oneText(db2, "SELECT x FROM u");
    check(z && strcmp(z,"9")==0, "attached image has dirty-only table");
    free(z);
    sqlite3_close(db2);
  }

  check(sqlite3_get_autocommit(db)==0, "attached source still in transaction");
  check(sqlite3_exec(db, "COMMIT", 0, 0, 0)==SQLITE_OK,
        "attached source commit after serialize");
  z = oneText(db, "SELECT group_concat(b) FROM aux.t");
  check(z && strstr(z,"committed") && strstr(z,"dirty"),
        "attached commit keeps dirty row");
  free(z);
  sqlite3_close(db);
  unlinkDb(zMain);
  unlinkDb(zAux);
}

static void attachedCreateOnlyCase(const char *zMain, const char *zAux){
  sqlite3 *db = 0;
  sqlite3 *db2 = 0;
  unsigned char *p = 0;
  sqlite3_int64 n = 0;
  char *z;
  char zSql[700];

  unlinkDb(zMain);
  unlinkDb(zAux);
  check(sqlite3_open(zMain, &db)==SQLITE_OK, "open attached fresh source");
  snprintf(zSql, sizeof(zSql), "ATTACH '%s' AS aux;", zAux);
  check(sqlite3_exec(db, zSql, 0, 0, 0)==SQLITE_OK, "attach fresh aux");
  check(sqlite3_exec(db,
      "BEGIN; CREATE TABLE aux.only_dirty(x INT);"
      "INSERT INTO aux.only_dirty VALUES(7);", 0, 0, 0)==SQLITE_OK,
      "attached create-only dirty txn");
  p = sqlite3_serialize(db, "aux", &n, 0);
  check(p!=0 && n>0, "attached fresh-db serialize returns image");
  db2 = deserializeInto(p, n);
  check(db2!=0, "attached fresh-db image deserializes");
  if( db2 ){
    z = oneText(db2, "SELECT x FROM only_dirty");
    check(z && strcmp(z,"7")==0,
          "attached image has create-only table and row");
    free(z);
    sqlite3_close(db2);
  }
  sqlite3_close(db);
  unlinkDb(zMain);
  unlinkDb(zAux);
}

static void attachedDoesNotLeakIntoMain(const char *zMain, const char *zAux){
  sqlite3 *db = 0;
  sqlite3 *db2 = 0;
  unsigned char *p = 0;
  sqlite3_int64 n = 0;
  char *z;
  char zSql[700];

  unlinkDb(zMain);
  unlinkDb(zAux);
  check(sqlite3_open(zMain, &db)==SQLITE_OK, "open crosstalk source");
  snprintf(zSql, sizeof(zSql), "ATTACH '%s' AS aux;", zAux);
  check(sqlite3_exec(db, zSql, 0, 0, 0)==SQLITE_OK, "attach crosstalk aux");
  check(sqlite3_exec(db,
      "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT);"
      "INSERT INTO t VALUES(1,'main-committed');"
      "CREATE TABLE aux.t(a INTEGER PRIMARY KEY, b TEXT);"
      "INSERT INTO aux.t VALUES(1,'aux-committed');"
      "BEGIN;"
      "INSERT INTO t VALUES(2,'main-dirty');"
      "INSERT INTO aux.t VALUES(2,'aux-dirty');", 0, 0, 0)==SQLITE_OK,
      "populate main and aux dirty txns");

  p = sqlite3_serialize(db, "main", &n, 0);
  check(p!=0 && n>0, "serialize main with aux attached");
  db2 = deserializeInto(p, n);
  check(db2!=0, "deserialize main image");
  p = 0;
  if( db2 ){
    z = oneText(db2, "SELECT group_concat(b) FROM t");
    check(z && strstr(z,"main-committed") && strstr(z,"main-dirty")
          && strstr(z,"aux-dirty")==0,
          "main image has main dirty rows only");
    free(z);
    sqlite3_close(db2);
  }

  p = sqlite3_serialize(db, "aux", &n, 0);
  check(p!=0 && n>0, "serialize aux with main dirty");
  db2 = deserializeInto(p, n);
  check(db2!=0, "deserialize aux image");
  p = 0;
  if( db2 ){
    z = oneText(db2, "SELECT group_concat(b) FROM t");
    check(z && strstr(z,"aux-committed") && strstr(z,"aux-dirty")
          && strstr(z,"main-dirty")==0,
          "aux image has aux dirty rows only");
    free(z);
    sqlite3_close(db2);
  }

  sqlite3_close(db);
  unlinkDb(zMain);
  unlinkDb(zAux);
}

static void nocopyCase(const char *zPath){
  sqlite3 *db = 0;
  unsigned char *p = 0;
  sqlite3_int64 nNoCopy = -2;
  sqlite3_int64 nFull = -2;

  unlink(zPath);
  check(sqlite3_open(zPath, &db)==SQLITE_OK, "open nocopy source");
  check(sqlite3_exec(db,
      "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT);"
      "INSERT INTO t VALUES(1,'x');", 0, 0, 0)==SQLITE_OK, "populate nocopy");

  p = sqlite3_serialize(db, "main", &nNoCopy, SQLITE_SERIALIZE_NOCOPY);
  check(p==0, "NOCOPY returns NULL for file-backed db");
  check(nNoCopy>0, "NOCOPY reports a positive size");

  p = sqlite3_serialize(db, "main", &nFull, 0);
  check(p!=0 && nFull>0, "copying serialize works");
  check(nNoCopy==nFull, "NOCOPY size matches the copying size");
  sqlite3_free(p);
  sqlite3_close(db);
  unlink(zPath);
}

int main(void){
  dirtyTxnCase("serialize_pending_test.db");
  createOnlyCase("serialize_pending_fresh_test.db");
  attachedDirtyTxnCase("serialize_pending_attach_main.db",
                       "serialize_pending_attach_aux.db");
  attachedCreateOnlyCase("serialize_pending_attach_fresh_main.db",
                         "serialize_pending_attach_fresh_aux.db");
  attachedDoesNotLeakIntoMain("serialize_pending_crosstalk_main.db",
                              "serialize_pending_crosstalk_aux.db");
  nocopyCase("serialize_pending_nocopy_test.db");
  if( failures ){
    fprintf(stderr, "%d failure(s)\n", failures);
    return 1;
  }
  printf("serialize_pending_test: all cases passed\n");
  return 0;
}

#else
int main(void){ return 0; }
#endif
