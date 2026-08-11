#include "sqlite3.h"
#include <stdio.h>
#include <stdlib.h>

extern int doltliteInstallAutoExt(void);

static int exec_sql(sqlite3 *db, const char *zSql){
  char *zErr = 0;
  int rc = sqlite3_exec(db, zSql, 0, 0, &zErr);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "%s\n", zErr ? zErr : sqlite3_errmsg(db));
    sqlite3_free(zErr);
    return 1;
  }
  return 0;
}

static int scalar_int(sqlite3 *db, const char *zSql, int *pVal){
  sqlite3_stmt *pStmt = 0;
  int rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "%s\n", sqlite3_errmsg(db));
    return 1;
  }
  rc = sqlite3_step(pStmt);
  if( rc!=SQLITE_ROW ){
    fprintf(stderr, "expected row, got rc=%d\n", rc);
    sqlite3_finalize(pStmt);
    return 1;
  }
  *pVal = sqlite3_column_int(pStmt, 0);
  sqlite3_finalize(pStmt);
  return 0;
}

int main(int argc, char **argv){
  sqlite3 *db = 0;
  sqlite3_stmt *pStmt = 0;
  int n = 0;

  if( argc!=2 ){
    fprintf(stderr, "usage: %s DB\n", argv[0]);
    return 1;
  }

  if( doltliteInstallAutoExt()!=SQLITE_OK ){
    fprintf(stderr, "doltliteInstallAutoExt failed\n");
    return 1;
  }

  if( sqlite3_open(argv[1], &db)!=SQLITE_OK ){
    fprintf(stderr, "%s\n", db ? sqlite3_errmsg(db) : "sqlite3_open failed");
    return 1;
  }
  if( exec_sql(db,
        "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT);"
        "INSERT INTO users VALUES(1,'alice'),(2,'bob'),(3,'charlie');"
        "SELECT dolt_add('-A');"
        "SELECT dolt_commit('-m','initial');") ){
    sqlite3_close(db);
    return 1;
  }
  if( sqlite3_prepare_v2(db, "SELECT dolt_remote('add','x','file:///x')",
                         -1, &pStmt, 0)==SQLITE_OK ){
    fprintf(stderr, "remote SQL is present in the amalgamation\n");
    sqlite3_finalize(pStmt);
    sqlite3_close(db);
    return 1;
  }
  if( scalar_int(db, "SELECT count(*) FROM users;", &n) ){
    sqlite3_close(db);
    return 1;
  }
  sqlite3_close(db);

  if( n!=3 ){
    fprintf(stderr, "expected 3 users, got %d\n", n);
    return 1;
  }
  return 0;
}
