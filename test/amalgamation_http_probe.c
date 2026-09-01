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
  int n = 0;
  char *zSql;
  const char *zSrc;
  const char *zClone;
  const char *zLazy = 0;
  const char *zUrl;

  if( argc!=4 && argc!=5 ){
    fprintf(stderr, "usage: %s SRC_DB CLONE_DB HTTP_URL [LAZY_DB]\n", argv[0]);
    return 1;
  }
  zSrc = argv[1];
  zClone = argv[2];
  zUrl = argv[3];
  if( argc==5 ) zLazy = argv[4];

  if( doltliteInstallAutoExt()!=SQLITE_OK ){
    fprintf(stderr, "doltliteInstallAutoExt failed\n");
    return 1;
  }

  if( sqlite3_open(zSrc, &db)!=SQLITE_OK ){
    fprintf(stderr, "%s\n", db ? sqlite3_errmsg(db) : "sqlite3_open failed");
    return 1;
  }
  if( scalar_int(db, "SELECT length(dolt_creds_new())>0;", &n) || !n ){
    fprintf(stderr, "amalgamation credential creation failed\n");
    sqlite3_close(db);
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
  zSql = sqlite3_mprintf(
      "SELECT dolt_remote('add','origin',%Q);"
      "SELECT dolt_push('origin','main');", zUrl);
  if( !zSql || exec_sql(db, zSql) ){
    sqlite3_free(zSql);
    sqlite3_close(db);
    return 1;
  }
  sqlite3_free(zSql);
  sqlite3_close(db);

  if( zLazy ){
    if( sqlite3_open(zLazy, &db)!=SQLITE_OK ){
      fprintf(stderr, "%s\n", db ? sqlite3_errmsg(db) : "sqlite3_open failed");
      return 1;
    }
    zSql = sqlite3_mprintf("SELECT dolt_clone('--lazy',%Q);", zUrl);
    if( !zSql || exec_sql(db, zSql) ){
      sqlite3_free(zSql);
      sqlite3_close(db);
      return 1;
    }
    sqlite3_free(zSql);
    sqlite3_close(db);
  }

  if( sqlite3_open(zClone, &db)!=SQLITE_OK ){
    fprintf(stderr, "%s\n", db ? sqlite3_errmsg(db) : "sqlite3_open failed");
    return 1;
  }
  zSql = sqlite3_mprintf("SELECT dolt_clone(%Q);", zUrl);
  if( !zSql || exec_sql(db, zSql) ){
    sqlite3_free(zSql);
    sqlite3_close(db);
    return 1;
  }
  sqlite3_free(zSql);
  if( scalar_int(db, "SELECT count(*) FROM users;", &n) ){
    sqlite3_close(db);
    return 1;
  }
  sqlite3_close(db);

  if( n!=3 ){
    fprintf(stderr, "expected 3 cloned users, got %d\n", n);
    return 1;
  }

  if( sqlite3_open(zSrc, &db)!=SQLITE_OK ){
    fprintf(stderr, "%s\n", db ? sqlite3_errmsg(db) : "sqlite3_open failed");
    return 1;
  }
  if( exec_sql(db,
        "INSERT INTO users VALUES(4,'dora');"
        "SELECT dolt_add('-A');"
        "SELECT dolt_commit('-m','second');"
        "SELECT dolt_push('origin','main');") ){
    sqlite3_close(db);
    return 1;
  }
  sqlite3_close(db);

  if( sqlite3_open(zClone, &db)!=SQLITE_OK ){
    fprintf(stderr, "%s\n", db ? sqlite3_errmsg(db) : "sqlite3_open failed");
    return 1;
  }
  if( exec_sql(db, "SELECT dolt_pull('origin','main');")
   || scalar_int(db, "SELECT count(*) FROM users;", &n) ){
    sqlite3_close(db);
    return 1;
  }
  sqlite3_close(db);

  if( n!=4 ){
    fprintf(stderr, "expected 4 users after pull, got %d\n", n);
    return 1;
  }
  return 0;
}
