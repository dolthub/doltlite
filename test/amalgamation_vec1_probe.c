#include "sqlite3.h"
#include <stdio.h>
#include <string.h>

int main(int argc, char **argv){
  sqlite3 *db = 0;
  sqlite3_stmt *stmt = 0;
  char *err = 0;
  int expectEnabled;
  int rc;

  if( argc<2 || argc>3 ) return 2;
  expectEnabled = strcmp(argv[1], "disabled")!=0;
  rc = sqlite3_open(":memory:", &db);
  if( rc!=SQLITE_OK ) return 3;
  if( argc==3 ){
    sqlite3_enable_load_extension(db, 1);
    rc = sqlite3_load_extension(db, argv[2], 0, &err);
    if( rc!=SQLITE_OK ){
      fprintf(stderr, "vec1 load failed: %s\n", err ? err : sqlite3_errmsg(db));
      sqlite3_free(err);
      sqlite3_close(db);
      return 4;
    }
  }
  rc = sqlite3_prepare_v2(db, "SELECT vec1_info()", -1, &stmt, 0);
  if( expectEnabled ){
    if( rc!=SQLITE_OK || sqlite3_step(stmt)!=SQLITE_ROW ){
      fprintf(stderr, "vec1 unavailable: %s\n", sqlite3_errmsg(db));
      sqlite3_finalize(stmt);
      sqlite3_close(db);
      return 5;
    }
  }else if( rc==SQLITE_OK ){
    fprintf(stderr, "vec1 unexpectedly available\n");
    sqlite3_finalize(stmt);
    sqlite3_close(db);
    return 6;
  }
  sqlite3_finalize(stmt);
  sqlite3_close(db);
  return 0;
}
