#include "sqlite3.h"
#include <stdio.h>
#include <string.h>

extern int doltliteInstallAutoExt(void);

int main(void){
  sqlite3 *db = 0;
  char *zErr = 0;
  int rc;

  if( doltliteInstallAutoExt()!=SQLITE_OK ) return 1;
  if( sqlite3_open(":memory:", &db)!=SQLITE_OK ) return 1;
  rc = sqlite3_exec(db, "SELECT dolt_clone('https://dolthub.com/a/b');",
                    0, 0, &zErr);
  if( rc==SQLITE_OK || !zErr ||
      !strstr(zErr, "DoltLite remotes are disabled in this build") ){
    fprintf(stderr, "unexpected disabled-remotes result: %s\n",
            zErr ? zErr : sqlite3_errmsg(db));
    sqlite3_free(zErr);
    sqlite3_close(db);
    return 1;
  }
  sqlite3_free(zErr);
  sqlite3_close(db);
  return 0;
}
