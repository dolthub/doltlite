#include "sqlite3.h"
#include <stdio.h>
#include <string.h>

static int discard(void *ctx, const void *data, int n){
  *(int*)ctx += n;
  return SQLITE_OK;
}

int main(int argc, char **argv){
  sqlite3 *db = 0, *out = 0;
  sqlite3_stmt *q = 0;
  char *error = 0;
  int bytes = 0;
  if( sqlite3_open(":memory:",&db)!=SQLITE_OK ) return 1;
  if( sqlite3_exec(db,"CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT);"
      "INSERT INTO t VALUES(7,'export'); SELECT dolt_commit('-Am','seed');"
      "UPDATE t SET v='working'",0,0,0)!=SQLITE_OK ) return 2;
  if( doltlite_dump_sql(db,0,discard,&bytes,&error)!=SQLITE_OK
   || bytes==0 || error ) return 3;
  if( sqlite3_open_v2("file::memory:?doltlite_engine=sqlite",&out,
      SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE|SQLITE_OPEN_URI,0)!=SQLITE_OK ) return 4;
  if( doltlite_export_sqlite(db,"HEAD",out,&error)!=SQLITE_OK || error ) return 5;
  if( sqlite3_prepare_v2(out,"SELECT id,v FROM t",-1,&q,0)!=SQLITE_OK ) return 6;
  if( sqlite3_step(q)!=SQLITE_ROW || sqlite3_column_int(q,0)!=7
   || strcmp((const char*)sqlite3_column_text(q,1),"export") ) return 7;
  sqlite3_finalize(q);
  sqlite3_close(out);
  if( argc>1 ){
    const char *formats[] = {"sql","sqlite"};
    int i;
    for(i=0; i<2; i++){
      char *path = sqlite3_mprintf("%s.%s",argv[1],formats[i]);
      char *sql = sqlite3_mprintf("SELECT dolt_dump('--file-name',%Q,'-r',%Q)",path,formats[i]);
      int rc = sqlite3_exec(db,sql,0,0,&error);
      if( rc!=SQLITE_OK ){
        fprintf(stderr,"file export: %s\n",error ? error : sqlite3_errmsg(db));
        return 8;
      }
      sqlite3_free(sql); sqlite3_free(path);
    }
  }
  sqlite3_close(db);
  puts("amalgamation logical export: PASS");
  return 0;
}
