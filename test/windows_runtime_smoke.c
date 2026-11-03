#include <windows.h>
#include <stdio.h>
#include <string.h>
#include "sqlite3.h"

typedef int (*OpenFn)(const char*, sqlite3**);
typedef int (*CloseFn)(sqlite3*);
typedef int (*ExecFn)(sqlite3*, const char*, sqlite3_callback, void*, char**);

static int checkResult(void *p, int n, char **values, char **names){
  (void)names;
  if( n!=1 || !values[0] || strcmp(values[0], "prolly:merged")!=0 ) return 1;
  *(int*)p = 1;
  return 0;
}

int wmain(int argc, wchar_t **argv){
  HMODULE library;
  FARPROC address;
  OpenFn openDb;
  CloseFn closeDb;
  ExecFn execSql;
  sqlite3 *db = 0;
  int rc = 1, matched = 0;
  if( argc!=2 ) return 2;
  library = LoadLibraryExW(argv[1], 0,
      LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR | LOAD_LIBRARY_SEARCH_SYSTEM32);
  if( !library ){
    fprintf(stderr, "LoadLibraryExW failed: %lu\n", (unsigned long)GetLastError());
    return 1;
  }
  address = GetProcAddress(library, "sqlite3_open");
  if( !address ) goto done;
  memcpy(&openDb, &address, sizeof(openDb));
  address = GetProcAddress(library, "sqlite3_close");
  if( !address ) goto done;
  memcpy(&closeDb, &address, sizeof(closeDb));
  address = GetProcAddress(library, "sqlite3_exec");
  if( !address ) goto done;
  memcpy(&execSql, &address, sizeof(execSql));
  if( openDb("runtime-smoke.db", &db)!=SQLITE_OK ) goto close;
  rc = execSql(db,
      "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT);"
      "INSERT INTO t VALUES(1,'base');"
      "SELECT dolt_commit('-Am','base');"
      "SELECT dolt_branch('feature'); SELECT dolt_checkout('feature');"
      "UPDATE t SET v='merged'; SELECT dolt_commit('-am','feature');"
      "SELECT dolt_checkout('main'); SELECT dolt_merge('feature');",
      0, 0, 0);
  if( rc!=SQLITE_OK ) goto close;
  rc = execSql(db,
      "SELECT doltlite_engine() || ':' || v FROM t WHERE id=1",
      checkResult, &matched, 0);
  if( rc==SQLITE_OK && !matched ) rc = 1;
close:
  if( db && closeDb(db)!=SQLITE_OK ) rc = 1;
done:
  if( !FreeLibrary(library) ) rc = 1;
  if( rc==SQLITE_OK ) puts("Windows DLL loads and runs without MSYS2 dependencies");
  else fprintf(stderr, "Windows DLL smoke test failed: %d\n", rc);
  return rc;
}
