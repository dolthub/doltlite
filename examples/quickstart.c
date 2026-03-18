/*
** Doltlite Quickstart — based on https://sqlite.org/quickstart.html
**
** Demonstrates Dolt version control features through the SQLite C API:
**   - Commits, branches, and merges
**   - Point-in-time queries (AS OF)
**   - Diff and log
**
** Build (from the build/ directory):
**   gcc -o quickstart ../examples/quickstart.c \
**       -I. -L. -ldoltlite -lpthread -lz
**
** Run:
**   ./quickstart
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "sqlite3.h"

/* Print a result set with column headers */
static int callback(void *label, int argc, char **argv, char **azColName){
  int i;
  for(i=0; i<argc; i++){
    printf("  %-15s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
  }
  printf("\n");
  return 0;
}

/* Execute SQL, print errors */
static void exec(sqlite3 *db, const char *sql){
  char *zErrMsg = 0;
  int rc = sqlite3_exec(db, sql, callback, 0, &zErrMsg);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "SQL error: %s\n  Statement: %s\n", zErrMsg, sql);
    sqlite3_free(zErrMsg);
  }
}

int main(int argc, char **argv){
  sqlite3 *db;
  int rc;
  const char *dbpath = argc > 1 ? argv[1] : "quickstart.db";

  remove(dbpath);  /* Start fresh */

  rc = sqlite3_open(dbpath, &db);
  if( rc ){
    fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
    sqlite3_close(db);
    return 1;
  }

  /* ---- 1. Create schema and make the first commit ---- */
  printf("== Create schema and first commit ==\n\n");
  exec(db,
    "CREATE TABLE employees("
    "  id INTEGER PRIMARY KEY,"
    "  first_name TEXT,"
    "  last_name TEXT"
    ");"
    "INSERT INTO employees VALUES(0, 'Tim',   'Sehn');"
    "INSERT INTO employees VALUES(1, 'Brian', 'Hendriks');"
    "INSERT INTO employees VALUES(2, 'Aaron', 'Son');"
  );
  exec(db, "SELECT dolt_commit('-A', '-m', 'Initial schema and data')");

  printf("Log after first commit:\n");
  exec(db, "SELECT commit_hash, message FROM dolt_log");

  /* ---- 2. Create a branch and make changes ---- */
  printf("== Branch and modify ==\n\n");
  exec(db, "SELECT dolt_branch('feature')");
  exec(db, "SELECT dolt_checkout('feature')");

  exec(db,
    "UPDATE employees SET first_name='Timothy' WHERE id=0;"
    "INSERT INTO employees VALUES(3, 'Daylon', 'Wilkins');"
  );
  exec(db, "SELECT dolt_commit('-A', '-m', 'Rename Tim, add Daylon')");

  /* ---- 3. Point-in-time query: see main without switching ---- */
  printf("== Point-in-time query (main vs feature) ==\n\n");

  printf("Employees on feature (current branch):\n");
  exec(db, "SELECT id, first_name, last_name FROM employees ORDER BY id");

  printf("Employees on main (without switching):\n");
  exec(db,
    "SELECT * FROM dolt_at_employees('main') ORDER BY id"
  );

  /* ---- 4. Switch back to main and merge ---- */
  printf("== Merge feature into main ==\n\n");
  exec(db, "SELECT dolt_checkout('main')");
  exec(db, "SELECT dolt_merge('feature')");

  printf("Employees on main after merge:\n");
  exec(db, "SELECT id, first_name, last_name FROM employees ORDER BY id");

  /* ---- 5. View the diff (audit log) ---- */
  printf("== Audit log: all changes to employees ==\n\n");
  exec(db,
    "SELECT diff_type, to_id, to_first_name, to_last_name"
    "  FROM dolt_diff_employees"
  );

  /* ---- 6. View commit log ---- */
  printf("== Full commit log ==\n\n");
  exec(db, "SELECT commit_hash, message FROM dolt_log");

  /* ---- 7. Tag the release ---- */
  printf("== Tag current state ==\n\n");
  exec(db, "SELECT dolt_tag('v1.0')");
  exec(db, "SELECT name, hash FROM dolt_tags");

  /* ---- Cleanup ---- */
  sqlite3_close(db);
  remove(dbpath);
  printf("Done.\n");
  return 0;
}
