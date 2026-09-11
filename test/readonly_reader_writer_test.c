#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#include <signal.h>
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

static int scalar(sqlite3 *db, const char *zSql){
  sqlite3_stmt *pStmt = 0;
  int n = -1;
  if( sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0)==SQLITE_OK
   && sqlite3_step(pStmt)==SQLITE_ROW ){
    n = sqlite3_column_int(pStmt, 0);
  }
  sqlite3_finalize(pStmt);
  return n;
}

/* Reading dolt_status on a read-only connection must not take the exclusive
** graph lock. It used to, to refresh an idle connection's view, which starved
** a concurrent writer: every insert came back SQLITE_BUSY and every commit
** claimed a peer had committed to the branch. */
static void test_readonly_status_does_not_starve_a_writer(const char *zPath){
  const int nRound = 150;
  pid_t pid;
  sqlite3 *db = 0;
  int status = 0;
  int i;
  int nInsErr = 0, nComErr = 0;

  remove(zPath);
  check("ro_starve: open", sqlite3_open(zPath, &db)==SQLITE_OK);
  if( !db ) return;
  check("ro_starve: seed", sqlite3_exec(db,
      "CREATE TABLE e(id TEXT PRIMARY KEY);"
      "SELECT dolt_commit('-Am','init');", 0, 0, 0)==SQLITE_OK);
  sqlite3_close(db);

  pid = fork();
  if( pid==0 ){
    /* Reader: a fresh read-only connection per round, like a status poller. */
    for(;;){
      sqlite3 *ro = 0;
      if( sqlite3_open_v2(zPath, &ro, SQLITE_OPEN_READONLY, 0)==SQLITE_OK ){
        (void)scalar(ro, "SELECT count(*) FROM dolt_status");
      }
      sqlite3_close(ro);
    }
    _exit(0);
  }
  check("ro_starve: fork", pid>0);
  if( pid<=0 ) return;

  if( sqlite3_open(zPath, &db)!=SQLITE_OK ){
    kill(pid, SIGKILL);
    waitpid(pid, &status, 0);
    check("ro_starve: writer open", 0);
    return;
  }
  for(i=1; i<=nRound; i++){
    char zSql[128];
    sqlite3_snprintf(sizeof(zSql), zSql, "INSERT INTO e VALUES('r%d')", i);
    if( sqlite3_exec(db, zSql, 0, 0, 0)!=SQLITE_OK ) nInsErr++;
    if( sqlite3_exec(db, "SELECT dolt_commit('-Am','c')", 0, 0, 0)!=SQLITE_OK ){
      nComErr++;
    }
  }
  check("ro_starve: no insert was refused", nInsErr==0);
  check("ro_starve: no commit was refused", nComErr==0);
  check("ro_starve: every row is present",
        scalar(db, "SELECT count(*) FROM e")==nRound);
  sqlite3_close(db);

  kill(pid, SIGKILL);
  waitpid(pid, &status, 0);

  /* And the rows are durable, not just visible to the writer. */
  check("ro_starve: reopen", sqlite3_open(zPath, &db)==SQLITE_OK);
  if( db ){
    check("ro_starve: rows survive reopen",
          scalar(db, "SELECT count(*) FROM e")==nRound);
    sqlite3_close(db);
  }
  remove(zPath);
}

/* Skipping that refresh must not cost a read-only connection its view of a
** peer's work: it still sees an uncommitted change and a later commit. */
static void test_readonly_still_sees_peer_changes(const char *zPath){
  sqlite3 *ro = 0, *rw = 0;

  remove(zPath);
  if( sqlite3_open(zPath, &rw)!=SQLITE_OK ) return;
  check("ro_fresh: seed", sqlite3_exec(rw,
      "CREATE TABLE e(id TEXT PRIMARY KEY);"
      "INSERT INTO e VALUES('a');"
      "SELECT dolt_commit('-Am','init');", 0, 0, 0)==SQLITE_OK);

  check("ro_fresh: open readonly",
        sqlite3_open_v2(zPath, &ro, SQLITE_OPEN_READONLY, 0)==SQLITE_OK);
  if( !ro ){ sqlite3_close(rw); return; }

  check("ro_fresh: clean to start",
        scalar(ro, "SELECT count(*) FROM dolt_status")==0);

  check("ro_fresh: peer insert",
        sqlite3_exec(rw, "INSERT INTO e VALUES('b')", 0, 0, 0)==SQLITE_OK);
  check("ro_fresh: sees the dirty table",
        scalar(ro, "SELECT count(*) FROM dolt_status")==1);
  check("ro_fresh: sees the new row",
        scalar(ro, "SELECT count(*) FROM e")==2);

  check("ro_fresh: peer commit",
        sqlite3_exec(rw, "SELECT dolt_commit('-Am','c')", 0, 0, 0)==SQLITE_OK);
  check("ro_fresh: clean after the commit",
        scalar(ro, "SELECT count(*) FROM dolt_status")==0);
  check("ro_fresh: still sees the row",
        scalar(ro, "SELECT count(*) FROM e")==2);

  sqlite3_close(ro);
  sqlite3_close(rw);
  remove(zPath);
}

int main(void){
  char zPath[256];
  snprintf(zPath, sizeof(zPath), "/tmp/dolt_ro_reader_%d.db", (int)getpid());

  test_readonly_status_does_not_starve_a_writer(zPath);
  test_readonly_still_sees_peer_changes(zPath);

  printf("readonly_reader_writer_test: %d passed, %d failed\n", nPass, nFail);
  return nFail ? 1 : 0;
}
