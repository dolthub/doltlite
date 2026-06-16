#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
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

static int execSql(sqlite3 *db, const char *sql){
  char *err = 0;
  int rc = sqlite3_exec(db, sql, 0, 0, &err);
  if( err ){
    fprintf(stderr, "exec err: %s -> %s\n", sql, err);
    sqlite3_free(err);
  }
  return rc;
}

static long long queryInt(sqlite3 *db, const char *sql){
  sqlite3_stmt *s = 0;
  long long v = -1;
  if( sqlite3_prepare_v2(db, sql, -1, &s, 0)!=SQLITE_OK ) return -1;
  if( sqlite3_step(s)==SQLITE_ROW ) v = sqlite3_column_int64(s, 0);
  sqlite3_finalize(s);
  return v;
}

static sqlite3 *open_db(const char *path){
  sqlite3 *db = 0;
  sqlite3_open(path, &db);
  sqlite3_busy_timeout(db, 5000);
  return db;
}

static void test_seq_bump_visible(void){
  const char *path = "/tmp/test_seq_bump.db";
  sqlite3 *b;
  pid_t pid;
  int status;
  long long maxId, newId;
  int rc;

  printf("--- Test 1: AUTOINCREMENT bump in other proc visible after refresh ---\n");
  remove(path);

  b = open_db(path);
  execSql(b, "CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)");
  execSql(b, "INSERT INTO t(v) VALUES('p1')");
  execSql(b, "SELECT dolt_commit('-A','-m','init')");
  check("seq_initial_max_id_is_1", queryInt(b, "SELECT MAX(id) FROM t")==1);

  pid = fork();
  if( pid==0 ){
    sqlite3 *c = open_db(path);
    execSql(c, "INSERT INTO t(v) VALUES('c2')");
    execSql(c, "INSERT INTO t(v) VALUES('c3')");
    execSql(c, "INSERT INTO t(v) VALUES('c4')");
    execSql(c, "INSERT INTO t(v) VALUES('c5')");
    execSql(c, "SELECT dolt_commit('-A','-m','child bump')");
    sqlite3_close(c);
    _exit(0);
  }
  waitpid(pid, &status, 0);
  check("seq_child_exited_ok", WIFEXITED(status) && WEXITSTATUS(status)==0);

  maxId = queryInt(b, "SELECT MAX(id) FROM t");
  check("seq_parent_sees_child_rows", maxId==5);

  rc = execSql(b, "INSERT INTO t(v) VALUES('p6')");
  check("seq_parent_insert_after_refresh_ok", rc==SQLITE_OK);

  newId = queryInt(b, "SELECT id FROM t WHERE v='p6'");
  check("seq_parent_insert_picks_id_6", newId==6);

  sqlite3_close(b);
  remove(path);
}

static void test_new_seq_visible(void){
  const char *path = "/tmp/test_seq_new.db";
  sqlite3 *b;
  pid_t pid;
  int status;
  long long newId;
  int rc;

  printf("--- Test 2: new AUTOINCREMENT table in other proc visible after refresh ---\n");
  remove(path);

  b = open_db(path);
  execSql(b, "CREATE TABLE other(x INTEGER)");
  execSql(b, "INSERT INTO other VALUES(1)");
  execSql(b, "SELECT dolt_commit('-A','-m','no seq yet')");
  check("newseq_initial_no_sequences",
        queryInt(b, "SELECT COUNT(*) FROM other")==1);

  pid = fork();
  if( pid==0 ){
    sqlite3 *c = open_db(path);
    execSql(c, "CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)");
    execSql(c, "INSERT INTO t(v) VALUES('c1')");
    execSql(c, "INSERT INTO t(v) VALUES('c2')");
    execSql(c, "INSERT INTO t(v) VALUES('c3')");
    execSql(c, "SELECT dolt_commit('-A','-m','add seq table')");
    sqlite3_close(c);
    _exit(0);
  }
  waitpid(pid, &status, 0);
  check("newseq_child_exited_ok", WIFEXITED(status) && WEXITSTATUS(status)==0);

  check("newseq_parent_sees_child_rows",
        queryInt(b, "SELECT COUNT(*) FROM t")==3);

  rc = execSql(b, "INSERT INTO t(v) VALUES('p4')");
  check("newseq_parent_insert_after_refresh_ok", rc==SQLITE_OK);

  newId = queryInt(b, "SELECT id FROM t WHERE v='p4'");
  check("newseq_parent_insert_picks_id_4", newId==4);

  sqlite3_close(b);
  remove(path);
}

int main(void){
  printf("=== AUTOINCREMENT cross-process reload tests ===\n\n");

  test_seq_bump_visible();
  test_new_seq_visible();

  printf("\n=== Results: %d passed, %d failed out of %d tests ===\n",
    nPass, nFail, nPass+nFail);
  return nFail > 0 ? 1 : 0;
}
