/* The commit and rollback hooks report SQL transaction boundaries. A version
** control operation that only moves refs never opens one, so it fires
** neither. Pinned here because the boundary is a published contract and the
** difference between the two groups is otherwise invisible. */
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include "sqlite3.h"
#include "lib/test_tmpdir.h"

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

static int nCommit = 0;
static int nRollback = 0;

static int onCommit(void *pArg){ (void)pArg; nCommit++; return 0; }
static int rejectCommit(void *pArg){ (void)pArg; nCommit++; return 1; }
static void onRollback(void *pArg){ (void)pArg; nRollback++; }

static int exec(sqlite3 *db, const char *zSql){
  return sqlite3_exec(db, zSql, 0, 0, 0);
}

/* Run zSql and report how many times each hook fired. */
static void fired(sqlite3 *db, const char *zSql, int *pnCommit, int *pnRollback){
  nCommit = 0;
  nRollback = 0;
  check(zSql, exec(db, zSql)==SQLITE_OK);
  *pnCommit = nCommit;
  *pnRollback = nRollback;
}

static void expectHooks(
  sqlite3 *db, const char *zLabel, const char *zSql, int bWantCommit
){
  int c = 0, r = 0;
  char zName[192];
  fired(db, zSql, &c, &r);
  snprintf(zName, sizeof(zName), "%s: commit hook %s", zLabel,
           bWantCommit ? "fires" : "silent");
  check(zName, c==(bWantCommit?1:0));
  snprintf(zName, sizeof(zName), "%s: rollback hook silent", zLabel);
  check(zName, r==0);
}

static int scalarInt(sqlite3 *db, const char *zSql){
  sqlite3_stmt *p = 0;
  int result = -1;
  if( sqlite3_prepare_v2(db, zSql, -1, &p, 0)==SQLITE_OK
   && sqlite3_step(p)==SQLITE_ROW ) result = sqlite3_column_int(p, 0);
  sqlite3_finalize(p);
  return result;
}

int main(void){
  char zPath[256];
  sqlite3 *db = 0;

  sqlite3_initialize();
  snprintf(zPath, sizeof(zPath), DOLTLITE_TEST_TMPDIR "/vc_commit_hook_%d.db", (int)getpid());
  remove(zPath);
  check("open", sqlite3_open(zPath, &db)==SQLITE_OK);
  if( !db ) return 1;
  check("seed", exec(db,
      "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT);"
      "INSERT INTO t VALUES(1,'x');"
      "SELECT dolt_commit('-Am','c0');"
      "SELECT dolt_checkout('-b','side');"
      "INSERT INTO t VALUES(9,'side');"
      "SELECT dolt_commit('-am','on side');"
      "SELECT dolt_checkout('main');")==SQLITE_OK);

  sqlite3_commit_hook(db, onCommit, 0);
  sqlite3_rollback_hook(db, onRollback, 0);

  /* Writing rows is a SQL transaction, so both groups are represented. */
  expectHooks(db, "insert", "INSERT INTO t VALUES(2,'y')", 1);
  expectHooks(db, "dolt_add", "SELECT dolt_add('-A')", 1);
  expectHooks(db, "add inside SQL writer", "INSERT INTO t VALUES(30,dolt_add('t'))", 1);

  /* These move refs and publish working state without a SQL transaction. */
  expectHooks(db, "dolt_commit", "SELECT dolt_commit('-m','c1')", 0);
  expectHooks(db, "dolt_branch", "SELECT dolt_branch('b2')", 0);
  expectHooks(db, "dolt_tag", "SELECT dolt_tag('v1')", 0);
  expectHooks(db, "dolt_checkout", "SELECT dolt_checkout('side')", 0);
  expectHooks(db, "dolt_checkout back", "SELECT dolt_checkout('main')", 0);
  expectHooks(db, "dolt_reset hard", "SELECT dolt_reset('--hard')", 0);

  /* A ref move inside an explicit transaction is still not one: the command
  ** seals the transaction itself, leaving the later COMMIT nothing to do. */
  expectHooks(db, "branch in txn",
              "BEGIN; SELECT dolt_branch('b3'); COMMIT;", 0);

  /* Rows written alongside a version control command still commit normally. */
  expectHooks(db, "rows beside dolt_commit",
              "BEGIN; INSERT INTO t VALUES(3,'z'); SELECT dolt_commit('-Am','c2');", 1);

  /* A rolled back SQL transaction reaches the rollback hook as usual. */
  nCommit = 0;
  nRollback = 0;
  check("rollback runs", exec(db,
      "BEGIN; INSERT INTO t VALUES(4,'q'); ROLLBACK;")==SQLITE_OK);
  check("rollback: rollback hook fires", nRollback==1);
  check("rollback: commit hook silent", nCommit==0);

  check("add failure seed", exec(db, "INSERT INTO t VALUES(5,'pending')")==SQLITE_OK);
  nCommit = nRollback = 0;
  check("add missing table fails", exec(db, "SELECT dolt_add('t','missing')")==SQLITE_ERROR);
  check("add missing table closes transaction", sqlite3_get_autocommit(db));
  check("add missing table hooks", nCommit==0 && nRollback==1);
  check("add missing table leaves staging unchanged", scalarInt(db,
      "SELECT count(*) FROM dolt_status WHERE staged")==0);

  sqlite3_commit_hook(db, rejectCommit, 0);
  nCommit = nRollback = 0;
  check("add rejected commit fails", (exec(db, "SELECT dolt_add('-A')")&255)==SQLITE_CONSTRAINT);
  check("add rejected commit closes transaction", sqlite3_get_autocommit(db));
  check("add rejected commit hooks", nCommit==1 && nRollback==1);
  check("add rejected commit leaves staging unchanged", scalarInt(db,
      "SELECT count(*) FROM dolt_status WHERE staged")==0);
  check("add rejected commit keeps working rows", scalarInt(db,
      "SELECT count(*) FROM t WHERE a=5")==1);
  {
    sqlite3 *peer = 0;
    check("add rejected commit peer open", sqlite3_open(zPath, &peer)==SQLITE_OK);
    check("add rejected commit durable staging unchanged", scalarInt(peer,
        "SELECT count(*) FROM dolt_status WHERE staged")==0);
    check("add rejected commit durable working rows", scalarInt(peer,
        "SELECT count(*) FROM t WHERE a=5")==1);
    check("add rejected commit releases lock", exec(peer,
        "INSERT INTO t VALUES(6,'peer')")==SQLITE_OK);
    sqlite3_close(peer);
  }
  sqlite3_commit_hook(db, onCommit, 0);
  expectHooks(db, "add retry after rejected commit", "SELECT dolt_add('t')", 1);
  expectHooks(db, "commit staged rows", "SELECT dolt_commit('-m','staged')", 0);
  expectHooks(db, "empty commit inside BEGIN",
      "BEGIN; SELECT dolt_commit('--allow-empty','-m','empty');", 0);

  sqlite3_commit_hook(db, rejectCommit, 0);
  nCommit = nRollback = 0;
  check("commit veto inside BEGIN",
      (exec(db, "BEGIN; INSERT INTO t VALUES(70,'veto');"
                "SELECT dolt_commit('-am','vetoed');")&255)==SQLITE_CONSTRAINT);
  check("commit veto inside BEGIN hooks", nCommit==1 && nRollback==1);
  check("commit veto inside BEGIN discards rows", scalarInt(db,
      "SELECT count(*) FROM t WHERE a=70")==0);
  check("commit veto inside BEGIN keeps HEAD", scalarInt(db,
      "SELECT count(*) FROM dolt_log WHERE message='vetoed'")==0);

  sqlite3_commit_hook(db, 0, 0);
  sqlite3_rollback_hook(db, 0, 0);
  sqlite3_close(db);
  remove(zPath);
  printf("vc_commit_hook_test: %d passed, %d failed\n", nPass, nFail);
  return nFail ? 1 : 0;
}
