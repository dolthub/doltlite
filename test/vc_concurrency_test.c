/*
** Version-control operation concurrency tests.
**
** Exercises concurrent dolt_checkout(), dolt_commit(), branch metadata reads,
** connection reopen, and post-concurrency dolt_merge() correctness across
** multiple OS processes. Each worker owns a separate branch and key range so
** correctness is deterministic while the VC operations still interleave.
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include "sqlite3.h"

#define N_WORKERS 3
#define N_COMMITS_PER_WORKER 6

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

static void cleanupDb(const char *path){
  char wal[256];
  char shm[256];
  remove(path);
  snprintf(wal, sizeof(wal), "%s-wal", path);
  remove(wal);
  snprintf(shm, sizeof(shm), "%s-shm", path);
  remove(shm);
}

static int isRetryableRc(int rc){
  return rc==SQLITE_BUSY || rc==SQLITE_LOCKED || rc==SQLITE_SCHEMA;
}

static int isRetryableMsg(const char *msg){
  return msg!=0 && (
    strstr(msg, "busy")!=0 ||
    strstr(msg, "locked")!=0 ||
    strstr(msg, "schema has changed")!=0
  );
}

static int isCommitConflictMsg(const char *msg){
  return msg!=0 && strstr(msg, "commit conflict: another connection committed")!=0;
}

static int execSql(sqlite3 *db, const char *sql){
  char *err = 0;
  int rc = sqlite3_exec(db, sql, 0, 0, &err);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "SQL error rc=%d: %s\n  SQL: %s\n",
            rc, err ? err : sqlite3_errmsg(db), sql);
  }
  sqlite3_free(err);
  return rc;
}

static int execSqlWithRetry(sqlite3 *db, const char *sql){
  int attempt;
  for( attempt=0; attempt<400; attempt++ ){
    char *err = 0;
    int rc = sqlite3_exec(db, sql, 0, 0, &err);
    if( rc==SQLITE_OK ){
      sqlite3_free(err);
      return SQLITE_OK;
    }
    if( rc==SQLITE_ERROR && isRetryableMsg(err ? err : sqlite3_errmsg(db)) ){
      sqlite3_free(err);
      sqlite3_sleep(5);
      continue;
    }
    sqlite3_free(err);
    if( !isRetryableRc(rc) ) return rc;
    sqlite3_sleep(5);
  }
  return SQLITE_BUSY;
}

static int queryTextWithRetry(sqlite3 *db, const char *sql, char *out, int nOut){
  int attempt;
  out[0] = 0;
  for( attempt=0; attempt<400; attempt++ ){
    sqlite3_stmt *stmt = 0;
    int rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);
    if( rc!=SQLITE_OK ){
      if( isRetryableRc(rc) || isRetryableMsg(sqlite3_errmsg(db)) ){
        sqlite3_sleep(5);
        continue;
      }
      snprintf(out, nOut, "ERROR: %s", sqlite3_errmsg(db));
      return rc;
    }
    rc = sqlite3_step(stmt);
    if( rc==SQLITE_ROW ){
      const char *v = (const char*)sqlite3_column_text(stmt, 0);
      if( v ) snprintf(out, nOut, "%s", v);
      sqlite3_finalize(stmt);
      return SQLITE_OK;
    }
    sqlite3_finalize(stmt);
    if( rc==SQLITE_DONE ){
      out[0] = 0;
      return SQLITE_OK;
    }
    if( !isRetryableRc(rc) && !isRetryableMsg(sqlite3_errmsg(db)) ){
      snprintf(out, nOut, "STEP_ERR(%d)", rc);
      return rc;
    }
    sqlite3_sleep(5);
  }
  snprintf(out, nOut, "BUSY");
  return SQLITE_BUSY;
}

static int queryCommitWithRetry(sqlite3 *db, const char *sql, char *out, int nOut){
  int attempt;
  out[0] = 0;
  for( attempt=0; attempt<1000; attempt++ ){
    sqlite3_stmt *stmt = 0;
    int rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);
    if( rc!=SQLITE_OK ){
      const char *msg = sqlite3_errmsg(db);
      if( isRetryableRc(rc) || isRetryableMsg(msg) || isCommitConflictMsg(msg) ){
        sqlite3_sleep(5);
        continue;
      }
      snprintf(out, nOut, "ERROR: %s", msg);
      return rc;
    }
    rc = sqlite3_step(stmt);
    if( rc==SQLITE_ROW ){
      const char *v = (const char*)sqlite3_column_text(stmt, 0);
      if( v ) snprintf(out, nOut, "%s", v);
      sqlite3_finalize(stmt);
      return SQLITE_OK;
    }
    sqlite3_finalize(stmt);
    if( rc==SQLITE_DONE ){
      out[0] = 0;
      return SQLITE_OK;
    }
    if( isRetryableRc(rc) || isRetryableMsg(sqlite3_errmsg(db))
        || isCommitConflictMsg(sqlite3_errmsg(db)) ){
      sqlite3_sleep(5);
      continue;
    }
    snprintf(out, nOut, "STEP_ERR(%d)", rc);
    return rc;
  }
  snprintf(out, nOut, "BUSY");
  return SQLITE_BUSY;
}

static int queryIntWithRetry(sqlite3 *db, const char *sql, int *pOut){
  char buf[128];
  int rc = queryTextWithRetry(db, sql, buf, sizeof(buf));
  if( rc==SQLITE_OK ){
    *pOut = atoi(buf);
  }
  return rc;
}

static int setupDb(const char *path){
  sqlite3 *db = 0;
  int rc;
  int worker;

  cleanupDb(path);
  rc = sqlite3_open(path, &db);
  if( rc!=SQLITE_OK ) return rc;
  sqlite3_busy_timeout(db, 5000);

  rc = execSql(db, "CREATE TABLE vc_rows(id INTEGER PRIMARY KEY, branch TEXT, seq INT)");
  if( rc==SQLITE_OK ){
    rc = execSql(db, "INSERT INTO vc_rows VALUES(0, 'main', 0)");
  }
  if( rc==SQLITE_OK ){
    rc = execSql(db, "SELECT dolt_commit('-A','-m','vc init')");
  }
  for( worker=0; rc==SQLITE_OK && worker<N_WORKERS; worker++ ){
    char sql[128];
    snprintf(sql, sizeof(sql), "SELECT dolt_branch('worker%d')", worker);
    rc = execSql(db, sql);
  }

  sqlite3_close(db);
  return rc;
}

static int runWorker(const char *path, int worker){
  sqlite3 *db = 0;
  char branch[32];
  char sql[256];
  char buf[256];
  int rc;
  int i;
  int branchCount = 0;

  snprintf(branch, sizeof(branch), "worker%d", worker);
  rc = sqlite3_open(path, &db);
  if( rc!=SQLITE_OK ) return 10;
  sqlite3_busy_timeout(db, 5000);

  for( i=1; i<=N_COMMITS_PER_WORKER; i++ ){
    snprintf(sql, sizeof(sql), "SELECT dolt_checkout('%s')", branch);
    rc = execSqlWithRetry(db, sql);
    if( rc!=SQLITE_OK ) goto worker_error;

    rc = queryTextWithRetry(db, "SELECT active_branch()", buf, sizeof(buf));
    if( rc!=SQLITE_OK || strcmp(buf, branch)!=0 ) goto worker_error;

    snprintf(sql, sizeof(sql),
             "INSERT INTO vc_rows VALUES(%d, '%s', %d)",
             worker*100 + i, branch, i);
    rc = execSqlWithRetry(db, sql);
    if( rc!=SQLITE_OK ) goto worker_error;

    snprintf(sql, sizeof(sql),
             "SELECT dolt_commit('-A','-m','%s commit %d')",
             branch, i);
    rc = queryCommitWithRetry(db, sql, buf, sizeof(buf));
    if( rc!=SQLITE_OK || strlen(buf)!=40 ) goto worker_error;

    rc = queryTextWithRetry(db, "SELECT message FROM dolt_log LIMIT 1",
                            buf, sizeof(buf));
    snprintf(sql, sizeof(sql), "%s commit %d", branch, i);
    if( rc!=SQLITE_OK || strcmp(buf, sql)!=0 ) goto worker_error;

    rc = queryIntWithRetry(db,
      "SELECT count(*) FROM dolt_branches", &branchCount);
    if( rc!=SQLITE_OK || branchCount<N_WORKERS+1 ) goto worker_error;

    /* Periodically bounce through main and reopen to exercise session state. */
    if( i%2==0 ){
      rc = execSqlWithRetry(db, "SELECT dolt_checkout('main')");
      if( rc!=SQLITE_OK ) goto worker_error;
      rc = queryTextWithRetry(db, "SELECT active_branch()", buf, sizeof(buf));
      if( rc!=SQLITE_OK || strcmp(buf, "main")!=0 ) goto worker_error;
      snprintf(sql, sizeof(sql), "SELECT dolt_checkout('%s')", branch);
      rc = execSqlWithRetry(db, sql);
      if( rc!=SQLITE_OK ) goto worker_error;
    }
    if( i==3 ){
      sqlite3_close(db);
      db = 0;
      rc = sqlite3_open(path, &db);
      if( rc!=SQLITE_OK ) return 11;
      sqlite3_busy_timeout(db, 5000);
    }
  }

  sqlite3_close(db);
  return 0;

worker_error:
  fprintf(stderr, "worker %d failed rc=%d msg=%s sql=%s\n",
          worker, rc, sqlite3_errmsg(db), sql);
  sqlite3_close(db);
  return 1;
}

static void verifyBranchesAndMerge(const char *path){
  sqlite3 *db = 0;
  int rc;
  int worker;
  int count = 0;
  char sql[256];
  char msg[256];

  rc = sqlite3_open(path, &db);
  check("verify_open", rc==SQLITE_OK);
  sqlite3_busy_timeout(db, 5000);

  for( worker=0; worker<N_WORKERS; worker++ ){
    snprintf(sql, sizeof(sql), "SELECT dolt_checkout('worker%d')", worker);
    rc = execSqlWithRetry(db, sql);
    check("verify_checkout_worker", rc==SQLITE_OK);

    snprintf(sql, sizeof(sql),
             "SELECT count(*) FROM vc_rows WHERE branch='worker%d'",
             worker);
    rc = queryIntWithRetry(db, sql, &count);
    check("verify_worker_row_count",
          rc==SQLITE_OK && count==N_COMMITS_PER_WORKER);

    rc = queryIntWithRetry(db, "SELECT count(*) FROM vc_rows", &count);
    check("verify_worker_total_row_count",
          rc==SQLITE_OK && count==N_COMMITS_PER_WORKER+1);

    snprintf(sql, sizeof(sql),
             "SELECT count(*) FROM vc_rows "
             "WHERE branch NOT IN ('main','worker%d')",
             worker);
    rc = queryIntWithRetry(db, sql, &count);
    check("verify_worker_branch_isolation", rc==SQLITE_OK && count==0);

    rc = queryTextWithRetry(db, "SELECT message FROM dolt_log LIMIT 1",
                            msg, sizeof(msg));
    snprintf(sql, sizeof(sql), "worker%d commit %d", worker, N_COMMITS_PER_WORKER);
    check("verify_worker_latest_commit",
          rc==SQLITE_OK && strcmp(msg, sql)==0);
  }

  rc = execSqlWithRetry(db, "SELECT dolt_checkout('main')");
  check("checkout_main_for_merge", rc==SQLITE_OK);
  rc = queryIntWithRetry(db, "SELECT count(*) FROM vc_rows", &count);
  check("main_still_initial", rc==SQLITE_OK && count==1);

  for( worker=0; worker<N_WORKERS; worker++ ){
    snprintf(sql, sizeof(sql), "SELECT dolt_merge('worker%d')", worker);
    rc = queryTextWithRetry(db, sql, msg, sizeof(msg));
    check("merge_worker_branch", rc==SQLITE_OK && strlen(msg)==40);
  }

  rc = queryIntWithRetry(db, "SELECT count(*) FROM vc_rows", &count);
  check("merged_row_count",
        rc==SQLITE_OK && count==1 + N_WORKERS*N_COMMITS_PER_WORKER);
  rc = queryTextWithRetry(db, "SELECT count(*) FROM dolt_log", msg, sizeof(msg));
  check("merged_log_readable", rc==SQLITE_OK && atoi(msg)>N_WORKERS);

  sqlite3_close(db);
}

int main(void){
  const char *path = "/tmp/test_vc_concurrency.db";
  pid_t pids[N_WORKERS];
  int status;
  int i;

  setvbuf(stdout, 0, _IOLBF, 0);
  printf("=== VC Concurrency Test ===\n\n");
  check("setup_db", setupDb(path)==SQLITE_OK);

  for( i=0; i<N_WORKERS; i++ ){
    pids[i] = fork();
    if( pids[i]==0 ) _exit(runWorker(path, i));
    check("fork_worker", pids[i]>=0);
  }

  for( i=0; i<N_WORKERS; i++ ){
    if( pids[i]<0 ) continue;
    status = 0;
    waitpid(pids[i], &status, 0);
    check("worker_exited_cleanly",
          WIFEXITED(status) && WEXITSTATUS(status)==0);
  }

  verifyBranchesAndMerge(path);
  cleanupDb(path);

  printf("\nResults: %d passed, %d failed out of %d tests\n",
         nPass, nFail, nPass+nFail);
  return nFail>0 ? 1 : 0;
}
