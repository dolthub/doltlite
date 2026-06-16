#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include "sqlite3.h"

#define N_WORKERS 4
#define N_KEYS_PER_WORKER 32
#define N_OPS_PER_WORKER 120
#define BASE_SEED 0x5eed1234u

typedef struct KeyState KeyState;
struct KeyState {
  int present;
  int val;
};

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

static void cleanup_db(const char *path){
  char wal[256];
  char shm[256];
  remove(path);
  snprintf(wal, sizeof(wal), "%s-wal", path);
  remove(wal);
  snprintf(shm, sizeof(shm), "%s-shm", path);
  remove(shm);
}

static unsigned nextRand(unsigned *pSeed){
  *pSeed = (*pSeed * 1103515245u) + 12345u;
  return *pSeed;
}

static int isRetryableRc(int rc){
  return rc==SQLITE_BUSY || rc==SQLITE_LOCKED || rc==SQLITE_SCHEMA;
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
  for( attempt=0; attempt<200; attempt++ ){
    char *err = 0;
    int rc = sqlite3_exec(db, sql, 0, 0, &err);
    if( rc==SQLITE_OK ){
      sqlite3_free(err);
      return SQLITE_OK;
    }
    sqlite3_free(err);
    if( !isRetryableRc(rc) ) return rc;
    sqlite3_sleep(5);
  }
  return SQLITE_BUSY;
}

static int queryInt64WithRetry(sqlite3 *db, const char *sql, sqlite3_int64 *pOut){
  int attempt;
  for( attempt=0; attempt<200; attempt++ ){
    sqlite3_stmt *stmt = 0;
    int rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);
    if( rc!=SQLITE_OK ){
      if( isRetryableRc(rc) ){
        sqlite3_sleep(5);
        continue;
      }
      return rc;
    }
    rc = sqlite3_step(stmt);
    if( rc==SQLITE_ROW ){
      *pOut = sqlite3_column_int64(stmt, 0);
      sqlite3_finalize(stmt);
      return SQLITE_OK;
    }
    sqlite3_finalize(stmt);
    if( !isRetryableRc(rc) ) return rc;
    sqlite3_sleep(5);
  }
  return SQLITE_BUSY;
}

static int envInt(const char *zName, int defaultValue){
  const char *zValue = getenv(zName);
  char *zEnd = 0;
  long v;
  if( zValue==0 || zValue[0]==0 ) return defaultValue;
  v = strtol(zValue, &zEnd, 10);
  if( zEnd==zValue || zEnd[0]!=0 || v<0 || v>0x7fffffff ){
    fprintf(stderr, "Ignoring invalid %s=%s\n", zName, zValue);
    return defaultValue;
  }
  return (int)v;
}

static void applyModelOp(KeyState state[N_WORKERS][N_KEYS_PER_WORKER],
                         int worker, int op, unsigned rnd){
  int key = (int)(rnd % N_KEYS_PER_WORKER);
  int action = (int)((rnd >> 8) % 5);
  int val = (worker + 1) * 100000 + op * 257 + key;
  KeyState *p = &state[worker][key];

  if( action==0 ){
    p->present = 0;
    p->val = 0;
  }else{
    p->present = 1;
    p->val = val;
  }
}

static void buildOpSql(char *zSql, size_t nSql, int worker, int op, unsigned rnd){
  int key = (int)(rnd % N_KEYS_PER_WORKER);
  int action = (int)((rnd >> 8) % 5);
  int id = worker * 1000 + key;
  int val = (worker + 1) * 100000 + op * 257 + key;

  if( action==0 ){
    snprintf(zSql, nSql, "DELETE FROM kv WHERE id=%d", id);
  }else{
    snprintf(zSql, nSql,
             "INSERT INTO kv(id, val, worker, op) VALUES(%d, %d, %d, %d) "
             "ON CONFLICT(id) DO UPDATE SET val=excluded.val, "
             "worker=excluded.worker, op=excluded.op",
             id, val, worker, op);
  }
}

static int setupDb(const char *path){
  sqlite3 *db = 0;
  int rc;

  cleanup_db(path);
  rc = sqlite3_open(path, &db);
  if( rc!=SQLITE_OK ) return rc;
  sqlite3_busy_timeout(db, 5000);
  rc = execSql(db,
    "CREATE TABLE kv("
    "id INTEGER PRIMARY KEY,"
    "val INTEGER NOT NULL,"
    "worker INTEGER NOT NULL,"
    "op INTEGER NOT NULL)");
  if( rc==SQLITE_OK ){
    rc = execSql(db, "CREATE INDEX kv_worker_idx ON kv(worker, op)");
  }
  if( rc==SQLITE_OK ){
    rc = execSql(db, "SELECT dolt_commit('-A','-m','stress init')");
  }
  sqlite3_close(db);
  return rc;
}

static int runWriter(const char *path, int worker, unsigned baseSeed){
  sqlite3 *db = 0;
  unsigned seed = baseSeed + (unsigned)worker * 7919u;
  int rc;
  int op;

  rc = sqlite3_open(path, &db);
  if( rc!=SQLITE_OK ) return 10;
  sqlite3_busy_timeout(db, 5000);

  for( op=0; op<N_OPS_PER_WORKER; op++ ){
    unsigned rnd = nextRand(&seed);
    char sql[512];
    buildOpSql(sql, sizeof(sql), worker, op, rnd);

    if( op%11==0 ){
      rc = execSqlWithRetry(db, "BEGIN IMMEDIATE");
      if( rc!=SQLITE_OK ) goto writer_error;
      usleep(1000 + (worker * 400));
      rc = execSqlWithRetry(db, sql);
      if( rc!=SQLITE_OK ) goto writer_rollback;
      rc = execSqlWithRetry(db, "COMMIT");
      if( rc!=SQLITE_OK ) goto writer_rollback;
    }else{
      rc = execSqlWithRetry(db, sql);
      if( rc!=SQLITE_OK ) goto writer_error;
    }

    if( op%37==0 ){
      sqlite3_int64 count = 0;
      rc = queryInt64WithRetry(db, "SELECT count(*) FROM kv", &count);
      if( rc!=SQLITE_OK || count<0 ) goto writer_error;
    }
  }

  sqlite3_close(db);
  return 0;

writer_rollback:
  execSqlWithRetry(db, "ROLLBACK");
writer_error:
  fprintf(stderr, "writer %d failed at op %d rc=%d: %s\n",
          worker, op, rc, sqlite3_errmsg(db));
  sqlite3_close(db);
  return 1;
}

static int runReader(const char *path){
  sqlite3 *db = 0;
  int i;
  int rc = sqlite3_open(path, &db);
  if( rc!=SQLITE_OK ) return 20;
  sqlite3_busy_timeout(db, 5000);

  for( i=0; i<350; i++ ){
    sqlite3_int64 count = 0;
    sqlite3_int64 sum = 0;
    rc = queryInt64WithRetry(db, "SELECT count(*) FROM kv", &count);
    if( rc!=SQLITE_OK || count<0 ) goto reader_error;
    rc = queryInt64WithRetry(db, "SELECT coalesce(sum(val),0) FROM kv", &sum);
    if( rc!=SQLITE_OK || sum<0 ) goto reader_error;

    if( i%75==0 ){
      sqlite3_close(db);
      rc = sqlite3_open(path, &db);
      if( rc!=SQLITE_OK ) return 21;
      sqlite3_busy_timeout(db, 5000);
    }
    usleep(2000);
  }

  sqlite3_close(db);
  return 0;

reader_error:
  fprintf(stderr, "reader failed at iteration %d rc=%d: %s\n",
          i, rc, sqlite3_errmsg(db));
  sqlite3_close(db);
  return 1;
}

static void expectedFinal(sqlite3_int64 *pCount, sqlite3_int64 *pSum,
                          unsigned baseSeed){
  KeyState state[N_WORKERS][N_KEYS_PER_WORKER];
  int worker, key, op;

  memset(state, 0, sizeof(state));
  for( worker=0; worker<N_WORKERS; worker++ ){
    unsigned seed = baseSeed + (unsigned)worker * 7919u;
    for( op=0; op<N_OPS_PER_WORKER; op++ ){
      unsigned rnd = nextRand(&seed);
      applyModelOp(state, worker, op, rnd);
    }
  }

  *pCount = 0;
  *pSum = 0;
  for( worker=0; worker<N_WORKERS; worker++ ){
    for( key=0; key<N_KEYS_PER_WORKER; key++ ){
      if( state[worker][key].present ){
        (*pCount)++;
        (*pSum) += state[worker][key].val;
      }
    }
  }
}

static int runStressIteration(int iter, unsigned baseSeed){
  char path[256];
  pid_t pids[N_WORKERS + 1];
  int nPids = 0;
  int i;
  int status;
  sqlite3_int64 expectedCount = 0, expectedSum = 0;
  sqlite3_int64 actualCount = 0, actualSum = 0;
  sqlite3 *db = 0;
  int rc;

  snprintf(path, sizeof(path), "/tmp/test_concurrent_stress_%d.db", iter);
  printf("--- iteration=%d seed=0x%x workers=%d ops_per_worker=%d keys_per_worker=%d ---\n",
         iter, baseSeed, N_WORKERS, N_OPS_PER_WORKER, N_KEYS_PER_WORKER);

  rc = setupDb(path);
  check("setup_db", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ){
    return 1;
  }

  pids[nPids] = fork();
  if( pids[nPids]==0 ) _exit(runReader(path));
  check("fork_reader", pids[nPids]>=0);
  nPids++;

  for( i=0; i<N_WORKERS; i++ ){
    pids[nPids] = fork();
    if( pids[nPids]==0 ) _exit(runWriter(path, i, baseSeed));
    check("fork_writer", pids[nPids]>=0);
    nPids++;
  }

  for( i=0; i<nPids; i++ ){
    if( pids[i]<0 ) continue;
    status = 0;
    waitpid(pids[i], &status, 0);
    check("child_exited_cleanly",
          WIFEXITED(status) && WEXITSTATUS(status)==0);
  }

  expectedFinal(&expectedCount, &expectedSum, baseSeed);

  rc = sqlite3_open(path, &db);
  check("open_final", rc==SQLITE_OK);
  sqlite3_busy_timeout(db, 5000);
  check("final_count_query",
        queryInt64WithRetry(db, "SELECT count(*) FROM kv", &actualCount)==SQLITE_OK);
  check("final_sum_query",
        queryInt64WithRetry(db, "SELECT coalesce(sum(val),0) FROM kv", &actualSum)==SQLITE_OK);
  check("final_count_matches", actualCount==expectedCount);
  check("final_sum_matches", actualSum==expectedSum);

  rc = execSql(db, "SELECT dolt_commit('-A','-m','stress final')");
  check("final_commit", rc==SQLITE_OK);
  check("dolt_log_readable",
        queryInt64WithRetry(db, "SELECT count(*) FROM dolt_log", &actualCount)==SQLITE_OK
        && actualCount>=2);
  sqlite3_close(db);
  cleanup_db(path);
  return nFail>0 ? 1 : 0;
}

int main(void){
  int seconds = envInt("DOLTLITE_STRESS_SECONDS", 0);
  int maxRuns = envInt("DOLTLITE_STRESS_RUNS", 1);
  int iter = 0;
  time_t deadline = seconds>0 ? time(0) + seconds : 0;

  setvbuf(stdout, 0, _IOLBF, 0);
  printf("=== Concurrent Stress Test ===\n");
  if( seconds>0 ){
    maxRuns = 0x7fffffff;
  }

  do {
    unsigned seed = BASE_SEED + (unsigned)iter * 104729u;
    if( runStressIteration(iter, seed)!=0 ) break;
    iter++;
  } while( iter<maxRuns && (seconds==0 || time(0)<deadline) );

  printf("\nResults: %d passed, %d failed out of %d tests\n",
         nPass, nFail, nPass+nFail);
  printf("Completed stress iterations: %d\n", iter);
  return nFail>0 ? 1 : 0;
}
