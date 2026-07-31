/* Growth-axis benchmark driver: measures how per-operation cost scales
** with history size and gates the growth RATIO rather than absolute wall
** time, so runner load shifts both sides of each ratio and the gates stay
** meaningful on shared CI hosts.
**
**   A. multi-writer refresh: two connections on separate branches
**      alternate single-row commits after a seeded history; each commit
**      makes the peer's next refresh see a grown file. Cost must not
**      scale with the seeded WAL (the incremental tail replay axis).
**   B. open latency: fresh open + first query against the same seeded
**      histories. Replay is linear in WAL today, so linear growth is
**      tolerated and superlinear growth fails; after dolt_gc the WAL is
**      gone and open must not be slower than the pre-gc open.
**   C. commit depth: one connection makes a long chain of single-row
**      commits; the last window must not cost more than the first.
**
** Usage: growth_axis_driver DIR SEED ROUNDS DEPTH
** Gate thresholds are fixed here so CI and local runs agree.
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include "sqlite3.h"

#define RATIO_A_MAX 3.0   /* refresh cost vs 5x seeded history */
#define RATIO_B_MAX 12.0  /* open cost vs 5x seeded history (linear ~5x) */
#define RATIO_B_GC_MAX 1.5 /* post-gc open vs pre-gc open */
#define RATIO_C_MAX 3.0   /* late commits vs early commits */

static int nPass = 0;
static int nFail = 0;

static void gate(const char *name, double ratio, double max){
  if( ratio <= max ){
    printf("  PASS: %s ratio %.2f <= %.2f\n", name, ratio, max);
    nPass++;
  }else{
    printf("  FAIL: %s ratio %.2f > %.2f\n", name, ratio, max);
    nFail++;
  }
}

static double now_ms(void){
  struct timeval tv;
  gettimeofday(&tv, 0);
  return tv.tv_sec*1000.0 + tv.tv_usec/1000.0;
}

static void die(const char *what, const char *detail){
  fprintf(stderr, "FATAL %s: %s\n", what, detail ? detail : "");
  exit(2);
}

static void x(sqlite3 *db, const char *sql){
  char *err = 0;
  if( sqlite3_exec(db, sql, 0, 0, &err)!=SQLITE_OK ){
    fprintf(stderr, "SQL failed: %s\n", err ? err : "?");
    die("exec", sql);
  }
}

static sqlite3 *openDb(const char *path){
  sqlite3 *db = 0;
  if( sqlite3_open(path, &db)!=SQLITE_OK ) die("open", path);
  sqlite3_busy_timeout(db, 30000);
  return db;
}

static void seedDb(const char *path, int nRows){
  sqlite3 *db = openDb(path);
  char sql[256];
  x(db, "SELECT dolt_config('user.name','growth'),"
        "dolt_config('user.email','g@e.com')");
  x(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)");
  x(db, "BEGIN");
  snprintf(sql, sizeof(sql),
    "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<%d) "
    "INSERT INTO t SELECT x, randomblob(200) FROM c", nRows);
  x(db, sql);
  x(db, "COMMIT");
  x(db, "SELECT dolt_commit('-Am','seed')");
  x(db, "SELECT dolt_branch('side')");
  sqlite3_close(db);
}

/* ms per commit for two connections alternating on separate branches. */
static double alternatingCost(const char *path, int seedRows, int nRounds){
  sqlite3 *a = openDb(path);
  sqlite3 *b = openDb(path);
  char sql[256];
  double t0;
  int i;
  x(b, "SELECT dolt_checkout('side')");
  t0 = now_ms();
  for(i=0; i<nRounds; i++){
    snprintf(sql, sizeof(sql),
      "INSERT INTO t VALUES(%d, randomblob(64));"
      "SELECT dolt_commit('-Am','a%d');", seedRows+1+i*2, i);
    x(a, sql);
    snprintf(sql, sizeof(sql),
      "INSERT INTO t VALUES(%d, randomblob(64));"
      "SELECT dolt_commit('-Am','b%d');", seedRows+2+i*2, i);
    x(b, sql);
  }
  t0 = now_ms() - t0;
  sqlite3_close(a);
  sqlite3_close(b);
  return t0 / (nRounds*2);
}

/* Fresh open + first query, which forces the store open and replay. */
static double openCost(const char *path){
  double t0 = now_ms();
  sqlite3 *db = openDb(path);
  sqlite3_stmt *stmt = 0;
  if( sqlite3_prepare_v2(db, "SELECT count(*) FROM t", -1, &stmt, 0)
      !=SQLITE_OK ) die("prepare", sqlite3_errmsg(db));
  if( sqlite3_step(stmt)!=SQLITE_ROW ) die("step", sqlite3_errmsg(db));
  sqlite3_finalize(stmt);
  t0 = now_ms() - t0;
  sqlite3_close(db);
  return t0;
}

/* Median-of-3 to keep one cold-cache or scheduler blip from gating. */
static double openCost3(const char *path){
  double a = openCost(path);
  double b = openCost(path);
  double c = openCost(path);
  if( (a<=b && b<=c) || (c<=b && b<=a) ) return b;
  if( (b<=a && a<=c) || (c<=a && a<=b) ) return a;
  return c;
}

static double commitWindow(sqlite3 *db, int idFrom, int n){
  char sql[256];
  double t0 = now_ms();
  int i;
  for(i=0; i<n; i++){
    snprintf(sql, sizeof(sql),
      "INSERT INTO t VALUES(%d, randomblob(64));"
      "SELECT dolt_commit('-Am','d%d');", idFrom+i, idFrom+i);
    x(db, sql);
  }
  return (now_ms() - t0) / n;
}

int main(int argc, char **argv){
  const char *dir = argc>1 ? argv[1] : "/tmp";
  int seed = argc>2 ? atoi(argv[2]) : 200000;
  int rounds = argc>3 ? atoi(argv[3]) : 40;
  int depth = argc>4 ? atoi(argv[4]) : 1000;
  char small[512], large[512], deep[512], sql[600];
  double aS, aL, oS, oL, oGc, cEarly, cLate;
  int window = depth/5;
  sqlite3 *db;

  snprintf(small, sizeof(small), "%s/growth_small.db", dir);
  snprintf(large, sizeof(large), "%s/growth_large.db", dir);
  snprintf(deep, sizeof(deep), "%s/growth_deep.db", dir);
  remove(small); remove(large); remove(deep);
  snprintf(sql, sizeof(sql), "%s/.growth_small.db-lock", dir); remove(sql);
  snprintf(sql, sizeof(sql), "%s/.growth_large.db-lock", dir); remove(sql);
  snprintf(sql, sizeof(sql), "%s/.growth_deep.db-lock", dir); remove(sql);

  printf("=== Growth-axis benchmarks (seed=%d rounds=%d depth=%d) ===\n\n",
         seed, rounds, depth);

  seedDb(small, seed);
  seedDb(large, seed*5);

  /* A: multi-writer refresh */
  aS = alternatingCost(small, seed, rounds);
  aL = alternatingCost(large, seed*5, rounds);
  printf("A multi-writer refresh: %.1f ms/commit @%d rows, "
         "%.1f ms/commit @%d rows\n", aS, seed, aL, seed*5);
  gate("refresh_cost_flat_vs_history", aL/(aS>0.05?aS:0.05), RATIO_A_MAX);

  /* B: open latency, pre- and post-gc (on the large db) */
  oS = openCost3(small);
  oL = openCost3(large);
  printf("B open+first-query: %.1f ms @%d rows, %.1f ms @%d rows\n",
         oS, seed, oL, seed*5);
  gate("open_cost_at_most_linear", oL/(oS>0.5?oS:0.5), RATIO_B_MAX);
  db = openDb(large);
  x(db, "SELECT dolt_gc()");
  sqlite3_close(db);
  oGc = openCost3(large);
  printf("B open after gc: %.1f ms (pre-gc %.1f ms)\n", oGc, oL);
  gate("open_after_gc_not_slower", oGc/(oL>0.5?oL:0.5), RATIO_B_GC_MAX);

  /* C: commit latency vs history depth (single connection, no gc) */
  seedDb(deep, 1000);
  db = openDb(deep);
  cEarly = commitWindow(db, 2000, window);
  commitWindow(db, 4000, depth - 2*window);
  cLate = commitWindow(db, 4000+depth, window);
  sqlite3_close(db);
  printf("C commit depth: %.1f ms/commit early, %.1f ms/commit late "
         "(%d deep)\n", cEarly, cLate, depth);
  gate("commit_cost_flat_vs_depth", cLate/(cEarly>0.05?cEarly:0.05),
       RATIO_C_MAX);

  remove(small); remove(large); remove(deep);

  printf("\nResults: %d passed, %d failed out of %d gates\n",
         nPass, nFail, nPass+nFail);
  return nFail>0 ? 1 : 0;
}
