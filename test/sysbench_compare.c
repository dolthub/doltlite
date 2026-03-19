/*
** sysbench_compare.c — Sysbench-style OLTP benchmark: doltlite vs stock SQLite
**
** Runs identical workloads against both engines in-process and prints
** a markdown comparison table. Links against sqlite3.h — caller provides
** the library (libdoltlite.a or libsqlite3.a) at link time.
**
** Build (from build/):
**   # Stock SQLite:
**   make DOLTLITE_PROLLY=0 sqlite3.o
**   gcc -O2 -o bench_sqlite ../test/sysbench_compare.c sqlite3.o -I. -lpthread -lz -lm -DENGINE_NAME='"SQLite"'
**
**   # Doltlite:
**   gcc -O2 -o bench_doltlite ../test/sysbench_compare.c -I. libdoltlite.a -lpthread -lz -lm -DENGINE_NAME='"Doltlite"'
**
** Run:
**   ./bench_sqlite    # prints JSON line
**   ./bench_doltlite  # prints JSON line
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "sqlite3.h"

#ifndef ENGINE_NAME
#define ENGINE_NAME "Unknown"
#endif

#define ROWS 10000
#define SEED 42

/* ---- Timing ---- */
static double now_ms(void){
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

/* ---- Simple LCG PRNG (deterministic, no libc rand state) ---- */
static unsigned long rng_state;
static void rng_seed(unsigned long s){ rng_state = s; }
static int rng_int(int lo, int hi){
  rng_state = rng_state * 6364136223846793005ULL + 1442695040888963407ULL;
  return lo + (int)((rng_state >> 33) % (unsigned long)(hi - lo + 1));
}
static void rng_str(char *buf, int len){
  int i;
  for(i=0; i<len; i++) buf[i] = 'a' + (rng_int(0,25));
  buf[len] = 0;
}

/* ---- Helpers ---- */
static void exec(sqlite3 *db, const char *sql){
  char *err = 0;
  int rc = sqlite3_exec(db, sql, 0, 0, &err);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "SQL error: %s\n  %s\n", err, sql);
    sqlite3_free(err);
  }
}

static void execf(sqlite3 *db, const char *fmt, ...){
  char buf[4096];
  va_list ap;
  va_start(ap, fmt);
  vsnprintf(buf, sizeof(buf), fmt, ap);
  va_end(ap);
  exec(db, buf);
}

/* ---- Benchmark results ---- */
#define MAX_TESTS 30
static struct { const char *name; double ms; } results[MAX_TESTS];
static int nResults = 0;

static void record(const char *name, double ms){
  results[nResults].name = name;
  results[nResults].ms = ms;
  nResults++;
}

/* ---- Prepare: create schema and insert ROWS ---- */
static void prepare(sqlite3 *db){
  int i;
  char c[121], pad[61];

  exec(db,
    "CREATE TABLE sbtest1("
    "  id INTEGER PRIMARY KEY,"
    "  k INTEGER NOT NULL DEFAULT 0,"
    "  c TEXT NOT NULL DEFAULT '',"
    "  pad TEXT NOT NULL DEFAULT ''"
    ");"
    "CREATE INDEX k_idx ON sbtest1(k);"
  );

  exec(db, "BEGIN");
  for(i=1; i<=ROWS; i++){
    rng_str(c, 120);
    rng_str(pad, 60);
    execf(db, "INSERT INTO sbtest1 VALUES(%d,%d,'%s','%s')", i, rng_int(1,ROWS), c, pad);
  }
  exec(db, "COMMIT");

  /* Second table for join tests */
  exec(db,
    "CREATE TABLE sbtest2("
    "  id INTEGER PRIMARY KEY,"
    "  k INTEGER NOT NULL DEFAULT 0,"
    "  c TEXT NOT NULL DEFAULT '',"
    "  pad TEXT NOT NULL DEFAULT ''"
    ");"
    "CREATE INDEX k_idx2 ON sbtest2(k);"
  );
  exec(db, "BEGIN");
  for(i=1; i<=1000; i++){
    rng_str(c, 120);
    rng_str(pad, 60);
    execf(db, "INSERT INTO sbtest2 VALUES(%d,%d,'%s','%s')", i, rng_int(1,ROWS), c, pad);
  }
  exec(db, "COMMIT");

  /* Types table */
  exec(db,
    "CREATE TABLE sbtest_types("
    "  id INTEGER PRIMARY KEY,"
    "  ival INTEGER,"
    "  rval REAL,"
    "  tval TEXT"
    ");"
  );
  exec(db, "BEGIN");
  for(i=1; i<=1000; i++){
    rng_str(c, 50);
    execf(db, "INSERT INTO sbtest_types VALUES(%d,%d,%f,'%s')",
      i, rng_int(-1000000,1000000), (double)rng_int(-1000000,1000000)/100.0, c);
  }
  exec(db, "COMMIT");
}

/* ---- Individual benchmarks ---- */

static void bench_bulk_insert(sqlite3 *db){
  /* Already done in prepare — re-time it with a fresh table */
  int i;
  char c[121], pad[61];
  double t0;

  exec(db, "CREATE TABLE sbtest_bulk(id INTEGER PRIMARY KEY, k INTEGER, c TEXT, pad TEXT)");
  t0 = now_ms();
  exec(db, "BEGIN");
  for(i=1; i<=ROWS; i++){
    rng_str(c, 120);
    rng_str(pad, 60);
    execf(db, "INSERT INTO sbtest_bulk VALUES(%d,%d,'%s','%s')", i, rng_int(1,ROWS), c, pad);
  }
  exec(db, "COMMIT");
  record("oltp_bulk_insert", now_ms()-t0);
  exec(db, "DROP TABLE sbtest_bulk");
}

static void bench_point_select(sqlite3 *db){
  int i;
  sqlite3_stmt *stmt;
  double t0;

  sqlite3_prepare_v2(db, "SELECT c FROM sbtest1 WHERE id=?", -1, &stmt, 0);
  t0 = now_ms();
  for(i=0; i<10000; i++){
    sqlite3_bind_int(stmt, 1, rng_int(1,ROWS));
    sqlite3_step(stmt);
    sqlite3_reset(stmt);
  }
  record("oltp_point_select", now_ms()-t0);
  sqlite3_finalize(stmt);
}

static void bench_range_select(sqlite3 *db){
  int i, s;
  sqlite3_stmt *stmt;
  double t0;

  sqlite3_prepare_v2(db, "SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ?", -1, &stmt, 0);
  t0 = now_ms();
  for(i=0; i<1000; i++){
    s = rng_int(1, ROWS-100);
    sqlite3_bind_int(stmt, 1, s);
    sqlite3_bind_int(stmt, 2, s+99);
    while(sqlite3_step(stmt)==SQLITE_ROW){}
    sqlite3_reset(stmt);
  }
  record("oltp_range_select", now_ms()-t0);
  sqlite3_finalize(stmt);
}

static void bench_sum_range(sqlite3 *db){
  int i, s;
  sqlite3_stmt *stmt;
  double t0;

  sqlite3_prepare_v2(db, "SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN ? AND ?", -1, &stmt, 0);
  t0 = now_ms();
  for(i=0; i<1000; i++){
    s = rng_int(1, ROWS-100);
    sqlite3_bind_int(stmt, 1, s);
    sqlite3_bind_int(stmt, 2, s+99);
    sqlite3_step(stmt);
    sqlite3_reset(stmt);
  }
  record("oltp_sum_range", now_ms()-t0);
  sqlite3_finalize(stmt);
}

static void bench_order_range(sqlite3 *db){
  int i, s;
  sqlite3_stmt *stmt;
  double t0;

  sqlite3_prepare_v2(db, "SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c", -1, &stmt, 0);
  t0 = now_ms();
  for(i=0; i<100; i++){
    s = rng_int(1, ROWS-100);
    sqlite3_bind_int(stmt, 1, s);
    sqlite3_bind_int(stmt, 2, s+99);
    while(sqlite3_step(stmt)==SQLITE_ROW){}
    sqlite3_reset(stmt);
  }
  record("oltp_order_range", now_ms()-t0);
  sqlite3_finalize(stmt);
}

static void bench_distinct_range(sqlite3 *db){
  int i, s;
  sqlite3_stmt *stmt;
  double t0;

  sqlite3_prepare_v2(db, "SELECT DISTINCT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c", -1, &stmt, 0);
  t0 = now_ms();
  for(i=0; i<100; i++){
    s = rng_int(1, ROWS-100);
    sqlite3_bind_int(stmt, 1, s);
    sqlite3_bind_int(stmt, 2, s+99);
    while(sqlite3_step(stmt)==SQLITE_ROW){}
    sqlite3_reset(stmt);
  }
  record("oltp_distinct_range", now_ms()-t0);
  sqlite3_finalize(stmt);
}

static void bench_index_scan(sqlite3 *db){
  int i;
  sqlite3_stmt *stmt;
  double t0;

  sqlite3_prepare_v2(db, "SELECT id, c FROM sbtest1 WHERE k=?", -1, &stmt, 0);
  t0 = now_ms();
  for(i=0; i<1000; i++){
    sqlite3_bind_int(stmt, 1, rng_int(1,ROWS));
    while(sqlite3_step(stmt)==SQLITE_ROW){}
    sqlite3_reset(stmt);
  }
  record("oltp_index_scan", now_ms()-t0);
  sqlite3_finalize(stmt);
}

static void bench_update_index(sqlite3 *db){
  int i;
  sqlite3_stmt *stmt;
  double t0;

  sqlite3_prepare_v2(db, "UPDATE sbtest1 SET k=? WHERE id=?", -1, &stmt, 0);
  t0 = now_ms();
  exec(db, "BEGIN");
  for(i=0; i<10000; i++){
    sqlite3_bind_int(stmt, 1, rng_int(1,ROWS));
    sqlite3_bind_int(stmt, 2, rng_int(1,ROWS));
    sqlite3_step(stmt);
    sqlite3_reset(stmt);
  }
  exec(db, "COMMIT");
  record("oltp_update_index", now_ms()-t0);
  sqlite3_finalize(stmt);
}

static void bench_update_non_index(sqlite3 *db){
  int i;
  char c[121];
  sqlite3_stmt *stmt;
  double t0;

  sqlite3_prepare_v2(db, "UPDATE sbtest1 SET c=? WHERE id=?", -1, &stmt, 0);
  t0 = now_ms();
  exec(db, "BEGIN");
  for(i=0; i<10000; i++){
    rng_str(c, 120);
    sqlite3_bind_text(stmt, 1, c, 120, SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt, 2, rng_int(1,ROWS));
    sqlite3_step(stmt);
    sqlite3_reset(stmt);
  }
  exec(db, "COMMIT");
  record("oltp_update_non_index", now_ms()-t0);
  sqlite3_finalize(stmt);
}

static void bench_delete_insert(sqlite3 *db){
  int i, id;
  char c[121], pad[61];
  sqlite3_stmt *del_stmt, *ins_stmt;
  double t0;

  sqlite3_prepare_v2(db, "DELETE FROM sbtest1 WHERE id=?", -1, &del_stmt, 0);
  sqlite3_prepare_v2(db, "INSERT OR REPLACE INTO sbtest1 VALUES(?,?,?,?)", -1, &ins_stmt, 0);
  t0 = now_ms();
  exec(db, "BEGIN");
  for(i=0; i<5000; i++){
    id = rng_int(1, ROWS);
    sqlite3_bind_int(del_stmt, 1, id);
    sqlite3_step(del_stmt);
    sqlite3_reset(del_stmt);
    rng_str(c, 120);
    rng_str(pad, 60);
    sqlite3_bind_int(ins_stmt, 1, id);
    sqlite3_bind_int(ins_stmt, 2, rng_int(1,ROWS));
    sqlite3_bind_text(ins_stmt, 3, c, 120, SQLITE_TRANSIENT);
    sqlite3_bind_text(ins_stmt, 4, pad, 60, SQLITE_TRANSIENT);
    sqlite3_step(ins_stmt);
    sqlite3_reset(ins_stmt);
  }
  exec(db, "COMMIT");
  record("oltp_delete_insert", now_ms()-t0);
  sqlite3_finalize(del_stmt);
  sqlite3_finalize(ins_stmt);
}

static void bench_oltp_insert(sqlite3 *db){
  int i;
  char c[121], pad[61];
  sqlite3_stmt *stmt;
  double t0;

  sqlite3_prepare_v2(db, "INSERT OR REPLACE INTO sbtest1 VALUES(?,?,?,?)", -1, &stmt, 0);
  t0 = now_ms();
  exec(db, "BEGIN");
  for(i=0; i<5000; i++){
    rng_str(c, 120);
    rng_str(pad, 60);
    sqlite3_bind_int(stmt, 1, rng_int(1,ROWS+5000));
    sqlite3_bind_int(stmt, 2, rng_int(1,ROWS));
    sqlite3_bind_text(stmt, 3, c, 120, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 4, pad, 60, SQLITE_TRANSIENT);
    sqlite3_step(stmt);
    sqlite3_reset(stmt);
  }
  exec(db, "COMMIT");
  record("oltp_insert", now_ms()-t0);
  sqlite3_finalize(stmt);
}

static void bench_write_only(sqlite3 *db){
  int i, id;
  char c[121], pad[61];
  sqlite3_stmt *upd_k, *upd_c, *del, *ins;
  double t0;

  sqlite3_prepare_v2(db, "UPDATE sbtest1 SET k=? WHERE id=?", -1, &upd_k, 0);
  sqlite3_prepare_v2(db, "UPDATE sbtest1 SET c=? WHERE id=?", -1, &upd_c, 0);
  sqlite3_prepare_v2(db, "DELETE FROM sbtest1 WHERE id=?", -1, &del, 0);
  sqlite3_prepare_v2(db, "INSERT OR REPLACE INTO sbtest1 VALUES(?,?,?,?)", -1, &ins, 0);
  t0 = now_ms();
  exec(db, "BEGIN");
  for(i=0; i<1000; i++){
    sqlite3_bind_int(upd_k, 1, rng_int(1,ROWS)); sqlite3_bind_int(upd_k, 2, rng_int(1,ROWS));
    sqlite3_step(upd_k); sqlite3_reset(upd_k);
    rng_str(c, 120);
    sqlite3_bind_text(upd_c, 1, c, 120, SQLITE_TRANSIENT); sqlite3_bind_int(upd_c, 2, rng_int(1,ROWS));
    sqlite3_step(upd_c); sqlite3_reset(upd_c);
    id = rng_int(1,ROWS);
    sqlite3_bind_int(del, 1, id); sqlite3_step(del); sqlite3_reset(del);
    rng_str(c, 120); rng_str(pad, 60);
    sqlite3_bind_int(ins, 1, id); sqlite3_bind_int(ins, 2, rng_int(1,ROWS));
    sqlite3_bind_text(ins, 3, c, 120, SQLITE_TRANSIENT);
    sqlite3_bind_text(ins, 4, pad, 60, SQLITE_TRANSIENT);
    sqlite3_step(ins); sqlite3_reset(ins);
  }
  exec(db, "COMMIT");
  record("oltp_write_only", now_ms()-t0);
  sqlite3_finalize(upd_k); sqlite3_finalize(upd_c);
  sqlite3_finalize(del); sqlite3_finalize(ins);
}

static void bench_select_random_points(sqlite3 *db){
  int i, j;
  sqlite3_stmt *stmt;
  double t0;
  char sql[256];

  t0 = now_ms();
  for(i=0; i<1000; i++){
    int pts[10];
    for(j=0;j<10;j++) pts[j] = rng_int(1,ROWS);
    snprintf(sql, sizeof(sql),
      "SELECT id,k,c,pad FROM sbtest1 WHERE id IN(%d,%d,%d,%d,%d,%d,%d,%d,%d,%d)",
      pts[0],pts[1],pts[2],pts[3],pts[4],pts[5],pts[6],pts[7],pts[8],pts[9]);
    sqlite3_prepare_v2(db, sql, -1, &stmt, 0);
    while(sqlite3_step(stmt)==SQLITE_ROW){}
    sqlite3_finalize(stmt);
  }
  record("select_random_points", now_ms()-t0);
}

static void bench_select_random_ranges(sqlite3 *db){
  int i, s;
  sqlite3_stmt *stmt;
  double t0;

  sqlite3_prepare_v2(db, "SELECT count(k) FROM sbtest1 WHERE id BETWEEN ? AND ?", -1, &stmt, 0);
  t0 = now_ms();
  for(i=0; i<1000; i++){
    s = rng_int(1, ROWS-10);
    sqlite3_bind_int(stmt, 1, s); sqlite3_bind_int(stmt, 2, s+9);
    sqlite3_step(stmt); sqlite3_reset(stmt);
  }
  record("select_random_ranges", now_ms()-t0);
  sqlite3_finalize(stmt);
}

static void bench_covering_index_scan(sqlite3 *db){
  int i, s;
  sqlite3_stmt *stmt;
  double t0;

  sqlite3_prepare_v2(db, "SELECT count(k) FROM sbtest1 WHERE k BETWEEN ? AND ?", -1, &stmt, 0);
  t0 = now_ms();
  for(i=0; i<1000; i++){
    s = rng_int(1, ROWS-100);
    sqlite3_bind_int(stmt, 1, s); sqlite3_bind_int(stmt, 2, s+99);
    sqlite3_step(stmt); sqlite3_reset(stmt);
  }
  record("covering_index_scan", now_ms()-t0);
  sqlite3_finalize(stmt);
}

static void bench_groupby_scan(sqlite3 *db){
  int i, s;
  sqlite3_stmt *stmt;
  double t0;

  sqlite3_prepare_v2(db,
    "SELECT k, count(*) FROM sbtest1 WHERE id BETWEEN ? AND ? GROUP BY k ORDER BY k",
    -1, &stmt, 0);
  t0 = now_ms();
  for(i=0; i<100; i++){
    s = rng_int(1, ROWS-1000);
    sqlite3_bind_int(stmt, 1, s); sqlite3_bind_int(stmt, 2, s+999);
    while(sqlite3_step(stmt)==SQLITE_ROW){}
    sqlite3_reset(stmt);
  }
  record("groupby_scan", now_ms()-t0);
  sqlite3_finalize(stmt);
}

static void bench_index_join(sqlite3 *db){
  int i, s;
  sqlite3_stmt *stmt;
  double t0;

  sqlite3_prepare_v2(db,
    "SELECT a.id, b.id FROM sbtest1 a JOIN sbtest2 b ON a.k=b.k WHERE a.id BETWEEN ? AND ?",
    -1, &stmt, 0);
  t0 = now_ms();
  for(i=0; i<500; i++){
    s = rng_int(1, ROWS-10);
    sqlite3_bind_int(stmt, 1, s); sqlite3_bind_int(stmt, 2, s+9);
    while(sqlite3_step(stmt)==SQLITE_ROW){}
    sqlite3_reset(stmt);
  }
  record("index_join", now_ms()-t0);
  sqlite3_finalize(stmt);
}

static void bench_index_join_scan(sqlite3 *db){
  int i, s;
  sqlite3_stmt *stmt;
  double t0;

  sqlite3_prepare_v2(db,
    "SELECT count(*) FROM sbtest1 a JOIN sbtest2 b ON a.k=b.k WHERE b.id BETWEEN ? AND ?",
    -1, &stmt, 0);
  t0 = now_ms();
  for(i=0; i<100; i++){
    s = rng_int(1, 950);
    sqlite3_bind_int(stmt, 1, s); sqlite3_bind_int(stmt, 2, s+49);
    sqlite3_step(stmt); sqlite3_reset(stmt);
  }
  record("index_join_scan", now_ms()-t0);
  sqlite3_finalize(stmt);
}

static void bench_types_delete_insert(sqlite3 *db){
  int i, id;
  char t[51];
  sqlite3_stmt *del, *ins;
  double t0;

  sqlite3_prepare_v2(db, "DELETE FROM sbtest_types WHERE id=?", -1, &del, 0);
  sqlite3_prepare_v2(db, "INSERT OR REPLACE INTO sbtest_types VALUES(?,?,?,?)", -1, &ins, 0);
  t0 = now_ms();
  exec(db, "BEGIN");
  for(i=0; i<5000; i++){
    id = rng_int(1, 1000);
    sqlite3_bind_int(del, 1, id); sqlite3_step(del); sqlite3_reset(del);
    rng_str(t, 50);
    sqlite3_bind_int(ins, 1, id);
    sqlite3_bind_int(ins, 2, rng_int(-1000000,1000000));
    sqlite3_bind_double(ins, 3, (double)rng_int(-1000000,1000000)/100.0);
    sqlite3_bind_text(ins, 4, t, 50, SQLITE_TRANSIENT);
    sqlite3_step(ins); sqlite3_reset(ins);
  }
  exec(db, "COMMIT");
  record("types_delete_insert", now_ms()-t0);
  sqlite3_finalize(del); sqlite3_finalize(ins);
}

static void bench_types_table_scan(sqlite3 *db){
  int i;
  sqlite3_stmt *stmt;
  double t0;
  char pat[10];

  t0 = now_ms();
  for(i=0; i<100; i++){
    rng_str(pat, 3);
    char sql[128];
    snprintf(sql, sizeof(sql), "SELECT count(*) FROM sbtest_types WHERE tval LIKE '%%%s%%'", pat);
    sqlite3_prepare_v2(db, sql, -1, &stmt, 0);
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);
  }
  record("types_table_scan", now_ms()-t0);
}

static void bench_table_scan(sqlite3 *db){
  sqlite3_stmt *stmt;
  double t0;

  t0 = now_ms();
  sqlite3_prepare_v2(db, "SELECT count(*) FROM sbtest1 WHERE c LIKE '%abc%'", -1, &stmt, 0);
  sqlite3_step(stmt);
  sqlite3_finalize(stmt);
  record("table_scan", now_ms()-t0);
}

static void bench_read_only(sqlite3 *db){
  /* Sysbench oltp_read_only: 10 point selects + 1 range + 1 sum + 1 order + 1 distinct per txn */
  int i, j, s;
  sqlite3_stmt *ps, *rs, *ss, *os, *ds;
  double t0;

  sqlite3_prepare_v2(db, "SELECT c FROM sbtest1 WHERE id=?", -1, &ps, 0);
  sqlite3_prepare_v2(db, "SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ?", -1, &rs, 0);
  sqlite3_prepare_v2(db, "SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN ? AND ?", -1, &ss, 0);
  sqlite3_prepare_v2(db, "SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c", -1, &os, 0);
  sqlite3_prepare_v2(db, "SELECT DISTINCT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c", -1, &ds, 0);

  t0 = now_ms();
  for(i=0; i<1000; i++){
    for(j=0; j<10; j++){
      sqlite3_bind_int(ps, 1, rng_int(1,ROWS)); sqlite3_step(ps); sqlite3_reset(ps);
    }
    s=rng_int(1,ROWS-100);
    sqlite3_bind_int(rs,1,s); sqlite3_bind_int(rs,2,s+99);
    while(sqlite3_step(rs)==SQLITE_ROW){} sqlite3_reset(rs);
    s=rng_int(1,ROWS-100);
    sqlite3_bind_int(ss,1,s); sqlite3_bind_int(ss,2,s+99);
    sqlite3_step(ss); sqlite3_reset(ss);
    s=rng_int(1,ROWS-100);
    sqlite3_bind_int(os,1,s); sqlite3_bind_int(os,2,s+99);
    while(sqlite3_step(os)==SQLITE_ROW){} sqlite3_reset(os);
    s=rng_int(1,ROWS-100);
    sqlite3_bind_int(ds,1,s); sqlite3_bind_int(ds,2,s+99);
    while(sqlite3_step(ds)==SQLITE_ROW){} sqlite3_reset(ds);
  }
  record("oltp_read_only", now_ms()-t0);
  sqlite3_finalize(ps); sqlite3_finalize(rs); sqlite3_finalize(ss);
  sqlite3_finalize(os); sqlite3_finalize(ds);
}

static void bench_read_write(sqlite3 *db){
  /* oltp_read_write: read_only + 2 updates + 1 delete/insert per txn */
  int i, j, s, id;
  char c[121], pad[61];
  sqlite3_stmt *ps, *rs, *ss, *uk, *uc, *del, *ins;
  double t0;

  sqlite3_prepare_v2(db, "SELECT c FROM sbtest1 WHERE id=?", -1, &ps, 0);
  sqlite3_prepare_v2(db, "SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ?", -1, &rs, 0);
  sqlite3_prepare_v2(db, "SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN ? AND ?", -1, &ss, 0);
  sqlite3_prepare_v2(db, "UPDATE sbtest1 SET k=? WHERE id=?", -1, &uk, 0);
  sqlite3_prepare_v2(db, "UPDATE sbtest1 SET c=? WHERE id=?", -1, &uc, 0);
  sqlite3_prepare_v2(db, "DELETE FROM sbtest1 WHERE id=?", -1, &del, 0);
  sqlite3_prepare_v2(db, "INSERT OR REPLACE INTO sbtest1 VALUES(?,?,?,?)", -1, &ins, 0);

  t0 = now_ms();
  exec(db, "BEGIN");
  for(i=0; i<1000; i++){
    for(j=0; j<10; j++){
      sqlite3_bind_int(ps, 1, rng_int(1,ROWS)); sqlite3_step(ps); sqlite3_reset(ps);
    }
    s=rng_int(1,ROWS-100);
    sqlite3_bind_int(rs,1,s); sqlite3_bind_int(rs,2,s+99);
    while(sqlite3_step(rs)==SQLITE_ROW){} sqlite3_reset(rs);
    s=rng_int(1,ROWS-100);
    sqlite3_bind_int(ss,1,s); sqlite3_bind_int(ss,2,s+99);
    sqlite3_step(ss); sqlite3_reset(ss);
    sqlite3_bind_int(uk,1,rng_int(1,ROWS)); sqlite3_bind_int(uk,2,rng_int(1,ROWS));
    sqlite3_step(uk); sqlite3_reset(uk);
    rng_str(c,120);
    sqlite3_bind_text(uc,1,c,120,SQLITE_TRANSIENT); sqlite3_bind_int(uc,2,rng_int(1,ROWS));
    sqlite3_step(uc); sqlite3_reset(uc);
    id=rng_int(1,ROWS);
    sqlite3_bind_int(del,1,id); sqlite3_step(del); sqlite3_reset(del);
    rng_str(c,120); rng_str(pad,60);
    sqlite3_bind_int(ins,1,id); sqlite3_bind_int(ins,2,rng_int(1,ROWS));
    sqlite3_bind_text(ins,3,c,120,SQLITE_TRANSIENT);
    sqlite3_bind_text(ins,4,pad,60,SQLITE_TRANSIENT);
    sqlite3_step(ins); sqlite3_reset(ins);
  }
  exec(db, "COMMIT");
  record("oltp_read_write", now_ms()-t0);
  sqlite3_finalize(ps); sqlite3_finalize(rs); sqlite3_finalize(ss);
  sqlite3_finalize(uk); sqlite3_finalize(uc);
  sqlite3_finalize(del); sqlite3_finalize(ins);
}

/* ---- Main ---- */
static void run_benchmarks(void){
  sqlite3 *db;
  int i;
  double t0;

  remove("/tmp/sysbench_compare.db");
  sqlite3_open("/tmp/sysbench_compare.db", &db);

  /* Prepare */
  rng_seed(SEED);
  t0 = now_ms();
  prepare(db);
  /* Don't count prepare time — it's the bulk_insert test */

  /* Run all benchmarks with fresh RNG seed for each */
  rng_seed(SEED + 1); bench_bulk_insert(db);
  rng_seed(SEED + 2); bench_point_select(db);
  rng_seed(SEED + 3); bench_range_select(db);
  rng_seed(SEED + 4); bench_sum_range(db);
  rng_seed(SEED + 5); bench_order_range(db);
  rng_seed(SEED + 6); bench_distinct_range(db);
  rng_seed(SEED + 7); bench_index_scan(db);
  rng_seed(SEED + 8); bench_update_index(db);
  rng_seed(SEED + 9); bench_update_non_index(db);
  rng_seed(SEED + 10); bench_delete_insert(db);
  rng_seed(SEED + 11); bench_oltp_insert(db);
  rng_seed(SEED + 12); bench_write_only(db);
  rng_seed(SEED + 13); bench_select_random_points(db);
  rng_seed(SEED + 14); bench_select_random_ranges(db);
  rng_seed(SEED + 15); bench_covering_index_scan(db);
  rng_seed(SEED + 16); bench_groupby_scan(db);
  rng_seed(SEED + 17); bench_index_join(db);
  rng_seed(SEED + 18); bench_index_join_scan(db);
  rng_seed(SEED + 19); bench_types_delete_insert(db);
  rng_seed(SEED + 20); bench_types_table_scan(db);
  rng_seed(SEED + 21); bench_table_scan(db);
  rng_seed(SEED + 22); bench_read_only(db);
  rng_seed(SEED + 23); bench_read_write(db);

  sqlite3_close(db);
  remove("/tmp/sysbench_compare.db");

  /* Output JSON */
  printf("{\"engine\":\"%s\",\"rows\":%d,\"results\":{", ENGINE_NAME, ROWS);
  for(i=0; i<nResults; i++){
    if(i>0) printf(",");
    printf("\"%s\":%.1f", results[i].name, results[i].ms);
  }
  printf("}}\n");
}

#include <pthread.h>

static void *bench_thread(void *arg){
  (void)arg;
  run_benchmarks();
  return NULL;
}

int main(void){
  /* Run on a thread with 8MB stack to avoid stack overflow in prolly tree's
  ** serializeCatalog -> sqlite3Prepare recursive call chain */
  pthread_t th;
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setstacksize(&attr, 64 * 1024 * 1024);
  pthread_create(&th, &attr, bench_thread, NULL);
  pthread_join(th, NULL);
  pthread_attr_destroy(&attr);
  return 0;
}
