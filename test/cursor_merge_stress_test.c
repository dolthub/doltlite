/*
** cursor_merge_stress_test.c — exercise prolly btree cursor merge of
** committed tree rows with in-txn mutmap edits (TREE/MUT/BOTH paths),
** nested SAVEPOINTs, secondary indexes, and multi-cursor scans.
**
** Built with DOLTLITE_PROLLY_CHECK=1 in CI so mutmap/tree invariants
** are asserted during flush and mutate.
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
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

static void check_int(const char *name, int got, int expected){
  if( got==expected ){
    nPass++;
  }else{
    nFail++;
    fprintf(stderr, "FAIL: %s: expected %d, got %d\n", name, expected, got);
  }
}

static void cleanup_db(const char *path){
  char buf[512];
  remove(path);
  snprintf(buf, sizeof(buf), "%s-wal", path);
  remove(buf);
  snprintf(buf, sizeof(buf), "%s-shm", path);
  remove(buf);
  /* doltlite graph lock sidecar */
  snprintf(buf, sizeof(buf), ".%s-lock", path);
  /* path may be absolute; also try basename-adjacent lock via dirname */
  remove(buf);
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

static int queryInt(sqlite3 *db, const char *sql, int *pOut){
  sqlite3_stmt *stmt = 0;
  int rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);
  if( rc!=SQLITE_OK ) return rc;
  rc = sqlite3_step(stmt);
  if( rc==SQLITE_ROW ){
    *pOut = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);
    return SQLITE_OK;
  }
  sqlite3_finalize(stmt);
  return rc==SQLITE_DONE ? SQLITE_ERROR : rc;
}

/* Model: for each id in [0, n), present[i] and val[i] if present. */
#define MAX_KEYS 4096

typedef struct Model Model;
struct Model {
  int n;
  unsigned char present[MAX_KEYS];
  int val[MAX_KEYS];
};

static void modelInit(Model *m, int n){
  int i;
  memset(m, 0, sizeof(*m));
  m->n = n;
  for(i=0; i<n; i++){
    m->present[i] = 1;
    m->val[i] = i * 10;
  }
}

static int modelCount(const Model *m){
  int i, c = 0;
  for(i=0; i<m->n; i++) if( m->present[i] ) c++;
  return c;
}

static int modelSum(const Model *m){
  int i, s = 0;
  for(i=0; i<m->n; i++) if( m->present[i] ) s += m->val[i];
  return s;
}

static int modelMinId(const Model *m){
  int i;
  for(i=0; i<m->n; i++) if( m->present[i] ) return i;
  return -1;
}

static int modelMaxId(const Model *m){
  int i;
  for(i=m->n-1; i>=0; i--) if( m->present[i] ) return i;
  return -1;
}

/* Collect all (id,val) via full table scan ordered by id. */
static int scanTable(sqlite3 *db, int *ids, int *vals, int cap, int *pn){
  sqlite3_stmt *stmt = 0;
  int rc, n = 0;
  rc = sqlite3_prepare_v2(db,
      "SELECT id, val FROM t ORDER BY id", -1, &stmt, 0);
  if( rc!=SQLITE_OK ) return rc;
  while( (rc = sqlite3_step(stmt))==SQLITE_ROW ){
    if( n>=cap ){
      sqlite3_finalize(stmt);
      return SQLITE_ERROR;
    }
    ids[n] = sqlite3_column_int(stmt, 0);
    vals[n] = sqlite3_column_int(stmt, 1);
    n++;
  }
  sqlite3_finalize(stmt);
  *pn = n;
  return rc==SQLITE_DONE ? SQLITE_OK : rc;
}

static int modelMatchesScan(const Model *m, const int *ids, const int *vals, int n){
  int i, expect = 0;
  if( n!=modelCount(m) ) return 0;
  for(i=0; i<m->n; i++){
    if( !m->present[i] ) continue;
    if( ids[expect]!=i || vals[expect]!=m->val[i] ) return 0;
    expect++;
  }
  return expect==n;
}

static int verifyModel(sqlite3 *db, const Model *m, const char *tag){
  int ids[MAX_KEYS], vals[MAX_KEYS], n = 0, cnt = -1, sum = -1;
  int rc;
  char name[128];

  rc = scanTable(db, ids, vals, MAX_KEYS, &n);
  snprintf(name, sizeof(name), "%s_scan_ok", tag);
  check(name, rc==SQLITE_OK);

  snprintf(name, sizeof(name), "%s_scan_matches_model", tag);
  check(name, modelMatchesScan(m, ids, vals, n));

  rc = queryInt(db, "SELECT count(*) FROM t", &cnt);
  snprintf(name, sizeof(name), "%s_count", tag);
  check(name, rc==SQLITE_OK && cnt==modelCount(m));

  rc = queryInt(db, "SELECT ifnull(sum(val),0) FROM t", &sum);
  snprintf(name, sizeof(name), "%s_sum", tag);
  check(name, rc==SQLITE_OK && sum==modelSum(m));

  /* Index path: lookup by val for a present mid key. */
  {
    int mid = -1, i, gotId = -1;
    for(i=0; i<m->n; i++){
      if( m->present[i] ){ mid = i; break; }
    }
    if( mid>=0 ){
      sqlite3_stmt *stmt = 0;
      char sql[128];
      snprintf(sql, sizeof(sql),
               "SELECT id FROM t WHERE val=%d ORDER BY id", m->val[mid]);
      rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);
      if( rc==SQLITE_OK && sqlite3_step(stmt)==SQLITE_ROW ){
        gotId = sqlite3_column_int(stmt, 0);
      }
      sqlite3_finalize(stmt);
      snprintf(name, sizeof(name), "%s_index_lookup", tag);
      check(name, gotId==mid);
    }
  }

  /* Range: ids in the open middle of the key space. */
  if( m->n>=8 ){
    int lo = m->n/4, hi = (3*m->n)/4;
    int expect = 0, i, got = 0;
    sqlite3_stmt *stmt = 0;
    char sql[128];
    for(i=lo; i<hi; i++) if( m->present[i] ) expect++;
    snprintf(sql, sizeof(sql),
             "SELECT count(*) FROM t WHERE id>=%d AND id<%d", lo, hi);
    rc = queryInt(db, sql, &got);
    snprintf(name, sizeof(name), "%s_range_count", tag);
    check(name, rc==SQLITE_OK && got==expect);

    /* Reverse-order range via ORDER BY id DESC. */
    snprintf(sql, sizeof(sql),
             "SELECT id FROM t WHERE id>=%d AND id<%d ORDER BY id DESC",
             lo, hi);
    rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);
    if( rc==SQLITE_OK ){
      int prev = hi + 1;
      int ordered = 1;
      int nSeen = 0;
      while( sqlite3_step(stmt)==SQLITE_ROW ){
        int id = sqlite3_column_int(stmt, 0);
        if( id>=prev ) ordered = 0;
        prev = id;
        nSeen++;
      }
      sqlite3_finalize(stmt);
      snprintf(name, sizeof(name), "%s_range_desc_order", tag);
      check(name, ordered && nSeen==expect);
    }
  }

  return nFail==0;
}

static sqlite3 *openFresh(const char *path){
  sqlite3 *db = 0;
  cleanup_db(path);
  if( sqlite3_open(path, &db)!=SQLITE_OK ) return 0;
  sqlite3_busy_timeout(db, 10000);
  return db;
}

/* Seed committed baseline: n keys with val=i*10, secondary index on val. */
static int seedBaseline(sqlite3 *db, int n){
  int i;
  char sql[128];
  if( execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val INT)")!=SQLITE_OK )
    return 0;
  if( execSql(db, "CREATE INDEX t_val ON t(val)")!=SQLITE_OK ) return 0;
  if( execSql(db, "BEGIN")!=SQLITE_OK ) return 0;
  for(i=0; i<n; i++){
    snprintf(sql, sizeof(sql), "INSERT INTO t VALUES(%d,%d)", i, i*10);
    if( execSql(db, sql)!=SQLITE_OK ) return 0;
  }
  if( execSql(db, "COMMIT")!=SQLITE_OK ) return 0;
  if( execSql(db, "SELECT dolt_commit('-A','-m','seed')")!=SQLITE_OK ) return 0;
  return 1;
}

/*
** Mixed TREE+MUT: committed baseline + in-txn insert/update/delete of
** interleaved keys, verified by full scan / count / sum / index / range.
*/
static void test_mixed_tree_mut_scan(void){
  const char *path = "/tmp/cursor_merge_mixed.db";
  sqlite3 *db;
  Model m;
  int i, n = 200;
  char sql[128];

  printf("--- mixed TREE+MUT scan (n=%d) ---\n", n);
  db = openFresh(path);
  check("mixed_open", db!=0);
  if( !db ) return;
  check("mixed_seed", seedBaseline(db, n));
  modelInit(&m, n);

  check("mixed_begin", execSql(db, "BEGIN")==SQLITE_OK);

  /* Delete every 5th committed row. */
  for(i=0; i<n; i+=5){
    snprintf(sql, sizeof(sql), "DELETE FROM t WHERE id=%d", i);
    check("mixed_del", execSql(db, sql)==SQLITE_OK);
    m.present[i] = 0;
  }
  /* Update every 3rd remaining row. */
  for(i=1; i<n; i+=3){
    if( !m.present[i] ) continue;
    m.val[i] = i*10 + 1;
    snprintf(sql, sizeof(sql), "UPDATE t SET val=%d WHERE id=%d", m.val[i], i);
    check("mixed_upd", execSql(db, sql)==SQLITE_OK);
  }
  /* Insert keys past the committed range (append) and a hole fill. */
  for(i=0; i<40; i++){
    int id = n + i;
    if( id>=MAX_KEYS ) break;
    m.present[id] = 1;
    m.val[id] = id*10 + 7;
    if( id+1>m.n ) m.n = id+1;
    snprintf(sql, sizeof(sql), "INSERT INTO t VALUES(%d,%d)", id, m.val[id]);
    check("mixed_ins_hi", execSql(db, sql)==SQLITE_OK);
  }
  /* Re-insert a deleted mid key with a new value. */
  {
    int id = 10;
    m.present[id] = 1;
    m.val[id] = 9999;
    snprintf(sql, sizeof(sql), "INSERT INTO t VALUES(%d,%d)", id, m.val[id]);
    check("mixed_reins", execSql(db, sql)==SQLITE_OK);
  }

  verifyModel(db, &m, "mixed_pre_commit");
  check("mixed_commit", execSql(db, "COMMIT")==SQLITE_OK);
  verifyModel(db, &m, "mixed_post_commit");

  /* After commit, a fresh connection must see the same snapshot. */
  {
    sqlite3 *db2 = 0;
    check("mixed_reopen", sqlite3_open(path, &db2)==SQLITE_OK);
    if( db2 ){
      sqlite3_busy_timeout(db2, 10000);
      verifyModel(db2, &m, "mixed_fresh_conn");
      sqlite3_close(db2);
    }
  }

  sqlite3_close(db);
  cleanup_db(path);
}

/*
** Nested SAVEPOINTs: mutations under sp1/sp2, scans between, ROLLBACK TO /
** RELEASE, verify model after each boundary.
*/
static void test_nested_savepoint_cursor(void){
  const char *path = "/tmp/cursor_merge_savepoint.db";
  sqlite3 *db;
  Model m;
  int n = 64;
  char sql[128];
  int i;

  printf("--- nested SAVEPOINT + cursor visibility ---\n");
  db = openFresh(path);
  check("sp_open", db!=0);
  if( !db ) return;
  check("sp_seed", seedBaseline(db, n));
  modelInit(&m, n);

  check("sp_begin", execSql(db, "BEGIN")==SQLITE_OK);
  check("sp_sp1", execSql(db, "SAVEPOINT sp1")==SQLITE_OK);

  for(i=0; i<n; i+=4){
    snprintf(sql, sizeof(sql), "DELETE FROM t WHERE id=%d", i);
    check("sp_del", execSql(db, sql)==SQLITE_OK);
    m.present[i] = 0;
  }
  verifyModel(db, &m, "sp_after_sp1_dels");

  check("sp_sp2", execSql(db, "SAVEPOINT sp2")==SQLITE_OK);
  for(i=1; i<n; i+=4){
    if( !m.present[i] ) continue;
    m.val[i] = 1000 + i;
    snprintf(sql, sizeof(sql), "UPDATE t SET val=%d WHERE id=%d", m.val[i], i);
    check("sp_upd", execSql(db, sql)==SQLITE_OK);
  }
  /* Insert under sp2. */
  m.present[n] = 1;
  m.val[n] = 42;
  m.n = n+1;
  check("sp_ins", execSql(db, "INSERT INTO t VALUES(64,42)")==SQLITE_OK);
  verifyModel(db, &m, "sp_after_sp2");

  /* Rollback sp2: undoes updates + insert, keeps sp1 deletes. */
  check("sp_rb_sp2", execSql(db, "ROLLBACK TO sp2")==SQLITE_OK);
  modelInit(&m, n);
  for(i=0; i<n; i+=4) m.present[i] = 0;
  verifyModel(db, &m, "sp_after_rb_sp2");

  /* New work under sp2 again, then RELEASE sp2 into sp1.
  ** id 0 was deleted under sp1 — re-insert it. */
  check("sp_sp2b", execSql(db, "SAVEPOINT sp2")==SQLITE_OK);
  m.present[0] = 1;
  m.val[0] = 222;
  check("sp_reins_0", execSql(db, "INSERT INTO t VALUES(0,222)")==SQLITE_OK);
  verifyModel(db, &m, "sp_after_sp2b");
  check("sp_rel_sp2", execSql(db, "RELEASE sp2")==SQLITE_OK);
  verifyModel(db, &m, "sp_after_rel_sp2");

  /* Rollback entire sp1: back to committed seed. */
  check("sp_rb_sp1", execSql(db, "ROLLBACK TO sp1")==SQLITE_OK);
  modelInit(&m, n);
  verifyModel(db, &m, "sp_after_rb_sp1");

  check("sp_rel_sp1", execSql(db, "RELEASE sp1")==SQLITE_OK);
  check("sp_commit", execSql(db, "COMMIT")==SQLITE_OK);
  verifyModel(db, &m, "sp_final");

  sqlite3_close(db);
  cleanup_db(path);
}

/*
** Multi-cursor: two concurrent prepared scans while a third statement
** mutates. After mutations, re-run full verification (SQLite may invalidate
** open cursors; we assert post-mutation scans match the model).
*/
static void test_multi_cursor_interleaved(void){
  const char *path = "/tmp/cursor_merge_multicur.db";
  sqlite3 *db;
  Model m;
  sqlite3_stmt *pA = 0, *pB = 0;
  int n = 100;
  int i, rc, nA = 0, nB = 0;
  char sql[128];

  printf("--- multi-cursor interleaved with mutations ---\n");
  db = openFresh(path);
  check("mc_open", db!=0);
  if( !db ) return;
  check("mc_seed", seedBaseline(db, n));
  modelInit(&m, n);

  check("mc_begin", execSql(db, "BEGIN")==SQLITE_OK);

  rc = sqlite3_prepare_v2(db, "SELECT id FROM t ORDER BY id", -1, &pA, 0);
  check("mc_prep_a", rc==SQLITE_OK);
  rc = sqlite3_prepare_v2(db, "SELECT id FROM t WHERE id%2=0 ORDER BY id",
                          -1, &pB, 0);
  check("mc_prep_b", rc==SQLITE_OK);

  /* Partial step both cursors over committed baseline. */
  for(i=0; i<10; i++){
    if( sqlite3_step(pA)==SQLITE_ROW ) nA++;
    if( sqlite3_step(pB)==SQLITE_ROW ) nB++;
  }
  check_int("mc_partial_a", nA, 10);
  check_int("mc_partial_b", nB, 10);

  /* Mutate behind them. */
  for(i=0; i<n; i+=7){
    snprintf(sql, sizeof(sql), "DELETE FROM t WHERE id=%d", i);
    check("mc_del", execSql(db, sql)==SQLITE_OK);
    m.present[i] = 0;
  }
  for(i=0; i<20; i++){
    int id = n + i;
    m.present[id] = 1;
    m.val[id] = id;
    if( id+1>m.n ) m.n = id+1;
    snprintf(sql, sizeof(sql), "INSERT INTO t VALUES(%d,%d)", id, id);
    check("mc_ins", execSql(db, sql)==SQLITE_OK);
  }

  /* Finish or reset cursors; either is acceptable if no crash/assert.
  ** Prefer reset + re-query for a definitive model check. */
  sqlite3_finalize(pA);
  sqlite3_finalize(pB);
  pA = pB = 0;

  verifyModel(db, &m, "mc_after_mut");

  check("mc_commit", execSql(db, "COMMIT")==SQLITE_OK);
  verifyModel(db, &m, "mc_post_commit");

  sqlite3_close(db);
  cleanup_db(path);
}

/*
** Burst mutmap pressure: many in-txn inserts/updates with periodic full
** scans so TREE+MUT merge runs after large mutmap growth (still below the
** 64k auto-flush limit, but enough to stress merge ordering).
*/
static void test_burst_mutmap_scans(void){
  const char *path = "/tmp/cursor_merge_burst.db";
  sqlite3 *db;
  Model m;
  int base = 128;
  int burst = 800;
  int i;
  char sql[128];

  printf("--- burst mutmap with periodic scans ---\n");
  db = openFresh(path);
  check("burst_open", db!=0);
  if( !db ) return;
  check("burst_seed", seedBaseline(db, base));
  modelInit(&m, base);

  check("burst_begin", execSql(db, "BEGIN")==SQLITE_OK);

  for(i=0; i<burst; i++){
    int id = base + i;
    int action = i % 5;
    if( id>=MAX_KEYS ) break;

    if( action==0 && m.n>0 ){
      /* delete a random existing low id if present */
      int del = i % base;
      if( m.present[del] ){
        snprintf(sql, sizeof(sql), "DELETE FROM t WHERE id=%d", del);
        check("burst_del", execSql(db, sql)==SQLITE_OK);
        m.present[del] = 0;
      }
    }else if( action==1 && m.present[i%base] ){
      int u = i % base;
      m.val[u] = 50000 + i;
      snprintf(sql, sizeof(sql), "UPDATE t SET val=%d WHERE id=%d", m.val[u], u);
      check("burst_upd", execSql(db, sql)==SQLITE_OK);
    }else{
      m.present[id] = 1;
      m.val[id] = 100000 + i;
      if( id+1>m.n ) m.n = id+1;
      snprintf(sql, sizeof(sql), "INSERT INTO t VALUES(%d,%d)", id, m.val[id]);
      check("burst_ins", execSql(db, sql)==SQLITE_OK);
    }

    if( (i%100)==99 ){
      char tag[32];
      snprintf(tag, sizeof(tag), "burst_%d", i);
      verifyModel(db, &m, tag);
    }
  }

  verifyModel(db, &m, "burst_end");
  check("burst_commit", execSql(db, "COMMIT")==SQLITE_OK);
  verifyModel(db, &m, "burst_committed");

  /* min/max id sanity via ordered cursors */
  {
    int lo = -1, hi = -1;
    queryInt(db, "SELECT id FROM t ORDER BY id ASC LIMIT 1", &lo);
    queryInt(db, "SELECT id FROM t ORDER BY id DESC LIMIT 1", &hi);
    check_int("burst_min_id", lo, modelMinId(&m));
    check_int("burst_max_id", hi, modelMaxId(&m));
  }

  sqlite3_close(db);
  cleanup_db(path);
}

/*
** WITHOUT ROWID + composite PK exercises non-intkey prolly paths through
** the same cursor merge machinery.
*/
static void test_without_rowid_mut_scan(void){
  const char *path = "/tmp/cursor_merge_wor.db";
  sqlite3 *db;
  int i, cnt = -1, rc;
  int ids[64], n = 0;

  printf("--- WITHOUT ROWID composite PK mut scan ---\n");
  db = openFresh(path);
  check("wor_open", db!=0);
  if( !db ) return;

  check("wor_create",
    execSql(db,
      "CREATE TABLE t(a INT NOT NULL, b INT NOT NULL, v INT, "
      "PRIMARY KEY(a,b)) WITHOUT ROWID")==SQLITE_OK);
  check("wor_begin", execSql(db, "BEGIN")==SQLITE_OK);
  for(i=0; i<30; i++){
    char sql[128];
    snprintf(sql, sizeof(sql),
             "INSERT INTO t VALUES(%d,%d,%d)", i/5, i%5, i);
    check("wor_ins", execSql(db, sql)==SQLITE_OK);
  }
  check("wor_commit_seed", execSql(db, "COMMIT")==SQLITE_OK);
  check("wor_dolt_commit",
    execSql(db, "SELECT dolt_commit('-A','-m','wor seed')")==SQLITE_OK);

  check("wor_txn", execSql(db, "BEGIN")==SQLITE_OK);
  check("wor_del",
    execSql(db, "DELETE FROM t WHERE a=1")==SQLITE_OK);
  check("wor_upd",
    execSql(db, "UPDATE t SET v=v+100 WHERE a=2")==SQLITE_OK);
  check("wor_ins2",
    execSql(db, "INSERT INTO t VALUES(9,0,900)")==SQLITE_OK);

  rc = queryInt(db, "SELECT count(*) FROM t", &cnt);
  check("wor_count_ok", rc==SQLITE_OK);
  /* seed 30, delete a=1 → 5 rows (b=0..4), +1 insert → 26 */
  check_int("wor_count", cnt, 26);

  {
    sqlite3_stmt *stmt = 0;
    rc = sqlite3_prepare_v2(db,
        "SELECT a*10+b FROM t ORDER BY a, b", -1, &stmt, 0);
    check("wor_scan_prep", rc==SQLITE_OK);
    n = 0;
    while( sqlite3_step(stmt)==SQLITE_ROW && n<64 ){
      ids[n++] = sqlite3_column_int(stmt, 0);
    }
    sqlite3_finalize(stmt);
    check("wor_scan_sorted", n==26);
    /* first key should be (0,0)=0; a=1 keys absent; a=2 present */
    check("wor_no_a1", n>0 && ids[0]==0);
    {
      int has1 = 0, has2 = 0, has9 = 0, j;
      for(j=0; j<n; j++){
        if( ids[j]/10==1 ) has1 = 1;
        if( ids[j]/10==2 ) has2 = 1;
        if( ids[j]==90 ) has9 = 1;
      }
      check("wor_deleted_a1", !has1);
      check("wor_kept_a2", has2);
      check("wor_inserted_9", has9);
    }
  }

  check("wor_commit", execSql(db, "COMMIT")==SQLITE_OK);
  sqlite3_close(db);
  cleanup_db(path);
}

/*
** Statement rollback (implicit savepoint around each statement when
** autocommit is off and a failed constraint fires) must not corrupt
** the mutmap merge for subsequent scans.
*/
static void test_constraint_fail_then_scan(void){
  const char *path = "/tmp/cursor_merge_constraint.db";
  sqlite3 *db;
  int cnt = -1;

  printf("--- constraint failure then scan ---\n");
  db = openFresh(path);
  check("cf_open", db!=0);
  if( !db ) return;

  check("cf_create",
    execSql(db,
      "CREATE TABLE t(id INTEGER PRIMARY KEY, val INT UNIQUE)")==SQLITE_OK);
  check("cf_seed",
    execSql(db, "INSERT INTO t VALUES(1,10),(2,20),(3,30)")==SQLITE_OK);
  check("cf_commit",
    execSql(db, "SELECT dolt_commit('-A','-m','cf seed')")==SQLITE_OK);

  check("cf_begin", execSql(db, "BEGIN")==SQLITE_OK);
  check("cf_ok_ins", execSql(db, "INSERT INTO t VALUES(4,40)")==SQLITE_OK);
  /* Unique violation should fail and roll back only that statement. */
  check("cf_dup_fails",
    execSql(db, "INSERT INTO t VALUES(5,40)")!=SQLITE_OK);
  check("cf_count",
    queryInt(db, "SELECT count(*) FROM t", &cnt)==SQLITE_OK && cnt==4);
  check("cf_scan_sum",
    queryInt(db, "SELECT sum(val) FROM t", &cnt)==SQLITE_OK && cnt==100);
  check("cf_commit2", execSql(db, "COMMIT")==SQLITE_OK);
  check("cf_final_count",
    queryInt(db, "SELECT count(*) FROM t", &cnt)==SQLITE_OK && cnt==4);

  sqlite3_close(db);
  cleanup_db(path);
}

int main(void){
  printf("=== cursor_merge_stress_test ===\n");

  test_mixed_tree_mut_scan();
  test_nested_savepoint_cursor();
  test_multi_cursor_interleaved();
  test_burst_mutmap_scans();
  test_without_rowid_mut_scan();
  test_constraint_fail_then_scan();

  printf("\nResults: %d passed, %d failed\n", nPass, nFail);
  return nFail ? 1 : 0;
}
