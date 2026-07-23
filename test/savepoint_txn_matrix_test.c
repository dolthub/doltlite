/*
** savepoint_txn_matrix_test.c — nested SAVEPOINT × mutmap × catalog/schema
** × VC ops matrix for prolly_btree_txn savepoint snapshots.
**
** Complements cursor_merge_stress_test (cursor/mutmap scan) and
** doltlite_savepoint.sh (shell VC interactions) with deeper combinatorial
** coverage of:
**   - nested SAVEPOINT + DML mutmap rollback/release
**   - nested SAVEPOINT + DDL (CREATE/ALTER/DROP/INDEX) + catalog reload
**   - prepared-statement validity after DDL under savepoints
**   - dolt_commit / dolt_add mid-savepoint with ROLLBACK TO
**   - multi-table mutmap + schema objects (VIEW)
**   - statement-level failure nested inside explicit savepoints
**
** Prefer building with DOLTLITE_PROLLY_CHECK=1 so flush/mutate asserts fire.
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

static void check_str(const char *name, const char *got, const char *expected){
  if( got && expected && strcmp(got, expected)==0 ){
    nPass++;
  }else{
    nFail++;
    fprintf(stderr, "FAIL: %s: expected '%s', got '%s'\n",
            name, expected ? expected : "(null)", got ? got : "(null)");
  }
}

static void cleanup_db(const char *path){
  char buf[512];
  remove(path);
  snprintf(buf, sizeof(buf), "%s-wal", path);
  remove(buf);
  snprintf(buf, sizeof(buf), "%s-shm", path);
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

/* Like execSql but silent on expected failures. */
static int execSqlQuiet(sqlite3 *db, const char *sql){
  char *err = 0;
  int rc = sqlite3_exec(db, sql, 0, 0, &err);
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

static char sBuf[4096];
static const char *queryText(sqlite3 *db, const char *sql){
  sqlite3_stmt *stmt = 0;
  int rc;
  sBuf[0] = 0;
  rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);
  if( rc!=SQLITE_OK ){
    snprintf(sBuf, sizeof(sBuf), "PREP_ERR:%s", sqlite3_errmsg(db));
    return sBuf;
  }
  rc = sqlite3_step(stmt);
  if( rc==SQLITE_ROW ){
    const char *v = (const char*)sqlite3_column_text(stmt, 0);
    if( v ) snprintf(sBuf, sizeof(sBuf), "%s", v);
  }
  sqlite3_finalize(stmt);
  return sBuf;
}

static int tableExists(sqlite3 *db, const char *zName){
  char sql[256];
  int n = 0;
  snprintf(sql, sizeof(sql),
           "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='%s'",
           zName);
  if( queryInt(db, sql, &n)!=SQLITE_OK ) return 0;
  return n>0;
}

static int indexExists(sqlite3 *db, const char *zName){
  char sql[256];
  int n = 0;
  snprintf(sql, sizeof(sql),
           "SELECT count(*) FROM sqlite_master WHERE type='index' AND name='%s'",
           zName);
  if( queryInt(db, sql, &n)!=SQLITE_OK ) return 0;
  return n>0;
}

static sqlite3 *openFresh(const char *path){
  sqlite3 *db = 0;
  cleanup_db(path);
  if( sqlite3_open(path, &db)!=SQLITE_OK ) return 0;
  sqlite3_busy_timeout(db, 10000);
  return db;
}

static int seedSimple(sqlite3 *db){
  if( execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")!=SQLITE_OK )
    return 0;
  if( execSql(db, "INSERT INTO t VALUES(1,'base')")!=SQLITE_OK ) return 0;
  if( execSql(db, "SELECT dolt_commit('-A','-m','init')")!=SQLITE_OK ) return 0;
  return 1;
}

/* ── 1. Nested SAVEPOINT DML mutmap matrix ───────────────────────────── */

static void test_nested_dml_mutmap(void){
  const char *path = "/tmp/sp_txn_dml.db";
  sqlite3 *db;
  int cnt = -1, sum = -1;

  printf("--- nested SAVEPOINT DML mutmap ---\n");
  db = openFresh(path);
  check("dml_open", db!=0);
  if( !db ) return;
  check("dml_seed", seedSimple(db));

  check("dml_begin", execSql(db, "BEGIN")==SQLITE_OK);
  check("dml_ins0", execSql(db, "INSERT INTO t VALUES(2,'L0')")==SQLITE_OK);
  check("dml_sp1", execSql(db, "SAVEPOINT sp1")==SQLITE_OK);
  check("dml_ins1", execSql(db, "INSERT INTO t VALUES(3,'L1')")==SQLITE_OK);
  check("dml_upd1", execSql(db, "UPDATE t SET v='L1u' WHERE id=1")==SQLITE_OK);
  check("dml_sp2", execSql(db, "SAVEPOINT sp2")==SQLITE_OK);
  check("dml_ins2", execSql(db, "INSERT INTO t VALUES(4,'L2')")==SQLITE_OK);
  check("dml_del2", execSql(db, "DELETE FROM t WHERE id=2")==SQLITE_OK);
  check("dml_sp3", execSql(db, "SAVEPOINT sp3")==SQLITE_OK);
  check("dml_ins3", execSql(db, "INSERT INTO t VALUES(5,'L3')")==SQLITE_OK);

  check("dml_cnt_deep",
    queryInt(db, "SELECT count(*) FROM t", &cnt)==SQLITE_OK && cnt==4);
  /* ids 1,3,4,5 after del 2; wait: 1,3,4,5 = 4; base+ins: 1,2,3,4,5 del2 = 1,3,4,5 yes */

  check("dml_rb3", execSql(db, "ROLLBACK TO sp3")==SQLITE_OK);
  check("dml_cnt_rb3",
    queryInt(db, "SELECT count(*) FROM t", &cnt)==SQLITE_OK && cnt==3);
  check("dml_no5",
    queryInt(db, "SELECT count(*) FROM t WHERE id=5", &cnt)==SQLITE_OK
    && cnt==0);

  check("dml_rb2", execSql(db, "ROLLBACK TO sp2")==SQLITE_OK);
  /* back to after sp1 work: 1(L1u),2(L0),3(L1) */
  check("dml_cnt_rb2",
    queryInt(db, "SELECT count(*) FROM t", &cnt)==SQLITE_OK && cnt==3);
  check_str("dml_v1_rb2", queryText(db, "SELECT v FROM t WHERE id=1"), "L1u");
  check("dml_row2_back",
    queryInt(db, "SELECT count(*) FROM t WHERE id=2", &cnt)==SQLITE_OK
    && cnt==1);

  check("dml_rb1", execSql(db, "ROLLBACK TO sp1")==SQLITE_OK);
  /* only L0 insert remains on top of base: 1 base, 2 L0 */
  check("dml_cnt_rb1",
    queryInt(db, "SELECT count(*) FROM t", &cnt)==SQLITE_OK && cnt==2);
  check_str("dml_v1_rb1", queryText(db, "SELECT v FROM t WHERE id=1"), "base");

  check("dml_rel1", execSql(db, "RELEASE sp1")==SQLITE_OK);
  check("dml_commit", execSql(db, "COMMIT")==SQLITE_OK);
  check("dml_final_cnt",
    queryInt(db, "SELECT count(*) FROM t", &cnt)==SQLITE_OK && cnt==2);

  /* reopen */
  sqlite3_close(db);
  check("dml_reopen", sqlite3_open(path, &db)==SQLITE_OK);
  check("dml_persist_cnt",
    queryInt(db, "SELECT count(*) FROM t", &cnt)==SQLITE_OK && cnt==2);

  sqlite3_close(db);
  cleanup_db(path);
  (void)sum;
}

/* ── 2. Nested SAVEPOINT + DDL catalog matrix ────────────────────────── */

static void test_nested_ddl_catalog(void){
  const char *path = "/tmp/sp_txn_ddl.db";
  sqlite3 *db;
  int cnt = -1;
  sqlite3_stmt *stale = 0;
  int rc;

  printf("--- nested SAVEPOINT + DDL catalog ---\n");
  db = openFresh(path);
  check("ddl_open", db!=0);
  if( !db ) return;
  check("ddl_seed", seedSimple(db));

  check("ddl_begin", execSql(db, "BEGIN")==SQLITE_OK);
  check("ddl_sp1", execSql(db, "SAVEPOINT sp1")==SQLITE_OK);

  check("ddl_create_u",
    execSql(db, "CREATE TABLE u(id INTEGER PRIMARY KEY, x INT)")==SQLITE_OK);
  check("ddl_ins_u", execSql(db, "INSERT INTO u VALUES(1,10)")==SQLITE_OK);
  check("ddl_u_exists", tableExists(db, "u"));

  check("ddl_sp2", execSql(db, "SAVEPOINT sp2")==SQLITE_OK);
  check("ddl_alter_t",
    execSql(db, "ALTER TABLE t ADD COLUMN z INT DEFAULT 0")==SQLITE_OK);
  check("ddl_upd_z",
    execSql(db, "UPDATE t SET z=7 WHERE id=1")==SQLITE_OK);
  check_int("ddl_z_val",
    (queryInt(db, "SELECT z FROM t WHERE id=1", &cnt)==SQLITE_OK) ? cnt : -1,
    7);

  check("ddl_idx",
    execSql(db, "CREATE INDEX t_z ON t(z)")==SQLITE_OK);
  check("ddl_idx_exists", indexExists(db, "t_z"));

  check("ddl_sp3", execSql(db, "SAVEPOINT sp3")==SQLITE_OK);
  check("ddl_create_v",
    execSql(db, "CREATE TABLE v(id INTEGER PRIMARY KEY)")==SQLITE_OK);
  check("ddl_drop_u", execSql(db, "DROP TABLE u")==SQLITE_OK);
  check("ddl_u_gone", !tableExists(db, "u"));
  check("ddl_v_exists", tableExists(db, "v"));

  /* ROLLBACK TO sp3: restore u, drop v */
  check("ddl_rb3", execSql(db, "ROLLBACK TO sp3")==SQLITE_OK);
  check("ddl_u_back", tableExists(db, "u"));
  check("ddl_v_gone", !tableExists(db, "v"));
  check("ddl_u_data",
    queryInt(db, "SELECT x FROM u WHERE id=1", &cnt)==SQLITE_OK && cnt==10);
  check("ddl_z_still",
    queryInt(db, "SELECT z FROM t WHERE id=1", &cnt)==SQLITE_OK && cnt==7);

  /* ROLLBACK TO sp2: undo ALTER and index and keep only u from sp1 */
  check("ddl_rb2", execSql(db, "ROLLBACK TO sp2")==SQLITE_OK);
  check("ddl_u_still", tableExists(db, "u"));
  check("ddl_no_idx", !indexExists(db, "t_z"));
  /* column z should be gone — querying z must fail */
  check("ddl_no_z_col",
    execSqlQuiet(db, "SELECT z FROM t WHERE id=1")!=SQLITE_OK);

  /* Prepare against u, then ROLLBACK TO sp1 dropping u — stmt should not
  ** silently return wrong data after schema restore. */
  check("ddl_prep_u",
    sqlite3_prepare_v2(db, "SELECT x FROM u WHERE id=1", -1, &stale, 0)
    ==SQLITE_OK);
  check("ddl_rb1", execSql(db, "ROLLBACK TO sp1")==SQLITE_OK);
  check("ddl_u_gone2", !tableExists(db, "u"));
  rc = sqlite3_step(stale);
  /* Expect error or done-without-row after schema change; not a false row. */
  check("ddl_stale_no_false_row", rc!=SQLITE_ROW);
  sqlite3_finalize(stale);

  check("ddl_rel1", execSql(db, "RELEASE sp1")==SQLITE_OK);
  check("ddl_commit", execSql(db, "COMMIT")==SQLITE_OK);

  /* Only original t remains. */
  check("ddl_final_t", tableExists(db, "t"));
  check("ddl_final_no_u", !tableExists(db, "u"));
  check("ddl_final_cnt",
    queryInt(db, "SELECT count(*) FROM t", &cnt)==SQLITE_OK && cnt==1);

  sqlite3_close(db);
  cleanup_db(path);
}

/* ── 3. DDL under savepoint then RELEASE (catalog sticks) ────────────── */

static void test_ddl_release_persists(void){
  const char *path = "/tmp/sp_txn_ddl_rel.db";
  sqlite3 *db;
  int cnt = -1;

  printf("--- SAVEPOINT DDL RELEASE persists ---\n");
  db = openFresh(path);
  check("rel_open", db!=0);
  if( !db ) return;
  check("rel_seed", seedSimple(db));

  check("rel_begin", execSql(db, "BEGIN")==SQLITE_OK);
  check("rel_sp1", execSql(db, "SAVEPOINT sp1")==SQLITE_OK);
  check("rel_create",
    execSql(db, "CREATE TABLE w(id INTEGER PRIMARY KEY, n INT)")==SQLITE_OK);
  check("rel_ins", execSql(db, "INSERT INTO w VALUES(1,99)")==SQLITE_OK);
  check("rel_alter",
    execSql(db, "ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'x'")==SQLITE_OK);
  check("rel_release", execSql(db, "RELEASE sp1")==SQLITE_OK);
  check("rel_commit", execSql(db, "COMMIT")==SQLITE_OK);

  check("rel_w", tableExists(db, "w"));
  check("rel_w_data",
    queryInt(db, "SELECT n FROM w", &cnt)==SQLITE_OK && cnt==99);
  check_str("rel_extra",
    queryText(db, "SELECT extra FROM t WHERE id=1"), "x");

  sqlite3_close(db);
  check("rel_reopen", sqlite3_open(path, &db)==SQLITE_OK);
  check("rel_persist_w", tableExists(db, "w"));
  check("rel_persist_extra",
    strcmp(queryText(db, "SELECT extra FROM t WHERE id=1"), "x")==0);

  sqlite3_close(db);
  cleanup_db(path);
}

/* ── 4. dolt_commit mid-savepoint seals VC savepoints ───────────────────
** dolt_commit seals active savepoints (same class as hard-reset). The
** commit is durable in dolt_log; SQL SAVEPOINT handles from before the
** commit are gone. Matches doltlite_savepoint.sh mid-savepoint commit. */

static void test_vc_commit_mid_savepoint(void){
  const char *path = "/tmp/sp_txn_vc_commit.db";
  sqlite3 *db;
  int logs = -1, cnt = -1;
  const char *hash;

  printf("--- dolt_commit mid-SAVEPOINT seals savepoints ---\n");
  db = openFresh(path);
  check("vc_open", db!=0);
  if( !db ) return;
  check("vc_seed", seedSimple(db));

  check("vc_begin", execSql(db, "BEGIN")==SQLITE_OK);
  check("vc_ins_pre", execSql(db, "INSERT INTO t VALUES(2,'pre')")==SQLITE_OK);
  check("vc_sp1", execSql(db, "SAVEPOINT sp1")==SQLITE_OK);
  check("vc_ins_post", execSql(db, "INSERT INTO t VALUES(3,'post')")==SQLITE_OK);

  hash = queryText(db, "SELECT dolt_commit('-A','-m','mid-sp')");
  check("vc_commit_hash", hash && strlen(hash)==40);

  /* Savepoint is sealed/invalidated by dolt_commit. */
  check("vc_rb_sp1_gone",
    execSqlQuiet(db, "ROLLBACK TO sp1")!=SQLITE_OK);

  /* All rows committed into the dolt commit remain visible. */
  check("vc_cnt_after_commit",
    queryInt(db, "SELECT count(*) FROM t", &cnt)==SQLITE_OK && cnt==3);
  check("vc_post_present",
    queryInt(db, "SELECT count(*) FROM t WHERE id=3", &cnt)==SQLITE_OK
    && cnt==1);
  check("vc_log_has_mid",
    queryInt(db,
      "SELECT count(*) FROM dolt_log WHERE message='mid-sp'", &logs)==SQLITE_OK
    && logs>=1);

  /* Outer SQL txn may already be sealed; COMMIT is best-effort. */
  (void)execSqlQuiet(db, "COMMIT");

  check("vc_final_cnt",
    queryInt(db, "SELECT count(*) FROM t", &cnt)==SQLITE_OK && cnt==3);

  /* Reopen: dolt history + data durable. */
  sqlite3_close(db);
  check("vc_reopen", sqlite3_open(path, &db)==SQLITE_OK);
  check("vc_persist_cnt",
    queryInt(db, "SELECT count(*) FROM t", &cnt)==SQLITE_OK && cnt==3);
  check("vc_persist_log",
    queryInt(db,
      "SELECT count(*) FROM dolt_log WHERE message='mid-sp'", &logs)==SQLITE_OK
    && logs>=1);

  sqlite3_close(db);
  cleanup_db(path);
}

/* ── 5. dolt_add mid-savepoint + ROLLBACK ─────────────────────────────── */

static void test_vc_add_mid_savepoint(void){
  const char *path = "/tmp/sp_txn_vc_add.db";
  sqlite3 *db;
  int cnt = -1;
  int staged = -1;

  printf("--- dolt_add mid-SAVEPOINT + ROLLBACK TO ---\n");
  db = openFresh(path);
  check("add_open", db!=0);
  if( !db ) return;
  check("add_seed", seedSimple(db));

  check("add_begin", execSql(db, "BEGIN")==SQLITE_OK);
  check("add_ins", execSql(db, "INSERT INTO t VALUES(2,'dirty')")==SQLITE_OK);
  check("add_sp1", execSql(db, "SAVEPOINT sp1")==SQLITE_OK);
  check("add_ins2", execSql(db, "INSERT INTO t VALUES(3,'staged-ish')")==SQLITE_OK);
  check("add_dolt_add", execSql(db, "SELECT dolt_add('t')")==SQLITE_OK);

  /* Status may show staged; after ROLLBACK TO sp1, working set for id=3 gone. */
  check("add_rb", execSql(db, "ROLLBACK TO sp1")==SQLITE_OK);
  check("add_cnt",
    queryInt(db, "SELECT count(*) FROM t", &cnt)==SQLITE_OK && cnt==2);
  check("add_no3",
    queryInt(db, "SELECT count(*) FROM t WHERE id=3", &cnt)==SQLITE_OK
    && cnt==0);

  check("add_rel", execSql(db, "RELEASE sp1")==SQLITE_OK);
  check("add_commit", execSql(db, "COMMIT")==SQLITE_OK);
  check("add_final",
    queryInt(db, "SELECT count(*) FROM t", &cnt)==SQLITE_OK && cnt==2);

  sqlite3_close(db);
  cleanup_db(path);
  (void)staged;
}

/* ── 6. Multi-table mutmap + VIEW under savepoints ───────────────────── */

static void test_multitable_view_savepoint(void){
  const char *path = "/tmp/sp_txn_multi.db";
  sqlite3 *db;
  int cnt = -1;

  printf("--- multi-table + VIEW under SAVEPOINT ---\n");
  db = openFresh(path);
  check("mt_open", db!=0);
  if( !db ) return;

  check("mt_create",
    execSql(db,
      "CREATE TABLE a(id INTEGER PRIMARY KEY, v INT);"
      "CREATE TABLE b(id INTEGER PRIMARY KEY, a_id INT, v INT);"
      "INSERT INTO a VALUES(1,10);"
      "INSERT INTO b VALUES(1,1,100);"
      "CREATE VIEW ab AS SELECT a.id, a.v AS av, b.v AS bv FROM a JOIN b ON b.a_id=a.id;"
    )==SQLITE_OK);
  check("mt_commit",
    execSql(db, "SELECT dolt_commit('-A','-m','multi init')")==SQLITE_OK);

  check("mt_begin", execSql(db, "BEGIN")==SQLITE_OK);
  check("mt_sp1", execSql(db, "SAVEPOINT sp1")==SQLITE_OK);
  check("mt_ins_a", execSql(db, "INSERT INTO a VALUES(2,20)")==SQLITE_OK);
  check("mt_ins_b", execSql(db, "INSERT INTO b VALUES(2,2,200)")==SQLITE_OK);
  check("mt_view_cnt",
    queryInt(db, "SELECT count(*) FROM ab", &cnt)==SQLITE_OK && cnt==2);

  check("mt_sp2", execSql(db, "SAVEPOINT sp2")==SQLITE_OK);
  check("mt_upd", execSql(db, "UPDATE a SET v=99 WHERE id=1")==SQLITE_OK);
  check("mt_del_b", execSql(db, "DELETE FROM b WHERE id=1")==SQLITE_OK);
  check("mt_view_mid",
    queryInt(db, "SELECT count(*) FROM ab", &cnt)==SQLITE_OK && cnt==1);

  check("mt_rb2", execSql(db, "ROLLBACK TO sp2")==SQLITE_OK);
  check("mt_view_rb2",
    queryInt(db, "SELECT count(*) FROM ab", &cnt)==SQLITE_OK && cnt==2);
  check_int("mt_a1_val",
    (queryInt(db, "SELECT v FROM a WHERE id=1", &cnt)==SQLITE_OK) ? cnt : -1,
    10);

  check("mt_rb1", execSql(db, "ROLLBACK TO sp1")==SQLITE_OK);
  check("mt_view_rb1",
    queryInt(db, "SELECT count(*) FROM ab", &cnt)==SQLITE_OK && cnt==1);
  check("mt_a_cnt",
    queryInt(db, "SELECT count(*) FROM a", &cnt)==SQLITE_OK && cnt==1);

  check("mt_rel", execSql(db, "RELEASE sp1")==SQLITE_OK);
  check("mt_commit2", execSql(db, "COMMIT")==SQLITE_OK);

  sqlite3_close(db);
  cleanup_db(path);
}

/* ── 7. Constraint fail inside nested savepoints ─────────────────────── */

static void test_constraint_in_nested_savepoint(void){
  const char *path = "/tmp/sp_txn_constraint.db";
  sqlite3 *db;
  int cnt = -1;

  printf("--- constraint failure inside nested SAVEPOINT ---\n");
  db = openFresh(path);
  check("cf_open", db!=0);
  if( !db ) return;

  check("cf_create",
    execSql(db,
      "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT UNIQUE);"
      "INSERT INTO t VALUES(1,10),(2,20);"
    )==SQLITE_OK);
  check("cf_seed",
    execSql(db, "SELECT dolt_commit('-A','-m','cf')")==SQLITE_OK);

  check("cf_begin", execSql(db, "BEGIN")==SQLITE_OK);
  check("cf_sp1", execSql(db, "SAVEPOINT sp1")==SQLITE_OK);
  check("cf_ok", execSql(db, "INSERT INTO t VALUES(3,30)")==SQLITE_OK);
  check("cf_sp2", execSql(db, "SAVEPOINT sp2")==SQLITE_OK);
  check("cf_dup", execSqlQuiet(db, "INSERT INTO t VALUES(4,30)")!=SQLITE_OK);
  /* Statement rolled back; sp2 still open; count still 3. */
  check("cf_cnt",
    queryInt(db, "SELECT count(*) FROM t", &cnt)==SQLITE_OK && cnt==3);

  check("cf_ok2", execSql(db, "INSERT INTO t VALUES(4,40)")==SQLITE_OK);
  check("cf_cnt2",
    queryInt(db, "SELECT count(*) FROM t", &cnt)==SQLITE_OK && cnt==4);

  check("cf_rb2", execSql(db, "ROLLBACK TO sp2")==SQLITE_OK);
  check("cf_cnt_rb2",
    queryInt(db, "SELECT count(*) FROM t", &cnt)==SQLITE_OK && cnt==3);

  check("cf_rb1", execSql(db, "ROLLBACK TO sp1")==SQLITE_OK);
  check("cf_cnt_rb1",
    queryInt(db, "SELECT count(*) FROM t", &cnt)==SQLITE_OK && cnt==2);

  check("cf_rel", execSql(db, "RELEASE sp1")==SQLITE_OK);
  check("cf_commit", execSql(db, "COMMIT")==SQLITE_OK);

  sqlite3_close(db);
  cleanup_db(path);
}

/* ── 8. CREATE INDEX + bulk mutmap + nested rollback ─────────────────── */

static void test_index_bulk_mutmap_savepoint(void){
  const char *path = "/tmp/sp_txn_idx_bulk.db";
  sqlite3 *db;
  int i, cnt = -1;
  char sql[128];

  printf("--- bulk mutmap + index under SAVEPOINT ---\n");
  db = openFresh(path);
  check("ib_open", db!=0);
  if( !db ) return;

  check("ib_create",
    execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT)")==SQLITE_OK);
  check("ib_begin_seed", execSql(db, "BEGIN")==SQLITE_OK);
  for(i=0; i<100; i++){
    snprintf(sql, sizeof(sql), "INSERT INTO t VALUES(%d,%d)", i, i*3);
    if( execSql(db, sql)!=SQLITE_OK ){
      check("ib_seed_ins", 0);
      break;
    }
  }
  check("ib_commit_seed", execSql(db, "COMMIT")==SQLITE_OK);
  check("ib_dolt",
    execSql(db, "SELECT dolt_commit('-A','-m','bulk')")==SQLITE_OK);

  check("ib_begin", execSql(db, "BEGIN")==SQLITE_OK);
  check("ib_sp1", execSql(db, "SAVEPOINT sp1")==SQLITE_OK);
  check("ib_idx", execSql(db, "CREATE INDEX t_v ON t(v)")==SQLITE_OK);

  for(i=0; i<100; i+=2){
    snprintf(sql, sizeof(sql), "UPDATE t SET v=%d WHERE id=%d", i*3+1, i);
    check("ib_upd", execSql(db, sql)==SQLITE_OK);
  }
  for(i=100; i<150; i++){
    snprintf(sql, sizeof(sql), "INSERT INTO t VALUES(%d,%d)", i, i);
    check("ib_ins", execSql(db, sql)==SQLITE_OK);
  }
  check("ib_cnt_mid",
    queryInt(db, "SELECT count(*) FROM t", &cnt)==SQLITE_OK && cnt==150);
  check("ib_idx_lookup",
    queryInt(db, "SELECT id FROM t WHERE v=1", &cnt)==SQLITE_OK && cnt==0);

  check("ib_sp2", execSql(db, "SAVEPOINT sp2")==SQLITE_OK);
  for(i=0; i<50; i++){
    snprintf(sql, sizeof(sql), "DELETE FROM t WHERE id=%d", i);
    check("ib_del", execSql(db, sql)==SQLITE_OK);
  }
  check("ib_cnt_del",
    queryInt(db, "SELECT count(*) FROM t", &cnt)==SQLITE_OK && cnt==100);

  check("ib_rb2", execSql(db, "ROLLBACK TO sp2")==SQLITE_OK);
  check("ib_cnt_rb2",
    queryInt(db, "SELECT count(*) FROM t", &cnt)==SQLITE_OK && cnt==150);

  check("ib_rb1", execSql(db, "ROLLBACK TO sp1")==SQLITE_OK);
  check("ib_cnt_rb1",
    queryInt(db, "SELECT count(*) FROM t", &cnt)==SQLITE_OK && cnt==100);
  check("ib_no_idx", !indexExists(db, "t_v"));
  /* original values restored */
  check_int("ib_v0",
    (queryInt(db, "SELECT v FROM t WHERE id=0", &cnt)==SQLITE_OK) ? cnt : -1,
    0);

  check("ib_rel", execSql(db, "RELEASE sp1")==SQLITE_OK);
  check("ib_commit", execSql(db, "COMMIT")==SQLITE_OK);

  sqlite3_close(db);
  cleanup_db(path);
}

/* ── 9. Nested SAVEPOINT + DROP/recreate same name ───────────────────── */

static void test_drop_recreate_under_savepoint(void){
  const char *path = "/tmp/sp_txn_recreate.db";
  sqlite3 *db;
  int cnt = -1;

  printf("--- DROP/recreate table under SAVEPOINT ---\n");
  db = openFresh(path);
  check("dr_open", db!=0);
  if( !db ) return;
  check("dr_seed", seedSimple(db));

  check("dr_begin", execSql(db, "BEGIN")==SQLITE_OK);
  check("dr_sp1", execSql(db, "SAVEPOINT sp1")==SQLITE_OK);
  check("dr_drop", execSql(db, "DROP TABLE t")==SQLITE_OK);
  check("dr_gone", !tableExists(db, "t"));
  check("dr_create",
    execSql(db,
      "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, extra INT DEFAULT 1)"
    )==SQLITE_OK);
  check("dr_ins",
    execSql(db, "INSERT INTO t VALUES(10,'new',5)")==SQLITE_OK);
  check("dr_cnt",
    queryInt(db, "SELECT count(*) FROM t", &cnt)==SQLITE_OK && cnt==1);
  check_int("dr_extra",
    (queryInt(db, "SELECT extra FROM t WHERE id=10", &cnt)==SQLITE_OK)
      ? cnt : -1,
    5);

  check("dr_rb1", execSql(db, "ROLLBACK TO sp1")==SQLITE_OK);
  check("dr_back", tableExists(db, "t"));
  check("dr_old_cnt",
    queryInt(db, "SELECT count(*) FROM t", &cnt)==SQLITE_OK && cnt==1);
  check_str("dr_old_v", queryText(db, "SELECT v FROM t WHERE id=1"), "base");
  check("dr_no_extra_col",
    execSqlQuiet(db, "SELECT extra FROM t")!=SQLITE_OK);

  check("dr_rel", execSql(db, "RELEASE sp1")==SQLITE_OK);
  check("dr_commit", execSql(db, "COMMIT")==SQLITE_OK);

  sqlite3_close(db);
  cleanup_db(path);
}

/* ── 10. Deep nest RELEASE chain then outer ROLLBACK ─────────────────── */

static void test_deep_release_then_outer_rollback(void){
  const char *path = "/tmp/sp_txn_deep_rel.db";
  sqlite3 *db;
  int cnt = -1;

  printf("--- deep RELEASE chain + outer ROLLBACK ---\n");
  db = openFresh(path);
  check("dp_open", db!=0);
  if( !db ) return;
  check("dp_seed", seedSimple(db));

  check("dp_begin", execSql(db, "BEGIN")==SQLITE_OK);
  check("dp_ins0", execSql(db, "INSERT INTO t VALUES(2,'keep-if-commit')")==SQLITE_OK);
  check("dp_sp1", execSql(db, "SAVEPOINT sp1")==SQLITE_OK);
  check("dp_ins1", execSql(db, "INSERT INTO t VALUES(3,'sp1')")==SQLITE_OK);
  check("dp_sp2", execSql(db, "SAVEPOINT sp2")==SQLITE_OK);
  check("dp_ins2", execSql(db, "INSERT INTO t VALUES(4,'sp2')")==SQLITE_OK);
  check("dp_sp3", execSql(db, "SAVEPOINT sp3")==SQLITE_OK);
  check("dp_ins3", execSql(db, "INSERT INTO t VALUES(5,'sp3')")==SQLITE_OK);
  check("dp_rel3", execSql(db, "RELEASE sp3")==SQLITE_OK);
  check("dp_rel2", execSql(db, "RELEASE sp2")==SQLITE_OK);
  check("dp_cnt_mid",
    queryInt(db, "SELECT count(*) FROM t", &cnt)==SQLITE_OK && cnt==5);

  /* ROLLBACK TO sp1 undoes everything from sp1 onward even after releases. */
  check("dp_rb1", execSql(db, "ROLLBACK TO sp1")==SQLITE_OK);
  check("dp_cnt_rb",
    queryInt(db, "SELECT count(*) FROM t", &cnt)==SQLITE_OK && cnt==2);

  check("dp_rel1", execSql(db, "RELEASE sp1")==SQLITE_OK);
  check("dp_rollback", execSql(db, "ROLLBACK")==SQLITE_OK);
  check("dp_final",
    queryInt(db, "SELECT count(*) FROM t", &cnt)==SQLITE_OK && cnt==1);

  sqlite3_close(db);
  cleanup_db(path);
}

/* ── 11. Prepared DML + SAVEPOINT rollback of schema+data ────────────── */

static void test_prepared_dml_across_savepoint_ddl(void){
  const char *path = "/tmp/sp_txn_prep_ddl.db";
  sqlite3 *db;
  sqlite3_stmt *ins = 0;
  int cnt = -1;
  int rc;

  printf("--- prepared DML across SAVEPOINT DDL ---\n");
  db = openFresh(path);
  check("pd_open", db!=0);
  if( !db ) return;
  check("pd_seed", seedSimple(db));

  check("pd_begin", execSql(db, "BEGIN")==SQLITE_OK);
  check("pd_prep",
    sqlite3_prepare_v2(db, "INSERT INTO t(id,v) VALUES(?,?)", -1, &ins, 0)
    ==SQLITE_OK);

  sqlite3_bind_int(ins, 1, 2);
  sqlite3_bind_text(ins, 2, "via-prep", -1, SQLITE_STATIC);
  check("pd_step1", sqlite3_step(ins)==SQLITE_DONE);
  sqlite3_reset(ins);

  check("pd_sp1", execSql(db, "SAVEPOINT sp1")==SQLITE_OK);
  check("pd_alter",
    execSql(db, "ALTER TABLE t ADD COLUMN z INT DEFAULT 0")==SQLITE_OK);

  /* Re-prepare may be required after ALTER; original stmt may be expired. */
  rc = sqlite3_bind_int(ins, 1, 3);
  if( rc==SQLITE_OK ){
    sqlite3_bind_text(ins, 2, "after-alter", -1, SQLITE_STATIC);
    rc = sqlite3_step(ins);
  }
  if( rc!=SQLITE_DONE ){
    sqlite3_finalize(ins);
    ins = 0;
    check("pd_reprep",
      sqlite3_prepare_v2(db, "INSERT INTO t(id,v) VALUES(?,?)", -1, &ins, 0)
      ==SQLITE_OK);
    sqlite3_bind_int(ins, 1, 3);
    sqlite3_bind_text(ins, 2, "after-alter", -1, SQLITE_STATIC);
    check("pd_step2", sqlite3_step(ins)==SQLITE_DONE);
  }else{
    check("pd_step2_same_stmt", 1);
  }
  sqlite3_reset(ins);

  check("pd_rb1", execSql(db, "ROLLBACK TO sp1")==SQLITE_OK);
  check("pd_cnt",
    queryInt(db, "SELECT count(*) FROM t", &cnt)==SQLITE_OK && cnt==2);
  check("pd_no_z", execSqlQuiet(db, "SELECT z FROM t")!=SQLITE_OK);

  check("pd_rel", execSql(db, "RELEASE sp1")==SQLITE_OK);
  check("pd_commit", execSql(db, "COMMIT")==SQLITE_OK);
  if( ins ) sqlite3_finalize(ins);

  sqlite3_close(db);
  cleanup_db(path);
}

int main(void){
  printf("=== savepoint_txn_matrix_test ===\n");

  test_nested_dml_mutmap();
  test_nested_ddl_catalog();
  test_ddl_release_persists();
  test_vc_commit_mid_savepoint();
  test_vc_add_mid_savepoint();
  test_multitable_view_savepoint();
  test_constraint_in_nested_savepoint();
  test_index_bulk_mutmap_savepoint();
  test_drop_recreate_under_savepoint();
  test_deep_release_then_outer_rollback();
  test_prepared_dml_across_savepoint_ddl();

  printf("\nResults: %d passed, %d failed\n", nPass, nFail);
  return nFail ? 1 : 0;
}
