/*
** Cross-connection AUTOINCREMENT sequence reload tests.
**
** ChunkStore keeps the AUTOINCREMENT counter for each table in
** ChunkRefs.aSequences (see vdbe.c notes around OP_NewRowid). When one
** process commits a sequence bump and another process refreshes via
** chunkStoreRefreshIfChanged -> csReloadFromDisk -> csAdoptOpenedStoreState,
** the adopted state must move the sequences array from the freshly opened
** ChunkStore to the live one. Historically that move was missing, so:
**
**   - the live store kept its old (now-freed) aSequences pointer (UAF),
**   - the freshly read sequences from disk were dropped on the floor.
**
** These tests reproduce the resulting AUTOINCREMENT regression at the
** SQL surface, with no internal-API peeking. They fork to make sure each
** connection has an independent ChunkStore.
**
** Build alongside the other adversarial C tests:
**   $(T.link) -I. -I$(TOP)/src -o sequence_reload_test \
**     test/sequence_reload_test.c libdoltlite.a -lz -lpthread -lm
*/
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

/*
** Test 1: AUTOINCREMENT sequence bumped in another process is visible
** after refresh, and the next INSERT here picks a non-colliding rowid.
**
** Buggy adopt: B's aSequences is the dangling old buffer, nSequences
** is stale (=1). B's next INSERT reads iSeq=1 (or garbage), picks id=2,
** which collides with row 2 already on disk -> INSERT fails.
*/
static void test_seq_bump_visible(void){
  const char *path = "/tmp/test_seq_bump.db";
  sqlite3 *b;
  pid_t pid;
  int status;
  long long maxId, newId;
  int rc;

  printf("--- Test 1: AUTOINCREMENT bump in other proc visible after refresh ---\n");
  remove(path);

  /* Parent setup: AUTOINCREMENT table, row id=1, commit. */
  b = open_db(path);
  execSql(b, "CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)");
  execSql(b, "INSERT INTO t(v) VALUES('p1')");
  execSql(b, "SELECT dolt_commit('-A','-m','init')");
  check("seq_initial_max_id_is_1", queryInt(b, "SELECT MAX(id) FROM t")==1);

  /* Child opens a fresh connection, inserts 4 more rows (id 2..5),
  ** commits. The sequence counter on disk advances to 5. */
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

  /* Parent's autocommit SELECT triggers btreeRefreshFromDisk -> reload.
  ** After the fix, the sequences array is moved into cs and the count
  ** reflects the on-disk state. */
  maxId = queryInt(b, "SELECT MAX(id) FROM t");
  check("seq_parent_sees_child_rows", maxId==5);

  /* Now insert another row from the parent. It must pick id=6, not id=2.
  ** A stale sequence counter would re-pick id=2 and collide. */
  rc = execSql(b, "INSERT INTO t(v) VALUES('p6')");
  check("seq_parent_insert_after_refresh_ok", rc==SQLITE_OK);

  newId = queryInt(b, "SELECT id FROM t WHERE v='p6'");
  check("seq_parent_insert_picks_id_6", newId==6);

  sqlite3_close(b);
  remove(path);
}

/*
** Test 2: A sequence created entirely in another process is visible
** after refresh, with the correct counter.
**
** Buggy adopt: tmp's freshly-deserialized aSequences (containing the
** new table's counter) is dropped on the floor when chunkStoreClose(&tmp)
** runs. B's nSequences stays at 0, so AUTOINCREMENT picks id=1 — which
** happens to match the on-disk row written by the child, producing a
** UNIQUE collision.
*/
static void test_new_seq_visible(void){
  const char *path = "/tmp/test_seq_new.db";
  sqlite3 *b;
  pid_t pid;
  int status;
  long long newId;
  int rc;

  printf("--- Test 2: new AUTOINCREMENT table in other proc visible after refresh ---\n");
  remove(path);

  /* Parent setup: a NON-autoincrement table only, so no sequences exist
  ** in the refs blob yet. */
  b = open_db(path);
  execSql(b, "CREATE TABLE other(x INTEGER)");
  execSql(b, "INSERT INTO other VALUES(1)");
  execSql(b, "SELECT dolt_commit('-A','-m','no seq yet')");
  check("newseq_initial_no_sequences",
        queryInt(b, "SELECT COUNT(*) FROM other")==1);

  /* Child creates the AUTOINCREMENT table and inserts rows 1..3. */
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

  /* Parent's SELECT triggers refresh. After fix: parent picks up the new
  ** sequence with iSeq=3. */
  check("newseq_parent_sees_child_rows",
        queryInt(b, "SELECT COUNT(*) FROM t")==3);

  /* Parent insert should pick id=4. Without the fix, the brand-new
  ** sequence from disk is dropped, so AUTOINCREMENT falls back to id=1
  ** and collides with the on-disk row. */
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
