#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#include <signal.h>
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
static void test_readonly_status_does_not_starve_a_writer(
  const char *zPath,
  int nReader
){
  const int nRound = 150;
  pid_t aPid[8];
  int k;
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

  for(k=0; k<nReader; k++){
    aPid[k] = fork();
    if( aPid[k]==0 ){
      /* Reader: a fresh read-only connection per round, like a status
      ** poller. Several overlap so a shared lock is almost always held. */
      for(;;){
        sqlite3 *ro = 0;
        if( sqlite3_open_v2(zPath, &ro, SQLITE_OPEN_READONLY, 0)==SQLITE_OK ){
          (void)scalar(ro, "SELECT count(*) FROM dolt_status");
        }
        sqlite3_close(ro);
      }
      _exit(0);
    }
    check("ro_starve: fork", aPid[k]>0);
    if( aPid[k]<=0 ){
      while( k-->0 ){ kill(aPid[k], SIGKILL); waitpid(aPid[k], &status, 0); }
      return;
    }
  }

  if( sqlite3_open(zPath, &db)!=SQLITE_OK ){
    for(k=0; k<nReader; k++){ kill(aPid[k], SIGKILL); waitpid(aPid[k], &status, 0); }
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

  for(k=0; k<nReader; k++){
    kill(aPid[k], SIGKILL);
    waitpid(aPid[k], &status, 0);
  }

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

static void test_readonly_open_replay(
  const char *zPath, int checkpoint, int replaceFile
){
  sqlite3 *writer = 0;
  char pause[384], ready[400];
  FILE *f;
  pid_t reader;
  int status = 0;
  int i;
  int rc;

  snprintf(pause, sizeof(pause), "%s.reader-pause", zPath);
  snprintf(ready, sizeof(ready), "%s.ready", pause);
  remove(zPath);
  unlink(ready);
  check("ro_open: writer open", sqlite3_open(zPath, &writer)==SQLITE_OK);
  if( !writer ) return;
  if( checkpoint ){
    setenv("DOLTLITE_WAL_CHECKPOINT_CHUNKS", "1", 1);
  }else{
    setenv("DOLTLITE_WAL_CHECKPOINT_CHUNKS", "1000000000", 1);
    setenv("DOLTLITE_WAL_CHECKPOINT_THRESHOLD", "1000000000", 1);
  }
  rc = sqlite3_exec(writer,
      "CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB);"
      "INSERT INTO t VALUES(1, randomblob(8192));"
      "SELECT dolt_commit('-Am','seed');", 0, 0, 0);
  unsetenv("DOLTLITE_WAL_CHECKPOINT_CHUNKS");
  unsetenv("DOLTLITE_WAL_CHECKPOINT_THRESHOLD");
  if( rc==SQLITE_OK && !replaceFile ){
    rc = sqlite3_exec(writer, "BEGIN; INSERT INTO t VALUES(2, randomblob(8192));",
                      0, 0, 0);
  }
  check("ro_open: writer ready", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto done;

  f = fopen(pause, "w");
  check("ro_open: create pause file", f!=0);
  if( !f ) goto done;
  fputs("pause", f);
  fclose(f);
  reader = fork();
  if( reader==0 ){
    sqlite3 *db = 0;
    int moved = 1;
    setenv("DOLTLITE_PAUSE_READONLY_OPEN_REPLAY", pause, 1);
    rc = sqlite3_open_v2(zPath, &db, SQLITE_OPEN_READONLY, 0);
    if( rc==SQLITE_OK && replaceFile ){
      rc = sqlite3_file_control(db, "main", SQLITE_FCNTL_HAS_MOVED, &moved);
      if( rc==SQLITE_OK && moved ) rc = SQLITE_ERROR;
    }
    if( rc==SQLITE_OK && scalar(db, "SELECT sum(length(data)) FROM t")!=16384 ){
      rc = SQLITE_ERROR;
    }
    sqlite3_close(db);
    _exit(rc!=SQLITE_OK);
  }
  check("ro_open: fork", reader>0);
  if( reader<0 ) goto done;
  for(i=0; i<200 && access(ready, F_OK)!=0; i++) sqlite3_sleep(10);
  check("ro_open: reader reached replay", access(ready, F_OK)==0);
  sqlite3_busy_timeout(writer, 10);
  if( replaceFile ){
    rc = sqlite3_exec(writer, "INSERT INTO t VALUES(2, randomblob(8192));"
                              "SELECT dolt_gc();", 0, 0, 0);
    check("ro_open: GC replaced the open reader's file", rc==SQLITE_OK);
  }else{
    rc = sqlite3_exec(writer, "COMMIT", 0, 0, 0);
    check("ro_open: writer committed while reader was in replay", rc==SQLITE_OK);
  }
  unlink(pause);
  check("ro_open: reader observed completed commit after open",
        waitpid(reader, &status, 0)==reader
        && WIFEXITED(status) && WEXITSTATUS(status)==0);

done:
  unlink(pause);
  unlink(ready);
  sqlite3_close(writer);
  remove(zPath);
}

/* A refused reset must not clear cs->readOnly; later VC writers stay refused. */
static int exec_rc(sqlite3 *db, const char *zSql){
  return sqlite3_exec(db, zSql, 0, 0, 0);
}

static void test_readonly_reset_does_not_unlock_writers(const char *zPath){
  sqlite3 *rw = 0, *ro = 0;
  char *zBranches = 0, *zTags = 0, *zMsg = 0;
  sqlite3_stmt *p = 0;

  remove(zPath);
  check("ro_reset: seed open", sqlite3_open(zPath, &rw)==SQLITE_OK);
  if( !rw ) return;
  check("ro_reset: seed", exec_rc(rw,
      "CREATE TABLE t(a INT PRIMARY KEY, b TEXT);"
      "INSERT INTO t VALUES(1,'x');"
      "SELECT dolt_commit('-A','-m','c1');"
      "SELECT dolt_tag('v1');"
      "SELECT dolt_branch('feat');"
      "INSERT INTO t VALUES(2,'y');"
      "SELECT dolt_add('t');")==SQLITE_OK);
  sqlite3_close(rw);
  rw = 0;

  check("ro_reset: open readonly",
        sqlite3_open_v2(zPath, &ro, SQLITE_OPEN_READONLY, 0)==SQLITE_OK);
  if( !ro ) return;
  check("ro_reset: branch before is readonly",
        exec_rc(ro, "SELECT dolt_branch('before_reset')")==SQLITE_READONLY);
  check("ro_reset: reset is readonly",
        exec_rc(ro, "SELECT dolt_reset('t')")==SQLITE_READONLY);
  {
    char *zCheckoutErr = 0;
    int checkoutRc = sqlite3_exec(ro, "SELECT dolt_checkout('feat')", 0, 0,
                                  &zCheckoutErr);
    check("ro_reset: checkout is readonly", checkoutRc==SQLITE_READONLY);
    check("ro_reset: checkout readonly message",
          zCheckoutErr
          && (strstr(zCheckoutErr, "readonly")!=0
              || strstr(zCheckoutErr, "read-only")!=0));
    sqlite3_free(zCheckoutErr);
  }
  check("ro_reset: branch after is still readonly",
        exec_rc(ro, "SELECT dolt_branch('after_reset')")==SQLITE_READONLY);
  check("ro_reset: tag is still readonly",
        exec_rc(ro, "SELECT dolt_tag('ro_tag')")==SQLITE_READONLY);
  check("ro_reset: tag delete is still readonly",
        exec_rc(ro, "SELECT dolt_tag('-d','v1')")==SQLITE_READONLY);
  check("ro_reset: branch delete is still readonly",
        exec_rc(ro, "SELECT dolt_branch('-D','feat')")==SQLITE_READONLY);
  check("ro_reset: commit is still readonly",
        exec_rc(ro, "SELECT dolt_commit('-m','ro commit')")==SQLITE_READONLY);
  check("ro_reset: insert is still readonly",
        exec_rc(ro, "INSERT INTO t VALUES(3,'z')")==SQLITE_READONLY);
  sqlite3_close(ro);

  check("ro_reset: reopen rw", sqlite3_open(zPath, &rw)==SQLITE_OK);
  if( !rw ){ remove(zPath); return; }
  if( sqlite3_prepare_v2(rw,
        "SELECT group_concat(name) FROM (SELECT name FROM dolt_branches ORDER BY name)",
        -1, &p, 0)==SQLITE_OK && sqlite3_step(p)==SQLITE_ROW ){
    zBranches = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(p, 0));
  }
  sqlite3_finalize(p); p = 0;
  check("ro_reset: branches unchanged",
        zBranches && strcmp(zBranches, "feat,main")==0);
  if( sqlite3_prepare_v2(rw,
        "SELECT group_concat(tag_name) FROM (SELECT tag_name FROM dolt_tags ORDER BY tag_name)",
        -1, &p, 0)==SQLITE_OK && sqlite3_step(p)==SQLITE_ROW ){
    zTags = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(p, 0));
  }
  sqlite3_finalize(p); p = 0;
  check("ro_reset: tags unchanged",
        zTags && strcmp(zTags, "v1")==0);
  if( sqlite3_prepare_v2(rw, "SELECT message FROM dolt_log LIMIT 1",
        -1, &p, 0)==SQLITE_OK && sqlite3_step(p)==SQLITE_ROW ){
    zMsg = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(p, 0));
  }
  sqlite3_finalize(p);
  check("ro_reset: log unchanged", zMsg && strcmp(zMsg, "c1")==0);
  check("ro_reset: still staged",
        scalar(rw, "SELECT count(*) FROM dolt_status WHERE staged=1")==1);
  sqlite3_free(zBranches);
  sqlite3_free(zTags);
  sqlite3_free(zMsg);
  sqlite3_close(rw);
  remove(zPath);
}

typedef struct StatusSnapshotPeer StatusSnapshotPeer;
struct StatusSnapshotPeer {
  int toPeer[2];
  int fromPeer[2];
  unsigned event;
  int fired;
  int peerRc;
  int txnState;
};

static int statusSnapshotPeer(
  unsigned event, void *arg, void *stmt, void *extra
){
  StatusSnapshotPeer *p = (StatusSnapshotPeer*)arg;
  const char *zSql = sqlite3_sql((sqlite3_stmt*)stmt);
  char ch = 'g';
  (void)extra;
  if( event!=p->event || p->fired || !zSql
   || strstr(zSql, "SELECT name, rootpage, sql")==0 ) return 0;
  p->fired = 1;
  p->txnState = sqlite3_txn_state(sqlite3_db_handle(stmt), "main");
  if( write(p->toPeer[1], &ch, 1)!=1
   || read(p->fromPeer[0], &p->peerRc, sizeof(p->peerRc))
       !=sizeof(p->peerRc) ) p->fired = -1;
  return 0;
}

static void test_status_snapshot_during_schema_change(
  const char *zPath, int readonly, unsigned event, int boundary, int withTable
){
  sqlite3 *db = 0;
  sqlite3_stmt *pStmt = 0;
  StatusSnapshotPeer p;
  pid_t pid;
  int status = 0, rc;
  char ch = 'g';

  remove(zPath);
  check("status_snapshot: seed open", sqlite3_open(zPath, &db)==SQLITE_OK);
  check("status_snapshot: seed", exec_rc(db,
      "CREATE TABLE w(id INTEGER PRIMARY KEY, v TEXT);"
      "INSERT INTO w VALUES(1,'seed');"
      "SELECT dolt_commit('-Am','seed');"
      "CREATE TABLE d(id INTEGER PRIMARY KEY, v TEXT)")==SQLITE_OK);
  sqlite3_close(db);
  db = 0;
  memset(&p, 0, sizeof(p));
  p.event = event;
  p.peerRc = SQLITE_ERROR;
  if( pipe(p.toPeer) || pipe(p.fromPeer) ){ perror("pipe"); exit(1); }
  pid = fork();
  if( pid<0 ){ perror("fork"); exit(1); }
  if( pid==0 ){
    sqlite3 *peer = 0;
    int peerRc = sqlite3_open(zPath, &peer);
    sqlite3_busy_timeout(peer, 1000);
    if( read(p.toPeer[0], &ch, 1)!=1 ) _exit(1);
    if( peerRc==SQLITE_OK ) peerRc = exec_rc(peer,
        "INSERT INTO w VALUES(2,'peer');"
        "CREATE TABLE e(id INTEGER PRIMARY KEY, v TEXT);"
        "SELECT dolt_gc()");
    sqlite3_close(peer);
    if( write(p.fromPeer[1], &peerRc, sizeof(peerRc))!=sizeof(peerRc) ) _exit(1);
    _exit(0);
  }
  check("status_snapshot: reader open", sqlite3_open_v2(zPath, &db,
      readonly ? SQLITE_OPEN_READONLY : SQLITE_OPEN_READWRITE, 0)==SQLITE_OK);
  if( boundary ) check("status_snapshot: begin", exec_rc(db,
      boundary==1 ? "BEGIN" : "SAVEPOINT caller")==SQLITE_OK);
  rc = sqlite3_prepare_v2(db,
      withTable
        ? "SELECT (SELECT count(*) FROM dolt_status), (SELECT count(*) FROM w)"
        : "SELECT count(*) FROM dolt_status",
      -1, &pStmt, 0);
  check("status_snapshot: prepare", rc==SQLITE_OK);
  sqlite3_trace_v2(db, event, statusSnapshotPeer, &p);
  rc = sqlite3_step(pStmt);
  check("status_snapshot: status and table use one snapshot", rc==SQLITE_ROW
      && sqlite3_column_int(pStmt, 0)==1
      && (!withTable || sqlite3_column_int(pStmt, 1)==1));
  if( rc==SQLITE_ROW ) rc = sqlite3_step(pStmt);
  check("status_snapshot: finish", rc==SQLITE_DONE);
  check("status_snapshot: finalize", sqlite3_finalize(pStmt)==SQLITE_OK);
  sqlite3_trace_v2(db, 0, 0, 0);
  check("status_snapshot: nested read kept snapshot",
      p.fired==1 && p.txnState==SQLITE_TXN_READ);
  check("status_snapshot: peer write proceeded", p.peerRc==SQLITE_OK);
  if( !p.fired ){
    if( write(p.toPeer[1], &ch, 1)!=1
     || read(p.fromPeer[0], &p.peerRc, sizeof(p.peerRc))
         !=sizeof(p.peerRc) ) exit(1);
  }
  waitpid(pid, &status, 0);
  check("status_snapshot: peer exit", WIFEXITED(status) && WEXITSTATUS(status)==0);
  close(p.toPeer[0]); close(p.toPeer[1]);
  close(p.fromPeer[0]); close(p.fromPeer[1]);
  check("status_snapshot: caller transaction preserved",
      sqlite3_get_autocommit(db)==(boundary==0));
  if( boundary ){
    check("status_snapshot: caller still sees old rows",
        scalar(db, "SELECT count(*) FROM w")==1);
    check("status_snapshot: end caller", exec_rc(db,
        boundary==1 ? "COMMIT" : "RELEASE caller")==SQLITE_OK);
  }
  check("status_snapshot: snapshot released",
      sqlite3_txn_state(db, "main")==SQLITE_TXN_NONE);
  check("status_snapshot: next read sees peer rows",
      scalar(db, "SELECT count(*) FROM w")==2);
  check("status_snapshot: next status sees peer schema",
      scalar(db, "SELECT count(*) FROM dolt_status")==3);
  check("status_snapshot: close", sqlite3_close(db)==SQLITE_OK);
  remove(zPath);
}

int main(void){
  char zPath[256];
  int readonly, boundary, withTable;
  snprintf(zPath, sizeof(zPath), DOLTLITE_TEST_TMPDIR "/dolt_ro_reader_%d.db", (int)getpid());

  test_readonly_status_does_not_starve_a_writer(zPath, 1);
  test_readonly_status_does_not_starve_a_writer(zPath, 4);
  test_readonly_still_sees_peer_changes(zPath);
  test_readonly_open_replay(zPath, 0, 0);
  test_readonly_open_replay(zPath, 1, 0);
  test_readonly_open_replay(zPath, 0, 1);
  test_readonly_reset_does_not_unlock_writers(zPath);
  for(readonly=0; readonly<2; readonly++){
    for(boundary=0; boundary<3; boundary++){
      for(withTable=0; withTable<2; withTable++){
        test_status_snapshot_during_schema_change(
            zPath, readonly, SQLITE_TRACE_STMT, boundary, withTable);
        test_status_snapshot_during_schema_change(
            zPath, readonly, SQLITE_TRACE_PROFILE, boundary, withTable);
      }
    }
  }

  printf("readonly_reader_writer_test: %d passed, %d failed\n", nPass, nFail);
  return nFail ? 1 : 0;
}
