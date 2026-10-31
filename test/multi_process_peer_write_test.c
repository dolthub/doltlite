/*
** A peer connection's acknowledged write must survive whatever version
** control operation this connection runs. Each operation runs on main
** against a peer that autocommits a row to main, either leaving it in the
** working set or committing it, in three ways:
**
**   stale:  the peer's write is acknowledged before the operation, after
**           this connection last read, so the operation starts stale;
**   midop:  the peer's write fires from the operation's progress handler at
**           every step in turn; an operation refused as busy is retried;
**   txn:    the peer holds an open transaction across the operation and
**           commits after it.
**
** A fresh connection then checks that every acknowledged peer row is on
** main and that the store passes integrity_check. Every operation must also
** have succeeded in some mid-operation run, or its row tested nothing. Two
** connections in one process contend through the VFS lock exactly as two
** processes do.
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/wait.h>
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

static int execSql(sqlite3 *db, const char *sql){
  return sqlite3_exec(db, sql, 0, 0, 0);
}

static char result_buf[4096];
static const char *queryText(sqlite3 *db, const char *sql){
  sqlite3_stmt *s = 0;
  int rc;
  result_buf[0] = 0;
  rc = sqlite3_prepare_v2(db, sql, -1, &s, 0);
  if( rc!=SQLITE_OK ){
    snprintf(result_buf, sizeof(result_buf), "ERR: %s", sqlite3_errmsg(db));
    return result_buf;
  }
  while( (rc = sqlite3_step(s))==SQLITE_ROW ){
    const char *v = (const char*)sqlite3_column_text(s, 0);
    snprintf(result_buf, sizeof(result_buf), "%s", v ? v : "");
  }
  if( rc!=SQLITE_DONE ){
    snprintf(result_buf, sizeof(result_buf), "ERR: %s", sqlite3_errmsg(db));
  }
  sqlite3_finalize(s);
  return result_buf;
}

static int isBusy(const char *z){
  return strstr(z, "busy")!=0 || strstr(z, "locked")!=0
      || strstr(z, "another connection committed")!=0
      || strstr(z, "changed")!=0;
}

static char zDir[256], zTemplate[300], zTemplateRemote[300];
static char zDb[300], zRemote[300], zInto[300];

static int copyFile(const char *zFrom, const char *zTo){
  FILE *in = fopen(zFrom, "rb");
  FILE *out;
  char buf[65536];
  size_t n;
  int ok = 1;
  if( !in ) return 0;
  out = fopen(zTo, "wb");
  if( !out ){ fclose(in); return 0; }
  while( (n = fread(buf, 1, sizeof(buf), in))>0 ){
    if( fwrite(buf, 1, n, out)!=n ){ ok = 0; break; }
  }
  fclose(in);
  if( fclose(out)!=0 ) ok = 0;
  return ok;
}

/* main has t, u and an FK pair pc/cc, an extra commit c2 tagged c2 (so f
** diverges), branch f with a commit on u, branch ff one commit ahead of main,
** branch f2, branch fc whose row 2 conflicts with main's, branch fk that
** deletes the parent main's child references, tag v0, remote o0, and remote
** origin, a file remote that holds main plus one newer commit on u. */
static void buildTemplate(void){
  sqlite3 *db = 0;
  sqlite3 *client = 0;
  char sql[2048];
  char zClient[300];
  snprintf(zTemplate, sizeof(zTemplate), "%s/template.db", zDir);
  snprintf(zTemplateRemote, sizeof(zTemplateRemote), "%s/template_remote.db", zDir);
  snprintf(zClient, sizeof(zClient), "%s/template_client.db", zDir);
  sqlite3_open(zTemplate, &db);
  snprintf(sql, sizeof(sql),
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'base');"
    "CREATE TABLE u(x INTEGER PRIMARY KEY);"
    "CREATE TABLE pc(id INTEGER PRIMARY KEY);"
    "CREATE TABLE cc(id INTEGER PRIMARY KEY, pid INT REFERENCES pc(id));"
    "INSERT INTO pc VALUES(1),(2);"
    "SELECT dolt_commit('-Am','init');"
    "SELECT dolt_tag('v0');"
    "SELECT dolt_branch('f2');"
    "SELECT dolt_remote('add','o0','file:///nonexistent-peer-write');"
    "SELECT dolt_branch('fc');"
    "SELECT dolt_checkout('fc');"
    "INSERT INTO t VALUES(2,'theirs');"
    "SELECT dolt_commit('-am','fc1');"
    "SELECT dolt_checkout('main');"
    "SELECT dolt_branch('fk');"
    "SELECT dolt_checkout('fk');"
    "DELETE FROM pc WHERE id=2;"
    "SELECT dolt_commit('-am','fk1');"
    "SELECT dolt_checkout('main');"
    "INSERT INTO cc VALUES(1,2);"
    "SELECT dolt_commit('-am','child');"
    "SELECT dolt_branch('f');"
    "SELECT dolt_checkout('f');"
    "INSERT INTO u VALUES(9);"
    "SELECT dolt_commit('-am','f1');"
    "SELECT dolt_checkout('main');"
    "INSERT INTO t VALUES(2,'two');"
    "SELECT dolt_commit('-am','c2');"
    "SELECT dolt_tag('c2');"
    "SELECT dolt_branch('ff');"
    "SELECT dolt_checkout('ff');"
    "INSERT INTO u VALUES(7);"
    "SELECT dolt_commit('-am','ff1');"
    "SELECT dolt_checkout('main');"
    "SELECT dolt_remote('add','origin','file://%s');"
    "SELECT dolt_push('origin','main');",
    zTemplateRemote);
  check("template_built", execSql(db, sql)==SQLITE_OK);
  sqlite3_close(db);
  sqlite3_open(zClient, &client);
  snprintf(sql, sizeof(sql),
    "SELECT dolt_clone('file://%s');"
    "INSERT INTO u VALUES(50);"
    "SELECT dolt_commit('-am','remote update');"
    "SELECT dolt_push('origin','main');", zTemplateRemote);
  check("template_remote_built", execSql(client, sql)==SQLITE_OK);
  sqlite3_close(client);
  remove(zClient);
}

/* A fresh copy of the template, at the same path every run so the remote
** URL the template recorded still resolves. */
static int freshCopy(void){
  remove(zDb);
  remove(zInto);
  return copyFile(zTemplate, zDb) && copyFile(zTemplateRemote, zRemote);
}

typedef struct Op Op;
struct Op {
  const char *zName;
  const char *zPre;
  const char *zSql;
  const char *zCheck;
  int bCommitOnly;
  const char *zFails;
};

static const Op aOp[] = {
  { "add", 0, "SELECT dolt_add('.')", "peer_write_kept_add" },
  { "commit", "INSERT INTO u VALUES(77)", "SELECT dolt_commit('-Am','mine')",
    "peer_write_kept_commit" },
  { "commit_named_main", "SELECT dolt_checkout('f')",
    "SELECT dolt_commit('--branch','main','-Am','mine','--allow-empty')",
    "peer_write_kept_commit_named_main" },
  { "commit_named_other", 0,
    "SELECT dolt_commit('--branch','f','-Am','mine','--allow-empty')",
    "peer_write_kept_commit_named_other" },
  { "commit_amend", "INSERT INTO u VALUES(77)",
    "SELECT dolt_commit('-a','--amend','-m','mine')",
    "peer_write_kept_commit_amend" },
  { "branch_create", 0, "SELECT dolt_branch('x')",
    "peer_write_kept_branch_create" },
  { "branch_copy", 0, "SELECT dolt_branch('-c','f2','f4')",
    "peer_write_kept_branch_copy" },
  { "branch_delete", 0, "SELECT dolt_branch('-d','f2')",
    "peer_write_kept_branch_delete" },
  { "branch_rename", 0, "SELECT dolt_branch('-m','f2','f3')",
    "peer_write_kept_branch_rename" },
  { "tag_create", 0, "SELECT dolt_tag('v1')", "peer_write_kept_tag_create" },
  { "tag_delete", 0, "SELECT dolt_tag('-d','v0')",
    "peer_write_kept_tag_delete" },
  { "remote_add", 0,
    "SELECT dolt_remote('add','o1','file:///nonexistent-peer-write-1')",
    "peer_write_kept_remote_add" },
  { "remote_remove", 0, "SELECT dolt_remote('remove','o0')",
    "peer_write_kept_remote_remove" },
  { "reset_soft", 0, "SELECT dolt_reset('--soft')",
    "peer_write_kept_reset_soft" },
  { "reset_soft_ref", 0, "SELECT dolt_reset('--soft','HEAD')",
    "peer_write_kept_reset_soft_ref" },
  { "reset_mixed", 0, "SELECT dolt_reset()", "peer_write_kept_reset_mixed" },
  { "reset_mixed_ref", 0, "SELECT dolt_reset('HEAD')",
    "peer_write_kept_reset_mixed_ref" },
  { "reset_table", 0, "SELECT dolt_reset('u')", "peer_write_kept_reset_table" },
  /* Discarding a peer's uncommitted row is what --hard is for. */
  { "reset_hard", 0, "SELECT dolt_reset('--hard')",
    "peer_write_kept_reset_hard", 1 },
  { "checkout_branch", 0, "SELECT dolt_checkout('f')",
    "peer_write_kept_checkout_branch" },
  { "checkout_new", 0, "SELECT dolt_checkout('-b','f5')",
    "peer_write_kept_checkout_new" },
  { "checkout_table", 0, "SELECT dolt_checkout('u')",
    "peer_write_kept_checkout_table" },
  { "merge_ff", 0, "SELECT dolt_merge('ff')", "peer_write_kept_merge_ff" },
  { "merge_ff_only", 0, "SELECT dolt_merge('--ff-only','ff')",
    "peer_write_kept_merge_ff_only" },
  { "merge", 0, "SELECT dolt_merge('f')", "peer_write_kept_merge" },
  { "merge_squash", 0, "SELECT dolt_merge('--squash','f')",
    "peer_write_kept_merge_squash" },
  { "cherry_pick", 0, "SELECT dolt_cherry_pick('f')",
    "peer_write_kept_cherry_pick" },
  { "revert_series", 0, "SELECT dolt_revert('c2','c2~1')",
    "peer_write_kept_revert_series" },
  { "revert", 0, "SELECT dolt_revert('c2')", "peer_write_kept_revert" },
  /* These fail by design; the rollback must not erase the peer's write. */
  { "merge_conflict", 0, "SELECT dolt_merge('fc')",
    "peer_write_kept_merge_conflict", 0, "conflicts detected" },
  { "cherry_pick_conflict", 0, "SELECT dolt_cherry_pick('fc')",
    "peer_write_kept_cherry_pick_conflict", 0, "conflict" },
  { "merge_violation", "PRAGMA foreign_keys=ON", "SELECT dolt_merge('fk')",
    "peer_write_kept_merge_violation", 0, "constraint violations" },
  { "cherry_pick_violation", "PRAGMA foreign_keys=ON",
    "SELECT dolt_cherry_pick('fk')",
    "peer_write_kept_cherry_pick_violation", 0, "constraint violations" },
  { "cherry_pick_empty", 0, "SELECT dolt_cherry_pick('HEAD')",
    "peer_write_kept_cherry_pick_empty", 0, "no changes" },
  { "rebase_interactive", 0, "SELECT dolt_rebase('-i','f')",
    "peer_write_kept_rebase_interactive" },
  { "rebase", 0, "SELECT dolt_rebase('f')", "peer_write_kept_rebase" },
  { "clean", 0, "SELECT dolt_clean()", "peer_write_kept_clean" },
  { "fetch", 0, "SELECT dolt_fetch('origin')", "peer_write_kept_fetch" },
  { "pull", 0, "SELECT dolt_pull('origin','main')", "peer_write_kept_pull" },
  { "push", 0, "SELECT dolt_push('origin','f2')", "peer_write_kept_push" },
  { "gc", 0, "SELECT dolt_gc()", "peer_write_kept_gc" },
  { "vacuum", 0, "VACUUM", "peer_write_kept_vacuum" },
  { "vacuum_into", 0, "VACUUM INTO '%s'", "peer_write_kept_vacuum_into" },
};

/* The peer either leaves its row in main's working set or commits it, so
** a dirty working set does not refuse every operation that needs a clean
** one before it can race the peer. */
#define PEER_WRITE  0
#define PEER_COMMIT 1
static const char *azPeerAction[] = { "write", "commit" };

#define MODE_STALE 0
#define MODE_MIDOP 1
#define MODE_TXN   2
static const char *azMode[] = { "stale", "midop", "txn" };

typedef struct Tally Tally;
struct Tally {
  int nAcked;
  int nOpOk;
  int nLost;
};

/* An operation that fails by design ran when it failed as designed. */
static int opRan(const Op *p, int rc, const char *zErr){
  if( p->zFails ) return rc!=SQLITE_OK && zErr && strstr(zErr, p->zFails)!=0;
  return rc==SQLITE_OK;
}

static void opSql(const Op *p, char *zOut, int nOut){
  if( strstr(p->zSql, "%s") ){
    snprintf(zOut, nOut, p->zSql, zInto);
  }else{
    snprintf(zOut, nOut, "%s", p->zSql);
  }
}

/* Returns whether the operation ran (see opRan). */
static int runOpSql(const Op *p, sqlite3 *db, const char *zSql, int *pBusy){
  char *zErr = 0;
  int rc = sqlite3_exec(db, zSql, 0, 0, &zErr);
  int ran = opRan(p, rc, zErr);
  if( pBusy ){
    *pBusy = rc!=SQLITE_OK
          && ((rc&0xff)==SQLITE_BUSY || (zErr && isBusy(zErr)));
  }
  sqlite3_free(zErr);
  return ran;
}

/* Acknowledged once the peer's INSERT autocommits; the commit is extra,
** unless only a committed row is owed to the peer. */
static int peerAct(sqlite3 *peer, int action, int bCommitOnly){
  int rc;
  if( execSql(peer, "INSERT INTO t VALUES(900,'peer')")!=SQLITE_OK ) return 0;
  if( action!=PEER_COMMIT ) return 1;
  rc = execSql(peer, "SELECT dolt_commit('-am','peer')");
  return !bCommitOnly || rc==SQLITE_OK;
}

/* The peer's row is on main and the store is intact. */
static int peerRowKept(const char *zOp, const char *zMode, int action, int k){
  sqlite3 *db = 0;
  const char *z;
  int ok;
  sqlite3_open(zDb, &db);
  sqlite3_busy_timeout(db, 5000);
  z = queryText(db, "SELECT count(*) FROM t WHERE id=900");
  ok = strcmp(z, "1")==0;
  if( ok ){
    z = queryText(db, "PRAGMA integrity_check");
    ok = strcmp(z, "ok")==0;
  }
  if( !ok ){
    fprintf(stderr, "%s/%s/%s step %d: %s\n",
            zOp, zMode, azPeerAction[action], k, z);
  }
  sqlite3_close(db);
  return ok;
}

static void tallyRun(Tally *t, int acked, int opOk, int kept){
  t->nAcked += acked;
  t->nOpOk += opOk;
  if( acked && !kept ) t->nLost++;
}

static void runStale(const Op *p, int action, Tally *t){
  sqlite3 *db = 0, *peer = 0;
  char zSql[512];
  int acked, opOk;
  if( !freshCopy() ){ t->nLost++; return; }
  opSql(p, zSql, sizeof(zSql));
  sqlite3_open(zDb, &db);
  sqlite3_open(zDb, &peer);
  sqlite3_busy_timeout(db, 5000);
  sqlite3_busy_timeout(peer, 5000);
  if( p->zPre ) execSql(db, p->zPre);
  queryText(db, "SELECT count(*) FROM t");
  acked = peerAct(peer, action, p->bCommitOnly);
  opOk = runOpSql(p, db, zSql, 0);
  execSql(db, "INSERT INTO t VALUES(3,'mine')");
  sqlite3_close(peer);
  sqlite3_close(db);
  tallyRun(t, acked, opOk,
           acked ? peerRowKept(p->zName, "stale", action, 0) : 1);
}

/* The peer runs inside the operation's progress handler on the same
** thread, so it never waits on a lock the paused operation holds. */
typedef struct MidOp MidOp;
struct MidOp {
  sqlite3 *peer;
  int action;
  int bCommitOnly;
  int fireAt;
  int nCalls;
  int fired;
  int acked;
};

static int fireMidOp(void *arg){
  MidOp *m = (MidOp*)arg;
  if( ++m->nCalls==m->fireAt && !m->fired ){
    m->fired = 1;
    m->acked = peerAct(m->peer, m->action, m->bCommitOnly);
  }
  return 0;
}

/* Returns 0 once k is past the operation's last step. A run refused as
** busy because the peer moved is retried, as an application would. */
static int runMidOp(const Op *p, int action, int k, Tally *t){
  sqlite3 *db = 0;
  MidOp m;
  char zSql[512];
  int ran, busy = 0;
  if( !freshCopy() ){ t->nLost++; return 0; }
  opSql(p, zSql, sizeof(zSql));
  memset(&m, 0, sizeof(m));
  sqlite3_open(zDb, &db);
  sqlite3_open(zDb, &m.peer);
  sqlite3_busy_timeout(db, 5000);
  if( p->zPre ) execSql(db, p->zPre);
  m.action = action;
  m.bCommitOnly = p->bCommitOnly;
  m.fireAt = k;
  sqlite3_progress_handler(db, 1, fireMidOp, &m);
  ran = runOpSql(p, db, zSql, &busy);
  sqlite3_progress_handler(db, 0, 0, 0);
  if( m.fired && busy ) ran = runOpSql(p, db, zSql, 0);
  execSql(db, "INSERT INTO t VALUES(3,'mine')");
  sqlite3_close(m.peer);
  sqlite3_close(db);
  if( !m.fired ) return 0;
  tallyRun(t, m.acked, ran,
           m.acked ? peerRowKept(p->zName, "midop", action, k) : 1);
  return 1;
}

/* The peer's transaction holds the write lock across the operation, so
** write operations are refused; the commit that follows must still land. */
static void runTxn(const Op *p, Tally *t){
  sqlite3 *db = 0, *peer = 0;
  char zSql[512];
  int committed, opOk;
  if( !freshCopy() ){ t->nLost++; return; }
  opSql(p, zSql, sizeof(zSql));
  sqlite3_open(zDb, &db);
  sqlite3_open(zDb, &peer);
  sqlite3_busy_timeout(db, 200);
  sqlite3_busy_timeout(peer, 5000);
  if( p->zPre ) execSql(db, p->zPre);
  execSql(peer, "BEGIN; INSERT INTO t VALUES(900,'peer')");
  opOk = runOpSql(p, db, zSql, 0);
  committed = execSql(peer, "COMMIT")==SQLITE_OK;
  if( !committed ) execSql(peer, "ROLLBACK");
  execSql(db, "INSERT INTO t VALUES(3,'mine')");
  sqlite3_close(peer);
  sqlite3_close(db);
  tallyRun(t, committed, opOk,
           committed ? peerRowKept(p->zName, "txn", PEER_WRITE, 0) : 1);
}

/* An autocommit VC operation in this process races a peer process's
** autocommit insert. In-process peers share the store's lock state and miss
** this, so the peer forks and fires from the operation's progress handler.
** An operation that holds its write transaction refuses the peer, which
** then retries once the operation is done. A follow-up clean then persists
** whatever working set this connection holds; the peer's acknowledged row
** must survive both. */
typedef struct ProcPeer ProcPeer;
struct ProcPeer {
  int toPeer[2];
  int fromPeer[2];
  int fireAt;
  int nCalls;
  int fired;
  char ack;
};

static int fireProcPeer(void *arg){
  ProcPeer *pp = (ProcPeer*)arg;
  char ch = 'g';
  if( ++pp->nCalls==pp->fireAt && !pp->fired ){
    pp->fired = 1;
    if( write(pp->toPeer[1], &ch, 1)!=1 || read(pp->fromPeer[0], &pp->ack, 1)!=1 ){
      pp->fired = -1;
    }
  }
  return 0;
}

static int runProcPeer(const char *zPre, const char *zSql, int k, int *pLost){
  char zPath[300];
  sqlite3 *db = 0;
  ProcPeer pp;
  pid_t pid;
  char ch = 'q';
  int status = 0;
  snprintf(zPath, sizeof(zPath), "%s/proc.db", zDir);
  remove(zPath);
  sqlite3_open(zPath, &db);
  execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
              "CREATE TABLE u(x INTEGER PRIMARY KEY);"
              "SELECT dolt_commit('-Am','init')");
  memset(&pp, 0, sizeof(pp));
  pp.fireAt = k;
  if( pipe(pp.toPeer) || pipe(pp.fromPeer) ){ perror("pipe"); exit(1); }
  pid = fork();
  if( pid==0 ){
    sqlite3 *peer = 0;
    int ok;
    sqlite3_open(zPath, &peer);
    sqlite3_busy_timeout(peer, 50);
    if( read(pp.toPeer[0], &ch, 1)!=1 || ch!='g' ) _exit(0);
    ok = execSql(peer, "INSERT INTO u VALUES(1)")==SQLITE_OK;
    ch = ok ? 'a' : 'n';
    if( write(pp.fromPeer[1], &ch, 1)!=1 ) _exit(1);
    if( !ok ){
      if( read(pp.toPeer[0], &ch, 1)!=1 ) _exit(1);
      sqlite3_busy_timeout(peer, 5000);
      ok = execSql(peer, "INSERT INTO u VALUES(1)")==SQLITE_OK;
      ch = ok ? 'a' : 'n';
      if( write(pp.fromPeer[1], &ch, 1)!=1 ) _exit(1);
    }
    sqlite3_close(peer);
    _exit(0);
  }
  if( pid<0 ){ perror("fork"); exit(1); }
  sqlite3_busy_timeout(db, 5000);
  execSql(db, "INSERT INTO t VALUES(3,'dirty')");
  if( zPre ) execSql(db, zPre);
  sqlite3_progress_handler(db, 1, fireProcPeer, &pp);
  execSql(db, zSql);
  sqlite3_progress_handler(db, 0, 0, 0);
  if( !pp.fired && write(pp.toPeer[1], &ch, 1)!=1 ){ perror("write"); exit(1); }
  if( pp.fired==1 && pp.ack=='n' ){
    ch = 'r';
    if( write(pp.toPeer[1], &ch, 1)!=1 || read(pp.fromPeer[0], &pp.ack, 1)!=1 ){
      perror("retry"); exit(1);
    }
  }
  execSql(db, "SELECT dolt_clean()");
  waitpid(pid, &status, 0);
  sqlite3_close(db);
  close(pp.toPeer[0]); close(pp.toPeer[1]);
  close(pp.fromPeer[0]); close(pp.fromPeer[1]);
  if( pp.fired==1 && pp.ack!='a' ){
    fprintf(stderr, "  %s step %d: peer never acknowledged\n", zSql, k);
    (*pLost)++;
  }else if( pp.fired==1 ){
    sqlite3_open(zPath, &db);
    if( strcmp(queryText(db, "SELECT count(*) FROM u"), "1")!=0 ){
      fprintf(stderr, "  %s step %d: peer row lost\n", zSql, k);
      (*pLost)++;
    }else if( strcmp(queryText(db, "SELECT count(*)>0 FROM t"), "1")!=0 ){
      fprintf(stderr, "  %s step %d: table t emptied\n", zSql, k);
      (*pLost)++;
    }
    sqlite3_close(db);
  }
  remove(zPath);
  return pp.fired!=0;
}

static void test_process_peer_write_kept(
  const char *zName,
  const char *zPre,
  const char *zSql
){
  char zCheck[96];
  int k, nLost = 0;
  for(k=1; k<5000 && runProcPeer(zPre, zSql, k, &nLost); k++){}
  printf("  process peer %-12s steps=%d lost=%d\n", zName, k-1, nLost);
  snprintf(zCheck, sizeof(zCheck), "process_peer_write_kept_%s", zName);
  check(zCheck, k>1 && nLost==0);
}

typedef struct AddFlushPeer AddFlushPeer;
struct AddFlushPeer {
  int toPeer[2];
  int fromPeer[2];
  int fired;
  int peerRc;
  int txnState;
  int waitForCommit;
  int nSkipScan;
};

static int fireAddFlushPeer(unsigned event, void *arg, void *stmt, void *elapsed){
  AddFlushPeer *p = (AddFlushPeer*)arg;
  const char *sql = sqlite3_sql((sqlite3_stmt*)stmt);
  char ch = 'g';
  (void)elapsed;
  if( event!=SQLITE_TRACE_PROFILE || p->fired || !sql ) return 0;
  if( sqlite3_stricmp(sql, "COMMIT")==0 ){
    p->waitForCommit = 0;
    return 0;
  }
  if( p->waitForCommit || strstr(sql, "SELECT name, rootpage, sql")==0 ){
    return 0;
  }
  if( p->nSkipScan>0 ){
    p->nSkipScan--;
    return 0;
  }
  p->fired = 1;
  p->txnState = sqlite3_txn_state(sqlite3_db_handle(stmt), "main");
  if( write(p->toPeer[1], &ch, 1)!=1
   || read(p->fromPeer[0], &p->peerRc, sizeof(p->peerRc))!=sizeof(p->peerRc) ){
    p->fired = -1;
  }
  return 0;
}

static void test_process_add_flush_lock(const char *zArg, int boundary){
  sqlite3 *db = 0;
  AddFlushPeer p;
  char zPath[300], zSql[128], zCheck[128], zHead[64];
  pid_t pid;
  int status = 0, rc, retryRc = SQLITE_ERROR;
  char ch = 'r';
  snprintf(zPath, sizeof(zPath), "%s/add-flush.db", zDir);
  remove(zPath);
  check("add_flush_open", sqlite3_open(zPath, &db)==SQLITE_OK);
  check("add_flush_seed", execSql(db,
      "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
      "CREATE TABLE u(x INTEGER PRIMARY KEY);"
      "SELECT dolt_commit('-Am','init');"
      "INSERT INTO t VALUES(3,'dirty')")==SQLITE_OK);
  snprintf(zHead, sizeof(zHead), "%s", queryText(db, "SELECT dolt_hashof('HEAD')"));
  sqlite3_close(db);
  memset(&p, 0, sizeof(p));
  p.waitForCommit = boundary==2;
  if( pipe(p.toPeer) || pipe(p.fromPeer) ){ perror("pipe"); exit(1); }
  pid = fork();
  if( pid<0 ){ perror("fork"); exit(1); }
  if( pid==0 ){
    sqlite3 *peer = 0;
    int peerRc = sqlite3_open(zPath, &peer);
    if( read(p.toPeer[0], &ch, 1)!=1 ) _exit(1);
    if( peerRc==SQLITE_OK ) peerRc = execSql(peer, "INSERT INTO u VALUES(1)");
    if( write(p.fromPeer[1], &peerRc, sizeof(peerRc))!=sizeof(peerRc) ) _exit(1);
    if( read(p.toPeer[0], &ch, 1)!=1 ) _exit(1);
    if( (peerRc&255)==SQLITE_BUSY || (peerRc&255)==SQLITE_LOCKED ){
      sqlite3_busy_timeout(peer, 5000);
      peerRc = execSql(peer, "INSERT INTO u VALUES(1)");
    }
    sqlite3_close(peer);
    if( write(p.fromPeer[1], &peerRc, sizeof(peerRc))!=sizeof(peerRc) ) _exit(1);
    _exit(0);
  }
  check("add_flush_parent_open", sqlite3_open(zPath, &db)==SQLITE_OK);
  sqlite3_busy_timeout(db, 5000);
  if( boundary==1 ) check("add_flush_begin", execSql(db, "BEGIN")==SQLITE_OK);
  if( boundary==2 ) check("add_flush_savepoint", execSql(db, "SAVEPOINT caller")==SQLITE_OK);
  if( boundary==3 ) check("add_flush_nested", execSql(db, "BEGIN; SAVEPOINT caller")==SQLITE_OK);
  sqlite3_trace_v2(db, SQLITE_TRACE_PROFILE, fireAddFlushPeer, &p);
  snprintf(zSql, sizeof(zSql), "SELECT dolt_add('%s')", zArg);
  rc = execSql(db, zSql);
  sqlite3_trace_v2(db, 0, 0, 0);
  snprintf(zCheck, sizeof(zCheck), "process_add_flush_%s_%d_lock_held", zArg, boundary);
  check(zCheck, p.fired==1 && p.txnState==SQLITE_TXN_WRITE
      && (p.peerRc&255)==SQLITE_BUSY);
  snprintf(zCheck, sizeof(zCheck), "process_add_flush_%s_%d_success", zArg, boundary);
  check(zCheck, rc==SQLITE_OK);
  check("add_flush_caller_boundary", sqlite3_get_autocommit(db)==(boundary==0 || boundary==2));
  if( !sqlite3_get_autocommit(db) ) check("add_flush_commit", execSql(db, "COMMIT")==SQLITE_OK);
  if( !p.fired ){
    ch = 'g';
    if( write(p.toPeer[1], &ch, 1)!=1
     || read(p.fromPeer[0], &p.peerRc, sizeof(p.peerRc))!=sizeof(p.peerRc) ) exit(1);
  }
  ch = 'r';
  if( write(p.toPeer[1], &ch, 1)!=1
   || read(p.fromPeer[0], &retryRc, sizeof(retryRc))!=sizeof(retryRc) ) exit(1);
  check("add_flush_peer_retry", retryRc==SQLITE_OK);
  waitpid(pid, &status, 0);
  check("add_flush_peer_exit", WIFEXITED(status) && WEXITSTATUS(status)==0);
  close(p.toPeer[0]); close(p.toPeer[1]);
  close(p.fromPeer[0]); close(p.fromPeer[1]);
  check("add_flush_followup_clean", execSql(db, "SELECT dolt_clean()")==SQLITE_OK);
  sqlite3_close(db);
  sqlite3_open(zPath, &db);
  snprintf(zCheck, sizeof(zCheck), "process_peer_write_kept_add_%s_%d", zArg, boundary);
  check(zCheck, strcmp(queryText(db, "SELECT count(*) FROM u WHERE x=1"), "1")==0);
  check("add_flush_dirty_kept", strcmp(queryText(db, "SELECT count(*) FROM t WHERE id=3"), "1")==0);
  check("add_flush_head_unchanged", strcmp(queryText(db, "SELECT dolt_hashof('HEAD')"), zHead)==0);
  check("add_flush_staged_commit", execSql(db, "SELECT dolt_commit('-m','staged')")==SQLITE_OK);
  check("add_flush_staged_t", strcmp(queryText(db, "SELECT count(*) FROM dolt_at_t('HEAD') WHERE id=3"), "1")==0);
  check("add_flush_unstaged_u", strcmp(queryText(db, "SELECT count(*) FROM dolt_at_u('HEAD')"), "0")==0);
  check("add_flush_peer_kept_after_commit", strcmp(queryText(db, "SELECT count(*) FROM u WHERE x=1"), "1")==0);
  check("add_flush_integrity", strcmp(queryText(db, "PRAGMA integrity_check"), "ok")==0);
  sqlite3_close(db);
  remove(zPath);
}

static int fireApplyInstallPeer(unsigned event, void *arg, void *stmt, void *elapsed){
  AddFlushPeer *p = (AddFlushPeer*)arg;
  const char *sql = sqlite3_sql((sqlite3_stmt*)stmt);
  char ch = 'g';
  (void)elapsed;
  if( event!=SQLITE_TRACE_PROFILE || p->fired || !sql
   || sqlite3_stricmp(sql, "COMMIT")!=0 ) return 0;
  p->fired = 1;
  p->txnState = sqlite3_txn_state(sqlite3_db_handle(stmt), "main");
  if( write(p->toPeer[1], &ch, 1)!=1
   || read(p->fromPeer[0], &p->peerRc, sizeof(p->peerRc))!=sizeof(p->peerRc) ){
    p->fired = -1;
  }
  return 0;
}

static void test_process_apply_clean_peer(
  int revert,
  int commitPeer,
  int afterInstall
){
  sqlite3 *db = 0;
  AddFlushPeer p;
  char zPath[300], zCheck[128], zHead[64], zPeerHead[64];
  const char *zOp = revert ? "revert" : "cherry_pick";
  const char *zExpected = revert ? "1,4,99" : "1,3,4,99";
  pid_t pid;
  int status = 0, rc;
  char ch = 'r';

  snprintf(zPath, sizeof(zPath), "%s/apply-clean.db", zDir);
  remove(zPath);
  check("apply_clean_open", sqlite3_open(zPath, &db)==SQLITE_OK);
  check("apply_clean_seed", execSql(db,
      "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
      "INSERT INTO t VALUES(1,'base');"
      "SELECT dolt_commit('-Am','base')")==SQLITE_OK);
  if( revert ){
    check("apply_clean_revert_seed", execSql(db,
        "CREATE INDEX tv ON t(v);"
        "SELECT dolt_commit('-am','index');"
        "DROP INDEX tv;"
        "SELECT dolt_commit('-am','drop index')")==SQLITE_OK);
  }else{
    check("apply_clean_pick_seed", execSql(db,
        "SELECT dolt_checkout('-b','feat');"
        "CREATE INDEX tv ON t(v);"
        "INSERT INTO t VALUES(2,'feat');"
        "SELECT dolt_commit('-am','index');"
        "SELECT dolt_checkout('main');"
        "INSERT INTO t VALUES(3,'main');"
        "SELECT dolt_commit('-am','main')")==SQLITE_OK);
  }
  snprintf(zHead, sizeof(zHead), "%s", queryText(db, "SELECT dolt_hashof('HEAD')"));
  check("apply_clean_seed_close", sqlite3_close(db)==SQLITE_OK);
  memset(&p, 0, sizeof(p));
  p.nSkipScan = revert;
  memset(zPeerHead, 0, sizeof(zPeerHead));
  if( pipe(p.toPeer) || pipe(p.fromPeer) ){ perror("pipe"); exit(1); }
  pid = fork();
  if( pid<0 ){ perror("fork"); exit(1); }
  if( pid==0 ){
    sqlite3 *peer = 0;
    int peerRc = sqlite3_open(zPath, &peer);
    char zTip[64] = {0};
    if( read(p.toPeer[0], &ch, 1)!=1 ) _exit(1);
    if( peerRc==SQLITE_OK ){
      peerRc = execSql(peer, "INSERT INTO t VALUES(99,'peer')");
    }
    if( write(p.fromPeer[1], &peerRc, sizeof(peerRc))!=sizeof(peerRc) ) _exit(1);
    if( read(p.toPeer[0], &ch, 1)!=1 ) _exit(1);
    sqlite3_busy_timeout(peer, 5000);
    if( (peerRc&255)==SQLITE_BUSY ){
      const char *zExpectedTip = revert ? "Revert \"drop index\"" : "index";
      peerRc = SQLITE_OK;
      if( afterInstall
       && strcmp(queryText(peer, "SELECT message FROM dolt_log LIMIT 1"),
                 zExpectedTip)!=0 ) peerRc = SQLITE_ERROR;
      if( peerRc==SQLITE_OK && afterInstall && commitPeer ){
        peerRc = execSql(peer,
            "INSERT INTO t VALUES(98,'peer commit');"
            "SELECT dolt_commit('-am','peer')");
      }
      if( peerRc==SQLITE_OK ){
        peerRc = execSql(peer, "INSERT INTO t VALUES(99,'peer')");
      }
      if( peerRc==SQLITE_OK && commitPeer && !afterInstall ){
        peerRc = execSql(peer, "SELECT dolt_commit('-am','peer')");
      }
      if( peerRc==SQLITE_OK && afterInstall ){
        peerRc = execSql(peer, "SELECT dolt_reset()");
      }
    }else{
      peerRc = SQLITE_ERROR;
    }
    snprintf(zTip, sizeof(zTip), "%s", queryText(peer, "SELECT dolt_hashof('HEAD')"));
    if( write(p.fromPeer[1], &peerRc, sizeof(peerRc))!=sizeof(peerRc) ) _exit(1);
    if( write(p.fromPeer[1], zTip, sizeof(zTip))!=sizeof(zTip) ) _exit(1);
    sqlite3_close(peer);
    _exit(0);
  }
  check("apply_clean_parent_open", sqlite3_open(zPath, &db)==SQLITE_OK);
  sqlite3_busy_timeout(db, 5000);
  check("apply_clean_initial_rows", strcmp(queryText(db,
      "SELECT count(*) FROM t"), revert ? "1" : "2")==0);
  sqlite3_trace_v2(db, SQLITE_TRACE_PROFILE,
      afterInstall ? fireApplyInstallPeer : fireAddFlushPeer, &p);
  rc = execSql(db, revert ? "SELECT dolt_revert('HEAD')"
                           : "SELECT dolt_cherry_pick('feat')");
  sqlite3_trace_v2(db, 0, 0, 0);
  check("apply_clean_hook_fired", p.fired==1);
  check("apply_clean_peer_refused", (p.peerRc&255)==SQLITE_BUSY);
  check("apply_clean_scan_holds_txn", p.txnState==SQLITE_TXN_WRITE);
  check("apply_published_success", rc==SQLITE_OK);
  if( !p.fired && write(p.toPeer[1], &ch, 1)!=1 ) exit(1);
  if( write(p.toPeer[1], &ch, 1)!=1
   || read(p.fromPeer[0], &p.peerRc, sizeof(p.peerRc))!=sizeof(p.peerRc)
   || read(p.fromPeer[0], zPeerHead, sizeof(zPeerHead))!=sizeof(zPeerHead) ) exit(1);
  check("apply_clean_peer_acknowledged", p.peerRc==SQLITE_OK);
  waitpid(pid, &status, 0);
  check("apply_clean_peer_exit", WIFEXITED(status) && WEXITSTATUS(status)==0);
  close(p.toPeer[0]); close(p.toPeer[1]);
  close(p.fromPeer[0]); close(p.fromPeer[1]);

  check("apply_clean_followup", execSql(db,
      "INSERT INTO t(id,v) VALUES(4,'after');"
      "ALTER TABLE t ADD COLUMN extra TEXT;"
      "SELECT dolt_merge('main');"
      "SELECT dolt_reset();"
      "SELECT dolt_branch('after');"
      "SELECT dolt_commit('-a','--amend','-m','after')")==SQLITE_OK);
  check("apply_clean_followup_peer_visible", strcmp(queryText(db,
      "SELECT count(*) FROM t WHERE id=99"), "1")==0);
  check("apply_clean_close", sqlite3_close(db)==SQLITE_OK);
  check("apply_clean_reopen", sqlite3_open(zPath, &db)==SQLITE_OK);
  if( afterInstall ){
    snprintf(zCheck, sizeof(zCheck), "process_apply_%s_%d_install_peer_kept",
        zOp, commitPeer);
    zExpected = revert ? (commitPeer ? "1,4,98,99" : "1,4,99")
                       : (commitPeer ? "1,2,3,4,98,99" : "1,2,3,4,99");
  }else{
    snprintf(zCheck, sizeof(zCheck), "process_apply_%s_%d_peer_kept", zOp, commitPeer);
    zExpected = revert ? "1,4,99" : "1,2,3,4,99";
  }
  check(zCheck, strcmp(queryText(db,
      "SELECT group_concat(id) FROM (SELECT id FROM t ORDER BY id)"),
      zExpected)==0);
  check("apply_clean_peer_in_head", strcmp(queryText(db,
      "SELECT count(*) FROM dolt_at_t('HEAD') WHERE id=99"), "1")==0);
  check("apply_install_index_kept", strcmp(queryText(db,
      "SELECT count(*) FROM sqlite_master WHERE name='tv'"), "1")==0);
  check("apply_clean_original_tip_kept", strcmp(queryText(db,
      "SELECT dolt_hashof('after')"), zPeerHead)==0);
  check("apply_clean_peer_commit_state", strcmp(zHead,zPeerHead)!=0);
  check("apply_clean_integrity", strcmp(queryText(db, "PRAGMA integrity_check"), "ok")==0);
  sqlite3_close(db);
  remove(zPath);
}

int main(void){
  int i;
  setvbuf(stdout, 0, _IOLBF, 0);
  printf("=== Peer write survives each VC operation ===\n\n");
  snprintf(zDir, sizeof(zDir), DOLTLITE_TEST_TMPDIR "/mp_peer_write_%d", (int)getpid());
  mkdir(zDir, 0700);
  snprintf(zDb, sizeof(zDb), "%s/db.db", zDir);
  snprintf(zRemote, sizeof(zRemote), "%s/template_remote_run.db", zDir);
  snprintf(zInto, sizeof(zInto), "%s/into.db", zDir);
  buildTemplate();
  /* The template recorded origin at the template remote's path; runs read a
  ** fresh copy of it, and the remote itself is copied back over it. */
  snprintf(zRemote, sizeof(zRemote), "%s", zTemplateRemote);
  snprintf(zTemplateRemote, sizeof(zTemplateRemote), "%s/template_remote_saved.db", zDir);
  check("template_remote_saved", copyFile(zRemote, zTemplateRemote));

  for(i=0; i<(int)(sizeof(aOp)/sizeof(aOp[0])); i++){
    const Op *p = &aOp[i];
    Tally aT[3][2];
    int a, mode, k, nSteps = 0, nLost = 0;
    char zName[96];
    memset(aT, 0, sizeof(aT));
    for(a=0; a<2; a++){
      if( p->bCommitOnly && a==PEER_WRITE ) continue;
      runStale(p, a, &aT[MODE_STALE][a]);
      for(k=1; k<5000 && runMidOp(p, a, k, &aT[MODE_MIDOP][a]); k++){}
      if( k-1>nSteps ) nSteps = k-1;
    }
    runTxn(p, &aT[MODE_TXN][PEER_WRITE]);
    printf("  %-18s steps=%-4d", p->zName, nSteps);
    for(mode=0; mode<3; mode++){
      for(a=0; a<(mode==MODE_TXN ? 1 : 2); a++){
        Tally *t = &aT[mode][a];
        printf(" %s/%s=%d/%d/%d", azMode[mode], azPeerAction[a],
               t->nAcked, t->nOpOk, t->nLost);
        nLost += t->nLost;
      }
    }
    printf("\n");
    check(p->zCheck, nLost==0);
    /* A matrix row where the operation never ran next to the peer's write
    ** would pass without testing anything. */
    snprintf(zName, sizeof(zName), "op_ran_against_peer_%s", p->zName);
    check(zName, aT[MODE_MIDOP][PEER_WRITE].nOpOk>0
              || aT[MODE_MIDOP][PEER_COMMIT].nOpOk>0);
  }

  for(i=0; i<4; i++){
    test_process_apply_clean_peer(i/2, i%2, 0);
    test_process_apply_clean_peer(i/2, i%2, 1);
    test_process_add_flush_lock(".", i);
    test_process_add_flush_lock("t", i);
    test_process_add_flush_lock("-A", i);
  }

  test_process_peer_write_kept("reset_table", 0, "SELECT dolt_reset('t')");
  test_process_peer_write_kept("clean", 0, "SELECT dolt_clean()");
  /* A schema change makes the checkout drop and recreate the table. */
  test_process_peer_write_kept("checkout_schema",
      "SELECT dolt_commit('-am','base');"
      "INSERT INTO t VALUES(4,'dirty');"
      "ALTER TABLE t ADD COLUMN c TEXT", "SELECT dolt_checkout('t')");

  remove(zDb); remove(zInto); remove(zRemote); remove(zTemplate);
  remove(zTemplateRemote);
  printf("\n=== Results: %d passed, %d failed out of %d tests ===\n",
         nPass, nFail, nPass+nFail);
  return nFail>0 ? 1 : 0;
}
