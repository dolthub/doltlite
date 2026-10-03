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

/* main has t and u, an extra commit c2 tagged c2 (so f diverges), branch f with a
** commit on u, branch ff one commit ahead of main, branch f2, tag v0,
** remote o0, and remote origin, a file remote that holds main plus one
** newer commit on u. */
static void buildTemplate(void){
  sqlite3 *db = 0;
  sqlite3 *client = 0;
  char sql[1024];
  char zClient[300];
  snprintf(zTemplate, sizeof(zTemplate), "%s/template.db", zDir);
  snprintf(zTemplateRemote, sizeof(zTemplateRemote), "%s/template_remote.db", zDir);
  snprintf(zClient, sizeof(zClient), "%s/template_client.db", zDir);
  sqlite3_open(zTemplate, &db);
  snprintf(sql, sizeof(sql),
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    "INSERT INTO t VALUES(1,'base');"
    "CREATE TABLE u(x INTEGER PRIMARY KEY);"
    "SELECT dolt_commit('-Am','init');"
    "SELECT dolt_tag('v0');"
    "SELECT dolt_branch('f2');"
    "SELECT dolt_remote('add','o0','file:///nonexistent-peer-write');"
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
};

static const Op aOp[] = {
  { "add", 0, "SELECT dolt_add('.')", "peer_write_kept_add" },
  { "commit", "INSERT INTO u VALUES(77)", "SELECT dolt_commit('-Am','mine')",
    "peer_write_kept_commit" },
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
  { "reset_table", 0, "SELECT dolt_reset('u')", "peer_write_kept_reset_table" },
  { "checkout_branch", 0, "SELECT dolt_checkout('f')",
    "peer_write_kept_checkout_branch" },
  { "checkout_new", 0, "SELECT dolt_checkout('-b','f5')",
    "peer_write_kept_checkout_new" },
  { "checkout_table", 0, "SELECT dolt_checkout('u')",
    "peer_write_kept_checkout_table" },
  { "merge_ff", 0, "SELECT dolt_merge('ff')", "peer_write_kept_merge_ff" },
  { "merge", 0, "SELECT dolt_merge('f')", "peer_write_kept_merge" },
  { "merge_squash", 0, "SELECT dolt_merge('--squash','f')",
    "peer_write_kept_merge_squash" },
  { "cherry_pick", 0, "SELECT dolt_cherry_pick('f')",
    "peer_write_kept_cherry_pick" },
  { "revert", 0, "SELECT dolt_revert('c2')", "peer_write_kept_revert" },
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

static void opSql(const Op *p, char *zOut, int nOut){
  if( strstr(p->zSql, "%s") ){
    snprintf(zOut, nOut, p->zSql, zInto);
  }else{
    snprintf(zOut, nOut, "%s", p->zSql);
  }
}

static int runOpSql(sqlite3 *db, const char *zSql, int *pBusy){
  char *zErr = 0;
  int rc = sqlite3_exec(db, zSql, 0, 0, &zErr);
  if( pBusy ){
    *pBusy = rc!=SQLITE_OK
          && ((rc&0xff)==SQLITE_BUSY || (zErr && isBusy(zErr)));
  }
  sqlite3_free(zErr);
  return rc;
}

/* Acknowledged once the peer's INSERT autocommits; the commit is extra. */
static int peerAct(sqlite3 *peer, int action){
  if( execSql(peer, "INSERT INTO t VALUES(900,'peer')")!=SQLITE_OK ) return 0;
  if( action==PEER_COMMIT ) execSql(peer, "SELECT dolt_commit('-am','peer')");
  return 1;
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
  acked = peerAct(peer, action);
  opOk = runOpSql(db, zSql, 0)==SQLITE_OK;
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
  int fireAt;
  int nCalls;
  int fired;
  int acked;
};

static int fireMidOp(void *arg){
  MidOp *m = (MidOp*)arg;
  if( ++m->nCalls==m->fireAt && !m->fired ){
    m->fired = 1;
    m->acked = peerAct(m->peer, m->action);
  }
  return 0;
}

/* Returns 0 once k is past the operation's last step. A run refused as
** busy because the peer moved is retried, as an application would. */
static int runMidOp(const Op *p, int action, int k, Tally *t){
  sqlite3 *db = 0;
  MidOp m;
  char zSql[512];
  int rc, busy = 0;
  if( !freshCopy() ){ t->nLost++; return 0; }
  opSql(p, zSql, sizeof(zSql));
  memset(&m, 0, sizeof(m));
  sqlite3_open(zDb, &db);
  sqlite3_open(zDb, &m.peer);
  sqlite3_busy_timeout(db, 5000);
  if( p->zPre ) execSql(db, p->zPre);
  m.action = action;
  m.fireAt = k;
  sqlite3_progress_handler(db, 1, fireMidOp, &m);
  rc = runOpSql(db, zSql, &busy);
  sqlite3_progress_handler(db, 0, 0, 0);
  if( m.fired && busy ) rc = runOpSql(db, zSql, 0);
  execSql(db, "INSERT INTO t VALUES(3,'mine')");
  sqlite3_close(m.peer);
  sqlite3_close(db);
  if( !m.fired ) return 0;
  tallyRun(t, m.acked, rc==SQLITE_OK,
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
  opOk = runOpSql(db, zSql, 0)==SQLITE_OK;
  committed = execSql(peer, "COMMIT")==SQLITE_OK;
  if( !committed ) execSql(peer, "ROLLBACK");
  execSql(db, "INSERT INTO t VALUES(3,'mine')");
  sqlite3_close(peer);
  sqlite3_close(db);
  tallyRun(t, committed, opOk,
           committed ? peerRowKept(p->zName, "txn", PEER_WRITE, 0) : 1);
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

  remove(zDb); remove(zInto); remove(zRemote); remove(zTemplate);
  remove(zTemplateRemote);
  printf("\n=== Results: %d passed, %d failed out of %d tests ===\n",
         nPass, nFail, nPass+nFail);
  return nFail>0 ? 1 : 0;
}
