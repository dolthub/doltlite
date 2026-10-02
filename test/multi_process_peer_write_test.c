/*
** A peer connection's acknowledged write must survive whatever version
** control operation this connection runs. Each operation runs on main
** against a peer that autocommits a row to main in three ways:
**
**   stale:  the peer's write is acknowledged before the operation, after
**           this connection last read, so the operation starts stale;
**   midop:  the peer's write fires from the operation's progress handler at
**           every step in turn; an operation refused as busy is retried;
**   txn:    the peer holds an open transaction across the operation and
**           commits after it.
**
** A fresh connection then checks that every acknowledged peer row is on
** main and that the store passes integrity_check. Two connections in one
** process contend through the VFS lock exactly as two processes do.
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
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

/* main has t and u, an extra commit c2 (so f diverges), branch f with a
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
  const char *zSql;
  const char *zCheck;
};

static const Op aOp[] = {
  { "add", "SELECT dolt_add('.')", "peer_write_kept_add" },
  { "commit", "SELECT dolt_commit('-Am','mine')", "peer_write_kept_commit" },
  { "branch_create", "SELECT dolt_branch('x')", "peer_write_kept_branch_create" },
  { "branch_delete", "SELECT dolt_branch('-d','f2')", "peer_write_kept_branch_delete" },
  { "branch_rename", "SELECT dolt_branch('-m','f2','f3')", "peer_write_kept_branch_rename" },
  { "tag_create", "SELECT dolt_tag('v1')", "peer_write_kept_tag_create" },
  { "tag_delete", "SELECT dolt_tag('-d','v0')", "peer_write_kept_tag_delete" },
  { "remote_add", "SELECT dolt_remote('add','o1','file:///nonexistent-peer-write-1')",
    "peer_write_kept_remote_add" },
  { "remote_remove", "SELECT dolt_remote('remove','o0')", "peer_write_kept_remote_remove" },
  { "reset_soft", "SELECT dolt_reset('--soft')", "peer_write_kept_reset_soft" },
  { "reset_table", "SELECT dolt_reset('u')", "peer_write_kept_reset_table" },
  { "checkout_branch", "SELECT dolt_checkout('f')", "peer_write_kept_checkout_branch" },
  { "checkout_table", "SELECT dolt_checkout('u')", "peer_write_kept_checkout_table" },
  { "merge_ff", "SELECT dolt_merge('ff')", "peer_write_kept_merge_ff" },
  { "merge", "SELECT dolt_merge('f')", "peer_write_kept_merge" },
  { "cherry_pick", "SELECT dolt_cherry_pick('f')", "peer_write_kept_cherry_pick" },
  { "revert", "SELECT dolt_revert('HEAD')", "peer_write_kept_revert" },
  { "clean", "SELECT dolt_clean()", "peer_write_kept_clean" },
  { "fetch", "SELECT dolt_fetch('origin')", "peer_write_kept_fetch" },
  { "pull", "SELECT dolt_pull('origin','main')", "peer_write_kept_pull" },
  { "gc", "SELECT dolt_gc()", "peer_write_kept_gc" },
  { "vacuum", "VACUUM", "peer_write_kept_vacuum" },
  { "vacuum_into", "VACUUM INTO '%s'", "peer_write_kept_vacuum_into" },
};

static void opSql(const Op *p, char *zOut, int nOut){
  if( strstr(p->zSql, "%s") ){
    snprintf(zOut, nOut, p->zSql, zInto);
  }else{
    snprintf(zOut, nOut, "%s", p->zSql);
  }
}

/* The peer's row is on main and the store is intact. */
static int peerRowKept(const char *zOp, const char *zMode, int k){
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
  if( !ok ) fprintf(stderr, "%s/%s step %d: %s\n", zOp, zMode, k, z);
  sqlite3_close(db);
  return ok;
}

static int runStale(const Op *p){
  sqlite3 *db = 0, *peer = 0;
  char zSql[512];
  int ok = 1;
  if( !freshCopy() ) return 0;
  opSql(p, zSql, sizeof(zSql));
  sqlite3_open(zDb, &db);
  sqlite3_open(zDb, &peer);
  sqlite3_busy_timeout(db, 5000);
  sqlite3_busy_timeout(peer, 5000);
  queryText(db, "SELECT count(*) FROM t");
  if( execSql(peer, "INSERT INTO t VALUES(900,'peer')")==SQLITE_OK ){
    queryText(db, zSql);
    execSql(db, "INSERT INTO t VALUES(3,'mine')");
    sqlite3_close(peer);
    sqlite3_close(db);
    ok = peerRowKept(p->zName, "stale", 0);
  }else{
    sqlite3_close(peer);
    sqlite3_close(db);
  }
  return ok;
}

typedef struct MidOp MidOp;
struct MidOp {
  sqlite3 *peer;
  int fireAt;
  int nCalls;
  int fired;
  int acked;
};

static int fireMidOp(void *arg){
  MidOp *m = (MidOp*)arg;
  if( ++m->nCalls==m->fireAt && !m->fired ){
    m->fired = 1;
    m->acked = execSql(m->peer, "INSERT INTO t VALUES(900,'peer')")==SQLITE_OK;
  }
  return 0;
}

/* Returns -1 once k is past the operation's last step. */
static int runMidOp(const Op *p, int k, int *pAcked){
  sqlite3 *db = 0;
  MidOp m;
  char zSql[512], zRes[512];
  *pAcked = 0;
  if( !freshCopy() ) return 0;
  opSql(p, zSql, sizeof(zSql));
  memset(&m, 0, sizeof(m));
  sqlite3_open(zDb, &db);
  sqlite3_open(zDb, &m.peer);
  sqlite3_busy_timeout(db, 5000);
  m.fireAt = k;
  sqlite3_progress_handler(db, 1, fireMidOp, &m);
  snprintf(zRes, sizeof(zRes), "%s", queryText(db, zSql));
  sqlite3_progress_handler(db, 0, 0, 0);
  if( m.fired && isBusy(zRes) ) queryText(db, zSql);
  execSql(db, "INSERT INTO t VALUES(3,'mine')");
  sqlite3_close(m.peer);
  sqlite3_close(db);
  if( !m.fired ) return -1;
  *pAcked = m.acked;
  return m.acked ? peerRowKept(p->zName, "midop", k) : 1;
}

static int runTxn(const Op *p){
  sqlite3 *db = 0, *peer = 0;
  char zSql[512];
  int committed;
  if( !freshCopy() ) return 0;
  opSql(p, zSql, sizeof(zSql));
  sqlite3_open(zDb, &db);
  sqlite3_open(zDb, &peer);
  sqlite3_busy_timeout(db, 200);
  sqlite3_busy_timeout(peer, 5000);
  execSql(peer, "BEGIN; INSERT INTO t VALUES(900,'peer')");
  queryText(db, zSql);
  committed = execSql(peer, "COMMIT")==SQLITE_OK;
  if( !committed ) execSql(peer, "ROLLBACK");
  execSql(db, "INSERT INTO t VALUES(3,'mine')");
  sqlite3_close(peer);
  sqlite3_close(db);
  return committed ? peerRowKept(p->zName, "txn", 0) : 1;
}

int main(void){
  int i;
  setvbuf(stdout, 0, _IOLBF, 0);
  printf("=== Peer write survives each VC operation ===\n\n");
  snprintf(zDir, sizeof(zDir), "/tmp/mp_peer_write_%d", (int)getpid());
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
    int k, nAcked = 0, nBad = 0;
    int stale = runStale(p);
    int txn = runTxn(p);
    for(k=1; k<5000; k++){
      int acked;
      int r = runMidOp(p, k, &acked);
      if( r<0 ) break;
      nAcked += acked;
      if( !r ) nBad++;
    }
    printf("  %-16s steps=%d peer_writes=%d bad_midop=%d stale=%s txn=%s\n",
           p->zName, k-1, nAcked, nBad, stale ? "ok" : "LOST", txn ? "ok" : "LOST");
    check(p->zCheck, stale && txn && nBad==0 && nAcked>0);
  }

  remove(zDb); remove(zInto); remove(zRemote); remove(zTemplate);
  remove(zTemplateRemote);
  printf("\n=== Results: %d passed, %d failed out of %d tests ===\n",
         nPass, nFail, nPass+nFail);
  return nFail>0 ? 1 : 0;
}
