/*
** Randomized multi-process, multi-connection version-control stress with a
** model checker. Workers run DML, DDL and dolt_* commands against shared
** branches and log every acknowledged row and commit; after each phase the
** parent checks that each acknowledged write is still present unless an op
** logged as destructive could have removed it.
**
**   vc_model_stress_test SEED DIR [NPROC NCONN NPHASE OPS]
**   vc_model_stress_test --check DIR [NPROC]
**
** Knobs (environment): VCMODEL_BUSY_MAX, VCMODEL_WEDGE_SEC,
** VCMODEL_STMT_WEDGE_SEC, VCMODEL_DESTRUCT (percent of destructive ops run),
** VCMODEL_FOCUS=<branch> (every connection on one branch),
** VCMODEL_TRIPWIRE (recheck own rows after every op), VCMODEL_ICHECK
** (integrity_check after every op), VCMODEL_CNT (log row counts per op),
** VCMODEL_NO_REVERT, VCMODEL_NO_CP, VCMODEL_NO_CLEAN, VCMODEL_NO_RESET_TABLE,
** VCMODEL_TRIP_NORETRY, VCMODEL_DBGEX (print excused rows and commits),
** VCMODEL_STALECHK (log a connection whose view lags a fresh reader's),
** VCMODEL_PRELOG (log each statement before it runs).
** VCMODEL_CLUSTERED=1 uses an INT PRIMARY KEY instead of a rowid alias.
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <unistd.h>
#include <signal.h>
#include <errno.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/time.h>
#include <poll.h>
#include "sqlite3.h"

static char zDir[512], zDb[600];
static int gBusyMax = 300;
static int gLastStepConn = 0;
static int gDestructPct = 40;
static int gSeed, nProc = 3, nConn = 3, nPhase = 4, nOps = 150;
static unsigned long long rngState;

static unsigned rnd(void){
  rngState ^= rngState << 13; rngState ^= rngState >> 7; rngState ^= rngState << 17;
  return (unsigned)(rngState >> 11);
}
static int rn(int n){ return n<=0 ? 0 : (int)(rnd() % (unsigned)n); }

static long long nowNs(void){
  struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
  return (long long)ts.tv_sec*1000000000LL + ts.tv_nsec;
}

static FILE *gLog;
static int gW;
static void logf_(const char *fmt, ...){
  va_list ap; va_start(ap, fmt); vfprintf(gLog, fmt, ap); va_end(ap);
  fputc('\n', gLog); fflush(gLog);
}

typedef struct Conn {
  sqlite3 *db;
  int inTxn;
  int nTxnOps;
  int tainted;
  int nPend;
  long long aPendId[256];
  long long aPendS[256];
  int nSp;
  int aSpMark[16];
  char zOpenAs[64];
} Conn;

static Conn aC[8];
static long long idCtr = 0;
static long long aMine[4096]; static int nMine = 0;
static int ctrMsg = 0, ctrTag = 0, ctrBr = 0, ctrCol = 0, ctrIdx = 0;
static char azMyTags[256][32]; static int nMyTags = 0;
static char azMyBr[256][32]; static int nMyBr = 0;
static const char *azBr[] = { "main", "b1", "b2", "b3" };

static int isFatal(int rc, const char *z){
  int p = rc & 0xff;
  if( p==SQLITE_CORRUPT || p==SQLITE_NOTFOUND || p==SQLITE_MISUSE
   || p==SQLITE_INTERNAL || p==SQLITE_FORMAT || p==SQLITE_IOERR
   || p==SQLITE_NOMEM || p==SQLITE_CANTOPEN || p==SQLITE_PROTOCOL ) return 1;
  if( z && (strstr(z,"malformed") || strstr(z,"corrupt") || strstr(z,"misuse")
         || strstr(z,"internal") || strstr(z,"not found in store")
         || strstr(z,"missing chunk")) ) return 1;
  return 0;
}
static int isBusy(int rc, const char *z){
  if( (rc&0xff)==SQLITE_BUSY || (rc&0xff)==SQLITE_LOCKED ) return 1;
  if( z && (strstr(z,"busy") || strstr(z,"locked") || strstr(z,"retry")
         || strstr(z,"another connection")) ) return 1;
  return 0;
}

static int gStmtWedgeSec = 60;
static void sampleStacks(pid_t pid);
static void onAlarm(int sig){
  (void)sig;
  logf_("WEDGE\tstatement exceeded %d s", gStmtWedgeSec);
  sampleStacks(getpid());
  _exit(9);
}
static int otherLocalTxn(Conn *c){
  int i;
  for(i=0;i<nConn;i++) if( &aC[i]!=c && !sqlite3_get_autocommit(aC[i].db) ) return 1;
  return 0;
}

static char gErr[1024];
static struct Conn *gLastConn;
static void curBranch(Conn *c, char *z, int n);
static int gLastRc;
static char gRes[256];
static int resCb(void *p, int n, char **v, char **cn){
  (void)p; (void)cn;
  if( n>0 && v[0] ) snprintf(gRes, sizeof(gRes), "%s", v[0]);
  return 0;
}
static int ex(Conn *c, const char *zOp, const char *zSql){
  char *zE = 0;
  int rc;
  long long tS = nowNs();
  if( getenv("VCMODEL_PRELOG") ) logf_("PRE\t%lld\tc%d\t%s\t%s", tS, (int)(c-aC), zOp, zSql);
  alarm(gStmtWedgeSec);
  gRes[0] = 0;
  rc = sqlite3_exec(c->db, zSql, resCb, 0, &zE);
  alarm(0);
  gLastRc = rc;
  gLastConn = c;
  snprintf(gErr, sizeof(gErr), "%s", zE ? zE : (rc ? sqlite3_errmsg(c->db) : ""));
  if( rc ){
    char clean[sizeof(gErr)]; int i;
    snprintf(clean, sizeof(clean), "%s", gErr);
    for(i=0; clean[i]; i++) if( clean[i]=='\t' || clean[i]=='\n' ) clean[i]=' ';
    logf_("E\t%d\t%s\t%s\t%s\t%lld\t%lld\tc%d\t%s", rc, zOp, clean, isFatal(rc, gErr) ? "FATAL" : "", tS, nowNs(), (int)(c-aC), zSql);
    if( isFatal(rc, gErr) ){
      char br[128]; sqlite3 *f = 0; sqlite3_stmt *st = 0; int exists = -1;
      logf_("FATALSQL\t%s", zSql);
      curBranch(c, br, sizeof(br));
      if( sqlite3_open(zDb, &f)==SQLITE_OK ){
        sqlite3_busy_timeout(f, 2000);
        if( sqlite3_prepare_v2(f, "SELECT count(*) FROM dolt_branches WHERE name=?", -1, &st, 0)==SQLITE_OK ){
          sqlite3_bind_text(st, 1, br, -1, SQLITE_TRANSIENT);
          if( sqlite3_step(st)==SQLITE_ROW ) exists = sqlite3_column_int(st, 0);
        }
        sqlite3_finalize(st);
      }
      sqlite3_close(f);
      logf_("FATALCTX\t%s\t%d", br, exists);
    }
  }else if( strncmp(zSql, "INSERT INTO w", 13)!=0 ){
    logf_("S\t%lld\t%lld\tc%d\t%s", tS, nowNs(), (int)(c-aC), zSql);
  }
  sqlite3_free(zE);
  return rc;
}

static void curBranch(Conn *c, char *z, int n){
  int tries;
  z[0] = 0;
  /* A peer's dolt_gc holds the lock long enough to fail active_branch(). */
  for(tries=0; tries<50 && !z[0]; tries++){
    sqlite3_stmt *s = 0;
    if( tries ) sqlite3_sleep(20);
    if( sqlite3_prepare_v2(c->db, "SELECT active_branch()", -1, &s, 0)==SQLITE_OK ){
      if( sqlite3_step(s)==SQLITE_ROW && sqlite3_column_text(s,0) )
        snprintf(z, n, "%s", (const char*)sqlite3_column_text(s,0));
    }
    sqlite3_finalize(s);
  }
  if( !z[0] ) snprintf(z, n, "?");
}

static long long aTrip[8192]; static char aTripBr[8192][48]; static int nTrip = 0;
static void tripAdd(long long id, const char *zBr){
  if( nTrip>=8192 || !strcmp(zBr, "?") ) return;
  aTrip[nTrip] = id;
  snprintf(aTripBr[nTrip], sizeof(aTripBr[0]), "%s", zBr);
  nTrip++;
}
static void tripRemove(long long id){
  int i;
  for(i=0;i<nTrip;i++){
    if( aTrip[i]==id ){
      nTrip--;
      aTrip[i] = aTrip[nTrip];
      memcpy(aTripBr[i], aTripBr[nTrip], sizeof(aTripBr[0]));
      return;
    }
  }
}
static long long newId(void){ return (long long)(gW+1)*100000000LL + (++idCtr); }

static void pendToUnknown(Conn *c){
  int i;
  for(i=0;i<c->nPend;i++) logf_("U\t%lld", c->aPendId[i]);
  c->nPend = 0; c->nSp = 0;
}

static void txnEnded(Conn *c){
  c->inTxn = 0; c->nPend = 0; c->nSp = 0; c->tainted = 0; c->nTxnOps = 0;
}

/* After a failing statement inside a txn: end the txn by rolling back. */
static void txnAbort(Conn *c){
  if( !sqlite3_get_autocommit(c->db) ){
    if( ex(c, "rollback", "ROLLBACK") ){
      pendToUnknown(c);
    }
  }
  if( !sqlite3_get_autocommit(c->db) ){
    logf_("NOTE\tstill-in-txn-after-rollback");
    pendToUnknown(c);
  }
  txnEnded(c);
}

static void txnCommit(Conn *c){
  char br[128];
  int i, tries;
  long long E;
  curBranch(c, br, sizeof(br));
  for(tries=0; tries<3; tries++){
    int rc = ex(c, "commit", "COMMIT");
    if( rc==SQLITE_OK ){
      E = nowNs();
      for(i=0;i<c->nPend;i++){
        if( c->tainted ) logf_("U\t%lld", c->aPendId[i]);
        else{ logf_("I\t%lld\t%s\t%lld\t%lld", c->aPendId[i], br, c->aPendS[i], E); tripAdd(c->aPendId[i], br); }
      }
      txnEnded(c);
      return;
    }
    if( sqlite3_get_autocommit(c->db) ){
      /* txn gone: rolled back by the error */
      txnEnded(c);
      return;
    }
    if( !isBusy(rc, gErr) ) break;
    usleep(20000);
  }
  txnAbort(c);
}

static void opInsert(Conn *c){
  char sql[512], br[128];
  long long id1 = newId(), id2 = rn(3)==0 ? newId() : 0, S, E;
  int rc, tries;
  if( id2 ) snprintf(sql, sizeof(sql), "INSERT INTO w(id,v) VALUES(%lld,'v%d'),(%lld,'v%d')", id1, rn(9), id2, rn(9));
  else snprintf(sql, sizeof(sql), "INSERT INTO w(id,v) VALUES(%lld,'v%d')", id1, rn(9));
  if( c->inTxn ){
    S = nowNs();
    rc = ex(c, "insert_txn", sql);
    if( rc==SQLITE_OK ){
      if( c->nPend < 250 ){
        c->aPendId[c->nPend] = id1; c->aPendS[c->nPend++] = S;
        if( id2 ){ c->aPendId[c->nPend] = id2; c->aPendS[c->nPend++] = S; }
      }
    }else{
      txnAbort(c);
    }
    return;
  }
  curBranch(c, br, sizeof(br));
  for(tries=0; tries<4; tries++){
    S = nowNs();
    rc = ex(c, "insert", sql);
    E = nowNs();
    if( rc==SQLITE_OK ){
      logf_("I\t%lld\t%s\t%lld\t%lld", id1, br, S, E);
      if( id2 ) logf_("I\t%lld\t%s\t%lld\t%lld", id2, br, S, E);
      tripAdd(id1, br); if( id2 ) tripAdd(id2, br);
      if( nMine<4096 ) aMine[nMine++] = id1;
      return;
    }
    if( !isBusy(rc, gErr) || otherLocalTxn(c) ) return;
    usleep(rn(20000));
  }
}

static void opUpdate(Conn *c){
  char sql[256];
  if( nMine==0 ) return;
  snprintf(sql, sizeof(sql), "UPDATE w SET v='u%d' WHERE id=%lld", rn(99), aMine[rn(nMine)]);
  if( ex(c, "update", sql) && c->inTxn ) txnAbort(c);
}

static void opDelete(Conn *c){
  char sql[256];
  long long id;
  if( nMine==0 ) return;
  id = aMine[rn(nMine)];
  snprintf(sql, sizeof(sql), "DELETE FROM w WHERE id=%lld", id);
  if( ex(c, "delete", sql) ){
    if( c->inTxn ) txnAbort(c);
    return;
  }
  logf_("D\t%lld", id);
  tripRemove(id);
}

static void opBegin(Conn *c){
  const char *z = rn(2) ? "BEGIN" : "BEGIN IMMEDIATE";
  if( c->inTxn ) return;
  if( ex(c, "begin", z)==SQLITE_OK ){
    c->inTxn = 1; c->nPend = 0; c->nSp = 0; c->tainted = 0; c->nTxnOps = 1 + rn(6);
  }
}

static void opSavepoint(Conn *c){
  char sql[64];
  if( !c->inTxn ) return;
  if( c->nSp<16 && (c->nSp==0 || rn(2)) ){
    snprintf(sql, sizeof(sql), "SAVEPOINT s%d", c->nSp);
    if( ex(c, "savepoint", sql)==SQLITE_OK ) c->aSpMark[c->nSp++] = c->nPend;
    else txnAbort(c);
  }else if( c->nSp>0 ){
    int k = rn(c->nSp);
    if( rn(2) ){
      snprintf(sql, sizeof(sql), "ROLLBACK TO s%d", k);
      if( ex(c, "rollback_to", sql)==SQLITE_OK ){
        c->nPend = c->aSpMark[k];
        c->nSp = k+1;
      }else txnAbort(c);
    }else{
      snprintf(sql, sizeof(sql), "RELEASE s%d", k);
      if( ex(c, "release", sql)==SQLITE_OK ){
        c->nSp = k;
      }else txnAbort(c);
    }
  }
}

/* Run a VC statement. flags: what to log on success (W working-destroy,
** K head-destroy, M history-rewrite, G global). Returns rc. */
#define FW 1
#define FK 2
#define FM 4
#define FG 8
#define FR 16
/* An excuse on an unknown branch must still cover the branch the op hit. */
static int excuseFlags(int flags, const char *zBr){
  return strcmp(zBr, "?") ? flags : (flags | FG);
}
static char gVictim[128];
/* Run zSql; when it rewrites history and no txn is open, half the time
** capture the HEAD it replaces under BEGIN IMMEDIATE so the model knows
** exactly which commit may legitimately vanish. */
static long long gVictimS;
static void readVictim(Conn *c){
  sqlite3_stmt *st = 0;
  snprintf(gVictim, sizeof(gVictim), "*");
  if( sqlite3_prepare_v2(c->db, "SELECT count(*) FROM dolt_commit_ancestors WHERE commit_hash=dolt_hashof('HEAD')", -1, &st, 0)==SQLITE_OK
   && sqlite3_step(st)==SQLITE_ROW && sqlite3_column_int(st,0)>1 ){
    sqlite3_finalize(st);
    return;
  }
  sqlite3_finalize(st); st = 0;
  if( sqlite3_prepare_v2(c->db, "SELECT message FROM dolt_log WHERE commit_hash=dolt_hashof('HEAD')", -1, &st, 0)==SQLITE_OK
   && sqlite3_step(st)==SQLITE_ROW && sqlite3_column_text(st,0) ){
    snprintf(gVictim, sizeof(gVictim), "%s", (const char*)sqlite3_column_text(st,0));
  }
  sqlite3_finalize(st);
}
static int runOp(Conn *c, const char *zOp, const char *zSql, int wantVictim){
  int rc;
  snprintf(gVictim, sizeof(gVictim), "*");
  gVictimS = nowNs();
  if( !wantVictim ) return ex(c, zOp, zSql);
  if( c->inTxn ){
    return ex(c, zOp, zSql);
  }
  if( rn(2) ){
    readVictim(c);
    return ex(c, zOp, zSql);
  }
  rc = ex(c, "begin_victim", "BEGIN IMMEDIATE");
  if( rc ) return rc;
  gVictimS = nowNs();
  readVictim(c);
  rc = ex(c, zOp, zSql);
  {
    int rc0 = gLastRc; char e0[1024]; snprintf(e0, sizeof(e0), "%s", gErr);
    if( !sqlite3_get_autocommit(c->db) ){
      if( rc ) ex(c, "rollback_victim", "ROLLBACK");
      else if( ex(c, "commit_victim", "COMMIT") ) ex(c, "rollback_victim", "ROLLBACK");
    }
    gLastRc = rc0; snprintf(gErr, sizeof(gErr), "%s", e0);
  }
  return rc;
}

static void staleProbe(Conn *c, const char *zWhere);
static int vc(Conn *c, const char *zOp, const char *zSql, int flags, const char *zTarget){
  char br[128];
  long long S, E;
  int rc, tries;
  if( (flags & (FW|FR)) && rn(100)>=gDestructPct ) return SQLITE_ABORT;
  curBranch(c, br, sizeof(br));
  for(tries=0; tries<4; tries++){
    S = nowNs();
    rc = runOp(c, zOp, zSql, (flags & FM) && !zTarget);
    E = nowNs();
    if( c->inTxn ){
      pendToUnknown(c);
      c->tainted = 1;
      if( sqlite3_get_autocommit(c->db) ) txnEnded(c);
    }
    if( rc==SQLITE_OK ){
      logf_("OK\t%s\t%s\t%lld\t%lld", zOp, br, S, E);
      if( flags ) logf_("X\t%d\t%s\t%lld\t%lld\t%s\t%s", excuseFlags(flags, zTarget ? zTarget : br), zTarget ? zTarget : br, (flags&FM) ? gVictimS : S, E, zOp, gVictim);
      return rc;
    }
    staleProbe(c, zOp);
    if( c->inTxn || !isBusy(rc, gErr) || otherLocalTxn(c) ) return rc;
    usleep(rn(30000));
  }
  return rc;
}

static void logRemoved(Conn *c){
  char sql[700], h[sizeof(gRes)];
  sqlite3_stmt *st = 0;
  snprintf(h, sizeof(h), "%s", gRes);
  if( strlen(h)<20 ){ logf_("NOTE\tno hash from op: %s", h); return; }
  snprintf(sql, sizeof(sql), "SELECT from_id FROM dolt_diff_w('%s~1','%s') WHERE diff_type='removed'", h, h);
  if( sqlite3_prepare_v2(c->db, sql, -1, &st, 0)==SQLITE_OK ){
    int rc;
    while( (rc = sqlite3_step(st))==SQLITE_ROW ){
      logf_("D\t%lld", sqlite3_column_int64(st, 0));
      tripRemove(sqlite3_column_int64(st, 0));
    }
    if( rc!=SQLITE_DONE ) logf_("E\t%d\tdiff_removed\t%s\t%s", rc, sqlite3_errmsg(c->db), isFatal(rc, sqlite3_errmsg(c->db)) ? "FATAL" : "");
  }else{
    logf_("E\t%d\tdiff_removed_prep\t%s\t", sqlite3_errcode(c->db), sqlite3_errmsg(c->db));
  }
  sqlite3_finalize(st);
}

static const char *otherBranch(void){ return azBr[rn(4)]; }

static void opCommit(Conn *c){
  char sql[256], msg[64], br[128];
  long long S, E;
  int rc, tries, kind = rn(10); if( kind>3 ) kind = rn(3);
  snprintf(msg, sizeof(msg), "m_%d_%d_%d", gSeed, gW, ++ctrMsg);
  if( kind==0 ) snprintf(sql, sizeof(sql), "SELECT dolt_commit('-am','%s')", msg);
  else if( kind==1 ) snprintf(sql, sizeof(sql), "SELECT dolt_commit('-Am','%s')", msg);
  else if( kind==2 ) snprintf(sql, sizeof(sql), "SELECT dolt_add('w'); SELECT dolt_commit('-m','%s')", msg);
  else snprintf(sql, sizeof(sql), "SELECT dolt_commit('-a','--amend','-m','%s')", msg);
  curBranch(c, br, sizeof(br));
  for(tries=0; tries<4; tries++){
    int wasTxn = c->inTxn;
    S = nowNs();
    rc = runOp(c, "commit_vc", sql, kind==3);
    E = nowNs();
    if( c->inTxn ){
      pendToUnknown(c); c->tainted = 1;
      if( sqlite3_get_autocommit(c->db) ) txnEnded(c);
    }
    if( rc==SQLITE_OK ){
      logf_("OK\tcommit_vc\t%s\t%lld\t%lld", br, S, E);
      if( kind==3 ) logf_("X\t%d\t%s\t%lld\t%lld\tamend\t%s\t%s", excuseFlags(FM, br), br, gVictimS, E, gVictim, msg);
      logf_("C\t%s\t%s\t%lld\t%lld\t%d", msg, br, S, E, !wasTxn ? 1 : 0);
      return;
    }
    if( c->inTxn || !isBusy(rc, gErr) || otherLocalTxn(c) ) return;
    usleep(rn(30000));
  }
}

static void opVc(Conn *c){
  char sql[512], br[128];
  int k = rn(100);
  curBranch(c, br, sizeof(br));
  if( k<10 ){
    vc(c, "add", rn(2) ? "SELECT dolt_add('.')" : "SELECT dolt_add('w')", 0, 0);
  }else if( k<20 ){
    if( getenv("VCMODEL_FOCUS") ) return;
    snprintf(sql, sizeof(sql), "SELECT dolt_checkout('%s')", otherBranch());
    vc(c, "checkout", sql, 0, 0);
  }else if( k<32 ){
    const char *o = otherBranch();
    if( !c->inTxn && rn(2) ){ opCommit(c); staleProbe(c, "commit"); }
    snprintf(sql, sizeof(sql), "SELECT dolt_merge(%s'%s')", rn(3)==0 ? "'--no-ff'," : "", o);
    vc(c, "merge", sql, 0, 0);
  }else if( k<38 ){
    if( !c->inTxn && rn(2) ){ opCommit(c); staleProbe(c, "commit"); }
    if( getenv("VCMODEL_NO_CP") ) return;
    snprintf(sql, sizeof(sql), "SELECT dolt_cherry_pick('%s')", otherBranch());
    if( vc(c, "cherry_pick", sql, 0, 0)==SQLITE_OK ) logRemoved(c);
  }else if( k<41 ){
    if( !c->inTxn && rn(2) ){ opCommit(c); staleProbe(c, "commit"); }
    if( getenv("VCMODEL_NO_REVERT") ) return;
    if( vc(c, "revert", "SELECT dolt_revert('HEAD')", 0, 0)==SQLITE_OK ) logRemoved(c);
  }else if( k<46 ){
    vc(c, "reset_soft", "SELECT dolt_reset('--soft')", 0, 0);
  }else if( k<50 ){
    vc(c, "reset_mixed", (rn(2) || getenv("VCMODEL_NO_RESET_TABLE")) ? "SELECT dolt_reset()" : "SELECT dolt_reset('w')", 0, 0);
  }else if( k<54 ){
    vc(c, "reset_hard", "SELECT dolt_reset('--hard')", FW, 0);
  }else if( k<56 ){
    vc(c, "reset_hard_1", "SELECT dolt_reset('--hard','HEAD~1')", FW|FK|FM, 0);
  }else if( k<58 ){
    vc(c, "reset_soft_1", "SELECT dolt_reset('--soft','HEAD~1')", FK|FM, 0);
  }else if( k<61 ){
    vc(c, "checkout_w", "SELECT dolt_checkout('w')", FW, 0);
  }else if( k<66 ){
    char name[32];
    snprintf(name, sizeof(name), "t_%d_%d", gW, ++ctrTag);
    snprintf(sql, sizeof(sql), "SELECT dolt_tag('%s')", name);
    if( vc(c, "tag", sql, 0, 0)==SQLITE_OK ){
      logf_("T\t%s", name);
      if( nMyTags<256 ) snprintf(azMyTags[nMyTags++], 32, "%s", name);
    }
  }else if( k<68 ){
    if( nMyTags ){
      int j = rn(nMyTags);
      logf_("TD\t%s", azMyTags[j]);
      snprintf(sql, sizeof(sql), "SELECT dolt_tag('-d','%s')", azMyTags[j]);
      vc(c, "tag_del", sql, 0, 0);
    }
  }else if( k<72 ){
    char name[32];
    snprintf(name, sizeof(name), "x_%d_%d", gW, ++ctrBr);
    snprintf(sql, sizeof(sql), "SELECT dolt_branch('%s')", name);
    if( vc(c, "branch", sql, 0, 0)==SQLITE_OK ){
      logf_("B\t%s", name);
      if( nMyBr<256 ) snprintf(azMyBr[nMyBr++], 32, "%s", name);
    }
  }else if( k<74 ){
    if( nMyBr ){
      int j = rn(nMyBr);
      logf_("BD\t%s", azMyBr[j]);
      snprintf(sql, sizeof(sql), "SELECT dolt_branch('-D','%s')", azMyBr[j]);
      vc(c, "branch_del", sql, 0, 0);
    }
  }else if( k<76 ){
    /* scratch branch b3: delete and recreate */
    if( getenv("VCMODEL_FOCUS") ) return;
    logf_("X\t%d\tb3\t%lld\t%lld\tb3_pre\t*", FR|FK|FM, nowNs(), nowNs());
    if( vc(c, "b3_del", "SELECT dolt_branch('-D','b3')", FR|FK|FM, "b3")==SQLITE_OK ){
      vc(c, "b3_create", "SELECT dolt_branch('b3')", FR|FK|FM, "b3");
    }else{
      vc(c, "b3_create", "SELECT dolt_branch('b3')", FR|FK|FM, "b3");
    }
  }else if( k<78 ){
    vc(c, "gc", "SELECT dolt_gc()", 0, 0);
  }else if( k<80 ){
    if( getenv("VCMODEL_NO_CLEAN") ) return;
    vc(c, "clean", "SELECT dolt_clean()", 0, 0);
  }else if( k<84 ){
    if( ctrCol<4 ){
      snprintf(sql, sizeof(sql), "ALTER TABLE w ADD COLUMN c_%d_%d TEXT", gW, ++ctrCol);
      vc(c, "add_column", sql, 0, 0);
    }
  }else if( k<86 ){
    if( ctrIdx<3 ){
      snprintf(sql, sizeof(sql), "CREATE INDEX i_%d_%d ON w(v)", gW, ++ctrIdx);
      vc(c, "create_index", sql, 0, 0);
    }
  }else if( k<90 ){
    vc(c, "drop_d", "DROP TABLE IF EXISTS d", 0, 0);
    vc(c, "create_d", "CREATE TABLE IF NOT EXISTS d(id INTEGER PRIMARY KEY, x TEXT)", 0, 0);
  }else if( k<94 ){
    snprintf(sql, sizeof(sql), "INSERT INTO d(x) VALUES('%d')", rn(1000));
    vc(c, "insert_d", sql, 0, 0);
  }else if( k<97 ){
    /* conflict-prone merge inside a txn, resolved */
    if( !c->inTxn ){
      int rc;
      snprintf(sql, sizeof(sql), "SELECT dolt_merge('%s')", otherBranch());
      if( ex(c, "begin_m", "BEGIN") ) return;
      c->inTxn = 1; c->tainted = 1;
      rc = ex(c, "merge_txn", sql);
      if( rc && !sqlite3_get_autocommit(c->db) && strstr(gErr, "conflict") ){
        if( ex(c, "resolve", rn(2) ? "SELECT dolt_conflicts_resolve('--ours','w')" : "SELECT dolt_conflicts_resolve('--theirs','w')")==SQLITE_OK ){
          char msg[64];
          snprintf(msg, sizeof(msg), "mm_%d_%d_%d", gSeed, gW, ++ctrMsg);
          snprintf(sql, sizeof(sql), "SELECT dolt_commit('-am','%s')", msg);
          ex(c, "commit_merge", sql);
        }
      }
      if( !sqlite3_get_autocommit(c->db) ){
        if( rn(2) ) ex(c, "commit", "COMMIT");
        if( !sqlite3_get_autocommit(c->db) ) ex(c, "rollback", "ROLLBACK");
      }
      txnEnded(c);
    }
  }else{
    vc(c, "status", "SELECT count(*) FROM dolt_status", 0, 0);
  }
}

static void closeTxns(void){
  int i;
  for(i=0;i<nConn;i++){
    Conn *c = &aC[i];
    if( c->inTxn ){
      if( rn(2) ) txnCommit(c); else txnAbort(c);
    }else if( !sqlite3_get_autocommit(c->db) ){
      ex(c, "rollback_stray", "ROLLBACK");
    }
  }
}

static void workerStep(void){
  int iConnPick = rn(nConn);
  Conn *c = &aC[iConnPick];
  int k;
  gLastStepConn = iConnPick;
  if( strstr(gErr, "failed to snapshot current branch state") && gLastConn ){
    char path[700];
    Conn *r = gLastConn;
    logf_("NOTE\treopen conn %d", (int)(r-aC));
    sqlite3_close(r->db);
    r->inTxn = 0; r->nPend = 0; r->nSp = 0;
    snprintf(path, sizeof(path), "%s%s", zDb, r->zOpenAs);
    sqlite3_open(path, &r->db);
    sqlite3_busy_timeout(r->db, 10 + rn(gBusyMax));
    gErr[0] = 0;
  }
  k = rn(100);
  if( c->inTxn && --c->nTxnOps<=0 ){
    if( rn(4) ) txnCommit(c); else txnAbort(c);
    return;
  }
  if( k<30 ) opInsert(c);
  else if( k<35 ) opUpdate(c);
  else if( k<38 ) opDelete(c);
  else if( k<45 ) opBegin(c);
  else if( k<50 ) opSavepoint(c);
  else if( k<62 ) opCommit(c);
  else opVc(c);
}

static int tripMissing(sqlite3 *db, const char *zBr, long long *aMissing, int *pnMissing){
  sqlite3_stmt *st = 0;
  char *zSql = sqlite3_mprintf("SELECT id FROM w WHERE id IN (");
  int i, rc, nAsked = 0;
  long long *aSeen = 0;
  int nSeen = 0;
  *pnMissing = 0;
  for(i=0;i<nTrip;i++){
    if( strcmp(aTripBr[i], zBr) ) continue;
    zSql = sqlite3_mprintf("%z%s%lld", zSql, nAsked++ ? "," : "", aTrip[i]);
  }
  zSql = sqlite3_mprintf("%z)", zSql);
  rc = sqlite3_prepare_v2(db, zSql, -1, &st, 0);
  sqlite3_free(zSql);
  if( rc ) return rc;
  aSeen = malloc(sizeof(long long)*(nAsked+1));
  while( (rc = sqlite3_step(st))==SQLITE_ROW ) aSeen[nSeen++] = sqlite3_column_int64(st, 0);
  sqlite3_finalize(st);
  if( rc==SQLITE_DONE ){
    rc = SQLITE_OK;
    for(i=0;i<nTrip;i++){
      int j, f = 0;
      if( strcmp(aTripBr[i], zBr) ) continue;
      for(j=0;j<nSeen;j++) if( aSeen[j]==aTrip[i] ) f = 1;
      if( !f ) aMissing[(*pnMissing)++] = aTrip[i];
    }
  }
  free(aSeen);
  return rc;
}

static void tripCheck(void){
  static char azBr[8][48];
  static sqlite3 *apDb[8];
  static int nBr = 0;
  static long long aMissing[8192];
  char path[700];
  int i, j, nMissing, rc;
  if( !getenv("VCMODEL_TRIPWIRE") || nTrip==0 ) return;
  snprintf(path, sizeof(path), "%s/TRIPPED", zDir);
  if( access(path, F_OK)==0 ){ logf_("NOTE\tpeer tripped, stopping"); fflush(gLog); _exit(0); }
  for(i=0;i<nTrip;i++){
    for(j=0;j<nBr;j++) if( !strcmp(azBr[j], aTripBr[i]) ) break;
    if( j==nBr && nBr<8 ){
      snprintf(azBr[nBr], sizeof(azBr[0]), "%.47s", aTripBr[i]);
      snprintf(path, sizeof(path), "%s@%s", zDb, azBr[nBr]);
      sqlite3_open(path, &apDb[nBr]);
      sqlite3_busy_timeout(apDb[nBr], 2000);
      nBr++;
    }
  }
  for(j=0;j<nBr;j++){
    rc = tripMissing(apDb[j], azBr[j], aMissing, &nMissing);
    if( rc ){ logf_("NOTE\ttrip %s rc=%d %s", azBr[j], rc, sqlite3_errmsg(apDb[j])); continue; }
    if( nMissing && !getenv("VCMODEL_TRIP_NORETRY") ){
      /* A reader can be served stale data while a peer commits; re-read. */
      usleep(50000);
      rc = tripMissing(apDb[j], azBr[j], aMissing, &nMissing);
      if( rc ) continue;
    }
    if( nMissing ){
      long long now = nowNs();
      for(i=0;i<nMissing;i++) logf_("TRIP\t%lld\tmissing\t%lld\t%s", now, aMissing[i], azBr[j]);
      snprintf(path, sizeof(path), "%s/TRIPPED", zDir);
      { FILE *f = fopen(path, "w"); if( f ) fclose(f); }
      fflush(gLog);
      _exit(7);
    }
  }
}

static void hashOf(sqlite3 *db, char *z, int n){
  sqlite3_stmt *st = 0;
  z[0] = 0;
  if( sqlite3_prepare_v2(db, "SELECT dolt_hashof_table('w')", -1, &st, 0)==SQLITE_OK
   && sqlite3_step(st)==SQLITE_ROW && sqlite3_column_text(st, 0) ){
    snprintf(z, n, "%s", (const char*)sqlite3_column_text(st, 0));
  }
  sqlite3_finalize(st);
}

/* After an autocommit op the connection must read what a fresh reader of its
** branch reads. A session that disagrees and does not move on a reread holds
** a stale view it believes is current, which its next write would publish. */
static int staleDetect(Conn *c, char *zDetail, int nDetail){
  static char azBr[8][48];
  static sqlite3 *apDb[8];
  static int nBr = 0;
  char br[128], path[800], hConn[64], hConn2[64], hFresh[64], hFresh2[64];
  int j;
  if( c->inTxn || !sqlite3_get_autocommit(c->db) ) return 0;
  curBranch(c, br, sizeof(br));
  if( !strcmp(br, "?") ) return 0;
  for(j=0;j<nBr;j++) if( !strcmp(azBr[j], br) ) break;
  if( j==nBr ){
    if( nBr==8 ) return 0;
    snprintf(azBr[nBr], sizeof(azBr[0]), "%.47s", br);
    snprintf(path, sizeof(path), "%s@%s", zDb, br);
    if( sqlite3_open_v2(path, &apDb[nBr], SQLITE_OPEN_READONLY, 0)!=SQLITE_OK ) return 0;
    sqlite3_busy_timeout(apDb[nBr], 2000);
    nBr++;
  }
  hashOf(c->db, hConn, sizeof(hConn));
  hashOf(apDb[j], hFresh, sizeof(hFresh));
  if( !hConn[0] || !hFresh[0] || !strcmp(hConn, hFresh) ) return 0;
  usleep(20000);
  hashOf(c->db, hConn2, sizeof(hConn2));
  hashOf(apDb[j], hFresh2, sizeof(hFresh2));
  if( !strcmp(hConn2, hFresh2) || strcmp(hConn, hConn2) ) return 0;
  snprintf(zDetail, nDetail, "c%d\t%s\tconn=%s\tfresh=%s", (int)(c-aC), br, hConn2, hFresh2);
  return 1;
}

static void staleProbe(Conn *c, const char *zWhere){
  char z[300], path[700];
  if( getenv("VCMODEL_STALECHK") && staleDetect(c, z, sizeof(z)) ){
    logf_("STALEAT\t%lld\t%s\t%s", nowNs(), zWhere, z);
    if( !strstr(z, "\tb3\t") ){
      snprintf(path, sizeof(path), "%s/TRIPPED", zDir);
      { FILE *f = fopen(path, "w"); if( f ) fclose(f); }
      fflush(gLog);
      _exit(7);
    }
  }
}

static void staleCheck(void){
  char z[300], path[700];
  if( !getenv("VCMODEL_STALECHK") || !staleDetect(&aC[gLastStepConn], z, sizeof(z)) ) return;
  logf_("STALE\t%lld\t%s", nowNs(), z);
  snprintf(path, sizeof(path), "%s/TRIPPED", zDir);
  { FILE *f = fopen(path, "w"); if( f ) fclose(f); }
  fflush(gLog);
  _exit(7);
}

static void icheck(void){
  char br[128], path[800], buf[2048]; sqlite3 *f = 0; sqlite3_stmt *st = 0; int n = 0, rc;
  if( !getenv("VCMODEL_ICHECK") ) return;
  snprintf(path, sizeof(path), "%s/TRIPPED", zDir);
  if( access(path, F_OK)==0 ){ logf_("NOTE\tpeer tripped, stopping"); fflush(gLog); _exit(0); }
  curBranch(&aC[gLastStepConn], br, sizeof(br));
  if( !strcmp(br, "?") ) return;
  snprintf(path, sizeof(path), "%s@%s", zDb, br);
  if( sqlite3_open_v2(path, &f, SQLITE_OPEN_READONLY, 0)!=SQLITE_OK ){ sqlite3_close(f); return; }
  sqlite3_busy_timeout(f, 2000);
  buf[0] = 0;
  if( sqlite3_prepare_v2(f, "PRAGMA integrity_check", -1, &st, 0)==SQLITE_OK ){
    while( (rc = sqlite3_step(st))==SQLITE_ROW && n<1800 ){
      n += snprintf(buf+n, sizeof(buf)-n, "%s|", (const char*)sqlite3_column_text(st,0));
    }
  }
  sqlite3_finalize(st); sqlite3_close(f);
  if( buf[0] && strcmp(buf, "ok|") && !strstr(buf, "no longer") && !strstr(buf, "not found") ){
    for(n=0; buf[n]; n++) if( buf[n]=='\n' || buf[n]=='\t' ) buf[n]=' ';
    logf_("ICFAIL\t%lld\t%s\tc%d\t%s", nowNs(), br, gLastStepConn, buf);
    snprintf(path, sizeof(path), "%s/TRIPPED", zDir);
    { FILE *ff = fopen(path, "w"); if( ff ) fclose(ff); }
    fflush(gLog); _exit(8);
  }
}

static void runWorker(int w, int rfd, int wfd){
  char path[700];
  int i, ph;
  gW = w;
  signal(SIGALRM, onAlarm);
  rngState = (unsigned long long)gSeed * 1000003ULL + (unsigned long long)w * 7919ULL + 1;
  for(i=0;i<5;i++) rnd();
  snprintf(path, sizeof(path), "%s/log.%d", zDir, w);
  gLog = fopen(path, "w");
  for(i=0;i<nConn;i++){
    Conn *c = &aC[i];
    memset(c, 0, sizeof(*c));
    if( getenv("VCMODEL_FOCUS") ) snprintf(c->zOpenAs, sizeof(c->zOpenAs), "@%s", getenv("VCMODEL_FOCUS"));
    else if( rn(2) ) snprintf(c->zOpenAs, sizeof(c->zOpenAs), "@%s", azBr[rn(4)]);
    snprintf(path, sizeof(path), "%s%s", zDb, c->zOpenAs);
    if( sqlite3_open(path, &c->db)!=SQLITE_OK ){
      logf_("E\t%d\topen\t%s\tFATAL", sqlite3_errcode(c->db), sqlite3_errmsg(c->db));
    }
    sqlite3_busy_timeout(c->db, 10 + rn(gBusyMax));
  }
  for(ph=0; ph<nPhase; ph++){
    char ch;
    for(i=0;i<nOps;i++){
      {
        char st[64]; int j, n = 0;
        for(j=0;j<nConn && n<60;j++){
          st[n++] = sqlite3_get_autocommit(aC[j].db) ? '.' : (aC[j].inTxn ? 'T' : '!');
          st[n++] = '0' + sqlite3_txn_state(aC[j].db, "main");
        }
        st[n] = 0;
        logf_("OP\t%d\t%d\t%lld\t%s", ph, i, nowNs(), st);
      }
      workerStep();
      if( getenv("VCMODEL_CNT") ){
        Conn *cc = &aC[gLastStepConn];
        sqlite3_stmt *q = 0; char br[128];
        curBranch(cc, br, sizeof(br));
        if( sqlite3_prepare_v2(cc->db, "SELECT count(*) FROM w", -1, &q, 0)==SQLITE_OK
         && sqlite3_step(q)==SQLITE_ROW ){
          logf_("N\t%d\t%s\t%d", gLastStepConn, br, sqlite3_column_int(q, 0));
        }
        sqlite3_finalize(q);
      }
      tripCheck();
      icheck();
      staleCheck();
    }
    closeTxns();
    {
      char st[64]; int j, n = 0;
      for(j=0;j<nConn && n<60;j++){
        st[n++] = sqlite3_get_autocommit(aC[j].db) ? '.' : '!';
        st[n++] = '0' + sqlite3_txn_state(aC[j].db, "main");
      }
      st[n] = 0;
      logf_("PHASE\t%d\t%lld\t%s", ph, nowNs(), st);
    }
    ch = 'd';
    if( write(wfd, &ch, 1)!=1 ) _exit(3);
    if( read(rfd, &ch, 1)!=1 ) _exit(4);
  }
  for(i=0;i<nConn;i++) sqlite3_close(aC[i].db);
  fclose(gLog);
  _exit(0);
}

typedef struct Ins { long long id; char br[48]; long long S, E; } Ins;
typedef struct Xev { int flags; char br[48]; long long S, E; char op[24]; char victim[64]; char newmsg[64]; } Xev;
typedef struct Cmt { char msg[64]; char br[48]; long long S, E; int all; } Cmt;
typedef struct StrSet { char (*a)[48]; int n, cap; } StrSet;

static Ins *aIns; static int nIns, capIns;
static Xev *aX; static int nX, capX;
static Cmt *aCm; static int nCm, capCm;
static long long *aUnk; static int nUnk, capUnk;
static long long *aDel; static int nDel, capDel;
static StrSet tagsMade, tagsDel, brMade, brDel;
static int nViol = 0, nFatal = 0, nKnownParked = 0;
static char zPendFatal[1200];
static int stRow, stRowEx, stCm, stCmEx, stHead, stHeadEx;

#define GROW(arr, n, cap) do{ if((n)>=(cap)){ (cap)=(cap)?(cap)*2:1024; (arr)=realloc((arr), sizeof(*(arr))*(cap)); } }while(0)

static void ssAdd(StrSet *s, const char *z){
  GROW(s->a, s->n, s->cap); snprintf(s->a[s->n++], 48, "%s", z);
}
static int ssHas(StrSet *s, const char *z){
  int i; for(i=0;i<s->n;i++) if( !strcmp(s->a[i], z) ) return 1; return 0;
}
static int cmpll(const void *a, const void *b){
  long long x = *(const long long*)a, y = *(const long long*)b; return x<y?-1:x>y;
}
static int hasLL(long long *a, int n, long long v){
  return bsearch(&v, a, n, sizeof(long long), cmpll)!=0;
}

static void viol(const char *fmt, ...){
  va_list ap; va_start(ap, fmt);
  printf("VIOLATION seed=%d: ", gSeed); vprintf(fmt, ap); printf("\n");
  va_end(ap); nViol++;
}

static void loadLogs(void){
  int w;
  nIns = nX = nCm = nUnk = nDel = 0;
  tagsMade.n = tagsDel.n = brMade.n = brDel.n = 0;
  nFatal = 0; nKnownParked = 0;
  for(w=0; w<nProc; w++){
    char path[700], line[2048];
    long long victimLockS = 0, victimLockE = 0;
    FILE *f;
    snprintf(path, sizeof(path), "%s/log.%d", zDir, w);
    f = fopen(path, "r");
    if( !f ) continue;
    while( fgets(line, sizeof(line), f) ){
      char *t[8]; int nt = 0; char *p = line, *q;
      line[strcspn(line, "\n")] = 0;
      while( nt<8 && (q = strsep(&p, "\t"))!=0 ) t[nt++] = q;
      if( !strcmp(t[0],"I") && nt>=5 ){
        GROW(aIns, nIns, capIns);
        aIns[nIns].id = atoll(t[1]); snprintf(aIns[nIns].br, 48, "%s", t[2]);
        aIns[nIns].S = atoll(t[3]); aIns[nIns].E = atoll(t[4]); nIns++;
      }else if( !strcmp(t[0],"U") ){
        GROW(aUnk, nUnk, capUnk); aUnk[nUnk++] = atoll(t[1]);
      }else if( !strcmp(t[0],"D") ){
        GROW(aDel, nDel, capDel); aDel[nDel++] = atoll(t[1]);
      }else if( !strcmp(t[0],"S") && nt>=5
             && !strcmp(t[4],"BEGIN IMMEDIATE") ){
        victimLockS = atoll(t[1]);
        victimLockE = atoll(t[2]);
      }else if( !strcmp(t[0],"X") && nt>=6 ){
        GROW(aX, nX, capX);
        aX[nX].flags = atoi(t[1]); snprintf(aX[nX].br, 48, "%s", t[2]);
        aX[nX].S = atoll(t[3]); aX[nX].E = atoll(t[4]);
        /* The victim was read after the lock wait, not at its start. */
        if( (aX[nX].flags & FM) && aX[nX].S<=victimLockS
         && victimLockE<=aX[nX].E && victimLockE>aX[nX].S ){
          aX[nX].S = victimLockE;
        }
        snprintf(aX[nX].op, 24, "%s", t[5]);
        snprintf(aX[nX].victim, 64, "%s", nt>6 ? t[6] : "*");
        snprintf(aX[nX].newmsg, 64, "%s", nt>7 ? t[7] : ""); nX++;
      }else if( !strcmp(t[0],"C") && nt>=6 ){
        GROW(aCm, nCm, capCm);
        snprintf(aCm[nCm].msg, 64, "%s", t[1]); snprintf(aCm[nCm].br, 48, "%s", t[2]);
        aCm[nCm].S = atoll(t[3]); aCm[nCm].E = atoll(t[4]); aCm[nCm].all = atoi(t[5]); nCm++;
      }else if( !strcmp(t[0],"T") ) ssAdd(&tagsMade, t[1]);
      else if( !strcmp(t[0],"TD") ) ssAdd(&tagsDel, t[1]);
      else if( !strcmp(t[0],"B") ) ssAdd(&brMade, t[1]);
      else if( !strcmp(t[0],"BD") ) ssAdd(&brDel, t[1]);
      else if( !strcmp(t[0],"E") && nt>=5 ){
        if( !strcmp(t[4],"FATAL") ){
          nFatal++;
          snprintf(zPendFatal, sizeof(zPendFatal), "worker %d op=%s rc=%s: %s", w, t[2], t[1], t[3]);
        }
      }else if( !strcmp(t[0],"FATALCTX") && nt>=3 ){
        if( atoi(t[2])==0 ) nKnownParked++;
        else viol("fatal error %s (branch %s exists=%s)", zPendFatal, t[1], t[2]);
      }else if( !strcmp(t[0],"WEDGE") ){
        viol("WEDGE worker %d: %s", w, t[1]);
      }
    }
    fclose(f);
  }
  qsort(aUnk, nUnk, sizeof(long long), cmpll);
  qsort(aDel, nDel, sizeof(long long), cmpll);
}

static int onBr(const Xev *x, const char *br){
  return (x->flags & FG) || !strcmp(x->br, br);
}
/* any event with flag f on branch br whose end > t */
static const Xev *excuse(int f, const char *br, long long t){
  int i;
  for(i=0;i<nX;i++){
    if( (aX[i].flags & f) && aX[i].E > t && onBr(&aX[i], br) ) return &aX[i];
  }
  return 0;
}
/* any event with flag f on br overlapping (lo, hi) */
static int anyBetween(int f, const char *br, long long lo, long long hi){
  int i;
  for(i=0;i<nX;i++){
    if( (aX[i].flags & f) && aX[i].E > lo && aX[i].S < hi && onBr(&aX[i], br) ) return 1;
  }
  return 0;
}
/* Does the commit m, followed through amends before time t, carry the
** message zVictim? */
static int chainHits(const Cmt *m, long long t, const char *zVictim){
  char cur[64];
  int i, changed = 1, guard = 0;
  long long curS = m->S, curE = m->E;
  snprintf(cur, sizeof(cur), "%s", m->msg);
  if( !strcmp(cur, zVictim) ) return 1;
  while( changed && guard++<1000 ){
    changed = 0;
    for(i=0;i<nX;i++){
      const Xev *x = &aX[i];
      if( strcmp(x->op, "amend") || x->E <= curS || x->S >= t ) continue;
      if( strcmp(x->br, m->br) || !x->newmsg[0] ) continue;
      if( !strcmp(x->newmsg, cur) ) continue;
      if( !strcmp(x->victim, cur) || !strcmp(x->victim, "*") || curE > x->S ){
        if( !strcmp(x->victim, "*") ) return 1;
        snprintf(cur, sizeof(cur), "%s", x->newmsg);
        curS = x->S;
        curE = x->E;
        if( !strcmp(cur, zVictim) ) return 1;
        changed = 1;
      }
    }
  }
  return 0;
}

/* Row p was in an acked commit -a on its branch that was still HEAD's
** ancestor (no HEAD regression) by time tHead. The victim is read under the
** rewriting op's lock, so amends up to tVictim (its end) can produce it. */
static int committedBefore(const struct Ins *p, long long t, long long tHead,
                           const char *zVictim, long long tVictim){
  int i;
  for(i=0;i<nCm;i++){
    const Cmt *m = &aCm[i];
    if( !m->all || strcmp(m->br, p->br) ) continue;
    if( m->S <= p->E || m->E >= t ) continue;
    if( zVictim && chainHits(m, tVictim, zVictim) ) continue;
    if( anyBetween(FW|FR, p->br, p->S, m->E) ) continue;
    if( anyBetween(FK, p->br, m->S, tHead) ) continue;
    return 1;
  }
  return 0;
}
/* Some acked op legitimately removed row p from its branch's working set. */
static const Xev *rowExcused(const struct Ins *p){
  int i;
  for(i=0;i<nX;i++){
    const Xev *x = &aX[i];
    if( x->E <= p->S || !onBr(x, p->br) ) continue;
    if( x->flags & FR ) return x;
    if( (x->flags & FW) && (x->flags & FK) && !strcmp(x->victim, "*") ) return x;
    /* A reset that waited for the lock reads HEAD when it gets it, so a
    ** peer's HEAD move up to its end may already have dropped the commit. */
    if( (x->flags & FW) && !committedBefore(p, x->S,
            (x->flags & FK) ? x->S : x->E,
            (x->flags & FK) ? x->victim : 0, x->E) ) return x;
  }
  return 0;
}

static const Xev *commitExcused(const Cmt *m){
  int i;
  for(i=0;i<nX;i++){
    const Xev *x = &aX[i];
    if( !(x->flags & FM) || x->E <= m->S || !onBr(x, m->br) ) continue;
    if( !strcmp(x->victim, "*") || !strcmp(x->victim, m->msg) ) return x;
    if( m->E > x->S && m->S < x->E ) return x;
    {
      /* A peer rewrite racing this one moves HEAD under it: it rewrites
      ** whatever HEAD is when it runs, not the victim it logged. */
      int j;
      for(j=0;j<nX;j++){
        const Xev *y = &aX[j];
        if( j==i || !(y->flags & FM) || strcmp(y->br, x->br) ) continue;
        if( y->S < x->E && x->S < y->E ) return x;
      }
    }
  }
  return 0;
}

typedef struct IdSet { long long *a; int n, cap; } IdSet;
static void idAdd(IdSet *s, long long v){ GROW(s->a, s->n, s->cap); s->a[s->n++] = v; }

static int readIds(sqlite3 *db, const char *sql, IdSet *s){
  sqlite3_stmt *st = 0; int rc;
  s->n = 0;
  rc = sqlite3_prepare_v2(db, sql, -1, &st, 0);
  if( rc ){ viol("check prepare '%s': %s", sql, sqlite3_errmsg(db)); return rc; }
  while( (rc = sqlite3_step(st))==SQLITE_ROW ) idAdd(s, sqlite3_column_int64(st, 0));
  if( rc!=SQLITE_DONE ){ viol("check '%s': rc=%d %s", sql, rc, sqlite3_errmsg(db)); }
  sqlite3_finalize(st);
  qsort(s->a, s->n, sizeof(long long), cmpll);
  return rc==SQLITE_DONE ? 0 : rc;
}

static int readStrs(sqlite3 *db, const char *sql, StrSet *s){
  sqlite3_stmt *st = 0; int rc;
  s->n = 0;
  rc = sqlite3_prepare_v2(db, sql, -1, &st, 0);
  if( rc ){ viol("check prepare '%s': %s", sql, sqlite3_errmsg(db)); return rc; }
  while( (rc = sqlite3_step(st))==SQLITE_ROW ){
    const char *z = (const char*)sqlite3_column_text(st, 0);
    ssAdd(s, z ? z : "");
  }
  if( rc!=SQLITE_DONE ) viol("check '%s': rc=%d %s", sql, rc, sqlite3_errmsg(db));
  sqlite3_finalize(st);
  return 0;
}

static int readTableIds(sqlite3 *db, IdSet *s){
  IdSet tables = {0};
  int rc;
  s->n = 0;
  rc = readIds(db, "SELECT 1 FROM main.sqlite_schema "
                   "WHERE type='table' AND name='w'", &tables);
  if( rc==SQLITE_OK && tables.n ) rc = readIds(db, "SELECT id FROM w", s);
  free(tables.a);
  return rc;
}

static int readHeadIds(sqlite3 *db, const char *br, IdSet *s){
  StrSet hash = {0};
  sqlite3 *hd = 0;
  char path[800];
  int rc = readStrs(db, "SELECT dolt_hashof('HEAD')", &hash);
  s->n = 0;
  if( rc || hash.n!=1 ){
    if( !rc ) viol("check HEAD of %s: expected one commit hash", br);
    free(hash.a);
    return rc ? rc : SQLITE_ERROR;
  }
  snprintf(path, sizeof(path), "%s@%s", zDb, hash.a[0]);
  free(hash.a);
  rc = sqlite3_open_v2(path, &hd, SQLITE_OPEN_READONLY, 0);
  if( rc ){
    viol("open HEAD of %s: %s", br, sqlite3_errmsg(hd));
  }else{
    sqlite3_busy_timeout(hd, 5000);
    rc = readTableIds(hd, s);
  }
  sqlite3_close(hd);
  return rc;
}

static void checkModel(int ph){
  sqlite3 *db = 0;
  StrSet branches = {0}, tags = {0}, logm = {0};
  IdSet ws = {0}, head = {0}, acked = {0};
  int i, b;
  long long t0 = nowNs();
  loadLogs();
  for(i=0;i<nIns;i++) idAdd(&acked, aIns[i].id);
  qsort(acked.a, acked.n, sizeof(long long), cmpll);
  if( sqlite3_open(zDb, &db) ){ viol("open main for check"); return; }
  sqlite3_busy_timeout(db, 5000);
  readStrs(db, "SELECT name FROM dolt_branches", &branches);
  readStrs(db, "SELECT tag_name FROM dolt_tags", &tags);
  {
    StrSet ic = {0};
    readStrs(db, "PRAGMA integrity_check", &ic);
    if( ic.n!=1 || strcmp(ic.a[0], "ok") ) viol("phase %d integrity_check main: %s", ph, ic.n ? ic.a[0] : "(none)");
  }
  for(i=0;i<4;i++) if( !ssHas(&branches, azBr[i]) && strcmp(azBr[i],"b3") ) viol("branch %s missing", azBr[i]);
  for(i=0;i<brMade.n;i++) if( !ssHas(&brDel, brMade.a[i]) && !ssHas(&branches, brMade.a[i]) ) viol("acked branch %s missing", brMade.a[i]);
  for(i=0;i<tagsMade.n;i++) if( !ssHas(&tagsDel, tagsMade.a[i]) && !ssHas(&tags, tagsMade.a[i]) ) viol("acked tag %s missing", tagsMade.a[i]);
  sqlite3_close(db);
  for(b=0;b<branches.n;b++){
    const char *br = branches.a[b];
    char path[800], sql[256];
    sqlite3 *bd = 0;
    int j;
    if( !strncmp(br, "x_", 2) ) continue;
    snprintf(path, sizeof(path), "%s@%s", zDb, br);
    if( sqlite3_open(path, &bd) ){ viol("open %s: %s", path, sqlite3_errmsg(bd)); sqlite3_close(bd); continue; }
    sqlite3_busy_timeout(bd, 5000);
    if( readTableIds(bd, &ws) || readHeadIds(bd, br, &head) ){
      sqlite3_close(bd);
      continue;
    }
    snprintf(sql, sizeof(sql), "SELECT message FROM dolt_log('%s')", br);
    readStrs(bd, sql, &logm);
    {
      StrSet ic = {0};
      readStrs(bd, "PRAGMA integrity_check", &ic);
      if( ic.n!=1 || strcmp(ic.a[0], "ok") ) viol("phase %d integrity_check %s: %s", ph, br, ic.n ? ic.a[0] : "(none)");
    }
    sqlite3_close(bd);
    /* phantom rows */
    for(j=0;j<ws.n;j++){
      long long id = ws.a[j];
      if( id < 100000000LL ) continue;
      if( !hasLL(acked.a, acked.n, id) && !hasLL(aUnk, nUnk, id) )
        viol("phase %d phantom id %lld in working set of %s (never acked)", ph, id, br);
    }
    for(j=0;j<head.n;j++){
      long long id = head.a[j];
      if( id < 100000000LL ) continue;
      if( !hasLL(acked.a, acked.n, id) && !hasLL(aUnk, nUnk, id) )
        viol("phase %d phantom id %lld in HEAD of %s (never acked)", ph, id, br);
    }
    /* acked rows still in the working set */
    for(j=0;j<nIns;j++){
      Ins *p = &aIns[j];
      if( strcmp(p->br, br) ) continue;
      if( hasLL(aDel, nDel, p->id) ) continue;
      if( rowExcused(p) ){ stRowEx++; if( getenv("VCMODEL_DBGEX") ) printf("  rowex %lld %s %lld %lld\n", p->id, rowExcused(p)->op, rowExcused(p)->S, rowExcused(p)->E); continue; }
      stRow++;
      if( !hasLL(ws.a, ws.n, p->id) )
        viol("phase %d LOST acked id %lld on %s working set (S=%lld E=%lld)", ph, p->id, br, p->S, p->E);
    }
    /* acked commits in the log, and their rows in HEAD */
    for(j=0;j<nCm;j++){
      Cmt *m = &aCm[j];
      int k;
      if( strcmp(m->br, br) ) continue;
      if( commitExcused(m) ){ stCmEx++; if( getenv("VCMODEL_DBGEX") ) printf("  cmex %s %s %s\n", commitExcused(m)->op, commitExcused(m)->victim, m->msg); } else stCm++;
      if( !commitExcused(m) && !ssHas(&logm, m->msg) )
        viol("phase %d LOST acked commit '%s' on %s (S=%lld E=%lld)", ph, m->msg, br, m->S, m->E);
      if( !m->all ) continue;
      if( excuse(FK, br, m->S) ){ stHeadEx++; continue; }
      for(k=0;k<nIns;k++){
        Ins *p = &aIns[k];
        if( strcmp(p->br, br) || p->E >= m->S ) continue;
        if( hasLL(aDel, nDel, p->id) || anyBetween(FW|FR, br, p->S, m->E) ) continue;
        stHead++;
        if( !hasLL(head.a, head.n, p->id) )
          viol("phase %d acked id %lld (E=%lld) on %s missing from HEAD after acked commit -a '%s' (S=%lld)", ph, p->id, p->E, br, m->msg, m->S);
      }
    }
  }
  printf("  verified rows=%d (excused %d) commits=%d (excused %d) headrows=%d (excused %d)\n", stRow, stRowEx, stCm, stCmEx, stHead, stHeadEx);
  stRow=stRowEx=stCm=stCmEx=stHead=stHeadEx=0;
  if( nKnownParked ) printf("  known: %d CORRUPT errors on connections parked on a deleted branch\n", nKnownParked);
  printf("  phase %d check: branches=%d acked=%d unknown=%d deleted=%d xev=%d commits=%d (%.1fs)\n",
         ph, branches.n, nIns, nUnk, nDel, nX, nCm, (nowNs()-t0)/1e9);
  fflush(stdout);
}

static void sampleStacks(pid_t pid){
  char cmd[900];
#ifdef __APPLE__
  snprintf(cmd, sizeof(cmd), "sample %d 2 -file %s/stack.%d.txt >/dev/null 2>&1", (int)pid, zDir, (int)pid);
#else
  snprintf(cmd, sizeof(cmd), "gdb -p %d -batch -ex 'thread apply all bt' >%s/stack.%d.txt 2>&1", (int)pid, zDir, (int)pid);
#endif
  if( system(cmd) ){}
}

static void buildDb(void){
  sqlite3 *db = 0;
  char *zE = 0;
  int rc;
  sqlite3_open(zDb, &db);
  rc = sqlite3_exec(db, getenv("VCMODEL_CLUSTERED")
                       && atoi(getenv("VCMODEL_CLUSTERED"))
      ? "CREATE TABLE w(id INT PRIMARY KEY, v TEXT)"
      : "CREATE TABLE w(id INTEGER PRIMARY KEY, v TEXT)", 0, 0, &zE);
  if( rc ){ fprintf(stderr, "build failed: %s\n", zE); exit(2); }
  rc = sqlite3_exec(db,
    "INSERT INTO w VALUES(1,'a'),(2,'b'),(3,'c');"
    "SELECT dolt_commit('-Am','init');"
    "INSERT INTO w VALUES(4,'d');"
    "SELECT dolt_commit('-am','base1');"
    "INSERT INTO w VALUES(5,'e');"
    "SELECT dolt_commit('-am','base2');"
    "INSERT INTO w VALUES(6,'f');"
    "SELECT dolt_commit('-am','base3');"
    "SELECT dolt_branch('b1');"
    "SELECT dolt_branch('b2');"
    "SELECT dolt_branch('b3');"
    "SELECT dolt_tag('base');", 0, 0, &zE);
  if( rc ){ fprintf(stderr, "build failed: %s\n", zE); exit(2); }
  sqlite3_close(db);
}

int main(int argc, char **argv){
  int w, ph, status;
  pid_t aPid[32];
  int aUp[32][2], aDown[32][2];
  int wedgeSec = 120;
  if( argc<3 ){ fprintf(stderr, "usage: %s SEED DIR [NPROC NCONN NPHASE OPS]\n", argv[0]); return 2; }
  if( !strcmp(argv[1], "--check") ){
    snprintf(zDir, sizeof(zDir), "%s", argv[2]);
    snprintf(zDb, sizeof(zDb), "%s/db.db", zDir);
    nProc = argc>3 ? atoi(argv[3]) : 8;
    if( nProc<1 || nProc>32 ){ fprintf(stderr, "NPROC must be 1-32\n"); return 2; }
    checkModel(99);
    printf("%d violations\n", nViol);
    return nViol!=0;
  }
  gSeed = atoi(argv[1]);
  snprintf(zDir, sizeof(zDir), "%s", argv[2]);
  if( argc>3 ) nProc = atoi(argv[3]);
  if( argc>4 ) nConn = atoi(argv[4]);
  if( argc>5 ) nPhase = atoi(argv[5]);
  if( argc>6 ) nOps = atoi(argv[6]);
  if( nProc<1 || nProc>32 || nConn<1 || nConn>(int)(sizeof(aC)/sizeof(aC[0]))
   || nPhase<1 || nOps<1 ){
    fprintf(stderr, "NPROC must be 1-32, NCONN 1-%d, NPHASE and OPS at least 1\n",
            (int)(sizeof(aC)/sizeof(aC[0])));
    return 2;
  }
  if( getenv("VCMODEL_DESTRUCT") ) gDestructPct = atoi(getenv("VCMODEL_DESTRUCT"));
  if( getenv("VCMODEL_BUSY_MAX") ) gBusyMax = atoi(getenv("VCMODEL_BUSY_MAX"));
  if( getenv("VCMODEL_WEDGE_SEC") ) wedgeSec = atoi(getenv("VCMODEL_WEDGE_SEC"));
  if( getenv("VCMODEL_STMT_WEDGE_SEC") ) gStmtWedgeSec = atoi(getenv("VCMODEL_STMT_WEDGE_SEC"));
  mkdir(zDir, 0700);
  snprintf(zDb, sizeof(zDb), "%s/db.db", zDir);
  setvbuf(stdout, 0, _IOLBF, 0);
  buildDb();
  for(w=0; w<nProc; w++){
    if( pipe(aUp[w]) || pipe(aDown[w]) ) return 2;
    aPid[w] = fork();
    if( aPid[w]==0 ){
      close(aUp[w][0]); close(aDown[w][1]);
      runWorker(w, aDown[w][0], aUp[w][1]);
    }
    close(aUp[w][1]); close(aDown[w][0]);
  }
  for(ph=0; ph<nPhase; ph++){
    long long deadline = nowNs() + (long long)wedgeSec*1000000000LL;
    int alive = 1;
    for(w=0; w<nProc; w++){
      char ch;
      struct pollfd pfd;
      int r;
      pfd.fd = aUp[w][0]; pfd.events = POLLIN;
      for(;;){
        long long left = (deadline - nowNs())/1000000;
        if( left<=0 ){ r = 0; break; }
        r = poll(&pfd, 1, (int)(left>1000?1000:left));
        if( r!=0 ) break;
      }
      if( r<=0 ){
        viol("WEDGE: worker %d (pid %d) did not finish phase %d within %ds", w, (int)aPid[w], ph, wedgeSec);
        if( alive ){ int w2; for(w2=0;w2<nProc;w2++) sampleStacks(aPid[w2]); }
        alive = 0;
      }else if( read(aUp[w][0], &ch, 1)!=1 ){
        viol("worker %d died in phase %d", w, ph);
        alive = 0;
      }
    }
    if( !alive ) break;
    checkModel(ph);
    for(w=0; w<nProc; w++){ char ch='g'; if( write(aDown[w][1], &ch, 1)!=1 ) viol("cannot resume worker %d", w); }
  }
  for(w=0; w<nProc; w++){
    if( nViol ) kill(aPid[w], SIGKILL);
    if( waitpid(aPid[w], &status, 0)>0 ){
      if( WIFSIGNALED(status) && !nViol ) viol("worker %d killed by signal %d", w, WTERMSIG(status));
      else if( WIFEXITED(status) && WEXITSTATUS(status)!=0 ) viol("worker %d exit %d", w, WEXITSTATUS(status));
    }
  }
  printf("seed %d: %s (%d violations)\n", gSeed, nViol ? "FAIL" : "ok", nViol);
  return nViol ? 1 : 0;
}
