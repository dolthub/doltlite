#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "vdbeInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_internal.h"
#include "prolly_btree_int.h"

#include <string.h>

void doltliteAuthShieldEnter(sqlite3 *db, DoltliteAuthShield *p){
  p->db = db;
#ifndef SQLITE_OMIT_AUTHORIZATION
  p->xAuth = db->xAuth;
  db->xAuth = 0;
#endif
}

void doltliteAuthShieldLeave(DoltliteAuthShield *p){
#ifndef SQLITE_OMIT_AUTHORIZATION
  p->db->xAuth = p->xAuth;
#endif
}

static int doltliteBeginSessionRead(sqlite3 *db){
  if( db->autoCommit || !doltliteGetChunkStore(db) ) return SQLITE_OK;
  return sqlite3BtreeBeginTrans(db->aDb[0].pBt, 0, 0);
}

static void doltliteInvokeShieldedFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv,
  int bRead
){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  void (*xFunc)(sqlite3_context*,int,sqlite3_value**) = sqlite3_user_data(ctx);
  DoltliteAuthShield shield;
  int rc;
  doltliteAuthShieldEnter(db, &shield);
  rc = bRead ? doltliteBeginSessionRead(db) : SQLITE_OK;
  if( rc==SQLITE_OK ){
    xFunc(ctx, argc, argv);
  }else{
    sqlite3_result_error_code(ctx, rc);
  }
  doltliteAuthShieldLeave(&shield);
  /* SQLITE_NOTFOUND is the store's internal "no such chunk"; one that escapes
  ** a command is a chunk the database should hold. A host chunk source may
  ** report NOTFOUND by contract. */
  if( ctx->isError==SQLITE_NOTFOUND
   && !doltliteChunkSourceActive(sqlite3_context_db_handle(ctx)) ){
    const char *zMsg = (const char*)sqlite3_value_text(ctx->pOut);
    if( !zMsg || strcmp(zMsg, sqlite3ErrStr(SQLITE_NOTFOUND))==0 ){
      sqlite3_result_error(ctx, sqlite3ErrStr(SQLITE_CORRUPT), -1);
    }
    sqlite3_result_error_code(ctx, SQLITE_CORRUPT);
  }
}

static void doltliteCommandFuncShield(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  doltliteInvokeShieldedFunc(ctx, argc, argv, db->pVcCommand==0);
}

int doltliteCreateShieldedFunc(
  sqlite3 *db,
  const char *zName,
  int nArg,
  int eTextRep,
  void (*xFunc)(sqlite3_context*,int,sqlite3_value**)
){
  return sqlite3_create_function(db, zName, nArg, eTextRep,
                                 (void*)xFunc, doltliteCommandFuncShield, 0, 0);
}

int doltliteVcInterruptDeferred(sqlite3 *db, Vdbe *p){
  return db->nVcInterruptMask
      || (db->pVcCommand && db->pVcCommand->vcInstalled)
      || (p && p->vcInstalled);
}

void doltliteVcCommandInstalled(sqlite3 *db){
  if( db->pVcCommand && !db->nVcInterruptMask ){
    db->pVcCommand->vcInstalled = 1;
  }
}

typedef struct AtomicJoinedCommand AtomicJoinedCommand;
struct AtomicJoinedCommand {
  DoltliteTxnState state;
  ProllyHash committedCatalogHash;
  ProllyHash committedCatalogShape;
  DoltVcState committedVc;
  u32 committedAMeta[16];
  u8 bUserSavepoint;
  u8 bReleaseSavepoints;
  u8 bSqlWrite;
  u8 bSealOnError;
};

static int cmdBeginAtomicJoined(Btree *p){
  AtomicJoinedCommand *s;
  sqlite3 *db = p->db;
  ChunkStore *cs = &p->pBt->store;
  ProllyHash tip;
  int found = 0;
  int rc;
  if( !cs->isMemory && !cs->isBuffer ){
    rc = chunkStoreReadDiskBranchTip(cs, doltliteGetSessionBranch(db),
                                    &tip, &found);
    if( rc!=SQLITE_OK ) return rc;
    if( !found && cs->refs.nBranches>0 ) return SQLITE_DONE;
  }
  rc = sqlite3BtreeBeginTrans(p, 0, 0);
  if( rc!=SQLITE_OK ) return rc;
  s = sqlite3_malloc64(sizeof(*s));
  if( !s ) return SQLITE_NOMEM;
  memset(s, 0, sizeof(*s));
  s->bUserSavepoint = db->pSavepoint!=0;
  s->bSqlWrite = p->inTrans==TRANS_WRITE;
  rc = doltliteSaveTxnState(db, &s->state);
  if( rc==SQLITE_OK ){
    if( !s->bSqlWrite ){
      btreeStoreCommittedFromCurrent(p, &s->state.sessionCatalogHash);
    }
    s->committedCatalogHash = p->committedCatalogHash;
    s->committedCatalogShape = p->committedCatalogShape;
    s->committedVc = p->committedVc;
    memcpy(s->committedAMeta, p->committedAMeta, sizeof(s->committedAMeta));
    rc = sqlite3_exec(db, "SAVEPOINT doltlite_command", 0, 0, 0);
  }
  if( rc!=SQLITE_OK ){
    doltliteTxnStateClear(&s->state);
    sqlite3_free(s);
    return rc;
  }
  p->pAtomicCommand = s;
  p->bAtomicCommandSeal = 0;
  p->bCommandTxn = 1;
  p->bCommandWritesRows = 0;
  p->bCommandKeep = 0;
  p->nCommandBranchInstall = p->nBranchInstall;
  p->pBt->store.bDeferPublish = 1;
  p->pBt->store.bPublishDeferred = 0;
  return SQLITE_OK;
}

int doltliteDeferJoinedCommandSeal(sqlite3 *db){
  Btree *p = db->nDb>0 ? db->aDb[0].pBt : 0;
  if( !p || !p->pAtomicCommand ) return 0;
  p->bAtomicCommandSeal = 1;
  return 1;
}

int doltlitePrepareJoinedCommit(sqlite3 *db){
  Btree *p = db->nDb>0 ? db->aDb[0].pBt : 0;
  int rc = SQLITE_OK;
  if( !p || !p->pAtomicCommand ) return SQLITE_DONE;
  if( p->inTrans==TRANS_READ ){
    rc = sqlite3BtreeCommit(p);
    if( rc==SQLITE_OK ) rc = doltliteReloadSessionWorkingState(db);
    if( rc==SQLITE_OK ) rc = sqlite3BtreeBeginTrans(p, 0, 0);
  }
  if( rc==SQLITE_OK ){
    p->bAtomicCommandSeal = 1;
    ((AtomicJoinedCommand*)p->pAtomicCommand)->bSealOnError = 1;
  }
  return rc;
}

int doltliteCommandHasUserSavepoint(sqlite3 *db){
  Btree *p = db->nDb>0 ? db->aDb[0].pBt : 0;
  if( p && p->pAtomicCommand ){
    return ((AtomicJoinedCommand*)p->pAtomicCommand)->bUserSavepoint;
  }
  return db->pSavepoint!=0;
}

int doltliteDeferJoinedSavepointRelease(sqlite3 *db){
  Btree *p = db->nDb>0 ? db->aDb[0].pBt : 0;
  if( !p || !p->pAtomicCommand ) return 0;
  ((AtomicJoinedCommand*)p->pAtomicCommand)->bReleaseSavepoints = 1;
  return 1;
}

static int cmdEndAtomicJoined(Btree *p, sqlite3_context *ctx){
  AtomicJoinedCommand *s = p->pAtomicCommand;
  BtShared *pBt = p->pBt;
  ChunkStore *cs = &pBt->store;
  sqlite3 *db = p->db;
  sqlite3_stmt *commit = 0;
  void *preparedCatalog = 0;
  ProllyHash catalog;
  int installed = 0;
  int vetoed = 0;
  int nFileWrite = 0;
  int hasDoltliteWrite = 0;
  int i;
  int rc = SQLITE_OK;
  int restoreRc;
  int sealError = s && s->bSealOnError && ctx->isError==SQLITE_ERROR;
  if( !s ) return SQLITE_OK;
  if( (!ctx->isError || sealError) && p->bAtomicCommandSeal ){
    rc = sqlite3VtabSync(db, ctx->pVdbe);
    if( rc==SQLITE_OK && sqlite3VdbeCheckFkDeferred(ctx->pVdbe)!=SQLITE_OK ){
      rc = ctx->pVdbe->rc;
      sqlite3_result_error(ctx, ctx->pVdbe->zErrMsg, -1);
      sqlite3_result_error_code(ctx, rc);
    }
    if( rc==SQLITE_OK && db->nVdbeWrite>0 ) rc = SQLITE_BUSY;
    for(i=0; rc==SQLITE_OK && i<db->nDb; i++){
      Btree *pWrite = db->aDb[i].pBt;
      if( i!=1 && pWrite && sqlite3BtreeTxnState(pWrite)==SQLITE_TXN_WRITE
       && !sqlite3PagerIsMemdb(sqlite3BtreePager(pWrite))
       && sqlite3BtreeGetFilename(pWrite)[0]
       && sqlite3BtreeHasPendingChanges(pWrite) ){
        nFileWrite++;
        hasDoltliteWrite |= sqlite3BtreeIsDoltliteFormat(pWrite);
      }
    }
    if( nFileWrite>1 && hasDoltliteWrite ){
      sqlite3_result_error(ctx,
          "atomic commit across multiple file-backed databases is not supported", -1);
      rc = SQLITE_ERROR;
    }
    if( rc==SQLITE_OK ) rc = doltliteFlushCatalogToHash(db, &catalog);
    if( rc==SQLITE_OK ) rc = doltliteSaveWorkingSetWithHash(db, &catalog);
    if( rc==SQLITE_OK ) rc = chunkStoreSerializeRefs(cs);
    if( rc==SQLITE_OK ){
      rc = doltliteBtreePrepareJoinedCatalog(p, &catalog, &preparedCatalog);
    }
    if( rc==SQLITE_OK ){
      rc = sqlite3_prepare_v2(db, "COMMIT", -1, &commit, 0);
    }
    if( rc==SQLITE_OK && (s->bSqlWrite || p->bCommandWritesRows)
     && db->xCommitCallback
     && db->xCommitCallback(db->pCommitArg) ){
      rc = SQLITE_CONSTRAINT_COMMITHOOK;
      vetoed = 1;
    }
    if( rc==SQLITE_OK ){
      cs->bDeferPublish = 0;
      rc = chunkStoreCommitWithBusyHandler(cs, prollyInvokeBusyHandler, pBt);
      cs->bDeferPublish = 1;
      installed = rc==SQLITE_OK;
    }
    if( installed ){
      /* A published root cannot become a failed command during SQL cleanup. */
      (void)sqlite3_step(commit);
      sqlite3CloseSavepoints(db);
      db->autoCommit = 1;
      doltliteVcCommandInstalled(db);
    }
  }
  sqlite3_finalize(commit);
  if( !installed && ((ctx->isError && !p->bCommandKeep) || rc!=SQLITE_OK) ){
    restoreRc = sqlite3_exec(db, "ROLLBACK TO doltlite_command", 0, 0, 0);
    if( restoreRc!=SQLITE_OK ) rc = restoreRc;
    restoreRc = doltliteRestoreTxnState(db, &s->state);
    if( restoreRc!=SQLITE_OK ) rc = restoreRc;
    p->committedCatalogHash = s->committedCatalogHash;
    p->committedCatalogShape = s->committedCatalogShape;
    p->committedVc = s->committedVc;
    memcpy(p->committedAMeta, s->committedAMeta, sizeof(p->committedAMeta));
  }
  if( !installed ){
    restoreRc = sqlite3_exec(db, "RELEASE doltlite_command", 0, 0, 0);
    if( restoreRc!=SQLITE_OK ) rc = restoreRc;
  }
  doltliteNestedTxnDrop(db);
  p->bCommandTxn = 0;
  p->pAtomicCommand = 0;
  cs->bDeferPublish = 0;
  cs->bPublishDeferred = 0;
  if( s->bReleaseSavepoints && !ctx->isError && rc==SQLITE_OK ){
    rc = doltliteVcSealActiveSavepoints(db);
  }
  if( vetoed ) (void)sqlite3_exec(db, "ROLLBACK", 0, 0, 0);
  doltliteTxnStateClear(&s->state);
  sqlite3_free(s);
  doltliteBtreeFinishJoinedCommand(p, installed ? &catalog : 0, preparedCatalog);
  if( sealError && rc!=SQLITE_OK ){
    sqlite3_result_error(ctx, sqlite3ErrStr(rc), -1);
    sqlite3_result_error_code(ctx, rc);
  }
  return rc;
}

#define DOLTLITE_CMD_UNSCOPED 0
#define DOLTLITE_CMD_SCOPED   1
#define DOLTLITE_CMD_NOWAIT   2
#define DOLTLITE_CMD_ATOMIC   3

/* A missing branch, a command that needs no transaction, and a read-only
** database are reported by the command itself. A lock busy is not. */
static int cmdScopeBeginAbsorbed(Btree *pMain, int rc){
  if( rc==SQLITE_DONE || (rc&0xff)==SQLITE_READONLY ) return 1;
  return (rc&0xff)==SQLITE_BUSY
      && doltliteBtreeMissingWriteBranch(pMain)!=0;
}

/* Begin the write transaction a command runs in: its own in autocommit,
** or a hold on the graph lock while it runs in the caller's. A read-only
** command, or one on a peer-deleted branch, cannot begin one and reports
** its own refusal; *pbProbe says a no-wait command found the lock held. */
static int cmdScopeBegin(
  sqlite3 *db,
  int eScope,
  DoltliteCmdScope *pScope,
  int *pbProbe
){
  Btree *pMain = db->nDb>0 ? db->aDb[0].pBt : 0;
  ChunkStore *cs = doltliteGetChunkStore(db);
  int rc;
  memset(pScope, 0, sizeof(*pScope));
  if( pbProbe ) *pbProbe = 0;
  if( eScope==DOLTLITE_CMD_UNSCOPED || !cs || doltliteIsDetached(db) ){
    return SQLITE_OK;
  }
  if( db->autoCommit && !db->pSavepoint ){
    rc = doltliteBtreeBeginCommandTxn(pMain, eScope!=DOLTLITE_CMD_NOWAIT);
    pScope->bScope = rc==SQLITE_OK;
    /* A no-wait command finds out whether it changes anything without the
    ** lock; one that does starts over in a transaction that waits. */
    if( (rc&0xff)==SQLITE_BUSY && !cmdScopeBeginAbsorbed(pMain, rc)
     && eScope==DOLTLITE_CMD_NOWAIT && pbProbe ){
      *pbProbe = 1;
      return SQLITE_OK;
    }
    if( cmdScopeBeginAbsorbed(pMain, rc) ) return SQLITE_OK;
    return rc;
  }
  rc = doltliteBtreeBeginJoinedCommand(pMain);
  if( rc==SQLITE_OK ){
    cs->bLockHoldOnly = 1;
    rc = doltliteLockAndRefreshBusy(db, cs);
    cs->bLockHoldOnly = 0;
    if( rc==SQLITE_OK ){
      cs->nOuterLockHold = 1;
    }else{
      doltliteBtreeEndJoinedCommand(pMain);
    }
  }
  pScope->bJoined = rc==SQLITE_OK;
  pScope->bInBegin = pScope->bJoined && !doltliteSavepointIsTopLevelTxn(db);
  if( pScope->bInBegin && eScope==DOLTLITE_CMD_ATOMIC ){
    rc = cmdBeginAtomicJoined(pMain);
    pScope->bAtomic = rc==SQLITE_OK;
    if( rc!=SQLITE_OK && rc!=SQLITE_DONE ){
      doltliteBtreeEndJoinedCommand(pMain);
      cs->nOuterLockHold = 0;
      chunkStoreUnlock(cs);
      pScope->bJoined = 0;
    }
  }
  if( rc==SQLITE_DONE || (rc&0xff)==SQLITE_READONLY ) return SQLITE_OK;
  return rc;
}

static void cmdScopeResultBeginError(sqlite3_context *ctx, int rc){
  sqlite3_result_error(ctx, (rc&0xff)==SQLITE_BUSY
      ? "database is busy: another connection holds the write lock"
      : sqlite3ErrStr(rc), -1);
  sqlite3_result_error_code(ctx, rc);
}

static void cmdScopeEnd(sqlite3_context *ctx, DoltliteCmdScope *pScope){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  Btree *pMain = db->nDb>0 ? db->aDb[0].pBt : 0;
  ChunkStore *cs = doltliteGetChunkStore(db);
  if( pScope->bAtomic ){
    int rc = cmdEndAtomicJoined(pMain, ctx);
    if( rc!=SQLITE_OK && !ctx->isError ){
      sqlite3_result_error(ctx, sqlite3ErrStr(rc), -1);
      sqlite3_result_error_code(ctx, rc);
    }
  }
  if( pScope->bScope ){
    int rc = doltliteBtreeEndCommandTxn(pMain, ctx->isError!=0,
                                        ctx->pVdbe->vcInstalled);
    if( rc!=SQLITE_OK && !ctx->isError ){
      sqlite3_result_error(ctx, sqlite3ErrStr(rc), -1);
      sqlite3_result_error_code(ctx, rc);
    }
  }
  if( pScope->bJoined ){
    doltliteBtreeEndJoinedCommand(pMain);
    cs->nOuterLockHold = 0;
    chunkStoreUnlock(cs);
  }
  /* As in Dolt, a command that commits the caller's BEGIN leaves it in a
  ** new transaction: what follows still rolls back with ROLLBACK. */
  if( pScope->bInBegin && db->autoCommit ){
    int rc = sqlite3_exec(db, "BEGIN", 0, 0, 0);
    if( rc==SQLITE_OK ) db->bVcReopenedTxn = 1;
    if( rc!=SQLITE_OK && !ctx->isError ){
      sqlite3_result_error(ctx, sqlite3_errmsg(db), -1);
      sqlite3_result_error_code(ctx, rc);
    }
  }
  memset(pScope, 0, sizeof(*pScope));
}

/* For a command that does part of its work, such as a network transfer,
** without the lock: the rest runs as a scoped command would. */
int doltliteCommandScopeEnter(sqlite3_context *ctx, DoltliteCmdScope *pScope){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  int rc;
  if( db->bVcNestedCommand ){
    memset(pScope, 0, sizeof(*pScope));
    return SQLITE_OK;
  }
  rc = cmdScopeBegin(db, DOLTLITE_CMD_SCOPED, pScope, 0);
  if( rc!=SQLITE_OK ) cmdScopeResultBeginError(ctx, rc);
  return rc;
}

void doltliteCommandScopeLeave(sqlite3_context *ctx, DoltliteCmdScope *pScope){
  cmdScopeEnd(ctx, pScope);
}

static void doltliteRunCommand(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv,
  int eScope
){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  Vdbe *pOuter = db->pVcCommand;
  Btree *pMain = db->nDb>0 ? db->aDb[0].pBt : 0;
  ChunkStore *cs = doltliteGetChunkStore(db);
  u8 bWasNested = db->bVcNestedCommand;
  DoltliteCmdScope scope;
  int bProbe = 0;
  int rcBegin;
  db->pVcCommand = ctx->pVdbe;
  db->bVcNestedCommand = pOuter!=0;
  rcBegin = cmdScopeBegin(db, pOuter ? DOLTLITE_CMD_UNSCOPED : eScope,
                          &scope, &bProbe);
  if( bProbe ){
    BusyHandler saved = db->busyHandler;
    db->busyHandler.xBusyHandler = 0;
    cs->bLockProbe = 1;
    cs->bLockProbeHit = 0;
    doltliteInvokeShieldedFunc(ctx, argc, argv, 1);
    cs->bLockProbe = 0;
    db->busyHandler = saved;
    if( cs->bLockProbeHit || (ctx->isError&0xff)==SQLITE_BUSY ){
      sqlite3_result_null(ctx);
      ctx->isError = 0;
      doltliteInvalidateSessionWorkingState(db);
      rcBegin = doltliteBtreeBeginCommandTxn(pMain, 1);
      scope.bScope = rcBegin==SQLITE_OK;
      bProbe = 0;
      /* Same absorbed results as the no-wait attempt. The branch may have
      ** been deleted while this retry waited for the lock. */
      if( cmdScopeBeginAbsorbed(pMain, rcBegin) ) rcBegin = SQLITE_OK;
    }
  }
  if( rcBegin!=SQLITE_OK ){
    cmdScopeResultBeginError(ctx, rcBegin);
    db->bVcNestedCommand = bWasNested;
    db->pVcCommand = pOuter;
    return;
  }
  if( !bProbe ) doltliteInvokeShieldedFunc(ctx, argc, argv, pOuter==0);
  cmdScopeEnd(ctx, &scope);
  if( !ctx->isError ){
    doltliteVcCommandInstalled(db);
  }else if( !ctx->pVdbe->vcInstalled && AtomicLoad(&db->u1.isInterrupted) ){
    sqlite3_result_error_code(ctx, SQLITE_INTERRUPT);
  }
  /* A failed command abandons what it built; its stored hits must not
  ** refuse the next publish. */
  if( !pOuter && doltliteGetChunkStore(db) ){
    if( ctx->isError ) chunkStoreClearStoredHits(doltliteGetChunkStore(db));
    chunkStoreClearSourceCommits(doltliteGetChunkStore(db));
  }
  db->bVcNestedCommand = bWasNested;
  db->pVcCommand = pOuter;
}

static void doltliteCommandFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  doltliteRunCommand(ctx, argc, argv, DOLTLITE_CMD_SCOPED);
}

static void doltliteNoWaitCommandFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  doltliteRunCommand(ctx, argc, argv, DOLTLITE_CMD_NOWAIT);
}

static void doltliteAtomicCommandFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  int eScope = DOLTLITE_CMD_ATOMIC;
  if( sqlite3_context_db_handle(ctx)->nVdbeWrite>0 ){
    eScope = DOLTLITE_CMD_SCOPED;
  }
  if( argc==1 && sqlite3_value_type(argv[0])==SQLITE_TEXT ){
    const char *z = (const char*)sqlite3_value_text(argv[0]);
    if( z && strcmp(z, "--abort")==0 ) eScope = DOLTLITE_CMD_SCOPED;
  }
  doltliteRunCommand(ctx, argc, argv, eScope);
}

static void doltliteUnscopedCommandFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  doltliteRunCommand(ctx, argc, argv, DOLTLITE_CMD_UNSCOPED);
}

static int doltliteNameIn(const char *zName, const char *const *azList, int n){
  int i;
  for(i=0; i<n; i++){
    if( sqlite3_stricmp(zName, azList[i])==0 ) return 1;
  }
  return 0;
}

/* Commands that move data over the network without holding the local lock,
** or need the lock free (gc), manage their own transactions. dolt_clean and
** dolt_reset may change nothing, and such a call must not wait on a peer. */
static int doltliteCommandScope(const char *zName){
  static const char *const azUnscoped[] = {
    "dolt_connect_branch", "dolt_config", "dolt_creds", "dolt_creds_new",
    "dolt_gc", "dolt_remote", "dolt_push", "dolt_fetch", "dolt_pull",
    "dolt_clone",
  };
  static const char *const azNoWait[] = { "dolt_clean", "dolt_reset" };
  if( doltliteNameIn(zName, azUnscoped, ArraySize(azUnscoped)) ){
    return DOLTLITE_CMD_UNSCOPED;
  }
  if( doltliteNameIn(zName, azNoWait, ArraySize(azNoWait)) ){
    return DOLTLITE_CMD_NOWAIT;
  }
  if( sqlite3_stricmp(zName, "dolt_commit")==0
   || sqlite3_stricmp(zName, "dolt_checkout")==0
   || sqlite3_stricmp(zName, "dolt_merge")==0 ){
    return DOLTLITE_CMD_ATOMIC;
  }
  return DOLTLITE_CMD_SCOPED;
}

int doltliteCreateCommandFunc(
  sqlite3 *db,
  const char *zName,
  int nArg,
  void (*xFunc)(sqlite3_context*,int,sqlite3_value**)
){
  int eScope = doltliteCommandScope(zName);
  return sqlite3_create_function(db, zName, nArg,
      DOLTLITE_COMMAND_FUNC_FLAGS, (void*)xFunc,
      eScope==DOLTLITE_CMD_UNSCOPED ? doltliteUnscopedCommandFunc
      : eScope==DOLTLITE_CMD_NOWAIT ? doltliteNoWaitCommandFunc
      : eScope==DOLTLITE_CMD_ATOMIC ? doltliteAtomicCommandFunc
      : doltliteCommandFunc, 0, 0);
}

/* Every method of a version-control virtual table runs internal SQL on the
** connection that registered it, so the whole module is shielded. Modules
** are per connection, and SQLite points each constructed vtab at the
** registered module, which is how a method finds its inner module. */
typedef struct ShieldedModule ShieldedModule;
struct ShieldedModule {
  sqlite3_module mod;
  const sqlite3_module *pInner;
  void *pAux;
  sqlite3 *db;
};

#define SM_VTAB(pVtab) ((ShieldedModule*)(pVtab)->pModule)
#define SM_CUR(pCur) SM_VTAB((pCur)->pVtab)
#define SM_ENTER(p) DoltliteAuthShield shield; doltliteAuthShieldEnter((p)->db, &shield)
#define SM_LEAVE() doltliteAuthShieldLeave(&shield)

static int smCreate(sqlite3 *db, void *pAux, int argc, const char *const*argv,
                    sqlite3_vtab **ppVtab, char **pzErr){
  ShieldedModule *p = (ShieldedModule*)pAux;
  int rc;
  SM_ENTER(p);
  rc = p->pInner->xCreate(db, p->pAux, argc, argv, ppVtab, pzErr);
  SM_LEAVE();
  return rc;
}
static int smConnect(sqlite3 *db, void *pAux, int argc, const char *const*argv,
                     sqlite3_vtab **ppVtab, char **pzErr){
  ShieldedModule *p = (ShieldedModule*)pAux;
  int rc;
  SM_ENTER(p);
  rc = p->pInner->xConnect(db, p->pAux, argc, argv, ppVtab, pzErr);
  SM_LEAVE();
  return rc;
}
static int smBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  ShieldedModule *p = SM_VTAB(pVtab);
  int rc;
  SM_ENTER(p);
  rc = p->pInner->xBestIndex(pVtab, pInfo);
  SM_LEAVE();
  return rc;
}
static int smDisconnect(sqlite3_vtab *pVtab){
  ShieldedModule *p = SM_VTAB(pVtab);
  int rc;
  SM_ENTER(p);
  rc = p->pInner->xDisconnect(pVtab);
  SM_LEAVE();
  return rc;
}
static int smDestroy(sqlite3_vtab *pVtab){
  ShieldedModule *p = SM_VTAB(pVtab);
  int rc;
  SM_ENTER(p);
  rc = p->pInner->xDestroy(pVtab);
  SM_LEAVE();
  return rc;
}
static int smOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor){
  ShieldedModule *p = SM_VTAB(pVtab);
  int rc;
  SM_ENTER(p);
  rc = p->db->pVcCommand ? SQLITE_OK : doltliteBeginSessionRead(p->db);
  if( rc==SQLITE_OK ) rc = p->pInner->xOpen(pVtab, ppCursor);
  SM_LEAVE();
  return rc;
}
static int smClose(sqlite3_vtab_cursor *pCur){
  ShieldedModule *p = SM_CUR(pCur);
  int rc;
  SM_ENTER(p);
  rc = p->pInner->xClose(pCur);
  SM_LEAVE();
  return rc;
}
static int smFilter(sqlite3_vtab_cursor *pCur, int idxNum, const char *idxStr,
                    int argc, sqlite3_value **argv){
  ShieldedModule *p = SM_CUR(pCur);
  int rc;
  SM_ENTER(p);
  rc = p->pInner->xFilter(pCur, idxNum, idxStr, argc, argv);
  SM_LEAVE();
  return rc;
}
static int smNext(sqlite3_vtab_cursor *pCur){
  ShieldedModule *p = SM_CUR(pCur);
  int rc;
  SM_ENTER(p);
  rc = p->pInner->xNext(pCur);
  SM_LEAVE();
  return rc;
}
static int smEof(sqlite3_vtab_cursor *pCur){
  ShieldedModule *p = SM_CUR(pCur);
  int rc;
  SM_ENTER(p);
  rc = p->pInner->xEof(pCur);
  SM_LEAVE();
  return rc;
}
static int smColumn(sqlite3_vtab_cursor *pCur, sqlite3_context *ctx, int i){
  ShieldedModule *p = SM_CUR(pCur);
  int rc;
  SM_ENTER(p);
  rc = p->pInner->xColumn(pCur, ctx, i);
  SM_LEAVE();
  return rc;
}
static int smRowid(sqlite3_vtab_cursor *pCur, sqlite3_int64 *pRowid){
  ShieldedModule *p = SM_CUR(pCur);
  int rc;
  SM_ENTER(p);
  rc = p->pInner->xRowid(pCur, pRowid);
  SM_LEAVE();
  return rc;
}
static int smUpdate(sqlite3_vtab *pVtab, int argc, sqlite3_value **argv,
                    sqlite3_int64 *pRowid){
  ShieldedModule *p = SM_VTAB(pVtab);
  int rc;
  SM_ENTER(p);
  rc = p->db->pVcCommand ? SQLITE_OK : doltliteBeginSessionRead(p->db);
  if( rc==SQLITE_OK ) rc = p->pInner->xUpdate(pVtab, argc, argv, pRowid);
  SM_LEAVE();
  return rc;
}
static int smBegin(sqlite3_vtab *pVtab){
  ShieldedModule *p = SM_VTAB(pVtab);
  int rc;
  SM_ENTER(p);
  rc = p->db->pVcCommand ? SQLITE_OK : doltliteBeginSessionRead(p->db);
  if( rc==SQLITE_OK ) rc = p->pInner->xBegin(pVtab);
  SM_LEAVE();
  return rc;
}
static int smSync(sqlite3_vtab *pVtab){
  ShieldedModule *p = SM_VTAB(pVtab);
  int rc;
  SM_ENTER(p);
  rc = p->pInner->xSync(pVtab);
  SM_LEAVE();
  return rc;
}
static int smCommit(sqlite3_vtab *pVtab){
  ShieldedModule *p = SM_VTAB(pVtab);
  int rc;
  SM_ENTER(p);
  rc = p->pInner->xCommit(pVtab);
  SM_LEAVE();
  return rc;
}
static int smRollback(sqlite3_vtab *pVtab){
  ShieldedModule *p = SM_VTAB(pVtab);
  int rc;
  SM_ENTER(p);
  rc = p->pInner->xRollback(pVtab);
  SM_LEAVE();
  return rc;
}
static int smFindFunction(sqlite3_vtab *pVtab, int nArg, const char *zName,
                          void (**pxFunc)(sqlite3_context*,int,sqlite3_value**),
                          void **ppArg){
  ShieldedModule *p = SM_VTAB(pVtab);
  int rc;
  SM_ENTER(p);
  rc = p->pInner->xFindFunction(pVtab, nArg, zName, pxFunc, ppArg);
  SM_LEAVE();
  return rc;
}
static int smRename(sqlite3_vtab *pVtab, const char *zNew){
  ShieldedModule *p = SM_VTAB(pVtab);
  int rc;
  SM_ENTER(p);
  rc = p->pInner->xRename(pVtab, zNew);
  SM_LEAVE();
  return rc;
}
static int smSavepoint(sqlite3_vtab *pVtab, int iSavepoint){
  ShieldedModule *p = SM_VTAB(pVtab);
  int rc;
  SM_ENTER(p);
  rc = p->pInner->xSavepoint(pVtab, iSavepoint);
  SM_LEAVE();
  return rc;
}
static int smRelease(sqlite3_vtab *pVtab, int iSavepoint){
  ShieldedModule *p = SM_VTAB(pVtab);
  int rc;
  SM_ENTER(p);
  rc = p->pInner->xRelease(pVtab, iSavepoint);
  SM_LEAVE();
  return rc;
}
static int smRollbackTo(sqlite3_vtab *pVtab, int iSavepoint){
  ShieldedModule *p = SM_VTAB(pVtab);
  int rc;
  SM_ENTER(p);
  rc = p->pInner->xRollbackTo(pVtab, iSavepoint);
  SM_LEAVE();
  return rc;
}
static int smIntegrity(sqlite3_vtab *pVtab, const char *zSchema,
                       const char *zTabName, int mFlags, char **pzErr){
  ShieldedModule *p = SM_VTAB(pVtab);
  int rc;
  SM_ENTER(p);
  rc = p->pInner->xIntegrity(pVtab, zSchema, zTabName, mFlags, pzErr);
  SM_LEAVE();
  return rc;
}

int doltliteCreateShieldedModule(
  sqlite3 *db,
  const char *zName,
  const sqlite3_module *pInner,
  void *pAux
){
  ShieldedModule *p = sqlite3_malloc(sizeof(*p));
  sqlite3_module *m;
  if( !p ) return SQLITE_NOMEM;
  memset(p, 0, sizeof(*p));
  p->pInner = pInner;
  p->pAux = pAux;
  p->db = db;
  m = &p->mod;
  m->iVersion = pInner->iVersion;
  /* xCreate==xConnect marks an eponymous module; keep that identity. */
  if( pInner->xCreate ){
    m->xCreate = pInner->xCreate==pInner->xConnect ? smConnect : smCreate;
  }
  if( pInner->xConnect ) m->xConnect = smConnect;
  if( pInner->xBestIndex ) m->xBestIndex = smBestIndex;
  if( pInner->xDisconnect ) m->xDisconnect = smDisconnect;
  if( pInner->xDestroy ){
    m->xDestroy = pInner->xDestroy==pInner->xDisconnect ? smDisconnect : smDestroy;
  }
  if( pInner->xOpen ) m->xOpen = smOpen;
  if( pInner->xClose ) m->xClose = smClose;
  if( pInner->xFilter ) m->xFilter = smFilter;
  if( pInner->xNext ) m->xNext = smNext;
  if( pInner->xEof ) m->xEof = smEof;
  if( pInner->xColumn ) m->xColumn = smColumn;
  if( pInner->xRowid ) m->xRowid = smRowid;
  if( pInner->xUpdate ) m->xUpdate = smUpdate;
  if( pInner->xBegin ) m->xBegin = smBegin;
  if( pInner->xSync ) m->xSync = smSync;
  if( pInner->xCommit ) m->xCommit = smCommit;
  if( pInner->xRollback ) m->xRollback = smRollback;
  if( pInner->xFindFunction ) m->xFindFunction = smFindFunction;
  if( pInner->xRename ) m->xRename = smRename;
  if( pInner->iVersion>=2 ){
    if( pInner->xSavepoint ) m->xSavepoint = smSavepoint;
    if( pInner->xRelease ) m->xRelease = smRelease;
    if( pInner->xRollbackTo ) m->xRollbackTo = smRollbackTo;
  }
  if( pInner->iVersion>=3 ) m->xShadowName = pInner->xShadowName;
  if( pInner->iVersion>=4 && pInner->xIntegrity ) m->xIntegrity = smIntegrity;
  return sqlite3_create_module_v2(db, zName, m, p, sqlite3_free);
}

static void doltliteCmdResultUnknownOption(sqlite3_context *ctx, const char *zOpt);
static void doltliteCmdResultMissingOptionValue(
  sqlite3_context *ctx, const char *zOptName
);
static void doltliteCmdResultDuplicateOption(
  sqlite3_context *ctx, const char *zOptName
);

static DoltliteCmdOption *cmdFindLongOption(
  DoltliteCmdOption *aOption,
  int nOption,
  const char *zName,
  int nName
){
  int i;
  for(i=0; i<nOption; i++){
    if( aOption[i].zLong
     && sqlite3Strlen30(aOption[i].zLong)==nName
     && memcmp(aOption[i].zLong, zName, (size_t)nName)==0
    ){
      return &aOption[i];
    }
  }
  return 0;
}

static DoltliteCmdOption *cmdFindShortOption(
  DoltliteCmdOption *aOption,
  int nOption,
  char name
){
  int i;
  for(i=0; i<nOption; i++){
    if( aOption[i].shortName==name ) return &aOption[i];
  }
  return 0;
}

static int cmdValueText(
  sqlite3_context *ctx,
  sqlite3_value *pValue,
  const char **pzValue
){
  const char *zValue;
  int eType = sqlite3_value_type(pValue);
  int nValue;
  *pzValue = 0;
  zValue = (const char*)sqlite3_value_text(pValue);
  if( !zValue ){
    if( eType!=SQLITE_NULL ){
      sqlite3_result_error_nomem(ctx);
      return SQLITE_NOMEM;
    }
    return SQLITE_OK;
  }
  nValue = sqlite3_value_bytes(pValue);
  if( memchr(zValue, 0, (size_t)nValue) ){
    sqlite3_result_error(ctx,
        "command arguments may not contain NUL bytes", -1);
    return SQLITE_ERROR;
  }
  *pzValue = zValue;
  return SQLITE_OK;
}

static int cmdOptionAlreadyProvided(DoltliteCmdOption *pOption){
  if( pOption->pzValue && *pOption->pzValue ) return 1;
  if( pOption->pSeen && *pOption->pSeen ) return 1;
  return 0;
}

static int cmdSetOption(
  sqlite3_context *ctx,
  DoltliteCmdOption *pOption,
  const char *zOpt,
  const char *zAttached,
  int argc,
  sqlite3_value **argv,
  int *pI
){
  const char *zValue;
  int rc;
  if( cmdOptionAlreadyProvided(pOption) ){
    doltliteCmdResultDuplicateOption(ctx, zOpt);
    return SQLITE_ERROR;
  }
  if( pOption->eType==DOLTLITE_CMD_OPTION_FLAG ){
    if( pOption->pSeen ) *pOption->pSeen = 1;
    return SQLITE_OK;
  }
  if( zAttached && zAttached[0] ){
    zValue = zAttached;
  }else if( *pI+1<argc ){
    rc = cmdValueText(ctx, argv[++*pI], &zValue);
    if( rc!=SQLITE_OK ) return rc;
  }else{
    doltliteCmdResultMissingOptionValue(ctx, zOpt);
    return SQLITE_ERROR;
  }
  if( !zValue ){
    doltliteCmdResultMissingOptionValue(ctx, zOpt);
    return SQLITE_ERROR;
  }
  if( pOption->pSeen ) *pOption->pSeen = 1;
  if( pOption->pzValue ) *pOption->pzValue = zValue;
  return SQLITE_OK;
}

void doltliteCmdArgsClear(DoltliteCmdArgs *pArgs){
  if( !pArgs ) return;
  sqlite3_free(pArgs->apPositional);
  sqlite3_free((void*)pArgs->azPositional);
  memset(pArgs, 0, sizeof(*pArgs));
}

int doltliteCmdParseArgs(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv,
  DoltliteCmdOption *aOption,
  int nOption,
  int flags,
  DoltliteCmdArgs *pArgs
){
  int endOptions = 0;
  int i;
  memset(pArgs, 0, sizeof(*pArgs));
  pArgs->iEndOptions = -1;
  for(i=0; i<nOption; i++){
    if( aOption[i].pSeen ) *aOption[i].pSeen = 0;
    if( aOption[i].pzValue ) *aOption[i].pzValue = 0;
  }
  if( argc>0 ){
    pArgs->apPositional = sqlite3_malloc64((sqlite3_uint64)argc * sizeof(*argv));
    pArgs->azPositional = sqlite3_malloc64(
        (sqlite3_uint64)argc * sizeof(*pArgs->azPositional));
    if( !pArgs->apPositional || !pArgs->azPositional ){
      doltliteCmdArgsClear(pArgs);
      sqlite3_result_error_nomem(ctx);
      return SQLITE_NOMEM;
    }
  }
  for(i=0; i<argc; i++){
    const char *zArg;
    DoltliteCmdOption *pOption = 0;
    int rc;
    rc = cmdValueText(ctx, argv[i], &zArg);
    if( rc!=SQLITE_OK ){
      doltliteCmdArgsClear(pArgs);
      return rc;
    }
    if( !zArg ){
      if( flags & DOLTLITE_CMD_PARSE_IGNORE_NULLS ) continue;
      sqlite3_result_error(ctx, "invalid empty argument", -1);
      doltliteCmdArgsClear(pArgs);
      return SQLITE_ERROR;
    }
    if( !endOptions && strcmp(zArg, "--")==0 ){
      endOptions = 1;
      pArgs->iEndOptions = pArgs->nPositional;
      continue;
    }
    if( !endOptions && zArg[0]=='-' && zArg[1]=='-' && zArg[2] ){
      const char *zName = zArg + 2;
      const char *zEquals = strchr(zName, '=');
      int nName = zEquals ? (int)(zEquals-zName) : sqlite3Strlen30(zName);
      pOption = cmdFindLongOption(aOption, nOption, zName, nName);
      if( !pOption || (zEquals && pOption->eType!=DOLTLITE_CMD_OPTION_VALUE) ){
        doltliteCmdResultUnknownOption(ctx, zArg);
        doltliteCmdArgsClear(pArgs);
        return SQLITE_ERROR;
      }
      rc = cmdSetOption(ctx, pOption, pOption->zLong,
                        zEquals ? zEquals+1 : 0, argc, argv, &i);
      if( rc!=SQLITE_OK ){
        doltliteCmdArgsClear(pArgs);
        return rc;
      }
      continue;
    }
    if( !endOptions && zArg[0]=='-' && zArg[1] && zArg[1]!='-' ){
      int j;
      int nShort = (flags & DOLTLITE_CMD_PARSE_SHORT_GROUPS)
                 ? sqlite3Strlen30(zArg)-1 : 1;
      if( !(flags & DOLTLITE_CMD_PARSE_SHORT_GROUPS) && zArg[2] ){
        doltliteCmdResultUnknownOption(ctx, zArg);
        doltliteCmdArgsClear(pArgs);
        return SQLITE_ERROR;
      }
      for(j=1; j<=nShort; j++){
        char zOpt[2] = { zArg[j], 0 };
        pOption = cmdFindShortOption(aOption, nOption, zArg[j]);
        if( !pOption ){
          char zUnknown[3] = { '-', zArg[j], 0 };
          doltliteCmdResultUnknownOption(ctx, zUnknown);
          doltliteCmdArgsClear(pArgs);
          return SQLITE_ERROR;
        }
        rc = cmdSetOption(ctx, pOption,
            pOption->zLong ? pOption->zLong : zOpt,
            pOption->eType==DOLTLITE_CMD_OPTION_VALUE ? zArg+j+1 : 0,
            argc, argv, &i);
        if( rc!=SQLITE_OK ){
          doltliteCmdArgsClear(pArgs);
          return rc;
        }
        if( pOption->eType==DOLTLITE_CMD_OPTION_VALUE ) break;
      }
      continue;
    }
    pArgs->apPositional[pArgs->nPositional] = argv[i];
    pArgs->azPositional[pArgs->nPositional++] = zArg;
  }
  return SQLITE_OK;
}

int doltliteCmdRejectDetached(sqlite3_context *ctx){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  if( !doltliteIsDetached(db) ) return 0;
  sqlite3_result_error(ctx,
      "this operation is not supported while in a detached head state", -1);
  return 1;
}

int doltliteCmdRejectReadOnly(sqlite3_context *ctx){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  if( !cs || !chunkStoreWriteRefused(cs) ) return 0;
  sqlite3_result_error_code(ctx, SQLITE_READONLY);
  return 1;
}

/* A command that replaces the session catalog frees the table entries an
** enclosing write statement's cursors still use. */
int doltliteCmdRejectInWriteStatement(sqlite3_context *ctx, const char *zVerb){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  char *zErr;
  if( db->nVdbeWrite<=0 ) return 0;
  zErr = sqlite3_mprintf("cannot %s while a write statement is active", zVerb);
  doltliteVcResultError(ctx, db, zErr ? zErr : "write statement is active");
  sqlite3_free(zErr);
  return 1;
}

static void doltliteCmdResultUnknownOption(sqlite3_context *ctx, const char *zOpt){
  char *zErr = sqlite3_mprintf("unknown option `%s`", zOpt ? zOpt : "");
  if( zErr ){
    sqlite3_result_error(ctx, zErr, -1);
    sqlite3_free(zErr);
  }else{
    sqlite3_result_error_nomem(ctx);
  }
}

static void doltliteCmdResultDuplicateOption(
  sqlite3_context *ctx,
  const char *zOptName
){
  char *zErr = sqlite3_mprintf("error: multiple values provided for `%s'",
                               zOptName ? zOptName : "");
  if( zErr ){
    sqlite3_result_error(ctx, zErr, -1);
    sqlite3_free(zErr);
  }else{
    sqlite3_result_error_nomem(ctx);
  }
}

static void doltliteCmdResultMissingOptionValue(
  sqlite3_context *ctx,
  const char *zOptName
){
  char *zErr = sqlite3_mprintf("no value for option `%s'",
                               zOptName ? zOptName : "");
  if( zErr ){
    sqlite3_result_error(ctx, zErr, -1);
    sqlite3_free(zErr);
  }else{
    sqlite3_result_error_nomem(ctx);
  }
}

int doltliteCmdParseAuthor(
  sqlite3_context *ctx,
  const char *zAuthor,
  char **pzName,
  char **pzEmail
){
  const char *z = zAuthor;
  if( pzName ) *pzName = 0;
  if( pzEmail ) *pzEmail = 0;
  if( !z || !z[0] ){
    sqlite3_result_error(ctx, "Option 'author' requires a value", -1);
    return SQLITE_ERROR;
  }
  while( 1 ){
    const char *zEnd = strchr(z, ')');
    const char *zSep = 0;
    const char *p;
    char *zName;
    char *zEmail;
    int nEmail;
    int i;
    int j;
    if( !zEnd ) zEnd = z + strlen(z);
    for(p=z+1; p+2<zEnd; p++){
      if( p[0]==' ' && p[1]=='<' ) zSep = p;
    }
    if( zSep ){
      if( !pzName || !pzEmail ) return SQLITE_OK;
      zName = sqlite3_mprintf("%.*s", (int)(zSep-z), z);
      nEmail = (int)(zEnd-zSep-2);
      zEmail = sqlite3_malloc64((sqlite3_uint64)nEmail+1);
      if( !zName || !zEmail ){
        sqlite3_free(zName);
        sqlite3_free(zEmail);
        sqlite3_result_error_nomem(ctx);
        return SQLITE_NOMEM;
      }
      for(i=0, j=0; i<nEmail; i++){
        if( zSep[i+2]!='>' ) zEmail[j++] = zSep[i+2];
      }
      zEmail[j] = 0;
      *pzName = zName;
      *pzEmail = zEmail;
      return SQLITE_OK;
    }
    if( !zEnd[0] ) break;
    z = zEnd + 1;
  }
  sqlite3_result_error(ctx,
    "Author not formatted correctly. Use 'Name <author@example.com>' format",
    -1);
  return SQLITE_ERROR;
}

void doltliteCmdResultWriteTxnError(sqlite3_context *ctx, sqlite3 *db, int rc){
  const char *zBranch = doltliteBtreeMissingWriteBranch(db->aDb[0].pBt);
  if( !zBranch && (rc&0xff)==SQLITE_BUSY && !doltliteIsDetached(db) ){
    ChunkStore *cs = doltliteGetChunkStore(db);
    const char *zCurrent = doltliteGetSessionBranch(db);
    if( cs && cs->refs.nBranches>0
     && chunkStoreFindBranch(cs, zCurrent, 0)==SQLITE_NOTFOUND ){
      zBranch = zCurrent;
    }
  }
  if( zBranch ){
    char *zErr = sqlite3_mprintf(
        "cannot write: branch '%s' no longer exists", zBranch);
    if( !zErr ){
      sqlite3_result_error_nomem(ctx);
      return;
    }
    sqlite3_result_error(ctx, zErr, -1);
    sqlite3_free(zErr);
  }else{
    sqlite3_result_error(ctx, sqlite3_errstr(rc), -1);
  }
  sqlite3_result_error_code(ctx, rc);
}

void doltliteCmdResultBranchBusyOrGone(
  sqlite3_context *ctx, sqlite3 *db, const char *zOp
){
  ChunkStore *cs;
  const char *zCurrent;
  int gone = 0;
  if( db && db->nDb>0
   && doltliteBtreeMissingWriteBranch(db->aDb[0].pBt) ){
    gone = 1;
  }
  if( !gone && db && !doltliteIsDetached(db) ){
    cs = doltliteGetChunkStore(db);
    zCurrent = doltliteGetSessionBranch(db);
    if( cs && cs->refs.nBranches>0
     && chunkStoreFindBranch(cs, zCurrent, 0)==SQLITE_NOTFOUND ){
      gone = 1;
    }
  }
  if( gone ){
    doltliteCmdResultWriteTxnError(ctx, db, SQLITE_BUSY);
  }else{
    doltliteCmdResultPeerBranchBusy(ctx, zOp);
  }
}

void doltliteCmdResultPeerBranchBusy(sqlite3_context *ctx, const char *zOp){
  char *zErr = sqlite3_mprintf(
    "%s conflict: another connection committed to this branch. "
    "Please retry your transaction.",
    zOp ? zOp : "branch");
  if( zErr ){
    sqlite3_result_error(ctx, zErr, -1);
    sqlite3_free(zErr);
    /* The message says to retry. A bare sqlite3_result_error is rc=1, which
    ** a BUSY retry loop treats as a hard failure. */
    sqlite3_result_error_code(ctx, SQLITE_BUSY);
  }else{
    sqlite3_result_error_nomem(ctx);
  }
}

static int markPendingReplayIfNotMerging(sqlite3 *db){
  u8 isMerging = 0;
  doltliteGetSessionMergeState(db, &isMerging, 0, 0);
  if( isMerging ) return SQLITE_OK;
  return doltliteSetSessionPendingReplayCommit(db, 1);
}

static int persistPendingAndError(
  sqlite3 *db,
  sqlite3_context *ctx,
  int bRegisterConflicts,
  const char *zMsg
){
  int rc;
  if( bRegisterConflicts ){
    rc = doltliteRegisterConflictTables(db);
    if( rc!=SQLITE_OK ) return rc;
  }
  rc = doltlitePersistOrSaveWorkingSet(db);
  if( rc!=SQLITE_OK ) return rc;
  rc = markPendingReplayIfNotMerging(db);
  if( rc!=SQLITE_OK ) return rc;
  sqlite3_result_error(ctx, zMsg, -1);
  return SQLITE_OK;
}

static int cmdReportConflicts(
  sqlite3 *db,
  sqlite3_context *ctx,
  int nConflicts,
  const char *zOp
){
  char msg[256];
  sqlite3_snprintf(sizeof(msg), msg,
    "%s has %d conflict(s). Resolve and then commit with dolt_commit.",
    zOp, nConflicts);
  return persistPendingAndError(db, ctx, 1, msg);
}

static int cmdReportConstraintViolations(
  sqlite3 *db,
  sqlite3_context *ctx,
  const char *zOp
){
  char msg[256];
  sqlite3_snprintf(sizeof(msg), msg,
    "%s resulted in constraint violations. Resolve the rows in "
    "dolt_constraint_violations and then commit with dolt_commit.",
    zOp);
  return persistPendingAndError(db, ctx, 0, msg);
}

static int cmdReportConflictsAndConstraintViolations(
  sqlite3 *db,
  sqlite3_context *ctx,
  int nConflicts,
  const char *zOp
){
  char msg[384];
  sqlite3_snprintf(sizeof(msg), msg,
    "%s has %d conflict(s) and constraint violations. Resolve "
    "dolt_conflicts and dolt_constraint_violations, then commit with "
    "dolt_commit.",
    zOp ? zOp : "Operation", nConflicts);
  return persistPendingAndError(db, ctx, 1, msg);
}

static int cmdFinishPlainAfterReport(
  sqlite3 *db,
  sqlite3_context *ctx,
  DoltliteTxnState *pSaved,
  int reportRc,
  int bSealOnPlain
){
  if( reportRc!=SQLITE_OK ){
    sqlite3_result_error_code(ctx,
        doltliteRestoreTxnStateOnFailure(db, pSaved, reportRc));
    return reportRc;
  }
  if( bSealOnPlain ){
    int rc = doltliteVcSealActiveSavepoints(db);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(ctx,
          doltliteRestoreTxnStateOnFailure(db, pSaved, rc));
      return rc;
    }
  }
  doltliteTxnStateClear(pSaved);
  doltliteCommandKeepEffects(db);
  return SQLITE_OK;
}

static int cmdPersistRestored(sqlite3 *db, const ProllyHash *pCatalog){
  int rc = doltlitePersistWorkingSetWithHash(db, pCatalog);
  if( rc==SQLITE_OK ) doltliteAdoptRollbackBaseline(db, pCatalog);
  return rc;
}

static int cmdRollbackAutocommitConflict(
  sqlite3 *db,
  sqlite3_context *ctx,
  DoltliteTxnState *pSaved
){
  int rc;
  int hadTopLevelSavepoint = db->pSavepoint!=0 && db->nSavepoint==0;
  sqlite3RollbackAll(db, SQLITE_OK);
  rc = doltliteRestoreTxnState(db, pSaved);
  /* Restore merge markers even if the catalog restore failed; pSaved is
  ** cleared below, and a leftover isMerging would block the next merge.
  ** Keep the first error. */
  {
    int rc2 = doltliteSetSessionMergeState(db, pSaved->sessionIsMerging,
                                          &pSaved->sessionMergeCommit,
                                          &pSaved->sessionConflictsCatalog);
    if( rc==SQLITE_OK ) rc = rc2;
    rc2 = doltliteSetSessionConstraintViolationsCatalog(
        db, &pSaved->sessionConstraintViolationsCatalog);
    if( rc==SQLITE_OK ) rc = rc2;
  }
  if( rc==SQLITE_OK ){
    rc = cmdPersistRestored(db, &pSaved->sessionCatalogHash);
  }
  doltliteTxnStateClear(pSaved);
  if( rc==SQLITE_OK && hadTopLevelSavepoint ){
    rc = doltliteVcSealTopLevelSavepointTxn(db);
  }
  if( rc==SQLITE_OK ){
    sqlite3_result_error(ctx,
      "cannot merge: conflicts detected, autocommit transaction rolled back. "
      "Run the merge inside BEGIN/COMMIT to inspect dolt_conflicts and "
      "dolt_schema_conflicts, resolve with dolt_conflicts_resolve(), then commit "
      "with dolt_commit(). Conflicts are never committed as conflicts",
      -1);
  }
  return rc;
}

static int cmdRollbackAutocommitToSaved(
  sqlite3 *db,
  DoltliteTxnState *pSaved
){
  /* One durable write: persisting the catalog before the merge and
  ** violation state are restored would leave them paired on a crash. */
  int rc = doltliteApplyHardReset(db, &pSaved->sessionCatalogHash);
  if( rc==SQLITE_OK ){
    rc = doltliteSetSessionBranch(db, pSaved->zSessionBranch);
  }
  if( rc==SQLITE_OK ){
    doltliteSetSessionHead(db, &pSaved->sessionHead);
    rc = doltliteSetSessionStaged(db, &pSaved->sessionStaged);
  }
  if( rc==SQLITE_OK ){
    rc = doltliteSetSessionMergeState(
        db, pSaved->sessionIsMerging,
        &pSaved->sessionMergeCommit,
        &pSaved->sessionConflictsCatalog);
  }
  if( rc==SQLITE_OK ){
    rc = doltliteSetSessionConstraintViolationsCatalog(
        db, &pSaved->sessionConstraintViolationsCatalog);
  }
  if( rc==SQLITE_OK ){
    rc = doltlitePersistWorkingSet(db);
  }
  doltliteTxnStateClear(pSaved);
  return rc;
}

static int cmdRollbackAutocommitConstraintViolations(
  sqlite3 *db,
  sqlite3_context *ctx,
  DoltliteTxnState *pSaved
){
  int rc = cmdRollbackAutocommitToSaved(db, pSaved);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(ctx, rc);
    return rc;
  }
  sqlite3_result_error(ctx,
    "Committing this transaction resulted in a working set with "
    "constraint violations, transaction rolled back.", -1);
  return SQLITE_OK;
}

static int cmdNestedRestoreOrError(
  sqlite3 *db,
  sqlite3_context *ctx,
  DoltliteTxnState *pSaved
){
  int rc = doltliteRestoreTxnStateOnFailure(db, pSaved, SQLITE_OK);
  if( rc!=SQLITE_OK ) sqlite3_result_error_code(ctx, rc);
  return rc;
}

int doltliteCmdFinishWithConflicts(
  sqlite3 *db,
  sqlite3_context *ctx,
  DoltliteTxnState *pSaved,
  int nConflicts,
  const char *zOp,
  int bSealOnPlain
){
  int rc;
  switch( doltliteVcTxnMode(db) ){
  case DOLTLITE_VC_TXN_AUTOCOMMIT_LIKE:
    rc = cmdRollbackAutocommitConflict(db, ctx, pSaved);
    if( rc!=SQLITE_OK ) sqlite3_result_error_code(ctx, rc);
    return rc==SQLITE_OK ? SQLITE_OK : rc;
  case DOLTLITE_VC_TXN_PLAIN:
    return cmdFinishPlainAfterReport(db, ctx, pSaved,
        cmdReportConflicts(db, ctx, nConflicts, zOp), bSealOnPlain);
  case DOLTLITE_VC_TXN_NESTED_SAVEPOINT:
    rc = cmdNestedRestoreOrError(db, ctx, pSaved);
    if( rc!=SQLITE_OK ) return rc;
    {
      char msg[256];
      sqlite3_snprintf(sizeof(msg), msg,
        "%s has %d conflict(s). Resolve and then commit with dolt_commit.",
        zOp ? zOp : "Operation", nConflicts);
      sqlite3_result_error(ctx, msg, -1);
    }
    return SQLITE_OK;
  }
  return SQLITE_OK;
}

int doltliteCmdFinishWithConstraintViolations(
  sqlite3 *db,
  sqlite3_context *ctx,
  DoltliteTxnState *pSaved,
  const char *zOp,
  int bSealOnPlain,
  const char *zNestedMsg
){
  int rc;
  switch( doltliteVcTxnMode(db) ){
  case DOLTLITE_VC_TXN_AUTOCOMMIT_LIKE:
    return cmdRollbackAutocommitConstraintViolations(db, ctx, pSaved);
  case DOLTLITE_VC_TXN_PLAIN:
    return cmdFinishPlainAfterReport(db, ctx, pSaved,
        cmdReportConstraintViolations(db, ctx, zOp), bSealOnPlain);
  case DOLTLITE_VC_TXN_NESTED_SAVEPOINT:
    rc = cmdNestedRestoreOrError(db, ctx, pSaved);
    if( rc!=SQLITE_OK ) return rc;
    if( zNestedMsg ){
      sqlite3_result_error(ctx, zNestedMsg, -1);
    }else{
      sqlite3_result_error(ctx,
        "Merge aborted: would have introduced constraint violations. "
        "The merge and the would-be violations have been rolled back "
        "with the enclosing savepoint, so dolt_constraint_violations "
        "is empty. Re-run the operation outside a nested SAVEPOINT to "
        "inspect the violations in dolt_constraint_violations.",
        -1);
    }
    return SQLITE_OK;
  }
  return SQLITE_OK;
}

static int cmdRollbackAutocommitConflictsAndCVs(
  sqlite3 *db,
  sqlite3_context *ctx,
  DoltliteTxnState *pSaved,
  int nConflicts,
  const char *zOp
){
  char msg[384];
  int rc = cmdRollbackAutocommitToSaved(db, pSaved);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(ctx, rc);
    return rc;
  }
  sqlite3_snprintf(sizeof(msg), msg,
    "%s has %d conflict(s) and constraint violations; transaction "
    "rolled back. Run inside BEGIN/COMMIT to inspect dolt_conflicts and "
    "dolt_constraint_violations.",
    zOp ? zOp : "Operation", nConflicts);
  sqlite3_result_error(ctx, msg, -1);
  return SQLITE_OK;
}

int doltliteCmdFinishWithConflictsAndConstraintViolations(
  sqlite3 *db,
  sqlite3_context *ctx,
  DoltliteTxnState *pSaved,
  int nConflicts,
  const char *zOp,
  int bSealOnPlain,
  const char *zNestedMsg
){
  int rc;
  if( nConflicts<=0 ){
    return doltliteCmdFinishWithConstraintViolations(
        db, ctx, pSaved, zOp, bSealOnPlain, zNestedMsg);
  }
  switch( doltliteVcTxnMode(db) ){
  case DOLTLITE_VC_TXN_AUTOCOMMIT_LIKE:
    return cmdRollbackAutocommitConflictsAndCVs(
        db, ctx, pSaved, nConflicts, zOp);
  case DOLTLITE_VC_TXN_PLAIN:
    return cmdFinishPlainAfterReport(db, ctx, pSaved,
        cmdReportConflictsAndConstraintViolations(
            db, ctx, nConflicts, zOp),
        bSealOnPlain);
  case DOLTLITE_VC_TXN_NESTED_SAVEPOINT:
    rc = cmdNestedRestoreOrError(db, ctx, pSaved);
    if( rc!=SQLITE_OK ) return rc;
    if( zNestedMsg ){
      sqlite3_result_error(ctx, zNestedMsg, -1);
    }else{
      char msg[384];
      sqlite3_snprintf(sizeof(msg), msg,
        "%s has %d conflict(s) and constraint violations. The merge and "
        "would-be violations have been rolled back with the enclosing "
        "savepoint. Re-run inside BEGIN/COMMIT (no nested SAVEPOINT) to "
        "inspect dolt_conflicts and dolt_constraint_violations.",
        zOp ? zOp : "Operation", nConflicts);
      sqlite3_result_error(ctx, msg, -1);
    }
    return SQLITE_OK;
  }
  return SQLITE_OK;
}

int doltliteCmdSourceResultError(
  sqlite3_context *ctx,
  ChunkStore *cs,
  int *pRc
){
  int pendingRc = SQLITE_OK;
  char *zErr = chunkStoreSourceTakeError(cs, &pendingRc);
  if( !zErr && pendingRc==SQLITE_OK ) return 0;
  if( zErr ) sqlite3_result_error(ctx, zErr, -1);
  if( pendingRc!=SQLITE_OK ) *pRc = pendingRc;
  sqlite3_result_error_code(ctx, *pRc);
  sqlite3_free(zErr);
  return 1;
}

void doltliteCmdSourceResultErrorOrCode(
  sqlite3_context *ctx,
  ChunkStore *cs,
  int rc
){
  if( !doltliteCmdSourceResultError(ctx, cs, &rc) ){
    sqlite3_result_error_code(ctx, rc);
  }
}

void doltliteCmdReportInvalidCommitHash(
  sqlite3_context *ctx,
  ChunkStore *cs,
  int rc
){
  if( doltliteCmdSourceResultError(ctx, cs, &rc) ) return;
  if( rc==SQLITE_NOTFOUND || rc==SQLITE_ERROR ){
    sqlite3_result_error(ctx, "invalid commit hash", -1);
  }else{
    sqlite3_result_error_code(ctx, rc);
  }
}

int doltliteVtabMapChunkSourceError(
  sqlite3_vtab *pVtab,
  sqlite3 *db,
  int sourceRc,
  int mappedRc
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  int pendingRc = SQLITE_OK;
  char *zErr = cs ? chunkStoreSourceTakeError(cs, &pendingRc) : 0;
  if( !zErr && pendingRc==SQLITE_OK ) return mappedRc;
  if( zErr ){
    sqlite3_free(pVtab->zErrMsg);
    pVtab->zErrMsg = zErr;
  }
  return pendingRc!=SQLITE_OK ? pendingRc : sourceRc;
}

int doltliteCmdReportLoadParentedCommitError(
  sqlite3_context *ctx,
  ChunkStore *cs,
  int rc,
  DoltliteCommit *pTarget,
  DoltliteCommit *pParent,
  DoltliteCommit *pOurs,
  const char *zInitialMsg
){
  if( rc==SQLITE_OK ) return 0;
  if( rc==SQLITE_NOTFOUND ){
    doltliteCommitClear(pTarget);
    doltliteCommitClear(pParent);
    doltliteCommitClear(pOurs);
    if( !doltliteCmdSourceResultError(ctx, cs, &rc) ){
      sqlite3_result_error(ctx, "commit not found", -1);
    }
    return 1;
  }
  if( rc==SQLITE_EMPTY ){
    doltliteCommitClear(pTarget);
    sqlite3_result_error(ctx, zInitialMsg, -1);
    return 1;
  }
  if( rc==SQLITE_DONE ){
    doltliteCommitClear(pTarget);
    doltliteCommitClear(pParent);
    sqlite3_result_error(ctx, "no commits on current branch", -1);
    return 1;
  }
  doltliteCommitClear(pTarget);
  doltliteCommitClear(pParent);
  doltliteCommitClear(pOurs);
  doltliteCmdSourceResultErrorOrCode(ctx, cs, rc);
  return 1;
}

void doltliteCmdFinishApplyMerged(
  sqlite3_context *ctx,
  ChunkStore *cs,
  int rc,
  int nConflicts,
  char *zApplyErr,
  const char *zOp,
  const char *zRef,
  const char *zDoneMsg,
  const char *zFailFmt,
  const char *zFailFallback,
  const char *hexBuf
){
  if( rc==SQLITE_BUSY ){
    sqlite3_free(zApplyErr);
    if( !doltliteCmdSourceResultError(ctx, cs, &rc) ){
      doltliteCmdResultPeerBranchBusy(ctx, zOp);
    }
    return;
  }
  if( rc==SQLITE_DONE ){
    sqlite3_free(zApplyErr);
    sqlite3_result_error(ctx, zDoneMsg, -1);
    return;
  }
  if( rc!=SQLITE_OK ){
    if( !doltliteCmdSourceResultError(ctx, cs, &rc) ){
      if( zApplyErr ){
        sqlite3_result_error(ctx, zApplyErr, -1);
      }else{
        char *zMsg = sqlite3_mprintf(zFailFmt, zRef);
        sqlite3_result_error(ctx, zMsg ? zMsg : zFailFallback, -1);
        sqlite3_free(zMsg);
      }
      /* Setting the code after the message keeps the message, so a caller can
      ** tell a read-only database or a busy peer from a failed apply. */
      if( rc!=SQLITE_ERROR && rc!=SQLITE_NOTFOUND ){
        sqlite3_result_error_code(ctx, rc);
      }
    }
    sqlite3_free(zApplyErr);
    return;
  }
  sqlite3_free(zApplyErr);
  if( nConflicts > 0 ) return;
  if( hexBuf && hexBuf[0] ){
    sqlite3_result_text(ctx, hexBuf, -1, SQLITE_TRANSIENT);
  }
}

#endif
