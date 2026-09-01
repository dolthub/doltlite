
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_ancestor.h"
#include "doltlite_remote.h"
#include "doltlite_commit.h"
#include "doltlite_internal.h"
#include "doltlite_creds.h"
#include "prolly_btree_int.h"
#include <string.h>
#include <stdlib.h>

#if DOLTLITE_ENABLE_REMOTES

typedef struct RemoteMutationCtx RemoteMutationCtx;
struct RemoteMutationCtx {
  const char *zName;
  const char *zUrl;
  int isDelete;
};

static const char *remoteSqlNormalizeName(
  sqlite3_context *ctx,
  const char *zName,
  char **pzOwned
){
  const char *zInput = zName;
  const char *zEnd;
  *pzOwned = 0;
  while( sqlite3Isspace(*zName) ) zName++;
  zEnd = zName + strlen(zName);
  while( zEnd>zName && sqlite3Isspace(zEnd[-1]) ) zEnd--;
  if( zName==zInput && zEnd[0]==0 ) return zName;
  *pzOwned = sqlite3_mprintf("%.*s", (int)(zEnd-zName), zName);
  if( !*pzOwned ) sqlite3_result_error_nomem(ctx);
  return *pzOwned;
}

static int remoteSqlNameIsValid(const char *zName){
  return strpbrk(zName,
    " \t\n\r./\\!@#$%^&*(){}[],.<>'\"?=+|")==0;
}

static int mutateRemoteRef(sqlite3 *db, ChunkStore *cs, void *pArg){
  RemoteMutationCtx *p = (RemoteMutationCtx*)pArg;
  (void)db;
  if( p->isDelete ) return chunkStoreDeleteRemote(cs, p->zName);
  return chunkStoreAddRemote(cs, p->zName, p->zUrl);
}

static DoltliteRemote *openRemoteByUrl(sqlite3_vfs *pVfs, const char *zUrl){
  if( strncmp(zUrl, "file://", 7)==0 ){
    return doltliteFsRemoteOpen(pVfs, zUrl + 7);
  }
  if( strncmp(zUrl, "http://", 7)==0 || strncmp(zUrl, "https://", 8)==0 ){

    return doltliteHttpRemoteOpen(zUrl);
  }

  return 0;
}

static void remoteSqlResultError(
  sqlite3_context *ctx,
  int rc,
  const char *zMsg
){
  if( zMsg ){
    sqlite3_result_error(ctx, zMsg, -1);
    sqlite3_result_error_code(ctx, rc);
  }else{
    sqlite3_result_error_code(ctx, rc);
  }
}

static const char *remoteSqlRemoteMsg(DoltliteRemote *pRemote, int rc){
  const char *z;
  if( pRemote && pRemote->xErrMsg ){
    z = pRemote->xErrMsg(pRemote);
    if( z && z[0] ) return z;
  }
  if( rc==SQLITE_BUSY ) return "push failed (remote refs changed)";
  if( rc==SQLITE_CONSTRAINT ){
    return "not a fast-forward of the remote branch (use force to overwrite)";
  }
  if( rc==SQLITE_AUTH ) return "remote unauthorized";
  if( rc==SQLITE_TOOBIG ) return "remote payload too large";
  return 0;
}

static void remoteSqlRestoreAndReport(
  sqlite3_context *ctx,
  sqlite3 *db,
  ChunkStore *cs,
  DoltliteTxnState *pSavedState,
  int opRc,
  const char *zMsg
){
  int restoreRc;
  (void)cs;
  restoreRc = doltliteRestoreTxnState(db, pSavedState);
  doltliteTxnStateClear(pSavedState);
  if( restoreRc!=SQLITE_OK ){
    sqlite3_result_error_code(ctx, restoreRc);
    return;
  }
  (void)doltliteVcSealSavepointError(db);
  remoteSqlResultError(ctx, opRc, zMsg);
}

static void remoteSqlClearAndSucceed(
  sqlite3_context *ctx,
  DoltliteTxnState *pSavedState
){
  doltliteTxnStateClear(pSavedState);
  sqlite3_result_int(ctx, 0);
}

static void remoteSqlExpireCurrentStatement(sqlite3 *db){
  sqlite3ExpirePreparedStatements(db, 1);
}

static int remoteSqlOpenNamedRemote(
  ChunkStore *cs,
  const char *zRemoteName,
  const char **pzUrl,
  DoltliteRemote **ppRemote
){
  int rc = chunkStoreFindRemote(cs, zRemoteName, pzUrl);
  if( rc!=SQLITE_OK || !*pzUrl ){
    return SQLITE_NOTFOUND;
  }

  *ppRemote = openRemoteByUrl(chunkFileGetVfs(&cs->file), *pzUrl);
  if( !*ppRemote ){
    return SQLITE_CANTOPEN;
  }
  return SQLITE_OK;
}

/* Report NOTFOUND/CANTOPEN from remoteSqlOpenNamedRemote; 1 means caller
** returns. pSaved is pull's txn state, or 0. */
static int remoteSqlReportOpenError(
  sqlite3_context *ctx,
  sqlite3 *db,
  int rc,
  DoltliteTxnState *pSaved
){
  if( rc!=SQLITE_NOTFOUND && rc!=SQLITE_CANTOPEN ) return 0;
  (void)doltliteVcSealSavepointError(db);
  if( pSaved ) doltliteTxnStateClear(pSaved);
  sqlite3_result_error(ctx,
    rc==SQLITE_NOTFOUND ? "remote not found"
      : "failed to open remote (URL must start with file:// or http://)", -1);
  return 1;
}

static int remoteSqlResetSessionToCommit(
  sqlite3 *db,
  const char *zBranch,
  const ProllyHash *pCommitHash
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHash catHash;
  int rc;

  if( !cs ) return SQLITE_ERROR;
  rc = doltliteCommitCatalogHash(db, pCommitHash, &catHash);
  if( rc!=SQLITE_OK ) return rc;

  if( zBranch ){
    rc = doltliteSetSessionBranch(db, zBranch);
    if( rc!=SQLITE_OK ) return rc;
  }
  rc = doltliteHardReset(db, &catHash);
  if( rc==SQLITE_OK ){
    doltliteSetSessionHead(db, pCommitHash);
    rc = doltliteSetSessionStaged(db, &catHash);
  }
  return rc;
}

static void doltRemoteFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  RemoteMutationCtx m;
  const char *zAction;
  const char *zName;
  int rc;

  if( !cs ){
    doltliteVcResultError(ctx, db, "no database");
    return;
  }
  if( argc<2 ){
    doltliteVcResultError(ctx, db, "usage: dolt_remote(action, name [, url])");
    return;
  }

  memset(&m, 0, sizeof(m));

  zAction = (const char*)sqlite3_value_text(argv[0]);
  zName = (const char*)sqlite3_value_text(argv[1]);
  if( !zAction || !zName ){
    doltliteVcResultError(ctx, db, "action and name required");
    return;
  }

  if( strcmp(zAction, "add")==0 ){
    const char *zUrl;
    const char *zNormalizedName;
    char *zOwnedName;
    if( argc<3 ){
      doltliteVcResultError(ctx, db, "url required for add");
      return;
    }
    if( argc>3 ){
      doltliteVcResultError(ctx, db, "too many arguments");
      return;
    }
    zUrl = (const char*)sqlite3_value_text(argv[2]);
    if( !zUrl ){
      doltliteVcResultError(ctx, db, "url required for add");
      return;
    }
    zNormalizedName = remoteSqlNormalizeName(ctx, zName, &zOwnedName);
    if( !zNormalizedName ) return;
    if( !remoteSqlNameIsValid(zNormalizedName) ){
      sqlite3_free(zOwnedName);
      doltliteVcResultError(ctx, db, "remote name invalid");
      return;
    }
    m.zName = zNormalizedName;
    m.zUrl = zUrl;
    rc = doltliteMutateRefs(db, mutateRemoteRef, &m);
    sqlite3_free(zOwnedName);
    if( rc!=SQLITE_OK ){
      (void)doltliteVcSealSavepointError(db);
      remoteSqlResultError(ctx, rc,
        rc==SQLITE_ERROR ? "remote already exists" : 0);
      return;
    }
  }else if( strcmp(zAction, "remove")==0 ){
    const char *zNormalizedName;
    char *zOwnedName;
    if( argc>2 ){
      doltliteVcResultError(ctx, db, "too many arguments");
      return;
    }
    zNormalizedName = remoteSqlNormalizeName(ctx, zName, &zOwnedName);
    if( !zNormalizedName ) return;
    m.zName = zNormalizedName;
    m.isDelete = 1;
    rc = doltliteMutateRefs(db, mutateRemoteRef, &m);
    sqlite3_free(zOwnedName);
    if( rc!=SQLITE_OK ){
      (void)doltliteVcSealSavepointError(db);
      remoteSqlResultError(ctx, rc,
        rc==SQLITE_NOTFOUND ? "remote not found" : 0);
      return;
    }
  }else{
    doltliteVcResultError(ctx, db, "unknown action: use 'add' or 'remove'");
    return;
  }

  rc = doltliteVcSealActiveSavepoints(db);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(ctx, rc);
    return;
  }
  sqlite3_result_int(ctx, 0);
}

static void doltPushParsedFunc(
  sqlite3_context *ctx,
  const char *zRemoteName,
  const char *zBranch,
  int bForce
){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  DoltliteRemote *pRemote = 0;
  const char *zUrl = 0;
  int rc;

  if( !cs ){ doltliteVcResultError(ctx, db, "no database"); return; }

  rc = remoteSqlOpenNamedRemote(cs, zRemoteName, &zUrl, &pRemote);
  if( remoteSqlReportOpenError(ctx, db, rc, 0) ) return;

  rc = doltlitePush(cs, pRemote, zBranch, bForce);
  if( rc!=SQLITE_OK ){
    const char *zMsg = remoteSqlRemoteMsg(pRemote, rc);
    char *zOwned;
    if( !zMsg && rc==SQLITE_ERROR ){
      zMsg = "push failed (not a fast-forward?)";
    }
    zOwned = zMsg ? sqlite3_mprintf("%s", zMsg) : 0;
    pRemote->xClose(pRemote);
    (void)doltliteVcSealSavepointError(db);
    remoteSqlResultError(ctx, rc, zOwned);
    sqlite3_free(zOwned);
    return;
  }
  pRemote->xClose(pRemote);
  sqlite3_result_int(ctx, 0);
}

static void doltPushFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  DoltliteCmdArgs args;
  int bForce = 0;
  DoltliteCmdOption aOption[] = {
    { "force", 0, DOLTLITE_CMD_OPTION_FLAG, &bForce, 0 }
  };
  int rc;

  if( argc<2 ){
    doltliteVcResultError(ctx, db,
        "usage: dolt_push(remote, branch [, '--force'])");
    return;
  }
  rc = doltliteCmdParseArgs(ctx, argc, argv, aOption, ArraySize(aOption),
                            0, &args);
  if( rc!=SQLITE_OK ){
    (void)doltliteVcSealSavepointError(db);
    return;
  }
  if( args.nPositional<2 ){
    doltliteCmdArgsClear(&args);
    doltliteVcResultError(ctx, db, "remote and branch required");
    return;
  }
  if( args.nPositional>2 ){
    doltliteCmdArgsClear(&args);
    doltliteVcResultError(ctx, db, "too many arguments");
    return;
  }
  doltPushParsedFunc(ctx, args.azPositional[0], args.azPositional[1], bForce);
  doltliteCmdArgsClear(&args);
}

static int parseRemoteBranchNames(
  DoltliteRemote *pRemote,
  char ***pazNames,
  int *pnNames
){
  u8 *refsData = 0;
  int nRefsData = 0;
  int rc;
  ChunkStore refsView;
  char **azNames = 0;
  int nNames = 0;
  int i;

  *pazNames = 0;
  *pnNames = 0;

  rc = pRemote->xGetRefs(pRemote, &refsData, &nRefsData);
  if( rc!=SQLITE_OK ) return rc;
  if( !refsData || nRefsData <= 0 ){
    sqlite3_free(refsData);
    return SQLITE_CORRUPT;
  }

  memset(&refsView, 0, sizeof(refsView));
  rc = chunkStoreLoadRefsFromBlob(&refsView, refsData, nRefsData);
  sqlite3_free(refsData);
  if( rc!=SQLITE_OK ){
    chunkStoreClose(&refsView);
    return rc;
  }
  {
    int nBr;
    const BranchRef *aBr;
    refsTableGetBranches(&refsView.refs, &nBr, &aBr);
    if( nBr > nRefsData / 44 ){
      chunkStoreClose(&refsView);
      return SQLITE_CORRUPT;
    }
    if( nBr>0 ){
      azNames = sqlite3_malloc(nBr * sizeof(char*));
      if( !azNames ){
        chunkStoreClose(&refsView);
        return SQLITE_NOMEM;
      }
      memset(azNames, 0, nBr * sizeof(char*));
      for(i=0; i<nBr; i++){
        azNames[nNames] = sqlite3_mprintf("%s", aBr[i].zName);
        if( !azNames[nNames] ){
          doltliteFreeStringArray(azNames, nNames);
          chunkStoreClose(&refsView);
          return SQLITE_NOMEM;
        }
        nNames++;
      }
    }
  }
  chunkStoreClose(&refsView);
  *pazNames = azNames;
  *pnNames = nNames;
  return SQLITE_OK;
}

static void doltFetchFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  DoltliteRemote *pRemote = 0;
  const char *zUrl = 0;
  const char *zRemoteName;
  int rc;

  if( !cs ){ doltliteVcResultError(ctx, db, "no database"); return; }
  if( argc<1 ){
    doltliteVcResultError(ctx, db, "usage: dolt_fetch(remote [, branch])");
    return;
  }

  zRemoteName = (const char*)sqlite3_value_text(argv[0]);
  if( !zRemoteName ){
    doltliteVcResultError(ctx, db, "remote name required");
    return;
  }
  if( argc>2 ){
    doltliteVcResultError(ctx, db, "too many arguments");
    return;
  }

  rc = remoteSqlOpenNamedRemote(cs, zRemoteName, &zUrl, &pRemote);
  if( remoteSqlReportOpenError(ctx, db, rc, 0) ) return;

  if( argc>=2 && sqlite3_value_type(argv[1])!=SQLITE_NULL ){

    const char *zBranch = (const char*)sqlite3_value_text(argv[1]);
    if( !zBranch ){
      pRemote->xClose(pRemote);
      doltliteVcResultError(ctx, db, "branch name required");
      return;
    }
    rc = doltliteFetch(cs, pRemote, zRemoteName, zBranch);
    if( rc!=SQLITE_OK ){
      const char *zMsg = remoteSqlRemoteMsg(pRemote, rc);
      char *zOwned = zMsg ? sqlite3_mprintf("%s", zMsg) : 0;
      pRemote->xClose(pRemote);
      (void)doltliteVcSealSavepointError(db);
      remoteSqlResultError(ctx, rc,
        zOwned ? zOwned
        : (rc==SQLITE_NOTFOUND ? "fetch failed: branch not found on remote" : 0));
      sqlite3_free(zOwned);
      return;
    }
  }else{

    char **azNames = 0;
    int nNames = 0;
    int i;
    char *zUrlOwned;

    rc = parseRemoteBranchNames(pRemote, &azNames, &nNames);
    if( rc!=SQLITE_OK ){
      pRemote->xClose(pRemote);
      doltliteVcResultError(ctx, db, "failed to read remote refs");
      return;
    }

    pRemote->xClose(pRemote);
    pRemote = 0;

    /* zUrl points into cs->refs.aRemotes, which doltliteFetch may reallocate. */
    zUrlOwned = sqlite3_mprintf("%s", zUrl);
    if( !zUrlOwned ){
      doltliteFreeStringArray(azNames, nNames);
      sqlite3_result_error_nomem(ctx);
      return;
    }

    for(i=0; i<nNames; i++){
      DoltliteRemote *pBrRemote = openRemoteByUrl(chunkFileGetVfs(&cs->file), zUrlOwned);
      if( !pBrRemote ){
        doltliteFreeStringArray(azNames, nNames);
        sqlite3_free(zUrlOwned);
        doltliteVcResultError(ctx, db, "failed to open remote (URL must start with file:// or http://)");
        return;
      }
      rc = doltliteFetch(cs, pBrRemote, zRemoteName, azNames[i]);
      if( rc!=SQLITE_OK ){
        const char *zMsg = remoteSqlRemoteMsg(pBrRemote, rc);
        char *zOwned = zMsg ? sqlite3_mprintf("%s", zMsg) : 0;
        pBrRemote->xClose(pBrRemote);
        doltliteFreeStringArray(azNames, nNames);
        sqlite3_free(zUrlOwned);
        (void)doltliteVcSealSavepointError(db);
        remoteSqlResultError(ctx, rc, zOwned ? zOwned : "fetch failed");
        sqlite3_free(zOwned);
        return;
      }
      pBrRemote->xClose(pBrRemote);
    }
    doltliteFreeStringArray(azNames, nNames);
    sqlite3_free(zUrlOwned);
  }

  if( pRemote ) pRemote->xClose(pRemote);
  sqlite3_result_int(ctx, 0);
}

typedef struct PullAdvanceCtx PullAdvanceCtx;
struct PullAdvanceCtx {
  const char *zBranch;
  ProllyHash newTip;
  int isCreate;
};

static int mutatePullAdvance(sqlite3 *db, ChunkStore *cs, void *pArg){
  PullAdvanceCtx *p = (PullAdvanceCtx*)pArg;
  (void)db;
  if( p->isCreate ) return chunkStoreAddBranch(cs, p->zBranch, &p->newTip);
  return chunkStoreUpdateBranch(cs, p->zBranch, &p->newTip);
}

static void doltPullFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  DoltliteRemote *pRemote = 0;
  const char *zUrl = 0;
  const char *zRemoteName;
  const char *zBranch;
  ProllyHash trackingCommit, localCommit;
  DoltliteTxnState savedState;
  int dirty = 0;
  int rc;

  if( !cs ){ doltliteVcResultError(ctx, db, "no database"); return; }
  if( argc<2 ){
    doltliteVcResultError(ctx, db, "usage: dolt_pull(remote, branch)");
    return;
  }

  zRemoteName = (const char*)sqlite3_value_text(argv[0]);
  zBranch = (const char*)sqlite3_value_text(argv[1]);
  if( !zRemoteName || !zBranch ){
    doltliteVcResultError(ctx, db, "remote and branch required");
    return;
  }
  if( argc>2 ){
    doltliteVcResultError(ctx, db, "too many arguments");
    return;
  }
  memset(&savedState, 0, sizeof(savedState));

  rc = doltliteSaveTxnState(db, &savedState);
  if( rc!=SQLITE_OK ){
    (void)doltliteVcSealSavepointError(db);
    sqlite3_result_error_code(ctx, rc);
    return;
  }

  rc = remoteSqlOpenNamedRemote(cs, zRemoteName, &zUrl, &pRemote);
  if( remoteSqlReportOpenError(ctx, db, rc, &savedState) ) return;

  rc = doltliteFetch(cs, pRemote, zRemoteName, zBranch);
  pRemote->xClose(pRemote);
  if( rc!=SQLITE_OK ){
    remoteSqlRestoreAndReport(ctx, db, cs, &savedState, rc,
      rc==SQLITE_NOTFOUND ? "fetch failed: branch not found on remote" : 0);
    return;
  }

  rc = chunkStoreFindTracking(cs, zRemoteName, zBranch, &trackingCommit);
  if( rc!=SQLITE_OK || prollyHashIsEmpty(&trackingCommit) ){
    remoteSqlRestoreAndReport(ctx, db, cs, &savedState, SQLITE_ERROR,
                              "tracking branch not found after fetch");
    return;
  }

  rc = chunkStoreFindBranch(cs, zBranch, &localCommit);
  if( rc!=SQLITE_OK ){
    /* Persist the new local branch under the lock; an unlocked in-memory
    ** mutation would be clobbered or left unpersisted. */
    DoltliteBranchExpectation exp;
    PullAdvanceCtx adv;
    exp.zBranch = zBranch;
    exp.pTip = 0;
    adv.zBranch = zBranch;
    adv.newTip = trackingCommit;
    adv.isCreate = 1;
    rc = doltliteMutateRefsExpected(db, &exp, 1, mutatePullAdvance, &adv);
    if( rc==SQLITE_BUSY ){
      doltliteCmdResultPeerBranchBusy(ctx, "pull");
      (void)doltliteRestoreTxnStateOnFailure(db, &savedState, rc);
      return;
    }
    if( rc!=SQLITE_OK ){
      remoteSqlRestoreAndReport(ctx, db, cs, &savedState, SQLITE_ERROR,
                                "failed to create local branch");
      return;
    }
    remoteSqlClearAndSucceed(ctx, &savedState);
    return;
  }

  if( prollyHashCompare(&localCommit, &trackingCommit)==0 ){
    remoteSqlClearAndSucceed(ctx, &savedState);
    return;
  }

  {
    ProllyHash ancestor;
    rc = doltliteFindAncestor(db, &trackingCommit, &localCommit, &ancestor);
    if( rc!=SQLITE_OK ){
      remoteSqlRestoreAndReport(ctx, db, cs, &savedState, rc, 0);
      return;
    }
    if( prollyHashCompare(&ancestor, &localCommit)!=0 ){
      char *zTrackingRef;
      if( strcmp(zBranch, doltliteGetSessionBranch(db))!=0 ){
        remoteSqlRestoreAndReport(
          ctx, db, cs, &savedState, SQLITE_ERROR,
          "cannot pull non-current branch without fast-forward");
        return;
      }
      if( strcmp(zRemoteName, "origin")==0
       && chunkStoreOriginSourceEnabled(cs) ){
        remoteSqlRestoreAndReport(
          ctx, db, cs, &savedState, SQLITE_ERROR,
          "cannot merge a non-fast-forward pull in a lazy store; "
          "materialize the store first");
        return;
      }
      /* Merge owns txn save/restore; drop pull's snapshot first. */
      doltliteTxnStateClear(&savedState);
      zTrackingRef = sqlite3_mprintf("%s/%s", zRemoteName, zBranch);
      if( !zTrackingRef ){
        sqlite3_result_error_nomem(ctx);
        return;
      }
      rc = doltliteMergeRef(db, ctx, zTrackingRef, 0, 0, 0, 0);
      sqlite3_free(zTrackingRef);
      if( rc!=SQLITE_OK ){
        return;
      }
      /* Pull returns 0 on success, not the merge commit hash. */
      sqlite3_result_int(ctx, 0);
      return;
    }
  }

  if( strcmp(zBranch, doltliteGetSessionBranch(db))==0 ){
    rc = doltliteHasUncommittedChanges(db, &dirty);
    if( rc!=SQLITE_OK ){
      remoteSqlRestoreAndReport(ctx, db, cs, &savedState, rc, 0);
      return;
    }
    if( dirty ){
      remoteSqlRestoreAndReport(ctx, db, cs, &savedState, SQLITE_ERROR,
                                "cannot pull with uncommitted changes");
      return;
    }
  }

  /* CAS-advance the branch: force-refresh under the graph lock, compare the
  ** on-disk tip to the fast-forward base, restore refs on failure. A stale
  ** view would clobber a peer ref change. */
  {
    DoltliteBranchExpectation exp;
    PullAdvanceCtx adv;
    exp.zBranch = zBranch;
    exp.pTip = &localCommit;
    adv.zBranch = zBranch;
    adv.newTip = trackingCommit;
    adv.isCreate = 0;
    rc = doltliteMutateRefsExpected(db, &exp, 1, mutatePullAdvance, &adv);
  }
  if( rc!=SQLITE_OK ){
    if( rc==SQLITE_BUSY ){
      doltliteCmdResultPeerBranchBusy(ctx, "pull");
      (void)doltliteRestoreTxnStateOnFailure(db, &savedState, rc);
    }else{
      remoteSqlRestoreAndReport(ctx, db, cs, &savedState, rc,
                                "failed to update branch");
    }
    return;
  }

  if( strcmp(zBranch, doltliteGetSessionBranch(db))==0 ){
    rc = remoteSqlResetSessionToCommit(db, 0, &trackingCommit);
    if( rc!=SQLITE_OK ){
      remoteSqlRestoreAndReport(ctx, db, cs, &savedState, SQLITE_ERROR,
                                "failed to update working tree from branch");
      return;
    }
  }
  doltliteTxnStateClear(&savedState);
  rc = doltliteVcSealBranchStyleTxn(db);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(ctx, rc);
    return;
  }
  sqlite3_result_int(ctx, 0);
}

static void doltCloneFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  DoltliteRemote *pRemote = 0;
  DoltliteCmdArgs args;
  DoltliteCmdOption aOption[] = {
    { "lazy", 0, DOLTLITE_CMD_OPTION_FLAG, 0, 0 }
  };
  const char *zUrl;
  DoltliteTxnState savedState;
  int bLazy = 0;
  int dirty = 0;
  int rc;

  if( !cs ){ doltliteVcResultError(ctx, db, "no database"); return; }
  if( argc<1 ){
    doltliteVcResultError(ctx, db, "usage: dolt_clone(['--lazy'], url)");
    return;
  }

  aOption[0].pSeen = &bLazy;
  rc = doltliteCmdParseArgs(ctx, argc, argv, aOption, ArraySize(aOption),
                            0, &args);
  if( rc!=SQLITE_OK ){
    (void)doltliteVcSealSavepointError(db);
    return;
  }
  if( args.nPositional<1 ){
    doltliteCmdArgsClear(&args);
    doltliteVcResultError(ctx, db, "url required");
    return;
  }
  if( args.nPositional>1 ){
    doltliteCmdArgsClear(&args);
    doltliteVcResultError(ctx, db, "too many arguments");
    return;
  }
  zUrl = args.azPositional[0];
  doltliteCmdArgsClear(&args);
  memset(&savedState, 0, sizeof(savedState));

  rc = doltliteHasUncommittedChanges(db, &dirty);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(ctx, rc);
    return;
  }
  if( dirty ){
    doltliteVcResultError(ctx, db,
      "database has uncommitted changes — clone into a fresh database");
    return;
  }

  if( !chunkStoreIsEmpty(cs) ){
    int virgin = 0;
    int nBr;
    const BranchRef *aBr;
    refsTableGetBranches(&cs->refs, &nBr, &aBr);
    if( nBr==1 ){
      DoltliteCommit c;
      memset(&c, 0, sizeof(c));
      if( doltliteLoadCommit(db, &aBr[0].commitHash, &c)==SQLITE_OK
       && doltliteCommitParentCount(&c)==0 ){
        virgin = 1;
      }
      doltliteCommitClear(&c);
    }
    if( !virgin ){
      doltliteVcResultError(ctx, db, "database is not empty — clone into a fresh database");
      return;
    }
  }

  rc = doltliteSaveTxnState(db, &savedState);
  if( rc!=SQLITE_OK ){
    (void)doltliteVcSealSavepointError(db);
    sqlite3_result_error_code(ctx, rc);
    return;
  }

  if( !chunkStoreIsEmpty(cs) ){
    chunkStoreClearRefs(cs);
  }

  pRemote = openRemoteByUrl(chunkFileGetVfs(&cs->file), zUrl);
  if( !pRemote ){
    /* openRemoteByUrl is NULL for bad schemes and open failures. file:// or
    ** http(s):// that fail to open are CANTOPEN, not a scheme error. */
    int openRc = SQLITE_ERROR;
    const char *zMsg =
      "failed to open remote (URL must start with file:// or http://)";
    if( zUrl
     && (strncmp(zUrl, "file://", 7)==0
      || strncmp(zUrl, "http://", 7)==0
      || strncmp(zUrl, "https://", 8)==0) ){
      openRc = SQLITE_CANTOPEN;
      zMsg = "failed to open remote";
    }
    remoteSqlRestoreAndReport(ctx, db, cs, &savedState, openRc, zMsg);
    return;
  }

  if( bLazy ){
    rc = doltliteOriginSourceEnable(cs, db, 0);
    if( rc!=SQLITE_OK ){
      pRemote->xClose(pRemote);
      remoteSqlRestoreAndReport(ctx, db, cs, &savedState, rc,
                                "failed to initialize lazy clone");
      return;
    }
  }
  rc = bLazy ? doltliteCloneLazy(cs, pRemote, zUrl)
             : doltliteClone(cs, pRemote);
  if( rc!=SQLITE_OK ){
    const char *zMsg = remoteSqlRemoteMsg(pRemote, rc);
    char *zOwned;
    if( !zMsg && rc==SQLITE_BUSY_SNAPSHOT ){
      zMsg = "clone failed (graph changed during install; retry)";
    }
    zOwned = zMsg ? sqlite3_mprintf("%s", zMsg) : 0;
    pRemote->xClose(pRemote);
    remoteSqlRestoreAndReport(ctx, db, cs, &savedState, rc,
                              zOwned ? zOwned : "clone failed");
    sqlite3_free(zOwned);
    return;
  }
  pRemote->xClose(pRemote);

  if( bLazy ){
    Btree *pBtree = sqlite3DbNameToBtree(db, 0);
    char *zPreparedBranch = 0;
    if( !pBtree || !sqlite3BtreeIsDoltliteFormat(pBtree) ){
      remoteSqlRestoreAndReport(ctx, db, cs, &savedState, SQLITE_ERROR,
                                "failed to initialize lazy clone");
      return;
    }
    pBtree->bDeferredOpen = 1;
    rc = doltliteBtreePrepareBackupBranch(
        pBtree, cs, &zPreparedBranch);
    if( rc==SQLITE_OK && zPreparedBranch ){
      doltliteBtreeInstallBackupBranch(pBtree, zPreparedBranch);
      zPreparedBranch = 0;
    }
    sqlite3_free(zPreparedBranch);
    if( rc!=SQLITE_OK ){
      remoteSqlRestoreAndReport(ctx, db, cs, &savedState, rc,
                                "failed to initialize lazy clone");
      return;
    }
    remoteSqlExpireCurrentStatement(db);
    remoteSqlClearAndSucceed(ctx, &savedState);
    return;
  }

  rc = chunkStoreAddRemote(cs, "origin", zUrl);
  if( rc!=SQLITE_OK ){
    remoteSqlRestoreAndReport(ctx, db, cs, &savedState, SQLITE_ERROR,
                              "failed to add origin remote");
    return;
  }

  {
    int i;
    int nBr;
    const BranchRef *aBr;
    refsTableGetBranches(&cs->refs, &nBr, &aBr);
    for(i=0; i<nBr; i++){
      rc = chunkStoreUpdateTracking(
          cs, "origin", aBr[i].zName, &aBr[i].commitHash);
      if( rc!=SQLITE_OK ){
        remoteSqlRestoreAndReport(ctx, db, cs, &savedState, rc,
                                  "failed to add origin tracking refs");
        return;
      }
    }
  }

  {
    const char *zDefault = chunkStoreGetDefaultBranch(cs);
    ProllyHash branchCommit;
    int nBr;
    const BranchRef *aBr;
    refsTableGetBranches(&cs->refs, &nBr, &aBr);

    if( !zDefault && nBr > 0 ){
      zDefault = aBr[0].zName;
    }

    if( zDefault ){
      rc = chunkStoreFindBranch(cs, zDefault, &branchCommit);
      if( rc!=SQLITE_OK || prollyHashIsEmpty(&branchCommit) ){
        remoteSqlRestoreAndReport(ctx, db, cs, &savedState, SQLITE_ERROR,
                                  "default branch missing from cloned refs");
        return;
      }
      rc = remoteSqlResetSessionToCommit(db, zDefault, &branchCommit);
      if( rc!=SQLITE_OK ){
        remoteSqlRestoreAndReport(
            ctx, db, cs, &savedState, SQLITE_ERROR,
            "failed to initialize working tree from default branch");
        return;
      }
      rc = chunkStoreSetDefaultBranch(cs, zDefault);
      if( rc!=SQLITE_OK ){
        remoteSqlRestoreAndReport(ctx, db, cs, &savedState, SQLITE_ERROR,
                                  "failed to record default branch");
        return;
      }
    }
  }

  rc = doltliteRemotePersistRefs(cs);
  if( rc!=SQLITE_OK ){
    remoteSqlRestoreAndReport(ctx, db, cs, &savedState, rc, 0);
    return;
  }
  remoteSqlExpireCurrentStatement(db);
  remoteSqlClearAndSucceed(ctx, &savedState);
}

typedef struct RemVtab RemVtab;
struct RemVtab { sqlite3_vtab base; sqlite3 *db; };
typedef struct RemCur RemCur;
struct RemCur { sqlite3_vtab_cursor base; int iRow; int bSingle; int iSingle; };

static int remConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  RemVtab *p; int rc;
  (void)pAux; (void)argc; (void)argv; (void)pzErr;
  rc = doltliteVtabConnectSimple(db,
    "CREATE TABLE x("
      "name TEXT, "
      "url TEXT, "
      "fetch_specs TEXT, "
      "params TEXT"
    ")",
    sizeof(*p), ppVtab);
  if( rc!=SQLITE_OK ) return rc;
  p = (RemVtab*)*ppVtab;
  p->db = db;
  return SQLITE_OK;
}
static int remOpen(sqlite3_vtab *v, sqlite3_vtab_cursor **pp){
  (void)v;
  return doltliteVtabOpenCursor(pp, sizeof(RemCur));
}
static int remFilter(sqlite3_vtab_cursor *c, int n, const char *s, int a, sqlite3_value **v){
  RemVtab *pVtab = (RemVtab*)c->pVtab;
  RemCur *pCur = (RemCur*)c;
  (void)s;
  (void)a;
  pCur->iRow = 0;
  pCur->bSingle = 0;
  pCur->iSingle = -1;
  if( n==1 ){
    ChunkStore *cs = doltliteGetChunkStore(pVtab->db);
    const char *zName = (const char*)sqlite3_value_text(v[0]);
    pCur->bSingle = 1;
    pCur->iSingle = cs ? csFindNamedRef(cs->refs.aRemotes, cs->refs.nRemotes,
                                        (int)sizeof(RemoteRef), zName) : -1;
    pCur->iRow = pCur->iSingle;
  }
  return SQLITE_OK;
}
static int remNext(sqlite3_vtab_cursor *c){ ((RemCur*)c)->iRow++; return SQLITE_OK; }
static int remEof(sqlite3_vtab_cursor *c){
  RemVtab *v = (RemVtab*)c->pVtab;
  RemCur *pCur = (RemCur*)c;
  ChunkStore *cs = doltliteGetChunkStore(v->db);
  if( !cs ) return 1;
  if( pCur->bSingle ) return pCur->iSingle<0 || pCur->iRow!=pCur->iSingle;
  return pCur->iRow >= refsTableRemoteCount(&cs->refs);
}
static int remColumn(sqlite3_vtab_cursor *c, sqlite3_context *ctx, int col){
  RemVtab *v = (RemVtab*)c->pVtab;
  ChunkStore *cs = doltliteGetChunkStore(v->db);
  const RemoteRef *rem;
  int nRm;
  const RemoteRef *aRm;
  if(!cs) return SQLITE_OK;
  refsTableGetRemotes(&cs->refs, &nRm, &aRm);
  rem = &aRm[((RemCur*)c)->iRow];
  switch(col){
    case 0:
      sqlite3_result_text(ctx, rem->zName, -1, SQLITE_TRANSIENT);
      break;
    case 1:
      sqlite3_result_text(ctx, rem->zUrl, -1, SQLITE_TRANSIENT);
      break;
    case 2: {

      char *zSpec = sqlite3_mprintf(
          "[\"refs/heads/*:refs/remotes/%s/*\"]", rem->zName);
      if( zSpec ){
        sqlite3_result_text(ctx, zSpec, -1, SQLITE_TRANSIENT);
        sqlite3_free(zSpec);
      }else{
        sqlite3_result_null(ctx);
      }
      break;
    }
    case 3:

      sqlite3_result_text(ctx, "{}", -1, SQLITE_STATIC);
      break;
  }
  return SQLITE_OK;
}
static int remRowid(sqlite3_vtab_cursor *c, sqlite3_int64 *r){
  *r=((RemCur*)c)->iRow; return SQLITE_OK;
}
static int remBestIndex(sqlite3_vtab *v, sqlite3_index_info *p){
  (void)v;
  p->estimatedCost=10;
  p->estimatedRows=5;
  return doltliteBestIndexEq(p, 0);
}

static sqlite3_module remotesModule = {
  0,0,remConnect,remBestIndex,doltliteVtabDisconnect,0,
  remOpen,doltliteVtabClose,remFilter,remNext,remEof,remColumn,remRowid,
  0,0,0,0,0,0,0,0,0,0,0,0
};

#define DOLTLITE_DEFAULT_LOGIN_URL "https://dolthub.com/settings/credentials"

#ifdef DOLTLITE_HAVE_AUTH
static void doltCredsNewFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  DoltliteCreds *cred = 0;
  char *kid = 0, *pub = 0, *msg = 0;
  const char *loginUrl;
  (void)argc;
  (void)argv;

  if( doltliteCredsGenerate(&cred)!=0 ){
    doltliteVcResultError(ctx, db, "failed to generate credential");
    return;
  }
  if( doltliteCredsSave(cred, 0)!=0 ){
    doltliteCredsFree(cred);
    doltliteVcResultError(ctx, db,
        "failed to save credential under ~/.doltlite/creds (is $HOME set?)");
    return;
  }

  kid = doltliteCredsKid(cred);
  pub = doltliteCredsPubKeyB32(cred);
  loginUrl = getenv("DOLTLITE_LOGIN_URL");
  if( !loginUrl || !*loginUrl ) loginUrl = DOLTLITE_DEFAULT_LOGIN_URL;

  if( kid && pub ){
    msg = sqlite3_mprintf(
        "Created credential %s\n"
        "Add this public key to your DoltHub account, then push:\n"
        "  %s#%s\n"
        "(public key: %s)",
        kid, loginUrl, pub, pub);
  }
  if( msg ){
    sqlite3_result_text(ctx, msg, -1, SQLITE_TRANSIENT);
    sqlite3_free(msg);
  }else{
    doltliteVcResultError(ctx, db, "out of memory");
  }

  sqlite3_free(kid);
  sqlite3_free(pub);
  doltliteCredsFree(cred);
}

static void doltCredsFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  const char *action = "list";

  if( argc>=1 && sqlite3_value_text(argv[0]) ){
    action = (const char*)sqlite3_value_text(argv[0]);
  }

  if( strcmp(action, "list")==0 ){
    char **kids = 0;
    int n = 0, i;
    char *msg;
    if( doltliteCredsList(0, &kids, &n)!=0 ){
      doltliteVcResultError(ctx, db, "failed to read ~/.doltlite/creds");
      return;
    }
    if( n==0 ){
      sqlite3_result_text(ctx, "no credentials; run SELECT dolt_creds_new()", -1,
                          SQLITE_STATIC);
      doltliteCredsFreeList(kids, n);
      return;
    }
    msg = sqlite3_mprintf("%d credential(s):", n);
    for(i=0; i<n && msg; i++){
      char *nx = sqlite3_mprintf("%s\n  %s", msg, kids[i]);
      sqlite3_free(msg);
      msg = nx;
    }
    if( msg ){
      sqlite3_result_text(ctx, msg, -1, SQLITE_TRANSIENT);
      sqlite3_free(msg);
    }else{
      doltliteVcResultError(ctx, db, "out of memory");
    }
    doltliteCredsFreeList(kids, n);
  }else if( strcmp(action, "rm")==0 || strcmp(action, "remove")==0 ){
    const char *kid;
    if( argc<2 || !sqlite3_value_text(argv[1]) ){
      doltliteVcResultError(ctx, db, "usage: dolt_creds('rm', <kid>)");
      return;
    }
    kid = (const char*)sqlite3_value_text(argv[1]);
    if( doltliteCredsRemove(0, kid)!=0 ){
      doltliteVcResultError(ctx, db, "no such credential");
    }else{
      char *msg = sqlite3_mprintf("Removed credential %s", kid);
      if( msg ){
        sqlite3_result_text(ctx, msg, -1, SQLITE_TRANSIENT);
        sqlite3_free(msg);
      }else{
        sqlite3_result_int(ctx, 0);
      }
    }
  }else if( strcmp(action, "export")==0 ){
    const char *kid;
    DoltliteCreds *cred = 0;
    if( argc<2 || argc>3 || !sqlite3_value_text(argv[1]) ||
        (argc==3 && !sqlite3_value_text(argv[2])) ){
      doltliteVcResultError(ctx, db,
          "usage: dolt_creds('export', <kid> [, <authorized-keys-dir>])");
      return;
    }
    kid = (const char*)sqlite3_value_text(argv[1]);
    if( doltliteCredsLoad(0, kid, &cred)!=0 ){
      doltliteVcResultError(ctx, db, "no such credential");
      return;
    }
    if( argc==2 ){
      char *json = doltliteCredsToPublicJwk(cred);
      if( json ){
        sqlite3_result_text(ctx, json, -1, SQLITE_TRANSIENT);
        sqlite3_free(json);
      }else{
        doltliteVcResultError(ctx, db, "out of memory");
      }
    }else{
      const char *dir = (const char*)sqlite3_value_text(argv[2]);
      if( doltliteCredsSavePublic(cred, dir)!=0 ){
        doltliteVcResultError(ctx, db, "failed to export public credential");
      }else{
        char *msg = sqlite3_mprintf(
            "Exported public credential %s to %s", kid, dir);
        if( msg ){
          sqlite3_result_text(ctx, msg, -1, SQLITE_TRANSIENT);
          sqlite3_free(msg);
        }else{
          doltliteVcResultError(ctx, db, "out of memory");
        }
      }
    }
    doltliteCredsFree(cred);
  }else{
    doltliteVcResultError(ctx, db,
        "usage: dolt_creds(['list'] | 'rm', <kid> | 'export', <kid> "
        "[, <authorized-keys-dir>]); use SELECT dolt_creds_new() to create");
  }
}
#endif

int doltliteRemoteSqlRegister(sqlite3 *db){
  int rc;
  rc = sqlite3_create_function(db, "dolt_remote", -1,
                               DOLTLITE_COMMAND_FUNC_FLAGS, 0,
                               doltRemoteFunc, 0, 0);
  if( rc==SQLITE_OK ) rc = sqlite3_create_function(db, "dolt_push", -1,
                                                   DOLTLITE_COMMAND_FUNC_FLAGS, 0,
                                                   doltPushFunc, 0, 0);
  if( rc==SQLITE_OK ) rc = sqlite3_create_function(db, "dolt_fetch", -1,
                                                   DOLTLITE_COMMAND_FUNC_FLAGS, 0,
                                                   doltFetchFunc, 0, 0);
  if( rc==SQLITE_OK ) rc = sqlite3_create_function(db, "dolt_pull", -1,
                                                   DOLTLITE_COMMAND_FUNC_FLAGS, 0,
                                                   doltPullFunc, 0, 0);
  if( rc==SQLITE_OK ) rc = sqlite3_create_function(db, "dolt_clone", -1,
                                                   DOLTLITE_COMMAND_FUNC_FLAGS, 0,
                                                   doltCloneFunc, 0, 0);
#ifdef DOLTLITE_HAVE_AUTH
  if( rc==SQLITE_OK ) rc = sqlite3_create_function(db, "dolt_creds_new", -1,
                                                   DOLTLITE_COMMAND_FUNC_FLAGS, 0,
                                                   doltCredsNewFunc, 0, 0);
  if( rc==SQLITE_OK ) rc = sqlite3_create_function(db, "dolt_creds", -1,
                                                   DOLTLITE_COMMAND_FUNC_FLAGS, 0,
                                                   doltCredsFunc, 0, 0);
#endif
  if( rc==SQLITE_OK ) rc = sqlite3_create_module(db, "dolt_remotes", &remotesModule, 0);
  return rc;
}

#else

static void doltliteRemotesDisabled(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  (void)argc;
  (void)argv;
  sqlite3_result_error(ctx,
      "DoltLite remotes are disabled in this build", -1);
}

int doltliteRemoteSqlRegister(sqlite3 *db){
  static const char *azName[] = {
    "dolt_remote", "dolt_push", "dolt_fetch", "dolt_pull", "dolt_clone"
  };
  int i;
  int rc = SQLITE_OK;
  for(i=0; i<ArraySize(azName) && rc==SQLITE_OK; i++){
    rc = sqlite3_create_function(db, azName[i], -1,
        DOLTLITE_COMMAND_FUNC_FLAGS, 0, doltliteRemotesDisabled, 0, 0);
  }
  return rc;
}

#endif

#endif
