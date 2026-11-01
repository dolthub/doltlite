
#ifdef DOLTLITE_PROLLY

#include "doltlite_branch_int.h"
#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_ancestor.h"
#include "doltlite_commit.h"
#include "doltlite_internal.h"
#include "doltlite_record.h"
#include "prolly_cursor.h"
#include <string.h>
#include <time.h>

static void activeBranchFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  (void)argc; (void)argv;
  if( doltliteIsDetached(db) ){
    sqlite3_result_null(ctx);
  }else{
    sqlite3_result_text(ctx, doltliteGetSessionBranch(db), -1, SQLITE_TRANSIENT);
  }
}

int branchNameEmpty(const char *zName){
  return zName==0 || zName[0]==0;
}

static void branchSealSavepointsOnError(sqlite3_context *ctx, int bHadSavepoint){
  if( bHadSavepoint ){
    sqlite3 *db = sqlite3_context_db_handle(ctx);
    (void)doltliteVcSealSavepointError(db);
  }
}

static void branchError(sqlite3_context *ctx, int bHadSavepoint, const char *zErr){
  (void)bHadSavepoint;
  doltliteVcResultError(ctx, sqlite3_context_db_handle(ctx), zErr);
}

static int branchHeldBySibling(
  sqlite3_context *ctx,
  int bHadSavepoint,
  const char *zName
){
  char *zMsg;
  if( !doltliteStoreSiblingBranch(sqlite3_context_db_handle(ctx), zName, 1) ){
    return 0;
  }
  zMsg = sqlite3_mprintf("branch '%s' is attached in this connection", zName);
  branchError(ctx, bHadSavepoint, zMsg ? zMsg : "branch is attached");
  sqlite3_free(zMsg);
  return 1;
}

static void branchErrorCode(sqlite3_context *ctx, int bHadSavepoint, int rc){
  branchSealSavepointsOnError(ctx, bHadSavepoint);
  sqlite3_result_error_code(ctx, rc);
}

static void branchNamedResultError(
  sqlite3_context *ctx,
  int bHadSavepoint,
  int rc,
  const char *zNotFound,
  const char *zExists
){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  if( rc==SQLITE_CONSTRAINT && doltliteSessionHasUnresolvedConflicts(db) ){
    branchError(ctx, bHadSavepoint,
      "cannot update refs: unresolved merge conflicts");
    return;
  }
  branchSealSavepointsOnError(ctx, bHadSavepoint);
  doltliteRefResultError(ctx, rc, zNotFound, zExists);
}

static int resetBranchRef(sqlite3 *db, ChunkStore *cs, const char *zName,
                          const ProllyHash *pCommit){
  ProllyHash catalog;
  int rc = doltliteCommitCatalogHash(db, pCommit, &catalog);
  if( rc==SQLITE_OK ) rc = chunkStoreUpdateBranch(cs, zName, pCommit);
  if( rc==SQLITE_OK ){
    rc = doltliteWriteBranchCleanWorkingState(db, zName, &catalog, pCommit);
  }
  return rc;
}

/* Dolt refuses to create a branch whose name differs from an existing one only
** by case, while every lookup stays exact. An exact match is left to the
** caller's own duplicate handling, and a rename may re-case its own name. */
static int branchNameTakenIgnoringCase(
  ChunkStore *cs,
  const char *zName,
  const char *zSelf
){
  const BranchRef *aBranch = 0;
  int nBranch = 0;
  int i;
  refsTableGetBranches(&cs->refs, &nBranch, &aBranch);
  for(i=0; i<nBranch; i++){
    const char *zHave = aBranch[i].zName;
    if( !zHave ) continue;
    if( strcmp(zHave, zName)==0 ) continue;
    if( zSelf && strcmp(zHave, zSelf)==0 ) continue;
    if( sqlite3_stricmp(zHave, zName)==0 ) return 1;
  }
  return 0;
}

int mutateBranchRef(sqlite3 *db, ChunkStore *cs, void *pArg){
  BranchMutationCtx *p = (BranchMutationCtx*)pArg;

  if( p->isDelete ){
    const char *zDefault = chunkStoreGetDefaultBranch(cs);
    (void)db;
    if( zDefault && strcmp(p->zName, zDefault)==0 ) return SQLITE_CONSTRAINT;
    return chunkStoreDeleteBranch(cs, p->zName);
  }

  if( !doltliteUserRefNameIsValid(p->zName) ) return SQLITE_CONSTRAINT;
  if( p->force && chunkStoreFindBranch(cs, p->zName, 0)==SQLITE_OK ){
    return resetBranchRef(db, cs, p->zName, &p->head);
  }
  if( branchNameTakenIgnoringCase(cs, p->zName, 0) ) return SQLITE_ERROR;
  return chunkStoreAddBranch(cs, p->zName, &p->head);
}

typedef struct BranchCreateCtx BranchCreateCtx;
struct BranchCreateCtx {
  BranchMutationCtx ref;
  const char *zStart;
  int resolveRc;
};

static int mutateBranchCreate(sqlite3 *db, ChunkStore *cs, void *pArg){
  BranchCreateCtx *p = (BranchCreateCtx*)pArg;
  p->resolveRc = doltliteResolveRefForMutation(db, p->zStart, &p->ref.head);
  if( p->resolveRc!=SQLITE_OK ) return p->resolveRc;
  return mutateBranchRef(db, cs, &p->ref);
}

typedef struct BranchCopyCtx BranchCopyCtx;
struct BranchCopyCtx {
  const char *zSrc;
  const char *zDest;
  int force;
};

static int mutateBranchCopy(sqlite3 *db, ChunkStore *cs, void *pArg){
  BranchCopyCtx *p = (BranchCopyCtx*)pArg;
  ProllyHash srcCommit;
  int rc;
  (void)db;

  rc = chunkStoreFindBranch(cs, p->zSrc, &srcCommit);
  if( rc!=SQLITE_OK ) return rc;
  if( !doltliteUserRefNameIsValid(p->zDest) ) return SQLITE_CONSTRAINT;
  if( p->force ){
    if( chunkStoreFindBranch(cs, p->zDest, 0)==SQLITE_OK ){
      return resetBranchRef(db, cs, p->zDest, &srcCommit);
    }
  }
  if( branchNameTakenIgnoringCase(cs, p->zDest, 0) ) return SQLITE_ERROR;
  return chunkStoreAddBranch(cs, p->zDest, &srcCommit);
}

static const char zBranchInUse[] = "unsafe to delete or rename branches in use "
    "in other sessions; use --force to force the change";

typedef struct BranchMoveCtx BranchMoveCtx;
struct BranchMoveCtx {
  const char *zSrc;
  const char *zDest;
  int force;
  int inUse;
};

static int mutateBranchMove(sqlite3 *db, ChunkStore *cs, void *pArg){
  BranchMoveCtx *p = (BranchMoveCtx*)pArg;
  ProllyHash srcCommit, srcWorkingSet;
  const char *zDefault;
  int srcIsDefault;
  int rc;
  (void)db;
  /* Same name is a no-op when the branch exists. Deleting it here would
  ** remove the branch. A missing name is not a destination collision. */
  if( strcmp(p->zSrc, p->zDest)==0 ){
    rc = chunkStoreFindBranch(cs, p->zSrc, 0);
    return rc==SQLITE_OK ? SQLITE_OK : rc;
  }
  if( !doltliteUserRefNameIsValid(p->zDest) ) return SQLITE_CONSTRAINT;
  rc = chunkStoreFindBranch(cs, p->zSrc, &srcCommit);
  if( rc!=SQLITE_OK ) return rc;
  if( !p->force ){
    rc = doltliteBranchInUseElsewhere(db, p->zSrc, &p->inUse);
    if( rc==SQLITE_NOTFOUND ) rc = SQLITE_OK;
    if( rc!=SQLITE_OK ) return rc;
    if( p->inUse ) return SQLITE_CONSTRAINT;
  }
  if( branchNameTakenIgnoringCase(cs, p->zDest, p->zSrc) ) return SQLITE_ERROR;
  zDefault = chunkStoreGetDefaultBranch(cs);
  srcIsDefault = zDefault && strcmp(p->zSrc, zDefault)==0;
  rc = chunkStoreGetBranchWorkingSet(cs, p->zSrc, &srcWorkingSet);
  if( rc==SQLITE_NOTFOUND ){
    memset(&srcWorkingSet, 0, sizeof(srcWorkingSet));
  }else if( rc!=SQLITE_OK ){
    return rc;
  }
  rc = chunkStoreFindBranch(cs, p->zDest, 0);
  if( rc==SQLITE_OK ){
    if( !p->force ) return SQLITE_ERROR;
    rc = chunkStoreUpdateBranch(cs, p->zDest, &srcCommit);
  }else if( rc==SQLITE_NOTFOUND ){
    rc = chunkStoreAddBranch(cs, p->zDest, &srcCommit);
  }
  if( rc!=SQLITE_OK ) return rc;
  rc = chunkStoreSetBranchWorkingSet(cs, p->zDest, &srcWorkingSet);
  if( rc!=SQLITE_OK ){
    return rc;
  }
  if( srcIsDefault ){
    rc = chunkStoreSetDefaultBranch(cs, p->zDest);
    if( rc!=SQLITE_OK ){
      return rc;
    }
  }
  rc = chunkStoreDeleteBranch(cs, p->zSrc);
  return rc;
}

enum DoltliteBranchMode {
  MODE_CREATE, MODE_DELETE, MODE_COPY, MODE_MOVE
};

typedef struct BranchDeleteCtx BranchDeleteCtx;
struct BranchDeleteCtx {
  int nName;
  const char **azName;
  int force;
  int notMerged;
  int isDefault;
  int isDirty;
  int inUse;
};

static int mutateBranchDelete(sqlite3 *db, ChunkStore *cs, void *pArg){
  BranchDeleteCtx *p = (BranchDeleteCtx*)pArg;
  ProllyHash currentHead;
  const char *zDefault = chunkStoreGetDefaultBranch(cs);
  int i;
  int rc;

  if( !p->force ) doltliteGetSessionHead(db, &currentHead);
  for(i=0; i<p->nName; i++){
    ProllyHash branchHead, ancestor;
    if( zDefault && strcmp(p->azName[i], zDefault)==0 ){
      p->isDefault = 1;
      return SQLITE_CONSTRAINT;
    }
    rc = chunkStoreFindBranch(cs, p->azName[i], &branchHead);
    if( rc!=SQLITE_OK ) return rc;
    if( !p->force ){
      /* Another connection may be working on it; deleting the branch
      ** discards its uncommitted rows. Where the VFS cannot tell whether
      ** a connection has it checked out, uncommitted changes stand in. */
      int inUse = 0;
      rc = doltliteBranchInUseElsewhere(db, p->azName[i], &inUse);
      if( rc==SQLITE_NOTFOUND ){
        int iRef = csFindNamedRef(cs->refs.aBranches, cs->refs.nBranches,
                                  (int)sizeof(BranchRef), p->azName[i]);
        if( iRef>=0 ){
          rc = doltliteBranchRefIsDirty(db, cs, &cs->refs.aBranches[iRef],
                                        &p->isDirty);
          if( rc!=SQLITE_OK ) return rc;
        }
        if( p->isDirty ) return SQLITE_CONSTRAINT;
      }else if( rc!=SQLITE_OK ){
        return rc;
      }else if( inUse ){
        p->inUse = 1;
        return SQLITE_CONSTRAINT;
      }
      rc = doltliteFindAncestor(db, &currentHead, &branchHead, &ancestor);
      if( rc!=SQLITE_OK && rc!=SQLITE_NOTFOUND ) return rc;
      if( rc==SQLITE_NOTFOUND
       || prollyHashCompare(&ancestor, &branchHead)!=0 ){
        p->notMerged = 1;
        return SQLITE_CONSTRAINT;
      }
    }
  }
  for(i=0; i<p->nName; i++){
    rc = chunkStoreDeleteBranch(cs, p->azName[i]);
    if( rc!=SQLITE_OK ) return rc;
  }
  return SQLITE_OK;
}

static void doltBranchParsedFunc(
  sqlite3_context *ctx,
  enum DoltliteBranchMode mode,
  int force,
  int nPositional,
  const char **aPositional
){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  int hadExplicitTxn = !db->autoCommit;
  int hadSavepoint = db->pSavepoint!=0;
  int bHookApproved = 0;
  int rc;

  if( !cs ){ branchError(ctx, hadSavepoint, doltliteVcUnavailableMessage(db)); return; }

  switch( mode ){
    case MODE_DELETE: {
      BranchDeleteCtx m;
      int i;
      if( nPositional<1 ){
        branchError(ctx, hadSavepoint, "branch name required"); return;
      }
      for(i=0; i<nPositional; i++){
        if( branchNameEmpty(aPositional[i]) ){
          branchError(ctx, hadSavepoint, "branch name required"); return;
        }
        if( strcmp(aPositional[i], doltliteGetSessionBranch(db))==0 ){
          branchError(ctx, hadSavepoint, "cannot delete the current branch");
          return;
        }
        if( branchHeldBySibling(ctx, hadSavepoint, aPositional[i]) ) return;
      }
      memset(&m, 0, sizeof(m));
      m.nName = nPositional;
      m.azName = aPositional;
      m.force = force;
      if( hadExplicitTxn && doltliteVcSealPrecheck(ctx, &bHookApproved) ){
        branchSealSavepointsOnError(ctx, hadSavepoint);
        return;
      }
      rc = doltliteMutateRefs(db, mutateBranchDelete, &m);
      if( rc==SQLITE_CONSTRAINT ){
        if( doltliteSessionHasUnresolvedConflicts(db) ){
          branchError(ctx, hadSavepoint,
            "cannot update refs: unresolved merge conflicts");
        }else if( m.isDefault ){
          branchError(ctx, hadSavepoint,
            "cannot delete the default branch; "
            "call dolt_default_branch(<other>) first");
        }else if( m.notMerged ){
          branchError(ctx, hadSavepoint, "branch is not fully merged");
        }else if( m.inUse ){
          branchError(ctx, hadSavepoint, zBranchInUse);
        }else if( m.isDirty ){
          branchError(ctx, hadSavepoint,
            "branch has uncommitted changes; use --force to delete it");
        }else{
          branchErrorCode(ctx, hadSavepoint, rc);
        }
        return;
      }
      if( m.notMerged ){
        branchError(ctx, hadSavepoint, "branch is not fully merged");
        return;
      }
      if( rc!=SQLITE_OK ){
        branchNamedResultError(ctx, hadSavepoint, rc, "branch not found", 0);
        return;
      }
      break;
    }

    case MODE_COPY: {
      BranchCopyCtx m;
      if( nPositional>2 ){
        branchError(ctx, hadSavepoint, "too many arguments");
        return;
      }
      if( nPositional<2 ){
        branchError(ctx, hadSavepoint, "copy requires source and destination");
        return;
      }
      if( branchNameEmpty(aPositional[0]) || branchNameEmpty(aPositional[1]) ){
        branchError(ctx, hadSavepoint, "branch name required");
        return;
      }
      if( !doltliteUserRefNameIsValid(aPositional[1]) ){
        branchError(ctx, hadSavepoint, "invalid branch name");
        return;
      }
      if( force
       && strcmp(aPositional[1], doltliteGetSessionBranch(db))==0 ){
        branchError(ctx, hadSavepoint,
          "cannot force-update the current branch");
        return;
      }
      if( force && branchHeldBySibling(ctx, hadSavepoint, aPositional[1]) ){
        return;
      }
      memset(&m, 0, sizeof(m));
      m.zSrc = aPositional[0];
      m.zDest = aPositional[1];
      m.force = force;
      if( hadExplicitTxn && doltliteVcSealPrecheck(ctx, &bHookApproved) ){
        branchSealSavepointsOnError(ctx, hadSavepoint);
        return;
      }
      rc = doltliteMutateRefs(db, mutateBranchCopy, &m);
      if( rc!=SQLITE_OK ){
        branchNamedResultError(ctx, hadSavepoint, rc,
          "source branch not found", "branch already exists");
        return;
      }
      break;
    }

    case MODE_MOVE: {
      BranchMoveCtx m;
      int renamingCurrent;
      char *zPreparedBranch = 0;
      if( nPositional>2 ){
        branchError(ctx, hadSavepoint, "too many arguments");
        return;
      }
      if( nPositional<2 ){
        branchError(ctx, hadSavepoint, "move requires source and destination");
        return;
      }
      if( branchNameEmpty(aPositional[0]) || branchNameEmpty(aPositional[1]) ){
        branchError(ctx, hadSavepoint, "branch name required");
        return;
      }
      /* Checked before the destination-name rules. A missing self-move is
      ** branch not found, including a name that would otherwise be invalid,
      ** and an existing branch stays where it is. */
      if( strcmp(aPositional[0], aPositional[1])==0 ){
        rc = chunkStoreFindBranch(cs, aPositional[0], 0);
        if( rc==SQLITE_NOTFOUND ){
          branchNamedResultError(ctx, hadSavepoint, rc, "branch not found", 0);
          return;
        }
        if( rc!=SQLITE_OK ){
          branchErrorCode(ctx, hadSavepoint, rc);
          return;
        }
        break;
      }
      if( !doltliteUserRefNameIsValid(aPositional[1]) ){
        branchError(ctx, hadSavepoint, "invalid branch name");
        return;
      }
      if( force
       && strcmp(aPositional[0], doltliteGetSessionBranch(db))!=0
       && strcmp(aPositional[1], doltliteGetSessionBranch(db))==0 ){
        branchError(ctx, hadSavepoint,
          "cannot force-update the current branch");
        return;
      }
      if( branchHeldBySibling(ctx, hadSavepoint, aPositional[0])
       || (force && branchHeldBySibling(ctx, hadSavepoint, aPositional[1])) ){
        return;
      }
      memset(&m, 0, sizeof(m));
      m.zSrc = aPositional[0];
      m.zDest = aPositional[1];
      m.force = force;
      renamingCurrent = strcmp(m.zSrc, doltliteGetSessionBranch(db))==0;
      if( renamingCurrent ){
        rc = doltlitePrepareSessionBranch(db, m.zDest, &zPreparedBranch);
        if( rc!=SQLITE_OK ){
          branchErrorCode(ctx, hadSavepoint, rc);
          return;
        }
      }
      if( hadExplicitTxn && doltliteVcSealPrecheck(ctx, &bHookApproved) ){
        branchSealSavepointsOnError(ctx, hadSavepoint);
        return;
      }
      rc = doltliteMutateRefs(db, mutateBranchMove, &m);
      if( rc!=SQLITE_OK && m.inUse ){
        sqlite3_free(zPreparedBranch);
        branchError(ctx, hadSavepoint, zBranchInUse);
        return;
      }
      if( rc!=SQLITE_OK ){
        sqlite3_free(zPreparedBranch);
        branchNamedResultError(ctx, hadSavepoint, rc,
          "source branch not found", "destination already exists");
        return;
      }
      if( renamingCurrent ){
        assert( zPreparedBranch!=0 );
        doltliteInstallPreparedSessionBranch(db, zPreparedBranch);
      }
      break;
    }

    case MODE_CREATE: {
      BranchCreateCtx m;
      const char *zName, *zStart;
      if( nPositional>2 ){
        branchError(ctx, hadSavepoint, "too many arguments");
        return;
      }
      if( nPositional<1 ){
        branchError(ctx, hadSavepoint, "branch name required"); return;
      }
      zName = aPositional[0];
      if( branchNameEmpty(zName) ){
        branchError(ctx, hadSavepoint, "branch name required"); return;
      }
      if( !doltliteUserRefNameIsValid(zName) ){
        branchError(ctx, hadSavepoint, "invalid branch name"); return;
      }
      zStart = nPositional>=2 ? aPositional[1] : 0;
      memset(&m, 0, sizeof(m));
      m.zStart = zStart;
      m.ref.zName = zName;
      m.ref.force = force;
      if( force
       && strcmp(zName, doltliteGetSessionBranch(db))==0
       && chunkStoreFindBranch(cs, zName, 0)==SQLITE_OK ){
        branchError(ctx, hadSavepoint,
          "cannot force-update the current branch");
        return;
      }
      if( force && branchHeldBySibling(ctx, hadSavepoint, zName) ) return;
      if( hadExplicitTxn && doltliteVcSealPrecheck(ctx, &bHookApproved) ){
        branchSealSavepointsOnError(ctx, hadSavepoint);
        return;
      }
      rc = doltliteMutateRefs(db, mutateBranchCreate, &m);
      if( rc!=SQLITE_OK ){
        if( m.resolveRc!=SQLITE_OK ){
          branchNamedResultError(ctx, hadSavepoint, m.resolveRc,
            "start point not found", "start point not found");
          return;
        }
        branchNamedResultError(ctx, hadSavepoint, rc,
          "branch not found", "branch already exists");
        return;
      }
      break;
    }
  }

  if( hadExplicitTxn ){
    int (*xHook)(void*) = db->xCommitCallback;
    if( bHookApproved ) db->xCommitCallback = 0;
    rc = doltliteVcSealBranchStyleTxn(db);
    db->xCommitCallback = xHook;
    if( rc!=SQLITE_OK ){
      doltliteVcResultSealError(ctx, db, rc);
      branchSealSavepointsOnError(ctx, hadSavepoint);
      return;
    }
  }
  sqlite3_result_int(ctx, 0);
}

static void doltBranchFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  DoltliteCmdArgs args;
  enum DoltliteBranchMode mode = MODE_CREATE;
  int isDelete = 0, isForceDelete = 0, isCopy = 0, isMove = 0, force = 0;
  int hadSavepoint = db->pSavepoint!=0;
  DoltliteCmdOption aOption[] = {
    { "delete", 'd', DOLTLITE_CMD_OPTION_FLAG, &isDelete, 0 },
    { 0, 'D', DOLTLITE_CMD_OPTION_FLAG, &isForceDelete, 0 },
    { "copy", 'c', DOLTLITE_CMD_OPTION_FLAG, &isCopy, 0 },
    { "move", 'm', DOLTLITE_CMD_OPTION_FLAG, &isMove, 0 },
    { "force", 'f', DOLTLITE_CMD_OPTION_FLAG, &force, 0 }
  };
  int rc;

  if( doltliteCmdRejectInWriteStatement(ctx, "branch") ) return;
  if( doltliteCmdRejectDetached(ctx) ) return;
  if( doltliteCmdRejectReadOnly(ctx) ) return;
  if( argc<1 ){
    branchError(ctx, hadSavepoint, "dolt_branch requires arguments");
    return;
  }
  rc = doltliteCmdParseArgs(ctx, argc, argv, aOption, ArraySize(aOption),
                            0, &args);
  if( rc!=SQLITE_OK ){
    (void)doltliteVcSealSavepointError(db);
    return;
  }
  if( isDelete + isForceDelete + isCopy + isMove > 1 ){
    doltliteCmdArgsClear(&args);
    branchError(ctx, hadSavepoint, "conflicting flags");
    return;
  }
  if( isDelete || isForceDelete ) mode = MODE_DELETE;
  else if( isCopy ) mode = MODE_COPY;
  else if( isMove ) mode = MODE_MOVE;
  if( isForceDelete ) force = 1;
  doltBranchParsedFunc(ctx, mode, force, args.nPositional,
                       args.azPositional);
  doltliteCmdArgsClear(&args);
}


int doltliteBranchRegister(sqlite3 *db){
  int rc;
  rc = doltliteCreateCommandFunc(db, "dolt_branch", -1,
                                 doltBranchFunc);
  if(rc==SQLITE_OK) rc = doltliteCreateCommandFunc(db, "dolt_checkout", -1,
                                 doltCheckoutFunc);
  if(rc==SQLITE_OK) rc = sqlite3_create_function(db, "active_branch", 0, SQLITE_UTF8, 0, activeBranchFunc, 0, 0);
  if(rc==SQLITE_OK) rc = doltliteCreateCommandFunc(db, "dolt_connect_branch", 1,
                                 doltConnectBranchFunc);
  if(rc==SQLITE_OK) rc = doltliteCreateShieldedModule(db, "dolt_branches", &doltliteBranchesModule, 0);
  if(rc==SQLITE_OK) rc = doltliteCreateShieldedModule(db, "dolt_remote_branches", &doltliteRemoteBranchesModule, 0);
  return rc;
}

#endif
