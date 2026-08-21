
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
    (void)doltliteVcSealActiveSavepoints(db);
  }
}

static void branchError(sqlite3_context *ctx, int bHadSavepoint, const char *zErr){
  (void)bHadSavepoint;
  doltliteVcResultError(ctx, sqlite3_context_db_handle(ctx), zErr);
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
  return chunkStoreAddBranch(cs, p->zName, &p->head);
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
  return chunkStoreAddBranch(cs, p->zDest, &srcCommit);
}

typedef struct BranchMoveCtx BranchMoveCtx;
struct BranchMoveCtx {
  const char *zSrc;
  const char *zDest;
  int force;
};

static int mutateBranchMove(sqlite3 *db, ChunkStore *cs, void *pArg){
  BranchMoveCtx *p = (BranchMoveCtx*)pArg;
  ProllyHash srcCommit, srcWorkingSet;
  const char *zDefault;
  int srcIsDefault;
  int rc;
  (void)db;
  if( strcmp(p->zSrc, p->zDest)==0 ) return SQLITE_ERROR;
  if( !doltliteUserRefNameIsValid(p->zDest) ) return SQLITE_CONSTRAINT;
  rc = chunkStoreFindBranch(cs, p->zSrc, &srcCommit);
  if( rc!=SQLITE_OK ) return rc;
  zDefault = chunkStoreGetDefaultBranch(cs);
  srcIsDefault = zDefault && strcmp(p->zSrc, zDefault)==0;
  rc = chunkStoreGetBranchWorkingSet(cs, p->zSrc, &srcWorkingSet);
  if( rc!=SQLITE_OK ) memset(&srcWorkingSet, 0, sizeof(srcWorkingSet));
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
      rc = doltliteFindAncestor(db, &currentHead, &branchHead, &ancestor);
      if( rc!=SQLITE_OK || prollyHashCompare(&ancestor, &branchHead)!=0 ){
        p->notMerged = 1;
        return rc==SQLITE_OK ? SQLITE_CONSTRAINT : rc;
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
      }
      memset(&m, 0, sizeof(m));
      m.nName = nPositional;
      m.azName = aPositional;
      m.force = force;
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
      memset(&m, 0, sizeof(m));
      m.zSrc = aPositional[0];
      m.zDest = aPositional[1];
      m.force = force;
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
      rc = doltliteMutateRefs(db, mutateBranchMove, &m);
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
      BranchMutationCtx m;
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
      if( zStart ){
        rc = doltliteResolveRef(db, zStart, &m.head);
        if( rc!=SQLITE_OK ){
          branchError(ctx, hadSavepoint, "start point not found");
          return;
        }
      }else{
        doltliteGetSessionHead(db, &m.head);
        if( prollyHashIsEmpty(&m.head) ){
          branchError(ctx, hadSavepoint, "no commits yet — commit first");
          return;
        }
      }
      m.zName = zName;
      m.force = force;
      if( force
       && strcmp(zName, doltliteGetSessionBranch(db))==0
       && chunkStoreFindBranch(cs, zName, 0)==SQLITE_OK ){
        branchError(ctx, hadSavepoint,
          "cannot force-update the current branch");
        return;
      }
      rc = doltliteMutateRefs(db, mutateBranchRef, &m);
      if( rc!=SQLITE_OK ){
        branchNamedResultError(ctx, hadSavepoint, rc,
          "branch not found", "branch already exists");
        return;
      }
      break;
    }
  }

  if( hadExplicitTxn ){
    rc = doltliteVcSealBranchStyleTxn(db);
    if( rc!=SQLITE_OK ){
      branchErrorCode(ctx, hadSavepoint, rc);
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

  if( doltliteCmdRejectDetached(ctx) ) return;
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
  rc = sqlite3_create_function(db, "dolt_branch", -1,
                               DOLTLITE_COMMAND_FUNC_FLAGS, 0,
                               doltBranchFunc, 0, 0);
  if(rc==SQLITE_OK) rc = sqlite3_create_function(db, "dolt_checkout", -1,
                                                  DOLTLITE_COMMAND_FUNC_FLAGS,
                                                  0, doltCheckoutFunc, 0, 0);
  if(rc==SQLITE_OK) rc = sqlite3_create_function(db, "active_branch", 0, SQLITE_UTF8, 0, activeBranchFunc, 0, 0);
  if(rc==SQLITE_OK) rc = sqlite3_create_function(db, "dolt_connect_branch", 1,
                                                  DOLTLITE_COMMAND_FUNC_FLAGS,
                                                  0, doltConnectBranchFunc, 0, 0);
  if(rc==SQLITE_OK) rc = sqlite3_create_module(db, "dolt_branches", &doltliteBranchesModule, 0);
  if(rc==SQLITE_OK) rc = sqlite3_create_module(db, "dolt_remote_branches", &doltliteRemoteBranchesModule, 0);
  return rc;
}

#endif
