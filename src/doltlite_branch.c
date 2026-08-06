
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
  sqlite3_result_text(ctx, doltliteGetSessionBranch(db), -1, SQLITE_TRANSIENT);
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

static void doltBranchFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  enum { MODE_CREATE, MODE_DELETE, MODE_COPY, MODE_MOVE } mode = MODE_CREATE;
  int force = 0;
  const char *aPositional[3] = {0, 0, 0};
  int nPositional = 0;
  int hadExplicitTxn = !db->autoCommit;
  int hadSavepoint = db->pSavepoint!=0;
  int i, rc;

  if( !cs ){ branchError(ctx, hadSavepoint, doltliteVcUnavailableMessage(db)); return; }
  if( argc<1 ){ branchError(ctx, hadSavepoint, "dolt_branch requires arguments"); return; }

  for(i=0; i<argc; i++){
    const char *arg = (const char*)sqlite3_value_text(argv[i]);
    if( !arg ) continue;
    if( strcmp(arg, "-d")==0 || strcmp(arg, "--delete")==0 ){
      if( mode!=MODE_CREATE ){
        branchError(ctx, hadSavepoint, "conflicting flags"); return;
      }
      mode = MODE_DELETE;
    }else if( strcmp(arg, "-D")==0 ){

      if( mode!=MODE_CREATE ){
        branchError(ctx, hadSavepoint, "conflicting flags"); return;
      }
      mode = MODE_DELETE;
      force = 1;
    }else if( strcmp(arg, "-c")==0 || strcmp(arg, "--copy")==0 ){
      if( mode!=MODE_CREATE ){
        branchError(ctx, hadSavepoint, "conflicting flags"); return;
      }
      mode = MODE_COPY;
    }else if( strcmp(arg, "-m")==0 || strcmp(arg, "--move")==0 ){
      if( mode!=MODE_CREATE ){
        branchError(ctx, hadSavepoint, "conflicting flags"); return;
      }
      mode = MODE_MOVE;
    }else if( strcmp(arg, "-f")==0 || strcmp(arg, "--force")==0 ){
      force = 1;
    }else if( arg[0]=='-' ){
      char *zErr = sqlite3_mprintf("unknown option `%s`", arg);
      if( zErr ){
        branchError(ctx, hadSavepoint, zErr);
        sqlite3_free(zErr);
      }else{
        sqlite3_result_error_nomem(ctx);
      }
      return;
    }else{
      if( nPositional >= 3 ){
        branchError(ctx, hadSavepoint, "too many arguments"); return;
      }
      aPositional[nPositional++] = arg;
    }
  }

  switch( mode ){
    case MODE_DELETE: {
      BranchMutationCtx m;
      ProllyHash branchHead, currentHead, ancestor;
      if( nPositional>1 ){
        branchError(ctx, hadSavepoint, "too many arguments");
        return;
      }
      if( nPositional<1 ){
        branchError(ctx, hadSavepoint, "branch name required"); return;
      }
      if( branchNameEmpty(aPositional[0]) ){
        branchError(ctx, hadSavepoint, "branch name required"); return;
      }
      if( strcmp(aPositional[0], doltliteGetSessionBranch(db))==0 ){
        branchError(ctx, hadSavepoint, "cannot delete the current branch");
        return;
      }
      if( !force ){
        rc = chunkStoreFindBranch(cs, aPositional[0], &branchHead);
        if( rc!=SQLITE_OK ){
          branchNamedResultError(ctx, hadSavepoint, rc, "branch not found", 0);
          return;
        }
        doltliteGetSessionHead(db, &currentHead);
        rc = doltliteFindAncestor(db, &currentHead, &branchHead, &ancestor);
        if( rc!=SQLITE_OK || prollyHashCompare(&ancestor, &branchHead)!=0 ){
          branchError(ctx, hadSavepoint, "branch is not fully merged");
          return;
        }
      }
      memset(&m, 0, sizeof(m));
      m.zName = aPositional[0];
      m.isDelete = 1;
      rc = doltliteMutateRefs(db, mutateBranchRef, &m);
      if( rc==SQLITE_CONSTRAINT ){
        branchError(ctx, hadSavepoint,
          "cannot delete the default branch; "
          "call dolt_default_branch(<other>) first");
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
  return rc;
}

#endif
