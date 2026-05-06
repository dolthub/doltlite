
#ifdef DOLTLITE_PROLLY

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

typedef struct BranchMutationCtx BranchMutationCtx;
struct BranchMutationCtx {
  const char *zName;
  ProllyHash head;
  int isDelete;
  int force;
};

static int branchNameEmpty(const char *zName){
  return zName==0 || zName[0]==0;
}

static void branchResultError(
  sqlite3_context *ctx,
  int rc,
  const char *zNotFound,
  const char *zExists
){
  if( rc==SQLITE_NOTFOUND ){
    sqlite3_result_error(ctx, zNotFound, -1);
  }else if( rc==SQLITE_ERROR && zExists ){
    sqlite3_result_error(ctx, zExists, -1);
  }else{
    sqlite3_result_error(ctx, sqlite3_errstr(rc), -1);
  }
}

static void branchSealSavepointsOnError(sqlite3_context *ctx, int bHadSavepoint){
  if( bHadSavepoint ){
    sqlite3 *db = sqlite3_context_db_handle(ctx);
    (void)doltliteVcSealActiveSavepoints(db);
  }
}

static void branchError(sqlite3_context *ctx, int bHadSavepoint, const char *zErr){
  branchSealSavepointsOnError(ctx, bHadSavepoint);
  sqlite3_result_error(ctx, zErr, -1);
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
  branchResultError(ctx, rc, zNotFound, zExists);
}

static int mutateBranchRef(sqlite3 *db, ChunkStore *cs, void *pArg){
  BranchMutationCtx *p = (BranchMutationCtx*)pArg;
  int rc;

  if( p->isDelete ){
    (void)db;
    return chunkStoreDeleteBranch(cs, p->zName);
  }

  if( p->force && chunkStoreFindBranch(cs, p->zName, 0)==SQLITE_OK ){
    return chunkStoreUpdateBranch(cs, p->zName, &p->head);
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
  if( p->force ){

    if( chunkStoreFindBranch(cs, p->zDest, 0)==SQLITE_OK ){
      return chunkStoreUpdateBranch(cs, p->zDest, &srcCommit);
    }
  }
  return chunkStoreAddBranch(cs, p->zDest, &srcCommit);
}

typedef struct BranchMoveCtx BranchMoveCtx;
struct BranchMoveCtx {
  const char *zSrc;
  const char *zDest;
};

static int mutateBranchMove(sqlite3 *db, ChunkStore *cs, void *pArg){
  BranchMoveCtx *p = (BranchMoveCtx*)pArg;
  ProllyHash srcCommit, srcWorkingSet;
  int rc;
  (void)db;

  rc = chunkStoreFindBranch(cs, p->zSrc, &srcCommit);
  if( rc!=SQLITE_OK ) return rc;
  rc = chunkStoreGetBranchWorkingSet(cs, p->zSrc, &srcWorkingSet);
  if( rc!=SQLITE_OK ) memset(&srcWorkingSet, 0, sizeof(srcWorkingSet));
  rc = chunkStoreAddBranch(cs, p->zDest, &srcCommit);
  if( rc!=SQLITE_OK ) return rc;
  if( !prollyHashIsEmpty(&srcWorkingSet) ){
    rc = chunkStoreSetBranchWorkingSet(cs, p->zDest, &srcWorkingSet);
    if( rc!=SQLITE_OK ){
      chunkStoreDeleteBranch(cs, p->zDest);
      return rc;
    }
  }
  rc = chunkStoreDeleteBranch(cs, p->zSrc);
  if( rc!=SQLITE_OK ){
    chunkStoreDeleteBranch(cs, p->zDest);
  }
  return rc;
}

typedef struct CheckoutSchemaInfo CheckoutSchemaInfo;
struct CheckoutSchemaInfo {
  int hasCurrent;
  int hasSource;
  int rebuilt;
  char *zCurrentSql;
  char *zSourceSql;
};

static void checkoutSchemaInfoClear(CheckoutSchemaInfo *aInfo, int nInfo){
  int i;
  if( !aInfo ) return;
  for(i=0; i<nInfo; i++){
    sqlite3_free(aInfo[i].zCurrentSql);
    sqlite3_free(aInfo[i].zSourceSql);
  }
  sqlite3_free(aInfo);
}

static int checkoutSchemaTextField(
  const u8 *pVal, int nVal,
  const DoltliteRecordInfo *pRi,
  int iField,
  char **pzOut
){
  int st, off, n;
  *pzOut = 0;
  if( iField<0 || iField>=pRi->nField ) return SQLITE_CORRUPT;
  st = pRi->aType[iField];
  off = pRi->aOffset[iField];
  if( off<0 || off>nVal ) return SQLITE_CORRUPT;
  if( st==0 ){
    *pzOut = sqlite3_mprintf("");
    return *pzOut ? SQLITE_OK : SQLITE_NOMEM;
  }
  if( st<13 || (st&1)==0 ) return SQLITE_CORRUPT;
  n = (st - 13) / 2;
  if( n<0 || off+n>nVal ) return SQLITE_CORRUPT;
  *pzOut = sqlite3_malloc(n + 1);
  if( !*pzOut ) return SQLITE_NOMEM;
  memcpy(*pzOut, pVal + off, n);
  (*pzOut)[n] = 0;
  return SQLITE_OK;
}

static int checkoutLoadLiveTableSql(
  sqlite3 *db,
  const char *zName,
  int *pFound,
  char **pzSql
){
  sqlite3_stmt *pStmt = 0;
  char *zQry;
  int rc;

  *pFound = 0;
  *pzSql = 0;
  zQry = sqlite3_mprintf(
      "SELECT sql FROM main.sqlite_master "
      "WHERE type='table' AND name='%q'",
      zName);
  if( !zQry ) return SQLITE_NOMEM;
  rc = sqlite3_prepare_v2(db, zQry, -1, &pStmt, 0);
  sqlite3_free(zQry);
  if( rc!=SQLITE_OK ) return rc;
  if( sqlite3_step(pStmt)==SQLITE_ROW ){
    const char *zSql = (const char*)sqlite3_column_text(pStmt, 0);
    *pFound = 1;
    *pzSql = sqlite3_mprintf("%s", zSql ? zSql : "");
    if( !*pzSql ){
      sqlite3_finalize(pStmt);
      return SQLITE_NOMEM;
    }
  }
  sqlite3_finalize(pStmt);
  return SQLITE_OK;
}

static int checkoutLoadSourceTableSql(
  sqlite3 *db,
  struct TableEntry *aSource,
  int nSource,
  const char *zName,
  int *pFound,
  char **pzSql
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  ProllyHash masterRoot;
  u8 masterFlags = 0;
  ProllyCursor cur;
  int i, rc, res;

  *pFound = 0;
  *pzSql = 0;
  memset(&masterRoot, 0, sizeof(masterRoot));
  for(i=0; i<nSource; i++){
    if( aSource[i].iTable==1 ){
      memcpy(&masterRoot, &aSource[i].root, sizeof(masterRoot));
      masterFlags = aSource[i].flags;
      break;
    }
  }
  if( !cs || !pCache || prollyHashIsEmpty(&masterRoot) ) return SQLITE_OK;

  prollyCursorInit(&cur, cs, pCache, &masterRoot, masterFlags);
  rc = prollyCursorFirst(&cur, &res);
  if( rc!=SQLITE_OK || res ){
    prollyCursorClose(&cur);
    return rc;
  }
  while( prollyCursorIsValid(&cur) ){
    const u8 *pVal = 0;
    int nVal = 0;
    DoltliteRecordInfo ri;
    char *zType = 0;
    char *zEntryName = 0;

    prollyCursorValue(&cur, &pVal, &nVal);
    doltliteParseRecord(pVal, nVal, &ri);
    if( ri.nField >= 5 ){
      rc = checkoutSchemaTextField(pVal, nVal, &ri, 0, &zType);
      if( rc==SQLITE_OK ) rc = checkoutSchemaTextField(pVal, nVal, &ri, 1, &zEntryName);
      if( rc!=SQLITE_OK ){
        sqlite3_free(zType);
        sqlite3_free(zEntryName);
        prollyCursorClose(&cur);
        return rc;
      }
      if( strcmp(zType, "table")==0 && strcmp(zEntryName, zName)==0 ){
        *pFound = 1;
        rc = checkoutSchemaTextField(pVal, nVal, &ri, 4, pzSql);
        sqlite3_free(zType);
        sqlite3_free(zEntryName);
        prollyCursorClose(&cur);
        return rc;
      }
    }
    sqlite3_free(zType);
    sqlite3_free(zEntryName);
    rc = prollyCursorNext(&cur);
    if( rc!=SQLITE_OK ){
      prollyCursorClose(&cur);
      return rc;
    }
  }
  prollyCursorClose(&cur);
  return SQLITE_OK;
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

  if( !cs ){ branchError(ctx, hadSavepoint, "no database"); return; }
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
      /* Short-term guard: doltlite does not yet have stable default-branch
      ** resolution, so on reopen p->zBranch initializes from the last-active
      ** branch (tracked via chunkStoreGetDefaultBranch, which dolt_checkout
      ** updates). If "main" is deleted, opening the DB would leave the
      ** session pointing at a nonexistent branch. Reject until we have real
      ** default-branch logic. */
      if( strcmp(aPositional[0], "main")==0 ){
        branchError(ctx, hadSavepoint,
          "cannot delete branch 'main' (doltlite requires main to exist)");
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
      memset(&m, 0, sizeof(m));
      m.zSrc = aPositional[0];
      m.zDest = aPositional[1];
      /* Same guard as MODE_DELETE: renaming "main" away would leave the
      ** repo with no main branch, and reopening would fail to resolve a
      ** session branch. Reject until default-branch logic is reworked. */
      if( strcmp(m.zSrc, "main")==0 ){
        branchError(ctx, hadSavepoint,
          "cannot rename branch 'main' (doltlite requires main to exist)");
        return;
      }
      renamingCurrent = strcmp(m.zSrc, doltliteGetSessionBranch(db))==0;
      rc = doltliteMutateRefs(db, mutateBranchMove, &m);
      if( rc!=SQLITE_OK ){
        branchNamedResultError(ctx, hadSavepoint, rc,
          "source branch not found", "destination already exists");
        return;
      }
      if( renamingCurrent ){
        doltliteSetSessionBranch(db, m.zDest);
        chunkStoreSetDefaultBranch(cs, m.zDest);
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

static int checkoutLoadAndApply(
  sqlite3 *db,
  ChunkStore *cs,
  const char *zBranch,
  ProllyHash *pCommitHash,
  ProllyHash *pCatHash
){
  int rc;
  ProllyHash committedCatHash;


  {
    DoltliteCommit commit;

    rc = doltliteLoadCommit(db, pCommitHash, &commit);
    if( rc!=SQLITE_OK ) return rc;

    memcpy(&committedCatHash, &commit.catalogHash, sizeof(ProllyHash));
    doltliteCommitClear(&commit);
  }


  {
    ProllyHash wsCatHash, wsCommitHash;
    memset(&wsCatHash, 0, sizeof(wsCatHash));
    memset(&wsCommitHash, 0, sizeof(wsCommitHash));
    if( chunkStoreReadBranchWorkingCatalog(cs, zBranch, &wsCatHash, &wsCommitHash)==SQLITE_OK
     && !prollyHashIsEmpty(&wsCommitHash)
     && memcmp(wsCommitHash.data, pCommitHash->data, PROLLY_HASH_SIZE)==0
     && memcmp(wsCatHash.data, committedCatHash.data, PROLLY_HASH_SIZE)!=0 ){

      memcpy(pCatHash, &wsCatHash, sizeof(ProllyHash));
      rc = doltliteSwitchCatalog(db, pCatHash);
      return rc;
    }
  }

  memcpy(pCatHash, &committedCatHash, sizeof(ProllyHash));
  rc = doltliteSwitchCatalog(db, pCatHash);
  return rc;
}

static int refreshBranchScopedTables(sqlite3 *db){
  int rc;
  extern int doltliteRegisterDiffTables(sqlite3 *db);
  extern int doltliteRegisterHistoryTables(sqlite3 *db);
  extern int doltliteRegisterAtTables(sqlite3 *db);
  extern int doltliteRegisterBlameTables(sqlite3 *db);

  rc = doltliteRegisterDiffTables(db);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteRegisterHistoryTables(db);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteRegisterAtTables(db);
  if( rc!=SQLITE_OK ) return rc;
  return doltliteRegisterBlameTables(db);
}

/* Checkout is a multi-step mutation: persist outgoing branch state,
** update refs, load target branch, reload session. If any step fails
** we must unwind every prior step — the saved* fields are the
** snapshot of session state taken before the mutation begins so
** checkoutRestoreSession can roll back cleanly. */
typedef struct CheckoutMutationCtx CheckoutMutationCtx;
struct CheckoutMutationCtx {
  const char *zTargetBranch;
  const char *zCurrentBranch;
  ProllyHash savedSessionHead;
  ProllyHash savedSessionStaged;
  ProllyHash savedMergeCommit;
  ProllyHash savedConflictsCatalog;
  ProllyHash savedPreRebaseCat;
  ProllyHash savedRebaseOnto;
  ProllyHash oldCatHash;
  ProllyHash oldCommitHash;
  ProllyHash targetCommit;
  ProllyHash targetCatHash;
  u8 savedIsMerging;
  u8 savedIsRebasing;
  const char *zSavedRebaseOrigBranch;
  const char *zSavedRebaseReturnBranch;
  int haveOldState;
  int bPersistUnderSavepoint;
};

static void checkoutRestoreSession(sqlite3 *db, CheckoutMutationCtx *p){
  doltliteSetSessionBranch(db, p->zCurrentBranch);
  doltliteSetSessionHead(db, &p->savedSessionHead);
  doltliteSetSessionStaged(db, &p->savedSessionStaged);
  doltliteSetSessionMergeState(db, p->savedIsMerging,
                               &p->savedMergeCommit,
                               &p->savedConflictsCatalog);
  doltliteSetSessionRebaseState(db, p->savedIsRebasing,
                                &p->savedPreRebaseCat,
                                &p->savedRebaseOnto,
                                p->zSavedRebaseOrigBranch,
                                p->zSavedRebaseReturnBranch);
  if( p->haveOldState ){
    doltliteSwitchCatalog(db, &p->oldCatHash);
  }
}

static int checkoutRestoreDurableState(
  sqlite3 *db,
  ChunkStore *cs,
  void *pArg
){
  CheckoutMutationCtx *p = (CheckoutMutationCtx*)pArg;
  int rc = SQLITE_OK;
  UNUSED_PARAMETER(cs);
  if( p->haveOldState ){
    rc = doltliteUpdateBranchWorkingState(db, p->zCurrentBranch,
                                          &p->oldCatHash, &p->oldCommitHash);
    if( rc!=SQLITE_OK ) return rc;
  }
  return SQLITE_OK;
}

static int checkoutMutateRefs(sqlite3 *db, ChunkStore *cs, void *pArg){
  CheckoutMutationCtx *p = (CheckoutMutationCtx*)pArg;
  int bSavepoint = db->pSavepoint!=0;
  int rc;

  rc = chunkStoreFindBranch(cs, p->zTargetBranch, &p->targetCommit);
  if( rc!=SQLITE_OK ) return rc;
  if( prollyHashIsEmpty(&p->targetCommit) ) return SQLITE_EMPTY;

  rc = checkoutLoadAndApply(db, cs, p->zTargetBranch,
                            &p->targetCommit, &p->targetCatHash);
  if( rc!=SQLITE_OK ) return rc;

  doltliteSetSessionBranch(db, p->zTargetBranch);
  doltliteSetSessionHead(db, &p->targetCommit);

  rc = doltliteLoadWorkingSet(db, p->zTargetBranch);
  if( rc!=SQLITE_OK ) return rc;

  {
    ProllyHash staged;
    doltliteGetSessionStaged(db, &staged);
    if( prollyHashIsEmpty(&staged) ){
      doltliteSetSessionStaged(db, &p->targetCatHash);
    }
  }

  rc = refreshBranchScopedTables(db);
  if( rc!=SQLITE_OK ) return rc;

  if( !bSavepoint || p->bPersistUnderSavepoint ){
    rc = doltlitePersistWorkingSetWithHash(db, &p->targetCatHash);
    if( rc!=SQLITE_OK ) return rc;
  }

  if( p->haveOldState ){
    rc = doltliteUpdateBranchWorkingState(db, p->zCurrentBranch,
                                          &p->oldCatHash, &p->oldCommitHash);
    if( rc!=SQLITE_OK ){
      checkoutRestoreSession(db, p);
      return rc;
    }
  }

  if( !bSavepoint || p->bPersistUnderSavepoint ){
    rc = doltliteUpdateBranchWorkingState(db, p->zTargetBranch,
                                          &p->targetCatHash, &p->targetCommit);
    if( rc!=SQLITE_OK ){
      checkoutRestoreSession(db, p);
    }
  }
  return rc;
}

static void doltConnectBranchFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  CheckoutMutationCtx m;
  const char *zBranch;
  char *zCurrentBranch = 0;
  ProllyHash targetCommit;
  ProllyHash targetCatHash;
  u8 *oldCatData = 0;
  int nOldCat = 0;
  int rc;

  (void)argc;
  memset(&m, 0, sizeof(m));
  if( !cs ){
    sqlite3_result_error(ctx, "no database open", -1);
    return;
  }
  zBranch = (const char*)sqlite3_value_text(argv[0]);
  if( branchNameEmpty(zBranch) ){
    sqlite3_result_error(ctx, "branch name required", -1);
    return;
  }

  rc = chunkStoreFindBranch(cs, zBranch, &targetCommit);
  if( rc!=SQLITE_OK || prollyHashIsEmpty(&targetCommit) ){
    sqlite3_result_error(ctx, "branch not found", -1);
    return;
  }

  zCurrentBranch = sqlite3_mprintf("%s", doltliteGetSessionBranch(db));
  if( !zCurrentBranch ){
    sqlite3_result_error_code(ctx, SQLITE_NOMEM);
    return;
  }
  m.zCurrentBranch = zCurrentBranch;
  m.haveOldState = 1;
  doltliteGetSessionHead(db, &m.savedSessionHead);
  doltliteGetSessionStaged(db, &m.savedSessionStaged);
  doltliteGetSessionMergeState(db, &m.savedIsMerging,
                               &m.savedMergeCommit,
                               &m.savedConflictsCatalog);
  doltliteGetSessionRebaseState(db, &m.savedIsRebasing,
                                &m.savedPreRebaseCat,
                                &m.savedRebaseOnto,
                                &m.zSavedRebaseOrigBranch,
                                &m.zSavedRebaseReturnBranch);
  if( doltliteHasUncommittedChanges(db) ){
    rc = doltliteFlushAndSerializeCatalog(db, &oldCatData, &nOldCat);
    if( rc!=SQLITE_OK ){
      sqlite3_free(zCurrentBranch);
      sqlite3_result_error_code(ctx, rc);
      return;
    }
    rc = chunkStorePut(cs, oldCatData, nOldCat, &m.oldCatHash);
    sqlite3_free(oldCatData);
    if( rc!=SQLITE_OK ){
      sqlite3_free(zCurrentBranch);
      sqlite3_result_error_code(ctx, rc);
      return;
    }
  }else{
    rc = doltliteGetPersistedWorkingCatalogHash(db, &m.oldCatHash);
    if( rc==SQLITE_NOTFOUND ){
      rc = doltliteGetHeadCatalogHash(db, &m.oldCatHash);
    }
    if( rc!=SQLITE_OK ){
      sqlite3_free(zCurrentBranch);
      sqlite3_result_error_code(ctx, rc);
      return;
    }
  }

  rc = checkoutLoadAndApply(db, cs, zBranch, &targetCommit, &targetCatHash);
  if( rc!=SQLITE_OK ){
    sqlite3_free(zCurrentBranch);
    sqlite3_result_error_code(ctx, rc);
    return;
  }

  doltliteSetSessionBranch(db, zBranch);
  doltliteSetSessionHead(db, &targetCommit);
  rc = doltliteLoadWorkingSet(db, zBranch);
  if( rc==SQLITE_OK ){
    ProllyHash staged;
    doltliteGetSessionStaged(db, &staged);
    if( prollyHashIsEmpty(&staged) ){
      doltliteSetSessionStaged(db, &targetCatHash);
    }
  }
  if( rc!=SQLITE_OK ){
    checkoutRestoreSession(db, &m);
    sqlite3_free(zCurrentBranch);
    sqlite3_result_error_code(ctx, rc);
    return;
  }
  rc = refreshBranchScopedTables(db);
  if( rc!=SQLITE_OK ){
    checkoutRestoreSession(db, &m);
    sqlite3_free(zCurrentBranch);
    sqlite3_result_error_code(ctx, rc);
    return;
  }
  sqlite3_free(zCurrentBranch);
  sqlite3_result_int(ctx, 0);
}

int doltliteCheckoutBranchForRebase(sqlite3 *db, const char *zBranch){
  ChunkStore *cs = doltliteGetChunkStore(db);
  CheckoutMutationCtx m;
  char *zCurrentBranch = 0;
  u8 *oldCatData = 0;
  int nOldCat = 0;
  int rc;

  if( !cs || !zBranch || branchNameEmpty(zBranch) ) return SQLITE_ERROR;
  memset(&m, 0, sizeof(m));

  doltliteGetSessionHead(db, &m.oldCommitHash);
  zCurrentBranch = sqlite3_mprintf("%s", doltliteGetSessionBranch(db));
  if( !zCurrentBranch ) return SQLITE_NOMEM;

  if( doltliteHasUncommittedChanges(db) ){
    rc = doltliteFlushAndSerializeCatalog(db, &oldCatData, &nOldCat);
    if( rc!=SQLITE_OK ){
      sqlite3_free(zCurrentBranch);
      return rc;
    }
    rc = chunkStorePut(cs, oldCatData, nOldCat, &m.oldCatHash);
    sqlite3_free(oldCatData);
    if( rc!=SQLITE_OK ){
      sqlite3_free(zCurrentBranch);
      return rc;
    }
  }else{
    rc = doltliteGetPersistedWorkingCatalogHash(db, &m.oldCatHash);
    if( rc==SQLITE_NOTFOUND ){
      rc = doltliteGetHeadCatalogHash(db, &m.oldCatHash);
    }
    if( rc!=SQLITE_OK ){
      sqlite3_free(zCurrentBranch);
      return rc;
    }
  }

  m.haveOldState = 1;
  m.zTargetBranch = zBranch;
  m.zCurrentBranch = zCurrentBranch;
  m.bPersistUnderSavepoint = 1;
  doltliteGetSessionHead(db, &m.savedSessionHead);
  doltliteGetSessionStaged(db, &m.savedSessionStaged);
  doltliteGetSessionMergeState(db, &m.savedIsMerging,
                               &m.savedMergeCommit,
                               &m.savedConflictsCatalog);
  doltliteGetSessionRebaseState(db, &m.savedIsRebasing,
                                &m.savedPreRebaseCat,
                                &m.savedRebaseOnto,
                                &m.zSavedRebaseOrigBranch,
                                &m.zSavedRebaseReturnBranch);

  rc = doltliteMutateRefs(db, checkoutMutateRefs, &m);
  if( rc!=SQLITE_OK ){
    checkoutRestoreSession(db, &m);
    {
      int restoreRc = doltliteMutateRefs(db, checkoutRestoreDurableState, &m);
      if( restoreRc!=SQLITE_OK ) rc = restoreRc;
    }
  }
  sqlite3_free(zCurrentBranch);
  return rc;
}

/* `dolt_checkout <table>...` path. Reached as a fallthrough when the
** first argument doesn't resolve to a branch — in Dolt, checkout
** overloads "branch name" and "table name". Copies the named tables
** from the staged catalog (or HEAD if nothing is staged) into the
** working catalog, mirroring Dolt's reset-a-single-table semantics. */
static int doltliteCheckoutTables(
  sqlite3 *db,
  const char *zSourceRef,
  sqlite3_value **argv,
  int iFirstName,
  int nNames
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHash workingHash, headCatHash, stagedHash;
  ProllyHash sourceCatHash;
  ProllyHash newWorkingHash;
  CheckoutSchemaInfo *aSchema = 0;
  struct TableEntry *aWorking = 0, *aSource = 0;
  int nWorking = 0, nSource = 0;
  int i, j;
  int rc;

  if( !cs ) return SQLITE_ERROR;
  if( nNames<=0 ) return SQLITE_NOTFOUND;


  if( zSourceRef ){
    ProllyHash sourceCommit;
    DoltliteCommit sourceC;
    memset(&sourceC, 0, sizeof(sourceC));
    rc = doltliteResolveRef(db, zSourceRef, &sourceCommit);
    if( rc!=SQLITE_OK ) return SQLITE_NOTFOUND;
    rc = doltliteLoadCommit(db, &sourceCommit, &sourceC);
    if( rc!=SQLITE_OK ) return rc;
    memcpy(&sourceCatHash, &sourceC.catalogHash, sizeof(ProllyHash));
    doltliteCommitClear(&sourceC);
  }else{
    doltliteGetSessionStaged(db, &stagedHash);
    if( !prollyHashIsEmpty(&stagedHash) ){
      memcpy(&sourceCatHash, &stagedHash, sizeof(ProllyHash));
    }else{
      rc = doltliteGetHeadCatalogHash(db, &headCatHash);
      if( rc!=SQLITE_OK ) return rc;
      memcpy(&sourceCatHash, &headCatHash, sizeof(ProllyHash));
    }
    if( prollyHashIsEmpty(&sourceCatHash) ){
      return SQLITE_NOTFOUND;
    }
  }


  rc = doltliteLoadCatalog(db, &sourceCatHash, &aSource, &nSource, 0);
  if( rc!=SQLITE_OK ) return rc;

  if( zSourceRef ){
    for(i=0; i<nNames; i++){
      const char *zName = (const char*)sqlite3_value_text(argv[iFirstName + i]);
      int srcIdx = -1;
      if( !zName ) continue;
      for(j=0; j<nSource; j++){
        if( aSource[j].zName && strcmp(aSource[j].zName, zName)==0 ){
          srcIdx = j;
          break;
        }
      }
      if( srcIdx<0 ){
        doltliteFreeCatalog(aSource, nSource);
        return SQLITE_NOTFOUND;
      }
    }
  }

  aSchema = sqlite3_malloc64((sqlite3_uint64)nNames * sizeof(CheckoutSchemaInfo));
  if( !aSchema ){
    doltliteFreeCatalog(aSource, nSource);
    return SQLITE_NOMEM;
  }
  memset(aSchema, 0, (size_t)nNames * sizeof(CheckoutSchemaInfo));

  for(i=0; i<nNames; i++){
    const char *zName = (const char*)sqlite3_value_text(argv[iFirstName + i]);
    if( !zName ) continue;
    rc = checkoutLoadLiveTableSql(db, zName,
                                  &aSchema[i].hasCurrent,
                                  &aSchema[i].zCurrentSql);
    if( rc==SQLITE_OK ){
      rc = checkoutLoadSourceTableSql(db, aSource, nSource, zName,
                                      &aSchema[i].hasSource,
                                      &aSchema[i].zSourceSql);
    }
    if( rc!=SQLITE_OK ){
      checkoutSchemaInfoClear(aSchema, nNames);
      doltliteFreeCatalog(aSource, nSource);
      return rc;
    }
  }

  for(i=0; i<nNames; i++){
    const char *zName = (const char*)sqlite3_value_text(argv[iFirstName + i]);
    int bSchemaChanged;
    char *zDrop;
    if( !zName ) continue;
    bSchemaChanged =
      (aSchema[i].hasCurrent != aSchema[i].hasSource)
      || (aSchema[i].hasCurrent && aSchema[i].hasSource
          && strcmp(aSchema[i].zCurrentSql ? aSchema[i].zCurrentSql : "",
                    aSchema[i].zSourceSql ? aSchema[i].zSourceSql : "")!=0);
    if( !bSchemaChanged ) continue;

    if( aSchema[i].hasCurrent ){
      zDrop = sqlite3_mprintf("DROP TABLE \"%w\"", zName);
      if( !zDrop ){
        checkoutSchemaInfoClear(aSchema, nNames);
        doltliteFreeCatalog(aSource, nSource);
        return SQLITE_NOMEM;
      }
      rc = sqlite3_exec(db, zDrop, 0, 0, 0);
      sqlite3_free(zDrop);
      if( rc!=SQLITE_OK ){
        checkoutSchemaInfoClear(aSchema, nNames);
        doltliteFreeCatalog(aSource, nSource);
        return rc;
      }
    }
    if( aSchema[i].hasSource ){
      rc = sqlite3_exec(db, aSchema[i].zSourceSql, 0, 0, 0);
      if( rc!=SQLITE_OK ){
        checkoutSchemaInfoClear(aSchema, nNames);
        doltliteFreeCatalog(aSource, nSource);
        return rc;
      }
    }
    aSchema[i].rebuilt = 1;
  }

  rc = doltliteFlushCatalogToHash(db, &workingHash);
  if( rc!=SQLITE_OK ){
    checkoutSchemaInfoClear(aSchema, nNames);
    doltliteFreeCatalog(aSource, nSource);
    return rc;
  }
  rc = doltliteLoadCatalog(db, &workingHash, &aWorking, &nWorking, 0);
  if( rc!=SQLITE_OK ){
    checkoutSchemaInfoClear(aSchema, nNames);
    doltliteFreeCatalog(aSource, nSource);
    return rc;
  }


  for(i=0; i<nNames; i++){
    const char *zName = (const char*)sqlite3_value_text(argv[iFirstName + i]);
    int srcIdx = -1, workIdx = -1;
    char *zDup;
    if( !zName ) continue;

    for(j=0; j<nSource; j++){
      if( aSource[j].zName && strcmp(aSource[j].zName, zName)==0 ){
        srcIdx = j; break;
      }
    }
    for(j=0; j<nWorking; j++){
      if( aWorking[j].zName && strcmp(aWorking[j].zName, zName)==0 ){
        workIdx = j; break;
      }
    }

    if( srcIdx<0 && workIdx<0 ){
      if( aSchema[i].rebuilt && !aSchema[i].hasSource ){
        continue;
      }
      checkoutSchemaInfoClear(aSchema, nNames);
      doltliteFreeCatalog(aWorking, nWorking);
      doltliteFreeCatalog(aSource, nSource);
      return SQLITE_NOTFOUND;
    }

    if( srcIdx<0 ){
      sqlite3_free(aWorking[workIdx].zName);
      if( workIdx+1<nWorking ){
        memmove(&aWorking[workIdx], &aWorking[workIdx+1],
                (nWorking-workIdx-1)*(int)sizeof(struct TableEntry));
      }
      nWorking--;
    }else if( workIdx<0 ){

      struct TableEntry *aNew = sqlite3_realloc(aWorking,
          (nWorking+1)*(int)sizeof(struct TableEntry));
      if( !aNew ){
        checkoutSchemaInfoClear(aSchema, nNames);
        doltliteFreeCatalog(aWorking, nWorking);
        doltliteFreeCatalog(aSource, nSource);
        return SQLITE_NOMEM;
      }
      aWorking = aNew;
      zDup = aSource[srcIdx].zName
               ? sqlite3_mprintf("%s", aSource[srcIdx].zName) : 0;
      if( aSource[srcIdx].zName && !zDup ){
        checkoutSchemaInfoClear(aSchema, nNames);
        doltliteFreeCatalog(aWorking, nWorking);
        doltliteFreeCatalog(aSource, nSource);
        return SQLITE_NOMEM;
      }
      aWorking[nWorking] = aSource[srcIdx];
      aWorking[nWorking].zName = zDup;
      nWorking++;
    }else{
      zDup = aSource[srcIdx].zName
               ? sqlite3_mprintf("%s", aSource[srcIdx].zName) : 0;
      if( aSource[srcIdx].zName && !zDup ){
        checkoutSchemaInfoClear(aSchema, nNames);
        doltliteFreeCatalog(aWorking, nWorking);
        doltliteFreeCatalog(aSource, nSource);
        return SQLITE_NOMEM;
      }
      {
        Pgno iCurrentTable = aWorking[workIdx].iTable;
        sqlite3_free(aWorking[workIdx].zName);
        aWorking[workIdx] = aSource[srcIdx];
        if( aSchema[i].rebuilt ){
          aWorking[workIdx].iTable = iCurrentTable;
        }
        aWorking[workIdx].zName = zDup;
      }
    }
  }


  {
    u8 *buf = 0;
    int nBuf = 0;
    rc = doltliteSerializeCatalogEntries(db, aWorking, nWorking, &buf, &nBuf);
    if( rc==SQLITE_OK ){
      rc = chunkStorePut(cs, buf, nBuf, &newWorkingHash);
    }
    sqlite3_free(buf);
    if( rc==SQLITE_OK ){
      rc = doltliteSwitchCatalog(db, &newWorkingHash);
    }
    if( rc==SQLITE_OK && zSourceRef ){
      doltliteSetSessionStaged(db, &newWorkingHash);
    }
    if( rc==SQLITE_OK ){
      rc = doltlitePersistWorkingSet(db);
    }
  }

  checkoutSchemaInfoClear(aSchema, nNames);
  doltliteFreeCatalog(aWorking, nWorking);
  doltliteFreeCatalog(aSource, nSource);
  return rc;
}

static void doltCheckoutFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  CheckoutMutationCtx m;
  BranchMutationCtx branchCreate;
  const char *zBranch;
  char *zCurrentBranch = 0;
  int isCreateAndSwitch = 0;
  int hadExplicitTxn = !db->autoCommit;
  int rc;

  if( !cs ){ (void)doltliteVcSealSavepointError(db); sqlite3_result_error(ctx, "no database", -1); return; }
  if( argc<1 ){ (void)doltliteVcSealSavepointError(db); sqlite3_result_error(ctx, "branch name required", -1); return; }
  zBranch = (const char*)sqlite3_value_text(argv[0]);
  if( !zBranch ){ (void)doltliteVcSealSavepointError(db); sqlite3_result_error(ctx, "branch name required", -1); return; }

  memset(&m, 0, sizeof(m));
  memset(&branchCreate, 0, sizeof(branchCreate));


  {
    u8 isMerging = 0;
    doltliteGetSessionMergeState(db, &isMerging, 0, 0);
    if( isMerging ){
      (void)doltliteVcSealSavepointError(db);
      sqlite3_result_error(ctx, "unresolved merge conflicts \xe2\x80\x94 commit or abort first", -1);
      return;
    }
  }


  if( strcmp(zBranch, "-b")==0 ){
    if( argc<2 ){ (void)doltliteVcSealSavepointError(db); sqlite3_result_error(ctx, "branch name required after -b", -1); return; }
    if( argc>3 ){ (void)doltliteVcSealSavepointError(db); sqlite3_result_error(ctx, "too many arguments", -1); return; }
    zBranch = (const char*)sqlite3_value_text(argv[1]);
    if( branchNameEmpty(zBranch) ){ (void)doltliteVcSealSavepointError(db); sqlite3_result_error(ctx, "branch name required after -b", -1); return; }

    if( argc>=3 ){
      const char *zStart = (const char*)sqlite3_value_text(argv[2]);
      if( !zStart ){
        (void)doltliteVcSealSavepointError(db);
        sqlite3_result_error(ctx, "start point not found", -1);
        return;
      }
      rc = doltliteResolveRef(db, zStart, &branchCreate.head);
      if( rc!=SQLITE_OK ){
        (void)doltliteVcSealSavepointError(db);
        sqlite3_result_error(ctx, "start point not found", -1);
        return;
      }
    }else{
      doltliteGetSessionHead(db, &branchCreate.head);
      if( prollyHashIsEmpty(&branchCreate.head) ){
        (void)doltliteVcSealSavepointError(db);
        sqlite3_result_error(ctx, "no commits yet — commit first", -1);
        return;
      }
    }
    branchCreate.zName = zBranch;
    rc = doltliteMutateRefs(db, mutateBranchRef, &branchCreate);
    if( rc!=SQLITE_OK ){
      (void)doltliteVcSealSavepointError(db);
      sqlite3_result_error(ctx, "branch already exists", -1);
      return;
    }
    isCreateAndSwitch = 1;
  }

  if( strcmp(zBranch, doltliteGetSessionBranch(db))==0 && argc==1 ){
    sqlite3_result_int(ctx, 0);
    return;
  }

  if( argc>1 && !isCreateAndSwitch ){
    ProllyHash sourceRef;
    int bHasSourceRef = 0;
    rc = doltliteResolveRef(db, zBranch, &sourceRef);
    if( rc==SQLITE_OK ){
      bHasSourceRef = 1;
      rc = doltliteCheckoutTables(db, zBranch, argv, 1, argc-1);
    }else{
      rc = doltliteCheckoutTables(db, 0, argv, 0, argc);
    }
    if( rc==SQLITE_NOTFOUND ){
      const char *zMissing = bHasSourceRef ? zBranch : zBranch;
      char *zErr = sqlite3_mprintf("no such branch or table: %s", zMissing);
      (void)doltliteVcSealSavepointError(db);
      sqlite3_result_error(ctx, zErr ? zErr : "no such branch or table", -1);
      sqlite3_free(zErr);
      return;
    }
    if( rc!=SQLITE_OK ){
      (void)doltliteVcSealSavepointError(db);
      sqlite3_result_error_code(ctx, rc);
      return;
    }
    if( !db->autoCommit || db->pSavepoint ){
      rc = doltliteVcSealBranchStyleTxn(db);
      if( rc!=SQLITE_OK ){
        sqlite3_result_error_code(ctx, rc);
        return;
      }
    }
    sqlite3_result_int(ctx, 0);
    return;
  }


  {
    u8 *oldCatData = 0; int nOldCat = 0;
  doltliteGetSessionHead(db, &m.oldCommitHash);
  zCurrentBranch = sqlite3_mprintf("%s", doltliteGetSessionBranch(db));
  if( !zCurrentBranch ){
      (void)doltliteVcSealSavepointError(db);
      sqlite3_result_error_nomem(ctx);
      return;
    }
    if( doltliteHasUncommittedChanges(db) ){
      rc = doltliteFlushAndSerializeCatalog(db, &oldCatData, &nOldCat);
      if( rc!=SQLITE_OK ){
        sqlite3_free(zCurrentBranch);
        (void)doltliteVcSealSavepointError(db);
        sqlite3_result_error(ctx, "failed to snapshot current branch state", -1);
        return;
      }
      rc = chunkStorePut(cs, oldCatData, nOldCat, &m.oldCatHash);
      sqlite3_free(oldCatData);
      if( rc!=SQLITE_OK ){
        sqlite3_free(zCurrentBranch);
        (void)doltliteVcSealSavepointError(db);
        sqlite3_result_error(ctx, "failed to snapshot current branch state", -1);
        return;
      }
    }else{
      rc = doltliteGetPersistedWorkingCatalogHash(db, &m.oldCatHash);
      if( rc==SQLITE_NOTFOUND ){
        rc = doltliteGetHeadCatalogHash(db, &m.oldCatHash);
      }
      if( rc!=SQLITE_OK ){
        sqlite3_free(zCurrentBranch);
        (void)doltliteVcSealSavepointError(db);
        sqlite3_result_error(ctx, "failed to snapshot current branch state", -1);
        return;
      }
    }
    m.haveOldState = 1;
  }
  doltliteGetSessionHead(db, &m.savedSessionHead);
  doltliteGetSessionStaged(db, &m.savedSessionStaged);
  doltliteGetSessionMergeState(db, &m.savedIsMerging,
                               &m.savedMergeCommit,
                               &m.savedConflictsCatalog);
  doltliteGetSessionRebaseState(db, &m.savedIsRebasing,
                                &m.savedPreRebaseCat,
                                &m.savedRebaseOnto,
                                &m.zSavedRebaseOrigBranch,
                                &m.zSavedRebaseReturnBranch);

  m.zTargetBranch = zBranch;
  m.zCurrentBranch = zCurrentBranch;
  rc = doltliteMutateRefs(db, checkoutMutateRefs, &m);
  if( rc!=SQLITE_OK ){
    checkoutRestoreSession(db, &m);
    {
      int restoreRc = doltliteMutateRefs(db, checkoutRestoreDurableState, &m);
      if( restoreRc!=SQLITE_OK ) rc = restoreRc;
    }
  }
  sqlite3_free(zCurrentBranch);
  zCurrentBranch = 0;
  if( rc==SQLITE_NOTFOUND ){

    rc = doltliteCheckoutTables(db, 0, argv, 0, argc);
    if( rc==SQLITE_NOTFOUND ){
      char *zErr = sqlite3_mprintf(
          "no such branch or table: %s", zBranch);
      (void)doltliteVcSealSavepointError(db);
      sqlite3_result_error(ctx, zErr ? zErr : "no such branch or table", -1);
      sqlite3_free(zErr);
      return;
    }
    if( rc!=SQLITE_OK ){
      (void)doltliteVcSealSavepointError(db);
      sqlite3_result_error_code(ctx, rc);
      return;
    }
    if( !db->autoCommit || db->pSavepoint ){
      rc = doltliteVcSealBranchStyleTxn(db);
      if( rc!=SQLITE_OK ){
        sqlite3_result_error_code(ctx, rc);
        return;
      }
    }
    sqlite3_result_int(ctx, 0);
    return;
  }
  if( rc==SQLITE_EMPTY ){
    (void)doltliteVcSealSavepointError(db);
    sqlite3_result_error(ctx, "target branch has no commits", -1);
    return;
  }
  if( rc==SQLITE_BUSY ){
    (void)doltliteVcSealSavepointError(db);
    sqlite3_result_error(ctx, "database is locked by another connection", -1);
    return;
  }
  if( rc!=SQLITE_OK ){
    (void)doltliteVcSealSavepointError(db);
    sqlite3_result_error(ctx, "checkout failed", -1);
    return;
  }
  if( hadExplicitTxn ){
    rc = doltliteVcSealBranchStyleTxn(db);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(ctx, rc);
      return;
    }
  }
  sqlite3_result_int(ctx, 0);
}

typedef struct BrVtab BrVtab;
struct BrVtab { sqlite3_vtab base; sqlite3 *db; };
typedef struct BrCur BrCur;
struct BrCur { sqlite3_vtab_cursor base; int iRow; };

static int brConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  BrVtab *p; int rc;
  (void)pAux; (void)argc; (void)argv; (void)pzErr;
  rc = sqlite3_declare_vtab(db,
    "CREATE TABLE x("
      "name TEXT, "
      "hash TEXT, "
      "latest_committer TEXT, "
      "latest_committer_email TEXT, "
      "latest_commit_date TEXT, "
      "latest_commit_message TEXT, "
      "remote TEXT, "
      "branch TEXT, "
      "dirty INTEGER"
    ")");
  if( rc!=SQLITE_OK ) return rc;
  p = sqlite3_malloc(sizeof(*p));
  if( !p ) return SQLITE_NOMEM;
  memset(p, 0, sizeof(*p)); p->db = db;
  *ppVtab = &p->base;
  return SQLITE_OK;
}
static int brDisconnect(sqlite3_vtab *v){ sqlite3_free(v); return SQLITE_OK; }
static int brOpen(sqlite3_vtab *v, sqlite3_vtab_cursor **pp){
  BrCur *c = sqlite3_malloc(sizeof(*c)); (void)v;
  if(!c) return SQLITE_NOMEM; memset(c,0,sizeof(*c)); *pp=&c->base; return SQLITE_OK;
}
static int brClose(sqlite3_vtab_cursor *c){ sqlite3_free(c); return SQLITE_OK; }
static int brFilter(sqlite3_vtab_cursor *c, int n, const char *s, int a, sqlite3_value **v){
  (void)n;(void)s;(void)a;(void)v;
  ((BrCur*)c)->iRow = 0; return SQLITE_OK;
}
static int brNext(sqlite3_vtab_cursor *c){ ((BrCur*)c)->iRow++; return SQLITE_OK; }
static int brEof(sqlite3_vtab_cursor *c){
  BrVtab *v = (BrVtab*)c->pVtab;
  ChunkStore *cs = doltliteGetChunkStore(v->db);
  return !cs || ((BrCur*)c)->iRow >= cs->nBranches;
}

static int brIsDirty(
  sqlite3 *db,
  ChunkStore *cs,
  struct BranchRef *br,
  int *pDirty
){
  ProllyHash stagedCat;
  ProllyHash commitCat;
  u8 *wsData = 0;
  int nWsData = 0;
  int rc;

  *pDirty = 0;

  if( strcmp(br->zName, doltliteGetSessionBranch(db))==0 ){
    *pDirty = doltliteHasUncommittedChanges(db) ? 1 : 0;
    return SQLITE_OK;
  }
  if( prollyHashIsEmpty(&br->workingSetHash) ){
    return SQLITE_OK;
  }

  rc = chunkStoreGet(cs, &br->workingSetHash, &wsData, &nWsData);
  if( rc!=SQLITE_OK || !wsData || nWsData < WS_TOTAL_SIZE || wsData[0] != WS_FORMAT_VERSION ){
    sqlite3_free(wsData);
    return rc==SQLITE_OK ? SQLITE_CORRUPT : rc;
  }
  memcpy(stagedCat.data, wsData + WS_STAGED_OFF, PROLLY_HASH_SIZE);
  sqlite3_free(wsData);

  if( prollyHashIsEmpty(&stagedCat) ){
    return SQLITE_OK;
  }

  {
    DoltliteCommit c;
    memset(&c, 0, sizeof(c));
    rc = doltliteLoadCommit(db, &br->commitHash, &c);
    if( rc==SQLITE_OK ){
      memcpy(commitCat.data, c.catalogHash.data, PROLLY_HASH_SIZE);
      *pDirty = prollyHashCompare(&stagedCat, &commitCat)!=0;
    }
    doltliteCommitClear(&c);
  }
  return rc;
}

static int brColumn(sqlite3_vtab_cursor *c, sqlite3_context *ctx, int col){
  BrVtab *v = (BrVtab*)c->pVtab;
  ChunkStore *cs = doltliteGetChunkStore(v->db);
  struct BranchRef *br;
  if(!cs) return SQLITE_OK;
  br = &cs->aBranches[((BrCur*)c)->iRow];

  switch(col){
    case 0:
      sqlite3_result_text(ctx, br->zName, -1, SQLITE_TRANSIENT);
      return SQLITE_OK;
    case 1: {
      char h[41];
      doltliteHashToHex(&br->commitHash, h);
      sqlite3_result_text(ctx, h, -1, SQLITE_TRANSIENT);
      return SQLITE_OK;
    }
    case 6: case 7:

      sqlite3_result_text(ctx, "", -1, SQLITE_STATIC);
      return SQLITE_OK;
    case 8: {
      int dirty = 0;
      int rc = brIsDirty(v->db, cs, br, &dirty);
      if( rc!=SQLITE_OK ){
        sqlite3_result_error_code(ctx, rc);
        return rc;
      }
      sqlite3_result_int(ctx, dirty);
      return SQLITE_OK;
    }
  }


  {
    DoltliteCommit cm;
    int rc;
    memset(&cm, 0, sizeof(cm));
    rc = doltliteLoadCommit(v->db, &br->commitHash, &cm);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(ctx, rc);
      doltliteCommitClear(&cm);
      return rc;
    }
    switch(col){
      case 2:
        sqlite3_result_text(ctx, cm.zName ? cm.zName : "",
                            -1, SQLITE_TRANSIENT);
        break;
      case 3:
        sqlite3_result_text(ctx, cm.zEmail ? cm.zEmail : "",
                            -1, SQLITE_TRANSIENT);
        break;
      case 4: {
        time_t t = (time_t)cm.timestamp;
        struct tm *tm = gmtime(&t);
        if( tm ){
          char buf[32];
          strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", tm);
          sqlite3_result_text(ctx, buf, -1, SQLITE_TRANSIENT);
        }else{
          sqlite3_result_null(ctx);
        }
        break;
      }
      case 5:
        sqlite3_result_text(ctx, cm.zMessage ? cm.zMessage : "",
                            -1, SQLITE_TRANSIENT);
        break;
    }
    doltliteCommitClear(&cm);
  }
  return SQLITE_OK;
}
static int brRowid(sqlite3_vtab_cursor *c, sqlite3_int64 *r){
  *r=((BrCur*)c)->iRow; return SQLITE_OK;
}
static int brBestIndex(sqlite3_vtab *v, sqlite3_index_info *p){
  (void)v; p->estimatedCost=10; p->estimatedRows=5; return SQLITE_OK;
}
static sqlite3_module brMod = {
  0,0,brConnect,brBestIndex,brDisconnect,0,
  brOpen,brClose,brFilter,brNext,brEof,brColumn,brRowid,
  0,0,0,0,0,0,0,0,0,0,0,0
};

int doltliteBranchRegister(sqlite3 *db){
  int rc;
  rc = sqlite3_create_function(db, "dolt_branch", -1, SQLITE_UTF8, 0, doltBranchFunc, 0, 0);
  if(rc==SQLITE_OK) rc = sqlite3_create_function(db, "dolt_checkout", -1, SQLITE_UTF8, 0, doltCheckoutFunc, 0, 0);
  if(rc==SQLITE_OK) rc = sqlite3_create_function(db, "active_branch", 0, SQLITE_UTF8, 0, activeBranchFunc, 0, 0);
  if(rc==SQLITE_OK) rc = sqlite3_create_function(db, "dolt_connect_branch", 1, SQLITE_UTF8, 0, doltConnectBranchFunc, 0, 0);
  if(rc==SQLITE_OK) rc = sqlite3_create_module(db, "dolt_branches", &brMod, 0);
  return rc;
}

#endif
