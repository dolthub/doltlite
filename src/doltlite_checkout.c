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

static int checkoutLoadAndApply(
  sqlite3 *db,
  ChunkStore *cs,
  const char *zBranch,
  ProllyHash *pCommitHash,
  ProllyHash *pCatHash
){
  int rc;
  ProllyHash committedCatHash;

  if( prollyHashIsEmpty(pCommitHash) ){
    /* Unborn branch: no commit, so its catalog baseline is empty. */
    memset(&committedCatHash, 0, sizeof(committedCatHash));
  }else{
    rc = doltliteCommitCatalogHash(db, pCommitHash, &committedCatHash);
    if( rc!=SQLITE_OK ) return rc;
  }

  doltliteResolveBranchEffectiveCatalog(cs, zBranch, pCommitHash,
                                        &committedCatHash, pCatHash);
  rc = doltliteSwitchCatalog(db, pCatHash);
  return rc;
}

static int refreshBranchScopedTables(sqlite3 *db){
  int rc;
  extern int doltliteRegisterDiffTables(sqlite3 *db);
  extern int doltliteRegisterWorkspaceTables(sqlite3 *db);
  extern int doltliteRegisterHistoryTables(sqlite3 *db);
  extern int doltliteRegisterAtTables(sqlite3 *db);
  extern int doltliteRegisterBlameTables(sqlite3 *db);

  rc = doltliteRegisterDiffTables(db);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteRegisterWorkspaceTables(db);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteRegisterHistoryTables(db);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteRegisterAtTables(db);
  if( rc!=SQLITE_OK ) return rc;
  return doltliteRegisterBlameTables(db);
}

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
  /* Top-level branch connection checkout must persist even though SQLite has
  ** a savepoint frame; ordinary nested savepoint checkout remains rollbackable. */
  int bPersistUnderSavepoint;
};

/* Snapshot the current branch's working catalog into *pOldCatHash: serialize
** uncommitted changes into the store, else use the persisted working hash
** (falling back to HEAD). Returns an error code; callers handle reporting. */
static int checkoutCaptureOldCatalog(sqlite3 *db, ChunkStore *cs,
                                     ProllyHash *pOldCatHash){
  int dirty;
  int rc = doltliteHasUncommittedChanges(db, &dirty);
  if( rc!=SQLITE_OK ) return rc;
  if( dirty ){
    u8 *oldCatData = 0;
    int nOldCat = 0;
    rc = doltliteFlushAndSerializeCatalog(db, &oldCatData, &nOldCat);
    if( rc!=SQLITE_OK ) return rc;
    rc = chunkStorePut(cs, oldCatData, nOldCat, pOldCatHash);
    sqlite3_free(oldCatData);
    return rc;
  }else{
    int rc = doltliteGetPersistedWorkingCatalogHash(db, pOldCatHash);
    if( rc==SQLITE_NOTFOUND ) rc = doltliteGetHeadCatalogHash(db, pOldCatHash);
    return rc;
  }
}

/* Mirror of checkoutRestoreSession: capture the session head/staged/merge/
** rebase state so a failed checkout can roll back to it. */
static void checkoutSaveSession(sqlite3 *db, CheckoutMutationCtx *p){
  doltliteGetSessionHead(db, &p->savedSessionHead);
  doltliteGetSessionStaged(db, &p->savedSessionStaged);
  doltliteGetSessionMergeState(db, &p->savedIsMerging,
                               &p->savedMergeCommit,
                               &p->savedConflictsCatalog);
  doltliteGetSessionRebaseState(db, &p->savedIsRebasing,
                                &p->savedPreRebaseCat,
                                &p->savedRebaseOnto,
                                &p->zSavedRebaseOrigBranch,
                                &p->zSavedRebaseReturnBranch);
}

static int checkoutRestoreSession(sqlite3 *db, CheckoutMutationCtx *p){
  int rc;
  rc = doltliteSetSessionBranch(db, p->zCurrentBranch);
  if( rc==SQLITE_OK ){
    doltliteSetSessionHead(db, &p->savedSessionHead);
    rc = doltliteSetSessionRebaseState(db, p->savedIsRebasing,
                                       &p->savedPreRebaseCat,
                                       &p->savedRebaseOnto,
                                       p->zSavedRebaseOrigBranch,
                                       p->zSavedRebaseReturnBranch);
  }
  if( rc==SQLITE_OK ){
    rc = doltliteSetSessionStaged(db, &p->savedSessionStaged);
  }
  if( rc==SQLITE_OK ){
    rc = doltliteSetSessionMergeState(db, p->savedIsMerging,
                                      &p->savedMergeCommit,
                                      &p->savedConflictsCatalog);
  }
  if( rc==SQLITE_OK && p->haveOldState ){
    rc = doltliteSwitchCatalog(db, &p->oldCatHash);
  }
  return rc;
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

  rc = checkoutLoadAndApply(db, cs, p->zTargetBranch,
                            &p->targetCommit, &p->targetCatHash);
  if( rc!=SQLITE_OK ) return rc;

  rc = doltliteSetSessionBranch(db, p->zTargetBranch);
  if( rc!=SQLITE_OK ) return rc;
  doltliteSetSessionHead(db, &p->targetCommit);

  rc = doltliteLoadWorkingSet(db, p->zTargetBranch);
  if( rc!=SQLITE_OK ) return rc;

  {
    ProllyHash staged;
    doltliteGetSessionStaged(db, &staged);
    if( prollyHashIsEmpty(&staged) ){
      rc = doltliteSetSessionStaged(db, &p->targetCatHash);
      if( rc!=SQLITE_OK ) return rc;
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
      int restoreRc = checkoutRestoreSession(db, p);
      if( restoreRc!=SQLITE_OK ) rc = restoreRc;
      return rc;
    }
  }

  if( !bSavepoint || p->bPersistUnderSavepoint ){
    rc = doltliteUpdateBranchWorkingState(db, p->zTargetBranch,
                                          &p->targetCatHash, &p->targetCommit);
    if( rc!=SQLITE_OK ){
      int restoreRc = checkoutRestoreSession(db, p);
      if( restoreRc!=SQLITE_OK ) rc = restoreRc;
    }
  }
  return rc;
}

static int checkoutCreateFromRemoteTracking(
  sqlite3 *db,
  const char *zBranch
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  const TrackingBranch *aTk = 0;
  int nTk = 0;
  int i;
  int iMatch = -1;
  BranchMutationCtx m;

  if( !cs || strchr(zBranch, '/')!=0 ) return SQLITE_NOTFOUND;
  refsTableGetTracking(&cs->refs, &nTk, &aTk);
  for(i=0; i<nTk; i++){
    if( aTk[i].zBranch && strcmp(aTk[i].zBranch, zBranch)==0 ){
      if( iMatch>=0 ) return SQLITE_NOTFOUND;
      iMatch = i;
    }
  }
  if( iMatch<0 ) return SQLITE_NOTFOUND;
  if( prollyHashIsEmpty(&aTk[iMatch].commitHash) ) return SQLITE_NOTFOUND;

  memset(&m, 0, sizeof(m));
  m.zName = zBranch;
  memcpy(&m.head, &aTk[iMatch].commitHash, sizeof(ProllyHash));
  return doltliteMutateRefs(db, mutateBranchRef, &m);
}

void doltConnectBranchFunc(
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
  int rc;

  (void)argc;
  memset(&m, 0, sizeof(m));
  if( !cs ){
    sqlite3_result_error(ctx, doltliteVcUnavailableMessage(db), -1);
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
  checkoutSaveSession(db, &m);
  rc = checkoutCaptureOldCatalog(db, cs, &m.oldCatHash);
  if( rc!=SQLITE_OK ){
    sqlite3_free(zCurrentBranch);
    sqlite3_result_error_code(ctx, rc);
    return;
  }

  rc = checkoutLoadAndApply(db, cs, zBranch, &targetCommit, &targetCatHash);
  if( rc!=SQLITE_OK ){
    sqlite3_free(zCurrentBranch);
    sqlite3_result_error_code(ctx, rc);
    return;
  }

  rc = doltliteSetSessionBranch(db, zBranch);
  if( rc==SQLITE_OK ){
    doltliteSetSessionHead(db, &targetCommit);
    rc = doltliteLoadWorkingSet(db, zBranch);
  }
  if( rc!=SQLITE_OK ){
    int restoreRc = checkoutRestoreSession(db, &m);
    if( restoreRc!=SQLITE_OK ) rc = restoreRc;
    sqlite3_free(zCurrentBranch);
    sqlite3_result_error_code(ctx, rc);
    return;
  }
  rc = refreshBranchScopedTables(db);
  if( rc!=SQLITE_OK ){
    int restoreRc = checkoutRestoreSession(db, &m);
    if( restoreRc!=SQLITE_OK ) rc = restoreRc;
    sqlite3_free(zCurrentBranch);
    sqlite3_result_error_code(ctx, rc);
    return;
  }
  sqlite3_free(zCurrentBranch);
  sqlite3_result_int(ctx, 0);
}

static int checkoutBranchForRebase(
  sqlite3 *db,
  const char *zBranch,
  const ProllyHash *pKnownOldCatHash
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  CheckoutMutationCtx m;
  char *zCurrentBranch = 0;
  int rc;

  assert( db!=0 && zBranch!=0 && zBranch[0]!=0 );
  if( !cs || !zBranch || branchNameEmpty(zBranch) ) return SQLITE_ERROR;
  memset(&m, 0, sizeof(m));

  doltliteGetSessionHead(db, &m.oldCommitHash);
  zCurrentBranch = sqlite3_mprintf("%s", doltliteGetSessionBranch(db));
  if( !zCurrentBranch ) return SQLITE_NOMEM;

  if( pKnownOldCatHash ){
    memcpy(&m.oldCatHash, pKnownOldCatHash, sizeof(ProllyHash));
  }else{
    rc = checkoutCaptureOldCatalog(db, cs, &m.oldCatHash);
    if( rc!=SQLITE_OK ){
      sqlite3_free(zCurrentBranch);
      return rc;
    }
  }

  m.haveOldState = 1;
  m.zTargetBranch = zBranch;
  m.zCurrentBranch = zCurrentBranch;
  m.bPersistUnderSavepoint = 1;
  checkoutSaveSession(db, &m);

  rc = doltliteMutateRefs(db, checkoutMutateRefs, &m);
  if( rc!=SQLITE_OK ){
    int restoreRc = checkoutRestoreSession(db, &m);
    if( restoreRc!=SQLITE_OK ) rc = restoreRc;
    {
      int durableRc = doltliteMutateRefs(db, checkoutRestoreDurableState, &m);
      if( durableRc!=SQLITE_OK ) rc = durableRc;
    }
  }
  sqlite3_free(zCurrentBranch);
  return rc;
}

int doltliteCheckoutBranchForRebase(sqlite3 *db, const char *zBranch){
  return checkoutBranchForRebase(db, zBranch, 0);
}

int doltliteCheckoutBranchForRebaseWithOldCatalog(
  sqlite3 *db,
  const char *zBranch,
  const ProllyHash *pOldCatHash
){
  if( !pOldCatHash ) return SQLITE_MISUSE;
  return checkoutBranchForRebase(db, zBranch, pOldCatHash);
}

/* A restored table's indexes must follow it: named indexes present only in
** the working schema are dropped, the source's named indexes are (re)created,
** and differing definitions are replaced. Entry roots are reconciled
** separately after the catalog flush. */
static int checkoutReconcileTableIndexes(
  sqlite3 *db,
  SchemaEntry *aSourceSchema,
  int nSourceSchema,
  const char *zTable
){
  sqlite3_stmt *pStmt = 0;
  char **azDrop = 0;
  int nDrop = 0;
  int i, j, rc;

  rc = sqlite3_prepare_v2(db,
      "SELECT name, sql FROM sqlite_master"
      " WHERE type='index' AND tbl_name=?1 AND sql IS NOT NULL",
      -1, &pStmt, 0);
  if( rc!=SQLITE_OK ) return rc;
  sqlite3_bind_text(pStmt, 1, zTable, -1, SQLITE_STATIC);
  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    const char *zName = (const char*)sqlite3_column_text(pStmt, 0);
    const char *zSql = (const char*)sqlite3_column_text(pStmt, 1);
    int keep = 0;
    if( !zName ) continue;
    for(j=0; j<nSourceSchema; j++){
      if( aSourceSchema[j].zType && strcmp(aSourceSchema[j].zType, "index")==0
       && aSourceSchema[j].zTblName && strcmp(aSourceSchema[j].zTblName, zTable)==0
       && aSourceSchema[j].zName && strcmp(aSourceSchema[j].zName, zName)==0
       && aSourceSchema[j].zSql ){
        char *zLiveCanon = doltliteCanonicalizeSchemaSql(zSql ? zSql : "", zName);
        char *zSrcCanon = doltliteCanonicalizeSchemaSql(aSourceSchema[j].zSql, zName);
        keep = zLiveCanon && zSrcCanon && strcmp(zLiveCanon, zSrcCanon)==0;
        sqlite3_free(zLiveCanon);
        sqlite3_free(zSrcCanon);
        break;
      }
    }
    if( !keep ){
      char **azNew = sqlite3_realloc((void*)azDrop, (nDrop+1)*(int)sizeof(char*));
      char *zOwn = azNew ? sqlite3_mprintf("%s", zName) : 0;
      if( azNew ) azDrop = azNew;
      if( !azNew || !zOwn ){
        sqlite3_finalize(pStmt);
        doltliteFreeNameList(azDrop, nDrop);
        return SQLITE_NOMEM;
      }
      azDrop[nDrop++] = zOwn;
    }
  }
  rc = sqlite3_finalize(pStmt);
  if( rc!=SQLITE_OK ){
    doltliteFreeNameList(azDrop, nDrop);
    return rc;
  }

  for(i=0; i<nDrop && rc==SQLITE_OK; i++){
    char *zSql = sqlite3_mprintf("DROP INDEX \"%w\"", azDrop[i]);
    if( !zSql ){ rc = SQLITE_NOMEM; break; }
    rc = sqlite3_exec(db, zSql, 0, 0, 0);
    sqlite3_free(zSql);
  }
  doltliteFreeNameList(azDrop, nDrop);
  if( rc!=SQLITE_OK ) return rc;

  for(j=0; j<nSourceSchema; j++){
    int exists;
    sqlite3_stmt *pChk = 0;
    if( !aSourceSchema[j].zType || strcmp(aSourceSchema[j].zType, "index")!=0 ) continue;
    if( !aSourceSchema[j].zTblName || strcmp(aSourceSchema[j].zTblName, zTable)!=0 ) continue;
    if( !aSourceSchema[j].zName || !aSourceSchema[j].zSql ) continue;
    rc = sqlite3_prepare_v2(db,
        "SELECT 1 FROM sqlite_master WHERE type='index' AND name=?1",
        -1, &pChk, 0);
    if( rc!=SQLITE_OK ) return rc;
    sqlite3_bind_text(pChk, 1, aSourceSchema[j].zName, -1, SQLITE_STATIC);
    exists = sqlite3_step(pChk)==SQLITE_ROW;
    sqlite3_finalize(pChk);
    if( !exists ){
      rc = sqlite3_exec(db, aSourceSchema[j].zSql, 0, 0, 0);
      if( rc!=SQLITE_OK ) return rc;
    }
  }
  return SQLITE_OK;
}

/* Point the freshly flushed working catalog's index entries for zTable at
** the source's trees. Each side is resolved through its OWN schema rows
** (index entries are unnamed, so name-keyed overlays cannot see them), and
** the DDL reconcile above guarantees the two row sets agree by name. */
static void checkoutAdoptSourceIndexRoots(
  struct TableEntry *aWorking, int nWorking,
  SchemaEntry *aWorkSchema, int nWorkSchema,
  struct TableEntry *aSource, int nSource,
  SchemaEntry *aSourceSchema, int nSourceSchema,
  const char *zTable
){
  int j, k;
  for(j=0; j<nSourceSchema; j++){
    struct TableEntry *pSrcEntry, *pWorkEntry;
    if( !aSourceSchema[j].zType || strcmp(aSourceSchema[j].zType, "index")!=0 ) continue;
    if( !aSourceSchema[j].zTblName || strcmp(aSourceSchema[j].zTblName, zTable)!=0 ) continue;
    if( !aSourceSchema[j].zName ) continue;
    pSrcEntry = doltliteFindTableByNumber(aSource, nSource,
                                          aSourceSchema[j].iRootpage);
    if( !pSrcEntry ) continue;
    for(k=0; k<nWorkSchema; k++){
      if( aWorkSchema[k].zType && strcmp(aWorkSchema[k].zType, "index")==0
       && aWorkSchema[k].zName
       && strcmp(aWorkSchema[k].zName, aSourceSchema[j].zName)==0 ){
        break;
      }
    }
    if( k>=nWorkSchema ) continue;
    pWorkEntry = doltliteFindTableByNumber(aWorking, nWorking,
                                           aWorkSchema[k].iRootpage);
    if( !pWorkEntry || pWorkEntry->iTable<=1 ) continue;
    pWorkEntry->root = pSrcEntry->root;
    pWorkEntry->schemaHash = pSrcEntry->schemaHash;
    pWorkEntry->flags = pSrcEntry->flags;
  }
}

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
  SchemaEntry *aSourceSchema = 0, *aWorkSchema = 0;
  int nWorking = 0, nSource = 0;
  int nSourceSchema = 0, nWorkSchema = 0;
  int i, j;
  int rc;

  if( !cs ) return SQLITE_ERROR;
  if( nNames<=0 ) return SQLITE_NOTFOUND;

  if( zSourceRef ){
    ProllyHash sourceCommit;
    rc = doltliteResolveRef(db, zSourceRef, &sourceCommit);
    if( rc!=SQLITE_OK ) return SQLITE_NOTFOUND;
    rc = doltliteCommitCatalogHash(db, &sourceCommit, &sourceCatHash);
    if( rc!=SQLITE_OK ) return rc;
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

  rc = loadSchemaFromCatalog(db, cs, doltliteGetCache(db), &sourceCatHash,
                             &aSourceSchema, &nSourceSchema);
  if( rc!=SQLITE_OK ){
    checkoutSchemaInfoClear(aSchema, nNames);
    doltliteFreeCatalog(aSource, nSource);
    return rc;
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
    if( bSchemaChanged ){
      if( aSchema[i].hasCurrent ){
        zDrop = sqlite3_mprintf("DROP TABLE \"%w\"", zName);
        if( !zDrop ){
          rc = SQLITE_NOMEM;
          break;
        }
        rc = sqlite3_exec(db, zDrop, 0, 0, 0);
        sqlite3_free(zDrop);
        if( rc!=SQLITE_OK ) break;
      }
      if( aSchema[i].hasSource ){
        rc = sqlite3_exec(db, aSchema[i].zSourceSql, 0, 0, 0);
        if( rc!=SQLITE_OK ) break;
      }
      aSchema[i].rebuilt = 1;
    }
    if( aSchema[i].hasSource ){
      rc = checkoutReconcileTableIndexes(db, aSourceSchema, nSourceSchema, zName);
      if( rc!=SQLITE_OK ) break;
    }
  }
  if( rc!=SQLITE_OK ){
    freeSchemaEntries(aSourceSchema, nSourceSchema);
    checkoutSchemaInfoClear(aSchema, nNames);
    doltliteFreeCatalog(aSource, nSource);
    return rc;
  }

  rc = doltliteFlushCatalogToHash(db, &workingHash);
  if( rc!=SQLITE_OK ){
    freeSchemaEntries(aSourceSchema, nSourceSchema);
    checkoutSchemaInfoClear(aSchema, nNames);
    doltliteFreeCatalog(aSource, nSource);
    return rc;
  }
  rc = doltliteLoadCatalog(db, &workingHash, &aWorking, &nWorking, 0);
  if( rc!=SQLITE_OK ){
    freeSchemaEntries(aSourceSchema, nSourceSchema);
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
      freeSchemaEntries(aSourceSchema, nSourceSchema);
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
        freeSchemaEntries(aSourceSchema, nSourceSchema);
        checkoutSchemaInfoClear(aSchema, nNames);
        doltliteFreeCatalog(aWorking, nWorking);
        doltliteFreeCatalog(aSource, nSource);
        return SQLITE_NOMEM;
      }
      aWorking = aNew;
      zDup = aSource[srcIdx].zName
               ? sqlite3_mprintf("%s", aSource[srcIdx].zName) : 0;
      if( aSource[srcIdx].zName && !zDup ){
        freeSchemaEntries(aSourceSchema, nSourceSchema);
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
        freeSchemaEntries(aSourceSchema, nSourceSchema);
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

  rc = loadSchemaFromCatalog(db, cs, doltliteGetCache(db), &workingHash,
                             &aWorkSchema, &nWorkSchema);
  if( rc!=SQLITE_OK ){
    freeSchemaEntries(aSourceSchema, nSourceSchema);
    checkoutSchemaInfoClear(aSchema, nNames);
    doltliteFreeCatalog(aWorking, nWorking);
    doltliteFreeCatalog(aSource, nSource);
    return rc;
  }
  for(i=0; i<nNames; i++){
    const char *zName = (const char*)sqlite3_value_text(argv[iFirstName + i]);
    if( !zName ) continue;
    checkoutAdoptSourceIndexRoots(aWorking, nWorking,
                                  aWorkSchema, nWorkSchema,
                                  aSource, nSource,
                                  aSourceSchema, nSourceSchema, zName);
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
      rc = doltliteSetSessionStaged(db, &newWorkingHash);
    }
    if( rc==SQLITE_OK ){
      rc = doltlitePersistWorkingSet(db);
    }
  }

  freeSchemaEntries(aSourceSchema, nSourceSchema);
  freeSchemaEntries(aWorkSchema, nWorkSchema);
  checkoutSchemaInfoClear(aSchema, nNames);
  doltliteFreeCatalog(aWorking, nWorking);
  doltliteFreeCatalog(aSource, nSource);
  return rc;
}

void doltCheckoutFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  CheckoutMutationCtx m;
  BranchMutationCtx branchCreate;
  const char *zBranch;
  char *zCurrentBranch = 0;
  int isCreateAndSwitch = 0;
  int hadExplicitTxn = !db->autoCommit;
  int rc;

  if( !cs ){ doltliteVcResultError(ctx, db, doltliteVcUnavailableMessage(db)); return; }
  if( argc<1 ){ doltliteVcResultError(ctx, db, "branch name required"); return; }
  zBranch = (const char*)sqlite3_value_text(argv[0]);
  if( !zBranch ){ doltliteVcResultError(ctx, db, "branch name required"); return; }

  memset(&m, 0, sizeof(m));
  memset(&branchCreate, 0, sizeof(branchCreate));

  {
    u8 isMerging = 0;
    doltliteGetSessionMergeState(db, &isMerging, 0, 0);
    if( isMerging ){
      doltliteVcResultError(ctx, db, "unresolved merge conflicts \xe2\x80\x94 commit or abort first");
      return;
    }
  }

  if( strcmp(zBranch, "-b")==0 ){
    if( argc<2 ){ doltliteVcResultError(ctx, db, "branch name required after -b"); return; }
    if( argc>3 ){ doltliteVcResultError(ctx, db, "too many arguments"); return; }
    zBranch = (const char*)sqlite3_value_text(argv[1]);
    if( branchNameEmpty(zBranch) ){ doltliteVcResultError(ctx, db, "branch name required after -b"); return; }

    if( argc>=3 ){
      const char *zStart = (const char*)sqlite3_value_text(argv[2]);
      if( !zStart ){
        doltliteVcResultError(ctx, db, "start point not found");
        return;
      }
      rc = doltliteResolveRef(db, zStart, &branchCreate.head);
      if( rc!=SQLITE_OK ){
        doltliteVcResultError(ctx, db, "start point not found");
        return;
      }
    }else{
      doltliteGetSessionHead(db, &branchCreate.head);
      if( prollyHashIsEmpty(&branchCreate.head) ){
        doltliteVcResultError(ctx, db, "no commits yet — commit first");
        return;
      }
    }
    branchCreate.zName = zBranch;
    rc = doltliteMutateRefs(db, mutateBranchRef, &branchCreate);
    if( rc!=SQLITE_OK ){
      doltliteVcResultError(ctx, db, "branch already exists");
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
    rc = doltliteResolveRef(db, zBranch, &sourceRef);
    if( rc==SQLITE_OK ){
      rc = doltliteCheckoutTables(db, zBranch, argv, 1, argc-1);
    }else{
      rc = doltliteCheckoutTables(db, 0, argv, 0, argc);
    }
    if( rc==SQLITE_NOTFOUND ){
      char *zErr = sqlite3_mprintf("no such branch or table: %s", zBranch);
      doltliteVcResultError(ctx, db, zErr ? zErr : "no such branch or table");
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

  doltliteGetSessionHead(db, &m.oldCommitHash);
  zCurrentBranch = sqlite3_mprintf("%s", doltliteGetSessionBranch(db));
  if( !zCurrentBranch ){
    (void)doltliteVcSealSavepointError(db);
    sqlite3_result_error_nomem(ctx);
    return;
  }
  rc = checkoutCaptureOldCatalog(db, cs, &m.oldCatHash);
  if( rc!=SQLITE_OK ){
    sqlite3_free(zCurrentBranch);
    doltliteVcResultError(ctx, db, "failed to snapshot current branch state");
    return;
  }
  m.haveOldState = 1;
  checkoutSaveSession(db, &m);

  m.zTargetBranch = zBranch;
  m.zCurrentBranch = zCurrentBranch;
  rc = doltliteMutateRefs(db, checkoutMutateRefs, &m);
  if( rc!=SQLITE_OK ){
    int restoreRc = checkoutRestoreSession(db, &m);
    if( restoreRc!=SQLITE_OK ) rc = restoreRc;
    {
      int durableRc = doltliteMutateRefs(db, checkoutRestoreDurableState, &m);
      if( durableRc!=SQLITE_OK ) rc = durableRc;
    }
  }
  sqlite3_free(zCurrentBranch);
  zCurrentBranch = 0;
  if( rc==SQLITE_NOTFOUND ){
    rc = checkoutCreateFromRemoteTracking(db, zBranch);
    if( rc==SQLITE_OK ){
      memset(&m, 0, sizeof(m));
      doltliteGetSessionHead(db, &m.oldCommitHash);
      zCurrentBranch = sqlite3_mprintf("%s", doltliteGetSessionBranch(db));
      if( !zCurrentBranch ){
        (void)doltliteVcSealSavepointError(db);
        sqlite3_result_error_nomem(ctx);
        return;
      }
      rc = checkoutCaptureOldCatalog(db, cs, &m.oldCatHash);
      if( rc!=SQLITE_OK ){
        sqlite3_free(zCurrentBranch);
        doltliteVcResultError(ctx, db, "failed to snapshot current branch state");
        return;
      }
      m.haveOldState = 1;
      checkoutSaveSession(db, &m);
      m.zTargetBranch = zBranch;
      m.zCurrentBranch = zCurrentBranch;
      rc = doltliteMutateRefs(db, checkoutMutateRefs, &m);
      if( rc!=SQLITE_OK ){
        int restoreRc = checkoutRestoreSession(db, &m);
        if( restoreRc!=SQLITE_OK ) rc = restoreRc;
        {
          int durableRc = doltliteMutateRefs(db, checkoutRestoreDurableState, &m);
          if( durableRc!=SQLITE_OK ) rc = durableRc;
        }
      }
      sqlite3_free(zCurrentBranch);
      zCurrentBranch = 0;
      goto checkout_done;
    }
    if( rc!=SQLITE_NOTFOUND ){
      doltliteVcResultError(ctx, db, "checkout failed");
      return;
    }

    rc = doltliteCheckoutTables(db, 0, argv, 0, argc);
    if( rc==SQLITE_NOTFOUND ){
      char *zErr = sqlite3_mprintf(
          "no such branch or table: %s", zBranch);
      doltliteVcResultError(ctx, db, zErr ? zErr : "no such branch or table");
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
checkout_done:
  if( rc==SQLITE_EMPTY ){
    doltliteVcResultError(ctx, db, "target branch has no commits");
    return;
  }
  if( rc==SQLITE_BUSY ){
    /* Keep SQLITE_BUSY (not ERROR) so clients and busy_timeout/retry loops
    ** treat peer lock contention as retryable, not a hard failure. */
    (void)doltliteVcSealSavepointError(db);
    sqlite3_result_error(ctx, "database is locked by another connection", -1);
    sqlite3_result_error_code(ctx, SQLITE_BUSY);
    return;
  }
  if( rc==SQLITE_NOMEM ){
    (void)doltliteVcSealSavepointError(db);
    sqlite3_result_error_nomem(ctx);
    return;
  }
  if( rc!=SQLITE_OK ){
    doltliteVcResultError(ctx, db, "checkout failed");
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

#endif
