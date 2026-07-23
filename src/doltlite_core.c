#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_hashset.h"
#include "chunk_store.h"
#include "prolly_cursor.h"
#include "prolly_cache.h"
#include "prolly_diff.h"
#include "doltlite_commit.h"
#include "doltlite_record.h"
#include "doltlite_internal.h"
#include "doltlite_name_index.h"
#include <stddef.h>
#include "doltlite_ignore.h"

#include <string.h>
#include <ctype.h>
#include <time.h>

void doltliteTxnStateClear(DoltliteTxnState *p){
  assert( p!=0 );
  sqlite3_free(p->zSessionBranch);
  memset(p, 0, sizeof(*p));
}

int doltliteSaveTxnState(sqlite3 *db, DoltliteTxnState *p){
  ChunkStore *cs;
  int rc;
  assert( db!=0 && p!=0 );
  cs = doltliteGetChunkStore(db);

  memset(p, 0, sizeof(*p));
  if( !cs ) return SQLITE_ERROR;

  memcpy(&p->refsHash, refsTableGetHash(&cs->refs), sizeof(ProllyHash));

  p->zSessionBranch = sqlite3_mprintf("%s", doltliteGetSessionBranch(db));
  if( !p->zSessionBranch ){
    doltliteTxnStateClear(p);
    return SQLITE_NOMEM;
  }
  doltliteGetSessionHead(db, &p->sessionHead);
  doltliteGetSessionStaged(db, &p->sessionStaged);
  doltliteGetSessionMergeState(db, &p->sessionIsMerging,
                               &p->sessionMergeCommit,
                               &p->sessionConflictsCatalog);
  doltliteGetSessionConstraintViolationsCatalog(
      db, &p->sessionConstraintViolationsCatalog);

  rc = doltliteFlushCatalogToHash(db, &p->sessionCatalogHash);
  if( rc!=SQLITE_OK ){
    doltliteTxnStateClear(p);
  }
  return rc;
}

int doltliteRestoreTxnState(sqlite3 *db, DoltliteTxnState *p){
  ChunkStore *cs;
  int rc;
  assert( db!=0 && p!=0 );
  assert( p->zSessionBranch!=0 );
  cs = doltliteGetChunkStore(db);

  if( !cs ) return SQLITE_ERROR;

  refsTableSetHash(&cs->refs, &p->refsHash);
  if( prollyHashIsEmpty(&p->refsHash) ){
    chunkStoreClearRefs(cs);
  }else{
    rc = chunkStoreReloadRefs(cs);
    if( rc!=SQLITE_OK ) return rc;
  }

  rc = doltliteSwitchCatalog(db, &p->sessionCatalogHash);
  if( rc!=SQLITE_OK ) return rc;

  rc = doltliteSetSessionBranch(db, p->zSessionBranch);
  if( rc!=SQLITE_OK ) return rc;
  doltliteSetSessionHead(db, &p->sessionHead);
  rc = doltliteSetSessionStaged(db, &p->sessionStaged);
  if( rc==SQLITE_OK ){
    rc = doltliteSetSessionMergeState(db, p->sessionIsMerging,
                                      &p->sessionMergeCommit,
                                      &p->sessionConflictsCatalog);
  }
  if( rc==SQLITE_OK ){
    rc = doltliteSetSessionConstraintViolationsCatalog(
        db, &p->sessionConstraintViolationsCatalog);
  }
  return rc;
}

int doltliteRestoreTxnStateOnFailure(
  sqlite3 *db,
  DoltliteTxnState *pSaved,
  int opRc
){
  int rc = doltliteRestoreTxnState(db, pSaved);
  doltliteTxnStateClear(pSaved);
  return rc==SQLITE_OK ? opRc : rc;
}

int doltliteRefreshAndConfirmHead(
  sqlite3 *db,
  ChunkStore *cs,
  const ProllyHash *pExpectedHead
){
  const char *zBranch;
  ProllyHash branchTip;
  int found = 0;
  int rc;
  assert( db!=0 && cs!=0 && pExpectedHead!=0 );

  rc = chunkStoreLockAndRefresh(cs);
  if( rc!=SQLITE_OK ) return rc;

  /* Refresh in-memory state so a subsequent advance builds on the current
  ** view. This is a no-op when we hold the lock reentrantly (a VC op already
  ** in a write transaction), so it cannot be the basis of the CAS below. */
  rc = chunkStoreForceRefresh(cs);
  if( rc!=SQLITE_OK ){
    chunkStoreUnlock(cs);
    return rc;
  }

  /* Compare against the authoritative on-disk tip, read directly rather than
  ** from the in-memory refs. The in-memory tip can be stale here: force-refresh
  ** above is suppressed under a reentrant lock, and even the lock-time heuristic
  ** can miss a peer commit when WAL reuse leaves the file size unchanged. A
  ** stale tip would let this advance clobber the peer's commit (lost update). */
  zBranch = doltliteGetSessionBranch(db);
  rc = chunkStoreReadDiskBranchTip(cs, zBranch, &branchTip, &found);
  if( rc!=SQLITE_OK ){
    chunkStoreUnlock(cs);
    return rc;
  }
  if( found && prollyHashCompare(&branchTip, pExpectedHead)!=0 ){
    chunkStoreUnlock(cs);
    return SQLITE_BUSY;
  }
  return SQLITE_OK;
}

int doltliteHasUncommittedChanges(sqlite3 *db, int *pDirty){
  ProllyHash headCatHash, stagedHash, workingCatHash;
  u8 *wCatData = 0; int nWCat = 0;
  int rc;

  if( !db || !pDirty ) return SQLITE_MISUSE;
  *pDirty = 0;

  rc = doltliteGetHeadCatalogHash(db, &headCatHash);
  if( rc!=SQLITE_OK ) return rc;
  if( prollyHashIsEmpty(&headCatHash) ){
    sqlite3_stmt *pStmt = 0;
    int stepRc;
    int finalizeRc;
    rc = sqlite3_prepare_v2(db,
      "SELECT 1 FROM sqlite_master "
      "WHERE type='table' "
      "AND name NOT LIKE 'sqlite_%' "
      "AND name NOT LIKE 'dolt_%' "
      "LIMIT 1",
      -1, &pStmt, 0);
    if( rc!=SQLITE_OK ) return rc;
    stepRc = sqlite3_step(pStmt);
    if( stepRc==SQLITE_ROW ){
      *pDirty = 1;
      rc = SQLITE_OK;
    }else if( stepRc==SQLITE_DONE ){
      rc = SQLITE_OK;
    }else{
      rc = stepRc;
    }
    finalizeRc = sqlite3_finalize(pStmt);
    return rc==SQLITE_OK ? finalizeRc : rc;
  }

  doltliteGetSessionStaged(db, &stagedHash);
  if( !prollyHashIsEmpty(&stagedHash)
   && prollyHashCompare(&headCatHash, &stagedHash)!=0 ){
    *pDirty = 1;
    return SQLITE_OK;
  }

  {
    ChunkStore *cs = doltliteGetChunkStore(db);
    if( !cs ) return SQLITE_ERROR;
    rc = doltliteFlushAndSerializeCatalog(db, &wCatData, &nWCat);
    if( rc!=SQLITE_OK ){
      sqlite3_free(wCatData);
      return rc;
    }
    rc = chunkStorePut(cs, wCatData, nWCat, &workingCatHash);
    sqlite3_free(wCatData);
    if( rc!=SQLITE_OK ) return rc;
    if( prollyHashCompare(&headCatHash, &workingCatHash)!=0 ){
      *pDirty = 1;
    }
    return SQLITE_OK;
  }
}

void doltliteUpdateSchemaHashes(sqlite3 *db){
  sqlite3_stmt *pStmt = 0;
  /* One scan of sqlite_master covers every table and index; both key their
  ** catalog entry by rootpage and canonicalize by their own name. */
  if( sqlite3_prepare_v2(
        db,
        "SELECT name, rootpage, sql "
        "FROM main.sqlite_master "
        "WHERE type IN ('table','index') AND sql IS NOT NULL",
        -1, &pStmt, 0
      )==SQLITE_OK ){
    while( sqlite3_step(pStmt)==SQLITE_ROW ){
      const char *zName = (const char*)sqlite3_column_text(pStmt, 0);
      Pgno iRoot = (Pgno)sqlite3_column_int(pStmt, 1);
      const char *zCreate = (const char*)sqlite3_column_text(pStmt, 2);
      if( zName && zCreate ){
        ProllyHash h;
        char *zCanon = doltliteCanonicalizeSchemaSql(zCreate, zName);
        if( zCanon ){
          prollyHashCompute(zCanon, (int)strlen(zCanon), &h);
          sqlite3_free(zCanon);
          doltliteSetTableSchemaHash(db, iRoot, &h);
        }
      }
    }
    sqlite3_finalize(pStmt);
  }
}

int doltliteLoadLiveSchemaSql(
  sqlite3 *db,
  const char *zType,
  const char *zDb,
  const char *zName,
  const char *zTblName,
  char **pzSql
){
  sqlite3_stmt *pStmt = 0;
  char *zQuery = 0;
  int rc;

  *pzSql = 0;
  if( !db || !zType || !zName ) return SQLITE_OK;

  if( zTblName && zTblName[0] ){
    zQuery = sqlite3_mprintf(
      "SELECT sql FROM \"%w\".sqlite_master "
      "WHERE type=%Q AND name=%Q AND tbl_name=%Q",
      zDb ? zDb : "main", zType, zName, zTblName
    );
  }else{
    zQuery = sqlite3_mprintf(
      "SELECT sql FROM \"%w\".sqlite_master "
      "WHERE type=%Q AND name=%Q",
      zDb ? zDb : "main", zType, zName
    );
  }
  if( !zQuery ) return SQLITE_NOMEM;

  rc = sqlite3_prepare_v2(db, zQuery, -1, &pStmt, 0);
  sqlite3_free(zQuery);
  if( rc!=SQLITE_OK ) return rc;

  rc = sqlite3_step(pStmt);
  if( rc==SQLITE_ROW ){
    const char *zSql = (const char*)sqlite3_column_text(pStmt, 0);
    if( zSql ){
      *pzSql = sqlite3_mprintf("%s", zSql);
      if( !*pzSql ){
        sqlite3_finalize(pStmt);
        return SQLITE_NOMEM;
      }
    }
    rc = SQLITE_OK;
  }else if( rc==SQLITE_DONE ){
    rc = SQLITE_OK;
  }
  sqlite3_finalize(pStmt);
  return rc;
}

int doltliteMutateRefs(sqlite3 *db, DoltliteRefsMutation xMutate, void *pArg){
  ChunkStore *cs = doltliteGetChunkStore(db);
  int rc;

  if( !cs ) return SQLITE_ERROR;

  rc = chunkStoreLockAndRefresh(cs);
  if( rc!=SQLITE_OK ) return rc;

  /* xMutate edits the in-memory refs and serializeRefs rewrites the whole refs
  ** blob, so a stale view would drop a peer's concurrent ref change (e.g. a
  ** branch delete clobbering main's just-merged advance). Reload persisted
  ** refs first rather than trusting the lock-time change heuristic. */
  rc = chunkStoreForceRefresh(cs);
  if( rc!=SQLITE_OK ){
    chunkStoreUnlock(cs);
    return rc;
  }

  rc = xMutate(db, cs, pArg);
  if( rc==SQLITE_OK ){
    rc = chunkStoreSerializeRefs(cs);
    if( rc==SQLITE_OK ) rc = chunkStoreCommit(cs);
  }

  chunkStoreUnlock(cs);
  return rc;
}

int doltliteFlushCatalogToHash(sqlite3 *db, ProllyHash *pHash){
  ChunkStore *cs = doltliteGetChunkStore(db);
  u8 *catData = 0;
  int nCatData = 0;
  int rc;
  rc = doltliteFlushAndSerializeCatalog(db, &catData, &nCatData);
  if( rc!=SQLITE_OK ) return rc;
  rc = chunkStorePut(cs, catData, nCatData, pHash);
  sqlite3_free(catData);
  return rc;
}

int doltlitePrepareCatalogForPersistence(sqlite3 *db){
  UNUSED_PARAMETER(db);
  return SQLITE_OK;
}

void freeSchemaMergeActions(SchemaMergeAction *a, int n){
  int i, j;
  for(i=0; i<n; i++){
    for(j=0; j<a[i].nAddColumns; j++){
      sqlite3_free(a[i].azAddColumns[j]);
    }
    sqlite3_free(a[i].azAddColumns);
    sqlite3_free(a[i].zTableName);
  }
  sqlite3_free(a);
}

int doltliteCreateAndStoreCommitWithTime(
  sqlite3 *db,
  const ProllyHash *pParent,
  const ProllyHash *pCatalog,
  const char *zMessage,
  const char *zAuthorName,
  const char *zAuthorEmail,
  const ProllyHash *aExtraParents,
  int nExtraParents,
  i64 explicitTimestamp,
  ProllyHash *pCommitHash
);

int doltliteCreateAndStoreCommit(
  sqlite3 *db,
  const ProllyHash *pParent,
  const ProllyHash *pCatalog,
  const char *zMessage,
  const char *zAuthorName,
  const char *zAuthorEmail,
  const ProllyHash *aExtraParents,
  int nExtraParents,
  ProllyHash *pCommitHash
){
  return doltliteCreateAndStoreCommitWithTime(db, pParent, pCatalog, zMessage,
      zAuthorName, zAuthorEmail, aExtraParents, nExtraParents, 0, pCommitHash);
}

int doltliteCreateAndStoreCommitWithTime(
  sqlite3 *db,
  const ProllyHash *pParent,
  const ProllyHash *pCatalog,
  const char *zMessage,
  const char *zAuthorName,
  const char *zAuthorEmail,
  const ProllyHash *aExtraParents,
  int nExtraParents,
  i64 explicitTimestamp,
  ProllyHash *pCommitHash
){
  ChunkStore *cs;
  DoltliteCommit c;
  u8 *commitData = 0;
  int nCommitData = 0;
  int rc, i;
  assert( db!=0 && pParent!=0 && pCatalog!=0 && pCommitHash!=0 );
  assert( nExtraParents>=0 );
  assert( nExtraParents==0 || aExtraParents!=0 );
  cs = doltliteGetChunkStore(db);
  assert( cs!=0 );

  memset(&c, 0, sizeof(c));
  memcpy(&c.parentHash, pParent, sizeof(ProllyHash));
  memcpy(&c.catalogHash, pCatalog, sizeof(ProllyHash));
  c.timestamp = explicitTimestamp ? explicitTimestamp : (i64)time(0);
  c.zName  = sqlite3_mprintf("%s", zAuthorName  ? zAuthorName  : doltliteGetAuthorName(db));
  if( c.zName==0 ){
    rc = SQLITE_NOMEM;
    goto create_commit_done;
  }
  c.zEmail = sqlite3_mprintf("%s", zAuthorEmail ? zAuthorEmail : doltliteGetAuthorEmail(db));
  if( c.zEmail==0 ){
    rc = SQLITE_NOMEM;
    goto create_commit_done;
  }
  c.zMessage = sqlite3_mprintf("%s", zMessage ? zMessage : "");
  if( c.zMessage==0 ){
    rc = SQLITE_NOMEM;
    goto create_commit_done;
  }

  if( nExtraParents > 0 && aExtraParents ){
    c.aParents[0] = *pParent;
    for(i=0; i<nExtraParents && (i+1)<DOLTLITE_MAX_PARENTS; i++){
      c.aParents[i+1] = aExtraParents[i];
    }
    c.nParents = 1 + (nExtraParents < DOLTLITE_MAX_PARENTS-1
                       ? nExtraParents : DOLTLITE_MAX_PARENTS-1);
  }

  rc = doltliteCommitSerialize(&c, &commitData, &nCommitData);
  if( rc==SQLITE_OK ) rc = chunkStorePut(cs, commitData, nCommitData, pCommitHash);
create_commit_done:
  sqlite3_free(commitData);
  doltliteCommitClear(&c);
  return rc;
}

int doltliteAdvanceBranch(
  sqlite3 *db,
  const ProllyHash *pNewHead,
  const ProllyHash *pCatalogHash,
  const ProllyHash *pWorkingCatHash
){
  ChunkStore *cs;
  const char *branch;
  DoltliteTxnState saved;
  int rc;
  assert( db!=0 && pNewHead!=0 && pCatalogHash!=0 );
  cs = doltliteGetChunkStore(db);
  assert( cs!=0 );
  branch = doltliteGetSessionBranch(db);
  assert( branch!=0 && branch[0]!=0 );

  rc = doltliteSaveTxnState(db, &saved);
  if( rc!=SQLITE_OK ) return rc;

  if( refsTableBranchCount(&cs->refs)==0 ){
    rc = chunkStoreAddBranch(cs, branch, pNewHead);
    if( rc==SQLITE_OK ){
      rc = chunkStoreSetDefaultBranch(cs, branch);
    }
  }else{
    rc = chunkStoreUpdateBranch(cs, branch, pNewHead);
  }
  if( rc!=SQLITE_OK ){
    return doltliteRestoreTxnStateOnFailure(db, &saved, rc);
  }

  doltliteSetSessionHead(db, pNewHead);
  rc = doltliteSetSessionStaged(db, pCatalogHash);
  if( rc!=SQLITE_OK ){
    return doltliteRestoreTxnStateOnFailure(db, &saved, rc);
  }
  if( pWorkingCatHash && !prollyHashIsEmpty(pWorkingCatHash) ){
    rc = doltliteSwitchCatalog(db, pWorkingCatHash);
  }else{
    rc = doltliteSwitchCatalog(db, pCatalogHash);
  }
  if( rc!=SQLITE_OK ){
    return doltliteRestoreTxnStateOnFailure(db, &saved, rc);
  }

  rc = doltlitePersistWorkingSetWithHash(db, pWorkingCatHash);
  if( rc!=SQLITE_OK ){
    return doltliteRestoreTxnStateOnFailure(db, &saved, rc);
  }

  doltliteTxnStateClear(&saved);
  return SQLITE_OK;
}

int doltlitePersistOrSaveWorkingSet(sqlite3 *db){
  if( doltliteVcTxnMode(db)==DOLTLITE_VC_TXN_AUTOCOMMIT_LIKE ){
    return doltlitePersistWorkingSet(db);
  }
  return doltliteSaveWorkingSet(db);
}

int doltliteReportConflicts(
  sqlite3 *db,
  sqlite3_context *ctx,
  int nConflicts,
  const char *zOp
){
  char msg[256];
  int rc;
  rc = doltliteRegisterConflictTables(db);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltlitePersistOrSaveWorkingSet(db);
  if( rc!=SQLITE_OK ) return rc;
  sqlite3_snprintf(sizeof(msg), msg,
    "%s has %d conflict(s). Resolve and then commit with dolt_commit.",
    zOp, nConflicts);
  sqlite3_result_error(ctx, msg, -1);
  return SQLITE_OK;
}

int doltliteReportConstraintViolations(
  sqlite3 *db,
  sqlite3_context *ctx,
  const char *zOp
){
  char msg[256];
  int rc;
  rc = doltlitePersistOrSaveWorkingSet(db);
  if( rc!=SQLITE_OK ) return rc;
  sqlite3_snprintf(sizeof(msg), msg,
    "%s resulted in constraint violations. Resolve the rows in "
    "dolt_constraint_violations and then commit with dolt_commit.",
    zOp);
  sqlite3_result_error(ctx, msg, -1);
  return SQLITE_OK;
}

int doltliteDetectPostMergeConstraintViolations(
  sqlite3 *db,
  const ProllyHash *pAncCatHash,
  int *pnViolations
){
  int nViolations = 0;
  int nUnique = 0;
  int nCheck = 0;
  char *zDetectErrMsg = 0;
  int rc;
  sqlite3_stmt *pStmt = 0;
  int needsDetection = 0;

  rc = sqlite3_prepare_v2(db,
      "SELECT 1 "
      "FROM main.sqlite_master "
      "WHERE (type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'dolt_%' "
      "       AND sql IS NOT NULL "
      "       AND (instr(upper(sql), 'REFERENCES')>0 "
      "            OR instr(upper(sql), 'CHECK')>0 "
      "            OR instr(upper(sql), 'UNIQUE')>0)) "
      "   OR (type='index' AND sql IS NOT NULL "
      "       AND instr(upper(sql), 'CREATE UNIQUE INDEX')>0) "
      "LIMIT 1",
      -1, &pStmt, 0);
  if( rc!=SQLITE_OK ) return rc;
  rc = sqlite3_step(pStmt);
  if( rc==SQLITE_ROW ){
    needsDetection = 1;
    rc = SQLITE_OK;
  }else if( rc==SQLITE_DONE ){
    rc = SQLITE_OK;
  }
  sqlite3_finalize(pStmt);
  if( rc!=SQLITE_OK ) return rc;
  if( !needsDetection ){
    if( pnViolations ) *pnViolations = 0;
    return SQLITE_OK;
  }

  rc = doltliteConstraintViolationBatchBegin(db);
  if( rc==SQLITE_OK ){
    rc = doltliteDetectMergeFkViolations(db, pAncCatHash,
                                         &zDetectErrMsg, &nViolations);
  }
  if( rc==SQLITE_OK ){
    rc = doltliteDetectMergeUniqueViolations(db, pAncCatHash,
                                             &zDetectErrMsg, &nUnique);
  }
  if( rc==SQLITE_OK ){
    rc = doltliteDetectMergeCheckViolations(db, pAncCatHash,
                                            &zDetectErrMsg, &nCheck);
  }
  {
    int erc = doltliteConstraintViolationBatchEnd(db, rc==SQLITE_OK);
    if( rc==SQLITE_OK ) rc = erc;
  }
  sqlite3_free(zDetectErrMsg);
  if( rc!=SQLITE_OK ) return rc;

  if( pnViolations ) *pnViolations = nViolations + nUnique + nCheck;
  return SQLITE_OK;
}

int doltliteSavepointIsTopLevelTxn(sqlite3 *db){
  assert( db!=0 );
  assert( db->nSavepoint>=0 );
  return db->pSavepoint!=0 && db->nSavepoint==0;
}

/* SQLite represents a top-level SAVEPOINT as the transaction boundary.
** Doltlite treats that like autocommit for VC operations: seal the boundary
** instead of leaving a savepoint that later ROLLBACK TO can undo. */
int doltliteVcSealTopLevelSavepointTxn(sqlite3 *db){
  if( doltliteSavepointIsTopLevelTxn(db) ){
    return sqlite3_exec(db, "COMMIT", 0, 0, 0);
  }
  return SQLITE_OK;
}

DoltliteVcTxnMode doltliteVcTxnMode(sqlite3 *db){
  if( db->autoCommit || doltliteSavepointIsTopLevelTxn(db) ){
    return DOLTLITE_VC_TXN_AUTOCOMMIT_LIKE;
  }
  if( db->pSavepoint ){
    return DOLTLITE_VC_TXN_NESTED_SAVEPOINT;
  }
  return DOLTLITE_VC_TXN_PLAIN;
}

int doltliteVcSealActiveSavepoints(sqlite3 *db){
  int rc = SQLITE_OK;
  while( rc==SQLITE_OK && db->pSavepoint ){
    char *zSql = sqlite3_mprintf("RELEASE SAVEPOINT \"%w\"", db->pSavepoint->zName);
    if( !zSql ) return SQLITE_NOMEM;
    rc = sqlite3_exec(db, zSql, 0, 0, 0);
    sqlite3_free(zSql);
  }
  return rc;
}

int doltliteVcSealSavepointError(sqlite3 *db){
  if( db->pSavepoint ){
    return doltliteVcSealActiveSavepoints(db);
  }
  return SQLITE_OK;
}

void doltliteVcResultError(sqlite3_context *ctx, sqlite3 *db, const char *zMsg){
  (void)doltliteVcSealSavepointError(db);
  sqlite3_result_error(ctx, zMsg, -1);
}

int doltliteVcSealBranchStyleTxn(sqlite3 *db){
  int rc;
  if( db->autoCommit ) return SQLITE_OK;
  if( db->pSavepoint ){
    return doltliteVcSealActiveSavepoints(db);
  }
  rc = sqlite3_exec(db, "COMMIT", 0, 0, 0);
  if( rc!=SQLITE_OK ) return rc;
  return sqlite3_exec(db, "BEGIN", 0, 0, 0);
}

int doltliteVcSealBranchStyleTxnMaybeKeepTopLevelSavepoint(sqlite3 *db){
  if( doltliteSavepointIsTopLevelTxn(db) ){
    return SQLITE_OK;
  }
  return doltliteVcSealBranchStyleTxn(db);
}

int doltlitePrimeSchemaCache(sqlite3 *db){
  sqlite3_stmt *pStmt = 0;
  int rc = sqlite3_prepare_v2(
      db, "SELECT name FROM main.sqlite_master LIMIT 1", -1, &pStmt, 0);
  if( rc!=SQLITE_OK ) return rc;
  while( (rc = sqlite3_step(pStmt))==SQLITE_ROW ){}
  if( rc==SQLITE_DONE ) rc = SQLITE_OK;
  sqlite3_finalize(pStmt);
  return rc;
}

void doltliteReportAutocommitConflictRollback(sqlite3_context *ctx){
  sqlite3_result_error(ctx,
    "Merge conflict detected, @autocommit transaction rolled back. "
    "@autocommit must be disabled so that merge conflicts can be "
    "resolved using the dolt_conflicts and dolt_schema_conflicts "
    "tables before manually committing the transaction. "
    "Alternatively, to commit transactions with merge conflicts, set "
    "@@dolt_allow_commit_conflicts = 1",
    -1);
}

int doltliteRollbackAutocommitConflict(
  sqlite3 *db,
  sqlite3_context *ctx,
  DoltliteTxnState *pSaved
){
  int rc;
  int hadTopLevelSavepoint = db->pSavepoint!=0 && db->nSavepoint==0;
  sqlite3RollbackAll(db, SQLITE_OK);
  rc = doltliteRestoreTxnState(db, pSaved);
  if( rc==SQLITE_OK ){
    rc = doltliteSetSessionMergeState(db, pSaved->sessionIsMerging,
                                      &pSaved->sessionMergeCommit,
                                      &pSaved->sessionConflictsCatalog);
  }
  if( rc==SQLITE_OK ){
    rc = doltliteSetSessionConstraintViolationsCatalog(
        db, &pSaved->sessionConstraintViolationsCatalog);
  }
  doltliteTxnStateClear(pSaved);
  if( rc==SQLITE_OK && hadTopLevelSavepoint ){
    rc = doltliteVcSealTopLevelSavepointTxn(db);
  }
  if( rc==SQLITE_OK ){
    doltliteReportAutocommitConflictRollback(ctx);
  }
  return rc;
}


#endif
