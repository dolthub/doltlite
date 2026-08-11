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

int mergeAbortInPlace(sqlite3 *db){
  ProllyHash headCatHash;
  int rc = doltliteGetHeadCatalogHash(db, &headCatHash);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteHardReset(db, &headCatHash);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteSetSessionStaged(db, &headCatHash);
  if( rc==SQLITE_OK ) rc = doltliteClearSessionMergeState(db);
  if( rc==SQLITE_OK ){
    extern int doltliteClearAllConstraintViolations(sqlite3*);
    if( doltliteSessionHasConstraintViolations(db) ){
      rc = doltliteClearAllConstraintViolations(db);
    }
  }
  if( rc==SQLITE_OK ) rc = doltlitePersistWorkingSet(db);
  if( rc!=SQLITE_OK ) return rc;
  return doltliteVcSealActiveSavepoints(db);
}

int mergeFastForward(
  sqlite3 *db,
  sqlite3_context *context,
  ChunkStore *cs,
  const ProllyHash *pOurHead,
  const ProllyHash *pTheirHead
){
  DoltliteCommit theirCommit;
  DoltliteTxnState savedState;
  int rc;
  char hx[PROLLY_HASH_SIZE*2+1];

  assert( db!=0 && cs!=0 && pOurHead!=0 && pTheirHead!=0 );
  memset(&theirCommit, 0, sizeof(theirCommit));
  memset(&savedState, 0, sizeof(savedState));

  rc = doltliteLoadCommit(db, pTheirHead, &theirCommit);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "failed to load commit", -1);
    return rc;
  }
  rc = doltliteSaveTxnState(db, &savedState);
  if( rc!=SQLITE_OK ){
    doltliteCommitClear(&theirCommit);
    sqlite3_result_error_code(context, rc);
    return rc;
  }
  rc = doltliteSwitchCatalog(db, &theirCommit.catalogHash);
  if( rc==SQLITE_OK ){
    rc = doltliteUpdateBranchWorkingState(db, doltliteGetSessionBranch(db),
                                          &theirCommit.catalogHash, NULL);
  }
  if( rc==SQLITE_OK ){
    rc = doltliteCompareAndAdvanceBranch(
        db, pOurHead, pTheirHead, &theirCommit.catalogHash, 0);
  }
  if( rc!=SQLITE_OK ){
    doltliteCommitClear(&theirCommit);
    if( rc==SQLITE_BUSY ) doltliteCmdResultPeerBranchBusy(context, "merge");
    sqlite3_result_error_code(context,
        doltliteRestoreTxnStateOnFailure(db, &savedState, rc));
    return rc;
  }
  rc = doltliteVcSealEnclosingTxn(db);
  if( rc!=SQLITE_OK ){
    doltliteCommitClear(&theirCommit);
    sqlite3_result_error_code(context, rc);
    return rc;
  }
  doltliteTxnStateClear(&savedState);
  doltliteCommitClear(&theirCommit);
  doltliteHashToHex(pTheirHead, hx);
  sqlite3_result_text(context, hx, -1, SQLITE_TRANSIENT);
  return SQLITE_OK;
}

static int doltliteApplyMergeSchemaActions(
  sqlite3 *db,
  const ProllyHash *pAncCatHash,
  const ProllyHash *pTheirCatHash,
  SchemaMergeAction *aSchemaActions,
  int nSchemaActions,
  ProllyHash *pMergedCatHash
){
  int rc = SQLITE_OK;
  int si;

  for(si=0; si<nSchemaActions && rc==SQLITE_OK; si++){
    int sj;
    for(sj=0; sj<aSchemaActions[si].nAddColumns; sj++){
      char *zAlter = sqlite3_mprintf("ALTER TABLE \"%w\" ADD COLUMN %s",
                                      aSchemaActions[si].zTableName,
                                      aSchemaActions[si].azAddColumns[sj]);
      if( !zAlter ) return SQLITE_NOMEM;
      rc = sqlite3_exec(db, zAlter, 0, 0, 0);
      sqlite3_free(zAlter);
      if( rc!=SQLITE_OK ) break;
    }
  }

  /* Row data (theirs' adds, edits to shared columns, deletes, and added-column
  ** values) is merged three-way in doltliteMergeCatalogs against a normalized
  ** theirs root, so only the schema evolution (the ALTERs above) happens here. */
  if( rc==SQLITE_OK ){
    rc = doltliteFlushCatalogToHash(db, pMergedCatHash);
  }
  return rc;
}

/* Load ours/theirs/ancestor commit catalogs for a three-way merge. */
static int mergeRefLoadCatalogs(
  sqlite3 *db,
  const ProllyHash *pOurHead,
  const ProllyHash *pTheirHead,
  const ProllyHash *pAncestorHash,
  DoltliteCommit *pOurCommit,
  DoltliteCommit *pTheirCommit,
  ProllyHash *pOurCat,
  ProllyHash *pTheirCat,
  ProllyHash *pAncCat,
  const char **pzFail
){
  DoltliteCommit ancCommit;
  int rc;

  memset(&ancCommit, 0, sizeof(ancCommit));
  *pzFail = 0;
  rc = doltliteLoadCommit(db, pOurHead, pOurCommit);
  if( rc!=SQLITE_OK ){ *pzFail = "failed to load our commit"; return rc; }
  *pOurCat = pOurCommit->catalogHash;

  rc = doltliteLoadCommit(db, pTheirHead, pTheirCommit);
  if( rc!=SQLITE_OK ){ *pzFail = "failed to load their commit"; return rc; }
  *pTheirCat = pTheirCommit->catalogHash;

  rc = doltliteLoadCommit(db, pAncestorHash, &ancCommit);
  if( rc!=SQLITE_OK ){ *pzFail = "failed to load ancestor"; return rc; }
  *pAncCat = ancCommit.catalogHash;
  doltliteCommitClear(&ancCommit);
  return SQLITE_OK;
}

/* After catalogs are merged: reindex, schema actions, flush/stage roots. */
static int mergeRefInstallMergedCatalog(
  sqlite3 *db,
  const ProllyHash *pAncCat,
  const ProllyHash *pTheirCat,
  ProllyHash *pMergedCat,
  int nMergeConflicts,
  SchemaMergeAction **paSchemaActions,
  int *pnSchemaActions,
  char ***pazReindex,
  int *pnReindex,
  char ***pazRebuildVtabs,
  int *pnRebuildVtabs
){
  int rc;

  /* Indexes adopted from the other branch carry only that branch's
  ** rows; rebuild them over the merged tables while the merged catalog
  ** is live so the flush below captures correct roots. */
  if( *pnReindex>0 ){
    rc = doltliteReindexNamedIndexes(db, *pazReindex, *pnReindex);
  }else{
    rc = SQLITE_OK;
  }
  doltliteFreeNameList(*pazReindex, *pnReindex);
  *pazReindex = 0;
  *pnReindex = 0;
  if( rc!=SQLITE_OK ){
    freeSchemaMergeActions(*paSchemaActions, *pnSchemaActions);
    *paSchemaActions = 0;
    *pnSchemaActions = 0;
    return rc;
  }

  if( *pnSchemaActions > 0 ){
    rc = doltliteApplyMergeSchemaActions(db, pAncCat, pTheirCat,
                                         *paSchemaActions, *pnSchemaActions,
                                         pMergedCat);
  }
  freeSchemaMergeActions(*paSchemaActions, *pnSchemaActions);
  *paSchemaActions = 0;
  *pnSchemaActions = 0;
  if( rc!=SQLITE_OK ) return rc;

  /* Derived vtab shadows whose conflicts were absorbed by the catalog
  ** merge: rebuild each owner from its merged %_base and stored model
  ** while the merged catalog is live, so the flush below captures the
  ** rebuilt shadow roots. The list is only ever populated on merges the
  ** filter proved otherwise conflict-free. */
  if( *pnRebuildVtabs>0 && nMergeConflicts==0 ){
    int ri;
    for(ri=0; ri<*pnRebuildVtabs && rc==SQLITE_OK; ri++){
      char *zSql = sqlite3_mprintf(
          "INSERT INTO \"%w\"(cmd, arg) VALUES('rebuild',"
          " (SELECT val FROM \"%w_model\" WHERE id=1))",
          (*pazRebuildVtabs)[ri], (*pazRebuildVtabs)[ri]);
      if( !zSql ){
        rc = SQLITE_NOMEM;
        break;
      }
      rc = sqlite3_exec(db, zSql, 0, 0, 0);
      sqlite3_free(zSql);
    }
  }
  doltliteFreeNameList(*pazRebuildVtabs, *pnRebuildVtabs);
  *pazRebuildVtabs = 0;
  *pnRebuildVtabs = 0;
  if( rc!=SQLITE_OK ) return rc;

  /* Regenerate stats after a clean merge; stat rows are derived data. */
  if( nMergeConflicts==0 ){
    sqlite3_stmt *pProbe = 0;
    int hasStat1 = 0;
    if( sqlite3_prepare_v2(db,
        "SELECT 1 FROM main.sqlite_master "
        "WHERE type='table' AND name='sqlite_stat1' LIMIT 1",
        -1, &pProbe, 0)==SQLITE_OK ){
      if( sqlite3_step(pProbe)==SQLITE_ROW ) hasStat1 = 1;
      sqlite3_finalize(pProbe);
    }
    if( hasStat1 ){
      (void)sqlite3_exec(db, "ANALYZE", 0, 0, 0);
    }
  }

  rc = doltliteFlushCatalogToHash(db, pMergedCat);
  if( rc==SQLITE_OK ) rc = doltliteSwitchCatalog(db, pMergedCat);
  if( rc==SQLITE_OK ) rc = doltlitePrimeSchemaCache(db);
  if( rc==SQLITE_OK ) rc = doltliteSetSessionStaged(db, pMergedCat);
  if( rc==SQLITE_OK ){
    rc = doltliteUpdateBranchWorkingState(db,
        doltliteGetSessionBranch(db), pMergedCat, NULL);
  }
  return rc;
}

/* Run post-merge constraint detectors. On SQLITE_OK, *pnViolations is the
** total CV count; *pzErr may own a detector error string. */
static int mergeRefDetectConstraintViolations(
  sqlite3 *db,
  const ProllyHash *pAncCat,
  int *pnViolations,
  char **pzErr
){
  int nFk = 0, nUnique = 0, nCheck = 0, nNotNull = 0, nStrict = 0;
  int vrc;
  int erc;

  *pnViolations = 0;
  *pzErr = 0;
  vrc = doltliteConstraintViolationBatchBegin(db);
  if( vrc==SQLITE_OK ){
    vrc = doltliteDetectMergeFkViolations(db, pAncCat, pzErr, &nFk, 0, 0);
  }
  if( vrc==SQLITE_OK ){
    vrc = doltliteDetectMergeUniqueViolations(db, pAncCat, pzErr, &nUnique, 0, 0);
  }
  if( vrc==SQLITE_OK ){
    vrc = doltliteDetectMergeCheckViolations(db, pAncCat, pzErr, &nCheck, 0, 0);
  }
  if( vrc==SQLITE_OK ){
    vrc = doltliteDetectMergeNotNullViolations(db, pAncCat, pzErr, &nNotNull,
                                              0, 0);
  }
  if( vrc==SQLITE_OK ){
    vrc = doltliteDetectMergeStrictViolations(db, pAncCat, pzErr, &nStrict,
                                             0, 0);
  }
  erc = doltliteConstraintViolationBatchEnd(db, vrc==SQLITE_OK);
  if( vrc==SQLITE_OK ) vrc = erc;
  if( vrc==SQLITE_OK ){
    *pnViolations = nFk + nUnique + nCheck + nNotNull + nStrict;
  }
  return vrc;
}

static int mergeRefCreateMergeCommit(
  sqlite3 *db,
  sqlite3_context *context,
  DoltliteTxnState *pSaved,
  const ProllyHash *pOurHead,
  const ProllyHash *pTheirHead,
  const ProllyHash *pMergedCat,
  const char *zBranch,
  const char *zMessage
){
  ProllyHash commitHash;
  char hexBuf[PROLLY_HASH_SIZE*2+1];
  char msg[256];
  int rc;

  rc = doltliteSetSessionStaged(db, pMergedCat);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context,
        doltliteRestoreTxnStateOnFailure(db, pSaved, rc));
    return SQLITE_ERROR;
  }

  if( zMessage && zMessage[0] ){
    sqlite3_snprintf(sizeof(msg), msg, "%s", zMessage);
  }else{
    snprintf(msg, sizeof(msg), "Merge branch '%s' into %s",
             zBranch, doltliteGetSessionBranch(db));
  }
  rc = doltliteCreateAndStoreCommit(db, pOurHead, pMergedCat,
      msg, NULL, NULL, pTheirHead, 1, &commitHash);
  if( rc!=SQLITE_OK ){
    /* The merged catalog is already live and staged; leaving it in place
    ** lets a retry with plain dolt_commit record the merged data as a
    ** single-parent commit, silently dropping theirHead from ancestry. */
    (void)doltliteRestoreTxnStateOnFailure(db, pSaved, rc);
    sqlite3_result_error(context, "failed to create merge commit", -1);
    return SQLITE_ERROR;
  }

  doltliteTestCrashFinalize("merge");
  rc = doltliteCompareAndAdvanceBranch(
      db, pOurHead, &commitHash, pMergedCat, 0);
  if( rc==SQLITE_BUSY ){
    doltliteCmdResultPeerBranchBusy(context, "merge");
    doltliteRestoreTxnStateOnFailure(db, pSaved, rc);
    return SQLITE_ERROR;
  }
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context,
        doltliteRestoreTxnStateOnFailure(db, pSaved, rc));
    return SQLITE_ERROR;
  }
  rc = doltliteVcSealEnclosingTxn(db);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    return SQLITE_ERROR;
  }
  doltliteTxnStateClear(pSaved);
  doltliteHashToHex(&commitHash, hexBuf);
  sqlite3_result_text(context, hexBuf, -1, SQLITE_TRANSIENT);
  return SQLITE_OK;
}

int doltliteMergeRef(
  sqlite3 *db,
  sqlite3_context *context,
  const char *zBranch,
  const char *zMessage,
  int noFastForward
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHash ourHead, theirHead, ancestorHash;
  ProllyHash ourCatHash, theirCatHash, ancCatHash, mergedCatHash;
  DoltliteTxnState savedState;
  int nMergeConflicts = 0;
  DoltliteCommit ourCommit, theirCommit;
  int graphLocked = 0;
  int dirty = 0;
  int rc;
  int bHaveSaved = 0;
  int bPeerBusy = 0;
  int bRestoreOnFail = 0;
  const char *zFail = 0;
  char *zOwnedErr = 0;
  SchemaMergeAction *aSchemaActions = 0;
  int nSchemaActions = 0;
  char **azReindex = 0;
  int nReindex = 0;
  char **azRebuildVtabs = 0;
  int nRebuildVtabs = 0;
  int nViolations = 0;

  memset(&ourCommit, 0, sizeof(ourCommit));
  memset(&theirCommit, 0, sizeof(theirCommit));
  memset(&savedState, 0, sizeof(savedState));
  memset(&mergedCatHash, 0, sizeof(mergedCatHash));

  if( !cs ){
    sqlite3_result_error(context, doltliteVcUnavailableMessage(db), -1);
    return SQLITE_ERROR;
  }
  if( !zBranch ){
    sqlite3_result_error(context, "branch name required", -1);
    return SQLITE_ERROR;
  }

  rc = doltliteEnsureWriteTxnAndSavepoints(db);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    return SQLITE_ERROR;
  }

  doltliteGetSessionHead(db, &ourHead);
  if( prollyHashIsEmpty(&ourHead) ){
    sqlite3_result_error(context, "no commits on current branch", -1);
    return SQLITE_ERROR;
  }

  rc = doltliteResolveRef(db, zBranch, &theirHead);
  if( rc!=SQLITE_OK || prollyHashIsEmpty(&theirHead) ){
    sqlite3_result_error(context, "merge source not found", -1);
    return SQLITE_ERROR;
  }

  if( prollyHashCompare(&ourHead, &theirHead)==0 ){
    sqlite3_result_text(context, "Already up to date", -1, SQLITE_STATIC);
    return SQLITE_OK;
  }

  rc = doltliteHasUncommittedChanges(db, &dirty);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    return SQLITE_ERROR;
  }
  if( dirty ){
    sqlite3_result_error(context,
      "uncommitted changes \xe2\x80\x94 commit or reset before merging", -1);
    return SQLITE_ERROR;
  }

  rc = doltliteFindAncestor(db, &ourHead, &theirHead, &ancestorHash);
  if( rc!=SQLITE_OK || prollyHashIsEmpty(&ancestorHash) ){
    sqlite3_result_error(context, "no common ancestor found", -1);
    return SQLITE_ERROR;
  }

  if( prollyHashCompare(&ancestorHash, &theirHead)==0 ){
    sqlite3_result_text(context, "Already up to date", -1, SQLITE_STATIC);
    return SQLITE_OK;
  }

  if( prollyHashCompare(&ancestorHash, &ourHead)==0 && !noFastForward ){
    return mergeFastForward(db, context, cs, &ourHead, &theirHead);
  }

  rc = mergeRefLoadCatalogs(db, &ourHead, &theirHead, &ancestorHash,
                            &ourCommit, &theirCommit,
                            &ourCatHash, &theirCatHash, &ancCatHash, &zFail);
  if( rc!=SQLITE_OK ) goto merge_fail;

  rc = doltliteSaveTxnState(db, &savedState);
  if( rc!=SQLITE_OK ) goto merge_fail;
  bHaveSaved = 1;

  rc = doltliteMergeCatalogs(db, &ancCatHash, &ourCatHash, &theirCatHash,
                              &mergedCatHash, &nMergeConflicts, &zOwnedErr,
                              &aSchemaActions, &nSchemaActions, 0,
                              &azReindex, &nReindex,
                              &azRebuildVtabs, &nRebuildVtabs);
  if( rc!=SQLITE_OK ){
    if( !zOwnedErr ) zFail = "merge failed";
    /* Catalog merge never mutated the live catalog; drop the snapshot. */
    goto merge_fail;
  }
  sqlite3_free(zOwnedErr);
  zOwnedErr = 0;

  if( nMergeConflicts>0 ){
    ProllyHash conflictsHash;
    doltliteGetSessionConflictsCatalog(db, &conflictsHash);
    /* From here the session is marked as merging, so every later exit has to
    ** restore the saved state rather than discard it. Discarding leaves
    ** isMerging set with a conflicts catalog while the caller is told the merge
    ** did not happen, and the next merge then refuses because one is already in
    ** progress. */
    bRestoreOnFail = 1;
    rc = doltliteSetSessionMergeState(db, 1, &theirHead, &conflictsHash);
    /* Caching the spec only sharpens dolt_merge_status.source, which falls
    ** back to the branch at theirHead, so losing it must not fail the merge. */
    (void)doltliteSetSessionMergeSourceSpec(db, zBranch, &theirHead);
    if( rc!=SQLITE_OK ) goto merge_fail;
  }

  rc = doltliteRefreshAndConfirmHead(db, cs, &ourHead);
  if( rc==SQLITE_BUSY ){
    bPeerBusy = 1;
    goto merge_fail;
  }
  if( rc!=SQLITE_OK ) goto merge_fail;
  graphLocked = 1;

  rc = doltliteSwitchCatalog(db, &mergedCatHash);
  doltliteCommitClear(&ourCommit);
  doltliteCommitClear(&theirCommit);
  if( rc!=SQLITE_OK ){
    bRestoreOnFail = 1;
    goto merge_fail;
  }

  rc = mergeRefInstallMergedCatalog(db, &ancCatHash, &theirCatHash,
                                    &mergedCatHash, nMergeConflicts,
                                    &aSchemaActions, &nSchemaActions,
                                    &azReindex, &nReindex,
                                    &azRebuildVtabs, &nRebuildVtabs);
  if( rc!=SQLITE_OK ){
    bRestoreOnFail = 1;
    goto merge_fail;
  }

  if( graphLocked ){
    chunkStoreUnlock(cs);
    graphLocked = 0;
  }

  rc = mergeRefDetectConstraintViolations(
      db, &ancCatHash, &nViolations, &zOwnedErr);
  if( rc!=SQLITE_OK ){
    bRestoreOnFail = 1;
    goto merge_fail;
  }
  if( nViolations > 0 ){
    /* A merge stopped by constraint violations is as unfinished as one
    ** stopped by conflicts: the caller has to resolve
    ** dolt_constraint_violations and commit. Record the merge so
    ** dolt_merge_status reports it, before the finish path persists the
    ** working set. Redundant when conflicts already set it, and the
    ** autocommit/nested-savepoint modes inside the finish call roll it back
    ** with the rest of the merge. When row/schema conflicts are also
    ** present, report both so the caller does not miss dolt_conflicts. */
    ProllyHash cvConflictsHash;
    doltliteGetSessionConflictsCatalog(db, &cvConflictsHash);
    if( doltliteSetSessionMergeState(db, 1, &theirHead,
                                    &cvConflictsHash)==SQLITE_OK ){
      (void)doltliteSetSessionMergeSourceSpec(db, zBranch, &theirHead);
    }
    if( nMergeConflicts>0 ){
      (void)doltliteCmdFinishWithConflictsAndConstraintViolations(
          db, context, &savedState, nMergeConflicts, "Merge", 0, 0);
    }else{
      (void)doltliteCmdFinishWithConstraintViolations(
          db, context, &savedState, "Merge", 0,
          "Merge aborted: would have introduced constraint violations. "
          "The merge and the would-be violations have been rolled back "
          "with the enclosing savepoint, so dolt_constraint_violations "
          "is empty. To inspect the violations, re-run the merge inside "
          "a plain BEGIN/COMMIT transaction (no SAVEPOINT) so the "
          "violations are preserved instead of rolled back.");
    }
    bHaveSaved = 0;
    return SQLITE_ERROR;
  }

  if( nMergeConflicts > 0 ){
    (void)doltliteCmdFinishWithConflicts(
        db, context, &savedState, nMergeConflicts, "Merge", 0);
    bHaveSaved = 0;
    return SQLITE_ERROR;
  }

  return mergeRefCreateMergeCommit(
      db, context, &savedState, &ourHead, &theirHead, &mergedCatHash,
      zBranch, zMessage);

merge_fail:
  if( graphLocked ){
    chunkStoreUnlock(cs);
    graphLocked = 0;
  }
  freeSchemaMergeActions(aSchemaActions, nSchemaActions);
  doltliteFreeNameList(azReindex, nReindex);
  doltliteFreeNameList(azRebuildVtabs, nRebuildVtabs);
  doltliteCommitClear(&ourCommit);
  doltliteCommitClear(&theirCommit);
  if( bHaveSaved ){
    if( bRestoreOnFail ){
      rc = doltliteRestoreTxnStateOnFailure(db, &savedState, rc);
    }else{
      doltliteTxnStateClear(&savedState);
    }
  }
  if( bPeerBusy ){
    doltliteCmdResultPeerBranchBusy(context, "merge");
  }else if( zOwnedErr ){
    sqlite3_result_error(context, zOwnedErr, -1);
  }else if( zFail ){
    sqlite3_result_error(context, zFail, -1);
  }else{
    sqlite3_result_error_code(context, rc);
  }
  sqlite3_free(zOwnedErr);
  return SQLITE_ERROR;
}

static void doltliteMergeFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  const char *zBranch = 0;
  const char *zMessage = 0;
  int isAbort = 0;
  int noFastForward = 0;
  u8 isMerging = 0;
  int rc, i;

  if( !doltliteGetChunkStore(db) ){
    sqlite3_result_error(context, doltliteVcUnavailableMessage(db), -1);
    return;
  }
  if( argc<1 ){
    sqlite3_result_error(context, "usage: dolt_merge('branch')", -1);
    return;
  }

  for(i=0; i<argc; i++){
    const char *arg = (const char*)sqlite3_value_text(argv[i]);
    if( !arg ) continue;
    if( strcmp(arg, "--abort")==0 ){
      isAbort = 1;
    }else if( strcmp(arg, "--no-ff")==0 ){
      noFastForward = 1;
    }else if( strcmp(arg, "-m")==0 || strcmp(arg, "--message")==0 ){
      zMessage = doltliteCmdTakeValueArg(context, argc, argv, &i, "message");
      if( !zMessage ) return;
    }else if( arg[0]=='-' ){
      doltliteCmdResultUnknownOption(context, arg);
      return;
    }else if( !zBranch ){
      zBranch = arg;
    }else{
      sqlite3_result_error(context, "too many positional arguments to dolt_merge", -1);
      return;
    }
  }

  if( isAbort ){
    if( zBranch || zMessage || noFastForward ){
      sqlite3_result_error(context,
        "--abort does not take other arguments", -1);
      return;
    }
    doltliteGetSessionMergeState(db, &isMerging, 0, 0);
    if( !isMerging ){
      sqlite3_result_error(context, "no merge in progress", -1);
      return;
    }
    rc = mergeAbortInPlace(db);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      return;
    }
    sqlite3_result_int(context, 0);
    return;
  }

  (void)doltliteMergeRef(db, context, zBranch, zMessage, noFastForward);
}


int doltliteMergeCmdRegister(sqlite3 *db){
  return sqlite3_create_function(db, "dolt_merge", -1,
                                 DOLTLITE_COMMAND_FUNC_FLAGS, 0,
                                 doltliteMergeFunc, 0, 0);
}

#endif
