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
#include <stddef.h>
#include "doltlite_ignore.h"

#include <string.h>
#include <ctype.h>
#include <time.h>

int doltliteLoadFirstParentCommit(
  sqlite3 *db,
  const DoltliteCommit *pCommit,
  DoltliteCommit *pParentCommit
){
  const ProllyHash *pParent = doltliteCommitParentHash(pCommit, 0);
  if( !pParent || prollyHashIsEmpty(pParent) ){
    return SQLITE_EMPTY;
  }
  return doltliteLoadCommit(db, pParent, pParentCommit);
}

int doltliteLoadHeadAndParentedCommit(
  sqlite3 *db,
  const ProllyHash *pTargetHash,
  ProllyHash *pOurHead,
  DoltliteCommit *pTargetCommit,
  DoltliteCommit *pParentCommit,
  DoltliteCommit *pOurCommit
){
  int rc = doltliteLoadCommit(db, pTargetHash, pTargetCommit);
  if( rc!=SQLITE_OK ) return rc;

  if( doltliteCommitParentCount(pTargetCommit)==0 ){
    return SQLITE_EMPTY;
  }

  rc = doltliteLoadFirstParentCommit(db, pTargetCommit, pParentCommit);
  if( rc!=SQLITE_OK ) return rc;

  doltliteGetSessionHead(db, pOurHead);
  if( prollyHashIsEmpty(pOurHead) ){
    return SQLITE_DONE;
  }

  rc = doltliteLoadCommit(db, pOurHead, pOurCommit);
  return rc;
}

typedef struct ApplyAbortRefsCtx ApplyAbortRefsCtx;
struct ApplyAbortRefsCtx {
  const char *zBranch;
  const ProllyHash *pExpectedHead;
  const ProllyHash *pCatalog;
  const ProllyHash *pWsBasis;
  int bPeerWrote;
};

static int applyAbortRefs(sqlite3 *db, ChunkStore *cs, void *pArg){
  ApplyAbortRefsCtx *p = (ApplyAbortRefsCtx*)pArg;
  ProllyHash head;
  int rc;
  rc = chunkStoreFindBranch(cs, p->zBranch, &head);
  if( rc!=SQLITE_OK ) return rc;
  if( prollyHashCompare(&head, p->pExpectedHead)!=0 ) return SQLITE_OK;
  /* A peer published the working set since this op began; it is on disk and
  ** restoring ours over it would erase it. */
  rc = doltliteBranchWorkingSetUnmoved(cs, p->zBranch, p->pWsBasis);
  if( rc==SQLITE_BUSY ){
    p->bPeerWrote = 1;
    return SQLITE_OK;
  }
  if( rc!=SQLITE_OK ) return rc;
  return doltliteUpdateBranchWorkingState(
      db, p->zBranch, p->pCatalog, p->pExpectedHead);
}

static int applyRestoreOriginalBranch(
  sqlite3 *db,
  const char *zBranch,
  const ProllyHash *pHead,
  const ProllyHash *pCatalog,
  const ProllyHash *pWsBasis,
  int opRc
){
  ApplyAbortRefsCtx ctx;
  int rc;
  memset(&ctx, 0, sizeof(ctx));
  ctx.zBranch = zBranch;
  ctx.pExpectedHead = pHead;
  ctx.pCatalog = pCatalog;
  ctx.pWsBasis = pWsBasis;
  rc = doltliteMutateRefs(db, applyAbortRefs, &ctx);
  if( ctx.bPeerWrote ){
    doltliteInvalidateSessionWorkingState(db);
  }else if( db->autoCommit ){
    doltliteAdoptRollbackBaseline(db, pCatalog);
  }
  return rc==SQLITE_OK ? opRc : rc;
}

static int cherryPickRestoreAndPersist(
  sqlite3 *db,
  DoltliteTxnState *pSaved,
  int opRc
){
  ProllyHash restoredCat = pSaved->sessionCatalogHash;
  ProllyHash savedHead = pSaved->sessionHead;
  ProllyHash wsBasis = pSaved->wsBasis;
  u32 nWsForeignAdopt = pSaved->nWsForeignAdopt;
  ProllyHash diskHead;
  ProllyHash sessionHead;
  ChunkStore *cs = doltliteGetChunkStore(db);
  int found = 0;
  int restoreRc;
  int persistRc;

  /* CompareAndAdvanceBranch can return an error after the new tip is already
  ** on disk with a matching working set. Restoring the pre-op working set
  ** then binds working-commit to the old tip; reopen discards that blob
  ** (working-commit != HEAD) and leaves staged empty. */
  memset(&diskHead, 0, sizeof(diskHead));
  doltliteGetSessionHead(db, &sessionHead);
  if( cs
   && chunkStoreReadDiskBranchTip(
        cs, doltliteGetSessionBranch(db), &diskHead, &found)==SQLITE_OK
   && found
   && prollyHashCompare(&diskHead, &savedHead)!=0
   && prollyHashCompare(&diskHead, &sessionHead)==0 ){
    doltliteTxnStateClear(pSaved);
    return opRc;
  }

  restoreRc = doltliteRestoreTxnStateOnFailure(db, pSaved, opRc);
  /* BUSY means a peer moved the tip or the working set; the disk holds its
  ** write, and persisting the pre-op catalog over it would erase it. */
  if( restoreRc==opRc && opRc!=SQLITE_BUSY
   && !prollyHashIsEmpty(&restoredCat) ){
    int bLocked, bPeerWrote;
    persistRc = doltliteLockForRestore(db, &savedHead, &wsBasis,
                                       nWsForeignAdopt, &bLocked, &bPeerWrote);
    if( persistRc==SQLITE_OK && !bPeerWrote ){
      persistRc = doltlitePersistWorkingSetWithHash(db, &restoredCat);
    }
    if( bLocked ) chunkStoreUnlock(cs);
    if( persistRc!=SQLITE_OK && persistRc!=SQLITE_NOMEM ) restoreRc = persistRc;
  }
  return restoreRc;
}

typedef struct ApplyConflictNames ApplyConflictNames;
struct ApplyConflictNames {
  char **az;
  int n;
  int nAlloc;
};

static int applyCollectConflictName(
  void *pCtx,
  const char *zName,
  int nConflicts
){
  ApplyConflictNames *p = (ApplyConflictNames*)pCtx;
  char *zCopy;
  int rc;
  (void)nConflicts;
  if( !zName || !zName[0] ) return SQLITE_OK;
  rc = DOLTLITE_GROW_ARRAY(&p->az, &p->nAlloc, p->n + 1, 8);
  if( rc!=SQLITE_OK ) return rc;
  zCopy = sqlite3_mprintf("%s", zName);
  if( !zCopy ) return SQLITE_NOMEM;
  p->az[p->n++] = zCopy;
  return SQLITE_OK;
}

static int applyNameIsConflict(char **az, int n, const char *zName){
  int i;
  if( !zName ) return 0;
  for(i=0; i<n; i++){
    if( az[i] && strcmp(az[i], zName)==0 ) return 1;
  }
  return 0;
}

/* Conflicted cherry-pick and rebase keep each conflicted table at its
** pre-merge staged entry and stage every other table from the merge.
** Working stays the merged catalog, so a schema change on a conflicted
** table is an unstaged modification beside the conflict row. */
static int applyStageConflictsAtBase(
  sqlite3 *db,
  const ProllyHash *pBaseCat,
  const ProllyHash *pMergedCat
){
  ChunkStore *cs;
  struct TableEntry *aBase = 0;
  struct TableEntry *aMerged = 0;
  struct TableEntry *pWorkMaster;
  struct TableEntry *pBaseMaster;
  struct TableEntry *pBaseEntry;
  ApplyConflictNames names;
  const char **azTouched = 0;
  ProllyHash composedRoot;
  ProllyHash stagedHash;
  u8 *buf = 0;
  int nBuf = 0;
  int nBase = 0;
  int nMerged = 0;
  int nTouched = 0;
  int i;
  int rc;

  memset(&names, 0, sizeof(names));
  memset(&composedRoot, 0, sizeof(composedRoot));
  memset(&stagedHash, 0, sizeof(stagedHash));
  cs = doltliteGetChunkStore(db);
  if( !cs || !pBaseCat || !pMergedCat ) return SQLITE_ERROR;

  rc = doltliteForEachConflict(db, applyCollectConflictName, &names);
  if( rc!=SQLITE_OK ) goto stage_done;
  rc = doltliteLoadCatalog(db, pMergedCat, &aMerged, &nMerged, 0);
  if( rc!=SQLITE_OK ) goto stage_done;
  rc = doltliteLoadCatalog(db, pBaseCat, &aBase, &nBase, 0);
  if( rc!=SQLITE_OK ) goto stage_done;

  for(i=0; i<nMerged; i++){
    if( aMerged[i].iTable<=1 || !aMerged[i].zName ) continue;
    if( !applyNameIsConflict(names.az, names.n, aMerged[i].zName) ) continue;
    pBaseEntry = doltliteFindTableByName(aBase, nBase, aMerged[i].zName);
    if( !pBaseEntry ) continue;
    aMerged[i].root = pBaseEntry->root;
    aMerged[i].schemaHash = pBaseEntry->schemaHash;
    aMerged[i].flags = pBaseEntry->flags;
  }

  if( nMerged>0 ){
    azTouched = sqlite3_malloc64((sqlite3_uint64)nMerged * sizeof(*azTouched));
    if( !azTouched ){
      rc = SQLITE_NOMEM;
      goto stage_done;
    }
  }
  for(i=0; i<nMerged; i++){
    if( aMerged[i].iTable<=1 || !aMerged[i].zName ) continue;
    if( applyNameIsConflict(names.az, names.n, aMerged[i].zName) ) continue;
    if( sqlite3_stricmp(aMerged[i].zName, "dolt_rebase")==0 ) continue;
    azTouched[nTouched++] = aMerged[i].zName;
  }

  pWorkMaster = doltliteFindTableByNumber(aMerged, nMerged, 1);
  pBaseMaster = doltliteFindTableByNumber(aBase, nBase, 1);
  if( !pWorkMaster ){
    rc = SQLITE_CORRUPT;
    goto stage_done;
  }
  rc = doltliteBuildNamedStageMasterRoot(db,
      &pWorkMaster->root, pWorkMaster->flags,
      pBaseMaster ? &pBaseMaster->root : 0,
      pBaseMaster ? pBaseMaster->flags : 0,
      azTouched, nTouched, aMerged, nMerged, 1, &composedRoot);
  if( rc!=SQLITE_OK ) goto stage_done;
  pWorkMaster->root = composedRoot;

  rc = doltliteSerializeCatalogEntries(db, aMerged, nMerged, &buf, &nBuf);
  if( rc==SQLITE_OK ){
    rc = chunkStorePut(cs, buf, nBuf, &stagedHash);
  }
  if( rc==SQLITE_OK ){
    rc = doltliteSetSessionStaged(db, &stagedHash);
  }

stage_done:
  sqlite3_free(buf);
  sqlite3_free(azTouched);
  doltliteFreeCatalog(aMerged, nMerged);
  doltliteFreeCatalog(aBase, nBase);
  doltliteFreeNameList(names.az, names.n);
  return rc;
}

int applyMergedCatalogAndCommit(
  sqlite3 *db,
  sqlite3_context *context,
  const ProllyHash *ancCatHash,
  const ProllyHash *ourCatHash,
  const ProllyHash *theirCatHash,
  const ProllyHash *ourHead,
  const ProllyHash *pCommitOurCatHash,
  const char *zMessage,
  const char *zAuthorName,
  const char *zAuthorEmail,
  int bPreferOurMaster,
  int bRejectUnchanged,
  int *pnConflicts,
  int *pnViolations,
  char **pzApplyErr,
  char *hexBuf
){
  ChunkStore *cs;
  DoltliteTxnState savedState;
  ProllyHash mergedCatHash;
  ProllyHash liveMergedCatHash;
  ProllyHash commitCatHash;
  int commitSplit = 0;
  ProllyHash commitHash;
  ProllyHash cleanWorkingSet;
  const ProllyHash *pCleanWorkingSet = 0;
  ProllyHash wsBasis;
  char *zMergeErr = 0;
  int graphLocked = 0;
  const char *zOpLabel;
  const char *zBranch;
  int rc;

  assert( db!=0 && context!=0 );
  assert( ancCatHash!=0 && ourCatHash!=0 && theirCatHash!=0 );
  assert( ourHead!=0 && zMessage!=0 && pnConflicts!=0 );
  if( pzApplyErr ) *pzApplyErr = 0;
  cs = doltliteGetChunkStore(db);
  memset(&savedState, 0, sizeof(savedState));
  if( hexBuf ) hexBuf[0] = '\0';
  zOpLabel = bPreferOurMaster ? "Revert" : "Cherry-pick";
  zBranch = doltliteGetSessionBranch(db);
  if( !doltliteGetSessionRebaseFlags(db) ){
    doltliteGetSessionWorkingSetBasis(db, &cleanWorkingSet);
    pCleanWorkingSet = &cleanWorkingSet;
  }
  memset(&wsBasis, 0, sizeof(wsBasis));
  if( cs ) chunkStorePeekWorkingSetBasis(cs, zBranch, &wsBasis);

  rc = doltliteEnsureWriteTxnAndSavepoints(db);
  if( rc!=SQLITE_OK ){
    return applyRestoreOriginalBranch(
        db, zBranch, ourHead, ourCatHash, &wsBasis, rc);
  }

  rc = doltliteSaveTxnState(db, &savedState);
  if( rc!=SQLITE_OK ){
    return applyRestoreOriginalBranch(
        db, zBranch, ourHead, ourCatHash, &wsBasis, rc);
  }

  {
    char **azReindex = 0;
    char **azRebuild = 0;
    SchemaMergeAction *aSchemaActions = 0;
    int nReindex = 0;
    int nRebuild = 0;
    int nSchemaActions = 0;
    rc = doltliteMergeCatalogs(db, ancCatHash, ourCatHash, theirCatHash,
                                &mergedCatHash, pnConflicts, &zMergeErr,
                                &aSchemaActions, &nSchemaActions,
                                bPreferOurMaster, 0,
                                &azReindex, &nReindex,
                                &azRebuild, &nRebuild);
    if( rc!=SQLITE_OK ){
      if( pzApplyErr && zMergeErr ){
        *pzApplyErr = zMergeErr;
        zMergeErr = 0;
      }
      sqlite3_free(zMergeErr);
      doltliteTxnStateClear(&savedState);
      doltliteFreeNameList(azReindex, nReindex);
      doltliteFreeNameList(azRebuild, nRebuild);
      freeSchemaMergeActions(aSchemaActions, nSchemaActions);
      return rc;
    }
    sqlite3_free(zMergeErr);

    rc = doltliteRefreshAndConfirmHead(db, cs, ourHead);
    if( rc!=SQLITE_OK ){
      doltliteFreeNameList(azReindex, nReindex);
      doltliteFreeNameList(azRebuild, nRebuild);
      freeSchemaMergeActions(aSchemaActions, nSchemaActions);
      return doltliteRestoreTxnStateOnFailure(db, &savedState, rc);
    }
    graphLocked = 1;

    rc = doltliteSwitchCatalog(db, &mergedCatHash);
    if( rc==SQLITE_OK ){
      rc = doltlitePrimeSchemaCache(db);
    }
    if( rc==SQLITE_OK && nReindex>0 ){
      rc = doltliteReindexNamedIndexes(db, azReindex, nReindex, 0);
    }
    /* Column adds, drops and renames the three-way produced are applied here
    ** for the same reason the branch merge applies them: the rows were relaid
    ** out for the merged schema, and without these the live catalog never
    ** adopts it and the leftover divergence surfaces as a schema conflict. */
    if( rc==SQLITE_OK && nSchemaActions>0 ){
      char *zActionErr = 0;
      rc = doltliteApplyMergeSchemaActions(db, ancCatHash, theirCatHash,
                                           aSchemaActions, nSchemaActions,
                                           &mergedCatHash, &zActionErr);
      if( rc!=SQLITE_OK && zActionErr && pzApplyErr && *pzApplyErr==0 ){
        *pzApplyErr = zActionErr;
        zActionErr = 0;
      }
      sqlite3_free(zActionErr);
    }
    if( rc==SQLITE_OK && nRebuild>0 ){
      rc = doltliteRebuildVirtualTables(db, azRebuild, nRebuild);
    }
    doltliteFreeNameList(azReindex, nReindex);
    doltliteFreeNameList(azRebuild, nRebuild);
    freeSchemaMergeActions(aSchemaActions, nSchemaActions);
    if( rc!=SQLITE_OK ) goto apply_rollback;
  }

  rc = doltliteFlushCatalogToHash(db, &liveMergedCatHash);
  if( rc==SQLITE_OK ){
    rc = doltliteSwitchCatalog(db, &liveMergedCatHash);
  }
  if( rc!=SQLITE_OK ) goto apply_rollback;

  if( *pnConflicts>0 ){
    const ProllyHash *pStageBase = ourCatHash;
    if( !prollyHashIsEmpty(&savedState.sessionStaged) ){
      pStageBase = &savedState.sessionStaged;
    }
    rc = applyStageConflictsAtBase(db, pStageBase, &liveMergedCatHash);
  }else{
    rc = doltliteSetSessionStaged(db, &liveMergedCatHash);
  }
  if( rc==SQLITE_OK ){
    rc = doltliteUpdateBranchWorkingState(db,
        doltliteGetSessionBranch(db), &liveMergedCatHash, NULL);
  }
  if( rc!=SQLITE_OK ) goto apply_rollback;

  if( graphLocked ){
    chunkStoreUnlock(cs);
    graphLocked = 0;
  }

  {
    int nViolations = 0;
    char *zDetectErrMsg = 0;

    rc = doltliteDetectConstraintViolationsInTxn(
        db, ancCatHash, &nViolations, &zDetectErrMsg);
    if( rc!=SQLITE_OK ){
      if( zDetectErrMsg ){
        sqlite3_result_error(context, zDetectErrMsg, -1);
        if( pzApplyErr && *pzApplyErr==0 ){
          *pzApplyErr = zDetectErrMsg;
        }else{
          sqlite3_free(zDetectErrMsg);
        }
      }
      goto apply_rollback;
    }
    sqlite3_free(zDetectErrMsg);

    if( nViolations > 0 ){
      if( pnViolations ){
        *pnViolations = nViolations;
      }
      if( *pnConflicts > 0 ){
        return doltliteCmdFinishWithConflictsAndConstraintViolations(
            db, context, &savedState, *pnConflicts, zOpLabel, 1, 0);
      }
      return doltliteCmdFinishWithConstraintViolations(
          db, context, &savedState, zOpLabel, 1,
          "Merge aborted: would have introduced constraint violations. "
          "The merge and the would-be violations have been rolled back "
          "with the enclosing savepoint, so dolt_constraint_violations "
          "is empty. Re-run the merge in autocommit mode (outside a "
          "transaction) to inspect the violations in "
          "dolt_constraint_violations.");
    }
  }

  if( *pnConflicts > 0 ){
    if( graphLocked ){
      chunkStoreUnlock(cs);
      graphLocked = 0;
    }
    return doltliteCmdFinishWithConflicts(
        db, context, &savedState, *pnConflicts, zOpLabel, 1);
  }

  /* Un-apply the uncommitted delta from liveMergedCatHash when the commit must
  ** be based on pCommitOurCatHash. Three-way with the pre-op working catalog
  ** as base. Overlap gate keeps the table sets disjoint. */
  commitCatHash = liveMergedCatHash;
  if( pCommitOurCatHash
   && prollyHashCompare(pCommitOurCatHash, ourCatHash)!=0 ){
    char **azReindexC = 0;
    int nReindexC = 0;
    int nCommitConflicts = 0;
    char *zCErr = 0;
    rc = doltliteMergeCatalogs(db, ourCatHash, &liveMergedCatHash,
                               pCommitOurCatHash, &commitCatHash,
                               &nCommitConflicts, &zCErr, 0, 0, 0, 0,
                               &azReindexC, &nReindexC, 0, 0);
    sqlite3_free(zCErr);
    doltliteFreeNameList(azReindexC, nReindexC);
    if( rc==SQLITE_OK && (nCommitConflicts>0 || nReindexC>0) ){
      rc = SQLITE_CONSTRAINT;
    }
    if( rc!=SQLITE_OK ) goto apply_rollback;
    commitSplit = 1;
  }

  /* Inverse already in HEAD: do not write an empty Revert commit. Restore
  ** the pre-op working set and tell the caller. */
  if( bRejectUnchanged && *pnConflicts==0 ){
    const ProllyHash *pHeadCat = pCommitOurCatHash
        ? pCommitOurCatHash : ourCatHash;
    if( prollyHashCompare(&commitCatHash, pHeadCat)==0 ){
      if( graphLocked ){
        chunkStoreUnlock(cs);
        graphLocked = 0;
      }
      return cherryPickRestoreAndPersist(db, &savedState, SQLITE_DONE);
    }
  }

  rc = doltliteCreateAndStoreCommit(db, ourHead, &commitCatHash,
      zMessage, zAuthorName, zAuthorEmail, NULL, 0, &commitHash);
  if( rc!=SQLITE_OK ) goto apply_rollback;

  rc = doltliteCompareAndAdvanceBranch(
      db, ourHead, pCleanWorkingSet, &commitHash, &commitCatHash,
      commitSplit ? &liveMergedCatHash : 0);
  if( rc!=SQLITE_OK ) goto apply_rollback;

  if( graphLocked ){
    chunkStoreUnlock(cs);
    graphLocked = 0;
  }
  rc = doltliteVcSealEnclosingTxn(db);
  if( rc!=SQLITE_OK ){
    doltliteTxnStateClear(&savedState);
    return rc;
  }
  doltliteTxnStateClear(&savedState);
  doltliteHashToHex(&commitHash, hexBuf);
  return SQLITE_OK;

apply_rollback:
  if( graphLocked ){
    chunkStoreUnlock(cs);
  }
  return cherryPickRestoreAndPersist(db, &savedState, rc);
}

static void doltliteCherryPickFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  ChunkStore *cs = doltliteGetChunkStore(db);
  const char *zRef;
  ProllyHash pickHash, ourHead;
  DoltliteCommit pickCommit, parentCommit, ourCommit;
  DoltliteCmdArgs args;
  int isAbort = 0;
  DoltliteCmdOption aOption[] = {
    { "abort", 0, DOLTLITE_CMD_OPTION_FLAG, &isAbort, 0 }
  };
  int nConflicts = 0;
  int dirty = 0;
  int rc;
  char *zApplyErr = 0;
  char hexBuf[PROLLY_HASH_SIZE*2+1];

  memset(&pickCommit, 0, sizeof(pickCommit));
  memset(&parentCommit, 0, sizeof(parentCommit));
  memset(&ourCommit, 0, sizeof(ourCommit));

  if( doltliteCmdRejectDetached(context) ) return;
  if( doltliteCmdRejectReadOnly(context) ) return;
  if( !cs ){ sqlite3_result_error(context, "no database", -1); return; }
  if( argc<1 ){
    sqlite3_result_error(context, "usage: dolt_cherry_pick('commit_hash')", -1);
    return;
  }

  rc = doltliteCmdParseArgs(context, argc, argv, aOption, ArraySize(aOption),
                            0, &args);
  if( rc!=SQLITE_OK ) return;
  if( isAbort ){
    if( args.nPositional>0 ){
      doltliteCmdArgsClear(&args);
      sqlite3_result_error(context,
        "--abort does not take other arguments", -1);
      return;
    }
    if( !doltliteSessionHasPendingReplayCommit(db) ){
      doltliteCmdArgsClear(&args);
      sqlite3_result_error(context, "no cherry-pick in progress", -1);
      return;
    }
    rc = mergeAbortInPlace(db);
    doltliteCmdArgsClear(&args);
    if( rc!=SQLITE_OK ){
      doltliteCmdSourceResultErrorOrCode(context, cs, rc);
      return;
    }
    sqlite3_result_int(context, 0);
    return;
  }
  if( args.nPositional>1 ){
    doltliteCmdArgsClear(&args);
    sqlite3_result_error(context,
      "cherry-picking multiple commits is not supported yet.", -1);
    return;
  }

  zRef = args.nPositional==1 ? args.azPositional[0] : 0;
  if( !zRef ){
    doltliteCmdArgsClear(&args);
    sqlite3_result_error(context, "commit hash required", -1);
    return;
  }
  doltliteCmdArgsClear(&args);

  rc = doltliteHasUncommittedChanges(db, &dirty);
  if( rc!=SQLITE_OK ){
    doltliteCmdSourceResultErrorOrCode(context, cs, rc);
    return;
  }
  if( dirty ){
    sqlite3_result_error(context,
      "cannot cherry-pick with uncommitted changes", -1);
    return;
  }

  rc = doltliteResolveRef(db,zRef, &pickHash);
  if( rc!=SQLITE_OK ){
    doltliteCmdReportInvalidCommitHash(context, cs, rc);
    return;
  }
  rc = doltliteLoadHeadAndParentedCommit(
    db, &pickHash,
    &ourHead, &pickCommit, &parentCommit, &ourCommit
  );
  if( doltliteCmdReportLoadParentedCommitError(
        context, cs, rc, &pickCommit, &parentCommit, &ourCommit,
        "cannot cherry-pick the initial commit") ){
    return;
  }
  if( doltliteCommitParentCount(&pickCommit)>1 ){
    doltliteCommitClear(&pickCommit);
    doltliteCommitClear(&parentCommit);
    doltliteCommitClear(&ourCommit);
    sqlite3_result_error(context,
      "cherry-picking a merge commit is not supported", -1);
    return;
  }

  {
    const char *zMsg = pickCommit.zMessage;
    char fallback[256];
    if( !zMsg || !*zMsg ){
      sqlite3_snprintf(sizeof(fallback), fallback, "cherry-pick of %s", zRef);
      zMsg = fallback;
    }

    rc = applyMergedCatalogAndCommit(db, context,
        &parentCommit.catalogHash, &ourCommit.catalogHash,
        &pickCommit.catalogHash, &ourHead, 0, zMsg, 0, 0, 0, 1, &nConflicts, 0,
        &zApplyErr, hexBuf);
  }

  doltliteCommitClear(&pickCommit);
  doltliteCommitClear(&parentCommit);
  doltliteCommitClear(&ourCommit);

  doltliteCmdFinishApplyMerged(
      context, cs, rc, nConflicts, zApplyErr, "cherry-pick", zRef,
      "no changes were made, nothing to commit",
      "cherry-pick of %s failed", "cherry-pick failed", hexBuf);
}


int doltliteCherryPickRegister(sqlite3 *db){
  return doltliteCreateCommandFunc(db, "dolt_cherry_pick", -1,
                                 doltliteCherryPickFunc);
}

#endif
