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
};

static int applyAbortRefs(sqlite3 *db, ChunkStore *cs, void *pArg){
  ApplyAbortRefsCtx *p = (ApplyAbortRefsCtx*)pArg;
  ProllyHash head;
  int rc;
  rc = chunkStoreFindBranch(cs, p->zBranch, &head);
  if( rc!=SQLITE_OK ) return rc;
  if( prollyHashCompare(&head, p->pExpectedHead)!=0 ) return SQLITE_OK;
  return doltliteUpdateBranchWorkingState(
      db, p->zBranch, p->pCatalog, p->pExpectedHead);
}

static int applyRestoreOriginalBranch(
  sqlite3 *db,
  const char *zBranch,
  const ProllyHash *pHead,
  const ProllyHash *pCatalog,
  int opRc
){
  ApplyAbortRefsCtx ctx;
  int rc;
  memset(&ctx, 0, sizeof(ctx));
  ctx.zBranch = zBranch;
  ctx.pExpectedHead = pHead;
  ctx.pCatalog = pCatalog;
  rc = doltliteMutateRefs(db, applyAbortRefs, &ctx);
  if( db->autoCommit ) doltliteAdoptRollbackBaseline(db, pCatalog);
  return rc==SQLITE_OK ? opRc : rc;
}

static int cherryPickRestoreAndPersist(
  sqlite3 *db,
  DoltliteTxnState *pSaved,
  int opRc
){
  ProllyHash restoredCat = pSaved->sessionCatalogHash;
  ProllyHash savedHead = pSaved->sessionHead;
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
  if( restoreRc==opRc && !prollyHashIsEmpty(&restoredCat) ){
    persistRc = doltlitePersistWorkingSetWithHash(db, &restoredCat);
    if( persistRc!=SQLITE_OK && persistRc!=SQLITE_NOMEM ) restoreRc = persistRc;
  }
  return restoreRc;
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

  rc = doltliteEnsureWriteTxnAndSavepoints(db);
  if( rc!=SQLITE_OK ){
    return applyRestoreOriginalBranch(
        db, zBranch, ourHead, ourCatHash, rc);
  }

  rc = doltliteSaveTxnState(db, &savedState);
  if( rc!=SQLITE_OK ){
    return applyRestoreOriginalBranch(
        db, zBranch, ourHead, ourCatHash, rc);
  }

  {
    char **azReindex = 0;
    int nReindex = 0;
    rc = doltliteMergeCatalogs(db, ancCatHash, ourCatHash, theirCatHash,
                                &mergedCatHash, pnConflicts, &zMergeErr, 0, 0,
                                bPreferOurMaster, 0,
                                &azReindex, &nReindex, 0, 0);
    if( rc!=SQLITE_OK ){
      if( pzApplyErr && zMergeErr ){
        *pzApplyErr = zMergeErr;
        zMergeErr = 0;
      }
      sqlite3_free(zMergeErr);
      doltliteTxnStateClear(&savedState);
      doltliteFreeNameList(azReindex, nReindex);
      return rc;
    }
    sqlite3_free(zMergeErr);

    rc = doltliteRefreshAndConfirmHead(db, cs, ourHead);
    if( rc!=SQLITE_OK ){
      doltliteFreeNameList(azReindex, nReindex);
      return doltliteRestoreTxnStateOnFailure(db, &savedState, rc);
    }
    graphLocked = 1;

    rc = doltliteSwitchCatalog(db, &mergedCatHash);
    if( rc==SQLITE_OK ){
      rc = doltlitePrimeSchemaCache(db);
    }
    if( rc==SQLITE_OK && nReindex>0 ){
      rc = doltliteReindexNamedIndexes(db, azReindex, nReindex);
    }
    doltliteFreeNameList(azReindex, nReindex);
    if( rc!=SQLITE_OK ) goto apply_rollback;
  }

  rc = doltliteFlushCatalogToHash(db, &liveMergedCatHash);
  if( rc==SQLITE_OK ){
    rc = doltliteSwitchCatalog(db, &liveMergedCatHash);
  }
  if( rc!=SQLITE_OK ) goto apply_rollback;

  rc = doltliteSetSessionStaged(db, &liveMergedCatHash);
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

    rc = doltliteDetectConstraintViolationsFiltered(
        db, ancCatHash, 0, 0, 1, &nViolations, &zDetectErrMsg);
    if( rc!=SQLITE_OK ){
      if( zDetectErrMsg ){
        sqlite3_result_error(context, zDetectErrMsg, -1);
        sqlite3_free(zDetectErrMsg);
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
      zMessage, NULL, NULL, NULL, 0, &commitHash);
  if( rc!=SQLITE_OK ) goto apply_rollback;

  rc = doltliteCompareAndAdvanceBranch(
      db, ourHead, &commitHash, &commitCatHash,
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
      if( !doltliteCmdSourceResultError(context, cs, &rc) ){
        sqlite3_result_error_code(context, rc);
      }
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
    if( !doltliteCmdSourceResultError(context, cs, &rc) ){
      sqlite3_result_error_code(context, rc);
    }
    return;
  }
  if( dirty ){
    sqlite3_result_error(context,
      "cannot cherry-pick with uncommitted changes", -1);
    return;
  }

  rc = doltliteResolveRef(db,zRef, &pickHash);
  if( rc!=SQLITE_OK ){
    if( !doltliteCmdSourceResultError(context, cs, &rc) ){
      if( rc==SQLITE_NOTFOUND || rc==SQLITE_ERROR ){
        sqlite3_result_error(context, "invalid commit hash", -1);
      }else{
        sqlite3_result_error_code(context, rc);
      }
    }
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
        &pickCommit.catalogHash, &ourHead, 0, zMsg, 0, 1, &nConflicts, 0,
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
  return sqlite3_create_function(db, "dolt_cherry_pick", -1,
                                 DOLTLITE_COMMAND_FUNC_FLAGS, 0,
                                 doltliteCherryPickFunc, 0, 0);
}

#endif
