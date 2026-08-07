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
  if( rc!=SQLITE_OK ) return SQLITE_NOTFOUND;

  if( doltliteCommitParentCount(pTargetCommit)==0 ){
    return SQLITE_EMPTY;
  }

  rc = doltliteLoadFirstParentCommit(db, pTargetCommit, pParentCommit);
  if( rc!=SQLITE_OK ) return SQLITE_NOTFOUND;

  doltliteGetSessionHead(db, pOurHead);
  if( prollyHashIsEmpty(pOurHead) ){
    return SQLITE_DONE;
  }

  rc = doltliteLoadCommit(db, pOurHead, pOurCommit);
  if( rc!=SQLITE_OK ) return SQLITE_ABORT;
  return SQLITE_OK;
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

int applyMergedCatalogAndCommit(
  sqlite3 *db,
  sqlite3_context *context,
  const ProllyHash *ancCatHash,
  const ProllyHash *ourCatHash,
  const ProllyHash *theirCatHash,
  const ProllyHash *ourHead,
  const char *zMessage,
  int *pnConflicts,
  char *hexBuf
){
  ChunkStore *cs;
  DoltliteTxnState savedState;
  ProllyHash mergedCatHash;
  ProllyHash liveMergedCatHash;
  ProllyHash commitHash;
  char *zMergeErr = 0;
  int graphLocked = 0;
  int bPreferOurMaster;
  const char *zOpLabel;
  const char *zBranch;
  int rc;

  assert( db!=0 && context!=0 );
  assert( ancCatHash!=0 && ourCatHash!=0 && theirCatHash!=0 );
  assert( ourHead!=0 && zMessage!=0 && pnConflicts!=0 );
  cs = doltliteGetChunkStore(db);
  memset(&savedState, 0, sizeof(savedState));
  if( hexBuf ) hexBuf[0] = '\0';
  bPreferOurMaster = (sqlite3_strnicmp(zMessage, "Revert", 6)==0);
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
                                bPreferOurMaster, &azReindex, &nReindex, 0, 0);
    if( rc!=SQLITE_OK ){
      sqlite3_free(zMergeErr);
      doltliteTxnStateClear(&savedState);
      doltliteFreeNameList(azReindex, nReindex);
      return rc;
    }
    sqlite3_free(zMergeErr);

    rc = doltliteRefreshAndConfirmHead(db, cs, ourHead);
    if( rc!=SQLITE_OK ){
      doltliteTxnStateClear(&savedState);
      doltliteFreeNameList(azReindex, nReindex);
      return rc;
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
    int nUnique = 0;
    int nCheck = 0;
    char *zDetectErrMsg = 0;

    rc = doltliteConstraintViolationBatchBegin(db);
    if( rc==SQLITE_OK ){
      rc = doltliteDetectMergeFkViolations(db, ancCatHash,
                                           &zDetectErrMsg, &nViolations,
                                           0, 0);
    }
    if( rc==SQLITE_OK ){
      rc = doltliteDetectMergeUniqueViolations(db, ancCatHash,
                                               &zDetectErrMsg, &nUnique,
                                               0, 0);
    }
    if( rc==SQLITE_OK ){
      rc = doltliteDetectMergeCheckViolations(db, ancCatHash,
                                              &zDetectErrMsg, &nCheck,
                                              0, 0);
    }
    {
      int erc = doltliteConstraintViolationBatchEnd(db, rc==SQLITE_OK);
      if( rc==SQLITE_OK ) rc = erc;
    }
    if( rc!=SQLITE_OK ){
      if( zDetectErrMsg ){
        sqlite3_result_error(context, zDetectErrMsg, -1);
        sqlite3_free(zDetectErrMsg);
      }
      goto apply_rollback;
    }
    sqlite3_free(zDetectErrMsg);

    if( nViolations + nUnique + nCheck > 0 ){
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

  rc = doltliteCreateAndStoreCommit(db, ourHead, &liveMergedCatHash,
      zMessage, NULL, NULL, NULL, 0, &commitHash);
  if( rc!=SQLITE_OK ) goto apply_rollback;

  rc = doltliteCompareAndAdvanceBranch(
      db, ourHead, &commitHash, &liveMergedCatHash, 0);
  if( rc!=SQLITE_OK ) goto apply_rollback;

  rc = doltliteVcSealActiveSavepoints(db);
  if( rc!=SQLITE_OK ) goto apply_rollback;

  if( graphLocked ){
    chunkStoreUnlock(cs);
  }
  doltliteTxnStateClear(&savedState);
  doltliteHashToHex(&commitHash, hexBuf);
  return SQLITE_OK;

apply_rollback:
  if( graphLocked ){
    chunkStoreUnlock(cs);
  }
  {
    return doltliteRestoreTxnStateOnFailure(db, &savedState, rc);
  }
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
  int nConflicts = 0;
  int dirty = 0;
  int rc;
  char hexBuf[PROLLY_HASH_SIZE*2+1];

  memset(&pickCommit, 0, sizeof(pickCommit));
  memset(&parentCommit, 0, sizeof(parentCommit));
  memset(&ourCommit, 0, sizeof(ourCommit));

  if( !cs ){ sqlite3_result_error(context, "no database", -1); return; }
  if( argc<1 ){
    sqlite3_result_error(context, "usage: dolt_cherry_pick('commit_hash')", -1);
    return;
  }
  if( argc>1 ){
    sqlite3_result_error(context,
      "cherry-picking multiple commits is not supported yet.", -1);
    return;
  }

  zRef = (const char*)sqlite3_value_text(argv[0]);
  if( !zRef ){
    sqlite3_result_error(context, "commit hash required", -1);
    return;
  }

  rc = doltliteHasUncommittedChanges(db, &dirty);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    return;
  }
  if( dirty ){
    sqlite3_result_error(context,
      "cannot cherry-pick with uncommitted changes", -1);
    return;
  }

  rc = doltliteResolveRef(db,zRef, &pickHash);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "invalid commit hash", -1);
    return;
  }
  rc = doltliteLoadHeadAndParentedCommit(
    db, &pickHash,
    &ourHead, &pickCommit, &parentCommit, &ourCommit
  );
  if( rc==SQLITE_NOTFOUND ){
    doltliteCommitClear(&pickCommit);
    doltliteCommitClear(&parentCommit);
    sqlite3_result_error(context, "commit not found", -1);
    return;
  }
  if( rc==SQLITE_EMPTY ){
    doltliteCommitClear(&pickCommit);
    sqlite3_result_error(context, "cannot cherry-pick the initial commit", -1);
    return;
  }
  if( rc==SQLITE_DONE ){
    doltliteCommitClear(&pickCommit);
    doltliteCommitClear(&parentCommit);
    sqlite3_result_error(context, "no commits on current branch", -1);
    return;
  }
  if( rc==SQLITE_ABORT ){
    doltliteCommitClear(&pickCommit);
    doltliteCommitClear(&parentCommit);
    sqlite3_result_error(context, "failed to load HEAD commit", -1);
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
        &pickCommit.catalogHash, &ourHead, zMsg, &nConflicts, hexBuf);
  }

  doltliteCommitClear(&pickCommit);
  doltliteCommitClear(&parentCommit);
  doltliteCommitClear(&ourCommit);

  if( rc==SQLITE_BUSY ){
    doltliteCmdResultPeerBranchBusy(context, "cherry-pick");
    return;
  }
  if( rc!=SQLITE_OK ){
    char *zMsg = sqlite3_mprintf("cherry-pick of %s failed", zRef);
    sqlite3_result_error(context, zMsg ? zMsg : "cherry-pick failed", -1);
    sqlite3_free(zMsg);
    return;
  }

  /* Conflict / CV finish helpers already set the context error (including the
  ** combined conflicts+CVs message). Do not overwrite it here. */
  if( nConflicts > 0 ) return;
  if( hexBuf[0] ){
    sqlite3_result_text(context, hexBuf, -1, SQLITE_TRANSIENT);
  }
}


int doltliteCherryPickRegister(sqlite3 *db){
  return sqlite3_create_function(db, "dolt_cherry_pick", -1,
                                 DOLTLITE_COMMAND_FUNC_FLAGS, 0,
                                 doltliteCherryPickFunc, 0, 0);
}

#endif
