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
  int graphLocked = 0;
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
  rc = doltliteRefreshAndConfirmHead(db, cs, pOurHead);
  if( rc==SQLITE_BUSY ){
    doltliteTxnStateClear(&savedState);
    doltliteCommitClear(&theirCommit);
    sqlite3_result_error(context,
      "merge conflict: another connection committed to this branch. Please retry your transaction.",
      -1);
    return rc;
  }
  if( rc!=SQLITE_OK ){
    doltliteTxnStateClear(&savedState);
    doltliteCommitClear(&theirCommit);
    sqlite3_result_error_code(context, rc);
    return rc;
  }
  graphLocked = 1;
  rc = doltliteSwitchCatalog(db, &theirCommit.catalogHash);
  if( rc==SQLITE_OK ){
    rc = doltliteUpdateBranchWorkingState(db, doltliteGetSessionBranch(db),
                                          &theirCommit.catalogHash, NULL);
  }
  if( rc==SQLITE_OK ){
    rc = doltliteAdvanceBranch(db, pTheirHead, &theirCommit.catalogHash, 0);
  }
  if( graphLocked ) chunkStoreUnlock(cs);
  if( rc!=SQLITE_OK ){
    doltliteCommitClear(&theirCommit);
    sqlite3_result_error_code(context,
        doltliteRestoreTxnStateOnFailure(db, &savedState, rc));
    return rc;
  }
  rc = doltliteVcSealActiveSavepoints(db);
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

static void doltliteMergeFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  ChunkStore *cs = doltliteGetChunkStore(db);
  const char *zBranch = 0;
  const char *zMessage = 0;
  int isAbort = 0;
  int noFastForward = 0;
  u8 isMerging = 0;
  ProllyHash ourHead, theirHead, ancestorHash;
  ProllyHash ourCatHash, theirCatHash, ancCatHash, mergedCatHash;
  DoltliteTxnState savedState;
  int nMergeConflicts = 0;
  DoltliteCommit ourCommit, theirCommit, ancCommit;
  int graphLocked = 0;
  int dirty = 0;
  int rc, i;

  memset(&ourCommit, 0, sizeof(ourCommit));
  memset(&theirCommit, 0, sizeof(theirCommit));
  memset(&ancCommit, 0, sizeof(ancCommit));
  memset(&savedState, 0, sizeof(savedState));

  if( !cs ){ sqlite3_result_error(context, doltliteVcUnavailableMessage(db), -1); return; }
  if( argc<1 ){ sqlite3_result_error(context, "usage: dolt_merge('branch')", -1); return; }

  for(i=0; i<argc; i++){
    const char *arg = (const char*)sqlite3_value_text(argv[i]);
    if( !arg ) continue;
    if( strcmp(arg, "--abort")==0 ){
      isAbort = 1;
    }else if( strcmp(arg, "--no-ff")==0 ){
      noFastForward = 1;
    }else if( strcmp(arg, "-m")==0 || strcmp(arg, "--message")==0 ){
      if( i+1<argc ){
        zMessage = (const char*)sqlite3_value_text(argv[++i]);
      }else{
        sqlite3_result_error(context, "-m requires a message", -1);
        return;
      }
    }else if( arg[0]=='-' ){
      char *zErr = sqlite3_mprintf("unknown option `%s`", arg);
      if( zErr ){
        sqlite3_result_error(context, zErr, -1);
        sqlite3_free(zErr);
      }else{
        sqlite3_result_error_nomem(context);
      }
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

  if( !zBranch ){
    sqlite3_result_error(context, "branch name required", -1);
    return;
  }

  doltliteGetSessionHead(db, &ourHead);
  if( prollyHashIsEmpty(&ourHead) ){
    sqlite3_result_error(context, "no commits on current branch", -1);
    return;
  }

  rc = doltliteResolveRef(db, zBranch, &theirHead);
  if( rc!=SQLITE_OK || prollyHashIsEmpty(&theirHead) ){
    sqlite3_result_error(context, "merge source not found", -1);
    return;
  }

  if( prollyHashCompare(&ourHead, &theirHead)==0 ){
    sqlite3_result_text(context, "Already up to date", -1, SQLITE_STATIC);
    return;
  }

  rc = doltliteHasUncommittedChanges(db, &dirty);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    return;
  }
  if( dirty ){
    sqlite3_result_error(context,
      "uncommitted changes \xe2\x80\x94 commit or reset before merging", -1);
    return;
  }

  rc = doltliteFindAncestor(db, &ourHead, &theirHead, &ancestorHash);
  if( rc!=SQLITE_OK || prollyHashIsEmpty(&ancestorHash) ){
    sqlite3_result_error(context, "no common ancestor found", -1);
    return;
  }

  if( prollyHashCompare(&ancestorHash, &theirHead)==0 ){
    sqlite3_result_text(context, "Already up to date", -1, SQLITE_STATIC);
    return;
  }

  if( prollyHashCompare(&ancestorHash, &ourHead)==0 && !noFastForward ){
    rc = mergeFastForward(db, context, cs, &ourHead, &theirHead);
    return;
  }

  rc = doltliteLoadCommit(db, &ourHead, &ourCommit);
  if( rc!=SQLITE_OK ){ sqlite3_result_error(context, "failed to load our commit", -1); return; }
  memcpy(&ourCatHash, &ourCommit.catalogHash, sizeof(ProllyHash));

  rc = doltliteLoadCommit(db, &theirHead, &theirCommit);
  if( rc!=SQLITE_OK ){ doltliteCommitClear(&ourCommit); sqlite3_result_error(context, "failed to load their commit", -1); return; }
  memcpy(&theirCatHash, &theirCommit.catalogHash, sizeof(ProllyHash));

  rc = doltliteLoadCommit(db, &ancestorHash, &ancCommit);
  if( rc!=SQLITE_OK ){ doltliteCommitClear(&ourCommit); doltliteCommitClear(&theirCommit); sqlite3_result_error(context, "failed to load ancestor", -1); return; }
  memcpy(&ancCatHash, &ancCommit.catalogHash, sizeof(ProllyHash));
  doltliteCommitClear(&ancCommit);

  rc = doltliteEnsureWriteTxnAndSavepoints(db);
  if( rc!=SQLITE_OK ){
    doltliteCommitClear(&ourCommit);
    doltliteCommitClear(&theirCommit);
    sqlite3_result_error_code(context, rc);
    return;
  }

  rc = doltliteSaveTxnState(db, &savedState);
  if( rc!=SQLITE_OK ){
    doltliteCommitClear(&ourCommit);
    doltliteCommitClear(&theirCommit);
    sqlite3_result_error_code(context, rc);
    return;
  }

  {
    char *zMergeErr = 0;
    SchemaMergeAction *aSchemaActions = 0;
    int nSchemaActions = 0;
    char **azReindex = 0;
    int nReindex = 0;
    rc = doltliteMergeCatalogs(db, &ancCatHash, &ourCatHash, &theirCatHash,
                                &mergedCatHash, &nMergeConflicts, &zMergeErr,
                                &aSchemaActions, &nSchemaActions, 0,
                                &azReindex, &nReindex);
    if( rc!=SQLITE_OK ){
      doltliteCommitClear(&ourCommit);
      doltliteCommitClear(&theirCommit);
      if( zMergeErr ){
        sqlite3_result_error(context, zMergeErr, -1);
        sqlite3_free(zMergeErr);
      }else{
        sqlite3_result_error(context, "merge failed", -1);
      }
      doltliteTxnStateClear(&savedState);
      freeSchemaMergeActions(aSchemaActions, nSchemaActions);
      doltliteFreeNameList(azReindex, nReindex);
      return;
    }
    sqlite3_free(zMergeErr);

    if( nMergeConflicts>0 ){
      ProllyHash conflictsHash;
      doltliteGetSessionConflictsCatalog(db, &conflictsHash);
      rc = doltliteSetSessionMergeState(db, 1, &theirHead, &conflictsHash);
      if( rc!=SQLITE_OK ){
        doltliteCommitClear(&ourCommit);
        doltliteCommitClear(&theirCommit);
        doltliteTxnStateClear(&savedState);
        freeSchemaMergeActions(aSchemaActions, nSchemaActions);
        doltliteFreeNameList(azReindex, nReindex);
        sqlite3_result_error_code(context, rc);
        return;
      }
    }

    rc = doltliteRefreshAndConfirmHead(db, cs, &ourHead);
    if( rc==SQLITE_BUSY ){
      doltliteTxnStateClear(&savedState);
      doltliteCommitClear(&ourCommit);
      doltliteCommitClear(&theirCommit);
      freeSchemaMergeActions(aSchemaActions, nSchemaActions);
      doltliteFreeNameList(azReindex, nReindex);
      sqlite3_result_error(context,
        "merge conflict: another connection committed to this branch. Please retry your transaction.",
        -1);
      return;
    }
    if( rc!=SQLITE_OK ){
      doltliteTxnStateClear(&savedState);
      doltliteCommitClear(&ourCommit);
      doltliteCommitClear(&theirCommit);
      freeSchemaMergeActions(aSchemaActions, nSchemaActions);
      doltliteFreeNameList(azReindex, nReindex);
      sqlite3_result_error_code(context, rc);
      return;
    }
    graphLocked = 1;

    rc = doltliteSwitchCatalog(db, &mergedCatHash);
    doltliteCommitClear(&ourCommit);
    doltliteCommitClear(&theirCommit);
    if( rc!=SQLITE_OK ){
      if( graphLocked ){
        chunkStoreUnlock(cs);
        graphLocked = 0;
      }
      freeSchemaMergeActions(aSchemaActions, nSchemaActions);
      doltliteFreeNameList(azReindex, nReindex);
      sqlite3_result_error_code(context,
          doltliteRestoreTxnStateOnFailure(db, &savedState, rc));
      return;
    }

    /* Indexes adopted from the other branch carry only that branch's
    ** rows; rebuild them over the merged tables while the merged catalog
    ** is live so the flush below captures correct roots. */
    if( nReindex>0 ){
      rc = doltliteReindexNamedIndexes(db, azReindex, nReindex);
    }
    doltliteFreeNameList(azReindex, nReindex);
    azReindex = 0;
    nReindex = 0;
    if( rc!=SQLITE_OK ){
      if( graphLocked ){
        chunkStoreUnlock(cs);
        graphLocked = 0;
      }
      freeSchemaMergeActions(aSchemaActions, nSchemaActions);
      sqlite3_result_error_code(context,
          doltliteRestoreTxnStateOnFailure(db, &savedState, rc));
      return;
    }

    if( nSchemaActions > 0 ){
      rc = doltliteApplyMergeSchemaActions(db, &ancCatHash, &theirCatHash,
                                           aSchemaActions, nSchemaActions,
                                           &mergedCatHash);
      freeSchemaMergeActions(aSchemaActions, nSchemaActions);
      if( rc!=SQLITE_OK ){
        if( graphLocked ){
          chunkStoreUnlock(cs);
          graphLocked = 0;
        }
        sqlite3_result_error_code(context,
            doltliteRestoreTxnStateOnFailure(db, &savedState, rc));
        return;
      }
    }else{
      freeSchemaMergeActions(aSchemaActions, nSchemaActions);
    }

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

    rc = doltliteFlushCatalogToHash(db, &mergedCatHash);
    if( rc==SQLITE_OK ){
      rc = doltliteSwitchCatalog(db, &mergedCatHash);
    }
    if( rc==SQLITE_OK ){
      rc = doltlitePrimeSchemaCache(db);
    }
    if( rc==SQLITE_OK ){
      rc = doltliteSetSessionStaged(db, &mergedCatHash);
    }
    if( rc==SQLITE_OK ){
      rc = doltliteUpdateBranchWorkingState(db,
          doltliteGetSessionBranch(db), &mergedCatHash, NULL);
    }
    if( rc!=SQLITE_OK ){
      if( graphLocked ){
        chunkStoreUnlock(cs);
        graphLocked = 0;
      }
      sqlite3_result_error_code(context,
          doltliteRestoreTxnStateOnFailure(db, &savedState, rc));
      return;
    }
  }

  if( graphLocked ){
    chunkStoreUnlock(cs);
    graphLocked = 0;
  }
  {
    int nViolations = 0;
    int nUnique = 0;
    int nCheck = 0;
    char *zDetectErrMsg = 0;
    int vrc = doltliteConstraintViolationBatchBegin(db);
    if( vrc == SQLITE_OK ){
      vrc = doltliteDetectMergeFkViolations(db, &ancCatHash,
                                            &zDetectErrMsg, &nViolations);
    }
    if( vrc == SQLITE_OK ){
      vrc = doltliteDetectMergeUniqueViolations(db, &ancCatHash,
                                                &zDetectErrMsg, &nUnique);
    }
    if( vrc == SQLITE_OK ){
      vrc = doltliteDetectMergeCheckViolations(db, &ancCatHash,
                                               &zDetectErrMsg, &nCheck);
    }
    {
      int erc = doltliteConstraintViolationBatchEnd(db, vrc==SQLITE_OK);
      if( vrc==SQLITE_OK ) vrc = erc;
    }
    if( vrc != SQLITE_OK ){
      if( zDetectErrMsg ){
        sqlite3_result_error(context, zDetectErrMsg, -1);
        sqlite3_free(zDetectErrMsg);
        doltliteRestoreTxnStateOnFailure(db, &savedState, vrc);
      }else{
        sqlite3_result_error_code(context,
            doltliteRestoreTxnStateOnFailure(db, &savedState, vrc));
      }
      return;
    }
    sqlite3_free(zDetectErrMsg);
    if( nViolations + nUnique + nCheck > 0 ){
      switch( doltliteVcTxnMode(db) ){
      case DOLTLITE_VC_TXN_AUTOCOMMIT_LIKE:
        rc = doltliteHardReset(db, &savedState.sessionCatalogHash);
        if( rc==SQLITE_OK ){
          rc = doltliteSetSessionBranch(db, savedState.zSessionBranch);
        }
        if( rc==SQLITE_OK ){
          doltliteSetSessionHead(db, &savedState.sessionHead);
          rc = doltliteSetSessionStaged(db, &savedState.sessionStaged);
        }
        if( rc==SQLITE_OK ){
          rc = doltliteSetSessionMergeState(
              db, savedState.sessionIsMerging,
              &savedState.sessionMergeCommit,
              &savedState.sessionConflictsCatalog);
        }
        if( rc==SQLITE_OK ){
          rc = doltliteSetSessionConstraintViolationsCatalog(
              db, &savedState.sessionConstraintViolationsCatalog);
        }
        if( rc==SQLITE_OK ){
          rc = doltlitePersistWorkingSet(db);
        }
        doltliteTxnStateClear(&savedState);
        if( rc!=SQLITE_OK ){
          sqlite3_result_error_code(context, rc);
        }else{
          sqlite3_result_error(context,
            "Committing this transaction resulted in a working set with "
            "constraint violations, transaction rolled back.", -1);
        }
        break;
      case DOLTLITE_VC_TXN_NESTED_SAVEPOINT:
        rc = doltliteRestoreTxnState(db, &savedState);
        if( rc==SQLITE_OK ){
          rc = doltliteSetSessionMergeState(
              db, savedState.sessionIsMerging,
              &savedState.sessionMergeCommit,
              &savedState.sessionConflictsCatalog);
        }
        if( rc==SQLITE_OK ){
          rc = doltliteSetSessionConstraintViolationsCatalog(
              db, &savedState.sessionConstraintViolationsCatalog);
        }
        doltliteTxnStateClear(&savedState);
        if( rc!=SQLITE_OK ){
          sqlite3_result_error_code(context, rc);
        }else{
          sqlite3_result_error(context,
            "Merge aborted: would have introduced constraint violations. "
            "The merge and the would-be violations have been rolled back "
            "with the enclosing savepoint, so dolt_constraint_violations "
            "is empty. To inspect the violations, re-run the merge inside "
            "a plain BEGIN/COMMIT transaction (no SAVEPOINT) so the "
            "violations are preserved instead of rolled back.",
            -1);
        }
        break;
      case DOLTLITE_VC_TXN_PLAIN:
        rc = doltliteReportConstraintViolations(db, context, "Merge");
        if( rc!=SQLITE_OK ){
          sqlite3_result_error_code(context,
              doltliteRestoreTxnStateOnFailure(db, &savedState, rc));
          return;
        }
        doltliteTxnStateClear(&savedState);
        break;
      }
      return;
    }
  }

  if( nMergeConflicts > 0 ){
    if( graphLocked ){
      chunkStoreUnlock(cs);
      graphLocked = 0;
    }
    switch( doltliteVcTxnMode(db) ){
    case DOLTLITE_VC_TXN_AUTOCOMMIT_LIKE:
      rc = doltliteRollbackAutocommitConflict(db, context, &savedState);
      if( rc!=SQLITE_OK ){
        sqlite3_result_error_code(context, rc);
      }
      return;
    case DOLTLITE_VC_TXN_NESTED_SAVEPOINT:
      rc = doltliteRestoreTxnState(db, &savedState);
      if( rc==SQLITE_OK ){
        rc = doltliteSetSessionMergeState(
            db, savedState.sessionIsMerging,
            &savedState.sessionMergeCommit,
            &savedState.sessionConflictsCatalog);
      }
      if( rc==SQLITE_OK ){
        rc = doltliteSetSessionConstraintViolationsCatalog(
            db, &savedState.sessionConstraintViolationsCatalog);
      }
      doltliteTxnStateClear(&savedState);
      if( rc!=SQLITE_OK ){
        sqlite3_result_error_code(context, rc);
      }else{
        char msg[256];
        sqlite3_snprintf(sizeof(msg), msg,
          "Merge has %d conflict(s). Resolve and then commit with dolt_commit.",
          nMergeConflicts);
        sqlite3_result_error(context, msg, -1);
      }
      return;
    case DOLTLITE_VC_TXN_PLAIN:
      rc = doltliteReportConflicts(db, context, nMergeConflicts, "Merge");
      if( rc!=SQLITE_OK ){
        sqlite3_result_error_code(context,
            doltliteRestoreTxnStateOnFailure(db, &savedState, rc));
        return;
      }
      doltliteTxnStateClear(&savedState);
      break;
    }
  }else{
    ProllyHash commitHash;
    char hexBuf[PROLLY_HASH_SIZE*2+1];
    char msg[256];

    rc = doltliteSetSessionStaged(db, &mergedCatHash);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context,
          doltliteRestoreTxnStateOnFailure(db, &savedState, rc));
      return;
    }

    if( zMessage && zMessage[0] ){
      sqlite3_snprintf(sizeof(msg), msg, "%s", zMessage);
    }else{
      snprintf(msg, sizeof(msg), "Merge branch '%s' into %s",
               zBranch, doltliteGetSessionBranch(db));
    }
    rc = doltliteCreateAndStoreCommit(db, &ourHead, &mergedCatHash,
        msg, NULL, NULL, &theirHead, 1, &commitHash);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(context, "failed to create merge commit", -1);
      return;
    }

    /* Re-confirm under the lock right before advancing; the merge's first
    ** confirm is staled by intervening lock-cycling SQL (ANALYZE, etc.). */
    rc = doltliteRefreshAndConfirmHead(db, cs, &ourHead);
    if( rc==SQLITE_BUSY ){
      sqlite3_result_error(context,
        "merge conflict: another connection committed to this branch. Please retry your transaction.",
        -1);
      doltliteRestoreTxnStateOnFailure(db, &savedState, rc);
      return;
    }
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context,
          doltliteRestoreTxnStateOnFailure(db, &savedState, rc));
      return;
    }
    graphLocked = 1;

    rc = doltliteAdvanceBranch(db, &commitHash, &mergedCatHash, 0);
    if( graphLocked ){
      chunkStoreUnlock(cs);
      graphLocked = 0;
    }
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context,
          doltliteRestoreTxnStateOnFailure(db, &savedState, rc));
      return;
    }
    rc = doltliteVcSealActiveSavepoints(db);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      return;
    }
    doltliteTxnStateClear(&savedState);

    doltliteHashToHex(&commitHash, hexBuf);
    sqlite3_result_text(context, hexBuf, -1, SQLITE_TRANSIENT);
  }
}


int doltliteMergeCmdRegister(sqlite3 *db){
  return sqlite3_create_function(db, "dolt_merge", -1,
                                 DOLTLITE_COMMAND_FUNC_FLAGS, 0,
                                 doltliteMergeFunc, 0, 0);
}

#endif
