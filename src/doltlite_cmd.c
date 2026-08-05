#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_internal.h"

#include <string.h>

/*
** Shared scaffolding for dolt_* SQL command functions: argv option errors,
** peer-branch BUSY messages, and the three-way VC txn outcome switch for
** merge conflicts / constraint violations.
*/

void doltliteCmdResultUnknownOption(sqlite3_context *ctx, const char *zOpt){
  char *zErr = sqlite3_mprintf("unknown option `%s`", zOpt ? zOpt : "");
  if( zErr ){
    sqlite3_result_error(ctx, zErr, -1);
    sqlite3_free(zErr);
  }else{
    sqlite3_result_error_nomem(ctx);
  }
}

void doltliteCmdResultMissingOptionValue(
  sqlite3_context *ctx,
  const char *zOptName
){
  char *zErr = sqlite3_mprintf("no value for option `%s'",
                               zOptName ? zOptName : "");
  if( zErr ){
    sqlite3_result_error(ctx, zErr, -1);
    sqlite3_free(zErr);
  }else{
    sqlite3_result_error_nomem(ctx);
  }
}

const char *doltliteCmdTakeValueArg(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv,
  int *pI,
  const char *zOptName
){
  int i = *pI;
  if( i+1>=argc ){
    doltliteCmdResultMissingOptionValue(ctx, zOptName);
    return 0;
  }
  *pI = i+1;
  return (const char*)sqlite3_value_text(argv[i+1]);
}

void doltliteCmdResultPeerBranchBusy(sqlite3_context *ctx, const char *zOp){
  char *zErr = sqlite3_mprintf(
    "%s conflict: another connection committed to this branch. "
    "Please retry your transaction.",
    zOp ? zOp : "branch");
  if( zErr ){
    sqlite3_result_error(ctx, zErr, -1);
    sqlite3_free(zErr);
  }else{
    sqlite3_result_error_nomem(ctx);
  }
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

int doltliteReportConflictsAndConstraintViolations(
  sqlite3 *db,
  sqlite3_context *ctx,
  int nConflicts,
  const char *zOp
){
  char msg[384];
  int rc;
  rc = doltliteRegisterConflictTables(db);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltlitePersistOrSaveWorkingSet(db);
  if( rc!=SQLITE_OK ) return rc;
  sqlite3_snprintf(sizeof(msg), msg,
    "%s has %d conflict(s) and constraint violations. Resolve "
    "dolt_conflicts and dolt_constraint_violations, then commit with "
    "dolt_commit.",
    zOp ? zOp : "Operation", nConflicts);
  sqlite3_result_error(ctx, msg, -1);
  return SQLITE_OK;
}

void doltliteReportAutocommitConflictRollback(sqlite3_context *ctx){
  sqlite3_result_error(ctx,
    "cannot merge: conflicts detected, autocommit transaction rolled back. "
    "Run the merge inside BEGIN/COMMIT to inspect dolt_conflicts and "
    "dolt_schema_conflicts, resolve with dolt_conflicts_resolve(), then commit "
    "with dolt_commit(). Conflicts are never committed as conflicts",
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
  /* Put the merge markers back even when the restore above failed. pSaved is
  ** cleared below, so a skipped restore leaves isMerging set from the merge we
  ** just told the caller was rolled back, and the next merge refuses because
  ** one is already in progress. These are in-memory writes; keep the first
  ** error for the return value rather than the last. */
  {
    int rc2 = doltliteSetSessionMergeState(db, pSaved->sessionIsMerging,
                                          &pSaved->sessionMergeCommit,
                                          &pSaved->sessionConflictsCatalog);
    if( rc==SQLITE_OK ) rc = rc2;
    rc2 = doltliteSetSessionConstraintViolationsCatalog(
        db, &pSaved->sessionConstraintViolationsCatalog);
    if( rc==SQLITE_OK ) rc = rc2;
  }
  if( rc==SQLITE_OK ){
    rc = doltlitePersistWorkingSet(db);
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

static int cmdRollbackAutocommitToSaved(
  sqlite3 *db,
  DoltliteTxnState *pSaved
){
  int rc = doltliteHardReset(db, &pSaved->sessionCatalogHash);
  if( rc==SQLITE_OK ){
    rc = doltliteSetSessionBranch(db, pSaved->zSessionBranch);
  }
  if( rc==SQLITE_OK ){
    doltliteSetSessionHead(db, &pSaved->sessionHead);
    rc = doltliteSetSessionStaged(db, &pSaved->sessionStaged);
  }
  if( rc==SQLITE_OK ){
    rc = doltliteSetSessionMergeState(
        db, pSaved->sessionIsMerging,
        &pSaved->sessionMergeCommit,
        &pSaved->sessionConflictsCatalog);
  }
  if( rc==SQLITE_OK ){
    rc = doltliteSetSessionConstraintViolationsCatalog(
        db, &pSaved->sessionConstraintViolationsCatalog);
  }
  if( rc==SQLITE_OK ){
    rc = doltlitePersistWorkingSet(db);
  }
  doltliteTxnStateClear(pSaved);
  return rc;
}

static int cmdRollbackAutocommitConstraintViolations(
  sqlite3 *db,
  sqlite3_context *ctx,
  DoltliteTxnState *pSaved
){
  int rc = cmdRollbackAutocommitToSaved(db, pSaved);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(ctx, rc);
    return rc;
  }
  sqlite3_result_error(ctx,
    "Committing this transaction resulted in a working set with "
    "constraint violations, transaction rolled back.", -1);
  return SQLITE_OK;
}

int doltliteCmdFinishWithConflicts(
  sqlite3 *db,
  sqlite3_context *ctx,
  DoltliteTxnState *pSaved,
  int nConflicts,
  const char *zOp,
  int bSealOnPlain
){
  int rc;
  switch( doltliteVcTxnMode(db) ){
  case DOLTLITE_VC_TXN_AUTOCOMMIT_LIKE:
    rc = doltliteRollbackAutocommitConflict(db, ctx, pSaved);
    if( rc!=SQLITE_OK ) sqlite3_result_error_code(ctx, rc);
    return rc==SQLITE_OK ? SQLITE_OK : rc;
  case DOLTLITE_VC_TXN_PLAIN:
    rc = doltliteReportConflicts(db, ctx, nConflicts, zOp);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(ctx,
          doltliteRestoreTxnStateOnFailure(db, pSaved, rc));
      return rc;
    }
    if( bSealOnPlain ){
      rc = doltliteVcSealActiveSavepoints(db);
      if( rc!=SQLITE_OK ){
        sqlite3_result_error_code(ctx,
            doltliteRestoreTxnStateOnFailure(db, pSaved, rc));
        return rc;
      }
    }
    doltliteTxnStateClear(pSaved);
    return SQLITE_OK;
  case DOLTLITE_VC_TXN_NESTED_SAVEPOINT:
    rc = doltliteRestoreTxnStateOnFailure(db, pSaved, SQLITE_OK);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(ctx, rc);
      return rc;
    }
    {
      char msg[256];
      sqlite3_snprintf(sizeof(msg), msg,
        "%s has %d conflict(s). Resolve and then commit with dolt_commit.",
        zOp ? zOp : "Operation", nConflicts);
      sqlite3_result_error(ctx, msg, -1);
    }
    return SQLITE_OK;
  }
  return SQLITE_OK;
}

int doltliteCmdFinishWithConstraintViolations(
  sqlite3 *db,
  sqlite3_context *ctx,
  DoltliteTxnState *pSaved,
  const char *zOp,
  int bSealOnPlain,
  const char *zNestedMsg
){
  int rc;
  switch( doltliteVcTxnMode(db) ){
  case DOLTLITE_VC_TXN_AUTOCOMMIT_LIKE:
    return cmdRollbackAutocommitConstraintViolations(db, ctx, pSaved);
  case DOLTLITE_VC_TXN_PLAIN:
    rc = doltliteReportConstraintViolations(db, ctx, zOp);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(ctx,
          doltliteRestoreTxnStateOnFailure(db, pSaved, rc));
      return rc;
    }
    if( bSealOnPlain ){
      rc = doltliteVcSealActiveSavepoints(db);
      if( rc!=SQLITE_OK ){
        sqlite3_result_error_code(ctx,
            doltliteRestoreTxnStateOnFailure(db, pSaved, rc));
        return rc;
      }
    }
    doltliteTxnStateClear(pSaved);
    return SQLITE_OK;
  case DOLTLITE_VC_TXN_NESTED_SAVEPOINT:
    rc = doltliteRestoreTxnStateOnFailure(db, pSaved, SQLITE_OK);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(ctx, rc);
      return rc;
    }
    if( zNestedMsg ){
      sqlite3_result_error(ctx, zNestedMsg, -1);
    }else{
      sqlite3_result_error(ctx,
        "Merge aborted: would have introduced constraint violations. "
        "The merge and the would-be violations have been rolled back "
        "with the enclosing savepoint, so dolt_constraint_violations "
        "is empty. Re-run the operation outside a nested SAVEPOINT to "
        "inspect the violations in dolt_constraint_violations.",
        -1);
    }
    return SQLITE_OK;
  }
  return SQLITE_OK;
}

static int cmdRollbackAutocommitConflictsAndCVs(
  sqlite3 *db,
  sqlite3_context *ctx,
  DoltliteTxnState *pSaved,
  int nConflicts,
  const char *zOp
){
  char msg[384];
  int rc = cmdRollbackAutocommitToSaved(db, pSaved);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(ctx, rc);
    return rc;
  }
  sqlite3_snprintf(sizeof(msg), msg,
    "%s has %d conflict(s) and constraint violations; transaction "
    "rolled back. Run inside BEGIN/COMMIT to inspect dolt_conflicts and "
    "dolt_constraint_violations.",
    zOp ? zOp : "Operation", nConflicts);
  sqlite3_result_error(ctx, msg, -1);
  return SQLITE_OK;
}

int doltliteCmdFinishWithConflictsAndConstraintViolations(
  sqlite3 *db,
  sqlite3_context *ctx,
  DoltliteTxnState *pSaved,
  int nConflicts,
  const char *zOp,
  int bSealOnPlain,
  const char *zNestedMsg
){
  int rc;
  if( nConflicts<=0 ){
    return doltliteCmdFinishWithConstraintViolations(
        db, ctx, pSaved, zOp, bSealOnPlain, zNestedMsg);
  }
  switch( doltliteVcTxnMode(db) ){
  case DOLTLITE_VC_TXN_AUTOCOMMIT_LIKE:
    return cmdRollbackAutocommitConflictsAndCVs(
        db, ctx, pSaved, nConflicts, zOp);
  case DOLTLITE_VC_TXN_PLAIN:
    rc = doltliteReportConflictsAndConstraintViolations(
        db, ctx, nConflicts, zOp);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(ctx,
          doltliteRestoreTxnStateOnFailure(db, pSaved, rc));
      return rc;
    }
    if( bSealOnPlain ){
      rc = doltliteVcSealActiveSavepoints(db);
      if( rc!=SQLITE_OK ){
        sqlite3_result_error_code(ctx,
            doltliteRestoreTxnStateOnFailure(db, pSaved, rc));
        return rc;
      }
    }
    doltliteTxnStateClear(pSaved);
    return SQLITE_OK;
  case DOLTLITE_VC_TXN_NESTED_SAVEPOINT:
    rc = doltliteRestoreTxnStateOnFailure(db, pSaved, SQLITE_OK);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(ctx, rc);
      return rc;
    }
    if( zNestedMsg ){
      sqlite3_result_error(ctx, zNestedMsg, -1);
    }else{
      char msg[384];
      sqlite3_snprintf(sizeof(msg), msg,
        "%s has %d conflict(s) and constraint violations. The merge and "
        "would-be violations have been rolled back with the enclosing "
        "savepoint. Re-run inside BEGIN/COMMIT (no nested SAVEPOINT) to "
        "inspect dolt_conflicts and dolt_constraint_violations.",
        zOp ? zOp : "Operation", nConflicts);
      sqlite3_result_error(ctx, msg, -1);
    }
    return SQLITE_OK;
  }
  return SQLITE_OK;
}

#endif
