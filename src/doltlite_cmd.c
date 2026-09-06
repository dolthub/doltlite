#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_internal.h"

#include <string.h>

static DoltliteCmdOption *cmdFindLongOption(
  DoltliteCmdOption *aOption,
  int nOption,
  const char *zName,
  int nName
){
  int i;
  for(i=0; i<nOption; i++){
    if( aOption[i].zLong
     && sqlite3Strlen30(aOption[i].zLong)==nName
     && memcmp(aOption[i].zLong, zName, (size_t)nName)==0
    ){
      return &aOption[i];
    }
  }
  return 0;
}

static DoltliteCmdOption *cmdFindShortOption(
  DoltliteCmdOption *aOption,
  int nOption,
  char name
){
  int i;
  for(i=0; i<nOption; i++){
    if( aOption[i].shortName==name ) return &aOption[i];
  }
  return 0;
}

static int cmdValueText(
  sqlite3_context *ctx,
  sqlite3_value *pValue,
  const char **pzValue
){
  const char *zValue;
  int eType = sqlite3_value_type(pValue);
  int nValue;
  *pzValue = 0;
  zValue = (const char*)sqlite3_value_text(pValue);
  if( !zValue ){
    if( eType!=SQLITE_NULL ){
      sqlite3_result_error_nomem(ctx);
      return SQLITE_NOMEM;
    }
    return SQLITE_OK;
  }
  nValue = sqlite3_value_bytes(pValue);
  if( memchr(zValue, 0, (size_t)nValue) ){
    sqlite3_result_error(ctx,
        "command arguments may not contain NUL bytes", -1);
    return SQLITE_ERROR;
  }
  *pzValue = zValue;
  return SQLITE_OK;
}

static int cmdSetOption(
  sqlite3_context *ctx,
  DoltliteCmdOption *pOption,
  const char *zOpt,
  const char *zAttached,
  int argc,
  sqlite3_value **argv,
  int *pI
){
  const char *zValue;
  int rc;
  if( pOption->eType==DOLTLITE_CMD_OPTION_FLAG ){
    if( pOption->pSeen ) *pOption->pSeen = 1;
    return SQLITE_OK;
  }
  if( zAttached && zAttached[0] ){
    zValue = zAttached;
  }else if( *pI+1<argc ){
    rc = cmdValueText(ctx, argv[++*pI], &zValue);
    if( rc!=SQLITE_OK ) return rc;
  }else{
    doltliteCmdResultMissingOptionValue(ctx, zOpt);
    return SQLITE_ERROR;
  }
  if( !zValue ){
    doltliteCmdResultMissingOptionValue(ctx, zOpt);
    return SQLITE_ERROR;
  }
  if( pOption->pSeen ) *pOption->pSeen = 1;
  if( pOption->pzValue ) *pOption->pzValue = zValue;
  return SQLITE_OK;
}

void doltliteCmdArgsClear(DoltliteCmdArgs *pArgs){
  if( !pArgs ) return;
  sqlite3_free(pArgs->apPositional);
  sqlite3_free((void*)pArgs->azPositional);
  memset(pArgs, 0, sizeof(*pArgs));
}

int doltliteCmdParseArgs(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv,
  DoltliteCmdOption *aOption,
  int nOption,
  int flags,
  DoltliteCmdArgs *pArgs
){
  int endOptions = 0;
  int i;
  memset(pArgs, 0, sizeof(*pArgs));
  for(i=0; i<nOption; i++){
    if( aOption[i].pSeen ) *aOption[i].pSeen = 0;
    if( aOption[i].pzValue ) *aOption[i].pzValue = 0;
  }
  if( argc>0 ){
    pArgs->apPositional = sqlite3_malloc64((sqlite3_uint64)argc * sizeof(*argv));
    pArgs->azPositional = sqlite3_malloc64(
        (sqlite3_uint64)argc * sizeof(*pArgs->azPositional));
    if( !pArgs->apPositional || !pArgs->azPositional ){
      doltliteCmdArgsClear(pArgs);
      sqlite3_result_error_nomem(ctx);
      return SQLITE_NOMEM;
    }
  }
  for(i=0; i<argc; i++){
    const char *zArg;
    DoltliteCmdOption *pOption = 0;
    int rc;
    rc = cmdValueText(ctx, argv[i], &zArg);
    if( rc!=SQLITE_OK ){
      doltliteCmdArgsClear(pArgs);
      return rc;
    }
    if( !zArg ){
      if( flags & DOLTLITE_CMD_PARSE_IGNORE_NULLS ) continue;
      sqlite3_result_error(ctx, "invalid empty argument", -1);
      doltliteCmdArgsClear(pArgs);
      return SQLITE_ERROR;
    }
    if( !endOptions && strcmp(zArg, "--")==0 ){
      endOptions = 1;
      continue;
    }
    if( !endOptions && zArg[0]=='-' && zArg[1]=='-' && zArg[2] ){
      const char *zName = zArg + 2;
      const char *zEquals = strchr(zName, '=');
      int nName = zEquals ? (int)(zEquals-zName) : sqlite3Strlen30(zName);
      pOption = cmdFindLongOption(aOption, nOption, zName, nName);
      if( !pOption || (zEquals && pOption->eType!=DOLTLITE_CMD_OPTION_VALUE) ){
        doltliteCmdResultUnknownOption(ctx, zArg);
        doltliteCmdArgsClear(pArgs);
        return SQLITE_ERROR;
      }
      rc = cmdSetOption(ctx, pOption, pOption->zLong,
                        zEquals ? zEquals+1 : 0, argc, argv, &i);
      if( rc!=SQLITE_OK ){
        doltliteCmdArgsClear(pArgs);
        return rc;
      }
      continue;
    }
    if( !endOptions && zArg[0]=='-' && zArg[1] && zArg[1]!='-' ){
      int j;
      int nShort = (flags & DOLTLITE_CMD_PARSE_SHORT_GROUPS)
                 ? sqlite3Strlen30(zArg)-1 : 1;
      if( !(flags & DOLTLITE_CMD_PARSE_SHORT_GROUPS) && zArg[2] ){
        doltliteCmdResultUnknownOption(ctx, zArg);
        doltliteCmdArgsClear(pArgs);
        return SQLITE_ERROR;
      }
      for(j=1; j<=nShort; j++){
        char zOpt[2] = { zArg[j], 0 };
        pOption = cmdFindShortOption(aOption, nOption, zArg[j]);
        if( !pOption ){
          char zUnknown[3] = { '-', zArg[j], 0 };
          doltliteCmdResultUnknownOption(ctx, zUnknown);
          doltliteCmdArgsClear(pArgs);
          return SQLITE_ERROR;
        }
        rc = cmdSetOption(ctx, pOption,
            pOption->zLong ? pOption->zLong : zOpt,
            pOption->eType==DOLTLITE_CMD_OPTION_VALUE ? zArg+j+1 : 0,
            argc, argv, &i);
        if( rc!=SQLITE_OK ){
          doltliteCmdArgsClear(pArgs);
          return rc;
        }
        if( pOption->eType==DOLTLITE_CMD_OPTION_VALUE ) break;
      }
      continue;
    }
    pArgs->apPositional[pArgs->nPositional] = argv[i];
    pArgs->azPositional[pArgs->nPositional++] = zArg;
  }
  return SQLITE_OK;
}

int doltliteCmdRejectDetached(sqlite3_context *ctx){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  if( !doltliteIsDetached(db) ) return 0;
  sqlite3_result_error(ctx,
      "this operation is not supported while in a detached head state", -1);
  return 1;
}

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

int doltliteCmdParseAuthor(
  sqlite3_context *ctx,
  const char *zAuthor,
  char **pzName,
  char **pzEmail
){
  const char *z = zAuthor;
  if( pzName ) *pzName = 0;
  if( pzEmail ) *pzEmail = 0;
  if( !z || !z[0] ){
    sqlite3_result_error(ctx, "Option 'author' requires a value", -1);
    return SQLITE_ERROR;
  }
  while( 1 ){
    const char *zEnd = strchr(z, ')');
    const char *zSep = 0;
    const char *p;
    char *zName;
    char *zEmail;
    int nEmail;
    int i;
    int j;
    if( !zEnd ) zEnd = z + strlen(z);
    for(p=z+1; p+2<zEnd; p++){
      if( p[0]==' ' && p[1]=='<' ) zSep = p;
    }
    if( zSep ){
      if( !pzName || !pzEmail ) return SQLITE_OK;
      zName = sqlite3_mprintf("%.*s", (int)(zSep-z), z);
      nEmail = (int)(zEnd-zSep-2);
      zEmail = sqlite3_malloc64((sqlite3_uint64)nEmail+1);
      if( !zName || !zEmail ){
        sqlite3_free(zName);
        sqlite3_free(zEmail);
        sqlite3_result_error_nomem(ctx);
        return SQLITE_NOMEM;
      }
      for(i=0, j=0; i<nEmail; i++){
        if( zSep[i+2]!='>' ) zEmail[j++] = zSep[i+2];
      }
      zEmail[j] = 0;
      *pzName = zName;
      *pzEmail = zEmail;
      return SQLITE_OK;
    }
    if( !zEnd[0] ) break;
    z = zEnd + 1;
  }
  sqlite3_result_error(ctx,
    "Author not formatted correctly. Use 'Name <author@example.com>' format",
    -1);
  return SQLITE_ERROR;
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

static int markPendingReplayIfNotMerging(sqlite3 *db){
  u8 isMerging = 0;
  doltliteGetSessionMergeState(db, &isMerging, 0, 0);
  if( isMerging ) return SQLITE_OK;
  return doltliteSetSessionPendingReplayCommit(db, 1);
}

static int persistPendingAndError(
  sqlite3 *db,
  sqlite3_context *ctx,
  int bRegisterConflicts,
  const char *zMsg
){
  int rc;
  if( bRegisterConflicts ){
    rc = doltliteRegisterConflictTables(db);
    if( rc!=SQLITE_OK ) return rc;
  }
  rc = doltlitePersistOrSaveWorkingSet(db);
  if( rc!=SQLITE_OK ) return rc;
  rc = markPendingReplayIfNotMerging(db);
  if( rc!=SQLITE_OK ) return rc;
  sqlite3_result_error(ctx, zMsg, -1);
  return SQLITE_OK;
}

static int cmdReportConflicts(
  sqlite3 *db,
  sqlite3_context *ctx,
  int nConflicts,
  const char *zOp
){
  char msg[256];
  sqlite3_snprintf(sizeof(msg), msg,
    "%s has %d conflict(s). Resolve and then commit with dolt_commit.",
    zOp, nConflicts);
  return persistPendingAndError(db, ctx, 1, msg);
}

static int cmdReportConstraintViolations(
  sqlite3 *db,
  sqlite3_context *ctx,
  const char *zOp
){
  char msg[256];
  sqlite3_snprintf(sizeof(msg), msg,
    "%s resulted in constraint violations. Resolve the rows in "
    "dolt_constraint_violations and then commit with dolt_commit.",
    zOp);
  return persistPendingAndError(db, ctx, 0, msg);
}

static int cmdReportConflictsAndConstraintViolations(
  sqlite3 *db,
  sqlite3_context *ctx,
  int nConflicts,
  const char *zOp
){
  char msg[384];
  sqlite3_snprintf(sizeof(msg), msg,
    "%s has %d conflict(s) and constraint violations. Resolve "
    "dolt_conflicts and dolt_constraint_violations, then commit with "
    "dolt_commit.",
    zOp ? zOp : "Operation", nConflicts);
  return persistPendingAndError(db, ctx, 1, msg);
}

static int cmdFinishPlainAfterReport(
  sqlite3 *db,
  sqlite3_context *ctx,
  DoltliteTxnState *pSaved,
  int reportRc,
  int bSealOnPlain
){
  if( reportRc!=SQLITE_OK ){
    sqlite3_result_error_code(ctx,
        doltliteRestoreTxnStateOnFailure(db, pSaved, reportRc));
    return reportRc;
  }
  if( bSealOnPlain ){
    int rc = doltliteVcSealActiveSavepoints(db);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(ctx,
          doltliteRestoreTxnStateOnFailure(db, pSaved, rc));
      return rc;
    }
  }
  doltliteTxnStateClear(pSaved);
  return SQLITE_OK;
}

static int cmdRollbackAutocommitConflict(
  sqlite3 *db,
  sqlite3_context *ctx,
  DoltliteTxnState *pSaved
){
  int rc;
  int hadTopLevelSavepoint = db->pSavepoint!=0 && db->nSavepoint==0;
  sqlite3RollbackAll(db, SQLITE_OK);
  rc = doltliteRestoreTxnState(db, pSaved);
  /* Restore merge markers even if the catalog restore failed; pSaved is
  ** cleared below, and a leftover isMerging would block the next merge.
  ** Keep the first error. */
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
    sqlite3_result_error(ctx,
      "cannot merge: conflicts detected, autocommit transaction rolled back. "
      "Run the merge inside BEGIN/COMMIT to inspect dolt_conflicts and "
      "dolt_schema_conflicts, resolve with dolt_conflicts_resolve(), then commit "
      "with dolt_commit(). Conflicts are never committed as conflicts",
      -1);
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
    rc = cmdRollbackAutocommitConflict(db, ctx, pSaved);
    if( rc!=SQLITE_OK ) sqlite3_result_error_code(ctx, rc);
    return rc==SQLITE_OK ? SQLITE_OK : rc;
  case DOLTLITE_VC_TXN_PLAIN:
    return cmdFinishPlainAfterReport(db, ctx, pSaved,
        cmdReportConflicts(db, ctx, nConflicts, zOp), bSealOnPlain);
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
    return cmdFinishPlainAfterReport(db, ctx, pSaved,
        cmdReportConstraintViolations(db, ctx, zOp), bSealOnPlain);
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
    return cmdFinishPlainAfterReport(db, ctx, pSaved,
        cmdReportConflictsAndConstraintViolations(
            db, ctx, nConflicts, zOp),
        bSealOnPlain);
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

int doltliteCmdSourceResultError(
  sqlite3_context *ctx,
  ChunkStore *cs,
  int *pRc
){
  int pendingRc = SQLITE_OK;
  char *zErr = chunkStoreSourceTakeError(cs, &pendingRc);
  if( !zErr && pendingRc==SQLITE_OK ) return 0;
  if( zErr ) sqlite3_result_error(ctx, zErr, -1);
  if( pendingRc!=SQLITE_OK ) *pRc = pendingRc;
  sqlite3_result_error_code(ctx, *pRc);
  sqlite3_free(zErr);
  return 1;
}

int doltliteVtabMapChunkSourceError(
  sqlite3_vtab *pVtab,
  sqlite3 *db,
  int sourceRc,
  int mappedRc
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  int pendingRc = SQLITE_OK;
  char *zErr = cs ? chunkStoreSourceTakeError(cs, &pendingRc) : 0;
  if( !zErr && pendingRc==SQLITE_OK ) return mappedRc;
  if( zErr ){
    sqlite3_free(pVtab->zErrMsg);
    pVtab->zErrMsg = zErr;
  }
  return pendingRc!=SQLITE_OK ? pendingRc : sourceRc;
}

#endif
