#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "vdbeInt.h"
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



static int revertTouchesTable(
  struct TableEntry *aCommit, int nCommit,
  struct TableEntry *aParent, int nParent,
  SchemaEntry *aCommitSchema, int nCommitSchema,
  SchemaEntry *aParentSchema, int nParentSchema,
  const char *zName
){
  struct TableEntry *pInCommit;
  struct TableEntry *pInParent;
  if( !zName ) return 0;
  pInCommit = doltliteFindTableByName(aCommit, nCommit, zName);
  pInParent = doltliteFindTableByName(aParent, nParent, zName);
  if( doltliteTableEntryDiffers(pInCommit, pInParent) ) return 1;
  return doltliteIndexSchemaRowsDifferForTable(
      aCommitSchema, nCommitSchema, aParentSchema, nParentSchema, zName);
}

static int doltliteRevertCheckDirty(
  sqlite3 *db,
  const ProllyHash *pCommitCat,
  const ProllyHash *pParentCat,
  int *pConflict
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHash headCatHash, stagedHash, workingCatHash;
  u8 *wBuf = 0; int nWBuf = 0;
  struct TableEntry *aCommit = 0, *aParent = 0;
  struct TableEntry *aWorking = 0, *aCompare = 0;
  SchemaEntry *aCommitSchema = 0, *aParentSchema = 0;
  int nCommit = 0, nParent = 0;
  int nWorking = 0, nCompare = 0;
  int nCommitSchema = 0, nParentSchema = 0;
  int i, rc;

  *pConflict = 0;

  if( !cs ) return SQLITE_OK;

  rc = doltliteGetHeadCatalogHash(db, &headCatHash);
  if( rc!=SQLITE_OK ) return rc;

  doltliteGetSessionStaged(db, &stagedHash);
  if( !prollyHashIsEmpty(&stagedHash)
   && prollyHashCompare(&headCatHash, &stagedHash)!=0 ){
    *pConflict = 1;
    return SQLITE_OK;
  }

  rc = doltliteFlushAndSerializeCatalog(db, &wBuf, &nWBuf);
  if( rc!=SQLITE_OK ) return rc;
  rc = chunkStorePut(cs, wBuf, nWBuf, &workingCatHash);
  sqlite3_free(wBuf);
  if( rc!=SQLITE_OK ) return rc;

  if( prollyHashCompare(&headCatHash, &workingCatHash)==0 ) return SQLITE_OK;

  rc = doltliteLoadCatalog(db, pCommitCat, &aCommit, &nCommit, 0);
  if( rc!=SQLITE_OK ) goto done;
  rc = doltliteLoadCatalog(db, pParentCat, &aParent, &nParent, 0);
  if( rc!=SQLITE_OK ) goto done;
  rc = doltliteLoadCatalog(db, &workingCatHash, &aWorking, &nWorking, 0);
  if( rc!=SQLITE_OK ) goto done;
  if( !prollyHashIsEmpty(&headCatHash) ){
    rc = doltliteLoadCatalog(db, &headCatHash, &aCompare, &nCompare, 0);
    if( rc!=SQLITE_OK ) goto done;
  }
  rc = loadSchemaFromCatalog(db, cs, doltliteGetCache(db), pCommitCat,
                             &aCommitSchema, &nCommitSchema);
  if( rc!=SQLITE_OK ) goto done;
  rc = loadSchemaFromCatalog(db, cs, doltliteGetCache(db), pParentCat,
                             &aParentSchema, &nParentSchema);
  if( rc!=SQLITE_OK ) goto done;

  for(i=0; i<nWorking; i++){
    struct TableEntry *pCmp;
    if( !aWorking[i].zName ) continue;
    pCmp = doltliteFindTableByName(aCompare, nCompare, aWorking[i].zName);
    if( !doltliteTableEntryDiffers(pCmp, &aWorking[i]) ) continue;
    if( revertTouchesTable(aCommit, nCommit, aParent, nParent,
                           aCommitSchema, nCommitSchema,
                           aParentSchema, nParentSchema,
                           aWorking[i].zName) ){
      *pConflict = 1;
      goto done;
    }
  }
  for(i=0; i<nCompare; i++){
    if( !aCompare[i].zName ) continue;
    if( doltliteFindTableByName(aWorking, nWorking, aCompare[i].zName) ) continue;
    if( revertTouchesTable(aCommit, nCommit, aParent, nParent,
                           aCommitSchema, nCommitSchema,
                           aParentSchema, nParentSchema,
                           aCompare[i].zName) ){
      *pConflict = 1;
      goto done;
    }
  }

done:
  doltliteFreeCatalog(aCommit, nCommit);
  doltliteFreeCatalog(aParent, nParent);
  doltliteFreeCatalog(aWorking, nWorking);
  doltliteFreeCatalog(aCompare, nCompare);
  freeSchemaEntries(aCommitSchema, nCommitSchema);
  freeSchemaEntries(aParentSchema, nParentSchema);
  return rc;
}

static int doltliteRevertOne(
  sqlite3_context *context, const char *zRef, const ProllyHash *pRevert,
  const char *zAuthorName, const char *zAuthorEmail
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHash ourHead, liveOurCatalog;
  DoltliteCommit revertCommit, parentCommit, ourCommit;
  int nConflicts = 0;
  int rc;
  char *zApplyErr = 0;
  char hexBuf[PROLLY_HASH_SIZE*2+1];
  memset(&revertCommit, 0, sizeof(revertCommit));
  memset(&parentCommit, 0, sizeof(parentCommit));
  memset(&ourCommit, 0, sizeof(ourCommit));
  rc = doltliteLoadHeadAndParentedCommit(
    db, pRevert,
    &ourHead, &revertCommit, &parentCommit, &ourCommit
  );
  if( doltliteCmdReportLoadParentedCommitError(
        context, cs, rc, &revertCommit, &parentCommit, &ourCommit,
        "cannot revert the initial commit") ){
    return rc;
  }

  {
    int dirtyConflict = 0;
    rc = doltliteRevertCheckDirty(db,
        &revertCommit.catalogHash, &parentCommit.catalogHash,
        &dirtyConflict);
    if( rc!=SQLITE_OK ) goto revert_error;
    if( dirtyConflict ){
      doltliteCommitClear(&revertCommit);
      doltliteCommitClear(&parentCommit);
      doltliteCommitClear(&ourCommit);
      sqlite3_result_error(context,
        "Your local changes would be overwritten by revert.\n"
        "hint: Please commit your changes before you revert.", -1);
      return SQLITE_ERROR;
    }
  }
  rc = doltliteFlushCatalogToHash(db, &liveOurCatalog);
  if( rc!=SQLITE_OK ) goto revert_error;

  {
    char msg[512];
    /* Commit from HEAD, not the working catalog, so unrelated dirty changes stay uncommitted. */
    const ProllyHash *pCommitOurs =
        prollyHashCompare(&liveOurCatalog, &ourCommit.catalogHash)!=0
          ? &ourCommit.catalogHash : 0;
    sqlite3_snprintf(sizeof(msg), msg, "Revert \"%s\"",
                     revertCommit.zMessage ? revertCommit.zMessage : zRef);

    rc = applyMergedCatalogAndCommit(db, context,
        &revertCommit.catalogHash, &liveOurCatalog,
        &parentCommit.catalogHash, &ourHead, pCommitOurs, msg,
        zAuthorName, zAuthorEmail, 1, 1,
        doltliteCommitParentHash(&revertCommit, 0),
        &nConflicts, 0, &zApplyErr, hexBuf);
  }

  doltliteCommitClear(&revertCommit);
  doltliteCommitClear(&parentCommit);
  doltliteCommitClear(&ourCommit);

  doltliteCmdFinishApplyMerged(
      context, cs, rc, nConflicts, zApplyErr, "revert", zRef,
      "nothing to commit",
      "revert of \"%s\" failed", "revert failed", hexBuf);
  return rc;

revert_error:
  doltliteCommitClear(&revertCommit);
  doltliteCommitClear(&parentCommit);
  doltliteCommitClear(&ourCommit);
  if( !doltliteCmdSourceResultError(context, cs, &rc) ){
    char *zMsg = sqlite3_mprintf("revert of \"%s\" failed", zRef);
    sqlite3_result_error(context, zMsg ? zMsg : "revert failed", -1);
    sqlite3_free(zMsg);
    if( rc!=SQLITE_ERROR && rc!=SQLITE_NOTFOUND ){
      sqlite3_result_error_code(context, rc);
    }
  }
  return rc;
}

static int revertSeriesQueue(
  sqlite3 *db, const ProllyHash *aHashes, int n,
  const ProllyHash *pCatalog, ProllyHash *pQueue
){
  int i, rc = SQLITE_OK;
  memset(pQueue, 0, sizeof(*pQueue));
  for(i=n-1; i>=0 && rc==SQLITE_OK; i--){
    ProllyHash next = *pQueue;
    rc = doltliteCreateAndStoreCommit(db, &aHashes[i], pCatalog,
        "Revert queue", 0, 0, &next, !prollyHashIsEmpty(&next), pQueue);
  }
  return rc;
}

static int revertSeriesRun(
  sqlite3_context *context, DoltliteReplaySeries *pSeries,
  const char *zAuthorName, const char *zAuthorEmail
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  int rc = SQLITE_OK;
  while( !prollyHashIsEmpty(&pSeries->remaining) ){
    DoltliteCommit entry;
    ProllyHash commit;
    const ProllyHash *pNext;
    char ref[PROLLY_HASH_SIZE*2+1];
    memset(&entry, 0, sizeof(entry));
    rc = doltliteLoadCommit(db, &pSeries->remaining, &entry);
    if( rc!=SQLITE_OK ) return rc;
    if( doltliteCommitParentCount(&entry)<1 ){
      doltliteCommitClear(&entry);
      return SQLITE_CORRUPT;
    }
    commit = *doltliteCommitParentHash(&entry, 0);
    pNext = doltliteCommitParentHash(&entry, 1);
    memset(&pSeries->remaining, 0, sizeof(pSeries->remaining));
    if( pNext ) pSeries->remaining = *pNext;
    doltliteCommitClear(&entry);
    rc = doltliteSetSessionReplaySeries(db, pSeries);
    if( rc!=SQLITE_OK ) return rc;
    doltliteHashToHex(&commit, ref);
    rc = doltliteRevertOne(context, ref, &commit, zAuthorName, zAuthorEmail);
    if( doltliteSessionHasPendingReplayCommit(db) ) return rc;
    {
      int clearRc = doltliteSetSessionReplaySeries(db, 0);
      if( clearRc!=SQLITE_OK ) return clearRc;
    }
    if( rc!=SQLITE_OK ) return rc;
    if( !prollyHashIsEmpty(&pSeries->remaining) ){
      rc = doltliteCheckpointCommand(context);
      if( rc!=SQLITE_OK ) return rc;
    }
  }
  return rc;
}

static void doltliteRevertFunc(
  sqlite3_context *context, int argc, sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  ChunkStore *cs = doltliteGetChunkStore(db);
  const char *zAuthor = 0;
  char *zName = 0, *zEmail = 0;
  DoltliteCmdArgs args;
  DoltliteReplaySeries series;
  int isAbort = 0, isContinue = 0;
  DoltliteCmdOption options[] = {
    { "author", 0, DOLTLITE_CMD_OPTION_VALUE, 0, &zAuthor },
    { "abort", 0, DOLTLITE_CMD_OPTION_FLAG, &isAbort, 0 },
    { "continue", 0, DOLTLITE_CMD_OPTION_FLAG, &isContinue, 0 }
  };
  ProllyHash *aHashes = 0;
  int i, rc;
  if( doltliteCmdRejectInWriteStatement(context, "revert") ) return;
  if( doltliteCmdRejectDetached(context) ) return;
  if( doltliteCmdRejectReadOnly(context) ) return;
  if( !cs ){ sqlite3_result_error(context, "no database", -1); return; }
  memset(&series, 0, sizeof(series));
  rc = doltliteCmdParseArgs(context, argc, argv, options, ArraySize(options), 0, &args);
  if( rc!=SQLITE_OK ) return;
  if( isAbort && isContinue ){
    sqlite3_result_error(context, "--continue and --abort are mutually exclusive", -1);
    goto done;
  }
  if( (isAbort || isContinue) && (args.nPositional || (isAbort && zAuthor)) ){
    sqlite3_result_error(context, "replay lifecycle does not take a revision", -1);
    goto done;
  }
  if( !isAbort && !isContinue && !args.nPositional ){
    sqlite3_result_error(context, "nothing specified to revert", -1);
    goto done;
  }
  if( zAuthor ){
    rc = doltliteCmdParseAuthor(context, zAuthor, &zName, &zEmail);
    if( rc!=SQLITE_OK ) goto done;
    if( !zEmail || !zEmail[0] ){
      sqlite3_result_error(context,
          "Aborting commit due to empty author email. Is your config set?", -1);
      goto done;
    }
  }
  if( isAbort || isContinue ){
    doltliteGetSessionReplaySeries(db, &series);
    doltliteReplayFinish(context, DOLTLITE_REPLAY_REVERT, isAbort, zName, zEmail);
    if( !isAbort && !context->isError && !prollyHashIsEmpty(&series.remaining) ){
      rc = doltliteCheckpointCommand(context);
      if( rc==SQLITE_OK ) rc = revertSeriesRun(context, &series, zName, zEmail);
      if( rc!=SQLITE_OK && !context->isError ) sqlite3_result_error_code(context, rc);
    }
    goto done;
  }
  if( doltliteSessionHasPendingReplayCommit(db) ){
    sqlite3_result_error(context, "a replay is already in progress", -1);
    goto done;
  }
  aHashes = sqlite3_malloc64((sqlite3_uint64)args.nPositional*sizeof(*aHashes));
  if( !aHashes ){ sqlite3_result_error_nomem(context); goto done; }
  for(i=0; i<args.nPositional; i++){
    rc = doltliteResolveRef(db, args.azPositional[i], &aHashes[i]);
    if( rc!=SQLITE_OK ){
      doltliteCmdReportInvalidCommitHash(context, cs, rc);
      goto done;
    }
  }
  if( args.nPositional==1 ){
    doltliteRevertOne(context, args.azPositional[0], aHashes, zName, zEmail);
  }else{
    ProllyHash head, working;
    doltliteGetSessionHead(db, &head);
    doltliteGetSessionStaged(db, &series.staged);
    doltliteGetSessionConstraintViolationsCatalog(db, &series.violations);
    rc = doltliteFlushCatalogToHash(db, &working);
    if( rc==SQLITE_OK ){
      rc = doltliteCreateAndStoreCommit(db, &head, &working,
          "Revert snapshot", 0, 0, 0, 0, &series.snapshot);
    }
    if( rc==SQLITE_OK ){
      rc = revertSeriesQueue(db, aHashes, args.nPositional, &working, &series.remaining);
    }
    if( rc==SQLITE_OK ) rc = revertSeriesRun(context, &series, zName, zEmail);
    if( rc!=SQLITE_OK && !context->isError ) sqlite3_result_error_code(context, rc);
  }
done:
  sqlite3_free(aHashes);
  sqlite3_free(zName);
  sqlite3_free(zEmail);
  doltliteCmdArgsClear(&args);
}


int doltliteRevertRegister(sqlite3 *db){
  return doltliteCreateCommandFunc(db, "dolt_revert", -1,
                                 doltliteRevertFunc);
}

#endif
