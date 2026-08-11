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

static int doltliteTableEntryDiffers(
  const struct TableEntry *a, const struct TableEntry *b
){
  if( !a && !b ) return 0;
  if( !a || !b ) return 1;
  if( prollyHashCompare(&a->root, &b->root)!=0 ) return 1;
  if( prollyHashCompare(&a->schemaHash, &b->schemaHash)!=0 ) return 1;
  return 0;
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
  int nCommit = 0, nParent = 0;
  int nWorking = 0, nCompare = 0;
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

  for(i=0; i<nWorking; i++){
    struct TableEntry *pCmp;
    struct TableEntry *pInCommit;
    struct TableEntry *pInParent;
    pCmp = doltliteFindTableByName(aCompare, nCompare, aWorking[i].zName);
    if( !doltliteTableEntryDiffers(pCmp, &aWorking[i]) ) continue;
    pInCommit = doltliteFindTableByName(aCommit, nCommit, aWorking[i].zName);
    pInParent = doltliteFindTableByName(aParent, nParent, aWorking[i].zName);
    if( doltliteTableEntryDiffers(pInCommit, pInParent) ){
      *pConflict = 1;
      goto done;
    }
  }
  for(i=0; i<nCompare; i++){
    struct TableEntry *pInCommit;
    struct TableEntry *pInParent;
    if( doltliteFindTableByName(aWorking, nWorking, aCompare[i].zName) ) continue;
    pInCommit = doltliteFindTableByName(aCommit, nCommit, aCompare[i].zName);
    pInParent = doltliteFindTableByName(aParent, nParent, aCompare[i].zName);
    if( doltliteTableEntryDiffers(pInCommit, pInParent) ){
      *pConflict = 1;
      goto done;
    }
  }

done:
  doltliteFreeCatalog(aCommit, nCommit);
  doltliteFreeCatalog(aParent, nParent);
  doltliteFreeCatalog(aWorking, nWorking);
  doltliteFreeCatalog(aCompare, nCompare);
  return rc;
}

static void doltliteRevertFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  ChunkStore *cs = doltliteGetChunkStore(db);
  const char *zRef;
  ProllyHash revertHash, ourHead;
  ProllyHash liveOurCatalog;
  DoltliteCommit revertCommit, parentCommit, ourCommit;
  int nConflicts = 0;
  int rc;
  char hexBuf[PROLLY_HASH_SIZE*2+1];

  memset(&revertCommit, 0, sizeof(revertCommit));
  memset(&parentCommit, 0, sizeof(parentCommit));
  memset(&ourCommit, 0, sizeof(ourCommit));

  if( doltliteCmdRejectDetached(context) ) return;
  if( !cs ){ sqlite3_result_error(context, "no database", -1); return; }

  if( argc<1 ){
    sqlite3_result_int(context, 0);
    return;
  }
  if( argc>1 ){
    char *zErr = sqlite3_mprintf("branch not found: %s",
        (const char*)sqlite3_value_text(argv[1]));
    if( zErr ){
      sqlite3_result_error(context, zErr, -1);
      sqlite3_free(zErr);
    }else{
      sqlite3_result_error_nomem(context);
    }
    return;
  }

  zRef = (const char*)sqlite3_value_text(argv[0]);
  if( !zRef ){
    sqlite3_result_int(context, 0);
    return;
  }

  rc = doltliteResolveRef(db,zRef, &revertHash);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "invalid commit hash", -1);
    return;
  }
  rc = doltliteLoadHeadAndParentedCommit(
    db, &revertHash,
    &ourHead, &revertCommit, &parentCommit, &ourCommit
  );
  if( rc==SQLITE_NOTFOUND ){
    doltliteCommitClear(&revertCommit);
    doltliteCommitClear(&parentCommit);
    sqlite3_result_error(context, "commit not found", -1);
    return;
  }
  if( rc==SQLITE_EMPTY ){
    doltliteCommitClear(&revertCommit);
    sqlite3_result_error(context, "cannot revert the initial commit", -1);
    return;
  }
  if( rc==SQLITE_DONE ){
    doltliteCommitClear(&revertCommit);
    doltliteCommitClear(&parentCommit);
    sqlite3_result_error(context, "no commits on current branch", -1);
    return;
  }
  if( rc==SQLITE_ABORT ){
    doltliteCommitClear(&revertCommit);
    doltliteCommitClear(&parentCommit);
    sqlite3_result_error(context, "failed to load HEAD commit", -1);
    return;
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
      return;
    }
  }
  rc = doltliteFlushCatalogToHash(db, &liveOurCatalog);
  if( rc!=SQLITE_OK ) goto revert_error;

  {
    char msg[512];
    /* Base the commit on HEAD, not the working catalog: the revert commit
    ** must contain only the revert, while unrelated uncommitted changes
    ** (the overlap gate above rejects related ones) stay in the working
    ** set, matching Dolt. */
    const ProllyHash *pCommitOurs =
        prollyHashCompare(&liveOurCatalog, &ourCommit.catalogHash)!=0
          ? &ourCommit.catalogHash : 0;
    sqlite3_snprintf(sizeof(msg), msg, "Revert \"%s\"",
                     revertCommit.zMessage ? revertCommit.zMessage : zRef);

    rc = applyMergedCatalogAndCommit(db, context,
        &revertCommit.catalogHash, &liveOurCatalog,
        &parentCommit.catalogHash, &ourHead, pCommitOurs, msg,
        &nConflicts, 0, hexBuf);
  }

  doltliteCommitClear(&revertCommit);
  doltliteCommitClear(&parentCommit);
  doltliteCommitClear(&ourCommit);

  if( rc==SQLITE_BUSY ){
    doltliteCmdResultPeerBranchBusy(context, "revert");
    return;
  }
  if( rc!=SQLITE_OK ){
    char *zMsg = sqlite3_mprintf("revert of \"%s\" failed", zRef);
    sqlite3_result_error(context, zMsg ? zMsg : "revert failed", -1);
    sqlite3_free(zMsg);
    return;
  }

  /* Conflict / CV finish helpers already set the context error (including the
  ** combined conflicts+CVs message). Do not overwrite it here. */
  if( nConflicts > 0 ) return;
  if( hexBuf[0] ){
    sqlite3_result_text(context, hexBuf, -1, SQLITE_TRANSIENT);
  }
  return;

revert_error:
  doltliteCommitClear(&revertCommit);
  doltliteCommitClear(&parentCommit);
  doltliteCommitClear(&ourCommit);
  {
    char *zMsg = sqlite3_mprintf("revert of \"%s\" failed", zRef);
    sqlite3_result_error(context, zMsg ? zMsg : "revert failed", -1);
    sqlite3_free(zMsg);
  }
}


int doltliteRevertRegister(sqlite3 *db){
  return sqlite3_create_function(db, "dolt_revert", -1,
                                 DOLTLITE_COMMAND_FUNC_FLAGS, 0,
                                 doltliteRevertFunc, 0, 0);
}

#endif
