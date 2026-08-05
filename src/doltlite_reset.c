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

static int resetPathMatchesName(const struct TableEntry *pEntry, const char *zName){
  return pEntry->zName && strcmp(pEntry->zName, zName)==0;
}

static int resetFindTableIndex(struct TableEntry *aTables, int nTables,
                               const char *zTable){
  int i;
  for(i=0; i<nTables; i++){
    if( resetPathMatchesName(&aTables[i], zTable) ) return i;
  }
  return -1;
}

static int resetStageNamedPaths(
  sqlite3 *db,
  ChunkStore *cs,
  const char **azPaths,
  int nPaths
){
  struct TableEntry *aHead = 0, *aStaged = 0;
  SchemaEntry *aHeadSchema = 0;
  int nHead = 0, nStaged = 0, nHeadSchema = 0;
  ProllyHash headCatHash, stagedHash;
  Pgno iNextFree = 2;
  int p, k;
  int rc;

  rc = doltliteGetHeadCatalogHash(db, &headCatHash);
  if( rc!=SQLITE_OK ) return rc;
  if( !prollyHashIsEmpty(&headCatHash) ){
    rc = doltliteLoadCatalog(db, &headCatHash, &aHead, &nHead, 0);
    if( rc!=SQLITE_OK ) return rc;
    rc = loadSchemaFromCatalog(db, cs, doltliteGetCache(db), &headCatHash,
                               &aHeadSchema, &nHeadSchema);
    if( rc!=SQLITE_OK ){
      doltliteFreeCatalog(aHead, nHead);
      return rc;
    }
  }

  doltliteGetSessionStaged(db, &stagedHash);
  if( !prollyHashIsEmpty(&stagedHash) ){
    rc = doltliteLoadCatalog(db, &stagedHash, &aStaged, &nStaged, 0);
    if( rc!=SQLITE_OK ) goto done;
  }

  for(k=0; k<nStaged; k++){
    if( aStaged[k].iTable >= iNextFree ) iNextFree = aStaged[k].iTable + 1;
  }

  for(p=0; p<nPaths; p++){
    const char *zTable = azPaths[p];
    int iH = resetFindTableIndex(aHead, nHead, zTable);
    int iS = resetFindTableIndex(aStaged, nStaged, zTable);
    char *zDup;
    if( iH<0 && iS<0 ){
      rc = SQLITE_NOTFOUND;
      goto done;
    }
    if( iH<0 ){
      sqlite3_free(aStaged[iS].zName);
      if( iS+1<nStaged ){
        memmove(&aStaged[iS], &aStaged[iS+1],
                (nStaged-iS-1)*(int)sizeof(struct TableEntry));
      }
      nStaged--;
    }else if( iS<0 ){
      /* Restoring a staged-dropped table: the HEAD entry is numbered in
      ** HEAD's domain and its schema row is absent from the staged master
      ** (and from the live schema -- the table is dropped there), so it
      ** gets a fresh number and its row comes from the HEAD fallback. */
      struct TableEntry *aNew = sqlite3_realloc(aStaged,
          (nStaged+1)*(int)sizeof(struct TableEntry));
      if( !aNew ){
        rc = SQLITE_NOMEM;
        goto done;
      }
      aStaged = aNew;
      addRemoveIndexEntriesOfTable(aStaged, &nStaged,
                                   aHeadSchema, nHeadSchema, zTable);
      zDup = aHead[iH].zName ? sqlite3_mprintf("%s", aHead[iH].zName) : 0;
      if( aHead[iH].zName && !zDup ){
        rc = SQLITE_NOMEM;
        goto done;
      }
      aStaged[nStaged] = aHead[iH];
      aStaged[nStaged].zName = zDup;
      aStaged[nStaged].iTable = iNextFree++;
      nStaged++;
      rc = addAppendIndexEntriesOfTable(0, &aStaged, &nStaged,
                                        aHead, nHead,
                                        aHeadSchema, nHeadSchema,
                                        zTable);
      if( rc!=SQLITE_OK ) goto done;
    }else{
      /* Take HEAD's content under the STAGED entry's number so the entry
      ** keeps pairing with the staged catalog's schema row. */
      Pgno iKeep = aStaged[iS].iTable;
      zDup = aHead[iH].zName ? sqlite3_mprintf("%s", aHead[iH].zName) : 0;
      if( aHead[iH].zName && !zDup ){
        rc = SQLITE_NOMEM;
        goto done;
      }
      sqlite3_free(aStaged[iS].zName);
      aStaged[iS] = aHead[iH];
      aStaged[iS].zName = zDup;
      aStaged[iS].iTable = iKeep;
    }
  }

  {
    u8 *buf = 0;
    int nBuf = 0;
    ProllyHash newStagedHash;
    rc = doltliteSerializeCatalogEntriesWithFallbackSchema(
        db, aStaged, nStaged, aHeadSchema, nHeadSchema, &buf, &nBuf);
    if( rc==SQLITE_OK ){
      rc = chunkStorePut(cs, buf, nBuf, &newStagedHash);
    }
    sqlite3_free(buf);
    if( rc==SQLITE_OK ){
      rc = doltliteSetSessionStaged(db, &newStagedHash);
    }
  }

done:
  doltliteFreeCatalog(aHead, nHead);
  doltliteFreeCatalog(aStaged, nStaged);
  if( aHeadSchema ){
    for(k=0; k<nHeadSchema; k++) clearSchemaEntry(&aHeadSchema[k]);
    sqlite3_free(aHeadSchema);
  }
  return rc;
}

static int doltlitePreserveUntrackedTablesOnHardReset(
  sqlite3 *db,
  ChunkStore *cs,
  const ProllyHash *pPreResetHeadCatHash,
  ProllyHash *pTargetCatHash
){
  struct TableEntry *aHead = 0;
  SchemaEntry *aTargetSchema = 0;
  int nHead = 0;
  int nTargetSchema = 0;
  int nUntracked = 0;
  char **azUntracked = 0;
  sqlite3_stmt *pStmt = 0;
  int j, k;
  int rc;

  rc = doltliteLoadCatalog(db, pPreResetHeadCatHash, &aHead, &nHead, 0);
  if( rc==SQLITE_OK ){
    rc = sqlite3_prepare_v2(db,
        "SELECT m.name FROM sqlite_master AS m WHERE m.type='table' "
        "AND m.name NOT LIKE 'sqlite_%' AND m.name NOT LIKE 'dolt_%' "
        "AND NOT EXISTS (SELECT 1 FROM dolt_status AS s "
        "WHERE s.status='renamed' AND "
        "substr(s.table_name, -(length(m.name)+4))=' -> ' || m.name)",
        -1, &pStmt, 0);
  }
  if( rc==SQLITE_OK ){
    while( sqlite3_step(pStmt)==SQLITE_ROW ){
      const char *zName = (const char*)sqlite3_column_text(pStmt, 0);
      int inHead = 0;
      if( !zName ) continue;
      for(k=0; k<nHead; k++){
        if( aHead[k].zName && strcmp(aHead[k].zName, zName)==0 ){
          inHead = 1;
          break;
        }
      }
      if( !inHead ){
        char **aNew = sqlite3_realloc(azUntracked,
            (nUntracked+1)*(int)sizeof(char*));
        if( !aNew ){ rc = SQLITE_NOMEM; break; }
        azUntracked = aNew;
        azUntracked[nUntracked++] = sqlite3_mprintf("%s", zName);
      }
    }
    sqlite3_finalize(pStmt);
    pStmt = 0;
  }

  if( rc==SQLITE_OK && nUntracked>0 ){
    rc = loadSchemaFromCatalog(db, cs, doltliteGetCache(db), pTargetCatHash,
                               &aTargetSchema, &nTargetSchema);
  }
  if( rc==SQLITE_OK && nUntracked>0 ){
    int nKeep = 0;
    rc = sqlite3_prepare_v2(db,
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=?",
        -1, &pStmt, 0);
    for(j=0; rc==SQLITE_OK && j<nUntracked; j++){
      int keep = 1;
      sqlite3_bind_text(pStmt, 1, azUntracked[j], -1, SQLITE_STATIC);
      while( keep && sqlite3_step(pStmt)==SQLITE_ROW ){
        const char *zIdx = (const char*)sqlite3_column_text(pStmt, 0);
        if( zIdx && findSchemaEntry(aTargetSchema, nTargetSchema, zIdx) ){
          keep = 0;
        }
      }
      sqlite3_reset(pStmt);
      if( keep ){
        azUntracked[nKeep++] = azUntracked[j];
      }else{
        sqlite3_free(azUntracked[j]);
      }
    }
    nUntracked = nKeep;
    sqlite3_finalize(pStmt);
    pStmt = 0;
  }

  /* An untracked table's indexes carry their own catalog entries. */
  if( rc==SQLITE_OK && nUntracked>0 ){
    rc = sqlite3_prepare_v2(db,
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=?",
        -1, &pStmt, 0);
    for(j=0; rc==SQLITE_OK && j<nUntracked; j++){
      sqlite3_bind_text(pStmt, 1, azUntracked[j], -1, SQLITE_STATIC);
      while( sqlite3_step(pStmt)==SQLITE_ROW ){
        const char *zIdx = (const char*)sqlite3_column_text(pStmt, 0);
        char **aNew;
        if( !zIdx ) continue;
        aNew = sqlite3_realloc(azUntracked,
            (nUntracked+1)*(int)sizeof(char*));
        if( !aNew ){ rc = SQLITE_NOMEM; break; }
        azUntracked = aNew;
        azUntracked[nUntracked++] = sqlite3_mprintf("%s", zIdx);
      }
      sqlite3_reset(pStmt);
    }
    sqlite3_finalize(pStmt);
    pStmt = 0;
  }

  if( rc==SQLITE_OK && nUntracked>0 ){
    ProllyHash workingHash;
    struct TableEntry *aWorking = 0, *aTarget = 0;
    SchemaEntry *aWorkSchema = 0;
    int nWorking = 0, nTarget = 0, nWorkSchema = 0;
    Pgno iNextFree = 2;

    rc = doltliteFlushCatalogToHash(db, &workingHash);
    if( rc==SQLITE_OK ){
      rc = doltliteLoadCatalog(db, &workingHash, &aWorking, &nWorking, 0);
    }
    if( rc==SQLITE_OK ){
      rc = doltliteLoadCatalog(db, pTargetCatHash, &aTarget, &nTarget, 0);
    }
    if( rc==SQLITE_OK ){
      rc = loadSchemaFromCatalog(db, cs, doltliteGetCache(db), &workingHash,
                                 &aWorkSchema, &nWorkSchema);
    }
    /* Build FROM the target so every tracked table -- including ones
    ** dropped in the working tree -- is restored, then append the
    ** untracked entries renumbered past the target range (working numbers
    ** can collide with target numbers for different tables). Loaded index
    ** entries carry no name, so each untracked object is located through
    ** the working schema rows (name -> working number -> entry); its
    ** schema row reaches the blob via the fallback list, which pairs by
    ** name and stamps the appended number. */
    if( rc==SQLITE_OK ){
      for(k=0; k<nTarget; k++){
        if( aTarget[k].iTable >= iNextFree ) iNextFree = aTarget[k].iTable + 1;
      }
      for(j=0; j<nUntracked && rc==SQLITE_OK; j++){
        SchemaEntry *pSe = findSchemaEntry(aWorkSchema, nWorkSchema,
                                           azUntracked[j]);
        struct TableEntry *pWork = 0;
        struct TableEntry *aNew;
        char *zDup;
        if( !pSe || pSe->iRootpage<=1 ) continue;
        for(k=0; k<nWorking; k++){
          if( aWorking[k].iTable==pSe->iRootpage ){
            pWork = &aWorking[k];
            break;
          }
        }
        if( !pWork ) continue;
        zDup = sqlite3_mprintf("%s", azUntracked[j]);
        aNew = zDup ? sqlite3_realloc(aTarget,
                        (nTarget+1)*(int)sizeof(struct TableEntry)) : 0;
        if( !aNew ){
          sqlite3_free(zDup);
          rc = SQLITE_NOMEM;
          break;
        }
        aTarget = aNew;
        aTarget[nTarget] = *pWork;
        aTarget[nTarget].zName = zDup;
        aTarget[nTarget].iTable = iNextFree++;
        nTarget++;
      }
    }
    if( rc==SQLITE_OK ){
      u8 *buf = 0;
      int nBuf = 0;
      ProllyHash mergedHash;
      rc = doltliteSerializeCatalogEntriesForeignDomain(
          db, aTarget, nTarget, aWorkSchema, nWorkSchema, &buf, &nBuf);
      if( rc==SQLITE_OK ){
        rc = chunkStorePut(cs, buf, nBuf, &mergedHash);
      }
      sqlite3_free(buf);
      if( rc==SQLITE_OK ){
        memcpy(pTargetCatHash, &mergedHash, sizeof(ProllyHash));
      }
    }
    doltliteFreeCatalog(aWorking, nWorking);
    doltliteFreeCatalog(aTarget, nTarget);
    if( aWorkSchema ){
      for(j=0; j<nWorkSchema; j++) clearSchemaEntry(&aWorkSchema[j]);
      sqlite3_free(aWorkSchema);
    }
  }

  if( pStmt ) sqlite3_finalize(pStmt);
  for(j=0; j<nUntracked; j++) sqlite3_free(azUntracked[j]);
  sqlite3_free(azUntracked);
  doltliteFreeCatalog(aHead, nHead);
  freeSchemaEntries(aTargetSchema, nTargetSchema);
  return rc;
}

static void doltliteResetFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHash targetCatHash;
  ProllyHash targetCommit;
  ProllyHash preResetHeadCatHash;
  ProllyHash sessionHeadBeforeLock;
  int havePreResetHead = 0;
  int isHard = 0;
  int isSoft = 0;
  const char *zRef = 0;
  const char **azPaths = 0;
  int nPaths = 0;
  int rc;
  int i;
  int graphLocked = 0;
  u8 isMerging = 0;
  int bSucceeded = 0;

  assert( context!=0 );
  assert( argc>=0 );
  if( !cs ){
    sqlite3_result_error(context, doltliteVcUnavailableMessage(db), -1);
    goto reset_cleanup;
  }

  if( doltliteGetHeadCatalogHash(db, &preResetHeadCatHash)==SQLITE_OK
   && !prollyHashIsEmpty(&preResetHeadCatHash) ){
    havePreResetHead = 1;
  }

  azPaths = (const char**)sqlite3_malloc(sizeof(char*) * (argc>0?argc:1));
  if( !azPaths ){ sqlite3_result_error_nomem(context); goto reset_cleanup; }
  for(i=0; i<argc; i++){
    const char *arg = (const char*)sqlite3_value_text(argv[i]);
    if( !arg ) continue;
    if( strcmp(arg, "--hard")==0 ){ isHard = 1; }
    else if( strcmp(arg, "--soft")==0 ){ isSoft = 1; }
    else if( arg[0]=='-' ){
      doltliteCmdResultUnknownOption(context, arg);
      sqlite3_free(azPaths);
      azPaths = 0;
      goto reset_cleanup;
    }
    else if( !zRef ){

      if( isHard || isSoft ){
        zRef = arg;
      }else{
        ProllyHash probe;
        if( doltliteResolveRef(db, arg, &probe)==SQLITE_OK ){
          zRef = arg;
        }else{
          azPaths[nPaths++] = arg;
        }
      }
    }
    else{

      azPaths[nPaths++] = arg;
    }
  }

  if( isHard && isSoft ){
    sqlite3_result_error(context,
      "--hard and --soft are mutually exclusive options.", -1);
    sqlite3_free(azPaths);
    azPaths = 0;
    goto reset_cleanup;
  }

  doltliteGetSessionMergeState(db, &isMerging, 0, 0);
  if( isMerging && !isHard ){
    sqlite3_free(azPaths);
    azPaths = 0;
    sqlite3_result_error(context,
      "cannot merge: conflicts detected, transaction rolled back. "
      "Resolve conflicts via dolt_conflicts and dolt_schema_conflicts with "
      "dolt_conflicts_resolve(), then commit with dolt_commit(). Conflicts are "
      "never committed as conflicts", -1);
    goto reset_cleanup;
  }

  if( nPaths>1 && !isHard && !isSoft && !zRef ){
    nPaths = 0;
  }

  if( nPaths>0 ){
    if( isHard || isSoft || zRef ){
      sqlite3_result_error(context,
        "table paths cannot be combined with --hard / --soft or a target ref", -1);
      sqlite3_free(azPaths);
      azPaths = 0;
      goto reset_cleanup;
    }
    rc = resetStageNamedPaths(db, cs, azPaths, nPaths);
    sqlite3_free(azPaths);
    azPaths = 0;
    if( rc==SQLITE_NOTFOUND ){
      sqlite3_result_error(context, "table not found", -1);
      goto reset_cleanup;
    }
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      goto reset_cleanup;
    }
    rc = doltlitePersistWorkingSet(db);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      goto reset_cleanup;
    }
    sqlite3_result_int(context, 0);
    goto reset_cleanup;
  }
  sqlite3_free(azPaths);
  azPaths = 0;

  if( isSoft && !zRef ){
    sqlite3_result_int(context, 0);
    goto reset_cleanup;
  }

  if( zRef ){
    DoltliteCommit commit;

    rc = doltliteResolveRef(db,zRef, &targetCommit);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(context, "commit not found", -1);
      goto reset_cleanup;
    }

    rc = doltliteLoadCommit(db, &targetCommit, &commit);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(context, "failed to load commit", -1);
      goto reset_cleanup;
    }
    memcpy(&targetCatHash, &commit.catalogHash, sizeof(ProllyHash));
    doltliteCommitClear(&commit);

    doltliteGetSessionHead(db, &sessionHeadBeforeLock);
    rc = doltliteRefreshAndConfirmHead(db, cs, &sessionHeadBeforeLock);
    if( rc==SQLITE_BUSY ){
      sqlite3_result_error(context,
        "reset conflict: another connection moved this branch. "
        "Please retry your transaction.", -1);
      goto reset_cleanup;
    }
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      goto reset_cleanup;
    }
    graphLocked = 1;

    /* Move the ref before the session head. There is no rollback on this path,
    ** so the other order leaves the session reading a commit the branch was
    ** never advanced to when the update fails. The update needs only the
    ** session branch, so the order costs nothing.
    **
    ** This is the ordering only: reset --hard is not atomic against a failure
    ** part way through, and dolt does not make it atomic either -- an explicit
    ** ROLLBACK around CALL dolt_reset('--hard', ...) leaves HEAD moved -- so
    ** there is no rollback contract here to hold up. */
    rc = chunkStoreUpdateBranch(cs, doltliteGetSessionBranch(db), &targetCommit);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      goto reset_cleanup;
    }
    doltliteSetSessionHead(db, &targetCommit);

    rc = doltliteClearSessionMergeState(db);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      goto reset_cleanup;
    }
  }else{
    rc = doltliteGetHeadCatalogHash(db, &targetCatHash);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(context, "failed to read HEAD", -1);
      goto reset_cleanup;
    }
  }

  if( !isSoft ){
    rc = doltliteSetSessionStaged(db, &targetCatHash);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      goto reset_cleanup;
    }
  }

  if( isHard ){

    ProllyHash origStagedAfterReset;
    memcpy(&origStagedAfterReset, &targetCatHash, sizeof(ProllyHash));

    if( prollyHashIsEmpty(&targetCatHash) ){
      sqlite3_result_error(context, "no commit to reset to", -1);
      goto reset_cleanup;
    }

    if( havePreResetHead ){
      rc = doltlitePreserveUntrackedTablesOnHardReset(
        db, cs, &preResetHeadCatHash, &targetCatHash
      );
      if( rc!=SQLITE_OK ){
        sqlite3_result_error_code(context, rc);
        goto reset_cleanup;
      }
    }

    rc = doltliteSaveWorkingSet(db);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      goto reset_cleanup;
    }
    rc = doltliteHardReset(db, &targetCatHash);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(context, "hard reset failed", -1);
      goto reset_cleanup;
    }

    rc = doltliteClearSessionMergeState(db);

    if( rc==SQLITE_OK ){
      extern int doltliteClearAllConstraintViolations(sqlite3*);
      if( doltliteSessionHasConstraintViolations(db) ){
        rc = doltliteClearAllConstraintViolations(db);
      }
    }

    if( rc==SQLITE_OK ){
      rc = doltliteSetSessionStaged(db, &origStagedAfterReset);
    }
    if( rc==SQLITE_OK ){
      rc = doltlitePersistWorkingSet(db);
    }
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      goto reset_cleanup;
    }
  }else{
    rc = doltlitePersistWorkingSet(db);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      goto reset_cleanup;
    }
  }

  sqlite3_result_int(context, 0);
  bSucceeded = 1;
reset_cleanup:
  sqlite3_free(azPaths);
  if( graphLocked ){
    chunkStoreUnlock(cs);
  }
  if( bSucceeded ){
    rc = doltliteVcSealActiveSavepoints(db);
  }else{
    rc = doltliteVcSealTopLevelSavepointTxn(db);
  }
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
  }
}


int doltliteResetRegister(sqlite3 *db){
  return sqlite3_create_function(db, "dolt_reset", -1,
                                 DOLTLITE_COMMAND_FUNC_FLAGS, 0,
                                 doltliteResetFunc, 0, 0);
}

#endif
