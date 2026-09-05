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
  return pEntry->zName && sqlite3_stricmp(pEntry->zName, zName)==0;
}

static int resetFindTableIndex(struct TableEntry *aTables, int nTables,
                               const char *zTable){
  int i;
  for(i=0; i<nTables; i++){
    if( resetPathMatchesName(&aTables[i], zTable) ) return i;
  }
  return -1;
}

/* True when zName is a table in live schema, staged, or HEAD. A table
** beats a same-named ref: dolt_reset('x') must not rewind HEAD. */
static int resetNameIsTablePath(
  sqlite3_context *context,
  sqlite3 *db,
  ChunkStore *cs,
  const char *zName,
  int *pFound
){
  struct TableEntry *aCat = 0;
  ProllyHash hash;
  Pgno iLive = 0;
  int nCat = 0;
  int rc;

  *pFound = 0;
  if( doltliteResolveTableName(db, zName, &iLive)==SQLITE_OK ){
    *pFound = 1;
    return SQLITE_OK;
  }
  doltliteGetSessionStaged(db, &hash);
  if( !prollyHashIsEmpty(&hash) ){
    rc = doltliteLoadCatalog(db, &hash, &aCat, &nCat, 0);
    if( rc!=SQLITE_OK ){
      if( doltliteCmdSourceResultError(context, cs, &rc) ) return rc;
    }else{
      *pFound = resetFindTableIndex(aCat, nCat, zName)>=0;
      doltliteFreeCatalog(aCat, nCat);
      if( *pFound ) return SQLITE_OK;
      aCat = 0;
      nCat = 0;
    }
  }
  rc = doltliteGetHeadCatalogHash(db, &hash);
  if( rc!=SQLITE_OK ){
    if( doltliteCmdSourceResultError(context, cs, &rc) ) return rc;
    return SQLITE_OK;
  }
  if( !prollyHashIsEmpty(&hash) ){
    rc = doltliteLoadCatalog(db, &hash, &aCat, &nCat, 0);
    if( rc!=SQLITE_OK ){
      if( doltliteCmdSourceResultError(context, cs, &rc) ) return rc;
    }else{
      *pFound = resetFindTableIndex(aCat, nCat, zName)>=0;
      doltliteFreeCatalog(aCat, nCat);
    }
  }
  return SQLITE_OK;
}

static int resetStageNamedPaths(
  sqlite3 *db,
  ChunkStore *cs,
  const char **azPaths,
  int nPaths
){
  struct TableEntry *aHead = 0, *aStaged = 0;
  SchemaEntry *aHeadSchema = 0;
  SchemaEntry *aStagedSchema = 0;
  int nHead = 0, nStaged = 0, nHeadSchema = 0, nStagedSchema = 0;
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
    /* Staged index entries are unnamed; resolve parents through staged schema. */
    rc = loadSchemaFromCatalog(db, cs, doltliteGetCache(db), &stagedHash,
                               &aStagedSchema, &nStagedSchema);
    if( rc!=SQLITE_OK ) goto done;
  }

  for(k=0; k<nStaged; k++){
    if( aStaged[k].iTable >= iNextFree ) iNextFree = aStaged[k].iTable + 1;
  }

  for(p=0; p<nPaths; p++){
    const char *zPath = azPaths[p];
    int iH = resetFindTableIndex(aHead, nHead, zPath);
    int iS = resetFindTableIndex(aStaged, nStaged, zPath);
    const char *zHeadTable = iH>=0 ? aHead[iH].zName : 0;
    const char *zStagedTable = iS>=0 ? aStaged[iS].zName : 0;
    char *zDup;
    if( iH<0 && iS<0 ){
      rc = SQLITE_NOTFOUND;
      goto done;
    }
    if( iH<0 ){
      /* Staged-only: new table, or new name of a staged rename. Dolt keeps
      ** a staged rename fully staged when its new name is reset; a new table
      ** leaves with its index entries. */
      struct TableEntry *pMate = 0;
      rc = doltliteCatalogRenameMate(db, aHead, nHead, aStaged, nStaged,
                                     &aStaged[iS], 0, &pMate);
      if( rc!=SQLITE_OK ) goto done;
      if( pMate ) continue;
      addRemoveIndexEntriesOfTable(aStaged, &nStaged,
                                   aStagedSchema, nStagedSchema, zStagedTable);
      iS = resetFindTableIndex(aStaged, nStaged, zStagedTable);
      sqlite3_free(aStaged[iS].zName);
      if( iS+1<nStaged ){
        memmove(&aStaged[iS], &aStaged[iS+1],
                (nStaged-iS-1)*(int)sizeof(struct TableEntry));
      }
      nStaged--;
    }else if( iS<0 ){
      /* HEAD-only: staged drop, or old name of a staged rename. Reset a
      ** rename as one object: retire the staged new-name entry and indexes. */
      struct TableEntry *pMate = 0;
      struct TableEntry *aNew;
      rc = doltliteCatalogRenameMate(db, aHead, nHead, aStaged, nStaged,
                                     &aHead[iH], 1, &pMate);
      if( rc!=SQLITE_OK ) goto done;
      if( pMate ){
        char *zMateName = sqlite3_mprintf("%s", pMate->zName);
        int iM;
        if( !zMateName ){
          rc = SQLITE_NOMEM;
          goto done;
        }
        addRemoveIndexEntriesOfTable(aStaged, &nStaged,
                                     aStagedSchema, nStagedSchema, zMateName);
        iM = resetFindTableIndex(aStaged, nStaged, zMateName);
        sqlite3_free(zMateName);
        if( iM>=0 ){
          sqlite3_free(aStaged[iM].zName);
          if( iM+1<nStaged ){
            memmove(&aStaged[iM], &aStaged[iM+1],
                    (nStaged-iM-1)*(int)sizeof(struct TableEntry));
          }
          nStaged--;
        }
      }
      /* Restoring a staged-dropped table: HEAD numbering/schema do not
      ** match staged, so give it a fresh number and take the HEAD row. */
      aNew = sqlite3_realloc(aStaged,
          (nStaged+1)*(int)sizeof(struct TableEntry));
      if( !aNew ){
        rc = SQLITE_NOMEM;
        goto done;
      }
      aStaged = aNew;
      addRemoveIndexEntriesOfTable(aStaged, &nStaged,
                                   aStagedSchema, nStagedSchema, zHeadTable);
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
                                        zHeadTable);
      if( rc!=SQLITE_OK ) goto done;
    }else{
      /* Take HEAD content under the STAGED number so it still pairs with
      ** the staged schema row. Replace staged index entries with HEAD's. */
      Pgno iKeep;
      addRemoveIndexEntriesOfTable(aStaged, &nStaged,
                                   aStagedSchema, nStagedSchema,
                                   zStagedTable);
      iS = resetFindTableIndex(aStaged, nStaged, zStagedTable);
      iKeep = aStaged[iS].iTable;
      zDup = aHead[iH].zName ? sqlite3_mprintf("%s", aHead[iH].zName) : 0;
      if( aHead[iH].zName && !zDup ){
        rc = SQLITE_NOMEM;
        goto done;
      }
      sqlite3_free(aStaged[iS].zName);
      aStaged[iS] = aHead[iH];
      aStaged[iS].zName = zDup;
      aStaged[iS].iTable = iKeep;
      rc = addAppendIndexEntriesOfTable(0, &aStaged, &nStaged,
                                        aHead, nHead,
                                        aHeadSchema, nHeadSchema,
                                        zHeadTable);
      if( rc!=SQLITE_OK ) goto done;
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
  if( aStagedSchema ){
    for(k=0; k<nStagedSchema; k++) clearSchemaEntry(&aStagedSchema[k]);
    sqlite3_free(aStagedSchema);
  }
  return rc;
}

static int doltlitePreserveUntrackedTablesOnHardReset(
  sqlite3 *db,
  ChunkStore *cs,
  const ProllyHash *pPreResetStagedCatHash,
  ProllyHash *pTargetCatHash
){
  struct TableEntry *aStaged = 0;
  SchemaEntry *aTargetSchema = 0;
  int nStaged = 0;
  int nTargetSchema = 0;
  int nUntracked = 0;
  char **azUntracked = 0;
  sqlite3_stmt *pStmt = 0;
  int j, k;
  int rc;

  rc = doltliteLoadCatalog(
      db, pPreResetStagedCatHash, &aStaged, &nStaged, 0);
  if( rc==SQLITE_OK ){
    rc = sqlite3_prepare_v2(db,
        "SELECT m.name FROM sqlite_master AS m WHERE m.type='table' "
        "AND m.name NOT LIKE 'sqlite_%' "
        "AND NOT EXISTS (SELECT 1 FROM dolt_status AS s "
        "WHERE s.status='renamed' AND "
        "substr(s.table_name, -(length(m.name)+4))=' -> ' || m.name)",
        -1, &pStmt, 0);
  }
  if( rc==SQLITE_OK ){
    while( sqlite3_step(pStmt)==SQLITE_ROW ){
      const char *zName = (const char*)sqlite3_column_text(pStmt, 0);
      int inStaged = 0;
      if( !zName ) continue;
      for(k=0; k<nStaged; k++){
        if( aStaged[k].zName && strcmp(aStaged[k].zName, zName)==0 ){
          inStaged = 1;
          break;
        }
      }
      if( !inStaged ){
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
    Pgno iNextFreeBase = 2;

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
    /* Build FROM the target so dropped tracked tables are restored, then
    ** append untracked entries past the target range (working numbers can
    ** collide). Locate untracked objects through working schema rows. */
    if( rc==SQLITE_OK ){
      for(k=0; k<nTarget; k++){
        if( aTarget[k].iTable >= iNextFree ) iNextFree = aTarget[k].iTable + 1;
      }
      for(k=0; k<nWorking; k++){
        if( aWorking[k].iTable >= iNextFree ) iNextFree = aWorking[k].iTable + 1;
      }
      iNextFreeBase = iNextFree;
      /* Every appended entry takes a fresh number, so the schema rows that
      ** carried its working number must follow it: the serializer pairs an
      ** index row to its entry by number alone, and only a table row can fall
      ** back to its name. A clustered primary key's autoindex row shares its
      ** table's number and needs no entry of its own. */
      for(j=0; j<nUntracked && rc==SQLITE_OK; j++){
        SchemaEntry *pSe = findSchemaEntry(aWorkSchema, nWorkSchema,
                                           azUntracked[j]);
        struct TableEntry *pWork = 0;
        struct TableEntry *aNew;
        char *zDup = 0;
        Pgno iOldPg;
        Pgno iNewPg;
        int isIndex;
        int m;
        if( !pSe || pSe->iRootpage<=1 ) continue;
        iOldPg = pSe->iRootpage;
        if( iOldPg >= iNextFreeBase ) continue;
        for(k=0; k<nWorking; k++){
          if( aWorking[k].iTable==iOldPg ){
            pWork = &aWorking[k];
            break;
          }
        }
        if( !pWork ) continue;
        isIndex = pSe->zType && strcmp(pSe->zType, "index")==0;
        if( !isIndex ){
          zDup = sqlite3_mprintf("%s", azUntracked[j]);
          if( !zDup ){ rc = SQLITE_NOMEM; break; }
        }
        aNew = sqlite3_realloc(aTarget,
                               (nTarget+1)*(int)sizeof(struct TableEntry));
        if( !aNew ){
          sqlite3_free(zDup);
          rc = SQLITE_NOMEM;
          break;
        }
        aTarget = aNew;
        aTarget[nTarget] = *pWork;
        aTarget[nTarget].zName = zDup;
        iNewPg = iNextFree++;
        aTarget[nTarget].iTable = iNewPg;
        nTarget++;
        for(m=0; m<nWorkSchema; m++){
          if( aWorkSchema[m].iRootpage==iOldPg ) aWorkSchema[m].iRootpage = iNewPg;
        }
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
  doltliteFreeCatalog(aStaged, nStaged);
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
  ProllyHash preResetStagedCatHash;
  ProllyHash sessionHeadBeforeLock;
  int havePreResetHead = 0;
  int isHard = 0;
  int isSoft = 0;
  DoltliteCmdArgs args;
  DoltliteCmdOption aOption[] = {
    { "hard", 0, DOLTLITE_CMD_OPTION_FLAG, &isHard, 0 },
    { "soft", 0, DOLTLITE_CMD_OPTION_FLAG, &isSoft, 0 }
  };
  const char *zRef = 0;
  const char **azPaths = 0;
  int nPaths = 0;
  int rc;
  int i;
  int nNullArgs = 0;
  int graphLocked = 0;
  u8 isMerging = 0;
  int bSucceeded = 0;

  memset(&args, 0, sizeof(args));

  assert( context!=0 );
  assert( argc>=0 );
  if( doltliteCmdRejectDetached(context) ) return;
  if( !cs ){
    sqlite3_result_error(context, doltliteVcUnavailableMessage(db), -1);
    goto reset_cleanup;
  }

  rc = doltliteGetHeadCatalogHash(db, &preResetHeadCatHash);
  if( rc!=SQLITE_OK && doltliteCmdSourceResultError(context, cs, &rc) ){
    goto reset_cleanup;
  }else if( rc==SQLITE_OK && !prollyHashIsEmpty(&preResetHeadCatHash) ){
    havePreResetHead = 1;
    doltliteGetSessionStaged(db, &preResetStagedCatHash);
    if( prollyHashIsEmpty(&preResetStagedCatHash) ){
      memcpy(&preResetStagedCatHash, &preResetHeadCatHash,
             sizeof(ProllyHash));
    }
  }

  for(i=0; i<argc; i++){
    if( sqlite3_value_type(argv[i])==SQLITE_NULL ) nNullArgs++;
  }
  rc = doltliteCmdParseArgs(context, argc, argv, aOption, ArraySize(aOption),
                            DOLTLITE_CMD_PARSE_IGNORE_NULLS, &args);
  if( rc!=SQLITE_OK ) goto reset_cleanup;
  if( argc==1 && nNullArgs==1 ){
    sqlite3_result_error(context, "commit not found", -1);
    goto reset_cleanup;
  }
  if( (isHard || isSoft) && args.nPositional+nNullArgs>1 ){
    sqlite3_result_error(context,
      isHard ? "--hard supports at most one additional param"
             : "--soft supports at most one additional param", -1);
    goto reset_cleanup;
  }
  azPaths = (const char**)sqlite3_malloc(
      sizeof(char*) * (args.nPositional>0 ? args.nPositional : 1));
  if( !azPaths ){ sqlite3_result_error_nomem(context); goto reset_cleanup; }
  for(i=0; i<args.nPositional; i++){
    const char *arg = args.azPositional[i];
    int isTable = 0;
    if( !zRef ){

      if( isHard || isSoft ){
        zRef = arg;
      }else{
        ProllyHash probe;
        rc = resetNameIsTablePath(context, db, cs, arg, &isTable);
        if( rc!=SQLITE_OK ){
          goto reset_cleanup;
        }
        if( !isTable ){
          rc = doltliteResolveRef(db, arg, &probe);
          if( rc==SQLITE_OK ){
            zRef = arg;
          }else if( doltliteCmdSourceResultError(context, cs, &rc) ){
            goto reset_cleanup;
          }else{
            azPaths[nPaths++] = arg;
          }
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

    /* Move the ref before the session head. The other order leaves the
    ** session reading a commit the branch never reached if the update fails.
    ** reset --hard is not atomic (nor in Dolt). */
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
        db, cs, &preResetStagedCatHash, &targetCatHash
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
  doltliteCmdArgsClear(&args);
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
