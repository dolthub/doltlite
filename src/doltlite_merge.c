#ifdef DOLTLITE_PROLLY

#include "doltlite_merge_int.h"

/* Catalog three-way merge: table roots, schema objects, conflicts, reindex. */

/* Rewrite every record in theirs' table root from theirs' column order into
** the merged column order (ours' columns, then theirs' added columns), using
** the ancestor to retain renamed-column identity. Missing fields use their
** declared defaults where needed, while encoded NULLs remain NULL. This lets
** the positional cell-level three-way merge run
** across a dual ADD COLUMN divergence: without it, theirs' added-column value
** would be read at the wrong ordinal. Records are content-addressed and rebuilt
** through the canonical doltliteBuildRecord, so the tree stays deterministic. */

static int serializeMergedCatalog(
  sqlite3 *db,
  const ProllyHash *oursCatHash,
  struct TableEntry *aMerged,
  int nMerged,
  Pgno iNextTable,
  SchemaEntry *aFallbackSchema,
  int nFallbackSchema,
  ProllyHash *pOutHash
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  u8 *buf = 0;
  int nBuf = 0;
  int rc;

  (void)oursCatHash;
  (void)iNextTable;

  rc = doltliteSerializeCatalogEntriesWithFallbackSchema(
      db, aMerged, nMerged, aFallbackSchema, nFallbackSchema, &buf, &nBuf);
  if( rc!=SQLITE_OK ) return rc;

  rc = chunkStorePut(cs, buf, nBuf, pOutHash);
  sqlite3_free(buf);
  return rc;
}

void freeConflictRows(DoltliteConflictRow *aRows, int nRows){
  int i;
  for(i=0; i<nRows; i++){
    doltliteConflictRowFree(&aRows[i]);
  }
  sqlite3_free(aRows);
}

void freeAddedColumns(char **azCols, int nCols){
  int i;
  for(i=0; i<nCols; i++) sqlite3_free(azCols[i]);
  sqlite3_free(azCols);
}

int appendConflictTable(
  MergeConflictTable **ppConflictTables,
  int *pnConflictTables,
  const char *zName,
  int nConflicts,
  DoltliteConflictRow *aConflictRows
){
  MergeConflictTable *aNew;
  aNew = sqlite3_realloc(*ppConflictTables,
    (*pnConflictTables+1)*(int)sizeof(MergeConflictTable));
  if( !aNew ){
    freeConflictRows(aConflictRows, nConflicts);
    return SQLITE_NOMEM;
  }
  *ppConflictTables = aNew;
  aNew[*pnConflictTables].zName = sqlite3_mprintf("%s", zName);
  if( !aNew[*pnConflictTables].zName ){
    freeConflictRows(aConflictRows, nConflicts);
    return SQLITE_NOMEM;
  }
  aNew[*pnConflictTables].nConflicts = nConflicts;
  aNew[*pnConflictTables].aRows = aConflictRows;
  aNew[*pnConflictTables].azSchemaObjects = 0;
  aNew[*pnConflictTables].nSchemaObjects = 0;
  (*pnConflictTables)++;
  return SQLITE_OK;
}

int appendSchemaConflict(
  MergeConflictTable **ppConflictTables,
  int *pnConflictTables,
  const char *zTable,
  const char *zObject,
  int *pAddedTable
){
  MergeConflictTable *pTable = 0;
  char **azNew;
  int i, rc;

  *pAddedTable = 0;
  for(i=0; i<*pnConflictTables; i++){
    if( (*ppConflictTables)[i].zName
     && sqlite3_stricmp((*ppConflictTables)[i].zName, zTable)==0 ){
      pTable = &(*ppConflictTables)[i];
      break;
    }
  }
  if( !pTable ){
    rc = appendConflictTable(ppConflictTables, pnConflictTables,
                             zTable, 0, 0);
    if( rc!=SQLITE_OK ) return rc;
    pTable = &(*ppConflictTables)[*pnConflictTables-1];
    *pAddedTable = 1;
  }

  for(i=0; i<pTable->nSchemaObjects; i++){
    if( sqlite3_stricmp(pTable->azSchemaObjects[i], zObject)==0 ){
      return SQLITE_OK;
    }
  }
  azNew = sqlite3_realloc(pTable->azSchemaObjects,
      (pTable->nSchemaObjects+1)*(int)sizeof(char*));
  if( !azNew ) return SQLITE_NOMEM;
  pTable->azSchemaObjects = azNew;
  azNew[pTable->nSchemaObjects] = sqlite3_mprintf("%s", zObject);
  if( !azNew[pTable->nSchemaObjects] ) return SQLITE_NOMEM;
  pTable->nSchemaObjects++;
  return SQLITE_OK;
}

int hasSchemaConflictObject(
  MergeConflictTable *aConflictTables,
  int nConflictTables,
  const char *zObject
){
  int i, j;
  if( !zObject ) return 0;
  for(i=0; i<nConflictTables; i++){
    for(j=0; j<aConflictTables[i].nSchemaObjects; j++){
      if( sqlite3_stricmp(aConflictTables[i].azSchemaObjects[j], zObject)==0 ){
        return 1;
      }
    }
  }
  return 0;
}

int hasSchemaConflictTable(
  MergeConflictTable *aConflictTables,
  int nConflictTables,
  const char *zTable
){
  int i;
  for(i=0; i<nConflictTables; i++){
    if( aConflictTables[i].nSchemaObjects>0
     && aConflictTables[i].zName
     && sqlite3_stricmp(aConflictTables[i].zName, zTable)==0 ){
      return 1;
    }
  }
  return 0;
}

int hasAnySchemaConflict(
  MergeConflictTable *aConflictTables,
  int nConflictTables
){
  int i;
  for(i=0; i<nConflictTables; i++){
    if( aConflictTables[i].nSchemaObjects>0 ) return 1;
  }
  return 0;
}

static int mergeSchemaEntriesSame(
  const SchemaEntry *pA,
  const SchemaEntry *pB
){
  if( !pA && !pB ) return 1;
  if( !pA || !pB ) return 0;
  if( sqlite3_stricmp(pA->zType ? pA->zType : "",
                      pB->zType ? pB->zType : "")!=0 ) return 0;
  if( sqlite3_stricmp(pA->zTblName ? pA->zTblName : "",
                      pB->zTblName ? pB->zTblName : "")!=0 ) return 0;
  if( (pA->zSql==0)!=(pB->zSql==0) ) return 0;
  return !pA->zSql || strcmp(pA->zSql, pB->zSql)==0;
}

static int mergeIndexFollowsDualRename(
  SchemaEntry *aAnc, int nAnc,
  SchemaEntry *aOurs, int nOurs,
  SchemaEntry *aTheirs, int nTheirs,
  const SchemaEntry *pAnc,
  const SchemaEntry *pOurs,
  const SchemaEntry *pTheirs
){
  const char *zAncBody;
  const char *zOursBody;
  const char *zTheirsBody;
  SchemaEntry *pAncTable;
  SchemaEntry *pOursTable;
  SchemaEntry *pTheirsTable;
  if( !pAnc || !pOurs || !pTheirs || !pAnc->zTblName
   || !pOurs->zTblName || !pTheirs->zTblName ){
    return 0;
  }
  if( sqlite3_stricmp(pAnc->zTblName, pOurs->zTblName)==0
   || sqlite3_stricmp(pAnc->zTblName, pTheirs->zTblName)==0
   || sqlite3_stricmp(pOurs->zTblName, pTheirs->zTblName)==0 ){
    return 0;
  }
  pAncTable = findSchemaEntry(aAnc, nAnc, pAnc->zTblName);
  pOursTable = findSchemaEntry(aOurs, nOurs, pOurs->zTblName);
  pTheirsTable = findSchemaEntry(aTheirs, nTheirs, pTheirs->zTblName);
  zAncBody = pAnc->zSql ? strchr(pAnc->zSql, '(') : 0;
  zOursBody = pOurs->zSql ? strchr(pOurs->zSql, '(') : 0;
  zTheirsBody = pTheirs->zSql ? strchr(pTheirs->zSql, '(') : 0;
  return pAncTable && pAncTable->zType
      && strcmp(pAncTable->zType, "table")==0
      && pOursTable && pOursTable->zType
      && strcmp(pOursTable->zType, "table")==0
      && pTheirsTable && pTheirsTable->zType
      && strcmp(pTheirsTable->zType, "table")==0
      && zAncBody && zOursBody && zTheirsBody
      && strcmp(zAncBody, zOursBody)==0
      && strcmp(zAncBody, zTheirsBody)==0
      && !findSchemaEntry(aAnc, nAnc, pOurs->zTblName)
      && !findSchemaEntry(aAnc, nAnc, pTheirs->zTblName);
}

static int preDetectIndexSchemaConflicts(
  SchemaEntry *aAnc, int nAnc,
  SchemaEntry *aOurs, int nOurs,
  SchemaEntry *aTheirs, int nTheirs,
  MergeConflictTable **ppConflictTables,
  int *pnConflictTables,
  int *pTotalConflicts
){
  int side, i, j, rc;
  for(side=0; side<3; side++){
    SchemaEntry *a = side==0 ? aAnc : (side==1 ? aOurs : aTheirs);
    int n = side==0 ? nAnc : (side==1 ? nOurs : nTheirs);
    for(i=0; i<n; i++){
      SchemaEntry *pAnc;
      SchemaEntry *pOurs;
      SchemaEntry *pTheirs;
      SchemaEntry *pAncTable;
      SchemaEntry *pOursTable;
      SchemaEntry *pTheirsTable;
      SchemaEntry *pRenamedTable;
      const char *zTable;
      int oursChanged;
      int theirsChanged;
      int seen = 0;
      int addedSchemaTable = 0;
      if( !a[i].zType || strcmp(a[i].zType, "index")!=0
       || !a[i].zName || !a[i].zSql ){
        continue;
      }
      for(j=0; j<i; j++){
        if( a[j].zType && strcmp(a[j].zType, "index")==0
         && a[j].zName
         && sqlite3_stricmp(a[j].zName, a[i].zName)==0 ){
          seen = 1;
          break;
        }
      }
      if( seen ) continue;
      pAnc = findSchemaEntry(aAnc, nAnc, a[i].zName);
      pOurs = findSchemaEntry(aOurs, nOurs, a[i].zName);
      pTheirs = findSchemaEntry(aTheirs, nTheirs, a[i].zName);
      oursChanged = !mergeSchemaEntriesSame(pAnc, pOurs);
      theirsChanged = !mergeSchemaEntriesSame(pAnc, pTheirs);
      if( mergeIndexFollowsDualRename(
            aAnc, nAnc, aOurs, nOurs, aTheirs, nTheirs,
            pAnc, pOurs, pTheirs) ){
        continue;
      }
      if( pAnc && pAnc->zTblName && !pOurs
       && pTheirs && pTheirs->zTblName
       && sqlite3_stricmp(pAnc->zTblName, pTheirs->zTblName)!=0 ){
        pAncTable = findSchemaEntry(aAnc, nAnc, pAnc->zTblName);
        pOursTable = findSchemaEntry(aOurs, nOurs, pAnc->zTblName);
        pTheirsTable = findSchemaEntry(aTheirs, nTheirs, pAnc->zTblName);
        pRenamedTable = findSchemaEntry(aTheirs, nTheirs,
                                        pTheirs->zTblName);
        if( pAncTable && pAncTable->zType
         && strcmp(pAncTable->zType, "table")==0
         && pOursTable && !pTheirsTable
         && pRenamedTable && pRenamedTable->zType
         && strcmp(pRenamedTable->zType, "table")==0
         && !findSchemaEntry(aAnc, nAnc, pTheirs->zTblName) ){
          rc = appendSchemaConflict(ppConflictTables, pnConflictTables,
                                    pAnc->zTblName, pAnc->zTblName,
                                    &addedSchemaTable);
          if( rc!=SQLITE_OK ) return rc;
          if( addedSchemaTable ) (*pTotalConflicts)++;
          rc = appendSchemaConflict(ppConflictTables, pnConflictTables,
                                    pAnc->zTblName, pTheirs->zTblName,
                                    &addedSchemaTable);
          if( rc!=SQLITE_OK ) return rc;
          rc = appendSchemaConflict(ppConflictTables, pnConflictTables,
                                    pAnc->zTblName, a[i].zName,
                                    &addedSchemaTable);
          if( rc!=SQLITE_OK ) return rc;
          continue;
        }
      }
      zTable = pOurs && pOurs->zTblName ? pOurs->zTblName
             : (pTheirs && pTheirs->zTblName ? pTheirs->zTblName
             : (pAnc && pAnc->zTblName ? pAnc->zTblName : a[i].zName));
      pAncTable = findSchemaEntry(aAnc, nAnc, zTable);
      pOursTable = findSchemaEntry(aOurs, nOurs, zTable);
      pTheirsTable = findSchemaEntry(aTheirs, nTheirs, zTable);
      if( pAncTable && pAncTable->zType
       && strcmp(pAncTable->zType, "table")==0
       && ((pOursTable && !pTheirsTable && oursChanged)
        || (!pOursTable && pTheirsTable && theirsChanged)) ){
        rc = appendSchemaConflict(ppConflictTables, pnConflictTables,
                                  zTable, zTable, &addedSchemaTable);
        if( rc!=SQLITE_OK ) return rc;
        if( addedSchemaTable ) (*pTotalConflicts)++;
        rc = appendSchemaConflict(ppConflictTables, pnConflictTables,
                                  zTable, a[i].zName, &addedSchemaTable);
        if( rc!=SQLITE_OK ) return rc;
        continue;
      }
      if( !oursChanged || !theirsChanged
       || mergeSchemaEntriesSame(pOurs, pTheirs) ){
        continue;
      }
      /* Dolt keeps the modified index definition when the other branch
      ** drops that index. Only competing surviving definitions conflict. */
      if( pAnc && (!pOurs || !pTheirs) ) continue;
      rc = appendSchemaConflict(ppConflictTables, pnConflictTables,
                                zTable, a[i].zName, &addedSchemaTable);
      if( rc!=SQLITE_OK ) return rc;
      if( addedSchemaTable ) (*pTotalConflicts)++;
    }
  }
  return SQLITE_OK;
}

int recordSchemaAddColumns(
  SchemaMergeAction **ppSchemaActions,
  int *pnSchemaActions,
  const char *zName,
  char **azAddCols,
  int nAddCols
){
  SchemaMergeAction *aNew;
  aNew = sqlite3_realloc(*ppSchemaActions,
    (*pnSchemaActions+1)*(int)sizeof(SchemaMergeAction));
  if( !aNew ) return SQLITE_NOMEM;
  *ppSchemaActions = aNew;
  aNew[*pnSchemaActions].zTableName = sqlite3_mprintf("%s", zName);
  if( !aNew[*pnSchemaActions].zTableName ) return SQLITE_NOMEM;
  aNew[*pnSchemaActions].azAddColumns = azAddCols;
  aNew[*pnSchemaActions].nAddColumns = nAddCols;
  (*pnSchemaActions)++;
  return SQLITE_OK;
}

int schemaEntryChangedByName(
  SchemaEntry *aAnc, int nAnc,
  SchemaEntry *aSide, int nSide,
  const char *zName
){
  SchemaEntry *pAnc = findSchemaEntry(aAnc, nAnc, zName);
  SchemaEntry *pSide = findSchemaEntry(aSide, nSide, zName);
  if( pAnc==0 && pSide==0 ) return 0;
  if( pAnc==0 || pSide==0 ) return 1;
  if( pAnc->zType && pSide->zType
   && strcmp(pAnc->zType, pSide->zType)!=0 ) return 1;
  if( pAnc->zTblName && pSide->zTblName
   && strcmp(pAnc->zTblName, pSide->zTblName)!=0 ) return 1;
  if( (pAnc->zSql==0) != (pSide->zSql==0) ) return 1;
  if( pAnc->zSql && strcmp(pAnc->zSql, pSide->zSql)!=0 ) return 1;
  return 0;
}

static int hasSchemaObject(
  SchemaEntry *aSchema,
  int nSchema,
  const char *zType,
  const char *zName,
  const char *zTblName
){
  int i;
  for(i=0; i<nSchema; i++){
    if( strcmp(aSchema[i].zType ? aSchema[i].zType : "", zType ? zType : "")!=0 ) continue;
    if( strcmp(aSchema[i].zName ? aSchema[i].zName : "", zName ? zName : "")!=0 ) continue;
    if( strcmp(aSchema[i].zTblName ? aSchema[i].zTblName : "",
               zTblName ? zTblName : "")!=0 ) continue;
    return 1;
  }
  return 0;
}

int replayDropsDisjointSchemaObject(
  SchemaEntry *aAncSchema, int nAncSchema,
  SchemaEntry *aTheirsSchema, int nTheirsSchema
){
  int i;
  for(i=0; i<nAncSchema; i++){
    const char *zType = aAncSchema[i].zType;
    if( !zType ) continue;
    if( strcmp(zType, "table")!=0 && strcmp(zType, "index")!=0 ) continue;
    if( !hasSchemaObject(aTheirsSchema, nTheirsSchema,
                         aAncSchema[i].zType,
                         aAncSchema[i].zName,
                         aAncSchema[i].zTblName) ){
      return 1;
    }
  }
  return 0;
}

SchemaEntry *findSchemaEntryByRootpage(
  SchemaEntry *aSchema,
  int nSchema,
  Pgno iRootpage
){
  int i;
  for(i=0; i<nSchema; i++){
    if( aSchema[i].iRootpage==iRootpage ) return &aSchema[i];
  }
  return 0;
}

static int mergeTableSchemaBodiesSame(
  const SchemaEntry *pA,
  const SchemaEntry *pB
){
  const char *zA;
  const char *zB;
  if( !pA || !pA->zType || !pA->zSql
   || !pB || !pB->zType || !pB->zSql
   || strcmp(pA->zType, "table")!=0
   || strcmp(pB->zType, "table")!=0 ){
    return 0;
  }
  zA = strchr(pA->zSql, '(');
  zB = strchr(pB->zSql, '(');
  return zA && zB && strcmp(zA, zB)==0;
}

static int mergeRenameNamesExclusive(
  struct TableEntry *aAnc, int nAnc,
  struct TableEntry *aDropped, int nDropped,
  struct TableEntry *aRenamed, int nRenamed,
  const char *zOld,
  const char *zNew
){
  return !doltliteFindTableByName(aRenamed, nRenamed, zOld)
      && !doltliteFindTableByName(aAnc, nAnc, zNew)
      && !doltliteFindTableByName(aDropped, nDropped, zOld)
      && !doltliteFindTableByName(aDropped, nDropped, zNew);
}

int mergeTableRenameOtherDrop(
  struct TableEntry *aAnc, int nAnc,
  struct TableEntry *aDropped, int nDropped,
  struct TableEntry *aRenamed, int nRenamed,
  SchemaEntry *aAncSchema, int nAncSchema,
  SchemaEntry *aRenamedSchema, int nRenamedSchema,
  struct TableEntry *pRenamed,
  const char **pzAncName
){
  struct TableEntry *pAnc;
  SchemaEntry *pAncSe;
  SchemaEntry *pRenamedSe;
  int i;

  if( !pRenamed || !pRenamed->zName || pRenamed->iTable<=1 ) return 0;
  pAnc = doltliteFindTableByNumber(aAnc, nAnc, pRenamed->iTable);
  pAncSe = pAnc ? findSchemaEntryByRootpage(
      aAncSchema, nAncSchema, pAnc->iTable) : 0;
  pRenamedSe = findSchemaEntryByRootpage(
      aRenamedSchema, nRenamedSchema, pRenamed->iTable);
  if( !pRenamedSe || !pRenamedSe->zType
   || strcmp(pRenamedSe->zType, "table")!=0 ) return 0;
  if( !pAnc || !pAnc->zName
   || strcmp(pAnc->zName, pRenamed->zName)==0
   || prollyHashCompare(&pAnc->root, &pRenamed->root)!=0
   || !mergeTableSchemaBodiesSame(pAncSe, pRenamedSe) ){
    pAnc = 0;
    pAncSe = 0;
  }
  if( pAnc && !mergeRenameNamesExclusive(
        aAnc, nAnc, aDropped, nDropped, aRenamed, nRenamed,
        pAnc->zName, pRenamed->zName) ){
    pAnc = 0;
    pAncSe = 0;
  }
  if( !pAnc ){
    for(i=0; i<nRenamedSchema; i++){
      SchemaEntry *pAncIdx;
      SchemaEntry *pCandidate;
      struct TableEntry *pCandidateEntry;
      if( !aRenamedSchema[i].zType || !aRenamedSchema[i].zName
       || !aRenamedSchema[i].zTblName
       || strcmp(aRenamedSchema[i].zType, "index")!=0
       || sqlite3_stricmp(aRenamedSchema[i].zTblName,
                          pRenamed->zName)!=0 ){
        continue;
      }
      pAncIdx = findSchemaEntry(
          aAncSchema, nAncSchema, aRenamedSchema[i].zName);
      if( !pAncIdx || !pAncIdx->zType || !pAncIdx->zTblName
       || strcmp(pAncIdx->zType, "index")!=0
       || sqlite3_stricmp(pAncIdx->zTblName, pRenamed->zName)==0 ){
        continue;
      }
      pCandidate = findSchemaEntry(
          aAncSchema, nAncSchema, pAncIdx->zTblName);
      if( !mergeTableSchemaBodiesSame(pCandidate, pRenamedSe) ){
        continue;
      }
      pCandidateEntry = doltliteFindTableByName(
          aAnc, nAnc, pCandidate->zName);
      if( pCandidateEntry
       && mergeRenameNamesExclusive(
            aAnc, nAnc, aDropped, nDropped, aRenamed, nRenamed,
            pCandidate->zName, pRenamed->zName) ){
        pAnc = pCandidateEntry;
        pAncSe = pCandidate;
        break;
      }
    }
  }
  if( !pAnc || !pAncSe ) return 0;
  /* The name the object had in the ancestor. Its catalog rows are the ones a
  ** merge has to resolve toward the rename, and only this function can name
  ** them: the row merge sees a rename as a delete plus an add, so it cannot
  ** tell this shape from a rename on both sides. */
  if( pzAncName ) *pzAncName = pAnc->zName;
  return 1;
}

struct TableEntry *findCatalogEntryBySchemaObject(
  struct TableEntry *aCat,
  int nCat,
  SchemaEntry *aSchema,
  int nSchema,
  const char *zType,
  const char *zName,
  const char *zTblName
){
  int i;
  for(i=0; i<nSchema; i++){
    if( strcmp(aSchema[i].zType ? aSchema[i].zType : "", zType ? zType : "")!=0 ) continue;
    if( strcmp(aSchema[i].zName ? aSchema[i].zName : "", zName ? zName : "")!=0 ) continue;
    if( strcmp(aSchema[i].zTblName ? aSchema[i].zTblName : "", zTblName ? zTblName : "")!=0 ) continue;
    return doltliteFindTableByNumber(aCat, nCat, aSchema[i].iRootpage);
  }
  return 0;
}

static int schemaChangesOverlapByName(
  SchemaEntry *aAnc, int nAnc,
  SchemaEntry *aOurs, int nOurs,
  SchemaEntry *aTheirs, int nTheirs
){
  int i;
  for(i=0; i<nOurs; i++){
    const char *zName = aOurs[i].zName;
    if( !zName ) continue;
    if( schemaEntryChangedByName(aAnc, nAnc, aOurs, nOurs, zName)
     && schemaEntryChangedByName(aAnc, nAnc, aTheirs, nTheirs, zName) ){
      return 1;
    }
  }
  for(i=0; i<nAnc; i++){
    const char *zName = aAnc[i].zName;
    if( !zName ) continue;
    if( findSchemaEntry(aOurs, nOurs, zName) ) continue;
    if( schemaEntryChangedByName(aAnc, nAnc, aTheirs, nTheirs, zName) ){
      return 1;
    }
  }
  return 0;
}

static int catalogHasDisjointSchemaChanges(
  sqlite3 *db,
  const ProllyHash *pCatAnc,
  const ProllyHash *pCatOurs,
  const ProllyHash *pCatTheirs
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  SchemaEntry *aAncSchema = 0, *aOursSchema = 0, *aTheirsSchema = 0;
  int nAncSchema = 0, nOursSchema = 0, nTheirsSchema = 0;
  int i, rc;
  int sawOursChanged = 0, sawTheirsChanged = 0;
  int result = 0;

  if( !cs || !pCache ) return 0;
  rc = loadSchemaFromCatalog(db, cs, pCache, pCatAnc, &aAncSchema, &nAncSchema);
  if( rc!=SQLITE_OK ) goto done;
  rc = loadSchemaFromCatalog(db, cs, pCache, pCatOurs, &aOursSchema, &nOursSchema);
  if( rc!=SQLITE_OK ) goto done;
  rc = loadSchemaFromCatalog(db, cs, pCache, pCatTheirs, &aTheirsSchema, &nTheirsSchema);
  if( rc!=SQLITE_OK ) goto done;

  for(i=0; i<nOursSchema; i++){
    if( aOursSchema[i].zName
     && schemaEntryChangedByName(aAncSchema, nAncSchema,
                                 aOursSchema, nOursSchema,
                                 aOursSchema[i].zName) ){
      sawOursChanged = 1;
      break;
    }
  }
  if( !sawOursChanged ){
    for(i=0; i<nAncSchema; i++){
      if( aAncSchema[i].zName
       && !findSchemaEntry(aOursSchema, nOursSchema, aAncSchema[i].zName) ){
        sawOursChanged = 1;
        break;
      }
    }
  }

  for(i=0; i<nTheirsSchema; i++){
    if( aTheirsSchema[i].zName
     && schemaEntryChangedByName(aAncSchema, nAncSchema,
                                 aTheirsSchema, nTheirsSchema,
                                 aTheirsSchema[i].zName) ){
      sawTheirsChanged = 1;
      break;
    }
  }
  if( !sawTheirsChanged ){
    for(i=0; i<nAncSchema; i++){
      if( aAncSchema[i].zName
       && !findSchemaEntry(aTheirsSchema, nTheirsSchema, aAncSchema[i].zName) ){
        sawTheirsChanged = 1;
        break;
      }
    }
  }

  if( sawOursChanged && sawTheirsChanged
   && !schemaChangesOverlapByName(aAncSchema, nAncSchema,
                                  aOursSchema, nOursSchema,
                                  aTheirsSchema, nTheirsSchema) ){
    result = 1;
  }
done:
  freeSchemaEntries(aAncSchema, nAncSchema);
  freeSchemaEntries(aOursSchema, nOursSchema);
  freeSchemaEntries(aTheirsSchema, nTheirsSchema);
  return result;
}

static Pgno remapSchemaRootpage(
  SchemaRootpageRemap *aRemap,
  int nRemap,
  Pgno iRootpage
){
  int i;
  for(i=0; i<nRemap; i++){
    if( aRemap[i].oldPg==iRootpage ) return aRemap[i].newPg;
  }
  return iRootpage;
}

static u8 *mergeBuildSchemaCatalogRecord(
  const char *zType,
  const char *zName,
  const char *zTblName,
  i64 iRootpage,
  const char *zSql,
  int *pnOut
){
  DoltliteSerialValue aMem[5];

  memset(aMem, 0, sizeof(aMem));

  aMem[0].eType = SQLITE_TEXT;    aMem[0].p = zType;    aMem[0].n = (int)strlen(zType);
  aMem[1].eType = SQLITE_TEXT;    aMem[1].p = zName;    aMem[1].n = (int)strlen(zName);
  aMem[2].eType = SQLITE_TEXT;    aMem[2].p = zTblName; aMem[2].n = (int)strlen(zTblName);
  aMem[3].eType = SQLITE_INTEGER; aMem[3].i = iRootpage;
  if( zSql ){
    aMem[4].eType = SQLITE_TEXT;
    aMem[4].p = zSql;
    aMem[4].n = (int)strlen(zSql);
  }else{
    aMem[4].eType = SQLITE_NULL;
    aMem[4].p = 0;
    aMem[4].n = 0;
  }

  return doltliteBuildRecord(aMem, 5, pnOut);
}

static SchemaEntry *mergedSchemaChoice(
  SchemaEntry *aAncSchema, int nAncSchema,
  SchemaEntry *aOursSchema, int nOursSchema,
  SchemaEntry *aTheirsSchema, int nTheirsSchema,
  MergeConflictTable *aConflictTables, int nConflictTables,
  const char *zName
){
  SchemaEntry *pAnc = findSchemaEntry(aAncSchema, nAncSchema, zName);
  SchemaEntry *pOurs = findSchemaEntry(aOursSchema, nOursSchema, zName);
  SchemaEntry *pTheirs = findSchemaEntry(aTheirsSchema, nTheirsSchema, zName);
  int oursChanged = schemaEntryChangedByName(aAncSchema, nAncSchema,
                                             aOursSchema, nOursSchema, zName);
  int theirsChanged = schemaEntryChangedByName(aAncSchema, nAncSchema,
                                               aTheirsSchema, nTheirsSchema, zName);
  if( hasSchemaConflictObject(aConflictTables, nConflictTables, zName) ){
    return pOurs;
  }
  if( oursChanged && !theirsChanged ) return pOurs;
  if( theirsChanged && !oursChanged ) return pTheirs;
  /* Both sides dropped the object: falling back to the ancestor would
  ** resurrect it in the merged catalog. */
  if( oursChanged && theirsChanged && !pOurs && !pTheirs ) return 0;
  if( pOurs ) return pOurs;
  if( pTheirs ) return pTheirs;
  return pAnc;
}

static int appendMergedSchemaCatalogRecord(
  sqlite3 *db,
  ProllyHash *pRoot,
  u8 flags,
  i64 iRowid,
  const SchemaEntry *pSe,
  Pgno iRootpage
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  u8 *pRec = 0;
  int nRec = 0;
  int rc;

  if( !pSe || !pSe->zName || !pSe->zType ) return SQLITE_OK;
  pRec = mergeBuildSchemaCatalogRecord(pSe->zType, pSe->zName,
                                  pSe->zTblName ? pSe->zTblName : pSe->zName,
                                  (i64)iRootpage, pSe->zSql, &nRec);
  if( !pRec ) return SQLITE_NOMEM;
  rc = prollyMutateInsert(cs, pCache, pRoot, flags, 0, 0,
                          iRowid, pRec, nRec, pRoot);
  sqlite3_free(pRec);
  return rc;
}

static int appendMergedHiddenIndexRow(
  sqlite3 *db,
  ProllyHash *pRoot,
  u8 flags,
  i64 *piNextRowid,
  SchemaEntry *aAncSchema, int nAncSchema,
  SchemaEntry *aOursSchema, int nOursSchema,
  SchemaEntry *aTheirsSchema, int nTheirsSchema,
  MergeConflictTable *aConflictTables, int nConflictTables,
  SchemaRootpageRemap *aRemap, int nRemap,
  const char *zName
){
  SchemaEntry *pSe;
  Pgno iRootpage;

  if( !zName ) return SQLITE_OK;
  pSe = mergedSchemaChoice(aAncSchema, nAncSchema,
                           aOursSchema, nOursSchema,
                           aTheirsSchema, nTheirsSchema,
                           aConflictTables, nConflictTables,
                           zName);
  if( !pSe || !pSe->zType || !pSe->zName ) return SQLITE_OK;
  if( strcmp(pSe->zType, "index")!=0 ) return SQLITE_OK;
  if( pSe->zSql ) return SQLITE_OK;

  iRootpage = pSe->iRootpage;
  if( pSe>=aTheirsSchema && pSe<aTheirsSchema+nTheirsSchema ){
    iRootpage = remapSchemaRootpage(aRemap, nRemap, iRootpage);
  }
  return appendMergedSchemaCatalogRecord(db, pRoot, flags, (*piNextRowid)++, pSe, iRootpage);
}

static int appendMergedAuxSchemaRow(
  sqlite3 *db,
  ProllyHash *pRoot,
  u8 flags,
  i64 *piNextRowid,
  SchemaEntry *aAncSchema, int nAncSchema,
  SchemaEntry *aOursSchema, int nOursSchema,
  SchemaEntry *aTheirsSchema, int nTheirsSchema,
  MergeConflictTable *aConflictTables, int nConflictTables,
  SchemaRootpageRemap *aRemap, int nRemap,
  const char *zName
){
  SchemaEntry *pSe;
  Pgno iRootpage;

  if( !zName ) return SQLITE_OK;
  pSe = mergedSchemaChoice(aAncSchema, nAncSchema,
                           aOursSchema, nOursSchema,
                           aTheirsSchema, nTheirsSchema,
                           aConflictTables, nConflictTables,
                           zName);
  if( !pSe || !pSe->zType || !pSe->zName ) return SQLITE_OK;
  /* Storage-backed tables are appended from the merged catalog entries and
  ** indexes from the index passes. Virtual tables have no catalog entry —
  ** their storage is the shadow tables — so their schema rows (type "table",
  ** rootpage 0) ride with the storage-free objects here. */
  if( strcmp(pSe->zType, "index")==0 ) return SQLITE_OK;
  if( strcmp(pSe->zType, "table")==0 && pSe->iRootpage!=0 ){
    return SQLITE_OK;
  }

  iRootpage = pSe->iRootpage;
  if( pSe>=aTheirsSchema && pSe<aTheirsSchema+nTheirsSchema ){
    iRootpage = remapSchemaRootpage(aRemap, nRemap, iRootpage);
  }
  return appendMergedSchemaCatalogRecord(db, pRoot, flags, (*piNextRowid)++, pSe, iRootpage);
}

static int rebuildDisjointSchemaRows(
  sqlite3 *db,
  struct TableEntry *aMerged, int nMerged,
  SchemaEntry *aTheirsSchema, int nTheirsSchema,
  SchemaEntry *aAncSchema, int nAncSchema,
  SchemaEntry *aOursSchema, int nOursSchema,
  MergeConflictTable *aConflictTables, int nConflictTables,
  SchemaRootpageRemap *aRemap, int nRemap
){
  struct TableEntry *pMaster = 0;
  ProllyHash root;
  i64 iNextRowid = 1;
  int i, rc = SQLITE_OK;

  for(i=0; i<nMerged; i++){
    if( aMerged[i].iTable==1 ){
      pMaster = &aMerged[i];
      break;
    }
  }
  if( !pMaster ) return SQLITE_OK;
  memset(&root, 0, sizeof(root));

  for(i=0; i<nMerged; i++){
    const char *zName = aMerged[i].zName;
    SchemaEntry *pSe = 0;

    if( aMerged[i].iTable<=1 || !zName ) continue;
    pSe = mergedSchemaChoice(aAncSchema, nAncSchema,
                             aOursSchema, nOursSchema,
                             aTheirsSchema, nTheirsSchema,
                             aConflictTables, nConflictTables,
                             zName);
    rc = appendMergedSchemaCatalogRecord(db, &root, pMaster->flags, iNextRowid++,
                                         pSe, aMerged[i].iTable);
    if( rc!=SQLITE_OK ) return rc;
  }

  for(i=0; i<nOursSchema; i++){
    SchemaEntry *pSe = &aOursSchema[i];
    if( !pSe->zName || !pSe->zType ) continue;
    if( strcmp(pSe->zType, "index")!=0 ) continue;
    if( !schemaEntryChangedByName(aAncSchema, nAncSchema,
                                  aOursSchema, nOursSchema,
                                  pSe->zName) ){
      continue;
    }
    rc = appendMergedSchemaCatalogRecord(db, &root, pMaster->flags, iNextRowid++,
                                         pSe, pSe->iRootpage);
    if( rc!=SQLITE_OK ) return rc;
  }

  for(i=0; i<nTheirsSchema; i++){
    SchemaEntry *pSe = &aTheirsSchema[i];
    Pgno iRootpage;
    if( !pSe->zName || !pSe->zType ) continue;
    if( strcmp(pSe->zType, "index")!=0 ) continue;
    if( !pSe->zTblName
     || !doltliteFindTableByName(aMerged, nMerged, pSe->zTblName) ){
      continue;
    }
    if( hasSchemaConflictObject(aConflictTables, nConflictTables, pSe->zName)
     || hasSchemaConflictTable(aConflictTables, nConflictTables,
                               pSe->zTblName) ){
      continue;
    }
    if( !schemaEntryChangedByName(aAncSchema, nAncSchema,
                                  aTheirsSchema, nTheirsSchema,
                                  pSe->zName) ){
      continue;
    }
    if( findSchemaEntry(aOursSchema, nOursSchema, pSe->zName)
     && schemaEntryChangedByName(aAncSchema, nAncSchema,
                                 aOursSchema, nOursSchema,
                                 pSe->zName) ){
      continue;
    }
    iRootpage = remapSchemaRootpage(aRemap, nRemap, pSe->iRootpage);
    rc = appendMergedSchemaCatalogRecord(db, &root, pMaster->flags, iNextRowid++,
                                         pSe, iRootpage);
    if( rc!=SQLITE_OK ) return rc;
  }

  for(i=0; i<nAncSchema; i++){
    rc = appendMergedHiddenIndexRow(db, &root, pMaster->flags, &iNextRowid,
                                    aAncSchema, nAncSchema,
                                    aOursSchema, nOursSchema,
                                    aTheirsSchema, nTheirsSchema,
                                    aConflictTables, nConflictTables,
                                    aRemap, nRemap,
                                    aAncSchema[i].zName);
    if( rc!=SQLITE_OK ) return rc;
  }
  for(i=0; i<nOursSchema; i++){
    if( findSchemaEntry(aAncSchema, nAncSchema, aOursSchema[i].zName) ) continue;
    rc = appendMergedHiddenIndexRow(db, &root, pMaster->flags, &iNextRowid,
                                    aAncSchema, nAncSchema,
                                    aOursSchema, nOursSchema,
                                    aTheirsSchema, nTheirsSchema,
                                    aConflictTables, nConflictTables,
                                    aRemap, nRemap,
                                    aOursSchema[i].zName);
    if( rc!=SQLITE_OK ) return rc;
  }
  for(i=0; i<nTheirsSchema; i++){
    if( findSchemaEntry(aAncSchema, nAncSchema, aTheirsSchema[i].zName) ) continue;
    if( findSchemaEntry(aOursSchema, nOursSchema, aTheirsSchema[i].zName) ) continue;
    rc = appendMergedHiddenIndexRow(db, &root, pMaster->flags, &iNextRowid,
                                    aAncSchema, nAncSchema,
                                    aOursSchema, nOursSchema,
                                    aTheirsSchema, nTheirsSchema,
                                    aConflictTables, nConflictTables,
                                    aRemap, nRemap,
                                    aTheirsSchema[i].zName);
    if( rc!=SQLITE_OK ) return rc;
  }

  for(i=0; i<nAncSchema; i++){
    rc = appendMergedAuxSchemaRow(db, &root, pMaster->flags, &iNextRowid,
                                  aAncSchema, nAncSchema,
                                  aOursSchema, nOursSchema,
                                  aTheirsSchema, nTheirsSchema,
                                  aConflictTables, nConflictTables,
                                  aRemap, nRemap,
                                  aAncSchema[i].zName);
    if( rc!=SQLITE_OK ) return rc;
  }
  for(i=0; i<nOursSchema; i++){
    if( findSchemaEntry(aAncSchema, nAncSchema, aOursSchema[i].zName) ) continue;
    rc = appendMergedAuxSchemaRow(db, &root, pMaster->flags, &iNextRowid,
                                  aAncSchema, nAncSchema,
                                  aOursSchema, nOursSchema,
                                  aTheirsSchema, nTheirsSchema,
                                  aConflictTables, nConflictTables,
                                  aRemap, nRemap,
                                  aOursSchema[i].zName);
    if( rc!=SQLITE_OK ) return rc;
  }
  for(i=0; i<nTheirsSchema; i++){
    if( findSchemaEntry(aAncSchema, nAncSchema, aTheirsSchema[i].zName) ) continue;
    if( findSchemaEntry(aOursSchema, nOursSchema, aTheirsSchema[i].zName) ) continue;
    rc = appendMergedAuxSchemaRow(db, &root, pMaster->flags, &iNextRowid,
                                  aAncSchema, nAncSchema,
                                  aOursSchema, nOursSchema,
                                  aTheirsSchema, nTheirsSchema,
                                  aConflictTables, nConflictTables,
                                  aRemap, nRemap,
                                  aTheirsSchema[i].zName);
    if( rc!=SQLITE_OK ) return rc;
  }

  memcpy(&pMaster->root, &root, sizeof(root));
  return rc;
}

int tryResolveSchemaDivergence(
  sqlite3 *db,
  const char *zName,
  const ProllyHash *pCatAnc,
  const ProllyHash *pCatOurs,
  const ProllyHash *pCatTheirs,
  SchemaMergeAction **ppSchemaActions,
  int *pnSchemaActions,
  int *pSkipRowMerge,
  int *pSchemaChoice,
  char **pzErrMsg
){
  ChunkStore *csLocal;
  ProllyCache *cacheLocal;
  SchemaEntry *aAncSchema = 0;
  SchemaEntry *aOursSchema = 0;
  SchemaEntry *aTheirsSchema = 0;
  int nAncSchema = 0;
  int nOursSchema = 0;
  int nTheirsSchema = 0;
  SchemaEntry *ancSchEntry;
  SchemaEntry *ourSchEntry;
  SchemaEntry *theirSchEntry;
  char **azAddCols = 0;
  int nAddCols = 0;
  int schemaChoice = SCHEMA_MERGE_DEFAULT;
  int resolvedDivergence = 0;
  char *zSchemaErr = 0;
  int rc;

  *pSkipRowMerge = 0;
  *pSchemaChoice = SCHEMA_MERGE_DEFAULT;
  csLocal = doltliteGetChunkStore(db);
  cacheLocal = doltliteGetCache(db);
  loadSchemaFromCatalog(db, csLocal, cacheLocal, pCatAnc, &aAncSchema, &nAncSchema);
  loadSchemaFromCatalog(db, csLocal, cacheLocal, pCatOurs, &aOursSchema, &nOursSchema);
  loadSchemaFromCatalog(db, csLocal, cacheLocal, pCatTheirs, &aTheirsSchema, &nTheirsSchema);

  ancSchEntry = findSchemaEntry(aAncSchema, nAncSchema, zName);
  ourSchEntry = findSchemaEntry(aOursSchema, nOursSchema, zName);
  theirSchEntry = findSchemaEntry(aTheirsSchema, nTheirsSchema, zName);

  if( ancSchEntry && ancSchEntry->zSql
   && ourSchEntry && ourSchEntry->zSql
   && theirSchEntry && theirSchEntry->zSql ){
    rc = trySchemaColumnMerge(
      ancSchEntry->zSql, ourSchEntry->zSql, theirSchEntry->zSql,
      &azAddCols, &nAddCols, &schemaChoice,
      &resolvedDivergence, &zSchemaErr);
  }else{
    rc = SQLITE_ERROR;
    zSchemaErr = sqlite3_mprintf("cannot load schemas for merge");
  }

  freeSchemaEntries(aAncSchema, nAncSchema);
  freeSchemaEntries(aOursSchema, nOursSchema);
  freeSchemaEntries(aTheirsSchema, nTheirsSchema);

  if( rc!=SQLITE_OK ){
    if( pzErrMsg ){
      if( zSchemaErr ){
        *pzErrMsg = sqlite3_mprintf(
          "schema conflict on table '%s' \xe2\x80\x94 %s",
          zName ? zName : "(unknown)", zSchemaErr);
      }else{
        *pzErrMsg = sqlite3_mprintf(
          "schema conflict on table '%s'",
          zName ? zName : "(unknown)");
      }
    }
    sqlite3_free(zSchemaErr);
    freeAddedColumns(azAddCols, nAddCols);
    return SQLITE_ERROR;
  }
  sqlite3_free(zSchemaErr);

  if( schemaChoice!=SCHEMA_MERGE_DEFAULT ){
    if( ppSchemaActions && pnSchemaActions ){
      rc = recordSchemaAddColumns(ppSchemaActions, pnSchemaActions, zName,
                                  azAddCols, nAddCols);
      if( rc!=SQLITE_OK ){
        freeAddedColumns(azAddCols, nAddCols);
        return rc;
      }
      azAddCols = 0;
      nAddCols = 0;
    }
    *pSchemaChoice = schemaChoice;
    freeAddedColumns(azAddCols, nAddCols);
    return SQLITE_OK;
  }

  if( nAddCols==0 && resolvedDivergence
   && ppSchemaActions && pnSchemaActions ){
    rc = recordSchemaAddColumns(ppSchemaActions, pnSchemaActions, zName, 0, 0);
    if( rc!=SQLITE_OK ) return rc;
  }

  if( nAddCols>0 ){
    if( ppSchemaActions && pnSchemaActions ){
      rc = recordSchemaAddColumns(ppSchemaActions, pnSchemaActions, zName,
                                  azAddCols, nAddCols);
      if( rc!=SQLITE_OK ){
        freeAddedColumns(azAddCols, nAddCols);
        return rc;
      }
      azAddCols = 0;
      nAddCols = 0;
    }
    freeAddedColumns(azAddCols, nAddCols);
    *pSkipRowMerge = 1;
    return SQLITE_OK;
  }

  freeAddedColumns(azAddCols, nAddCols);
  return SQLITE_OK;
}


static void mergeFreeConflictTables(
  MergeConflictTable *aConflictTables,
  int nConflictTables
){
  int ci;
  for(ci=0; ci<nConflictTables; ci++){
    int oi;
    freeConflictRows(aConflictTables[ci].aRows, aConflictTables[ci].nConflicts);
    for(oi=0; oi<aConflictTables[ci].nSchemaObjects; oi++){
      sqlite3_free(aConflictTables[ci].azSchemaObjects[oi]);
    }
    sqlite3_free(aConflictTables[ci].azSchemaObjects);
    sqlite3_free(aConflictTables[ci].zName);
  }
  sqlite3_free(aConflictTables);
}

static int loadMergeCatalogs(
  sqlite3 *db,
  const ProllyHash *ancestor,
  const ProllyHash *ours,
  const ProllyHash *theirs,
  struct TableEntry **paAnc, int *pnAnc, Pgno *piNextAnc,
  struct TableEntry **paOurs, int *pnOurs, Pgno *piNextOurs,
  struct TableEntry **paTheirs, int *pnTheirs, Pgno *piNextTheirs
){
  int rc;
  rc = doltliteLoadCatalog(db, ancestor, paAnc, pnAnc, piNextAnc);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteLoadCatalog(db, ours, paOurs, pnOurs, piNextOurs);
  if( rc!=SQLITE_OK ) return rc;
  return doltliteLoadCatalog(db, theirs, paTheirs, pnTheirs, piNextTheirs);
}

static int allocMergedCatalogEntries(
  int nOurs,
  int nTheirs,
  struct TableEntry **paMerged
){
  int nMergedAlloc = nOurs + nTheirs;
  if( nMergedAlloc==0 ) nMergedAlloc = 1;
  *paMerged = sqlite3_malloc(nMergedAlloc * (int)sizeof(struct TableEntry));
  return *paMerged ? SQLITE_OK : SQLITE_NOMEM;
}

static int recordMergeConflicts(
  sqlite3 *db,
  MergeConflictTable *aConflictTables,
  int nConflictTables
){
  ProllyHash conflictsHash;
  int rc;

  rc = doltliteSerializeConflicts(
      doltliteGetChunkStore(db),
      aConflictTables, nConflictTables,
      &conflictsHash);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteSetSessionConflictsCatalog(db, &conflictsHash);
  if( rc!=SQLITE_OK ) return rc;
  return doltliteSetSessionMergeConflicts(db, &conflictsHash);
}

int doltliteMergeCatalogs(
  sqlite3 *db,
  const ProllyHash *ancestor,
  const ProllyHash *ours,
  const ProllyHash *theirs,
  ProllyHash *pMergedHash,
  int *pnConflicts,
  char **pzErrMsg,
  SchemaMergeAction **ppActions,
  int *pnActions,
  int bPreferOurMaster,
  char ***pazReindex,
  int *pnReindex,
  char ***pazRebuildVtabs,
  int *pnRebuildVtabs
){
  struct TableEntry *aAnc = 0, *aOurs = 0, *aTheirs = 0;
  int nAnc = 0, nOurs = 0, nTheirs = 0;
  Pgno iNextAnc = 2, iNextOurs = 2, iNextTheirs = 2;
  struct TableEntry *aMerged = 0;
  int nMerged = 0;
  Pgno iNextMerged;
  int rc;
  int totalConflicts = 0;
  int bDisjointSchemaChanges = 0;
  SchemaRootpageRemap *aRemap = 0;
  int nRemap = 0;
  SchemaEntry *aAncSchema = 0, *aOursSchema = 0, *aTheirsSchema = 0;
  int nAncSchema = 0, nOursSchema = 0, nTheirsSchema = 0;

  MergeConflictTable *aConflictTables = 0;
  int nConflictTables = 0;

  assert( db!=0 && ancestor!=0 && ours!=0 && theirs!=0 && pMergedHash!=0 );
  rc = loadMergeCatalogs(db, ancestor, ours, theirs,
                         &aAnc, &nAnc, &iNextAnc,
                         &aOurs, &nOurs, &iNextOurs,
                         &aTheirs, &nTheirs, &iNextTheirs);
  if( rc!=SQLITE_OK ) goto merge_cleanup;

  rc = allocMergedCatalogEntries(nOurs, nTheirs, &aMerged);
  if( rc!=SQLITE_OK ) goto merge_cleanup;

  {
    ChunkStore *cs = doltliteGetChunkStore(db);
    ProllyCache *pCache = doltliteGetCache(db);
    rc = loadSchemaFromCatalog(db, cs, pCache, ancestor, &aAncSchema, &nAncSchema);
    if( rc==SQLITE_OK ) rc = loadSchemaFromCatalog(db, cs, pCache, ours, &aOursSchema, &nOursSchema);
    if( rc==SQLITE_OK ) rc = loadSchemaFromCatalog(db, cs, pCache, theirs, &aTheirsSchema, &nTheirsSchema);
    if( rc!=SQLITE_OK ) goto merge_cleanup;
  }

  iNextMerged = iNextOurs > iNextTheirs ? iNextOurs : iNextTheirs;
  bDisjointSchemaChanges = catalogHasDisjointSchemaChanges(db, ancestor, ours, theirs);

  rc = preDetectIndexSchemaConflicts(
      aAncSchema, nAncSchema, aOursSchema, nOursSchema,
      aTheirsSchema, nTheirsSchema,
      &aConflictTables, &nConflictTables, &totalConflicts);
  if( rc!=SQLITE_OK ) goto merge_cleanup;

  rc = mergeCatalogPass1(db, aAnc, nAnc, aOurs, nOurs, aTheirs, nTheirs,
                          aAncSchema, nAncSchema,
                          aOursSchema, nOursSchema,
                          aTheirsSchema, nTheirsSchema,
                          aMerged, &nMerged,
                          &aConflictTables, &nConflictTables,
                          &totalConflicts, pzErrMsg,
                          ancestor, ours, theirs,
                          ppActions, pnActions,
                          bDisjointSchemaChanges,
                          bPreferOurMaster,
                          pazReindex, pnReindex);
  if( rc!=SQLITE_OK ){
    int k;
    for(k=0; k<nMerged; k++) aMerged[k].zName = 0;
    goto merge_cleanup;
  }

  {
    int k;
    for(k=0; k<nMerged; k++){
      if( aMerged[k].zName ){
        char *z = sqlite3_mprintf("%s", aMerged[k].zName);
        if( !z ){
          /* Entries not yet re-duped still alias aOurs; null them so cleanup
          ** frees each aliased name once (via aOurs), never twice. */
          int j;
          for(j=k; j<nMerged; j++) aMerged[j].zName = 0;
          rc = SQLITE_NOMEM;
          goto merge_cleanup;
        }
        aMerged[k].zName = z;
      }
    }
  }

  rc = mergeCatalogPass2(aAnc, nAnc, aOurs, nOurs, aTheirs, nTheirs,
                          aAncSchema, nAncSchema,
                          aOursSchema, nOursSchema,
                          aTheirsSchema, nTheirsSchema,
                          aMerged, &nMerged, &iNextMerged,
                          bDisjointSchemaChanges,
                          aConflictTables, nConflictTables,
                          &aRemap, &nRemap,
                          pazReindex, pnReindex);
  if( rc!=SQLITE_OK ) goto merge_cleanup;

  rc = rebuildDisjointSchemaRows(db, aMerged, nMerged,
                                 aTheirsSchema, nTheirsSchema,
                                 aAncSchema, nAncSchema,
                                 aOursSchema, nOursSchema,
                                 aConflictTables, nConflictTables,
                                 aRemap, nRemap);
  if( rc!=SQLITE_OK ) goto merge_cleanup;

  if( rc==SQLITE_OK && nConflictTables>0 && pazRebuildVtabs ){
    rc = mergeFilterDerivedShadowConflicts(db,
        aConflictTables, &nConflictTables, &totalConflicts,
        pazRebuildVtabs, pnRebuildVtabs);
  }
  if( rc!=SQLITE_OK ) goto merge_cleanup;

  rc = serializeMergedCatalog(db, ours, aMerged, nMerged, iNextMerged,
                              aTheirsSchema, nTheirsSchema, pMergedHash);

  if( totalConflicts>0 && nConflictTables>0 && rc==SQLITE_OK ){
    rc = recordMergeConflicts(db, aConflictTables, nConflictTables);
  }
  if( pnConflicts && rc==SQLITE_OK ) *pnConflicts = totalConflicts;

merge_cleanup:
  sqlite3_free(aRemap);
  freeSchemaEntries(aAncSchema, nAncSchema);
  freeSchemaEntries(aOursSchema, nOursSchema);
  freeSchemaEntries(aTheirsSchema, nTheirsSchema);
  mergeFreeConflictTables(aConflictTables, nConflictTables);
  doltliteFreeCatalog(aAnc, nAnc);
  doltliteFreeCatalog(aOurs, nOurs);
  doltliteFreeCatalog(aTheirs, nTheirs);
  doltliteFreeCatalog(aMerged, nMerged);
  return rc;
}


#endif

void doltliteFreeNameList(char **az, int n){
  int i;
  for(i=0; i<n; i++) sqlite3_free(az[i]);
  sqlite3_free(az);
}

/* Rebuild the named (merge-adopted) indexes over the merged rows. REINDEX
** enforces uniqueness while rebuilding, but rows that collide under an
** adopted unique index are exactly what the merge must SURFACE — as
** resolvable constraint violations from the detector that runs after this
** — not a bare "constraint failed" that aborts the merge with nothing
** recorded. A unique index holds duplicates physically (entries carry the
** row key), so the rebuild runs with enforcement off and the detector
** owns the outcome, as Dolt does. Enforcement returns for ordinary DML
** once onError is restored. */
int doltliteReindexNamedIndexes(sqlite3 *db, char **az, int n){
  int i, rc = SQLITE_OK;
  if( n>0 ){
    /* The adopted indexes exist only in the just-switched catalog; the
    ** schema reloads on this prepare, so the lookups below can see them. */
    rc = sqlite3_exec(db, "SELECT 1 FROM sqlite_master LIMIT 1", 0, 0, 0);
    if( rc!=SQLITE_OK ) return rc;
  }
  for(i=0; i<n && rc==SQLITE_OK; i++){
    Index *pIdx = sqlite3FindIndex(db, az[i], "main");
    u8 savedOnError = pIdx ? pIdx->onError : 0;
    int suppressed = pIdx!=0;
    char *zSql = sqlite3_mprintf("REINDEX \"%w\"", az[i]);
    if( !zSql ) return SQLITE_NOMEM;
    if( pIdx ) pIdx->onError = OE_None;
    rc = sqlite3_exec(db, zSql, 0, 0, 0);
    sqlite3_free(zSql);
    if( suppressed ){
      /* Re-find rather than trust the pointer: a failed exec can reset
      ** the schema and free the Index. */
      pIdx = sqlite3FindIndex(db, az[i], "main");
      if( pIdx ) pIdx->onError = savedOnError;
    }
  }
  return rc;
}
