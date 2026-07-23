#ifdef DOLTLITE_PROLLY

#include "doltlite_merge_int.h"

/* Catalog three-way merge: table roots, schema objects, conflicts, reindex. */

/* Rewrite every record in theirs' table root from theirs' column order into
** the merged column order (ours' columns, then theirs' added columns), keyed
** by column name. A merged column absent from a their-record is emitted as
** NULL; trailing NULLs are dropped so an unchanged row re-encodes identically
** to the ancestor's. This lets the positional cell-level three-way merge run
** across a dual ADD COLUMN divergence: without it, theirs' added-column value
** would be read at the wrong ordinal. Records are content-addressed and rebuilt
** through the canonical doltliteBuildRecord, so the tree stays deterministic. */
static int normalizeTheirsToMergedLayout(
  sqlite3 *db,
  const ProllyHash *pTheirsRoot,
  u8 flags,
  const char *zOursSql,
  const char *zTheirsSql,
  ProllyHash *pOutRoot
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *cache = doltliteGetCache(db);
  ParsedColumn *aOurs = 0, *aTheirs = 0;
  int nOurs = 0, nTheirs = 0;
  int *aMap = 0;
  int nMerged;
  ProllyMutMap mm;
  int mmInit = 0;
  ProllyCursor cur;
  int curInit = 0;
  int isIntKey = (flags & PROLLY_NODE_INTKEY) ? 1 : 0;
  int rc, res, j;

  memset(pOutRoot, 0, sizeof(*pOutRoot));
  rc = parseColumns(zOursSql, &aOurs, &nOurs);
  if( rc!=SQLITE_OK ) return rc;
  rc = parseColumns(zTheirsSql, &aTheirs, &nTheirs);
  if( rc!=SQLITE_OK ){ freeColumns(aOurs, nOurs); return rc; }

  nMerged = nOurs;
  aMap = sqlite3_malloc((nTheirs>0 ? nTheirs : 1) * (int)sizeof(int));
  if( !aMap ){ rc = SQLITE_NOMEM; goto done; }
  for(j=0; j<nTheirs; j++){
    int oi, found = -1;
    for(oi=0; oi<nOurs; oi++){
      if( aOurs[oi].zName && aTheirs[j].zName
       && strcmp(aOurs[oi].zName, aTheirs[j].zName)==0 ){ found = oi; break; }
    }
    aMap[j] = (found>=0) ? found : nMerged++;
  }
  if( nMerged > DOLTLITE_MAX_RECORD_FIELDS ){ rc = SQLITE_ERROR; goto done; }

  rc = prollyMutMapInit(&mm, (u8)isIntKey);
  if( rc!=SQLITE_OK ) goto done;
  mmInit = 1;

  prollyCursorInit(&cur, cs, cache, pTheirsRoot, flags);
  curInit = 1;
  rc = prollyCursorFirst(&cur, &res);
  if( rc!=SQLITE_OK ) goto done;

  while( prollyCursorIsValid(&cur) ){
    const u8 *pVal = 0; int nVal = 0;
    const u8 *pKey = 0; int nKey = 0; i64 intKey = 0;
    DoltliteRecordInfo info;
    DoltliteSerialValue aMem[DOLTLITE_MAX_RECORD_FIELDS];
    int nEmit = 0, k;
    u8 *pNew = 0; int nNew = 0;

    prollyCursorValue(&cur, &pVal, &nVal);
    if( isIntKey ){
      intKey = prollyCursorIntKey(&cur);
    }else{
      prollyCursorKey(&cur, &pKey, &nKey);
    }

    doltliteParseRecord(pVal, nVal, &info);
    for(k=0; k<nMerged; k++){
      memset(&aMem[k], 0, sizeof(aMem[k]));
      aMem[k].eType = SQLITE_NULL;
    }
    for(j=0; j<info.nField && j<nTheirs; j++){
      int tgt = aMap[j];
      int st = info.aType[j];
      const u8 *body = pVal + info.aOffset[j];
      DoltliteSerialValue *m = &aMem[tgt];
      if( st==0 ){
        m->eType = SQLITE_NULL;
      }else if( dlSerialIsInt(st) ){
        m->eType = SQLITE_INTEGER;
        if( st==8 ) m->i = 0;
        else if( st==9 ) m->i = 1;
        else m->i = dlReadIntBytes(body, dlSerialTypeLen((u64)st));
      }else if( st==7 ){
        u64 bits = (u64)dlReadIntBytes(body, 8);
        double d;
        memcpy(&d, &bits, sizeof(d));
        m->eType = SQLITE_FLOAT;
        m->r = d;
      }else if( st>=12 ){
        m->eType = (st & 1) ? SQLITE_TEXT : SQLITE_BLOB;
        m->p = body;
        m->n = dlSerialTypeLen((u64)st);
      }
      if( m->eType!=SQLITE_NULL && tgt+1>nEmit ) nEmit = tgt+1;
    }

    if( nEmit>0 ){
      pNew = doltliteBuildRecord(aMem, nEmit, &nNew);
      if( !pNew ){ rc = SQLITE_NOMEM; goto done; }
    }
    rc = prollyMutMapInsert(&mm, pKey, nKey, intKey, pNew, nNew);
    sqlite3_free(pNew);
    if( rc!=SQLITE_OK ) goto done;

    rc = prollyCursorNext(&cur);
    if( rc!=SQLITE_OK ) goto done;
  }

  {
    ProllyMutator mut;
    memset(&mut, 0, sizeof(mut));
    mut.pStore = cs;
    mut.pCache = cache;
    memset(&mut.oldRoot, 0, sizeof(mut.oldRoot));
    mut.pEdits = &mm;
    mut.flags = flags;
    rc = prollyMutateFlush(&mut);
    if( rc==SQLITE_OK ) memcpy(pOutRoot, &mut.newRoot, sizeof(ProllyHash));
  }

done:
  if( curInit ) prollyCursorClose(&cur);
  if( mmInit ) prollyMutMapFree(&mm);
  sqlite3_free(aMap);
  freeColumns(aOurs, nOurs);
  freeColumns(aTheirs, nTheirs);
  return rc;
}

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

typedef DoltliteConflictTable MergeConflictTable;

static void freeConflictRows(DoltliteConflictRow *aRows, int nRows){
  int i;
  for(i=0; i<nRows; i++){
    doltliteConflictRowFree(&aRows[i]);
  }
  sqlite3_free(aRows);
}

static void freeAddedColumns(char **azCols, int nCols){
  int i;
  for(i=0; i<nCols; i++) sqlite3_free(azCols[i]);
  sqlite3_free(azCols);
}

static int appendConflictTable(
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

static int appendSchemaConflict(
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

static int hasSchemaConflictObject(
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

static int hasSchemaConflictTable(
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

static int hasAnySchemaConflict(
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
      if( !oursChanged || !theirsChanged
       || mergeSchemaEntriesSame(pOurs, pTheirs) ){
        continue;
      }
      /* Dolt keeps the modified index definition when the other branch
      ** drops that index. Only competing surviving definitions conflict. */
      if( pAnc && (!pOurs || !pTheirs) ) continue;
      zTable = pOurs && pOurs->zTblName ? pOurs->zTblName
             : (pTheirs && pTheirs->zTblName ? pTheirs->zTblName
                                             : a[i].zName);
      rc = appendSchemaConflict(ppConflictTables, pnConflictTables,
                                zTable, a[i].zName, &addedSchemaTable);
      if( rc!=SQLITE_OK ) return rc;
      if( addedSchemaTable ) (*pTotalConflicts)++;
    }
  }
  return SQLITE_OK;
}

static int recordSchemaAddColumns(
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

static int schemaEntryChangedByName(
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

static int replayDropsDisjointSchemaObject(
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

static SchemaEntry *findSchemaEntryByRootpage(
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

static struct TableEntry *findCatalogEntryBySchemaObject(
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

typedef struct SchemaRootpageRemap SchemaRootpageRemap;
struct SchemaRootpageRemap {
  Pgno oldPg;
  Pgno newPg;
};

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
  if( strcmp(pSe->zType, "table")==0 || strcmp(pSe->zType, "index")==0 ){
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
    if( !schemaEntryChangedByName(aAncSchema, nAncSchema,
                                  aTheirsSchema, nTheirsSchema,
                                  pSe->zName) ){
      continue;
    }
    if( schemaEntryChangedByName(aAncSchema, nAncSchema,
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

static int tryResolveSchemaDivergence(
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
      &azAddCols, &nAddCols, &schemaChoice, &zSchemaErr);
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
                                  0, 0);
      if( rc!=SQLITE_OK ) return rc;
    }
    *pSchemaChoice = schemaChoice;
    freeAddedColumns(azAddCols, nAddCols);
    return SQLITE_OK;
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

static int mergeAppendReindexName(char ***paz, int *pn, const char *zName){
  char **azNew;
  char *zDup;
  if( !paz ) return SQLITE_OK;
  azNew = sqlite3_realloc(*paz, (*pn+1)*(int)sizeof(char*));
  if( !azNew ) return SQLITE_NOMEM;
  *paz = azNew;
  zDup = sqlite3_mprintf("%s", zName);
  if( !zDup ) return SQLITE_NOMEM;
  (*paz)[(*pn)++] = zDup;
  return SQLITE_OK;
}

/* Inline row-merge index maintenance encodes sort keys with a BINARY/ASC
** KeyInfo. Indexes whose key columns fold case/space (NOCASE, RTRIM) or sort
** DESC need the real collation and order, so rebuild them canonically over the
** merged table via the post-merge REINDEX instead. */
static int mergeIndexNeedsRebuild(Index *pIdx){
  int k;
  for(k=0; k<pIdx->nKeyCol; k++){
    const char *zColl = pIdx->azColl ? pIdx->azColl[k] : 0;
    if( zColl
     && (sqlite3StrICmp(zColl,"NOCASE")==0 || sqlite3StrICmp(zColl,"RTRIM")==0) ){
      return 1;
    }
    if( pIdx->aSortOrder && (pIdx->aSortOrder[k] & KEYINFO_ORDER_DESC) ){
      return 1;
    }
  }
  return 0;
}


typedef struct IndexMergePatch IndexMergePatch;
struct IndexMergePatch {
  Pgno iTable;
  ProllyHash mergedRoot;
};

/* Shared state for pass-1 catalog merge. Keeps the long parameter list off
** the leaf helpers and makes the patch list a single ownership root. */
typedef struct MergePass1Ctx MergePass1Ctx;
struct MergePass1Ctx {
  sqlite3 *db;
  struct TableEntry *aAnc; int nAnc;
  struct TableEntry *aOurs; int nOurs;
  struct TableEntry *aTheirs; int nTheirs;
  SchemaEntry *aAncSchema; int nAncSchema;
  SchemaEntry *aOursSchema; int nOursSchema;
  SchemaEntry *aTheirsSchema; int nTheirsSchema;
  struct TableEntry *aMerged; int *pnMerged;
  MergeConflictTable **ppConflictTables; int *pnConflictTables;
  int *pTotalConflicts;
  char **pzErrMsg;
  const ProllyHash *pCatAnc;
  const ProllyHash *pCatOurs;
  const ProllyHash *pCatTheirs;
  SchemaMergeAction **ppSchemaActions; int *pnSchemaActions;
  int bDisjointSchemaChanges;
  int bPreferOurMaster;
  char ***pazReindex; int *pnReindex;
  IndexMergePatch *aPatches;
  int nPatches;
  int nPatchesAlloc;
};

static void mergePass1Free(MergePass1Ctx *c){
  sqlite3_free(c->aPatches);
  c->aPatches = 0;
  c->nPatches = c->nPatchesAlloc = 0;
}

static int mergePass1NoteSchemaConflict(
  MergePass1Ctx *c,
  const char *zTable,
  const char *zObject
){
  int added = 0;
  int rc = appendSchemaConflict(
      c->ppConflictTables, c->pnConflictTables,
      zTable ? zTable : "", zObject ? zObject : "", &added);
  if( rc!=SQLITE_OK ) return rc;
  if( added ) (*c->pTotalConflicts)++;
  return SQLITE_OK;
}

/* Build secondary-index merge descriptors for a named table. Indexes that
** need real collation/order go on the reindex list instead. */
static int mergePass1CollectIndexes(
  MergePass1Ctx *c,
  const char *zName,
  MergeIndexInfo **paIdxInfo,
  int *pnIdxInfo
){
  Table *pTab;
  Index *pIdx;
  int nIdx = 0;
  MergeIndexInfo *aIdxInfo = 0;
  int nIdxInfo = 0;
  int rc = SQLITE_OK;

  *paIdxInfo = 0;
  *pnIdxInfo = 0;
  if( !zName || !c->db ) return SQLITE_OK;

  pTab = sqlite3FindTable(c->db, zName, "main");
  if( !pTab ) return SQLITE_OK;

  for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext) nIdx++;
  if( nIdx<=0 ) return SQLITE_OK;

  aIdxInfo = sqlite3_malloc(nIdx * (int)sizeof(MergeIndexInfo));
  if( !aIdxInfo ){
    /* Proceeding would merge table rows but leave secondary indexes stale. */
    return SQLITE_NOMEM;
  }
  memset(aIdxInfo, 0, nIdx*(int)sizeof(MergeIndexInfo));

  for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
    struct TableEntry *oursIdx;
    if( pIdx->idxType==SQLITE_IDXTYPE_PRIMARYKEY ) continue;
    if( mergeIndexNeedsRebuild(pIdx) ){
      rc = mergeAppendReindexName(c->pazReindex, c->pnReindex, pIdx->zName);
      if( rc!=SQLITE_OK ){
        sqlite3_free(aIdxInfo);
        return rc;
      }
      continue;
    }
    oursIdx = doltliteFindTableByNumber(c->aOurs, c->nOurs, pIdx->tnum);
    if( oursIdx ){
      MergeIndexInfo *mi = &aIdxInfo[nIdxInfo];
      mi->iTable = pIdx->tnum;
      memcpy(&mi->oursRoot, &oursIdx->root, sizeof(ProllyHash));
      mi->nColumn = pIdx->nKeyCol;
      mi->aiColumn = pIdx->aiColumn;
      mi->pKeyInfo = 0;
      mi->iPKey = pTab->iPKey;
      nIdxInfo++;
    }
  }

  *paIdxInfo = aIdxInfo;
  *pnIdxInfo = nIdxInfo;
  return SQLITE_OK;
}

static int mergePass1RecordIndexPatches(
  MergePass1Ctx *c,
  MergeIndexInfo *aIdxInfo,
  int nIdxInfo
){
  int ix;
  for(ix=0; ix<nIdxInfo; ix++){
    if( c->nPatches >= c->nPatchesAlloc ){
      int newAlloc = c->nPatchesAlloc ? c->nPatchesAlloc*2 : 4;
      void *pNew = sqlite3_realloc(c->aPatches,
          newAlloc * (int)sizeof(IndexMergePatch));
      if( !pNew ) return SQLITE_NOMEM;
      c->aPatches = pNew;
      c->nPatchesAlloc = newAlloc;
    }
    c->aPatches[c->nPatches].iTable = aIdxInfo[ix].iTable;
    memcpy(&c->aPatches[c->nPatches].mergedRoot,
           &aIdxInfo[ix].mergedRoot, sizeof(ProllyHash));
    c->nPatches++;
  }
  return SQLITE_OK;
}

/* Three-way merge one table's data (and inline secondary indexes). */
static int mergePass1MergeTableData(
  MergePass1Ctx *c,
  const char *zName,
  const char *zLogicalName,
  struct TableEntry *pOurs,
  struct TableEntry *pAnc,
  const ProllyHash *pTheirsRoot,
  int ourSchemaChanged,
  int theirSchemaChanged,
  int schemaChoice,
  struct TableEntry *pTheirsEntry
){
  ProllyHash mergedTableRoot;
  int nConflicts = 0;
  DoltliteConflictRow *aConflictRows = 0;
  MergeIndexInfo *aIdxInfo = 0;
  int nIdxInfo = 0;
  int rc;
  int handled = 0;

  rc = mergePass1CollectIndexes(c, zName, &aIdxInfo, &nIdxInfo);
  if( rc!=SQLITE_OK ) return rc;

  if( canFastMerge(c->db, zName, !ourSchemaChanged && !theirSchemaChanged) ){
    rc = prollyThreeWayMergeFast(
      doltliteGetChunkStore(c->db), doltliteGetCache(c->db),
      &pAnc->root, &pOurs->root, pTheirsRoot,
      pOurs->flags, &mergedTableRoot, &handled);
    if( rc!=SQLITE_OK ){
      sqlite3_free(aIdxInfo);
      return rc;
    }
  }

  if( !handled ){
    rc = mergeTableRows(c->db, &pAnc->root, &pOurs->root,
                        pTheirsRoot, pOurs->flags,
                        &mergedTableRoot, &nConflicts, &aConflictRows,
                        aIdxInfo, nIdxInfo);
    if( rc!=SQLITE_OK ){
      sqlite3_free(aIdxInfo);
      return rc;
    }
  }

  rc = mergePass1RecordIndexPatches(c, aIdxInfo, nIdxInfo);
  sqlite3_free(aIdxInfo);
  if( rc!=SQLITE_OK ) return rc;

  {
    struct TableEntry merged = *pOurs;
    memcpy(&merged.root, &mergedTableRoot, sizeof(ProllyHash));
    if( schemaChoice==SCHEMA_MERGE_THEIRS
     || (theirSchemaChanged && !ourSchemaChanged) ){
      memcpy(&merged.schemaHash, &pTheirsEntry->schemaHash, sizeof(ProllyHash));
      merged.flags = pTheirsEntry->flags;
    }
    c->aMerged[(*c->pnMerged)++] = merged;
  }

  if( nConflicts>0 ){
    *c->pTotalConflicts += nConflicts;
    rc = appendConflictTable(c->ppConflictTables, c->pnConflictTables,
                             zLogicalName ? zLogicalName : "",
                             nConflicts, aConflictRows);
    if( rc!=SQLITE_OK ) return rc;
  }
  return SQLITE_OK;
}

/* Resolve catalog lookups for one ours entry (named table or unnamed index). */
static int mergePass1ResolveOursEntry(
  MergePass1Ctx *c,
  int iOurs,
  const char **pzName,
  const char **pzLogicalName,
  const char **pzSchemaMergeName,
  const char **pzSchemaConflictTable,
  struct TableEntry **ppAnc,
  struct TableEntry **ppTheirs,
  int *pbSkipMaster
){
  const char *zName = c->aOurs[iOurs].zName;

  *pzName = zName;
  *pzLogicalName = zName;
  *pzSchemaMergeName = zName;
  *pzSchemaConflictTable = zName;
  *ppAnc = 0;
  *ppTheirs = 0;
  *pbSkipMaster = 0;

  if( c->aOurs[iOurs].iTable==1 ){
    *pbSkipMaster = 1;
    return SQLITE_OK;
  }

  if( !zName ){
    SchemaEntry *pOurSe = findSchemaEntryByRootpage(
        c->aOursSchema, c->nOursSchema, c->aOurs[iOurs].iTable);
    if( pOurSe && pOurSe->zName && pOurSe->zType
     && strcmp(pOurSe->zType, "index")==0 ){
      *pzLogicalName = pOurSe->zName;
      *pzSchemaMergeName = pOurSe->zName;
      *pzSchemaConflictTable = pOurSe->zTblName
          ? pOurSe->zTblName : pOurSe->zName;
      *ppAnc = findCatalogEntryBySchemaObject(
          c->aAnc, c->nAnc, c->aAncSchema, c->nAncSchema,
          pOurSe->zType, pOurSe->zName, pOurSe->zTblName);
      *ppTheirs = findCatalogEntryBySchemaObject(
          c->aTheirs, c->nTheirs, c->aTheirsSchema, c->nTheirsSchema,
          pOurSe->zType, pOurSe->zName, pOurSe->zTblName);
      return SQLITE_OK;
    }
    *ppAnc = doltliteFindTableByNumber(c->aAnc, c->nAnc, c->aOurs[iOurs].iTable);
    *ppTheirs = doltliteFindTableByNumber(
        c->aTheirs, c->nTheirs, c->aOurs[iOurs].iTable);
    return SQLITE_OK;
  }

  *ppAnc = doltliteFindTableByName(c->aAnc, c->nAnc, zName);
  *ppTheirs = doltliteFindTableByName(c->aTheirs, c->nTheirs, zName);
  return SQLITE_OK;
}

/* Ours added the object (absent from ancestor). */
static int mergePass1OursAdded(
  MergePass1Ctx *c,
  int iOurs,
  const char *zName,
  const char *zSchemaMergeName,
  const char *zSchemaConflictTable,
  struct TableEntry *theirsEntry
){
  int rc;
  if( theirsEntry ){
    if( !zName && c->bDisjointSchemaChanges ){
      c->aMerged[(*c->pnMerged)++] = c->aOurs[iOurs];
      return SQLITE_OK;
    }
    if( prollyHashCompare(&c->aOurs[iOurs].root, &theirsEntry->root)!=0
     || prollyHashCompare(&c->aOurs[iOurs].schemaHash,
                          &theirsEntry->schemaHash)!=0 ){
      if( zName
       && (strcmp(zName, "sqlite_stat1")==0
        || strcmp(zName, "sqlite_stat4")==0) ){
        c->aMerged[(*c->pnMerged)++] = c->aOurs[iOurs];
        return SQLITE_OK;
      }
      rc = mergePass1NoteSchemaConflict(c, zSchemaConflictTable, zSchemaMergeName);
      if( rc!=SQLITE_OK ) return rc;
    }
  }
  c->aMerged[(*c->pnMerged)++] = c->aOurs[iOurs];
  return SQLITE_OK;
}

/* Ancestor had it; theirs dropped it. */
static int mergePass1OursModifyTheirsDelete(
  MergePass1Ctx *c,
  int iOurs,
  const char *zName,
  const char *zSchemaMergeName,
  const char *zSchemaConflictTable,
  struct TableEntry *ancEntry
){
  int oursChanged = prollyHashCompare(&c->aOurs[iOurs].root, &ancEntry->root)!=0
                 || prollyHashCompare(&c->aOurs[iOurs].schemaHash,
                                      &ancEntry->schemaHash)!=0;
  int rc;

  if( !oursChanged ) return SQLITE_OK;

  /* An index root moves with table data; only a definition change is a
  ** real modify-versus-delete conflict. Unchanged definition: DROP wins. */
  if( !zName && zSchemaMergeName && zSchemaMergeName[0]
   && !schemaEntryChangedByName(c->aAncSchema, c->nAncSchema,
                                c->aOursSchema, c->nOursSchema,
                                zSchemaMergeName) ){
    return SQLITE_OK;
  }
  if( !zName && zSchemaMergeName && zSchemaMergeName[0] ){
    if( c->ppSchemaActions && c->pnSchemaActions ){
      rc = recordSchemaAddColumns(c->ppSchemaActions, c->pnSchemaActions,
                                  zSchemaMergeName, 0, 0);
      if( rc!=SQLITE_OK ) return rc;
    }
    c->aMerged[(*c->pnMerged)++] = c->aOurs[iOurs];
    return SQLITE_OK;
  }
  rc = mergePass1NoteSchemaConflict(c, zSchemaConflictTable, zSchemaMergeName);
  if( rc!=SQLITE_OK ) return rc;
  c->aMerged[(*c->pnMerged)++] = c->aOurs[iOurs];
  return SQLITE_OK;
}

/* Both sides still have the object. */
static int mergePass1BothSides(
  MergePass1Ctx *c,
  int iOurs,
  const char *zName,
  const char *zLogicalName,
  const char *zSchemaMergeName,
  const char *zSchemaConflictTable,
  struct TableEntry *ancEntry,
  struct TableEntry *theirsEntry
){
  int oursChanged = prollyHashCompare(&c->aOurs[iOurs].root, &ancEntry->root)!=0
                 || prollyHashCompare(&c->aOurs[iOurs].schemaHash,
                                      &ancEntry->schemaHash)!=0;
  int theirsChanged = prollyHashCompare(&theirsEntry->root, &ancEntry->root)!=0;
  int ourSchemaChanged = prollyHashCompare(
      &c->aOurs[iOurs].schemaHash, &ancEntry->schemaHash)!=0;
  int theirSchemaChanged = prollyHashCompare(
      &theirsEntry->schemaHash, &ancEntry->schemaHash)!=0;
  int bNamedSchemaObject = (!zName && zSchemaMergeName && zSchemaMergeName[0]);
  int skipRowMerge = 0;
  int schemaChoice = SCHEMA_MERGE_DEFAULT;
  int bDualAddColMerge = 0;
  int bSchemaConflict = 0;
  ProllyHash theirsNormRoot;
  const ProllyHash *pMergeTheirsRoot = &theirsEntry->root;
  int rc = SQLITE_OK;

  if( zSchemaMergeName && zSchemaMergeName[0] ){
    ourSchemaChanged = ourSchemaChanged || schemaEntryChangedByName(
        c->aAncSchema, c->nAncSchema, c->aOursSchema, c->nOursSchema,
        zSchemaMergeName);
    theirSchemaChanged = theirSchemaChanged || schemaEntryChangedByName(
        c->aAncSchema, c->nAncSchema, c->aTheirsSchema, c->nTheirsSchema,
        zSchemaMergeName);
  }

  if( ourSchemaChanged && theirSchemaChanged
   && (bNamedSchemaObject
       || prollyHashCompare(&c->aOurs[iOurs].schemaHash,
                            &theirsEntry->schemaHash)!=0) ){
    rc = tryResolveSchemaDivergence(
      c->db, zSchemaMergeName, c->pCatAnc, c->pCatOurs, c->pCatTheirs,
      c->ppSchemaActions, c->pnSchemaActions, &skipRowMerge,
      &schemaChoice, c->pzErrMsg);
    if( rc==SQLITE_ERROR ){
      sqlite3_free(c->pzErrMsg ? *c->pzErrMsg : 0);
      if( c->pzErrMsg ) *c->pzErrMsg = 0;
      rc = mergePass1NoteSchemaConflict(c, zSchemaConflictTable, zSchemaMergeName);
      if( rc!=SQLITE_OK ) return rc;
      c->aMerged[(*c->pnMerged)++] = c->aOurs[iOurs];
      skipRowMerge = 1;
      bSchemaConflict = 1;
    }else if( rc!=SQLITE_OK ){
      return rc;
    }
    if( !bSchemaConflict && schemaChoice==SCHEMA_MERGE_THEIRS ){
      SchemaEntry *pOurSe = findSchemaEntry(
          c->aOursSchema, c->nOursSchema, zSchemaMergeName);
      SchemaEntry *pTheirSe = findSchemaEntry(
          c->aTheirsSchema, c->nTheirsSchema, zSchemaMergeName);
      char *zSql = pTheirSe && pTheirSe->zSql
                 ? sqlite3_mprintf("%s", pTheirSe->zSql) : 0;
      if( !pOurSe || !zSql ){
        sqlite3_free(zSql);
        return pOurSe ? SQLITE_NOMEM : SQLITE_CORRUPT;
      }
      sqlite3_free(pOurSe->zSql);
      pOurSe->zSql = zSql;
    }
    if( !bSchemaConflict && skipRowMerge && zName ){
      SchemaEntry *ourSE = findSchemaEntry(c->aOursSchema, c->nOursSchema, zName);
      SchemaEntry *theirSE = findSchemaEntry(
          c->aTheirsSchema, c->nTheirsSchema, zName);
      if( ourSE && ourSE->zSql && theirSE && theirSE->zSql ){
        rc = normalizeTheirsToMergedLayout(c->db, &theirsEntry->root,
                                           c->aOurs[iOurs].flags,
                                           ourSE->zSql, theirSE->zSql,
                                           &theirsNormRoot);
        if( rc!=SQLITE_OK ) return rc;
        pMergeTheirsRoot = &theirsNormRoot;
        bDualAddColMerge = 1;
        skipRowMerge = 0;
      }else{
        c->aMerged[(*c->pnMerged)++] = c->aOurs[iOurs];
      }
    }else if( !bSchemaConflict && skipRowMerge ){
      c->aMerged[(*c->pnMerged)++] = c->aOurs[iOurs];
    }
  }

  /* sqlite_sequence / sqlite_stat* are derived; refresh after merge. */
  if( !skipRowMerge && zName
   && strcmp(zName, "sqlite_sequence")==0
   && oursChanged && theirsChanged ){
    c->aMerged[(*c->pnMerged)++] = c->aOurs[iOurs];
    skipRowMerge = 1;
  }
  if( !skipRowMerge && zName
   && oursChanged && theirsChanged
   && (strcmp(zName, "sqlite_stat1")==0
    || strcmp(zName, "sqlite_stat4")==0) ){
    c->aMerged[(*c->pnMerged)++] = c->aOurs[iOurs];
    skipRowMerge = 1;
  }

  if( skipRowMerge ) return SQLITE_OK;

  if( bDualAddColMerge || (oursChanged && theirsChanged) ){
    return mergePass1MergeTableData(
        c, zName, zLogicalName, &c->aOurs[iOurs], ancEntry,
        pMergeTheirsRoot, ourSchemaChanged, theirSchemaChanged,
        schemaChoice, theirsEntry);
  }
  if( theirsChanged ){
    struct TableEntry merged = c->aOurs[iOurs];
    memcpy(&merged.root, &theirsEntry->root, sizeof(ProllyHash));
    memcpy(&merged.schemaHash, &theirsEntry->schemaHash, sizeof(ProllyHash));
    merged.flags = theirsEntry->flags;
    c->aMerged[(*c->pnMerged)++] = merged;
  }else{
    c->aMerged[(*c->pnMerged)++] = c->aOurs[iOurs];
  }
  return SQLITE_OK;
}

static int mergePass1MergeOursEntry(MergePass1Ctx *c, int iOurs){
  const char *zName = 0;
  const char *zLogicalName = 0;
  const char *zSchemaMergeName = 0;
  const char *zSchemaConflictTable = 0;
  struct TableEntry *ancEntry = 0;
  struct TableEntry *theirsEntry = 0;
  int bSkipMaster = 0;
  int rc;

  rc = mergePass1ResolveOursEntry(c, iOurs,
      &zName, &zLogicalName, &zSchemaMergeName, &zSchemaConflictTable,
      &ancEntry, &theirsEntry, &bSkipMaster);
  if( rc!=SQLITE_OK ) return rc;
  if( bSkipMaster ) return SQLITE_OK;

  if( hasSchemaConflictObject(*c->ppConflictTables, *c->pnConflictTables,
                              zSchemaMergeName)
   || (zName && hasSchemaConflictTable(*c->ppConflictTables,
                                       *c->pnConflictTables, zName)) ){
    c->aMerged[(*c->pnMerged)++] = c->aOurs[iOurs];
    return SQLITE_OK;
  }

  if( !ancEntry ){
    return mergePass1OursAdded(c, iOurs, zName, zSchemaMergeName,
                               zSchemaConflictTable, theirsEntry);
  }
  if( !theirsEntry ){
    return mergePass1OursModifyTheirsDelete(c, iOurs, zName,
        zSchemaMergeName, zSchemaConflictTable, ancEntry);
  }
  return mergePass1BothSides(c, iOurs, zName, zLogicalName,
      zSchemaMergeName, zSchemaConflictTable, ancEntry, theirsEntry);
}

/* Modify-versus-delete where the survivor is on theirs. */
static int mergePass1TheirsModifyDelete(MergePass1Ctx *c){
  int i, rc;

  for(i=0; i<c->nTheirs; i++){
    const char *zObject = c->aTheirs[i].zName;
    const char *zTable = zObject;
    struct TableEntry *pAncEntry = 0;
    struct TableEntry *pOurEntry = 0;
    int changed = 0;

    if( c->aTheirs[i].iTable<=1 ) continue;
    if( zObject ){
      pAncEntry = doltliteFindTableByName(c->aAnc, c->nAnc, zObject);
      pOurEntry = doltliteFindTableByName(c->aOurs, c->nOurs, zObject);
      if( pAncEntry ){
        changed = prollyHashCompare(&c->aTheirs[i].root, &pAncEntry->root)!=0
               || prollyHashCompare(&c->aTheirs[i].schemaHash,
                                    &pAncEntry->schemaHash)!=0;
      }
    }else{
      SchemaEntry *pTheirSe = findSchemaEntryByRootpage(
          c->aTheirsSchema, c->nTheirsSchema, c->aTheirs[i].iTable);
      if( !pTheirSe || !pTheirSe->zName || !pTheirSe->zType
       || strcmp(pTheirSe->zType, "index")!=0 ){
        continue;
      }
      zObject = pTheirSe->zName;
      zTable = pTheirSe->zTblName ? pTheirSe->zTblName : pTheirSe->zName;
      pAncEntry = findCatalogEntryBySchemaObject(
          c->aAnc, c->nAnc, c->aAncSchema, c->nAncSchema,
          pTheirSe->zType, pTheirSe->zName, pTheirSe->zTblName);
      pOurEntry = findCatalogEntryBySchemaObject(
          c->aOurs, c->nOurs, c->aOursSchema, c->nOursSchema,
          pTheirSe->zType, pTheirSe->zName, pTheirSe->zTblName);
      if( pAncEntry ){
        changed = schemaEntryChangedByName(c->aAncSchema, c->nAncSchema,
                                           c->aTheirsSchema, c->nTheirsSchema,
                                           pTheirSe->zName);
      }
    }
    if( !pAncEntry || pOurEntry || !changed ) continue;
    if( !c->aTheirs[i].zName ){
      if( c->ppSchemaActions && c->pnSchemaActions ){
        rc = recordSchemaAddColumns(c->ppSchemaActions, c->pnSchemaActions,
                                    zObject, 0, 0);
        if( rc!=SQLITE_OK ) return rc;
      }
      continue;
    }
    rc = mergePass1NoteSchemaConflict(c, zTable, zObject);
    if( rc!=SQLITE_OK ) return rc;
  }
  return SQLITE_OK;
}

/* Merge catalog root (iTable==1). Deferred so schema actions are known. */
static int mergePass1MergeMaster(MergePass1Ctx *c, int iTable1Idx){
  struct TableEntry *ancEntry;
  struct TableEntry *theirsEntry;
  int hasSchemaActions;
  int bPreferOurMasterHere;
  int rc = SQLITE_OK;

  if( iTable1Idx < 0 ) return SQLITE_OK;

  ancEntry = doltliteFindTableByNumber(c->aAnc, c->nAnc, 1);
  theirsEntry = doltliteFindTableByNumber(c->aTheirs, c->nTheirs, 1);
  hasSchemaActions = (c->ppSchemaActions && c->pnSchemaActions
                      && *c->pnSchemaActions > 0);
  bPreferOurMasterHere = hasAnySchemaConflict(
      *c->ppConflictTables, *c->pnConflictTables) || (c->bPreferOurMaster
      && replayDropsDisjointSchemaObject(c->aAncSchema, c->nAncSchema,
                                         c->aTheirsSchema, c->nTheirsSchema));

  if( !ancEntry ){
    if( theirsEntry ){
      if( prollyHashCompare(&c->aOurs[iTable1Idx].root, &theirsEntry->root)!=0
       || prollyHashCompare(&c->aOurs[iTable1Idx].schemaHash,
                            &theirsEntry->schemaHash)!=0 ){
        return SQLITE_ERROR;
      }
    }
    c->aMerged[(*c->pnMerged)++] = c->aOurs[iTable1Idx];
    return SQLITE_OK;
  }
  if( !theirsEntry ){
    int oursChanged = prollyHashCompare(
        &c->aOurs[iTable1Idx].root, &ancEntry->root)!=0;
    if( oursChanged ) return SQLITE_ERROR;
    return SQLITE_OK;
  }

  {
    int oursChanged = prollyHashCompare(
        &c->aOurs[iTable1Idx].root, &ancEntry->root)!=0;
    int theirsChanged = prollyHashCompare(
        &theirsEntry->root, &ancEntry->root)!=0;
    if( bPreferOurMasterHere ){
      c->aMerged[(*c->pnMerged)++] = c->aOurs[iTable1Idx];
      /* Original pass1 returned here without applying index patches. */
      return SQLITE_DONE;
    }
    if( oursChanged && theirsChanged && hasSchemaActions ){
      c->aMerged[(*c->pnMerged)++] = c->aOurs[iTable1Idx];
    }else if( oursChanged && theirsChanged && c->bDisjointSchemaChanges ){
      c->aMerged[(*c->pnMerged)++] = c->aOurs[iTable1Idx];
    }else if( oursChanged && theirsChanged ){
      ProllyHash mergedTableRoot;
      int nConflicts = 0;
      DoltliteConflictRow *aConflictRows = 0;
      int theirSchemaChanged2 = prollyHashCompare(
          &theirsEntry->schemaHash, &ancEntry->schemaHash)!=0;

      rc = mergeTableRows(c->db, &ancEntry->root, &c->aOurs[iTable1Idx].root,
                          &theirsEntry->root, c->aOurs[iTable1Idx].flags,
                          &mergedTableRoot, &nConflicts, &aConflictRows,
                          NULL, 0);
      if( rc!=SQLITE_OK ) return rc;

      {
        struct TableEntry merged = c->aOurs[iTable1Idx];
        memcpy(&merged.root, &mergedTableRoot, sizeof(ProllyHash));
        if( theirSchemaChanged2
         && prollyHashCompare(&c->aOurs[iTable1Idx].schemaHash,
                              &ancEntry->schemaHash)==0 ){
          memcpy(&merged.schemaHash, &theirsEntry->schemaHash,
                 sizeof(ProllyHash));
          merged.flags = theirsEntry->flags;
        }
        c->aMerged[(*c->pnMerged)++] = merged;
      }

      if( nConflicts>0 ){
        *c->pTotalConflicts += nConflicts;
        rc = appendConflictTable(c->ppConflictTables, c->pnConflictTables,
                                 "(sqlite_master)", nConflicts, aConflictRows);
        if( rc!=SQLITE_OK ) return rc;
      }
    }else if( theirsChanged ){
      struct TableEntry merged = c->aOurs[iTable1Idx];
      memcpy(&merged.root, &theirsEntry->root, sizeof(ProllyHash));
      memcpy(&merged.schemaHash, &theirsEntry->schemaHash, sizeof(ProllyHash));
      merged.flags = theirsEntry->flags;
      c->aMerged[(*c->pnMerged)++] = merged;
    }else{
      c->aMerged[(*c->pnMerged)++] = c->aOurs[iTable1Idx];
    }
  }
  return rc;
}

static void mergePass1ApplyIndexPatches(MergePass1Ctx *c){
  int ip, k;
  for(ip=0; ip<c->nPatches; ip++){
    for(k=0; k<*c->pnMerged; k++){
      if( c->aMerged[k].iTable==c->aPatches[ip].iTable ){
        memcpy(&c->aMerged[k].root, &c->aPatches[ip].mergedRoot,
               sizeof(ProllyHash));
        break;
      }
    }
  }
}

static int mergeCatalogPass1(
  sqlite3 *db,
  struct TableEntry *aAnc, int nAnc,
  struct TableEntry *aOurs, int nOurs,
  struct TableEntry *aTheirs, int nTheirs,
  SchemaEntry *aAncSchema, int nAncSchema,
  SchemaEntry *aOursSchema, int nOursSchema,
  SchemaEntry *aTheirsSchema, int nTheirsSchema,
  struct TableEntry *aMerged, int *pnMerged,
  MergeConflictTable **ppConflictTables, int *pnConflictTables,
  int *pTotalConflicts,
  char **pzErrMsg,
  const ProllyHash *pCatAnc,
  const ProllyHash *pCatOurs,
  const ProllyHash *pCatTheirs,
  SchemaMergeAction **ppSchemaActions, int *pnSchemaActions,
  int bDisjointSchemaChanges,
  int bPreferOurMaster,
  char ***pazReindex, int *pnReindex
){
  MergePass1Ctx c;
  int i, rc = SQLITE_OK;
  int iTable1Idx = -1;

  memset(&c, 0, sizeof(c));
  c.db = db;
  c.aAnc = aAnc; c.nAnc = nAnc;
  c.aOurs = aOurs; c.nOurs = nOurs;
  c.aTheirs = aTheirs; c.nTheirs = nTheirs;
  c.aAncSchema = aAncSchema; c.nAncSchema = nAncSchema;
  c.aOursSchema = aOursSchema; c.nOursSchema = nOursSchema;
  c.aTheirsSchema = aTheirsSchema; c.nTheirsSchema = nTheirsSchema;
  c.aMerged = aMerged; c.pnMerged = pnMerged;
  c.ppConflictTables = ppConflictTables;
  c.pnConflictTables = pnConflictTables;
  c.pTotalConflicts = pTotalConflicts;
  c.pzErrMsg = pzErrMsg;
  c.pCatAnc = pCatAnc; c.pCatOurs = pCatOurs; c.pCatTheirs = pCatTheirs;
  c.ppSchemaActions = ppSchemaActions; c.pnSchemaActions = pnSchemaActions;
  c.bDisjointSchemaChanges = bDisjointSchemaChanges;
  c.bPreferOurMaster = bPreferOurMaster;
  c.pazReindex = pazReindex; c.pnReindex = pnReindex;

  for(i=0; i<nOurs; i++){
    if( aOurs[i].iTable==1 ){
      iTable1Idx = i;
      continue;
    }
    rc = mergePass1MergeOursEntry(&c, i);
    if( rc!=SQLITE_OK ){
      mergePass1Free(&c);
      return rc;
    }
  }

  /* Conflicts whose surviving object is on theirs must be recorded before
  ** the master-catalog root is chosen below. */
  rc = mergePass1TheirsModifyDelete(&c);
  if( rc!=SQLITE_OK ){
    mergePass1Free(&c);
    return rc;
  }

  rc = mergePass1MergeMaster(&c, iTable1Idx);
  if( rc==SQLITE_DONE ){
    /* Prefer-our-master path: original returned before patch apply. */
    mergePass1Free(&c);
    return SQLITE_OK;
  }
  if( rc!=SQLITE_OK ){
    mergePass1Free(&c);
    return rc;
  }

  /* Prefer inline index roots; they already account for row conflicts. */
  mergePass1ApplyIndexPatches(&c);
  mergePass1Free(&c);
  return rc;
}

static int mergeCatalogPass2(
  struct TableEntry *aAnc, int nAnc,
  struct TableEntry *aOurs, int nOurs,
  struct TableEntry *aTheirs, int nTheirs,
  SchemaEntry *aAncSchema, int nAncSchema,
  SchemaEntry *aOursSchema, int nOursSchema,
  SchemaEntry *aTheirsSchema, int nTheirsSchema,
  struct TableEntry *aMerged, int *pnMerged,
  Pgno *piNextMerged,
  int bDisjointSchemaChanges,
  MergeConflictTable *aConflictTables,
  int nConflictTables,
  SchemaRootpageRemap **ppaRemap,
  int *pnRemap,
  char ***pazReindex,
  int *pnReindex
){
  int i, rc = SQLITE_OK;

  for(i=0; i<nTheirs; i++){
    const char *zName = aTheirs[i].zName;
    struct TableEntry *oursEntry;

    if( aTheirs[i].iTable<=1 ) continue;

    if( !zName ){
      SchemaEntry *pTheirSe = findSchemaEntryByRootpage(
          aTheirsSchema, nTheirsSchema, aTheirs[i].iTable);
      if( pTheirSe && pTheirSe->zName && pTheirSe->zType
       && strcmp(pTheirSe->zType, "index")==0 ){
        if( hasSchemaConflictObject(aConflictTables, nConflictTables,
                                    pTheirSe->zName) ){
          continue;
        }
        oursEntry = findCatalogEntryBySchemaObject(
            aOurs, nOurs, aOursSchema, nOursSchema,
            pTheirSe->zType, pTheirSe->zName, pTheirSe->zTblName);
        if( oursEntry ) continue;

        {
          struct TableEntry *ancEntry = findCatalogEntryBySchemaObject(
              aAnc, nAnc, aAncSchema, nAncSchema,
              pTheirSe->zType, pTheirSe->zName, pTheirSe->zTblName);
          if( !ancEntry ){
            struct TableEntry newEntry = aTheirs[i];
            int j, conflict = 0;
            Pgno oldPg = newEntry.iTable;
            int forceRemap = bDisjointSchemaChanges;
            for(j=0; j<*pnMerged; j++){
              if( aMerged[j].iTable==newEntry.iTable ){
                conflict = 1;
                break;
              }
            }
            if( conflict || forceRemap ){
              SchemaRootpageRemap *aNew;
              int nOld = *pnRemap;
              newEntry.iTable = (*piNextMerged)++;
              aNew = sqlite3_realloc(*ppaRemap,
                                     (nOld+1)*(int)sizeof(SchemaRootpageRemap));
              if( !aNew ) return SQLITE_NOMEM;
              *ppaRemap = aNew;
              aNew[nOld].oldPg = oldPg;
              aNew[nOld].newPg = newEntry.iTable;
              *pnRemap = nOld + 1;
            }
            if( newEntry.iTable >= *piNextMerged ) *piNextMerged = newEntry.iTable + 1;
            aMerged[(*pnMerged)++] = newEntry;
            /* The adopted tree covers only theirs' rows; ours' row changes
            ** never touched it. Record the index for a rebuild over the
            ** merged table once the merged catalog is live. */
            rc = mergeAppendReindexName(pazReindex, pnReindex, pTheirSe->zName);
            if( rc!=SQLITE_OK ) return rc;
          }else if( schemaEntryChangedByName(
                        aAncSchema, nAncSchema,
                        aTheirsSchema, nTheirsSchema,
                        pTheirSe->zName) ){
            struct TableEntry newEntry = aTheirs[i];
            int j, conflict = 0;
            Pgno oldPg = newEntry.iTable;
            for(j=0; j<*pnMerged; j++){
              if( aMerged[j].iTable==newEntry.iTable ){
                conflict = 1;
                break;
              }
            }
            if( conflict ){
              SchemaRootpageRemap *aNew;
              int nOld = *pnRemap;
              newEntry.iTable = (*piNextMerged)++;
              aNew = sqlite3_realloc(*ppaRemap,
                  (nOld+1)*(int)sizeof(SchemaRootpageRemap));
              if( !aNew ) return SQLITE_NOMEM;
              *ppaRemap = aNew;
              aNew[nOld].oldPg = oldPg;
              aNew[nOld].newPg = newEntry.iTable;
              *pnRemap = nOld + 1;
            }
            if( newEntry.iTable >= *piNextMerged ){
              *piNextMerged = newEntry.iTable + 1;
            }
            aMerged[(*pnMerged)++] = newEntry;
            rc = mergeAppendReindexName(pazReindex, pnReindex, pTheirSe->zName);
            if( rc!=SQLITE_OK ) return rc;
          }else{
            /* An unmodified index does not override our explicit DROP. */
          }
        }
        continue;
      }

      if( !doltliteFindTableByNumber(aOurs, nOurs, aTheirs[i].iTable) ){
        struct TableEntry newEntry = aTheirs[i];
        int j, conflict = 0;
        Pgno oldPg = newEntry.iTable;
        int forceRemap = bDisjointSchemaChanges;
        for(j=0; j<*pnMerged; j++){
          if( aMerged[j].iTable==newEntry.iTable ){
            conflict = 1;
            break;
          }
        }
        if( conflict || forceRemap ){
          SchemaRootpageRemap *aNew;
          int nOld = *pnRemap;
          Pgno newPg = *piNextMerged;
          aNew = sqlite3_realloc(*ppaRemap,
                                 (nOld+1)*(int)sizeof(SchemaRootpageRemap));
          if( !aNew ) return SQLITE_NOMEM;
          *ppaRemap = aNew;
          aNew[nOld].oldPg = oldPg;
          aNew[nOld].newPg = newPg;
          *pnRemap = nOld + 1;
          newEntry.iTable = newPg;
          (*piNextMerged)++;
        }
        if( newEntry.iTable >= *piNextMerged ) *piNextMerged = newEntry.iTable + 1;
        aMerged[(*pnMerged)++] = newEntry;
      }else if( bDisjointSchemaChanges ){
        struct TableEntry *oursIdx = doltliteFindTableByNumber(aOurs, nOurs, aTheirs[i].iTable);
        struct TableEntry *ancIdx = doltliteFindTableByNumber(aAnc, nAnc, aTheirs[i].iTable);
        if( oursIdx && !ancIdx
         && (prollyHashCompare(&oursIdx->root, &aTheirs[i].root)!=0
             || prollyHashCompare(&oursIdx->schemaHash, &aTheirs[i].schemaHash)!=0) ){
          struct TableEntry newEntry = aTheirs[i];
          SchemaRootpageRemap *aNew;
          int nOld = *pnRemap;
          Pgno newPg = *piNextMerged;
          aNew = sqlite3_realloc(*ppaRemap,
                                 (nOld+1)*(int)sizeof(SchemaRootpageRemap));
          if( !aNew ) return SQLITE_NOMEM;
          *ppaRemap = aNew;
          aNew[nOld].oldPg = aTheirs[i].iTable;
          aNew[nOld].newPg = newPg;
          *pnRemap = nOld + 1;
          newEntry.iTable = newPg;
          (*piNextMerged)++;
          aMerged[(*pnMerged)++] = newEntry;
        }
      }
      continue;
    }

    if( hasSchemaConflictObject(aConflictTables, nConflictTables, zName) ){
      continue;
    }
    oursEntry = doltliteFindTableByName(aOurs, nOurs, zName);
    if( oursEntry ) continue;

    {
      struct TableEntry *ancEntry = doltliteFindTableByName(aAnc, nAnc, zName);
      if( !ancEntry ){

        struct TableEntry newEntry = aTheirs[i];
        int forceRemap = bDisjointSchemaChanges;
        {
          int j, conflict = 0;
          for(j=0; j<*pnMerged; j++){
            if( aMerged[j].iTable==newEntry.iTable ){ conflict = 1; break; }
          }
          if( conflict || forceRemap ) newEntry.iTable = (*piNextMerged)++;
        }
        if( newEntry.iTable >= *piNextMerged ) *piNextMerged = newEntry.iTable + 1;

        newEntry.zName = sqlite3_mprintf("%s", zName);
        aMerged[(*pnMerged)++] = newEntry;
      }else{

        int theirsChanged = prollyHashCompare(&aTheirs[i].root, &ancEntry->root)!=0;
        if( theirsChanged ){
          return SQLITE_ERROR;
        }

      }
    }
  }
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
  return doltliteSetSessionMergeState(db, 1, 0, &conflictsHash);
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
  int *pnReindex
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

int doltliteReindexNamedIndexes(sqlite3 *db, char **az, int n){
  int i, rc = SQLITE_OK;
  for(i=0; i<n && rc==SQLITE_OK; i++){
    char *zSql = sqlite3_mprintf("REINDEX \"%w\"", az[i]);
    if( !zSql ) return SQLITE_NOMEM;
    rc = sqlite3_exec(db, zSql, 0, 0, 0);
    sqlite3_free(zSql);
  }
  return rc;
}
