#ifdef DOLTLITE_PROLLY

#include "doltlite_merge_int.h"

int mergeRowTable(
  MergePass1Ctx *c,
  const char *zName,
  int schemaChanged,
  int useTheirs,
  sqlite3 **ppSchemaDb,
  Table **ppTab
){
  SchemaEntry *pSchema;
  sqlite3 *tmp = 0;
  char *zErr = 0;
  int i, j, rc;

  *ppSchemaDb = 0;
  *ppTab = zName ? sqlite3FindTable(c->db, zName, "main") : 0;
  if( !zName || !schemaChanged ) return SQLITE_OK;
  pSchema = useTheirs
      ? findSchemaEntry(c->aTheirsSchema, c->nTheirsSchema, zName)
      : findSchemaEntry(c->aOursSchema, c->nOursSchema, zName);
  if( !pSchema || !pSchema->zSql ) return SQLITE_CORRUPT;
  rc = sqlite3_open(":memory:", &tmp);
  if( rc==SQLITE_OK ) rc = sqlite3_exec(tmp, pSchema->zSql, 0, 0, &zErr);
  for(i=0; rc==SQLITE_OK && c->pnSchemaActions && i<*c->pnSchemaActions; i++){
    SchemaMergeAction *pAction = &(*c->ppSchemaActions)[i];
    if( sqlite3_stricmp(pAction->zTableName, zName)!=0 ) continue;
    /* Relayout appends added fields; drops and renames run after row merge. */
    for(j=0; j<pAction->nAddColumns && rc==SQLITE_OK; j++){
      char *zSql = sqlite3_mprintf("ALTER TABLE \"%w\" ADD COLUMN %s",
                                    zName, pAction->azAddColumns[j]);
      rc = zSql ? sqlite3_exec(tmp, zSql, 0, 0, &zErr) : SQLITE_NOMEM;
      sqlite3_free(zSql);
    }
  }
  /* Row merge maintains our indexes over records in the merged layout, so
  ** their column numbers have to come from this table, not the live one.
  ** SQLite refuses to drop an indexed column, so the only index this cannot
  ** define is one our side added on a column their side dropped; it is left
  ** unbound here and pass 2 drops it from the merged catalog. */
  for(i=0; rc==SQLITE_OK && i<c->nOursSchema; i++){
    SchemaEntry *pSe = &c->aOursSchema[i];
    if( !pSe->zType || strcmp(pSe->zType, "index")!=0 ) continue;
    if( !pSe->zSql || !pSe->zTblName ) continue;
    if( sqlite3_stricmp(pSe->zTblName, zName)!=0 ) continue;
    sqlite3_exec(tmp, pSe->zSql, 0, 0, 0);
  }
  if( rc==SQLITE_OK ){
    Parse sParse;
    sqlite3_mutex_enter(tmp->mutex);
    sqlite3ParseObjectInit(&sParse, tmp);
    *ppTab = sqlite3LocateTable(&sParse, 0, zName, "main");
    rc = sParse.rc;
    if( !*ppTab && rc==SQLITE_OK ) rc = SQLITE_CORRUPT;
    sqlite3DbFree(tmp, sParse.zErrMsg);
    sqlite3ParseObjectReset(&sParse);
    sqlite3_mutex_leave(tmp->mutex);
  }
  if( rc!=SQLITE_OK ){
    if( zErr && c->pzErrMsg ){
      *c->pzErrMsg = sqlite3_mprintf(
          "cannot merge: schema change could not be applied: %s", zErr);
      if( !*c->pzErrMsg ) rc = SQLITE_NOMEM;
    }
    sqlite3_free(zErr);
    sqlite3_close(tmp);
    return rc;
  }
  *ppSchemaDb = tmp;
  return SQLITE_OK;
}

int mergeRowPolicy(
  MergePass1Ctx *c,
  const char *zName,
  Table *pTab,
  int useTheirs,
  MergeRowPolicy *pPolicy
){
  SchemaEntry *pAnc = findSchemaEntry(c->aAncSchema, c->nAncSchema, zName);
  SchemaEntry *pSelected = useTheirs
      ? findSchemaEntry(c->aTheirsSchema, c->nTheirsSchema, zName)
      : findSchemaEntry(c->aOursSchema, c->nOursSchema, zName);
  ParsedColumn *aAnc = 0, *aSelected = 0;
  int nAnc = 0, nSelected = 0;
  int i, j, k, rc;

  if( !pAnc || !pAnc->zSql || !pSelected || !pTab ) return SQLITE_OK;
  pPolicy->bSchemaIsTheirs = useTheirs;
  rc = parseColumns(pAnc->zSql, &aAnc, &nAnc);
  if( rc!=SQLITE_OK ) return rc;
  rc = parseColumns(pSelected->zSql, &aSelected, &nSelected);
  if( rc!=SQLITE_OK ){
    freeColumns(aAnc, nAnc);
    return rc;
  }
  pPolicy->aiDeleteCompareFields = sqlite3_malloc(pTab->nNVCol*sizeof(int));
  pPolicy->aiDropFields = sqlite3_malloc(pTab->nNVCol*sizeof(int));
  if( !pPolicy->aiDeleteCompareFields || !pPolicy->aiDropFields ){
    freeColumns(aAnc, nAnc);
    freeColumns(aSelected, nSelected);
    return SQLITE_NOMEM;
  }
  for(i=0; i<pTab->nCol; i++){
    const char *zCol = pTab->aCol[i].zCnName;
    int iAnc = parsedColumnIndexByName(aAnc, nAnc, zCol);
    int bDrop = 0;
    if( pTab->aCol[i].colFlags & COLFLAG_VIRTUAL ) continue;
    if( iAnc<0 && i<nAnc && i<nSelected
     && parsedColumnDefinitionsMatch(&aAnc[i], &aSelected[i]) ){
      for(j=0; j<pTab->nCol; j++){
        if( sqlite3_stricmp(pTab->aCol[j].zCnName, aAnc[i].zName)==0 ) break;
      }
      if( j==pTab->nCol ) iAnc = i;
    }
    if( iAnc<0 ) continue;
    for(j=0; c->pnSchemaActions && j<*c->pnSchemaActions; j++){
      SchemaMergeAction *pAction = &(*c->ppSchemaActions)[j];
      if( sqlite3_stricmp(pAction->zTableName, zName)!=0 ) continue;
      for(k=0; k<pAction->nDropColumns; k++){
        if( sqlite3_stricmp(pAction->azDropColumns[k], zCol)==0 ) bDrop = 1;
      }
    }
    if( bDrop ){
      pPolicy->aiDropFields[pPolicy->nDropFields++] =
          HasRowid(pTab) ? sqlite3TableColumnToStorage(pTab, i)
          : sqlite3TableColumnToIndex(sqlite3PrimaryKeyIndex(pTab), i);
    }
    pPolicy->aiDeleteCompareFields[pPolicy->nDeleteCompareFields++] =
        HasRowid(pTab) ? sqlite3TableColumnToStorage(pTab, i)
        : sqlite3TableColumnToIndex(sqlite3PrimaryKeyIndex(pTab), i);
  }
  freeColumns(aAnc, nAnc);
  freeColumns(aSelected, nSelected);
  return SQLITE_OK;
}

/* mergePass1CollectIndexes reads the live table, whose record layout is
** ours. Once mergeRowTable has produced the merged-layout table, every
** index has to be read through it or a column dropped on their side shifts
** every later index column onto the wrong field. */
int mergeRebindIndexes(
  MergePass1Ctx *c,
  MergeIndexInfo *aIdxInfo,
  int nIdxInfo,
  sqlite3 *pSchemaDb,
  Table *pTab
){
  int i;
  if( !pSchemaDb || !pTab ) return SQLITE_OK;
  for(i=0; i<nIdxInfo; i++){
    MergeIndexInfo *mi = &aIdxInfo[i];
    Index *pMerged;
    KeyInfo *pKeyInfo;
    int rc;
    if( !mi->pIdx || !mi->pIdx->zName ) continue;
    for(pMerged=pTab->pIndex; pMerged; pMerged=pMerged->pNext){
      if( pMerged->zName && sqlite3_stricmp(pMerged->zName, mi->pIdx->zName)==0 ){
        break;
      }
    }
    if( !pMerged ) continue;
    pKeyInfo = doltliteKeyInfoOfIndex(c->db, pMerged);
    if( !pKeyInfo ) return SQLITE_NOMEM;
    sqlite3KeyInfoUnref(mi->pKeyInfo);
    mi->pKeyInfo = pKeyInfo;
    doltlitePartialIndexClear(&mi->part);
    rc = doltlitePartialIndexLoad(pSchemaDb, pMerged, &mi->part);
    if( rc!=SQLITE_OK ) return rc;
    mi->nColumn = pMerged->nKeyCol;
    mi->aiColumn = pMerged->aiColumn;
    mi->iPKey = pTab->iPKey;
    mi->pIdx = pMerged;
  }
  return SQLITE_OK;
}

static int mergeGeneratedPrepare(
  sqlite3 *db, Table *pTab, sqlite3_stmt **ppStmt
){
#ifndef SQLITE_OMIT_GENERATED_COLUMNS
  Parse sParse;
  Vdbe *v;
  int regRecord = pTab->nCol+2;
  char *zAffinity;
  int i, rc;

  zAffinity = sqlite3DbMallocRaw(db, pTab->nNVCol+1);
  if( !zAffinity ) return SQLITE_NOMEM;
  sqlite3ParseObjectInit(&sParse, db);
  v = sqlite3GetVdbe(&sParse);
  if( !v ){
    sqlite3DbFree(db, zAffinity);
    sqlite3ParseObjectReset(&sParse);
    return SQLITE_NOMEM;
  }
  sParse.nVar = 2;
  sParse.nTab = 1;
  sParse.nMem = regRecord+pTab->nNVCol;
  sqlite3VdbeAddOp2(v, OP_Variable, 1, regRecord);
  sqlite3VdbeAddOp2(v, OP_Variable, 2, 1);
  sqlite3VdbeAddOp3(v, OP_OpenPseudo, 0, regRecord, pTab->nNVCol);
  for(i=0; i<pTab->nCol; i++){
    int iField;
    int reg = 2+sqlite3TableColumnToStorage(pTab, i);
    if( pTab->aCol[i].colFlags & COLFLAG_GENERATED ) continue;
    if( i==pTab->iPKey ){
      sqlite3VdbeAddOp2(v, OP_Copy, 1, reg);
      continue;
    }
    iField = HasRowid(pTab) ? sqlite3TableColumnToStorage(pTab, i)
        : sqlite3TableColumnToIndex(sqlite3PrimaryKeyIndex(pTab), i);
    sqlite3VdbeAddOp3(v, OP_Column, 0, iField, reg);
    sqlite3ColumnDefault(v, pTab, i, reg);
  }
  sqlite3ComputeGeneratedColumns(&sParse, 2, pTab);
  for(i=0; i<pTab->nNVCol; i++){
    int iCol = HasRowid(pTab) ? sqlite3StorageColumnToTable(pTab, i)
        : sqlite3PrimaryKeyIndex(pTab)->aiColumn[i];
    zAffinity[i] = pTab->aCol[iCol].affinity;
    if( iCol==pTab->iPKey ){
      sqlite3VdbeAddOp2(v, OP_Null, 0, regRecord+1+i);
    }else{
      sqlite3VdbeAddOp2(v, OP_Copy,
          2+sqlite3TableColumnToStorage(pTab, iCol), regRecord+1+i);
    }
  }
  zAffinity[pTab->nNVCol] = 0;
  sqlite3VdbeAddOp4(v, OP_MakeRecord, regRecord+1,
                    pTab->nNVCol, regRecord, zAffinity, P4_DYNAMIC);
  sqlite3VdbeAddOp2(v, OP_ResultRow, regRecord, 1);
  sqlite3VdbeSetNumCols(v, 1);
  sqlite3FinishCoding(&sParse);
  rc = db->mallocFailed ? SQLITE_NOMEM : sParse.rc;
  sqlite3DbFree(db, sParse.zErrMsg);
  sqlite3ParseObjectReset(&sParse);
  if( rc!=SQLITE_DONE ){
    sqlite3_finalize((sqlite3_stmt*)v);
    return rc;
  }
  *ppStmt = (sqlite3_stmt*)v;
  return SQLITE_OK;
#else
  UNUSED_PARAMETER(db);
  UNUSED_PARAMETER(pTab);
  UNUSED_PARAMETER(ppStmt);
  return SQLITE_ERROR;
#endif
}

/* Relayout can fill a newly added generated column with its default instead
** of its expression value. Recompute before updating rows or indexes. */
int mergeGeneratedSideRow(
  sqlite3 *db, Table *pTab, sqlite3_stmt **ppStmt, i64 intKey,
  const u8 **ppVal, int *pnVal, u8 **ppOwned
){
  int rc;
  *ppOwned = 0;
  if( !pTab || !*ppVal || *pnVal<=0 ) return SQLITE_OK;
  rc = doltliteDupBytes(*ppVal, *pnVal, ppOwned);
  if( rc!=SQLITE_OK ) return rc;
  rc = mergeGeneratedRecord(db, pTab, ppStmt, intKey, ppOwned, pnVal);
  if( rc!=SQLITE_OK ){
    sqlite3_free(*ppOwned);
    *ppOwned = 0;
    return rc;
  }
  *ppVal = *ppOwned;
  return SQLITE_OK;
}

int mergeGeneratedRecord(
  sqlite3 *db, Table *pTab, sqlite3_stmt **ppStmt, i64 intKey,
  u8 **ppRecord, int *pnRecord
){
  sqlite3_stmt *pStmt;
  u8 *pOut = 0;
  int nOut = 0;
  int rc;
  if( !pTab ) return SQLITE_OK;
  if( !*ppStmt ){
    rc = mergeGeneratedPrepare(db, pTab, ppStmt);
    if( rc!=SQLITE_OK ) return rc;
  }
  pStmt = *ppStmt;
  rc = sqlite3_bind_blob(pStmt, 1, *ppRecord, *pnRecord, SQLITE_TRANSIENT);
  if( rc==SQLITE_OK ) rc = sqlite3_bind_int64(pStmt, 2, intKey);
  if( rc==SQLITE_OK ) rc = sqlite3_step(pStmt);
  if( rc==SQLITE_ROW ){
    const u8 *pBlob = sqlite3_column_blob(pStmt, 0);
    nOut = sqlite3_column_bytes(pStmt, 0);
    rc = pBlob ? doltliteDupBytes(pBlob, nOut, &pOut) : SQLITE_NOMEM;
  }
  sqlite3_reset(pStmt);
  if( rc!=SQLITE_OK ) return rc;
  sqlite3_free(*ppRecord);
  *ppRecord = pOut;
  *pnRecord = nOut;
  return SQLITE_OK;
}

#endif
