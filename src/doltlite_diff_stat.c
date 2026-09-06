

#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_diff.h"
#include "prolly_cursor.h"
#include "prolly_cache.h"
#include "chunk_store.h"
#include "doltlite_commit.h"
#include "doltlite_record.h"
#include "doltlite_internal.h"
#include <stddef.h>
#include <string.h>

static int dsLoadColInfo(sqlite3 *db,
                         const ProllyHash *pCatHash,
                         const char *zTableName,
                         DoltliteColInfo *pOut){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  SchemaEntry entry;
  int found = 0;
  sqlite3 *tmp = 0;
  int rc;

  memset(pOut, 0, sizeof(*pOut));
  if( prollyHashIsEmpty(pCatHash) ) return SQLITE_OK;

  rc = loadSchemaEntryFromCatalog(db, cs, pCache, pCatHash, zTableName,
                                  &entry, &found);
  if( rc!=SQLITE_OK ) return rc;
  if( !found || !entry.zSql ){
    clearSchemaEntry(&entry);
    return SQLITE_OK;
  }

  rc = sqlite3_open(":memory:", &tmp);
  if( rc==SQLITE_OK ) rc = sqlite3_exec(tmp, entry.zSql, 0, 0, 0);
  if( rc==SQLITE_OK ) rc = doltliteGetColumnNames(tmp, zTableName, pOut);
  if( tmp ) sqlite3_close(tmp);
  clearSchemaEntry(&entry);
  if( rc!=SQLITE_OK ) doltliteFreeColInfo(pOut);
  return rc;
}

static int dsLoadCreateSql(
  sqlite3 *db,
  const ProllyHash *pCatHash,
  const char *zTableName,
  char **pzSqlOut
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  SchemaEntry entry;
  int found = 0;
  int rc;

  *pzSqlOut = 0;
  if( prollyHashIsEmpty(pCatHash) ) return SQLITE_OK;

  rc = loadSchemaEntryFromCatalog(db, cs, pCache, pCatHash, zTableName,
                                  &entry, &found);
  if( rc!=SQLITE_OK ) return rc;
  if( found && entry.zSql ){
    *pzSqlOut = sqlite3_mprintf("%s", entry.zSql);
    if( !*pzSqlOut ){
      clearSchemaEntry(&entry);
      return SQLITE_NOMEM;
    }
  }
  clearSchemaEntry(&entry);
  return SQLITE_OK;
}

static int dsCountRows(sqlite3 *db, const ProllyHash *pRoot, u8 flags,
                       i64 *pnRow){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  u64 n = 0;
  int rc;
  UNUSED_PARAMETER(flags);
  if( pnRow ) *pnRow = 0;
  if( !cs || !pCache ) return SQLITE_ERROR;
  if( prollyHashIsEmpty(pRoot) ) return SQLITE_OK;
  rc = prollySubtreeCount(cs, pCache, pRoot, &n);
  if( rc!=SQLITE_OK ) return rc;
  if( pnRow ) *pnRow = (i64)n;
  return SQLITE_OK;
}

typedef struct DsColMap DsColMap;
struct DsColMap {
  /* Indices are record-field indices via aColToRec, not declared column indices. */
  int *aToRecTo;
  int *aToRecFrom;
  int *aFromRec;
  u8 *aFromMatched;
  int nTo;
  int nFrom;
};

static void dsFreeColMap(DsColMap *pMap){
  sqlite3_free(pMap->aToRecTo);
  sqlite3_free(pMap->aToRecFrom);
  sqlite3_free(pMap->aFromRec);
  sqlite3_free(pMap->aFromMatched);
  memset(pMap, 0, sizeof(*pMap));
}

static int dsBuildColMap(
  const DoltliteColInfo *pFrom,
  const DoltliteColInfo *pTo,
  DsColMap *pMap
){
  int i, j;
  memset(pMap, 0, sizeof(*pMap));
  pMap->nTo = pTo->nCol;
  pMap->nFrom = pFrom->nCol;
  if( pTo->nCol>0 ){
    pMap->aToRecTo = sqlite3_malloc(pTo->nCol * (int)sizeof(int));
    pMap->aToRecFrom = sqlite3_malloc(pTo->nCol * (int)sizeof(int));
    if( !pMap->aToRecTo || !pMap->aToRecFrom ){
      dsFreeColMap(pMap);
      return SQLITE_NOMEM;
    }
  }
  if( pFrom->nCol>0 ){
    pMap->aFromRec = sqlite3_malloc(pFrom->nCol * (int)sizeof(int));
    pMap->aFromMatched = sqlite3_malloc(pFrom->nCol * (int)sizeof(u8));
    if( !pMap->aFromRec || !pMap->aFromMatched ){
      dsFreeColMap(pMap);
      return SQLITE_NOMEM;
    }
    memset(pMap->aFromMatched, 0, pFrom->nCol * (int)sizeof(u8));
    for(j=0; j<pFrom->nCol; j++){
      pMap->aFromRec[j] = pFrom->aColToRec ? pFrom->aColToRec[j] : j;
    }
  }
  for(i=0; i<pTo->nCol; i++){
    pMap->aToRecTo[i] = pTo->aColToRec ? pTo->aColToRec[i] : i;
    pMap->aToRecFrom[i] = -1;
    for(j=0; j<pFrom->nCol; j++){
      if( strcmp(pFrom->azName[j], pTo->azName[i])==0 ){
        pMap->aToRecFrom[i] = pMap->aFromRec[j];
        pMap->aFromMatched[j] = 1;
        break;
      }
    }
  }
  return SQLITE_OK;
}

/* *pnDiffer is rows_modified: a gained/lost column counts only if it held a
** value. *pnModified is cells_modified: added columns count, dropped ones do
** not (already in cells_added/cells_deleted). */
static void dsCountChangedCells(
  const u8 *pFromRec, int nFromRec,
  const u8 *pToRec,   int nToRec,
  const DsColMap *pColMap,
  int *pnDiffer,
  int *pnModified
){
  DoltliteRecordInfo fromRi, toRi;
  int i;
  int nDiffer = 0;
  int nModified = 0;

  *pnDiffer = 0;
  *pnModified = 0;
  if( !pFromRec || !pToRec ) return;
  doltliteParseRecord(pFromRec, nFromRec, &fromRi);
  doltliteParseRecord(pToRec,   nToRec,   &toRi);

  for(i=0; i<pColMap->nTo; i++){
    int toRec = pColMap->aToRecTo ? pColMap->aToRecTo[i] : i;
    int fromRec = pColMap->aToRecFrom ? pColMap->aToRecFrom[i] : -1;
    int fromType, fromOffset, toType, toOffset;
    if( fromRec<0 ){
      /* Trailing NULLs are omitted from the record even when the to-column exists. */
      nModified++;
      if( toRec<toRi.nField && toRi.aType[toRec]!=0 ) nDiffer++;
      continue;
    }
    fromType = fromRec<fromRi.nField ? fromRi.aType[fromRec] : 0;
    fromOffset = fromRec<fromRi.nField ? fromRi.aOffset[fromRec] : 0;
    toType = toRec<toRi.nField ? toRi.aType[toRec] : 0;
    toOffset = toRec<toRi.nField ? toRi.aOffset[toRec] : 0;
    if( !doltliteFieldValuesEqual(
            fromType, pFromRec, nFromRec, fromOffset,
            toType,   pToRec,   nToRec,   toOffset) ){
      nDiffer++;
      nModified++;
    }
  }

  for(i=0; i<pColMap->nFrom; i++){
    int fromRec;
    if( pColMap->aFromMatched && pColMap->aFromMatched[i] ) continue;
    fromRec = pColMap->aFromRec ? pColMap->aFromRec[i] : i;
    if( fromRec>=fromRi.nField ) continue;
    if( fromRi.aType[fromRec]!=0 ) nDiffer++;
  }

  *pnDiffer = nDiffer;
  *pnModified = nModified;
}

/* A row whose primary key covers every column stores an empty record, so its
** values must come back from the key before the sides can be compared. */
static int dsCountModifiedRow(
  sqlite3 *db,
  const ProllyDiffChange *pChange,
  const DoltliteColInfo *pFromCi,
  const DoltliteColInfo *pToCi,
  const DsColMap *pColMap,
  int *pnDiffer,
  int *pnModified
){
  const u8 *pOld = pChange->pOldVal;
  const u8 *pNew = pChange->pNewVal;
  int nOld = pChange->nOldVal;
  int nNew = pChange->nNewVal;
  u8 *pOldOwned = 0;
  u8 *pNewOwned = 0;
  int nOwned = 0;
  int rc = SQLITE_OK;

  *pnDiffer = 0;
  *pnModified = 0;
  if( nOld==0 ){
    rc = doltliteRecordFromClusteredKeyCols(db, pFromCi, pChange->pKey,
                                            pChange->nKey, &pOldOwned, &nOwned);
    if( rc==SQLITE_OK && pOldOwned ){
      pOld = pOldOwned;
      nOld = nOwned;
    }
  }
  if( rc==SQLITE_OK && nNew==0 ){
    rc = doltliteRecordFromClusteredKeyCols(db, pToCi, pChange->pKey,
                                            pChange->nKey, &pNewOwned, &nOwned);
    if( rc==SQLITE_OK && pNewOwned ){
      pNew = pNewOwned;
      nNew = nOwned;
    }
  }
  if( rc==SQLITE_OK ){
    dsCountChangedCells(pOld, nOld, pNew, nNew, pColMap, pnDiffer, pnModified);
  }
  sqlite3_free(pOldOwned);
  sqlite3_free(pNewOwned);
  return rc;
}

typedef struct DsStatRow DsStatRow;
struct DsStatRow {
  char *zTableName;
  int schemaChanged;
  i64 rowsUnmodified;
  i64 rowsAdded;
  i64 rowsDeleted;
  i64 rowsModified;
  i64 cellsAdded;
  i64 cellsDeleted;
  i64 cellsModified;
  i64 oldRowCount;
  i64 newRowCount;
  i64 oldCellCount;
  i64 newCellCount;
};

typedef struct DsSummaryRow DsSummaryRow;
struct DsSummaryRow {
  char *zFromName;
  char *zToName;
  char *zDiffType;
  u8 dataChange;
  u8 schemaChange;
};

typedef struct DsFilterCtx DsFilterCtx;
struct DsFilterCtx {
  const char *zFromRef;
  const char *zToRef;
  const char *zTblFilter;
  ProllyHash fromCat;
  ProllyHash toCat;
  char **azNames;
  int nNames;
};

static void dsFilterCtxClear(DsFilterCtx *pCtx);

typedef DoltliteNameIndex DsNameIndex;

static void dsNameIndexClear(DsNameIndex *pIdx){
  doltliteNameIndexFree(pIdx);
}

static int dsNameIndexInit(
  DsNameIndex *pIdx,
  struct TableEntry *aCat,
  int nCat
){
  return doltliteNameIndexInit(pIdx, aCat, nCat,
                               (int)sizeof(struct TableEntry),
                               (int)offsetof(struct TableEntry, zName));
}



static struct TableEntry *dsFindTableByNameNoCase(
  struct TableEntry *aCat,
  int nCat,
  const char *zName
){
  int i;
  if( !zName ) return 0;
  for(i=0; i<nCat; i++){
    if( aCat[i].zName && sqlite3_stricmp(aCat[i].zName, zName)==0 ){
      return &aCat[i];
    }
  }
  return 0;
}

static int dsRequireRefs(sqlite3_vtab *pVtab, int idxNum, const char *zName){
  if( (idxNum & 3)!=3 ){
    sqlite3_free(pVtab->zErrMsg);
    pVtab->zErrMsg = sqlite3_mprintf("%s requires from_ref and to_ref", zName);
    return SQLITE_ERROR;
  }
  return SQLITE_OK;
}

static int dsArgText(
  sqlite3_vtab *pVtab,
  sqlite3_value *pValue,
  const char *zName,
  const char **pzValue
){
  if( sqlite3_value_type(pValue)==SQLITE_NULL ){
    sqlite3_free(pVtab->zErrMsg);
    pVtab->zErrMsg = sqlite3_mprintf("%s: invalid argument: NULL", zName);
    return pVtab->zErrMsg ? SQLITE_ERROR : SQLITE_NOMEM;
  }
  *pzValue = (const char*)sqlite3_value_text(pValue);
  return *pzValue ? SQLITE_OK : SQLITE_NOMEM;
}

static int dsComputeTableStats(
  sqlite3 *db,
  const char *zFromName,
  const char *zToName,
  const ProllyHash *pFromCatHash,
  const ProllyHash *pToCatHash,
  struct TableEntry *pFromEntry,
  struct TableEntry *pToEntry,
  DsStatRow *pOut
){
  int hasFrom = 0, hasTo = 0;
  int schemaChanged = 0;
  ProllyHash fromRoot, toRoot;
  u8 fromFlags = 0, toFlags = 0;
  char *zFromSql = 0, *zToSql = 0;
  DoltliteColInfo fromCi, toCi;
  int nFromCols = 0, nToCols = 0;
  DsColMap colMap;
  i64 oldCount = 0, newCount = 0;
  i64 rowsMod = 0, rowsAdd = 0, rowsDel = 0;
  i64 cellsMod = 0, cellsAdd = 0, cellsDel = 0;
  int rc;

  memset(pOut, 0, sizeof(*pOut));
  memset(&colMap, 0, sizeof(colMap));

  hasFrom = pFromEntry!=0;
  hasTo = pToEntry!=0;

  memset(&fromRoot, 0, sizeof(fromRoot));
  memset(&toRoot,   0, sizeof(toRoot));
  if( pFromEntry ){
    memcpy(&fromRoot, &pFromEntry->root, sizeof(ProllyHash));
    fromFlags = pFromEntry->flags;
  }
  if( pToEntry ){
    memcpy(&toRoot, &pToEntry->root, sizeof(ProllyHash));
    toFlags = pToEntry->flags;
  }

  if( !hasFrom && !hasTo ) return SQLITE_OK;

  memset(&fromCi, 0, sizeof(fromCi));
  memset(&toCi, 0, sizeof(toCi));

  if( hasFrom ){
    rc = dsLoadCreateSql(db, pFromCatHash, zFromName, &zFromSql);
    if( rc!=SQLITE_OK ) return rc;
    rc = dsLoadColInfo(db, pFromCatHash, zFromName, &fromCi);
    if( rc!=SQLITE_OK ) goto done;
    nFromCols = fromCi.nCol;
  }
  if( hasTo ){
    rc = dsLoadCreateSql(db, pToCatHash, zToName, &zToSql);
    if( rc!=SQLITE_OK ) goto done;
    rc = dsLoadColInfo(db, pToCatHash, zToName, &toCi);
    if( rc!=SQLITE_OK ){
      goto done;
    }
    nToCols = toCi.nCol;
  }

  schemaChanged =
    hasFrom && hasTo &&
    strcmp(zFromSql ? zFromSql : "", zToSql ? zToSql : "")!=0;

  if( hasFrom && hasTo ){
    rc = dsBuildColMap(&fromCi, &toCi, &colMap);
    if( rc!=SQLITE_OK ) goto done;
  }

  if( hasFrom ){
    rc = dsCountRows(db, &fromRoot, fromFlags, &oldCount);
    if( rc!=SQLITE_OK ) goto done;
  }
  if( hasTo ){
    rc = dsCountRows(db, &toRoot, toFlags, &newCount);
    if( rc!=SQLITE_OK ) goto done;
  }

  if( hasFrom && hasTo
   && prollyHashCompare(&fromRoot, &toRoot)!=0 ){
    ChunkStore *cs = doltliteGetChunkStore(db);
    ProllyCache *pCache = doltliteGetCache(db);
    ProllyDiffIter iter;
    ProllyDiffChange *pChange = 0;
    u8 ff = fromFlags ? fromFlags : toFlags;
    u8 tf = toFlags ? toFlags : fromFlags;
    int rcRow = SQLITE_OK;
    if( !cs || !pCache ){
      rc = SQLITE_ERROR;
      goto done;
    }
    rc = prollyDiffIterOpen(&iter, cs, pCache, &fromRoot, &toRoot, ff, tf);
    if( rc!=SQLITE_OK ) goto done;
    while( (rc = prollyDiffIterStep(&iter, &pChange))==SQLITE_ROW && pChange ){
      switch( pChange->type ){
        case PROLLY_DIFF_ADD:
          rowsAdd++;
          cellsAdd += nToCols;
          break;
        case PROLLY_DIFF_DELETE:
          rowsDel++;
          cellsDel += nFromCols;
          break;
        case PROLLY_DIFF_MODIFY: {
          int nDiffer = 0, nModified = 0;
          rcRow = dsCountModifiedRow(db, pChange, &fromCi, &toCi,
                                     &colMap, &nDiffer, &nModified);
          if( rcRow==SQLITE_OK && nDiffer>0 ){
            rowsMod++;
            cellsMod += nModified;
          }
          break;
        }
      }
      if( rcRow!=SQLITE_OK ) break;
    }
    prollyDiffIterClose(&iter);
    if( rcRow!=SQLITE_OK ){
      rc = rcRow;
      goto done;
    }
    if( rc!=SQLITE_DONE && rc!=SQLITE_ROW ) goto done;
    rc = SQLITE_OK;
  }

  if( hasFrom && hasTo ){
    i64 rowsInBoth = oldCount - rowsDel;
    if( rowsInBoth < 0 ) rowsInBoth = 0;
    if( nToCols > nFromCols ){
      cellsAdd += (i64)rowsInBoth * (nToCols - nFromCols);
    }else if( nFromCols > nToCols ){
      cellsDel += (i64)rowsInBoth * (nFromCols - nToCols);
    }
  }

  if( !hasFrom && hasTo ){
    rowsAdd = newCount;
    cellsAdd = (i64)newCount * nToCols;
  }
  if( hasFrom && !hasTo ){
    rowsDel = oldCount;
    cellsDel = (i64)oldCount * nFromCols;
  }

  pOut->zTableName     = sqlite3_mprintf("%s",
                             (hasTo && zToName) ? zToName : zFromName);
  pOut->schemaChanged  = schemaChanged;
  pOut->rowsAdded      = rowsAdd;
  pOut->rowsDeleted    = rowsDel;
  pOut->rowsModified   = rowsMod;
  pOut->rowsUnmodified = hasFrom ? oldCount - rowsDel - rowsMod : 0;
  if( pOut->rowsUnmodified<0 ) pOut->rowsUnmodified = 0;
  pOut->cellsAdded     = cellsAdd;
  pOut->cellsDeleted   = cellsDel;
  pOut->cellsModified  = cellsMod;
  pOut->oldRowCount    = oldCount;
  pOut->newRowCount    = newCount;
  pOut->oldCellCount   = (i64)oldCount * nFromCols;
  pOut->newCellCount   = (i64)newCount * nToCols;

  if( schemaChanged
   && rowsAdd==0 && rowsDel==0 && rowsMod==0
   && cellsAdd==0 && cellsDel==0 && cellsMod==0 ){
    pOut->rowsUnmodified = 0;
    pOut->oldRowCount = 0;
    pOut->newRowCount = 0;
    pOut->oldCellCount = 0;
    pOut->newCellCount = 0;
  }

done:
  sqlite3_free(zFromSql);
  sqlite3_free(zToSql);
  doltliteFreeColInfo(&fromCi);
  doltliteFreeColInfo(&toCi);
  dsFreeColMap(&colMap);
  return rc;
}

typedef struct DstVtab DstVtab;
struct DstVtab {
  sqlite3_vtab base;
  sqlite3 *db;
};

typedef struct DstCursor DstCursor;
struct DstCursor {
  sqlite3_vtab_cursor base;
  DsFilterCtx fctx;
  struct TableEntry *aFromCat;
  struct TableEntry *aToCat;
  int nFromCat;
  int nToCat;
  DsNameIndex fromIdx;
  DsNameIndex toIdx;
  DsStatRow row;
  int iName;
  int hasRow;
  sqlite3_int64 iRowid;
};

static const char *dstSchema =
  "CREATE TABLE x("
  "  table_name       TEXT,"
  "  rows_unmodified  INTEGER,"
  "  rows_added       INTEGER,"
  "  rows_deleted     INTEGER,"
  "  rows_modified    INTEGER,"
  "  cells_added      INTEGER,"
  "  cells_deleted    INTEGER,"
  "  cells_modified   INTEGER,"
  "  old_row_count    INTEGER,"
  "  new_row_count    INTEGER,"
  "  old_cell_count   INTEGER,"
  "  new_cell_count   INTEGER,"
  "  from_ref         TEXT HIDDEN,"
  "  to_ref           TEXT HIDDEN,"
  "  tbl              TEXT HIDDEN"
  ")";

#define DST_COL_FROM_REF 12
#define DST_COL_TO_REF   13
#define DST_COL_TBL      14

static void dstClearRow(DstCursor *c){
  sqlite3_free(c->row.zTableName);
  memset(&c->row, 0, sizeof(c->row));
}

static void dstCursorReset(DstCursor *c){
  dstClearRow(c);
  dsNameIndexClear(&c->fromIdx);
  dsNameIndexClear(&c->toIdx);
  doltliteFreeCatalog(c->aFromCat, c->nFromCat);
  doltliteFreeCatalog(c->aToCat, c->nToCat);
  dsFilterCtxClear(&c->fctx);
  c->aFromCat = 0;
  c->aToCat = 0;
  c->nFromCat = 0;
  c->nToCat = 0;
  c->iName = 0;
  c->hasRow = 0;
  c->iRowid = 0;
}

static int dstConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  DstVtab *v; int rc;
  (void)pAux; (void)argc; (void)argv; (void)pzErr;
  rc = doltliteVtabConnectSimple(db, dstSchema, sizeof(*v), ppVtab);
  if( rc!=SQLITE_OK ) return rc;
  v = (DstVtab*)*ppVtab;
  v->db = db;
  return SQLITE_OK;
}

static int dstBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  (void)pVtab;
  return doltliteBestIndexRefs(pInfo, DST_COL_FROM_REF, DST_COL_TO_REF,
                               DST_COL_TBL);
}

static int dstOpen(sqlite3_vtab *v, sqlite3_vtab_cursor **pp){
  (void)v;
  return doltliteVtabOpenCursor(pp, sizeof(DstCursor));
}

static int dstClose(sqlite3_vtab_cursor *cur){
  DstCursor *c = (DstCursor*)cur;
  dstCursorReset(c);
  sqlite3_free(c);
  return SQLITE_OK;
}

static int dsAppendTableNames(
  struct TableEntry *aCat, int nCat,
  const DsNameIndex *pSkip,
  char ***paz, int *pn, int *pAlloc
){
  int i;
  for(i=0; i<nCat; i++){
    const char *zName = aCat[i].zName;
    if( !zName || aCat[i].iTable==1 ) continue;
    if( pSkip && addNameIndexFind(pSkip, zName) ) continue;
    if( *pn>=*pAlloc ){
      int newAlloc = *pAlloc ? *pAlloc*2 : 8;
      char **aNew = sqlite3_realloc(*paz, newAlloc*(int)sizeof(char*));
      if( !aNew ) return SQLITE_NOMEM;
      *paz = aNew;
      *pAlloc = newAlloc;
    }
    (*paz)[*pn] = sqlite3_mprintf("%s", zName);
    if( !(*paz)[*pn] ) return SQLITE_NOMEM;
    (*pn)++;
  }
  return SQLITE_OK;
}

static int dsCollectTableNames(
  sqlite3 *db,
  const ProllyHash *pFromCat,
  const ProllyHash *pToCat,
  char ***pazOut, int *pnOut
){
  struct TableEntry *aFrom = 0, *aTo = 0;
  int nFrom = 0, nTo = 0;
  DsNameIndex fromIdx;
  char **az = 0;
  int n = 0, alloc = 0;
  int rc, j;

  memset(&fromIdx, 0, sizeof(fromIdx));
  *pazOut = 0;
  *pnOut = 0;

  rc = doltliteLoadCatalog(db, pFromCat, &aFrom, &nFrom, 0);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteLoadCatalog(db, pToCat, &aTo, &nTo, 0);
  if( rc!=SQLITE_OK ){
    doltliteFreeCatalog(aFrom, nFrom);
    return rc;
  }

  rc = dsNameIndexInit(&fromIdx, aFrom, nFrom);
  if( rc!=SQLITE_OK ) goto fail;
  rc = dsAppendTableNames(aFrom, nFrom, 0, &az, &n, &alloc);
  if( rc==SQLITE_OK ){
    rc = dsAppendTableNames(aTo, nTo, &fromIdx, &az, &n, &alloc);
  }
  if( rc!=SQLITE_OK ) goto fail;

  dsNameIndexClear(&fromIdx);
  doltliteFreeCatalog(aFrom, nFrom);
  doltliteFreeCatalog(aTo, nTo);
  *pazOut = az;
  *pnOut = n;
  return SQLITE_OK;

fail:
  for(j=0; j<n; j++) sqlite3_free(az[j]);
  sqlite3_free(az);
  dsNameIndexClear(&fromIdx);
  doltliteFreeCatalog(aFrom, nFrom);
  doltliteFreeCatalog(aTo, nTo);
  return rc;
}

static void dsFilterCtxClear(DsFilterCtx *pCtx){
  doltliteFreeStringArray(pCtx->azNames, pCtx->nNames);
  memset(pCtx, 0, sizeof(*pCtx));
}

static int dsRefError(
  sqlite3_vtab *pVtab,
  const char *zName,
  const char *zRef,
  int rc
){
  const char *zKind;
  if( rc!=SQLITE_NOTFOUND && rc!=SQLITE_ERROR ) return rc;
  zKind = rc==SQLITE_NOTFOUND ? "ref not found" : "invalid ref";
  sqlite3_free(pVtab->zErrMsg);
  pVtab->zErrMsg = sqlite3_mprintf(
      "%s: %s: %s", zName, zKind, zRef ? zRef : "");
  return pVtab->zErrMsg ? SQLITE_ERROR : SQLITE_NOMEM;
}

static int dsFilterInit(
  sqlite3 *db,
  sqlite3_vtab *pVtab,
  int idxNum,
  int argc,
  sqlite3_value **argv,
  const char *zName,
  DsFilterCtx *pCtx
){
  int rc;
  int argIdx = 0;

  memset(pCtx, 0, sizeof(*pCtx));
  rc = dsRequireRefs(pVtab, idxNum, zName);
  if( rc!=SQLITE_OK ) return rc;

  if( (idxNum & 1) && argIdx<argc ){
    rc = dsArgText(pVtab, argv[argIdx++], zName, &pCtx->zFromRef);
    if( rc!=SQLITE_OK ) return rc;
  }
  if( (idxNum & 2) && argIdx<argc ){
    rc = dsArgText(pVtab, argv[argIdx++], zName, &pCtx->zToRef);
    if( rc!=SQLITE_OK ) return rc;
  }
  if( (idxNum & 4) && argIdx<argc ){
    rc = dsArgText(pVtab, argv[argIdx++], zName, &pCtx->zTblFilter);
    if( rc!=SQLITE_OK ) return rc;
  }

  rc = doltliteResolveCatalogHashForRef(db, pCtx->zFromRef, &pCtx->fromCat);
  if( rc!=SQLITE_OK ) return dsRefError(
      pVtab, zName, pCtx->zFromRef, rc);
  rc = doltliteResolveCatalogHashForRef(db, pCtx->zToRef, &pCtx->toCat);
  if( rc!=SQLITE_OK ) return dsRefError(
      pVtab, zName, pCtx->zToRef, rc);
  if( pCtx->zTblFilter ){
    pCtx->azNames = sqlite3_malloc((int)sizeof(char*));
    if( !pCtx->azNames ) return SQLITE_NOMEM;
    pCtx->azNames[0] = sqlite3_mprintf("%s", pCtx->zTblFilter);
    if( !pCtx->azNames[0] ){
      sqlite3_free(pCtx->azNames);
      pCtx->azNames = 0;
      return SQLITE_NOMEM;
    }
    pCtx->nNames = 1;
    return SQLITE_OK;
  }
  return dsCollectTableNames(db, &pCtx->fromCat, &pCtx->toCat,
                             &pCtx->azNames, &pCtx->nNames);
}

static int dsTableNameMatchesFilter(const DsFilterCtx *pCtx, const char *zName){
  return !pCtx->zTblFilter
      || sqlite3_stricmp(zName, pCtx->zTblFilter)==0;
}

/* Name missing on the other side: same iTable under a name also absent from
** this side is a rename. A name still on both sides is coincidental reuse. */
static struct TableEntry *dsRenamePartner(
  struct TableEntry *aOther, int nOther,
  const struct TableEntry *pRef,
  DsNameIndex *pRefSideIdx
){
  struct TableEntry *p;
  if( !pRef ) return 0;
  p = doltliteFindTableByNumber(aOther, nOther, pRef->iTable);
  if( !p || !p->zName ) return 0;
  if( addNameIndexFind(pRefSideIdx, p->zName) ) return 0;
  return p;
}

static int dstAdvance(DstCursor *c, sqlite3 *db){
  DsFilterCtx *pCtx = &c->fctx;
  int rc;

  dstClearRow(c);
  c->hasRow = 0;

  while( c->iName<pCtx->nNames ){
    const char *zName = pCtx->azNames[c->iName++];
    const char *zFromName = zName;
    const char *zToName = zName;
    struct TableEntry *pFromEntry, *pToEntry;
    DsStatRow row;

    if( !dsTableNameMatchesFilter(pCtx, zName) ) continue;

    if( pCtx->zTblFilter ){
      pFromEntry = dsFindTableByNameNoCase(c->aFromCat, c->nFromCat, zName);
      pToEntry = dsFindTableByNameNoCase(c->aToCat, c->nToCat, zName);
      if( pFromEntry ) zFromName = pFromEntry->zName;
      if( pToEntry ) zToName = pToEntry->zName;
    }else{
      pFromEntry = addNameIndexFind(&c->fromIdx, zName);
      pToEntry = addNameIndexFind(&c->toIdx, zName);
      if( pFromEntry && !pToEntry ){
        struct TableEntry *pRen =
            dsRenamePartner(c->aToCat, c->nToCat, pFromEntry, &c->fromIdx);
        if( pRen ){
          rc = dsComputeTableStats(db, zName, pRen->zName,
                                   &pCtx->fromCat, &pCtx->toCat,
                                   pFromEntry, pRen, &row);
          if( rc!=SQLITE_OK ){ sqlite3_free(row.zTableName); return rc; }
          if( row.zTableName
           && (row.rowsAdded || row.rowsDeleted || row.rowsModified
            || row.cellsAdded || row.cellsDeleted || row.cellsModified) ){
            c->row = row;
            c->hasRow = 1;
            return SQLITE_OK;
          }
          sqlite3_free(row.zTableName);
          continue;
        }
      }else if( !pFromEntry && pToEntry
             && dsRenamePartner(c->aFromCat, c->nFromCat, pToEntry,
                                &c->toIdx) ){
        continue;
      }
    }
    if( pFromEntry && pToEntry
     && prollyHashCompare(&pFromEntry->root, &pToEntry->root)==0
     && prollyHashCompare(&pFromEntry->schemaHash, &pToEntry->schemaHash)==0 ){
      continue;
    }
    rc = dsComputeTableStats(db, zFromName, zToName,
                             &pCtx->fromCat, &pCtx->toCat,
                             pFromEntry, pToEntry, &row);
    if( rc!=SQLITE_OK ){
      sqlite3_free(row.zTableName);
      return rc;
    }
    if( !row.zTableName ) continue;
    if( pCtx->zTblFilter ){
      char *zFilteredName = sqlite3_mprintf("%s", pCtx->zTblFilter);
      if( !zFilteredName ){
        sqlite3_free(row.zTableName);
        return SQLITE_NOMEM;
      }
      sqlite3_free(row.zTableName);
      row.zTableName = zFilteredName;
    }

    if( row.rowsAdded==0 && row.rowsDeleted==0 && row.rowsModified==0
     && row.cellsAdded==0 && row.cellsDeleted==0 && row.cellsModified==0 ){
      if( !row.schemaChanged ){
        sqlite3_free(row.zTableName);
        continue;
      }
    }
    c->row = row;
    c->hasRow = 1;
    return SQLITE_OK;
  }

  return SQLITE_OK;
}

static int dstFilter(sqlite3_vtab_cursor *cur,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  DstCursor *c = (DstCursor*)cur;
  DstVtab *v = (DstVtab*)cur->pVtab;
  sqlite3 *db = v->db;
  int rc;
  (void)idxStr;

  dstCursorReset(c);

  rc = dsFilterInit(db, &v->base, idxNum, argc, argv, "dolt_diff_stat",
                    &c->fctx);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteLoadCatalog(db, &c->fctx.fromCat, &c->aFromCat, &c->nFromCat, 0);
  if( rc!=SQLITE_OK ) goto done;
  rc = doltliteLoadCatalog(db, &c->fctx.toCat, &c->aToCat, &c->nToCat, 0);
  if( rc!=SQLITE_OK ) goto done;
  /* Table filter must name a table on at least one side; else "table not found". */
  if( c->fctx.zTblFilter
   && !dsFindTableByNameNoCase(c->aFromCat, c->nFromCat,
                               c->fctx.zTblFilter)
   && !dsFindTableByNameNoCase(c->aToCat, c->nToCat,
                               c->fctx.zTblFilter) ){
    sqlite3_free(v->base.zErrMsg);
    v->base.zErrMsg = sqlite3_mprintf("table not found: %s", c->fctx.zTblFilter);
    rc = SQLITE_ERROR;
    goto done;
  }
  if( !c->fctx.zTblFilter ){
    rc = dsNameIndexInit(&c->fromIdx, c->aFromCat, c->nFromCat);
    if( rc!=SQLITE_OK ) goto done;
    rc = dsNameIndexInit(&c->toIdx, c->aToCat, c->nToCat);
    if( rc!=SQLITE_OK ) goto done;
  }

  rc = dstAdvance(c, db);

done:
  if( rc!=SQLITE_OK ) dstCursorReset(c);
  return rc;
}

static int dstNext(sqlite3_vtab_cursor *cur){
  DstCursor *c = (DstCursor*)cur;
  DstVtab *v = (DstVtab*)cur->pVtab;
  c->iRowid++;
  return dstAdvance(c, v->db);
}

static int dstEof(sqlite3_vtab_cursor *cur){
  DstCursor *c = (DstCursor*)cur;
  return !c->hasRow;
}

static int dstColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int col){
  DstCursor *c = (DstCursor*)cur;
  DsStatRow *r = &c->row;
  if( !c->hasRow ) return SQLITE_OK;
  switch( col ){
    case 0:  sqlite3_result_text(ctx, r->zTableName, -1, SQLITE_TRANSIENT); break;
    case 1:  sqlite3_result_int64(ctx, r->rowsUnmodified); break;
    case 2:  sqlite3_result_int64(ctx, r->rowsAdded); break;
    case 3:  sqlite3_result_int64(ctx, r->rowsDeleted); break;
    case 4:  sqlite3_result_int64(ctx, r->rowsModified); break;
    case 5:  sqlite3_result_int64(ctx, r->cellsAdded); break;
    case 6:  sqlite3_result_int64(ctx, r->cellsDeleted); break;
    case 7:  sqlite3_result_int64(ctx, r->cellsModified); break;
    case 8:  sqlite3_result_int64(ctx, r->oldRowCount); break;
    case 9:  sqlite3_result_int64(ctx, r->newRowCount); break;
    case 10: sqlite3_result_int64(ctx, r->oldCellCount); break;
    case 11: sqlite3_result_int64(ctx, r->newCellCount); break;
    default: sqlite3_result_null(ctx); break;
  }
  return SQLITE_OK;
}

static int dstRowid(sqlite3_vtab_cursor *cur, sqlite3_int64 *r){
  *r = ((DstCursor*)cur)->iRowid;
  return SQLITE_OK;
}

static sqlite3_module diffStatModule = {
  0, 0, dstConnect, dstBestIndex, doltliteVtabDisconnect, 0,
  dstOpen, dstClose, dstFilter, dstNext, dstEof,
  dstColumn, dstRowid,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};

typedef struct DssVtab DssVtab;
struct DssVtab {
  sqlite3_vtab base;
  sqlite3 *db;
};

typedef struct DssCursor DssCursor;
struct DssCursor {
  sqlite3_vtab_cursor base;
  DsFilterCtx fctx;
  struct TableEntry *aFromCat;
  struct TableEntry *aToCat;
  int nFromCat;
  int nToCat;
  DsNameIndex fromIdx;
  DsNameIndex toIdx;
  DsSummaryRow row;
  int iName;
  int hasRow;
  sqlite3_int64 iRowid;
};

static const char *dssSchema =
  "CREATE TABLE x("
  "  from_table_name TEXT,"
  "  to_table_name   TEXT,"
  "  diff_type       TEXT,"
  "  data_change     INTEGER,"
  "  schema_change   INTEGER,"
  "  from_ref        TEXT HIDDEN,"
  "  to_ref          TEXT HIDDEN,"
  "  tbl             TEXT HIDDEN"
  ")";

#define DSS_COL_FROM_REF 5
#define DSS_COL_TO_REF   6
#define DSS_COL_TBL      7

static void dssClearRow(DssCursor *c){
  sqlite3_free(c->row.zFromName);
  sqlite3_free(c->row.zToName);
  sqlite3_free(c->row.zDiffType);
  memset(&c->row, 0, sizeof(c->row));
}

static void dssCursorReset(DssCursor *c){
  dssClearRow(c);
  dsNameIndexClear(&c->fromIdx);
  dsNameIndexClear(&c->toIdx);
  doltliteFreeCatalog(c->aFromCat, c->nFromCat);
  doltliteFreeCatalog(c->aToCat, c->nToCat);
  dsFilterCtxClear(&c->fctx);
  c->aFromCat = 0;
  c->aToCat = 0;
  c->nFromCat = 0;
  c->nToCat = 0;
  c->iName = 0;
  c->hasRow = 0;
  c->iRowid = 0;
}

static int dssConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  DssVtab *v; int rc;
  (void)pAux; (void)argc; (void)argv; (void)pzErr;
  rc = doltliteVtabConnectSimple(db, dssSchema, sizeof(*v), ppVtab);
  if( rc!=SQLITE_OK ) return rc;
  v = (DssVtab*)*ppVtab;
  v->db = db;
  return SQLITE_OK;
}

static int dssBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  (void)pVtab;
  return doltliteBestIndexRefs(pInfo, DSS_COL_FROM_REF, DSS_COL_TO_REF,
                               DSS_COL_TBL);
}

static int dssOpen(sqlite3_vtab *v, sqlite3_vtab_cursor **pp){
  (void)v;
  return doltliteVtabOpenCursor(pp, sizeof(DssCursor));
}

static int dssClose(sqlite3_vtab_cursor *cur){
  DssCursor *c = (DssCursor*)cur;
  dssCursorReset(c);
  sqlite3_free(c);
  return SQLITE_OK;
}

static int dssSetRow(DssCursor *c, const char *zFrom, const char *zTo,
                     const char *zDiffType, int dataChange, int schemaChange){
  dssClearRow(c);
  c->row.zFromName = sqlite3_mprintf("%s", zFrom ? zFrom : "");
  c->row.zToName = sqlite3_mprintf("%s", zTo ? zTo : "");
  c->row.zDiffType = sqlite3_mprintf("%s", zDiffType);
  c->row.dataChange = (u8)(dataChange ? 1 : 0);
  c->row.schemaChange = (u8)(schemaChange ? 1 : 0);
  if( !c->row.zFromName || !c->row.zToName || !c->row.zDiffType ){
    dssClearRow(c);
    return SQLITE_NOMEM;
  }
  c->hasRow = 1;
  return SQLITE_OK;
}

/* Schema change rewrites every row, so root hash cannot answer data change;
** walk values. Only used when schema also changed. */
static int dssDataActuallyChanged(
  sqlite3 *db,
  const DsFilterCtx *pCtx,
  const char *zTableName,
  struct TableEntry *pFromEntry,
  struct TableEntry *pToEntry,
  int *pChanged
){
  DoltliteColInfo fromCi, toCi;
  DsColMap colMap;
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  ProllyDiffIter iter;
  ProllyDiffChange *pChange = 0;
  int changed = 0;
  int iterOpen = 0;
  int rc;

  memset(&fromCi, 0, sizeof(fromCi));
  memset(&toCi, 0, sizeof(toCi));
  memset(&colMap, 0, sizeof(colMap));
  *pChanged = 1;
  if( !cs || !pCache ) return SQLITE_OK;

  rc = dsLoadColInfo(db, &pCtx->fromCat, zTableName, &fromCi);
  if( rc!=SQLITE_OK ) goto done;
  rc = dsLoadColInfo(db, &pCtx->toCat, zTableName, &toCi);
  if( rc!=SQLITE_OK ) goto done;
  rc = dsBuildColMap(&fromCi, &toCi, &colMap);
  if( rc!=SQLITE_OK ) goto done;

  rc = prollyDiffIterOpen(&iter, cs, pCache, &pFromEntry->root,
                          &pToEntry->root,
                          pFromEntry->flags ? pFromEntry->flags
                                            : pToEntry->flags,
                          pToEntry->flags ? pToEntry->flags
                                          : pFromEntry->flags);
  if( rc!=SQLITE_OK ) goto done;
  iterOpen = 1;

  while( (rc = prollyDiffIterStep(&iter, &pChange))==SQLITE_ROW && pChange ){
    int nDiffer = 0, nModified = 0;
    if( pChange->type!=PROLLY_DIFF_MODIFY ){
      changed = 1;
      break;
    }
    rc = dsCountModifiedRow(db, pChange, &fromCi, &toCi,
                            &colMap, &nDiffer, &nModified);
    if( rc!=SQLITE_OK ) goto done;
    if( nDiffer>0 ){
      changed = 1;
      break;
    }
  }
  if( rc==SQLITE_ROW || rc==SQLITE_DONE ) rc = SQLITE_OK;
  if( rc==SQLITE_OK ) *pChanged = changed;

done:
  if( iterOpen ) prollyDiffIterClose(&iter);
  doltliteFreeColInfo(&fromCi);
  doltliteFreeColInfo(&toCi);
  dsFreeColMap(&colMap);
  return rc;
}

static int dssAppendTableChange(
  DssCursor *c,
  sqlite3 *db,
  const char *zTableName,
  struct TableEntry *pFromEntry,
  struct TableEntry *pToEntry
){
  int rc;
  i64 rowCount = 0;
  int dataChange;
  int schemaChange;
  const char *zDiffType;

  if( !pFromEntry && !pToEntry ) return SQLITE_OK;

  if( pFromEntry && pToEntry ){
    int rootsDiffer = prollyHashCompare(&pFromEntry->root, &pToEntry->root)!=0;
    int schemasDiffer = prollyHashCompare(&pFromEntry->schemaHash,
                                          &pToEntry->schemaHash)!=0;
    if( !rootsDiffer && !schemasDiffer ) return SQLITE_OK;
    if( rootsDiffer && schemasDiffer ){
      rc = dssDataActuallyChanged(db, &c->fctx, zTableName,
                                  pFromEntry, pToEntry, &rootsDiffer);
      if( rc!=SQLITE_OK ) return rc;
    }
    return dssSetRow(c, zTableName, zTableName, "modified",
                     rootsDiffer, schemasDiffer);
  }

  if( pToEntry ){
    rc = dsCountRows(db, &pToEntry->root, pToEntry->flags, &rowCount);
    if( rc!=SQLITE_OK ) return rc;
    dataChange = rowCount > 0;
    schemaChange = 1;
    zDiffType = "added";
    return dssSetRow(c, "", zTableName, zDiffType, dataChange, schemaChange);
  }

  rc = dsCountRows(db, &pFromEntry->root, pFromEntry->flags, &rowCount);
  if( rc!=SQLITE_OK ) return rc;
  dataChange = rowCount > 0;
  schemaChange = 1;
  zDiffType = "dropped";
  return dssSetRow(c, zTableName, "", zDiffType, dataChange, schemaChange);
}

static int dssAdvance(DssCursor *c, sqlite3 *db){
  DsFilterCtx *pCtx = &c->fctx;
  int rc;

  dssClearRow(c);
  c->hasRow = 0;

  while( c->iName<pCtx->nNames ){
    const char *zName = pCtx->azNames[c->iName++];
    struct TableEntry *pFromEntry, *pToEntry;

    if( !dsTableNameMatchesFilter(pCtx, zName) ) continue;

    if( pCtx->zTblFilter ){
      pFromEntry = dsFindTableByNameNoCase(c->aFromCat, c->nFromCat, zName);
      pToEntry = dsFindTableByNameNoCase(c->aToCat, c->nToCat, zName);
    }else{
      pFromEntry = addNameIndexFind(&c->fromIdx, zName);
      pToEntry = addNameIndexFind(&c->toIdx, zName);
      if( pFromEntry && !pToEntry ){
        struct TableEntry *pRen =
            dsRenamePartner(c->aToCat, c->nToCat, pFromEntry, &c->fromIdx);
        if( pRen ){
          int dataChange =
              prollyHashCompare(&pFromEntry->root, &pRen->root)!=0;
          rc = dssSetRow(c, pFromEntry->zName, pRen->zName, "renamed",
                         dataChange, 1);
          if( rc!=SQLITE_OK ) return rc;
          return SQLITE_OK;
        }
      }else if( !pFromEntry && pToEntry
             && dsRenamePartner(c->aFromCat, c->nFromCat, pToEntry,
                                &c->toIdx) ){
        continue;
      }
    }
    if( pCtx->zTblFilter ){
      if( pToEntry ) zName = pToEntry->zName;
      else if( pFromEntry ) zName = pFromEntry->zName;
    }
    rc = dssAppendTableChange(c, db, zName, pFromEntry, pToEntry);
    if( rc!=SQLITE_OK ) return rc;
    if( c->hasRow ) return SQLITE_OK;
  }

  return SQLITE_OK;
}

static int dssFilter(sqlite3_vtab_cursor *cur,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  DssCursor *c = (DssCursor*)cur;
  DssVtab *v = (DssVtab*)cur->pVtab;
  sqlite3 *db = v->db;
  int rc;
  (void)idxStr;

  dssCursorReset(c);

  rc = dsFilterInit(db, &v->base, idxNum, argc, argv,
                    "dolt_diff_summary", &c->fctx);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteLoadCatalog(db, &c->fctx.fromCat, &c->aFromCat, &c->nFromCat, 0);
  if( rc!=SQLITE_OK ) goto done;
  rc = doltliteLoadCatalog(db, &c->fctx.toCat, &c->aToCat, &c->nToCat, 0);
  if( rc!=SQLITE_OK ) goto done;
  if( !c->fctx.zTblFilter ){
    rc = dsNameIndexInit(&c->fromIdx, c->aFromCat, c->nFromCat);
    if( rc!=SQLITE_OK ) goto done;
    rc = dsNameIndexInit(&c->toIdx, c->aToCat, c->nToCat);
    if( rc!=SQLITE_OK ) goto done;
  }

  rc = dssAdvance(c, db);

done:
  if( rc!=SQLITE_OK ) dssCursorReset(c);
  return rc;
}

static int dssNext(sqlite3_vtab_cursor *cur){
  DssCursor *c = (DssCursor*)cur;
  DssVtab *v = (DssVtab*)cur->pVtab;
  c->iRowid++;
  return dssAdvance(c, v->db);
}

static int dssEof(sqlite3_vtab_cursor *cur){
  DssCursor *c = (DssCursor*)cur;
  return !c->hasRow;
}

static int dssColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int col){
  DssCursor *c = (DssCursor*)cur;
  DsSummaryRow *r = &c->row;
  if( !c->hasRow ) return SQLITE_OK;
  switch( col ){
    case 0: sqlite3_result_text(ctx, r->zFromName, -1, SQLITE_TRANSIENT); break;
    case 1: sqlite3_result_text(ctx, r->zToName,   -1, SQLITE_TRANSIENT); break;
    case 2: sqlite3_result_text(ctx, r->zDiffType, -1, SQLITE_TRANSIENT); break;
    case 3: sqlite3_result_int(ctx, r->dataChange); break;
    case 4: sqlite3_result_int(ctx, r->schemaChange); break;
    default: sqlite3_result_null(ctx); break;
  }
  return SQLITE_OK;
}

static int dssRowid(sqlite3_vtab_cursor *cur, sqlite3_int64 *r){
  *r = ((DssCursor*)cur)->iRowid;
  return SQLITE_OK;
}

static sqlite3_module diffSummaryModule = {
  0, 0, dssConnect, dssBestIndex, doltliteVtabDisconnect, 0,
  dssOpen, dssClose, dssFilter, dssNext, dssEof,
  dssColumn, dssRowid,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};

int doltliteDiffStatRegister(sqlite3 *db){
  int rc = sqlite3_create_module(db, "dolt_diff_stat",
                                 &diffStatModule, 0);
  if( rc==SQLITE_OK ){
    rc = sqlite3_create_module(db, "dolt_diff_summary",
                               &diffSummaryModule, 0);
  }
  return rc;
}

#endif
