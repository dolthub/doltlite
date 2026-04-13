

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
#include <string.h>

static int dsLoadColNames(sqlite3 *db,
                          const ProllyHash *pCatHash,
                          const char *zTableName,
                          char ***pazOut, int *pnOut){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  SchemaEntry *aSchemas = 0;
  int nSchemas = 0;
  SchemaEntry *pEntry;
  sqlite3 *tmp = 0;
  sqlite3_stmt *pStmt = 0;
  char *zPragma = 0;
  char **az = 0;
  int n = 0, alloc = 0;
  int rc;

  *pazOut = 0;
  *pnOut = 0;
  if( prollyHashIsEmpty(pCatHash) ) return SQLITE_OK;

  rc = loadSchemaFromCatalog(db, cs, pCache, pCatHash, &aSchemas, &nSchemas);
  if( rc!=SQLITE_OK ) return rc;
  pEntry = findSchemaEntry(aSchemas, nSchemas, zTableName);
  if( !pEntry || !pEntry->zSql ){
    freeSchemaEntries(aSchemas, nSchemas);
    return SQLITE_OK;
  }

  rc = sqlite3_open(":memory:", &tmp);
  if( rc!=SQLITE_OK ) goto cleanup;
  rc = sqlite3_exec(tmp, pEntry->zSql, 0, 0, 0);
  if( rc!=SQLITE_OK ) goto cleanup;

  zPragma = sqlite3_mprintf("PRAGMA table_info(\"%w\")", zTableName);
  if( !zPragma ){ rc = SQLITE_NOMEM; goto cleanup; }
  rc = sqlite3_prepare_v2(tmp, zPragma, -1, &pStmt, 0);
  if( rc!=SQLITE_OK ) goto cleanup;

  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    const char *zName = (const char*)sqlite3_column_text(pStmt, 1);
    if( n>=alloc ){
      int newAlloc = alloc ? alloc*2 : 8;
      char **aNew = sqlite3_realloc(az, newAlloc*(int)sizeof(char*));
      if( !aNew ){ rc = SQLITE_NOMEM; break; }
      az = aNew;
      alloc = newAlloc;
    }
    az[n] = sqlite3_mprintf("%s", zName ? zName : "");
    if( !az[n] ){ rc = SQLITE_NOMEM; break; }
    n++;
  }

cleanup:
  if( pStmt ) sqlite3_finalize(pStmt);
  sqlite3_free(zPragma);
  if( tmp ) sqlite3_close(tmp);
  freeSchemaEntries(aSchemas, nSchemas);
  if( rc!=SQLITE_OK && rc!=SQLITE_DONE ){
    int k;
    for(k=0; k<n; k++) sqlite3_free(az[k]);
    sqlite3_free(az);
    return rc;
  }
  *pazOut = az;
  *pnOut = n;
  return SQLITE_OK;
}

static void dsFreeColNames(char **az, int n){
  int i;
  for(i=0; i<n; i++) sqlite3_free(az[i]);
  sqlite3_free(az);
}

static int dsCountRows(sqlite3 *db, const ProllyHash *pRoot, u8 flags,
                       i64 *pnRow){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  ProllyCursor cur;
  int rc, res;
  i64 n = 0;
  if( pnRow ) *pnRow = 0;
  if( !cs || !pCache ) return SQLITE_ERROR;
  if( prollyHashIsEmpty(pRoot) ) return SQLITE_OK;
  prollyCursorInit(&cur, cs, pCache, pRoot, flags);
  rc = prollyCursorFirst(&cur, &res);
  if( rc!=SQLITE_OK ){ prollyCursorClose(&cur); return rc; }
  while( !res && prollyCursorIsValid(&cur) ){
    n++;
    rc = prollyCursorNext(&cur);
    if( rc!=SQLITE_OK ){ prollyCursorClose(&cur); return rc; }
  }
  prollyCursorClose(&cur);
  if( pnRow ) *pnRow = n;
  return SQLITE_OK;
}

static i64 dsReadInt(const u8 *p, int nBytes){
  i64 v;
  int i;
  if( nBytes<=0 ) return 0;
  v = (p[0] & 0x80) ? -1 : 0;
  for(i=0; i<nBytes; i++) v = (v<<8) | p[i];
  return v;
}

/* SQLite packs integers into the narrowest serial type that fits, so
** 42 may be stored as type-1 (1 byte) on one side and type-2 (2 bytes)
** on the other. Raw memcmp of fields would call those "modified".
** Here we coerce any integer-family type (1..6, 8, 9) to a signed
** int64 and compare numerically, so equal numeric values produce
** equal cell counts regardless of encoding width. */
static int dsFieldValuesEqual(
  int aType, const u8 *pA, int nA, int aOff,
  int bType, const u8 *pB, int nB, int bOff
){
  i64 ai, bi;
  int aLen, bLen;
  if( aType==0 && bType==0 ) return 1;
  if( aType==0 || bType==0 ) return 0;
  {
    int aIsInt = (aType>=1 && aType<=6) || aType==8 || aType==9;
    int bIsInt = (bType>=1 && bType<=6) || bType==8 || bType==9;
    if( aIsInt && bIsInt ){
      if( aType==8 )      ai = 0;
      else if( aType==9 ) ai = 1;
      else{
        aLen = dlSerialTypeLen(aType);
        if( aOff<0 || aOff+aLen>nA ) return 0;
        ai = dsReadInt(pA+aOff, aLen);
      }
      if( bType==8 )      bi = 0;
      else if( bType==9 ) bi = 1;
      else{
        bLen = dlSerialTypeLen(bType);
        if( bOff<0 || bOff+bLen>nB ) return 0;
        bi = dsReadInt(pB+bOff, bLen);
      }
      return ai==bi;
    }
  }
  if( aType != bType ) return 0;
  aLen = dlSerialTypeLen(aType);
  if( aLen<0 ) return 0;
  if( aOff<0 || aOff+aLen>nA ) return 0;
  if( bOff<0 || bOff+aLen>nB ) return 0;
  return memcmp(pA+aOff, pB+bOff, aLen)==0;
}

static int dsCountChangedCells(
  const u8 *pFromRec, int nFromRec,
  const u8 *pToRec,   int nToRec,
  char **azFromCols,  int nFromCols,
  char **azToCols,    int nToCols
){
  DoltliteRecordInfo fromRi, toRi;
  int i, changed = 0;
  if( !pFromRec || !pToRec ) return 0;
  doltliteParseRecord(pFromRec, nFromRec, &fromRi);
  doltliteParseRecord(pToRec,   nToRec,   &toRi);


  for(i=0; i<nToCols; i++){
    int fromIdx;
    for(fromIdx=0; fromIdx<nFromCols; fromIdx++){
      if( strcmp(azFromCols[fromIdx], azToCols[i])==0 ) break;
    }
    if( fromIdx>=nFromCols ){

      if( i<toRi.nField && toRi.aType[i]!=0 ) changed++;
      continue;
    }
    if( i>=toRi.nField || fromIdx>=fromRi.nField ) continue;
    if( !dsFieldValuesEqual(
            fromRi.aType[fromIdx], pFromRec, nFromRec, fromRi.aOffset[fromIdx],
            toRi.aType[i],         pToRec,   nToRec,   toRi.aOffset[i]) ){
      changed++;
    }
  }

  for(i=0; i<nFromCols; i++){
    int toIdx;
    for(toIdx=0; toIdx<nToCols; toIdx++){
      if( strcmp(azToCols[toIdx], azFromCols[i])==0 ) break;
    }
    if( toIdx<nToCols ) continue;
    if( i>=fromRi.nField ) continue;
    if( fromRi.aType[i]!=0 ) changed++;
  }
  return changed;
}

typedef struct DsStatRow DsStatRow;
struct DsStatRow {
  char *zTableName;
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

static int dsResolveCatHash(sqlite3 *db, const char *zRef,
                            ProllyHash *pOut){
  DoltliteCommit commit;
  ProllyHash commitHash;
  int rc;
  if( zRef ){
    rc = doltliteResolveRef(db, zRef, &commitHash);
    if( rc!=SQLITE_OK ) return rc;
    memset(&commit, 0, sizeof(commit));
    rc = doltliteLoadCommit(db, &commitHash, &commit);
    if( rc!=SQLITE_OK ) return rc;
    memcpy(pOut, &commit.catalogHash, sizeof(ProllyHash));
    doltliteCommitClear(&commit);
    return SQLITE_OK;
  }
  return doltliteGetHeadCatalogHash(db, pOut);
}

static int dsRequireRefs(sqlite3_vtab *pVtab, int idxNum, const char *zName){
  if( (idxNum & 3)!=3 ){
    sqlite3_free(pVtab->zErrMsg);
    pVtab->zErrMsg = sqlite3_mprintf("%s requires from_ref and to_ref", zName);
    return SQLITE_ERROR;
  }
  return SQLITE_OK;
}

static int dsComputeTableStats(
  sqlite3 *db,
  const char *zTableName,
  const ProllyHash *pFromCatHash,
  const ProllyHash *pToCatHash,
  DsStatRow *pOut
){
  struct TableEntry *aFrom = 0, *aTo = 0;
  int nFromCat = 0, nToCat = 0;
  struct TableEntry *pFromEntry, *pToEntry;
  int hasFrom = 0, hasTo = 0;
  ProllyHash fromRoot, toRoot;
  u8 fromFlags = 0, toFlags = 0;
  char **azFromCols = 0, **azToCols = 0;
  int nFromCols = 0, nToCols = 0;
  i64 oldCount = 0, newCount = 0;
  int rowsMod = 0, rowsAdd = 0, rowsDel = 0;
  i64 cellsMod = 0, cellsAdd = 0, cellsDel = 0;
  int rc;

  memset(pOut, 0, sizeof(*pOut));

  rc = doltliteLoadCatalog(db, pFromCatHash, &aFrom, &nFromCat, 0);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteLoadCatalog(db, pToCatHash, &aTo, &nToCat, 0);
  if( rc!=SQLITE_OK ){
    sqlite3_free(aFrom);
    return rc;
  }

  pFromEntry = doltliteFindTableByName(aFrom, nFromCat, zTableName);
  pToEntry   = doltliteFindTableByName(aTo,   nToCat,   zTableName);
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
  sqlite3_free(aFrom);
  sqlite3_free(aTo);

  if( !hasFrom && !hasTo ) return SQLITE_OK;


  if( hasFrom ){
    rc = dsLoadColNames(db, pFromCatHash, zTableName, &azFromCols, &nFromCols);
    if( rc!=SQLITE_OK ) return rc;
  }
  if( hasTo ){
    rc = dsLoadColNames(db, pToCatHash, zTableName, &azToCols, &nToCols);
    if( rc!=SQLITE_OK ){
      dsFreeColNames(azFromCols, nFromCols);
      return rc;
    }
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
    u8 flags = fromFlags ? fromFlags : toFlags;
    if( !cs || !pCache ){
      rc = SQLITE_ERROR;
      goto done;
    }
    rc = prollyDiffIterOpen(&iter, cs, pCache, &fromRoot, &toRoot, flags);
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
          int changed = dsCountChangedCells(
              pChange->pOldVal, pChange->nOldVal,
              pChange->pNewVal, pChange->nNewVal,
              azFromCols, nFromCols, azToCols, nToCols);
          if( changed>0 ){
            rowsMod++;
            cellsMod += changed;
          }
          break;
        }
      }
    }
    prollyDiffIterClose(&iter);
    if( rc!=SQLITE_DONE && rc!=SQLITE_ROW ) goto done;
    rc = SQLITE_OK;
  }


  /* Column-count delta: if the schema widened/narrowed, every
  ** surviving row gains or loses cells even if the data is identical.
  ** Count those as cellsAdded/cellsDeleted so dolt_diff_stat matches
  ** Dolt's reported totals. */
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

  pOut->zTableName     = sqlite3_mprintf("%s", zTableName);
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

done:
  dsFreeColNames(azFromCols, nFromCols);
  dsFreeColNames(azToCols, nToCols);
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
  DsStatRow *aRows;
  int nRows;
  int iRow;
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

static void dstFreeRows(DstCursor *c){
  int i;
  for(i=0; i<c->nRows; i++) sqlite3_free(c->aRows[i].zTableName);
  sqlite3_free(c->aRows);
  c->aRows = 0;
  c->nRows = 0;
}

static int dstConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  DstVtab *v; int rc;
  (void)pAux; (void)argc; (void)argv; (void)pzErr;
  rc = sqlite3_declare_vtab(db, dstSchema);
  if( rc!=SQLITE_OK ) return rc;
  v = sqlite3_malloc(sizeof(*v));
  if( !v ) return SQLITE_NOMEM;
  memset(v, 0, sizeof(*v));
  v->db = db;
  *ppVtab = &v->base;
  return SQLITE_OK;
}

static int dstDisconnect(sqlite3_vtab *v){ sqlite3_free(v); return SQLITE_OK; }

static int dstBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  int iFrom = -1, iTo = -1, iTbl = -1;
  int i, argvIdx = 1;
  (void)pVtab;
  for(i=0; i<pInfo->nConstraint; i++){
    if( !pInfo->aConstraint[i].usable ) continue;
    if( pInfo->aConstraint[i].op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    switch( pInfo->aConstraint[i].iColumn ){
      case DST_COL_FROM_REF: iFrom = i; break;
      case DST_COL_TO_REF:   iTo   = i; break;
      case DST_COL_TBL:      iTbl  = i; break;
    }
  }
  if( iFrom>=0 ){
    pInfo->aConstraintUsage[iFrom].argvIndex = argvIdx++;
    pInfo->aConstraintUsage[iFrom].omit = 1;
  }
  if( iTo>=0 ){
    pInfo->aConstraintUsage[iTo].argvIndex = argvIdx++;
    pInfo->aConstraintUsage[iTo].omit = 1;
  }
  if( iTbl>=0 ){
    pInfo->aConstraintUsage[iTbl].argvIndex = argvIdx++;
    pInfo->aConstraintUsage[iTbl].omit = 1;
  }
  pInfo->idxNum = (iFrom>=0 ? 1 : 0) | (iTo>=0 ? 2 : 0) | (iTbl>=0 ? 4 : 0);
  pInfo->estimatedCost = 1000.0;
  return SQLITE_OK;
}

static int dstOpen(sqlite3_vtab *v, sqlite3_vtab_cursor **pp){
  DstCursor *c; (void)v;
  c = sqlite3_malloc(sizeof(*c));
  if( !c ) return SQLITE_NOMEM;
  memset(c, 0, sizeof(*c));
  *pp = &c->base;
  return SQLITE_OK;
}

static int dstClose(sqlite3_vtab_cursor *cur){
  DstCursor *c = (DstCursor*)cur;
  dstFreeRows(c);
  sqlite3_free(c);
  return SQLITE_OK;
}

static int dstAppend(DstCursor *c, const DsStatRow *r){
  DsStatRow *aNew = sqlite3_realloc(c->aRows,
                                    (c->nRows+1)*(int)sizeof(DsStatRow));
  if( !aNew ) return SQLITE_NOMEM;
  c->aRows = aNew;
  c->aRows[c->nRows] = *r;
  c->nRows++;
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
  char **az = 0;
  int n = 0, alloc = 0;
  int rc, i, j;

  *pazOut = 0;
  *pnOut = 0;

  rc = doltliteLoadCatalog(db, pFromCat, &aFrom, &nFrom, 0);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteLoadCatalog(db, pToCat, &aTo, &nTo, 0);
  if( rc!=SQLITE_OK ){
    sqlite3_free(aFrom);
    return rc;
  }

  for(i=0; i<nFrom; i++){
    const char *zName = aFrom[i].zName;
    int dup = 0;
    if( !zName || aFrom[i].iTable==1 ) continue;
    for(j=0; j<n; j++){ if( strcmp(az[j], zName)==0 ){ dup=1; break; } }
    if( dup ) continue;
    if( n>=alloc ){
      int newAlloc = alloc ? alloc*2 : 8;
      char **aNew = sqlite3_realloc(az, newAlloc*(int)sizeof(char*));
      if( !aNew ){ rc = SQLITE_NOMEM; goto fail; }
      az = aNew;
      alloc = newAlloc;
    }
    az[n] = sqlite3_mprintf("%s", zName);
    if( !az[n] ){ rc = SQLITE_NOMEM; goto fail; }
    n++;
  }
  for(i=0; i<nTo; i++){
    const char *zName = aTo[i].zName;
    int dup = 0;
    if( !zName || aTo[i].iTable==1 ) continue;
    for(j=0; j<n; j++){ if( strcmp(az[j], zName)==0 ){ dup=1; break; } }
    if( dup ) continue;
    if( n>=alloc ){
      int newAlloc = alloc ? alloc*2 : 8;
      char **aNew = sqlite3_realloc(az, newAlloc*(int)sizeof(char*));
      if( !aNew ){ rc = SQLITE_NOMEM; goto fail; }
      az = aNew;
      alloc = newAlloc;
    }
    az[n] = sqlite3_mprintf("%s", zName);
    if( !az[n] ){ rc = SQLITE_NOMEM; goto fail; }
    n++;
  }

  sqlite3_free(aFrom);
  sqlite3_free(aTo);
  *pazOut = az;
  *pnOut = n;
  return SQLITE_OK;

fail:
  for(j=0; j<n; j++) sqlite3_free(az[j]);
  sqlite3_free(az);
  sqlite3_free(aFrom);
  sqlite3_free(aTo);
  return rc;
}

static int dstFilter(sqlite3_vtab_cursor *cur,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  DstCursor *c = (DstCursor*)cur;
  DstVtab *v = (DstVtab*)cur->pVtab;
  sqlite3 *db = v->db;
  const char *zFromRef = 0, *zToRef = 0, *zTblFilter = 0;
  ProllyHash fromCat, toCat;
  char **azNames = 0;
  int nNames = 0;
  int rc, argIdx = 0, i;
  (void)idxStr;

  dstFreeRows(c);
  c->iRow = 0;

  rc = dsRequireRefs(&v->base, idxNum, "dolt_diff_stat");
  if( rc!=SQLITE_OK ) return rc;

  if( (idxNum & 1) && argIdx<argc )
    zFromRef = (const char*)sqlite3_value_text(argv[argIdx++]);
  if( (idxNum & 2) && argIdx<argc )
    zToRef = (const char*)sqlite3_value_text(argv[argIdx++]);
  if( (idxNum & 4) && argIdx<argc )
    zTblFilter = (const char*)sqlite3_value_text(argv[argIdx++]);

  rc = dsResolveCatHash(db, zFromRef, &fromCat);
  if( rc!=SQLITE_OK ) return rc;
  rc = dsResolveCatHash(db, zToRef, &toCat);
  if( rc!=SQLITE_OK ) return rc;

  rc = dsCollectTableNames(db, &fromCat, &toCat, &azNames, &nNames);
  if( rc!=SQLITE_OK ) return rc;

  for(i=0; i<nNames; i++){
    DsStatRow row;
    if( zTblFilter && strcmp(azNames[i], zTblFilter)!=0 ) continue;
    rc = dsComputeTableStats(db, azNames[i], &fromCat, &toCat, &row);
    if( rc!=SQLITE_OK ){
      sqlite3_free(row.zTableName);
      goto done;
    }
    if( !row.zTableName ){

      continue;
    }

    if( row.rowsAdded==0 && row.rowsDeleted==0 && row.rowsModified==0
     && row.cellsAdded==0 && row.cellsDeleted==0 && row.cellsModified==0 ){
      sqlite3_free(row.zTableName);
      continue;
    }
    rc = dstAppend(c, &row);
    if( rc!=SQLITE_OK ){ sqlite3_free(row.zTableName); goto done; }
  }

done:
  for(i=0; i<nNames; i++) sqlite3_free(azNames[i]);
  sqlite3_free(azNames);
  return rc;
}

static int dstNext(sqlite3_vtab_cursor *cur){
  ((DstCursor*)cur)->iRow++;
  return SQLITE_OK;
}

static int dstEof(sqlite3_vtab_cursor *cur){
  DstCursor *c = (DstCursor*)cur;
  return c->iRow >= c->nRows;
}

static int dstColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int col){
  DstCursor *c = (DstCursor*)cur;
  DsStatRow *r;
  if( c->iRow >= c->nRows ) return SQLITE_OK;
  r = &c->aRows[c->iRow];
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
  *r = ((DstCursor*)cur)->iRow;
  return SQLITE_OK;
}

static sqlite3_module diffStatModule = {
  0, 0, dstConnect, dstBestIndex, dstDisconnect, 0,
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
  DsSummaryRow *aRows;
  int nRows;
  int iRow;
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

static void dssFreeRows(DssCursor *c){
  int i;
  for(i=0; i<c->nRows; i++){
    sqlite3_free(c->aRows[i].zFromName);
    sqlite3_free(c->aRows[i].zToName);
    sqlite3_free(c->aRows[i].zDiffType);
  }
  sqlite3_free(c->aRows);
  c->aRows = 0;
  c->nRows = 0;
}

static int dssConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  DssVtab *v; int rc;
  (void)pAux; (void)argc; (void)argv; (void)pzErr;
  rc = sqlite3_declare_vtab(db, dssSchema);
  if( rc!=SQLITE_OK ) return rc;
  v = sqlite3_malloc(sizeof(*v));
  if( !v ) return SQLITE_NOMEM;
  memset(v, 0, sizeof(*v));
  v->db = db;
  *ppVtab = &v->base;
  return SQLITE_OK;
}

static int dssDisconnect(sqlite3_vtab *v){ sqlite3_free(v); return SQLITE_OK; }

static int dssBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  int iFrom = -1, iTo = -1, iTbl = -1;
  int i, argvIdx = 1;
  (void)pVtab;
  for(i=0; i<pInfo->nConstraint; i++){
    if( !pInfo->aConstraint[i].usable ) continue;
    if( pInfo->aConstraint[i].op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    switch( pInfo->aConstraint[i].iColumn ){
      case DSS_COL_FROM_REF: iFrom = i; break;
      case DSS_COL_TO_REF:   iTo   = i; break;
      case DSS_COL_TBL:      iTbl  = i; break;
    }
  }
  if( iFrom>=0 ){
    pInfo->aConstraintUsage[iFrom].argvIndex = argvIdx++;
    pInfo->aConstraintUsage[iFrom].omit = 1;
  }
  if( iTo>=0 ){
    pInfo->aConstraintUsage[iTo].argvIndex = argvIdx++;
    pInfo->aConstraintUsage[iTo].omit = 1;
  }
  if( iTbl>=0 ){
    pInfo->aConstraintUsage[iTbl].argvIndex = argvIdx++;
    pInfo->aConstraintUsage[iTbl].omit = 1;
  }
  pInfo->idxNum = (iFrom>=0 ? 1 : 0) | (iTo>=0 ? 2 : 0) | (iTbl>=0 ? 4 : 0);
  pInfo->estimatedCost = 1000.0;
  return SQLITE_OK;
}

static int dssOpen(sqlite3_vtab *v, sqlite3_vtab_cursor **pp){
  DssCursor *c; (void)v;
  c = sqlite3_malloc(sizeof(*c));
  if( !c ) return SQLITE_NOMEM;
  memset(c, 0, sizeof(*c));
  *pp = &c->base;
  return SQLITE_OK;
}

static int dssClose(sqlite3_vtab_cursor *cur){
  DssCursor *c = (DssCursor*)cur;
  dssFreeRows(c);
  sqlite3_free(c);
  return SQLITE_OK;
}

static int dssAppend(DssCursor *c, const char *zFrom, const char *zTo,
                     const char *zDiffType, int dataChange, int schemaChange){
  DsSummaryRow *aNew = sqlite3_realloc(c->aRows,
      (c->nRows+1)*(int)sizeof(DsSummaryRow));
  if( !aNew ) return SQLITE_NOMEM;
  c->aRows = aNew;
  c->aRows[c->nRows].zFromName  = sqlite3_mprintf("%s", zFrom ? zFrom : "");
  c->aRows[c->nRows].zToName    = sqlite3_mprintf("%s", zTo   ? zTo   : "");
  c->aRows[c->nRows].zDiffType  = sqlite3_mprintf("%s", zDiffType);
  c->aRows[c->nRows].dataChange   = (u8)(dataChange ? 1 : 0);
  c->aRows[c->nRows].schemaChange = (u8)(schemaChange ? 1 : 0);
  if( !c->aRows[c->nRows].zFromName
   || !c->aRows[c->nRows].zToName
   || !c->aRows[c->nRows].zDiffType ){
    return SQLITE_NOMEM;
  }
  c->nRows++;
  return SQLITE_OK;
}

static int dssFilter(sqlite3_vtab_cursor *cur,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  DssCursor *c = (DssCursor*)cur;
  DssVtab *v = (DssVtab*)cur->pVtab;
  sqlite3 *db = v->db;
  const char *zFromRef = 0, *zToRef = 0, *zTblFilter = 0;
  ProllyHash fromCat, toCat;
  struct TableEntry *aFromCat = 0, *aToCat = 0;
  int nFromCat = 0, nToCat = 0;
  char **azNames = 0;
  int nNames = 0;
  int rc, argIdx = 0, i;
  (void)idxStr;

  dssFreeRows(c);
  c->iRow = 0;

  rc = dsRequireRefs(&v->base, idxNum, "dolt_diff_summary");
  if( rc!=SQLITE_OK ) return rc;

  if( (idxNum & 1) && argIdx<argc )
    zFromRef = (const char*)sqlite3_value_text(argv[argIdx++]);
  if( (idxNum & 2) && argIdx<argc )
    zToRef = (const char*)sqlite3_value_text(argv[argIdx++]);
  if( (idxNum & 4) && argIdx<argc )
    zTblFilter = (const char*)sqlite3_value_text(argv[argIdx++]);

  rc = dsResolveCatHash(db, zFromRef, &fromCat);
  if( rc!=SQLITE_OK ) return rc;
  rc = dsResolveCatHash(db, zToRef, &toCat);
  if( rc!=SQLITE_OK ) return rc;

  rc = dsCollectTableNames(db, &fromCat, &toCat, &azNames, &nNames);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteLoadCatalog(db, &fromCat, &aFromCat, &nFromCat, 0);
  if( rc!=SQLITE_OK ) goto done;
  rc = doltliteLoadCatalog(db, &toCat, &aToCat, &nToCat, 0);
  if( rc!=SQLITE_OK ) goto done;

  for(i=0; i<nNames; i++){
    struct TableEntry *pFromEntry, *pToEntry;
    i64 oldCount = 0, newCount = 0;
    int dataChange, schemaChange;
    const char *zDiffType;

    if( zTblFilter && strcmp(azNames[i], zTblFilter)!=0 ) continue;

    pFromEntry = doltliteFindTableByName(aFromCat, nFromCat, azNames[i]);
    pToEntry   = doltliteFindTableByName(aToCat,   nToCat,   azNames[i]);

    if( pFromEntry && pToEntry ){
      int rootsDiffer = (prollyHashCompare(&pFromEntry->root,
                                           &pToEntry->root)!=0);
      int schemasDiffer = (prollyHashCompare(&pFromEntry->schemaHash,
                                             &pToEntry->schemaHash)!=0);
      if( !rootsDiffer && !schemasDiffer ){
        continue;
      }
      dataChange = rootsDiffer;
      schemaChange = schemasDiffer;
      zDiffType = "modified";
      rc = dssAppend(c, azNames[i], azNames[i], zDiffType,
                     dataChange, schemaChange);
    }else if( !pFromEntry && pToEntry ){
      rc = dsCountRows(db, &pToEntry->root, pToEntry->flags, &newCount);
      if( rc!=SQLITE_OK ) goto done;
      dataChange = newCount > 0;
      schemaChange = 1;
      rc = dssAppend(c, "", azNames[i], "added",
                     dataChange, schemaChange);
    }else if( pFromEntry && !pToEntry ){
      rc = dsCountRows(db, &pFromEntry->root, pFromEntry->flags, &oldCount);
      if( rc!=SQLITE_OK ) goto done;
      dataChange = oldCount > 0;
      schemaChange = 1;
      rc = dssAppend(c, azNames[i], "", "dropped",
                     dataChange, schemaChange);
    }

    if( rc!=SQLITE_OK ) goto done;
  }

done:
  sqlite3_free(aFromCat);
  sqlite3_free(aToCat);
  for(i=0; i<nNames; i++) sqlite3_free(azNames[i]);
  sqlite3_free(azNames);
  return rc;
}

static int dssNext(sqlite3_vtab_cursor *cur){
  ((DssCursor*)cur)->iRow++;
  return SQLITE_OK;
}

static int dssEof(sqlite3_vtab_cursor *cur){
  DssCursor *c = (DssCursor*)cur;
  return c->iRow >= c->nRows;
}

static int dssColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int col){
  DssCursor *c = (DssCursor*)cur;
  DsSummaryRow *r;
  if( c->iRow >= c->nRows ) return SQLITE_OK;
  r = &c->aRows[c->iRow];
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
  *r = ((DssCursor*)cur)->iRow;
  return SQLITE_OK;
}

static sqlite3_module diffSummaryModule = {
  0, 0, dssConnect, dssBestIndex, dssDisconnect, 0,
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
