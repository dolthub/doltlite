#ifdef DOLTLITE_PROLLY

#include "doltlite_merge_constraints_int.h"
#include "doltlite_merge_int.h"
#include "vdbeInt.h"

/* DoltliteColInfo omits VIRTUAL columns, so aColToRec[i] is not declared
** column i. Match the stored column by name. */
static int partialNamedField(const DoltliteColInfo *pCols, const char *zName){
  int j;
  if( !pCols || !pCols->azName || !zName ) return -1;
  for(j=0; j<pCols->nCol; j++){
    if( pCols->azName[j] && sqlite3_stricmp(pCols->azName[j], zName)==0 ){
      return pCols->aColToRec ? pCols->aColToRec[j] : j;
    }
  }
  return -1;
}

static int partialStoredSlot(
  const Table *pTab,
  const DoltliteColInfo *pCols,
  int iCol
){
  int j, nBefore = 0;
  int iField = partialNamedField(pCols, pTab->aCol[iCol].zCnName);
  if( iField>=0 ) return iField;
  if( !pCols || pCols->bHasRowid ){
    for(j=0; j<iCol; j++){
      if( !doltliteColumnIsVirtual(pTab, j) ) nBefore++;
    }
    return nBefore;
  }
  for(j=0; j<iCol; j++){
    if( doltliteColumnIsVirtual(pTab, j) ) continue;
    if( pTab->aCol[j].colFlags & COLFLAG_PRIMKEY ) continue;
    nBefore++;
  }
  return pCols->nPk + nBefore;
}

/* Record every member of a colliding group, including pre-merge
** rows. Dolt records both sides; the other detectors' skip halved this.
** pPk non-NULL selects the WITHOUT ROWID fetch; rowid is then ignored. */
static int appendUniqueViolation(
  sqlite3 *db,
  const char *zTable,
  const char *zIndexName,
  const char *zCols,
  sqlite3_int64 rowid,
  const MergePkInfo *pPk,
  const u8 *pPkRec, int nPkRec,
  int *pAppended
){
  u8 *pKey = 0;
  int nKey = 0;
  u8 *pVal = 0;
  int nVal = 0;
  char *zInfo = 0;
  int rc;

  if( pAppended ) *pAppended = 0;
  if( pPk ){
    rc = fetchRowByPkFromTable(db, zTable, pPkRec, nPkRec, pPk->nPk,
                               &pKey, &nKey, &pVal, &nVal);
  }else{
    rc = fetchOrphanRow(db, zTable, rowid, &pKey, &nKey, &pVal, &nVal);
  }
  if( rc==SQLITE_NOTFOUND ) return SQLITE_OK;
  if( rc!=SQLITE_OK ) return rc;

  {
    sqlite3_str *pInfo = sqlite3_str_new(db);
    sqlite3_str_appendall(pInfo, "{\"Columns\": [");
    sqlite3_str_appendall(pInfo, zCols);
    sqlite3_str_appendall(pInfo, "], \"Name\": ");
    doltliteMergeAppendJsonString(pInfo, zIndexName ? zIndexName : "");
    sqlite3_str_appendchar(pInfo, 1, '}');
    zInfo = sqlite3_str_finish(pInfo);
  }
  if( !zInfo ){
    sqlite3_free(pKey);
    sqlite3_free(pVal);
    return SQLITE_NOMEM;
  }
  rc = doltliteAppendConstraintViolation(
      db, zTable, DOLTLITE_CV_UNIQUE_INDEX,
      pPk ? (sqlite3_int64)0 : rowid, pKey, nKey, pVal, nVal, zInfo);
  sqlite3_free(zInfo);
  sqlite3_free(pKey);
  sqlite3_free(pVal);
  if( rc==SQLITE_OK && pAppended ) *pAppended = 1;
  return rc;
}

static int uniqueIndexRecordHasNull(UnpackedRecord *pRecord, int nKeyCol){
  int i;
  for(i=0; i<nKeyCol; i++){
    if( sqlite3_value_type((sqlite3_value*)&pRecord->aMem[i])==SQLITE_NULL ){
      return 1;
    }
  }
  return 0;
}

static int uniqueNextToken(const char **pz, int *pType){
  int n;
  while( **pz ){
    n = sqlite3GetToken((const u8*)*pz, pType);
    if( n<=0 || *pType==TK_ILLEGAL ) return -1;
    if( *pType!=TK_SPACE && *pType!=TK_COMMENT ) return n;
    *pz += n;
  }
  return 0;
}

static char *uniqueIndexWhereFromSql(const char *zSql, int *pRc){
  const char *p = zSql;
  int depth = 0;
  int seenOn = 0;
  int type, n;
  char *zWhere;

  *pRc = SQLITE_OK;
  if( !zSql ){
    *pRc = SQLITE_CORRUPT;
    return 0;
  }
  while( (n = uniqueNextToken(&p, &type))>0 ){
    if( type==TK_LP ){
      depth++;
    }else if( type==TK_RP ){
      if( depth>0 ) depth--;
    }else if( depth==0 && type==TK_ON ){
      seenOn = 1;
    }else if( depth==0 && seenOn && type==TK_WHERE ){
      p += n;
      if( uniqueNextToken(&p, &type)<=0 ){
        *pRc = SQLITE_CORRUPT;
        return 0;
      }
      zWhere = sqlite3_mprintf("%s", p);
      if( !zWhere ) *pRc = SQLITE_NOMEM;
      return zWhere;
    }
    p += n;
  }
  *pRc = SQLITE_CORRUPT;
  return 0;
}

int doltlitePartialIndexWhereSql(sqlite3 *db, Index *pIdx, char **pzWhere){
  sqlite3_stmt *pStmt = 0;
  int rc;
  int stepRc;

  *pzWhere = 0;
  if( !pIdx || !pIdx->pPartIdxWhere ) return SQLITE_OK;
  if( !pIdx->zName ) return SQLITE_CORRUPT;
  rc = sqlite3_prepare_v2(db,
      "SELECT sql FROM main.sqlite_master WHERE type='index' AND name=?1",
      -1, &pStmt, 0);
  if( rc!=SQLITE_OK ) return rc;
  rc = sqlite3_bind_text(pStmt, 1, pIdx->zName, -1, SQLITE_STATIC);
  if( rc==SQLITE_OK ){
    stepRc = sqlite3_step(pStmt);
    if( stepRc==SQLITE_ROW ){
      *pzWhere = uniqueIndexWhereFromSql(
          (const char*)sqlite3_column_text(pStmt, 0), &rc);
    }else{
      rc = stepRc==SQLITE_DONE ? SQLITE_CORRUPT : stepRc;
    }
  }
  return finishConstraintStmt(pStmt, rc);
}

int doltlitePartialIndexMatchesRecord(
  sqlite3 *db,
  Index *pIdx,
  const char *zWhere,
  const u8 *pRec, int nRec,
  i64 intKey,
  const DoltliteColInfo *pCols,
  sqlite3_stmt **ppCached,
  int *pMatch
){
  Table *pTab;
  sqlite3_stmt *pStmt = 0;
  DoltliteRecordInfo info = {0};
  int i, iParam, rc;

  *pMatch = 0;
  if( !zWhere || !zWhere[0] ){
    *pMatch = 1;
    return SQLITE_OK;
  }
  if( !pIdx || !pIdx->pTable || !pRec || nRec<=0 ) return SQLITE_CORRUPT;
  pTab = pIdx->pTable;
  rc = doltliteParseRecordStrict(pRec, nRec, &info);
  if( rc!=SQLITE_OK ) return rc;
  if( ppCached && *ppCached ){
    pStmt = *ppCached;
    sqlite3_reset(pStmt);
    sqlite3_clear_bindings(pStmt);
  }else{
    rc = doltlitePrepareIndexExpr(db, pTab, pIdx->pPartIdxWhere, -1, &pStmt);
    if( rc!=SQLITE_OK ){
      doltliteRecordInfoClear(&info);
      return rc;
    }
    if( ppCached ) *ppCached = pStmt;
  }
  for(i=0, iParam=1; i<pTab->nCol && rc==SQLITE_OK; i++){
    int iField;
    DoltliteSerialValue v;
    if( doltliteColumnIsVirtual(pTab, i) ) continue;
    if( HasRowid(pTab) && i==pTab->iPKey ){
      rc = sqlite3_bind_int64(pStmt, iParam, intKey);
      iParam++;
      continue;
    }
    iField = partialStoredSlot(pTab, pCols, i);
    if( iField<0 || iField>=info.nField ){
      rc = doltliteBindColumnDefault(db, pStmt, iParam, pTab, i);
      iParam++;
      continue;
    }
    rc = doltliteSerialValueFromField(pRec, nRec, &info, iField, &v);
    if( rc!=SQLITE_OK ) break;
    if( v.eType==SQLITE_NULL ){
      rc = sqlite3_bind_null(pStmt, iParam);
    }else if( v.eType==SQLITE_INTEGER ){
      rc = sqlite3_bind_int64(pStmt, iParam, v.i);
    }else if( v.eType==SQLITE_FLOAT ){
      rc = sqlite3_bind_double(pStmt, iParam, v.r);
    }else if( v.eType==SQLITE_TEXT ){
      rc = sqlite3_bind_text(pStmt, iParam, (const char*)v.p, v.n, SQLITE_TRANSIENT);
    }else{
      rc = sqlite3_bind_blob(pStmt, iParam, v.p, v.n, SQLITE_TRANSIENT);
    }
    iParam++;
  }
  if( rc==SQLITE_OK && HasRowid(pTab) ){
    rc = sqlite3_bind_int64(pStmt, pTab->nNVCol+1, intKey);
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3_step(pStmt);
    if( rc==SQLITE_ROW ){
      *pMatch = sqlite3VdbeBooleanValue(sqlite3_column_value(pStmt, 0), 0);
      rc = SQLITE_OK;
    }else if( rc==SQLITE_DONE ){
      rc = SQLITE_OK;
    }
  }
  if( ppCached ){
    /* The caller owns the statement; leave it prepared for the next row. */
    sqlite3_reset(pStmt);
    doltliteRecordInfoClear(&info);
    return rc;
  }
  doltliteRecordInfoClear(&info);
  return finishConstraintStmt(pStmt, rc);
}

static KeyInfo *uniqueIndexKeyInfo(sqlite3 *db, Index *pIdx, int *pRc){
  Parse sParse;
  KeyInfo *pKeyInfo;

  sqlite3ParseObjectInit(&sParse, db);
  pKeyInfo = sqlite3KeyInfoOfIndex(&sParse, pIdx);
  *pRc = sParse.rc;
  if( !pKeyInfo && *pRc==SQLITE_OK ) *pRc = SQLITE_NOMEM;
  sqlite3ParseObjectReset(&sParse);
  return pKeyInfo;
}

typedef struct UniqueIndexEntry UniqueIndexEntry;
struct UniqueIndexEntry {
  u8 *pKey;
  int nKey;
  u8 *pPk;
  int nPk;
  sqlite3_int64 rowid;
  UnpackedRecord *pUnpacked;
};

#ifndef SQLITE_OMIT_GENERATED_COLUMNS
static int uniqueBindStoredColumn(
  sqlite3 *db,
  sqlite3_stmt *pStmt,
  int iParam,
  const u8 *pRecord,
  int nRecord,
  const DoltliteRecordInfo *pInfo,
  Table *pTab,
  int iColumn
){
  DoltliteSerialValue v;
  int iField;
  int rc;
  iField = mergeConstraintStoredField(pTab, iColumn);
  if( iField<0 || iField>=pInfo->nField ){
    return doltliteBindColumnDefault(db, pStmt, iParam, pTab, iColumn);
  }
  rc = doltliteSerialValueFromField(pRecord, nRecord, pInfo, iField, &v);
  if( rc!=SQLITE_OK ) return rc;
  if( v.eType==SQLITE_NULL ) return sqlite3_bind_null(pStmt, iParam);
  if( v.eType==SQLITE_INTEGER ) return sqlite3_bind_int64(pStmt, iParam, v.i);
  if( v.eType==SQLITE_FLOAT ) return sqlite3_bind_double(pStmt, iParam, v.r);
  if( v.eType==SQLITE_TEXT ){
    return sqlite3_bind_text(pStmt, iParam, (const char*)v.p, v.n,
                             SQLITE_TRANSIENT);
  }
  return sqlite3_bind_blob(pStmt, iParam, v.p, v.n, SQLITE_TRANSIENT);
}

/* A VIRTUAL column is not a field of the WITHOUT ROWID record. Evaluate its
** generation expression via SQLite's code generator against the stored
** columns of the record. */
static int uniqueEvalVirtualColumn(
  sqlite3 *db,
  Table *pTab,
  int iCol,
  const u8 *pRecord,
  int nRecord,
  const DoltliteRecordInfo *pInfo,
  DoltliteSerialValue *pOut,
  u8 **ppOwned
){
  sqlite3_stmt *pStmt = 0;
  Expr *pExpr;
  sqlite3_value *pVal;
  int i, iParam, eType, n, rc;

  *ppOwned = 0;
  memset(pOut, 0, sizeof(*pOut));
  if( !db || !pTab || iCol<0 || iCol>=pTab->nCol ) return SQLITE_CORRUPT;
  pExpr = sqlite3ColumnExpr(pTab, &pTab->aCol[iCol]);
  if( !pExpr ) return SQLITE_CORRUPT;
  rc = doltlitePrepareIndexExpr(db, pTab, pExpr, iCol, &pStmt);
  if( rc!=SQLITE_OK ) return rc;
  for(i=0, iParam=1; i<pTab->nCol && rc==SQLITE_OK; i++){
    if( doltliteColumnIsVirtual(pTab, i) ) continue;
    rc = uniqueBindStoredColumn(
        db, pStmt, iParam++, pRecord, nRecord, pInfo, pTab, i);
  }
  if( rc==SQLITE_OK ) rc = sqlite3_step(pStmt);
  if( rc!=SQLITE_ROW ){
    sqlite3_finalize(pStmt);
    return rc==SQLITE_DONE ? SQLITE_ERROR : rc;
  }
  pVal = sqlite3_column_value(pStmt, 0);
  eType = sqlite3_value_type(pVal);
  if( eType==SQLITE_INTEGER ){
    pOut->eType = SQLITE_INTEGER;
    pOut->i = sqlite3_value_int64(pVal);
  }else if( eType==SQLITE_FLOAT ){
    pOut->eType = SQLITE_FLOAT;
    pOut->r = sqlite3_value_double(pVal);
  }else if( eType==SQLITE_TEXT || eType==SQLITE_BLOB ){
    n = sqlite3_value_bytes(pVal);
    *ppOwned = sqlite3_malloc(n ? n : 1);
    if( !*ppOwned ){
      sqlite3_finalize(pStmt);
      return SQLITE_NOMEM;
    }
    if( n>0 ){
      memcpy(*ppOwned, eType==SQLITE_TEXT
             ? (const void*)sqlite3_value_text(pVal)
             : sqlite3_value_blob(pVal), (size_t)n);
    }
    pOut->eType = eType;
    pOut->p = *ppOwned;
    pOut->n = n;
  }else{
    pOut->eType = SQLITE_NULL;
  }
  sqlite3_finalize(pStmt);
  return SQLITE_OK;
}
#endif

static int uniqueRecordFromTableRow(
  sqlite3 *db,
  const u8 *pRecord,
  int nRecord,
  const DoltliteRecordInfo *pInfo,
  Index *pIdx,
  int nField,
  u8 **ppOut,
  int *pnOut,
  int *pHasNull
){
  Table *pTab = pIdx->pTable;
  DoltliteSerialValue *aValue;
  u8 **apOwned = 0;
  int i;
  int rc = SQLITE_OK;

  *ppOut = 0;
  *pnOut = 0;
  if( pHasNull ) *pHasNull = 0;
  aValue = sqlite3_malloc64(
      (sqlite3_int64)nField * sizeof(DoltliteSerialValue));
  apOwned = sqlite3_malloc64((sqlite3_int64)nField * sizeof(u8*));
  if( !aValue || !apOwned ){
    sqlite3_free(aValue);
    sqlite3_free(apOwned);
    return SQLITE_NOMEM;
  }
  memset(aValue, 0, (size_t)nField * sizeof(DoltliteSerialValue));
  memset(apOwned, 0, (size_t)nField * sizeof(u8*));
  for(i=0; i<nField; i++){
    int iColumn = pIdx->aiColumn[i];
    int iRecord;
    if( iColumn<0 || iColumn>=pTab->nCol ){
      rc = SQLITE_CORRUPT;
      break;
    }
#ifndef SQLITE_OMIT_GENERATED_COLUMNS
    if( (pTab->aCol[iColumn].colFlags & COLFLAG_VIRTUAL)!=0 ){
      rc = uniqueEvalVirtualColumn(
          db, pTab, iColumn, pRecord, nRecord, pInfo, &aValue[i], &apOwned[i]);
    }else
#endif
    {
      iRecord = mergeConstraintStoredField(pTab, iColumn);
      if( iRecord>=pInfo->nField ){
        rc = doltliteColumnDefaultSerialValue(
            db, pTab, iColumn, &aValue[i], &apOwned[i]);
      }else{
        rc = doltliteSerialValueFromField(
            pRecord, nRecord, pInfo, iRecord, &aValue[i]);
      }
    }
    if( rc!=SQLITE_OK ) break;
    if( pHasNull && aValue[i].eType==SQLITE_NULL ) *pHasNull = 1;
  }
  if( rc==SQLITE_OK ){
    *ppOut = doltliteBuildRecord(aValue, nField, pnOut);
    if( !*ppOut ) rc = SQLITE_NOMEM;
  }
  for(i=0; i<nField; i++) sqlite3_free(apOwned[i]);
  sqlite3_free(apOwned);
  sqlite3_free(aValue);
  return rc;
}

static void uniqueEntryClear(sqlite3 *db, UniqueIndexEntry *pEntry){
  sqlite3_free(pEntry->pKey);
  sqlite3_free(pEntry->pPk);
  sqlite3DbFree(db, pEntry->pUnpacked);
  memset(pEntry, 0, sizeof(*pEntry));
}

static void uniqueIndexEntriesFree(
  sqlite3 *db,
  UniqueIndexEntry *aEntry,
  int nEntry
){
  int i;
  for(i=0; i<nEntry; i++) uniqueEntryClear(db, &aEntry[i]);
  sqlite3_free(aEntry);
}

static int uniqueIndexEntryCompare(
  UniqueIndexEntry *pLeft,
  UniqueIndexEntry *pRight,
  int *pCmp
){
  pRight->pUnpacked->errCode = 0;
  *pCmp = sqlite3VdbeRecordCompare(
      pLeft->nKey, pLeft->pKey, pRight->pUnpacked);
  return pRight->pUnpacked->errCode;
}

static int uniqueIndexEntriesSort(
  UniqueIndexEntry *aEntry,
  int nEntry
){
  UniqueIndexEntry *aTmp;
  UniqueIndexEntry *aSrc = aEntry;
  UniqueIndexEntry *aDst;
  int width;
  int rc = SQLITE_OK;

  if( nEntry<2 ) return SQLITE_OK;
  aTmp = sqlite3_malloc64(
      (sqlite3_int64)nEntry * sizeof(UniqueIndexEntry));
  if( !aTmp ) return SQLITE_NOMEM;
  aDst = aTmp;
  for(width=1; width<nEntry && rc==SQLITE_OK; width*=2){
    int left;
    for(left=0; left<nEntry; left+=width*2){
      int mid = left + width;
      int right = mid + width;
      int i = left;
      int j;
      int out = left;
      if( mid>nEntry ) mid = nEntry;
      if( right>nEntry ) right = nEntry;
      j = mid;
      while( i<mid && j<right ){
        int cmp;
        rc = uniqueIndexEntryCompare(&aSrc[i], &aSrc[j], &cmp);
        if( rc!=SQLITE_OK ) break;
        aDst[out++] = cmp<=0 ? aSrc[i++] : aSrc[j++];
      }
      while( rc==SQLITE_OK && i<mid ) aDst[out++] = aSrc[i++];
      while( rc==SQLITE_OK && j<right ) aDst[out++] = aSrc[j++];
      if( rc!=SQLITE_OK ) break;
    }
    if( rc==SQLITE_OK ){
      UniqueIndexEntry *aSwap = aSrc;
      aSrc = aDst;
      aDst = aSwap;
    }
    if( width>nEntry/2 ) break;
  }
  if( rc==SQLITE_OK && aSrc!=aEntry ){
    memcpy(aEntry, aSrc, (size_t)nEntry * sizeof(UniqueIndexEntry));
  }
  sqlite3_free(aTmp);
  return rc;
}

static int uniqueEntryUnpack(
  sqlite3 *db,
  KeyInfo *pKeyInfo,
  Index *pIdx,
  UniqueIndexEntry *pEntry
){
  pEntry->pUnpacked = sqlite3VdbeAllocUnpackedRecord(pKeyInfo);
  if( !pEntry->pUnpacked ) return SQLITE_NOMEM;
  memset(pEntry->pUnpacked->aMem, 0,
         sizeof(Mem) * (size_t)(pKeyInfo->nKeyField + 1));
  sqlite3VdbeRecordUnpack(pEntry->nKey, pEntry->pKey, pEntry->pUnpacked);
  if( pEntry->pUnpacked->nField < pIdx->nKeyCol ){
    sqlite3DbFree(db, pEntry->pUnpacked);
    pEntry->pUnpacked = 0;
    return SQLITE_CORRUPT;
  }
  pEntry->pUnpacked->nField = pIdx->nKeyCol;
  return SQLITE_OK;
}

static int uniqueEntryPush(
  UniqueIndexEntry **paEntry,
  int *pnEntry,
  int *pnAlloc,
  UniqueIndexEntry *pEntry
){
  if( *pnEntry==*pnAlloc ){
    int nNew = *pnAlloc ? *pnAlloc*2 : 64;
    UniqueIndexEntry *aNew;
    if( nNew<*pnAlloc || nNew>0x7fffffff/(int)sizeof(UniqueIndexEntry) ){
      return SQLITE_TOOBIG;
    }
    aNew = sqlite3_realloc64(
        *paEntry, (sqlite3_int64)nNew * sizeof(UniqueIndexEntry));
    if( !aNew ) return SQLITE_NOMEM;
    *paEntry = aNew;
    *pnAlloc = nNew;
  }
  (*paEntry)[(*pnEntry)++] = *pEntry;
  return SQLITE_OK;
}

static int uniqueReportCollisions(
  sqlite3 *db,
  const char *zTable,
  Index *pIdx,
  const char *zCols,
  UniqueIndexEntry *aEntry,
  int nEntry,
  const MergePkInfo *pPk,
  int *pnFound
){
  int winnerHandled = 0;
  int rc;
  int i;

  rc = uniqueIndexEntriesSort(aEntry, nEntry);
  if( rc!=SQLITE_OK ) return rc;
  for(i=1; i<nEntry && rc==SQLITE_OK; i++){
    int cmp;
    rc = uniqueIndexEntryCompare(&aEntry[i-1], &aEntry[i], &cmp);
    if( rc==SQLITE_OK && cmp==0 ){
      int appended = 0;
      if( !winnerHandled ){
        rc = appendUniqueViolation(
            db, zTable, pIdx->zName, zCols,
            aEntry[i-1].rowid, pPk, aEntry[i-1].pPk, aEntry[i-1].nPk,
            &appended);
        if( rc!=SQLITE_OK ) break;
        if( appended && pnFound ) (*pnFound)++;
        winnerHandled = 1;
      }
      appended = 0;
      rc = appendUniqueViolation(
          db, zTable, pIdx->zName, zCols,
          aEntry[i].rowid, pPk, aEntry[i].pPk, aEntry[i].nPk,
          &appended);
      if( rc!=SQLITE_OK ) break;
      if( appended && pnFound ) (*pnFound)++;
    }else{
      winnerHandled = 0;
    }
  }
  return rc;
}

static int detectUniqueViolationsForIndex(
  sqlite3 *db,
  const char *zTable,
  Index *pIdx,
  const char *zSelect,
  const char *zJson,
  int *pnFound,
  char **pzErrMsg
){
  sqlite3_stmt *pScan = 0;
  KeyInfo *pKeyInfo = 0;
  UniqueIndexEntry *aEntry = 0;
  char *zQuery = 0;
  int nEntry = 0;
  int nAlloc = 0;
  int rc;
  char *zRowid = 0;

  rc = loadMergeRowidSql(db, zTable, &zRowid, pzErrMsg);
  if( rc!=SQLITE_OK ) goto unique_done;
  pKeyInfo = uniqueIndexKeyInfo(db, pIdx, &rc);
  if( !pKeyInfo ) goto unique_done;
  {
    char *zWhere = 0;
    rc = doltlitePartialIndexWhereSql(db, pIdx, &zWhere);
    if( rc!=SQLITE_OK ) goto unique_done;
    if( zWhere ){
      zQuery = sqlite3_mprintf(
          "SELECT %s, %s FROM main.\"%w\" NOT INDEXED WHERE (%s\n)",
          zRowid, zSelect, zTable, zWhere);
      sqlite3_free(zWhere);
    }else{
      zQuery = sqlite3_mprintf(
          "SELECT %s, %s FROM main.\"%w\" NOT INDEXED", zRowid, zSelect, zTable);
    }
  }
  if( !zQuery ){ rc = SQLITE_NOMEM; goto unique_done; }
  rc = sqlite3_prepare_v2(db, zQuery, -1, &pScan, 0);
  if( rc!=SQLITE_OK ) goto unique_done;

  while( (rc = sqlite3_step(pScan))==SQLITE_ROW ){
    UniqueIndexEntry entry;

    memset(&entry, 0, sizeof(entry));
    entry.rowid = sqlite3_column_int64(pScan, 0);
    entry.pKey = buildRecordFromStmtCols(
        pScan, 1, pIdx->nKeyCol, &entry.nKey);
    if( !entry.pKey ){ rc = SQLITE_NOMEM; break; }
    rc = uniqueEntryUnpack(db, pKeyInfo, pIdx, &entry);
    if( rc!=SQLITE_OK ){
      uniqueEntryClear(db, &entry);
      break;
    }
    if( uniqueIndexRecordHasNull(entry.pUnpacked, pIdx->nKeyCol) ){
      uniqueEntryClear(db, &entry);
      continue;
    }
    rc = uniqueEntryPush(&aEntry, &nEntry, &nAlloc, &entry);
    if( rc!=SQLITE_OK ){
      uniqueEntryClear(db, &entry);
      break;
    }
  }
  if( rc==SQLITE_DONE ) rc = SQLITE_OK;
  if( rc==SQLITE_OK ){
    rc = uniqueReportCollisions(
        db, zTable, pIdx, zJson, aEntry, nEntry, 0, pnFound);
  }

unique_done:
  rc = finishConstraintStmt(pScan, rc);
  sqlite3_free(zRowid);
  sqlite3_free(zQuery);
  uniqueIndexEntriesFree(db, aEntry, nEntry);
  sqlite3KeyInfoUnref(pKeyInfo);
  return rc;
}

static int detectUniqueViolationsForIndexWithoutRowid(
  sqlite3 *db,
  struct TableEntry *pCurrent,
  const char *zTable,
  Index *pIdx,
  const char *zCols,
  const MergePkInfo *pPk,
  int *pnFound
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  Index *pPkIdx = sqlite3PrimaryKeyIndex(pIdx->pTable);
  KeyInfo *pKeyInfo = 0;
  DoltliteColInfo cols;
  ProllyCursor cursor;
  UniqueIndexEntry *aEntry = 0;
  int nEntry = 0;
  int nAlloc = 0;
  int cursorOpen = 0;
  char *zPartWhere = 0;
  int rc;
  int res = 0;

  memset(&cols, 0, sizeof(cols));
  if( !cs || !pCache || !pCurrent || !pPkIdx ) return SQLITE_ERROR;
  rc = doltliteGetColumnNames(db, zTable, &cols);
  if( rc!=SQLITE_OK ) goto without_rowid_done;
  pKeyInfo = uniqueIndexKeyInfo(db, pIdx, &rc);
  if( !pKeyInfo ) goto without_rowid_done;
  if( prollyHashIsEmpty(&pCurrent->root) ) goto without_rowid_done;
  rc = doltlitePartialIndexWhereSql(db, pIdx, &zPartWhere);
  if( rc!=SQLITE_OK ) goto without_rowid_done;

  prollyCursorInit(
      &cursor, cs, pCache, &pCurrent->root, pCurrent->flags);
  cursorOpen = 1;
  rc = prollyCursorFirst(&cursor, &res);
  while( rc==SQLITE_OK && res==0 && prollyCursorIsValid(&cursor) ){
    const u8 *pKey = 0;
    const u8 *pValue = 0;
    const u8 *pRecord;
    u8 *pOwnedRecord = 0;
    int nKey = 0;
    int nValue = 0;
    int nRecord;
    DoltliteRecordInfo info = {0};
    UniqueIndexEntry entry;
    int hasNull = 0;
    int partialMatch = 1;

    memset(&entry, 0, sizeof(entry));
    prollyCursorKey(&cursor, &pKey, &nKey);
    prollyCursorValue(&cursor, &pValue, &nValue);
    pRecord = pValue;
    nRecord = nValue;
    if( nRecord<=0 ){
      rc = doltliteRecordFromClusteredKey(
          db, zTable, pKey, nKey, &pOwnedRecord, &nRecord);
      if( rc!=SQLITE_OK ) break;
      pRecord = pOwnedRecord;
    }
    rc = doltliteParseRecordStrict(pRecord, nRecord, &info);
    if( rc==SQLITE_OK && zPartWhere ){
      rc = doltlitePartialIndexMatchesRecord(db, pIdx, zPartWhere,
                                      pRecord, nRecord, 0, &cols, 0,
                                      &partialMatch);
    }
    if( rc==SQLITE_OK && !partialMatch ){
      sqlite3_free(pOwnedRecord);
      doltliteRecordInfoClear(&info);
      rc = prollyCursorNext(&cursor);
      continue;
    }
    if( rc==SQLITE_OK ){
      rc = uniqueRecordFromTableRow(
          db, pRecord, nRecord, &info, pIdx, pIdx->nKeyCol,
          &entry.pKey, &entry.nKey, &hasNull);
    }
    if( rc==SQLITE_OK && !hasNull ){
      rc = uniqueRecordFromTableRow(
          db, pRecord, nRecord, &info, pPkIdx, pPkIdx->nKeyCol,
          &entry.pPk, &entry.nPk, 0);
    }
    sqlite3_free(pOwnedRecord);
    if( rc!=SQLITE_OK ){
      uniqueEntryClear(db, &entry);
      break;
    }
    if( !hasNull ){
      rc = uniqueEntryUnpack(db, pKeyInfo, pIdx, &entry);
      if( rc==SQLITE_OK ){
        rc = uniqueEntryPush(&aEntry, &nEntry, &nAlloc, &entry);
      }
      if( rc!=SQLITE_OK ){
        uniqueEntryClear(db, &entry);
        break;
      }
    }else{
      uniqueEntryClear(db, &entry);
    }
    doltliteRecordInfoClear(&info);
    rc = prollyCursorNext(&cursor);
  }
  if( rc==SQLITE_DONE ) rc = SQLITE_OK;
  if( rc==SQLITE_OK ){
    rc = uniqueReportCollisions(
        db, zTable, pIdx, zCols, aEntry, nEntry, pPk, pnFound);
  }

without_rowid_done:
  sqlite3_free(zPartWhere);
  if( cursorOpen ) prollyCursorClose(&cursor);
  uniqueIndexEntriesFree(db, aEntry, nEntry);
  doltliteFreeColInfo(&cols);
  sqlite3KeyInfoUnref(pKeyInfo);
  return rc;
}

typedef struct UniqueTokSpan UniqueTokSpan;
struct UniqueTokSpan {
  const char *z;
  int n;
  int type;
};

static int uniqueTokIsKw(const UniqueTokSpan *pTok, const char *zKw){
  int k = (int)strlen(zKw);
  return pTok->n==k && sqlite3_strnicmp(pTok->z, zKw, k)==0
      && pTok->z[0]!='"' && pTok->z[0]!='`' && pTok->z[0]!='[';
}

static void uniqueStripSort(char *z){
  UniqueTokSpan aLast[4];
  const char *p = z;
  const char *zFirst = 0;
  int nLast = 0, type, nTok;
  while( (nTok = uniqueNextToken(&p, &type))>0 ){
    if( !zFirst ) zFirst = p;
    if( nLast==4 ){
      memmove(aLast, aLast+1, 3*sizeof(UniqueTokSpan));
      nLast = 3;
    }
    aLast[nLast].z = p;
    aLast[nLast].n = nTok;
    aLast[nLast].type = type;
    nLast++;
    p += nTok;
  }
  if( nTok<0 || nLast==0 ){
    z[0] = 0;
    return;
  }
  if( nLast>=2 && uniqueTokIsKw(&aLast[nLast-2], "NULLS")
   && (uniqueTokIsKw(&aLast[nLast-1], "FIRST")
    || uniqueTokIsKw(&aLast[nLast-1], "LAST")) ){
    nLast -= 2;
  }
  if( nLast>=1 && (aLast[nLast-1].type==TK_ASC || aLast[nLast-1].type==TK_DESC) ){
    nLast--;
  }
  if( nLast==0 ){
    z[0] = 0;
    return;
  }
  {
    int nOut = (int)((aLast[nLast-1].z - zFirst) + aLast[nLast-1].n);
    if( zFirst!=z ) memmove(z, zFirst, (size_t)nOut);
    z[nOut] = 0;
  }
}

static char *uniqueDupRange(const char *zStart, const char *zEnd, int *pRc){
  int n = (int)(zEnd - zStart);
  char *z;
  if( n<0 ){ *pRc = SQLITE_CORRUPT; return 0; }
  z = sqlite3_malloc(n+1);
  if( !z ){ *pRc = SQLITE_NOMEM; return 0; }
  memcpy(z, zStart, (size_t)n);
  z[n] = 0;
  uniqueStripSort(z);
  if( !z[0] ){ sqlite3_free(z); *pRc = SQLITE_CORRUPT; return 0; }
  return z;
}

/* Key expressions as written in CREATE INDEX, one string per key column. */
static int uniqueIndexKeyExprs(
  sqlite3 *db, Index *pIdx, char ***pazExpr
){
  sqlite3_stmt *pStmt = 0;
  const char *zSql = 0;
  char *zOwned = 0;
  const char *p, *zItem;
  char **az = 0;
  int n = 0;
  int depth = 0;
  int type, nTok = 0;
  int rc;
  int stepRc;

  *pazExpr = 0;
  rc = sqlite3_prepare_v2(db,
      "SELECT sql FROM main.sqlite_master WHERE type='index' AND name=?1",
      -1, &pStmt, 0);
  if( rc!=SQLITE_OK ) return rc;
  rc = sqlite3_bind_text(pStmt, 1, pIdx->zName, -1, SQLITE_STATIC);
  if( rc==SQLITE_OK ){
    stepRc = sqlite3_step(pStmt);
    if( stepRc==SQLITE_ROW ){
      zSql = (const char*)sqlite3_column_text(pStmt, 0);
      if( zSql ) zOwned = sqlite3_mprintf("%s", zSql);
      if( !zOwned ) rc = SQLITE_NOMEM;
    }else{
      rc = stepRc==SQLITE_DONE ? SQLITE_CORRUPT : stepRc;
    }
  }
  rc = finishConstraintStmt(pStmt, rc);
  if( rc!=SQLITE_OK ){ sqlite3_free(zOwned); return rc; }

  p = zOwned;
  while( (nTok = uniqueNextToken(&p, &type))>0 ){
    if( type==TK_LP ){
      depth++;
    }else if( type==TK_RP ){
      if( depth>0 ) depth--;
    }else if( depth==0 && type==TK_ON ){
      p += nTok;
      break;
    }
    p += nTok;
  }
  if( nTok<=0 ){ rc = SQLITE_CORRUPT; goto expr_done; }
  nTok = uniqueNextToken(&p, &type);
  if( nTok<=0 || type==TK_LP || type==TK_RP || type==TK_DOT ){
    rc = SQLITE_CORRUPT;
    goto expr_done;
  }
  p += nTok;
  nTok = uniqueNextToken(&p, &type);
  if( nTok>0 && type==TK_DOT ){
    p += nTok;
    nTok = uniqueNextToken(&p, &type);
    if( nTok<=0 || type==TK_LP || type==TK_RP || type==TK_DOT ){
      rc = SQLITE_CORRUPT;
      goto expr_done;
    }
    p += nTok;
    nTok = uniqueNextToken(&p, &type);
  }
  if( nTok<=0 || type!=TK_LP ){ rc = SQLITE_CORRUPT; goto expr_done; }
  p += nTok;
  zItem = p;
  depth = 1;
  az = sqlite3_malloc64((sqlite3_int64)pIdx->nKeyCol * sizeof(char*));
  if( !az ){ rc = SQLITE_NOMEM; goto expr_done; }
  memset(az, 0, (size_t)pIdx->nKeyCol * sizeof(char*));
  while( depth>0 && rc==SQLITE_OK ){
    nTok = uniqueNextToken(&p, &type);
    if( nTok<=0 ){ rc = SQLITE_CORRUPT; break; }
    if( type==TK_LP ){
      depth++;
    }else if( depth==1 && (type==TK_COMMA || type==TK_RP) ){
      if( n>=pIdx->nKeyCol ){ rc = SQLITE_CORRUPT; break; }
      az[n] = uniqueDupRange(zItem, p, &rc);
      if( rc!=SQLITE_OK ) break;
      n++;
      if( type==TK_RP ){ depth = 0; p += nTok; break; }
      p += nTok;
      zItem = p;
      continue;
    }else if( type==TK_RP ){
      depth--;
    }
    p += nTok;
  }
  if( rc==SQLITE_OK && (n!=pIdx->nKeyCol || depth!=0) ) rc = SQLITE_CORRUPT;

expr_done:
  if( rc==SQLITE_OK ){
    *pazExpr = az;
  }else if( az ){
    int i;
    for(i=0; i<n; i++) sqlite3_free(az[i]);
    sqlite3_free(az);
  }
  sqlite3_free(zOwned);
  return rc;
}

/* Select-list text and the JSON label list for an expression index. */
static int uniqueIndexExprLists(
  sqlite3 *db, Index *pIdx, char **pzSelect, char **pzJson
){
  char **az = 0;
  sqlite3_str *pSelect;
  sqlite3_str *pJson;
  int i, rc;

  *pzSelect = 0;
  *pzJson = 0;
  rc = uniqueIndexKeyExprs(db, pIdx, &az);
  if( rc!=SQLITE_OK ) return rc;
  pSelect = sqlite3_str_new(0);
  pJson = sqlite3_str_new(0);
  for(i=0; i<pIdx->nKeyCol; i++){
    if( i>0 ){
      sqlite3_str_appendall(pSelect, ", ");
      sqlite3_str_appendall(pJson, ", ");
    }
    /* CASE keeps the value but does not match the index expression, so
    ** the probe is computed from the row. A wrong index entry must not
    ** hide a collision or invent one. */
    sqlite3_str_appendf(pSelect, "(CASE WHEN 1 THEN %s\nEND)", az[i]);
    doltliteMergeAppendJsonString(pJson, az[i]);
    sqlite3_free(az[i]);
  }
  sqlite3_free(az);
  *pzSelect = sqlite3_str_finish(pSelect);
  *pzJson = sqlite3_str_finish(pJson);
  if( !*pzSelect || !*pzJson ){
    sqlite3_free(*pzSelect);
    sqlite3_free(*pzJson);
    *pzSelect = 0;
    *pzJson = 0;
    return SQLITE_NOMEM;
  }
  return SQLITE_OK;
}

static int detectUniqueExprViolationsWithoutRowid(
  sqlite3 *db,
  const char *zTable,
  Index *pIdx,
  const char *zSelect,
  const char *zJson,
  const MergePkInfo *pPk,
  int *pnFound
){
  sqlite3_stmt *pScan = 0;
  KeyInfo *pKeyInfo = 0;
  UniqueIndexEntry *aEntry = 0;
  char *zQuery = 0;
  char *zWhere = 0;
  int nEntry = 0;
  int nAlloc = 0;
  int rc;

  if( !pPk || pPk->nPk<=0 || !pPk->zPkCols ) return SQLITE_CORRUPT;
  pKeyInfo = uniqueIndexKeyInfo(db, pIdx, &rc);
  if( !pKeyInfo ) goto expr_wo_done;
  rc = doltlitePartialIndexWhereSql(db, pIdx, &zWhere);
  if( rc!=SQLITE_OK ) goto expr_wo_done;
  if( zWhere ){
    zQuery = sqlite3_mprintf(
        "SELECT %s, %s FROM main.\"%w\" NOT INDEXED WHERE (%s\n)",
        pPk->zPkCols, zSelect, zTable, zWhere);
  }else{
    zQuery = sqlite3_mprintf(
        "SELECT %s, %s FROM main.\"%w\" NOT INDEXED",
        pPk->zPkCols, zSelect, zTable);
  }
  if( !zQuery ){ rc = SQLITE_NOMEM; goto expr_wo_done; }
  rc = sqlite3_prepare_v2(db, zQuery, -1, &pScan, 0);
  if( rc!=SQLITE_OK ) goto expr_wo_done;

  while( (rc = sqlite3_step(pScan))==SQLITE_ROW ){
    UniqueIndexEntry entry;
    memset(&entry, 0, sizeof(entry));
    entry.pPk = buildRecordFromStmtCols(pScan, 0, pPk->nPk, &entry.nPk);
    entry.pKey = buildRecordFromStmtCols(
        pScan, pPk->nPk, pIdx->nKeyCol, &entry.nKey);
    if( !entry.pPk || !entry.pKey ){
      uniqueEntryClear(db, &entry);
      rc = SQLITE_NOMEM;
      break;
    }
    rc = uniqueEntryUnpack(db, pKeyInfo, pIdx, &entry);
    if( rc!=SQLITE_OK ){
      uniqueEntryClear(db, &entry);
      break;
    }
    if( uniqueIndexRecordHasNull(entry.pUnpacked, pIdx->nKeyCol) ){
      uniqueEntryClear(db, &entry);
      continue;
    }
    rc = uniqueEntryPush(&aEntry, &nEntry, &nAlloc, &entry);
    if( rc!=SQLITE_OK ){
      uniqueEntryClear(db, &entry);
      break;
    }
  }
  if( rc==SQLITE_DONE ) rc = SQLITE_OK;
  if( rc==SQLITE_OK ){
    rc = uniqueReportCollisions(
        db, zTable, pIdx, zJson, aEntry, nEntry, pPk, pnFound);
  }

expr_wo_done:
  rc = finishConstraintStmt(pScan, rc);
  sqlite3_free(zWhere);
  sqlite3_free(zQuery);
  uniqueIndexEntriesFree(db, aEntry, nEntry);
  sqlite3KeyInfoUnref(pKeyInfo);
  return rc;
}

static int uniqueWalkTable(
  sqlite3 *db,
  const char *zTable,
  const char *zSql,
  struct TableEntry *aAnc, int nAnc,
  struct TableEntry *aCur, int nCur,
  void *pCtx
){
  MergeConstraintWalk *pWalk = (MergeConstraintWalk*)pCtx;
  sqlite3_stmt *pIdxList = 0;
  char *zIdxQ;
  int hasRowid = 1;
  MergePkInfo pkInfo;
  int indexStepRc;
  int rc;
  int *pnFound = pWalk->pnFound;
  (void)zSql; (void)aAnc; (void)nAnc;

  memset(&pkInfo, 0, sizeof(pkInfo));
  rc = tableHasRowid(db, zTable, &hasRowid);
  if( rc!=SQLITE_OK ) return rc;
  if( !hasRowid ){
    rc = loadMergePkInfo(db, zTable, &pkInfo);
    if( rc != SQLITE_OK ) return rc;
  }

  zIdxQ = sqlite3_mprintf("PRAGMA main.index_list(%Q)", zTable);
  if( !zIdxQ ){
    freeMergePkInfo(&pkInfo);
    return SQLITE_NOMEM;
  }
  rc = sqlite3_prepare_v2(db, zIdxQ, -1, &pIdxList, 0);
  sqlite3_free(zIdxQ);
  if( rc != SQLITE_OK ){
    freeMergePkInfo(&pkInfo);
    return rc;
  }

  while( (indexStepRc = sqlite3_step(pIdxList)) == SQLITE_ROW ){
    int unique = sqlite3_column_int(pIdxList, 2);
    const char *zIdxRaw;
    const char *zOrigin;
    char *zIdx;
    Index *pIdx;
    sqlite3_str *pColList;
    char *zColList;
    int i;
    int supported = 1;

    if( !unique ) continue;
    zIdxRaw = (const char*)sqlite3_column_text(pIdxList, 1);
    if( !zIdxRaw ) continue;

    zOrigin = (const char*)sqlite3_column_text(pIdxList, 3);
    /* A clustered key is the table tree; a rowid table's key (one named
    ** rowid cannot be clustered) is a separate index that can collide. */
    if( zOrigin && strcmp(zOrigin, "pk")==0 && !hasRowid ) continue;

    zIdx = sqlite3_mprintf("%s", zIdxRaw);
    if( !zIdx ){
      rc = SQLITE_NOMEM;
      break;
    }
    pIdx = sqlite3FindIndex(db, zIdx, "main");
    if( !pIdx ){
      sqlite3_free(zIdx);
      rc = SQLITE_CORRUPT;
      break;
    }

    pColList = sqlite3_str_new(0);
    {
      sqlite3_str *pJsonCols = sqlite3_str_new(0);
      int hasExpr = 0;
      char *zSelect = 0;
      char *zJson = 0;
      char *zJsonCols = 0;
      for(i=0; i<pIdx->nKeyCol; i++){
        int cno = pIdx->aiColumn[i];
        if( cno==XN_EXPR ){
          hasExpr = 1;
          continue;
        }
        if( cno<0 || cno>=pIdx->pTable->nCol ){
          supported = 0;
          break;
        }
        if( sqlite3_str_length(pColList)>0 ){
          sqlite3_str_appendall(pColList, ", ");
          sqlite3_str_appendall(pJsonCols, ", ");
        }
        sqlite3_str_appendf(
            pColList, "\"%w\"", pIdx->pTable->aCol[cno].zCnName);
        doltliteMergeAppendJsonString(
            pJsonCols, pIdx->pTable->aCol[cno].zCnName);
      }
      {
        int strErr = sqlite3_str_errcode(pColList);
        int jsonErr = sqlite3_str_errcode(pJsonCols);
        zColList = sqlite3_str_finish(pColList);
        zJsonCols = sqlite3_str_finish(pJsonCols);
        if( strErr ) rc = strErr;
        else if( jsonErr ) rc = jsonErr;
        else if( !zColList || !zJsonCols ){
          if( !zColList ) zColList = sqlite3_mprintf("");
          if( !zJsonCols ) zJsonCols = sqlite3_mprintf("");
          if( !zColList || !zJsonCols ) rc = SQLITE_NOMEM;
        }
      }
      if( rc==SQLITE_OK && supported && hasExpr ){
        rc = uniqueIndexExprLists(db, pIdx, &zSelect, &zJson);
      }else if( supported && zColList && *zColList ){
        zSelect = zColList;
        zJson = zJsonCols;
      }
      if( rc==SQLITE_OK && supported && zSelect && *zSelect ){
        if( hasRowid ){
          rc = detectUniqueViolationsForIndex(
              db, zTable, pIdx, zSelect, zJson, pnFound, pWalk->pzErrMsg);
        }else if( hasExpr ){
          rc = detectUniqueExprViolationsWithoutRowid(
              db, zTable, pIdx, zSelect, zJson, &pkInfo, pnFound);
        }else{
          rc = detectUniqueViolationsForIndexWithoutRowid(
              db, doltliteFindTableByName(aCur, nCur, zTable),
              zTable, pIdx, zJson, &pkInfo, pnFound);
        }
      }
      if( zSelect!=zColList ) sqlite3_free(zSelect);
      if( zJson!=zJsonCols ) sqlite3_free(zJson);
      sqlite3_free(zColList);
      sqlite3_free(zJsonCols);
    }
    sqlite3_free(zIdx);
    if( rc != SQLITE_OK ) break;
  }

  if( rc==SQLITE_OK && indexStepRc!=SQLITE_DONE ) rc = indexStepRc;
  rc = finishConstraintStmt(pIdxList, rc);
  freeMergePkInfo(&pkInfo);
  return rc;
}

int doltliteDetectMergeUniqueViolations(
  sqlite3 *db,
  const ProllyHash *pAncCatHash,
  char **pzErrMsg,
  int *pnFound,
  const char **azTables,
  int nTables
){
  MergeConstraintWalk walk;
  if( pnFound ) *pnFound = 0;
  walk.pnFound = pnFound;
  walk.pzErrMsg = pzErrMsg;
  return walkMergeUserTables(db, pAncCatHash, pzErrMsg, azTables, nTables,
                             1, 0, uniqueWalkTable, &walk);
}


#endif
