#ifdef DOLTLITE_PROLLY

#include "doltlite_merge_constraints_int.h"
#include "vdbeInt.h"

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

  zInfo = sqlite3_mprintf(
      "{\"Columns\": [%s], \"Name\": \"%w\"}",
      zCols, zIndexName);
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

static int uniqueIsIdent(char c){
  return sqlite3Isalnum(c) || c=='_';
}

static char *uniqueIndexWhereFromSql(const char *zSql, int *pRc){
  const char *p = zSql;
  int depth = 0;
  int seenOn = 0;
  int quote = 0;
  char *zWhere;

  *pRc = SQLITE_OK;
  if( !zSql ){
    *pRc = SQLITE_CORRUPT;
    return 0;
  }
  while( *p ){
    if( quote ){
      if( *p==quote ){
        if( p[1]==quote ){ p += 2; continue; }
        quote = 0;
      }
      p++;
      continue;
    }
    if( *p=='\'' || *p=='"' || *p=='`' ){ quote = *p++; continue; }
    if( *p=='[' ){ quote = ']'; p++; continue; }
    if( *p=='(' ){ depth++; p++; continue; }
    if( *p==')' ){
      if( depth>0 ) depth--;
      p++;
      continue;
    }
    if( depth==0 && (p==zSql || !uniqueIsIdent(p[-1]))
     && (p[0]=='O' || p[0]=='o') && (p[1]=='N' || p[1]=='n')
     && !uniqueIsIdent(p[2]) ){
      seenOn = 1;
      p += 2;
      continue;
    }
    if( depth==0 && seenOn
     && sqlite3_strnicmp(p, "WHERE", 5)==0 && !uniqueIsIdent(p[5]) ){
      p += 5;
      while( *p==' ' || *p=='\t' || *p=='\n' || *p=='\r' ) p++;
      if( !*p ){
        *pRc = SQLITE_CORRUPT;
        return 0;
      }
      zWhere = sqlite3_mprintf("%s", p);
      if( !zWhere ) *pRc = SQLITE_NOMEM;
      return zWhere;
    }
    p++;
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
  const DoltliteColInfo *pCols,
  sqlite3_stmt **ppCached,
  int *pMatch
){
  Table *pTab;
  sqlite3_stmt *pStmt = 0;
  DoltliteRecordInfo info;
  int i, rc;

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
    sqlite3_str *pSql = sqlite3_str_new(0);
    char *zSql;
    sqlite3_str_appendall(pSql, "SELECT 1 FROM (SELECT ");
    for(i=0; i<pTab->nCol; i++){
      if( i ) sqlite3_str_appendall(pSql, ", ");
      sqlite3_str_appendf(pSql, "?%d AS \"%w\"", i+1, pTab->aCol[i].zCnName);
    }
    sqlite3_str_appendf(pSql, ") WHERE (%s)", zWhere);
    zSql = sqlite3_str_finish(pSql);
    if( !zSql ) return SQLITE_NOMEM;
    rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
    sqlite3_free(zSql);
    if( rc!=SQLITE_OK ) return rc;
    if( ppCached ) *ppCached = pStmt;
  }
  for(i=0; i<pTab->nCol && rc==SQLITE_OK; i++){
    int iField = (pCols && i<pCols->nCol) ? pCols->aColToRec[i] : i;
    DoltliteSerialValue v;
    if( iField<0 || iField>=info.nField ){
      rc = sqlite3_bind_null(pStmt, i+1);
      continue;
    }
    rc = doltliteSerialValueFromField(pRec, nRec, &info, iField, &v);
    if( rc!=SQLITE_OK ) break;
    if( v.eType==SQLITE_NULL ){
      rc = sqlite3_bind_null(pStmt, i+1);
    }else if( v.eType==SQLITE_INTEGER ){
      rc = sqlite3_bind_int64(pStmt, i+1, v.i);
    }else if( v.eType==SQLITE_FLOAT ){
      rc = sqlite3_bind_double(pStmt, i+1, v.r);
    }else if( v.eType==SQLITE_TEXT ){
      rc = sqlite3_bind_text(pStmt, i+1, (const char*)v.p, v.n, SQLITE_TRANSIENT);
    }else{
      rc = sqlite3_bind_blob(pStmt, i+1, v.p, v.n, SQLITE_TRANSIENT);
    }
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3_step(pStmt);
    if( rc==SQLITE_ROW ){
      *pMatch = 1;
      rc = SQLITE_OK;
    }else if( rc==SQLITE_DONE ){
      rc = SQLITE_OK;
    }
  }
  if( ppCached ){
    /* The caller owns the statement; leave it prepared for the next row. */
    sqlite3_reset(pStmt);
    return rc;
  }
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

/* Record slots come from the index's own table, never from column info read
** back over the connection: mid-merge that read still returns the pre-merge
** catalog, so a column the merged schema adds looks absent and an index over
** it cannot be mapped. */
static int uniqueRecordFromTableRow(
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
  int i;
  int rc = SQLITE_OK;

  *ppOut = 0;
  *pnOut = 0;
  if( pHasNull ) *pHasNull = 0;
  aValue = sqlite3_malloc64(
      (sqlite3_int64)nField * sizeof(DoltliteSerialValue));
  if( !aValue ) return SQLITE_NOMEM;
  memset(aValue, 0, (size_t)nField * sizeof(DoltliteSerialValue));
  for(i=0; i<nField; i++){
    int iColumn = pIdx->aiColumn[i];
    int iRecord;
    if( iColumn<0 || iColumn>=pTab->nCol ){
      rc = SQLITE_CORRUPT;
      break;
    }
    iRecord = HasRowid(pTab)
        ? sqlite3TableColumnToStorage(pTab, iColumn)
        : sqlite3TableColumnToIndex(sqlite3PrimaryKeyIndex(pTab), iColumn);
    rc = doltliteSerialValueFromField(
        pRecord, nRecord, pInfo, iRecord, &aValue[i]);
    if( rc!=SQLITE_OK ) break;
    if( pHasNull && aValue[i].eType==SQLITE_NULL ) *pHasNull = 1;
  }
  if( rc==SQLITE_OK ){
    *ppOut = doltliteBuildRecord(aValue, nField, pnOut);
    if( !*ppOut ) rc = SQLITE_NOMEM;
  }
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
  const char *zCols,
  int *pnFound
){
  sqlite3_stmt *pScan = 0;
  KeyInfo *pKeyInfo = 0;
  UniqueIndexEntry *aEntry = 0;
  char *zQuery = 0;
  int nEntry = 0;
  int nAlloc = 0;
  int rc;

  pKeyInfo = uniqueIndexKeyInfo(db, pIdx, &rc);
  if( !pKeyInfo ) goto unique_done;
  {
    char *zWhere = 0;
    rc = doltlitePartialIndexWhereSql(db, pIdx, &zWhere);
    if( rc!=SQLITE_OK ) goto unique_done;
    if( zWhere ){
      zQuery = sqlite3_mprintf(
          "SELECT rowid, %s FROM main.\"%w\" NOT INDEXED WHERE (%s)",
          zCols, zTable, zWhere);
      sqlite3_free(zWhere);
    }else{
      zQuery = sqlite3_mprintf(
          "SELECT rowid, %s FROM main.\"%w\" NOT INDEXED", zCols, zTable);
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
        db, zTable, pIdx, zCols, aEntry, nEntry, 0, pnFound);
  }

unique_done:
  rc = finishConstraintStmt(pScan, rc);
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
    DoltliteRecordInfo info;
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
                                      pRecord, nRecord, &cols, 0,
                                      &partialMatch);
    }
    if( rc==SQLITE_OK && !partialMatch ){
      sqlite3_free(pOwnedRecord);
      rc = prollyCursorNext(&cursor);
      continue;
    }
    if( rc==SQLITE_OK ){
      rc = uniqueRecordFromTableRow(
          pRecord, nRecord, &info, pIdx, pIdx->nKeyCol,
          &entry.pKey, &entry.nKey, &hasNull);
    }
    if( rc==SQLITE_OK && !hasNull ){
      rc = uniqueRecordFromTableRow(
          pRecord, nRecord, &info, pPkIdx, pPkIdx->nKeyCol,
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

static int uniqueWalkTable(
  sqlite3 *db,
  const char *zTable,
  const char *zSql,
  struct TableEntry *aAnc, int nAnc,
  struct TableEntry *aCur, int nCur,
  void *pCtx
){
  sqlite3_stmt *pIdxList = 0;
  char *zIdxQ;
  int hasRowid = 1;
  MergePkInfo pkInfo;
  int indexStepRc;
  int rc;
  int *pnFound = (int*)pCtx;
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
    if( zOrigin && strcmp(zOrigin, "pk")==0 ) continue;

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
    for(i=0; i<pIdx->nKeyCol; i++){
      int cno = pIdx->aiColumn[i];
      if( i>0 ) sqlite3_str_appendall(pColList, ", ");
      if( cno>=0 && cno<pIdx->pTable->nCol ){
        sqlite3_str_appendf(
            pColList, "\"%w\"", pIdx->pTable->aCol[cno].zCnName);
      }else{
        supported = 0;
        sqlite3_str_appendall(pColList, "null");
      }
    }
    zColList = sqlite3_str_finish(pColList);
    if( !zColList ) rc = SQLITE_NOMEM;
    if( supported && zColList && *zColList ){
      if( hasRowid ){
        rc = detectUniqueViolationsForIndex(
            db, zTable, pIdx, zColList, pnFound);
      }else{
        rc = detectUniqueViolationsForIndexWithoutRowid(
            db, doltliteFindTableByName(aCur, nCur, zTable),
            zTable, pIdx, zColList, &pkInfo, pnFound);
      }
    }
    sqlite3_free(zColList);
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
  if( pnFound ) *pnFound = 0;
  return walkMergeUserTables(db, pAncCatHash, pzErrMsg, azTables, nTables,
                             1, 0, uniqueWalkTable, pnFound);
}


#endif
