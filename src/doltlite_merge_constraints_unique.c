#ifdef DOLTLITE_PROLLY

#include "doltlite_merge_constraints_int.h"
#include "vdbeInt.h"

static int appendUniqueViolationByRowid(
  sqlite3 *db,
  struct TableEntry *aAnc, int nAnc,
  const char *zTable,
  const char *zIndexName,
  const char *zCols,
  sqlite3_int64 rowid,
  int *pAppended
){
  u8 *pKey = 0;
  int nKey = 0;
  u8 *pVal = 0;
  int nVal = 0;
  char *zInfo = 0;
  int rc;

  if( pAppended ) *pAppended = 0;
  rc = fetchOrphanRow(db, zTable, rowid, &pKey, &nKey, &pVal, &nVal);
  if( rc==SQLITE_NOTFOUND ) return SQLITE_OK;
  if( rc!=SQLITE_OK ) return rc;

  if( aAnc ){
    u8 *pAncVal = 0;
    int nAncVal = 0;
    int ancRc = fetchAncestorRowByName(db, aAnc, nAnc, zTable,
                                       rowid, &pAncVal, &nAncVal);
    int preExisting = (ancRc==SQLITE_OK)
        && isRowPreExisting(pVal, nVal, pAncVal, nAncVal);
    sqlite3_free(pAncVal);
    if( preExisting ){
      sqlite3_free(pKey);
      sqlite3_free(pVal);
      return SQLITE_OK;
    }
  }

  zInfo = sqlite3_mprintf(
      "{\"Columns\": [%s], \"Name\": \"%w\"}",
      zCols, zIndexName);
  rc = doltliteAppendConstraintViolation(
      db, zTable, DOLTLITE_CV_UNIQUE_INDEX,
      rowid, pKey, nKey, pVal, nVal, zInfo);
  sqlite3_free(zInfo);
  sqlite3_free(pKey);
  sqlite3_free(pVal);
  if( rc==SQLITE_OK && pAppended ) *pAppended = 1;
  return rc;
}

static int appendUniqueViolationByPk(
  sqlite3 *db,
  struct TableEntry *aAnc, int nAnc,
  const char *zTable,
  const char *zIndexName,
  const char *zCols,
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
  rc = fetchRowByPkFromTable(db, zTable, pPkRec, nPkRec, pPk->nPk,
                             &pKey, &nKey, &pVal, &nVal);
  if( rc==SQLITE_NOTFOUND ) return SQLITE_OK;
  if( rc!=SQLITE_OK ) return rc;

  if( aAnc ){
    u8 *pAncVal = 0;
    int nAncVal = 0;
    int ancRc = fetchAncestorRowByKey(db, aAnc, nAnc, zTable,
                                      pKey, nKey, &pAncVal, &nAncVal);
    int preExisting = (ancRc==SQLITE_OK)
        && isRowPreExisting(pVal, nVal, pAncVal, nAncVal);
    sqlite3_free(pAncVal);
    if( preExisting ){
      sqlite3_free(pKey);
      sqlite3_free(pVal);
      return SQLITE_OK;
    }
  }

  zInfo = sqlite3_mprintf(
      "{\"Columns\": [%s], \"Name\": \"%w\"}",
      zCols, zIndexName);
  rc = doltliteAppendConstraintViolation(
      db, zTable, DOLTLITE_CV_UNIQUE_INDEX,
      0, pKey, nKey, pVal, nVal, zInfo);
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

static int uniqueValueFromRecord(
  const u8 *pRecord,
  int nRecord,
  const DoltliteRecordInfo *pInfo,
  int iField,
  DoltliteSerialValue *pValue
){
  int st;
  int off;
  int n;

  memset(pValue, 0, sizeof(*pValue));
  if( iField<0 || iField>=pInfo->nField ) return SQLITE_CORRUPT;
  st = pInfo->aType[iField];
  off = pInfo->aOffset[iField];
  n = dlSerialTypeLen((u64)st);
  if( n<0 || off<0 || off>nRecord-n ) return SQLITE_CORRUPT;
  if( st==0 ){
    pValue->eType = SQLITE_NULL;
  }else if( st==8 || st==9 || (st>=1 && st<=6) ){
    pValue->eType = SQLITE_INTEGER;
    pValue->i = st==8 ? 0 : st==9 ? 1 : dlReadIntBytes(pRecord + off, n);
  }else if( st==7 ){
    u64 bits = 0;
    int i;
    pValue->eType = SQLITE_FLOAT;
    for(i=0; i<8; i++) bits = (bits<<8) | pRecord[off+i];
    memcpy(&pValue->r, &bits, 8);
  }else if( st>=13 && (st&1)==1 ){
    pValue->eType = SQLITE_TEXT;
    pValue->p = pRecord + off;
    pValue->n = n;
  }else if( st>=12 && (st&1)==0 ){
    pValue->eType = SQLITE_BLOB;
    pValue->p = pRecord + off;
    pValue->n = n;
  }else{
    return SQLITE_CORRUPT;
  }
  return SQLITE_OK;
}

static int uniqueRecordFromTableRow(
  const u8 *pRecord,
  int nRecord,
  const DoltliteRecordInfo *pInfo,
  const DoltliteColInfo *pCols,
  Index *pIdx,
  int nField,
  u8 **ppOut,
  int *pnOut,
  int *pHasNull
){
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
    if( iColumn<0 || iColumn>=pCols->nCol ){
      rc = SQLITE_NOTFOUND;
      break;
    }
    iRecord = pCols->aColToRec[iColumn];
    rc = uniqueValueFromRecord(
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

static void uniqueIndexEntriesFree(
  sqlite3 *db,
  UniqueIndexEntry *aEntry,
  int nEntry
){
  int i;
  for(i=0; i<nEntry; i++){
    sqlite3_free(aEntry[i].pKey);
    sqlite3_free(aEntry[i].pPk);
    sqlite3DbFree(db, aEntry[i].pUnpacked);
  }
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

static int detectUniqueViolationsForIndex(
  sqlite3 *db,
  struct TableEntry *aAnc, int nAnc,
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
  int winnerHandled = 0;
  int rc;
  int i;

  pKeyInfo = uniqueIndexKeyInfo(db, pIdx, &rc);
  if( !pKeyInfo ) goto unique_done;
  zQuery = sqlite3_mprintf(
      "SELECT rowid, %s FROM main.\"%w\" NOT INDEXED", zCols, zTable);
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
    entry.pUnpacked = sqlite3VdbeAllocUnpackedRecord(pKeyInfo);
    if( !entry.pUnpacked ){
      sqlite3_free(entry.pKey);
      rc = SQLITE_NOMEM;
      break;
    }
    memset(entry.pUnpacked->aMem, 0,
           sizeof(Mem) * (size_t)(pKeyInfo->nKeyField + 1));
    sqlite3VdbeRecordUnpack(entry.nKey, entry.pKey, entry.pUnpacked);
    if( entry.pUnpacked->nField < pIdx->nKeyCol ){
      sqlite3_free(entry.pKey);
      sqlite3DbFree(db, entry.pUnpacked);
      rc = SQLITE_CORRUPT;
      break;
    }
    entry.pUnpacked->nField = pIdx->nKeyCol;
    if( uniqueIndexRecordHasNull(entry.pUnpacked, pIdx->nKeyCol) ){
      sqlite3_free(entry.pKey);
      sqlite3DbFree(db, entry.pUnpacked);
      continue;
    }
    if( nEntry==nAlloc ){
      int nNew = nAlloc ? nAlloc*2 : 64;
      UniqueIndexEntry *aNew;
      if( nNew<nAlloc || nNew>0x7fffffff/(int)sizeof(UniqueIndexEntry) ){
        sqlite3_free(entry.pKey);
        sqlite3DbFree(db, entry.pUnpacked);
        rc = SQLITE_TOOBIG;
        break;
      }
      aNew = sqlite3_realloc64(
          aEntry, (sqlite3_int64)nNew * sizeof(UniqueIndexEntry));
      if( !aNew ){
        sqlite3_free(entry.pKey);
        sqlite3DbFree(db, entry.pUnpacked);
        rc = SQLITE_NOMEM;
        break;
      }
      aEntry = aNew;
      nAlloc = nNew;
    }
    aEntry[nEntry++] = entry;
  }
  if( rc==SQLITE_DONE ) rc = SQLITE_OK;
  if( rc!=SQLITE_OK ) goto unique_done;
  rc = uniqueIndexEntriesSort(aEntry, nEntry);
  if( rc!=SQLITE_OK ) goto unique_done;

  for(i=1; i<nEntry && rc==SQLITE_OK; i++){
    int cmp;
    rc = uniqueIndexEntryCompare(&aEntry[i-1], &aEntry[i], &cmp);
    if( rc==SQLITE_OK && cmp==0 ){
      int appended = 0;
      if( !winnerHandled ){
        rc = appendUniqueViolationByRowid(
            db, aAnc, nAnc, zTable, pIdx->zName, zCols,
            aEntry[i-1].rowid, &appended);
        if( rc!=SQLITE_OK ) break;
        if( appended && pnFound ) (*pnFound)++;
        winnerHandled = 1;
      }
      appended = 0;
      rc = appendUniqueViolationByRowid(
          db, aAnc, nAnc, zTable, pIdx->zName, zCols,
          aEntry[i].rowid, &appended);
      if( rc!=SQLITE_OK ) break;
      if( appended && pnFound ) (*pnFound)++;
    }else{
      winnerHandled = 0;
    }
  }

unique_done:
  sqlite3_finalize(pScan);
  sqlite3_free(zQuery);
  uniqueIndexEntriesFree(db, aEntry, nEntry);
  sqlite3KeyInfoUnref(pKeyInfo);
  return rc;
}

static int detectUniqueViolationsForIndexWithoutRowid(
  sqlite3 *db,
  struct TableEntry *aAnc, int nAnc,
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
  int winnerHandled = 0;
  int rc;
  int res = 0;
  int i;

  memset(&cols, 0, sizeof(cols));
  if( !cs || !pCache || !pCurrent || !pPkIdx ) return SQLITE_ERROR;
  rc = doltliteGetColumnNames(db, zTable, &cols);
  if( rc!=SQLITE_OK ) goto without_rowid_done;
  pKeyInfo = uniqueIndexKeyInfo(db, pIdx, &rc);
  if( !pKeyInfo ) goto without_rowid_done;
  if( prollyHashIsEmpty(&pCurrent->root) ) goto without_rowid_done;

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
    if( rc==SQLITE_OK ){
      rc = uniqueRecordFromTableRow(
          pRecord, nRecord, &info, &cols, pIdx, pIdx->nKeyCol,
          &entry.pKey, &entry.nKey, &hasNull);
    }
    if( rc==SQLITE_OK && !hasNull ){
      rc = uniqueRecordFromTableRow(
          pRecord, nRecord, &info, &cols, pPkIdx, pPkIdx->nKeyCol,
          &entry.pPk, &entry.nPk, 0);
    }
    sqlite3_free(pOwnedRecord);
    if( rc!=SQLITE_OK ){
      sqlite3_free(entry.pKey);
      sqlite3_free(entry.pPk);
      break;
    }
    if( !hasNull ){
      entry.pUnpacked = sqlite3VdbeAllocUnpackedRecord(pKeyInfo);
      if( !entry.pUnpacked ){
        sqlite3_free(entry.pKey);
        sqlite3_free(entry.pPk);
        rc = SQLITE_NOMEM;
        break;
      }
      memset(entry.pUnpacked->aMem, 0,
             sizeof(Mem) * (size_t)(pKeyInfo->nKeyField + 1));
      sqlite3VdbeRecordUnpack(entry.nKey, entry.pKey, entry.pUnpacked);
      if( entry.pUnpacked->nField<pIdx->nKeyCol ){
        sqlite3_free(entry.pKey);
        sqlite3_free(entry.pPk);
        sqlite3DbFree(db, entry.pUnpacked);
        rc = SQLITE_CORRUPT;
        break;
      }
      entry.pUnpacked->nField = pIdx->nKeyCol;
      if( nEntry==nAlloc ){
        int nNew = nAlloc ? nAlloc*2 : 64;
        UniqueIndexEntry *aNew;
        if( nNew<nAlloc || nNew>0x7fffffff/(int)sizeof(UniqueIndexEntry) ){
          sqlite3_free(entry.pKey);
          sqlite3_free(entry.pPk);
          sqlite3DbFree(db, entry.pUnpacked);
          rc = SQLITE_TOOBIG;
          break;
        }
        aNew = sqlite3_realloc64(
            aEntry, (sqlite3_int64)nNew * sizeof(UniqueIndexEntry));
        if( !aNew ){
          sqlite3_free(entry.pKey);
          sqlite3_free(entry.pPk);
          sqlite3DbFree(db, entry.pUnpacked);
          rc = SQLITE_NOMEM;
          break;
        }
        aEntry = aNew;
        nAlloc = nNew;
      }
      aEntry[nEntry++] = entry;
    }else{
      sqlite3_free(entry.pKey);
    }
    rc = prollyCursorNext(&cursor);
  }
  if( rc==SQLITE_DONE ) rc = SQLITE_OK;
  if( rc!=SQLITE_OK ) goto without_rowid_done;
  rc = uniqueIndexEntriesSort(aEntry, nEntry);
  if( rc!=SQLITE_OK ) goto without_rowid_done;

  for(i=1; i<nEntry && rc==SQLITE_OK; i++){
    int cmp;
    rc = uniqueIndexEntryCompare(&aEntry[i-1], &aEntry[i], &cmp);
    if( rc==SQLITE_OK && cmp==0 ){
      int appended = 0;
      if( !winnerHandled ){
        rc = appendUniqueViolationByPk(
            db, aAnc, nAnc, zTable, pIdx->zName, zCols, pPk,
            aEntry[i-1].pPk, aEntry[i-1].nPk, &appended);
        if( rc!=SQLITE_OK ) break;
        if( appended && pnFound ) (*pnFound)++;
        winnerHandled = 1;
      }
      appended = 0;
      rc = appendUniqueViolationByPk(
          db, aAnc, nAnc, zTable, pIdx->zName, zCols, pPk,
          aEntry[i].pPk, aEntry[i].nPk, &appended);
      if( rc!=SQLITE_OK ) break;
      if( appended && pnFound ) (*pnFound)++;
    }else{
      winnerHandled = 0;
    }
  }

without_rowid_done:
  if( cursorOpen ) prollyCursorClose(&cursor);
  uniqueIndexEntriesFree(db, aEntry, nEntry);
  doltliteFreeColInfo(&cols);
  sqlite3KeyInfoUnref(pKeyInfo);
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
  sqlite3_stmt *pTbls = 0;
  struct TableEntry *aAnc = 0;
  int nAnc = 0;
  struct TableEntry *aCur = 0;
  int nCur = 0;
  int rc;

  (void)pzErrMsg;
  if( pnFound ) *pnFound = 0;

  rc = loadAncestorAndCurrentCatalogs(db, pAncCatHash, &aAnc, &nAnc,
                                      &aCur, &nCur);
  if( rc!=SQLITE_OK ) return rc;

  rc = sqlite3_prepare_v2(db,
      "SELECT name FROM main.sqlite_master WHERE type='table' "
      "AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'dolt_%'",
      -1, &pTbls, 0);
  if( rc != SQLITE_OK ){
    doltliteFreeCatalog(aAnc, nAnc);
    doltliteFreeCatalog(aCur, nCur);
    return rc;
  }

  while( (rc = sqlite3_step(pTbls)) == SQLITE_ROW ){
    const char *zTableRaw = (const char*)sqlite3_column_text(pTbls, 0);
    char *zTable;
    sqlite3_stmt *pIdxList = 0;
    char *zIdxQ;
    int hasRowid = 1;
    MergePkInfo pkInfo;

    if( !zTableRaw ) continue;
    zTable = sqlite3_mprintf("%s", zTableRaw);
    if( !zTable ){ rc = SQLITE_NOMEM; break; }
    if( !cvTableAllowed(zTable, azTables, nTables) ){
      sqlite3_free(zTable);
      continue;
    }
    if( !catalogTableChanged(aAnc, nAnc, aCur, nCur, zTable) ){
      sqlite3_free(zTable);
      continue;
    }
    memset(&pkInfo, 0, sizeof(pkInfo));
    hasRowid = tableHasRowid(db, zTable);
    if( !hasRowid ){
      rc = loadMergePkInfo(db, zTable, &pkInfo);
      if( rc != SQLITE_OK ){
        sqlite3_free(zTable);
        break;
      }
    }

    zIdxQ = sqlite3_mprintf("PRAGMA main.index_list(%Q)", zTable);
    if( !zIdxQ ){ sqlite3_free(zTable); rc = SQLITE_NOMEM; break; }
    rc = sqlite3_prepare_v2(db, zIdxQ, -1, &pIdxList, 0);
    sqlite3_free(zIdxQ);
    if( rc != SQLITE_OK ){
      freeMergePkInfo(&pkInfo);
      sqlite3_free(zTable);
      break;
    }

    while( sqlite3_step(pIdxList) == SQLITE_ROW ){
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
      if( !zIdx ) break;
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
      if( supported && zColList && *zColList ){
        if( hasRowid ){
          rc = detectUniqueViolationsForIndex(
              db, aAnc, nAnc, zTable, pIdx, zColList, pnFound);
        }else{
          rc = detectUniqueViolationsForIndexWithoutRowid(
              db, aAnc, nAnc,
              doltliteFindTableByName(aCur, nCur, zTable),
              zTable, pIdx, zColList, &pkInfo, pnFound);
        }
      }
      sqlite3_free(zColList);
      sqlite3_free(zIdx);
      if( rc != SQLITE_OK ) break;
    }

    sqlite3_finalize(pIdxList);
    freeMergePkInfo(&pkInfo);
    sqlite3_free(zTable);
    if( rc != SQLITE_OK ) break;
  }
  if( rc == SQLITE_DONE ) rc = SQLITE_OK;
  sqlite3_finalize(pTbls);
  doltliteFreeCatalog(aAnc, nAnc);
  doltliteFreeCatalog(aCur, nCur);
  return rc;
}


#endif
