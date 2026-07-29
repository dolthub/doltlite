#ifdef DOLTLITE_PROLLY

#include "doltlite_merge_constraints_int.h"

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

static int uniqueIndexRowHasNull(sqlite3_stmt *pScan, int colFirst, int colLast){
  int i;
  for(i=colFirst; i<colLast; i++){
    if( sqlite3_column_type(pScan, i) == SQLITE_NULL ) return 1;
  }
  return 0;
}

static int detectUniqueViolationsForIndex(
  sqlite3 *db,
  struct TableEntry *aAnc, int nAnc,
  const char *zTable,
  const char *zIndexName,
  const char *zCols,
  char **pzErrMsg,
  int *pnFound
){
  sqlite3_stmt *pScan = 0;
  char *zQuery;
  char *zWinnerKey = 0;
  sqlite3_int64 winnerRowid = 0;
  int winnerHandled = 0;
  int rc;
  (void)pzErrMsg;

  zQuery = sqlite3_mprintf(
    "SELECT rowid, %s FROM main.\"%w\" NOT INDEXED ORDER BY %s, rowid",
    zCols, zTable, zCols);
  if( !zQuery ) return SQLITE_NOMEM;
  rc = sqlite3_prepare_v2(db, zQuery, -1, &pScan, 0);
  sqlite3_free(zQuery);
  if( rc != SQLITE_OK ) return rc;

  while( (rc = sqlite3_step(pScan)) == SQLITE_ROW ){
    sqlite3_int64 rowid = sqlite3_column_int64(pScan, 0);
    int nc = sqlite3_column_count(pScan);
    int i;
    sqlite3_str *pS;
    char *zRowKey;
    int isDup;

    if( uniqueIndexRowHasNull(pScan, 1, nc) ) continue;

    pS = sqlite3_str_new(0);
    for(i=1; i<nc; i++){
      const char *zV = (const char*)sqlite3_column_text(pScan, i);
      sqlite3_str_appendf(pS, "%s%Q", i>1?"|":"", zV ? zV : "");
    }
    zRowKey = sqlite3_str_finish(pS);
    if( !zRowKey ){ rc = SQLITE_NOMEM; break; }

    isDup = zWinnerKey && strcmp(zWinnerKey, zRowKey)==0;
    if( !isDup ){
      sqlite3_free(zWinnerKey);
      zWinnerKey = zRowKey;
      winnerRowid = rowid;
      winnerHandled = 0;
      continue;
    }
    sqlite3_free(zRowKey);

    if( !winnerHandled ){
      int appended = 0;
      rc = appendUniqueViolationByRowid(db, aAnc, nAnc, zTable, zIndexName,
                                        zCols, winnerRowid, &appended);
      if( rc != SQLITE_OK ) break;
      if( appended && pnFound ) (*pnFound)++;
      winnerHandled = 1;
    }

    {
      int appended = 0;
      rc = appendUniqueViolationByRowid(db, aAnc, nAnc, zTable, zIndexName,
                                        zCols, rowid, &appended);
      if( rc != SQLITE_OK ) break;
      if( appended && pnFound ) (*pnFound)++;
    }
  }
  sqlite3_free(zWinnerKey);
  if( rc == SQLITE_DONE ) rc = SQLITE_OK;
  sqlite3_finalize(pScan);
  return rc;
}

static int detectUniqueViolationsForIndexWithoutRowid(
  sqlite3 *db,
  struct TableEntry *aAnc, int nAnc,
  const char *zTable,
  const char *zIndexName,
  const char *zCols,
  const MergePkInfo *pPk,
  int *pnFound
){
  sqlite3_stmt *pScan = 0;
  sqlite3_str *pSql = 0;
  char *zQuery = 0;
  char *zWinnerKey = 0;
  u8 *pWinnerPkRec = 0;
  int nWinnerPkRec = 0;
  int winnerHandled = 0;
  int rc;

  pSql = sqlite3_str_new(0);
  sqlite3_str_appendf(pSql,
      "SELECT %s, %s FROM main.\"%w\" NOT INDEXED ORDER BY %s, %s",
      zCols, pPk->zPkCols, zTable, zCols, pPk->zPkCols);
  zQuery = sqlite3_str_finish(pSql);
  if( !zQuery ) return SQLITE_NOMEM;
  rc = sqlite3_prepare_v2(db, zQuery, -1, &pScan, 0);
  sqlite3_free(zQuery);
  if( rc!=SQLITE_OK ) return rc;

  while( (rc = sqlite3_step(pScan)) == SQLITE_ROW ){
    int nc = sqlite3_column_count(pScan);
    int nDupKeyCol = nc - pPk->nPk;
    sqlite3_str *pS;
    char *zRowKey = 0;
    int i, isDup;

    if( uniqueIndexRowHasNull(pScan, 0, nDupKeyCol) ) continue;

    pS = sqlite3_str_new(0);
    for(i=0; i<nDupKeyCol; i++){
      const char *zV = (const char*)sqlite3_column_text(pScan, i);
      sqlite3_str_appendf(pS, "%s%Q", i>0?"|":"", zV ? zV : "");
    }
    zRowKey = sqlite3_str_finish(pS);
    if( !zRowKey ){ rc = SQLITE_NOMEM; break; }

    isDup = zWinnerKey && strcmp(zWinnerKey, zRowKey)==0;
    if( !isDup ){
      u8 *pPkRec = 0;
      int nPkRec = 0;
      pPkRec = buildRecordFromStmtCols(pScan, nDupKeyCol, pPk->nPk, &nPkRec);
      if( !pPkRec ){
        sqlite3_free(zRowKey);
        rc = SQLITE_NOMEM;
        break;
      }
      sqlite3_free(zWinnerKey);
      sqlite3_free(pWinnerPkRec);
      zWinnerKey = zRowKey;
      pWinnerPkRec = pPkRec;
      nWinnerPkRec = nPkRec;
      winnerHandled = 0;
      continue;
    }
    sqlite3_free(zRowKey);

    {
      u8 *pPkRec = 0; int nPkRec = 0;
      pPkRec = buildRecordFromStmtCols(pScan, nDupKeyCol, pPk->nPk, &nPkRec);
      if( !pPkRec ){ rc = SQLITE_NOMEM; break; }
      if( !winnerHandled ){
        int appended = 0;
        rc = appendUniqueViolationByPk(db, aAnc, nAnc, zTable, zIndexName,
                                       zCols, pPk, pWinnerPkRec, nWinnerPkRec,
                                       &appended);
        if( rc != SQLITE_OK ){
          sqlite3_free(pPkRec);
          break;
        }
        if( appended && pnFound ) (*pnFound)++;
        winnerHandled = 1;
      }
      {
        int appended = 0;
        rc = appendUniqueViolationByPk(db, aAnc, nAnc, zTable, zIndexName,
                                       zCols, pPk, pPkRec, nPkRec, &appended);
        sqlite3_free(pPkRec);
        if( rc != SQLITE_OK ) break;
        if( appended && pnFound ) (*pnFound)++;
      }
    }
  }

  sqlite3_free(zWinnerKey);
  sqlite3_free(pWinnerPkRec);
  if( rc == SQLITE_DONE ) rc = SQLITE_OK;
  sqlite3_finalize(pScan);
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
      char *zColsQ;
      sqlite3_stmt *pCols = 0;
      sqlite3_str *pColList;
      char *zColList;
      int idxRc;

      if( !unique ) continue;
      zIdxRaw = (const char*)sqlite3_column_text(pIdxList, 1);
      if( !zIdxRaw ) continue;

      zOrigin = (const char*)sqlite3_column_text(pIdxList, 3);
      if( zOrigin && strcmp(zOrigin, "pk")==0 ) continue;

      zIdx = sqlite3_mprintf("%s", zIdxRaw);
      if( !zIdx ) break;
      zColsQ = sqlite3_mprintf("PRAGMA main.index_xinfo(%Q)", zIdx);
      if( !zColsQ ){ sqlite3_free(zIdx); break; }
      idxRc = sqlite3_prepare_v2(db, zColsQ, -1, &pCols, 0);
      sqlite3_free(zColsQ);
      if( idxRc != SQLITE_OK ){ sqlite3_free(zIdx); continue; }

      pColList = sqlite3_str_new(0);
      while( sqlite3_step(pCols) == SQLITE_ROW ){
        int cno = sqlite3_column_int(pCols, 1);
        int isKey = sqlite3_column_int(pCols, 5);
        const char *zCol = (const char*)sqlite3_column_text(pCols, 2);
        if( cno < 0 || !zCol || !isKey ) continue;
        if( sqlite3_str_length(pColList) > 0 ){
          sqlite3_str_appendall(pColList, ", ");
        }
        sqlite3_str_appendf(pColList, "\"%w\"", zCol);
      }
      sqlite3_finalize(pCols);
      zColList = sqlite3_str_finish(pColList);
      if( zColList && *zColList ){
        if( hasRowid ){
          rc = detectUniqueViolationsForIndex(db, aAnc, nAnc, zTable,
                                               zIdx, zColList, pzErrMsg,
                                               pnFound);
        }else{
          rc = detectUniqueViolationsForIndexWithoutRowid(
              db, aAnc, nAnc, zTable, zIdx, zColList, &pkInfo, pnFound);
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
