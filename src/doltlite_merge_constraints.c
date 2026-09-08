#ifdef DOLTLITE_PROLLY

#include "doltlite_merge_constraints_int.h"

static int copyCursorRow(
  ProllyCursor *pCur,
  u8 **ppKey, int *pnKey,
  u8 **ppVal, int *pnVal
){
  const u8 *pKey = 0, *pVal = 0;
  int nKey = 0, nVal = 0;

  *ppKey = 0; *pnKey = 0;
  *ppVal = 0; *pnVal = 0;

  prollyCursorKey(pCur, &pKey, &nKey);
  prollyCursorValue(pCur, &pVal, &nVal);
  if( pKey && nKey>0 ){
    *ppKey = sqlite3_malloc(nKey);
    if( !*ppKey ) return SQLITE_NOMEM;
    memcpy(*ppKey, pKey, nKey);
    *pnKey = nKey;
  }
  if( pVal && nVal>0 ){
    *ppVal = sqlite3_malloc(nVal);
    if( !*ppVal ){
      sqlite3_free(*ppKey);
      *ppKey = 0; *pnKey = 0;
      return SQLITE_NOMEM;
    }
    memcpy(*ppVal, pVal, nVal);
    *pnVal = nVal;
  }
  return SQLITE_OK;
}

static int fetchRowAtCursor(
  ChunkStore *cs,
  ProllyCache *pCache,
  const ProllyHash *pRoot,
  u8 flags,
  int bIntKey,
  i64 iKey,
  const u8 *pBlob, int nBlob,
  u8 **ppKey, int *pnKey,
  u8 **ppVal, int *pnVal
){
  ProllyCursor cur;
  int res, rc;

  *ppKey = 0; *pnKey = 0;
  *ppVal = 0; *pnVal = 0;

  if( prollyHashIsEmpty(pRoot) ) return SQLITE_NOTFOUND;

  prollyCursorInit(&cur, cs, pCache, pRoot, flags);
  rc = bIntKey
      ? prollyCursorSeekInt(&cur, iKey, &res)
      : prollyCursorSeekBlob(&cur, pBlob, nBlob, &res);
  if( rc!=SQLITE_OK || res!=0 ){
    prollyCursorClose(&cur);
    return rc!=SQLITE_OK ? rc : SQLITE_NOTFOUND;
  }
  rc = copyCursorRow(&cur, ppKey, pnKey, ppVal, pnVal);
  prollyCursorClose(&cur);
  return rc;
}



int catalogTableChanged(
  struct TableEntry *aAnc, int nAnc,
  struct TableEntry *aCur, int nCur,
  const char *zTable
){
  return doltliteTableEntryDiffers(
      doltliteFindTableByName(aAnc, nAnc, zTable),
      doltliteFindTableByName(aCur, nCur, zTable));
}


static int cvTableAllowed(
  const char *zTable,
  const char **azTables,
  int nTables
){
  int i;
  if( !zTable ) return 0;
  if( nTables<=0 || !azTables ) return 1;
  for(i=0; i<nTables; i++){
    if( azTables[i] && sqlite3_stricmp(azTables[i], zTable)==0 ) return 1;
  }
  return 0;
}

static int loadAncestorAndCurrentCatalogs(
  sqlite3 *db,
  const ProllyHash *pAncCatHash,
  struct TableEntry **paAnc, int *pnAnc,
  struct TableEntry **paCur, int *pnCur
){
  ProllyHash curHash;
  int rc;

  *paAnc = 0;
  *pnAnc = 0;
  *paCur = 0;
  *pnCur = 0;

  if( pAncCatHash && !prollyHashIsEmpty(pAncCatHash) ){
    rc = doltliteLoadCatalog(db, pAncCatHash, paAnc, pnAnc, 0);
    if( rc!=SQLITE_OK ) return rc;
  }

  rc = doltliteFlushCatalogToHash(db, &curHash);
  if( rc!=SQLITE_OK ){
    doltliteFreeCatalog(*paAnc, *pnAnc);
    *paAnc = 0;
    *pnAnc = 0;
    return rc;
  }
  rc = doltliteLoadCatalog(db, &curHash, paCur, pnCur, 0);
  if( rc!=SQLITE_OK ){
    doltliteFreeCatalog(*paAnc, *pnAnc);
    *paAnc = 0;
    *pnAnc = 0;
  }
  return rc;
}

int walkMergeUserTables(
  sqlite3 *db,
  const ProllyHash *pAncCatHash,
  char **pzErrMsg,
  const char **azTables,
  int nTables,
  int skipUnchanged,
  int wantSql,
  MergeUserTableWalk xWalk,
  void *pCtx
){
  sqlite3_stmt *pTbls = 0;
  struct TableEntry *aAnc = 0;
  int nAnc = 0;
  struct TableEntry *aCur = 0;
  int nCur = 0;
  int rc;
  int stepRc;

  rc = loadAncestorAndCurrentCatalogs(db, pAncCatHash, &aAnc, &nAnc,
                                      &aCur, &nCur);
  if( rc!=SQLITE_OK ) return rc;

  rc = sqlite3_prepare_v2(db,
      wantSql
        ? "SELECT name, sql FROM main.sqlite_master WHERE type='table' "
          "AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'dolt_%'"
        : "SELECT name FROM main.sqlite_master WHERE type='table' "
          "AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'dolt_%'",
      -1, &pTbls, 0);
  if( rc!=SQLITE_OK ){
    doltliteFreeCatalog(aAnc, nAnc);
    doltliteFreeCatalog(aCur, nCur);
    return rc;
  }

  while( (stepRc = sqlite3_step(pTbls))==SQLITE_ROW ){
    const char *zTableRaw = (const char*)sqlite3_column_text(pTbls, 0);
    const char *zSqlRaw = wantSql ? (const char*)sqlite3_column_text(pTbls, 1) : 0;
    char *zTable;
    char *zSql = 0;

    if( !zTableRaw ) continue;
    if( wantSql && !zSqlRaw ) continue;
    zTable = sqlite3_mprintf("%s", zTableRaw);
    if( !zTable ){ rc = SQLITE_NOMEM; break; }
    if( wantSql ){
      zSql = sqlite3_mprintf("%s", zSqlRaw);
      if( !zSql ){
        sqlite3_free(zTable);
        rc = SQLITE_NOMEM;
        break;
      }
    }
    if( !cvTableAllowed(zTable, azTables, nTables)
     || (skipUnchanged
         && !catalogTableChanged(aAnc, nAnc, aCur, nCur, zTable)) ){
      sqlite3_free(zTable);
      sqlite3_free(zSql);
      continue;
    }
    rc = xWalk(db, zTable, zSql, aAnc, nAnc, aCur, nCur, pCtx);
    sqlite3_free(zTable);
    sqlite3_free(zSql);
    if( rc!=SQLITE_OK ) break;
  }
  if( rc==SQLITE_OK && stepRc!=SQLITE_DONE && stepRc!=SQLITE_ROW ){
    rc = stepRc;
  }
  rc = finishConstraintStmt(pTbls, rc);
  doltliteFreeCatalog(aAnc, nAnc);
  doltliteFreeCatalog(aCur, nCur);
  setConstraintError(db, pzErrMsg, rc);
  return rc;
}

void freeMergePkInfo(MergePkInfo *pPk){
  if( !pPk ) return;
  doltliteFreeStringArray(pPk->azPk, pPk->nPk);
  sqlite3_free(pPk->zPkCols);
  memset(pPk, 0, sizeof(*pPk));
}

int finishConstraintStmt(sqlite3_stmt *pStmt, int rc){
  int finalizeRc = sqlite3_finalize(pStmt);
  return rc==SQLITE_OK ? finalizeRc : rc;
}

void setConstraintError(sqlite3 *db, char **pzErrMsg, int rc){
  if( rc!=SQLITE_OK && pzErrMsg && !*pzErrMsg ){
    const char *zErr = (sqlite3_errcode(db) & 0xff)==rc
        ? sqlite3_errmsg(db) : sqlite3_errstr(rc);
    *pzErrMsg = sqlite3_mprintf("%s", zErr);
  }
}

int loadMergePkInfo(sqlite3 *db, const char *zTable, MergePkInfo *pPk){
  sqlite3_stmt *pStmt = 0;
  sqlite3_str *pCols = 0;
  char *zQuery = 0;
  int rc;
  int stepRc;
  int nPk = 0;
  char **azPk = 0;
  int i;

  memset(pPk, 0, sizeof(*pPk));
  zQuery = sqlite3_mprintf("PRAGMA main.table_info(%Q)", zTable);
  if( !zQuery ) return SQLITE_NOMEM;
  rc = sqlite3_prepare_v2(db, zQuery, -1, &pStmt, 0);
  sqlite3_free(zQuery);
  if( rc!=SQLITE_OK ) return rc;

  while( (stepRc = sqlite3_step(pStmt))==SQLITE_ROW ){
    int pk = sqlite3_column_int(pStmt, 5);
    if( pk>nPk ) nPk = pk;
  }
  if( stepRc!=SQLITE_DONE ){
    return finishConstraintStmt(pStmt, stepRc);
  }
  rc = sqlite3_reset(pStmt);
  if( rc!=SQLITE_OK ){
    return finishConstraintStmt(pStmt, rc);
  }
  if( nPk<=0 ){
    rc = finishConstraintStmt(pStmt, SQLITE_OK);
    return rc==SQLITE_OK ? SQLITE_NOTFOUND : rc;
  }

  azPk = sqlite3_malloc64((sqlite3_int64)nPk * sizeof(char*));
  if( !azPk ){
    return finishConstraintStmt(pStmt, SQLITE_NOMEM);
  }
  memset(azPk, 0, (size_t)nPk * sizeof(char*));

  while( (stepRc = sqlite3_step(pStmt))==SQLITE_ROW ){
    const char *zCol = (const char*)sqlite3_column_text(pStmt, 1);
    int pk = sqlite3_column_int(pStmt, 5);
    if( pk<=0 || pk>nPk ) continue;
    azPk[pk-1] = sqlite3_mprintf("%s", zCol ? zCol : "");
    if( !azPk[pk-1] ){
      rc = SQLITE_NOMEM;
      break;
    }
  }
  if( rc==SQLITE_OK && stepRc!=SQLITE_DONE ) rc = stepRc;
  rc = finishConstraintStmt(pStmt, rc);
  if( rc!=SQLITE_OK ){
    doltliteFreeStringArray(azPk, nPk);
    return rc;
  }

  pCols = sqlite3_str_new(0);
  for(i=0; i<nPk; i++){
    if( !azPk[i] ){
      sqlite3_free(sqlite3_str_finish(pCols));
      doltliteFreeStringArray(azPk, nPk);
      return SQLITE_CORRUPT;
    }
    if( i>0 ) sqlite3_str_appendall(pCols, ", ");
    sqlite3_str_appendf(pCols, "\"%w\"", azPk[i]);
  }

  pPk->nPk = nPk;
  pPk->azPk = azPk;
  pPk->zPkCols = sqlite3_str_finish(pCols);
  if( !pPk->zPkCols ){
    freeMergePkInfo(pPk);
    return SQLITE_NOMEM;
  }
  return SQLITE_OK;
}

u8 *buildRecordFromStmtCols(
  sqlite3_stmt *pStmt,
  int iStart,
  int nField,
  int *pnOut
){
  DoltliteSerialValue *aMem = 0;
  u8 *pOut;
  int i;

  *pnOut = 0;
  if( nField<=0 ) return 0;

  aMem = sqlite3_malloc64((sqlite3_int64)nField * sizeof(DoltliteSerialValue));
  if( !aMem ) return 0;
  memset(aMem, 0, (size_t)nField * sizeof(DoltliteSerialValue));

  for(i=0; i<nField; i++){
    int iCol = iStart + i;
    int eType = sqlite3_column_type(pStmt, iCol);
    aMem[i].eType = eType;
    switch( eType ){
      case SQLITE_INTEGER:
        aMem[i].i = sqlite3_column_int64(pStmt, iCol);
        break;
      case SQLITE_FLOAT:
        aMem[i].r = sqlite3_column_double(pStmt, iCol);
        break;
      case SQLITE_BLOB:
        aMem[i].n = sqlite3_column_bytes(pStmt, iCol);
        aMem[i].p = sqlite3_column_blob(pStmt, iCol);
        break;
      case SQLITE_TEXT:
        aMem[i].p = sqlite3_column_text(pStmt, iCol);
        aMem[i].n = sqlite3_column_bytes(pStmt, iCol);
        break;
      case SQLITE_NULL:
      default:
        break;
    }
  }

  pOut = doltliteBuildRecord(aMem, nField, pnOut);
  sqlite3_free(aMem);
  return pOut;
}

static int recordPrefixEquals(
  const u8 *pLeft, int nLeft,
  const u8 *pRight, int nRight,
  int nField
){
  DoltliteRecordInfo a, b;
  int i;

  if( nField<=0 ) return 1;
  if( doltliteParseRecordStrict(pLeft, nLeft, &a)!=SQLITE_OK ) return 0;
  if( doltliteParseRecordStrict(pRight, nRight, &b)!=SQLITE_OK ) return 0;
  if( a.nField < nField || b.nField < nField ) return 0;

  for(i=0; i<nField; i++){
    int nA = dlSerialTypeLen((u64)a.aType[i]);
    int nB = dlSerialTypeLen((u64)b.aType[i]);
    if( a.aType[i]!=b.aType[i] ) return 0;
    if( nA!=nB ) return 0;
    if( nA>0 && memcmp(pLeft + a.aOffset[i], pRight + b.aOffset[i], (size_t)nA)!=0 ){
      return 0;
    }
  }
  return 1;
}

/* PK-equals-all-columns rows store an empty value; match the sort-key
** record instead or callers skip the check as "no such row". */
static int fetchRowByPkRecord(
  ChunkStore *cs,
  ProllyCache *pCache,
  const ProllyHash *pRoot,
  u8 flags,
  sqlite3 *db,
  const char *zTable,
  const u8 *pPkRec, int nPkRec,
  int nPkField,
  u8 **ppKey, int *pnKey,
  u8 **ppVal, int *pnVal
){
  ProllyCursor cur;
  int rc, res;

  *ppKey = 0; *pnKey = 0;
  *ppVal = 0; *pnVal = 0;
  if( prollyHashIsEmpty(pRoot) ) return SQLITE_NOTFOUND;

  prollyCursorInit(&cur, cs, pCache, pRoot, flags);
  rc = prollyCursorFirst(&cur, &res);
  if( rc!=SQLITE_OK ){
    prollyCursorClose(&cur);
    return rc;
  }
  while( res==0 && prollyCursorIsValid(&cur) ){
    const u8 *pVal = 0;
    int nVal = 0;
    prollyCursorValue(&cur, &pVal, &nVal);
    if( pVal && nVal>0
     && recordPrefixEquals(pVal, nVal, pPkRec, nPkRec, nPkField) ){
      rc = copyCursorRow(&cur, ppKey, pnKey, ppVal, pnVal);
      prollyCursorClose(&cur);
      return rc;
    }
    if( (!pVal || nVal==0) && db && zTable ){
      const u8 *pKey = 0;
      int nKey = 0;
      u8 *pKeyRec = 0;
      int nKeyRec = 0;
      prollyCursorKey(&cur, &pKey, &nKey);
      rc = doltliteRecordFromClusteredKey(db, zTable, pKey, nKey,
                                          &pKeyRec, &nKeyRec);
      if( rc!=SQLITE_OK ){
        prollyCursorClose(&cur);
        return rc;
      }
      if( pKeyRec
       && recordPrefixEquals(pKeyRec, nKeyRec, pPkRec, nPkRec, nPkField) ){
        u8 *pKeyCopy = 0;
        if( nKey>0 ){
          pKeyCopy = sqlite3_malloc(nKey);
          if( !pKeyCopy ){
            sqlite3_free(pKeyRec);
            prollyCursorClose(&cur);
            return SQLITE_NOMEM;
          }
          memcpy(pKeyCopy, pKey, nKey);
        }
        *ppKey = pKeyCopy;
        *pnKey = nKey;
        *ppVal = pKeyRec;
        *pnVal = nKeyRec;
        prollyCursorClose(&cur);
        return SQLITE_OK;
      }
      sqlite3_free(pKeyRec);
    }
    rc = prollyCursorNext(&cur);
    if( rc!=SQLITE_OK ){
      prollyCursorClose(&cur);
      return rc;
    }
  }
  prollyCursorClose(&cur);
  return SQLITE_NOTFOUND;
}

static int fetchAncestorRow(
  sqlite3 *db,
  struct TableEntry *aAnc, int nAnc,
  const char *zTable,
  int bIntKey,
  i64 rowid,
  const u8 *pKey, int nKey,
  u8 **ppAncVal, int *pnAncVal
){
  ChunkStore *cs;
  ProllyCache *pCache;
  struct TableEntry *pTE;
  u8 *pAncKey = 0; int nAncKey = 0;
  int rc;

  *ppAncVal = 0;
  *pnAncVal = 0;

  if( !aAnc || nAnc==0 ) return SQLITE_NOTFOUND;

  cs = doltliteGetChunkStore(db);
  pCache = doltliteGetCache(db);
  if( !cs || !pCache ) return SQLITE_ERROR;

  pTE = doltliteFindTableByName(aAnc, nAnc, zTable);
  if( !pTE ) return SQLITE_NOTFOUND;
  if( !bIntKey && (pTE->flags & PROLLY_NODE_INTKEY) ) return SQLITE_NOTFOUND;

  rc = fetchRowAtCursor(cs, pCache, &pTE->root, pTE->flags,
                        bIntKey, rowid, pKey, nKey,
                        &pAncKey, &nAncKey, ppAncVal, pnAncVal);
  sqlite3_free(pAncKey);
  return rc;
}

int fetchAncestorRowByName(
  sqlite3 *db,
  struct TableEntry *aAnc, int nAnc,
  const char *zTable,
  i64 rowid,
  u8 **ppAncVal, int *pnAncVal
){
  return fetchAncestorRow(db, aAnc, nAnc, zTable, 1, rowid, 0, 0,
                          ppAncVal, pnAncVal);
}

int fetchAncestorRowByKey(
  sqlite3 *db,
  struct TableEntry *aAnc, int nAnc,
  const char *zTable,
  const u8 *pKey, int nKey,
  u8 **ppAncVal, int *pnAncVal
){
  return fetchAncestorRow(db, aAnc, nAnc, zTable, 0, 0, pKey, nKey,
                          ppAncVal, pnAncVal);
}

int isRowPreExisting(
  const u8 *pMergedVal, int nMergedVal,
  const u8 *pAncVal, int nAncVal
){
  if( !pMergedVal || !pAncVal ) return 0;
  if( nMergedVal != nAncVal ) return 0;
  if( nMergedVal == 0 ) return 1;
  return memcmp(pMergedVal, pAncVal, nMergedVal)==0 ? 1 : 0;
}

int tableHasRowid(sqlite3 *db, const char *zTable, int *pHasRowid){
  sqlite3_stmt *pStmt = 0;
  char *zQuery;
  int rc;
  int stepRc;

  *pHasRowid = 0;
  zQuery = sqlite3_mprintf(
      "SELECT wr FROM pragma_table_list WHERE schema='main' AND name=%Q",
      zTable);
  if( !zQuery ) return SQLITE_NOMEM;
  rc = sqlite3_prepare_v2(db, zQuery, -1, &pStmt, 0);
  sqlite3_free(zQuery);
  if( rc!=SQLITE_OK ) return rc;
  stepRc = sqlite3_step(pStmt);
  if( stepRc==SQLITE_ROW ){
    *pHasRowid = !sqlite3_column_int(pStmt, 0);
    rc = SQLITE_OK;
  }else{
    rc = stepRc==SQLITE_DONE ? SQLITE_NOTFOUND : stepRc;
  }
  return finishConstraintStmt(pStmt, rc);
}

int fetchOrphanRow(
  sqlite3 *db,
  const char *zTable,
  i64 rowid,
  u8 **ppKey, int *pnKey,
  u8 **ppVal, int *pnVal
){
  ChunkStore *cs;
  ProllyCache *pCache;
  ProllyHash root;
  u8 flags = 0;
  Pgno iTable;
  int rc;

  *ppKey = 0; *pnKey = 0;
  *ppVal = 0; *pnVal = 0;

  cs = doltliteGetChunkStore(db);
  pCache = doltliteGetCache(db);
  if( !cs || !pCache ) return SQLITE_ERROR;

  rc = doltliteResolveTableName(db, zTable, &iTable);
  if( rc != SQLITE_OK ) return rc;

  rc = doltliteGetSessionTableRoot(db, iTable, &root, &flags);
  if( rc != SQLITE_OK ) return rc;

  return fetchRowAtCursor(cs, pCache, &root, flags, 1, rowid, 0, 0,
                          ppKey, pnKey, ppVal, pnVal);
}

int fetchRowByPkFromTable(
  sqlite3 *db,
  const char *zTable,
  const u8 *pPkRec, int nPkRec,
  int nPkField,
  u8 **ppKey, int *pnKey,
  u8 **ppVal, int *pnVal
){
  ChunkStore *cs;
  ProllyCache *pCache;
  ProllyHash root;
  u8 flags = 0;
  Pgno iTable;
  int rc;

  *ppKey = 0; *pnKey = 0;
  *ppVal = 0; *pnVal = 0;

  cs = doltliteGetChunkStore(db);
  pCache = doltliteGetCache(db);
  if( !cs || !pCache ) return SQLITE_ERROR;

  rc = doltliteResolveTableName(db, zTable, &iTable);
  if( rc != SQLITE_OK ) return rc;

  rc = doltliteGetSessionTableRoot(db, iTable, &root, &flags);
  if( rc != SQLITE_OK ) return rc;
  if( flags & PROLLY_NODE_INTKEY ) return SQLITE_NOTFOUND;

  return fetchRowByPkRecord(cs, pCache, &root, flags, db, zTable,
                            pPkRec, nPkRec, nPkField,
                            ppKey, pnKey, ppVal, pnVal);
}

int scanMergeColumnFlagViolations(
  sqlite3 *db,
  const char *zTable,
  struct TableEntry *aAnc, int nAnc,
  char **azCols, char **azExtra, int nCols,
  u8 cvType,
  int *pnFound
){
  int hasRowid;
  int nKeyCol;
  MergePkInfo pkInfo;
  sqlite3_str *pStr;
  char *zQuery = 0;
  sqlite3_stmt *pQ = 0;
  int i;
  int rc;
  int queryStepRc;

  memset(&pkInfo, 0, sizeof(pkInfo));
  rc = tableHasRowid(db, zTable, &hasRowid);
  if( rc!=SQLITE_OK ) return rc;
  if( !hasRowid ){
    rc = loadMergePkInfo(db, zTable, &pkInfo);
    if( rc!=SQLITE_OK ) return rc;
  }
  nKeyCol = hasRowid ? 1 : pkInfo.nPk;

  pStr = sqlite3_str_new(db);
  sqlite3_str_appendf(pStr, "SELECT %s",
                      hasRowid ? "rowid" : pkInfo.zPkCols);
  for(i=0; i<nCols; i++){
    if( azExtra ){
      sqlite3_str_appendf(pStr, ", typeof(\"%w\") NOT IN ('null','%s')",
                          azCols[i], azExtra[i]);
    }else{
      sqlite3_str_appendf(pStr, ", typeof(\"%w\")='null'", azCols[i]);
    }
  }
  sqlite3_str_appendf(pStr, " FROM main.\"%w\" NOT INDEXED WHERE 0", zTable);
  for(i=0; i<nCols; i++){
    if( azExtra ){
      sqlite3_str_appendf(pStr, " OR typeof(\"%w\") NOT IN ('null','%s')",
                          azCols[i], azExtra[i]);
    }else{
      sqlite3_str_appendf(pStr, " OR typeof(\"%w\")='null'", azCols[i]);
    }
  }
  zQuery = sqlite3_str_finish(pStr);
  if( !zQuery ){
    freeMergePkInfo(&pkInfo);
    return SQLITE_NOMEM;
  }
  rc = sqlite3_prepare_v2(db, zQuery, -1, &pQ, 0);
  sqlite3_free(zQuery);
  if( rc!=SQLITE_OK ){
    freeMergePkInfo(&pkInfo);
    return rc;
  }

  while( (queryStepRc = sqlite3_step(pQ))==SQLITE_ROW ){
    u8 *pKey = 0; int nKey = 0;
    u8 *pVal = 0; int nVal = 0;
    i64 intKey = 0;
    sqlite3_str *pInfo;
    char *zInfo;
    int nNamed = 0;
    int appendRc;

    if( hasRowid ){
      intKey = sqlite3_column_int64(pQ, 0);
      rc = fetchOrphanRow(db, zTable, intKey, &pKey, &nKey, &pVal, &nVal);
    }else{
      u8 *pPkRec = 0; int nPkRec = 0;
      pPkRec = buildRecordFromStmtCols(pQ, 0, pkInfo.nPk, &nPkRec);
      if( !pPkRec ){ rc = SQLITE_NOMEM; break; }
      rc = fetchRowByPkFromTable(db, zTable, pPkRec, nPkRec, pkInfo.nPk,
                                 &pKey, &nKey, &pVal, &nVal);
      sqlite3_free(pPkRec);
    }
    if( rc==SQLITE_NOTFOUND ){ rc = SQLITE_OK; continue; }
    if( rc!=SQLITE_OK ){
      sqlite3_free(pKey);
      sqlite3_free(pVal);
      break;
    }

    if( aAnc ){
      u8 *pAncVal = 0; int nAncVal = 0;
      int ancRc = hasRowid
          ? fetchAncestorRowByName(db, aAnc, nAnc, zTable,
                                   intKey, &pAncVal, &nAncVal)
          : fetchAncestorRowByKey(db, aAnc, nAnc, zTable,
                                  pKey, nKey, &pAncVal, &nAncVal);
      int preExisting = (ancRc==SQLITE_OK)
          && isRowPreExisting(pVal, nVal, pAncVal, nAncVal);
      sqlite3_free(pAncVal);
      if( preExisting ){
        sqlite3_free(pKey);
        sqlite3_free(pVal);
        continue;
      }
    }

    pInfo = sqlite3_str_new(db);
    sqlite3_str_appendall(pInfo, "{\"Columns\": [");
    for(i=0; i<nCols; i++){
      if( sqlite3_column_int(pQ, nKeyCol + i) ){
        sqlite3_str_appendf(pInfo, "%s\"%w\"", nNamed ? ", " : "", azCols[i]);
        nNamed++;
      }
    }
    sqlite3_str_appendall(pInfo, "]}");
    zInfo = sqlite3_str_finish(pInfo);
    if( !zInfo ){
      sqlite3_free(pKey);
      sqlite3_free(pVal);
      rc = SQLITE_NOMEM;
      break;
    }
    appendRc = doltliteAppendConstraintViolation(
        db, zTable, cvType, intKey, pKey, nKey, pVal, nVal, zInfo);
    sqlite3_free(zInfo);
    sqlite3_free(pKey);
    sqlite3_free(pVal);
    if( appendRc!=SQLITE_OK ){ rc = appendRc; break; }
    if( pnFound ) (*pnFound)++;
  }
  if( rc==SQLITE_OK && queryStepRc!=SQLITE_DONE ) rc = queryStepRc;
  rc = finishConstraintStmt(pQ, rc);
  freeMergePkInfo(&pkInfo);
  return rc;
}


#endif
