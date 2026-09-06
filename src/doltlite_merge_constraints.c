#ifdef DOLTLITE_PROLLY

#include "doltlite_merge_constraints_int.h"

int copyCursorRow(
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

int fetchRowByRowid(
  ChunkStore *cs,
  ProllyCache *pCache,
  const ProllyHash *pRoot,
  u8 flags,
  i64 targetRowid,
  u8 **ppKey, int *pnKey,
  u8 **ppVal, int *pnVal
){
  ProllyCursor cur;
  int res, rc;

  *ppKey = 0; *pnKey = 0;
  *ppVal = 0; *pnVal = 0;

  if( prollyHashIsEmpty(pRoot) ) return SQLITE_NOTFOUND;

  prollyCursorInit(&cur, cs, pCache, pRoot, flags);
  rc = prollyCursorSeekInt(&cur, targetRowid, &res);
  if( rc!=SQLITE_OK ){
    prollyCursorClose(&cur);
    return rc;
  }
  if( res!=0 ){
    prollyCursorClose(&cur);
    return SQLITE_NOTFOUND;
  }
  rc = copyCursorRow(&cur, ppKey, pnKey, ppVal, pnVal);
  prollyCursorClose(&cur);
  return rc;
}

int fetchRowByBlobKey(
  ChunkStore *cs,
  ProllyCache *pCache,
  const ProllyHash *pRoot,
  u8 flags,
  const u8 *pKey, int nKey,
  u8 **ppKey, int *pnKey,
  u8 **ppVal, int *pnVal
){
  ProllyCursor cur;
  int res, rc;

  *ppKey = 0; *pnKey = 0;
  *ppVal = 0; *pnVal = 0;

  if( prollyHashIsEmpty(pRoot) ) return SQLITE_NOTFOUND;

  prollyCursorInit(&cur, cs, pCache, pRoot, flags);
  rc = prollyCursorSeekBlob(&cur, pKey, nKey, &res);
  if( rc!=SQLITE_OK ){
    prollyCursorClose(&cur);
    return rc;
  }
  if( res!=0 ){
    prollyCursorClose(&cur);
    return SQLITE_NOTFOUND;
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


int cvTableAllowed(
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

int loadAncestorAndCurrentCatalogs(
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

int recordPrefixEquals(
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
int fetchRowByPkRecord(
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

int fetchAncestorRowByName(
  sqlite3 *db,
  struct TableEntry *aAnc, int nAnc,
  const char *zTable,
  i64 rowid,
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

  rc = fetchRowByRowid(cs, pCache, &pTE->root, pTE->flags, rowid,
                       &pAncKey, &nAncKey, ppAncVal, pnAncVal);
  sqlite3_free(pAncKey);
  return rc;
}

int fetchAncestorRowByKey(
  sqlite3 *db,
  struct TableEntry *aAnc, int nAnc,
  const char *zTable,
  const u8 *pKey, int nKey,
  u8 **ppAncVal, int *pnAncVal
){
  ChunkStore *cs;
  ProllyCache *pCache;
  struct TableEntry *pTE;
  u8 *pAncKey = 0;
  int nAncKey = 0;
  int rc;

  *ppAncVal = 0;
  *pnAncVal = 0;

  if( !aAnc || nAnc==0 ) return SQLITE_NOTFOUND;

  cs = doltliteGetChunkStore(db);
  pCache = doltliteGetCache(db);
  if( !cs || !pCache ) return SQLITE_ERROR;

  pTE = doltliteFindTableByName(aAnc, nAnc, zTable);
  if( !pTE ) return SQLITE_NOTFOUND;
  if( pTE->flags & PROLLY_NODE_INTKEY ) return SQLITE_NOTFOUND;

  rc = fetchRowByBlobKey(cs, pCache, &pTE->root, pTE->flags, pKey, nKey,
                         &pAncKey, &nAncKey, ppAncVal, pnAncVal);
  sqlite3_free(pAncKey);
  return rc;
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

  return fetchRowByRowid(cs, pCache, &root, flags, rowid,
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


#endif
