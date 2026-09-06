#ifdef DOLTLITE_PROLLY

#include "doltlite_merge_int.h"



/* KeyInfo without Parse. Matches sqlite3KeyInfoOfIndex so VC paths
** encode the same sort keys as VDBE. */
KeyInfo *doltliteKeyInfoOfIndex(sqlite3 *db, Index *pIdx){
  int i;
  int nCol;
  int nKey;
  KeyInfo *pKey;

  if( !db || !pIdx ) return 0;
  nCol = pIdx->nColumn;
  nKey = pIdx->nKeyCol;
  if( pIdx->uniqNotNull ){
    pKey = sqlite3KeyInfoAlloc(db, nKey, nCol - nKey);
  }else{
    pKey = sqlite3KeyInfoAlloc(db, nCol, 0);
  }
  if( !pKey ) return 0;
  for(i=0; i<nCol; i++){
    const char *zColl = pIdx->azColl ? pIdx->azColl[i] : 0;
    if( !zColl || zColl==sqlite3StrBINARY
     || sqlite3StrICmp(zColl, "BINARY")==0 ){
      pKey->aColl[i] = 0;
    }else{
      pKey->aColl[i] = sqlite3FindCollSeq(db, ENC(db), zColl, 0);
    }
    pKey->aSortFlags[i] = pIdx->aSortOrder ? pIdx->aSortOrder[i] : 0;
  }
  return pKey;
}

static int indexKeyInfoNeedsPayload(
  const KeyInfo *pKeyInfo,
  const u8 *pIdxRec,
  int nIdxRec
){
  int i;
  if( pKeyInfo ){
    if( pKeyInfo->nKeyField < pKeyInfo->nAllField ) return 1;
    for(i=0; i<pKeyInfo->nAllField; i++){
      const CollSeq *pColl = pKeyInfo->aColl[i];
      if( pColl && pColl->zName
       && (sqlite3StrICmp(pColl->zName, "NOCASE")==0
        || sqlite3StrICmp(pColl->zName, "RTRIM")==0) ){
        return 1;
      }
    }
  }
  return sortKeyRecordNeedsPayload(pIdxRec, nIdxRec, 0);
}

static int indexColumnIsExpr(const i16 *aiColumn, int nIdxCol){
  int i;
  for(i=0; i<nIdxCol; i++){
    if( aiColumn[i]==XN_EXPR ) return 1;
  }
  return 0;
}

static int serialValueFromRecordField(
  const u8 *pRec, int nRec,
  const DoltliteRecordInfo *pInfo,
  int iField,
  DoltliteSerialValue *pValue
){
  int st, off, n;
  memset(pValue, 0, sizeof(*pValue));
  if( iField<0 || iField>=pInfo->nField ) return SQLITE_CORRUPT;
  st = pInfo->aType[iField];
  off = pInfo->aOffset[iField];
  n = dlSerialTypeLen((u64)st);
  if( n<0 || off<0 || off>nRec-n ) return SQLITE_CORRUPT;
  if( st==0 ){
    pValue->eType = SQLITE_NULL;
  }else if( st==8 || st==9 || (st>=1 && st<=6) ){
    pValue->eType = SQLITE_INTEGER;
    pValue->i = st==8 ? 0 : st==9 ? 1 : dlReadIntBytes(pRec + off, n);
  }else if( st==7 ){
    u64 bits = 0;
    int i;
    pValue->eType = SQLITE_FLOAT;
    for(i=0; i<8; i++) bits = (bits<<8) | pRec[off+i];
    memcpy(&pValue->r, &bits, 8);
  }else if( st>=13 && (st&1)==1 ){
    pValue->eType = SQLITE_TEXT;
    pValue->p = pRec + off;
    pValue->n = n;
  }else if( st>=12 && (st&1)==0 ){
    pValue->eType = SQLITE_BLOB;
    pValue->p = pRec + off;
    pValue->n = n;
  }else{
    return SQLITE_CORRUPT;
  }
  return SQLITE_OK;
}

static int bindIndexExprRow(
  sqlite3_stmt *pStmt,
  Table *pTab,
  const u8 *pRec, int nRec,
  int iPKey, i64 intKey
){
  DoltliteRecordInfo info;
  int i, rc;
  doltliteParseRecord(pRec, nRec, &info);
  for(i=0; i<pTab->nCol; i++){
    if( i==iPKey ){
      rc = sqlite3_bind_int64(pStmt, i+1, intKey);
    }else if( i<info.nField ){
      DoltliteSerialValue v;
      rc = serialValueFromRecordField(pRec, nRec, &info, i, &v);
      if( rc!=SQLITE_OK ) return rc;
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
    }else{
      rc = sqlite3_bind_null(pStmt, i+1);
    }
    if( rc!=SQLITE_OK ) return rc;
  }
  return SQLITE_OK;
}

static int indexExprToSql(sqlite3_str *p, const Expr *pExpr, Table *pTab){
  int i;
  if( !pExpr ) return SQLITE_ERROR;
  switch( pExpr->op ){
    case TK_COLLATE:
    case TK_UPLUS:
      return indexExprToSql(p, pExpr->pLeft, pTab);
    case TK_UMINUS:
      sqlite3_str_appendall(p, "-(");
      if( indexExprToSql(p, pExpr->pLeft, pTab) ) return SQLITE_ERROR;
      sqlite3_str_appendall(p, ")");
      return SQLITE_OK;
    case TK_COLUMN:
      if( pExpr->iColumn<0 ){
        sqlite3_str_appendall(p, "rowid");
      }else if( pTab && pExpr->iColumn<pTab->nCol ){
        sqlite3_str_appendf(p, "\"%w\"", pTab->aCol[pExpr->iColumn].zCnName);
      }else{
        return SQLITE_ERROR;
      }
      return SQLITE_OK;
    case TK_STRING:
      sqlite3_str_appendf(p, "%Q", pExpr->u.zToken);
      return SQLITE_OK;
    case TK_FLOAT:
      sqlite3_str_appendall(p, pExpr->u.zToken);
      return SQLITE_OK;
    case TK_NULL:
      sqlite3_str_appendall(p, "NULL");
      return SQLITE_OK;
    case TK_INTEGER:
      if( ExprHasProperty(pExpr, EP_IntValue) ){
        sqlite3_str_appendf(p, "%d", pExpr->u.iValue);
      }else{
        sqlite3_str_appendall(p, pExpr->u.zToken);
      }
      return SQLITE_OK;
    case TK_FUNCTION:
      sqlite3_str_appendf(p, "%s(", pExpr->u.zToken);
      if( pExpr->x.pList ){
        for(i=0; i<pExpr->x.pList->nExpr; i++){
          if( i ) sqlite3_str_appendall(p, ",");
          if( indexExprToSql(p, pExpr->x.pList->a[i].pExpr, pTab) ){
            return SQLITE_ERROR;
          }
        }
      }
      sqlite3_str_appendall(p, ")");
      return SQLITE_OK;
    case TK_PLUS:
    case TK_MINUS:
    case TK_STAR:
    case TK_SLASH:
    case TK_REM:
    case TK_CONCAT:
      sqlite3_str_appendall(p, "(");
      if( indexExprToSql(p, pExpr->pLeft, pTab) ) return SQLITE_ERROR;
      sqlite3_str_appendall(p,
          pExpr->op==TK_PLUS ? "+" :
          pExpr->op==TK_MINUS ? "-" :
          pExpr->op==TK_STAR ? "*" :
          pExpr->op==TK_SLASH ? "/" :
          pExpr->op==TK_REM ? "%" : "||");
      if( indexExprToSql(p, pExpr->pRight, pTab) ) return SQLITE_ERROR;
      sqlite3_str_appendall(p, ")");
      return SQLITE_OK;
    default:
      return SQLITE_ERROR;
  }
}

static int evalIndexExprColumn(
  sqlite3 *db,
  Index *pIdx,
  const u8 *pRec, int nRec,
  int iPKey, i64 intKey,
  int iIdxCol,
  DoltliteSerialValue *pOut,
  u8 **ppKeep
){
  Table *pTab;
  sqlite3 *pEval = 0;
  sqlite3_str *pSql;
  char *zSql;
  sqlite3_stmt *pStmt = 0;
  sqlite3_value *pVal;
  const char *zSpan;
  int i, n, rc;
  int eType;

  *ppKeep = 0;
  memset(pOut, 0, sizeof(*pOut));
  (void)db;
  if( !pIdx || !pIdx->pTable || !pIdx->aColExpr
   || iIdxCol<0 || iIdxCol>=pIdx->aColExpr->nExpr ){
    return SQLITE_ERROR;
  }
  pTab = pIdx->pTable;
  zSpan = pIdx->aColExpr->a[iIdxCol].zEName;
  pSql = sqlite3_str_new(0);
  sqlite3_str_appendall(pSql, "SELECT (");
  if( zSpan && zSpan[0] ){
    sqlite3_str_appendall(pSql, zSpan);
  }else if( indexExprToSql(pSql, pIdx->aColExpr->a[iIdxCol].pExpr, pTab) ){
    sqlite3_free(sqlite3_str_finish(pSql));
    return SQLITE_ERROR;
  }
  sqlite3_str_appendall(pSql, ") FROM (SELECT ");
  for(i=0; i<pTab->nCol; i++){
    if( i ) sqlite3_str_appendall(pSql, ", ");
    sqlite3_str_appendf(pSql, "?%d AS \"%w\"", i+1, pTab->aCol[i].zCnName);
  }
  sqlite3_str_appendall(pSql, ")");
  zSql = sqlite3_str_finish(pSql);
  if( !zSql ) return SQLITE_NOMEM;
  rc = sqlite3_open(":memory:", &pEval);
  if( rc==SQLITE_OK ) rc = sqlite3_prepare_v2(pEval, zSql, -1, &pStmt, 0);
  sqlite3_free(zSql);
  if( rc==SQLITE_OK ) rc = bindIndexExprRow(pStmt, pTab, pRec, nRec, iPKey, intKey);
  if( rc==SQLITE_OK ) rc = sqlite3_step(pStmt);
  if( rc!=SQLITE_ROW ){
    sqlite3_finalize(pStmt);
    sqlite3_close(pEval);
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
    *ppKeep = sqlite3_malloc(n ? n : 1);
    if( !*ppKeep ){
      sqlite3_finalize(pStmt);
      sqlite3_close(pEval);
      return SQLITE_NOMEM;
    }
    if( n>0 ){
      memcpy(*ppKeep, eType==SQLITE_TEXT
             ? (const void*)sqlite3_value_text(pVal)
             : sqlite3_value_blob(pVal), n);
    }
    pOut->eType = eType;
    pOut->p = *ppKeep;
    pOut->n = n;
  }else{
    pOut->eType = SQLITE_NULL;
  }
  sqlite3_finalize(pStmt);
  sqlite3_close(pEval);
  return SQLITE_OK;
}

static int doltliteBuildIndexEntryWithExpr(
  sqlite3 *db,
  Index *pIdx,
  const u8 *pRec, int nRec,
  const i16 *aiColumn, int nIdxCol,
  KeyInfo *pKeyInfo,
  int iPKey, i64 intKey,
  const u8 *pTreeKey, int nTreeKey,
  u8 **ppSortKey, int *pnSortKey,
  u8 **ppIdxRec, int *pnIdxRec,
  int *pStorePayload
){
  DoltliteRecordInfo info;
  DoltliteSerialValue *aMem = 0;
  u8 **apKeep = 0;
  u8 *pIdxRec = 0;
  int nIdxRec = 0;
  int nOut = 0;
  int nAlloc;
  int hasRowid;
  int storePayload = 0;
  int i, rc;

  doltliteParseRecord(pRec, nRec, &info);
  if( info.nField==0 ) return SQLITE_CORRUPT;
  hasRowid = pIdx && pIdx->pTable && HasRowid(pIdx->pTable);

  nAlloc = nIdxCol + 1;
  aMem = sqlite3_malloc(nAlloc * (int)sizeof(DoltliteSerialValue));
  apKeep = sqlite3_malloc(nAlloc * (int)sizeof(u8*));
  if( !aMem || !apKeep ){
    sqlite3_free(aMem);
    sqlite3_free(apKeep);
    return SQLITE_NOMEM;
  }
  memset(aMem, 0, nAlloc * (int)sizeof(DoltliteSerialValue));
  memset(apKeep, 0, nAlloc * (int)sizeof(u8*));

  for(i=0; i<nIdxCol; i++){
    int col = aiColumn[i];
    if( col==XN_EXPR ){
      rc = evalIndexExprColumn(db, pIdx, pRec, nRec, iPKey, intKey,
                               i, &aMem[nOut], &apKeep[nOut]);
      if( rc!=SQLITE_OK ) goto expr_fail;
      nOut++;
    }else if( col==XN_ROWID || col==iPKey ){
      aMem[nOut].eType = SQLITE_INTEGER;
      aMem[nOut].i = intKey;
      nOut++;
    }else if( col>=0 && col<info.nField ){
      if( iPKey>=0 && col==iPKey ){
        aMem[nOut].eType = SQLITE_INTEGER;
        aMem[nOut].i = intKey;
      }else{
        rc = serialValueFromRecordField(pRec, nRec, &info, col, &aMem[nOut]);
        if( rc!=SQLITE_OK ) goto expr_fail;
      }
      nOut++;
    }
  }
  /* SQLite appends rowid even when an IPK is already an index term. */
  if( hasRowid ){
    aMem[nOut].eType = SQLITE_INTEGER;
    aMem[nOut].i = intKey;
    nOut++;
  }

  pIdxRec = doltliteBuildRecord(aMem, nOut, &nIdxRec);
  if( !pIdxRec ){
    rc = SQLITE_NOMEM;
    goto expr_fail;
  }
  for(i=0; i<nOut; i++) sqlite3_free(apKeep[i]);
  sqlite3_free(apKeep);
  sqlite3_free(aMem);
  apKeep = 0;
  aMem = 0;

  storePayload = indexKeyInfoNeedsPayload(pKeyInfo, pIdxRec, nIdxRec);
  rc = sortKeyFromRecordPrefixColl(pIdxRec, nIdxRec, 0, pKeyInfo,
                                    ppSortKey, pnSortKey);
  if( rc==SQLITE_OK && !hasRowid && pTreeKey && nTreeKey>0 ){
    u8 *pCombined = sqlite3_realloc(*ppSortKey, *pnSortKey + nTreeKey);
    if( !pCombined ){
      sqlite3_free(*ppSortKey);
      *ppSortKey = 0;
      *pnSortKey = 0;
      sqlite3_free(pIdxRec);
      return SQLITE_NOMEM;
    }
    memcpy(pCombined + *pnSortKey, pTreeKey, nTreeKey);
    *ppSortKey = pCombined;
    *pnSortKey += nTreeKey;
  }
  if( rc!=SQLITE_OK ){
    sqlite3_free(pIdxRec);
    return rc;
  }
  if( pStorePayload ) *pStorePayload = storePayload;
  if( storePayload && ppIdxRec ){
    *ppIdxRec = pIdxRec;
    if( pnIdxRec ) *pnIdxRec = nIdxRec;
  }else{
    sqlite3_free(pIdxRec);
  }
  return SQLITE_OK;

expr_fail:
  if( apKeep ){
    for(i=0; i<nAlloc; i++) sqlite3_free(apKeep[i]);
  }
  sqlite3_free(apKeep);
  sqlite3_free(aMem);
  sqlite3_free(pIdxRec);
  return rc;
}

static int doltliteBuildIndexEntry(
  sqlite3 *db,
  Index *pIdx,
  const u8 *pRec, int nRec,
  const i16 *aiColumn, int nIdxCol,
  KeyInfo *pKeyInfo,
  int iPKey, i64 intKey,
  const u8 *pTreeKey, int nTreeKey,
  u8 **ppSortKey, int *pnSortKey,
  u8 **ppIdxRec, int *pnIdxRec,
  int *pStorePayload
){
  DoltliteRecordInfo info;
  u8 *pIdxRec = 0;
  int nIdxRec = 0;
  u32 ipkType = 0;
  u32 ipkLen = 0;
  int useIpk = 0;
  int storePayload = 0;
  int rc;

  if( ppSortKey ) *ppSortKey = 0;
  if( pnSortKey ) *pnSortKey = 0;
  if( ppIdxRec ) *ppIdxRec = 0;
  if( pnIdxRec ) *pnIdxRec = 0;
  if( pStorePayload ) *pStorePayload = 0;

  if( indexColumnIsExpr(aiColumn, nIdxCol)
   || (iPKey<0 && pIdx && pIdx->pTable && HasRowid(pIdx->pTable)) ){
    return doltliteBuildIndexEntryWithExpr(
        db, pIdx, pRec, nRec, aiColumn, nIdxCol, pKeyInfo, iPKey, intKey,
        pTreeKey, nTreeKey, ppSortKey, pnSortKey, ppIdxRec, pnIdxRec,
        pStorePayload);
  }

  doltliteParseRecord(pRec, nRec, &info);
  if( info.nField==0 ) return SQLITE_CORRUPT;

  if( iPKey>=0 && iPKey<info.nField ){
    int st = info.aType[iPKey];
    if( st==0 || st==8 || st==9 ){
      useIpk = 1;
      dlIpkSerialType(intKey, &ipkType, &ipkLen);
    }
  }

  {
    int i, hdrLen = 0, bodyLen = 0;
    int nTotal;
    u8 *p;

    int nOutField;
    int *aFieldOrder = sqlite3_malloc((nIdxCol + 1) * sizeof(int));
    if( !aFieldOrder ){
      return SQLITE_NOMEM;
    }

    {
      int out = 0;
      for(i=0; i<nIdxCol; i++){
        int col = aiColumn[i];
        if( col>=0 && col<info.nField ){
          aFieldOrder[out++] = col;
        }
      }
      if( iPKey>=0 && iPKey<info.nField ){
        aFieldOrder[out++] = iPKey;
      }
      nOutField = out;
    }

    for(i=0; i<nOutField; i++){
      int col = aFieldOrder[i];
      int st = (useIpk && col==iPKey) ? (int)ipkType : info.aType[col];
      int flen = st>0 ? dlSerialTypeLen((u64)st) : 0;
      hdrLen += sqlite3VarintLen(st);
      bodyLen += flen;
    }

    {
      int tentative = hdrLen + 1;
      if( tentative > 126 ) tentative++;
      hdrLen = tentative;
    }

    nTotal = hdrLen + bodyLen;
    pIdxRec = sqlite3_malloc(nTotal);
    if( !pIdxRec ){
      sqlite3_free(aFieldOrder);
      return SQLITE_NOMEM;
    }

    p = pIdxRec;
    {
      int hs = hdrLen;
      if( hs <= 0x7f ){ *p++ = (u8)hs; }
      else{ *p++ = (u8)(0x80|(hs>>7)); *p++ = (u8)(hs&0x7f); }
    }
    for(i=0; i<nOutField; i++){
      int col = aFieldOrder[i];
      int st = (useIpk && col==iPKey) ? (int)ipkType : info.aType[col];
      p += sqlite3PutVarint(p, st);
    }

    for(i=0; i<nOutField; i++){
      int col = aFieldOrder[i];
      int st;
      int flen;
      if( useIpk && col==iPKey ){
        st = (int)ipkType;
        flen = (int)ipkLen;
        if( flen>0 ){
          dlIpkWriteBE(p, intKey, flen);
          p += flen;
        }
        continue;
      }
      st = info.aType[col];
      flen = st>0 ? dlSerialTypeLen((u64)st) : 0;
      if( flen>0 ){
        memcpy(p, pRec + info.aOffset[col], flen);
        p += flen;
      }
    }
    nIdxRec = (int)(p - pIdxRec);
    sqlite3_free(aFieldOrder);
  }

  storePayload = indexKeyInfoNeedsPayload(pKeyInfo, pIdxRec, nIdxRec);
  rc = sortKeyFromRecordPrefixColl(pIdxRec, nIdxRec, 0, pKeyInfo,
                                    ppSortKey, pnSortKey);
  /* WITHOUT ROWID secondary indexes suffix the table-tree key. */
  if( rc==SQLITE_OK && pIdx && pIdx->pTable
   && !HasRowid(pIdx->pTable) && pTreeKey && nTreeKey>0 ){
    u8 *pCombined = sqlite3_realloc(*ppSortKey, *pnSortKey + nTreeKey);
    if( !pCombined ){
      sqlite3_free(*ppSortKey);
      *ppSortKey = 0;
      *pnSortKey = 0;
      sqlite3_free(pIdxRec);
      return SQLITE_NOMEM;
    }
    memcpy(pCombined + *pnSortKey, pTreeKey, nTreeKey);
    *ppSortKey = pCombined;
    *pnSortKey += nTreeKey;
  }
  if( rc!=SQLITE_OK ){
    sqlite3_free(pIdxRec);
    return rc;
  }
  if( pStorePayload ) *pStorePayload = storePayload;
  if( storePayload && ppIdxRec ){
    *ppIdxRec = pIdxRec;
    if( pnIdxRec ) *pnIdxRec = nIdxRec;
  }else{
    sqlite3_free(pIdxRec);
  }
  return SQLITE_OK;
}

/* Apply old/new row values to one index mutmap. Shared so NOCASE/
** RTRIM/DESC match VDBE. */
/* A partial index holds only the rows its WHERE clause admits. The VDBE
** applies that test for ordinary DML; index maintenance that builds entries
** directly has to apply it too, or a merge files rows the index excludes and
** constraint checks read violations that do not exist. */
int doltlitePartialIndexLoad(
  sqlite3 *db,
  Index *pIdx,
  DoltlitePartialIndex *pOut
){
  int rc;
  memset(pOut, 0, sizeof(*pOut));
  if( !pIdx || !pIdx->pPartIdxWhere || !pIdx->pTable || !pIdx->pTable->zName ){
    return SQLITE_OK;
  }
  rc = doltlitePartialIndexWhereSql(db, pIdx, &pOut->zWhere);
  if( rc==SQLITE_OK ){
    rc = doltliteGetColumnNames(db, pIdx->pTable->zName, &pOut->cols);
    if( rc==SQLITE_OK ) pOut->colsInit = 1;
  }
  if( rc!=SQLITE_OK ) doltlitePartialIndexClear(pOut);
  return rc;
}

void doltlitePartialIndexClear(DoltlitePartialIndex *p){
  if( !p ) return;
  sqlite3_free(p->zWhere);
  if( p->colsInit ) doltliteFreeColInfo(&p->cols);
  if( p->pStmt ) sqlite3_finalize(p->pStmt);
  memset(p, 0, sizeof(*p));
}

static int indexRowInPartialIndex(
  sqlite3 *db,
  Index *pIdx,
  DoltlitePartialIndex *pPart,
  const u8 *pRec, int nRec,
  int *pIn
){
  *pIn = 1;
  if( !pPart->zWhere ) return SQLITE_OK;
  if( !pRec || nRec<=0 ) return SQLITE_OK;
  return doltlitePartialIndexMatchesRecord(
      db, pIdx, pPart->zWhere, pRec, nRec,
      pPart->colsInit ? &pPart->cols : 0, &pPart->pStmt, pIn);
}

int doltliteIndexMutMapRowDelta(
  sqlite3 *db,
  Index *pIdx,
  ProllyMutMap *pMap,
  const i16 *aiColumn, int nIdxCol,
  KeyInfo *pKeyInfo,
  int iPKey, i64 intKey,
  const u8 *pTreeKey, int nTreeKey,
  const u8 *pOldVal, int nOldVal,
  const u8 *pNewVal, int nNewVal,
  DoltlitePartialIndex *pPart
){
  DoltlitePartialIndex partLocal;
  int partLocalInit = 0;
  int oldIn = 1, newIn = 1;
  int rc = SQLITE_OK;

  if( !pMap ) return SQLITE_MISUSE;

  if( pIdx && pIdx->pPartIdxWhere ){
    if( !pPart ){
      rc = doltlitePartialIndexLoad(db, pIdx, &partLocal);
      if( rc!=SQLITE_OK ) return rc;
      partLocalInit = 1;
      pPart = &partLocal;
    }
    if( rc==SQLITE_OK ){
      rc = indexRowInPartialIndex(db, pIdx, pPart, pOldVal, nOldVal, &oldIn);
    }
    if( rc==SQLITE_OK ){
      rc = indexRowInPartialIndex(db, pIdx, pPart, pNewVal, nNewVal, &newIn);
    }
    if( rc!=SQLITE_OK ){
      if( partLocalInit ) doltlitePartialIndexClear(&partLocal);
      return rc;
    }
    if( !oldIn ){ pOldVal = 0; nOldVal = 0; }
    if( !newIn ){ pNewVal = 0; nNewVal = 0; }
  }

  if( pOldVal && nOldVal>0 ){
    u8 *pSK = 0;
    int nSK = 0;
    rc = doltliteBuildIndexEntry(
        db, pIdx, pOldVal, nOldVal, aiColumn, nIdxCol, pKeyInfo, iPKey, intKey,
        pTreeKey, nTreeKey, &pSK, &nSK, 0, 0, 0);
    if( rc==SQLITE_OK ){
      rc = prollyMutMapDelete(pMap, pSK, nSK, 0);
    }
    sqlite3_free(pSK);
    if( rc!=SQLITE_OK ){
      if( partLocalInit ) doltlitePartialIndexClear(&partLocal);
      return rc;
    }
  }

  if( pNewVal && nNewVal>0 ){
    u8 *pSK = 0;
    u8 *pRec = 0;
    int nSK = 0, nRec = 0, store = 0;
    rc = doltliteBuildIndexEntry(
        db, pIdx, pNewVal, nNewVal, aiColumn, nIdxCol, pKeyInfo, iPKey, intKey,
        pTreeKey, nTreeKey, &pSK, &nSK, &pRec, &nRec, &store);
    if( rc==SQLITE_OK ){
      if( store ){
        rc = prollyMutMapInsert(pMap, pSK, nSK, 0, pRec, nRec);
      }else{
        rc = prollyMutMapInsert(pMap, pSK, nSK, 0, 0, 0);
      }
    }
    sqlite3_free(pSK);
    sqlite3_free(pRec);
  }
  if( partLocalInit ) doltlitePartialIndexClear(&partLocal);
  return rc;
}

int doltliteIndexApplyRowDelta(
  sqlite3 *db,
  ChunkStore *cs,
  ProllyCache *cache,
  ProllyHash *pIdxRoot,
  u8 idxFlags,
  Index *pIdx,
  int iPKey, i64 intKey,
  const u8 *pTreeKey, int nTreeKey,
  const u8 *pOldVal, int nOldVal,
  const u8 *pNewVal, int nNewVal
){
  KeyInfo *pKeyInfo = 0;
  ProllyMutMap mm;
  ProllyMutator mut;
  int rc;

  if( !cs || !cache || !pIdxRoot || !pIdx ) return SQLITE_MISUSE;
  if( (!pOldVal || nOldVal<=0) && (!pNewVal || nNewVal<=0) ) return SQLITE_OK;

  pKeyInfo = doltliteKeyInfoOfIndex(db, pIdx);
  if( !pKeyInfo ) return SQLITE_NOMEM;

  rc = prollyMutMapInit(&mm, 0);
  if( rc!=SQLITE_OK ){
    sqlite3KeyInfoUnref(pKeyInfo);
    return rc;
  }

  rc = doltliteIndexMutMapRowDelta(
      db, pIdx, &mm, pIdx->aiColumn, pIdx->nKeyCol, pKeyInfo,
      iPKey, intKey, pTreeKey, nTreeKey,
      pOldVal, nOldVal, pNewVal, nNewVal, 0);
  if( rc==SQLITE_OK && !prollyMutMapIsEmpty(&mm) ){
    memset(&mut, 0, sizeof(mut));
    mut.pStore = cs;
    mut.pCache = cache;
    mut.oldRoot = *pIdxRoot;
    mut.pEdits = &mm;
    mut.flags = idxFlags ? idxFlags : (u8)PROLLY_NODE_BLOBKEY;
    rc = prollyMutateFlush(&mut);
    if( rc==SQLITE_OK ) *pIdxRoot = mut.newRoot;
  }

  prollyMutMapFree(&mm);
  sqlite3KeyInfoUnref(pKeyInfo);
  return rc;
}

typedef struct RowMergeCtx RowMergeCtx;
struct RowMergeCtx {
  sqlite3 *db;
  ProllyMutMap *pEdits;
  const MergeRowPolicy *pPolicy;
  u8 isIntKey;
  MergeIndexInfo *aIndexes;
  int nIndexes;
  int nConflicts;

  DoltliteConflictRow *aConflicts;
  int nConflictsAlloc;
};

typedef struct RecField RecField;
struct RecField { u64 st; int off; int len; };

static int parseRecordFields(const u8 *pRec, int nRec,
                             RecField **ppFields, int *pnFields){
  const u8 *pPos, *pEnd, *pHdrEnd;
  u64 hdrSize;
  int hdrBytes, nFields = 0, nAlloc = 0;
  i64 bodyOff;
  RecField *aFields = 0;

  if(!pRec || nRec<1) { *ppFields=0; *pnFields=0; return 0; }
  pPos = pRec; pEnd = pRec + nRec;
  hdrBytes = dlReadVarint(pPos, pEnd, &hdrSize);
  if(hdrBytes<=0){ *ppFields=0; *pnFields=0; return -1; }
  pPos += hdrBytes;
  if((u64)hdrBytes > hdrSize || hdrSize > (u64)nRec){
    *ppFields=0; *pnFields=0; return -1;
  }
  pHdrEnd = pRec + (int)hdrSize;
  bodyOff = (i64)hdrSize;

  while(pPos < pHdrEnd && pPos < pEnd){
    u64 st; int stBytes, sz;
    stBytes = dlReadVarint(pPos, pHdrEnd, &st);
    if(stBytes<=0){
      sqlite3_free(aFields);
      *ppFields=0; *pnFields=0;
      return -1;
    }
    pPos += stBytes;
    sz = dlSerialTypeLen(st);
    if(sz < 0 || bodyOff + (i64)sz > (i64)nRec){
      sqlite3_free(aFields);
      *ppFields=0; *pnFields=0;
      return -1;
    }

    if( DOLTLITE_GROW_ARRAY(&aFields, &nAlloc, nFields+1, 16)!=SQLITE_OK ){
      sqlite3_free(aFields);
      *ppFields=0; *pnFields=0;
      return -1;
    }
    aFields[nFields].st = st;
    aFields[nFields].off = (int)bodyOff;
    aFields[nFields].len = sz;
    nFields++;
    bodyOff += sz;
  }

  *ppFields = aFields;
  *pnFields = nFields;
  return nFields;
}

/* sqlite_master fields 1/2 are name and tbl_name. Match the base
** row: it carries the ancestor name the policy was built from. */
static int catalogRowNamedInList(
  const char **azNames,
  int nNames,
  const u8 *pBase, int nBase
){
  RecField *aBase = 0;
  int nBaseF = 0;
  int matched = 0;
  int i, f;

  if( !azNames || nNames<=0 ) return 0;
  if( !pBase || nBase<=0 ) return 0;
  /* Field count, or -1; not an SQLITE_ code. */
  if( parseRecordFields(pBase, nBase, &aBase, &nBaseF) < 0 ) return 0;
  for(f=1; f<=2 && !matched; f++){
    if( f>=nBaseF ) break;
    for(i=0; i<nNames && !matched; i++){
      const char *z = azNames[i];
      int n;
      if( !z ) continue;
      n = (int)strlen(z);
      if( aBase[f].len==n
       && sqlite3_strnicmp((const char*)(pBase + aBase[f].off), z, n)==0 ){
        matched = 1;
      }
    }
  }
  sqlite3_free(aBase);
  return matched;
}

static int catalogRowNamedByPolicy(
  const MergeRowPolicy *pPolicy,
  const u8 *pBase, int nBase
){
  if( !pPolicy ) return 0;
  return catalogRowNamedInList(
      pPolicy->azRenameOverDrop, pPolicy->nRenameOverDrop, pBase, nBase);
}

static int catalogRowNamedByDualRename(
  const MergeRowPolicy *pPolicy,
  const u8 *pBase, int nBase
){
  if( !pPolicy ) return 0;
  return catalogRowNamedInList(
      pPolicy->azDualRename, pPolicy->nDualRename, pBase, nBase);
}

static int fieldEquals(const u8 *pRecA, const RecField *fA,
                       const u8 *pRecB, const RecField *fB){
  if(fA->st != fB->st) return 1;
  if(fA->len != fB->len) return 1;
  if(fA->len==0) return 0;
  return memcmp(pRecA + fA->off, pRecB + fB->off, fA->len);
}

static int recordsEqualPrefix(
  const u8 *pA,
  int nA,
  const u8 *pB,
  int nB,
  int nField,
  int *pbEqual
){
  static const RecField nullField = { 0, 0, 0 };
  RecField *aA = 0, *aB = 0;
  int nFieldA = 0, nFieldB = 0;
  int i;

  *pbEqual = 0;
  if( parseRecordFields(pA, nA, &aA, &nFieldA)<0 ) return SQLITE_CORRUPT;
  if( parseRecordFields(pB, nB, &aB, &nFieldB)<0 ){
    sqlite3_free(aA);
    return SQLITE_CORRUPT;
  }
  *pbEqual = 1;
  for(i=0; i<nField; i++){
    const RecField *pFieldA = i<nFieldA ? &aA[i] : &nullField;
    const RecField *pFieldB = i<nFieldB ? &aB[i] : &nullField;
    if( fieldEquals(pA, pFieldA, pB, pFieldB)!=0 ){
      *pbEqual = 0;
      break;
    }
  }
  sqlite3_free(aA);
  sqlite3_free(aB);
  return SQLITE_OK;
}

typedef struct MergeWinner MergeWinner;
struct MergeWinner { const u8 *pRec; RecField *pField; };

static u8 *buildMergedRecord(MergeWinner *aWinners, int nFields, int *pnOut){
  int hdrSize = 0, bodySize = 0, pos, i;
  u8 *result;

  for(i=0; i<nFields; i++){
    u64 st = aWinners[i].pField->st;
    if(st <= 0x7f) hdrSize += 1;
    else if(st <= 0x3fff) hdrSize += 2;
    else if(st <= 0x1fffff) hdrSize += 3;
    else hdrSize += 4;
    bodySize += aWinners[i].pField->len;
  }

  { int tentative = hdrSize + 1;
    if(tentative > 0x7f) tentative++;
    hdrSize = tentative;
  }

  result = sqlite3_malloc(hdrSize + bodySize);
  if(!result){ *pnOut = 0; return 0; }

  pos = 0;
  { u64 hs = (u64)hdrSize;
    if(hs <= 0x7f){ result[pos++] = (u8)hs; }
    else{ result[pos++] = (u8)(0x80 | (hs>>7)); result[pos++] = (u8)(hs&0x7f); }
  }

  for(i=0; i<nFields; i++){
    u64 st = aWinners[i].pField->st;
    if(st <= 0x7f){
      result[pos++] = (u8)st;
    }else if(st <= 0x3fff){
      result[pos++] = (u8)(0x80 | (st>>7));
      result[pos++] = (u8)(st&0x7f);
    }else if(st <= 0x1fffff){
      result[pos++] = (u8)(0x80 | (st>>14));
      result[pos++] = (u8)(0x80 | ((st>>7)&0x7f));
      result[pos++] = (u8)(st&0x7f);
    }else{
      result[pos++] = (u8)(0x80 | (st>>21));
      result[pos++] = (u8)(0x80 | ((st>>14)&0x7f));
      result[pos++] = (u8)(0x80 | ((st>>7)&0x7f));
      result[pos++] = (u8)(st&0x7f);
    }
  }

  for(i=0; i<nFields; i++){
    if(aWinners[i].pField->len > 0){
      memcpy(result + pos, aWinners[i].pRec + aWinners[i].pField->off,
             aWinners[i].pField->len);
      pos += aWinners[i].pField->len;
    }
  }

  *pnOut = pos;

#ifndef NDEBUG
  {
    int nfCheck = 0;
    RecField *aCheck = 0;
    if( parseRecordFields(result, pos, &aCheck, &nfCheck) >= 0 ){
      assert( nfCheck == nFields );
      sqlite3_free(aCheck);
    }
  }
#endif

  return result;
}

static u8 *tryCellMerge(
  const u8 *pBase, int nBase,
  const u8 *pOurs, int nOurs,
  const u8 *pTheirs, int nTheirs,
  int *pnMerged
){
  RecField *aBase=0, *aOurs=0, *aTheirs=0;
  int nfBase=0, nfOurs=0, nfTheirs=0;
  int nfMax, i;
  u8 *result = 0;

  if(parseRecordFields(pBase, nBase, &aBase, &nfBase)<0) goto fail;
  if(parseRecordFields(pOurs, nOurs, &aOurs, &nfOurs)<0) goto fail;
  if(parseRecordFields(pTheirs, nTheirs, &aTheirs, &nfTheirs)<0) goto fail;

  nfMax = nfBase;
  if(nfOurs > nfMax) nfMax = nfOurs;
  if(nfTheirs > nfMax) nfMax = nfTheirs;

  {
    MergeWinner *winners;
    /* Past stored width is a trailing NULL (SQLite omits them). Treat
    ** absent and explicit NULL the same so dual ADD COLUMN can merge. */
    static const RecField kNullField = { 0, 0, 0 };
    int nEmit = 0;

    winners = sqlite3_malloc(nfMax * (int)sizeof(*winners));
    if(!winners) goto fail;

    for(i=0; i<nfMax; i++){
      RecField *fB = (i<nfBase)   ? &aBase[i]   : (RecField*)&kNullField;
      RecField *fO = (i<nfOurs)   ? &aOurs[i]   : (RecField*)&kNullField;
      RecField *fT = (i<nfTheirs) ? &aTheirs[i] : (RecField*)&kNullField;
      int oursChanged   = fieldEquals(pBase, fB, pOurs, fO)!=0;
      int theirsChanged = fieldEquals(pBase, fB, pTheirs, fT)!=0;

      if(!theirsChanged){

        winners[i].pRec = pOurs; winners[i].pField = fO;
      }else if(!oursChanged){

        winners[i].pRec = pTheirs; winners[i].pField = fT;
      }else if(fieldEquals(pOurs, fO, pTheirs, fT)==0){

        winners[i].pRec = pOurs; winners[i].pField = fO;
      }else{
        sqlite3_free(winners); goto fail;
      }
      if( winners[i].pField->st != 0 ) nEmit = i+1;
    }

    /* Drop trailing NULLs for canonical encoding. */
    result = buildMergedRecord(winners, nEmit, pnMerged);
    sqlite3_free(winners);
  }

  sqlite3_free(aBase);
  sqlite3_free(aOurs);
  sqlite3_free(aTheirs);
  return result;

fail:
  sqlite3_free(aBase);
  sqlite3_free(aOurs);
  sqlite3_free(aTheirs);
  *pnMerged = 0;
  return 0;
}

static int rowMergeCallback(void *pCtx, const ThreeWayChange *pChange){
  RowMergeCtx *ctx = (RowMergeCtx*)pCtx;
  int rc = SQLITE_OK;

  switch( pChange->type ){
    case THREE_WAY_LEFT_ADD:
    case THREE_WAY_LEFT_MODIFY:
    case THREE_WAY_LEFT_DELETE:

      break;

    case THREE_WAY_RIGHT_ADD:

      rc = prollyMutMapInsert(ctx->pEdits,
          pChange->pKey, pChange->nKey, pChange->intKey,
          pChange->pTheirVal, pChange->nTheirVal);
      if( rc==SQLITE_OK && ctx->nIndexes>0
       && pChange->pTheirVal && pChange->nTheirVal>0 ){
        int ix;
        for(ix=0; ix<ctx->nIndexes && rc==SQLITE_OK; ix++){
          MergeIndexInfo *mi = &ctx->aIndexes[ix];
          rc = doltliteIndexMutMapRowDelta(
              ctx->db, mi->pIdx, mi->pEdits, mi->aiColumn, mi->nColumn,
              mi->pKeyInfo, mi->iPKey, pChange->intKey,
              pChange->pKey, pChange->nKey,
              0, 0, pChange->pTheirVal, pChange->nTheirVal, &mi->part);
        }
      }
      break;

    case THREE_WAY_RIGHT_MODIFY:

      rc = prollyMutMapInsert(ctx->pEdits,
          pChange->pKey, pChange->nKey, pChange->intKey,
          pChange->pTheirVal, pChange->nTheirVal);
      if( rc==SQLITE_OK && ctx->nIndexes>0 ){
        int ix;
        for(ix=0; ix<ctx->nIndexes && rc==SQLITE_OK; ix++){
          MergeIndexInfo *mi = &ctx->aIndexes[ix];
          rc = doltliteIndexMutMapRowDelta(
              ctx->db, mi->pIdx, mi->pEdits, mi->aiColumn, mi->nColumn,
              mi->pKeyInfo, mi->iPKey, pChange->intKey,
              pChange->pKey, pChange->nKey,
              pChange->pBaseVal, pChange->nBaseVal,
              pChange->pTheirVal, pChange->nTheirVal, &mi->part);
        }
      }
      break;

    case THREE_WAY_RIGHT_DELETE:

      rc = prollyMutMapDelete(ctx->pEdits,
          pChange->pKey, pChange->nKey, pChange->intKey);
      if( rc==SQLITE_OK && ctx->nIndexes>0
       && pChange->pBaseVal && pChange->nBaseVal>0 ){
        int ix;
        for(ix=0; ix<ctx->nIndexes && rc==SQLITE_OK; ix++){
          MergeIndexInfo *mi = &ctx->aIndexes[ix];
          rc = doltliteIndexMutMapRowDelta(
              ctx->db, mi->pIdx, mi->pEdits, mi->aiColumn, mi->nColumn,
              mi->pKeyInfo, mi->iPKey, pChange->intKey,
              pChange->pKey, pChange->nKey,
              pChange->pBaseVal, pChange->nBaseVal, 0, 0, &mi->part);
        }
      }
      break;

    case THREE_WAY_CONVERGENT:

      break;

    case THREE_WAY_CONFLICT_MM: {

      u8 *pMerged = 0;
      int nMerged = 0;

      if( pChange->pBaseVal && pChange->nBaseVal>0
       && pChange->pOurVal && pChange->nOurVal>0
       && pChange->pTheirVal && pChange->nTheirVal>0 ){
        pMerged = tryCellMerge(
            pChange->pBaseVal, pChange->nBaseVal,
            pChange->pOurVal, pChange->nOurVal,
            pChange->pTheirVal, pChange->nTheirVal,
            &nMerged);
      }

      if( pMerged ){
        rc = prollyMutMapInsert(ctx->pEdits,
            pChange->pKey, pChange->nKey, pChange->intKey,
            pMerged, nMerged);
        if( rc==SQLITE_OK && ctx->nIndexes>0 ){
          int ix;
          for(ix=0; ix<ctx->nIndexes && rc==SQLITE_OK; ix++){
            MergeIndexInfo *mi = &ctx->aIndexes[ix];
            rc = doltliteIndexMutMapRowDelta(
                ctx->db, mi->pIdx, mi->pEdits, mi->aiColumn, mi->nColumn,
                mi->pKeyInfo, mi->iPKey, pChange->intKey,
                pChange->pKey, pChange->nKey,
                pChange->pOurVal, pChange->nOurVal, pMerged, nMerged, &mi->part);
          }
        }
        sqlite3_free(pMerged);
        break;
      }

      if( catalogRowNamedByDualRename(ctx->pPolicy,
                                      pChange->pBaseVal, pChange->nBaseVal) ){
        break;
      }

    }
    deliberate_fall_through
    case THREE_WAY_CONFLICT_DM: {

      if( pChange->type==THREE_WAY_CONFLICT_DM && ctx->pPolicy
       && ctx->pPolicy->nDeleteCompareFields>0 ){
        const u8 *pSurv = pChange->pOurVal ? pChange->pOurVal
                                           : pChange->pTheirVal;
        int nSurv = pChange->pOurVal ? pChange->nOurVal : pChange->nTheirVal;
        int bSharedEqual = 0;
        rc = recordsEqualPrefix(
            pChange->pBaseVal, pChange->nBaseVal,
            pSurv, nSurv, ctx->pPolicy->nDeleteCompareFields,
            &bSharedEqual);
        if( rc!=SQLITE_OK ) return rc;
        if( bSharedEqual ){
          if( pChange->pOurVal && !pChange->pTheirVal ){
            rc = prollyMutMapDelete(ctx->pEdits,
                pChange->pKey, pChange->nKey, pChange->intKey);
            if( rc==SQLITE_OK && ctx->nIndexes>0 ){
              int ix;
              for(ix=0; ix<ctx->nIndexes && rc==SQLITE_OK; ix++){
                MergeIndexInfo *mi = &ctx->aIndexes[ix];
                rc = doltliteIndexMutMapRowDelta(
                    ctx->db, mi->pIdx, mi->pEdits,
                    mi->aiColumn, mi->nColumn, mi->pKeyInfo, mi->iPKey,
                    pChange->intKey, pChange->pKey, pChange->nKey,
                    pChange->pOurVal, pChange->nOurVal, 0, 0, &mi->part);
              }
            }
          }
          break;
        }
      }

      /* Policy-named catalog row (rename vs drop): rename wins, as Dolt.
      ** Inferring here would also pick a winner for a dual rename. */
      if( pChange->type==THREE_WAY_CONFLICT_DM
       && catalogRowNamedByPolicy(ctx->pPolicy,
                                  pChange->pBaseVal, pChange->nBaseVal) ){
        const u8 *pSurv = pChange->pOurVal ? pChange->pOurVal
                                           : pChange->pTheirVal;
        int nSurv = pChange->pOurVal ? pChange->nOurVal : pChange->nTheirVal;
        if( pSurv && nSurv>0 ){
          rc = prollyMutMapInsert(ctx->pEdits,
              pChange->pKey, pChange->nKey, pChange->intKey, pSurv, nSurv);
          break;
        }
      }

      rc = DOLTLITE_GROW_ARRAY(&ctx->aConflicts, &ctx->nConflictsAlloc,
                                ctx->nConflicts + 1, 16);
      if( rc!=SQLITE_OK ) return rc;
      {
        DoltliteConflictRow *cr = &ctx->aConflicts[ctx->nConflicts];
        memset(cr, 0, sizeof(*cr));
        cr->intKey = pChange->intKey;
        if( pChange->pKey && pChange->nKey>0 ){
          rc = doltliteDupBytes(pChange->pKey, pChange->nKey, &cr->pKey);
          if( rc!=SQLITE_OK ) return rc;
          cr->nKey = pChange->nKey;
        }
        if( pChange->pBaseVal && pChange->nBaseVal>0 ){
          rc = doltliteDupBytes(pChange->pBaseVal, pChange->nBaseVal, &cr->pBaseVal);
          if( rc!=SQLITE_OK ){
            sqlite3_free(cr->pKey);
            memset(cr, 0, sizeof(*cr));
            return rc;
          }
          cr->nBaseVal = pChange->nBaseVal;
        }
        if( pChange->pOurVal && pChange->nOurVal>0 ){
          rc = doltliteDupBytes(pChange->pOurVal, pChange->nOurVal, &cr->pOurVal);
          if( rc!=SQLITE_OK ){
            sqlite3_free(cr->pKey);
            sqlite3_free(cr->pBaseVal);
            memset(cr, 0, sizeof(*cr));
            return rc;
          }
          cr->nOurVal = pChange->nOurVal;
        }
        if( pChange->pTheirVal && pChange->nTheirVal>0 ){
          rc = doltliteDupBytes(pChange->pTheirVal, pChange->nTheirVal, &cr->pTheirVal);
          if( rc!=SQLITE_OK ){
            sqlite3_free(cr->pKey);
            sqlite3_free(cr->pBaseVal);
            sqlite3_free(cr->pOurVal);
            memset(cr, 0, sizeof(*cr));
            return rc;
          }
          cr->nTheirVal = pChange->nTheirVal;
        }
        ctx->nConflicts++;
      }
      break;
    }
  }
  return rc;
}

/* Other side changed this field in a pre-existing row. Drop vs that
** edit is a Dolt conflict; adds/removes and other columns are not. */
int mergeRowEditsColumn(
  sqlite3 *db,
  const ProllyHash *pAncRoot,
  const ProllyHash *pOtherRoot,
  u8 ancFlags,
  u8 otherFlags,
  int iField,
  int bNonNullOnly,
  int *pbEdited
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  ProllyDiffIter iter;
  ProllyDiffChange *pChange = 0;
  int rc;

  *pbEdited = 0;
  if( !cs || !pCache || iField<0 ) return SQLITE_OK;
  if( prollyHashIsEmpty(pAncRoot) || prollyHashIsEmpty(pOtherRoot) ){
    return SQLITE_OK;
  }
  memset(&iter, 0, sizeof(iter));
  rc = prollyDiffIterOpen(&iter, cs, pCache, pAncRoot, pOtherRoot,
                          ancFlags, otherFlags);
  if( rc!=SQLITE_OK ) return rc;

  while( (rc = prollyDiffIterStep(&iter, &pChange))==SQLITE_ROW ){
    RecField *aOld = 0;
    RecField *aNew = 0;
    static const RecField kNullField = { 0, 0, 0 };
    int nOld = 0, nNew = 0;
    if( pChange->type!=PROLLY_DIFF_MODIFY ) continue;
    if( !pChange->pOldVal || !pChange->pNewVal ) continue;
    if( parseRecordFields(pChange->pOldVal, pChange->nOldVal, &aOld, &nOld)<0 ){
      continue;
    }
    if( parseRecordFields(pChange->pNewVal, pChange->nNewVal, &aNew, &nNew)<0 ){
      sqlite3_free(aOld);
      continue;
    }
    if( (!bNonNullOnly || (iField<nNew && aNew[iField].st!=0))
     && fieldEquals(pChange->pOldVal,
                    iField<nOld ? &aOld[iField] : (RecField*)&kNullField,
                    pChange->pNewVal,
                    iField<nNew ? &aNew[iField] : (RecField*)&kNullField)!=0 ){
      *pbEdited = 1;
    }
    sqlite3_free(aOld);
    sqlite3_free(aNew);
    if( *pbEdited ) break;
  }
  prollyDiffIterClose(&iter);
  return rc==SQLITE_ROW || rc==SQLITE_DONE ? SQLITE_OK : rc;
}

int canFastMerge(
  sqlite3 *db,
  const char *zName,
  int schemaUnchangedBothSides
){
  Table *pTab;
  FKey *pFK;
  int i;

  if( !schemaUnchangedBothSides ) return 0;
  if( !zName || !db ) return 0;

  pTab = sqlite3FindTable(db, zName, 0);
  if( !pTab ) return 0;

  if( pTab->pIndex ) return 0;
  if( pTab->pCheck && pTab->pCheck->nExpr>0 ) return 0;

  for(i=0; i<pTab->nCol; i++){
    Column *pCol = &pTab->aCol[i];
    if( (pCol->colFlags & COLFLAG_PRIMKEY)!=0 ) continue;
    if( pCol->notNull!=OE_None ) return 0;
  }

  for(pFK=pTab->u.tab.pFKey; pFK; pFK=pFK->pNextFrom){
    if( pFK->aAction[0]!=OE_None || pFK->aAction[1]!=OE_None ) return 0;
  }
  for(pFK=sqlite3FkReferences(pTab); pFK; pFK=pFK->pNextTo){
    if( pFK->aAction[0]!=OE_None || pFK->aAction[1]!=OE_None ) return 0;
  }

  return 1;
}

static void freeRowMergeCtx(RowMergeCtx *ctx){
  int i;
  for(i=0; i<ctx->nConflicts; i++){
    doltliteConflictRowFree(&ctx->aConflicts[i]);
  }
  sqlite3_free(ctx->aConflicts);
  if( ctx->pEdits ){
    prollyMutMapFree(ctx->pEdits);
    sqlite3_free(ctx->pEdits);
  }
}

int mergeTableRows(
  sqlite3 *db,
  const ProllyHash *pAncRoot,
  const ProllyHash *pOursRoot,
  const ProllyHash *pTheirsRoot,
  u8 flags,
  u8 ancFlags,
  u8 theirsFlags,
  ProllyHash *pMergedRoot,
  int *pnConflicts,
  DoltliteConflictRow **ppConflicts,
  MergeIndexInfo *aIndexes,
  int nIndexes,
  const MergeRowPolicy *pPolicy
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *cache = doltliteGetCache(db);

  RowMergeCtx ctx;
  ProllyMutator mut;
  int rc;
  int i;

  if( ((flags ^ theirsFlags) & PROLLY_NODE_INTKEY)!=0 ){
    return SQLITE_ERROR;
  }
  if( !prollyHashIsEmpty(pAncRoot)
   && ((flags ^ ancFlags) & PROLLY_NODE_INTKEY)!=0 ){
    return SQLITE_ERROR;
  }

  memset(&ctx, 0, sizeof(ctx));
  ctx.db = db;
  ctx.isIntKey = (flags & PROLLY_NODE_INTKEY) ? 1 : 0;
  ctx.pPolicy = pPolicy;
  ctx.aIndexes = aIndexes;
  ctx.nIndexes = nIndexes;
  ctx.pEdits = sqlite3_malloc(sizeof(ProllyMutMap));
  if( !ctx.pEdits ) return SQLITE_NOMEM;
  rc = prollyMutMapInit(ctx.pEdits, ctx.isIntKey);
  if( rc!=SQLITE_OK ){ sqlite3_free(ctx.pEdits); return rc; }

  for(i=0; i<nIndexes; i++){
    aIndexes[i].pEdits = sqlite3_malloc(sizeof(ProllyMutMap));
    if( !aIndexes[i].pEdits ){ rc = SQLITE_NOMEM; goto merge_err; }
    rc = prollyMutMapInit(aIndexes[i].pEdits, 0);
    if( rc!=SQLITE_OK ) goto merge_err;
  }

  rc = prollyThreeWayDiff(cs, cache, pAncRoot, pOursRoot, pTheirsRoot,
                          ancFlags, flags, theirsFlags,
                          rowMergeCallback, &ctx);
  if( rc!=SQLITE_OK ) goto merge_err;

  if( !prollyMutMapIsEmpty(ctx.pEdits) ){
    memset(&mut, 0, sizeof(mut));
    mut.pStore = cs;
    mut.pCache = cache;
    memcpy(&mut.oldRoot, pOursRoot, sizeof(ProllyHash));
    mut.pEdits = ctx.pEdits;
    mut.flags = flags;
    rc = prollyMutateFlush(&mut);
    if( rc==SQLITE_OK ){
      memcpy(pMergedRoot, &mut.newRoot, sizeof(ProllyHash));
    }
  }else{
    memcpy(pMergedRoot, pOursRoot, sizeof(ProllyHash));
  }

  for(i=0; i<nIndexes && rc==SQLITE_OK; i++){
    if( !prollyMutMapIsEmpty(aIndexes[i].pEdits) ){
      ProllyMutator idxMut;
      memset(&idxMut, 0, sizeof(idxMut));
      idxMut.pStore = cs;
      idxMut.pCache = cache;
      memcpy(&idxMut.oldRoot, &aIndexes[i].oursRoot, sizeof(ProllyHash));
      idxMut.pEdits = aIndexes[i].pEdits;
      idxMut.flags = PROLLY_NODE_BLOBKEY;
      rc = prollyMutateFlush(&idxMut);
      if( rc==SQLITE_OK ){
        memcpy(&aIndexes[i].mergedRoot, &idxMut.newRoot, sizeof(ProllyHash));
      }
    }else{
      memcpy(&aIndexes[i].mergedRoot, &aIndexes[i].oursRoot, sizeof(ProllyHash));
    }
  }

  *pnConflicts = ctx.nConflicts;
  *ppConflicts = ctx.aConflicts;
  ctx.aConflicts = 0;
  ctx.nConflicts = 0;

merge_err:
  for(i=0; i<nIndexes; i++){
    if( aIndexes[i].pEdits ){
      prollyMutMapFree(aIndexes[i].pEdits);
      sqlite3_free(aIndexes[i].pEdits);
      aIndexes[i].pEdits = 0;
    }
  }
  freeRowMergeCtx(&ctx);
  return rc;
}


#endif
