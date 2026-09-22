#ifdef DOLTLITE_PROLLY

#include "doltlite_merge_int.h"

/* Index key encoding for merge and DML row-delta. VIRTUAL generated
** columns are not record fields; table column numbers after them are not
** record field numbers. This module maps storage columns and evaluates
** VIRTUAL expressions so a rebuild matches incremental maintenance. */

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

static int indexNeedsExprBuild(Index *pIdx, const i16 *aiColumn, int nIdxCol){
  Table *pTab = pIdx ? pIdx->pTable : 0;
  if( indexColumnIsExpr(aiColumn, nIdxCol) ) return 1;
  /* VIRTUAL columns are not record fields, so table column numbers after
  ** them are not record field numbers. Evaluate those keys through the
  ** expression path, which maps storage columns and computes VIRTUAL. */
  if( pTab && (pTab->tabFlags & TF_HasVirtual) ) return 1;
  return 0;
}

static int indexExprToSql(sqlite3_str *p, const Expr *pExpr, Table *pTab);

int doltliteColumnIsVirtual(const Table *pTab, int iCol){
#ifndef SQLITE_OMIT_GENERATED_COLUMNS
  return (pTab->aCol[iCol].colFlags & COLFLAG_VIRTUAL)!=0;
#else
  (void)pTab;
  (void)iCol;
  return 0;
#endif
}

/* Stored columns are parameters, in declared order. VIRTUAL columns are
** projected from their generation expressions so a later VIRTUAL column
** can read an earlier one. Binding those columns as NULL made
** UNIQUE(doubled+1) store a NULL key and drop the merged row. */
static int indexExprSourceSql(Table *pTab, char **pzSql){
  sqlite3_str *pInner;
  char *zCur;
  int i, nBind = 0;

  *pzSql = 0;
  pInner = sqlite3_str_new(0);
  sqlite3_str_appendall(pInner, "SELECT ");
  for(i=0; i<pTab->nCol; i++){
    if( doltliteColumnIsVirtual(pTab, i) ) continue;
    if( nBind ) sqlite3_str_appendall(pInner, ", ");
    nBind++;
    sqlite3_str_appendf(pInner, "?%d AS \"%w\"", nBind, pTab->aCol[i].zCnName);
  }
  if( nBind==0 ) sqlite3_str_appendall(pInner, "NULL AS \"_\"");
  zCur = sqlite3_str_finish(pInner);
  if( !zCur ) return SQLITE_NOMEM;
  for(i=0; i<pTab->nCol; i++){
    sqlite3_str *pWrap;
    char *zWrap;
    Expr *pExpr;
    if( !doltliteColumnIsVirtual(pTab, i) ) continue;
    pExpr = sqlite3ColumnExpr(pTab, &pTab->aCol[i]);
    pWrap = sqlite3_str_new(0);
    sqlite3_str_appendall(pWrap, "SELECT *, (");
    if( indexExprToSql(pWrap, pExpr, pTab)!=SQLITE_OK ){
      sqlite3_free(sqlite3_str_finish(pWrap));
      sqlite3_free(zCur);
      return SQLITE_ERROR;
    }
    sqlite3_str_appendf(pWrap, ") AS \"%w\" FROM (", pTab->aCol[i].zCnName);
    sqlite3_str_appendall(pWrap, zCur);
    sqlite3_str_appendall(pWrap, ")");
    zWrap = sqlite3_str_finish(pWrap);
    sqlite3_free(zCur);
    if( !zWrap ) return SQLITE_NOMEM;
    zCur = zWrap;
  }
  *pzSql = zCur;
  return SQLITE_OK;
}

static int bindIndexExprRow(
  sqlite3_stmt *pStmt,
  Table *pTab,
  const u8 *pRec, int nRec,
  int iPKey, i64 intKey
){
  DoltliteRecordInfo info = {0};
  int i, iParam, rc = SQLITE_OK;
  doltliteParseRecord(pRec, nRec, &info);
  for(i=0, iParam=1; i<pTab->nCol && rc==SQLITE_OK; i++){
    int iField;
    DoltliteSerialValue v;
    if( doltliteColumnIsVirtual(pTab, i) ) continue;
    if( i==iPKey ){
      rc = sqlite3_bind_int64(pStmt, iParam, intKey);
      iParam++;
      continue;
    }
    iField = i;
#ifndef SQLITE_OMIT_GENERATED_COLUMNS
    /* Table column numbers are not record field numbers: INTEGER PRIMARY
    ** KEY is the btree key, and VIRTUAL generated columns are not stored.
    ** sqlite3TableColumnToStorage skips VIRTUAL; IPK is still field 0 NULL. */
    iField = sqlite3TableColumnToStorage(pTab, i);
#endif
    if( iField>=0 && iField<info.nField ){
      rc = doltliteSerialValueFromField(pRec, nRec, &info, iField, &v);
      if( rc!=SQLITE_OK ) break;
      if( v.eType==SQLITE_NULL ){
        rc = sqlite3_bind_null(pStmt, iParam);
      }else if( v.eType==SQLITE_INTEGER ){
        rc = sqlite3_bind_int64(pStmt, iParam, v.i);
      }else if( v.eType==SQLITE_FLOAT ){
        rc = sqlite3_bind_double(pStmt, iParam, v.r);
      }else if( v.eType==SQLITE_TEXT ){
        rc = sqlite3_bind_text(pStmt, iParam, (const char*)v.p, v.n,
                               SQLITE_TRANSIENT);
      }else{
        rc = sqlite3_bind_blob(pStmt, iParam, v.p, v.n, SQLITE_TRANSIENT);
      }
    }else{
      rc = sqlite3_bind_null(pStmt, iParam);
    }
    iParam++;
  }
  doltliteRecordInfoClear(&info);
  return rc;
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
        int k;
        const char *zRowid = 0;
        if( pTab && HasRowid(pTab) ){
          for(k=0; k<pTab->nCol; k++){
            if( (pTab->aCol[k].colFlags & COLFLAG_PRIMKEY)!=0 ){
              zRowid = pTab->aCol[k].zCnName;
              break;
            }
          }
        }
        if( zRowid ) sqlite3_str_appendf(p, "\"%w\"", zRowid);
        else sqlite3_str_appendall(p, "rowid");
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

int doltliteAppendExprSql(sqlite3_str *p, const Expr *pExpr, Table *pTab){
  if( !p || !pExpr ) return SQLITE_ERROR;
  return indexExprToSql(p, pExpr, pTab);
}

static int evalExprOnRecord(
  Table *pTab,
  const Expr *pExpr,
  const char *zSpan,
  const u8 *pRec, int nRec,
  int iPKey, i64 intKey,
  DoltliteSerialValue *pOut,
  u8 **ppKeep
){
  sqlite3 *pEval = 0;
  sqlite3_str *pSql;
  char *zSql;
  sqlite3_stmt *pStmt = 0;
  sqlite3_value *pVal;
  int n, rc;
  int eType;

  *ppKeep = 0;
  memset(pOut, 0, sizeof(*pOut));
  if( !pTab || (!pExpr && (!zSpan || !zSpan[0])) ) return SQLITE_ERROR;
  pSql = sqlite3_str_new(0);
  sqlite3_str_appendall(pSql, "SELECT (");
  if( zSpan && zSpan[0] ){
    sqlite3_str_appendall(pSql, zSpan);
  }else if( indexExprToSql(pSql, pExpr, pTab) ){
    sqlite3_free(sqlite3_str_finish(pSql));
    return SQLITE_ERROR;
  }
  sqlite3_str_appendall(pSql, ") FROM (");
  {
    char *zSrc = 0;
    int srcRc = indexExprSourceSql(pTab, &zSrc);
    if( srcRc!=SQLITE_OK ){
      sqlite3_free(zSrc);
      sqlite3_free(sqlite3_str_finish(pSql));
      return srcRc;
    }
    sqlite3_str_appendall(pSql, zSrc);
    sqlite3_free(zSrc);
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

static int evalIndexExprColumn(
  sqlite3 *db,
  Index *pIdx,
  const u8 *pRec, int nRec,
  int iPKey, i64 intKey,
  int iIdxCol,
  DoltliteSerialValue *pOut,
  u8 **ppKeep
){
  const char *zSpan;
  (void)db;
  if( !pIdx || !pIdx->pTable || !pIdx->aColExpr
   || iIdxCol<0 || iIdxCol>=pIdx->aColExpr->nExpr ){
    return SQLITE_ERROR;
  }
  zSpan = pIdx->aColExpr->a[iIdxCol].zEName;
  return evalExprOnRecord(pIdx->pTable, pIdx->aColExpr->a[iIdxCol].pExpr, zSpan,
                          pRec, nRec, iPKey, intKey, pOut, ppKeep);
}

#ifndef SQLITE_OMIT_GENERATED_COLUMNS
static int evalGeneratedColumn(
  Table *pTab,
  const u8 *pRec, int nRec,
  int iPKey, i64 intKey,
  int iCol,
  DoltliteSerialValue *pOut,
  u8 **ppKeep
){
  Expr *pExpr;
  if( !pTab || iCol<0 || iCol>=pTab->nCol ) return SQLITE_ERROR;
  pExpr = sqlite3ColumnExpr(pTab, &pTab->aCol[iCol]);
  if( !pExpr ) return SQLITE_ERROR;
  return evalExprOnRecord(pTab, pExpr, 0, pRec, nRec, iPKey, intKey,
                          pOut, ppKeep);
}
#endif

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
  DoltliteRecordInfo info = {0};
  DoltliteSerialValue *aMem = 0;
  u8 **apKeep = 0;
  u8 *pIdxRec = 0;
  int nIdxRec = 0;
  int nOut = 0;
  int nAlloc;
  int hasRowid;
  int storePayload = 0;
  int i, rc;

  /* A parsed record with no fields is every column NULL. */
  rc = doltliteParseRecordStrict(pRec, nRec, &info);
  if( rc!=SQLITE_OK ){
    doltliteRecordInfoClear(&info);
    return rc;
  }
  hasRowid = pIdx && pIdx->pTable && HasRowid(pIdx->pTable);

  nAlloc = nIdxCol + 1;
  aMem = sqlite3_malloc(nAlloc * (int)sizeof(DoltliteSerialValue));
  apKeep = sqlite3_malloc(nAlloc * (int)sizeof(u8*));
  if( !aMem || !apKeep ){
    sqlite3_free(aMem); sqlite3_free(apKeep);
    doltliteRecordInfoClear(&info); return SQLITE_NOMEM;
  }
  memset(aMem, 0, nAlloc * (int)sizeof(DoltliteSerialValue));
  memset(apKeep, 0, nAlloc * (int)sizeof(u8*));

  for(i=0; i<nIdxCol; i++){
    int col = aiColumn[i];
    Table *pTab = pIdx ? pIdx->pTable : 0;
    if( col==XN_EXPR ){
      rc = evalIndexExprColumn(db, pIdx, pRec, nRec, iPKey, intKey,
                               i, &aMem[nOut], &apKeep[nOut]);
      if( rc!=SQLITE_OK ) goto expr_fail;
      nOut++;
    }else if( col==XN_ROWID || col==iPKey ){
      aMem[nOut].eType = SQLITE_INTEGER;
      aMem[nOut].i = intKey;
      nOut++;
#ifndef SQLITE_OMIT_GENERATED_COLUMNS
    }else if( pTab && col>=0 && col<pTab->nCol
           && (pTab->aCol[col].colFlags & COLFLAG_VIRTUAL) ){
      rc = evalGeneratedColumn(pTab, pRec, nRec, iPKey, intKey, col,
                               &aMem[nOut], &apKeep[nOut]);
      if( rc!=SQLITE_OK ) goto expr_fail;
      nOut++;
#endif
    }else if( pTab && col>=0 && col<pTab->nCol ){
      int iStore = sqlite3TableColumnToStorage(pTab, col);
      if( iPKey>=0 && col==iPKey ){
        aMem[nOut].eType = SQLITE_INTEGER;
        aMem[nOut].i = intKey;
      }else if( iStore>=0 && iStore<info.nField ){
        rc = doltliteSerialValueFromField(pRec, nRec, &info, iStore, &aMem[nOut]);
        if( rc!=SQLITE_OK ) goto expr_fail;
      }else{
        aMem[nOut].eType = SQLITE_NULL;
      }
      nOut++;
    }else if( col>=0 && col<info.nField ){
      rc = doltliteSerialValueFromField(pRec, nRec, &info, col, &aMem[nOut]);
      if( rc!=SQLITE_OK ) goto expr_fail;
      nOut++;
    }else if( col>=0 ){
      aMem[nOut].eType = SQLITE_NULL;
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
    doltliteRecordInfoClear(&info);
    return rc;
  }
  if( pStorePayload ) *pStorePayload = storePayload;
  if( storePayload && ppIdxRec ){
    *ppIdxRec = pIdxRec;
    if( pnIdxRec ) *pnIdxRec = nIdxRec;
  }else{
    sqlite3_free(pIdxRec);
  }
  doltliteRecordInfoClear(&info);
  return SQLITE_OK;

expr_fail:
  if( apKeep ){
    for(i=0; i<nAlloc; i++) sqlite3_free(apKeep[i]);
  }
  sqlite3_free(apKeep);
  sqlite3_free(aMem);
  sqlite3_free(pIdxRec);
  doltliteRecordInfoClear(&info);
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
  DoltliteRecordInfo info = {0};
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

  if( indexNeedsExprBuild(pIdx, aiColumn, nIdxCol)
   || (iPKey<0 && pIdx && pIdx->pTable && HasRowid(pIdx->pTable)) ){
    return doltliteBuildIndexEntryWithExpr(
        db, pIdx, pRec, nRec, aiColumn, nIdxCol, pKeyInfo, iPKey, intKey,
        pTreeKey, nTreeKey, ppSortKey, pnSortKey, ppIdxRec, pnIdxRec,
        pStorePayload);
  }

  /* A record may stop short of the table's last columns; the missing
  ** trailing fields are NULL, and the index key must still carry them.
  ** A parsed record with no fields is every column NULL. */
  rc = doltliteParseRecordStrict(pRec, nRec, &info);
  if( rc!=SQLITE_OK ){
    doltliteRecordInfoClear(&info);
    return rc;
  }
  if( iPKey>=0 ){
    int st = iPKey<info.nField ? info.aType[iPKey] : 0;
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
    if( !aFieldOrder ){ doltliteRecordInfoClear(&info); return SQLITE_NOMEM; }

    {
      int out = 0;
      for(i=0; i<nIdxCol; i++){
        int col = aiColumn[i];
        if( col>=0 ){
          aFieldOrder[out++] = col;
        }
      }
      if( iPKey>=0 ){
        aFieldOrder[out++] = iPKey;
      }
      nOutField = out;
    }

    for(i=0; i<nOutField; i++){
      int col = aFieldOrder[i];
      int st = (useIpk && col==iPKey) ? (int)ipkType
             : (col<info.nField ? info.aType[col] : 0);
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
      int st = (useIpk && col==iPKey) ? (int)ipkType
             : (col<info.nField ? info.aType[col] : 0);
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
      if( col>=info.nField ) continue;
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

#endif /* DOLTLITE_PROLLY */
