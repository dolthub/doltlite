
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "doltlite_record.h"
#include "sortkey.h"
#include <string.h>
#include <stdio.h>

u32 doltliteSerialTypeOf(const DoltliteSerialValue *pVal, u32 *pLen){
  if( pVal->eType == SQLITE_NULL ){ *pLen = 0; return 0; }
  if( pVal->eType == SQLITE_INTEGER ){
    i64 v = pVal->i;
    if( v==0 ){ *pLen = 0; return 8; }
    if( v==1 ){ *pLen = 0; return 9; }
    if( v>=-128 && v<=127 ){ *pLen = 1; return 1; }
    if( v>=-32768 && v<=32767 ){ *pLen = 2; return 2; }
    if( v>=-8388608 && v<=8388607 ){ *pLen = 3; return 3; }
    if( v>=-2147483648LL && v<=2147483647LL ){ *pLen = 4; return 4; }
    if( v>=-140737488355328LL && v<=140737488355327LL ){ *pLen = 6; return 5; }
    *pLen = 8; return 6;
  }
  if( pVal->eType == SQLITE_FLOAT ){ *pLen = 8; return 7; }
  if( pVal->eType == SQLITE_TEXT ){
    *pLen = (u32)pVal->n;
    return (u32)(pVal->n * 2 + 13);
  }
  if( pVal->eType == SQLITE_BLOB ){
    *pLen = (u32)pVal->n;
    return (u32)(pVal->n * 2 + 12);
  }
  *pLen = 0;
  return 0;
}

void doltliteSerialPut(u8 *pOut, const DoltliteSerialValue *pVal, u32 serialType){
  i64 v;
  int nByte, i;
  if( serialType==0 || serialType==8 || serialType==9 ) return;
  if( serialType==7 ){
    sqlite3_uint64 u;
    memcpy(&u, &pVal->r, 8);
    v = (i64)u;
    nByte = 8;
  }else if( serialType>=1 && serialType<=6 ){
    v = pVal->i;
    nByte = dlSerialTypeLen(serialType);
  }else{
    if( serialType>=12 ) memcpy(pOut, pVal->p, (size_t)pVal->n);
    return;
  }
  for(i=nByte-1; i>=0; i--){
    pOut[i] = (u8)(v & 0xFF);
    v >>= 8;
  }
}

int doltliteFieldValuesEqual(
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
        ai = dlReadIntBytes(pA+aOff, aLen);
      }
      if( bType==8 )      bi = 0;
      else if( bType==9 ) bi = 1;
      else{
        bLen = dlSerialTypeLen(bType);
        if( bOff<0 || bOff+bLen>nB ) return 0;
        bi = dlReadIntBytes(pB+bOff, bLen);
      }
      return ai==bi;
    }
  }

  if( aType==7 && bType==7 ){
    u64 ar = 0, br = 0;
    double da, db;
    int i;
    if( aOff<0 || aOff+8>nA ) return 0;
    if( bOff<0 || bOff+8>nB ) return 0;
    for(i=0; i<8; i++) ar = (ar<<8) | pA[aOff+i];
    for(i=0; i<8; i++) br = (br<<8) | pB[bOff+i];
    memcpy(&da, &ar, 8);
    memcpy(&db, &br, 8);
    return da == db;
  }

  if( aType != bType ) return 0;
  aLen = dlSerialTypeLen(aType);
  if( aLen<0 ) return 0;
  if( aOff<0 || aOff+aLen>nA ) return 0;
  if( bOff<0 || bOff+aLen>nB ) return 0;
  return memcmp(pA+aOff, pB+bOff, aLen)==0;
}

typedef struct RecBuf RecBuf;
struct RecBuf {
  char *z;
  int n;
  int nAlloc;
};

static int recBufAppend(RecBuf *b, const char *z, int n){
  char *zNew;
  i64 nNeed;
  if( n<0 ) n = (int)strlen(z);
  if( n<0 ) return SQLITE_NOMEM;
  nNeed = (i64)b->n + (i64)n + 1;
  if( nNeed > (i64)0x7fffffff ) return SQLITE_NOMEM;
  if( nNeed > (i64)b->nAlloc ){
    i64 nNew = b->nAlloc ? (i64)b->nAlloc * 2 : (i64)128;
    while( nNew < nNeed ){
      if( nNew > (i64)0x7fffffff/2 ){
        nNew = (i64)0x7fffffff;
        break;
      }
      nNew *= 2;
    }
    if( nNew < nNeed ) return SQLITE_NOMEM;
    zNew = sqlite3_realloc(b->z, (int)nNew);
    if( !zNew ) return SQLITE_NOMEM;
    b->z = zNew;
    b->nAlloc = (int)nNew;
  }
  memcpy(b->z + b->n, z, n);
  b->n += n;
  b->z[b->n] = 0;
  return SQLITE_OK;
}

char *doltliteDecodeRecord(const u8 *pData, int nData){
  const u8 *p;
  const u8 *pEnd;
  u64 hdrSize;
  int hdrBytes;
  const u8 *pHdrEnd;
  const u8 *pBody;
  RecBuf buf;
  int fieldIdx = 0;
  int rc = SQLITE_OK;

  if( !pData || nData < 1 ) return 0;
  p = pData;
  pEnd = pData + nData;

  memset(&buf, 0, sizeof(buf));

  hdrBytes = dlReadVarint(p, pEnd, &hdrSize);
  if( hdrBytes<=0 ) return 0;
  p += hdrBytes;
  if( hdrSize<(u64)hdrBytes || hdrSize>(u64)nData ) return 0;
  pHdrEnd = pData + hdrSize;
  pBody = pData + hdrSize;

  while( rc==SQLITE_OK && p < pHdrEnd && p < pEnd ){
    u64 st;
    int stBytes = dlReadVarint(p, pHdrEnd, &st);
    if( stBytes<=0 ) rc = SQLITE_CORRUPT;
    if( rc!=SQLITE_OK ) break;
    p += stBytes;

    if( fieldIdx > 0 ){
      rc = recBufAppend(&buf, "|", 1);
      if( rc!=SQLITE_OK ) break;
    }

    if( st==0 ){
      rc = recBufAppend(&buf, "NULL", 4);
    }else if( st==8 ){
      rc = recBufAppend(&buf, "0", 1);
    }else if( st==9 ){
      rc = recBufAppend(&buf, "1", 1);
    }else if( st>=1 && st<=6 ){
      int nBytes = dlSerialTypeLen((u64)st);
      char tmp[32];
      if( pBody + nBytes > pEnd ){ rc = SQLITE_CORRUPT; break; }
      sqlite3_snprintf(sizeof(tmp), tmp, "%lld", dlReadIntBytes(pBody, nBytes));
      rc = recBufAppend(&buf, tmp, -1);
      pBody += nBytes;
    }else if( st==7 ){
      double v;
      u64 bits = 0;
      int i;
      char tmp[64];
      if( pBody + 8 > pEnd ){ rc = SQLITE_CORRUPT; break; }
      for(i=0; i<8; i++) bits = (bits<<8) | pBody[i];
      memcpy(&v, &bits, 8);
      sqlite3_snprintf(sizeof(tmp), tmp, "%!.15g", v);
      rc = recBufAppend(&buf, tmp, -1);
      pBody += 8;
    }else if( st>=12 && (st&1)==0 ){
      int len = dlSerialTypeLen(st);
      int i;
      if( len<0 || len>(int)(pEnd-pBody) ){ rc = SQLITE_CORRUPT; break; }
      rc = recBufAppend(&buf, "x'", 2);
      for(i=0; rc==SQLITE_OK && i<len; i++){
        char hex[3];
        sqlite3_snprintf(3, hex, "%02x", pBody[i]);
        rc = recBufAppend(&buf, hex, 2);
      }
      if( rc==SQLITE_OK ) rc = recBufAppend(&buf, "'", 1);
      pBody += len;
    }else if( st>=13 && (st&1)==1 ){
      int len = dlSerialTypeLen(st);
      if( len<0 || len>(int)(pEnd-pBody) ){ rc = SQLITE_CORRUPT; break; }
      rc = recBufAppend(&buf, (const char*)pBody, len);
      pBody += len;
    }else{
      rc = SQLITE_CORRUPT;
    }

    fieldIdx++;
  }

  if( rc!=SQLITE_OK || p!=pHdrEnd ){
    sqlite3_free(buf.z);
    return 0;
  }
  return buf.z;
}

void doltliteFreeColInfo(DoltliteColInfo *ci){
  int i;
  for(i=0; i<ci->nCol; i++) sqlite3_free(ci->azName[i]);
  sqlite3_free(ci->azName);
  sqlite3_free(ci->aColToRec);
  sqlite3_free(ci->aPkSortFlags);
  ci->azName = 0;
  ci->aColToRec = 0;
  ci->aPkSortFlags = 0;
  ci->nCol = 0;
  ci->nPk = 0;
  ci->bHasRowid = 0;
}

int doltliteGetColumnNames(sqlite3 *db, const char *zTable, DoltliteColInfo *ci){
  char *zSql;
  sqlite3_stmt *pStmt = 0;
  int rc, nCol;
  int nPkCols = 0;
  int iCandidateAlias = -1;
  int *aPk = 0;
  int *aRecPos = 0;
  int *aStoredPk = 0;
  int *aStoredDecl = 0;
  int nStored = 0;
  int nRecPos = 0;
  int i, iNonPk;

  memset(ci, 0, sizeof(*ci));
  ci->iPkCol = -1;
  /* table_xinfo lists generated columns. STORED occupies a record field so
  ** later columns sit one slot further; VIRTUAL occupies nothing.
  ** hidden: 0 ordinary, 2 virtual generated, 3 stored generated. */
  zSql = sqlite3_mprintf("PRAGMA main.table_xinfo(\"%w\")", zTable);
  if( !zSql ) return SQLITE_NOMEM;

  rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  sqlite3_free(zSql);
  if( rc!=SQLITE_OK ) return rc;

  nCol = 0;
  while( (rc = sqlite3_step(pStmt))==SQLITE_ROW ) nCol++;
  if( rc!=SQLITE_DONE ){
    sqlite3_finalize(pStmt);
    return rc;
  }
  sqlite3_reset(pStmt);

  ci->azName = sqlite3_malloc(nCol * (int)sizeof(char*));
  if( !ci->azName ){ sqlite3_finalize(pStmt); return SQLITE_NOMEM; }
  memset(ci->azName, 0, nCol * (int)sizeof(char*));
  ci->nCol = 0;

  if( nCol>0 ){
    aPk = sqlite3_malloc(nCol * (int)sizeof(int));
    aRecPos = sqlite3_malloc(nCol * (int)sizeof(int));
    aStoredPk = sqlite3_malloc(nCol * (int)sizeof(int));
    aStoredDecl = sqlite3_malloc(nCol * (int)sizeof(int));
    if( !aPk || !aRecPos || !aStoredPk || !aStoredDecl ){
      sqlite3_free(aPk);
      sqlite3_free(aRecPos);
      sqlite3_free(aStoredPk);
      sqlite3_free(aStoredDecl);
      doltliteFreeColInfo(ci);
      sqlite3_finalize(pStmt);
      return SQLITE_NOMEM;
    }
  }

  while( (rc = sqlite3_step(pStmt))==SQLITE_ROW ){
    const char *zName = (const char*)sqlite3_column_text(pStmt, 1);
    int pk = sqlite3_column_int(pStmt, 5);
    const char *zType = (const char*)sqlite3_column_text(pStmt, 2);
    int hidden = sqlite3_column_int(pStmt, 6);

    if( hidden==2 ){
      /* Virtual: absent from the record. */
      continue;
    }
    if( hidden==3 ){
      /* Stored: occupies a field every later column sits behind. */
      aStoredPk[nStored] = pk;
      aStoredDecl[nStored] = -1;
      nStored++;
      nRecPos++;
      continue;
    }

    if( pk>0 ) nPkCols++;
    if( pk==1 && zType && sqlite3_stricmp(zType,"INTEGER")==0 ){
      iCandidateAlias = ci->nCol;
    }

    aPk[ci->nCol] = pk;
    aRecPos[ci->nCol] = nRecPos++;
    aStoredPk[nStored] = pk;
    aStoredDecl[nStored] = ci->nCol;
    nStored++;
    ci->azName[ci->nCol] = sqlite3_mprintf("%s", zName ? zName : "");
    if( !ci->azName[ci->nCol] ){
      sqlite3_free(aPk);
      sqlite3_free(aRecPos);
      sqlite3_free(aStoredPk);
      sqlite3_free(aStoredDecl);
      doltliteFreeColInfo(ci);
      sqlite3_finalize(pStmt);
      return SQLITE_NOMEM;
    }
    ci->nCol++;
  }
  if( rc!=SQLITE_DONE ){
    sqlite3_free(aPk);
    sqlite3_free(aRecPos);
    sqlite3_free(aStoredPk);
    sqlite3_free(aStoredDecl);
    doltliteFreeColInfo(ci);
    sqlite3_finalize(pStmt);
    return rc;
  }

  /* iPkCol is a rowid alias. WITHOUT ROWID has no integer key; treating
  ** INTEGER pk as an alias yields raw key bytes and drops xBestIndex pk
  ** constraints. */
  {
    Table *pTab = sqlite3FindTable(db, zTable, "main");
    if( pTab ){
      ci->bHasRowid = HasRowid(pTab);
      if( nPkCols==1 && iCandidateAlias>=0 && ci->bHasRowid ){
        ci->iPkCol = iCandidateAlias;
      }
      if( !ci->bHasRowid ){
        Index *pPk = sqlite3PrimaryKeyIndex(pTab);
        if( pPk && pPk->nKeyCol>0 ){
          ci->aPkSortFlags = sqlite3_malloc(pPk->nKeyCol);
          if( !ci->aPkSortFlags ){
            sqlite3_free(aPk);
            doltliteFreeColInfo(ci);
            sqlite3_finalize(pStmt);
            return SQLITE_NOMEM;
          }
          for(i=0; i<pPk->nKeyCol; i++){
            ci->aPkSortFlags[i] = pPk->aSortOrder[i];
          }
          ci->nPk = pPk->nKeyCol;
        }
      }
    }else{
      ci->bHasRowid = 1;
    }
  }

  if( ci->nCol>0 ){
    ci->aColToRec = sqlite3_malloc(ci->nCol * (int)sizeof(int));
    if( !ci->aColToRec ){
      sqlite3_free(aPk);
      doltliteFreeColInfo(ci);
      sqlite3_finalize(pStmt);
      return SQLITE_NOMEM;
    }
    if( ci->iPkCol>=0 || nPkCols==0 ){
      /* Declared order over record slots: a stored generated column consumes one. */
      for(i=0; i<ci->nCol; i++) ci->aColToRec[i] = aRecPos[i];
    }else{
      /* Clustered: key columns in key order, then other stored columns in
      ** declared order (generated included). */
      int iSlot;
      for(i=0; i<ci->nCol; i++){
        if( aPk[i]>0 ) ci->aColToRec[i] = aPk[i] - 1;
      }
      iNonPk = nPkCols;
      for(iSlot=0; iSlot<nStored; iSlot++){
        if( aStoredPk[iSlot]>0 ) continue;
        if( aStoredDecl[iSlot]>=0 ){
          ci->aColToRec[aStoredDecl[iSlot]] = iNonPk;
        }
        iNonPk++;
      }
    }
  }

  sqlite3_free(aPk);
  sqlite3_free(aRecPos);
  sqlite3_free(aStoredPk);
  sqlite3_free(aStoredDecl);
  sqlite3_finalize(pStmt);
  return SQLITE_OK;
}

int doltliteParseRecordStrict(
  const u8 *pData,
  int nData,
  DoltliteRecordInfo *pInfo
){
  const u8 *p;
  const u8 *pEnd;
  u64 hdrSize;
  int hdrBytes, off;
  const u8 *pHdrEnd;

  memset(pInfo, 0, sizeof(*pInfo));
  if( !pData || nData < 1 ) return SQLITE_CORRUPT;
  p = pData;
  pEnd = pData + nData;

  hdrBytes = dlReadVarint(p, pEnd, &hdrSize);
  if( hdrBytes<=0 ) return SQLITE_CORRUPT;
  if( hdrSize < (u64)hdrBytes || hdrSize > (u64)nData ) return SQLITE_CORRUPT;
  p += hdrBytes;
  pHdrEnd = pData + (int)hdrSize;
  off = (int)hdrSize;

  while( p < pHdrEnd ){
    u64 st;
    int stBytes = dlReadVarint(p, pHdrEnd, &st);
    int nField;
    int nSerial;
    if( stBytes<=0 ) return SQLITE_CORRUPT;
    if( st==10 || st==11 || st>(u64)INT_MAX ) return SQLITE_CORRUPT;
    nField = pInfo->nField;
    if( nField >= DOLTLITE_MAX_RECORD_FIELDS ) return SQLITE_CORRUPT;
    p += stBytes;
    nSerial = dlSerialTypeLen(st);
    if( nSerial<0 || nSerial>nData-off ) return SQLITE_CORRUPT;
    pInfo->aType[nField] = (int)st;
    pInfo->aOffset[nField] = off;
    off += nSerial;
    pInfo->nField = nField + 1;
  }
  if( p != pHdrEnd ) return SQLITE_CORRUPT;
  if( off != nData ) return SQLITE_CORRUPT;
  return SQLITE_OK;
}

void doltliteParseRecord(const u8 *pData, int nData, DoltliteRecordInfo *pInfo){
  (void)doltliteParseRecordStrict(pData, nData, pInfo);
}

typedef struct DoltliteDecodedField DoltliteDecodedField;
struct DoltliteDecodedField {
  int eType;
  i64 i;
  double r;
  const u8 *p;
  int n;
};

static void doltliteDecodeField(
  const u8 *pData, int nData,
  int st, int off,
  DoltliteDecodedField *pOut
){
  memset(pOut, 0, sizeof(*pOut));
  pOut->eType = SQLITE_NULL;

  if( st==0 ) return;
  if( st==8 ){
    pOut->eType = SQLITE_INTEGER;
    pOut->i = 0;
    return;
  }
  if( st==9 ){
    pOut->eType = SQLITE_INTEGER;
    pOut->i = 1;
    return;
  }
  if( st>=1 && st<=6 ){
    int nB = dlSerialTypeLen((u64)st);
    if( off>=0 && off<=nData-nB ){
      pOut->eType = SQLITE_INTEGER;
      pOut->i = dlReadIntBytes(pData + off, nB);
    }
    return;
  }
  if( st==7 ){
    if( off>=0 && off<=nData-8 ){
      const u8 *q = pData + off;
      u64 bits = 0;
      int i;
      for(i=0; i<8; i++) bits = (bits<<8) | q[i];
      pOut->eType = SQLITE_FLOAT;
      memcpy(&pOut->r, &bits, 8);
    }
    return;
  }
  if( st>=13 && (st&1)==1 ){
    int len = (st-13)/2;
    if( off>=0 && off<=nData-len ){
      pOut->eType = SQLITE_TEXT;
      pOut->p = pData + off;
      pOut->n = len;
    }
    return;
  }
  if( st>=12 && (st&1)==0 ){
    int len = (st-12)/2;
    if( off>=0 && off<=nData-len ){
      pOut->eType = SQLITE_BLOB;
      pOut->p = pData + off;
      pOut->n = len;
    }
  }
}

void doltliteResultField(
  sqlite3_context *ctx, const u8 *pData, int nData,
  int st, int off
){
  DoltliteDecodedField f;
  doltliteDecodeField(pData, nData, st, off, &f);
  switch( f.eType ){
    case SQLITE_INTEGER:
      sqlite3_result_int64(ctx, f.i);
      return;
    case SQLITE_FLOAT:
      sqlite3_result_double(ctx, f.r);
      return;
    case SQLITE_TEXT:
      sqlite3_result_text(ctx, (const char*)f.p, f.n, SQLITE_TRANSIENT);
      return;
    case SQLITE_BLOB:
      sqlite3_result_blob(ctx, f.p, f.n, SQLITE_TRANSIENT);
      return;
  }
  sqlite3_result_null(ctx);
}

void doltliteResultUserCol(
  sqlite3_context *ctx,
  const DoltliteColInfo *ci,
  const u8 *pRec, int nRec,
  i64 intKey,
  int bRootIntKey,
  int iDeclaredCol
){
  int iRecField;
  DoltliteRecordInfo ri;

  if( !pRec || nRec<=0 || !ci || iDeclaredCol<0 || iDeclaredCol>=ci->nCol ){
    sqlite3_result_null(ctx);
    return;
  }

  /* intKey is the row key only on an intkey tree. A historical root with
  ** a different key shape stores the whole row in the record. */
  if( iDeclaredCol==ci->iPkCol && ci->iPkCol>=0 && bRootIntKey ){
    sqlite3_result_int64(ctx, intKey);
    return;
  }

  iRecField = ci->aColToRec ? ci->aColToRec[iDeclaredCol] : iDeclaredCol;
  if( iRecField<0 ){
    sqlite3_result_null(ctx);
    return;
  }

  doltliteParseRecord(pRec, nRec, &ri);
  if( iRecField>=ri.nField ){
    sqlite3_result_null(ctx);
    return;
  }
  doltliteResultField(ctx, pRec, nRec,
                      ri.aType[iRecField], ri.aOffset[iRecField]);
}

/* Reconstruct a clustered row whose stored value is empty (PK covers every
** column). Decode the sort key like getCursorPayload's non-intkey branch.
** *ppRec is sqlite3_malloc'd or 0 for rowid tables. */
int doltliteRecordFromClusteredKey(
  sqlite3 *db, const char *zTable,
  const u8 *pKey, int nKey,
  u8 **ppRec, int *pnRec
){
  Table *pTab;
  Index *pPk;
  KeyInfo *pKI;
  u8 *pBuf = 0;
  int nAlloc = 0, nRec = 0;
  int i, rc;

  *ppRec = 0;
  *pnRec = 0;
  if( !pKey || nKey<=0 || !zTable ) return SQLITE_OK;
  pTab = sqlite3FindTable(db, zTable, "main");
  if( !pTab || HasRowid(pTab) ) return SQLITE_OK;
  pPk = sqlite3PrimaryKeyIndex(pTab);
  if( !pPk ) return SQLITE_OK;

  pKI = sqlite3KeyInfoAlloc(db, pPk->nKeyCol, 0);
  if( !pKI ) return SQLITE_NOMEM;
  for(i=0; i<pPk->nKeyCol; i++){
    pKI->aSortFlags[i] = pPk->aSortOrder[i];
  }
  rc = recordFromSortKeyBufferColl(pKey, nKey, pKI, &pBuf, &nAlloc, &nRec);
  sqlite3KeyInfoUnref(pKI);
  if( rc!=SQLITE_OK ){
    sqlite3_free(pBuf);
    return rc;
  }
  *ppRec = pBuf;
  *pnRec = nRec;
  return SQLITE_OK;
}

/* Same reconstruction keyed by DoltliteColInfo, so a historical schema
** decodes keys its own PK wrote. */
int doltliteRecordFromClusteredKeyCols(
  sqlite3 *db, const DoltliteColInfo *ci,
  const u8 *pKey, int nKey,
  u8 **ppRec, int *pnRec
){
  KeyInfo *pKI;
  u8 *pBuf = 0;
  int nAlloc = 0, nRec = 0;
  int i, rc;

  *ppRec = 0;
  *pnRec = 0;
  if( !pKey || nKey<=0 || !ci ) return SQLITE_OK;
  if( ci->bHasRowid || ci->nPk<=0 || !ci->aPkSortFlags ) return SQLITE_OK;

  pKI = sqlite3KeyInfoAlloc(db, ci->nPk, 0);
  if( !pKI ) return SQLITE_NOMEM;
  for(i=0; i<ci->nPk; i++){
    pKI->aSortFlags[i] = ci->aPkSortFlags[i];
  }
  rc = recordFromSortKeyBufferColl(pKey, nKey, pKI, &pBuf, &nAlloc, &nRec);
  sqlite3KeyInfoUnref(pKI);
  if( rc!=SQLITE_OK ){
    sqlite3_free(pBuf);
    return rc;
  }
  *ppRec = pBuf;
  *pnRec = nRec;
  return SQLITE_OK;
}

void doltliteSideColsClear(DoltliteSideCols *pSide){
  doltliteFreeColInfo(&pSide->ci);
  sqlite3_free(pSide->aDeclToSide);
  memset(pSide, 0, sizeof(*pSide));
}

void doltliteResultSideCol(
  sqlite3_context *ctx,
  const DoltliteSideCols *pSide,
  const DoltliteColInfo *pDeclared,
  const u8 *pRec, int nRec,
  i64 intKey, int bRootIntKey, int iDeclaredCol
){
  if( pSide && pSide->valid ){
    int iSide = -1;
    if( iDeclaredCol>=0 && iDeclaredCol<pDeclared->nCol ){
      iSide = pSide->aDeclToSide[iDeclaredCol];
    }
    if( iSide<0 ){
      sqlite3_result_null(ctx);
      return;
    }
    doltliteResultUserCol(ctx, &pSide->ci, pRec, nRec,
                          intKey, bRootIntKey, iSide);
    return;
  }
  doltliteResultUserCol(ctx, pDeclared, pRec, nRec,
                        intKey, bRootIntKey, iDeclaredCol);
}

u8 *doltliteBuildRecord(const DoltliteSerialValue *aMem, int nField, int *pnOut){
  u32 *aType, *aLen;
  u8 *pOut, *pHdr, *pBody;
  int i, hdrSize = 0, bodySize = 0, pos;

  *pnOut = 0;
  if( nField<=0 ) return 0;

  aType = sqlite3_malloc64((sqlite3_int64)nField * sizeof(u32));
  aLen = sqlite3_malloc64((sqlite3_int64)nField * sizeof(u32));
  if( !aType || !aLen ){
    sqlite3_free(aType);
    sqlite3_free(aLen);
    return 0;
  }

  for(i=0; i<nField; i++){
    aType[i] = doltliteSerialTypeOf(&aMem[i], &aLen[i]);
    hdrSize += sqlite3VarintLen(aType[i]);
    bodySize += (int)aLen[i];
  }
  /* Adding the header-size varint can cross a width: 127 one-byte type codes
  ** need a two-byte size. Stock sqlite3VdbeMakeRecord does the same. */
  {
    int nVarint = sqlite3VarintLen(hdrSize);
    hdrSize += nVarint;
    if( nVarint < sqlite3VarintLen(hdrSize) ) hdrSize++;
  }

  pOut = sqlite3_malloc(hdrSize + bodySize);
  if( !pOut ){
    sqlite3_free(aType);
    sqlite3_free(aLen);
    return 0;
  }

  pos = sqlite3PutVarint(pOut, hdrSize);
  pHdr = pOut + pos;
  pBody = pOut + hdrSize;
  for(i=0; i<nField; i++){
    pHdr += sqlite3PutVarint(pHdr, aType[i]);
    if( aLen[i]>0 ){
      doltliteSerialPut(pBody, &aMem[i], aType[i]);
      pBody += aLen[i];
    }
  }
  *pnOut = hdrSize + bodySize;
  sqlite3_free(aType);
  sqlite3_free(aLen);
  return pOut;
}

#endif
