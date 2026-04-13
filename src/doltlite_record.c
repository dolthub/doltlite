
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "doltlite_record.h"
#include <string.h>
#include <stdio.h>

static i64 recReadInt(const u8 *p, int nBytes){
  i64 v;
  int i;

  v = (p[0] & 0x80) ? -1 : 0;
  for(i=0; i<nBytes; i++){
    v = (v << 8) | p[i];
  }
  return v;
}

typedef struct RecBuf RecBuf;
struct RecBuf {
  char *z;
  int n;
  int nAlloc;
};

static int recBufAppend(RecBuf *b, const char *z, int n){
  char *zNew;
  if( n<0 ) n = (int)strlen(z);
  if( b->n + n + 1 > b->nAlloc ){
    int nNew = b->nAlloc ? b->nAlloc*2 : 128;
    while( nNew < b->n + n + 1 ) nNew *= 2;
    zNew = sqlite3_realloc(b->z, nNew);
    if( !zNew ) return SQLITE_NOMEM;
    b->z = zNew;
    b->nAlloc = nNew;
  }
  memcpy(b->z + b->n, z, n);
  b->n += n;
  b->z[b->n] = 0;
  return SQLITE_OK;
}

char *doltliteDecodeRecord(const u8 *pData, int nData){
  const u8 *p = pData;
  const u8 *pEnd = pData + nData;
  u64 hdrSize;
  int hdrBytes;
  const u8 *pHdrEnd;
  const u8 *pBody;
  RecBuf buf;
  int fieldIdx = 0;
  int rc = SQLITE_OK;

  if( !pData || nData < 1 ) return 0;

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
      static const int sizes[] = {0,1,2,3,4,6,8};
      int nBytes = sizes[st];
      char tmp[32];
      if( pBody + nBytes > pEnd ){ rc = SQLITE_CORRUPT; break; }
      sqlite3_snprintf(sizeof(tmp), tmp, "%lld", recReadInt(pBody, nBytes));
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
      int len = ((int)st - 12) / 2;
      int i;
      if( len<0 || pBody + len > pEnd ){ rc = SQLITE_CORRUPT; break; }
      rc = recBufAppend(&buf, "x'", 2);
      for(i=0; rc==SQLITE_OK && i<len; i++){
        char hex[3];
        sqlite3_snprintf(3, hex, "%02x", pBody[i]);
        rc = recBufAppend(&buf, hex, 2);
      }
      if( rc==SQLITE_OK ) rc = recBufAppend(&buf, "'", 1);
      pBody += len;
    }else if( st>=13 && (st&1)==1 ){
      int len = ((int)st - 13) / 2;
      if( len<0 || pBody + len > pEnd ){ rc = SQLITE_CORRUPT; break; }
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

void doltliteResultRecord(sqlite3_context *ctx, const u8 *pData, int nData){
  if( !pData || nData<=0 ){
    sqlite3_result_null(ctx);
    return;
  }
  {
    char *z = doltliteDecodeRecord(pData, nData);
    if( z ){
      sqlite3_result_text(ctx, z, -1, sqlite3_free);
    }else{
      sqlite3_result_null(ctx);
    }
  }
}

void doltliteFreeColInfo(DoltliteColInfo *ci){
  int i;
  for(i=0; i<ci->nCol; i++) sqlite3_free(ci->azName[i]);
  sqlite3_free(ci->azName);
  ci->azName = 0;
  ci->nCol = 0;
}

/* iPkCol is set only for a rowid-aliased INTEGER PRIMARY KEY column.
** Other PK columns live in the record payload like any user column,
** so callers that serialize INT PK values separately (at, history,
** diff vtables) use iPkCol to pull the rowid from the cursor
** instead of the record. */
int doltliteGetColumnNames(sqlite3 *db, const char *zTable, DoltliteColInfo *ci){
  char *zSql;
  sqlite3_stmt *pStmt = 0;
  int rc, nCol;

  memset(ci, 0, sizeof(*ci));
  ci->iPkCol = -1;
  zSql = sqlite3_mprintf("PRAGMA table_info(\"%w\")", zTable);
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

  while( (rc = sqlite3_step(pStmt))==SQLITE_ROW ){
    const char *zName = (const char*)sqlite3_column_text(pStmt, 1);
    int pk = sqlite3_column_int(pStmt, 5);
    const char *zType = (const char*)sqlite3_column_text(pStmt, 2);


    if( pk==1 && zType && sqlite3_stricmp(zType,"INTEGER")==0 ){
      ci->iPkCol = ci->nCol;
    }

    ci->azName[ci->nCol] = sqlite3_mprintf("%s", zName ? zName : "");
    if( !ci->azName[ci->nCol] ){
      doltliteFreeColInfo(ci);
      sqlite3_finalize(pStmt);
      return SQLITE_NOMEM;
    }
    ci->nCol++;
  }
  if( rc!=SQLITE_DONE ){
    doltliteFreeColInfo(ci);
    sqlite3_finalize(pStmt);
    return rc;
  }

  sqlite3_finalize(pStmt);
  return SQLITE_OK;
}

int doltliteParseRecordStrict(
  const u8 *pData,
  int nData,
  DoltliteRecordInfo *pInfo
){
  const u8 *p = pData;
  const u8 *pEnd = pData + nData;
  u64 hdrSize;
  int hdrBytes, off;
  const u8 *pHdrEnd;

  memset(pInfo, 0, sizeof(*pInfo));
  if( !pData || nData < 1 ) return SQLITE_CORRUPT;

  hdrBytes = dlReadVarint(p, pEnd, &hdrSize);
  if( hdrBytes<=0 ) return SQLITE_CORRUPT;
  if( (int)hdrSize < hdrBytes || (int)hdrSize > nData ) return SQLITE_CORRUPT;
  p += hdrBytes;
  pHdrEnd = pData + (int)hdrSize;
  off = (int)hdrSize;

  while( p < pHdrEnd ){
    u64 st;
    int stBytes = dlReadVarint(p, pHdrEnd, &st);
    int nField;
    int nSerial;
    if( stBytes<=0 ) return SQLITE_CORRUPT;
    nField = pInfo->nField;
    if( nField >= DOLTLITE_MAX_RECORD_FIELDS ) return SQLITE_CORRUPT;
    p += stBytes;
    nSerial = dlSerialTypeLen(st);
    if( off > nData - nSerial ) return SQLITE_CORRUPT;
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

void doltliteResultField(
  sqlite3_context *ctx, const u8 *pData, int nData,
  int st, int off
){
  if( st==0 ){ sqlite3_result_null(ctx); return; }
  if( st==8 ){ sqlite3_result_int(ctx, 0); return; }
  if( st==9 ){ sqlite3_result_int(ctx, 1); return; }
  if( st>=1 && st<=6 ){
    static const int sizes[] = {0,1,2,3,4,6,8};
    int nB = sizes[st];
    if( off+nB <= nData ){
      const u8 *q = pData + off;
      i64 v = (q[0] & 0x80) ? -1 : 0;
      int i;
      for(i=0; i<nB; i++) v = (v<<8) | q[i];
      sqlite3_result_int64(ctx, v);
    }else{
      sqlite3_result_null(ctx);
    }
    return;
  }
  if( st==7 ){
    if( off+8 <= nData ){
      const u8 *q = pData + off;
      double v; u64 bits = 0; int i;
      for(i=0; i<8; i++) bits = (bits<<8) | q[i];
      memcpy(&v, &bits, 8);
      sqlite3_result_double(ctx, v);
    }else{
      sqlite3_result_null(ctx);
    }
    return;
  }
  if( st>=13 && (st&1)==1 ){
    int len = (st-13)/2;
    if( off+len <= nData )
      sqlite3_result_text(ctx, (const char*)(pData+off), len, SQLITE_TRANSIENT);
    else sqlite3_result_null(ctx);
    return;
  }
  if( st>=12 && (st&1)==0 ){
    int len = (st-12)/2;
    if( off+len <= nData )
      sqlite3_result_blob(ctx, pData+off, len, SQLITE_TRANSIENT);
    else sqlite3_result_null(ctx);
    return;
  }
  sqlite3_result_null(ctx);
}

void doltliteResultRecordPkField(
  sqlite3_context *ctx, const u8 *pData, int nData, int iPkField
){
  DoltliteRecordInfo ri;
  if( !pData || nData<1 || iPkField<0 ){
    sqlite3_result_null(ctx);
    return;
  }
  doltliteParseRecord(pData, nData, &ri);
  if( iPkField>=ri.nField ){
    sqlite3_result_null(ctx);
    return;
  }
  doltliteResultField(ctx, pData, nData,
                      ri.aType[iPkField], ri.aOffset[iPkField]);
}

int doltliteBindField(
  sqlite3_stmt *pStmt, int iParam,
  const u8 *pData, int nData,
  int st, int off
){
  if( st==0 ) return sqlite3_bind_null(pStmt, iParam);
  if( st==8 ) return sqlite3_bind_int(pStmt, iParam, 0);
  if( st==9 ) return sqlite3_bind_int(pStmt, iParam, 1);
  if( st>=1 && st<=6 ){
    static const int sizes[] = {0,1,2,3,4,6,8};
    int nB = sizes[st];
    if( off+nB <= nData ){
      const u8 *q = pData + off;
      i64 v = (q[0] & 0x80) ? -1 : 0;
      int i;
      for(i=0; i<nB; i++) v = (v<<8) | q[i];
      return sqlite3_bind_int64(pStmt, iParam, v);
    }
    return sqlite3_bind_null(pStmt, iParam);
  }
  if( st==7 ){
    if( off+8 <= nData ){
      const u8 *q = pData + off;
      double v; u64 bits = 0; int i;
      for(i=0; i<8; i++) bits = (bits<<8) | q[i];
      memcpy(&v, &bits, 8);
      return sqlite3_bind_double(pStmt, iParam, v);
    }
    return sqlite3_bind_null(pStmt, iParam);
  }
  if( st>=13 && (st&1)==1 ){
    int len = (st-13)/2;
    if( off+len <= nData )
      return sqlite3_bind_text(pStmt, iParam, (const char*)(pData+off), len, SQLITE_TRANSIENT);
    return sqlite3_bind_null(pStmt, iParam);
  }
  if( st>=12 && (st&1)==0 ){
    int len = (st-12)/2;
    if( off+len <= nData )
      return sqlite3_bind_blob(pStmt, iParam, pData+off, len, SQLITE_TRANSIENT);
    return sqlite3_bind_null(pStmt, iParam);
  }
  return sqlite3_bind_null(pStmt, iParam);
}

#endif
