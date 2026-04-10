/* SQLite record-format decoding helpers and varint/serial-type utilities. */
#ifndef DOLTLITE_RECORD_H
#define DOLTLITE_RECORD_H

#include "sqliteInt.h"

char *doltliteDecodeRecord(const u8 *pData, int nData);

void doltliteResultRecord(sqlite3_context *ctx, const u8 *pData, int nData);

typedef struct DoltliteColInfo DoltliteColInfo;
struct DoltliteColInfo {
  char **azName;    
  int nCol;         
  int iPkCol;       
};

int doltliteGetColumnNames(sqlite3 *db, const char *zTable, DoltliteColInfo *ci);

static inline int doltliteLoadUserTableColumns(
  sqlite3 *db,
  const char *zTable,
  DoltliteColInfo *pCols,
  char **pzErr
){
  int rc = doltliteGetColumnNames(db, zTable, pCols);
  if( rc!=SQLITE_OK ) return rc;
  if( pCols->nCol<=0 ){
    if( pzErr ){
      *pzErr = sqlite3_mprintf("table '%s' not found or has no columns",
                               zTable ? zTable : "");
      if( !*pzErr ) return SQLITE_NOMEM;
    }
    return SQLITE_ERROR;
  }
  return SQLITE_OK;
}

void doltliteFreeColInfo(DoltliteColInfo *ci);

static inline int dlReadVarint(const u8 *p, const u8 *pEnd, u64 *pVal){
  u64 v;
  int i;
  if( p >= pEnd ){ *pVal = 0; return 0; }
  v = p[0];
  if( !(v & 0x80) ){ *pVal = v; return 1; }
  v &= 0x7f;
  for(i = 1; i < 9 && p+i < pEnd; i++){
    v = (v << 7) | (p[i] & 0x7f);
    if( !(p[i] & 0x80) ){ *pVal = v; return i + 1; }
  }
  *pVal = v;
  return i;
}

static inline int dlSerialTypeLen(u64 st){
  static const u8 aLen[] = {0, 1, 2, 3, 4, 6, 8};
  if( st <= 6 ) return aLen[st];
  if( st == 7 ) return 8;
  if( st >= 12 ) return (int)(st - 12) / 2;
  return 0;
}

/*
** Parsed record header: field count, serial types, and data offsets.
** Shared across diff_table, history, at, schema_diff, and merge.
*/
#define DOLTLITE_MAX_RECORD_FIELDS 64

typedef struct DoltliteRecordInfo DoltliteRecordInfo;
struct DoltliteRecordInfo {
  int nField;
  int aType[DOLTLITE_MAX_RECORD_FIELDS];
  int aOffset[DOLTLITE_MAX_RECORD_FIELDS];
};

int doltliteParseRecordStrict(const u8 *pData, int nData,
                              DoltliteRecordInfo *pInfo);

void doltliteParseRecord(const u8 *pData, int nData, DoltliteRecordInfo *pInfo);

void doltliteResultField(sqlite3_context *ctx, const u8 *pData, int nData,
                         int serialType, int offset);
void doltliteResultRecordPkField(sqlite3_context *ctx,
                                 const u8 *pData, int nData,
                                 int iPkField);

int doltliteBindField(sqlite3_stmt *pStmt, int iParam,
                      const u8 *pData, int nData,
                      int serialType, int offset);

#endif
