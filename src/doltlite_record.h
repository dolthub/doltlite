/* SQLite record-format decoding helpers and varint/serial-type utilities. */
#ifndef DOLTLITE_RECORD_H
#define DOLTLITE_RECORD_H

#include "prolly_record.h"

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

void doltliteResultField(sqlite3_context *ctx, const u8 *pData, int nData,
                         int serialType, int offset);
void doltliteResultRecordPkField(sqlite3_context *ctx,
                                 const u8 *pData, int nData,
                                 int iPkField);

int doltliteBindField(sqlite3_stmt *pStmt, int iParam,
                      const u8 *pData, int nData,
                      int serialType, int offset);

#endif
