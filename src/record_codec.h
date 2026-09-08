#ifndef RECORD_CODEC_H
#define RECORD_CODEC_H

#include "prolly_record.h"
#include "prolly_hash.h"

char *doltliteDecodeRecord(const u8 *pData, int nData);

typedef struct DoltliteColInfo DoltliteColInfo;
struct DoltliteColInfo {
  char **azName;
  int nCol;
  int iPkCol;
  /* aColToRec[i] is the record field index for the i-th declared
  ** column. Identity for rowid-aliased and keyless tables. For
  ** WITHOUT ROWID tables (including all doltlite tables with a
  ** non-INT-PK, which build.c auto-converts) the layout is
  ** PK-first: PK columns in PRIMARY KEY declaration order, then
  ** non-PK columns in declared order — matching aiColumn[] that
  ** SQLite's convertToWithoutRowidTable builds for the covering
  ** PK index. */
  int *aColToRec;
  /* Clustered-key metadata, filled from the same table the names came
  ** from, so a historical schema decodes its own keys. */
  int bHasRowid;
  int nPk;
  u8 *aPkSortFlags;
};

/* One side of a diff-style read: the table's columns as the schema at the
** visited commit declares them, plus a by-name projection of the vtab's
** declared columns onto them. Cached by schema hash — adjacent commits
** nearly always share a schema. An invalid side falls back to rendering
** with the declared layout, which is the pre-existing behavior. */
typedef struct DoltliteSideCols DoltliteSideCols;
struct DoltliteSideCols {
  DoltliteColInfo ci;
  int *aDeclToSide;   /* declared column -> ci column, -1 when absent */
  ProllyHash schemaHash;
  int valid;
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

int doltliteRecordFromClusteredKey(sqlite3 *db, const char *zTable,
    const u8 *pKey, int nKey, u8 **ppRec, int *pnRec);
int doltliteRecordFromClusteredKeyCols(sqlite3 *db,
    const DoltliteColInfo *ci,
    const u8 *pKey, int nKey, u8 **ppRec, int *pnRec);
void doltliteSideColsClear(DoltliteSideCols *pSide);
int doltliteSideColsLoad(sqlite3 *db,
    const ProllyHash *pCatHash, const ProllyHash *pSchemaHash,
    const char *zTable, const DoltliteColInfo *pDeclared,
    int bSideHasData, DoltliteSideCols *pSide);
void doltliteResultSideCol(sqlite3_context *ctx,
    const DoltliteSideCols *pSide,
    const DoltliteColInfo *pDeclared,
    const u8 *pRec, int nRec,
    i64 intKey, int bRootIntKey, int iDeclaredCol);
void doltliteResultUserCol(sqlite3_context *ctx,
                           const DoltliteColInfo *ci,
                           const u8 *pRec, int nRec,
                           i64 intKey,
                           int bRootIntKey,
                           int iDeclaredCol);

int doltliteFieldValuesEqual(int aType, const u8 *pA, int nA, int aOff,
                             int bType, const u8 *pB, int nB, int bOff);

/* A SQLite value to be encoded into a record. eType is one of the
** SQLITE_* type codes; i/r/p+n carry the integer, real and text/blob
** payloads respectively. */
typedef struct DoltliteSerialValue DoltliteSerialValue;
struct DoltliteSerialValue {
  int eType;
  i64 i;
  double r;
  const void *p;
  int n;
};

static inline int doltliteSerialValueFromPayload(
  const u8 *pRec, int nRec, int st, int off, DoltliteSerialValue *pValue
){
  int n;
  memset(pValue, 0, sizeof(*pValue));
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

static inline int doltliteSerialValueFromField(
  const u8 *pRec, int nRec,
  const DoltliteRecordInfo *pInfo,
  int iField,
  DoltliteSerialValue *pValue
){
  memset(pValue, 0, sizeof(*pValue));
  if( iField<0 || iField>=pInfo->nField ) return SQLITE_CORRUPT;
  return doltliteSerialValueFromPayload(
      pRec, nRec, pInfo->aType[iField], pInfo->aOffset[iField], pValue);
}

/* Assemble an SQLite-format record (header varints + body) from nField
** serial values. Returns a sqlite3_malloc'd buffer of *pnOut bytes, or 0
** on OOM or nField<=0. */
u8 *doltliteBuildRecord(const DoltliteSerialValue *aMem, int nField, int *pnOut);

#endif
