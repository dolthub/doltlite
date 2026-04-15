
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
  /* aColToRec[i] is the record field index for the i-th declared
  ** column. Identity for rowid-aliased and keyless tables. For
  ** WITHOUT ROWID tables (including all doltlite tables with a
  ** non-INT-PK, which build.c auto-converts) the layout is
  ** PK-first: PK columns in PRIMARY KEY declaration order, then
  ** non-PK columns in declared order — matching aiColumn[] that
  ** SQLite's convertToWithoutRowidTable builds for the covering
  ** PK index. */
  int *aColToRec;
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

/* Project a user-schema column into a SQLite result context.
**
** This is the single correct way to project a column from a
** doltlite catalog record. Callers pass their declared column
** index (the position of the column in the CREATE TABLE text);
** the helper knows how that maps to the physical record layout:
**
**   - If iDeclaredCol is the rowid-alias column (ci->iPkCol),
**     the value is the cursor's intKey, not a record field.
**   - Otherwise the declared index is mapped through
**     ci->aColToRec to the PK-first record field index, because
**     doltlite's auto-converted WITHOUT ROWID tables store PK
**     columns before non-PK columns regardless of their declared
**     positions (see build.c:2722 and convertToWithoutRowidTable).
**
** Callers MUST NOT read ri.aType[iDeclaredCol] directly — that's
** the whole class of bug this helper exists to close. Use this
** instead for every per-column projection in a vtab xColumn.
**
** If pRec is NULL or empty (diff delete side, missing history
** row, etc.) every column projects as SQL NULL, except the
** rowid-alias column which still returns intKey.
*/
void doltliteResultUserCol(sqlite3_context *ctx,
                           const DoltliteColInfo *ci,
                           const u8 *pRec, int nRec,
                           i64 intKey,
                           int iDeclaredCol);

int doltliteBindField(sqlite3_stmt *pStmt, int iParam,
                      const u8 *pData, int nData,
                      int serialType, int offset);

#endif
