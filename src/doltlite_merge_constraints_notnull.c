#ifdef DOLTLITE_PROLLY

#include "doltlite_merge_constraints_int.h"

/* Merge can pair a NOT NULL schema with a NULL from the other side.
** Use typeof(c)='null': IS NULL is strength-reduced away on NOT NULL
** columns, so the obvious query never sees these rows. */

static int loadNotNullColumns(
  sqlite3 *db,
  const char *zTable,
  char ***pazCols,
  int *pnCols
){
  sqlite3_stmt *pQ = 0;
  char *zSql;
  char **az = 0;
  int n = 0;
  int nAlloc = 0;
  int rc;
  int stepRc;

  *pazCols = 0;
  *pnCols = 0;
  zSql = sqlite3_mprintf(
      "SELECT name FROM pragma_table_xinfo(%Q) "
      "WHERE \"notnull\"=1 AND hidden!=1", zTable);
  if( !zSql ) return SQLITE_NOMEM;
  rc = sqlite3_prepare_v2(db, zSql, -1, &pQ, 0);
  sqlite3_free(zSql);
  if( rc!=SQLITE_OK ) return rc;

  while( (stepRc = sqlite3_step(pQ))==SQLITE_ROW ){
    const char *zName = (const char*)sqlite3_column_text(pQ, 0);
    if( !zName ) continue;
    if( n==nAlloc ){
      int nNew = nAlloc ? nAlloc*2 : 4;
      char **azNew = sqlite3_realloc(az, nNew*(int)sizeof(char*));
      if( !azNew ){ rc = SQLITE_NOMEM; break; }
      az = azNew;
      nAlloc = nNew;
    }
    az[n] = sqlite3_mprintf("%s", zName);
    if( !az[n] ){ rc = SQLITE_NOMEM; break; }
    n++;
  }
  if( rc==SQLITE_OK && stepRc!=SQLITE_DONE ) rc = stepRc;
  rc = finishConstraintStmt(pQ, rc);
  if( rc!=SQLITE_OK ){
    doltliteFreeNameList(az, n);
    return rc;
  }
  *pazCols = az;
  *pnCols = n;
  return SQLITE_OK;
}

static int notNullWalkTable(
  sqlite3 *db,
  const char *zTable,
  const char *zSql,
  struct TableEntry *aAnc, int nAnc,
  struct TableEntry *aCur, int nCur,
  void *pCtx
){
  char **azCols = 0;
  int nCols = 0;
  int rc;
  (void)zSql; (void)aCur; (void)nCur;
  rc = loadNotNullColumns(db, zTable, &azCols, &nCols);
  if( rc!=SQLITE_OK ) return rc;
  if( nCols==0 ) return SQLITE_OK;
  rc = scanMergeColumnFlagViolations(
      db, zTable, aAnc, nAnc, azCols, 0, nCols,
      DOLTLITE_CV_NOT_NULL, (int*)pCtx);
  doltliteFreeNameList(azCols, nCols);
  return rc;
}

int doltliteDetectMergeNotNullViolations(
  sqlite3 *db,
  const ProllyHash *pAncCatHash,
  char **pzErrMsg,
  int *pnFound,
  const char **azTables,
  int nTables
){
  if( pnFound ) *pnFound = 0;
  return walkMergeUserTables(db, pAncCatHash, pzErrMsg, azTables, nTables,
                             1, 0, notNullWalkTable, pnFound);
}

#endif
