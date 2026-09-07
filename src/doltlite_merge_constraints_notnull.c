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
      "SELECT name FROM pragma_table_info(%Q) WHERE \"notnull\"=1", zTable);
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

int doltliteDetectMergeNotNullViolations(
  sqlite3 *db,
  const ProllyHash *pAncCatHash,
  char **pzErrMsg,
  int *pnFound,
  const char **azTables,
  int nTables
){
  sqlite3_stmt *pTbls = 0;
  struct TableEntry *aAnc = 0;
  int nAnc = 0;
  struct TableEntry *aCur = 0;
  int nCur = 0;
  int rc;
  int stepRc;

  if( pnFound ) *pnFound = 0;

  rc = loadAncestorAndCurrentCatalogs(db, pAncCatHash, &aAnc, &nAnc,
                                      &aCur, &nCur);
  if( rc!=SQLITE_OK ) return rc;

  rc = sqlite3_prepare_v2(db,
      "SELECT name FROM main.sqlite_master WHERE type='table' "
      "AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'dolt_%'",
      -1, &pTbls, 0);
  if( rc!=SQLITE_OK ){
    doltliteFreeCatalog(aAnc, nAnc);
    doltliteFreeCatalog(aCur, nCur);
    return rc;
  }

  while( (stepRc = sqlite3_step(pTbls))==SQLITE_ROW ){
    const char *zTableRaw = (const char*)sqlite3_column_text(pTbls, 0);
    char *zTable;
    char **azCols = 0;
    int nCols = 0;

    if( !zTableRaw ) continue;
    zTable = sqlite3_mprintf("%s", zTableRaw);
    if( !zTable ){ rc = SQLITE_NOMEM; break; }
    if( !cvTableAllowed(zTable, azTables, nTables)
     || !catalogTableChanged(aAnc, nAnc, aCur, nCur, zTable) ){
      sqlite3_free(zTable);
      continue;
    }

    rc = loadNotNullColumns(db, zTable, &azCols, &nCols);
    if( rc!=SQLITE_OK ){
      sqlite3_free(zTable);
      break;
    }
    if( nCols==0 ){
      sqlite3_free(zTable);
      continue;
    }

    rc = scanMergeColumnFlagViolations(
        db, zTable, aAnc, nAnc, azCols, 0, nCols,
        DOLTLITE_CV_NOT_NULL, pnFound);
    doltliteFreeNameList(azCols, nCols);
    sqlite3_free(zTable);
    if( rc!=SQLITE_OK ) break;
  }
  if( rc==SQLITE_OK && stepRc!=SQLITE_DONE && stepRc!=SQLITE_ROW ){
    rc = stepRc;
  }
  rc = finishConstraintStmt(pTbls, rc);
  doltliteFreeCatalog(aAnc, nAnc);
  doltliteFreeCatalog(aCur, nCur);
  setConstraintError(db, pzErrMsg, rc);
  return rc;
}

#endif
