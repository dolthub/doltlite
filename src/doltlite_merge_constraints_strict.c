#ifdef DOLTLITE_PROLLY

#include "doltlite_merge_constraints_int.h"

/* Merge can pair a STRICT schema with a forbidden stored type. NULL
** is the NOT NULL detector's; ANY admits everything. */

/* STRICT storage class, or 0 for ANY. */
static const char *strictAllowedTypeof(const char *zDecl){
  if( sqlite3_stricmp(zDecl, "INT")==0
   || sqlite3_stricmp(zDecl, "INTEGER")==0 ){
    return "integer";
  }
  if( sqlite3_stricmp(zDecl, "TEXT")==0 ) return "text";
  if( sqlite3_stricmp(zDecl, "REAL")==0 ) return "real";
  if( sqlite3_stricmp(zDecl, "BLOB")==0 ) return "blob";
  return 0;
}

static int tableIsStrict(sqlite3 *db, const char *zTable, int *pStrict){
  sqlite3_stmt *pQ = 0;
  char *zSql;
  int rc;
  int stepRc;

  *pStrict = 0;
  zSql = sqlite3_mprintf(
      "SELECT strict FROM pragma_table_list WHERE schema='main' AND name=%Q",
      zTable);
  if( !zSql ) return SQLITE_NOMEM;
  rc = sqlite3_prepare_v2(db, zSql, -1, &pQ, 0);
  sqlite3_free(zSql);
  if( rc!=SQLITE_OK ) return rc;
  stepRc = sqlite3_step(pQ);
  if( stepRc==SQLITE_ROW ){
    *pStrict = sqlite3_column_int(pQ, 0);
    rc = SQLITE_OK;
  }else{
    rc = stepRc==SQLITE_DONE ? SQLITE_NOTFOUND : stepRc;
  }
  return finishConstraintStmt(pQ, rc);
}

static int loadStrictColumns(
  sqlite3 *db,
  const char *zTable,
  char ***pazCols,
  char ***pazAllowed,
  int *pnCols
){
  sqlite3_stmt *pQ = 0;
  char *zSql;
  char **azCols = 0;
  char **azAllowed = 0;
  int n = 0;
  int nAlloc = 0;
  int rc;
  int stepRc;

  *pazCols = 0;
  *pazAllowed = 0;
  *pnCols = 0;
  zSql = sqlite3_mprintf(
      "SELECT name, type FROM pragma_table_info(%Q)", zTable);
  if( !zSql ) return SQLITE_NOMEM;
  rc = sqlite3_prepare_v2(db, zSql, -1, &pQ, 0);
  sqlite3_free(zSql);
  if( rc!=SQLITE_OK ) return rc;

  while( (stepRc = sqlite3_step(pQ))==SQLITE_ROW ){
    const char *zName = (const char*)sqlite3_column_text(pQ, 0);
    const char *zType = (const char*)sqlite3_column_text(pQ, 1);
    const char *zAllowed;
    if( !zName || !zType ) continue;
    zAllowed = strictAllowedTypeof(zType);
    if( !zAllowed ) continue;
    if( n==nAlloc ){
      int nNew = nAlloc ? nAlloc*2 : 4;
      char **azNew = sqlite3_realloc(azCols, nNew*(int)sizeof(char*));
      char **azNew2 = azNew
          ? sqlite3_realloc(azAllowed, nNew*(int)sizeof(char*)) : 0;
      if( azNew ) azCols = azNew;
      if( !azNew2 ){ rc = SQLITE_NOMEM; break; }
      azAllowed = azNew2;
      nAlloc = nNew;
    }
    azCols[n] = sqlite3_mprintf("%s", zName);
    azAllowed[n] = sqlite3_mprintf("%s", zAllowed);
    if( !azCols[n] || !azAllowed[n] ){
      sqlite3_free(azCols[n]);
      sqlite3_free(azAllowed[n]);
      rc = SQLITE_NOMEM;
      break;
    }
    n++;
  }
  if( rc==SQLITE_OK && stepRc!=SQLITE_DONE ) rc = stepRc;
  rc = finishConstraintStmt(pQ, rc);
  if( rc!=SQLITE_OK ){
    doltliteFreeNameList(azCols, n);
    doltliteFreeNameList(azAllowed, n);
    return rc;
  }
  *pazCols = azCols;
  *pazAllowed = azAllowed;
  *pnCols = n;
  return SQLITE_OK;
}

static int strictWalkTable(
  sqlite3 *db,
  const char *zTable,
  const char *zSql,
  struct TableEntry *aAnc, int nAnc,
  struct TableEntry *aCur, int nCur,
  void *pCtx
){
  char **azCols = 0;
  char **azAllowed = 0;
  int nCols = 0;
  int isStrict;
  int rc;
  (void)zSql; (void)aCur; (void)nCur;
  rc = tableIsStrict(db, zTable, &isStrict);
  if( rc!=SQLITE_OK ) return rc;
  if( !isStrict ) return SQLITE_OK;
  rc = loadStrictColumns(db, zTable, &azCols, &azAllowed, &nCols);
  if( rc!=SQLITE_OK ) return rc;
  if( nCols==0 ) return SQLITE_OK;
  rc = scanMergeColumnFlagViolations(
      db, zTable, aAnc, nAnc, azCols, azAllowed, nCols,
      DOLTLITE_CV_STRICT_TYPE, (int*)pCtx);
  doltliteFreeNameList(azCols, nCols);
  doltliteFreeNameList(azAllowed, nCols);
  return rc;
}

int doltliteDetectMergeStrictViolations(
  sqlite3 *db,
  const ProllyHash *pAncCatHash,
  char **pzErrMsg,
  int *pnFound,
  const char **azTables,
  int nTables
){
  if( pnFound ) *pnFound = 0;
  return walkMergeUserTables(db, pAncCatHash, pzErrMsg, azTables, nTables,
                             1, 0, strictWalkTable, pnFound);
}

#endif
