#ifdef DOLTLITE_PROLLY

#include "doltlite_merge_constraints_int.h"

/* STRICT type violations left behind by a merge.
**
** A merge can bring together a STRICT table schema and a row from the other
** side whose stored type the declared column type forbids. Neither side
** violated anything on its own, so nothing along the write path objects, and
** the row lands: integrity_check reports it, but nothing records a violation
** and the commit is clean.
**
** The predicate is typeof(c) against the declared type's allowed storage
** class. NULL passes here — the NOT NULL detector owns missing values — and
** ANY columns allow everything, so neither generates a term.
*/

static void freeNames(char **az, int n){
  int i;
  for(i=0; i<n; i++) sqlite3_free(az[i]);
  sqlite3_free(az);
}

/* The storage class a STRICT column admits, or 0 for ANY. Declared types in
** a STRICT table are exactly INT, INTEGER, TEXT, REAL, BLOB, ANY. */
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

/* True when zTable is declared STRICT. */
static int tableIsStrict(sqlite3 *db, const char *zTable){
  sqlite3_stmt *pQ = 0;
  char *zSql;
  int strict = 0;

  zSql = sqlite3_mprintf(
      "SELECT strict FROM pragma_table_list WHERE schema='main' AND name=%Q",
      zTable);
  if( !zSql ) return 0;
  if( sqlite3_prepare_v2(db, zSql, -1, &pQ, 0)==SQLITE_OK
   && sqlite3_step(pQ)==SQLITE_ROW ){
    strict = sqlite3_column_int(pQ, 0);
  }
  sqlite3_free(zSql);
  sqlite3_finalize(pQ);
  return strict;
}

/* The typed columns of a STRICT table with the storage class each admits. */
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

  *pazCols = 0;
  *pazAllowed = 0;
  *pnCols = 0;
  zSql = sqlite3_mprintf(
      "SELECT name, type FROM pragma_table_info(%Q)", zTable);
  if( !zSql ) return SQLITE_NOMEM;
  rc = sqlite3_prepare_v2(db, zSql, -1, &pQ, 0);
  sqlite3_free(zSql);
  if( rc!=SQLITE_OK ) return rc;

  while( sqlite3_step(pQ)==SQLITE_ROW ){
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
  sqlite3_finalize(pQ);
  if( rc!=SQLITE_OK ){
    freeNames(azCols, n);
    freeNames(azAllowed, n);
    return rc;
  }
  *pazCols = azCols;
  *pazAllowed = azAllowed;
  *pnCols = n;
  return SQLITE_OK;
}

int doltliteDetectMergeStrictViolations(
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

  (void)pzErrMsg;
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
    char **azAllowed = 0;
    int nCols = 0;
    int hasRowid;
    int nKeyCol;
    MergePkInfo pkInfo;
    sqlite3_str *pStr;
    char *zQuery = 0;
    sqlite3_stmt *pQ = 0;
    int i;

    if( !zTableRaw ) continue;
    zTable = sqlite3_mprintf("%s", zTableRaw);
    if( !zTable ){ rc = SQLITE_NOMEM; break; }
    if( !cvTableAllowed(zTable, azTables, nTables)
     || !catalogTableChanged(aAnc, nAnc, aCur, nCur, zTable)
     || !tableIsStrict(db, zTable) ){
      sqlite3_free(zTable);
      continue;
    }

    rc = loadStrictColumns(db, zTable, &azCols, &azAllowed, &nCols);
    if( rc!=SQLITE_OK ){
      sqlite3_free(zTable);
      break;
    }
    if( nCols==0 ){
      sqlite3_free(zTable);
      continue;
    }

    memset(&pkInfo, 0, sizeof(pkInfo));
    hasRowid = tableHasRowid(db, zTable);
    if( !hasRowid ){
      rc = loadMergePkInfo(db, zTable, &pkInfo);
      if( rc!=SQLITE_OK ){
        freeNames(azCols, nCols);
        freeNames(azAllowed, nCols);
        sqlite3_free(zTable);
        break;
      }
    }
    nKeyCol = hasRowid ? 1 : pkInfo.nPk;

    /* One row per offending row, carrying a flag per typed column so every
    ** column holding a forbidden storage class is named. */
    pStr = sqlite3_str_new(db);
    sqlite3_str_appendf(pStr, "SELECT %s",
                        hasRowid ? "rowid" : pkInfo.zPkCols);
    for(i=0; i<nCols; i++){
      sqlite3_str_appendf(pStr, ", typeof(\"%w\") NOT IN ('null','%s')",
                          azCols[i], azAllowed[i]);
    }
    sqlite3_str_appendf(pStr, " FROM main.\"%w\" NOT INDEXED WHERE 0", zTable);
    for(i=0; i<nCols; i++){
      sqlite3_str_appendf(pStr, " OR typeof(\"%w\") NOT IN ('null','%s')",
                          azCols[i], azAllowed[i]);
    }
    zQuery = sqlite3_str_finish(pStr);
    if( !zQuery ){
      freeNames(azCols, nCols);
      freeNames(azAllowed, nCols);
      freeMergePkInfo(&pkInfo);
      sqlite3_free(zTable);
      rc = SQLITE_NOMEM;
      break;
    }
    rc = sqlite3_prepare_v2(db, zQuery, -1, &pQ, 0);
    sqlite3_free(zQuery);
    if( rc!=SQLITE_OK ){
      /* A table the merge left unreadable is the other detectors' business. */
      freeNames(azCols, nCols);
      freeNames(azAllowed, nCols);
      freeMergePkInfo(&pkInfo);
      sqlite3_free(zTable);
      rc = SQLITE_OK;
      continue;
    }

    while( sqlite3_step(pQ)==SQLITE_ROW ){
      u8 *pKey = 0; int nKey = 0;
      u8 *pVal = 0; int nVal = 0;
      i64 intKey = 0;
      sqlite3_str *pInfo;
      char *zInfo;
      int nNamed = 0;
      int appendRc;

      if( hasRowid ){
        intKey = sqlite3_column_int64(pQ, 0);
        rc = fetchOrphanRow(db, zTable, intKey, &pKey, &nKey, &pVal, &nVal);
      }else{
        u8 *pPkRec = 0; int nPkRec = 0;
        pPkRec = buildRecordFromStmtCols(pQ, 0, pkInfo.nPk, &nPkRec);
        if( !pPkRec ){ rc = SQLITE_NOMEM; break; }
        rc = fetchRowByPkFromTable(db, zTable, pPkRec, nPkRec, pkInfo.nPk,
                                   &pKey, &nKey, &pVal, &nVal);
        sqlite3_free(pPkRec);
      }
      if( rc==SQLITE_NOTFOUND ){ rc = SQLITE_OK; continue; }
      if( rc!=SQLITE_OK ){
        sqlite3_free(pKey);
        sqlite3_free(pVal);
        break;
      }

      /* A row that already held the forbidden type before the merge is not
      ** the merge's doing, same rule the other detectors apply. */
      if( aAnc ){
        u8 *pAncVal = 0; int nAncVal = 0;
        int ancRc = hasRowid
            ? fetchAncestorRowByName(db, aAnc, nAnc, zTable,
                                     intKey, &pAncVal, &nAncVal)
            : fetchAncestorRowByKey(db, aAnc, nAnc, zTable,
                                    pKey, nKey, &pAncVal, &nAncVal);
        int preExisting = (ancRc==SQLITE_OK)
            && isRowPreExisting(pVal, nVal, pAncVal, nAncVal);
        sqlite3_free(pAncVal);
        if( preExisting ){
          sqlite3_free(pKey);
          sqlite3_free(pVal);
          continue;
        }
      }

      pInfo = sqlite3_str_new(db);
      sqlite3_str_appendall(pInfo, "{\"Columns\": [");
      for(i=0; i<nCols; i++){
        if( sqlite3_column_int(pQ, nKeyCol + i) ){
          sqlite3_str_appendf(pInfo, "%s\"%w\"", nNamed ? ", " : "", azCols[i]);
          nNamed++;
        }
      }
      sqlite3_str_appendall(pInfo, "]}");
      zInfo = sqlite3_str_finish(pInfo);
      if( !zInfo ){
        sqlite3_free(pKey);
        sqlite3_free(pVal);
        rc = SQLITE_NOMEM;
        break;
      }
      appendRc = doltliteAppendConstraintViolation(
          db, zTable, DOLTLITE_CV_STRICT_TYPE,
          intKey, pKey, nKey, pVal, nVal, zInfo);
      sqlite3_free(zInfo);
      sqlite3_free(pKey);
      sqlite3_free(pVal);
      if( appendRc!=SQLITE_OK ){ rc = appendRc; break; }
      if( pnFound ) (*pnFound)++;
    }

    sqlite3_finalize(pQ);
    freeNames(azCols, nCols);
    freeNames(azAllowed, nCols);
    freeMergePkInfo(&pkInfo);
    sqlite3_free(zTable);
    if( rc!=SQLITE_OK ) break;
  }
  if( rc==SQLITE_OK && stepRc!=SQLITE_DONE && stepRc!=SQLITE_ROW ){
    rc = stepRc;
  }
  sqlite3_finalize(pTbls);
  doltliteFreeCatalog(aAnc, nAnc);
  doltliteFreeCatalog(aCur, nCur);
  return rc;
}

#endif
