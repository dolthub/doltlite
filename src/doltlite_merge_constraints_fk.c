#ifdef DOLTLITE_PROLLY

#include "doltlite_merge_constraints_int.h"

/* FK specs whose parent-side column is unnamed (azTo[i]==0) reference the
** parent's primary key positionally; fill those slots from zParent's PK. */
static int backfillParentPk(sqlite3 *db, const char *zParent,
                            char **azTo, int nCol){
  int i, needParentPk = 0;
  MergePkInfo parentPk;
  int rc;
  for(i=0; i<nCol; i++) if( !azTo[i] ) needParentPk = 1;
  if( !needParentPk ) return SQLITE_OK;
  memset(&parentPk, 0, sizeof(parentPk));
  rc = loadMergePkInfo(db, zParent, &parentPk);
  if( rc == SQLITE_OK ){
    for(i=0; i<nCol; i++){
      if( !azTo[i] && i < parentPk.nPk ){
        azTo[i] = sqlite3_mprintf("%s", parentPk.azPk[i]);
        if( !azTo[i] ){
          rc = SQLITE_NOMEM;
          break;
        }
      }else if( !azTo[i] ){
        rc = SQLITE_CORRUPT;
        break;
      }
    }
  }
  freeMergePkInfo(&parentPk);
  return rc;
}


static int tableColumnIndex(const DoltliteColInfo *pCols, const char *zName){
  int i;
  for(i=0; i<pCols->nCol; i++){
    if( pCols->azName[i] && sqlite3_stricmp(pCols->azName[i], zName)==0 ){
      return i;
    }
  }
  return -1;
}

static int recordFieldEqualsInt64(
  const u8 *pRec,
  int nRec,
  int serialType,
  int off,
  i64 v
){
  i64 got;
  int nByte;
  if( serialType==8 ) return v==0;
  if( serialType==9 ) return v==1;
  if( serialType<1 || serialType>6 ) return 0;
  nByte = dlSerialTypeLen((u64)serialType);
  if( off<0 || off+nByte>nRec ) return 0;
  got = dlReadIntBytes(pRec + off, nByte);
  return got==v;
}

static int fkParentExistsInCatalog(
  sqlite3 *db,
  struct TableEntry *aCur, int nCur,
  const char *zParentTable,
  char **azTo,
  int nCol,
  const u8 *pChildFkRec,
  int nChildFkRec,
  int *pExists
){
  ChunkStore *cs;
  ProllyCache *pCache;
  struct TableEntry *pParent;
  DoltliteColInfo parentCols;
  DoltliteRecordInfo childInfo;
  int *aiParentCol = 0;
  ProllyCursor cur;
  int res = 0;
  int rc;
  int i;

  *pExists = 0;
  if( !aCur || nCur==0 ) return SQLITE_OK;
  pParent = doltliteFindTableByName(aCur, nCur, zParentTable);
  if( !pParent || prollyHashIsEmpty(&pParent->root) ) return SQLITE_OK;

  rc = doltliteParseRecordStrict(pChildFkRec, nChildFkRec, &childInfo);
  if( rc!=SQLITE_OK ) return rc;
  if( childInfo.nField<nCol ) return SQLITE_CORRUPT;

  memset(&parentCols, 0, sizeof(parentCols));
  rc = doltliteGetColumnNames(db, zParentTable, &parentCols);
  if( rc!=SQLITE_OK ) return rc;

  aiParentCol = sqlite3_malloc64((sqlite3_int64)nCol * sizeof(int));
  if( !aiParentCol ){
    doltliteFreeColInfo(&parentCols);
    return SQLITE_NOMEM;
  }
  for(i=0; i<nCol; i++){
    aiParentCol[i] = tableColumnIndex(&parentCols, azTo[i]);
    if( aiParentCol[i]<0 ){
      sqlite3_free(aiParentCol);
      doltliteFreeColInfo(&parentCols);
      return SQLITE_ERROR;
    }
  }

  cs = doltliteGetChunkStore(db);
  pCache = doltliteGetCache(db);
  if( !cs || !pCache ){
    sqlite3_free(aiParentCol);
    doltliteFreeColInfo(&parentCols);
    return SQLITE_ERROR;
  }

  prollyCursorInit(&cur, cs, pCache, &pParent->root, pParent->flags);
  rc = prollyCursorFirst(&cur, &res);
  while( rc==SQLITE_OK && res==0 && prollyCursorIsValid(&cur) ){
    const u8 *pParentVal = 0;
    int nParentVal = 0;
    DoltliteRecordInfo parentInfo;
    int match = 1;

    prollyCursorValue(&cur, &pParentVal, &nParentVal);
    rc = doltliteParseRecordStrict(pParentVal, nParentVal, &parentInfo);
    if( rc!=SQLITE_OK ) break;

    for(i=0; i<nCol && match; i++){
      int iParent = aiParentCol[i];
      int iParentRec = parentCols.aColToRec[iParent];
      if( (pParent->flags & PROLLY_NODE_INTKEY)
       && parentCols.iPkCol==iParent ){
        match = recordFieldEqualsInt64(
            pChildFkRec, nChildFkRec,
            childInfo.aType[i], childInfo.aOffset[i],
            prollyCursorIntKey(&cur));
      }else{
        if( iParentRec<0 || iParentRec>=parentInfo.nField ){
          match = 0;
        }else{
          match = doltliteFieldValuesEqual(
              childInfo.aType[i], pChildFkRec, nChildFkRec,
              childInfo.aOffset[i],
              parentInfo.aType[iParentRec], pParentVal, nParentVal,
              parentInfo.aOffset[iParentRec]);
        }
      }
    }
    if( match ){
      *pExists = 1;
      break;
    }
    rc = prollyCursorNext(&cur);
  }
  prollyCursorClose(&cur);
  sqlite3_free(aiParentCol);
  doltliteFreeColInfo(&parentCols);
  return rc==SQLITE_DONE ? SQLITE_OK : rc;
}

static char *buildFkViolationInfo(
  sqlite3 *db,
  const char *zChildTable,
  int fkid,
  int *pRc
){
  sqlite3_stmt *pStmt = 0;
  sqlite3_str *pJson;
  sqlite3_str *pCols;
  sqlite3_str *pRefCols;
  char *zColsBuf = 0;
  char *zRefColsBuf = 0;
  char *zParentBuf = 0;
  char *zOnUpBuf = 0;
  char *zOnDelBuf = 0;
  char *zQuery;
  char *zResult;
  int rc;
  int nMatches = 0;
  int stepRc = SQLITE_DONE;

  *pRc = SQLITE_OK;

  pJson = sqlite3_str_new(0);
  pCols = sqlite3_str_new(0);
  pRefCols = sqlite3_str_new(0);

  zQuery = sqlite3_mprintf("PRAGMA main.foreign_key_list(%Q)", zChildTable);
  if( !zQuery ){
    *pRc = SQLITE_NOMEM;
  }else{
    rc = sqlite3_prepare_v2(db, zQuery, -1, &pStmt, 0);
    sqlite3_free(zQuery);
    if( rc != SQLITE_OK ){
      *pRc = rc;
    }else{
      while( (stepRc = sqlite3_step(pStmt)) == SQLITE_ROW ){
        int id = sqlite3_column_int(pStmt, 0);
        const char *zParent, *zFrom, *zTo, *zOnUp, *zOnDel;
        if( id != fkid ) continue;
        zParent = (const char*)sqlite3_column_text(pStmt, 2);
        zFrom   = (const char*)sqlite3_column_text(pStmt, 3);
        zTo     = (const char*)sqlite3_column_text(pStmt, 4);
        zOnUp   = (const char*)sqlite3_column_text(pStmt, 5);
        zOnDel  = (const char*)sqlite3_column_text(pStmt, 6);
        if( nMatches>0 ){
          sqlite3_str_appendall(pCols, ", ");
          sqlite3_str_appendall(pRefCols, ", ");
        }
        sqlite3_str_appendf(pCols, "\"%w\"", zFrom ? zFrom : "");
        sqlite3_str_appendf(pRefCols, "\"%w\"", zTo ? zTo : "");
        if( nMatches==0 ){
          if( zParent ) zParentBuf = sqlite3_mprintf("%s", zParent);
          zOnUpBuf  = sqlite3_mprintf("%s", zOnUp  ? zOnUp  : "NO ACTION");
          zOnDelBuf = sqlite3_mprintf("%s", zOnDel ? zOnDel : "NO ACTION");
          if( (zParent && !zParentBuf) || !zOnUpBuf || !zOnDelBuf ){
            *pRc = SQLITE_NOMEM;
            break;
          }
        }
        nMatches++;
      }
      if( *pRc==SQLITE_OK && stepRc!=SQLITE_DONE ) *pRc = stepRc;
    }
  }
  *pRc = finishConstraintStmt(pStmt, *pRc);

  zColsBuf    = sqlite3_str_finish(pCols);
  zRefColsBuf = sqlite3_str_finish(pRefCols);

  if( *pRc!=SQLITE_OK || !zColsBuf || !zRefColsBuf ){
    if( *pRc==SQLITE_OK ) *pRc = SQLITE_NOMEM;
    sqlite3_free(sqlite3_str_finish(pJson));
    sqlite3_free(zColsBuf);
    sqlite3_free(zRefColsBuf);
    sqlite3_free(zParentBuf);
    sqlite3_free(zOnUpBuf);
    sqlite3_free(zOnDelBuf);
    return 0;
  }

  sqlite3_str_appendall(pJson, "{");
  sqlite3_str_appendf(pJson,
      "\"Columns\": [%s], \"ReferencedTable\": \"%w\", "
      "\"ReferencedColumns\": [%s], "
      "\"OnUpdate\": \"%w\", \"OnDelete\": \"%w\"}",
      zColsBuf ? zColsBuf : "",
      zParentBuf ? zParentBuf : "",
      zRefColsBuf ? zRefColsBuf : "",
      zOnUpBuf ? zOnUpBuf : "NO ACTION",
      zOnDelBuf ? zOnDelBuf : "NO ACTION");
  zResult = sqlite3_str_finish(pJson);
  if( !zResult ) *pRc = SQLITE_NOMEM;
  sqlite3_free(zColsBuf);
  sqlite3_free(zRefColsBuf);
  sqlite3_free(zParentBuf);
  sqlite3_free(zOnUpBuf);
  sqlite3_free(zOnDelBuf);
  return zResult;
}

static int detectFkViolationsForSpec(
  sqlite3 *db,
  struct TableEntry *aCur, int nCur,
  struct TableEntry *aAnc, int nAnc,
  const char *zChildTable,
  int hasRowid,
  const MergePkInfo *pChildPk,
  const char *zParentTable,
  int fkid,
  char **azFrom,
  char **azTo,
  int nCol,
  int *pnFound
){
  sqlite3_str *pSql = 0;
  char *zQuery = 0;
  sqlite3_stmt *pStmt = 0;
  int nKeyCol;
  int rc;
  int stepRc;

  pSql = sqlite3_str_new(0);
  sqlite3_str_appendf(pSql, "SELECT %s", hasRowid ? "rowid" : pChildPk->zPkCols);
  for(int i=0; i<nCol; i++){
    sqlite3_str_appendf(pSql, ", c.\"%w\"", azFrom[i]);
  }
  sqlite3_str_appendf(pSql, " FROM main.\"%w\" AS c WHERE ", zChildTable);
  for(int i=0; i<nCol; i++){
    if( i>0 ) sqlite3_str_appendall(pSql, " AND ");
    sqlite3_str_appendf(pSql, "c.\"%w\" IS NOT NULL", azFrom[i]);
  }
  sqlite3_str_appendf(pSql, " AND NOT EXISTS (SELECT 1 FROM main.\"%w\" AS p WHERE ",
      zParentTable);
  for(int i=0; i<nCol; i++){
    if( i>0 ) sqlite3_str_appendall(pSql, " AND ");
    sqlite3_str_appendf(pSql, "p.\"%w\" = c.\"%w\"", azTo[i], azFrom[i]);
  }
  sqlite3_str_appendall(pSql, ")");
  zQuery = sqlite3_str_finish(pSql);
  if( !zQuery ) return SQLITE_NOMEM;

  rc = sqlite3_prepare_v2(db, zQuery, -1, &pStmt, 0);
  sqlite3_free(zQuery);
  if( rc!=SQLITE_OK ) return rc;
  nKeyCol = hasRowid ? 1 : pChildPk->nPk;

  while( (stepRc = sqlite3_step(pStmt))==SQLITE_ROW ){
    u8 *pKey = 0; int nKey = 0;
    u8 *pVal = 0; int nVal = 0;
    u8 *pChildFkRec = 0; int nChildFkRec = 0;
    i64 intKey = 0;
    char *zInfo;
    int appendRc;

    if( hasRowid ){
      intKey = sqlite3_column_int64(pStmt, 0);
      rc = fetchOrphanRow(db, zChildTable, intKey, &pKey, &nKey, &pVal, &nVal);
    }else{
      u8 *pPkRec = 0; int nPkRec = 0;
      pPkRec = buildRecordFromStmtCols(pStmt, 0, pChildPk->nPk, &nPkRec);
      if( !pPkRec ){ rc = SQLITE_NOMEM; break; }
      rc = fetchRowByPkFromTable(db, zChildTable, pPkRec, nPkRec, pChildPk->nPk,
                                 &pKey, &nKey, &pVal, &nVal);
      sqlite3_free(pPkRec);
    }
    if( rc == SQLITE_NOTFOUND ){ rc = SQLITE_OK; continue; }
    if( rc != SQLITE_OK ){
      sqlite3_free(pKey);
      sqlite3_free(pVal);
      break;
    }

    pChildFkRec = buildRecordFromStmtCols(pStmt, nKeyCol, nCol, &nChildFkRec);
    if( !pChildFkRec ){
      sqlite3_free(pKey);
      sqlite3_free(pVal);
      rc = SQLITE_NOMEM;
      break;
    }
    {
      int parentExists = 0;
      rc = fkParentExistsInCatalog(db, aCur, nCur, zParentTable,
                                   azTo, nCol, pChildFkRec, nChildFkRec,
                                   &parentExists);
      sqlite3_free(pChildFkRec);
      pChildFkRec = 0;
      if( rc!=SQLITE_OK ){
        sqlite3_free(pKey);
        sqlite3_free(pVal);
        break;
      }
      if( parentExists ){
        sqlite3_free(pKey);
        sqlite3_free(pVal);
        continue;
      }
    }

    if( aAnc ){
      u8 *pAncVal = 0; int nAncVal = 0;
      int ancRc = hasRowid
          ? fetchAncestorRowByName(db, aAnc, nAnc, zChildTable,
                                   intKey, &pAncVal, &nAncVal)
          : fetchAncestorRowByKey(db, aAnc, nAnc, zChildTable,
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

    zInfo = buildFkViolationInfo(db, zChildTable, fkid, &rc);
    if( rc!=SQLITE_OK ){
      sqlite3_free(pKey);
      sqlite3_free(pVal);
      break;
    }
    appendRc = doltliteAppendConstraintViolation(
        db, zChildTable, DOLTLITE_CV_FOREIGN_KEY,
        intKey, pKey, nKey, pVal, nVal, zInfo);
    sqlite3_free(zInfo);
    sqlite3_free(pKey);
    sqlite3_free(pVal);
    if( appendRc != SQLITE_OK ){
      rc = appendRc;
      break;
    }
    if( pnFound ) (*pnFound)++;
  }

  if( rc==SQLITE_OK && stepRc!=SQLITE_DONE ) rc = stepRc;
  rc = finishConstraintStmt(pStmt, rc);
  return rc;
}

int doltliteDetectMergeFkViolations(
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
  int nFound = 0;
  int stepRc;

  if( pnFound ) *pnFound = 0;

  rc = loadAncestorAndCurrentCatalogs(db, pAncCatHash, &aAnc, &nAnc,
                                      &aCur, &nCur);
  if( rc!=SQLITE_OK ) return rc;

  rc = sqlite3_prepare_v2(db,
      "SELECT name FROM main.sqlite_master WHERE type='table' "
      "AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'dolt_%'",
      -1, &pTbls, 0);
  if( rc != SQLITE_OK ){
    doltliteFreeCatalog(aAnc, nAnc);
    doltliteFreeCatalog(aCur, nCur);
    return rc;
  }

  while( (stepRc = sqlite3_step(pTbls)) == SQLITE_ROW ){
    const char *zTableRaw = (const char*)sqlite3_column_text(pTbls, 0);
    char *zTable = 0;
    char *zFkQ = 0;
    sqlite3_stmt *pFk = 0;
    int hasRowid = 1;
    MergePkInfo childPk;
    int curId = -1;
    char *zParent = 0;
    char **azFrom = 0;
    char **azTo = 0;
    int nCol = 0;
    int nAlloc = 0;
    int childChanged;
    int fkStepRc;

    if( !zTableRaw ) continue;
    zTable = sqlite3_mprintf("%s", zTableRaw);
    if( !zTable ){ rc = SQLITE_NOMEM; break; }
    if( !cvTableAllowed(zTable, azTables, nTables) ){
      sqlite3_free(zTable);
      continue;
    }
    childChanged = catalogTableChanged(aAnc, nAnc, aCur, nCur, zTable);
    memset(&childPk, 0, sizeof(childPk));
    rc = tableHasRowid(db, zTable, &hasRowid);
    if( rc!=SQLITE_OK ){
      sqlite3_free(zTable);
      break;
    }
    if( !hasRowid ){
      rc = loadMergePkInfo(db, zTable, &childPk);
      if( rc != SQLITE_OK ){
        sqlite3_free(zTable);
        break;
      }
    }

    zFkQ = sqlite3_mprintf("PRAGMA main.foreign_key_list(%Q)", zTable);
    if( !zFkQ ){
      freeMergePkInfo(&childPk);
      sqlite3_free(zTable);
      rc = SQLITE_NOMEM;
      break;
    }
    rc = sqlite3_prepare_v2(db, zFkQ, -1, &pFk, 0);
    sqlite3_free(zFkQ);
    if( rc != SQLITE_OK ){
      freeMergePkInfo(&childPk);
      sqlite3_free(zTable);
      break;
    }

    while( (fkStepRc = sqlite3_step(pFk)) == SQLITE_ROW ){
      int id = sqlite3_column_int(pFk, 0);
      const char *zParentRaw = (const char*)sqlite3_column_text(pFk, 2);
      const char *zFromRaw = (const char*)sqlite3_column_text(pFk, 3);
      const char *zToRaw = (const char*)sqlite3_column_text(pFk, 4);

      if( curId>=0 && id!=curId ){
        int parentChanged = catalogTableChanged(aAnc, nAnc, aCur, nCur, zParent);
        if( childChanged || parentChanged ){
          struct TableEntry *aCheckAnc = parentChanged ? 0 : aAnc;
          int nCheckAnc = parentChanged ? 0 : nAnc;
          rc = backfillParentPk(db, zParent, azTo, nCol);
          if( rc != SQLITE_OK ) break;
          rc = detectFkViolationsForSpec(db, aCur, nCur, aCheckAnc, nCheckAnc,
              zTable, hasRowid, &childPk, zParent, curId,
              azFrom, azTo, nCol, &nFound);
        }
        doltliteFreeStringArray(azFrom, nCol);
        doltliteFreeStringArray(azTo, nCol);
        azFrom = 0; azTo = 0; nCol = 0; nAlloc = 0;
        sqlite3_free(zParent); zParent = 0;
        if( rc != SQLITE_OK ) break;
      }

      if( curId != id ){
        curId = id;
        zParent = sqlite3_mprintf("%s", zParentRaw ? zParentRaw : "");
        if( !zParent ){ rc = SQLITE_NOMEM; break; }
      }

      if( nCol >= nAlloc ){
        int nNew = nAlloc ? nAlloc*2 : 4;
        char **azFromNew;
        char **azToNew;
        azFromNew = sqlite3_realloc64(azFrom, (sqlite3_int64)nNew * sizeof(char*));
        if( !azFromNew ){
          rc = SQLITE_NOMEM;
          break;
        }
        azFrom = azFromNew;
        azToNew = sqlite3_realloc64(azTo, (sqlite3_int64)nNew * sizeof(char*));
        if( !azToNew ){
          rc = SQLITE_NOMEM;
          break;
        }
        azTo = azToNew;
        nAlloc = nNew;
      }
      azFrom[nCol] = sqlite3_mprintf("%s", zFromRaw ? zFromRaw : "");
      azTo[nCol] = zToRaw ? sqlite3_mprintf("%s", zToRaw) : 0;
      if( !azFrom[nCol] || (zToRaw && !azTo[nCol]) ){
        rc = SQLITE_NOMEM;
        break;
      }
      nCol++;
    }

    if( rc==SQLITE_OK && fkStepRc!=SQLITE_DONE ) rc = fkStepRc;
    if( rc==SQLITE_OK && curId>=0 ){
      int parentChanged = catalogTableChanged(aAnc, nAnc, aCur, nCur, zParent);
      if( childChanged || parentChanged ){
        struct TableEntry *aCheckAnc = parentChanged ? 0 : aAnc;
        int nCheckAnc = parentChanged ? 0 : nAnc;
        rc = backfillParentPk(db, zParent, azTo, nCol);
        if( rc==SQLITE_OK ){
          rc = detectFkViolationsForSpec(db, aCur, nCur, aCheckAnc, nCheckAnc,
              zTable, hasRowid, &childPk, zParent, curId,
              azFrom, azTo, nCol, &nFound);
        }
      }
    }

    doltliteFreeStringArray(azFrom, nCol);
    doltliteFreeStringArray(azTo, nCol);
    sqlite3_free(zParent);
    rc = finishConstraintStmt(pFk, rc);
    freeMergePkInfo(&childPk);
    sqlite3_free(zTable);
    if( rc != SQLITE_OK ) break;
  }

  if( rc == SQLITE_OK && stepRc != SQLITE_DONE && stepRc != SQLITE_ROW ){
    rc = stepRc;
  }
  rc = finishConstraintStmt(pTbls, rc);
  doltliteFreeCatalog(aAnc, nAnc);
  doltliteFreeCatalog(aCur, nCur);
  if( pnFound ) *pnFound = nFound;
  setConstraintError(db, pzErrMsg, rc);
  return rc;
}


#endif
