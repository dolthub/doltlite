#ifdef DOLTLITE_PROLLY

#include "doltlite_merge_constraints_int.h"
#include "vdbeInt.h"

/* Unnamed azTo slots are the parent PK by position. */
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

typedef struct FkParentLookup FkParentLookup;
struct FkParentLookup {
  int ready;
  int isIntPk;
  int cursorOpen;
  int nCol;
  struct TableEntry *pParent;
  Table *pTab;
  DoltliteColInfo cols;
  int *aiCol;
  ProllyCursor cur;
  Btree *pIndex;
  BtCursor *pIndexCur;
  KeyInfo *pKeyInfo;
  UnpackedRecord *pProbe;
};

static void fkParentLookupClear(sqlite3 *db, FkParentLookup *p){
  if( p->cursorOpen ){
    sqlite3BtreeCloseCursor(p->pIndexCur);
  }else if( p->pIndex ){
    sqlite3BtreeClose(p->pIndex);
  }
  sqlite3_free(p->pIndexCur);
  if( p->pProbe ){
    for(int i=0; i<p->nCol; i++){
      sqlite3VdbeMemRelease(&p->pProbe->aMem[i]);
    }
    sqlite3DbFree(db, p->pProbe);
  }
  sqlite3KeyInfoUnref(p->pKeyInfo);
  if( p->ready && p->pParent ) prollyCursorClose(&p->cur);
  sqlite3_free(p->aiCol);
  doltliteFreeColInfo(&p->cols);
}

static int fkParentLookupInit(
  sqlite3 *db,
  struct TableEntry *aCur, int nCur,
  const char *zParentTable,
  char **azTo, int nCol,
  FkParentLookup *p
){
  Table *pTab;
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  DoltliteSerialValue *aValue = 0;
  Pgno root;
  int rc, res = 0;

  p->pParent = doltliteFindTableByName(aCur, nCur, zParentTable);
  if( !p->pParent || prollyHashIsEmpty(&p->pParent->root) ){
    p->pParent = 0;
    p->ready = 1;
    return SQLITE_OK;
  }
  if( !cs || !pCache ) return SQLITE_ERROR;
  prollyCursorInit(&p->cur, cs, pCache, &p->pParent->root, p->pParent->flags);
  p->ready = 1;
  p->nCol = nCol;
  rc = doltliteGetColumnNames(db, zParentTable, &p->cols);
  if( rc!=SQLITE_OK ) return rc;
  pTab = sqlite3FindTable(db, zParentTable, "main");
  if( !pTab ) return SQLITE_ERROR;
  p->pTab = pTab;
  p->aiCol = sqlite3_malloc64((sqlite3_int64)nCol * sizeof(int));
  p->pKeyInfo = sqlite3KeyInfoAlloc(db, nCol, 0);
  if( !p->aiCol || !p->pKeyInfo ) return SQLITE_NOMEM;
  for(int i=0; i<nCol; i++){
    int col = tableColumnIndex(&p->cols, azTo[i]);
    const char *zColl;
    if( col<0 || col>=pTab->nCol ) return SQLITE_ERROR;
    p->aiCol[i] = col;
    zColl = sqlite3ColumnColl(&pTab->aCol[col]);
    p->pKeyInfo->aColl[i] = sqlite3FindCollSeq(db, ENC(db),
                                            zColl ? zColl : "BINARY", 0);
    if( !p->pKeyInfo->aColl[i] || !p->pKeyInfo->aColl[i]->xCmp ){
      return SQLITE_ERROR;
    }
  }
  p->pProbe = sqlite3VdbeAllocUnpackedRecord(p->pKeyInfo);
  if( !p->pProbe ) return SQLITE_NOMEM;
  p->pProbe->nField = nCol;
  p->pProbe->default_rc = 0;
  for(int i=0; i<nCol; i++){
    sqlite3VdbeMemInit(&p->pProbe->aMem[i], db, MEM_Null);
  }
  p->isIntPk = nCol==1 && (p->pParent->flags & PROLLY_NODE_INTKEY)
      && p->cols.iPkCol==p->aiCol[0];
  if( p->isIntPk ) return SQLITE_OK;

  /* Rebuilt secondary indexes need not yet reflect the merged table root. */
  rc = sqlite3BtreeOpen(db->pVfs, 0, db, &p->pIndex,
      BTREE_OMIT_JOURNAL | BTREE_SINGLE,
      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_EXCLUSIVE
      | SQLITE_OPEN_DELETEONCLOSE | SQLITE_OPEN_TRANSIENT_DB);
  if( rc==SQLITE_OK ) rc = sqlite3BtreeBeginTrans(p->pIndex, 1, 0);
  if( rc==SQLITE_OK ) rc = sqlite3BtreeCreateTable(p->pIndex, &root, BTREE_BLOBKEY);
  if( rc!=SQLITE_OK ) return rc;
  p->pIndexCur = sqlite3MallocZero(sqlite3BtreeCursorSize());
  if( !p->pIndexCur ) return SQLITE_NOMEM;
  rc = sqlite3BtreeCursor(p->pIndex, root, BTREE_WRCSR, p->pKeyInfo, p->pIndexCur);
  if( rc!=SQLITE_OK ) return rc;
  p->cursorOpen = 1;
  aValue = sqlite3_malloc64((sqlite3_int64)nCol * sizeof(*aValue));
  if( !aValue ) return SQLITE_NOMEM;
  rc = prollyCursorFirst(&p->cur, &res);
  while( rc==SQLITE_OK && res==0 && prollyCursorIsValid(&p->cur) ){
    const u8 *pVal;
    int nVal;
    u8 *pDecoded = 0;
    u8 *pRecord = 0;
    int nRecord = 0;
    DoltliteRecordInfo info = {0};
    BtreePayload payload;
    prollyCursorValue(&p->cur, &pVal, &nVal);
    if( nVal==0 && (p->pParent->flags & PROLLY_NODE_INTKEY)==0 ){
      const u8 *pKey;
      int nKey;
      prollyCursorKey(&p->cur, &pKey, &nKey);
      rc = doltliteRecordFromClusteredKeyCols(db, &p->cols,
          pKey, nKey, &pDecoded, &nVal);
      pVal = pDecoded;
    }
    if( rc==SQLITE_OK ) rc = doltliteParseRecordStrict(pVal, nVal, &info);
    memset(aValue, 0, (size_t)nCol * sizeof(*aValue));
    for(int i=0; i<nCol && rc==SQLITE_OK; i++){
      int col = p->aiCol[i];
      if( (p->pParent->flags & PROLLY_NODE_INTKEY) && col==p->cols.iPkCol ){
        aValue[i].eType = SQLITE_INTEGER;
        aValue[i].i = prollyCursorIntKey(&p->cur);
      }else{
        rc = doltliteSerialValueFromField(pVal, nVal, &info,
                                          p->cols.aColToRec[col], &aValue[i]);
      }
    }
    if( rc==SQLITE_OK ){
      pRecord = doltliteBuildRecord(aValue, nCol, &nRecord);
      if( !pRecord ) rc = SQLITE_NOMEM;
    }
    if( rc==SQLITE_OK ){
      memset(&payload, 0, sizeof(payload));
      payload.pKey = pRecord;
      payload.nKey = nRecord;
      rc = sqlite3BtreeInsert(p->pIndexCur, &payload, 0, 0);
    }
    sqlite3_free(pRecord);
    sqlite3_free(pDecoded);
    doltliteRecordInfoClear(&info);
    if( rc==SQLITE_OK ) rc = prollyCursorNext(&p->cur);
  }
  sqlite3_free(aValue);
  prollyCursorClose(&p->cur);
  return rc==SQLITE_DONE ? SQLITE_OK : rc;
}

static int fkParentExistsInCatalog(
  sqlite3 *db,
  FkParentLookup *p,
  sqlite3_stmt *pStmt,
  int iFirst,
  int *pExists
){
  int rc = SQLITE_OK;
  int res = 0;
  *pExists = 0;
  if( !p->pParent ) return SQLITE_OK;
  for(int i=0; i<p->nCol; i++){
    Mem *pValue = &p->pProbe->aMem[i];
    rc = sqlite3VdbeMemCopy(pValue, (Mem*)sqlite3_column_value(pStmt, iFirst+i));
    if( rc!=SQLITE_OK ) return rc;
    sqlite3ValueApplyAffinity(pValue, p->pTab->aCol[p->aiCol[i]].affinity, ENC(db));
    if( db->mallocFailed ) return SQLITE_NOMEM;
  }
  if( p->isIntPk ){
    Mem *pValue = &p->pProbe->aMem[0];
    if( (pValue->flags & MEM_Int)==0 ) return SQLITE_OK;
    rc = prollyCursorSeekInt(&p->cur, pValue->u.i, &res);
    *pExists = rc==SQLITE_OK && res==0 && prollyCursorIsValid(&p->cur);
  }else{
    p->pProbe->errCode = 0;
    rc = sqlite3BtreeIndexMoveto(p->pIndexCur, p->pProbe, &res);
    if( rc==SQLITE_OK && p->pProbe->errCode ) rc = p->pProbe->errCode;
    *pExists = rc==SQLITE_OK && res==0;
  }
  return rc;
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
  FkParentLookup parent = {0};
  char *zInfo = 0;
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
    /* Only the parent key contributes comparison affinity. */
    sqlite3_str_appendf(pSql, "p.\"%w\" = +c.\"%w\"", azTo[i], azFrom[i]);
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
    i64 intKey = 0;
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

    {
      int parentExists = 0;
      if( !parent.ready ){
        rc = fkParentLookupInit(db, aCur, nCur, zParentTable, azTo, nCol, &parent);
      }
      if( rc==SQLITE_OK ){
        rc = fkParentExistsInCatalog(db, &parent, pStmt, nKeyCol, &parentExists);
      }
      if( rc!=SQLITE_OK || parentExists ){
        sqlite3_free(pKey);
        sqlite3_free(pVal);
        if( rc!=SQLITE_OK ) break;
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

    if( !zInfo ) zInfo = buildFkViolationInfo(db, zChildTable, fkid, &rc);
    if( rc!=SQLITE_OK ){
      sqlite3_free(pKey);
      sqlite3_free(pVal);
      break;
    }
    appendRc = doltliteAppendConstraintViolation(
        db, zChildTable, DOLTLITE_CV_FOREIGN_KEY,
        intKey, pKey, nKey, pVal, nVal, zInfo);
    sqlite3_free(pKey);
    sqlite3_free(pVal);
    if( appendRc != SQLITE_OK ){
      rc = appendRc;
      break;
    }
    if( pnFound ) (*pnFound)++;
  }

  if( rc==SQLITE_OK && stepRc!=SQLITE_DONE ) rc = stepRc;
  fkParentLookupClear(db, &parent);
  sqlite3_free(zInfo);
  rc = finishConstraintStmt(pStmt, rc);
  return rc;
}

typedef struct FkWalk FkWalk;
struct FkWalk {
  int *pnFound;
  char **pzErrMsg;
};

static int fkWalkTable(
  sqlite3 *db,
  const char *zTable,
  const char *zSql,
  struct TableEntry *aAnc, int nAnc,
  struct TableEntry *aCur, int nCur,
  void *pCtx
){
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
  int rc;
  FkWalk *pWalk = (FkWalk*)pCtx;
  (void)zSql;

  childChanged = catalogTableChanged(aAnc, nAnc, aCur, nCur, zTable);
  memset(&childPk, 0, sizeof(childPk));
  rc = tableHasRowid(db, zTable, &hasRowid);
  if( rc!=SQLITE_OK ) return rc;
  if( !hasRowid ){
    rc = loadMergePkInfo(db, zTable, &childPk);
    if( rc != SQLITE_OK ) return rc;
  }

  zFkQ = sqlite3_mprintf("PRAGMA main.foreign_key_list(%Q)", zTable);
  if( !zFkQ ){
    freeMergePkInfo(&childPk);
    return SQLITE_NOMEM;
  }
  rc = sqlite3_prepare_v2(db, zFkQ, -1, &pFk, 0);
  sqlite3_free(zFkQ);
  if( rc != SQLITE_OK ){
    freeMergePkInfo(&childPk);
    return rc;
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
            azFrom, azTo, nCol, pWalk->pnFound);
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
            azFrom, azTo, nCol, pWalk->pnFound);
      }
    }
  }

  doltliteFreeStringArray(azFrom, nCol);
  doltliteFreeStringArray(azTo, nCol);
  sqlite3_free(zParent);
  setConstraintError(db, pWalk->pzErrMsg, rc);
  rc = finishConstraintStmt(pFk, rc);
  freeMergePkInfo(&childPk);
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
  FkWalk walk;
  walk.pnFound = pnFound;
  walk.pzErrMsg = pzErrMsg;
  if( pnFound ) *pnFound = 0;
  return walkMergeUserTables(db, pAncCatHash, pzErrMsg, azTables, nTables,
                             0, 0, fkWalkTable, &walk);
}


#endif
