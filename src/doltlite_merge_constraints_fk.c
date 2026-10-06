#ifdef DOLTLITE_PROLLY

#include "doltlite_merge_constraints_int.h"
#include "doltlite_merge_int.h"
#include "vdbeInt.h"
#include <ctype.h>

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

/* Declared column, including generated ones. Column-info indexes skip
** those, so they are not subscripts of Table.aCol. */
static int tableDeclIndex(const Table *pTab, const char *zName){
  int i;
  if( !pTab || !zName ) return -1;
  for(i=0; i<pTab->nCol; i++){
    const char *zCol = pTab->aCol[i].zCnName;
    if( zCol && sqlite3_stricmp(zCol, zName)==0 ) return i;
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
  int *aiCol;       /* Index in cols, which omits generated columns */
  int *aiDecl;      /* Index in Table.aCol, collation and affinity */
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
  sqlite3_free(p->aiDecl);
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
  p->aiDecl = sqlite3_malloc64((sqlite3_int64)nCol * sizeof(int));
  p->pKeyInfo = sqlite3KeyInfoAlloc(db, nCol, 0);
  if( !p->aiCol || !p->aiDecl || !p->pKeyInfo ) return SQLITE_NOMEM;
  for(int i=0; i<nCol; i++){
    int col = tableColumnIndex(&p->cols, azTo[i]);
    int decl = tableDeclIndex(pTab, azTo[i]);
    const char *zColl;
    if( col<0 || decl<0 ) return SQLITE_ERROR;
    p->aiCol[i] = col;
    p->aiDecl[i] = decl;
    zColl = sqlite3ColumnColl(&pTab->aCol[decl]);
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
    sqlite3ValueApplyAffinity(pValue, p->pTab->aCol[p->aiDecl[i]].affinity, ENC(db));
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
  int *pnFound,
  char **pzErrMsg
){
  sqlite3_str *pSql = 0;
  char *zQuery = 0;
  sqlite3_stmt *pStmt = 0;
  int nKeyCol;
  char *zRowid = 0;
  FkParentLookup parent = {0};
  char *zInfo = 0;
  int rc;
  int stepRc;

  if( hasRowid ){
    rc = loadMergeRowidSql(db, zChildTable, &zRowid, pzErrMsg);
    if( rc!=SQLITE_OK ) return rc;
  }
  pSql = sqlite3_str_new(0);
  sqlite3_str_appendf(pSql, "SELECT %s", hasRowid ? zRowid : pChildPk->zPkCols);
  sqlite3_free(zRowid);
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
  MergeConstraintWalk *pWalk = (MergeConstraintWalk*)pCtx;
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
            azFrom, azTo, nCol, pWalk->pnFound, pWalk->pzErrMsg);
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
            azFrom, azTo, nCol, pWalk->pnFound, pWalk->pzErrMsg);
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
  MergeConstraintWalk walk;
  walk.pnFound = pnFound;
  walk.pzErrMsg = pzErrMsg;
  if( pnFound ) *pnFound = 0;
  return walkMergeUserTables(db, pAncCatHash, pzErrMsg, azTables, nTables,
                             0, 0, fkWalkTable, &walk);
}



/* Clause text shared with check-constraint merge. */
static int dlNext(const char **pz, const char *zEnd, int *pType){
  int n;
  while( *pz<zEnd && **pz ){
    n = sqlite3GetToken((const u8*)*pz, pType);
    if( n<=0 || *pType==TK_ILLEGAL ) return -1;
    if( *pz + n > zEnd ) return 0;
    if( *pType!=TK_SPACE && *pType!=TK_COMMENT ) return n;
    *pz += n;
  }
  return 0;
}

static int dlSkipParen(const char **pz, const char *zEnd){
  int type, n, depth = 0;
  const char *p;
  n = dlNext(pz, zEnd, &type);
  if( n<=0 || type!=TK_LP ) return SQLITE_CORRUPT;
  p = *pz;
  while( p<zEnd ){
    int t, k = sqlite3GetToken((const u8*)p, &t);
    if( k<=0 || p+k>zEnd ) return SQLITE_CORRUPT;
    if( t==TK_LP ) depth++;
    if( t==TK_RP ){
      depth--;
      p += k;
      if( depth==0 ){ *pz = p; return SQLITE_OK; }
      continue;
    }
    p += k;
  }
  return SQLITE_CORRUPT;
}

static int dlSkipDefault(const char **pz, const char *zEnd){
  int type, n;
  n = dlNext(pz, zEnd, &type);
  if( n<=0 ) return n<0 ? SQLITE_CORRUPT : SQLITE_OK;
  if( type==TK_LP ) return dlSkipParen(pz, zEnd);
  if( type==TK_PLUS || type==TK_MINUS ){
    *pz += n;
    n = dlNext(pz, zEnd, &type);
    if( n<=0 ) return n<0 ? SQLITE_CORRUPT : SQLITE_OK;
  }
  *pz += n;
  return SQLITE_OK;
}

static int dlRefStop(const char *z, const char *zEnd, int type, int n){
  if( type==TK_CONSTRAINT || type==TK_PRIMARY || type==TK_UNIQUE
   || type==TK_CHECK || type==TK_DEFAULT || type==TK_COLLATE
   || type==TK_FOREIGN || type==TK_AS ) return 1;
  if( n==9 && sqlite3_strnicmp(z, "GENERATED", 9)==0 ) return 1;
  if( type==TK_NOT ){
    const char *q = z + n;
    int t2, n2 = dlNext(&q, zEnd, &t2);
    if( n2>0 && t2==TK_NULL ) return 1;
  }
  return 0;
}

static int dlSkipReferences(const char **pz, const char *zEnd){
  int type, n, depth = 0;
  while( (n = dlNext(pz, zEnd, &type))>0 ){
    if( depth==0 && dlRefStop(*pz, zEnd, type, n) ) return SQLITE_OK;
    if( type==TK_LP ) depth++;
    else if( type==TK_RP && depth>0 ) depth--;
    *pz += n;
  }
  return n<0 ? SQLITE_CORRUPT : SQLITE_OK;
}

/* Column text with DEFAULT, CHECK, and REFERENCES removed. NOT NULL stays. */
static int dlStripColumn(const char *zDef, char **pzOut){
  sqlite3_str *pStr;
  const char *z = zDef ? zDef : "";
  const char *zEnd = z + strlen(z);
  int rc = SQLITE_OK, nOut = 0;
  *pzOut = 0;
  pStr = sqlite3_str_new(0);
  if( !pStr ) return SQLITE_NOMEM;
  while( rc==SQLITE_OK ){
    int type, n;
    const char *tok;
    n = dlNext(&z, zEnd, &type);
    if( n==0 ) break;
    if( n<0 ){ rc = SQLITE_CORRUPT; break; }
    tok = z;
    if( type==TK_CONSTRAINT ){
      const char *q = z + n;
      int nt, nn, kt, kn;
      nn = dlNext(&q, zEnd, &nt);
      if( nn<=0 ){ rc = nn<0 ? SQLITE_CORRUPT : SQLITE_OK; break; }
      q += nn;
      kn = dlNext(&q, zEnd, &kt);
      if( kn<=0 ){ rc = kn<0 ? SQLITE_CORRUPT : SQLITE_OK; break; }
      if( kt==TK_CHECK || kt==TK_DEFAULT || kt==TK_REFERENCES || kt==TK_FOREIGN ){
        z = q;
        continue;
      }
    }
    if( type==TK_CHECK ){
      z += n;
      rc = dlSkipParen(&z, zEnd);
      continue;
    }
    if( type==TK_DEFAULT ){
      z += n;
      rc = dlSkipDefault(&z, zEnd);
      continue;
    }
    if( type==TK_REFERENCES || type==TK_FOREIGN ){
      z += n;
      rc = dlSkipReferences(&z, zEnd);
      continue;
    }
    if( nOut ) sqlite3_str_appendchar(pStr, 1, ' ');
    sqlite3_str_append(pStr, tok, n);
    nOut++;
    z += n;
  }
  if( rc==SQLITE_OK && sqlite3_str_errcode(pStr) ) rc = SQLITE_NOMEM;
  if( rc!=SQLITE_OK ){
    sqlite3_str_finish(pStr);
    return rc==SQLITE_NOMEM ? rc : SQLITE_CORRUPT;
  }
  *pzOut = sqlite3_str_finish(pStr);
  if( !*pzOut ) *pzOut = sqlite3_mprintf("");
  return *pzOut ? SQLITE_OK : SQLITE_NOMEM;
}

int dlCoresMatch(const char *zA, const char *zB){
  char *a = 0, *b = 0;
  int rc, same = 0;
  rc = dlStripColumn(zA ? zA : "", &a);
  if( rc==SQLITE_OK ) rc = dlStripColumn(zB ? zB : "", &b);
  if( rc==SQLITE_OK && a && b ) same = schemaDefinitionsEquivalent(a, b);
  sqlite3_free(a);
  sqlite3_free(b);
  return rc==SQLITE_OK ? same : -rc;
}

typedef struct DlPiece DlPiece;
struct DlPiece {
  const char *z;
  int n;
  int kind; /* 0 column, 1 drop (check/fk), 2 other table constraint */
};

static int dlSegmentKind(const char *z, int n){
  const char *p = z, *e = z + n;
  int type, k, t2, n2;
  k = dlNext(&p, e, &type);
  if( k<=0 ) return 0;
  if( type==TK_CHECK || type==TK_FOREIGN ) return 1;
  if( type==TK_PRIMARY || type==TK_UNIQUE ) return 2;
  if( type==TK_CONSTRAINT ){
    p += k;
    k = dlNext(&p, e, &type);
    if( k<=0 ) return 2;
    p += k;
    n2 = dlNext(&p, e, &t2);
    if( n2<=0 ) return 2;
    if( t2==TK_CHECK || t2==TK_FOREIGN ) return 1;
    return 2;
  }
  return 0;
}

static int dlPushPiece(DlPiece **pp, int *pn, int *pAlloc,
                          const char *z, int n){
  while( n>0 && isspace((unsigned char)*z) ){ z++; n--; }
  while( n>0 && isspace((unsigned char)z[n-1]) ) n--;
  if( n<=0 ) return SQLITE_OK;
  if( DOLTLITE_GROW_ARRAY(pp, pAlloc, *pn+1, 8)!=SQLITE_OK ) return SQLITE_NOMEM;
  (*pp)[*pn].z = z;
  (*pp)[*pn].n = n;
  (*pp)[*pn].kind = dlSegmentKind(z, n);
  (*pn)++;
  return SQLITE_OK;
}

static int dlParsePieces(
  const char *zSql, const char **pzHead, int *pnHead,
  DlPiece **pp, int *pn, const char **pzTail
){
  const char *q, *end, *seg;
  int depth = 0, nAlloc = 0, rc = SQLITE_OK;
  *pzHead = zSql;
  *pnHead = 0;
  *pp = 0;
  *pn = 0;
  *pzTail = 0;
  if( !zSql ) return SQLITE_CORRUPT;
  q = zSql;
  end = zSql + strlen(zSql);
  while( q<end ){
    int type, n = sqlite3GetToken((const u8*)q, &type);
    if( n<=0 || type==TK_ILLEGAL ) return SQLITE_CORRUPT;
    if( type==TK_LP ){
      *pnHead = (int)((q + n) - zSql);
      q += n;
      depth = 1;
      break;
    }
    q += n;
  }
  if( depth==0 ) return SQLITE_CORRUPT;
  seg = q;
  while( q<end && depth ){
    int type, n = sqlite3GetToken((const u8*)q, &type);
    if( n<=0 || type==TK_ILLEGAL ){ rc = SQLITE_CORRUPT; break; }
    if( type==TK_LP ){
      depth++;
    }else if( type==TK_RP ){
      depth--;
      if( depth==0 ){
        rc = dlPushPiece(pp, pn, &nAlloc, seg, (int)(q - seg));
        *pzTail = q;
        break;
      }
    }else if( type==TK_COMMA && depth==1 ){
      rc = dlPushPiece(pp, pn, &nAlloc, seg, (int)(q - seg));
      if( rc!=SQLITE_OK ) break;
      seg = q + n;
    }
    q += n;
  }
  if( rc==SQLITE_OK && depth!=0 ) rc = SQLITE_CORRUPT;
  if( rc!=SQLITE_OK ){
    sqlite3_free(*pp);
    *pp = 0;
    *pn = 0;
  }
  return rc;
}

static int dlNeutralSql(const char *zSql, char **pzOut){
  DlPiece *a = 0;
  const char *zHead = 0, *zTail = 0;
  sqlite3_str *pStr;
  int nHead = 0, n = 0, i, rc, nKept = 0;
  *pzOut = 0;
  rc = dlParsePieces(zSql, &zHead, &nHead, &a, &n, &zTail);
  if( rc!=SQLITE_OK ) return rc;
  pStr = sqlite3_str_new(0);
  if( !pStr ){ sqlite3_free(a); return SQLITE_NOMEM; }
  sqlite3_str_append(pStr, zHead, nHead);
  for(i=0; i<n && rc==SQLITE_OK; i++){
    char *zPiece = 0, *zCol = 0;
    if( a[i].kind==1 ) continue;
    if( nKept ) sqlite3_str_appendchar(pStr, 1, ',');
    nKept++;
    if( a[i].kind==2 ){
      sqlite3_str_append(pStr, a[i].z, a[i].n);
      continue;
    }
    zPiece = sqlite3_mprintf("%.*s", a[i].n, a[i].z);
    if( !zPiece ){ rc = SQLITE_NOMEM; break; }
    rc = dlStripColumn(zPiece, &zCol);
    sqlite3_free(zPiece);
    if( rc!=SQLITE_OK ) break;
    sqlite3_str_appendall(pStr, zCol ? zCol : "");
    sqlite3_free(zCol);
  }
  if( rc==SQLITE_OK && zTail ) sqlite3_str_appendall(pStr, zTail);
  sqlite3_free(a);
  if( rc==SQLITE_OK && sqlite3_str_errcode(pStr) ) rc = SQLITE_NOMEM;
  if( rc!=SQLITE_OK ){
    sqlite3_str_finish(pStr);
    return rc==SQLITE_NOMEM ? rc : SQLITE_CORRUPT;
  }
  *pzOut = sqlite3_str_finish(pStr);
  if( !*pzOut ) *pzOut = sqlite3_mprintf("");
  return *pzOut ? SQLITE_OK : SQLITE_NOMEM;
}

int dlNeutralSame(const char *zA, const char *zB, int *pb){
  char *a = 0, *b = 0;
  int rc;
  *pb = 0;
  rc = dlNeutralSql(zA, &a);
  if( rc==SQLITE_OK ) rc = dlNeutralSql(zB, &b);
  if( rc==SQLITE_OK && a && b ) *pb = schemaDefinitionsEquivalent(a, b);
  sqlite3_free(a);
  sqlite3_free(b);
  return rc;
}

static char *dlTokName(const char *z, int n){
  char *s = sqlite3_mprintf("%.*s", n, z);
  int i;
  if( !s ) return 0;
  sqlite3Dequote(s);
  for(i=0; s[i]; i++) s[i] = (char)tolower((unsigned char)s[i]);
  return s;
}

static char *dlPieceName(const char *z, int n){
  const char *p = z, *e = z + n;
  int type, k = dlNext(&p, e, &type);
  if( k<=0 ) return 0;
  return dlTokName(p, k);
}

static int dlHasName(char **az, int n, const char *z){
  int i;
  if( !z ) return 0;
  for(i=0; i<n; i++){
    if( az[i] && sqlite3_stricmp(az[i], z)==0 ) return 1;
  }
  return 0;
}

static int dlAddName(char ***paz, int *pn, int *pAlloc, const char *z){
  char *c;
  int rc;
  if( !z || !z[0] || dlHasName(*paz, *pn, z) ) return SQLITE_OK;
  rc = DOLTLITE_GROW_ARRAY(paz, pAlloc, *pn+1, 4);
  if( rc!=SQLITE_OK ) return rc;
  c = sqlite3_mprintf("%s", z);
  if( !c ) return SQLITE_NOMEM;
  (*paz)[(*pn)++] = c;
  return SQLITE_OK;
}

static void dlRemoveName(char **az, int *pn, const char *z){
  int i;
  for(i=0; i<*pn; i++){
    if( az[i] && sqlite3_stricmp(az[i], z)==0 ){
      sqlite3_free(az[i]);
      memmove(az+i, az+i+1, (size_t)(*pn-i-1)*sizeof(char*));
      (*pn)--;
      return;
    }
  }
}

void dlFreeNames(char **az, int n){
  int i;
  for(i=0; i<n; i++) sqlite3_free(az[i]);
  sqlite3_free(az);
}

int dlMergedNames(
  ParsedColumn *aAnc, int nAnc,
  ParsedColumn *aWin, int nWin,
  ParsedColumn *aOth, int nOth,
  char ***paz, int *pn
){
  int i, nAlloc = 0, rc = SQLITE_OK;
  *paz = 0;
  *pn = 0;
  for(i=0; i<nWin && rc==SQLITE_OK; i++){
    rc = dlAddName(paz, pn, &nAlloc, aWin[i].zName);
  }
  for(i=0; i<nOth && rc==SQLITE_OK; i++){
    if( parsedColumnIndexByName(aAnc, nAnc, aOth[i].zName)>=0 ) continue;
    if( parsedColumnIndexByName(aWin, nWin, aOth[i].zName)>=0 ) continue;
    rc = dlAddName(paz, pn, &nAlloc, aOth[i].zName);
  }
  for(i=0; i<nAnc && rc==SQLITE_OK; i++){
    if( parsedColumnIndexByName(aWin, nWin, aAnc[i].zName)<0 ) continue;
    if( parsedColumnIndexByName(aOth, nOth, aAnc[i].zName)>=0 ) continue;
    if( columnRenamedAt(aOth, nOth, aAnc, nAnc, i, aWin, nWin) ){
      dlRemoveName(*paz, pn, aAnc[i].zName);
      if( i<nOth ) rc = dlAddName(paz, pn, &nAlloc, aOth[i].zName);
    }else{
      dlRemoveName(*paz, pn, aAnc[i].zName);
    }
  }
  return rc;
}

int dlUnionCols(
  ParsedColumn *a, int na, ParsedColumn *b, int nb, ParsedColumn *c, int nc,
  ParsedColumn **pp, int *pn
){
  int n = na + nb + nc, i, k = 0;
  ParsedColumn *u;
  *pp = 0;
  *pn = 0;
  if( n<=0 ) return SQLITE_OK;
  u = sqlite3_malloc(sizeof(ParsedColumn) * n);
  if( !u ) return SQLITE_NOMEM;
  memset(u, 0, sizeof(ParsedColumn) * n);
  for(i=0; i<na; i++) u[k++].zName = a[i].zName;
  for(i=0; i<nb; i++) u[k++].zName = b[i].zName;
  for(i=0; i<nc; i++) u[k++].zName = c[i].zName;
  *pp = u;
  *pn = k;
  return SQLITE_OK;
}

/* 0 every noted column is already in the winning CREATE.
** 1 every noted column survives, but one arrives via ADD COLUMN.
** 2 a noted column is not in the merged set. */
int dlRefClass(
  const char *zCols, char **azMerged, int nMerged,
  ParsedColumn *aWin, int nWin
){
  const char *p;
  int pending = 0;
  if( !zCols || !zCols[0] ) return 0;
  for(p=zCols; *p; ){
    const char *e = strchr(p, '\n');
    int n = e ? (int)(e-p) : (int)strlen(p);
    char *z = sqlite3_mprintf("%.*s", n, p);
    int inWin;
    if( !z ) return -SQLITE_NOMEM;
    if( !dlHasName(azMerged, nMerged, z) ){
      sqlite3_free(z);
      return 2;
    }
    inWin = parsedColumnIndexByName(aWin, nWin, z)>=0;
    sqlite3_free(z);
    if( !inWin ) pending = 1;
    if( !e ) break;
    p = e + 1;
  }
  return pending ? 1 : 0;
}
int dlCutRaw(char **pzSql, const char *zRaw){
  char *zSql, *hit, *start, *end, *zNew;
  if( !pzSql || !*pzSql || !zRaw || !zRaw[0] ) return SQLITE_OK;
  zSql = *pzSql;
  hit = strstr(zSql, zRaw);
  if( !hit ) return SQLITE_OK;
  start = hit;
  end = hit + strlen(zRaw);
  while( start>zSql && isspace((unsigned char)start[-1]) ) start--;
  if( start>zSql && start[-1]==',' ){
    start--;
  }else{
    while( *end && isspace((unsigned char)*end) ) end++;
    if( *end==',' ) end++;
  }
  zNew = sqlite3_mprintf("%.*s%s", (int)(start-zSql), zSql, end);
  if( !zNew ) return SQLITE_NOMEM;
  sqlite3_free(zSql);
  *pzSql = zNew;
  return SQLITE_OK;
}

int dlRewriteColumns(
  const char *zSql, char **azName, char **azDef, int nRep,
  char **pzOut, int *pChanged
){
  DlPiece *a = 0;
  const char *zHead = 0, *zTail = 0;
  sqlite3_str *pStr;
  int nHead = 0, n = 0, i, rc, nKept = 0;
  *pzOut = 0;
  *pChanged = 0;
  rc = dlParsePieces(zSql, &zHead, &nHead, &a, &n, &zTail);
  if( rc!=SQLITE_OK ) return rc;
  pStr = sqlite3_str_new(0);
  if( !pStr ){ sqlite3_free(a); return SQLITE_NOMEM; }
  sqlite3_str_append(pStr, zHead, nHead);
  for(i=0; i<n && rc==SQLITE_OK; i++){
    char *zName = 0;
    const char *zEmit = a[i].z;
    int nEmit = a[i].n;
    int r;
    if( a[i].kind==0 && nRep>0 ){
      zName = dlPieceName(a[i].z, a[i].n);
      if( !zName ){ rc = SQLITE_NOMEM; break; }
      for(r=0; r<nRep; r++){
        if( azName[r] && sqlite3_stricmp(azName[r], zName)==0 ){
          zEmit = azDef[r];
          nEmit = (int)strlen(azDef[r]);
          *pChanged = 1;
          break;
        }
      }
    }
    sqlite3_free(zName);
    if( nKept ) sqlite3_str_appendchar(pStr, 1, ',');
    nKept++;
    sqlite3_str_append(pStr, zEmit, nEmit);
  }
  if( rc==SQLITE_OK && zTail ) sqlite3_str_appendall(pStr, zTail);
  sqlite3_free(a);
  if( rc==SQLITE_OK && sqlite3_str_errcode(pStr) ) rc = SQLITE_NOMEM;
  if( rc!=SQLITE_OK ){
    sqlite3_str_finish(pStr);
    return rc==SQLITE_NOMEM ? rc : SQLITE_CORRUPT;
  }
  *pzOut = sqlite3_str_finish(pStr);
  if( !*pzOut ) return SQLITE_NOMEM;
  return SQLITE_OK;
}

int dlDeferRaw(SchemaMergeAction *a, int n, const char *zTable,
                      const char *zRaw){
  int i, hit = -1, nHave;
  char **az;
  char *zCopy;
  if( !a || !zTable || !zRaw || !zRaw[0] ) return SQLITE_OK;
  for(i=0; i<n; i++){
    if( !a[i].zTableName || sqlite3_stricmp(a[i].zTableName, zTable)!=0 ){
      continue;
    }
    if( !a[i].zRenameTable ){ hit = i; break; }
    if( hit<0 ) hit = i;
  }
  if( hit<0 ) return SQLITE_OK;
  for(i=0; i<a[hit].nClauses; i++){
    if( a[hit].azClauses[i] && strcmp(a[hit].azClauses[i], zRaw)==0 ){
      return SQLITE_OK;
    }
  }
  nHave = a[hit].nClauses;
  az = sqlite3_realloc(a[hit].azClauses, (nHave+1)*(int)sizeof(char*));
  if( !az ) return SQLITE_NOMEM;
  a[hit].azClauses = az;
  zCopy = sqlite3_mprintf("%s", zRaw);
  if( !zCopy ) return SQLITE_NOMEM;
  az[nHave] = zCopy;
  a[hit].nClauses = nHave + 1;
  return SQLITE_OK;
}

void dlFksFree(DlFk *a, int n){
  int i;
  if( !a ) return;
  for(i=0; i<n; i++){
    sqlite3_free(a[i].zName);
    sqlite3_free(a[i].zRaw);
    sqlite3_free(a[i].zCols);
  }
  sqlite3_free(a);
}

static int dlFkAddCol(DlFk *p, const char *z, int n){
  char *zName = dlTokName(z, n);
  char *zNew;
  if( !zName ) return SQLITE_NOMEM;
  if( !zName[0] || dlColsContain(p->zCols, zName) ){
    sqlite3_free(zName);
    return SQLITE_OK;
  }
  zNew = sqlite3_mprintf("%s%s%s", p->zCols ? p->zCols : "",
                         p->zCols ? "\n" : "", zName);
  sqlite3_free(zName);
  if( !zNew ) return SQLITE_NOMEM;
  sqlite3_free(p->zCols);
  p->zCols = zNew;
  return SQLITE_OK;
}

static int dlFillFk(const char *z, int n, DlFk *p){
  const char *q = z, *e = z + n, *zName = 0;
  int type, k, nName = 0, depth, rc;
  memset(p, 0, sizeof(*p));
  p->zRaw = sqlite3_mprintf("%.*s", n, z);
  if( !p->zRaw ) return SQLITE_NOMEM;
  k = dlNext(&q, e, &type);
  if( k>0 && type==TK_CONSTRAINT ){
    q += k;
    k = dlNext(&q, e, &type);
    if( k<=0 ) return SQLITE_CORRUPT;
    zName = q;
    nName = k;
    q += k;
  }
  if( zName ){
    p->zName = dlTokName(zName, nName);
    if( !p->zName ) return SQLITE_NOMEM;
    if( !p->zName[0] ){ sqlite3_free(p->zName); p->zName = 0; }
  }
  while( (k = dlNext(&q, e, &type))>0 ){
    if( type==TK_FOREIGN ) break;
    q += k;
  }
  if( k<=0 ) return SQLITE_CORRUPT;
  q += k;
  k = dlNext(&q, e, &type);
  if( k<=0 ) return SQLITE_CORRUPT;
  q += k;
  k = dlNext(&q, e, &type);
  if( k<=0 || type!=TK_LP ) return SQLITE_CORRUPT;
  q += k;
  depth = 1;
  while( depth>0 ){
    const char *tok;
    k = dlNext(&q, e, &type);
    if( k<=0 ) return SQLITE_CORRUPT;
    tok = q;
    q += k;
    if( type==TK_LP ) depth++;
    else if( type==TK_RP ) depth--;
    else if( depth==1 && type!=TK_COMMA ){
      rc = dlFkAddCol(p, tok, k);
      if( rc!=SQLITE_OK ) return rc;
    }
  }
  return SQLITE_OK;
}

int dlCollectFks(const char *zSql, DlFk **pp, int *pn){
  DlPiece *a = 0;
  const char *zHead = 0, *zTail = 0;
  int nHead = 0, n = 0, i, nAlloc = 0, rc;
  *pp = 0;
  *pn = 0;
  rc = dlParsePieces(zSql, &zHead, &nHead, &a, &n, &zTail);
  if( rc!=SQLITE_OK ) return rc;
  for(i=0; i<n && rc==SQLITE_OK; i++){
    DlFk fk;
    if( a[i].kind!=1 || dlSegmentKind(a[i].z, a[i].n)!=1 ) continue;
    if( dlSegmentKind(a[i].z, a[i].n)==1 ){
      const char *p = a[i].z, *e = a[i].z + a[i].n;
      int type, k, t2, n2, isFk = 0;
      k = dlNext(&p, e, &type);
      if( k>0 && type==TK_FOREIGN ) isFk = 1;
      if( k>0 && type==TK_CONSTRAINT ){
        p += k;
        k = dlNext(&p, e, &type);
        if( k>0 ){
          p += k;
          n2 = dlNext(&p, e, &t2);
          if( n2>0 && t2==TK_FOREIGN ) isFk = 1;
        }
      }
      if( !isFk ) continue;
    }
    rc = dlFillFk(a[i].z, a[i].n, &fk);
    if( rc!=SQLITE_OK ){
      sqlite3_free(fk.zName);
      sqlite3_free(fk.zRaw);
      sqlite3_free(fk.zCols);
      if( rc==SQLITE_CORRUPT ) rc = SQLITE_OK;
      break;
    }
    rc = DOLTLITE_GROW_ARRAY(pp, &nAlloc, *pn+1, 4);
    if( rc!=SQLITE_OK ){
      sqlite3_free(fk.zName);
      sqlite3_free(fk.zRaw);
      sqlite3_free(fk.zCols);
      break;
    }
    (*pp)[(*pn)++] = fk;
  }
  sqlite3_free(a);
  if( rc!=SQLITE_OK ){
    dlFksFree(*pp, *pn);
    *pp = 0;
    *pn = 0;
  }
  return rc;
}

int dlFkSame(const DlFk *a, const DlFk *b){
  if( !a->zRaw || !b->zRaw ) return 0;
  return schemaDefinitionsEquivalent(a->zRaw, b->zRaw);
}

/* Same constraint: a shared name, or the same child columns. */
int dlFkCorresponds(const DlFk *a, const DlFk *b){
  if( !a || !b ) return 0;
  if( a->zName && b->zName && sqlite3_stricmp(a->zName, b->zName)==0 ){
    return 1;
  }
  if( a->zCols && b->zCols && strcmp(a->zCols, b->zCols)==0 ) return 1;
  return 0;
}

int dlPushRaw(char ***paz, int *pn, int *pAlloc, const char *z){
  char *c;
  int rc, i;
  if( !z ) return SQLITE_OK;
  for(i=0; i<*pn; i++){
    if( (*paz)[i] && strcmp((*paz)[i], z)==0 ) return SQLITE_OK;
  }
  rc = DOLTLITE_GROW_ARRAY(paz, pAlloc, *pn+1, 4);
  if( rc!=SQLITE_OK ) return rc;
  c = sqlite3_mprintf("%s", z);
  if( !c ) return SQLITE_NOMEM;
  (*paz)[(*pn)++] = c;
  return SQLITE_OK;
}

void dlFreeRaws(char **az, int n){
  int i;
  for(i=0; i<n; i++) sqlite3_free(az[i]);
  sqlite3_free(az);
}
#endif
