
#ifdef DOLTLITE_PROLLY

#include "doltlite_vtab_util.h"
#include "doltlite_commit.h"
#include "doltlite_ancestor.h"
#include "doltlite_internal.h"

#include <string.h>
#include <time.h>

#define AT_IDX_REF    0x01
#define AT_IDX_PK_EQ  0x02
#define AT_IDX_PK_GE  0x04
#define AT_IDX_PK_LE  0x08
#define AT_IDX_PK_GT  0x10
#define AT_IDX_PK_LT  0x20
#define AT_IDX_PK_ANY \
  (AT_IDX_PK_EQ|AT_IDX_PK_GE|AT_IDX_PK_LE|AT_IDX_PK_GT|AT_IDX_PK_LT)

static char *atBuildSchema(const DoltliteColInfo *ci){
  static const char *const azReserved[] = {"commit_ref"};
  sqlite3_str *pStr = sqlite3_str_new(0);
  char *z;
  if( !pStr ) return 0;
  sqlite3_str_appendall(pStr, "CREATE TABLE x(");
  if( doltliteAppendDisambiguatedColumnList(
          pStr,ci->azName,ci->nCol,"",", ",azReserved,
          ArraySize(azReserved),ci->iPkCol,ci->azDecl)!=SQLITE_OK ){
    sqlite3_str_reset(pStr);
    return 0;
  }
  sqlite3_str_appendall(pStr, ", commit_ref TEXT HIDDEN)");
  z = sqlite3_str_finish(pStr);
  return z;
}

typedef struct AtVtab AtVtab;
struct AtVtab {
  sqlite3_vtab base;
  sqlite3 *db;
  char *zTableName;
  DoltliteColInfo cols;
};

typedef struct AtCursor AtCursor;
struct AtCursor {
  DoltliteVtabCursorCommon common;
  /* Invalid renders with the declared layout. */
  DoltliteSideCols side;
  char *zCommitRef;
  int idxNum;
  int pkSeekable;
  DoltlitePkRange pkRange;
  u8 *pPkBlob;
  int nPkBlob;
};

static int atTakeChunkSourceError(
  ChunkStore *cs,
  char **pzErr,
  int *pRc
){
  int sourceRc = SQLITE_OK;
  char *zErr = chunkStoreSourceTakeError(cs, &sourceRc);
  if( !zErr && sourceRc==SQLITE_OK ) return 0;
  if( pzErr ) *pzErr = zErr;
  else sqlite3_free(zErr);
  if( pRc && sourceRc!=SQLITE_OK ) *pRc = sourceRc;
  return 1;
}

static int atEnqueueReachableRoots(
  sqlite3 *db,
  DoltliteCommitQueue *pQueue
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  const BranchRef *aBr = 0;
  const TagRef *aTag = 0;
  const TrackingBranch *aTracking = 0;
  int nBr = 0, nTag = 0, nTracking = 0;
  int i, rc = SQLITE_OK;
  ProllyHash head;

  if( !cs ) return SQLITE_OK;
  memset(&head, 0, sizeof(head));
  doltliteGetSessionHead(db, &head);
  rc = doltliteCommitQueueEnqueue(pQueue, &head);
  refsTableGetBranches(&cs->refs, &nBr, &aBr);
  for(i=0; i<nBr && rc==SQLITE_OK; i++){
    rc = doltliteCommitQueueEnqueue(pQueue, &aBr[i].commitHash);
  }
  refsTableGetTags(&cs->refs, &nTag, &aTag);
  for(i=0; i<nTag && rc==SQLITE_OK; i++){
    rc = doltliteCommitQueueEnqueue(pQueue, &aTag[i].commitHash);
  }
  refsTableGetTracking(&cs->refs, &nTracking, &aTracking);
  for(i=0; i<nTracking && rc==SQLITE_OK; i++){
    rc = doltliteCommitQueueEnqueue(pQueue, &aTracking[i].commitHash);
  }
  return rc;
}

/* True if the live table schema hashes to pSchemaHash (safe to render as declared). */
static int sideColsDeclaredSchemaMatches(
  sqlite3 *db,
  const char *zTable,
  const ProllyHash *pSchemaHash,
  int *pbMatch
){
  sqlite3_stmt *pStmt = 0;
  char *zQ;
  int rc;

  *pbMatch = 0;
  zQ = sqlite3_mprintf(
      "SELECT sql FROM main.sqlite_master WHERE type='table' AND name=%Q",
      zTable);
  if( !zQ ) return SQLITE_NOMEM;
  rc = sqlite3_prepare_v2(db, zQ, -1, &pStmt, 0);
  sqlite3_free(zQ);
  if( rc!=SQLITE_OK ) return rc;
  if( sqlite3_step(pStmt)==SQLITE_ROW ){
    const char *zSql = (const char*)sqlite3_column_text(pStmt, 0);
    if( zSql && zSql[0] ){
      char *zCanon = doltliteCanonicalizeSchemaSql(zSql, zTable);
      if( zCanon ){
        ProllyHash h;
        prollyHashCompute((const u8*)zCanon, (int)strlen(zCanon), &h);
        *pbMatch = prollyHashCompare(&h, pSchemaHash)==0;
        sqlite3_free(zCanon);
      }
    }
  }
  sqlite3_finalize(pStmt);
  return SQLITE_OK;
}

static int atOpenSchemaDb(sqlite3 *db, sqlite3 **ppTmp){
  sqlite3 *tmp = 0;
  HashElem *pElem;
  int i, rc;
  *ppTmp = 0;
  rc = sqlite3_open(":memory:", &tmp);
  sqlite3_mutex_enter(db->mutex);
  for(pElem=sqliteHashFirst(&db->aCollSeq); rc==SQLITE_OK && pElem;
      pElem=sqliteHashNext(pElem)){
    CollSeq *aColl = sqliteHashData(pElem);
    for(i=0; i<3 && rc==SQLITE_OK; i++){
      CollSeq *pColl = &aColl[i];
      int enc = pColl->enc & SQLITE_UTF16_ALIGNED
          ? SQLITE_UTF16_ALIGNED : pColl->enc;
      if( !pColl->xCmp ) continue;
      /* The caller owns collation contexts; this connection only borrows them. */
      rc = sqlite3_create_collation(tmp, pColl->zName, enc,
                                    pColl->pUser, pColl->xCmp);
    }
  }
  sqlite3_mutex_leave(db->mutex);
  if( rc!=SQLITE_OK ){
    sqlite3_close(tmp);
    return rc;
  }
  *ppTmp = tmp;
  return SQLITE_OK;
}

/* Load columns as pCatHash declares them. Invalid-side fallback to declared
** layout is allowed only when the table is absent or the live schema is
** identical; otherwise fail rather than decode with the wrong layout. */
int doltliteSideColsLoad(
  sqlite3 *db,
  const ProllyHash *pCatHash,
  const ProllyHash *pSchemaHash,
  const char *zTable,
  const DoltliteColInfo *pDeclared,
  int bSideHasData,
  DoltliteSideCols *pSide
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  SchemaEntry entry;
  int found = 0;
  sqlite3 *tmp = 0;
  int i, j;
  int rc;

  if( pSide->valid
   && prollyHashCompare(&pSide->schemaHash, pSchemaHash)==0 ){
    return SQLITE_OK;
  }
  doltliteSideColsClear(pSide);
  if( !cs || !pCache || prollyHashIsEmpty(pCatHash) ) return SQLITE_OK;

  rc = loadSchemaEntryFromCatalog(db, cs, pCache, pCatHash, zTable,
                                  &entry, &found);
  if( rc!=SQLITE_OK ) return rc;
  if( !found || !entry.zSql ){
    clearSchemaEntry(&entry);
    return bSideHasData ? SQLITE_CORRUPT : SQLITE_OK;
  }

  rc = atOpenSchemaDb(db, &tmp);
  if( rc==SQLITE_OK ) rc = sqlite3_exec(tmp, entry.zSql, 0, 0, 0);
  if( rc==SQLITE_OK ) rc = doltliteGetColumnNames(tmp, zTable, &pSide->ci);
  if( tmp ) sqlite3_close(tmp);
  clearSchemaEntry(&entry);
  if( rc!=SQLITE_OK || pSide->ci.nCol<=0 ){
    int match = 0;
    int rc2;
    doltliteSideColsClear(pSide);
    rc2 = sideColsDeclaredSchemaMatches(db, zTable, pSchemaHash, &match);
    if( rc2==SQLITE_OK && match ) return SQLITE_OK;
    if( rc2!=SQLITE_OK ) return rc2;
    return rc==SQLITE_OK ? SQLITE_ERROR : rc;
  }

  if( pDeclared->nCol>0 ){
    pSide->aDeclToSide = sqlite3_malloc(pDeclared->nCol * (int)sizeof(int));
    if( !pSide->aDeclToSide ){
      doltliteSideColsClear(pSide);
      return SQLITE_NOMEM;
    }
    for(i=0; i<pDeclared->nCol; i++){
      pSide->aDeclToSide[i] = -1;
      for(j=0; j<pSide->ci.nCol; j++){
        if( sqlite3_stricmp(pDeclared->azName[i], pSide->ci.azName[j])==0 ){
          pSide->aDeclToSide[i] = j;
          break;
        }
      }
    }
  }
  memcpy(&pSide->schemaHash, pSchemaHash, sizeof(ProllyHash));
  pSide->valid = 1;
  return SQLITE_OK;
}

static const char *atHistoricalLiteralArg(sqlite3 *db, int iArg){
  ExprList *pArgs = db->pDoltliteHistoricalArgs;
  Expr *pExpr;
  if( !pArgs || iArg<0 || iArg>=pArgs->nExpr ) return 0;
  pExpr = pArgs->a[iArg].pExpr;
  return pExpr && pExpr->op==TK_STRING ? pExpr->u.zToken : 0;
}

static int atResolveSchemaRef(
  sqlite3 *db,
  const char *zRef,
  int branchEffective,
  int allowWorkspace,
  ProllyHash *pCommit,
  ProllyHash *pCatalog
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  DoltliteCommit commit;
  int rc;

  memset(&commit, 0, sizeof(commit));
  memset(pCommit, 0, sizeof(*pCommit));
  memset(pCatalog, 0, sizeof(*pCatalog));
  if( allowWorkspace
   && (doltliteRefIsWorking(zRef) || doltliteRefIsStaged(zRef)) ){
    rc = doltliteResolveCatalogHashForRef(db, zRef, pCatalog);
    if( rc==SQLITE_OK ) doltliteGetSessionHead(db, pCommit);
    return rc;
  }

  rc = doltliteResolveRef(db, zRef, pCommit);
  if( rc==SQLITE_OK ) rc = doltliteLoadCommit(db, pCommit, &commit);
  if( rc==SQLITE_OK ) *pCatalog = commit.catalogHash;
  doltliteCommitClear(&commit);
  if( rc==SQLITE_OK && branchEffective && cs ){
    ProllyHash branchCommit;
    if( chunkStoreFindBranch(cs, zRef, &branchCommit)==SQLITE_OK
     && !prollyHashIsEmpty(&branchCommit) ){
      ProllyHash effective;
      rc = doltliteResolveBranchEffectiveCatalog(
          cs, zRef, &branchCommit, pCatalog, &effective);
      if( rc==SQLITE_OK ) *pCatalog = effective;
    }
  }
  return rc;
}

static int atResolveLiteralScope(
  sqlite3 *db,
  const char *zModule,
  ProllyHash *aCommit,
  ProllyHash *aCatalog,
  int *pnRef,
  int *pScoped,
  char **pzErr
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ExprList *pArgs = db->pDoltliteHistoricalArgs;
  const char *azRef[2] = {0, 0};
  char *zLeft = 0;
  char *zRight = 0;
  int branchEffective = 0;
  int allowWorkspace = 0;
  int nRef = 0;
  int rangeType = DOLTLITE_RANGE_NONE;
  int primary;
  int i;
  int rc = SQLITE_OK;

  *pnRef = 0;
  *pScoped = 0;
  if( !pArgs || !zModule ) return SQLITE_OK;
  if( sqlite3_strnicmp(zModule, "dolt_at_", 8)==0 ){
    if( pArgs->nExpr!=1 ) return SQLITE_OK;
    azRef[0] = atHistoricalLiteralArg(db, 0);
    branchEffective = 1;
    allowWorkspace = 1;
    nRef = azRef[0] ? 1 : 0;
  }else if( sqlite3_strnicmp(zModule, "dolt_history_", 13)==0 ){
    if( pArgs->nExpr!=1 ) return SQLITE_OK;
    azRef[0] = atHistoricalLiteralArg(db, 0);
    nRef = azRef[0] ? 1 : 0;
  }else if( sqlite3_strnicmp(zModule, "dolt_diff_", 10)==0 ){
    allowWorkspace = 1;
    if( pArgs->nExpr==2 ){
      azRef[0] = atHistoricalLiteralArg(db, 1);
      azRef[1] = atHistoricalLiteralArg(db, 0);
      nRef = azRef[0] && azRef[1] ? 2 : 0;
    }else if( pArgs->nExpr==1 ){
      const char *zSpec = atHistoricalLiteralArg(db, 0);
      if( zSpec ){
        rc = doltliteSplitRevisionRange(
            zSpec, &zLeft, &zRight, &rangeType);
        if( rc==SQLITE_OK ){
          azRef[0] = zRight;
          azRef[1] = zLeft;
          nRef = 2;
        }else if( rc==SQLITE_NOMEM ){
          goto done;
        }else{
          rc = SQLITE_OK;
        }
      }
    }
  }
  if( nRef==0 ) goto done;

  for(i=0; i<nRef; i++){
    rc = atResolveSchemaRef(db, azRef[i], branchEffective, allowWorkspace,
                            &aCommit[i], &aCatalog[i]);
    if( rc!=SQLITE_OK ) goto resolve_failed;
  }
  if( rangeType==DOLTLITE_RANGE_THREE_DOT ){
    DoltliteCommit ancestorCommit;
    ProllyHash ancestor;
    memset(&ancestorCommit, 0, sizeof(ancestorCommit));
    rc = doltliteFindAncestor(db, &aCommit[1], &aCommit[0], &ancestor);
    if( rc==SQLITE_OK ){
      rc = doltliteLoadCommit(db, &ancestor, &ancestorCommit);
    }
    if( rc==SQLITE_OK ){
      aCommit[1] = ancestor;
      aCatalog[1] = ancestorCommit.catalogHash;
    }
    doltliteCommitClear(&ancestorCommit);
    if( rc!=SQLITE_OK ) goto resolve_failed;
  }
  *pnRef = nRef;
  *pScoped = 1;
  goto done;

resolve_failed:
  primary = rc & 0xff;
  if( atTakeChunkSourceError(cs, pzErr, &rc) ) goto done;
  if( primary==SQLITE_ERROR || primary==SQLITE_NOTFOUND ) rc = SQLITE_OK;

done:
  sqlite3_free(zLeft);
  sqlite3_free(zRight);
  return rc;
}

static int atLoadColumnDeclarations(
  sqlite3 *db, const char *zTable, DoltliteColInfo *ci
){
  Table *pTab;
  int i, rc = SQLITE_OK;
  if( ci->nCol==0 ) return SQLITE_OK;
  ci->azDecl = sqlite3_malloc64(ci->nCol*sizeof(char*));
  if( !ci->azDecl ) return SQLITE_NOMEM;
  memset(ci->azDecl, 0, ci->nCol*sizeof(char*));
  ci->aAffinity = sqlite3_malloc(ci->nCol);
  if( !ci->aAffinity ) return SQLITE_NOMEM;
  sqlite3_mutex_enter(db->mutex);
  pTab = sqlite3FindTable(db, zTable, "main");
  if( !pTab ) rc = SQLITE_NOTFOUND;
  for(i=0; rc==SQLITE_OK && i<ci->nCol; i++){
    int iCol = sqlite3ColumnIndex(pTab, ci->azName[i]);
    const char *zType = "BLOB";
    const char *zColl;
    if( iCol<0 ){
      rc = SQLITE_CORRUPT;
      break;
    }
    ci->aAffinity[i] = pTab->aCol[iCol].affinity;
    switch( ci->aAffinity[i] ){
      case SQLITE_AFF_TEXT: zType = "TEXT"; break;
      case SQLITE_AFF_NUMERIC: zType = "NUMERIC"; break;
      case SQLITE_AFF_INTEGER: zType = "INTEGER"; break;
      case SQLITE_AFF_REAL: zType = "REAL"; break;
    }
    zColl = sqlite3ColumnColl(&pTab->aCol[iCol]);
    ci->azDecl[i] = sqlite3_mprintf(" %s COLLATE \"%w\"",
                                     zType,zColl ? zColl : "BINARY");
    if( !ci->azDecl[i] ) rc = SQLITE_NOMEM;
  }
  sqlite3_mutex_leave(db->mutex);
  return rc;
}

static int atLoadSchemaColumns(
  sqlite3 *db,
  ChunkStore *cs,
  ProllyCache *pCache,
  const ProllyHash *pCatalog,
  const char *zTableName,
  DoltliteColInfo *pCols
){
  SchemaEntry entry;
  int found = 0;
  sqlite3 *tmp = 0;
  int rc;

  memset(&entry, 0, sizeof(entry));
  if( prollyHashIsEmpty(pCatalog) ) return SQLITE_OK;
  rc = loadSchemaEntryFromCatalog(db, cs, pCache, pCatalog,
                                  zTableName, &entry, &found);
  if( rc==SQLITE_OK && found && entry.zSql ){
    rc = atOpenSchemaDb(db, &tmp);
    if( rc==SQLITE_OK ) rc = sqlite3_exec(tmp, entry.zSql, 0, 0, 0);
    if( rc==SQLITE_OK ) rc = doltliteGetColumnNames(tmp, zTableName, pCols);
    if( rc==SQLITE_OK ) rc = atLoadColumnDeclarations(tmp, zTableName, pCols);
    if( rc==SQLITE_OK && pCols->nCol<=0 ){
      doltliteFreeColInfo(pCols);
      rc = SQLITE_NOTFOUND;
    }
  }
  if( tmp ) sqlite3_close(tmp);
  clearSchemaEntry(&entry);
  return rc;
}

int doltliteLoadHistoricalTableColumns(
  sqlite3 *db,
  const char *zModule,
  const char *zTableName,
  DoltliteColInfo *pCols,
  char **pzErr
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  ProllyHash aCommit[2];
  ProllyHash aCatalog[2];
  DoltliteCommitQueue q;
  ProllyHash cur;
  int nRef = 0;
  int scoped = 0;
  int has;
  int i;
  int rc;

  memset(pCols, 0, sizeof(*pCols));
  pCols->iPkCol = -1;
  if( sqlite3FindTable(db, zTableName, "main") ){
    rc = doltliteGetColumnNames(db, zTableName, pCols);
    if( rc==SQLITE_OK ) rc = atLoadColumnDeclarations(db, zTableName, pCols);
    if( rc!=SQLITE_OK ) return rc;
    if( pCols->nCol>0 ) return SQLITE_OK;
    doltliteFreeColInfo(pCols);
  }

  memset(aCommit, 0, sizeof(aCommit));
  memset(aCatalog, 0, sizeof(aCatalog));
  rc = atResolveLiteralScope(db, zModule, aCommit, aCatalog,
                             &nRef, &scoped, pzErr);
  for(i=0; rc==SQLITE_OK && i<nRef && pCols->nCol<=0; i++){
    rc = atLoadSchemaColumns(
        db, cs, pCache, &aCatalog[i], zTableName, pCols);
    if( rc==SQLITE_NOTFOUND ){
      if( !atTakeChunkSourceError(cs, pzErr, &rc) ) rc = SQLITE_OK;
    }else if( rc!=SQLITE_OK ){
      atTakeChunkSourceError(cs, pzErr, &rc);
    }
  }
  if( rc!=SQLITE_OK || pCols->nCol>0 ) return rc;

  memset(&q, 0, sizeof(q));
  memset(&cur, 0, sizeof(cur));
  rc = doltliteCommitQueueInit(&q, &cur);
  if( scoped ){
    for(i=0; i<nRef && rc==SQLITE_OK; i++){
      rc = doltliteCommitQueueEnqueue(&q, &aCommit[i]);
    }
  }else if( rc==SQLITE_OK ){
    rc = atEnqueueReachableRoots(db, &q);
  }
  while( rc==SQLITE_OK && pCols->nCol<=0 ){
    DoltliteCommit commit;
    memset(&commit, 0, sizeof(commit));
    rc = doltliteCommitQueueNext(&q, &cur, &has);
    if( rc!=SQLITE_OK || !has ) break;
    rc = doltliteLoadCommit(db, &cur, &commit);
    if( rc==SQLITE_OK ) rc = doltliteCommitQueueEnqueueParents(&q, &commit);
    if( rc==SQLITE_OK ){
      rc = atLoadSchemaColumns(
          db, cs, pCache, &commit.catalogHash, zTableName, pCols);
    }
    doltliteCommitClear(&commit);
    if( rc==SQLITE_NOTFOUND ){
      if( !atTakeChunkSourceError(cs, pzErr, &rc) ) rc = SQLITE_OK;
    }else if( rc!=SQLITE_OK ){
      atTakeChunkSourceError(cs, pzErr, &rc);
    }
  }
  doltliteCommitQueueClear(&q);

  if( rc==SQLITE_OK && pCols->nCol<=0 ) return SQLITE_NOTFOUND;
  return rc;
}

static void atCursorReset(AtCursor *c){
  doltliteSideColsClear(&c->side);
  doltliteVtabCommonReset(&c->common);
  sqlite3_free(c->zCommitRef);
  c->zCommitRef = 0;
  sqlite3_free(c->pPkBlob);
  c->pPkBlob = 0;
  c->nPkBlob = 0;
}

static int atConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  (void)pAux;
  return doltliteVtabConnectHistoricalTable(db, argc, argv,
                                            "dolt_at_",
                                            sizeof(AtVtab), atBuildSchema,
                                            ppVtab, pzErr);
}

static int atOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **pp){
  (void)pVtab;
  return doltliteVtabOpenCursor(pp, sizeof(AtCursor));
}

static int atClose(sqlite3_vtab_cursor *cur){
  AtCursor *c=(AtCursor*)cur;
  atCursorReset(c); sqlite3_free(c); return SQLITE_OK;
}

static int atBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  DoltliteVtabCommon *v=(DoltliteVtabCommon*)pVtab;
  int nCols=v->cols.nCol;
  int iRef=-1, iEq=-1, iGe=-1, iLe=-1, iGt=-1, iLt=-1;
  int i, argvIdx=1, idxNum=0;
  int iPkCol = v->cols.iPkCol;

  int refCol = nCols > 0 ? nCols : 2;
  (void)pVtab;

  for(i=0;i<pInfo->nConstraint;i++){
    if(!pInfo->aConstraint[i].usable) continue;
    if( pInfo->aConstraint[i].iColumn==refCol
     && pInfo->aConstraint[i].op==SQLITE_INDEX_CONSTRAINT_EQ ){
      iRef=i;
    }else if( iPkCol>=0 && pInfo->aConstraint[i].iColumn==iPkCol ){
      switch( pInfo->aConstraint[i].op ){
        case SQLITE_INDEX_CONSTRAINT_EQ: if( iEq<0 ) iEq=i; break;
        case SQLITE_INDEX_CONSTRAINT_GE: if( iGe<0 ) iGe=i; break;
        case SQLITE_INDEX_CONSTRAINT_LE: if( iLe<0 ) iLe=i; break;
        case SQLITE_INDEX_CONSTRAINT_GT: if( iGt<0 ) iGt=i; break;
        case SQLITE_INDEX_CONSTRAINT_LT: if( iLt<0 ) iLt=i; break;
        default: break;
      }
    }
  }

  if(iRef>=0){
    pInfo->aConstraintUsage[iRef].argvIndex=argvIdx++;
    pInfo->aConstraintUsage[iRef].omit=1;
    idxNum |= AT_IDX_REF;
    /* Historical roots may differ in key shape: never omit PK constraints. */
    if( iPkCol<0 ){
      pInfo->idxNum = idxNum;
      if( doltliteBestIndexClusteredPkEq(pInfo, &v->cols, AT_IDX_PK_EQ,
                                         &argvIdx)!=SQLITE_OK ){
        return SQLITE_NOMEM;
      }
      idxNum = pInfo->idxNum;
    }else if( iEq>=0 ){
      pInfo->aConstraintUsage[iEq].argvIndex=argvIdx++;
      idxNum |= AT_IDX_PK_EQ;
    }else{
      if( iGe>=0 ){
        pInfo->aConstraintUsage[iGe].argvIndex=argvIdx++;
        idxNum |= AT_IDX_PK_GE;
      }
      if( iGt>=0 ){
        pInfo->aConstraintUsage[iGt].argvIndex=argvIdx++;
        idxNum |= AT_IDX_PK_GT;
      }
      if( iLe>=0 ){
        pInfo->aConstraintUsage[iLe].argvIndex=argvIdx++;
        idxNum |= AT_IDX_PK_LE;
      }
      if( iLt>=0 ){
        pInfo->aConstraintUsage[iLt].argvIndex=argvIdx++;
        idxNum |= AT_IDX_PK_LT;
      }
    }
    pInfo->idxNum=idxNum;
    pInfo->estimatedCost=1000.0;
    pInfo->estimatedRows=1000;
    if( idxNum & AT_IDX_PK_EQ ){
      pInfo->estimatedCost=10.0;
      pInfo->estimatedRows=1;
    }else if( idxNum & AT_IDX_PK_ANY ){
      pInfo->estimatedCost=100.0;
      pInfo->estimatedRows=100;
    }
  }else{
    pInfo->idxNum=0;
    pInfo->estimatedCost=1e12;
  }
  return SQLITE_OK;
}

static int atFilter(sqlite3_vtab_cursor *cur,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  AtCursor *c=(AtCursor*)cur;
  DoltliteVtabCommon *v=(DoltliteVtabCommon*)cur->pVtab;
  sqlite3 *db=v->db;
  ChunkStore *cs=doltliteGetChunkStore(db);
  void *pBt; ProllyCache *pCache;
  const char *zRef;
  ProllyHash catHash;
  ProllyHash tableRoot; u8 flags=0;
  ProllyHash schemaHash;
  int rc, res;
  (void)idxStr;

  memset(&schemaHash, 0, sizeof(schemaHash));

  atCursorReset(c);
  c->idxNum = idxNum;
  if(!cs||(idxNum & AT_IDX_REF)==0||argc<1) return SQLITE_OK;
  if( (idxNum & AT_IDX_PK_EQ) && v->cols.iPkCol<0 && v->cols.nPk>0 ){
    int iPk;
    if( argc<1+v->cols.nPk ) return SQLITE_OK;
    for(iPk=0; iPk<v->cols.nPk; iPk++){
      if( sqlite3_value_type(argv[1+iPk])==SQLITE_NULL ) return SQLITE_OK;
    }
    rc = doltliteSortKeyFromPkValues(v->db, v->zTableName,
                                     v->cols.nPk, argv+1,
                                     &c->pPkBlob, &c->nPkBlob);
    if( rc!=SQLITE_OK ) return rc;
  }else{
    doltlitePkRangeFromArgs(idxNum,
        AT_IDX_PK_EQ, AT_IDX_PK_GE, AT_IDX_PK_LE,
        AT_IDX_PK_GT, AT_IDX_PK_LT,
        argc-1, argv+1, &c->pkRange);
    if( c->pkRange.isEmpty ) return SQLITE_OK;
  }

  pBt=doltliteGetBtShared(db);
  if(!pBt) return SQLITE_OK;
  pCache=doltliteGetCache(db);

  if( sqlite3_value_type(argv[0])==SQLITE_NULL ){
    sqlite3_free(cur->pVtab->zErrMsg);
    cur->pVtab->zErrMsg = sqlite3_mprintf(
        "dolt_at_%s: invalid argument: NULL", v->zTableName);
    return cur->pVtab->zErrMsg ? SQLITE_ERROR : SQLITE_NOMEM;
  }
  zRef=(const char*)sqlite3_value_text(argv[0]);
  if(!zRef) return SQLITE_NOMEM;
  c->zCommitRef = sqlite3_mprintf("%s", zRef);
  if( !c->zCommitRef ) return SQLITE_NOMEM;

  rc=doltliteResolveCatalogHashForRef(db,zRef,&catHash);
  if(rc==SQLITE_NOTFOUND){
    sqlite3_free(cur->pVtab->zErrMsg);
    cur->pVtab->zErrMsg = 0;
    if( atTakeChunkSourceError(cs, &cur->pVtab->zErrMsg, &rc) ) return rc;
    cur->pVtab->zErrMsg = sqlite3_mprintf("ref not found: %s", zRef);
    return SQLITE_ERROR;
  }
  if(rc!=SQLITE_OK) return rc;

  {
    ProllyHash branchCommit;
    ProllyHash effCatHash;
    int isBranch = (chunkStoreFindBranch(cs,zRef,&branchCommit)==SQLITE_OK
                    && !prollyHashIsEmpty(&branchCommit));
    if( isBranch ){
      rc = doltliteResolveBranchEffectiveCatalog(
          cs, zRef, &branchCommit, &catHash, &effCatHash);
    }else{
      memcpy(&effCatHash, &catHash, sizeof(ProllyHash));
    }
    if( rc==SQLITE_OK ){
      rc=doltliteLoadTableRootByName(db,&effCatHash,v->zTableName,&tableRoot,
                                     &flags,&schemaHash);
    }
    if( rc==SQLITE_OK ){
      rc = doltliteSideColsLoad(db, &effCatHash, &schemaHash,
                                v->zTableName, &v->cols,
                                !prollyHashIsEmpty(&tableRoot), &c->side);
    }
  }
  if(rc==SQLITE_NOTFOUND){
    sqlite3_free(cur->pVtab->zErrMsg);
    cur->pVtab->zErrMsg = 0;
    if( atTakeChunkSourceError(cs, &cur->pVtab->zErrMsg, &rc) ) return rc;
    return SQLITE_OK;
  }
  if(rc!=SQLITE_OK) return rc;

  if( prollyHashIsEmpty(&tableRoot) ) return SQLITE_OK;

  prollyCursorInit(&c->common.tblCur, cs, pCache, &tableRoot, flags);
  c->common.rootIntKey = (flags & PROLLY_NODE_INTKEY) != 0;

  if( !c->common.rootIntKey && c->pPkBlob
   && (idxNum & AT_IDX_PK_EQ) ){
    rc = prollyCursorSeekBlob(&c->common.tblCur, c->pPkBlob, c->nPkBlob, &res);
    if( rc!=SQLITE_OK ){
      prollyCursorClose(&c->common.tblCur);
      return rc;
    }
    if( res!=0 || !prollyCursorIsValid(&c->common.tblCur) ){
      prollyCursorClose(&c->common.tblCur);
      return SQLITE_OK;
    }
    c->common.tblCurOpen = 1;
    return doltliteVtabCommonCaptureRowSide(&c->common, v->db, v->zTableName,
                                            &c->side);
  }

  c->pkSeekable = c->common.rootIntKey
              && (idxNum & AT_IDX_PK_ANY) != 0
              && doltliteSideColsMatchIntPk(&c->side, &v->cols);

  if( c->pkSeekable && (idxNum & AT_IDX_PK_EQ) && c->pkRange.hasPkLo ){
    rc = prollyCursorSeekInt(&c->common.tblCur, c->pkRange.pkLo, &res);
    if( rc!=SQLITE_OK ){
      prollyCursorClose(&c->common.tblCur);
      return rc;
    }
    if( res!=0 || !prollyCursorIsValid(&c->common.tblCur) ){
      prollyCursorClose(&c->common.tblCur);
      return SQLITE_OK;
    }
    c->common.tblCurOpen = 1;
    return doltliteVtabCommonCaptureRowSide(&c->common, v->db, v->zTableName,
                                            &c->side);
  }

  if( c->pkSeekable && c->pkRange.hasPkLo ){
    i64 startKey = c->pkRange.pkLo;
    if( c->pkRange.pkLoStrict ) startKey++;
    rc = prollyCursorSeekInt(&c->common.tblCur, startKey, &res);
    if( rc!=SQLITE_OK ){
      prollyCursorClose(&c->common.tblCur);
      return rc;
    }
    if( res<0 ){
      rc = prollyCursorNext(&c->common.tblCur);
      if( rc!=SQLITE_OK ){
        prollyCursorClose(&c->common.tblCur);
        return rc;
      }
    }
    if( !prollyCursorIsValid(&c->common.tblCur) || !doltlitePkRangeMatchesCursorUpper(&c->pkRange, &c->common.tblCur) ){
      prollyCursorClose(&c->common.tblCur);
      return SQLITE_OK;
    }
    c->common.tblCurOpen = 1;
    return doltliteVtabCommonCaptureRowSide(&c->common, v->db, v->zTableName,
                                            &c->side);
  }

  rc = prollyCursorFirst(&c->common.tblCur, &res);
  if( rc!=SQLITE_OK ){
    prollyCursorClose(&c->common.tblCur);
    return rc;
  }
  if( res ){
    prollyCursorClose(&c->common.tblCur);
    return SQLITE_OK;
  }
  if( c->pkSeekable && c->pkRange.hasPkHi && !doltlitePkRangeMatchesCursorUpper(&c->pkRange, &c->common.tblCur) ){
    prollyCursorClose(&c->common.tblCur);
    return SQLITE_OK;
  }
  c->common.tblCurOpen = 1;
  return doltliteVtabCommonCaptureRowSide(&c->common, v->db, v->zTableName,
                                          &c->side);
}

static int atNext(sqlite3_vtab_cursor *cur){
  AtCursor *c=(AtCursor*)cur;
  DoltliteVtabCommon *v=(DoltliteVtabCommon*)cur->pVtab;
  int rc;
  c->common.iRowid++;
  if( !c->common.tblCurOpen ){
    c->common.hasRow = 0;
    return SQLITE_OK;
  }
  if( (c->idxNum & AT_IDX_PK_EQ)
   && ((c->pkSeekable && c->pkRange.hasPkLo) || c->pPkBlob) ){
    prollyCursorClose(&c->common.tblCur);
    c->common.tblCurOpen = 0;
    c->common.hasRow = 0;
    return SQLITE_OK;
  }
  rc = prollyCursorNext(&c->common.tblCur);
  if( rc!=SQLITE_OK ){
    prollyCursorClose(&c->common.tblCur);
    c->common.tblCurOpen = 0;
    c->common.hasRow = 0;
    return rc;
  }
  if( !prollyCursorIsValid(&c->common.tblCur) ){
    prollyCursorClose(&c->common.tblCur);
    c->common.tblCurOpen = 0;
    c->common.hasRow = 0;
    return SQLITE_OK;
  }
  if( c->pkSeekable
   && !doltlitePkRangeMatchesCursorUpper(&c->pkRange, &c->common.tblCur) ){
    prollyCursorClose(&c->common.tblCur);
    c->common.tblCurOpen = 0;
    c->common.hasRow = 0;
    return SQLITE_OK;
  }
  return doltliteVtabCommonCaptureRowSide(&c->common, v->db, v->zTableName,
                                          &c->side);
}

static int atColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int col){
  AtCursor *c=(AtCursor*)cur;
  DoltliteVtabCommon *v=(DoltliteVtabCommon*)cur->pVtab;
  int nCols=v->cols.nCol;

  if( !c->common.hasRow ) return SQLITE_OK;

  if( col==nCols ){
    sqlite3_result_text(ctx, c->zCommitRef ? c->zCommitRef : "",
                        -1, SQLITE_TRANSIENT);
  }else if(nCols>0 && col<nCols){
    doltliteResultSideCol(ctx, &c->side, &v->cols,
                          c->common.pVal, c->common.nVal,
                          c->common.intKey, c->common.rootIntKey, col,
                          v->cols.aAffinity[col]);
  }

  return SQLITE_OK;
}

static sqlite3_module atModule = {
  0, atConnect, atConnect, atBestIndex,
  doltliteVtabCommonDisconnect, doltliteVtabCommonDisconnect,
  atOpen, atClose,
  atFilter, atNext,
  doltliteVtabCommonEof, atColumn, doltliteVtabCommonRowid,
  0,0,0,0,0,0,0,0,0,0,0,0
};

static int atRegisterModule(
  sqlite3 *db,
  const char *zPrefix,
  const char *zName,
  const sqlite3_module *pModule
){
  char *zMod;
  int rc;
  zMod = sqlite3_mprintf("%s%s", zPrefix, zName);
  if( !zMod ) return SQLITE_NOMEM;
  rc = sqlite3_create_module(db, zMod, pModule, 0);
  sqlite3_free(zMod);
  return rc;
}

Module *doltliteHistoricalModuleRegister(sqlite3 *db, const char *zName){
  const sqlite3_module *pModule;
  const char *zPrefix;
  int nPrefix;
  int rc;

  if( db->nDb<=0 || !sqlite3BtreeIsDoltliteFormat(db->aDb[0].pBt) ){
    return 0;
  }
  if( sqlite3_strnicmp(zName, "dolt_at_", 8)==0 && zName[8] ){
    zPrefix = "dolt_at_";
    nPrefix = 8;
    pModule = &atModule;
  }else if( sqlite3_strnicmp(zName, "dolt_diff_", 10)==0 && zName[10] ){
    zPrefix = "dolt_diff_";
    nPrefix = 10;
    pModule = doltliteDiffTableModule();
  }else if( sqlite3_strnicmp(zName, "dolt_history_", 13)==0 && zName[13] ){
    zPrefix = "dolt_history_";
    nPrefix = 13;
    pModule = doltliteHistoryTableModule();
  }else{
    return 0;
  }

  rc = atRegisterModule(db, zPrefix, zName+nPrefix, pModule);
  if( rc!=SQLITE_OK ) return 0;
  return (Module*)sqlite3HashFind(&db->aModule, zName);
}

void doltliteHistoricalModulesReset(sqlite3 *db){
  HashElem *pElem;
  for(pElem=sqliteHashFirst(&db->aModule); pElem;
      pElem=sqliteHashNext(pElem)){
    Module *pModule = (Module*)sqliteHashData(pElem);
    const char *zName = pModule->zName;
    if( sqlite3_strnicmp(zName, "dolt_at_", 8)==0
     || sqlite3_strnicmp(zName, "dolt_diff_", 10)==0
     || sqlite3_strnicmp(zName, "dolt_history_", 13)==0 ){
      sqlite3VtabEponymousTableClear(db, pModule);
    }
  }
}

#endif
