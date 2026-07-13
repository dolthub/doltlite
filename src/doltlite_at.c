
#ifdef DOLTLITE_PROLLY

#include "doltlite_vtab_util.h"
#include "doltlite_commit.h"
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
  sqlite3_str *pStr = sqlite3_str_new(0);
  char *z;
  if( !pStr ) return 0;
  sqlite3_str_appendall(pStr, "CREATE TABLE x(");
  if( doltliteAppendQuotedColumnList(pStr, ci->azName, ci->nCol, 0, 0)!=SQLITE_OK ){
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
  char *zCommitRef;
  int idxNum;
  DoltlitePkRange pkRange;
};

static void atCursorReset(AtCursor *c){
  doltliteVtabCommonReset(&c->common);
  sqlite3_free(c->zCommitRef);
  c->zCommitRef = 0;
}

static int atRowMatchesUpper(AtCursor *c){
  i64 k;
  if( !c->pkRange.hasPkHi ) return 1;
  k = prollyCursorIntKey(&c->common.tblCur);
  if( c->pkRange.pkHiStrict ){
    return k < c->pkRange.pkHi;
  }
  return k <= c->pkRange.pkHi;
}

static int atConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  (void)pAux;
  return doltliteVtabConnectUserTable(db, argc, argv, "dolt_at_",
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
    if( iEq>=0 ){
      pInfo->aConstraintUsage[iEq].argvIndex=argvIdx++;
      pInfo->aConstraintUsage[iEq].omit=1;
      idxNum |= AT_IDX_PK_EQ;
    }else{
      if( iGe>=0 ){
        pInfo->aConstraintUsage[iGe].argvIndex=argvIdx++;
        pInfo->aConstraintUsage[iGe].omit=1;
        idxNum |= AT_IDX_PK_GE;
      }
      if( iGt>=0 ){
        pInfo->aConstraintUsage[iGt].argvIndex=argvIdx++;
        pInfo->aConstraintUsage[iGt].omit=1;
        idxNum |= AT_IDX_PK_GT;
      }
      if( iLe>=0 ){
        pInfo->aConstraintUsage[iLe].argvIndex=argvIdx++;
        pInfo->aConstraintUsage[iLe].omit=1;
        idxNum |= AT_IDX_PK_LE;
      }
      if( iLt>=0 ){
        pInfo->aConstraintUsage[iLt].argvIndex=argvIdx++;
        pInfo->aConstraintUsage[iLt].omit=1;
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
  int rc, res;
  int seekable;
  (void)idxStr;

  atCursorReset(c);
  c->idxNum = idxNum;
  if(!cs||(idxNum & AT_IDX_REF)==0||argc<1) return SQLITE_OK;
  doltlitePkRangeFromArgs(idxNum,
      AT_IDX_PK_EQ, AT_IDX_PK_GE, AT_IDX_PK_LE,
      AT_IDX_PK_GT, AT_IDX_PK_LT,
      argc-1, argv+1, &c->pkRange);

  pBt=doltliteGetBtShared(db);
  if(!pBt) return SQLITE_OK;
  pCache=doltliteGetCache(db);

  zRef=(const char*)sqlite3_value_text(argv[0]);
  if(!zRef) return SQLITE_OK;
  c->zCommitRef = sqlite3_mprintf("%s", zRef);
  if( !c->zCommitRef ) return SQLITE_NOMEM;

  rc=doltliteRefToCatalogHash(db,zRef,&catHash);
  if(rc==SQLITE_NOTFOUND){
    sqlite3_free(cur->pVtab->zErrMsg);
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
      doltliteResolveBranchEffectiveCatalog(cs, zRef, &branchCommit,
                                            &catHash, &effCatHash);
    }else{
      memcpy(&effCatHash, &catHash, sizeof(ProllyHash));
    }
    rc=doltliteLoadTableRootByName(db,&effCatHash,v->zTableName,&tableRoot,
                                   &flags,0);
  }
  if(rc==SQLITE_NOTFOUND) return SQLITE_OK;
  if(rc!=SQLITE_OK) return rc;

  if( prollyHashIsEmpty(&tableRoot) ) return SQLITE_OK;

  prollyCursorInit(&c->common.tblCur, cs, pCache, &tableRoot, flags);

  seekable = (flags & PROLLY_NODE_INTKEY) != 0
          && (idxNum & AT_IDX_PK_ANY) != 0;

  if( seekable && (idxNum & AT_IDX_PK_EQ) ){
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
    return doltliteVtabCommonCaptureRow(&c->common, v->db, v->zTableName);
  }

  if( seekable && c->pkRange.hasPkLo ){
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
    if( !prollyCursorIsValid(&c->common.tblCur) || !atRowMatchesUpper(c) ){
      prollyCursorClose(&c->common.tblCur);
      return SQLITE_OK;
    }
    c->common.tblCurOpen = 1;
    return doltliteVtabCommonCaptureRow(&c->common, v->db, v->zTableName);
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
  if( seekable && c->pkRange.hasPkHi && !atRowMatchesUpper(c) ){
    prollyCursorClose(&c->common.tblCur);
    return SQLITE_OK;
  }
  c->common.tblCurOpen = 1;
  return doltliteVtabCommonCaptureRow(&c->common, v->db, v->zTableName);
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
  if( c->idxNum & AT_IDX_PK_EQ ){
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
  if( (c->idxNum & AT_IDX_PK_ANY) && !atRowMatchesUpper(c) ){
    prollyCursorClose(&c->common.tblCur);
    c->common.tblCurOpen = 0;
    c->common.hasRow = 0;
    return SQLITE_OK;
  }
  return doltliteVtabCommonCaptureRow(&c->common, v->db, v->zTableName);
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
    doltliteResultUserCol(ctx, &v->cols, c->common.pVal, c->common.nVal,
                          c->common.intKey, col);
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

int doltliteRegisterAtTables(sqlite3 *db){
  return doltliteForEachUserTable(db, "dolt_at_", &atModule);
}

#endif
