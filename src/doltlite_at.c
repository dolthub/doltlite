
#ifdef DOLTLITE_PROLLY

#include "doltlite_vtab_util.h"
#include "doltlite_commit.h"
#include "doltlite_internal.h"

#include <string.h>
#include <time.h>

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
};

static void atCursorReset(AtCursor *c){
  doltliteVtabCommonReset(&c->common);
  sqlite3_free(c->zCommitRef);
  c->zCommitRef = 0;
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
  int iRef=-1, i, argvIdx=1;

  int refCol = nCols > 0 ? nCols : 2;
  (void)pVtab;

  for(i=0;i<pInfo->nConstraint;i++){
    if(!pInfo->aConstraint[i].usable) continue;
    if(pInfo->aConstraint[i].op!=SQLITE_INDEX_CONSTRAINT_EQ) continue;
    if(pInfo->aConstraint[i].iColumn==refCol) iRef=i;
  }

  if(iRef>=0){
    pInfo->aConstraintUsage[iRef].argvIndex=argvIdx++;
    pInfo->aConstraintUsage[iRef].omit=1;
    pInfo->idxNum=1;
    pInfo->estimatedCost=1000.0;
  }else{
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
  ProllyHash commitHash;
  DoltliteCommit commit;
  struct TableEntry *aTables=0; int nTables=0;
  ProllyHash tableRoot; u8 flags=0;
  int rc, res;
  (void)idxStr;

  atCursorReset(c);
  if(!cs||idxNum!=1||argc<1) return SQLITE_OK;

  pBt=doltliteGetBtShared(db);
  if(!pBt) return SQLITE_OK;
  pCache=doltliteGetCache(db);

  zRef=(const char*)sqlite3_value_text(argv[0]);
  if(!zRef) return SQLITE_OK;
  c->zCommitRef = sqlite3_mprintf("%s", zRef);
  if( !c->zCommitRef ) return SQLITE_NOMEM;

  rc=doltliteResolveRef(db,zRef,&commitHash);
  if(rc==SQLITE_NOTFOUND){
    sqlite3_free(cur->pVtab->zErrMsg);
    cur->pVtab->zErrMsg = sqlite3_mprintf("ref not found: %s", zRef);
    return SQLITE_ERROR;
  }
  if(rc!=SQLITE_OK) return rc;

  memset(&commit,0,sizeof(commit));
  rc=doltliteLoadCommit(db,&commitHash,&commit);
  if(rc!=SQLITE_OK) return rc;

  {
    ProllyHash branchCommit;
    ProllyHash effCatHash;
    int isBranch = (chunkStoreFindBranch(cs,zRef,&branchCommit)==SQLITE_OK
                    && !prollyHashIsEmpty(&branchCommit));
    if( isBranch ){
      doltliteResolveBranchEffectiveCatalog(cs, zRef, &branchCommit,
                                            &commit.catalogHash, &effCatHash);
    }else{
      memcpy(&effCatHash, &commit.catalogHash, sizeof(ProllyHash));
    }
    rc=doltliteLoadCatalog(db,&effCatHash,&aTables,&nTables,0);
  }
  doltliteCommitClear(&commit);
  if(rc!=SQLITE_OK) return rc;

  rc=doltliteFindTableRootByName(aTables,nTables,v->zTableName,&tableRoot,&flags,0);
  doltliteFreeCatalog(aTables,nTables);
  if(rc==SQLITE_NOTFOUND) return SQLITE_OK;
  if(rc!=SQLITE_OK) return rc;

  if( prollyHashIsEmpty(&tableRoot) ) return SQLITE_OK;

  prollyCursorInit(&c->common.tblCur, cs, pCache, &tableRoot, flags);
  rc = prollyCursorFirst(&c->common.tblCur, &res);
  if( rc!=SQLITE_OK ){
    prollyCursorClose(&c->common.tblCur);
    return rc;
  }
  if( res ){
    prollyCursorClose(&c->common.tblCur);
    return SQLITE_OK;
  }
  c->common.tblCurOpen = 1;
  return doltliteVtabCommonCaptureRow(&c->common);
}

static int atNext(sqlite3_vtab_cursor *cur){
  AtCursor *c=(AtCursor*)cur;
  int rc;
  c->common.iRowid++;
  if( !c->common.tblCurOpen ){
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
  return doltliteVtabCommonCaptureRow(&c->common);
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
