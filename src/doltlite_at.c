
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_cursor.h"
#include "prolly_cache.h"
#include "chunk_store.h"
#include "doltlite_commit.h"
#include "doltlite_record.h"
#include "doltlite_internal.h"

#include <string.h>
#include <time.h>

static char *atBuildSchema(DoltliteColInfo *ci){
  int i;
  sqlite3_str *pStr = sqlite3_str_new(0);
  char *z;
  if( !pStr ) return 0;
  sqlite3_str_appendall(pStr, "CREATE TABLE x(");
  for(i=0; i<ci->nCol; i++){
    if( i>0 ) sqlite3_str_appendall(pStr, ", ");
    if( doltliteAppendQuotedIdent(pStr, ci->azName[i])!=SQLITE_OK ){
      sqlite3_str_reset(pStr);
      return 0;
    }
  }
  sqlite3_str_appendall(pStr, ", commit_ref TEXT HIDDEN)");
  z = sqlite3_str_finish(pStr);
  return z;
}

typedef struct AtRow AtRow;
struct AtRow {
  i64 intKey;
  u8 *pVal; int nVal;
};

typedef struct AtVtab AtVtab;
struct AtVtab {
  sqlite3_vtab base;
  sqlite3 *db;
  char *zTableName;
  DoltliteColInfo cols;
};

typedef struct AtCursor AtCursor;
struct AtCursor {
  sqlite3_vtab_cursor base;
  AtRow *aRows;
  int nRows;
  int nAlloc;
  int iRow;
  char *zCommitRef;
};

static void freeAtRows(AtCursor *c){
  int i; for(i=0;i<c->nRows;i++) sqlite3_free(c->aRows[i].pVal);
  sqlite3_free(c->aRows); c->aRows=0; c->nRows=0; c->nAlloc=0;
  sqlite3_free(c->zCommitRef); c->zCommitRef=0;
}

static int atScanTree(AtCursor *pCur, ChunkStore *cs, ProllyCache *pCache,
                      const ProllyHash *pRoot, u8 flags){
  ProllyCursor cur; int res, rc;
  if(prollyHashIsEmpty(pRoot)) return SQLITE_OK;
  prollyCursorInit(&cur,cs,pCache,pRoot,flags);
  rc=prollyCursorFirst(&cur,&res);
  if(rc!=SQLITE_OK||res){prollyCursorClose(&cur);return rc;}
  while(prollyCursorIsValid(&cur)){
    const u8 *pVal; int nVal; AtRow *r;
    if(pCur->nRows>=pCur->nAlloc){
      int nNew=pCur->nAlloc?pCur->nAlloc*2:128;
      AtRow *aNew=sqlite3_realloc(pCur->aRows,nNew*(int)sizeof(AtRow));
      if(!aNew){prollyCursorClose(&cur);return SQLITE_NOMEM;}
      pCur->aRows=aNew; pCur->nAlloc=nNew;
    }
    r=&pCur->aRows[pCur->nRows]; memset(r,0,sizeof(*r));
    r->intKey=prollyCursorIntKey(&cur);
    prollyCursorValue(&cur,&pVal,&nVal);
    if( pVal && nVal>0 ){
      r->pVal = sqlite3_malloc(nVal);
      if( !r->pVal ){
        prollyCursorClose(&cur);
        return SQLITE_NOMEM;
      }
      memcpy(r->pVal,pVal,nVal);
      r->nVal=nVal;
    }
    pCur->nRows++;
    rc=prollyCursorNext(&cur); if(rc!=SQLITE_OK) break;
  }
  prollyCursorClose(&cur);
  return rc;
}

static int atConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  AtVtab *v; int rc; const char *zMod; char *zSchema;
  (void)pAux;

  v=sqlite3_malloc(sizeof(*v)); if(!v) return SQLITE_NOMEM;
  memset(v,0,sizeof(*v)); v->db=db;

  zMod=argv[0];
  if(zMod&&strncmp(zMod,"dolt_at_",8)==0)
    v->zTableName=sqlite3_mprintf("%s",zMod+8);
  else if(argc>3) v->zTableName=sqlite3_mprintf("%s",argv[3]);
  else v->zTableName=sqlite3_mprintf("");

  rc = doltliteLoadUserTableColumns(db, v->zTableName, &v->cols, pzErr);
  if( rc!=SQLITE_OK ){
    sqlite3_free(v->zTableName);doltliteFreeColInfo(&v->cols);sqlite3_free(v);
    return rc;
  }
  zSchema=atBuildSchema(&v->cols);
  if(!zSchema){sqlite3_free(v->zTableName);doltliteFreeColInfo(&v->cols);sqlite3_free(v);return SQLITE_NOMEM;}

  rc=sqlite3_declare_vtab(db,zSchema); sqlite3_free(zSchema);
  if(rc!=SQLITE_OK){sqlite3_free(v->zTableName);doltliteFreeColInfo(&v->cols);sqlite3_free(v);return rc;}

  *ppVtab=&v->base; return SQLITE_OK;
}

static int atDisconnect(sqlite3_vtab *pVtab){
  AtVtab *v=(AtVtab*)pVtab;
  sqlite3_free(v->zTableName); doltliteFreeColInfo(&v->cols);
  sqlite3_free(v); return SQLITE_OK;
}

static int atBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  AtVtab *v=(AtVtab*)pVtab;
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

static int atOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **pp){
  AtCursor *c;(void)pVtab;
  c=sqlite3_malloc(sizeof(*c)); if(!c) return SQLITE_NOMEM;
  memset(c,0,sizeof(*c)); *pp=&c->base; return SQLITE_OK;
}

static int atClose(sqlite3_vtab_cursor *cur){
  AtCursor *c=(AtCursor*)cur;
  freeAtRows(c); sqlite3_free(c); return SQLITE_OK;
}

static int atFilter(sqlite3_vtab_cursor *cur,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  AtCursor *c=(AtCursor*)cur;
  AtVtab *v=(AtVtab*)cur->pVtab;
  sqlite3 *db=v->db;
  ChunkStore *cs=doltliteGetChunkStore(db);
  void *pBt; ProllyCache *pCache;
  const char *zRef;
  ProllyHash commitHash;
  DoltliteCommit commit;
  struct TableEntry *aTables=0; int nTables=0;
  ProllyHash tableRoot; u8 flags=0;
  u8 *data=0; int nData=0; int rc;
  (void)idxStr;

  freeAtRows(c); c->iRow=0;
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


  /* When the ref is a branch name (not a commit hash or tag), prefer
  ** its working catalog over the committed catalog IF the working set
  ** is still based on HEAD — that lets dolt_at_<table>@branch show
  ** staged-but-uncommitted rows on that branch. If the working set
  ** points at a different commit (mid-merge, stale WIP) fall through
  ** and use the committed catalog so the output still makes sense. */
  {
    ProllyHash branchCommit;
    int isBranch = (chunkStoreFindBranch(cs,zRef,&branchCommit)==SQLITE_OK
                    && !prollyHashIsEmpty(&branchCommit));
    if( isBranch ){
      ProllyHash wsCatHash, wsCommitHash;
      memset(&wsCatHash,0,sizeof(wsCatHash));
      memset(&wsCommitHash,0,sizeof(wsCommitHash));
      if( chunkStoreReadBranchWorkingCatalog(cs,zRef,&wsCatHash,&wsCommitHash)==SQLITE_OK
       && !prollyHashIsEmpty(&wsCommitHash)
       && memcmp(wsCommitHash.data,branchCommit.data,PROLLY_HASH_SIZE)==0
       && memcmp(wsCatHash.data,commit.catalogHash.data,PROLLY_HASH_SIZE)!=0 ){

        rc=doltliteLoadCatalog(db,&wsCatHash,&aTables,&nTables,0);
        doltliteCommitClear(&commit);
        if(rc!=SQLITE_OK) return rc;
        goto at_find_root;
      }
    }
  }

  rc=doltliteLoadCatalog(db,&commit.catalogHash,&aTables,&nTables,0);
  doltliteCommitClear(&commit);
  if(rc!=SQLITE_OK) return rc;

at_find_root:

  rc=doltliteFindTableRootByName(aTables,nTables,v->zTableName,&tableRoot,&flags);
  doltliteFreeCatalog(aTables,nTables);
  if(rc==SQLITE_NOTFOUND) return SQLITE_OK;
  if(rc!=SQLITE_OK) return rc;

  rc = atScanTree(c,cs,pCache,&tableRoot,flags);
  return rc;
}

static int atNext(sqlite3_vtab_cursor *cur){((AtCursor*)cur)->iRow++;return SQLITE_OK;}

static int atEof(sqlite3_vtab_cursor *cur){
  AtCursor *c=(AtCursor*)cur; return c->iRow>=c->nRows;
}

static int atColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int col){
  AtCursor *c=(AtCursor*)cur;
  AtVtab *v=(AtVtab*)cur->pVtab;
  AtRow *r=&c->aRows[c->iRow];
  int nCols=v->cols.nCol;

  if( col==nCols ){
    sqlite3_result_text(ctx, c->zCommitRef ? c->zCommitRef : "",
                        -1, SQLITE_TRANSIENT);
  }else if(nCols>0 && col<nCols){
    doltliteResultUserCol(ctx, &v->cols, r->pVal, r->nVal, r->intKey, col);
  }

  return SQLITE_OK;
}

static int atRowid(sqlite3_vtab_cursor *cur, sqlite3_int64 *r){
  *r=((AtCursor*)cur)->iRow; return SQLITE_OK;
}

static sqlite3_module atModule = {
  0, atConnect, atConnect, atBestIndex, atDisconnect, atDisconnect,
  atOpen, atClose, atFilter, atNext, atEof, atColumn, atRowid,
  0,0,0,0,0,0,0,0,0,0,0,0
};

int doltliteRegisterAtTables(sqlite3 *db){
  return doltliteForEachUserTable(db, "dolt_at_", &atModule);
}

int doltliteAtRegister(sqlite3 *db){
  return doltliteRegisterAtTables(db);
}

#endif
