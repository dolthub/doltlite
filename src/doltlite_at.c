
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
  int i, sz=256;
  char *z;
  for(i=0;i<ci->nCol;i++) sz+=(int)strlen(ci->azName[i])+10;
  z=sqlite3_malloc(sz); if(!z) return 0;
  strcpy(z,"CREATE TABLE x(");
  for(i=0;i<ci->nCol;i++){
    if(i>0) strcat(z,", ");
    strcat(z,"\""); strcat(z,ci->azName[i]); strcat(z,"\"");
  }
  strcat(z,", commit_ref TEXT HIDDEN)");
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
};

static void freeAtRows(AtCursor *c){
  int i; for(i=0;i<c->nRows;i++) sqlite3_free(c->aRows[i].pVal);
  sqlite3_free(c->aRows); c->aRows=0; c->nRows=0; c->nAlloc=0;
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
    if(pVal&&nVal>0){r->pVal=sqlite3_malloc(nVal);if(r->pVal)memcpy(r->pVal,pVal,nVal);r->nVal=nVal;}
    pCur->nRows++;
    rc=prollyCursorNext(&cur); if(rc!=SQLITE_OK) break;
  }
  prollyCursorClose(&cur); return SQLITE_OK;
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

  doltliteGetColumnNames(db,v->zTableName,&v->cols);

  if(v->cols.nCol<=0){
    sqlite3_free(v->zTableName);doltliteFreeColInfo(&v->cols);sqlite3_free(v);
    *pzErr=sqlite3_mprintf("table '%s' not found or has no columns",v->zTableName?v->zTableName:"");
    return SQLITE_ERROR;
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

  rc=doltliteResolveRef(db,zRef,&commitHash);
  if(rc!=SQLITE_OK) return SQLITE_OK;

  memset(&commit,0,sizeof(commit));
  rc=doltliteLoadCommit(db,&commitHash,&commit);
  if(rc!=SQLITE_OK) return SQLITE_OK;

  /* For branch refs, prefer the working state catalog if it has
  ** uncommitted changes (same logic as checkout). */
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
        /* Working state has uncommitted changes — use it. */
        rc=doltliteLoadCatalog(db,&wsCatHash,&aTables,&nTables,0);
        doltliteCommitClear(&commit);
        if(rc!=SQLITE_OK) return SQLITE_OK;
        goto at_find_root;
      }
    }
  }

  rc=doltliteLoadCatalog(db,&commit.catalogHash,&aTables,&nTables,0);
  doltliteCommitClear(&commit);
  if(rc!=SQLITE_OK) return SQLITE_OK;

at_find_root:

  rc=doltliteFindTableRootByName(aTables,nTables,v->zTableName,&tableRoot,&flags);
  sqlite3_free(aTables);
  if(rc!=SQLITE_OK) return SQLITE_OK;

  atScanTree(c,cs,pCache,&tableRoot,flags);
  return SQLITE_OK;
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

  if(nCols>0 && col<nCols){
    
    if(col==v->cols.iPkCol){
      sqlite3_result_int64(ctx,r->intKey);
    }else{
      if(r->pVal&&r->nVal>0){
        DoltliteRecordInfo ri; doltliteParseRecord(r->pVal,r->nVal,&ri);
        if(col<ri.nField) doltliteResultField(ctx,r->pVal,r->nVal,ri.aType[col],ri.aOffset[col]);
        else sqlite3_result_null(ctx);
      }else sqlite3_result_null(ctx);
    }
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

void doltliteRegisterAtTables(sqlite3 *db){
  ProllyHash headCommit; u8 *data=0;int nData=0;
  DoltliteCommit commit; struct TableEntry *aT=0;int nT=0,i,rc;
  ChunkStore *cs=doltliteGetChunkStore(db);
  if(!cs) return;
  doltliteGetSessionHead(db,&headCommit);
  if(prollyHashIsEmpty(&headCommit)) return;
  rc=chunkStoreGet(cs,&headCommit,&data,&nData); if(rc!=SQLITE_OK) return;
  memset(&commit,0,sizeof(commit));
  rc=doltliteCommitDeserialize(data,nData,&commit); sqlite3_free(data);
  if(rc!=SQLITE_OK) return;
  rc=doltliteLoadCatalog(db,&commit.catalogHash,&aT,&nT,0);
  doltliteCommitClear(&commit); if(rc!=SQLITE_OK) return;
  for(i=0;i<nT;i++){
    if(aT[i].zName&&aT[i].iTable>1){
      char *zMod=sqlite3_mprintf("dolt_at_%s",aT[i].zName);
      if(zMod){sqlite3_create_module(db,zMod,&atModule,0);sqlite3_free(zMod);}
    }
  }
  sqlite3_free(aT);
}

int doltliteAtRegister(sqlite3 *db){
  
  
  doltliteRegisterAtTables(db);
  return SQLITE_OK;
}

#endif 
