
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

#define HT_MAX_COLS 64

typedef struct HtRecInfo HtRecInfo;
struct HtRecInfo { int nField; int aType[HT_MAX_COLS]; int aOffset[HT_MAX_COLS]; };

static void htParseRecord(const u8 *pData, int nData, HtRecInfo *ri){
  const u8 *p=pData, *pEnd=pData+nData;
  u64 hdrSize; int hdrBytes, off;
  memset(ri,0,sizeof(*ri));
  if(!pData||nData<1) return;
  hdrBytes=dlReadVarint(p,pEnd,&hdrSize); p+=hdrBytes;
  off=(int)hdrSize;
  while(p<pData+hdrSize && p<pEnd && ri->nField<HT_MAX_COLS){
    u64 st; int stBytes=dlReadVarint(p,pData+hdrSize,&st); p+=stBytes;
    ri->aType[ri->nField]=(int)st; ri->aOffset[ri->nField]=off;
    off += dlSerialTypeLen(st);
    ri->nField++;
  }
}

static void htResultField(sqlite3_context *ctx, const u8 *pData, int nData, int st, int off){
  if(st==0){sqlite3_result_null(ctx);return;}
  if(st==8){sqlite3_result_int(ctx,0);return;}
  if(st==9){sqlite3_result_int(ctx,1);return;}
  if(st>=1&&st<=6){
    static const int sz[]={0,1,2,3,4,6,8}; int nB=sz[st];
    if(off+nB<=nData){const u8*q=pData+off;i64 v=(q[0]&0x80)?-1:0;int i;
      for(i=0;i<nB;i++)v=(v<<8)|q[i];sqlite3_result_int64(ctx,v);}
    else sqlite3_result_null(ctx); return;
  }
  if(st==7){if(off+8<=nData){const u8*q=pData+off;double v;u64 bits=0;int i;
    for(i=0;i<8;i++)bits=(bits<<8)|q[i];memcpy(&v,&bits,8);
    sqlite3_result_double(ctx,v);}else sqlite3_result_null(ctx);return;}
  if(st>=13&&(st&1)==1){int len=(st-13)/2;
    if(off+len<=nData)sqlite3_result_text(ctx,(const char*)(pData+off),len,SQLITE_TRANSIENT);
    else sqlite3_result_null(ctx);return;}
  if(st>=12&&(st&1)==0){int len=(st-12)/2;
    if(off+len<=nData)sqlite3_result_blob(ctx,pData+off,len,SQLITE_TRANSIENT);
    else sqlite3_result_null(ctx);return;}
  sqlite3_result_null(ctx);
}

static char *htBuildSchema(DoltliteColInfo *ci){
  int i, sz=256;
  char *z;
  for(i=0;i<ci->nCol;i++) sz+=(int)strlen(ci->azName[i])+10;
  z=sqlite3_malloc(sz); if(!z) return 0;
  strcpy(z,"CREATE TABLE x(");
  for(i=0;i<ci->nCol;i++){
    if(i>0) strcat(z,", ");
    strcat(z,"\""); strcat(z,ci->azName[i]); strcat(z,"\"");
  }
  strcat(z,", commit_hash TEXT, committer TEXT, commit_date TEXT)");
  return z;
}

typedef struct HistoryRow HistoryRow;
struct HistoryRow {
  i64 intKey;
  u8 *pVal; int nVal;
  char zCommit[PROLLY_HASH_SIZE*2+1];
  char *zCommitter;
  i64 commitDate;
};

typedef struct HistVtab HistVtab;
struct HistVtab {
  sqlite3_vtab base;
  sqlite3 *db;
  char *zTableName;
  DoltliteColInfo cols;
};

typedef struct HistCursor HistCursor;
struct HistCursor {
  sqlite3_vtab_cursor base;
  HistoryRow *aRows;
  int nRows;
  int nAlloc;
  int iRow;
};

static void freeHistoryRows(HistCursor *c){
  int i;
  for(i=0;i<c->nRows;i++){
    sqlite3_free(c->aRows[i].pVal);
    sqlite3_free(c->aRows[i].zCommitter);
  }
  sqlite3_free(c->aRows);
  c->aRows=0; c->nRows=0; c->nAlloc=0;
}

static int htFindRoot(struct TableEntry *a, int n, const char *zName,
                      ProllyHash *pRoot, u8 *pFlags){
  struct TableEntry *e = doltliteFindTableByName(a, n, zName);
  if( e ){
    memcpy(pRoot, &e->root, sizeof(ProllyHash));
    if( pFlags ) *pFlags = e->flags;
    return SQLITE_OK;
  }
  memset(pRoot,0,sizeof(ProllyHash)); if(pFlags)*pFlags=0;
  return SQLITE_NOTFOUND;
}

static int htScanAtCommit(
  HistCursor *pCur, ChunkStore *cs, ProllyCache *pCache,
  const ProllyHash *pRoot, u8 flags,
  const char *zCommitHex, const char *zCommitter, i64 commitDate
){
  ProllyCursor cur; int res, rc;
  if(prollyHashIsEmpty(pRoot)) return SQLITE_OK;
  prollyCursorInit(&cur,cs,pCache,pRoot,flags);
  rc=prollyCursorFirst(&cur,&res);
  if(rc!=SQLITE_OK||res){prollyCursorClose(&cur);return rc;}
  while(prollyCursorIsValid(&cur)){
    const u8 *pVal; int nVal; HistoryRow *r;
    if(pCur->nRows>=pCur->nAlloc){
      int nNew=pCur->nAlloc?pCur->nAlloc*2:128;
      HistoryRow *aNew=sqlite3_realloc(pCur->aRows,nNew*(int)sizeof(HistoryRow));
      if(!aNew){prollyCursorClose(&cur);return SQLITE_NOMEM;}
      pCur->aRows=aNew; pCur->nAlloc=nNew;
    }
    r=&pCur->aRows[pCur->nRows]; memset(r,0,sizeof(*r));
    r->intKey=prollyCursorIntKey(&cur);
    prollyCursorValue(&cur,&pVal,&nVal);
    if(pVal&&nVal>0){r->pVal=sqlite3_malloc(nVal);if(r->pVal)memcpy(r->pVal,pVal,nVal);r->nVal=nVal;}
    memcpy(r->zCommit,zCommitHex,PROLLY_HASH_SIZE*2+1);
    r->zCommitter=sqlite3_mprintf("%s",zCommitter?zCommitter:"");
    r->commitDate=commitDate;
    pCur->nRows++;
    rc=prollyCursorNext(&cur); if(rc!=SQLITE_OK) break;
  }
  prollyCursorClose(&cur); return SQLITE_OK;
}

static int htWalkHistory(HistCursor *pCur, sqlite3 *db, const char *zTableName){
  ChunkStore *cs=doltliteGetChunkStore(db);
  ProllyCache *pCache; ProllyHash curHash; int rc;
  if(!cs) return SQLITE_OK;
  pCache=doltliteGetCache(db);
  if(!pCache) return SQLITE_OK;
  chunkStoreGetHeadCommit(cs,&curHash);
  while(!prollyHashIsEmpty(&curHash)){
    u8 *data=0;int nData=0; DoltliteCommit commit;
    ProllyHash tableRoot; u8 flags=0;
    char hexBuf[PROLLY_HASH_SIZE*2+1];
    memset(&commit,0,sizeof(commit));
    rc=chunkStoreGet(cs,&curHash,&data,&nData); if(rc!=SQLITE_OK) break;
    rc=doltliteCommitDeserialize(data,nData,&commit); sqlite3_free(data);
    if(rc!=SQLITE_OK) break;
    doltliteHashToHex(&curHash,hexBuf);
    {struct TableEntry *aT=0;int nT=0;
      rc=doltliteLoadCatalog(db,&commit.catalogHash,&aT,&nT,0);
      if(rc==SQLITE_OK){
        if(htFindRoot(aT,nT,zTableName,&tableRoot,&flags)==SQLITE_OK)
          htScanAtCommit(pCur,cs,pCache,&tableRoot,flags,hexBuf,commit.zName,commit.timestamp);
        sqlite3_free(aT);
      }
    }
    {ProllyHash next;memcpy(&next,&commit.parentHash,sizeof(ProllyHash));
      doltliteCommitClear(&commit);memcpy(&curHash,&next,sizeof(ProllyHash));}
  }
  return SQLITE_OK;
}

static int htConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  HistVtab *v; int rc; const char *zMod; char *zSchema;
  (void)pAux;(void)pzErr;

  v=sqlite3_malloc(sizeof(*v)); if(!v) return SQLITE_NOMEM;
  memset(v,0,sizeof(*v)); v->db=db;

  zMod=argv[0];
  if(zMod&&strncmp(zMod,"dolt_history_",13)==0)
    v->zTableName=sqlite3_mprintf("%s",zMod+13);
  else if(argc>3) v->zTableName=sqlite3_mprintf("%s",argv[3]);
  else v->zTableName=sqlite3_mprintf("");

  doltliteGetColumnNames(db,v->zTableName,&v->cols);

  if(v->cols.nCol>0){
    zSchema=htBuildSchema(&v->cols);
  }else{
    zSchema=sqlite3_mprintf("CREATE TABLE x(value TEXT, commit_hash TEXT, committer TEXT, commit_date TEXT)");
  }
  if(!zSchema){sqlite3_free(v->zTableName);doltliteFreeColInfo(&v->cols);sqlite3_free(v);return SQLITE_NOMEM;}

  rc=sqlite3_declare_vtab(db,zSchema); sqlite3_free(zSchema);
  if(rc!=SQLITE_OK){sqlite3_free(v->zTableName);doltliteFreeColInfo(&v->cols);sqlite3_free(v);return rc;}

  *ppVtab=&v->base; return SQLITE_OK;
}

static int htDisconnect(sqlite3_vtab *pVtab){
  HistVtab *v=(HistVtab*)pVtab;
  sqlite3_free(v->zTableName); doltliteFreeColInfo(&v->cols);
  sqlite3_free(v); return SQLITE_OK;
}

static int htBestIndex(sqlite3_vtab *v, sqlite3_index_info *p){
  (void)v; p->estimatedCost=100000.0; return SQLITE_OK;
}

static int htOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **pp){
  HistCursor *c;(void)pVtab;
  c=sqlite3_malloc(sizeof(*c)); if(!c) return SQLITE_NOMEM;
  memset(c,0,sizeof(*c)); *pp=&c->base; return SQLITE_OK;
}

static int htClose(sqlite3_vtab_cursor *cur){
  HistCursor *c=(HistCursor*)cur;
  freeHistoryRows(c); sqlite3_free(c); return SQLITE_OK;
}

static int htFilter(sqlite3_vtab_cursor *cur,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  HistCursor *c=(HistCursor*)cur;
  HistVtab *v=(HistVtab*)cur->pVtab;
  (void)idxNum;(void)idxStr;(void)argc;(void)argv;
  freeHistoryRows(c); c->iRow=0;
  htWalkHistory(c,v->db,v->zTableName);
  return SQLITE_OK;
}

static int htNext(sqlite3_vtab_cursor *cur){((HistCursor*)cur)->iRow++;return SQLITE_OK;}

static int htEof(sqlite3_vtab_cursor *cur){
  HistCursor *c=(HistCursor*)cur; return c->iRow>=c->nRows;
}

static int htColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int col){
  HistCursor *c=(HistCursor*)cur;
  HistVtab *v=(HistVtab*)cur->pVtab;
  HistoryRow *r;
  int nCols;
  if( c->iRow>=c->nRows ) return SQLITE_OK;
  r=&c->aRows[c->iRow];
  nCols=v->cols.nCol;

  

  if(nCols>0 && col<nCols){
    
    if(col==v->cols.iPkCol){
      
      sqlite3_result_int64(ctx,r->intKey);
    }else{
      
      if(r->pVal&&r->nVal>0){
        HtRecInfo ri; htParseRecord(r->pVal,r->nVal,&ri);
        if(col<ri.nField) htResultField(ctx,r->pVal,r->nVal,ri.aType[col],ri.aOffset[col]);
        else sqlite3_result_null(ctx);
      }else sqlite3_result_null(ctx);
    }
  }else{
    int fixedCol=col-nCols;
    if(nCols==0) fixedCol=col;
    switch(fixedCol){
      case 0: 
        if(nCols==0){
          
          char *z=doltliteDecodeRecord(r->pVal,r->nVal);
          if(z) sqlite3_result_text(ctx,z,-1,sqlite3_free);
          else sqlite3_result_null(ctx);
        }else{
          sqlite3_result_text(ctx,r->zCommit,-1,SQLITE_TRANSIENT);
        }
        break;
      case 1: 
        sqlite3_result_text(ctx,r->zCommitter,-1,SQLITE_TRANSIENT);
        break;
      case 2: 
        {time_t t=(time_t)r->commitDate;struct tm *tm=gmtime(&t);
          if(tm){char b[32];strftime(b,sizeof(b),"%Y-%m-%d %H:%M:%S",tm);
            sqlite3_result_text(ctx,b,-1,SQLITE_TRANSIENT);
          }else sqlite3_result_null(ctx);}
        break;
    }
  }
  return SQLITE_OK;
}

static int htRowid(sqlite3_vtab_cursor *cur, sqlite3_int64 *r){
  *r=((HistCursor*)cur)->iRow; return SQLITE_OK;
}

static sqlite3_module historyModule = {
  0, htConnect, htConnect, htBestIndex, htDisconnect, htDisconnect,
  htOpen, htClose, htFilter, htNext, htEof, htColumn, htRowid,
  0,0,0,0,0,0,0,0,0,0,0,0
};

void doltliteRegisterHistoryTables(sqlite3 *db){
  ChunkStore *cs=doltliteGetChunkStore(db);
  ProllyHash headCommit; u8 *data=0;int nData=0;
  DoltliteCommit commit; struct TableEntry *aT=0;int nT=0,i,rc;
  if(!cs) return;
  chunkStoreGetHeadCommit(cs,&headCommit);
  if(prollyHashIsEmpty(&headCommit)) return;
  rc=chunkStoreGet(cs,&headCommit,&data,&nData); if(rc!=SQLITE_OK) return;
  memset(&commit,0,sizeof(commit));
  rc=doltliteCommitDeserialize(data,nData,&commit); sqlite3_free(data);
  if(rc!=SQLITE_OK) return;
  rc=doltliteLoadCatalog(db,&commit.catalogHash,&aT,&nT,0);
  doltliteCommitClear(&commit); if(rc!=SQLITE_OK) return;
  for(i=0;i<nT;i++){
    if(aT[i].zName&&aT[i].iTable>1){
      char *zMod=sqlite3_mprintf("dolt_history_%s",aT[i].zName);
      if(zMod){sqlite3_create_module(db,zMod,&historyModule,0);sqlite3_free(zMod);}
    }
  }
  sqlite3_free(aT);
}

#endif 
