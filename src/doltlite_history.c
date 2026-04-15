
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_cursor.h"
#include "prolly_cache.h"
#include "prolly_hashset.h"
#include "chunk_store.h"
#include "doltlite_commit.h"

#include "doltlite_record.h"
#include "doltlite_internal.h"
#include <string.h>
#include <time.h>

static char *htBuildSchema(DoltliteColInfo *ci){
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
  sqlite3_str_appendall(pStr, ", commit_hash TEXT, committer TEXT, commit_date TEXT)");
  z = sqlite3_str_finish(pStr);
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

static void freeHistoryRow(HistoryRow *r){
  sqlite3_free(r->pVal);
  sqlite3_free(r->zCommitter);
  memset(r, 0, sizeof(*r));
}

static void freeHistoryRows(HistCursor *c){
  int i;
  for(i=0;i<c->nRows;i++){
    freeHistoryRow(&c->aRows[i]);
  }
  sqlite3_free(c->aRows);
  c->aRows=0; c->nRows=0; c->nAlloc=0;
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
    if(pVal&&nVal>0){
      r->pVal=sqlite3_malloc(nVal);
      if( !r->pVal ){
        freeHistoryRow(r);
        prollyCursorClose(&cur);
        return SQLITE_NOMEM;
      }
      memcpy(r->pVal,pVal,nVal);
      r->nVal=nVal;
    }
    memcpy(r->zCommit,zCommitHex,PROLLY_HASH_SIZE*2+1);
    r->zCommitter=sqlite3_mprintf("%s",zCommitter?zCommitter:"");
    if( !r->zCommitter ){
      freeHistoryRow(r);
      prollyCursorClose(&cur);
      return SQLITE_NOMEM;
    }
    r->commitDate=commitDate;
    pCur->nRows++;
    rc=prollyCursorNext(&cur); if(rc!=SQLITE_OK) break;
  }
  prollyCursorClose(&cur); return rc;
}

/* dolt_history_<table> expands to one row per (commit, live row) in
** the table's history — so a 100-row table over 10 commits emits
** 1000 rows. The scan walks the commit graph BFS, reloads the table
** root at each visited commit, and emits every live row with that
** commit's metadata attached. No de-duplication: unchanged rows
** reappear under every commit, matching Dolt's dolt_history_T
** semantics. */
static int htWalkHistory(HistCursor *pCur, sqlite3 *db, const char *zTableName){
  ChunkStore *cs=doltliteGetChunkStore(db);
  ProllyCache *pCache;
  ProllyHash *queue=0;
  ProllyHashSet visited, queued;
  int qHead=0, qTail=0, qAlloc=0;
  int rc=SQLITE_OK;
  ProllyHash head;

  if(!cs) return SQLITE_OK;
  pCache=doltliteGetCache(db);
  if(!pCache) return SQLITE_OK;
  memset(&visited, 0, sizeof(visited));
  memset(&queued, 0, sizeof(queued));

  doltliteGetSessionHead(db, &head);
  if(prollyHashIsEmpty(&head)) return SQLITE_OK;

  rc = prollyHashSetInit(&visited, 64);
  if( rc!=SQLITE_OK ) return rc;
  rc = prollyHashSetInit(&queued, 64);
  if( rc!=SQLITE_OK ){
    prollyHashSetFree(&visited);
    return rc;
  }

  qAlloc=16;
  queue=sqlite3_malloc(qAlloc*(int)sizeof(ProllyHash));
  if(!queue){
    prollyHashSetFree(&visited);
    prollyHashSetFree(&queued);
    return SQLITE_NOMEM;
  }
  queue[qTail++]=head;
  rc = prollyHashSetAdd(&queued, &head);
  if( rc!=SQLITE_OK ) goto history_done;

  while(qHead<qTail){
    ProllyHash cur=queue[qHead++];
    DoltliteCommit commit;
    ProllyHash tableRoot; u8 flags=0;
    char hexBuf[PROLLY_HASH_SIZE*2+1];
    int i;

    if( prollyHashSetContains(&visited, &cur) ) continue;
    rc = prollyHashSetAdd(&visited, &cur);
    if( rc!=SQLITE_OK ) break;

    memset(&commit,0,sizeof(commit));
    rc=doltliteLoadCommit(db,&cur,&commit);
    if(rc!=SQLITE_OK) break;

    doltliteHashToHex(&cur,hexBuf);
    {
      struct TableEntry *aT=0; int nT=0;
      rc=doltliteLoadCatalog(db,&commit.catalogHash,&aT,&nT,0);
      if(rc==SQLITE_OK){
        if(doltliteFindTableRootByName(aT,nT,zTableName,&tableRoot,&flags)==SQLITE_OK)
          rc = htScanAtCommit(pCur,cs,pCache,&tableRoot,flags,hexBuf,commit.zName,commit.timestamp);
        doltliteFreeCatalog(aT,nT);
      }
      if( rc!=SQLITE_OK ){ doltliteCommitClear(&commit); break; }
    }

    for(i=0; i<doltliteCommitParentCount(&commit); i++){
      const ProllyHash *pParent = doltliteCommitParentHash(&commit, i);
      if( !pParent || prollyHashIsEmpty(pParent) ) continue;
      if( prollyHashSetContains(&visited, pParent) ) continue;
      if( prollyHashSetContains(&queued, pParent) ) continue;
      if(qTail>=qAlloc){
        int na=qAlloc*2;
        ProllyHash *tmp=sqlite3_realloc(queue,na*(int)sizeof(ProllyHash));
        if(!tmp){rc=SQLITE_NOMEM;break;}
        queue=tmp; qAlloc=na;
      }
      queue[qTail++]=*pParent;
      rc = prollyHashSetAdd(&queued, pParent);
      if( rc!=SQLITE_OK ) break;
    }
    doltliteCommitClear(&commit);
    if(rc!=SQLITE_OK) break;
  }

history_done:
  sqlite3_free(queue);
  prollyHashSetFree(&visited);
  prollyHashSetFree(&queued);
  return rc;
}

static int htConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  HistVtab *v; int rc; const char *zMod; char *zSchema;
  (void)pAux;

  v=sqlite3_malloc(sizeof(*v)); if(!v) return SQLITE_NOMEM;
  memset(v,0,sizeof(*v)); v->db=db;

  zMod=argv[0];
  if(zMod&&strncmp(zMod,"dolt_history_",13)==0)
    v->zTableName=sqlite3_mprintf("%s",zMod+13);
  else if(argc>3) v->zTableName=sqlite3_mprintf("%s",argv[3]);
  else v->zTableName=sqlite3_mprintf("");

  rc = doltliteLoadUserTableColumns(db, v->zTableName, &v->cols, pzErr);
  if( rc!=SQLITE_OK ){
    sqlite3_free(v->zTableName);doltliteFreeColInfo(&v->cols);sqlite3_free(v);
    return rc;
  }
  zSchema=htBuildSchema(&v->cols);
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
  return htWalkHistory(c,v->db,v->zTableName);
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
    doltliteResultUserCol(ctx, &v->cols, r->pVal, r->nVal, r->intKey, col);
  }else{
    int fixedCol=col-nCols;
    switch(fixedCol){
      case 0:
        sqlite3_result_text(ctx,r->zCommit,-1,SQLITE_TRANSIENT);
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

int doltliteRegisterHistoryTables(sqlite3 *db){
  return doltliteForEachUserTable(db, "dolt_history_", &historyModule);
}

#endif
