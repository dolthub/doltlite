
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
  ProllyHash *aQueue;
  int qHead, qTail, qAlloc;
  ProllyHashSet visited, queued;
  int visitedInit, queuedInit;
  char zCommitHex[PROLLY_HASH_SIZE*2+1];
  char *zCommitter;
  i64 commitDate;
  ProllyCursor tblCur;
  int tblCurOpen;
  i64 intKey;
  u8 *pVal; int nVal;
  int hasRow;
  i64 iRowid;
};

static void htCursorReset(HistCursor *c){
  if( c->tblCurOpen ){
    prollyCursorClose(&c->tblCur);
    c->tblCurOpen = 0;
  }
  sqlite3_free(c->pVal);
  c->pVal = 0; c->nVal = 0;
  sqlite3_free(c->zCommitter);
  c->zCommitter = 0;
  sqlite3_free(c->aQueue);
  c->aQueue = 0;
  c->qHead = c->qTail = c->qAlloc = 0;
  if( c->visitedInit ){
    prollyHashSetFree(&c->visited);
    c->visitedInit = 0;
  }
  if( c->queuedInit ){
    prollyHashSetFree(&c->queued);
    c->queuedInit = 0;
  }
  c->hasRow = 0;
  c->iRowid = 0;
}

static int htCaptureRow(HistCursor *c){
  const u8 *pVal; int nVal;
  sqlite3_free(c->pVal);
  c->pVal = 0; c->nVal = 0;
  c->intKey = prollyCursorIntKey(&c->tblCur);
  prollyCursorValue(&c->tblCur, &pVal, &nVal);
  if( pVal && nVal>0 ){
    c->pVal = sqlite3_malloc(nVal);
    if( !c->pVal ) return SQLITE_NOMEM;
    memcpy(c->pVal, pVal, nVal);
    c->nVal = nVal;
  }
  c->hasRow = 1;
  return SQLITE_OK;
}

static int htOpenTableAtCommit(HistCursor *c, sqlite3 *db,
    const char *zTableName, const ProllyHash *pCommitHash){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  DoltliteCommit commit;
  struct TableEntry *aT = 0; int nT = 0;
  ProllyHash tableRoot; u8 flags = 0;
  int rc, res;

  memset(&commit, 0, sizeof(commit));
  rc = doltliteLoadCommit(db, pCommitHash, &commit);
  if( rc!=SQLITE_OK ) return rc;

  doltliteHashToHex(pCommitHash, c->zCommitHex);
  sqlite3_free(c->zCommitter);
  c->zCommitter = sqlite3_mprintf("%s", commit.zName ? commit.zName : "");
  c->commitDate = commit.timestamp;
  if( !c->zCommitter ){
    doltliteCommitClear(&commit);
    return SQLITE_NOMEM;
  }

  {
    int i;
    for(i=0; i<doltliteCommitParentCount(&commit); i++){
      const ProllyHash *pParent = doltliteCommitParentHash(&commit, i);
      if( !pParent || prollyHashIsEmpty(pParent) ) continue;
      if( prollyHashSetContains(&c->visited, pParent) ) continue;
      if( prollyHashSetContains(&c->queued, pParent) ) continue;
      if( c->qTail >= c->qAlloc ){
        int nNew = c->qAlloc ? c->qAlloc*2 : 16;
        ProllyHash *tmp = sqlite3_realloc(c->aQueue,
                                           nNew*(int)sizeof(ProllyHash));
        if( !tmp ){
          doltliteCommitClear(&commit);
          return SQLITE_NOMEM;
        }
        c->aQueue = tmp; c->qAlloc = nNew;
      }
      c->aQueue[c->qTail++] = *pParent;
      rc = prollyHashSetAdd(&c->queued, pParent);
      if( rc!=SQLITE_OK ){
        doltliteCommitClear(&commit);
        return rc;
      }
    }
  }

  rc = doltliteLoadCatalog(db, &commit.catalogHash, &aT, &nT, 0);
  doltliteCommitClear(&commit);
  if( rc!=SQLITE_OK ) return rc;

  if( doltliteFindTableRootByName(aT, nT, zTableName, &tableRoot, &flags)
      !=SQLITE_OK || prollyHashIsEmpty(&tableRoot) ){
    doltliteFreeCatalog(aT, nT);
    return SQLITE_OK;
  }
  doltliteFreeCatalog(aT, nT);

  prollyCursorInit(&c->tblCur, cs, pCache, &tableRoot, flags);
  rc = prollyCursorFirst(&c->tblCur, &res);
  if( rc!=SQLITE_OK ){
    prollyCursorClose(&c->tblCur);
    return rc;
  }
  if( res ){
    prollyCursorClose(&c->tblCur);
    return SQLITE_OK;
  }
  c->tblCurOpen = 1;
  return SQLITE_OK;
}

static int htAdvance(HistCursor *c, sqlite3 *db, const char *zTableName){
  int rc;

  if( c->tblCurOpen ){
    rc = prollyCursorNext(&c->tblCur);
    if( rc!=SQLITE_OK ){
      prollyCursorClose(&c->tblCur);
      c->tblCurOpen = 0;
      return rc;
    }
    if( prollyCursorIsValid(&c->tblCur) ){
      return htCaptureRow(c);
    }
    prollyCursorClose(&c->tblCur);
    c->tblCurOpen = 0;
  }

  while( c->qHead < c->qTail ){
    ProllyHash cur = c->aQueue[c->qHead++];

    if( prollyHashSetContains(&c->visited, &cur) ) continue;
    rc = prollyHashSetAdd(&c->visited, &cur);
    if( rc!=SQLITE_OK ) return rc;

    rc = htOpenTableAtCommit(c, db, zTableName, &cur);
    if( rc!=SQLITE_OK ) return rc;

    if( c->tblCurOpen ){
      return htCaptureRow(c);
    }
  }

  c->hasRow = 0;
  return SQLITE_OK;
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
  htCursorReset(c); sqlite3_free(c); return SQLITE_OK;
}

static int htFilter(sqlite3_vtab_cursor *cur,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  HistCursor *c=(HistCursor*)cur;
  HistVtab *v=(HistVtab*)cur->pVtab;
  ProllyHash head;
  ChunkStore *cs;
  int rc;
  (void)idxNum;(void)idxStr;(void)argc;(void)argv;

  htCursorReset(c);

  cs = doltliteGetChunkStore(v->db);
  if( !cs ) return SQLITE_OK;

  doltliteGetSessionHead(v->db, &head);
  if( prollyHashIsEmpty(&head) ) return SQLITE_OK;

  rc = prollyHashSetInit(&c->visited, 64);
  if( rc!=SQLITE_OK ) return rc;
  c->visitedInit = 1;

  rc = prollyHashSetInit(&c->queued, 64);
  if( rc!=SQLITE_OK ) return rc;
  c->queuedInit = 1;

  c->qAlloc = 16;
  c->aQueue = sqlite3_malloc(c->qAlloc * (int)sizeof(ProllyHash));
  if( !c->aQueue ) return SQLITE_NOMEM;
  c->aQueue[0] = head;
  c->qTail = 1;
  rc = prollyHashSetAdd(&c->queued, &head);
  if( rc!=SQLITE_OK ) return rc;

  return htAdvance(c, v->db, v->zTableName);
}

static int htNext(sqlite3_vtab_cursor *cur){
  HistCursor *c=(HistCursor*)cur;
  HistVtab *v=(HistVtab*)cur->pVtab;
  c->iRowid++;
  return htAdvance(c, v->db, v->zTableName);
}

static int htEof(sqlite3_vtab_cursor *cur){
  return !((HistCursor*)cur)->hasRow;
}

static int htColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int col){
  HistCursor *c=(HistCursor*)cur;
  HistVtab *v=(HistVtab*)cur->pVtab;
  int nCols;
  if( !c->hasRow ) return SQLITE_OK;
  nCols=v->cols.nCol;

  if(nCols>0 && col<nCols){
    doltliteResultUserCol(ctx, &v->cols, c->pVal, c->nVal, c->intKey, col);
  }else{
    int fixedCol=col-nCols;
    switch(fixedCol){
      case 0:
        sqlite3_result_text(ctx,c->zCommitHex,-1,SQLITE_TRANSIENT);
        break;
      case 1:
        sqlite3_result_text(ctx,c->zCommitter,-1,SQLITE_TRANSIENT);
        break;
      case 2:
        {time_t t=(time_t)c->commitDate;struct tm *tm=gmtime(&t);
          if(tm){char b[32];strftime(b,sizeof(b),"%Y-%m-%d %H:%M:%S",tm);
            sqlite3_result_text(ctx,b,-1,SQLITE_TRANSIENT);
          }else sqlite3_result_null(ctx);}
        break;
    }
  }
  return SQLITE_OK;
}

static int htRowid(sqlite3_vtab_cursor *cur, sqlite3_int64 *r){
  *r=((HistCursor*)cur)->iRowid; return SQLITE_OK;
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
