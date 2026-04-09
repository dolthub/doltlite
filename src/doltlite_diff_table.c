
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_diff.h"
#include "prolly_cache.h"
#include "chunk_store.h"
#include "doltlite_commit.h"
#include "doltlite_record.h"
#include "doltlite_internal.h"

#include <assert.h>
#include <string.h>
#include <time.h>



static char *buildDiffSchema(DoltliteColInfo *ci){
  
  int i;
  int sz = 256;
  char *z;

  for(i=0; i<ci->nCol; i++) sz += 2 * ((int)strlen(ci->azName[i]) + 20);

  z = sqlite3_malloc(sz);
  if( !z ) return 0;

  {
    char *p = z;
    char *end = z + sz;

    p += snprintf(p, end-p, "CREATE TABLE x(");

    
    for(i=0; i<ci->nCol; i++){
      if( i > 0 ) p += snprintf(p, end-p, ", ");
      p += snprintf(p, end-p, "\"from_%s\"", ci->azName[i]);
    }

    
    for(i=0; i<ci->nCol; i++){
      p += snprintf(p, end-p, ", \"to_%s\"", ci->azName[i]);
    }

    p += snprintf(p, end-p, ", from_commit TEXT, to_commit TEXT"
              ", from_commit_date TEXT, to_commit_date TEXT"
              ", diff_type TEXT)");
    assert( p < end );
  }

  return z;
}

typedef struct AuditRow AuditRow;
struct AuditRow {
  u8 diffType;
  i64 intKey;
  u8 *pOldVal; int nOldVal;
  u8 *pNewVal; int nNewVal;
  char zFromCommit[PROLLY_HASH_SIZE*2+1];
  char zToCommit[PROLLY_HASH_SIZE*2+1];
  i64 fromDate;
  i64 toDate;
};

typedef struct DiffTblVtab DiffTblVtab;
struct DiffTblVtab {
  sqlite3_vtab base;
  sqlite3 *db;
  char *zTableName;
  DoltliteColInfo cols;       
};

typedef struct DiffTblCursor DiffTblCursor;
struct DiffTblCursor {
  sqlite3_vtab_cursor base;
  AuditRow *aRows;
  int nRows;
  int nAlloc;
  int iRow;
};

typedef struct CollectCtx CollectCtx;
struct CollectCtx {
  DiffTblCursor *pCur;
  char *zFromCommit;
  char *zToCommit;
  i64 fromDate;
  i64 toDate;
};

static int auditDiffCollect(void *pCtx, const ProllyDiffChange *pChange){
  CollectCtx *c = (CollectCtx*)pCtx;
  DiffTblCursor *pCur = c->pCur;
  AuditRow *r;

  if( pCur->nRows >= pCur->nAlloc ){
    int nNew = pCur->nAlloc ? pCur->nAlloc*2 : 64;
    AuditRow *aNew = sqlite3_realloc(pCur->aRows, nNew*(int)sizeof(AuditRow));
    if( !aNew ) return SQLITE_NOMEM;
    pCur->aRows = aNew;
    pCur->nAlloc = nNew;
  }

  r = &pCur->aRows[pCur->nRows];
  memset(r, 0, sizeof(*r));
  r->diffType = pChange->type;
  r->intKey = pChange->intKey;

  if( pChange->pOldVal && pChange->nOldVal>0 ){
    r->pOldVal = sqlite3_malloc(pChange->nOldVal);
    if( r->pOldVal ) memcpy(r->pOldVal, pChange->pOldVal, pChange->nOldVal);
    r->nOldVal = pChange->nOldVal;
  }
  if( pChange->pNewVal && pChange->nNewVal>0 ){
    r->pNewVal = sqlite3_malloc(pChange->nNewVal);
    if( r->pNewVal ) memcpy(r->pNewVal, pChange->pNewVal, pChange->nNewVal);
    r->nNewVal = pChange->nNewVal;
  }

  memcpy(r->zFromCommit, c->zFromCommit, PROLLY_HASH_SIZE*2+1);
  memcpy(r->zToCommit, c->zToCommit, PROLLY_HASH_SIZE*2+1);
  r->fromDate = c->fromDate;
  r->toDate = c->toDate;

  pCur->nRows++;
  return SQLITE_OK;
}

static void freeAuditRows(DiffTblCursor *pCur){
  int i;
  for(i=0; i<pCur->nRows; i++){
    sqlite3_free(pCur->aRows[i].pOldVal);
    sqlite3_free(pCur->aRows[i].pNewVal);
  }
  sqlite3_free(pCur->aRows);
  pCur->aRows = 0;
  pCur->nRows = 0;
  pCur->nAlloc = 0;
}


static int walkHistoryAndDiff(
  DiffTblCursor *pCur, sqlite3 *db, const char *zTableName
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  void *pBt = doltliteGetBtShared(db);
  ProllyCache *pCache;
  ProllyHash curHash;
  int rc;

  if( !cs || !pBt ) return SQLITE_OK;
  pCache = doltliteGetCache(db);

  doltliteGetSessionHead(db, &curHash);
  if( prollyHashIsEmpty(&curHash) ) return SQLITE_OK;

  while( !prollyHashIsEmpty(&curHash) ){
    u8 *data = 0; int nData = 0;
    DoltliteCommit commit;
    ProllyHash curRoot, parentRoot;
    u8 flags = 0;
    char curHex[PROLLY_HASH_SIZE*2+1];
    char parentHex[PROLLY_HASH_SIZE*2+1];

    memset(&commit, 0, sizeof(commit));
    rc = chunkStoreGet(cs, &curHash, &data, &nData);
    if( rc!=SQLITE_OK ) break;
    rc = doltliteCommitDeserialize(data, nData, &commit);
    sqlite3_free(data);
    if( rc!=SQLITE_OK ) break;

    doltliteHashToHex(&curHash, curHex);

    {
      struct TableEntry *aTables = 0; int nTables = 0;
      rc = doltliteLoadCatalog(db, &commit.catalogHash, &aTables, &nTables, 0);
      if( rc==SQLITE_OK ){
        doltliteFindTableRootByName(aTables, nTables, zTableName, &curRoot, &flags);
        sqlite3_free(aTables);
      }else{
        memset(&curRoot, 0, sizeof(curRoot));
      }
    }

    if( !prollyHashIsEmpty(&commit.parentHash) ){
      DoltliteCommit parentCommit;
      u8 *pdata = 0; int npdata = 0;

      memset(&parentCommit, 0, sizeof(parentCommit));
      rc = chunkStoreGet(cs, &commit.parentHash, &pdata, &npdata);
      if( rc==SQLITE_OK ){
        rc = doltliteCommitDeserialize(pdata, npdata, &parentCommit);
        sqlite3_free(pdata);
      }

      if( rc==SQLITE_OK ){
        doltliteHashToHex(&commit.parentHash, parentHex);

        {
          struct TableEntry *aPT = 0; int nPT = 0;
          rc = doltliteLoadCatalog(db, &parentCommit.catalogHash, &aPT, &nPT, 0);
          if( rc==SQLITE_OK ){
            doltliteFindTableRootByName(aPT, nPT, zTableName, &parentRoot, 0);
            sqlite3_free(aPT);
          }else{
            memset(&parentRoot, 0, sizeof(parentRoot));
          }
        }

        if( prollyHashCompare(&parentRoot, &curRoot)!=0 ){
          CollectCtx ctx;
          ctx.pCur = pCur;
          ctx.zFromCommit = parentHex;
          ctx.zToCommit = curHex;
          ctx.fromDate = parentCommit.timestamp;
          ctx.toDate = commit.timestamp;
          prollyDiff(cs, pCache, &parentRoot, &curRoot, flags,
                     auditDiffCollect, &ctx);
        }

        doltliteCommitClear(&parentCommit);
      }
    }else{
      if( !prollyHashIsEmpty(&curRoot) ){
        ProllyHash emptyRoot;
        CollectCtx ctx;
        memset(&emptyRoot, 0, sizeof(emptyRoot));
        memset(parentHex, '0', PROLLY_HASH_SIZE*2);
        parentHex[PROLLY_HASH_SIZE*2] = 0;


        ctx.pCur = pCur;
        ctx.zFromCommit = parentHex;
        ctx.zToCommit = curHex;
        ctx.fromDate = 0;
        ctx.toDate = commit.timestamp;
        prollyDiff(cs, pCache, &emptyRoot, &curRoot, flags,
                   auditDiffCollect, &ctx);
      }
    }

    {
      ProllyHash nextHash;
      memcpy(&nextHash, &commit.parentHash, sizeof(ProllyHash));
      doltliteCommitClear(&commit);
      memcpy(&curHash, &nextHash, sizeof(ProllyHash));
    }
  }

  return SQLITE_OK;
}

static int dtConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  DiffTblVtab *pVtab;
  int rc;
  const char *zModName;
  char *zSchema;
  (void)pAux; (void)pzErr;

  pVtab = sqlite3_malloc(sizeof(*pVtab));
  if( !pVtab ) return SQLITE_NOMEM;
  memset(pVtab, 0, sizeof(*pVtab));
  pVtab->db = db;

  
  zModName = argv[0];
  if( zModName && strncmp(zModName, "dolt_diff_", 10)==0 ){
    pVtab->zTableName = sqlite3_mprintf("%s", zModName + 10);
  }else if( argc > 3 ){
    pVtab->zTableName = sqlite3_mprintf("%s", argv[3]);
  }else{
    pVtab->zTableName = sqlite3_mprintf("");
  }

  
  doltliteGetColumnNames(db, pVtab->zTableName, &pVtab->cols);

  
  if( pVtab->cols.nCol <= 0 ){
    sqlite3_free(pVtab->zTableName);
    doltliteFreeColInfo(&pVtab->cols);
    sqlite3_free(pVtab);
    *pzErr = sqlite3_mprintf("table '%s' not found or has no columns", argv[3] ? argv[3] : "");
    return SQLITE_ERROR;
  }
  zSchema = buildDiffSchema(&pVtab->cols);

  if( !zSchema ){
    sqlite3_free(pVtab->zTableName);
    doltliteFreeColInfo(&pVtab->cols);
    sqlite3_free(pVtab);
    return SQLITE_NOMEM;
  }

  rc = sqlite3_declare_vtab(db, zSchema);
  sqlite3_free(zSchema);
  if( rc!=SQLITE_OK ){
    sqlite3_free(pVtab->zTableName);
    doltliteFreeColInfo(&pVtab->cols);
    sqlite3_free(pVtab);
    return rc;
  }

  *ppVtab = &pVtab->base;
  return SQLITE_OK;
}

static int dtDisconnect(sqlite3_vtab *pBase){
  DiffTblVtab *pVtab = (DiffTblVtab*)pBase;
  sqlite3_free(pVtab->zTableName);
  doltliteFreeColInfo(&pVtab->cols);
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

static int dtBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  (void)pVtab;
  pInfo->estimatedCost = 10000.0;
  return SQLITE_OK;
}

static int dtOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **pp){
  DiffTblCursor *c; (void)pVtab;
  c = sqlite3_malloc(sizeof(*c));
  if( !c ) return SQLITE_NOMEM;
  memset(c, 0, sizeof(*c));
  *pp = &c->base;
  return SQLITE_OK;
}

static int dtClose(sqlite3_vtab_cursor *cur){
  DiffTblCursor *c = (DiffTblCursor*)cur;
  freeAuditRows(c);
  sqlite3_free(c);
  return SQLITE_OK;
}

static int dtFilter(sqlite3_vtab_cursor *cur,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  DiffTblCursor *c = (DiffTblCursor*)cur;
  DiffTblVtab *pVtab = (DiffTblVtab*)cur->pVtab;
  (void)idxNum; (void)idxStr; (void)argc; (void)argv;
  freeAuditRows(c);
  c->iRow = 0;
  walkHistoryAndDiff(c, pVtab->db, pVtab->zTableName);
  return SQLITE_OK;
}

static int dtNext(sqlite3_vtab_cursor *cur){
  ((DiffTblCursor*)cur)->iRow++;
  return SQLITE_OK;
}

static int dtEof(sqlite3_vtab_cursor *cur){
  DiffTblCursor *c = (DiffTblCursor*)cur;
  return c->iRow >= c->nRows;
}

static int dtColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int col){
  DiffTblCursor *c = (DiffTblCursor*)cur;
  DiffTblVtab *pVtab = (DiffTblVtab*)cur->pVtab;
  AuditRow *r = &c->aRows[c->iRow];
  int nCols = pVtab->cols.nCol;

  

  if( nCols > 0 && col < nCols ){
    
    int colIdx = col;
    if( colIdx == pVtab->cols.iPkCol ){
      
      if( r->pOldVal && r->nOldVal > 0 ){
        sqlite3_result_int64(ctx, r->intKey);
      }else{
        sqlite3_result_null(ctx);
      }
    }else{
      
      if( r->pOldVal && r->nOldVal > 0 ){
        DoltliteRecordInfo ri;
        doltliteParseRecord(r->pOldVal, r->nOldVal, &ri);
        if( colIdx < ri.nField ){
          doltliteResultField(ctx, r->pOldVal, r->nOldVal,
                        ri.aType[colIdx], ri.aOffset[colIdx]);
        }else{
          sqlite3_result_null(ctx);
        }
      }else{
        sqlite3_result_null(ctx);
      }
    }
  }else if( nCols > 0 && col < 2*nCols ){
    
    int colIdx = col - nCols;
    if( colIdx == pVtab->cols.iPkCol ){
      if( r->pNewVal && r->nNewVal > 0 ){
        sqlite3_result_int64(ctx, r->intKey);
      }else{
        sqlite3_result_null(ctx);
      }
    }else{
      if( r->pNewVal && r->nNewVal > 0 ){
        DoltliteRecordInfo ri;
        doltliteParseRecord(r->pNewVal, r->nNewVal, &ri);
        if( colIdx < ri.nField ){
          doltliteResultField(ctx, r->pNewVal, r->nNewVal,
                        ri.aType[colIdx], ri.aOffset[colIdx]);
        }else{
          sqlite3_result_null(ctx);
        }
      }else{
        sqlite3_result_null(ctx);
      }
    }
  }else{
    
    int fixedCol = col - 2*nCols;

    switch( fixedCol ){
      case 0:
        sqlite3_result_text(ctx, r->zFromCommit, -1, SQLITE_TRANSIENT);
        break;
      case 1:
        sqlite3_result_text(ctx, r->zToCommit, -1, SQLITE_TRANSIENT);
        break;
      case 2: 
        { time_t t = (time_t)r->fromDate; struct tm *tm = gmtime(&t);
          if(tm){ char b[32]; strftime(b,sizeof(b),"%Y-%m-%d %H:%M:%S",tm);
            sqlite3_result_text(ctx,b,-1,SQLITE_TRANSIENT);
          }else sqlite3_result_null(ctx); }
        break;
      case 3: 
        { time_t t = (time_t)r->toDate; struct tm *tm = gmtime(&t);
          if(tm){ char b[32]; strftime(b,sizeof(b),"%Y-%m-%d %H:%M:%S",tm);
            sqlite3_result_text(ctx,b,-1,SQLITE_TRANSIENT);
          }else sqlite3_result_null(ctx); }
        break;
      case 4: 
        switch( r->diffType ){
          case PROLLY_DIFF_ADD:    sqlite3_result_text(ctx,"added",-1,SQLITE_STATIC); break;
          case PROLLY_DIFF_DELETE: sqlite3_result_text(ctx,"removed",-1,SQLITE_STATIC); break;
          case PROLLY_DIFF_MODIFY: sqlite3_result_text(ctx,"modified",-1,SQLITE_STATIC); break;
        }
        break;
    }
  }

  return SQLITE_OK;
}

static int dtRowid(sqlite3_vtab_cursor *cur, sqlite3_int64 *r){
  *r = ((DiffTblCursor*)cur)->iRow;
  return SQLITE_OK;
}

static sqlite3_module diffTableModule = {
  0, dtConnect, dtConnect, dtBestIndex, dtDisconnect, dtDisconnect,
  dtOpen, dtClose, dtFilter, dtNext, dtEof, dtColumn, dtRowid,
  0,0,0,0,0,0,0,0,0,0,0,0
};

void doltliteRegisterDiffTables(sqlite3 *db){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHash headCommit;
  u8 *data = 0; int nData = 0;
  DoltliteCommit commit;
  struct TableEntry *aTables = 0;
  int nTables = 0, i, rc;

  if( !cs ) return;
  doltliteGetSessionHead(db, &headCommit);
  if( prollyHashIsEmpty(&headCommit) ) return;

  rc = chunkStoreGet(cs, &headCommit, &data, &nData);
  if( rc!=SQLITE_OK ) return;

  memset(&commit, 0, sizeof(commit));
  rc = doltliteCommitDeserialize(data, nData, &commit);
  sqlite3_free(data);
  if( rc!=SQLITE_OK ) return;

  rc = doltliteLoadCatalog(db, &commit.catalogHash, &aTables, &nTables, 0);
  doltliteCommitClear(&commit);
  if( rc!=SQLITE_OK ) return;

  for(i=0; i<nTables; i++){
    if( aTables[i].zName && aTables[i].iTable > 1 ){
      char *zModName = sqlite3_mprintf("dolt_diff_%s", aTables[i].zName);
      if( zModName ){
        sqlite3_create_module(db, zModName, &diffTableModule, 0);
        sqlite3_free(zModName);
      }
    }
  }
  sqlite3_free(aTables);
}

#endif 
