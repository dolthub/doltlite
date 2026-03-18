/*
** dolt_at_<tablename>(commit_ref) — point-in-time table query with real columns.
**
** Returns the table as it existed at a specific commit, with the actual
** table column names (not generic rowid_val/value):
**
**   SELECT * FROM dolt_at_users('v1.0');
**   -- id | name | email
**
**   SELECT * FROM dolt_at_users('main');
**   SELECT * FROM dolt_at_users('abc123...');
**
** This is the SQLite equivalent of Dolt's AS OF clause.
** One eponymous virtual table is registered per user table.
*/
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_cursor.h"
#include "prolly_cache.h"
#include "chunk_store.h"
#include "doltlite_commit.h"

#include <string.h>
#include <time.h>

extern ChunkStore *doltliteGetChunkStore(sqlite3 *db);
extern void *doltliteGetBtShared(sqlite3 *db);

struct TableEntry { Pgno iTable; ProllyHash root; ProllyHash schemaHash; u8 flags; char *zName; void *pPending; };
extern int doltliteLoadCatalog(sqlite3 *db, const ProllyHash *catHash,
                               struct TableEntry **ppTables, int *pnTables,
                               Pgno *piNextTable);

/* --------------------------------------------------------------------------
** Varint reader
** -------------------------------------------------------------------------- */

static int atReadVarint(const u8 *p, const u8 *pEnd, u64 *pVal){
  u64 v=0; int i;
  for(i=0;i<9&&p+i<pEnd;i++){
    if(i<8){v=(v<<7)|(p[i]&0x7f);if(!(p[i]&0x80)){*pVal=v;return i+1;}}
    else{v=(v<<8)|p[i];*pVal=v;return 9;}
  }
  *pVal=v; return i?i:1;
}

/* --------------------------------------------------------------------------
** Record field parser and result setter (same approach as dolt_history)
** -------------------------------------------------------------------------- */

#define AT_MAX_COLS 64

typedef struct AtRecInfo AtRecInfo;
struct AtRecInfo { int nField; int aType[AT_MAX_COLS]; int aOffset[AT_MAX_COLS]; };

static void atParseRecord(const u8 *pData, int nData, AtRecInfo *ri){
  const u8 *p=pData, *pEnd=pData+nData;
  u64 hdrSize; int hdrBytes, off;
  memset(ri,0,sizeof(*ri));
  if(!pData||nData<1) return;
  hdrBytes=atReadVarint(p,pEnd,&hdrSize); p+=hdrBytes;
  off=(int)hdrSize;
  while(p<pData+hdrSize && p<pEnd && ri->nField<AT_MAX_COLS){
    u64 st; int stBytes=atReadVarint(p,pData+hdrSize,&st); p+=stBytes;
    ri->aType[ri->nField]=(int)st; ri->aOffset[ri->nField]=off;
    if(st==0){}else if(st==1)off+=1;else if(st==2)off+=2;else if(st==3)off+=3;
    else if(st==4)off+=4;else if(st==5)off+=6;else if(st==6)off+=8;else if(st==7)off+=8;
    else if(st==8||st==9){}else if(st>=12&&(st&1)==0)off+=((int)st-12)/2;
    else if(st>=13&&(st&1)==1)off+=((int)st-13)/2;
    ri->nField++;
  }
}

static void atResultField(sqlite3_context *ctx, const u8 *pData, int nData, int st, int off){
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

/* --------------------------------------------------------------------------
** Column info
** -------------------------------------------------------------------------- */

typedef struct AtColInfo AtColInfo;
struct AtColInfo {
  char **azName;
  int nCol;
  int iPkCol;
};

static void atFreeColInfo(AtColInfo *ci){
  int i; for(i=0;i<ci->nCol;i++) sqlite3_free(ci->azName[i]);
  sqlite3_free(ci->azName); ci->azName=0; ci->nCol=0;
}

static int atGetColumnNames(sqlite3 *db, const char *zTable, AtColInfo *ci){
  char *zSql; sqlite3_stmt *pStmt=0; int rc, nCol;
  memset(ci,0,sizeof(*ci)); ci->iPkCol=-1;
  zSql=sqlite3_mprintf("PRAGMA table_info(\"%w\")",zTable);
  if(!zSql) return SQLITE_NOMEM;
  rc=sqlite3_prepare_v2(db,zSql,-1,&pStmt,0); sqlite3_free(zSql);
  if(rc!=SQLITE_OK) return rc;
  nCol=0; while(sqlite3_step(pStmt)==SQLITE_ROW) nCol++;
  sqlite3_reset(pStmt);
  ci->azName=sqlite3_malloc(nCol*(int)sizeof(char*));
  if(!ci->azName){sqlite3_finalize(pStmt);return SQLITE_NOMEM;}
  memset(ci->azName,0,nCol*(int)sizeof(char*)); ci->nCol=0;
  while(sqlite3_step(pStmt)==SQLITE_ROW){
    const char *zName=(const char*)sqlite3_column_text(pStmt,1);
    int pk=sqlite3_column_int(pStmt,5);
    const char *zType=(const char*)sqlite3_column_text(pStmt,2);
    if(pk==1&&zType&&sqlite3_stricmp(zType,"INTEGER")==0) ci->iPkCol=ci->nCol;
    ci->azName[ci->nCol]=sqlite3_mprintf("%s",zName?zName:"");
    ci->nCol++;
  }
  sqlite3_finalize(pStmt); return SQLITE_OK;
}

static char *atBuildSchema(AtColInfo *ci){
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

/* --------------------------------------------------------------------------
** Buffered row
** -------------------------------------------------------------------------- */

typedef struct AtRow AtRow;
struct AtRow {
  i64 intKey;
  u8 *pVal; int nVal;
};

/* --------------------------------------------------------------------------
** Virtual table structures
** -------------------------------------------------------------------------- */

typedef struct AtVtab AtVtab;
struct AtVtab {
  sqlite3_vtab base;
  sqlite3 *db;
  char *zTableName;
  AtColInfo cols;
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

/* --------------------------------------------------------------------------
** Resolve ref and scan table
** -------------------------------------------------------------------------- */

static int atResolveRef(ChunkStore *cs, const char *zRef, ProllyHash *pCommit){
  int rc;
  if(zRef&&strlen(zRef)==40){
    rc=doltliteHexToHash(zRef,pCommit);
    if(rc==SQLITE_OK&&chunkStoreHas(cs,pCommit)) return SQLITE_OK;
  }
  rc=chunkStoreFindBranch(cs,zRef,pCommit);
  if(rc==SQLITE_OK&&!prollyHashIsEmpty(pCommit)) return SQLITE_OK;
  rc=chunkStoreFindTag(cs,zRef,pCommit);
  if(rc==SQLITE_OK&&!prollyHashIsEmpty(pCommit)) return SQLITE_OK;
  return SQLITE_NOTFOUND;
}

static int atFindRoot(struct TableEntry *a, int n, const char *zName,
                      ProllyHash *pRoot, u8 *pFlags){
  int i;
  for(i=0;i<n;i++){
    if(a[i].zName&&strcmp(a[i].zName,zName)==0){
      memcpy(pRoot,&a[i].root,sizeof(ProllyHash));
      if(pFlags)*pFlags=a[i].flags; return SQLITE_OK;
    }
  }
  memset(pRoot,0,sizeof(ProllyHash)); if(pFlags)*pFlags=0;
  return SQLITE_NOTFOUND;
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

/* --------------------------------------------------------------------------
** Virtual table methods
** -------------------------------------------------------------------------- */

static int atConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  AtVtab *v; int rc; const char *zMod; char *zSchema;
  (void)pAux;(void)pzErr;

  v=sqlite3_malloc(sizeof(*v)); if(!v) return SQLITE_NOMEM;
  memset(v,0,sizeof(*v)); v->db=db;

  zMod=argv[0];
  if(zMod&&strncmp(zMod,"dolt_at_",8)==0)
    v->zTableName=sqlite3_mprintf("%s",zMod+8);
  else if(argc>3) v->zTableName=sqlite3_mprintf("%s",argv[3]);
  else v->zTableName=sqlite3_mprintf("");

  atGetColumnNames(db,v->zTableName,&v->cols);

  if(v->cols.nCol>0){
    zSchema=atBuildSchema(&v->cols);
  }else{
    zSchema=sqlite3_mprintf("CREATE TABLE x(rowid_val INTEGER, value TEXT, commit_ref TEXT HIDDEN)");
  }
  if(!zSchema){sqlite3_free(v->zTableName);atFreeColInfo(&v->cols);sqlite3_free(v);return SQLITE_NOMEM;}

  rc=sqlite3_declare_vtab(db,zSchema); sqlite3_free(zSchema);
  if(rc!=SQLITE_OK){sqlite3_free(v->zTableName);atFreeColInfo(&v->cols);sqlite3_free(v);return rc;}

  *ppVtab=&v->base; return SQLITE_OK;
}

static int atDisconnect(sqlite3_vtab *pVtab){
  AtVtab *v=(AtVtab*)pVtab;
  sqlite3_free(v->zTableName); atFreeColInfo(&v->cols);
  sqlite3_free(v); return SQLITE_OK;
}

static int atBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  AtVtab *v=(AtVtab*)pVtab;
  int nCols=v->cols.nCol;
  int iRef=-1, i, argvIdx=1;
  /* The commit_ref is the last column (HIDDEN) */
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
  pCache=(ProllyCache*)(((char*)pBt)+sizeof(ChunkStore));

  zRef=(const char*)sqlite3_value_text(argv[0]);
  if(!zRef) return SQLITE_OK;

  rc=atResolveRef(cs,zRef,&commitHash);
  if(rc!=SQLITE_OK) return SQLITE_OK;

  memset(&commit,0,sizeof(commit));
  rc=chunkStoreGet(cs,&commitHash,&data,&nData);
  if(rc!=SQLITE_OK) return SQLITE_OK;
  rc=doltliteCommitDeserialize(data,nData,&commit);
  sqlite3_free(data);
  if(rc!=SQLITE_OK) return SQLITE_OK;

  rc=doltliteLoadCatalog(db,&commit.catalogHash,&aTables,&nTables,0);
  doltliteCommitClear(&commit);
  if(rc!=SQLITE_OK) return SQLITE_OK;

  rc=atFindRoot(aTables,nTables,v->zTableName,&tableRoot,&flags);
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
    /* Table column */
    if(col==v->cols.iPkCol){
      sqlite3_result_int64(ctx,r->intKey);
    }else{
      if(r->pVal&&r->nVal>0){
        AtRecInfo ri; atParseRecord(r->pVal,r->nVal,&ri);
        if(col<ri.nField) atResultField(ctx,r->pVal,r->nVal,ri.aType[col],ri.aOffset[col]);
        else sqlite3_result_null(ctx);
      }else sqlite3_result_null(ctx);
    }
  }
  /* commit_ref is HIDDEN — not returned by SELECT * */
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

/* --------------------------------------------------------------------------
** Registration: register dolt_at_<tablename> for each user table.
** -------------------------------------------------------------------------- */

void doltliteRegisterAtTables(sqlite3 *db){
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
      char *zMod=sqlite3_mprintf("dolt_at_%s",aT[i].zName);
      if(zMod){sqlite3_create_module(db,zMod,&atModule,0);sqlite3_free(zMod);}
    }
  }
  sqlite3_free(aT);
}

int doltliteAtRegister(sqlite3 *db){
  /* Also register the generic dolt_at TVF for backwards compatibility */
  /* (kept as a separate simpler module — not reimplemented here) */
  doltliteRegisterAtTables(db);
  return SQLITE_OK;
}

#endif /* DOLTLITE_PROLLY */
