
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_record.h"
#include "doltlite_internal.h"
#include <string.h>

static i64 cfReadInt(const u8 *pRec, int nBytes){
  i64 v;
  int i;
  assert( nBytes >= 1 && nBytes <= 8 );
  if( nBytes < 1 || nBytes > 8 ) return 0;
  v = (pRec[0] & 0x80) ? -1 : 0;
  for(i=0; i<nBytes; i++){
    v = (v << 8) | pRec[i];
  }
  return v;
}

static char *buildInsertSql(
  const char *zTable,
  i64 intKey,
  char **azCol, int nCol,
  const u8 *pRec, const u8 *pRecEnd,
  const u8 *pHdrEnd, const u8 *pBody
){
  char *zIns = sqlite3_mprintf("INSERT OR REPLACE INTO \"%w\"(rowid", zTable);
  char *zVals = sqlite3_mprintf("VALUES(%lld", intKey);
  char *zTmp;
  const u8 *pBodyPos = pBody;
  int colIdx = 0;

  if( !zIns || !zVals ){
    sqlite3_free(zIns); sqlite3_free(zVals);
    return 0;
  }

  while( pRec < pHdrEnd && pRec < pRecEnd && colIdx < nCol ){
    u64 st;
    int stBytes = dlReadVarint(pRec, pHdrEnd, &st);
    pRec += stBytes;

    
    if( !zIns || !zVals ){
      sqlite3_free(zIns); sqlite3_free(zVals);
      return 0;
    }

    
    zTmp = sqlite3_mprintf("%s,\"%w\"", zIns, azCol[colIdx]);
    sqlite3_free(zIns); zIns = zTmp;

    
    if( st==0 ){
      zTmp = sqlite3_mprintf("%s,NULL", zVals);
      sqlite3_free(zVals); zVals = zTmp;
    }else if( st==8 ){
      zTmp = sqlite3_mprintf("%s,0", zVals);
      sqlite3_free(zVals); zVals = zTmp;
    }else if( st==9 ){
      zTmp = sqlite3_mprintf("%s,1", zVals);
      sqlite3_free(zVals); zVals = zTmp;
    }else if( st>=1 && st<=6 ){
      static const int sizes[] = {0,1,2,3,4,6,8};
      int nBytes = sizes[st];
      if( pBodyPos + nBytes <= pRecEnd ){
        i64 v = cfReadInt(pBodyPos, nBytes);
        zTmp = sqlite3_mprintf("%s,%lld", zVals, v);
        sqlite3_free(zVals); zVals = zTmp;
      }
      pBodyPos += nBytes;
    }else if( st==7 ){
      
      if( pBodyPos + 8 <= pRecEnd ){
        double v;
        u64 bits = 0;
        int k;
        for(k=0; k<8; k++) bits = (bits<<8) | pBodyPos[k];
        memcpy(&v, &bits, 8);
        zTmp = sqlite3_mprintf("%s,%!.15g", zVals, v);
        sqlite3_free(zVals); zVals = zTmp;
      }
      pBodyPos += 8;
    }else if( st>=12 && (st&1)==0 ){
      
      int len = ((int)st - 12) / 2;
      if( pBodyPos + len <= pRecEnd ){
        zTmp = sqlite3_mprintf("%s,X'", zVals);
        sqlite3_free(zVals); zVals = zTmp;
        {
          int k;
          for(k=0; k<len; k++){
            zTmp = sqlite3_mprintf("%s%02x", zVals, pBodyPos[k]);
            sqlite3_free(zVals); zVals = zTmp;
          }
        }
        zTmp = sqlite3_mprintf("%s'", zVals);
        sqlite3_free(zVals); zVals = zTmp;
      }
      pBodyPos += len;
    }else if( st>=13 && (st&1)==1 ){
      
      int len = ((int)st - 13) / 2;
      if( pBodyPos + len <= pRecEnd ){
        
        char *zText = sqlite3_malloc(len+1);
        if( zText ){
          memcpy(zText, pBodyPos, len);
          zText[len] = 0;
          zTmp = sqlite3_mprintf("%s,%Q", zVals, zText);
          sqlite3_free(zVals); zVals = zTmp;
          sqlite3_free(zText);
        }
      }
      pBodyPos += len;
    }

    colIdx++;
  }

  
  zTmp = sqlite3_mprintf("%s) %s)", zIns, zVals);
  sqlite3_free(zIns);
  sqlite3_free(zVals);
  return zTmp;
}

static int applyTheirRecord(
  sqlite3 *db,
  const char *zTable,
  i64 intKey,
  const u8 *pRec,
  int nRec
){
  sqlite3_stmt *pInfo = 0;
  char *zSql;
  int rc, nCol = 0;
  const u8 *pPos, *pRecEnd, *pHdrEnd, *pBody;
  u64 hdrSize;
  int hdrBytes;

  
  char **azCol = 0;
  int nColAlloc = 0;

  zSql = sqlite3_mprintf("PRAGMA table_info(\"%w\")", zTable);
  if( !zSql ) return SQLITE_NOMEM;
  rc = sqlite3_prepare_v2(db, zSql, -1, &pInfo, 0);
  sqlite3_free(zSql);
  if( rc!=SQLITE_OK ) return rc;

  while( sqlite3_step(pInfo)==SQLITE_ROW ){
    const char *zName = (const char*)sqlite3_column_text(pInfo, 1);
    int pk = sqlite3_column_int(pInfo, 5);
    
    if( pk==1 ){
      const char *zType = (const char*)sqlite3_column_text(pInfo, 2);
      if( zType && sqlite3_stricmp(zType, "INTEGER")==0 ){
        continue; 
      }
    }
    if( nCol >= nColAlloc ){
      nColAlloc = nColAlloc ? nColAlloc*2 : 8;
      azCol = sqlite3_realloc(azCol, nColAlloc * (int)sizeof(char*));
      if( !azCol ){ sqlite3_finalize(pInfo); return SQLITE_NOMEM; }
    }
    azCol[nCol] = sqlite3_mprintf("%s", zName);
    nCol++;
  }
  sqlite3_finalize(pInfo);

  if( nCol==0 ){
    sqlite3_free(azCol);
    return SQLITE_ERROR;
  }

  
  pPos = pRec;
  pRecEnd = pRec + nRec;
  hdrBytes = dlReadVarint(pPos, pRecEnd, &hdrSize);
  pPos += hdrBytes;
  pHdrEnd = pRec + (int)hdrSize;
  pBody = pRec + (int)hdrSize;

  
  if( pPos < pHdrEnd ){
    u64 stSkip;
    int skipBytes = dlReadVarint(pPos, pHdrEnd, &stSkip);
    pPos += skipBytes;
    
    if( stSkip==0 || stSkip==8 || stSkip==9 ) {}
    else if( stSkip>=1 && stSkip<=6 ){ static const int s[]={0,1,2,3,4,6,8}; pBody+=s[stSkip]; }
    else if( stSkip==7 ) pBody+=8;
    else if( stSkip>=12 && (stSkip&1)==0 ) pBody+=((int)stSkip-12)/2;
    else if( stSkip>=13 && (stSkip&1)==1 ) pBody+=((int)stSkip-13)/2;
  }

  
  zSql = buildInsertSql(zTable, intKey, azCol, nCol, pPos, pRecEnd, pHdrEnd, pBody);
  if( zSql ){
    rc = sqlite3_exec(db, zSql, 0, 0, 0);
    sqlite3_free(zSql);
  }else{
    rc = SQLITE_NOMEM;
  }

  
  {
    int k;
    for(k=0; k<nCol; k++) sqlite3_free(azCol[k]);
    sqlite3_free(azCol);
  }

  return rc;
}

typedef struct ConflictTableInfo ConflictTableInfo;
struct ConflictTableInfo {
  char *zName;
  int nConflicts;
  struct ConflictRow {
    i64 intKey;
    u8 *pKey; int nKey;       
    u8 *pBaseVal; int nBaseVal;
    u8 *pOurVal; int nOurVal;
    u8 *pTheirVal; int nTheirVal;
  } *aRows;
};

static void freeConflictTables(ConflictTableInfo *aTables, int nTables);

static void freeConflictRow(struct ConflictRow *pRow){
  if( !pRow ) return;
  sqlite3_free(pRow->pKey);
  sqlite3_free(pRow->pBaseVal);
  sqlite3_free(pRow->pOurVal);
  sqlite3_free(pRow->pTheirVal);
  memset(pRow, 0, sizeof(*pRow));
}

static int dupConflictBytes(const u8 *pIn, int nIn, u8 **ppOut){
  u8 *pCopy;
  *ppOut = 0;
  if( !pIn || nIn<=0 ) return SQLITE_OK;
  pCopy = sqlite3_malloc(nIn);
  if( !pCopy ) return SQLITE_NOMEM;
  memcpy(pCopy, pIn, nIn);
  *ppOut = pCopy;
  return SQLITE_OK;
}

static void removeConflictRow(ConflictTableInfo *pTable, int iRow){
  if( !pTable || iRow<0 || iRow>=pTable->nConflicts ) return;
  freeConflictRow(&pTable->aRows[iRow]);
  if( iRow < pTable->nConflicts - 1 ){
    memmove(&pTable->aRows[iRow], &pTable->aRows[iRow+1],
            (pTable->nConflicts - iRow - 1) * sizeof(struct ConflictRow));
  }
  pTable->nConflicts--;
  if( pTable->aRows ){
    memset(&pTable->aRows[pTable->nConflicts], 0, sizeof(struct ConflictRow));
  }
}

static void removeConflictTable(ConflictTableInfo *aTables, int *pnTables, int iTable){
  int nTables;
  if( !aTables || !pnTables ) return;
  nTables = *pnTables;
  if( iTable<0 || iTable>=nTables ) return;
  sqlite3_free(aTables[iTable].zName);
  {
    int j;
    for(j=0; j<aTables[iTable].nConflicts; j++){
      freeConflictRow(&aTables[iTable].aRows[j]);
    }
  }
  sqlite3_free(aTables[iTable].aRows);
  memset(&aTables[iTable], 0, sizeof(aTables[iTable]));
  if( iTable < nTables - 1 ){
    memmove(&aTables[iTable], &aTables[iTable+1],
            (nTables - iTable - 1) * sizeof(ConflictTableInfo));
  }
  (*pnTables)--;
  memset(&aTables[*pnTables], 0, sizeof(aTables[*pnTables]));
}

int doltliteSerializeConflicts(
  ChunkStore *cs,
  ConflictTableInfo *aTables, int nTables,
  ProllyHash *pHash
){
  int sz = 2;  
  int i, j, rc;
  u8 *buf, *p;

  for(i=0; i<nTables; i++){
    int nl = aTables[i].zName ? (int)strlen(aTables[i].zName) : 0;
    sz += 2 + nl + 4;
    for(j=0; j<aTables[i].nConflicts; j++){
      sz += 8 + 4 + aTables[i].aRows[j].nBaseVal
              + 4 + aTables[i].aRows[j].nOurVal
              + 4 + aTables[i].aRows[j].nTheirVal;
    }
  }

  buf = sqlite3_malloc(sz);
  if( !buf ) return SQLITE_NOMEM;
  p = buf;

  p[0]=(u8)nTables; p[1]=(u8)(nTables>>8); p+=2;
  for(i=0; i<nTables; i++){
    int nl = aTables[i].zName ? (int)strlen(aTables[i].zName) : 0;
    int nc = aTables[i].nConflicts;
    p[0]=(u8)nl; p[1]=(u8)(nl>>8); p+=2;
    if(nl>0) memcpy(p, aTables[i].zName, nl);
    p += nl;
    p[0]=(u8)nc; p[1]=(u8)(nc>>8); p[2]=(u8)(nc>>16); p[3]=(u8)(nc>>24); p+=4;
    for(j=0; j<nc; j++){
      struct ConflictRow *cr = &aTables[i].aRows[j];
      i64 k = cr->intKey;
      p[0]=(u8)k; p[1]=(u8)(k>>8); p[2]=(u8)(k>>16); p[3]=(u8)(k>>24);
      p[4]=(u8)(k>>32); p[5]=(u8)(k>>40); p[6]=(u8)(k>>48); p[7]=(u8)(k>>56);
      p+=8;
      { int n=cr->nBaseVal; p[0]=(u8)n; p[1]=(u8)(n>>8); p[2]=(u8)(n>>16); p[3]=(u8)(n>>24); p+=4; }
      if(cr->nBaseVal>0){ memcpy(p, cr->pBaseVal, cr->nBaseVal); p+=cr->nBaseVal; }
      { int n=cr->nOurVal; p[0]=(u8)n; p[1]=(u8)(n>>8); p[2]=(u8)(n>>16); p[3]=(u8)(n>>24); p+=4; }
      if(cr->nOurVal>0){ memcpy(p, cr->pOurVal, cr->nOurVal); p+=cr->nOurVal; }
      { int n=cr->nTheirVal; p[0]=(u8)n; p[1]=(u8)(n>>8); p[2]=(u8)(n>>16); p[3]=(u8)(n>>24); p+=4; }
      if(cr->nTheirVal>0){ memcpy(p, cr->pTheirVal, cr->nTheirVal); p+=cr->nTheirVal; }
    }
  }

  rc = chunkStorePut(cs, buf, (int)(p-buf), pHash);
  sqlite3_free(buf);
  return rc;
}

static int loadAllConflicts(
  sqlite3 *db,
  ChunkStore *cs,
  ConflictTableInfo **ppTables, int *pnTables
){
  ProllyHash hash;
  u8 *data = 0; int nData = 0;
  extern void doltliteGetSessionConflictsCatalog(sqlite3*, ProllyHash*);
  const u8 *p;
  int nTables, i, j, rc;
  ConflictTableInfo *aTables;

  doltliteGetSessionConflictsCatalog(db, &hash);
  if( prollyHashIsEmpty(&hash) ){ *ppTables = 0; *pnTables = 0; return SQLITE_OK; }

  rc = chunkStoreGet(cs, &hash, &data, &nData);
  if( rc!=SQLITE_OK ) return rc;
  if( nData<2 ){ sqlite3_free(data); return SQLITE_CORRUPT; }

  p = data;
  nTables = p[0]|(p[1]<<8); p+=2;

  aTables = sqlite3_malloc(nTables * (int)sizeof(ConflictTableInfo));
  if( !aTables ){ sqlite3_free(data); return SQLITE_NOMEM; }
  memset(aTables, 0, nTables * (int)sizeof(ConflictTableInfo));

  for(i=0; i<nTables; i++){
    int nl, nc;
    if( p+2 > data+nData ){ rc = SQLITE_CORRUPT; goto conflicts_cleanup; }
    nl = p[0]|(p[1]<<8); p+=2;
    if( nl<0 || p+nl > data+nData ){ rc = SQLITE_CORRUPT; goto conflicts_cleanup; }
    aTables[i].zName = sqlite3_malloc(nl+1);
    if( !aTables[i].zName ){ rc = SQLITE_NOMEM; goto conflicts_cleanup; }
    memcpy(aTables[i].zName, p, nl); aTables[i].zName[nl]=0;
    p += nl;
    if( p+4 > data+nData ){ rc = SQLITE_CORRUPT; goto conflicts_cleanup; }
    nc = p[0]|(p[1]<<8)|(p[2]<<16)|(p[3]<<24); p+=4;
    if( nc<0 ){ rc = SQLITE_CORRUPT; goto conflicts_cleanup; }
    aTables[i].nConflicts = nc;
    aTables[i].aRows = sqlite3_malloc(nc * (int)sizeof(struct ConflictRow));
    if( !aTables[i].aRows ){ rc = SQLITE_NOMEM; goto conflicts_cleanup; }
    memset(aTables[i].aRows, 0, nc * (int)sizeof(struct ConflictRow));

    for(j=0; j<nc; j++){
      struct ConflictRow *cr = &aTables[i].aRows[j];
      int bvl, ovl, tvl;
      if( p+8 > data+nData ){ rc = SQLITE_CORRUPT; goto conflicts_cleanup; }
      cr->intKey = (i64)((u64)p[0] | ((u64)p[1]<<8) | ((u64)p[2]<<16) | ((u64)p[3]<<24) |
                         ((u64)p[4]<<32) | ((u64)p[5]<<40) | ((u64)p[6]<<48) | ((u64)p[7]<<56));
      p+=8;
      if( p+4 > data+nData ){ rc = SQLITE_CORRUPT; goto conflicts_cleanup; }
      bvl = p[0]|(p[1]<<8)|(p[2]<<16)|(p[3]<<24); p+=4;
      if( bvl<0 || p+bvl > data+nData ){ rc = SQLITE_CORRUPT; goto conflicts_cleanup; }
      if(bvl>0){
        cr->pBaseVal = sqlite3_malloc(bvl);
        if( !cr->pBaseVal ){ rc = SQLITE_NOMEM; goto conflicts_cleanup; }
        memcpy(cr->pBaseVal, p, bvl);
        cr->nBaseVal = bvl;
      }
      p += bvl;
      if( p+4 > data+nData ){ rc = SQLITE_CORRUPT; goto conflicts_cleanup; }
      ovl = p[0]|(p[1]<<8)|(p[2]<<16)|(p[3]<<24); p+=4;
      if( ovl<0 || p+ovl > data+nData ){ rc = SQLITE_CORRUPT; goto conflicts_cleanup; }
      if(ovl>0){
        cr->pOurVal = sqlite3_malloc(ovl);
        if( !cr->pOurVal ){ rc = SQLITE_NOMEM; goto conflicts_cleanup; }
        memcpy(cr->pOurVal, p, ovl);
        cr->nOurVal = ovl;
      }
      p += ovl;
      if( p+4 > data+nData ){ rc = SQLITE_CORRUPT; goto conflicts_cleanup; }
      tvl = p[0]|(p[1]<<8)|(p[2]<<16)|(p[3]<<24); p+=4;
      if( tvl<0 || p+tvl > data+nData ){ rc = SQLITE_CORRUPT; goto conflicts_cleanup; }
      if(tvl>0){
        cr->pTheirVal = sqlite3_malloc(tvl);
        if( !cr->pTheirVal ){ rc = SQLITE_NOMEM; goto conflicts_cleanup; }
        memcpy(cr->pTheirVal, p, tvl);
        cr->nTheirVal = tvl;
      }
      p += tvl;
    }
  }

  if( p!=data+nData ){ rc = SQLITE_CORRUPT; goto conflicts_cleanup; }

  *ppTables = aTables;
  *pnTables = nTables;
  sqlite3_free(data);
  return SQLITE_OK;

conflicts_cleanup:
  freeConflictTables(aTables, nTables);
  sqlite3_free(data);
  return rc;
}

static void freeConflictTables(ConflictTableInfo *aTables, int nTables){
  int i, j;
  for(i=0; i<nTables; i++){
    for(j=0; j<aTables[i].nConflicts; j++){
      freeConflictRow(&aTables[i].aRows[j]);
    }
    sqlite3_free(aTables[i].aRows);
    sqlite3_free(aTables[i].zName);
  }
  sqlite3_free(aTables);
}

static int storeUpdatedConflicts(
  sqlite3 *db,
  ChunkStore *cs,
  ConflictTableInfo *aTables, int nTables
){
  int totalConflicts = 0;
  int i;
  for(i=0; i<nTables; i++) totalConflicts += aTables[i].nConflicts;

  {
    int rc;
    extern void doltliteSetSessionConflictsCatalog(sqlite3*, const ProllyHash*);
    extern void doltliteSetSessionMergeState(sqlite3*, u8, const ProllyHash*, const ProllyHash*);
    extern int doltliteSaveWorkingSet(sqlite3*);
    if( totalConflicts==0 ){
      doltliteSetSessionConflictsCatalog(db, &(ProllyHash){{0}});
    }else{
      ProllyHash newHash;
      int rc = doltliteSerializeConflicts(cs, aTables, nTables, &newHash);
      if( rc!=SQLITE_OK ) return rc;
      doltliteSetSessionConflictsCatalog(db, &newHash);
      doltliteSetSessionMergeState(db, 1, 0, &newHash);
    }
    rc = doltliteSaveWorkingSet(db);
    if( rc!=SQLITE_OK ) return rc;
    rc = chunkStoreSerializeRefs(cs);
    if( rc!=SQLITE_OK ) return rc;
    return chunkStoreCommit(cs);
  }
}

typedef struct ConflictsVtab ConflictsVtab;
struct ConflictsVtab { sqlite3_vtab base; sqlite3 *db; };
typedef struct ConflictsCur ConflictsCur;
struct ConflictsCur {
  sqlite3_vtab_cursor base;
  ConflictTableInfo *aTables; int nTables; int iRow;
};

static int cfConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  ConflictsVtab *v; int rc;
  (void)pAux;(void)argc;(void)argv;(void)pzErr;
  rc = sqlite3_declare_vtab(db, "CREATE TABLE x(table_name TEXT, num_conflicts INTEGER)");
  if(rc!=SQLITE_OK) return rc;
  v = sqlite3_malloc(sizeof(*v)); if(!v) return SQLITE_NOMEM;
  memset(v,0,sizeof(*v)); v->db=db; *ppVtab=&v->base; return SQLITE_OK;
}
static int cfDisconnect(sqlite3_vtab *v){ sqlite3_free(v); return SQLITE_OK; }
static int cfOpen(sqlite3_vtab *v, sqlite3_vtab_cursor **pp){
  ConflictsCur *c=sqlite3_malloc(sizeof(*c)); (void)v;
  if(!c) return SQLITE_NOMEM; memset(c,0,sizeof(*c)); *pp=&c->base; return SQLITE_OK;
}
static int cfClose(sqlite3_vtab_cursor *cur){
  ConflictsCur *c=(ConflictsCur*)cur;
  freeConflictTables(c->aTables, c->nTables);
  sqlite3_free(c); return SQLITE_OK;
}
static int cfFilter(sqlite3_vtab_cursor *cur, int n, const char *s, int a, sqlite3_value **v){
  ConflictsCur *c=(ConflictsCur*)cur;
  ConflictsVtab *vt=(ConflictsVtab*)cur->pVtab;
  (void)n;(void)s;(void)a;(void)v;
  c->iRow=0;
  return loadAllConflicts(vt->db, doltliteGetChunkStore(vt->db), &c->aTables, &c->nTables);
}
static int cfNext(sqlite3_vtab_cursor *cur){ ((ConflictsCur*)cur)->iRow++; return SQLITE_OK; }
static int cfEof(sqlite3_vtab_cursor *cur){ ConflictsCur *c=(ConflictsCur*)cur; return c->iRow>=c->nTables; }
static int cfColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int col){
  ConflictsCur *c=(ConflictsCur*)cur;
  switch(col){
    case 0: sqlite3_result_text(ctx, c->aTables[c->iRow].zName, -1, SQLITE_TRANSIENT); break;
    case 1: sqlite3_result_int(ctx, c->aTables[c->iRow].nConflicts); break;
  }
  return SQLITE_OK;
}
static int cfRowid(sqlite3_vtab_cursor *cur, sqlite3_int64 *r){ *r=((ConflictsCur*)cur)->iRow; return SQLITE_OK; }
static int cfBestIndex(sqlite3_vtab *v, sqlite3_index_info *p){ (void)v; p->estimatedCost=10; return SQLITE_OK; }

static sqlite3_module conflictsModule = {
  0,0,cfConnect,cfBestIndex,cfDisconnect,0,cfOpen,cfClose,cfFilter,cfNext,cfEof,
  cfColumn,cfRowid,0,0,0,0,0,0,0,0,0,0,0,0
};

typedef struct CfRowVtab CfRowVtab;
struct CfRowVtab {
  sqlite3_vtab base;
  sqlite3 *db;
  char *zTableName;    
};

typedef struct CfRowCur CfRowCur;
struct CfRowCur {
  sqlite3_vtab_cursor base;
  ConflictTableInfo *aTables;
  int nTables;
  int iTableIdx;       
  int iRow;            
};

static int cfrConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  CfRowVtab *v; int rc;
  const char *zModuleName;
  (void)pAux;(void)pzErr;

  rc = sqlite3_declare_vtab(db,
    "CREATE TABLE x(base_rowid INTEGER, base_value BLOB,"
    " our_rowid INTEGER, our_value BLOB,"
    " their_rowid INTEGER, their_value BLOB)");
  if(rc!=SQLITE_OK) return rc;

  v = sqlite3_malloc(sizeof(*v));
  if(!v) return SQLITE_NOMEM;
  memset(v,0,sizeof(*v));
  v->db = db;

  
  zModuleName = argv[0]; 
  if( zModuleName && strncmp(zModuleName, "dolt_conflicts_", 15)==0 ){
    v->zTableName = sqlite3_mprintf("%s", zModuleName + 15);
  }else if( argc > 3 ){
    
    v->zTableName = sqlite3_mprintf("%s", argv[3]);
  }else{
    v->zTableName = sqlite3_mprintf("");
  }

  *ppVtab = &v->base;
  return SQLITE_OK;
}

static int cfrDisconnect(sqlite3_vtab *pVtab){
  CfRowVtab *v = (CfRowVtab*)pVtab;
  sqlite3_free(v->zTableName);
  sqlite3_free(v);
  return SQLITE_OK;
}

static int cfrOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **pp){
  CfRowCur *c = sqlite3_malloc(sizeof(*c));
  (void)pVtab;
  if(!c) return SQLITE_NOMEM;
  memset(c,0,sizeof(*c));
  c->iTableIdx = -1;
  *pp = &c->base;
  return SQLITE_OK;
}

static int cfrClose(sqlite3_vtab_cursor *cur){
  CfRowCur *c = (CfRowCur*)cur;
  freeConflictTables(c->aTables, c->nTables);
  sqlite3_free(c);
  return SQLITE_OK;
}

static int cfrFilter(sqlite3_vtab_cursor *cur, int n, const char *s, int a, sqlite3_value **v){
  CfRowCur *c = (CfRowCur*)cur;
  CfRowVtab *vt = (CfRowVtab*)cur->pVtab;
  int i, rc;
  (void)n;(void)s;(void)a;(void)v;

  c->iRow = 0;
  c->iTableIdx = -1;
  rc = loadAllConflicts(vt->db, doltliteGetChunkStore(vt->db), &c->aTables, &c->nTables);
  if( rc!=SQLITE_OK ) return rc;

  
  for(i=0; i<c->nTables; i++){
    if( c->aTables[i].zName && strcmp(c->aTables[i].zName, vt->zTableName)==0 ){
      c->iTableIdx = i;
      break;
    }
  }
  return SQLITE_OK;
}

static int cfrNext(sqlite3_vtab_cursor *cur){
  ((CfRowCur*)cur)->iRow++;
  return SQLITE_OK;
}

static int cfrEof(sqlite3_vtab_cursor *cur){
  CfRowCur *c = (CfRowCur*)cur;
  if( c->iTableIdx < 0 ) return 1;
  return c->iRow >= c->aTables[c->iTableIdx].nConflicts;
}

static int cfrColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int col){
  CfRowCur *c = (CfRowCur*)cur;
  struct ConflictRow *cr;
  if( c->iTableIdx < 0 ) return SQLITE_OK;
  if( c->iRow >= c->aTables[c->iTableIdx].nConflicts ) return SQLITE_OK;
  cr = &c->aTables[c->iTableIdx].aRows[c->iRow];

  switch(col){
    case 0: 
      sqlite3_result_int64(ctx, cr->intKey);
      break;
    case 1: 
      doltliteResultRecord(ctx, cr->pBaseVal, cr->nBaseVal);
      break;
    case 2: 
      sqlite3_result_int64(ctx, cr->intKey);
      break;
    case 3: 
      doltliteResultRecord(ctx, cr->pOurVal, cr->nOurVal);
      break;
    case 4: 
      sqlite3_result_int64(ctx, cr->intKey);
      break;
    case 5: 
      doltliteResultRecord(ctx, cr->pTheirVal, cr->nTheirVal);
      break;
  }
  return SQLITE_OK;
}

static int cfrRowid(sqlite3_vtab_cursor *cur, sqlite3_int64 *r){
  CfRowCur *c = (CfRowCur*)cur;
  if( c->iTableIdx >= 0 && c->iRow < c->aTables[c->iTableIdx].nConflicts ){
    *r = c->aTables[c->iTableIdx].aRows[c->iRow].intKey;
  }else{
    *r = 0;
  }
  return SQLITE_OK;
}

static int cfrBestIndex(sqlite3_vtab *v, sqlite3_index_info *p){
  (void)v;
  p->estimatedCost = 10;
  return SQLITE_OK;
}

static int cfrUpdate(
  sqlite3_vtab *pVtab,
  int nArg,
  sqlite3_value **apArg,
  sqlite3_int64 *pRowid
){
  CfRowVtab *v = (CfRowVtab*)pVtab;
  ChunkStore *cs = doltliteGetChunkStore(v->db);
  ConflictTableInfo *aTables = 0;
  int nTables = 0;
  int i, j, rc;
  i64 deleteRowid;

  (void)pRowid;

  
  if( nArg != 1 ){
    pVtab->zErrMsg = sqlite3_mprintf("only DELETE is supported on conflict tables");
    return SQLITE_ERROR;
  }

  deleteRowid = sqlite3_value_int64(apArg[0]);

  
  rc = loadAllConflicts(v->db, cs, &aTables, &nTables);
  if( rc!=SQLITE_OK ) return rc;

  
  for(i=0; i<nTables; i++){
    if( !aTables[i].zName || strcmp(aTables[i].zName, v->zTableName)!=0 )
      continue;

    for(j=0; j<aTables[i].nConflicts; j++){
      if( aTables[i].aRows[j].intKey == deleteRowid ){
        removeConflictRow(&aTables[i], j);

        
        if( aTables[i].nConflicts == 0 ){
          removeConflictTable(aTables, &nTables, i);
        }

        
        rc = storeUpdatedConflicts(v->db, cs, aTables, nTables);
        freeConflictTables(aTables, nTables);
        return rc;
      }
    }
    break;
  }

  freeConflictTables(aTables, nTables);
  return SQLITE_OK; 
}

static sqlite3_module cfRowModule = {
  0,                   
  cfrConnect,          
  cfrConnect,          
  cfrBestIndex,        
  cfrDisconnect,       
  cfrDisconnect,       
  cfrOpen,             
  cfrClose,            
  cfrFilter,           
  cfrNext,             
  cfrEof,              
  cfrColumn,           
  cfrRowid,            
  cfrUpdate,           
  0,0,0,0,0,0,0,0,0,0,0  
};

void doltliteRegisterConflictTables(sqlite3 *db){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ConflictTableInfo *aTables = 0;
  int nTables = 0;
  int i;

  if( !cs ) return;
  loadAllConflicts(db, cs, &aTables, &nTables);

  for(i=0; i<nTables; i++){
    if( aTables[i].zName ){
      char *zModuleName = sqlite3_mprintf("dolt_conflicts_%s", aTables[i].zName);
      if( zModuleName ){
        
        sqlite3_create_module(db, zModuleName, &cfRowModule, 0);
        sqlite3_free(zModuleName);
      }
    }
  }
  freeConflictTables(aTables, nTables);
}

static void conflictsResolveFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  const char *zMode, *zTable;
  ConflictTableInfo *aTables = 0;
  int nTables = 0;
  int i, j, rc;

  if(!cs){ sqlite3_result_error(ctx,"no database",-1); return; }
  if(argc<2){ sqlite3_result_error(ctx,"usage: dolt_conflicts_resolve('--ours'|'--theirs','table')",-1); return; }

  zMode = (const char*)sqlite3_value_text(argv[0]);
  zTable = (const char*)sqlite3_value_text(argv[1]);
  if(!zMode||!zTable){ sqlite3_result_error(ctx,"invalid args",-1); return; }

  rc = loadAllConflicts(db, cs, &aTables, &nTables);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(ctx, rc);
    return;
  }

  if( strcmp(zMode,"--ours")==0 ){
    
    for(i=0; i<nTables; i++){
      if( aTables[i].zName && strcmp(aTables[i].zName, zTable)==0 ){
        removeConflictTable(aTables, &nTables, i);
        break;
      }
    }
    rc = storeUpdatedConflicts(db, cs, aTables, nTables);
    freeConflictTables(aTables, nTables);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(ctx, rc);
      return;
    }
    sqlite3_result_int(ctx, 0);

  }else if( strcmp(zMode,"--theirs")==0 ){
    
    for(i=0; i<nTables; i++){
      if( !aTables[i].zName || strcmp(aTables[i].zName, zTable)!=0 ) continue;

      
      for(j=0; j<aTables[i].nConflicts; j++){
        struct ConflictRow *cr = &aTables[i].aRows[j];
        if( cr->pTheirVal && cr->nTheirVal > 0 ){
          
          rc = applyTheirRecord(db, zTable, cr->intKey,
                                cr->pTheirVal, cr->nTheirVal);
          if( rc!=SQLITE_OK ){
            freeConflictTables(aTables, nTables);
            sqlite3_result_error(ctx, "failed to apply theirs value", -1);
            return;
          }
        }else{
          
          char *zSql = sqlite3_mprintf("DELETE FROM \"%w\" WHERE rowid=%lld",
                                        zTable, cr->intKey);
          if( !zSql ){
            freeConflictTables(aTables, nTables);
            sqlite3_result_error_code(ctx, SQLITE_NOMEM);
            return;
          }
          rc = sqlite3_exec(db, zSql, 0, 0, 0);
          sqlite3_free(zSql);
          if( rc!=SQLITE_OK ){
            freeConflictTables(aTables, nTables);
            sqlite3_result_error(ctx, "failed to apply theirs value", -1);
            return;
          }
        }
      }

      removeConflictTable(aTables, &nTables, i);
      break;
    }
    rc = storeUpdatedConflicts(db, cs, aTables, nTables);
    freeConflictTables(aTables, nTables);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(ctx, rc);
      return;
    }
    sqlite3_result_int(ctx, 0);

  }else{
    freeConflictTables(aTables, nTables);
    sqlite3_result_error(ctx, "use --ours or --theirs", -1);
  }
}

int doltliteConflictsRegister(sqlite3 *db){
  int rc;
  rc = sqlite3_create_module(db, "dolt_conflicts", &conflictsModule, 0);
  if( rc==SQLITE_OK )
    rc = sqlite3_create_function(db, "dolt_conflicts_resolve", -1, SQLITE_UTF8, 0,
                                  conflictsResolveFunc, 0, 0);
  
  if( rc==SQLITE_OK )
    doltliteRegisterConflictTables(db);
  return rc;
}

#endif 
