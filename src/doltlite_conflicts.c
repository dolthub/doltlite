
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_record.h"
#include "doltlite_internal.h"
#include <string.h>

/* Historical note: an earlier version of this file built SQL text to
** apply conflict resolutions — INSERT OR REPLACE strings with literal
** values inlined per type (%lld, %Q, X'..', %!.15g), handed to
** sqlite3_exec. That round-tripped the stored record bytes through
** the parser and VDBE purely to land them in the prolly tree.
** Resolution now calls doltliteApplyRawRowMutation (prolly_btree.c)
** which writes the bytes directly into the target table's tree. No
** SQL engine involvement; triggers and FK checks correctly do NOT
** fire (they already ran on the original commits). */

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

/* Conflict catalog format:
**
**   Header: 4 magic bytes 'D' 'L' 'C' version
**     - version 1: rows carry (nKey, key, intKey, base, our, their)
**     - v0 (legacy, before the rename-conformity refactor): (intKey,
**       base, our, their) with no version header; that format only
**       worked for INTEGER PK tables and is no longer produced. It
**       is NOT read by loadAllConflicts — any on-disk conflict
**       catalog written by a pre-rename-conformity doltlite must be
**       cleared with dolt_merge('--abort') before upgrading.
**   Then: uint16 nTables
**   Per table: uint16 nameLen, name, uint32 nConflicts, rows[]
**   Per row:  uint32 nKey, keyBytes, int64 intKey,
**             uint32 nBase, baseVal, uint32 nOur, ourVal,
**             uint32 nTheir, theirVal
**
** The key bytes are needed so the --ours / --theirs resolve paths
** (which now write directly into the prolly tree via
** doltliteApplyRawRowMutation) can reach user-PK rows. INTEGER-PK
** tables still use intKey and store nKey=0.
*/
#define DOLTLITE_CONFLICTS_MAGIC0 'D'
#define DOLTLITE_CONFLICTS_MAGIC1 'L'
#define DOLTLITE_CONFLICTS_MAGIC2 'C'
#define DOLTLITE_CONFLICTS_VERSION 1

int doltliteSerializeConflicts(
  ChunkStore *cs,
  ConflictTableInfo *aTables, int nTables,
  ProllyHash *pHash
){
  int sz = 4 + 2;  /* magic+version header + nTables */
  int i, j, rc;
  u8 *buf, *p;

  for(i=0; i<nTables; i++){
    int nl = aTables[i].zName ? (int)strlen(aTables[i].zName) : 0;
    sz += 2 + nl + 4;
    for(j=0; j<aTables[i].nConflicts; j++){
      sz += 4 + aTables[i].aRows[j].nKey  /* nKey + key bytes */
              + 8                          /* intKey */
              + 4 + aTables[i].aRows[j].nBaseVal
              + 4 + aTables[i].aRows[j].nOurVal
              + 4 + aTables[i].aRows[j].nTheirVal;
    }
  }

  buf = sqlite3_malloc(sz);
  if( !buf ) return SQLITE_NOMEM;
  p = buf;

  p[0] = DOLTLITE_CONFLICTS_MAGIC0;
  p[1] = DOLTLITE_CONFLICTS_MAGIC1;
  p[2] = DOLTLITE_CONFLICTS_MAGIC2;
  p[3] = DOLTLITE_CONFLICTS_VERSION;
  p += 4;
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
      { int n=cr->nKey; p[0]=(u8)n; p[1]=(u8)(n>>8); p[2]=(u8)(n>>16); p[3]=(u8)(n>>24); p+=4; }
      if(cr->nKey>0){ memcpy(p, cr->pKey, cr->nKey); p+=cr->nKey; }
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
  if( nData<(4+2) ){ sqlite3_free(data); return SQLITE_CORRUPT; }

  p = data;
  /* Header: 'D' 'L' 'C' version. Any on-disk catalog without this
  ** header is from before the rename-conformity refactor and cannot
  ** be read — those only carried intKey and the user-PK resolve path
  ** would have no key bytes. */
  if( p[0]!=DOLTLITE_CONFLICTS_MAGIC0
   || p[1]!=DOLTLITE_CONFLICTS_MAGIC1
   || p[2]!=DOLTLITE_CONFLICTS_MAGIC2
   || p[3]!=DOLTLITE_CONFLICTS_VERSION ){
    sqlite3_free(data);
    return SQLITE_CORRUPT;
  }
  p += 4;
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
      int kvl, bvl, ovl, tvl;
      if( p+4 > data+nData ){ rc = SQLITE_CORRUPT; goto conflicts_cleanup; }
      kvl = p[0]|(p[1]<<8)|(p[2]<<16)|(p[3]<<24); p+=4;
      if( kvl<0 || p+kvl > data+nData ){ rc = SQLITE_CORRUPT; goto conflicts_cleanup; }
      if(kvl>0){
        cr->pKey = sqlite3_malloc(kvl);
        if( !cr->pKey ){ rc = SQLITE_NOMEM; goto conflicts_cleanup; }
        memcpy(cr->pKey, p, kvl);
        cr->nKey = kvl;
      }
      p += kvl;
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
    if( totalConflicts==0 ){
      doltliteSetSessionConflictsCatalog(db, &(ProllyHash){{0}});
    }else{
      ProllyHash newHash;
      int rc = doltliteSerializeConflicts(cs, aTables, nTables, &newHash);
      if( rc!=SQLITE_OK ) return rc;
      doltliteSetSessionConflictsCatalog(db, &newHash);
      doltliteSetSessionMergeState(db, 1, 0, &newHash);
    }
    return doltlitePersistWorkingSet(db);
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
  /* Column is named "table" to match Dolt; the quoting is required because
  ** "table" is a reserved keyword in SQL. Callers must quote it on access:
  ** SELECT "table" FROM dolt_conflicts. */
  rc = sqlite3_declare_vtab(db, "CREATE TABLE x(\"table\" TEXT, num_conflicts INTEGER)");
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
  DoltliteColInfo cols;
};

typedef struct CfRowCur CfRowCur;
struct CfRowCur {
  sqlite3_vtab_cursor base;
  ConflictTableInfo *aTables;
  int nTables;
  int iTableIdx;       
  int iRow;            
};

/* Build a dolt-conformant dolt_conflicts_<table> schema by projecting
** every user column of `t` under three prefixes (base_, our_, their_),
** plus the standard metadata columns Dolt emits: from_root_ish,
** our_diff_type, their_diff_type, dolt_conflict_id. */
static char *cfrBuildSchema(const DoltliteColInfo *ci){
  sqlite3_str *pStr = sqlite3_str_new(0);
  int i;
  char *z;
  if( !pStr ) return 0;
  sqlite3_str_appendall(pStr, "CREATE TABLE x(from_root_ish TEXT");
  /* base_* */
  for(i=0; i<ci->nCol; i++){
    sqlite3_str_appendf(pStr, ", \"base_%w\"", ci->azName[i]);
  }
  /* our_* + our_diff_type */
  for(i=0; i<ci->nCol; i++){
    sqlite3_str_appendf(pStr, ", \"our_%w\"", ci->azName[i]);
  }
  sqlite3_str_appendall(pStr, ", our_diff_type TEXT");
  /* their_* + their_diff_type */
  for(i=0; i<ci->nCol; i++){
    sqlite3_str_appendf(pStr, ", \"their_%w\"", ci->azName[i]);
  }
  sqlite3_str_appendall(pStr, ", their_diff_type TEXT");
  sqlite3_str_appendall(pStr, ", dolt_conflict_id TEXT");
  sqlite3_str_appendall(pStr, ")");
  z = sqlite3_str_finish(pStr);
  return z;
}

static int cfrConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  CfRowVtab *v;
  int rc;
  const char *zModuleName;
  char *zSchema;
  (void)pAux;

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
  if( !v->zTableName ){
    sqlite3_free(v);
    return SQLITE_NOMEM;
  }

  rc = doltliteLoadUserTableColumns(db, v->zTableName, &v->cols, pzErr);
  if( rc!=SQLITE_OK ){
    sqlite3_free(v->zTableName);
    doltliteFreeColInfo(&v->cols);
    sqlite3_free(v);
    return rc;
  }

  zSchema = cfrBuildSchema(&v->cols);
  if( !zSchema ){
    sqlite3_free(v->zTableName);
    doltliteFreeColInfo(&v->cols);
    sqlite3_free(v);
    return SQLITE_NOMEM;
  }
  rc = sqlite3_declare_vtab(db, zSchema);
  sqlite3_free(zSchema);
  if( rc!=SQLITE_OK ){
    sqlite3_free(v->zTableName);
    doltliteFreeColInfo(&v->cols);
    sqlite3_free(v);
    return rc;
  }

  *ppVtab = &v->base;
  return SQLITE_OK;
}

static int cfrDisconnect(sqlite3_vtab *pVtab){
  CfRowVtab *v = (CfRowVtab*)pVtab;
  sqlite3_free(v->zTableName);
  doltliteFreeColInfo(&v->cols);
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

/* Emit one projected column from a record body. If iPkCol>=0 and
** iUserCol==iPkCol, the PK value comes from intKey (rowid-aliased
** INTEGER PRIMARY KEY case) rather than the record body. */
static void cfrEmitRecordCol(
  sqlite3_context *ctx,
  const u8 *pRec, int nRec,
  int iUserCol,
  int iPkCol,
  i64 intKey
){
  if( !pRec || nRec<=0 ){
    sqlite3_result_null(ctx);
    return;
  }
  if( iUserCol==iPkCol ){
    sqlite3_result_int64(ctx, intKey);
    return;
  }
  {
    DoltliteRecordInfo ri;
    doltliteParseRecord(pRec, nRec, &ri);
    if( iUserCol >= ri.nField ){
      sqlite3_result_null(ctx);
      return;
    }
    doltliteResultField(ctx, pRec, nRec, ri.aType[iUserCol], ri.aOffset[iUserCol]);
  }
}

/* Classify a conflict row on one side as added/modified/removed
** relative to its base. Dolt uses "added" if the base is missing,
** "removed" if the side is missing, otherwise "modified". */
static const char *cfrDiffType(const u8 *pBase, int nBase,
                               const u8 *pSide, int nSide){
  int baseHas = (pBase && nBase>0);
  int sideHas = (pSide && nSide>0);
  if( !sideHas ) return "removed";
  if( !baseHas ) return "added";
  return "modified";
}

static int cfrColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int col){
  CfRowCur *c = (CfRowCur*)cur;
  CfRowVtab *v = (CfRowVtab*)cur->pVtab;
  struct ConflictRow *cr;
  int nUserCols;
  int colBaseStart, colOurStart, colOurDiff;
  int colTheirStart, colTheirDiff, colConflictId;

  if( c->iTableIdx < 0 ) return SQLITE_OK;
  if( c->iRow >= c->aTables[c->iTableIdx].nConflicts ) return SQLITE_OK;
  cr = &c->aTables[c->iTableIdx].aRows[c->iRow];

  /* Schema layout:
  **   col 0                     : from_root_ish
  **   col 1..nUserCols          : base_<col>
  **   col nUserCols+1..2*nUserCols : our_<col>
  **   col 2*nUserCols+1         : our_diff_type
  **   col 2*nUserCols+2..3*nUserCols+1 : their_<col>
  **   col 3*nUserCols+2         : their_diff_type
  **   col 3*nUserCols+3         : dolt_conflict_id
  */
  nUserCols = v->cols.nCol;
  colBaseStart  = 1;
  colOurStart   = 1 + nUserCols;
  colOurDiff    = 1 + 2*nUserCols;
  colTheirStart = 2 + 2*nUserCols;
  colTheirDiff  = 2 + 3*nUserCols;
  colConflictId = 3 + 3*nUserCols;

  if( col==0 ){
    /* from_root_ish: doltlite doesn't track this per-conflict. Emit
    ** NULL — oracle tests should not compare this column. */
    sqlite3_result_null(ctx);
  }else if( col>=colBaseStart && col<colOurStart ){
    cfrEmitRecordCol(ctx, cr->pBaseVal, cr->nBaseVal,
                     col - colBaseStart, v->cols.iPkCol, cr->intKey);
  }else if( col>=colOurStart && col<colOurDiff ){
    cfrEmitRecordCol(ctx, cr->pOurVal, cr->nOurVal,
                     col - colOurStart, v->cols.iPkCol, cr->intKey);
  }else if( col==colOurDiff ){
    sqlite3_result_text(ctx,
      cfrDiffType(cr->pBaseVal, cr->nBaseVal, cr->pOurVal, cr->nOurVal),
      -1, SQLITE_STATIC);
  }else if( col>=colTheirStart && col<colTheirDiff ){
    cfrEmitRecordCol(ctx, cr->pTheirVal, cr->nTheirVal,
                     col - colTheirStart, v->cols.iPkCol, cr->intKey);
  }else if( col==colTheirDiff ){
    sqlite3_result_text(ctx,
      cfrDiffType(cr->pBaseVal, cr->nBaseVal, cr->pTheirVal, cr->nTheirVal),
      -1, SQLITE_STATIC);
  }else if( col==colConflictId ){
    /* Stable synthetic id: intKey + iRow. Oracle tests should not
    ** compare this column since Dolt uses a different scheme. */
    char buf[64];
    sqlite3_snprintf(sizeof(buf), buf, "%lld:%d", cr->intKey, c->iRow);
    sqlite3_result_text(ctx, buf, -1, SQLITE_TRANSIENT);
  }else{
    sqlite3_result_null(ctx);
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

int doltliteRegisterConflictTables(sqlite3 *db){
  return doltliteForEachUserTable(db, "dolt_conflicts_", &cfRowModule);
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


      /* Write each conflict row's theirs bytes directly into the
      ** target table's prolly tree. pTheirVal==NULL means theirs is a
      ** delete — doltliteApplyRawRowMutation stages a tree delete at
      ** the PK when pVal is NULL. Keys: intKey for INTEGER-PK tables,
      ** pKey/nKey for user-PK (the merge path captured those into the
      ** conflict row when the conflict was recorded). */
      for(j=0; j<aTables[i].nConflicts; j++){
        struct ConflictRow *cr = &aTables[i].aRows[j];
        rc = doltliteApplyRawRowMutation(db, zTable,
                                         cr->pKey, cr->nKey, cr->intKey,
                                         cr->pTheirVal, cr->nTheirVal);
        if( rc!=SQLITE_OK ){
          freeConflictTables(aTables, nTables);
          sqlite3_result_error(ctx, "failed to apply theirs value", -1);
          return;
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
    rc = doltliteRegisterConflictTables(db);
  return rc;
}

#endif 
