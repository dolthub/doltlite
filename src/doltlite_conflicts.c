
#ifdef DOLTLITE_PROLLY

#include "doltlite_vtab_util.h"
#include "doltlite_internal.h"
#include <string.h>

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

static void removeConflictRow(ConflictTableInfo *pTable, int iRow){
  if( !pTable || iRow<0 || iRow>=pTable->nConflicts ) return;
  freeConflictRow(&pTable->aRows[iRow]);
  doltliteArrayRemoveAt(pTable->aRows, &pTable->nConflicts, iRow,
                        (int)sizeof(struct ConflictRow));
}

static void removeConflictTable(ConflictTableInfo *aTables, int *pnTables, int iTable){
  int j;
  if( !aTables || !pnTables || iTable<0 || iTable>=*pnTables ) return;
  sqlite3_free(aTables[iTable].zName);
  for(j=0; j<aTables[iTable].nConflicts; j++){
    freeConflictRow(&aTables[iTable].aRows[j]);
  }
  sqlite3_free(aTables[iTable].aRows);
  doltliteArrayRemoveAt(aTables, pnTables, iTable, (int)sizeof(ConflictTableInfo));
}

#define DOLTLITE_CONFLICTS_MAGIC0 'D'
#define DOLTLITE_CONFLICTS_MAGIC1 'L'
#define DOLTLITE_CONFLICTS_MAGIC2 'C'
#define DOLTLITE_CONFLICTS_VERSION 1

int doltliteSerializeConflicts(
  ChunkStore *cs,
  ConflictTableInfo *aTables, int nTables,
  ProllyHash *pHash
){
  int sz = 4 + 2;
  int i, j, rc;
  u8 *buf;
  DlByteWriter w;

  for(i=0; i<nTables; i++){
    int nl = aTables[i].zName ? (int)strlen(aTables[i].zName) : 0;
    sz += 2 + nl + 4;
    for(j=0; j<aTables[i].nConflicts; j++){
      sz += 4 + aTables[i].aRows[j].nKey
              + 8
              + 4 + aTables[i].aRows[j].nBaseVal
              + 4 + aTables[i].aRows[j].nOurVal
              + 4 + aTables[i].aRows[j].nTheirVal;
    }
  }

  buf = sqlite3_malloc(sz);
  if( !buf ) return SQLITE_NOMEM;
  w.p = buf;

  dlWriteU8(&w, DOLTLITE_CONFLICTS_MAGIC0);
  dlWriteU8(&w, DOLTLITE_CONFLICTS_MAGIC1);
  dlWriteU8(&w, DOLTLITE_CONFLICTS_MAGIC2);
  dlWriteU8(&w, DOLTLITE_CONFLICTS_VERSION);
  dlWriteU16(&w, nTables);
  for(i=0; i<nTables; i++){
    int nl = aTables[i].zName ? (int)strlen(aTables[i].zName) : 0;
    dlWriteU16Name(&w, aTables[i].zName, nl);
    dlWriteU32(&w, aTables[i].nConflicts);
    for(j=0; j<aTables[i].nConflicts; j++){
      struct ConflictRow *cr = &aTables[i].aRows[j];
      dlWriteU32Blob(&w, cr->pKey, cr->nKey);
      dlWriteI64(&w, cr->intKey);
      dlWriteU32Blob(&w, cr->pBaseVal, cr->nBaseVal);
      dlWriteU32Blob(&w, cr->pOurVal, cr->nOurVal);
      dlWriteU32Blob(&w, cr->pTheirVal, cr->nTheirVal);
    }
  }

  rc = chunkStorePut(cs, buf, (int)(w.p-buf), pHash);
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
  DlByteReader r;
  int nTables, i, j, rc;
  ConflictTableInfo *aTables;

  doltliteGetSessionConflictsCatalog(db, &hash);
  if( prollyHashIsEmpty(&hash) ){ *ppTables = 0; *pnTables = 0; return SQLITE_OK; }

  rc = chunkStoreGet(cs, &hash, &data, &nData);
  if( rc!=SQLITE_OK ) return rc;
  if( nData<(4+2) ){ sqlite3_free(data); return SQLITE_CORRUPT; }

  dlReaderInit(&r, data, nData);
  if( dlReadU8(&r)!=DOLTLITE_CONFLICTS_MAGIC0
   || dlReadU8(&r)!=DOLTLITE_CONFLICTS_MAGIC1
   || dlReadU8(&r)!=DOLTLITE_CONFLICTS_MAGIC2
   || dlReadU8(&r)!=DOLTLITE_CONFLICTS_VERSION ){
    sqlite3_free(data);
    return SQLITE_CORRUPT;
  }
  nTables = dlReadU16(&r);

  aTables = sqlite3_malloc(nTables * (int)sizeof(ConflictTableInfo));
  if( !aTables ){ sqlite3_free(data); return SQLITE_NOMEM; }
  memset(aTables, 0, nTables * (int)sizeof(ConflictTableInfo));

  for(i=0; i<nTables; i++){
    int nc;
    rc = dlReadU16Name(&r, &aTables[i].zName);
    if( rc!=SQLITE_OK ) goto conflicts_cleanup;
    nc = dlReadU32(&r);
    if( r.err || nc<0 ){ rc = SQLITE_CORRUPT; goto conflicts_cleanup; }
    /* Reject impossible counts up front: each row needs at least one byte, so
    ** nc can't exceed the bytes remaining. malloc64 avoids 32-bit overflow. */
    if( (sqlite3_uint64)nc > (sqlite3_uint64)(r.end - r.p) ){
      rc = SQLITE_CORRUPT; goto conflicts_cleanup;
    }
    aTables[i].nConflicts = nc;
    aTables[i].aRows = sqlite3_malloc64((sqlite3_uint64)nc * sizeof(struct ConflictRow));
    if( !aTables[i].aRows ){ rc = SQLITE_NOMEM; goto conflicts_cleanup; }
    memset(aTables[i].aRows, 0, (sqlite3_uint64)nc * sizeof(struct ConflictRow));

    for(j=0; j<nc; j++){
      struct ConflictRow *cr = &aTables[i].aRows[j];
      rc = dlReadU32Blob(&r, &cr->pKey, &cr->nKey);
      if( rc!=SQLITE_OK ) goto conflicts_cleanup;
      cr->intKey = dlReadI64(&r);
      rc = dlReadU32Blob(&r, &cr->pBaseVal, &cr->nBaseVal);
      if( rc!=SQLITE_OK ) goto conflicts_cleanup;
      rc = dlReadU32Blob(&r, &cr->pOurVal, &cr->nOurVal);
      if( rc!=SQLITE_OK ) goto conflicts_cleanup;
      rc = dlReadU32Blob(&r, &cr->pTheirVal, &cr->nTheirVal);
      if( rc!=SQLITE_OK ) goto conflicts_cleanup;
    }
  }

  if( r.err || r.p != r.end ){ rc = SQLITE_CORRUPT; goto conflicts_cleanup; }

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
  DoltliteVcTxnMode mode;
  for(i=0; i<nTables; i++) totalConflicts += aTables[i].nConflicts;

  {
    int rc;
    extern void doltliteSetSessionConflictsCatalog(sqlite3*, const ProllyHash*);
    extern void doltliteSetSessionMergeState(sqlite3*, u8, const ProllyHash*, const ProllyHash*);
    rc = doltliteEnsureWriteTxnAndSavepoints(db);
    if( rc!=SQLITE_OK ) return rc;
    if( totalConflicts==0 ){
      doltliteSetSessionConflictsCatalog(db, &(ProllyHash){{0}});
    }else{
      ProllyHash newHash;
      rc = doltliteSerializeConflicts(cs, aTables, nTables, &newHash);
      if( rc!=SQLITE_OK ) return rc;
      doltliteSetSessionConflictsCatalog(db, &newHash);
      doltliteSetSessionMergeState(db, 1, 0, &newHash);
    }
    mode = doltliteVcTxnMode(db);
    if( mode==DOLTLITE_VC_TXN_AUTOCOMMIT_LIKE ){
      rc = doltlitePersistWorkingSet(db);
      if( rc!=SQLITE_OK ) return rc;
      return doltliteVcSealActiveSavepoints(db);
    }
    return doltliteSaveWorkingSet(db);
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

  rc = sqlite3_declare_vtab(db, "CREATE TABLE x(\"table\" TEXT, num_conflicts INTEGER)");
  if(rc!=SQLITE_OK) return rc;
  v = sqlite3_malloc(sizeof(*v)); if(!v) return SQLITE_NOMEM;
  memset(v,0,sizeof(*v)); v->db=db; *ppVtab=&v->base; return SQLITE_OK;
}
static int cfOpen(sqlite3_vtab *v, sqlite3_vtab_cursor **pp){
  (void)v;
  return doltliteVtabOpenCursor(pp, sizeof(ConflictsCur));
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
  0,0,cfConnect,cfBestIndex,doltliteVtabDisconnect,0,cfOpen,cfClose,cfFilter,cfNext,cfEof,
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

static char *cfrBuildSchema(const DoltliteColInfo *ci){
  sqlite3_str *pStr = sqlite3_str_new(0);
  char *z;
  if( !pStr ) return 0;
  sqlite3_str_appendall(pStr, "CREATE TABLE x(from_root_ish TEXT");

  if( ci->nCol>0 ){
    sqlite3_str_appendall(pStr, ", ");
    if( doltliteAppendQuotedColumnList(pStr, ci->azName, ci->nCol, "base_", ", ")!=SQLITE_OK ){
      sqlite3_str_reset(pStr);
      return 0;
    }

    sqlite3_str_appendall(pStr, ", ");
    if( doltliteAppendQuotedColumnList(pStr, ci->azName, ci->nCol, "our_", ", ")!=SQLITE_OK ){
      sqlite3_str_reset(pStr);
      return 0;
    }
  }
  sqlite3_str_appendall(pStr, ", our_diff_type TEXT");

  if( ci->nCol>0 ){
    sqlite3_str_appendall(pStr, ", ");
    if( doltliteAppendQuotedColumnList(pStr, ci->azName, ci->nCol, "their_", ", ")!=SQLITE_OK ){
      sqlite3_str_reset(pStr);
      return 0;
    }
  }
  sqlite3_str_appendall(pStr, ", their_diff_type TEXT");
  sqlite3_str_appendall(pStr, ", dolt_conflict_id TEXT");
  sqlite3_str_appendall(pStr, ")");
  z = sqlite3_str_finish(pStr);
  return z;
}

static int cfrConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  (void)pAux;
  return doltliteVtabConnectUserTable(db, argc, argv, "dolt_conflicts_",
                                      sizeof(CfRowVtab), cfrBuildSchema,
                                      ppVtab, pzErr);
}

static int cfrOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **pp){
  CfRowCur *c;
  (void)pVtab;
  if( doltliteVtabOpenCursor(pp, sizeof(CfRowCur))!=SQLITE_OK ){
    return SQLITE_NOMEM;
  }
  c = (CfRowCur*)*pp;
  c->iTableIdx = -1;
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

static void cfrEmitRecordCol(
  sqlite3_context *ctx,
  const u8 *pRec, int nRec,
  int iUserCol,
  const DoltliteColInfo *pCols,
  i64 intKey
){
  doltliteResultUserCol(ctx, pCols, pRec, nRec, intKey, iUserCol);
}

static const char *cfrDiffType(const u8 *pBase, int nBase,
                               const u8 *pSide, int nSide){
  int baseHas = (pBase && nBase>0);
  int sideHas = (pSide && nSide>0);
  return doltliteDiffTypeNameFromPresence(baseHas, sideHas);
}

static sqlite3_int64 cfrConflictRowid(const struct ConflictRow *cr){
  u64 h = DOLTLITE_FNV1A_OFFSET;
  h = doltliteFnv1aBytes(h, cr->pKey, cr->nKey);
  h = doltliteFnv1aSep(h);
  h = doltliteFnv1aI64(h, cr->intKey);
  h = doltliteFnv1aSep(h);
  h = doltliteFnv1aBytes(h, cr->pBaseVal, cr->nBaseVal);
  h = doltliteFnv1aSep(h);
  h = doltliteFnv1aBytes(h, cr->pOurVal, cr->nOurVal);
  h = doltliteFnv1aSep(h);
  h = doltliteFnv1aBytes(h, cr->pTheirVal, cr->nTheirVal);
  return (sqlite3_int64)(h & 0x7fffffffffffffffULL);
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

  nUserCols = v->cols.nCol;
  colBaseStart  = 1;
  colOurStart   = 1 + nUserCols;
  colOurDiff    = 1 + 2*nUserCols;
  colTheirStart = 2 + 2*nUserCols;
  colTheirDiff  = 2 + 3*nUserCols;
  colConflictId = 3 + 3*nUserCols;

  if( col==0 ){

    sqlite3_result_null(ctx);
  }else if( col>=colBaseStart && col<colOurStart ){
    cfrEmitRecordCol(ctx, cr->pBaseVal, cr->nBaseVal,
                     col - colBaseStart, &v->cols, cr->intKey);
  }else if( col>=colOurStart && col<colOurDiff ){
    cfrEmitRecordCol(ctx, cr->pOurVal, cr->nOurVal,
                     col - colOurStart, &v->cols, cr->intKey);
  }else if( col==colOurDiff ){
    sqlite3_result_text(ctx,
      cfrDiffType(cr->pBaseVal, cr->nBaseVal, cr->pOurVal, cr->nOurVal),
      -1, SQLITE_STATIC);
  }else if( col>=colTheirStart && col<colTheirDiff ){
    cfrEmitRecordCol(ctx, cr->pTheirVal, cr->nTheirVal,
                     col - colTheirStart, &v->cols, cr->intKey);
  }else if( col==colTheirDiff ){
    sqlite3_result_text(ctx,
      cfrDiffType(cr->pBaseVal, cr->nBaseVal, cr->pTheirVal, cr->nTheirVal),
      -1, SQLITE_STATIC);
  }else if( col==colConflictId ){

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
    *r = cfrConflictRowid(&c->aTables[c->iTableIdx].aRows[c->iRow]);
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
      if( cfrConflictRowid(&aTables[i].aRows[j]) == deleteRowid ){
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
  doltliteVtabCommonDisconnect,
  doltliteVtabCommonDisconnect,
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

static int conflictsResolveTableExists(sqlite3 *db, const char *zTable, int *pExists){
  sqlite3_stmt *pStmt = 0;
  int rc;

  *pExists = 0;
  rc = sqlite3_prepare_v2(db,
      "SELECT 1 FROM main.sqlite_master WHERE type='table' AND name=?1",
      -1, &pStmt, 0);
  if( rc!=SQLITE_OK ) return rc;
  rc = sqlite3_bind_text(pStmt, 1, zTable, -1, SQLITE_TRANSIENT);
  if( rc==SQLITE_OK ){
    rc = sqlite3_step(pStmt);
    if( rc==SQLITE_ROW ){
      *pExists = 1;
      rc = SQLITE_OK;
    }else if( rc==SQLITE_DONE ){
      rc = SQLITE_OK;
    }
  }
  sqlite3_finalize(pStmt);
  return rc;
}

static int conflictsResolveSealSuccessfulTopSavepoint(sqlite3 *db){
  if( doltliteVcTxnMode(db)==DOLTLITE_VC_TXN_AUTOCOMMIT_LIKE ){
    return doltliteVcSealActiveSavepoints(db);
  }
  return SQLITE_OK;
}

/* Finish an --ours/--theirs resolve. When the table was not among the conflict
** tables, an already-existing table is a no-op success and anything else is
** "table not found"; otherwise persist the updated conflict set. Frees aTables
** and sets the SQL result on every path. */
static void conflictsResolveFinish(
  sqlite3_context *ctx,
  sqlite3 *db,
  ChunkStore *cs,
  ConflictTableInfo *aTables,
  int nTables,
  int found,
  const char *zTable
){
  int tableExists = 0;
  int rc;

  if( !found ){
    rc = conflictsResolveTableExists(db, zTable, &tableExists);
    freeConflictTables(aTables, nTables);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(ctx, rc);
      return;
    }
    if( tableExists ){
      rc = conflictsResolveSealSuccessfulTopSavepoint(db);
      if( rc!=SQLITE_OK ){
        sqlite3_result_error_code(ctx, rc);
        return;
      }
      sqlite3_result_int(ctx, 0);
      return;
    }
    sqlite3_result_error(ctx, "table not found", -1);
    return;
  }
  rc = storeUpdatedConflicts(db, cs, aTables, nTables);
  freeConflictTables(aTables, nTables);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(ctx, rc);
    return;
  }
  sqlite3_result_int(ctx, 0);
}

static void conflictsResolveFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  const char *zMode, *zTable;
  ConflictTableInfo *aTables = 0;
  int nTables = 0;
  int found = 0;
  int i, j, rc;

  if(!cs){ sqlite3_result_error(ctx,"no database",-1); return; }
  if(argc!=2){ sqlite3_result_error(ctx,"usage: dolt_conflicts_resolve('--ours'|'--theirs','table')",-1); return; }

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
        found = 1;
        removeConflictTable(aTables, &nTables, i);
        break;
      }
    }
    conflictsResolveFinish(ctx, db, cs, aTables, nTables, found, zTable);

  }else if( strcmp(zMode,"--theirs")==0 ){

    for(i=0; i<nTables; i++){
      if( !aTables[i].zName || strcmp(aTables[i].zName, zTable)!=0 ) continue;
      found = 1;

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
    conflictsResolveFinish(ctx, db, cs, aTables, nTables, found, zTable);

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
