
#ifdef DOLTLITE_PROLLY

#include "doltlite_vtab_util.h"
#include "doltlite_internal.h"
#include <string.h>

typedef struct ConflictTableInfo ConflictTableInfo;
struct ConflictTableInfo {
  char *zName;
  int nConflicts;
  DoltliteConflictRow *aRows;
};

static void freeConflictTable(ConflictTableInfo *pTable);
static void freeConflictTables(ConflictTableInfo *aTables, int nTables);
static sqlite3_int64 cfrConflictRowid(const DoltliteConflictRow *cr);

void doltliteConflictRowFree(DoltliteConflictRow *pRow){
  if( !pRow ) return;
  sqlite3_free(pRow->pKey);
  sqlite3_free(pRow->pBaseVal);
  sqlite3_free(pRow->pOurVal);
  sqlite3_free(pRow->pTheirVal);
}

static void freeConflictRow(DoltliteConflictRow *pRow){
  if( !pRow ) return;
  doltliteConflictRowFree(pRow);
  memset(pRow, 0, sizeof(*pRow));
}

static void removeConflictTable(ConflictTableInfo *aTables, int *pnTables, int iTable){
  if( !aTables || !pnTables || iTable<0 || iTable>=*pnTables ) return;
  freeConflictTable(&aTables[iTable]);
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

  dlWriteFramedHeader(&w, DOLTLITE_CONFLICTS_MAGIC0, DOLTLITE_CONFLICTS_MAGIC1,
                      DOLTLITE_CONFLICTS_MAGIC2, DOLTLITE_CONFLICTS_VERSION, nTables);
  for(i=0; i<nTables; i++){
    int nl = aTables[i].zName ? (int)strlen(aTables[i].zName) : 0;
    dlWriteU16Name(&w, aTables[i].zName, nl);
    dlWriteU32(&w, aTables[i].nConflicts);
    for(j=0; j<aTables[i].nConflicts; j++){
      DoltliteConflictRow *cr = &aTables[i].aRows[j];
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

static int readConflictRow(DlByteReader *r, DoltliteConflictRow *cr){
  int rc;
  rc = dlReadU32Blob(r, &cr->pKey, &cr->nKey);
  if( rc!=SQLITE_OK ) return rc;
  cr->intKey = dlReadI64(r);
  rc = dlReadU32Blob(r, &cr->pBaseVal, &cr->nBaseVal);
  if( rc!=SQLITE_OK ) return rc;
  rc = dlReadU32Blob(r, &cr->pOurVal, &cr->nOurVal);
  if( rc!=SQLITE_OK ) return rc;
  return dlReadU32Blob(r, &cr->pTheirVal, &cr->nTheirVal);
}

static int skipConflictBlob(DlByteReader *r){
  int n = dlReadU32(r);
  if( r->err ) return SQLITE_CORRUPT;
  if( n<0 || (size_t)n > (size_t)(r->end - r->p) ){
    r->err = 1;
    return SQLITE_CORRUPT;
  }
  r->p += n;
  return SQLITE_OK;
}

static int skipConflictRow(DlByteReader *r){
  int rc;
  rc = skipConflictBlob(r);
  if( rc!=SQLITE_OK ) return rc;
  (void)dlReadI64(r);
  if( r->err ) return SQLITE_CORRUPT;
  rc = skipConflictBlob(r);
  if( rc!=SQLITE_OK ) return rc;
  rc = skipConflictBlob(r);
  if( rc!=SQLITE_OK ) return rc;
  return skipConflictBlob(r);
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
  if( dlReadFramedHeader(&r, DOLTLITE_CONFLICTS_MAGIC0, DOLTLITE_CONFLICTS_MAGIC1,
                         DOLTLITE_CONFLICTS_MAGIC2, DOLTLITE_CONFLICTS_VERSION,
                         &nTables)!=SQLITE_OK ){
    sqlite3_free(data);
    return SQLITE_CORRUPT;
  }

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
    aTables[i].aRows = sqlite3_malloc64((sqlite3_uint64)nc * sizeof(DoltliteConflictRow));
    if( !aTables[i].aRows ){ rc = SQLITE_NOMEM; goto conflicts_cleanup; }
    memset(aTables[i].aRows, 0, (sqlite3_uint64)nc * sizeof(DoltliteConflictRow));

    for(j=0; j<nc; j++){
      DoltliteConflictRow *cr = &aTables[i].aRows[j];
      rc = readConflictRow(&r, cr);
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

static int loadConflictTable(
  sqlite3 *db,
  ChunkStore *cs,
  const char *zTableName,
  ConflictTableInfo *pTable,
  int *pFound
){
  ProllyHash hash;
  u8 *data = 0; int nData = 0;
  extern void doltliteGetSessionConflictsCatalog(sqlite3*, ProllyHash*);
  DlByteReader r;
  int nTables, i, j, rc;

  memset(pTable, 0, sizeof(*pTable));
  *pFound = 0;

  doltliteGetSessionConflictsCatalog(db, &hash);
  if( prollyHashIsEmpty(&hash) ) return SQLITE_OK;

  rc = chunkStoreGet(cs, &hash, &data, &nData);
  if( rc!=SQLITE_OK ) return rc;
  if( nData<(4+2) ){ sqlite3_free(data); return SQLITE_CORRUPT; }

  dlReaderInit(&r, data, nData);
  if( dlReadFramedHeader(&r, DOLTLITE_CONFLICTS_MAGIC0, DOLTLITE_CONFLICTS_MAGIC1,
                         DOLTLITE_CONFLICTS_MAGIC2, DOLTLITE_CONFLICTS_VERSION,
                         &nTables)!=SQLITE_OK ){
    sqlite3_free(data);
    return SQLITE_CORRUPT;
  }

  for(i=0; i<nTables; i++){
    char *zName = 0;
    int nc;
    int isMatch;

    rc = dlReadU16Name(&r, &zName);
    if( rc!=SQLITE_OK ) goto conflict_table_cleanup;
    nc = dlReadU32(&r);
    if( r.err || nc<0 ){
      sqlite3_free(zName);
      rc = SQLITE_CORRUPT; goto conflict_table_cleanup;
    }
    if( (sqlite3_uint64)nc > (sqlite3_uint64)(r.end - r.p) ){
      sqlite3_free(zName);
      rc = SQLITE_CORRUPT; goto conflict_table_cleanup;
    }

    isMatch = (*pFound==0 && zName && strcmp(zName, zTableName)==0);
    if( isMatch ){
      pTable->zName = zName;
      zName = 0;
      pTable->nConflicts = nc;
      if( nc>0 ){
        pTable->aRows = sqlite3_malloc64((sqlite3_uint64)nc * sizeof(DoltliteConflictRow));
        if( !pTable->aRows ){ rc = SQLITE_NOMEM; goto conflict_table_cleanup; }
        memset(pTable->aRows, 0, (sqlite3_uint64)nc * sizeof(DoltliteConflictRow));
      }
      for(j=0; j<nc; j++){
        rc = readConflictRow(&r, &pTable->aRows[j]);
        if( rc!=SQLITE_OK ) goto conflict_table_cleanup;
      }
      *pFound = 1;
    }else{
      sqlite3_free(zName);
      for(j=0; j<nc; j++){
        rc = skipConflictRow(&r);
        if( rc!=SQLITE_OK ) goto conflict_table_cleanup;
      }
    }
  }

  if( r.err || r.p != r.end ){ rc = SQLITE_CORRUPT; goto conflict_table_cleanup; }

  sqlite3_free(data);
  return SQLITE_OK;

conflict_table_cleanup:
  freeConflictTable(pTable);
  sqlite3_free(data);
  return rc;
}

static void freeConflictTable(ConflictTableInfo *pTable){
  int j;
  if( !pTable ) return;
  for(j=0; j<pTable->nConflicts; j++){
    freeConflictRow(&pTable->aRows[j]);
  }
  sqlite3_free(pTable->aRows);
  sqlite3_free(pTable->zName);
  memset(pTable, 0, sizeof(*pTable));
}

static void freeConflictTables(ConflictTableInfo *aTables, int nTables){
  int i;
  for(i=0; i<nTables; i++){
    freeConflictTable(&aTables[i]);
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

static int storeConflictBytes(
  sqlite3 *db,
  ChunkStore *cs,
  const u8 *pData,
  int nData,
  int nTables
){
  int rc;
  DoltliteVcTxnMode mode;
  extern void doltliteSetSessionConflictsCatalog(sqlite3*, const ProllyHash*);
  extern void doltliteSetSessionMergeState(sqlite3*, u8, const ProllyHash*, const ProllyHash*);

  rc = doltliteEnsureWriteTxnAndSavepoints(db);
  if( rc!=SQLITE_OK ) return rc;
  if( nTables==0 ){
    doltliteSetSessionConflictsCatalog(db, &(ProllyHash){{0}});
  }else{
    ProllyHash newHash;
    rc = chunkStorePut(cs, pData, nData, &newHash);
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

static int deleteConflictRowFromCatalog(
  sqlite3 *db,
  ChunkStore *cs,
  const char *zTableName,
  i64 deleteRowid
){
  ProllyHash hash;
  u8 *data = 0;
  u8 *out = 0;
  int nData = 0;
  DlByteReader r;
  DlByteWriter w;
  int nTables, nOutTables = 0;
  int i, j, rc = SQLITE_OK;
  int deleted = 0;
  extern void doltliteGetSessionConflictsCatalog(sqlite3*, ProllyHash*);

  doltliteGetSessionConflictsCatalog(db, &hash);
  if( prollyHashIsEmpty(&hash) ) return SQLITE_OK;

  rc = chunkStoreGet(cs, &hash, &data, &nData);
  if( rc!=SQLITE_OK ) return rc;
  if( nData<(4+2) ){ sqlite3_free(data); return SQLITE_CORRUPT; }

  out = sqlite3_malloc(nData);
  if( !out ){ sqlite3_free(data); return SQLITE_NOMEM; }

  dlReaderInit(&r, data, nData);
  if( dlReadFramedHeader(&r, DOLTLITE_CONFLICTS_MAGIC0, DOLTLITE_CONFLICTS_MAGIC1,
                         DOLTLITE_CONFLICTS_MAGIC2, DOLTLITE_CONFLICTS_VERSION,
                         &nTables)!=SQLITE_OK ){
    rc = SQLITE_CORRUPT;
    goto delete_conflict_done;
  }

  w.p = out;
  dlWriteFramedHeader(&w, DOLTLITE_CONFLICTS_MAGIC0, DOLTLITE_CONFLICTS_MAGIC1,
                      DOLTLITE_CONFLICTS_MAGIC2, DOLTLITE_CONFLICTS_VERSION, 0);

  for(i=0; i<nTables; i++){
    const u8 *pTableStart = r.p;
    u8 *pOutTableStart = w.p;
    char *zName = 0;
    int nc;
    int isMatch;

    rc = dlReadU16Name(&r, &zName);
    if( rc!=SQLITE_OK ) goto delete_conflict_done;
    nc = dlReadU32(&r);
    if( r.err || nc<0 ){
      sqlite3_free(zName);
      rc = SQLITE_CORRUPT;
      goto delete_conflict_done;
    }
    if( (sqlite3_uint64)nc > (sqlite3_uint64)(r.end - r.p) ){
      sqlite3_free(zName);
      rc = SQLITE_CORRUPT;
      goto delete_conflict_done;
    }

    isMatch = (!deleted && zName && strcmp(zName, zTableName)==0);
    if( isMatch ){
      u8 *pCountOut = 0;
      int nKeep = 0;
      int nl = (int)strlen(zName);

      dlWriteU16Name(&w, zName, nl);
      pCountOut = w.p;
      dlWriteU32(&w, 0);
      for(j=0; j<nc; j++){
        DoltliteConflictRow cr;
        memset(&cr, 0, sizeof(cr));
        rc = readConflictRow(&r, &cr);
        if( rc!=SQLITE_OK ){
          freeConflictRow(&cr);
          sqlite3_free(zName);
          goto delete_conflict_done;
        }
        if( !deleted && cfrConflictRowid(&cr) == deleteRowid ){
          deleted = 1;
        }else{
          dlWriteU32Blob(&w, cr.pKey, cr.nKey);
          dlWriteI64(&w, cr.intKey);
          dlWriteU32Blob(&w, cr.pBaseVal, cr.nBaseVal);
          dlWriteU32Blob(&w, cr.pOurVal, cr.nOurVal);
          dlWriteU32Blob(&w, cr.pTheirVal, cr.nTheirVal);
          nKeep++;
        }
        freeConflictRow(&cr);
      }
      if( nKeep==0 ){
        w.p = pOutTableStart;
      }else{
        DlByteWriter cw;
        cw.p = pCountOut;
        dlWriteU32(&cw, nKeep);
        nOutTables++;
      }
    }else{
      for(j=0; j<nc; j++){
        rc = skipConflictRow(&r);
        if( rc!=SQLITE_OK ){
          sqlite3_free(zName);
          goto delete_conflict_done;
        }
      }
      assert( !isMatch );
      {
        int nCopy = (int)(r.p - pTableStart);
        memcpy(w.p, pTableStart, nCopy);
        w.p += nCopy;
        nOutTables++;
      }
    }
    sqlite3_free(zName);
  }

  if( r.err || r.p != r.end ){ rc = SQLITE_CORRUPT; goto delete_conflict_done; }
  if( deleted ){
    DlByteWriter hw;
    hw.p = out;
    dlWriteFramedHeader(&hw, DOLTLITE_CONFLICTS_MAGIC0, DOLTLITE_CONFLICTS_MAGIC1,
                        DOLTLITE_CONFLICTS_MAGIC2, DOLTLITE_CONFLICTS_VERSION,
                        nOutTables);
    rc = storeConflictBytes(db, cs, out, (int)(w.p - out), nOutTables);
  }

delete_conflict_done:
  sqlite3_free(out);
  sqlite3_free(data);
  return rc;
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

  rc = doltliteVtabConnectSimple(db,
      "CREATE TABLE x(\"table\" TEXT, num_conflicts INTEGER)",
      sizeof(*v), ppVtab);
  if(rc!=SQLITE_OK) return rc;
  v = (ConflictsVtab*)*ppVtab;
  v->db = db;
  return SQLITE_OK;
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
  int rc;
  (void)s;
  c->iRow=0;
  freeConflictTables(c->aTables, c->nTables);
  c->aTables = 0;
  c->nTables = 0;
  if( n==1 && a>=1 ){
    const char *zTable = (const char*)sqlite3_value_text(v[0]);
    ConflictTableInfo table;
    int found = 0;
    if( !zTable ) return SQLITE_OK;
    rc = loadConflictTable(vt->db, doltliteGetChunkStore(vt->db),
                           zTable, &table, &found);
    if( rc!=SQLITE_OK ) return rc;
    if( found ){
      c->aTables = sqlite3_malloc(sizeof(*c->aTables));
      if( !c->aTables ){
        freeConflictTable(&table);
        return SQLITE_NOMEM;
      }
      c->aTables[0] = table;
      c->nTables = 1;
    }
    return SQLITE_OK;
  }
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
static int cfBestIndex(sqlite3_vtab *v, sqlite3_index_info *p){
  (void)v;
  p->estimatedCost = 10;
  return doltliteBestIndexEq(p, 0);
}

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
  ConflictTableInfo table;
  int hasTable;
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
  (void)pVtab;
  if( doltliteVtabOpenCursor(pp, sizeof(CfRowCur))!=SQLITE_OK ){
    return SQLITE_NOMEM;
  }
  return SQLITE_OK;
}

static int cfrClose(sqlite3_vtab_cursor *cur){
  CfRowCur *c = (CfRowCur*)cur;
  freeConflictTable(&c->table);
  sqlite3_free(c);
  return SQLITE_OK;
}

static int cfrFilter(sqlite3_vtab_cursor *cur, int n, const char *s, int a, sqlite3_value **v){
  CfRowCur *c = (CfRowCur*)cur;
  CfRowVtab *vt = (CfRowVtab*)cur->pVtab;
  int rc;
  (void)n;(void)s;(void)a;(void)v;

  freeConflictTable(&c->table);
  c->iRow = 0;
  c->hasTable = 0;
  rc = loadConflictTable(vt->db, doltliteGetChunkStore(vt->db),
                         vt->zTableName, &c->table, &c->hasTable);
  return rc;
}

static int cfrNext(sqlite3_vtab_cursor *cur){
  ((CfRowCur*)cur)->iRow++;
  return SQLITE_OK;
}

static int cfrEof(sqlite3_vtab_cursor *cur){
  CfRowCur *c = (CfRowCur*)cur;
  return !c->hasTable || c->iRow >= c->table.nConflicts;
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

static sqlite3_int64 cfrConflictRowid(const DoltliteConflictRow *cr){
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
  DoltliteConflictRow *cr;
  int nUserCols;
  int colBaseStart, colOurStart, colOurDiff;
  int colTheirStart, colTheirDiff, colConflictId;

  if( !c->hasTable ) return SQLITE_OK;
  if( c->iRow >= c->table.nConflicts ) return SQLITE_OK;
  cr = &c->table.aRows[c->iRow];

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
  if( c->hasTable && c->iRow < c->table.nConflicts ){
    *r = cfrConflictRowid(&c->table.aRows[c->iRow]);
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
  i64 deleteRowid;

  (void)pRowid;

  if( nArg != 1 ){
    pVtab->zErrMsg = sqlite3_mprintf("only DELETE is supported on conflict tables");
    return SQLITE_ERROR;
  }

  deleteRowid = sqlite3_value_int64(apArg[0]);
  return deleteConflictRowFromCatalog(v->db, cs, v->zTableName, deleteRowid);
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
        DoltliteConflictRow *cr = &aTables[i].aRows[j];
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
