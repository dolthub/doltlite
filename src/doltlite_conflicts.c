
#ifdef DOLTLITE_PROLLY

#include "doltlite_vtab_util.h"
#include "doltlite_internal.h"
#include <string.h>

typedef DoltliteConflictTable ConflictTableInfo;

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

#define DOLTLITE_CONFLICTS_MAGIC0 'D'
#define DOLTLITE_CONFLICTS_MAGIC1 'L'
#define DOLTLITE_CONFLICTS_MAGIC2 'C'
#define DOLTLITE_CONFLICTS_VERSION 1

int doltliteSerializeConflicts(
  ChunkStore *cs,
  DoltliteConflictTable *aTables, int nTables,
  ProllyHash *pHash
){
  sqlite3_int64 sz = 4 + 2;
  int i, j, rc;
  u8 *buf;
  DlByteWriter w;

  if( nTables<0 || nTables>0xffff ) return SQLITE_TOOBIG;
  if( nTables>0 && !aTables ) return SQLITE_CORRUPT;
  for(i=0; i<nTables; i++){
    size_t nName = aTables[i].zName ? strlen(aTables[i].zName) : 0;
    int nl;
    if( nName>0xffff || aTables[i].nConflicts<0 ) return SQLITE_TOOBIG;
    if( aTables[i].nConflicts>0 && !aTables[i].aRows ) return SQLITE_CORRUPT;
    nl = (int)nName;
    rc = dlAddSize(&sz, 2 + nl + 4);
    if( rc!=SQLITE_OK ) return rc;
    for(j=0; j<aTables[i].nConflicts; j++){
      DoltliteConflictRow *cr = &aTables[i].aRows[j];
      if( cr->nKey<0 || cr->nBaseVal<0 || cr->nOurVal<0
       || cr->nTheirVal<0
      ){
        return SQLITE_CORRUPT;
      }
      if( (cr->nKey>0 && !cr->pKey)
       || (cr->nBaseVal>0 && !cr->pBaseVal)
       || (cr->nOurVal>0 && !cr->pOurVal)
       || (cr->nTheirVal>0 && !cr->pTheirVal)
      ){
        return SQLITE_CORRUPT;
      }
      rc = dlAddSize(&sz, 24);
      if( rc==SQLITE_OK ) rc = dlAddSize(&sz, cr->nKey);
      if( rc==SQLITE_OK ) rc = dlAddSize(&sz, cr->nBaseVal);
      if( rc==SQLITE_OK ) rc = dlAddSize(&sz, cr->nOurVal);
      if( rc==SQLITE_OK ) rc = dlAddSize(&sz, cr->nTheirVal);
      if( rc!=SQLITE_OK ) return rc;
    }
  }

  buf = sqlite3_malloc64((sqlite3_uint64)sz);
  if( !buf ) return SQLITE_NOMEM;
  dlWriterInit(&w, buf, (int)sz);

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

  if( w.err || w.p!=w.end ){
    sqlite3_free(buf);
    return SQLITE_CORRUPT;
  }
  rc = sqlite3FaultSim(950) ? SQLITE_IOERR
                            : chunkStorePut(cs, buf, (int)sz, pHash);
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

static int deserializeAllConflicts(
  const u8 *data,
  int nData,
  ConflictTableInfo **ppTables, int *pnTables
){
  DlByteReader r;
  int nTables, i, j, rc;
  ConflictTableInfo *aTables;

  *ppTables = 0;
  *pnTables = 0;
  if( !data || nData<(4+2) ) return SQLITE_CORRUPT;

  dlReaderInit(&r, data, nData);
  if( dlReadFramedHeader(&r, DOLTLITE_CONFLICTS_MAGIC0, DOLTLITE_CONFLICTS_MAGIC1,
                         DOLTLITE_CONFLICTS_MAGIC2, DOLTLITE_CONFLICTS_VERSION,
                         &nTables)!=SQLITE_OK ){
    return SQLITE_CORRUPT;
  }

  aTables = sqlite3_malloc(nTables ? nTables * (int)sizeof(ConflictTableInfo) : 1);
  if( !aTables ) return SQLITE_NOMEM;
  memset(aTables, 0, nTables ? nTables * (int)sizeof(ConflictTableInfo) : 1);

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
    if( nc>0 ){
      aTables[i].aRows = sqlite3_malloc64(
          (sqlite3_uint64)nc * sizeof(DoltliteConflictRow));
      if( !aTables[i].aRows ){ rc = SQLITE_NOMEM; goto conflicts_cleanup; }
      memset(aTables[i].aRows, 0,
             (sqlite3_uint64)nc * sizeof(DoltliteConflictRow));
    }

    for(j=0; j<nc; j++){
      DoltliteConflictRow *cr = &aTables[i].aRows[j];
      rc = readConflictRow(&r, cr);
      if( rc!=SQLITE_OK ) goto conflicts_cleanup;
    }
  }

  if( r.err || r.p != r.end ){ rc = SQLITE_CORRUPT; goto conflicts_cleanup; }

  *ppTables = aTables;
  *pnTables = nTables;
  return SQLITE_OK;

conflicts_cleanup:
  freeConflictTables(aTables, nTables);
  return rc;
}

int doltliteDeserializeConflictsForTest(const u8 *data, int nData){
  ConflictTableInfo *aTables = 0;
  int nTables = 0;
  int rc = deserializeAllConflicts(data, nData, &aTables, &nTables);
  freeConflictTables(aTables, nTables);
  return rc;
}

static int loadAllConflicts(
  sqlite3 *db,
  ChunkStore *cs,
  ConflictTableInfo **ppTables, int *pnTables
){
  ProllyHash hash;
  u8 *data = 0;
  int nData = 0;
  int rc;

  doltliteGetSessionConflictsCatalog(db, &hash);
  if( prollyHashIsEmpty(&hash) ){
    *ppTables = 0;
    *pnTables = 0;
    return SQLITE_OK;
  }
  rc = chunkStoreGet(cs, &hash, &data, &nData);
  if( rc==SQLITE_OK ){
    rc = deserializeAllConflicts(data, nData, ppTables, pnTables);
  }
  sqlite3_free(data);
  return rc;
}

int doltliteSessionHasSchemaConflicts(sqlite3 *db){
  ConflictTableInfo *aTables = 0;
  int nTables = 0;
  int i;
  int rc = loadAllConflicts(db, doltliteGetChunkStore(db),
                            &aTables, &nTables);
  if( rc!=SQLITE_OK ) return 0;
  for(i=0; i<nTables; i++){
    if( aTables[i].nConflicts==0 ){
      freeConflictTables(aTables, nTables);
      return 1;
    }
  }
  freeConflictTables(aTables, nTables);
  return 0;
}

int doltliteForEachSchemaConflict(
  sqlite3 *db,
  int (*xConflict)(void*, const char*),
  void *pCtx
){
  ConflictTableInfo *aTables = 0;
  int nTables = 0;
  int i;
  int rc = loadAllConflicts(db, doltliteGetChunkStore(db),
                            &aTables, &nTables);
  if( rc!=SQLITE_OK ) return rc;
  for(i=0; i<nTables && rc==SQLITE_OK; i++){
    if( aTables[i].nConflicts==0 ){
      rc = xConflict(pCtx, aTables[i].zName);
    }
  }
  freeConflictTables(aTables, nTables);
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
  for(j=0; j<pTable->nSchemaObjects; j++){
    sqlite3_free(pTable->azSchemaObjects[j]);
  }
  sqlite3_free(pTable->azSchemaObjects);
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

static int storeConflictBytes(
  sqlite3 *db,
  ChunkStore *cs,
  const u8 *pData,
  int nData,
  int nTables
){
  int rc;
  DoltliteVcTxnMode mode;

  rc = doltliteEnsureWriteTxnAndSavepoints(db);
  if( rc!=SQLITE_OK ) return rc;
  if( nTables==0 ){
    rc = doltliteSetSessionConflictsCatalog(db, &(ProllyHash){{0}});
  }else{
    ProllyHash newHash;
    rc = chunkStorePut(cs, pData, nData, &newHash);
    if( rc!=SQLITE_OK ) return rc;
    rc = doltliteSetSessionConflictsCatalog(db, &newHash);
    if( rc==SQLITE_OK ){
      rc = doltliteSetSessionMergeConflicts(db, &newHash);
    }
  }
  if( rc!=SQLITE_OK ) return rc;
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

  dlWriterInit(&w, out, nData);
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
        dlWriterInit(&cw, pCountOut, 4);
        dlWriteU32(&cw, nKeep);
        assert( !cw.err );
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
        dlWriteBytes(&w, pTableStart, nCopy);
        nOutTables++;
      }
    }
    sqlite3_free(zName);
  }

  if( r.err || r.p != r.end || w.err ){
    rc = SQLITE_CORRUPT;
    goto delete_conflict_done;
  }
  if( deleted ){
    DlByteWriter hw;
    dlWriterInit(&hw, out, 6);
    dlWriteFramedHeader(&hw, DOLTLITE_CONFLICTS_MAGIC0, DOLTLITE_CONFLICTS_MAGIC1,
                        DOLTLITE_CONFLICTS_MAGIC2, DOLTLITE_CONFLICTS_VERSION,
                        nOutTables);
    assert( !hw.err );
    rc = storeConflictBytes(db, cs, out, (int)(w.p - out), nOutTables);
  }

delete_conflict_done:
  sqlite3_free(out);
  sqlite3_free(data);
  return rc;
}

static int removeConflictTableFromCatalog(
  sqlite3 *db,
  ChunkStore *cs,
  const char *zTableName,
  int *pFound
){
  ProllyHash hash;
  u8 *data = 0;
  u8 *out = 0;
  int nData = 0;
  DlByteReader r;
  DlByteWriter w;
  int nTables, nOutTables = 0;
  int i, j, rc = SQLITE_OK;
  extern void doltliteGetSessionConflictsCatalog(sqlite3*, ProllyHash*);

  *pFound = 0;
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
    goto remove_conflict_done;
  }

  dlWriterInit(&w, out, nData);
  dlWriteFramedHeader(&w, DOLTLITE_CONFLICTS_MAGIC0, DOLTLITE_CONFLICTS_MAGIC1,
                      DOLTLITE_CONFLICTS_MAGIC2, DOLTLITE_CONFLICTS_VERSION, 0);

  for(i=0; i<nTables; i++){
    const u8 *pTableStart = r.p;
    char *zName = 0;
    int nc;
    int isMatch;

    rc = dlReadU16Name(&r, &zName);
    if( rc!=SQLITE_OK ) goto remove_conflict_done;
    nc = dlReadU32(&r);
    if( r.err || nc<0 ){
      sqlite3_free(zName);
      rc = SQLITE_CORRUPT;
      goto remove_conflict_done;
    }
    if( (sqlite3_uint64)nc > (sqlite3_uint64)(r.end - r.p) ){
      sqlite3_free(zName);
      rc = SQLITE_CORRUPT;
      goto remove_conflict_done;
    }

    isMatch = (*pFound==0 && zName && strcmp(zName, zTableName)==0);
    for(j=0; j<nc; j++){
      rc = skipConflictRow(&r);
      if( rc!=SQLITE_OK ){
        sqlite3_free(zName);
        goto remove_conflict_done;
      }
    }
    if( isMatch ){
      *pFound = 1;
    }else{
      int nCopy = (int)(r.p - pTableStart);
      dlWriteBytes(&w, pTableStart, nCopy);
      nOutTables++;
    }
    sqlite3_free(zName);
  }

  if( r.err || r.p != r.end || w.err ){
    rc = SQLITE_CORRUPT;
    goto remove_conflict_done;
  }
  if( *pFound ){
    DlByteWriter hw;
    dlWriterInit(&hw, out, 6);
    dlWriteFramedHeader(&hw, DOLTLITE_CONFLICTS_MAGIC0, DOLTLITE_CONFLICTS_MAGIC1,
                        DOLTLITE_CONFLICTS_MAGIC2, DOLTLITE_CONFLICTS_VERSION,
                        nOutTables);
    assert( !hw.err );
    rc = storeConflictBytes(db, cs, out, (int)(w.p - out), nOutTables);
  }

remove_conflict_done:
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

/* Dolt lists a schema conflict in dolt_conflicts only when the conflicted
** table exists in the working root.  A conflict whose "ours" side deleted
** the table remains visible in dolt_schema_conflicts and dolt_status. */
static void cfPruneDeletedSchemaConflicts(sqlite3 *db, ConflictsCur *c){
  int i;
  int nOut = 0;
  for(i=0; i<c->nTables; i++){
    ConflictTableInfo *p = &c->aTables[i];
    if( p->nConflicts==0 && sqlite3FindTable(db, p->zName, 0)==0 ){
      freeConflictTable(p);
      continue;
    }
    if( nOut!=i ){
      c->aTables[nOut] = c->aTables[i];
      memset(&c->aTables[i], 0, sizeof(c->aTables[i]));
    }
    nOut++;
  }
  c->nTables = nOut;
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
    if( found && (table.nConflicts!=0
                  || sqlite3FindTable(vt->db, table.zName, 0)!=0) ){
      c->aTables = sqlite3_malloc(sizeof(*c->aTables));
      if( !c->aTables ){
        freeConflictTable(&table);
        return SQLITE_NOMEM;
      }
      c->aTables[0] = table;
      c->nTables = 1;
    }else if( found ){
      freeConflictTable(&table);
    }
    return SQLITE_OK;
  }
  rc = loadAllConflicts(vt->db, doltliteGetChunkStore(vt->db),
                        &c->aTables, &c->nTables);
  if( rc==SQLITE_OK ) cfPruneDeletedSchemaConflicts(vt->db, c);
  return rc;
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

typedef struct SchemaConflictRow SchemaConflictRow;
struct SchemaConflictRow {
  char *zTable;
  char *zBase;
  char *zOurs;
  char *zTheirs;
  char *zDescription;
};

typedef struct SchemaConflictsVtab SchemaConflictsVtab;
struct SchemaConflictsVtab { sqlite3_vtab base; sqlite3 *db; };

typedef struct SchemaConflictsCur SchemaConflictsCur;
struct SchemaConflictsCur {
  sqlite3_vtab_cursor base;
  SchemaConflictRow *aRows;
  int nRows;
  int iRow;
};

static void schemaConflictsFreeRows(SchemaConflictsCur *pCur){
  int i;
  for(i=0; i<pCur->nRows; i++){
    sqlite3_free(pCur->aRows[i].zTable);
    sqlite3_free(pCur->aRows[i].zBase);
    sqlite3_free(pCur->aRows[i].zOurs);
    sqlite3_free(pCur->aRows[i].zTheirs);
    sqlite3_free(pCur->aRows[i].zDescription);
  }
  sqlite3_free(pCur->aRows);
  pCur->aRows = 0;
  pCur->nRows = 0;
  pCur->iRow = 0;
}

static int schemaEntrySame(const SchemaEntry *pA, const SchemaEntry *pB){
  if( !pA && !pB ) return 1;
  if( !pA || !pB ) return 0;
  if( sqlite3_stricmp(pA->zType ? pA->zType : "",
                      pB->zType ? pB->zType : "")!=0 ) return 0;
  if( sqlite3_stricmp(pA->zTblName ? pA->zTblName : "",
                      pB->zTblName ? pB->zTblName : "")!=0 ) return 0;
  if( (pA->zSql==0)!=(pB->zSql==0) ) return 0;
  return !pA->zSql || strcmp(pA->zSql, pB->zSql)==0;
}

static SchemaEntry *schemaTableEntry(
  SchemaEntry *aSchema,
  int nSchema,
  const char *zTable
){
  SchemaEntry *p = findSchemaEntry(aSchema, nSchema, zTable);
  if( p && p->zType && strcmp(p->zType, "table")==0 ) return p;
  return 0;
}

static char *schemaConflictSql(
  SchemaEntry *aSchema,
  int nSchema,
  const char *zTable
){
  SchemaEntry *pTable = schemaTableEntry(aSchema, nSchema, zTable);
  sqlite3_str *pStr = sqlite3_str_new(0);
  int i;
  int nAdded = 0;
  char *z;

  if( !pStr ) return 0;
  if( pTable && pTable->zSql ){
    sqlite3_str_appendall(pStr, pTable->zSql);
    nAdded++;
  }
  for(i=0; i<nSchema; i++){
    SchemaEntry *p = &aSchema[i];
    if( !p->zSql || !p->zType || !p->zName ) continue;
    if( strcmp(p->zType, "index")==0 && p->zTblName
     && sqlite3_stricmp(p->zTblName, zTable)==0 ){
      if( nAdded ) sqlite3_str_appendall(pStr, ";\n");
      sqlite3_str_appendall(pStr, p->zSql);
      nAdded++;
    }
  }
  if( nAdded==0 ){
    SchemaEntry *p = findSchemaEntry(aSchema, nSchema, zTable);
    if( p && p->zSql ){
      sqlite3_str_appendall(pStr, p->zSql);
      nAdded++;
    }
  }
  if( nAdded==0 ) sqlite3_str_appendall(pStr, "<deleted>");
  z = sqlite3_str_finish(pStr);
  return z;
}

static char *schemaConflictIndexDescription(
  SchemaEntry *aBase, int nBase,
  SchemaEntry *aOurs, int nOurs,
  SchemaEntry *aTheirs, int nTheirs,
  const char *zTable
){
  int side, i;
  for(side=0; side<3; side++){
    SchemaEntry *a = side==0 ? aBase : (side==1 ? aOurs : aTheirs);
    int n = side==0 ? nBase : (side==1 ? nOurs : nTheirs);
    for(i=0; i<n; i++){
      SchemaEntry *pBase;
      SchemaEntry *pOurs;
      SchemaEntry *pTheirs;
      int oursChanged;
      int theirsChanged;
      if( !a[i].zType || strcmp(a[i].zType, "index")!=0
       || !a[i].zName || !a[i].zTblName
       || sqlite3_stricmp(a[i].zTblName, zTable)!=0 ){
        continue;
      }
      pBase = findSchemaEntry(aBase, nBase, a[i].zName);
      pOurs = findSchemaEntry(aOurs, nOurs, a[i].zName);
      pTheirs = findSchemaEntry(aTheirs, nTheirs, a[i].zName);
      oursChanged = !schemaEntrySame(pBase, pOurs);
      theirsChanged = !schemaEntrySame(pBase, pTheirs);
      if( !oursChanged || !theirsChanged || schemaEntrySame(pOurs, pTheirs) ){
        continue;
      }
      if( !pBase ){
        return sqlite3_mprintf(
            "both branches added index '%s' with different definitions",
            a[i].zName);
      }
      if( !pOurs || !pTheirs ){
        return sqlite3_mprintf(
            "index '%s' modified on one branch and deleted on the other",
            a[i].zName);
      }
      return sqlite3_mprintf(
          "both branches modified index '%s' differently", a[i].zName);
    }
  }
  return 0;
}

static char *schemaConflictDescription(
  SchemaEntry *aBase, int nBase,
  SchemaEntry *aOurs, int nOurs,
  SchemaEntry *aTheirs, int nTheirs,
  const char *zTable,
  const char *zBase,
  const char *zOurs,
  const char *zTheirs
){
  SchemaEntry *pBase = schemaTableEntry(aBase, nBase, zTable);
  SchemaEntry *pOurs = schemaTableEntry(aOurs, nOurs, zTable);
  SchemaEntry *pTheirs = schemaTableEntry(aTheirs, nTheirs, zTable);
  char *zDetail = 0;
  int rc;

  if( !pBase && pOurs && pTheirs ){
    return sqlite3_mprintf(
        "table '%s' added on both branches with different definitions", zTable);
  }
  if( pBase && (!pOurs || !pTheirs) ){
    const char *zSurvivor = pOurs ? zOurs : zTheirs;
    const char *zKind = strcmp(zBase, zSurvivor)==0
        ? "data modification" : "schema modification";
    return sqlite3_mprintf("cannot merge a table deletion with %s", zKind);
  }
  if( pBase && pOurs && pTheirs ){
    rc = doltliteTableSchemaConflictDetail(pBase->zSql, pOurs->zSql,
                                           pTheirs->zSql, &zDetail);
    if( rc!=SQLITE_OK ){
      sqlite3_free(zDetail);
      return 0;
    }
    if( zDetail ) return zDetail;
  }
  zDetail = schemaConflictIndexDescription(
      aBase, nBase, aOurs, nOurs, aTheirs, nTheirs, zTable);
  if( zDetail ) return zDetail;
  return sqlite3_mprintf("incompatible schema changes for table '%s'", zTable);
}

static int schemaConflictsLoadRows(SchemaConflictsCur *pCur, sqlite3 *db){
  ConflictTableInfo *aTables = 0;
  int nTables = 0;
  ProllyHash ourHead, theirHead, ancestorHash;
  DoltliteCommit ourCommit, theirCommit, ancestorCommit;
  SchemaEntry *aBase = 0, *aOurs = 0, *aTheirs = 0;
  int nBase = 0, nOurs = 0, nTheirs = 0;
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  u8 isMerging = 0;
  int i, nSchemaRows = 0;
  int rc;

  memset(&ourCommit, 0, sizeof(ourCommit));
  memset(&theirCommit, 0, sizeof(theirCommit));
  memset(&ancestorCommit, 0, sizeof(ancestorCommit));
  doltliteGetSessionMergeState(db, &isMerging, &theirHead, 0);
  if( !isMerging || prollyHashIsEmpty(&theirHead) ) return SQLITE_OK;
  rc = loadAllConflicts(db, cs, &aTables, &nTables);
  if( rc!=SQLITE_OK ) goto done;
  for(i=0; i<nTables; i++){
    if( aTables[i].nConflicts==0 ) nSchemaRows++;
  }
  if( nSchemaRows==0 ) goto done;

  doltliteGetSessionHead(db, &ourHead);
  rc = doltliteFindAncestor(db, &ourHead, &theirHead, &ancestorHash);
  if( rc!=SQLITE_OK ) goto done;
  rc = doltliteLoadCommit(db, &ourHead, &ourCommit);
  if( rc!=SQLITE_OK ) goto done;
  rc = doltliteLoadCommit(db, &theirHead, &theirCommit);
  if( rc!=SQLITE_OK ) goto done;
  rc = doltliteLoadCommit(db, &ancestorHash, &ancestorCommit);
  if( rc!=SQLITE_OK ) goto done;
  rc = loadSchemaFromCatalog(db, cs, pCache, &ancestorCommit.catalogHash,
                             &aBase, &nBase);
  if( rc==SQLITE_OK ){
    rc = loadSchemaFromCatalog(db, cs, pCache, &ourCommit.catalogHash,
                               &aOurs, &nOurs);
  }
  if( rc==SQLITE_OK ){
    rc = loadSchemaFromCatalog(db, cs, pCache, &theirCommit.catalogHash,
                               &aTheirs, &nTheirs);
  }
  if( rc!=SQLITE_OK ) goto done;

  pCur->aRows = sqlite3_malloc(nSchemaRows*(int)sizeof(SchemaConflictRow));
  if( !pCur->aRows ){ rc = SQLITE_NOMEM; goto done; }
  memset(pCur->aRows, 0, nSchemaRows*(int)sizeof(SchemaConflictRow));
  for(i=0; i<nTables; i++){
    SchemaConflictRow *pRow;
    if( aTables[i].nConflicts!=0 ) continue;
    pRow = &pCur->aRows[pCur->nRows];
    pCur->nRows++;
    pRow->zTable = sqlite3_mprintf("%s", aTables[i].zName);
    pRow->zBase = schemaConflictSql(aBase, nBase, aTables[i].zName);
    pRow->zOurs = schemaConflictSql(aOurs, nOurs, aTables[i].zName);
    pRow->zTheirs = schemaConflictSql(aTheirs, nTheirs, aTables[i].zName);
    if( !pRow->zTable || !pRow->zBase || !pRow->zOurs || !pRow->zTheirs ){
      rc = SQLITE_NOMEM;
      goto done;
    }
    pRow->zDescription = schemaConflictDescription(
        aBase, nBase, aOurs, nOurs, aTheirs, nTheirs,
        aTables[i].zName, pRow->zBase, pRow->zOurs, pRow->zTheirs);
    if( !pRow->zDescription ){ rc = SQLITE_NOMEM; goto done; }
  }

done:
  freeConflictTables(aTables, nTables);
  freeSchemaEntries(aBase, nBase);
  freeSchemaEntries(aOurs, nOurs);
  freeSchemaEntries(aTheirs, nTheirs);
  doltliteCommitClear(&ourCommit);
  doltliteCommitClear(&theirCommit);
  doltliteCommitClear(&ancestorCommit);
  if( rc!=SQLITE_OK ) schemaConflictsFreeRows(pCur);
  return rc;
}

static int scConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  SchemaConflictsVtab *p;
  int rc;
  (void)pAux; (void)argc; (void)argv; (void)pzErr;
  rc = doltliteVtabConnectSimple(db,
      "CREATE TABLE x(table_name TEXT PRIMARY KEY, base_schema TEXT, "
      "our_schema TEXT, their_schema TEXT, description TEXT)",
      sizeof(*p), ppVtab);
  if( rc==SQLITE_OK ){
    p = (SchemaConflictsVtab*)*ppVtab;
    p->db = db;
  }
  return rc;
}

static int scOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor){
  (void)pVtab;
  return doltliteVtabOpenCursor(ppCursor, sizeof(SchemaConflictsCur));
}

static int scClose(sqlite3_vtab_cursor *pCursor){
  SchemaConflictsCur *pCur = (SchemaConflictsCur*)pCursor;
  schemaConflictsFreeRows(pCur);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

static int scFilter(sqlite3_vtab_cursor *pCursor, int idxNum,
    const char *idxStr, int argc, sqlite3_value **argv){
  SchemaConflictsCur *pCur = (SchemaConflictsCur*)pCursor;
  SchemaConflictsVtab *pVtab = (SchemaConflictsVtab*)pCursor->pVtab;
  (void)idxNum; (void)idxStr; (void)argc; (void)argv;
  schemaConflictsFreeRows(pCur);
  return schemaConflictsLoadRows(pCur, pVtab->db);
}

static int scNext(sqlite3_vtab_cursor *pCursor){
  ((SchemaConflictsCur*)pCursor)->iRow++;
  return SQLITE_OK;
}

static int scEof(sqlite3_vtab_cursor *pCursor){
  SchemaConflictsCur *pCur = (SchemaConflictsCur*)pCursor;
  return pCur->iRow>=pCur->nRows;
}

static int scColumn(sqlite3_vtab_cursor *pCursor, sqlite3_context *ctx, int iCol){
  SchemaConflictsCur *pCur = (SchemaConflictsCur*)pCursor;
  SchemaConflictRow *pRow;
  if( pCur->iRow>=pCur->nRows ) return SQLITE_OK;
  pRow = &pCur->aRows[pCur->iRow];
  switch( iCol ){
    case 0: sqlite3_result_text(ctx, pRow->zTable, -1, SQLITE_TRANSIENT); break;
    case 1: sqlite3_result_text(ctx, pRow->zBase, -1, SQLITE_TRANSIENT); break;
    case 2: sqlite3_result_text(ctx, pRow->zOurs, -1, SQLITE_TRANSIENT); break;
    case 3: sqlite3_result_text(ctx, pRow->zTheirs, -1, SQLITE_TRANSIENT); break;
    case 4: sqlite3_result_text(ctx, pRow->zDescription, -1, SQLITE_TRANSIENT); break;
  }
  return SQLITE_OK;
}

static int scRowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid){
  *pRowid = ((SchemaConflictsCur*)pCursor)->iRow + 1;
  return SQLITE_OK;
}

static int scBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  (void)pVtab;
  pInfo->estimatedCost = 10.0;
  pInfo->estimatedRows = 4;
  return SQLITE_OK;
}

static sqlite3_module schemaConflictsModule = {
  0,0,scConnect,scBestIndex,doltliteVtabDisconnect,0,
  scOpen,scClose,scFilter,scNext,scEof,scColumn,scRowid,
  0,0,0,0,0,0,0,0,0,0,0,0
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
  doltliteResultUserCol(ctx, pCols, pRec, nRec, intKey, 1, iUserCol);
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

static void conflictsResolveFinishNoConflictTable(
  sqlite3_context *ctx,
  sqlite3 *db,
  const char *zTable
){
  int tableExists = 0;
  int rc;

  rc = conflictsResolveTableExists(db, zTable, &tableExists);
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
}

static void conflictsResolveFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  const char *zMode, *zTable;
  ConflictTableInfo table;
  int found = 0;
  int j, rc;

  if(!cs){ sqlite3_result_error(ctx,"no database",-1); return; }
  if(argc!=2){ sqlite3_result_error(ctx,"usage: dolt_conflicts_resolve('--ours'|'--theirs','table')",-1); return; }

  zMode = (const char*)sqlite3_value_text(argv[0]);
  zTable = (const char*)sqlite3_value_text(argv[1]);
  if(!zMode||!zTable){ sqlite3_result_error(ctx,"invalid args",-1); return; }

  rc = loadConflictTable(db, cs, zTable, &table, &found);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(ctx, rc);
    return;
  }
  if( found && table.nConflicts==0 ){
    freeConflictTable(&table);
    sqlite3_result_error(ctx,
      "Unable to automatically resolve schema conflicts since data changes "
      "may not have been fully merged yet. Abort this merge, align the "
      "schemas on one side, then rerun the merge.", -1);
    return;
  }
  freeConflictTable(&table);
  found = 0;

  if( strcmp(zMode,"--ours")==0 ){
    rc = removeConflictTableFromCatalog(db, cs, zTable, &found);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(ctx, rc);
      return;
    }
    if( found ){
      sqlite3_result_int(ctx, 0);
    }else{
      conflictsResolveFinishNoConflictTable(ctx, db, zTable);
    }

  }else if( strcmp(zMode,"--theirs")==0 ){
    rc = loadConflictTable(db, cs, zTable, &table, &found);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(ctx, rc);
      return;
    }

    if( found ){
      for(j=0; j<table.nConflicts; j++){
        DoltliteConflictRow *cr = &table.aRows[j];
        rc = doltliteApplyRawRowMutation(db, zTable,
                                         cr->pKey, cr->nKey, cr->intKey,
                                         cr->pTheirVal, cr->nTheirVal);
        if( rc!=SQLITE_OK ){
          freeConflictTable(&table);
          sqlite3_result_error(ctx, "failed to apply theirs value", -1);
          return;
        }
      }
      freeConflictTable(&table);
      rc = removeConflictTableFromCatalog(db, cs, zTable, &found);
      if( rc!=SQLITE_OK ){
        sqlite3_result_error_code(ctx, rc);
        return;
      }
      sqlite3_result_int(ctx, 0);
    }else{
      conflictsResolveFinishNoConflictTable(ctx, db, zTable);
    }

  }else{
    sqlite3_result_error(ctx, "use --ours or --theirs", -1);
  }
}

int doltliteConflictsRegister(sqlite3 *db){
  int rc;
  rc = sqlite3_create_module(db, "dolt_conflicts", &conflictsModule, 0);
  if( rc==SQLITE_OK )
    rc = sqlite3_create_module(db, "dolt_schema_conflicts",
                               &schemaConflictsModule, 0);
  if( rc==SQLITE_OK )
    rc = sqlite3_create_function(db, "dolt_conflicts_resolve", -1,
                                  DOLTLITE_COMMAND_FUNC_FLAGS, 0,
                                  conflictsResolveFunc, 0, 0);

  if( rc==SQLITE_OK )
    rc = doltliteRegisterConflictTables(db);
  return rc;
}

#endif
