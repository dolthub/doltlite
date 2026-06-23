
#ifdef DOLTLITE_PROLLY

#include "doltlite_vtab_util.h"
#include "doltlite_internal.h"
#include "doltlite_constraint_violations.h"

#include <string.h>

static void freeViolationTable(ConstraintViolationTable *pTable);
static sqlite3_int64 cvrViolationRowid(const ConstraintViolationRow *r);

static void freeViolationRow(ConstraintViolationRow *r){
  if( !r ) return;
  sqlite3_free(r->pKey);
  sqlite3_free(r->pVal);
  sqlite3_free(r->zInfo);
  memset(r, 0, sizeof(*r));
}

static void freeViolationTables(ConstraintViolationTable *a, int n){
  int i;
  if( !a ) return;
  for(i=0; i<n; i++){
    freeViolationTable(&a[i]);
  }
  sqlite3_free(a);
}

static ConstraintViolationTable *findOrCreateViolationTable(
  ConstraintViolationTable **paTables, int *pnTables, const char *zTable
){
  int i;
  ConstraintViolationTable *aNew;
  ConstraintViolationTable *pT;
  for(i=0; i<*pnTables; i++){
    if( (*paTables)[i].zName
     && strcmp((*paTables)[i].zName, zTable)==0 ){
      return &(*paTables)[i];
    }
  }
  aNew = sqlite3_realloc(*paTables,
      ((*pnTables) + 1) * (int)sizeof(ConstraintViolationTable));
  if( !aNew ) return 0;
  *paTables = aNew;
  pT = &aNew[*pnTables];
  memset(pT, 0, sizeof(*pT));
  pT->zName = sqlite3_mprintf("%s", zTable);
  if( !pT->zName ) return 0;
  (*pnTables)++;
  return pT;
}

#define DCV_MAGIC0 'D'
#define DCV_MAGIC1 'C'
#define DCV_MAGIC2 'V'
#define DCV_VERSION 1

static int serializeViolations(
  ChunkStore *cs,
  ConstraintViolationTable *aTables, int nTables,
  ProllyHash *pHash
){
  int sz = 4 + 2;
  int i, j, rc;
  u8 *buf;
  DlByteWriter w;

  for(i=0; i<nTables; i++){
    int nl = aTables[i].zName ? (int)strlen(aTables[i].zName) : 0;
    sz += 2 + nl + 4;
    for(j=0; j<aTables[i].nRows; j++){
      int ni = aTables[i].aRows[j].zInfo
             ? (int)strlen(aTables[i].aRows[j].zInfo) : 0;
      sz += 1
          + 4 + aTables[i].aRows[j].nKey
          + 8
          + 4 + aTables[i].aRows[j].nVal
          + 4 + ni;
    }
  }

  buf = sqlite3_malloc(sz);
  if( !buf ) return SQLITE_NOMEM;
  w.p = buf;

  dlWriteFramedHeader(&w, DCV_MAGIC0, DCV_MAGIC1, DCV_MAGIC2, DCV_VERSION, nTables);

  for(i=0; i<nTables; i++){
    int nl = aTables[i].zName ? (int)strlen(aTables[i].zName) : 0;
    dlWriteU16Name(&w, aTables[i].zName, nl);
    dlWriteU32(&w, aTables[i].nRows);
    for(j=0; j<aTables[i].nRows; j++){
      ConstraintViolationRow *r = &aTables[i].aRows[j];
      int ni = r->zInfo ? (int)strlen(r->zInfo) : 0;
      dlWriteU8(&w, (u8)r->violationType);
      dlWriteU32Blob(&w, r->pKey, r->nKey);
      dlWriteI64(&w, r->intKey);
      dlWriteU32Blob(&w, r->pVal, r->nVal);
      dlWriteU32Blob(&w, (const u8*)r->zInfo, ni);
    }
  }

  rc = chunkStorePut(cs, buf, (int)(w.p-buf), pHash);
  sqlite3_free(buf);
  return rc;
}

static int readViolationRow(DlByteReader *rd, ConstraintViolationRow *r){
  int rc;
  r->violationType = dlReadU8(rd);
  rc = dlReadU32Blob(rd, &r->pKey, &r->nKey);
  if( rc!=SQLITE_OK ) return rc;
  r->intKey = dlReadI64(rd);
  rc = dlReadU32Blob(rd, &r->pVal, &r->nVal);
  if( rc!=SQLITE_OK ) return rc;
  return dlReadU32Str(rd, &r->zInfo);
}

static int skipViolationBlob(DlByteReader *rd){
  int n = dlReadU32(rd);
  if( rd->err ) return SQLITE_CORRUPT;
  if( n<0 || (size_t)n > (size_t)(rd->end - rd->p) ){
    rd->err = 1;
    return SQLITE_CORRUPT;
  }
  rd->p += n;
  return SQLITE_OK;
}

static int skipViolationRow(DlByteReader *rd){
  int rc;
  (void)dlReadU8(rd);
  if( rd->err ) return SQLITE_CORRUPT;
  rc = skipViolationBlob(rd);
  if( rc!=SQLITE_OK ) return rc;
  (void)dlReadI64(rd);
  if( rd->err ) return SQLITE_CORRUPT;
  rc = skipViolationBlob(rd);
  if( rc!=SQLITE_OK ) return rc;
  return skipViolationBlob(rd);
}

static int loadAllViolations(
  sqlite3 *db,
  ChunkStore *cs,
  ConstraintViolationTable **ppTables, int *pnTables
){
  ProllyHash hash;
  u8 *data = 0; int nData = 0;
  DlByteReader rd;
  int nTables, i, j, rc;
  ConstraintViolationTable *aTables;

  *ppTables = 0;
  *pnTables = 0;

  doltliteGetSessionConstraintViolationsCatalog(db, &hash);
  if( prollyHashIsEmpty(&hash) ) return SQLITE_OK;

  rc = chunkStoreGet(cs, &hash, &data, &nData);
  if( rc!=SQLITE_OK ) return rc;
  if( nData<(4+2) ){ sqlite3_free(data); return SQLITE_CORRUPT; }

  dlReaderInit(&rd, data, nData);
  if( dlReadFramedHeader(&rd, DCV_MAGIC0, DCV_MAGIC1, DCV_MAGIC2, DCV_VERSION,
                         &nTables)!=SQLITE_OK ){
    sqlite3_free(data);
    return SQLITE_CORRUPT;
  }

  aTables = sqlite3_malloc(nTables ? nTables * (int)sizeof(*aTables) : 1);
  if( !aTables ){ sqlite3_free(data); return SQLITE_NOMEM; }
  memset(aTables, 0, nTables ? nTables * (int)sizeof(*aTables) : 1);

  for(i=0; i<nTables; i++){
    int nr;
    rc = dlReadU16Name(&rd, &aTables[i].zName);
    if( rc!=SQLITE_OK ) goto fail;
    nr = dlReadU32(&rd);
    if( rd.err || nr<0 ){ rc = SQLITE_CORRUPT; goto fail; }
    /* Reject impossible counts up front: every row needs at least one byte, so
    ** nr can't exceed the bytes remaining. sqlite3_malloc64 below avoids 32-bit
    ** overflow on the row-array allocation. */
    if( (sqlite3_uint64)nr > (sqlite3_uint64)(rd.end - rd.p) ){
      rc = SQLITE_CORRUPT; goto fail;
    }
    aTables[i].nRows = nr;
    aTables[i].aRows = sqlite3_malloc64(nr ? (sqlite3_uint64)nr * sizeof(ConstraintViolationRow) : 1);
    if( !aTables[i].aRows ){ rc = SQLITE_NOMEM; goto fail; }
    memset(aTables[i].aRows, 0, nr ? (sqlite3_uint64)nr * sizeof(ConstraintViolationRow) : 1);

    for(j=0; j<nr; j++){
      ConstraintViolationRow *r = &aTables[i].aRows[j];
      rc = readViolationRow(&rd, r);
      if( rc!=SQLITE_OK ) goto fail;
    }
  }

  if( rd.err || rd.p != rd.end ){ rc = SQLITE_CORRUPT; goto fail; }

  *ppTables = aTables;
  *pnTables = nTables;
  sqlite3_free(data);
  (void)db;
  return SQLITE_OK;

fail:
  freeViolationTables(aTables, nTables);
  sqlite3_free(data);
  return rc;
}

static int loadViolationTable(
  sqlite3 *db,
  ChunkStore *cs,
  const char *zTableName,
  ConstraintViolationTable *pTable,
  int *pFound
){
  ProllyHash hash;
  u8 *data = 0; int nData = 0;
  DlByteReader rd;
  int nTables, i, j, rc;

  memset(pTable, 0, sizeof(*pTable));
  *pFound = 0;

  doltliteGetSessionConstraintViolationsCatalog(db, &hash);
  if( prollyHashIsEmpty(&hash) ) return SQLITE_OK;

  rc = chunkStoreGet(cs, &hash, &data, &nData);
  if( rc!=SQLITE_OK ) return rc;
  if( nData<(4+2) ){ sqlite3_free(data); return SQLITE_CORRUPT; }

  dlReaderInit(&rd, data, nData);
  if( dlReadFramedHeader(&rd, DCV_MAGIC0, DCV_MAGIC1, DCV_MAGIC2, DCV_VERSION,
                         &nTables)!=SQLITE_OK ){
    sqlite3_free(data);
    return SQLITE_CORRUPT;
  }

  for(i=0; i<nTables; i++){
    char *zName = 0;
    int nr;
    int isMatch;

    rc = dlReadU16Name(&rd, &zName);
    if( rc!=SQLITE_OK ) goto fail;
    nr = dlReadU32(&rd);
    if( rd.err || nr<0 ){
      sqlite3_free(zName);
      rc = SQLITE_CORRUPT; goto fail;
    }
    if( (sqlite3_uint64)nr > (sqlite3_uint64)(rd.end - rd.p) ){
      sqlite3_free(zName);
      rc = SQLITE_CORRUPT; goto fail;
    }

    isMatch = (*pFound==0 && zName && strcmp(zName, zTableName)==0);
    if( isMatch ){
      pTable->zName = zName;
      zName = 0;
      pTable->nRows = nr;
      pTable->aRows = sqlite3_malloc64(nr ? (sqlite3_uint64)nr * sizeof(ConstraintViolationRow) : 1);
      if( !pTable->aRows ){ rc = SQLITE_NOMEM; goto fail; }
      memset(pTable->aRows, 0, nr ? (sqlite3_uint64)nr * sizeof(ConstraintViolationRow) : 1);
      for(j=0; j<nr; j++){
        rc = readViolationRow(&rd, &pTable->aRows[j]);
        if( rc!=SQLITE_OK ) goto fail;
      }
      *pFound = 1;
    }else{
      sqlite3_free(zName);
      for(j=0; j<nr; j++){
        rc = skipViolationRow(&rd);
        if( rc!=SQLITE_OK ) goto fail;
      }
    }
  }

  if( rd.err || rd.p != rd.end ){ rc = SQLITE_CORRUPT; goto fail; }

  sqlite3_free(data);
  return SQLITE_OK;

fail:
  freeViolationTable(pTable);
  sqlite3_free(data);
  return rc;
}

static void freeViolationTable(ConstraintViolationTable *pTable){
  int j;
  if( !pTable ) return;
  for(j=0; j<pTable->nRows; j++){
    freeViolationRow(&pTable->aRows[j]);
  }
  sqlite3_free(pTable->aRows);
  sqlite3_free(pTable->zName);
  memset(pTable, 0, sizeof(*pTable));
}

static int storeUpdatedViolations(
  sqlite3 *db,
  ChunkStore *cs,
  ConstraintViolationTable *aTables, int nTables
){
  int totalRows = 0;
  int i;
  for(i=0; i<nTables; i++) totalRows += aTables[i].nRows;

  if( totalRows==0 ){
    static const ProllyHash emptyHash = {{0}};
    doltliteSetSessionConstraintViolationsCatalog(db, &emptyHash);
  }else{
    ProllyHash newHash;
    int rc = serializeViolations(cs, aTables, nTables, &newHash);
    if( rc!=SQLITE_OK ) return rc;
    doltliteSetSessionConstraintViolationsCatalog(db, &newHash);
  }
  if( doltliteVcTxnMode(db)==DOLTLITE_VC_TXN_AUTOCOMMIT_LIKE ){
    return doltlitePersistWorkingSet(db);
  }
  return doltliteSaveWorkingSet(db);
}

static int storeViolationBytes(
  sqlite3 *db,
  ChunkStore *cs,
  const u8 *pData,
  int nData,
  int nTables
){
  if( nTables==0 ){
    static const ProllyHash emptyHash = {{0}};
    doltliteSetSessionConstraintViolationsCatalog(db, &emptyHash);
  }else{
    ProllyHash newHash;
    int rc = chunkStorePut(cs, pData, nData, &newHash);
    if( rc!=SQLITE_OK ) return rc;
    doltliteSetSessionConstraintViolationsCatalog(db, &newHash);
  }
  if( doltliteVcTxnMode(db)==DOLTLITE_VC_TXN_AUTOCOMMIT_LIKE ){
    return doltlitePersistWorkingSet(db);
  }
  return doltliteSaveWorkingSet(db);
}

static int deleteViolationRowFromCatalog(
  sqlite3 *db,
  ChunkStore *cs,
  const char *zTableName,
  i64 deleteRowid
){
  ProllyHash hash;
  u8 *data = 0;
  u8 *out = 0;
  int nData = 0;
  DlByteReader rd;
  DlByteWriter w;
  int nTables, nOutTables = 0;
  int i, j, rc = SQLITE_OK;
  int deleted = 0;

  doltliteGetSessionConstraintViolationsCatalog(db, &hash);
  if( prollyHashIsEmpty(&hash) ) return SQLITE_OK;

  rc = chunkStoreGet(cs, &hash, &data, &nData);
  if( rc!=SQLITE_OK ) return rc;
  if( nData<(4+2) ){ sqlite3_free(data); return SQLITE_CORRUPT; }

  out = sqlite3_malloc(nData);
  if( !out ){ sqlite3_free(data); return SQLITE_NOMEM; }

  dlReaderInit(&rd, data, nData);
  if( dlReadFramedHeader(&rd, DCV_MAGIC0, DCV_MAGIC1, DCV_MAGIC2, DCV_VERSION,
                         &nTables)!=SQLITE_OK ){
    rc = SQLITE_CORRUPT;
    goto delete_violation_done;
  }

  w.p = out;
  dlWriteFramedHeader(&w, DCV_MAGIC0, DCV_MAGIC1, DCV_MAGIC2, DCV_VERSION, 0);

  for(i=0; i<nTables; i++){
    const u8 *pTableStart = rd.p;
    u8 *pOutTableStart = w.p;
    char *zName = 0;
    int nr;
    int isMatch;

    rc = dlReadU16Name(&rd, &zName);
    if( rc!=SQLITE_OK ) goto delete_violation_done;
    nr = dlReadU32(&rd);
    if( rd.err || nr<0 ){
      sqlite3_free(zName);
      rc = SQLITE_CORRUPT;
      goto delete_violation_done;
    }
    if( (sqlite3_uint64)nr > (sqlite3_uint64)(rd.end - rd.p) ){
      sqlite3_free(zName);
      rc = SQLITE_CORRUPT;
      goto delete_violation_done;
    }

    isMatch = (!deleted && zName && strcmp(zName, zTableName)==0);
    if( isMatch ){
      u8 *pCountOut = 0;
      int nKeep = 0;
      int nl = (int)strlen(zName);

      dlWriteU16Name(&w, zName, nl);
      pCountOut = w.p;
      dlWriteU32(&w, 0);
      for(j=0; j<nr; j++){
        ConstraintViolationRow vr;
        memset(&vr, 0, sizeof(vr));
        rc = readViolationRow(&rd, &vr);
        if( rc!=SQLITE_OK ){
          freeViolationRow(&vr);
          sqlite3_free(zName);
          goto delete_violation_done;
        }
        if( !deleted && cvrViolationRowid(&vr) == deleteRowid ){
          deleted = 1;
        }else{
          int ni = vr.zInfo ? (int)strlen(vr.zInfo) : 0;
          dlWriteU8(&w, (u8)vr.violationType);
          dlWriteU32Blob(&w, vr.pKey, vr.nKey);
          dlWriteI64(&w, vr.intKey);
          dlWriteU32Blob(&w, vr.pVal, vr.nVal);
          dlWriteU32Blob(&w, (const u8*)vr.zInfo, ni);
          nKeep++;
        }
        freeViolationRow(&vr);
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
      for(j=0; j<nr; j++){
        rc = skipViolationRow(&rd);
        if( rc!=SQLITE_OK ){
          sqlite3_free(zName);
          goto delete_violation_done;
        }
      }
      assert( !isMatch );
      {
        int nCopy = (int)(rd.p - pTableStart);
        memcpy(w.p, pTableStart, nCopy);
        w.p += nCopy;
        nOutTables++;
      }
    }
    sqlite3_free(zName);
  }

  if( rd.err || rd.p != rd.end ){ rc = SQLITE_CORRUPT; goto delete_violation_done; }
  if( deleted ){
    DlByteWriter hw;
    hw.p = out;
    dlWriteFramedHeader(&hw, DCV_MAGIC0, DCV_MAGIC1, DCV_MAGIC2, DCV_VERSION,
                        nOutTables);
    rc = storeViolationBytes(db, cs, out, (int)(w.p - out), nOutTables);
  }

delete_violation_done:
  sqlite3_free(out);
  sqlite3_free(data);
  return rc;
}

/* Append one violation row to an in-memory table set. */
static int cvAppendRow(
  ConstraintViolationTable **paTables, int *pnTables,
  const char *zTable, u8 violationType, i64 intKey,
  const u8 *pKey, int nKey, const u8 *pVal, int nVal,
  const char *zInfoJson
){
  ConstraintViolationTable *pT;
  ConstraintViolationRow *pRow, *aNew;
  int rc;

  pT = findOrCreateViolationTable(paTables, pnTables, zTable);
  if( !pT ) return SQLITE_NOMEM;

  aNew = sqlite3_realloc(pT->aRows,
      (pT->nRows + 1) * (int)sizeof(ConstraintViolationRow));
  if( !aNew ) return SQLITE_NOMEM;
  pT->aRows = aNew;
  pRow = &pT->aRows[pT->nRows];
  memset(pRow, 0, sizeof(*pRow));

  pRow->violationType = violationType;
  pRow->intKey = intKey;
  rc = doltliteDupBytes(pKey, nKey, &pRow->pKey);
  if( rc==SQLITE_OK ){ pRow->nKey = nKey; rc = doltliteDupBytes(pVal, nVal, &pRow->pVal); }
  if( rc==SQLITE_OK && pVal ) pRow->nVal = nVal;
  if( rc==SQLITE_OK && zInfoJson ){
    pRow->zInfo = sqlite3_mprintf("%s", zInfoJson);
    if( !pRow->zInfo ) rc = SQLITE_NOMEM;
  }
  if( rc!=SQLITE_OK ){
    freeViolationRow(pRow);
    return rc;
  }
  pT->nRows++;
  return SQLITE_OK;
}

typedef struct ConstraintViolationBatch ConstraintViolationBatch;
struct ConstraintViolationBatch {
  ConstraintViolationTable *aTables;
  int nTables;
  int nAppended;   /* stays 0 when no violation was added, matching the
                   ** non-batch path, which never stores in that case */
};

int doltliteConstraintViolationBatchBegin(sqlite3 *db){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ConstraintViolationBatch *pBatch;
  int rc;
  if( !cs ) return SQLITE_ERROR;
  if( doltliteGetCvBatch(db) ) return SQLITE_MISUSE;  /* not nestable */
  pBatch = sqlite3_malloc(sizeof(*pBatch));
  if( !pBatch ) return SQLITE_NOMEM;
  memset(pBatch, 0, sizeof(*pBatch));
  rc = loadAllViolations(db, cs, &pBatch->aTables, &pBatch->nTables);
  if( rc!=SQLITE_OK ){ sqlite3_free(pBatch); return rc; }
  doltliteSetCvBatch(db, pBatch);
  return SQLITE_OK;
}

int doltliteConstraintViolationBatchEnd(sqlite3 *db, int commit){
  ConstraintViolationBatch *pBatch = (ConstraintViolationBatch*)doltliteGetCvBatch(db);
  int rc = SQLITE_OK;
  if( !pBatch ) return SQLITE_OK;
  doltliteSetCvBatch(db, 0);
  if( commit && pBatch->nAppended>0 ){
    ChunkStore *cs = doltliteGetChunkStore(db);
    rc = cs ? storeUpdatedViolations(db, cs, pBatch->aTables, pBatch->nTables)
            : SQLITE_ERROR;
  }
  freeViolationTables(pBatch->aTables, pBatch->nTables);
  sqlite3_free(pBatch);
  return rc;
}

int doltliteAppendConstraintViolation(
  sqlite3 *db,
  const char *zTable,
  u8 violationType,
  i64 intKey,
  const u8 *pKey, int nKey,
  const u8 *pVal, int nVal,
  const char *zInfoJson
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ConstraintViolationBatch *pBatch;
  ConstraintViolationTable *aTables = 0;
  int nTables = 0;
  int rc;

  if( !cs || !zTable ) return SQLITE_ERROR;

  /* Inside a batch, accumulate in memory; the single load and store happen
  ** at Begin/End, turning a per-violation O(total) round-trip into O(1). */
  pBatch = (ConstraintViolationBatch*)doltliteGetCvBatch(db);
  if( pBatch ){
    rc = cvAppendRow(&pBatch->aTables, &pBatch->nTables, zTable,
                     violationType, intKey, pKey, nKey, pVal, nVal, zInfoJson);
    if( rc==SQLITE_OK ) pBatch->nAppended++;
    return rc;
  }

  rc = loadAllViolations(db, cs, &aTables, &nTables);
  if( rc!=SQLITE_OK ) return rc;
  rc = cvAppendRow(&aTables, &nTables, zTable, violationType, intKey,
                   pKey, nKey, pVal, nVal, zInfoJson);
  if( rc==SQLITE_OK ){
    rc = storeUpdatedViolations(db, cs, aTables, nTables);
  }
  freeViolationTables(aTables, nTables);
  return rc;
}

int doltliteClearAllConstraintViolations(sqlite3 *db){
  static const ProllyHash emptyHash = {{0}};
  doltliteSetSessionConstraintViolationsCatalog(db, &emptyHash);
  if( doltliteVcTxnMode(db)==DOLTLITE_VC_TXN_AUTOCOMMIT_LIKE ){
    return doltlitePersistWorkingSet(db);
  }
  return doltliteSaveWorkingSet(db);
}

typedef struct CvSumVtab CvSumVtab;
struct CvSumVtab { sqlite3_vtab base; sqlite3 *db; };
typedef struct CvSumCur CvSumCur;
struct CvSumCur {
  sqlite3_vtab_cursor base;
  ConstraintViolationTable *aTables;
  int nTables;
  int iRow;
};

static int cvsConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  CvSumVtab *v;
  int rc;
  (void)pAux;(void)argc;(void)argv;(void)pzErr;
  rc = doltliteVtabConnectSimple(db,
      "CREATE TABLE x(\"table\" TEXT, num_violations INTEGER)",
      sizeof(*v), ppVtab);
  if( rc!=SQLITE_OK ) return rc;
  v = (CvSumVtab*)*ppVtab;
  v->db = db;
  return SQLITE_OK;
}
static int cvsOpen(sqlite3_vtab *v, sqlite3_vtab_cursor **pp){
  (void)v;
  return doltliteVtabOpenCursor(pp, sizeof(CvSumCur));
}
static int cvsClose(sqlite3_vtab_cursor *cur){
  CvSumCur *c = (CvSumCur*)cur;
  freeViolationTables(c->aTables, c->nTables);
  sqlite3_free(c);
  return SQLITE_OK;
}
static int cvsFilter(sqlite3_vtab_cursor *cur, int n, const char *s, int a, sqlite3_value **v){
  CvSumCur *c = (CvSumCur*)cur;
  CvSumVtab *vt = (CvSumVtab*)cur->pVtab;
  (void)n;(void)s;(void)a;(void)v;
  freeViolationTables(c->aTables, c->nTables);
  c->aTables = 0;
  c->nTables = 0;
  c->iRow = 0;
  return loadAllViolations(vt->db, doltliteGetChunkStore(vt->db),
                           &c->aTables, &c->nTables);
}
static int cvsNext(sqlite3_vtab_cursor *cur){ ((CvSumCur*)cur)->iRow++; return SQLITE_OK; }
static int cvsEof(sqlite3_vtab_cursor *cur){
  CvSumCur *c = (CvSumCur*)cur;
  return c->iRow >= c->nTables;
}
static int cvsColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int col){
  CvSumCur *c = (CvSumCur*)cur;
  switch( col ){
    case 0:
      sqlite3_result_text(ctx, c->aTables[c->iRow].zName, -1, SQLITE_TRANSIENT);
      break;
    case 1:
      sqlite3_result_int(ctx, c->aTables[c->iRow].nRows);
      break;
  }
  return SQLITE_OK;
}
static int cvsRowid(sqlite3_vtab_cursor *cur, sqlite3_int64 *r){
  *r = ((CvSumCur*)cur)->iRow;
  return SQLITE_OK;
}
static int cvsBestIndex(sqlite3_vtab *v, sqlite3_index_info *p){
  (void)v;
  p->estimatedCost = 10;
  return SQLITE_OK;
}

static sqlite3_module cvSummaryModule = {
  0, 0, cvsConnect, cvsBestIndex, doltliteVtabDisconnect, 0, cvsOpen, cvsClose,
  cvsFilter, cvsNext, cvsEof, cvsColumn, cvsRowid,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};

typedef struct CvRowVtab CvRowVtab;
struct CvRowVtab {
  sqlite3_vtab base;
  sqlite3 *db;
  char *zTableName;
  DoltliteColInfo cols;
};

typedef struct CvRowCur CvRowCur;
struct CvRowCur {
  sqlite3_vtab_cursor base;
  ConstraintViolationTable table;
  int hasTable;
  int iRow;
};

static char *cvrBuildSchema(const DoltliteColInfo *ci){
  sqlite3_str *pStr = sqlite3_str_new(0);
  char *z;
  if( !pStr ) return 0;
  sqlite3_str_appendall(pStr, "CREATE TABLE x(violation_type TEXT");
  if( ci->nCol>0 ){
    sqlite3_str_appendall(pStr, ", ");
    if( doltliteAppendQuotedColumnList(pStr, ci->azName, ci->nCol, 0, 0)!=SQLITE_OK ){
      sqlite3_str_reset(pStr);
      return 0;
    }
  }
  sqlite3_str_appendall(pStr, ", violation_info TEXT)");
  z = sqlite3_str_finish(pStr);
  return z;
}

static int cvrConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  (void)pAux;
  return doltliteVtabConnectUserTable(db, argc, argv,
                                      "dolt_constraint_violations_",
                                      sizeof(CvRowVtab), cvrBuildSchema,
                                      ppVtab, pzErr);
}

static int cvrOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **pp){
  (void)pVtab;
  if( doltliteVtabOpenCursor(pp, sizeof(CvRowCur))!=SQLITE_OK ){
    return SQLITE_NOMEM;
  }
  return SQLITE_OK;
}

static int cvrClose(sqlite3_vtab_cursor *cur){
  CvRowCur *c = (CvRowCur*)cur;
  freeViolationTable(&c->table);
  sqlite3_free(c);
  return SQLITE_OK;
}

static int cvrFilter(sqlite3_vtab_cursor *cur, int n, const char *s, int a, sqlite3_value **vp){
  CvRowCur *c = (CvRowCur*)cur;
  CvRowVtab *v = (CvRowVtab*)cur->pVtab;
  int rc;
  (void)n;(void)s;(void)a;(void)vp;

  freeViolationTable(&c->table);
  c->iRow = 0;
  c->hasTable = 0;
  rc = loadViolationTable(v->db, doltliteGetChunkStore(v->db),
                          v->zTableName, &c->table, &c->hasTable);
  return rc;
}

static int cvrNext(sqlite3_vtab_cursor *cur){
  ((CvRowCur*)cur)->iRow++;
  return SQLITE_OK;
}

static int cvrEof(sqlite3_vtab_cursor *cur){
  CvRowCur *c = (CvRowCur*)cur;
  return !c->hasTable || c->iRow >= c->table.nRows;
}

static const char *violationTypeName(u8 t){
  switch( t ){
    case DOLTLITE_CV_FOREIGN_KEY: return "foreign key";
    case DOLTLITE_CV_UNIQUE_INDEX: return "unique index";
    case DOLTLITE_CV_CHECK_CONSTRAINT: return "check constraint";
    default: return "unknown";
  }
}

static int cvrColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int col){
  CvRowCur *c = (CvRowCur*)cur;
  CvRowVtab *v = (CvRowVtab*)cur->pVtab;
  ConstraintViolationRow *r;
  int nUserCols;

  if( !c->hasTable ) return SQLITE_OK;
  if( c->iRow >= c->table.nRows ) return SQLITE_OK;
  r = &c->table.aRows[c->iRow];

  nUserCols = v->cols.nCol;

  if( col==0 ){
    sqlite3_result_text(ctx, violationTypeName(r->violationType), -1, SQLITE_STATIC);
  }else if( col >= 1 && col <= nUserCols ){
    doltliteResultUserCol(ctx, &v->cols, r->pVal, r->nVal, r->intKey, col - 1);
  }else if( col == nUserCols + 1 ){
    sqlite3_result_text(ctx, r->zInfo ? r->zInfo : "", -1, SQLITE_TRANSIENT);
  }else{
    sqlite3_result_null(ctx);
  }
  return SQLITE_OK;
}

static sqlite3_int64 cvrViolationRowid(const ConstraintViolationRow *r){
  u64 h = DOLTLITE_FNV1A_OFFSET;
  h = doltliteFnv1aBytes(h, r->pKey, r->nKey);
  h = doltliteFnv1aSep(h);
  h = doltliteFnv1aI64(h, r->intKey);
  h = doltliteFnv1aSep(h);
  h = doltliteFnv1aBytes(h, r->pVal, r->nVal);
  h = doltliteFnv1aSep(h);
  h ^= (u64)r->violationType;
  h *= DOLTLITE_FNV1A_PRIME;
  h = doltliteFnv1aSep(h);
  h = doltliteFnv1aStr(h, r->zInfo);
  return (sqlite3_int64)(h & 0x7fffffffffffffffULL);
}

static int cvrRowid(sqlite3_vtab_cursor *cur, sqlite3_int64 *r){
  CvRowCur *c = (CvRowCur*)cur;
  if( c->hasTable && c->iRow < c->table.nRows ){
    *r = cvrViolationRowid(&c->table.aRows[c->iRow]);
  }else{
    *r = 0;
  }
  return SQLITE_OK;
}

static int cvrBestIndex(sqlite3_vtab *v, sqlite3_index_info *p){
  (void)v;
  p->estimatedCost = 10;
  return SQLITE_OK;
}

static int cvrUpdate(
  sqlite3_vtab *pVtab,
  int nArg,
  sqlite3_value **apArg,
  sqlite3_int64 *pRowid
){
  CvRowVtab *v = (CvRowVtab*)pVtab;
  ChunkStore *cs = doltliteGetChunkStore(v->db);
  i64 deleteRowid;
  (void)pRowid;

  if( nArg != 1 ){
    pVtab->zErrMsg = sqlite3_mprintf(
        "only DELETE is supported on dolt_constraint_violations_<table>");
    return SQLITE_ERROR;
  }
  deleteRowid = sqlite3_value_int64(apArg[0]);
  return deleteViolationRowFromCatalog(v->db, cs, v->zTableName, deleteRowid);
}

static sqlite3_module cvRowModule = {
  0,
  cvrConnect,
  cvrConnect,
  cvrBestIndex,
  doltliteVtabCommonDisconnect,
  doltliteVtabCommonDisconnect,
  cvrOpen,
  cvrClose,
  cvrFilter,
  cvrNext,
  cvrEof,
  cvrColumn,
  cvrRowid,
  cvrUpdate,
  0,0,0,0,0,0,0,0,0,0,0
};

int doltliteRefreshConstraintViolationTables(sqlite3 *db){
  return doltliteForEachUserTable(db, "dolt_constraint_violations_", &cvRowModule);
}

int doltliteConstraintViolationsRegister(sqlite3 *db){
  int rc;
  rc = sqlite3_create_module(db, "dolt_constraint_violations",
                             &cvSummaryModule, 0);
  if( rc==SQLITE_OK ){
    rc = doltliteRefreshConstraintViolationTables(db);
  }
  return rc;
}

#endif
