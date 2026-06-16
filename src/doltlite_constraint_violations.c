
#ifdef DOLTLITE_PROLLY

#include "doltlite_vtab_util.h"
#include "doltlite_internal.h"
#include "doltlite_constraint_violations.h"

#include <string.h>

static void freeViolationRow(ConstraintViolationRow *r){
  if( !r ) return;
  sqlite3_free(r->pKey);
  sqlite3_free(r->pVal);
  sqlite3_free(r->zInfo);
  memset(r, 0, sizeof(*r));
}

static int dupBytes(const u8 *pIn, int nIn, u8 **ppOut){
  u8 *pCopy;
  *ppOut = 0;
  if( !pIn || nIn<=0 ) return SQLITE_OK;
  pCopy = sqlite3_malloc(nIn);
  if( !pCopy ) return SQLITE_NOMEM;
  memcpy(pCopy, pIn, nIn);
  *ppOut = pCopy;
  return SQLITE_OK;
}

static void freeViolationTables(ConstraintViolationTable *a, int n){
  int i, j;
  if( !a ) return;
  for(i=0; i<n; i++){
    for(j=0; j<a[i].nRows; j++){
      freeViolationRow(&a[i].aRows[j]);
    }
    sqlite3_free(a[i].aRows);
    sqlite3_free(a[i].zName);
  }
  sqlite3_free(a);
}

static void removeViolationRow(ConstraintViolationTable *pTable, int iRow){
  if( !pTable || iRow<0 || iRow>=pTable->nRows ) return;
  freeViolationRow(&pTable->aRows[iRow]);
  doltliteArrayRemoveAt(pTable->aRows, &pTable->nRows, iRow,
                        (int)sizeof(ConstraintViolationRow));
}

static void removeViolationTable(ConstraintViolationTable *a, int *pn, int iTable){
  int j;
  if( !a || !pn || iTable<0 || iTable>=*pn ) return;
  sqlite3_free(a[iTable].zName);
  for(j=0; j<a[iTable].nRows; j++){
    freeViolationRow(&a[iTable].aRows[j]);
  }
  sqlite3_free(a[iTable].aRows);
  doltliteArrayRemoveAt(a, pn, iTable, (int)sizeof(ConstraintViolationTable));
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

  dlWriteU8(&w, DCV_MAGIC0);
  dlWriteU8(&w, DCV_MAGIC1);
  dlWriteU8(&w, DCV_MAGIC2);
  dlWriteU8(&w, DCV_VERSION);
  dlWriteU16(&w, nTables);

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
  if( dlReadU8(&rd)!=DCV_MAGIC0
   || dlReadU8(&rd)!=DCV_MAGIC1
   || dlReadU8(&rd)!=DCV_MAGIC2
   || dlReadU8(&rd)!=DCV_VERSION ){
    sqlite3_free(data);
    return SQLITE_CORRUPT;
  }
  nTables = dlReadU16(&rd);
  if( nTables<0 ){ sqlite3_free(data); return SQLITE_CORRUPT; }

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
      r->violationType = dlReadU8(&rd);
      rc = dlReadU32Blob(&rd, &r->pKey, &r->nKey);
      if( rc!=SQLITE_OK ) goto fail;
      r->intKey = dlReadI64(&rd);
      rc = dlReadU32Blob(&rd, &r->pVal, &r->nVal);
      if( rc!=SQLITE_OK ) goto fail;
      rc = dlReadU32Str(&rd, &r->zInfo);
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
  ConstraintViolationTable *aTables = 0;
  int nTables = 0;
  ConstraintViolationTable *pT;
  ConstraintViolationRow *pRow;
  int rc;
  ConstraintViolationRow *aNew;

  if( !cs || !zTable ) return SQLITE_ERROR;

  rc = loadAllViolations(db, cs, &aTables, &nTables);
  if( rc!=SQLITE_OK ) return rc;

  pT = findOrCreateViolationTable(&aTables, &nTables, zTable);
  if( !pT ){ freeViolationTables(aTables, nTables); return SQLITE_NOMEM; }

  aNew = sqlite3_realloc(pT->aRows,
      (pT->nRows + 1) * (int)sizeof(ConstraintViolationRow));
  if( !aNew ){ freeViolationTables(aTables, nTables); return SQLITE_NOMEM; }
  pT->aRows = aNew;
  pRow = &pT->aRows[pT->nRows];
  memset(pRow, 0, sizeof(*pRow));

  pRow->violationType = violationType;
  pRow->intKey = intKey;
  rc = dupBytes(pKey, nKey, &pRow->pKey);
  if( rc==SQLITE_OK ){ pRow->nKey = nKey; rc = dupBytes(pVal, nVal, &pRow->pVal); }
  if( rc==SQLITE_OK && pVal ) pRow->nVal = nVal;
  if( rc==SQLITE_OK && zInfoJson ){
    pRow->zInfo = sqlite3_mprintf("%s", zInfoJson);
    if( !pRow->zInfo ) rc = SQLITE_NOMEM;
  }
  if( rc!=SQLITE_OK ){
    freeViolationRow(pRow);
    freeViolationTables(aTables, nTables);
    return rc;
  }
  pT->nRows++;

  rc = storeUpdatedViolations(db, cs, aTables, nTables);
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
  rc = sqlite3_declare_vtab(db,
      "CREATE TABLE x(\"table\" TEXT, num_violations INTEGER)");
  if( rc!=SQLITE_OK ) return rc;
  v = sqlite3_malloc(sizeof(*v));
  if( !v ) return SQLITE_NOMEM;
  memset(v, 0, sizeof(*v));
  v->db = db;
  *ppVtab = &v->base;
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
  ConstraintViolationTable *aTables;
  int nTables;
  int iTableIdx;
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
  CvRowCur *c;
  (void)pVtab;
  if( doltliteVtabOpenCursor(pp, sizeof(CvRowCur))!=SQLITE_OK ){
    return SQLITE_NOMEM;
  }
  c = (CvRowCur*)*pp;
  c->iTableIdx = -1;
  return SQLITE_OK;
}

static int cvrClose(sqlite3_vtab_cursor *cur){
  CvRowCur *c = (CvRowCur*)cur;
  freeViolationTables(c->aTables, c->nTables);
  sqlite3_free(c);
  return SQLITE_OK;
}

static int cvrFilter(sqlite3_vtab_cursor *cur, int n, const char *s, int a, sqlite3_value **vp){
  CvRowCur *c = (CvRowCur*)cur;
  CvRowVtab *v = (CvRowVtab*)cur->pVtab;
  int i, rc;
  (void)n;(void)s;(void)a;(void)vp;

  freeViolationTables(c->aTables, c->nTables);
  c->aTables = 0;
  c->nTables = 0;
  c->iRow = 0;
  c->iTableIdx = -1;
  rc = loadAllViolations(v->db, doltliteGetChunkStore(v->db),
                         &c->aTables, &c->nTables);
  if( rc!=SQLITE_OK ) return rc;

  for(i=0; i<c->nTables; i++){
    if( c->aTables[i].zName
     && strcmp(c->aTables[i].zName, v->zTableName)==0 ){
      c->iTableIdx = i;
      break;
    }
  }
  return SQLITE_OK;
}

static int cvrNext(sqlite3_vtab_cursor *cur){
  ((CvRowCur*)cur)->iRow++;
  return SQLITE_OK;
}

static int cvrEof(sqlite3_vtab_cursor *cur){
  CvRowCur *c = (CvRowCur*)cur;
  if( c->iTableIdx < 0 ) return 1;
  return c->iRow >= c->aTables[c->iTableIdx].nRows;
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

  if( c->iTableIdx < 0 ) return SQLITE_OK;
  if( c->iRow >= c->aTables[c->iTableIdx].nRows ) return SQLITE_OK;
  r = &c->aTables[c->iTableIdx].aRows[c->iRow];

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
  if( c->iTableIdx >= 0 && c->iRow < c->aTables[c->iTableIdx].nRows ){
    *r = cvrViolationRowid(&c->aTables[c->iTableIdx].aRows[c->iRow]);
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
  ConstraintViolationTable *aTables = 0;
  int nTables = 0;
  int i, j, rc;
  i64 deleteRowid;
  (void)pRowid;

  if( nArg != 1 ){
    pVtab->zErrMsg = sqlite3_mprintf(
        "only DELETE is supported on dolt_constraint_violations_<table>");
    return SQLITE_ERROR;
  }
  deleteRowid = sqlite3_value_int64(apArg[0]);

  rc = loadAllViolations(v->db, cs, &aTables, &nTables);
  if( rc!=SQLITE_OK ) return rc;

  for(i=0; i<nTables; i++){
    if( !aTables[i].zName
     || strcmp(aTables[i].zName, v->zTableName)!=0 ) continue;
    for(j=0; j<aTables[i].nRows; j++){
      if( cvrViolationRowid(&aTables[i].aRows[j]) == deleteRowid ){
        removeViolationRow(&aTables[i], j);
        if( aTables[i].nRows == 0 ){
          removeViolationTable(aTables, &nTables, i);
        }
        rc = storeUpdatedViolations(v->db, cs, aTables, nTables);
        freeViolationTables(aTables, nTables);
        return rc;
      }
    }
    break;
  }

  freeViolationTables(aTables, nTables);
  return SQLITE_OK;
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
