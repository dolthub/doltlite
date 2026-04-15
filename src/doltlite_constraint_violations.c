
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_record.h"
#include "doltlite_internal.h"
#include "doltlite_constraint_violations.h"

#include <string.h>

/* In-memory view of a single violation row. Mirrors the shape of
** a ConflictRow but without the base/our/their distinction —
** violations come from ONE side of the merge or from a post-merge
** walk, so there's a single value payload. violation_type picks
** the namespace (FK / unique / check) and violation_info is a
** free-form JSON string describing the specific constraint. */
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
  if( iRow < pTable->nRows - 1 ){
    memmove(&pTable->aRows[iRow], &pTable->aRows[iRow+1],
            (pTable->nRows - iRow - 1) * sizeof(ConstraintViolationRow));
  }
  pTable->nRows--;
  if( pTable->aRows ){
    memset(&pTable->aRows[pTable->nRows], 0, sizeof(ConstraintViolationRow));
  }
}

static void removeViolationTable(ConstraintViolationTable *a, int *pn, int iTable){
  int n;
  if( !a || !pn ) return;
  n = *pn;
  if( iTable<0 || iTable>=n ) return;
  sqlite3_free(a[iTable].zName);
  {
    int j;
    for(j=0; j<a[iTable].nRows; j++){
      freeViolationRow(&a[iTable].aRows[j]);
    }
  }
  sqlite3_free(a[iTable].aRows);
  memset(&a[iTable], 0, sizeof(a[iTable]));
  if( iTable < n - 1 ){
    memmove(&a[iTable], &a[iTable+1],
            (n - iTable - 1) * sizeof(ConstraintViolationTable));
  }
  (*pn)--;
  memset(&a[*pn], 0, sizeof(a[*pn]));
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
  u8 *buf, *p;

  for(i=0; i<nTables; i++){
    int nl = aTables[i].zName ? (int)strlen(aTables[i].zName) : 0;
    sz += 2 + nl + 4;
    for(j=0; j<aTables[i].nRows; j++){
      int ni = aTables[i].aRows[j].zInfo
             ? (int)strlen(aTables[i].aRows[j].zInfo) : 0;
      sz += 1          /* violation_type */
          + 4 + aTables[i].aRows[j].nKey
          + 8          /* intKey */
          + 4 + aTables[i].aRows[j].nVal
          + 4 + ni;
    }
  }

  buf = sqlite3_malloc(sz);
  if( !buf ) return SQLITE_NOMEM;
  p = buf;

  p[0] = DCV_MAGIC0;
  p[1] = DCV_MAGIC1;
  p[2] = DCV_MAGIC2;
  p[3] = DCV_VERSION;
  p += 4;
  p[0] = (u8)nTables; p[1] = (u8)(nTables>>8); p += 2;

  for(i=0; i<nTables; i++){
    int nl = aTables[i].zName ? (int)strlen(aTables[i].zName) : 0;
    int nr = aTables[i].nRows;
    p[0] = (u8)nl; p[1] = (u8)(nl>>8); p += 2;
    if( nl>0 ){ memcpy(p, aTables[i].zName, nl); p += nl; }
    p[0]=(u8)nr; p[1]=(u8)(nr>>8); p[2]=(u8)(nr>>16); p[3]=(u8)(nr>>24); p += 4;
    for(j=0; j<nr; j++){
      ConstraintViolationRow *r = &aTables[i].aRows[j];
      int ni = r->zInfo ? (int)strlen(r->zInfo) : 0;

      *p++ = (u8)r->violationType;

      p[0]=(u8)r->nKey; p[1]=(u8)(r->nKey>>8);
      p[2]=(u8)(r->nKey>>16); p[3]=(u8)(r->nKey>>24); p+=4;
      if( r->nKey>0 ){ memcpy(p, r->pKey, r->nKey); p += r->nKey; }

      {
        u64 k = (u64)r->intKey;
        p[0]=(u8)k;      p[1]=(u8)(k>>8);
        p[2]=(u8)(k>>16); p[3]=(u8)(k>>24);
        p[4]=(u8)(k>>32); p[5]=(u8)(k>>40);
        p[6]=(u8)(k>>48); p[7]=(u8)(k>>56);
        p += 8;
      }

      p[0]=(u8)r->nVal; p[1]=(u8)(r->nVal>>8);
      p[2]=(u8)(r->nVal>>16); p[3]=(u8)(r->nVal>>24); p+=4;
      if( r->nVal>0 ){ memcpy(p, r->pVal, r->nVal); p += r->nVal; }

      p[0]=(u8)ni; p[1]=(u8)(ni>>8);
      p[2]=(u8)(ni>>16); p[3]=(u8)(ni>>24); p+=4;
      if( ni>0 ){ memcpy(p, r->zInfo, ni); p += ni; }
    }
  }

  rc = chunkStorePut(cs, buf, (int)(p-buf), pHash);
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
  const u8 *p;
  int nTables, i, j, rc;
  ConstraintViolationTable *aTables;

  *ppTables = 0;
  *pnTables = 0;

  doltliteGetSessionConstraintViolationsCatalog(db, &hash);
  if( prollyHashIsEmpty(&hash) ) return SQLITE_OK;

  rc = chunkStoreGet(cs, &hash, &data, &nData);
  if( rc!=SQLITE_OK ) return rc;
  if( nData<(4+2) ){ sqlite3_free(data); return SQLITE_CORRUPT; }

  p = data;
  if( p[0]!=DCV_MAGIC0
   || p[1]!=DCV_MAGIC1
   || p[2]!=DCV_MAGIC2
   || p[3]!=DCV_VERSION ){
    sqlite3_free(data);
    return SQLITE_CORRUPT;
  }
  p += 4;
  nTables = p[0] | (p[1]<<8); p += 2;
  if( nTables<0 ){ sqlite3_free(data); return SQLITE_CORRUPT; }

  aTables = sqlite3_malloc(nTables ? nTables * (int)sizeof(*aTables) : 1);
  if( !aTables ){ sqlite3_free(data); return SQLITE_NOMEM; }
  memset(aTables, 0, nTables ? nTables * (int)sizeof(*aTables) : 1);

  for(i=0; i<nTables; i++){
    int nl, nr;
    if( p+2 > data+nData ){ rc = SQLITE_CORRUPT; goto fail; }
    nl = p[0] | (p[1]<<8); p += 2;
    if( nl<0 || p+nl > data+nData ){ rc = SQLITE_CORRUPT; goto fail; }
    aTables[i].zName = sqlite3_malloc(nl+1);
    if( !aTables[i].zName ){ rc = SQLITE_NOMEM; goto fail; }
    memcpy(aTables[i].zName, p, nl); aTables[i].zName[nl] = 0;
    p += nl;
    if( p+4 > data+nData ){ rc = SQLITE_CORRUPT; goto fail; }
    nr = p[0]|(p[1]<<8)|(p[2]<<16)|(p[3]<<24); p+=4;
    if( nr<0 ){ rc = SQLITE_CORRUPT; goto fail; }
    aTables[i].nRows = nr;
    aTables[i].aRows = sqlite3_malloc(nr ? nr * (int)sizeof(ConstraintViolationRow) : 1);
    if( !aTables[i].aRows ){ rc = SQLITE_NOMEM; goto fail; }
    memset(aTables[i].aRows, 0, nr ? nr * (int)sizeof(ConstraintViolationRow) : 1);

    for(j=0; j<nr; j++){
      ConstraintViolationRow *r = &aTables[i].aRows[j];
      int kvl, vvl, nil_;
      if( p+1 > data+nData ){ rc = SQLITE_CORRUPT; goto fail; }
      r->violationType = *p++;
      if( p+4 > data+nData ){ rc = SQLITE_CORRUPT; goto fail; }
      kvl = p[0]|(p[1]<<8)|(p[2]<<16)|(p[3]<<24); p+=4;
      if( kvl<0 || p+kvl > data+nData ){ rc = SQLITE_CORRUPT; goto fail; }
      if( kvl>0 ){
        rc = dupBytes(p, kvl, &r->pKey);
        if( rc!=SQLITE_OK ) goto fail;
        r->nKey = kvl;
      }
      p += kvl;
      if( p+8 > data+nData ){ rc = SQLITE_CORRUPT; goto fail; }
      r->intKey = (i64)((u64)p[0] | ((u64)p[1]<<8) | ((u64)p[2]<<16) |
                        ((u64)p[3]<<24) | ((u64)p[4]<<32) | ((u64)p[5]<<40) |
                        ((u64)p[6]<<48) | ((u64)p[7]<<56));
      p += 8;
      if( p+4 > data+nData ){ rc = SQLITE_CORRUPT; goto fail; }
      vvl = p[0]|(p[1]<<8)|(p[2]<<16)|(p[3]<<24); p+=4;
      if( vvl<0 || p+vvl > data+nData ){ rc = SQLITE_CORRUPT; goto fail; }
      if( vvl>0 ){
        rc = dupBytes(p, vvl, &r->pVal);
        if( rc!=SQLITE_OK ) goto fail;
        r->nVal = vvl;
      }
      p += vvl;
      if( p+4 > data+nData ){ rc = SQLITE_CORRUPT; goto fail; }
      nil_ = p[0]|(p[1]<<8)|(p[2]<<16)|(p[3]<<24); p+=4;
      if( nil_<0 || p+nil_ > data+nData ){ rc = SQLITE_CORRUPT; goto fail; }
      if( nil_>0 ){
        r->zInfo = sqlite3_malloc(nil_+1);
        if( !r->zInfo ){ rc = SQLITE_NOMEM; goto fail; }
        memcpy(r->zInfo, p, nil_);
        r->zInfo[nil_] = 0;
        p += nil_;
      }
    }
  }

  if( p != data+nData ){ rc = SQLITE_CORRUPT; goto fail; }

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
  return doltlitePersistWorkingSet(db);
}

/* Public append API — used by the post-merge walk (Phase 4) to
** record each detected violation. Copies the caller's bytes so
** the caller can release its own buffers immediately. */
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
  return doltlitePersistWorkingSet(db);
}

/* ── Summary vtable: dolt_constraint_violations ──────────── */

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
static int cvsDisconnect(sqlite3_vtab *v){ sqlite3_free(v); return SQLITE_OK; }
static int cvsOpen(sqlite3_vtab *v, sqlite3_vtab_cursor **pp){
  CvSumCur *c = sqlite3_malloc(sizeof(*c));
  (void)v;
  if( !c ) return SQLITE_NOMEM;
  memset(c, 0, sizeof(*c));
  *pp = &c->base;
  return SQLITE_OK;
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
  0, 0, cvsConnect, cvsBestIndex, cvsDisconnect, 0, cvsOpen, cvsClose,
  cvsFilter, cvsNext, cvsEof, cvsColumn, cvsRowid,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};

/* ── Per-table vtable: dolt_constraint_violations_<table> ─ */

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

/* Per-table schema: violation_type TEXT, <user PK+value cols>,
** violation_info TEXT. The user columns keep their original
** declared names so (violation_type, pk, v1, v2, violation_info)
** is the row shape. */
static char *cvrBuildSchema(const DoltliteColInfo *ci){
  sqlite3_str *pStr = sqlite3_str_new(0);
  int i;
  char *z;
  if( !pStr ) return 0;
  sqlite3_str_appendall(pStr, "CREATE TABLE x(violation_type TEXT");
  for(i=0; i<ci->nCol; i++){
    sqlite3_str_appendf(pStr, ", \"%w\"", ci->azName[i]);
  }
  sqlite3_str_appendall(pStr, ", violation_info TEXT)");
  z = sqlite3_str_finish(pStr);
  return z;
}

static int cvrConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  CvRowVtab *v;
  int rc;
  const char *zMod;
  char *zSchema;
  (void)pAux;

  v = sqlite3_malloc(sizeof(*v));
  if( !v ) return SQLITE_NOMEM;
  memset(v, 0, sizeof(*v));
  v->db = db;

  zMod = argv[0];
  if( zMod && strncmp(zMod, "dolt_constraint_violations_", 27)==0 ){
    v->zTableName = sqlite3_mprintf("%s", zMod + 27);
  }else if( argc > 3 ){
    v->zTableName = sqlite3_mprintf("%s", argv[3]);
  }else{
    v->zTableName = sqlite3_mprintf("");
  }
  if( !v->zTableName ){ sqlite3_free(v); return SQLITE_NOMEM; }

  rc = doltliteLoadUserTableColumns(db, v->zTableName, &v->cols, pzErr);
  if( rc!=SQLITE_OK ){
    sqlite3_free(v->zTableName);
    doltliteFreeColInfo(&v->cols);
    sqlite3_free(v);
    return rc;
  }

  zSchema = cvrBuildSchema(&v->cols);
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

static int cvrDisconnect(sqlite3_vtab *pVtab){
  CvRowVtab *v = (CvRowVtab*)pVtab;
  sqlite3_free(v->zTableName);
  doltliteFreeColInfo(&v->cols);
  sqlite3_free(v);
  return SQLITE_OK;
}

static int cvrOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **pp){
  CvRowCur *c = sqlite3_malloc(sizeof(*c));
  (void)pVtab;
  if( !c ) return SQLITE_NOMEM;
  memset(c, 0, sizeof(*c));
  c->iTableIdx = -1;
  *pp = &c->base;
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

static int cvrRowid(sqlite3_vtab_cursor *cur, sqlite3_int64 *r){
  CvRowCur *c = (CvRowCur*)cur;
  if( c->iTableIdx >= 0 && c->iRow < c->aTables[c->iTableIdx].nRows ){
    *r = c->aTables[c->iTableIdx].aRows[c->iRow].intKey;
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

/* DELETE support: user clears a violation from the per-table
** vtable to signal "I've resolved this". Matches dolt_conflicts
** DELETE semantics — rowid comes from the row's intKey. */
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
      if( aTables[i].aRows[j].intKey == deleteRowid ){
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
  cvrDisconnect,
  cvrDisconnect,
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

static int doltliteRegisterConstraintViolationTables(sqlite3 *db){
  return doltliteForEachUserTable(db, "dolt_constraint_violations_", &cvRowModule);
}

int doltliteConstraintViolationsRegister(sqlite3 *db){
  int rc;
  rc = sqlite3_create_module(db, "dolt_constraint_violations",
                             &cvSummaryModule, 0);
  if( rc==SQLITE_OK ){
    rc = doltliteRegisterConstraintViolationTables(db);
  }
  return rc;
}

#endif
