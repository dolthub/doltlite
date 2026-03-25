/*
** dolt_diff_<tablename> — per-table audit log with Dolt-style column schema.
**
** Schema mirrors Dolt:
**   from_<col1>, from_<col2>, ..., to_<col1>, to_<col2>, ...,
**   from_commit, to_commit, from_commit_date, to_commit_date, diff_type
**
** Values are decoded from SQLite record format into native types.
*/
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_diff.h"
#include "prolly_cache.h"
#include "chunk_store.h"
#include "doltlite_commit.h"

#include <assert.h>
#include <string.h>
#include <time.h>

extern ChunkStore *doltliteGetChunkStore(sqlite3 *db);
extern void *doltliteGetBtShared(sqlite3 *db);
extern int doltliteGetHeadCatalogHash(sqlite3 *db, ProllyHash *pCatHash);
extern int doltliteFlushAndSerializeCatalog(sqlite3 *db, u8 **ppOut, int *pnOut);

struct TableEntry { Pgno iTable; ProllyHash root; ProllyHash schemaHash; u8 flags; char *zName; void *pPending; };
extern int doltliteLoadCatalog(sqlite3 *db, const ProllyHash *catHash,
                               struct TableEntry **ppTables, int *pnTables,
                               Pgno *piNextTable);

/* --------------------------------------------------------------------------
** Varint reader (big-endian SQLite format)
** -------------------------------------------------------------------------- */

static int dtReadVarint(const u8 *p, const u8 *pEnd, u64 *pVal){
  u64 v = 0;
  int i;
  for(i=0; i<9 && p+i<pEnd; i++){
    if( i<8 ){
      v = (v << 7) | (p[i] & 0x7f);
      if( (p[i] & 0x80)==0 ){ *pVal = v; return i+1; }
    }else{
      v = (v << 8) | p[i];
      *pVal = v;
      return 9;
    }
  }
  *pVal = v;
  return i ? i : 1;
}

/* --------------------------------------------------------------------------
** Parse record header to get serial types and body offsets
** -------------------------------------------------------------------------- */

#define DT_MAX_COLS 64

typedef struct RecordInfo RecordInfo;
struct RecordInfo {
  int nField;
  int aType[DT_MAX_COLS];
  int aOffset[DT_MAX_COLS];
  int bodyStart;
};

static void dtParseRecord(const u8 *pData, int nData, RecordInfo *pInfo){
  const u8 *p = pData;
  const u8 *pEnd = pData + nData;
  u64 hdrSize;
  int hdrBytes;
  const u8 *pHdrEnd;
  int off;

  memset(pInfo, 0, sizeof(*pInfo));
  if( !pData || nData < 1 ) return;

  hdrBytes = dtReadVarint(p, pEnd, &hdrSize);
  p += hdrBytes;
  pHdrEnd = pData + (int)hdrSize;
  off = (int)hdrSize;
  pInfo->bodyStart = off;

  while( p < pHdrEnd && p < pEnd && pInfo->nField < DT_MAX_COLS ){
    u64 st;
    int stBytes = dtReadVarint(p, pHdrEnd, &st);
    p += stBytes;

    pInfo->aType[pInfo->nField] = (int)st;
    pInfo->aOffset[pInfo->nField] = off;

    if( st==0 ) {}
    else if( st==1 ) off += 1;
    else if( st==2 ) off += 2;
    else if( st==3 ) off += 3;
    else if( st==4 ) off += 4;
    else if( st==5 ) off += 6;
    else if( st==6 ) off += 8;
    else if( st==7 ) off += 8;
    else if( st==8 || st==9 ) {}
    else if( st>=12 && (st&1)==0 ) off += ((int)st-12)/2;
    else if( st>=13 && (st&1)==1 ) off += ((int)st-13)/2;

    pInfo->nField++;
  }
}

/* Set a sqlite3_context result from a record field */
static void dtResultField(
  sqlite3_context *ctx,
  const u8 *pData, int nData,
  int fieldType, int fieldOffset
){
  int st = fieldType;

  if( st==0 ){ sqlite3_result_null(ctx); return; }
  if( st==8 ){ sqlite3_result_int(ctx, 0); return; }
  if( st==9 ){ sqlite3_result_int(ctx, 1); return; }

  if( st>=1 && st<=6 ){
    static const int sizes[] = {0,1,2,3,4,6,8};
    int nBytes = sizes[st];
    if( fieldOffset + nBytes <= nData ){
      const u8 *p = pData + fieldOffset;
      i64 v = (p[0] & 0x80) ? -1 : 0;
      int i;
      for(i=0; i<nBytes; i++) v = (v<<8) | p[i];
      sqlite3_result_int64(ctx, v);
    }else{
      sqlite3_result_null(ctx);
    }
    return;
  }

  if( st==7 ){
    if( fieldOffset + 8 <= nData ){
      const u8 *p = pData + fieldOffset;
      double v;
      u64 bits = 0;
      int i;
      for(i=0; i<8; i++) bits = (bits<<8) | p[i];
      memcpy(&v, &bits, 8);
      sqlite3_result_double(ctx, v);
    }else{
      sqlite3_result_null(ctx);
    }
    return;
  }

  if( st>=13 && (st&1)==1 ){
    int len = (st-13)/2;
    if( fieldOffset + len <= nData ){
      sqlite3_result_text(ctx, (const char*)(pData+fieldOffset), len, SQLITE_TRANSIENT);
    }else{
      sqlite3_result_null(ctx);
    }
    return;
  }

  if( st>=12 && (st&1)==0 ){
    int len = (st-12)/2;
    if( fieldOffset + len <= nData ){
      sqlite3_result_blob(ctx, pData+fieldOffset, len, SQLITE_TRANSIENT);
    }else{
      sqlite3_result_null(ctx);
    }
    return;
  }

  sqlite3_result_null(ctx);
}

/* --------------------------------------------------------------------------
** Column name extraction from sqlite_master
** -------------------------------------------------------------------------- */

typedef struct ColInfo ColInfo;
struct ColInfo {
  char **azName;    /* Column names (owned) */
  int nCol;         /* Number of columns (ALL columns including PK) */
  int iPkCol;       /* Index of the INTEGER PRIMARY KEY column, or -1 */
};

static void freeColInfo(ColInfo *ci){
  int i;
  for(i=0; i<ci->nCol; i++) sqlite3_free(ci->azName[i]);
  sqlite3_free(ci->azName);
  ci->azName = 0;
  ci->nCol = 0;
}

static int getColumnNames(sqlite3 *db, const char *zTable, ColInfo *ci){
  char *zSql;
  sqlite3_stmt *pStmt = 0;
  int rc, nCol;

  memset(ci, 0, sizeof(*ci));
  ci->iPkCol = -1;
  zSql = sqlite3_mprintf("PRAGMA table_info(\"%w\")", zTable);
  if( !zSql ) return SQLITE_NOMEM;

  rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  sqlite3_free(zSql);
  if( rc!=SQLITE_OK ) return rc;

  /* Count columns first */
  nCol = 0;
  while( sqlite3_step(pStmt)==SQLITE_ROW ) nCol++;
  sqlite3_reset(pStmt);

  ci->azName = sqlite3_malloc(nCol * (int)sizeof(char*));
  if( !ci->azName ){ sqlite3_finalize(pStmt); return SQLITE_NOMEM; }
  memset(ci->azName, 0, nCol * (int)sizeof(char*));
  ci->nCol = 0;

  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    const char *zName = (const char*)sqlite3_column_text(pStmt, 1);
    int pk = sqlite3_column_int(pStmt, 5);
    const char *zType = (const char*)sqlite3_column_text(pStmt, 2);

    /* Track which column is the INTEGER PRIMARY KEY (rowid alias) */
    if( pk==1 && zType && sqlite3_stricmp(zType,"INTEGER")==0 ){
      ci->iPkCol = ci->nCol;
    }

    ci->azName[ci->nCol] = sqlite3_mprintf("%s", zName ? zName : "");
    ci->nCol++;
  }

  sqlite3_finalize(pStmt);
  return SQLITE_OK;
}

/* --------------------------------------------------------------------------
** Build the virtual table schema dynamically from column names
** -------------------------------------------------------------------------- */

static char *buildDiffSchema(ColInfo *ci){
  /* Schema: from_<col1>, ..., to_<col1>, ...,
  **         from_commit, to_commit, from_commit_date, to_commit_date, diff_type */
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

    /* from_<col> columns */
    for(i=0; i<ci->nCol; i++){
      if( i > 0 ) p += snprintf(p, end-p, ", ");
      p += snprintf(p, end-p, "\"from_%s\"", ci->azName[i]);
    }

    /* to_<col> columns */
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

/* --------------------------------------------------------------------------
** Buffered audit row
** -------------------------------------------------------------------------- */

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

/* --------------------------------------------------------------------------
** Virtual table structures
** -------------------------------------------------------------------------- */

typedef struct DiffTblVtab DiffTblVtab;
struct DiffTblVtab {
  sqlite3_vtab base;
  sqlite3 *db;
  char *zTableName;
  ColInfo cols;       /* Column names for this table */
};

typedef struct DiffTblCursor DiffTblCursor;
struct DiffTblCursor {
  sqlite3_vtab_cursor base;
  AuditRow *aRows;
  int nRows;
  int nAlloc;
  int iRow;
};

/* --------------------------------------------------------------------------
** Diff callback
** -------------------------------------------------------------------------- */

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

/* --------------------------------------------------------------------------
** Find table root by name
** -------------------------------------------------------------------------- */

static int findTableRootByName(
  struct TableEntry *a, int n, const char *zName,
  ProllyHash *pRoot, u8 *pFlags
){
  int i;
  for(i=0; i<n; i++){
    if( a[i].zName && strcmp(a[i].zName, zName)==0 ){
      memcpy(pRoot, &a[i].root, sizeof(ProllyHash));
      if( pFlags ) *pFlags = a[i].flags;
      return SQLITE_OK;
    }
  }
  memset(pRoot, 0, sizeof(ProllyHash));
  if( pFlags ) *pFlags = 0;
  return SQLITE_NOTFOUND;
}

/* --------------------------------------------------------------------------
** Walk commit history and diff
** -------------------------------------------------------------------------- */

static int walkHistoryAndDiff(
  DiffTblCursor *pCur, sqlite3 *db, const char *zTableName
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  void *pBt = doltliteGetBtShared(db);
  ProllyCache *pCache;
  ProllyHash curHash;
  int rc;

  if( !cs || !pBt ) return SQLITE_OK;
  pCache = (ProllyCache*)(((char*)pBt) + sizeof(ChunkStore));

  chunkStoreGetHeadCommit(cs, &curHash);
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
        findTableRootByName(aTables, nTables, zTableName, &curRoot, &flags);
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
            findTableRootByName(aPT, nPT, zTableName, &parentRoot, 0);
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
        memset(&emptyRoot, 0, sizeof(emptyRoot));
        memset(parentHex, '0', PROLLY_HASH_SIZE*2);
        parentHex[PROLLY_HASH_SIZE*2] = 0;

        CollectCtx ctx;
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

/* --------------------------------------------------------------------------
** Virtual table methods
** -------------------------------------------------------------------------- */

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

  /* Extract table name from module name: "dolt_diff_<tablename>" */
  zModName = argv[0];
  if( zModName && strncmp(zModName, "dolt_diff_", 10)==0 ){
    pVtab->zTableName = sqlite3_mprintf("%s", zModName + 10);
  }else if( argc > 3 ){
    pVtab->zTableName = sqlite3_mprintf("%s", argv[3]);
  }else{
    pVtab->zTableName = sqlite3_mprintf("");
  }

  /* Get column names from the actual table */
  getColumnNames(db, pVtab->zTableName, &pVtab->cols);

  /* Build dynamic schema */
  if( pVtab->cols.nCol > 0 ){
    zSchema = buildDiffSchema(&pVtab->cols);
  }else{
    /* Fallback: generic schema */
    zSchema = sqlite3_mprintf(
      "CREATE TABLE x(from_value, to_value,"
      " from_commit TEXT, to_commit TEXT,"
      " from_commit_date TEXT, to_commit_date TEXT,"
      " diff_type TEXT)");
  }

  if( !zSchema ){
    sqlite3_free(pVtab->zTableName);
    freeColInfo(&pVtab->cols);
    sqlite3_free(pVtab);
    return SQLITE_NOMEM;
  }

  rc = sqlite3_declare_vtab(db, zSchema);
  sqlite3_free(zSchema);
  if( rc!=SQLITE_OK ){
    sqlite3_free(pVtab->zTableName);
    freeColInfo(&pVtab->cols);
    sqlite3_free(pVtab);
    return rc;
  }

  *ppVtab = &pVtab->base;
  return SQLITE_OK;
}

static int dtDisconnect(sqlite3_vtab *pBase){
  DiffTblVtab *pVtab = (DiffTblVtab*)pBase;
  sqlite3_free(pVtab->zTableName);
  freeColInfo(&pVtab->cols);
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

  /* Column layout:
  ** 0..nCols-1         : from_<col> values
  ** nCols..2*nCols-1   : to_<col> values
  ** 2*nCols            : from_commit
  ** 2*nCols+1          : to_commit
  ** 2*nCols+2          : from_commit_date
  ** 2*nCols+3          : to_commit_date
  ** 2*nCols+4          : diff_type
  */

  if( nCols > 0 && col < nCols ){
    /* from_<col> */
    int colIdx = col;
    if( colIdx == pVtab->cols.iPkCol ){
      /* PK column: value comes from the rowid, not the record body.
      ** For 'added' rows (no old val), the PK didn't exist → NULL. */
      if( r->pOldVal && r->nOldVal > 0 ){
        sqlite3_result_int64(ctx, r->intKey);
      }else{
        sqlite3_result_null(ctx);
      }
    }else{
      /* Non-PK column: decode from old record body.
      ** The record body has fields for all columns in schema order,
      ** but the PK field stores NULL (placeholder). Map schema col
      ** index directly to record field index. */
      if( r->pOldVal && r->nOldVal > 0 ){
        RecordInfo ri;
        dtParseRecord(r->pOldVal, r->nOldVal, &ri);
        if( colIdx < ri.nField ){
          dtResultField(ctx, r->pOldVal, r->nOldVal,
                        ri.aType[colIdx], ri.aOffset[colIdx]);
        }else{
          sqlite3_result_null(ctx);
        }
      }else{
        sqlite3_result_null(ctx);
      }
    }
  }else if( nCols > 0 && col < 2*nCols ){
    /* to_<col> */
    int colIdx = col - nCols;
    if( colIdx == pVtab->cols.iPkCol ){
      if( r->pNewVal && r->nNewVal > 0 ){
        sqlite3_result_int64(ctx, r->intKey);
      }else{
        sqlite3_result_null(ctx);
      }
    }else{
      if( r->pNewVal && r->nNewVal > 0 ){
        RecordInfo ri;
        dtParseRecord(r->pNewVal, r->nNewVal, &ri);
        if( colIdx < ri.nField ){
          dtResultField(ctx, r->pNewVal, r->nNewVal,
                        ri.aType[colIdx], ri.aOffset[colIdx]);
        }else{
          sqlite3_result_null(ctx);
        }
      }else{
        sqlite3_result_null(ctx);
      }
    }
  }else{
    /* Fixed columns at the end */
    int fixedCol = col - 2*nCols;
    if( nCols == 0 ) fixedCol = col; /* fallback mode */

    switch( fixedCol ){
      case 0: /* from_commit */
        if( nCols == 0 ){
          /* fallback: from_value blob */
          if( r->pOldVal )
            sqlite3_result_blob(ctx, r->pOldVal, r->nOldVal, SQLITE_TRANSIENT);
          else
            sqlite3_result_null(ctx);
        }else{
          sqlite3_result_text(ctx, r->zFromCommit, -1, SQLITE_TRANSIENT);
        }
        break;
      case 1: /* to_commit */
        if( nCols == 0 ){
          if( r->pNewVal )
            sqlite3_result_blob(ctx, r->pNewVal, r->nNewVal, SQLITE_TRANSIENT);
          else
            sqlite3_result_null(ctx);
        }else{
          sqlite3_result_text(ctx, r->zToCommit, -1, SQLITE_TRANSIENT);
        }
        break;
      case 2: /* from_commit_date */
        { time_t t = (time_t)r->fromDate; struct tm *tm = gmtime(&t);
          if(tm){ char b[32]; strftime(b,sizeof(b),"%Y-%m-%d %H:%M:%S",tm);
            sqlite3_result_text(ctx,b,-1,SQLITE_TRANSIENT);
          }else sqlite3_result_null(ctx); }
        break;
      case 3: /* to_commit_date */
        { time_t t = (time_t)r->toDate; struct tm *tm = gmtime(&t);
          if(tm){ char b[32]; strftime(b,sizeof(b),"%Y-%m-%d %H:%M:%S",tm);
            sqlite3_result_text(ctx,b,-1,SQLITE_TRANSIENT);
          }else sqlite3_result_null(ctx); }
        break;
      case 4: /* diff_type */
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

/* --------------------------------------------------------------------------
** Registration
** -------------------------------------------------------------------------- */

void doltliteRegisterDiffTables(sqlite3 *db){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHash headCommit;
  u8 *data = 0; int nData = 0;
  DoltliteCommit commit;
  struct TableEntry *aTables = 0;
  int nTables = 0, i, rc;

  if( !cs ) return;
  chunkStoreGetHeadCommit(cs, &headCommit);
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

#endif /* DOLTLITE_PROLLY */
