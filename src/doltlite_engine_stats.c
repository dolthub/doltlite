#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "doltlite_internal.h"
#include "pager_shim.h"

/* Returned by sqlite3_db_status before that call zeroes the same fields.
** The name list follows the ProllyStats field order. */
#define ENGINE_STAT_N 15
typedef char prolly_stats_covers_engine_names[
  (sizeof(ProllyStats)==ENGINE_STAT_N*sizeof(u64)) ? 1 : -1];

static const char *const azEngineStatName[ENGINE_STAT_N] = {
  "cache_hit",
  "cache_miss",
  "chunk_read",
  "verify_bytes",
  "chunk_write",
  "hash_bytes",
  "seek",
  "compare",
  "sortkey_parse",
  "record_bytes",
  "pending_insert",
  "pending_lookup",
  "pending_merge",
  "readahead",
  "readahead_unused"
};

typedef struct EngineStatsVtab EngineStatsVtab;
struct EngineStatsVtab {
  sqlite3_vtab base;
  sqlite3 *db;
};

typedef struct EngineStatsCursor EngineStatsCursor;
struct EngineStatsCursor {
  sqlite3_vtab_cursor base;
  int iRow;
  int bReset;
  u64 aValue[ENGINE_STAT_N];
};

static void engineStatsSnapshot(ProllyStats *pStats, u64 *a, int reset){
  a[0] += pStats->nCacheHit;
  a[1] += pStats->nCacheMiss;
  a[2] += pStats->nChunkRead;
  a[3] += pStats->nVerifyBytes;
  a[4] += pStats->nChunkWrite;
  a[5] += pStats->nHashBytes;
  a[6] += pStats->nSeek;
  a[7] += pStats->nCompare;
  a[8] += pStats->nSortKeyParse;
  a[9] += pStats->nRecordBytes;
  a[10] += pStats->nPendingInsert;
  a[11] += pStats->nPendingLookup;
  a[12] += pStats->nPendingMerge;
  a[13] += pStats->nReadAhead;
  a[14] += pStats->nReadAheadUnused;
  if( reset ) memset(pStats, 0, sizeof(*pStats));
}

static void engineStatsCollect(sqlite3 *db, u64 *a, int reset){
  int i;
  memset(a, 0, sizeof(u64) * ENGINE_STAT_N);
  for(i=0; i<db->nDb; i++){
    Btree *pBt;
    ProllyStats *pStats;
    pBt = db->aDb[i].pBt;
    if( pBt==0 ) continue;
    pStats = pagerShimStats(sqlite3BtreePager(pBt));
    if( pStats==0 ) continue;
    engineStatsSnapshot(pStats, a, reset);
  }
}

static int statsConnect(sqlite3 *db, void *pAux, int argc,
    const char *const *argv, sqlite3_vtab **ppVtab, char **pzErr){
  EngineStatsVtab *pVtab;
  int rc;
  (void)pAux; (void)argc; (void)argv; (void)pzErr;
  rc = doltliteVtabConnectSimple(db,
      "CREATE TABLE x(name TEXT, value INTEGER, reset HIDDEN)",
      sizeof(*pVtab), ppVtab);
  if( rc!=SQLITE_OK ) return rc;
  pVtab = (EngineStatsVtab*)*ppVtab;
  pVtab->db = db;
  return SQLITE_OK;
}

static int statsBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  int i;
  (void)pVtab;
  pInfo->estimatedCost = 1.0;
  pInfo->estimatedRows = ENGINE_STAT_N;
  for(i=0; i<pInfo->nConstraint; i++){
    if( !pInfo->aConstraint[i].usable ) continue;
    if( pInfo->aConstraint[i].iColumn!=2 ) continue;
    if( pInfo->aConstraint[i].op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    pInfo->aConstraintUsage[i].argvIndex = 1;
    pInfo->aConstraintUsage[i].omit = 1;
    break;
  }
  return SQLITE_OK;
}

static int statsOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor){
  (void)pVtab;
  return doltliteVtabOpenCursor(ppCursor, sizeof(EngineStatsCursor));
}

static int statsFilter(sqlite3_vtab_cursor *pCursor, int idxNum,
    const char *idxStr, int argc, sqlite3_value **argv){
  EngineStatsCursor *c = (EngineStatsCursor*)pCursor;
  EngineStatsVtab *p = (EngineStatsVtab*)pCursor->pVtab;
  int reset = 0;
  (void)idxNum; (void)idxStr;
  if( argc>0 && sqlite3_value_int(argv[0])!=0 ) reset = 1;
  c->bReset = reset;
  c->iRow = 0;
  engineStatsCollect(p->db, c->aValue, reset);
  return SQLITE_OK;
}

static int statsNext(sqlite3_vtab_cursor *pCursor){
  EngineStatsCursor *c = (EngineStatsCursor*)pCursor;
  c->iRow++;
  return SQLITE_OK;
}

static int statsEof(sqlite3_vtab_cursor *pCursor){
  EngineStatsCursor *c = (EngineStatsCursor*)pCursor;
  return c->iRow>=ENGINE_STAT_N;
}

static int statsColumn(sqlite3_vtab_cursor *pCursor, sqlite3_context *ctx,
    int iCol){
  EngineStatsCursor *c = (EngineStatsCursor*)pCursor;
  if( c->iRow<0 || c->iRow>=ENGINE_STAT_N ){
    sqlite3_result_null(ctx);
    return SQLITE_OK;
  }
  if( iCol==0 ){
    sqlite3_result_text(ctx, azEngineStatName[c->iRow], -1, SQLITE_STATIC);
  }else if( iCol==1 ){
    sqlite3_result_int64(ctx, (sqlite3_int64)c->aValue[c->iRow]);
  }else if( iCol==2 ){
    sqlite3_result_int(ctx, c->bReset);
  }else{
    sqlite3_result_null(ctx);
  }
  return SQLITE_OK;
}

static int statsRowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid){
  EngineStatsCursor *c = (EngineStatsCursor*)pCursor;
  *pRowid = c->iRow;
  return SQLITE_OK;
}

static sqlite3_module engineStatsModule = {
  0, 0, statsConnect, statsBestIndex, doltliteVtabDisconnect, 0,
  statsOpen, doltliteVtabClose, statsFilter, statsNext, statsEof,
  statsColumn, statsRowid,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};

int doltliteEngineStatsRegister(sqlite3 *db){
  return sqlite3_create_module(db, "dolt_engine_stats", &engineStatsModule, 0);
}

#endif
